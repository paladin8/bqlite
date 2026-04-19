//! Reader-side encoded view construction (CP2 of the zero-copy
//! scan/filter plan).
//!
//! Parses a column chunk from its framed v1/v2 bytes into a
//! [`PinnedChunk`] + [`EncodedKind`], without eagerly decoding row
//! values. LZ4 decompression happens at most once per chunk and the
//! resulting buffer is shared via `Arc<[u8]>`.
//!
//! # CP2 coverage
//!
//! This checkpoint ships encoded views for:
//! - `Constant` — literal parsed into a [`ScalarValue`].
//! - `PlainFixed` — width inferred from column type.
//! - `PlainString` — no side data.
//! - `Rle` — no side data.
//!
//! Every other encoding (`Dictionary`, `Delta`, `DoubleDelta`,
//! `BitPacking`, `For`, `PFor`, `Alp`, `Fsst`) routes through the
//! transitional [`EncodedColumn::Materialized`] fallback, which
//! invokes the existing row-group decode path. CP3 and CP4 promote
//! `Dictionary` and `Rle` kernels; CP6 covers the rest.
//!
//! # Nulls are carried separately
//!
//! Columns with a validity bitmap keep the bitmap in
//! [`PinnedChunk::nulls`] — it is never spliced into a dense Arrow
//! array on the encoded path. The materialization boundary
//! ([`crate::segment::materialize::materialize_encoded_column`])
//! re-splices nulls when a dense `ArrayRef` is required.

use std::borrow::Cow;
use std::sync::Arc;

use ::arrow::array::ArrayRef;

use bqlite_core::encoded::{
    ArcBytes, EncodedBatch, EncodedColumn, EncodedKind, PinnedChunk,
};
use bqlite_core::scalar::ScalarValue;
use bqlite_core::{BqlType, BqliteError, ColumnDef, Result};

use crate::encoding::{decompress_lz4, EncodingType};
use crate::segment::layout::{ColumnChunkMeta, CompressionType};

/// Result of parsing one column chunk's framed bytes.
///
/// Either a real encoded view ([`EncodedColumn::Encoded`]) for
/// encodings with a first-class kernel surface, or a transitional
/// materialized fallback the reader has already decoded to an Arrow
/// array.
pub struct PinnedColumn {
    pub column: EncodedColumn,
}

/// Parse one column chunk into an [`EncodedColumn`] without decoding
/// row values for the encodings that have an encoded kind in CP2.
///
/// `bytes` is the entire segment file; `meta` points at the column
/// chunk inside it. `row_group_row_count` is the outer row-group's
/// logical row count (may exceed `meta.row_count` for all-null
/// columns — though v1 currently materializes those via Constant).
///
/// Returns either an `Encoded` variant (for Constant / PlainFixed /
/// PlainString / Rle) or a `Materialized` fallback. Callers that
/// always need a dense array should run the result through
/// [`crate::segment::materialize::materialize_encoded_column`].
pub fn pin_column_chunk(
    bytes: &[u8],
    meta: &ColumnChunkMeta,
    write_time_col: &ColumnDef,
    row_group_row_count: usize,
    materialize_fallback: impl FnOnce() -> Result<ArrayRef>,
) -> Result<EncodedColumn> {
    let chunk_start = meta.byte_offset as usize;
    let chunk_end = chunk_start + meta.byte_length as usize;
    if chunk_end > bytes.len() {
        return Err(BqliteError::Corruption(format!(
            "segment reader: column chunk for ordinal {} extends beyond file ({} > {})",
            meta.column_ordinal,
            chunk_end,
            bytes.len()
        )));
    }
    let chunk_bytes = &bytes[chunk_start..chunk_end];
    let mut cursor = 0usize;

    // 1. Null bitmap (if nullable).
    let null_bitmap: Option<ArcBytes> = if write_time_col.nullable {
        let bitmap_len = row_group_row_count.div_ceil(8);
        if chunk_bytes.len() < bitmap_len {
            return Err(BqliteError::Corruption(format!(
                "segment reader: column chunk for `{}` too short for null bitmap \
                 (expected at least {} bytes, got {})",
                write_time_col.name,
                bitmap_len,
                chunk_bytes.len()
            )));
        }
        let bits: Arc<[u8]> = Arc::from(&chunk_bytes[..bitmap_len]);
        cursor += bitmap_len;
        Some(bits)
    } else {
        None
    };

    // 2. Encoding discriminant.
    if chunk_bytes.len() <= cursor {
        return Err(BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}` missing encoding discriminant",
            write_time_col.name
        )));
    }
    let encoding_byte = chunk_bytes[cursor];
    cursor += 1;
    let encoding = EncodingType::from_discriminant(encoding_byte).map_err(|e| {
        BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}`: {e}",
            write_time_col.name
        ))
    })?;

    // 3. Encoding-specific params.
    let params_start = cursor;
    let params_len = super::reader::parse_encoding_params_len_pub(
        encoding,
        &chunk_bytes[cursor..],
        &write_time_col.bql_type,
    )
    .map_err(|e| {
        BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}`: {e}",
            write_time_col.name
        ))
    })?;
    cursor += params_len;
    let on_disk_params = &chunk_bytes[params_start..params_start + params_len];

    // 4. uncompressed_payload_length: u32 LE.
    if chunk_bytes.len() < cursor + 4 {
        return Err(BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}` missing uncompressed_payload_length",
            write_time_col.name
        )));
    }
    let uncompressed_payload_length = u32::from_le_bytes(
        chunk_bytes[cursor..cursor + 4]
            .try_into()
            .expect("slice length checked above"),
    ) as usize;
    cursor += 4;

    // 5. Payload bytes (compressed or raw).
    let on_disk_payload = &chunk_bytes[cursor..];

    // 6. Decompress if needed (at most once).
    let compression = CompressionType::from_discriminant(meta.compression).map_err(|e| {
        BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}`: {e}",
            write_time_col.name
        ))
    })?;
    let uncompressed_payload: Cow<'_, [u8]> = match compression {
        CompressionType::None => {
            if on_disk_payload.len() != uncompressed_payload_length {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: column chunk for `{}`: uncompressed payload length \
                     disagrees with on-disk size ({} vs {})",
                    write_time_col.name,
                    uncompressed_payload_length,
                    on_disk_payload.len()
                )));
            }
            Cow::Borrowed(on_disk_payload)
        }
        CompressionType::Lz4 => Cow::Owned(
            decompress_lz4(on_disk_payload, uncompressed_payload_length).map_err(|e| {
                BqliteError::Corruption(format!(
                    "segment reader: column chunk for `{}`: lz4 decompress failed: {e}",
                    write_time_col.name
                ))
            })?,
        ),
    };

    // 7. Branch on encoding: build an Encoded view where supported,
    //    else fall through to the materialized fallback.
    let rows = meta.row_count as u32;
    match encoding {
        EncodingType::Constant => {
            // Parse the constant literal into a ScalarValue so
            // downstream kernels can short-circuit without re-parsing.
            let scalar = parse_constant_literal(on_disk_params, &write_time_col.bql_type)?;
            let payload_arc: ArcBytes = Arc::from(uncompressed_payload.as_ref());
            let params_arc: ArcBytes = Arc::from(on_disk_params);
            Ok(EncodedColumn::Encoded {
                chunk: PinnedChunk {
                    payload: payload_arc,
                    nulls: null_bitmap,
                    params: params_arc,
                },
                kind: EncodedKind::Constant {
                    value: Arc::new(scalar),
                },
                rows,
            })
        }
        EncodingType::Plain => {
            let payload_arc: ArcBytes = Arc::from(uncompressed_payload.as_ref());
            let params_arc: ArcBytes = Arc::from(on_disk_params);
            let kind = match &write_time_col.bql_type {
                BqlType::Bool => EncodedKind::Bool,
                BqlType::Int | BqlType::Float | BqlType::Timestamp => {
                    EncodedKind::PlainFixed { width: 8 }
                }
                BqlType::String => EncodedKind::PlainString,
                BqlType::List(_) | BqlType::Map(_) => {
                    // Nested types stay on the materialized fallback
                    // until the plain-encoding path for them is
                    // revisited.
                    return Ok(EncodedColumn::Materialized {
                        array: materialize_fallback()?,
                        rows,
                    });
                }
            };
            Ok(EncodedColumn::Encoded {
                chunk: PinnedChunk {
                    payload: payload_arc,
                    nulls: null_bitmap,
                    params: params_arc,
                },
                kind,
                rows,
            })
        }
        EncodingType::Rle => {
            let payload_arc: ArcBytes = Arc::from(uncompressed_payload.as_ref());
            let params_arc: ArcBytes = Arc::from(on_disk_params);
            Ok(EncodedColumn::Encoded {
                chunk: PinnedChunk {
                    payload: payload_arc,
                    nulls: null_bitmap,
                    params: params_arc,
                },
                kind: EncodedKind::Rle,
                rows,
            })
        }
        // Everything else: transitional materialized fallback. CP3
        // promotes Dictionary; CP4 RLE kernels build on CP2's Rle
        // view; CP6 covers compressed numeric / FSST.
        EncodingType::Dictionary
        | EncodingType::Delta
        | EncodingType::DoubleDelta
        | EncodingType::BitPacking
        | EncodingType::Fsst
        | EncodingType::For
        | EncodingType::PFor
        | EncodingType::Alp => Ok(EncodedColumn::Materialized {
            array: materialize_fallback()?,
            rows,
        }),
    }
}

/// Parse a `Constant` literal block (same layout as
/// `constant::decode_literal`, but emitting a [`ScalarValue`]).
fn parse_constant_literal(params: &[u8], ty: &BqlType) -> Result<ScalarValue> {
    if params.is_empty() {
        return Err(BqliteError::Corruption(
            "segment reader: Constant encoding missing value_kind byte".into(),
        ));
    }
    let value_kind = params[0];
    if value_kind != 0 {
        return Err(BqliteError::Corruption(format!(
            "segment reader: Constant value_kind {value_kind} is not the literal \
             discriminant 0 supported by the encoded view"
        )));
    }
    let literal = &params[1..];
    match ty {
        BqlType::Bool => {
            if literal.len() != 1 {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Constant Bool literal is {} bytes (expected 1)",
                    literal.len()
                )));
            }
            match literal[0] {
                0 => Ok(ScalarValue::Bool(false)),
                1 => Ok(ScalarValue::Bool(true)),
                other => Err(BqliteError::Corruption(format!(
                    "segment reader: Constant Bool literal byte {other} is not 0 or 1"
                ))),
            }
        }
        BqlType::Int => {
            if literal.len() != 8 {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Constant Int literal is {} bytes (expected 8)",
                    literal.len()
                )));
            }
            Ok(ScalarValue::Int(i64::from_le_bytes(
                literal.try_into().unwrap(),
            )))
        }
        BqlType::Float => {
            if literal.len() != 8 {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Constant Float literal is {} bytes (expected 8)",
                    literal.len()
                )));
            }
            Ok(ScalarValue::Float(f64::from_le_bytes(
                literal.try_into().unwrap(),
            )))
        }
        BqlType::Timestamp => {
            if literal.len() != 8 {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Constant Timestamp literal is {} bytes (expected 8)",
                    literal.len()
                )));
            }
            Ok(ScalarValue::Timestamp(i64::from_le_bytes(
                literal.try_into().unwrap(),
            )))
        }
        BqlType::String => {
            if literal.len() < 4 {
                return Err(BqliteError::Corruption(
                    "segment reader: Constant String literal missing length prefix".into(),
                ));
            }
            let len = u32::from_le_bytes(literal[..4].try_into().unwrap()) as usize;
            if literal.len() != 4 + len {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Constant String literal length mismatch \
                     (expected {} bytes, got {})",
                    4 + len,
                    literal.len()
                )));
            }
            let s = std::str::from_utf8(&literal[4..]).map_err(|e| {
                BqliteError::Corruption(format!(
                    "segment reader: Constant String literal has invalid UTF-8: {e}"
                ))
            })?;
            Ok(ScalarValue::String(s.to_owned()))
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Corruption(format!(
            "segment reader: Constant encoding does not carry a literal for nested type {ty:?}"
        ))),
    }
}

/// Build an [`EncodedBatch`] from a vector of per-column
/// [`EncodedColumn`]s, validating the shared row count.
pub fn encoded_batch(row_count: u32, columns: Vec<EncodedColumn>) -> EncodedBatch {
    EncodedBatch::new(row_count, columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_constant_bool_false() {
        let params = &[0u8, 0u8]; // value_kind=0, value=false
        let sv = parse_constant_literal(params, &BqlType::Bool).unwrap();
        assert_eq!(sv, ScalarValue::Bool(false));
    }

    #[test]
    fn parse_constant_int_roundtrip() {
        let mut params = vec![0u8];
        params.extend_from_slice(&42i64.to_le_bytes());
        let sv = parse_constant_literal(&params, &BqlType::Int).unwrap();
        assert_eq!(sv, ScalarValue::Int(42));
    }

    #[test]
    fn parse_constant_string_roundtrip() {
        let s = "hello";
        let mut params = vec![0u8];
        params.extend_from_slice(&(s.len() as u32).to_le_bytes());
        params.extend_from_slice(s.as_bytes());
        let sv = parse_constant_literal(&params, &BqlType::String).unwrap();
        assert_eq!(sv, ScalarValue::String("hello".to_string()));
    }

    #[test]
    fn parse_constant_rejects_all_null_value_kind() {
        let params = &[1u8]; // value_kind=1, reserved for all-null
        let err = parse_constant_literal(params, &BqlType::Int).unwrap_err();
        assert!(matches!(err, BqliteError::Corruption(_)));
    }
}
