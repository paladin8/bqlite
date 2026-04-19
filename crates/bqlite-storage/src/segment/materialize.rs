//! Selected-row materialization from [`EncodedColumn`] / [`EncodedBatch`]
//! (CP2 of the zero-copy scan/filter plan).
//!
//! The materialization boundary in the scan/filter segment. Takes an
//! `EncodedColumn` — possibly with an optional row selection — and
//! produces a dense Arrow `ArrayRef` containing only the selected
//! rows.
//!
//! # CP2 scope
//!
//! CP2 ships full-row-group materialization: every encoded column is
//! decoded to a dense array with null splicing applied. This matches
//! the byte-for-byte output of the existing `decode_column_chunk`
//! path so the encoded path can be used as a drop-in alternative to
//! `next_row_group`.
//!
//! Selected-row materialization (consuming a `RowSelection` to skip
//! rows) is added in CP3 alongside the first selection-producing
//! kernels. The scaffolding in `materialize_encoded_column_selected`
//! accepts the selection argument today and applies it via
//! `arrow::compute::take` as a CP2-compatible fallback; CP3 and CP6
//! replace the inner decode with selection-aware kernels.

use std::sync::Arc;

use ::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray, StringViewBuilder,
    TimestampNanosecondArray, UInt32Array,
};
use ::arrow::compute;
use ::arrow::datatypes::TimeUnit;

use bqlite_core::encoded::{EncodedBatch, EncodedColumn, EncodedKind, RowSelection};
use bqlite_core::scalar::ScalarValue;
use bqlite_core::{BqlType, BqliteError, Result};

use crate::encoding::Encoding;

/// Materialize every row of an `EncodedColumn` into a dense Arrow
/// `ArrayRef`, splicing nulls back in.
///
/// For `EncodedColumn::Materialized` this is `array.clone()` — the
/// reader already decoded and spliced nulls on that path.
pub fn materialize_encoded_column(col: &EncodedColumn, ty: &BqlType) -> Result<ArrayRef> {
    match col {
        EncodedColumn::Materialized { array, .. } => Ok(array.clone()),
        EncodedColumn::Encoded { chunk, kind, rows } => {
            materialize_encoded_kind(chunk, kind, *rows as usize, ty)
        }
    }
}

/// Materialize just the selected rows of an `EncodedColumn`, applying
/// `selection` (when `Some`) after a full dense decode.
///
/// CP2 implements this by decoding the whole column and then calling
/// `arrow::compute::take`. CP3/CP4 replace the decode step with
/// selection-aware kernels for the encodings that have them.
pub fn materialize_encoded_column_selected(
    col: &EncodedColumn,
    ty: &BqlType,
    selection: Option<&RowSelection>,
) -> Result<ArrayRef> {
    let dense = materialize_encoded_column(col, ty)?;
    match selection {
        None => Ok(dense),
        Some(sel) => take_selected(&dense, sel),
    }
}

/// Materialize an entire [`EncodedBatch`] into a `Vec<ArrayRef>`
/// ordered by input columns.
///
/// `types` lists each column's logical `BqlType`. Lengths must match;
/// callers typically pass the plan's output types.
pub fn materialize_encoded_batch(batch: &EncodedBatch, types: &[BqlType]) -> Result<Vec<ArrayRef>> {
    if batch.columns.len() != types.len() {
        return Err(BqliteError::Execution(format!(
            "materialize_encoded_batch: batch has {} columns but {} types were provided",
            batch.columns.len(),
            types.len()
        )));
    }
    batch
        .columns
        .iter()
        .zip(types.iter())
        .map(|(col, ty)| materialize_encoded_column(col, ty))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

fn take_selected(array: &ArrayRef, selection: &RowSelection) -> Result<ArrayRef> {
    let indices: Vec<u32> = match selection {
        RowSelection::Indices(sv) => sv.as_slice().to_vec(),
        RowSelection::Runs(runs) => runs
            .iter()
            .flat_map(|r| r.start..r.end())
            .collect::<Vec<u32>>(),
    };
    let index_array = UInt32Array::from(indices);
    compute::take(array.as_ref(), &index_array, None).map_err(|e| {
        BqliteError::Execution(format!("materialize: arrow::compute::take failed: {e}"))
    })
}

fn materialize_encoded_kind(
    chunk: &bqlite_core::encoded::PinnedChunk,
    kind: &EncodedKind,
    rows: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    match kind {
        EncodedKind::Constant { value } => {
            materialize_constant(value, rows, ty, chunk.nulls.as_deref())
        }
        EncodedKind::Bool | EncodedKind::PlainFixed { .. } | EncodedKind::PlainString => {
            materialize_plain(chunk, ty, rows)
        }
        EncodedKind::Rle => materialize_rle(chunk, ty, rows),
        EncodedKind::Dictionary { values } => {
            materialize_dictionary(chunk, values.as_ref(), ty, rows)
        }
        // Every other kind in the encoded IR is unreachable on the
        // CP2 read path because `pin_column_chunk` routes them
        // through `EncodedColumn::Materialized`. If they ever show
        // up, route them through the encoding trait so the error is
        // a pure programmer mistake rather than silent divergence.
        other => Err(BqliteError::Execution(format!(
            "materialize: EncodedKind::{other:?} has no direct materializer; \
             CP2 pins these via the Materialized fallback"
        ))),
    }
}

fn materialize_constant(
    value: &ScalarValue,
    rows: usize,
    ty: &BqlType,
    nulls: Option<&[u8]>,
) -> Result<ArrayRef> {
    let dense: ArrayRef = match (value, ty) {
        (ScalarValue::Bool(b), BqlType::Bool) => Arc::new(BooleanArray::from(vec![*b; rows])),
        (ScalarValue::Int(v), BqlType::Int) => Arc::new(Int64Array::from(vec![*v; rows])),
        (ScalarValue::Float(v), BqlType::Float) => Arc::new(Float64Array::from(vec![*v; rows])),
        (ScalarValue::Timestamp(v), BqlType::Timestamp) => {
            Arc::new(TimestampNanosecondArray::from(vec![*v; rows]).with_timezone("UTC"))
        }
        (ScalarValue::String(s), BqlType::String) => {
            let mut b = StringViewBuilder::with_capacity(rows);
            for _ in 0..rows {
                b.append_value(s);
            }
            Arc::new(b.finish())
        }
        (_, _) => {
            return Err(BqliteError::Execution(format!(
                "materialize_constant: scalar/type mismatch ({value:?} vs {ty:?})"
            )))
        }
    };
    match nulls {
        None => Ok(dense),
        Some(bitmap) => apply_null_bitmap(&dense, bitmap, rows, ty),
    }
}

fn materialize_plain(
    chunk: &bqlite_core::encoded::PinnedChunk,
    ty: &BqlType,
    rows: usize,
) -> Result<ArrayRef> {
    // Reconstruct a BorrowedEncodedChunk and delegate to the existing
    // Plain decoder so the byte-level layout contract lives in one
    // place (encoding/plain.rs).
    let non_null_count = match chunk.nulls.as_deref() {
        None => rows,
        Some(b) => super::reader::count_set_bits_pub(b, rows),
    };
    let borrowed = crate::encoding::BorrowedEncodedChunk {
        encoding: crate::encoding::EncodingType::Plain,
        params: &chunk.params,
        payload: &chunk.payload,
        row_count: non_null_count,
    };
    let dense = crate::encoding::Plain.decode_borrowed(borrowed, ty)?;
    match chunk.nulls.as_deref() {
        None => Ok(dense),
        Some(bitmap) => super::reader::splice_nulls_pub(&dense, bitmap, rows, ty),
    }
}

fn materialize_rle(
    chunk: &bqlite_core::encoded::PinnedChunk,
    ty: &BqlType,
    rows: usize,
) -> Result<ArrayRef> {
    let non_null_count = match chunk.nulls.as_deref() {
        None => rows,
        Some(b) => super::reader::count_set_bits_pub(b, rows),
    };
    let borrowed = crate::encoding::BorrowedEncodedChunk {
        encoding: crate::encoding::EncodingType::Rle,
        params: &chunk.params,
        payload: &chunk.payload,
        row_count: non_null_count,
    };
    let dense = crate::encoding::Rle.decode_borrowed(borrowed, ty)?;
    match chunk.nulls.as_deref() {
        None => Ok(dense),
        Some(bitmap) => super::reader::splice_nulls_pub(&dense, bitmap, rows, ty),
    }
}

/// Materialize a [`EncodedKind::Dictionary`] column back to a dense
/// Arrow array. Codes live in `chunk.payload` (bit-packed, non-null
/// count); the dictionary region lives in `values` (raw bytes parsed
/// per logical type); `chunk.params` is the on-disk 5-byte block
/// `dict_id u32 LE + code_bit_width u8` — only byte 4 is relevant
/// here.
fn materialize_dictionary(
    chunk: &bqlite_core::encoded::PinnedChunk,
    values: &[u8],
    ty: &BqlType,
    rows: usize,
) -> Result<ArrayRef> {
    if chunk.params.len() != 5 {
        return Err(BqliteError::Execution(format!(
            "materialize: Dictionary chunk params expected 5 bytes, got {}",
            chunk.params.len()
        )));
    }
    let code_bit_width = chunk.params[4];
    let non_null_count = match chunk.nulls.as_deref() {
        None => rows,
        Some(b) => super::reader::count_set_bits_pub(b, rows),
    };
    let codes =
        crate::encoding::dictionary::unpack_codes(&chunk.payload, non_null_count, code_bit_width)?;
    let dense: ArrayRef = match ty {
        BqlType::Int => {
            if !values.len().is_multiple_of(8) {
                return Err(BqliteError::Execution(format!(
                    "materialize: Dictionary Int region length {} is not a multiple of 8",
                    values.len()
                )));
            }
            let cardinality = values.len() / 8;
            let mut dict: Vec<i64> = Vec::with_capacity(cardinality);
            for c in values.chunks_exact(8) {
                dict.push(i64::from_le_bytes(c.try_into().unwrap()));
            }
            let mut out = Vec::with_capacity(non_null_count);
            for code in &codes {
                let idx = *code as usize;
                if idx >= cardinality {
                    return Err(BqliteError::Execution(format!(
                        "materialize: Dictionary code {code} out of bounds (cardinality {cardinality})"
                    )));
                }
                out.push(dict[idx]);
            }
            Arc::new(Int64Array::from(out))
        }
        BqlType::String => {
            let mut dict: Vec<&str> = Vec::new();
            let mut off = 0usize;
            while off < values.len() {
                if off + 4 > values.len() {
                    return Err(BqliteError::Execution(
                        "materialize: Dictionary String region truncated at length prefix".into(),
                    ));
                }
                let len = u32::from_le_bytes(values[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                if off + len > values.len() {
                    return Err(BqliteError::Execution(
                        "materialize: Dictionary String region truncated at value".into(),
                    ));
                }
                let s = std::str::from_utf8(&values[off..off + len]).map_err(|e| {
                    BqliteError::Execution(format!(
                        "materialize: Dictionary String value is not valid UTF-8: {e}"
                    ))
                })?;
                off += len;
                dict.push(s);
            }
            let mut b = StringViewBuilder::with_capacity(non_null_count);
            for code in &codes {
                let idx = *code as usize;
                if idx >= dict.len() {
                    return Err(BqliteError::Execution(format!(
                        "materialize: Dictionary code {code} out of bounds (cardinality {})",
                        dict.len()
                    )));
                }
                b.append_value(dict[idx]);
            }
            Arc::new(b.finish())
        }
        other => {
            return Err(BqliteError::Execution(format!(
                "materialize: Dictionary encoding does not support BqlType::{other}"
            )));
        }
    };
    match chunk.nulls.as_deref() {
        None => Ok(dense),
        Some(bitmap) => super::reader::splice_nulls_pub(&dense, bitmap, rows, ty),
    }
}

fn apply_null_bitmap(
    dense: &ArrayRef,
    bitmap: &[u8],
    rows: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    // `dense` is already full-length; reconstruct with a null buffer.
    use ::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
    let bb = BooleanBuffer::new(Buffer::from_slice_ref(bitmap), 0, rows);
    let nb = NullBuffer::new(bb);
    match ty {
        BqlType::Bool => {
            let a = dense
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("apply_null_bitmap: expected BooleanArray".into())
                })?;
            Ok(Arc::new(BooleanArray::new(a.values().clone(), Some(nb))))
        }
        BqlType::Int => {
            let a = dense.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("apply_null_bitmap: expected Int64Array".into())
            })?;
            Ok(Arc::new(Int64Array::new(a.values().clone(), Some(nb))))
        }
        BqlType::Float => {
            let a = dense
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    BqliteError::Execution("apply_null_bitmap: expected Float64Array".into())
                })?;
            Ok(Arc::new(Float64Array::new(a.values().clone(), Some(nb))))
        }
        BqlType::Timestamp => {
            let a = dense
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "apply_null_bitmap: expected TimestampNanosecondArray".into(),
                    )
                })?;
            Ok(Arc::new(
                TimestampNanosecondArray::new(a.values().clone(), Some(nb)).with_timezone("UTC"),
            ))
        }
        BqlType::String => {
            // StringViewArray construction from an existing view array
            // + null buffer is awkward; rebuild through the builder.
            let a = dense
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("apply_null_bitmap: expected StringViewArray".into())
                })?;
            let mut b = StringViewBuilder::with_capacity(rows);
            for i in 0..rows {
                if nb.is_valid(i) {
                    b.append_value(a.value(i));
                } else {
                    b.append_null();
                }
            }
            Ok(Arc::new(b.finish()))
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "apply_null_bitmap: nested type {ty:?} is not yet supported on the encoded path"
        ))),
    }
}

// Silence dead-code lint in non-test profile.
#[allow(dead_code)]
const _TIMEUNIT_USED: TimeUnit = TimeUnit::Nanosecond;

#[cfg(test)]
mod tests {
    use super::*;
    use ::arrow::array::StringViewArray;
    use bqlite_core::encoded::{ArcBytes, PinnedChunk, SelectionVector};

    fn arc_bytes(b: Vec<u8>) -> ArcBytes {
        Arc::from(b)
    }

    #[test]
    fn materialize_constant_int_fills_rows() {
        let chunk = PinnedChunk {
            payload: arc_bytes(vec![]),
            nulls: None,
            params: arc_bytes(vec![]),
        };
        let col = EncodedColumn::Encoded {
            chunk,
            kind: EncodedKind::Constant {
                value: Arc::new(ScalarValue::Int(7)),
            },
            rows: 5,
        };
        let arr = materialize_encoded_column(&col, &BqlType::Int).unwrap();
        let ints = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 5);
        assert!(ints.iter().all(|v| v == Some(7)));
    }

    #[test]
    fn materialize_constant_string_fills_rows() {
        let chunk = PinnedChunk {
            payload: arc_bytes(vec![]),
            nulls: None,
            params: arc_bytes(vec![]),
        };
        let col = EncodedColumn::Encoded {
            chunk,
            kind: EncodedKind::Constant {
                value: Arc::new(ScalarValue::String("x".to_string())),
            },
            rows: 3,
        };
        let arr = materialize_encoded_column(&col, &BqlType::String).unwrap();
        let sv = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(sv.len(), 3);
        for i in 0..3 {
            assert_eq!(sv.value(i), "x");
        }
    }

    #[test]
    fn take_selected_applies_indices() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));
        let sel = RowSelection::Indices(SelectionVector::from_sorted(vec![0, 2, 4]));
        let out = take_selected(&arr, &sel).unwrap();
        let ints = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.values().to_vec(), vec![10, 30, 50]);
    }

    #[test]
    fn materialize_materialized_fallback_passes_through() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let col = EncodedColumn::Materialized {
            array: arr.clone(),
            rows: 3,
        };
        let out = materialize_encoded_column(&col, &BqlType::Int).unwrap();
        assert_eq!(out.len(), 3);
        assert!(Arc::ptr_eq(&arr, &out));
    }
}
