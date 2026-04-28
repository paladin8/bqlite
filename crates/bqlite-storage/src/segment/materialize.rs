//! Selected-row materialization from [`EncodedColumn`] / [`EncodedBatch`]
//! (CP2 of the zero-copy scan/filter plan).
//!
//! The materialization boundary in the scan/filter segment. Takes an
//! `EncodedColumn` — possibly with an optional row selection — and
//! produces a dense Arrow `ArrayRef` containing only the selected
//! rows.
//!
//! # Null-mask preservation
//!
//! When a row selection is supplied, the boundary builds the output
//! array directly at `selection.len()` and applies the chunk's null
//! bitmap only to the selected logical rows. The forbidden
//! "decode → splice nulls into a row-group-wide buffer → take"
//! sequence is replaced by a single rank-walk over the bitmap that
//! both decides validity and indexes into the non-null value stream
//! (`docs/design/storage/zero-copy-scan-filter.md` §3 / §12 CP2).
//!
//! Without a selection, the boundary still has to materialize every
//! row, so the row-group-wide buffer is the answer rather than an
//! intermediate — the design only forbids it when a filter is about
//! to narrow the rows. The unselected path therefore retains the
//! existing "decode + splice" implementation.

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

/// Materialize just the selected rows of an `EncodedColumn`.
///
/// When `selection` is `Some(_)` and the column is `Encoded`, the
/// output is built directly at `selection.len()` rows; the null
/// bitmap is applied only to the selected logical rows via a rank
/// walk over the bitmap, never spliced row-group-wide. When
/// `selection` is `None` the column is fully materialized.
///
/// `EncodedColumn::Materialized` columns already have nulls baked in
/// (the materialized scan path spliced them at decode time), so the
/// selection is applied via `arrow::compute::take`.
pub fn materialize_encoded_column_selected(
    col: &EncodedColumn,
    ty: &BqlType,
    selection: Option<&RowSelection>,
) -> Result<ArrayRef> {
    match (col, selection) {
        (EncodedColumn::Materialized { array, .. }, None) => Ok(array.clone()),
        (EncodedColumn::Materialized { array, .. }, Some(sel)) => take_selected(array, sel),
        (EncodedColumn::Encoded { chunk, kind, rows }, None) => {
            materialize_encoded_kind(chunk, kind, *rows as usize, ty)
        }
        (EncodedColumn::Encoded { chunk, kind, rows }, Some(sel)) => {
            materialize_encoded_kind_selected(chunk, kind, *rows as usize, ty, sel)
        }
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

/// Selection-aware variant of [`materialize_encoded_kind`].
///
/// Decodes the chunk once into a non-null dense array (the encoder's
/// stored size), then walks `selection` and the null bitmap together
/// to emit `selection.len()` output rows directly — no row-group-wide
/// intermediate, no `arrow::compute::take` over a spliced array.
fn materialize_encoded_kind_selected(
    chunk: &bqlite_core::encoded::PinnedChunk,
    kind: &EncodedKind,
    rows: usize,
    ty: &BqlType,
    selection: &RowSelection,
) -> Result<ArrayRef> {
    match kind {
        EncodedKind::Constant { value } => {
            materialize_constant_selected(value, rows, ty, chunk.nulls.as_deref(), selection)
        }
        EncodedKind::Bool | EncodedKind::PlainFixed { .. } | EncodedKind::PlainString => {
            materialize_plain_selected(chunk, ty, rows, selection)
        }
        EncodedKind::Rle => materialize_rle_selected(chunk, ty, rows, selection),
        EncodedKind::Dictionary { values } => {
            materialize_dictionary_selected(chunk, values.as_ref(), ty, rows, selection)
        }
        other => Err(BqliteError::Execution(format!(
            "materialize: EncodedKind::{other:?} has no selection-aware materializer; \
             CP2 pins these via the Materialized fallback"
        ))),
    }
}

#[inline]
fn bitmap_is_set(bitmap: &[u8], idx: usize) -> bool {
    bitmap[idx / 8] & (1 << (idx % 8)) != 0
}

/// Walk a [`RowSelection`] in ascending row order and call `emit`
/// once per selected row with `(output_index, source)`. `source` is
/// `None` if the chunk's null bitmap marks the row null at that
/// logical position, otherwise `Some(rank)` where `rank` is the
/// 0-based index of that row within the non-null values the encoder
/// stored.
///
/// Builds and returns a [`NullBuffer`] of length `selection.len()`
/// recording which output positions are non-null. The caller fills
/// the values buffer via `emit` and stitches the two buffers into
/// the final Arrow array.
///
/// `null_bitmap == None` means the column has no nulls at all; every
/// selected row is valid and `source = Some(row_index)`.
///
/// **Selection ordering precondition**: `selection` must visit rows
/// in strictly ascending order and contain no duplicates (the
/// `SelectionVector` / `RowSelection::from_runs` invariants enforce
/// this). The rank walk is correct only under that contract — a
/// duplicate or out-of-order entry would re-emit a stale rank.
fn walk_selection_with_nulls<F>(
    null_bitmap: Option<&[u8]>,
    rows: usize,
    selection: &RowSelection,
    mut emit: F,
) -> Result<::arrow::buffer::NullBuffer>
where
    F: FnMut(usize, Option<usize>) -> Result<()>,
{
    let sel_len = selection.len();
    let mut nulls_bb = ::arrow::array::builder::BooleanBufferBuilder::new(sel_len);

    let process_row = |out_idx: &mut usize,
                       last_walked: &mut usize,
                       running_rank: &mut usize,
                       nulls_bb: &mut ::arrow::array::builder::BooleanBufferBuilder,
                       emit: &mut F,
                       row: u32|
     -> Result<()> {
        let r = row as usize;
        if r >= rows {
            return Err(BqliteError::Execution(format!(
                "materialize_selected: selection row {r} out of range (rows = {rows})"
            )));
        }
        match null_bitmap {
            None => {
                emit(*out_idx, Some(r))?;
                nulls_bb.append(true);
            }
            Some(bitmap) => {
                // Catch up rank to position r (exclusive), then test
                // bit r itself.
                while *last_walked < r {
                    if bitmap_is_set(bitmap, *last_walked) {
                        *running_rank += 1;
                    }
                    *last_walked += 1;
                }
                let valid = bitmap_is_set(bitmap, r);
                if valid {
                    emit(*out_idx, Some(*running_rank))?;
                    nulls_bb.append(true);
                } else {
                    emit(*out_idx, None)?;
                    nulls_bb.append(false);
                }
            }
        }
        *out_idx += 1;
        Ok(())
    };

    let mut out_idx = 0usize;
    let mut last_walked = 0usize;
    let mut running_rank = 0usize;
    match selection {
        RowSelection::Indices(sv) => {
            for &row in sv.as_slice() {
                process_row(
                    &mut out_idx,
                    &mut last_walked,
                    &mut running_rank,
                    &mut nulls_bb,
                    &mut emit,
                    row,
                )?;
            }
        }
        RowSelection::Runs(runs) => {
            for run in runs {
                for row in run.start..run.end() {
                    process_row(
                        &mut out_idx,
                        &mut last_walked,
                        &mut running_rank,
                        &mut nulls_bb,
                        &mut emit,
                        row,
                    )?;
                }
            }
        }
    }
    Ok(::arrow::buffer::NullBuffer::new(nulls_bb.finish()))
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

// ─────────────────────────────────────────────────────────────────────────────
// Selection-aware materializers (§12 CP2: null-mask preservation)
//
// These emit an Arrow array sized to `selection.len()` directly. Nulls
// from the chunk's bitmap are folded into the output's null buffer at
// the selected positions only — no row-group-wide intermediate.
// ─────────────────────────────────────────────────────────────────────────────

fn materialize_constant_selected(
    value: &ScalarValue,
    rows: usize,
    ty: &BqlType,
    nulls: Option<&[u8]>,
    selection: &RowSelection,
) -> Result<ArrayRef> {
    match (value, ty) {
        (ScalarValue::Bool(b), BqlType::Bool) => {
            let mut values = Vec::with_capacity(selection.len());
            let null_buffer = walk_selection_with_nulls(nulls, rows, selection, |_, src| {
                values.push(if src.is_some() { *b } else { false });
                Ok(())
            })?;
            let bb = ::arrow::buffer::BooleanBuffer::from_iter(values);
            Ok(Arc::new(BooleanArray::new(bb, Some(null_buffer))))
        }
        (ScalarValue::Int(v), BqlType::Int) => {
            let mut values = Vec::with_capacity(selection.len());
            let null_buffer = walk_selection_with_nulls(nulls, rows, selection, |_, src| {
                values.push(if src.is_some() { *v } else { 0 });
                Ok(())
            })?;
            Ok(Arc::new(Int64Array::new(values.into(), Some(null_buffer))))
        }
        (ScalarValue::Float(v), BqlType::Float) => {
            let mut values = Vec::with_capacity(selection.len());
            let null_buffer = walk_selection_with_nulls(nulls, rows, selection, |_, src| {
                values.push(if src.is_some() { *v } else { 0.0 });
                Ok(())
            })?;
            Ok(Arc::new(Float64Array::new(
                values.into(),
                Some(null_buffer),
            )))
        }
        (ScalarValue::Timestamp(v), BqlType::Timestamp) => {
            let mut values = Vec::with_capacity(selection.len());
            let null_buffer = walk_selection_with_nulls(nulls, rows, selection, |_, src| {
                values.push(if src.is_some() { *v } else { 0 });
                Ok(())
            })?;
            Ok(Arc::new(
                TimestampNanosecondArray::new(values.into(), Some(null_buffer))
                    .with_timezone("UTC"),
            ))
        }
        (ScalarValue::String(s), BqlType::String) => {
            let mut builder = StringViewBuilder::with_capacity(selection.len());
            walk_selection_with_nulls(nulls, rows, selection, |_, src| {
                if src.is_some() {
                    builder.append_value(s);
                } else {
                    builder.append_null();
                }
                Ok(())
            })?;
            Ok(Arc::new(builder.finish()))
        }
        (_, _) => Err(BqliteError::Execution(format!(
            "materialize_constant_selected: scalar/type mismatch ({value:?} vs {ty:?})"
        ))),
    }
}

fn materialize_plain_selected(
    chunk: &bqlite_core::encoded::PinnedChunk,
    ty: &BqlType,
    rows: usize,
    selection: &RowSelection,
) -> Result<ArrayRef> {
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
    select_with_nulls(&dense, ty, chunk.nulls.as_deref(), rows, selection)
}

fn materialize_rle_selected(
    chunk: &bqlite_core::encoded::PinnedChunk,
    ty: &BqlType,
    rows: usize,
    selection: &RowSelection,
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
    select_with_nulls(&dense, ty, chunk.nulls.as_deref(), rows, selection)
}

fn materialize_dictionary_selected(
    chunk: &bqlite_core::encoded::PinnedChunk,
    values: &[u8],
    ty: &BqlType,
    rows: usize,
    selection: &RowSelection,
) -> Result<ArrayRef> {
    if chunk.params.len() != 5 {
        return Err(BqliteError::Execution(format!(
            "materialize_dictionary_selected: chunk params expected 5 bytes, got {}",
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
    match ty {
        BqlType::Int => {
            if !values.len().is_multiple_of(8) {
                return Err(BqliteError::Execution(format!(
                    "materialize_dictionary_selected: Int region length {} is not a multiple of 8",
                    values.len()
                )));
            }
            let cardinality = values.len() / 8;
            let mut dict: Vec<i64> = Vec::with_capacity(cardinality);
            for c in values.chunks_exact(8) {
                dict.push(i64::from_le_bytes(c.try_into().unwrap()));
            }
            let mut out_values = Vec::with_capacity(selection.len());
            let null_buffer = walk_selection_with_nulls(
                chunk.nulls.as_deref(),
                rows,
                selection,
                |_, src| match src {
                    None => {
                        out_values.push(0);
                        Ok(())
                    }
                    Some(rank) => {
                        let code = *codes.get(rank).ok_or_else(|| {
                            BqliteError::Execution(format!(
                                "materialize_dictionary_selected: rank {rank} out of bounds (codes len {})",
                                codes.len()
                            ))
                        })? as usize;
                        if code >= cardinality {
                            return Err(BqliteError::Execution(format!(
                                "materialize_dictionary_selected: code {code} out of bounds (cardinality {cardinality})"
                            )));
                        }
                        out_values.push(dict[code]);
                        Ok(())
                    }
                },
            )?;
            Ok(Arc::new(Int64Array::new(
                out_values.into(),
                Some(null_buffer),
            )))
        }
        BqlType::String => {
            let mut dict: Vec<&str> = Vec::new();
            let mut off = 0usize;
            while off < values.len() {
                if off + 4 > values.len() {
                    return Err(BqliteError::Execution(
                        "materialize_dictionary_selected: String region truncated at length prefix"
                            .into(),
                    ));
                }
                let len = u32::from_le_bytes(values[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                if off + len > values.len() {
                    return Err(BqliteError::Execution(
                        "materialize_dictionary_selected: String region truncated at value".into(),
                    ));
                }
                let s = std::str::from_utf8(&values[off..off + len]).map_err(|e| {
                    BqliteError::Execution(format!(
                        "materialize_dictionary_selected: String value is not valid UTF-8: {e}"
                    ))
                })?;
                off += len;
                dict.push(s);
            }
            let mut builder = StringViewBuilder::with_capacity(selection.len());
            walk_selection_with_nulls(chunk.nulls.as_deref(), rows, selection, |_, src| {
                match src {
                    None => builder.append_null(),
                    Some(rank) => {
                        let code = *codes.get(rank).ok_or_else(|| {
                            BqliteError::Execution(format!(
                                "materialize_dictionary_selected: rank {rank} out of bounds"
                            ))
                        })? as usize;
                        if code >= dict.len() {
                            return Err(BqliteError::Execution(format!(
                                "materialize_dictionary_selected: code {code} out of bounds (cardinality {})",
                                dict.len()
                            )));
                        }
                        builder.append_value(dict[code]);
                    }
                }
                Ok(())
            })?;
            Ok(Arc::new(builder.finish()))
        }
        other => Err(BqliteError::Execution(format!(
            "materialize_dictionary_selected: Dictionary does not support BqlType::{other}"
        ))),
    }
}

/// Type-dispatched "pick rows from a dense Arrow array via
/// `walk_selection_with_nulls`" helper used by `materialize_plain_selected`
/// and `materialize_rle_selected`. `dense` has length `non_null_count`
/// and contains the values the encoder physically stored — i.e. only
/// non-null rows. The output is sized to `selection.len()` and its null
/// buffer reflects the chunk's bitmap at the selected logical
/// positions only.
fn select_with_nulls(
    dense: &ArrayRef,
    ty: &BqlType,
    null_bitmap: Option<&[u8]>,
    rows: usize,
    selection: &RowSelection,
) -> Result<ArrayRef> {
    match ty {
        BqlType::Bool => {
            let src = dense
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("select_with_nulls: expected BooleanArray".into())
                })?;
            let mut values = Vec::with_capacity(selection.len());
            let null_buffer =
                walk_selection_with_nulls(null_bitmap, rows, selection, |_, source| {
                    match source {
                        None => values.push(false),
                        Some(rank) => values.push(src.value(rank)),
                    }
                    Ok(())
                })?;
            let bb = ::arrow::buffer::BooleanBuffer::from_iter(values);
            Ok(Arc::new(BooleanArray::new(bb, Some(null_buffer))))
        }
        BqlType::Int => {
            let src = dense.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("select_with_nulls: expected Int64Array".into())
            })?;
            let src_values = src.values();
            let mut out = Vec::with_capacity(selection.len());
            let null_buffer =
                walk_selection_with_nulls(null_bitmap, rows, selection, |_, source| {
                    match source {
                        None => out.push(0),
                        Some(rank) => out.push(src_values[rank]),
                    }
                    Ok(())
                })?;
            Ok(Arc::new(Int64Array::new(out.into(), Some(null_buffer))))
        }
        BqlType::Float => {
            let src = dense
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    BqliteError::Execution("select_with_nulls: expected Float64Array".into())
                })?;
            let src_values = src.values();
            let mut out = Vec::with_capacity(selection.len());
            let null_buffer =
                walk_selection_with_nulls(null_bitmap, rows, selection, |_, source| {
                    match source {
                        None => out.push(0.0),
                        Some(rank) => out.push(src_values[rank]),
                    }
                    Ok(())
                })?;
            Ok(Arc::new(Float64Array::new(out.into(), Some(null_buffer))))
        }
        BqlType::Timestamp => {
            let src = dense
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "select_with_nulls: expected TimestampNanosecondArray".into(),
                    )
                })?;
            let src_values = src.values();
            let mut out = Vec::with_capacity(selection.len());
            let null_buffer =
                walk_selection_with_nulls(null_bitmap, rows, selection, |_, source| {
                    match source {
                        None => out.push(0),
                        Some(rank) => out.push(src_values[rank]),
                    }
                    Ok(())
                })?;
            Ok(Arc::new(
                TimestampNanosecondArray::new(out.into(), Some(null_buffer)).with_timezone("UTC"),
            ))
        }
        BqlType::String => {
            let src = dense
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("select_with_nulls: expected StringViewArray".into())
                })?;
            let mut builder = StringViewBuilder::with_capacity(selection.len());
            walk_selection_with_nulls(null_bitmap, rows, selection, |_, source| {
                match source {
                    None => builder.append_null(),
                    Some(rank) => builder.append_value(src.value(rank)),
                }
                Ok(())
            })?;
            Ok(Arc::new(builder.finish()))
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "select_with_nulls: nested type {ty:?} is not supported on the encoded selection path"
        ))),
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

    // ── CP2 selection-aware materializers (null-mask preservation) ──────

    /// LSB-first null bitmap of size `valid.len()`. Bit i set ↔ row i
    /// is valid (non-null).
    fn make_null_bitmap(valid: &[bool]) -> Vec<u8> {
        let bytes = valid.len().div_ceil(8);
        let mut out = vec![0u8; bytes];
        for (i, v) in valid.iter().enumerate() {
            if *v {
                out[i / 8] |= 1u8 << (i % 8);
            }
        }
        out
    }

    /// Build an `EncodedColumn::Encoded` Plain Int chunk for testing.
    fn plain_int_encoded(values_dense: Vec<i64>, valid: Option<&[bool]>) -> EncodedColumn {
        // Plain Int payload: each i64 LE.
        let mut payload = Vec::with_capacity(values_dense.len() * 8);
        for v in &values_dense {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        let nulls = valid.map(|v| arc_bytes(make_null_bitmap(v)));
        let rows = valid.map_or(values_dense.len() as u32, |v| v.len() as u32);
        EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: arc_bytes(payload),
                nulls,
                params: arc_bytes(vec![]),
            },
            kind: EncodedKind::PlainFixed { width: 8 },
            rows,
        }
    }

    #[test]
    fn selection_aware_plain_int_no_nulls_indices() {
        let col = plain_int_encoded(vec![10, 20, 30, 40, 50], None);
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2, 4]));
        let out = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
        let ints = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 3);
        assert_eq!(ints.values(), &[10i64, 30, 50]);
        assert_eq!(ints.null_count(), 0);
    }

    #[test]
    fn selection_aware_plain_int_with_nulls_indices() {
        // Logical rows: 10, NULL, 30, NULL, 50. Encoder stored only
        // the three non-null values [10, 30, 50].
        let col = plain_int_encoded(vec![10, 30, 50], Some(&[true, false, true, false, true]));
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 2, 3, 4]));
        let out = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
        let ints = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 4);
        // Selected logical rows 1..=4: NULL, 30, NULL, 50.
        assert!(ints.is_null(0));
        assert_eq!(ints.value(1), 30);
        assert!(ints.is_null(2));
        assert_eq!(ints.value(3), 50);
    }

    #[test]
    fn selection_aware_plain_int_with_nulls_runs() {
        let col = plain_int_encoded(vec![10, 30, 50], Some(&[true, false, true, false, true]));
        // Two runs: rows [0,2) and rows [3,5).
        let sel = RowSelection::from_runs(vec![
            bqlite_core::encoded::RowRun { start: 0, len: 2 },
            bqlite_core::encoded::RowRun { start: 3, len: 2 },
        ]);
        let out = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
        let ints = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 4);
        // Logical rows in order: 0=10, 1=NULL, 3=NULL, 4=50.
        assert_eq!(ints.value(0), 10);
        assert!(ints.is_null(1));
        assert!(ints.is_null(2));
        assert_eq!(ints.value(3), 50);
    }

    #[test]
    fn selection_aware_matches_unselected_then_take() {
        // For every selection, the new selection-aware path must yield
        // the same Arrow values + nulls as "fully materialize then take".
        let col = plain_int_encoded(
            vec![10, 30, 50, 70],
            Some(&[true, false, true, false, true, false, true]),
        );
        let cases = vec![
            RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3, 4, 5, 6])),
            RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2, 4, 6])),
            RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 3, 5])),
            RowSelection::from_indices(SelectionVector::from_sorted(vec![6])),
            RowSelection::from_runs(vec![bqlite_core::encoded::RowRun { start: 1, len: 5 }]),
        ];
        for sel in cases {
            let new_path =
                materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
            let dense = materialize_encoded_column(&col, &BqlType::Int).unwrap();
            let old_path = take_selected(&dense, &sel).unwrap();
            assert_eq!(
                new_path.len(),
                old_path.len(),
                "length mismatch for selection {:?}",
                sel
            );
            let new_ints = new_path.as_any().downcast_ref::<Int64Array>().unwrap();
            let old_ints = old_path.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..new_ints.len() {
                assert_eq!(
                    new_ints.is_null(i),
                    old_ints.is_null(i),
                    "null-mask mismatch at row {i} for {sel:?}"
                );
                if !new_ints.is_null(i) {
                    assert_eq!(
                        new_ints.value(i),
                        old_ints.value(i),
                        "value mismatch at row {i} for {sel:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn selection_aware_constant_with_nulls() {
        // 5 logical rows, all set to 7 except rows 1 and 3 are null.
        let bitmap = make_null_bitmap(&[true, false, true, false, true]);
        let chunk = PinnedChunk {
            payload: arc_bytes(vec![]),
            nulls: Some(arc_bytes(bitmap)),
            params: arc_bytes(vec![]),
        };
        let col = EncodedColumn::Encoded {
            chunk,
            kind: EncodedKind::Constant {
                value: Arc::new(ScalarValue::Int(7)),
            },
            rows: 5,
        };
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3, 4]));
        let out = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
        let ints = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 5);
        assert_eq!(ints.value(0), 7);
        assert!(ints.is_null(1));
        assert_eq!(ints.value(2), 7);
        assert!(ints.is_null(3));
        assert_eq!(ints.value(4), 7);
    }

    #[test]
    fn selection_aware_constant_string_with_nulls() {
        let bitmap = make_null_bitmap(&[true, false, true]);
        let chunk = PinnedChunk {
            payload: arc_bytes(vec![]),
            nulls: Some(arc_bytes(bitmap)),
            params: arc_bytes(vec![]),
        };
        let col = EncodedColumn::Encoded {
            chunk,
            kind: EncodedKind::Constant {
                value: Arc::new(ScalarValue::String("hi".to_string())),
            },
            rows: 3,
        };
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2]));
        let out = materialize_encoded_column_selected(&col, &BqlType::String, Some(&sel)).unwrap();
        let sv = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(sv.value(0), "hi");
        assert!(sv.is_null(1));
        assert_eq!(sv.value(2), "hi");
    }

    #[test]
    fn selection_aware_does_not_allocate_row_group_buffer() {
        // Smoke check: the selection-aware path must not depend on the
        // logical row count for sizing (otherwise it would be making
        // the row-group-wide allocation the design forbids). Use a row
        // count of 100 with a selection of just two rows; the output
        // length is 2.
        let mut valid: Vec<bool> = (0..100).map(|i| i % 2 == 0).collect();
        valid[0] = true;
        valid[2] = true;
        let dense_values: Vec<i64> = (0..50).map(|i| (i * 10) as i64).collect();
        let col = plain_int_encoded(dense_values, Some(&valid));
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2]));
        let out = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
        let ints = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.len(), 2);
        assert_eq!(ints.value(0), 0); // logical row 0 → rank 0
        assert_eq!(ints.value(1), 10); // logical row 2 → rank 1 (rank 0 = row 0, rank 1 = row 2)
    }

    #[test]
    fn selection_aware_empty_selection_yields_zero_rows() {
        let col = plain_int_encoded(vec![10, 20, 30], Some(&[true, true, true]));
        let sel = RowSelection::empty();
        let out = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel)).unwrap();
        assert_eq!(out.len(), 0);
        // Null buffer is empty too — no nulls, no values.
        assert_eq!(out.null_count(), 0);
    }

    #[test]
    fn selection_aware_out_of_range_row_errors() {
        let col = plain_int_encoded(vec![1, 2, 3], None);
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 5]));
        let err = materialize_encoded_column_selected(&col, &BqlType::Int, Some(&sel))
            .expect_err("row 5 exceeds logical row count 3");
        assert!(matches!(err, BqliteError::Execution(_)));
    }
}
