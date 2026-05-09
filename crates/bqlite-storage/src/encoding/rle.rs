//! Run-length encoding — compact representation for sorted repetitive columns.
//!
//! RLE stores each maximal run of identical values as a `(run_end, value)`
//! pair. The on-disk layout is:
//!
//! ```text
//! params:
//!     run_count:  u32 LE         // number of runs
//! payload:
//!     run_ends:   [u32 LE; run_count]   // cumulative row end per run
//!     values:     <type-dependent>       // one value per run
//! ```
//!
//! `run_ends[i]` is the cumulative number of rows up to and including
//! run `i`, matching Arrow's `RunEndEncodedArray` convention for zero-copy
//! decode. For example, a 4-row array `[A, A, B, A]` has run_ends
//! `[2, 3, 4]` and values `[A, B, A]`.
//!
//! Values encoding per `BqlType`:
//!
//! | `BqlType`    | Format                                    |
//! |--------------|-------------------------------------------|
//! | `Int`        | `[i64 LE; run_count]`                     |
//! | `Timestamp`  | `[i64 LE; run_count]`                     |
//! | `Bool`       | packed LSB-first bitmap, `⌈run_count/8⌉` bytes |
//! | `String`     | `[u32 LE length][utf8 bytes]` per run     |
//!
//! See `docs/design/storage/advanced-encodings.md` §3 for the go-decision
//! evidence, compression analysis, and selector guard specification.
//!
//! # Applicable types
//!
//! `Bool`, `Int`, `String`, `Timestamp`. `Float` and nested types are
//! excluded; [`Rle::applicable_to`] returns `false` for them.
//!
//! # Null handling
//!
//! RLE operates on dense arrays — the writer strips nulls into a separate
//! bitmap before encoding. A nullable input is a contract violation caught
//! by `super::require_dense`.
//!
//! # Edge cases
//!
//! - `row_count == 0`: legal. `encode` returns an empty chunk (params =
//!   `[0,0,0,0]`, payload = `[]`). `decode` returns an empty array. The
//!   selector uses Plain for empty arrays so RLE never receives them from
//!   the selector; this path is defensive.
//! - `run_count == row_count` (every value unique): RLE expands rather
//!   than compresses. The selector guard (`run_count < row_count / 2`)
//!   prevents this in practice (TASK-419).
//! - `run_count == 1`: entire column is a single run — maximum RLE
//!   compression for non-uniform data. (Uniform arrays go to `Constant`
//!   before reaching RLE.)
//!
//! # Selector guard
//!
//! Per `advanced-encodings.md` §3.7: only choose RLE when
//! `run_count < row_count / 2` (average run length > 2). This guard lives
//! in the selector (`TASK-419`), not here. [`Rle::estimate_size`] returns
//! the exact payload size unconditionally so the selector can compare
//! accurately.

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, Int64Array, StringArray, StringViewArray,
    StringViewBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the RLE encoding.
///
/// Stateless; freely clonable and stored behind a `Box<dyn Encoding>`.
/// See the module-level documentation for the byte layout this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rle;

impl Rle {
    /// Construct a new Rle encoder. Rle has no configuration — this is
    /// sugar for `Rle` the unit struct.
    pub const fn new() -> Self {
        Rle
    }

    /// Number of runs the current array would produce when RLE-encoded.
    ///
    /// Used by the encoding selector (TASK-419) to apply the §3.7 guard
    /// from `advanced-encodings.md` — "only choose RLE when estimated
    /// average run length > 2" — without paying for a full
    /// [`Self::estimate_size`] call, which additionally scans string
    /// bytes. Returns `0` for empty arrays.
    pub fn estimated_run_count(array: &dyn Array) -> Result<usize> {
        if array.is_empty() {
            return Ok(0);
        }
        match array.data_type() {
            DataType::Boolean => {
                let a = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "Rle::estimated_run_count: expected BooleanArray, downcast failed"
                                .into(),
                        )
                    })?;
                Ok(count_bool_runs(a))
            }
            DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => count_i64_runs(array),
            DataType::Utf8View => {
                let a = array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "Rle::estimated_run_count: expected StringViewArray, downcast failed"
                                .into(),
                        )
                    })?;
                Ok(count_string_view_runs(a).0)
            }
            DataType::Utf8 => {
                let a = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "Rle::estimated_run_count: expected StringArray, downcast failed"
                                .into(),
                        )
                    })?;
                Ok(count_string_utf8_runs(a).0)
            }
            other => Err(BqliteError::Execution(format!(
                "Rle::estimated_run_count: unsupported Arrow type {other:?}"
            ))),
        }
    }
}

/// Params block length: `run_count: u32 LE` (4 bytes).
const PARAMS_LEN: usize = 4;

impl Encoding for Rle {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Rle
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        // RLE covers Bool, Int, String, Timestamp — types where runs of
        // identical values are common (sorted entity_id, flag columns,
        // low-cardinality event types). Float is excluded: it has no v1
        // or Wave 4 Delta/BitPacking path that gains from RLE either.
        matches!(
            ty,
            BqlType::Bool | BqlType::Int | BqlType::String | BqlType::Timestamp
        )
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        // For every type the estimate is the exact payload size: the run
        // count (a linear scan) determines everything for fixed-width types,
        // and String requires a full scan anyway to sum the string bytes.
        estimate_payload_size(array)
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Rle")?;
        let row_count = array.len();
        let (run_ends, values_bytes) = collect_runs(array)?;
        let run_count = run_ends.len() as u32;

        // params: run_count as u32 LE
        let mut params = Vec::with_capacity(PARAMS_LEN);
        params.extend_from_slice(&run_count.to_le_bytes());
        debug_assert_eq!(params.len(), PARAMS_LEN);

        // payload: run_ends || values
        let mut payload = Vec::with_capacity(run_ends.len() * 4 + values_bytes.len());
        for end in &run_ends {
            payload.extend_from_slice(&end.to_le_bytes());
        }
        payload.extend_from_slice(&values_bytes);

        Ok(EncodedChunk {
            encoding: EncodingType::Rle,
            params,
            payload,
            row_count,
        })
    }

    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef> {
        decode_impl(
            chunk.encoding,
            &chunk.params,
            &chunk.payload,
            chunk.row_count,
            ty,
        )
    }

    fn decode_borrowed(&self, chunk: BorrowedEncodedChunk<'_>, ty: &BqlType) -> Result<ArrayRef> {
        decode_impl(
            chunk.encoding,
            chunk.params,
            chunk.payload,
            chunk.row_count,
            ty,
        )
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Number of bytes needed to pack `bit_count` bits LSB-first.
fn bitmap_byte_count(bit_count: usize) -> usize {
    bit_count.div_ceil(8)
}

// ── estimate_size implementation ─────────────────────────────────────────────

fn estimate_payload_size(array: &dyn Array) -> Result<usize> {
    if array.is_empty() {
        return Ok(0);
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle::estimate_size: expected BooleanArray, downcast failed".into(),
                    )
                })?;
            let run_count = count_bool_runs(a);
            // run_ends: run_count * 4 bytes
            // values:   ceil(run_count / 8) bytes (packed bitmap)
            Ok(run_count * 4 + bitmap_byte_count(run_count))
        }
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let run_count = count_i64_runs(array)?;
            // run_ends: run_count * 4 bytes
            // values:   run_count * 8 bytes (i64 LE per run)
            Ok(run_count * 4 + run_count * 8)
        }
        DataType::Utf8View => {
            let a = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle::estimate_size: expected StringViewArray, downcast failed".into(),
                    )
                })?;
            let (run_count, total_str_bytes) = count_string_view_runs(a);
            // run_ends:        run_count * 4 bytes
            // length prefixes: run_count * 4 bytes
            // string data:     total_str_bytes
            Ok(run_count * 4 + run_count * 4 + total_str_bytes)
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle::estimate_size: expected StringArray, downcast failed".into(),
                    )
                })?;
            let (run_count, total_str_bytes) = count_string_utf8_runs(a);
            Ok(run_count * 4 + run_count * 4 + total_str_bytes)
        }
        other => Err(BqliteError::Execution(format!(
            "Rle::estimate_size: unsupported Arrow type {other:?} — \
             Rle covers Bool, Int64, Timestamp(Nanosecond), and Utf8/Utf8View"
        ))),
    }
}

// ── run counters ──────────────────────────────────────────────────────────────

fn count_bool_runs(array: &BooleanArray) -> usize {
    if array.is_empty() {
        return 0;
    }
    let mut count = 1usize;
    let mut prev = array.value(0);
    for i in 1..array.len() {
        let v = array.value(i);
        if v != prev {
            count += 1;
            prev = v;
        }
    }
    count
}

fn count_i64_runs(array: &dyn Array) -> Result<usize> {
    if array.is_empty() {
        return Ok(0);
    }
    let values = extract_i64_slice(array)?;
    let mut count = 1usize;
    let mut prev = values[0];
    for &v in &values[1..] {
        if v != prev {
            count += 1;
            prev = v;
        }
    }
    Ok(count)
}

/// Returns `(run_count, total_unique_string_bytes)` for a `StringViewArray`.
fn count_string_view_runs(array: &StringViewArray) -> (usize, usize) {
    if array.is_empty() {
        return (0, 0);
    }
    let mut run_count = 1usize;
    let mut total_bytes = array.value(0).len();
    let mut prev = array.value(0);
    for i in 1..array.len() {
        let v = array.value(i);
        if v != prev {
            run_count += 1;
            total_bytes += v.len();
            prev = v;
        }
    }
    (run_count, total_bytes)
}

/// Returns `(run_count, total_unique_string_bytes)` for a `StringArray`.
fn count_string_utf8_runs(array: &StringArray) -> (usize, usize) {
    if array.is_empty() {
        return (0, 0);
    }
    let mut run_count = 1usize;
    let mut total_bytes = array.value(0).len();
    let mut prev = array.value(0);
    for i in 1..array.len() {
        let v = array.value(i);
        if v != prev {
            run_count += 1;
            total_bytes += v.len();
            prev = v;
        }
    }
    (run_count, total_bytes)
}

// ── run collection ────────────────────────────────────────────────────────────

/// Collect RLE runs from `array`.
///
/// Returns `(run_ends, values_bytes)` where:
/// - `run_ends[i]` is the cumulative row count at the end of run `i`
///   (last element equals `array.len()`).
/// - `values_bytes` is the per-run value payload in type-dependent format
///   (see module docs).
fn collect_runs(array: &dyn Array) -> Result<(Vec<u32>, Vec<u8>)> {
    match array.data_type() {
        DataType::Boolean => collect_bool_runs(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle::encode: expected BooleanArray, downcast failed".into(),
                    )
                })?,
        ),
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => collect_i64_runs(array),
        DataType::Utf8View => collect_string_view_runs_encoded(
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle::encode: expected StringViewArray, downcast failed".into(),
                    )
                })?,
        ),
        DataType::Utf8 => collect_string_utf8_runs_encoded(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle::encode: expected StringArray, downcast failed".into(),
                    )
                })?,
        ),
        other => Err(BqliteError::Execution(format!(
            "Rle::encode: unsupported Arrow type {other:?} — \
             Rle covers Bool, Int64, Timestamp(Nanosecond), and Utf8/Utf8View"
        ))),
    }
}

fn collect_bool_runs(array: &BooleanArray) -> Result<(Vec<u32>, Vec<u8>)> {
    if array.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut run_ends: Vec<u32> = Vec::new();
    let mut run_values: Vec<bool> = Vec::new();
    let mut prev = array.value(0);
    for i in 1..array.len() {
        let v = array.value(i);
        if v != prev {
            run_ends.push(i as u32);
            run_values.push(prev);
            prev = v;
        }
    }
    // Finalise the last run.
    run_ends.push(array.len() as u32);
    run_values.push(prev);

    // Pack bool run values as a LSB-first bitmap.
    let run_count = run_values.len();
    let mut bitmap = vec![0u8; bitmap_byte_count(run_count)];
    for (i, &v) in run_values.iter().enumerate() {
        if v {
            bitmap[i / 8] |= 1u8 << (i % 8);
        }
    }
    Ok((run_ends, bitmap))
}

fn collect_i64_runs(array: &dyn Array) -> Result<(Vec<u32>, Vec<u8>)> {
    if array.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let values = extract_i64_slice(array)?;
    let mut run_ends: Vec<u32> = Vec::new();
    let mut run_values: Vec<i64> = Vec::new();
    let mut prev = values[0];
    for (i, &v) in values.iter().enumerate().skip(1) {
        if v != prev {
            run_ends.push(i as u32);
            run_values.push(prev);
            prev = v;
        }
    }
    run_ends.push(values.len() as u32);
    run_values.push(prev);

    let mut values_bytes = Vec::with_capacity(run_values.len() * 8);
    for v in run_values {
        values_bytes.extend_from_slice(&v.to_le_bytes());
    }
    Ok((run_ends, values_bytes))
}

fn collect_string_view_runs_encoded(array: &StringViewArray) -> Result<(Vec<u32>, Vec<u8>)> {
    if array.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut run_ends: Vec<u32> = Vec::new();
    let mut values_bytes: Vec<u8> = Vec::new();
    let mut prev = array.value(0);
    for i in 1..array.len() {
        let v = array.value(i);
        if v != prev {
            run_ends.push(i as u32);
            append_length_prefixed_str(prev, &mut values_bytes);
            prev = v;
        }
    }
    // Finalise the last run.
    run_ends.push(array.len() as u32);
    append_length_prefixed_str(prev, &mut values_bytes);
    Ok((run_ends, values_bytes))
}

fn collect_string_utf8_runs_encoded(array: &StringArray) -> Result<(Vec<u32>, Vec<u8>)> {
    if array.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut run_ends: Vec<u32> = Vec::new();
    let mut values_bytes: Vec<u8> = Vec::new();
    let mut prev = array.value(0);
    for i in 1..array.len() {
        let v = array.value(i);
        if v != prev {
            run_ends.push(i as u32);
            append_length_prefixed_str(prev, &mut values_bytes);
            prev = v;
        }
    }
    run_ends.push(array.len() as u32);
    append_length_prefixed_str(prev, &mut values_bytes);
    Ok((run_ends, values_bytes))
}

/// Write a UTF-8 string as `[u32 LE length][utf8 bytes]` into `buf`.
#[inline]
fn append_length_prefixed_str(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

// ── decode implementation ─────────────────────────────────────────────────────

fn decode_impl(
    encoding: EncodingType,
    params: &[u8],
    payload: &[u8],
    row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    if encoding != EncodingType::Rle {
        return Err(BqliteError::Execution(format!(
            "Rle::decode called on a {:?} chunk — dispatch must route each \
             chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if params.len() != PARAMS_LEN {
        return Err(BqliteError::Execution(format!(
            "Rle::decode expects a {PARAMS_LEN}-byte params block \
             (run_count: u32 LE), got {} bytes",
            params.len()
        )));
    }
    let run_count = u32::from_le_bytes(params[..4].try_into().unwrap()) as usize;

    // Validate: at minimum we need run_count * 4 bytes for run_ends.
    let run_ends_bytes = run_count * 4;
    if payload.len() < run_ends_bytes {
        return Err(BqliteError::Execution(format!(
            "Rle::decode: payload too short for {run_count} run ends: \
             need {run_ends_bytes} bytes, got {}",
            payload.len()
        )));
    }

    // Parse cumulative run ends.
    let (run_ends_slice, values_slice) = payload.split_at(run_ends_bytes);
    let mut run_ends = Vec::with_capacity(run_count);
    for i in 0..run_count {
        let end =
            u32::from_le_bytes(run_ends_slice[i * 4..(i + 1) * 4].try_into().unwrap()) as usize;
        run_ends.push(end);
    }

    // Structural invariant: run_ends must be strictly increasing and the
    // last end must equal row_count.
    if run_count == 0 {
        if row_count != 0 {
            return Err(BqliteError::Execution(format!(
                "Rle::decode: run_count == 0 but row_count == {row_count} — \
                 segment corruption"
            )));
        }
        // Empty array — return type-appropriate empty output.
        return make_empty_array(ty);
    }
    // Validate monotonicity.
    for i in 1..run_ends.len() {
        if run_ends[i] <= run_ends[i - 1] {
            return Err(BqliteError::Execution(format!(
                "Rle::decode: run_ends are not strictly increasing at index {i} \
                 ({} <= {}) — segment corruption",
                run_ends[i],
                run_ends[i - 1]
            )));
        }
    }
    let last_end = run_ends[run_count - 1];
    if last_end != row_count {
        return Err(BqliteError::Execution(format!(
            "Rle::decode: last run_end {last_end} != row_count {row_count} — \
             segment corruption"
        )));
    }

    match ty {
        BqlType::Bool => decode_bool_runs(&run_ends, values_slice, row_count, run_count),
        BqlType::Int => decode_i64_runs(&run_ends, values_slice, row_count, run_count, false),
        BqlType::Timestamp => decode_i64_runs(&run_ends, values_slice, row_count, run_count, true),
        BqlType::String => decode_string_runs(&run_ends, values_slice, row_count, run_count),
        other => Err(BqliteError::Execution(format!(
            "Rle::decode does not support BqlType::{other} — \
             Rle covers Bool, Int, String, and Timestamp"
        ))),
    }
}

fn make_empty_array(ty: &BqlType) -> Result<ArrayRef> {
    match ty {
        BqlType::Bool => Ok(Arc::new(BooleanArray::from(Vec::<bool>::new())) as ArrayRef),
        BqlType::Int => Ok(Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef),
        BqlType::Timestamp => Ok(Arc::new(
            TimestampNanosecondArray::from(Vec::<i64>::new()).with_timezone("UTC"),
        ) as ArrayRef),
        BqlType::String => {
            let mut builder = StringViewBuilder::new();
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        other => Err(BqliteError::Execution(format!(
            "Rle::decode: make_empty_array: unsupported type {other}"
        ))),
    }
}

fn decode_bool_runs(
    run_ends: &[usize],
    values: &[u8],
    row_count: usize,
    run_count: usize,
) -> Result<ArrayRef> {
    let expected_bitmap_bytes = bitmap_byte_count(run_count);
    if values.len() != expected_bitmap_bytes {
        return Err(BqliteError::Execution(format!(
            "Rle::decode(Bool): expected {expected_bitmap_bytes} value bytes \
             for {run_count} runs, got {}",
            values.len()
        )));
    }
    let mut builder = BooleanBufferBuilder::new(row_count);
    let mut pos = 0usize;
    for (i, &end) in run_ends.iter().enumerate() {
        let bit = (values[i / 8] >> (i % 8)) & 1 == 1;
        let run_len = end - pos;
        for _ in 0..run_len {
            builder.append(bit);
        }
        pos = end;
    }
    let buffer = builder.finish();
    Ok(Arc::new(BooleanArray::new(buffer, None)) as ArrayRef)
}

fn decode_i64_runs(
    run_ends: &[usize],
    values: &[u8],
    row_count: usize,
    run_count: usize,
    as_timestamp: bool,
) -> Result<ArrayRef> {
    let expected_values_bytes = run_count * 8;
    if values.len() != expected_values_bytes {
        let type_name = if as_timestamp { "Timestamp" } else { "Int" };
        return Err(BqliteError::Execution(format!(
            "Rle::decode({type_name}): expected {expected_values_bytes} value \
             bytes for {run_count} runs, got {}",
            values.len()
        )));
    }
    let mut out = Vec::with_capacity(row_count);
    let mut pos = 0usize;
    for (i, &end) in run_ends.iter().enumerate() {
        let v = i64::from_le_bytes(values[i * 8..(i + 1) * 8].try_into().unwrap());
        let run_len = end - pos;
        for _ in 0..run_len {
            out.push(v);
        }
        pos = end;
    }
    if as_timestamp {
        Ok(Arc::new(TimestampNanosecondArray::from(out).with_timezone("UTC")) as ArrayRef)
    } else {
        Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
    }
}

fn decode_string_runs(
    run_ends: &[usize],
    values: &[u8],
    row_count: usize,
    run_count: usize,
) -> Result<ArrayRef> {
    // Phase 1: parse all run string values from the values slice.
    // Each entry is [u32 LE length][utf8 bytes].
    let mut run_strs: Vec<&str> = Vec::with_capacity(run_count);
    let mut cursor = 0usize;
    for run_idx in 0..run_count {
        if cursor + 4 > values.len() {
            return Err(BqliteError::Execution(format!(
                "Rle::decode(String): payload truncated reading length \
                 prefix for run {run_idx}"
            )));
        }
        let len = u32::from_le_bytes(values[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + len > values.len() {
            return Err(BqliteError::Execution(format!(
                "Rle::decode(String): payload truncated reading string \
                 of length {len} for run {run_idx}"
            )));
        }
        let s = std::str::from_utf8(&values[cursor..cursor + len]).map_err(|e| {
            BqliteError::Execution(format!(
                "Rle::decode(String): invalid UTF-8 in run {run_idx}: {e}"
            ))
        })?;
        run_strs.push(s);
        cursor += len;
    }
    if cursor != values.len() {
        return Err(BqliteError::Execution(format!(
            "Rle::decode(String): {} trailing bytes after all run values",
            values.len() - cursor
        )));
    }

    // Phase 2: broadcast each run value into the output StringViewArray.
    let mut builder = StringViewBuilder::with_capacity(row_count);
    let mut pos = 0usize;
    for (i, &end) in run_ends.iter().enumerate() {
        let s = run_strs[i];
        let run_len = end - pos;
        for _ in 0..run_len {
            builder.append_value(s);
        }
        pos = end;
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

// ── i64 value extraction ──────────────────────────────────────────────────────

/// Extract a slice view of the i64 backing buffer from an `Int64Array` or
/// `TimestampNanosecondArray`. Returns a slice reference, not a copy.
fn extract_i64_slice(array: &dyn Array) -> Result<arrow::buffer::ScalarBuffer<i64>> {
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("Rle: expected Int64Array, downcast failed".into())
            })?;
            Ok(a.values().clone())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Rle: expected TimestampNanosecondArray, downcast failed".into(),
                    )
                })?;
            Ok(a.values().clone())
        }
        other => Err(BqliteError::Execution(format!(
            "Rle: expected Int64 or Timestamp(Nanosecond), got {other:?}"
        ))),
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int64Array, StringViewArray, TimestampNanosecondArray};
    use std::sync::Arc;

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let rle = Rle;
        let chunk = rle.encode(array.as_ref()).unwrap();
        rle.decode(&chunk, &ty).unwrap()
    }

    // ── applicable_to ──

    #[test]
    fn applicable_to_covers_bool_int_string_timestamp() {
        let r = Rle;
        assert!(r.applicable_to(&BqlType::Bool));
        assert!(r.applicable_to(&BqlType::Int));
        assert!(r.applicable_to(&BqlType::String));
        assert!(r.applicable_to(&BqlType::Timestamp));
        assert!(!r.applicable_to(&BqlType::Float));
        assert!(!r.applicable_to(&BqlType::List(Box::new(BqlType::Int))));
    }

    #[test]
    fn encoding_type_is_rle_discriminant_five() {
        assert_eq!(Rle.encoding_type(), EncodingType::Rle);
        assert_eq!(Rle.encoding_type().discriminant(), 5);
    }

    // ── nullable input rejection ──

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = Rle.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── round-trip: Bool ──

    #[test]
    fn bool_round_trip_alternating() {
        // Alternating true/false — worst case for compression but must
        // still round-trip correctly.
        let values: Vec<bool> = (0..8).map(|i| i % 2 == 0).collect();
        let array: ArrayRef = Arc::new(BooleanArray::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_long_runs() {
        // Long true run followed by short false run.
        let mut values = vec![true; 100];
        values.extend(vec![false; 5]);
        let array: ArrayRef = Arc::new(BooleanArray::from(values));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_single_element_true() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_single_element_false() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![false]));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn bool_round_trip_empty() {
        let array: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
        let decoded = round_trip(array.clone(), BqlType::Bool);
        assert_eq!(decoded.len(), 0);
    }

    // ── round-trip: Int ──

    #[test]
    fn int_round_trip_long_entity_id_run() {
        // Simulates entity_id sorted column: ~655 rows per entity.
        let values: Vec<i64> = (0..100)
            .flat_map(|entity| std::iter::repeat_n(entity * 1000, 655))
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_single_element() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_all_same_value() {
        // All equal — RLE produces a single run. (Selector would normally
        // pick Constant before RLE for this case, but the encoding must
        // still work.)
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 100]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_all_unique() {
        // Every value unique — maximum expansion case. Must still round-trip.
        let values: Vec<i64> = (0..20).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_extrema() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            i64::MIN,
            i64::MIN,
            i64::MAX,
            i64::MAX,
            0,
        ]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_empty() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.len(), 0);
    }

    // ── round-trip: Timestamp ──

    #[test]
    fn timestamp_round_trip_sorted_runs() {
        // Monotonic timestamps within each entity (entity-sorted column).
        let base: i64 = 1_700_000_000_000_000_000;
        let values: Vec<i64> = (0..3)
            .flat_map(|entity| (0..10).map(move |t| base + entity * 1_000_000_000 + t * 1_000_000))
            .collect();
        // Duplicate each timestamp to create runs.
        let run_values: Vec<i64> = values.iter().flat_map(|&v| [v, v]).collect();
        let array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(run_values).with_timezone("UTC"));
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn timestamp_round_trip_preserves_utc_timezone() {
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000_i64,
                1_700_000_000_000_000_000,
                1_700_000_000_000_001_000,
            ])
            .with_timezone("UTC"),
        );
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── round-trip: String ──

    #[test]
    fn string_round_trip_sorted_entity_ids() {
        // Simulates entity_id column: long runs of repeated strings.
        let values: Vec<&str> = (0..5)
            .flat_map(|i| {
                let s: &'static str = ["alice", "bob", "charlie", "dave", "eve"][i];
                std::iter::repeat_n(s, 10)
            })
            .collect();
        let array: ArrayRef = Arc::new(StringViewArray::from(values));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn string_round_trip_empty_strings_in_runs() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["", "", "abc", "abc", "", ""]));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn string_round_trip_single_element() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["hello"]));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn string_round_trip_empty_array() {
        let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<&str>::new()));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.len(), 0);
    }

    // ── params / payload structure ──

    #[test]
    fn params_length_is_four_bytes() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 1, 2, 2, 3]));
        let chunk = Rle.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.params.len(), PARAMS_LEN);
    }

    #[test]
    fn run_count_in_params_matches_payload_structure() {
        // [1, 1, 2, 2, 3] has 3 runs.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 1, 2, 2, 3]));
        let chunk = Rle.encode(array.as_ref()).unwrap();
        let run_count = u32::from_le_bytes(chunk.params[..4].try_into().unwrap()) as usize;
        assert_eq!(run_count, 3);
        // run_ends: 3 * 4 = 12 bytes; values: 3 * 8 = 24 bytes
        assert_eq!(chunk.payload.len(), 12 + 24);
    }

    #[test]
    fn row_count_preserved_in_chunk() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 50]));
        let chunk = Rle.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.row_count, 50);
    }

    #[test]
    fn estimate_size_matches_encoded_payload_length_int() {
        let values: Vec<i64> = (0..20).map(|i| (i / 3) * 100).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let encoded = Rle.encode(array.as_ref()).unwrap();
        assert_eq!(
            estimated,
            encoded.payload.len(),
            "estimate_size must be exact for Int"
        );
    }

    #[test]
    fn estimate_size_matches_encoded_payload_length_bool() {
        let values: Vec<bool> = (0..32).map(|i| i % 4 < 2).collect();
        let array: ArrayRef = Arc::new(BooleanArray::from(values));
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let encoded = Rle.encode(array.as_ref()).unwrap();
        assert_eq!(
            estimated,
            encoded.payload.len(),
            "estimate_size must be exact for Bool"
        );
    }

    #[test]
    fn estimate_size_matches_encoded_payload_length_string() {
        let values = vec!["click", "click", "purchase", "view", "view", "view"];
        let array: ArrayRef = Arc::new(StringViewArray::from(values));
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let encoded = Rle.encode(array.as_ref()).unwrap();
        assert_eq!(
            estimated,
            encoded.payload.len(),
            "estimate_size must be exact for String"
        );
    }

    // ── decode error handling ──

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0u8; PARAMS_LEN],
            payload: vec![],
            row_count: 0,
        };
        let err = Rle.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(
                msg.contains("Plain"),
                "error should mention the wrong discriminant; got: {msg}"
            ),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_params_length_mismatch() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Rle,
            params: vec![0u8; PARAMS_LEN - 1],
            payload: vec![],
            row_count: 0,
        };
        let err = Rle.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(
                msg.contains("params block"),
                "error should mention params; got: {msg}"
            ),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_run_count_row_count_mismatch() {
        // Claim 1 run but row_count == 0 — corruption.
        let mut params = [0u8; PARAMS_LEN];
        params[..4].copy_from_slice(&1u32.to_le_bytes());
        // 1 run_end pointing to row 5
        let mut payload = vec![0u8; 4 + 8];
        payload[..4].copy_from_slice(&5u32.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::Rle,
            params: params.to_vec(),
            payload,
            row_count: 0,
        };
        let err = Rle.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(
                msg.contains("run_end") || msg.contains("row_count"),
                "error should mention the mismatch; got: {msg}"
            ),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── compression advantage sanity check ──

    #[test]
    fn rle_compresses_sorted_entity_id_column() {
        // Entity_id column: 100 entities × 655 rows/entity ≈ 65 500 rows.
        // RLE payload should be much smaller than the Plain payload.
        use crate::encoding::{plain::Plain, Encoding};
        let values: Vec<i64> = (0..100_i64)
            .flat_map(|entity| std::iter::repeat_n(entity, 655))
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let rle_chunk = Rle.encode(array.as_ref()).unwrap();
        let plain_chunk = Plain.encode(array.as_ref()).unwrap();
        assert!(
            rle_chunk.payload.len() < plain_chunk.payload.len() / 100,
            "RLE should achieve >100× compression on sorted entity_id; \
             rle={}, plain={}",
            rle_chunk.payload.len(),
            plain_chunk.payload.len()
        );
    }
}
