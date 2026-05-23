//! Dictionary encoding — per-column sorted distinct values + bit-packed codes.
//!
//! Dictionary maps distinct values to small integer codes, stores each
//! code at the minimum bit width needed to represent the cardinality,
//! and bit-packs the code stream into the payload. It is the workhorse
//! for low-cardinality string and integer columns (event types, enum
//! roles, country codes, …) where storing one ordinal per row is much
//! cheaper than storing the full value per row.
//!
//! See `docs/design/storage/segment-format-v1.md` §9.2 for the
//! authoritative byte-level layout the on-disk segment form uses.
//!
//! # Applicable types
//!
//! Wave 2 Dictionary encodes `Int` (`Int64Array`) and `String`
//! (`StringArray` / `StringViewArray`). `Bool`, `Float`, `Timestamp`,
//! and nested types are explicitly out of scope: `Bool` has no
//! compression benefit at two distinct values (Plain's bit-packing is
//! already optimal); `Float` has no canonical total order for dict
//! ordering under `PartialOrd`; `Timestamp` benefits more from Delta's
//! residual encoding for monotonic runs; nested types are deferred.
//!
//! # Self-contained `EncodedChunk` vs. segment-level layout
//!
//! The on-disk segment layout pinned by §9.2 stores each column's
//! dictionary once at the **segment** level (in the segment
//! dictionaries region, see §11) and the per-row-group column chunk
//! stores only `dict_id: u32 LE + code_bit_width: u8` params plus the
//! bit-packed code stream. That layout is what TASK-213 / TASK-214
//! write to disk.
//!
//! The [`Encoding`] trait, however, operates on **self-contained**
//! [`EncodedChunk`]s so that round-trip property tests hold without a
//! separate segment-level store and so that the writer orchestrator can
//! treat every encoding uniformly during row-group assembly. To honor
//! both constraints, the trait-level chunk produced by `Dictionary`
//! inlines the dictionary values into the `params` block. The segment
//! writer (TASK-213 / TASK-214) will parse each dictionary chunk's
//! params, hoist the dictionary values to the segment-level
//! dictionaries region, and rewrite the on-disk params to the
//! `dict_id + code_bit_width` shape §9.2 mandates. The reader (TASK-215)
//! performs the reverse hoist on open. This module is the
//! correctness-baseline form the writer / reader hoist around.
//!
//! # Params layout (self-contained form)
//!
//! ```text
//! [type_tag:        u8]          // 0 = Int, 1 = String
//! [code_bit_width:  u8]          // 1..=24; §10.3 caps cardinality at 2^24
//! [cardinality:     u32 LE]      // number of distinct values in dict_values
//! [dict_values:     variable]    // cardinality values in ascending order:
//!                                //   Int:    cardinality × i64 LE
//!                                //   String: cardinality × ([u32 LE length][utf8 bytes])
//! ```
//!
//! The dictionary is **sorted** ascending (numerically for `Int`,
//! byte-wise lexicographically for `String`) — this matches §9.2's
//! "codes are ordinal into the sorted dictionary" rule and makes the
//! equality-predicate rewrite path in TASK-202 / TASK-216 work
//! uniformly across encodings.
//!
//! # Payload layout
//!
//! `row_count` codes bit-packed at `code_bit_width` bits per code,
//! LSB-first within each byte, padded up to the next multiple of 8
//! bytes. The padding is identical in spirit to Delta's and
//! BitPacking's §9.3/§9.4 tail padding: a FastLanes-style SIMD decoder
//! can read one full 8-byte lane past the last code without a bounds
//! check. The padded byte count is what `uncompressed_payload_length`
//! reports on disk and what [`EncodedChunk::payload`] carries here.
//!
//! # Null handling
//!
//! Dictionary operates on dense arrays — the writer strips nulls into
//! a separate bitmap before handing values to the encoding layer. A
//! nullable input is a contract violation, caught by
//! `super::require_dense`.
//!
//! # Edge cases
//!
//! - `row_count == 0`: legal. Cardinality is 0, `code_bit_width` is
//!   the minimum (1), and both the dictionary and the payload are
//!   empty. The selector (TASK-212) may still prefer `Constant` or
//!   `Plain` for empty chunks; Dictionary never rejects them.
//! - `cardinality == 1`: legal. All codes are 0, `code_bit_width` is
//!   the minimum (1), and the payload bits are all zero.
//! - `cardinality > 2^24`: illegal. `encode` returns
//!   `BqliteError::Execution` so the selector falls back to another
//!   encoding (`Plain` is the universal fallback).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringViewArray, StringViewBuilder};
use arrow::datatypes::DataType;
use bqlite_core::{BqlType, BqliteError, Result};

use super::{require_dense, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the Dictionary encoding.
///
/// Stateless; freely clonable and stored behind a
/// `Box<dyn Encoding>`. See the module-level documentation for the
/// byte layout this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct Dictionary;

impl Dictionary {
    /// Construct a new Dictionary encoder. Dictionary has no
    /// configuration — this is sugar for `Dictionary` the unit struct.
    pub const fn new() -> Self {
        Dictionary
    }
}

/// Maximum cardinality — §9.2 caps `code_bit_width` at 24 bits, so the
/// dictionary can hold at most `2^24` distinct values per column chunk.
/// `storage-format.md` §10.3 references the same 16M ceiling.
const MAX_CARDINALITY: usize = 1 << 24;

/// Maximum legal code width in bits per §9.2.
const MAX_CODE_BIT_WIDTH: u8 = 24;

/// Params header size (the fixed prefix before the variable-length
/// dictionary values): `type_tag: u8 + code_bit_width: u8 + cardinality: u32 LE`.
const PARAMS_HEADER_LEN: usize = 6;

/// SIMD lane size — the code stream is padded up to a multiple of this
/// so a FastLanes-style bit-packed unpacker can read one full lane
/// past the last code without a bounds check. Same 8-byte padding
/// Delta and BitPacking emit (§9.3 / §9.4).
const SIMD_LANE_BYTES: usize = 8;

/// Width of the per-value length prefix in the string dictionary block.
/// Matches Plain's §9.1 string layout so readers can share parsing
/// helpers in a later refactor.
const STRING_LENGTH_PREFIX: usize = 4;

/// Type tag discriminant for the params block.
///
/// Stored as the first byte of `params`. Disambiguates the
/// variable-length dictionary block's encoding between `Int` (fixed
/// `i64 LE` stride) and `String` (length-prefixed UTF-8). The
/// `BqlType` the decoder is asked for must match this tag — a
/// mismatch is a typed error, not a silent mis-decode.
const TYPE_TAG_INT: u8 = 0;
const TYPE_TAG_STRING: u8 = 1;

impl Encoding for Dictionary {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Dictionary
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::Int | BqlType::String)
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        // The selector ranks encodings by payload bytes only (not the
        // params block — see the trait contract in `mod.rs`), so this
        // is the size of the bit-packed code stream only.
        //
        // To compute that exactly, we need the cardinality (to pick the
        // bit width), which means walking the array once. That matches
        // Delta's pattern — `Delta::estimate_size` also walks the array
        // to pick a residual width. For Wave 2's row-group size
        // (65,536 rows) the walk is cheap.
        let row_count = array.len();
        let cardinality = count_distinct(array)?;
        let width = code_bit_width_for(cardinality)?;
        Ok(payload_byte_count(row_count, width))
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Dictionary")?;
        let row_count = array.len();
        match array.data_type() {
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    BqliteError::Execution(
                        "Dictionary: expected Arrow Int64Array, downcast failed".into(),
                    )
                })?;
                let values = arr.values();
                encode_int(values, row_count)
            }
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "Dictionary: expected Arrow StringArray, downcast failed".into(),
                        )
                    })?;
                let values: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
                encode_string(&values)
            }
            DataType::Utf8View => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .ok_or_else(|| {
                        BqliteError::Execution(
                            "Dictionary: expected Arrow StringViewArray, downcast failed".into(),
                        )
                    })?;
                let values: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
                encode_string(&values)
            }
            other => Err(BqliteError::Execution(format!(
                "Dictionary::encode does not support Arrow type {other:?} — \
                 v1 Dictionary encoding covers Int64, Utf8, and Utf8View only"
            ))),
        }
    }

    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef> {
        if chunk.encoding != EncodingType::Dictionary {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode called on a {:?} chunk — dispatch must \
                 route each chunk to its declared encoding's decoder",
                chunk.encoding
            )));
        }
        if chunk.params.len() < PARAMS_HEADER_LEN {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: params block truncated — expected at \
                 least {PARAMS_HEADER_LEN} bytes for the header, got {}",
                chunk.params.len()
            )));
        }
        let type_tag = chunk.params[0];
        let code_bit_width = chunk.params[1];
        let cardinality =
            u32::from_le_bytes(chunk.params[2..PARAMS_HEADER_LEN].try_into().unwrap()) as usize;
        let dict_bytes = &chunk.params[PARAMS_HEADER_LEN..];

        if cardinality > MAX_CARDINALITY {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: cardinality {cardinality} exceeds the \
                 v1 maximum of {MAX_CARDINALITY} — segment corruption"
            )));
        }
        // A non-empty code stream over an empty dictionary is corruption
        // by construction — every code would be out-of-bounds. Fail here
        // with a specific message rather than surfacing the first OOB
        // lookup as a generic "code out of bounds" error.
        if chunk.row_count > 0 && cardinality == 0 {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: row_count {} with empty dictionary — \
                 segment corruption",
                chunk.row_count
            )));
        }

        // Validate the on-disk type tag matches the caller-requested
        // logical type. Catches writer/reader schema drift or a
        // corrupt chunk before we try to parse the dictionary under
        // the wrong layout.
        match (type_tag, ty) {
            (TYPE_TAG_INT, BqlType::Int) | (TYPE_TAG_STRING, BqlType::String) => {}
            (tag, other) => {
                return Err(BqliteError::Execution(format!(
                    "Dictionary::decode: type tag {tag} does not match \
                     requested BqlType::{other} — v1 tags are \
                     {TYPE_TAG_INT} (Int) and {TYPE_TAG_STRING} (String)"
                )));
            }
        }

        let expected_code_bytes = payload_byte_count(chunk.row_count, code_bit_width);
        if chunk.payload.len() != expected_code_bytes {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: expected {expected_code_bytes} code-stream \
                 bytes for {} rows at {code_bit_width}-bit codes, got {}",
                chunk.row_count,
                chunk.payload.len()
            )));
        }

        let codes = unpack_codes(&chunk.payload, chunk.row_count, code_bit_width)?;

        match ty {
            BqlType::Int => {
                let dict = parse_int_dict(dict_bytes, cardinality)?;
                let mut values = Vec::with_capacity(chunk.row_count);
                for code in codes {
                    let idx = code as usize;
                    if idx >= cardinality {
                        return Err(BqliteError::Execution(format!(
                            "Dictionary::decode: code {code} out of bounds \
                             for dictionary of size {cardinality}"
                        )));
                    }
                    values.push(dict[idx]);
                }
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            }
            BqlType::String => {
                let dict = parse_string_dict(dict_bytes, cardinality)?;
                let mut builder = StringViewBuilder::with_capacity(chunk.row_count);
                for code in codes {
                    let idx = code as usize;
                    if idx >= cardinality {
                        return Err(BqliteError::Execution(format!(
                            "Dictionary::decode: code {code} out of bounds \
                             for dictionary of size {cardinality}"
                        )));
                    }
                    builder.append_value(dict[idx]);
                }
                // Produce the schema-canonical StringViewArray, matching
                // Plain::decode(String) and bqlite_core::arrow::bql_type_to_arrow.
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            other => Err(BqliteError::Execution(format!(
                "Dictionary::decode does not support BqlType::{other} — v1 \
                 Dictionary encoding covers Int and String only"
            ))),
        }
    }
}

// ── encode helpers ───────────────────────────────────────────────────────────

fn encode_int(values: &[i64], row_count: usize) -> Result<EncodedChunk> {
    // Build sorted distinct dictionary. `sort_unstable` + `dedup` is
    // allocation-light and O(n log n); for the typical row-group size
    // (65,536 rows) this runs in under a millisecond.
    let mut distinct: Vec<i64> = values.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    let cardinality = distinct.len();
    if cardinality > MAX_CARDINALITY {
        return Err(BqliteError::Execution(format!(
            "Dictionary::encode: cardinality {cardinality} exceeds the v1 \
             maximum of {MAX_CARDINALITY} distinct values \
             (segment-format-v1.md §9.2)"
        )));
    }
    let code_bit_width = code_bit_width_for(cardinality)?;

    // Binary-search each value in the sorted dict to produce its code.
    // O(n log k); faster than building a HashMap for small k.
    let mut codes: Vec<u32> = Vec::with_capacity(row_count);
    for v in values {
        let code = distinct
            .binary_search(v)
            .expect("value must be present in dict built from the same slice");
        codes.push(code as u32);
    }

    let mut params = Vec::with_capacity(PARAMS_HEADER_LEN + 8 * cardinality);
    params.push(TYPE_TAG_INT);
    params.push(code_bit_width);
    params.extend_from_slice(&(cardinality as u32).to_le_bytes());
    for v in &distinct {
        params.extend_from_slice(&v.to_le_bytes());
    }

    let payload = pack_codes(&codes, code_bit_width, row_count)?;
    Ok(EncodedChunk {
        encoding: EncodingType::Dictionary,
        params,
        payload,
        row_count,
    })
}

fn encode_string(values: &[&str]) -> Result<EncodedChunk> {
    let row_count = values.len();
    // Build sorted distinct dictionary (byte-wise lex order, which is
    // the canonical total order for Rust `str` and matches the UTF-8
    // collation readers expect).
    let mut distinct: Vec<&str> = values.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    let cardinality = distinct.len();
    if cardinality > MAX_CARDINALITY {
        return Err(BqliteError::Execution(format!(
            "Dictionary::encode: cardinality {cardinality} exceeds the v1 \
             maximum of {MAX_CARDINALITY} distinct values \
             (segment-format-v1.md §9.2)"
        )));
    }
    let code_bit_width = code_bit_width_for(cardinality)?;

    let mut codes: Vec<u32> = Vec::with_capacity(row_count);
    for v in values {
        let code = distinct
            .binary_search(v)
            .expect("value must be present in dict built from the same slice");
        codes.push(code as u32);
    }

    // Precompute params block size: header + per-value (length prefix
    // + UTF-8 bytes). An exact capacity avoids reallocation during
    // the copy loop.
    let dict_bytes: usize = distinct
        .iter()
        .map(|s| STRING_LENGTH_PREFIX + s.len())
        .sum();
    let mut params = Vec::with_capacity(PARAMS_HEADER_LEN + dict_bytes);
    params.push(TYPE_TAG_STRING);
    params.push(code_bit_width);
    params.extend_from_slice(&(cardinality as u32).to_le_bytes());
    for (idx, s) in distinct.iter().enumerate() {
        if s.len() > u32::MAX as usize {
            return Err(BqliteError::Execution(format!(
                "Dictionary::encode: dict value at index {idx} is {} bytes, \
                 exceeding the v1 per-value cap of u32::MAX ({} bytes)",
                s.len(),
                u32::MAX
            )));
        }
        params.extend_from_slice(&(s.len() as u32).to_le_bytes());
        params.extend_from_slice(s.as_bytes());
    }

    let payload = pack_codes(&codes, code_bit_width, row_count)?;
    Ok(EncodedChunk {
        encoding: EncodingType::Dictionary,
        params,
        payload,
        row_count,
    })
}

// ── distinct-value counting (for estimate_size) ──────────────────────────────

pub(crate) fn count_distinct(array: &dyn Array) -> Result<usize> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("Dictionary: downcast Int64Array failed".into())
            })?;
            let mut v: Vec<i64> = arr.values().to_vec();
            v.sort_unstable();
            v.dedup();
            Ok(v.len())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Dictionary: downcast StringArray failed".into())
                })?;
            let mut v: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
            v.sort_unstable();
            v.dedup();
            Ok(v.len())
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution("Dictionary: downcast StringViewArray failed".into())
                })?;
            let mut v: Vec<&str> = (0..arr.len()).map(|i| arr.value(i)).collect();
            v.sort_unstable();
            v.dedup();
            Ok(v.len())
        }
        other => Err(BqliteError::Execution(format!(
            "Dictionary::estimate_size does not support Arrow type {other:?} — \
             v1 Dictionary encoding covers Int64, Utf8, and Utf8View only"
        ))),
    }
}

// ── bit width selection ──────────────────────────────────────────────────────

/// Pick the smallest `code_bit_width` that can represent every ordinal
/// in `0..cardinality`.
///
/// Floors at 1 (width 0 is not a legal v1 wire value per §9.2 `1..=24`).
/// An empty dictionary or a single-entry dictionary therefore encodes
/// at width 1 with an all-zero payload. Errors if `cardinality`
/// exceeds [`MAX_CARDINALITY`] — the selector must then fall back to
/// Plain.
fn code_bit_width_for(cardinality: usize) -> Result<u8> {
    if cardinality > MAX_CARDINALITY {
        return Err(BqliteError::Execution(format!(
            "Dictionary: cardinality {cardinality} exceeds the v1 \
             maximum of {MAX_CARDINALITY} distinct values"
        )));
    }
    if cardinality <= 1 {
        return Ok(1);
    }
    // Max code is `cardinality - 1`; need `ceil(log2(cardinality))` bits.
    let max_code = (cardinality - 1) as u32;
    let bits = 32 - max_code.leading_zeros();
    debug_assert!(bits as u8 <= MAX_CODE_BIT_WIDTH);
    Ok(bits as u8)
}

/// Exact payload byte count for a bit-packed code stream: the smallest
/// multiple of `SIMD_LANE_BYTES` that holds `row_count` codes at
/// `bit_width` bits per code.
pub fn payload_byte_count(row_count: usize, bit_width: u8) -> usize {
    let unpadded_bits = (bit_width as usize) * row_count;
    let unpadded_bytes = unpadded_bits.div_ceil(8);
    unpadded_bytes.div_ceil(SIMD_LANE_BYTES) * SIMD_LANE_BYTES
}

// ── bit packing / unpacking ──────────────────────────────────────────────────

/// Pack `codes` into a byte buffer at `bit_width` bits per code,
/// LSB-first within each byte. Pads up to the next [`SIMD_LANE_BYTES`]
/// multiple so callers can unpack with a SIMD routine that reads one
/// full lane past the last code.
pub(crate) fn pack_codes(codes: &[u32], bit_width: u8, row_count: usize) -> Result<Vec<u8>> {
    if !(1..=MAX_CODE_BIT_WIDTH).contains(&bit_width) {
        return Err(BqliteError::Execution(format!(
            "Dictionary::encode: code_bit_width {bit_width} out of valid range 1..={MAX_CODE_BIT_WIDTH}"
        )));
    }
    debug_assert_eq!(codes.len(), row_count);
    let expected_bytes = payload_byte_count(row_count, bit_width);
    let mut out = vec![0u8; expected_bytes];
    // bit_width is in 1..=24 so `1u64 << bit_width` never overflows.
    let mask: u64 = (1u64 << bit_width) - 1;
    let mut bit_pos: usize = 0;
    for &code in codes {
        let mut value = (code as u64) & mask;
        let mut remaining = bit_width as usize;
        let mut byte_idx = bit_pos / 8;
        let mut bit_in_byte = bit_pos % 8;
        while remaining > 0 {
            let free_in_byte = 8 - bit_in_byte;
            let take = remaining.min(free_in_byte);
            let chunk = (value & ((1u64 << take) - 1)) as u8;
            out[byte_idx] |= chunk << bit_in_byte;
            value >>= take;
            remaining -= take;
            bit_in_byte += take;
            if bit_in_byte == 8 {
                bit_in_byte = 0;
                byte_idx += 1;
            }
        }
        bit_pos += bit_width as usize;
    }
    debug_assert!(bit_pos <= expected_bytes * 8);
    Ok(out)
}

/// Inverse of `pack_codes`: reconstruct a `Vec<u32>` of length
/// `row_count` from a bit-packed code stream.
pub fn unpack_codes(payload: &[u8], row_count: usize, bit_width: u8) -> Result<Vec<u32>> {
    if !(1..=MAX_CODE_BIT_WIDTH).contains(&bit_width) {
        return Err(BqliteError::Execution(format!(
            "Dictionary::decode: code_bit_width {bit_width} out of valid range 1..={MAX_CODE_BIT_WIDTH}"
        )));
    }
    let mut codes = Vec::with_capacity(row_count);
    let mask: u64 = (1u64 << bit_width) - 1;
    let mut bit_pos: usize = 0;
    for _ in 0..row_count {
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        let mut remaining = bit_width as usize;
        let mut byte_idx = bit_pos / 8;
        let mut bit_in_byte = bit_pos % 8;
        while remaining > 0 {
            let free_in_byte = 8 - bit_in_byte;
            let take = remaining.min(free_in_byte);
            let byte = payload[byte_idx];
            // `take` reaches 8 on aligned reads; compute the mask in
            // u32 to sidestep the `1u8 << 8` overflow panic in debug.
            let take_mask = ((1u32 << take) - 1) as u8;
            let chunk = ((byte >> bit_in_byte) & take_mask) as u64;
            value |= chunk << shift;
            shift += take as u32;
            remaining -= take;
            bit_in_byte += take;
            if bit_in_byte == 8 {
                bit_in_byte = 0;
                byte_idx += 1;
            }
        }
        bit_pos += bit_width as usize;
        codes.push((value & mask) as u32);
    }
    Ok(codes)
}

/// Specialised unpacker for `bit_width ≤ 8`: returns codes as `Vec<u8>`
/// directly, avoiding the 4× memory overhead and per-element widen +
/// narrow round-trip the generic [`unpack_codes`] requires when the
/// caller wants a `u8` keys buffer (e.g. the `Dict<UInt8>` push-through
/// materialiser).
///
/// At `bit_width = 8`, this is effectively a memcpy of the leading
/// `row_count` bytes from `payload`. At `bit_width < 8`, each code
/// spans at most two adjacent payload bytes, so the inner loop is
/// branchless after the per-row offset calculation.
pub fn unpack_codes_u8(payload: &[u8], row_count: usize, bit_width: u8) -> Result<Vec<u8>> {
    if !(1..=8).contains(&bit_width) {
        return Err(BqliteError::Execution(format!(
            "Dictionary::decode: unpack_codes_u8 requires bit_width in 1..=8, got {bit_width}"
        )));
    }
    let mut codes = vec![0u8; row_count];
    if row_count == 0 {
        return Ok(codes);
    }
    // bit_width = 8 → aligned copy.
    if bit_width == 8 {
        if payload.len() < row_count {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: payload {} bytes too short for {row_count} 8-bit codes",
                payload.len()
            )));
        }
        codes.copy_from_slice(&payload[..row_count]);
        return Ok(codes);
    }
    // bit_width < 8 → each code spans at most 2 adjacent bytes.
    let width = bit_width as usize;
    let mask: u8 = (1u8 << width) - 1;
    for (i, slot) in codes.iter_mut().enumerate() {
        let bit_pos = i * width;
        let byte_idx = bit_pos / 8;
        let bit_in_byte = bit_pos % 8;
        let low = payload[byte_idx] >> bit_in_byte;
        let value = if bit_in_byte + width <= 8 {
            low & mask
        } else {
            // Spans into the next byte.
            let hi_bits = (bit_in_byte + width) - 8;
            let hi = payload[byte_idx + 1] & ((1u8 << hi_bits) - 1);
            (low | (hi << (8 - bit_in_byte))) & mask
        };
        *slot = value;
    }
    Ok(codes)
}

// ── dictionary-value parsing (decode side) ───────────────────────────────────

fn parse_int_dict(bytes: &[u8], cardinality: usize) -> Result<Vec<i64>> {
    let expected = 8 * cardinality;
    if bytes.len() != expected {
        return Err(BqliteError::Execution(format!(
            "Dictionary::decode: expected {expected} dict bytes for \
             {cardinality} Int values, got {}",
            bytes.len()
        )));
    }
    let mut values = Vec::with_capacity(cardinality);
    for chunk in bytes.chunks_exact(8) {
        values.push(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
    // Invariant: dict is sorted ascending. A writer that honors the
    // encode path always produces a sorted dict; a corrupt chunk may
    // not. Validating here catches that without a second pass by the
    // caller — O(n) single scan.
    for window in values.windows(2) {
        if window[0] > window[1] {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: Int dict is not sorted ascending \
                 (found {} > {}) — segment corruption",
                window[0], window[1]
            )));
        }
    }
    Ok(values)
}

fn parse_string_dict<'a>(bytes: &'a [u8], cardinality: usize) -> Result<Vec<&'a str>> {
    let mut values: Vec<&'a str> = Vec::with_capacity(cardinality);
    let mut offset = 0usize;
    for i in 0..cardinality {
        if offset + STRING_LENGTH_PREFIX > bytes.len() {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: dict truncated at value {i}/{cardinality}: \
                 missing length prefix at offset {offset}"
            )));
        }
        let len = u32::from_le_bytes(
            bytes[offset..offset + STRING_LENGTH_PREFIX]
                .try_into()
                .unwrap(),
        ) as usize;
        offset += STRING_LENGTH_PREFIX;
        if offset + len > bytes.len() {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: dict truncated at value {i}/{cardinality}: \
                 missing {len}-byte payload at offset {offset}"
            )));
        }
        let value_bytes = &bytes[offset..offset + len];
        offset += len;
        let s = std::str::from_utf8(value_bytes).map_err(|e| {
            BqliteError::Execution(format!(
                "Dictionary::decode: invalid UTF-8 at dict value \
                 {i}/{cardinality}: {e}"
            ))
        })?;
        values.push(s);
    }
    if offset != bytes.len() {
        return Err(BqliteError::Execution(format!(
            "Dictionary::decode: {} trailing bytes after reading {cardinality} \
             String dict values",
            bytes.len() - offset
        )));
    }
    // Sorted-ascending invariant, same as the Int path.
    for window in values.windows(2) {
        if window[0] > window[1] {
            return Err(BqliteError::Execution(format!(
                "Dictionary::decode: String dict is not sorted ascending \
                 (found {:?} > {:?}) — segment corruption",
                window[0], window[1]
            )));
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, StringViewArray};

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let dict = Dictionary;
        let chunk = dict.encode(array.as_ref()).unwrap();
        dict.decode(&chunk, &ty).unwrap()
    }

    // ── applicable_to / discriminant ──

    #[test]
    fn applicable_to_covers_int_and_string_only() {
        let d = Dictionary;
        assert!(d.applicable_to(&BqlType::Int));
        assert!(d.applicable_to(&BqlType::String));
        assert!(!d.applicable_to(&BqlType::Bool));
        assert!(!d.applicable_to(&BqlType::Float));
        assert!(!d.applicable_to(&BqlType::Timestamp));
        assert!(!d.applicable_to(&BqlType::List(Box::new(BqlType::Int))));
    }

    #[test]
    fn encoding_type_is_dictionary_discriminant_one() {
        assert_eq!(Dictionary.encoding_type(), EncodingType::Dictionary);
        assert_eq!(Dictionary.encoding_type().discriminant(), 1);
    }

    // ── round trips ──

    #[test]
    fn int_round_trip_low_cardinality() {
        // 5 values, 3 distinct — the canonical Dictionary workload.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 3, 5, 1, 3]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_single_value() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64, 42, 42]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_all_distinct() {
        // cardinality == row_count: dict stores every value; codes
        // form a permutation of 0..row_count.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 20, 5, 30, 15]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_empty_array() {
        // Empty is legal for Dictionary: cardinality 0, width 1, zero
        // bytes of code stream.
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.row_count, 0);
        assert_eq!(chunk.payload.len(), 0);
        let decoded = Dictionary.decode(&chunk, &BqlType::Int).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn int_round_trip_negative_and_extrema() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            i64::MIN,
            -1,
            0,
            1,
            i64::MAX,
            -1,
            i64::MIN,
        ]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn string_round_trip_accepts_utf8_input() {
        let values = vec![
            "click".to_string(),
            "view".to_string(),
            "click".to_string(),
            "purchase".to_string(),
            "click".to_string(),
        ];
        let array: ArrayRef = Arc::new(StringArray::from(values.clone()));
        let decoded = round_trip(array, BqlType::String);
        // Decode always produces StringViewArray; compare values.
        let view = decoded
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Dictionary::decode(String) must produce StringViewArray");
        let decoded_values: Vec<String> =
            (0..view.len()).map(|i| view.value(i).to_string()).collect();
        assert_eq!(decoded_values, values);
    }

    #[test]
    fn string_round_trip_accepts_utf8_view_input() {
        // StringViewArray is the schema-canonical type; feeding it
        // through encode/decode round-trips identically under Arrow's
        // PartialEq.
        let values = vec![
            "view".to_string(),
            String::new(),
            "héllo 🌍".to_string(),
            "view".to_string(),
        ];
        let array: ArrayRef = Arc::new(StringViewArray::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn string_round_trip_empty_array() {
        let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<String>::new()));
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.row_count, 0);
        assert_eq!(chunk.payload.len(), 0);
        let decoded = Dictionary.decode(&chunk, &BqlType::String).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn string_round_trip_empty_string_value() {
        // The empty string is a legal distinct value. Two empty strings
        // at indices 0 and 2 must round-trip to the same code.
        let values = vec![
            String::new(),
            "a".to_string(),
            String::new(),
            "b".to_string(),
        ];
        let array: ArrayRef = Arc::new(StringViewArray::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::String);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── dictionary shape invariants (inspecting chunk.params) ──

    #[test]
    fn int_dict_is_sorted_ascending_in_params() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![5_i64, 3, 5, 1, 3, 10, 1]));
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.params[0], TYPE_TAG_INT);
        let cardinality = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap()) as usize;
        let dict = parse_int_dict(&chunk.params[PARAMS_HEADER_LEN..], cardinality).unwrap();
        assert_eq!(dict, vec![1, 3, 5, 10]);
    }

    #[test]
    fn string_dict_is_sorted_ascending_in_params() {
        let array: ArrayRef =
            Arc::new(StringViewArray::from(vec!["click", "view", "click", "buy"]));
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.params[0], TYPE_TAG_STRING);
        let cardinality = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap()) as usize;
        let dict = parse_string_dict(&chunk.params[PARAMS_HEADER_LEN..], cardinality).unwrap();
        assert_eq!(dict, vec!["buy", "click", "view"]);
    }

    #[test]
    fn code_bit_width_scales_with_cardinality() {
        // cardinality 0/1 → width 1 (floor)
        assert_eq!(code_bit_width_for(0).unwrap(), 1);
        assert_eq!(code_bit_width_for(1).unwrap(), 1);
        // cardinality 2 → 1 bit (max code 1)
        assert_eq!(code_bit_width_for(2).unwrap(), 1);
        // cardinality 3..=4 → 2 bits
        assert_eq!(code_bit_width_for(3).unwrap(), 2);
        assert_eq!(code_bit_width_for(4).unwrap(), 2);
        // cardinality 5..=8 → 3 bits
        assert_eq!(code_bit_width_for(5).unwrap(), 3);
        assert_eq!(code_bit_width_for(8).unwrap(), 3);
        // cardinality 9..=16 → 4 bits
        assert_eq!(code_bit_width_for(9).unwrap(), 4);
        assert_eq!(code_bit_width_for(16).unwrap(), 4);
        // At the max: 2^24 distinct → 24 bits
        assert_eq!(code_bit_width_for(1 << 24).unwrap(), 24);
    }

    #[test]
    fn code_bit_width_rejects_over_cap() {
        let err = code_bit_width_for((1 << 24) + 1).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("exceeds")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── estimate_size ──

    #[test]
    fn estimate_size_matches_encoded_payload_length_int() {
        // Code stream length is what `estimate_size` returns — not the
        // params block size.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 1, 2, 3, 1]));
        let estimated = Dictionary.estimate_size(array.as_ref()).unwrap();
        let encoded = Dictionary.encode(array.as_ref()).unwrap();
        assert_eq!(estimated, encoded.payload.len());
    }

    #[test]
    fn estimate_size_matches_encoded_payload_length_string() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "a", "c", "a", "b"]));
        let estimated = Dictionary.estimate_size(array.as_ref()).unwrap();
        let encoded = Dictionary.encode(array.as_ref()).unwrap();
        assert_eq!(estimated, encoded.payload.len());
    }

    #[test]
    fn estimate_size_handles_empty_array() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let estimated = Dictionary.estimate_size(array.as_ref()).unwrap();
        assert_eq!(estimated, 0);
    }

    // ── payload shape ──

    #[test]
    fn payload_is_padded_to_simd_lane_multiple() {
        let cases: &[Vec<i64>] = &[
            vec![0],
            vec![0, 0, 0],
            vec![1, 2, 3, 4, 5, 6, 7],
            (0..17).collect(),
            (0..32).collect(),
        ];
        for values in cases {
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let chunk = Dictionary.encode(array.as_ref()).unwrap();
            assert_eq!(
                chunk.payload.len() % SIMD_LANE_BYTES,
                0,
                "payload {} not a multiple of {SIMD_LANE_BYTES} for values {values:?}",
                chunk.payload.len()
            );
        }
    }

    // ── encode error paths ──

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = Dictionary.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_unsupported_type() {
        use arrow::array::Float64Array;
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let err = Dictionary.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("Int64, Utf8, and Utf8View"),
                    "unexpected: {msg}"
                )
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── decode error paths ──

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![TYPE_TAG_INT, 1, 0, 0, 0, 0],
            payload: vec![],
            row_count: 0,
        };
        let err = Dictionary.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_params() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params: vec![0u8; PARAMS_HEADER_LEN - 1],
            payload: vec![],
            row_count: 0,
        };
        let err = Dictionary.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("params block truncated")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_type_tag_mismatch() {
        // Encode an Int chunk then ask for a String decode — tag
        // validation must catch it.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let chunk = Dictionary.encode(array.as_ref()).unwrap();
        let err = Dictionary.decode(&chunk, &BqlType::String).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("type tag")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_payload_length_mismatch() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 1, 2]));
        let mut chunk = Dictionary.encode(array.as_ref()).unwrap();
        // Truncate one byte off the SIMD-aligned payload.
        chunk.payload.pop();
        let err = Dictionary.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("code-stream")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_oob_code() {
        // Craft a chunk whose code stream references an index beyond
        // the dictionary. Dict cardinality = 2 (width 1), but we flip
        // all payload bits to 1 — codes become 1 which is the max
        // valid code for cardinality 2, so this test instead uses
        // cardinality 1 with a forced bit-1 code.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64, 42, 42]));
        let mut chunk = Dictionary.encode(array.as_ref()).unwrap();
        // Dictionary has 1 entry, width 1; setting bit 0 of the first
        // payload byte produces code 1 which is OOB.
        chunk.payload[0] |= 1;
        let err = Dictionary.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("out of bounds")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_unsorted_int_dict() {
        // Hand-craft a chunk whose dict is unsorted. Two entries
        // [5, 1], width 1. Payload is one code 0 (→ value 5), padded
        // to 8 bytes.
        let mut params = vec![TYPE_TAG_INT, 1];
        params.extend_from_slice(&2u32.to_le_bytes());
        params.extend_from_slice(&5i64.to_le_bytes());
        params.extend_from_slice(&1i64.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params,
            payload: vec![0u8; SIMD_LANE_BYTES],
            row_count: 1,
        };
        let err = Dictionary.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("not sorted")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_rows_over_empty_dict() {
        // Hand-craft a corrupt chunk: cardinality 0 but row_count 3.
        // The up-front check must fire with a specific message rather
        // than falling through to the generic "code out of bounds"
        // error at first lookup.
        let mut params = vec![TYPE_TAG_INT, 1];
        params.extend_from_slice(&0u32.to_le_bytes()); // cardinality = 0
        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params,
            payload: vec![0u8; SIMD_LANE_BYTES],
            row_count: 3,
        };
        let err = Dictionary.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("empty dictionary")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── bit pack / unpack helpers ──

    #[test]
    fn pack_unpack_round_trip_all_widths() {
        for width in 1u8..=24 {
            let max_code = if width == 32 {
                u32::MAX
            } else {
                (1u32 << width) - 1
            };
            let codes: Vec<u32> = (0..=max_code.min(255))
                .chain([max_code])
                .collect::<Vec<_>>()
                .into_iter()
                .take(10)
                .collect();
            let row_count = codes.len();
            let packed = pack_codes(&codes, width, row_count).unwrap();
            assert_eq!(packed.len() % SIMD_LANE_BYTES, 0);
            let unpacked = unpack_codes(&packed, row_count, width).unwrap();
            assert_eq!(unpacked, codes, "round trip failed at width {width}");
        }
    }

    #[test]
    fn unpack_codes_u8_matches_unpack_codes_for_widths_one_through_eight() {
        // Cross-check the u8-specialised unpacker against the generic
        // `unpack_codes` for every bit width it covers, over a value
        // pattern that exercises both the in-byte and byte-spanning
        // paths.
        for width in 1u8..=8 {
            let max_code = (1u32 << width) - 1;
            let mut codes: Vec<u32> = (0..=max_code.min(255)).collect();
            // Add a few wrapping patterns to stress byte boundaries.
            for _ in 0..3 {
                codes.extend([0u32, max_code, max_code / 2 + 1, 1, 0]);
            }
            let row_count = codes.len();
            let packed = pack_codes(&codes, width, row_count).unwrap();
            let unpacked_u32 = unpack_codes(&packed, row_count, width).unwrap();
            let unpacked_u8 = unpack_codes_u8(&packed, row_count, width).unwrap();
            assert_eq!(unpacked_u8.len(), row_count);
            for (i, (&a, &b)) in unpacked_u32.iter().zip(unpacked_u8.iter()).enumerate() {
                assert_eq!(
                    a, b as u32,
                    "mismatch at width {width}, row {i}: u32={a} u8={b}"
                );
            }
        }
    }

    #[test]
    fn unpack_codes_u8_rejects_widths_outside_one_through_eight() {
        // The specialised path is only valid for `bit_width ∈ 1..=8`;
        // wider codes don't fit a byte, narrower than 1 isn't a legal
        // dict width per §10.3.
        let payload = vec![0u8; 8];
        assert!(unpack_codes_u8(&payload, 1, 0).is_err());
        assert!(unpack_codes_u8(&payload, 1, 9).is_err());
        assert!(unpack_codes_u8(&payload, 1, 24).is_err());
    }

    #[test]
    fn unpack_codes_u8_handles_empty_row_count() {
        // Empty input is legal for any in-range bit width.
        for width in 1u8..=8 {
            let out = unpack_codes_u8(&[], 0, width).unwrap();
            assert!(out.is_empty(), "empty row_count should yield empty Vec");
        }
    }

    #[test]
    fn pack_codes_rejects_invalid_width() {
        let err = pack_codes(&[0, 1], 0, 2).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("out of valid range")),
            other => panic!("expected Execution error, got {other:?}"),
        }
        let err = pack_codes(&[0, 1], 25, 2).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("out of valid range")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }
}
