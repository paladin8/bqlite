//! ALP (Adaptive Lossless floating-Point) encoding.
//!
//! ALP exploits the observation that many real-world float columns
//! contain "round" values (prices, percentages, scores) that can be
//! losslessly represented as `mantissa × 10^exponent` where the
//! mantissa is a small integer.  ALP finds per-chunk `(exponent,
//! factor)` pairs such that `round(value × factor) / factor == value`
//! for most values.  The integer mantissas are then encoded with
//! per-block FOR (Frame-of-Reference) + BitPacking, achieving
//! near-integer compression ratios.
//!
//! Values that don't decompose cleanly ("exceptions") are stored in a
//! separate patch list at full f64 precision.
//!
//! The byte layout is pinned by `segment-format-v2.md` §5.6:
//!
//! ```text
//! encoding_params (19 bytes):
//!     exponent:        u8        // 0..=18
//!     factor:          f64 LE    // 10^exponent
//!     patch_count:     u32 LE
//!     for_block_size:  u16 LE    // 128
//!     for_block_count: u32 LE
//!
//! payload:
//!     1. Mantissa stream — FOR-encoded i64 array
//!        For each FOR block (for_block_count blocks):
//!            block_min:  i64 LE
//!            bit_width:  u8
//!            packed offsets (padded to 8-byte boundary)
//!     2. Patch indices — [u32 LE; patch_count]
//!     3. Patch values  — [f64 LE; patch_count]
//! ```
//!
//! Reference: Afroozeh & Leis, "ALP: Adaptive Lossless floating-Point
//! Compression," SIGMOD 2023.
//!
//! # Implementation notes
//!
//! The mantissa stream uses FOR encoding internally.  Since the
//! standalone FOR encoding (TASK-415) may not yet be merged, this
//! module includes self-contained FOR encode/decode routines for i64
//! mantissa arrays.  These operate on fixed 128-value blocks matching
//! `BitPacker4x::BLOCK_LEN`.

use arrow::array::{Array, ArrayRef, Float64Array};
use bitpacking::{BitPacker, BitPacker4x};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the ALP encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct Alp;

impl Alp {
    /// Construct a new ALP encoder.
    pub const fn new() -> Self {
        Alp
    }
}

// ── Constants ──────────────────────────────────────────────────────────────

/// FOR block size for the mantissa stream.  Matches
/// `BitPacker4x::BLOCK_LEN` and `segment-format-v2.md` §5.6.
const FOR_BLOCK_SIZE: usize = 128;

/// Total encoding-params size in bytes (§5.6):
/// `exponent(1) + factor(8) + patch_count(4) + for_block_size(2) +
/// for_block_count(4) = 19`.
const PARAMS_SIZE: usize = 19;

/// Maximum decimal exponent to try.  The ALP paper uses 0–18; 10^18
/// is exactly representable in f64 (fits within the 53-bit
/// significand).
const MAX_EXPONENT: u8 = 18;

/// 8-byte padding granularity for FOR-block packed offsets, matching
/// the v2 segment format's requirement for the FOR encoding.
const PADDING_GRANULARITY: usize = 8;

/// `BitPacker4x` maximum bit width (u32 values only).
const BITPACKER4X_MAX_BIT_WIDTH: u8 = 32;

/// Precomputed powers of 10 as f64, from 10^0 through 10^18.
const FACTORS: [f64; 19] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];

// ── ALP decomposition ──────────────────────────────────────────────────────

/// Try to decompose a single f64 value at the given `(factor,
/// inv_factor)` pair.
///
/// Returns `Some(mantissa)` when the value losslessly round-trips
/// through the integer mantissa domain, `None` otherwise.  The
/// round-trip check uses the same arithmetic as the decode path
/// (`(mantissa as f64) * inv_factor`) so the property
/// `decode(encode(v)) == v` is guaranteed for every non-exception
/// value.
#[inline]
fn try_decompose(value: f64, factor: f64, inv_factor: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    let m_f64 = (value * factor).round();
    // Mantissa must be finite and fit in i64.
    if !m_f64.is_finite() || m_f64 < i64::MIN as f64 || m_f64 > i64::MAX as f64 {
        return None;
    }
    let m = m_f64 as i64;
    // Strict IEEE 754 equality, matching the decode path exactly.
    // Note: we use `* inv_factor` (not `/ factor`) because the decode
    // path computes `mantissa * (1.0 / factor)`.  Division and
    // multiplication by the reciprocal can differ by a ULP, so the
    // encode check must use the same operation as decode.
    if (m as f64) * inv_factor == value {
        Some(m)
    } else {
        None
    }
}

/// Select the best exponent for a slice of f64 values.
///
/// Tries every exponent in 0..=18 and returns the one that maximises
/// the number of decomposable values.  A full scan is used (no
/// sampling) because row groups are bounded at 65 536 rows and the
/// per-value check is a handful of FP ops.
fn select_best_exponent(values: &[f64]) -> u8 {
    if values.is_empty() {
        return 0;
    }

    let mut best_exp = 0u8;
    let mut best_count = 0usize;

    for exp in 0..=MAX_EXPONENT {
        let factor = FACTORS[exp as usize];
        let inv_factor = 1.0 / factor;
        let count = values
            .iter()
            .filter(|&&v| try_decompose(v, factor, inv_factor).is_some())
            .count();
        if count > best_count {
            best_count = count;
            best_exp = exp;
        }
    }

    best_exp
}

/// Decompose all values at the chosen exponent.
///
/// Returns `(mantissas, patch_indices, patch_values)` where
/// `mantissas` is a dense array of integer mantissas for
/// decomposable values (exceptions removed) and the patch vectors
/// hold the positions and raw f64 values of exceptions.
fn decompose(values: &[f64], exponent: u8) -> (Vec<i64>, Vec<u32>, Vec<f64>) {
    let factor = FACTORS[exponent as usize];
    let inv_factor = 1.0 / factor;

    let mut mantissas = Vec::with_capacity(values.len());
    let mut patch_indices = Vec::new();
    let mut patch_values = Vec::new();

    for (i, &value) in values.iter().enumerate() {
        match try_decompose(value, factor, inv_factor) {
            Some(m) => mantissas.push(m),
            None => {
                patch_indices.push(i as u32);
                patch_values.push(value);
            }
        }
    }

    (mantissas, patch_indices, patch_values)
}

// ── FOR encoding (internal to ALP) ─────────────────────────────────────────

/// Compute `(block_min, bit_width)` for a block of i64 values.
fn for_block_frame(values: &[i64]) -> (i64, u8) {
    debug_assert!(!values.is_empty());
    let mut min = values[0];
    let mut max = values[0];
    for &v in &values[1..] {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    let max_offset = ((max as i128) - (min as i128)) as u64;
    let bit_width = if max_offset == 0 {
        1
    } else {
        (64 - max_offset.leading_zeros()) as u8
    };
    (min, bit_width)
}

/// Padded byte count for `count` values at `bit_width` bits each,
/// rounded up to the next multiple of [`PADDING_GRANULARITY`].
fn padded_packed_bytes(count: usize, bit_width: u8) -> usize {
    if count == 0 {
        return 0;
    }
    let total_bits = count * bit_width as usize;
    let raw_bytes = total_bits.div_ceil(8);
    raw_bytes.div_ceil(PADDING_GRANULARITY) * PADDING_GRANULARITY
}

/// FOR-encode an i64 mantissa array.  Returns `(block_count, payload)`.
fn for_encode(mantissas: &[i64]) -> (u32, Vec<u8>) {
    if mantissas.is_empty() {
        return (0, Vec::new());
    }

    let block_count = mantissas.len().div_ceil(FOR_BLOCK_SIZE);
    // Upper bound: 9 bytes header + up to 128*64/8 = 1024 bytes per block.
    let mut payload = Vec::with_capacity(block_count * (9 + FOR_BLOCK_SIZE * 8));
    let bitpacker = BitPacker4x::new();

    for block in mantissas.chunks(FOR_BLOCK_SIZE) {
        let (block_min, bit_width) = for_block_frame(block);
        payload.extend_from_slice(&block_min.to_le_bytes());
        payload.push(bit_width);

        let packed_size = padded_packed_bytes(block.len(), bit_width);
        let packed_start = payload.len();
        payload.resize(packed_start + packed_size, 0);

        if block.len() == FOR_BLOCK_SIZE && bit_width <= BITPACKER4X_MAX_BIT_WIDTH {
            // Fast path: full block with narrow offsets → BitPacker4x.
            let mut u32_block = [0u32; FOR_BLOCK_SIZE];
            for (slot, &value) in u32_block.iter_mut().zip(block) {
                let offset = ((value as i128) - (block_min as i128)) as u64;
                *slot = offset as u32;
            }
            bitpacker.compress(&u32_block, &mut payload[packed_start..], bit_width);
        } else {
            // Scalar fallback: partial block or wide offsets (>32 bits).
            for (j, &value) in block.iter().enumerate() {
                let offset = ((value as i128) - (block_min as i128)) as u64;
                write_bits(
                    &mut payload[packed_start..],
                    j * bit_width as usize,
                    bit_width as usize,
                    offset,
                );
            }
        }
    }

    (block_count as u32, payload)
}

/// FOR-decode a mantissa payload.  Returns `(mantissas,
/// bytes_consumed)` where `bytes_consumed` is the total number of
/// payload bytes read (so the caller can locate the patch sections
/// that follow).
fn for_decode(
    payload: &[u8],
    block_count: u32,
    mantissa_count: usize,
) -> Result<(Vec<i64>, usize)> {
    if mantissa_count == 0 {
        return Ok((Vec::new(), 0));
    }

    let mut mantissas = Vec::with_capacity(mantissa_count);
    let bitpacker = BitPacker4x::new();
    let mut cursor = 0usize;
    let mut remaining = mantissa_count;

    for _ in 0..block_count {
        if cursor + 9 > payload.len() {
            return Err(BqliteError::Execution(
                "Alp FOR decode: payload truncated at block header".to_string(),
            ));
        }

        let block_min = i64::from_le_bytes(payload[cursor..cursor + 8].try_into().unwrap());
        let bit_width = payload[cursor + 8];
        cursor += 9;

        if bit_width == 0 || bit_width > 64 {
            return Err(BqliteError::Execution(format!(
                "Alp FOR decode: invalid bit_width {bit_width} (must be 1..=64)"
            )));
        }

        let block_len = remaining.min(FOR_BLOCK_SIZE);
        let packed_size = padded_packed_bytes(block_len, bit_width);

        if cursor + packed_size > payload.len() {
            return Err(BqliteError::Execution(
                "Alp FOR decode: payload truncated at packed offsets".to_string(),
            ));
        }

        let packed = &payload[cursor..cursor + packed_size];

        if block_len == FOR_BLOCK_SIZE && bit_width <= BITPACKER4X_MAX_BIT_WIDTH {
            // Fast path: full block, narrow offsets.
            let mut u32_block = [0u32; FOR_BLOCK_SIZE];
            bitpacker.decompress(packed, &mut u32_block, bit_width);
            for &offset in &u32_block {
                mantissas.push(((block_min as i128) + (offset as i128)) as i64);
            }
        } else {
            // Scalar fallback.
            for j in 0..block_len {
                let offset = read_bits(packed, j * bit_width as usize, bit_width as usize);
                mantissas.push(((block_min as i128) + (offset as i128)) as i64);
            }
        }

        cursor += packed_size;
        remaining -= block_len;
    }

    Ok((mantissas, cursor))
}

// ── Bit manipulation ───────────────────────────────────────────────────────
//
// Scalar bit-packing routines used by the FOR encode/decode paths for
// partial blocks and wide (>32-bit) offsets.  These are functionally
// identical to the routines in `bitpacking.rs` — duplicated here to
// avoid cross-module coupling while the standalone FOR encoding
// (TASK-415) is in flight.

/// Write `width` bits of `value` at bit offset `start` into `out`,
/// LSB-first within each byte.
fn write_bits(out: &mut [u8], start: usize, width: usize, value: u64) {
    let mut bits_written = 0usize;
    while bits_written < width {
        let byte_idx = (start + bits_written) / 8;
        let bit_in_byte = (start + bits_written) % 8;
        let chunk_bits = (width - bits_written).min(8 - bit_in_byte);
        let chunk_mask: u8 = if chunk_bits == 8 {
            u8::MAX
        } else {
            (1u8 << chunk_bits) - 1
        };
        let chunk = ((value >> bits_written) as u8) & chunk_mask;
        out[byte_idx] |= chunk << bit_in_byte;
        bits_written += chunk_bits;
    }
}

/// Read `width` bits at bit offset `start` from `bytes`, LSB-first
/// within each byte.
fn read_bits(bytes: &[u8], start: usize, width: usize) -> u64 {
    let mut value = 0u64;
    let mut bits_read = 0usize;
    while bits_read < width {
        let byte_idx = (start + bits_read) / 8;
        let bit_in_byte = (start + bits_read) % 8;
        let chunk_bits = (width - bits_read).min(8 - bit_in_byte);
        let chunk_mask: u8 = if chunk_bits == 8 {
            u8::MAX
        } else {
            (1u8 << chunk_bits) - 1
        };
        let chunk = (bytes[byte_idx] >> bit_in_byte) & chunk_mask;
        value |= (chunk as u64) << bits_read;
        bits_read += chunk_bits;
    }
    value
}

// ── f64 accessor ───────────────────────────────────────────────────────────

/// Extract an `&[f64]` view over a `Float64Array`.
fn values_as_f64(array: &dyn Array) -> Result<&[f64]> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.values().as_ref())
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "Alp encoding: expected Float64Array, got {:?}",
                array.data_type()
            ))
        })
}

// ── Encoding trait ─────────────────────────────────────────────────────────

impl Encoding for Alp {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Alp
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::Float)
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let values = values_as_f64(array)?;
        if values.is_empty() {
            return Ok(0);
        }

        let exponent = select_best_exponent(values);
        let factor = FACTORS[exponent as usize];
        let inv_factor = 1.0 / factor;

        // Count decomposable values and track global mantissa range
        // for the bit-width upper bound.
        let mut mantissa_count = 0usize;
        let mut patch_count = 0usize;
        let mut mantissa_min = i64::MAX;
        let mut mantissa_max = i64::MIN;

        for &v in values {
            match try_decompose(v, factor, inv_factor) {
                Some(m) => {
                    mantissa_count += 1;
                    mantissa_min = mantissa_min.min(m);
                    mantissa_max = mantissa_max.max(m);
                }
                None => {
                    patch_count += 1;
                }
            }
        }

        // Upper-bound FOR payload: global bit width >= any per-block width.
        let global_bit_width = if mantissa_count <= 1 {
            1u8
        } else {
            let range = ((mantissa_max as i128) - (mantissa_min as i128)) as u64;
            if range == 0 {
                1
            } else {
                (64 - range.leading_zeros()) as u8
            }
        };

        let block_count = mantissa_count.div_ceil(FOR_BLOCK_SIZE);
        let max_packed_per_block = padded_packed_bytes(FOR_BLOCK_SIZE, global_bit_width);
        let for_est = block_count * (9 + max_packed_per_block);
        let patch_est = patch_count * 12; // 4-byte index + 8-byte value

        Ok(for_est + patch_est)
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Alp")?;
        let values = values_as_f64(array)?;
        let row_count = array.len();

        if row_count == 0 {
            let mut params = Vec::with_capacity(PARAMS_SIZE);
            params.push(0u8); // exponent
            params.extend_from_slice(&1.0f64.to_le_bytes()); // factor
            params.extend_from_slice(&0u32.to_le_bytes()); // patch_count
            params.extend_from_slice(&(FOR_BLOCK_SIZE as u16).to_le_bytes());
            params.extend_from_slice(&0u32.to_le_bytes()); // for_block_count
            return Ok(EncodedChunk {
                encoding: EncodingType::Alp,
                params,
                payload: Vec::new(),
                row_count: 0,
            });
        }

        let exponent = select_best_exponent(values);
        let factor = FACTORS[exponent as usize];
        let (mantissas, patch_indices, patch_values) = decompose(values, exponent);
        let patch_count = patch_indices.len();

        // FOR-encode mantissa stream.
        let (for_block_count, for_payload) = for_encode(&mantissas);

        // Build params (19 bytes fixed).
        let mut params = Vec::with_capacity(PARAMS_SIZE);
        params.push(exponent);
        params.extend_from_slice(&factor.to_le_bytes());
        params.extend_from_slice(&(patch_count as u32).to_le_bytes());
        params.extend_from_slice(&(FOR_BLOCK_SIZE as u16).to_le_bytes());
        params.extend_from_slice(&for_block_count.to_le_bytes());
        debug_assert_eq!(params.len(), PARAMS_SIZE);

        // Build payload: FOR stream ‖ patch indices ‖ patch values.
        let mut payload = Vec::with_capacity(for_payload.len() + patch_count * 4 + patch_count * 8);
        payload.extend_from_slice(&for_payload);
        for &idx in &patch_indices {
            payload.extend_from_slice(&idx.to_le_bytes());
        }
        for &val in &patch_values {
            payload.extend_from_slice(&val.to_le_bytes());
        }

        Ok(EncodedChunk {
            encoding: EncodingType::Alp,
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

fn decode_impl(
    encoding: EncodingType,
    params: &[u8],
    payload: &[u8],
    row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    if encoding != EncodingType::Alp {
        return Err(BqliteError::Execution(format!(
            "Alp::decode called on a {encoding:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder"
        )));
    }
    if !matches!(ty, BqlType::Float) {
        return Err(BqliteError::Execution(format!(
            "Alp::decode does not support type {ty} — ALP decodes to Float only \
             (segment-format-v2.md §5.6)"
        )));
    }
    if params.len() != PARAMS_SIZE {
        return Err(BqliteError::Execution(format!(
            "Alp::decode: encoding params must be exactly {PARAMS_SIZE} bytes, got {}",
            params.len()
        )));
    }

    if row_count == 0 {
        return Ok(Arc::new(Float64Array::from(Vec::<f64>::new())) as ArrayRef);
    }

    // Parse params.
    let exponent = params[0];
    if exponent > MAX_EXPONENT {
        return Err(BqliteError::Execution(format!(
            "Alp::decode: exponent {exponent} exceeds maximum {MAX_EXPONENT}"
        )));
    }
    let factor = f64::from_le_bytes(params[1..9].try_into().unwrap());
    let patch_count = u32::from_le_bytes(params[9..13].try_into().unwrap()) as usize;
    let for_block_size = u16::from_le_bytes(params[13..15].try_into().unwrap());
    let for_block_count = u32::from_le_bytes(params[15..19].try_into().unwrap());

    if for_block_size as usize != FOR_BLOCK_SIZE {
        return Err(BqliteError::Execution(format!(
            "Alp::decode: for_block_size must be {FOR_BLOCK_SIZE}, got {for_block_size}"
        )));
    }

    let mantissa_count = row_count.checked_sub(patch_count).ok_or_else(|| {
        BqliteError::Execution(format!(
            "Alp::decode: patch_count ({patch_count}) exceeds row_count ({row_count})"
        ))
    })?;

    // 1. FOR-decode mantissa stream.
    let (mantissas, for_bytes_consumed) = for_decode(payload, for_block_count, mantissa_count)?;

    // 2. Parse patch indices.
    let patch_indices_start = for_bytes_consumed;
    let patch_indices_end = patch_indices_start + patch_count * 4;
    if patch_indices_end > payload.len() {
        return Err(BqliteError::Execution(
            "Alp::decode: payload truncated at patch indices".to_string(),
        ));
    }
    let patch_indices: Vec<u32> = payload[patch_indices_start..patch_indices_end]
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .collect();

    // Patch indices must be strictly ascending and < row_count.  The
    // streaming reconstruction loop below scans forward through
    // `i = 0..row_count` and consumes one patch per matching `i`; any
    // violation walks past the mantissa stream (panic in release
    // builds with bounds checks elided is undefined behavior on the
    // surrounding slice).  Treat the chunk as corrupt instead.
    let mut last: Option<u32> = None;
    for &idx in &patch_indices {
        if (idx as usize) >= row_count {
            return Err(BqliteError::Corruption(format!(
                "Alp::decode: patch index {idx} out of range for row_count {row_count}"
            )));
        }
        if let Some(prev) = last {
            if idx <= prev {
                return Err(BqliteError::Corruption(format!(
                    "Alp::decode: patch indices must be strictly ascending, found {prev} followed by {idx}"
                )));
            }
        }
        last = Some(idx);
    }

    // 3. Parse patch values.
    let patch_values_end = patch_indices_end + patch_count * 8;
    if patch_values_end > payload.len() {
        return Err(BqliteError::Execution(
            "Alp::decode: payload truncated at patch values".to_string(),
        ));
    }
    let patch_values: Vec<f64> = payload[patch_indices_end..patch_values_end]
        .chunks_exact(8)
        .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
        .collect();

    // 4. Reconstruct the output array by interleaving mantissa-derived
    //    values and exceptions.
    let inv_factor = 1.0 / factor;
    let mut output = Vec::with_capacity(row_count);
    let mut m_idx = 0usize;
    let mut p_idx = 0usize;

    for i in 0..row_count {
        if p_idx < patch_count && patch_indices[p_idx] == i as u32 {
            output.push(patch_values[p_idx]);
            p_idx += 1;
        } else {
            output.push((mantissas[m_idx] as f64) * inv_factor);
            m_idx += 1;
        }
    }

    Ok(Arc::new(Float64Array::from(output)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(values: &[f64]) -> ArrayRef {
        let array: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        let chunk = Alp.encode(array.as_ref()).unwrap();
        Alp.decode(&chunk, &BqlType::Float).unwrap()
    }

    fn assert_f64_eq(decoded: &ArrayRef, expected: &[f64]) {
        let arr = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.len(), expected.len());
        for (i, &exp) in expected.iter().enumerate() {
            let got = arr.value(i);
            assert!(
                exp == got || (exp.is_nan() && got.is_nan()),
                "mismatch at index {i}: expected {exp}, got {got}"
            );
        }
    }

    // ── applicability ──────────────────────────────────────────────

    #[test]
    fn applicable_to_float_only() {
        let a = Alp;
        assert!(a.applicable_to(&BqlType::Float));
        assert!(!a.applicable_to(&BqlType::Int));
        assert!(!a.applicable_to(&BqlType::Bool));
        assert!(!a.applicable_to(&BqlType::String));
        assert!(!a.applicable_to(&BqlType::Timestamp));
    }

    #[test]
    fn encoding_type_is_alp_discriminant_ten() {
        assert_eq!(Alp.encoding_type(), EncodingType::Alp);
        assert_eq!(Alp.encoding_type().discriminant(), 10);
    }

    // ── round-trip: round floats ───────────────────────────────────

    #[test]
    fn round_trip_prices_two_decimal_places() {
        let values: Vec<f64> = (0..200).map(|i| 9.99 + i as f64 * 0.01).collect();
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    #[test]
    fn round_trip_percentages() {
        let values: Vec<f64> = (0..=100).map(|i| i as f64 * 0.01).collect();
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    #[test]
    fn round_trip_integers() {
        let values: Vec<f64> = (0..256).map(|i| i as f64).collect();
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    // ── round-trip: all-exception columns ──────────────────────────

    #[test]
    fn round_trip_random_floats() {
        // Values that don't decompose cleanly at any exponent.
        let values = vec![
            std::f64::consts::PI,
            std::f64::consts::E,
            std::f64::consts::SQRT_2,
            1.0 / 3.0,
            1.0 / 7.0,
        ];
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    // ── round-trip: edge cases ─────────────────────────────────────

    #[test]
    fn round_trip_empty() {
        let decoded = round_trip(&[]);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn round_trip_single_value() {
        let decoded = round_trip(&[42.0]);
        assert_f64_eq(&decoded, &[42.0]);
    }

    #[test]
    fn round_trip_nan_inf() {
        let values = vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.0, 2.0];
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    #[test]
    fn round_trip_nan_preserves_payload() {
        // A quiet NaN stored as exception must preserve its exact bit
        // pattern through the patch list.
        let nan = f64::NAN;
        let values = vec![nan];
        let decoded = round_trip(&values);
        let arr = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0).to_bits(), nan.to_bits());
    }

    #[test]
    fn round_trip_subnormals() {
        // Subnormals typically become exceptions.
        let values = vec![f64::MIN_POSITIVE / 2.0, f64::MIN_POSITIVE / 4.0, 1.0];
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    #[test]
    fn round_trip_positive_and_negative_zero() {
        // ±0.0 both decompose cleanly (0 * factor == 0.0).  Under
        // IEEE 754 equality -0.0 == 0.0, so the round-trip check
        // passes for both.  The decoded value may be +0.0 regardless
        // of the input sign — this is per spec (§5.6 edge cases).
        let values = vec![0.0, -0.0, 1.0];
        let decoded = round_trip(&values);
        let arr = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 0.0);
        assert_eq!(arr.value(1), 0.0); // -0.0 == 0.0 under IEEE 754
        assert_eq!(arr.value(2), 1.0);
    }

    // ── mixed decomposable + exception ─────────────────────────────

    #[test]
    fn round_trip_mixed() {
        let values = vec![
            1.0,
            2.5,
            std::f64::consts::PI, // exception
            3.75,
            f64::NAN, // exception
            100.0,
        ];
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    // ── all identical values ───────────────────────────────────────

    #[test]
    fn round_trip_all_identical() {
        let values = vec![42.0; 200];
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    // ── large array crossing multiple FOR blocks ───────────────────

    #[test]
    fn round_trip_multi_block() {
        // >128 values to exercise the FOR multi-block path.
        let values: Vec<f64> = (0..300).map(|i| i as f64 * 0.01).collect();
        let decoded = round_trip(&values);
        assert_f64_eq(&decoded, &values);
    }

    // ── exponent selection ─────────────────────────────────────────

    #[test]
    fn select_exponent_for_two_decimal_prices() {
        let values: Vec<f64> = (0..100).map(|i| 9.99 + i as f64 * 0.01).collect();
        let exp = select_best_exponent(&values);
        // Prices with 2 decimal places should select exponent 2.
        assert_eq!(exp, 2);
    }

    #[test]
    fn select_exponent_for_integers() {
        let values: Vec<f64> = (0..100).map(|i| i as f64).collect();
        let exp = select_best_exponent(&values);
        assert_eq!(exp, 0);
    }

    // ── estimate_size ──────────────────────────────────────────────

    #[test]
    fn estimate_size_upper_bounds_actual_payload() {
        for values in [
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
            vec![9.99, 19.99, 29.99, 39.99],
            (0..200).map(|i| i as f64 * 0.01).collect(),
            vec![std::f64::consts::PI; 10],
        ] {
            let array: ArrayRef = Arc::new(Float64Array::from(values.clone()));
            let estimated = Alp.estimate_size(array.as_ref()).unwrap();
            let actual = Alp.encode(array.as_ref()).unwrap().payload.len();
            assert!(
                estimated >= actual,
                "estimate ({estimated}) < actual ({actual}) for values: {values:?}"
            );
        }
    }

    // ── decode error handling ──────────────────────────────────────

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0u8; PARAMS_SIZE],
            payload: Vec::new(),
            row_count: 0,
        };
        let err = Alp.decode(&chunk, &BqlType::Float).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_non_float_type() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Alp,
            params: vec![0u8; PARAMS_SIZE],
            payload: Vec::new(),
            row_count: 0,
        };
        let err = Alp.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Float")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_malformed_params_length() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Alp,
            params: vec![0u8; 10], // too short
            payload: Vec::new(),
            row_count: 0,
        };
        let err = Alp.decode(&chunk, &BqlType::Float).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("19 bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_exponent_out_of_range() {
        let mut params = vec![0u8; PARAMS_SIZE];
        params[0] = 19; // exponent > MAX_EXPONENT
        let chunk = EncodedChunk {
            encoding: EncodingType::Alp,
            params,
            payload: Vec::new(),
            row_count: 1,
        };
        let err = Alp.decode(&chunk, &BqlType::Float).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("exceeds maximum")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)]));
        let err = Alp.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    /// Build an all-exception ALP chunk (mantissa_count == 0, FOR
    /// stream empty) with caller-supplied patch indices.  Used by the
    /// patch-index validation tests below to forge corrupted payloads
    /// without going through the encoder.
    fn build_alp_chunk_with_indices(patch_indices: &[u32], row_count: usize) -> EncodedChunk {
        let patch_count = patch_indices.len();
        let mut params = Vec::with_capacity(PARAMS_SIZE);
        params.push(0u8); // exponent
        params.extend_from_slice(&1.0f64.to_le_bytes()); // factor
        params.extend_from_slice(&(patch_count as u32).to_le_bytes());
        params.extend_from_slice(&(FOR_BLOCK_SIZE as u16).to_le_bytes());
        params.extend_from_slice(&0u32.to_le_bytes()); // for_block_count = 0

        let mut payload = Vec::with_capacity(patch_count * 12);
        for &idx in patch_indices {
            payload.extend_from_slice(&idx.to_le_bytes());
        }
        for _ in 0..patch_count {
            payload.extend_from_slice(&0.0f64.to_le_bytes());
        }

        EncodedChunk {
            encoding: EncodingType::Alp,
            params,
            payload,
            row_count,
        }
    }

    #[test]
    fn decode_rejects_out_of_range_patch_index() {
        // patch_indices[1] = 99, but row_count = 2 — index points
        // outside the output array.  Without validation the decoder
        // walks past the end of the (empty) mantissa stream.
        let chunk = build_alp_chunk_with_indices(&[0, 99], 2);
        let err = Alp.decode(&chunk, &BqlType::Float).unwrap_err();
        match err {
            BqliteError::Corruption(msg) => {
                assert!(
                    msg.contains("patch") && msg.contains("99"),
                    "expected message to mention patch index 99, got: {msg}"
                );
            }
            other => panic!("expected Corruption error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_duplicate_patch_indices() {
        // Two patches claiming the same row.  The decoder would
        // double-consume that slot and underflow the mantissa stream
        // for the remaining row.
        let chunk = build_alp_chunk_with_indices(&[0, 0], 2);
        let err = Alp.decode(&chunk, &BqlType::Float).unwrap_err();
        match err {
            BqliteError::Corruption(msg) => {
                assert!(
                    msg.contains("patch"),
                    "expected message to mention patch indices, got: {msg}"
                );
            }
            other => panic!("expected Corruption error, got {other:?}"),
        }
    }

    #[test]
    fn decode_accepts_well_formed_all_exception_chunk() {
        // Pins the helper itself: well-formed indices ([0, 1, 2],
        // strictly ascending and < row_count) must decode cleanly.
        // Without this, the negative tests above could be defeated
        // by an implementation that returns Corruption unconditionally
        // or by a helper that produces malformed chunks for unrelated
        // reasons.
        let chunk = build_alp_chunk_with_indices(&[0, 1, 2], 3);
        let decoded = Alp.decode(&chunk, &BqlType::Float).unwrap();
        assert_f64_eq(&decoded, &[0.0, 0.0, 0.0]);
    }

    #[test]
    fn decode_rejects_unsorted_patch_indices() {
        // Indices [2, 1, 0] are descending.  The decode loop scans
        // forward through `i`, so a non-ascending stream silently
        // produces wrong output (later indices are never matched).
        let chunk = build_alp_chunk_with_indices(&[2, 1, 0], 3);
        let err = Alp.decode(&chunk, &BqlType::Float).unwrap_err();
        match err {
            BqliteError::Corruption(msg) => {
                assert!(
                    msg.contains("patch"),
                    "expected message to mention patch indices, got: {msg}"
                );
            }
            other => panic!("expected Corruption error, got {other:?}"),
        }
    }

    // ── row_count preserved ────────────────────────────────────────

    #[test]
    fn row_count_preserved() {
        for n in [0, 1, 5, 128, 129, 256, 300] {
            let values: Vec<f64> = (0..n).map(|i| i as f64 * 0.1).collect();
            let array: ArrayRef = Arc::new(Float64Array::from(values));
            let chunk = Alp.encode(array.as_ref()).unwrap();
            assert_eq!(chunk.row_count, n);
        }
    }

    // ── FOR internal helpers ───────────────────────────────────────

    #[test]
    fn for_block_frame_all_identical() {
        let values = vec![42_i64; 128];
        let (min, width) = for_block_frame(&values);
        assert_eq!(min, 42);
        assert_eq!(width, 1);
    }

    #[test]
    fn for_block_frame_narrow_range() {
        let (min, width) = for_block_frame(&[10, 12, 11, 15, 10]);
        assert_eq!(min, 10);
        // max_offset = 5 → 3 bits
        assert_eq!(width, 3);
    }

    #[test]
    fn for_encode_decode_round_trip() {
        let values: Vec<i64> = (0..300).map(|i| 1000 + i * 7).collect();
        let (block_count, payload) = for_encode(&values);
        let (decoded, _) = for_decode(&payload, block_count, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn for_encode_decode_wide_range() {
        // Range exceeding u32 to exercise the >32-bit scalar path.
        let values: Vec<i64> = (0..5).map(|i| i64::MIN / 2 + i * (i64::MAX / 4)).collect();
        let (block_count, payload) = for_encode(&values);
        let (decoded, _) = for_decode(&payload, block_count, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn decode_borrowed_matches_decode() {
        let values = vec![1.0, 2.5, 3.75, 100.0];
        let array: ArrayRef = Arc::new(Float64Array::from(values.clone()));
        let chunk = Alp.encode(array.as_ref()).unwrap();

        let borrowed = BorrowedEncodedChunk {
            encoding: chunk.encoding,
            params: &chunk.params,
            payload: &chunk.payload,
            row_count: chunk.row_count,
        };
        let decoded_owned = Alp.decode(&chunk, &BqlType::Float).unwrap();
        let decoded_borrowed = Alp.decode_borrowed(borrowed, &BqlType::Float).unwrap();
        assert_eq!(decoded_owned.as_ref(), decoded_borrowed.as_ref());
    }
}
