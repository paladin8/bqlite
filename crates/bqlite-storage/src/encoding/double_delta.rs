//! DoubleDelta encoding — second-order delta (delta-of-deltas) for monotonic
//! integer/timestamp columns.
//!
//! DoubleDelta extends the Delta encoding by storing the differences of
//! consecutive first-order deltas rather than the deltas themselves. When
//! those second-order deltas are small (the data advances at an approximately
//! constant rate), this achieves significantly better compression than Delta:
//! ~2× over Delta and ~7× over Plain on near-constant-interval timestamps.
//!
//! The on-disk layout per `docs/design/storage/segment-format-v2.md` §4:
//!
//! ```text
//! encoding_params (17 bytes):
//!     base_value:   i64 LE    // values[0]
//!     first_delta:  i64 LE    // values[1] - values[0]  (0 if row_count == 1)
//!     dd_bit_width: u8        // 1..=64 (floor: 1 even when all dd == 0)
//! payload:
//!     bit-packed zigzag(dd[i]) stream
//!     (row_count - 2) values, padded to 8-byte SIMD-lane multiple
//! ```
//!
//! The `first_delta` is stored in params rather than in the payload because
//! it bootstraps the cumulative-sum reconstruction and does not benefit from
//! second-order compression (there is no "previous delta" to subtract from it).
//!
//! # Applicable types
//!
//! `BqlType::Int` (`Int64Array`) and `BqlType::Timestamp`
//! (`TimestampNanosecondArray`, any timezone). Both are `i64` at the Arrow
//! level, so the encode/decode paths share a helper that only threads the
//! Arrow array type at the outermost boundary. Decoding a Timestamp chunk
//! re-applies the canonical `"UTC"` timezone so downstream operators see
//! the schema-canonical type.
//!
//! `Bool`, `Float`, `String`, `List`, `Map` are `applicable_to` → `false`.
//!
//! # Null handling
//!
//! DoubleDelta operates on dense arrays — the writer strips nulls into a
//! separate bitmap before handing values to the encoding layer. A nullable
//! input is a contract violation caught by `super::require_dense`.
//!
//! # Edge cases
//!
//! - `row_count == 0`: illegal. `encode` returns `BqliteError::Execution`; the
//!   selector routes empty chunks through `Constant` or `Plain`.
//! - `row_count == 1`: legal. `base_value` carries the lone value;
//!   `first_delta = 0`; payload is 0 bytes.
//! - `row_count == 2`: legal. `base_value` and `first_delta` carry both values;
//!   payload is 0 bytes (no double-deltas to store).
//! - Overflow: `encode` computes residuals in `i128`. If `first_delta` or any
//!   `dd` value exceeds `i64` range, it returns `BqliteError::Execution` so the
//!   selector falls back to Delta or another encoding.
//!
//! # Relationship to Delta
//!
//! DoubleDelta is strictly better than Delta when
//! `dd_bit_width < delta_bit_width`. When `dd_bit_width ≥ delta_bit_width`
//! (random or non-linear data), Delta is preferred for its lower decode cost
//! (one prefix-sum pass vs. two). The selector guards this via
//! `estimate_size` comparison; see `advanced-encodings.md` §4.7.
//!
//! See `docs/design/storage/advanced-encodings.md` §4 for the full
//! analysis and `docs/design/storage/segment-format-v2.md` for the
//! byte-level layout this module implements.

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the DoubleDelta encoding.
///
/// Stateless; freely clonable and stored behind a `Box<dyn Encoding>`.
/// See the module-level documentation for the byte layout this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct DoubleDelta;

impl DoubleDelta {
    /// Construct a new DoubleDelta encoder. DoubleDelta has no configuration —
    /// this is sugar for `DoubleDelta` the unit struct.
    pub const fn new() -> Self {
        DoubleDelta
    }
}

/// Params bytes: `base_value: i64 LE` (8) + `first_delta: i64 LE` (8)
/// + `dd_bit_width: u8` (1) = 17 bytes.
const PARAMS_LEN: usize = 17;

/// SIMD lane size — the on-disk payload is padded up to a multiple of this
/// so a bit-packed unpacker can read one full lane past the last value
/// without a bounds check.
const SIMD_LANE_BYTES: usize = 8;

impl Encoding for DoubleDelta {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::DoubleDelta
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::Int | BqlType::Timestamp)
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let values = values_i64(array)?;
        if values.is_empty() {
            return Err(BqliteError::Execution(
                "DoubleDelta::estimate_size: row_count == 0 is illegal for DoubleDelta \
                 (segment-format-v2.md §4). The selector routes empty chunks \
                 through Constant or Plain."
                    .into(),
            ));
        }
        let width = pick_dd_bit_width(&values)?;
        Ok(payload_byte_count(values.len(), width))
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "DoubleDelta")?;
        let values = values_i64(array)?;
        let row_count = values.len();
        if row_count == 0 {
            return Err(BqliteError::Execution(
                "DoubleDelta::encode: row_count == 0 is illegal for DoubleDelta \
                 (segment-format-v2.md §4). The selector routes empty chunks \
                 through Constant or Plain."
                    .into(),
            ));
        }

        let base_value = values[0];
        // For row_count == 1 there is no second value, so first_delta is 0.
        let first_delta: i64 = if row_count >= 2 {
            let d = values[1] as i128 - values[0] as i128;
            if d < i64::MIN as i128 || d > i64::MAX as i128 {
                return Err(BqliteError::Execution(format!(
                    "DoubleDelta::encode: first_delta {d} overflows i64 — \
                     segment-format-v2.md §4 requires the selector to \
                     fall back to another encoding"
                )));
            }
            d as i64
        } else {
            0
        };

        let width = pick_dd_bit_width(&values)?;

        let mut params = Vec::with_capacity(PARAMS_LEN);
        params.extend_from_slice(&base_value.to_le_bytes());
        params.extend_from_slice(&first_delta.to_le_bytes());
        params.push(width);
        debug_assert_eq!(params.len(), PARAMS_LEN);

        let payload = bit_pack_dd_values(&values, width)?;
        Ok(EncodedChunk {
            encoding: EncodingType::DoubleDelta,
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
    if encoding != EncodingType::DoubleDelta {
        return Err(BqliteError::Execution(format!(
            "DoubleDelta::decode called on a {:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if params.len() != PARAMS_LEN {
        return Err(BqliteError::Execution(format!(
            "DoubleDelta::decode expects a {PARAMS_LEN}-byte params block \
             (base_value: i64 LE + first_delta: i64 LE + dd_bit_width: u8) per \
             segment-format-v2.md §4, got {} bytes",
            params.len()
        )));
    }
    if row_count == 0 {
        return Err(BqliteError::Execution(
            "DoubleDelta::decode: chunk.row_count == 0 is illegal for DoubleDelta \
             (segment-format-v2.md §4)"
                .into(),
        ));
    }

    let base_value = i64::from_le_bytes(params[..8].try_into().unwrap());
    let first_delta = i64::from_le_bytes(params[8..16].try_into().unwrap());
    let width = params[16];

    let values = bit_unpack_dd_values(payload, base_value, first_delta, row_count, width)?;

    match ty {
        BqlType::Int => Ok(Arc::new(Int64Array::from(values)) as ArrayRef),
        BqlType::Timestamp => {
            let array = TimestampNanosecondArray::from(values).with_timezone("UTC");
            Ok(Arc::new(array) as ArrayRef)
        }
        other => Err(BqliteError::Execution(format!(
            "DoubleDelta::decode does not support BqlType::{other} — \
             DoubleDelta encoding covers Int and Timestamp only"
        ))),
    }
}

// ── value extraction ────────────────────────────────────────────────────────

/// Downcast a dense Arrow array to its backing `i64` values.
///
/// Accepts `Int64` and `Timestamp(Nanosecond, _)`. Every other Arrow
/// type is a typed error — the selector should never route a
/// non-integer column to DoubleDelta, but `encode` guards the invariant
/// explicitly.
fn values_i64(array: &dyn Array) -> Result<Vec<i64>> {
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution(
                    "DoubleDelta: expected Arrow Int64Array, downcast failed".into(),
                )
            })?;
            Ok(a.values().to_vec())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "DoubleDelta: expected Arrow TimestampNanosecondArray, downcast failed"
                            .into(),
                    )
                })?;
            Ok(a.values().to_vec())
        }
        other => Err(BqliteError::Execution(format!(
            "DoubleDelta does not support Arrow type {other:?} — DoubleDelta \
             encoding covers Int64 and Timestamp(Nanosecond) only"
        ))),
    }
}

// ── bit width selection ─────────────────────────────────────────────────────

/// Compute the smallest `dd_bit_width` that fits every second-order delta
/// value (delta-of-deltas), zigzag-encoded.
///
/// Returns 1 as a floor — bit-packing at width 0 is undefined and an
/// all-zero double-delta run still needs a meaningful width so the
/// round-trip invariant holds.
///
/// Errors if `first_delta` or any `dd` value cannot fit in `i64`. The
/// overflow check is computed in `i128`; the spec mandates that the writer
/// detect and reject such chunks so the selector can fall back to another
/// encoding.
fn pick_dd_bit_width(values: &[i64]) -> Result<u8> {
    // 0 or 1 values: no double-deltas, use floor width 1.
    // 2 values: only first_delta exists; no double-deltas, use floor width 1.
    // Also validate first_delta fits in i64.
    if values.len() >= 2 {
        let d = values[1] as i128 - values[0] as i128;
        if d < i64::MIN as i128 || d > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "DoubleDelta::encode: first_delta {d} overflows i64 — \
                 segment-format-v2.md §4 requires the selector to \
                 fall back to another encoding"
            )));
        }
    }
    if values.len() <= 2 {
        return Ok(1);
    }

    let mut max_zigzag: u64 = 0;
    // Compute dd[i] = delta[i] - delta[i-1] for i in 1..n-1, where
    // delta[i] = values[i+1] - values[i].
    for window in values.windows(3) {
        let v0 = window[0] as i128;
        let v1 = window[1] as i128;
        let v2 = window[2] as i128;
        let delta0 = v1 - v0; // delta[i-1]
        let delta1 = v2 - v1; // delta[i]
                              // Validate that deltas fit in i64 (needed for the first_delta check
                              // and consistency with the encode path).
        if delta0 < i64::MIN as i128 || delta0 > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "DoubleDelta::encode: delta {delta0} overflows i64 — \
                 segment-format-v2.md §4 requires the selector to \
                 fall back to another encoding"
            )));
        }
        if delta1 < i64::MIN as i128 || delta1 > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "DoubleDelta::encode: delta {delta1} overflows i64 — \
                 segment-format-v2.md §4 requires the selector to \
                 fall back to another encoding"
            )));
        }
        let dd = delta1 - delta0;
        if dd < i64::MIN as i128 || dd > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "DoubleDelta::encode: double-delta {dd} overflows i64 — \
                 segment-format-v2.md §4 requires the selector to \
                 fall back to another encoding"
            )));
        }
        let z = zigzag_encode(dd as i64);
        if z > max_zigzag {
            max_zigzag = z;
        }
    }

    if max_zigzag == 0 {
        Ok(1)
    } else {
        let bits = 64 - max_zigzag.leading_zeros();
        Ok(bits as u8)
    }
}

/// Exact payload byte count for a DoubleDelta chunk.
///
/// Formula: `ceil(width × max(0, row_count − 2) / 8)`, rounded up to the
/// next multiple of `SIMD_LANE_BYTES`. For `row_count <= 2` the double-delta
/// payload is 0 bytes.
fn payload_byte_count(row_count: usize, width: u8) -> usize {
    let dd_count = row_count.saturating_sub(2);
    let unpadded_bits = (width as usize) * dd_count;
    let unpadded_bytes = unpadded_bits.div_ceil(8);
    // Round up to SIMD_LANE_BYTES multiple.
    unpadded_bytes.div_ceil(SIMD_LANE_BYTES) * SIMD_LANE_BYTES
}

// ── zigzag encoding ─────────────────────────────────────────────────────────

/// Zigzag-encode a signed `i64` value into an unsigned `u64`.
///
/// `zigzag(0) == 0`, `zigzag(-1) == 1`, `zigzag(1) == 2`, … —
/// maps small-magnitude signed values into small unsigned ones so
/// bit-packing can use fewer bits.
#[inline]
fn zigzag_encode(x: i64) -> u64 {
    ((x << 1) ^ (x >> 63)) as u64
}

/// Inverse of [`zigzag_encode`].
#[inline]
fn zigzag_decode(z: u64) -> i64 {
    ((z >> 1) as i64) ^ -((z & 1) as i64)
}

// ── bit packing / unpacking ─────────────────────────────────────────────────

/// Bit-pack the double-delta values of `values` into a byte buffer.
///
/// Computes `dd[i] = delta[i] - delta[i-1]` for `i` in `1..n-1` inline,
/// zigzag-encodes each, and packs at `width` bits LSB-first. Pads the
/// buffer up to the next multiple of `SIMD_LANE_BYTES`.
fn bit_pack_dd_values(values: &[i64], width: u8) -> Result<Vec<u8>> {
    if !(1..=64).contains(&width) {
        return Err(BqliteError::Execution(format!(
            "DoubleDelta::encode: dd_bit_width {width} out of valid range 1..=64"
        )));
    }
    let row_count = values.len();
    let expected_bytes = payload_byte_count(row_count, width);
    let mut out = vec![0u8; expected_bytes];

    if row_count <= 2 {
        // No double-deltas to pack.
        return Ok(out);
    }

    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };

    let mut bit_pos: usize = 0;
    // Slide a window of 3 to compute delta[i-1] and delta[i], then dd.
    for window in values.windows(3) {
        let delta0 = (window[1] as i128 - window[0] as i128) as i64;
        let delta1 = (window[2] as i128 - window[1] as i128) as i64;
        let dd = (delta1 as i128 - delta0 as i128) as i64;
        let z = zigzag_encode(dd) & mask;

        // Write `width` bits LSB-first starting at `bit_pos`.
        let mut remaining = width as usize;
        let mut value = z;
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
        bit_pos += width as usize;
    }

    debug_assert!(bit_pos <= expected_bytes * 8);
    Ok(out)
}

/// Inverse of [`bit_pack_dd_values`]: reconstruct the original `values`
/// vector from a bit-packed double-delta stream.
///
/// Takes `base_value` and `first_delta` (from the chunk's params), the
/// full `row_count`, and `width`. Returns a `Vec<i64>` of length
/// `row_count` reconstructed via two prefix-sum passes:
///
/// 1. Unpack and zigzag-decode to get double-deltas.
/// 2. Prefix-sum over double-deltas to recover first-order deltas.
/// 3. Prefix-sum over first-order deltas (starting from `base_value`) to
///    recover original values.
fn bit_unpack_dd_values(
    payload: &[u8],
    base_value: i64,
    first_delta: i64,
    row_count: usize,
    width: u8,
) -> Result<Vec<i64>> {
    if !(1..=64).contains(&width) {
        return Err(BqliteError::Execution(format!(
            "DoubleDelta::decode: dd_bit_width {width} out of valid range 1..=64"
        )));
    }
    let expected_bytes = payload_byte_count(row_count, width);
    if payload.len() != expected_bytes {
        return Err(BqliteError::Execution(format!(
            "DoubleDelta::decode: expected {expected_bytes} payload bytes for \
             {row_count} rows at {width}-bit double-deltas, got {}",
            payload.len()
        )));
    }

    let mut values = Vec::with_capacity(row_count);
    values.push(base_value);
    if row_count == 1 {
        return Ok(values);
    }
    // Row 1 is reconstructed from first_delta.
    let v1 = (base_value as i128 + first_delta as i128) as i64;
    values.push(v1);
    if row_count == 2 {
        return Ok(values);
    }

    // Unpack double-deltas from the bit-packed stream.
    let dd_count = row_count - 2;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };

    let mut prev_delta = first_delta as i128;
    let mut prev_value = v1 as i128;
    let mut bit_pos: usize = 0;

    for _ in 0..dd_count {
        // Read `width` bits LSB-first starting at `bit_pos`.
        let mut remaining = width as usize;
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        let mut byte_idx = bit_pos / 8;
        let mut bit_in_byte = bit_pos % 8;
        while remaining > 0 {
            let free_in_byte = 8 - bit_in_byte;
            let take = remaining.min(free_in_byte);
            let byte = payload[byte_idx];
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
        bit_pos += width as usize;

        let z = value & mask;
        let dd = zigzag_decode(z) as i128;

        // Reconstruct: curr_delta = prev_delta + dd, curr_value = prev_value + curr_delta.
        let curr_delta = prev_delta + dd;
        let curr_value = prev_value + curr_delta;

        // Validate that the reconstructed value fits in i64. Overflow here
        // indicates a corrupt or malformed chunk.
        if curr_value < i64::MIN as i128 || curr_value > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "DoubleDelta::decode: decoded value {curr_value} overflows i64 — \
                 segment corruption (a writer that produced this chunk \
                 already would have errored per segment-format-v2.md §4)"
            )));
        }

        values.push(curr_value as i64);
        prev_delta = curr_delta;
        prev_value = curr_value;
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampNanosecondArray};

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let dd = DoubleDelta;
        let chunk = dd.encode(array.as_ref()).unwrap();
        dd.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_int_and_timestamp_only() {
        let d = DoubleDelta;
        assert!(d.applicable_to(&BqlType::Int));
        assert!(d.applicable_to(&BqlType::Timestamp));
        assert!(!d.applicable_to(&BqlType::Bool));
        assert!(!d.applicable_to(&BqlType::Float));
        assert!(!d.applicable_to(&BqlType::String));
        assert!(!d.applicable_to(&BqlType::List(Box::new(BqlType::Int))));
    }

    #[test]
    fn encoding_type_is_double_delta_discriminant_three() {
        assert_eq!(DoubleDelta.encoding_type(), EncodingType::DoubleDelta);
        assert_eq!(DoubleDelta.encoding_type().discriminant(), 3);
    }

    #[test]
    fn zigzag_round_trip_examples() {
        for v in [0i64, -1, 1, -2, 2, i64::MIN, i64::MAX, -100, 100] {
            let z = zigzag_encode(v);
            let back = zigzag_decode(z);
            assert_eq!(back, v, "zigzag round-trip failed for {v}");
        }
    }

    #[test]
    fn int_round_trip_monotonic_timestamps() {
        // Near-constant-interval nanosecond timestamps: base ~1.7×10¹⁸ ns,
        // step = 1 ms ± small jitter. DoubleDelta's sweet spot.
        let base = 1_700_000_000_000_000_000_i64;
        let step = 1_000_000_i64; // 1 ms in ns
        let jitter: Vec<i64> = vec![0, 100, -50, 200, -100, 50, 300, -200, 0, 150];
        let values: Vec<i64> = jitter
            .iter()
            .enumerate()
            .map(|(i, &j)| base + i as i64 * step + j)
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_single_element() {
        // row_count == 1: only base_value in params, no payload.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_two_elements() {
        // row_count == 2: base_value + first_delta in params, no payload.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10_i64, 25]));
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.payload.len(), 0, "two elements produce no payload");
        let decoded = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_strictly_monotonic_seq_id() {
        // Strictly monotonic seq_id (Δ = 1, dd = 0). Per the spec, dd_bit_width
        // floors to 1 and all packed bits are zero — smallest possible payload.
        let values: Vec<i64> = (0..32).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        assert_eq!(
            chunk.params[16], 1,
            "dd_bit_width should be 1 (floor) for all-zero dd"
        );
        let decoded = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap();
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_negative_values() {
        // Series with negative values and mixed sign deltas.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![-100_i64, -95, -91, -88, -86, -85]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_constant_series() {
        // All-equal values → all-zero deltas → all-zero double-deltas.
        // Width floors to 1.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 8]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_extrema_near_i64_max() {
        // Values near i64::MAX with small deltas exercise sign extension
        // of large base_values; the double-deltas remain narrow.
        let base = i64::MAX - 100;
        let values: Vec<i64> = (0..8).map(|i| base + i * 10).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn timestamp_round_trip_preserves_utc() {
        // TimestampNanosecondArray equality includes the timezone metadata,
        // so this also asserts that DoubleDelta::decode reapplies "UTC".
        let base = 1_700_000_000_000_000_000_i64;
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![
                base,
                base + 1_000_000,
                base + 2_000_100,
                base + 3_000_050,
                base + 4_000_200,
            ])
            .with_timezone("UTC"),
        );
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn payload_is_padded_to_simd_lane_multiple() {
        let cases: &[Vec<i64>] = &[
            vec![1, 2, 3],
            vec![0_i64; 10],
            (0..17).collect(),
            (0..32).collect(),
        ];
        for values in cases {
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
            assert_eq!(
                chunk.payload.len() % SIMD_LANE_BYTES,
                0,
                "payload not SIMD-aligned for {:?}",
                values,
            );
        }
    }

    #[test]
    fn row_count_one_has_empty_payload_and_zero_first_delta() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1234_i64]));
        let chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.payload.len(), 0);
        let base_value = i64::from_le_bytes(chunk.params[..8].try_into().unwrap());
        let first_delta = i64::from_le_bytes(chunk.params[8..16].try_into().unwrap());
        assert_eq!(base_value, 1234);
        assert_eq!(first_delta, 0);
        let decoded = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap();
        let decoded_int = decoded.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(decoded_int.len(), 1);
        assert_eq!(decoded_int.value(0), 1234);
    }

    #[test]
    fn encode_rejects_empty_array() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("row_count == 0"),
                    "error should mention the illegal row_count, got: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_overflow_first_delta() {
        // first_delta = i64::MAX - i64::MIN overflows i64 in the i128 check.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX]));
        let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("overflows i64"), "unexpected: {msg}");
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_overflow_double_delta() {
        // Construct a sequence where dd overflows i64:
        // delta[0] = i64::MAX, delta[1] = i64::MIN → dd = delta[1] - delta[0]
        // = i64::MIN - i64::MAX which overflows i64.
        // We need three values where delta[0] and delta[1] are large with
        // opposite signs. Use small base to keep first_delta in range.
        let base = 0_i64;
        let v1 = i64::MAX / 2; // delta[0] = i64::MAX / 2
                               // Make delta[1] very negative to cause dd overflow:
                               // delta[1] = v2 - v1, so v2 = v1 + i64::MIN/2
        let v2 = v1.wrapping_add(i64::MIN / 2 + 1);
        // We need delta[0] + delta[1] to cause overflow in dd computation:
        // delta[0] = v1 - base = i64::MAX/2
        // delta[1] = v2 - v1 = i64::MIN/2 + 1
        // dd = delta[1] - delta[0] = (i64::MIN/2 + 1) - i64::MAX/2
        //    ≈ i64::MIN/2 - i64::MAX/2 which doesn't overflow i64...
        //
        // For a reliable overflow test, use values that produce large consecutive
        // deltas of opposite signs. Specifically: v0=0, v1=i64::MAX/2, v2=0
        // gives delta[0]=i64::MAX/2, delta[1]=-i64::MAX/2, dd=-i64::MAX which
        // is in range. Let's try a harder case:
        // v0=0, v1=i64::MAX/2+1, v2=-(i64::MAX/2+1):
        // delta[0] = i64::MAX/2+1, delta[1] = -(i64::MAX/2+1)*2 which may overflow.
        let _ = (base, v1, v2); // silence unused warnings

        // More direct approach: values[1] - values[0] and values[2] - values[1]
        // both near i64::MAX with opposite signs.
        let vals: Vec<i64> = vec![0, i64::MAX / 2, i64::MIN / 2];
        // delta[0] = i64::MAX/2, delta[1] = i64::MIN/2 - i64::MAX/2
        // dd = delta[1] - delta[0] = i64::MIN/2 - i64::MAX/2 - i64::MAX/2
        //    = i64::MIN/2 - i64::MAX which overflows.
        let array: ArrayRef = Arc::new(Int64Array::from(vals));
        let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("overflows i64"), "unexpected: {msg}");
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_unsupported_type() {
        use arrow::array::Float64Array;
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let err = DoubleDelta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("Int64 and Timestamp"), "unexpected: {msg}");
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0u8; PARAMS_LEN],
            payload: vec![0u8; 8],
            row_count: 1,
        };
        let err = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_params_length_mismatch() {
        let chunk = EncodedChunk {
            encoding: EncodingType::DoubleDelta,
            params: vec![0u8; PARAMS_LEN - 1],
            payload: vec![0u8; 8],
            row_count: 1,
        };
        let err = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("params block")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_payload_length_mismatch() {
        let values: Vec<i64> = (0..8).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let mut chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        if chunk.payload.is_empty() {
            // Row count 8 should have a non-empty payload; if not, skip.
            return;
        }
        chunk.payload.pop();
        let err = DoubleDelta.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("payload bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn estimate_size_matches_encoded_payload_length() {
        let values: Vec<i64> = (0..20).map(|i| i * 1_000_000).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let estimated = DoubleDelta.estimate_size(array.as_ref()).unwrap();
        let encoded = DoubleDelta.encode(array.as_ref()).unwrap();
        assert_eq!(estimated, encoded.payload.len());
    }

    #[test]
    fn double_delta_compresses_better_than_delta_on_near_constant_intervals() {
        // Near-constant-interval timestamps — DoubleDelta's sweet spot.
        // dd values are tiny (jitter ±500 ns) so dd_bit_width << delta_bit_width.
        use super::super::delta::Delta;
        let base = 1_700_000_000_000_000_000_i64;
        let step = 1_000_000_i64; // 1 ms in ns
        let jitter: Vec<i64> = (-8..8).map(|i| i * 500).collect();
        let values: Vec<i64> = jitter
            .iter()
            .enumerate()
            .map(|(i, &j)| base + i as i64 * step + j)
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));

        let dd_chunk = DoubleDelta.encode(array.as_ref()).unwrap();
        let delta_chunk = Delta.encode(array.as_ref()).unwrap();

        assert!(
            dd_chunk.payload.len() <= delta_chunk.payload.len(),
            "DoubleDelta ({} bytes) should not exceed Delta ({} bytes) on near-constant intervals",
            dd_chunk.payload.len(),
            delta_chunk.payload.len(),
        );
    }
}
