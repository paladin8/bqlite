//! Delta encoding — first-value + bit-packed consecutive residuals.
//!
//! Delta stores a run of ordered-ish integers compactly by writing
//! the first value as-is (`base_value`) and then encoding the series
//! of differences `values[i+1] - values[i]` into a bit-packed stream
//! of zigzag-encoded `i64` residuals. Every residual uses the same
//! number of bits (`residual_bit_width`), chosen at encode time from
//! the maximum absolute residual so the total payload is
//! `≈ width × (row_count − 1) / 8` bytes. It is the workhorse for
//! monotonic-ish timestamp columns within an entity, where
//! consecutive values differ by small nanosecond deltas.
//!
//! See `docs/design/storage/segment-format-v1.md` §9.3 for the
//! authoritative byte-level layout this module implements.
//!
//! # Applicable types
//!
//! Wave 2 Delta encodes `Int` (`Int64Array`) and `Timestamp`
//! (`TimestampNanosecondArray`, any timezone). Both are i64 at the
//! Arrow level, so the encode/decode paths share a helper that only
//! needs to thread the Arrow array type at the outermost boundary.
//! Decoding a Timestamp chunk re-applies the canonical `"UTC"`
//! timezone so downstream operators see the schema-canonical type.
//!
//! `Bool`, `Float`, `String`, `List`, `Map` are all
//! `applicable_to` → `false`. The selector (TASK-212) will not
//! route them here, and `encode` rejects them with a typed error if
//! it somehow does.
//!
//! # Null handling
//!
//! Delta operates on dense arrays — the writer strips nulls into a
//! separate bitmap before handing values to the encoding layer. A
//! nullable input is a contract violation, caught by
//! [`super::require_dense`].
//!
//! # Edge cases
//!
//! Per the spec:
//!
//! - `row_count == 0`: illegal. `encode` returns `BqliteError::Execution`;
//!   the selector is expected to route empty chunks through `Constant`
//!   (`Constant::encode` with a NULL value) or `Plain`.
//! - `row_count == 1`: legal. `base_value` carries the lone value;
//!   the residual payload is 0 bytes plus SIMD padding.
//! - Overflow: `encode` computes residuals in `i128`. If any residual
//!   cannot fit in `i64`, it returns `BqliteError::Execution` so the
//!   selector falls back to another encoding.
//!
//! # Bit packing
//!
//! Residuals are zigzag-encoded to an unsigned `u64`
//! (`zigzag(x) = (x << 1) ^ (x >> 63)`) and then bit-packed at
//! `residual_bit_width` bits per value. The packer is a simple
//! byte-level accumulator — correct on every width in `1..=64`. The
//! on-disk payload is padded up to a multiple of 8 bytes so a
//! FastLanes SIMD unpacker can read one full lane past the last
//! residual without a bounds check. The padding is part of
//! `uncompressed_payload_length` and the chunk's `byte_length`.

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, BorrowedEncodedChunk, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the Delta encoding.
///
/// Stateless; freely clonable and stored behind a
/// `Box<dyn Encoding>`. See the module-level documentation for the
/// byte layout this impl produces.
#[derive(Debug, Clone, Copy, Default)]
pub struct Delta;

impl Delta {
    /// Construct a new Delta encoder. Delta has no configuration —
    /// this is sugar for `Delta` the unit struct.
    pub const fn new() -> Self {
        Delta
    }
}

/// Params bytes: `base_value: i64 LE` (8 bytes) + `residual_bit_width: u8` (1 byte).
const PARAMS_LEN: usize = 9;

/// SIMD lane size — the on-disk payload is padded up to a multiple
/// of this so a bit-packed unpacker can read one full lane past the
/// last residual without a bounds check.
const SIMD_LANE_BYTES: usize = 8;

impl Encoding for Delta {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::Delta
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        // v1 Delta handles integer-backed scalar types only.
        matches!(ty, BqlType::Int | BqlType::Timestamp)
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let values = values_i64(array)?;
        // For `row_count == 0` the spec says Delta is illegal; return
        // an error so the selector doesn't rank Delta against a legal
        // alternative (Constant / Plain).
        if values.is_empty() {
            return Err(BqliteError::Execution(
                "Delta::estimate_size: row_count == 0 is illegal for Delta \
                 (segment-format-v1.md §9.3). The selector routes empty \
                 chunks through Constant or Plain."
                    .into(),
            ));
        }
        let width = pick_residual_bit_width(&values)?;
        Ok(payload_byte_count(values.len(), width))
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "Delta")?;
        let values = values_i64(array)?;
        let row_count = values.len();
        if row_count == 0 {
            return Err(BqliteError::Execution(
                "Delta::encode: row_count == 0 is illegal for Delta \
                 (segment-format-v1.md §9.3). The selector routes empty \
                 chunks through Constant or Plain."
                    .into(),
            ));
        }
        let base_value = values[0];
        let width = pick_residual_bit_width(&values)?;
        let mut params = Vec::with_capacity(PARAMS_LEN);
        params.extend_from_slice(&base_value.to_le_bytes());
        params.push(width);
        debug_assert_eq!(params.len(), PARAMS_LEN);

        let payload = bit_pack_residuals(&values, width)?;
        Ok(EncodedChunk {
            encoding: EncodingType::Delta,
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
    if encoding != EncodingType::Delta {
        return Err(BqliteError::Execution(format!(
            "Delta::decode called on a {:?} chunk — dispatch must \
             route each chunk to its declared encoding's decoder",
            encoding
        )));
    }
    if params.len() != PARAMS_LEN {
        return Err(BqliteError::Execution(format!(
            "Delta::decode expects a {PARAMS_LEN}-byte params block \
             (base_value: i64 LE + residual_bit_width: u8) per \
             segment-format-v1.md §9.3, got {} bytes",
            params.len()
        )));
    }
    if row_count == 0 {
        return Err(BqliteError::Execution(
            "Delta::decode: chunk.row_count == 0 is illegal for Delta \
             (segment-format-v1.md §9.3)"
                .into(),
        ));
    }
    let base_value = i64::from_le_bytes(params[..8].try_into().unwrap());
    let width = params[8];
    let values = bit_unpack_residuals(payload, base_value, row_count, width)?;
    match ty {
        BqlType::Int => Ok(Arc::new(Int64Array::from(values)) as ArrayRef),
        BqlType::Timestamp => {
            let array = TimestampNanosecondArray::from(values).with_timezone("UTC");
            Ok(Arc::new(array) as ArrayRef)
        }
        other => Err(BqliteError::Execution(format!(
            "Delta::decode does not support BqlType::{other} — \
             v1 Delta encoding covers Int and Timestamp only"
        ))),
    }
}

// ── value extraction ────────────────────────────────────────────────────────

/// Downcast a dense Arrow array to its backing `i64` values.
///
/// Accepts `Int64` and `Timestamp(Nanosecond, _)`. Every other Arrow
/// type is a typed error — the selector should never route a
/// non-integer column to Delta, but `encode` guards the invariant
/// explicitly.
fn values_i64(array: &dyn Array) -> Result<Vec<i64>> {
    match array.data_type() {
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("Delta: expected Arrow Int64Array, downcast failed".into())
            })?;
            Ok(a.values().to_vec())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "Delta: expected Arrow TimestampNanosecondArray, downcast failed".into(),
                    )
                })?;
            Ok(a.values().to_vec())
        }
        other => Err(BqliteError::Execution(format!(
            "Delta does not support Arrow type {other:?} — v1 Delta \
             encoding covers Int64 and Timestamp(Nanosecond) only"
        ))),
    }
}

// ── bit width selection ─────────────────────────────────────────────────────

/// Compute the smallest `residual_bit_width` that fits every
/// consecutive residual in `values`, zigzag-encoded.
///
/// Returns 1 as a floor — bit-packing at width 0 is undefined and
/// an all-zero residual run (a strictly constant series — but
/// that's a `Constant` shape, not Delta) still needs a
/// meaningful width so the round-trip invariant holds.
///
/// Errors if any consecutive residual overflows `i64`. The
/// overflow check is computed in `i128`; the spec mandates that
/// the writer detect and reject such chunks so the selector can
/// fall back to another encoding.
fn pick_residual_bit_width(values: &[i64]) -> Result<u8> {
    if values.len() <= 1 {
        // Single-element / empty: no residuals, choose the minimum
        // legal width. Empty arrays are rejected by `encode` before
        // reaching this helper, but `estimate_size` may still ask
        // about an empty array — returning 1 keeps the payload size
        // formula well-defined.
        return Ok(1);
    }
    let mut max_zigzag: u64 = 0;
    for window in values.windows(2) {
        let prev = window[0] as i128;
        let next = window[1] as i128;
        let residual = next - prev;
        if residual < i64::MIN as i128 || residual > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "Delta::encode: consecutive residual {residual} overflows \
                 i64 — segment-format-v1.md §9.3 requires the selector to \
                 fall back to another encoding"
            )));
        }
        let r = residual as i64;
        let z = zigzag_encode(r);
        if z > max_zigzag {
            max_zigzag = z;
        }
    }
    if max_zigzag == 0 {
        Ok(1)
    } else {
        // `64 - leading_zeros` is the number of significant bits.
        let bits = 64 - max_zigzag.leading_zeros();
        Ok(bits as u8)
    }
}

/// Exact payload byte count for a Delta chunk.
///
/// Formula: `ceil(width × (row_count − 1) / 8)`, rounded up to the
/// next multiple of `SIMD_LANE_BYTES` so the unpacker can read one
/// full lane past the last residual. For `row_count <= 1` the
/// residual payload is zero bytes plus the SIMD padding.
fn payload_byte_count(row_count: usize, width: u8) -> usize {
    let residual_count = row_count.saturating_sub(1);
    let unpadded_bits = (width as usize) * residual_count;
    let unpadded_bytes = unpadded_bits.div_ceil(8);
    // Round up to SIMD_LANE_BYTES multiple.
    unpadded_bytes.div_ceil(SIMD_LANE_BYTES) * SIMD_LANE_BYTES
}

// ── bit packing / unpacking ─────────────────────────────────────────────────

/// Zigzag-encode a signed `i64` residual into an unsigned `u64`.
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

/// Bit-pack the consecutive residuals of `values` into a byte buffer.
///
/// Writes `residual_bit_width` bits per residual, LSB-first within
/// each byte. Pads the buffer up to the next multiple of
/// `SIMD_LANE_BYTES` so callers can unpack with a SIMD routine that
/// reads one full lane past the last residual.
fn bit_pack_residuals(values: &[i64], width: u8) -> Result<Vec<u8>> {
    if !(1..=64).contains(&width) {
        return Err(BqliteError::Execution(format!(
            "Delta::encode: residual_bit_width {width} out of valid range 1..=64"
        )));
    }
    let row_count = values.len();
    let expected_bytes = payload_byte_count(row_count, width);
    let mut out = vec![0u8; expected_bytes];
    let mut bit_pos: usize = 0;
    for window in values.windows(2) {
        let prev = window[0] as i128;
        let next = window[1] as i128;
        let residual = (next - prev) as i64;
        let z = zigzag_encode(residual);
        // Mask to width bits — the selector already validated that
        // every residual fits in `width`, so a mask above `width`
        // is defensive-only.
        let mask = if width == 64 {
            u64::MAX
        } else {
            (1u64 << width) - 1
        };
        let masked = z & mask;
        // Write `width` bits LSB-first starting at `bit_pos`.
        let mut remaining = width as usize;
        let mut value = masked;
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

/// Inverse of [`bit_pack_residuals`]: reconstruct the original
/// `values` vector from a bit-packed residual stream.
///
/// Takes `base_value` (from the chunk's params), the full
/// `row_count`, and `width`. Returns a `Vec<i64>` of length
/// `row_count` whose first element is `base_value` and whose
/// subsequent elements are `prev + zigzag_decode(residual)`.
fn bit_unpack_residuals(
    payload: &[u8],
    base_value: i64,
    row_count: usize,
    width: u8,
) -> Result<Vec<i64>> {
    if !(1..=64).contains(&width) {
        return Err(BqliteError::Execution(format!(
            "Delta::decode: residual_bit_width {width} out of valid range 1..=64"
        )));
    }
    let expected_bytes = payload_byte_count(row_count, width);
    if payload.len() != expected_bytes {
        return Err(BqliteError::Execution(format!(
            "Delta::decode: expected {expected_bytes} payload bytes for \
             {row_count} rows at {width}-bit residuals, got {}",
            payload.len()
        )));
    }
    let mut values = Vec::with_capacity(row_count);
    values.push(base_value);
    if row_count <= 1 {
        return Ok(values);
    }
    let mut bit_pos: usize = 0;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    for _ in 0..row_count - 1 {
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
            // `take` can reach 8 (whole byte), so compute the mask in
            // u32 to avoid the `1u8 << 8` overflow panic in debug builds.
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
        let residual = zigzag_decode(z);
        let prev = *values.last().unwrap() as i128;
        let next_i128 = prev + residual as i128;
        if next_i128 < i64::MIN as i128 || next_i128 > i64::MAX as i128 {
            return Err(BqliteError::Execution(format!(
                "Delta::decode: decoded value {next_i128} overflows i64 — \
                 segment corruption (a writer that produced this chunk \
                 already would have errored per segment-format-v1.md §9.3)"
            )));
        }
        values.push(next_i128 as i64);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampNanosecondArray};

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let delta = Delta;
        let chunk = delta.encode(array.as_ref()).unwrap();
        delta.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_int_and_timestamp_only() {
        let d = Delta;
        assert!(d.applicable_to(&BqlType::Int));
        assert!(d.applicable_to(&BqlType::Timestamp));
        assert!(!d.applicable_to(&BqlType::Bool));
        assert!(!d.applicable_to(&BqlType::Float));
        assert!(!d.applicable_to(&BqlType::String));
        assert!(!d.applicable_to(&BqlType::List(Box::new(BqlType::Int))));
    }

    #[test]
    fn encoding_type_is_delta_discriminant_two() {
        assert_eq!(Delta.encoding_type(), EncodingType::Delta);
        assert_eq!(Delta.encoding_type().discriminant(), 2);
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
        // Typical usage: monotonic-ish nanosecond timestamps within
        // an entity. Consecutive deltas are tiny (millisecond-scale)
        // so the bit width is small.
        let values: Vec<i64> = (0..16)
            .map(|i| 1_700_000_000_000_000_000 + i * 1_000_000)
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_single_element() {
        // Single-value arrays carry the value in `base_value` with
        // a zero-length residual payload (modulo SIMD padding).
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_constant_series() {
        // All-equal values produce an all-zero residual stream. Width
        // floors to 1 (zigzag(0) == 0 which has no significant bits,
        // but width 0 is undefined — the floor keeps bit_pack sane).
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 5]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_negative_deltas() {
        // Descending values — residuals are negative, zigzag maps
        // them to small positives.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![100_i64, 99, 97, 90, 70, 10]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_mixed_deltas() {
        // Mix of positive and negative deltas — tests sign handling
        // in both zigzag and the bit-pack accumulator.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![0_i64, 5, 3, 8, -2, -10, 0, 100]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_extrema() {
        // Values near i64 extrema with small deltas exercise the
        // sign extension of large base_values; the residuals stay
        // small so the width stays narrow.
        let array: ArrayRef =
            Arc::new(Int64Array::from(vec![i64::MAX - 2, i64::MAX - 1, i64::MAX]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn timestamp_round_trip_preserves_utc() {
        // TimestampNanosecondArray equality includes the timezone
        // metadata, so this also asserts that Delta::decode reapplies
        // the canonical "UTC" stamp.
        let array: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000_i64,
                1_700_000_000_000_001_000,
                1_700_000_000_000_002_500,
            ])
            .with_timezone("UTC"),
        );
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn encode_rejects_empty_array() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let err = Delta.encode(array.as_ref()).unwrap_err();
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
        let err = Delta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_overflow_residual() {
        // Consecutive residual `i64::MAX - i64::MIN` overflows i64 in
        // the i128 check. The writer must reject and let the selector
        // fall back.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX]));
        let err = Delta.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("overflows i64"), "unexpected: {msg}");
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn encode_rejects_unsupported_type() {
        // Float64 is not applicable to Delta; encode must reject it.
        use arrow::array::Float64Array;
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0]));
        let err = Delta.encode(array.as_ref()).unwrap_err();
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
        let err = Delta.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_params_length_mismatch() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Delta,
            params: vec![0u8; PARAMS_LEN - 1],
            payload: vec![0u8; 8],
            row_count: 1,
        };
        let err = Delta.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("params block")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_payload_length_mismatch() {
        // Encode a small run, then truncate the payload and expect a
        // well-typed error on decode.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3, 4]));
        let mut chunk = Delta.encode(array.as_ref()).unwrap();
        chunk.payload.pop(); // break the SIMD-aligned length
        let err = Delta.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("payload bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn estimate_size_matches_encoded_payload_length() {
        let values: Vec<i64> = (0..20).map(|i| i * 1_000).collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let estimated = Delta.estimate_size(array.as_ref()).unwrap();
        let encoded = Delta.encode(array.as_ref()).unwrap();
        assert_eq!(estimated, encoded.payload.len());
    }

    #[test]
    fn payload_is_padded_to_simd_lane_multiple() {
        // Every encoded payload is a multiple of SIMD_LANE_BYTES.
        let cases: &[Vec<i64>] = &[
            vec![42],
            vec![0, 1],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            (0..32).collect(),
        ];
        for values in cases {
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let chunk = Delta.encode(array.as_ref()).unwrap();
            assert_eq!(
                chunk.payload.len() % SIMD_LANE_BYTES,
                0,
                "payload length {} not a multiple of {SIMD_LANE_BYTES} for values {values:?}",
                chunk.payload.len(),
            );
        }
    }

    #[test]
    fn row_count_one_has_empty_residual_payload() {
        // Single-element arrays have zero residuals and the SIMD
        // padding formula `ceil(0 / 8) * 8 == 0` produces an empty
        // payload. That is correct: a decoder reading zero residuals
        // never executes the unpack loop, so no SIMD over-read can
        // happen and there is nothing to pad past. The base_value in
        // params carries the lone value.
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1234_i64]));
        let chunk = Delta.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.payload.len(), 0);
        let base_value = i64::from_le_bytes(chunk.params[..8].try_into().unwrap());
        assert_eq!(base_value, 1234);
        let decoded = Delta.decode(&chunk, &BqlType::Int).unwrap();
        let decoded_int = decoded.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(decoded_int.len(), 1);
        assert_eq!(decoded_int.value(0), 1234);
    }
}
