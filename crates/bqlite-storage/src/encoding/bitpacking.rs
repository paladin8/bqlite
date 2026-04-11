//! BitPacking encoding — variable-width packed integers with a
//! frame-of-reference base.
//!
//! BitPacking is the `Int` / `Timestamp` space optimiser: every value
//! is stored as an unsigned offset from a per-chunk `min_value` using
//! the minimum number of bits required to represent the chunk's
//! numeric range. The byte layout is pinned by
//! `docs/design/storage/segment-format-v1.md` §9.4:
//!
//! ```text
//! encoding_params:
//!     min_value: i64 LE          // subtracted from each value before packing
//!     bit_width: u8              // 1..=64
//! payload:
//!     packed bit stream of `row_count` unsigned offsets, LSB-first
//!     within each byte, padded up to the next multiple of 8 bytes
//!     so a SIMD decoder can read one full 8-byte lane past the last
//!     code without a bounds check.
//! ```
//!
//! # Bit width selection
//!
//! For every non-empty chunk the writer computes
//! `max_offset = max_i((value_i as i128) - (min_value as i128))`
//! and picks
//! `bit_width = max(1, 64 - max_offset.leading_zeros())`.
//!
//! The `max(1, …)` floor matches the `1..=64` range the format
//! specifies — `bit_width = 0` is not a legal v1 wire value. An
//! all-identical input (every value equal to `min_value`) therefore
//! encodes at `bit_width = 1` with a payload of zero bits (padded up
//! to 8 bytes). The selector (TASK-212) would normally route such a
//! chunk to [`Constant`](super::constant::Constant); BitPacking still
//! handles it correctly if called.
//!
//! # Type support
//!
//! BitPacking applies to `Int` (`Int64Array`) and `Timestamp`
//! (`TimestampNanosecondArray`). `Bool`, `Float`, and string types
//! are not applicable — `Bool` is already packed by Plain,
//! `Float` has no monotonic total order under `i128` subtraction,
//! and strings are handled by `Dictionary` / `Plain`. Nested types
//! are deferred.
//!
//! # v1 implementation notes
//!
//! `segment-format-v1.md` §9.4 mentions "SIMD-accelerated unpacking
//! via the `bitpacking` crate's FastLanes layout". The v1 BitPacking
//! impl lands a **portable, non-SIMD** packer/unpacker to keep the
//! dep surface minimal. The byte layout it produces is compatible
//! with any LSB-first bit-packed decoder — a later performance task
//! can swap the hot loop for `bitpacking::BitPacker4x` without
//! changing the on-disk bytes. The 8-byte tail padding documented
//! in §9.4 is already emitted, so the SIMD swap is strictly
//! additive.

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use bqlite_core::{BqlType, BqliteError, Result};
use std::sync::Arc;

use super::{require_dense, EncodedChunk, Encoding, EncodingType};

/// Zero-sized marker for the BitPacking encoding.
#[derive(Debug, Clone, Copy, Default)]
pub struct BitPacking;

impl BitPacking {
    /// Construct a new BitPacking encoder. BitPacking has no
    /// configuration — this is sugar for `BitPacking` the unit
    /// struct.
    pub const fn new() -> Self {
        BitPacking
    }
}

/// Width of the `min_value` parameter on disk.
const MIN_VALUE_BYTE_WIDTH: usize = 8;
/// Width of the `bit_width` parameter on disk.
const BIT_WIDTH_BYTE_WIDTH: usize = 1;
/// Total encoding-header parameter block size.
const PARAMS_BYTE_WIDTH: usize = MIN_VALUE_BYTE_WIDTH + BIT_WIDTH_BYTE_WIDTH;
/// FastLanes tail-padding granularity. §9.4 pins this at 8 bytes so
/// a future SIMD decoder can read one full u64 lane past the last
/// packed code without a bounds check.
const PADDING_GRANULARITY: usize = 8;

impl Encoding for BitPacking {
    fn encoding_type(&self) -> EncodingType {
        EncodingType::BitPacking
    }

    fn applicable_to(&self, ty: &BqlType) -> bool {
        matches!(ty, BqlType::Int | BqlType::Timestamp)
    }

    fn estimate_size(&self, array: &dyn Array) -> Result<usize> {
        let values = values_as_i64(array)?;
        Ok(estimated_payload_bytes(values))
    }

    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk> {
        require_dense(array, "BitPacking")?;
        let values = values_as_i64(array)?;
        let row_count = array.len();

        let (min_value, bit_width) = select_frame(values);
        let payload = pack_offsets(values, min_value, bit_width);

        let mut params = Vec::with_capacity(PARAMS_BYTE_WIDTH);
        params.extend_from_slice(&min_value.to_le_bytes());
        params.push(bit_width);

        Ok(EncodedChunk {
            encoding: EncodingType::BitPacking,
            params,
            payload,
            row_count,
        })
    }

    fn decode(&self, chunk: &EncodedChunk, ty: &BqlType) -> Result<ArrayRef> {
        if chunk.encoding != EncodingType::BitPacking {
            return Err(BqliteError::Execution(format!(
                "BitPacking::decode called on a {:?} chunk — dispatch must \
                 route each chunk to its declared encoding's decoder",
                chunk.encoding
            )));
        }
        if chunk.params.len() != PARAMS_BYTE_WIDTH {
            return Err(BqliteError::Execution(format!(
                "BitPacking::decode: encoding header must be exactly {} bytes \
                 (i64 min_value + u8 bit_width), got {}",
                PARAMS_BYTE_WIDTH,
                chunk.params.len()
            )));
        }

        let min_value =
            i64::from_le_bytes(chunk.params[..MIN_VALUE_BYTE_WIDTH].try_into().unwrap());
        let bit_width = chunk.params[MIN_VALUE_BYTE_WIDTH];

        if bit_width == 0 || bit_width > 64 {
            return Err(BqliteError::Execution(format!(
                "BitPacking::decode: bit_width must be in 1..=64 per \
                 segment-format-v1.md §9.4, got {bit_width}"
            )));
        }

        let expected_len = payload_byte_len(chunk.row_count, bit_width);
        if chunk.payload.len() != expected_len {
            return Err(BqliteError::Execution(format!(
                "BitPacking::decode: expected {expected_len} payload bytes for \
                 {} rows × {bit_width}-bit codes (padded to multiple of {}), got {}",
                chunk.row_count,
                PADDING_GRANULARITY,
                chunk.payload.len()
            )));
        }

        let offsets = unpack_offsets(&chunk.payload, chunk.row_count, bit_width);
        let values: Vec<i64> = offsets
            .into_iter()
            .map(|offset| {
                // `min_value + offset` always fits in i64 because
                // encode picked `min_value` as the true minimum and
                // the largest offset was `max_value − min_value`,
                // which is ≤ u64::MAX. Reconstruct via i128 so the
                // intermediate arithmetic never overflows.
                ((min_value as i128) + (offset as i128)) as i64
            })
            .collect();

        match ty {
            BqlType::Int => Ok(Arc::new(Int64Array::from(values)) as ArrayRef),
            BqlType::Timestamp => Ok(Arc::new(
                TimestampNanosecondArray::from(values).with_timezone("UTC"),
            ) as ArrayRef),
            other => Err(BqliteError::Execution(format!(
                "BitPacking::decode does not support type {other} — v1 \
                 BitPacking decodes to Int or Timestamp only (segment-format-v1.md §9.4)"
            ))),
        }
    }
}

// ── i64 accessor ────────────────────────────────────────────────────────────

/// Extract an `&[i64]` view over an Int64Array or TimestampNanosecond
/// array. BitPacking treats both as signed 64-bit integers.
fn values_as_i64(array: &dyn Array) -> Result<&[i64]> {
    match array.data_type() {
        DataType::Int64 => {
            let arr = downcast::<Int64Array>(array, "Int64Array")?;
            Ok(arr.values())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = downcast::<TimestampNanosecondArray>(array, "TimestampNanosecondArray")?;
            Ok(arr.values())
        }
        DataType::List(_) | DataType::Map(_, _) => Err(BqliteError::Execution(format!(
            "BitPacking: nested type {:?} is deferred — v1 BitPacking encoding \
             covers Int64 and Timestamp(Nanosecond) only; List and Map land in \
             a later Wave 2 task",
            array.data_type()
        ))),
        other => Err(BqliteError::Execution(format!(
            "BitPacking: unsupported Arrow type {other:?} — v1 BitPacking \
             encoding covers Int64 and Timestamp(Nanosecond) only"
        ))),
    }
}

// ── frame selection ────────────────────────────────────────────────────────

/// Pick `(min_value, bit_width)` for a chunk of i64 values. For an
/// empty chunk both fields default to sentinel values the decoder
/// handles correctly (min_value = 0, bit_width = 1).
fn select_frame(values: &[i64]) -> (i64, u8) {
    if values.is_empty() {
        return (0, 1);
    }
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

/// Payload byte count per §9.4: packed bit stream rounded up to the
/// next multiple of [`PADDING_GRANULARITY`] (8 bytes).
fn payload_byte_len(row_count: usize, bit_width: u8) -> usize {
    if row_count == 0 {
        return 0;
    }
    let total_bits = row_count.saturating_mul(bit_width as usize);
    let packed_bytes = total_bits.div_ceil(8);
    packed_bytes.div_ceil(PADDING_GRANULARITY) * PADDING_GRANULARITY
}

/// Upper-bound payload estimate the selector uses — exact under v1.
fn estimated_payload_bytes(values: &[i64]) -> usize {
    let (_, bit_width) = select_frame(values);
    payload_byte_len(values.len(), bit_width)
}

// ── bit packing / unpacking ─────────────────────────────────────────────────

/// Pack `values` into a bit stream, LSB-first within each byte.
///
/// Offsets are computed as `(value − min_value) as u64` via `i128`
/// intermediate arithmetic so subtraction never overflows. Each
/// offset is fit into exactly `bit_width` bits; the highest bit of
/// each offset is guaranteed zero because `bit_width` was chosen to
/// cover the largest offset in the chunk.
fn pack_offsets(values: &[i64], min_value: i64, bit_width: u8) -> Vec<u8> {
    let row_count = values.len();
    let mut bytes = vec![0u8; payload_byte_len(row_count, bit_width)];
    if row_count == 0 || bit_width == 0 {
        return bytes;
    }
    let width = bit_width as usize;
    for (i, &value) in values.iter().enumerate() {
        let offset = ((value as i128) - (min_value as i128)) as u64;
        write_bits(&mut bytes, i * width, width, offset);
    }
    bytes
}

/// Unpack `row_count` bit-packed values out of `bytes`.
fn unpack_offsets(bytes: &[u8], row_count: usize, bit_width: u8) -> Vec<u64> {
    let mut offsets = Vec::with_capacity(row_count);
    if row_count == 0 {
        return offsets;
    }
    let width = bit_width as usize;
    for i in 0..row_count {
        offsets.push(read_bits(bytes, i * width, width));
    }
    offsets
}

/// Write `width` bits of `value` at bit offset `start` into `out`,
/// LSB-first within each byte. `width` must be ≤ 64 and
/// `value >> width` must be zero.
///
/// Each iteration writes at most 8 bits (`chunk_bits <= 8`) because
/// the step size is bounded by `8 - bit_in_byte`. The `chunk_mask`
/// computation therefore never needs to special-case a 64-bit
/// shift.
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
/// within each byte. `width` must be in 1..=64.
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

// ── downcast helper ─────────────────────────────────────────────────────────

fn downcast<'a, T: 'static>(array: &'a dyn Array, expected: &'static str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        BqliteError::Execution(format!(
            "BitPacking encoding: expected Arrow array of type {expected}, \
             got data_type = {:?}",
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(array: ArrayRef, ty: BqlType) -> ArrayRef {
        let chunk = BitPacking.encode(array.as_ref()).unwrap();
        BitPacking.decode(&chunk, &ty).unwrap()
    }

    #[test]
    fn applicable_to_covers_int_and_timestamp_only() {
        let b = BitPacking;
        assert!(b.applicable_to(&BqlType::Int));
        assert!(b.applicable_to(&BqlType::Timestamp));
        assert!(!b.applicable_to(&BqlType::Bool));
        assert!(!b.applicable_to(&BqlType::Float));
        assert!(!b.applicable_to(&BqlType::String));
    }

    #[test]
    fn encoding_type_is_bitpacking_discriminant_four() {
        assert_eq!(BitPacking.encoding_type(), EncodingType::BitPacking);
        assert_eq!(BitPacking.encoding_type().discriminant(), 4);
    }

    #[test]
    fn frame_selection_narrow_range_yields_small_bit_width() {
        let (min, width) = select_frame(&[10_i64, 12, 11, 15, 10]);
        assert_eq!(min, 10);
        // max_offset = 5 → leading zeros 61 → width 3
        assert_eq!(width, 3);
    }

    #[test]
    fn frame_selection_all_identical_yields_width_one() {
        let (min, width) = select_frame(&[42_i64; 8]);
        assert_eq!(min, 42);
        assert_eq!(width, 1);
    }

    #[test]
    fn frame_selection_full_i64_range_yields_width_sixtyfour() {
        let (min, width) = select_frame(&[i64::MIN, i64::MAX]);
        assert_eq!(min, i64::MIN);
        assert_eq!(width, 64);
    }

    #[test]
    fn payload_byte_len_pads_to_multiple_of_eight() {
        // 1 row × 3 bits = 3 bits → 1 byte → padded to 8.
        assert_eq!(payload_byte_len(1, 3), 8);
        // 8 rows × 8 bits = 64 bits → 8 bytes → already aligned.
        assert_eq!(payload_byte_len(8, 8), 8);
        // 9 rows × 8 bits = 72 bits → 9 bytes → padded to 16.
        assert_eq!(payload_byte_len(9, 8), 16);
        // 0 rows → 0 bytes (no padding needed).
        assert_eq!(payload_byte_len(0, 1), 0);
    }

    #[test]
    fn int_round_trip_narrow_range() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 12, 11, 15, 10, 13, 14, 11]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_full_range() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![
            i64::MIN,
            0,
            i64::MAX,
            -1,
            1,
            i64::MIN + 1,
            i64::MAX - 1,
        ]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn int_round_trip_all_identical() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64; 16]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
        // Verify the selected bit_width is 1 (not 0) per §9.4.
        let chunk = BitPacking.encode(array.as_ref()).unwrap();
        assert_eq!(chunk.params[MIN_VALUE_BYTE_WIDTH], 1);
    }

    #[test]
    fn int_round_trip_empty() {
        let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let decoded = round_trip(array, BqlType::Int);
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn int_round_trip_single_value() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64]));
        let decoded = round_trip(array.clone(), BqlType::Int);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn timestamp_round_trip_has_utc_timezone() {
        let nanos = vec![
            1_700_000_000_000_000_000_i64,
            1_700_000_000_000_000_001,
            1_700_000_000_000_000_002,
            1_700_000_000_000_000_003,
        ];
        let array: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(nanos.clone()).with_timezone("UTC"));
        let decoded = round_trip(array.clone(), BqlType::Timestamp);
        assert_eq!(decoded.as_ref(), array.as_ref());
    }

    #[test]
    fn estimate_size_matches_encoded_payload() {
        let b = BitPacking;
        for values in [
            vec![10_i64, 12, 11, 15, 10],
            vec![42_i64; 16],
            vec![i64::MIN, 0, i64::MAX],
            vec![],
        ] {
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            let estimated = b.estimate_size(array.as_ref()).unwrap();
            let actual = b.encode(array.as_ref()).unwrap().payload.len();
            assert_eq!(
                estimated, actual,
                "estimate mismatch for values: {values:?}"
            );
        }
    }

    #[test]
    fn encode_rejects_nullable_input() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(3)]));
        let err = BitPacking.encode(array.as_ref()).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("null_count")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_wrong_encoding_discriminant() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Plain,
            params: vec![0u8; PARAMS_BYTE_WIDTH],
            payload: Vec::new(),
            row_count: 0,
        };
        let err = BitPacking.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Plain")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_malformed_params_length() {
        let chunk = EncodedChunk {
            encoding: EncodingType::BitPacking,
            params: vec![0u8; 5], // too short
            payload: Vec::new(),
            row_count: 0,
        };
        let err = BitPacking.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("9 bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_bit_width_zero() {
        let mut params = vec![0u8; MIN_VALUE_BYTE_WIDTH];
        params.push(0u8); // bit_width = 0
        let chunk = EncodedChunk {
            encoding: EncodingType::BitPacking,
            params,
            payload: vec![0u8; 8],
            row_count: 1,
        };
        let err = BitPacking.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("1..=64")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_truncated_payload() {
        // 2 rows × 8 bits = 16 bits → padded to 8 bytes.
        // Provide 7 bytes instead of 8.
        let mut params = 0_i64.to_le_bytes().to_vec();
        params.push(8u8); // bit_width = 8
        let chunk = EncodedChunk {
            encoding: EncodingType::BitPacking,
            params,
            payload: vec![0u8; 7],
            row_count: 2,
        };
        let err = BitPacking.decode(&chunk, &BqlType::Int).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("expected 8 payload bytes")),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn write_read_bits_round_trip_every_width() {
        for width in 1..=64usize {
            // Encode 5 values, each fitting in `width` bits.
            let mask = if width == 64 {
                u64::MAX
            } else {
                (1u64 << width) - 1
            };
            let values: Vec<u64> = (0..5)
                .map(|i| (0x0123_4567_89AB_CDEF_u64.wrapping_mul(i + 1)) & mask)
                .collect();
            let byte_count = (5 * width).div_ceil(8).div_ceil(8) * 8;
            let mut bytes = vec![0u8; byte_count];
            for (i, &v) in values.iter().enumerate() {
                write_bits(&mut bytes, i * width, width, v);
            }
            let decoded: Vec<u64> = (0..5)
                .map(|i| read_bits(&bytes, i * width, width))
                .collect();
            assert_eq!(decoded, values, "mismatch at width {width}");
        }
    }
}
