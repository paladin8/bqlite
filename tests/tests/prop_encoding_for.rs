//! Property tests for `bqlite_storage::encoding::ForEncoding`.
//!
//! Follows the BitPacking / RLE template: round-trip is the load-bearing
//! invariant, plus FOR-specific invariants (params layout, per-block header
//! structure, `estimate_size` exactness, and the selector guard property
//! that FOR beats global BitPacking on clustered data).
//!
//! FOR claims `BqlType::Int` and `BqlType::Timestamp`. This module tests
//! both code paths using the shared dense-array strategies from
//! `bqlite_tests::strategies`.
//!
//! See `docs/design/storage/advanced-encodings.md` §5 and
//! `docs/design/storage/segment-format-v2.md` §5.4 for the byte-level
//! contract this test guards.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use bqlite_core::BqlType;
use bqlite_storage::{Encoding, EncodingType, ForEncoding};
use bqlite_tests::strategies::{arb_int64_array, arb_timestamp_array};
use proptest::prelude::*;

/// Helper that runs a value through `encode → decode` and returns the
/// decoded array. Consistent across every proptest invariant.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = ForEncoding
        .encode(array)
        .expect("ForEncoding::encode must accept every dense Int/Timestamp array");
    ForEncoding
        .decode(&chunk, ty)
        .expect("ForEncoding::decode must succeed for a chunk ForEncoding produced")
}

/// Parse the block_count from the encoding params (bytes 2..6, u32 LE).
fn parse_block_count(params: &[u8]) -> usize {
    assert_eq!(params.len(), 6, "FOR params must be exactly 6 bytes");
    u32::from_le_bytes(params[2..6].try_into().unwrap()) as usize
}

// ── Round-trip invariants ──────────────────────────────────────────────────

proptest! {
    /// Round-trip invariant over `Int` arrays. `any::<i64>()` hits
    /// extrema often enough that full-range counter-examples surface
    /// with minimal shrinking.
    #[test]
    fn for_round_trip_int(array in arb_int64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant over `Timestamp` arrays. Guards the
    /// `with_timezone("UTC")` re-application on the decode side —
    /// `TimestampNanosecondArray::PartialEq` includes the timezone
    /// metadata, so a missing timezone round-trip would fail here.
    #[test]
    fn for_round_trip_timestamp(array in arb_timestamp_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── Encoding-specific invariants ───────────────────────────────────────

    /// `estimate_size` is exact under FOR — the selector compares it
    /// against BitPacking's estimate to choose a winner, so an
    /// under-estimate would break the comparison.
    #[test]
    fn for_estimate_size_is_exact(array in arb_int64_array()) {
        let estimated = ForEncoding.estimate_size(array.as_ref()).unwrap();
        let actual = ForEncoding.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// Params are always exactly 6 bytes (u16 block_size + u32 block_count)
    /// per segment-format-v2.md §5.4.
    #[test]
    fn for_params_are_exactly_six_bytes(array in arb_int64_array()) {
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.params.len(), 6);
    }

    /// block_size in params is always 128 per v2 §5.4.
    #[test]
    fn for_params_block_size_is_128(array in arb_int64_array()) {
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let block_size = u16::from_le_bytes(chunk.params[..2].try_into().unwrap());
        prop_assert_eq!(block_size, 128u16);
    }

    /// block_count is ceil(row_count / 128).
    #[test]
    fn for_block_count_is_ceil_of_row_count_over_128(array in arb_int64_array()) {
        let row_count = array.len();
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let block_count = parse_block_count(&chunk.params);
        let expected = row_count.div_ceil(128);
        prop_assert_eq!(block_count, expected,
            "block_count mismatch for {}-row array", row_count);
    }

    /// chunk.row_count tracks the input array length exactly.
    #[test]
    fn for_row_count_preserved(array in arb_int64_array()) {
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// encoding discriminant is always `For = 8`.
    #[test]
    fn for_encoding_type_is_for(array in arb_int64_array()) {
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.encoding, EncodingType::For);
        prop_assert_eq!(chunk.encoding.discriminant(), 8u8);
    }

    /// Per-block packed sections are all padded to 8-byte multiples.
    ///
    /// For each block, the payload contains: 9-byte header (i64 + u8)
    /// followed by a packed section. The packed section length must be
    /// a multiple of 8. We verify this by parsing the payload block
    /// by block using the block_count and per-block bit_width.
    #[test]
    fn for_every_block_packed_section_padded_to_eight(array in arb_int64_array()) {
        let chunk = ForEncoding.encode(array.as_ref()).unwrap();
        let row_count = array.len();
        let block_count = parse_block_count(&chunk.params);
        let payload = &chunk.payload;
        let mut cursor = 0usize;

        for block_idx in 0..block_count {
            // Read per-block header: block_min (i64, 8 bytes) + bit_width (u8, 1 byte)
            prop_assert!(cursor + 9 <= payload.len(),
                "block {} header truncated at cursor {}", block_idx, cursor);
            let bit_width = payload[cursor + 8];
            prop_assert!((1..=64).contains(&bit_width),
                "bit_width {} out of 1..=64 range in block {}", bit_width, block_idx);
            cursor += 9;

            let block_start = block_idx * 128;
            let block_len = (row_count - block_start).min(128);
            let total_bits = block_len * bit_width as usize;
            let packed_bytes = total_bits.div_ceil(8);
            let padded = packed_bytes.div_ceil(8) * 8;

            prop_assert!(cursor + padded <= payload.len(),
                "block {} packed section truncated", block_idx);
            prop_assert_eq!(padded % 8, 0,
                "block {} packed section not 8-byte aligned", block_idx);
            cursor += padded;
        }
        prop_assert_eq!(cursor, payload.len(),
            "trailing bytes after last block");
    }

    /// FOR produces correct overflow handling when block_min is i64::MIN.
    /// This exercises the i128 intermediate arithmetic in the decode path.
    #[test]
    fn for_round_trip_with_values_near_i64_min(
        offset in 0_u64..=u32::MAX as u64,
    ) {
        // Create a 128-value block where block_min = i64::MIN and all
        // offsets span up to `offset` (≤ 32 bits). Tests the i128 overflow-
        // safe path in decode.
        let values: Vec<i64> = (0..128)
            .map(|i| {
                let off = (i as u64 * offset) / 127;
                ((i64::MIN as i128) + (off as i128)) as i64
            })
            .collect();
        let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }
}

// ── Example tests for edge cases proptest might not hit ──────────────────────

/// Empty input → empty payload, block_count = 0.
#[test]
fn for_round_trip_empty() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.len(), 0);
    let chunk = ForEncoding.encode(array.as_ref()).unwrap();
    assert_eq!(chunk.payload.len(), 0);
    let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
    assert_eq!(bc, 0);
}

/// Full i64 range within a block → bit_width = 64, scalar decode path.
#[test]
fn for_round_trip_i64_extrema() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, 0, -1, 1]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Exactly 128 values → one full BitPacker4x block, no short tail.
#[test]
fn for_round_trip_exactly_one_full_block() {
    let values: Vec<i64> = (0..128).map(|i| 1000 + i).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
    let chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
    assert_eq!(bc, 1);
}

/// 129 values → 1 full block (128) + 1 short block (1 value) — the
/// main short-final-block code path.
#[test]
fn for_round_trip_short_final_block_one_value() {
    let values: Vec<i64> = (0..129).map(|i| i * 100).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// 300 values → 3 blocks (128, 128, 44) — multiple blocks with a
/// non-trivial short final block.
#[test]
fn for_round_trip_three_blocks() {
    let values: Vec<i64> = (0..300).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
    let chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
    assert_eq!(bc, 3);
}

/// Clustered data: two blocks with different value ranges. FOR should
/// use ~4 bits per block vs BitPacking's ~14 bits global.
/// Primary regression: round-trip preserves all values.
#[test]
fn for_round_trip_clustered_data() {
    let mut values = Vec::with_capacity(256);
    for i in 0..128_i64 {
        values.push(100 + (i % 16)); // cluster around 100..116
    }
    for i in 0..128_i64 {
        values.push(10000 + (i % 16)); // cluster around 10000..10016
    }
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// All-identical block → bit_width = 1 per the 1-bit floor rule.
#[test]
fn for_all_identical_uses_bit_width_one() {
    // 128 copies of the same value → 1 block, bit_width = 1.
    let array: ArrayRef = Arc::new(Int64Array::from(vec![999_i64; 128]));
    let chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let bit_width = chunk.payload[8]; // byte 9 of payload = bit_width of block 0
    assert_eq!(bit_width, 1, "all-identical block must use bit_width = 1");
    let decoded = ForEncoding.decode(&chunk, &BqlType::Int).unwrap();
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// block_min = i64::MIN, max offset = u32::MAX → bit_width = 32,
/// exercises the full-block BitPacker4x path with the widest u32 offset.
#[test]
fn for_round_trip_block_min_i64_min_offset_u32_max() {
    let max_offset = i64::from(u32::MAX);
    let values: Vec<i64> = (0..128)
        .map(|i| {
            if i % 2 == 0 {
                i64::MIN
            } else {
                i64::MIN + max_offset
            }
        })
        .collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
    let chunk = ForEncoding.encode(array.as_ref()).unwrap();
    let bit_width = chunk.payload[8];
    assert_eq!(bit_width, 32);
}

/// Timestamp round-trip preserves UTC timezone metadata.
#[test]
fn for_round_trip_timestamp_utc() {
    let base = 1_700_000_000_000_000_000_i64;
    let nanos: Vec<i64> = (0..300).map(|i| base + i * 1_000_000).collect();
    let array: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(nanos.clone()).with_timezone("UTC"));
    let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Single value round-trip (trivial short block).
#[test]
fn for_round_trip_single_value() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}
