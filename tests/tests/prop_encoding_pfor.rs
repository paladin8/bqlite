//! Property tests for `bqlite_storage::encoding::Pfor`.
//!
//! Follows the FOR / BitPacking template: round-trip is the load-bearing
//! invariant, plus PFOR-specific invariants (params layout, per-block
//! header structure, `estimate_size` exactness, block-count arithmetic).
//!
//! PFOR claims `BqlType::Int` and `BqlType::Timestamp`. This module
//! tests both code paths using the shared dense-array strategies from
//! `bqlite_tests::strategies`.
//!
//! See `docs/design/storage/advanced-encodings.md` §6 and
//! `docs/design/storage/segment-format-v2.md` §5.5 for the byte-level
//! contract this test guards.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use bqlite_core::BqlType;
use bqlite_storage::{Encoding, EncodingType, Pfor};
use bqlite_tests::strategies::{arb_int64_array, arb_timestamp_array};
use proptest::prelude::*;

/// Helper that runs a value through `encode → decode` and returns the
/// decoded array. Consistent across every proptest invariant.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = Pfor
        .encode(array)
        .expect("Pfor::encode must accept every dense Int/Timestamp array");
    Pfor.decode(&chunk, ty)
        .expect("Pfor::decode must succeed for a chunk Pfor produced")
}

/// Parse the block_count from the encoding params (bytes 2..6, u32 LE).
fn parse_block_count(params: &[u8]) -> usize {
    assert_eq!(params.len(), 6, "PFOR params must be exactly 6 bytes");
    u32::from_le_bytes(params[2..6].try_into().unwrap()) as usize
}

// ── Round-trip invariants ──────────────────────────────────────────────────

proptest! {
    /// Round-trip invariant over `Int` arrays. `any::<i64>()` hits
    /// extrema often enough that full-range counter-examples surface
    /// with minimal shrinking.
    #[test]
    fn pfor_round_trip_int(array in arb_int64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant over `Timestamp` arrays. Guards the
    /// `with_timezone("UTC")` re-application on the decode side.
    #[test]
    fn pfor_round_trip_timestamp(array in arb_timestamp_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── Encoding-specific invariants ───────────────────────────────────────

    /// `estimate_size` is exact under PFOR — the selector (TASK-419)
    /// compares it against FOR/BitPacking's estimates to choose a
    /// winner, so an under- or over-estimate would break the comparison.
    #[test]
    fn pfor_estimate_size_is_exact(array in arb_int64_array()) {
        let estimated = Pfor.estimate_size(array.as_ref()).unwrap();
        let actual = Pfor.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// Params are always exactly 6 bytes (u16 block_size + u32 block_count).
    #[test]
    fn pfor_params_are_exactly_six_bytes(array in arb_int64_array()) {
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.params.len(), 6);
    }

    /// block_size in params is always 128.
    #[test]
    fn pfor_params_block_size_is_128(array in arb_int64_array()) {
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let block_size = u16::from_le_bytes(chunk.params[..2].try_into().unwrap());
        prop_assert_eq!(block_size, 128u16);
    }

    /// block_count is ceil(row_count / 128).
    #[test]
    fn pfor_block_count_is_ceil_of_row_count_over_128(array in arb_int64_array()) {
        let row_count = array.len();
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let block_count = parse_block_count(&chunk.params);
        let expected = row_count.div_ceil(128);
        prop_assert_eq!(block_count, expected,
            "block_count mismatch for {}-row array", row_count);
    }

    /// chunk.row_count tracks the input array length exactly.
    #[test]
    fn pfor_row_count_preserved(array in arb_int64_array()) {
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// Encoding discriminant is always `PFor = 9`.
    #[test]
    fn pfor_encoding_type_is_pfor(array in arb_int64_array()) {
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.encoding, EncodingType::PFor);
        prop_assert_eq!(chunk.encoding.discriminant(), 9u8);
    }

    /// Walk every block of the encoded payload and confirm the per-block
    /// header is well-formed and the byte-count arithmetic is consistent:
    /// `HEADER(11) + padded_8(ceil(block_len * main_width / 8)) +
    /// patch_count * 10`.
    #[test]
    fn pfor_every_block_header_structure_valid(array in arb_int64_array()) {
        let chunk = Pfor.encode(array.as_ref()).unwrap();
        let row_count = array.len();
        let block_count = parse_block_count(&chunk.params);
        let payload = &chunk.payload;
        let mut cursor = 0usize;

        for block_idx in 0..block_count {
            prop_assert!(cursor + 11 <= payload.len(),
                "block {} header truncated at cursor {}", block_idx, cursor);
            let main_width = payload[cursor + 8];
            prop_assert!((1..=64).contains(&main_width),
                "main_width {} out of 1..=64 range in block {}", main_width, block_idx);
            let patch_count = u16::from_le_bytes(
                payload[cursor + 9..cursor + 11].try_into().unwrap(),
            ) as usize;
            cursor += 11;

            let block_start = block_idx * 128;
            let block_len = (row_count - block_start).min(128);
            prop_assert!(patch_count <= block_len,
                "patch_count {} > block_len {} in block {}",
                patch_count, block_len, block_idx);

            let total_bits = block_len * main_width as usize;
            let packed_bytes = total_bits.div_ceil(8);
            let padded = packed_bytes.div_ceil(8) * 8;
            prop_assert_eq!(padded % 8, 0,
                "block {} packed_main not 8-byte aligned", block_idx);
            prop_assert!(cursor + padded <= payload.len(),
                "block {} packed_main truncated", block_idx);
            cursor += padded;

            let patch_bytes = patch_count * 10;
            prop_assert!(cursor + patch_bytes <= payload.len(),
                "block {} patch list truncated", block_idx);
            cursor += patch_bytes;
        }
        prop_assert_eq!(cursor, payload.len(),
            "trailing bytes after last block");
    }

    /// PFOR handles values near i64::MIN correctly — exercises the
    /// i128 intermediate arithmetic in the decode path.
    #[test]
    fn pfor_round_trip_with_values_near_i64_min(
        offset in 0_u64..=u32::MAX as u64,
    ) {
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

    /// Mixed-outlier round-trip: 127 small values + one `big` outlier per
    /// block. Covers the "patch_count = 1" path with arbitrary big value.
    #[test]
    fn pfor_round_trip_one_big_outlier(
        small_base in 0_i64..=1000,
        big in (i64::from(u16::MAX) + 1)..=i64::MAX,
    ) {
        let mut values: Vec<i64> = (0..127).map(|i| small_base + (i as i64 % 16)).collect();
        values.push(big);
        let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }
}

// ── Example tests for edge cases proptest might not hit ──────────────────────

/// Empty input → empty payload, block_count = 0.
#[test]
fn pfor_round_trip_empty() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.len(), 0);
    let chunk = Pfor.encode(array.as_ref()).unwrap();
    assert_eq!(chunk.payload.len(), 0);
    let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
    assert_eq!(bc, 0);
}

/// Full i64 range within a block → wide `main_width`, scalar decode path.
#[test]
fn pfor_round_trip_i64_extrema() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, 0, -1, 1]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Exactly 128 values → one full BitPacker4x block, no short tail.
#[test]
fn pfor_round_trip_exactly_one_full_block() {
    let values: Vec<i64> = (0..128).map(|i| 1000 + i).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
    let chunk = Pfor.encode(array.as_ref()).unwrap();
    let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
    assert_eq!(bc, 1);
}

/// 129 values → 1 full block (128) + 1 short block (1 value).
#[test]
fn pfor_round_trip_short_final_block_one_value() {
    let values: Vec<i64> = (0..129).map(|i| i * 100).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// 300 values → 3 blocks (128, 128, 44).
#[test]
fn pfor_round_trip_three_blocks() {
    let values: Vec<i64> = (0..300).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
    let chunk = Pfor.encode(array.as_ref()).unwrap();
    let bc = u32::from_le_bytes(chunk.params[2..6].try_into().unwrap());
    assert_eq!(bc, 3);
}

/// One big outlier per block across 128+1 values.
#[test]
fn pfor_round_trip_one_outlier_per_block() {
    let mut values: Vec<i64> = (0..127).map(|i| 100 + (i % 16) as i64).collect();
    values.push(i64::MAX - 1);
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Worst-case input for selector: wide range on every value. The frame
/// selector will pick `main_width = 64`, 0 patches — all-patched would
/// cost more. This is the "worst-case all-patched fallback" from the
/// task description: we verify the decoder still round-trips on the
/// input shape the selector sees when considering that fallback.
#[test]
fn pfor_round_trip_alternating_zero_and_i64_max() {
    let mut values: Vec<i64> = Vec::with_capacity(128);
    for i in 0..128 {
        values.push(if i % 2 == 0 { 0 } else { i64::MAX });
    }
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Timestamp round-trip preserves UTC timezone metadata.
#[test]
fn pfor_round_trip_timestamp_utc() {
    let base = 1_700_000_000_000_000_000_i64;
    let nanos: Vec<i64> = (0..300).map(|i| base + i * 1_000_000).collect();
    let array: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(nanos.clone()).with_timezone("UTC"));
    let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Single value round-trip (trivial short block).
#[test]
fn pfor_round_trip_single_value() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}
