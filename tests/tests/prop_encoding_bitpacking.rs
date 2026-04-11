//! Property tests for `bqlite_storage::encoding::BitPacking`.
//!
//! Copied from the TASK-206 `prop_encoding_plain.rs` template and
//! narrowed to the two primitive types BitPacking supports: `Int`
//! and `Timestamp`. Round-trip is the load-bearing invariant; the
//! encoding-specific properties assert BitPacking's frame-of-
//! reference contract (bit width exactly fits the chunk's range,
//! payload byte count is padded to a multiple of 8, and the
//! estimator reports the exact encoded size).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, TimestampNanosecondArray};
use bqlite_core::BqlType;
use bqlite_storage::{BitPacking, Encoding};
use bqlite_tests::strategies::{arb_int64_array, arb_timestamp_array};
use proptest::prelude::*;

/// Helper mirroring the Plain template. Kept out of `proptest!`
/// blocks so the encode / decode calls are consistent across every
/// invariant.
fn round_trip(array: &dyn Array, ty: &BqlType) -> ArrayRef {
    let chunk = BitPacking
        .encode(array)
        .expect("BitPacking::encode must accept every dense Int/Timestamp array");
    BitPacking
        .decode(&chunk, ty)
        .expect("BitPacking::decode must succeed for a chunk BitPacking produced")
}

proptest! {
    /// Round-trip invariant over `Int` arrays. `any::<i64>()` hits
    /// extrema often enough that full-range counter-examples surface
    /// with tiny shrinks.
    #[test]
    fn bitpacking_round_trip_int(array in arb_int64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant over `Timestamp` arrays. Guards the
    /// `with_timezone("UTC")` re-application on the decode side —
    /// `TimestampNanosecondArray::PartialEq` includes the timezone
    /// metadata, so a missing timezone round-trip would fail here.
    #[test]
    fn bitpacking_round_trip_timestamp(array in arb_timestamp_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `estimate_size` is exact under v1 BitPacking — the selector
    /// benefits from exact byte counts and the impl commits to it.
    #[test]
    fn bitpacking_estimate_size_is_exact(array in arb_int64_array()) {
        let estimated = BitPacking.estimate_size(array.as_ref()).unwrap();
        let actual = BitPacking.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// Payload byte count is always a multiple of 8 — `segment-format-v1.md`
    /// §9.4 pins this for FastLanes-compatible SIMD tail reads.
    #[test]
    fn bitpacking_payload_is_padded_to_eight_bytes(array in arb_int64_array()) {
        let chunk = BitPacking.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.payload.len() % 8, 0);
    }

    /// Encoding header is always exactly 9 bytes (i64 min_value +
    /// u8 bit_width).
    #[test]
    fn bitpacking_params_are_exactly_nine_bytes(array in arb_int64_array()) {
        let chunk = BitPacking.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.params.len(), 9);
    }

    /// `bit_width` lives in `1..=64` per §9.4. In particular,
    /// `bit_width == 0` is illegal — the `max(1, ..)` floor in
    /// `select_frame` guarantees this even for empty or
    /// all-identical chunks.
    #[test]
    fn bitpacking_bit_width_is_in_one_to_sixtyfour(array in arb_int64_array()) {
        let chunk = BitPacking.encode(array.as_ref()).unwrap();
        let bit_width = chunk.params[8]; // byte 9 (after i64 min_value)
        prop_assert!((1..=64).contains(&bit_width));
    }

    /// `chunk.row_count` tracks the input array length exactly.
    #[test]
    fn bitpacking_row_count_preserved(array in arb_int64_array()) {
        let chunk = BitPacking.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }
}

// ── Example tests for edge cases proptest might miss ─────────────────────

/// Empty input → empty payload, default frame. Useful regression
/// coverage because the `select_frame` fast path for empties is
/// the only one not exercised by `arb_int64_array` (which has
/// `0..=32` as its size range and rarely shrinks to exactly zero
/// with non-flake default case counts).
#[test]
fn bitpacking_round_trip_empty() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.len(), 0);
}

/// Full i64 range — forces `bit_width = 64`, the widest path.
/// Regression test for the `chunk_mask` branch in `write_bits` /
/// `read_bits` that special-cases `width == 64` to avoid shifting
/// by the full word size (which is undefined behavior in Rust).
#[test]
fn bitpacking_round_trip_i64_extrema() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, 0, -1, 1]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Narrow range (3 bits) with 17 elements — exercises the byte-
/// boundary crossing at bit offsets 24, 48, 72, 96 etc. Regression
/// test for `write_bits` / `read_bits` bit-boundary walks.
#[test]
fn bitpacking_round_trip_narrow_range_crosses_byte_boundaries() {
    let values: Vec<i64> = (0..17_i64).map(|i| 10 + (i % 8)).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Timestamp chunk with nearly-constant intervals — the canonical
/// BitPacking use case documented in `segment-format-v1.md` §9.4
/// ("for narrow-range integer columns"). Exercises 1-bit-wide
/// packing on the deltas.
#[test]
fn bitpacking_round_trip_timestamp_near_constant_intervals() {
    let base = 1_700_000_000_000_000_000_i64;
    let nanos: Vec<i64> = (0..32).map(|i| base + i).collect();
    let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("UTC"));
    let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
    assert_eq!(decoded.as_ref(), array.as_ref());
}
