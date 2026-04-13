//! Property tests for `bqlite_storage::encoding::Rle`.
//!
//! Follows the Plain / Dictionary template: round-trip is the
//! load-bearing invariant, plus RLE-specific invariants
//! (`run_ends` monotonicity, `estimate_size` exactness).
//!
//! Rle claims `BqlType::Bool`, `BqlType::Int`, `BqlType::String`, and
//! `BqlType::Timestamp`. This module tests all four code paths using
//! the shared dense-array strategies from `bqlite_tests::strategies`.
//!
//! See `docs/design/storage/advanced-encodings.md` §3 for the
//! byte-level contract this test guards.

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringViewArray, TimestampNanosecondArray};
use bqlite_core::BqlType;
use bqlite_storage::{Encoding, Rle};
use bqlite_tests::strategies::{
    arb_bool_array, arb_int64_array, arb_string_array, arb_timestamp_array,
};
use proptest::prelude::*;
use std::sync::Arc;

/// Helper that runs one value through `encode → decode` and returns
/// the decoded array. Every round-trip test calls this so the `encode`
/// and `decode` invocations stay consistent across cases.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = Rle
        .encode(array)
        .expect("Rle::encode must accept every dense Bool/Int/String/Timestamp array");
    Rle.decode(&chunk, ty)
        .expect("Rle::decode must succeed for a chunk Rle produced")
}

/// Parse `run_count` from the params block and extract the `run_ends`
/// slice from the start of the payload. Returns `(run_count, run_ends)`.
fn parse_run_ends(params: &[u8], payload: &[u8]) -> (usize, Vec<u32>) {
    assert_eq!(params.len(), 4, "params must be exactly 4 bytes");
    let run_count = u32::from_le_bytes(params[..4].try_into().unwrap()) as usize;
    let run_ends: Vec<u32> = (0..run_count)
        .map(|i| {
            let off = i * 4;
            u32::from_le_bytes(payload[off..off + 4].try_into().unwrap())
        })
        .collect();
    (run_count, run_ends)
}

proptest! {
    // ── Round-trip invariants ─────────────────────────────────────────

    /// Round-trip invariant on `BqlType::Bool`.
    #[test]
    fn rle_round_trip_bool(array in arb_bool_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Bool);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on `BqlType::Int`. Covers the full `i64`
    /// domain so sign-bit and two's-complement bugs in the `[i64 LE]`
    /// value encoding surface with minimal counter-examples.
    #[test]
    fn rle_round_trip_int(array in arb_int64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on `BqlType::String`. Uses the
    /// schema-canonical `StringViewArray` shape so input and output
    /// types agree under Arrow's `PartialEq`.
    #[test]
    fn rle_round_trip_string(array in arb_string_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::String);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant on `BqlType::Timestamp`. Guards the
    /// `with_timezone("UTC")` re-application in `decode` — Arrow's
    /// `PartialEq` includes the timezone annotation, so a missing
    /// marker would fail here.
    #[test]
    fn rle_round_trip_timestamp(array in arb_timestamp_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    // ── estimate_size exactness ───────────────────────────────────────
    //
    // RLE's estimator traverses the array once to count runs and compute
    // the exact payload byte count — so we assert equality, not just
    // `estimate >= actual`.

    /// `estimate_size == encode().payload.len()` for Bool.
    #[test]
    fn rle_estimate_size_is_exact_bool(array in arb_bool_array()) {
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let actual = Rle.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// `estimate_size == encode().payload.len()` for Int.
    #[test]
    fn rle_estimate_size_is_exact_int(array in arb_int64_array()) {
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let actual = Rle.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// `estimate_size == encode().payload.len()` for String. The
    /// variable-length `[u32 LE len][bytes]` value encoding is the most
    /// error-prone estimator path; shrinking reliably pinpoints byte
    /// miscounts here.
    #[test]
    fn rle_estimate_size_is_exact_string(array in arb_string_array()) {
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let actual = Rle.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// `estimate_size == encode().payload.len()` for Timestamp.
    #[test]
    fn rle_estimate_size_is_exact_timestamp(array in arb_timestamp_array()) {
        let estimated = Rle.estimate_size(array.as_ref()).unwrap();
        let actual = Rle.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    // ── run_ends monotonicity ─────────────────────────────────────────
    //
    // `run_ends[i]` must be strictly increasing and `run_ends.last()`
    // must equal `row_count`. Strictly increasing guarantees runs are
    // non-empty. The terminal-value check ensures no rows are phantom.

    /// `run_ends` monotonicity for Int.
    #[test]
    fn rle_run_ends_monotone_int(array in arb_int64_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        let (run_count, run_ends) = parse_run_ends(&chunk.params, &chunk.payload);
        if run_count == 0 {
            prop_assert_eq!(array.len(), 0);
            return Ok(());
        }
        for w in run_ends.windows(2) {
            prop_assert!(
                w[0] < w[1],
                "run_ends must be strictly increasing; got consecutive [{}, {}]",
                w[0], w[1]
            );
        }
        prop_assert_eq!(*run_ends.last().unwrap() as usize, chunk.row_count);
    }

    /// `run_ends` monotonicity for Bool.
    #[test]
    fn rle_run_ends_monotone_bool(array in arb_bool_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        let (run_count, run_ends) = parse_run_ends(&chunk.params, &chunk.payload);
        if run_count == 0 {
            prop_assert_eq!(array.len(), 0);
            return Ok(());
        }
        for w in run_ends.windows(2) {
            prop_assert!(
                w[0] < w[1],
                "run_ends must be strictly increasing; got consecutive [{}, {}]",
                w[0], w[1]
            );
        }
        prop_assert_eq!(*run_ends.last().unwrap() as usize, chunk.row_count);
    }

    /// `run_ends` monotonicity for String.
    #[test]
    fn rle_run_ends_monotone_string(array in arb_string_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        let (run_count, run_ends) = parse_run_ends(&chunk.params, &chunk.payload);
        if run_count == 0 {
            prop_assert_eq!(array.len(), 0);
            return Ok(());
        }
        for w in run_ends.windows(2) {
            prop_assert!(
                w[0] < w[1],
                "run_ends must be strictly increasing; got consecutive [{}, {}]",
                w[0], w[1]
            );
        }
        prop_assert_eq!(*run_ends.last().unwrap() as usize, chunk.row_count);
    }

    /// `run_ends` monotonicity for Timestamp. Timestamp shares the
    /// `collect_i64_runs` path with Int, so this catches regressions in
    /// that shared path when called via the Timestamp branch.
    #[test]
    fn rle_run_ends_monotone_timestamp(array in arb_timestamp_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        let (run_count, run_ends) = parse_run_ends(&chunk.params, &chunk.payload);
        if run_count == 0 {
            prop_assert_eq!(array.len(), 0);
            return Ok(());
        }
        for w in run_ends.windows(2) {
            prop_assert!(
                w[0] < w[1],
                "run_ends must be strictly increasing; got consecutive [{}, {}]",
                w[0], w[1]
            );
        }
        prop_assert_eq!(*run_ends.last().unwrap() as usize, chunk.row_count);
    }

    // ── row_count preservation ────────────────────────────────────────

    /// `chunk.row_count == array.len()` for Bool.
    #[test]
    fn rle_row_count_preserved_bool(array in arb_bool_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// `chunk.row_count == array.len()` for Int.
    #[test]
    fn rle_row_count_preserved_int(array in arb_int64_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// `chunk.row_count == array.len()` for String.
    #[test]
    fn rle_row_count_preserved_string(array in arb_string_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// `chunk.row_count == array.len()` for Timestamp.
    #[test]
    fn rle_row_count_preserved_timestamp(array in arb_timestamp_array()) {
        let chunk = Rle.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }
}

// ── Example tests covering edge cases proptest might miss ───────────────

/// Empty Int array round-trips as a zero-length chunk with
/// `run_count = 0`, `payload = []`.
#[test]
fn rle_round_trip_empty_int_array() {
    let array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.len(), 0);
}

/// Empty Bool array round-trips cleanly.
#[test]
fn rle_round_trip_empty_bool_array() {
    let array: ArrayRef = Arc::new(BooleanArray::from(Vec::<bool>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::Bool);
    assert_eq!(decoded.len(), 0);
}

/// Empty String array round-trips cleanly.
#[test]
fn rle_round_trip_empty_string_array() {
    let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<String>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    assert_eq!(decoded.len(), 0);
}

/// A single run of identical ints — classic best-case for RLE.
#[test]
fn rle_round_trip_constant_int() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64; 100]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// A single run of identical bools.
#[test]
fn rle_round_trip_constant_bool() {
    let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; 64]));
    let decoded = round_trip(array.as_ref(), &BqlType::Bool);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Alternating ints — worst-case RLE (every row is its own run).
#[test]
fn rle_round_trip_alternating_int() {
    let values: Vec<i64> = (0..50).map(|i| i % 2).collect();
    let array: ArrayRef = Arc::new(Int64Array::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Alternating bools — exercises the LSB-first bitmap at every row
/// boundary.
#[test]
fn rle_round_trip_alternating_bool() {
    let values: Vec<bool> = (0..50).map(|i| i % 2 == 0).collect();
    let array: ArrayRef = Arc::new(BooleanArray::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Bool);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Single-element Int array — `run_count == 1`, `run_ends == [1]`.
#[test]
fn rle_round_trip_single_element_int() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Single-element Timestamp — exercises the Timestamp decode path with
/// the timezone re-application.
#[test]
fn rle_round_trip_single_timestamp() {
    let array: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(vec![0_i64]).with_timezone("UTC"));
    let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Multiple string runs with varying lengths.
#[test]
fn rle_round_trip_multi_run_string() {
    let values = vec!["aaa", "aaa", "bbb", "bbb", "bbb", "ccc"];
    let array: ArrayRef = Arc::new(StringViewArray::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// `i64::MIN` and `i64::MAX` in the same chunk exercises sign-bit
/// preservation in the `[i64 LE]` value stream.
#[test]
fn rle_round_trip_int_extrema() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![
        i64::MIN,
        i64::MIN,
        0,
        0,
        i64::MAX,
        i64::MAX,
    ]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Bool array whose run boundary straddles a bitmap byte (8 trues then
/// 1 false). The `⌈run_count/8⌉` bool bitmap packing must not bleed
/// the high bit of byte 0 into byte 1.
#[test]
fn rle_round_trip_bool_byte_boundary() {
    let mut values = vec![true; 8];
    values.push(false);
    let array: ArrayRef = Arc::new(BooleanArray::from(values));
    let decoded = round_trip(array.as_ref(), &BqlType::Bool);
    assert_eq!(decoded.as_ref(), array.as_ref());
}
