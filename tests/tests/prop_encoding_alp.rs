//! Property tests for `bqlite_storage::encoding::Alp`.
//!
//! ALP (Adaptive Lossless floating-Point) encodes "round" float values
//! (prices, percentages, scores) as integer mantissas via a decimal
//! exponent, with a patch list for exceptions.  These tests exercise
//! the round-trip invariant across several float distributions:
//!
//! - **Finite floats** (the generic `arb_float64_array` strategy) —
//!   integer-valued floats that decompose cleanly at exponent 0.
//! - **Round prices** — N.NN floats with 2 decimal places that
//!   decompose at exponent 2.
//! - **Percentages** — 0.00..1.00 with 2 decimal places.
//! - **All-exception** — irrational-ish values that never decompose.
//! - **Mixed** — a blend of decomposable and exception values.
//!
//! See `docs/design/storage/segment-format-v2.md` §5.6 for the
//! byte-level spec and `docs/design/storage/advanced-encodings.md` §8
//! for the ALP evaluation.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use bqlite_core::BqlType;
use bqlite_storage::{Alp, Encoding};
use bqlite_tests::strategies::arb_float64_array;
use proptest::prelude::*;

/// Encode → decode round-trip helper.
fn round_trip(array: &dyn arrow::array::Array) -> ArrayRef {
    let chunk = Alp
        .encode(array)
        .expect("Alp::encode must accept every dense Float64Array");
    Alp.decode(&chunk, &BqlType::Float)
        .expect("Alp::decode must succeed for a chunk Alp produced")
}

// ── Custom strategies ──────────────────────────────────────────────────────

/// Strategy producing f64 values with exactly 2 decimal places (prices).
fn arb_price() -> impl Strategy<Value = f64> {
    (0..100_000i64).prop_map(|n| n as f64 / 100.0)
}

/// Strategy producing a dense Float64Array of 0..=32 "price" values.
fn arb_price_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(arb_price(), 0..=32)
        .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
}

/// Strategy producing f64 percentage values (0.00..1.00, 2 decimal places).
fn arb_percentage() -> impl Strategy<Value = f64> {
    (0..=100i32).prop_map(|n| n as f64 / 100.0)
}

/// Strategy producing a dense Float64Array of 0..=32 percentage values.
fn arb_percentage_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(arb_percentage(), 0..=32)
        .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
}

/// Strategy producing "irrational-ish" f64 values that are unlikely
/// to decompose cleanly at any exponent.
fn arb_irrational() -> impl Strategy<Value = f64> {
    any::<u64>().prop_map(|bits| {
        // Mask to valid finite f64 by clearing the exponent's top bits
        // (avoiding NaN/Inf) and ensuring a non-zero significand.
        let bits = (bits & 0x7FEF_FFFF_FFFF_FFFF) | 0x0010_0000_0000_0000;
        f64::from_bits(bits)
    })
}

/// Strategy producing a dense Float64Array of 0..=32 irrational values.
fn arb_irrational_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(arb_irrational(), 0..=32)
        .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
}

/// Strategy producing a mixed array: ~50% round values, ~50% irrationals.
fn arb_mixed_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(prop_oneof![arb_price(), arb_irrational()], 0..=32)
        .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
}

/// Strategy producing arrays large enough to span multiple FOR blocks.
fn arb_multi_block_price_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(arb_price(), 129..=300)
        .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
}

// ── Property tests ─────────────────────────────────────────────────────────

proptest! {
    /// Round-trip on finite integer-valued floats (the standard
    /// `arb_float64_array` strategy).  These decompose cleanly at
    /// exponent 0 since they are exact integers.
    #[test]
    fn alp_round_trip_finite_floats(array in arb_float64_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip on "price" arrays (2 decimal places).
    #[test]
    fn alp_round_trip_prices(array in arb_price_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip on percentage arrays (0.00..1.00).
    #[test]
    fn alp_round_trip_percentages(array in arb_percentage_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip on all-exception arrays (irrationals).  Every value
    /// ends up in the patch list; the FOR mantissa stream is empty.
    #[test]
    fn alp_round_trip_all_exceptions(array in arb_irrational_array()) {
        let decoded = round_trip(array.as_ref());
        // Bit-exact comparison for exception values.
        let expected = array.as_any().downcast_ref::<Float64Array>().unwrap();
        let actual = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        prop_assert_eq!(expected.len(), actual.len());
        for i in 0..expected.len() {
            prop_assert_eq!(
                expected.value(i).to_bits(),
                actual.value(i).to_bits(),
                "bit mismatch at index {}", i
            );
        }
    }

    /// Round-trip on mixed (decomposable + exception) arrays.
    #[test]
    fn alp_round_trip_mixed(array in arb_mixed_array()) {
        let decoded = round_trip(array.as_ref());
        let expected = array.as_any().downcast_ref::<Float64Array>().unwrap();
        let actual = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
        prop_assert_eq!(expected.len(), actual.len());
        for i in 0..expected.len() {
            let e = expected.value(i);
            let a = actual.value(i);
            prop_assert!(
                e == a || (e.is_nan() && a.is_nan()),
                "value mismatch at index {}: expected {}, got {}", i, e, a
            );
        }
    }

    /// Round-trip on multi-block arrays (>128 values).
    #[test]
    fn alp_round_trip_multi_block(array in arb_multi_block_price_array()) {
        let decoded = round_trip(array.as_ref());
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `estimate_size >= encode().payload.len()`: ALP's estimate is
    /// an upper bound based on the global mantissa bit width.
    #[test]
    fn alp_estimate_is_upper_bound(array in arb_price_array()) {
        let estimated = Alp.estimate_size(array.as_ref()).unwrap();
        let actual = Alp.encode(array.as_ref()).unwrap().payload.len();
        prop_assert!(
            estimated >= actual,
            "estimate ({}) < actual ({})", estimated, actual
        );
    }

    /// `chunk.row_count` matches the input array length.
    #[test]
    fn alp_row_count_preserved(array in arb_float64_array()) {
        let chunk = Alp.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }
}
