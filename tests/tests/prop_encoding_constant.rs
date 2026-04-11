//! Property tests for `bqlite_storage::encoding::Constant`.
//!
//! Copied from `prop_encoding_plain.rs` (the TASK-206 template) and
//! adapted for the uniformity contract Constant adds: every input
//! array must contain at least one value, and every value must be
//! bit-exactly equal to the first. Round-trip is the load-bearing
//! invariant, same as Plain.
//!
//! The key difference versus the Plain template: the per-type
//! strategies from `bqlite_tests::strategies` produce arbitrary
//! values, not uniform ones. This file defines local strategies
//! that emit uniform arrays of 1..=32 copies of a single drawn
//! value, which is what Constant's encoder accepts.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray,
    TimestampNanosecondArray,
};
use bqlite_core::BqlType;
use bqlite_storage::{Constant, Encoding};
use bqlite_tests::strategies::arb_finite_f64;
use proptest::prelude::*;

/// Run an array through encode → decode and return the decoded
/// array. Mirrors the `round_trip` helper in the Plain template.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = Constant
        .encode(array)
        .expect("Constant::encode must accept every uniform primitive");
    Constant
        .decode(&chunk, ty)
        .expect("Constant::decode must succeed for a chunk Constant produced")
}

/// Row-count range for the uniform-array strategies below. The lower
/// bound is **1** because Constant::encode rejects empty arrays
/// (`segment-format-v1.md` §9.5 + `constant.rs` module docs). The
/// upper bound matches `bqlite_tests::strategies::ENCODING_ARRAY_SIZE_RANGE`.
const UNIFORM_ARRAY_SIZE_RANGE: std::ops::RangeInclusive<usize> = 1..=32;

fn arb_uniform_bool() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    (any::<bool>(), UNIFORM_ARRAY_SIZE_RANGE).prop_map(|(v, n)| {
        (
            BqlType::Bool,
            Arc::new(BooleanArray::from(vec![v; n])) as ArrayRef,
        )
    })
}

fn arb_uniform_int() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    (any::<i64>(), UNIFORM_ARRAY_SIZE_RANGE).prop_map(|(v, n)| {
        (
            BqlType::Int,
            Arc::new(Int64Array::from(vec![v; n])) as ArrayRef,
        )
    })
}

fn arb_uniform_float() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    (arb_finite_f64(), UNIFORM_ARRAY_SIZE_RANGE).prop_map(|(v, n)| {
        (
            BqlType::Float,
            Arc::new(Float64Array::from(vec![v; n])) as ArrayRef,
        )
    })
}

fn arb_uniform_timestamp() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    (i64::MIN..i64::MAX, UNIFORM_ARRAY_SIZE_RANGE).prop_map(|(v, n)| {
        (
            BqlType::Timestamp,
            Arc::new(TimestampNanosecondArray::from(vec![v; n]).with_timezone("UTC")) as ArrayRef,
        )
    })
}

fn arb_uniform_string() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    ("[a-zA-Z0-9 _]{0,16}", UNIFORM_ARRAY_SIZE_RANGE).prop_map(|(s, n)| {
        (
            BqlType::String,
            Arc::new(StringViewArray::from(vec![s; n])) as ArrayRef,
        )
    })
}

/// Combined strategy — one of the five primitives Constant supports,
/// paired with a uniform dense array of 1..=32 copies of a single
/// drawn value.
fn arb_uniform_primitive() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    prop_oneof![
        arb_uniform_bool(),
        arb_uniform_int(),
        arb_uniform_float(),
        arb_uniform_timestamp(),
        arb_uniform_string(),
    ]
}

proptest! {
    /// Round-trip invariant across every primitive Constant claims to
    /// support: `decode(encode(x)) == x` for any uniform dense array.
    #[test]
    fn constant_round_trip_every_primitive(
        (ty, array) in arb_uniform_primitive(),
    ) {
        let decoded = round_trip(array.as_ref(), &ty);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `chunk.payload` is always empty for Constant — that is the
    /// entire point of the encoding. Shrinks that land here would
    /// indicate a regression where the encoder started stuffing bytes
    /// into the payload instead of the header.
    #[test]
    fn constant_payload_is_always_empty(
        (_ty, array) in arb_uniform_primitive(),
    ) {
        let chunk = Constant.encode(array.as_ref()).unwrap();
        prop_assert!(chunk.payload.is_empty());
    }

    /// `estimate_size` is zero for Constant regardless of input size
    /// — matches the `estimate_size: 0` contract documented in
    /// `constant.rs` and guards against a future selector refactor
    /// that accidentally makes Constant look expensive.
    #[test]
    fn constant_estimate_size_is_zero(
        (_ty, array) in arb_uniform_primitive(),
    ) {
        prop_assert_eq!(Constant.estimate_size(array.as_ref()).unwrap(), 0);
    }

    /// `chunk.row_count` tracks the input array length exactly.
    /// Constant preserves row count through the encoding header's
    /// `row_count` field (carried on `EncodedChunk`, not in the
    /// params bytes).
    #[test]
    fn constant_row_count_preserved(
        (_ty, array) in arb_uniform_primitive(),
    ) {
        let chunk = Constant.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }

    /// Non-uniform int arrays MUST be rejected with a typed
    /// `BqliteError::Execution`. The selector relies on this
    /// invariant to treat Constant as a pre-filter predicate.
    #[test]
    fn constant_rejects_non_uniform_ints(
        (first, rest_len) in (any::<i64>(), 1usize..=16),
    ) {
        // Build a length-(rest_len + 1) array whose first value is
        // `first` and whose second value is `first.wrapping_add(1)`
        // — guaranteed to differ.
        let mut values = Vec::with_capacity(rest_len + 1);
        values.push(first);
        values.push(first.wrapping_add(1));
        for _ in 2..=rest_len {
            values.push(first);
        }
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        let result = Constant.encode(array.as_ref());
        prop_assert!(result.is_err());
    }
}

// ── Example tests for edge cases proptest might miss ─────────────────────

/// Single-element Constant chunks are legal — row_count = 1 is the
/// minimum. This is a common case in real data (one-entity row
/// groups with one event) and worth an explicit regression test.
#[test]
fn constant_single_element_round_trip() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![42_i64]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Large uniform chunk — exercises the broadcast-on-decode path at
/// a size that pressures `Vec::with_capacity` allocation but stays
/// well under the row-group default (65,536).
#[test]
fn constant_large_uniform_chunk_round_trip() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7_i64; 1024]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Negative-zero float is preserved bit-exactly on round-trip.
/// Duplicates `plain.rs`'s unit test, landed here so the property
/// template file also covers the edge case for Constant.
#[test]
fn constant_round_trip_negative_zero_float() {
    let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0_f64; 3]));
    let decoded = round_trip(array.as_ref(), &BqlType::Float);
    let decoded_float = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
    for i in 0..decoded_float.len() {
        assert_eq!(decoded_float.value(i).to_bits(), (-0.0_f64).to_bits());
    }
}

/// Empty strings round-trip through the length-prefixed literal path.
#[test]
fn constant_round_trip_empty_string() {
    let array: ArrayRef = Arc::new(StringViewArray::from(vec![""; 5]));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    let decoded_view = decoded.as_any().downcast_ref::<StringViewArray>().unwrap();
    for i in 0..decoded_view.len() {
        assert_eq!(decoded_view.value(i), "");
    }
}
