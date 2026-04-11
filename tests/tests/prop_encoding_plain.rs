//! Property tests for `bqlite_storage::encoding::Plain`.
//!
//! This file is the **template** every other encoding property-test
//! binary under `tests/tests/prop_encoding_*.rs` copies. The pattern:
//!
//! 1. Pull a dense-Arrow-array strategy from
//!    [`bqlite_tests::strategies`] — [`arb_primitive_array_with_type`]
//!    for a "one invariant, every type" test, or one of the
//!    per-type strategies (`arb_int64_array`, …) for per-type tests.
//! 2. Wrap each invariant in a `proptest!` block with a single
//!    `prop_assert_*` as the assertion, mirroring the scalar-level
//!    template in `prop_property_value.rs`.
//! 3. Start with the **round-trip invariant**:
//!    `decode(encode(x)) == x`. Every encoding ships this property,
//!    parameterized by whatever strategy covers its applicable types.
//! 4. Add encoding-specific invariants as separate `#[test]` blocks
//!    — for example, "Dictionary `code_bit_width ≤ 24`" or
//!    "Delta residuals stay in i64 range".
//!
//! The round-trip property is the load-bearing one: it catches every
//! byte-level layout mismatch the unit tests might miss. Keep it
//! first in the file so shrinking reports counter-examples through
//! it preferentially.
//!
//! See `docs/design/storage/segment-format-v1.md` §9 for the
//! byte-level spec this test asserts against indirectly (by
//! round-tripping the Plain impl).

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray};
use bqlite_core::BqlType;
use bqlite_storage::{Encoding, Plain};
use bqlite_tests::strategies::{
    arb_bool_array, arb_float64_array, arb_int64_array, arb_primitive_array_with_type,
    arb_string_array, arb_timestamp_array,
};
use proptest::prelude::*;

/// Helper that runs one value through `encode → decode` and returns
/// the decoded array. Every round-trip test calls this so the `encode`
/// and `decode` invocations stay consistent across cases.
fn round_trip(array: &dyn arrow::array::Array, ty: &BqlType) -> ArrayRef {
    let chunk = Plain
        .encode(array)
        .expect("Plain::encode must accept every dense primitive");
    Plain
        .decode(&chunk, ty)
        .expect("Plain::decode must succeed for a chunk Plain produced")
}

proptest! {
    /// Round-trip invariant across every type Plain claims to
    /// support: `decode(encode(x)) == x` holds for any dense Arrow
    /// array of a primitive [`BqlType`].
    ///
    /// This is the single property that guards the Plain byte layout
    /// against drift from `segment-format-v1.md` §9.1. If the spec
    /// and the impl disagree on (for example) whether the Bool
    /// bitmap is LSB-first, this test fails on the first non-trivial
    /// pattern.
    #[test]
    fn plain_round_trip_every_primitive(
        (ty, array) in arb_primitive_array_with_type(),
    ) {
        let decoded = round_trip(array.as_ref(), &ty);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant, narrowed to [`BooleanArray`]. Duplicates
    /// one slice of the "every primitive" property but calls out the
    /// byte-boundary edge explicitly — Bool is the only v1 primitive
    /// whose payload byte count is non-linear in `row_count`, so
    /// shrinks that land here usually point at packing bugs.
    #[test]
    fn plain_round_trip_bool(array in arb_bool_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Bool);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant, narrowed to [`Int64Array`]. Useful
    /// because `any::<i64>()` hits `i64::MIN`, `i64::MAX`, and `0`
    /// often enough that sign-extension and two's-complement bugs
    /// surface here with tiny counter-examples.
    #[test]
    fn plain_round_trip_int(array in arb_int64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Int);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant, narrowed to [`Float64Array`]. The
    /// `arb_finite_f64` strategy keeps NaN out of the domain —
    /// Plain's encode/decode is a pure byte copy and would handle
    /// NaN correctly, but testing NaN round-trips would mask the
    /// fact that `Float64Array::PartialEq` is NaN-hostile.
    #[test]
    fn plain_round_trip_float(array in arb_float64_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Float);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant, narrowed to [`StringViewArray`]. The
    /// variable-length `[u32 LE length][bytes]` layout is the most
    /// error-prone of the five Plain paths; shrinking on this test
    /// reliably produces one-string counter-examples. Uses the
    /// schema-canonical `Utf8View` shape so input and output match
    /// under Arrow's `PartialEq`.
    #[test]
    fn plain_round_trip_string(array in arb_string_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::String);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// Round-trip invariant, narrowed to the `TimestampNanosecond`
    /// array variant. This property also guards the round-trip
    /// timezone stamp — the encoded chunk does not carry the `"UTC"`
    /// marker, so `Plain::decode` must re-apply it. Arrow's
    /// `PartialEq` on `TimestampNanosecondArray` includes the
    /// timezone, so a missing `with_timezone("UTC")` on the decode
    /// side would fail this test.
    #[test]
    fn plain_round_trip_timestamp(array in arb_timestamp_array()) {
        let decoded = round_trip(array.as_ref(), &BqlType::Timestamp);
        prop_assert_eq!(decoded.as_ref(), array.as_ref());
    }

    /// `estimate_size == encode().payload.len()`: Plain's estimate is
    /// exact, so a cheap byte-count comparison fails the moment the
    /// estimator and the encoder drift apart. Later encodings may
    /// return upper bounds rather than exact counts; their template
    /// switches this to `prop_assert!(estimate >= actual)`.
    #[test]
    fn plain_estimate_size_is_exact(
        (_ty, array) in arb_primitive_array_with_type(),
    ) {
        let estimated = Plain.estimate_size(array.as_ref()).unwrap();
        let actual = Plain.encode(array.as_ref()).unwrap().payload.len();
        prop_assert_eq!(estimated, actual);
    }

    /// `chunk.row_count` tracks the input array length exactly.
    /// Catches regressions where a future encoding revision forgets
    /// to propagate row count from the input to the chunk (the
    /// reader in TASK-215 cross-checks this against the footer's
    /// `ColumnChunkMeta.row_count`).
    #[test]
    fn plain_row_count_preserved(
        (_ty, array) in arb_primitive_array_with_type(),
    ) {
        let chunk = Plain.encode(array.as_ref()).unwrap();
        prop_assert_eq!(chunk.row_count, array.len());
    }
}

// ── Example tests that cover edge cases proptest might miss ─────────────

/// Example: encode/decode a single-element [`BooleanArray`]. Proptest
/// hits this often but an explicit regression test documents the
/// edge.
#[test]
fn plain_round_trip_bool_single_true() {
    let array: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    let decoded = round_trip(array.as_ref(), &BqlType::Bool);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Example: encode/decode a single-element [`BooleanArray`] that
/// crosses the first sub-byte boundary but only writes one bit.
#[test]
fn plain_round_trip_bool_single_false() {
    let array: ArrayRef = Arc::new(BooleanArray::from(vec![false]));
    let decoded = round_trip(array.as_ref(), &BqlType::Bool);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Example: empty [`StringViewArray`] must round-trip to an empty
/// array, not to `None` or to a zero-row error.
#[test]
fn plain_round_trip_empty_string_array() {
    let array: ArrayRef = Arc::new(StringViewArray::from(Vec::<String>::new()));
    let decoded = round_trip(array.as_ref(), &BqlType::String);
    assert_eq!(decoded.len(), 0);
}

/// Example: an [`Int64Array`] that exercises the i64 extrema.
#[test]
fn plain_round_trip_int_extrema() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX]));
    let decoded = round_trip(array.as_ref(), &BqlType::Int);
    assert_eq!(decoded.as_ref(), array.as_ref());
}

/// Example: a [`Float64Array`] with signed zeros — `Float64Array`'s
/// `PartialEq` treats `-0.0 == 0.0` which is consistent with IEEE
/// 754 semantics, but the byte-level round-trip preserves the sign
/// bit. This test asserts the round-trip is bit-exact.
#[test]
fn plain_round_trip_float_signed_zero() {
    let array: ArrayRef = Arc::new(Float64Array::from(vec![0.0_f64, -0.0]));
    let decoded = round_trip(array.as_ref(), &BqlType::Float);
    let decoded_float = decoded.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(decoded_float.value(0).to_bits(), 0.0_f64.to_bits());
    assert_eq!(decoded_float.value(1).to_bits(), (-0.0_f64).to_bits());
}
