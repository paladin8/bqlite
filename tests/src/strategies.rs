//! Shared `proptest` strategies for the bqlite property-test suite.
//!
//! This module is re-exported from [`crate`] (the `bqlite-tests` library)
//! and consumed by every property-test binary under `tests/tests/prop_*.rs`
//! via the normal module path:
//!
//! ```ignore
//! use bqlite_tests::strategies::{arb_property_value, arb_time_range};
//! ```
//!
//! Authoring guidance for individual property tests lives in the
//! `bqlite-tests` crate-level docstring at `tests/src/lib.rs`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray,
};
use bqlite_core::{BqlType, PropertyValue, TimeRange, Timestamp};
use proptest::prelude::*;

/// Maximum depth for recursive `PropertyValue` variants (list/map).
///
/// Kept small so that shrinking stays fast and counter-examples are
/// readable. Individual tests can still exercise deeper structures by
/// composing this strategy manually.
pub const DEFAULT_RECURSION_DEPTH: u32 = 3;

/// Maximum collection size for list/map variants.
pub const DEFAULT_COLLECTION_SIZE: usize = 4;

/// Strategy producing a decimal-round-trip-safe [`f64`].
///
/// Only integer-valued floats in the `i32` range are generated: every
/// such value is representable exactly in `f64` **and** round-trips
/// bit-exactly through any decimal text format (JSON, `Display`, CSV).
/// `PropertyValue` equality uses `total_cmp`, so even a single ULP of
/// drift from an arbitrary-`f64` strategy is enough to flake any test
/// that routes through a decimal format — which is the default here,
/// since JSON round-trip is the canonical template invariant.
///
/// Tests that specifically need the full `f64` domain (subnormals,
/// infinities, NaN, extreme magnitudes, fractional values) should
/// define a local strategy rather than widening this one, and must
/// not compare via text-round-trip.
pub fn arb_finite_f64() -> impl Strategy<Value = f64> {
    any::<i32>().prop_map(|n| n as f64)
}

/// Strategy producing any [`PropertyValue`] scalar (no list/map).
///
/// This is the building block most property tests want — recursive
/// variants add shrinking complexity that is rarely worth it for a
/// scalar-focused invariant.
pub fn arb_scalar_property_value() -> impl Strategy<Value = PropertyValue> {
    prop_oneof![
        Just(PropertyValue::Null),
        any::<bool>().prop_map(PropertyValue::Bool),
        any::<i64>().prop_map(PropertyValue::Int),
        arb_finite_f64().prop_map(PropertyValue::Float),
        // Keep strings short so failing cases print legibly.
        "[a-zA-Z0-9 _]{0,16}".prop_map(PropertyValue::String),
        any::<i64>().prop_map(PropertyValue::Timestamp),
    ]
}

/// Strategy producing an arbitrary [`PropertyValue`], including nested
/// `List` and `Map` variants up to [`DEFAULT_RECURSION_DEPTH`].
pub fn arb_property_value() -> impl Strategy<Value = PropertyValue> {
    arb_scalar_property_value().prop_recursive(
        DEFAULT_RECURSION_DEPTH,
        32,
        DEFAULT_COLLECTION_SIZE as u32,
        |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..=DEFAULT_COLLECTION_SIZE)
                    .prop_map(PropertyValue::List),
                prop::collection::vec(
                    ("[a-z]{1,4}".prop_map(String::from), inner),
                    0..=DEFAULT_COLLECTION_SIZE,
                )
                .prop_map(PropertyValue::Map),
            ]
        },
    )
}

/// Strategy producing any [`BqlType`] scalar (no list/map).
pub fn arb_scalar_bql_type() -> impl Strategy<Value = BqlType> {
    prop_oneof![
        Just(BqlType::Bool),
        Just(BqlType::Int),
        Just(BqlType::Float),
        Just(BqlType::String),
        Just(BqlType::Timestamp),
    ]
}

/// Strategy producing an arbitrary [`BqlType`] including nested `List` and
/// `Map` variants up to [`DEFAULT_RECURSION_DEPTH`]. Both nested types are
/// unary, so each recursive step adds exactly one child.
pub fn arb_bql_type() -> impl Strategy<Value = BqlType> {
    arb_scalar_bql_type().prop_recursive(DEFAULT_RECURSION_DEPTH, 16, 1, |inner| {
        prop_oneof![
            inner.clone().prop_map(|t| BqlType::List(Box::new(t))),
            inner.prop_map(|t| BqlType::Map(Box::new(t))),
        ]
    })
}

/// Strategy producing any **valid** [`Timestamp`] value — every `i64`
/// nanosecond below the reserved sentinel.
///
/// The generated range is `[i64::MIN, i64::MAX)`. [`Timestamp::MAX`] is
/// reserved as an exclusive upper-bound sentinel in `bqlite` and must not
/// appear in event data or test fixtures (see the `Timestamp::MAX` doc
/// comment in `crates/bqlite-core/src/time.rs`). Excluding it here keeps
/// downstream property tests honest: any `Timestamp` they see was already
/// an allowed event value, so assertions like
/// `TimeRange::instant(ts).is_some()` are unconditional instead of
/// threading an `Option` through every claim.
pub fn arb_timestamp() -> impl Strategy<Value = Timestamp> {
    (i64::MIN..i64::MAX).prop_map(Timestamp::from_nanos)
}

/// Strategy producing any [`TimeRange`].
///
/// Mixes two sub-strategies so both branches of `is_empty`-aware code paths
/// are exercised reliably:
///
/// 1. an unconstrained `(start, end)` pair, which is empty roughly half the
///    time (for `start >= end`), and
/// 2. a `(start, length)` pair with `length >= 1`. With the reserved
///    `Timestamp::MAX` sentinel, `start` is drawn from `[MIN, MAX)` and the
///    saturating add clamps `end` at `MAX` — the resulting range is
///    non-empty everywhere except at the narrow degenerate edge where
///    `start == MAX - 1` and `len == 1`, which produces the legitimate
///    one-nanosecond range `[MAX - 1, MAX)`. The distribution still tilts
///    heavily toward non-empty ranges, which is what matters for
///    `prop_intersect_idempotent`, the contained-in-both check, and so on.
///
/// Without the second branch, properties that depend on a non-empty
/// intersection would only fire on lucky draws.
pub fn arb_time_range() -> impl Strategy<Value = TimeRange> {
    prop_oneof![
        (any::<i64>(), any::<i64>())
            .prop_map(|(a, b)| TimeRange::new(Timestamp::from_nanos(a), Timestamp::from_nanos(b))),
        ((i64::MIN..i64::MAX), 1_i64..=i64::MAX).prop_map(|(start, len)| {
            let end = start.saturating_add(len);
            TimeRange::new(Timestamp::from_nanos(start), Timestamp::from_nanos(end))
        }),
    ]
}

// ── Arrow array strategies ─────────────────────────────────────────────────
//
// The strategies below produce **dense** (null-free) Arrow arrays, one
// variant per primitive [`BqlType`]. They are the building block for
// encoding property tests — every Wave 2 encoding impl ships a
// `tests/tests/prop_encoding_<name>.rs` file whose round-trip tests
// pull an array from one of these strategies, feed it through the
// encoding's `encode → decode` pipeline, and assert equality.
//
// Size caps are tuned to keep shrinking fast (small counter-examples
// are readable) while still covering the common byte-boundary cases:
// Bool's `ceil(len / 8)` bit packing, variable-length string offsets,
// and single-element / empty edges.

/// Default per-array size range for the encoding property-test
/// strategies. Large enough to cross Bool's 8-row byte boundary, small
/// enough that shrinking reports one-or-two-element counter-examples.
pub const ENCODING_ARRAY_SIZE_RANGE: std::ops::RangeInclusive<usize> = 0..=32;

/// Strategy producing a dense [`BooleanArray`] of 0..=32 values.
///
/// "Dense" means `null_count() == 0`. The encoding layer's contract
/// is that the writer strips nulls before feeding dense values into
/// `bqlite_storage::Encoding::encode`, so every round-trip property
/// test uses dense arrays.
pub fn arb_bool_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(any::<bool>(), ENCODING_ARRAY_SIZE_RANGE)
        .prop_map(|v| Arc::new(BooleanArray::from(v)) as ArrayRef)
}

/// Strategy producing a dense [`Int64Array`] of 0..=32 values
/// covering the full i64 domain.
pub fn arb_int64_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(any::<i64>(), ENCODING_ARRAY_SIZE_RANGE)
        .prop_map(|v| Arc::new(Int64Array::from(v)) as ArrayRef)
}

/// Strategy producing a dense [`Float64Array`] of 0..=32 values.
///
/// Uses [`arb_finite_f64`] so that every value bit-exact round-trips
/// through decimal text formats (JSON, RFC 3339 etc). Encodings that
/// round-trip via raw IEEE 754 bytes (including `Plain`) are
/// indifferent to non-finite values, but standardizing on
/// `arb_finite_f64` keeps the strategy compatible with tests that
/// *later* want to compose it with a JSON assertion. Tests that
/// specifically need the full f64 domain (NaN, ±∞, subnormals)
/// should define a local strategy — same pattern
/// `prop_property_value.rs` uses for its ordering laws.
pub fn arb_float64_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(arb_finite_f64(), ENCODING_ARRAY_SIZE_RANGE)
        .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef)
}

/// Strategy producing a dense [`TimestampNanosecondArray`] with the
/// canonical `"UTC"` timezone stamp. Values cover the Timestamp-safe
/// range `[i64::MIN, i64::MAX)` — [`Timestamp::MAX`] is reserved as
/// an exclusive upper-bound sentinel and excluded, matching
/// [`arb_timestamp`].
pub fn arb_timestamp_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec(i64::MIN..i64::MAX, ENCODING_ARRAY_SIZE_RANGE)
        .prop_map(|v| Arc::new(TimestampNanosecondArray::from(v).with_timezone("UTC")) as ArrayRef)
}

/// Strategy producing a dense [`StringViewArray`] of 0..=32 values.
///
/// Uses `StringViewArray` (Arrow `Utf8View`) because that is the
/// schema-canonical type for `BqlType::String` per
/// `bqlite_core::arrow::bql_type_to_arrow`. Encoding property tests
/// that feed the output of this strategy through `encode → decode`
/// compare arrays of the same Arrow shape, avoiding cross-type
/// equality gotchas.
///
/// The per-value alphabet matches the scalar `PropertyValue` strategy:
/// short ASCII alphanumerics plus space and underscore. This keeps
/// counter-examples readable. Tests that specifically cover UTF-8
/// edge cases (multi-byte code points, lone surrogates, etc.) should
/// define a local strategy.
pub fn arb_string_array() -> impl Strategy<Value = ArrayRef> {
    prop::collection::vec("[a-zA-Z0-9 _]{0,16}", ENCODING_ARRAY_SIZE_RANGE)
        .prop_map(|v| Arc::new(StringViewArray::from(v)) as ArrayRef)
}

/// Strategy producing a `(BqlType, ArrayRef)` pair — one of the five
/// primitive types Plain v1 supports, paired with a matching dense
/// array. Every Wave 2 encoding property test can use this to exercise
/// "this encoding round-trips through every type it claims to handle"
/// without writing five near-identical test functions.
pub fn arb_primitive_array_with_type() -> impl Strategy<Value = (BqlType, ArrayRef)> {
    prop_oneof![
        arb_bool_array().prop_map(|a| (BqlType::Bool, a)),
        arb_int64_array().prop_map(|a| (BqlType::Int, a)),
        arb_float64_array().prop_map(|a| (BqlType::Float, a)),
        arb_string_array().prop_map(|a| (BqlType::String, a)),
        arb_timestamp_array().prop_map(|a| (BqlType::Timestamp, a)),
    ]
}
