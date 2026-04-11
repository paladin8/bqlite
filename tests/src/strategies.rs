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

/// Strategy producing any [`Timestamp`] value across the full `i64`
/// nanosecond range.
pub fn arb_timestamp() -> impl Strategy<Value = Timestamp> {
    any::<i64>().prop_map(Timestamp::from_nanos)
}

/// Strategy producing any [`TimeRange`].
///
/// Mixes two sub-strategies so both branches of `is_empty`-aware code paths
/// are exercised reliably:
///
/// 1. an unconstrained `(start, end)` pair, which is empty roughly half the
///    time (for `start >= end`), and
/// 2. a `(start, length)` pair with `length >= 1`, which is non-empty
///    everywhere except at `start == i64::MAX`, where the saturating add
///    collapses to `[MAX, MAX)`. The single empty corner case is harmless;
///    the rest of the second branch reliably tilts the distribution toward
///    non-empty ranges.
///
/// Without the second branch, properties that depend on a non-empty
/// intersection — `prop_intersect_idempotent`, the contained-in-both check,
/// and so on — would only fire on lucky draws.
pub fn arb_time_range() -> impl Strategy<Value = TimeRange> {
    prop_oneof![
        (any::<i64>(), any::<i64>())
            .prop_map(|(a, b)| TimeRange::new(Timestamp::from_nanos(a), Timestamp::from_nanos(b))),
        (any::<i64>(), 1_i64..=i64::MAX).prop_map(|(start, len)| {
            let end = start.saturating_add(len);
            TimeRange::new(Timestamp::from_nanos(start), Timestamp::from_nanos(end))
        }),
    ]
}
