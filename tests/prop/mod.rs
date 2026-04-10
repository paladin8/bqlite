//! Shared `proptest` strategies for the bqlite property-test suite.
//!
//! This module is **not** a standalone target — it is included by each
//! per-topic test file via `#[path = "mod.rs"] mod strategies;`. The
//! indirection keeps strategy definitions reusable across sibling test
//! files (`property_value.rs`, future `time.rs`, future `schema.rs`, …)
//! without turning `tests/prop/` into a separate library crate.
//!
//! See `tests/prop/README.md` for the full pattern.

// Strategies here are imported by individual `tests/prop/<topic>.rs`
// files via `#[path = "mod.rs"]`. Each `[[test]]` target compiles as
// its own crate, so a strategy used by only one of them still looks
// "dead" to the compiler of every other target. The allow is
// deliberate; do not delete "unused" strategies without checking
// every sibling test file — and remember that Wave 2+ tests will
// grow this module.
#![allow(dead_code)]

use bqlite_core::{BqlType, PropertyValue};
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
