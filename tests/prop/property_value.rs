//! Property-tests for [`bqlite_core::PropertyValue`].
//!
//! This file is the **template** for every other property-test file under
//! `tests/prop/`. The pattern it demonstrates:
//!
//! 1. Pull a strategy from `prop/mod.rs` (included here via `#[path]`).
//! 2. Wrap each invariant in a `proptest!` block with a single
//!    `prop_assert_eq!` / `prop_assert!` as the assertion.
//! 3. Keep per-test logic small — one invariant per test — so a failing
//!    shrink points at exactly one property.
//!
//! See `tests/prop/README.md` for the full playbook.

// Pull the shared strategies into this test binary. `mod.rs` is not a
// standalone Cargo target, so we reach into it directly via `#[path]`
// instead of declaring a normal `mod strategies;`. Every future
// `tests/prop/<topic>.rs` file uses this same one-line include.
#[path = "mod.rs"]
mod strategies;

use std::cmp::Ordering;

use bqlite_core::{BqlType, PropertyValue};
use proptest::prelude::*;
use strategies::{
    arb_bql_type, arb_property_value, arb_scalar_property_value, DEFAULT_COLLECTION_SIZE,
    DEFAULT_RECURSION_DEPTH,
};

proptest! {
    /// Round-trip invariant: encoding a [`PropertyValue`] to JSON and
    /// decoding it back yields a value equal to the original.
    ///
    /// Exercises the full [`PropertyValue`] surface, including nested
    /// `List` and `Map` variants, plus the `serde` derives that the rest
    /// of the codebase relies on for debug dumps and test fixtures.
    #[test]
    fn json_roundtrip_is_identity(v in arb_property_value()) {
        let encoded = serde_json::to_string(&v)
            .expect("PropertyValue must always serialize to JSON");
        let decoded: PropertyValue = serde_json::from_str(&encoded)
            .expect("serialized PropertyValue must always deserialize");
        prop_assert_eq!(decoded, v);
    }

    /// Round-trip invariant for scalar `Timestamp` values: coercing to
    /// `Int` (epoch nanos) and back to `Timestamp` is lossless.
    ///
    /// This covers the specific coercion pair documented in
    /// docs/design/type-system.md §4 and is a template for future
    /// coercion-round-trip tests.
    #[test]
    fn timestamp_int_coercion_roundtrip(ns in any::<i64>()) {
        let original = PropertyValue::Timestamp(ns);
        let as_int = original
            .coerce_to(&BqlType::Int)
            .expect("Timestamp always coerces to Int");
        let back = as_int
            .coerce_to(&BqlType::Timestamp)
            .expect("Int always coerces to Timestamp");
        prop_assert_eq!(back, original);
    }

    /// For any non-null scalar value `v`, `bql_type()` returns the static
    /// type that `v` coerces into under identity coercion. Guards against
    /// accidental drift between the `bql_type()` classifier and the
    /// `coerce_to()` identity branches.
    #[test]
    fn bql_type_agrees_with_identity_coercion(v in arb_scalar_property_value()) {
        match v.bql_type() {
            Some(ty) => {
                prop_assert_eq!(v.coerce_to(&ty), Some(v.clone()));
            }
            None => {
                prop_assert_eq!(v, PropertyValue::Null);
            }
        }
    }

    /// Recursive extension of `bql_type_agrees_with_identity_coercion`:
    /// the identity coercion law also holds for `List` and `Map` values.
    /// The `coerce_to` identity arm for collections ignores the inner
    /// element type, so the round-trip is a clone — this property guards
    /// against that contract regressing into something stricter. Mirrors
    /// the scalar version's match arms (rather than `if let`) so the
    /// `Null` case is asserted instead of silently skipped.
    #[test]
    fn bql_type_agrees_with_identity_coercion_recursive(v in arb_property_value()) {
        match v.bql_type() {
            Some(ty) => {
                prop_assert_eq!(v.coerce_to(&ty), Some(v.clone()));
            }
            None => {
                prop_assert_eq!(v, PropertyValue::Null);
            }
        }
    }

    /// `Null.coerce_to(t) == Some(Null)` for any target — null is
    /// type-agnostic at the boundary (TRY_CAST semantics, §4.2). The
    /// existing example test covers a fixed list of scalar targets;
    /// this property covers the full recursive `BqlType` surface.
    #[test]
    fn null_coerces_to_any_type(target in arb_bql_type()) {
        prop_assert_eq!(
            PropertyValue::Null.coerce_to(&target),
            Some(PropertyValue::Null)
        );
    }

    /// Idempotence: coercing twice to the same target equals coercing
    /// once. After a successful first coerce the value already matches
    /// the target, so the second coerce hits the identity arm — this
    /// invariant fails the moment a coerce arm starts mutating values
    /// of the target type instead of returning them unchanged.
    #[test]
    fn coerce_to_is_idempotent(v in arb_property_value(), target in arb_bql_type()) {
        if let Some(once) = v.coerce_to(&target) {
            prop_assert_eq!(once.coerce_to(&target), Some(once));
        }
    }
}

// ── Ordering laws ────────────────────────────────────────────────────────────

/// Recursive `PropertyValue` strategy that includes `f64::NAN`, `±∞`, and
/// subnormals. The shared `arb_property_value` deliberately excludes those
/// because the JSON round-trip test routes values through a decimal text
/// format and any non-finite float would flake.
///
/// Per `tests/prop/README.md`: tests that need the full `f64` domain define
/// a local strategy rather than widening the shared one. The ordering laws
/// must hold for NaN because `PropertyValue::cmp` uses `f64::total_cmp`,
/// which is a total order over the entire `f64` lattice — that is exactly
/// what these tests are guarding.
fn arb_property_value_with_nan() -> impl Strategy<Value = PropertyValue> {
    let leaf = prop_oneof![
        Just(PropertyValue::Null),
        any::<bool>().prop_map(PropertyValue::Bool),
        any::<i64>().prop_map(PropertyValue::Int),
        any::<f64>().prop_map(PropertyValue::Float),
        // Short alphanumeric strings keep counter-examples readable per the
        // README guideline; the ordering laws don't depend on the alphabet.
        "[a-zA-Z0-9 _]{0,16}".prop_map(PropertyValue::String),
        any::<i64>().prop_map(PropertyValue::Timestamp),
    ];
    // The recursion budget tracks the shared `arb_property_value` constants
    // so the two strategies shrink at a comparable rate and stay in sync if
    // the defaults are tuned later. The small Map key alphabet encourages
    // duplicate keys, which exercises the position-by-position lexicographic
    // comparison rather than always hitting the length-mismatch fast path.
    leaf.prop_recursive(
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

proptest! {
    /// Reflexivity: every value compares equal to itself, including
    /// `Float(NaN)` (`total_cmp` treats NaN-vs-NaN as equal).
    #[test]
    fn cmp_is_reflexive(v in arb_property_value_with_nan()) {
        prop_assert_eq!(v.cmp(&v), Ordering::Equal);
    }

    /// Antisymmetry: `a.cmp(&b) == b.cmp(&a).reverse()` for any pair.
    #[test]
    fn cmp_is_antisymmetric(
        a in arb_property_value_with_nan(),
        b in arb_property_value_with_nan(),
    ) {
        prop_assert_eq!(a.cmp(&b), b.cmp(&a).reverse());
    }

    /// Transitivity: `a ≤ b ∧ b ≤ c ⇒ a ≤ c`. Stated as "not Greater" so
    /// the property covers both `Less` and `Equal` in one shot.
    #[test]
    fn cmp_is_transitive(
        a in arb_property_value_with_nan(),
        b in arb_property_value_with_nan(),
        c in arb_property_value_with_nan(),
    ) {
        if a.cmp(&b) != Ordering::Greater && b.cmp(&c) != Ordering::Greater {
            prop_assert!(a.cmp(&c) != Ordering::Greater);
        }
    }

    /// `PartialEq` and `Ord` agree: `a == b` iff `a.cmp(&b) == Equal`.
    /// The wrinkle this guards is `Float(NaN)`, which compares equal to
    /// itself under `total_cmp` and must therefore also be `==` to itself
    /// under `PartialEq`.
    #[test]
    fn eq_iff_cmp_equal(
        a in arb_property_value_with_nan(),
        b in arb_property_value_with_nan(),
    ) {
        prop_assert_eq!(a == b, a.cmp(&b) == Ordering::Equal);
    }

    /// `PartialOrd::partial_cmp` must equal `Some(Ord::cmp(...))` — the
    /// total-order contract requires this and the current `partial_cmp`
    /// is implemented in terms of `cmp`. The property guards against
    /// future divergence (e.g. someone re-introducing `f64::partial_cmp`
    /// for `Float`, which would silently lose NaN-vs-NaN equality).
    #[test]
    fn partial_cmp_matches_cmp(
        a in arb_property_value_with_nan(),
        b in arb_property_value_with_nan(),
    ) {
        prop_assert_eq!(a.partial_cmp(&b), Some(a.cmp(&b)));
    }
}
