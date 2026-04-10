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

use bqlite_core::{BqlType, PropertyValue};
use proptest::prelude::*;
use strategies::{arb_property_value, arb_scalar_property_value};

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
}
