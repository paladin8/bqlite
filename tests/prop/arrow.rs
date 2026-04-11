//! Property test for the bqlite ↔ Arrow type round-trip documented in
//! `docs/design/type-system.md` §7.3.
//!
//! The example test `round_trip_all_variants` in
//! `crates/bqlite-core/src/arrow.rs` exhausts the depth-1 cases by hand;
//! this file generalizes the same invariant to arbitrary nesting up to
//! [`strategies::DEFAULT_RECURSION_DEPTH`]. A failure here shrinks down
//! to the minimal nested shape that breaks the canonical mapping in §7.1.

#[path = "mod.rs"]
mod strategies;

use bqlite_core::{arrow_to_bql_type, bql_type_to_arrow};
use proptest::prelude::*;
use strategies::arb_bql_type;

proptest! {
    /// Documented round-trip guarantee (§7.3): for any `BqlType` shape
    /// (including arbitrarily nested `List` and `Map`),
    /// `arrow_to_bql_type(&bql_type_to_arrow(&t)) == Ok(t)`.
    #[test]
    fn prop_bql_type_arrow_round_trip(bql in arb_bql_type()) {
        let dt = bql_type_to_arrow(&bql);
        let back = arrow_to_bql_type(&dt).expect("round trip must succeed");
        prop_assert_eq!(back, bql);
    }
}
