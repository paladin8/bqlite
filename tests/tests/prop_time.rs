//! Property tests for [`bqlite_core::TimeRange`] and [`bqlite_core::Timestamp`].
//!
//! These tests pin down the algebraic laws that the rest of the engine
//! relies on when reasoning about half-open `[start, end)` intervals:
//! `overlaps` and `intersect` agree, `intersect` is commutative and
//! idempotent, `contains` is consistent with intersection against an
//! instant range, and `shift` is a measure-preserving translation. The
//! existing example tests in `crates/bqlite-core/src/time.rs` cover
//! specific edge cases by hand; this file covers the same surface over
//! arbitrary inputs.

use bqlite_core::TimeRange;
use bqlite_tests::strategies::{arb_time_range, arb_timestamp};
use proptest::prelude::*;

proptest! {
    /// `overlaps` and `intersect` must agree: two ranges overlap iff they
    /// produce a non-empty intersection.
    #[test]
    fn prop_overlaps_iff_intersect_some(a in arb_time_range(), b in arb_time_range()) {
        prop_assert_eq!(a.overlaps(b), a.intersect(b).is_some());
    }

    /// `intersect` is commutative.
    #[test]
    fn prop_intersect_commutative(a in arb_time_range(), b in arb_time_range()) {
        prop_assert_eq!(a.intersect(b), b.intersect(a));
    }

    /// When two ranges intersect, the result is non-empty and lies inside
    /// both inputs.
    #[test]
    fn prop_intersect_contained_in_both(a in arb_time_range(), b in arb_time_range()) {
        if let Some(c) = a.intersect(b) {
            prop_assert!(!c.is_empty());
            prop_assert!(c.start >= a.start);
            prop_assert!(c.start >= b.start);
            prop_assert!(c.end <= a.end);
            prop_assert!(c.end <= b.end);
        }
    }

    /// Idempotence: a non-empty range intersected with itself is itself; an
    /// empty range intersected with itself is `None` (because `intersect`
    /// only returns `Some` for non-empty results).
    #[test]
    fn prop_intersect_idempotent(r in arb_time_range()) {
        if r.is_empty() {
            prop_assert_eq!(r.intersect(r), None);
        } else {
            prop_assert_eq!(r.intersect(r), Some(r));
        }
    }

    /// `r.contains(ts)` is equivalent to `r ∩ [ts, ts+1) ≠ ∅`.
    ///
    /// `arb_timestamp()` draws `ts` from the valid event-timestamp range
    /// `[MIN, MAX)` — [`Timestamp::MAX`] is reserved as an exclusive-bound
    /// sentinel and is never generated — so `TimeRange::instant(ts)` is
    /// guaranteed to return `Some(_)` here. The previous boundary dodge
    /// (both sides of the equivalence being trivially false at `ts == MAX`)
    /// is no longer needed, because the domain excludes `MAX` by construction.
    #[test]
    fn prop_contains_iff_instant_intersect(r in arb_time_range(), ts in arb_timestamp()) {
        let instant = TimeRange::instant(ts).expect("arb_timestamp excludes MAX sentinel");
        prop_assert_eq!(r.contains(ts), r.intersect(instant).is_some());
    }

    /// Shifting forward by `n` and back by `-n` is the identity, when
    /// neither shift overflows. `i64::MIN` is excluded because `-MIN` is
    /// unrepresentable as `i64`.
    #[test]
    fn prop_shift_round_trip(
        r in arb_time_range(),
        nanos in (i64::MIN + 1)..=i64::MAX,
    ) {
        if let Some(forward) = r.shift(nanos) {
            if let Some(back) = forward.shift(-nanos) {
                prop_assert_eq!(back, r);
            }
        }
    }

    /// Shift preserves duration: translation is measure-preserving.
    /// `duration_nanos` returns `None` for empty ranges and `Some(n)`
    /// otherwise — both cases must agree before and after the shift.
    #[test]
    fn prop_shift_preserves_duration(r in arb_time_range(), nanos in any::<i64>()) {
        if let Some(shifted) = r.shift(nanos) {
            prop_assert_eq!(shifted.duration_nanos(), r.duration_nanos());
        }
    }
}
