//! Wave 1 scaffold for the demand-propagation protocol.
//!
//! This module freezes the **name** and **shape** of
//! [`DemandCapabilities`] — the operator-side capability advertisement
//! type — so that Wave 1 operator trait work (TASK-108) and Wave 1
//! operator stubs (TASK-117) can compile against a stable-but-minimal
//! demand surface while the real protocol is still in design.
//!
//! # What Wave 1 freezes
//!
//! - The type name [`DemandCapabilities`] for the operator-side
//!   capability advertisement.
//! - Its current placement: `bqlite-core`, at the bottom of the
//!   dependency stack, so the operator traits in `bqlite-operators`
//!   and the planner in `bqlite-planner` can both reference it
//!   without adding new crate dependencies.
//! - A single placeholder variant, [`DemandCapabilities::None`] —
//!   "no special demand handling" — returned by every Wave 1
//!   implementor via the default body.
//! - A minimal [`DemandPropagation`] trait. Its single defaulted
//!   method mirrors the
//!   [`EntityOperator::supported_demands`](../../bqlite_operators/trait.EntityOperator.html#method.supported_demands)
//!   method added in this task as an additive extension to the trait
//!   frozen by TASK-108.
//!
//! # What Wave 1 does NOT freeze
//!
//! - Which capability bits eventually exist. The real set (step
//!   counts, match counts, full match detail, layered extraction,
//!   aggregation fusion, ...) is a Wave 4+ `[DESIGN]` task per the
//!   scope note in `TASKS.md` TASK-110 and the forward references in
//!   `docs/design/operators/operator-traits.md` §7 and
//!   `docs/design/sequence-matching.md` §13.5.
//! - How capabilities compose with the planner-side `DemandSet` type
//!   in the backward propagation pass (`docs/design/planner-pipeline.md`
//!   §9.2–§9.3).
//! - The eventual home crate. `docs/design/sequence-matching.md`
//!   §13.5 and `docs/design/execution-model.md` §13.2 place both
//!   `DemandCapabilities` and `DemandSet` in `bqlite-planner` in the
//!   final design. The Wave 1 location in `bqlite-core` is a scaffold
//!   convenience — the real protocol task is free to relocate this
//!   type to `bqlite-planner` as part of its own design work. Keeping
//!   it in `bqlite-core` today means operator stubs (TASK-117) can
//!   import it without waiting for the eventual planner landing.
//!
//! # Forward compatibility
//!
//! The enum is marked `#[non_exhaustive]` so Wave 4+ can add variants
//! without breaking pattern matches in Wave 1 operator stubs. The
//! [`DemandPropagation`] trait has a default method body so
//! implementors can start with an empty `impl DemandPropagation for
//! MyOp {}` block and rely on the default until they grow real
//! capabilities.
//!
//! **Enum → struct migration note.** The final shape in
//! `docs/design/sequence-matching.md` §13.5 is a *struct* with
//! boolean capability fields (`supports_step_reached`,
//! `supports_match_count`, ...), not an enum. `#[non_exhaustive]`
//! protects against added variants but does **not** protect a
//! wholesale enum → struct migration. To keep that migration cheap,
//! Wave 1 call sites should treat [`DemandCapabilities::None`] as
//! opaque — compare via `== DemandCapabilities::none()` rather than
//! via exhaustive `match` on the enum variants. When Wave 4+ lands
//! the real struct, sites that followed this rule rebase by
//! swapping a single constant; sites that destructured the enum
//! will have to be rewritten.

use serde::{Deserialize, Serialize};

// ─────────────────────────────────────────────────────────────────────────────
// DemandCapabilities
// ─────────────────────────────────────────────────────────────────────────────

/// Operator-side demand capability advertisement — Wave 1 scaffold.
///
/// Wave 1 only exposes the [`DemandCapabilities::None`] variant — the
/// "no special demand handling" placeholder that every Wave 1
/// operator returns. The real capability set is a Wave 4+ design
/// task, documented in `docs/design/sequence-matching.md` §13.5 and
/// `docs/design/operators/operator-traits.md` §7.
///
/// The enum is `#[non_exhaustive]` so later waves can add variants
/// without forcing a synchronized rebase of Wave 1 operator stubs.
///
/// # Relationship to `DemandSet`
///
/// `DemandSet` (the downstream-needs struct, `docs/design/planner-pipeline.md`
/// §9.3) and `DemandCapabilities` (the operator-side advertisement)
/// are dual concepts. In the final protocol, the physical planner
/// matches a `DemandSet` for each plan node against the upstream
/// operator's `DemandCapabilities` to decide whether the operator can
/// satisfy the demand directly. Neither type is implemented in Wave 1;
/// this enum is the scaffold that keeps the name reserved and the
/// shape stable until the real protocol lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub enum DemandCapabilities {
    /// No special demand handling — the operator processes full
    /// batches as delivered by its input, has no layered-extraction
    /// hooks, and participates in no fusion.
    ///
    /// This is the Wave 1 default for every operator and the value
    /// returned by the default [`DemandPropagation::supported_demands`]
    /// body.
    #[default]
    None,
}

impl DemandCapabilities {
    /// The "no capabilities" variant, usable in `const` contexts.
    ///
    /// Equivalent to [`DemandCapabilities::default`], but callable
    /// from const initializers because `Default::default` is not
    /// `const`. Prefer this form when writing the default body of a
    /// trait method or a `const` item.
    pub const fn none() -> Self {
        Self::None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DemandPropagation
// ─────────────────────────────────────────────────────────────────────────────

/// Scaffold trait for operator-side demand advertisement — Wave 1
/// placeholder for the real propagation protocol.
///
/// The trait carries a single method, [`supported_demands`](Self::supported_demands),
/// with a default body returning [`DemandCapabilities::None`]. The
/// method is mirrored on
/// [`bqlite_operators::EntityOperator`](../../bqlite_operators/trait.EntityOperator.html)
/// as an additive extension to the trait frozen by TASK-108 — both
/// surfaces exist in Wave 1 so that:
///
/// - Planner code that reasons about demand at plan time (projection
///   pruning, fusion detection, strategy selection — see
///   `docs/design/planner-pipeline.md` §6.6 and §9) can take a
///   `&dyn DemandPropagation` without reaching into
///   `bqlite-operators`.
/// - Operator kinds that are **not** [`EntityOperator`] implementors
///   still have a uniform way to advertise capabilities to the
///   planner.
/// - `EntityOperator` implementors can advertise capabilities without
///   a second `impl` block — the defaulted method on the operator
///   trait is enough.
///
/// The default body means new implementors can start with an empty
/// `impl DemandPropagation for MyOp {}` block and override only when
/// real capabilities land. A later wave will unify the two surfaces
/// (operator trait method + standalone trait) into a single
/// propagation trait as part of the real Wave 4+ protocol design —
/// see `docs/design/planner-pipeline.md` §9.2 and
/// `docs/design/sequence-matching.md` §13.5 for the forward direction.
///
/// [`EntityOperator`]: ../../bqlite_operators/trait.EntityOperator.html
pub trait DemandPropagation {
    /// Advertise the demand capabilities this operator supports.
    ///
    /// Wave 1 default returns [`DemandCapabilities::None`] — the
    /// scaffold value shared by every Wave 1 operator. Implementors
    /// with real capabilities override the method when the Wave 4+
    /// protocol lands.
    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::None
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── DemandCapabilities ───────────────────────────────────────────────

    #[test]
    fn default_is_none_variant() {
        assert_eq!(DemandCapabilities::default(), DemandCapabilities::None);
    }

    #[test]
    fn const_none_matches_default() {
        assert_eq!(DemandCapabilities::none(), DemandCapabilities::default());
    }

    #[test]
    fn none_usable_as_const_item() {
        // Exercises the `const fn` constructor — a regression test
        // against accidentally removing the `const` qualifier in a
        // later refactor that would silently break downstream const
        // initializers.
        const NONE: DemandCapabilities = DemandCapabilities::none();
        assert_eq!(NONE, DemandCapabilities::None);
    }

    #[test]
    fn is_copy_and_clone() {
        fn assert_copy<T: Copy + Clone>() {}
        assert_copy::<DemandCapabilities>();
    }

    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DemandCapabilities>();
    }

    #[test]
    fn equality_and_hash_are_consistent() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(DemandCapabilities::None);
        set.insert(DemandCapabilities::None);
        assert_eq!(set.len(), 1, "duplicate inserts must collapse");
        assert!(set.contains(&DemandCapabilities::None));
    }

    #[test]
    fn serde_json_round_trip() {
        let original = DemandCapabilities::None;
        let encoded = serde_json::to_string(&original).expect("serialize");
        let decoded: DemandCapabilities = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(decoded, original);
    }

    // ── DemandPropagation ────────────────────────────────────────────────

    struct DefaultingOperator;
    impl DemandPropagation for DefaultingOperator {}

    #[test]
    fn propagation_default_returns_none() {
        let op = DefaultingOperator;
        assert_eq!(op.supported_demands(), DemandCapabilities::None);
    }

    struct OverridingOperator;
    impl DemandPropagation for OverridingOperator {
        fn supported_demands(&self) -> DemandCapabilities {
            // Wave 1 only has `None`, but the override path must
            // compile and be observed so Wave 4+ can add real
            // variants without surprising refactors.
            DemandCapabilities::none()
        }
    }

    #[test]
    fn propagation_override_is_observed() {
        let op = OverridingOperator;
        assert_eq!(op.supported_demands(), DemandCapabilities::none());
    }

    #[test]
    fn propagation_is_object_safe() {
        // Planner code is allowed to take `&dyn DemandPropagation`.
        // Any change that removes object-safety is a Wave 4+ design
        // decision that must be made explicitly via a [TRAIT] task,
        // not slipped in by accident.
        let op: Box<dyn DemandPropagation> = Box::new(DefaultingOperator);
        assert_eq!(op.supported_demands(), DemandCapabilities::None);
    }
}
