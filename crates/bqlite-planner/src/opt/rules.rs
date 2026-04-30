//! Wave 5 optimizer rule stubs (TASK-521 framework, TASK-527 bodies).
//!
//! Two stub rules registered in `OptimizerPipeline::v1` so the trace
//! surface and budget enforcement are exercised before TASK-527
//! implements the actual structural transforms:
//!
//! - [`Tier3PredicateShapeRule`] — Pass 6.5 from
//!   `optimizer-direction.md` §7 row 7. Reads `value_set_indexed`. With
//!   an empty registry (the steady state until TASK-435/447 land) the
//!   rule has no eligible columns and reports `Skipped`.
//! - [`MatchAnchorPresenceRule`] — Pass 7 from §7 row 8. Reads
//!   `entity_presence_indexed`. Same empty-registry no-op behavior.
//!
//! Both stubs preserve the plan unchanged and consult their declared
//! stats fields so the framework's debug-only budget enforcement is
//! exercised on every planner test run.

use crate::opt::registry::{OptimizerRule, RuleContext, RulePhase};
use crate::physical::PhysicalPlan;
use crate::stats::StatsBudget;

/// Pass 6.5 — Tier-3 value-set predicate-shape gating
/// (`optimizer-direction.md` §7 row 7, owned by TASK-527).
///
/// When TASK-435 / TASK-447 populate `PlannerStats.value_set_indexed`,
/// this rule will reshape pushable predicates into the form the scan's
/// Tier-3 skip-index can intersect against. Until then the rule reads
/// the registry, finds it empty, and reports `Skipped`.
pub struct Tier3PredicateShapeRule;

impl OptimizerRule for Tier3PredicateShapeRule {
    fn id(&self) -> &'static str {
        "tier3_predicate_shape"
    }

    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }

    fn budget(&self) -> StatsBudget {
        StatsBudget {
            index_registry: true,
            ..StatsBudget::none()
        }
    }

    fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        // Future: scan the plan's pushable predicates for columns that
        // carry a registered value-set index, and rewrite them into a
        // shape the scan can intersect. For now we exercise the budget
        // enforcement by consulting the registry on a synthetic key —
        // a valid empty-registry probe that demonstrates the rule
        // reads what it declared.
        //
        // The probe value is intentionally generic: the surface
        // contract is "rule reads value_set_indexed under the
        // `index_registry` budget bit" and that is what the trace
        // reflects, not "rule fired on a specific column".
        let _ = ctx.stats().value_set_indexed("", "");
        ctx.record_skipped("no value-set indexes registered");
        plan
    }
}

/// Pass 7 — MATCH anchor presence-bitmap pushdown
/// (`optimizer-direction.md` §7 row 8, owned by TASK-527).
///
/// When TASK-435 / TASK-447 populate
/// `PlannerStats.entity_presence_indexed`, this rule will mark
/// `ScanPhysical` to apply Tier-2 row-group pruning at runtime. The
/// bitmap itself is consulted at scan time, not plan time.
pub struct MatchAnchorPresenceRule;

impl OptimizerRule for MatchAnchorPresenceRule {
    fn id(&self) -> &'static str {
        "match_anchor_presence"
    }

    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }

    fn budget(&self) -> StatsBudget {
        StatsBudget {
            index_registry: true,
            ..StatsBudget::none()
        }
    }

    fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        let _ = ctx.stats().entity_presence_indexed("", "");
        ctx.record_skipped("no entity-presence bitmaps registered");
        plan
    }
}
