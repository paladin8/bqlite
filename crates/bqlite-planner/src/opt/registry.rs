//! Optimizer rule registry and pipeline driver (TASK-521).
//!
//! Replaces the bespoke sequence of pass-function calls in
//! [`crate::finalize_physical`] with a uniform rule trait, a registry
//! of rule implementations, and a driver that runs them in registration
//! order, recording each rule's outcome into a [`RuleTrace`] for EXPLAIN
//! visibility.
//!
//! The framework is the merge-first scaffold for Wave 5 rule work. The
//! v1 ruleset (`OptimizerPipeline::v1`) wraps the existing physical
//! passes — [`fuse_match_aggregate`](crate::opt::fuse_match_aggregate),
//! [`pushdown_sample`](crate::opt::sample_pushdown), and
//! [`pushdown_predicates`](crate::opt::pushdown), and
//! [`prune_columns`](crate::opt::prune) — plus two stub rules for
//! Pass 6.5 (Tier-3 predicate-shape gating) and Pass 7 (MATCH anchor
//! presence-bitmap pushdown) defined in
//! [`crate::opt::rules`]. The stubs read from the empty index registry,
//! find no eligible columns, and report `Skipped` to the trace; they
//! become live the moment TASK-435 / TASK-447 populate the registry.
//!
//! See `docs/design/planner/optimizer-direction.md` §6 (PlannerStats),
//! §7 (per-rule policy matrix), §8 (determinism + EXPLAIN visibility),
//! §9 (planner-pipeline.md reconciliation), §10.1 (this task's scope).
//!
//! ## Pass scope
//!
//! This registry owns the **physical-side** passes only. The Wave 0
//! Pass 1 (expression inlining), Pass 2 (predicate pushdown — note: the
//! AST-level dual; the physical version is owned here as
//! `predicate_pushdown`), Pass 3 (scan-predicate extraction from MATCH),
//! Pass 4 (projection pruning), and Pass 5 (constant folding) live at
//! the AST / lowering level and are applied before any
//! [`PhysicalPlan`] node exists. The pipeline driver does **not**
//! re-run those passes.
//!
//! ## Determinism
//!
//! The driver is a single sequential walk over `self.rules`. There is
//! no plan-space search, no fixpoint iteration, no candidate scoring.
//! Given identical `(PhysicalPlan, PlannerStats)` inputs the driver
//! produces an identical `(PhysicalPlan, RuleTrace)` pair. This is the
//! contract `optimizer-direction.md` §8.1 fixes; the v1 property test
//! lives in `lib.rs` tests.

use crate::opt::{
    filter_order::order_stateless_filters, fuse_match_aggregate::fuse_match_aggregate,
    prune::prune_columns, pushdown::pushdown_predicates, sample_pushdown::pushdown_sample,
};
use crate::physical::PhysicalPlan;
use crate::stats::{PlannerStats, PlannerStatsView, StatsBudget};

// ─────────────────────────────────────────────────────────────────────────────
// Rule trait
// ─────────────────────────────────────────────────────────────────────────────

/// Phase the rule runs in.
///
/// Phase 5.1 (`PlanTime`) rules read manifest-derived stats and the
/// index registry. Phase 5.2 (`PostCohort`) rules also read cohort
/// sizes, which are bound by the engine's query coordinator after
/// cohort materialization completes.
///
/// `optimizer-direction.md` §5.1 + §5.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RulePhase {
    /// Runs at plan time, before cohort materialization.
    PlanTime,
    /// Runs after cohort sizes are bound. Currently the only rule in
    /// this phase is Pass 8 (cohort/entity pushdown), owned by
    /// TASK-522 and not registered in production by this task. The
    /// variant is exercised by tests that construct post-cohort
    /// marker rules to verify framework dispatch.
    PostCohort,
}

/// One optimizer rule. Implementations are pure functions of
/// `(PhysicalPlan, PlannerStats)`; the [`RuleContext`] is a side-channel
/// for trace recording and stats access.
pub trait OptimizerRule: Send + Sync {
    /// Stable identifier used in trace output.
    fn id(&self) -> &'static str;

    /// Phase of the optimizer pipeline this rule runs in.
    fn phase(&self) -> RulePhase;

    /// Stats fields the rule is permitted to read. The driver enforces
    /// the budget by narrowing [`PlannerStats`] into a
    /// [`PlannerStatsView`] before invoking the rule; reads against
    /// fields outside the budget panic in debug builds.
    fn budget(&self) -> StatsBudget;

    /// Apply the rule. The default outcome (recorded in the trace) is
    /// [`RuleTraceOutcome::Applied`]; rules that wish to record a
    /// no-op call [`RuleContext::record_skipped`] before returning the
    /// unchanged plan.
    fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> PhysicalPlan;
}

// ─────────────────────────────────────────────────────────────────────────────
// RuleContext, trace types
// ─────────────────────────────────────────────────────────────────────────────

/// Per-rule context handed to [`OptimizerRule::apply`]. Provides
/// budget-checked stats access and the trace side-channel.
pub struct RuleContext<'a> {
    view: PlannerStatsView<'a>,
    /// Recorded outcome for the in-flight rule. `None` means default
    /// (Applied); `Some` means the rule called `record_skipped`.
    pending_skipped: Option<&'static str>,
}

impl<'a> RuleContext<'a> {
    /// Returns the budget-narrowed stats view for this rule.
    pub fn stats(&self) -> &PlannerStatsView<'a> {
        &self.view
    }

    /// Mark the in-flight rule as skipped. The reason appears in the
    /// trace. Calling this multiple times retains the most recent
    /// reason — rules should call it at most once.
    pub fn record_skipped(&mut self, reason: &'static str) {
        self.pending_skipped = Some(reason);
    }
}

/// Outcome of a single rule, recorded in the trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleTraceOutcome {
    /// Rule ran and produced a (possibly identical) plan.
    Applied,
    /// Rule chose not to run; the reason explains the gate that failed.
    Skipped(&'static str),
}

/// One trace entry per rule that ran (or was eligible to run).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleTraceEntry {
    pub rule: &'static str,
    pub phase: RulePhase,
    pub outcome: RuleTraceOutcome,
}

/// Ordered list of [`RuleTraceEntry`] values produced by an
/// [`OptimizerPipeline`] run.
///
/// `optimizer-direction.md` §8.2 requires every rule that fired and
/// every rule that was eligible-but-skipped to appear here. The trace
/// is part of the deterministic surface: identical inputs produce
/// identical traces (§8.1).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RuleTrace {
    pub entries: Vec<RuleTraceEntry>,
}

impl RuleTrace {
    /// Construct a trace pre-allocated to hold `capacity` entries.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline driver
// ─────────────────────────────────────────────────────────────────────────────

/// Ordered list of optimizer rules.
///
/// Build the canonical Wave 5 pipeline with [`OptimizerPipeline::v1`].
/// Apply the plan-time half with [`OptimizerPipeline::run_plan_time`]
/// and the post-cohort half with [`OptimizerPipeline::run_post_cohort`]
/// after the engine binds cohort sizes.
pub struct OptimizerPipeline {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl OptimizerPipeline {
    /// Construct an empty pipeline. Used by tests; production code
    /// should prefer [`OptimizerPipeline::v1`].
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Register a rule. Order matters — rules run in registration
    /// order.
    pub fn with(mut self, rule: Box<dyn OptimizerRule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// Number of registered rules.
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Construct the canonical Wave 5 pipeline per the order specified
    /// in `optimizer-direction.md` §9 (decimal-extended pass numbers).
    ///
    /// Phase-5.1 rules registered, in order:
    ///
    /// 1. Pass 6 — `fuse_match_aggregate`
    /// 2. Pass 6 (auxiliary) — `sample_pushdown` *(runs before
    ///    predicate pushdown so the planner can fold sample state into
    ///    the scan before the filter is elided; preserves the existing
    ///    `finalize_physical` ordering)*
    /// 3. Pass 2 (physical) — `predicate_pushdown`
    /// 4. Pass 4 (physical) — `projection_pruning`
    /// 5. Pass 6.5 — `tier3_predicate_shape` (TASK-527 stub)
    /// 6. Pass 7 — `match_anchor_presence` (TASK-527 stub)
    /// 7. Pass 9 — `stateless_filter_order` (TASK-527)
    ///
    /// Phase-5.2 rules: none registered here. TASK-522 will register
    /// Pass 8 (cohort/entity pushdown) on top of this baseline.
    pub fn v1() -> Self {
        Self::new()
            .with(Box::new(FuseMatchAggregateRule))
            .with(Box::new(SamplePushdownRule))
            .with(Box::new(PredicatePushdownRule))
            .with(Box::new(ProjectionPruningRule))
            .with(Box::new(crate::opt::rules::Tier3PredicateShapeRule))
            .with(Box::new(crate::opt::rules::MatchAnchorPresenceRule))
            .with(Box::new(StatelessFilterOrderingRule))
    }

    /// Run every plan-time (`RulePhase::PlanTime`) rule in registration
    /// order against `plan`, recording each outcome into `trace`. The
    /// returned [`PhysicalPlan`] is the plan after all rules have run.
    pub fn run_plan_time(
        &self,
        mut plan: PhysicalPlan,
        stats: &PlannerStats,
        trace: &mut RuleTrace,
    ) -> PhysicalPlan {
        trace.entries.reserve(self.rules.len());
        for rule in &self.rules {
            if rule.phase() != RulePhase::PlanTime {
                continue;
            }
            plan = self.run_one(rule.as_ref(), plan, stats, trace);
        }
        plan
    }

    /// Run every post-cohort (`RulePhase::PostCohort`) rule in
    /// registration order. Panics if `stats.cohorts_bound()` is false
    /// — the driver enforces the snapshot discipline in
    /// `optimizer-direction.md` §6.2.
    ///
    /// Reserved for TASK-522; no Wave 5 rules are registered in this
    /// phase yet. The function is provided so TASK-522 has a stable
    /// hook.
    pub fn run_post_cohort(
        &self,
        mut plan: PhysicalPlan,
        stats: &PlannerStats,
        trace: &mut RuleTrace,
    ) -> PhysicalPlan {
        assert!(
            stats.cohorts_bound(),
            "OptimizerPipeline::run_post_cohort called before \
             PlannerStats::bind_cohorts (optimizer-direction.md §6.2)",
        );
        for rule in &self.rules {
            if rule.phase() != RulePhase::PostCohort {
                continue;
            }
            plan = self.run_one(rule.as_ref(), plan, stats, trace);
        }
        plan
    }

    /// Apply a single rule, narrowing the stats view and recording the
    /// outcome into `trace`.
    fn run_one(
        &self,
        rule: &dyn OptimizerRule,
        plan: PhysicalPlan,
        stats: &PlannerStats,
        trace: &mut RuleTrace,
    ) -> PhysicalPlan {
        let view = stats.view_for(rule.id(), rule.budget());
        let mut ctx = RuleContext {
            view,
            pending_skipped: None,
        };
        let plan = rule.apply(plan, &mut ctx);
        let outcome = match ctx.pending_skipped {
            None => RuleTraceOutcome::Applied,
            Some(reason) => RuleTraceOutcome::Skipped(reason),
        };
        trace.entries.push(RuleTraceEntry {
            rule: rule.id(),
            phase: rule.phase(),
            outcome,
        });
        plan
    }
}

impl Default for OptimizerPipeline {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wrapper rules around existing physical passes
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps [`fuse_match_aggregate`] (Pass 6, TASK-320).
pub struct FuseMatchAggregateRule;

impl OptimizerRule for FuseMatchAggregateRule {
    fn id(&self) -> &'static str {
        "fuse_match_aggregate"
    }
    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }
    fn budget(&self) -> StatsBudget {
        StatsBudget::none()
    }
    fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        fuse_match_aggregate(plan)
    }
}

/// Wraps [`pushdown_sample`] (Wave 4 sample pushdown, TASK-430).
pub struct SamplePushdownRule;

impl OptimizerRule for SamplePushdownRule {
    fn id(&self) -> &'static str {
        "sample_pushdown"
    }
    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }
    fn budget(&self) -> StatsBudget {
        StatsBudget::none()
    }
    fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        pushdown_sample(plan)
    }
}

/// Wraps [`pushdown_predicates`] (Pass 2 physical, TASK-227).
pub struct PredicatePushdownRule;

impl OptimizerRule for PredicatePushdownRule {
    fn id(&self) -> &'static str {
        "predicate_pushdown"
    }
    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }
    fn budget(&self) -> StatsBudget {
        StatsBudget::none()
    }
    fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        pushdown_predicates(plan)
    }
}

/// Wraps [`prune_columns`] (Pass 4 physical, TASK-228).
pub struct ProjectionPruningRule;

impl OptimizerRule for ProjectionPruningRule {
    fn id(&self) -> &'static str {
        "projection_pruning"
    }
    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }
    fn budget(&self) -> StatsBudget {
        StatsBudget::none()
    }
    fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        prune_columns(plan)
    }
}

/// Wraps [`order_stateless_filters`] (Pass 9, TASK-527). See
/// `optimizer-direction.md` §7 row 10 — pure structural reorder of
/// top-level conjuncts in fused-segment Filter steps; no stats access.
pub struct StatelessFilterOrderingRule;

impl OptimizerRule for StatelessFilterOrderingRule {
    fn id(&self) -> &'static str {
        "stateless_filter_order"
    }
    fn phase(&self) -> RulePhase {
        RulePhase::PlanTime
    }
    fn budget(&self) -> StatsBudget {
        StatsBudget::none()
    }
    fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
        order_stateless_filters(plan)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::ScanPhysical;
    use bqlite_core::property::BqlType;
    use bqlite_core::schema::ColumnDef;
    use bqlite_core::OperatorSchema;

    fn dummy_scan() -> PhysicalPlan {
        PhysicalPlan::Scan(ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: OperatorSchema::new(vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ])
            .unwrap(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        })
    }

    /// In-test rule used to exercise framework primitives.
    struct AlwaysApplied;
    impl OptimizerRule for AlwaysApplied {
        fn id(&self) -> &'static str {
            "always_applied"
        }
        fn phase(&self) -> RulePhase {
            RulePhase::PlanTime
        }
        fn budget(&self) -> StatsBudget {
            StatsBudget::none()
        }
        fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
            plan
        }
    }

    struct AlwaysSkipped;
    impl OptimizerRule for AlwaysSkipped {
        fn id(&self) -> &'static str {
            "always_skipped"
        }
        fn phase(&self) -> RulePhase {
            RulePhase::PlanTime
        }
        fn budget(&self) -> StatsBudget {
            StatsBudget::none()
        }
        fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> PhysicalPlan {
            ctx.record_skipped("not eligible");
            plan
        }
    }

    /// In-test rule that reads a stat outside its declared budget —
    /// used to exercise the debug-only enforcement.
    struct OutOfBudgetReader;
    impl OptimizerRule for OutOfBudgetReader {
        fn id(&self) -> &'static str {
            "out_of_budget_reader"
        }
        fn phase(&self) -> RulePhase {
            RulePhase::PlanTime
        }
        fn budget(&self) -> StatsBudget {
            StatsBudget::none()
        }
        fn apply(&self, plan: PhysicalPlan, ctx: &mut RuleContext<'_>) -> PhysicalPlan {
            // Reads a registry field without declaring index_registry.
            let _ = ctx.stats().value_set_indexed("events", "user_id");
            plan
        }
    }

    /// Phase-5.2 rule for testing the post-cohort dispatch.
    struct PostCohortMarker;
    impl OptimizerRule for PostCohortMarker {
        fn id(&self) -> &'static str {
            "post_cohort_marker"
        }
        fn phase(&self) -> RulePhase {
            RulePhase::PostCohort
        }
        fn budget(&self) -> StatsBudget {
            StatsBudget {
                cohort_sizes: true,
                ..Default::default()
            }
        }
        fn apply(&self, plan: PhysicalPlan, _ctx: &mut RuleContext<'_>) -> PhysicalPlan {
            plan
        }
    }

    #[test]
    fn empty_pipeline_produces_empty_trace() {
        let pipeline = OptimizerPipeline::new();
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
        assert!(trace.entries.is_empty());
    }

    #[test]
    fn applied_rule_records_applied_outcome() {
        let pipeline = OptimizerPipeline::new().with(Box::new(AlwaysApplied));
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
        assert_eq!(trace.entries.len(), 1);
        assert_eq!(trace.entries[0].rule, "always_applied");
        assert_eq!(trace.entries[0].phase, RulePhase::PlanTime);
        assert_eq!(trace.entries[0].outcome, RuleTraceOutcome::Applied);
    }

    #[test]
    fn skipped_rule_records_reason() {
        let pipeline = OptimizerPipeline::new().with(Box::new(AlwaysSkipped));
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
        assert_eq!(trace.entries.len(), 1);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("not eligible")
        );
    }

    #[test]
    fn run_plan_time_skips_post_cohort_rules() {
        // PlanTime+PostCohort rules registered together; run_plan_time
        // only walks PlanTime rules.
        let pipeline = OptimizerPipeline::new()
            .with(Box::new(AlwaysApplied))
            .with(Box::new(PostCohortMarker));
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
        assert_eq!(trace.entries.len(), 1);
        assert_eq!(trace.entries[0].rule, "always_applied");
    }

    #[test]
    #[should_panic(expected = "called before PlannerStats::bind_cohorts")]
    fn run_post_cohort_panics_when_cohorts_unbound() {
        let pipeline = OptimizerPipeline::new().with(Box::new(PostCohortMarker));
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_post_cohort(dummy_scan(), &stats, &mut trace);
    }

    #[test]
    fn run_post_cohort_runs_post_cohort_rules_after_binding() {
        let pipeline = OptimizerPipeline::new()
            .with(Box::new(AlwaysApplied))
            .with(Box::new(PostCohortMarker));
        let mut stats = PlannerStats::empty();
        stats.bind_cohorts(&[]);
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_post_cohort(dummy_scan(), &stats, &mut trace);
        assert_eq!(trace.entries.len(), 1);
        assert_eq!(trace.entries[0].rule, "post_cohort_marker");
        assert_eq!(trace.entries[0].phase, RulePhase::PostCohort);
    }

    #[test]
    #[cfg_attr(
        not(debug_assertions),
        ignore = "budget enforcement is debug-only per optimizer-direction.md §6.4"
    )]
    #[should_panic(expected = "without declaring StatsBudget::index_registry")]
    fn rule_reading_outside_its_budget_panics_in_debug() {
        let pipeline = OptimizerPipeline::new().with(Box::new(OutOfBudgetReader));
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
    }

    #[test]
    fn v1_registers_seven_rules_in_documented_order() {
        let pipeline = OptimizerPipeline::v1();
        assert_eq!(pipeline.rule_count(), 7);
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
        let ids: Vec<&str> = trace.entries.iter().map(|e| e.rule).collect();
        assert_eq!(
            ids,
            vec![
                "fuse_match_aggregate",
                "sample_pushdown",
                "predicate_pushdown",
                "projection_pruning",
                "tier3_predicate_shape",
                "match_anchor_presence",
                "stateless_filter_order",
            ]
        );
    }

    #[test]
    fn v1_stub_rules_report_skipped_with_empty_registry() {
        let pipeline = OptimizerPipeline::v1();
        let stats = PlannerStats::empty();
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(dummy_scan(), &stats, &mut trace);
        // The four pass wrappers + Pass 9 reorder report Applied; the
        // two TASK-527 stubs (6.5 / 7) report Skipped because the
        // registry is empty.
        let by_id: Vec<_> = trace
            .entries
            .iter()
            .map(|e| (e.rule, e.outcome.clone()))
            .collect();
        assert_eq!(
            by_id,
            vec![
                ("fuse_match_aggregate", RuleTraceOutcome::Applied),
                ("sample_pushdown", RuleTraceOutcome::Applied),
                ("predicate_pushdown", RuleTraceOutcome::Applied),
                ("projection_pruning", RuleTraceOutcome::Applied),
                (
                    "tier3_predicate_shape",
                    RuleTraceOutcome::Skipped("no value-set indexes registered")
                ),
                (
                    "match_anchor_presence",
                    RuleTraceOutcome::Skipped("no entity-presence bitmaps registered")
                ),
                ("stateless_filter_order", RuleTraceOutcome::Applied),
            ]
        );
    }
}
