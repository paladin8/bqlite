//! Wave 5 optimizer rules — Pass 6.5 and Pass 7 (TASK-527).
//!
//! Two rules registered in `OptimizerPipeline::v1` that consult the
//! manifest's index registry for plan-time visibility:
//!
//! - [`Tier3PredicateShapeRule`] — Pass 6.5 from
//!   `optimizer-direction.md` §7 row 7. Reads `value_set_indexed`.
//!   Walks the plan, finds every `ScanPhysical`, and probes the
//!   registry for each pushed conjunct's column. Reports `Applied`
//!   when any pushed conjunct matches a registered (table, column)
//!   pair, `Skipped` otherwise. Until TASK-435 / TASK-447 populate
//!   the registry the answer is uniformly `Skipped`.
//! - [`MatchAnchorPresenceRule`] — Pass 7 from §7 row 8. Reads
//!   `entity_presence_indexed`. Walks the plan, finds every
//!   `SequenceMatch` over a scan-anchored input, probes the registry
//!   for each event type the NFA cares about. Reports `Applied`
//!   when any anchor matches a registered (table, anchor) pair.
//!
//! Both rules are pure read-only probes for plan-time EXPLAIN
//! visibility. The runtime scan operator independently consults the
//! index registry from the manifest (`storage-format.md` §11.2.2 /
//! §11.2.3). Per `optimizer-direction.md` §10.3 these rules do **not**
//! mutate the plan — they exist so `EXPLAIN` can show whether the
//! gate fired and so a user diagnosing a slow query never sees a
//! hidden data-dependent decision (§8.2).

use crate::compiled::{CompiledExpr, CompiledNode};
use crate::opt::registry::{OptimizerRule, RuleContext, RulePhase};
use crate::physical::{
    AggregatePhysical, DistinctPhysical, ExplainPhysical, FusedSegmentPhysical, PhysicalPlan,
    ScanPhysical, SequenceMatchPhysical, SortPhysical,
};
use crate::stats::StatsBudget;

/// Pass 6.5 — Tier-3 value-set predicate-shape gating
/// (`optimizer-direction.md` §7 row 7, owned by TASK-527).
///
/// Walks the plan, finds every `ScanPhysical`, and asks the registry
/// (via `PlannerStats.value_set_indexed`) whether any of the scan's
/// pushed conjuncts target a column that carries a registered
/// value-set index. Predicate pushdown (Pass 2) and Pass 10 already
/// produce the `IN`-set / equality-literal shape the scan can
/// intersect against the index; this rule's plan-time work is to
/// verify the shape is right and to surface the gate's value in
/// EXPLAIN output.
///
/// The rule does **not** mutate the plan. The scan operator reads the
/// index registry independently from the manifest at scan time per
/// `storage-format.md` §11.2.3.
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
        let mut applied_any = false;
        walk_scans(&plan, &mut |scan| {
            for conjunct in &scan.scan_predicates {
                if let Some(col) = pushable_column(conjunct) {
                    if ctx.stats().value_set_indexed(&scan.table, col) {
                        applied_any = true;
                    }
                }
            }
        });
        if !applied_any {
            ctx.record_skipped("no value-set indexes registered");
        }
        plan
    }
}

/// Pass 7 — MATCH anchor presence-bitmap pushdown
/// (`optimizer-direction.md` §7 row 8, owned by TASK-527).
///
/// Walks the plan, finds every `SequenceMatchPhysical` whose input is
/// (eventually) a `ScanPhysical`, and asks the registry whether any
/// event type the NFA cares about
/// ([`crate::compile::CompiledNfa::relevant_event_types`]) is
/// registered as an anchor for that scan's table. The Tier-2
/// entity-presence bitmap is per `(table, anchor_event_type)` per
/// `optimizer-direction.md` §4.4; the rule iterates over each event
/// type so the gate aligns with the runtime's pruning capability.
///
/// The rule does **not** mutate the plan; the scan operator reads the
/// bitmap from the manifest at scan time per `storage-format.md`
/// §11.2.2.
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
        let mut applied_any = false;
        walk_match_over_scans(&plan, &mut |scan, sm| {
            for anchor in &sm.compiled_nfa.relevant_event_types {
                if ctx.stats().entity_presence_indexed(&scan.table, anchor) {
                    applied_any = true;
                }
            }
        });
        if !applied_any {
            ctx.record_skipped("no entity-presence bitmaps registered");
        }
        plan
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan walkers
// ─────────────────────────────────────────────────────────────────────────────

/// Recursively visit every `ScanPhysical` in the plan tree.
fn walk_scans<F: FnMut(&ScanPhysical)>(plan: &PhysicalPlan, visit: &mut F) {
    match plan {
        PhysicalPlan::Scan(scan) => visit(scan),
        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical { input, .. } = seg;
            walk_scans(input, visit);
        }
        PhysicalPlan::Explain(ExplainPhysical { plan, .. }) => walk_scans(plan, visit),
        PhysicalPlan::SequenceMatch(sm) => walk_scans(&sm.input, visit),
        PhysicalPlan::Aggregate(AggregatePhysical { input, .. }) => walk_scans(input, visit),
        PhysicalPlan::Sort(SortPhysical { input, .. }) => walk_scans(input, visit),
        PhysicalPlan::Distinct(DistinctPhysical { input, .. }) => walk_scans(input, visit),
        // Leaf / interior-without-scan-children variants: nothing to do.
        _ => {}
    }
}

/// Recursively visit every `SequenceMatchPhysical` whose input
/// resolves (through fused-segment wrappers) to a `ScanPhysical`. The
/// callback receives both the scan (carrying the `table` name) and
/// the match descriptor (carrying the NFA's event-type set).
fn walk_match_over_scans<F: FnMut(&ScanPhysical, &SequenceMatchPhysical)>(
    plan: &PhysicalPlan,
    visit: &mut F,
) {
    match plan {
        PhysicalPlan::SequenceMatch(sm) => {
            if let Some(scan) = scan_under(&sm.input) {
                visit(scan, sm);
            }
            // Recurse into the SequenceMatch's input to catch nested
            // matches (rare in v1 but kept for completeness).
            walk_match_over_scans(&sm.input, visit);
        }
        PhysicalPlan::FusedSegment(seg) => {
            walk_match_over_scans(&seg.input, visit);
        }
        PhysicalPlan::Explain(ExplainPhysical { plan, .. }) => walk_match_over_scans(plan, visit),
        PhysicalPlan::Aggregate(AggregatePhysical { input, .. }) => {
            walk_match_over_scans(input, visit)
        }
        PhysicalPlan::Sort(SortPhysical { input, .. }) => walk_match_over_scans(input, visit),
        PhysicalPlan::Distinct(DistinctPhysical { input, .. }) => {
            walk_match_over_scans(input, visit)
        }
        _ => {}
    }
}

/// If `plan` is a `Scan` (possibly wrapped in one or more
/// `FusedSegment` layers), return a reference to the underlying
/// `ScanPhysical`. Returns `None` otherwise.
fn scan_under(plan: &PhysicalPlan) -> Option<&ScanPhysical> {
    let mut node = plan;
    loop {
        match node {
            PhysicalPlan::Scan(scan) => return Some(scan),
            PhysicalPlan::FusedSegment(seg) => node = &seg.input,
            _ => return None,
        }
    }
}

/// Extract the column name from a conjunct that has a Tier-3-pushable
/// shape: `Compare { Column = Literal }` (or symmetric) and
/// positive `InLiteralSet { Column, negated: false, .. }`. Returns
/// `None` for any other shape.
///
/// `NOT IN` is excluded because it cannot be intersected with a
/// value-set index in the obvious way — the runtime would have to
/// compute the complement against the column's full active value
/// set, which Tier-3 does not support. The spec wording is "IN set
/// or equality literal" (`optimizer-direction.md` §7 row 7).
fn pushable_column(expr: &CompiledExpr) -> Option<&str> {
    use bqlite_ast::expr::CompareOp;
    match &expr.node {
        CompiledNode::Compare {
            op: CompareOp::Equal,
            left,
            right,
            ..
        } => column_name(left).or_else(|| column_name(right)),
        CompiledNode::InLiteralSet {
            input,
            negated: false,
            ..
        } => column_name(input),
        _ => None,
    }
}

fn column_name(expr: &CompiledExpr) -> Option<&str> {
    match &expr.node {
        CompiledNode::Column { name, .. } => Some(name.as_str()),
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{property::BqlType, schema::ColumnDef, OperatorSchema, PropertyValue};

    use crate::compile::{CompiledNfa, MatchExecutionConfig, MatchStrategy, PatternClass};
    use crate::compiled::{ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode, InSetKernel};
    use crate::demand::DemandSet;
    use crate::opt::registry::{
        OptimizerPipeline, RuleTrace, RuleTraceOutcome, StatelessFilterOrderingRule,
    };
    use crate::physical::{
        FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan, ScanPhysical, SequenceMatchPhysical,
        DEFAULT_FILTER_TILE_SIZE, DEFAULT_SPARSITY_FACTOR,
    };
    use crate::stats::PlannerStats;

    use super::*;

    fn empty_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("country", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
        ])
        .unwrap()
    }

    fn make_scan(table: &str, preds: Vec<CompiledExpr>) -> ScanPhysical {
        ScanPhysical {
            table: table.into(),
            query_range: None,
            reader_range: None,
            scan_predicates: preds,
            projected_columns: vec![],
            output_schema: empty_schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        }
    }

    fn col(idx: usize, name: &str, ty: BqlType) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: idx,
                name: name.into(),
            },
            result_type: ty,
            nullable: false,
        }
    }

    fn eq_col_str(idx: usize, name: &str, value: &str) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Equal,
                left: Box::new(col(idx, name, BqlType::String)),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::String(value.into())),
                    result_type: BqlType::String,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqString),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn gt_col_int(idx: usize, name: &str, value: i64) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Greater,
                left: Box::new(col(idx, name, BqlType::Int)),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Int(value)),
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn in_col_strs(idx: usize, name: &str, vs: &[&str]) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(col(idx, name, BqlType::String)),
                values: vs
                    .iter()
                    .map(|s| PropertyValue::String((*s).into()))
                    .collect(),
                negated: false,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn nfa_with_event_types(types: &[&str]) -> CompiledNfa {
        let mut set = BTreeSet::new();
        for t in types {
            set.insert((*t).to_string());
        }
        CompiledNfa {
            states: vec![],
            accept_state: 0,
            relevant_event_types: set,
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            session_window: false,
            emit_all: false,
            state_to_step: vec![],
            brackets: None,
        }
    }

    fn match_over_scan(scan: ScanPhysical, nfa: CompiledNfa) -> PhysicalPlan {
        PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
            compiled_nfa: nfa,
            strategy: MatchStrategy::StepCounter,
            match_all: false,
            demand: DemandSet::default(),
            execution_config: MatchExecutionConfig::default(),
            fused_aggregate: None,
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: empty_schema(),
        }))
    }

    fn fused_over_scan(scan: ScanPhysical) -> PhysicalPlan {
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(scan)),
            steps: vec![FusedSegmentStep::Filter {
                predicate: gt_col_int(3, "amount", 0),
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        })
    }

    fn run_rule<R: OptimizerRule + 'static>(
        rule: R,
        plan: PhysicalPlan,
        stats: &PlannerStats,
    ) -> RuleTrace {
        let pipeline = OptimizerPipeline::new().with(Box::new(rule));
        let mut trace = RuleTrace::default();
        let _ = pipeline.run_plan_time(plan, stats, &mut trace);
        trace
    }

    // ── Tier3PredicateShapeRule ─────────────────────────────────────────────

    #[test]
    fn tier3_skips_with_empty_registry() {
        let plan = PhysicalPlan::Scan(make_scan("events", vec![eq_col_str(2, "country", "US")]));
        let stats = PlannerStats::empty();
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(trace.entries.len(), 1);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no value-set indexes registered")
        );
    }

    #[test]
    fn tier3_applied_when_registered_column_has_eq_literal_predicate() {
        let mut stats = PlannerStats::empty();
        stats
            .value_set_indexed
            .insert(("events".into(), "country".into()), true);

        let plan = PhysicalPlan::Scan(make_scan("events", vec![eq_col_str(2, "country", "US")]));
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(trace.entries[0].outcome, RuleTraceOutcome::Applied);
    }

    #[test]
    fn tier3_applied_when_registered_column_has_in_literal_set_predicate() {
        let mut stats = PlannerStats::empty();
        stats
            .value_set_indexed
            .insert(("events".into(), "country".into()), true);

        let plan = PhysicalPlan::Scan(make_scan(
            "events",
            vec![in_col_strs(2, "country", &["US", "CA"])],
        ));
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(trace.entries[0].outcome, RuleTraceOutcome::Applied);
    }

    #[test]
    fn tier3_skips_when_pushable_column_is_unregistered() {
        let mut stats = PlannerStats::empty();
        // Registered, but on a different column.
        stats
            .value_set_indexed
            .insert(("events".into(), "other".into()), true);

        let plan = PhysicalPlan::Scan(make_scan("events", vec![eq_col_str(2, "country", "US")]));
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no value-set indexes registered")
        );
    }

    #[test]
    fn tier3_skips_when_registered_column_has_only_non_pushable_predicate() {
        let mut stats = PlannerStats::empty();
        stats
            .value_set_indexed
            .insert(("events".into(), "amount".into()), true);

        // amount is registered but the predicate is a range (not eq /
        // not IN), so Tier-3 cannot use it.
        let plan = PhysicalPlan::Scan(make_scan("events", vec![gt_col_int(3, "amount", 100)]));
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no value-set indexes registered")
        );
    }

    #[test]
    fn tier3_walks_into_fused_segment_input() {
        let mut stats = PlannerStats::empty();
        stats
            .value_set_indexed
            .insert(("events".into(), "country".into()), true);

        let scan = make_scan("events", vec![eq_col_str(2, "country", "US")]);
        let plan = fused_over_scan(scan);
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(trace.entries[0].outcome, RuleTraceOutcome::Applied);
    }

    // ── MatchAnchorPresenceRule ─────────────────────────────────────────────

    #[test]
    fn match_anchor_skips_with_empty_registry() {
        let scan = make_scan("events", vec![]);
        let nfa = nfa_with_event_types(&["signup"]);
        let plan = match_over_scan(scan, nfa);
        let stats = PlannerStats::empty();
        let trace = run_rule(MatchAnchorPresenceRule, plan, &stats);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no entity-presence bitmaps registered")
        );
    }

    #[test]
    fn match_anchor_applied_when_anchor_event_type_is_registered() {
        let mut stats = PlannerStats::empty();
        stats
            .entity_presence_indexed
            .insert(("events".into(), "signup".into()), true);

        let scan = make_scan("events", vec![]);
        let nfa = nfa_with_event_types(&["signup", "purchase"]);
        let plan = match_over_scan(scan, nfa);
        let trace = run_rule(MatchAnchorPresenceRule, plan, &stats);
        assert_eq!(trace.entries[0].outcome, RuleTraceOutcome::Applied);
    }

    #[test]
    fn match_anchor_skips_when_no_anchor_event_type_is_registered() {
        let mut stats = PlannerStats::empty();
        stats
            .entity_presence_indexed
            .insert(("events".into(), "logout".into()), true);

        let scan = make_scan("events", vec![]);
        let nfa = nfa_with_event_types(&["signup", "purchase"]);
        let plan = match_over_scan(scan, nfa);
        let trace = run_rule(MatchAnchorPresenceRule, plan, &stats);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no entity-presence bitmaps registered")
        );
    }

    #[test]
    fn match_anchor_handles_segment_between_match_and_scan() {
        let mut stats = PlannerStats::empty();
        stats
            .entity_presence_indexed
            .insert(("events".into(), "signup".into()), true);

        // SequenceMatch over FusedSegment([Filter] over Scan).
        let scan = make_scan("events", vec![]);
        let inner = fused_over_scan(scan);
        let nfa = nfa_with_event_types(&["signup"]);
        let plan = PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
            compiled_nfa: nfa,
            strategy: MatchStrategy::StepCounter,
            match_all: false,
            demand: DemandSet::default(),
            execution_config: MatchExecutionConfig::default(),
            fused_aggregate: None,
            input: Box::new(inner),
            output_schema: empty_schema(),
        }));
        let trace = run_rule(MatchAnchorPresenceRule, plan, &stats);
        assert_eq!(trace.entries[0].outcome, RuleTraceOutcome::Applied);
    }

    #[test]
    fn match_anchor_skips_when_no_match_in_plan() {
        let mut stats = PlannerStats::empty();
        // Registry has an entry, but the plan has no SequenceMatch.
        stats
            .entity_presence_indexed
            .insert(("events".into(), "signup".into()), true);

        let plan = PhysicalPlan::Scan(make_scan("events", vec![]));
        let trace = run_rule(MatchAnchorPresenceRule, plan, &stats);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no entity-presence bitmaps registered")
        );
    }

    fn not_in_col_strs(idx: usize, name: &str, vs: &[&str]) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(col(idx, name, BqlType::String)),
                values: vs
                    .iter()
                    .map(|s| PropertyValue::String((*s).into()))
                    .collect(),
                negated: true,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    #[test]
    fn tier3_skips_when_only_predicate_is_negated_in_literal_set() {
        // NOT IN cannot be intersected with a value-set index; the
        // rule must not report Applied even when the column is
        // registered.
        let mut stats = PlannerStats::empty();
        stats
            .value_set_indexed
            .insert(("events".into(), "country".into()), true);

        let plan = PhysicalPlan::Scan(make_scan(
            "events",
            vec![not_in_col_strs(2, "country", &["US"])],
        ));
        let trace = run_rule(Tier3PredicateShapeRule, plan, &stats);
        assert_eq!(
            trace.entries[0].outcome,
            RuleTraceOutcome::Skipped("no value-set indexes registered")
        );
    }

    // ── No-mutation contract (§10.3 + §4.4): both rules return the
    //    plan untouched. Verified via Debug-format equality before
    //    and after the run. ──────────────────────────────────────────────

    #[test]
    fn tier3_does_not_mutate_the_plan() {
        let mut stats = PlannerStats::empty();
        stats
            .value_set_indexed
            .insert(("events".into(), "country".into()), true);
        let plan = PhysicalPlan::Scan(make_scan("events", vec![eq_col_str(2, "country", "US")]));
        let before = format!("{plan:?}");

        // Run the rule via the pipeline so the same code path executes
        // as in production; capture the output plan and compare.
        let pipeline = OptimizerPipeline::new().with(Box::new(Tier3PredicateShapeRule));
        let mut trace = RuleTrace::default();
        let after = pipeline.run_plan_time(plan, &stats, &mut trace);
        assert_eq!(before, format!("{after:?}"));
    }

    #[test]
    fn match_anchor_does_not_mutate_the_plan() {
        let mut stats = PlannerStats::empty();
        stats
            .entity_presence_indexed
            .insert(("events".into(), "signup".into()), true);
        let scan = make_scan("events", vec![]);
        let nfa = nfa_with_event_types(&["signup"]);
        let plan = match_over_scan(scan, nfa);
        let before = format!("{plan:?}");

        let pipeline = OptimizerPipeline::new().with(Box::new(MatchAnchorPresenceRule));
        let mut trace = RuleTrace::default();
        let after = pipeline.run_plan_time(plan, &stats, &mut trace);
        assert_eq!(before, format!("{after:?}"));
    }

    // Sanity: importing StatelessFilterOrderingRule from the registry
    // module does not regress when this file changes.
    #[allow(dead_code)]
    fn _import_canary() {
        let _ = StatelessFilterOrderingRule;
    }
}
