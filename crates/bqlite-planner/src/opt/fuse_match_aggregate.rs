//! Match-aggregate fusion optimizer pass (TASK-320, Pass 6).
//!
//! Detects `Aggregate(SequenceMatch(...))` in the physical plan where the
//! aggregate's expressions can be fulfilled entirely from the SequenceMatch's
//! output schema (`entity_id`, `step_reached`, bound variables). When
//! eligible, the aggregate is fused into `SequenceMatchPhysical.fused_aggregate`
//! and the standalone `Aggregate` node is elided.
//!
//! # Pattern
//!
//! Conservative — only the direct adjacency pattern is fused:
//!
//! ```text
//! Aggregate(input = SequenceMatch(...))
//! ```
//!
//! Any intermediate node (Filter, Sort, Project) between the Aggregate
//! and the SequenceMatch blocks fusion. These cases are left unchanged.
//!
//! The `Filter`-separated pattern (`Aggregate(Filter(SequenceMatch(...)))`),
//! termed `FilterThenAggregate` in planner-pipeline.md §7.2, is **not**
//! fused in Wave 3. Implementing it would require a `fused_filter` field on
//! `CompiledFusableAggregate` and filter-evaluation logic in
//! `SequenceMatchOperator` (TASK-321). This is deferred to a follow-up task;
//! `DemandSet.fused_filter` is reserved for that extension.
//! See `docs/design/planner/wave3-lowering.md` §4.2 for the rationale.
//!
//! # Eligibility
//!
//! For a direct `Aggregate(SequenceMatch)` pair, fusion is eligible when:
//!
//! 1. Every group-by expression references only columns present in the
//!    SequenceMatch's output schema.
//! 2. Every aggregate argument expression references only columns present in
//!    the SequenceMatch's output schema (`COUNT(*)` — arg-less — is always
//!    eligible).
//!
//! All Wave 3 aggregate functions are incrementally computable so no
//! function-level eligibility check is needed (aggregate-operator.md §6.2).
//!
//! # Effect
//!
//! When fused:
//! - A [`CompiledFusableAggregate`] is built from the aggregate's
//!   `aggregates` and `group_by` fields and placed in
//!   `SequenceMatchPhysical.fused_aggregate`.
//! - The SequenceMatch's `output_schema` is replaced with the aggregate's
//!   `output_schema` so downstream operators see the correct column shape.
//! - The `Aggregate` node is removed from the tree.
//!
//! When not fused, the pass recurses into child plans so nested eligible
//! patterns deeper in the tree can still be discovered.
//!
//! # Usage
//!
//! Applied as a post-lowering physical optimizer pass in
//! [`crate::plan`] before predicate pushdown and projection pruning.
//!
//! See `docs/design/planner/wave3-lowering.md` §4 and
//! `docs/design/operators/aggregate-operator.md` §9 for the full spec.

use std::collections::HashSet;

use crate::compiled::{CompiledExpr, CompiledNode};
use crate::demand::{CompiledAggExpr, CompiledFusableAggregate};
use crate::physical::{
    AggregatePhysical, DistinctPhysical, ExplainPhysical, FilterPhysical, LimitPhysical,
    PhysicalPlan, ProjectPhysical, SequenceMatchPhysical, SortPhysical,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the stateful-aggregate fusion optimizer pass over a [`PhysicalPlan`] tree.
///
/// Detects `Aggregate(Stateful(...))` adjacency where `Stateful` is one of
/// the four fusion-eligible operators — `SequenceMatch`, `Sessionize`,
/// `EventSelect`, or `Attribute` — and fuses the aggregate into the
/// stateful node's `fused_aggregate` field, eliding the standalone
/// `Aggregate` node. Originally TASK-320 (MATCH only); extended in
/// TASK-520 to cover the three Wave 4 stateful operators.
///
/// For each fused operator the pass:
/// - Builds a [`CompiledFusableAggregate`] from the aggregate descriptor.
/// - Replaces the operator's `output_schema` with the aggregate's output
///   schema and stashes the operator's pre-fusion native schema in
///   `pre_fusion_output_schema` so the runtime operator can keep building
///   per-entity batches in the native shape.
///
/// All other nodes are recursed into so nested eligible patterns are
/// found.
pub fn fuse_match_aggregate(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        // ── Aggregate: attempt fusion with an immediately adjacent stateful op
        PhysicalPlan::Aggregate(agg) => fuse_aggregate_node(agg),

        // ── Recursive cases: recurse into child plans ─────────────────────────
        PhysicalPlan::Filter(filter) => {
            let FilterPhysical {
                predicate,
                input,
                tile_size,
                output_schema,
            } = filter;
            PhysicalPlan::Filter(FilterPhysical {
                predicate,
                input: Box::new(fuse_match_aggregate(*input)),
                tile_size,
                output_schema,
            })
        }

        PhysicalPlan::Project(proj) => {
            let ProjectPhysical {
                expressions,
                input,
                output_schema,
            } = proj;
            PhysicalPlan::Project(ProjectPhysical {
                expressions,
                input: Box::new(fuse_match_aggregate(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Limit(limit) => {
            let LimitPhysical {
                count,
                input,
                output_schema,
            } = limit;
            PhysicalPlan::Limit(LimitPhysical {
                count,
                input: Box::new(fuse_match_aggregate(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Sort(sort) => {
            let SortPhysical {
                keys,
                max_rows,
                input,
                output_schema,
            } = sort;
            PhysicalPlan::Sort(SortPhysical {
                keys,
                max_rows,
                input: Box::new(fuse_match_aggregate(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Distinct(distinct) => {
            let DistinctPhysical {
                max_groups,
                input,
                output_schema,
            } = distinct;
            PhysicalPlan::Distinct(DistinctPhysical {
                max_groups,
                input: Box::new(fuse_match_aggregate(*input)),
                output_schema,
            })
        }

        // ── SequenceMatch: recurse into child, preserve any existing fused_aggregate.
        // A SequenceMatch with fused_aggregate already set (e.g. by a prior pass
        // run or via the logical-level fused_downstream path) must not be
        // overwritten here — we only recurse into the child scan/filter subtree.
        PhysicalPlan::SequenceMatch(seq_match) => {
            let SequenceMatchPhysical {
                compiled_nfa,
                strategy,
                match_all,
                demand,
                execution_config,
                fused_aggregate,
                input,
                output_schema,
            } = *seq_match;
            PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
                compiled_nfa,
                strategy,
                match_all,
                demand,
                execution_config,
                fused_aggregate, // preserved — do not re-fuse a node already handled
                input: Box::new(fuse_match_aggregate(*input)),
                output_schema,
            }))
        }

        // ── Wave 4 stateful operators: recurse into child, preserve any
        // existing fused_aggregate. Same rationale as SequenceMatch above.
        PhysicalPlan::Sessionize(mut sess) => {
            sess.input = Box::new(fuse_match_aggregate(*sess.input));
            PhysicalPlan::Sessionize(sess)
        }

        PhysicalPlan::EventSelect(mut es) => {
            es.input = Box::new(fuse_match_aggregate(*es.input));
            PhysicalPlan::EventSelect(es)
        }

        PhysicalPlan::Attribute(mut attr) => {
            attr.input = Box::new(fuse_match_aggregate(*attr.input));
            PhysicalPlan::Attribute(attr)
        }

        PhysicalPlan::Explain(explain) => {
            let ExplainPhysical {
                plan: inner,
                output_schema,
            } = explain;
            PhysicalPlan::Explain(ExplainPhysical {
                plan: Box::new(fuse_match_aggregate(*inner)),
                output_schema,
            })
        }

        // ── Leaf nodes: Scan, DDL, DML — no children to recurse ───────────────
        other => other,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-stateful-operator dispatch
// ─────────────────────────────────────────────────────────────────────────────

/// Dispatch an `Aggregate` node to the appropriate fusion arm based on its
/// immediate child. Falls back to recursive descent through the child when
/// the child is not a fusion-eligible stateful operator. TASK-520.
fn fuse_aggregate_node(agg: AggregatePhysical) -> PhysicalPlan {
    let AggregatePhysical {
        aggregates,
        group_by,
        max_groups,
        input,
        output_schema: agg_output_schema,
    } = agg;

    match *input {
        // Direct adjacency with a SequenceMatch — the original TASK-320 case.
        PhysicalPlan::SequenceMatch(seq_match) => {
            let native_cols: HashSet<&str> = seq_match
                .output_schema
                .columns()
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            if is_eligible(&aggregates, &group_by, &native_cols) {
                let fused = build_compiled_fusable(
                    &aggregates,
                    &group_by,
                    agg_output_schema.clone(),
                    max_groups,
                );
                PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
                    fused_aggregate: Some(fused),
                    output_schema: agg_output_schema,
                    ..*seq_match
                }))
            } else {
                let seq_input = fuse_match_aggregate(PhysicalPlan::SequenceMatch(seq_match));
                PhysicalPlan::Aggregate(AggregatePhysical {
                    aggregates,
                    group_by,
                    max_groups,
                    input: Box::new(seq_input),
                    output_schema: agg_output_schema,
                })
            }
        }

        // ── Wave 4 stateful fusion targets ────────────────────────────────
        PhysicalPlan::Sessionize(sess) => {
            let native_cols: HashSet<&str> = sess
                .output_schema
                .columns()
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            // Don't double-fuse if a prior pass already set fused_aggregate.
            if sess.fused_aggregate.is_none() && is_eligible(&aggregates, &group_by, &native_cols) {
                let fused = build_compiled_fusable(
                    &aggregates,
                    &group_by,
                    agg_output_schema.clone(),
                    max_groups,
                );
                let mut sess = sess;
                // Stash the native schema so the runtime operator can keep
                // building per-entity batches in the native shape.
                sess.pre_fusion_output_schema = Some(sess.output_schema.clone());
                sess.fused_aggregate = Some(fused);
                sess.output_schema = agg_output_schema;
                sess.input = Box::new(fuse_match_aggregate(*sess.input));
                PhysicalPlan::Sessionize(sess)
            } else {
                let recursed = fuse_match_aggregate(PhysicalPlan::Sessionize(sess));
                PhysicalPlan::Aggregate(AggregatePhysical {
                    aggregates,
                    group_by,
                    max_groups,
                    input: Box::new(recursed),
                    output_schema: agg_output_schema,
                })
            }
        }

        PhysicalPlan::EventSelect(es) => {
            let native_cols: HashSet<&str> = es
                .output_schema
                .columns()
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            if es.fused_aggregate.is_none() && is_eligible(&aggregates, &group_by, &native_cols) {
                let fused = build_compiled_fusable(
                    &aggregates,
                    &group_by,
                    agg_output_schema.clone(),
                    max_groups,
                );
                let mut es = es;
                es.pre_fusion_output_schema = Some(es.output_schema.clone());
                es.fused_aggregate = Some(fused);
                es.output_schema = agg_output_schema;
                es.input = Box::new(fuse_match_aggregate(*es.input));
                PhysicalPlan::EventSelect(es)
            } else {
                let recursed = fuse_match_aggregate(PhysicalPlan::EventSelect(es));
                PhysicalPlan::Aggregate(AggregatePhysical {
                    aggregates,
                    group_by,
                    max_groups,
                    input: Box::new(recursed),
                    output_schema: agg_output_schema,
                })
            }
        }

        PhysicalPlan::Attribute(attr) => {
            let native_cols: HashSet<&str> = attr
                .output_schema
                .columns()
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            if attr.fused_aggregate.is_none() && is_eligible(&aggregates, &group_by, &native_cols) {
                let fused = build_compiled_fusable(
                    &aggregates,
                    &group_by,
                    agg_output_schema.clone(),
                    max_groups,
                );
                let mut attr = attr;
                attr.pre_fusion_output_schema = Some(attr.output_schema.clone());
                attr.fused_aggregate = Some(fused);
                attr.output_schema = agg_output_schema;
                attr.input = Box::new(fuse_match_aggregate(*attr.input));
                PhysicalPlan::Attribute(attr)
            } else {
                let recursed = fuse_match_aggregate(PhysicalPlan::Attribute(attr));
                PhysicalPlan::Aggregate(AggregatePhysical {
                    aggregates,
                    group_by,
                    max_groups,
                    input: Box::new(recursed),
                    output_schema: agg_output_schema,
                })
            }
        }

        // Non-adjacent: Aggregate over something other than a fusion target.
        // Recurse into the child so nested patterns deeper in the tree are
        // still discovered.
        other_input => {
            let recursed = fuse_match_aggregate(other_input);
            PhysicalPlan::Aggregate(AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input: Box::new(recursed),
                output_schema: agg_output_schema,
            })
        }
    }
}

/// Build a [`CompiledFusableAggregate`] from the aggregate descriptor
/// fields. `CompiledAgg` (physical.rs) and `CompiledAggExpr` (demand.rs)
/// are structurally identical; we copy field-by-field.
fn build_compiled_fusable(
    aggregates: &[crate::physical::CompiledAgg],
    group_by: &[(CompiledExpr, String)],
    output_schema: bqlite_core::OperatorSchema,
    max_groups: usize,
) -> CompiledFusableAggregate {
    CompiledFusableAggregate {
        aggregates: aggregates
            .iter()
            .map(|a| CompiledAggExpr {
                function: a.function,
                arg: a.arg.clone(),
                output_name: a.output_name.clone(),
            })
            .collect(),
        group_by: group_by.to_vec(),
        output_schema,
        max_groups,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Eligibility helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Return `true` when all aggregate argument and group-by expressions
/// reference only columns present in `native_cols`.
///
/// This ensures the aggregate can be evaluated purely from the upstream
/// stateful operator's output, without needing any columns that were
/// dropped at its boundary.
fn is_eligible(
    aggregates: &[crate::physical::CompiledAgg],
    group_by: &[(CompiledExpr, String)],
    native_cols: &HashSet<&str>,
) -> bool {
    // Group-by expressions: the fused path resolves group-by columns by
    // name (HashAccumulator::update_batch reads `group_by_columns` as
    // strings — see aggregate/mod.rs). A non-trivial expression like
    // `UPPER(col)` would survive eligibility but then fail at runtime
    // because the engine adapter only stores the *output* name as the
    // lookup key. Restrict group-by to simple column references for the
    // same reason aggregate args are restricted (TASK-520 bugfix to the
    // pre-existing TASK-320 helper).
    for (expr, _name) in group_by {
        if !is_simple_column_ref(expr) {
            return false;
        }
        if !refs_only_match_cols(expr, native_cols) {
            return false;
        }
    }
    // Check aggregate argument expressions.
    // COUNT(*) has no argument (arg is None) and is always eligible.
    //
    // Non-trivial expressions (CAST, Compare, Arith, etc.) are NOT
    // eligible for fusion because the fused path in
    // `finish_entity_into` passes intermediate per-entity output batches
    // through `update_batch`, which resolves columns by name. It cannot
    // evaluate compiled expressions. These aggregates must go through
    // the non-fused `HashAggregateOperator` path.
    for agg in aggregates {
        if let Some(arg) = &agg.arg {
            // Only fuse simple column references, not computed expressions.
            if !is_simple_column_ref(arg) {
                return false;
            }
            if !refs_only_match_cols(arg, native_cols) {
                return false;
            }
        }
    }
    true
}

/// Return `true` when the expression is a simple column reference
/// (a single `Column` node with no surrounding computation).
fn is_simple_column_ref(expr: &CompiledExpr) -> bool {
    matches!(expr.node, CompiledNode::Column { .. })
}

/// Return `true` when every `Column` node in `expr` is in `match_col_names`.
///
/// Walks the full expression tree recursively. Exhaustively matches every
/// [`CompiledNode`] variant so that adding a new variant in the same crate
/// surfaces a compile error here rather than silently admitting ineligible
/// column references.
///
/// **Intra-crate note.** `CompiledNode` is `#[non_exhaustive]`, but since this
/// function lives in the same crate, the exhaustive match is valid and any new
/// variant will produce a compile error. If this function is ever moved to a
/// different crate, a wildcard arm returning `false` (conservative: treat
/// unknown nodes as ineligible) must be added instead.
fn refs_only_match_cols(expr: &CompiledExpr, match_col_names: &HashSet<&str>) -> bool {
    match &expr.node {
        CompiledNode::Literal(_) => true,
        CompiledNode::Column { name, .. } => match_col_names.contains(name.as_str()),
        CompiledNode::Arith { left, right, .. } | CompiledNode::Compare { left, right, .. } => {
            refs_only_match_cols(left, match_col_names)
                && refs_only_match_cols(right, match_col_names)
        }
        CompiledNode::Unary { operand, .. } => refs_only_match_cols(operand, match_col_names),
        CompiledNode::And { operands, .. } | CompiledNode::Or { operands, .. } => operands
            .iter()
            .all(|op| refs_only_match_cols(op, match_col_names)),
        CompiledNode::Not(inner) => refs_only_match_cols(inner, match_col_names),
        CompiledNode::IsNull { input, .. } => refs_only_match_cols(input, match_col_names),
        CompiledNode::FunctionCall { args, .. } => args
            .iter()
            .all(|a| refs_only_match_cols(a, match_col_names)),
        CompiledNode::Cast { input, .. } | CompiledNode::ImplicitCoerce { input, .. } => {
            refs_only_match_cols(input, match_col_names)
        }
        CompiledNode::InLiteralSet { input, .. } => refs_only_match_cols(input, match_col_names),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{AggFunction, BqlType, ColumnDef, OperatorSchema};

    use crate::compile::{
        CompiledNfa, MatchExecutionConfig, MatchStrategy, NfaState, PatternClass,
    };
    use crate::compiled::{ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode};
    use crate::demand::DemandSet;
    use crate::physical::{
        AggregatePhysical, CompiledAgg, FilterPhysical, PhysicalPlan, ProjectPhysical,
        ProjectPhysicalItem, ScanPhysical, SequenceMatchPhysical, DEFAULT_FILTER_TILE_SIZE,
        DEFAULT_MAX_GROUPS,
    };

    use super::*;

    // ── Test schema helpers ───────────────────────────────────────────────────

    fn scan_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .expect("scan schema")
    }

    /// SequenceMatch output schema for a 3-step funnel with emit_all=true.
    fn match_output_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("step_reached", BqlType::Int),
        ])
        .expect("match output schema")
    }

    /// Aggregate output schema: one SUM per funnel step.
    fn agg_output_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::nullable("reached_step_2", BqlType::Int),
            ColumnDef::nullable("reached_step_3", BqlType::Int),
        ])
        .expect("agg output schema")
    }

    /// A minimal CompiledNfa for tests (2-state linear NFA, no transitions).
    fn minimal_nfa(emit_all: bool) -> CompiledNfa {
        CompiledNfa {
            states: vec![
                NfaState {
                    transitions: vec![],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![],
                    poison_transitions: vec![],
                },
            ],
            accept_state: 1,
            relevant_event_types: BTreeSet::new(),
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            session_window: false,
            emit_all,
            state_to_step: vec![0, 1],
        }
    }

    fn minimal_scan() -> PhysicalPlan {
        PhysicalPlan::Scan(ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: scan_schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        })
    }

    /// Build a SequenceMatchPhysical with the given output schema.
    fn seq_match_physical(output_schema: OperatorSchema) -> Box<SequenceMatchPhysical> {
        Box::new(SequenceMatchPhysical {
            compiled_nfa: minimal_nfa(true),
            strategy: MatchStrategy::StepCounter,
            match_all: false,
            demand: DemandSet::default(),
            execution_config: MatchExecutionConfig::default(),
            fused_aggregate: None,
            input: Box::new(minimal_scan()),
            output_schema,
        })
    }

    /// A column reference expression pointing into the match output schema.
    fn col_ref(name: &str, idx: usize, ty: BqlType, nullable: bool) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: idx,
                name: name.into(),
            },
            result_type: ty,
            nullable,
        }
    }

    /// `Compare(Column(name), Literal(0))` predicate — used in filter tests.
    fn compare_pred(col_name: &str, col_idx: usize) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Greater,
                left: Box::new(col_ref(col_name, col_idx, BqlType::Int, false)),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(bqlite_core::PropertyValue::Int(0)),
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    /// Build an AggregatePhysical that computes SUM(step_reached) over a
    /// SequenceMatch — the fused-funnel shape. `step_reached` is at index 1
    /// in match_output_schema().
    ///
    /// Both aggregate expressions SUM the same `step_reached` source column
    /// but produce distinct named output slots (`reached_step_2`,
    /// `reached_step_3`), mirroring the FUNNEL desugaring pattern where each
    /// step gets its own `SUM(CAST(step_reached >= N AS INT))` output.
    fn funnel_aggregate_plan() -> PhysicalPlan {
        let match_schema = match_output_schema();
        let seq = seq_match_physical(match_schema);

        PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![
                // Represents SUM(CAST(step_reached >= 2 AS INT)) for step 2.
                CompiledAgg {
                    function: AggFunction::Sum,
                    arg: Some(col_ref("step_reached", 1, BqlType::Int, false)),
                    output_name: "reached_step_2".into(),
                },
                // Represents SUM(CAST(step_reached >= 3 AS INT)) for step 3.
                CompiledAgg {
                    function: AggFunction::Sum,
                    arg: Some(col_ref("step_reached", 1, BqlType::Int, false)),
                    output_name: "reached_step_3".into(),
                },
            ],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            output_schema: agg_output_schema(),
        })
    }

    // ── Test: fused-funnel shape (Aggregate directly over SequenceMatch) ──────

    #[test]
    fn fused_funnel_shape_elides_aggregate_and_sets_fused_aggregate() {
        let plan = funnel_aggregate_plan();
        let result = fuse_match_aggregate(plan);

        // The Aggregate node must be gone — the root is now SequenceMatch.
        let PhysicalPlan::SequenceMatch(fused) = result else {
            panic!("expected SequenceMatch root after fusion, got {result:?}");
        };

        // fused_aggregate must be populated.
        assert!(
            fused.fused_aggregate.is_some(),
            "fused_aggregate must be Some after fusion"
        );

        // The output schema must be the aggregate's output schema, not the
        // match's output schema.
        let out_cols: Vec<&str> = fused
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            out_cols,
            vec!["reached_step_2", "reached_step_3"],
            "output schema must match aggregate schema after fusion"
        );

        // The fused aggregate must carry both SUM expressions.
        let fa = fused.fused_aggregate.unwrap();
        assert_eq!(fa.aggregates.len(), 2);
        assert_eq!(fa.aggregates[0].function, AggFunction::Sum);
        assert_eq!(fa.aggregates[0].output_name, "reached_step_2");
        assert_eq!(fa.aggregates[1].output_name, "reached_step_3");
        assert!(fa.group_by.is_empty());
        assert_eq!(fa.output_schema, agg_output_schema());
    }

    // ── Test: aggregate with COUNT(*) (no arg) is eligible ───────────────────

    #[test]
    fn count_star_aggregate_fuses_because_no_column_references() {
        let seq = seq_match_physical(match_output_schema());
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None, // COUNT(*) — no column reference
                output_name: "n".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            output_schema: OperatorSchema::new(vec![ColumnDef::required("n", BqlType::Int)])
                .unwrap(),
        });

        let result = fuse_match_aggregate(plan);

        let PhysicalPlan::SequenceMatch(fused) = result else {
            panic!("expected SequenceMatch root after fusion, got {result:?}");
        };
        assert!(fused.fused_aggregate.is_some(), "COUNT(*) must fuse");
    }

    // ── Test: unfused "match then project then aggregate" shape ───────────────

    #[test]
    fn match_project_aggregate_does_not_fuse() {
        // Plan shape: Aggregate(Project(SequenceMatch(...)))
        // The Project sits between Aggregate and SequenceMatch → no fusion.
        let match_schema = match_output_schema();
        let seq = seq_match_physical(match_schema.clone());

        // Project that passes step_reached through with a rename.
        let proj_out = OperatorSchema::new(vec![ColumnDef::required(
            "step_reached_renamed",
            BqlType::Int,
        )])
        .unwrap();
        let project = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![ProjectPhysicalItem {
                expr: col_ref("step_reached", 1, BqlType::Int, false),
                output_name: "step_reached_renamed".into(),
            }],
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            output_schema: proj_out.clone(),
        });

        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(project),
            output_schema: OperatorSchema::new(vec![ColumnDef::required("n", BqlType::Int)])
                .unwrap(),
        });

        let result = fuse_match_aggregate(plan);

        // Root must still be Aggregate (no fusion happened).
        let PhysicalPlan::Aggregate(agg) = &result else {
            panic!("expected Aggregate root (no fusion), got {result:?}");
        };

        // The intermediate Project must be preserved.
        let PhysicalPlan::Project(_) = agg.input.as_ref() else {
            panic!("expected Project under Aggregate after no-fusion");
        };
    }

    // ── Test: filter between match and aggregate does not fuse in Wave 3 ────────
    // The FilterThenAggregate pattern is deferred — see module doc and
    // docs/design/planner/wave3-lowering.md §4.2 for rationale.

    #[test]
    fn filter_between_match_and_aggregate_is_not_fused_in_wave3() {
        // Plan shape: Aggregate(Filter(SequenceMatch(...)))
        // A Filter sits between the Aggregate and the SequenceMatch.
        // FilterThenAggregate fusion is deferred to a future task — left unchanged.
        let match_schema = match_output_schema();
        let seq = seq_match_physical(match_schema.clone());

        // Filter on step_reached > 0.
        let filter = PhysicalPlan::Filter(FilterPhysical {
            predicate: compare_pred("step_reached", 1),
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: match_schema.clone(),
        });

        let agg_schema =
            OperatorSchema::new(vec![ColumnDef::nullable("total", BqlType::Int)]).unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Sum,
                arg: Some(col_ref("step_reached", 1, BqlType::Int, false)),
                output_name: "total".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(filter),
            output_schema: agg_schema,
        });

        let result = fuse_match_aggregate(plan);

        // Root must still be Aggregate — FilterThenAggregate not fused in Wave 3.
        let PhysicalPlan::Aggregate(agg) = &result else {
            panic!(
                "expected Aggregate root (FilterThenAggregate not fused in Wave 3), \
                 got {result:?}"
            );
        };

        // The Filter must be preserved directly under the Aggregate.
        let PhysicalPlan::Filter(_) = agg.input.as_ref() else {
            panic!("expected Filter under Aggregate after no-fusion (Wave 3 scope)");
        };
    }

    // ── Test: ineligible — aggregate arg references a scan column (not in match)

    #[test]
    fn aggregate_referencing_scan_column_does_not_fuse() {
        // The aggregate references `amount` which is NOT in the SequenceMatch
        // output schema (which only has entity_id and step_reached).
        let seq = seq_match_physical(match_output_schema());

        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Sum,
                // `amount` (index 3) is a scan column, not in match output.
                arg: Some(col_ref("amount", 3, BqlType::Int, true)),
                output_name: "total_amount".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            output_schema: OperatorSchema::new(vec![ColumnDef::nullable(
                "total_amount",
                BqlType::Int,
            )])
            .unwrap(),
        });

        let result = fuse_match_aggregate(plan);

        // Root must still be Aggregate — column eligibility check failed.
        assert!(
            matches!(&result, PhysicalPlan::Aggregate(_)),
            "expected Aggregate root when agg arg references non-match column"
        );
    }

    // ── Test: ineligible — group-by key references a scan column ─────────────

    #[test]
    fn group_by_referencing_scan_column_does_not_fuse() {
        let seq = seq_match_physical(match_output_schema());

        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            // GROUP BY country — not in match output.
            group_by: vec![(
                col_ref("country", 5, BqlType::String, true),
                "country".into(),
            )],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            output_schema: OperatorSchema::new(vec![
                ColumnDef::required("country", BqlType::String),
                ColumnDef::required("n", BqlType::Int),
            ])
            .unwrap(),
        });

        let result = fuse_match_aggregate(plan);

        assert!(
            matches!(&result, PhysicalPlan::Aggregate(_)),
            "expected Aggregate root when group-by references non-match column"
        );
    }

    // ── Test: aggregate by entity_id (a match-output column) is eligible ──────

    #[test]
    fn group_by_entity_id_from_match_output_fuses() {
        // GROUP BY entity_id — entity_id is in match output schema (index 0).
        let match_schema = match_output_schema();
        let seq = seq_match_physical(match_schema.clone());

        let agg_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("n", BqlType::Int),
        ])
        .unwrap();

        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![(
                col_ref("entity_id", 0, BqlType::String, false),
                "entity_id".into(),
            )],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq)),
            output_schema: agg_schema.clone(),
        });

        let result = fuse_match_aggregate(plan);

        let PhysicalPlan::SequenceMatch(fused) = result else {
            panic!("expected SequenceMatch root after GROUP BY entity_id fusion");
        };
        assert!(
            fused.fused_aggregate.is_some(),
            "must fuse when GROUP BY entity_id"
        );
        assert_eq!(fused.output_schema, agg_schema);
    }

    // ── Test: nested pattern — fusion works on inner Aggregate ───────────────

    #[test]
    fn nested_aggregate_over_sequence_match_fuses_inner() {
        // Plan: Limit(Aggregate(SequenceMatch(...)))
        // The fusion pass recurses into the Limit and fuses the inner pair.
        let match_schema = match_output_schema();
        let inner_agg_schema =
            OperatorSchema::new(vec![ColumnDef::nullable("reached", BqlType::Int)]).unwrap();

        let agg = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Sum,
                arg: Some(col_ref("step_reached", 1, BqlType::Int, false)),
                output_name: "reached".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq_match_physical(
                match_schema,
            ))),
            output_schema: inner_agg_schema.clone(),
        });

        let plan = PhysicalPlan::Limit(crate::physical::LimitPhysical {
            count: 10,
            input: Box::new(agg),
            output_schema: inner_agg_schema.clone(),
        });

        let result = fuse_match_aggregate(plan);

        let PhysicalPlan::Limit(limit) = result else {
            panic!("expected Limit at root");
        };
        let PhysicalPlan::SequenceMatch(fused) = *limit.input else {
            panic!("expected fused SequenceMatch under Limit");
        };
        assert!(
            fused.fused_aggregate.is_some(),
            "inner Aggregate must be fused through Limit wrapper"
        );
    }

    // ── Test: DDL leaf is returned unchanged ─────────────────────────────────

    #[test]
    fn ddl_leaf_is_returned_unchanged() {
        use crate::physical::DropTablePhysical;
        let plan = PhysicalPlan::DropTable(DropTablePhysical {
            name: "events".into(),
            output_schema: OperatorSchema::new(vec![]).expect("empty"),
        });
        let result = fuse_match_aggregate(plan);
        assert!(matches!(result, PhysicalPlan::DropTable(_)));
    }

    // ── Test: SequenceMatch without aggregate is returned unchanged ───────────

    #[test]
    fn bare_sequence_match_is_returned_unchanged() {
        let seq = seq_match_physical(match_output_schema());
        let plan = PhysicalPlan::SequenceMatch(seq.clone());
        let result = fuse_match_aggregate(plan);

        let PhysicalPlan::SequenceMatch(result_seq) = result else {
            panic!("expected SequenceMatch unchanged");
        };
        assert!(
            result_seq.fused_aggregate.is_none(),
            "bare SequenceMatch must not get a fused_aggregate"
        );
    }

    // ── Test: non-column-ref aggregate arg blocks fusion ───────────────────────

    #[test]
    fn cast_expression_aggregate_arg_blocks_fusion() {
        // SUM(CAST(step_reached >= 1 AS INT)) — the arg is a Cast
        // expression, not a simple column reference. Fusion must be
        // blocked because the fused path cannot evaluate computed
        // expressions.
        let cast_expr = CompiledExpr {
            node: CompiledNode::Cast {
                input: Box::new(CompiledExpr {
                    node: CompiledNode::Compare {
                        op: bqlite_ast::CompareOp::GreaterOrEqual,
                        kernel: CompareKernel::ArrowKernel(ArrowKernelId::GeInt),
                        left: Box::new(col_ref("step_reached", 1, BqlType::Int, false)),
                        right: Box::new(CompiledExpr {
                            node: CompiledNode::Literal(bqlite_core::PropertyValue::Int(1)),
                            result_type: BqlType::Int,
                            nullable: false,
                        }),
                    },
                    result_type: BqlType::Bool,
                    nullable: false,
                }),
                target_type: BqlType::Int,
                kernel: crate::compiled::CastKernel::ArrowKernel(ArrowKernelId::CastBoolToInt),
            },
            result_type: BqlType::Int,
            nullable: false,
        };

        let agg = AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Sum,
                arg: Some(cast_expr),
                output_name: "signup".into(),
            }],
            group_by: vec![],
            max_groups: 10_000,
            input: Box::new(PhysicalPlan::SequenceMatch(seq_match_physical(
                match_output_schema(),
            ))),
            output_schema: agg_output_schema(),
        };

        let plan = PhysicalPlan::Aggregate(agg);
        let result = fuse_match_aggregate(plan);

        // Fusion must NOT occur: the Aggregate node must remain.
        let PhysicalPlan::Aggregate(result_agg) = result else {
            panic!("expected Aggregate (fusion blocked), got a fused SequenceMatch");
        };
        // The child SequenceMatch must not have a fused_aggregate.
        let PhysicalPlan::SequenceMatch(child_seq) = *result_agg.input else {
            panic!("expected SequenceMatch child");
        };
        assert!(
            child_seq.fused_aggregate.is_none(),
            "SequenceMatch must not have fused_aggregate when arg is a CAST expression"
        );
    }

    // ── TASK-520: Wave 4 stateful operators ─────────────────────────────────

    use crate::demand::DemandSet as DemandSet2;
    use crate::physical::{AttributePhysical, EventSelectPhysical, SessionizePhysical};

    /// Build a `SessionizePhysical` with the given native output schema.
    fn sess_physical(output_schema: OperatorSchema) -> SessionizePhysical {
        SessionizePhysical {
            gap_ns: 1_000,
            end_events: Vec::new(),
            demand: DemandSet2::default(),
            forwarded_columns: Vec::new(),
            fused_aggregate: None,
            input: Box::new(minimal_scan()),
            output_schema,
            pre_fusion_output_schema: None,
        }
    }

    /// Native sessionize output schema: input cols + session_id + session_duration.
    fn sess_native_output() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::required("session_id", BqlType::Int),
            ColumnDef::required("session_duration", BqlType::Int),
        ])
        .expect("sessionize native output schema")
    }

    #[test]
    fn aggregate_over_sessionize_count_star_fuses() {
        // STATS COUNT(*) over a sessionize → simplest fusion shape.
        let agg_schema = OperatorSchema::new(vec![ColumnDef::required("n", BqlType::Int)]).unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::Sessionize(
                sess_physical(sess_native_output()),
            )),
            output_schema: agg_schema.clone(),
        });
        let result = fuse_match_aggregate(plan);
        let PhysicalPlan::Sessionize(fused) = result else {
            panic!("expected fused Sessionize root, got something else");
        };
        assert!(fused.fused_aggregate.is_some());
        assert!(fused.pre_fusion_output_schema.is_some());
        assert_eq!(fused.output_schema, agg_schema);
        // Native schema preserved.
        assert_eq!(
            fused.pre_fusion_output_schema.as_ref().unwrap(),
            &sess_native_output()
        );
    }

    #[test]
    fn aggregate_over_sessionize_group_by_session_id_fuses() {
        let agg_schema = OperatorSchema::new(vec![
            ColumnDef::required("session_id", BqlType::Int),
            ColumnDef::required("n", BqlType::Int),
        ])
        .unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![(
                col_ref("session_id", 3, BqlType::Int, false),
                "session_id".into(),
            )],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::Sessionize(
                sess_physical(sess_native_output()),
            )),
            output_schema: agg_schema.clone(),
        });
        let result = fuse_match_aggregate(plan);
        let PhysicalPlan::Sessionize(fused) = result else {
            panic!("expected fused Sessionize root");
        };
        assert!(fused.fused_aggregate.is_some());
        assert_eq!(fused.output_schema, agg_schema);
    }

    #[test]
    fn aggregate_over_sessionize_referencing_scan_column_does_not_fuse() {
        // SUM over `amount` — not in sessionize native output → block fusion.
        let agg_schema =
            OperatorSchema::new(vec![ColumnDef::nullable("total", BqlType::Int)]).unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Sum,
                arg: Some(col_ref("amount", 5, BqlType::Int, true)),
                output_name: "total".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::Sessionize(
                sess_physical(sess_native_output()),
            )),
            output_schema: agg_schema,
        });
        let result = fuse_match_aggregate(plan);
        // Aggregate stays at root because `amount` is not in sess output.
        assert!(matches!(&result, PhysicalPlan::Aggregate(_)));
    }

    /// Build a minimal `EventSelectPhysical` with the given native output schema.
    fn es_physical(output_schema: OperatorSchema) -> EventSelectPhysical {
        EventSelectPhysical {
            kind: crate::logical::EventSelectKind::First,
            event_types: vec!["purchase".into()],
            predicate: None,
            lookback: None,
            forwarded_columns: vec![],
            fused_aggregate: None,
            input: Box::new(minimal_scan()),
            output_schema,
            pre_fusion_output_schema: None,
        }
    }

    fn es_native_output() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .expect("event_select native output schema")
    }

    #[test]
    fn aggregate_over_event_select_count_star_fuses() {
        let agg_schema = OperatorSchema::new(vec![ColumnDef::required("n", BqlType::Int)]).unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::EventSelect(es_physical(es_native_output()))),
            output_schema: agg_schema.clone(),
        });
        let result = fuse_match_aggregate(plan);
        let PhysicalPlan::EventSelect(fused) = result else {
            panic!("expected fused EventSelect root");
        };
        assert!(fused.fused_aggregate.is_some());
        assert!(fused.pre_fusion_output_schema.is_some());
        assert_eq!(fused.output_schema, agg_schema);
    }

    /// Build a minimal `AttributePhysical` with the given native output schema.
    fn attr_physical(output_schema: OperatorSchema) -> AttributePhysical {
        // touchpoint_key: a Column expression that always references the
        // first input column. Real lowering produces a typed column ref;
        // for the optimizer test we just need any CompiledExpr of String
        // type — the optimizer pass does not introspect it.
        let touchpoint_key = CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: "entity_id".into(),
            },
            result_type: BqlType::String,
            nullable: false,
        };
        AttributePhysical {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["click".into()],
            window_ns: 1_000,
            touchpoint_key,
            forwarded_conversion_columns: vec![],
            fused_aggregate: None,
            conversion_range: None,
            input: Box::new(minimal_scan()),
            output_schema,
            pre_fusion_output_schema: None,
        }
    }

    fn attr_native_output() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_key", BqlType::String),
        ])
        .expect("attribute native output schema")
    }

    #[test]
    fn aggregate_over_attribute_group_by_touchpoint_key_fuses() {
        let agg_schema = OperatorSchema::new(vec![
            ColumnDef::nullable("touchpoint_key", BqlType::String),
            ColumnDef::required("n", BqlType::Int),
        ])
        .unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![(
                col_ref("touchpoint_key", 3, BqlType::String, true),
                "touchpoint_key".into(),
            )],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::Attribute(attr_physical(attr_native_output()))),
            output_schema: agg_schema.clone(),
        });
        let result = fuse_match_aggregate(plan);
        let PhysicalPlan::Attribute(fused) = result else {
            panic!("expected fused Attribute root");
        };
        assert!(fused.fused_aggregate.is_some());
        assert_eq!(fused.output_schema, agg_schema);
    }

    // ── B4 fix: group-by with non-trivial expression must not fuse ─────────
    //
    // `HashAccumulator::update_batch` resolves group-by columns by name.
    // A group-by like `UPPER(col)` would survive a refs-only check but
    // then fail at runtime because the engine adapter only registers the
    // column's *output name* as the lookup key, not the expression. The
    // updated `is_eligible` helper now applies `is_simple_column_ref` to
    // group-by expressions for the same reason it already applied to
    // aggregate args.

    #[test]
    fn group_by_non_trivial_expression_blocks_fusion() {
        let cast_group_by = CompiledExpr {
            node: CompiledNode::Cast {
                input: Box::new(col_ref("step_reached", 1, BqlType::Int, false)),
                target_type: BqlType::String,
                kernel: crate::compiled::CastKernel::ArrowKernel(
                    crate::compiled::ArrowKernelId::CastIntToString,
                ),
            },
            result_type: BqlType::String,
            nullable: false,
        };
        let agg_schema = OperatorSchema::new(vec![
            ColumnDef::required("step_reached_str", BqlType::String),
            ColumnDef::required("n", BqlType::Int),
        ])
        .unwrap();
        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![(cast_group_by, "step_reached_str".into())],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::SequenceMatch(seq_match_physical(
                match_output_schema(),
            ))),
            output_schema: agg_schema,
        });
        let result = fuse_match_aggregate(plan);
        // Aggregate must stay at root — the non-trivial group-by blocks fusion.
        assert!(
            matches!(&result, PhysicalPlan::Aggregate(_)),
            "non-trivial group-by must not fuse (B4 fix)"
        );
    }
}
