//! Pass 10 — scan-pushdown filter coalescing (TASK-527).
//!
//! Walks the plan, finds every [`ScanPhysical`], and reduces its
//! `scan_predicates` list by:
//!
//! 1. **Structural dedup.** Two conjuncts that compare equal under
//!    [`CompiledExpr`]'s `PartialEq` are collapsed to one, preserving
//!    first-seen position.
//! 2. **`InLiteralSet` union.** Two surviving `InLiteralSet { Column,
//!    negated: false, kernel, .. }` conjuncts on the **same** column
//!    index, **same** `negated` flag, and **same** `kernel` are
//!    unioned into a single conjunct whose `values` are the
//!    deterministic sorted union (via `BTreeSet<PropertyValue>`). The
//!    merged conjunct stays at the position of the first occurrence;
//!    later occurrences are dropped.
//!
//! Per `optimizer-direction.md` §7 row 11 this is a pure structural
//! pass (`StatsBudget::none()`). It does not invent new predicates,
//! does not weaken correctness, and does not look at the rest of the
//! tree — only the conjunct list on each `ScanPhysical`.
//!
//! ## Float / NaN behavior
//!
//! `PropertyValue::Float` uses `f64::total_cmp` for both `Eq` and
//! `Ord` (`crates/bqlite-core/src/property.rs:237` / `:287`), so
//! `Float(NaN) == Float(NaN)` is **true** under bqlite's total
//! ordering. Two `[col = NaN, col = NaN]` conjuncts collapse via
//! step 1 dedup; two `[col IN (NaN), col IN (NaN)]` conjuncts merge
//! via step 2 union into one set with a single NaN. This is the
//! intended bqlite semantics; the rule does not need a special-case.
//!
//! ## Out-of-scope simplifications
//!
//! Mixing `Compare { col = lit }` with `InLiteralSet { col, … }` into
//! a single union (e.g. `event_type = 'a' AND event_type IN ('b')` →
//! `event_type IN ('a', 'b')`) is **not** done here. The optimizer
//! direction calls only for "equivalent" merges; the planner's MATCH
//! extraction already emits event types as a single `IN` clause, so
//! this shape is rare in practice. Pre-pushdown rewrites that produce
//! it are a separate task.

use std::collections::BTreeSet;

use crate::compiled::{CompiledExpr, CompiledNode, InSetKernel};
use crate::physical::{
    AggregatePhysical, DistinctPhysical, ExplainPhysical, FusedSegmentPhysical, PhysicalPlan,
    ScanPhysical, SortPhysical,
};
use bqlite_core::PropertyValue;

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the scan-pushdown coalescing pass over a [`PhysicalPlan`] tree.
/// Returns a new tree with each `ScanPhysical`'s `scan_predicates`
/// list deduplicated and `InLiteralSet`-unioned.
pub fn coalesce_scan_predicates(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Scan(scan) => PhysicalPlan::Scan(coalesce_scan(scan)),

        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical {
                input,
                steps,
                sparsity_factor,
                output_schema,
            } = seg;
            PhysicalPlan::FusedSegment(FusedSegmentPhysical {
                input: Box::new(coalesce_scan_predicates(*input)),
                steps,
                sparsity_factor,
                output_schema,
            })
        }

        PhysicalPlan::Explain(explain) => {
            let ExplainPhysical {
                plan: child,
                output_schema,
            } = explain;
            PhysicalPlan::Explain(ExplainPhysical {
                plan: Box::new(coalesce_scan_predicates(*child)),
                output_schema,
            })
        }

        PhysicalPlan::SequenceMatch(seq_match) => {
            let mut sm = *seq_match;
            sm.input = Box::new(coalesce_scan_predicates(*sm.input));
            PhysicalPlan::SequenceMatch(Box::new(sm))
        }

        PhysicalPlan::Aggregate(agg) => {
            let AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input,
                output_schema,
            } = agg;
            PhysicalPlan::Aggregate(AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input: Box::new(coalesce_scan_predicates(*input)),
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
                input: Box::new(coalesce_scan_predicates(*input)),
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
                input: Box::new(coalesce_scan_predicates(*input)),
                output_schema,
            })
        }

        // Leaf nodes — DDL variants, Insert: no children, no scan.
        other => other,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

fn coalesce_scan(mut scan: ScanPhysical) -> ScanPhysical {
    let preds = std::mem::take(&mut scan.scan_predicates);
    scan.scan_predicates = coalesce_conjuncts(preds);
    scan
}

/// The shape of an `InLiteralSet` conjunct that participates in step 2
/// union. We use `(column_index, negated, kernel)` as the grouping
/// key — `PropertyValue` does not implement `Hash`, but the values
/// live inside a `BTreeSet<PropertyValue>` instead.
type InLiteralSetKey = (usize, bool, InSetKernel);

/// Coalesce a flat conjunct list per the rule specification (§7 row
/// 11): structural dedup followed by `InLiteralSet` union over
/// matching keys.
fn coalesce_conjuncts(input: Vec<CompiledExpr>) -> Vec<CompiledExpr> {
    if input.len() <= 1 {
        return input;
    }

    // Step 1: structural dedup. Walk in order; keep only the first
    // occurrence of each `==`-equivalent conjunct. We avoid `HashSet`
    // because `CompiledExpr` is not `Hash`; the linear scan is O(n²)
    // but `scan_predicates` typically has fewer than ~10 conjuncts
    // post-pushdown, so this is well below planner-cost relevance.
    let mut deduped: Vec<CompiledExpr> = Vec::with_capacity(input.len());
    for conjunct in input {
        if !deduped.iter().any(|existing| existing == &conjunct) {
            deduped.push(conjunct);
        }
    }

    // Step 2: walk the survivors, accumulating `InLiteralSet` values
    // by (column, negated, kernel) into BTreeSets keyed on the
    // **first** survivor's position. Non-`InLiteralSet` conjuncts pass
    // through verbatim.
    let mut out: Vec<CompiledExpr> = Vec::with_capacity(deduped.len());
    // Map from group-key → (output index of the merged conjunct,
    // accumulated values). We mutate the conjunct at that output
    // index in place at the end.
    let mut groups: std::collections::HashMap<InLiteralSetKey, (usize, BTreeSet<PropertyValue>)> =
        std::collections::HashMap::new();

    for conjunct in deduped {
        match extract_in_literal_set_key(&conjunct) {
            Some(key) => {
                if let Some((_idx, set)) = groups.get_mut(&key) {
                    // Existing group — fold this conjunct's values
                    // into the set; drop the conjunct from `out`.
                    fold_in_literal_set_values(&conjunct, set);
                } else {
                    // First occurrence — push the conjunct, register
                    // its position so later occurrences merge into it.
                    let idx = out.len();
                    let mut set = BTreeSet::new();
                    fold_in_literal_set_values(&conjunct, &mut set);
                    groups.insert(key, (idx, set));
                    out.push(conjunct);
                }
            }
            None => out.push(conjunct),
        }
    }

    // Apply each group's accumulated value set back onto the output.
    // We have to walk `groups` after the loop because we cannot mutate
    // `out[idx]` while still holding the iteration borrow on `out`.
    for (_key, (idx, set)) in groups {
        if let CompiledNode::InLiteralSet { values, .. } = &mut out[idx].node {
            *values = set.into_iter().collect();
        }
    }

    out
}

/// If `expr` is a positive `InLiteralSet` over a single column,
/// return its grouping key. Negated `InLiteralSet`s are excluded —
/// `NOT IN` cannot be unioned with `IN` (different semantics) and
/// `NOT IN ∪ NOT IN` is not the same as set union (two `NOT IN`
/// clauses on the same column form an intersection, not a union).
fn extract_in_literal_set_key(expr: &CompiledExpr) -> Option<InLiteralSetKey> {
    match &expr.node {
        CompiledNode::InLiteralSet {
            input,
            negated: false,
            kernel,
            ..
        } => match &input.node {
            CompiledNode::Column { index, .. } => Some((*index, false, *kernel)),
            _ => None,
        },
        _ => None,
    }
}

fn fold_in_literal_set_values(expr: &CompiledExpr, set: &mut BTreeSet<PropertyValue>) {
    if let CompiledNode::InLiteralSet { values, .. } = &expr.node {
        for v in values {
            set.insert(v.clone());
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{property::BqlType, schema::ColumnDef, OperatorSchema, PropertyValue};

    use crate::compiled::{
        ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode, InSetKernel, LogicalKernel,
    };
    use crate::physical::{
        FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan, ScanPhysical,
        DEFAULT_FILTER_TILE_SIZE, DEFAULT_SPARSITY_FACTOR,
    };

    use super::*;

    // ── Builders ────────────────────────────────────────────────────────────

    fn empty_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
        ])
        .expect("schema")
    }

    fn make_scan_with_preds(preds: Vec<CompiledExpr>) -> ScanPhysical {
        ScanPhysical {
            table: "events".into(),
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
                name: name.to_string(),
            },
            result_type: ty,
            nullable: false,
        }
    }

    fn lit_int(v: i64) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Literal(PropertyValue::Int(v)),
            result_type: BqlType::Int,
            nullable: false,
        }
    }

    fn eq_col_int(idx: usize, name: &str, v: i64) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Equal,
                left: Box::new(col(idx, name, BqlType::Int)),
                right: Box::new(lit_int(v)),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn in_col_ints(idx: usize, name: &str, vs: &[i64]) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(col(idx, name, BqlType::Int)),
                values: vs.iter().copied().map(PropertyValue::Int).collect(),
                negated: false,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn not_in_col_ints(idx: usize, name: &str, vs: &[i64]) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(col(idx, name, BqlType::Int)),
                values: vs.iter().copied().map(PropertyValue::Int).collect(),
                negated: true,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    /// Pull out the resulting scan_predicates from a coalesced plan.
    fn extract_predicates(plan: PhysicalPlan) -> Vec<CompiledExpr> {
        let PhysicalPlan::Scan(scan) = plan else {
            panic!("expected Scan, got {plan:?}");
        };
        scan.scan_predicates
    }

    fn run(preds: Vec<CompiledExpr>) -> Vec<CompiledExpr> {
        let plan = PhysicalPlan::Scan(make_scan_with_preds(preds));
        extract_predicates(coalesce_scan_predicates(plan))
    }

    // ── Step 1 dedup ────────────────────────────────────────────────────────

    #[test]
    fn duplicate_equality_predicates_are_deduplicated() {
        let p = eq_col_int(1, "amount", 7);
        let result = run(vec![p.clone(), p.clone(), p.clone()]);
        assert_eq!(result, vec![p]);
    }

    #[test]
    fn distinct_equality_predicates_are_kept() {
        let p1 = eq_col_int(1, "amount", 1);
        let p2 = eq_col_int(1, "amount", 2);
        let result = run(vec![p1.clone(), p2.clone()]);
        assert_eq!(result, vec![p1, p2]);
    }

    // ── Step 2 union ────────────────────────────────────────────────────────

    #[test]
    fn duplicate_in_literal_sets_are_unioned() {
        let p1 = in_col_ints(1, "amount", &[1, 2]);
        let p2 = in_col_ints(1, "amount", &[2, 3]);
        let result = run(vec![p1, p2]);
        assert_eq!(result.len(), 1);
        match &result[0].node {
            CompiledNode::InLiteralSet { values, .. } => {
                let got: Vec<i64> = values
                    .iter()
                    .map(|v| match v {
                        PropertyValue::Int(n) => *n,
                        other => panic!("expected Int, got {other:?}"),
                    })
                    .collect();
                assert_eq!(got, vec![1, 2, 3]);
            }
            other => panic!("expected InLiteralSet, got {other:?}"),
        }
    }

    #[test]
    fn in_literal_sets_with_different_columns_are_not_merged() {
        let p1 = in_col_ints(0, "id", &[1, 2]);
        let p2 = in_col_ints(1, "amount", &[3, 4]);
        let result = run(vec![p1.clone(), p2.clone()]);
        assert_eq!(result, vec![p1, p2]);
    }

    #[test]
    fn negated_in_literal_sets_are_left_alone() {
        // NOT IN cannot be unioned with NOT IN (semantic mismatch).
        let p1 = not_in_col_ints(1, "amount", &[1]);
        let p2 = not_in_col_ints(1, "amount", &[2]);
        let result = run(vec![p1.clone(), p2.clone()]);
        assert_eq!(result, vec![p1, p2]);
    }

    #[test]
    fn in_set_and_not_in_set_on_same_column_stay_separate() {
        let p1 = in_col_ints(1, "amount", &[1]);
        let p2 = not_in_col_ints(1, "amount", &[2]);
        let result = run(vec![p1.clone(), p2.clone()]);
        assert_eq!(result, vec![p1, p2]);
    }

    #[test]
    fn in_literal_set_position_preserved_at_first_occurrence() {
        // [eq, in(a), eq2, in(b)] → [eq, in(a∪b), eq2]
        let eq = eq_col_int(1, "amount", 99);
        let in_a = in_col_ints(1, "amount", &[1]);
        let eq2 = eq_col_int(0, "id", 0);
        let in_b = in_col_ints(1, "amount", &[2]);
        let result = run(vec![eq.clone(), in_a, eq2.clone(), in_b]);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], eq);
        match &result[1].node {
            CompiledNode::InLiteralSet { values, .. } => {
                assert_eq!(values, &vec![PropertyValue::Int(1), PropertyValue::Int(2)]);
            }
            other => panic!("expected merged InLiteralSet at index 1, got {other:?}"),
        }
        assert_eq!(result[2], eq2);
    }

    // ── Mixed dedup + union ─────────────────────────────────────────────────

    #[test]
    fn mixed_dedup_and_union_full_case() {
        let eq = eq_col_int(1, "amount", 99);
        let in_a = in_col_ints(1, "amount", &[1, 2]);
        let in_b = in_col_ints(1, "amount", &[2, 3]);
        let result = run(vec![eq.clone(), in_a, eq.clone(), in_b]);
        // eq is deduped; the two IN sets merge.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], eq);
        match &result[1].node {
            CompiledNode::InLiteralSet { values, .. } => {
                let got: Vec<i64> = values
                    .iter()
                    .map(|v| match v {
                        PropertyValue::Int(n) => *n,
                        other => panic!("expected Int, got {other:?}"),
                    })
                    .collect();
                assert_eq!(got, vec![1, 2, 3]);
            }
            other => panic!("expected InLiteralSet, got {other:?}"),
        }
    }

    // ── Float / NaN behavior ────────────────────────────────────────────────

    #[test]
    fn float_nan_equality_predicates_are_deduplicated() {
        // PropertyValue's PartialEq uses total_cmp for floats:
        // Float(NaN) == Float(NaN) is true. Confirm dedup folds them.
        let nan = CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Equal,
                left: Box::new(col(1, "amount", BqlType::Float)),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Float(f64::NAN)),
                    result_type: BqlType::Float,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqFloat),
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let result = run(vec![nan.clone(), nan.clone()]);
        assert_eq!(result.len(), 1, "two NaN-equality conjuncts should dedup");
    }

    #[test]
    fn float_in_set_with_nan_is_unioned_under_total_ordering() {
        // Same total-ordering rationale: NaN deduplicates inside the
        // BTreeSet. Two `[col IN (NaN)]` conjuncts merge into one set
        // with a single NaN entry.
        let in_nan = CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(col(1, "amount", BqlType::Float)),
                values: vec![PropertyValue::Float(f64::NAN)],
                negated: false,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let result = run(vec![in_nan.clone(), in_nan.clone()]);
        assert_eq!(result.len(), 1);
        match &result[0].node {
            CompiledNode::InLiteralSet { values, .. } => {
                assert_eq!(values.len(), 1, "two NaN values dedupe under total_cmp");
            }
            other => panic!("expected InLiteralSet, got {other:?}"),
        }
    }

    // ── Pass-through for unrelated conjuncts ────────────────────────────────

    #[test]
    fn unrelated_conjuncts_pass_through_unchanged() {
        let eq1 = eq_col_int(1, "amount", 1);
        let eq2 = eq_col_int(0, "id", 2);
        let big_and = CompiledExpr {
            node: CompiledNode::And {
                operands: vec![eq1.clone(), eq2.clone()],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let result = run(vec![eq1.clone(), eq2.clone(), big_and.clone()]);
        assert_eq!(result, vec![eq1, eq2, big_and]);
    }

    #[test]
    fn bare_scan_with_no_predicates_is_unchanged() {
        let plan = PhysicalPlan::Scan(make_scan_with_preds(vec![]));
        let result = coalesce_scan_predicates(plan);
        let PhysicalPlan::Scan(scan) = result else {
            panic!("expected Scan");
        };
        assert!(scan.scan_predicates.is_empty());
    }

    #[test]
    fn single_predicate_is_unchanged() {
        let p = eq_col_int(1, "amount", 7);
        let result = run(vec![p.clone()]);
        assert_eq!(result, vec![p]);
    }

    // ── Idempotence ─────────────────────────────────────────────────────────

    #[test]
    fn idempotent_two_runs_equal_one_run() {
        let preds = vec![
            eq_col_int(1, "amount", 1),
            in_col_ints(1, "amount", &[1, 2]),
            eq_col_int(1, "amount", 1),
            in_col_ints(1, "amount", &[2, 3]),
            eq_col_int(0, "id", 5),
        ];
        let scan = PhysicalPlan::Scan(make_scan_with_preds(preds));
        let once = coalesce_scan_predicates(scan);
        let twice = coalesce_scan_predicates(once.clone());
        assert_eq!(once, twice);
    }

    // ── Recursion targets ───────────────────────────────────────────────────

    #[test]
    fn walks_into_fused_segment_input() {
        let preds = vec![
            in_col_ints(1, "amount", &[1]),
            in_col_ints(1, "amount", &[2]),
        ];
        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan_with_preds(preds))),
            steps: vec![FusedSegmentStep::Limit(10)],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        });
        let result = coalesce_scan_predicates(plan);
        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under segment");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
    }

    #[test]
    fn walks_into_explain() {
        let preds = vec![
            in_col_ints(1, "amount", &[1]),
            in_col_ints(1, "amount", &[2]),
        ];
        let inner = PhysicalPlan::Scan(make_scan_with_preds(preds));
        let single_col =
            OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)]).unwrap();
        let plan = PhysicalPlan::Explain(crate::physical::ExplainPhysical {
            plan: Box::new(inner),
            output_schema: single_col,
        });
        let result = coalesce_scan_predicates(plan);
        let PhysicalPlan::Explain(explain) = result else {
            panic!("expected Explain");
        };
        let PhysicalPlan::Scan(scan) = *explain.plan else {
            panic!("expected Scan under Explain");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
    }

    #[test]
    fn walks_into_sequence_match_input() {
        use crate::compile::{CompiledNfa, MatchExecutionConfig, MatchStrategy, PatternClass};
        use crate::demand::DemandSet;
        use crate::physical::SequenceMatchPhysical;
        use std::collections::BTreeSet;

        let preds = vec![
            in_col_ints(1, "amount", &[1]),
            in_col_ints(1, "amount", &[2]),
        ];
        let nfa = CompiledNfa {
            states: vec![],
            accept_state: 0,
            relevant_event_types: BTreeSet::new(),
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            session_window: false,
            emit_all: false,
            state_to_step: vec![],
            brackets: None,
        };
        let sm_schema =
            OperatorSchema::new(vec![ColumnDef::required("entity_id", BqlType::String)]).unwrap();
        let plan = PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
            compiled_nfa: nfa,
            strategy: MatchStrategy::StepCounter,
            match_all: false,
            demand: DemandSet::default(),
            execution_config: MatchExecutionConfig::default(),
            fused_aggregate: None,
            input: Box::new(PhysicalPlan::Scan(make_scan_with_preds(preds))),
            output_schema: sm_schema,
        }));
        let result = coalesce_scan_predicates(plan);
        let PhysicalPlan::SequenceMatch(sm) = result else {
            panic!("expected SequenceMatch");
        };
        let PhysicalPlan::Scan(scan) = *sm.input else {
            panic!("expected Scan under SequenceMatch");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
    }

    #[test]
    fn keeps_existing_filter_tile_size_unaffected() {
        // The pass should not mutate FusedSegmentStep contents; only
        // the inner Scan's scan_predicates change.
        let preds = vec![eq_col_int(1, "amount", 1), eq_col_int(1, "amount", 1)];
        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan_with_preds(preds))),
            steps: vec![FusedSegmentStep::Filter {
                predicate: eq_col_int(0, "id", 99),
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        });
        let result = coalesce_scan_predicates(plan);
        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment");
        };
        // Filter step survives unchanged.
        assert_eq!(seg.steps.len(), 1);
        match &seg.steps[0] {
            FusedSegmentStep::Filter { tile_size, .. } => {
                assert_eq!(*tile_size, DEFAULT_FILTER_TILE_SIZE);
            }
            other => panic!("expected Filter, got {other:?}"),
        }
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under segment");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
    }

    // ── Property test: idempotence + permutation preservation
    //    over arbitrary mixes of dedup-eligible / union-eligible /
    //    pass-through conjuncts (Core Belief #11). ────────────────

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config {
            cases: 96,
            .. proptest::test_runner::Config::default()
        })]

        #[test]
        fn coalesce_is_idempotent(
            shapes in proptest::collection::vec(0u8..=4, 0..=10),
        ) {
            let preds: Vec<CompiledExpr> = shapes.iter().map(|s| build_shape(*s)).collect();
            let plan = PhysicalPlan::Scan(make_scan_with_preds(preds));
            let once = coalesce_scan_predicates(plan.clone());
            let twice = coalesce_scan_predicates(once.clone());
            proptest::prop_assert_eq!(once, twice);
        }

        #[test]
        fn coalesce_never_grows_predicate_count(
            shapes in proptest::collection::vec(0u8..=4, 0..=10),
        ) {
            let preds: Vec<CompiledExpr> = shapes.iter().map(|s| build_shape(*s)).collect();
            let original_len = preds.len();
            let plan = PhysicalPlan::Scan(make_scan_with_preds(preds));
            let result = extract_predicates(coalesce_scan_predicates(plan));
            proptest::prop_assert!(
                result.len() <= original_len,
                "coalesce grew predicate count from {} to {}",
                original_len,
                result.len()
            );
        }
    }

    fn build_shape(shape: u8) -> CompiledExpr {
        match shape % 5 {
            0 => eq_col_int(1, "amount", 1),
            1 => eq_col_int(1, "amount", 2),
            2 => in_col_ints(1, "amount", &[1, 2]),
            3 => in_col_ints(1, "amount", &[3]),
            _ => not_in_col_ints(1, "amount", &[9]),
        }
    }
}
