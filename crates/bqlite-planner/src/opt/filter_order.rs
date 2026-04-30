//! Pass 9 — stateless filter conjunct ordering inside fused segment
//! (TASK-527).
//!
//! Reorders top-level conjuncts of a top-level [`CompiledNode::And`]
//! sitting inside a [`FusedSegmentStep::Filter`] step by a static
//! "cheapest-first" heuristic. Per `optimizer-direction.md` §7 row 10:
//!
//! > Equality literals first, then small `IN` sets, then range
//! > comparisons, then `LIKE`/regex, then arbitrary scalar functions.
//! > Tie-break by source order.
//!
//! The heuristic is independent of the data (no `PlannerStats` access,
//! `StatsBudget::none()`); it produces a stable plan even on synthetic
//! inputs where the heuristic is wrong, matching the determinism
//! contract in `optimizer-direction.md` §8.1. Per-batch sparsity-driven
//! materialization is a runtime decision, not a plan-time one
//! (`execution-model.md` §3.8.3).
//!
//! ## Scope of recursion
//!
//! The rule reorders only the **top-level** conjuncts of a **top-level
//! `CompiledNode::And`** inside a `Filter` step's predicate. It does
//! not recurse into `Or` / `Not` / nested `And`s. A
//! `Filter { predicate: Or(a, b) }` is left alone (no top-level `And`).
//! A `Filter { predicate: And(a, Or(b, c), d) }` reorders the three
//! top-level conjuncts but does not look inside the `Or`. This matches
//! the spec's "equality literals first … tie-break by source order",
//! which is about top-level conjunct order.
//!
//! Conjuncts are also not moved across `Filter` steps — those have
//! semantically distinct positions when split by an upstream
//! `Project` / `Limit` step.

use crate::compiled::{CompiledExpr, CompiledNode};
use crate::physical::{
    AggregatePhysical, DistinctPhysical, ExplainPhysical, FusedSegmentPhysical, FusedSegmentStep,
    PhysicalPlan, SortPhysical,
};

/// `IN`-set size threshold separating "small IN" (rank 2) from "large
/// IN" (rank 4). Hard-coded per `optimizer-direction.md` §10.4: tuning
/// constants live in code, not in design docs.
///
/// Rationale: a small literal set evaluates as an unrolled
/// equality fan-out per row (linear in the set, but with the constants
/// of branchless integer / inline-string compares), while a larger set
/// has to pay the upfront cost of building an Arrow `is_in` set kernel
/// before any rows are tested. The exact crossover depends on the
/// column type and dictionary state; eight is a conservative default
/// that orders any genuinely small enumeration ahead of range
/// comparisons. Bench-driven tuning of this constant is owned by the
/// Wave 5 bench gate (TASK-526).
const SMALL_IN_THRESHOLD: usize = 8;

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the stateless-filter conjunct-ordering pass over a
/// [`PhysicalPlan`] tree. Returns a new tree with every fused-segment
/// `Filter` step's `And`-rooted predicate reordered by the rank table
/// in [`rank`].
pub fn order_stateless_filters(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical {
                input,
                steps,
                sparsity_factor,
                output_schema,
            } = seg;
            let new_steps = steps
                .into_iter()
                .map(|step| match step {
                    FusedSegmentStep::Filter {
                        predicate,
                        tile_size,
                    } => FusedSegmentStep::Filter {
                        predicate: reorder_top_level_conjuncts(predicate),
                        tile_size,
                    },
                    other => other,
                })
                .collect();
            PhysicalPlan::FusedSegment(FusedSegmentPhysical {
                input: Box::new(order_stateless_filters(*input)),
                steps: new_steps,
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
                plan: Box::new(order_stateless_filters(*child)),
                output_schema,
            })
        }

        PhysicalPlan::SequenceMatch(seq_match) => {
            let mut sm = *seq_match;
            sm.input = Box::new(order_stateless_filters(*sm.input));
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
                input: Box::new(order_stateless_filters(*input)),
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
                input: Box::new(order_stateless_filters(*input)),
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
                input: Box::new(order_stateless_filters(*input)),
                output_schema,
            })
        }

        // Leaf nodes — Scan, DDL variants, Insert: no child plan to recurse,
        // and no `Filter` step to reorder.
        other => other,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Reorder the top-level conjuncts of `predicate` if and only if its
/// root is a [`CompiledNode::And`]. Sort is stable on the input order
/// (tie-break by original index) so equally-ranked conjuncts preserve
/// source order — required for determinism per `optimizer-direction.md`
/// §8.1.
///
/// The `And.kernel`, `result_type`, and `nullable` fields are
/// preserved verbatim. Reordering is sound for `LogicalKernel::ArrowKleene`
/// because `and_kleene` is commutative for both definedness and
/// nullability under three-valued Kleene semantics.
fn reorder_top_level_conjuncts(predicate: CompiledExpr) -> CompiledExpr {
    let CompiledExpr {
        node,
        result_type,
        nullable,
    } = predicate;
    match node {
        CompiledNode::And { operands, kernel } => {
            // Defense in depth: an `And` with a single operand is a
            // no-op — `pushdown.rs` debug_asserts this never happens but
            // we still tolerate it without re-shaping.
            if operands.len() <= 1 {
                return CompiledExpr {
                    node: CompiledNode::And { operands, kernel },
                    result_type,
                    nullable,
                };
            }

            // Pair each operand with its original index so the stable
            // sort tie-breaks by source order. We cannot use
            // `sort_by_key(rank)` directly because that would require
            // borrowing the operand to compute `rank`; precomputing the
            // rank into the pair sidesteps the borrow.
            let mut indexed: Vec<(u8, usize, CompiledExpr)> = operands
                .into_iter()
                .enumerate()
                .map(|(idx, op)| (rank(&op), idx, op))
                .collect();
            indexed.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let new_operands: Vec<CompiledExpr> =
                indexed.into_iter().map(|(_, _, op)| op).collect();
            CompiledExpr {
                node: CompiledNode::And {
                    operands: new_operands,
                    kernel,
                },
                result_type,
                nullable,
            }
        }
        other => CompiledExpr {
            node: other,
            result_type,
            nullable,
        },
    }
}

/// Rank a single conjunct for ordering. Lower ranks run first.
///
/// | Rank | Shape |
/// |------|-------|
/// | 0    | `Compare { Eq/Ne, Column vs Literal }` (or symmetric) |
/// | 1    | `IsNull { Column, .. }` |
/// | 2    | `InLiteralSet { Column, .. }` with `≤ SMALL_IN_THRESHOLD` literals |
/// | 3    | `Compare { Lt/Le/Gt/Ge, Column vs Literal }` (or symmetric) |
/// | 4    | `InLiteralSet { Column, .. }` with `> SMALL_IN_THRESHOLD` literals |
/// | 5    | `FunctionCall` whose signature name is `like` |
/// | 6    | Everything else (other function calls, column-vs-column, nested logical, arithmetic-bearing compares) |
fn rank(expr: &CompiledExpr) -> u8 {
    use bqlite_ast::expr::CompareOp;
    match &expr.node {
        CompiledNode::Compare {
            op, left, right, ..
        } if is_column_vs_literal(left, right) => match op {
            CompareOp::Equal | CompareOp::NotEqual => 0,
            CompareOp::Less
            | CompareOp::LessOrEqual
            | CompareOp::Greater
            | CompareOp::GreaterOrEqual => 3,
        },
        CompiledNode::IsNull { input, .. } if is_column(input) => 1,
        CompiledNode::InLiteralSet { input, values, .. } if is_column(input) => {
            if values.len() <= SMALL_IN_THRESHOLD {
                2
            } else {
                4
            }
        }
        CompiledNode::FunctionCall { signature, .. } if signature.name == "like" => 5,
        _ => 6,
    }
}

fn is_column(expr: &CompiledExpr) -> bool {
    matches!(expr.node, CompiledNode::Column { .. })
}

fn is_literal(expr: &CompiledExpr) -> bool {
    matches!(expr.node, CompiledNode::Literal(_))
}

fn is_column_vs_literal(left: &CompiledExpr, right: &CompiledExpr) -> bool {
    (is_column(left) && is_literal(right)) || (is_literal(left) && is_column(right))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{property::BqlType, schema::ColumnDef, OperatorSchema, PropertyValue};

    use crate::compiled::{
        ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode, FunctionKernel, InSetKernel,
        LogicalKernel,
    };
    use crate::expr::ScalarFunctionSig;
    use crate::physical::{
        FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan, ProjectPhysicalItem, ScanPhysical,
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

    fn make_scan() -> ScanPhysical {
        ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: empty_schema(),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
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

    fn lit_str(s: &str) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Literal(PropertyValue::String(s.into())),
            result_type: BqlType::String,
            nullable: false,
        }
    }

    fn cmp(op: CompareOp, left: CompiledExpr, right: CompiledExpr) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op,
                left: Box::new(left),
                right: Box::new(right),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn eq_col_lit(idx: usize, name: &str, v: i64) -> CompiledExpr {
        cmp(CompareOp::Equal, col(idx, name, BqlType::Int), lit_int(v))
    }

    fn gt_col_lit(idx: usize, name: &str, v: i64) -> CompiledExpr {
        cmp(CompareOp::Greater, col(idx, name, BqlType::Int), lit_int(v))
    }

    fn isnull_col(idx: usize, name: &str) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::IsNull {
                input: Box::new(col(idx, name, BqlType::Int)),
                negated: false,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn in_set(idx: usize, name: &str, n: usize) -> CompiledExpr {
        let values: Vec<PropertyValue> = (0..n as i64).map(PropertyValue::Int).collect();
        CompiledExpr {
            node: CompiledNode::InLiteralSet {
                input: Box::new(col(idx, name, BqlType::Int)),
                values,
                negated: false,
                kernel: InSetKernel::ArrowIsIn,
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn like_call() -> CompiledExpr {
        let sig = Arc::new(ScalarFunctionSig {
            name: "like".to_string(),
            params: vec![BqlType::String, BqlType::String],
            return_type: BqlType::Bool,
            nullable: true,
        });
        CompiledExpr {
            node: CompiledNode::FunctionCall {
                signature: sig,
                args: vec![col(0, "id", BqlType::String), lit_str("foo%")],
                kernel: FunctionKernel::Registered(crate::compiled::FunctionId(0)),
            },
            result_type: BqlType::Bool,
            nullable: true,
        }
    }

    fn other_function_call() -> CompiledExpr {
        let sig = Arc::new(ScalarFunctionSig {
            name: "lower".to_string(),
            params: vec![BqlType::String],
            return_type: BqlType::String,
            nullable: false,
        });
        let inner = CompiledExpr {
            node: CompiledNode::FunctionCall {
                signature: sig,
                args: vec![col(0, "id", BqlType::String)],
                kernel: FunctionKernel::Registered(crate::compiled::FunctionId(1)),
            },
            result_type: BqlType::String,
            nullable: false,
        };
        // Wrap into a Compare to make a Bool predicate; this is the
        // "everything else" rank-6 case (function-bearing comparison).
        cmp(CompareOp::Equal, inner, lit_str("bar"))
    }

    fn and_of(operands: Vec<CompiledExpr>) -> CompiledExpr {
        let nullable = operands.iter().any(|o| o.nullable);
        CompiledExpr {
            node: CompiledNode::And {
                operands,
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable,
        }
    }

    fn filter_segment(predicate: CompiledExpr) -> PhysicalPlan {
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            steps: vec![FusedSegmentStep::Filter {
                predicate,
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        })
    }

    /// Pull the leading Filter predicate out of a result plan.
    fn extract_filter_pred(plan: PhysicalPlan) -> CompiledExpr {
        let PhysicalPlan::FusedSegment(seg) = plan else {
            panic!("expected FusedSegment, got {plan:?}");
        };
        let step = seg.steps.into_iter().next().expect("at least one step");
        match step {
            FusedSegmentStep::Filter { predicate, .. } => predicate,
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    fn ranks_of(and_expr: &CompiledExpr) -> Vec<u8> {
        match &and_expr.node {
            CompiledNode::And { operands, .. } => operands.iter().map(rank).collect(),
            _ => panic!("expected And"),
        }
    }

    // ── Stable equality conjuncts ────────────────────────────────────────────

    #[test]
    fn pure_equality_conjuncts_are_stable() {
        let preds = vec![
            eq_col_lit(1, "amount", 1),
            eq_col_lit(1, "amount", 2),
            eq_col_lit(1, "amount", 3),
        ];
        let plan = filter_segment(and_of(preds.clone()));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        assert_eq!(operands, preds);
    }

    // ── Range after equality ────────────────────────────────────────────────

    #[test]
    fn range_orders_after_equality() {
        let r = gt_col_lit(1, "amount", 0);
        let e = eq_col_lit(1, "amount", 1);
        let plan = filter_segment(and_of(vec![r.clone(), e.clone()]));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        assert_eq!(operands, vec![e, r]);
    }

    // ── IS NULL between equality and range ──────────────────────────────────

    #[test]
    fn is_null_orders_between_equality_and_range() {
        let r = gt_col_lit(1, "amount", 0);
        let n = isnull_col(1, "amount");
        let e = eq_col_lit(1, "amount", 1);
        let plan = filter_segment(and_of(vec![r.clone(), n.clone(), e.clone()]));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        assert_eq!(operands, vec![e, n, r]);
    }

    // ── Small IN before range ───────────────────────────────────────────────

    #[test]
    fn small_in_orders_before_range() {
        let r = gt_col_lit(1, "amount", 0);
        let s = in_set(1, "amount", 4); // ≤ 8 → small
        let plan = filter_segment(and_of(vec![r.clone(), s.clone()]));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        assert_eq!(operands, vec![s, r]);
    }

    // ── Large IN after range, before LIKE ───────────────────────────────────

    #[test]
    fn large_in_orders_after_range_and_before_like() {
        let r = gt_col_lit(1, "amount", 0);
        let big = in_set(1, "amount", 16); // > 8 → large
        let l = like_call();
        let plan = filter_segment(and_of(vec![l.clone(), big.clone(), r.clone()]));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        // Expected order: range (rank 3), big-IN (rank 4), like (rank 5).
        assert_eq!(operands, vec![r, big, l]);
    }

    // ── LIKE before "everything else" ───────────────────────────────────────

    #[test]
    fn like_orders_before_other_function_calls() {
        let l = like_call();
        let other = other_function_call();
        let plan = filter_segment(and_of(vec![other.clone(), l.clone()]));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        assert_eq!(operands, vec![l, other]);
    }

    // ── Single conjunct: not an And, returned unchanged ─────────────────────

    #[test]
    fn single_conjunct_predicate_is_returned_unchanged() {
        let pred = eq_col_lit(1, "amount", 7);
        let plan = filter_segment(pred.clone());
        let result = order_stateless_filters(plan);
        let got = extract_filter_pred(result);
        assert_eq!(got, pred);
    }

    // ── Single-operand And: defense in depth, no-op ─────────────────────────

    #[test]
    fn single_operand_and_is_a_noop() {
        let inner = eq_col_lit(1, "amount", 7);
        let pred = and_of(vec![inner.clone()]);
        let plan = filter_segment(pred.clone());
        let result = order_stateless_filters(plan);
        let got = extract_filter_pred(result);
        assert_eq!(got, pred);
    }

    // ── Or / Not / nested logicals: not recursed into ───────────────────────

    #[test]
    fn or_predicate_is_left_alone_no_top_level_and() {
        let inner = CompiledExpr {
            node: CompiledNode::Or {
                operands: vec![gt_col_lit(1, "amount", 0), eq_col_lit(1, "amount", 1)],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let plan = filter_segment(inner.clone());
        let result = order_stateless_filters(plan);
        let got = extract_filter_pred(result);
        assert_eq!(got, inner);
    }

    #[test]
    fn nested_and_inside_or_is_not_recursed_into() {
        // And(eq, Or(eq, gt), gt) — top-level conjuncts get reordered
        // (eq, Or, gt → eq, gt, Or — Or is rank 6, gt rank 3, eq rank 0).
        let inner_or = CompiledExpr {
            node: CompiledNode::Or {
                operands: vec![eq_col_lit(1, "amount", 9), gt_col_lit(1, "amount", 9)],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let e = eq_col_lit(1, "amount", 1);
        let g = gt_col_lit(1, "amount", 0);
        let plan = filter_segment(and_of(vec![e.clone(), inner_or.clone(), g.clone()]));
        let result = order_stateless_filters(plan);
        let pred = extract_filter_pred(result);
        let CompiledNode::And { operands, .. } = pred.node else {
            panic!("expected And");
        };
        // eq (0), gt (3), Or (6).
        assert_eq!(operands, vec![e, g, inner_or]);
    }

    // ── Non-Filter steps untouched ──────────────────────────────────────────

    #[test]
    fn non_filter_steps_are_untouched() {
        let pred = and_of(vec![gt_col_lit(1, "amount", 0), eq_col_lit(1, "amount", 1)]);
        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            steps: vec![
                FusedSegmentStep::Filter {
                    predicate: pred,
                    tile_size: DEFAULT_FILTER_TILE_SIZE,
                },
                FusedSegmentStep::Project(vec![ProjectPhysicalItem {
                    expr: col(1, "amount", BqlType::Int),
                    output_name: "amount".into(),
                }]),
                FusedSegmentStep::Limit(5),
            ],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        });
        let result = order_stateless_filters(plan);
        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment");
        };
        assert!(matches!(seg.steps[1], FusedSegmentStep::Project(_)));
        assert!(matches!(seg.steps[2], FusedSegmentStep::Limit(5)));
    }

    // ── Recursion targets ───────────────────────────────────────────────────

    #[test]
    fn recurses_into_explain() {
        let pred = and_of(vec![gt_col_lit(1, "amount", 0), eq_col_lit(1, "amount", 1)]);
        let inner = filter_segment(pred);
        let single_col =
            OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)]).unwrap();
        let plan = PhysicalPlan::Explain(crate::physical::ExplainPhysical {
            plan: Box::new(inner),
            output_schema: single_col,
        });
        let result = order_stateless_filters(plan);
        let PhysicalPlan::Explain(explain) = result else {
            panic!("expected Explain");
        };
        let pred = extract_filter_pred(*explain.plan);
        assert_eq!(ranks_of(&pred), vec![0, 3]);
    }

    // ── Idempotence ─────────────────────────────────────────────────────────

    #[test]
    fn idempotent_two_runs_equal_one_run() {
        let plan = filter_segment(and_of(vec![
            like_call(),
            gt_col_lit(1, "amount", 0),
            in_set(1, "amount", 16),
            isnull_col(1, "amount"),
            eq_col_lit(1, "amount", 1),
        ]));
        let once = order_stateless_filters(plan.clone());
        let twice = order_stateless_filters(once.clone());
        assert_eq!(extract_filter_pred(once), extract_filter_pred(twice));
    }

    // ── Nullability and kernel are preserved ────────────────────────────────

    #[test]
    fn nullable_flag_and_kernel_are_preserved() {
        // Build an And with one nullable conjunct so the And.nullable
        // is true; assert the post-reorder And keeps nullable=true and
        // the same ArrowKleene kernel.
        let nullable_eq = CompiledExpr {
            nullable: true,
            ..eq_col_lit(1, "amount", 1)
        };
        let r = gt_col_lit(1, "amount", 0);
        let pred = and_of(vec![r.clone(), nullable_eq.clone()]);
        assert!(pred.nullable, "test setup: outer And should be nullable");
        let plan = filter_segment(pred);
        let result = order_stateless_filters(plan);
        let got = extract_filter_pred(result);
        assert!(got.nullable, "nullable flag must be preserved");
        match got.node {
            CompiledNode::And { kernel, operands } => {
                assert!(matches!(kernel, LogicalKernel::ArrowKleene));
                assert_eq!(operands, vec![nullable_eq, r]);
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    // ── Walks into other interior nodes ─────────────────────────────────────

    #[test]
    fn recurses_into_sequence_match_input() {
        use crate::compile::{CompiledNfa, MatchExecutionConfig, MatchStrategy, PatternClass};
        use crate::demand::DemandSet;
        use crate::physical::SequenceMatchPhysical;
        use std::collections::BTreeSet;

        let inner = filter_segment(and_of(vec![
            gt_col_lit(1, "amount", 0),
            eq_col_lit(1, "amount", 1),
        ]));
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
        };
        let sm_schema =
            OperatorSchema::new(vec![ColumnDef::required("entity_id", BqlType::String)]).unwrap();
        let sm = PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
            compiled_nfa: nfa,
            strategy: MatchStrategy::StepCounter,
            match_all: false,
            demand: DemandSet::default(),
            execution_config: MatchExecutionConfig::default(),
            fused_aggregate: None,
            input: Box::new(inner),
            output_schema: sm_schema,
        }));
        let result = order_stateless_filters(sm);
        let PhysicalPlan::SequenceMatch(sm) = result else {
            panic!("expected SequenceMatch");
        };
        let pred = extract_filter_pred(*sm.input);
        assert_eq!(ranks_of(&pred), vec![0, 3]);
    }

    #[test]
    fn recurses_into_distinct_input() {
        let inner = filter_segment(and_of(vec![
            gt_col_lit(1, "amount", 0),
            eq_col_lit(1, "amount", 1),
        ]));
        let dist_schema = empty_schema();
        let dist = PhysicalPlan::Distinct(crate::physical::DistinctPhysical {
            max_groups: 1_000,
            input: Box::new(inner),
            output_schema: dist_schema,
        });
        let result = order_stateless_filters(dist);
        let PhysicalPlan::Distinct(dist) = result else {
            panic!("expected Distinct");
        };
        let pred = extract_filter_pred(*dist.input);
        assert_eq!(ranks_of(&pred), vec![0, 3]);
    }

    // ── Property test (Core Belief #11): planner rewrite fixpoint and
    //    rank-monotonic output across arbitrary conjunct vectors ─────────────

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config {
            cases: 96,
            .. proptest::test_runner::Config::default()
        })]

        #[test]
        fn reorder_is_idempotent_and_rank_monotonic(
            shapes in proptest::collection::vec(0u8..=6, 1..=12),
        ) {
            // Materialize each shape into a representative conjunct.
            let conjuncts: Vec<CompiledExpr> = shapes
                .iter()
                .map(|s| build_shape(*s))
                .collect();

            // Single-conjunct case: the rule returns the predicate
            // unchanged (no top-level And), idempotence is trivial.
            if conjuncts.len() == 1 {
                let plan = filter_segment(conjuncts[0].clone());
                let once = order_stateless_filters(plan.clone());
                let twice = order_stateless_filters(once.clone());
                proptest::prop_assert_eq!(extract_filter_pred(once), extract_filter_pred(twice));
                return Ok(());
            }

            let pred = and_of(conjuncts.clone());
            let plan = filter_segment(pred);
            let once = order_stateless_filters(plan.clone());
            let twice = order_stateless_filters(once.clone());

            // Idempotence.
            let once_pred = extract_filter_pred(once);
            let twice_pred = extract_filter_pred(twice.clone());
            proptest::prop_assert_eq!(once_pred.clone(), twice_pred);

            // Rank monotonicity: ranks of the reordered conjunct list
            // are non-decreasing.
            let ranks = ranks_of(&once_pred);
            for w in ranks.windows(2) {
                proptest::prop_assert!(w[0] <= w[1], "ranks not monotonic: {ranks:?}");
            }

            // Permutation preservation: the multiset of conjuncts is
            // identical to the input — reordering does not drop or
            // synthesize predicates.
            let CompiledNode::And { operands: out, .. } = extract_filter_pred(twice).node else {
                proptest::prop_assert!(false, "expected And in output");
                return Ok(());
            };
            let mut sorted_in: Vec<String> = conjuncts.iter().map(|e| format!("{e:?}")).collect();
            let mut sorted_out: Vec<String> = out.iter().map(|e| format!("{e:?}")).collect();
            sorted_in.sort();
            sorted_out.sort();
            proptest::prop_assert_eq!(sorted_in, sorted_out);
        }
    }

    /// Build a representative `CompiledExpr` for each rank shape so the
    /// proptest covers every branch of [`rank`].
    fn build_shape(shape: u8) -> CompiledExpr {
        match shape % 7 {
            0 => eq_col_lit(1, "amount", shape as i64),
            1 => isnull_col(1, "amount"),
            2 => in_set(1, "amount", 4), // small IN
            3 => gt_col_lit(1, "amount", shape as i64),
            4 => in_set(1, "amount", 16), // large IN
            5 => like_call(),
            _ => other_function_call(),
        }
    }

    #[test]
    fn recurses_into_aggregate_and_sort_input_chains() {
        let inner = filter_segment(and_of(vec![
            gt_col_lit(1, "amount", 0),
            eq_col_lit(1, "amount", 1),
        ]));
        let agg_schema = OperatorSchema::new(vec![ColumnDef::required("c", BqlType::Int)]).unwrap();
        let agg = PhysicalPlan::Aggregate(crate::physical::AggregatePhysical {
            aggregates: vec![],
            group_by: vec![],
            max_groups: 1_000_000,
            input: Box::new(inner),
            output_schema: agg_schema,
        });
        let result = order_stateless_filters(agg);
        let PhysicalPlan::Aggregate(agg) = result else {
            panic!("expected Aggregate");
        };
        let pred = extract_filter_pred(*agg.input);
        assert_eq!(ranks_of(&pred), vec![0, 3]);
    }
}
