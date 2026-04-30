//! Predicate pushdown optimizer pass (TASK-227, retargeted by TASK-519).
//!
//! Walks the physical plan looking for a [`FusedSegmentPhysical`] whose first
//! [`FusedSegmentStep`] is a `Filter` and whose `input` is a [`ScanPhysical`].
//! For that shape, the pass moves conjuncts that the storage layer can
//! evaluate (per [`CompiledExpr::supported_pushdown_shape`]) into
//! `ScanPhysical::scan_predicates`. Residual conjuncts stay in the same
//! `Filter` step; the step is dropped when no residue remains. When dropping
//! the step empties the segment, the bare scan is returned directly.
//!
//! # Algorithm
//!
//! 1. Walk the tree recursively.
//! 2. When visiting `FusedSegment(steps=[Filter, …], input=Scan)`:
//!    - Decompose the predicate into top-level conjuncts (if the predicate is
//!      a variadic `And`, use its operands; otherwise treat the whole
//!      expression as one conjunct).
//!    - Ask each conjunct `supported_pushdown_shape()`.
//!    - Pushable conjuncts → `scan.scan_predicates`.
//!    - Residual conjuncts → rebuild the `Filter` step at index 0 with the
//!      conjoined residue.
//!    - If the residue is empty → drop the `Filter` step. If the segment now
//!      has zero steps, return the bare `Scan`; otherwise rebuild the segment
//!      without the leading Filter.
//! 3. For any other [`FusedSegmentPhysical`] shape — recurse into `input`
//!    (so a nested `FusedSegment(Filter+Scan)` deeper in the tree is still
//!    rewritten), then reassemble.
//! 4. For every interior non-stateless node — recurse into children and
//!    reassemble.
//! 5. Leaf nodes (`Scan`, DDL, `Insert`) → return unchanged.
//!
//! # Conservatism
//!
//! Only the `FusedSegment(Filter[0]+Scan)` pattern is rewritten. A `Filter`
//! step at index ≥ 1 is **not** pushed, even if the underlying input is a
//! `Scan`: the preceding `Project` step has already rewritten the schema, so
//! the Filter's column references no longer correspond to the Scan's columns
//! and pushing would silently misroute them. This matches the Wave 2
//! conservatism (`engine/operator-fusion.md` §6.4 / `D2` of TASK-519's plan).

use bqlite_core::BqlType;

use crate::compiled::{CompiledExpr, CompiledNode, LogicalKernel};
use crate::physical::{
    ExplainPhysical, FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan, ScanPhysical,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the predicate pushdown pass over a [`PhysicalPlan`] tree.
///
/// Returns a new tree with pushable predicates migrated from a leading
/// `FusedSegmentStep::Filter` into the nearest `ScanPhysical::scan_predicates`
/// whenever the segment's input is a `Scan`. All other nodes are returned
/// structurally unchanged but recursed into so nested patterns are handled.
pub fn pushdown_predicates(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::FusedSegment(seg) => push_fused_segment(seg),

        PhysicalPlan::Explain(explain) => {
            let ExplainPhysical {
                plan: child,
                output_schema,
            } = explain;
            PhysicalPlan::Explain(ExplainPhysical {
                plan: Box::new(pushdown_predicates(*child)),
                output_schema,
            })
        }

        // Wave 3 interior nodes: recurse into children.
        PhysicalPlan::SequenceMatch(seq_match) => {
            let mut sm = *seq_match;
            sm.input = Box::new(pushdown_predicates(*sm.input));
            PhysicalPlan::SequenceMatch(Box::new(sm))
        }

        PhysicalPlan::Aggregate(agg) => {
            let crate::physical::AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input,
                output_schema,
            } = agg;
            PhysicalPlan::Aggregate(crate::physical::AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input: Box::new(pushdown_predicates(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Sort(sort) => {
            let crate::physical::SortPhysical {
                keys,
                max_rows,
                input,
                output_schema,
            } = sort;
            PhysicalPlan::Sort(crate::physical::SortPhysical {
                keys,
                max_rows,
                input: Box::new(pushdown_predicates(*input)),
                output_schema,
            })
        }

        PhysicalPlan::Distinct(distinct) => {
            let crate::physical::DistinctPhysical {
                max_groups,
                input,
                output_schema,
            } = distinct;
            PhysicalPlan::Distinct(crate::physical::DistinctPhysical {
                max_groups,
                input: Box::new(pushdown_predicates(*input)),
                output_schema,
            })
        }

        // Leaf nodes — Scan, DDL variants, Insert: no child plan to recurse.
        other => other,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Handle a [`FusedSegmentPhysical`]: push the leading Filter step into a
/// direct `ScanPhysical` child if eligible, otherwise recurse into the child
/// and reassemble.
fn push_fused_segment(seg: FusedSegmentPhysical) -> PhysicalPlan {
    let FusedSegmentPhysical {
        input,
        steps,
        sparsity_factor,
        output_schema,
    } = seg;

    // Eligible shape: steps[0] is a Filter, *input is a Scan.
    let leading_is_filter = matches!(steps.first(), Some(FusedSegmentStep::Filter { .. }));
    let input_is_scan = matches!(*input, PhysicalPlan::Scan(_));

    if !leading_is_filter || !input_is_scan {
        // Recurse into the child so any nested FusedSegment(Filter+Scan)
        // pattern deeper in the tree is still rewritten, then reassemble.
        return PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(pushdown_predicates(*input)),
            steps,
            sparsity_factor,
            output_schema,
        });
    }

    let scan = match *input {
        PhysicalPlan::Scan(s) => s,
        _ => unreachable!("input_is_scan guarded above"),
    };

    let mut steps_iter = steps.into_iter();
    let leading = steps_iter.next().expect("leading_is_filter guarded above");
    let (predicate, tile_size) = match leading {
        FusedSegmentStep::Filter {
            predicate,
            tile_size,
        } => (predicate, tile_size),
        _ => unreachable!("leading_is_filter guarded above"),
    };
    let remaining_steps: Vec<FusedSegmentStep> = steps_iter.collect();

    let (pushed, residue) = split_conjuncts(predicate);
    let new_scan = ScanPhysical {
        scan_predicates: {
            let mut preds = scan.scan_predicates;
            preds.extend(pushed);
            preds
        },
        ..scan
    };

    if residue.is_empty() {
        // Filter step elided.
        if remaining_steps.is_empty() {
            // Segment is empty — return the bare Scan directly.
            PhysicalPlan::Scan(new_scan)
        } else {
            // Rebuild the segment without the leading Filter step. The
            // segment's `output_schema` does not change (Filter is
            // schema-transparent).
            PhysicalPlan::FusedSegment(FusedSegmentPhysical {
                input: Box::new(PhysicalPlan::Scan(new_scan)),
                steps: remaining_steps,
                sparsity_factor,
                output_schema,
            })
        }
    } else {
        // Keep a narrower Filter step over the rebuilt scan.
        let mut new_steps = Vec::with_capacity(remaining_steps.len() + 1);
        new_steps.push(FusedSegmentStep::Filter {
            predicate: conjoin(residue),
            tile_size,
        });
        new_steps.extend(remaining_steps);
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(new_scan)),
            steps: new_steps,
            sparsity_factor,
            output_schema,
        })
    }
}

/// Decompose a predicate into its top-level AND conjuncts and partition them
/// into (pushable, residual).
///
/// If `predicate` is a variadic `And`, its operands become the conjuncts.
/// Otherwise the entire expression is treated as a single conjunct. In both
/// cases each conjunct is classified individually via
/// [`CompiledExpr::supported_pushdown_shape`].
fn split_conjuncts(predicate: CompiledExpr) -> (Vec<CompiledExpr>, Vec<CompiledExpr>) {
    let conjuncts = match predicate.node {
        CompiledNode::And { operands, .. } => {
            debug_assert!(
                !operands.is_empty(),
                "And with zero operands should not reach pushdown; \
                 the type-checker must not emit empty conjunctions"
            );
            operands
        }
        _ => vec![predicate],
    };

    let mut pushed = Vec::with_capacity(conjuncts.len());
    let mut residue = Vec::new();
    for conjunct in conjuncts {
        if conjunct.supported_pushdown_shape() {
            pushed.push(conjunct);
        } else {
            residue.push(conjunct);
        }
    }
    (pushed, residue)
}

/// Combine a non-empty list of Bool expressions into a single predicate.
///
/// One expression is returned as-is. Multiple expressions are wrapped in a
/// variadic `And` using the `ArrowKleene` kernel. The result is nullable iff
/// any operand is nullable (three-valued Kleene semantics).
fn conjoin(exprs: Vec<CompiledExpr>) -> CompiledExpr {
    debug_assert!(!exprs.is_empty(), "conjoin: called with empty expr list");
    if exprs.len() == 1 {
        exprs.into_iter().next().expect("len checked")
    } else {
        let nullable = exprs.iter().any(|e| e.nullable);
        CompiledExpr {
            node: CompiledNode::And {
                operands: exprs,
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{ColumnDef, OperatorSchema, PropertyValue};

    use crate::compiled::{ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode};
    use crate::physical::{
        ExplainPhysical, FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan, ProjectPhysicalItem,
        ScanPhysical, DEFAULT_FILTER_TILE_SIZE, DEFAULT_SPARSITY_FACTOR,
    };

    use super::*;

    // ── Helpers ─────────────────────────────────────────────────────────────

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

    /// A `Compare(Column, Literal)` — always passes `supported_pushdown_shape()`.
    fn pushable_expr() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Equal,
                left: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 1,
                        name: "amount".into(),
                    },
                    result_type: BqlType::Int,
                    nullable: true,
                }),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Int(42)),
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    /// A `Compare(Column, Column)` — always fails `supported_pushdown_shape()`.
    fn nonpushable_expr() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Equal,
                left: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 0,
                        name: "id".into(),
                    },
                    result_type: BqlType::String,
                    nullable: false,
                }),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 1,
                        name: "amount".into(),
                    },
                    result_type: BqlType::Int,
                    nullable: true,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::EqInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    /// Build a single-step `FusedSegment(Filter)` over `make_scan()` —
    /// the canonical input shape pushdown rewrites.
    fn filter_over_scan(predicate: CompiledExpr) -> PhysicalPlan {
        filter_over_scan_with_tile(predicate, DEFAULT_FILTER_TILE_SIZE)
    }

    fn filter_over_scan_with_tile(predicate: CompiledExpr, tile_size: usize) -> PhysicalPlan {
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            steps: vec![FusedSegmentStep::Filter {
                predicate,
                tile_size,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        })
    }

    /// Helper: extract the leading Filter step from a result plan,
    /// asserting the segment is otherwise well-formed.
    fn expect_leading_filter(plan: PhysicalPlan) -> (CompiledExpr, usize, ScanPhysical) {
        let PhysicalPlan::FusedSegment(seg) = plan else {
            panic!("expected FusedSegment, got {plan:?}");
        };
        assert!(!seg.steps.is_empty(), "expected non-empty steps");
        let step = seg.steps.into_iter().next().unwrap();
        let (predicate, tile_size) = match step {
            FusedSegmentStep::Filter {
                predicate,
                tile_size,
            } => (predicate, tile_size),
            other => panic!("expected Filter step, got {other:?}"),
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
        (predicate, tile_size, scan)
    }

    // ── zero-residue: single pushable predicate → filter elided ─────────────

    #[test]
    fn single_pushable_predicate_is_pushed_and_filter_elided() {
        let pred = pushable_expr();
        let plan = filter_over_scan(pred.clone());

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Scan(scan) = result else {
            panic!("expected Scan (filter elided), got {result:?}");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
        assert_eq!(scan.scan_predicates[0], pred);
    }

    // ── full-residue: single non-pushable predicate → filter unchanged ───────

    #[test]
    fn single_nonpushable_predicate_stays_in_filter_and_scan_is_empty() {
        let pred = nonpushable_expr();
        let plan = filter_over_scan(pred.clone());

        let result = pushdown_predicates(plan);

        let (residual, _, scan) = expect_leading_filter(result);
        assert_eq!(residual, pred, "predicate must be unchanged");
        assert!(
            scan.scan_predicates.is_empty(),
            "scan should have no pushed predicates"
        );
    }

    // ── partial-pushdown: And(pushable, nonpushable) ─────────────────────────

    #[test]
    fn and_with_one_pushable_one_nonpushable_splits_correctly() {
        let p1 = pushable_expr();
        let p2 = nonpushable_expr();
        let and_pred = CompiledExpr {
            node: CompiledNode::And {
                operands: vec![p1.clone(), p2.clone()],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let plan = filter_over_scan(and_pred);

        let result = pushdown_predicates(plan);

        let (residual, _, scan) = expect_leading_filter(result);
        assert_eq!(residual, p2, "residue must be the non-pushable conjunct");
        assert_eq!(scan.scan_predicates.len(), 1);
        assert_eq!(
            scan.scan_predicates[0], p1,
            "pushable conjunct goes to scan"
        );
    }

    // ── all conjuncts of And are pushable → filter elided ───────────────────

    #[test]
    fn and_where_all_conjuncts_are_pushable_elides_filter() {
        let p1 = pushable_expr();
        let p2 = pushable_expr();
        let and_pred = CompiledExpr {
            node: CompiledNode::And {
                operands: vec![p1.clone(), p2.clone()],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let plan = filter_over_scan(and_pred);

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Scan(scan) = result else {
            panic!("expected Scan (filter fully elided), got {result:?}");
        };
        assert_eq!(scan.scan_predicates.len(), 2);
        assert_eq!(scan.scan_predicates[0], p1);
        assert_eq!(scan.scan_predicates[1], p2);
    }

    // ── And where all conjuncts are non-pushable → scan stays empty ──────────

    #[test]
    fn and_where_no_conjuncts_are_pushable_keeps_filter_with_and() {
        let p1 = nonpushable_expr();
        let p2 = nonpushable_expr();
        let and_pred = CompiledExpr {
            node: CompiledNode::And {
                operands: vec![p1.clone(), p2.clone()],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let plan = filter_over_scan(and_pred.clone());

        let result = pushdown_predicates(plan);

        let (residual, _, scan) = expect_leading_filter(result);
        // Residue is reconstructed as an And of both operands.
        match &residual.node {
            CompiledNode::And { operands, .. } => {
                assert_eq!(operands.len(), 2);
                assert_eq!(operands[0], p1);
                assert_eq!(operands[1], p2);
            }
            other => panic!("expected And residue, got {other:?}"),
        }
        assert!(scan.scan_predicates.is_empty());
    }

    // ── bare Scan is unchanged ───────────────────────────────────────────────

    #[test]
    fn bare_scan_is_returned_unchanged() {
        let scan_plan = PhysicalPlan::Scan(make_scan());
        let result = pushdown_predicates(scan_plan);
        let PhysicalPlan::Scan(scan) = result else {
            panic!("expected Scan, got {result:?}");
        };
        assert!(scan.scan_predicates.is_empty());
    }

    // ── Filter at index ≥ 1 is NOT pushed (D2 negative test) ────────────────

    #[test]
    fn filter_after_project_in_segment_is_not_pushed() {
        // FusedSegment(steps=[Project, Filter], input=Scan) — the Project
        // step has rewritten the schema, so the Filter step's column refs
        // no longer correspond to the Scan's columns. Pushing would silently
        // misroute. Per D2 of TASK-519's plan, only steps[0]==Filter is
        // pushable.
        let pred = pushable_expr();
        let project_step = FusedSegmentStep::Project(vec![ProjectPhysicalItem {
            expr: CompiledExpr {
                node: CompiledNode::Column {
                    index: 1,
                    name: "amount".into(),
                },
                result_type: BqlType::Int,
                nullable: true,
            },
            output_name: "amount".to_string(),
        }]);
        let filter_step = FusedSegmentStep::Filter {
            predicate: pred.clone(),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
        };
        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            steps: vec![project_step, filter_step],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment, got {result:?}");
        };
        // Both steps must remain; nothing pushed.
        assert_eq!(seg.steps.len(), 2);
        assert!(matches!(seg.steps[0], FusedSegmentStep::Project(_)));
        let FusedSegmentStep::Filter {
            predicate: residual,
            ..
        } = &seg.steps[1]
        else {
            panic!("expected Filter at index 1");
        };
        assert_eq!(*residual, pred, "Filter at index 1 must not be touched");
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under segment");
        };
        assert!(
            scan.scan_predicates.is_empty(),
            "Scan must not receive pushed predicates from a non-leading Filter"
        );
    }

    // ── leading Filter elided, downstream steps preserved ────────────────────

    #[test]
    fn full_pushdown_preserves_downstream_project_and_limit_steps() {
        // FusedSegment([Filter, Project, Limit] over Scan), Filter pushable.
        // After pushdown the Filter step is dropped but Project and Limit
        // must remain. (Equivalent to legacy `Limit(Project(Scan))` shape.)
        let pred = pushable_expr();
        let project_step = FusedSegmentStep::Project(vec![ProjectPhysicalItem {
            expr: CompiledExpr {
                node: CompiledNode::Column {
                    index: 1,
                    name: "amount".into(),
                },
                result_type: BqlType::Int,
                nullable: true,
            },
            output_name: "amount".to_string(),
        }]);
        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            steps: vec![
                FusedSegmentStep::Filter {
                    predicate: pred.clone(),
                    tile_size: DEFAULT_FILTER_TILE_SIZE,
                },
                project_step,
                FusedSegmentStep::Limit(10),
            ],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment after partial elision, got {result:?}");
        };
        assert_eq!(seg.steps.len(), 2);
        assert!(matches!(seg.steps[0], FusedSegmentStep::Project(_)));
        assert!(matches!(seg.steps[1], FusedSegmentStep::Limit(10)));
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under segment");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
        assert_eq!(scan.scan_predicates[0], pred);
    }

    // ── tile_size is preserved on residual filter ────────────────────────────

    #[test]
    fn residual_filter_preserves_tile_size() {
        let pred = nonpushable_expr();
        let custom_tile = 3_500;
        let plan = filter_over_scan_with_tile(pred.clone(), custom_tile);

        let result = pushdown_predicates(plan);

        let (_, tile_size, _) = expect_leading_filter(result);
        assert_eq!(tile_size, custom_tile);
    }

    // ── existing scan predicates are preserved ───────────────────────────────

    #[test]
    fn existing_scan_predicates_are_preserved_alongside_pushed_ones() {
        let existing_pred = pushable_expr();
        let new_pred = pushable_expr();

        // Build a scan that already has one predicate.
        let scan_with_existing = ScanPhysical {
            scan_predicates: vec![existing_pred.clone()],
            ..make_scan()
        };
        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(scan_with_existing)),
            steps: vec![FusedSegmentStep::Filter {
                predicate: new_pred.clone(),
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: empty_schema(),
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Scan(scan) = result else {
            panic!("expected Scan, got {result:?}");
        };
        assert_eq!(scan.scan_predicates.len(), 2);
        assert_eq!(scan.scan_predicates[0], existing_pred);
        assert_eq!(scan.scan_predicates[1], new_pred);
    }

    // ── Explain wraps pushed result ──────────────────────────────────────────

    #[test]
    fn explain_wrapping_filter_over_scan_pushes_into_inner_scan() {
        let pred = pushable_expr();
        let single_col_schema =
            OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)]).unwrap();
        let inner = filter_over_scan(pred.clone());
        let plan = PhysicalPlan::Explain(ExplainPhysical {
            plan: Box::new(inner),
            output_schema: single_col_schema,
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Explain(explain) = result else {
            panic!("expected Explain, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *explain.plan else {
            panic!("expected Scan inside Explain (filter elided)");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
        assert_eq!(scan.scan_predicates[0], pred);
    }
}
