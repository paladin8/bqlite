//! Predicate pushdown optimizer pass (TASK-227).
//!
//! Walks the physical plan looking for a [`FilterPhysical`] directly above a
//! [`ScanPhysical`] and moves conjuncts that the storage layer can evaluate
//! (per [`CompiledExpr::supported_pushdown_shape`]) into
//! `ScanPhysical::scan_predicates`. The residual conjuncts stay in
//! `FilterPhysical`; the filter is elided when no residue remains.
//!
//! # Algorithm
//!
//! 1. Walk the tree recursively (depth-first, post-order for inner nodes).
//! 2. When visiting a `Filter(Scan(…))` pattern:
//!    - Decompose the predicate into top-level conjuncts (if the predicate is
//!      a variadic `And`, use its operands; otherwise treat the whole
//!      expression as one conjunct).
//!    - Ask each conjunct `supported_pushdown_shape()`.
//!    - Pushable conjuncts → `scan.scan_predicates`.
//!    - Residual conjuncts → reconstruct `FilterPhysical` (single expression
//!      or a new `And` from the residue list).
//!    - If residue is empty → return the `Scan` directly (filter elided).
//! 3. For `Filter` over any other child → recurse into the child, then
//!    reassemble the filter unchanged.
//! 4. For every other interior node (`Project`, `Limit`, `Explain`) → recurse
//!    into child inputs and reassemble.
//! 5. Leaf nodes (`Scan`, DDL, `Insert`) → return unchanged.
//!
//! # Conservatism
//!
//! Only the direct `Filter(Scan)` pattern is rewritten. Filters above
//! `Project(Scan)`, `Limit(Scan)`, or deeper trees are recursed into but not
//! moved. Wave 2 scope per TASK-227.

use bqlite_core::BqlType;

use crate::compiled::{CompiledExpr, CompiledNode, LogicalKernel};
use crate::physical::{
    ExplainPhysical, FilterPhysical, LimitPhysical, PhysicalPlan, ProjectPhysical, ScanPhysical,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the predicate pushdown pass over a [`PhysicalPlan`] tree.
///
/// Returns a new tree with pushable predicates migrated from
/// `FilterPhysical.predicate` into the nearest `ScanPhysical.scan_predicates`
/// when a `Filter(Scan)` pattern is detected. All other nodes are returned
/// structurally unchanged (but recursed into so nested patterns are handled).
pub fn pushdown_predicates(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter(filter) => push_filter(filter),

        PhysicalPlan::Project(proj) => {
            let ProjectPhysical {
                expressions,
                input,
                output_schema,
            } = proj;
            PhysicalPlan::Project(ProjectPhysical {
                expressions,
                input: Box::new(pushdown_predicates(*input)),
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
                input: Box::new(pushdown_predicates(*input)),
                output_schema,
            })
        }

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

/// Handle a `FilterPhysical` node: push into a direct `ScanPhysical` child if
/// present, otherwise recurse into the child and reassemble.
fn push_filter(filter: FilterPhysical) -> PhysicalPlan {
    let FilterPhysical {
        predicate,
        input,
        tile_size,
        output_schema,
    } = filter;

    match *input {
        PhysicalPlan::Scan(scan) => {
            // Direct Filter-over-Scan: attempt pushdown.
            let (pushed, residue) = split_conjuncts(predicate);

            // Merge pushed conjuncts into an updated scan (preserving any
            // predicates already present from a prior pass or logical phase).
            let new_scan = ScanPhysical {
                scan_predicates: {
                    let mut preds = scan.scan_predicates;
                    preds.extend(pushed);
                    preds
                },
                ..scan
            };

            if residue.is_empty() {
                // All conjuncts pushed → elide the filter entirely.
                PhysicalPlan::Scan(new_scan)
            } else {
                // Some conjuncts remain → rebuild a narrower filter.
                PhysicalPlan::Filter(FilterPhysical {
                    predicate: conjoin(residue),
                    input: Box::new(PhysicalPlan::Scan(new_scan)),
                    tile_size,
                    output_schema,
                })
            }
        }

        other_input => {
            // Filter is not directly above a Scan: recurse into the child
            // so any nested Filter(Scan) patterns are handled, then
            // reassemble with the original predicate unchanged.
            let optimized_input = pushdown_predicates(other_input);
            PhysicalPlan::Filter(FilterPhysical {
                predicate,
                input: Box::new(optimized_input),
                tile_size,
                output_schema,
            })
        }
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
        ExplainPhysical, FilterPhysical, LimitPhysical, PhysicalPlan, ProjectPhysical,
        ScanPhysical, DEFAULT_FILTER_TILE_SIZE,
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

    /// Wrap `predicate` in a `FilterPhysical` over `make_scan()`.
    fn filter_over_scan(predicate: CompiledExpr) -> PhysicalPlan {
        PhysicalPlan::Filter(FilterPhysical {
            predicate,
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: empty_schema(),
        })
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

        let PhysicalPlan::Filter(filter) = result else {
            panic!("expected Filter, got {result:?}");
        };
        assert_eq!(filter.predicate, pred, "predicate must be unchanged");
        let PhysicalPlan::Scan(scan) = *filter.input else {
            panic!("expected Scan as filter input");
        };
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

        let PhysicalPlan::Filter(filter) = result else {
            panic!("expected Filter (partial residue), got {result:?}");
        };
        // Residue is the single non-pushable conjunct (unwrapped from And).
        assert_eq!(
            filter.predicate, p2,
            "residue must be the non-pushable conjunct"
        );
        let PhysicalPlan::Scan(scan) = *filter.input else {
            panic!("expected Scan as filter input");
        };
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

        let PhysicalPlan::Filter(filter) = result else {
            panic!("expected Filter, got {result:?}");
        };
        // Residue is reconstructed as an And of both operands.
        match &filter.predicate.node {
            CompiledNode::And { operands, .. } => {
                assert_eq!(operands.len(), 2);
                assert_eq!(operands[0], p1);
                assert_eq!(operands[1], p2);
            }
            other => panic!("expected And residue, got {other:?}"),
        }
        let PhysicalPlan::Scan(scan) = *filter.input else {
            panic!("expected Scan under filter");
        };
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

    // ── Filter not directly over Scan: recurse into child ───────────────────

    #[test]
    fn filter_over_project_over_scan_is_not_pushed_but_project_child_is_walked() {
        // Filter(Project(Scan)) — the filter is not directly over a scan, so
        // the predicate should stay in the filter unchanged. However, the pass
        // must recurse through `Project` so that nested `Filter(Scan)` patterns
        // inside the project's child would be handled (none here, but the walk
        // itself should not error).
        let pred = pushable_expr();
        let scan_schema = empty_schema();
        let project_plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![],
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            output_schema: scan_schema.clone(),
        });
        let plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: pred.clone(),
            input: Box::new(project_plan),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: scan_schema,
        });

        let result = pushdown_predicates(plan);

        // Filter must remain (not pushed across Project).
        let PhysicalPlan::Filter(filter) = result else {
            panic!("expected Filter, got {result:?}");
        };
        assert_eq!(filter.predicate, pred, "predicate must be unchanged");
        assert!(matches!(*filter.input, PhysicalPlan::Project(_)));
    }

    // ── Project(Filter(Scan)) — project wraps a pushed plan ─────────────────

    #[test]
    fn project_over_filter_over_scan_pushes_into_inner_scan() {
        let pred = pushable_expr();
        let schema = empty_schema();
        let filter_plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: pred.clone(),
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema.clone(),
        });
        let plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![],
            input: Box::new(filter_plan),
            output_schema: schema,
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Project(proj) = result else {
            panic!("expected Project, got {result:?}");
        };
        // Inner filter must have been elided; Project's input is now a Scan.
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!(
                "expected Scan under Project (filter elided), got {:?}",
                proj.input
            );
        };
        assert_eq!(scan.scan_predicates.len(), 1);
        assert_eq!(scan.scan_predicates[0], pred);
    }

    // ── Limit(Filter(Scan)) ──────────────────────────────────────────────────

    #[test]
    fn limit_over_filter_over_scan_pushes_into_inner_scan() {
        let pred = pushable_expr();
        let schema = empty_schema();
        let filter_plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: pred.clone(),
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema.clone(),
        });
        let plan = PhysicalPlan::Limit(LimitPhysical {
            count: 10,
            input: Box::new(filter_plan),
            output_schema: schema,
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Limit(limit) = result else {
            panic!("expected Limit, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *limit.input else {
            panic!("expected Scan under Limit (filter elided)");
        };
        assert_eq!(scan.scan_predicates.len(), 1);
        assert_eq!(scan.scan_predicates[0], pred);
    }

    // ── tile_size is preserved on residual filter ────────────────────────────

    #[test]
    fn residual_filter_preserves_tile_size() {
        let pred = nonpushable_expr();
        let custom_tile = 3_500;
        let plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: pred.clone(),
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            tile_size: custom_tile,
            output_schema: empty_schema(),
        });

        let result = pushdown_predicates(plan);

        let PhysicalPlan::Filter(filter) = result else {
            panic!("expected Filter, got {result:?}");
        };
        assert_eq!(filter.tile_size, custom_tile);
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
        let schema = empty_schema();
        let plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: new_pred.clone(),
            input: Box::new(PhysicalPlan::Scan(scan_with_existing)),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema,
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
        let schema = empty_schema();
        let single_col_schema =
            OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)]).unwrap();
        let filter_plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: pred.clone(),
            input: Box::new(PhysicalPlan::Scan(make_scan())),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema,
        });
        let plan = PhysicalPlan::Explain(ExplainPhysical {
            plan: Box::new(filter_plan),
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
