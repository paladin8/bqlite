//! Column-index remap optimizer pass.
//!
//! Closes the gap left by `compiled.rs` §5.3: a compiled expression's
//! `CompiledNode::Column { index, name }` carries the *full table-schema*
//! ordinal that type-checking resolved against the table's catalog
//! schema, but the runtime batch the operator sees may have fewer
//! columns once [`crate::opt::prune::prune_columns`] has trimmed the
//! scan to its demanded subset.
//!
//! The runtime scan operator emits columns in **table-schema order over
//! the projected subset** (see
//! `bqlite-operators::scan::build_output_schema` and
//! `bqlite-storage::segment::reader::SegmentFileScan::build_scan_plan`).
//! When the pruner drops a column with a *lower* ordinal than a column
//! that downstream operators still reference, every higher-ordinal
//! column shifts down. The compiled-expression indices then no longer
//! match the batch positions, so the runtime evaluator (`eval.rs`)
//! either reads the wrong column or fails with
//! `column \`X\` has index N but batch only has N columns`.
//!
//! # What this pass does
//!
//! Walks the post-prune [`PhysicalPlan`] bottom-up, computing the
//! actual *runtime* `OperatorSchema` that each subtree emits, and
//! rewriting every `CompiledExpr::Column { index, name }` reachable
//! from operators that consume those runtime batches so that
//! `index == schema.column(name).0`.
//!
//! The rewrite touches:
//!
//! - [`ScanPhysical::scan_predicates`] — re-indexed against the scan's
//!   runtime output schema.
//! - [`ScanPhysical::output_schema`] — replaced with the runtime
//!   schema (table-order over `projected_columns`), so the engine's
//!   bind step sees the schema the runtime actually produces.
//! - [`FusedSegmentStep::Filter::predicate`] and
//!   [`FusedSegmentStep::Project`] item expressions — re-indexed
//!   against the schema entering that step.
//! - [`AggregatePhysical::group_by`] and
//!   [`AggregatePhysical::aggregates`] arg expressions.
//! - [`SortPhysical::keys`].
//!
//! Operators whose pruning arm in [`crate::opt::prune`] does not
//! shrink their child schema (`SequenceMatch`, `Distinct`, `Explain`)
//! recurse normally — their expressions already match the
//! still-unpruned child batches. DDL / DML leaves are passthrough.
//!
//! # Invariant after this pass
//!
//! For every operator node in the returned plan:
//!
//! 1. The node's `input.output_schema()` matches the schema the
//!    runtime batch produced by `input` will have.
//! 2. Every `CompiledNode::Column { index, name }` reachable from the
//!    node's own expressions satisfies
//!    `input.output_schema().column(name).unwrap().0 == index`.
//!
//! The engine's bind step and runtime evaluator rely on (2) to read
//! the right column out of every batch.

use bqlite_core::OperatorSchema;

use crate::compiled::{CompiledExpr, CompiledNode};
use crate::physical::{
    AggregatePhysical, CompiledAgg, DistinctPhysical, ExplainPhysical, FusedSegmentPhysical,
    FusedSegmentStep, PhysicalPlan, ProjectPhysicalItem, ScanPhysical, SequenceMatchPhysical,
    SortPhysical,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the column-index remap pass over a [`PhysicalPlan`] tree.
///
/// Must run *after* [`crate::opt::prune::prune_columns`] — the pruner
/// populates the `projected_columns` lists this pass reads to compute
/// runtime schemas.
pub fn remap_column_indices(plan: PhysicalPlan) -> PhysicalPlan {
    let (rewritten, _schema) = remap_node(plan);
    rewritten
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal worker
// ─────────────────────────────────────────────────────────────────────────────

/// Recursive worker. Returns the rewritten plan plus the
/// `OperatorSchema` the rewritten plan emits at runtime — the parent
/// uses that schema to rewrite its own expressions.
fn remap_node(plan: PhysicalPlan) -> (PhysicalPlan, OperatorSchema) {
    match plan {
        // ── Scan: compute runtime schema from projected_columns ─────────────
        PhysicalPlan::Scan(scan) => {
            let runtime_schema = scan_runtime_schema(&scan);
            let ScanPhysical {
                table,
                query_range,
                reader_range,
                scan_predicates,
                projected_columns,
                output_schema: _,
                entity_key_col,
                timestamp_col,
                sample,
            } = scan;
            let scan_predicates = scan_predicates
                .into_iter()
                .map(|p| rewrite_expr(p, &runtime_schema))
                .collect();
            let new_scan = ScanPhysical {
                table,
                query_range,
                reader_range,
                scan_predicates,
                projected_columns,
                output_schema: runtime_schema.clone(),
                entity_key_col,
                timestamp_col,
                sample,
            };
            (PhysicalPlan::Scan(new_scan), runtime_schema)
        }

        // ── FusedSegment: walk steps; rewrite each step's exprs ─────────────
        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical {
                input,
                steps,
                sparsity_factor,
                output_schema: _,
            } = seg;
            let (new_input, mut current_schema) = remap_node(*input);
            let mut new_steps = Vec::with_capacity(steps.len());
            for step in steps {
                match step {
                    FusedSegmentStep::Filter {
                        predicate,
                        tile_size,
                    } => {
                        let new_pred = rewrite_expr(predicate, &current_schema);
                        new_steps.push(FusedSegmentStep::Filter {
                            predicate: new_pred,
                            tile_size,
                        });
                        // Filter never reshapes columns.
                    }
                    FusedSegmentStep::Project(items) => {
                        let new_items: Vec<ProjectPhysicalItem> = items
                            .into_iter()
                            .map(|item| ProjectPhysicalItem {
                                expr: rewrite_expr(item.expr, &current_schema),
                                output_name: item.output_name,
                            })
                            .collect();
                        current_schema = project_output_schema(&new_items);
                        new_steps.push(FusedSegmentStep::Project(new_items));
                    }
                    FusedSegmentStep::Limit(n) => {
                        new_steps.push(FusedSegmentStep::Limit(n));
                        // Limit never reshapes columns.
                    }
                }
            }
            let new_seg = FusedSegmentPhysical {
                input: Box::new(new_input),
                steps: new_steps,
                sparsity_factor,
                output_schema: current_schema.clone(),
            };
            (PhysicalPlan::FusedSegment(new_seg), current_schema)
        }

        // ── Aggregate: rewrite group_by + aggregates against child schema ──
        PhysicalPlan::Aggregate(agg) => {
            let AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input,
                output_schema,
            } = agg;
            let (new_input, input_schema) = remap_node(*input);
            let new_group_by = group_by
                .into_iter()
                .map(|(expr, name)| (rewrite_expr(expr, &input_schema), name))
                .collect();
            let new_aggregates = aggregates
                .into_iter()
                .map(|a| CompiledAgg {
                    function: a.function,
                    arg: a.arg.map(|e| rewrite_expr(e, &input_schema)),
                    output_name: a.output_name,
                })
                .collect();
            let agg_output = output_schema.clone();
            let new_agg = AggregatePhysical {
                aggregates: new_aggregates,
                group_by: new_group_by,
                max_groups,
                input: Box::new(new_input),
                output_schema,
            };
            (PhysicalPlan::Aggregate(new_agg), agg_output)
        }

        // ── Sort: rewrite key exprs against child schema ────────────────────
        PhysicalPlan::Sort(sort) => {
            let SortPhysical {
                keys,
                max_rows,
                input,
                output_schema: _,
            } = sort;
            let (new_input, input_schema) = remap_node(*input);
            let new_keys = keys
                .into_iter()
                .map(|(expr, dir)| (rewrite_expr(expr, &input_schema), dir))
                .collect();
            let new_sort = SortPhysical {
                keys: new_keys,
                max_rows,
                input: Box::new(new_input),
                // Sort output == input schema — keep them in sync after
                // the input's schema may have changed.
                output_schema: input_schema.clone(),
            };
            (PhysicalPlan::Sort(new_sort), input_schema)
        }

        // ── Distinct: no internal exprs; output == input schema ─────────────
        PhysicalPlan::Distinct(distinct) => {
            let DistinctPhysical {
                max_groups,
                input,
                output_schema: _,
            } = distinct;
            let (new_input, input_schema) = remap_node(*input);
            let new_distinct = DistinctPhysical {
                max_groups,
                input: Box::new(new_input),
                output_schema: input_schema.clone(),
            };
            (PhysicalPlan::Distinct(new_distinct), input_schema)
        }

        // ── SequenceMatch: its pruning arm demands all child cols, so
        //    child indices stay full-schema-valid. Recurse, drop the new
        //    schema (SequenceMatch declares its own output), pass through.
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
            let (new_input, _input_schema) = remap_node(*input);
            let out = output_schema.clone();
            let new_seq = SequenceMatchPhysical {
                compiled_nfa,
                strategy,
                match_all,
                demand,
                execution_config,
                fused_aggregate,
                input: Box::new(new_input),
                output_schema,
            };
            (PhysicalPlan::SequenceMatch(Box::new(new_seq)), out)
        }

        // ── Explain: rewrite inner plan ─────────────────────────────────────
        PhysicalPlan::Explain(explain) => {
            let ExplainPhysical {
                plan: inner,
                output_schema,
            } = explain;
            let (new_inner, _inner_schema) = remap_node(*inner);
            let out = output_schema.clone();
            let new_explain = ExplainPhysical {
                plan: Box::new(new_inner),
                output_schema,
            };
            (PhysicalPlan::Explain(new_explain), out)
        }

        // ── Everything else: passthrough. None of these variants is
        //    visited by the pruning pass today, so their internal column
        //    indices remain valid against their full-schema inputs.
        other => {
            let schema = other.output_schema().clone();
            (other, schema)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema construction
// ─────────────────────────────────────────────────────────────────────────────

/// Compute the runtime [`OperatorSchema`] a scan emits, given its
/// `projected_columns` list. Mirrors
/// `bqlite-operators::scan::build_output_schema` (the runtime
/// authority): columns are emitted in **table-schema order**
/// (preserving the order from the scan's declared `output_schema`)
/// filtered to the projected subset. System columns (`__seq_id`,
/// `__batch_id`) appear last in the order they appear in the original
/// schema.
fn scan_runtime_schema(scan: &ScanPhysical) -> OperatorSchema {
    use std::collections::HashSet;
    if scan.projected_columns.is_empty() {
        // Lowering writes an empty list when no pruning is needed and
        // the scan reads every column. In that case the cached
        // `output_schema` is already the runtime schema.
        return scan.output_schema.clone();
    }
    let projected: HashSet<&str> = scan
        .projected_columns
        .iter()
        .map(String::as_str)
        .collect();
    let cols = scan
        .output_schema
        .columns()
        .iter()
        .filter(|c| projected.contains(c.name.as_str()))
        .cloned()
        .collect();
    OperatorSchema::new(cols)
        .expect("scan_runtime_schema: projected column subset is well-formed by construction")
}

/// Compute the schema a [`FusedSegmentStep::Project`] step emits, from
/// its compiled output items. Mirrors `bqlite-engine::bind::project_output_schema`.
fn project_output_schema(items: &[ProjectPhysicalItem]) -> OperatorSchema {
    let cols = items
        .iter()
        .map(|item| {
            if item.expr.nullable {
                bqlite_core::ColumnDef::nullable(&item.output_name, item.expr.result_type.clone())
            } else {
                bqlite_core::ColumnDef::required(&item.output_name, item.expr.result_type.clone())
            }
        })
        .collect();
    OperatorSchema::new(cols).expect("project output schema construction must succeed")
}

// ─────────────────────────────────────────────────────────────────────────────
// Expression rewriter
// ─────────────────────────────────────────────────────────────────────────────

/// Walk `expr` and rewrite every `CompiledNode::Column { index, name }`
/// so that `index == schema.column(name).unwrap().0`. Panics if a
/// referenced column name is absent from `schema` — that's a planner
/// invariant violation (the prune pass should have included every
/// referenced column).
fn rewrite_expr(expr: CompiledExpr, schema: &OperatorSchema) -> CompiledExpr {
    let CompiledExpr {
        node,
        result_type,
        nullable,
    } = expr;
    let new_node = match node {
        CompiledNode::Literal(v) => CompiledNode::Literal(v),
        CompiledNode::Column { index: _, name } => {
            let new_index = schema
                .column(&name)
                .map(|(idx, _)| idx)
                .unwrap_or_else(|| {
                    let have: Vec<&str> =
                        schema.columns().iter().map(|c| c.name.as_str()).collect();
                    panic!(
                        "remap_column_indices: column `{name}` not in input schema; have {have:?}"
                    )
                });
            CompiledNode::Column {
                index: new_index,
                name,
            }
        }
        CompiledNode::Arith {
            op,
            left,
            right,
            kernel,
        } => CompiledNode::Arith {
            op,
            left: Box::new(rewrite_expr(*left, schema)),
            right: Box::new(rewrite_expr(*right, schema)),
            kernel,
        },
        CompiledNode::Compare {
            op,
            left,
            right,
            kernel,
        } => CompiledNode::Compare {
            op,
            left: Box::new(rewrite_expr(*left, schema)),
            right: Box::new(rewrite_expr(*right, schema)),
            kernel,
        },
        CompiledNode::Unary {
            op,
            operand,
            kernel,
        } => CompiledNode::Unary {
            op,
            operand: Box::new(rewrite_expr(*operand, schema)),
            kernel,
        },
        CompiledNode::And { operands, kernel } => CompiledNode::And {
            operands: operands
                .into_iter()
                .map(|e| rewrite_expr(e, schema))
                .collect(),
            kernel,
        },
        CompiledNode::Or { operands, kernel } => CompiledNode::Or {
            operands: operands
                .into_iter()
                .map(|e| rewrite_expr(e, schema))
                .collect(),
            kernel,
        },
        CompiledNode::Not(inner) => CompiledNode::Not(Box::new(rewrite_expr(*inner, schema))),
        CompiledNode::IsNull { input, negated } => CompiledNode::IsNull {
            input: Box::new(rewrite_expr(*input, schema)),
            negated,
        },
        CompiledNode::FunctionCall {
            signature,
            args,
            kernel,
        } => CompiledNode::FunctionCall {
            signature,
            args: args.into_iter().map(|a| rewrite_expr(a, schema)).collect(),
            kernel,
        },
        CompiledNode::Cast {
            input,
            target_type,
            kernel,
        } => CompiledNode::Cast {
            input: Box::new(rewrite_expr(*input, schema)),
            target_type,
            kernel,
        },
        CompiledNode::ImplicitCoerce {
            input,
            target_type,
            kernel,
        } => CompiledNode::ImplicitCoerce {
            input: Box::new(rewrite_expr(*input, schema)),
            target_type,
            kernel,
        },
        CompiledNode::InLiteralSet {
            input,
            values,
            negated,
            kernel,
        } => CompiledNode::InLiteralSet {
            input: Box::new(rewrite_expr(*input, schema)),
            values,
            negated,
            kernel,
        },
    };
    CompiledExpr {
        node: new_node,
        result_type,
        nullable,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema, PropertyValue};

    use crate::compiled::{ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode};
    use crate::opt::prune::prune_columns;
    use crate::physical::{
        AggregatePhysical, CompiledAgg, FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan,
        ProjectPhysicalItem, ScanPhysical, DEFAULT_FILTER_TILE_SIZE, DEFAULT_MAX_GROUPS,
        DEFAULT_SPARSITY_FACTOR,
    };

    use super::*;

    fn ten_col_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
            ColumnDef::nullable("price", BqlType::Float),
            ColumnDef::nullable("category", BqlType::String),
            ColumnDef::nullable("quantity", BqlType::Int),
            ColumnDef::nullable("discount", BqlType::Float),
            ColumnDef::nullable("region", BqlType::String),
            ColumnDef::nullable("flag", BqlType::Bool),
        ])
        .expect("ten_col_schema")
    }

    fn make_scan(projected: Vec<&str>) -> ScanPhysical {
        ScanPhysical {
            table: "purchases".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: projected.into_iter().map(String::from).collect(),
            output_schema: ten_col_schema(),
            entity_key_col: "user_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        }
    }

    fn col(name: &str, idx: usize, ty: BqlType, nullable: bool) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: idx,
                name: name.into(),
            },
            result_type: ty,
            nullable,
        }
    }

    fn pushable_gt(col_name: &str, full_schema_idx: usize, threshold: i64) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Greater,
                left: Box::new(col(col_name, full_schema_idx, BqlType::Int, true)),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Int(threshold)),
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    /// Helper: extract `Column { index, name }` from a CompiledExpr that
    /// is `Compare(Column, Literal)`.
    fn extract_compare_column(expr: &CompiledExpr) -> (usize, &str) {
        match &expr.node {
            CompiledNode::Compare { left, .. } => match &left.node {
                CompiledNode::Column { index, name } => (*index, name.as_str()),
                other => panic!("expected Column in left of Compare, got {other:?}"),
            },
            other => panic!("expected Compare expr, got {other:?}"),
        }
    }

    // ── The headline bug: STATS GROUP BY category over Scan ──────────────────

    #[test]
    fn aggregate_group_by_property_column_gets_remapped_to_pruned_index() {
        // Pre-conditions:
        // - 10-column scan with `category` at full-schema index 5.
        // - Pruner shrinks scan to projected_columns = {user_id, ts, category}.
        // - Runtime scan emits in table-schema order: [user_id (0), ts (1), category (2)].
        // - Aggregate's group_by Column{name: "category"} should be rewritten
        //   from idx=5 → idx=2.
        let scan = make_scan(vec!["user_id", "ts", "category"]);
        let group_by_expr = col("category", 5, BqlType::String, true);

        let count_agg_output = OperatorSchema::new(vec![
            ColumnDef::nullable("category", BqlType::String),
            ColumnDef::required("n", BqlType::Int),
        ])
        .expect("agg out");

        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: bqlite_core::AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![(group_by_expr, "category".into())],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: count_agg_output,
        });

        let remapped = remap_column_indices(plan);

        let PhysicalPlan::Aggregate(agg) = remapped else {
            panic!("expected Aggregate after remap");
        };
        let (idx, name) = match &agg.group_by[0].0.node {
            CompiledNode::Column { index, name } => (*index, name.as_str()),
            other => panic!("expected Column, got {other:?}"),
        };
        assert_eq!(name, "category");
        assert_eq!(
            idx, 2,
            "category must remap from full-schema idx 5 to runtime idx 2 \
             (table-order subset position in [user_id, ts, category])"
        );
    }

    // ── Filter predicate over pruned scan ─────────────────────────────────────

    #[test]
    fn fused_segment_filter_predicate_property_column_gets_remapped() {
        // `purchases | where amount > 0 | select user_id, amount`
        // - amount is at full-schema idx 3.
        // - Pruner: projected_columns = {user_id, ts, amount}.
        // - Runtime emits: [user_id (0), ts (1), amount (2)].
        // - Filter predicate and Project item both reference amount at idx 3.
        let scan = make_scan(vec!["user_id", "ts", "amount"]);

        let project_items = vec![
            ProjectPhysicalItem {
                expr: col("user_id", 0, BqlType::String, false),
                output_name: "user_id".into(),
            },
            ProjectPhysicalItem {
                expr: col("amount", 3, BqlType::Int, true),
                output_name: "amount".into(),
            },
        ];
        let project_schema = OperatorSchema::new(vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
        ])
        .expect("project schema");

        let plan = PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(scan)),
            steps: vec![
                FusedSegmentStep::Filter {
                    predicate: pushable_gt("amount", 3, 0),
                    tile_size: DEFAULT_FILTER_TILE_SIZE,
                },
                FusedSegmentStep::Project(project_items),
            ],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: project_schema,
        });

        let remapped = remap_column_indices(plan);
        let PhysicalPlan::FusedSegment(seg) = remapped else {
            panic!("expected FusedSegment");
        };
        // Filter step: amount index should be 2.
        let FusedSegmentStep::Filter { predicate, .. } = &seg.steps[0] else {
            panic!("expected Filter at step 0");
        };
        let (filter_idx, filter_name) = extract_compare_column(predicate);
        assert_eq!(filter_name, "amount");
        assert_eq!(filter_idx, 2);

        // Project step: user_id index should be 0, amount should be 2.
        let FusedSegmentStep::Project(items) = &seg.steps[1] else {
            panic!("expected Project at step 1");
        };
        let extract_col = |e: &CompiledExpr| match &e.node {
            CompiledNode::Column { index, name } => (*index, name.clone()),
            other => panic!("expected Column, got {other:?}"),
        };
        let (uid_idx, uid_name) = extract_col(&items[0].expr);
        let (amt_idx, amt_name) = extract_col(&items[1].expr);
        assert_eq!(uid_name, "user_id");
        assert_eq!(uid_idx, 0);
        assert_eq!(amt_name, "amount");
        assert_eq!(amt_idx, 2);
    }

    // ── Scan output_schema gets updated to the runtime schema ────────────────

    #[test]
    fn scan_output_schema_is_replaced_with_runtime_schema() {
        let scan = make_scan(vec!["user_id", "ts", "category"]);
        let plan = PhysicalPlan::Scan(scan);
        let remapped = remap_column_indices(plan);
        let PhysicalPlan::Scan(scan) = remapped else {
            panic!("expected Scan");
        };
        let names: Vec<&str> = scan
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        // Table-schema order over the subset: user_id (0), ts (1), category (5).
        // category was originally at idx 5 but compresses to idx 2 in the
        // projected schema. Other intermediate cols (event_type, amount,
        // price) are dropped.
        assert_eq!(names, vec!["user_id", "ts", "category"]);
    }

    // ── End-to-end prune + remap composition matches expected indices ────────

    #[test]
    fn prune_then_remap_composes_correctly_for_aggregate_over_scan() {
        // Build the plan as the lowering pass would emit it: Aggregate's
        // group_by carries the *full-schema* index (5 for category), and
        // the scan has an empty `projected_columns` (the pruning pass
        // hasn't run yet).
        let mut scan = make_scan(vec![]);
        scan.projected_columns.clear();

        let count_agg_output = OperatorSchema::new(vec![
            ColumnDef::nullable("category", BqlType::String),
            ColumnDef::required("n", BqlType::Int),
        ])
        .expect("agg out");

        let plan = PhysicalPlan::Aggregate(AggregatePhysical {
            aggregates: vec![CompiledAgg {
                function: bqlite_core::AggFunction::Count,
                arg: None,
                output_name: "n".into(),
            }],
            group_by: vec![(col("category", 5, BqlType::String, true), "category".into())],
            max_groups: DEFAULT_MAX_GROUPS,
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: count_agg_output,
        });

        // Run the two passes in production order.
        let pruned = prune_columns(plan);
        let remapped = remap_column_indices(pruned);

        let PhysicalPlan::Aggregate(agg) = remapped else {
            panic!("expected Aggregate");
        };
        let PhysicalPlan::Scan(scan) = *agg.input else {
            panic!("expected Scan under Aggregate");
        };
        // Scan's projected_columns must include category (its sole demand),
        // plus the mandatory entity-key + timestamp.
        assert!(scan.projected_columns.contains(&"category".to_string()));
        assert!(scan.projected_columns.contains(&"user_id".to_string()));
        assert!(scan.projected_columns.contains(&"ts".to_string()));

        // The runtime scan output schema must have category at its
        // table-order subset position (2: user_id=0, ts=1, category=2).
        let names: Vec<&str> = scan
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["user_id", "ts", "category"]);

        // And the aggregate's group_by index must match that runtime
        // position.
        let (idx, name) = match &agg.group_by[0].0.node {
            CompiledNode::Column { index, name } => (*index, name.as_str()),
            other => panic!("expected Column, got {other:?}"),
        };
        assert_eq!(name, "category");
        assert_eq!(idx, 2);
    }

    // ── Scan predicate gets remapped ─────────────────────────────────────────

    #[test]
    fn scan_predicate_property_column_gets_remapped() {
        // After predicate pushdown: scan has scan_predicates = [amount > 0]
        // (amount at idx 3) and projected_columns = {user_id, ts, amount}.
        let mut scan = make_scan(vec!["user_id", "ts", "amount"]);
        scan.scan_predicates = vec![pushable_gt("amount", 3, 0)];
        let plan = PhysicalPlan::Scan(scan);

        let remapped = remap_column_indices(plan);
        let PhysicalPlan::Scan(scan) = remapped else {
            panic!("expected Scan");
        };
        let (idx, name) = extract_compare_column(&scan.scan_predicates[0]);
        assert_eq!(name, "amount");
        assert_eq!(idx, 2);
    }

    // ── No pruning → indices unchanged ───────────────────────────────────────

    #[test]
    fn empty_projected_columns_means_runtime_schema_equals_input_schema() {
        // Bare scan with projected_columns = []. The pruner writes this
        // shape only when no columns demand pruning (every declared
        // column is required). The remap pass must preserve the full
        // schema unchanged.
        let scan = make_scan(vec![]);
        let plan = PhysicalPlan::Scan(scan);
        let remapped = remap_column_indices(plan);
        let PhysicalPlan::Scan(scan) = remapped else {
            panic!("expected Scan");
        };
        assert_eq!(scan.output_schema.columns().len(), 10);
        assert_eq!(scan.output_schema.columns()[5].name, "category");
    }
}
