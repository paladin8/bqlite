//! Projection pruning optimizer pass (TASK-228).
//!
//! Implements a backward demand-set collection pass per
//! `docs/design/planner-pipeline.md` §6.6 (Pass 4). Walks the physical plan
//! from the output root toward the scan, accumulating the set of column names
//! each operator actually needs, then writes the final accumulated set into
//! `ScanPhysical::projected_columns` so the scan decodes only the demanded
//! columns from disk.
//!
//! # Algorithm
//!
//! 1. Begin at the root with its output schema column names as the initial
//!    demand.
//! 2. Walk toward the `Scan` (depth-first, top-down demand propagation):
//!    - **`FilterPhysical`**: output schema = input schema, so pass the
//!      downstream demand unchanged. Additionally, the filter's predicate
//!      references columns — collect those and add them to the demand so the
//!      scan decodes them even if nothing else needs them.
//!    - **`ProjectPhysical`**: the project evaluates all its output expressions
//!      (there is no partial-expression evaluation in Wave 2). The demand
//!      to its child is therefore the union of every column name referenced
//!      by every expression in `proj.expressions`, regardless of which output
//!      columns the downstream demanded.
//!    - **`LimitPhysical`**: passes demand through unchanged (limit is a row
//!      cap, not a column restriction).
//!    - **`ExplainPhysical`**: recurses into the inner plan with all of the
//!      inner plan's output columns as demand (the `Explain` wrapper does not
//!      restrict columns).
//! 3. At `ScanPhysical`: the accumulated demand (plus any column names
//!    referenced by `scan.scan_predicates` already pushed into the scan by
//!    TASK-227) becomes `projected_columns`, written as a sorted
//!    `Vec<String>`.
//! 4. DDL / DML leaf nodes are returned unchanged.
//!
//! # Why scan predicates need special handling
//!
//! When TASK-227's predicate pushdown pass moves conjuncts into
//! `ScanPhysical::scan_predicates`, those predicates reference columns that
//! the scan must decode even if no downstream operator requests them. For
//! example, `Project(entity_id, ts)(Scan[amount > 0])` requires `amount` in
//! the scan even though the Project only outputs `entity_id` and `ts`.
//! The pruning pass handles this by collecting column names from
//! `scan.scan_predicates` at the `Scan` arm before writing
//! `projected_columns`.

use std::collections::HashSet;

use bqlite_core::OperatorSchema;

use crate::compiled::{CompiledExpr, CompiledNode};
use crate::physical::{
    AggregatePhysical, DistinctPhysical, ExplainPhysical, FilterPhysical, LimitPhysical,
    PhysicalPlan, ProjectPhysical, ScanPhysical, SequenceMatchPhysical, SortPhysical,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Run the projection pruning pass over a [`PhysicalPlan`] tree.
///
/// Returns a new tree where every `ScanPhysical::projected_columns` is set to
/// the minimal sorted list of column names demanded by the operators above it.
/// All other node fields are structurally unchanged.
pub fn prune_columns(plan: PhysicalPlan) -> PhysicalPlan {
    // Seed the backward pass with all output columns of the root node.
    let initial_demand: HashSet<String> = plan
        .output_schema()
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    prune_with_demand(plan, initial_demand)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Recursive worker: propagate `demand` downward through `plan`, rewriting
/// `ScanPhysical::projected_columns` when the scan is reached.
fn prune_with_demand(plan: PhysicalPlan, demand: HashSet<String>) -> PhysicalPlan {
    match plan {
        // ── Scan: demand → projected_columns ─────────────────────────────────
        PhysicalPlan::Scan(scan) => {
            let ScanPhysical {
                table,
                time_range,
                scan_predicates,
                projected_columns: _,
                output_schema,
                entity_key_col,
                timestamp_col,
            } = scan;

            // Include columns referenced by scan-level pushed predicates
            // (populated by TASK-227) — the scan must decode them even when
            // no downstream operator references them by name.
            let mut all_demand = demand;
            for pred in &scan_predicates {
                collect_column_names(pred, &mut all_demand);
            }

            // Always include entity-key and timestamp — required by the k-way
            // merge scan regardless of downstream demand.
            all_demand.insert(entity_key_col.clone());
            all_demand.insert(timestamp_col.clone());

            // System columns (names starting with `__`, e.g. `__seq_id`,
            // `__batch_id`) appear in the planner-level `OperatorSchema`
            // produced by `OperatorSchema::from_table`, but they are NOT part
            // of the physical table schema on disk. The scan operator validates
            // that every name in `projected_columns` maps to a real table
            // column, so system column names must be excluded here.
            //
            // Sort for deterministic plan shape (regression-friendly).
            let mut cols: Vec<String> = all_demand
                .into_iter()
                .filter(|c| !c.starts_with("__"))
                .collect();
            cols.sort_unstable();

            PhysicalPlan::Scan(ScanPhysical {
                table,
                time_range,
                scan_predicates,
                projected_columns: cols,
                output_schema,
                entity_key_col,
                timestamp_col,
            })
        }

        // ── Filter: pass demand through + add predicate column refs ──────────
        PhysicalPlan::Filter(filter) => {
            let FilterPhysical {
                predicate,
                input,
                tile_size,
                output_schema,
            } = filter;

            let mut child_demand = demand;
            collect_column_names(&predicate, &mut child_demand);

            PhysicalPlan::Filter(FilterPhysical {
                predicate,
                input: Box::new(prune_with_demand(*input, child_demand)),
                tile_size,
                output_schema,
            })
        }

        // ── Project: child demand = columns referenced by all expressions ────
        PhysicalPlan::Project(proj) => {
            let ProjectPhysical {
                expressions,
                input,
                output_schema,
            } = proj;

            // Wave 2 ProjectPhysical evaluates every output expression, so
            // the child always needs all columns referenced across all
            // expressions — independent of which output columns downstream
            // actually requested.
            let mut child_demand = HashSet::new();
            for item in &expressions {
                collect_column_names(&item.expr, &mut child_demand);
            }

            PhysicalPlan::Project(ProjectPhysical {
                expressions,
                input: Box::new(prune_with_demand(*input, child_demand)),
                output_schema,
            })
        }

        // ── Limit: transparent column-wise, pass demand through ───────────────
        PhysicalPlan::Limit(limit) => {
            let LimitPhysical {
                count,
                input,
                output_schema,
            } = limit;
            PhysicalPlan::Limit(LimitPhysical {
                count,
                input: Box::new(prune_with_demand(*input, demand)),
                output_schema,
            })
        }

        // ── Explain: recurse into inner plan with all its output columns ──────
        PhysicalPlan::Explain(explain) => {
            let ExplainPhysical {
                plan: inner,
                output_schema,
            } = explain;
            // The Explain wrapper does not restrict the inner plan's columns —
            // the inner plan is compiled in full so EXPLAIN can display it.
            let inner_demand: HashSet<String> = inner
                .output_schema()
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            PhysicalPlan::Explain(ExplainPhysical {
                plan: Box::new(prune_with_demand(*inner, inner_demand)),
                output_schema,
            })
        }

        // ── Wave 3: SequenceMatch ──────────────────────────────────────────────
        // SequenceMatch produces its own output columns (entity_id, step_reached,
        // step properties, etc.) — it does NOT pass input columns through. The
        // scan-side demand comes from the pattern's predicates, variable bindings,
        // and step-property forwarding, which are already computed by the physical
        // lowering. We recurse into the child with the demand the SequenceMatch
        // already determined (all input schema columns the pattern needs).
        PhysicalPlan::SequenceMatch(seq_match) => {
            let SequenceMatchPhysical {
                compiled_nfa,
                strategy: _,
                match_all,
                demand: seq_demand,
                execution_config: _,
                fused_aggregate,
                input,
                output_schema,
            } = *seq_match;
            // The SequenceMatch's child demand is all its input schema columns.
            let child_demand: HashSet<String> = input
                .output_schema()
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();

            // Prune the SequenceMatch output schema: remove columns not
            // in the parent demand set. This ensures that `match_duration`
            // and `match_events` are dropped when the downstream (e.g.
            // STATS referencing only `step_reached`) does not need them.
            // `entity_id` is always kept — the SequenceMatchAdapter needs
            // it for entity boundary detection and output batch construction.
            let pruned_output = if !demand.is_empty() {
                let pruned_cols: Vec<_> = output_schema
                    .columns()
                    .iter()
                    .filter(|c| demand.contains(&c.name) || c.name == "entity_id")
                    .cloned()
                    .collect();
                if pruned_cols.len() < output_schema.columns().len() && !pruned_cols.is_empty() {
                    OperatorSchema::new(pruned_cols).unwrap_or_else(|_| output_schema.clone())
                } else {
                    output_schema
                }
            } else {
                output_schema
            };

            // Recompute strategy after pruning: if match_duration and
            // match_events are no longer in the output schema, the NFA
            // strategy is no longer required — step counter suffices.
            let pruned_needs_match_detail = pruned_output.column("match_duration").is_some()
                || pruned_output.column("match_events").is_some();
            let pruned_execution_config = crate::compile::MatchExecutionConfig {
                track_match_duration: pruned_needs_match_detail,
                track_match_events: pruned_needs_match_detail,
            };
            let pruned_strategy = crate::compile::select_strategy(
                compiled_nfa.pattern_class,
                &pruned_execution_config,
            );

            PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
                compiled_nfa,
                strategy: pruned_strategy,
                match_all,
                demand: seq_demand,
                execution_config: pruned_execution_config,
                fused_aggregate,
                input: Box::new(prune_with_demand(*input, child_demand)),
                output_schema: pruned_output,
            }))
        }

        // ── Wave 3: Aggregate ───────────────────────────────────────────────────
        // Aggregate is a demand boundary: the child demand is the set of columns
        // referenced by group-by expressions and aggregate arguments.
        PhysicalPlan::Aggregate(agg) => {
            let AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input,
                output_schema,
            } = agg;
            let mut child_demand = HashSet::new();
            for (expr, _name) in &group_by {
                collect_column_names(expr, &mut child_demand);
            }
            for compiled_agg in &aggregates {
                if let Some(arg) = &compiled_agg.arg {
                    collect_column_names(arg, &mut child_demand);
                }
            }
            PhysicalPlan::Aggregate(AggregatePhysical {
                aggregates,
                group_by,
                max_groups,
                input: Box::new(prune_with_demand(*input, child_demand)),
                output_schema,
            })
        }

        // ── Wave 3: Sort ─────────────────────────────────────────────────────────
        // Sort is transparent column-wise (output = input schema), but sort keys
        // reference columns that must be demanded from the child.
        PhysicalPlan::Sort(sort) => {
            let SortPhysical {
                keys,
                max_rows,
                input,
                output_schema,
            } = sort;
            let mut child_demand = demand;
            for (expr, _dir) in &keys {
                collect_column_names(expr, &mut child_demand);
            }
            PhysicalPlan::Sort(SortPhysical {
                keys,
                max_rows,
                input: Box::new(prune_with_demand(*input, child_demand)),
                output_schema,
            })
        }

        // ── Wave 3: Distinct ─────────────────────────────────────────────────────
        // Distinct deduplicates on all output columns, so it demands all of its
        // own output schema columns from the child.
        PhysicalPlan::Distinct(distinct) => {
            let DistinctPhysical {
                max_groups,
                input,
                output_schema,
            } = distinct;
            let child_demand: HashSet<String> = output_schema
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            PhysicalPlan::Distinct(DistinctPhysical {
                max_groups,
                input: Box::new(prune_with_demand(*input, child_demand)),
                output_schema,
            })
        }

        // ── DDL / DML leaf nodes: no columns to prune ─────────────────────────
        other => other,
    }
}

/// Collect every `Column { name }` reference reachable from `expr` into `names`.
///
/// Walks the full expression tree recursively. Exhaustively matches every
/// [`CompiledNode`] variant so that adding a new node variant in the same crate
/// surfaces a compile error here rather than silently omitting its column refs.
fn collect_column_names(expr: &CompiledExpr, names: &mut HashSet<String>) {
    match &expr.node {
        CompiledNode::Literal(_) => {}
        CompiledNode::Column { name, .. } => {
            names.insert(name.clone());
        }
        CompiledNode::Arith { left, right, .. } | CompiledNode::Compare { left, right, .. } => {
            collect_column_names(left, names);
            collect_column_names(right, names);
        }
        CompiledNode::Unary { operand, .. } => {
            collect_column_names(operand, names);
        }
        CompiledNode::And { operands, .. } | CompiledNode::Or { operands, .. } => {
            for op in operands {
                collect_column_names(op, names);
            }
        }
        CompiledNode::Not(inner) => {
            collect_column_names(inner, names);
        }
        CompiledNode::IsNull { input, .. } => {
            collect_column_names(input, names);
        }
        CompiledNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_names(arg, names);
            }
        }
        CompiledNode::Cast { input, .. } | CompiledNode::ImplicitCoerce { input, .. } => {
            collect_column_names(input, names);
        }
        CompiledNode::InLiteralSet { input, .. } => {
            collect_column_names(input, names);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bqlite_ast::expr::CompareOp;
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema, PropertyValue};

    use crate::compiled::{
        ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode, LogicalKernel,
    };
    use crate::physical::{
        FilterPhysical, LimitPhysical, PhysicalPlan, ProjectPhysical, ProjectPhysicalItem,
        ScanPhysical, DEFAULT_FILTER_TILE_SIZE,
    };

    use super::*;

    // ── Test helpers ─────────────────────────────────────────────────────────

    /// Build a 10-column `OperatorSchema` for pruning tests.
    fn ten_col_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("col1", BqlType::Int),
            ColumnDef::nullable("col2", BqlType::Int),
            ColumnDef::nullable("col3", BqlType::Int),
            ColumnDef::nullable("col4", BqlType::Int),
            ColumnDef::nullable("col5", BqlType::Int),
            ColumnDef::nullable("col6", BqlType::Int),
            ColumnDef::nullable("col7", BqlType::Int),
        ])
        .expect("ten_col_schema")
    }

    fn two_col_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
        ])
        .expect("two_col_schema")
    }

    fn make_scan_with_schema(schema: OperatorSchema) -> ScanPhysical {
        ScanPhysical {
            table: "events".into(),
            time_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: schema,
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
        }
    }

    /// A `CompiledExpr` that is a simple column reference.
    fn col_expr(name: &str, idx: usize, ty: BqlType, nullable: bool) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: idx,
                name: name.into(),
            },
            result_type: ty,
            nullable,
        }
    }

    /// A `Compare(Column, Literal)` predicate — pushable shape.
    fn pushable_pred(col_name: &str, col_idx: usize) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Greater,
                left: Box::new(col_expr(col_name, col_idx, BqlType::Int, true)),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Int(0)),
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    // ── Core requirement: 2-of-10 column projection ───────────────────────────

    #[test]
    fn project_two_columns_from_ten_column_scan_prunes_to_exactly_two() {
        // The primary acceptance criterion from the task description:
        // "a query selecting 2 columns from a 10-column table results in a
        // scan reading exactly 2 columns."
        let scan = make_scan_with_schema(ten_col_schema());
        let two_out = two_col_schema();

        let plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: two_out,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Project(proj) = result else {
            panic!("expected Project, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!("expected Scan under Project");
        };
        assert_eq!(
            scan.projected_columns,
            vec!["entity_id", "ts"],
            "scan must read exactly the 2 projected columns"
        );
    }

    // ── Bare Scan: projected_columns = all schema columns ────────────────────

    #[test]
    fn bare_scan_gets_all_columns_in_projected_columns() {
        let schema = ten_col_schema();
        let col_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
        let scan = make_scan_with_schema(schema);
        let plan = PhysicalPlan::Scan(scan);

        let result = prune_columns(plan);

        let PhysicalPlan::Scan(scan) = result else {
            panic!("expected Scan, got {result:?}");
        };
        let mut expected = col_names;
        expected.sort_unstable();
        assert_eq!(
            scan.projected_columns, expected,
            "bare scan must list all schema columns explicitly"
        );
    }

    // ── Filter(Scan): filter predicate columns included in projected_columns ─

    #[test]
    fn filter_over_scan_includes_predicate_columns_in_projected() {
        // Filter with `col1 > 0` over a scan that has a full schema.
        // Since Filter output = input schema, all schema columns + predicate
        // columns end up in projected_columns. The predicate references col1
        // which is already in the full schema, so projected_columns = all cols.
        let schema = ten_col_schema();
        let all_col_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();

        let scan = make_scan_with_schema(schema.clone());
        let plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: pushable_pred("col1", 3),
            input: Box::new(PhysicalPlan::Scan(scan)),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Filter(filter) = result else {
            panic!("expected Filter, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *filter.input else {
            panic!("expected Scan under Filter");
        };
        let mut expected = all_col_names;
        expected.sort_unstable();
        assert_eq!(
            scan.projected_columns, expected,
            "filter-only query must include all columns (no projection pruning)"
        );
    }

    // ── Limit(Project(Scan)): limit does not block pruning ────────────────────

    #[test]
    fn limit_over_project_passes_column_demand_to_scan() {
        let scan = make_scan_with_schema(ten_col_schema());
        let two_out = two_col_schema();

        let project = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: two_out.clone(),
        });
        let plan = PhysicalPlan::Limit(LimitPhysical {
            count: 10,
            input: Box::new(project),
            output_schema: two_out,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Limit(limit) = result else {
            panic!("expected Limit, got {result:?}");
        };
        let PhysicalPlan::Project(proj) = *limit.input else {
            panic!("expected Project under Limit");
        };
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!("expected Scan under Project");
        };
        assert_eq!(
            scan.projected_columns,
            vec!["entity_id", "ts"],
            "limit must not widen scan beyond what project needs"
        );
    }

    // ── Scan with pushed predicates: extra columns included ───────────────────

    #[test]
    fn scan_with_pushed_predicate_includes_predicate_column() {
        // Simulate the post-pushdown state: Scan has `col3 > 0` pushed in,
        // but the downstream Project only selects entity_id and ts.
        // The scan must also decode col3 for the predicate evaluation.
        let scan_with_pred = ScanPhysical {
            table: "events".into(),
            time_range: None,
            scan_predicates: vec![pushable_pred("col3", 5)],
            projected_columns: vec![],
            output_schema: ten_col_schema(),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
        };
        let two_out = two_col_schema();

        let plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            input: Box::new(PhysicalPlan::Scan(scan_with_pred)),
            output_schema: two_out,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Project(proj) = result else {
            panic!("expected Project, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!("expected Scan under Project");
        };
        assert_eq!(
            scan.projected_columns,
            vec!["col3", "entity_id", "ts"],
            "scan must include predicate column alongside projected columns"
        );
    }

    // ── Projection with expression referencing multiple input columns ─────────

    #[test]
    fn project_expression_with_two_input_columns_demands_both() {
        // `output = col1 + col2` — the expression references two scan columns.
        let sum_expr = CompiledExpr {
            node: CompiledNode::Arith {
                op: bqlite_ast::expr::BinaryOp::Add,
                left: Box::new(col_expr("col1", 3, BqlType::Int, true)),
                right: Box::new(col_expr("col2", 4, BqlType::Int, true)),
                kernel: crate::compiled::ArithKernel::ArrowKernel(ArrowKernelId::AddInt),
            },
            result_type: BqlType::Int,
            nullable: true,
        };
        let out_schema = OperatorSchema::new(vec![ColumnDef::nullable("sum", BqlType::Int)])
            .expect("output schema");

        let scan = make_scan_with_schema(ten_col_schema());
        let plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![ProjectPhysicalItem {
                expr: sum_expr,
                output_name: "sum".into(),
            }],
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: out_schema,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Project(proj) = result else {
            panic!("expected Project");
        };
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!("expected Scan under Project");
        };
        // entity_id and ts are always included by the pruning pass
        // (required for k-way merge in the scan operator).
        assert_eq!(
            scan.projected_columns,
            vec!["col1", "col2", "entity_id", "ts"],
            "expression referencing two input columns demands both from scan (plus mandatory merge columns)"
        );
    }

    // ── And predicate in filter adds both column refs ─────────────────────────

    #[test]
    fn filter_with_and_predicate_demands_all_referenced_columns() {
        // Filter: `col1 > 0 AND col2 > 0` but only entity_id/ts are in project.
        let and_pred = CompiledExpr {
            node: CompiledNode::And {
                operands: vec![pushable_pred("col1", 3), pushable_pred("col2", 4)],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let schema = ten_col_schema();
        let two_out = two_col_schema();

        let scan = make_scan_with_schema(schema.clone());
        let plan = PhysicalPlan::Filter(FilterPhysical {
            predicate: and_pred,
            input: Box::new(PhysicalPlan::Scan(scan)),
            tile_size: DEFAULT_FILTER_TILE_SIZE,
            output_schema: schema,
        });

        // Wrap in a Project that only selects entity_id, ts.
        // Filter sits between Project and Scan.
        // But wait — that would require the filter to refer to columns in its
        // input schema (the scan). After type-checking, Filter output = Filter
        // input. So Filter(Scan) with a predicate referencing col1 and col2.
        // Project above it selects entity_id and ts.
        // Plan: Project(Filter(Scan))
        let project_plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            input: Box::new(plan),
            output_schema: two_out,
        });

        let result = prune_columns(project_plan);

        let PhysicalPlan::Project(proj) = result else {
            panic!("expected Project");
        };
        let PhysicalPlan::Filter(filter) = *proj.input else {
            panic!("expected Filter under Project");
        };
        let PhysicalPlan::Scan(scan) = *filter.input else {
            panic!("expected Scan under Filter");
        };
        // Filter needs col1, col2 for predicate; Project needs entity_id, ts.
        // scan.projected_columns must include all four.
        assert!(
            scan.projected_columns.contains(&"col1".to_string()),
            "col1 referenced by filter predicate must be in projected_columns"
        );
        assert!(
            scan.projected_columns.contains(&"col2".to_string()),
            "col2 referenced by filter predicate must be in projected_columns"
        );
        assert!(
            scan.projected_columns.contains(&"entity_id".to_string()),
            "entity_id demanded by project must be in projected_columns"
        );
        assert!(
            scan.projected_columns.contains(&"ts".to_string()),
            "ts demanded by project must be in projected_columns"
        );
        // Exactly 4 columns, nothing extra.
        assert_eq!(scan.projected_columns.len(), 4);
    }

    // ── Projected columns are sorted ─────────────────────────────────────────

    #[test]
    fn projected_columns_are_sorted_for_determinism() {
        // Use a Project with expressions in reverse-alpha order.
        let scan = make_scan_with_schema(ten_col_schema());
        let three_out = OperatorSchema::new(vec![
            ColumnDef::nullable("col3", BqlType::Int),
            ColumnDef::nullable("col2", BqlType::Int),
            ColumnDef::nullable("col1", BqlType::Int),
        ])
        .expect("three col schema");

        let plan = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![
                ProjectPhysicalItem {
                    expr: col_expr("col3", 5, BqlType::Int, true),
                    output_name: "col3".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("col2", 4, BqlType::Int, true),
                    output_name: "col2".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("col1", 3, BqlType::Int, true),
                    output_name: "col1".into(),
                },
            ],
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: three_out,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Project(proj) = result else {
            panic!("expected Project");
        };
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!("expected Scan under Project");
        };
        // entity_id and ts are always included by the pruning pass
        // (required for k-way merge in the scan operator).
        assert_eq!(
            scan.projected_columns,
            vec!["col1", "col2", "col3", "entity_id", "ts"],
            "projected_columns must be sorted regardless of expression order (plus mandatory merge columns)"
        );
    }

    // ── Explain(Project(Scan)): inner plan is pruned, not wrapper schema ────────

    #[test]
    fn explain_prunes_inner_plan_using_inner_output_schema_not_wrapper_schema() {
        // ExplainPhysical has a single-column `(plan: String)` output schema.
        // If the pruning pass mistakenly seeded the inner plan's demand from
        // the wrapper schema, the scan would get projected_columns = ["plan"]
        // (or even empty). The correct behavior is to seed from the inner
        // plan's own output schema so the inner Project(Scan) is pruned
        // to exactly the 2 columns the project needs.
        use crate::physical::ExplainPhysical;

        let scan = make_scan_with_schema(ten_col_schema());
        let two_out = two_col_schema();
        let explain_out = OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)])
            .expect("explain schema");

        let project = PhysicalPlan::Project(ProjectPhysical {
            expressions: vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: two_out,
        });
        let plan = PhysicalPlan::Explain(ExplainPhysical {
            plan: Box::new(project),
            output_schema: explain_out,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Explain(explain) = result else {
            panic!("expected Explain, got {result:?}");
        };
        let PhysicalPlan::Project(proj) = *explain.plan else {
            panic!("expected Project inside Explain");
        };
        let PhysicalPlan::Scan(scan) = *proj.input else {
            panic!("expected Scan under Project");
        };
        // Must be the Project's 2 columns — NOT the wrapper's "plan" column.
        assert_eq!(
            scan.projected_columns,
            vec!["entity_id", "ts"],
            "Explain must seed demand from inner plan output, not wrapper schema"
        );
    }

    // ── DDL leaf node is returned unchanged ───────────────────────────────────

    #[test]
    fn drop_table_leaf_is_returned_unchanged() {
        use crate::physical::DropTablePhysical;
        let plan = PhysicalPlan::DropTable(DropTablePhysical {
            name: "events".into(),
            output_schema: OperatorSchema::new(vec![]).expect("empty"),
        });
        let result = prune_columns(plan);
        assert!(matches!(result, PhysicalPlan::DropTable(_)));
    }
}
