//! Projection pruning optimizer pass (TASK-228, retargeted by TASK-519).
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
//!    - **`FusedSegmentPhysical`**: walk `steps` in *reverse* order — the
//!      last step appended is the outermost in the chain (`engine/operator-fusion.md`
//!      §4.6). Each step transforms demand:
//!        - `Limit`: identity.
//!        - `Project`: replace demand wholesale with the union of column
//!          names referenced by every output expression (Wave 2 evaluates
//!          every output expression, no partial-expression dropping).
//!        - `Filter`: union demand with the column names referenced by
//!          the predicate (the segment's input must decode them even when
//!          no other operator references them).
//!
//!      The final demand becomes the demand for the segment's `input`,
//!      which is then recursed into.
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
    AggregatePhysical, DistinctPhysical, ExplainPhysical, FusedSegmentPhysical, FusedSegmentStep,
    PhysicalPlan, ScanPhysical, SequenceMatchPhysical, SortPhysical,
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
    // Seed the backward pass with all *declared* output columns of the root
    // node.  System columns (`__`-prefixed, e.g. `__seq_id`, `__batch_id`)
    // are excluded from the seed: they appear in every `ScanPhysical`
    // output schema (via `OperatorSchema::from_table`) but most queries do
    // not reference them.  Seeding with system columns would propagate them
    // into the scan projection for every filter/pass-through query even when
    // no expression actually uses them.  System columns enter the demand only
    // via `collect_column_names` when a predicate or projection expression
    // explicitly references one.
    let initial_demand: HashSet<String> = plan
        .output_schema()
        .columns()
        .iter()
        .filter(|c| !c.name.starts_with("__"))
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
                query_range,
                reader_range,
                scan_predicates,
                projected_columns: _,
                output_schema,
                entity_key_col,
                timestamp_col,
                sample,
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

            // Separate declared and system columns.  System column names
            // (e.g. `__seq_id`, `__batch_id`) are synthesised by the scan
            // operator from segment metadata rather than decoded from disk, so
            // they cannot appear on their own in `projected_columns`.  They
            // ARE valid after TASK-508 wired end-to-end materialisation, but
            // only when every declared column that precedes them in the logical
            // schema is also projected.  `OperatorSchema::from_table` orders
            // columns as: declared columns (0..N-1) then `__seq_id` (N) then
            // `__batch_id` (N+1).  `CompiledNode::Column { index }` carries
            // the full-schema ordinal, so the pruned batch must preserve those
            // positions.  When any system column is demanded we therefore widen
            // the projection to the complete declared column set so that
            // system-column indices remain valid in the output batch.
            use bqlite_core::schema::{BATCH_ID_COLUMN, SEQ_ID_COLUMN};

            // Check which system columns are demanded before consuming the set.
            let need_seq_id = all_demand.contains(SEQ_ID_COLUMN);
            let need_batch_id = all_demand.contains(BATCH_ID_COLUMN);
            let any_system = need_seq_id || need_batch_id;

            let mut cols: Vec<String> = if !any_system {
                // No system column demand — project only demanded declared cols.
                all_demand
                    .into_iter()
                    .filter(|c| !c.starts_with("__"))
                    .collect()
            } else {
                // System columns demanded: include ALL declared columns from
                // the output schema so their full-schema indices are preserved.
                // `OperatorSchema::from_table` positions declared columns at
                // 0..N-1, `__seq_id` at N, `__batch_id` at N+1.
                // `CompiledNode::Column { index }` carries the full-schema
                // ordinal, so every declared column must be present for those
                // indices to remain valid in the pruned batch.
                output_schema
                    .columns()
                    .iter()
                    .filter(|c| !c.name.starts_with("__"))
                    .map(|c| c.name.clone())
                    .collect()
            };
            // Sort declared columns for deterministic plan shape.
            cols.sort_unstable();

            // Append system columns in canonical logical-schema order:
            // `__seq_id` at N (before `__batch_id` at N+1).
            // When `__batch_id` is demanded but `__seq_id` is not, we still
            // include `__seq_id` to keep `__batch_id` at its full-schema index
            // N+1.  `CompiledNode::Column { index }` carries the full-schema
            // ordinal, so the batch must preserve that position.
            if need_seq_id || need_batch_id {
                cols.push(SEQ_ID_COLUMN.to_string());
            }
            if need_batch_id {
                cols.push(BATCH_ID_COLUMN.to_string());
            }

            PhysicalPlan::Scan(ScanPhysical {
                table,
                query_range,
                reader_range,
                scan_predicates,
                projected_columns: cols,
                output_schema,
                entity_key_col,
                timestamp_col,
                sample,
            })
        }

        // ── FusedSegment: walk steps in reverse, transforming demand ────────
        PhysicalPlan::FusedSegment(seg) => {
            let FusedSegmentPhysical {
                input,
                steps,
                sparsity_factor,
                output_schema,
            } = seg;

            // Walk steps in reverse so the last appended step (outermost
            // in the legacy `Limit(Project(Filter(Scan)))` shape) is
            // processed first. Each step transforms the demand it passes
            // *back to its predecessor*. See D3 of TASK-519's plan.
            let mut child_demand = demand;
            for step in steps.iter().rev() {
                match step {
                    FusedSegmentStep::Limit(_) => {
                        // identity — limit does not affect column demand.
                    }
                    FusedSegmentStep::Project(items) => {
                        // Wave 2 Project evaluates every output expression,
                        // so the child needs every column referenced
                        // across them — independent of downstream demand.
                        let mut new_demand = HashSet::new();
                        for item in items {
                            collect_column_names(&item.expr, &mut new_demand);
                        }
                        child_demand = new_demand;
                    }
                    FusedSegmentStep::Filter { predicate, .. } => {
                        collect_column_names(predicate, &mut child_demand);
                    }
                }
            }

            PhysicalPlan::FusedSegment(FusedSegmentPhysical {
                input: Box::new(prune_with_demand(*input, child_demand)),
                steps,
                sparsity_factor,
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

            // Recompute strategy after pruning. `match_events` still lowers
            // to a typed NULL column today, so only a real match-duration
            // demand should influence the runtime configuration here.
            let pruned_needs_match_duration = pruned_output.column("match_duration").is_some();
            let pruned_execution_config = crate::compile::MatchExecutionConfig {
                track_match_duration: pruned_needs_match_duration,
                track_match_events: false,
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
        FusedSegmentPhysical, FusedSegmentStep, PhysicalPlan, ProjectPhysicalItem, ScanPhysical,
        DEFAULT_FILTER_TILE_SIZE, DEFAULT_SPARSITY_FACTOR,
    };

    use super::*;

    // ── FusedSegment fixture builders ─────────────────────────────────────

    /// Wrap a scan in a single-step `FusedSegment([Project(items)])`.
    fn project_segment(
        items: Vec<ProjectPhysicalItem>,
        scan: ScanPhysical,
        out: OperatorSchema,
    ) -> PhysicalPlan {
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(scan)),
            steps: vec![FusedSegmentStep::Project(items)],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: out,
        })
    }

    /// Wrap a scan in a single-step `FusedSegment([Filter(predicate)])`.
    fn filter_segment(
        predicate: CompiledExpr,
        scan: ScanPhysical,
        out: OperatorSchema,
    ) -> PhysicalPlan {
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(scan)),
            steps: vec![FusedSegmentStep::Filter {
                predicate,
                tile_size: DEFAULT_FILTER_TILE_SIZE,
            }],
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: out,
        })
    }

    /// Build a multi-step `FusedSegment(steps over scan)` in source/append
    /// order. Steps appended in source order match `lower_physical`'s
    /// behaviour: index 0 = innermost (closest to scan), index N-1 =
    /// outermost.
    fn segment_over_scan(
        steps: Vec<FusedSegmentStep>,
        scan: ScanPhysical,
        out: OperatorSchema,
    ) -> PhysicalPlan {
        PhysicalPlan::FusedSegment(FusedSegmentPhysical {
            input: Box::new(PhysicalPlan::Scan(scan)),
            steps,
            sparsity_factor: DEFAULT_SPARSITY_FACTOR,
            output_schema: out,
        })
    }

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
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: schema,
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
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

        let plan = project_segment(
            vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            scan,
            two_out,
        );

        let result = prune_columns(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
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
        let plan = filter_segment(pushable_pred("col1", 3), scan, schema);

        let result = prune_columns(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
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
        // FusedSegment([Project, Limit]) over Scan. Steps in source order:
        // index 0 = Project (innermost), index 1 = Limit (outermost). The
        // pruning pass walks steps in reverse so Limit is processed first
        // (identity), then Project (replace demand). The scan must read
        // exactly the 2 columns the Project needs.
        let scan = make_scan_with_schema(ten_col_schema());
        let two_out = two_col_schema();

        let plan = segment_over_scan(
            vec![
                FusedSegmentStep::Project(vec![
                    ProjectPhysicalItem {
                        expr: col_expr("entity_id", 0, BqlType::String, false),
                        output_name: "entity_id".into(),
                    },
                    ProjectPhysicalItem {
                        expr: col_expr("ts", 1, BqlType::Timestamp, false),
                        output_name: "ts".into(),
                    },
                ]),
                FusedSegmentStep::Limit(10),
            ],
            scan,
            two_out,
        );

        let result = prune_columns(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
        assert_eq!(
            scan.projected_columns,
            vec!["entity_id", "ts"],
            "limit must not widen scan beyond what project needs"
        );
    }

    // ── Multi-step demand reset (D3): downstream demand of a Limit step
    // ── must NOT leak past a Project step that doesn't reference it.
    #[test]
    fn project_resets_demand_inside_segment() {
        // FusedSegment([Filter(col1), Project(col2), Limit(5)]) over Scan.
        // Source order: Filter at 0, Project at 1, Limit at 2. The
        // pruning pass walks in reverse: Limit (identity) → Project
        // (replace demand with `col2`) → Filter (union with `col1`).
        // Final demand passed to scan: { col1, col2 }. The downstream
        // demand seeded from output_schema (col2 only) is dropped at
        // the Project step.
        let scan = make_scan_with_schema(ten_col_schema());
        let one_out = OperatorSchema::new(vec![ColumnDef::nullable("col2", BqlType::Int)])
            .expect("one-col schema");

        let plan = segment_over_scan(
            vec![
                FusedSegmentStep::Filter {
                    predicate: pushable_pred("col1", 3),
                    tile_size: DEFAULT_FILTER_TILE_SIZE,
                },
                FusedSegmentStep::Project(vec![ProjectPhysicalItem {
                    expr: col_expr("col2", 4, BqlType::Int, true),
                    output_name: "col2".into(),
                }]),
                FusedSegmentStep::Limit(5),
            ],
            scan,
            one_out,
        );

        let result = prune_columns(plan);
        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
        assert_eq!(
            scan.projected_columns,
            vec!["col1", "col2", "entity_id", "ts"],
            "Project must reset demand mid-segment; Filter unions its predicate columns"
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
            query_range: None,
            reader_range: None,
            scan_predicates: vec![pushable_pred("col3", 5)],
            projected_columns: vec![],
            output_schema: ten_col_schema(),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        };
        let two_out = two_col_schema();

        let plan = project_segment(
            vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            scan_with_pred,
            two_out,
        );

        let result = prune_columns(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment, got {result:?}");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
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
        let plan = project_segment(
            vec![ProjectPhysicalItem {
                expr: sum_expr,
                output_name: "sum".into(),
            }],
            scan,
            out_schema,
        );

        let result = prune_columns(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
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
        // FusedSegment([Filter(col1 AND col2), Project(entity_id, ts)])
        // over Scan. Source order: Filter at 0, Project at 1. Reverse
        // walk: Project (replace demand with entity_id, ts), then Filter
        // (union with col1, col2). Final demand: { col1, col2, entity_id,
        // ts }.
        let and_pred = CompiledExpr {
            node: CompiledNode::And {
                operands: vec![pushable_pred("col1", 3), pushable_pred("col2", 4)],
                kernel: LogicalKernel::ArrowKleene,
            },
            result_type: BqlType::Bool,
            nullable: false,
        };
        let two_out = two_col_schema();
        let scan = make_scan_with_schema(ten_col_schema());

        let plan = segment_over_scan(
            vec![
                FusedSegmentStep::Filter {
                    predicate: and_pred,
                    tile_size: DEFAULT_FILTER_TILE_SIZE,
                },
                FusedSegmentStep::Project(vec![
                    ProjectPhysicalItem {
                        expr: col_expr("entity_id", 0, BqlType::String, false),
                        output_name: "entity_id".into(),
                    },
                    ProjectPhysicalItem {
                        expr: col_expr("ts", 1, BqlType::Timestamp, false),
                        output_name: "ts".into(),
                    },
                ]),
            ],
            scan,
            two_out,
        );

        let result = prune_columns(plan);
        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
        };
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

        let plan = project_segment(
            vec![
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
            scan,
            three_out,
        );

        let result = prune_columns(plan);

        let PhysicalPlan::FusedSegment(seg) = result else {
            panic!("expected FusedSegment");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
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
        // plan's own output schema so the inner FusedSegment(Scan) is pruned
        // to exactly the 2 columns the project step needs.
        use crate::physical::ExplainPhysical;

        let scan = make_scan_with_schema(ten_col_schema());
        let two_out = two_col_schema();
        let explain_out = OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)])
            .expect("explain schema");

        let inner = project_segment(
            vec![
                ProjectPhysicalItem {
                    expr: col_expr("entity_id", 0, BqlType::String, false),
                    output_name: "entity_id".into(),
                },
                ProjectPhysicalItem {
                    expr: col_expr("ts", 1, BqlType::Timestamp, false),
                    output_name: "ts".into(),
                },
            ],
            scan,
            two_out,
        );
        let plan = PhysicalPlan::Explain(ExplainPhysical {
            plan: Box::new(inner),
            output_schema: explain_out,
        });

        let result = prune_columns(plan);

        let PhysicalPlan::Explain(explain) = result else {
            panic!("expected Explain, got {result:?}");
        };
        let PhysicalPlan::FusedSegment(seg) = *explain.plan else {
            panic!("expected FusedSegment inside Explain");
        };
        let PhysicalPlan::Scan(scan) = *seg.input else {
            panic!("expected Scan under FusedSegment");
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
