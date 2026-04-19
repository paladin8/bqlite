//! # bqlite-planner
//!
//! Query planner for bqlite.
//!
//! Transforms an AST [`Statement`] into a plain-data [`PhysicalPlan`]
//! descriptor that `bqlite-engine` binds to executable operators at
//! query-execution time. See
//! [`docs/design/planner-pipeline.md`](../../docs/design/planner-pipeline.md)
//! for the full compiler pipeline.
//!
//! ## Wave 2 scope
//!
//! Post TASK-224 (logical plan), TASK-225 (expression compilation),
//! and TASK-226 (physical descriptors + lowering), the planner
//! accepts every Wave 2 statement shape the parser can produce:
//!
//! - `Statement::Query(Pipeline)` — bare source, plus `WHERE`,
//!   `SELECT`, and `LIMIT` pipeline stages. Lowers to
//!   `Scan` / `Filter` / `Project` / `Limit` physical descriptors.
//! - `Statement::Explain(Pipeline)` — lowers the inner pipeline and
//!   wraps it in [`PhysicalPlan::Explain`].
//! - DDL (`Statement::CreateTable` / `DropTable` / `AlterTable` /
//!   `Describe`) — lowers to the matching DDL physical descriptors.
//! - DML (`Statement::Insert`) — lowers `VALUES` / `FROM` bodies via
//!   the logical-phase resolver in [`crate::logical`].
//!
//! `Statement::Delete` and `Statement::DefineAlias` remain rejected
//! by the logical lowering with forward-compat error messages
//! pointing at the Wave 4 tasks that implement them.
//!
//! ## Two-phase compilation
//!
//! [`plan`] is a thin orchestrator that runs both phases:
//!
//! ```text
//! Statement ──▶ LogicalPlan ──▶ PhysicalPlan
//!            (logical::lower_statement)
//!                           (physical::lower_physical)
//! ```
//!
//! `logical::lower_statement` resolves tables against the catalog,
//! type-checks expressions, and validates DDL / DML shapes.
//! `physical::lower_physical` compiles each `TypedExpr` into a
//! `CompiledExpr` and reshapes a handful of fields for the engine
//! bind step. Both phases hold nothing but plain data — the
//! `Box<dyn PhysicalOperator>` materialization lives above this crate
//! in `bqlite-engine` (TASK-232), per
//! `docs/design/planner-pipeline.md` §15.
//!
//! ```no_run
//! use bqlite_ast::Statement;
//! use bqlite_core::Catalog;
//! use bqlite_planner::{plan, PhysicalPlan};
//!
//! fn run(stmt: Statement, catalog: &dyn Catalog, now_ns: i64) -> bqlite_core::Result<PhysicalPlan> {
//!     plan(stmt, catalog, now_ns)
//! }
//! ```

use bqlite_ast::Statement;
use bqlite_core::{Catalog, Result};

pub mod compile;
pub mod compiled;
pub mod demand;
pub mod explain;
pub mod expr;
pub mod logical;
pub mod opt;
pub mod physical;

pub use compile::{
    classify_pattern, compile_pattern, select_strategy, CompiledNfa, MatchExecutionConfig,
    MatchStrategy, NfaState, PatternClass, PoisonTransition, Transition, VariableBindingDef,
};
pub use compiled::{
    ArithKernel, ArrowKernelId, CastKernel, CompareKernel, CompiledExpr, CompiledNode, FunctionId,
    FunctionKernel, InSetKernel, LogicalKernel, UnaryKernel,
};
pub use demand::{
    check_demand_satisfied, ColumnId, CompiledAggExpr, CompiledFusableAggregate,
    DemandCapabilities, DemandPropagation, DemandSet, FusableAggExpr, FusableAggregate,
    StepPropertyRef,
};
pub use explain::{build_explain_node, format_explain, format_expr, ExplainNode};
pub use expr::{FunctionRegistry, ScalarFunctionSig, TypedExpr, TypedExprKind};
pub use logical::{
    classify_delete_predicate, lower_statement, lower_statements, AliasTable, CheapDeleteSpec,
    DeleteFilter, EntityRole, EventSelectKind, FusedDownstream, IngestFormat, InsertFromDescriptor,
    InsertLogicalBody, LogicalPlan, MatchWindowSpec, ProjectItem, SequencePattern, SortDirection,
    TimeRangeSpec, TypedAggExpr,
};
pub use physical::{
    lower_physical, AggregatePhysical, AlterTableAddColumnPhysical, AttributePhysical, CompiledAgg,
    CreateTablePhysical, DeletePhysical, DescribePhysical, DistinctPhysical, DropTablePhysical,
    EventSelectPhysical, ExplainPhysical, FilterPhysical, InsertPhysical, LimitPhysical,
    MergeSourcesPhysical, PhysicalDeleteFilter, PhysicalPlan, ProjectPhysical, ProjectPhysicalItem,
    SamplePhysical, ScanPhysical, SequenceMatchPhysical, SessionizePhysical, SortPhysical,
    SubqueryFilterPhysical, DEFAULT_FILTER_TILE_SIZE, DEFAULT_MAX_GROUPS, DEFAULT_SAMPLE_SEED,
    DEFAULT_SORT_MAX_ROWS, MAX_FILTER_TILE_SIZE, MIN_FILTER_TILE_SIZE,
};

// ─────────────────────────────────────────────────────────────────────────────
// Plan entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Compile a [`Statement`] into a [`PhysicalPlan`] using `catalog` to
/// resolve table references.
///
/// This is the single public entry point for the planner. It runs the
/// full AST → logical → physical → optimization pipeline:
///
/// 1. [`logical::lower_statement`] — catalog resolution, expression
///    typing, DDL / DML validation. Any failure surfaces here as
///    [`bqlite_core::BqliteError::Plan`] or
///    [`bqlite_core::BqliteError::Schema`].
/// 2. [`physical::lower_physical`] — infallible one-to-one lowering
///    that swaps `TypedExpr` for `CompiledExpr` and resolves AST time
///    ranges into absolute [`bqlite_core::TimeRange`] bounds using
///    `now_ns` as the current Unix epoch nanoseconds.
/// 3. [`opt::fuse_match_aggregate::fuse_match_aggregate`] (TASK-320) —
///    detects `Aggregate(SequenceMatch(...))` pairs where the aggregate
///    can be fulfilled from the match output, fuses the aggregate into
///    `SequenceMatchPhysical.fused_aggregate`, and elides the
///    `Aggregate` node. Must run before pushdown and prune so the plan
///    shape is correct for those passes.
/// 4. [`opt::pushdown::pushdown_predicates`] (TASK-227) — moves
///    `Filter(Scan)` conjuncts into `ScanPhysical::scan_predicates`
///    and elides the `Filter` node when all conjuncts are pushable.
///    DDL / DML leaf nodes are returned unchanged.
/// 5. [`opt::prune::prune_columns`] (TASK-228) — propagates a
///    backward demand set from the root to the scan and writes
///    `ScanPhysical::projected_columns` with the minimal sorted column
///    list. DDL / DML leaf nodes are returned unchanged.
///
/// # Errors
///
/// Propagates every failure from `lower_statement` verbatim:
///
/// - `BqliteError::Plan` — unknown table, unsupported pipeline stage,
///   invalid `INSERT` shape, etc.
/// - `BqliteError::Schema` — DDL validation failures (duplicate
///   columns, missing role columns, invalid `ALTER` action).
pub fn plan(statement: Statement, catalog: &dyn Catalog, now_ns: i64) -> Result<PhysicalPlan> {
    let logical = lower_statement(statement, catalog)?;
    finalize_physical(logical, now_ns)
}

/// Lower a full BQL script (zero or more `DefineAlias` statements followed
/// by exactly one terminal statement) into an optimized [`PhysicalPlan`].
///
/// Exists alongside [`plan`] so the engine entrypoint (which consumes the
/// parser's `Vec<Statement>` directly) does not need to re-case the
/// single-vs-multi path. `plan(stmt, ...)` is now a thin shim over
/// `plan_script(vec![stmt], ...)`.
pub fn plan_script(
    statements: Vec<Statement>,
    catalog: &dyn Catalog,
    now_ns: i64,
) -> Result<PhysicalPlan> {
    let logical = lower_statements(statements, catalog)?;
    finalize_physical(logical, now_ns)
}

fn finalize_physical(logical: LogicalPlan, now_ns: i64) -> Result<PhysicalPlan> {
    let physical = lower_physical(logical, now_ns);
    // Wave 3 fusion pass (TASK-320): fuse Aggregate(SequenceMatch) pairs.
    // Runs first so the plan shape downstream sees the fused schema.
    let physical = opt::fuse_match_aggregate::fuse_match_aggregate(physical);
    // Wave 4 sample pushdown (TASK-430). Runs before predicate pushdown
    // so `Sample(Filter(Scan))` first becomes `Filter(Scan_with_sample)`;
    // the subsequent predicate-pushdown pass then collapses the filter
    // into `Scan::scan_predicates`. Running in the other order would
    // leave the filter above the scan because predicate pushdown does
    // not recurse through `Sample`.
    let physical = opt::sample_pushdown::pushdown_sample(physical);
    // Apply Wave 2 optimizer passes in order (TASK-227, TASK-228).
    // Both passes treat DDL / DML leaves as identity, so it is safe to
    // apply them unconditionally regardless of statement kind.
    let physical = opt::pushdown::pushdown_predicates(physical);
    let physical = opt::prune::prune_columns(physical);
    Ok(physical)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bqlite_ast::{
        expr::{Expr, Literal, Spanned},
        pipeline::{Source, TableRef},
        span::{Name, Span},
        DeleteStmt, Pipeline, PipelineStage,
    };
    use bqlite_core::{
        catalog::unknown_table_error,
        property::BqlType,
        schema::{ColumnDef, TableSchema},
        BqliteError, OperatorSchema,
    };

    use super::*;

    // ── Helpers ─────────────────────────────────────────────────────

    /// A minimal in-memory `Catalog` used only by planner tests.
    #[derive(Debug, Default)]
    struct InMemoryCatalog {
        tables: BTreeMap<String, TableSchema>,
    }

    impl InMemoryCatalog {
        fn with(mut self, schema: TableSchema) -> Self {
            self.tables.insert(schema.name().to_string(), schema);
            self
        }
    }

    impl Catalog for InMemoryCatalog {
        fn resolve_table(&self, name: &str) -> Result<TableSchema> {
            self.tables
                .get(name)
                .cloned()
                .ok_or_else(|| unknown_table_error(name))
        }

        fn list_tables(&self) -> Vec<&str> {
            self.tables.keys().map(String::as_str).collect()
        }
    }

    fn events_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .expect("minimal events schema must validate")
    }

    fn table_ref(name: &str) -> TableRef {
        TableRef {
            name: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    fn bare_pipeline(name: &str) -> Pipeline {
        Pipeline {
            source: Source {
                primary: table_ref(name),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        }
    }

    // ── Happy path ──────────────────────────────────────────────────

    #[test]
    fn plans_known_table_to_scan_physical() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Query(bare_pipeline("events"));

        let physical = plan(stmt, &catalog, 0).expect("events must plan");

        let PhysicalPlan::Scan(scan) = physical else {
            panic!("expected Scan, got {physical:?}");
        };
        assert_eq!(scan.table, "events");
        let col_names: Vec<&str> = scan
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            col_names,
            vec!["entity_id", "ts", "event_type", "__seq_id", "__batch_id"]
        );
    }

    #[test]
    fn physical_output_schema_matches_logical_output_schema() {
        // Lowering is one-to-one so both sides report the same schema.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Query(bare_pipeline("events"));

        let physical = plan(stmt, &catalog, 0).unwrap();
        let expected = OperatorSchema::from_table(&events_schema());
        assert_eq!(physical.output_schema(), &expected);
    }

    #[test]
    fn plans_via_dyn_catalog_reference() {
        // Guard the object-safe surface — the engine hands us
        // `&dyn Catalog`, so construct the call through that exact
        // shape rather than `&InMemoryCatalog`.
        let owned = InMemoryCatalog::default().with(events_schema());
        let catalog: &dyn Catalog = &owned;
        let stmt = Statement::Query(bare_pipeline("events"));
        assert!(plan(stmt, catalog, 0).is_ok());
    }

    // ── TASK-430: SAMPLE pushdown end-to-end ─────────────────────────

    /// Build a pipeline that applies SAMPLE directly to a table scan.
    fn sample_over_table(table: &str, fraction: f64, seed: Option<i64>) -> Pipeline {
        Pipeline {
            source: Source {
                primary: table_ref(table),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Sample(bqlite_ast::Sample {
                fraction,
                seed,
                span: Span::EMPTY,
            })],
            span: Span::EMPTY,
        }
    }

    #[test]
    fn plan_pushes_sample_into_scan_and_elides_sample_node() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Query(sample_over_table("events", 0.25, Some(7)));
        let physical = plan(stmt, &catalog, 0).unwrap();
        let PhysicalPlan::Scan(scan) = physical else {
            panic!("expected Scan (sample pushed, Sample elided), got {physical:?}");
        };
        let s = scan.sample.expect("sample pushdown attached");
        assert!((s.fraction - 0.25).abs() < f64::EPSILON);
        assert_eq!(s.seed, 7);
    }

    #[test]
    fn plan_pushes_sample_past_pushable_filter() {
        // `events | WHERE event_type = 'purchase' | SAMPLE(fraction: 0.5)`.
        // After lowering we get `Sample(Filter(Scan))`. The predicate
        // pushdown pass first folds the filter conjunct into
        // `Scan::scan_predicates` (producing `Sample(Scan)`), and then
        // the sample pushdown pass elides the `Sample` node. The final
        // shape must be a bare `Scan` carrying both the pushed predicate
        // and the sample parameters — proving the pass ordering works.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let where_stage = PipelineStage::Where {
            predicate: Spanned {
                node: Expr::Compare {
                    op: bqlite_ast::expr::CompareOp::Equal,
                    left: Box::new(Spanned {
                        node: Expr::Column(Name::synthetic("event_type")),
                        span: Span::EMPTY,
                    }),
                    right: Box::new(Spanned {
                        node: Expr::Literal(Literal::String("purchase".into())),
                        span: Span::EMPTY,
                    }),
                },
                span: Span::EMPTY,
            },
            span: Span::EMPTY,
        };
        let sample_stage = PipelineStage::Sample(bqlite_ast::Sample {
            fraction: 0.5,
            seed: Some(17),
            span: Span::EMPTY,
        });
        let pipeline = Pipeline {
            source: Source {
                primary: table_ref("events"),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![where_stage, sample_stage],
            span: Span::EMPTY,
        };
        let physical = plan(Statement::Query(pipeline), &catalog, 0).unwrap();
        let PhysicalPlan::Scan(scan) = physical else {
            panic!("expected Scan with both predicate + sample pushed, got {physical:?}");
        };
        assert_eq!(
            scan.scan_predicates.len(),
            1,
            "filter predicate must have been pushed"
        );
        let s = scan.sample.expect("sample must have been pushed");
        assert!((s.fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(s.seed, 17);
    }

    #[test]
    fn plan_sample_with_no_seed_substitutes_default() {
        // `None` logical seed is resolved at lowering time to
        // `DEFAULT_SAMPLE_SEED` per physical.rs; the pushdown pass
        // carries that value unchanged.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Query(sample_over_table("events", 0.5, None));
        let physical = plan(stmt, &catalog, 0).unwrap();
        let PhysicalPlan::Scan(scan) = physical else {
            panic!("expected Scan, got {physical:?}");
        };
        let s = scan.sample.expect("sample attached");
        assert_eq!(s.seed, crate::physical::DEFAULT_SAMPLE_SEED);
    }

    // ── Unknown table ───────────────────────────────────────────────

    #[test]
    fn rejects_unknown_table_with_plan_error() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::Query(bare_pipeline("ghost"));

        match plan(stmt, &catalog, 0) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("ghost"), "got: {msg}");
                assert!(msg.contains("unknown table"), "got: {msg}");
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Error passthrough from logical lowering ─────────────────────

    #[test]
    fn propagates_unsupported_pipeline_stage_error() {
        // PIVOT is a later-wave stage (not Wave 4); logical lowering rejects it
        // and `plan` must propagate that error verbatim. SESSIONIZE used to be
        // the canary here, but TASK-425 brought SESSIONIZE online as part of
        // Wave 4 — PIVOT is the next still-deferred shape.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Pivot {
            pivot_column: Name::synthetic("event_type"),
            value_column: Name::synthetic("ts"),
            values: None,
            span: Span::EMPTY,
        });
        match plan(Statement::Query(pipeline), &catalog, 0) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("PIVOT"), "got: {msg}");
            }
            other => panic!("expected Plan error for PIVOT, got {other:?}"),
        }
    }

    #[test]
    fn rejects_joins_on_unknown_table() {
        // With TASK-425 CP5a, source JOINs are supported — but only against
        // tables that exist in the catalog. A JOIN against a non-existent
        // table surfaces the catalog's unknown-table error, which is the
        // right shape for this test post-Wave-4.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.source.joins.push(table_ref("other"));
        match plan(Statement::Query(pipeline), &catalog, 0) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("other"), "got: {msg}");
                assert!(msg.contains("unknown table"), "got: {msg}");
            }
            other => panic!("expected Plan error for unknown JOIN target, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_cheap_delete_without_allow_scan() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        // `WHERE true` is not in the cheap-class allowlist; without
        // ALLOW SCAN the planner must reject with the SS4 error
        // message that mentions the suggested `ALLOW SCAN` fix.
        let stmt = Statement::Delete(DeleteStmt {
            table: Name::synthetic("events"),
            predicate: Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY),
            allow_scan: false,
            span: Span::EMPTY,
        });
        match plan(stmt, &catalog, 0) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("ALLOW SCAN"), "got: {msg}");
            }
            other => panic!("expected Plan error for non-cheap DELETE, got {other:?}"),
        }
    }

    #[test]
    fn lowers_cheap_delete_to_physical_delete() {
        // Entity equality is the simplest cheap-class shape. The
        // resulting PhysicalPlan::Delete should carry the entity
        // value with `EntityRole::AsTombstone`.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Delete(DeleteStmt {
            table: Name::synthetic("events"),
            predicate: Spanned::new(
                Expr::Compare {
                    op: bqlite_ast::expr::CompareOp::Equal,
                    left: Box::new(Spanned::new(
                        Expr::Column(Name::synthetic("entity_id")),
                        Span::EMPTY,
                    )),
                    right: Box::new(Spanned::new(
                        Expr::Literal(Literal::String("alice".into())),
                        Span::EMPTY,
                    )),
                },
                Span::EMPTY,
            ),
            allow_scan: false,
            span: Span::EMPTY,
        });
        let physical = plan(stmt, &catalog, 0).expect("cheap DELETE must plan");
        let PhysicalPlan::Delete(d) = physical else {
            panic!("expected Delete, got something else");
        };
        assert_eq!(d.table_name, "events");
        assert_eq!(d.entity_key_col, "entity_id");
        assert_eq!(d.timestamp_col, "ts");
        assert!(!d.allow_scan);
        match d.filter {
            PhysicalDeleteFilter::Cheap(spec) => {
                assert_eq!(spec.entity_keys.len(), 1);
                assert!(matches!(spec.entity_role, EntityRole::AsTombstone));
                assert!(spec.seq_ids.is_empty());
                assert!(spec.batch_ids.is_empty());
                assert!(spec.time_range.is_none());
            }
            PhysicalDeleteFilter::AllowScan { .. } => panic!("expected Cheap variant"),
        }
    }

    #[test]
    fn rejects_define_alias_as_sole_terminal_statement() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::DefineAlias {
            name: Name::synthetic("active_users"),
            body: bare_pipeline("events"),
            span: Span::EMPTY,
        };
        match plan(stmt, &catalog, 0) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("alias"), "got: {msg}");
            }
            other => panic!("expected Plan error for alias, got {other:?}"),
        }
    }
}
