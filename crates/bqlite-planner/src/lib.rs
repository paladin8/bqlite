//! # bqlite-planner
//!
//! Query planner for bqlite.
//!
//! Transforms an AST [`Statement`] into a plain-data [`PhysicalPlan`]
//! descriptor that `bqlite-engine` binds to executable operators at
//! query-execution time. See
//! [`docs/design/planner-pipeline.md`](../../docs/design/planner-pipeline.md)
//! for the full compiler pipeline — the post–Wave 1 planner will grow a
//! typed logical plan, optimizer passes, expression compiler,
//! desugaring, and fusion framework.
//!
//! ## Wave 1 scope — deliberately minimal
//!
//! This module is the TASK-115 stub. Its job is to give the engine
//! (TASK-118) something to bind against so the end-to-end Wave 1 smoke
//! test (`bqlite query "events"` → TASK-123) can parse → plan →
//! execute against an empty database. The accepted surface is exactly
//! what the Wave 1 parser (TASK-114) can produce:
//!
//! - **Only** [`Statement::Query`] wrapping a [`Pipeline`] with
//!   an empty `stages` list, no joins, and no time-range filter.
//! - The primary table is resolved via the [`Catalog`] handle the
//!   caller passes in; unknown tables surface as
//!   [`BqliteError::Plan`], matching the `TypeError` contract in
//!   `docs/design/planner-pipeline.md` §4.1.
//!
//! Every other statement shape — DDL, DML, `EXPLAIN`, alias
//! definitions, or a query with any pipeline stage — is explicitly
//! rejected with a `Plan` error that names the unsupported construct.
//! Those productions come online in Wave 2 (TASK-224 / TASK-226).
//!
//! ## Data shape
//!
//! Per `docs/design/planner-pipeline.md` §15, the planner emits a
//! **plain-data** tree of structs — never `Box<dyn PhysicalOperator>`.
//! The trait object lives above the planner in the crate dependency
//! graph (`bqlite-operators`), so a planner that held one would
//! violate the dependency direction enforced by
//! `scripts/check-dep-direction.sh`. Binding the plain-data tree to
//! executable operators is TASK-118's job.
//!
//! ```no_run
//! use bqlite_ast::Statement;
//! use bqlite_core::Catalog;
//! use bqlite_planner::{plan, PhysicalPlan};
//!
//! fn run(stmt: Statement, catalog: &dyn Catalog) -> bqlite_core::Result<PhysicalPlan> {
//!     plan(stmt, catalog)
//! }
//! ```

use bqlite_ast::{Pipeline, Statement};
use bqlite_core::{BqliteError, Catalog, OperatorSchema, Result, TableSchema};

// ─────────────────────────────────────────────────────────────────────────────
// Logical plan
// ─────────────────────────────────────────────────────────────────────────────

/// A bqlite logical plan node.
///
/// Wave 1 exposes a single variant — [`LogicalPlan::Scan`] — because
/// the parser only accepts a bare table reference. Wave 2's TASK-224
/// grows this enum with `Filter`, `Project`, `Limit`, `CreateTable`,
/// `Insert`, and `Explain` once their productions land in the parser
/// and the expression compiler (TASK-225) is available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalPlan {
    /// Read every row from a catalog-resolved table.
    ///
    /// The [`TableSchema`] is the authoritative catalog entry, and the
    /// [`OperatorSchema`] is the cached per-node output contract
    /// required by `docs/design/planner-pipeline.md` §4.5: *every
    /// `LogicalPlan` node exposes an `output_schema()` method; the
    /// schema is computed once at construction time and cached on the
    /// node itself*.
    Scan {
        /// The resolved catalog entry for the scanned table.
        table: TableSchema,
        /// Cached operator-output schema — a scan exposes the table's
        /// declared columns plus the `__seq_id` / `__batch_id` system
        /// columns, as produced by [`OperatorSchema::from_table`].
        output_schema: OperatorSchema,
    },
}

impl LogicalPlan {
    /// Build a Wave 1 `Scan` node from a resolved [`TableSchema`].
    ///
    /// The output schema is materialized once here; later waves that
    /// introduce projection pruning will update this in the same
    /// location rather than recompute it on every traversal.
    pub fn scan(table: TableSchema) -> Self {
        let output_schema = OperatorSchema::from_table(&table);
        LogicalPlan::Scan {
            table,
            output_schema,
        }
    }

    /// The output schema of this logical node.
    ///
    /// Returning a reference is deliberate — callers of the optimizer
    /// do repeated schema lookups during demand propagation
    /// (`docs/design/planner-pipeline.md` §6.6) and cannot afford to
    /// rebuild the schema per visit.
    pub fn output_schema(&self) -> &OperatorSchema {
        match self {
            LogicalPlan::Scan { output_schema, .. } => output_schema,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Physical plan
// ─────────────────────────────────────────────────────────────────────────────

/// Plain-data physical plan description.
///
/// Per `docs/design/planner-pipeline.md` §15, the planner never holds
/// `Box<dyn PhysicalOperator>`; the engine's bind step (TASK-118)
/// consumes a `PhysicalPlan` value and materializes an operator tree
/// against the concrete implementations in `bqlite-operators`.
///
/// Wave 1 only carries a scan descriptor — see [`ScanPhysical`] for the
/// fields that get added in Wave 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlan {
    /// Scan every row from a catalog table.
    Scan(ScanPhysical),
}

impl PhysicalPlan {
    /// The schema a downstream consumer observes from this plan root.
    pub fn output_schema(&self) -> &OperatorSchema {
        match self {
            PhysicalPlan::Scan(scan) => &scan.output_schema,
        }
    }
}

/// Plain-data description of a scan operator — Wave 1 subset.
///
/// The full descriptor in `docs/design/planner-pipeline.md` §9.5 also
/// carries `scan_predicates`, `projected_columns`, and a `time_range`.
/// Those fields land in Wave 2 together with the expression compiler
/// (TASK-225) and the predicate-pushdown / projection-pruning passes
/// (TASK-227 / TASK-228). Wave 1 deliberately emits neither, because
/// the parser cannot produce them: a bare table reference has no
/// predicate and no projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPhysical {
    /// Catalog name of the table being scanned — the engine's bind
    /// step re-resolves this through the database's catalog so the
    /// operator sees the same `TableSchema` the planner validated
    /// against.
    pub table: String,
    /// The schema a consumer observes from this scan. Equal to
    /// [`OperatorSchema::from_table`] of the resolved `TableSchema`
    /// for Wave 1; pruning passes will shrink this in Wave 2.
    pub output_schema: OperatorSchema,
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Compile a [`Statement`] into a [`PhysicalPlan`] using `catalog` to
/// resolve table references.
///
/// See the crate documentation for the Wave 1 scope. In short: only
/// `Statement::Query` wrapping an empty pipeline over a known table is
/// accepted; everything else returns [`BqliteError::Plan`] with a
/// message that names the unsupported construct.
///
/// # Errors
///
/// - [`BqliteError::Plan`] if the statement variant is unsupported by
///   Wave 1, if the pipeline contains any stage/join/time-range (since
///   those cannot yet be lowered), or if the catalog cannot resolve
///   the primary table.
/// - Any error returned by [`Catalog::resolve_table`] is forwarded
///   verbatim so callers can pattern-match on the existing
///   unknown-table message shape produced by
///   `bqlite_core::catalog::unknown_table_error`.
pub fn plan(statement: Statement, catalog: &dyn Catalog) -> Result<PhysicalPlan> {
    let logical = build_logical(statement, catalog)?;
    Ok(lower(logical))
}

/// AST → logical: the only Wave 1 shape is a query over a bare table.
fn build_logical(statement: Statement, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    match statement {
        Statement::Query(pipeline) => plan_query_pipeline(pipeline, catalog),
        Statement::Explain(_) => Err(unsupported_statement("EXPLAIN")),
        Statement::Describe(_) => Err(unsupported_statement("DESCRIBE")),
        Statement::CreateTable(_) => Err(unsupported_statement("CREATE TABLE")),
        Statement::AlterTable(_) => Err(unsupported_statement("ALTER TABLE")),
        Statement::DropTable(_) => Err(unsupported_statement("DROP TABLE")),
        Statement::Insert(_) => Err(unsupported_statement("INSERT")),
        Statement::Delete(_) => Err(unsupported_statement("DELETE")),
        Statement::DefineAlias { .. } => Err(unsupported_statement("alias definition")),
    }
}

fn plan_query_pipeline(pipeline: Pipeline, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    // The Wave 1 parser emits `Pipeline { source: {primary, joins:[], time_range:None}, stages:[] }`,
    // but we still validate defensively because nothing in the type
    // system prevents a builder or a future parser fix from handing us
    // a richer pipeline. Rejecting it loudly here beats a silent
    // "identifier scan" that ignores later stages.
    if !pipeline.stages.is_empty() {
        return Err(BqliteError::Plan(
            "Wave 1 planner does not yet support pipeline stages \
             (filter / select / limit land in Wave 2 — TASK-224)"
                .into(),
        ));
    }
    if !pipeline.source.joins.is_empty() {
        return Err(BqliteError::Plan(
            "Wave 1 planner does not yet support JOIN clauses".into(),
        ));
    }
    if pipeline.source.time_range.is_some() {
        return Err(BqliteError::Plan(
            "Wave 1 planner does not yet support source time ranges \
             (`LAST` / `BETWEEN` land in Wave 2)"
                .into(),
        ));
    }

    let table_name = pipeline.source.primary.name.text.as_str();
    let table_schema = catalog.resolve_table(table_name)?;
    Ok(LogicalPlan::scan(table_schema))
}

/// Logical → physical: one-to-one lowering. No optimizer passes.
fn lower(logical: LogicalPlan) -> PhysicalPlan {
    match logical {
        LogicalPlan::Scan {
            table,
            output_schema,
        } => PhysicalPlan::Scan(ScanPhysical {
            table: table.name().to_string(),
            output_schema,
        }),
    }
}

fn unsupported_statement(kind: &str) -> BqliteError {
    BqliteError::Plan(format!(
        "Wave 1 planner does not yet support {kind} statements"
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bqlite_ast::{
        expr::{Expr, Literal, Spanned},
        operator::PipelineStage,
        pipeline::{Source, TableRef, TimeRange},
        span::{Name, Span},
        AlterAction, AlterTableStmt, ColumnDef as AstColumnDef, ColumnRole, CreateTableStmt,
        DeleteStmt, DescribeStmt, DropTableStmt, InsertBody, InsertStmt,
    };
    use bqlite_core::{
        catalog::unknown_table_error,
        property::BqlType,
        schema::{ColumnDef, TableSchema},
    };

    use super::*;

    // ── Helpers ─────────────────────────────────────────────────────

    /// A minimal in-memory `Catalog` used only by planner tests.
    /// The manifest-backed catalog lives in `bqlite-storage` and would
    /// drag in I/O setup we don't want here.
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

    /// Build the minimum viable `events` schema that TASK-125's
    /// bootstrap rule seeds into every fresh database.
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

    /// Build a `TableRef` with a synthetic span so tests can construct
    /// `Pipeline` values without owning any source text.
    fn table_ref(name: &str) -> TableRef {
        TableRef {
            name: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    /// Build a bare-identifier pipeline — the exact shape the Wave 1
    /// parser emits for `bqlite query "<name>"`.
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

        let physical = plan(stmt, &catalog).expect("events must plan");

        match physical {
            PhysicalPlan::Scan(scan) => {
                assert_eq!(scan.table, "events");
                // Output schema should match what the engine will bind
                // against — the declared columns plus `__seq_id` and
                // `__batch_id`.
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
        }
    }

    #[test]
    fn physical_output_schema_matches_logical_output_schema() {
        // §4.5 invariant: logical and physical report the same output
        // schema because lowering is one-to-one with no pruning.
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Query(bare_pipeline("events"));

        let physical = plan(stmt, &catalog).unwrap();
        let expected = OperatorSchema::from_table(&events_schema());
        assert_eq!(physical.output_schema(), &expected);
    }

    #[test]
    fn logical_scan_constructor_caches_output_schema() {
        let node = LogicalPlan::scan(events_schema());
        let first = node.output_schema() as *const OperatorSchema;
        let second = node.output_schema() as *const OperatorSchema;
        assert_eq!(
            first, second,
            "repeated calls must return the same borrowed value"
        );
        assert_eq!(node.output_schema().len(), 5);
    }

    #[test]
    fn plans_via_dyn_catalog_reference() {
        // Guard the object-safe surface. The engine will hand us
        // `&dyn Catalog`, so constructing the planner call through
        // that exact shape is the load-bearing test, not
        // `&InMemoryCatalog`.
        let owned = InMemoryCatalog::default().with(events_schema());
        let catalog: &dyn Catalog = &owned;
        let stmt = Statement::Query(bare_pipeline("events"));
        assert!(plan(stmt, catalog).is_ok());
    }

    // ── Unknown table ───────────────────────────────────────────────

    #[test]
    fn rejects_unknown_table_with_plan_error() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::Query(bare_pipeline("ghost"));

        match plan(stmt, &catalog) {
            Err(BqliteError::Plan(msg)) => {
                assert!(
                    msg.contains("ghost"),
                    "error should name the missing table, got: {msg}"
                );
                assert!(
                    msg.contains("unknown table"),
                    "error should match the catalog's unknown-table phrasing, got: {msg}"
                );
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Unsupported pipeline shapes ─────────────────────────────────

    #[test]
    fn rejects_pipeline_stages() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Limit {
            count: 10,
            span: Span::EMPTY,
        });
        match plan(Statement::Query(pipeline), &catalog) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("pipeline stages"), "got: {msg}");
            }
            other => panic!("expected Plan error for pipeline stages, got {other:?}"),
        }
    }

    #[test]
    fn rejects_joins_on_source() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.source.joins.push(table_ref("other"));
        match plan(Statement::Query(pipeline), &catalog) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("JOIN"), "got: {msg}");
            }
            other => panic!("expected Plan error for joins, got {other:?}"),
        }
    }

    #[test]
    fn rejects_source_time_range() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let mut pipeline = bare_pipeline("events");
        pipeline.source.time_range = Some(TimeRange::Last(86_400_000_000_000));
        match plan(Statement::Query(pipeline), &catalog) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("time ranges"), "got: {msg}");
            }
            other => panic!("expected Plan error for time range, got {other:?}"),
        }
    }

    // ── Unsupported statement variants ──────────────────────────────

    #[test]
    fn rejects_explain_statement() {
        let catalog = InMemoryCatalog::default().with(events_schema());
        let stmt = Statement::Explain(bare_pipeline("events"));
        expect_unsupported(&catalog, stmt, "EXPLAIN");
    }

    #[test]
    fn rejects_create_table() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::CreateTable(CreateTableStmt {
            table: Name::synthetic("events"),
            columns: vec![],
            span: Span::EMPTY,
        });
        expect_unsupported(&catalog, stmt, "CREATE TABLE");
    }

    #[test]
    fn rejects_drop_table() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::DropTable(DropTableStmt {
            table: Name::synthetic("events"),
            span: Span::EMPTY,
        });
        expect_unsupported(&catalog, stmt, "DROP TABLE");
    }

    #[test]
    fn rejects_insert() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::Insert(InsertStmt {
            table: Name::synthetic("events"),
            body: InsertBody::Values(vec![]),
            span: Span::EMPTY,
        });
        expect_unsupported(&catalog, stmt, "INSERT");
    }

    #[test]
    fn rejects_describe() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::Describe(DescribeStmt {
            table: Name::synthetic("events"),
            span: Span::EMPTY,
        });
        expect_unsupported(&catalog, stmt, "DESCRIBE");
    }

    #[test]
    fn rejects_alter_table() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::AlterTable(AlterTableStmt {
            table: Name::synthetic("events"),
            action: AlterAction::AddColumn(AstColumnDef {
                name: Name::synthetic("source"),
                data_type: BqlType::String,
                role: ColumnRole::Regular,
                not_null: false,
                default: None,
                span: Span::EMPTY,
            }),
            span: Span::EMPTY,
        });
        expect_unsupported(&catalog, stmt, "ALTER TABLE");
    }

    #[test]
    fn rejects_delete() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::Delete(DeleteStmt {
            table: Name::synthetic("events"),
            predicate: Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY),
            span: Span::EMPTY,
        });
        expect_unsupported(&catalog, stmt, "DELETE");
    }

    #[test]
    fn rejects_define_alias() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::DefineAlias {
            name: Name::synthetic("active_users"),
            body: bare_pipeline("events"),
            span: Span::EMPTY,
        };
        expect_unsupported(&catalog, stmt, "alias definition");
    }

    /// Shared assertion: the plan call must fail with a
    /// `BqliteError::Plan` whose message contains `construct` —
    /// confirming both the error variant and the user-visible naming.
    fn expect_unsupported(catalog: &dyn Catalog, stmt: Statement, construct: &str) {
        match plan(stmt, catalog) {
            Err(BqliteError::Plan(msg)) => {
                assert!(
                    msg.contains(construct),
                    "message `{msg}` must name the construct `{construct}`"
                );
            }
            other => panic!("expected Plan error for {construct}, got {other:?}"),
        }
    }
}
