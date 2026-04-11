//! Wave 2 logical plan enum + AST → logical lowering.
//!
//! This module is the runtime counterpart to
//! `docs/design/planner/logical-plan-nodes.md` (TASK-204). Every
//! Wave 2 depth node enumerated in §3 of that doc is represented here,
//! with the schema-at-construction-time invariant from §4: every
//! `LogicalPlan` value caches its `OperatorSchema` at build time, and
//! the construction APIs are the only way to produce a well-schemed
//! value. Pattern-matching an invalid plan is impossible because
//! there is no public constructor that skips the schema computation.
//!
//! ## Scope
//!
//! - **Wave 2 depth**: Scan, Filter, Project, Limit, CreateTable,
//!   DropTable, AlterTableAddColumn, Describe, Insert, Explain —
//!   §4.1 through §4.10 of the design doc.
//! - **Later waves**: `SequenceMatch`, `Aggregate`, `Sort`, etc. are
//!   not in this enum. When those come online (Wave 3+), their
//!   variants get added to [`LogicalPlan`] directly — the enum is
//!   the planner's extensibility point and does not churn across
//!   waves.
//!
//! ## Where expression typing lives
//!
//! TASK-204 §1 says: *"Per-node fields that carry expressions are
//! typed as `TypedExpr` here without further elaboration."* The full
//! `TypedExpr` compiler lands in TASK-225 (depends on TASK-205). This
//! module is TASK-224, which depends only on TASK-204 — so the
//! Filter/Project nodes store raw [`bqlite_ast::expr::Spanned<Expr>`]
//! values for now, with the following split of responsibilities:
//!
//! - **Project output schema** (where typing *must* exist so the
//!   schema-at-construction invariant holds) handles the bare
//!   column-reference and wildcard cases directly by consulting the
//!   input schema. These are the only forms the Wave 2 acceptance
//!   query (§acceptance in TASKS.md) produces on the `SELECT` side.
//! - **Any other expression in Project** — arithmetic, function
//!   calls, comparisons — is rejected at lowering time with a
//!   `Plan` error that names TASK-225 as the task that will lift the
//!   restriction. This is honest to TASK-224's scope (logical plan
//!   enum, not expression compiler) and does not pretend to support
//!   forms the Wave 2 acceptance gate does not need.
//! - **Filter predicates** are stored verbatim. Filter's output
//!   schema equals its input's, so no typing happens at construction
//!   and the raw `Spanned<Expr>` is enough for TASK-227's predicate
//!   pushdown pass to operate on (which re-walks the expression
//!   against the scan's advertised `CompiledExpr` shape anyway —
//!   docs/design/planner/expression-compilation.md).
//!
//! When TASK-225 lands, these `Spanned<Expr>` fields move to
//! `TypedExpr` values produced by its schema-resolving walker.

use bqlite_ast::expr::{Expr, Spanned};
use bqlite_ast::pipeline::{Pipeline, TimeRange};
use bqlite_ast::{PipelineStage, SelectItem, SelectItemKind, Statement};
use bqlite_core::{
    BqlType, BqliteError, Catalog, ColumnDef, OperatorSchema, PropertyValue, Result, TableSchema,
};

// ─────────────────────────────────────────────────────────────────────────────
// The logical plan enum
// ─────────────────────────────────────────────────────────────────────────────

/// A bqlite logical plan node.
///
/// Every variant is the cached, validated form that downstream
/// passes (TASK-227 predicate pushdown, TASK-228 projection pruning,
/// TASK-226 physical lowering, TASK-232 engine bind) consume. See
/// `docs/design/planner/logical-plan-nodes.md` §4 for the
/// authoritative per-variant specification; this doc comment only
/// points at the design doc and records the Wave 2 scope.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    /// `Pipeline.source` — read every row from a catalog-resolved
    /// table. §4.1 of the design doc.
    Scan {
        /// The resolved catalog entry for the primary table.
        table: TableSchema,
        /// Optional `LAST <duration>` / `BETWEEN <ts> AND <ts>` range
        /// from `Pipeline.source.time_range`. Always `None` until the
        /// parser emits the syntax; the field exists so Wave 4 does
        /// not have to retrofit the enum.
        time_range: Option<TimeRange>,
        /// `JOIN <table>` tables. Empty in Wave 2; populated by Wave 4.
        joined_tables: Vec<TableSchema>,
        /// Scan-level predicates populated by TASK-227's predicate
        /// pushdown pass. Stored as raw `Spanned<Expr>` until TASK-225
        /// migrates them to `TypedExpr`. Always empty when this node
        /// is first constructed by lowering.
        scan_predicates: Vec<Spanned<Expr>>,
        /// Declared-column names the scan actually reads, populated
        /// by TASK-228's projection pruning pass. Empty means
        /// "read all columns" — the pruning pass replaces the empty
        /// list with a narrower one.
        projected_columns: Vec<String>,
        /// Cached output schema — the declared table columns plus
        /// `__seq_id` / `__batch_id` per §4.1.
        output_schema: OperatorSchema,
    },

    /// `| WHERE <predicate>` — row filter. §4.2.
    Filter {
        /// The predicate expression. Stored raw for Wave 2; TASK-225
        /// migrates to `TypedExpr`.
        predicate: Spanned<Expr>,
        input: Box<LogicalPlan>,
        /// Identical to `input.output_schema()` — filter never
        /// changes the column shape.
        output_schema: OperatorSchema,
    },

    /// `| SELECT <items>` — projection. §4.3.
    Project {
        /// Output items in order. Each item carries its final
        /// output name and the cached output type needed to
        /// construct the node's `output_schema`.
        expressions: Vec<ProjectItem>,
        input: Box<LogicalPlan>,
        /// Built from `expressions` at construction time.
        output_schema: OperatorSchema,
    },

    /// `| LIMIT <count>` — row cap. §4.4.
    Limit {
        count: u64,
        input: Box<LogicalPlan>,
        /// Identical to `input.output_schema()`.
        output_schema: OperatorSchema,
    },

    /// `CREATE TABLE <name> (<columns>)`. §4.5.
    ///
    /// Held as a destructured record rather than a `TableSchema`
    /// because the table does not yet exist in the catalog. The
    /// engine bind step reconstructs a `TableSchema` via
    /// `TableSchema::new(...)` with these fields.
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        entity_key: String,
        event_time: String,
        event_type: String,
        /// Empty — DDL produces no rows.
        output_schema: OperatorSchema,
    },

    /// `DROP TABLE <name>`. §4.6.
    DropTable {
        name: String,
        /// Empty.
        output_schema: OperatorSchema,
    },

    /// `ALTER TABLE <name> ADD COLUMN <column>`. §4.7.
    AlterTableAddColumn {
        name: String,
        /// The new column, already validated against the schema-
        /// evolution rules (type-system.md §5.3).
        column: ColumnDef,
        /// Empty.
        output_schema: OperatorSchema,
    },

    /// `DESCRIBE <name>`. §4.8.
    Describe {
        name: String,
        /// Fixed four-column schema: `(name, type, nullable, role)`.
        output_schema: OperatorSchema,
    },

    /// `INSERT INTO <table> <body>`. §4.9.
    Insert {
        /// Catalog-resolved target table.
        table: TableSchema,
        /// Resolved insert body — literals coerced for `VALUES`,
        /// options normalized and map resolved for `FROM`.
        body: InsertLogicalBody,
        /// Empty.
        output_schema: OperatorSchema,
    },

    /// `EXPLAIN <pipeline>`. §4.10.
    Explain {
        /// The lowered child plan. Storing the lowered plan rather
        /// than the raw `Pipeline` makes `EXPLAIN` fail-fast on type
        /// errors — see §9 of the design doc.
        plan: Box<LogicalPlan>,
        /// Fixed single-column schema: `(plan: String)`.
        output_schema: OperatorSchema,
    },
}

/// A single output item in a `LogicalPlan::Project`.
///
/// The struct stores both the untyped source expression (for
/// downstream passes that need to re-walk it, e.g. predicate
/// pushdown that inlines project expressions into filter residuals)
/// and the cached output name + type that feed
/// `OperatorSchema::new`.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectItem {
    /// The source expression — stored raw for Wave 2 (see module
    /// docs). TASK-225 replaces this with `TypedExpr`.
    pub expr: Spanned<Expr>,
    /// Output column name — either the user's `AS alias`, the bare
    /// column's own name, or a planner-assigned synthetic name.
    pub output_name: String,
    /// Cached output type — computed from the input schema during
    /// lowering so the project's `output_schema` can be built at
    /// construction time.
    pub bql_type: BqlType,
    /// Whether the output column is nullable. For bare column
    /// references this is the input column's `nullable` flag.
    pub nullable: bool,
}

/// Resolved body of a `LogicalPlan::Insert`.
///
/// Distinct from the AST's [`bqlite_ast::InsertBody`] because the
/// AST is catalog-free and the logical form is catalog-resolved:
/// literals are coerced to `PropertyValue`, option keys are
/// normalized, and the `map` clause is validated against the target
/// table schema. §4.9 of the design doc.
#[derive(Debug, Clone, PartialEq)]
pub enum InsertLogicalBody {
    /// `VALUES (...)` — literal tuples, each value coerced to its
    /// target column's `BqlType` at plan time.
    Values(Vec<Vec<PropertyValue>>),
    /// `FROM <path> WITH (...)` — bulk load with a resolved ingest
    /// descriptor.
    From(InsertFromDescriptor),
}

/// Resolved `INSERT ... FROM` descriptor (§4.9).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertFromDescriptor {
    /// File path literal from the AST (`'data.csv'` → `"data.csv"`).
    pub path: String,
    /// Format resolved from `WITH (format: '...')` or inferred from
    /// the path extension.
    pub format: IngestFormat,
    /// Normalized options from the flat `WITH (...)` list, **excluding**
    /// the `format` key (which moves to `format`) and the `map` key
    /// (which moves to `column_map`). Values are coerced to
    /// `PropertyValue`.
    pub options: Vec<(String, PropertyValue)>,
    /// Resolved column mapping, `(source, target)` pairs. Empty when
    /// the AST had no `map` clause. Every `target` is guaranteed to
    /// be a valid column name in the target table; duplicate
    /// `target`s are rejected at lowering time.
    pub column_map: Vec<(String, String)>,
}

/// Supported ingest formats (§4.9).
///
/// The enum is full-surface so Wave 4's JSONL / Parquet support
/// (TASK-410) is a pure engine extension rather than another
/// planner change. Wave 2 lowering only accepts `Csv`; the other
/// two variants produce a `Plan` error naming the `format: '...'`
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IngestFormat {
    Csv,
    JsonL,
    Parquet,
}

impl LogicalPlan {
    /// The cached output schema for this plan node.
    ///
    /// Reference-returning by design — callers (optimizer passes,
    /// physical lowering, explain formatting) walk the plan tree
    /// many times and must not pay a per-visit schema rebuild.
    pub fn output_schema(&self) -> &OperatorSchema {
        match self {
            LogicalPlan::Scan { output_schema, .. }
            | LogicalPlan::Filter { output_schema, .. }
            | LogicalPlan::Project { output_schema, .. }
            | LogicalPlan::Limit { output_schema, .. }
            | LogicalPlan::CreateTable { output_schema, .. }
            | LogicalPlan::DropTable { output_schema, .. }
            | LogicalPlan::AlterTableAddColumn { output_schema, .. }
            | LogicalPlan::Describe { output_schema, .. }
            | LogicalPlan::Insert { output_schema, .. }
            | LogicalPlan::Explain { output_schema, .. } => output_schema,
        }
    }

    // ─── Constructors ─────────────────────────────────────────────────────

    /// Build a Wave 1-compatible `Scan` node: bare table, no joins,
    /// no time range, no optimizer-populated predicate/projection
    /// state.
    ///
    /// This constructor matches the Wave 1 TASK-115 stub so the
    /// existing smoke-test path continues to work while richer
    /// scans land via [`Self::scan_with_time_range`] and the
    /// optimizer passes.
    pub fn scan(table: TableSchema) -> Self {
        Self::scan_full(table, None, Vec::new())
    }

    /// Build a `Scan` with an optional time range. Joined tables are
    /// still empty (Wave 4).
    pub fn scan_with_time_range(table: TableSchema, time_range: Option<TimeRange>) -> Self {
        Self::scan_full(table, time_range, Vec::new())
    }

    /// Internal full-arity scan constructor. Kept private so public
    /// callers don't accidentally set `scan_predicates` /
    /// `projected_columns` on a freshly-built node — those fields
    /// are optimizer-populated.
    fn scan_full(
        table: TableSchema,
        time_range: Option<TimeRange>,
        joined_tables: Vec<TableSchema>,
    ) -> Self {
        let output_schema = OperatorSchema::from_table(&table);
        LogicalPlan::Scan {
            table,
            time_range,
            joined_tables,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema,
        }
    }

    /// Wrap `input` in a `Filter` whose output schema equals
    /// `input.output_schema()`.
    pub fn filter(predicate: Spanned<Expr>, input: LogicalPlan) -> Self {
        let output_schema = input.output_schema().clone();
        LogicalPlan::Filter {
            predicate,
            input: Box::new(input),
            output_schema,
        }
    }

    /// Wrap `input` in a `Project` with the given items. Builds the
    /// output schema from `expressions` and enforces
    /// `OperatorSchema`'s duplicate-name rule.
    pub fn project(expressions: Vec<ProjectItem>, input: LogicalPlan) -> Result<Self> {
        let cols: Vec<ColumnDef> = expressions
            .iter()
            .map(|item| ColumnDef {
                name: item.output_name.clone(),
                bql_type: item.bql_type.clone(),
                nullable: item.nullable,
                default_value: None,
            })
            .collect();
        let output_schema = OperatorSchema::new(cols)?;
        Ok(LogicalPlan::Project {
            expressions,
            input: Box::new(input),
            output_schema,
        })
    }

    /// Wrap `input` in a `Limit` whose output schema equals
    /// `input.output_schema()`.
    pub fn limit(count: u64, input: LogicalPlan) -> Self {
        let output_schema = input.output_schema().clone();
        LogicalPlan::Limit {
            count,
            input: Box::new(input),
            output_schema,
        }
    }

    /// Wrap `plan` in an `Explain` node. The output schema is the
    /// fixed single-column `(plan: String)` shape documented in
    /// §4.10.
    pub fn explain(plan: LogicalPlan) -> Self {
        LogicalPlan::Explain {
            plan: Box::new(plan),
            output_schema: explain_output_schema(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared fixed schemas (cheap to rebuild; cached lazily would need a sync
// primitive — the per-call cost is three `ColumnDef` allocations)
// ─────────────────────────────────────────────────────────────────────────────

/// Build the canonical empty output schema used by DDL / DML nodes.
///
/// Wired into the DDL lowering paths in TASK-224 C2; the C1
/// checkpoint tests the helper directly so a regression surfaces
/// before C2 starts touching it.
#[allow(dead_code)]
fn empty_output_schema() -> OperatorSchema {
    OperatorSchema::new(Vec::new()).expect("empty column list is always a valid schema")
}

/// Build the fixed four-column `DESCRIBE` output schema.
/// `(name, type, nullable, role)` — see §4.8.
///
/// Same C1/C2 split as [`empty_output_schema`]: tested in C1,
/// wired into `Describe` lowering in C2.
#[allow(dead_code)]
fn describe_output_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("name", BqlType::String),
        ColumnDef::required("type", BqlType::String),
        ColumnDef::required("nullable", BqlType::Bool),
        ColumnDef::required("role", BqlType::String),
    ])
    .expect("describe output schema is a fixed, non-duplicating shape")
}

/// Build the fixed single-column `EXPLAIN` output schema — §4.10.
fn explain_output_schema() -> OperatorSchema {
    OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)])
        .expect("explain output schema is a fixed, non-duplicating shape")
}

// ─────────────────────────────────────────────────────────────────────────────
// AST → LogicalPlan lowering
// ─────────────────────────────────────────────────────────────────────────────

/// Lower an AST [`Statement`] into a [`LogicalPlan`].
///
/// This is the single entry point TASK-226 (physical lowering) and
/// the engine bind step (TASK-232) call once the parser produces
/// Wave 2 statement shapes. Table references are resolved via
/// `catalog`; unknown tables surface as [`BqliteError::Plan`] via
/// `bqlite_core::catalog::unknown_table_error`.
///
/// Wave 2 scope: Query and Explain pipelines are fully lowered in
/// this checkpoint (TASK-224 C1). DDL and Insert lowering lands in
/// C2 — they currently return a placeholder `Plan` error.
pub fn lower_statement(statement: Statement, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    match statement {
        Statement::Query(pipeline) => lower_query_pipeline(pipeline, catalog),
        Statement::Explain(pipeline) => {
            let plan = lower_query_pipeline(pipeline, catalog)?;
            Ok(LogicalPlan::explain(plan))
        }
        Statement::Delete(_) => Err(BqliteError::Plan(
            "DELETE is deferred to Wave 4 alongside tombstones (TASK-404)".into(),
        )),
        Statement::DefineAlias { .. } => Err(BqliteError::Plan(
            "alias definitions are deferred to Wave 4 (TASK-407)".into(),
        )),
        Statement::CreateTable(_)
        | Statement::DropTable(_)
        | Statement::AlterTable(_)
        | Statement::Describe(_)
        | Statement::Insert(_) => Err(BqliteError::Plan(
            "DDL/DML logical lowering lands in TASK-224 checkpoint 2 — \
             Query and Explain pipelines are supported in checkpoint 1"
                .into(),
        )),
    }
}

/// Lower a `Pipeline` (the body of `Statement::Query` or the inner
/// pipeline of `Statement::Explain`) into a logical-plan tree.
///
/// The fold order is documented in planner-pipeline.md §4.2 and
/// logical-plan-nodes.md §7.2: start from a `Scan` over the source,
/// then fold pipeline stages left-to-right so the deepest node in
/// the final tree is the scan and the shallowest node is the last
/// stage.
fn lower_query_pipeline(pipeline: Pipeline, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    if !pipeline.source.joins.is_empty() {
        return Err(BqliteError::Plan(
            "JOIN clauses are deferred to Wave 4 (TASK-407 cohorts / entity-joins)".into(),
        ));
    }

    // Resolve the source table against the catalog.
    let table_name = pipeline.source.primary.name.text.as_str();
    let table_schema = catalog.resolve_table(table_name)?;

    // Build the initial Scan. Time range carries through from the
    // AST's `source.time_range` field — the parser already decodes
    // `LAST <duration>` into nanoseconds and `BETWEEN ... AND ...`
    // into `(String, String)` (query-language.md §16).
    let mut plan = LogicalPlan::scan_with_time_range(table_schema, pipeline.source.time_range);

    // Fold pipeline stages in order. Each stage wraps `plan` in a
    // new logical node whose input is the previous `plan`.
    for stage in pipeline.stages {
        plan = fold_stage(stage, plan)?;
    }

    Ok(plan)
}

/// Fold a single AST pipeline stage into the accumulated plan.
fn fold_stage(stage: PipelineStage, acc: LogicalPlan) -> Result<LogicalPlan> {
    match stage {
        PipelineStage::Where { predicate, .. } => Ok(LogicalPlan::filter(predicate, acc)),

        PipelineStage::Select {
            distinct, items, ..
        } => {
            if distinct {
                return Err(BqliteError::Plan(
                    "SELECT DISTINCT lowers to `Distinct(Project(...))` in Wave 3 — \
                     Wave 2 does not yet support the `distinct` flag"
                        .into(),
                ));
            }
            lower_select(items, acc)
        }

        PipelineStage::Limit { count, .. } => Ok(LogicalPlan::limit(count, acc)),

        // Everything else is a later-wave shape. Each rejection names
        // the stage and the wave it lands in so the error message
        // doubles as documentation for users who run into it.
        other => Err(BqliteError::Plan(format!(
            "pipeline stage `{}` is not yet supported in Wave 2 — see the Wave 2 scope in TASKS.md",
            stage_kind_name(&other)
        ))),
    }
}

/// Stage-kind name for error messages — avoids exposing the enum
/// `Debug` format in user-visible errors.
fn stage_kind_name(stage: &PipelineStage) -> &'static str {
    match stage {
        PipelineStage::Where { .. } => "WHERE",
        PipelineStage::Select { .. } => "SELECT",
        PipelineStage::Let { .. } => "LET",
        PipelineStage::Match { .. } => "MATCH",
        PipelineStage::Funnel(_) => "FUNNEL",
        PipelineStage::Retention(_) => "RETENTION",
        PipelineStage::Sessionize(_) => "SESSIONIZE",
        PipelineStage::Stats { .. } => "STATS",
        PipelineStage::OrderBy { .. } => "ORDER BY",
        PipelineStage::Limit { .. } => "LIMIT",
        PipelineStage::Pivot { .. } => "PIVOT",
        PipelineStage::EventSelect(_) => "FIRST/LAST/NTH",
        PipelineStage::Sample(_) => "SAMPLE",
        PipelineStage::Attribute(_) => "ATTRIBUTE",
    }
}

/// Lower a `SELECT` stage into a `Project` node.
///
/// Wave 2 handles the shapes the acceptance query (TASKS.md Wave 2
/// acceptance block) and its obvious neighbours produce:
///
/// - Bare column references, optionally aliased.
/// - `*` wildcards, which expand to one `ProjectItem` per input
///   column.
///
/// Every other expression form is rejected with a clear "TASK-225
/// will lift this" error rather than silently emitting a wrong-typed
/// schema. This keeps the Wave 2 contract honest: the logical-plan
/// task ships the plan tree; the expression-compiler task ships
/// typed expressions on top of it.
fn lower_select(items: Vec<SelectItem>, acc: LogicalPlan) -> Result<LogicalPlan> {
    if items.is_empty() {
        return Err(BqliteError::Plan(
            "SELECT must have at least one output item".into(),
        ));
    }

    // A wildcard is only legal as the sole item (query-language.md §10).
    let has_wildcard = items.iter().any(|it| {
        matches!(
            it.kind,
            SelectItemKind::Wildcard | SelectItemKind::QualifiedWildcard(_)
        )
    });
    if has_wildcard && items.len() > 1 {
        return Err(BqliteError::Plan(
            "`*` wildcard must be the sole item in a SELECT — mixing `*` with other items is \
             not supported"
                .into(),
        ));
    }

    let input_schema = acc.output_schema().clone();
    let mut project_items: Vec<ProjectItem> = Vec::new();

    for item in items {
        match item.kind {
            SelectItemKind::Wildcard => {
                // Expand to one item per input column, preserving the
                // input's order. Each expanded item stores a
                // synthetic `Expr::Column` so TASK-225's compiler has
                // an expression to operate on if it re-walks the
                // project. System columns (`__seq_id`, `__batch_id`)
                // are included because query-language.md §10 says
                // `SELECT *` means every visible column.
                for col in input_schema.columns() {
                    project_items.push(ProjectItem {
                        expr: Spanned::new(
                            Expr::Column(bqlite_ast::Name::synthetic(&col.name)),
                            item.span,
                        ),
                        output_name: col.name.clone(),
                        bql_type: col.bql_type.clone(),
                        nullable: col.nullable,
                    });
                }
            }

            SelectItemKind::QualifiedWildcard(_) => {
                return Err(BqliteError::Plan(
                    "qualified wildcards `table.*` are deferred to Wave 4 joins (TASK-407)".into(),
                ));
            }

            SelectItemKind::Expr(expr) => {
                // Wave 2 scope: only bare column references (with or
                // without an alias) are schema-typeable here; TASK-225
                // lifts this for arbitrary expressions.
                match &expr.node {
                    Expr::Column(name) => {
                        let column_name = name.text.clone();
                        let (_, col_def) = input_schema.column(&column_name).ok_or_else(|| {
                            BqliteError::Plan(format!(
                                "SELECT: unknown column `{column_name}` in input schema"
                            ))
                        })?;
                        let output_name = item
                            .alias
                            .map(|a| a.text)
                            .unwrap_or_else(|| column_name.clone());
                        project_items.push(ProjectItem {
                            expr,
                            output_name,
                            bql_type: col_def.bql_type.clone(),
                            nullable: col_def.nullable,
                        });
                    }
                    Expr::Qualified { .. } => {
                        return Err(BqliteError::Plan(
                            "qualified column references `table.col` are deferred to Wave 4 \
                             joins (TASK-407)"
                                .into(),
                        ));
                    }
                    _ => {
                        return Err(BqliteError::Plan(
                            "SELECT on computed expressions (arithmetic, function calls, \
                             comparisons, ...) requires the expression compiler landing in \
                             TASK-225; Wave 2 TASK-224 supports bare column references and \
                             `*` wildcards only"
                                .into(),
                        ));
                    }
                }
            }
        }
    }

    LogicalPlan::project(project_items, acc)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bqlite_ast::expr::{Expr, Literal, Spanned};
    use bqlite_ast::pipeline::{Source, TableRef};
    use bqlite_ast::{Name, PipelineStage, SelectItem, SelectItemKind, Span};

    use bqlite_core::catalog::unknown_table_error;
    use bqlite_core::{BqlType, ColumnDef as CoreColumnDef, TableSchema};

    use super::*;

    // ── Helpers ─────────────────────────────────────────────────────────────

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

    fn purchases_schema() -> TableSchema {
        TableSchema::new(
            "purchases",
            vec![
                CoreColumnDef::required("user_id", BqlType::String),
                CoreColumnDef::required("ts", BqlType::Timestamp),
                CoreColumnDef::required("event", BqlType::String),
                CoreColumnDef::nullable("amount", BqlType::Float),
                CoreColumnDef::nullable("country", BqlType::String),
            ],
            "user_id",
            "ts",
            "event",
        )
        .unwrap()
    }

    fn bare_pipeline(name: &str) -> Pipeline {
        Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic(name),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        }
    }

    fn pipeline_with_stages(name: &str, stages: Vec<PipelineStage>) -> Pipeline {
        let mut p = bare_pipeline(name);
        p.stages = stages;
        p
    }

    fn lit_true() -> Spanned<Expr> {
        Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY)
    }

    fn column_expr(name: &str) -> Spanned<Expr> {
        Spanned::new(Expr::Column(Name::synthetic(name)), Span::EMPTY)
    }

    fn select_bare_column(name: &str) -> SelectItem {
        SelectItem {
            kind: SelectItemKind::Expr(column_expr(name)),
            alias: None,
            span: Span::EMPTY,
        }
    }

    fn select_aliased_column(name: &str, alias: &str) -> SelectItem {
        SelectItem {
            kind: SelectItemKind::Expr(column_expr(name)),
            alias: Some(Name::synthetic(alias)),
            span: Span::EMPTY,
        }
    }

    // ── Scan lowering ───────────────────────────────────────────────────────

    #[test]
    fn bare_pipeline_lowers_to_scan_with_full_output_schema() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(Statement::Query(bare_pipeline("purchases")), &cat).unwrap();
        match &plan {
            LogicalPlan::Scan {
                table,
                time_range,
                joined_tables,
                scan_predicates,
                projected_columns,
                output_schema,
            } => {
                assert_eq!(table.name(), "purchases");
                assert!(time_range.is_none());
                assert!(joined_tables.is_empty());
                assert!(scan_predicates.is_empty());
                assert!(projected_columns.is_empty());
                // Output schema = declared columns + system columns.
                let names: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(
                    names,
                    vec![
                        "user_id",
                        "ts",
                        "event",
                        "amount",
                        "country",
                        "__seq_id",
                        "__batch_id"
                    ]
                );
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    #[test]
    fn scan_caches_output_schema_stably() {
        let plan = LogicalPlan::scan(purchases_schema());
        let first = plan.output_schema() as *const OperatorSchema;
        let second = plan.output_schema() as *const OperatorSchema;
        assert_eq!(first, second);
    }

    #[test]
    fn unknown_table_surfaces_as_plan_error() {
        let cat = InMemoryCatalog::default();
        let err = lower_statement(Statement::Query(bare_pipeline("ghost")), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("ghost"));
                assert!(msg.contains("unknown table"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn join_clauses_rejected_until_wave_4() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("other"),
            span: Span::EMPTY,
        });
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("JOIN")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Filter lowering ─────────────────────────────────────────────────────

    #[test]
    fn where_stage_wraps_scan_in_filter() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: lit_true(),
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Filter {
                predicate: _,
                input,
                output_schema,
            } => {
                // Filter's output schema equals the Scan's.
                assert_eq!(output_schema, input.output_schema());
                assert!(matches!(&**input, LogicalPlan::Scan { .. }));
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    // ── Limit lowering ──────────────────────────────────────────────────────

    #[test]
    fn limit_stage_wraps_scan_in_limit() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Limit {
                count: 100,
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Limit {
                count,
                input,
                output_schema,
            } => {
                assert_eq!(*count, 100);
                assert_eq!(output_schema, input.output_schema());
                assert!(matches!(&**input, LogicalPlan::Scan { .. }));
            }
            other => panic!("expected Limit, got {other:?}"),
        }
    }

    // ── Project lowering (bare columns) ─────────────────────────────────────

    #[test]
    fn select_bare_columns_builds_project() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![
                    select_bare_column("user_id"),
                    select_bare_column("ts"),
                    select_bare_column("amount"),
                ],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Project {
                expressions,
                input,
                output_schema,
            } => {
                assert!(matches!(&**input, LogicalPlan::Scan { .. }));
                let names: Vec<&str> = expressions.iter().map(|i| i.output_name.as_str()).collect();
                assert_eq!(names, vec!["user_id", "ts", "amount"]);

                // Type-propagation: amount is declared nullable in
                // purchases, so the Project output preserves that.
                let amount_item = expressions
                    .iter()
                    .find(|i| i.output_name == "amount")
                    .unwrap();
                assert_eq!(amount_item.bql_type, BqlType::Float);
                assert!(amount_item.nullable);

                // Output schema column order matches the items.
                let schema_names: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(schema_names, vec!["user_id", "ts", "amount"]);
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn select_with_alias_uses_alias_as_output_name() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_aliased_column("amount", "total")],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Project { expressions, .. } => {
                assert_eq!(expressions.len(), 1);
                assert_eq!(expressions[0].output_name, "total");
                assert_eq!(expressions[0].bql_type, BqlType::Float);
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn select_wildcard_expands_to_all_input_columns() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![SelectItem {
                    kind: SelectItemKind::Wildcard,
                    alias: None,
                    span: Span::EMPTY,
                }],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Project { expressions, .. } => {
                let names: Vec<&str> = expressions.iter().map(|i| i.output_name.as_str()).collect();
                // Wildcard expands to the input's full schema
                // including system columns.
                assert_eq!(
                    names,
                    vec![
                        "user_id",
                        "ts",
                        "event",
                        "amount",
                        "country",
                        "__seq_id",
                        "__batch_id"
                    ]
                );
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn select_unknown_column_is_a_plan_error() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_bare_column("ghost")],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("ghost"));
                assert!(msg.contains("unknown column"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn select_distinct_rejected_until_wave_3() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: true,
                items: vec![select_bare_column("user_id")],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("DISTINCT")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn select_computed_expression_rejected_until_task_225() {
        // `SELECT user_id + 1` — arithmetic on a column. Cannot be
        // schema-typed without the expression compiler.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![SelectItem {
                    kind: SelectItemKind::Expr(Spanned::new(
                        Expr::Binary {
                            op: bqlite_ast::BinaryOp::Add,
                            left: Box::new(column_expr("user_id")),
                            right: Box::new(Spanned::new(
                                Expr::Literal(Literal::Int(1)),
                                Span::EMPTY,
                            )),
                        },
                        Span::EMPTY,
                    )),
                    alias: Some(Name::synthetic("adjusted")),
                    span: Span::EMPTY,
                }],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("TASK-225"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn select_wildcard_mixed_with_items_is_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![
                    SelectItem {
                        kind: SelectItemKind::Wildcard,
                        alias: None,
                        span: Span::EMPTY,
                    },
                    select_bare_column("user_id"),
                ],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("sole item")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Multi-stage fold (the Wave 2 acceptance shape) ──────────────────────

    #[test]
    fn where_then_select_then_limit_folds_into_correct_tree() {
        // Mirrors the Wave 2 acceptance query shape:
        //   purchases | where <pred> | select user_id, ts, amount | limit 100
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![
                PipelineStage::Where {
                    predicate: lit_true(),
                    span: Span::EMPTY,
                },
                PipelineStage::Select {
                    distinct: false,
                    items: vec![
                        select_bare_column("user_id"),
                        select_bare_column("ts"),
                        select_bare_column("amount"),
                    ],
                    span: Span::EMPTY,
                },
                PipelineStage::Limit {
                    count: 100,
                    span: Span::EMPTY,
                },
            ],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();

        // Outer Limit → Project → Filter → Scan.
        let LogicalPlan::Limit { input, count, .. } = &plan else {
            panic!("expected outer Limit, got {plan:?}");
        };
        assert_eq!(*count, 100);

        let LogicalPlan::Project { input: inner, .. } = &**input else {
            panic!("expected Project below Limit");
        };
        let LogicalPlan::Filter { input: scan, .. } = &**inner else {
            panic!("expected Filter below Project");
        };
        assert!(matches!(&**scan, LogicalPlan::Scan { .. }));

        // The outermost output schema is the Project's — three
        // columns, Filter/Limit pass it through unchanged.
        let names: Vec<&str> = plan
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["user_id", "ts", "amount"]);
    }

    // ── Explain ─────────────────────────────────────────────────────────────

    #[test]
    fn explain_wraps_lowered_child_and_uses_fixed_schema() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(Statement::Explain(bare_pipeline("purchases")), &cat).unwrap();
        match &plan {
            LogicalPlan::Explain {
                plan: child,
                output_schema,
            } => {
                assert!(matches!(&**child, LogicalPlan::Scan { .. }));
                let names: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(names, vec!["plan"]);
                assert_eq!(output_schema.columns()[0].bql_type, BqlType::String);
            }
            other => panic!("expected Explain, got {other:?}"),
        }
    }

    #[test]
    fn explain_fails_fast_on_unknown_table_in_child_pipeline() {
        let cat = InMemoryCatalog::default();
        let err = lower_statement(Statement::Explain(bare_pipeline("ghost")), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("ghost")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── DDL / DML stubs — C2 lands these ────────────────────────────────────

    #[test]
    fn create_table_is_deferred_to_c2() {
        use bqlite_ast::{
            AlterAction, AlterTableStmt, ColumnDef as AstColumnDef, ColumnRole, CreateTableStmt,
            DescribeStmt, DropTableStmt, InsertBody, InsertStmt,
        };
        let cat = InMemoryCatalog::default();
        let stmt = Statement::CreateTable(CreateTableStmt {
            table: Name::synthetic("t"),
            columns: vec![],
            span: Span::EMPTY,
        });
        let _ = AstColumnDef {
            name: Name::synthetic("c"),
            data_type: BqlType::Int,
            role: ColumnRole::Regular,
            not_null: false,
            default: None,
            span: Span::EMPTY,
        }; // smoke-reference the AST type so the import is live
        let err = lower_statement(stmt, &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("C2") || msg.contains("checkpoint 2")),
            other => panic!("expected Plan error, got {other:?}"),
        }

        // Same placeholder applies to the other DDL / Insert variants.
        for stmt in [
            Statement::DropTable(DropTableStmt {
                table: Name::synthetic("t"),
                span: Span::EMPTY,
            }),
            Statement::AlterTable(AlterTableStmt {
                table: Name::synthetic("t"),
                action: AlterAction::AddColumn(AstColumnDef {
                    name: Name::synthetic("c"),
                    data_type: BqlType::Int,
                    role: ColumnRole::Regular,
                    not_null: false,
                    default: None,
                    span: Span::EMPTY,
                }),
                span: Span::EMPTY,
            }),
            Statement::Describe(DescribeStmt {
                table: Name::synthetic("t"),
                span: Span::EMPTY,
            }),
            Statement::Insert(InsertStmt {
                table: Name::synthetic("t"),
                body: InsertBody::Values(vec![]),
                span: Span::EMPTY,
            }),
        ] {
            assert!(matches!(
                lower_statement(stmt, &cat),
                Err(BqliteError::Plan(_))
            ));
        }
    }

    #[test]
    fn delete_is_deferred_to_wave_4() {
        use bqlite_ast::DeleteStmt;
        let cat = InMemoryCatalog::default();
        let stmt = Statement::Delete(DeleteStmt {
            table: Name::synthetic("t"),
            predicate: lit_true(),
            span: Span::EMPTY,
        });
        let err = lower_statement(stmt, &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("Wave 4")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn explain_schema_and_describe_schema_helpers_are_valid() {
        // Tripwire: if someone accidentally introduces duplicate
        // column names in the fixed schemas, the helper will panic
        // at runtime. This test exercises both helpers to surface
        // that failure up front in CI.
        let e = explain_output_schema();
        assert_eq!(e.len(), 1);
        let d = describe_output_schema();
        assert_eq!(d.len(), 4);
        let empty = empty_output_schema();
        assert_eq!(empty.len(), 0);
    }
}
