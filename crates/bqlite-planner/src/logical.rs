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
//! Post TASK-225: every expression-carrying field on a logical plan
//! node is a [`TypedExpr`] — a schema-resolved, type-checked
//! expression built via [`TypedExpr::from_ast`]. Filter's
//! `predicate`, `Project`'s `ProjectItem::expr`, and `Scan`'s
//! optimizer-populated `scan_predicates` all use the typed form,
//! which means the logical-plan construction invariant — "holding a
//! value implies the plan is well-typed" — extends all the way
//! down to the expression level.
//!
//! Lowering obtains a single [`FunctionRegistry`] per query
//! (constructed via [`FunctionRegistry::with_builtins`]) and
//! threads it into every `TypedExpr::from_ast` call made during
//! pipeline-stage folding. Later waves that need query-scoped
//! custom functions can plumb a caller-owned registry through this
//! same API surface without reshaping the fold.

use std::collections::HashSet;

use bqlite_ast::expr::{Expr, Literal};
use bqlite_ast::pipeline::{Pipeline, TimeRange};
use bqlite_ast::{
    AlterAction, AlterTableStmt, ColumnDef as AstColumnDef, ColumnRole, CreateTableStmt,
    DescribeStmt, DropTableStmt, InsertBody, InsertStmt, PipelineStage, SelectItem, SelectItemKind,
    Statement,
};
use bqlite_core::{
    BqlType, BqliteError, Catalog, ColumnDef, OperatorSchema, PropertyValue, Result, TableSchema,
};

use crate::expr::{FunctionRegistry, TypedExpr};

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
        /// pushdown pass. Always empty when this node is first
        /// constructed by lowering; TASK-227 rewrites the plan tree
        /// to move pushable conjuncts from a parent `Filter` into
        /// this vec.
        scan_predicates: Vec<TypedExpr>,
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
        /// The predicate expression. Type-checked against
        /// `input.output_schema()` at construction time; the
        /// constructor rejects predicates whose `result_type` is
        /// not [`BqlType::Bool`].
        predicate: TypedExpr,
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
/// Post-TASK-225, the item carries a fully [`TypedExpr`] and a
/// planner-assigned output name. The result type and nullability
/// the project's `output_schema` needs at construction time are
/// already cached on the [`TypedExpr`] — there is no separate
/// `bql_type` / `nullable` field.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectItem {
    /// The typed output expression. Type-checked against the
    /// project's input schema at construction time; the expression
    /// compiler (TASK-225) resolves column references, infers
    /// result types, and rejects unsupported expression shapes.
    pub expr: TypedExpr,
    /// Output column name — either the user's `AS alias`, the bare
    /// column's own name, or a planner-assigned synthetic name.
    pub output_name: String,
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

    /// Wrap `input` in a `Filter`. The predicate must already be
    /// type-checked against `input.output_schema()`; its
    /// `result_type` must be [`BqlType::Bool`], otherwise
    /// construction returns a plan error.
    pub fn filter(predicate: TypedExpr, input: LogicalPlan) -> Result<Self> {
        if predicate.result_type != BqlType::Bool {
            return Err(BqliteError::Plan(format!(
                "filter predicate must have type `Bool`, got `{}`",
                predicate.result_type
            )));
        }
        let output_schema = input.output_schema().clone();
        Ok(LogicalPlan::Filter {
            predicate,
            input: Box::new(input),
            output_schema,
        })
    }

    /// Wrap `input` in a `Project` with the given items. Builds the
    /// output schema from each item's [`TypedExpr::result_type`]
    /// and [`TypedExpr::nullable`], then runs it through
    /// [`OperatorSchema::new`]'s duplicate-name check.
    pub fn project(expressions: Vec<ProjectItem>, input: LogicalPlan) -> Result<Self> {
        let cols: Vec<ColumnDef> = expressions
            .iter()
            .map(|item| ColumnDef {
                name: item.output_name.clone(),
                bql_type: item.expr.result_type.clone(),
                nullable: item.expr.nullable,
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
fn empty_output_schema() -> OperatorSchema {
    OperatorSchema::new(Vec::new()).expect("empty column list is always a valid schema")
}

/// Build the fixed four-column `DESCRIBE` output schema.
/// `(name, type, nullable, role)` — see §4.8.
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
/// Every Wave 2 depth variant from logical-plan-nodes.md §3 is
/// covered: pipeline queries (+ EXPLAIN) landed in C1, DDL /
/// Insert landed in C2. `Statement::Delete` and
/// `Statement::DefineAlias` are explicitly rejected with forward-
/// compat messages pointing at the Wave 4 tasks that own them.
pub fn lower_statement(statement: Statement, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    match statement {
        Statement::Query(pipeline) => lower_query_pipeline(pipeline, catalog),
        Statement::Explain(pipeline) => {
            let plan = lower_query_pipeline(pipeline, catalog)?;
            Ok(LogicalPlan::explain(plan))
        }
        Statement::CreateTable(stmt) => lower_create_table(stmt, catalog),
        Statement::DropTable(stmt) => lower_drop_table(stmt, catalog),
        Statement::AlterTable(stmt) => lower_alter_table(stmt, catalog),
        Statement::Describe(stmt) => lower_describe(stmt, catalog),
        Statement::Insert(stmt) => lower_insert(stmt, catalog),
        Statement::Delete(_) => Err(BqliteError::Plan(
            "DELETE is deferred to Wave 4 alongside tombstones (TASK-404)".into(),
        )),
        Statement::DefineAlias { .. } => Err(BqliteError::Plan(
            "alias definitions are deferred to Wave 4 (TASK-407)".into(),
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

    // Function registry for expression-level type checking. Wave 2
    // ships the built-in set (`like`, `regex`); later waves extend
    // via the registry's `register` API.
    let registry = FunctionRegistry::with_builtins();

    // Fold pipeline stages in order. Each stage wraps `plan` in a
    // new logical node whose input is the previous `plan`.
    for stage in pipeline.stages {
        plan = fold_stage(stage, plan, &registry)?;
    }

    Ok(plan)
}

/// Fold a single AST pipeline stage into the accumulated plan.
fn fold_stage(
    stage: PipelineStage,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    match stage {
        PipelineStage::Where { predicate, .. } => {
            let typed = TypedExpr::from_ast(&predicate, acc.output_schema(), registry)?;
            LogicalPlan::filter(typed, acc)
        }

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
            lower_select(items, acc, registry)
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
/// Every expression in the items list is type-checked against the
/// input schema via [`TypedExpr::from_ast`], which fully supports
/// Wave 2's expression surface (arithmetic, comparisons, function
/// calls, etc). The `*` wildcard expands to one item per input
/// column; qualified wildcards and qualified column references
/// remain deferred to Wave 4 joins.
///
/// Output naming per §4.3:
/// - Explicit alias (`expr AS name`) → `name`.
/// - Bare column reference → the column's own name.
/// - Computed expression without an alias → parser error (caught
///   upstream), but if one slips through we fall back to a
///   planner-synthetic `expr_<idx>` label to avoid crashing.
fn lower_select(
    items: Vec<SelectItem>,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    if items.is_empty() {
        return Err(BqliteError::Plan(
            "SELECT must have at least one output item".into(),
        ));
    }

    let input_schema = acc.output_schema().clone();
    let mut project_items: Vec<ProjectItem> = Vec::new();

    for (idx, item) in items.into_iter().enumerate() {
        match item.kind {
            SelectItemKind::Wildcard => {
                // `SELECT *` excludes the implicit `__seq_id` /
                // `__batch_id` system columns per query-language.md
                // §10 — they remain accessible when named explicitly.
                for (column_index, col) in input_schema.columns().iter().enumerate() {
                    if col.is_system() {
                        continue;
                    }
                    project_items.push(ProjectItem {
                        expr: TypedExpr::column(column_index, col, item.span),
                        output_name: col.name.clone(),
                    });
                }
            }

            SelectItemKind::QualifiedWildcard(_) => {
                return Err(BqliteError::Plan(
                    "qualified wildcards `table.*` are deferred to Wave 4 joins (TASK-407)".into(),
                ));
            }

            SelectItemKind::Expr(expr) => {
                // Derive the output name from the alias if present,
                // or from a bare column reference, or fall back to a
                // synthetic label.
                let output_name = if let Some(alias) = item.alias.clone() {
                    alias.text
                } else {
                    match &expr.node {
                        Expr::Column(name) => name.text.clone(),
                        Expr::Paren(inner) => match &inner.node {
                            Expr::Column(name) => name.text.clone(),
                            _ => format!("expr_{idx}"),
                        },
                        _ => format!("expr_{idx}"),
                    }
                };
                let typed = TypedExpr::from_ast(&expr, &input_schema, registry)?;
                project_items.push(ProjectItem {
                    expr: typed,
                    output_name,
                });
            }
        }
    }

    LogicalPlan::project(project_items, acc)
}

// ─────────────────────────────────────────────────────────────────────────────
// DDL lowering — §4.5 – §4.8
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `CREATE TABLE <name> (<columns>)` into a `CreateTable` node.
///
/// Validation happens *at plan time*, not at engine bind time:
///
/// 1. The target name must not already exist in the catalog.
/// 2. Role assignment: exactly one column per required role
///    (ENTITY KEY, EVENT TIME, EVENT TYPE).
/// 3. [`TableSchema::new`] is invoked as the authoritative validator
///    — it enforces every rule in type-system.md §5.1 (type
///    constraints on role columns, NOT NULL on role columns,
///    unique names, reserved-prefix check, and default-value type
///    consistency).
///
/// The returned [`LogicalPlan::CreateTable`] holds the **destructured**
/// pieces (columns, entity_key, event_time, event_type) rather than
/// a `TableSchema` value, matching the design doc §4.5 because the
/// catalog does not yet contain the table — `TableSchema` carries a
/// catalog identity in its `name()` field that only becomes meaningful
/// once the engine bind step (TASK-232) atomically writes it to the
/// manifest.
fn lower_create_table(stmt: CreateTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let name = stmt.table.text.clone();

    // §4.5 error: duplicate table.
    if catalog.resolve_table(&name).is_ok() {
        return Err(BqliteError::Schema(format!(
            "CREATE TABLE: table `{name}` already exists"
        )));
    }

    // Scan roles. Exactly one per required role — anything else is a
    // plan-time error before we even hand the list to TableSchema::new.
    let mut entity_key: Option<String> = None;
    let mut event_time: Option<String> = None;
    let mut event_type: Option<String> = None;
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(stmt.columns.len());

    for ast_col in stmt.columns {
        let col_name = ast_col.name.text.clone();
        match ast_col.role {
            ColumnRole::EntityKey => {
                if entity_key.is_some() {
                    return Err(BqliteError::Schema(format!(
                        "CREATE TABLE `{name}`: multiple ENTITY KEY columns"
                    )));
                }
                entity_key = Some(col_name.clone());
            }
            ColumnRole::EventTime => {
                if event_time.is_some() {
                    return Err(BqliteError::Schema(format!(
                        "CREATE TABLE `{name}`: multiple EVENT TIME columns"
                    )));
                }
                event_time = Some(col_name.clone());
            }
            ColumnRole::EventType => {
                if event_type.is_some() {
                    return Err(BqliteError::Schema(format!(
                        "CREATE TABLE `{name}`: multiple EVENT TYPE columns"
                    )));
                }
                event_type = Some(col_name.clone());
            }
            ColumnRole::Regular => {}
        }
        columns.push(ast_column_to_core(ast_col)?);
    }

    let entity_key = entity_key.ok_or_else(|| {
        BqliteError::Schema(format!(
            "CREATE TABLE `{name}`: exactly one column must be declared ENTITY KEY"
        ))
    })?;
    let event_time = event_time.ok_or_else(|| {
        BqliteError::Schema(format!(
            "CREATE TABLE `{name}`: exactly one column must be declared EVENT TIME"
        ))
    })?;
    let event_type = event_type.ok_or_else(|| {
        BqliteError::Schema(format!(
            "CREATE TABLE `{name}`: exactly one column must be declared EVENT TYPE"
        ))
    })?;

    // Authoritative §5.1 validation — duplicates, role-column types,
    // NOT NULL on role columns, default-value type consistency, etc.
    // We throw the TableSchema away and hold only its pre-validated
    // inputs; the engine bind step reconstructs the same value.
    TableSchema::new(
        name.clone(),
        columns.clone(),
        &entity_key,
        &event_time,
        &event_type,
    )?;

    Ok(LogicalPlan::CreateTable {
        name,
        columns,
        entity_key,
        event_time,
        event_type,
        output_schema: empty_output_schema(),
    })
}

/// Lower `DROP TABLE <name>`.
///
/// Catalog lookup raises the unknown-table `Plan` error for a
/// missing table, matching §4.6. There is no `IF EXISTS` modifier.
fn lower_drop_table(stmt: DropTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let name = stmt.table.text.clone();
    let _ = catalog.resolve_table(&name)?;
    Ok(LogicalPlan::DropTable {
        name,
        output_schema: empty_output_schema(),
    })
}

/// Lower `ALTER TABLE <name> ADD COLUMN <column>`.
///
/// Enforces the §4.7 plan-time rules:
/// - Unknown table → `Plan` error.
/// - Duplicate column name → `Schema` error.
/// - Role columns are frozen at CREATE TABLE → only `Regular` roles
///   are allowed.
/// - NOT NULL without DEFAULT would make existing rows read as NULL
///   for the added column, violating the constraint → `Schema`
///   error.
fn lower_alter_table(stmt: AlterTableStmt, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let name = stmt.table.text.clone();
    let table = catalog.resolve_table(&name)?;

    match stmt.action {
        AlterAction::AddColumn(ast_col) => {
            let col_name = ast_col.name.text.clone();

            // Role columns are frozen at CREATE TABLE. Only `Regular`
            // columns can be added via ALTER in v1.
            if !matches!(ast_col.role, ColumnRole::Regular) {
                return Err(BqliteError::Schema(format!(
                    "ALTER TABLE `{name}` ADD COLUMN `{col_name}`: \
                     role columns (ENTITY KEY / EVENT TIME / EVENT TYPE) are frozen \
                     at CREATE TABLE and cannot be added via ALTER"
                )));
            }

            // Duplicate-name check against the existing schema.
            if table.columns().iter().any(|c| c.name == col_name) {
                return Err(BqliteError::Schema(format!(
                    "ALTER TABLE `{name}` ADD COLUMN `{col_name}`: column already exists"
                )));
            }

            let column = ast_column_to_core(ast_col)?;

            // NOT NULL without DEFAULT is a schema-evolution error —
            // existing rows would have no value to supply for the
            // new column (type-system.md §5.3).
            if !column.nullable && column.default_value.is_none() {
                return Err(BqliteError::Schema(format!(
                    "ALTER TABLE `{name}` ADD COLUMN `{col_name}`: \
                     a NOT NULL column must declare a DEFAULT — existing rows would \
                     otherwise read as NULL"
                )));
            }

            Ok(LogicalPlan::AlterTableAddColumn {
                name,
                column,
                output_schema: empty_output_schema(),
            })
        }
    }
}

/// Lower `DESCRIBE <name>`.
///
/// Unknown table → `Plan` error. The fixed four-column output
/// schema is built once via [`describe_output_schema`].
fn lower_describe(stmt: DescribeStmt, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let name = stmt.table.text.clone();
    let _ = catalog.resolve_table(&name)?;
    Ok(LogicalPlan::Describe {
        name,
        output_schema: describe_output_schema(),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// DML lowering — §4.9 INSERT
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `INSERT INTO <table> <body>`.
///
/// Resolves the target table against the catalog, then resolves
/// the body — coercing literals for `VALUES`, normalizing options
/// and the column-rename map for `FROM`. Both paths share the
/// rule that the target table must exist at plan time.
fn lower_insert(stmt: InsertStmt, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let table = catalog.resolve_table(&stmt.table.text)?;
    let body = resolve_insert_body(stmt.body, &table)?;
    Ok(LogicalPlan::Insert {
        table,
        body,
        output_schema: empty_output_schema(),
    })
}

/// Resolve an AST [`InsertBody`] against the target table's schema.
fn resolve_insert_body(body: InsertBody, table: &TableSchema) -> Result<InsertLogicalBody> {
    match body {
        InsertBody::Values(rows) => {
            resolve_insert_values(rows, table).map(InsertLogicalBody::Values)
        }
        InsertBody::From { path, options, map } => {
            resolve_insert_from(path, options, map, table).map(InsertLogicalBody::From)
        }
    }
}

/// Resolve `INSERT INTO t VALUES (lit, ...), ...` into a
/// `Vec<Vec<PropertyValue>>`.
///
/// Per §4.9 each row is:
/// - **Arity-checked** against the table's declared column count
///   (system columns `__seq_id` / `__batch_id` are excluded because
///   they are auto-generated by the ingest layer).
/// - **Coerced** column-by-column to the target `BqlType` via
///   [`PropertyValue::coerce_to`].
/// - **NOT NULL**-checked for NULL literals in non-nullable columns.
///
/// Errors name the offending row index so an operator can fix the
/// failing tuple without re-running the query.
fn resolve_insert_values(
    rows: Vec<Vec<Literal>>,
    table: &TableSchema,
) -> Result<Vec<Vec<PropertyValue>>> {
    let expected_arity = table.columns().len();
    let mut coerced_rows: Vec<Vec<PropertyValue>> = Vec::with_capacity(rows.len());

    for (row_idx, row) in rows.into_iter().enumerate() {
        if row.len() != expected_arity {
            return Err(BqliteError::Plan(format!(
                "INSERT INTO `{}` VALUES row {row_idx}: arity {} does not match \
                 the table's {expected_arity} declared columns",
                table.name(),
                row.len()
            )));
        }

        let mut coerced_row: Vec<PropertyValue> = Vec::with_capacity(expected_arity);
        for (col_idx, lit) in row.into_iter().enumerate() {
            let col = &table.columns()[col_idx];
            let raw = literal_to_property_value(lit);

            // NULL in NOT NULL column — error before attempting coercion.
            if matches!(raw, PropertyValue::Null) {
                if !col.nullable {
                    return Err(BqliteError::Plan(format!(
                        "INSERT INTO `{}` VALUES row {row_idx}: NULL in NOT NULL column `{}`",
                        table.name(),
                        col.name
                    )));
                }
                coerced_row.push(PropertyValue::Null);
                continue;
            }

            let coerced = raw.coerce_to(&col.bql_type).ok_or_else(|| {
                BqliteError::Plan(format!(
                    "INSERT INTO `{}` VALUES row {row_idx}: cannot coerce literal for column `{}` to {}",
                    table.name(),
                    col.name,
                    col.bql_type
                ))
            })?;
            coerced_row.push(coerced);
        }
        coerced_rows.push(coerced_row);
    }

    Ok(coerced_rows)
}

/// Resolve `INSERT INTO t FROM '<path>' WITH (...)` into an
/// [`InsertFromDescriptor`].
///
/// Normalizes the flat option list (extracting `format` into a
/// typed [`IngestFormat`] and coercing `delimiter` / `header` to
/// `PropertyValue`), rejects unknown option keys, and resolves
/// the `map` clause against the target schema (every target must
/// exist; duplicate targets error). Per §4.9, source-file I/O is
/// deferred to engine-time (TASK-233), so the descriptor is
/// catalog-checked but the file is not opened here.
///
/// Wave 2 only ships `IngestFormat::Csv`; `JsonL` and `Parquet`
/// are rejected with a forward-compat error that names TASK-410.
fn resolve_insert_from(
    path: String,
    options: Vec<bqlite_ast::InsertOption>,
    map: Option<Vec<bqlite_ast::ColumnMapping>>,
    table: &TableSchema,
) -> Result<InsertFromDescriptor> {
    let mut format_opt: Option<IngestFormat> = None;
    let mut resolved_options: Vec<(String, PropertyValue)> = Vec::new();

    for opt in options {
        let key = opt.key.text;
        match key.as_str() {
            "format" => {
                let Literal::String(fmt_str) = opt.value else {
                    return Err(BqliteError::Plan(
                        "INSERT FROM: `format` option must be a string literal".into(),
                    ));
                };
                format_opt = Some(parse_ingest_format(&fmt_str)?);
            }
            "delimiter" | "header" => {
                resolved_options.push((key, literal_to_property_value(opt.value)));
            }
            other => {
                return Err(BqliteError::Plan(format!(
                    "INSERT FROM: unknown option `{other}` — \
                     known options are `format`, `delimiter`, `header`, and `map`"
                )));
            }
        }
    }

    // Default format = CSV when the option is absent. Inferring from
    // the path extension is a Wave 4 ergonomics improvement.
    let format = format_opt.unwrap_or(IngestFormat::Csv);
    if format != IngestFormat::Csv {
        return Err(BqliteError::Plan(format!(
            "INSERT FROM: `{format:?}` ingest is deferred to Wave 4 (TASK-410); \
             Wave 2 supports `csv` only"
        )));
    }

    // Resolve the column map against the target schema. Every
    // `target` must exist; duplicate targets error. Source-column
    // validation happens at engine-time (TASK-233) against the
    // live file schema.
    let mut column_map: Vec<(String, String)> = Vec::new();
    if let Some(mappings) = map {
        let mut seen_targets: HashSet<String> = HashSet::new();
        for mapping in mappings {
            let source = mapping.source.text;
            let target = mapping.target.text;
            if table.column(&target).is_none() {
                return Err(BqliteError::Plan(format!(
                    "INSERT FROM `{}`: map target `{target}` is not a column on the target table",
                    table.name()
                )));
            }
            if !seen_targets.insert(target.clone()) {
                return Err(BqliteError::Plan(format!(
                    "INSERT FROM `{}`: duplicate map target `{target}`",
                    table.name()
                )));
            }
            column_map.push((source, target));
        }
    }

    Ok(InsertFromDescriptor {
        path,
        format,
        options: resolved_options,
        column_map,
    })
}

/// Parse a `format: '<name>'` option value into an [`IngestFormat`].
fn parse_ingest_format(s: &str) -> Result<IngestFormat> {
    match s.to_ascii_lowercase().as_str() {
        "csv" => Ok(IngestFormat::Csv),
        "jsonl" | "json" => Ok(IngestFormat::JsonL),
        "parquet" => Ok(IngestFormat::Parquet),
        other => Err(BqliteError::Plan(format!(
            "INSERT FROM: unknown format `{other}` — supported formats are csv, jsonl, parquet"
        ))),
    }
}

/// Convert an AST [`Literal`] into a [`PropertyValue`].
///
/// Duration literals collapse to `PropertyValue::Int` (nanoseconds)
/// since the type system tracks durations as `BqlType::Int` per
/// type-system.md §2.2. Timestamp literals preserve their Timestamp
/// type-tag so coercion rules apply correctly at the column boundary.
fn literal_to_property_value(literal: Literal) -> PropertyValue {
    match literal {
        Literal::Null => PropertyValue::Null,
        Literal::Bool(b) => PropertyValue::Bool(b),
        Literal::Int(i) => PropertyValue::Int(i),
        Literal::Float(f) => PropertyValue::Float(f),
        Literal::String(s) => PropertyValue::String(s),
        Literal::Duration(ns) => PropertyValue::Int(ns),
        Literal::Timestamp(ts) => PropertyValue::Timestamp(ts),
    }
}

/// Convert an AST [`AstColumnDef`] into a [`bqlite_core::ColumnDef`].
///
/// Discards the AST's `role` field because the parent caller
/// (CreateTable / AlterTable lowering) handles roles separately.
/// Coerces the `default` literal into a typed `PropertyValue` at
/// plan time so the node carries validation-ready values.
fn ast_column_to_core(ast: AstColumnDef) -> Result<ColumnDef> {
    let default_value = ast.default.map(literal_to_property_value);
    Ok(ColumnDef {
        name: ast.name.text,
        bql_type: ast.data_type,
        nullable: !ast.not_null,
        default_value,
    })
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
                assert_eq!(amount_item.expr.result_type, BqlType::Float);
                assert!(amount_item.expr.nullable);

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
                assert_eq!(expressions[0].expr.result_type, BqlType::Float);
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn select_wildcard_expands_to_non_system_input_columns() {
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
                // Per query-language.md §10, `SELECT *` excludes the
                // implicit `__seq_id` / `__batch_id` system columns.
                assert_eq!(names, vec!["user_id", "ts", "event", "amount", "country"]);
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
    fn select_computed_expression_lowers_through_type_checker() {
        // `SELECT amount * 2 AS doubled` — arithmetic on a Float
        // column. Post TASK-225 the type checker handles this and
        // the project's output schema reflects the computed type.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: false,
                items: vec![SelectItem {
                    kind: SelectItemKind::Expr(Spanned::new(
                        Expr::Binary {
                            op: bqlite_ast::BinaryOp::Multiply,
                            left: Box::new(column_expr("amount")),
                            right: Box::new(Spanned::new(
                                Expr::Literal(Literal::Int(2)),
                                Span::EMPTY,
                            )),
                        },
                        Span::EMPTY,
                    )),
                    alias: Some(Name::synthetic("doubled")),
                    span: Span::EMPTY,
                }],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Project { expressions, .. } => {
                assert_eq!(expressions.len(), 1);
                assert_eq!(expressions[0].output_name, "doubled");
                assert_eq!(expressions[0].expr.result_type, BqlType::Float);
                // amount is nullable, so the result carries that.
                assert!(expressions[0].expr.nullable);
            }
            other => panic!("expected Project, got {other:?}"),
        }
    }

    #[test]
    fn select_type_mismatched_arith_is_rejected_by_type_checker() {
        // `SELECT user_id + 1` — arithmetic on a String column is
        // a type mismatch caught by the expression compiler.
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
                    alias: Some(Name::synthetic("bad")),
                    span: Span::EMPTY,
                }],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("type mismatch")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn where_with_typed_predicate_produces_filter() {
        // `| WHERE event = 'checkout' AND amount > 100` — mirrors
        // the Wave 2 acceptance query's predicate. Verifies that
        // the type checker wires up properly inside fold_stage.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let predicate = Spanned::new(
            Expr::And(vec![
                Spanned::new(
                    Expr::Compare {
                        op: bqlite_ast::CompareOp::Equal,
                        left: Box::new(column_expr("event")),
                        right: Box::new(Spanned::new(
                            Expr::Literal(Literal::String("checkout".into())),
                            Span::EMPTY,
                        )),
                    },
                    Span::EMPTY,
                ),
                Spanned::new(
                    Expr::Compare {
                        op: bqlite_ast::CompareOp::Greater,
                        left: Box::new(column_expr("amount")),
                        right: Box::new(Spanned::new(
                            Expr::Literal(Literal::Int(100)),
                            Span::EMPTY,
                        )),
                    },
                    Span::EMPTY,
                ),
            ]),
            Span::EMPTY,
        );
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Filter {
                predicate,
                output_schema,
                ..
            } => {
                assert_eq!(predicate.result_type, BqlType::Bool);
                // Output schema equals the scan's (identity Filter).
                assert_eq!(output_schema.len(), 7); // 5 declared + 2 system cols
            }
            other => panic!("expected Filter, got {other:?}"),
        }
    }

    #[test]
    fn where_with_non_bool_predicate_is_rejected() {
        // A Filter whose predicate is an Int literal is a type
        // mismatch caught at construction time.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: Spanned::new(Expr::Literal(Literal::Int(7)), Span::EMPTY),
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("Bool")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn select_wildcard_mixed_with_computed_expands_inline() {
        // `SELECT *, amount * 1.1 AS adjusted` — per
        // query-language.md §10 the wildcard expands to every
        // non-system input column and the computed expression
        // is appended in declaration order.
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
                    SelectItem {
                        kind: SelectItemKind::Expr(Spanned::new(
                            Expr::Binary {
                                op: bqlite_ast::BinaryOp::Multiply,
                                left: Box::new(column_expr("amount")),
                                right: Box::new(Spanned::new(
                                    Expr::Literal(Literal::Float(1.1)),
                                    Span::EMPTY,
                                )),
                            },
                            Span::EMPTY,
                        )),
                        alias: Some(Name::synthetic("adjusted")),
                        span: Span::EMPTY,
                    },
                ],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Project { expressions, .. } => {
                let names: Vec<&str> = expressions.iter().map(|i| i.output_name.as_str()).collect();
                assert_eq!(
                    names,
                    vec!["user_id", "ts", "event", "amount", "country", "adjusted"]
                );
                let adjusted = expressions.last().unwrap();
                assert_eq!(adjusted.expr.result_type, BqlType::Float);
            }
            other => panic!("expected Project, got {other:?}"),
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

    // ── DDL: CreateTable ────────────────────────────────────────────────────

    fn ast_column(
        name: &str,
        data_type: BqlType,
        role: bqlite_ast::ColumnRole,
        not_null: bool,
    ) -> bqlite_ast::ColumnDef {
        bqlite_ast::ColumnDef {
            name: Name::synthetic(name),
            data_type,
            role,
            not_null,
            default: None,
            span: Span::EMPTY,
        }
    }

    fn ast_column_with_default(
        name: &str,
        data_type: BqlType,
        role: bqlite_ast::ColumnRole,
        not_null: bool,
        default: Literal,
    ) -> bqlite_ast::ColumnDef {
        bqlite_ast::ColumnDef {
            name: Name::synthetic(name),
            data_type,
            role,
            not_null,
            default: Some(default),
            span: Span::EMPTY,
        }
    }

    fn valid_purchases_create_stmt() -> bqlite_ast::CreateTableStmt {
        bqlite_ast::CreateTableStmt {
            table: Name::synthetic("purchases"),
            columns: vec![
                ast_column(
                    "user_id",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EntityKey,
                    true,
                ),
                ast_column(
                    "ts",
                    BqlType::Timestamp,
                    bqlite_ast::ColumnRole::EventTime,
                    true,
                ),
                ast_column(
                    "event",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EventType,
                    true,
                ),
                ast_column(
                    "amount",
                    BqlType::Float,
                    bqlite_ast::ColumnRole::Regular,
                    false,
                ),
            ],
            span: Span::EMPTY,
        }
    }

    #[test]
    fn create_table_happy_path_destructures_roles_and_columns() {
        let cat = InMemoryCatalog::default();
        let plan =
            lower_statement(Statement::CreateTable(valid_purchases_create_stmt()), &cat).unwrap();
        match plan {
            LogicalPlan::CreateTable {
                name,
                columns,
                entity_key,
                event_time,
                event_type,
                output_schema,
            } => {
                assert_eq!(name, "purchases");
                assert_eq!(columns.len(), 4);
                assert_eq!(entity_key, "user_id");
                assert_eq!(event_time, "ts");
                assert_eq!(event_type, "event");
                assert_eq!(output_schema.len(), 0);
                // Column order and types preserved.
                assert_eq!(columns[0].name, "user_id");
                assert_eq!(columns[0].bql_type, BqlType::String);
                assert!(!columns[0].nullable);
                assert_eq!(columns[3].name, "amount");
                assert_eq!(columns[3].bql_type, BqlType::Float);
                assert!(columns[3].nullable);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_duplicate_table_name() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(Statement::CreateTable(valid_purchases_create_stmt()), &cat)
            .unwrap_err();
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("already exists")),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_missing_entity_key_role() {
        let cat = InMemoryCatalog::default();
        let stmt = bqlite_ast::CreateTableStmt {
            table: Name::synthetic("purchases"),
            columns: vec![
                ast_column(
                    "user_id",
                    BqlType::String,
                    bqlite_ast::ColumnRole::Regular,
                    true,
                ),
                ast_column(
                    "ts",
                    BqlType::Timestamp,
                    bqlite_ast::ColumnRole::EventTime,
                    true,
                ),
                ast_column(
                    "event",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EventType,
                    true,
                ),
            ],
            span: Span::EMPTY,
        };
        let err = lower_statement(Statement::CreateTable(stmt), &cat).unwrap_err();
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("ENTITY KEY")),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_multiple_entity_key_columns() {
        let cat = InMemoryCatalog::default();
        let stmt = bqlite_ast::CreateTableStmt {
            table: Name::synthetic("purchases"),
            columns: vec![
                ast_column(
                    "a",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EntityKey,
                    true,
                ),
                ast_column(
                    "b",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EntityKey,
                    true,
                ),
                ast_column(
                    "ts",
                    BqlType::Timestamp,
                    bqlite_ast::ColumnRole::EventTime,
                    true,
                ),
                ast_column(
                    "event",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EventType,
                    true,
                ),
            ],
            span: Span::EMPTY,
        };
        let err = lower_statement(Statement::CreateTable(stmt), &cat).unwrap_err();
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("multiple ENTITY KEY")),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn create_table_rejects_duplicate_column_name() {
        // TableSchema::new catches the duplicate; our lowering
        // forwards the error unchanged.
        let cat = InMemoryCatalog::default();
        let stmt = bqlite_ast::CreateTableStmt {
            table: Name::synthetic("purchases"),
            columns: vec![
                ast_column(
                    "user_id",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EntityKey,
                    true,
                ),
                ast_column(
                    "ts",
                    BqlType::Timestamp,
                    bqlite_ast::ColumnRole::EventTime,
                    true,
                ),
                ast_column(
                    "event",
                    BqlType::String,
                    bqlite_ast::ColumnRole::EventType,
                    true,
                ),
                ast_column(
                    "user_id",
                    BqlType::Int,
                    bqlite_ast::ColumnRole::Regular,
                    false,
                ),
            ],
            span: Span::EMPTY,
        };
        let err = lower_statement(Statement::CreateTable(stmt), &cat).unwrap_err();
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("duplicate")),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    // ── DDL: DropTable ──────────────────────────────────────────────────────

    #[test]
    fn drop_table_happy_path_emits_drop_node() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(
            Statement::DropTable(bqlite_ast::DropTableStmt {
                table: Name::synthetic("purchases"),
                span: Span::EMPTY,
            }),
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::DropTable {
                name,
                output_schema,
            } => {
                assert_eq!(name, "purchases");
                assert_eq!(output_schema.len(), 0);
            }
            other => panic!("expected DropTable, got {other:?}"),
        }
    }

    #[test]
    fn drop_table_rejects_unknown_table() {
        let cat = InMemoryCatalog::default();
        let err = lower_statement(
            Statement::DropTable(bqlite_ast::DropTableStmt {
                table: Name::synthetic("ghost"),
                span: Span::EMPTY,
            }),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("ghost"));
                assert!(msg.contains("unknown table"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── DDL: AlterTable ADD COLUMN ──────────────────────────────────────────

    fn alter_add_column(table: &str, column: bqlite_ast::ColumnDef) -> Statement {
        Statement::AlterTable(bqlite_ast::AlterTableStmt {
            table: Name::synthetic(table),
            action: bqlite_ast::AlterAction::AddColumn(column),
            span: Span::EMPTY,
        })
    }

    #[test]
    fn alter_add_column_happy_path() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let new_col = ast_column(
            "referrer",
            BqlType::String,
            bqlite_ast::ColumnRole::Regular,
            false,
        );
        let plan = lower_statement(alter_add_column("purchases", new_col), &cat).unwrap();
        match plan {
            LogicalPlan::AlterTableAddColumn {
                name,
                column,
                output_schema,
            } => {
                assert_eq!(name, "purchases");
                assert_eq!(column.name, "referrer");
                assert_eq!(column.bql_type, BqlType::String);
                assert!(column.nullable);
                assert_eq!(output_schema.len(), 0);
            }
            other => panic!("expected AlterTableAddColumn, got {other:?}"),
        }
    }

    #[test]
    fn alter_add_column_not_null_with_default_ok() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let new_col = ast_column_with_default(
            "source",
            BqlType::String,
            bqlite_ast::ColumnRole::Regular,
            true,
            Literal::String("unknown".into()),
        );
        let plan = lower_statement(alter_add_column("purchases", new_col), &cat).unwrap();
        match plan {
            LogicalPlan::AlterTableAddColumn { column, .. } => {
                assert!(!column.nullable);
                assert!(matches!(
                    column.default_value,
                    Some(PropertyValue::String(_))
                ));
            }
            other => panic!("expected AlterTableAddColumn, got {other:?}"),
        }
    }

    #[test]
    fn alter_add_column_not_null_without_default_is_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let new_col = ast_column(
            "source",
            BqlType::String,
            bqlite_ast::ColumnRole::Regular,
            true, // NOT NULL
        );
        let err = lower_statement(alter_add_column("purchases", new_col), &cat).unwrap_err();
        match err {
            BqliteError::Schema(msg) => {
                assert!(msg.contains("NOT NULL"));
                assert!(msg.contains("DEFAULT"));
            }
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn alter_add_column_rejects_role_columns() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let new_col = ast_column(
            "another_key",
            BqlType::String,
            bqlite_ast::ColumnRole::EntityKey,
            true,
        );
        let err = lower_statement(alter_add_column("purchases", new_col), &cat).unwrap_err();
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("frozen")),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn alter_add_column_rejects_duplicate_name() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let new_col = ast_column(
            "amount",
            BqlType::Int,
            bqlite_ast::ColumnRole::Regular,
            false,
        );
        let err = lower_statement(alter_add_column("purchases", new_col), &cat).unwrap_err();
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("already exists")),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn alter_add_column_rejects_unknown_table() {
        let cat = InMemoryCatalog::default();
        let new_col = ast_column("x", BqlType::Int, bqlite_ast::ColumnRole::Regular, false);
        let err = lower_statement(alter_add_column("ghost", new_col), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("ghost")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── DDL: Describe ───────────────────────────────────────────────────────

    #[test]
    fn describe_happy_path_has_fixed_four_column_schema() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(
            Statement::Describe(bqlite_ast::DescribeStmt {
                table: Name::synthetic("purchases"),
                span: Span::EMPTY,
            }),
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::Describe {
                name,
                output_schema,
            } => {
                assert_eq!(name, "purchases");
                let cols: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(cols, vec!["name", "type", "nullable", "role"]);
            }
            other => panic!("expected Describe, got {other:?}"),
        }
    }

    #[test]
    fn describe_rejects_unknown_table() {
        let cat = InMemoryCatalog::default();
        let err = lower_statement(
            Statement::Describe(bqlite_ast::DescribeStmt {
                table: Name::synthetic("ghost"),
                span: Span::EMPTY,
            }),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("ghost")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── DML: Insert VALUES ──────────────────────────────────────────────────

    fn insert_values(table: &str, rows: Vec<Vec<Literal>>) -> Statement {
        Statement::Insert(bqlite_ast::InsertStmt {
            table: Name::synthetic(table),
            body: bqlite_ast::InsertBody::Values(rows),
            span: Span::EMPTY,
        })
    }

    #[test]
    fn insert_values_happy_path_coerces_literals() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        // purchases: user_id String, ts Timestamp, event String,
        // amount Float (nullable), country String (nullable)
        let plan = lower_statement(
            insert_values(
                "purchases",
                vec![
                    vec![
                        Literal::String("u1".into()),
                        Literal::Timestamp(1_700_000_000_000_000_000),
                        Literal::String("view".into()),
                        Literal::Int(12), // Int → Float coercion
                        Literal::String("US".into()),
                    ],
                    vec![
                        Literal::String("u2".into()),
                        Literal::Timestamp(1_700_000_001_000_000_000),
                        Literal::String("checkout".into()),
                        Literal::Float(99.5),
                        Literal::Null, // nullable column, valid
                    ],
                ],
            ),
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::Insert {
                table,
                body,
                output_schema,
            } => {
                assert_eq!(table.name(), "purchases");
                assert_eq!(output_schema.len(), 0);
                match body {
                    InsertLogicalBody::Values(rows) => {
                        assert_eq!(rows.len(), 2);
                        // Row 0: amount was Int, coerced to Float
                        assert!(matches!(rows[0][3], PropertyValue::Float(_)));
                        // Row 1: country was Null (nullable column)
                        assert!(matches!(rows[1][4], PropertyValue::Null));
                    }
                    other => panic!("expected Values body, got {other:?}"),
                }
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_rejects_arity_mismatch() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_values(
                "purchases",
                vec![vec![Literal::String("u1".into()), Literal::Int(0)]],
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("arity"));
                assert!(msg.contains("row 0"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_rejects_null_in_not_null_column() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_values(
                "purchases",
                vec![vec![
                    Literal::Null, // user_id is NOT NULL
                    Literal::Timestamp(1_700_000_000_000_000_000),
                    Literal::String("view".into()),
                    Literal::Float(12.5),
                    Literal::String("US".into()),
                ]],
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("NULL"));
                assert!(msg.contains("user_id"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_rejects_type_mismatch() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        // ts column is Timestamp; passing a Bool is not coercible.
        let err = lower_statement(
            insert_values(
                "purchases",
                vec![vec![
                    Literal::String("u1".into()),
                    Literal::Bool(true), // type mismatch on ts
                    Literal::String("view".into()),
                    Literal::Float(1.0),
                    Literal::String("US".into()),
                ]],
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("cannot coerce")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_rejects_unknown_table() {
        let cat = InMemoryCatalog::default();
        let err = lower_statement(insert_values("ghost", vec![]), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("ghost")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── DML: Insert FROM ────────────────────────────────────────────────────

    fn insert_from(
        table: &str,
        path: &str,
        options: Vec<(&str, Literal)>,
        map: Option<Vec<(&str, &str)>>,
    ) -> Statement {
        Statement::Insert(bqlite_ast::InsertStmt {
            table: Name::synthetic(table),
            body: bqlite_ast::InsertBody::From {
                path: path.to_string(),
                options: options
                    .into_iter()
                    .map(|(k, v)| bqlite_ast::InsertOption {
                        key: Name::synthetic(k),
                        value: v,
                        span: Span::EMPTY,
                    })
                    .collect(),
                map: map.map(|mappings| {
                    mappings
                        .into_iter()
                        .map(|(s, t)| bqlite_ast::ColumnMapping {
                            source: Name::synthetic(s),
                            target: Name::synthetic(t),
                            span: Span::EMPTY,
                        })
                        .collect()
                }),
            },
            span: Span::EMPTY,
        })
    }

    #[test]
    fn insert_from_csv_with_map_happy_path() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(
            insert_from(
                "purchases",
                "data.csv",
                vec![
                    ("format", Literal::String("csv".into())),
                    ("delimiter", Literal::String(",".into())),
                ],
                Some(vec![("uid", "user_id"), ("evt", "event")]),
            ),
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::Insert {
                body: InsertLogicalBody::From(desc),
                ..
            } => {
                assert_eq!(desc.path, "data.csv");
                assert_eq!(desc.format, IngestFormat::Csv);
                assert_eq!(desc.options.len(), 1);
                assert_eq!(desc.options[0].0, "delimiter");
                assert_eq!(
                    desc.column_map,
                    vec![
                        ("uid".to_string(), "user_id".to_string()),
                        ("evt".to_string(), "event".to_string()),
                    ]
                );
            }
            other => panic!("expected Insert::From, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_defaults_to_csv_when_format_option_absent() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan =
            lower_statement(insert_from("purchases", "data.csv", vec![], None), &cat).unwrap();
        match plan {
            LogicalPlan::Insert {
                body: InsertLogicalBody::From(desc),
                ..
            } => assert_eq!(desc.format, IngestFormat::Csv),
            other => panic!("expected Insert::From, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_rejects_unknown_option_key() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_from(
                "purchases",
                "data.csv",
                vec![("mystery", Literal::String("oops".into()))],
                None,
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("unknown option")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_rejects_jsonl_in_wave_2() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_from(
                "purchases",
                "data.jsonl",
                vec![("format", Literal::String("jsonl".into()))],
                None,
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("Wave 4") || msg.contains("TASK-410"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_rejects_non_string_format_literal() {
        // `format: 123` should reject cleanly rather than silently
        // accept a wrong-typed value.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_from(
                "purchases",
                "data.csv",
                vec![("format", Literal::Int(123))],
                None,
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("string literal")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_rejects_unknown_format_name() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_from(
                "purchases",
                "data.bin",
                vec![("format", Literal::String("avro".into()))],
                None,
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("unknown format")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_map_rejects_unknown_target_column() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_from(
                "purchases",
                "data.csv",
                vec![],
                Some(vec![("src_col", "ghost_target")]),
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("ghost_target")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_map_rejects_duplicate_target() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statement(
            insert_from(
                "purchases",
                "data.csv",
                vec![],
                Some(vec![("a", "user_id"), ("b", "user_id")]),
            ),
            &cat,
        )
        .unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("duplicate map target")),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Deferred-statement rejections ───────────────────────────────────────

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
