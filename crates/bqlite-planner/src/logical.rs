//! Logical plan enum + AST → logical lowering (Wave 2 + Wave 3).
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

use bqlite_ast::expr::{Expr, Literal, SortDir};
use bqlite_ast::pattern::{BracketSpec, MatchMode, MatchPattern, MatchWindow, StepEvent};
use bqlite_ast::pipeline::{Pipeline, TimeRange};
use bqlite_ast::{
    AlterAction, AlterTableStmt, ColumnDef as AstColumnDef, ColumnRole, CreateTableStmt,
    DescribeStmt, DropTableStmt, InsertBody, InsertStmt, PipelineStage, SelectItem, SelectItemKind,
    Statement,
};
use bqlite_core::{
    AggFunction, BqlType, BqliteError, Catalog, ColumnDef, OperatorSchema, PropertyValue, Result,
    TableSchema, Timestamp,
};

use crate::demand::{ColumnId, FusableAggregate, StepPropertyRef};
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
        /// from `Pipeline.source.time_range`. Populated by the parser
        /// when the source includes a time-range clause. Never mutated
        /// after construction — the pristine user-specified range is
        /// preserved here and the reader extension is tracked separately
        /// in `reader_backward_ns` / `reader_forward_ns`.
        time_range: Option<TimeRange>,
        /// Nanoseconds to extend the segment-reader start earlier (backward).
        /// Default 0. Added to the resolved `time_range` start when lowering
        /// to a physical plan. No-op when `time_range` is `None`.
        reader_backward_ns: i64,
        /// Nanoseconds to extend the segment-reader end later (forward).
        /// Default 0. Added to the resolved `time_range` end when lowering
        /// to a physical plan. No-op when `time_range` is `None`.
        reader_forward_ns: i64,
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

    // ── Wave 3 variants ────────────────────────────────────────────────────
    /// `| MATCH <pattern>` — NFA-based sequence pattern matching. Wave 3.
    ///
    /// Carries the **AST-level** pattern (`SequencePattern`) — compilation
    /// to `CompiledNfa` happens during logical→physical lowering (TASK-318).
    /// `step_properties` starts empty and is filled by demand analysis
    /// (Pass 4). `fused_downstream` starts `None` and is set by the
    /// match-aggregate fusion optimizer (TASK-320, Pass 6).
    ///
    /// See `docs/design/planner/wave3-lowering.md` §2.1 for the full
    /// lowering specification.
    SequenceMatch {
        /// The validated AST-level pattern. Compiled to `CompiledNfa` in
        /// TASK-318's physical lowering pass.
        pattern: SequencePattern,
        /// First vs. all non-overlapping matches per entity.
        mode: MatchMode,
        /// When `true`, every entity emits exactly one row regardless of
        /// match completion; a synthetic `step_reached` column indicates
        /// progress. Used internally by FUNNEL / RETENTION desugaring.
        emit_all: bool,
        /// Optional time window constraint (nanoseconds or session).
        window: Option<MatchWindowSpec>,
        /// Retention bracket durations — always `None` in Wave 3; populated
        /// by RETENTION desugaring in Wave 4.
        brackets: Option<BracketSpec>,
        /// Per-step, per-column properties demanded by downstream.
        /// Populated by demand analysis (Pass 4); empty at construction.
        step_properties: Vec<StepPropertyRef>,
        /// Fused downstream aggregate specification.
        /// Set by match-aggregate fusion optimizer (TASK-320, Pass 6).
        fused_downstream: Option<FusedDownstream>,
        /// Child plan (the scan / filter feeding this match).
        input: Box<LogicalPlan>,
        /// Output schema — maximum schema at construction time, pruned by
        /// demand analysis. See wave3-lowering.md §2.1.3.
        output_schema: OperatorSchema,
    },

    /// `| STATS <aggregates> [GROUP BY <keys>]` — hash aggregation. Wave 3.
    ///
    /// Output schema is: group-by columns first (in declaration order),
    /// then aggregate result columns. The input schema is **not** passed
    /// through — only group-by keys and aggregate results are visible
    /// downstream (standard SQL aggregate contract).
    ///
    /// See `docs/design/planner/wave3-lowering.md` §2.2 for the lowering
    /// specification.
    Aggregate {
        /// Typed, validated aggregate expressions (function + args + names).
        aggregates: Vec<TypedAggExpr>,
        /// Group-by key expressions paired with output column names.
        group_by: Vec<(TypedExpr, String)>,
        /// Child plan feeding this aggregate.
        input: Box<LogicalPlan>,
        /// Output schema: group-by cols + aggregate cols.
        output_schema: OperatorSchema,
    },

    /// `| ORDER BY <keys>` — pipeline sort. Wave 3.
    ///
    /// Output schema is identical to the input schema: Sort does not add,
    /// remove, or rename columns. `SortOperator` is a pipeline breaker —
    /// it materializes all input before emitting sorted output.
    ///
    /// See `docs/design/planner/wave3-lowering.md` §2.3 and
    /// `docs/design/operators/sort-distinct.md` §3 for details.
    Sort {
        /// Sort keys in priority order (primary, secondary, …).
        /// Each key is a typed expression plus a direction.
        keys: Vec<(TypedExpr, SortDirection)>,
        /// Child plan feeding this sort.
        input: Box<LogicalPlan>,
        /// Identical to `input.output_schema()`.
        output_schema: OperatorSchema,
    },

    /// Deduplication node wrapping a `Project`. Wave 3.
    ///
    /// Lowered from `SELECT DISTINCT` by wrapping the inner `Project`
    /// in a `Distinct` node. `DistinctOperator` deduplicates on **all**
    /// output columns. Output schema is identical to its input's schema.
    ///
    /// See `docs/design/planner/wave3-lowering.md` §2.4 and
    /// `docs/design/operators/sort-distinct.md` §4 for details.
    Distinct {
        /// Child plan (must be a `Project` from SELECT DISTINCT lowering).
        input: Box<LogicalPlan>,
        /// Identical to `input.output_schema()`.
        output_schema: OperatorSchema,
    },

    // ── Wave 4 variants ────────────────────────────────────────────────────
    /// `| SESSIONIZE gap: <dur> [end: <events>]` — session assignment.
    /// Wave 4.
    ///
    /// Annotates each input event with `session_id` and `session_duration`
    /// columns, grouping events into sessions based on inactivity gaps and
    /// optional explicit end events.
    ///
    /// See `docs/design/operators/sessionize.md` and
    /// `docs/design/planner/logical-plan-nodes.md` §5.2.
    Sessionize {
        /// Minimum inactivity gap (nanoseconds) that triggers a new session.
        /// Boundary is exclusive: new session iff delta > gap.
        gap: i64,
        /// Event types that explicitly end a session. Empty = gap-only mode.
        end_events: Vec<String>,
        /// Columns that downstream operators need forwarded through the
        /// session buffer. Populated by demand analysis.
        forwarded_columns: Vec<ColumnId>,
        /// Fused downstream aggregate specification (Wave 5).
        /// Always `None` in v1.
        fused_downstream: Option<FusedDownstream>,
        /// Child plan feeding this sessionize operator.
        input: Box<LogicalPlan>,
        /// Output schema: input columns + `session_id: Int64 NOT NULL` +
        /// `session_duration: Int64 NOT NULL`.
        output_schema: OperatorSchema,
    },

    /// `| FIRST / LAST / NTH` — per-entity event sub-selection. Wave 4.
    ///
    /// Selects a single qualifying event per entity based on position
    /// (first, last, or nth). Parameterized by `EventSelectKind` to
    /// distinguish the three selection modes.
    ///
    /// See `docs/design/operators/event-select-sample.md` Block A and
    /// `docs/design/planner/logical-plan-nodes.md` §5.2.
    EventSelect {
        /// Selection mode: FIRST, LAST, or NTH(n).
        kind: EventSelectKind,
        /// Event types eligible for selection. Length >= 1.
        event_types: Vec<String>,
        /// Optional per-event predicate (from WHERE clause), type-checked
        /// against the input schema. Applied before position selection.
        predicate: Option<TypedExpr>,
        /// Scan-range backward extension for FIRST/NTH. `None` for LAST.
        lookback: Option<i64>,
        /// Columns that downstream operators need forwarded through the
        /// candidate row. Populated by demand analysis.
        forwarded_columns: Vec<ColumnId>,
        /// Fused downstream aggregate specification (Wave 5).
        /// Always `None` in v1.
        fused_downstream: Option<FusedDownstream>,
        /// Child plan feeding this event-select operator.
        input: Box<LogicalPlan>,
        /// Output schema: source-table columns (one row per entity).
        output_schema: OperatorSchema,
    },

    /// `| ATTRIBUTE conversion: <e> touchpoints: <e> window: <d>
    ///   touchpoint_key: <expr>` — multi-touch attribution. Wave 4.
    ///
    /// Finds touchpoint events preceding each conversion event within a
    /// time window and auto-unnests them into flat rows — one row per
    /// `(entity, conversion, matched-touchpoint)` triple.
    ///
    /// See `docs/design/operators/attribute.md` and
    /// `docs/design/planner/logical-plan-nodes.md` §5.2.
    Attribute {
        /// Event type(s) that trigger conversion emission.
        conversion_events: Vec<String>,
        /// Event type(s) eligible as touchpoints.
        touchpoint_events: Vec<String>,
        /// Lookback window in nanoseconds.
        window: i64,
        /// Expression evaluated per qualifying touchpoint; result becomes
        /// the `touchpoint_key` output column. Typed to `String`.
        touchpoint_key: TypedExpr,
        /// Demand-driven forwarded conversion properties.
        forwarded_conversion_columns: Vec<ColumnId>,
        /// Fused downstream aggregate specification (Wave 5).
        /// Always `None` in v1.
        fused_downstream: Option<FusedDownstream>,
        /// Child plan feeding this attribute operator.
        input: Box<LogicalPlan>,
        /// Output schema: `entity_id`, `conversion_ts`,
        /// demand-forwarded conversion properties, `touchpoint_ts?`,
        /// `touchpoint_key?`.
        output_schema: OperatorSchema,
    },

    /// `WHERE col IN QUERY <alias>` / `WHERE col IN (<subquery>)` —
    /// cohort-based entity filtering. Wave 4.
    ///
    /// Materializes the subquery into a hash set and probes the outer
    /// stream row-by-row. Output schema is identical to the outer input
    /// (filter, not transform).
    ///
    /// See `docs/design/language/cohorts-aliases-joins.md` §4 and
    /// `docs/design/planner/logical-plan-nodes.md` §5.2.
    SubqueryFilter {
        /// LHS column expression(s) for the IN check. Length 1 for
        /// single-column cohorts; length N for tuple cohorts.
        columns: Vec<TypedExpr>,
        /// Inner pipeline producing the cohort set.
        subquery: Box<LogicalPlan>,
        /// Outer input stream being filtered.
        input: Box<LogicalPlan>,
        /// Identical to `input.output_schema()`.
        output_schema: OperatorSchema,
    },

    /// `| SAMPLE fraction: <f> [seed: <s>]` — deterministic entity-level
    /// sampling. Wave 4.
    ///
    /// Keeps roughly `fraction` of entities using a deterministic hash of
    /// `entity_id`. Output schema is identical to the input schema.
    ///
    /// See `docs/design/operators/event-select-sample.md` Block B and
    /// `docs/design/planner/logical-plan-nodes.md` §5.2.
    Sample {
        /// Fraction of entities to keep, in `[0.0, 1.0]`.
        fraction: f64,
        /// Optional explicit RNG seed for reproducible sampling.
        /// `None` → engine uses a database-UUID-derived default seed.
        seed: Option<i64>,
        /// Child plan feeding this sample operator.
        input: Box<LogicalPlan>,
        /// Identical to `input.output_schema()`.
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
    /// Format resolved from `WITH (format: '...')`. Currently required;
    /// auto-inference from the path extension is not yet implemented.
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
/// (TASK-410, TASK-449) is a pure engine extension rather than
/// another planner change. Wave 4 lowering accepts `Csv` and
/// `JsonL`; `Parquet` is deferred to TASK-449 and produces a `Plan`
/// error naming the `format: '...'` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IngestFormat {
    Csv,
    JsonL,
    Parquet,
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 3 logical plan support types
// ─────────────────────────────────────────────────────────────────────────────

/// Planner-owned representation of a MATCH pattern.
///
/// Produced by TASK-318's AST→logical lowering from a `MatchPattern`.
/// For Wave 3, this carries the AST-level pattern after validation
/// (event type references, predicate type-checks, variable binding scoping).
/// Compilation to `CompiledNfa` happens later during logical→physical
/// lowering (TASK-318) via `crate::compile::compile_pattern`.
///
/// See `docs/design/planner/wave3-lowering.md` §2.1.1.
#[derive(Debug, Clone, PartialEq)]
pub struct SequencePattern {
    /// The underlying AST pattern, after planner-level validation.
    ///
    /// TASK-318 populates this during AST→logical lowering; TASK-311's
    /// `compile_pattern` converts it to a `CompiledNfa` during physical
    /// lowering.
    pub inner: MatchPattern,
}

/// Optional time window constraint on a MATCH pattern.
///
/// See `docs/design/planner/wave3-lowering.md` §2.1.1 (MatchWindowSpec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatchWindowSpec {
    /// `WITHIN <duration>` — nanoseconds. All steps in the pattern must
    /// complete within this duration from the first matched event.
    Duration(i64),
    /// `WITHIN SESSION` — NFA resets on `session_id` column transitions.
    /// Requires an upstream SESSIONIZE operator (Wave 4). A Wave 3 query
    /// using this variant will fail at schema validation because the
    /// `session_id` column is not present without SESSIONIZE.
    Session,
}

/// Fused downstream aggregate specification (TASK-320 Pass 6).
///
/// Set on `LogicalPlan::SequenceMatch` by the match-aggregate fusion
/// optimizer (TASK-320) when it detects a fusable Aggregate immediately
/// downstream (optionally separated by a Filter).
///
/// Empty until Pass 6 runs; `None` on every newly constructed node.
#[derive(Debug, Clone, PartialEq)]
pub struct FusedDownstream {
    /// The aggregate specification extracted by the fusion optimizer.
    pub aggregate: FusableAggregate,
}

/// A validated, type-checked aggregate expression.
///
/// Produced by TASK-318's `PipelineStage::Stats` lowering for each
/// `AggItem` in the STATS clause. Carries resolved function, type-checked
/// argument expressions, output column name, and inferred output type.
///
/// See `docs/design/planner/wave3-lowering.md` §2.2.1 for the full
/// lowering rules.
#[derive(Debug, Clone, PartialEq)]
pub struct TypedAggExpr {
    /// The resolved aggregate function.
    pub function: AggFunction,
    /// Type-checked argument expressions. Empty for `COUNT(*)`.
    /// One element for all other aggregate functions.
    pub args: Vec<TypedExpr>,
    /// Whether this is a `COUNT_DISTINCT`-style distinct aggregate.
    /// Reserved for v2; always `false` in Wave 3.
    pub distinct: bool,
    /// Output column name — from the BQL alias (query-language.md §7.1).
    pub output_name: String,
    /// Output BQL type derived from `AggFunction::output_type`.
    pub output_type: BqlType,
    /// Whether this aggregate output is nullable (false for COUNT variants).
    pub nullable: bool,
    // TODO(TASK-318): add `span: Span` for source-location tracking in type
    // error messages. Deferred because TASK-317 is scaffolding only — TASK-318
    // owns the lowering that constructs TypedAggExpr at parse sites.
}

/// Sort direction for `LogicalPlan::Sort` keys.
///
/// Shared between the logical `Sort` node and the physical `SortPhysical`
/// descriptor. The null-ordering convention is fixed: NULLs last in ASC,
/// NULLs first in DESC — see `docs/design/operators/sort-distinct.md` §3.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SortDirection {
    /// Ascending order — NULLs sort last (query-language.md §15).
    Asc,
    /// Descending order — NULLs sort first (query-language.md §15).
    Desc,
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 logical plan support types
// ─────────────────────────────────────────────────────────────────────────────

/// Selection mode for `LogicalPlan::EventSelect`.
///
/// Planner-level mirror of the AST's `EventSelectKind`. The AST uses
/// `Nth(u64)` because the parser doesn't validate the range; the
/// planner narrows to `u32` during AST→logical lowering (TASK-425)
/// after validating `n >= 1`.
///
/// See `docs/design/operators/event-select-sample.md` §4.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventSelectKind {
    /// Select the first qualifying event per entity.
    First,
    /// Select the last qualifying event per entity.
    Last,
    /// Select the nth qualifying event per entity (1-indexed).
    /// Invariant: `n >= 1`, enforced at plan construction time.
    Nth(u32),
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
            | LogicalPlan::Explain { output_schema, .. }
            // Wave 3 variants — all carry an output_schema field.
            | LogicalPlan::SequenceMatch { output_schema, .. }
            | LogicalPlan::Aggregate { output_schema, .. }
            | LogicalPlan::Sort { output_schema, .. }
            | LogicalPlan::Distinct { output_schema, .. }
            // Wave 4 variants.
            | LogicalPlan::Sessionize { output_schema, .. }
            | LogicalPlan::EventSelect { output_schema, .. }
            | LogicalPlan::Attribute { output_schema, .. }
            | LogicalPlan::SubqueryFilter { output_schema, .. }
            | LogicalPlan::Sample { output_schema, .. } => output_schema,
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
            reader_backward_ns: 0,
            reader_forward_ns: 0,
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

    /// Extend the segment-reader window backwards (earlier start) by `ns`
    /// nanoseconds, per planner-pipeline.md §4.4.
    ///
    /// Walks through `Filter`/`Project`/`Limit` wrappers to reach the
    /// deepest `Scan`. If the Scan has no time range (`None`), the method
    /// is a no-op — there is nothing to extend (unbounded scans already
    /// cover all time). The pristine `time_range` field is never mutated;
    /// only `reader_backward_ns` is updated.
    #[allow(dead_code)]
    pub(crate) fn extend_scan_reader_backward(&mut self, ns: i64) -> Result<()> {
        match self {
            LogicalPlan::Scan {
                time_range: Some(_),
                reader_backward_ns,
                ..
            } => {
                *reader_backward_ns = reader_backward_ns.saturating_add(ns);
                Ok(())
            }
            LogicalPlan::Scan { .. } => Ok(()),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. } => input.extend_scan_reader_backward(ns),
            _ => Ok(()),
        }
    }

    /// Extend the segment-reader window forwards (later end) by `ns`
    /// nanoseconds, per planner-pipeline.md §4.4.
    ///
    /// Walks through `Filter`/`Project`/`Limit` wrappers to reach the
    /// deepest `Scan`. If the Scan has no time range (`None`), the method
    /// is a no-op. The pristine `time_range` field is never mutated;
    /// only `reader_forward_ns` is updated.
    pub(crate) fn extend_scan_reader_forward(&mut self, ns: i64) -> Result<()> {
        match self {
            LogicalPlan::Scan {
                time_range: Some(_),
                reader_forward_ns,
                ..
            } => {
                *reader_forward_ns = reader_forward_ns.saturating_add(ns);
                Ok(())
            }
            LogicalPlan::Scan { .. } => Ok(()),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. } => input.extend_scan_reader_forward(ns),
            _ => Ok(()),
        }
    }
}

#[allow(dead_code)]
fn extend_between_end(end: &str, extension_ns: i64) -> Result<String> {
    if extension_ns <= 0 {
        return Ok(end.to_string());
    }

    let parsed_end = parse_time_range_timestamp(end, "BETWEEN end")?;
    let widened_end = parsed_end
        .checked_add_nanos(extension_ns)
        .unwrap_or(Timestamp::MAX_VALID);

    match PropertyValue::Timestamp(widened_end.as_nanos()).coerce_to(&BqlType::String) {
        Some(PropertyValue::String(s)) => Ok(s),
        _ => Err(BqliteError::Plan(format!(
            "failed to format widened BETWEEN end timestamp `{end}`"
        ))),
    }
}

#[allow(dead_code)]
fn parse_time_range_timestamp(raw: &str, context: &str) -> Result<Timestamp> {
    match PropertyValue::String(raw.to_string()).coerce_to(&BqlType::Timestamp) {
        Some(PropertyValue::Timestamp(ns)) => Ok(Timestamp::from_nanos(ns)),
        _ => Err(BqliteError::Plan(format!(
            "{context} `{raw}` is not a valid RFC 3339 timestamp"
        ))),
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
    let mut plan =
        LogicalPlan::scan_with_time_range(table_schema.clone(), pipeline.source.time_range);

    // Function registry for expression-level type checking. Wave 2
    // ships the built-in set (`like`, `regex`); later waves extend
    // via the registry's `register` API.
    let registry = FunctionRegistry::with_builtins();

    // Fold pipeline stages in order. Each stage wraps `plan` in a
    // new logical node whose input is the previous `plan`.
    for stage in pipeline.stages {
        plan = fold_stage(stage, plan, &registry, catalog, &table_schema)?;
    }

    Ok(plan)
}

/// Fold a single AST pipeline stage into the accumulated plan.
fn fold_stage(
    stage: PipelineStage,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
    catalog: &dyn Catalog,
    source_table: &TableSchema,
) -> Result<LogicalPlan> {
    match stage {
        PipelineStage::Where { predicate, .. } => {
            let typed = TypedExpr::from_ast(&predicate, acc.output_schema(), registry)?;
            LogicalPlan::filter(typed, acc)
        }

        PipelineStage::Select {
            distinct, items, ..
        } => {
            let project = lower_select(items, acc, registry)?;
            if distinct {
                // SELECT DISTINCT lowers to Distinct(Project(...))
                // per wave3-lowering.md §2.4.
                let output_schema = project.output_schema().clone();
                Ok(LogicalPlan::Distinct {
                    input: Box::new(project),
                    output_schema,
                })
            } else {
                Ok(project)
            }
        }

        PipelineStage::Limit { count, .. } => Ok(LogicalPlan::limit(count, acc)),

        // ── Wave 3 stages ────────────────────────────────────────────
        PipelineStage::Match { pattern, .. } => {
            lower_match(pattern, acc, registry, catalog, source_table)
        }

        PipelineStage::Stats {
            aggregates,
            group_by,
            ..
        } => lower_stats(aggregates, group_by, acc, registry),

        PipelineStage::OrderBy { items, .. } => lower_order_by(items, acc, registry),

        // ── Wave 4 stages ────────────────────────────────────────────
        PipelineStage::Sessionize(args) => lower_sessionize(args, acc, source_table),

        PipelineStage::Sample(args) => lower_sample(args, acc),

        // ── Wave 3 desugaring ─────────────────────────────────────────
        // FUNNEL is syntactic sugar that expands into a MATCH (EMIT ALL)
        // followed by a STATS stage. Desugaring is deferred to the
        // planner (not the parser) because the aggregate output names are
        // derived from the step list — a schema-aware operation.
        // See opt::desugar_funnel and planner-pipeline.md §4.3.
        PipelineStage::Funnel(f) => {
            let (match_stage, stats_stage) = crate::opt::desugar_funnel(f)?;
            // Fold the two desugared stages in order (MATCH first, then STATS).
            let after_match = fold_stage(match_stage, acc, registry, catalog, source_table)?;
            fold_stage(stats_stage, after_match, registry, catalog, source_table)
        }

        // Everything else is a later-wave shape.
        other => Err(BqliteError::Plan(format!(
            "pipeline stage `{}` is not yet supported — see TASKS.md for the implementation wave",
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
// Wave 3 MATCH lowering — wave3-lowering.md §2.1
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| MATCH <pattern>` into a `SequenceMatch` logical node.
///
/// Validates event-type references against the catalog, type-checks step
/// predicates, converts the AST `MatchPattern` into a planner-owned
/// `SequencePattern`, and builds the output schema per §2.1.3.
///
/// Step-property columns (`s.plan`, `p.amount`) are eagerly added to the
/// output schema for all named steps so that downstream type-checking can
/// resolve qualified references. The demand propagation pass (Pass 4) later
/// prunes unused columns.
fn lower_match(
    pattern: MatchPattern,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
    _catalog: &dyn Catalog,
    source_table: &TableSchema,
) -> Result<LogicalPlan> {
    if pattern.steps.is_empty() {
        return Err(BqliteError::Plan(
            "MATCH pattern must have at least one step".into(),
        ));
    }

    let input_schema = acc.output_schema();

    // Resolve role column names from the source table schema, not hardcoded.
    let entity_key_name = source_table.entity_key_column().name.clone();
    let timestamp_name = source_table.timestamp_column().name.clone();
    let event_type_name_col = source_table.event_type_column().name.clone();

    // Validate the event_type column exists on the input schema (required for
    // the NFA to dispatch events to the correct step).
    if input_schema.column(&event_type_name_col).is_none() {
        return Err(BqliteError::Plan(format!(
            "MATCH requires the input to have an event type column (`{event_type_name_col}`)"
        )));
    }

    // Validate that all $var references in step predicates appear only in
    // equality comparisons. This catches misuse (e.g., `$var > 100`) at
    // logical lowering, before physical planning.
    crate::compile::validate_variable_usage(&pattern)?;

    // Pre-collect variable binding columns from step predicates BEFORE
    // type-checking. Step predicates may reference `$var` variables that
    // are not in the input schema — they are created by the MATCH operator.
    // We must build an augmented schema that includes these variable columns
    // so that `TypedExpr::from_ast` can resolve `$var` references.
    let var_columns = collect_variable_columns(&pattern, input_schema);
    let owned_step_schema;
    let step_schema: &OperatorSchema = if var_columns.is_empty() {
        input_schema
    } else {
        let mut cols = input_schema.columns().to_vec();
        cols.extend(var_columns.iter().cloned());
        owned_step_schema = OperatorSchema::new(cols)?;
        &owned_step_schema
    };

    // Collect step name → event type mapping. Validate event types against
    // the catalog and check for duplicate step names.
    let mut step_names: Vec<(String, String)> = Vec::new(); // (step_name, event_type)
    let mut seen_step_names: HashSet<String> = HashSet::new();

    for step in &pattern.steps {
        // Resolve the event type name. For Wave 3, only Single events are
        // validated against the catalog schema (alternations and cross-table
        // references are validated structurally by the pattern compiler).
        let event_type_name = match &step.event {
            StepEvent::Single(event_ref) => event_ref.event.text.clone(),
            StepEvent::Alternation(refs) => {
                // Use the first event type for step-property resolution.
                // (All alternatives must have the same step name if named.)
                refs[0].event.text.clone()
            }
        };

        // Type-check step predicates against the augmented schema that
        // includes `$var` columns, so `column = $var` resolves correctly.
        // Full variable-usage validation (equality-only, no bare $var
        // outside comparisons) is enforced by the pattern compiler's
        // `collect_variable_comparisons` in compile.rs.
        if let Some(pred) = &step.predicate {
            let typed = TypedExpr::from_ast(pred, step_schema, registry)?;
            if typed.result_type != BqlType::Bool {
                return Err(BqliteError::Plan(format!(
                    "MATCH step predicate must have type `Bool`, got `{}`",
                    typed.result_type
                )));
            }
        }

        // Record named steps for step-property resolution.
        if let Some(name) = &step.name {
            if !seen_step_names.insert(name.text.clone()) {
                return Err(BqliteError::Plan(format!(
                    "MATCH: duplicate step name `{}`",
                    name.text
                )));
            }
            step_names.push((name.text.clone(), event_type_name));
        }
    }

    // Determine match mode and emit_all per §2.1.2.
    let mode = match pattern.mode {
        MatchMode::EmitAll => MatchMode::First,
        other => other,
    };
    let emit_all = pattern.emit_all || pattern.mode == MatchMode::EmitAll;

    // Convert AST MatchWindow to planner MatchWindowSpec per §2.1.1.
    let window = pattern.window.map(|w| match w {
        MatchWindow::Within(ns) => MatchWindowSpec::Duration(ns),
        MatchWindow::WithinSession => MatchWindowSpec::Session,
    });

    // Build the output schema per §2.1.3.
    let mut output_columns: Vec<ColumnDef> = Vec::new();

    // 1. entity_id is always present — use the table's entity key column.
    if let Some((_, col_def)) = input_schema.column(&entity_key_name) {
        output_columns.push(ColumnDef {
            name: "entity_id".to_string(),
            bql_type: col_def.bql_type.clone(),
            nullable: false,
            default_value: None,
        });
    } else {
        return Err(BqliteError::Plan(format!(
            "MATCH requires the input to have an entity key column (`{entity_key_name}`)"
        )));
    }

    // 2. Variable binding columns — already collected above for the
    // augmented step-predicate schema. Reuse them here.
    output_columns.extend(var_columns);

    // 3. step_reached column when emit_all is true.
    if emit_all {
        output_columns.push(ColumnDef {
            name: "step_reached".to_string(),
            bql_type: BqlType::Int,
            nullable: false,
            default_value: None,
        });
    }

    // 4. match_duration and match_events: demand-dependent columns added to
    // the maximum schema per §2.1.3 item 4. Pruned by demand analysis if unused.
    output_columns.push(ColumnDef {
        name: "match_duration".to_string(),
        bql_type: BqlType::Int,
        nullable: true,
        default_value: None,
    });
    output_columns.push(ColumnDef {
        name: "match_events".to_string(),
        bql_type: BqlType::Map(Box::new(BqlType::Timestamp)),
        nullable: true,
        default_value: None,
    });

    // 5. Step-property columns for all named steps.
    // Eagerly added so downstream type-checking can resolve qualified references
    // (e.g., `s.plan`). Demand analysis prunes unused ones.
    // NOTE: deviates from §2.1.3 item 5 which says step properties are NOT added
    // at construction time. We add them eagerly so TypedExpr::from_ast can resolve
    // qualified references in downstream expressions without a second pass.
    let role_columns: HashSet<&str> = [
        entity_key_name.as_str(),
        timestamp_name.as_str(),
        event_type_name_col.as_str(),
    ]
    .into_iter()
    .collect();

    for (step_name, _) in &step_names {
        for col in input_schema.columns() {
            if col.is_system() || role_columns.contains(col.name.as_str()) {
                continue;
            }
            let qualified_name = format!("{}.{}", step_name, col.name);
            output_columns.push(ColumnDef {
                name: qualified_name,
                bql_type: col.bql_type.clone(),
                nullable: true, // nullable because entity may not reach this step
                default_value: None,
            });
        }
        // Also include timestamp as a step property for time-based operations
        // (e.g., QUANTIZE(s.ts, 1d) in a GROUP BY after MATCH).
        let ts_name = format!("{}.{}", step_name, timestamp_name);
        output_columns.push(ColumnDef {
            name: ts_name,
            bql_type: BqlType::Timestamp,
            nullable: true,
            default_value: None,
        });
    }

    let output_schema = OperatorSchema::new(output_columns)?;

    // Carry brackets through from the AST (always None in Wave 3).
    let brackets = pattern.brackets.clone();

    // ── Scan time-range extension (planner-pipeline.md §4.4) ─────────
    // Extend the scan's upper time bound so events beyond the user's
    // stated range can complete matches started near the boundary.
    //
    // Three rules:
    //   1. WITHIN window only → extend by window_ns
    //   2. BRACKETS only     → extend by max(bracket durations)
    //   3. Both              → extend by max(window_ns, max_bracket)
    //
    // BETWEEN ranges are widened by extending their inclusive end
    // timestamp. LAST ranges still widen through the stored duration.
    let mut acc = acc;
    let window_ns = match &window {
        Some(MatchWindowSpec::Duration(ns)) => *ns,
        _ => 0,
    };
    let max_bracket = brackets
        .as_ref()
        .and_then(|b| b.durations.iter().copied().max())
        .unwrap_or(0);
    let extension = window_ns.max(max_bracket);
    if extension > 0 {
        acc.extend_scan_reader_forward(extension)?;
    }

    Ok(LogicalPlan::SequenceMatch {
        pattern: SequencePattern { inner: pattern },
        mode,
        emit_all,
        window,
        brackets,
        step_properties: Vec::new(), // filled by demand analysis (Pass 4)
        fused_downstream: None,      // filled by fusion optimizer (TASK-320)
        input: Box::new(acc),
        output_schema,
    })
}

/// Collect `$var` binding declarations from the MATCH pattern's step
/// predicates and return them as `ColumnDef` entries.
///
/// Uses a two-pass approach: first collects type-refined bindings from
/// `Compare(Variable, Column)` patterns, then fills in remaining variables
/// with a default `String` type. This ensures variables compared against
/// typed columns get the correct type regardless of expression tree order.
///
/// Returns columns named `$<name>` in first-occurrence order matching
/// the pattern compiler's `resolve_variable_bindings` so that binding key
/// indices align with output schema column positions.
///
/// Full variable type inference and validation is deferred to the pattern
/// compiler (TASK-311's `compile_pattern`).
fn collect_variable_columns(
    pattern: &MatchPattern,
    input_schema: &OperatorSchema,
) -> Vec<ColumnDef> {
    // Pass 1: collect type-refined bindings from Compare(Variable, Column).
    let mut refined: std::collections::HashMap<String, BqlType> = std::collections::HashMap::new();
    for step in &pattern.steps {
        if let Some(pred) = &step.predicate {
            collect_refined_variable_types(&pred.node, input_schema, &mut refined);
        }
    }

    // Pass 2: collect all variable names in first-occurrence order.
    // The order must match the pattern compiler's `resolve_variable_bindings`
    // so that binding key indices align with output schema column positions.
    let mut seen_set: HashSet<String> = HashSet::new();
    let mut var_list: Vec<String> = Vec::new();
    for step in &pattern.steps {
        if let Some(pred) = &step.predicate {
            collect_all_variables_ordered(&pred.node, &mut seen_set, &mut var_list);
        }
    }

    var_list
        .into_iter()
        .map(|var_name| {
            let bql_type = refined.get(&var_name).cloned().unwrap_or(BqlType::String);
            ColumnDef {
                name: var_name,
                bql_type,
                nullable: false, // bindings are non-nullable per §2.1.3
                default_value: None,
            }
        })
        .collect()
}

/// Pass 1: Scan expressions for `Compare($var, column)` patterns and record
/// the column's type as the variable's refined type.
fn collect_refined_variable_types(
    expr: &Expr,
    input_schema: &OperatorSchema,
    refined: &mut std::collections::HashMap<String, BqlType>,
) {
    match expr {
        Expr::Compare { left, right, .. } => {
            if let (Expr::Variable(var_name), Expr::Column(col_name))
            | (Expr::Column(col_name), Expr::Variable(var_name)) = (&left.node, &right.node)
            {
                let key = format!("${}", var_name.text);
                if let std::collections::hash_map::Entry::Vacant(e) = refined.entry(key) {
                    if let Some((_, col)) = input_schema.column(&col_name.text) {
                        e.insert(col.bql_type.clone());
                    }
                }
            }
            // Recurse into sub-expressions.
            collect_refined_variable_types(&left.node, input_schema, refined);
            collect_refined_variable_types(&right.node, input_schema, refined);
        }
        Expr::Binary { left, right, .. } => {
            collect_refined_variable_types(&left.node, input_schema, refined);
            collect_refined_variable_types(&right.node, input_schema, refined);
        }
        Expr::And(exprs) | Expr::Or(exprs) => {
            for e in exprs {
                collect_refined_variable_types(&e.node, input_schema, refined);
            }
        }
        Expr::Not(inner) | Expr::Paren(inner) => {
            collect_refined_variable_types(&inner.node, input_schema, refined);
        }
        Expr::Unary { operand, .. } => {
            collect_refined_variable_types(&operand.node, input_schema, refined);
        }
        Expr::IsNull { expr: inner, .. } | Expr::Cast { expr: inner, .. } => {
            collect_refined_variable_types(&inner.node, input_schema, refined);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_refined_variable_types(&arg.node, input_schema, refined);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_refined_variable_types(&expr.node, input_schema, refined);
            collect_refined_variable_types(&low.node, input_schema, refined);
            collect_refined_variable_types(&high.node, input_schema, refined);
        }
        Expr::In { lhs, rhs, .. } => {
            for e in lhs {
                collect_refined_variable_types(&e.node, input_schema, refined);
            }
            if let bqlite_ast::expr::InRhs::List(exprs) = rhs {
                for e in exprs {
                    collect_refined_variable_types(&e.node, input_schema, refined);
                }
            }
        }
        Expr::Case {
            arms, else_expr, ..
        } => {
            for arm in arms {
                collect_refined_variable_types(&arm.condition.node, input_schema, refined);
                collect_refined_variable_types(&arm.value.node, input_schema, refined);
            }
            if let Some(else_e) = else_expr {
                collect_refined_variable_types(&else_e.node, input_schema, refined);
            }
        }
        _ => {}
    }
}

/// Pass 2: Collect all `$var` names from an expression tree in first-occurrence
/// order. Uses `seen` for dedup and `ordered` for output order.
fn collect_all_variables_ordered(
    expr: &Expr,
    seen: &mut HashSet<String>,
    ordered: &mut Vec<String>,
) {
    match expr {
        Expr::Variable(name) => {
            let key = format!("${}", name.text);
            if seen.insert(key.clone()) {
                ordered.push(key);
            }
        }
        Expr::Compare { left, right, .. } | Expr::Binary { left, right, .. } => {
            collect_all_variables_ordered(&left.node, seen, ordered);
            collect_all_variables_ordered(&right.node, seen, ordered);
        }
        Expr::And(exprs) | Expr::Or(exprs) => {
            for e in exprs {
                collect_all_variables_ordered(&e.node, seen, ordered);
            }
        }
        Expr::Not(inner) | Expr::Paren(inner) => {
            collect_all_variables_ordered(&inner.node, seen, ordered);
        }
        Expr::Unary { operand, .. } => {
            collect_all_variables_ordered(&operand.node, seen, ordered);
        }
        Expr::IsNull { expr: inner, .. } | Expr::Cast { expr: inner, .. } => {
            collect_all_variables_ordered(&inner.node, seen, ordered);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_all_variables_ordered(&arg.node, seen, ordered);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_all_variables_ordered(&expr.node, seen, ordered);
            collect_all_variables_ordered(&low.node, seen, ordered);
            collect_all_variables_ordered(&high.node, seen, ordered);
        }
        Expr::In { lhs, rhs, .. } => {
            for e in lhs {
                collect_all_variables_ordered(&e.node, seen, ordered);
            }
            if let bqlite_ast::expr::InRhs::List(exprs) = rhs {
                for e in exprs {
                    collect_all_variables_ordered(&e.node, seen, ordered);
                }
            }
        }
        Expr::Case {
            arms, else_expr, ..
        } => {
            for arm in arms {
                collect_all_variables_ordered(&arm.condition.node, seen, ordered);
                collect_all_variables_ordered(&arm.value.node, seen, ordered);
            }
            if let Some(else_e) = else_expr {
                collect_all_variables_ordered(&else_e.node, seen, ordered);
            }
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 3 STATS lowering — wave3-lowering.md §2.2
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| STATS <aggregates> [GROUP BY <keys>]` into an `Aggregate` logical node.
///
/// Type-checks each aggregate expression and group-by key against the input
/// schema. Builds the output schema with group-by columns first, then
/// aggregate result columns, per §2.2.2.
fn lower_stats(
    agg_items: Vec<bqlite_ast::AggItem>,
    group_items: Vec<bqlite_ast::GroupItem>,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    if agg_items.is_empty() {
        return Err(BqliteError::Plan(
            "STATS must have at least one aggregate expression".into(),
        ));
    }

    let input_schema = acc.output_schema().clone();

    // Type-check group-by expressions.
    let mut group_by: Vec<(TypedExpr, String)> = Vec::with_capacity(group_items.len());
    for (idx, item) in group_items.into_iter().enumerate() {
        let typed = TypedExpr::from_ast(&item.expr, &input_schema, registry)?;
        let output_name = if let Some(alias) = item.alias {
            alias.text
        } else {
            // Generate name from expression.
            match &item.expr.node {
                Expr::Column(name) => name.text.clone(),
                Expr::Qualified { table, column } => {
                    format!("{}.{}", table.text, column.text)
                }
                _ => format!("group_{idx}"),
            }
        };
        group_by.push((typed, output_name));
    }

    // Type-check and resolve aggregate expressions per §2.2.1.
    let mut aggregates: Vec<TypedAggExpr> = Vec::with_capacity(agg_items.len());
    for item in agg_items {
        let typed_agg = resolve_agg_item(item, &input_schema, registry)?;
        aggregates.push(typed_agg);
    }

    // Check for output name uniqueness per §5.4.
    let mut output_names: HashSet<String> = HashSet::new();
    for (_, name) in &group_by {
        if !output_names.insert(name.clone()) {
            return Err(BqliteError::Plan(format!(
                "STATS: duplicate output name `{name}` in GROUP BY"
            )));
        }
    }
    for agg in &aggregates {
        if !output_names.insert(agg.output_name.clone()) {
            return Err(BqliteError::Plan(format!(
                "STATS: duplicate output name `{}` — each aggregate and group-by key \
                 must have a distinct name",
                agg.output_name
            )));
        }
    }

    // Build output schema: group-by columns first, then aggregate results.
    let mut output_columns: Vec<ColumnDef> = Vec::new();
    for (expr, name) in &group_by {
        output_columns.push(ColumnDef {
            name: name.clone(),
            bql_type: expr.result_type.clone(),
            nullable: expr.nullable,
            default_value: None,
        });
    }
    for agg in &aggregates {
        output_columns.push(ColumnDef {
            name: agg.output_name.clone(),
            bql_type: agg.output_type.clone(),
            nullable: agg.nullable,
            default_value: None,
        });
    }
    let output_schema = OperatorSchema::new(output_columns)?;

    Ok(LogicalPlan::Aggregate {
        aggregates,
        group_by,
        input: Box::new(acc),
        output_schema,
    })
}

/// Resolve a single `AggItem` into a `TypedAggExpr`.
///
/// Maps the function name to an `AggFunction` variant, validates argument
/// counts and types, and infers the output type per §2.2.1 table.
fn resolve_agg_item(
    item: bqlite_ast::AggItem,
    input_schema: &OperatorSchema,
    registry: &FunctionRegistry,
) -> Result<TypedAggExpr> {
    let function_name = item.function.text.to_lowercase();
    let output_name = item.alias.text.clone();

    // Match function name to AggFunction variant.
    let (function, requires_arg) = match function_name.as_str() {
        "count" if item.args.is_empty() => (AggFunction::Count, false),
        "count" => (AggFunction::CountColumn, true),
        "count_distinct" => (AggFunction::CountDistinct, true),
        "sum" => (AggFunction::Sum, true),
        "avg" => (AggFunction::Avg, true),
        "min" => (AggFunction::Min, true),
        "max" => (AggFunction::Max, true),
        "p50" => (AggFunction::P50, true),
        "p90" => (AggFunction::P90, true),
        "p95" => (AggFunction::P95, true),
        "p99" => (AggFunction::P99, true),
        unknown => {
            return Err(BqliteError::Plan(format!(
                "STATS: unknown aggregate function `{unknown}` — \
                 supported functions: count, sum, avg, min, max, count_distinct, p50, p90, p95, p99"
            )));
        }
    };

    // Validate argument count.
    if requires_arg && item.args.is_empty() {
        return Err(BqliteError::Plan(format!(
            "STATS: aggregate function `{function_name}` requires exactly 1 argument"
        )));
    }
    if !requires_arg && !item.args.is_empty() {
        // COUNT(*) with args => this is COUNT(col), already handled above
        // This shouldn't happen given the matching above, but guard it.
    }
    if item.args.len() > 1 {
        return Err(BqliteError::Plan(format!(
            "STATS: aggregate function `{function_name}` takes at most 1 argument, got {}",
            item.args.len()
        )));
    }

    // Type-check argument expressions.
    let mut args: Vec<TypedExpr> = Vec::new();
    for arg_expr in &item.args {
        let typed = TypedExpr::from_ast(arg_expr, input_schema, registry)?;
        args.push(typed);
    }

    // Determine the output type from the function and argument type.
    let arg_type = args.first().map(|a| &a.result_type);
    let output_type = function
        .output_type(arg_type)
        .ok_or_else(|| {
            let arg_type_str = arg_type
                .map(|t| format!("`{t}`"))
                .unwrap_or_else(|| "none".into());
            BqliteError::Plan(format!(
                "STATS: aggregate function `{function_name}` does not accept argument type {arg_type_str}"
            ))
        })?;
    let nullable = function.output_nullable();

    Ok(TypedAggExpr {
        function,
        args,
        distinct: false,
        output_name,
        output_type,
        nullable,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 3 ORDER BY lowering — wave3-lowering.md §2.3
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| ORDER BY <items>` into a `Sort` logical node.
///
/// Type-checks each sort key expression against the input schema and
/// validates that the expression type is orderable (all BQL types except
/// `Map` are orderable). Output schema is identical to input.
fn lower_order_by(
    items: Vec<bqlite_ast::expr::OrderItem>,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    if items.is_empty() {
        return Err(BqliteError::Plan(
            "ORDER BY must have at least one sort key".into(),
        ));
    }

    let input_schema = acc.output_schema().clone();
    let mut keys: Vec<(TypedExpr, SortDirection)> = Vec::with_capacity(items.len());

    for item in items {
        let typed = TypedExpr::from_ast(&item.expr, &input_schema, registry)?;

        // Validate orderability per §5.5.
        if matches!(typed.result_type, BqlType::Map(_)) {
            return Err(BqliteError::Plan(
                "ORDER BY: sort key has type `Map` which is not orderable — \
                 only Bool, Int, Float, String, Timestamp, and List types can be sorted"
                    .into(),
            ));
        }

        let direction = match item.direction {
            SortDir::Asc => SortDirection::Asc,
            SortDir::Desc => SortDirection::Desc,
        };
        keys.push((typed, direction));
    }

    let output_schema = input_schema;

    Ok(LogicalPlan::Sort {
        keys,
        input: Box::new(acc),
        output_schema,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 SESSIONIZE lowering — operators/sessionize.md §4–§6
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| SESSIONIZE gap: <d> [end: <events>]` into a `Sessionize` logical
/// node.
///
/// Validations:
/// - `gap > 0` (per sessionize.md §5.1 — boundary is exclusive, `delta > gap`).
/// - `end` event-type list contains no duplicates (parser guarantees this but
///   we guard defensively; sessionize.md §5.4).
/// - The input must expose the source table's event-type column so the operator
///   can dispatch per-event tests against `end_events`.
///
/// Output schema = input columns followed by `session_id: Int NOT NULL` and
/// `session_duration: Int NOT NULL` (per §6.1–§6.2).
fn lower_sessionize(
    args: bqlite_ast::Sessionize,
    acc: LogicalPlan,
    source_table: &TableSchema,
) -> Result<LogicalPlan> {
    if args.gap <= 0 {
        return Err(BqliteError::Plan(format!(
            "SESSIONIZE: gap must be positive — got {}ns",
            args.gap
        )));
    }

    let input_schema = acc.output_schema();

    let end_events: Vec<String> = match args.end {
        None => Vec::new(),
        Some(refs) => {
            let mut out: Vec<String> = Vec::with_capacity(refs.len());
            for r in refs {
                let name = r.event.text;
                if out.iter().any(|existing| existing == &name) {
                    return Err(BqliteError::Plan(format!(
                        "SESSIONIZE: duplicate end-event type `{name}`"
                    )));
                }
                out.push(name);
            }
            out
        }
    };

    // Per sessionize.md §4: the operator only inspects event types when
    // `end_events` is non-empty ("`event_type_idx` is `Some` only when
    // `end_events` is non-empty. In gap-only mode, the operator never
    // inspects event types and does not require the `event_type` column.")
    // Only require the event-type column on the input schema when we'll
    // actually need it.
    if !end_events.is_empty() {
        let event_type_col = &source_table.event_type_column().name;
        if input_schema.column(event_type_col).is_none() {
            return Err(BqliteError::Plan(format!(
                "SESSIONIZE with explicit end events requires the input to expose \
                 event type column `{event_type_col}`"
            )));
        }
    }

    let mut cols = input_schema.columns().to_vec();
    cols.push(ColumnDef::required("session_id", BqlType::Int));
    cols.push(ColumnDef::required("session_duration", BqlType::Int));
    let output_schema = OperatorSchema::new(cols)?;

    Ok(LogicalPlan::Sessionize {
        gap: args.gap,
        end_events,
        forwarded_columns: Vec::new(),
        fused_downstream: None,
        input: Box::new(acc),
        output_schema,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 SAMPLE lowering — operators/event-select-sample.md §15–§17
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| SAMPLE(fraction: <f> [seed: <s>])` into a `Sample` logical node.
///
/// The parser already rejects out-of-range fractions, but we re-check
/// defensively so the planner layer can be audited independently (it is the
/// last place an out-of-range value could slip through before the operator
/// assumes `fraction ∈ [0, 1]`). NaN / infinity are also rejected.
///
/// Output schema equals the input schema: SAMPLE never reshapes.
fn lower_sample(args: bqlite_ast::Sample, acc: LogicalPlan) -> Result<LogicalPlan> {
    if !args.fraction.is_finite() || !(0.0..=1.0).contains(&args.fraction) {
        return Err(BqliteError::Plan(format!(
            "SAMPLE: fraction must be in [0.0, 1.0] — got {}",
            args.fraction
        )));
    }

    let output_schema = acc.output_schema().clone();
    Ok(LogicalPlan::Sample {
        fraction: args.fraction,
        seed: args.seed,
        input: Box::new(acc),
        output_schema,
    })
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
/// Wave 4 ships `IngestFormat::Csv` and `IngestFormat::JsonL`;
/// All three formats (`Csv`, `JsonL`, `Parquet`) are accepted; the
/// Parquet engine path was landed by TASK-449.
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
/// Map a format string from the `WITH (format: '...')` clause to an
/// [`IngestFormat`] variant.
///
/// Accepted values (case-insensitive):
/// - `"csv"` → [`IngestFormat::Csv`]
/// - `"jsonl"` or `"json"` → [`IngestFormat::JsonL`]
/// - `"parquet"` → [`IngestFormat::Parquet`] (TASK-449)
fn parse_ingest_format(s: &str) -> Result<IngestFormat> {
    match s.to_ascii_lowercase().as_str() {
        "csv" => Ok(IngestFormat::Csv),
        // Accept both "jsonl" (canonical) and "json" (common alias).
        "jsonl" | "json" => Ok(IngestFormat::JsonL),
        "parquet" => Ok(IngestFormat::Parquet),
        other => Err(BqliteError::Plan(format!(
            "INSERT FROM: unknown format `{other}` — supported formats: csv, jsonl (json), parquet"
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
                reader_backward_ns,
                reader_forward_ns,
                joined_tables,
                scan_predicates,
                projected_columns,
                output_schema,
            } => {
                assert_eq!(table.name(), "purchases");
                assert!(time_range.is_none());
                assert_eq!(*reader_backward_ns, 0);
                assert_eq!(*reader_forward_ns, 0);
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
    fn select_distinct_lowers_to_distinct_project() {
        // SELECT DISTINCT user_id lowers to Distinct(Project(...)) per §2.4.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Select {
                distinct: true,
                items: vec![select_bare_column("user_id")],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Distinct {
                ref input,
                ref output_schema,
            } => {
                assert!(matches!(**input, LogicalPlan::Project { .. }));
                assert_eq!(output_schema.columns().len(), 1);
                assert_eq!(output_schema.columns()[0].name, "user_id");
            }
            other => panic!("expected Distinct(Project(...)), got {other:?}"),
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
    fn insert_from_accepts_jsonl_in_wave_4() {
        // TASK-410: JSONL is now accepted; the planner should produce a
        // valid descriptor with `IngestFormat::JsonL`.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(
            insert_from(
                "purchases",
                "data.jsonl",
                vec![("format", Literal::String("jsonl".into()))],
                None,
            ),
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::Insert {
                body: InsertLogicalBody::From(desc),
                ..
            } => assert_eq!(desc.format, IngestFormat::JsonL),
            other => panic!("expected Insert::From, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_accepts_json_alias_for_jsonl() {
        // `"json"` is a documented alias for `"jsonl"` in parse_ingest_format.
        // Verify the planner accepts it and resolves to JsonL.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(
            insert_from(
                "purchases",
                "data.json",
                vec![("format", Literal::String("json".into()))],
                None,
            ),
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::Insert {
                body: InsertLogicalBody::From(desc),
                ..
            } => assert_eq!(desc.format, IngestFormat::JsonL),
            other => panic!("expected Insert::From, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_accepts_parquet_with_task_449() {
        // TASK-449 landed Parquet support — the planner must now accept it.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(
            insert_from(
                "purchases",
                "data.parquet",
                vec![("format", Literal::String("parquet".into()))],
                None,
            ),
            &cat,
        )
        .expect("parquet format must be accepted after TASK-449");
        match plan {
            LogicalPlan::Insert {
                body: InsertLogicalBody::From(desc),
                ..
            } => assert_eq!(desc.format, IngestFormat::Parquet),
            other => panic!("expected Insert::From, got {other:?}"),
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
            allow_scan: false,
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

    // ── Wave 3: MATCH lowering ────────────────────────────────────────────────

    fn match_step(name: Option<&str>, event: &str) -> bqlite_ast::pattern::Step {
        use bqlite_ast::pattern::{EventRef, Step, StepEvent};
        Step {
            name: name.map(Name::synthetic),
            event: StepEvent::Single(EventRef {
                table: None,
                event: Name::synthetic(event),
                span: Span::EMPTY,
            }),
            predicate: None,
            repetition: None,
            immediately_next: false,
            without_next: None,
            span: Span::EMPTY,
        }
    }

    #[test]
    fn match_two_step_pattern_produces_sequence_match() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Match {
                pattern: bqlite_ast::pattern::MatchPattern {
                    steps: vec![
                        match_step(Some("s"), "signup"),
                        match_step(Some("p"), "purchase"),
                    ],
                    mode: MatchMode::First,
                    emit_all: false,
                    window: None,
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::SequenceMatch {
                mode,
                emit_all,
                window,
                output_schema,
                ..
            } => {
                assert_eq!(*mode, MatchMode::First);
                assert!(!emit_all);
                assert!(window.is_none());
                // Should have: entity_id, match_duration, match_events,
                // + step properties for s and p (amount, country, s.ts, p.ts)
                assert!(output_schema.column("entity_id").is_some());
                assert!(output_schema.column("match_duration").is_some());
                assert!(output_schema.column("match_events").is_some());
                assert!(output_schema.column("s.amount").is_some());
                assert!(output_schema.column("p.country").is_some());
                assert!(output_schema.column("s.ts").is_some());
                assert!(output_schema.column("p.ts").is_some());
            }
            other => panic!("expected SequenceMatch, got {other:?}"),
        }
    }

    #[test]
    fn match_emit_all_produces_step_reached_column() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Match {
                pattern: bqlite_ast::pattern::MatchPattern {
                    steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                    mode: MatchMode::First,
                    emit_all: true,
                    window: None,
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::SequenceMatch {
                emit_all,
                output_schema,
                ..
            } => {
                assert!(emit_all);
                assert!(output_schema.column("step_reached").is_some());
                let (_, col) = output_schema.column("step_reached").unwrap();
                assert_eq!(col.bql_type, BqlType::Int);
                assert!(!col.nullable);
            }
            other => panic!("expected SequenceMatch, got {other:?}"),
        }
    }

    #[test]
    fn match_empty_pattern_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Match {
                pattern: bqlite_ast::pattern::MatchPattern {
                    steps: vec![],
                    mode: MatchMode::First,
                    emit_all: false,
                    window: None,
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("at least one step")));
    }

    #[test]
    fn match_duplicate_step_names_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Match {
                pattern: bqlite_ast::pattern::MatchPattern {
                    steps: vec![
                        match_step(Some("s"), "signup"),
                        match_step(Some("s"), "purchase"),
                    ],
                    mode: MatchMode::First,
                    emit_all: false,
                    window: None,
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("duplicate step name")));
    }

    #[test]
    fn match_with_window_produces_match_window_spec() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Match {
                pattern: bqlite_ast::pattern::MatchPattern {
                    steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                    mode: MatchMode::First,
                    emit_all: false,
                    window: Some(bqlite_ast::pattern::MatchWindow::Within(
                        604_800_000_000_000,
                    )),
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::SequenceMatch { window, .. } => {
                assert_eq!(
                    *window,
                    Some(MatchWindowSpec::Duration(604_800_000_000_000))
                );
            }
            other => panic!("expected SequenceMatch, got {other:?}"),
        }
    }

    // ── Wave 3: STATS lowering ──────────────────────────────────────────────────

    fn agg_item(alias: &str, func: &str, args: Vec<Spanned<Expr>>) -> bqlite_ast::AggItem {
        bqlite_ast::AggItem {
            function: Name::synthetic(func),
            args,
            distinct: false,
            alias: Name::synthetic(alias),
            span: Span::EMPTY,
        }
    }

    fn group_item(expr: Spanned<Expr>, alias: Option<&str>) -> bqlite_ast::GroupItem {
        bqlite_ast::GroupItem {
            expr,
            alias: alias.map(Name::synthetic),
            span: Span::EMPTY,
        }
    }

    #[test]
    fn stats_count_star_with_group_by() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Stats {
                aggregates: vec![agg_item("total", "count", vec![])],
                group_by: vec![group_item(column_expr("country"), None)],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Aggregate {
                aggregates,
                group_by,
                output_schema,
                ..
            } => {
                assert_eq!(aggregates.len(), 1);
                assert_eq!(aggregates[0].function, AggFunction::Count);
                assert!(aggregates[0].args.is_empty());
                assert_eq!(aggregates[0].output_name, "total");
                assert_eq!(aggregates[0].output_type, BqlType::Int);
                assert!(!aggregates[0].nullable);
                assert_eq!(group_by.len(), 1);
                assert_eq!(group_by[0].1, "country");
                let col_names: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(col_names, vec!["country", "total"]);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn stats_sum_with_type_validation() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Stats {
                aggregates: vec![agg_item("total_amount", "sum", vec![column_expr("amount")])],
                group_by: vec![],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Aggregate {
                aggregates,
                output_schema,
                ..
            } => {
                assert_eq!(aggregates[0].function, AggFunction::Sum);
                // SUM(Float) → Float
                assert_eq!(aggregates[0].output_type, BqlType::Float);
                assert!(aggregates[0].nullable);
                assert_eq!(output_schema.columns()[0].name, "total_amount");
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn stats_unknown_function_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Stats {
                aggregates: vec![agg_item("x", "median", vec![column_expr("amount")])],
                group_by: vec![],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(msg) if msg.contains("unknown aggregate function"))
        );
    }

    #[test]
    fn stats_sum_string_type_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Stats {
                aggregates: vec![agg_item("bad", "sum", vec![column_expr("country")])],
                group_by: vec![],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("does not accept")));
    }

    #[test]
    fn stats_duplicate_output_name_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Stats {
                aggregates: vec![
                    agg_item("x", "count", vec![]),
                    agg_item("x", "sum", vec![column_expr("amount")]),
                ],
                group_by: vec![],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("duplicate output name")));
    }

    // ── Wave 3: ORDER BY lowering ───────────────────────────────────────────────

    #[test]
    fn order_by_sorts_by_column() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::OrderBy {
                items: vec![bqlite_ast::expr::OrderItem {
                    expr: column_expr("amount"),
                    direction: bqlite_ast::expr::SortDir::Desc,
                    span: Span::EMPTY,
                }],
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Sort {
                keys,
                output_schema,
                ..
            } => {
                assert_eq!(keys.len(), 1);
                assert_eq!(keys[0].1, SortDirection::Desc);
                // Sort output schema equals input schema.
                let scan_schema = OperatorSchema::from_table(&purchases_schema());
                assert_eq!(output_schema, &scan_schema);
            }
            other => panic!("expected Sort, got {other:?}"),
        }
    }

    #[test]
    fn order_by_empty_items_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::OrderBy {
                items: vec![],
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("at least one sort key")));
    }

    // ── Wave 3: FUNNEL desugaring (integration through lower_statement) ───────
    //
    // These tests verify that `PipelineStage::Funnel` flowing through
    // `fold_stage` → `desugar_funnel` → `fold_stage(Match)` → `fold_stage(Stats)`
    // produces the correct LogicalPlan tree.  Pure unit tests for the AST rewrite
    // live in `opt::desugar_funnel::tests`.

    #[test]
    fn funnel_two_steps_lowers_to_aggregate_over_sequence_match() {
        use bqlite_ast::Funnel;
        use bqlite_core::AggFunction;

        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Funnel(Funnel {
                steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                window: Some(604_800_000_000_000), // 7 days in ns
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();

        // Outer node is Aggregate.
        let LogicalPlan::Aggregate {
            aggregates,
            group_by,
            input,
            ..
        } = plan
        else {
            panic!("expected Aggregate at top level, got {plan:?}");
        };

        // No group-by — FUNNEL produces a bare aggregate.
        assert!(
            group_by.is_empty(),
            "FUNNEL aggregate must have no GROUP BY"
        );

        // Two aggregates: one per step.
        assert_eq!(aggregates.len(), 2, "two steps → two aggregates");
        assert_eq!(aggregates[0].output_name, "signup");
        assert_eq!(aggregates[0].function, AggFunction::Sum);
        assert_eq!(aggregates[1].output_name, "purchase");
        assert_eq!(aggregates[1].function, AggFunction::Sum);

        // Inner node is SequenceMatch with emit_all: true and 7d window.
        let LogicalPlan::SequenceMatch {
            emit_all,
            window,
            output_schema,
            ..
        } = *input
        else {
            panic!("expected SequenceMatch inside Aggregate");
        };
        assert!(emit_all, "FUNNEL MATCH must have emit_all = true");
        assert_eq!(
            window,
            Some(MatchWindowSpec::Duration(604_800_000_000_000)),
            "7d window must be preserved"
        );
        assert!(
            output_schema.column("step_reached").is_some(),
            "emit_all → step_reached column present"
        );
    }

    #[test]
    fn funnel_named_steps_use_step_names_as_aggregate_output_names() {
        use bqlite_ast::Funnel;
        use bqlite_core::AggFunction;

        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Funnel(Funnel {
                steps: vec![
                    match_step(Some("s"), "signup"),
                    match_step(Some("p"), "purchase"),
                ],
                window: None,
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();

        let LogicalPlan::Aggregate { aggregates, .. } = plan else {
            panic!("expected Aggregate, got {plan:?}");
        };
        assert_eq!(aggregates.len(), 2);
        // Step names, not event type names.
        assert_eq!(aggregates[0].output_name, "s");
        assert_eq!(aggregates[0].function, AggFunction::Sum);
        assert_eq!(aggregates[1].output_name, "p");
        assert_eq!(aggregates[1].function, AggFunction::Sum);
    }

    #[test]
    fn funnel_three_steps_produces_three_aggregates() {
        use bqlite_ast::Funnel;

        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Funnel(Funnel {
                steps: vec![
                    match_step(None, "signup"),
                    match_step(None, "activation"),
                    match_step(None, "purchase"),
                ],
                window: None,
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();

        let LogicalPlan::Aggregate { aggregates, .. } = plan else {
            panic!("expected Aggregate, got {plan:?}");
        };
        assert_eq!(aggregates.len(), 3);
        assert_eq!(aggregates[0].output_name, "signup");
        assert_eq!(aggregates[1].output_name, "activation");
        assert_eq!(aggregates[2].output_name, "purchase");
    }

    #[test]
    fn funnel_duplicate_step_names_rejected_at_lowering_time() {
        use bqlite_ast::Funnel;

        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Funnel(Funnel {
                // Both steps would produce "signup" — should be rejected.
                steps: vec![match_step(None, "signup"), match_step(None, "signup")],
                window: None,
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        let BqliteError::Plan(msg) = err else {
            panic!("expected Plan error, got {err:?}");
        };
        assert!(
            msg.contains("signup"),
            "error must mention the colliding name; got: {msg}"
        );
    }

    // ── Source time-range lowering ──────────────────────────────────────

    fn pipeline_with_time_range(
        name: &str,
        time_range: Option<TimeRange>,
        stages: Vec<PipelineStage>,
    ) -> Pipeline {
        Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic(name),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range,
                span: Span::EMPTY,
            },
            stages,
            span: Span::EMPTY,
        }
    }

    #[test]
    fn scan_with_last_time_range_lowered() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_30d = 30 * 86_400_000_000_000_i64;
        let pipeline = pipeline_with_time_range("purchases", Some(TimeRange::Last(ns_30d)), vec![]);
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Scan { time_range, .. } => {
                assert_eq!(*time_range, Some(TimeRange::Last(ns_30d)));
            }
            _ => panic!("expected Scan, got {plan:?}"),
        }
    }

    #[test]
    fn scan_with_between_time_range_lowered() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_time_range(
            "purchases",
            Some(TimeRange::Between {
                start: "2024-01-01".into(),
                end: "2024-02-01".into(),
            }),
            vec![],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::Scan { time_range, .. } => {
                assert_eq!(
                    *time_range,
                    Some(TimeRange::Between {
                        start: "2024-01-01".into(),
                        end: "2024-02-01".into(),
                    })
                );
            }
            _ => panic!("expected Scan, got {plan:?}"),
        }
    }

    #[test]
    fn scan_time_range_extended_by_match_within_window() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_30d = 30 * 86_400_000_000_000_i64;
        let ns_7d = 7 * 86_400_000_000_000_i64;
        let pipeline = pipeline_with_time_range(
            "purchases",
            Some(TimeRange::Last(ns_30d)),
            vec![PipelineStage::Match {
                pattern: MatchPattern {
                    mode: MatchMode::First,
                    emit_all: false,
                    steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                    window: Some(MatchWindow::Within(ns_7d)),
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        // The SequenceMatch wraps a Scan. The pristine time_range must remain
        // Last(30d) unchanged; reader_forward_ns captures the 7d extension.
        match &plan {
            LogicalPlan::SequenceMatch { input, .. } => match input.as_ref() {
                LogicalPlan::Scan {
                    time_range,
                    reader_forward_ns,
                    reader_backward_ns,
                    ..
                } => {
                    assert_eq!(
                        *time_range,
                        Some(TimeRange::Last(ns_30d)),
                        "time_range must not be mutated"
                    );
                    assert_eq!(
                        *reader_forward_ns, ns_7d,
                        "reader_forward_ns must hold the 7d extension"
                    );
                    assert_eq!(*reader_backward_ns, 0, "reader_backward_ns must remain 0");
                }
                other => panic!("expected Scan under SequenceMatch, got {other:?}"),
            },
            _ => panic!("expected SequenceMatch, got {plan:?}"),
        }
    }

    #[test]
    fn scan_between_range_extended_by_match_within_window() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_2d = 2 * 86_400_000_000_000_i64;
        let pipeline = pipeline_with_time_range(
            "purchases",
            Some(TimeRange::Between {
                start: "2024-01-02T00:00:00Z".into(),
                end: "2024-01-03T00:00:00Z".into(),
            }),
            vec![PipelineStage::Match {
                pattern: MatchPattern {
                    mode: MatchMode::First,
                    emit_all: false,
                    steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                    window: Some(MatchWindow::Within(ns_2d)),
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        // The pristine time_range must remain unchanged (Jan2..Jan3).
        // reader_forward_ns captures the 2d extension.
        match &plan {
            LogicalPlan::SequenceMatch { input, .. } => match input.as_ref() {
                LogicalPlan::Scan {
                    time_range,
                    reader_forward_ns,
                    reader_backward_ns,
                    ..
                } => {
                    assert_eq!(
                        *time_range,
                        Some(TimeRange::Between {
                            start: "2024-01-02T00:00:00Z".into(),
                            end: "2024-01-03T00:00:00Z".into(),
                        }),
                        "time_range must not be mutated"
                    );
                    assert_eq!(
                        *reader_forward_ns, ns_2d,
                        "reader_forward_ns must hold the 2d extension"
                    );
                    assert_eq!(*reader_backward_ns, 0, "reader_backward_ns must remain 0");
                }
                other => panic!("expected Scan under SequenceMatch, got {other:?}"),
            },
            _ => panic!("expected SequenceMatch, got {plan:?}"),
        }
    }

    #[test]
    fn scan_time_range_not_extended_when_no_window() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_30d = 30 * 86_400_000_000_000_i64;
        let pipeline = pipeline_with_time_range(
            "purchases",
            Some(TimeRange::Last(ns_30d)),
            vec![PipelineStage::Match {
                pattern: MatchPattern {
                    mode: MatchMode::First,
                    emit_all: false,
                    steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                    window: None,
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::SequenceMatch { input, .. } => match input.as_ref() {
                LogicalPlan::Scan {
                    time_range,
                    reader_backward_ns,
                    reader_forward_ns,
                    ..
                } => {
                    assert_eq!(*time_range, Some(TimeRange::Last(ns_30d)));
                    assert_eq!(
                        *reader_backward_ns, 0,
                        "reader_backward_ns must be 0 when no window"
                    );
                    assert_eq!(
                        *reader_forward_ns, 0,
                        "reader_forward_ns must be 0 when no window"
                    );
                }
                other => panic!("expected Scan under SequenceMatch, got {other:?}"),
            },
            _ => panic!("expected SequenceMatch, got {plan:?}"),
        }
    }

    #[test]
    fn scan_time_range_no_op_when_none() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_7d = 7 * 86_400_000_000_000_i64;
        let pipeline = pipeline_with_time_range(
            "purchases",
            None,
            vec![PipelineStage::Match {
                pattern: MatchPattern {
                    mode: MatchMode::First,
                    emit_all: false,
                    steps: vec![match_step(None, "signup"), match_step(None, "purchase")],
                    window: Some(MatchWindow::Within(ns_7d)),
                    brackets: None,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match &plan {
            LogicalPlan::SequenceMatch { input, .. } => match input.as_ref() {
                LogicalPlan::Scan { time_range, .. } => {
                    assert!(time_range.is_none());
                }
                other => panic!("expected Scan under SequenceMatch, got {other:?}"),
            },
            _ => panic!("expected SequenceMatch, got {plan:?}"),
        }
    }

    // ── Wave 4 CP1: SESSIONIZE + SAMPLE lowering ───────────────────────────

    fn event_ref(name: &str) -> bqlite_ast::pattern::EventRef {
        bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    #[test]
    fn sessionize_default_end_events_lowers_cleanly() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: 30 * 60 * 1_000_000_000, // 30 min
                end: None,
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Sessionize {
                gap,
                end_events,
                forwarded_columns,
                fused_downstream,
                input,
                output_schema,
            } => {
                assert_eq!(gap, 30 * 60 * 1_000_000_000);
                assert!(end_events.is_empty());
                assert!(forwarded_columns.is_empty());
                assert!(fused_downstream.is_none());
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
                let names: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                // Input columns + session_id + session_duration at the end.
                assert_eq!(
                    names,
                    vec![
                        "user_id",
                        "ts",
                        "event",
                        "amount",
                        "country",
                        "__seq_id",
                        "__batch_id",
                        "session_id",
                        "session_duration"
                    ]
                );
                let sid = output_schema.column("session_id").unwrap().1;
                assert_eq!(sid.bql_type, BqlType::Int);
                assert!(!sid.nullable);
                let sdur = output_schema.column("session_duration").unwrap().1;
                assert_eq!(sdur.bql_type, BqlType::Int);
                assert!(!sdur.nullable);
            }
            other => panic!("expected Sessionize, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_with_end_events_keeps_order() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: 60_000_000_000,
                end: Some(vec![event_ref("logout"), event_ref("tab_close")]),
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Sessionize { end_events, .. } => {
                assert_eq!(end_events, vec!["logout", "tab_close"]);
            }
            other => panic!("expected Sessionize, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_duplicate_end_event_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: 60_000_000_000,
                end: Some(vec![event_ref("logout"), event_ref("logout")]),
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("duplicate end-event type"));
                assert!(msg.contains("logout"));
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_zero_gap_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: 0,
                end: None,
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be positive")));
    }

    #[test]
    fn sessionize_negative_gap_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: -1,
                end: None,
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be positive")));
    }

    #[test]
    fn sample_fraction_valid_lowers_cleanly() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sample(bqlite_ast::Sample {
                fraction: 0.25,
                seed: None,
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Sample {
                fraction,
                seed,
                input,
                output_schema,
            } => {
                assert_eq!(fraction, 0.25);
                assert!(seed.is_none());
                // Output schema matches the input (Scan) schema exactly.
                assert_eq!(&output_schema, input.output_schema());
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
            }
            other => panic!("expected Sample, got {other:?}"),
        }
    }

    #[test]
    fn sample_with_seed_preserves_seed() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sample(bqlite_ast::Sample {
                fraction: 1.0,
                seed: Some(42),
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Sample { fraction, seed, .. } => {
                assert_eq!(fraction, 1.0);
                assert_eq!(seed, Some(42));
            }
            other => panic!("expected Sample, got {other:?}"),
        }
    }

    #[test]
    fn sample_fraction_above_one_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sample(bqlite_ast::Sample {
                fraction: 1.5,
                seed: None,
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be in [0.0, 1.0]")));
    }

    #[test]
    fn sample_negative_fraction_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sample(bqlite_ast::Sample {
                fraction: -0.1,
                seed: None,
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be in [0.0, 1.0]")));
    }

    #[test]
    fn sample_nan_fraction_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Sample(bqlite_ast::Sample {
                fraction: f64::NAN,
                seed: None,
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be in [0.0, 1.0]")));
    }

    #[test]
    fn sessionize_then_sample_composes() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![
                PipelineStage::Sessionize(bqlite_ast::Sessionize {
                    gap: 60_000_000_000,
                    end: None,
                    span: Span::EMPTY,
                }),
                PipelineStage::Sample(bqlite_ast::Sample {
                    fraction: 0.5,
                    seed: Some(7),
                    span: Span::EMPTY,
                }),
            ],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Sample { input, .. } => match *input {
                LogicalPlan::Sessionize { input: inner, .. } => {
                    assert!(matches!(*inner, LogicalPlan::Scan { .. }));
                }
                other => panic!("expected Sessionize under Sample, got {other:?}"),
            },
            other => panic!("expected Sample on top, got {other:?}"),
        }
    }

    #[test]
    fn extend_scan_reader_forward_reaches_through_filter() {
        let schema = purchases_schema();
        let ns_30d = 30 * 86_400_000_000_000_i64;
        let ns_7d = 7 * 86_400_000_000_000_i64;
        let scan = LogicalPlan::scan_with_time_range(schema.clone(), Some(TimeRange::Last(ns_30d)));
        let predicate = TypedExpr {
            kind: crate::expr::TypedExprKind::Literal(PropertyValue::Bool(true)),
            result_type: BqlType::Bool,
            nullable: false,
            span: Span::EMPTY,
        };
        let mut plan = LogicalPlan::filter(predicate, scan).unwrap();
        plan.extend_scan_reader_forward(ns_7d).unwrap();
        // Walk down to the scan to check: time_range unchanged, reader_forward_ns set.
        match &plan {
            LogicalPlan::Filter { input, .. } => match input.as_ref() {
                LogicalPlan::Scan {
                    time_range,
                    reader_forward_ns,
                    ..
                } => {
                    assert_eq!(
                        *time_range,
                        Some(TimeRange::Last(ns_30d)),
                        "time_range must not be mutated"
                    );
                    assert_eq!(*reader_forward_ns, ns_7d, "reader_forward_ns must be set");
                }
                other => panic!("expected Scan under Filter, got {other:?}"),
            },
            _ => panic!("expected Filter, got {plan:?}"),
        }
    }
}
