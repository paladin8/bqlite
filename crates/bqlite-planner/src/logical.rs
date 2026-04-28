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

use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};

use bqlite_ast::expr::{Expr, InRhs, Literal, SortDir, Spanned};
use bqlite_ast::pattern::{BracketSpec, MatchMode, MatchPattern, MatchWindow, StepEvent};
use bqlite_ast::pipeline::{Pipeline, TimeRange};
use bqlite_ast::{
    AlterAction, AlterTableStmt, ColumnDef as AstColumnDef, ColumnRole, CreateTableStmt,
    DescribeStmt, DropTableStmt, InsertBody, InsertStmt, PipelineStage, SelectItem, SelectItemKind,
    Statement,
};
use bqlite_core::{
    AggFunction, BqlType, BqliteError, Catalog, ColumnDef, OperatorSchema, PropertyValue, Result,
    ScalarValue, TableSchema, Timestamp,
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
        /// Original query time range `(start_ns, end_ns)`, captured at
        /// logical-lowering time so the physical layer can apply the
        /// extension-aware conversion-emission filter from
        /// `operators/attribute.md` §12 ("only conversions whose
        /// `conversion_ts` falls within this original range trigger
        /// emission — touchpoints from the extended zone are deque
        /// material only").
        ///
        /// `None` when the source is unbounded (no `LAST`/`BETWEEN`
        /// clause) or when the range is a `LAST <dur>` form that needs
        /// `now_ns` to resolve — the physical layer handles the latter
        /// via a small fallback walk, since it is the first place
        /// `now_ns` is in scope.
        conversion_range: Option<(i64, i64)>,
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

    /// `DELETE FROM <table> WHERE <predicate> [ALLOW SCAN]`. Wave 4.
    ///
    /// The single statement-level node that owns DELETE execution.
    /// Carries the resolved table schema and a [`DeleteFilter`] that
    /// classifies the predicate at plan time per
    /// `docs/design/storage/deletes.md` §3 (cheap-class taxonomy) and
    /// §4 (default-reject + `ALLOW SCAN` opt-in).
    ///
    /// Output schema is empty: DELETE produces no result rows; the
    /// engine returns the `rows_affected` count out-of-band on
    /// `ExecutionResult` per §11. See
    /// `docs/design/storage/deletes.md` §15 for the full per-task
    /// breakdown.
    Delete {
        /// Catalog-resolved target table.
        table: TableSchema,
        /// Classified delete filter (cheap-class entries vs. ALLOW SCAN).
        filter: DeleteFilter,
        /// Whether the source statement carried the `ALLOW SCAN`
        /// suffix. Carried through for diagnostics and for the
        /// engine's idempotence-caveat documentation (deletes.md
        /// §10.2). The classifier already used this flag to decide
        /// between `Cheap` and `AllowScan` variants, so the engine
        /// does not branch on it again — the variant is the source
        /// of truth.
        allow_scan: bool,
        /// Empty schema — DELETE produces no rows.
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

// ─────────────────────────────────────────────────────────────────────────────
// DELETE planner support types (deletes.md §3 / §4)
// ─────────────────────────────────────────────────────────────────────────────

/// Plan-time classification of a DELETE predicate.
///
/// Either a [`CheapDeleteSpec`] decomposition that the engine writes
/// directly to per-shard tombstone files, or a full-scan path
/// (`ALLOW SCAN`) where the engine drives a `Filter(Scan)` to
/// materialize matching `__seq_id`s. See
/// `docs/design/storage/deletes.md` §3 for the cheap-class taxonomy
/// and §4 for the `ALLOW SCAN` opt-in semantics.
#[derive(Debug, Clone, PartialEq)]
pub enum DeleteFilter {
    /// Cheap-class direct tombstone writes — no data scan required.
    Cheap(CheapDeleteSpec),
    /// `ALLOW SCAN` — engine scans the table, evaluates the
    /// predicate, and tombstones every matching row by `__seq_id`.
    AllowScan {
        /// The full predicate, type-checked against the source
        /// table's schema. Engine compiles this to a `CompiledExpr`
        /// at bind time and feeds it to `ScanPhysical::scan_predicates`.
        predicate: TypedExpr,
    },
}

/// Decomposed cheap-class DELETE predicate.
///
/// Same-granularity terms are collapsed (multiple time-range
/// comparisons → one [`TimeRangeDelete`] with both bounds; multiple
/// entity equalities / IN-lists → one deduplicated `entity_keys` vec).
/// Cross-granularity is rejected at the classifier with the SS3.2
/// exception: entity equality / IN combined with `__seq_id` or
/// `__batch_id` is accepted, with the entity terms playing the
/// shard-targeting role (see [`EntityRole`]).
///
/// **Invariant:** at least one tombstone-producing field is non-empty:
///
/// - `entity_role == AsTombstone` and `entity_keys` non-empty, or
/// - `seq_ids` non-empty, or
/// - `batch_ids` non-empty, or
/// - `time_range` is `Some`.
///
/// Furthermore, when `entity_role == AsShardTarget`, `entity_keys` is
/// non-empty **and** at least one of `seq_ids` / `batch_ids` is
/// non-empty (the shard-target role is meaningless on its own).
/// Both invariants are checked by the classifier.
#[derive(Debug, Clone, PartialEq)]
pub struct CheapDeleteSpec {
    /// Entity-key values from the predicate. Plays one of two roles
    /// per [`entity_role`](Self::entity_role).
    pub entity_keys: Vec<ScalarValue>,
    /// Whether `entity_keys` are themselves the tombstone or just
    /// shard-targeting metadata for a paired row/batch tombstone.
    pub entity_role: EntityRole,
    /// `__seq_id` literals (for row-level tombstones).
    pub seq_ids: Vec<u64>,
    /// `__batch_id` literals (for batch-level tombstones).
    pub batch_ids: Vec<u64>,
    /// Time-range bounds, collapsed from one or two ts comparisons
    /// or a single `BETWEEN`. `None` when no time-range term is
    /// present.
    pub time_range: Option<TimeRangeSpec>,
}

/// Plain-data view of a [`bqlite_storage::TimeRangeDelete`] held in
/// the planner.
///
/// The planner cannot import `bqlite-storage` (it sits below in the
/// crate graph), so the equivalent shape lives here and the engine
/// converts to `TimeRangeDelete` at write time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeRangeSpec {
    /// Lower bound (epoch nanoseconds). `None` = unbounded below.
    pub min_ts: Option<i64>,
    pub min_inclusive: bool,
    /// Upper bound (epoch nanoseconds). `None` = unbounded above.
    pub max_ts: Option<i64>,
    pub max_inclusive: bool,
}

/// The role `entity_keys` play inside a [`CheapDeleteSpec`].
///
/// See `docs/design/storage/deletes.md` §3.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityRole {
    /// Entity equality / IN-list is itself the tombstone — written as
    /// entity-level entries to the targeted shards.
    AsTombstone,
    /// Entity equality / IN-list paired with `__seq_id` or
    /// `__batch_id` terms — used only to narrow which shards the
    /// engine writes the row/batch tombstones to. The entity values
    /// are not written as entity-level tombstones.
    AsShardTarget,
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
            | LogicalPlan::Sample { output_schema, .. }
            | LogicalPlan::Delete { output_schema, .. } => output_schema,
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
            // Recurse through every wrapper that sits above the primary scan.
            // Must stay in sync with `find_primary_scan` — otherwise a widening
            // call can silently become a no-op for valid compositions such as
            // `SESSIONIZE | ATTRIBUTE(window: ...)` (attribute.md §14.1).
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sessionize { input, .. }
            | LogicalPlan::EventSelect { input, .. }
            | LogicalPlan::Sample { input, .. }
            | LogicalPlan::SubqueryFilter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::SequenceMatch { input, .. }
            | LogicalPlan::Attribute { input, .. } => input.extend_scan_reader_backward(ns),
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
            // Match the wrapper set used by `extend_scan_reader_backward` so the
            // forward-widening path is symmetric. MATCH / pattern windowing
            // relies on this for pipelines like `SESSIONIZE | MATCH`.
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Sessionize { input, .. }
            | LogicalPlan::EventSelect { input, .. }
            | LogicalPlan::Sample { input, .. }
            | LogicalPlan::SubqueryFilter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Distinct { input, .. }
            | LogicalPlan::SequenceMatch { input, .. }
            | LogicalPlan::Attribute { input, .. } => input.extend_scan_reader_forward(ns),
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

/// Alias resolution context threaded through pipeline lowering.
///
/// Holds the source-ordered list of alias names, their raw bodies, a cache of
/// already-lowered bodies, and the active resolution stack for cycle
/// detection. Uses interior mutability (`RefCell`) so callers can pass a
/// `&AliasTable` reference through the recursive lowering chain without
/// refactoring every helper's signature to `&mut`.
///
/// Semantics follow `docs/design/language/cohorts-aliases-joins.md` §2:
/// - Source-order definitions: a reference to alias `b` in alias `a`'s body
///   is valid only if `b` was defined earlier (§2.3 forward-reference rule).
/// - Last-wins on duplicate names (§2.2): redefining an alias overrides the
///   prior definition.
/// - Cycle detection (§2.3): two aliases referring to each other (directly or
///   transitively) raise a `Plan` error naming the full resolution path.
#[derive(Debug, Default)]
pub struct AliasTable {
    /// Source-ordered list of `(alias_name, position)`. Forward-reference
    /// check uses `position`: a reference from alias body at position `j`
    /// is valid iff the referenced alias has a position `i < j`.
    order: Vec<String>,
    /// Alias name → (pipeline body, position in `order`). Last-wins: the
    /// loader overwrites on duplicate names.
    definitions: BTreeMap<String, (Pipeline, usize)>,
    /// Lowered alias bodies, keyed by name. Populated lazily on first
    /// reference. `RefCell` enables interior mutability through a shared
    /// reference (lowering helpers take `&AliasTable`).
    resolved: RefCell<BTreeMap<String, LogicalPlan>>,
    /// Active resolution stack for cycle detection. Pushed when a name
    /// starts resolving and popped when it finishes.
    path: RefCell<Vec<String>>,
}

impl AliasTable {
    /// Build an empty alias table — used by the legacy single-statement
    /// entrypoint and by all pipelines lowered outside a multi-statement
    /// script.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Install one alias definition. Position-indexed so the forward-
    /// reference check can compare source-order positions rather than
    /// rescan the entire map.
    fn push_definition(&mut self, name: String, body: Pipeline) {
        let pos = self.order.len();
        self.order.push(name.clone());
        // Last-wins on duplicates: later definitions shadow earlier ones
        // (cohorts-aliases-joins.md §2.2).
        self.definitions.insert(name, (body, pos));
    }

    /// Return the source-order position of the most recent definition of
    /// `name`, if any. Used to enforce the forward-reference rule.
    fn position_of(&self, name: &str) -> Option<usize> {
        self.definitions.get(name).map(|(_, pos)| *pos)
    }
}

/// Lower a single AST [`Statement`] into a [`LogicalPlan`] with no alias
/// context. Back-compat shim over [`lower_statements`] for callers that do
/// not go through a multi-statement script.
///
/// Table references are resolved via `catalog`; unknown tables surface as
/// [`BqliteError::Plan`] via `bqlite_core::catalog::unknown_table_error`.
pub fn lower_statement(statement: Statement, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let aliases = AliasTable::empty();
    lower_statement_with_aliases(statement, catalog, &aliases)
}

/// Lower a sequence of `Statement`s — the shape `bqlite_parser::parse` emits
/// from a full BQL script.
///
/// The input is expected to be zero or more `Statement::DefineAlias` items
/// followed by exactly one non-alias terminal (`Query` / `Explain` / DDL /
/// `Insert` / `Delete`). Alias definitions are collected source-order into an
/// [`AliasTable`]; the terminal is then lowered with that table in scope so
/// `IN alias <name>` references in the terminal's WHERE clauses resolve to
/// the cached lowered bodies.
pub fn lower_statements(statements: Vec<Statement>, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    if statements.is_empty() {
        return Err(BqliteError::Plan(
            "empty BQL script — expected at least one terminal statement".into(),
        ));
    }

    let mut aliases = AliasTable::empty();
    let mut terminal: Option<Statement> = None;

    for stmt in statements {
        match stmt {
            Statement::DefineAlias { name, body, .. } => {
                if terminal.is_some() {
                    return Err(BqliteError::Plan(
                        "alias definitions must precede the terminal statement".into(),
                    ));
                }
                aliases.push_definition(name.text, body);
            }
            other => {
                if terminal.is_some() {
                    return Err(BqliteError::Plan(
                        "script must contain exactly one non-alias terminal statement".into(),
                    ));
                }
                terminal = Some(other);
            }
        }
    }

    let terminal = terminal.ok_or_else(|| {
        BqliteError::Plan(
            "script contains only alias definitions — a terminal statement is required".into(),
        )
    })?;

    lower_statement_with_aliases(terminal, catalog, &aliases)
}

/// Lower a single statement with an alias table already in scope. This is
/// the shared implementation for both [`lower_statement`] and
/// [`lower_statements`].
fn lower_statement_with_aliases(
    statement: Statement,
    catalog: &dyn Catalog,
    aliases: &AliasTable,
) -> Result<LogicalPlan> {
    match statement {
        Statement::Query(pipeline) => lower_query_pipeline(pipeline, catalog, aliases),
        Statement::Explain(pipeline) => {
            let plan = lower_query_pipeline(pipeline, catalog, aliases)?;
            Ok(LogicalPlan::explain(plan))
        }
        Statement::CreateTable(stmt) => lower_create_table(stmt, catalog),
        Statement::DropTable(stmt) => lower_drop_table(stmt, catalog),
        Statement::AlterTable(stmt) => lower_alter_table(stmt, catalog),
        Statement::Describe(stmt) => lower_describe(stmt, catalog),
        Statement::Insert(stmt) => lower_insert(stmt, catalog),
        Statement::Delete(stmt) => lower_delete(stmt, catalog),
        Statement::DefineAlias { .. } => Err(BqliteError::Plan(
            "alias definitions must precede the terminal statement — \
             they cannot appear as the terminal statement itself"
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
/// Name of the planner-injected discriminator column that tags rows emitted
/// by a joined-source scan with the ordinal position of the table they came
/// from (0 = primary, 1..N = joined tables in source order). Per
/// `docs/design/language/cohorts-aliases-joins.md` §3.8. The spec names the
/// type `Int8`; `BqlType` has no `Int8` so we use `Int` here (the Arrow
/// layer may narrow when materializing).
pub const SOURCE_TABLE_ID_COLUMN: &str = "__source_table_id";

fn lower_query_pipeline(
    pipeline: Pipeline,
    catalog: &dyn Catalog,
    aliases: &AliasTable,
) -> Result<LogicalPlan> {
    // Resolve the primary table against the catalog.
    let primary_name = pipeline.source.primary.name.text.as_str();
    let primary_schema = catalog.resolve_table(primary_name)?;

    // Resolve each joined table and validate entity-key type compatibility
    // per cohorts-aliases-joins.md §3.11. Self-joins are rejected at the
    // parser (TASK-452) and re-guarded here defensively.
    let mut joined_schemas: Vec<TableSchema> = Vec::with_capacity(pipeline.source.joins.len());
    for joined_ref in &pipeline.source.joins {
        let joined_name = joined_ref.name.text.as_str();
        if joined_name == primary_name || joined_schemas.iter().any(|t| t.name() == joined_name) {
            return Err(BqliteError::Plan(format!(
                "JOIN `{joined_name}` is a self-join — self-joins are forbidden \
                 (query-language.md §19.2)"
            )));
        }
        let joined = catalog.resolve_table(joined_name)?;
        let primary_ek = &primary_schema.entity_key_column().bql_type;
        let joined_ek = &joined.entity_key_column().bql_type;
        if primary_ek != joined_ek {
            return Err(BqliteError::Plan(format!(
                "JOIN entity-key type mismatch: primary `{primary_name}` has \
                 `{primary_ek}`, joined `{joined_name}` has `{joined_ek}` \
                 (cohorts-aliases-joins.md §3.11)"
            )));
        }
        joined_schemas.push(joined);
    }

    // Build the initial Scan. Time range carries through from the
    // AST's `source.time_range` field — the parser already decodes
    // `LAST <duration>` into nanoseconds and `BETWEEN ... AND ...`
    // into `(String, String)` (query-language.md §16).
    let mut plan = if joined_schemas.is_empty() {
        LogicalPlan::scan_with_time_range(primary_schema.clone(), pipeline.source.time_range)
    } else {
        build_joined_scan(
            primary_schema.clone(),
            joined_schemas,
            pipeline.source.time_range,
        )?
    };

    // Function registry for expression-level type checking. Wave 2
    // ships the built-in set (`like`, `regex`); later waves extend
    // via the registry's `register` API.
    let registry = FunctionRegistry::with_builtins();

    // Fold pipeline stages in order. Each stage wraps `plan` in a
    // new logical node whose input is the previous `plan`.
    for stage in pipeline.stages {
        plan = fold_stage(stage, plan, &registry, catalog, &primary_schema, aliases)?;
    }

    Ok(plan)
}

/// Build a [`LogicalPlan::Scan`] for an entity-aligned multi-table source.
///
/// Combined output schema (cohorts-aliases-joins.md §3.8):
/// - Each user column from every joined table (primary first, then joins in
///   source order) is named `<table>.<column>`. Bare references
///   (`Expr::Column("user_id")`) therefore cannot resolve in joined contexts —
///   users must write `purchases.user_id` / `logins.user_id`, matching the
///   mandatory-qualification rule of §3.11.
/// - A `__source_table_id: Int NOT NULL` discriminator is appended so
///   downstream operators can route rows back to their originating table.
/// - The implicit system columns `__seq_id` and `__batch_id` are
///   appended bare-named (no `<table>.` qualifier) and NOT NULL. They
///   are populated by `MergeSourcesOperator`'s bare-name resolution
///   path against each sub-scan's emitted system columns
///   (system-columns.md §4.2).
///
/// Entity-key type compatibility is verified by the caller.
fn build_joined_scan(
    primary: TableSchema,
    joined: Vec<TableSchema>,
    time_range: Option<TimeRange>,
) -> Result<LogicalPlan> {
    let mut cols: Vec<ColumnDef> = Vec::new();

    // Per-table non-system columns, qualified with `<table>.<column>`.
    //
    // Every cross-table qualified column is marked **nullable** in the
    // combined schema, regardless of its declared nullability in the
    // source table. Rationale: in the merged output, a row picked from
    // sub-scan `i` carries that table's column values and NULL for every
    // other table's columns (cohorts-aliases-joins.md §3.8). Preserving
    // the source's non-nullable flag here would let the physical layer
    // declare columns non-nullable that the runtime cannot fill with
    // non-null values, causing Arrow `RecordBatch::try_new` to fail at
    // `MergeSourcesOperator` emit time. TASK-436 flips these to nullable
    // so the schema matches the runtime contract.
    //
    // `default_value` is dropped for the same reason: the default
    // applies to the source table's canonical insert, not to
    // cross-table merge-output synthesis.
    for col in primary.columns() {
        if col.is_system() {
            continue;
        }
        cols.push(ColumnDef {
            name: format!("{}.{}", primary.name(), col.name),
            bql_type: col.bql_type.clone(),
            nullable: true,
            default_value: None,
        });
    }
    for t in &joined {
        for col in t.columns() {
            if col.is_system() {
                continue;
            }
            cols.push(ColumnDef {
                name: format!("{}.{}", t.name(), col.name),
                bql_type: col.bql_type.clone(),
                nullable: true,
                default_value: None,
            });
        }
    }

    // Discriminator + system columns. Per
    // `docs/design/storage/system-columns.md` §4.2 (which extends
    // `cohorts-aliases-joins.md` §3.8), the combined schema declares
    // `__source_table_id`, `__seq_id`, and `__batch_id` as bare-named,
    // NOT NULL, Int. The merge picks one row from one sub-scan at a
    // time, and that sub-scan's emitted `__seq_id` / `__batch_id`
    // populate the output (each sub-scan materialises them as of
    // TASK-508 — see `bqlite-operators::scan` module docs). The
    // `__source_table_id` spec type is `Int8`; `BqlType` lacks `Int8`
    // so we use `Int` here (see the constant's doc-comment above).
    cols.push(ColumnDef::required(SOURCE_TABLE_ID_COLUMN, BqlType::Int));
    cols.push(ColumnDef::required(
        bqlite_core::schema::SEQ_ID_COLUMN,
        BqlType::Int,
    ));
    cols.push(ColumnDef::required(
        bqlite_core::schema::BATCH_ID_COLUMN,
        BqlType::Int,
    ));

    let output_schema = OperatorSchema::new(cols)?;

    Ok(LogicalPlan::Scan {
        table: primary,
        time_range,
        reader_backward_ns: 0,
        reader_forward_ns: 0,
        joined_tables: joined,
        scan_predicates: Vec::new(),
        projected_columns: Vec::new(),
        output_schema,
    })
}

/// Fold a single AST pipeline stage into the accumulated plan.
fn fold_stage(
    stage: PipelineStage,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
    catalog: &dyn Catalog,
    source_table: &TableSchema,
    aliases: &AliasTable,
) -> Result<LogicalPlan> {
    match stage {
        PipelineStage::Where { predicate, .. } => {
            lower_where(predicate, acc, registry, catalog, aliases)
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

        PipelineStage::EventSelect(args) => lower_event_select(args, acc, registry),

        PipelineStage::Attribute(args) => lower_attribute(args, acc, registry, source_table),

        // ── Wave 3 desugaring ─────────────────────────────────────────
        // FUNNEL is syntactic sugar that expands into a MATCH (EMIT ALL)
        // followed by a STATS stage. Desugaring is deferred to the
        // planner (not the parser) because the aggregate output names are
        // derived from the step list — a schema-aware operation.
        // See opt::desugar_funnel and planner-pipeline.md §4.3.
        PipelineStage::Funnel(f) => {
            let (match_stage, stats_stage) = crate::opt::desugar_funnel(f)?;
            // Fold the two desugared stages in order (MATCH first, then STATS).
            let after_match =
                fold_stage(match_stage, acc, registry, catalog, source_table, aliases)?;
            fold_stage(
                stats_stage,
                after_match,
                registry,
                catalog,
                source_table,
                aliases,
            )
        }

        // ── Wave 4 desugaring ─────────────────────────────────────────
        // RETENTION is syntactic sugar that expands into a two-step
        // MATCH FIRST … BRACKETS … EMIT ALL followed by a STATS stage
        // with AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket.
        // See opt::desugar_retention and query-language.md §6.3.
        PipelineStage::Retention(r) => {
            let (match_stage, stats_stage) = crate::opt::desugar_retention(r)?;
            let after_match =
                fold_stage(match_stage, acc, registry, catalog, source_table, aliases)?;
            fold_stage(
                stats_stage,
                after_match,
                registry,
                catalog,
                source_table,
                aliases,
            )
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

// ─────────────────────────────────────────────────────────────────────────────
// WHERE lowering — handles plain predicates + cohort-subquery conjuncts
// ─────────────────────────────────────────────────────────────────────────────

/// Lower a `WHERE <predicate>` pipeline stage.
///
/// The top-level conjuncts of the predicate are walked (flattening nested
/// `AND` chains); each conjunct is dispatched on shape:
/// - `<lhs> IN QUERY (<subquery>)` / `(<lhs1>, ..., <lhsN>) IN QUERY (<subquery>)`:
///   the conjunct is lifted into a [`LogicalPlan::SubqueryFilter`] wrapper
///   above `acc`. The subquery is fully lowered via [`lower_query_pipeline`],
///   and arity / positional type compatibility with the LHS tuple is enforced
///   per `docs/design/language/cohorts-aliases-joins.md` §4.
/// - `<lhs> IN <alias>`: deferred to CP4 — produces a `Plan` error in this
///   checkpoint.
/// - Anything else: stays in the residual predicate that becomes a standard
///   [`LogicalPlan::Filter`] on top of all the lifted `SubqueryFilter`s.
///
/// `NOT IN (subquery)` is rejected in v1 — the semantics (NULL propagation
/// rules per BQL §4.3) are non-obvious and the workload hasn't come up.
fn lower_where(
    predicate: Spanned<Expr>,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
    catalog: &dyn Catalog,
    aliases: &AliasTable,
) -> Result<LogicalPlan> {
    // Flatten top-level AND chain into a vec of conjuncts.
    let mut conjuncts: Vec<Spanned<Expr>> = Vec::new();
    flatten_and_conjuncts(predicate, &mut conjuncts);

    // Partition into subquery-filter conjuncts (IN QUERY / IN alias) and
    // residual predicate conjuncts.
    let mut acc = acc;
    let mut residual: Vec<Spanned<Expr>> = Vec::new();
    for conjunct in conjuncts {
        match &conjunct.node {
            Expr::In {
                lhs,
                rhs: InRhs::Query(subq),
                negated,
            } => {
                if *negated {
                    return Err(BqliteError::Plan(
                        "NOT IN (subquery) is not supported in v1 — BQL's three-valued logic \
                         makes NULL-handling ambiguous (type-system.md §4.3); deferred to a later wave"
                            .into(),
                    ));
                }
                let subq_plan = lower_query_pipeline((**subq).clone(), catalog, aliases)?;
                acc = apply_subquery_filter(lhs, subq_plan, acc, registry)?;
            }
            Expr::In {
                lhs,
                rhs: InRhs::Alias(name),
                negated,
            } => {
                if *negated {
                    return Err(BqliteError::Plan(
                        "NOT IN <alias> is not supported in v1 — see the NOT IN (subquery) \
                         note for rationale"
                            .into(),
                    ));
                }
                // `IN alias` references can only appear inside the terminal
                // pipeline (which sees the full alias table) or inside an
                // alias body with source-order awareness. The caller-position
                // check is handled by resolve_alias() — here we just need
                // to pass it the alias table.
                let subq_plan = resolve_alias(&name.text, aliases, catalog)?;
                acc = apply_subquery_filter(lhs, subq_plan, acc, registry)?;
            }
            _ => residual.push(conjunct),
        }
    }

    // If any conjuncts remain, re-combine them via AND (or the single
    // conjunct verbatim) and wrap `acc` in a Filter.
    if let Some(combined) = recombine_and_conjuncts(residual) {
        let typed = TypedExpr::from_ast(&combined, acc.output_schema(), registry)?;
        return LogicalPlan::filter(typed, acc);
    }

    Ok(acc)
}

/// Flatten a predicate expression that may be an `AND` chain into its
/// top-level conjuncts. Preserves ordering. Drills through `Paren` wrappers
/// so that `(a AND b) AND c` flattens to `[a, b, c]` like `a AND b AND c`.
fn flatten_and_conjuncts(pred: Spanned<Expr>, out: &mut Vec<Spanned<Expr>>) {
    match pred.node {
        Expr::And(children) => {
            for child in children {
                flatten_and_conjuncts(child, out);
            }
        }
        Expr::Paren(inner) => flatten_and_conjuncts(*inner, out),
        other => out.push(Spanned::new(other, pred.span)),
    }
}

/// Rebuild a single expression from a list of conjuncts. Returns `None` for
/// an empty list (no residual predicate needed).
fn recombine_and_conjuncts(mut conjuncts: Vec<Spanned<Expr>>) -> Option<Spanned<Expr>> {
    match conjuncts.len() {
        0 => None,
        1 => Some(conjuncts.pop().unwrap()),
        _ => {
            // Re-package as Expr::And, preserving the first conjunct's span
            // as the overall span (diagnostics anchor on the leading term).
            let anchor_span = conjuncts[0].span;
            Some(Spanned::new(Expr::And(conjuncts), anchor_span))
        }
    }
}

/// Resolve an `IN alias <name>` reference against the current alias table,
/// returning the fully-lowered alias body as a [`LogicalPlan`].
///
/// Handles:
/// - Unknown-alias rejection with a hint that references must be in source
///   order.
/// - Forward-reference rejection: aliases referring to other aliases defined
///   later in source order (cohorts-aliases-joins.md §2.3).
/// - Cycle detection via the active-resolution stack (`aliases.path`).
/// - Per-name caching in `aliases.resolved` so repeated references share
///   one lowered plan (still `.clone()`'d into the `SubqueryFilter.subquery`
///   child, but the expensive lowering work runs once).
fn resolve_alias(name: &str, aliases: &AliasTable, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    // Unknown alias.
    let Some(ref_pos) = aliases.position_of(name) else {
        return Err(BqliteError::Plan(format!(
            "alias `{name}` is undefined — all `IN alias` references must name \
             an alias defined earlier in the script (cohorts-aliases-joins.md §2.3)"
        )));
    };

    // Forward-reference check (§2.3): when the reference comes from inside
    // another alias body, the referenced alias must be at a strictly earlier
    // source-order position. Terminal-query references have no active alias
    // on the stack, so they can refer to any defined alias.
    if let Some(current_name) = aliases.path.borrow().last().cloned() {
        if current_name == name {
            return Err(BqliteError::Plan(format!(
                "alias `{name}` cannot reference itself — self-references are a \
                 degenerate cycle (cohorts-aliases-joins.md §2.3–§2.4)"
            )));
        }
        let current_pos = aliases
            .position_of(&current_name)
            .expect("active alias on path must have a definition");
        if ref_pos >= current_pos {
            return Err(BqliteError::Plan(format!(
                "alias `{name}` is referenced from `{current_name}` but defined later — \
                 alias references must resolve in source order (cohorts-aliases-joins.md §2.3)"
            )));
        }
    }

    // Cycle detection (§2.4): if `name` is already on the active resolution
    // stack, this reference closes a cycle.
    //
    // NOTE (agent-2, 2026-04-19): with the strict source-order rule above,
    // transitive cycles (A -> B -> A) are unreachable today — any pair of
    // aliases that could form a cycle would already fail the forward-ref
    // check. The branch is kept as a safety net for future rule relaxations
    // (e.g. hoisted alias scoping).
    {
        let path_ref = aliases.path.borrow();
        if path_ref.iter().any(|n| n == name) {
            let mut full_path: Vec<String> = path_ref.clone();
            full_path.push(name.to_string());
            return Err(BqliteError::Plan(format!(
                "alias cycle detected: {}",
                full_path.join(" -> ")
            )));
        }
    }

    // Cache hit: return the previously-lowered plan.
    if let Some(cached) = aliases.resolved.borrow().get(name) {
        return Ok(cached.clone());
    }

    // Cache miss — lower the alias body with `name` pushed onto the path.
    let (body, _pos) = aliases
        .definitions
        .get(name)
        .expect("position_of checked above implies definitions.get is Some");
    aliases.path.borrow_mut().push(name.to_string());
    let result = lower_query_pipeline(body.clone(), catalog, aliases);
    aliases.path.borrow_mut().pop();
    let plan = result?;
    // Install into cache before returning.
    aliases
        .resolved
        .borrow_mut()
        .insert(name.to_string(), plan.clone());
    Ok(plan)
}

/// Apply a single `<lhs> IN QUERY (subq)` or `<lhs> IN <alias>` conjunct,
/// producing a [`LogicalPlan::SubqueryFilter`] above `acc`. The caller is
/// responsible for lowering the subquery or resolving the alias — this
/// function only performs the wrap + validation.
///
/// Validates:
/// - The subquery's non-system output columns number equals the LHS tuple
///   arity (cohorts-aliases-joins.md §4.1).
/// - Per-column positional types match exactly (no coercion in v1). The BQL
///   `PartialEq` over `BqlType` is the comparison; differing types produce
///   `BqliteError::Plan("IN QUERY column N type mismatch: LHS <a>, subquery <b>")`.
/// - Each LHS expression type-checks against the outer schema.
fn apply_subquery_filter(
    lhs: &[Spanned<Expr>],
    subq_plan: LogicalPlan,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    if lhs.is_empty() {
        return Err(BqliteError::Plan(
            "IN QUERY: LHS must have at least one expression".into(),
        ));
    }

    // Collect the subquery's non-system output columns in declaration order.
    // Cohorts project only user columns — system columns (__seq_id / __batch_id)
    // are filtered out per cohorts-aliases-joins.md §4.1, which states the
    // "subquery produces one row per cohort member" using declared columns.
    let subq_cols: Vec<&ColumnDef> = subq_plan
        .output_schema()
        .columns()
        .iter()
        .filter(|c| !c.is_system())
        .collect();

    if subq_cols.len() != lhs.len() {
        return Err(BqliteError::Plan(format!(
            "IN QUERY arity mismatch: LHS has {} column(s), subquery produces {}",
            lhs.len(),
            subq_cols.len()
        )));
    }

    let outer_schema = acc.output_schema().clone();
    let mut typed_lhs: Vec<TypedExpr> = Vec::with_capacity(lhs.len());
    for (i, (lhs_expr, subq_col)) in lhs.iter().zip(subq_cols.iter()).enumerate() {
        let typed = TypedExpr::from_ast(lhs_expr, &outer_schema, registry)?;
        if typed.result_type != subq_col.bql_type {
            return Err(BqliteError::Plan(format!(
                "IN QUERY column {} type mismatch: LHS `{}`, subquery `{}`",
                i, typed.result_type, subq_col.bql_type
            )));
        }
        typed_lhs.push(typed);
    }

    Ok(LogicalPlan::SubqueryFilter {
        columns: typed_lhs,
        subquery: Box::new(subq_plan),
        input: Box::new(acc),
        output_schema: outer_schema,
    })
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

            SelectItemKind::QualifiedWildcard(table) => {
                // Expand `table.*` by emitting one ProjectItem per column
                // in the combined schema whose name starts with `<table>.`.
                // Only meaningful in joined-source pipelines; in a single-
                // table pipeline the schema has bare names and no matches
                // exist — we surface that as a clear error.
                let prefix = format!("{}.", table.text);
                let mut any_matched = false;
                for (column_index, col) in input_schema.columns().iter().enumerate() {
                    if col.is_system() {
                        continue;
                    }
                    if col.name.starts_with(&prefix) {
                        any_matched = true;
                        project_items.push(ProjectItem {
                            expr: TypedExpr::column(column_index, col, item.span),
                            output_name: col.name.clone(),
                        });
                    }
                }
                if !any_matched {
                    return Err(BqliteError::Plan(format!(
                        "qualified wildcard `{prefix}*` did not match any columns — \
                         `{table_name}` is not a joined source table, or it exposes no user columns",
                        table_name = table.text
                    )));
                }
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

    // 3a. bracket / bracket_end columns when BRACKETS is specified.
    // query-language.md §4.12 eventually requires one row per (entity,
    // binding track, bracket), but the matcher runtime does not yet
    // track brackets (the matcher's `step_counter` module has a single
    // scan-widening reference but no per-bracket emission). Until the
    // matcher learns to enumerate brackets, these columns are
    // advertised as nullable so `MATCH … BRACKETS [..]` (and therefore
    // the RETENTION sugar that desugars to it) can at least complete
    // end-to-end and feed downstream stages without violating Arrow's
    // non-nullable contract. Per-bracket semantics need a matcher
    // change before the nullability can be tightened.
    if pattern.brackets.is_some() {
        output_columns.push(ColumnDef {
            name: "bracket".to_string(),
            bql_type: BqlType::Int,
            nullable: true,
            default_value: None,
        });
        output_columns.push(ColumnDef {
            name: "bracket_end".to_string(),
            bql_type: BqlType::Int,
            nullable: true,
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

    // Validate that BRACKETS durations are strictly ascending (§4.12).
    // Out-of-order durations would produce inverted or overlapping time
    // slices at runtime; reject early with a clear diagnostic.
    if let Some(b) = &brackets {
        for i in 1..b.durations.len() {
            if b.durations[i - 1] >= b.durations[i] {
                return Err(BqliteError::Plan(format!(
                    "BRACKETS durations must be strictly ascending — \
                     got {}ns >= {}ns at positions {} and {}; \
                     example: BRACKETS [7d, 14d, 30d]",
                    b.durations[i - 1],
                    b.durations[i],
                    i - 1,
                    i,
                )));
            }
        }
    }

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

    // Drop planner-only system columns (`__seq_id` / `__batch_id`) from
    // sessionize's advertised output. They live in the planner-level
    // `OperatorSchema::from_table` contract but are not physically
    // produced by `ScanOperator` (see `bqlite-operators::scan` module
    // docs: "Implicit system columns [...] are not included"), so any
    // sessionize output batch would have to null-pad them — which
    // violates their non-nullable contract. Operators that genuinely
    // need `__seq_id` resolve it against the scan's own schema, not
    // against sessionize's output.
    let mut cols: Vec<ColumnDef> = input_schema
        .columns()
        .iter()
        .filter(|c| !c.name.starts_with("__"))
        .cloned()
        .collect();
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
// Wave 4 EventSelect lowering — operators/event-select-sample.md §4–§11
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| FIRST | LAST | NTH <events> [WHERE <predicate>] [lookback: <d>]`
/// into an `EventSelect` logical node.
///
/// Validations:
/// - `kind` passes through the AST mirror 1-for-1; `Nth(n)` enforces `n >= 1`
///   defensively (the parser also enforces).
/// - `events` is non-empty (parser-guaranteed; defensive check).
/// - Duplicate event-type names in `events` are rejected (parser-guaranteed;
///   defensive check).
/// - Optional `predicate` is type-checked against the input schema; its
///   `result_type` must be `BqlType::Bool`.
/// - `lookback` is only valid for FIRST and NTH per §11 (the parser also
///   rejects it on LAST; defensive check here).
///
/// Scan-range extension: when `lookback` is `Some(ns)` and `kind` is FIRST/NTH,
/// the primary scan's reader window is widened backward by `ns` *after* all
/// validations pass, so a rejected lowering never mutates `acc`.
///
/// Output schema equals the input schema (one row per surviving entity).
fn lower_event_select(
    args: bqlite_ast::EventSelect,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    // Narrow the AST kind to the planner mirror, validating the n >= 1
    // invariant defensively (parser also enforces).
    let kind = match args.kind {
        bqlite_ast::EventSelectKind::First => EventSelectKind::First,
        bqlite_ast::EventSelectKind::Last => EventSelectKind::Last,
        bqlite_ast::EventSelectKind::Nth(n) => {
            if n == 0 {
                return Err(BqliteError::Plan(
                    "NTH: position must be >= 1 — got 0".into(),
                ));
            }
            EventSelectKind::Nth(n)
        }
    };

    if args.events.is_empty() {
        return Err(BqliteError::Plan(
            "FIRST/LAST/NTH: event list must have at least one event type".into(),
        ));
    }

    let mut event_types: Vec<String> = Vec::with_capacity(args.events.len());
    for ev in &args.events {
        let name = ev.event.text.clone();
        if event_types.iter().any(|existing| existing == &name) {
            return Err(BqliteError::Plan(format!(
                "FIRST/LAST/NTH: duplicate event type `{name}`"
            )));
        }
        event_types.push(name);
    }

    // lookback: FIRST/NTH only (§11). The parser rejects `lookback:` on LAST,
    // but guard defensively in case a direct AST construction slips through.
    if args.lookback.is_some() && matches!(kind, EventSelectKind::Last) {
        return Err(BqliteError::Plan(
            "LAST does not accept a `lookback:` parameter — use FIRST or NTH".into(),
        ));
    }

    let input_schema = acc.output_schema().clone();

    // Type-check the optional predicate against the input schema.
    let typed_predicate = match args.predicate {
        None => None,
        Some(spanned_expr) => {
            let t = TypedExpr::from_ast(&spanned_expr, &input_schema, registry)?;
            if t.result_type != BqlType::Bool {
                return Err(BqliteError::Plan(format!(
                    "FIRST/LAST/NTH: WHERE predicate must be Bool, got `{}`",
                    t.result_type
                )));
            }
            Some(t)
        }
    };

    // All validation complete — extend the scan backwards if requested.
    let mut acc = acc;
    if let Some(lookback_ns) = args.lookback {
        if lookback_ns > 0 {
            acc.extend_scan_reader_backward(lookback_ns)?;
        }
    }

    // Drop planner-only system columns (`__seq_id` / `__batch_id`) from
    // EventSelect's advertised output. The scan runtime does not
    // physically emit those system columns, and EventSelect does not
    // read their values (same-`ts` tie-breaking uses positional order
    // per the doc on `EventSelectInputMap`). Keeping them in the output
    // schema would force the operator to populate non-nullable columns
    // with no underlying data. See the same treatment in
    // `lower_sessionize`.
    let output_schema = OperatorSchema::new(
        input_schema
            .columns()
            .iter()
            .filter(|c| !c.name.starts_with("__"))
            .cloned()
            .collect(),
    )?;

    Ok(LogicalPlan::EventSelect {
        kind,
        event_types,
        predicate: typed_predicate,
        lookback: args.lookback,
        forwarded_columns: Vec::new(),
        fused_downstream: None,
        input: Box::new(acc),
        output_schema,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 ATTRIBUTE lowering — operators/attribute.md §4–§12
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `| ATTRIBUTE conversion: <e> touchpoints: <e> window: <d>
///   touchpoint_key: <expr>` into an `Attribute` logical node.
///
/// Validations:
/// - `conversion` and `touchpoints` lists are non-empty and contain no
///   duplicates (parser-guaranteed; defensive).
/// - `window >= 0`. Zero is accepted (attribute.md §16.1: a zero-duration
///   window is semantically valid — every conversion LEFT-UNNESTs since no
///   touchpoint can fall in an empty lookback interval).
/// - `touchpoint_key` expression type-checks against the input schema and
///   produces `BqlType::String` (per the grammar note in query-language.md
///   §14.3: "Use `CAST(… AS STRING)` if the source column isn't already a
///   string").
///
/// Scan-range extension: the primary scan's reader window is widened backward
/// by `window` (per `attribute.md` §12). This happens *after* validation so a
/// rejected lowering never mutates `acc`.
///
/// The `conversion_range` field captures the pristine query range
/// `(start_ns, end_ns)` when the primary scan carries a resolvable
/// `BETWEEN <a> AND <b>` range. `LAST <dur>` ranges need `now_ns` to resolve
/// and are handled in the physical layer (see `physical::lower_physical`).
///
/// Output schema (attribute.md §4.1):
///   `entity_id: <entity_key_type> NOT NULL`
///   `conversion_ts: Timestamp NOT NULL`
///   `touchpoint_ts: Timestamp NULL`
///   `touchpoint_key: String NULL`
/// Forwarded conversion properties are added by demand analysis.
fn lower_attribute(
    args: bqlite_ast::Attribute,
    acc: LogicalPlan,
    registry: &FunctionRegistry,
    source_table: &TableSchema,
) -> Result<LogicalPlan> {
    // window >= 0. Zero is semantically valid (every conversion LEFT-UNNESTs
    // since no touchpoint can fall in an empty lookback window); negative
    // windows have no defined semantics and are rejected.
    // Per attribute.md §16.1: "window: 0s — not rejected at plan time."
    if args.window < 0 {
        return Err(BqliteError::Plan(format!(
            "ATTRIBUTE: window must be non-negative — got {}ns",
            args.window
        )));
    }

    // Validate conversion / touchpoint event lists (defensive — parser guarantees).
    if args.conversion.is_empty() {
        return Err(BqliteError::Plan(
            "ATTRIBUTE: conversion must name at least one event type".into(),
        ));
    }
    if args.touchpoints.is_empty() {
        return Err(BqliteError::Plan(
            "ATTRIBUTE: touchpoints must name at least one event type".into(),
        ));
    }
    let mut conversion_events: Vec<String> = Vec::with_capacity(args.conversion.len());
    for r in &args.conversion {
        let n = r.event.text.clone();
        if conversion_events.iter().any(|e| e == &n) {
            return Err(BqliteError::Plan(format!(
                "ATTRIBUTE: duplicate conversion event type `{n}`"
            )));
        }
        conversion_events.push(n);
    }
    let mut touchpoint_events: Vec<String> = Vec::with_capacity(args.touchpoints.len());
    for r in &args.touchpoints {
        let n = r.event.text.clone();
        if touchpoint_events.iter().any(|e| e == &n) {
            return Err(BqliteError::Plan(format!(
                "ATTRIBUTE: duplicate touchpoint event type `{n}`"
            )));
        }
        touchpoint_events.push(n);
    }

    let input_schema = acc.output_schema().clone();

    // Type-check touchpoint_key.
    let typed_key = TypedExpr::from_ast(&args.touchpoint_key, &input_schema, registry)?;
    if typed_key.result_type != BqlType::String {
        return Err(BqliteError::Plan(format!(
            "ATTRIBUTE: touchpoint_key must evaluate to String, got `{}`",
            typed_key.result_type
        )));
    }

    // Build the output schema per §4.1. `forwarded_conversion_columns` is
    // empty at construction — demand analysis may later splice columns
    // between `conversion_ts` and `touchpoint_ts`.
    let entity_key_type = source_table.entity_key_column().bql_type.clone();
    let entity_key_name = source_table.entity_key_column().name.clone();
    let out_cols: Vec<ColumnDef> = vec![
        ColumnDef {
            name: entity_key_name,
            bql_type: entity_key_type,
            nullable: false,
            default_value: None,
        },
        ColumnDef::required("conversion_ts", BqlType::Timestamp),
        ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
        ColumnDef::nullable("touchpoint_key", BqlType::String),
    ];
    let output_schema = OperatorSchema::new(out_cols)?;

    // Capture the pristine query range if it is a BETWEEN (LAST is resolved
    // at physical-lowering time when now_ns is available).
    let conversion_range = attribute_conversion_range(&acc);

    // All validation passed — extend scan backward by window.
    let mut acc = acc;
    acc.extend_scan_reader_backward(args.window)?;

    Ok(LogicalPlan::Attribute {
        conversion_events,
        touchpoint_events,
        window: args.window,
        touchpoint_key: typed_key,
        forwarded_conversion_columns: Vec::new(),
        fused_downstream: None,
        conversion_range,
        input: Box::new(acc),
        output_schema,
    })
}

/// Walk the logical plan until the primary `Scan` is found and return the
/// pristine query range `(start_ns, end_ns)` when it carries a
/// `BETWEEN <start> AND <end>` clause. Returns `None` for unbounded scans
/// and for `LAST <dur>` ranges — the latter need `now_ns` and are resolved
/// in the physical layer instead.
fn attribute_conversion_range(plan: &LogicalPlan) -> Option<(i64, i64)> {
    let scan = find_primary_scan(plan)?;
    match scan {
        LogicalPlan::Scan {
            time_range: Some(TimeRange::Between { start, end }),
            ..
        } => {
            // Reuse the helper in `physical.rs`'s lowering path by re-parsing
            // the same RFC-3339 timestamps here. Duplicating a tiny parse call
            // avoids a logical↔physical dependency inversion.
            let start_ts = parse_time_range_timestamp(start, "ATTRIBUTE BETWEEN start").ok()?;
            // The physical layer models `BETWEEN` as `[start, end+1)`.
            let end_ts = parse_time_range_timestamp(end, "ATTRIBUTE BETWEEN end").ok()?;
            let end_exclusive = end_ts
                .checked_add_nanos(1)
                .unwrap_or(bqlite_core::Timestamp::MAX);
            Some((start_ts.as_nanos(), end_exclusive.as_nanos()))
        }
        _ => None,
    }
}

/// Walk downward through pipeline wrappers to the primary `Scan` node, or
/// return `None` if the plan is a DDL / DML shape with no scan under it.
/// Visits the `input` child of every stateful/relational wrapper; does not
/// descend into `SubqueryFilter.subquery` (that is a cohort, not the outer
/// pipeline's primary source).
///
/// Shared with `crate::physical` so the scan-range extension walker and the
/// `conversion_range` resolver agree on which wrappers are traversed.
pub(crate) fn find_primary_scan(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Scan { .. } => Some(plan),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Sessionize { input, .. }
        | LogicalPlan::EventSelect { input, .. }
        | LogicalPlan::Sample { input, .. }
        | LogicalPlan::SubqueryFilter { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Distinct { input, .. }
        | LogicalPlan::SequenceMatch { input, .. }
        | LogicalPlan::Attribute { input, .. } => find_primary_scan(input),
        LogicalPlan::Explain { plan: child, .. } => find_primary_scan(child),
        _ => None,
    }
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
// DML lowering — DELETE (TASK-453, deletes.md §3 / §4)
// ─────────────────────────────────────────────────────────────────────────────

/// Lower `DELETE FROM <table> WHERE <pred> [ALLOW SCAN]`.
///
/// Resolves the target table, then runs the predicate through the
/// cheap-class classifier. Non-cheap predicates without `ALLOW SCAN`
/// surface as a `BqliteError::Plan` containing the SS4 suggestion
/// text so users see the recommended fix.
fn lower_delete(stmt: bqlite_ast::DeleteStmt, catalog: &dyn Catalog) -> Result<LogicalPlan> {
    let table = catalog.resolve_table(&stmt.table.text)?;
    let registry = FunctionRegistry::with_builtins();
    let filter = classify_delete_predicate(&stmt.predicate, &table, stmt.allow_scan, &registry)?;
    Ok(LogicalPlan::Delete {
        table,
        filter,
        allow_scan: stmt.allow_scan,
        output_schema: empty_output_schema(),
    })
}

/// Classify a DELETE predicate per `deletes.md` §3.
///
/// Returns [`DeleteFilter::Cheap`] when the predicate is a
/// conjunction of allowlisted terms (entity equality / IN, `__seq_id`
/// equality / IN, `__batch_id` equality / IN, time-range
/// comparisons, or `BETWEEN`). Returns
/// [`DeleteFilter::AllowScan`] when the predicate is non-cheap and
/// `allow_scan == true`. Returns `BqliteError::Plan` when the
/// predicate is non-cheap and `allow_scan == false`.
pub fn classify_delete_predicate(
    predicate: &Spanned<Expr>,
    table: &TableSchema,
    allow_scan: bool,
    registry: &FunctionRegistry,
) -> Result<DeleteFilter> {
    let entity_key_col = &table.entity_key_column().name;
    let entity_key_type = table.entity_key_column().bql_type.clone();
    let ts_col = &table.timestamp_column().name;

    let mut spec = ClassifierState::default();
    let mut conjuncts: Vec<&Spanned<Expr>> = Vec::new();
    flatten_top_level_and(predicate, &mut conjuncts);

    let mut all_cheap = true;
    for conjunct in conjuncts {
        match classify_conjunct(
            conjunct,
            entity_key_col,
            &entity_key_type,
            ts_col,
            &mut spec,
        ) {
            Ok(()) => {}
            Err(ClassifierError::NotCheap) => {
                all_cheap = false;
                break;
            }
            Err(ClassifierError::Plan(msg)) => return Err(BqliteError::Plan(msg)),
        }
    }

    if all_cheap {
        if let Some(spec) = spec.into_cheap_spec()? {
            return Ok(DeleteFilter::Cheap(spec));
        }
        // Empty cheap-spec — predicate parsed but produced no
        // tombstone-bearing terms (e.g. `WHERE 1 = 1` collapsed to
        // nothing). Fall through to non-cheap rejection.
    }

    if !allow_scan {
        return Err(BqliteError::Plan(format!(
            "DELETE FROM `{}`: predicate is not in the cheap class and would \
             require a full table scan. Use ALLOW SCAN at the end of the \
             statement to opt in.",
            table.name()
        )));
    }

    let typed = TypedExpr::from_ast(predicate, &OperatorSchema::from_table(table), registry)?;
    if typed.result_type != BqlType::Bool {
        return Err(BqliteError::Plan(format!(
            "DELETE FROM `{}`: WHERE predicate must have type `Bool`, got `{}`",
            table.name(),
            typed.result_type
        )));
    }
    Ok(DeleteFilter::AllowScan { predicate: typed })
}

/// Working state for the cheap-class classifier.
///
/// Accumulates per-granularity terms across conjuncts; `into_cheap_spec`
/// validates the cross-granularity rules (entity+row/batch is the
/// only allowed mix) and emits the final [`CheapDeleteSpec`].
#[derive(Debug, Default)]
struct ClassifierState {
    entity_keys: Vec<ScalarValue>,
    seq_ids: Vec<u64>,
    batch_ids: Vec<u64>,
    /// Time-range partial bounds — collapsed into a single
    /// `TimeRangeSpec` at the end. Tracks both lower and upper
    /// bounds independently so multiple comparisons or `BETWEEN`
    /// fold into one final range.
    time_min: Option<(i64, bool)>, // (value, inclusive)
    time_max: Option<(i64, bool)>,
}

#[derive(Debug)]
enum ClassifierError {
    /// Predicate term is outside the cheap-class allowlist; caller
    /// decides whether to fall back to ALLOW SCAN or reject.
    NotCheap,
    /// Predicate term is structurally malformed in a way the
    /// classifier can localize (e.g., `IN ()` empty list).
    Plan(String),
}

impl ClassifierState {
    fn into_cheap_spec(self) -> Result<Option<CheapDeleteSpec>> {
        let has_entity = !self.entity_keys.is_empty();
        let has_seq = !self.seq_ids.is_empty();
        let has_batch = !self.batch_ids.is_empty();
        let time_range = collapse_time_range(self.time_min, self.time_max);
        let has_time = time_range.is_some();

        // Cross-granularity rules:
        // - time-range with anything else (not even entity-as-shard-target):
        //   not cheap, since shard targeting is meaningless for time-range
        //   tombstones (they apply to all shards in the file's window) and
        //   the design doc only carves out entity+seq_id / entity+batch_id.
        if has_time && (has_entity || has_seq || has_batch) {
            return Err(BqliteError::Plan(
                "DELETE: time-range predicate cannot be combined with other \
                 granularities (entity, __seq_id, __batch_id) — see \
                 docs/design/storage/deletes.md §3.2"
                    .into(),
            ));
        }
        if has_seq && has_batch {
            return Err(BqliteError::Plan(
                "DELETE: __seq_id and __batch_id predicates cannot be combined \
                 in a single cheap-class DELETE — they live in different \
                 tombstone granularities (deletes.md §3.2)"
                    .into(),
            ));
        }

        // Now classify the entity role.
        let entity_role = if has_entity && (has_seq || has_batch) {
            EntityRole::AsShardTarget
        } else {
            EntityRole::AsTombstone
        };

        let any_tombstone = (matches!(entity_role, EntityRole::AsTombstone) && has_entity)
            || has_seq
            || has_batch
            || has_time;
        if !any_tombstone {
            return Ok(None);
        }

        Ok(Some(CheapDeleteSpec {
            entity_keys: self.entity_keys,
            entity_role,
            seq_ids: self.seq_ids,
            batch_ids: self.batch_ids,
            time_range,
        }))
    }
}

/// Collapse independent (min, max) bound observations into a single
/// `TimeRangeSpec`. Returns `None` when no bound was recorded.
fn collapse_time_range(
    min: Option<(i64, bool)>,
    max: Option<(i64, bool)>,
) -> Option<TimeRangeSpec> {
    if min.is_none() && max.is_none() {
        return None;
    }
    Some(TimeRangeSpec {
        min_ts: min.map(|(v, _)| v),
        min_inclusive: min.map(|(_, i)| i).unwrap_or(false),
        max_ts: max.map(|(v, _)| v),
        max_inclusive: max.map(|(_, i)| i).unwrap_or(false),
    })
}

/// Flatten a top-level `Expr::And` chain into a flat conjunct list.
///
/// Recurses through nested `Expr::And` nodes; any non-`And`
/// expression is appended as its own single conjunct. The output
/// vec preserves the source-order of conjuncts.
fn flatten_top_level_and<'a>(spanned: &'a Spanned<Expr>, out: &mut Vec<&'a Spanned<Expr>>) {
    match &spanned.node {
        Expr::And(items) => {
            for item in items {
                flatten_top_level_and(item, out);
            }
        }
        _ => out.push(spanned),
    }
}

/// Classify a single conjunct.
fn classify_conjunct(
    conjunct: &Spanned<Expr>,
    entity_key_col: &str,
    entity_key_type: &BqlType,
    ts_col: &str,
    spec: &mut ClassifierState,
) -> std::result::Result<(), ClassifierError> {
    let inner = unwrap_paren(&conjunct.node);
    match inner {
        Expr::Compare { op, left, right } => classify_compare(
            *op,
            left,
            right,
            entity_key_col,
            entity_key_type,
            ts_col,
            spec,
        ),
        Expr::In {
            lhs,
            rhs,
            negated: false,
        } if lhs.len() == 1 => classify_in(&lhs[0], rhs, entity_key_col, entity_key_type, spec),
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => classify_between(expr, low, high, ts_col, spec),
        _ => Err(ClassifierError::NotCheap),
    }
}

/// Strip a single layer of `Expr::Paren` so `(foo = bar)` matches
/// the same arms as `foo = bar`.
fn unwrap_paren(expr: &Expr) -> &Expr {
    match expr {
        Expr::Paren(inner) => unwrap_paren(&inner.node),
        other => other,
    }
}

fn classify_compare(
    op: bqlite_ast::expr::CompareOp,
    left: &Spanned<Expr>,
    right: &Spanned<Expr>,
    entity_key_col: &str,
    entity_key_type: &BqlType,
    ts_col: &str,
    spec: &mut ClassifierState,
) -> std::result::Result<(), ClassifierError> {
    use bqlite_ast::expr::CompareOp as Op;

    let (col_name, lit, op) = match (unwrap_paren(&left.node), unwrap_paren(&right.node)) {
        (Expr::Column(name), Expr::Literal(lit)) => (name.text.as_str(), lit, op),
        // Reverse direction: `<lit> <op> <col>` flips to `<col> flipped(op) <lit>`.
        (Expr::Literal(lit), Expr::Column(name)) => (name.text.as_str(), lit, flip_compare(op)),
        _ => return Err(ClassifierError::NotCheap),
    };

    if matches!(op, Op::NotEqual) {
        return Err(ClassifierError::NotCheap);
    }

    if col_name == bqlite_core::SEQ_ID_COLUMN {
        if !matches!(op, Op::Equal) {
            return Err(ClassifierError::NotCheap);
        }
        let id = literal_as_u64(lit, "__seq_id")?;
        spec.seq_ids.push(id);
        return Ok(());
    }
    if col_name == bqlite_core::BATCH_ID_COLUMN {
        if !matches!(op, Op::Equal) {
            return Err(ClassifierError::NotCheap);
        }
        let id = literal_as_u64(lit, "__batch_id")?;
        spec.batch_ids.push(id);
        return Ok(());
    }
    if col_name == ts_col {
        let ns = literal_as_timestamp(lit)?;
        return record_time_bound(op, ns, spec);
    }
    if col_name == entity_key_col {
        if !matches!(op, Op::Equal) {
            return Err(ClassifierError::NotCheap);
        }
        let value = literal_as_entity_key(lit, entity_key_type)?;
        spec.entity_keys.push(value);
        return Ok(());
    }
    Err(ClassifierError::NotCheap)
}

fn classify_in(
    lhs: &Spanned<Expr>,
    rhs: &bqlite_ast::expr::InRhs,
    entity_key_col: &str,
    entity_key_type: &BqlType,
    spec: &mut ClassifierState,
) -> std::result::Result<(), ClassifierError> {
    let col_name = match unwrap_paren(&lhs.node) {
        Expr::Column(name) => name.text.as_str(),
        _ => return Err(ClassifierError::NotCheap),
    };
    let items = match rhs {
        bqlite_ast::expr::InRhs::List(items) => items,
        // IN QUERY / IN <alias> are subqueries — not cheap.
        _ => return Err(ClassifierError::NotCheap),
    };
    if items.is_empty() {
        return Err(ClassifierError::Plan(format!(
            "DELETE: empty IN-list for column `{col_name}` is not a valid \
             cheap-class predicate"
        )));
    }

    if col_name == bqlite_core::SEQ_ID_COLUMN {
        for item in items {
            let lit = literal_or_not_cheap(&item.node)?;
            spec.seq_ids.push(literal_as_u64(lit, "__seq_id")?);
        }
        return Ok(());
    }
    if col_name == bqlite_core::BATCH_ID_COLUMN {
        for item in items {
            let lit = literal_or_not_cheap(&item.node)?;
            spec.batch_ids.push(literal_as_u64(lit, "__batch_id")?);
        }
        return Ok(());
    }
    if col_name == entity_key_col {
        for item in items {
            let lit = literal_or_not_cheap(&item.node)?;
            spec.entity_keys
                .push(literal_as_entity_key(lit, entity_key_type)?);
        }
        return Ok(());
    }
    Err(ClassifierError::NotCheap)
}

fn classify_between(
    expr: &Spanned<Expr>,
    low: &Spanned<Expr>,
    high: &Spanned<Expr>,
    ts_col: &str,
    spec: &mut ClassifierState,
) -> std::result::Result<(), ClassifierError> {
    let col_name = match unwrap_paren(&expr.node) {
        Expr::Column(name) => name.text.as_str(),
        _ => return Err(ClassifierError::NotCheap),
    };
    if col_name != ts_col {
        return Err(ClassifierError::NotCheap);
    }
    let low_lit = literal_or_not_cheap(&low.node)?;
    let high_lit = literal_or_not_cheap(&high.node)?;
    let low_ns = literal_as_timestamp(low_lit)?;
    let high_ns = literal_as_timestamp(high_lit)?;
    record_time_bound(bqlite_ast::expr::CompareOp::GreaterOrEqual, low_ns, spec)?;
    record_time_bound(bqlite_ast::expr::CompareOp::LessOrEqual, high_ns, spec)?;
    Ok(())
}

fn record_time_bound(
    op: bqlite_ast::expr::CompareOp,
    ns: i64,
    spec: &mut ClassifierState,
) -> std::result::Result<(), ClassifierError> {
    use bqlite_ast::expr::CompareOp as Op;
    match op {
        Op::Less => {
            // ts < ns ⇒ upper bound exclusive at ns.
            tighten_max(&mut spec.time_max, ns, false);
        }
        Op::LessOrEqual => {
            tighten_max(&mut spec.time_max, ns, true);
        }
        Op::Greater => {
            tighten_min(&mut spec.time_min, ns, false);
        }
        Op::GreaterOrEqual => {
            tighten_min(&mut spec.time_min, ns, true);
        }
        Op::Equal | Op::NotEqual => return Err(ClassifierError::NotCheap),
    }
    Ok(())
}

/// Pick the tighter (larger) lower bound when two are observed.
///
/// `ts >= a AND ts >= b` collapses to `ts >= max(a, b)`. When the
/// values are equal, the **exclusive** bound wins because conjunction
/// takes the intersection — `ts > 100 AND ts >= 100` admits exactly
/// the same rows as `ts > 100`, so the tighter (exclusive) form is
/// the canonical representation.
fn tighten_min(slot: &mut Option<(i64, bool)>, ns: i64, inclusive: bool) {
    match slot {
        None => *slot = Some((ns, inclusive)),
        Some((cur, cur_inc)) => {
            if ns > *cur || (ns == *cur && !inclusive && *cur_inc) {
                *slot = Some((ns, inclusive));
            }
        }
    }
}

/// Pick the tighter (smaller) upper bound when two are observed.
///
/// Symmetric to [`tighten_min`]: `ts <= a AND ts < a` collapses to
/// `ts < a` (exclusive wins on equal values for the same intersection
/// reason).
fn tighten_max(slot: &mut Option<(i64, bool)>, ns: i64, inclusive: bool) {
    match slot {
        None => *slot = Some((ns, inclusive)),
        Some((cur, cur_inc)) => {
            if ns < *cur || (ns == *cur && !inclusive && *cur_inc) {
                *slot = Some((ns, inclusive));
            }
        }
    }
}

fn flip_compare(op: bqlite_ast::expr::CompareOp) -> bqlite_ast::expr::CompareOp {
    use bqlite_ast::expr::CompareOp as Op;
    match op {
        Op::Equal => Op::Equal,
        Op::NotEqual => Op::NotEqual,
        Op::Less => Op::Greater,
        Op::LessOrEqual => Op::GreaterOrEqual,
        Op::Greater => Op::Less,
        Op::GreaterOrEqual => Op::LessOrEqual,
    }
}

fn literal_or_not_cheap(expr: &Expr) -> std::result::Result<&Literal, ClassifierError> {
    match unwrap_paren(expr) {
        Expr::Literal(lit) => Ok(lit),
        _ => Err(ClassifierError::NotCheap),
    }
}

fn literal_as_u64(lit: &Literal, column: &str) -> std::result::Result<u64, ClassifierError> {
    match lit {
        Literal::Int(n) if *n >= 0 => Ok(*n as u64),
        Literal::Int(n) => Err(ClassifierError::Plan(format!(
            "DELETE: `{column}` literal must be a non-negative integer, got {n}"
        ))),
        _ => Err(ClassifierError::Plan(format!(
            "DELETE: `{column}` literal must be an integer, got {lit:?}"
        ))),
    }
}

fn literal_as_timestamp(lit: &Literal) -> std::result::Result<i64, ClassifierError> {
    match lit {
        Literal::Timestamp(ns) => Ok(*ns),
        // Plain integer literals in nanoseconds are accepted for
        // ergonomics — many test queries hand-code epoch-ns values.
        // The parser preserves the type-tag through `Literal::Timestamp`
        // when an `@<rfc3339>` literal is used; bare integers reach
        // here as `Literal::Int`.
        Literal::Int(ns) => Ok(*ns),
        _ => Err(ClassifierError::Plan(format!(
            "DELETE: timestamp literal expected for time-range predicate, got {lit:?}"
        ))),
    }
}

fn literal_as_entity_key(
    lit: &Literal,
    entity_key_type: &BqlType,
) -> std::result::Result<ScalarValue, ClassifierError> {
    match (lit, entity_key_type) {
        (Literal::String(s), BqlType::String) => Ok(ScalarValue::String(s.clone())),
        (Literal::Int(n), BqlType::Int) => Ok(ScalarValue::Int(*n)),
        _ => Err(ClassifierError::Plan(format!(
            "DELETE: entity-key literal type does not match column type \
             ({entity_key_type:?})"
        ))),
    }
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
    fn join_unknown_table_rejected_via_catalog() {
        // Post-CP5a: source JOINs are supported, but each joined table must
        // exist in the catalog. An unknown target surfaces the standard
        // unknown-table error rather than a blanket "JOINs deferred" message.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("other"),
            span: Span::EMPTY,
        });
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("other"), "got: {msg}");
                assert!(msg.contains("unknown table"), "got: {msg}");
            }
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
    fn delete_unknown_table_via_lower_statement() {
        // After TASK-453, DELETE is no longer deferred — `lower_statement`
        // dispatches to `lower_delete`, which surfaces unknown-table
        // errors via the catalog.
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
            BqliteError::Plan(msg) => assert!(msg.contains("t"), "got: {msg}"),
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

    // ── Wave 4: RETENTION desugaring (integration through lower_statement) ──
    //
    // These tests verify that `PipelineStage::Retention` flowing through
    // `fold_stage` → `desugar_retention` → `fold_stage(Match)` → `fold_stage(Stats)`
    // produces the correct LogicalPlan tree.  Pure unit tests for the AST rewrite
    // live in `opt::desugar_retention::tests`.

    #[test]
    fn retention_lowers_to_avg_aggregate_over_sequence_match_with_brackets() {
        use bqlite_ast::pattern::BracketSpec;
        use bqlite_ast::Retention;
        use bqlite_core::AggFunction;

        let cat = InMemoryCatalog::default().with(purchases_schema());

        let entry = bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic("signup"),
            span: Span::EMPTY,
        };
        let activity = bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic("purchase"),
            span: Span::EMPTY,
        };
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Retention(Retention {
                entry,
                activity,
                brackets: BracketSpec {
                    durations: vec![
                        86_400_000_000_000,      // 1d
                        7 * 86_400_000_000_000,  // 7d
                        30 * 86_400_000_000_000, // 30d
                    ],
                    cumulative: false,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();

        // Outer node: Aggregate with GROUP BY bracket.
        let LogicalPlan::Aggregate {
            aggregates,
            group_by,
            input,
            ..
        } = plan
        else {
            panic!("expected Aggregate at top level, got {plan:?}");
        };

        // GROUP BY bracket (single key).
        assert_eq!(group_by.len(), 1, "RETENTION must GROUP BY bracket");

        // One aggregate: retention_rate = AVG(…).
        assert_eq!(aggregates.len(), 1, "RETENTION produces one aggregate");
        assert_eq!(aggregates[0].output_name, "retention_rate");
        assert_eq!(aggregates[0].function, AggFunction::Avg);

        // Inner node: SequenceMatch with emit_all=true and brackets set.
        let LogicalPlan::SequenceMatch {
            emit_all,
            window,
            brackets,
            output_schema,
            ..
        } = *input
        else {
            panic!("expected SequenceMatch inside Aggregate, got {input:?}");
        };
        assert!(emit_all, "RETENTION MATCH must have emit_all = true");
        assert!(
            window.is_none(),
            "BRACKETS and WITHIN are mutually exclusive"
        );

        let brackets = brackets.expect("brackets must be present on RETENTION match");
        assert_eq!(brackets.durations.len(), 3);
        assert!(!brackets.cumulative);

        assert!(
            output_schema.column("step_reached").is_some(),
            "emit_all → step_reached column present"
        );
        assert!(
            output_schema.column("bracket").is_some(),
            "brackets → bracket column present"
        );
    }

    #[test]
    fn retention_cumulative_brackets_propagated() {
        use bqlite_ast::pattern::BracketSpec;
        use bqlite_ast::Retention;

        let cat = InMemoryCatalog::default().with(purchases_schema());

        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Retention(Retention {
                entry: bqlite_ast::pattern::EventRef {
                    table: None,
                    event: Name::synthetic("signup"),
                    span: Span::EMPTY,
                },
                activity: bqlite_ast::pattern::EventRef {
                    table: None,
                    event: Name::synthetic("purchase"),
                    span: Span::EMPTY,
                },
                brackets: BracketSpec {
                    durations: vec![86_400_000_000_000, 7 * 86_400_000_000_000],
                    cumulative: true,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            })],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();

        let LogicalPlan::Aggregate { input, .. } = plan else {
            panic!("expected Aggregate");
        };
        let LogicalPlan::SequenceMatch { brackets, .. } = *input else {
            panic!("expected SequenceMatch");
        };
        let brackets = brackets.expect("brackets must be set");
        assert!(
            brackets.cumulative,
            "cumulative flag must propagate from Retention to SequenceMatch"
        );
    }

    #[test]
    fn brackets_descending_order_rejected() {
        // BRACKETS via RETENTION sugar with reversed durations [30d, 7d, 1d]
        // must be a plan error. The validation fires in lower_match after the
        // RETENTION desugar pass forwards the BracketSpec through.
        use bqlite_ast::pattern::BracketSpec;
        use bqlite_ast::Retention;

        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_1d = 86_400_000_000_000_i64;
        let entry = bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic("signup"),
            span: Span::EMPTY,
        };
        let activity = bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic("purchase"),
            span: Span::EMPTY,
        };
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Retention(Retention {
                entry,
                activity,
                brackets: BracketSpec {
                    // descending: 30d, 7d, 1d — must be rejected
                    durations: vec![30 * ns_1d, 7 * ns_1d, ns_1d],
                    cumulative: false,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(ref msg) if msg.contains("strictly ascending")),
            "expected 'strictly ascending' plan error, got {err:?}"
        );
    }

    #[test]
    fn brackets_equal_adjacent_rejected() {
        // BRACKETS [7d, 7d, 30d] via RETENTION sugar — equal adjacent
        // durations are also not strictly ascending and must be rejected.
        use bqlite_ast::pattern::BracketSpec;
        use bqlite_ast::Retention;

        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_7d = 7 * 86_400_000_000_000_i64;
        let ns_30d = 30 * 86_400_000_000_000_i64;
        let entry = bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic("signup"),
            span: Span::EMPTY,
        };
        let activity = bqlite_ast::pattern::EventRef {
            table: None,
            event: Name::synthetic("purchase"),
            span: Span::EMPTY,
        };
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Retention(Retention {
                entry,
                activity,
                brackets: BracketSpec {
                    // equal adjacent: [7d, 7d, 30d]
                    durations: vec![ns_7d, ns_7d, ns_30d],
                    cumulative: false,
                    span: Span::EMPTY,
                },
                span: Span::EMPTY,
            })],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(ref msg) if msg.contains("strictly ascending")),
            "expected 'strictly ascending' plan error, got {err:?}"
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
                // Declared input columns (minus planner-only `__seq_id` /
                // `__batch_id` system columns, which sessionize drops from
                // its advertised output because the scan runtime does not
                // physically emit them) followed by session_id +
                // session_duration.
                assert_eq!(
                    names,
                    vec![
                        "user_id",
                        "ts",
                        "event",
                        "amount",
                        "country",
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

    // ── Wave 4 CP2: EventSelect + Attribute lowering ───────────────────────

    fn event_select_stage(
        kind: bqlite_ast::EventSelectKind,
        events: Vec<&str>,
        lookback: Option<i64>,
        predicate: Option<Spanned<Expr>>,
    ) -> PipelineStage {
        PipelineStage::EventSelect(bqlite_ast::EventSelect {
            kind,
            events: events.into_iter().map(event_ref).collect(),
            predicate,
            lookback,
            span: Span::EMPTY,
        })
    }

    #[test]
    fn event_select_first_lowers_with_lookback_extends_scan() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_1d: i64 = 86_400 * 1_000_000_000;
        let ns_2h: i64 = 2 * 3_600 * 1_000_000_000;
        // Construct a pipeline whose source has `LAST 1d` + `| FIRST(purchase, lookback: 2h)`.
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.time_range = Some(TimeRange::Last(ns_1d));
        pipeline.stages.push(event_select_stage(
            bqlite_ast::EventSelectKind::First,
            vec!["purchase"],
            Some(ns_2h),
            None,
        ));
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::EventSelect {
                kind,
                event_types,
                predicate,
                lookback,
                forwarded_columns,
                fused_downstream,
                input,
                output_schema,
            } => {
                assert_eq!(kind, EventSelectKind::First);
                assert_eq!(event_types, vec!["purchase"]);
                assert!(predicate.is_none());
                assert_eq!(lookback, Some(ns_2h));
                assert!(forwarded_columns.is_empty());
                assert!(fused_downstream.is_none());
                match *input {
                    LogicalPlan::Scan {
                        time_range,
                        reader_backward_ns,
                        ..
                    } => {
                        assert_eq!(time_range, Some(TimeRange::Last(ns_1d)));
                        assert_eq!(reader_backward_ns, ns_2h);
                    }
                    other => panic!("expected Scan under EventSelect, got {other:?}"),
                }
                // Input's declared columns (purchases_schema has 5:
                // user_id, ts, event, amount, country). The system
                // columns `__seq_id` / `__batch_id` are dropped from
                // EventSelect's advertised output — see
                // `lower_event_select`.
                assert_eq!(output_schema.columns().len(), 5);
            }
            other => panic!("expected EventSelect, got {other:?}"),
        }
    }

    #[test]
    fn event_select_last_without_lookback_lowers_cleanly() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::Last,
                vec!["logout"],
                None,
                None,
            )],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::EventSelect {
                kind,
                event_types,
                lookback,
                input,
                ..
            } => {
                assert_eq!(kind, EventSelectKind::Last);
                assert_eq!(event_types, vec!["logout"]);
                assert!(lookback.is_none());
                if let LogicalPlan::Scan {
                    reader_backward_ns, ..
                } = *input
                {
                    assert_eq!(reader_backward_ns, 0);
                }
            }
            other => panic!("expected EventSelect, got {other:?}"),
        }
    }

    #[test]
    fn event_select_last_with_lookback_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::Last,
                vec!["logout"],
                Some(3_600_000_000_000),
                None,
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(msg) if msg.contains("LAST does not accept a `lookback:`"))
        );
    }

    #[test]
    fn event_select_nth_zero_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::Nth(0),
                vec!["purchase"],
                None,
                None,
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be >= 1")));
    }

    #[test]
    fn event_select_nth_positive_lowers_cleanly() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::Nth(3),
                vec!["purchase"],
                None,
                None,
            )],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        assert!(matches!(
            plan,
            LogicalPlan::EventSelect {
                kind: EventSelectKind::Nth(3),
                ..
            }
        ));
    }

    #[test]
    fn event_select_predicate_non_bool_rejected() {
        // `FIRST(purchase WHERE amount)` — amount is Float, not Bool.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::First,
                vec!["purchase"],
                None,
                Some(column_expr("amount")),
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must be Bool")));
    }

    #[test]
    fn event_select_duplicate_event_type_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::First,
                vec!["purchase", "purchase"],
                None,
                None,
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("duplicate event type")));
    }

    #[test]
    fn event_select_output_schema_matches_input_minus_system_cols() {
        // EventSelect's advertised output equals the input's declared
        // columns; the planner-only `__seq_id` / `__batch_id` system
        // columns are stripped — see `lower_event_select`.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![event_select_stage(
                bqlite_ast::EventSelectKind::First,
                vec!["purchase"],
                None,
                None,
            )],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        let LogicalPlan::EventSelect {
            input,
            output_schema,
            ..
        } = plan
        else {
            panic!("expected EventSelect");
        };
        let expected_names: Vec<&str> = input
            .output_schema()
            .columns()
            .iter()
            .filter(|c| !c.name.starts_with("__"))
            .map(|c| c.name.as_str())
            .collect();
        let actual_names: Vec<&str> = output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(actual_names, expected_names);
    }

    fn attribute_stage(
        window: i64,
        conversion: Vec<&str>,
        touchpoints: Vec<&str>,
        key_column: &str,
    ) -> PipelineStage {
        PipelineStage::Attribute(bqlite_ast::Attribute {
            conversion: conversion.into_iter().map(event_ref).collect(),
            touchpoints: touchpoints.into_iter().map(event_ref).collect(),
            window,
            touchpoint_key: column_expr(key_column),
            span: Span::EMPTY,
        })
    }

    #[test]
    fn attribute_lowers_with_window_extends_scan() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_1d: i64 = 86_400 * 1_000_000_000;
        let ns_7d: i64 = 7 * ns_1d;
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.time_range = Some(TimeRange::Last(ns_1d));
        pipeline.stages.push(attribute_stage(
            ns_7d,
            vec!["purchase"],
            vec!["ad_click"],
            "country",
        ));
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Attribute {
                conversion_events,
                touchpoint_events,
                window,
                forwarded_conversion_columns,
                fused_downstream,
                conversion_range,
                input,
                output_schema,
                ..
            } => {
                assert_eq!(conversion_events, vec!["purchase"]);
                assert_eq!(touchpoint_events, vec!["ad_click"]);
                assert_eq!(window, ns_7d);
                assert!(forwarded_conversion_columns.is_empty());
                assert!(fused_downstream.is_none());
                // `LAST` ranges are resolved at physical-lowering time, not here.
                assert!(conversion_range.is_none());
                // The underlying scan got widened backwards by the window.
                if let LogicalPlan::Scan {
                    reader_backward_ns, ..
                } = *input
                {
                    assert_eq!(reader_backward_ns, ns_7d);
                }
                // Output schema: user_id, conversion_ts, touchpoint_ts, touchpoint_key.
                let names: Vec<&str> = output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(
                    names,
                    vec![
                        "user_id",
                        "conversion_ts",
                        "touchpoint_ts",
                        "touchpoint_key"
                    ]
                );
            }
            other => panic!("expected Attribute, got {other:?}"),
        }
    }

    #[test]
    fn attribute_with_between_captures_conversion_range() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_7d: i64 = 7 * 86_400 * 1_000_000_000;
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.time_range = Some(TimeRange::Between {
            start: "2024-01-01T00:00:00Z".into(),
            end: "2024-01-31T23:59:59Z".into(),
        });
        pipeline.stages.push(attribute_stage(
            ns_7d,
            vec!["purchase"],
            vec!["ad_click"],
            "country",
        ));
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::Attribute {
                conversion_range, ..
            } => {
                // BETWEEN was captured at logical-lowering time.
                let (start, end) = conversion_range.expect("conversion_range captured");
                assert!(end > start, "end must be > start");
            }
            other => panic!("expected Attribute, got {other:?}"),
        }
    }

    #[test]
    fn attribute_negative_window_rejected() {
        // Negative windows have no defined semantics and are rejected.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![attribute_stage(
                -1,
                vec!["purchase"],
                vec!["ad_click"],
                "country",
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(ref msg) if msg.contains("window must be non-negative")),
            "expected non-negative window error, got {err:?}"
        );
    }

    #[test]
    fn attribute_zero_window_accepted() {
        // window: 0s is semantically valid per attribute.md §16.1:
        // every conversion LEFT-UNNESTs since no touchpoint can precede
        // it. The planner must not reject it.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![attribute_stage(
                0,
                vec!["purchase"],
                vec!["ad_click"],
                "country",
            )],
        );
        let result = lower_statement(Statement::Query(pipeline), &cat);
        assert!(
            result.is_ok(),
            "window: 0s should be accepted; got {result:?}"
        );
    }

    #[test]
    fn attribute_touchpoint_key_non_string_rejected() {
        // `touchpoint_key: amount` — amount is Float, not String.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![attribute_stage(
                60_000_000_000,
                vec!["purchase"],
                vec!["ad_click"],
                "amount",
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("must evaluate to String")));
    }

    #[test]
    fn attribute_duplicate_conversion_event_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![attribute_stage(
                60_000_000_000,
                vec!["purchase", "purchase"],
                vec!["ad_click"],
                "country",
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(msg) if msg.contains("duplicate conversion event type"))
        );
    }

    #[test]
    fn attribute_after_sessionize_extends_underlying_scan_backward() {
        // Regression test: `extend_scan_reader_backward` must walk through
        // `Sessionize` (and other Wave 4 wrappers) to reach the primary Scan.
        // attribute.md §14.1 permits the `SESSIONIZE | ATTRIBUTE` composition;
        // §12 requires the window widening to land on the underlying scan.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_30d: i64 = 30 * 86_400 * 1_000_000_000;
        let ns_7d: i64 = 7 * 86_400 * 1_000_000_000;
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.time_range = Some(TimeRange::Last(ns_30d));
        pipeline
            .stages
            .push(PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: 30 * 60 * 1_000_000_000,
                end: None,
                span: Span::EMPTY,
            }));
        pipeline.stages.push(attribute_stage(
            ns_7d,
            vec!["purchase"],
            vec!["ad_click"],
            "country",
        ));
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        // Walk to the Scan and assert `reader_backward_ns == ns_7d` (the
        // SESSIONIZE above ATTRIBUTE must not block the widening).
        let LogicalPlan::Attribute { input, .. } = plan else {
            panic!("expected Attribute on top");
        };
        let LogicalPlan::Sessionize { input, .. } = *input else {
            panic!("expected Sessionize under Attribute");
        };
        match *input {
            LogicalPlan::Scan {
                reader_backward_ns, ..
            } => {
                assert_eq!(reader_backward_ns, ns_7d, "scan must be widened by window");
            }
            other => panic!("expected Scan under Sessionize, got {other:?}"),
        }
    }

    #[test]
    fn first_with_lookback_after_sessionize_extends_scan_backward() {
        // Same walker-regression test for EventSelect (FIRST) composed over
        // SESSIONIZE with `lookback:`. event-select-sample.md §11 plus §7.1.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_30d: i64 = 30 * 86_400 * 1_000_000_000;
        let ns_2h: i64 = 2 * 3_600 * 1_000_000_000;
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.time_range = Some(TimeRange::Last(ns_30d));
        pipeline
            .stages
            .push(PipelineStage::Sessionize(bqlite_ast::Sessionize {
                gap: 30 * 60 * 1_000_000_000,
                end: None,
                span: Span::EMPTY,
            }));
        pipeline.stages.push(event_select_stage(
            bqlite_ast::EventSelectKind::First,
            vec!["purchase"],
            Some(ns_2h),
            None,
        ));
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        let LogicalPlan::EventSelect { input, .. } = plan else {
            panic!("expected EventSelect on top");
        };
        let LogicalPlan::Sessionize { input, .. } = *input else {
            panic!("expected Sessionize under EventSelect");
        };
        match *input {
            LogicalPlan::Scan {
                reader_backward_ns, ..
            } => {
                assert_eq!(reader_backward_ns, ns_2h);
            }
            other => panic!("expected Scan under Sessionize, got {other:?}"),
        }
    }

    #[test]
    fn attribute_between_and_last_use_same_exclusivity_convention() {
        // Both BETWEEN-at-logical and LAST-at-physical resolution paths must
        // produce ranges using the `end exclusive` convention.
        // BETWEEN <s> AND <e> → [start_ns, end_ns + 1)
        // LAST <dur>        → [now_ns - dur, now_ns)
        // Spot-check by constructing one each and verifying the `end` side is
        // non-inclusive (end > any value inside the window).
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let ns_7d: i64 = 7 * 86_400 * 1_000_000_000;
        // BETWEEN case
        let mut p_between = bare_pipeline("purchases");
        p_between.source.time_range = Some(TimeRange::Between {
            start: "2024-01-01T00:00:00Z".into(),
            end: "2024-01-31T23:59:59Z".into(),
        });
        p_between.stages.push(attribute_stage(
            ns_7d,
            vec!["purchase"],
            vec!["ad_click"],
            "country",
        ));
        let plan = lower_statement(Statement::Query(p_between), &cat).unwrap();
        let LogicalPlan::Attribute {
            conversion_range: Some((start, end)),
            ..
        } = plan
        else {
            panic!("expected Attribute with BETWEEN range captured");
        };
        assert!(end > start, "BETWEEN end must exceed start");
        // Width should be approximately 31 days (Jan 1 .. Jan 31 inclusive).
        // The `+ 1ns` exclusive-end convention (physical.rs: `resolve_ast_time_range`)
        // means the width is (end - start) where `start` is Jan 1 00:00:00 and
        // `end` is Jan 31 23:59:59 + 1ns. Check this is > 30 days and < 32 days.
        let width_ns = end - start;
        let ns_30d: i64 = 30 * 86_400 * 1_000_000_000;
        let ns_32d: i64 = 32 * 86_400 * 1_000_000_000;
        assert!(
            width_ns > ns_30d && width_ns < ns_32d,
            "expected BETWEEN range to span ~31 days, got {} ns",
            width_ns
        );
    }

    #[test]
    fn attribute_duplicate_touchpoint_event_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![attribute_stage(
                60_000_000_000,
                vec!["purchase"],
                vec!["ad_click", "ad_click"],
                "country",
            )],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(msg) if msg.contains("duplicate touchpoint event type"))
        );
    }

    // ── Wave 4 CP3: IN QUERY → SubqueryFilter lowering ────────────────────

    fn in_query_single_col(outer_col: &str, inner_table: &str, inner_col: &str) -> Spanned<Expr> {
        let inner_pipeline = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic(inner_table),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_bare_column(inner_col)],
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        Spanned::new(
            Expr::In {
                lhs: vec![column_expr(outer_col)],
                rhs: InRhs::Query(Box::new(inner_pipeline)),
                negated: false,
            },
            Span::EMPTY,
        )
    }

    fn orders_schema() -> TableSchema {
        TableSchema::new(
            "orders",
            vec![
                CoreColumnDef::required("user_id", BqlType::String),
                CoreColumnDef::required("ts", BqlType::Timestamp),
                CoreColumnDef::required("event", BqlType::String),
            ],
            "user_id",
            "ts",
            "event",
        )
        .unwrap()
    }

    #[test]
    fn where_in_query_single_column_lowers_to_subquery_filter() {
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_query_single_col("user_id", "orders", "user_id"),
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        match plan {
            LogicalPlan::SubqueryFilter {
                columns,
                subquery,
                input,
                output_schema,
            } => {
                assert_eq!(columns.len(), 1);
                assert_eq!(columns[0].result_type, BqlType::String);
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
                // Outer output schema is identity of the outer input.
                assert_eq!(&output_schema, input.output_schema());
                // Subquery lowered through lower_query_pipeline → Project(Scan).
                assert!(matches!(*subquery, LogicalPlan::Project { .. }));
            }
            other => panic!("expected SubqueryFilter, got {other:?}"),
        }
    }

    #[test]
    fn where_in_query_arity_mismatch_rejected() {
        // Outer LHS has 1 element, subquery has 3 non-system output columns.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let inner_pipeline = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("orders"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            // SELECT * expands to 3 columns (user_id, ts, event).
            stages: vec![PipelineStage::Select {
                distinct: false,
                items: vec![SelectItem {
                    kind: SelectItemKind::Wildcard,
                    alias: None,
                    span: Span::EMPTY,
                }],
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        let predicate = Spanned::new(
            Expr::In {
                lhs: vec![column_expr("user_id")],
                rhs: InRhs::Query(Box::new(inner_pipeline)),
                negated: false,
            },
            Span::EMPTY,
        );
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("arity mismatch")));
    }

    #[test]
    fn where_in_query_type_mismatch_rejected() {
        // outer LHS is `amount` (Float), subquery selects `user_id` (String).
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_query_single_col("amount", "orders", "user_id"),
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("type mismatch")));
    }

    #[test]
    fn where_negated_in_query_rejected() {
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let inner_pipeline = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("orders"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_bare_column("user_id")],
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        let predicate = Spanned::new(
            Expr::In {
                lhs: vec![column_expr("user_id")],
                rhs: InRhs::Query(Box::new(inner_pipeline)),
                negated: true,
            },
            Span::EMPTY,
        );
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("NOT IN")));
    }

    #[test]
    fn lower_statement_unbound_in_alias_reference_rejected() {
        // Using `lower_statement` (empty alias table) with an unbound
        // `IN alias` reference must reject with an undefined-alias error.
        // Distinct from `alias_undefined_reference_rejected` below which
        // goes through `lower_statements` and defines zero aliases.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let predicate = Spanned::new(
            Expr::In {
                lhs: vec![column_expr("user_id")],
                rhs: InRhs::Alias(Name::synthetic("vip")),
                negated: false,
            },
            Span::EMPTY,
        );
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("undefined")));
    }

    #[test]
    fn where_in_query_combined_with_other_conjuncts() {
        // `WHERE event = 'checkout' AND user_id IN QUERY (orders | SELECT user_id)
        //        AND amount > 100` — the IN QUERY gets lifted into SubqueryFilter
        // while the remaining conjuncts fold into a Filter on top.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let conj1 = Spanned::new(
            Expr::Compare {
                op: bqlite_ast::CompareOp::Equal,
                left: Box::new(column_expr("event")),
                right: Box::new(Spanned::new(
                    Expr::Literal(Literal::String("checkout".into())),
                    Span::EMPTY,
                )),
            },
            Span::EMPTY,
        );
        let conj2 = in_query_single_col("user_id", "orders", "user_id");
        let conj3 = Spanned::new(
            Expr::Compare {
                op: bqlite_ast::CompareOp::Greater,
                left: Box::new(column_expr("amount")),
                right: Box::new(Spanned::new(Expr::Literal(Literal::Int(100)), Span::EMPTY)),
            },
            Span::EMPTY,
        );
        let combined = Spanned::new(Expr::And(vec![conj1, conj2, conj3]), Span::EMPTY);
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: combined,
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        // Top: Filter (residual event = 'checkout' AND amount > 100).
        // Under: SubqueryFilter. Under that: Scan.
        let LogicalPlan::Filter {
            predicate, input, ..
        } = plan
        else {
            panic!("expected Filter on top");
        };
        assert_eq!(predicate.result_type, BqlType::Bool);
        let LogicalPlan::SubqueryFilter {
            input: inner_input, ..
        } = *input
        else {
            panic!("expected SubqueryFilter under Filter");
        };
        assert!(matches!(*inner_input, LogicalPlan::Scan { .. }));
    }

    #[test]
    fn where_only_in_query_produces_bare_subquery_filter() {
        // When the entire WHERE is a single IN QUERY conjunct, no outer
        // Filter should be emitted.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_query_single_col("user_id", "orders", "user_id"),
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        assert!(matches!(plan, LogicalPlan::SubqueryFilter { .. }));
    }

    #[test]
    fn where_nested_in_query_inside_or_rejected() {
        // `WHERE foo OR (user_id IN QUERY (...))` — the subquery conjunct is
        // no longer at the top-level AND chain, so it reaches TypedExpr::from_ast
        // and surfaces the "IN QUERY must appear as a top-level conjunct" error.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let foo = Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY);
        let in_query = in_query_single_col("user_id", "orders", "user_id");
        let predicate = Spanned::new(Expr::Or(vec![foo, in_query]), Span::EMPTY);
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        );
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("top-level WHERE conjunct")));
    }

    #[test]
    fn where_in_query_tuple_two_columns() {
        // `(user_id, event) IN QUERY (orders | SELECT user_id, event)` — tuple cohort.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let inner_pipeline = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("orders"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_bare_column("user_id"), select_bare_column("event")],
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        let predicate = Spanned::new(
            Expr::In {
                lhs: vec![column_expr("user_id"), column_expr("event")],
                rhs: InRhs::Query(Box::new(inner_pipeline)),
                negated: false,
            },
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
        match plan {
            LogicalPlan::SubqueryFilter { columns, .. } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].result_type, BqlType::String);
                assert_eq!(columns[1].result_type, BqlType::String);
            }
            other => panic!("expected SubqueryFilter, got {other:?}"),
        }
    }

    #[test]
    fn where_flattens_nested_and_with_in_query() {
        // `(a AND user_id IN QUERY (...)) AND (b AND c)` — flatten into four
        // conjuncts: [a, IN_QUERY, b, c]. IN_QUERY lifts to SubqueryFilter;
        // residual a AND b AND c folds into Filter.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let a = Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY);
        let b = Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY);
        let c = Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY);
        let in_q = in_query_single_col("user_id", "orders", "user_id");
        let left = Spanned::new(Expr::And(vec![a, in_q]), Span::EMPTY);
        let right = Spanned::new(Expr::And(vec![b, c]), Span::EMPTY);
        let predicate = Spanned::new(Expr::And(vec![left, right]), Span::EMPTY);
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        // Filter (residual a AND b AND c) over SubqueryFilter over Scan.
        let LogicalPlan::Filter { input, .. } = plan else {
            panic!("expected Filter on top");
        };
        assert!(matches!(*input, LogicalPlan::SubqueryFilter { .. }));
    }

    #[test]
    fn where_paren_wrapper_drills_through() {
        // `WHERE (user_id IN QUERY (...))` — the Paren wrapper should not
        // block the flatten walker from noticing the IN QUERY conjunct.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(orders_schema());
        let in_q = in_query_single_col("user_id", "orders", "user_id");
        let wrapped = Spanned::new(Expr::Paren(Box::new(in_q)), Span::EMPTY);
        let pipeline = pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: wrapped,
                span: Span::EMPTY,
            }],
        );
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        // Bare SubqueryFilter (no residual Filter wrapping it).
        assert!(matches!(plan, LogicalPlan::SubqueryFilter { .. }));
    }

    // ── Wave 4 CP4: DefineAlias + IN alias resolution ─────────────────────

    fn define_alias(name: &str, body: Pipeline) -> Statement {
        Statement::DefineAlias {
            name: Name::synthetic(name),
            body,
            span: Span::EMPTY,
        }
    }

    fn in_alias_predicate(outer_col: &str, alias_name: &str) -> Spanned<Expr> {
        Spanned::new(
            Expr::In {
                lhs: vec![column_expr(outer_col)],
                rhs: InRhs::Alias(Name::synthetic(alias_name)),
                negated: false,
            },
            Span::EMPTY,
        )
    }

    fn vip_alias_body() -> Pipeline {
        // `purchases | SELECT user_id` (simple projection; used as a cohort).
        Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("purchases"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_bare_column("user_id")],
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        }
    }

    #[test]
    fn alias_def_plus_in_alias_reference_resolves() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        // `vip = purchases | SELECT user_id`
        // then `purchases | WHERE user_id IN alias vip`
        let alias_stmt = define_alias("vip", vip_alias_body());
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_alias_predicate("user_id", "vip"),
                span: Span::EMPTY,
            }],
        ));
        let plan = lower_statements(vec![alias_stmt, terminal], &cat).unwrap();
        match plan {
            LogicalPlan::SubqueryFilter {
                columns,
                subquery,
                input,
                ..
            } => {
                assert_eq!(columns.len(), 1);
                // The subquery is the lowered vip body: Project over Scan.
                assert!(matches!(*subquery, LogicalPlan::Project { .. }));
                assert!(matches!(*input, LogicalPlan::Scan { .. }));
            }
            other => panic!("expected SubqueryFilter, got {other:?}"),
        }
    }

    #[test]
    fn alias_undefined_reference_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_alias_predicate("user_id", "never_defined"),
                span: Span::EMPTY,
            }],
        ));
        let err = lower_statements(vec![terminal], &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(msg) if msg.contains("never_defined") && msg.contains("undefined"))
        );
    }

    #[test]
    fn alias_forward_reference_rejected() {
        // `a = purchases | WHERE user_id IN alias b   (references b before b is defined)
        //  b = purchases | SELECT user_id
        //  purchases | WHERE user_id IN alias a`
        // The reference from `a`'s body to `b` is a forward reference, even
        // though both aliases are defined before the terminal.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let a_body = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("purchases"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![
                PipelineStage::Where {
                    predicate: in_alias_predicate("user_id", "b"),
                    span: Span::EMPTY,
                },
                PipelineStage::Select {
                    distinct: false,
                    items: vec![select_bare_column("user_id")],
                    span: Span::EMPTY,
                },
            ],
            span: Span::EMPTY,
        };
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_alias_predicate("user_id", "a"),
                span: Span::EMPTY,
            }],
        ));
        let err = lower_statements(
            vec![
                define_alias("a", a_body),
                define_alias("b", vip_alias_body()),
                terminal,
            ],
            &cat,
        )
        .unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("defined later")));
    }

    // `alias_direct_cycle_detected` was folded into
    // `alias_self_reference_reports_dedicated_error` — the dedicated
    // self-ref branch supersedes the earlier "contains any `a`" assertion.

    #[test]
    fn alias_self_reference_reports_dedicated_error() {
        // An alias body that references itself hits the dedicated self-ref
        // branch added in CP4 review follow-up, not the generic forward-ref
        // or cycle-detection branches.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let a_body = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("purchases"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![
                PipelineStage::Where {
                    predicate: in_alias_predicate("user_id", "a"),
                    span: Span::EMPTY,
                },
                PipelineStage::Select {
                    distinct: false,
                    items: vec![select_bare_column("user_id")],
                    span: Span::EMPTY,
                },
            ],
            span: Span::EMPTY,
        };
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_alias_predicate("user_id", "a"),
                span: Span::EMPTY,
            }],
        ));
        let err = lower_statements(vec![define_alias("a", a_body), terminal], &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("cannot reference itself")));
    }

    #[test]
    fn alias_last_wins_on_duplicate_definition() {
        // Two definitions of `vip` with *different column types*:
        //   vip = purchases | SELECT amount   (Float — would type-mismatch user_id)
        //   vip = purchases | SELECT user_id  (String — type-matches user_id)
        // Terminal uses `WHERE user_id IN alias vip`. Under last-wins the
        // second body is picked and the plan lowers cleanly; under first-wins
        // the type check would fail.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let first_body = Pipeline {
            source: Source {
                primary: TableRef {
                    name: Name::synthetic("purchases"),
                    span: Span::EMPTY,
                },
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Select {
                distinct: false,
                items: vec![select_bare_column("amount")],
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        let second_body = vip_alias_body();
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate: in_alias_predicate("user_id", "vip"),
                span: Span::EMPTY,
            }],
        ));
        let plan = lower_statements(
            vec![
                define_alias("vip", first_body),
                define_alias("vip", second_body),
                terminal,
            ],
            &cat,
        )
        .unwrap();
        match plan {
            LogicalPlan::SubqueryFilter { columns, .. } => {
                // LHS was `user_id` (String). If first-wins had been used, the
                // vip body would expose Float and `apply_subquery_filter`'s
                // positional type check would have failed.
                assert_eq!(columns[0].result_type, BqlType::String);
            }
            other => panic!("expected SubqueryFilter, got {other:?}"),
        }
    }

    #[test]
    fn alias_referenced_twice_caches_lowered_plan() {
        // Two `IN alias vip` references in the same terminal — the cache
        // should be populated after the first, and the second reference
        // hits it. Observable via test plumbing: after calling
        // `lower_statements`, the AliasTable is consumed, so this test
        // mostly verifies end-to-end correctness of multiple references.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![
                PipelineStage::Where {
                    predicate: in_alias_predicate("user_id", "vip"),
                    span: Span::EMPTY,
                },
                PipelineStage::Where {
                    predicate: in_alias_predicate("user_id", "vip"),
                    span: Span::EMPTY,
                },
            ],
        ));
        let plan =
            lower_statements(vec![define_alias("vip", vip_alias_body()), terminal], &cat).unwrap();
        // Expect SubqueryFilter(SubqueryFilter(Scan)) from the two WHEREs.
        let LogicalPlan::SubqueryFilter { input, .. } = plan else {
            panic!("expected outer SubqueryFilter");
        };
        assert!(matches!(*input, LogicalPlan::SubqueryFilter { .. }));
    }

    #[test]
    fn not_in_alias_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let predicate = Spanned::new(
            Expr::In {
                lhs: vec![column_expr("user_id")],
                rhs: InRhs::Alias(Name::synthetic("vip")),
                negated: true,
            },
            Span::EMPTY,
        );
        let terminal = Statement::Query(pipeline_with_stages(
            "purchases",
            vec![PipelineStage::Where {
                predicate,
                span: Span::EMPTY,
            }],
        ));
        let err = lower_statements(vec![define_alias("vip", vip_alias_body()), terminal], &cat)
            .unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("NOT IN")));
    }

    #[test]
    fn lower_statements_single_query_matches_lower_statement() {
        // Back-compat regression: a single-statement Vec must produce the
        // same plan shape as lower_statement(stmt) did pre-CP4.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let stmt = Statement::Query(bare_pipeline("purchases"));
        let via_batch = lower_statements(vec![stmt.clone()], &cat).unwrap();
        let via_single = lower_statement(stmt, &cat).unwrap();
        assert_eq!(via_batch, via_single);
    }

    #[test]
    fn empty_script_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statements(vec![], &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("empty")));
    }

    #[test]
    fn script_with_only_aliases_rejected() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let err = lower_statements(vec![define_alias("vip", vip_alias_body())], &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("terminal")));
    }

    // ── Wave 4 CP5a: Entity-aligned source JOIN logical path ──────────────

    fn logins_schema() -> TableSchema {
        TableSchema::new(
            "logins",
            vec![
                CoreColumnDef::required("user_id", BqlType::String),
                CoreColumnDef::required("ts", BqlType::Timestamp),
                CoreColumnDef::required("event", BqlType::String),
                CoreColumnDef::nullable("device", BqlType::String),
            ],
            "user_id",
            "ts",
            "event",
        )
        .unwrap()
    }

    fn clicks_schema_int_entity_key() -> TableSchema {
        TableSchema::new(
            "clicks",
            vec![
                CoreColumnDef::required("user_id", BqlType::Int), // different entity-key type
                CoreColumnDef::required("ts", BqlType::Timestamp),
                CoreColumnDef::required("event", BqlType::String),
            ],
            "user_id",
            "ts",
            "event",
        )
        .unwrap()
    }

    #[test]
    fn bare_pipeline_without_joins_still_lowers_to_plain_scan() {
        // Regression: the single-table path must remain unchanged in both
        // schema shape and `joined_tables` emptiness.
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let plan = lower_statement(Statement::Query(bare_pipeline("purchases")), &cat).unwrap();
        let LogicalPlan::Scan {
            joined_tables,
            output_schema,
            ..
        } = plan
        else {
            panic!("expected Scan");
        };
        assert!(joined_tables.is_empty());
        // Column names are bare — not dotted.
        let names: Vec<&str> = output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(names.contains(&"user_id"));
        assert!(names.contains(&"amount"));
    }

    #[test]
    fn joined_pipeline_lowers_with_combined_schema_and_discriminator() {
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(logins_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("logins"),
            span: Span::EMPTY,
        });
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        let LogicalPlan::Scan {
            joined_tables,
            output_schema,
            ..
        } = plan
        else {
            panic!("expected Scan");
        };
        assert_eq!(joined_tables.len(), 1);
        assert_eq!(joined_tables[0].name(), "logins");
        // Column names are dotted; __source_table_id is present.
        let names: Vec<&str> = output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(names.contains(&"purchases.user_id"));
        assert!(names.contains(&"purchases.amount"));
        assert!(names.contains(&"logins.user_id"));
        assert!(names.contains(&"logins.device"));
        assert!(names.contains(&SOURCE_TABLE_ID_COLUMN));
        // __source_table_id is NOT NULL and Int.
        let (_, stid) = output_schema.column(SOURCE_TABLE_ID_COLUMN).unwrap();
        assert_eq!(stid.bql_type, BqlType::Int);
        assert!(!stid.nullable);
    }

    #[test]
    fn joined_pipeline_includes_system_columns_in_combined_schema() {
        // Per `docs/design/storage/system-columns.md` §4.2, the joined
        // schema declares __seq_id and __batch_id as bare-named, NOT
        // NULL, Int columns. They are populated by
        // MergeSourcesOperator's bare-name resolution path against
        // each sub-scan's emitted system columns (now that
        // ScanOperator materialises them as of TASK-508).
        //
        // This test was previously a negative regression guard
        // (`joined_pipeline_omits_system_columns_from_combined_schema`)
        // and is flipped to assert the post-TASK-508 contract.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(logins_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("logins"),
            span: Span::EMPTY,
        });
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        let LogicalPlan::Scan { output_schema, .. } = plan else {
            panic!("expected Scan");
        };
        let names: Vec<&str> = output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(
            names.contains(&"__seq_id"),
            "expected __seq_id in joined combined schema, got {names:?}"
        );
        assert!(
            names.contains(&"__batch_id"),
            "expected __batch_id in joined combined schema, got {names:?}"
        );
        let (_, seq) = output_schema.column("__seq_id").unwrap();
        assert_eq!(seq.bql_type, BqlType::Int);
        assert!(!seq.nullable, "__seq_id must be NOT NULL");
        let (_, bid) = output_schema.column("__batch_id").unwrap();
        assert_eq!(bid.bql_type, BqlType::Int);
        assert!(!bid.nullable, "__batch_id must be NOT NULL");
    }

    #[test]
    fn joined_pipeline_entity_key_type_mismatch_rejected() {
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(clicks_schema_int_entity_key());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("clicks"),
            span: Span::EMPTY,
        });
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("entity-key type mismatch")));
    }

    #[test]
    fn joined_pipeline_self_join_rejected_at_logical() {
        let cat = InMemoryCatalog::default().with(purchases_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("purchases"),
            span: Span::EMPTY,
        });
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(matches!(err, BqliteError::Plan(msg) if msg.contains("self-join")));
    }

    #[test]
    fn joined_pipeline_qualified_reference_resolves() {
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(logins_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("logins"),
            span: Span::EMPTY,
        });
        // `| WHERE purchases.amount > 100` — qualified reference resolves
        // via the dotted column in the combined schema.
        let predicate = Spanned::new(
            Expr::Compare {
                op: bqlite_ast::CompareOp::Greater,
                left: Box::new(Spanned::new(
                    Expr::Qualified {
                        table: Name::synthetic("purchases"),
                        column: Name::synthetic("amount"),
                    },
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(Expr::Literal(Literal::Int(100)), Span::EMPTY)),
            },
            Span::EMPTY,
        );
        pipeline.stages.push(PipelineStage::Where {
            predicate,
            span: Span::EMPTY,
        });
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        assert!(matches!(plan, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn joined_pipeline_bare_reference_fails_with_unknown_column() {
        // In a joined pipeline, bare `Expr::Column("amount")` does not
        // resolve because the combined schema only exposes `purchases.amount`.
        // This enforces the mandatory-qualification rule (cohorts-aliases-
        // joins.md §3.11) via the existing unknown-column error.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(logins_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("logins"),
            span: Span::EMPTY,
        });
        let predicate = Spanned::new(
            Expr::Compare {
                op: bqlite_ast::CompareOp::Greater,
                left: Box::new(column_expr("amount")),
                right: Box::new(Spanned::new(Expr::Literal(Literal::Int(100)), Span::EMPTY)),
            },
            Span::EMPTY,
        );
        pipeline.stages.push(PipelineStage::Where {
            predicate,
            span: Span::EMPTY,
        });
        let err = lower_statement(Statement::Query(pipeline), &cat).unwrap_err();
        assert!(
            matches!(err, BqliteError::Plan(msg) if msg.contains("unknown column") && msg.contains("amount"))
        );
    }

    #[test]
    fn joined_pipeline_qualified_wildcard_expands() {
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(logins_schema());
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("logins"),
            span: Span::EMPTY,
        });
        pipeline.stages.push(PipelineStage::Select {
            distinct: false,
            items: vec![SelectItem {
                kind: SelectItemKind::QualifiedWildcard(Name::synthetic("logins")),
                alias: None,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        });
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        let LogicalPlan::Project { expressions, .. } = plan else {
            panic!("expected Project");
        };
        let names: Vec<&str> = expressions.iter().map(|i| i.output_name.as_str()).collect();
        // logins has user_id, ts, event, device (all non-system).
        assert_eq!(
            names,
            vec![
                "logins.user_id",
                "logins.ts",
                "logins.event",
                "logins.device"
            ]
        );
    }

    #[test]
    fn joined_pipeline_attribute_widens_reader_on_joined_scan() {
        // `purchases JOIN logins LAST 1d | ATTRIBUTE ... window: 7d` — the
        // ATTRIBUTE stage applies window extension to the combined scan.
        // The single `reader_backward_ns` field on Scan applies uniformly to
        // every joined sub-scan when CP5b fans it out.
        let cat = InMemoryCatalog::default()
            .with(purchases_schema())
            .with(logins_schema());
        let ns_1d: i64 = 86_400 * 1_000_000_000;
        let ns_7d: i64 = 7 * ns_1d;
        let mut pipeline = bare_pipeline("purchases");
        pipeline.source.joins.push(TableRef {
            name: Name::synthetic("logins"),
            span: Span::EMPTY,
        });
        pipeline.source.time_range = Some(TimeRange::Last(ns_1d));
        // touchpoint_key references a qualified column from the joined table.
        pipeline
            .stages
            .push(PipelineStage::Attribute(bqlite_ast::Attribute {
                conversion: vec![event_ref("purchase")],
                touchpoints: vec![event_ref("login")],
                window: ns_7d,
                touchpoint_key: Spanned::new(
                    Expr::Qualified {
                        table: Name::synthetic("logins"),
                        column: Name::synthetic("device"),
                    },
                    Span::EMPTY,
                ),
                span: Span::EMPTY,
            }));
        let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
        let LogicalPlan::Attribute { input, .. } = plan else {
            panic!("expected Attribute");
        };
        match *input {
            LogicalPlan::Scan {
                reader_backward_ns,
                joined_tables,
                ..
            } => {
                assert_eq!(reader_backward_ns, ns_7d);
                assert_eq!(joined_tables.len(), 1);
            }
            other => panic!("expected Scan, got {other:?}"),
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

    // ── DELETE classifier (TASK-453) ────────────────────────────────────────
    //
    // Each test exercises one shape from `docs/design/storage/deletes.md`
    // §3 (cheap-class taxonomy) or §4 (ALLOW SCAN opt-in). Tests build
    // AST `Spanned<Expr>` values directly and pass them through
    // `classify_delete_predicate` so the test surface is independent
    // of the parser.

    use bqlite_ast::expr::{CompareOp, InRhs};

    fn col(name: &str) -> Spanned<Expr> {
        Spanned::new(Expr::Column(Name::synthetic(name)), Span::EMPTY)
    }

    fn lit(literal: Literal) -> Spanned<Expr> {
        Spanned::new(Expr::Literal(literal), Span::EMPTY)
    }

    fn cmp(op: CompareOp, left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
        Spanned::new(
            Expr::Compare {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
            Span::EMPTY,
        )
    }

    fn classify(predicate: Spanned<Expr>, allow_scan: bool) -> Result<DeleteFilter> {
        let table = purchases_schema();
        let registry = FunctionRegistry::with_builtins();
        classify_delete_predicate(&predicate, &table, allow_scan, &registry)
    }

    fn cheap(predicate: Spanned<Expr>) -> CheapDeleteSpec {
        match classify(predicate, false).expect("must classify as cheap") {
            DeleteFilter::Cheap(spec) => spec,
            DeleteFilter::AllowScan { .. } => panic!("expected Cheap variant"),
        }
    }

    #[test]
    fn entity_equality_is_cheap_entity_tombstone() {
        // user_id = 'alice'
        let pred = cmp(
            CompareOp::Equal,
            col("user_id"),
            lit(Literal::String("alice".into())),
        );
        let spec = cheap(pred);
        assert_eq!(spec.entity_keys, vec![ScalarValue::String("alice".into())]);
        assert!(matches!(spec.entity_role, EntityRole::AsTombstone));
        assert!(spec.seq_ids.is_empty());
        assert!(spec.batch_ids.is_empty());
        assert!(spec.time_range.is_none());
    }

    #[test]
    fn entity_in_list_is_cheap_entity_tombstone() {
        // user_id IN ('alice', 'bob', 'carol')
        let pred = Spanned::new(
            Expr::In {
                lhs: vec![col("user_id")],
                rhs: InRhs::List(vec![
                    lit(Literal::String("alice".into())),
                    lit(Literal::String("bob".into())),
                    lit(Literal::String("carol".into())),
                ]),
                negated: false,
            },
            Span::EMPTY,
        );
        let spec = cheap(pred);
        assert_eq!(spec.entity_keys.len(), 3);
        assert!(matches!(spec.entity_role, EntityRole::AsTombstone));
    }

    #[test]
    fn seq_id_equality_is_cheap_row_level() {
        let pred = cmp(CompareOp::Equal, col("__seq_id"), lit(Literal::Int(42)));
        let spec = cheap(pred);
        assert_eq!(spec.seq_ids, vec![42]);
        assert!(spec.entity_keys.is_empty());
        assert!(matches!(spec.entity_role, EntityRole::AsTombstone));
    }

    #[test]
    fn seq_id_in_list_is_cheap_row_level() {
        let pred = Spanned::new(
            Expr::In {
                lhs: vec![col("__seq_id")],
                rhs: InRhs::List(vec![
                    lit(Literal::Int(1)),
                    lit(Literal::Int(2)),
                    lit(Literal::Int(3)),
                ]),
                negated: false,
            },
            Span::EMPTY,
        );
        let spec = cheap(pred);
        assert_eq!(spec.seq_ids, vec![1, 2, 3]);
    }

    #[test]
    fn batch_id_equality_is_cheap_batch_level() {
        let pred = cmp(CompareOp::Equal, col("__batch_id"), lit(Literal::Int(7)));
        let spec = cheap(pred);
        assert_eq!(spec.batch_ids, vec![7]);
    }

    #[test]
    fn time_range_lt_is_cheap_with_exclusive_max() {
        // ts < 1_700_000_000_000_000_000
        let pred = cmp(
            CompareOp::Less,
            col("ts"),
            lit(Literal::Timestamp(1_700_000_000_000_000_000)),
        );
        let spec = cheap(pred);
        let r = spec.time_range.expect("must have time_range");
        assert_eq!(r.max_ts, Some(1_700_000_000_000_000_000));
        assert!(!r.max_inclusive);
        assert!(r.min_ts.is_none());
    }

    #[test]
    fn time_range_le_is_cheap_with_inclusive_max() {
        let pred = cmp(
            CompareOp::LessOrEqual,
            col("ts"),
            lit(Literal::Timestamp(100)),
        );
        let r = cheap(pred).time_range.unwrap();
        assert_eq!(r.max_ts, Some(100));
        assert!(r.max_inclusive);
    }

    #[test]
    fn time_range_gt_is_cheap_with_exclusive_min() {
        let pred = cmp(CompareOp::Greater, col("ts"), lit(Literal::Timestamp(50)));
        let r = cheap(pred).time_range.unwrap();
        assert_eq!(r.min_ts, Some(50));
        assert!(!r.min_inclusive);
    }

    #[test]
    fn time_range_ge_is_cheap_with_inclusive_min() {
        let pred = cmp(
            CompareOp::GreaterOrEqual,
            col("ts"),
            lit(Literal::Timestamp(50)),
        );
        let r = cheap(pred).time_range.unwrap();
        assert_eq!(r.min_ts, Some(50));
        assert!(r.min_inclusive);
    }

    #[test]
    fn time_range_between_collapses_to_inclusive_bounds() {
        // ts BETWEEN 100 AND 200
        let pred = Spanned::new(
            Expr::Between {
                expr: Box::new(col("ts")),
                low: Box::new(lit(Literal::Timestamp(100))),
                high: Box::new(lit(Literal::Timestamp(200))),
                negated: false,
            },
            Span::EMPTY,
        );
        let r = cheap(pred).time_range.unwrap();
        assert_eq!(r.min_ts, Some(100));
        assert!(r.min_inclusive);
        assert_eq!(r.max_ts, Some(200));
        assert!(r.max_inclusive);
    }

    #[test]
    fn time_range_two_bounds_collapse_via_and() {
        // ts >= 100 AND ts < 200
        let pred = Spanned::new(
            Expr::And(vec![
                cmp(
                    CompareOp::GreaterOrEqual,
                    col("ts"),
                    lit(Literal::Timestamp(100)),
                ),
                cmp(CompareOp::Less, col("ts"), lit(Literal::Timestamp(200))),
            ]),
            Span::EMPTY,
        );
        let r = cheap(pred).time_range.unwrap();
        assert_eq!(r.min_ts, Some(100));
        assert!(r.min_inclusive);
        assert_eq!(r.max_ts, Some(200));
        assert!(!r.max_inclusive);
    }

    #[test]
    fn time_range_reversed_compare_is_normalized() {
        // 100 <= ts → ts >= 100
        let pred = cmp(
            CompareOp::LessOrEqual,
            lit(Literal::Timestamp(100)),
            col("ts"),
        );
        let r = cheap(pred).time_range.unwrap();
        assert_eq!(r.min_ts, Some(100));
        assert!(r.min_inclusive);
    }

    #[test]
    fn entity_plus_seq_id_is_cheap_with_shard_target() {
        // user_id = 'alice' AND __seq_id IN (10, 20)
        let pred = Spanned::new(
            Expr::And(vec![
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("alice".into())),
                ),
                Spanned::new(
                    Expr::In {
                        lhs: vec![col("__seq_id")],
                        rhs: InRhs::List(vec![lit(Literal::Int(10)), lit(Literal::Int(20))]),
                        negated: false,
                    },
                    Span::EMPTY,
                ),
            ]),
            Span::EMPTY,
        );
        let spec = cheap(pred);
        assert_eq!(spec.entity_keys, vec![ScalarValue::String("alice".into())]);
        assert!(matches!(spec.entity_role, EntityRole::AsShardTarget));
        assert_eq!(spec.seq_ids, vec![10, 20]);
    }

    #[test]
    fn entity_plus_batch_id_is_cheap_with_shard_target() {
        let pred = Spanned::new(
            Expr::And(vec![
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("alice".into())),
                ),
                cmp(CompareOp::Equal, col("__batch_id"), lit(Literal::Int(7))),
            ]),
            Span::EMPTY,
        );
        let spec = cheap(pred);
        assert!(matches!(spec.entity_role, EntityRole::AsShardTarget));
        assert_eq!(spec.batch_ids, vec![7]);
    }

    #[test]
    fn entity_plus_time_range_is_rejected_cross_granularity() {
        // user_id = 'alice' AND ts < 100  — design doc §3.2: not cheap
        let pred = Spanned::new(
            Expr::And(vec![
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("alice".into())),
                ),
                cmp(CompareOp::Less, col("ts"), lit(Literal::Timestamp(100))),
            ]),
            Span::EMPTY,
        );
        let err = classify(pred, false).expect_err("cross-granularity must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("cross") || msg.contains("granularit"),
            "got: {msg}"
        );
    }

    #[test]
    fn seq_plus_batch_is_rejected_cross_granularity() {
        let pred = Spanned::new(
            Expr::And(vec![
                cmp(CompareOp::Equal, col("__seq_id"), lit(Literal::Int(1))),
                cmp(CompareOp::Equal, col("__batch_id"), lit(Literal::Int(2))),
            ]),
            Span::EMPTY,
        );
        let err = classify(pred, false).expect_err("seq+batch must reject");
        assert!(err.to_string().contains("granular"), "got: {err}");
    }

    #[test]
    fn or_is_not_cheap_and_rejected_without_allow_scan() {
        // user_id = 'alice' OR user_id = 'bob' — top-level OR
        let pred = Spanned::new(
            Expr::Or(vec![
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("alice".into())),
                ),
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("bob".into())),
                ),
            ]),
            Span::EMPTY,
        );
        let err = classify(pred, false).expect_err("OR must reject");
        assert!(err.to_string().contains("ALLOW SCAN"), "got: {err}");
    }

    #[test]
    fn or_with_allow_scan_returns_allow_scan_variant() {
        let pred = Spanned::new(
            Expr::Or(vec![
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("alice".into())),
                ),
                cmp(
                    CompareOp::Equal,
                    col("user_id"),
                    lit(Literal::String("bob".into())),
                ),
            ]),
            Span::EMPTY,
        );
        match classify(pred, true).expect("ALLOW SCAN must accept") {
            DeleteFilter::AllowScan { predicate } => {
                assert_eq!(predicate.result_type, BqlType::Bool);
            }
            DeleteFilter::Cheap(_) => panic!("expected AllowScan variant"),
        }
    }

    #[test]
    fn not_equal_is_not_cheap() {
        // event != 'spam'  (a non-system column — not cheap regardless)
        let pred = cmp(
            CompareOp::NotEqual,
            col("event"),
            lit(Literal::String("spam".into())),
        );
        let err = classify(pred, false).expect_err("must reject");
        assert!(err.to_string().contains("ALLOW SCAN"), "got: {err}");
    }

    #[test]
    fn arbitrary_user_column_predicate_is_not_cheap() {
        // amount > 100 — not in the allowlist
        let pred = cmp(CompareOp::Greater, col("amount"), lit(Literal::Int(100)));
        let err = classify(pred, false).expect_err("must reject user-column compare");
        assert!(err.to_string().contains("ALLOW SCAN"), "got: {err}");
    }

    #[test]
    fn empty_in_list_is_rejected() {
        let pred = Spanned::new(
            Expr::In {
                lhs: vec![col("__seq_id")],
                rhs: InRhs::List(vec![]),
                negated: false,
            },
            Span::EMPTY,
        );
        let err = classify(pred, false).expect_err("empty IN list must error");
        assert!(err.to_string().contains("empty IN-list"), "got: {err}");
    }

    #[test]
    fn parenthesized_predicate_is_unwrapped() {
        // ((user_id = 'alice'))
        let inner = cmp(
            CompareOp::Equal,
            col("user_id"),
            lit(Literal::String("alice".into())),
        );
        let pred = Spanned::new(
            Expr::Paren(Box::new(Spanned::new(
                Expr::Paren(Box::new(inner)),
                Span::EMPTY,
            ))),
            Span::EMPTY,
        );
        let spec = cheap(pred);
        assert_eq!(spec.entity_keys.len(), 1);
    }

    #[test]
    fn negative_seq_id_literal_is_rejected() {
        let pred = cmp(CompareOp::Equal, col("__seq_id"), lit(Literal::Int(-1)));
        let err = classify(pred, false).expect_err("negative seq_id must error");
        assert!(err.to_string().contains("non-negative"), "got: {err}");
    }

    #[test]
    fn delete_unknown_table_returns_plan_error() {
        let catalog = InMemoryCatalog::default();
        let stmt = bqlite_ast::DeleteStmt {
            table: Name::synthetic("ghost"),
            predicate: cmp(
                CompareOp::Equal,
                col("user_id"),
                lit(Literal::String("alice".into())),
            ),
            allow_scan: false,
            span: Span::EMPTY,
        };
        let err = lower_delete(stmt, &catalog).expect_err("unknown table must error");
        assert!(err.to_string().contains("ghost"), "got: {err}");
    }

    #[test]
    fn lower_delete_produces_logical_delete_node() {
        let catalog = InMemoryCatalog::default().with(purchases_schema());
        let stmt = bqlite_ast::DeleteStmt {
            table: Name::synthetic("purchases"),
            predicate: cmp(
                CompareOp::Equal,
                col("user_id"),
                lit(Literal::String("alice".into())),
            ),
            allow_scan: false,
            span: Span::EMPTY,
        };
        let plan = lower_delete(stmt, &catalog).expect("lowering must succeed");
        match plan {
            LogicalPlan::Delete {
                table,
                filter,
                allow_scan,
                ..
            } => {
                assert_eq!(table.name(), "purchases");
                assert!(!allow_scan);
                assert!(matches!(filter, DeleteFilter::Cheap(_)));
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }
}
