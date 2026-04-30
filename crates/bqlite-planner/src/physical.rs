//! Wave 2 physical plan descriptors + logical → physical lowering.
//!
//! This module is the runtime-side counterpart to
//! [`crate::logical::LogicalPlan`] — every Wave 2 logical variant has a
//! plain-data physical mirror here carrying the runtime form of any
//! expression the operator layer consumes ([`CompiledExpr`]). The
//! engine's bind step (TASK-232) walks this tree and materializes one
//! `Box<dyn PhysicalOperator>` per descriptor.
//!
//! ## Scope (TASK-226)
//!
//! - **Descriptors**: `ScanPhysical`, `FilterPhysical`, `ProjectPhysical`,
//!   `LimitPhysical`, `CreateTablePhysical`, `DropTablePhysical`,
//!   `AlterTableAddColumnPhysical`, `DescribePhysical`, `InsertPhysical`,
//!   `ExplainPhysical`. Each mirrors the corresponding
//!   [`LogicalPlan`] variant from `docs/design/planner/logical-plan-nodes.md`
//!   §4.1–§4.10 one-for-one.
//! - **Lowering**: [`lower_physical`] is an infallible structural
//!   walk. The logical plan has already been validated at construction
//!   (§4.5 of planner-pipeline.md) so physical lowering never rejects
//!   a well-formed input — it only swaps `TypedExpr` for
//!   [`CompiledExpr`], unwraps the resolved `TableSchema` into a
//!   bindable name, and threads the optimizer-populated
//!   `scan_predicates` / `projected_columns` through unchanged.
//! - **No trait objects.** Every descriptor is plain data
//!   (`Debug + Clone + PartialEq`) so the tree can be cloned, diffed
//!   in tests, and serialized by a future EXPLAIN JSON formatter
//!   without touching any `dyn Trait` surface. The fused-operator
//!   framework from `planner-pipeline.md` §7 that will introduce
//!   `Box<dyn Fused…>` lives in Wave 5 and does not apply here.
//!
//! ## Out of scope
//!
//! - **Engine binding.** Turning these descriptors into executable
//!   operators is TASK-232 — it extends `bqlite-engine`'s `bind_physical`
//!   with one arm per variant. This module holds no `Box<dyn ...>` and
//!   never imports `bqlite-operators`.
//! - **Optimizer passes.** TASK-227 (predicate pushdown) and TASK-228
//!   (projection pruning) rewrite the *physical* tree; both consume
//!   descriptors produced here and write back into `ScanPhysical`'s
//!   `scan_predicates` / `projected_columns` fields. Wave 2 always
//!   lowers with those fields empty / all-columns — they are populated
//!   by the optimizer pass, not by lowering.
//! - **Later-wave variants.** `SequenceMatch`, `Aggregate`, `Sort`,
//!   etc. are enumerated in `planner-pipeline.md` §9.5 and land in
//!   Waves 3/4 as new `PhysicalPlan` variants. The enum is the
//!   extensibility point; later waves extend it without renaming the
//!   Wave 2 variants.

use bqlite_core::{AggFunction, ColumnDef, OperatorSchema, TableSchema};

use crate::compile::{CompiledNfa, MatchExecutionConfig, MatchStrategy};
use crate::compiled::CompiledExpr;
use crate::demand::{ColumnId, CompiledFusableAggregate, DemandCapabilities, DemandSet};
use crate::logical::{EventSelectKind, InsertLogicalBody, LogicalPlan, ProjectItem, SortDirection};

// ─────────────────────────────────────────────────────────────────────────────
// Tunables
// ─────────────────────────────────────────────────────────────────────────────

/// Default per-tile batch size used by [`FilterPhysical`].
///
/// Per `docs/design/execution-model.md` §3.6 the filter kernel walks
/// its input in cache-friendly tiles so the predicate evaluation stays
/// L1/L2-resident. 2,048 rows is the §3.6 default; it is clamped to
/// [`MIN_FILTER_TILE_SIZE`] / [`MAX_FILTER_TILE_SIZE`] by
/// [`FilterPhysical::new`]. TASK-226 only sets the default — TASK-231
/// (filter operator) and TASK-232 (engine bind) thread it through to
/// the runtime.
pub const DEFAULT_FILTER_TILE_SIZE: usize = 2_048;

/// Minimum legal tile size for [`FilterPhysical::tile_size`].
pub const MIN_FILTER_TILE_SIZE: usize = 1_024;

/// Maximum legal tile size for [`FilterPhysical::tile_size`].
pub const MAX_FILTER_TILE_SIZE: usize = 4_096;

// Compile-time invariant: future edits to the tile-size constants
// must keep the window non-empty and contain the default.
const _: () = {
    assert!(MIN_FILTER_TILE_SIZE <= DEFAULT_FILTER_TILE_SIZE);
    assert!(DEFAULT_FILTER_TILE_SIZE <= MAX_FILTER_TILE_SIZE);
};

/// Clamp a caller-supplied tile size into the `[MIN, MAX]` window.
#[inline]
fn clamp_filter_tile_size(raw: usize) -> usize {
    raw.clamp(MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE)
}

// ─────────────────────────────────────────────────────────────────────────────
// Physical plan enum
// ─────────────────────────────────────────────────────────────────────────────

/// Plain-data physical plan description.
///
/// Per `docs/design/planner-pipeline.md` §15, the planner never holds
/// `Box<dyn PhysicalOperator>`; the engine's bind step (TASK-232)
/// consumes a `PhysicalPlan` value and materializes an operator tree
/// against the concrete implementations in `bqlite-operators`.
///
/// Every Wave 2 variant is spelled out here; later waves extend the
/// enum with additional variants (`SequenceMatch`, `Aggregate`, …)
/// without renaming the existing ones.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    /// Entity-sorted scan over a catalog table. §4.1 / §9.5.
    Scan(ScanPhysical),
    /// Row-level filter over a child plan. §4.2.
    Filter(FilterPhysical),
    /// Column projection over a child plan. §4.3.
    Project(ProjectPhysical),
    /// Row cap over a child plan. §4.4.
    Limit(LimitPhysical),
    /// `CREATE TABLE` DDL. §4.5.
    CreateTable(CreateTablePhysical),
    /// `DROP TABLE` DDL. §4.6.
    DropTable(DropTablePhysical),
    /// `ALTER TABLE ADD COLUMN` DDL. §4.7.
    AlterTableAddColumn(AlterTableAddColumnPhysical),
    /// `DESCRIBE` metadata read. §4.8.
    Describe(DescribePhysical),
    /// `INSERT` DML (both `VALUES` and `FROM` bodies). §4.9.
    Insert(InsertPhysical),
    /// `EXPLAIN` meta-node wrapping a child physical plan. §4.10.
    Explain(ExplainPhysical),

    // ── Wave 3 variants ────────────────────────────────────────────────────
    /// NFA-based sequence pattern matching. Wave 3.
    ///
    /// Boxed because `SequenceMatchPhysical` carries a `CompiledNfa` (a
    /// heap-owning program object) that is much larger than any other variant.
    /// Boxing keeps the enum footprint bounded to a single pointer for this arm.
    SequenceMatch(Box<SequenceMatchPhysical>),
    /// Hash aggregation over an input plan. Wave 3.
    Aggregate(AggregatePhysical),
    /// Pipeline sort (materializes all input, then sorts). Wave 3.
    Sort(SortPhysical),
    /// Row deduplication via hash-set (streaming). Wave 3.
    Distinct(DistinctPhysical),

    // ── Wave 4 variants ────────────────────────────────────────────────────
    /// Session assignment operator. Wave 4.
    Sessionize(SessionizePhysical),
    /// Per-entity event sub-selection (FIRST/LAST/NTH). Wave 4.
    EventSelect(EventSelectPhysical),
    /// Multi-touch attribution operator. Wave 4.
    Attribute(AttributePhysical),
    /// Cohort-based entity filtering via hash-set probe. Wave 4.
    SubqueryFilter(SubqueryFilterPhysical),
    /// Deterministic entity-level sampling. Wave 4.
    Sample(SamplePhysical),
    /// N-ary entity-aligned merge of multiple table scans. Wave 4.
    ///
    /// Produced for source expressions with `JOIN` clauses. Single-table
    /// sources produce an ordinary `Scan` — no wrapping needed.
    MergeSources(MergeSourcesPhysical),

    /// `DELETE FROM <table> WHERE <pred> [ALLOW SCAN]`. Wave 4
    /// (TASK-453).
    ///
    /// Carries the same classified [`DeleteFilter`] the logical plan
    /// produced — the engine consumes the variant directly without
    /// re-walking the AST. See `docs/design/storage/deletes.md` §3 / §4
    /// for the cheap-class taxonomy and the `ALLOW SCAN` opt-in.
    Delete(DeletePhysical),
}

impl PhysicalPlan {
    /// The schema a downstream consumer observes from this plan root.
    ///
    /// Identical to the originating logical node's `output_schema()`
    /// because lowering is one-to-one and never prunes columns. The
    /// Wave 2 projection-pruning pass (TASK-228) *may* shrink
    /// `ScanPhysical::output_schema` to reflect the pruned column set,
    /// but that rewrite happens after lowering.
    pub fn output_schema(&self) -> &OperatorSchema {
        match self {
            PhysicalPlan::Scan(n) => &n.output_schema,
            PhysicalPlan::Filter(n) => &n.output_schema,
            PhysicalPlan::Project(n) => &n.output_schema,
            PhysicalPlan::Limit(n) => &n.output_schema,
            PhysicalPlan::CreateTable(n) => &n.output_schema,
            PhysicalPlan::DropTable(n) => &n.output_schema,
            PhysicalPlan::AlterTableAddColumn(n) => &n.output_schema,
            PhysicalPlan::Describe(n) => &n.output_schema,
            PhysicalPlan::Insert(n) => &n.output_schema,
            PhysicalPlan::Explain(n) => &n.output_schema,
            // Wave 3 variants — all carry an output_schema field.
            PhysicalPlan::SequenceMatch(n) => &n.output_schema,
            PhysicalPlan::Aggregate(n) => &n.output_schema,
            PhysicalPlan::Sort(n) => &n.output_schema,
            PhysicalPlan::Distinct(n) => &n.output_schema,
            // Wave 4 variants.
            PhysicalPlan::Sessionize(n) => &n.output_schema,
            PhysicalPlan::EventSelect(n) => &n.output_schema,
            PhysicalPlan::Attribute(n) => &n.output_schema,
            PhysicalPlan::SubqueryFilter(n) => &n.output_schema,
            PhysicalPlan::Sample(n) => &n.output_schema,
            PhysicalPlan::MergeSources(n) => &n.output_schema,
            PhysicalPlan::Delete(n) => &n.output_schema,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-variant descriptors
// ─────────────────────────────────────────────────────────────────────────────

/// Plain-data description of an entity-sorted scan.
///
/// Mirrors [`LogicalPlan::Scan`] with `TypedExpr` swapped for
/// [`CompiledExpr`]. The resolved `TableSchema` from the logical form
/// is unwrapped into `table` (the catalog name) — the engine bind
/// step re-resolves through its own catalog handle so the operator
/// sees the exact `TableSchema` the manifest holds at bind time.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanPhysical {
    /// Catalog name of the table being scanned.
    pub table: String,
    /// User-specified time range, resolved to absolute nanosecond bounds
    /// with `now_ns`. `None` means the user did not specify a range
    /// (unbounded). Used for row-level timestamp predicates.
    pub query_range: Option<bqlite_core::TimeRange>,
    /// Segment-reader window: `query_range` extended by
    /// `reader_backward_ns` / `reader_forward_ns` from the logical plan.
    /// Equals `query_range` when no extension applies.
    /// `None` only when `query_range` is `None`.
    pub reader_range: Option<bqlite_core::TimeRange>,
    /// Scan-level predicates populated by the predicate-pushdown pass
    /// (TASK-227). Empty at lowering time; TASK-227 rewrites
    /// `FilterPhysical`-above-`ScanPhysical` patterns to move pushable
    /// conjuncts into this vec.
    pub scan_predicates: Vec<CompiledExpr>,
    /// Column names the scan must decode, populated by the projection-
    /// pruning pass (TASK-228). Empty means "decode every declared
    /// column"; the pruning pass replaces the empty list with the
    /// minimal set demanded by downstream operators.
    pub projected_columns: Vec<String>,
    /// Cached output schema — `OperatorSchema::from_table(&table)` of
    /// the resolved logical node. TASK-228 may shrink this to match
    /// `projected_columns`; lowering always preserves the full shape.
    pub output_schema: OperatorSchema,
    /// Name of the entity-key column. Populated during lowering from the
    /// `TableSchema::entity_key_column()`. Required by the scan operator
    /// for k-way merge; the pruning pass always includes this column.
    pub entity_key_col: String,
    /// Name of the timestamp column. Populated during lowering from the
    /// `TableSchema::timestamp_column()`. Required by the scan operator
    /// for k-way merge; the pruning pass always includes this column.
    pub timestamp_col: String,
    /// Entity-level SAMPLE pushdown. Wave 4 (TASK-430). Empty at
    /// lowering time; populated by [`crate::opt::sample_pushdown`]
    /// when a parent [`SamplePhysical`] node feeds this scan through
    /// entity-key-independent intermediate stages. Carries the
    /// (fraction, seed) pair the scan operator's entity-id hash
    /// filter evaluates; `None` means no sample pushdown has been
    /// applied and the scan streams every entity the projection
    /// would normally emit.
    ///
    /// See `docs/design/operators/event-select-sample.md` §18 for
    /// the pushdown contract.
    pub sample: Option<SamplePushdown>,
}

/// Pushed-down entity-level SAMPLE parameters attached to a
/// [`ScanPhysical`] by the sample pushdown pass (TASK-430).
///
/// Mirrors the `(fraction, seed)` pair on [`SamplePhysical`] so the
/// engine bind step can materialize a
/// [`bqlite_storage::SampleFilter`] without looking up the original
/// `SamplePhysical` descriptor — the pass elides it from the plan
/// whenever the push succeeds.
#[derive(Debug, Clone, PartialEq)]
pub struct SamplePushdown {
    /// Fraction of entities to retain, in `[0.0, 1.0]`.
    pub fraction: f64,
    /// Resolved seed. The planner always substitutes
    /// [`DEFAULT_SAMPLE_SEED`] for a `None` logical seed at
    /// lowering time, so the physical-plan level never carries an
    /// unresolved `Option`.
    pub seed: i64,
}

/// Plain-data description of a vectorized row filter.
///
/// Wave 2 ships the copy-based filter from TASK-231. The
/// `tile_size` field is carried through from the descriptor so the
/// engine bind step (TASK-232) can hand a construction-time parameter
/// to `FilterOperator::new`. Later waves that fuse filter into scan
/// replace this node rather than adding fields.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPhysical {
    /// The predicate to evaluate against every input row. Guaranteed
    /// to have `result_type == BqlType::Bool` because the logical
    /// constructor enforces this invariant (see
    /// [`LogicalPlan::filter`]).
    pub predicate: CompiledExpr,
    /// The child plan feeding this filter.
    pub input: Box<PhysicalPlan>,
    /// Tile size handed to `FilterOperator::new`; clamped to
    /// `[MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE]` at construction.
    pub tile_size: usize,
    /// Identical to `input.output_schema()` — filter never changes
    /// the column shape.
    pub output_schema: OperatorSchema,
}

impl FilterPhysical {
    /// Construct a filter descriptor with a caller-supplied tile
    /// size, clamped into the legal window.
    pub fn new(predicate: CompiledExpr, input: PhysicalPlan, tile_size: usize) -> Self {
        let output_schema = input.output_schema().clone();
        Self {
            predicate,
            input: Box::new(input),
            tile_size: clamp_filter_tile_size(tile_size),
            output_schema,
        }
    }

    /// Construct a filter descriptor using [`DEFAULT_FILTER_TILE_SIZE`].
    pub fn with_default_tile_size(predicate: CompiledExpr, input: PhysicalPlan) -> Self {
        Self::new(predicate, input, DEFAULT_FILTER_TILE_SIZE)
    }
}

/// Plain-data description of a projection operator.
///
/// The output schema is built at lowering time from each expression's
/// `result_type` / `nullable`; projecting a column through a
/// `CompiledExpr::Column` preserves its input-side type verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectPhysical {
    /// Ordered output expressions, each paired with its emitted name.
    pub expressions: Vec<ProjectPhysicalItem>,
    /// The child plan feeding this projection.
    pub input: Box<PhysicalPlan>,
    /// Output schema — derived from `expressions` at lowering time.
    /// Cached so the engine bind step (and EXPLAIN) does not pay a
    /// schema rebuild per traversal.
    pub output_schema: OperatorSchema,
}

/// A single output item in a [`ProjectPhysical`].
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectPhysicalItem {
    /// The compiled output expression. Evaluated per input batch by
    /// TASK-231's project operator.
    pub expr: CompiledExpr,
    /// Final output column name — carried through from the logical
    /// [`ProjectItem::output_name`] unchanged.
    pub output_name: String,
}

/// Plain-data description of a row-count limit.
#[derive(Debug, Clone, PartialEq)]
pub struct LimitPhysical {
    /// Maximum number of rows to emit.
    pub count: u64,
    /// The child plan feeding this limit.
    pub input: Box<PhysicalPlan>,
    /// Identical to `input.output_schema()`.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `CREATE TABLE`.
///
/// Holds the validated *pieces* of a future `TableSchema` rather than
/// a `TableSchema` value because the table does not yet exist in the
/// catalog. The engine bind step (TASK-232) reconstructs a
/// `TableSchema` via `TableSchema::new(name, columns, entity_key,
/// event_time, event_type)` and atomically registers it through the
/// manifest API (TASK-217).
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTablePhysical {
    /// Target table name.
    pub name: String,
    /// Declared columns in DDL order, already normalized to
    /// `bqlite_core::ColumnDef`.
    pub columns: Vec<ColumnDef>,
    /// Name of the `ENTITY KEY` column.
    pub entity_key: String,
    /// Name of the `EVENT TIME` column.
    pub event_time: String,
    /// Name of the `EVENT TYPE` column.
    pub event_type: String,
    /// Empty — DDL produces no rows.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `DROP TABLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTablePhysical {
    /// Target table name.
    pub name: String,
    /// Empty.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `ALTER TABLE ADD COLUMN`.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableAddColumnPhysical {
    /// Target table name.
    pub name: String,
    /// The new column, already validated against the schema-evolution
    /// rules from `type-system.md` §5.3.
    pub column: ColumnDef,
    /// Empty.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `DESCRIBE <table>`.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribePhysical {
    /// Target table name — the engine looks up the `TableSchema` via
    /// its own catalog handle at bind time.
    pub name: String,
    /// Fixed four-column schema: `(name, type, nullable, role)`.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `INSERT INTO <table> <body>`.
///
/// Carries the resolved `TableSchema` so the engine bind step can run
/// type-coercion / partitioning without re-hitting the catalog. The
/// body re-uses [`InsertLogicalBody`] directly because the logical
/// and physical representations are identical plain data — no
/// expression compilation is needed on either path (TASK-238 / TASK-233
/// consume `InsertLogicalBody` as-is).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertPhysical {
    /// Catalog-resolved target table.
    pub table: TableSchema,
    /// Resolved insert body — literals coerced for `VALUES`, options
    /// normalized and map resolved for `FROM`.
    pub body: InsertLogicalBody,
    /// Empty.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `EXPLAIN <pipeline>`.
///
/// Wraps the child physical plan rather than the raw pipeline so the
/// pipeline is type-checked eagerly (an `EXPLAIN` of an ill-typed
/// query still raises the underlying error — EXPLAIN does not hide
/// failures). TASK-229's `ExplainNode` builder walks this child tree
/// to produce the final text form.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainPhysical {
    /// The child physical plan being explained.
    pub plan: Box<PhysicalPlan>,
    /// Fixed single-column schema: `(plan: String)`.
    pub output_schema: OperatorSchema,
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 3 physical descriptors
// ─────────────────────────────────────────────────────────────────────────────

/// A single compiled aggregate expression on a physical plan node.
///
/// Produced by TASK-318's physical lowering from a [`crate::logical::TypedAggExpr`].
/// Consumed by the `HashAccumulator` inside the `HashAggregateOperator` (TASK-307).
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledAgg {
    /// The resolved aggregate function.
    pub function: AggFunction,
    /// Compiled argument expression. `None` for `COUNT(*)`.
    pub arg: Option<CompiledExpr>,
    /// Output column name — from the BQL alias.
    pub output_name: String,
}

/// Physical descriptor for the sequence-matching operator. Wave 3.
///
/// Produced by TASK-318's physical lowering from
/// [`crate::logical::LogicalPlan::SequenceMatch`]. Materialized into a
/// `SequenceMatchOperator` (TASK-321) by the engine bind step (TASK-323).
///
/// See `docs/design/planner/wave3-lowering.md` §2.1 and
/// `docs/design/operators/match-operator.md` for the full specification.
#[derive(Debug, Clone)]
pub struct SequenceMatchPhysical {
    /// The compiled NFA program produced by TASK-311's `compile_pattern`.
    pub compiled_nfa: CompiledNfa,
    /// Execution strategy selected by `select_strategy(pattern_class, config)`.
    pub strategy: MatchStrategy,
    /// Whether to reset and continue after each match (MATCH ALL) or
    /// stop after the first match per binding track (MATCH FIRST).
    /// Derived from `MatchMode::All` vs `MatchMode::First` during
    /// physical lowering. `emit_all` is already on `CompiledNfa`.
    pub match_all: bool,
    /// Downstream demand set populated by demand analysis (Pass 4).
    /// Drives column pruning and step-property forwarding.
    pub demand: crate::demand::DemandSet,
    /// Execution configuration derived from demand (controls strategy).
    pub execution_config: MatchExecutionConfig,
    /// Fused downstream aggregate, if the match-aggregate fusion optimizer
    /// (TASK-320) detected a fusable Aggregate immediately downstream.
    /// `None` until Pass 6 runs.
    pub fused_aggregate: Option<CompiledFusableAggregate>,
    /// Child plan (scan / filter feeding this match operator).
    pub input: Box<PhysicalPlan>,
    /// Output schema — pruned from the maximum schema by demand analysis.
    pub output_schema: OperatorSchema,
}

impl SequenceMatchPhysical {
    /// Planner-side capability declaration. Must match
    /// `SequenceMatchOperator::supported_demands()` — enforced by
    /// contract tests in `bqlite-operators`.
    pub const DEMAND_CAPS: DemandCapabilities = DemandCapabilities {
        supports_step_reached: true,
        supports_match_count: true,
        supports_full_detail: true,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: true,
        supports_forwarded_columns: false,
        supports_eager_group_emit: false,
    };
}

// Manual PartialEq because CompiledNfa derives Debug+Clone but not PartialEq.
// `compiled_nfa` is intentionally excluded: structural NFA equality is not
// required for plan-equivalence tests (two compilations of the same pattern
// produce identical NFA structure, but the comparison cost is unjustified for
// test assertions). If NFA structural equality becomes necessary, PartialEq
// must be derived or implemented for CompiledNfa and its subtypes.
// `execution_config` IS compared: its flags (track_match_duration,
// track_match_events) are runtime-significant and affect strategy selection.
impl PartialEq for SequenceMatchPhysical {
    fn eq(&self, other: &Self) -> bool {
        self.strategy == other.strategy
            && self.match_all == other.match_all
            && self.execution_config == other.execution_config
            && self.demand == other.demand
            && self.fused_aggregate == other.fused_aggregate
            && self.input == other.input
            && self.output_schema == other.output_schema
    }
}

/// Physical descriptor for hash aggregation. Wave 3.
///
/// Produced by TASK-318's physical lowering from
/// [`crate::logical::LogicalPlan::Aggregate`]. Materialized into a
/// `HashAggregateOperator` (TASK-307) by the engine bind step (TASK-323).
///
/// See `docs/design/operators/aggregate-operator.md` for the full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregatePhysical {
    /// Compiled aggregate expressions (function + arg + output name).
    pub aggregates: Vec<CompiledAgg>,
    /// Compiled group-by key expressions paired with their output column names.
    pub group_by: Vec<(CompiledExpr, String)>,
    /// Hard cap on group cardinality. Default: 1,000,000.
    /// Matches `DEFAULT_MAX_GROUPS` from aggregate-operator.md §4.3.
    pub max_groups: usize,
    /// Child plan feeding this aggregate.
    pub input: Box<PhysicalPlan>,
    /// Output schema: group-by columns first, aggregate columns next.
    pub output_schema: OperatorSchema,
}

/// Default maximum group count for [`AggregatePhysical`] and
/// [`DistinctPhysical`].
///
/// 1,000,000 groups × ~100 bytes each ≈ 100 MB, within the 3 GB query
/// budget (aggregate-operator.md §4.3, sort-distinct.md §4.6).
pub const DEFAULT_MAX_GROUPS: usize = 1_000_000;

/// Default maximum row count for [`SortPhysical`].
///
/// 10,000,000 rows × ~100 bytes each ≈ 1 GB, with headroom in a 3 GB
/// query budget (sort-distinct.md §3.6).
pub const DEFAULT_SORT_MAX_ROWS: usize = 10_000_000;

/// Physical descriptor for pipeline sort. Wave 3.
///
/// Produced by TASK-318's physical lowering from
/// [`crate::logical::LogicalPlan::Sort`]. Materialized into a
/// `SortOperator` (TASK-322) by the engine bind step (TASK-323).
///
/// See `docs/design/operators/sort-distinct.md` §3 for the full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct SortPhysical {
    /// Compiled sort key expressions in priority order (primary, secondary, …).
    /// Each key is a `CompiledExpr` that evaluates to a scalar value over an
    /// input batch, plus a direction controlling null-ordering.
    pub keys: Vec<(CompiledExpr, SortDirection)>,
    /// Hard cap on total input rows. Default: 10,000,000.
    /// Returns `BqliteError::Execution` if exceeded — no spill in Wave 3.
    pub max_rows: usize,
    /// Child plan feeding this sort.
    pub input: Box<PhysicalPlan>,
    /// Identical to `input.output_schema()` — Sort never changes the schema.
    pub output_schema: OperatorSchema,
}

/// Physical descriptor for row deduplication. Wave 3.
///
/// Produced by TASK-318's physical lowering from
/// [`crate::logical::LogicalPlan::Distinct`]. Materialized into a
/// `DistinctOperator` (TASK-322) by the engine bind step (TASK-323).
///
/// See `docs/design/operators/sort-distinct.md` §4 for the full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctPhysical {
    /// Hard cap on distinct row count. Default: 1,000,000.
    /// Returns [`bqlite_core::BqliteError::MaxGroupsExceeded`] if exceeded
    /// — no spill in Wave 3.
    pub max_groups: usize,
    /// Child plan feeding this distinct operator.
    pub input: Box<PhysicalPlan>,
    /// Identical to `input.output_schema()` — Distinct never changes the schema.
    pub output_schema: OperatorSchema,
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 physical descriptors
// ─────────────────────────────────────────────────────────────────────────────

/// Physical descriptor for the SESSIONIZE operator. Wave 4.
///
/// Produced by TASK-425's physical lowering from
/// [`crate::logical::LogicalPlan::Sessionize`]. Materialized into a
/// `SessionizeOperator` (TASK-428) by the engine bind step (TASK-438).
///
/// See `docs/design/operators/sessionize.md` §4 for the full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionizePhysical {
    /// Minimum inactivity gap (nanoseconds) that triggers a new session.
    /// Boundary is exclusive: new session iff delta > gap_ns.
    pub gap_ns: i64,
    /// Event types that explicitly end a session. Empty = gap-only mode.
    pub end_events: Vec<String>,
    /// Demand set from downstream operators.
    pub demand: DemandSet,
    /// Columns that downstream operators need forwarded through the
    /// session buffer.
    pub forwarded_columns: Vec<ColumnId>,
    /// Fused aggregate specification. `Some` after the stateful-aggregate
    /// fusion pass replaces a downstream `Aggregate` with a fused per-entity
    /// accumulator update. See `docs/design/planner-pipeline.md` §7.4.2 and
    /// `docs/design/engine/operator-fusion.md` §5.1 (TASK-520).
    pub fused_aggregate: Option<CompiledFusableAggregate>,
    /// Child plan feeding this sessionize operator.
    pub input: Box<PhysicalPlan>,
    /// Output schema. When `fused_aggregate` is `None`, this is the
    /// operator's native session-row schema (`input ∪ {session_id,
    /// session_duration}`). When `fused_aggregate` is `Some`, the fusion
    /// pass replaces this with the aggregate's output schema and stashes
    /// the original native schema in [`pre_fusion_output_schema`] so the
    /// runtime operator can still construct per-entity batches.
    pub output_schema: OperatorSchema,
    /// Native operator-output schema preserved across fusion. `None`
    /// before fusion (or when no fusion fires); `Some(_)` after fusion
    /// stores the schema that `output_schema` had immediately before it
    /// was replaced with the aggregate output schema. Operators read
    /// `pre_fusion_output_schema.as_ref().unwrap_or(&output_schema)` to
    /// know the native schema for batch building. See TASK-520.
    pub pre_fusion_output_schema: Option<OperatorSchema>,
}

impl SessionizePhysical {
    /// Planner-side capability declaration. Must match the runtime
    /// `SessionizeOperator::supported_demands()` — enforced by contract tests.
    pub const DEMAND_CAPS: DemandCapabilities = DemandCapabilities {
        supports_step_reached: false,
        supports_match_count: false,
        supports_full_detail: false,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: false,
        supports_forwarded_columns: true,
        supports_eager_group_emit: false,
    };

    /// Native (pre-fusion) output schema. Returns
    /// `pre_fusion_output_schema` when populated by the fusion pass, else
    /// `output_schema` (which is the native schema when not fused).
    pub fn native_output_schema(&self) -> &OperatorSchema {
        self.pre_fusion_output_schema
            .as_ref()
            .unwrap_or(&self.output_schema)
    }
}

/// Physical descriptor for EventSelect (FIRST/LAST/NTH). Wave 4.
///
/// Produced by TASK-425's physical lowering from
/// [`crate::logical::LogicalPlan::EventSelect`]. Materialized into an
/// `EventSelectOperator` (TASK-429) by the engine bind step (TASK-438).
///
/// See `docs/design/operators/event-select-sample.md` Block A for the
/// full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct EventSelectPhysical {
    /// Selection mode: FIRST, LAST, or NTH(n).
    pub kind: EventSelectKind,
    /// Event types eligible for selection. Length >= 1.
    pub event_types: Vec<String>,
    /// Optional per-event predicate, compiled from WHERE clause.
    pub predicate: Option<CompiledExpr>,
    /// Scan-range backward extension for FIRST/NTH. `None` for LAST.
    pub lookback: Option<i64>,
    /// Columns that downstream operators need forwarded through the
    /// candidate row.
    pub forwarded_columns: Vec<ColumnId>,
    /// Fused aggregate specification. `Some` after stateful-aggregate
    /// fusion (TASK-520). See planner-pipeline.md §7.4.3.
    pub fused_aggregate: Option<CompiledFusableAggregate>,
    /// Child plan feeding this event-select operator.
    pub input: Box<PhysicalPlan>,
    /// Output schema. Native single-row-per-entity schema when not fused;
    /// aggregate output schema when `fused_aggregate.is_some()`. The
    /// pre-fusion native schema is preserved in
    /// [`pre_fusion_output_schema`].
    pub output_schema: OperatorSchema,
    /// Native (pre-fusion) output schema preserved across fusion. See
    /// `SessionizePhysical::pre_fusion_output_schema`.
    pub pre_fusion_output_schema: Option<OperatorSchema>,
}

impl EventSelectPhysical {
    /// Planner-side capability declaration. Must match the runtime
    /// `EventSelectOperator::supported_demands()` — enforced by contract tests.
    pub const DEMAND_CAPS: DemandCapabilities = DemandCapabilities {
        supports_step_reached: false,
        supports_match_count: false,
        supports_full_detail: false,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: false,
        supports_forwarded_columns: true,
        supports_eager_group_emit: false,
    };

    /// Native (pre-fusion) output schema.
    pub fn native_output_schema(&self) -> &OperatorSchema {
        self.pre_fusion_output_schema
            .as_ref()
            .unwrap_or(&self.output_schema)
    }
}

/// Physical descriptor for the ATTRIBUTE operator. Wave 4.
///
/// Produced by TASK-425's physical lowering from
/// [`crate::logical::LogicalPlan::Attribute`]. Materialized into an
/// `AttributeOperator` (TASK-431) by the engine bind step (TASK-438).
///
/// See `docs/design/operators/attribute.md` for the full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct AttributePhysical {
    /// Event type(s) that trigger conversion emission.
    pub conversion_events: Vec<String>,
    /// Event type(s) eligible as touchpoints.
    pub touchpoint_events: Vec<String>,
    /// Lookback window in nanoseconds.
    pub window_ns: i64,
    /// Compiled expression evaluated per qualifying touchpoint; result
    /// becomes the `touchpoint_key` output column.
    pub touchpoint_key: CompiledExpr,
    /// Demand-driven forwarded conversion properties.
    pub forwarded_conversion_columns: Vec<ColumnId>,
    /// Fused aggregate specification. `Some` after stateful-aggregate
    /// fusion (TASK-520). See planner-pipeline.md §7.4.4.
    pub fused_aggregate: Option<CompiledFusableAggregate>,
    /// Original query time range `(start_ns, end_ns)` for scan-extension-
    /// aware conversion emission filtering. When the planner widens the
    /// scan backward by `window` (attribute.md §12), only conversions
    /// whose `conversion_ts` falls within this original range trigger
    /// emission — touchpoints from the extended zone are deque material
    /// only. `None` when no time range is specified (unbounded scan).
    pub conversion_range: Option<(i64, i64)>,
    /// Child plan feeding this attribute operator.
    pub input: Box<PhysicalPlan>,
    /// Output schema. Native flat-row schema when not fused; aggregate
    /// schema when `fused_aggregate.is_some()`. Pre-fusion native schema
    /// preserved in [`pre_fusion_output_schema`].
    pub output_schema: OperatorSchema,
    /// Native (pre-fusion) output schema preserved across fusion. See
    /// `SessionizePhysical::pre_fusion_output_schema`.
    pub pre_fusion_output_schema: Option<OperatorSchema>,
}

impl AttributePhysical {
    /// Planner-side capability declaration. Must match the runtime
    /// `AttributeOperator::supported_demands()` — enforced by contract tests.
    pub const DEMAND_CAPS: DemandCapabilities = DemandCapabilities {
        supports_step_reached: false,
        supports_match_count: false,
        supports_full_detail: false,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: false,
        supports_forwarded_columns: true,
        supports_eager_group_emit: false,
    };

    /// Native (pre-fusion) output schema.
    pub fn native_output_schema(&self) -> &OperatorSchema {
        self.pre_fusion_output_schema
            .as_ref()
            .unwrap_or(&self.output_schema)
    }
}

/// Physical descriptor for cohort-based SubqueryFilter. Wave 4.
///
/// Produced by TASK-425's physical lowering from
/// [`crate::logical::LogicalPlan::SubqueryFilter`]. Materialized into a
/// `SubqueryFilterOperator` (TASK-437) by the engine bind step (TASK-438).
///
/// The engine bind step executes the `subquery` plan to materialize a
/// hash set, then wires the set into the operator. The physical plan
/// carries the subquery as a child plan (plain data), not the runtime
/// hash set.
///
/// See `docs/design/language/cohorts-aliases-joins.md` §4 for the full
/// spec.
#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryFilterPhysical {
    /// Compiled LHS column expression(s) for the IN check. Length 1 for
    /// single-column cohorts; length N for tuple cohorts.
    pub lhs_columns: Vec<CompiledExpr>,
    /// Inner pipeline producing the cohort set. Executed at query start
    /// by the engine bind step.
    pub subquery: Box<PhysicalPlan>,
    /// Outer input stream being filtered.
    pub input: Box<PhysicalPlan>,
    /// Identical to `input.output_schema()` — filter, not transform.
    pub output_schema: OperatorSchema,
}

/// Physical descriptor for deterministic entity-level SAMPLE. Wave 4.
///
/// Produced by TASK-425's physical lowering from
/// [`crate::logical::LogicalPlan::Sample`]. SAMPLE is pushed down to the
/// scan layer by TASK-430; at the physical plan level it is a standalone
/// node that the engine bind step may fuse with the scan.
///
/// See `docs/design/operators/event-select-sample.md` Block B for the
/// full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct SamplePhysical {
    /// Fraction of entities to keep, in `[0.0, 1.0]`.
    pub fraction: f64,
    /// Resolved seed for deterministic sampling. When the logical plan's
    /// `seed` is `None`, the engine bind step substitutes a database-
    /// UUID-derived default; at the physical-plan level a seed is always
    /// present.
    pub seed: i64,
    /// Child plan feeding this sample operator.
    pub input: Box<PhysicalPlan>,
    /// Identical to `input.output_schema()` — SAMPLE never changes the
    /// column shape.
    pub output_schema: OperatorSchema,
}

/// Default SAMPLE seed used when the user does not provide an explicit
/// `seed:` parameter. The engine bind step replaces this with the
/// database-UUID-derived seed; 0 is used as a placeholder at the
/// planner level.
pub const DEFAULT_SAMPLE_SEED: i64 = 0;

/// Physical descriptor for N-ary entity-aligned source merge. Wave 4.
///
/// Produced by TASK-425's source-resolution step when the source
/// expression contains one or more `JOIN` clauses. Single-table
/// source expressions produce an ordinary `Scan` — no wrapping.
///
/// The operator performs a k-way merge over N independent entity-sorted
/// scans (one per joined table), emitting a unified entity-sorted event
/// stream with a `__source_table_id` discriminator column.
///
/// See `docs/design/language/cohorts-aliases-joins.md` §3.7–3.8 for
/// the full spec.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeSourcesPhysical {
    /// One scan descriptor per joined table, in JOIN-clause order.
    /// Index 0 is the primary table; indices 1..N are the joined tables.
    pub tables: Vec<ScanPhysical>,
    /// Merge sort key specification. The canonical order is
    /// `(entity_id ASC, ts ASC, table_order ASC, __seq_id ASC)` per
    /// cohorts-aliases-joins.md §3.2. Carried explicitly so EXPLAIN can
    /// render it and future waves can parameterize it.
    pub order: Vec<(String, SortDirection)>,
    /// Table-id → table-name map. `table_id_map[i]` is the catalog name
    /// of the table whose events carry `__source_table_id == i`.
    /// Length equals `tables.len()`.
    pub table_id_map: Vec<String>,
    /// Output schema: union of all joined tables' declared columns plus
    /// `__source_table_id: Int NOT NULL`.
    ///
    /// Note: the design doc specifies `Int8` for `__source_table_id`,
    /// but `BqlType` has no `Int8` variant. The planner uses `Int` (i64);
    /// the operator layer may narrow to `Int8` in the Arrow representation.
    pub output_schema: OperatorSchema,
}

/// Plain-data description of `DELETE FROM <table> WHERE <pred>
/// [ALLOW SCAN]`. Wave 4 (TASK-453).
///
/// Carries the resolved table reference and the engine-ready filter:
///
/// - For [`PhysicalDeleteFilter::Cheap`] the engine writes per-shard
///   tombstone files directly from the decomposed spec.
/// - For [`PhysicalDeleteFilter::AllowScan`] the engine builds a
///   `Filter(Scan)` driver, materializes matching `__seq_id`s, and
///   writes row-level tombstones to the contributing shards.
///
/// `output_schema` is empty per `deletes.md` SS11 — DELETE produces
/// no result rows; the count travels alongside the
/// `ExecutionResult::rows_affected` field at engine level.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletePhysical {
    /// Catalog name of the target table — re-resolved against the
    /// engine's manifest catalog at bind time so the operator sees
    /// the manifest's current schema.
    pub table_name: String,
    /// Entity-key column name on the target table. Carried so the
    /// engine does not need to re-resolve via the catalog for shard
    /// targeting and entity-level row counting.
    pub entity_key_col: String,
    /// Timestamp column name on the target table.
    pub timestamp_col: String,
    /// Engine-ready filter description.
    pub filter: PhysicalDeleteFilter,
    /// `true` when the source statement carried `ALLOW SCAN`. Carried
    /// for diagnostics; the engine branches on `filter`'s variant.
    pub allow_scan: bool,
    /// Empty schema — DELETE produces no rows.
    pub output_schema: OperatorSchema,
}

/// Engine-ready DELETE filter, mirroring [`crate::logical::DeleteFilter`]
/// with `TypedExpr` swapped for `CompiledExpr`.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalDeleteFilter {
    /// Cheap-class direct tombstone writes.
    Cheap(crate::logical::CheapDeleteSpec),
    /// `ALLOW SCAN` — engine drives a scan with this compiled
    /// predicate to materialize matching `__seq_id`s.
    AllowScan { predicate: CompiledExpr },
}

// ─────────────────────────────────────────────────────────────────────────────
// Time-range resolution helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Resolve an AST time-range (still carrying string timestamps or relative
/// nanosecond durations) into an absolute [`bqlite_core::TimeRange`] using
/// `now_ns` as the current time.
///
/// Returns `None` when `tr` is `None` (unbounded scan).
fn resolve_ast_time_range(
    tr: Option<&bqlite_ast::pipeline::TimeRange>,
    now_ns: i64,
) -> bqlite_core::Result<Option<bqlite_core::TimeRange>> {
    use bqlite_ast::pipeline::TimeRange as AstTr;
    use bqlite_core::{TimeRange, Timestamp};
    let Some(tr) = tr else { return Ok(None) };
    match tr {
        AstTr::Last(ns) => {
            let end = Timestamp::from_nanos(now_ns);
            let start = end.checked_sub_nanos(*ns).unwrap_or(Timestamp::MIN);
            Ok(Some(TimeRange::new(start, end)))
        }
        AstTr::Between { start, end } => {
            let start_ts = parse_time_range_ts(start)?;
            let end_ts = parse_time_range_ts(end)?;
            let exclusive_end = end_ts.checked_add_nanos(1).unwrap_or(Timestamp::MAX);
            Ok(Some(TimeRange::new(start_ts, exclusive_end)))
        }
    }
}

/// Parse an RFC 3339 timestamp string into a [`bqlite_core::Timestamp`].
fn parse_time_range_ts(raw: &str) -> bqlite_core::Result<bqlite_core::Timestamp> {
    use bqlite_core::{BqlType, BqliteError, PropertyValue, Timestamp};
    match PropertyValue::String(raw.to_string()).coerce_to(&BqlType::Timestamp) {
        Some(PropertyValue::Timestamp(ns)) => Ok(Timestamp::from_nanos(ns)),
        _ => Err(BqliteError::Plan(format!(
            "`{raw}` is not a valid RFC 3339 timestamp"
        ))),
    }
}

/// Walk downward through a [`LogicalPlan`] to find the primary `Scan`
/// whose `time_range` is a `LAST <dur>` form, and resolve it into an
/// absolute `(start_ns, end_ns)` pair using `now_ns`.
///
/// This is the physical-layer fallback for
/// [`LogicalPlan::Attribute.conversion_range`] (operators/attribute.md §12):
/// `BETWEEN` ranges are captured at logical-lowering time (they do not need
/// a clock), but `LAST` ranges need `now_ns` which is only available here.
///
/// Returns `None` when:
/// - no primary scan exists (DDL/DML shapes),
/// - the scan carries no time range (unbounded),
/// - the scan already carries a `BETWEEN` range (the logical layer should
///   have captured it directly; we do not re-extract here).
fn resolve_last_range_from_scan(plan: &LogicalPlan, now_ns: i64) -> Option<(i64, i64)> {
    match crate::logical::find_primary_scan(plan)? {
        LogicalPlan::Scan {
            time_range: time_range @ Some(bqlite_ast::pipeline::TimeRange::Last(_)),
            ..
        } => {
            let resolved = resolve_ast_time_range(time_range.as_ref(), now_ns).ok()??;
            Some((resolved.start.as_nanos(), resolved.end.as_nanos()))
        }
        _ => None,
    }
}

/// Apply reader extension to a base [`bqlite_core::TimeRange`], returning
/// a widened range (or `None` when the base is `None`).
fn apply_reader_extension(
    base: Option<bqlite_core::TimeRange>,
    backward_ns: i64,
    forward_ns: i64,
) -> Option<bqlite_core::TimeRange> {
    use bqlite_core::{TimeRange, Timestamp};
    let r = base?;
    if backward_ns == 0 && forward_ns == 0 {
        return Some(r);
    }
    let new_start = r
        .start
        .checked_sub_nanos(backward_ns)
        .unwrap_or(Timestamp::MIN);
    let new_end = r
        .end
        .checked_add_nanos(forward_ns)
        .unwrap_or(Timestamp::MAX);
    Some(TimeRange::new(new_start, new_end))
}

// ─────────────────────────────────────────────────────────────────────────────
// Logical → physical lowering
// ─────────────────────────────────────────────────────────────────────────────

/// Infallibly lower a [`LogicalPlan`] tree into a [`PhysicalPlan`].
///
/// Every Wave 2 logical variant lowers one-for-one. The walk is
/// infallible because the logical tree is pre-validated: the
/// logical constructors enforce the schema-at-construction-time
/// invariant (see `docs/design/planner/logical-plan-nodes.md` §4),
/// so this function only needs to swap expression types and reshape
/// a few fields.
///
/// `now_ns` is the current Unix epoch in nanoseconds, used to resolve
/// `LAST <dur>` time ranges into absolute `[start, end)` bounds.
/// Pass `0` in tests that do not exercise time ranges.
pub fn lower_physical(plan: LogicalPlan, now_ns: i64) -> PhysicalPlan {
    match plan {
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
            // Compile any optimizer-populated scan predicates into
            // runtime form. Wave 2 lowering always sees an empty list
            // here (TASK-227 populates the *physical* scan_predicates
            // as a post-lowering rewrite) — compiling the logical
            // predicates is future-proof for a hypothetical logical-
            // phase pushdown without changing this call site.
            let compiled_predicates: Vec<CompiledExpr> = scan_predicates
                .iter()
                .map(CompiledExpr::from_typed)
                .collect();
            let query_range =
                resolve_ast_time_range(time_range.as_ref(), now_ns).unwrap_or_else(|e| {
                    panic!(
                        "time range validation failed (logical phase should have caught this): {e}"
                    )
                });
            let reader_range =
                apply_reader_extension(query_range, reader_backward_ns, reader_forward_ns);

            if joined_tables.is_empty() {
                PhysicalPlan::Scan(ScanPhysical {
                    table: table.name().to_string(),
                    query_range,
                    reader_range,
                    scan_predicates: compiled_predicates,
                    projected_columns,
                    output_schema,
                    entity_key_col: table.entity_key_column().name.clone(),
                    timestamp_col: table.timestamp_column().name.clone(),
                    sample: None,
                })
            } else {
                // Entity-aligned multi-table source (TASK-425 CP5b). Fan out
                // to one ScanPhysical per table (primary + joined), sharing
                // the same `query_range` / `reader_range`. Predicates and
                // projection from the combined-schema logical Scan are not
                // forwarded because they reference dotted column names that
                // do not exist in per-table schemas; the joined-scan runtime
                // in TASK-436 owns the qualified-to-bare rewrite.
                //
                // The debug_assert mirrors the "fail loud on unexpected
                // shape" invariant the single-table path has held since
                // Wave 2: if a future optimizer pass starts populating
                // these fields against a joined scan before TASK-436 lands
                // the rewrite, this assertion catches it in debug builds.
                debug_assert!(
                    compiled_predicates.is_empty() && projected_columns.is_empty(),
                    "joined-source pushdown / pruning not yet implemented; \
                     TASK-436 owns the qualified-to-bare rewrite"
                );
                let _ = compiled_predicates;
                let _ = projected_columns;

                let all_tables: Vec<&TableSchema> = std::iter::once(&table)
                    .chain(joined_tables.iter())
                    .collect();
                let table_id_map: Vec<String> =
                    all_tables.iter().map(|t| t.name().to_string()).collect();
                let tables: Vec<ScanPhysical> = all_tables
                    .iter()
                    .map(|t| ScanPhysical {
                        table: t.name().to_string(),
                        query_range,
                        reader_range,
                        scan_predicates: Vec::new(),
                        projected_columns: Vec::new(),
                        output_schema: OperatorSchema::from_table(t),
                        entity_key_col: t.entity_key_column().name.clone(),
                        timestamp_col: t.timestamp_column().name.clone(),
                        sample: None,
                    })
                    .collect();
                // Canonical merge order from cohorts-aliases-joins.md §3.2.
                // These names refer to the **post-merge canonical output
                // columns**, not per-sub-scan input column names:
                // cohorts-aliases-joins.md line 366 declares "the output
                // entity-key column is always named `entity_id` regardless
                // of the underlying tables' entity-key column names", and
                // the matching convention applies to `ts`. Each sub-scan
                // still carries its own table-local `entity_key_col` /
                // `timestamp_col` above.
                // `__table_order` is a synthetic key the MergeSources
                // operator interprets by sub-scan index (not a real column);
                // it is stored explicitly so EXPLAIN can render the full
                // merge-key shape.
                let order = vec![
                    ("entity_id".to_string(), SortDirection::Asc),
                    ("ts".to_string(), SortDirection::Asc),
                    ("__table_order".to_string(), SortDirection::Asc),
                    (
                        bqlite_core::schema::SEQ_ID_COLUMN.to_string(),
                        SortDirection::Asc,
                    ),
                ];
                PhysicalPlan::MergeSources(MergeSourcesPhysical {
                    tables,
                    order,
                    table_id_map,
                    output_schema,
                })
            }
        }

        LogicalPlan::Filter {
            predicate,
            input,
            output_schema,
        } => {
            let compiled = CompiledExpr::from_typed(&predicate);
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Filter(FilterPhysical {
                predicate: compiled,
                input: Box::new(child),
                tile_size: DEFAULT_FILTER_TILE_SIZE,
                output_schema,
            })
        }

        LogicalPlan::Project {
            expressions,
            input,
            output_schema,
        } => {
            let compiled_items: Vec<ProjectPhysicalItem> = expressions
                .into_iter()
                .map(|ProjectItem { expr, output_name }| ProjectPhysicalItem {
                    expr: CompiledExpr::from_typed(&expr),
                    output_name,
                })
                .collect();
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Project(ProjectPhysical {
                expressions: compiled_items,
                input: Box::new(child),
                output_schema,
            })
        }

        LogicalPlan::Limit {
            count,
            input,
            output_schema,
        } => {
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Limit(LimitPhysical {
                count,
                input: Box::new(child),
                output_schema,
            })
        }

        LogicalPlan::CreateTable {
            name,
            columns,
            entity_key,
            event_time,
            event_type,
            output_schema,
        } => PhysicalPlan::CreateTable(CreateTablePhysical {
            name,
            columns,
            entity_key,
            event_time,
            event_type,
            output_schema,
        }),

        LogicalPlan::DropTable {
            name,
            output_schema,
        } => PhysicalPlan::DropTable(DropTablePhysical {
            name,
            output_schema,
        }),

        LogicalPlan::AlterTableAddColumn {
            name,
            column,
            output_schema,
        } => PhysicalPlan::AlterTableAddColumn(AlterTableAddColumnPhysical {
            name,
            column,
            output_schema,
        }),

        LogicalPlan::Describe {
            name,
            output_schema,
        } => PhysicalPlan::Describe(DescribePhysical {
            name,
            output_schema,
        }),

        LogicalPlan::Insert {
            table,
            body,
            output_schema,
        } => PhysicalPlan::Insert(InsertPhysical {
            table,
            body,
            output_schema,
        }),

        LogicalPlan::Explain {
            plan,
            output_schema,
        } => {
            let child = lower_physical(*plan, now_ns);
            PhysicalPlan::Explain(ExplainPhysical {
                plan: Box::new(child),
                output_schema,
            })
        }

        // ── Wave 3 variants ────────────────────────────────────────────
        LogicalPlan::SequenceMatch {
            pattern,
            mode,
            emit_all: _,
            window: _,
            brackets: _,
            step_properties,
            fused_downstream,
            input,
            output_schema,
        } => {
            // Compile the AST-level pattern into a CompiledNfa via TASK-311's
            // pattern compiler. Uses the input schema for column resolution.
            let input_schema = input.output_schema().clone();
            let registry = crate::expr::FunctionRegistry::with_builtins();
            let compiled_nfa =
                crate::compile::compile_pattern(&pattern.inner, &input_schema, &registry)
                    .unwrap_or_else(|e| {
                        panic!(
                            "compile_pattern failed on pre-validated pattern \
                             (this indicates a bug in logical lowering): {e}"
                        )
                    });

            // Build execution config from demand analysis results.
            let demand = crate::demand::DemandSet {
                columns: output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect(),
                needs_match_detail: output_schema.column("match_duration").is_some()
                    || output_schema.column("match_events").is_some(),
                needs_step_reached: output_schema.column("step_reached").is_some(),
                step_properties: step_properties.clone(),
                forwarded: Vec::new(),
                fused_aggregate: None,
                fused_filter: None,
            };

            let execution_config = MatchExecutionConfig {
                track_match_duration: output_schema.column("match_duration").is_some(),
                // `match_events` is still materialized as a typed NULL column.
                // Until real trace tracking lands, it should not force the
                // general NFA path for otherwise-linear patterns.
                track_match_events: false,
            };

            // Select strategy based on pattern class and demand.
            let strategy =
                crate::compile::select_strategy(compiled_nfa.pattern_class, &execution_config);

            // Compile fused downstream aggregate if present (TASK-320).
            // `FusableAggregate` (logical form) does not carry `max_groups`
            // because fusion via the logical path is not yet exercised
            // (logical lowering never sets `fused_downstream`). The physical
            // pass (TASK-320) sets `fused_aggregate` directly with the
            // correct `max_groups` value; this arm is a forward-compat path
            // that falls back to `DEFAULT_MAX_GROUPS`.
            let fused_aggregate =
                fused_downstream.map(|fd| crate::demand::CompiledFusableAggregate {
                    aggregates: fd
                        .aggregate
                        .aggregates
                        .iter()
                        .map(|a| crate::demand::CompiledAggExpr {
                            function: a.function,
                            arg: a.arg.as_ref().map(CompiledExpr::from_typed),
                            output_name: a.output_name.clone(),
                        })
                        .collect(),
                    group_by: fd
                        .aggregate
                        .group_by
                        .iter()
                        .map(|(e, n)| (CompiledExpr::from_typed(e), n.clone()))
                        .collect(),
                    output_schema: fd.aggregate.output_schema.clone(),
                    max_groups: DEFAULT_MAX_GROUPS,
                });

            let child = lower_physical(*input, now_ns);
            let match_all = mode == bqlite_ast::pattern::MatchMode::All;

            PhysicalPlan::SequenceMatch(Box::new(SequenceMatchPhysical {
                compiled_nfa,
                strategy,
                match_all,
                demand,
                execution_config,
                fused_aggregate,
                input: Box::new(child),
                output_schema,
            }))
        }

        LogicalPlan::Aggregate {
            aggregates,
            group_by,
            input,
            output_schema,
        } => {
            // Compile aggregate expressions: TypedAggExpr → CompiledAgg.
            // All Wave 3 aggregate functions take 0 or 1 argument (§2.2.1).
            let compiled_aggs: Vec<CompiledAgg> = aggregates
                .into_iter()
                .map(|agg| {
                    debug_assert!(
                        agg.args.len() <= 1,
                        "aggregate function {} has {} args, expected 0 or 1",
                        agg.output_name,
                        agg.args.len()
                    );
                    CompiledAgg {
                        function: agg.function,
                        arg: agg.args.first().map(CompiledExpr::from_typed),
                        output_name: agg.output_name,
                    }
                })
                .collect();

            // Compile group-by expressions.
            let compiled_group_by: Vec<(CompiledExpr, String)> = group_by
                .iter()
                .map(|(expr, name)| (CompiledExpr::from_typed(expr), name.clone()))
                .collect();

            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Aggregate(AggregatePhysical {
                aggregates: compiled_aggs,
                group_by: compiled_group_by,
                max_groups: DEFAULT_MAX_GROUPS,
                input: Box::new(child),
                output_schema,
            })
        }

        LogicalPlan::Sort {
            keys,
            input,
            output_schema,
        } => {
            // Compile sort-key expressions.
            let compiled_keys: Vec<(CompiledExpr, SortDirection)> = keys
                .iter()
                .map(|(expr, dir)| (CompiledExpr::from_typed(expr), *dir))
                .collect();

            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Sort(SortPhysical {
                keys: compiled_keys,
                max_rows: DEFAULT_SORT_MAX_ROWS,
                input: Box::new(child),
                output_schema,
            })
        }

        LogicalPlan::Distinct {
            input,
            output_schema,
        } => {
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Distinct(DistinctPhysical {
                max_groups: DEFAULT_MAX_GROUPS,
                input: Box::new(child),
                output_schema,
            })
        }

        // ── Wave 4 variants ──────────────────────────────────────────────
        LogicalPlan::Sessionize {
            gap,
            end_events,
            forwarded_columns,
            fused_downstream,
            input,
            output_schema,
        } => {
            let fused_aggregate = fused_downstream.map(compile_fused_downstream);
            let demand = DemandSet {
                columns: output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect(),
                forwarded: forwarded_columns.clone(),
                ..DemandSet::default()
            };
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Sessionize(SessionizePhysical {
                gap_ns: gap,
                end_events,
                demand,
                forwarded_columns,
                fused_aggregate,
                input: Box::new(child),
                output_schema,
                pre_fusion_output_schema: None,
            })
        }

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
            let compiled_predicate = predicate.as_ref().map(CompiledExpr::from_typed);
            let fused_aggregate = fused_downstream.map(compile_fused_downstream);
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::EventSelect(EventSelectPhysical {
                kind,
                event_types,
                predicate: compiled_predicate,
                lookback,
                forwarded_columns,
                fused_aggregate,
                input: Box::new(child),
                output_schema,
                pre_fusion_output_schema: None,
            })
        }

        LogicalPlan::Attribute {
            conversion_events,
            touchpoint_events,
            window,
            touchpoint_key,
            forwarded_conversion_columns,
            fused_downstream,
            conversion_range,
            input,
            output_schema,
        } => {
            let compiled_key = CompiledExpr::from_typed(&touchpoint_key);
            let fused_aggregate = fused_downstream.map(compile_fused_downstream);
            // If the logical layer captured a BETWEEN range, use it verbatim.
            // Otherwise, handle the LAST case that needs `now_ns` here.
            let final_conversion_range =
                conversion_range.or_else(|| resolve_last_range_from_scan(&input, now_ns));
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Attribute(AttributePhysical {
                conversion_events,
                touchpoint_events,
                window_ns: window,
                touchpoint_key: compiled_key,
                forwarded_conversion_columns,
                fused_aggregate,
                conversion_range: final_conversion_range,
                input: Box::new(child),
                output_schema,
                pre_fusion_output_schema: None,
            })
        }

        LogicalPlan::SubqueryFilter {
            columns,
            subquery,
            input,
            output_schema,
        } => {
            let compiled_cols: Vec<CompiledExpr> =
                columns.iter().map(CompiledExpr::from_typed).collect();
            let compiled_subquery = lower_physical(*subquery, now_ns);
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::SubqueryFilter(SubqueryFilterPhysical {
                lhs_columns: compiled_cols,
                subquery: Box::new(compiled_subquery),
                input: Box::new(child),
                output_schema,
            })
        }

        LogicalPlan::Sample {
            fraction,
            seed,
            input,
            output_schema,
        } => {
            let child = lower_physical(*input, now_ns);
            PhysicalPlan::Sample(SamplePhysical {
                fraction,
                seed: seed.unwrap_or(DEFAULT_SAMPLE_SEED),
                input: Box::new(child),
                output_schema,
            })
        }

        LogicalPlan::Delete {
            table,
            filter,
            allow_scan,
            output_schema,
        } => {
            let entity_key_col = table.entity_key_column().name.clone();
            let timestamp_col = table.timestamp_column().name.clone();
            let table_name = table.name().to_string();
            let physical_filter = match filter {
                crate::logical::DeleteFilter::Cheap(spec) => PhysicalDeleteFilter::Cheap(spec),
                crate::logical::DeleteFilter::AllowScan { predicate } => {
                    PhysicalDeleteFilter::AllowScan {
                        predicate: CompiledExpr::from_typed(&predicate),
                    }
                }
            };
            PhysicalPlan::Delete(DeletePhysical {
                table_name,
                entity_key_col,
                timestamp_col,
                filter: physical_filter,
                allow_scan,
                output_schema,
            })
        }
    }
}

/// Compile a logical `FusedDownstream` into a physical
/// `CompiledFusableAggregate`. Shared by Wave 4 stateful node lowering.
fn compile_fused_downstream(fd: crate::logical::FusedDownstream) -> CompiledFusableAggregate {
    CompiledFusableAggregate {
        aggregates: fd
            .aggregate
            .aggregates
            .iter()
            .map(|a| crate::demand::CompiledAggExpr {
                function: a.function,
                arg: a.arg.as_ref().map(CompiledExpr::from_typed),
                output_name: a.output_name.clone(),
            })
            .collect(),
        group_by: fd
            .aggregate
            .group_by
            .iter()
            .map(|(e, n)| (CompiledExpr::from_typed(e), n.clone()))
            .collect(),
        output_schema: fd.aggregate.output_schema.clone(),
        max_groups: DEFAULT_MAX_GROUPS,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bqlite_ast::expr::{Expr, Literal, Spanned};
    use bqlite_ast::operator::PipelineStage;
    use bqlite_ast::pipeline::{Pipeline, Source, TableRef};
    use bqlite_ast::span::{Name, Span};
    use bqlite_ast::{
        AlterAction, AlterTableStmt, ColumnDef as AstColumnDef, ColumnRole, CreateTableStmt,
        DescribeStmt, DropTableStmt, InsertBody, InsertStmt, SelectItem, SelectItemKind, Statement,
    };
    use bqlite_core::catalog::unknown_table_error;
    use bqlite_core::property::BqlType;
    use bqlite_core::schema::{ColumnDef, TableSchema};
    use bqlite_core::Catalog;

    use crate::compiled::CompiledNode;
    use crate::logical::{lower_statement, InsertLogicalBody};

    use super::*;

    // ── Helpers ─────────────────────────────────────────────────────

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
        fn resolve_table(&self, name: &str) -> bqlite_core::Result<TableSchema> {
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
                ColumnDef::nullable("amount", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .expect("events schema")
    }

    fn catalog_with_events() -> InMemoryCatalog {
        InMemoryCatalog::default().with(events_schema())
    }

    fn name(text: &str) -> Name {
        Name::synthetic(text)
    }

    fn table_ref(text: &str) -> TableRef {
        TableRef {
            name: name(text),
            span: Span::EMPTY,
        }
    }

    fn bare_pipeline(table: &str) -> Pipeline {
        Pipeline {
            source: Source {
                primary: table_ref(table),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        }
    }

    fn col_expr(col: &str) -> Spanned<Expr> {
        Spanned::new(Expr::Column(name(col)), Span::EMPTY)
    }

    fn int_literal(value: i64) -> Spanned<Expr> {
        Spanned::new(Expr::Literal(Literal::Int(value)), Span::EMPTY)
    }

    /// Lower a `Statement` straight to `PhysicalPlan` for a test.
    /// Mirrors the compiler pipeline the `plan()` entry point uses.
    fn plan_physical(stmt: Statement, catalog: &dyn Catalog) -> PhysicalPlan {
        let logical = lower_statement(stmt, catalog).expect("logical lowering must succeed");
        lower_physical(logical, 0)
    }

    // ── ScanPhysical ────────────────────────────────────────────────

    #[test]
    fn lower_bare_query_produces_scan_with_empty_optimizer_fields() {
        let catalog = catalog_with_events();
        let stmt = Statement::Query(bare_pipeline("events"));
        let physical = plan_physical(stmt, &catalog);

        match physical {
            PhysicalPlan::Scan(scan) => {
                assert_eq!(scan.table, "events");
                assert!(scan.query_range.is_none());
                assert!(scan.reader_range.is_none());
                assert!(
                    scan.scan_predicates.is_empty(),
                    "lowering produces an empty predicate list; TASK-227 \
                     populates it in a separate pass"
                );
                assert!(
                    scan.projected_columns.is_empty(),
                    "empty means `decode all columns`; TASK-228 populates it"
                );
                // `OperatorSchema::from_table(&events_schema())` —
                // declared columns plus system `__seq_id` / `__batch_id`.
                let names: Vec<&str> = scan
                    .output_schema
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                assert_eq!(
                    names,
                    vec![
                        "entity_id",
                        "ts",
                        "event_type",
                        "amount",
                        "__seq_id",
                        "__batch_id"
                    ]
                );
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    // ── FilterPhysical ─────────────────────────────────────────────

    #[test]
    fn lower_pipeline_with_where_produces_filter_over_scan() {
        let catalog = catalog_with_events();
        // `events | where amount > 10`
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Where {
            predicate: Spanned::new(
                Expr::Compare {
                    op: bqlite_ast::expr::CompareOp::Greater,
                    left: Box::new(col_expr("amount")),
                    right: Box::new(int_literal(10)),
                },
                Span::EMPTY,
            ),
            span: Span::EMPTY,
        });
        let physical = plan_physical(Statement::Query(pipeline), &catalog);

        let PhysicalPlan::Filter(filter) = physical else {
            panic!("expected Filter, got {physical:?}");
        };
        // Tile size defaults to DEFAULT_FILTER_TILE_SIZE.
        assert_eq!(filter.tile_size, DEFAULT_FILTER_TILE_SIZE);
        // Predicate must be a Bool-returning compare node.
        assert_eq!(filter.predicate.result_type, BqlType::Bool);
        assert!(matches!(
            filter.predicate.node,
            CompiledNode::Compare { .. }
        ));
        // Filter's output schema mirrors the scan's.
        assert_eq!(filter.output_schema, *filter.input.output_schema());
        // Child must be a Scan.
        assert!(matches!(*filter.input, PhysicalPlan::Scan(_)));
    }

    #[test]
    fn filter_tile_size_clamped_into_legal_window() {
        // Build a trivial filter directly to exercise `FilterPhysical::new`.
        let scan = PhysicalPlan::Scan(ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema: OperatorSchema::from_table(&events_schema()),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        });
        let pred = CompiledExpr {
            node: CompiledNode::Literal(bqlite_core::PropertyValue::Bool(true)),
            result_type: BqlType::Bool,
            nullable: false,
        };

        let too_small = FilterPhysical::new(pred.clone(), scan.clone(), 16);
        assert_eq!(too_small.tile_size, MIN_FILTER_TILE_SIZE);

        let too_big = FilterPhysical::new(pred.clone(), scan.clone(), 1_000_000);
        assert_eq!(too_big.tile_size, MAX_FILTER_TILE_SIZE);

        let just_right = FilterPhysical::new(pred, scan, 2_500);
        assert_eq!(just_right.tile_size, 2_500);
    }

    // ── ProjectPhysical ────────────────────────────────────────────

    #[test]
    fn lower_pipeline_with_select_produces_project_over_scan() {
        let catalog = catalog_with_events();
        // `events | select amount, entity_id`
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Select {
            distinct: false,
            items: vec![
                SelectItem {
                    kind: SelectItemKind::Expr(col_expr("amount")),
                    alias: None,
                    span: Span::EMPTY,
                },
                SelectItem {
                    kind: SelectItemKind::Expr(col_expr("entity_id")),
                    alias: None,
                    span: Span::EMPTY,
                },
            ],
            span: Span::EMPTY,
        });
        let physical = plan_physical(Statement::Query(pipeline), &catalog);

        let PhysicalPlan::Project(proj) = physical else {
            panic!("expected Project, got {physical:?}");
        };
        assert_eq!(proj.expressions.len(), 2);
        assert_eq!(proj.expressions[0].output_name, "amount");
        assert_eq!(proj.expressions[1].output_name, "entity_id");
        let out_names: Vec<&str> = proj
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(out_names, vec!["amount", "entity_id"]);
        assert!(matches!(*proj.input, PhysicalPlan::Scan(_)));
    }

    // ── LimitPhysical ──────────────────────────────────────────────

    #[test]
    fn lower_pipeline_with_limit_produces_limit_over_scan() {
        let catalog = catalog_with_events();
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Limit {
            count: 10,
            span: Span::EMPTY,
        });
        let physical = plan_physical(Statement::Query(pipeline), &catalog);

        let PhysicalPlan::Limit(limit) = physical else {
            panic!("expected Limit, got {physical:?}");
        };
        assert_eq!(limit.count, 10);
        assert!(matches!(*limit.input, PhysicalPlan::Scan(_)));
        assert_eq!(limit.output_schema, *limit.input.output_schema());
    }

    // ── DDL variants ───────────────────────────────────────────────

    #[test]
    fn lower_create_table_produces_create_table_physical() {
        let catalog = InMemoryCatalog::default();
        let stmt = Statement::CreateTable(CreateTableStmt {
            table: name("orders"),
            columns: vec![
                AstColumnDef {
                    name: name("entity_id"),
                    data_type: BqlType::String,
                    role: ColumnRole::EntityKey,
                    not_null: true,
                    default: None,
                    span: Span::EMPTY,
                },
                AstColumnDef {
                    name: name("ts"),
                    data_type: BqlType::Timestamp,
                    role: ColumnRole::EventTime,
                    not_null: true,
                    default: None,
                    span: Span::EMPTY,
                },
                AstColumnDef {
                    name: name("event_type"),
                    data_type: BqlType::String,
                    role: ColumnRole::EventType,
                    not_null: true,
                    default: None,
                    span: Span::EMPTY,
                },
                AstColumnDef {
                    name: name("total"),
                    data_type: BqlType::Float,
                    role: ColumnRole::Regular,
                    not_null: false,
                    default: None,
                    span: Span::EMPTY,
                },
            ],
            span: Span::EMPTY,
        });
        let physical = plan_physical(stmt, &catalog);

        let PhysicalPlan::CreateTable(create) = physical else {
            panic!("expected CreateTable, got {physical:?}");
        };
        assert_eq!(create.name, "orders");
        assert_eq!(create.entity_key, "entity_id");
        assert_eq!(create.event_time, "ts");
        assert_eq!(create.event_type, "event_type");
        assert_eq!(create.columns.len(), 4);
        assert!(create.output_schema.columns().is_empty());
    }

    #[test]
    fn lower_drop_table_produces_drop_table_physical() {
        let catalog = catalog_with_events();
        let stmt = Statement::DropTable(DropTableStmt {
            table: name("events"),
            span: Span::EMPTY,
        });
        let physical = plan_physical(stmt, &catalog);

        let PhysicalPlan::DropTable(drop) = physical else {
            panic!("expected DropTable, got {physical:?}");
        };
        assert_eq!(drop.name, "events");
        assert!(drop.output_schema.columns().is_empty());
    }

    #[test]
    fn lower_alter_table_add_column_produces_alter_physical() {
        let catalog = catalog_with_events();
        let stmt = Statement::AlterTable(AlterTableStmt {
            table: name("events"),
            action: AlterAction::AddColumn(AstColumnDef {
                name: name("region"),
                data_type: BqlType::String,
                role: ColumnRole::Regular,
                not_null: false,
                default: None,
                span: Span::EMPTY,
            }),
            span: Span::EMPTY,
        });
        let physical = plan_physical(stmt, &catalog);

        let PhysicalPlan::AlterTableAddColumn(alter) = physical else {
            panic!("expected AlterTableAddColumn, got {physical:?}");
        };
        assert_eq!(alter.name, "events");
        assert_eq!(alter.column.name, "region");
        assert_eq!(alter.column.bql_type, BqlType::String);
        assert!(alter.column.nullable);
    }

    #[test]
    fn lower_describe_produces_describe_physical_with_fixed_schema() {
        let catalog = catalog_with_events();
        let stmt = Statement::Describe(DescribeStmt {
            table: name("events"),
            span: Span::EMPTY,
        });
        let physical = plan_physical(stmt, &catalog);

        let PhysicalPlan::Describe(desc) = physical else {
            panic!("expected Describe, got {physical:?}");
        };
        assert_eq!(desc.name, "events");
        let cols: Vec<&str> = desc
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, vec!["name", "type", "nullable", "role"]);
    }

    // ── InsertPhysical ─────────────────────────────────────────────

    #[test]
    fn lower_insert_values_carries_coerced_rows() {
        let catalog = catalog_with_events();
        let stmt = Statement::Insert(InsertStmt {
            table: name("events"),
            body: InsertBody::Values(vec![vec![
                Literal::String("user-1".into()),
                Literal::Timestamp(1_700_000_000_000_000_000),
                Literal::String("signup".into()),
                Literal::Int(42),
            ]]),
            span: Span::EMPTY,
        });
        let physical = plan_physical(stmt, &catalog);

        let PhysicalPlan::Insert(insert) = physical else {
            panic!("expected Insert, got {physical:?}");
        };
        assert_eq!(insert.table.name(), "events");
        match insert.body {
            InsertLogicalBody::Values(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 4);
            }
            InsertLogicalBody::From(_) => {
                panic!("expected Values body")
            }
        }
        assert!(insert.output_schema.columns().is_empty());
    }

    // ── ExplainPhysical ────────────────────────────────────────────

    #[test]
    fn lower_explain_wraps_a_full_child_plan() {
        let catalog = catalog_with_events();
        let stmt = Statement::Explain(bare_pipeline("events"));
        let physical = plan_physical(stmt, &catalog);

        let PhysicalPlan::Explain(explain) = physical else {
            panic!("expected Explain, got {physical:?}");
        };
        // Child must be a fully lowered Scan — not held as raw AST.
        assert!(matches!(*explain.plan, PhysicalPlan::Scan(_)));
        // Output schema is the fixed single-column `(plan: String)`.
        let cols: Vec<&str> = explain
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(cols, vec!["plan"]);
    }

    // ── Multi-stage composition ────────────────────────────────────

    #[test]
    fn lower_where_select_limit_pipeline_nests_in_correct_order() {
        // `events | where amount > 0 | select amount | limit 5`
        // must lower to `Limit(Project(Filter(Scan)))` so that the
        // engine bind step builds operators in the same order
        // logical lowering folded them.
        let catalog = catalog_with_events();
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Where {
            predicate: Spanned::new(
                Expr::Compare {
                    op: bqlite_ast::expr::CompareOp::Greater,
                    left: Box::new(col_expr("amount")),
                    right: Box::new(int_literal(0)),
                },
                Span::EMPTY,
            ),
            span: Span::EMPTY,
        });
        pipeline.stages.push(PipelineStage::Select {
            distinct: false,
            items: vec![SelectItem {
                kind: SelectItemKind::Expr(col_expr("amount")),
                alias: None,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        });
        pipeline.stages.push(PipelineStage::Limit {
            count: 5,
            span: Span::EMPTY,
        });

        let physical = plan_physical(Statement::Query(pipeline), &catalog);

        let PhysicalPlan::Limit(limit) = physical else {
            panic!("expected Limit at root, got {physical:?}");
        };
        assert_eq!(limit.count, 5);
        let PhysicalPlan::Project(proj) = *limit.input else {
            panic!("expected Project under Limit");
        };
        assert_eq!(proj.expressions.len(), 1);
        assert_eq!(proj.expressions[0].output_name, "amount");
        let PhysicalPlan::Filter(filter) = *proj.input else {
            panic!("expected Filter under Project");
        };
        assert_eq!(filter.predicate.result_type, BqlType::Bool);
        assert!(matches!(*filter.input, PhysicalPlan::Scan(_)));
    }

    // ── output_schema invariant ────────────────────────────────────

    #[test]
    fn physical_output_schema_matches_logical_output_schema() {
        // Lowering is one-to-one so the root's output schema must be
        // bit-identical on both sides.
        let catalog = catalog_with_events();
        let stmt = Statement::Query(bare_pipeline("events"));
        let logical = lower_statement(stmt, &catalog).expect("logical");
        let expected = logical.output_schema().clone();
        let physical = lower_physical(logical, 0);
        assert_eq!(physical.output_schema(), &expected);
    }

    // ── Wave 3: AggregatePhysical ─────────────────────────────────

    #[test]
    fn lower_stats_count_star_produces_aggregate_physical() {
        use bqlite_ast::AggItem;
        let catalog = catalog_with_events();
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Stats {
            aggregates: vec![AggItem {
                function: name("count"),
                args: vec![],
                distinct: false,
                alias: name("total"),
                span: Span::EMPTY,
            }],
            group_by: vec![],
            span: Span::EMPTY,
        });
        let stmt = Statement::Query(pipeline);
        let physical = crate::plan(stmt, &catalog, 0).expect("plan");

        let PhysicalPlan::Aggregate(agg) = physical else {
            panic!("expected Aggregate, got {physical:?}");
        };
        assert_eq!(agg.aggregates.len(), 1);
        assert_eq!(agg.aggregates[0].function, bqlite_core::AggFunction::Count);
        assert!(agg.aggregates[0].arg.is_none());
        assert_eq!(agg.aggregates[0].output_name, "total");
        assert_eq!(agg.max_groups, DEFAULT_MAX_GROUPS);
        assert!(agg.group_by.is_empty());
        assert_eq!(agg.output_schema.columns().len(), 1);
        assert_eq!(agg.output_schema.columns()[0].name, "total");
    }

    #[test]
    fn lower_stats_with_group_by_produces_aggregate_physical() {
        use bqlite_ast::{AggItem, GroupItem};
        let catalog = catalog_with_events();
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Stats {
            aggregates: vec![AggItem {
                function: name("sum"),
                args: vec![col_expr("amount")],
                distinct: false,
                alias: name("total_amount"),
                span: Span::EMPTY,
            }],
            group_by: vec![GroupItem {
                expr: col_expr("event_type"),
                alias: None,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        });
        let stmt = Statement::Query(pipeline);
        let physical = crate::plan(stmt, &catalog, 0).expect("plan");

        let PhysicalPlan::Aggregate(agg) = physical else {
            panic!("expected Aggregate, got {physical:?}");
        };
        assert_eq!(agg.aggregates.len(), 1);
        assert_eq!(agg.aggregates[0].function, bqlite_core::AggFunction::Sum);
        assert!(agg.aggregates[0].arg.is_some());
        assert_eq!(agg.group_by.len(), 1);
        assert_eq!(agg.group_by[0].1, "event_type");
        // Output: [event_type, total_amount]
        let col_names: Vec<&str> = agg
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(col_names, vec!["event_type", "total_amount"]);
    }

    // ── Wave 3: SortPhysical ──────────────────────────────────────

    #[test]
    fn lower_order_by_produces_sort_physical() {
        use bqlite_ast::expr::{OrderItem, SortDir};
        let catalog = catalog_with_events();
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::OrderBy {
            items: vec![OrderItem {
                expr: col_expr("amount"),
                direction: SortDir::Desc,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        });
        let stmt = Statement::Query(pipeline);
        let physical = crate::plan(stmt, &catalog, 0).expect("plan");

        let PhysicalPlan::Sort(sort) = physical else {
            panic!("expected Sort, got {physical:?}");
        };
        assert_eq!(sort.keys.len(), 1);
        assert_eq!(sort.keys[0].1, crate::logical::SortDirection::Desc);
        assert_eq!(sort.max_rows, DEFAULT_SORT_MAX_ROWS);
        // Sort output schema == input schema
        assert_eq!(sort.output_schema, *sort.input.output_schema());
    }

    // ── Wave 3: DistinctPhysical ──────────────────────────────────

    #[test]
    fn lower_select_distinct_produces_distinct_project_physical() {
        let catalog = catalog_with_events();
        let mut pipeline = bare_pipeline("events");
        pipeline.stages.push(PipelineStage::Select {
            distinct: true,
            items: vec![SelectItem {
                kind: SelectItemKind::Expr(col_expr("entity_id")),
                alias: None,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        });
        let stmt = Statement::Query(pipeline);
        let physical = crate::plan(stmt, &catalog, 0).expect("plan");

        let PhysicalPlan::Distinct(distinct) = physical else {
            panic!("expected Distinct, got {physical:?}");
        };
        assert_eq!(distinct.max_groups, DEFAULT_MAX_GROUPS);
        assert!(matches!(*distinct.input, PhysicalPlan::Project(_)));
        assert_eq!(distinct.output_schema.columns().len(), 1);
        assert_eq!(distinct.output_schema.columns()[0].name, "entity_id");
    }

    // ── Wave 4 variant tests ──────────────────────────────────────────

    use crate::demand::ColumnId;
    use crate::logical::EventSelectKind;

    fn sessionize_output_schema() -> bqlite_core::OperatorSchema {
        bqlite_core::OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::required("session_id", BqlType::Int),
            ColumnDef::required("session_duration", BqlType::Int),
        ])
        .unwrap()
    }

    fn event_select_output_schema() -> bqlite_core::OperatorSchema {
        bqlite_core::OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    fn attribute_output_schema() -> bqlite_core::OperatorSchema {
        bqlite_core::OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_key", BqlType::String),
        ])
        .unwrap()
    }

    #[test]
    fn sessionize_logical_output_schema() {
        let scan = LogicalPlan::scan(events_schema());
        let os = sessionize_output_schema();
        let node = LogicalPlan::Sessionize {
            gap: 1_800_000_000_000, // 30 min in ns
            end_events: vec!["logout".into()],
            forwarded_columns: vec![],
            fused_downstream: None,
            input: Box::new(scan),
            output_schema: os.clone(),
        };
        assert_eq!(node.output_schema(), &os);
        assert_eq!(node.output_schema().columns().len(), 5);
    }

    #[test]
    fn sessionize_lowering_produces_physical() {
        let scan = LogicalPlan::scan(events_schema());
        let os = sessionize_output_schema();
        let node = LogicalPlan::Sessionize {
            gap: 1_800_000_000_000,
            end_events: vec!["logout".into()],
            forwarded_columns: vec!["amount".into()],
            fused_downstream: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::Sessionize(sess) = physical else {
            panic!("expected Sessionize, got {physical:?}");
        };
        assert_eq!(sess.gap_ns, 1_800_000_000_000);
        assert_eq!(sess.end_events, vec!["logout".to_string()]);
        assert_eq!(
            sess.forwarded_columns,
            vec!["amount".to_string()] as Vec<ColumnId>
        );
        assert!(sess.fused_aggregate.is_none());
        assert!(matches!(*sess.input, PhysicalPlan::Scan(_)));
    }

    #[test]
    fn event_select_first_lowering() {
        let scan = LogicalPlan::scan(events_schema());
        let os = event_select_output_schema();
        let node = LogicalPlan::EventSelect {
            kind: EventSelectKind::First,
            event_types: vec!["purchase".into()],
            predicate: None,
            lookback: None,
            forwarded_columns: vec![],
            fused_downstream: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::EventSelect(es) = physical else {
            panic!("expected EventSelect, got {physical:?}");
        };
        assert_eq!(es.kind, EventSelectKind::First);
        assert_eq!(es.event_types, vec!["purchase".to_string()]);
        assert!(es.predicate.is_none());
        assert!(es.lookback.is_none());
    }

    #[test]
    fn event_select_nth_lowering() {
        let scan = LogicalPlan::scan(events_schema());
        let os = event_select_output_schema();
        let node = LogicalPlan::EventSelect {
            kind: EventSelectKind::Nth(3),
            event_types: vec!["click".into(), "tap".into()],
            predicate: None,
            lookback: Some(86_400_000_000_000), // 1 day
            forwarded_columns: vec![],
            fused_downstream: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::EventSelect(es) = physical else {
            panic!("expected EventSelect, got {physical:?}");
        };
        assert_eq!(es.kind, EventSelectKind::Nth(3));
        assert_eq!(es.event_types.len(), 2);
        assert_eq!(es.lookback, Some(86_400_000_000_000));
    }

    #[test]
    fn event_select_last_lowering() {
        let scan = LogicalPlan::scan(events_schema());
        let os = event_select_output_schema();
        let node = LogicalPlan::EventSelect {
            kind: EventSelectKind::Last,
            event_types: vec!["purchase".into()],
            predicate: None,
            lookback: None,
            forwarded_columns: vec![],
            fused_downstream: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::EventSelect(es) = physical else {
            panic!("expected EventSelect, got {physical:?}");
        };
        assert_eq!(es.kind, EventSelectKind::Last);
        assert!(es.lookback.is_none());
    }

    #[test]
    fn attribute_lowering() {
        let scan = LogicalPlan::scan(events_schema());
        let os = attribute_output_schema();
        // Build a simple typed expression for touchpoint_key.
        let key_expr = crate::expr::TypedExpr {
            kind: crate::expr::TypedExprKind::Column {
                column_index: 2,
                name: "event_type".into(),
            },
            result_type: BqlType::String,
            nullable: false,
            span: Span::EMPTY,
        };
        let node = LogicalPlan::Attribute {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["ad_click".into(), "email_open".into()],
            window: 2_592_000_000_000_000, // 30 days in ns
            touchpoint_key: key_expr,
            forwarded_conversion_columns: vec![],
            fused_downstream: None,
            conversion_range: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::Attribute(attr) = physical else {
            panic!("expected Attribute, got {physical:?}");
        };
        assert_eq!(attr.conversion_events, vec!["purchase".to_string()]);
        assert_eq!(attr.touchpoint_events.len(), 2);
        assert_eq!(attr.window_ns, 2_592_000_000_000_000);
        assert!(attr.fused_aggregate.is_none());
        assert!(attr.conversion_range.is_none());
    }

    #[test]
    fn attribute_conversion_range_last_resolved_at_physical() {
        // When the primary scan uses `LAST <dur>`, the logical layer stores
        // `conversion_range: None` and the physical layer resolves it using
        // `now_ns`. This test verifies the fallback path.
        let ns_30d: i64 = 30 * 86_400 * 1_000_000_000;
        let scan = LogicalPlan::scan_with_time_range(
            events_schema(),
            Some(bqlite_ast::pipeline::TimeRange::Last(ns_30d)),
        );
        let os = attribute_output_schema();
        let key_expr = crate::expr::TypedExpr {
            kind: crate::expr::TypedExprKind::Column {
                column_index: 2,
                name: "event_type".into(),
            },
            result_type: BqlType::String,
            nullable: false,
            span: Span::EMPTY,
        };
        let node = LogicalPlan::Attribute {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["ad_click".into()],
            window: ns_30d,
            touchpoint_key: key_expr,
            forwarded_conversion_columns: vec![],
            fused_downstream: None,
            conversion_range: None,
            input: Box::new(scan),
            output_schema: os,
        };
        // Pick a fixed now_ns so the test is deterministic.
        let now_ns: i64 = 1_700_000_000_000_000_000;
        let physical = lower_physical(node, now_ns);
        let PhysicalPlan::Attribute(attr) = physical else {
            panic!("expected Attribute");
        };
        let (start, end) = attr.conversion_range.expect("resolved from LAST");
        assert_eq!(end, now_ns);
        assert_eq!(start, now_ns - ns_30d);
    }

    #[test]
    fn subquery_filter_lowering() {
        let scan = LogicalPlan::scan(events_schema());
        let subquery = LogicalPlan::scan(events_schema());
        let input_schema = scan.output_schema().clone();
        // Build a column expression for the LHS of the IN check.
        let col_expr = crate::expr::TypedExpr {
            kind: crate::expr::TypedExprKind::Column {
                column_index: 0,
                name: "entity_id".into(),
            },
            result_type: BqlType::String,
            nullable: false,
            span: Span::EMPTY,
        };
        let node = LogicalPlan::SubqueryFilter {
            columns: vec![col_expr],
            subquery: Box::new(subquery),
            input: Box::new(scan),
            output_schema: input_schema,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::SubqueryFilter(sqf) = physical else {
            panic!("expected SubqueryFilter, got {physical:?}");
        };
        assert_eq!(sqf.lhs_columns.len(), 1);
        // SubqueryFilter's output schema is identical to its input's.
        assert!(matches!(*sqf.input, PhysicalPlan::Scan(_)));
        assert!(matches!(*sqf.subquery, PhysicalPlan::Scan(_)));
    }

    #[test]
    fn sample_lowering_with_explicit_seed() {
        let scan = LogicalPlan::scan(events_schema());
        let os = scan.output_schema().clone();
        let node = LogicalPlan::Sample {
            fraction: 0.1,
            seed: Some(42),
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::Sample(sample) = physical else {
            panic!("expected Sample, got {physical:?}");
        };
        assert!((sample.fraction - 0.1).abs() < f64::EPSILON);
        assert_eq!(sample.seed, 42);
    }

    #[test]
    fn sample_lowering_default_seed() {
        let scan = LogicalPlan::scan(events_schema());
        let os = scan.output_schema().clone();
        let node = LogicalPlan::Sample {
            fraction: 0.5,
            seed: None,
            input: Box::new(scan),
            output_schema: os,
        };
        let physical = lower_physical(node, 0);
        let PhysicalPlan::Sample(sample) = physical else {
            panic!("expected Sample, got {physical:?}");
        };
        assert!((sample.fraction - 0.5).abs() < f64::EPSILON);
        assert_eq!(sample.seed, DEFAULT_SAMPLE_SEED);
    }

    // ── Wave 4 CP5b: joined Scan → MergeSources lowering ─────────────────

    fn logins_schema_for_physical() -> TableSchema {
        TableSchema::new(
            "logins",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("device", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .expect("logins schema")
    }

    #[test]
    fn single_table_scan_unchanged_post_cp5b() {
        // Regression: single-table Scan continues to lower to a plain
        // PhysicalPlan::Scan with the same shape as before.
        let scan = LogicalPlan::scan(events_schema());
        let physical = lower_physical(scan, 0);
        assert!(matches!(physical, PhysicalPlan::Scan(_)));
    }

    #[test]
    fn joined_scan_lowers_to_merge_sources() {
        // Construct a joined logical Scan directly (matching what
        // `lower_query_pipeline` produces in CP5a) and verify the physical
        // lowering produces a MergeSources with the canonical shape.
        let primary = events_schema();
        let joined = logins_schema_for_physical();
        // Build the combined output schema the way build_joined_scan does.
        // Cross-table columns are marked nullable because merge-output
        // rows from sub-scan `i` carry NULL for every other table's
        // columns (cohorts-aliases-joins.md §3.8; TASK-436).
        let mut cols: Vec<ColumnDef> = Vec::new();
        for t in [&primary, &joined] {
            for c in t.columns() {
                if c.is_system() {
                    continue;
                }
                cols.push(ColumnDef {
                    name: format!("{}.{}", t.name(), c.name),
                    bql_type: c.bql_type.clone(),
                    nullable: true,
                    default_value: None,
                });
            }
        }
        cols.push(ColumnDef::required(
            crate::logical::SOURCE_TABLE_ID_COLUMN,
            BqlType::Int,
        ));
        cols.push(ColumnDef::required(
            bqlite_core::schema::SEQ_ID_COLUMN,
            BqlType::Int,
        ));
        cols.push(ColumnDef::required(
            bqlite_core::schema::BATCH_ID_COLUMN,
            BqlType::Int,
        ));
        let output_schema = OperatorSchema::new(cols).unwrap();

        let logical = LogicalPlan::Scan {
            table: primary.clone(),
            time_range: None,
            reader_backward_ns: 0,
            reader_forward_ns: 0,
            joined_tables: vec![joined.clone()],
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: output_schema.clone(),
        };
        let physical = lower_physical(logical, 0);
        let PhysicalPlan::MergeSources(ms) = physical else {
            panic!("expected MergeSources, got {physical:?}");
        };
        assert_eq!(ms.tables.len(), 2);
        assert_eq!(ms.tables[0].table, "events");
        assert_eq!(ms.tables[1].table, "logins");
        // Sub-scan output schemas are per-table (bare names), not the
        // combined dotted schema.
        let t0_names: Vec<&str> = ms.tables[0]
            .output_schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(t0_names.contains(&"entity_id") && !t0_names.contains(&"events.entity_id"));
        // table_id_map lists catalog names in JOIN order.
        assert_eq!(ms.table_id_map, vec!["events", "logins"]);
        // Canonical merge key shape.
        let expected_order = vec![
            ("entity_id".to_string(), SortDirection::Asc),
            ("ts".to_string(), SortDirection::Asc),
            ("__table_order".to_string(), SortDirection::Asc),
            (
                bqlite_core::schema::SEQ_ID_COLUMN.to_string(),
                SortDirection::Asc,
            ),
        ];
        assert_eq!(ms.order, expected_order);
        // Combined output schema passes through.
        assert_eq!(ms.output_schema, output_schema);
    }

    #[test]
    fn joined_scan_three_tables_preserves_table_id_map_order() {
        // A 3-table JOIN (primary + 2 joined) locks in that table_id_map
        // indexing matches the chain order of `iter::once(primary).chain(joined)`.
        let primary = events_schema();
        let joined1 = logins_schema_for_physical();
        let joined2 = TableSchema::new(
            "clicks",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("url", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .expect("clicks schema");
        let output_schema = OperatorSchema::from_table(&primary);
        let logical = LogicalPlan::Scan {
            table: primary.clone(),
            time_range: None,
            reader_backward_ns: 0,
            reader_forward_ns: 0,
            joined_tables: vec![joined1.clone(), joined2.clone()],
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema,
        };
        let PhysicalPlan::MergeSources(ms) = lower_physical(logical, 0) else {
            panic!("expected MergeSources");
        };
        assert_eq!(ms.tables.len(), 3);
        assert_eq!(ms.table_id_map, vec!["events", "logins", "clicks"]);
        assert_eq!(ms.tables[0].table, "events");
        assert_eq!(ms.tables[1].table, "logins");
        assert_eq!(ms.tables[2].table, "clicks");
    }

    #[test]
    fn joined_scan_carries_table_local_entity_key_and_timestamp_cols() {
        // When joined tables declare differently-named entity-key / timestamp
        // columns (cohorts-aliases-joins.md line 366 allows type-compat but
        // not name-compat), each sub-scan's `entity_key_col` / `timestamp_col`
        // must be the table-local name while the canonical `order` vec stays
        // fixed at `entity_id` / `ts` (post-merge canonical names).
        let primary = TableSchema::new(
            "purchases",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("event_ts", BqlType::Timestamp),
                ColumnDef::required("event", BqlType::String),
            ],
            "user_id",
            "event_ts",
            "event",
        )
        .expect("purchases schema");
        let joined = TableSchema::new(
            "logins",
            vec![
                ColumnDef::required("uid", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("kind", BqlType::String),
            ],
            "uid",
            "ts",
            "kind",
        )
        .expect("logins schema");
        let output_schema = OperatorSchema::from_table(&primary);
        let logical = LogicalPlan::Scan {
            table: primary.clone(),
            time_range: None,
            reader_backward_ns: 0,
            reader_forward_ns: 0,
            joined_tables: vec![joined.clone()],
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema,
        };
        let PhysicalPlan::MergeSources(ms) = lower_physical(logical, 0) else {
            panic!("expected MergeSources");
        };
        // Per-sub-scan entity_key_col / timestamp_col are table-local.
        assert_eq!(ms.tables[0].entity_key_col, "user_id");
        assert_eq!(ms.tables[0].timestamp_col, "event_ts");
        assert_eq!(ms.tables[1].entity_key_col, "uid");
        assert_eq!(ms.tables[1].timestamp_col, "ts");
        // The `order` vec uses the post-merge canonical names `entity_id`
        // and `ts` regardless of table-local names.
        assert_eq!(ms.order[0].0, "entity_id");
        assert_eq!(ms.order[1].0, "ts");
    }

    #[test]
    fn joined_scan_replicates_reader_range_across_sub_scans() {
        use bqlite_ast::pipeline::TimeRange as AstTr;
        let primary = events_schema();
        let joined = logins_schema_for_physical();
        let ns_1d: i64 = 86_400 * 1_000_000_000;
        let ns_7d: i64 = 7 * ns_1d;
        let now_ns: i64 = 1_700_000_000_000_000_000;
        let output_schema = OperatorSchema::from_table(&primary);
        let logical = LogicalPlan::Scan {
            table: primary.clone(),
            time_range: Some(AstTr::Last(ns_1d)),
            reader_backward_ns: ns_7d,
            reader_forward_ns: 0,
            joined_tables: vec![joined.clone()],
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema,
        };
        let physical = lower_physical(logical, now_ns);
        let PhysicalPlan::MergeSources(ms) = physical else {
            panic!("expected MergeSources");
        };
        // Every sub-scan shares the same query_range / reader_range.
        let r0_query = ms.tables[0].query_range;
        let r1_query = ms.tables[1].query_range;
        let r0_reader = ms.tables[0].reader_range;
        let r1_reader = ms.tables[1].reader_range;
        assert_eq!(r0_query, r1_query);
        assert_eq!(r0_reader, r1_reader);
        // Reader range widened backward by 7 days from the query range.
        let qr = r0_query.expect("query range is Some");
        let rr = r0_reader.expect("reader range is Some");
        assert_eq!(rr.end, qr.end);
        assert_eq!(rr.start.as_nanos(), qr.start.as_nanos() - ns_7d);
    }

    #[test]
    fn merge_sources_physical_construction() {
        let schema = events_schema();
        let scan1 = ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: bqlite_core::OperatorSchema::from_table(&schema),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        };
        let scan2 = ScanPhysical {
            table: "clicks".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: bqlite_core::OperatorSchema::from_table(&schema),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        };
        // Build a merged output schema with __source_table_id.
        let merged_schema = bqlite_core::OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ])
        .unwrap();
        let merge = MergeSourcesPhysical {
            tables: vec![scan1, scan2],
            order: vec![
                ("entity_id".into(), SortDirection::Asc),
                ("ts".into(), SortDirection::Asc),
                ("__source_table_id".into(), SortDirection::Asc),
            ],
            table_id_map: vec!["events".into(), "clicks".into()],
            output_schema: merged_schema.clone(),
        };
        let plan = PhysicalPlan::MergeSources(merge);
        assert_eq!(plan.output_schema(), &merged_schema);
        // Verify the __source_table_id column is present.
        assert!(plan.output_schema().column("__source_table_id").is_some());
    }

    #[test]
    fn wave4_physical_output_schemas_match_input() {
        // Sample and SubqueryFilter preserve input schema.
        let scan = LogicalPlan::scan(events_schema());
        let scan_schema = scan.output_schema().clone();

        let sample = LogicalPlan::Sample {
            fraction: 0.2,
            seed: None,
            input: Box::new(scan.clone()),
            output_schema: scan_schema.clone(),
        };
        assert_eq!(sample.output_schema(), &scan_schema);

        let subquery = LogicalPlan::scan(events_schema());
        let col_expr = crate::expr::TypedExpr {
            kind: crate::expr::TypedExprKind::Column {
                column_index: 0,
                name: "entity_id".into(),
            },
            result_type: BqlType::String,
            nullable: false,
            span: Span::EMPTY,
        };
        let sqf = LogicalPlan::SubqueryFilter {
            columns: vec![col_expr],
            subquery: Box::new(subquery),
            input: Box::new(scan),
            output_schema: scan_schema.clone(),
        };
        assert_eq!(sqf.output_schema(), &scan_schema);
    }
}
