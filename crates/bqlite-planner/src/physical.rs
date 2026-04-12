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

use bqlite_ast::pipeline::TimeRange;
use bqlite_core::{AggFunction, ColumnDef, OperatorSchema, TableSchema};

use crate::compile::{CompiledNfa, MatchExecutionConfig, MatchStrategy};
use crate::compiled::CompiledExpr;
use crate::demand::CompiledFusableAggregate;
use crate::logical::{InsertLogicalBody, LogicalPlan, ProjectItem, SortDirection};

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
    /// Optional `LAST <dur>` / `BETWEEN <ts> AND <ts>` range from the
    /// logical source. `None` in Wave 2 — the parser does not yet
    /// emit the syntax — but the field exists so TASK-230 / TASK-243
    /// (later waves) do not have to retrofit it.
    pub time_range: Option<TimeRange>,
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
    /// Returns `BqliteError::Execution` if exceeded — no spill in Wave 3.
    pub max_groups: usize,
    /// Child plan feeding this distinct operator.
    pub input: Box<PhysicalPlan>,
    /// Identical to `input.output_schema()` — Distinct never changes the schema.
    pub output_schema: OperatorSchema,
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
pub fn lower_physical(plan: LogicalPlan) -> PhysicalPlan {
    match plan {
        LogicalPlan::Scan {
            table,
            time_range,
            joined_tables,
            scan_predicates,
            projected_columns,
            output_schema,
        } => {
            // Joins are rejected upstream by `lower_query_pipeline`
            // (Wave 4 — TASK-407). If a future change starts producing
            // joined logical scans without adding a physical mirror,
            // this assertion catches it loudly in debug builds rather
            // than silently dropping the joined tables.
            debug_assert!(
                joined_tables.is_empty(),
                "physical lowering does not yet handle joined source tables; \
                 add a physical mirror in TASK-407 before lifting the upstream rejection"
            );
            let _ = joined_tables;
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
            PhysicalPlan::Scan(ScanPhysical {
                table: table.name().to_string(),
                time_range,
                scan_predicates: compiled_predicates,
                projected_columns,
                output_schema,
                entity_key_col: table.entity_key_column().name.clone(),
                timestamp_col: table.timestamp_column().name.clone(),
            })
        }

        LogicalPlan::Filter {
            predicate,
            input,
            output_schema,
        } => {
            let compiled = CompiledExpr::from_typed(&predicate);
            let child = lower_physical(*input);
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
            let child = lower_physical(*input);
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
            let child = lower_physical(*input);
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
            let child = lower_physical(*plan);
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
                track_match_duration: demand.needs_match_detail,
                track_match_events: demand.needs_match_detail,
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

            let child = lower_physical(*input);
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

            let child = lower_physical(*input);
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

            let child = lower_physical(*input);
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
            let child = lower_physical(*input);
            PhysicalPlan::Distinct(DistinctPhysical {
                max_groups: DEFAULT_MAX_GROUPS,
                input: Box::new(child),
                output_schema,
            })
        }
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
        lower_physical(logical)
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
                assert!(scan.time_range.is_none());
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
            time_range: None,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema: OperatorSchema::from_table(&events_schema()),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
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
        let physical = lower_physical(logical);
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
        let physical = crate::plan(stmt, &catalog).expect("plan");

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
        let physical = crate::plan(stmt, &catalog).expect("plan");

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
        let physical = crate::plan(stmt, &catalog).expect("plan");

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
        let physical = crate::plan(stmt, &catalog).expect("plan");

        let PhysicalPlan::Distinct(distinct) = physical else {
            panic!("expected Distinct, got {physical:?}");
        };
        assert_eq!(distinct.max_groups, DEFAULT_MAX_GROUPS);
        assert!(matches!(*distinct.input, PhysicalPlan::Project(_)));
        assert_eq!(distinct.output_schema.columns().len(), 1);
        assert_eq!(distinct.output_schema.columns()[0].name, "entity_id");
    }
}
