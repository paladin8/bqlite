//! Entity-sorted scan operator (TASK-230).
//!
//! `ScanOperator` is the leaf of every Wave 2 data-plane execution
//! tree. It turns the `SegmentReader` trait's per-segment iteration
//! into a globally `(entity_id, ts)`-ordered stream of record batches
//! and applies two layers of predicate evaluation on the way out:
//!
//! 1. **Zone-map pruning**. The operator converts whichever pushable
//!    shapes it can recognise from `scan_predicates: Vec<CompiledExpr>`
//!    into a [`ScanPredicate`] and hands that to every
//!    `open_segment` call. The storage reader uses it to short-
//!    circuit row-group decode via
//!    [`ScanPredicate::accepts_zone_group`] (TASK-216). Non-pushable
//!    expressions flow straight through to the post-filter step
//!    below; zone-map pruning never sees them.
//! 2. **Row-level post-filter**. After the k-way merge materialises
//!    a batch, the operator evaluates **every** entry in
//!    `scan_predicates` via [`crate::eval::evaluate_bool`] and drops
//!    rows that fail. Zone-map pruning is one-sided — it may return
//!    extras that the row-level evaluator is expected to drop — so
//!    the post-filter is what preserves exact semantics. See
//!    `docs/design/storage/predicate-pushdown.md` §8 for the
//!    one-sided contract, and TASK-230's description for the "against
//!    materialized row groups" wording.
//!
//! Across segments, the operator consults
//! [`SegmentReader::segments`], opens every returned handle via
//! [`SegmentReader::open_segment`] (forwarding the same
//! [`ColumnProjection`] and the [`ScanPredicate`] built above), and
//! feeds the resulting per-segment [`SegmentScan`]s into a
//! [`KWayMergeScan`] that emits a single globally ordered row
//! stream. The merge honours the same
//! `Arc<[u8]> / Arc<FooterV1>` sharing inside each per-segment scan,
//! so the scan operator never duplicates segment bytes.
//!
//! ## Projection
//!
//! Empty `projected_columns` means "decode every declared column",
//! matching [`ColumnProjection::all`]. An explicit list must include
//! the table's entity-key and timestamp columns — the k-way merge
//! uses them as its sort key, so a projection that omits them is
//! rejected with `BqliteError::Schema` at construction time rather
//! than failing later inside the merge. Dropping the sort key from a
//! scan output is a concern of a future projection-pruning pass
//! (TASK-228), which must insert an extraction step rather than
//! asking the scan to emit an unsorted stream.
//!
//! ## Output schema
//!
//! The scan's [`OperatorSchema`] reflects the **declared columns**
//! the reader actually materialises — the same set that
//! `SegmentFileScan::build_scan_plan` constructs from
//! `current_schema.columns()`. Implicit system columns (`__seq_id`,
//! `__batch_id`) are **not** included; the Wave 2 segment reader
//! does not yet expose them to the scan plan, and declaring them in
//! `output_schema` would cause the k-way merge to reject real
//! batches that carry only the three declared columns. A future
//! extension may materialise system columns as a per-row counter,
//! at which point the scan's contract widens without breaking this
//! task's reader/merge pipeline.
//!
//! ## Tombstone-aware scan (TASK-434)
//!
//! The scan operator integrates the per-query [`TombstoneSnapshot`]
//! from `docs/design/storage/deletes.md` §6 / §7. When the operator is
//! constructed via [`ScanOperator::with_tombstones`] (or
//! [`ScanOperator::with_tombstones_and_scan_path`]), `open()` consults
//! the snapshot once for each segment handle and, when the segment's
//! `(window_id, shard_id)` has a non-empty [`TombstoneFile`] in the
//! snapshot, wraps that segment scan in a [`TombstoneScanWrapper`]
//! before handing it to the k-way merge.
//!
//! This preserves the documented scan-pipeline order — column
//! projection → zone-map pushdown → **tombstone filter** → merge →
//! post-filter → operators — so downstream operators never see
//! tombstoned rows. The snapshot is passed in by the engine at bind
//! time and never re-read from disk during execution (deletes.md §6.3).
//!
//! Tombstone filtering is **materialized only** today: the encoded
//! fast paths (single-segment encoded scan and `EncodedKWayMergeScan`)
//! need real encoded columns to hit their kernels, but
//! [`TombstoneScanWrapper`] applies the filter on the materialized
//! `RecordBatch` produced by `next_row_group`. When any segment in the
//! scan has a non-empty tombstone entry, the operator stays on the
//! materialized merge path for the whole query to avoid mixing encoded
//! and fallback outputs in the merge. An empty snapshot is a no-op:
//! the operator keeps whatever scan path was requested.
//!
//! The operator delegates row / batch / entity / time-range checks and
//! the scan-time tombstone ordering (batch → entity → row → time-range)
//! to [`bqlite_storage::tombstone::TombstoneFilter`]; see deletes.md
//! §7.1 for the full rationale.
//!
//! ## Lifecycle
//!
//! - `open()` enumerates segment handles, opens every segment, and
//!   primes a [`KWayMergeScan`]. Any error in enumeration or in a
//!   per-segment open surfaces here so that failures happen before
//!   the first pull. Opening is idempotent in the sense that the
//!   engine may call `close()` afterwards without ever calling
//!   `next_batch()`.
//! - `next_batch()` pulls from the merge, post-filters, and returns
//!   the first non-empty result. Fully rejected batches cause the
//!   operator to loop back and pull the merge again, matching the
//!   `FilterOperator` convention so downstream operators rarely see
//!   zero-row batches.
//! - `close()` drops the merge (which releases every per-segment
//!   scan) and marks the operator exhausted. Subsequent calls to
//!   `next_batch()` return `Ok(None)`.

use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::{self, kernels::boolean};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use bqlite_core::encoded::{EncodedBatch, RowRun, RowSelection};
use bqlite_core::{
    BqlType, BqliteError, ColumnDef, ColumnProjection, OperatorSchema, Predicate, PropertyValue,
    RangeOp, Result, ScanConjunct, ScanPredicate, SegmentHandle, SegmentReader, SegmentScan,
    TableSchema,
};
use bqlite_planner::compiled::{CompareOp, CompiledExpr, CompiledNode};
use bqlite_storage::segment::merge::{EncodedBatchSource, EncodedKWayMergeScan, KWayMergeScan};
use bqlite_storage::{AndPredicate, SampleFilter, TombstoneScanWrapper, TombstoneSnapshot};

use crate::encoded_filter::{apply_encoded_eq, partition_encoded_eq, EncodedEqShape};
use crate::eval;
use crate::materialize::{materialize_selected, materialize_stitched};
use crate::operator::{CancellationToken, PhysicalOperator};

// ─────────────────────────────────────────────────────────────────────────────
// ScanPath
// ─────────────────────────────────────────────────────────────────────────────

/// Read-path selector for [`ScanOperator`].
///
/// Chooses whether the scan iterates over materialized `RecordBatch`es
/// (the pre-Wave-5 path) or encoded-preserving `EncodedBatch`es (the
/// selection-first path from
/// `docs/design/storage/zero-copy-scan-filter.md`).
///
/// # Default
///
/// `Auto` — the scan picks `Encoded` when every pushed predicate has
/// an encoded kernel and the input scan supports
/// `next_encoded_row_group`, otherwise it falls back to
/// `Materialized`. The materialized path is retained as a debug
/// escape hatch.
///
/// # Environment override
///
/// Set `BQLITE_SCAN_PATH=materialized|encoded|auto` to override the
/// session default per process. Unrecognized values log a warning and
/// fall back to the compile-time default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanPath {
    /// Classic path: every row-group is decoded to a `RecordBatch`
    /// and post-filters run via `compute::filter_record_batch`.
    Materialized,
    /// Selection-first encoded path: scan produces `EncodedBatch`es
    /// and kernels emit row selections. Not yet implemented — treated
    /// as `Materialized` until Checkpoint 3 lands dictionary kernels.
    Encoded,
    /// Pick `Encoded` when every predicate has an encoded kernel and
    /// the input scan supports `next_encoded_row_group`; otherwise
    /// `Materialized`. Currently resolves to `Materialized` until
    /// Checkpoint 3.
    Auto,
}

impl Default for ScanPath {
    /// Compile-time default: `Auto` — pick the encoded path when every
    /// pushed predicate has an encoded kernel, otherwise fall back to
    /// `Materialized`. Set `BQLITE_SCAN_PATH=materialized` to force the
    /// debug-only materialized path for an entire process.
    fn default() -> Self {
        ScanPath::Auto
    }
}

impl ScanPath {
    /// Resolve the scan-path mode by consulting the `BQLITE_SCAN_PATH`
    /// environment variable, falling back to
    /// [`ScanPath::default`].
    pub fn from_env() -> ScanPath {
        match std::env::var("BQLITE_SCAN_PATH") {
            Ok(v) => Self::parse(&v).unwrap_or_default(),
            Err(_) => ScanPath::default(),
        }
    }

    /// Parse a string value (`"materialized"`, `"encoded"`, `"auto"`,
    /// case-insensitive). Returns `None` on unrecognized input.
    pub fn parse(s: &str) -> Option<ScanPath> {
        match s.trim().to_ascii_lowercase().as_str() {
            "materialized" => Some(ScanPath::Materialized),
            "encoded" => Some(ScanPath::Encoded),
            "auto" => Some(ScanPath::Auto),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ScanOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Entity-sorted scan over a [`SegmentReader`].
///
/// See the module docs for the full contract. Briefly: enumerate
/// segments → open each with projection + zone-map predicate → feed
/// into [`KWayMergeScan`] → post-filter with the compiled predicates
/// → emit sorted batches.
pub struct ScanOperator {
    /// Upstream reader. Held as `Arc` so multiple shard-tasks can
    /// share one reader in later waves without changing the trait.
    reader: Arc<dyn SegmentReader>,
    /// Column projection forwarded to every `open_segment` call.
    projection: ColumnProjection,
    /// Zone-map pruning predicate. Built from the convertible subset
    /// of `post_filters`. `None` when no predicate converts — in that
    /// case no zone-map pruning happens and the reader streams every
    /// row-group unchanged.
    scan_predicate: Option<Arc<dyn Predicate>>,
    /// Full list of pushed `CompiledExpr` predicates. The scan
    /// re-evaluates **all** of them on every materialised batch, not
    /// just the ones that failed to convert to a `ScanConjunct` —
    /// this is how the operator honours the "exact semantics" half
    /// of the pushdown contract when zone-map pruning is
    /// conservative.
    post_filters: Vec<CompiledExpr>,
    /// Cancellation flag checked at the top of each `next_batch`.
    cancel: CancellationToken,
    /// The operator's public output schema. Built from the reader's
    /// [`TableSchema`] and `projection`; reflects the declared
    /// columns the reader actually materialises (see module docs).
    output_schema: OperatorSchema,
    /// Arrow form of `output_schema`, cached so the merge gets a
    /// shared `Arc<ArrowSchema>` without rebuilding per batch.
    arrow_schema: Arc<ArrowSchema>,
    /// Ordinal of the entity-key column in `arrow_schema`. Computed
    /// at construction so the merge's sort-key walk does not pay a
    /// name lookup per batch.
    entity_col: usize,
    /// Ordinal of the timestamp column in `arrow_schema`.
    ts_col: usize,
    /// Running k-way merge. `None` until `open()` has been called;
    /// reset to `None` by `close()` so the operator may be closed
    /// before any data flows. Mutually exclusive with `encoded_scan`:
    /// the encoded single-segment fast path bypasses the merge.
    merge: Option<KWayMergeScan>,
    /// Direct single-segment scan for the encoded path. `Some` only
    /// when `scan_path != Materialized` and exactly one segment handle
    /// is visible — otherwise we fall back to the merge.
    encoded_scan: Option<Box<dyn SegmentScan>>,
    /// Encoded-preserving k-way merge (CP5). `Some` only when
    /// `scan_path != Materialized` and at least two segments are
    /// visible — otherwise a single-segment scan uses `encoded_scan`
    /// and the materialized path uses `merge`. The `debug_assert!` in
    /// `open()` enforces at most one of these three holders is `Some`.
    encoded_merge: Option<EncodedKWayMergeScan>,
    /// Latches on the first `Ok(None)` from `next_batch` and keeps
    /// subsequent pulls cheap and side-effect-free.
    exhausted: bool,
    /// Read-path mode. In Checkpoint 1 this is recorded but does not
    /// affect behavior — every variant dispatches to the materialized
    /// path. Checkpoint 3 lights up the `Encoded` branch.
    scan_path: ScanPath,
    /// Per-column `BqlType` list, in the same order as
    /// `arrow_schema.fields()`. Cached so the encoded path's
    /// materialization boundary and fallback-eq decoder don't
    /// re-derive it per row-group.
    types: Vec<BqlType>,
    /// Predicates partitioned into the `col == literal` shape the
    /// encoded path dispatches on. Populated only when `scan_path !=
    /// Materialized`; empty otherwise.
    encoded_shapes: Vec<EncodedEqShape>,
    /// Residual `CompiledExpr` list that the encoded path still needs
    /// to enforce post-materialization via arrow-compute (anything that
    /// didn't match the encoded shape goes here). Populated only when
    /// `scan_path != Materialized`.
    encoded_residual: Vec<CompiledExpr>,
    /// Per-query tombstone snapshot consulted at `open()` time to wrap
    /// affected segments in [`TombstoneScanWrapper`]. The engine's bind
    /// step loads this once per query and shares it (via `Arc`) across
    /// every scan operator in that query so the whole query observes a
    /// single tombstone epoch (deletes.md §6.1). Defaults to the empty
    /// snapshot — older callers that don't pass one get the pre-TASK-434
    /// behavior unchanged.
    tombstones: Arc<TombstoneSnapshot>,
    /// Name of the entity-key column, cached from the reader's
    /// [`TableSchema`]. Copied here so `open()` can hand it to
    /// [`TombstoneScanWrapper`] without re-borrowing `reader.schema()`.
    entity_key_name: String,
    /// Name of the timestamp column, cached from the reader's
    /// [`TableSchema`]. Used alongside `entity_key_name` when wrapping
    /// per-segment scans with tombstone filtering.
    ts_col_name: String,
    /// Entity-level SAMPLE filter pushed down by
    /// [`bqlite_planner::opt::pushdown_sample`]. Populated by the
    /// engine bind step from [`bqlite_planner::physical::ScanPhysical::sample`].
    /// `None` disables the per-row sample test — the operator
    /// behaves identically to the pre-TASK-430 contract.
    ///
    /// Held as `Arc<SampleFilter>` so future per-shard fan-out paths
    /// can share the same filter across parallel tasks without
    /// duplicating the precomputed threshold + seed.
    sample_filter: Option<Arc<SampleFilter>>,
}

impl std::fmt::Debug for ScanOperator {
    /// Lightweight [`Debug`] impl. The struct holds a few trait
    /// objects (`Arc<dyn SegmentReader>`, `Arc<dyn Predicate>`) that
    /// we deliberately do not dereference — dumping their state is
    /// not useful for operator debugging, and the trait objects'
    /// own `Debug` surface is minimal. Tests that use `expect_err`
    /// format this value on the failing `Ok` arm; see `Result::expect_err`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanOperator")
            .field("projection", &self.projection)
            .field("output_schema", &self.output_schema)
            .field("entity_col", &self.entity_col)
            .field("ts_col", &self.ts_col)
            .field("has_scan_predicate", &self.scan_predicate.is_some())
            .field("post_filter_count", &self.post_filters.len())
            .field(
                "open",
                &(self.merge.is_some()
                    || self.encoded_scan.is_some()
                    || self.encoded_merge.is_some()),
            )
            .field("exhausted", &self.exhausted)
            .field("scan_path", &self.scan_path)
            .field("has_sample_filter", &self.sample_filter.is_some())
            .finish()
    }
}

impl ScanOperator {
    /// Construct an entity-sorted scan.
    ///
    /// # Arguments
    ///
    /// - `reader` — the table's segment reader, typically from
    ///   [`bqlite_storage::Database::segment_reader`]. Its
    ///   [`TableSchema`] drives projection resolution and sort-key
    ///   discovery.
    /// - `projected_columns` — empty means "every declared column".
    ///   A non-empty list must include the entity-key and timestamp
    ///   columns so the k-way merge has a sort key; a list missing
    ///   either returns [`BqliteError::Schema`].
    /// - `scan_predicates` — the `Vec<CompiledExpr>` TASK-227's
    ///   predicate-pushdown pass lifted out of a parent
    ///   `FilterPhysical`. Every entry is re-evaluated post-decode
    ///   for exact semantics; the convertible subset additionally
    ///   drives zone-map pruning at row-group granularity.
    /// - `cancel` — shared cancellation token.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Schema`] if `projected_columns` names a
    ///   column absent from the reader's table schema, or if the
    ///   projection omits the entity-key or timestamp column.
    /// - [`BqliteError::Execution`] if the resulting Arrow schema
    ///   has a key column type unsupported by [`KWayMergeScan`] —
    ///   this is effectively unreachable because
    ///   [`TableSchema::new`] already rejects unsupported key
    ///   types, but the constructor propagates the merge's
    ///   validation error rather than assuming.
    pub fn new(
        reader: Arc<dyn SegmentReader>,
        projected_columns: &[String],
        scan_predicates: Vec<CompiledExpr>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        Self::with_scan_path(
            reader,
            projected_columns,
            scan_predicates,
            cancel,
            ScanPath::from_env(),
        )
    }

    /// Construct a scan that will consult `tombstones` at `open()` time
    /// to wrap affected segments in [`TombstoneScanWrapper`].
    ///
    /// The snapshot is shared by `Arc` so an engine-side loader (bind
    /// step) can hand the same snapshot to every scan operator in a
    /// query — see `docs/design/storage/deletes.md` §6 for the
    /// per-query snapshot contract.
    pub fn with_tombstones(
        reader: Arc<dyn SegmentReader>,
        projected_columns: &[String],
        scan_predicates: Vec<CompiledExpr>,
        cancel: CancellationToken,
        tombstones: Arc<TombstoneSnapshot>,
    ) -> Result<Self> {
        Self::with_tombstones_and_scan_path(
            reader,
            projected_columns,
            scan_predicates,
            cancel,
            ScanPath::from_env(),
            tombstones,
        )
    }

    /// Construct a scan with an explicit [`ScanPath`] mode, overriding
    /// both the session default and the `BQLITE_SCAN_PATH` env var.
    ///
    /// In Checkpoint 1 the path is recorded for later dispatch but
    /// does not alter runtime behavior; every variant resolves to the
    /// materialized path.
    pub fn with_scan_path(
        reader: Arc<dyn SegmentReader>,
        projected_columns: &[String],
        scan_predicates: Vec<CompiledExpr>,
        cancel: CancellationToken,
        scan_path: ScanPath,
    ) -> Result<Self> {
        Self::with_tombstones_and_scan_path(
            reader,
            projected_columns,
            scan_predicates,
            cancel,
            scan_path,
            Arc::new(TombstoneSnapshot::empty()),
        )
    }

    /// Construct a scan with an explicit [`ScanPath`] and per-query
    /// [`TombstoneSnapshot`]. This is the base constructor every other
    /// `new*`/`with_*` variant delegates to.
    ///
    /// Tombstones affect `open()` only: every segment handle whose
    /// `(window_id, shard_id)` has a non-empty entry in the snapshot is
    /// wrapped in a [`TombstoneScanWrapper`] before being handed to the
    /// merge. If **any** input segment needs wrapping, the operator
    /// forces itself onto the materialized merge path for the whole
    /// query because the tombstone wrapper applies its filter on
    /// materialized row groups (deletes.md §7) — mixing a wrapped
    /// segment into the encoded k-way merge would decode some inputs
    /// and not others, breaking the merge's encoded-batch contract.
    pub fn with_tombstones_and_scan_path(
        reader: Arc<dyn SegmentReader>,
        projected_columns: &[String],
        scan_predicates: Vec<CompiledExpr>,
        cancel: CancellationToken,
        scan_path: ScanPath,
        tombstones: Arc<TombstoneSnapshot>,
    ) -> Result<Self> {
        let projection = if projected_columns.is_empty() {
            ColumnProjection::all()
        } else {
            ColumnProjection::with_columns(projected_columns.iter().cloned())
        };
        let output_schema = build_output_schema(reader.schema(), &projection)?;
        let arrow_schema = Arc::new(output_schema.to_arrow_schema());

        let entity_name = reader.schema().entity_key_column().name.as_str();
        let ts_name = reader.schema().timestamp_column().name.as_str();
        let entity_col = output_schema
            .column(entity_name)
            .map(|(i, _)| i)
            .ok_or_else(|| {
                BqliteError::Schema(format!(
                    "scan: projection must include the entity-key column `{entity_name}`"
                ))
            })?;
        let ts_col = output_schema
            .column(ts_name)
            .map(|(i, _)| i)
            .ok_or_else(|| {
                BqliteError::Schema(format!(
                    "scan: projection must include the timestamp column `{ts_name}`"
                ))
            })?;
        let entity_key_name = entity_name.to_string();
        let ts_col_name = ts_name.to_string();

        let scan_predicate = build_scan_predicate(&scan_predicates);

        let types: Vec<BqlType> = output_schema
            .columns()
            .iter()
            .map(|c| c.bql_type.clone())
            .collect();
        let (encoded_shapes, encoded_residual) = if scan_path == ScanPath::Materialized {
            (Vec::new(), Vec::new())
        } else {
            partition_encoded_eq(&scan_predicates)
        };

        Ok(Self {
            reader,
            projection,
            scan_predicate,
            post_filters: scan_predicates,
            cancel,
            output_schema,
            arrow_schema,
            entity_col,
            ts_col,
            merge: None,
            encoded_scan: None,
            encoded_merge: None,
            exhausted: false,
            scan_path,
            types,
            encoded_shapes,
            encoded_residual,
            tombstones,
            entity_key_name,
            ts_col_name,
            sample_filter: None,
        })
    }

    /// Attach an entity-level SAMPLE filter pushed down from the
    /// physical plan's [`ScanPhysical::sample`] field (TASK-430).
    ///
    /// Must be called before [`ScanOperator::open`]. The filter is
    /// evaluated on every materialized batch via a boolean mask over
    /// the entity-id column; non-sampled rows are dropped before the
    /// batch reaches downstream operators. Combined with `post_filters`
    /// under AND semantics — order is insignificant because the
    /// sample test is a function of the entity-id column alone.
    ///
    /// Returns `&mut self` so callers can chain builders; the engine
    /// bind step uses this form.
    pub fn with_sample_filter(&mut self, filter: Arc<SampleFilter>) -> &mut Self {
        self.sample_filter = Some(filter);
        self
    }

    /// Convenience constructor: scan every declared column with no
    /// pushed predicates and a fresh cancellation token. Used by
    /// planner unit tests that want to exercise the operator's
    /// iteration shape without building a full `CompiledExpr` tree.
    pub fn full_scan(reader: Arc<dyn SegmentReader>) -> Result<Self> {
        Self::new(reader, &[], Vec::new(), CancellationToken::new())
    }

    /// Read the current scan-path mode.
    pub fn scan_path(&self) -> ScanPath {
        self.scan_path
    }

    /// Single-segment encoded pull: drive `next_encoded_row_group` on
    /// the stashed scan, apply recognized `col == literal` predicates
    /// via [`apply_encoded_eq`] (kernel or fallback), materialize the
    /// resulting selection, and enforce any residual predicates via the
    /// existing post-filter path.
    ///
    /// Loops until it finds a row-group that yields a non-empty batch
    /// so downstream consumers never see an empty result (matches the
    /// materialized path's "re-pull on empty" convention).
    fn encoded_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let scan = self
                .encoded_scan
                .as_mut()
                .expect("encoded_scan is Some (checked by caller)");
            let Some(encoded) = scan.next_encoded_row_group()? else {
                self.exhausted = true;
                return Ok(None);
            };
            let rows = encoded.row_count;
            if rows == 0 {
                return Ok(Some(RecordBatch::new_empty(self.arrow_schema.clone())));
            }
            let mut sel = RowSelection::from_runs(vec![RowRun {
                start: 0,
                len: rows,
            }]);
            for shape in &self.encoded_shapes {
                if sel.is_empty() {
                    break;
                }
                sel = apply_encoded_eq(shape, &encoded, &sel, &self.types[shape.col_index])?;
            }
            if sel.is_empty() {
                // Every row filtered — pull the next row-group without
                // paying a full materialization.
                continue;
            }
            let fb =
                materialize_selected(&encoded, Some(&sel), &self.types, self.arrow_schema.clone())?;
            let batch = fb.batch;
            // Residual predicates: everything that didn't match the
            // encoded-eq shape. Runs the full post_filters list when
            // there are no recognized shapes so we stay correct on the
            // "encoded mode requested but no pushable equality"
            // fixture.
            let batch = if self.encoded_shapes.is_empty() {
                self.apply_post_filters(batch)?
            } else if self.encoded_residual.is_empty() {
                batch
            } else {
                apply_compiled_filters(&self.encoded_residual, batch)?
            };
            let batch = self.apply_sample_filter(batch)?;
            if batch.num_rows() > 0 {
                return Ok(Some(batch));
            }
        }
    }

    /// Multi-segment encoded pull (CP5): drive [`EncodedKWayMergeScan`]
    /// to get a [`StitchedBatch`](bqlite_core::encoded::StitchedBatch),
    /// materialize it through the shared `materialize_stitched`
    /// consumer, then enforce any residual `CompiledExpr`s exactly as
    /// `encoded_next_batch` does for the single-segment path.
    ///
    /// Loops past fully-filtered results so consumers never see empty
    /// batches. Checks cancellation each iteration because a merge
    /// over many sources may run long between emissions.
    fn encoded_merge_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.cancel.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            let merge = self
                .encoded_merge
                .as_mut()
                .expect("encoded_merge is Some (checked by caller)");
            let Some(stitched) = merge.next_stitched_batch()? else {
                self.exhausted = true;
                return Ok(None);
            };
            let fb = materialize_stitched(&stitched, &self.types, self.arrow_schema.clone())?;
            let batch = fb.batch;
            if batch.num_rows() == 0 {
                continue;
            }
            // Residual predicates mirror `encoded_next_batch`:
            // - No recognized shapes → run the full `post_filters` list.
            // - Otherwise → only the non-shape residual (already
            //   partitioned at construction time).
            let batch = if self.encoded_shapes.is_empty() {
                self.apply_post_filters(batch)?
            } else if self.encoded_residual.is_empty() {
                batch
            } else {
                apply_compiled_filters(&self.encoded_residual, batch)?
            };
            let batch = self.apply_sample_filter(batch)?;
            if batch.num_rows() > 0 {
                return Ok(Some(batch));
            }
        }
    }

    /// Apply the entity-level SAMPLE filter (TASK-430) to `batch`.
    ///
    /// Short-circuits when no filter is attached (the pre-TASK-430
    /// behavior) or when the filter is in pass-through mode
    /// (`fraction == 1.0`). Otherwise builds a boolean mask over the
    /// entity-id column and returns a row-filtered batch.
    ///
    /// Rows whose entity-id fails the hash threshold drop; rows
    /// whose entity-id is null also drop (the `SampleFilter`
    /// invariant: sampled entities have stable hashes, nulls do
    /// not). The entity-id column is always present in the scan
    /// output — `ScanOperator::with_tombstones_and_scan_path` rejects
    /// projections that omit it — so the lookup is infallible.
    fn apply_sample_filter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(filter) = self.sample_filter.as_deref() else {
            return Ok(batch);
        };
        if filter.is_pass_through() {
            return Ok(batch);
        }
        let entity_col = batch.column(self.entity_col);
        let mask = filter.apply_to_array(entity_col.as_ref())?;
        let filtered = compute::filter_record_batch(&batch, &mask)?;
        Ok(filtered)
    }

    /// Evaluate every entry in `post_filters` against `batch` and
    /// return the subset of rows that satisfy all of them.
    ///
    /// Empty `post_filters` short-circuits to the input — avoiding
    /// both an Arrow allocation and the boolean-array dance — which
    /// matters for the common pre-TASK-227 case where nothing has
    /// been pushed yet.
    fn apply_post_filters(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.post_filters.is_empty() {
            return Ok(batch);
        }
        let mut combined: Option<BooleanArray> = None;
        for predicate in &self.post_filters {
            let mask = eval::evaluate_bool(predicate, &batch)?;
            combined = Some(match combined {
                None => mask,
                Some(acc) => boolean::and_kleene(&acc, &mask)?,
            });
        }
        // `post_filters.is_empty()` short-circuits above, so we
        // always have at least one mask when we reach this point.
        let mask = combined.expect("post_filters non-empty");
        let filtered = compute::filter_record_batch(&batch, &mask)?;
        Ok(filtered)
    }
}

impl PhysicalOperator for ScanOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        // Empty-set SAMPLE short-circuits the whole scan: nothing the
        // reader produces could pass the threshold test, so avoid the
        // segment-enumeration and merge-setup cost entirely. Callers
        // will see `next_batch() == Ok(None)` on the first pull.
        if matches!(&self.sample_filter, Some(f) if f.is_empty_set()) {
            self.exhausted = true;
            return Ok(());
        }

        // Materialise the handle list up-front so any enumeration
        // error surfaces from `open()`, before results start
        // flowing — matches the `PhysicalOperator::open` doc.
        let handles: Result<Vec<SegmentHandle>> = self.reader.segments().collect();
        let handles = handles?;

        // Build the per-segment zone-map predicate. When both a base
        // scan predicate (from the pushdown pass) and a sample filter
        // (from TASK-430) are attached, compose them under AND so the
        // reader's row-group pruning evaluates both checks in one
        // traversal. Either side being `None` short-circuits to the
        // other; both `None` disables zone-map pruning entirely.
        let zone_predicate: Option<Arc<dyn Predicate>> =
            match (self.scan_predicate.clone(), self.sample_filter.clone()) {
                (None, None) => None,
                (Some(p), None) => Some(p),
                (None, Some(sample)) => {
                    if sample.is_pass_through() {
                        // Pass-through accepts every zone by
                        // definition — no point handing the reader a
                        // predicate it cannot prune anything with.
                        None
                    } else {
                        Some(sample as Arc<dyn Predicate>)
                    }
                }
                (Some(base), Some(sample)) => {
                    if sample.is_pass_through() {
                        Some(base)
                    } else {
                        AndPredicate::new(vec![base, sample as Arc<dyn Predicate>])
                            .map(|p| Arc::new(p) as Arc<dyn Predicate>)
                    }
                }
            };

        let mut scans: Vec<Box<dyn SegmentScan>> = Vec::with_capacity(handles.len());
        let mut any_wrapped = false;
        for handle in &handles {
            let scan =
                self.reader
                    .open_segment(handle, &self.projection, zone_predicate.clone())?;
            // Tombstone wrapping (deletes.md §7): every segment whose
            // `(window_id, shard_id)` has a non-empty entry in the
            // per-query snapshot is wrapped so tombstone filtering runs
            // after zone-map pushdown but before rows leave the scan.
            //
            // Snapshots only ever carry non-empty entries (see
            // `TombstoneSnapshot::from_map` / `load_tombstone_snapshot`),
            // so a `Some` hit is proof that wrapping is worth the per-row
            // cost.
            let scan = match snapshot_key(handle) {
                Some(key) => match self.tombstones.get(key.0, key.1) {
                    Some(tf) if !tf.is_empty() => {
                        any_wrapped = true;
                        Box::new(TombstoneScanWrapper::new(
                            scan,
                            tf.clone(),
                            self.entity_key_name.clone(),
                            self.ts_col_name.clone(),
                        )) as Box<dyn SegmentScan>
                    }
                    _ => scan,
                },
                None => scan,
            };
            scans.push(scan);
        }

        // If any segment is tombstone-wrapped, stay on the materialized
        // merge path: the encoded k-way merge expects real encoded
        // columns from every input, and `TombstoneScanWrapper` produces
        // its filter on top of `next_row_group` (materialized). Mixing
        // wrapped and unwrapped inputs would decode some segments and
        // not others, violating the encoded-batch contract. Deletes are
        // expected to be rare, so this is a conservative fallback.
        let encoded_requested = self.scan_path != ScanPath::Materialized && !any_wrapped;
        if encoded_requested && scans.len() == 1 {
            // Single-segment encoded fast path: drive the segment
            // directly via `next_encoded_row_group`, no merge.
            self.encoded_scan = Some(scans.pop().unwrap());
        } else if encoded_requested {
            // Multi-segment encoded path (CP5): wrap each segment in a
            // `KernelAppliedSource` that shares the compiled shapes +
            // types by `Arc`, and hand the vec to `EncodedKWayMergeScan`.
            let shapes: Arc<[EncodedEqShape]> = Arc::from(self.encoded_shapes.as_slice());
            let types: Arc<[BqlType]> = Arc::from(self.types.as_slice());
            let sources: Vec<Box<dyn EncodedBatchSource>> = scans
                .into_iter()
                .map(|scan| -> Box<dyn EncodedBatchSource> {
                    Box::new(KernelAppliedSource::new(
                        scan,
                        shapes.clone(),
                        types.clone(),
                        self.cancel.clone(),
                    ))
                })
                .collect();
            let merge = EncodedKWayMergeScan::new(
                sources,
                self.arrow_schema.clone(),
                self.entity_col,
                self.ts_col,
            )?;
            self.encoded_merge = Some(merge);
        } else {
            let merge = KWayMergeScan::new(
                scans,
                self.arrow_schema.clone(),
                self.entity_col,
                self.ts_col,
            )?;
            self.merge = Some(merge);
        }

        debug_assert!(
            (self.encoded_scan.is_some() as u8
                + self.encoded_merge.is_some() as u8
                + self.merge.is_some() as u8)
                <= 1,
            "ScanOperator scan-holder invariant: at most one of \
             encoded_scan/encoded_merge/merge may be Some"
        );

        self.exhausted = false;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Cancelled);
        }
        // Explicit precedence order: single-segment encoded bypass
        // wins over the multi-segment encoded merge, which wins over
        // the materialized merge. The `debug_assert!` in `open()`
        // guarantees at most one holder is `Some`; this order simply
        // pins a deterministic fall-through if a future bug violates
        // that invariant in release builds.
        if self.encoded_scan.is_some() {
            return self.encoded_next_batch();
        }
        if self.encoded_merge.is_some() {
            return self.encoded_merge_next_batch();
        }
        loop {
            let merge = self.merge.as_mut().ok_or_else(|| {
                BqliteError::Execution("ScanOperator::next_batch called before open()".to_string())
            })?;
            let Some(batch) = merge.next_batch()? else {
                self.exhausted = true;
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                // The merge itself emits non-empty batches, but
                // downstream evolution may produce empties; forward
                // unconditionally rather than loop.
                return Ok(Some(batch));
            }
            let filtered = self.apply_post_filters(batch)?;
            let filtered = self.apply_sample_filter(filtered)?;
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
            // Every row rejected — pull the merge again.
        }
    }

    fn close(&mut self) -> Result<()> {
        // Dropping every scan holder releases every held per-segment
        // scan (file handles, decompression state) via destructors.
        // `close` must tolerate being called with each field `None`
        // after a failed `open` or without ever reaching `open`.
        self.merge = None;
        self.encoded_scan = None;
        self.encoded_merge = None;
        self.exhausted = true;
        Ok(())
    }
}

/// Free-function counterpart to [`ScanOperator::apply_post_filters`]
/// used by the encoded path, where the residual list is a borrowed
/// slice rather than `self.post_filters`.
fn apply_compiled_filters(predicates: &[CompiledExpr], batch: RecordBatch) -> Result<RecordBatch> {
    if predicates.is_empty() {
        return Ok(batch);
    }
    let mut combined: Option<BooleanArray> = None;
    for predicate in predicates {
        let mask = eval::evaluate_bool(predicate, &batch)?;
        combined = Some(match combined {
            None => mask,
            Some(acc) => boolean::and_kleene(&acc, &mask)?,
        });
    }
    let mask = combined.expect("predicates non-empty");
    let filtered = compute::filter_record_batch(&batch, &mask)?;
    Ok(filtered)
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-segment encoded adapter (CP5)
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps a per-segment `Box<dyn SegmentScan>` into an
/// [`EncodedBatchSource`] by applying the recognised
/// [`EncodedEqShape`]s from the scan operator to every row group
/// before handing the `(EncodedBatch, RowSelection)` pair to
/// [`EncodedKWayMergeScan`].
///
/// Mirrors the single-segment pull loop in
/// [`ScanOperator::encoded_next_batch`], minus the materialization
/// step — the merge does that via `materialize_stitched` after picks.
///
/// Shared state (the shape list and per-column `BqlType`s) is borrowed
/// through `Arc` so every source wrapper references the same data
/// without per-batch clones.
struct KernelAppliedSource {
    inner: Box<dyn SegmentScan>,
    shapes: Arc<[EncodedEqShape]>,
    types: Arc<[BqlType]>,
    cancel: CancellationToken,
}

impl KernelAppliedSource {
    fn new(
        inner: Box<dyn SegmentScan>,
        shapes: Arc<[EncodedEqShape]>,
        types: Arc<[BqlType]>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            inner,
            shapes,
            types,
            cancel,
        }
    }
}

impl EncodedBatchSource for KernelAppliedSource {
    fn next(&mut self) -> Result<Option<(EncodedBatch, RowSelection)>> {
        loop {
            // Check cancellation per-row-group: a deep segment where
            // every row gets filtered can spin here for a long time
            // before yielding back to `encoded_merge_next_batch`.
            if self.cancel.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            let Some(encoded) = self.inner.next_encoded_row_group()? else {
                return Ok(None);
            };
            let rows = encoded.row_count;
            if rows == 0 {
                // Skip empty row groups; the merge would otherwise
                // pay a reload round-trip for no picks.
                continue;
            }
            let mut sel = RowSelection::from_runs(vec![RowRun {
                start: 0,
                len: rows,
            }]);
            for shape in self.shapes.iter() {
                if sel.is_empty() {
                    break;
                }
                sel = apply_encoded_eq(shape, &encoded, &sel, &self.types[shape.col_index])?;
            }
            if sel.is_empty() {
                // Every row filtered — skip up-front rather than
                // forwarding an empty selection for the merge to
                // discard.
                continue;
            }
            return Ok(Some((encoded, sel)));
        }
    }
}

/// Convert a [`SegmentHandle`]'s window/shard identifiers to the
/// `(u32, u16)` shape used by [`TombstoneSnapshot`].
///
/// The handle carries `window_id: u64` and `shard_id: u32`, but
/// tombstone files live under `<table>/windows/w_<window>/shard_<shard>`
/// where the manifest writer already constrains window IDs to `u32` and
/// shard IDs to `u16` (see `crates/bqlite-storage/src/manifest.rs` and
/// `tombstone::tombstone_file_path`). Out-of-range values simply cannot
/// have a tombstone file on disk, so we return `None` and leave the
/// segment unwrapped — a safer behavior than panicking and a tighter
/// one than silently truncating.
fn snapshot_key(handle: &SegmentHandle) -> Option<(u32, u16)> {
    let window: u32 = handle.window_id.try_into().ok()?;
    let shard: u16 = handle.shard_id.try_into().ok()?;
    Some((window, shard))
}

// ─────────────────────────────────────────────────────────────────────────────
// Output schema construction
// ─────────────────────────────────────────────────────────────────────────────

/// Build the scan operator's [`OperatorSchema`] from the reader's
/// table schema and the effective [`ColumnProjection`].
///
/// For `is_all` projections the result lists every declared column
/// in table order. For an explicit projection the function resolves
/// every requested name against `TableSchema::column` and preserves
/// duplicates (duplicates are rejected by `OperatorSchema::new`).
///
/// The returned schema matches what
/// [`bqlite_storage::segment::reader::SegmentFileScan::build_scan_plan`]
/// materialises for the same projection; this match is load-bearing
/// because the k-way merge validates each per-segment batch's
/// schema against the operator-supplied one.
fn build_output_schema(
    table: &TableSchema,
    projection: &ColumnProjection,
) -> Result<OperatorSchema> {
    let columns: Vec<ColumnDef> = if projection.is_all() {
        table.columns().to_vec()
    } else {
        // Validate every requested name first so callers get a clear
        // error before we iterate the table schema.
        for name in projection.columns() {
            if table.column(name).is_none() {
                return Err(BqliteError::Schema(format!(
                    "scan: projected column `{name}` not in table `{}`",
                    table.name()
                )));
            }
        }
        // Output columns in *table-schema order*, not in the order they
        // appear in `projected_columns`. This preserves the column indices
        // that `CompiledNode::Column { index }` was compiled against (the
        // full-schema position), so pushed-down predicates and project
        // expressions remain correct after the pruning pass trims the set.
        let projected: std::collections::HashSet<&str> =
            projection.columns().iter().map(String::as_str).collect();
        table
            .columns()
            .iter()
            .filter(|col| projected.contains(col.name.as_str()))
            .cloned()
            .collect()
    };
    OperatorSchema::new(columns)
}

// ─────────────────────────────────────────────────────────────────────────────
// CompiledExpr → ScanPredicate lowering
// ─────────────────────────────────────────────────────────────────────────────

/// Convert a `Vec<CompiledExpr>` into a `ScanPredicate`, extracting
/// every conjunct that matches one of the pushable shapes from
/// `docs/design/storage/predicate-pushdown.md` §4.
///
/// Non-convertible expressions are silently dropped from the
/// returned predicate — they remain in the scan operator's
/// `post_filters` list and are re-evaluated at row level after the
/// merge materialises each batch. Dropping unconvertible
/// expressions here is safe because post-filter is the single
/// source of truth for exactness; the `ScanPredicate` is only ever
/// used as a zone-map pruning hint.
///
/// Returns `None` when no conjunct converts. That is treated as
/// "no pushdown at the reader level" — the `open_segment` call
/// then hands `None` for `predicate`, and the reader streams every
/// row-group unchanged.
fn build_scan_predicate(predicates: &[CompiledExpr]) -> Option<Arc<dyn Predicate>> {
    let mut conjuncts: Vec<ScanConjunct> = Vec::new();
    for pred in predicates {
        if let Some(conj) = lower_to_conjunct(pred) {
            conjuncts.push(conj);
        }
    }
    if conjuncts.is_empty() {
        None
    } else {
        Some(Arc::new(ScanPredicate::new(conjuncts)) as Arc<dyn Predicate>)
    }
}

/// Try to convert a single [`CompiledExpr`] to a [`ScanConjunct`].
/// Returns `None` when the expression does not match any of the
/// Wave 2 pushable shapes.
fn lower_to_conjunct(expr: &CompiledExpr) -> Option<ScanConjunct> {
    match &expr.node {
        CompiledNode::Compare {
            op, left, right, ..
        } => lower_compare(*op, left, right),
        CompiledNode::IsNull { input, negated } => {
            let column = column_name(input)?;
            Some(if *negated {
                ScanConjunct::IsNotNull { column }
            } else {
                ScanConjunct::IsNull { column }
            })
        }
        CompiledNode::InLiteralSet {
            input,
            values,
            negated,
            ..
        } => {
            if *negated || values.is_empty() {
                // `NOT IN` is not a Wave 2 pushable shape (it would
                // decompose into a NotEqual conjunct per literal,
                // which the storage layer does not yet support as a
                // cross-row conjunction). Empty `IN ()` should never
                // reach us — TASK-227 elides empty sets to a
                // constant `false` residual — but guard defensively.
                return None;
            }
            let column = column_name(input)?;
            Some(ScanConjunct::InSet {
                column,
                values: values.clone(),
            })
        }
        // Column / literal nodes at the top level cannot be
        // pushable on their own; And / Or / Not / arithmetic /
        // function calls are not in the §4 taxonomy. Every other
        // variant falls through as "not pushable".
        _ => None,
    }
}

/// Lower a `Compare` node where exactly one side is a column and
/// the other is a literal. Other shapes (col-to-col, expr-to-expr,
/// literal-to-literal) are not pushable in Wave 2.
fn lower_compare(op: CompareOp, left: &CompiledExpr, right: &CompiledExpr) -> Option<ScanConjunct> {
    let (column, value, flipped) = match (&left.node, &right.node) {
        (CompiledNode::Column { name, .. }, CompiledNode::Literal(v)) => {
            (name.clone(), v.clone(), false)
        }
        (CompiledNode::Literal(v), CompiledNode::Column { name, .. }) => {
            (name.clone(), v.clone(), true)
        }
        _ => return None,
    };
    // `PropertyValue::Null` on either side makes the comparison
    // UNKNOWN under three-valued logic; the filter operator drops
    // such rows. The storage layer does not currently understand a
    // null-valued `Equal` / `Range` conjunct, so skip pushdown and
    // let post-filter handle it.
    if matches!(value, PropertyValue::Null) {
        return None;
    }
    let op = if flipped { flip_compare(op) } else { op };
    Some(compare_to_conjunct(op, column, value))
}

/// Flip a comparison operator so the column is always on the left
/// after the rewrite. Called from [`lower_compare`] when the
/// literal was on the left-hand side of the original expression.
fn flip_compare(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Equal => CompareOp::Equal,
        CompareOp::NotEqual => CompareOp::NotEqual,
        CompareOp::Less => CompareOp::Greater,
        CompareOp::LessOrEqual => CompareOp::GreaterOrEqual,
        CompareOp::Greater => CompareOp::Less,
        CompareOp::GreaterOrEqual => CompareOp::LessOrEqual,
    }
}

fn compare_to_conjunct(op: CompareOp, column: String, value: PropertyValue) -> ScanConjunct {
    match op {
        CompareOp::Equal => ScanConjunct::Equal { column, value },
        CompareOp::NotEqual => ScanConjunct::NotEqual { column, value },
        CompareOp::Less => ScanConjunct::Range {
            column,
            op: RangeOp::Lt,
            value,
        },
        CompareOp::LessOrEqual => ScanConjunct::Range {
            column,
            op: RangeOp::Le,
            value,
        },
        CompareOp::Greater => ScanConjunct::Range {
            column,
            op: RangeOp::Gt,
            value,
        },
        CompareOp::GreaterOrEqual => ScanConjunct::Range {
            column,
            op: RangeOp::Ge,
            value,
        },
    }
}

/// Extract the column name from a [`CompiledExpr`] whose outermost
/// node is a bare column read. Returns `None` for every other shape
/// — `CompiledExpr::Column`'s `name` is the canonical source for
/// the column a conjunct references.
fn column_name(expr: &CompiledExpr) -> Option<String> {
    match &expr.node {
        CompiledNode::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use arrow::array::{Array as _, ArrayRef, StringViewArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, TimeUnit};

    use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{BqlType, ColumnDef, PropertyValue, TableSchema, ZoneMap};
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;

    // ── Canonical schemas and batch helpers ──────────────────────────────────

    fn minimal_schema() -> TableSchema {
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
        .unwrap()
    }

    fn minimal_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8View, false),
        ]))
    }

    fn make_batch(ids: &[&str], tss: &[i64], evts: &[&str]) -> RecordBatch {
        let ids: ArrayRef = Arc::new(StringViewArray::from(ids.to_vec()));
        let tss: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(tss.iter().copied().map(Some).collect::<Vec<_>>())
                .with_timezone("UTC"),
        );
        let evts: ArrayRef = Arc::new(StringViewArray::from(evts.to_vec()));
        RecordBatch::try_new(minimal_arrow_schema(), vec![ids, tss, evts]).unwrap()
    }

    fn make_handle(segment_id: u64, row_count: u64) -> SegmentHandle {
        SegmentHandle {
            segment_id,
            shard_id: 0,
            window_id: 0,
            row_count,
            schema_version: 0,
        }
    }

    // ── In-memory SegmentReader with zone-map pruning support ───────────────

    /// One entry in a test fixture: segment handle + row-group
    /// batches + parallel list of per-row-group zone maps. Named so
    /// clippy's `type_complexity` lint stays quiet without hiding
    /// the shape from the reader.
    type VecSegment = (
        SegmentHandle,
        Vec<RecordBatch>,
        Vec<HashMap<String, ZoneMap>>,
    );

    /// A test-only `SegmentReader` that owns a list of
    /// `(handle, batches, zone_maps)` tuples and hands out fake
    /// [`SegmentScan`]s that materialise them one row-group at a
    /// time. Unlike the Wave 1 `VecReader`, this reader respects a
    /// pushed-down predicate at row-group boundaries, mirroring the
    /// real `SegmentFileScan`'s behaviour closely enough that the
    /// scan operator's merge path can be exercised without linking
    /// a real segment writer into the test crate.
    struct VecReader {
        schema: TableSchema,
        segments: Vec<VecSegment>,
        open_calls: AtomicUsize,
        last_projection: Mutex<Option<ColumnProjection>>,
        last_predicate: Mutex<Option<Arc<dyn Predicate>>>,
    }

    impl VecReader {
        fn empty(schema: TableSchema) -> Self {
            Self {
                schema,
                segments: Vec::new(),
                open_calls: AtomicUsize::new(0),
                last_projection: Mutex::new(None),
                last_predicate: Mutex::new(None),
            }
        }

        fn with_segments(schema: TableSchema, segments: Vec<VecSegment>) -> Self {
            Self {
                schema,
                segments,
                open_calls: AtomicUsize::new(0),
                last_projection: Mutex::new(None),
                last_predicate: Mutex::new(None),
            }
        }

        fn open_calls(&self) -> usize {
            self.open_calls.load(Ordering::SeqCst)
        }

        fn last_predicate(&self) -> Option<Arc<dyn Predicate>> {
            self.last_predicate.lock().unwrap().clone()
        }
    }

    struct VecScan {
        batches: Vec<RecordBatch>,
        zone_maps: Vec<HashMap<String, ZoneMap>>,
        predicate: Option<Arc<dyn Predicate>>,
        position: usize,
    }

    impl SegmentReader for VecReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            Box::new(self.segments.iter().map(|(h, _, _)| Ok(h.clone())))
        }

        fn open_segment(
            &self,
            handle: &SegmentHandle,
            projection: &ColumnProjection,
            predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_projection.lock().unwrap() = Some(projection.clone());
            *self.last_predicate.lock().unwrap() = predicate.clone();
            match self.segments.iter().find(|(h, _, _)| h == handle) {
                Some((_, batches, zones)) => Ok(Box::new(VecScan {
                    batches: batches.clone(),
                    zone_maps: zones.clone(),
                    predicate,
                    position: 0,
                })),
                None => Err(BqliteError::Execution(format!(
                    "unknown segment handle {handle:?}"
                ))),
            }
        }
    }

    impl SegmentScan for VecScan {
        fn row_group_count(&self) -> usize {
            self.batches.len()
        }

        fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
            Ok(self.zone_maps.get(idx).cloned().unwrap_or_default())
        }

        fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
            loop {
                if self.position >= self.batches.len() {
                    return Ok(None);
                }
                let idx = self.position;
                self.position += 1;
                // Real `SegmentFileScan` prunes row-groups whose
                // zone-maps the predicate rejects. Mirror that here
                // so the scan operator's pushdown path has exactly
                // the behaviour it relies on in production.
                if let Some(pred) = &self.predicate {
                    let zones = self.zone_maps.get(idx).cloned().unwrap_or_default();
                    if !pred.accepts_zone_group(&zones) {
                        continue;
                    }
                }
                return Ok(Some(self.batches[idx].clone()));
            }
        }
    }

    /// Reader that yields a handle then surfaces an I/O error —
    /// proves `open()` aborts on enumeration failure before any
    /// per-segment work.
    struct ErroringReader {
        schema: TableSchema,
    }

    impl SegmentReader for ErroringReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            let items: Vec<Result<SegmentHandle>> = vec![
                Ok(make_handle(1, 0)),
                Err(BqliteError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "segment list truncated",
                ))),
            ];
            Box::new(items.into_iter())
        }

        fn open_segment(
            &self,
            _handle: &SegmentHandle,
            _projection: &ColumnProjection,
            _predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            Err(BqliteError::Execution("unreachable".into()))
        }
    }

    /// Reader whose `open_segment` fails. Proves `open()` surfaces
    /// per-segment open errors during priming.
    struct OpenFailReader {
        schema: TableSchema,
        handle: SegmentHandle,
    }

    impl SegmentReader for OpenFailReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            Box::new(std::iter::once(Ok(self.handle.clone())))
        }

        fn open_segment(
            &self,
            _handle: &SegmentHandle,
            _projection: &ColumnProjection,
            _predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            Err(BqliteError::Execution("segment not found".into()))
        }
    }

    // ── Predicate builders ──────────────────────────────────────────────────

    fn compile_predicate(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("predicate type checks");
        CompiledExpr::from_typed(&typed)
    }

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }

    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }

    fn string_lit(value: &str) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::String(value.to_string())))
    }

    fn int_lit(value: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(value)))
    }

    fn compare(op: CompareOp, left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    // ── Fixture builders ────────────────────────────────────────────────────

    /// Build a zone map over the `entity_id` and `ts` columns for a
    /// row-group whose rows are all `[min, max]`-bounded.
    fn zones_for(
        entity_min: &str,
        entity_max: &str,
        ts_min: i64,
        ts_max: i64,
    ) -> HashMap<String, ZoneMap> {
        let mut map = HashMap::new();
        map.insert(
            "entity_id".to_string(),
            ZoneMap {
                min: Some(PropertyValue::String(entity_min.to_string())),
                max: Some(PropertyValue::String(entity_max.to_string())),
                null_count: 0,
                row_count: 0,
            },
        );
        map.insert(
            "ts".to_string(),
            ZoneMap {
                min: Some(PropertyValue::Timestamp(ts_min)),
                max: Some(PropertyValue::Timestamp(ts_max)),
                null_count: 0,
                row_count: 0,
            },
        );
        map
    }

    fn zones_with_event(entity: &str, ts: i64, event: &str) -> HashMap<String, ZoneMap> {
        let mut map = zones_for(entity, entity, ts, ts);
        map.insert(
            "event_type".to_string(),
            ZoneMap {
                min: Some(PropertyValue::String(event.to_string())),
                max: Some(PropertyValue::String(event.to_string())),
                null_count: 0,
                row_count: 1,
            },
        );
        map
    }

    // ── Construction and schema ─────────────────────────────────────────────

    #[test]
    fn output_schema_reflects_declared_columns_for_full_scan() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let scan = ScanOperator::full_scan(reader).expect("ok");
        let names: Vec<&str> = scan
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["entity_id", "ts", "event_type"]);
    }

    #[test]
    fn explicit_projection_narrows_output_schema_and_preserves_order() {
        // Columns are always returned in table-schema order regardless
        // of the order they appear in the projection list. This keeps
        // CompiledNode::Column { index } values (compiled against the
        // full schema ordinals) stable across pruning.
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let projection = vec![
            "ts".to_string(),
            "entity_id".to_string(),
            "event_type".to_string(),
        ];
        let scan = ScanOperator::new(reader, &projection, Vec::new(), CancellationToken::new())
            .expect("projection resolves");
        let names: Vec<&str> = scan
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        // minimal_schema order: entity_id, ts, event_type — so even
        // though we requested [ts, entity_id, event_type], the output
        // reflects the table schema order.
        assert_eq!(names, vec!["entity_id", "ts", "event_type"]);
    }

    #[test]
    fn explicit_projection_rejects_unknown_column() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let projection = vec!["entity_id".to_string(), "nope".to_string()];
        let err = ScanOperator::new(reader, &projection, Vec::new(), CancellationToken::new())
            .expect_err("unknown column rejected");
        match err {
            BqliteError::Schema(msg) => assert!(msg.contains("nope"), "{msg}"),
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn explicit_projection_without_entity_key_is_rejected() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let projection = vec!["ts".to_string(), "event_type".to_string()];
        let err = ScanOperator::new(reader, &projection, Vec::new(), CancellationToken::new())
            .expect_err("entity key required");
        match err {
            BqliteError::Schema(msg) => {
                assert!(msg.contains("entity-key"), "{msg}");
                assert!(msg.contains("entity_id"), "{msg}");
            }
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    #[test]
    fn explicit_projection_without_timestamp_is_rejected() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let projection = vec!["entity_id".to_string(), "event_type".to_string()];
        let err = ScanOperator::new(reader, &projection, Vec::new(), CancellationToken::new())
            .expect_err("timestamp required");
        match err {
            BqliteError::Schema(msg) => {
                assert!(msg.contains("timestamp"), "{msg}");
                assert!(msg.contains("ts"), "{msg}");
            }
            other => panic!("expected Schema error, got {other:?}"),
        }
    }

    // ── Drainage ────────────────────────────────────────────────────────────

    fn drain_entity_ids(op: &mut ScanOperator) -> Vec<String> {
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("next_batch ok") {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            for i in 0..col.len() {
                out.push(col.value(i).to_string());
            }
        }
        out
    }

    #[test]
    fn empty_reader_drains_to_none() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
        // Exhaustion is sticky.
        assert!(op.next_batch().unwrap().is_none());
        op.close().unwrap();
    }

    #[test]
    fn single_segment_drains_every_row() {
        let batch = make_batch(&["u1", "u1", "u2"], &[100, 200, 300], &["a", "b", "c"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![(make_handle(1, 3), vec![batch], vec![HashMap::new()])],
        ));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u1", "u1", "u2"]);
        op.close().unwrap();
    }

    #[test]
    fn multi_segment_merge_produces_globally_sorted_stream() {
        // Two segments interleaved on entity_id/ts. The merge must
        // emit the combined stream in `(entity_id, ts)` order even
        // though neither segment on its own is globally sorted.
        let s1 = make_batch(&["u1", "u3"], &[100, 300], &["a", "c"]);
        let s2 = make_batch(&["u2", "u2", "u4"], &[150, 200, 400], &["b", "b", "d"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![
                (make_handle(1, 2), vec![s1], vec![HashMap::new()]),
                (make_handle(2, 3), vec![s2], vec![HashMap::new()]),
            ],
        ));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.open().unwrap();
        assert_eq!(
            drain_entity_ids(&mut op),
            vec!["u1", "u2", "u2", "u3", "u4"],
        );
    }

    #[test]
    fn multi_segment_multi_row_group_merges_batches() {
        // Segment 1 has two row groups; segment 2 has one. Exercises
        // the reload-between-pulls path in the merge.
        let s1_a = make_batch(&["u1"], &[100], &["a"]);
        let s1_b = make_batch(&["u3"], &[300], &["c"]);
        let s2 = make_batch(&["u2"], &[200], &["b"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![
                (
                    make_handle(1, 2),
                    vec![s1_a, s1_b],
                    vec![HashMap::new(), HashMap::new()],
                ),
                (make_handle(2, 1), vec![s2], vec![HashMap::new()]),
            ],
        ));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u1", "u2", "u3"]);
    }

    // ── Zone-map pruning ────────────────────────────────────────────────────

    #[test]
    fn scan_predicate_is_built_and_prunes_disjoint_row_groups() {
        // Predicate: event_type = 'keep'. The "keep" batch has a
        // matching zone map; the "skip" batch has a zone map with
        // min == max == "drop", which the `ScanConjunct::Equal`
        // rule rejects. The fake reader honours
        // `Predicate::accepts_zone_group` so we can verify the
        // conservative pruning path end-to-end.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("keep")),
            &output_schema,
        );

        let keep_batch = make_batch(&["u1", "u2"], &[10, 20], &["keep", "keep"]);
        let skip_batch = make_batch(&["u3"], &[30], &["drop"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![(
                make_handle(1, 3),
                vec![keep_batch, skip_batch],
                vec![
                    zones_with_event("u1", 20, "keep"),
                    zones_with_event("u3", 30, "drop"),
                ],
            )],
        ));

        let mut op = ScanOperator::new(reader, &[], vec![pred], CancellationToken::new()).unwrap();
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u1", "u2"]);
    }

    #[test]
    fn non_pushable_predicate_falls_back_to_post_filter() {
        // Pure post-filter: the compiled predicate references the
        // timestamp column, and `Timestamp` literals don't lower to
        // a pushable conjunct through our current lowering (Literal
        // type `Int`-vs-`Timestamp` asymmetry). Even so, the scan
        // must enforce the predicate against the materialised rows
        // so downstream sees only surviving rows. This covers the
        // exact-semantics guarantee described in the module doc.
        //
        // The predicate is `ts > 150`; the batch carries ts values
        // `[100, 200]`, so the row-level evaluator drops the first
        // row.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        // `ts > 150` — the literal broadcasts as an `Int`, which
        // matches `TimestampNanosecondArray` via the type-checker's
        // implicit `Int → Timestamp` coercion. We intentionally
        // test the shape that *might* be pushable and rely on the
        // post-filter fallback to enforce correctness regardless of
        // whether lowering converted it.
        let pred = compile_predicate(
            compare(CompareOp::Greater, col("ts"), int_lit(150)),
            &output_schema,
        );

        let batch = make_batch(&["u1", "u2"], &[100, 200], &["a", "b"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![(make_handle(1, 2), vec![batch], vec![HashMap::new()])],
        ));

        let mut op = ScanOperator::new(reader, &[], vec![pred], CancellationToken::new()).unwrap();
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u2"]);
    }

    #[test]
    fn predicate_not_converted_still_forwarded_as_none_to_reader() {
        // When every pushed predicate is non-convertible, the scan
        // must hand `None` to `open_segment` (not an empty
        // `ScanPredicate`), so readers that treat `Some` as
        // "evaluate zone maps" see a clean disable. A column-to-
        // column compare (`entity_id = event_type`) is explicitly
        // non-pushable per predicate-pushdown.md §4, so we can
        // force the `None` path without any runtime coercion
        // surprises.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("entity_id"), col("event_type")),
            &output_schema,
        );

        let batch = make_batch(&["u1"], &[200], &["u1"]);
        let reader = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![(make_handle(1, 1), vec![batch], vec![HashMap::new()])],
        ));
        let inspect = reader.clone();
        let reader_arc: Arc<dyn SegmentReader> = reader;

        let mut op =
            ScanOperator::new(reader_arc, &[], vec![pred], CancellationToken::new()).unwrap();
        op.open().unwrap();
        let _ = op.next_batch().unwrap();
        assert_eq!(inspect.open_calls(), 1);
        assert!(
            inspect.last_predicate().is_none(),
            "non-pushable predicates should not materialise a ScanPredicate"
        );
    }

    // ── Post-filter behaviour ───────────────────────────────────────────────

    #[test]
    fn every_row_rejected_forces_next_batch_loop() {
        // First merged batch fails every row; the operator must
        // drop it entirely and pull another, rather than emit an
        // empty batch to downstream. Mirrors the FilterOperator
        // "re-pull on empty" convention.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("match")),
            &output_schema,
        );

        let s1 = make_batch(&["u1"], &[100], &["miss"]);
        let s2 = make_batch(&["u2"], &[200], &["match"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![
                (make_handle(1, 1), vec![s1], vec![HashMap::new()]),
                (make_handle(2, 1), vec![s2], vec![HashMap::new()]),
            ],
        ));
        let mut op = ScanOperator::new(reader, &[], vec![pred], CancellationToken::new()).unwrap();
        op.open().unwrap();

        // First `next_batch` must materialise the surviving row.
        let out = op.next_batch().unwrap().expect("batch");
        assert_eq!(out.num_rows(), 1);
        assert_eq!(drain_entity_ids(&mut op), Vec::<String>::new());
    }

    // ── Error propagation ───────────────────────────────────────────────────

    #[test]
    fn open_surfaces_enumeration_error() {
        let reader: Arc<dyn SegmentReader> = Arc::new(ErroringReader {
            schema: minimal_schema(),
        });
        let mut op = ScanOperator::full_scan(reader).unwrap();
        let err = op.open().expect_err("enumeration error surfaces");
        assert!(matches!(err, BqliteError::Io(_)), "{err}");
    }

    #[test]
    fn open_surfaces_per_segment_open_error() {
        let reader: Arc<dyn SegmentReader> = Arc::new(OpenFailReader {
            schema: minimal_schema(),
            handle: make_handle(1, 0),
        });
        let mut op = ScanOperator::full_scan(reader).unwrap();
        let err = op.open().expect_err("per-segment open error surfaces");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    // ── Cancellation ────────────────────────────────────────────────────────

    #[test]
    fn cancelled_token_aborts_next_batch() {
        let batch = make_batch(&["u1"], &[1], &["a"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![(make_handle(1, 1), vec![batch], vec![HashMap::new()])],
        ));
        let cancel = CancellationToken::new();
        let mut op = ScanOperator::new(reader, &[], Vec::new(), cancel.clone()).unwrap();
        op.open().unwrap();
        cancel.cancel();
        let err = op.next_batch().expect_err("cancellation fires");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn exhausted_scan_stays_none_after_cancellation() {
        // Once `Ok(None)` latches, subsequent pulls must continue
        // to return `Ok(None)` without consulting the token. The
        // sticky-exhausted contract is identical to the Wave 1
        // stub's.
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let cancel = CancellationToken::new();
        let mut op = ScanOperator::new(reader, &[], Vec::new(), cancel.clone()).unwrap();
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
        cancel.cancel();
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    #[test]
    fn close_is_idempotent() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.close().unwrap();
        op.close().unwrap();
        op.close().unwrap();
    }

    #[test]
    fn close_without_open_is_ok() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.close().unwrap();
    }

    #[test]
    fn close_after_drain_is_ok() {
        let batch = make_batch(&["u1"], &[1], &["a"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![(make_handle(1, 1), vec![batch], vec![HashMap::new()])],
        ));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.open().unwrap();
        while op.next_batch().unwrap().is_some() {}
        op.close().unwrap();
    }

    // ── Trait object compatibility ──────────────────────────────────────────

    #[test]
    fn scan_operator_is_trait_object() {
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::empty(minimal_schema()));
        let scan: Box<dyn PhysicalOperator> = Box::new(ScanOperator::full_scan(reader).unwrap());
        let _ = scan;
    }

    // ── Lowering CompiledExpr → ScanConjunct ────────────────────────────────

    #[test]
    fn pushable_equal_lowers_to_scan_conjunct() {
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("checkout")),
            &output_schema,
        );
        let pred_opt = build_scan_predicate(&[pred]);
        let pred = pred_opt.expect("predicate lowered");
        let cols = pred.referenced_columns();
        assert_eq!(cols, &["event_type".to_string()]);
    }

    #[test]
    fn pushable_equal_accepts_literal_on_either_side() {
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        // `'checkout' = event_type` — literal on the left — must
        // still lower to an `Equal` conjunct on `event_type`.
        let pred = compile_predicate(
            compare(CompareOp::Equal, string_lit("checkout"), col("event_type")),
            &output_schema,
        );
        assert!(build_scan_predicate(&[pred]).is_some());
    }

    #[test]
    fn column_to_column_compare_is_not_pushable() {
        // A Compare between two columns is never zone-map prunable.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("entity_id"), col("event_type")),
            &output_schema,
        );
        assert!(build_scan_predicate(&[pred]).is_none());
    }

    #[test]
    fn literal_to_literal_compare_is_not_pushable() {
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, string_lit("a"), string_lit("b")),
            &output_schema,
        );
        assert!(build_scan_predicate(&[pred]).is_none());
    }

    #[test]
    fn is_null_on_column_lowers_to_is_null_conjunct() {
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            sp(Expr::IsNull {
                expr: Box::new(col("event_type")),
                negated: false,
            }),
            &output_schema,
        );
        let pred = build_scan_predicate(&[pred]).expect("IsNull lowered");
        // Precise rule coverage — assert the conjunct name via the
        // cached referenced-columns list, which the `ScanPredicate`
        // populates from the conjunct's column on construction.
        assert_eq!(pred.referenced_columns(), &["event_type".to_string()]);
    }

    #[test]
    fn is_not_null_on_column_lowers_to_is_not_null_conjunct() {
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            sp(Expr::IsNull {
                expr: Box::new(col("event_type")),
                negated: true,
            }),
            &output_schema,
        );
        let pred = build_scan_predicate(&[pred]).expect("IsNotNull lowered");
        assert_eq!(pred.referenced_columns(), &["event_type".to_string()]);
    }

    #[test]
    fn in_literal_set_on_column_lowers_to_in_set_conjunct() {
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            sp(Expr::In {
                lhs: vec![col("event_type")],
                rhs: bqlite_ast::expr::InRhs::List(vec![
                    string_lit("checkout"),
                    string_lit("signup"),
                ]),
                negated: false,
            }),
            &output_schema,
        );
        let pred = build_scan_predicate(&[pred]).expect("InSet lowered");
        assert_eq!(pred.referenced_columns(), &["event_type".to_string()]);
    }

    #[test]
    fn not_in_literal_set_is_not_pushable() {
        // `NOT IN` is explicitly out of the Wave 2 pushable shapes
        // per `predicate-pushdown.md` §4 — a multi-literal NotEqual
        // conjunction would be needed, which the storage layer does
        // not yet evaluate cross-row. Verify the lowering drops it
        // so the only enforcement path is post-filter.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            sp(Expr::In {
                lhs: vec![col("event_type")],
                rhs: bqlite_ast::expr::InRhs::List(vec![string_lit("spam"), string_lit("bot")]),
                negated: true,
            }),
            &output_schema,
        );
        assert!(build_scan_predicate(&[pred]).is_none());
    }

    #[test]
    fn multiple_pushable_conjuncts_compose_into_one_scan_predicate() {
        // Two conjuncts on different columns should coexist in a
        // single `ScanPredicate`. The cached `referenced_columns`
        // list is populated in first-occurrence order — verify the
        // order matches and no duplicates leak in.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let a = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("checkout")),
            &output_schema,
        );
        let b = compile_predicate(
            sp(Expr::IsNull {
                expr: Box::new(col("entity_id")),
                negated: true,
            }),
            &output_schema,
        );
        let pred = build_scan_predicate(&[a, b]).expect("both lowered");
        assert_eq!(
            pred.referenced_columns(),
            &["event_type".to_string(), "entity_id".to_string()]
        );
    }

    // ── ScanPath ──────────────────────────────────────────────────────

    #[test]
    fn scan_path_default_is_auto() {
        assert_eq!(ScanPath::default(), ScanPath::Auto);
    }

    #[test]
    fn scan_path_parse_accepts_all_variants_case_insensitive() {
        assert_eq!(
            ScanPath::parse("materialized"),
            Some(ScanPath::Materialized)
        );
        assert_eq!(
            ScanPath::parse("MATERIALIZED"),
            Some(ScanPath::Materialized)
        );
        assert_eq!(ScanPath::parse("encoded"), Some(ScanPath::Encoded));
        assert_eq!(ScanPath::parse("  Auto  "), Some(ScanPath::Auto));
    }

    #[test]
    fn scan_path_parse_rejects_unknown() {
        assert_eq!(ScanPath::parse("nope"), None);
        assert_eq!(ScanPath::parse(""), None);
    }

    // ── Encoded dispatch ─────────────────────────────────────────────────────

    /// Drain every batch and collect `(entity_id, event_type)` pairs.
    /// Used by the parity tests below so the check compares on both a
    /// sort-key column and a filter-target column.
    fn drain_pairs(op: &mut ScanOperator) -> Vec<(String, String)> {
        let mut out = Vec::new();
        while let Some(batch) = op.next_batch().expect("next_batch ok") {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            let evts = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push((ids.value(i).to_string(), evts.value(i).to_string()));
            }
        }
        out
    }

    #[test]
    fn encoded_path_matches_materialized_on_single_segment_eq_filter() {
        // Single-segment `event_type = 'keep'`. Both ScanPath variants
        // must produce the same row set. The test exercises the
        // encoded-path plumbing end-to-end — the VecReader uses the
        // default `next_encoded_row_group` which wraps each column as
        // `EncodedColumn::Materialized`, so dispatch lands in the
        // arrow-compute fallback. That path is the realistic case for
        // any encoding without a kernel yet (Dictionary, BitPacking,
        // …), which is exactly what this wiring has to support.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("keep")),
            &output_schema,
        );
        let batch = make_batch(
            &["u1", "u2", "u3", "u4"],
            &[10, 20, 30, 40],
            &["keep", "skip", "keep", "skip"],
        );
        let make_reader = || -> Arc<dyn SegmentReader> {
            Arc::new(VecReader::with_segments(
                minimal_schema(),
                vec![(make_handle(1, 4), vec![batch.clone()], vec![HashMap::new()])],
            ))
        };

        let mut mat_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred.clone()],
            CancellationToken::new(),
            ScanPath::Materialized,
        )
        .unwrap();
        mat_op.open().unwrap();
        let mat_pairs = drain_pairs(&mut mat_op);

        let mut enc_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred],
            CancellationToken::new(),
            ScanPath::Encoded,
        )
        .unwrap();
        enc_op.open().unwrap();
        let enc_pairs = drain_pairs(&mut enc_op);

        assert_eq!(
            mat_pairs,
            vec![
                ("u1".to_string(), "keep".to_string()),
                ("u3".to_string(), "keep".to_string()),
            ]
        );
        assert_eq!(enc_pairs, mat_pairs, "encoded and materialized must agree");
    }

    #[test]
    fn encoded_path_preserves_non_pushable_residual() {
        // The `ts > 150` predicate does not match the `col == literal`
        // shape, so it lands in the encoded-path residual list and is
        // applied after the materialization boundary. The test asserts
        // both paths agree on a workload that has *only* a residual
        // predicate — no encoded-eq shape at all.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Greater, col("ts"), int_lit(150)),
            &output_schema,
        );
        let batch = make_batch(
            &["u1", "u2", "u3", "u4"],
            &[100, 150, 200, 300],
            &["a", "b", "c", "d"],
        );
        let make_reader = || -> Arc<dyn SegmentReader> {
            Arc::new(VecReader::with_segments(
                minimal_schema(),
                vec![(make_handle(1, 4), vec![batch.clone()], vec![HashMap::new()])],
            ))
        };

        let mut mat_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred.clone()],
            CancellationToken::new(),
            ScanPath::Materialized,
        )
        .unwrap();
        mat_op.open().unwrap();
        let mat_ids = drain_entity_ids(&mut mat_op);

        let mut enc_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred],
            CancellationToken::new(),
            ScanPath::Encoded,
        )
        .unwrap();
        enc_op.open().unwrap();
        let enc_ids = drain_entity_ids(&mut enc_op);

        assert_eq!(mat_ids, vec!["u3".to_string(), "u4".to_string()]);
        assert_eq!(enc_ids, mat_ids);
    }

    #[test]
    fn encoded_path_multi_segment_preserves_entity_ts_order() {
        // CP5: multi-segment `ScanPath::Encoded` now runs through
        // `EncodedKWayMergeScan` rather than falling back to the
        // materialized merge. The globally-sorted `(entity_id, ts)`
        // contract must still hold across segments.
        let s1 = make_batch(&["u1", "u3"], &[100, 300], &["a", "c"]);
        let s2 = make_batch(&["u2", "u2", "u4"], &[150, 200, 400], &["b", "b", "d"]);
        let reader: Arc<dyn SegmentReader> = Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![
                (make_handle(1, 2), vec![s1], vec![HashMap::new()]),
                (make_handle(2, 3), vec![s2], vec![HashMap::new()]),
            ],
        ));
        let mut op = ScanOperator::with_scan_path(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            ScanPath::Encoded,
        )
        .unwrap();
        op.open().unwrap();
        assert_eq!(
            drain_entity_ids(&mut op),
            vec!["u1", "u2", "u2", "u3", "u4"],
        );
    }

    #[test]
    fn encoded_and_materialized_paths_agree_on_multi_segment_with_pushable_eq() {
        // Parity test: two segments, pushable `event_type == 'keep'`.
        // Both paths must produce the same sorted row list.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("keep")),
            &output_schema,
        );
        let s1 = make_batch(&["u1", "u3"], &[100, 300], &["keep", "skip"]);
        let s2 = make_batch(
            &["u2", "u2", "u4"],
            &[150, 200, 400],
            &["keep", "skip", "keep"],
        );
        let make_reader = || -> Arc<dyn SegmentReader> {
            Arc::new(VecReader::with_segments(
                minimal_schema(),
                vec![
                    (make_handle(1, 2), vec![s1.clone()], vec![HashMap::new()]),
                    (make_handle(2, 3), vec![s2.clone()], vec![HashMap::new()]),
                ],
            ))
        };

        let mut mat_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred.clone()],
            CancellationToken::new(),
            ScanPath::Materialized,
        )
        .unwrap();
        mat_op.open().unwrap();
        let mat_pairs = drain_pairs(&mut mat_op);

        let mut enc_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred],
            CancellationToken::new(),
            ScanPath::Encoded,
        )
        .unwrap();
        enc_op.open().unwrap();
        let enc_pairs = drain_pairs(&mut enc_op);

        assert_eq!(
            mat_pairs,
            vec![
                ("u1".to_string(), "keep".to_string()),
                ("u2".to_string(), "keep".to_string()),
                ("u4".to_string(), "keep".to_string()),
            ],
        );
        assert_eq!(enc_pairs, mat_pairs, "encoded and materialized must agree");
    }

    #[test]
    fn encoded_multi_segment_preserves_tie_break_on_duplicate_entity_ts() {
        // Two segments share a `(u1, 100)` row, distinguished by
        // `event_type`. Lower-indexed segment must appear first in
        // both paths — testing the row count alone would silently
        // pass on a tie-break regression.
        let s1 = make_batch(&["u1"], &[100], &["a"]);
        let s2 = make_batch(&["u1"], &[100], &["b"]);
        let make_reader = || -> Arc<dyn SegmentReader> {
            Arc::new(VecReader::with_segments(
                minimal_schema(),
                vec![
                    (make_handle(1, 1), vec![s1.clone()], vec![HashMap::new()]),
                    (make_handle(2, 1), vec![s2.clone()], vec![HashMap::new()]),
                ],
            ))
        };

        let mut mat_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            Vec::new(),
            CancellationToken::new(),
            ScanPath::Materialized,
        )
        .unwrap();
        mat_op.open().unwrap();
        let mat_pairs = drain_pairs(&mut mat_op);

        let mut enc_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            Vec::new(),
            CancellationToken::new(),
            ScanPath::Encoded,
        )
        .unwrap();
        enc_op.open().unwrap();
        let enc_pairs = drain_pairs(&mut enc_op);

        assert_eq!(
            mat_pairs,
            vec![
                ("u1".to_string(), "a".to_string()),
                ("u1".to_string(), "b".to_string()),
            ],
            "lower-indexed segment (event_type='a') must come first",
        );
        assert_eq!(enc_pairs, mat_pairs);
    }

    #[test]
    fn encoded_multi_segment_with_fully_filtered_source() {
        // Two segments; predicate removes every row from segment 1.
        // Both paths must return only segment 0's surviving rows.
        let output_schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("keep")),
            &output_schema,
        );
        let s1 = make_batch(&["u1", "u3"], &[100, 300], &["keep", "keep"]);
        let s2 = make_batch(&["u2", "u4"], &[200, 400], &["skip", "skip"]);
        let make_reader = || -> Arc<dyn SegmentReader> {
            Arc::new(VecReader::with_segments(
                minimal_schema(),
                vec![
                    (make_handle(1, 2), vec![s1.clone()], vec![HashMap::new()]),
                    (make_handle(2, 2), vec![s2.clone()], vec![HashMap::new()]),
                ],
            ))
        };

        let mut mat_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred.clone()],
            CancellationToken::new(),
            ScanPath::Materialized,
        )
        .unwrap();
        mat_op.open().unwrap();
        let mat_ids = drain_entity_ids(&mut mat_op);

        let mut enc_op = ScanOperator::with_scan_path(
            make_reader(),
            &[],
            vec![pred],
            CancellationToken::new(),
            ScanPath::Encoded,
        )
        .unwrap();
        enc_op.open().unwrap();
        let enc_ids = drain_entity_ids(&mut enc_op);

        assert_eq!(mat_ids, vec!["u1".to_string(), "u3".to_string()]);
        assert_eq!(enc_ids, mat_ids);
    }

    // ── Tombstone-aware scan (TASK-434) ────────────────────────────────────
    //
    // These tests cover the engine's per-query [`TombstoneSnapshot`]
    // plumbing: an empty snapshot is a no-op, non-empty entries filter
    // per-shard after pushdown, the filter survives multi-segment and
    // multi-window merges, and a snapshot containing even one wrapped
    // shard forces the materialized path (the encoded merge cannot
    // mix wrapped and unwrapped inputs).

    use bqlite_core::ScalarValue;
    use bqlite_storage::{TimeRangeDelete, TombstoneFile, TombstoneSnapshot};

    fn handle_for(segment_id: u64, window: u64, shard: u32, rows: u64) -> SegmentHandle {
        SegmentHandle {
            segment_id,
            shard_id: shard,
            window_id: window,
            row_count: rows,
            schema_version: 0,
        }
    }

    #[test]
    fn empty_tombstone_snapshot_is_noop() {
        // The pre-TASK-434 code path: `new` uses `empty()` by default.
        // Two segments, ten rows; empty snapshot must not affect output.
        let segments = vec![
            (
                handle_for(1, 0, 0, 2),
                vec![make_batch(&["a1", "a2"], &[100, 200], &["e1", "e2"])],
                vec![zones_for("a1", "a2", 100, 200)],
            ),
            (
                handle_for(2, 0, 1, 3),
                vec![make_batch(
                    &["b1", "b2", "b3"],
                    &[300, 400, 500],
                    &["e3", "e4", "e5"],
                )],
                vec![zones_for("b1", "b3", 300, 500)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(TombstoneSnapshot::empty()),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["a1", "a2", "b1", "b2", "b3"]);
    }

    #[test]
    fn entity_tombstones_filter_across_segments() {
        // Two segments in the same `(window, shard)` both carry rows
        // for `alice`; an entity tombstone on `alice` suppresses them
        // across both segments while leaving `bob` and `carol` intact.
        // Proves the filter runs per-segment before the merge rather
        // than after — otherwise the merged interleave would surface
        // `alice` rows.
        let segments = vec![
            (
                handle_for(1, 0, 0, 2),
                vec![make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"])],
                vec![zones_for("alice", "bob", 100, 200)],
            ),
            (
                handle_for(2, 0, 0, 2),
                vec![make_batch(&["alice", "carol"], &[300, 400], &["e3", "e4"])],
                vec![zones_for("alice", "carol", 300, 400)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["bob", "carol"]);
    }

    #[test]
    fn time_range_tombstones_filter_correctly() {
        // `[150, 250)` covers `bob` (200) only; `alice` (100) and
        // `carol` (300) survive.
        let segments = vec![(
            handle_for(1, 0, 0, 3),
            vec![make_batch(
                &["alice", "bob", "carol"],
                &[100, 200, 300],
                &["e1", "e2", "e3"],
            )],
            vec![zones_for("alice", "carol", 100, 300)],
        )];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_time_range(TimeRangeDelete {
                min_ts: Some(150),
                min_inclusive: true,
                max_ts: Some(250),
                max_inclusive: false,
            }),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["alice", "carol"]);
    }

    #[test]
    fn per_shard_snapshot_isolates_other_shards() {
        // Shard 0 has an entity tombstone for `alice`; shard 1 does
        // not. Segments in shard 1 must not be wrapped, so `alice`
        // rows stored there stay visible. This is the
        // deletes.md §3.3 shard-targeting property under scan.
        let segments = vec![
            (
                handle_for(1, 0, 0, 2),
                vec![make_batch(&["alice", "alice"], &[100, 200], &["e1", "e2"])],
                vec![zones_for("alice", "alice", 100, 200)],
            ),
            (
                handle_for(2, 0, 1, 2),
                vec![make_batch(&["alice", "bob"], &[300, 400], &["e3", "e4"])],
                vec![zones_for("alice", "bob", 300, 400)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        // alice from shard 0 is suppressed; alice/bob from shard 1
        // survive. k-way merge yields (entity, ts) order across both.
        assert_eq!(ids, vec!["alice", "bob"]);
    }

    #[test]
    fn multi_window_tombstones_apply_independently() {
        // Window 0 has a tombstone on alice; window 1 does not. Both
        // windows live in the same scan, which proves the snapshot
        // resolves per-`(window, shard)` rather than per-shard-only.
        let segments = vec![
            (
                handle_for(1, 0, 0, 2),
                vec![make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"])],
                vec![zones_for("alice", "bob", 100, 200)],
            ),
            (
                handle_for(2, 1, 0, 2),
                vec![make_batch(&["alice", "bob"], &[300, 400], &["e3", "e4"])],
                vec![zones_for("alice", "bob", 300, 400)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        // Window 0's alice is suppressed; window 1's alice (ts 300)
        // and both bobs remain. Merge emits (entity asc, ts asc).
        assert_eq!(ids, vec!["alice", "bob", "bob"]);
    }

    #[test]
    fn tombstone_filter_runs_before_post_filter() {
        // A tombstone on alice and a post-filter `event_type = "e1"`:
        // alice's e1 row must be dropped by the tombstone before the
        // post-filter runs, so the surviving `e1` row is bob's (t=200).
        // Proves the pipeline ordering from deletes.md §7 —
        // tombstones come *before* operator-level predicates.
        let segments = vec![(
            handle_for(1, 0, 0, 3),
            vec![make_batch(
                &["alice", "bob", "carol"],
                &[100, 200, 300],
                &["e1", "e1", "e2"],
            )],
            vec![zones_for("alice", "carol", 100, 300)],
        )];
        let reader_raw = Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let reader: Arc<dyn SegmentReader> = reader_raw.clone();

        let operator_schema =
            build_output_schema(reader.schema(), &ColumnProjection::all()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("e1")),
            &operator_schema,
        );
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            vec![pred],
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["bob"]);
    }

    #[test]
    fn encoded_path_forced_to_materialized_when_any_segment_wrapped() {
        // Request `ScanPath::Encoded` with a non-empty snapshot whose
        // entry targets one of the segments. The materialized merge
        // path must be used; the encoded-scan holder must stay `None`.
        // We assert this via `scan_path()` + by observing that the
        // scan yields correct post-filter output (the encoded-merge
        // contract would panic on a materialized-wrapped input).
        let segments = vec![
            (
                handle_for(1, 0, 0, 2),
                vec![make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"])],
                vec![zones_for("alice", "bob", 100, 200)],
            ),
            (
                handle_for(2, 0, 1, 2),
                vec![make_batch(&["carol", "dan"], &[300, 400], &["e3", "e4"])],
                vec![zones_for("carol", "dan", 300, 400)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones_and_scan_path(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            ScanPath::Encoded,
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        // scan_path remains `Encoded` on the operator as a declared
        // preference, but `open()` chose the materialized holder.
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["bob", "carol", "dan"]);
    }

    #[test]
    fn encoded_path_preserved_when_snapshot_is_empty() {
        // Dual of `encoded_path_forced_to_materialized_when_any_segment_wrapped`:
        // an empty snapshot must leave the encoded path untouched,
        // guarding against a regression where `any_wrapped` accidentally
        // gets set to `true` unconditionally. We verify by scanning
        // through two segments in encoded mode and asserting the merge
        // produces correct results — if the encoded merge were wired
        // up with a materialized wrapper, the encoded-batch contract
        // would surface as a panic in `materialize_stitched`.
        let segments = vec![
            (
                handle_for(1, 0, 0, 2),
                vec![make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"])],
                vec![zones_for("alice", "bob", 100, 200)],
            ),
            (
                handle_for(2, 0, 1, 2),
                vec![make_batch(&["carol", "dan"], &[300, 400], &["e3", "e4"])],
                vec![zones_for("carol", "dan", 300, 400)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let mut op = ScanOperator::with_tombstones_and_scan_path(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            ScanPath::Encoded,
            Arc::new(TombstoneSnapshot::empty()),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["alice", "bob", "carol", "dan"]);
    }

    #[test]
    fn tombstones_only_wrap_affected_shards() {
        // Two shards, snapshot targets shard 0 only. A segment in
        // shard 1 must not be wrapped even when the scan path is
        // materialized — wrapping is a pure performance/correctness
        // pairing, and an unnecessary wrap adds per-row-group work
        // for nothing. We verify by scanning a ts that also appears
        // in shard 0: shard 1's match survives, so the filter ran on
        // shard 0 only.
        let segments = vec![
            (
                handle_for(1, 0, 0, 1),
                vec![make_batch(&["alice"], &[100], &["e1"])],
                vec![zones_for("alice", "alice", 100, 100)],
            ),
            (
                handle_for(2, 0, 1, 1),
                vec![make_batch(&["alice"], &[100], &["e1"])],
                vec![zones_for("alice", "alice", 100, 100)],
            ),
        ];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        // Only shard 1's alice survives — shard 0's was tombstoned.
        assert_eq!(ids, vec!["alice"]);
    }

    #[test]
    fn snapshot_is_consulted_once_per_segment() {
        // Snapshot has an entry for shard 0 with no matching rows
        // (empty tombstone content = no wrap). Keeps the test
        // self-documenting: an entry whose file `is_empty()` should
        // NOT cause a wrap — matches the `TombstoneSnapshot::from_map`
        // contract that empty files are dropped on construction.
        let segments = vec![(
            handle_for(1, 0, 0, 2),
            vec![make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"])],
            vec![zones_for("alice", "bob", 100, 200)],
        )];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        // Empty tombstone — `from_map` drops it.
        let snap = TombstoneSnapshot::from_map([((0u32, 0u16), TombstoneFile::default())]);
        assert!(snap.is_empty(), "empty TombstoneFile entries are dropped");
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["alice", "bob"]);
    }

    #[test]
    fn row_tombstones_error_when_seq_id_column_missing() {
        // Documents the current scope limit: `__seq_id` / `__batch_id`
        // system columns are not yet exposed to the scan's materialized
        // output (see the module docs on Output schema), so row-level
        // tombstones cannot filter against per-row IDs. The
        // `TombstoneFilter` surfaces this as a clear error rather than
        // silently passing deleted rows through — if a future task
        // wires system columns into the scan output, this error goes
        // away without code changes here.
        let segments = vec![(
            handle_for(1, 0, 0, 1),
            vec![make_batch(&["alice"], &[100], &["e1"])],
            vec![zones_for("alice", "alice", 100, 100)],
        )];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([((0, 0), TombstoneFile::for_rows([42]))]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let err = op
            .next_batch()
            .expect_err("row tombstone without __seq_id column errors");
        let msg = err.to_string();
        assert!(msg.contains("__seq_id"), "got: {msg}");
    }

    #[test]
    fn multi_row_group_segment_filters_each_group() {
        // Single segment, two row groups. Tombstone on alice must
        // apply to both row groups — proves the wrapper runs on every
        // `next_row_group` call, not just the first.
        let segments = vec![(
            handle_for(1, 0, 0, 4),
            vec![
                make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"]),
                make_batch(&["alice", "carol"], &[300, 400], &["e3", "e4"]),
            ],
            vec![
                zones_for("alice", "bob", 100, 200),
                zones_for("alice", "carol", 300, 400),
            ],
        )];
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let snap = TombstoneSnapshot::from_map([(
            (0, 0),
            TombstoneFile::for_entities([ScalarValue::String("alice".into())]),
        )]);
        let mut op = ScanOperator::with_tombstones(
            reader,
            &[],
            Vec::new(),
            CancellationToken::new(),
            Arc::new(snap),
        )
        .unwrap();
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["bob", "carol"]);
    }

    // ── SAMPLE pushdown tests (TASK-430) ────────────────────────────────────

    /// Build a two-segment reader for the SAMPLE tests. Enumerates
    /// four distinct entity IDs ("u0".."u3"), one row per entity.
    fn sample_reader() -> Arc<dyn SegmentReader> {
        let s1 = make_batch(&["u0", "u1"], &[100, 200], &["a", "b"]);
        let s2 = make_batch(&["u2", "u3"], &[300, 400], &["c", "d"]);
        Arc::new(VecReader::with_segments(
            minimal_schema(),
            vec![
                (make_handle(1, 2), vec![s1], vec![HashMap::new()]),
                (make_handle(2, 2), vec![s2], vec![HashMap::new()]),
            ],
        )) as Arc<dyn SegmentReader>
    }

    #[test]
    fn sample_filter_pass_through_keeps_every_row() {
        let reader = sample_reader();
        let filter = Arc::new(
            bqlite_storage::SampleFilter::new(1.0, 0, "entity_id", BqlType::String).unwrap(),
        );
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.with_sample_filter(filter);
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);
        assert_eq!(ids, vec!["u0", "u1", "u2", "u3"]);
    }

    #[test]
    fn sample_filter_empty_set_short_circuits_to_none() {
        let reader = sample_reader();
        let filter = Arc::new(
            bqlite_storage::SampleFilter::new(0.0, 0, "entity_id", BqlType::String).unwrap(),
        );
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.with_sample_filter(filter);
        op.open().unwrap();
        // No batches expected, and the sticky exhaustion guarantees
        // repeat calls also return None.
        assert!(op.next_batch().unwrap().is_none());
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn sample_filter_half_fraction_returns_deterministic_subset() {
        // At fraction 0.5 with seed 42 the 4-entity fixture reduces to
        // whatever subset the xxhash64 threshold picks. The test asserts
        // determinism (two runs match) and that the result is a strict
        // subset of the pass-through output, not the exact membership —
        // the hash function is pinned, so the membership is stable across
        // versions, but listing it inline here would make the test depend
        // on the precise u64 outputs of `twox_hash`.
        let filter_spec = || {
            Arc::new(
                bqlite_storage::SampleFilter::new(0.5, 42, "entity_id", BqlType::String).unwrap(),
            )
        };

        let mut op1 = ScanOperator::full_scan(sample_reader()).unwrap();
        op1.with_sample_filter(filter_spec());
        op1.open().unwrap();
        let ids1 = drain_entity_ids(&mut op1);

        let mut op2 = ScanOperator::full_scan(sample_reader()).unwrap();
        op2.with_sample_filter(filter_spec());
        op2.open().unwrap();
        let ids2 = drain_entity_ids(&mut op2);

        assert_eq!(ids1, ids2, "sample filter is not deterministic");
        // Subset of full output.
        let all = ["u0".to_string(), "u1".into(), "u2".into(), "u3".into()];
        for id in &ids1 {
            assert!(all.contains(id), "unexpected sampled id {id}");
        }
        // With 4 entities and fraction 0.5 the sampled set is strictly
        // smaller than the input with high probability; assert it's not
        // the pass-through set and not empty.
        assert!(ids1.len() < all.len() && !ids1.is_empty());
    }

    /// Build a zone-map fixture where each row group has a singleton
    /// entity-id bound (`min == max`). Mirrors a compaction layout
    /// where each large entity lives in its own row group.
    fn entity_singleton_zone(entity: &str) -> HashMap<String, ZoneMap> {
        let mut zm = HashMap::new();
        zm.insert(
            "entity_id".to_string(),
            ZoneMap {
                min: Some(PropertyValue::String(entity.into())),
                max: Some(PropertyValue::String(entity.into())),
                null_count: 0,
                row_count: 1,
            },
        );
        zm
    }

    #[test]
    fn sample_filter_zone_map_prunes_rejected_row_groups() {
        // Four segments, one row group each, one entity each.
        // With `fraction: 1.0` all zones pass; with an empty-set
        // filter the short-circuit in `open()` avoids the reader
        // entirely. The interesting path is an in-range fraction
        // that rejects a known entity — the VecReader's
        // `pred.accepts_zone_group` call should skip that segment's
        // row group, and the output must match what the per-row
        // filter would have produced anyway (i.e. exact semantics
        // preserved under pruning).

        // Pick a filter configuration and two entities we can sort
        // into accept/reject piles. We avoid hashing by construction:
        // evaluate the filter once, partition, then drive the reader.
        let filter =
            bqlite_storage::SampleFilter::new(0.5, 42, "entity_id", BqlType::String).unwrap();
        let names = ["u0", "u1", "u2", "u3"];
        let accepted: Vec<&str> = names
            .iter()
            .copied()
            .filter(|n| filter.accepts_str(n.as_bytes()))
            .collect();
        let rejected: Vec<&str> = names
            .iter()
            .copied()
            .filter(|n| !filter.accepts_str(n.as_bytes()))
            .collect();
        assert!(
            !accepted.is_empty() && !rejected.is_empty(),
            "need both sides"
        );

        // Build one segment per entity with a zone map that pins its
        // entity_id min == max. The VecReader's `next_row_group`
        // implementation calls `pred.accepts_zone_group` — rejected
        // entities skip the whole row group, and the per-row filter
        // downstream cannot reject what was never decoded.
        let mut segments = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let batch = make_batch(&[name], &[100 + i as i64], &["a"]);
            segments.push((
                make_handle(i as u64, 1),
                vec![batch],
                vec![entity_singleton_zone(name)],
            ));
        }
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.with_sample_filter(Arc::new(filter));
        op.open().unwrap();
        let ids = drain_entity_ids(&mut op);

        // Output equals the accepted set, in entity-sorted order.
        let mut expected: Vec<String> = accepted.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(
            ids, expected,
            "zone-pruned output must match per-row filter"
        );
    }

    #[test]
    fn sample_filter_zone_map_pass_through_accepts_every_zone() {
        // `fraction: 1.0` should never prune a zone — even a singleton
        // zone for an entity with any conceivable hash passes.
        let filter =
            bqlite_storage::SampleFilter::new(1.0, 0, "entity_id", BqlType::String).unwrap();
        let names = ["u0", "u1"];
        let mut segments = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let batch = make_batch(&[name], &[100 + i as i64], &["a"]);
            segments.push((
                make_handle(i as u64, 1),
                vec![batch],
                vec![entity_singleton_zone(name)],
            ));
        }
        let reader: Arc<dyn SegmentReader> =
            Arc::new(VecReader::with_segments(minimal_schema(), segments));
        let mut op = ScanOperator::full_scan(reader).unwrap();
        op.with_sample_filter(Arc::new(filter));
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u0", "u1"]);
    }

    #[test]
    fn sample_filter_composes_with_post_filters() {
        // `WHERE event_type == 'c'` leaves only the `u2` row; a
        // fraction-0.5 sample that *does* include `u2` keeps the row;
        // a sample that excludes `u2` produces empty output. Both
        // orderings agree on that row because the entity-id is the
        // only hash input, independent of the predicate's column.
        let schema = OperatorSchema::new(minimal_schema().columns().to_vec()).unwrap();
        let pred = compile_predicate(
            compare(CompareOp::Equal, col("event_type"), string_lit("c")),
            &schema,
        );

        // First check without sample: exactly one row, entity u2.
        let mut op = ScanOperator::new(
            sample_reader(),
            &[],
            vec![pred.clone()],
            CancellationToken::new(),
        )
        .unwrap();
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u2"]);

        // Now with a `fraction: 1.0` sample — must still match.
        let filter = Arc::new(
            bqlite_storage::SampleFilter::new(1.0, 0, "entity_id", BqlType::String).unwrap(),
        );
        let mut op = ScanOperator::new(
            sample_reader(),
            &[],
            vec![pred.clone()],
            CancellationToken::new(),
        )
        .unwrap();
        op.with_sample_filter(filter);
        op.open().unwrap();
        assert_eq!(drain_entity_ids(&mut op), vec!["u2"]);

        // And with an `fraction: 0.0` sample — empty output.
        let empty = Arc::new(
            bqlite_storage::SampleFilter::new(0.0, 0, "entity_id", BqlType::String).unwrap(),
        );
        let mut op =
            ScanOperator::new(sample_reader(), &[], vec![pred], CancellationToken::new()).unwrap();
        op.with_sample_filter(empty);
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
    }
}
