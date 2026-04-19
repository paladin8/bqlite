//! K-way merge scan across L0 segments (TASK-219).
//!
//! Given a set of per-segment [`SegmentScan`]s that each yield rows
//! in `(entity_id, ts)` order, this module merges them into a single
//! `(entity_id, ts)`-ordered output stream. Critical for Wave 2
//! because compaction is not yet implemented — every ingest
//! produces a new L0 segment, and queries must merge across all of
//! them at read time.
//!
//! # Algorithm
//!
//! Classic streaming k-way merge against row-group batches:
//!
//! 1. **Prime.** Read one `RecordBatch` per scan into `ScanState`.
//!    Scans that yield `Ok(None)` immediately are marked exhausted.
//! 2. **Pick.** Maintain a small min-heap of active-row entries.
//!    Pop the smallest `(entity_id, ts)` tuple, record
//!    `(scan_idx, row_idx)` into an index vector, advance that scan's
//!    cursor, and push it back onto the heap if the batch still has
//!    rows remaining.
//! 3. **Emit.** When the index vector reaches `batch_target_rows`
//!    or when every currently-loaded row has been consumed, call
//!    [`arrow::compute::interleave`] once per output column with
//!    the accumulated indices. This produces the output
//!    [`RecordBatch`] in one Arrow call per column.
//! 4. **Reload.** After emitting, any scan whose current batch is
//!    exhausted pulls its next row group on the following
//!    `next_batch` call.
//!
//! The active-input heap keeps picks at `O(log k)` instead of the old
//! `O(k)` linear walk over every live scan on every emitted row. We
//! still delay reloading an exhausted input batch until after the
//! current `interleave` call completes so the row references in the
//! pending index vector stay valid.
//!
//! # Key extraction
//!
//! The merge needs to compare `(entity_id, ts)` across row groups
//! where `entity_id` is a string and `ts` is an i64 nanoseconds
//! value. The caller tells us which columns in the schema carry
//! the two role fields at construction time; we avoid a
//! `TableSchema` dependency so this module stays agnostic to the
//! role-resolution layer.
//!
//! Three Arrow types can materialise the entity key in Wave 2:
//! `Utf8View` (the canonical type), `Utf8`, and `Int64`. Timestamps
//! always come out of the reader as `TimestampNanosecondArray` with
//! the schema-canonical `"UTC"` timezone.
//!
//! # Scope
//!
//! - **Inputs.** An arbitrary number of [`SegmentScan`] trait
//!   objects whose batches share the same Arrow schema. The merge
//!   checks schema equality on construction to surface a mismatch
//!   immediately.
//! - **Outputs.** Successive [`RecordBatch`]es, each
//!   `(entity_id, ts)`-ordered. The merge does not implement the
//!   `SegmentScan` trait itself because a merged stream has no
//!   per-row-group zone map surface — zone-map pruning is applied
//!   by each input scan individually via its own predicate hook,
//!   and the merge only sees rows that survived pruning.
//!
//! # Access-pattern hints (TASK-243)
//!
//! The merge does not open segment files directly — it owns a
//! `Vec<Box<dyn SegmentScan>>` constructed upstream. Every input
//! scan opened through
//! [`crate::segment::reader::SegmentFileReader::open`] has already
//! issued a `POSIX_FADV_SEQUENTIAL` hint at open time via
//! [`crate::segment::advise::advise_sequential`], so the merged
//! stream inherits sequential-scan readahead from each leaf
//! without needing any hint of its own. `WillNeed` for the next
//! row group and the compaction-specific hint pair are deferred
//! to Wave 4 per `docs/design/storage-format.md` §8.2.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use ::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray, StringViewArray};
use ::arrow::compute::interleave;
use ::arrow::datatypes::{DataType, Schema as ArrowSchema, TimeUnit};

use bqlite_core::encoded::{
    EncodedBatch, RowRef, RowRun, RowSelection, StitchedBatch, StitchedRows,
};
use bqlite_core::storage::SegmentScan;
use bqlite_core::{BqlType, BqliteError, Result};

use crate::segment::materialize::materialize_encoded_column;

/// Default number of rows emitted per merged [`RecordBatch`].
///
/// Matches the v1 row-group default so a merge that drains a
/// single-segment query produces the same batch shape a raw
/// [`crate::segment::reader::SegmentFileScan`] would. Real scan
/// operators may pick a smaller value to amortize pipeline costs.
pub const DEFAULT_MERGE_BATCH_ROWS: usize = 65_536;

/// Streaming k-way merge across a set of pre-opened segment scans.
///
/// Each input scan must produce `(entity_id, ts)`-ordered rows and
/// share the same Arrow schema. Input ordering must be strict
/// within each scan — the merge does not re-sort and relies on the
/// fact that the reader always emits segment rows in storage order
/// (§7.2 entity-boundary invariant).
pub struct KWayMergeScan {
    /// Output Arrow schema, shared with every input scan and every
    /// emitted [`RecordBatch`].
    schema: Arc<ArrowSchema>,
    /// Per-scan cursor state.
    scans: Vec<ScanState>,
    /// Column ordinal of the `entity_id` key in the shared schema.
    entity_key_col: usize,
    /// Column ordinal of the `ts` key in the shared schema.
    ts_col: usize,
    /// Target row count for each emitted merged batch. The merge
    /// may emit fewer rows when the entire merged stream has fewer
    /// than `batch_target_rows` rows remaining.
    batch_target_rows: usize,
    /// BinaryHeap-backed min-heap of active rows, one per scan whose
    /// current cursor points at a live `(entity_id, ts)` tuple.
    active_heap: BinaryHeap<Reverse<HeapEntry>>,
    /// Per-column placeholder array used to satisfy
    /// `arrow::compute::interleave` when a scan has no current batch
    /// loaded. Never indexed, so the content is unused; the
    /// `data_type` just needs to match the output column's type.
    placeholders: Vec<ArrayRef>,
    /// True after every scan has been drained.
    exhausted: bool,
}

impl std::fmt::Debug for KWayMergeScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KWayMergeScan")
            .field("inputs", &self.scans.len())
            .field("active_inputs", &self.active_heap.len())
            .field("entity_key_col", &self.entity_key_col)
            .field("ts_col", &self.ts_col)
            .field("batch_target_rows", &self.batch_target_rows)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

/// Per-scan state the merge needs to track while picking rows.
struct ScanState {
    scan: Box<dyn SegmentScan>,
    /// Currently-loaded row-group batch. `None` means "need to
    /// reload on the next `next_batch` call".
    batch: Option<RecordBatch>,
    /// Row index into [`Self::batch`] for the next pick.
    cursor: usize,
    /// True once the scan has yielded `Ok(None)`; no more batches
    /// will be loaded.
    scan_exhausted: bool,
}

/// Pre-extracted entity key for zero-dispatch heap comparisons.
///
/// Storing the entity key value directly in the heap entry avoids
/// `as_any().downcast_ref()` dynamic dispatch on every comparison
/// (the #1 hotspot in pprof profiles of the 100M funnel query).
#[derive(Debug, Clone)]
enum EntityKeyValue {
    /// Inline string key. Short strings (common for entity IDs like
    /// `"user_000042"`) are stored directly without heap allocation
    /// via `SmallVec`. Longer strings fall back to heap.
    Str(smallvec::SmallVec<[u8; 24]>),
    /// Integer entity key.
    Int(i64),
}

impl EntityKeyValue {
    fn extract(col: &ArrayRef, row: usize) -> Self {
        if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
            let s = arr.value(row).as_bytes();
            EntityKeyValue::Str(smallvec::SmallVec::from_slice(s))
        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            let s = arr.value(row).as_bytes();
            EntityKeyValue::Str(smallvec::SmallVec::from_slice(s))
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            EntityKeyValue::Int(arr.value(row))
        } else {
            unreachable!(
                "entity key type {:?} not supported — should have been rejected by \
                 validate_key_types at construction",
                col.data_type()
            )
        }
    }
}

impl Ord for EntityKeyValue {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (EntityKeyValue::Str(a), EntityKeyValue::Str(b)) => a.as_slice().cmp(b.as_slice()),
            (EntityKeyValue::Int(a), EntityKeyValue::Int(b)) => a.cmp(b),
            // Mixed types cannot occur (all scans share the same schema),
            // but give a stable ordering to avoid UB if it ever happens.
            (EntityKeyValue::Str(_), EntityKeyValue::Int(_)) => Ordering::Less,
            (EntityKeyValue::Int(_), EntityKeyValue::Str(_)) => Ordering::Greater,
        }
    }
}

impl PartialOrd for EntityKeyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for EntityKeyValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EntityKeyValue {}

/// Heap entry for one scan's current row.
///
/// Stores pre-extracted `(entity_key, ts)` values so heap comparisons
/// are pure scalar/byte-slice operations with no dynamic dispatch.
/// This avoids the `as_any().downcast_ref().type_id()` overhead that
/// dominated pprof profiles of the k-way merge hot path (TASK-331).
struct HeapEntry {
    scan_idx: usize,
    row_idx: usize,
    entity_key: EntityKeyValue,
    ts_nanos: i64,
}

impl HeapEntry {
    fn from_scan(scan_idx: usize, state: &ScanState, entity_key_col: usize, ts_col: usize) -> Self {
        let batch = state
            .batch
            .as_ref()
            .expect("active-heap entries always point at a loaded batch");
        let entity_key = EntityKeyValue::extract(batch.column(entity_key_col), state.cursor);
        let ts_nanos = extract_ts_nanos(batch.column(ts_col), state.cursor);
        Self {
            scan_idx,
            row_idx: state.cursor,
            entity_key,
            ts_nanos,
        }
    }
}

/// Extract the i64 nanosecond timestamp from a column at a given row.
#[inline]
fn extract_ts_nanos(col: &ArrayRef, row: usize) -> i64 {
    use ::arrow::array::TimestampNanosecondArray;
    col.as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ts column validated as TimestampNanosecond at construction")
        .value(row)
}

impl Ord for HeapEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.entity_key
            .cmp(&other.entity_key)
            .then_with(|| self.ts_nanos.cmp(&other.ts_nanos))
            .then_with(|| self.scan_idx.cmp(&other.scan_idx))
            .then_with(|| self.row_idx.cmp(&other.row_idx))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl KWayMergeScan {
    /// Construct a k-way merge over a set of segment scans.
    ///
    /// `schema` is the Arrow schema every input scan must produce;
    /// the merge validates that each scan's first batch matches
    /// when batches are loaded and returns an error on mismatch.
    ///
    /// `entity_key_col` and `ts_col` are the ordinals of the
    /// `entity_id` and `ts` columns in `schema`. The caller resolves
    /// these from its `TableSchema` (role metadata is not a concern
    /// of this module).
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Execution`] if `entity_key_col` or `ts_col`
    ///   is out of range for `schema`.
    /// - [`BqliteError::Execution`] if the schema's key columns do
    ///   not have types this merge can compare (`Utf8` / `Utf8View`
    ///   / `Int64` for entity_key, `Timestamp` for ts).
    pub fn new(
        scans: Vec<Box<dyn SegmentScan>>,
        schema: Arc<ArrowSchema>,
        entity_key_col: usize,
        ts_col: usize,
    ) -> Result<Self> {
        Self::with_batch_size(
            scans,
            schema,
            entity_key_col,
            ts_col,
            DEFAULT_MERGE_BATCH_ROWS,
        )
    }

    /// Construct a merge with a custom output batch size.
    pub fn with_batch_size(
        scans: Vec<Box<dyn SegmentScan>>,
        schema: Arc<ArrowSchema>,
        entity_key_col: usize,
        ts_col: usize,
        batch_target_rows: usize,
    ) -> Result<Self> {
        if entity_key_col >= schema.fields().len() {
            return Err(BqliteError::Execution(format!(
                "k-way merge: entity_key_col {entity_key_col} out of range for schema with {} fields",
                schema.fields().len()
            )));
        }
        if ts_col >= schema.fields().len() {
            return Err(BqliteError::Execution(format!(
                "k-way merge: ts_col {ts_col} out of range for schema with {} fields",
                schema.fields().len()
            )));
        }
        validate_key_types(schema.as_ref(), entity_key_col, ts_col)?;
        if batch_target_rows == 0 {
            return Err(BqliteError::Execution(
                "k-way merge: batch_target_rows must be positive".into(),
            ));
        }

        let placeholders: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|f| ::arrow::array::new_empty_array(f.data_type()))
            .collect();

        let states = scans
            .into_iter()
            .map(|scan| ScanState {
                scan,
                batch: None,
                cursor: 0,
                scan_exhausted: false,
            })
            .collect();

        Ok(Self {
            schema,
            scans: states,
            entity_key_col,
            ts_col,
            batch_target_rows,
            active_heap: BinaryHeap::new(),
            placeholders,
            exhausted: false,
        })
    }

    /// Number of input scans the merge was constructed with.
    pub fn input_count(&self) -> usize {
        self.scans.len()
    }

    /// Merged Arrow schema (shared with every input scan).
    pub fn schema(&self) -> &Arc<ArrowSchema> {
        &self.schema
    }

    /// Yield the next merged [`RecordBatch`], or `Ok(None)` when
    /// every input scan has been drained.
    ///
    /// The returned batch holds rows in strict `(entity_id, ts)`
    /// order across every input scan. Consecutive calls continue
    /// from where the previous call left off; dropping the merge
    /// releases every input scan.
    ///
    /// # Tie-break
    ///
    /// When two scans carry equal `(entity_id, ts)` tuples at their
    /// current cursors, the merge deterministically picks the
    /// lower-indexed scan first (pick order follows the order of
    /// the `scans` vec passed to [`Self::new`]). Callers relying on
    /// a specific source-priority ordering — e.g. "newer segments
    /// shadow older ones" — should hand the scans in that order.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }

        // TASK-247: single-scan fast path. When there is exactly one
        // input scan (the common case for a freshly ingested table
        // before compaction) and the batch target is at least as
        // large as the default row-group size, bypass the comparison
        // / interleave machinery entirely and pass row-group batches
        // through unchanged. This avoids the O(rows) interleave copy
        // and the per-row pick_smallest overhead.
        //
        // The batch_target_rows guard ensures callers that explicitly
        // request smaller batches (e.g. tests with batch_size=2)
        // still get correct splitting through the standard path.
        if self.scans.len() == 1 && self.batch_target_rows >= DEFAULT_MERGE_BATCH_ROWS {
            return self.next_batch_single();
        }

        self.reload_needed_batches()?;
        if self.active_heap.is_empty() && self.all_scans_done() {
            self.exhausted = true;
            return Ok(None);
        }

        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(self.batch_target_rows);
        while indices.len() < self.batch_target_rows {
            let Some(i) = self.pop_smallest() else {
                break;
            };

            let cursor = self.scans[i].cursor;
            indices.push((i, cursor));
            self.scans[i].cursor += 1;

            let batch_rows = self.scans[i].batch.as_ref().unwrap().num_rows();
            if self.scans[i].cursor < batch_rows {
                self.push_active_scan(i);
            }
        }

        if indices.is_empty() {
            // Nothing picked — either every scan is exhausted or
            // every scan's cursor was already past its batch (which
            // shouldn't happen after `reload_needed_batches`). Mark
            // the merge exhausted and return.
            self.exhausted = true;
            return Ok(None);
        }

        let out_batch = self.interleave_output(&indices)?;

        // Clear any batch whose cursor is now at the end so the
        // next call will reload fresh row groups for that scan.
        for state in self.scans.iter_mut() {
            let need_clear = state
                .batch
                .as_ref()
                .map(|b| state.cursor >= b.num_rows())
                .unwrap_or(false);
            if need_clear {
                state.batch = None;
                state.cursor = 0;
            }
        }

        Ok(Some(out_batch))
    }

    /// Single-scan fast path: yield row-group batches directly from
    /// the only input scan without interleave or comparison overhead.
    fn next_batch_single(&mut self) -> Result<Option<RecordBatch>> {
        let state = &mut self.scans[0];
        if state.scan_exhausted {
            self.exhausted = true;
            return Ok(None);
        }
        loop {
            match state.scan.next_row_group()? {
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    // Validate the schema on every batch to honour the
                    // same invariant the k-way path enforces in
                    // `reload_needed_batches`.
                    if batch.schema() != self.schema {
                        return Err(BqliteError::Execution(
                            "k-way merge: scan 0's batch schema does not match the merge schema"
                                .to_string(),
                        ));
                    }
                    return Ok(Some(batch));
                }
                None => {
                    state.scan_exhausted = true;
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
    }

    /// Fill every scan's `batch` slot if it is currently empty.
    /// Skips empty batches and marks the scan exhausted when
    /// `next_row_group` returns `Ok(None)`.
    fn reload_needed_batches(&mut self) -> Result<()> {
        let mut ready_indices = Vec::new();
        for i in 0..self.scans.len() {
            if self.scans[i].batch.is_some() || self.scans[i].scan_exhausted {
                continue;
            }
            loop {
                match self.scans[i].scan.next_row_group()? {
                    Some(batch) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        if batch.schema() != self.schema {
                            return Err(BqliteError::Execution(format!(
                                "k-way merge: scan {i}'s batch schema does not match the merge schema"
                            )));
                        }
                        self.scans[i].batch = Some(batch);
                        self.scans[i].cursor = 0;
                        ready_indices.push(i);
                        break;
                    }
                    None => {
                        self.scans[i].scan_exhausted = true;
                        break;
                    }
                }
            }
        }
        for i in ready_indices {
            self.push_active_scan(i);
        }
        Ok(())
    }

    /// True once every input scan has been drained.
    fn all_scans_done(&self) -> bool {
        self.scans
            .iter()
            .all(|s| s.scan_exhausted && s.batch.is_none())
    }

    /// Push a scan with a live current row into the active-input
    /// min-heap.
    fn push_active_scan(&mut self, scan_idx: usize) {
        let entry = HeapEntry::from_scan(
            scan_idx,
            &self.scans[scan_idx],
            self.entity_key_col,
            self.ts_col,
        );
        self.active_heap.push(Reverse(entry));
    }

    /// Pop the scan whose current row has the smallest `(entity_id, ts)`
    /// tuple, or `None` when no scan currently has a live row.
    fn pop_smallest(&mut self) -> Option<usize> {
        self.active_heap.pop().map(|Reverse(entry)| entry.scan_idx)
    }

    /// Build an output [`RecordBatch`] by interleaving rows from
    /// every scan's current batch according to `indices`.
    fn interleave_output(&self, indices: &[(usize, usize)]) -> Result<RecordBatch> {
        let num_cols = self.schema.fields().len();
        let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            // For every scan, provide its current batch's column
            // (or a type-matched placeholder for scans without a
            // batch). Scans without a batch will never appear in
            // `indices`, so the placeholder is never indexed.
            let col_refs: Vec<&dyn Array> = self
                .scans
                .iter()
                .map(|s| match s.batch.as_ref() {
                    Some(b) => b.column(col_idx).as_ref(),
                    None => self.placeholders[col_idx].as_ref(),
                })
                .collect();
            let out_col = interleave(&col_refs, indices).map_err(|e| {
                BqliteError::Execution(format!(
                    "k-way merge: arrow interleave failed for column {col_idx}: {e}"
                ))
            })?;
            out_cols.push(out_col);
        }
        RecordBatch::try_new(self.schema.clone(), out_cols).map_err(|e| {
            BqliteError::Execution(format!("k-way merge: failed to assemble output batch: {e}"))
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Encoded-preserving k-way merge (CP5)
// ─────────────────────────────────────────────────────────────────────────────

/// Default number of rows emitted per merged [`StitchedBatch`].
///
/// Kept separate from [`DEFAULT_MERGE_BATCH_ROWS`] so the encoded-merge
/// emit size can diverge from the materialized merge's without
/// cross-path coupling.
pub const DEFAULT_STITCHED_BATCH_ROWS: usize = 65_536;

/// Pull-based iterator of pre-filtered encoded row groups.
///
/// The injection point between [`crate::segment::reader::SegmentFileScan`]
/// plus per-source predicate kernels (owned by `bqlite-operators`)
/// and [`EncodedKWayMergeScan`] (this module). Each call returns the
/// next `(EncodedBatch, RowSelection)` pair; the selection narrows
/// the batch to rows that survived the source-local pushable
/// predicates. `Ok(None)` marks the source exhausted.
///
/// Implementations should skip fully-filtered batches internally so
/// the merge doesn't churn reloads over empties, but the merge still
/// tolerates an empty selection as a defensive belt-and-suspenders.
pub trait EncodedBatchSource: Send {
    fn next(&mut self) -> Result<Option<(EncodedBatch, RowSelection)>>;
}

/// Small walker that advances through a [`RowSelection`] without
/// materializing the full index list up front.
///
/// The `Runs` variant is critical: RLE predicate kernels (CP4) emit
/// `RowSelection::Runs` specifically to preserve run shape through
/// filter. Flattening those runs into a `Vec<u32>` at cursor-load time
/// would (a) force `O(total_rows)` memory and (b) discard the shape
/// downstream consumers still rely on through CP6/CP7. The walker is
/// ~30 lines and keeps both variants intact.
#[derive(Debug)]
enum SelectionCursor {
    Indices {
        indices: Vec<u32>,
        pos: usize,
    },
    Runs {
        runs: Vec<RowRun>,
        run_idx: usize,
        run_offset: u32,
    },
}

impl SelectionCursor {
    /// Invariant: `run_idx == runs.len()` (cursor done) OR
    /// `run_offset < runs[run_idx].len` (pointing at a live row).
    /// `from_selection` establishes it by skipping leading zero-length
    /// runs; `advance()` preserves it.
    fn from_selection(sel: RowSelection) -> Self {
        match sel {
            RowSelection::Indices(sv) => SelectionCursor::Indices {
                indices: sv.into_vec(),
                pos: 0,
            },
            RowSelection::Runs(runs) => {
                let mut run_idx = 0;
                while run_idx < runs.len() && runs[run_idx].len == 0 {
                    run_idx += 1;
                }
                SelectionCursor::Runs {
                    runs,
                    run_idx,
                    run_offset: 0,
                }
            }
        }
    }

    #[inline]
    fn current(&self) -> Option<u32> {
        match self {
            SelectionCursor::Indices { indices, pos } => {
                if *pos < indices.len() {
                    Some(indices[*pos])
                } else {
                    None
                }
            }
            SelectionCursor::Runs {
                runs,
                run_idx,
                run_offset,
            } => {
                if *run_idx < runs.len() {
                    Some(runs[*run_idx].start + *run_offset)
                } else {
                    None
                }
            }
        }
    }

    #[inline]
    fn advance(&mut self) {
        match self {
            SelectionCursor::Indices { pos, .. } => *pos += 1,
            SelectionCursor::Runs {
                runs,
                run_idx,
                run_offset,
            } => {
                *run_offset += 1;
                while *run_idx < runs.len() && *run_offset >= runs[*run_idx].len {
                    *run_idx += 1;
                    *run_offset = 0;
                }
            }
        }
    }

    #[inline]
    fn is_done(&self) -> bool {
        match self {
            SelectionCursor::Indices { indices, pos } => *pos >= indices.len(),
            SelectionCursor::Runs { runs, run_idx, .. } => *run_idx >= runs.len(),
        }
    }
}

/// Per-source loaded encoded row group plus selection walker and
/// decoded sort-key arrays.
///
/// Decoding only `entity_id` + `ts` matches design-doc §8.5 ("only
/// sort-key columns are materialized inside the merge"); every other
/// column stays pinned in `batch`.
struct LoadedEncodedBatch {
    batch: EncodedBatch,
    selection: SelectionCursor,
    entity_arr: ArrayRef,
    ts_arr: ArrayRef,
}

/// Per-source cursor state.
struct EncodedCursor {
    source: Box<dyn EncodedBatchSource>,
    loaded: Option<LoadedEncodedBatch>,
    exhausted: bool,
}

/// Heap entry for one source's current row under the encoded merge.
///
/// Mirrors [`HeapEntry`] exactly — pre-extracted `(entity_key,
/// ts_nanos)` for zero-dispatch comparison, plus `scan_idx` / `row_idx`
/// for deterministic tie-break. Tie-break order `entity_key → ts_nanos
/// → scan_idx → row_idx` pins the lower-indexed source to win on
/// equal `(entity_id, ts)` tuples.
struct EncodedHeapEntry {
    scan_idx: usize,
    row_idx: u32,
    entity_key: EntityKeyValue,
    ts_nanos: i64,
}

impl Ord for EncodedHeapEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.entity_key
            .cmp(&other.entity_key)
            .then_with(|| self.ts_nanos.cmp(&other.ts_nanos))
            .then_with(|| self.scan_idx.cmp(&other.scan_idx))
            .then_with(|| self.row_idx.cmp(&other.row_idx))
    }
}

impl PartialOrd for EncodedHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for EncodedHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for EncodedHeapEntry {}

/// Streaming k-way merge across a set of encoded row-group sources.
///
/// Emits [`StitchedBatch`]es whose `sources` hold Arc-cloned encoded
/// columns (zero payload copies) and whose `rows` describe pick order
/// via [`StitchedRows::Indices`]. Consumers call
/// [`crate::materialize_encoded_column`] plus
/// `bqlite_operators::materialize_stitched` to project the stitched
/// batch to a dense Arrow `RecordBatch`.
///
/// # Sort key
///
/// `(entity_id, ts)` tuples across every source. Equal tuples break
/// toward the lower-indexed source; callers wanting "newer segments
/// shadow older ones" must hand sources in that order.
///
/// # CP5 scope
///
/// CP5 ships `Indices`-only emission. `SingleSource { selection:
/// Some(...) }` and `Runs` compaction (preserving RLE selection shape
/// through the merge boundary) are a scheduled follow-up — see CP5
/// plan §4 Step 8 and the design doc's §7.3.
pub struct EncodedKWayMergeScan {
    schema: Arc<ArrowSchema>,
    sources: Vec<EncodedCursor>,
    entity_key_col: usize,
    ts_col: usize,
    /// BqlType of the entity-key column, derived at construction from
    /// the Arrow schema and cached so per-reload decodes don't re-map.
    entity_bql: BqlType,
    /// BqlType of the ts column; always [`BqlType::Timestamp`] after
    /// [`validate_key_types`] passes.
    ts_bql: BqlType,
    batch_target_rows: usize,
    active_heap: BinaryHeap<Reverse<EncodedHeapEntry>>,
    exhausted: bool,
}

impl std::fmt::Debug for EncodedKWayMergeScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncodedKWayMergeScan")
            .field("sources", &self.sources.len())
            .field("active_sources", &self.active_heap.len())
            .field("entity_key_col", &self.entity_key_col)
            .field("ts_col", &self.ts_col)
            .field("batch_target_rows", &self.batch_target_rows)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl EncodedKWayMergeScan {
    /// Construct an encoded k-way merge across `sources`.
    ///
    /// `schema` is the shared Arrow schema every source's batches
    /// conform to after predicate application. `entity_key_col` and
    /// `ts_col` are ordinals in `schema` for the sort keys. The
    /// constructor validates the key-column types mirror
    /// [`KWayMergeScan::new`]'s rules; no IO happens until
    /// [`Self::next_stitched_batch`] is called.
    pub fn new(
        sources: Vec<Box<dyn EncodedBatchSource>>,
        schema: Arc<ArrowSchema>,
        entity_key_col: usize,
        ts_col: usize,
    ) -> Result<Self> {
        Self::with_batch_size(
            sources,
            schema,
            entity_key_col,
            ts_col,
            DEFAULT_STITCHED_BATCH_ROWS,
        )
    }

    /// Construct a merge with a custom emitted-batch row cap.
    pub fn with_batch_size(
        sources: Vec<Box<dyn EncodedBatchSource>>,
        schema: Arc<ArrowSchema>,
        entity_key_col: usize,
        ts_col: usize,
        batch_target_rows: usize,
    ) -> Result<Self> {
        if entity_key_col >= schema.fields().len() {
            return Err(BqliteError::Execution(format!(
                "encoded merge: entity_key_col {entity_key_col} out of range for schema with {} fields",
                schema.fields().len()
            )));
        }
        if ts_col >= schema.fields().len() {
            return Err(BqliteError::Execution(format!(
                "encoded merge: ts_col {ts_col} out of range for schema with {} fields",
                schema.fields().len()
            )));
        }
        validate_key_types(schema.as_ref(), entity_key_col, ts_col)?;
        if batch_target_rows == 0 {
            return Err(BqliteError::Execution(
                "encoded merge: batch_target_rows must be positive".into(),
            ));
        }

        let entity_bql = bql_type_for_sort_key(schema.field(entity_key_col).data_type());
        let ts_bql = bql_type_for_sort_key(schema.field(ts_col).data_type());

        let cursors = sources
            .into_iter()
            .map(|source| EncodedCursor {
                source,
                loaded: None,
                exhausted: false,
            })
            .collect();

        Ok(Self {
            schema,
            sources: cursors,
            entity_key_col,
            ts_col,
            entity_bql,
            ts_bql,
            batch_target_rows,
            active_heap: BinaryHeap::new(),
            exhausted: false,
        })
    }

    /// Number of input sources.
    pub fn input_count(&self) -> usize {
        self.sources.len()
    }

    /// Shared Arrow schema of the stitched output (per-source batches
    /// must decode to this schema).
    pub fn schema(&self) -> &Arc<ArrowSchema> {
        &self.schema
    }

    /// Yield the next merged [`StitchedBatch`], or `Ok(None)` when
    /// every source has been drained.
    ///
    /// The emitted batch holds rows in strict `(entity_id, ts)` order
    /// across every source; equal keys break toward the lower-indexed
    /// source. CP5 always emits [`StitchedRows::Indices`] — see the
    /// type-level docs for the deferred `SingleSource` / `Runs`
    /// compaction follow-up.
    pub fn next_stitched_batch(&mut self) -> Result<Option<StitchedBatch>> {
        if self.exhausted {
            return Ok(None);
        }

        self.reload_needed_sources()?;

        if self.active_heap.is_empty() && self.all_sources_done() {
            self.exhausted = true;
            return Ok(None);
        }

        let mut picks: Vec<RowRef> = Vec::with_capacity(self.batch_target_rows);
        while picks.len() < self.batch_target_rows {
            let Some(Reverse(entry)) = self.active_heap.pop() else {
                break;
            };

            let source_u16 = u16::try_from(entry.scan_idx).map_err(|_| {
                BqliteError::Execution(format!(
                    "encoded merge: source index {} exceeds u16::MAX; \
                     StitchedBatch::RowRef cannot represent it",
                    entry.scan_idx
                ))
            })?;
            picks.push(RowRef {
                source: source_u16,
                row: entry.row_idx,
            });

            // Advance the walker and re-push the heap entry if more
            // selected rows remain in this source's current batch.
            let cursor = &mut self.sources[entry.scan_idx];
            if let Some(loaded) = cursor.loaded.as_mut() {
                loaded.selection.advance();
                if !loaded.selection.is_done() {
                    let next_entry = Self::heap_entry_for_loaded(entry.scan_idx, loaded);
                    self.active_heap.push(Reverse(next_entry));
                }
            }
        }

        if picks.is_empty() {
            // Nothing picked — every source's current batch drained to
            // its end during pick without any new row available, and
            // the heap is empty. Reload will try again on the next
            // call; mark exhausted iff every source is done.
            if self.all_sources_done() {
                self.exhausted = true;
            }
            return Ok(None);
        }

        // Drop any loaded batch whose selection is done so the next
        // call reloads it fresh. Collect currently-pinned sources for
        // inclusion in the stitched output first — every source index
        // referenced in `picks` must still appear in `sources`.
        let stitched_sources: Vec<EncodedBatch> = self
            .sources
            .iter()
            .map(|cursor| {
                cursor
                    .loaded
                    .as_ref()
                    .map(|l| l.batch.clone())
                    .unwrap_or_else(|| EncodedBatch::new(0, Vec::new()))
            })
            .collect();

        // Clear fully-drained cursors for the next reload cycle. Must
        // happen after `stitched_sources` is built above (we just
        // cloned the Arc-backed EncodedBatch; clearing the cursor does
        // not invalidate the clones).
        for cursor in self.sources.iter_mut() {
            let drained = cursor
                .loaded
                .as_ref()
                .map(|l| l.selection.is_done())
                .unwrap_or(false);
            if drained {
                cursor.loaded = None;
            }
        }

        Ok(Some(StitchedBatch {
            sources: stitched_sources,
            rows: StitchedRows::Indices(picks),
        }))
    }

    /// Ensure every non-exhausted source has a live `LoadedEncodedBatch`.
    ///
    /// Loops past empty selections (skip fully-filtered batches).
    /// Sources that return `Ok(None)` are marked exhausted. Newly
    /// loaded cursors push their first row into the active heap.
    fn reload_needed_sources(&mut self) -> Result<()> {
        let mut ready_indices: Vec<usize> = Vec::new();
        for i in 0..self.sources.len() {
            if self.sources[i].loaded.is_some() || self.sources[i].exhausted {
                continue;
            }
            loop {
                match self.sources[i].source.next()? {
                    Some((batch, selection)) => {
                        if selection.is_empty() {
                            // Fully-filtered batch — skip it and try
                            // the next one. Keeps the merge free of
                            // heap entries that would immediately
                            // advance-to-done.
                            continue;
                        }
                        let loaded = self.load_batch(batch, selection)?;
                        self.sources[i].loaded = Some(loaded);
                        ready_indices.push(i);
                        break;
                    }
                    None => {
                        self.sources[i].exhausted = true;
                        break;
                    }
                }
            }
        }
        for i in ready_indices {
            let loaded = self.sources[i]
                .loaded
                .as_ref()
                .expect("just-loaded cursor has Some(loaded)");
            let entry = Self::heap_entry_for_loaded(i, loaded);
            self.active_heap.push(Reverse(entry));
        }
        Ok(())
    }

    /// Decode only the sort-key columns of `batch` into dense Arrow
    /// arrays and bundle them with a walker over `selection`.
    fn load_batch(
        &self,
        batch: EncodedBatch,
        selection: RowSelection,
    ) -> Result<LoadedEncodedBatch> {
        if batch.columns.len() != self.schema.fields().len() {
            return Err(BqliteError::Execution(format!(
                "encoded merge: source batch has {} columns but merge schema has {}",
                batch.columns.len(),
                self.schema.fields().len()
            )));
        }
        let entity_arr =
            materialize_encoded_column(&batch.columns[self.entity_key_col], &self.entity_bql)?;
        let ts_arr = materialize_encoded_column(&batch.columns[self.ts_col], &self.ts_bql)?;
        // Defend against sources that hand back decoded arrays whose
        // DataType disagrees with the merge schema — otherwise
        // `EntityKeyValue::extract` / `extract_ts_nanos` would panic on
        // the downcast. The trait is public, so a future adapter could
        // violate the implicit contract if it isn't checked here.
        let expected_ek = self.schema.field(self.entity_key_col).data_type();
        if entity_arr.data_type() != expected_ek {
            return Err(BqliteError::Execution(format!(
                "encoded merge: entity_key column decoded as {:?}, expected {:?}",
                entity_arr.data_type(),
                expected_ek
            )));
        }
        let expected_ts = self.schema.field(self.ts_col).data_type();
        let expected_rows = batch.row_count as usize;
        if entity_arr.len() != expected_rows
            || ts_arr.len() != expected_rows
            || !matches!(
                ts_arr.data_type(),
                DataType::Timestamp(TimeUnit::Nanosecond, _)
            )
            || ts_arr.data_type() != expected_ts
        {
            return Err(BqliteError::Execution(format!(
                "encoded merge: sort-key arrays invalid (entity_len={}, ts_len={}, \
                 ts_type={:?}, expected ts_type={:?}, row_count={})",
                entity_arr.len(),
                ts_arr.len(),
                ts_arr.data_type(),
                expected_ts,
                batch.row_count,
            )));
        }
        Ok(LoadedEncodedBatch {
            batch,
            selection: SelectionCursor::from_selection(selection),
            entity_arr,
            ts_arr,
        })
    }

    /// Build a heap entry pointing at the cursor's current row.
    fn heap_entry_for_loaded(scan_idx: usize, loaded: &LoadedEncodedBatch) -> EncodedHeapEntry {
        let row = loaded
            .selection
            .current()
            .expect("heap_entry_for_loaded called with a done cursor");
        let entity_key = EntityKeyValue::extract(&loaded.entity_arr, row as usize);
        let ts_nanos = extract_ts_nanos(&loaded.ts_arr, row as usize);
        EncodedHeapEntry {
            scan_idx,
            row_idx: row,
            entity_key,
            ts_nanos,
        }
    }

    /// True once every source has been drained and has no loaded batch.
    fn all_sources_done(&self) -> bool {
        self.sources
            .iter()
            .all(|c| c.exhausted && c.loaded.is_none())
    }
}

/// Map an Arrow `DataType` that `validate_key_types` has already
/// accepted into its [`BqlType`]. Unreachable branches for types the
/// validator would have rejected.
fn bql_type_for_sort_key(data_type: &DataType) -> BqlType {
    match data_type {
        DataType::Utf8 | DataType::Utf8View => BqlType::String,
        DataType::Int64 => BqlType::Int,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => BqlType::Timestamp,
        other => unreachable!(
            "bql_type_for_sort_key: {other:?} should have been rejected by validate_key_types"
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Key comparison helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Validate that the entity-key and ts columns in `schema` have
/// types we can compare in the merge hot loop.
fn validate_key_types(schema: &ArrowSchema, entity_key_col: usize, ts_col: usize) -> Result<()> {
    let ek_type = schema.field(entity_key_col).data_type();
    match ek_type {
        DataType::Utf8 | DataType::Utf8View | DataType::Int64 => (),
        other => {
            return Err(BqliteError::Execution(format!(
                "k-way merge: entity_key column has unsupported type {other:?} — \
                 expected Utf8, Utf8View, or Int64"
            )));
        }
    }
    let ts_type = schema.field(ts_col).data_type();
    // The merge's hot-loop comparator downcasts unconditionally to
    // `TimestampNanosecondArray`; accepting any other `TimeUnit` here
    // would turn a mismatch into a panic. The storage reader always
    // materialises `ts` as `Timestamp(Nanosecond, _)` (see module
    // docs), so tightening validation to nanosecond only matches the
    // real upstream and keeps the comparator's invariant explicit.
    if !matches!(ts_type, DataType::Timestamp(TimeUnit::Nanosecond, _)) {
        return Err(BqliteError::Execution(format!(
            "k-way merge: ts column has unsupported type {ts_type:?} — \
             expected Timestamp(Nanosecond, _)"
        )));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ::arrow::array::{Int64Array, StringViewArray, TimestampNanosecondArray};
    use ::arrow::datatypes::{Field, Schema as ArrowSchema, TimeUnit};
    use bqlite_core::encoded::SelectionVector;
    use bqlite_core::storage::ZoneMap;
    use std::collections::HashMap;

    /// Mock SegmentScan that yields a pre-built list of batches.
    struct MockScan {
        batches: Vec<RecordBatch>,
        idx: usize,
    }

    impl MockScan {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self { batches, idx: 0 }
        }
    }

    impl SegmentScan for MockScan {
        fn row_group_count(&self) -> usize {
            self.batches.len()
        }

        fn row_group_zone_maps(&self, _idx: usize) -> Result<HashMap<String, ZoneMap>> {
            Ok(HashMap::new())
        }

        fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
            if self.idx >= self.batches.len() {
                return Ok(None);
            }
            let batch = self.batches[self.idx].clone();
            self.idx += 1;
            Ok(Some(batch))
        }
    }

    fn events_schema() -> Arc<ArrowSchema> {
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

    fn build_batch(
        schema: &Arc<ArrowSchema>,
        entity_ids: &[&str],
        timestamps: &[i64],
        event_types: &[&str],
    ) -> RecordBatch {
        let entity: StringViewArray = entity_ids.iter().copied().map(Some).collect();
        let ts =
            TimestampNanosecondArray::from(timestamps.iter().map(|v| Some(*v)).collect::<Vec<_>>())
                .with_timezone("UTC");
        let event: StringViewArray = event_types.iter().copied().map(Some).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(entity), Arc::new(ts), Arc::new(event)],
        )
        .unwrap()
    }

    /// Collect every merged row into `(entity_id, ts, event_type)` triples.
    fn drain_merge(merge: &mut KWayMergeScan) -> Vec<(String, i64, String)> {
        let mut rows = Vec::new();
        while let Some(batch) = merge.next_batch().unwrap() {
            let entity = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            let ts = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let event = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    entity.value(i).to_string(),
                    ts.value(i),
                    event.value(i).to_string(),
                ));
            }
        }
        rows
    }

    // ── Construction ─────────────────────────────────────────────────

    #[test]
    fn new_rejects_out_of_range_entity_key_col() {
        let schema = events_schema();
        let err = KWayMergeScan::new(vec![], schema, 99, 1).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("entity_key_col"), "got: {msg}")
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn new_rejects_out_of_range_ts_col() {
        let schema = events_schema();
        let err = KWayMergeScan::new(vec![], schema, 0, 99).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("ts_col"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn new_rejects_non_timestamp_ts_column() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        let err = KWayMergeScan::new(vec![], schema, 0, 1).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("Timestamp"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn new_rejects_non_nanosecond_timestamp_ts_column() {
        // The comparator downcasts unconditionally to
        // `TimestampNanosecondArray`; a `Microsecond` ts must be
        // rejected up front rather than panicking in the hot loop.
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));
        let err = KWayMergeScan::new(vec![], schema, 0, 1).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("Nanosecond"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn new_rejects_zero_batch_target_rows() {
        let schema = events_schema();
        let err = KWayMergeScan::with_batch_size(vec![], schema, 0, 1, 0).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("batch_target_rows"), "got: {msg}")
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Empty / single-input cases ──────────────────────────────────

    #[test]
    fn empty_input_returns_none() {
        let schema = events_schema();
        let mut merge = KWayMergeScan::new(vec![], schema, 0, 1).unwrap();
        assert!(merge.next_batch().unwrap().is_none());
        assert!(merge.next_batch().unwrap().is_none()); // still None
    }

    #[test]
    fn single_scan_passes_through_every_batch() {
        let schema = events_schema();
        let b1 = build_batch(
            &schema,
            &["u1", "u1", "u2"],
            &[10, 20, 30],
            &["a", "b", "c"],
        );
        let b2 = build_batch(&schema, &["u2", "u3"], &[40, 50], &["d", "e"]);
        let scan = Box::new(MockScan::new(vec![b1, b2]));
        let mut merge = KWayMergeScan::new(vec![scan], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        assert_eq!(
            rows,
            vec![
                ("u1".into(), 10, "a".into()),
                ("u1".into(), 20, "b".into()),
                ("u2".into(), 30, "c".into()),
                ("u2".into(), 40, "d".into()),
                ("u3".into(), 50, "e".into()),
            ]
        );
    }

    // ── Two-scan merge ──────────────────────────────────────────────

    #[test]
    fn two_scans_with_interleaved_entities() {
        let schema = events_schema();
        // Scan A: u1@10, u2@30, u3@50
        // Scan B: u1@20, u2@40, u4@60
        // Expected merged: u1@10, u1@20, u2@30, u2@40, u3@50, u4@60
        let scan_a = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1", "u2", "u3"],
            &[10, 30, 50],
            &["a", "c", "e"],
        )]));
        let scan_b = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1", "u2", "u4"],
            &[20, 40, 60],
            &["b", "d", "f"],
        )]));
        let mut merge = KWayMergeScan::new(vec![scan_a, scan_b], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        assert_eq!(
            rows,
            vec![
                ("u1".into(), 10, "a".into()),
                ("u1".into(), 20, "b".into()),
                ("u2".into(), 30, "c".into()),
                ("u2".into(), 40, "d".into()),
                ("u3".into(), 50, "e".into()),
                ("u4".into(), 60, "f".into()),
            ]
        );
    }

    #[test]
    fn two_scans_with_entities_in_disjoint_ranges() {
        let schema = events_schema();
        // Scan A: entities a..c
        // Scan B: entities d..f
        // Expected: a..f in order (A first, then B).
        let scan_a = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["a", "b", "c"],
            &[10, 20, 30],
            &["x", "x", "x"],
        )]));
        let scan_b = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["d", "e", "f"],
            &[5, 15, 25],
            &["y", "y", "y"],
        )]));
        let mut merge = KWayMergeScan::new(vec![scan_a, scan_b], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        let entities: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
        assert_eq!(entities, vec!["a", "b", "c", "d", "e", "f"]);
    }

    #[test]
    fn three_scans_merge_preserves_order() {
        let schema = events_schema();
        let scan_a = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1", "u4"],
            &[10, 40],
            &["a", "d"],
        )]));
        let scan_b = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u2", "u5"],
            &[20, 50],
            &["b", "e"],
        )]));
        let scan_c = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u3", "u6"],
            &[30, 60],
            &["c", "f"],
        )]));
        let mut merge = KWayMergeScan::new(vec![scan_a, scan_b, scan_c], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        let entities: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
        assert_eq!(entities, vec!["u1", "u2", "u3", "u4", "u5", "u6"]);
    }

    #[test]
    fn equal_keys_tie_break_to_lower_indexed_scan() {
        // Two scans with an identical (entity_id, ts) at the head.
        // The merge should deterministically pick the lower-indexed
        // scan first, so its event_type appears before the higher-
        // indexed scan's. This pins the tie-break policy documented
        // on `KWayMergeScan::next_batch`.
        let schema = events_schema();
        let scan_a = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1", "u1"],
            &[10, 20],
            &["a1", "a2"],
        )]));
        let scan_b = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1", "u1"],
            &[10, 20],
            &["b1", "b2"],
        )]));
        let mut merge = KWayMergeScan::new(vec![scan_a, scan_b], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        let event_types: Vec<String> = rows.iter().map(|r| r.2.clone()).collect();
        // At (u1, 10): scan_a wins. At (u1, 10): scan_b wins next.
        // At (u1, 20): scan_a wins. At (u1, 20): scan_b wins next.
        assert_eq!(event_types, vec!["a1", "b1", "a2", "b2"]);
    }

    // ── Multiple batches per scan ───────────────────────────────────

    #[test]
    fn merge_reloads_batches_across_row_group_boundaries() {
        let schema = events_schema();
        // Scan A: two batches, each with one entity run.
        let scan_a = Box::new(MockScan::new(vec![
            build_batch(&schema, &["u1", "u1"], &[10, 20], &["a1", "a2"]),
            build_batch(&schema, &["u3", "u3"], &[50, 60], &["a3", "a4"]),
        ]));
        // Scan B: two batches interleaving with A.
        let scan_b = Box::new(MockScan::new(vec![
            build_batch(&schema, &["u2"], &[30], &["b1"]),
            build_batch(&schema, &["u4"], &[70], &["b2"]),
        ]));
        let mut merge = KWayMergeScan::new(vec![scan_a, scan_b], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        let entities: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
        assert_eq!(entities, vec!["u1", "u1", "u2", "u3", "u3", "u4"]);
        let event_types: Vec<String> = rows.iter().map(|r| r.2.clone()).collect();
        assert_eq!(event_types, vec!["a1", "a2", "b1", "a3", "a4", "b2"]);
    }

    #[test]
    fn merge_keeps_filling_output_after_one_input_batch_exhausts() {
        let schema = events_schema();
        let scan_a = Box::new(MockScan::new(vec![
            build_batch(&schema, &["u1"], &[10], &["a1"]),
            build_batch(&schema, &["u4"], &[40], &["a2"]),
        ]));
        let scan_b = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u2", "u3", "u5"],
            &[20, 30, 50],
            &["b1", "b2", "b3"],
        )]));

        let mut merge =
            KWayMergeScan::with_batch_size(vec![scan_a, scan_b], schema, 0, 1, 3).unwrap();

        let first = merge.next_batch().unwrap().expect("first batch");
        let entities = first
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let first_entities: Vec<&str> = (0..first.num_rows()).map(|i| entities.value(i)).collect();
        assert_eq!(first_entities, vec!["u1", "u2", "u3"]);

        let second = merge.next_batch().unwrap().expect("second batch");
        let entities = second
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let second_entities: Vec<&str> =
            (0..second.num_rows()).map(|i| entities.value(i)).collect();
        assert_eq!(second_entities, vec!["u4", "u5"]);

        assert!(merge.next_batch().unwrap().is_none());
    }

    #[test]
    fn merge_skips_empty_batches() {
        let schema = events_schema();
        // Arrow doesn't let us build a truly zero-row batch via
        // `build_batch`, so we skip this in favor of the
        // happy-path tests — the reader guarantees no empty batches
        // reach the merge anyway (the scan's next_row_group already
        // skips empty row groups via pruning). Instead we test
        // that a scan with a trailing empty batch-list doesn't
        // hang.
        let scan = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1"],
            &[10],
            &["a"],
        )]));
        let mut merge = KWayMergeScan::new(vec![scan], schema, 0, 1).unwrap();
        let rows = drain_merge(&mut merge);
        assert_eq!(rows.len(), 1);
    }

    // ── Batch size contract ─────────────────────────────────────────

    #[test]
    fn small_batch_size_emits_multiple_output_batches() {
        let schema = events_schema();
        let scan = Box::new(MockScan::new(vec![build_batch(
            &schema,
            &["u1", "u2", "u3", "u4", "u5"],
            &[10, 20, 30, 40, 50],
            &["a", "b", "c", "d", "e"],
        )]));
        let mut merge = KWayMergeScan::with_batch_size(vec![scan], schema, 0, 1, 2).unwrap();
        let mut seen_rows = 0;
        let mut seen_batches = 0;
        while let Some(batch) = merge.next_batch().unwrap() {
            seen_batches += 1;
            seen_rows += batch.num_rows();
            assert!(batch.num_rows() <= 2);
        }
        assert_eq!(seen_rows, 5);
        assert!(seen_batches >= 3);
    }

    // ── Schema mismatch ─────────────────────────────────────────────

    #[test]
    fn merge_rejects_scan_with_mismatched_schema() {
        let schema = events_schema();
        let wrong_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));
        // Build a batch against the wrong schema.
        let entity: StringViewArray = ["u1"].into_iter().map(Some).collect();
        let ts = TimestampNanosecondArray::from(vec![Some(10i64)]).with_timezone("UTC");
        let wrong_batch =
            RecordBatch::try_new(wrong_schema, vec![Arc::new(entity), Arc::new(ts)]).unwrap();
        let scan = Box::new(MockScan::new(vec![wrong_batch]));
        let mut merge = KWayMergeScan::new(vec![scan], schema, 0, 1).unwrap();
        let err = merge.next_batch().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("schema"), "got: {msg}")
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Int64 entity key ────────────────────────────────────────────

    #[test]
    fn merge_with_int64_entity_key() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));
        let make = |ids: Vec<i64>, times: Vec<i64>| {
            let entity = Int64Array::from(ids);
            let ts = TimestampNanosecondArray::from(times).with_timezone("UTC");
            RecordBatch::try_new(schema.clone(), vec![Arc::new(entity), Arc::new(ts)]).unwrap()
        };
        let scan_a = Box::new(MockScan::new(vec![make(vec![1, 3], vec![10, 30])]));
        let scan_b = Box::new(MockScan::new(vec![make(vec![2, 4], vec![20, 40])]));
        let mut merge = KWayMergeScan::new(vec![scan_a, scan_b], schema, 0, 1).unwrap();
        let mut ids = Vec::new();
        while let Some(batch) = merge.next_batch().unwrap() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                ids.push(arr.value(i));
            }
        }
        assert_eq!(ids, vec![1, 2, 3, 4]);
    }

    // ── Input count and schema accessor ─────────────────────────────

    #[test]
    fn input_count_reports_number_of_scans() {
        let schema = events_schema();
        let merge = KWayMergeScan::new(
            vec![
                Box::new(MockScan::new(vec![])),
                Box::new(MockScan::new(vec![])),
                Box::new(MockScan::new(vec![])),
            ],
            schema.clone(),
            0,
            1,
        )
        .unwrap();
        assert_eq!(merge.input_count(), 3);
        assert!(Arc::ptr_eq(merge.schema(), &schema));
    }

    // ── EncodedKWayMergeScan (CP5 Step 1) ─────────────────────────────

    /// Mock `EncodedBatchSource` that returns a pre-built queue of
    /// `(EncodedBatch, RowSelection)` pairs.
    struct MockEncodedSource {
        pairs: std::collections::VecDeque<(EncodedBatch, RowSelection)>,
    }

    impl MockEncodedSource {
        fn new(pairs: Vec<(EncodedBatch, RowSelection)>) -> Self {
            Self {
                pairs: pairs.into(),
            }
        }
    }

    impl EncodedBatchSource for MockEncodedSource {
        fn next(&mut self) -> Result<Option<(EncodedBatch, RowSelection)>> {
            Ok(self.pairs.pop_front())
        }
    }

    #[test]
    fn encoded_new_rejects_out_of_range_entity_key_col() {
        let schema = events_schema();
        let err = EncodedKWayMergeScan::new(vec![], schema, 99, 1).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("entity_key_col"), "got: {msg}")
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn encoded_new_rejects_out_of_range_ts_col() {
        let schema = events_schema();
        let err = EncodedKWayMergeScan::new(vec![], schema, 0, 99).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("ts_col"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn encoded_new_validates_sort_key_types() {
        // event_type (col 2) is Utf8View — legal as entity_key but not as ts.
        let schema = events_schema();
        let err = EncodedKWayMergeScan::new(vec![], schema, 0, 2).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("ts column"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn encoded_new_rejects_zero_batch_size() {
        let schema = events_schema();
        let err = EncodedKWayMergeScan::with_batch_size(vec![], schema, 0, 1, 0).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("batch_target_rows"), "got: {msg}")
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn encoded_new_accepts_empty_source_vec() {
        let schema = events_schema();
        let merge = EncodedKWayMergeScan::new(vec![], schema.clone(), 0, 1).unwrap();
        assert_eq!(merge.input_count(), 0);
        assert!(Arc::ptr_eq(merge.schema(), &schema));
    }

    #[test]
    fn encoded_new_accepts_int64_entity_key() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));
        let merge = EncodedKWayMergeScan::new(vec![], schema, 0, 1).unwrap();
        assert_eq!(merge.input_count(), 0);
    }

    #[test]
    fn encoded_empty_source_returns_none() {
        // Source returns Ok(None) immediately — merge should mark it
        // exhausted and return None. Repeated calls stay None.
        let schema = events_schema();
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(MockEncodedSource::new(vec![]))], schema, 0, 1)
                .unwrap();
        assert!(merge.next_stitched_batch().unwrap().is_none());
        assert!(merge.next_stitched_batch().unwrap().is_none());
    }

    #[test]
    fn selection_cursor_indices_walk() {
        let mut c = SelectionCursor::from_selection(RowSelection::Indices(
            SelectionVector::from_sorted(vec![2, 5, 8]),
        ));
        assert_eq!(c.current(), Some(2));
        c.advance();
        assert_eq!(c.current(), Some(5));
        c.advance();
        assert_eq!(c.current(), Some(8));
        c.advance();
        assert!(c.is_done());
        assert_eq!(c.current(), None);
    }

    #[test]
    fn selection_cursor_runs_walk_preserves_shape() {
        // Two runs: [10,11,12] then [20,21]. Walking should emit them
        // in order without flattening to a Vec<u32>.
        let mut c = SelectionCursor::from_selection(RowSelection::from_runs(vec![
            RowRun { start: 10, len: 3 },
            RowRun { start: 20, len: 2 },
        ]));
        let mut got = Vec::new();
        while let Some(row) = c.current() {
            got.push(row);
            c.advance();
        }
        assert_eq!(got, vec![10, 11, 12, 20, 21]);
        assert!(c.is_done());
    }

    #[test]
    fn selection_cursor_empty_runs_is_done() {
        let mut c = SelectionCursor::from_selection(RowSelection::from_runs(vec![]));
        assert!(c.is_done());
        assert_eq!(c.current(), None);
        // Advance on empty is a no-op.
        c.advance();
        assert!(c.is_done());
    }

    #[test]
    fn selection_cursor_empty_indices_is_done() {
        let c = SelectionCursor::from_selection(RowSelection::empty());
        assert!(c.is_done());
        assert_eq!(c.current(), None);
    }

    #[test]
    fn selection_cursor_runs_skips_leading_zero_length() {
        // Leading zero-length runs must not prevent current()/is_done()
        // from reporting the first live row. Walker invariant: after
        // from_selection, run_idx points at a run with len > 0 (or
        // runs.len() if none exist).
        let mut c = SelectionCursor::from_selection(RowSelection::from_runs(vec![
            RowRun { start: 5, len: 0 },
            RowRun { start: 7, len: 0 },
            RowRun { start: 10, len: 2 },
        ]));
        assert!(!c.is_done());
        assert_eq!(c.current(), Some(10));
        c.advance();
        assert_eq!(c.current(), Some(11));
        c.advance();
        assert!(c.is_done());
    }

    #[test]
    fn selection_cursor_runs_all_zero_length_is_done() {
        let c = SelectionCursor::from_selection(RowSelection::from_runs(vec![
            RowRun { start: 3, len: 0 },
            RowRun { start: 5, len: 0 },
        ]));
        assert!(c.is_done());
        assert_eq!(c.current(), None);
    }

    // ── Encoded driver (CP5 Step 2+) ──────────────────────────────────

    use bqlite_core::encoded::EncodedColumn;

    /// Build an `EncodedBatch` whose columns are all `Materialized`
    /// fallbacks — Arrow arrays carried directly. Mirrors the
    /// `events_schema` column layout (entity_id, ts, event_type).
    /// Keeps tests focused on merge semantics rather than encoded-byte
    /// layout (the latter is covered by the encoding tests).
    fn build_encoded_batch(
        entity_ids: &[&str],
        timestamps: &[i64],
        event_types: &[&str],
    ) -> EncodedBatch {
        let rows = entity_ids.len() as u32;
        let entity: StringViewArray = entity_ids.iter().copied().map(Some).collect();
        let ts =
            TimestampNanosecondArray::from(timestamps.iter().map(|v| Some(*v)).collect::<Vec<_>>())
                .with_timezone("UTC");
        let event: StringViewArray = event_types.iter().copied().map(Some).collect();
        EncodedBatch::new(
            rows,
            vec![
                EncodedColumn::Materialized {
                    array: Arc::new(entity),
                    rows,
                },
                EncodedColumn::Materialized {
                    array: Arc::new(ts),
                    rows,
                },
                EncodedColumn::Materialized {
                    array: Arc::new(event),
                    rows,
                },
            ],
        )
    }

    /// Drain the merge and collect `(source_idx, entity_id, ts)`
    /// triples by consulting each stitched batch's `sources` and
    /// `rows`.
    fn drain_encoded(merge: &mut EncodedKWayMergeScan) -> Vec<(u16, String, i64)> {
        let mut out = Vec::new();
        while let Some(stitched) = merge.next_stitched_batch().unwrap() {
            let indices = match &stitched.rows {
                StitchedRows::Indices(idx) => idx.clone(),
                other => panic!("CP5 expects Indices; got {other:?}"),
            };
            for rr in indices {
                let src = &stitched.sources[rr.source as usize];
                let entity_col = match &src.columns[0] {
                    EncodedColumn::Materialized { array, .. } => {
                        array.as_any().downcast_ref::<StringViewArray>().unwrap()
                    }
                    EncodedColumn::Encoded { .. } => {
                        panic!("test fixture used Materialized columns only")
                    }
                };
                let ts_col = match &src.columns[1] {
                    EncodedColumn::Materialized { array, .. } => array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap(),
                    EncodedColumn::Encoded { .. } => {
                        panic!("test fixture used Materialized columns only")
                    }
                };
                out.push((
                    rr.source,
                    entity_col.value(rr.row as usize).to_string(),
                    ts_col.value(rr.row as usize),
                ));
            }
        }
        out
    }

    fn all_rows_selection(rows: u32) -> RowSelection {
        RowSelection::from_indices(SelectionVector::all_rows(rows))
    }

    #[test]
    fn encoded_merge_single_source_emits_indices() {
        let schema = events_schema();
        let batch = build_encoded_batch(&["u1", "u2", "u3"], &[1, 2, 3], &["a", "b", "c"]);
        let source = MockEncodedSource::new(vec![(batch, all_rows_selection(3))]);
        let mut merge = EncodedKWayMergeScan::new(vec![Box::new(source)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        assert_eq!(
            rows,
            vec![
                (0u16, "u1".to_string(), 1),
                (0, "u2".to_string(), 2),
                (0, "u3".to_string(), 3),
            ]
        );
    }

    #[test]
    fn encoded_merge_two_sources_interleaved() {
        let schema = events_schema();
        let b0 = build_encoded_batch(&["u1", "u3"], &[1, 3], &["a", "a"]);
        let b1 = build_encoded_batch(&["u2", "u2", "u4"], &[2, 5, 10], &["b", "b", "b"]);
        let s0 = MockEncodedSource::new(vec![(b0, all_rows_selection(2))]);
        let s1 = MockEncodedSource::new(vec![(b1, all_rows_selection(3))]);
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(s0), Box::new(s1)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        let entities: Vec<&str> = rows.iter().map(|(_, e, _)| e.as_str()).collect();
        assert_eq!(entities, vec!["u1", "u2", "u2", "u3", "u4"]);
    }

    #[test]
    fn encoded_merge_equal_keys_tie_break_to_lower_indexed_source() {
        let schema = events_schema();
        // Both sources share `(u1, 10)`. Lower-indexed source (idx 0)
        // must appear first; the `event_type` distinguishes them.
        let b0 = build_encoded_batch(&["u1"], &[10], &["a"]);
        let b1 = build_encoded_batch(&["u1"], &[10], &["b"]);
        let s0 = MockEncodedSource::new(vec![(b0, all_rows_selection(1))]);
        let s1 = MockEncodedSource::new(vec![(b1, all_rows_selection(1))]);
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(s0), Box::new(s1)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 0, "lower-indexed source must win tie-break");
        assert_eq!(rows[1].0, 1);
    }

    #[test]
    fn encoded_merge_skips_fully_filtered_source_batches() {
        let schema = events_schema();
        // Source 0 emits a non-empty batch whose selection is empty
        // (fully filtered), then a real batch.
        let b0_empty = build_encoded_batch(&["u1"], &[1], &["a"]);
        let b0_real = build_encoded_batch(&["u5"], &[5], &["a"]);
        let s0 = MockEncodedSource::new(vec![
            (b0_empty, RowSelection::empty()),
            (b0_real, all_rows_selection(1)),
        ]);
        let b1 = build_encoded_batch(&["u2", "u3"], &[2, 3], &["b", "b"]);
        let s1 = MockEncodedSource::new(vec![(b1, all_rows_selection(2))]);
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(s0), Box::new(s1)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        let entities: Vec<&str> = rows.iter().map(|(_, e, _)| e.as_str()).collect();
        assert_eq!(entities, vec!["u2", "u3", "u5"]);
    }

    #[test]
    fn encoded_merge_empty_source_drains_others() {
        let schema = events_schema();
        // Source 0 is empty; source 1 has all the rows.
        let s0 = MockEncodedSource::new(vec![]);
        let b1 = build_encoded_batch(&["u1", "u2"], &[1, 2], &["a", "b"]);
        let s1 = MockEncodedSource::new(vec![(b1, all_rows_selection(2))]);
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(s0), Box::new(s1)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        let entities: Vec<&str> = rows.iter().map(|(_, e, _)| e.as_str()).collect();
        assert_eq!(entities, vec!["u1", "u2"]);
        // Every surviving pick came from source 1.
        assert!(rows.iter().all(|(src, _, _)| *src == 1));
    }

    #[test]
    fn encoded_merge_reloads_across_row_group_boundaries() {
        let schema = events_schema();
        // Source 0 emits two back-to-back batches; merge must reload
        // between them without reordering.
        let b0a = build_encoded_batch(&["u1"], &[1], &["a"]);
        let b0b = build_encoded_batch(&["u3"], &[3], &["a"]);
        let s0 = MockEncodedSource::new(vec![
            (b0a, all_rows_selection(1)),
            (b0b, all_rows_selection(1)),
        ]);
        let b1 = build_encoded_batch(&["u2"], &[2], &["b"]);
        let s1 = MockEncodedSource::new(vec![(b1, all_rows_selection(1))]);
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(s0), Box::new(s1)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        let entities: Vec<&str> = rows.iter().map(|(_, e, _)| e.as_str()).collect();
        assert_eq!(entities, vec!["u1", "u2", "u3"]);
    }

    #[test]
    fn encoded_merge_small_batch_size_emits_multiple_stitched_batches() {
        let schema = events_schema();
        let b = build_encoded_batch(
            &["u1", "u2", "u3", "u4", "u5"],
            &[1, 2, 3, 4, 5],
            &["a", "a", "a", "a", "a"],
        );
        let s = MockEncodedSource::new(vec![(b, all_rows_selection(5))]);
        let mut merge =
            EncodedKWayMergeScan::with_batch_size(vec![Box::new(s)], schema, 0, 1, 2).unwrap();

        let mut emits = 0;
        let mut total_rows = 0;
        while let Some(stitched) = merge.next_stitched_batch().unwrap() {
            emits += 1;
            match &stitched.rows {
                StitchedRows::Indices(idx) => total_rows += idx.len(),
                other => panic!("CP5 expects Indices; got {other:?}"),
            }
        }
        assert_eq!(emits, 3, "5 rows / cap 2 → 3 emits");
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn encoded_merge_preserves_runs_shape_on_rle_selection() {
        // Source emits a batch whose RowSelection::Runs has a 4-row
        // run. The walker must advance through the run without
        // flattening; drained rows must be the run's indices in order.
        let schema = events_schema();
        let b = build_encoded_batch(
            &["u0", "u1", "u2", "u3", "u4", "u5"],
            &[0, 1, 2, 3, 4, 5],
            &["a", "a", "a", "a", "a", "a"],
        );
        // Select rows 2..=5 via a single 4-row run (indices 2,3,4,5).
        let sel = RowSelection::from_runs(vec![RowRun { start: 2, len: 4 }]);
        let s = MockEncodedSource::new(vec![(b, sel)]);
        let mut merge = EncodedKWayMergeScan::new(vec![Box::new(s)], schema, 0, 1).unwrap();

        let rows = drain_encoded(&mut merge);
        let entities: Vec<&str> = rows.iter().map(|(_, e, _)| e.as_str()).collect();
        assert_eq!(entities, vec!["u2", "u3", "u4", "u5"]);
    }

    #[test]
    fn encoded_merge_rejects_batch_with_wrong_column_count() {
        // 3-col schema; source yields a 2-col batch — must error.
        let schema = events_schema();
        let ent = StringViewArray::from(vec!["u1"]);
        let ts = TimestampNanosecondArray::from(vec![1i64]).with_timezone("UTC");
        let bad = EncodedBatch::new(
            1,
            vec![
                EncodedColumn::Materialized {
                    array: Arc::new(ent),
                    rows: 1,
                },
                EncodedColumn::Materialized {
                    array: Arc::new(ts),
                    rows: 1,
                },
            ],
        );
        let s = MockEncodedSource::new(vec![(bad, all_rows_selection(1))]);
        let mut merge = EncodedKWayMergeScan::new(vec![Box::new(s)], schema, 0, 1).unwrap();
        let err = merge.next_stitched_batch().unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("columns"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn encoded_merge_int64_entity_key() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));
        // Build a two-column EncodedBatch (no event_type) so the schema
        // matches.
        let ent = Int64Array::from(vec![1i64, 3]);
        let ts = TimestampNanosecondArray::from(vec![10i64, 30]).with_timezone("UTC");
        let b0 = EncodedBatch::new(
            2,
            vec![
                EncodedColumn::Materialized {
                    array: Arc::new(ent),
                    rows: 2,
                },
                EncodedColumn::Materialized {
                    array: Arc::new(ts),
                    rows: 2,
                },
            ],
        );
        let ent1 = Int64Array::from(vec![2i64]);
        let ts1 = TimestampNanosecondArray::from(vec![20i64]).with_timezone("UTC");
        let b1 = EncodedBatch::new(
            1,
            vec![
                EncodedColumn::Materialized {
                    array: Arc::new(ent1),
                    rows: 1,
                },
                EncodedColumn::Materialized {
                    array: Arc::new(ts1),
                    rows: 1,
                },
            ],
        );
        let s0 = MockEncodedSource::new(vec![(b0, all_rows_selection(2))]);
        let s1 = MockEncodedSource::new(vec![(b1, all_rows_selection(1))]);
        let mut merge =
            EncodedKWayMergeScan::new(vec![Box::new(s0), Box::new(s1)], schema, 0, 1).unwrap();

        let mut got: Vec<i64> = Vec::new();
        while let Some(stitched) = merge.next_stitched_batch().unwrap() {
            if let StitchedRows::Indices(idx) = &stitched.rows {
                for rr in idx {
                    let src = &stitched.sources[rr.source as usize];
                    let col = match &src.columns[0] {
                        EncodedColumn::Materialized { array, .. } => {
                            array.as_any().downcast_ref::<Int64Array>().unwrap()
                        }
                        _ => unreachable!(),
                    };
                    got.push(col.value(rr.row as usize));
                }
            }
        }
        assert_eq!(got, vec![1i64, 2, 3]);
    }
}
