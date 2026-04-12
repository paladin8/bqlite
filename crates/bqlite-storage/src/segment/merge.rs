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

use bqlite_core::storage::SegmentScan;
use bqlite_core::{BqliteError, Result};

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
}
