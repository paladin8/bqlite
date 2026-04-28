//! `SegmentScan` decorators that apply tombstone filtering per row group.
//!
//! This module hosts two wrappers that differ in how they derive the
//! per-row `__seq_id` / `__batch_id` used by row- and batch-level
//! tombstone checks:
//!
//! - [`TombstoneScanWrapper`] — the query-time wrapper. Applies
//!   [`TombstoneFilter`] from `deletes.md` §7 against row-group batches
//!   produced by a normal `ScanOperator`. Carries the segment's
//!   `seq_id_first` and `batch_id` from manifest metadata
//!   (`SegmentHandle::seq_id_first` / `SegmentHandle::batch_id`) so it
//!   can synthesise the system columns when the scan projection omits
//!   them: row- and batch-level tombstone checks need those values even
//!   when the downstream query does not reference `__seq_id` /
//!   `__batch_id` directly.  Synthesised columns are stripped from the
//!   returned batch so callers never observe extra columns.
//! - [`CompactionTombstoneScan`] — the compaction-time wrapper. Also
//!   carries `seq_id_first` and `batch_id` but derives per-row
//!   `__seq_id` without materialised columns (compaction-time segments
//!   do not include system columns in their scan output). Used by
//!   `compact_one` to physically reclaim tombstoned rows. See
//!   `deletes.md` §12.
//!
//! Both wrappers preserve the scan-pipeline ordering: column projection
//! → zone-map pushdown → **tombstone filtering** → merge → post-filter
//! → operators.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;

use bqlite_core::error::Result;
use bqlite_core::metrics::Metrics;
use bqlite_core::schema::{BATCH_ID_COLUMN, SEQ_ID_COLUMN};
use bqlite_core::{BqliteError, SegmentScan, ZoneMap};

use crate::tombstone::{EntityDeleteIndex, TombstoneFile, TombstoneFilter};

/// A [`SegmentScan`] that removes tombstoned rows from every row group
/// before handing them to the merge layer.
///
/// Zone-map metadata is delegated directly to the inner scan — tombstone
/// filtering does not affect zone-map semantics because zone maps
/// describe the *unfiltered* data and are used by predicate pushdown
/// which runs before tombstone checks.
///
/// ## System-column synthesis
///
/// When `__seq_id` or `__batch_id` are absent from a row-group batch
/// (because the scan projection omitted them), this wrapper synthesises
/// them from `seq_id_first` / `batch_id` metadata before applying the
/// tombstone filter, then strips those synthetic columns from the
/// returned batch so downstream operators see the schema they expect.
pub struct TombstoneScanWrapper {
    inner: Box<dyn SegmentScan>,
    tombstones: TombstoneFile,
    entity_key_col: String,
    ts_col: String,
    /// First `__seq_id` value for this segment (storage-format §6.2).
    /// Row at segment-relative offset `n` has `__seq_id = seq_id_first + n`.
    seq_id_first: u64,
    /// Ingest batch identifier for this segment (storage-format §6.2).
    /// All rows share the same `__batch_id`.
    batch_id: u64,
    /// Cumulative rows yielded by the inner scan across all previous
    /// `next_row_group` calls.  Used to derive the `__seq_id` of rows
    /// in subsequent batches when synthesis is needed.
    next_row_offset: u64,
    /// Type-specialized entity-delete sets, computed once at wrap time
    /// and reused across batches (audit P3 #7 — avoids rebuilding the
    /// HashSet inside `apply_entity_deletes` for every row group).
    entity_index: EntityDeleteIndex,
    /// `true` when the segment's `batch_id` is in `tombstones.batch_deletes`.
    /// Computed once at construction; when set, every `next_row_group`
    /// call returns an empty batch immediately.
    all_batch_dropped: bool,
}

impl TombstoneScanWrapper {
    /// Wrap `inner` with tombstone filtering for the given shard's tombstone state.
    ///
    /// `seq_id_first` and `batch_id` are the segment-level identifiers from
    /// [`bqlite_core::SegmentHandle`] / `SegmentMeta`.  They are used to
    /// synthesise `__seq_id` / `__batch_id` values when those columns are not
    /// present in the row-group batch.
    pub fn new(
        inner: Box<dyn SegmentScan>,
        tombstones: TombstoneFile,
        entity_key_col: String,
        ts_col: String,
        seq_id_first: u64,
        batch_id: u64,
    ) -> Self {
        let entity_index = tombstones.entity_delete_index();
        let all_batch_dropped = tombstones.batch_deletes.contains(&batch_id);
        Self {
            inner,
            tombstones,
            entity_key_col,
            ts_col,
            seq_id_first,
            batch_id,
            next_row_offset: 0,
            entity_index,
            all_batch_dropped,
        }
    }
}

impl SegmentScan for TombstoneScanWrapper {
    fn row_group_count(&self) -> usize {
        self.inner.row_group_count()
    }

    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
        self.inner.row_group_zone_maps(idx)
    }

    fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
        // Fast-path: if the entire segment is batch-deleted, exhaust the
        // inner scan without returning any rows.
        if self.all_batch_dropped {
            // Drain all remaining row groups so the inner scan reaches its
            // natural exhaustion state.  Subsequent calls on the already-
            // exhausted inner scan return `Ok(None)` immediately (per the
            // `SegmentScan` contract), so re-entrancy is safe.
            while self.inner.next_row_group()?.is_some() {}
            return Ok(None);
        }

        let Some(batch) = self.inner.next_row_group()? else {
            return Ok(None);
        };

        let num_rows = batch.num_rows();
        let start_offset = self.next_row_offset;
        self.next_row_offset = start_offset.checked_add(num_rows as u64).ok_or_else(|| {
            BqliteError::Execution(
                "TombstoneScanWrapper: cumulative row offset overflowed u64".into(),
            )
        })?;

        if num_rows == 0 {
            return Ok(Some(batch));
        }

        // Determine which system columns need to be synthesised.
        let has_seq_id = batch.schema().column_with_name(SEQ_ID_COLUMN).is_some();
        let has_batch_id = batch.schema().column_with_name(BATCH_ID_COLUMN).is_some();

        let need_seq = !has_seq_id && !self.tombstones.row_deletes.is_empty();
        // Synthesise `__batch_id` whenever batch deletes exist and the column is absent.
        // `TombstoneFilter::apply_batch_deletes` errors when the column is missing, even
        // for segments whose batch_id is not among the deletes.  The `all_batch_dropped`
        // fast-path above already handles the case where this segment IS targeted, so
        // synthesis here produces a constant array of a non-matching value — zero rows
        // are removed, but the filter runs without error.
        let need_batch = !has_batch_id && !self.tombstones.batch_deletes.is_empty();

        if !need_seq && !need_batch {
            // All required system columns are already present (or no deletes
            // need them).  Delegate directly to TombstoneFilter.
            let filter = TombstoneFilter::new(&self.tombstones, &self.entity_key_col, &self.ts_col);
            let filtered = filter.filter_batch_with_index(batch, &self.entity_index)?;
            return Ok(Some(filtered));
        }

        // Synthesise missing system columns and augment the batch.
        let original_col_count = batch.num_columns();
        let mut extra_cols: Vec<(String, ArrayRef)> = Vec::with_capacity(2);

        if need_seq {
            let base = self.seq_id_first.checked_add(start_offset).ok_or_else(|| {
                BqliteError::Execution(
                    "TombstoneScanWrapper: seq_id overflow when synthesising __seq_id".into(),
                )
            })?;
            let ids: Vec<i64> = (0..num_rows as u64).map(|n| (base + n) as i64).collect();
            extra_cols.push((SEQ_ID_COLUMN.to_string(), Arc::new(Int64Array::from(ids))));
        }
        if need_batch {
            let arr = Int64Array::from(vec![self.batch_id as i64; num_rows]);
            extra_cols.push((BATCH_ID_COLUMN.to_string(), Arc::new(arr)));
        }

        // Build the augmented schema and batch.
        let mut schema_fields = batch.schema().fields().to_vec();
        for (name, _) in &extra_cols {
            schema_fields.push(Arc::new(arrow::datatypes::Field::new(
                name.as_str(),
                arrow::datatypes::DataType::Int64,
                false,
            )));
        }
        let aug_schema = Arc::new(arrow::datatypes::Schema::new(schema_fields));
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (_, arr) in &extra_cols {
            columns.push(arr.clone());
        }
        let augmented = RecordBatch::try_new(aug_schema, columns).map_err(|e| {
            BqliteError::Execution(format!(
                "TombstoneScanWrapper: failed to build augmented batch: {e}"
            ))
        })?;

        // Apply tombstone filter on the augmented batch.
        let filter = TombstoneFilter::new(&self.tombstones, &self.entity_key_col, &self.ts_col);
        let filtered = filter.filter_batch_with_index(augmented, &self.entity_index)?;

        // Strip the synthesised columns: project back to the original column
        // count so downstream operators see the schema they expect.
        let indices: Vec<usize> = (0..original_col_count).collect();
        let stripped = filtered.project(&indices).map_err(|e| {
            BqliteError::Execution(format!(
                "TombstoneScanWrapper: failed to project out synthetic columns: {e}"
            ))
        })?;
        Ok(Some(stripped))
    }

    fn attach_metrics(&mut self, metrics: Arc<dyn Metrics>) {
        self.inner.attach_metrics(metrics);
    }
}

/// Per-segment tombstone filter used during compaction.
///
/// Unlike [`TombstoneScanWrapper`], which expects `__seq_id` and
/// `__batch_id` to be materialised as columns on every batch, the
/// compaction scan output today only contains the declared table
/// columns — so row- and batch-level tombstones need out-of-band
/// segment context. This wrapper carries the segment's `batch_id` and
/// `seq_id_first` (both are manifest metadata we already have during
/// compaction) and derives `__seq_id = seq_id_first + cumulative_row_offset`
/// for every row yielded by the inner scan. Entity- and time-range
/// checks reuse the column-based logic from [`TombstoneFilter`] since
/// entity-key and timestamp columns ARE present in the scan output.
///
/// See `docs/design/storage/deletes.md` §12 — compaction is the site
/// where tombstones are physically applied.
pub struct CompactionTombstoneScan {
    inner: Box<dyn SegmentScan>,
    tombstones: TombstoneFile,
    entity_key_col: String,
    ts_col: String,
    /// First `__seq_id` covered by this segment. Row `n` in batch `b`
    /// has `__seq_id = seq_id_first + cumulative_offset(b) + n`.
    seq_id_first: u64,
    /// Rows already yielded by the inner scan, used to derive the
    /// absolute `__seq_id` of rows in subsequent batches.
    next_row_offset: u64,
    /// Cached "entire segment is batch-deleted" flag, computed once
    /// from the tombstone file at construction time.
    all_dropped: bool,
    /// Type-specialized entity-delete sets, computed once at wrap time
    /// and reused across batches (audit P3 #7).
    entity_index: EntityDeleteIndex,
}

impl CompactionTombstoneScan {
    /// Wrap `inner` for compaction-time tombstone filtering.
    ///
    /// `seq_id_first` and `batch_id` come from the segment's
    /// [`crate::manifest::SegmentMeta`] — the manifest metadata is
    /// authoritative for per-segment `__seq_id` / `__batch_id`
    /// derivation (see `docs/design/storage-format.md` §6.2 / §12.3).
    pub fn new(
        inner: Box<dyn SegmentScan>,
        tombstones: TombstoneFile,
        entity_key_col: String,
        ts_col: String,
        seq_id_first: u64,
        batch_id: u64,
    ) -> Self {
        let all_dropped = tombstones.batch_deletes.contains(&batch_id);
        let entity_index = tombstones.entity_delete_index();
        Self {
            inner,
            tombstones,
            entity_key_col,
            ts_col,
            seq_id_first,
            next_row_offset: 0,
            all_dropped,
            entity_index,
        }
    }
}

impl SegmentScan for CompactionTombstoneScan {
    fn row_group_count(&self) -> usize {
        self.inner.row_group_count()
    }

    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
        self.inner.row_group_zone_maps(idx)
    }

    fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
        if self.all_dropped {
            // §12.4 batch-level reclamation: the whole segment is
            // obsolete. Return None immediately; the merger will
            // simply exhaust this input with no rows contributed.
            return Ok(None);
        }
        let Some(batch) = self.inner.next_row_group()? else {
            return Ok(None);
        };
        let num_rows = batch.num_rows();
        let start_offset = self.next_row_offset;
        // A segment with > u64::MAX rows is absurd; a checked_add
        // turns any bug that would silently saturate the offset into
        // a clear runtime error instead of silent data corruption.
        self.next_row_offset = start_offset.checked_add(num_rows as u64).ok_or_else(|| {
            bqlite_core::error::BqliteError::Execution(
                "CompactionTombstoneScan: cumulative row offset overflowed u64".into(),
            )
        })?;
        if num_rows == 0 {
            return Ok(Some(batch));
        }

        let has_row_work = !self.tombstones.row_deletes.is_empty();
        let has_entity_work = !self.tombstones.entity_deletes.is_empty();
        let has_time_work = !self.tombstones.time_range_deletes.is_empty();
        if !has_row_work && !has_entity_work && !has_time_work {
            return Ok(Some(batch));
        }

        let mut alive = vec![true; num_rows];

        // Row-level: derive __seq_id from seq_id_first + row offset.
        // `seq_id_first + start_offset + i` cannot overflow — the
        // `checked_add` above already bounded `next_row_offset`, so
        // `start_offset + i < u64::MAX`. `seq_id_first` is a u64 from
        // the footer, and compaction never produces >2^63-wide segment
        // ranges.
        if has_row_work {
            for (i, flag) in alive.iter_mut().enumerate() {
                if *flag {
                    let seq_id = self.seq_id_first + start_offset + i as u64;
                    if self.tombstones.row_deletes.contains(&seq_id) {
                        *flag = false;
                    }
                }
            }
        }

        // Entity + time-range: reuse [`TombstoneFilter`]'s promoted
        // `pub(crate)` helpers directly against &self.tombstones. Each
        // helper only touches its own granularity's field, so no view
        // clone is needed — the other fields are simply unread.
        if has_entity_work || has_time_work {
            let filter = TombstoneFilter::new(&self.tombstones, &self.entity_key_col, &self.ts_col);
            if has_entity_work {
                filter.apply_entity_deletes_with_index(&batch, &mut alive, &self.entity_index)?;
            }
            if has_time_work {
                filter.apply_time_range_deletes(&batch, &mut alive)?;
            }
        }

        if alive.iter().all(|&a| a) {
            return Ok(Some(batch));
        }
        let mask = arrow::array::BooleanArray::from(alive);
        let filtered = arrow::compute::filter_record_batch(&batch, &mask).map_err(|e| {
            bqlite_core::error::BqliteError::Execution(format!(
                "CompactionTombstoneScan filter_record_batch failed: {e}"
            ))
        })?;
        Ok(Some(filtered))
    }

    fn attach_metrics(&mut self, metrics: Arc<dyn Metrics>) {
        self.inner.attach_metrics(metrics);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringViewArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{Result, ScalarValue, SegmentScan, ZoneMap};

    use crate::tombstone::TombstoneFile;

    use super::TombstoneScanWrapper;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
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
        let schema = test_schema();
        let id_arr: ArrayRef = Arc::new(StringViewArray::from(ids.to_vec()));
        let ts_arr: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(tss.iter().copied().map(Some).collect::<Vec<_>>())
                .with_timezone("UTC"),
        );
        let evt_arr: ArrayRef = Arc::new(StringViewArray::from(evts.to_vec()));
        RecordBatch::try_new(schema, vec![id_arr, ts_arr, evt_arr]).unwrap()
    }

    struct MockScan {
        batches: Vec<RecordBatch>,
        pos: usize,
    }

    impl MockScan {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self { batches, pos: 0 }
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
            if self.pos >= self.batches.len() {
                return Ok(None);
            }
            let batch = self.batches[self.pos].clone();
            self.pos += 1;
            Ok(Some(batch))
        }
    }

    #[test]
    fn passthrough_when_no_tombstones() {
        let batch = make_batch(&["a", "b"], &[100, 200], &["e1", "e2"]);
        let mut wrapper = TombstoneScanWrapper::new(
            Box::new(MockScan::new(vec![batch])),
            TombstoneFile::default(),
            "entity_id".into(),
            "ts".into(),
            0,
            0,
        );
        let result = wrapper.next_row_group().unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
        assert!(wrapper.next_row_group().unwrap().is_none());
    }

    #[test]
    fn filters_entity_deletes() {
        let batch = make_batch(
            &["alice", "bob", "alice"],
            &[100, 200, 300],
            &["e1", "e2", "e3"],
        );
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let mut wrapper = TombstoneScanWrapper::new(
            Box::new(MockScan::new(vec![batch])),
            tf,
            "entity_id".into(),
            "ts".into(),
            0,
            0,
        );
        let result = wrapper.next_row_group().unwrap().unwrap();
        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "bob");
    }

    #[test]
    fn returns_empty_batch_when_all_removed() {
        let batch = make_batch(&["alice"], &[100], &["e1"]);
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let mut wrapper = TombstoneScanWrapper::new(
            Box::new(MockScan::new(vec![batch])),
            tf,
            "entity_id".into(),
            "ts".into(),
            0,
            0,
        );
        let result = wrapper.next_row_group().unwrap().unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn filters_across_multiple_row_groups() {
        let b1 = make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"]);
        let b2 = make_batch(&["carol", "alice"], &[300, 400], &["e3", "e4"]);
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let mut wrapper = TombstoneScanWrapper::new(
            Box::new(MockScan::new(vec![b1, b2])),
            tf,
            "entity_id".into(),
            "ts".into(),
            0,
            0,
        );
        let r1 = wrapper.next_row_group().unwrap().unwrap();
        assert_eq!(r1.num_rows(), 1); // bob survives
        let r2 = wrapper.next_row_group().unwrap().unwrap();
        assert_eq!(r2.num_rows(), 1); // carol survives
        assert!(wrapper.next_row_group().unwrap().is_none());
    }

    #[test]
    fn delegates_row_group_count() {
        let b1 = make_batch(&["a"], &[100], &["e1"]);
        let b2 = make_batch(&["b"], &[200], &["e2"]);
        let wrapper = TombstoneScanWrapper::new(
            Box::new(MockScan::new(vec![b1, b2])),
            TombstoneFile::default(),
            "entity_id".into(),
            "ts".into(),
            0,
            0,
        );
        assert_eq!(wrapper.row_group_count(), 2);
    }

    // ── CompactionTombstoneScan ─────────────────────────────────────────

    use super::CompactionTombstoneScan;
    use crate::tombstone::TimeRangeDelete;
    use std::collections::HashSet;

    fn new_compaction_scan(
        batches: Vec<RecordBatch>,
        tombstones: TombstoneFile,
        seq_id_first: u64,
        batch_id: u64,
    ) -> CompactionTombstoneScan {
        CompactionTombstoneScan::new(
            Box::new(MockScan::new(batches)),
            tombstones,
            "entity_id".into(),
            "ts".into(),
            seq_id_first,
            batch_id,
        )
    }

    #[test]
    fn compaction_passthrough_when_no_tombstones() {
        let batch = make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"]);
        let mut scan = new_compaction_scan(vec![batch.clone()], TombstoneFile::default(), 0, 1);
        let out = scan.next_row_group().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out, batch);
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn compaction_drops_entire_segment_on_batch_delete() {
        let batch = make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"]);
        let tf = TombstoneFile::for_batches([42]);
        let mut scan = new_compaction_scan(vec![batch], tf, 0, 42);
        // First call returns None because the segment's batch_id is
        // tombstoned; the inner scan is never consulted.
        assert!(scan.next_row_group().unwrap().is_none());
        // Subsequent calls remain None — `all_dropped` is a permanent
        // short-circuit for this wrapper.
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn compaction_batch_delete_dominates_other_granularities() {
        // If batch_id is tombstoned, the wrapper returns None without
        // consulting row/entity/time-range tombstones — tests the
        // `all_dropped` priority in `next_row_group`.
        let batch = make_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"]);
        let tf = TombstoneFile {
            batch_deletes: HashSet::from([7]),
            row_deletes: HashSet::from([0, 1]),
            entity_deletes: HashSet::from([ScalarValue::String("alice".into())]),
            time_range_deletes: vec![TimeRangeDelete {
                min_ts: None,
                min_inclusive: false,
                max_ts: Some(500),
                max_inclusive: true,
            }],
        };
        let mut scan = new_compaction_scan(vec![batch], tf, 0, 7);
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn compaction_applies_row_delete_by_derived_seq_id() {
        // Segment spans __seq_id 100..=102. Tombstone 101 (row 1).
        let batch = make_batch(
            &["alice", "bob", "carol"],
            &[100, 200, 300],
            &["e1", "e2", "e3"],
        );
        let tf = TombstoneFile::for_rows([101]);
        let mut scan = new_compaction_scan(vec![batch], tf, 100, 1);
        let out = scan.next_row_group().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let ids = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "alice");
        assert_eq!(ids.value(1), "carol");
    }

    #[test]
    fn compaction_applies_row_delete_across_multiple_row_groups() {
        // seq_id_first = 100. RG1 rows 100, 101; RG2 rows 102, 103.
        // Tombstone seq_id = 102 — second row-group's first row.
        let rg1 = make_batch(&["a", "b"], &[100, 200], &["e1", "e2"]);
        let rg2 = make_batch(&["c", "d"], &[300, 400], &["e3", "e4"]);
        let tf = TombstoneFile::for_rows([102]);
        let mut scan = new_compaction_scan(vec![rg1, rg2], tf, 100, 1);
        let r1 = scan.next_row_group().unwrap().unwrap();
        assert_eq!(r1.num_rows(), 2); // unaffected
        let r2 = scan.next_row_group().unwrap().unwrap();
        assert_eq!(r2.num_rows(), 1);
        let ids = r2
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "d");
    }

    #[test]
    fn compaction_applies_entity_delete() {
        let batch = make_batch(
            &["alice", "bob", "alice"],
            &[100, 200, 300],
            &["e1", "e2", "e3"],
        );
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let mut scan = new_compaction_scan(vec![batch], tf, 0, 1);
        let out = scan.next_row_group().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        let ids = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "bob");
    }

    #[test]
    fn compaction_applies_time_range_delete() {
        let batch = make_batch(
            &["a", "b", "c", "d"],
            &[100, 200, 300, 400],
            &["e1", "e2", "e3", "e4"],
        );
        // Drop everything in [200, 300].
        let tf = TombstoneFile::for_time_range(TimeRangeDelete {
            min_ts: Some(200),
            min_inclusive: true,
            max_ts: Some(300),
            max_inclusive: true,
        });
        let mut scan = new_compaction_scan(vec![batch], tf, 0, 1);
        let out = scan.next_row_group().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let ids = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "a");
        assert_eq!(ids.value(1), "d");
    }

    #[test]
    fn compaction_combines_row_and_entity_deletes() {
        // seq_id 100..=102. Tombstone seq_id 100 (row 0) AND entity "carol" (row 2).
        let batch = make_batch(
            &["alice", "bob", "carol"],
            &[100, 200, 300],
            &["e1", "e2", "e3"],
        );
        let tf = TombstoneFile {
            row_deletes: HashSet::from([100]),
            entity_deletes: HashSet::from([ScalarValue::String("carol".into())]),
            ..Default::default()
        };
        let mut scan = new_compaction_scan(vec![batch], tf, 100, 1);
        let out = scan.next_row_group().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        let ids = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "bob");
    }

    #[test]
    fn compaction_delegates_row_group_count() {
        let b1 = make_batch(&["a"], &[100], &["e1"]);
        let b2 = make_batch(&["b"], &[200], &["e2"]);
        let scan = new_compaction_scan(vec![b1, b2], TombstoneFile::default(), 0, 1);
        assert_eq!(scan.row_group_count(), 2);
    }

    #[test]
    fn compaction_delegates_zone_maps() {
        // MockScan returns an empty zone-map map; the wrapper must
        // forward the call without modification.
        let b1 = make_batch(&["a"], &[100], &["e1"]);
        let scan = new_compaction_scan(vec![b1], TombstoneFile::default(), 0, 1);
        let zm = scan.row_group_zone_maps(0).unwrap();
        assert!(zm.is_empty());
    }
}
