//! Selection-first tombstone filtering for the encoded read path
//! (TASK-517).
//!
//! [`TombstoneScanWrapper`](crate::tombstone_scan::TombstoneScanWrapper)
//! is the materialized-path tombstone filter. It applies
//! [`TombstoneFilter::filter_batch`](crate::tombstone::TombstoneFilter)
//! to every `RecordBatch` returned by `next_row_group`. That wrapper is
//! correct, but it forces a materialization before filter and so
//! defeats the §3.2 copy budget in
//! `docs/design/storage/zero-copy-scan-filter.md`.
//!
//! [`EncodedTombstoneSource`] is the encoded analogue, per §8.4 of the
//! same design doc. It wraps an [`EncodedBatchSource`] and produces a
//! [`RowSelection`] of rows *not* covered by any tombstone, intersected
//! with whatever selection upstream kernels emitted. The encoded merge
//! consumes the resulting `(EncodedBatch, RowSelection)` pair without
//! ever materializing payload columns before filter.
//!
//! # Decode budget
//!
//! The wrapper materializes only the entity-key column (always) and the
//! `ts` column (only when the file carries `time_range_deletes`). Both
//! columns are *also* re-decoded by
//! [`EncodedKWayMergeScan::load_batch`](crate::segment::merge::EncodedKWayMergeScan)
//! for sort-key extraction. We accept the duplicate decode for v1; both
//! decodes go from a pinned chunk into bounded scratch and never
//! produce a payload copy in the §3.2 sense. Sharing the decoded arrays
//! with the merge would require attaching them to the returned
//! `EncodedBatch` and is left as a follow-up.
//!
//! # Selection variant rule
//!
//! The wrapper always emits its tombstone selection in `Runs` form.
//! Entity- and time-range tombstones over entity/ts-sorted segments
//! naturally produce contiguous kept-row runs, and the §7.2 contract
//! ("runs survive composition") prefers Runs through
//! `RowSelection::intersect`. The intersect helper coerces to
//! `Indices` only when upstream is already `Indices`. The fragmented
//! row-delete case (alternating drops) costs ~2x storage in Runs form,
//! but the row-group bound keeps that overhead bounded — and the merge
//! consumer handles either variant identically per §7.3.

use arrow::array::{Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};

use bqlite_core::encoded::{EncodedBatch, RowRun, RowSelection};
use bqlite_core::{BqlType, BqliteError, Result};

use crate::segment::materialize::materialize_encoded_column;
use crate::segment::merge::EncodedBatchSource;
use crate::tombstone::{EntityDeleteIndex, TombstoneFile};

/// Wrap an [`EncodedBatchSource`] with selection-first tombstone
/// filtering. Output rows are exactly those of `inner` minus rows
/// covered by `tombstones`, with the source's `EncodedBatch` payload
/// passed through unchanged.
///
/// Construction takes the segment-level identifiers (`seq_id_first`,
/// `batch_id`) so row- and batch-level tombstones can be resolved
/// without requiring `__seq_id` / `__batch_id` columns in the encoded
/// batch — those columns are not normally projected on the encoded
/// path.
pub struct EncodedTombstoneSource {
    inner: Box<dyn EncodedBatchSource>,
    tombstones: TombstoneFile,
    entity_index: EntityDeleteIndex,
    /// Ordinal of the entity-key column inside every produced
    /// [`EncodedBatch`]. The wrapper materializes this column on every
    /// batch when entity tombstones are present.
    entity_key_col: usize,
    /// Ordinal of the timestamp column inside every produced
    /// [`EncodedBatch`]. Only consulted when `time_range_deletes` is
    /// non-empty; the column is not decoded otherwise.
    ts_col: usize,
    /// Logical type of the entity-key column, used to drive the
    /// materializer.
    entity_bql: BqlType,
    /// First `__seq_id` value in this segment. Row at the segment-
    /// relative offset `n` has `__seq_id = seq_id_first + n`. Mirrors
    /// `TombstoneScanWrapper`'s synthesis path.
    seq_id_first: u64,
    /// Cumulative rows yielded by the inner source across previous
    /// `next` calls. Used to derive `__seq_id` for `row_deletes`
    /// without a materialized column.
    next_row_offset: u64,
    /// Pre-computed flag: this segment's `batch_id` is in
    /// `tombstones.batch_deletes`. When true, every batch returns an
    /// empty selection without decoding non-key columns.
    all_batch_dropped: bool,
}

impl EncodedTombstoneSource {
    /// Wrap `inner` with tombstone filtering for the given shard's
    /// tombstone state.
    ///
    /// `entity_key_col` and `ts_col` are column ordinals in the
    /// inner-source `EncodedBatch`'s column vector. `entity_bql` is the
    /// entity-key column's logical type ([`BqlType::String`] or
    /// [`BqlType::Int`]).
    ///
    /// `seq_id_first` and `batch_id` are segment-level identifiers from
    /// [`SegmentHandle`](bqlite_core::SegmentHandle); they let row- and
    /// batch-level tombstones be resolved without requiring `__seq_id`
    /// / `__batch_id` columns to be projected.
    pub fn new(
        inner: Box<dyn EncodedBatchSource>,
        tombstones: TombstoneFile,
        entity_key_col: usize,
        ts_col: usize,
        entity_bql: BqlType,
        seq_id_first: u64,
        batch_id: u64,
    ) -> Self {
        let entity_index = tombstones.entity_delete_index();
        let all_batch_dropped = tombstones.batch_deletes.contains(&batch_id);
        Self {
            inner,
            tombstones,
            entity_index,
            entity_key_col,
            ts_col,
            entity_bql,
            seq_id_first,
            next_row_offset: 0,
            all_batch_dropped,
        }
    }
}

impl EncodedBatchSource for EncodedTombstoneSource {
    fn next(&mut self) -> Result<Option<(EncodedBatch, RowSelection)>> {
        let Some((batch, upstream_sel)) = self.inner.next()? else {
            return Ok(None);
        };

        let num_rows = batch.row_count;
        let start_offset = self.next_row_offset;
        self.next_row_offset = start_offset.checked_add(num_rows as u64).ok_or_else(|| {
            BqliteError::Execution(
                "EncodedTombstoneSource: cumulative row offset overflowed u64".into(),
            )
        })?;

        // Empty tombstone file → passthrough (no decode).
        if self.tombstones.is_empty() {
            return Ok(Some((batch, upstream_sel)));
        }

        // Whole-segment batch tombstone → empty selection without
        // decoding non-key columns. We still hand back the original
        // batch so the merge cursor stays aligned with the source's
        // position.
        if self.all_batch_dropped {
            return Ok(Some((batch, RowSelection::empty())));
        }

        // Empty upstream selection → nothing for the tombstone filter
        // to do; preserve the upstream emptiness.
        if upstream_sel.is_empty() {
            return Ok(Some((batch, upstream_sel)));
        }

        if num_rows == 0 {
            return Ok(Some((batch, upstream_sel)));
        }

        // Build the alive mask over *all* rows of the batch. The mask
        // is independent of `upstream_sel` — we intersect at the end.
        let mut alive = vec![true; num_rows as usize];

        // Entity-level tombstones.
        if !self.entity_index.is_empty() {
            let entity_col = batch
                .columns
                .get(self.entity_key_col)
                .ok_or_else(|| {
                    BqliteError::Execution(format!(
                        "EncodedTombstoneSource: entity_key_col {} out of range for batch with {} columns",
                        self.entity_key_col,
                        batch.columns.len()
                    ))
                })?;
            let entity_arr = materialize_encoded_column(entity_col, &self.entity_bql)?;
            apply_entity_deletes(&entity_arr, &self.entity_index, &mut alive)?;
        }

        // Time-range tombstones.
        if !self.tombstones.time_range_deletes.is_empty() {
            let ts_col = batch.columns.get(self.ts_col).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "EncodedTombstoneSource: ts_col {} out of range for batch with {} columns",
                    self.ts_col,
                    batch.columns.len()
                ))
            })?;
            let ts_arr = materialize_encoded_column(ts_col, &BqlType::Timestamp)?;
            apply_time_range_deletes(&ts_arr, &self.tombstones, &mut alive)?;
        }

        // Row-level tombstones via synthesized `__seq_id`.
        if !self.tombstones.row_deletes.is_empty() {
            let base = self.seq_id_first.checked_add(start_offset).ok_or_else(|| {
                BqliteError::Execution(
                    "EncodedTombstoneSource: seq_id overflow when synthesising __seq_id".into(),
                )
            })?;
            for (i, flag) in alive.iter_mut().enumerate() {
                if *flag && self.tombstones.row_deletes.contains(&(base + i as u64)) {
                    *flag = false;
                }
            }
        }

        // Convert alive mask to a RowSelection. Always Runs — see
        // module doc §"Selection variant rule"; the intersect helper
        // coerces the result to Indices when upstream is Indices.
        let tomb_sel = mask_to_selection(&alive);

        // If tombstones removed nothing, return the upstream selection
        // unchanged (avoids an O(n) intersect that produces the same
        // result).
        if tomb_sel.len() == alive.len() {
            return Ok(Some((batch, upstream_sel)));
        }

        let intersected = RowSelection::intersect(&upstream_sel, &tomb_sel);
        Ok(Some((batch, intersected)))
    }
}

/// Apply entity-level tombstones to `alive` using a precomputed
/// [`EntityDeleteIndex`].
///
/// Mirrors `TombstoneFilter::apply_entity_deletes_with_index` over
/// `RecordBatch`, but operates on a decoded `ArrayRef` so the encoded
/// path can keep value buffers borrowed from the segment chunk.
fn apply_entity_deletes(
    arr: &dyn Array,
    index: &EntityDeleteIndex,
    alive: &mut [bool],
) -> Result<()> {
    if let Some(sva) = arr.as_any().downcast_ref::<StringViewArray>() {
        for (i, flag) in alive.iter_mut().enumerate() {
            if *flag && index.contains_str(sva.value(i)) {
                *flag = false;
            }
        }
    } else if let Some(ia) = arr.as_any().downcast_ref::<Int64Array>() {
        for (i, flag) in alive.iter_mut().enumerate() {
            if *flag && index.contains_int(ia.value(i)) {
                *flag = false;
            }
        }
    } else {
        return Err(BqliteError::Execution(format!(
            "EncodedTombstoneSource: unsupported entity-key column type {:?}",
            arr.data_type()
        )));
    }
    Ok(())
}

/// Apply time-range tombstones to `alive` against a decoded
/// `TimestampNanosecondArray`.
fn apply_time_range_deletes(
    arr: &dyn Array,
    tombstones: &TombstoneFile,
    alive: &mut [bool],
) -> Result<()> {
    let ts = arr
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "EncodedTombstoneSource: ts column has unexpected type {:?}; expected {:?}",
                arr.data_type(),
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            ))
        })?;
    for (i, flag) in alive.iter_mut().enumerate() {
        if *flag {
            let v = ts.value(i);
            for range in &tombstones.time_range_deletes {
                if range.contains_timestamp(v) {
                    *flag = false;
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Convert a per-row alive mask into a [`RowSelection::Runs`].
///
/// Always emits `Runs`. The §7.2 contract says runs survive composition
/// when both sides are runs — emitting Runs unconditionally preserves
/// downstream RLE-preserving optimisations through the
/// `RowSelection::intersect` call. The §7.3 dual ("Selection size is
/// measured in logical rows, not variant cardinality") guarantees the
/// merge consumer treats the two variants as semantically identical, so
/// the only cost of always-Runs is a small amount of redundant struct
/// header bytes when row-level tombstones produce highly fragmented
/// selections — bounded by the row-group size and a non-issue against
/// the merge's own working set.
fn mask_to_selection(alive: &[bool]) -> RowSelection {
    if alive.is_empty() {
        return RowSelection::empty();
    }

    let mut runs: Vec<RowRun> = Vec::new();
    let mut i = 0u32;
    let len = alive.len();
    while (i as usize) < len {
        if alive[i as usize] {
            let start = i;
            while (i as usize) < len && alive[i as usize] {
                i += 1;
            }
            runs.push(RowRun {
                start,
                len: i - start,
            });
        } else {
            i += 1;
        }
    }

    if runs.is_empty() {
        return RowSelection::empty();
    }
    RowSelection::from_runs(runs)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::encoded::{EncodedColumn, SelectionVector};
    use bqlite_core::scalar::ScalarValue;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// Mock source that yields a queue of pre-built batches.
    struct MockSource {
        batches: VecDeque<(EncodedBatch, RowSelection)>,
    }

    impl MockSource {
        fn new(batches: Vec<(EncodedBatch, RowSelection)>) -> Self {
            Self {
                batches: batches.into(),
            }
        }
    }

    impl EncodedBatchSource for MockSource {
        fn next(&mut self) -> Result<Option<(EncodedBatch, RowSelection)>> {
            Ok(self.batches.pop_front())
        }
    }

    /// Build an `EncodedBatch` with [entity_id (Utf8View), ts (Timestamp ns)]
    /// columns from parallel slices.
    fn build_batch(entities: &[&str], timestamps: &[i64]) -> EncodedBatch {
        assert_eq!(entities.len(), timestamps.len());
        let n = entities.len() as u32;
        let entity_arr: StringViewArray = entities.iter().copied().map(Some).collect();
        let ts_arr =
            TimestampNanosecondArray::from(timestamps.iter().map(|v| Some(*v)).collect::<Vec<_>>())
                .with_timezone("UTC");
        let cols = vec![
            EncodedColumn::Materialized {
                array: Arc::new(entity_arr),
                rows: n,
            },
            EncodedColumn::Materialized {
                array: Arc::new(ts_arr),
                rows: n,
            },
        ];
        EncodedBatch::new(n, cols)
    }

    fn full_selection(n: u32) -> RowSelection {
        RowSelection::from_runs(vec![RowRun { start: 0, len: n }])
    }

    fn drain(src: &mut EncodedTombstoneSource) -> Vec<RowSelection> {
        let mut out = Vec::new();
        while let Some((_batch, sel)) = src.next().unwrap() {
            out.push(sel);
        }
        out
    }

    fn collect_indices(sel: &RowSelection) -> Vec<u32> {
        sel.as_indices().as_slice().to_vec()
    }

    #[test]
    fn empty_tombstones_passes_upstream_selection_unchanged() {
        let batch = build_batch(&["alice", "bob", "carol"], &[10, 20, 30]);
        let upstream = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2]));
        let mock = Box::new(MockSource::new(vec![(batch, upstream.clone())]));
        let mut wrapper = EncodedTombstoneSource::new(
            mock,
            TombstoneFile::default(),
            0,
            1,
            BqlType::String,
            0,
            0,
        );
        let sels = drain(&mut wrapper);
        assert_eq!(sels.len(), 1);
        assert_eq!(collect_indices(&sels[0]), collect_indices(&upstream));
    }

    #[test]
    fn entity_tombstone_drops_contiguous_block_and_emits_runs() {
        // Entity-sorted: a single tombstoned entity becomes a contiguous gap.
        let batch = build_batch(&["alice", "alice", "bob", "bob", "carol"], &[1, 2, 3, 4, 5]);
        let tombstones = TombstoneFile::for_entities([ScalarValue::String("bob".into())]);
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(5))]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 99);
        let sels = drain(&mut wrapper);
        assert_eq!(sels.len(), 1);
        assert!(matches!(sels[0], RowSelection::Runs(_)));
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 1, 4]);
    }

    #[test]
    fn entity_tombstone_int_keys() {
        let n = 4u32;
        let entity_arr = Int64Array::from(vec![10i64, 20, 20, 30]);
        let ts_arr = TimestampNanosecondArray::from(vec![1i64, 2, 3, 4]).with_timezone("UTC");
        let cols = vec![
            EncodedColumn::Materialized {
                array: Arc::new(entity_arr),
                rows: n,
            },
            EncodedColumn::Materialized {
                array: Arc::new(ts_arr),
                rows: n,
            },
        ];
        let batch = EncodedBatch::new(n, cols);
        let tombstones = TombstoneFile::for_entities([ScalarValue::Int(20)]);
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(n))]));
        let mut wrapper = EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::Int, 0, 0);
        let sels = drain(&mut wrapper);
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 3]);
    }

    #[test]
    fn batch_tombstone_drops_entire_segment_without_decoding() {
        let batch = build_batch(&["alice", "bob"], &[10, 20]);
        let tombstones = TombstoneFile::for_batches([42]);
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(2))]));
        // Segment carries batch_id = 42 → all_batch_dropped = true.
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 42);
        let sels = drain(&mut wrapper);
        assert_eq!(sels.len(), 1);
        assert!(sels[0].is_empty());
    }

    #[test]
    fn batch_tombstone_for_other_segment_passes_through() {
        let batch = build_batch(&["alice"], &[10]);
        let tombstones = TombstoneFile::for_batches([7]);
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(1))]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 99);
        let sels = drain(&mut wrapper);
        assert_eq!(collect_indices(&sels[0]), vec![0u32]);
    }

    #[test]
    fn row_tombstone_uses_synthesized_seq_ids() {
        // seq_id_first = 100, batch has 4 rows → __seq_ids 100..104.
        // Tombstone row 102 → drop the third row.
        let batch = build_batch(&["a", "b", "c", "d"], &[1, 2, 3, 4]);
        let tombstones = TombstoneFile::for_rows([102]);
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(4))]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 100, 0);
        let sels = drain(&mut wrapper);
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 1, 3]);
    }

    #[test]
    fn row_tombstone_carries_offset_across_batches() {
        // Two batches of 3 rows each, seq_id_first = 50.
        // Batch 1 covers __seq_ids 50, 51, 52 → tombstone 51 → drop row 1.
        // Batch 2 covers __seq_ids 53, 54, 55 → tombstone 54 → drop row 1.
        let b1 = build_batch(&["a", "b", "c"], &[1, 2, 3]);
        let b2 = build_batch(&["d", "e", "f"], &[4, 5, 6]);
        let tombstones = TombstoneFile::for_rows([51, 54]);
        let mock = Box::new(MockSource::new(vec![
            (b1, full_selection(3)),
            (b2, full_selection(3)),
        ]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 50, 0);
        let sels = drain(&mut wrapper);
        assert_eq!(sels.len(), 2);
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 2]);
        assert_eq!(collect_indices(&sels[1]), vec![0u32, 2]);
    }

    #[test]
    fn time_range_tombstone_drops_rows_in_range() {
        let batch = build_batch(&["a", "b", "c", "d"], &[10, 20, 30, 40]);
        // Range [20, 30] inclusive → drops rows 1 and 2.
        let tombstones = TombstoneFile::for_time_range(crate::tombstone::TimeRangeDelete {
            min_ts: Some(20),
            min_inclusive: true,
            max_ts: Some(30),
            max_inclusive: true,
        });
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(4))]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 0);
        let sels = drain(&mut wrapper);
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 3]);
    }

    #[test]
    fn upstream_runs_intersect_tombstone_runs_stays_runs() {
        // Upstream selection: rows 1..5 (Runs).
        // Entity tombstone on "bob" at rows 3..5 (Runs).
        // Result should keep rows 1, 2 in Runs form.
        let batch = build_batch(&["alice", "alice", "alice", "bob", "bob"], &[1, 2, 3, 4, 5]);
        let upstream = RowSelection::from_runs(vec![RowRun { start: 1, len: 4 }]);
        let tombstones = TombstoneFile::for_entities([ScalarValue::String("bob".into())]);
        let mock = Box::new(MockSource::new(vec![(batch, upstream)]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 0);
        let sels = drain(&mut wrapper);
        assert_eq!(sels.len(), 1);
        assert!(
            matches!(sels[0], RowSelection::Runs(_)),
            "expected Runs, got {:?}",
            sels[0]
        );
        assert_eq!(collect_indices(&sels[0]), vec![1u32, 2]);
    }

    #[test]
    fn upstream_indices_intersect_tombstone_runs_coerces_to_indices() {
        let batch = build_batch(&["a", "b", "c", "d"], &[1, 2, 3, 4]);
        // Upstream: rows 0, 2, 3 as Indices.
        let upstream = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2, 3]));
        // Tombstone on "c" → drop row 2.
        let tombstones = TombstoneFile::for_entities([ScalarValue::String("c".into())]);
        let mock = Box::new(MockSource::new(vec![(batch, upstream)]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 0);
        let sels = drain(&mut wrapper);
        assert!(matches!(sels[0], RowSelection::Indices(_)));
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 3]);
    }

    #[test]
    fn all_four_granularities_compose_in_one_source() {
        // Two batches.
        // Batch 1: __seq_id 100..104 — entities ["a","b","c","d"], ts [1,2,3,4].
        //   Drop "b" (entity), seq 102 (row), ts in [4,4] (range).
        //   → keep row 0 ("a", seq 100, ts 1).
        // Batch 2: __seq_id 104..107 — entities ["a","a","e"], ts [10,20,30].
        //   Same tombstones; nothing applicable except entity "b" which isn't here.
        //   → keep all three rows.
        let b1 = build_batch(&["a", "b", "c", "d"], &[1, 2, 3, 4]);
        let b2 = build_batch(&["a", "a", "e"], &[10, 20, 30]);
        let mut tombstones = TombstoneFile::for_entities([ScalarValue::String("b".into())]);
        tombstones.merge(&TombstoneFile::for_rows([102]));
        tombstones.merge(&TombstoneFile::for_time_range(
            crate::tombstone::TimeRangeDelete {
                min_ts: Some(4),
                min_inclusive: true,
                max_ts: Some(4),
                max_inclusive: true,
            },
        ));
        let mock = Box::new(MockSource::new(vec![
            (b1, full_selection(4)),
            (b2, full_selection(3)),
        ]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 100, 999);
        let sels = drain(&mut wrapper);
        assert_eq!(collect_indices(&sels[0]), vec![0u32]);
        assert_eq!(collect_indices(&sels[1]), vec![0u32, 1, 2]);
    }

    #[test]
    fn empty_upstream_short_circuits_without_decoding_when_tombstones_present() {
        let batch = build_batch(&["a", "b"], &[1, 2]);
        let tombstones = TombstoneFile::for_entities([ScalarValue::String("a".into())]);
        let mock = Box::new(MockSource::new(vec![(batch, RowSelection::empty())]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 0);
        let sels = drain(&mut wrapper);
        assert!(sels[0].is_empty());
    }

    #[test]
    fn whole_batch_entity_tombstone_emits_empty_selection() {
        // Every row has the same entity, and that entity is tombstoned.
        // The merge's heap-reload path explicitly skips empty selections —
        // this test guards against the wrapper returning a corrupt
        // `Runs` with a zero-length run that would mis-prime the heap.
        let batch = build_batch(&["bob", "bob", "bob"], &[1, 2, 3]);
        let tombstones = TombstoneFile::for_entities([ScalarValue::String("bob".into())]);
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(3))]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 0);
        let sels = drain(&mut wrapper);
        assert!(sels[0].is_empty());
    }

    #[test]
    fn next_returns_none_when_inner_exhausted() {
        let mock = Box::new(MockSource::new(vec![]));
        let mut wrapper = EncodedTombstoneSource::new(
            mock,
            TombstoneFile::default(),
            0,
            1,
            BqlType::String,
            0,
            0,
        );
        assert!(wrapper.next().unwrap().is_none());
    }

    #[test]
    fn time_range_inclusive_vs_exclusive_bounds() {
        let batch = build_batch(&["a", "b", "c", "d"], &[10, 20, 30, 40]);
        // Exclusive lower, inclusive upper: (20, 30] → drops only row 2 (ts 30).
        let tombstones = TombstoneFile::for_time_range(crate::tombstone::TimeRangeDelete {
            min_ts: Some(20),
            min_inclusive: false,
            max_ts: Some(30),
            max_inclusive: true,
        });
        let mock = Box::new(MockSource::new(vec![(batch, full_selection(4))]));
        let mut wrapper =
            EncodedTombstoneSource::new(mock, tombstones, 0, 1, BqlType::String, 0, 0);
        let sels = drain(&mut wrapper);
        assert_eq!(collect_indices(&sels[0]), vec![0u32, 1, 3]);
    }
}
