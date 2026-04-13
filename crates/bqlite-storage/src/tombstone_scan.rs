//! `SegmentScan` decorator that applies tombstone filtering per row group.
//!
//! Wraps an inner [`SegmentScan`] and applies the [`TombstoneFilter`]
//! from `deletes.md` SS7 after each `next_row_group()` call, before
//! rows reach the k-way merge. This preserves the scan-pipeline
//! ordering: column projection → zone-map pushdown → **tombstone
//! filtering** → merge → post-filter → operators.
//!
//! Created by `ScanOperator::open()` when a segment's `(window, shard)`
//! has non-empty tombstones in the query's `TombstoneSnapshot`.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

use bqlite_core::error::Result;
use bqlite_core::{SegmentScan, ZoneMap};

use crate::tombstone::{TombstoneFile, TombstoneFilter};

/// A [`SegmentScan`] that removes tombstoned rows from every row group
/// before handing them to the merge layer.
///
/// Zone-map metadata is delegated directly to the inner scan — tombstone
/// filtering does not affect zone-map semantics because zone maps
/// describe the *unfiltered* data and are used by predicate pushdown
/// which runs before tombstone checks.
pub struct TombstoneScanWrapper {
    inner: Box<dyn SegmentScan>,
    tombstones: TombstoneFile,
    entity_key_col: String,
    ts_col: String,
}

impl TombstoneScanWrapper {
    /// Wrap `inner` with tombstone filtering for the given shard's tombstone state.
    pub fn new(
        inner: Box<dyn SegmentScan>,
        tombstones: TombstoneFile,
        entity_key_col: String,
        ts_col: String,
    ) -> Self {
        Self {
            inner,
            tombstones,
            entity_key_col,
            ts_col,
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
        match self.inner.next_row_group()? {
            None => Ok(None),
            Some(batch) => {
                let filter =
                    TombstoneFilter::new(&self.tombstones, &self.entity_key_col, &self.ts_col);
                let filtered = filter.filter_batch(batch)?;
                Ok(Some(filtered))
            }
        }
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
        );
        assert_eq!(wrapper.row_group_count(), 2);
    }
}
