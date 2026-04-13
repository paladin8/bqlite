# Tombstone-Aware Scan + Merge Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply tombstones in the read path after pushdown but before rows leave the scan layer, preserving exact visibility rules from `docs/design/storage/deletes.md`.

**Architecture:** A `TombstoneFilter` applies four-granularity tombstone checks (batch, entity, row, time-range) to RecordBatches. A `TombstoneScanWrapper` wraps per-segment `SegmentScan` objects to filter rows before they enter the k-way merge. The engine's `bind_scan` loads a `TombstoneSnapshot` at query bind time and passes it to `ScanOperator`, which wraps each segment's scan with the appropriate shard-specific filter during `open()`.

**Tech Stack:** Rust, Apache Arrow (BooleanArray, filter_record_batch), bqlite-core (SegmentScan trait, ScalarValue), bqlite-storage (TombstoneFile, TombstoneSnapshot)

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/bqlite-storage/src/tombstone.rs` | Modify | Add `TombstoneFilter` struct + `filter_batch()` method |
| `crates/bqlite-storage/src/tombstone_scan.rs` | Create | `TombstoneScanWrapper` implementing `SegmentScan` |
| `crates/bqlite-storage/src/lib.rs` | Modify | Add `pub mod tombstone_scan;` |
| `crates/bqlite-operators/src/scan.rs` | Modify | Add `tombstone_snapshot` field, wrap scans in `open()` |
| `crates/bqlite-engine/src/bind.rs` | Modify | Load tombstone snapshot, pass to `ScanOperator` |

---

### Task 1: TombstoneFilter — batch filtering logic

**Files:**
- Modify: `crates/bqlite-storage/src/tombstone.rs` (append after `TombstoneSnapshot` impl block, before `#[cfg(test)]`)

This task adds the core filtering function that takes a `TombstoneFile` and removes matching rows from a `RecordBatch`. The filter checks each granularity in the order specified by `deletes.md` SS7.1: batch → entity → row → time-range. Columns are looked up by name; if a needed column is absent but the corresponding tombstone set is non-empty, the filter returns an error (this is a correctness invariant — those tombstones can't exist until system columns are materialized).

- [ ] **Step 1: Add TombstoneFilter struct and empty-tombstone short-circuit test**

Add to `crates/bqlite-storage/src/tombstone.rs` (in the test module):

```rust
#[test]
fn filter_noop_on_empty_tombstones() {
    let filter = TombstoneFilter::new(
        &TombstoneFile::default(),
        "entity_id",
        "ts",
    );
    let batch = make_filter_test_batch(
        &["alice", "bob"],
        &[100, 200],
        &["click", "view"],
    );
    let result = filter.filter_batch(batch.clone()).unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(result, batch);
}
```

Add the test helper `make_filter_test_batch` in the test module:

```rust
fn make_filter_test_batch(ids: &[&str], tss: &[i64], evts: &[&str]) -> RecordBatch {
    use arrow::array::{ArrayRef, StringViewArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("event_type", DataType::Utf8View, false),
    ]));
    let id_arr: ArrayRef = Arc::new(StringViewArray::from(ids.to_vec()));
    let ts_arr: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(tss.iter().copied().map(Some).collect::<Vec<_>>())
            .with_timezone("UTC"),
    );
    let evt_arr: ArrayRef = Arc::new(StringViewArray::from(evts.to_vec()));
    RecordBatch::try_new(schema, vec![id_arr, ts_arr, evt_arr]).unwrap()
}
```

- [ ] **Step 2: Implement TombstoneFilter struct with empty short-circuit**

Add to `crates/bqlite-storage/src/tombstone.rs` (production code, before the test module):

```rust
use std::sync::Arc;
use arrow::array::{Array, BooleanArray, Int64Array, StringViewArray, TimestampNanosecondArray, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;

/// Applies tombstone checks to a `RecordBatch`, removing rows that match
/// any entry in the associated `TombstoneFile`.
///
/// Filter order follows `deletes.md` SS7.1: batch → entity → row → time-range.
/// A row is suppressed if **any** check matches.
pub struct TombstoneFilter<'a> {
    tombstones: &'a TombstoneFile,
    entity_key_col: &'a str,
    ts_col: &'a str,
}

impl<'a> TombstoneFilter<'a> {
    /// Build a filter for the given tombstone state.
    ///
    /// `entity_key_col` and `ts_col` are the column names in the
    /// `RecordBatch` to use for entity-level and time-range checks.
    pub fn new(
        tombstones: &'a TombstoneFile,
        entity_key_col: &'a str,
        ts_col: &'a str,
    ) -> Self {
        Self { tombstones, entity_key_col, ts_col }
    }

    /// Filter `batch`, returning a new batch with tombstoned rows removed.
    ///
    /// Returns the batch unchanged when tombstones are empty (zero cost).
    pub fn filter_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if self.tombstones.is_empty() || batch.num_rows() == 0 {
            return Ok(batch);
        }

        let num_rows = batch.num_rows();
        let mut alive = vec![true; num_rows];

        // 1. Batch-level: __batch_id
        self.apply_batch_deletes(&batch, &mut alive)?;
        // 2. Entity-level
        self.apply_entity_deletes(&batch, &mut alive)?;
        // 3. Row-level: __seq_id
        self.apply_row_deletes(&batch, &mut alive)?;
        // 4. Time-range
        self.apply_time_range_deletes(&batch, &mut alive)?;

        let mask = BooleanArray::from(alive);
        filter_record_batch(&batch, &mask).map_err(|e| {
            BqliteError::Execution(format!("tombstone filter_record_batch failed: {e}"))
        })
    }
}
```

- [ ] **Step 3: Implement entity-level filtering**

Add methods to `TombstoneFilter` impl block:

```rust
fn apply_entity_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()> {
    if self.tombstones.entity_deletes.is_empty() {
        return Ok(());
    }
    let col_idx = batch.schema().index_of(self.entity_key_col).map_err(|_| {
        BqliteError::Execution(format!(
            "tombstone filter: entity key column '{}' not found in batch",
            self.entity_key_col
        ))
    })?;
    let col = batch.column(col_idx);

    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..alive.len() {
            if alive[i] {
                let val = ScalarValue::String(arr.value(i).to_string());
                if self.tombstones.entity_deletes.contains(&val) {
                    alive[i] = false;
                }
            }
        }
    } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        for i in 0..alive.len() {
            if alive[i] {
                let val = ScalarValue::Int(arr.value(i));
                if self.tombstones.entity_deletes.contains(&val) {
                    alive[i] = false;
                }
            }
        }
    } else {
        return Err(BqliteError::Execution(format!(
            "tombstone filter: unsupported entity key column type {:?}",
            col.data_type()
        )));
    }
    Ok(())
}
```

- [ ] **Step 4: Write entity-level filtering tests**

```rust
#[test]
fn filter_entity_deletes_string() {
    let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let batch = make_filter_test_batch(
        &["alice", "bob", "alice", "carol"],
        &[100, 200, 300, 400],
        &["click", "view", "click", "view"],
    );
    let result = filter.filter_batch(batch).unwrap();
    assert_eq!(result.num_rows(), 2);
    let ids = result.column(0).as_any().downcast_ref::<StringViewArray>().unwrap();
    assert_eq!(ids.value(0), "bob");
    assert_eq!(ids.value(1), "carol");
}

#[test]
fn filter_entity_deletes_int() {
    use arrow::array::{ArrayRef, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let tss: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![Some(100), Some(200), Some(300)])
            .with_timezone("UTC"),
    );
    let batch = RecordBatch::try_new(schema, vec![ids, tss]).unwrap();

    let tf = TombstoneFile::for_entities([ScalarValue::Int(2)]);
    let filter = TombstoneFilter::new(&tf, "user_id", "ts");
    let result = filter.filter_batch(batch).unwrap();
    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(col.value(0), 1);
    assert_eq!(col.value(1), 3);
}
```

- [ ] **Step 5: Implement time-range filtering**

```rust
fn apply_time_range_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()> {
    if self.tombstones.time_range_deletes.is_empty() {
        return Ok(());
    }
    let col_idx = batch.schema().index_of(self.ts_col).map_err(|_| {
        BqliteError::Execution(format!(
            "tombstone filter: timestamp column '{}' not found in batch",
            self.ts_col
        ))
    })?;
    let col = batch.column(col_idx);
    let ts_arr = col.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| {
        BqliteError::Execution(format!(
            "tombstone filter: timestamp column '{}' is not TimestampNanosecondArray",
            self.ts_col
        ))
    })?;

    for i in 0..alive.len() {
        if alive[i] {
            let ts = ts_arr.value(i);
            for range in &self.tombstones.time_range_deletes {
                if range.contains_timestamp(ts) {
                    alive[i] = false;
                    break;
                }
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 6: Write time-range filtering tests**

```rust
#[test]
fn filter_time_range_deletes() {
    let range = TimeRangeDelete {
        min_ts: Some(150),
        min_inclusive: true,
        max_ts: Some(250),
        max_inclusive: false,
    };
    let tf = TombstoneFile::for_time_range(range);
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let batch = make_filter_test_batch(
        &["a", "b", "c", "d"],
        &[100, 150, 200, 300],
        &["e1", "e2", "e3", "e4"],
    );
    let result = filter.filter_batch(batch).unwrap();
    assert_eq!(result.num_rows(), 2);
    let tss = result.column(1).as_any()
        .downcast_ref::<TimestampNanosecondArray>().unwrap();
    assert_eq!(tss.value(0), 100);
    assert_eq!(tss.value(1), 300);
}

#[test]
fn filter_multiple_time_ranges() {
    let tf = TombstoneFile {
        time_range_deletes: vec![
            TimeRangeDelete {
                min_ts: None,
                min_inclusive: false,
                max_ts: Some(100),
                max_inclusive: true,
            },
            TimeRangeDelete {
                min_ts: Some(300),
                min_inclusive: false,
                max_ts: None,
                max_inclusive: false,
            },
        ],
        ..Default::default()
    };
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let batch = make_filter_test_batch(
        &["a", "b", "c", "d"],
        &[50, 100, 200, 400],
        &["e1", "e2", "e3", "e4"],
    );
    let result = filter.filter_batch(batch).unwrap();
    assert_eq!(result.num_rows(), 1);
    let tss = result.column(1).as_any()
        .downcast_ref::<TimestampNanosecondArray>().unwrap();
    assert_eq!(tss.value(0), 200);
}
```

- [ ] **Step 7: Implement row-level and batch-level filtering**

```rust
fn apply_batch_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()> {
    if self.tombstones.batch_deletes.is_empty() {
        return Ok(());
    }
    let col_idx = batch.schema().index_of(bqlite_core::BATCH_ID_COLUMN).map_err(|_| {
        BqliteError::Execution(format!(
            "tombstone filter: batch tombstones exist but column '{}' not in batch",
            bqlite_core::BATCH_ID_COLUMN
        ))
    })?;
    let col = batch.column(col_idx);
    let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        BqliteError::Execution(format!(
            "tombstone filter: {} column is not Int64Array",
            bqlite_core::BATCH_ID_COLUMN
        ))
    })?;
    for i in 0..alive.len() {
        if alive[i] && self.tombstones.batch_deletes.contains(&(arr.value(i) as u64)) {
            alive[i] = false;
        }
    }
    Ok(())
}

fn apply_row_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()> {
    if self.tombstones.row_deletes.is_empty() {
        return Ok(());
    }
    let col_idx = batch.schema().index_of(bqlite_core::SEQ_ID_COLUMN).map_err(|_| {
        BqliteError::Execution(format!(
            "tombstone filter: row tombstones exist but column '{}' not in batch",
            bqlite_core::SEQ_ID_COLUMN
        ))
    })?;
    let col = batch.column(col_idx);
    let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        BqliteError::Execution(format!(
            "tombstone filter: {} column is not Int64Array",
            bqlite_core::SEQ_ID_COLUMN
        ))
    })?;
    for i in 0..alive.len() {
        if alive[i] && self.tombstones.row_deletes.contains(&(arr.value(i) as u64)) {
            alive[i] = false;
        }
    }
    Ok(())
}
```

- [ ] **Step 8: Write row/batch filtering tests and combined-granularity test**

```rust
#[test]
fn filter_row_deletes() {
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())), false),
        Field::new("__seq_id", DataType::Int64, false),
    ]));
    let ids: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
    let tss: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![Some(100), Some(200), Some(300)])
            .with_timezone("UTC"),
    );
    let seqs: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let batch = RecordBatch::try_new(schema, vec![ids, tss, seqs]).unwrap();

    let tf = TombstoneFile::for_rows([20]);
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let result = filter.filter_batch(batch).unwrap();
    assert_eq!(result.num_rows(), 2);
    let seq_col = result.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(seq_col.value(0), 10);
    assert_eq!(seq_col.value(1), 30);
}

#[test]
fn filter_combined_entity_and_time_range() {
    let tf = TombstoneFile {
        entity_deletes: HashSet::from([ScalarValue::String("alice".into())]),
        time_range_deletes: vec![TimeRangeDelete {
            min_ts: Some(250),
            min_inclusive: true,
            max_ts: None,
            max_inclusive: false,
        }],
        ..Default::default()
    };
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let batch = make_filter_test_batch(
        &["alice", "bob", "carol", "bob"],
        &[100, 200, 300, 400],
        &["e1", "e2", "e3", "e4"],
    );
    let result = filter.filter_batch(batch).unwrap();
    // alice removed by entity; carol(300) and bob(400) removed by time-range
    assert_eq!(result.num_rows(), 1);
    let ids = result.column(0).as_any().downcast_ref::<StringViewArray>().unwrap();
    assert_eq!(ids.value(0), "bob");
}

#[test]
fn filter_all_rows_removed() {
    let tf = TombstoneFile::for_entities([ScalarValue::String("a".into())]);
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let batch = make_filter_test_batch(&["a", "a"], &[100, 200], &["e1", "e2"]);
    let result = filter.filter_batch(batch).unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn filter_missing_seq_id_column_errors_when_needed() {
    let tf = TombstoneFile::for_rows([1]);
    let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
    let batch = make_filter_test_batch(&["a"], &[100], &["e1"]);
    let err = filter.filter_batch(batch).unwrap_err();
    assert!(err.to_string().contains("__seq_id"), "got: {err}");
}
```

- [ ] **Step 9: Run tests and commit checkpoint 1a**

```bash
scripts/local-ci.sh
```

---

### Task 2: TombstoneScanWrapper — SegmentScan decorator

**Files:**
- Create: `crates/bqlite-storage/src/tombstone_scan.rs`
- Modify: `crates/bqlite-storage/src/lib.rs` (add `pub mod tombstone_scan;`)

This type wraps a `Box<dyn SegmentScan>` and applies `TombstoneFilter` to each row group. It implements `SegmentScan` so it plugs transparently into `KWayMergeScan`.

- [ ] **Step 1: Create tombstone_scan.rs with TombstoneScanWrapper**

```rust
//! SegmentScan decorator that applies tombstone filtering per row group.
//!
//! Wraps an inner `SegmentScan` and applies the tombstone filter from
//! `deletes.md` SS7 after each `next_row_group()` call, before rows
//! reach the k-way merge. This preserves the scan-pipeline ordering:
//! column projection → zone-map pushdown → tombstone filtering → merge.

use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

use bqlite_core::error::Result;
use bqlite_core::{SegmentScan, ZoneMap};

use crate::tombstone::{TombstoneFile, TombstoneFilter};

/// A `SegmentScan` that removes tombstoned rows from every row group
/// before handing them to the merge layer.
///
/// Created by `ScanOperator::open()` when a segment's `(window, shard)`
/// has non-empty tombstones in the query's `TombstoneSnapshot`.
pub struct TombstoneScanWrapper {
    inner: Box<dyn SegmentScan>,
    tombstones: TombstoneFile,
    entity_key_col: String,
    ts_col: String,
}

impl TombstoneScanWrapper {
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
                let filter = TombstoneFilter::new(
                    &self.tombstones,
                    &self.entity_key_col,
                    &self.ts_col,
                );
                let filtered = filter.filter_batch(batch)?;
                Ok(Some(filtered))
            }
        }
    }
}
```

- [ ] **Step 2: Add module declaration to lib.rs**

In `crates/bqlite-storage/src/lib.rs`, add `pub mod tombstone_scan;` next to the existing `pub mod tombstone;` line.

- [ ] **Step 3: Write tests for TombstoneScanWrapper**

Add tests in `crates/bqlite-storage/src/tombstone_scan.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringViewArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{Result, ScalarValue, SegmentScan, ZoneMap};

    use crate::tombstone::{TombstoneFile, TimeRangeDelete};

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
            TimestampNanosecondArray::from(
                tss.iter().copied().map(Some).collect::<Vec<_>>(),
            )
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
    fn wrapper_passthrough_when_no_tombstones() {
        let batch = make_batch(&["a", "b"], &[100, 200], &["e1", "e2"]);
        let inner = Box::new(MockScan::new(vec![batch.clone()]));
        let mut wrapper = TombstoneScanWrapper::new(
            inner,
            TombstoneFile::default(),
            "entity_id".into(),
            "ts".into(),
        );
        let result = wrapper.next_row_group().unwrap().unwrap();
        assert_eq!(result.num_rows(), 2);
        assert!(wrapper.next_row_group().unwrap().is_none());
    }

    #[test]
    fn wrapper_filters_entity_deletes() {
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
    fn wrapper_returns_empty_batch_when_all_removed() {
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
    fn wrapper_filters_across_multiple_row_groups() {
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
    fn wrapper_delegates_row_group_count() {
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
```

- [ ] **Step 4: Run tests and commit checkpoint 1b**

```bash
scripts/local-ci.sh
```

---

### Task 3: Wire tombstone snapshot into ScanOperator

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs`

Add `tombstone_snapshot: Option<Arc<TombstoneSnapshot>>` and entity/ts column name fields to `ScanOperator`. In `open()`, wrap each per-segment `SegmentScan` with `TombstoneScanWrapper` when the segment's `(window_id, shard_id)` has tombstones in the snapshot.

- [ ] **Step 1: Add new fields and update constructor**

In `ScanOperator` struct, add:
```rust
/// Per-query tombstone snapshot, loaded at engine bind time (SS6.3).
/// `None` when no tombstones are active for the query's table.
tombstone_snapshot: Option<Arc<bqlite_storage::tombstone::TombstoneSnapshot>>,
/// Entity key column name, used by the tombstone filter.
entity_key_name: String,
/// Timestamp column name, used by the tombstone filter.
ts_name: String,
```

Update `new()` signature to accept `tombstone_snapshot: Option<Arc<bqlite_storage::tombstone::TombstoneSnapshot>>`, and store the entity/ts column names from `reader.schema()`.

Update `full_scan()` to pass `None` for tombstone_snapshot.

- [ ] **Step 2: Update `open()` to wrap scans with tombstone filter**

In the `open()` method, after opening each segment, check if the snapshot has tombstones for that segment's (window_id, shard_id). If so, wrap the scan:

```rust
fn open(&mut self) -> Result<()> {
    let handles: Result<Vec<SegmentHandle>> = self.reader.segments().collect();
    let handles = handles?;

    let mut scans: Vec<Box<dyn SegmentScan>> = Vec::with_capacity(handles.len());
    for handle in &handles {
        let scan =
            self.reader
                .open_segment(handle, &self.projection, self.scan_predicate.clone())?;
        let scan = self.maybe_wrap_with_tombstones(scan, handle)?;
        scans.push(scan);
    }

    let merge = KWayMergeScan::new(
        scans,
        self.arrow_schema.clone(),
        self.entity_col,
        self.ts_col,
    )?;
    self.merge = Some(merge);
    self.exhausted = false;
    Ok(())
}
```

Add the helper method:

```rust
/// Wrap a per-segment scan with tombstone filtering if the query's
/// tombstone snapshot has entries for this segment's `(window, shard)`.
fn maybe_wrap_with_tombstones(
    &self,
    scan: Box<dyn SegmentScan>,
    handle: &SegmentHandle,
) -> Result<Box<dyn SegmentScan>> {
    let snap = match &self.tombstone_snapshot {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(scan),
    };
    let window_id = u32::try_from(handle.window_id).map_err(|_| {
        BqliteError::Execution(format!(
            "segment window_id {} exceeds u32 range for tombstone lookup",
            handle.window_id
        ))
    })?;
    let shard_id = u16::try_from(handle.shard_id).map_err(|_| {
        BqliteError::Execution(format!(
            "segment shard_id {} exceeds u16 range for tombstone lookup",
            handle.shard_id
        ))
    })?;
    match snap.get(window_id, shard_id) {
        Some(tf) if !tf.is_empty() => {
            Ok(Box::new(
                bqlite_storage::tombstone_scan::TombstoneScanWrapper::new(
                    scan,
                    tf.clone(),
                    self.entity_key_name.clone(),
                    self.ts_name.clone(),
                ),
            ))
        }
        _ => Ok(scan),
    }
}
```

- [ ] **Step 3: Update all existing test callers of `ScanOperator::new`**

Every test that calls `ScanOperator::new(reader, cols, preds, cancel)` needs a 5th argument `None`. `full_scan()` handles this internally.

- [ ] **Step 4: Write scan operator tombstone integration tests**

```rust
#[test]
fn scan_with_tombstone_filters_entities() {
    // Build a VecReader with two segments from shard 0, window 0
    // Tombstone snapshot deletes entity "alice"
    // Verify scan output excludes all alice rows
}

#[test]
fn scan_with_empty_tombstone_snapshot_is_noop() {
    // Same as a normal scan — no performance regression
}

#[test]
fn scan_tombstone_only_applies_to_matching_shard() {
    // Segment from shard 0 with tombstones; segment from shard 1 without
    // Only shard 0's rows are filtered
}
```

- [ ] **Step 5: Run local-ci and commit**

```bash
scripts/local-ci.sh
```

---

### Task 4: Wire tombstone snapshot loading in engine bind_scan

**Files:**
- Modify: `crates/bqlite-engine/src/bind.rs`

Load the `TombstoneSnapshot` at bind time by enumerating the segment handles to discover which `(window_id, shard_id)` pairs the query touches, then loading the snapshot from the `Database`.

- [ ] **Step 1: Update bind_scan to load and pass tombstone snapshot**

```rust
fn bind_scan(scan: &ScanPhysical, db: &Database) -> Result<Box<dyn PhysicalOperator>> {
    let reader_range = scan.reader_range.unwrap_or_else(TimeRange::unbounded);
    let reader_box: Box<dyn SegmentReader> =
        db.segment_reader_for_time_range(&scan.table, reader_range)?;
    let reader: Arc<dyn SegmentReader> = Arc::from(reader_box);

    // Enumerate segments to discover (window, shard) pairs for tombstone loading.
    // This is a fast in-memory iteration over the manifest snapshot.
    let handles: Vec<SegmentHandle> = reader.segments().collect::<Result<Vec<_>>>()?;
    let mut tombstone_targets: Vec<(u32, u16)> = Vec::new();
    for h in &handles {
        let wid = u32::try_from(h.window_id).unwrap_or(u32::MAX);
        let sid = u16::try_from(h.shard_id).unwrap_or(u16::MAX);
        if !tombstone_targets.contains(&(wid, sid)) {
            tombstone_targets.push((wid, sid));
        }
    }
    let tombstone_snapshot = if tombstone_targets.is_empty() {
        None
    } else {
        let snap = db.load_tombstone_snapshot(&scan.table, &tombstone_targets)?;
        if snap.is_empty() { None } else { Some(Arc::new(snap)) }
    };

    let mut scan_predicates = scan.scan_predicates.clone();
    scan_predicates.extend(build_time_range_predicates(scan, reader_range)?);
    let op = ScanOperator::new(
        reader,
        &scan.projected_columns,
        scan_predicates,
        CancellationToken::new(),
        tombstone_snapshot,
    )?;
    Ok(Box::new(op))
}
```

- [ ] **Step 2: Add necessary imports**

Add to `bind.rs` imports:
```rust
use bqlite_core::SegmentHandle;
use bqlite_storage::tombstone::TombstoneSnapshot;
```

- [ ] **Step 3: Run local-ci and commit**

```bash
scripts/local-ci.sh
```

---

## Design Doc Reconciliation

Changes to reconcile against `docs/design/storage/deletes.md`:

1. **SS6.3** — Tombstone snapshot is loaded in `bind_scan` (engine bind step) and passed to `ScanOperator` as `Arc<TombstoneSnapshot>`. ✓ matches spec.
2. **SS7** — Filtering happens after zone-map pushdown (inside `TombstoneScanWrapper::next_row_group`, which runs after the inner `SegmentScan` does its pushdown) and before merge/operators. ✓ matches spec.
3. **SS7.1** — Check order: batch → entity → row → time-range with early-exit per row. ✓ matches spec.
4. **Row/batch columns**: `__seq_id` and `__batch_id` are not yet materialized as per-row Arrow columns in segments (they exist as segment-footer metadata). The filter correctly errors if tombstones of those types exist but columns are absent. This is safe because DELETE (TASK-453) depends on this task and can't produce such tombstones until system columns are materialized.
