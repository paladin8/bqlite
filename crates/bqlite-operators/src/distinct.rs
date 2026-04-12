//! Streaming distinct operator: first-occurrence row deduplication via an
//! `AHashSet<GroupKey>`.
//!
//! ## Algorithm
//!
//! `DistinctOperator` processes input **streaming** (one batch at a time),
//! avoiding full materialization. For each input batch it:
//!
//! 1. Builds a `GroupKey` for every row by extracting scalar values from
//!    all columns in column order.
//! 2. Checks the key against a `HashSet`. First-occurrence rows are added to
//!    the set (after checking the `max_groups` cap) and included in the output
//!    mask; duplicates are excluded.
//! 3. Applies `arrow::compute::filter_record_batch` with the mask and returns
//!    the filtered batch (zero-copy column slicing).
//! 4. If the entire batch is duplicate, the loop continues to pull the next
//!    child batch rather than emitting an empty batch.
//!
//! ## GroupKey construction
//!
//! `DistinctOperator` reuses the [`GroupKey`] type from the aggregate module
//! (TASK-307).  The key is built from **all columns** of the input batch in
//! column order, matching `SELECT DISTINCT` semantics: two rows are equal iff
//! every column value is equal under the SQL `GROUP BY` / `DISTINCT`
//! convention (`NULL = NULL` is **true** for deduplication).
//!
//! The implementation follows the TASK-307 kernel interface: column arrays are
//! pre-collected once per batch, then each row is constructed by indexing into
//! the pre-collected columns — not by materializing a separate `Vec` per row
//! inside a nested scan.
//!
//! ## Null handling
//!
//! `NULL = NULL` is **true** for deduplication purposes, matching standard
//! SQL `SELECT DISTINCT` semantics.  The `GroupKey::Hash` / `GroupKey::Eq`
//! implementations in `aggregate.rs` already enforce this by treating
//! `ScalarValue::Null` as equal to `ScalarValue::Null`.
//!
//! ## Cancellation
//!
//! The cancellation token is checked at the top of every `next_batch` call
//! per `operator-traits.md §5`.
//!
//! ## Memory
//!
//! The `AHashSet<GroupKey>` is the dominant allocation.  At `max_groups = 1 M`
//! and ~80 bytes per key the peak is ~80 MB.  No `MemoryBudget` registration
//! occurs in Wave 3; the `max_groups` hard cap is the sole overflow defense.
//!
//! See `docs/design/operators/sort-distinct.md §4` for the full spec.

use ahash::AHashSet;

use arrow::array::{Array, ArrayRef, BooleanArray, BooleanBuilder};
use arrow::array::{Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use arrow::compute;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;

use bqlite_core::{BqliteError, OperatorSchema, Result, ScalarValue};

use crate::aggregate::GroupKey;
use crate::operator::{CancellationToken, PhysicalOperator};

// ─────────────────────────────────────────────────────────────────────────────
// DistinctOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Streaming row deduplication operator.
///
/// Constructed by the engine bind step (TASK-323) from a
/// [`bqlite_planner::physical::DistinctPhysical`] descriptor plus a bound
/// child operator and cancellation token.
pub struct DistinctOperator {
    child: Box<dyn PhysicalOperator>,
    /// Hard cap on distinct row count. `BqliteError::Execution` on breach.
    max_groups: usize,
    cancel: CancellationToken,
    schema: OperatorSchema,
    /// Set of rows already emitted, keyed by all-column `GroupKey`.
    seen: AHashSet<GroupKey>,
    /// Latch set when the child is exhausted or `close()` is called.
    done: bool,
}

impl DistinctOperator {
    /// Construct a new `DistinctOperator`.
    ///
    /// - `child` — the child operator whose output will be deduplicated.
    /// - `max_groups` — maximum number of distinct rows before overflow error.
    ///   Use [`bqlite_planner::physical::DEFAULT_MAX_GROUPS`] for the default.
    /// - `cancel` — shared cancellation flag; checked at every `next_batch`
    ///   entry.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        max_groups: usize,
        cancel: CancellationToken,
    ) -> Self {
        let schema = child.output_schema().clone();
        Self {
            child,
            max_groups,
            cancel,
            schema,
            seen: AHashSet::new(),
            done: false,
        }
    }

    /// Borrow the child operator (used by tests and the engine bind step).
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }
}

impl PhysicalOperator for DistinctOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    /// Pull the next batch of deduplicated rows.
    ///
    /// Loops over input batches, skipping entirely-duplicate batches (rather
    /// than emitting empty output), until a non-empty result or exhaustion.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Cancellation check at entry — before the done latch and before any
        // child pull — per operator-traits.md §5.
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Cancelled);
        }
        if self.done {
            return Ok(None);
        }

        loop {
            match self.child.next_batch()? {
                None => {
                    self.done = true;
                    return Ok(None);
                }
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        // Empty input batch — treat the same as a fully-duplicate
                        // batch: skip without emitting zero-row output.
                        continue;
                    }
                    let mask = self.build_inclusion_mask(&batch)?;
                    if mask.true_count() == 0 {
                        // Every row in this batch is a duplicate; skip it
                        // and pull the next child batch instead of emitting
                        // a zero-row output batch.
                        continue;
                    }
                    let output =
                        compute::filter_record_batch(&batch, &mask).map_err(BqliteError::Arrow)?;
                    return Ok(Some(output));
                }
            }
        }
    }

    /// Release resources: drop the `AHashSet` and close the child.
    /// Idempotent — safe to call more than once.  Only forwards `close()` to
    /// the child on the first call; subsequent calls are no-ops.
    fn close(&mut self) -> Result<()> {
        if !self.done {
            self.done = true;
            // Drop the seen set to release memory immediately.
            self.seen = AHashSet::new();
            return self.child.close();
        }
        Ok(())
    }
}

impl DistinctOperator {
    /// Build a `BooleanArray` mask: `true` for first-occurrence rows,
    /// `false` for duplicates. Updates `self.seen` as a side effect.
    ///
    /// Returns `BqliteError::Execution` if a new key would exceed `max_groups`.
    fn build_inclusion_mask(&mut self, batch: &RecordBatch) -> Result<BooleanArray> {
        let num_rows = batch.num_rows();
        let columns = batch.columns();
        let mut builder = BooleanBuilder::with_capacity(num_rows);

        for row in 0..num_rows {
            let key = build_group_key(columns, row);
            if self.seen.contains(&key) {
                builder.append_value(false);
            } else {
                // New key — check the cap before inserting.
                if self.seen.len() >= self.max_groups {
                    return Err(BqliteError::Execution(format!(
                        "DistinctOperator: distinct row count exceeds max_groups limit {}",
                        self.max_groups
                    )));
                }
                self.seen.insert(key);
                builder.append_value(true);
            }
        }
        Ok(builder.finish())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build a `GroupKey` from a single row across all `columns`.
///
/// Follows the TASK-307 kernel interface: `columns` are pre-collected once per
/// batch by the caller; this function only indexes into them — no per-call
/// column lookup or extra allocation beyond the `Vec<ScalarValue>` that becomes
/// the key.
fn build_group_key(columns: &[ArrayRef], row: usize) -> GroupKey {
    let values: Vec<ScalarValue> = columns.iter().map(|col| extract_scalar(col, row)).collect();
    GroupKey(values)
}

/// Extract a `ScalarValue` from a column array at the given row index.
///
/// Mirrors `HashAccumulator::extract_scalar` from `aggregate.rs`.  Both
/// functions must handle the same Arrow data types and produce the same
/// `ScalarValue` variants so that `GroupKey` equality is consistent between
/// aggregation and deduplication.
///
/// Any Arrow type not yet mapped (e.g. List, Map, Struct) is represented as
/// `ScalarValue::Null`.  This is a deliberate conservative fallback: treating
/// unknown-typed cells as NULL means two rows that differ only in an unmapped
/// column would (incorrectly) be considered duplicates — a false-negative on
/// deduplication that is safer than false-positive schema errors.  TASK-322's
/// scope covers the scalar types from `type-system.md`; complex type support
/// is a later-wave concern.
fn extract_scalar(array: &ArrayRef, row: usize) -> ScalarValue {
    if array.is_null(row) {
        return ScalarValue::Null;
    }
    match array.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            ScalarValue::Bool(arr.value(row))
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            ScalarValue::Int(arr.value(row))
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            ScalarValue::Float(arr.value(row))
        }
        arrow::datatypes::DataType::Utf8View => {
            let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            ScalarValue::String(arr.value(row).to_owned())
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            ScalarValue::Timestamp(arr.value(row))
        }
        _ => ScalarValue::Null,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{BqlType, BqliteError, ColumnDef, OperatorSchema};

    use crate::operator::{CancellationToken, PhysicalOperator};

    use super::DistinctOperator;

    // ── Test fixtures ────────────────────────────────────────────────────────

    fn int_schema() -> OperatorSchema {
        OperatorSchema::new(vec![ColumnDef::required("v", BqlType::Int)])
            .expect("schema construction must succeed")
    }

    fn make_int_batch(values: &[i64]) -> RecordBatch {
        let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![array]).expect("record batch construction must succeed")
    }

    /// A pre-canned `PhysicalOperator` that yields a fixed list of batches.
    struct VecOp {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        idx: usize,
    }

    impl VecOp {
        fn new(schema: OperatorSchema, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                batches,
                idx: 0,
            }
        }
    }

    impl PhysicalOperator for VecOp {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next_batch(&mut self) -> bqlite_core::Result<Option<RecordBatch>> {
            if self.idx >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.idx].clone();
            self.idx += 1;
            Ok(Some(b))
        }
    }

    // ── empty input ──────────────────────────────────────────────────────────

    #[test]
    fn distinct_empty_input_returns_none() {
        let child = Box::new(VecOp::new(int_schema(), vec![]));
        let mut op = DistinctOperator::new(child, 1000, CancellationToken::new());
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── no duplicates ────────────────────────────────────────────────────────

    #[test]
    fn distinct_all_unique_rows_pass_through() {
        let batch = make_int_batch(&[1, 2, 3, 4]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 4);
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── duplicates within one batch ──────────────────────────────────────────

    #[test]
    fn distinct_removes_intra_batch_duplicates() {
        let batch = make_int_batch(&[1, 2, 1, 3, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        // First-occurrence order must be preserved.
        assert_eq!(values, vec![1, 2, 3]);
    }

    // ── duplicates across batches ────────────────────────────────────────────

    #[test]
    fn distinct_removes_cross_batch_duplicates() {
        let b1 = make_int_batch(&[1, 2]);
        let b2 = make_int_batch(&[2, 3]); // 2 is a cross-batch duplicate
        let child = Box::new(VecOp::new(int_schema(), vec![b1, b2]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out1 = op.next_batch().unwrap().unwrap();
        assert_eq!(out1.num_rows(), 2); // 1, 2

        let out2 = op.next_batch().unwrap().unwrap();
        assert_eq!(out2.num_rows(), 1); // 3 only (2 was already seen)
        let col = out2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 3);

        assert!(op.next_batch().unwrap().is_none());
    }

    // ── fully duplicate batch is skipped (no empty-batch emission) ───────────

    #[test]
    fn distinct_skips_entirely_duplicate_batch() {
        let b1 = make_int_batch(&[1, 2]);
        let b2 = make_int_batch(&[1, 2]); // all dups
        let b3 = make_int_batch(&[3]);
        let child = Box::new(VecOp::new(int_schema(), vec![b1, b2, b3]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out1 = op.next_batch().unwrap().unwrap();
        assert_eq!(out1.num_rows(), 2);

        // b2 is fully duplicate; the operator should skip it and pull b3
        // without emitting an empty batch.
        let out2 = op.next_batch().unwrap().unwrap();
        assert_eq!(out2.num_rows(), 1);
        let col = out2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 3);

        assert!(op.next_batch().unwrap().is_none());
    }

    // ── multi-column deduplication ───────────────────────────────────────────

    #[test]
    fn distinct_multi_column_considers_all_columns() {
        // Two columns: (a, b). Rows (1,1), (1,2), (1,1) — only (1,1) is dup.
        let col_a: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1]));
        let col_b: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 1]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col_a, col_b]).unwrap();

        let schema = OperatorSchema::new(vec![
            ColumnDef::required("a", BqlType::Int),
            ColumnDef::required("b", BqlType::Int),
        ])
        .unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2, "only (1,1) should be deduplicated");
    }

    // ── NULL = NULL for deduplication ────────────────────────────────────────

    #[test]
    fn distinct_null_equals_null_for_dedup() {
        // Two rows with NULL — should be treated as duplicates.
        let col: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();

        let schema = OperatorSchema::new(vec![ColumnDef::nullable("v", BqlType::Int)]).unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1, "two NULLs must dedup to one");
    }

    // ── string column deduplication ──────────────────────────────────────────

    #[test]
    fn distinct_string_column() {
        let col: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "a", "c"]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();

        let schema = OperatorSchema::new(vec![ColumnDef::required("s", BqlType::String)]).unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3); // "a", "b", "c"
    }

    // ── bool column deduplication ────────────────────────────────────────────

    #[test]
    fn distinct_bool_column() {
        let col: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true, false, true]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "b",
            DataType::Boolean,
            false,
        )]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();

        let schema = OperatorSchema::new(vec![ColumnDef::required("b", BqlType::Bool)]).unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2); // true, false
    }

    // ── output schema unchanged ──────────────────────────────────────────────

    #[test]
    fn distinct_output_schema_unchanged() {
        let schema = int_schema();
        let child = Box::new(VecOp::new(schema.clone(), vec![]));
        let op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());
        assert_eq!(*op.output_schema(), schema);
    }

    // ── max_groups cap ───────────────────────────────────────────────────────

    #[test]
    fn distinct_exceeds_max_groups_returns_execution_error() {
        let batch = make_int_batch(&[1, 2, 3]); // 3 unique rows
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = DistinctOperator::new(child, 2 /* cap at 2 */, CancellationToken::new());

        match op.next_batch() {
            Err(BqliteError::Execution(msg)) => {
                assert_eq!(
                    msg,
                    "DistinctOperator: distinct row count exceeds max_groups limit 2"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn distinct_exactly_at_max_groups_is_allowed() {
        let batch = make_int_batch(&[10, 20]); // 2 unique rows, cap is 2
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = DistinctOperator::new(child, 2, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    // ── cancellation ─────────────────────────────────────────────────────────

    #[test]
    fn distinct_cancelled_returns_cancelled_error() {
        let token = CancellationToken::new();
        token.cancel();
        let child = Box::new(VecOp::new(int_schema(), vec![make_int_batch(&[1, 2])]));
        let mut op = DistinctOperator::new(child, 1_000_000, token);

        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    // ── close is idempotent ──────────────────────────────────────────────────

    #[test]
    fn distinct_close_is_idempotent() {
        let child = Box::new(VecOp::new(int_schema(), vec![]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());
        op.close().unwrap();
        op.close().unwrap();
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── extract_scalar handles nullable columns ───────────────────────────────

    #[test]
    fn distinct_nullable_int_column() {
        let col: ArrayRef = Arc::new(Int64Array::from(vec![Some(1i64), None, Some(1), None]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();

        let schema = OperatorSchema::new(vec![ColumnDef::nullable("v", BqlType::Int)]).unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));
        let mut op = DistinctOperator::new(child, 1_000_000, CancellationToken::new());

        let out = op.next_batch().unwrap().unwrap();
        // 1, NULL — two distinct values (second 1 and second NULL are dups)
        assert_eq!(out.num_rows(), 2);
    }
}
