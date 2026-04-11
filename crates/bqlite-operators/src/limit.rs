//! Wave 2 implementation of the limit operator.
//!
//! `LimitOperator` wraps a child [`PhysicalOperator`] and cuts the
//! pulled row stream off at a fixed row count. It is stateless beyond
//! a single remaining-row counter and does not allocate any column
//! buffers — when a child batch exceeds the remaining budget, the
//! operator returns a zero-copy [`RecordBatch::slice`] of the
//! prefix and halts the child on the next pull.
//!
//! ## Scope
//!
//! - **Respects the entity-aligned batch contract** (execution-model.md
//!   §3.5). The operator does not track entity boundaries — if the
//!   limit clips mid-entity, the clipped prefix is still a valid
//!   contiguous slice of the child's output because all rows for
//!   that entity are contiguous by the scan-level invariant. Wave 2
//!   semantics for `| limit N` do not require entity-level rounding.
//! - **Stateless beyond the counter.** Per the Wave 2 scope in TASK-
//!   231, the operator does not implement the
//!   `FilteredBatch` / `SelectionVector` contract from execution-
//!   model.md §3.8 — that surface is a Wave 5 refactor
//!   (TASK-503). Wave 2 ships the correct-but-simpler copy-free slice
//!   path.
//! - **Early child termination.** Once the row budget is exhausted,
//!   subsequent `next_batch` calls return `Ok(None)` without touching
//!   the child. The child's `close()` is still called during
//!   [`PhysicalOperator::close`] so resources are released.
//!
//! ## Lifecycle
//!
//! `open`, `close`, and `output_schema` delegate to the child. The
//! only interesting method is `next_batch`.

use arrow::record_batch::RecordBatch;

use bqlite_core::{OperatorSchema, Result};

use crate::operator::PhysicalOperator;

/// Pull-based limit operator.
///
/// Constructed with a child operator and a row budget. Each call to
/// [`PhysicalOperator::next_batch`] pulls one batch from the child,
/// potentially slices the tail off, and returns it. Once the budget
/// is exhausted the operator latches to `Ok(None)` without
/// re-invoking the child.
pub struct LimitOperator {
    child: Box<dyn PhysicalOperator>,
    /// Number of rows remaining in the budget. Decremented by each
    /// forwarded batch; reaches zero when the limit is hit.
    remaining: u64,
    /// Once `true`, `next_batch` short-circuits to `Ok(None)`. Set
    /// the first time the budget is exhausted or the child runs out.
    exhausted: bool,
}

impl LimitOperator {
    /// Wrap a child operator with a row budget.
    ///
    /// A budget of `0` is legal — the first `next_batch` returns
    /// `Ok(None)` without pulling the child, matching the Wave 2
    /// parser which accepts `| limit 0` as a well-formed pipeline
    /// terminator. A missing explicit limit is the caller's problem:
    /// planners that do not want to limit must simply not wire this
    /// operator.
    pub fn new(child: Box<dyn PhysicalOperator>, limit: u64) -> Self {
        Self {
            child,
            remaining: limit,
            exhausted: limit == 0,
        }
    }

    /// Borrow the wrapped child. Mirrors [`crate::FilterOperator::child`]
    /// for tests that want to inspect the subtree.
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }
}

impl PhysicalOperator for LimitOperator {
    fn output_schema(&self) -> &OperatorSchema {
        self.child.output_schema()
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }

        let Some(batch) = self.child.next_batch()? else {
            // Child exhausted before we hit the limit — latch so we
            // stop polling it.
            self.exhausted = true;
            return Ok(None);
        };

        let rows = batch.num_rows() as u64;
        if rows <= self.remaining {
            self.remaining -= rows;
            if self.remaining == 0 {
                // We've filled the budget with exactly this batch.
                // Latch so the next call short-circuits. The child
                // gets one fewer pull — acceptable because the child
                // contract allows `next_batch` to stop at any point.
                self.exhausted = true;
            }
            return Ok(Some(batch));
        }

        // Child batch overshoots the budget. Slice the prefix.
        // `RecordBatch::slice` is zero-copy — it shares the underlying
        // buffers and only rewrites offsets.
        let take = self.remaining as usize;
        let sliced = batch.slice(0, take);
        self.remaining = 0;
        self.exhausted = true;
        Ok(Some(sliced))
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_core::{BqlType, BqliteError, ColumnDef};

    use super::*;
    use crate::operator::CancellationToken;

    fn scalar_schema() -> OperatorSchema {
        OperatorSchema::new(vec![ColumnDef::required("value", BqlType::Int)]).unwrap()
    }

    fn make_batch(rows: Vec<i64>) -> RecordBatch {
        let col: ArrayRef = Arc::new(Int64Array::from(rows));
        let schema = ArrowSchema::new(vec![Field::new("value", DataType::Int64, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![col]).unwrap()
    }

    /// Recording child that counts how many times `next_batch` was
    /// invoked — lets tests assert "limit halted the child early".
    struct RecordingChild {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        next_idx: usize,
        pulls: Arc<AtomicUsize>,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        cancel: CancellationToken,
        fail_next: bool,
    }

    impl RecordingChild {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                schema: scalar_schema(),
                batches,
                next_idx: 0,
                pulls: Arc::new(AtomicUsize::new(0)),
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
                cancel: CancellationToken::new(),
                fail_next: false,
            }
        }

        fn pulls_counter(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.pulls)
        }

        fn opens_counter(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.opens)
        }

        fn closes_counter(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.closes)
        }

        fn with_cancel(mut self, cancel: CancellationToken) -> Self {
            self.cancel = cancel;
            self
        }

        fn failing() -> Self {
            Self {
                fail_next: true,
                ..Self::new(vec![])
            }
        }
    }

    impl PhysicalOperator for RecordingChild {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            if self.fail_next {
                return Err(BqliteError::Execution("boom".into()));
            }
            if self.cancel.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            if self.next_idx >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.next_idx].clone();
            self.next_idx += 1;
            Ok(Some(b))
        }

        fn close(&mut self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // ── Happy paths ──────────────────────────────────────────────────────────

    #[test]
    fn limit_zero_emits_nothing_and_skips_child() {
        let child = RecordingChild::new(vec![make_batch(vec![1, 2, 3])]);
        let pulls = child.pulls_counter();
        let mut limit = LimitOperator::new(Box::new(child), 0);

        assert!(limit.next_batch().unwrap().is_none());
        // Subsequent calls also return None and do not touch the child.
        assert!(limit.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn limit_larger_than_child_forwards_all_batches() {
        let child = RecordingChild::new(vec![make_batch(vec![1, 2]), make_batch(vec![3])]);
        let pulls = child.pulls_counter();
        let mut limit = LimitOperator::new(Box::new(child), 100);

        let mut total = 0;
        while let Some(batch) = limit.next_batch().unwrap() {
            total += batch.num_rows();
        }
        assert_eq!(total, 3);
        // Child was pulled for each of the two batches plus one trailing
        // `None` to detect exhaustion.
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
        // Idempotent after exhaustion: subsequent calls latch on `None`
        // without re-polling the child.
        assert!(limit.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn limit_equals_child_total_emits_all_and_latches() {
        let child = RecordingChild::new(vec![make_batch(vec![1, 2]), make_batch(vec![3, 4])]);
        let pulls = child.pulls_counter();
        let mut limit = LimitOperator::new(Box::new(child), 4);

        let a = limit.next_batch().unwrap().unwrap();
        assert_eq!(a.num_rows(), 2);
        let b = limit.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 2);
        // Budget exhausted — subsequent calls return None without
        // pulling the child again.
        let pulls_before = pulls.load(Ordering::SeqCst);
        assert!(limit.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), pulls_before);
    }

    #[test]
    fn limit_slices_overflowing_batch() {
        // One 5-row batch, limit 3 → slice the first 3 rows and halt.
        let child = RecordingChild::new(vec![make_batch(vec![1, 2, 3, 4, 5])]);
        let mut limit = LimitOperator::new(Box::new(child), 3);

        let batch = limit.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert_eq!(col.value(2), 3);
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn limit_halts_child_early_after_slice() {
        // Limit clips the first batch; the second batch must never be pulled.
        let child = RecordingChild::new(vec![
            make_batch(vec![1, 2, 3, 4, 5]),
            make_batch(vec![6, 7]),
        ]);
        let pulls = child.pulls_counter();
        let mut limit = LimitOperator::new(Box::new(child), 2);

        let batch = limit.next_batch().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(limit.next_batch().unwrap().is_none());
        assert_eq!(
            pulls.load(Ordering::SeqCst),
            1,
            "child must not be pulled a second time after limit is hit"
        );
    }

    #[test]
    fn limit_respects_multiple_batches_under_budget() {
        // Three 2-row batches, limit 5 → first two batches pass through,
        // third batch is sliced to 1 row.
        let child = RecordingChild::new(vec![
            make_batch(vec![1, 2]),
            make_batch(vec![3, 4]),
            make_batch(vec![5, 6]),
        ]);
        let mut limit = LimitOperator::new(Box::new(child), 5);

        let a = limit.next_batch().unwrap().unwrap();
        assert_eq!(a.num_rows(), 2);
        let b = limit.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 2);
        let c = limit.next_batch().unwrap().unwrap();
        assert_eq!(c.num_rows(), 1);
        assert!(limit.next_batch().unwrap().is_none());
    }

    #[test]
    fn limit_handles_empty_child() {
        let child = RecordingChild::new(vec![]);
        let pulls = child.pulls_counter();
        let mut limit = LimitOperator::new(Box::new(child), 10);
        assert!(limit.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        // Subsequent calls idempotently return None without re-polling
        // the child — the `exhausted` latch fires after the first
        // `None` from the child.
        assert!(limit.next_batch().unwrap().is_none());
        assert_eq!(
            pulls.load(Ordering::SeqCst),
            1,
            "child must not be polled after the first None"
        );
    }

    // ── Lifecycle & error paths ──────────────────────────────────────────────

    #[test]
    fn lifecycle_forwards_to_child() {
        let child = RecordingChild::new(vec![make_batch(vec![1])]);
        let opens = child.opens_counter();
        let closes = child.closes_counter();
        let mut limit = LimitOperator::new(Box::new(child), 100);

        limit.open().unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        assert!(limit.next_batch().unwrap().is_some());

        limit.close().unwrap();
        limit.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn errors_propagate_from_child() {
        let child = RecordingChild::failing();
        let mut limit = LimitOperator::new(Box::new(child), 10);
        let err = limit.next_batch().expect_err("child error must surface");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn cancellation_propagates_from_child() {
        let cancel = CancellationToken::new();
        let child =
            RecordingChild::new(vec![make_batch(vec![1, 2, 3])]).with_cancel(cancel.clone());
        let mut limit = LimitOperator::new(Box::new(child), 100);
        cancel.cancel();
        let err = limit.next_batch().expect_err("cancelled");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn output_schema_matches_child() {
        let child = RecordingChild::new(vec![]);
        let limit = LimitOperator::new(Box::new(child), 100);
        assert_eq!(limit.output_schema().columns().len(), 1);
        assert_eq!(limit.output_schema().columns()[0].name, "value");
    }

    #[test]
    fn child_accessor_exposes_subtree() {
        let child = RecordingChild::new(vec![]);
        let limit = LimitOperator::new(Box::new(child), 100);
        assert_eq!(
            limit.child().output_schema().columns().len(),
            limit.output_schema().columns().len()
        );
    }

    #[test]
    fn trait_object_compiles() {
        let child = RecordingChild::new(vec![]);
        let limit: Box<dyn PhysicalOperator> = Box::new(LimitOperator::new(Box::new(child), 10));
        let _ = limit;
    }
}
