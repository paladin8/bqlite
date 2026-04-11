//! Wave 1 stub implementation of the filter operator.
//!
//! `FilterOperator` wraps a child [`PhysicalOperator`] and, in this
//! stub, forwards every batch unchanged. The Wave 2 real filter will:
//!
//! - Hold a compiled predicate IR (landing with the parser/planner
//!   expression work).
//! - Evaluate the predicate column-wise per batch.
//! - Prune rows using Arrow's `filter` kernel.
//! - Expose zone-map acceptance to the scan so row-groups can be
//!   skipped outright.
//!
//! Wave 1 carries the type + composition surface only. Giving the
//! engine's bind step (TASK-118) a real `Box<dyn PhysicalOperator>`
//! implementor to compose around the scan is enough to unblock the
//! smoke test at TASK-123; no row filtering actually happens yet.
//!
//! ## Lifecycle
//!
//! `open`, `next_batch`, and `close` all delegate straight through to
//! the child. The child is responsible for cancellation checking,
//! schema propagation, and error reporting — the wrapper adds nothing
//! the trait doesn't already guarantee.

use arrow::record_batch::RecordBatch;

use bqlite_core::{OperatorSchema, Result};

use crate::operator::PhysicalOperator;

/// Pass-through filter operator.
///
/// In Wave 1 the filter is a no-op: the child's output is its output,
/// the child's schema is its schema, and lifecycle calls forward
/// verbatim. Constructing one is how the planner wires the filter
/// position in the physical plan before real predicate evaluation
/// exists — the shape is correct even though the filter accepts
/// every row.
pub struct FilterOperator {
    child: Box<dyn PhysicalOperator>,
}

impl FilterOperator {
    /// Wrap a child operator. Ownership transfers: the filter's
    /// lifecycle controls the child's.
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self { child }
    }

    /// Borrow the wrapped child. Handy for planner tests that need
    /// to inspect the subtree without tearing the wrapper apart.
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }
}

impl PhysicalOperator for FilterOperator {
    fn output_schema(&self) -> &OperatorSchema {
        self.child.output_schema()
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Wave 1 stub — pass the batch through unchanged. The real
        // filter (Wave 2) evaluates a compiled predicate here and
        // returns a row-pruned batch (or an empty one if every row
        // failed the predicate, which consumers already tolerate per
        // the trait contract).
        self.child.next_batch()
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

    /// Shared counters the tests inspect after the filter drives
    /// the child. Writing into `Arc<AtomicUsize>` instead of plain
    /// struct fields lets the test read the values through the
    /// lifecycle path without owning the trait-object child directly.
    #[derive(Default, Clone)]
    struct CallCounters {
        open: Arc<AtomicUsize>,
        close: Arc<AtomicUsize>,
    }

    impl CallCounters {
        fn opens(&self) -> usize {
            self.open.load(Ordering::SeqCst)
        }
        fn closes(&self) -> usize {
            self.close.load(Ordering::SeqCst)
        }
    }

    /// Minimal child operator that records lifecycle invocations
    /// through shared atomics so the filter's delegation can be
    /// asserted.
    struct RecordingChild {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        next_idx: usize,
        counters: CallCounters,
        cancel: CancellationToken,
        fail_next: bool,
    }

    impl RecordingChild {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                schema: scalar_schema(),
                batches,
                next_idx: 0,
                counters: CallCounters::default(),
                cancel: CancellationToken::new(),
                fail_next: false,
            }
        }

        fn with_counters(mut self, counters: CallCounters) -> Self {
            self.counters = counters;
            self
        }

        fn with_cancel(mut self, cancel: CancellationToken) -> Self {
            self.cancel = cancel;
            self
        }

        fn failing() -> Self {
            Self {
                schema: scalar_schema(),
                batches: vec![],
                next_idx: 0,
                counters: CallCounters::default(),
                cancel: CancellationToken::new(),
                fail_next: true,
            }
        }
    }

    impl PhysicalOperator for RecordingChild {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            self.counters.open.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
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
            self.counters.close.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // ── Delegation ───────────────────────────────────────────────────────────

    #[test]
    fn output_schema_matches_child() {
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::new(child);
        assert_eq!(filter.output_schema().columns().len(), 1);
        assert_eq!(filter.output_schema().columns()[0].name, "value");
    }

    #[test]
    fn next_batch_forwards_child_batches_unchanged() {
        let batches = vec![make_batch(vec![1, 2, 3]), make_batch(vec![4, 5])];
        let child = Box::new(RecordingChild::new(batches));
        let mut filter = FilterOperator::new(child);

        let a = filter.next_batch().unwrap().unwrap();
        assert_eq!(a.num_rows(), 3);
        let b = filter.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 2);
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn lifecycle_forwards_to_child() {
        let counters = CallCounters::default();
        let child = Box::new(
            RecordingChild::new(vec![make_batch(vec![1, 2])]).with_counters(counters.clone()),
        );
        let mut filter = FilterOperator::new(child);

        filter.open().unwrap();
        assert_eq!(counters.opens(), 1);

        assert!(filter.next_batch().unwrap().is_some());
        assert!(filter.next_batch().unwrap().is_none());

        filter.close().unwrap();
        assert_eq!(counters.closes(), 1);
        // Repeated `close` forwards every call — matches the
        // "must be idempotent" clause on the child side as well.
        filter.close().unwrap();
        filter.close().unwrap();
        assert_eq!(counters.closes(), 3);
    }

    #[test]
    fn errors_propagate_from_child() {
        let child = Box::new(RecordingChild::failing());
        let mut filter = FilterOperator::new(child);
        let err = filter.next_batch().expect_err("child error must surface");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn cancellation_propagates_from_child() {
        let cancel = CancellationToken::new();
        let child =
            Box::new(RecordingChild::new(vec![make_batch(vec![1])]).with_cancel(cancel.clone()));
        let mut filter = FilterOperator::new(child);
        cancel.cancel();
        let err = filter.next_batch().expect_err("cancelled");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn child_accessor_returns_same_schema() {
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::new(child);
        assert_eq!(
            filter.child().output_schema().columns().len(),
            filter.output_schema().columns().len()
        );
    }

    #[test]
    fn trait_object_compiles() {
        let child = Box::new(RecordingChild::new(vec![]));
        let filter: Box<dyn PhysicalOperator> = Box::new(FilterOperator::new(child));
        let _ = filter;
    }
}
