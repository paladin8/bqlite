//! Wave 1 stub implementation of the project operator.
//!
//! `ProjectOperator` wraps a child [`PhysicalOperator`] and, in this
//! stub, forwards every batch unchanged. The Wave 2 real project
//! will:
//!
//! - Hold a list of output expressions (column refs today, computed
//!   expressions in later waves).
//! - Evaluate expressions per batch using Arrow compute kernels.
//! - Build an output `RecordBatch` whose schema is narrower than (or
//!   a rearrangement of) the child's schema.
//!
//! Wave 1 carries the type + composition surface only; the stub keeps
//! the child's schema as-is so the engine's bind step (TASK-118) can
//! attach a projection node without having to coordinate with the
//! planner's expression work, which lands in later waves.
//!
//! Filter and project are siblings in Wave 1 — same delegation
//! pattern, different operator position. Split across modules now so
//! that the real implementations can grow independently without
//! shuffling file boundaries.

use arrow::record_batch::RecordBatch;

use bqlite_core::{OperatorSchema, Result};

use crate::operator::PhysicalOperator;

/// Pass-through project operator.
///
/// The child's batches flow through unchanged and the child's schema
/// is the project's output schema. The real project (Wave 2) will
/// reshape each batch into the requested column order.
pub struct ProjectOperator {
    child: Box<dyn PhysicalOperator>,
}

impl ProjectOperator {
    /// Wrap a child operator. Ownership transfers: the project's
    /// lifecycle controls the child's.
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self { child }
    }

    /// Borrow the wrapped child. Matches [`FilterOperator::child`]
    /// for planner-test convenience.
    ///
    /// [`FilterOperator::child`]: crate::filter::FilterOperator::child
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }
}

impl PhysicalOperator for ProjectOperator {
    fn output_schema(&self) -> &OperatorSchema {
        self.child.output_schema()
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Wave 1 stub — the child's batch is the project's batch.
        // Wave 2 replaces this with an Arrow `project` kernel call
        // driven by the compiled expression list.
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
    use crate::filter::FilterOperator;
    use crate::operator::CancellationToken;

    fn scalar_schema() -> OperatorSchema {
        OperatorSchema::new(vec![ColumnDef::required("value", BqlType::Int)]).unwrap()
    }

    fn make_batch(rows: Vec<i64>) -> RecordBatch {
        let col: ArrayRef = Arc::new(Int64Array::from(rows));
        let schema = ArrowSchema::new(vec![Field::new("value", DataType::Int64, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![col]).unwrap()
    }

    /// Shared lifecycle counters (same pattern as `filter.rs`).
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

    /// Minimal child operator matching the one in `filter.rs`. Kept
    /// local so the two test modules stay independent — the Wave 2
    /// real implementations will drop these fixtures.
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
        let project = ProjectOperator::new(child);
        assert_eq!(project.output_schema().columns().len(), 1);
        assert_eq!(project.output_schema().columns()[0].name, "value");
    }

    #[test]
    fn next_batch_forwards_child_batches_unchanged() {
        let batches = vec![make_batch(vec![1, 2, 3]), make_batch(vec![4])];
        let child = Box::new(RecordingChild::new(batches));
        let mut project = ProjectOperator::new(child);

        let a = project.next_batch().unwrap().unwrap();
        assert_eq!(a.num_rows(), 3);
        let b = project.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 1);
        assert!(project.next_batch().unwrap().is_none());
    }

    #[test]
    fn lifecycle_forwards_to_child() {
        let counters = CallCounters::default();
        let child = Box::new(
            RecordingChild::new(vec![make_batch(vec![1])]).with_counters(counters.clone()),
        );
        let mut project = ProjectOperator::new(child);

        project.open().unwrap();
        assert_eq!(counters.opens(), 1);

        assert!(project.next_batch().unwrap().is_some());
        assert!(project.next_batch().unwrap().is_none());

        project.close().unwrap();
        project.close().unwrap();
        assert_eq!(counters.closes(), 2);
    }

    #[test]
    fn errors_propagate_from_child() {
        let child = Box::new(RecordingChild::failing());
        let mut project = ProjectOperator::new(child);
        let err = project.next_batch().expect_err("child error must surface");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn cancellation_propagates_from_child() {
        let cancel = CancellationToken::new();
        let child =
            Box::new(RecordingChild::new(vec![make_batch(vec![1])]).with_cancel(cancel.clone()));
        let mut project = ProjectOperator::new(child);
        cancel.cancel();
        let err = project.next_batch().expect_err("cancelled");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn composes_with_filter() {
        // Shape-check that the two Wave 1 stubs compose. The engine
        // bind step will build trees like `project(filter(scan(…)))`;
        // making sure `project(filter(child))` drains correctly today
        // is cheap insurance.
        let child = Box::new(RecordingChild::new(vec![
            make_batch(vec![1, 2]),
            make_batch(vec![3]),
        ]));
        let filter = Box::new(FilterOperator::new(child));
        let mut project = ProjectOperator::new(filter);
        project.open().unwrap();
        let mut rows = 0;
        while let Some(batch) = project.next_batch().unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
        project.close().unwrap();
    }

    #[test]
    fn child_accessor_returns_same_schema() {
        let child = Box::new(RecordingChild::new(vec![]));
        let project = ProjectOperator::new(child);
        assert_eq!(
            project.child().output_schema().columns().len(),
            project.output_schema().columns().len()
        );
    }

    #[test]
    fn trait_object_compiles() {
        let child = Box::new(RecordingChild::new(vec![]));
        let project: Box<dyn PhysicalOperator> = Box::new(ProjectOperator::new(child));
        let _ = project;
    }
}
