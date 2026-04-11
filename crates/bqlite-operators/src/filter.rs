//! Wave 2 implementation of the filter operator.
//!
//! `FilterOperator` wraps a child [`PhysicalOperator`] and evaluates a
//! compiled predicate against every batch, returning a zero-copy
//! filtered view via `arrow::compute::filter_record_batch`.
//!
//! ## Tile iteration
//!
//! Per execution-model.md §3.6 and TASK-231's scope, predicate
//! evaluation walks the batch in **execution tiles** (default 2,048
//! rows, clamped to `[MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE]`).
//! For each tile, the evaluator produces a `BooleanArray` over the
//! tile; the tile-level masks are concatenated into one full-batch
//! mask, and a single `filter_record_batch` call turns that mask
//! into the output. The tile loop is a cache-residency optimization
//! for the inner kernel work — it is **not** the
//! `FilteredBatch` / `SelectionVector` path from execution-model.md
//! §3.8 (that chain is a Wave 5 refactor, TASK-503, which replaces
//! this operator rather than extending it).
//!
//! Small batches (`num_rows ≤ tile_size`) skip the tile loop and
//! evaluate the predicate in one shot to avoid the per-tile
//! allocation and concat overhead.
//!
//! ## NULL and Kleene semantics
//!
//! The evaluator produces a Bool array with a null bitmap reflecting
//! three-valued logic. `arrow::compute::filter_record_batch` treats
//! NULL in the mask as "drop" — which is the SQL-spec behavior for
//! `WHERE p` — so rows whose predicate evaluates to NULL are
//! excluded rather than retained.
//!
//! ## Empty output
//!
//! A batch whose predicate rejects every row is re-pulled on the
//! next `next_batch()` call; filter avoids surfacing zero-row
//! batches to downstream operators when the next child pull would
//! cheaply produce something. The trait contract allows mid-stream
//! empty batches, so the loop is a soft preference, not a
//! correctness requirement.

use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute;
use arrow::record_batch::RecordBatch;

use bqlite_core::{BqliteError, OperatorSchema, Result};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::physical::{
    DEFAULT_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE, MIN_FILTER_TILE_SIZE,
};

use crate::eval;
use crate::operator::PhysicalOperator;

/// Pull-based filter operator.
///
/// The output schema is identical to the child's — filter never
/// reshapes columns — and the predicate is evaluated per batch via
/// the shared [`crate::eval`] evaluator.
pub struct FilterOperator {
    child: Box<dyn PhysicalOperator>,
    predicate: CompiledExpr,
    tile_size: usize,
    schema: OperatorSchema,
}

impl FilterOperator {
    /// Wrap a child operator with a compiled predicate and a tile
    /// size.
    ///
    /// The `tile_size` is clamped to
    /// `[MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE]` — the engine
    /// bind step (TASK-232) already clamps the descriptor value, but
    /// the operator clamps defensively so direct callers (tests,
    /// benches, embedded uses) cannot hand in an out-of-range value.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        predicate: CompiledExpr,
        tile_size: usize,
    ) -> Self {
        let schema = child.output_schema().clone();
        Self {
            child,
            predicate,
            tile_size: tile_size.clamp(MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE),
            schema,
        }
    }

    /// Convenience constructor that uses
    /// [`DEFAULT_FILTER_TILE_SIZE`].
    pub fn with_default_tile_size(
        child: Box<dyn PhysicalOperator>,
        predicate: CompiledExpr,
    ) -> Self {
        Self::new(child, predicate, DEFAULT_FILTER_TILE_SIZE)
    }

    /// Borrow the wrapped child. Retained from the Wave 1 stub for
    /// planner tests that need to inspect the subtree.
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }

    /// The tile size in use, after clamping.
    pub fn tile_size(&self) -> usize {
        self.tile_size
    }

    /// Evaluate the predicate over `batch` in tile-sized slices,
    /// concatenating the tile-level masks into one full-batch
    /// BooleanArray.
    fn evaluate_tiled_mask(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let total = batch.num_rows();
        if total == 0 {
            return Ok(BooleanArray::from(Vec::<bool>::new()));
        }
        if total <= self.tile_size {
            return eval::evaluate_bool(&self.predicate, batch);
        }

        let mut tile_masks: Vec<BooleanArray> = Vec::with_capacity(total.div_ceil(self.tile_size));
        let mut start = 0;
        while start < total {
            let len = std::cmp::min(self.tile_size, total - start);
            let tile = batch.slice(start, len);
            let tile_mask = eval::evaluate_bool(&self.predicate, &tile)?;
            tile_masks.push(tile_mask);
            start += len;
        }

        let refs: Vec<&dyn Array> = tile_masks.iter().map(|m| m as &dyn Array).collect();
        let concat = compute::concat(&refs).map_err(BqliteError::Arrow)?;
        let concat_ref: ArrayRef = concat;
        concat_ref
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| {
                BqliteError::Execution(
                    "tile-mask concat did not produce a BooleanArray".to_string(),
                )
            })
    }
}

impl PhysicalOperator for FilterOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Loop over child pulls, skipping entirely-rejected batches.
        // Downstream operators tolerate mid-stream empty batches, but
        // execution-model.md §3.2 recommends producers avoid emitting
        // them when cheap — a single extra pull of the child is cheap.
        loop {
            let Some(batch) = self.child.next_batch()? else {
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                // An empty input already satisfies "no rows to
                // evaluate"; forward it downstream without touching
                // the predicate.
                return Ok(Some(batch));
            }
            let mask = self.evaluate_tiled_mask(&batch)?;
            let filtered =
                compute::filter_record_batch(&batch, &mask).map_err(BqliteError::Arrow)?;
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
            // Every row was filtered; pull the child again.
        }
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

    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{BqlType, ColumnDef};
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;
    use crate::operator::CancellationToken;

    // ── Schema / batch fixtures ──────────────────────────────────────────────

    fn int_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("value", BqlType::Int),
        ])
        .unwrap()
    }

    fn int_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn make_batch(ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let id_array: ArrayRef = Arc::new(Int64Array::from(ids));
        let val_array: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(int_arrow_schema(), vec![id_array, val_array]).unwrap()
    }

    // ── Predicate builders ───────────────────────────────────────────────────

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }

    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }

    fn int_lit(i: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(i)))
    }

    fn compare(op: CompareOp, left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
        sp(Expr::Compare {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    fn compile_predicate(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("predicate type checks");
        CompiledExpr::from_typed(&typed)
    }

    fn value_gt(literal: i64, schema: &OperatorSchema) -> CompiledExpr {
        compile_predicate(
            compare(CompareOp::Greater, col("value"), int_lit(literal)),
            schema,
        )
    }

    fn id_eq(literal: i64, schema: &OperatorSchema) -> CompiledExpr {
        compile_predicate(
            compare(CompareOp::Equal, col("id"), int_lit(literal)),
            schema,
        )
    }

    // ── Recording child ──────────────────────────────────────────────────────

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
                schema: int_schema(),
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

    // ── Correctness ──────────────────────────────────────────────────────────

    fn ids_of(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    #[test]
    fn output_schema_matches_child() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::with_default_tile_size(child, predicate);
        assert_eq!(filter.output_schema().columns().len(), 2);
        assert_eq!(filter.output_schema().columns()[0].name, "id");
    }

    #[test]
    fn filters_rows_matching_predicate() {
        let schema = int_schema();
        let predicate = value_gt(15, &schema);
        let batches = vec![make_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 15])];
        let child = Box::new(RecordingChild::new(batches));
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);

        let out = filter.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(ids_of(&out), vec![2, 3]);
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn fully_rejecting_batch_is_skipped_and_child_re_pulled() {
        let schema = int_schema();
        let predicate = id_eq(99, &schema);
        // First batch all rejected; second batch has the match.
        let batches = vec![
            make_batch(vec![1, 2], vec![10, 20]),
            make_batch(vec![99, 3], vec![10, 20]),
        ];
        let child = RecordingChild::new(batches);
        let pulls = child.pulls_counter();
        let mut filter = FilterOperator::with_default_tile_size(Box::new(child), predicate);

        let out = filter.next_batch().unwrap().unwrap();
        assert_eq!(ids_of(&out), vec![99]);
        // Assert that the filter actually re-pulled the child after
        // the first batch was rejected: the single returned batch
        // must correspond to two child pulls, not one.
        assert_eq!(pulls.load(Ordering::SeqCst), 2);
        assert!(filter.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn empty_input_batch_passes_through() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let batches = vec![make_batch(vec![], vec![])];
        let child = Box::new(RecordingChild::new(batches));
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);

        let out = filter.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
        assert!(filter.next_batch().unwrap().is_none());
    }

    #[test]
    fn multiple_batches_filtered_independently() {
        let schema = int_schema();
        let predicate = value_gt(15, &schema);
        let batches = vec![
            make_batch(vec![1, 2], vec![10, 20]),
            make_batch(vec![3, 4], vec![5, 30]),
        ];
        let child = Box::new(RecordingChild::new(batches));
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);

        let a = filter.next_batch().unwrap().unwrap();
        assert_eq!(ids_of(&a), vec![2]);
        let b = filter.next_batch().unwrap().unwrap();
        assert_eq!(ids_of(&b), vec![4]);
        assert!(filter.next_batch().unwrap().is_none());
    }

    // ── Tile iteration ───────────────────────────────────────────────────────

    #[test]
    fn tile_size_is_clamped_to_minimum() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::new(child, predicate, 10);
        assert_eq!(filter.tile_size(), MIN_FILTER_TILE_SIZE);
    }

    #[test]
    fn tile_size_is_clamped_to_maximum() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::new(child, predicate, 999_999);
        assert_eq!(filter.tile_size(), MAX_FILTER_TILE_SIZE);
    }

    #[test]
    fn default_tile_size_matches_planner_constant() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::with_default_tile_size(child, predicate);
        assert_eq!(filter.tile_size(), DEFAULT_FILTER_TILE_SIZE);
    }

    #[test]
    fn tiled_evaluation_produces_correct_full_batch_mask() {
        // Use MIN_FILTER_TILE_SIZE (1024) and a 3500-row batch so the
        // tile loop runs multiple passes (4 tiles: 1024 + 1024 + 1024
        // + 428). Predicate keeps every row where value > 1000.
        let schema = int_schema();
        let predicate = value_gt(1000, &schema);
        let ids: Vec<i64> = (0..3500).collect();
        let values: Vec<i64> = (0..3500).collect();
        let batches = vec![make_batch(ids, values)];
        let child = Box::new(RecordingChild::new(batches));
        let mut filter = FilterOperator::new(child, predicate, MIN_FILTER_TILE_SIZE);

        let out = filter.next_batch().unwrap().unwrap();
        // Rows kept: values 1001..=3499 → 2499 rows.
        assert_eq!(out.num_rows(), 2499);
        let ids_out = ids_of(&out);
        assert_eq!(ids_out[0], 1001);
        assert_eq!(ids_out[ids_out.len() - 1], 3499);
    }

    // ── Null semantics ───────────────────────────────────────────────────────

    #[test]
    fn null_predicate_rows_are_dropped() {
        // Nullable `value` column; predicate `value > 10`. Rows with
        // NULL value must be dropped (three-valued logic + SQL WHERE
        // semantics), not retained.
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::nullable("value", BqlType::Int),
        ])
        .unwrap();
        let predicate = value_gt(10, &schema);

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4]));
        let value_array: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(5), None, Some(20), Some(15)]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![id_array, value_array]).unwrap();
        let child = Box::new(RecordingChild {
            schema,
            batches: vec![batch],
            next_idx: 0,
            pulls: Arc::new(AtomicUsize::new(0)),
            opens: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
            cancel: CancellationToken::new(),
            fail_next: false,
        });
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);

        let out = filter.next_batch().unwrap().unwrap();
        assert_eq!(ids_of(&out), vec![3, 4]);
    }

    // ── StringView materialization check ─────────────────────────────────────

    #[test]
    fn dictionary_and_string_view_batches_are_not_disturbed() {
        // Filter on an Int predicate, but the batch also carries a
        // StringView column — execution-model.md §3.7 says string
        // materialization is always Utf8View. Assert that the
        // filtered output preserves the Utf8View type (filter is a
        // row-selection operation, not a type rewrite).
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("id", BqlType::Int),
            ColumnDef::required("name", BqlType::String),
        ])
        .unwrap();
        let predicate =
            compile_predicate(compare(CompareOp::Greater, col("id"), int_lit(1)), &schema);

        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let name_array: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![id_array, name_array]).unwrap();
        let child = Box::new(RecordingChild {
            schema,
            batches: vec![batch],
            next_idx: 0,
            pulls: Arc::new(AtomicUsize::new(0)),
            opens: Arc::new(AtomicUsize::new(0)),
            closes: Arc::new(AtomicUsize::new(0)),
            cancel: CancellationToken::new(),
            fail_next: false,
        });
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);
        let out = filter.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(out.schema().field(1).data_type(), &DataType::Utf8View);
    }

    // ── Lifecycle & error paths ──────────────────────────────────────────────

    #[test]
    fn lifecycle_forwards_to_child() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = RecordingChild::new(vec![make_batch(vec![1], vec![10])]);
        let opens = child.opens_counter();
        let closes = child.closes_counter();
        let mut filter = FilterOperator::with_default_tile_size(Box::new(child), predicate);

        filter.open().unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1);

        assert!(filter.next_batch().unwrap().is_some());

        filter.close().unwrap();
        filter.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn errors_propagate_from_child() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::failing());
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);
        let err = filter.next_batch().expect_err("child error must surface");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn cancellation_propagates_from_child() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let cancel = CancellationToken::new();
        let child = Box::new(
            RecordingChild::new(vec![make_batch(vec![1], vec![10])]).with_cancel(cancel.clone()),
        );
        let mut filter = FilterOperator::with_default_tile_size(child, predicate);
        cancel.cancel();
        let err = filter.next_batch().expect_err("cancelled");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn child_accessor_returns_same_schema() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![]));
        let filter = FilterOperator::with_default_tile_size(child, predicate);
        assert_eq!(
            filter.child().output_schema().columns().len(),
            filter.output_schema().columns().len()
        );
    }

    #[test]
    fn pull_counts_match_for_multi_batch_pipeline() {
        // Two batches, neither fully-rejecting. Filter pulls the
        // child once per batch plus one trailing None.
        let schema = int_schema();
        let predicate = value_gt(5, &schema);
        let child = RecordingChild::new(vec![
            make_batch(vec![1], vec![10]),
            make_batch(vec![2], vec![20]),
        ]);
        let pulls = child.pulls_counter();
        let mut filter = FilterOperator::with_default_tile_size(Box::new(child), predicate);
        while filter.next_batch().unwrap().is_some() {}
        assert_eq!(pulls.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn trait_object_compiles() {
        let schema = int_schema();
        let predicate = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![]));
        let filter: Box<dyn PhysicalOperator> =
            Box::new(FilterOperator::with_default_tile_size(child, predicate));
        let _ = filter;
    }
}
