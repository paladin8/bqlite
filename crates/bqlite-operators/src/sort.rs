//! Pipeline sort operator: materializes all input rows in memory, applies
//! Arrow `lexsort`, and emits sorted output in `DEFAULT_OUTPUT_BATCH_SIZE`
//! chunks.
//!
//! ## Algorithm
//!
//! **Phase 1 — accumulation**: `next_batch` drains the child operator,
//! pushing each input batch into an in-memory buffer. Each batch's
//! Arrow array memory is charged to the per-query [`MemoryBudget`]
//! through a [`MemoryReservation`] held alongside the batch, so the
//! buffered footprint is visible to the tracker.  If the running row
//! count exceeds `max_rows` the operator returns
//! `BqliteError::Execution` immediately.
//!
//! **Phase 2 — sort and drain**: when the child is exhausted the
//! operator concatenates all buffered batches into one `RecordBatch` via
//! `arrow::compute::concat_batches` (zero-copy where possible), evaluates
//! each sort-key expression over the concatenated batch, calls
//! `arrow::compute::lexsort_to_indices` for a stable sort, reorders
//! columns with `arrow::compute::take`, then splits the result into
//! `DEFAULT_OUTPUT_BATCH_SIZE`-row chunks for pipeline-friendly emission.
//!
//! ## Null ordering
//!
//! Per `docs/design/operators/sort-distinct.md §3.3`:
//! - `ASC`  → NULLs **last**  (`SortOptions { nulls_first: false }`)
//! - `DESC` → NULLs **first** (`SortOptions { nulls_first: true  }`)
//!
//! ## Cancellation
//!
//! The cancellation token is checked at the top of every `next_batch`
//! call (before pulling from the child or draining output) per
//! `operator-traits.md §5`.
//!
//! ## Memory
//!
//! The operator now reserves through the per-query budget on every
//! buffered batch; `MemoryBudgetExceeded` propagates immediately when
//! the budget is exhausted (TASK-513 CP2a). The pre-existing `max_rows`
//! hard cap (default `DEFAULT_SORT_MAX_ROWS` = 10 M) is preserved as
//! an absolute upper bound. The on-disk spill writer + k-way merge land
//! in TASK-513 CP2b, layered on top of this scaffolding.
//!
//! See `docs/design/operators/sort-distinct.md §3` for the full spec.

use std::sync::Arc;

use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn, SortOptions};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use bqlite_core::memory::{MemoryBudget, MemoryReservation};
use bqlite_core::{BqliteError, OperatorSchema, Result};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::logical::SortDirection;

use crate::eval;
use crate::operator::{CancellationToken, PhysicalOperator, DEFAULT_OUTPUT_BATCH_SIZE};

// ─────────────────────────────────────────────────────────────────────────────
// SortOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Two-phase pipeline sort operator.
///
/// Constructed by the engine bind step (TASK-323) from a
/// [`bqlite_planner::physical::SortPhysical`] descriptor plus a bound
/// child operator and cancellation token.
pub struct SortOperator {
    child: Box<dyn PhysicalOperator>,
    /// Compiled sort key expressions paired with their direction.
    keys: Vec<(CompiledExpr, SortDirection)>,
    /// Hard cap on total input rows. `BqliteError::Execution` on breach.
    max_rows: usize,
    cancel: CancellationToken,
    /// Per-query memory budget. Each buffered input batch is reserved
    /// against this budget so the tracker observes the in-flight sort
    /// footprint and can fire spill handlers on pressure (CP2b).
    budget: Arc<dyn MemoryBudget>,
    schema: OperatorSchema,
    /// Cached Arrow schema derived from `schema`; used by `concat_batches`.
    arrow_schema: Arc<ArrowSchema>,
    state: SortState,
}

/// One in-memory input batch plus the budget reservation that pinned its
/// Arrow array bytes.  Dropping the entry releases the bytes back to the
/// tracker.  The reservation is held alongside the batch so the spill
/// handler (CP2b) can compute the freed-bytes total exactly when it
/// drains the buffer.
struct BufferedBatch {
    batch: RecordBatch,
    /// Held-for-drop. CP2a never reads this; CP2b's spill handler will
    /// inspect `reservation.bytes()` to compute the total bytes freed
    /// when the buffer drains.
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

/// Internal state machine for `SortOperator`.
///
/// `std::mem::replace` is used in `next_batch` to move out of the current
/// variant without a double-borrow on `self`. If a panic or early `Err`
/// return occurs, the state is left as `Done`, which is safe for cleanup.
enum SortState {
    /// Phase 1: consuming input from the child.
    Accumulating {
        buffer: Vec<BufferedBatch>,
        total_rows: usize,
    },
    /// Phase 2: sorted output ready; drain one batch per `next_batch` call.
    Draining {
        output: Vec<RecordBatch>,
        /// Index of the next batch to emit.
        idx: usize,
    },
    /// Child was empty, all output has been emitted, or the operator is closed.
    Done,
}

impl SortOperator {
    /// Construct a new `SortOperator`.
    ///
    /// - `child` — the child operator whose output will be sorted.
    /// - `keys` — sort key expressions in priority order with their
    ///   directions.  An empty `keys` vec is legal; the operator will
    ///   emit all input rows in their original order (stable by
    ///   definition — `lexsort_to_indices` on zero keys is a no-op).
    /// - `max_rows` — maximum number of input rows before overflow error.
    ///   Use [`bqlite_planner::physical::DEFAULT_SORT_MAX_ROWS`] for the
    ///   default.
    /// - `cancel` — shared cancellation flag; checked at every
    ///   `next_batch` entry.
    /// - `budget` — per-query [`MemoryBudget`]. Each accumulated batch's
    ///   Arrow array bytes are reserved against this budget; tests that
    ///   don't care about enforcement can pass
    ///   `Arc::new(bqlite_core::UnboundedMemory::new())`.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        keys: Vec<(CompiledExpr, SortDirection)>,
        max_rows: usize,
        cancel: CancellationToken,
        budget: Arc<dyn MemoryBudget>,
    ) -> Self {
        let schema = child.output_schema().clone();
        let arrow_schema = Arc::new(schema.to_arrow_schema());
        Self {
            child,
            keys,
            max_rows,
            cancel,
            budget,
            schema,
            arrow_schema,
            state: SortState::Accumulating {
                buffer: Vec::new(),
                total_rows: 0,
            },
        }
    }

    /// Borrow the child operator (used by tests and the engine bind step
    /// when it needs to inspect the subtree).
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }
}

impl PhysicalOperator for SortOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    /// Drive the two-phase sort algorithm.
    ///
    /// Phase 1 calls return `Ok(Some(_))` only after the full sort
    /// completes and the first output chunk is ready. Phase 2 calls
    /// drain the pre-sorted chunks one per call. `Ok(None)` signals
    /// exhaustion.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Cancellation check at entry — before any child pull or output drain.
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Cancelled);
        }

        // `std::mem::replace` temporarily puts `Done` into `self.state`
        // so we can move out of the current variant without a double-borrow.
        // Every arm either restores a valid state or returns early.
        loop {
            match std::mem::replace(&mut self.state, SortState::Done) {
                // ── Phase 1: accumulate ────────────────────────────────────
                SortState::Accumulating {
                    mut buffer,
                    mut total_rows,
                } => {
                    match self.child.next_batch()? {
                        Some(batch) => {
                            let new_total = total_rows + batch.num_rows();
                            if new_total > self.max_rows {
                                // State is already `Done` from the replace — no restore needed.
                                return Err(BqliteError::Execution(format!(
                                    "SortOperator: input row count {} exceeds max_rows limit {}",
                                    new_total, self.max_rows
                                )));
                            }
                            // Charge the batch's Arrow array bytes to the
                            // budget. On `MemoryBudgetExceeded`, the
                            // budget has already exhausted its single
                            // retry through any registered spill handler
                            // (memory-budget.md § 4.1) — propagate.
                            // CP2b registers a real spill handler that
                            // can free bytes here; CP2a propagates the
                            // typed error.
                            let bytes = batch.get_array_memory_size() as u64;
                            let reservation = self.budget.try_reserve(bytes)?;
                            total_rows = new_total;
                            buffer.push(BufferedBatch { batch, reservation });
                            // Restore accumulating state and loop to pull next batch.
                            self.state = SortState::Accumulating { buffer, total_rows };

                            // Check cancellation between child pulls.
                            if self.cancel.is_cancelled() {
                                self.state = SortState::Done;
                                return Err(BqliteError::Cancelled);
                            }
                            // Continue looping to pull the next child batch.
                        }
                        None => {
                            // Child exhausted — transition to phase 2.
                            if total_rows == 0 {
                                // Empty input → empty output.
                                // State is already `Done`.
                                return Ok(None);
                            }
                            let output = sort_and_split(&buffer, &self.arrow_schema, &self.keys)?;
                            // Drop reservations once the rearranged
                            // output owns the data — the buffer's
                            // borrowed views into the input batches are
                            // no longer live after `take`.
                            drop(buffer);
                            self.state = SortState::Draining { output, idx: 0 };
                            // Loop immediately to drain the first output batch.
                        }
                    }
                }

                // ── Phase 2: drain sorted output ───────────────────────────
                SortState::Draining { output, mut idx } => {
                    if idx >= output.len() {
                        // All output emitted.
                        // State is already `Done`.
                        return Ok(None);
                    }
                    let batch = output[idx].clone();
                    idx += 1;
                    self.state = SortState::Draining { output, idx };
                    return Ok(Some(batch));
                }

                // ── Exhausted ──────────────────────────────────────────────
                SortState::Done => {
                    self.state = SortState::Done;
                    return Ok(None);
                }
            }
        }
    }

    /// Release resources: drop the accumulated buffer and drain queue,
    /// and close the child. Idempotent — safe to call more than once.  Only
    /// forwards `close()` to the child on the first call; subsequent calls
    /// are no-ops.
    fn close(&mut self) -> Result<()> {
        if !matches!(self.state, SortState::Done) {
            self.state = SortState::Done;
            return self.child.close();
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Concatenate `buffer`, sort by `keys`, split into `DEFAULT_OUTPUT_BATCH_SIZE`
/// chunks, and return the chunk list.
///
/// Returns an empty `Vec` if the combined batch has zero rows (the caller
/// already handles the pre-sort empty-input check, but this is a safety net).
fn sort_and_split(
    buffer: &[BufferedBatch],
    arrow_schema: &Arc<ArrowSchema>,
    keys: &[(CompiledExpr, SortDirection)],
) -> Result<Vec<RecordBatch>> {
    // ── Step 1: concat all buffered batches into one ──────────────────────
    let batches: Vec<RecordBatch> = buffer.iter().map(|b| b.batch.clone()).collect();
    let combined = concat_batches(arrow_schema, &batches)?;
    let num_rows = combined.num_rows();
    if num_rows == 0 {
        return Ok(vec![]);
    }

    // ── Steps 2–4: sort (skip when there are no sort keys) ────────────────
    // `arrow::compute::lexsort_to_indices` requires at least one column.
    // When `keys` is empty the caller wants a "sort by nothing" — i.e. the
    // original concatenation order is preserved, which is already stable.
    let sorted = if keys.is_empty() {
        combined
    } else {
        // Step 2: evaluate sort-key expressions over the combined batch.
        let sort_columns: Vec<SortColumn> = keys
            .iter()
            .map(|(expr, dir)| {
                let values = eval::evaluate(expr, &combined)?;
                Ok(SortColumn {
                    values,
                    options: Some(sort_options_for(dir)),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Step 3: stable lexicographic sort → row indices.
        let indices = lexsort_to_indices(&sort_columns, None)?;

        // Step 4: reorder every column of `combined` by `indices`.
        let sorted_cols: Vec<_> = combined
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map_err(BqliteError::Arrow))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(combined.schema(), sorted_cols)?
    };

    // ── Step 5: split into pipeline-sized output chunks ───────────────────
    // The final chunk may be smaller than DEFAULT_OUTPUT_BATCH_SIZE when
    // `num_rows` is not a multiple — that is expected and correct.
    let mut output = Vec::with_capacity(num_rows.div_ceil(DEFAULT_OUTPUT_BATCH_SIZE));
    let mut offset = 0;
    while offset < num_rows {
        let len = DEFAULT_OUTPUT_BATCH_SIZE.min(num_rows - offset);
        output.push(sorted.slice(offset, len));
        offset += len;
    }
    Ok(output)
}

/// Map a `SortDirection` to Arrow `SortOptions` following the null-ordering
/// convention from `sort-distinct.md §3.3` (matches DuckDB / BigQuery /
/// Oracle / Postgres defaults):
///
/// | Direction | NULL position | `SortOptions`                                |
/// |-----------|---------------|----------------------------------------------|
/// | `ASC`     | last          | `{ descending: false, nulls_first: false }`  |
/// | `DESC`    | first         | `{ descending: true,  nulls_first: true  }`  |
#[inline]
fn sort_options_for(dir: &SortDirection) -> SortOptions {
    match dir {
        SortDirection::Asc => SortOptions {
            descending: false,
            nulls_first: false,
        },
        SortDirection::Desc => SortOptions {
            descending: true,
            nulls_first: true,
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::memory::{MemoryBudget, MemoryTracker};
    use bqlite_core::{BqlType, BqliteError, ColumnDef, OperatorSchema, Result, UnboundedMemory};
    use bqlite_planner::compiled::{CompiledExpr, CompiledNode};
    use bqlite_planner::logical::SortDirection;

    fn unbounded_budget() -> Arc<dyn MemoryBudget> {
        Arc::new(UnboundedMemory::new())
    }

    use crate::operator::{CancellationToken, DEFAULT_OUTPUT_BATCH_SIZE};

    use super::SortOperator;
    use crate::operator::PhysicalOperator;

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

    /// Build a `CompiledExpr` that references column 0 by index.
    fn col0_expr() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: "v".to_string(),
            },
            result_type: BqlType::Int,
            nullable: false,
        }
    }

    // ── sort_options_for ─────────────────────────────────────────────────────

    #[test]
    fn asc_produces_nulls_last() {
        use super::sort_options_for;
        let opts = sort_options_for(&SortDirection::Asc);
        assert!(!opts.descending);
        assert!(!opts.nulls_first); // NULLs last
    }

    #[test]
    fn desc_produces_nulls_first() {
        use super::sort_options_for;
        let opts = sort_options_for(&SortDirection::Desc);
        assert!(opts.descending);
        assert!(opts.nulls_first); // NULLs first
    }

    // ── empty input ──────────────────────────────────────────────────────────

    #[test]
    fn sort_empty_input_returns_none() {
        let child = Box::new(VecOp::new(int_schema(), vec![]));
        let mut op = SortOperator::new(
            child,
            vec![],
            1000,
            CancellationToken::new(),
            unbounded_budget(),
        );
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── single batch sort ────────────────────────────────────────────────────

    #[test]
    fn sort_single_batch_asc() {
        let batch = make_int_batch(&[5, 3, 1, 4, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);

        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn sort_single_batch_desc() {
        let batch = make_int_batch(&[3, 1, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Desc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(values, vec![3, 2, 1]);
    }

    // ── multi-batch sort ─────────────────────────────────────────────────────

    #[test]
    fn sort_multiple_batches_concatenated_and_sorted() {
        let b1 = make_int_batch(&[4, 6]);
        let b2 = make_int_batch(&[1, 5]);
        let b3 = make_int_batch(&[2, 3]);
        let child = Box::new(VecOp::new(int_schema(), vec![b1, b2, b3]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        // Collect all output rows across potentially multiple batches.
        let mut all_values: Vec<i64> = Vec::new();
        while let Some(batch) = op.next_batch().unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            all_values.extend((0..col.len()).map(|i| col.value(i)));
        }
        assert_eq!(all_values, vec![1, 2, 3, 4, 5, 6]);
    }

    // ── stability ────────────────────────────────────────────────────────────

    #[test]
    fn sort_is_stable_on_equal_keys() {
        // Two-column schema: key col (all same) + order col (to detect stability).
        let key_col: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 1, 1, 1]));
        let order_col: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40]));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("order", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(arrow_schema, vec![key_col, order_col]).unwrap();

        let schema = OperatorSchema::new(vec![
            ColumnDef::required("key", BqlType::Int),
            ColumnDef::required("order", BqlType::Int),
        ])
        .unwrap();
        let child = Box::new(VecOp::new(schema, vec![batch]));

        // Sort by col 0 only (all equal); original order of col 1 must be preserved.
        let key_expr = CompiledExpr {
            node: CompiledNode::Column {
                index: 0,
                name: "key".to_string(),
            },
            result_type: BqlType::Int,
            nullable: false,
        };
        let keys = vec![(key_expr, SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let order_values: Vec<i64> = {
            let col = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..col.len()).map(|i| col.value(i)).collect()
        };
        assert_eq!(order_values, vec![10, 20, 30, 40], "sort must be stable");
    }

    // ── output schema unchanged ───────────────────────────────────────────────

    #[test]
    fn sort_output_schema_unchanged() {
        let schema = int_schema();
        let child = Box::new(VecOp::new(schema.clone(), vec![]));
        let op = SortOperator::new(
            child,
            vec![],
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );
        assert_eq!(*op.output_schema(), schema);
    }

    // ── max_rows cap ─────────────────────────────────────────────────────────

    #[test]
    fn sort_exceeds_max_rows_returns_execution_error() {
        let batch = make_int_batch(&[1, 2, 3]); // 3 rows
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = SortOperator::new(
            child,
            vec![],
            2, /* cap at 2 */
            CancellationToken::new(),
            unbounded_budget(),
        );

        match op.next_batch() {
            Err(BqliteError::Execution(msg)) => {
                assert_eq!(
                    msg,
                    "SortOperator: input row count 3 exceeds max_rows limit 2"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn sort_exactly_at_max_rows_is_allowed() {
        let batch = make_int_batch(&[2, 1]); // 2 rows, cap is 2
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op =
            SortOperator::new(child, keys, 2, CancellationToken::new(), unbounded_budget());

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
    }

    // ── cancellation ─────────────────────────────────────────────────────────

    #[test]
    fn sort_cancelled_before_first_call_returns_cancelled() {
        let token = CancellationToken::new();
        token.cancel();
        let child = Box::new(VecOp::new(int_schema(), vec![make_int_batch(&[1, 2])]));
        let mut op = SortOperator::new(child, vec![], 1_000_000, token, unbounded_budget());

        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    #[test]
    fn sort_cancelled_during_drain_returns_cancelled() {
        // Forces 2 output chunks; cancels the token after the first drain call.
        let token = CancellationToken::new();
        let n = DEFAULT_OUTPUT_BATCH_SIZE + 1;
        let values: Vec<i64> = (0..n as i64).collect();
        let batch = make_int_batch(&values);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(child, keys, n + 100, token.clone(), unbounded_budget());

        // First call triggers full sort and returns the first output batch.
        let first = op.next_batch().unwrap();
        assert!(first.is_some(), "expected first output batch");

        // Cancel before the second drain call.
        token.cancel();
        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled during drain, got {other:?}"),
        }
    }

    // ── output batch splitting ───────────────────────────────────────────────

    #[test]
    fn sort_splits_output_into_batch_size_chunks() {
        // Produce DEFAULT_OUTPUT_BATCH_SIZE + 1 rows; expect 2 output batches.
        let n = DEFAULT_OUTPUT_BATCH_SIZE + 1;
        let values: Vec<i64> = (0..n as i64).rev().collect(); // descending → need sort
        let batch = make_int_batch(&values);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let keys = vec![(col0_expr(), SortDirection::Asc)];
        let mut op = SortOperator::new(
            child,
            keys,
            n + 100,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let b1 = op.next_batch().unwrap().unwrap();
        assert_eq!(b1.num_rows(), DEFAULT_OUTPUT_BATCH_SIZE);

        let b2 = op.next_batch().unwrap().unwrap();
        assert_eq!(b2.num_rows(), 1);

        assert!(op.next_batch().unwrap().is_none());
    }

    // ── close is idempotent ──────────────────────────────────────────────────

    #[test]
    fn sort_close_is_idempotent() {
        let child = Box::new(VecOp::new(int_schema(), vec![]));
        let mut op = SortOperator::new(
            child,
            vec![],
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );
        op.close().unwrap();
        op.close().unwrap();
        // After close, next_batch returns None (Done state).
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── no-key sort ───────────────────────────────────────────────────────────

    #[test]
    fn sort_with_no_keys_preserves_order() {
        let batch = make_int_batch(&[3, 1, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let mut op = SortOperator::new(
            child,
            vec![], /* no keys */
            1_000_000,
            CancellationToken::new(),
            unbounded_budget(),
        );

        let out = op.next_batch().unwrap().unwrap();
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        // With no sort keys, lexsort is a no-op and rows keep their input order.
        let values: Vec<i64> = (0..col.len()).map(|i| col.value(i)).collect();
        assert_eq!(values, vec![3, 1, 2]);
    }

    // ── Memory budget plumbing (TASK-513 CP2a) ────────────────────────────────

    #[test]
    fn sort_charges_buffered_batches_to_budget() -> Result<()> {
        // Sort accumulates two batches; the tracker must observe a peak
        // ≥ Σ get_array_memory_size of the input batches.
        let batch_a = make_int_batch(&[3, 1, 2]);
        let batch_b = make_int_batch(&[6, 5, 4]);
        let expected_min_peak =
            (batch_a.get_array_memory_size() + batch_b.get_array_memory_size()) as u64;
        let child = Box::new(VecOp::new(int_schema(), vec![batch_a, batch_b]));
        let tracker = MemoryTracker::new(1_000_000);
        let budget: Arc<dyn MemoryBudget> = tracker.clone();
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
        );
        // Drive the operator until it returns one full output batch.
        let _ = op.next_batch()?.expect("must produce output");
        // After phase-2 transition the buffer is dropped, so peak is what
        // we want to assert against — used has already returned to ~0.
        assert!(
            tracker.peak_bytes() >= expected_min_peak,
            "peak {} should be at least {}",
            tracker.peak_bytes(),
            expected_min_peak
        );
        Ok(())
    }

    #[test]
    fn sort_overflow_returns_typed_budget_error() {
        // Tracker with a tiny budget (smaller than even one Int64Array
        // batch) — the first try_reserve must surface
        // `BqliteError::MemoryBudgetExceeded` since no spill handler is
        // registered in CP2a.
        let batch = make_int_batch(&[1, 2, 3, 4, 5]);
        let bytes = batch.get_array_memory_size() as u64;
        let tiny_budget = bytes.saturating_sub(1);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let tracker = MemoryTracker::new(tiny_budget);
        let budget: Arc<dyn MemoryBudget> = tracker;
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
        );
        match op.next_batch() {
            Err(BqliteError::MemoryBudgetExceeded { used, budget }) => {
                assert_eq!(budget, tiny_budget);
                assert!(
                    used >= bytes,
                    "used should reflect the rejected reservation: {used}"
                );
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
    }

    #[test]
    fn sort_releases_reservation_after_drain() -> Result<()> {
        // After the merge-and-drain phase completes and every output
        // batch has been read, all reservations are released.
        let batch = make_int_batch(&[3, 1, 2]);
        let child = Box::new(VecOp::new(int_schema(), vec![batch]));
        let tracker = MemoryTracker::new(1_000_000);
        let budget: Arc<dyn MemoryBudget> = tracker.clone();
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            1_000_000,
            CancellationToken::new(),
            budget,
        );
        // Drive to completion.
        while op.next_batch()?.is_some() {}
        assert_eq!(
            tracker.used_bytes(),
            0,
            "all reservations must be released after drain"
        );
        Ok(())
    }
}
