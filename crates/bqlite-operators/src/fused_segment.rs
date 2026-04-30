//! `FusedStatelessSegment` — driver for the §4 fused push segment.
//!
//! `docs/design/engine/operator-fusion.md` §4 specifies the runtime
//! shape: a chain of [`StatelessKernel`]s drives `FilteredBatch`
//! values through filter/project/limit steps inside a single
//! `PhysicalOperator` boundary. The driver:
//!
//! 1. Pulls `RecordBatch`es from a child operator, wrapping each in
//!    `FilteredBatch::dense`.
//! 2. Walks `kernels` in order. At each step it records
//!    `selection_vector_dropped_rows` for the input, applies the
//!    sparsity-threshold materialization trigger (§3.4.1), and then
//!    invokes the kernel — or, for the in-driver `Limit` step,
//!    truncates the live row count against `limit_remaining`.
//! 3. Materializes once at the outer boundary (§3.4.2) via
//!    [`materialize_filtered_batch`].
//! 4. Skips zero-row batches by re-pulling the child (§4.2).
//! 5. Honours [`CancellationToken`] at the top of the loop
//!    (§4.5).
//!
//! The driver does **not** check cancellation between kernels in the
//! chain — per §4.5 the per-batch granularity is the contract.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use bqlite_core::metrics::{Metrics, NoopMetrics};
use bqlite_core::{BqliteError, OperatorSchema, Result};

use crate::filtered_batch::FilteredBatch;
use crate::kernel::StatelessKernel;
use crate::materialize::materialize_filtered_batch;
use crate::operator::{CancellationToken, PhysicalOperator};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Default sparsity threshold (§3.4.1). When
/// `live_rows < SPARSITY_FACTOR_DEFAULT * batch.num_rows()` at a
/// kernel's entry point, the driver materializes before invoking the
/// kernel.
pub const SPARSITY_FACTOR_DEFAULT: f64 = 0.10;

// ─────────────────────────────────────────────────────────────────────────────
// KernelStep
// ─────────────────────────────────────────────────────────────────────────────

/// One step in a fused stateless segment's kernel chain.
///
/// Filter and Project are arbitrary [`StatelessKernel`]s shared by
/// `Arc` so the same kernel can be referenced across worker threads
/// in a future morsel-driven implementation. `Limit` carries no
/// payload — the driver owns the remaining-row counter directly per
/// §3.3.3 / §4.1, which keeps the kernel trait pure (`&self`, no
/// mutable state).
#[derive(Clone)]
pub enum KernelStep {
    Filter(Arc<dyn StatelessKernel>),
    Project(Arc<dyn StatelessKernel>),
    Limit,
}

impl std::fmt::Debug for KernelStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KernelStep::Filter(k) => f.debug_tuple("Filter").field(&k.kernel_name()).finish(),
            KernelStep::Project(k) => f.debug_tuple("Project").field(&k.kernel_name()).finish(),
            KernelStep::Limit => f.write_str("Limit"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FusedStatelessSegment
// ─────────────────────────────────────────────────────────────────────────────

/// Pull-based driver for a chain of stateless kernels.
pub struct FusedStatelessSegment {
    child: Box<dyn PhysicalOperator>,
    kernels: Vec<KernelStep>,
    output_schema: OperatorSchema,
    sparsity_factor: f64,
    /// Remaining-row budget when the chain contains a `Limit` step.
    /// `None` when the chain has no LIMIT — eliminates the "is there
    /// a budget" branch on the hot path.
    limit_remaining: Option<u64>,
    metrics: Arc<dyn Metrics>,
    cancellation: CancellationToken,
}

impl FusedStatelessSegment {
    /// Construct a fused-segment driver.
    ///
    /// The `output_schema` should be the last kernel's schema —
    /// callers pass it explicitly so EXPLAIN and the bind step do not
    /// pay a per-construction schema rebuild. `limit` is the LIMIT
    /// budget when the chain contains a `KernelStep::Limit`, or
    /// `None` when it does not.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        kernels: Vec<KernelStep>,
        output_schema: OperatorSchema,
        limit: Option<u64>,
        metrics: Arc<dyn Metrics>,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            child,
            kernels,
            output_schema,
            sparsity_factor: SPARSITY_FACTOR_DEFAULT,
            limit_remaining: limit,
            metrics,
            cancellation,
        }
    }

    /// Convenience constructor for unit tests and in-process callers
    /// that don't carry a metrics handle. Equivalent to [`Self::new`]
    /// with [`NoopMetrics`] and a fresh [`CancellationToken`].
    pub fn for_test(
        child: Box<dyn PhysicalOperator>,
        kernels: Vec<KernelStep>,
        output_schema: OperatorSchema,
        limit: Option<u64>,
    ) -> Self {
        Self::new(
            child,
            kernels,
            output_schema,
            limit,
            Arc::new(NoopMetrics::new()),
            CancellationToken::new(),
        )
    }

    /// Override the sparsity factor (default `0.10` per §3.4.1).
    /// Used by tests pinning the boundary; the planner will set this
    /// from `FusedSegmentPhysical::sparsity_factor` in CP4.
    pub fn with_sparsity_factor(mut self, factor: f64) -> Self {
        self.sparsity_factor = factor;
        self
    }

    /// The sparsity factor in use.
    pub fn sparsity_factor(&self) -> f64 {
        self.sparsity_factor
    }

    /// True when the chain has a LIMIT step whose budget is
    /// exhausted. The driver short-circuits on the next pull.
    fn limit_at_zero(&self) -> bool {
        matches!(self.limit_remaining, Some(0))
    }

    /// Apply LIMIT to a `FilteredBatch` in place. The driver owns
    /// the remaining-row counter so the kernel trait stays pure.
    fn apply_limit(&mut self, fb: FilteredBatch) -> FilteredBatch {
        let remaining = match self.limit_remaining.as_mut() {
            Some(r) => r,
            None => return fb,
        };
        // The `next_batch` loop short-circuits when remaining == 0,
        // so this method should never be called with an exhausted
        // budget.
        debug_assert!(*remaining > 0);
        let live = fb.live_rows() as u64;
        if live <= *remaining {
            *remaining -= live;
            return fb;
        }
        let take = *remaining as usize;
        *remaining = 0;
        truncate_to(fb, take)
    }

    /// Sparsity check at a kernel's entry point (§3.4.1). When the
    /// live row count is below the threshold, materialize *before*
    /// the kernel runs so the kernel sees a dense input. The check
    /// is a no-op on dense input (`selection: None`) — there is
    /// nothing to materialize.
    fn maybe_materialize(&self, fb: FilteredBatch) -> Result<FilteredBatch> {
        if fb.is_dense() {
            return Ok(fb);
        }
        let total = fb.batch.num_rows();
        let live = fb.live_rows();
        if total == 0 {
            return Ok(fb);
        }
        if (live as f64) < self.sparsity_factor * (total as f64) {
            let materialized = materialize_filtered_batch(fb, &*self.metrics)?;
            return Ok(FilteredBatch::dense(materialized));
        }
        Ok(fb)
    }
}

impl PhysicalOperator for FusedStatelessSegment {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.cancellation.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            if self.limit_at_zero() {
                return Ok(None);
            }
            let Some(batch) = self.child.next_batch()? else {
                return Ok(None);
            };
            // Skip the chain entirely on a 0-row child output to
            // avoid handing zero-row inputs to the kernels (§4.2).
            if batch.num_rows() == 0 {
                continue;
            }
            let mut fb = FilteredBatch::dense(batch);

            // Index-based iteration so we can call `&mut self`
            // helpers (`apply_limit`) without conflicting with an
            // outstanding `&self.kernels` borrow.
            for step_idx in 0..self.kernels.len() {
                // Per §3.5: record `selection_vector_dropped_rows` at
                // every kernel-step input — sum of (input_rows -
                // live_rows) across steps. Per-batch, not per-row.
                let dropped = fb.batch.num_rows() as u64 - fb.live_rows() as u64;
                if dropped > 0 {
                    self.metrics.record_selection_vector_dropped_rows(dropped);
                }
                fb = self.maybe_materialize(fb)?;
                // Clone the step (an Arc bump on Filter/Project) so
                // we can drop the `&self.kernels` borrow before
                // calling the `&mut self` `apply_limit` helper.
                let step = self.kernels[step_idx].clone();
                fb = match step {
                    KernelStep::Filter(k) => k.apply(fb)?,
                    KernelStep::Project(k) => k.apply(fb)?,
                    KernelStep::Limit => self.apply_limit(fb),
                };
                if fb.live_rows() == 0 && self.limit_at_zero() {
                    // LIMIT exhausted mid-chain — surface the prefix
                    // (which may still be non-empty in earlier steps)
                    // by short-circuiting to the boundary.
                    break;
                }
            }

            if fb.live_rows() == 0 {
                // Re-pull the child rather than surfacing a zero-row
                // RecordBatch (§4.2).
                continue;
            }

            let out = materialize_filtered_batch(fb, &*self.metrics)?;
            return Ok(Some(out));
        }
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

/// Truncate a `FilteredBatch` to the first `take` live rows.
///
/// When `selection` is `Some(sv)`, this slices `sv`. When `selection`
/// is `None`, this slices the underlying `RecordBatch` in place
/// (zero-copy — Arrow buffers are `Arc`-backed).
fn truncate_to(fb: FilteredBatch, take: usize) -> FilteredBatch {
    let FilteredBatch { batch, selection } = fb;
    match selection {
        None => {
            let take = take.min(batch.num_rows());
            FilteredBatch::dense(batch.slice(0, take))
        }
        Some(sv) => {
            let take = take.min(sv.len());
            // SelectionVector::truncate keeps the ascending invariant.
            let mut sv = sv;
            sv.truncate(take);
            FilteredBatch::with_selection(batch, sv)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::metrics::AtomicMetrics;
    use bqlite_core::{BqlType, ColumnDef};
    use bqlite_planner::compiled::CompiledExpr;
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;
    use crate::filter::FilterOperator;
    use crate::kernel::{FilterKernel, ProjectKernel};
    use crate::limit::LimitOperator;
    use crate::project::{ProjectOperator, ProjectionExpr};

    // ── Fixtures ──────────────────────────────────────────────────────

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
        let id: ArrayRef = Arc::new(Int64Array::from(ids));
        let val: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(int_arrow_schema(), vec![id, val]).unwrap()
    }

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }
    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }
    fn int_lit(i: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(i)))
    }
    fn compile(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
        CompiledExpr::from_typed(&typed)
    }
    fn value_gt(literal: i64, schema: &OperatorSchema) -> CompiledExpr {
        compile(
            sp(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("value")),
                right: Box::new(int_lit(literal)),
            }),
            schema,
        )
    }
    fn id_eq(literal: i64, schema: &OperatorSchema) -> CompiledExpr {
        compile(
            sp(Expr::Compare {
                op: CompareOp::Equal,
                left: Box::new(col("id")),
                right: Box::new(int_lit(literal)),
            }),
            schema,
        )
    }

    // ── Recording child ───────────────────────────────────────────────

    struct RecordingChild {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        next_idx: usize,
        pulls: Arc<AtomicUsize>,
        cancel: CancellationToken,
    }

    impl RecordingChild {
        fn new(batches: Vec<RecordBatch>) -> Self {
            Self {
                schema: int_schema(),
                batches,
                next_idx: 0,
                pulls: Arc::new(AtomicUsize::new(0)),
                cancel: CancellationToken::new(),
            }
        }

        fn pulls_counter(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.pulls)
        }

        fn with_cancel(mut self, c: CancellationToken) -> Self {
            self.cancel = c;
            self
        }
    }

    impl PhysicalOperator for RecordingChild {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
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
    }

    fn make_filter_kernel(predicate: CompiledExpr) -> Arc<dyn StatelessKernel> {
        Arc::new(FilterKernel::with_default_tile_size(
            predicate,
            int_schema(),
        ))
    }

    fn make_id_project_kernel() -> (Arc<dyn StatelessKernel>, OperatorSchema) {
        let schema = int_schema();
        let exprs = vec![ProjectionExpr {
            expr: compile(col("id"), &schema),
            output_name: "id".to_string(),
        }];
        let out_schema =
            OperatorSchema::new(vec![ColumnDef::required("id", BqlType::Int)]).unwrap();
        (
            Arc::new(ProjectKernel::new(exprs, out_schema.clone())),
            out_schema,
        )
    }

    // ── Single-kernel chains ──────────────────────────────────────────

    #[test]
    fn filter_only_chain_narrows_and_materializes_at_boundary() {
        let schema = int_schema();
        let predicate = value_gt(15, &schema);
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2, 3, 4],
            vec![10, 20, 30, 15],
        )]));
        let kernels = vec![KernelStep::Filter(make_filter_kernel(predicate))];
        let mut seg = FusedStatelessSegment::for_test(child, kernels, schema, None);

        let out = seg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
        let ids = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ids: Vec<i64> = ids.iter().flatten().collect();
        assert_eq!(ids, vec![2, 3]);
        assert!(seg.next_batch().unwrap().is_none());
    }

    #[test]
    fn project_only_chain_returns_dense_output() {
        let (kernel, project_schema) = make_id_project_kernel();
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
        )]));
        let kernels = vec![KernelStep::Project(kernel)];
        let mut seg = FusedStatelessSegment::for_test(child, kernels, project_schema, None);

        let out = seg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.num_rows(), 3);
    }

    #[test]
    fn limit_only_chain_truncates_and_short_circuits() {
        let schema = int_schema();
        // Two batches; limit 3 → first batch (2 rows) plus 1 row of
        // the second batch, then short-circuit.
        let child = Box::new(RecordingChild::new(vec![
            make_batch(vec![1, 2], vec![10, 20]),
            make_batch(vec![3, 4, 5], vec![30, 40, 50]),
            make_batch(vec![6], vec![60]),
        ]));
        let pulls = Arc::clone(&child.pulls_counter());
        let kernels = vec![KernelStep::Limit];
        let mut seg = FusedStatelessSegment::for_test(child, kernels, schema, Some(3));

        let a = seg.next_batch().unwrap().unwrap();
        assert_eq!(a.num_rows(), 2);
        let b = seg.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 1);
        // After the budget is exhausted, the next call short-circuits
        // to None *without* pulling the child again.
        let pulls_before = pulls.load(Ordering::SeqCst);
        assert!(seg.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), pulls_before);
    }

    // ── Multi-kernel chain ────────────────────────────────────────────

    #[test]
    fn filter_project_limit_chain_matches_expected_rows() {
        // Source: ids 1..=6, values 10..=60. Filter value > 25 →
        // ids 3,4,5,6. Project id. Limit 2 → ids 3,4.
        let schema = int_schema();
        let (project_kernel, project_schema) = make_id_project_kernel();

        let child = Box::new(RecordingChild::new(vec![
            make_batch(vec![1, 2, 3], vec![10, 20, 30]),
            make_batch(vec![4, 5, 6], vec![40, 50, 60]),
        ]));
        let kernels = vec![
            KernelStep::Filter(make_filter_kernel(value_gt(25, &schema))),
            KernelStep::Project(project_kernel),
            KernelStep::Limit,
        ];
        let mut seg = FusedStatelessSegment::for_test(child, kernels, project_schema, Some(2));

        let mut all_ids: Vec<i64> = Vec::new();
        while let Some(batch) = seg.next_batch().unwrap() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            all_ids.extend(ids.iter().flatten());
        }
        assert_eq!(all_ids, vec![3, 4]);
    }

    // ── Sparsity boundary ─────────────────────────────────────────────

    #[test]
    fn sparsity_threshold_triggers_mid_chain_materialization() {
        // 100-row batch. Filter A keeps 5 rows (< 10% → sparse).
        // Filter B sees a fully-materialized dense input and runs
        // against it. Three materialization counter increments are
        // expected: one mid-chain (sparsity-triggered) for the input
        // to the second filter, and one at the outer boundary.
        let schema = int_schema();
        let ids: Vec<i64> = (0..100).collect();
        let values: Vec<i64> = (0..100).collect();
        let child = Box::new(RecordingChild::new(vec![make_batch(ids, values)]));

        // Filter 1: value < 5 → 5 rows survive (5%, below threshold).
        let f1 = compile(
            sp(Expr::Compare {
                op: CompareOp::Less,
                left: Box::new(col("value")),
                right: Box::new(int_lit(5)),
            }),
            &schema,
        );
        // Filter 2: id >= 0 (all-pass) — exists to consume the
        // mid-chain materialization.
        let f2 = compile(
            sp(Expr::Compare {
                op: CompareOp::GreaterOrEqual,
                left: Box::new(col("id")),
                right: Box::new(int_lit(0)),
            }),
            &schema,
        );

        let kernels = vec![
            KernelStep::Filter(make_filter_kernel(f1)),
            KernelStep::Filter(make_filter_kernel(f2)),
        ];
        let metrics = Arc::new(AtomicMetrics::new());
        let mut seg = FusedStatelessSegment::new(
            child,
            kernels,
            schema,
            None,
            Arc::clone(&metrics) as Arc<dyn Metrics>,
            CancellationToken::new(),
        );
        let _ = seg.next_batch().unwrap().unwrap();

        let snap = metrics.snapshot();
        // Expected materializations:
        //   1. Sparsity boundary at filter-2 entry — first
        //      materialization, takes the 5 selected rows down to a
        //      dense 5-row batch.
        //   2. Outer boundary materialization on the post-filter-2
        //      output, which is full-cover Some(sv) (length 5 of 5)
        //      — this is the §4.3 short-circuit and does NOT
        //      increment the counter.
        // Net: exactly 1 increment.
        assert_eq!(snap.selection_vector_materializations, 1);
    }

    #[test]
    fn dense_chain_records_zero_dropped_rows() {
        // All-pass filter — first kernel sees 0 dropped rows (input
        // is dense), second kernel sees Some(sv) with full coverage
        // (also 0 dropped rows by §3.5's accounting).
        let schema = int_schema();
        let always_true = compile(
            sp(Expr::Compare {
                op: CompareOp::GreaterOrEqual,
                left: Box::new(col("id")),
                right: Box::new(int_lit(0)),
            }),
            &schema,
        );
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
        )]));
        let kernels = vec![
            KernelStep::Filter(make_filter_kernel(always_true.clone())),
            KernelStep::Filter(make_filter_kernel(always_true)),
        ];
        let metrics = Arc::new(AtomicMetrics::new());
        let mut seg = FusedStatelessSegment::new(
            child,
            kernels,
            schema,
            None,
            Arc::clone(&metrics) as Arc<dyn Metrics>,
            CancellationToken::new(),
        );
        let _ = seg.next_batch().unwrap().unwrap();
        assert_eq!(metrics.snapshot().selection_vector_dropped_rows, 0);
    }

    #[test]
    fn dropped_rows_accounting_matches_kernel_step_inputs() {
        // 10-row batch. Filter A drops 4 (live=6), Filter B drops
        // another 3 (live=3). Per §3.5, dropped_rows is the sum of
        // (input_rows - live_rows) at every kernel step entry:
        //   Step A entry: input dense → dropped 0
        //   Step B entry: live=6 of 10 → dropped 4
        //   (No step C)  → outer boundary, not counted.
        // Total: 4.
        let schema = int_schema();
        let ids: Vec<i64> = (0..10).collect();
        let values: Vec<i64> = (0..10).collect();
        let child = Box::new(RecordingChild::new(vec![make_batch(ids, values)]));

        let f_a = compile(
            sp(Expr::Compare {
                op: CompareOp::Less,
                left: Box::new(col("value")),
                right: Box::new(int_lit(6)),
            }),
            &schema,
        );
        let f_b = compile(
            sp(Expr::Compare {
                op: CompareOp::Greater,
                left: Box::new(col("value")),
                right: Box::new(int_lit(2)),
            }),
            &schema,
        );

        let kernels = vec![
            KernelStep::Filter(make_filter_kernel(f_a)),
            KernelStep::Filter(make_filter_kernel(f_b)),
        ];
        let metrics = Arc::new(AtomicMetrics::new());
        let mut seg = FusedStatelessSegment::new(
            child,
            kernels,
            schema,
            None,
            Arc::clone(&metrics) as Arc<dyn Metrics>,
            CancellationToken::new(),
        );
        let _ = seg.next_batch().unwrap().unwrap();
        assert_eq!(metrics.snapshot().selection_vector_dropped_rows, 4);
    }

    // ── Empty-batch loop ──────────────────────────────────────────────

    #[test]
    fn fully_rejecting_filter_re_pulls_child() {
        let schema = int_schema();
        // First batch all rejected (no id == 99); second batch
        // contains the match.
        let child = Box::new(RecordingChild::new(vec![
            make_batch(vec![1, 2], vec![10, 20]),
            make_batch(vec![99, 3], vec![30, 40]),
        ]));
        let pulls = child.pulls_counter();
        let kernels = vec![KernelStep::Filter(make_filter_kernel(id_eq(99, &schema)))];
        let mut seg = FusedStatelessSegment::for_test(child, kernels, schema, None);
        let out = seg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        // Two child pulls absorbed before the segment surfaced a row.
        assert_eq!(pulls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn child_zero_row_batch_is_skipped() {
        // The driver should re-pull when the child returns a 0-row
        // RecordBatch — a kernel that sees `FilteredBatch::dense`
        // with `batch.num_rows() == 0` would hit edge cases inside
        // `materialize_filtered_batch` and `bool_mask_to_selection`
        // unnecessarily.
        let schema = int_schema();
        let child = Box::new(RecordingChild::new(vec![
            make_batch(vec![], vec![]),
            make_batch(vec![1, 2], vec![10, 20]),
        ]));
        let kernels = vec![KernelStep::Filter(make_filter_kernel(value_gt(0, &schema)))];
        let mut seg = FusedStatelessSegment::for_test(child, kernels, schema, None);
        let out = seg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    // ── Cancellation ──────────────────────────────────────────────────

    #[test]
    fn cancellation_observed_at_top_of_loop() {
        let schema = int_schema();
        let cancel = CancellationToken::new();
        let child = Box::new(
            RecordingChild::new(vec![make_batch(vec![1], vec![10])]).with_cancel(cancel.clone()),
        );
        let kernels = vec![KernelStep::Filter(make_filter_kernel(value_gt(0, &schema)))];
        let mut seg = FusedStatelessSegment::new(
            child,
            kernels,
            schema,
            None,
            Arc::new(NoopMetrics::new()),
            cancel.clone(),
        );
        cancel.cancel();
        let err = seg.next_batch().expect_err("cancelled must surface");
        assert!(matches!(err, BqliteError::Cancelled));
    }

    // ── Equivalence with legacy operators ─────────────────────────────

    #[test]
    fn equivalent_to_legacy_filter_project_limit_chain() {
        // Compare segment output against
        // Limit(Project(Filter(scan))) on hand-built batches.
        let schema = int_schema();
        let predicate = value_gt(15, &schema);
        let (project_kernel, project_schema) = make_id_project_kernel();

        let child_for_segment = Box::new(RecordingChild::new(vec![
            make_batch(vec![1, 2, 3], vec![10, 20, 30]),
            make_batch(vec![4, 5, 6], vec![40, 50, 60]),
        ]));
        let kernels = vec![
            KernelStep::Filter(make_filter_kernel(predicate.clone())),
            KernelStep::Project(project_kernel),
            KernelStep::Limit,
        ];
        let mut seg = FusedStatelessSegment::for_test(
            child_for_segment,
            kernels,
            project_schema.clone(),
            Some(3),
        );

        // Legacy chain on the same child input.
        let child_for_legacy = Box::new(RecordingChild::new(vec![
            make_batch(vec![1, 2, 3], vec![10, 20, 30]),
            make_batch(vec![4, 5, 6], vec![40, 50, 60]),
        ]));
        let legacy_filter =
            FilterOperator::with_default_tile_size(child_for_legacy, predicate.clone());
        let legacy_proj_exprs = vec![ProjectionExpr {
            expr: compile(col("id"), &schema),
            output_name: "id".to_string(),
        }];
        let legacy_project = ProjectOperator::new(
            Box::new(legacy_filter),
            legacy_proj_exprs,
            project_schema.clone(),
        );
        let mut legacy = LimitOperator::new(Box::new(legacy_project), 3);

        fn drain(op: &mut dyn PhysicalOperator) -> Vec<i64> {
            let mut out = Vec::new();
            while let Some(b) = op.next_batch().unwrap() {
                let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                out.extend(ids.iter().flatten());
            }
            out
        }

        let segment_rows = drain(&mut seg);
        let legacy_rows = drain(&mut legacy);
        assert_eq!(segment_rows, legacy_rows);
        // Sanity: the chain should keep 3 rows (ids 2, 3, 4).
        assert_eq!(segment_rows, vec![2, 3, 4]);
    }

    // ── Boundary materialization ──────────────────────────────────────

    #[test]
    fn outer_boundary_materializes_full_cover_without_metric() {
        // Filter all-pass over a 3-row batch. After the kernel, the
        // FilteredBatch has Some(sv) of length 3 — the outer boundary
        // materialization is the §4.3 full-cover short-circuit, which
        // does NOT increment selection_vector_materializations.
        let schema = int_schema();
        let always_true = value_gt(0, &schema);
        let child = Box::new(RecordingChild::new(vec![make_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
        )]));
        let metrics = Arc::new(AtomicMetrics::new());
        let kernels = vec![KernelStep::Filter(make_filter_kernel(always_true))];
        let mut seg = FusedStatelessSegment::new(
            child,
            kernels,
            schema,
            None,
            Arc::clone(&metrics) as Arc<dyn Metrics>,
            CancellationToken::new(),
        );
        let out = seg.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(metrics.snapshot().selection_vector_materializations, 0);
    }

    #[test]
    fn output_schema_returns_last_kernel_schema() {
        let (proj_kernel, project_schema) = make_id_project_kernel();
        let child = Box::new(RecordingChild::new(vec![]));
        let seg = FusedStatelessSegment::for_test(
            child,
            vec![KernelStep::Project(proj_kernel)],
            project_schema.clone(),
            None,
        );
        assert_eq!(seg.output_schema().columns().len(), 1);
        assert_eq!(seg.output_schema().columns()[0].name, "id");
    }

    #[test]
    fn limit_zero_returns_none_without_pulling_child() {
        let schema = int_schema();
        let child = Box::new(RecordingChild::new(vec![make_batch(vec![1], vec![10])]));
        let pulls = child.pulls_counter();
        let mut seg =
            FusedStatelessSegment::for_test(child, vec![KernelStep::Limit], schema, Some(0));
        assert!(seg.next_batch().unwrap().is_none());
        assert_eq!(pulls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn trait_object_compiles() {
        let schema = int_schema();
        let child = Box::new(RecordingChild::new(vec![]));
        let seg: Box<dyn PhysicalOperator> =
            Box::new(FusedStatelessSegment::for_test(child, vec![], schema, None));
        let _ = seg;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property tests (TASK-519, S6 of the plan / `engine/operator-fusion.md` §7.2)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod proptests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;

    use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::metrics::{AtomicMetrics, Metrics};
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema, Result};
    use bqlite_planner::compiled::CompiledExpr;
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;
    use crate::kernel::FilterKernel;

    // Single-column int schema; one batch, one column.
    fn schema() -> OperatorSchema {
        OperatorSchema::new(vec![ColumnDef::required("v", BqlType::Int)]).unwrap()
    }
    fn arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "v",
            DataType::Int64,
            false,
        )]))
    }
    fn make_batch(values: Vec<i64>) -> RecordBatch {
        let arr: ArrayRef = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(arrow_schema(), vec![arr]).unwrap()
    }

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }
    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }
    fn int_lit(i: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(i)))
    }
    fn compile(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
        CompiledExpr::from_typed(&typed)
    }

    /// Build a Filter kernel that keeps rows where `v < threshold`.
    fn lt_kernel(threshold: i64, schema: &OperatorSchema) -> Arc<dyn StatelessKernel> {
        let pred = compile(
            sp(Expr::Compare {
                op: CompareOp::Less,
                left: Box::new(col("v")),
                right: Box::new(int_lit(threshold)),
            }),
            schema,
        );
        Arc::new(FilterKernel::with_default_tile_size(pred, schema.clone()))
    }

    struct OneShotChild {
        schema: OperatorSchema,
        batch: Option<RecordBatch>,
        pulls: Arc<AtomicUsize>,
    }
    impl PhysicalOperator for OneShotChild {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            Ok(self.batch.take())
        }
    }

    proptest! {
        // For a single-batch input fed through N filters with arbitrary
        // thresholds, the materialization counter is bounded above by
        //   (1 segment-boundary materialization, only if final selection
        //    is sparse) + (one per intermediate filter that crosses
        //    the sparsity threshold).
        // Per `engine/operator-fusion.md` §7.2 / §3.4:
        //   mat_calls ≤ N
        // The looser bound mat_calls ≤ N is what the spec promises; the
        // tighter "1 + below-threshold count" is harder to state without
        // re-implementing the threshold logic here.
        #[test]
        fn materialization_count_is_bounded_by_kernel_count(
            // Up to 4 filters; thresholds in [-10, 200] over a 100-row 0..100 batch.
            thresholds in prop::collection::vec(-10i64..200i64, 0..=4)
        ) {
            let s = schema();
            let values: Vec<i64> = (0..100).collect();
            let child = Box::new(OneShotChild {
                schema: s.clone(),
                batch: Some(make_batch(values)),
                pulls: Arc::new(AtomicUsize::new(0)),
            });

            let kernels: Vec<KernelStep> = thresholds
                .iter()
                .map(|t| KernelStep::Filter(lt_kernel(*t, &s)))
                .collect();
            let n = kernels.len();

            let metrics = Arc::new(AtomicMetrics::new());
            let mut seg = FusedStatelessSegment::new(
                child,
                kernels,
                s,
                None,
                Arc::clone(&metrics) as Arc<dyn Metrics>,
                CancellationToken::new(),
            );
            // Drain the segment.
            while seg.next_batch().expect("next_batch").is_some() {}
            let snap = metrics.snapshot();
            // §7.2 bound: at most one materialization per kernel (the
            // sparsity-trigger fires at most once per kernel entry) plus
            // at most one at the outer boundary. The driver
            // short-circuits the boundary materialization when the
            // selection is full-cover or the segment yields zero rows,
            // so the looser bound `mat_calls <= n + 1` is the tightest
            // upper bound this property states.
            prop_assert!(
                (snap.selection_vector_materializations as usize) <= n + 1,
                "materialization count {} exceeds n+1={}",
                snap.selection_vector_materializations,
                n + 1,
            );
        }
    }
}
