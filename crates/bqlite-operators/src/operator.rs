//! Core execution traits for bqlite physical operators.
//!
//! This module freezes the Wave 1 v0 trait surface for physical operator
//! execution. See [`docs/design/operators/operator-traits.md`](../../../../docs/design/operators/operator-traits.md)
//! for the design note. Any change after Wave 1 requires a
//! high-priority `[TRAIT]` task.
//!
//! Two traits live here:
//!
//! - [`PhysicalOperator`] — pull-based batch iterator. Every stateless
//!   operator (scan, filter, project, limit) and every
//!   `EntityOperatorAdapter` wrapping a stateful operator implements it.
//! - [`EntityOperator`] — stateful per-entity operator. Sequence match,
//!   sessionize, and window-function operators will implement it in later
//!   waves.
//!
//! Both traits rely on [`OperatorSchema`] from `bqlite-core` for schema
//! propagation and on [`BqliteError`] for error reporting.
//!
//! ## Cancellation
//!
//! Cancellation is **cooperative** and **flag-based** via the
//! [`CancellationToken`] type. It is deliberately not a trait method: a
//! method would let one thread mutate an operator while another is
//! mid-pull. Operators receive a cloned token at construction time and
//! check it at natural yield points (between batches or between entity
//! sub-batches). See design note §5 for details.
//!
//! ## Wave 1 deferrals
//!
//! - `finish_entity_into` + `Accumulator` — aggregation fusion, later
//!   wave.
//! - Metrics hooks — TASK-112.
//! - `EntityOperatorAdapter` implementation — the adapter lands with the
//!   first wave that ships an `EntityOperator` implementor.
//!
//! The `DemandCapabilities` protocol (TASK-409, TASK-427) lives in
//! [`bqlite_planner::demand`]. The struct advertises per-operator
//! capability bits (step-reached, match-count, full-detail,
//! aggregation-fusion, step-property forwarding, generic column
//! forwarding, eager-group-emit). The [`EntityOperator::supported_demands`]
//! method below mirrors the standalone [`bqlite_planner::DemandPropagation`]
//! trait — operators override it to advertise real capabilities.
//! See `docs/design/planner/demand-protocol.md` for the full spec.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use bqlite_core::{EntityId, OperatorSchema, Result};
use bqlite_planner::DemandCapabilities;

// ---------------------------------------------------------------------------
// Pipeline-wide constants
// ---------------------------------------------------------------------------

/// Default output batch size for operators that split a large materialized
/// result into pipeline-friendly chunks (e.g. `SortOperator`).
///
/// 65,536 rows is the pipeline-wide default from `execution-model.md §4.2`.
/// Operators must reference this constant by name — not hardcode the integer
/// — so that the value can be changed in one place.
pub const DEFAULT_OUTPUT_BATCH_SIZE: usize = 65_536;

// ---------------------------------------------------------------------------
// CancellationToken
// ---------------------------------------------------------------------------

/// Shared cancellation flag passed to operators at construction time.
///
/// Callers set the flag by invoking [`cancel`](Self::cancel); operators
/// observe it between batches (or between entity sub-batches) via
/// [`is_cancelled`](Self::is_cancelled) and return
/// [`BqliteError::Cancelled`](bqlite_core::BqliteError::Cancelled) when it
/// fires.
///
/// `CancellationToken` is `Clone` — all clones share the same underlying
/// `Arc<AtomicBool>`. Engine code creates one token per query and clones
/// it into every operator in the tree. The engine-side wrapper
/// (`QueryContext`, landing in a later wave) owns the authoritative token
/// and exposes `cancel()` to timeout timers, Ctrl-C handlers, and early
/// termination paths.
///
/// Relaxed ordering is sufficient because the token carries no other
/// synchronized state — it is a hint, not a handshake. Worst-case
/// cancellation latency is one batch or one entity sub-batch, which is
/// already the floor regardless of ordering.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Construct a fresh, not-yet-cancelled token.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` once any holder of this token (or a clone of it)
    /// has called [`cancel`](Self::cancel).
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Mark the token cancelled. Idempotent — repeated calls are cheap.
    #[inline]
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// PhysicalOperator
// ---------------------------------------------------------------------------

/// Pull-based physical operator.
///
/// Every stateless operator implements this trait directly. Stateful
/// per-entity operators implement [`EntityOperator`] and are wrapped in an
/// `EntityOperatorAdapter` (landing in a later wave) that presents them
/// as a `PhysicalOperator` to the engine.
///
/// # Lifecycle
///
/// ```text
/// constructed ─open()─▶ opened ─next_batch()*─▶ drained ─close()─▶ closed
/// ```
///
/// 1. [`open`](Self::open) is called exactly once before the first
///    [`next_batch`](Self::next_batch). Defaulted to a no-op so
///    combinators can ignore it.
/// 2. [`next_batch`](Self::next_batch) is called repeatedly until it
///    returns `Ok(None)` or `Err(_)`.
/// 3. [`close`](Self::close) is called exactly once after iteration
///    completes (by exhaustion or by error). It must be idempotent —
///    the engine may call it defensively during tear-down even if
///    `open` failed.
/// 4. After `close`, calling any other method is a programming error.
///
/// # Batch invariants
///
/// Callers rely on these without re-checking them per batch. Producers
/// that violate them are incorrect, not merely inefficient.
///
/// - **Entity alignment.** A batch never splits an entity; all rows for
///   an `entity_id` are contiguous within a single batch, or streamed
///   as successive sub-batches with no other entity interleaved.
/// - **Sort order.** Rows are sorted by `(entity_id, ts)` ascending.
/// - **Schema stability.** Every batch matches [`output_schema`](Self::output_schema)
///   — column order, types, and nullability are fixed for the operator's
///   lifetime.
/// - **Exhaustion signalling.** Use `Ok(None)` for exhaustion, not an
///   empty batch. Mid-stream empty batches are legal (e.g. a filter that
///   drops every row of an input batch) and consumers must tolerate
///   them, but producers should avoid emitting them when cheap.
///
/// # Error propagation
///
/// Errors surface as [`BqliteError`](bqlite_core::BqliteError). The
/// variants an operator is expected to raise are documented in the design
/// note (§4.3): `Io`, `Arrow`, `Schema`, `Execution`, `Cancelled`.
/// `Parse` and `Plan` are plan-time variants and should not appear at
/// runtime.
pub trait PhysicalOperator: Send {
    /// The operator's output schema, known at plan time and stable for
    /// the lifetime of the operator. Must match every `RecordBatch`
    /// returned from [`next_batch`](Self::next_batch).
    fn output_schema(&self) -> &OperatorSchema;

    /// Acquire any OS resources needed before producing results.
    ///
    /// Called exactly once, before the first `next_batch`. The default
    /// implementation is a no-op — stateless combinators leave it alone.
    /// Segment readers, spill files, and thread-local caches are acquired
    /// here so that failure surfaces cleanly before results start
    /// flowing.
    fn open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Pull the next batch of rows.
    ///
    /// Returns `Ok(None)` when the operator is exhausted; subsequent
    /// calls after `Ok(None)` must continue to return `Ok(None)` without
    /// side effects.
    ///
    /// Errors abort the query — the engine tears down the operator tree
    /// and propagates the error to the caller of `Engine::query`.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Release any OS resources held by the operator.
    ///
    /// Called exactly once after iteration completes. Must be
    /// idempotent: the engine may call `close` during tear-down even if
    /// [`open`](Self::open) never succeeded or [`next_batch`](Self::next_batch)
    /// was never called.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EntityOperator
// ---------------------------------------------------------------------------

/// Stateful per-entity operator.
///
/// The operator itself is immutable (`&self`) — all mutable state lives
/// in [`State`](Self::State) and is created fresh for each entity by
/// [`create_state`](Self::create_state). This is what makes
/// `Send + Sync` sound when the compiled operator is shared across
/// shard-tasks via `Arc`: no shard-task can mutate the operator itself.
///
/// Instances are wrapped in an `EntityOperatorAdapter` (landing in a
/// later wave) that converts them into [`PhysicalOperator`]s by scanning
/// the input stream for entity-id changes and routing per-entity
/// sub-batches through the methods below. Adapter contract (from
/// execution-model.md §4.1 and the design note §6.3):
///
/// 1. Sub-batches for a single entity arrive **consecutively**, with no
///    other entity interleaved.
/// 2. Within a sub-batch, rows are sorted by timestamp ascending and
///    belong to exactly one entity.
/// 3. [`finish_entity`](Self::finish_entity) is called exactly once per
///    entity, after the final [`process_sub_batch`](Self::process_sub_batch).
///
/// Large entities (power-law distributions — think a bot that fired 5M
/// events) cross row-group boundaries and arrive as multiple sub-batches.
/// The operator carries compact state in `Self::State` across sub-batches;
/// the scan drops each sub-batch before producing the next, so only one
/// sub-batch per entity is resident at a time.
///
/// # Error reporting
///
/// Hot-path methods return `()` and `Option<RecordBatch>` rather than
/// `Result<...>` so the per-event inner loop can stay branch-free.
/// Recoverable errors are surfaced through other channels per the design
/// note:
///
/// - **Memory pressure** is caught at the allocation site via
///   [`MemoryBudget::try_reserve`](bqlite_core::MemoryBudget::try_reserve).
///   Operators that cannot spill set an error flag on the shared
///   [`CancellationToken`] (or a companion failure channel, landing with
///   the engine in a later wave) and return early from
///   `process_sub_batch`; the adapter observes the flag between
///   sub-batches and aborts the query.
/// - **Cancellation** is checked by the adapter between sub-batches, not
///   inside the per-event loop.
/// - **Invariant violations** panic — the engine catches panics at the
///   shard-task boundary and surfaces them as an execution error.
///
/// # Fused aggregation
///
/// [`finish_entity_into`](Self::finish_entity_into) provides the fused
/// aggregation path. When a fused accumulator is active, the
/// `EntityOperatorAdapter` calls this INSTEAD of
/// [`finish_entity`](Self::finish_entity). The default implementation
/// calls `finish_entity()` and feeds the result into the accumulator —
/// operators override for zero-materialization fusion.
///
/// The [`supported_demands`](Self::supported_demands) method advertises
/// demand capabilities per `docs/design/planner/demand-protocol.md` §5.
/// The default body returns [`DemandCapabilities::none()`] (all `false`);
/// operators with real capabilities override it.
pub trait EntityOperator: Send + Sync {
    /// Per-entity mutable state. Created fresh for each entity by
    /// [`create_state`](Self::create_state) and consumed by
    /// [`finish_entity`](Self::finish_entity).
    type State: Send;

    /// Create initial state for a new entity.
    ///
    /// The `entity_id` is passed so operators that need per-entity
    /// warning attribution (event-limit exceeded, active-state
    /// overflow, ...) can capture it. Most operators ignore it.
    fn create_state(&self, entity_id: &EntityId) -> Self::State;

    /// The operator's output schema. Same semantics as
    /// [`PhysicalOperator::output_schema`].
    fn output_schema(&self) -> &OperatorSchema;

    /// Process a sub-batch of events for the current entity.
    ///
    /// The adapter guarantees every row in `batch` belongs to the same
    /// entity, rows are sorted by timestamp ascending, and the sub-batch
    /// row count is bounded by the pipeline batch size.
    fn process_sub_batch(&self, state: &mut Self::State, batch: &RecordBatch);

    /// Extract results after all sub-batches for the entity have been
    /// processed.
    ///
    /// Called exactly once per entity, after the final
    /// [`process_sub_batch`](Self::process_sub_batch). Consumes `state`
    /// — there is no reuse.
    ///
    /// Returns `None` when the entity produces no output rows (e.g. the
    /// pattern did not match). Returns `Some(batch)` with one or more
    /// rows for operators that emit one row per entity, one row per
    /// session, one row per match, etc.
    fn finish_entity(&self, state: Self::State) -> Option<RecordBatch>;

    /// Fused aggregation path. If the operator has a fused accumulator,
    /// the adapter calls this INSTEAD of `finish_entity()`. Updates the
    /// accumulator directly without materializing per-entity rows.
    ///
    /// The default implementation calls `finish_entity()` and feeds the
    /// result into the accumulator — operators override for
    /// zero-materialization fusion (see execution-model.md §8.4).
    ///
    /// Returns `Err` if the accumulator hits its group-cardinality cap
    /// during the update. The `EntityOperatorAdapter` (TASK-307) will
    /// propagate this to abort the query.
    fn finish_entity_into(
        &self,
        state: Self::State,
        accumulator: &mut dyn crate::aggregate::Accumulator,
    ) -> Result<()> {
        if let Some(batch) = self.finish_entity(state) {
            accumulator.update_batch(&batch)?;
        }
        Ok(())
    }

    /// Drain any per-entity diagnostics the operator stashed on its
    /// state during `process_sub_batch` (e.g. cap-exceeded events).
    /// The adapter calls this **before** consuming the state via
    /// [`finish_entity`](Self::finish_entity) /
    /// [`finish_entity_into`](Self::finish_entity_into), then forwards
    /// each returned warning to the per-query `WarningSink` in
    /// `bqlite-engine`.
    ///
    /// The default implementation returns an empty vec — stateless
    /// operators and stateful operators with no diagnostic channel
    /// inherit zero overhead. Sessionize, Attribute, and SequenceMatch
    /// override to report their cap-exceeded events.
    ///
    /// `entity_id` is supplied by the adapter so operators do not
    /// need to thread the EntityId through their state purely for
    /// attribution. Operators that already carry the EntityId on
    /// their state (Sessionize, Attribute) may ignore the argument;
    /// operators that don't (the matcher's StepCounterState) populate
    /// the warning from this argument.
    ///
    /// See `docs/design/engine/cancellation.md` §7.4.
    fn take_pending_warnings(
        &self,
        _state: &mut Self::State,
        _entity_id: &EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        Vec::new()
    }

    /// The set of input columns this operator actually reads.
    ///
    /// Drives projection pruning at the scan layer. Returning an empty
    /// slice means the operator is metadata-only (e.g.
    /// `COUNT(DISTINCT entity_id)`).
    fn required_columns(&self) -> &[String];

    /// Advertise the demand capabilities this operator supports.
    ///
    /// The default returns [`DemandCapabilities::none()`] (all `false`).
    /// Operators with real capabilities override this to advertise
    /// which demand shapes they can satisfy. The planner matches the
    /// returned capabilities against the downstream [`DemandSet`] during
    /// physical planning.
    ///
    /// This mirrors the standalone [`DemandPropagation`] trait in
    /// `bqlite-planner` — both surfaces coexist per
    /// `docs/design/planner/demand-protocol.md` §5.2.
    ///
    /// [`DemandSet`]: bqlite_planner::DemandSet
    /// [`DemandPropagation`]: bqlite_planner::DemandPropagation
    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities::none()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{BqlType, BqliteError, ColumnDef, EntityId, OperatorSchema};

    use super::*;

    // ── CancellationToken ────────────────────────────────────────────────

    #[test]
    fn cancellation_token_new_is_not_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn cancellation_token_default_matches_new() {
        let a = CancellationToken::default();
        let b = CancellationToken::new();
        assert_eq!(a.is_cancelled(), b.is_cancelled());
    }

    #[test]
    fn cancellation_token_observes_cancel() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_token_cancel_is_idempotent() {
        let token = CancellationToken::new();
        token.cancel();
        token.cancel();
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancellation_token_clone_shares_state() {
        let original = CancellationToken::new();
        let clone_a = original.clone();
        let clone_b = original.clone();
        assert!(!clone_a.is_cancelled());
        assert!(!clone_b.is_cancelled());

        clone_a.cancel();

        assert!(original.is_cancelled());
        assert!(clone_a.is_cancelled());
        assert!(clone_b.is_cancelled());
    }

    // ── Test fixtures ────────────────────────────────────────────────────

    fn make_schema() -> OperatorSchema {
        OperatorSchema::new(vec![ColumnDef::required("value", BqlType::Int)])
            .expect("schema construction should succeed")
    }

    fn make_batch(rows: Vec<i64>) -> RecordBatch {
        let array: ArrayRef = Arc::new(Int64Array::from(rows));
        let schema = ArrowSchema::new(vec![Field::new("value", DataType::Int64, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![array])
            .expect("record batch construction should succeed")
    }

    // ── PhysicalOperator conformance test ────────────────────────────────

    /// Minimal `PhysicalOperator` that exercises the full lifecycle and
    /// cancellation flow. Opens once, yields pre-canned batches until
    /// exhausted (or cancelled), closes once. Counts lifecycle calls so
    /// tests can assert on call ordering and idempotence.
    struct VecOp {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        next_idx: usize,
        cancel: CancellationToken,
        open_calls: usize,
        close_calls: usize,
    }

    impl VecOp {
        fn new(
            schema: OperatorSchema,
            batches: Vec<RecordBatch>,
            cancel: CancellationToken,
        ) -> Self {
            Self {
                schema,
                batches,
                next_idx: 0,
                cancel,
                open_calls: 0,
                close_calls: 0,
            }
        }
    }

    impl PhysicalOperator for VecOp {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            self.open_calls += 1;
            Ok(())
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            if self.cancel.is_cancelled() {
                return Err(BqliteError::Cancelled);
            }
            if self.next_idx >= self.batches.len() {
                return Ok(None);
            }
            let batch = self.batches[self.next_idx].clone();
            self.next_idx += 1;
            Ok(Some(batch))
        }

        fn close(&mut self) -> Result<()> {
            self.close_calls += 1;
            Ok(())
        }
    }

    #[test]
    fn physical_operator_default_open_close_are_noop() {
        struct Bare {
            schema: OperatorSchema,
        }
        impl PhysicalOperator for Bare {
            fn output_schema(&self) -> &OperatorSchema {
                &self.schema
            }
            fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
                Ok(None)
            }
        }

        let mut op = Bare {
            schema: make_schema(),
        };
        assert!(op.open().is_ok());
        assert!(op.next_batch().unwrap().is_none());
        assert!(op.close().is_ok());
    }

    #[test]
    fn physical_operator_lifecycle_drains_then_closes() {
        let cancel = CancellationToken::new();
        let batches = vec![make_batch(vec![1, 2, 3]), make_batch(vec![4, 5])];
        let mut op = VecOp::new(make_schema(), batches, cancel);

        op.open().unwrap();
        assert_eq!(op.open_calls, 1);

        let a = op.next_batch().unwrap().unwrap();
        assert_eq!(a.num_rows(), 3);
        let b = op.next_batch().unwrap().unwrap();
        assert_eq!(b.num_rows(), 2);
        assert!(op.next_batch().unwrap().is_none());
        // Exhausted — subsequent calls continue to return None.
        assert!(op.next_batch().unwrap().is_none());

        op.close().unwrap();
        assert_eq!(op.close_calls, 1);
    }

    #[test]
    fn physical_operator_surfaces_cancellation() {
        let cancel = CancellationToken::new();
        let mut op = VecOp::new(
            make_schema(),
            vec![make_batch(vec![1]), make_batch(vec![2])],
            cancel.clone(),
        );

        // First batch still flows.
        assert!(op.next_batch().unwrap().is_some());

        // Cancellation observed at the top of the next call.
        cancel.cancel();
        match op.next_batch() {
            Err(BqliteError::Cancelled) => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    #[test]
    fn physical_operator_close_is_idempotent() {
        let mut op = VecOp::new(make_schema(), vec![], CancellationToken::new());
        op.close().unwrap();
        op.close().unwrap();
        op.close().unwrap();
        assert_eq!(op.close_calls, 3);
    }

    #[test]
    fn physical_operator_trait_object_compiles() {
        // The engine bind step will materialize a `Box<dyn PhysicalOperator>`
        // tree — confirm the trait is object-safe under the Wave 1 shape.
        let op = VecOp::new(make_schema(), vec![], CancellationToken::new());
        let _boxed: Box<dyn PhysicalOperator> = Box::new(op);
    }

    // ── EntityOperator conformance test ──────────────────────────────────

    /// Per-entity summing operator. `create_state` → accumulator,
    /// `process_sub_batch` → add column, `finish_entity` → single-row
    /// batch with the total (or `None` when the total is zero).
    struct SumOp {
        schema: OperatorSchema,
        required: Vec<String>,
    }

    struct SumState {
        entity: EntityId,
        total: i64,
    }

    impl EntityOperator for SumOp {
        type State = SumState;

        fn create_state(&self, entity_id: &EntityId) -> SumState {
            SumState {
                entity: entity_id.clone(),
                total: 0,
            }
        }

        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn process_sub_batch(&self, state: &mut SumState, batch: &RecordBatch) {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column must be Int64Array");
            for i in 0..column.len() {
                state.total += column.value(i);
            }
        }

        fn finish_entity(&self, state: SumState) -> Option<RecordBatch> {
            if state.total == 0 {
                return None;
            }
            // Include the entity in the output to prove create_state saw it.
            let _ = state.entity; // silence warning on unused read
            Some(make_batch(vec![state.total]))
        }

        fn required_columns(&self) -> &[String] {
            &self.required
        }
    }

    fn sum_op() -> SumOp {
        SumOp {
            schema: make_schema(),
            required: vec!["value".to_string()],
        }
    }

    #[test]
    fn entity_operator_processes_single_sub_batch() {
        let op = sum_op();
        let mut state = op.create_state(&EntityId::from("u1"));
        op.process_sub_batch(&mut state, &make_batch(vec![1, 2, 3, 4]));
        let out = op.finish_entity(state).expect("sum should be non-zero");
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 10);
    }

    #[test]
    fn entity_operator_accumulates_across_sub_batches() {
        let op = sum_op();
        let mut state = op.create_state(&EntityId::from("u1"));
        op.process_sub_batch(&mut state, &make_batch(vec![1, 2]));
        op.process_sub_batch(&mut state, &make_batch(vec![3, 4]));
        op.process_sub_batch(&mut state, &make_batch(vec![5]));
        let out = op.finish_entity(state).expect("sum should be non-zero");
        let col = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.value(0), 15);
    }

    #[test]
    fn entity_operator_finish_can_return_none() {
        let op = sum_op();
        let state = op.create_state(&EntityId::from("u1"));
        assert!(op.finish_entity(state).is_none());
    }

    #[test]
    fn entity_operator_required_columns_reflects_inputs() {
        let op = sum_op();
        assert_eq!(op.required_columns(), &["value".to_string()]);
    }

    #[test]
    fn entity_operator_create_state_captures_entity_id() {
        let op = sum_op();
        let state = op.create_state(&EntityId::from(42_i64));
        assert_eq!(state.entity, EntityId::from(42_i64));
        assert_eq!(state.total, 0);
    }

    #[test]
    fn entity_operator_is_send_sync() {
        // The compiled operator is shared across shard-tasks via Arc, so
        // the trait must be Send + Sync. Static assertion via a function
        // that only compiles for Send + Sync types.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SumOp>();
    }

    // ── supported_demands (TASK-110 / TASK-427) ──────────────────────────

    #[test]
    fn entity_operator_default_take_pending_warnings_is_empty() {
        let op = sum_op();
        let mut state = op.create_state(&EntityId::from("u1"));
        let warnings = op.take_pending_warnings(&mut state, &EntityId::from("u1"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn entity_operator_default_supported_demands_is_none() {
        let op = sum_op();
        assert_eq!(op.supported_demands(), DemandCapabilities::none());
    }

    /// Override operator that returns a non-default capability set.
    struct OverrideOp {
        schema: OperatorSchema,
        required: Vec<String>,
    }

    impl EntityOperator for OverrideOp {
        type State = ();

        fn create_state(&self, _entity_id: &EntityId) -> Self::State {}

        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn process_sub_batch(&self, _state: &mut Self::State, _batch: &RecordBatch) {}

        fn finish_entity(&self, _state: Self::State) -> Option<RecordBatch> {
            None
        }

        fn required_columns(&self) -> &[String] {
            &self.required
        }

        fn supported_demands(&self) -> DemandCapabilities {
            DemandCapabilities {
                supports_forwarded_columns: true,
                ..DemandCapabilities::none()
            }
        }
    }

    #[test]
    fn entity_operator_supported_demands_override_is_observed() {
        let op = OverrideOp {
            schema: make_schema(),
            required: vec![],
        };
        let caps = op.supported_demands();
        assert!(caps.supports_forwarded_columns);
        assert!(!caps.supports_step_reached);
    }
}
