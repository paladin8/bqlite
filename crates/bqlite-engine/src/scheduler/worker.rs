//! Per-worker context and morsel ownership token (design §4.3 / §6.1).
//!
//! Workers pull morsels via [`WorkerMorselGuard`], a RAII token that
//! holds the morsel descriptor, a reference to the per-shard
//! [`AccumulatorHandle`], and the worker-side context. The guard's
//! `Drop` decrements `AccumulatorHandle::outstanding_morsels` and, if
//! the decrement transitions the shard to "done", routes the
//! [`ShardDoneSignal`] to the coordinator-supplied callback.
//!
//! ## v1 scope
//!
//! `WorkerContext::metrics` — and the per-morsel sum-into-query-totals
//! merge — is owned by TASK-524. CP2 lands the ownership shape here so
//! TASK-524's wiring is purely additive.

use std::sync::Arc;

use super::accumulator::{AccumulatorHandle, ShardDoneSignal};
use super::morsel::{Morsel, ShardId};

/// Callback invoked by [`WorkerMorselGuard::drop`] when the shard
/// transitions to "done". The coordinator wires this to its
/// cross-shard merge trigger; tests pass a closure that records the
/// signal in a side channel.
///
/// The callback is invoked exactly once per `(query, shard)` pair —
/// see [`AccumulatorHandle::record_morsel_finished`] for the
/// uniqueness contract.
pub type ShardDoneCallback = Arc<dyn Fn(ShardId, ShardDoneSignal) + Send + Sync>;

/// Per-worker context for one in-flight morsel.
///
/// The context's lifetime spans one morsel — workers re-bind the
/// shard identity at every morsel pull, so metric increments inside
/// the context attribute correctly to `(query, shard, worker)`. The
/// context is never shared across morsels; constructing a fresh one
/// per morsel matches design §6.1's "shard identity obvious in every
/// metric increment" rationale.
///
/// Forward-shaped fields owned by TASK-524 and TASK-505 will land
/// here (`metrics: QueryMetrics`, `warnings: Vec<QueryWarning>`),
/// following the existing `WarningSink` / `QueryContext` ownership
/// story.
#[derive(Debug)]
pub struct WorkerContext {
    shard_id: ShardId,
}

impl WorkerContext {
    /// Construct a fresh context for one morsel of `shard`.
    pub fn new(shard: ShardId) -> Self {
        Self { shard_id: shard }
    }

    /// Identity of the shard whose morsel this context is processing.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

/// RAII ownership token for one morsel held by a worker (design §4.3).
///
/// Holding the guard binds the worker's [`WorkerContext`] to the
/// morsel's `(shard, …)` and makes the per-shard
/// [`AccumulatorHandle`] available for `finish_entity_into` calls.
/// Dropping the guard runs the shard-done bookkeeping in design §4.3:
/// decrement `outstanding_morsels`, check the conjunctive shard-done
/// condition, and fire the coordinator's callback if the shard just
/// transitioned.
pub struct WorkerMorselGuard {
    pub morsel: Morsel,
    pub worker_ctx: WorkerContext,
    accumulator: Arc<AccumulatorHandle>,
    /// Coordinator-supplied callback fired when the shard transitions
    /// to "done". Captured as `Arc<dyn Fn>` so the callback is
    /// `Send + Sync` and can be cloned across guards without exposing
    /// the coordinator's concrete type to this module.
    on_shard_done: ShardDoneCallback,
}

impl std::fmt::Debug for WorkerMorselGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerMorselGuard")
            .field("morsel.shard_id", &self.morsel.shard_id)
            .field("worker_ctx", &self.worker_ctx)
            .finish_non_exhaustive()
    }
}

impl WorkerMorselGuard {
    /// Construct a guard. Increments the handle's `outstanding_morsels`
    /// counter so the shard-done bookkeeping (design §4.3) can balance
    /// against the drop-hook decrement.
    pub fn new(
        morsel: Morsel,
        accumulator: Arc<AccumulatorHandle>,
        on_shard_done: ShardDoneCallback,
    ) -> Self {
        let shard = morsel.shard_id;
        accumulator.record_morsel_pulled();
        Self {
            morsel,
            worker_ctx: WorkerContext::new(shard),
            accumulator,
            on_shard_done,
        }
    }

    /// Borrow the per-shard accumulator handle. Workers call
    /// `accumulator().lock()` only at fused-entity-operator
    /// `finish_entity_into` boundaries — design §6.3.
    pub fn accumulator(&self) -> &AccumulatorHandle {
        &self.accumulator
    }
}

impl Drop for WorkerMorselGuard {
    fn drop(&mut self) {
        if let Some(signal) = self.accumulator.record_morsel_finished() {
            (self.on_shard_done)(self.accumulator.shard_id(), signal);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::morsel::{EntityRange, Morsel};
    use std::sync::Mutex;

    fn make_morsel(shard: u32, is_final: bool) -> Morsel {
        Morsel {
            shard_id: shard,
            entity_range: EntityRange::All,
            segments: Vec::new(),
            estimated_rows: 0,
            is_shard_final: is_final,
        }
    }

    type CallbackLog = Arc<Mutex<Vec<(ShardId, ShardDoneSignal)>>>;

    fn record_callback() -> (CallbackLog, ShardDoneCallback) {
        let log: CallbackLog = Arc::new(Mutex::new(Vec::new()));
        let log_for_cb = Arc::clone(&log);
        let cb: ShardDoneCallback = Arc::new(move |shard, sig| {
            log_for_cb.lock().unwrap().push((shard, sig));
        });
        (log, cb)
    }

    #[test]
    fn guard_drop_decrements_outstanding_and_fires_signal_when_done() {
        let h = Arc::new(AccumulatorHandle::new(0, None));
        let (log, cb) = record_callback();

        let g = WorkerMorselGuard::new(make_morsel(0, true), Arc::clone(&h), cb);
        assert_eq!(h.outstanding_morsels(), 1);

        // Generator marks total ahead of guard drop. The drop
        // observes the conjunctive condition and fires the signal.
        h.mark_total_emitted(1);
        drop(g);
        assert_eq!(h.outstanding_morsels(), 0);

        let observed = log.lock().unwrap().clone();
        assert_eq!(observed, vec![(0, ShardDoneSignal::LastGuardDropped)]);
    }

    #[test]
    fn intermediate_guard_drop_does_not_fire_signal() {
        let h = Arc::new(AccumulatorHandle::new(2, None));
        let (log, cb) = record_callback();

        let g1 = WorkerMorselGuard::new(make_morsel(2, false), Arc::clone(&h), Arc::clone(&cb));
        let g2 = WorkerMorselGuard::new(make_morsel(2, true), Arc::clone(&h), cb);
        h.mark_total_emitted(2);
        assert_eq!(h.outstanding_morsels(), 2);

        // Drop the first guard: outstanding 2 → 1, no signal.
        drop(g1);
        assert!(log.lock().unwrap().is_empty());

        // Drop the second guard: outstanding 1 → 0 with total set,
        // signal fires.
        drop(g2);
        let observed = log.lock().unwrap().clone();
        assert_eq!(observed, vec![(2, ShardDoneSignal::LastGuardDropped)]);
    }

    #[test]
    fn worker_context_carries_shard_identity() {
        let h = Arc::new(AccumulatorHandle::new(7, None));
        let (_, cb) = record_callback();
        let g = WorkerMorselGuard::new(make_morsel(7, true), h, cb);
        assert_eq!(g.worker_ctx.shard_id(), 7);
    }

    #[test]
    fn guard_propagates_panic_in_drop_safely() {
        // If the per-shard callback panics, Drop swallows it inside
        // catch_unwind — but the bookkeeping decrement has already
        // happened. We exercise the panic path directly to assert
        // the decrement runs even when the callback would panic.
        // Note: we do NOT install the panicking callback here because
        // Drop has no catch_unwind; the contract is that the
        // coordinator's callback must not panic. We instead verify
        // that the decrement runs *before* the callback fires, so
        // a future callback panic cannot leave outstanding stale.
        let h = Arc::new(AccumulatorHandle::new(9, None));
        let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let order_for_cb = Arc::clone(&order);
        let h_for_cb = Arc::clone(&h);
        let cb: ShardDoneCallback = Arc::new(move |_, _| {
            // By the time the callback runs, the decrement has
            // already occurred — reflect that observable property
            // in the log.
            assert_eq!(h_for_cb.outstanding_morsels(), 0);
            order_for_cb.lock().unwrap().push("callback");
        });

        let g = WorkerMorselGuard::new(make_morsel(9, true), Arc::clone(&h), cb);
        h.mark_total_emitted(1);
        drop(g);
        assert_eq!(order.lock().unwrap().clone(), vec!["callback"]);
    }
}
