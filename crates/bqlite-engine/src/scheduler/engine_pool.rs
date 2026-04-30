//! Engine-level morsel scheduler component (design §5).
//!
//! [`MorselScheduler`] owns the Rayon worker pool and the
//! [`CoreBudget`] semaphore through which queries acquire
//! `query_threads` permits at submit time. The
//! [`MorselScheduler::run_degenerate`] entry point is the v1
//! single-morsel dispatch path: each query is shipped as one
//! whole-database morsel, runs to completion on a worker, and the
//! result is delivered back to the calling thread through the
//! Rayon scope's join.
//!
//! Per design §11, this scaffolding is forward-compatible with the
//! per-operator morsel-aware execution that lands once individual
//! operators (scan, filter, aggregate, …) take a
//! `(shard_id, [entity_lo, entity_hi))` parameter. The single-morsel
//! v1 path does not yet exercise the [`MorselQueue`] /
//! [`AccumulatorHandle`] / [`WorkerMorselGuard`] machinery on the hot
//! path; CP3 wires the *engine submission* path through the
//! [`CoreBudget`] permit gate plus the Rayon pool, and follow-on
//! tasks replace `run_degenerate` with the multi-morsel dispatch
//! when operators learn the per-shard contract.
//!
//! ## Sharing with the storage compaction scheduler
//!
//! The morsel scheduler doc (§7.1) calls for one [`CoreBudget`]
//! shared between the engine and the compaction scheduler. The
//! storage crate today owns its own [`CoreBudget`] inside the
//! compaction scheduler ([`bqlite_storage::compaction::CoreBudget`]
//! is exposed but not yet plumbed through the database). The v1
//! engine therefore constructs its own [`CoreBudget`] sized at
//! `query_threads`. Wiring the two together so a query and
//! compaction actually share permits is a follow-on (TASK-525 stress
//! suite is the natural place to need it). The query-side
//! [`CoreBudget::acquire_n`] contract from CP1 is identical either
//! way, so the migration is purely additive.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bqlite_storage::compaction::CoreBudget;

use super::accumulator::AccumulatorHandle;
use super::morsel::{Morsel, ShardSnapshot};
use super::queue::MorselQueue;
use super::worker::{ShardDoneCallback, WorkerMorselGuard};

/// Engine-level worker pool plus capacity-sharing semaphore.
pub struct MorselScheduler {
    /// `query_threads`-permit semaphore. Each query acquires
    /// `query_threads` permits at submit time and releases them on
    /// finalize.
    core_budget: Arc<CoreBudget>,
    /// Rayon worker pool. v1 uses one logical worker per query
    /// (the single degenerate morsel runs to completion on it);
    /// follow-on tasks dispatch many morsels at once.
    pool: rayon::ThreadPool,
    /// Configured query thread count — the per-query batch size for
    /// `core_budget.acquire_n`.
    query_threads: usize,
}

impl std::fmt::Debug for MorselScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MorselScheduler")
            .field("query_threads", &self.query_threads)
            .field("available_permits", &self.core_budget.available())
            .finish_non_exhaustive()
    }
}

impl MorselScheduler {
    /// Construct a scheduler with the given worker count.
    ///
    /// The internal [`CoreBudget`] is sized at `query_threads`; a
    /// query submitted via [`Self::run_degenerate`] acquires every
    /// permit, so concurrent queries serialize on the budget per
    /// design §5.3. Pass a larger permit count by constructing the
    /// scheduler via [`Self::with_core_budget`] (used by tests that
    /// want to exercise the queue under non-saturated conditions).
    pub fn new(query_threads: usize) -> std::result::Result<Arc<Self>, BuildError> {
        let core_budget = CoreBudget::new(query_threads);
        Self::with_core_budget(query_threads, core_budget)
    }

    /// Construct a scheduler reusing an externally-supplied
    /// [`CoreBudget`]. Used by tests and (eventually) by the engine
    /// to share permits with the storage compaction scheduler.
    pub fn with_core_budget(
        query_threads: usize,
        core_budget: Arc<CoreBudget>,
    ) -> std::result::Result<Arc<Self>, BuildError> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(query_threads)
            .thread_name(|i| format!("bqlite-query-{i}"))
            .build()
            .map_err(BuildError::Pool)?;
        Ok(Arc::new(Self {
            core_budget,
            pool,
            query_threads,
        }))
    }

    /// Number of worker threads in the Rayon pool.
    pub fn query_threads(&self) -> usize {
        self.query_threads
    }

    /// Currently available `CoreBudget` permits (test/observability
    /// helper).
    pub fn available_permits(&self) -> usize {
        self.core_budget.available()
    }

    /// Submit `work` to the worker pool with `query_threads` permits
    /// held for the duration of the call.
    ///
    /// This is the v1 engine entry point. It acquires
    /// `query_threads` permits from the [`CoreBudget`] (FIFO atomic
    /// batch — design §7.1), dispatches `work` onto the Rayon pool
    /// via `ThreadPool::scope` so the closure can borrow non-`'static`
    /// state, and returns the closure's result. Permits are released
    /// when the returned guard drops.
    ///
    /// Concurrent submissions serialize on the [`CoreBudget`] —
    /// design §5.3 mandates that on a saturated worker pool, queries
    /// run serially. The morsel queue + accumulator handle + worker
    /// guard machinery is not exercised by this path; the v1 engine
    /// runs the operator tree end-to-end inside the worker. The
    /// follow-on work that splits the query across morsels uses
    /// [`Self::run_degenerate`] (and its successor for true sub-shard
    /// generation) to dispatch through the queue protocol.
    ///
    /// **Panic propagation.** A panic in `work` propagates back to
    /// the caller via Rayon's `ThreadPool::scope` panic-resume
    /// contract: the worker thread catches the panic, re-raises on
    /// scope exit, and the calling thread observes the panic on the
    /// `submit` call. The engine's outer `catch_unwind` boundary in
    /// `Engine::query_with_options` (per `cancellation.md` §4.1)
    /// converts that re-raised panic into
    /// [`bqlite_core::BqliteError::OperatorPanic`].
    pub fn submit<F, R>(&self, work: F) -> R
    where
        F: FnOnce() -> R + Send,
        R: Send,
    {
        let _permits = self.core_budget.acquire_n(self.query_threads);

        let result_slot: Mutex<Option<R>> = Mutex::new(None);
        let result_ref = &result_slot;

        self.pool.scope(|s| {
            s.spawn(move |_| {
                *result_ref.lock().expect("result slot poisoned") = Some(work());
            });
        });

        result_slot
            .into_inner()
            .expect("result slot poisoned")
            .expect("worker did not write a result")
    }

    /// v1 degenerate dispatch through the morsel-queue + accumulator
    /// handle protocol: ship `work` to the worker pool as a single
    /// whole-shard morsel and return its result.
    ///
    /// Used by tests and follow-on work that wants to exercise the
    /// queue + accumulator handoff. Engine submission uses the
    /// simpler [`Self::submit`] in v1.
    ///
    /// The submission:
    /// 1. Acquires `query_threads` permits from the [`CoreBudget`]
    ///    (FIFO, atomic batch — design §7.1).
    /// 2. Builds a single-shard [`AccumulatorHandle`] and
    ///    [`MorselQueue`], pushes one whole-shard morsel, and marks
    ///    the queue drained — exercising the queue + accumulator
    ///    bookkeeping even though there is only one morsel.
    /// 3. Spawns the work onto the Rayon pool via
    ///    `ThreadPool::scope`, which lets `work` borrow the
    ///    surrounding non-`'static` state. The scope joins before
    ///    returning, so the Rayon worker has finished by the time
    ///    [`Self::run_degenerate`] returns.
    /// 4. Releases the permits when the returned guard drops.
    ///
    /// `work` receives the [`WorkerMorselGuard`] for the morsel so
    /// follow-on work that wants to call `accumulator().lock()` at
    /// `finish_entity_into` boundaries can do so without the
    /// scheduler's plumbing churning.
    pub fn run_degenerate<F, R>(&self, snapshot: ShardSnapshot, work: F) -> R
    where
        F: FnOnce(&mut WorkerMorselGuard) -> R + Send,
        R: Send,
    {
        // Empty shards have no morsel to dispatch against; the
        // accumulator + queue protocol still works, but the worker's
        // `pop_or_park` returns `Drained` immediately and `work` is
        // never called — there is no `R` to return. Forward-compat
        // guard: until follow-on work introduces an `Option<R>`
        // variant, callers must dispatch only against non-empty
        // shards.
        assert!(
            snapshot
                .windows
                .iter()
                .any(|w| !w.segments.is_empty()),
            "run_degenerate requires a non-empty shard snapshot; use \
             submit() for queries that may not produce a worker call",
        );
        let _permits = self.core_budget.acquire_n(self.query_threads);

        // Per-query bookkeeping — exercised even for the degenerate
        // single-morsel path so the protocol from design §4.3 is the
        // only path the engine takes.
        let shard_id = snapshot.shard_id;
        let accumulator = Arc::new(AccumulatorHandle::new(shard_id, None));
        let queue = Arc::new(MorselQueue::new(2.max(self.query_threads * 2)));
        let mut generator = super::morsel::MorselGenerator::degenerate(snapshot);

        let morsel: Option<Morsel> = generator.take_next();
        let total_emitted = generator.total_emitted().unwrap_or(0);
        if let Some(m) = morsel.clone() {
            // Single-producer push; capacity is guaranteed sufficient
            // for one morsel because we just constructed the queue.
            queue
                .push(m)
                .expect("morsel queue capacity > 1 by construction");
        }
        // Mark generator drained on the queue and the accumulator.
        // `mark_total_emitted` returns `Some(...)` only for empty
        // shards (no in-flight morsels at all); the worker path below
        // covers the populated case.
        accumulator.mark_total_emitted(total_emitted);
        queue.mark_drained();

        let result_slot: Mutex<Option<R>> = Mutex::new(None);
        let result_ref = &result_slot;

        // Capture `accumulator` and `queue` for the spawned closure.
        // Both are `Arc`-shared, so cloning them is cheap.
        let queue_for_worker = Arc::clone(&queue);
        let accumulator_for_worker = Arc::clone(&accumulator);

        // No-op shard-done callback for the v1 degenerate path —
        // there is no cross-shard merge yet because there is only
        // one shard. Follow-ons replace this with the coordinator
        // hook from design §6.4.
        let on_shard_done: ShardDoneCallback = Arc::new(|_, _| {});

        self.pool.scope(|s| {
            s.spawn(move |_| {
                // No morsel for empty shards — the accumulator has
                // already signaled "done" via `mark_total_emitted`.
                let morsel = match queue_for_worker.pop_or_park(Duration::from_millis(10)) {
                    Ok(m) => m,
                    Err(_drained) => return, // empty shard: nothing to do
                };
                let mut guard =
                    WorkerMorselGuard::new(morsel, accumulator_for_worker, on_shard_done);
                let r = work(&mut guard);
                *result_ref.lock().expect("result slot poisoned") = Some(r);
            });
        });

        // Pool scope blocks until the spawned closure has joined,
        // so the `Mutex<Option<R>>` is now populated whenever the
        // shard had a morsel. For empty shards (no work), the caller
        // would not have provided a meaningful `R`; the v1 engine
        // guarantees `snapshot.windows` is non-empty for every
        // dispatched query.
        result_slot
            .into_inner()
            .expect("result slot poisoned")
            .expect("worker did not write a result")
    }
}

/// Errors constructing a [`MorselScheduler`].
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// Underlying Rayon pool build failed (e.g. invalid thread count
    /// or OS resource exhaustion).
    #[error("rayon thread pool build failed: {0}")]
    Pool(rayon::ThreadPoolBuildError),
}

impl From<BuildError> for bqlite_core::BqliteError {
    fn from(e: BuildError) -> Self {
        bqlite_core::BqliteError::Execution(format!("scheduler build failed: {e}"))
    }
}

/// Convenience for the engine: build the v1 scheduler directly from
/// an [`crate::EngineConfig`] without the caller needing to know the
/// `query_threads` resolution rule.
pub fn build_from_config(
    config: &crate::EngineConfig,
) -> std::result::Result<Arc<MorselScheduler>, BuildError> {
    MorselScheduler::new(config.resolve_query_threads())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::morsel::WindowSegments;
    use bqlite_core::storage::SegmentHandle;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn fake_snapshot(shard: u32) -> ShardSnapshot {
        ShardSnapshot {
            shard_id: shard,
            windows: vec![WindowSegments {
                window_id: 0,
                segments: Arc::from(vec![SegmentHandle {
                    segment_id: 0,
                    shard_id: shard,
                    window_id: 0,
                    row_count: 1,
                    schema_version: 1,
                    seq_id_first: 0,
                    batch_id: 0,
                }]),
            }],
        }
    }

    #[test]
    fn run_degenerate_dispatches_to_worker_and_returns_result() {
        let sched = MorselScheduler::new(1).expect("scheduler builds");
        let r: u32 = sched.run_degenerate(fake_snapshot(0), |g| {
            assert_eq!(g.morsel.shard_id, 0);
            42
        });
        assert_eq!(r, 42);
        // Permits are released on guard drop; full count is back.
        assert_eq!(sched.available_permits(), 1);
    }

    #[test]
    fn concurrent_queries_serialize_on_core_budget() {
        // 1-permit budget, 1 query_thread. Two concurrent queries
        // must serialize at acquire_n; the second blocks until the
        // first completes.
        let sched = MorselScheduler::new(1).expect("scheduler builds");

        let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let order_a = Arc::clone(&order);
        let sched_a = Arc::clone(&sched);
        let h_a = std::thread::spawn(move || {
            sched_a.run_degenerate(fake_snapshot(0), |_| {
                order_a.lock().unwrap().push("a-start");
                std::thread::sleep(Duration::from_millis(50));
                order_a.lock().unwrap().push("a-end");
            });
        });

        // Wait for A to claim the permit.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if sched.available_permits() == 0 {
                break;
            }
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }

        let order_b = Arc::clone(&order);
        let sched_b = Arc::clone(&sched);
        let h_b = std::thread::spawn(move || {
            sched_b.run_degenerate(fake_snapshot(0), |_| {
                order_b.lock().unwrap().push("b-start");
                order_b.lock().unwrap().push("b-end");
            });
        });

        h_a.join().unwrap();
        h_b.join().unwrap();

        let observed = order.lock().unwrap().clone();
        // a-end must come before b-start: serialization on the
        // permit gate guarantees no overlap.
        let a_end = observed.iter().position(|s| *s == "a-end").unwrap();
        let b_start = observed.iter().position(|s| *s == "b-start").unwrap();
        assert!(
            a_end < b_start,
            "queries must serialize on the budget permit: {observed:?}"
        );
    }

    #[test]
    fn run_degenerate_respects_send_bounds_for_borrowed_state() {
        // The closure captures a non-'static reference and a counter
        // by `&AtomicU32`; this exercises the rayon::scope lifetime
        // contract that lets `&mut Database` flow into the worker.
        let counter = AtomicU32::new(0);
        let sched = MorselScheduler::new(1).expect("scheduler builds");
        sched.run_degenerate(fake_snapshot(0), |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn build_from_config_resolves_query_threads() {
        let cfg = crate::EngineConfig {
            query_threads: Some(2),
            ..crate::EngineConfig::default()
        };
        let sched = build_from_config(&cfg).expect("scheduler builds");
        assert_eq!(sched.query_threads(), 2);
        assert_eq!(sched.available_permits(), 2);
    }
}
