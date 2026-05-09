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

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bqlite_core::BqliteError;
use bqlite_operators::CancellationToken;
use bqlite_storage::compaction::CoreBudget;

use super::accumulator::AccumulatorHandle;
use super::morsel::{Morsel, ShardSnapshot};
use super::queue::MorselQueue;
use super::worker::{ShardDoneCallback, WorkerMorselGuard};

/// Per-worker scratch carried through one [`MorselScheduler::run_per_shard`]
/// invocation. The dispatch hands a fresh `&mut PerWorkerCtx` into every
/// closure invocation on a given worker; the worker increments these
/// counters as it pulls morsels. After the dispatch returns, the engine
/// folds one [`crate::perf::WorkerMetricsSnapshot`] per unique worker
/// thread (identified via [`rayon::current_thread_index`]) into the
/// query metrics aggregate.
///
/// `PerWorkerCtx` is `Clone` (not `Copy`) because TASK-537 added the
/// per-morsel `processed_events` vector — sized at most by the morsel
/// count for this worker (≤ shard count today; sub-shard halving will
/// raise the ceiling). Construction sites use struct-update syntax:
/// `PerWorkerCtx { rayon_thread_index, ..PerWorkerCtx::default() }`.
#[derive(Debug, Default, Clone)]
pub struct PerWorkerCtx {
    /// Morsels this worker has pulled in the current dispatch.
    pub morsels_dispatched: u64,
    /// Index of this worker in the Rayon pool, captured on the first
    /// pull. `None` when the closure runs outside a Rayon worker
    /// (the `pool.scope` invariant rules that out for production
    /// callers, but tests may construct contexts directly).
    pub rayon_thread_index: Option<usize>,
    /// Wall-time the worker spent actively executing morsels — sum of
    /// `Instant::now()` deltas between morsel pull and morsel guard
    /// drop, recorded by the worker driver loop. TASK-537.
    pub worker_busy_ns: u64,
    /// Wall-time the worker spent waiting on its morsel queue —
    /// `pop_or_park` elapsed time per pull, summed across all pulls
    /// for this dispatch. Includes pulls that returned `Drained`. TASK-537.
    pub worker_idle_ns: u64,
    /// Per-morsel processed-event counts. The closure body pushes the
    /// row count it processed for the morsel after running its body.
    /// Folded into `entity_event_skew_p99` (p99 - p50 spread) at
    /// dispatch finalize per TASK-537 scope (b). The design doc names
    /// this metric as a per-entity sample, but TASK-537 explicitly
    /// scopes v1 to per-morsel counts as a proxy until the
    /// `EntityOperatorAdapter::finish_entity` hook lands.
    pub processed_events_per_morsel: Vec<u64>,
    /// Sum of CPU cycles read from this worker's `PerfCounters` during
    /// the dispatch — zero when CPU metrics collection is off or the
    /// platform integration is the disabled stub. TASK-537.
    pub total_cpu_cycles: u64,
    /// Sum of branch-prediction misses read from this worker's
    /// `PerfCounters` during the dispatch.
    pub branch_misses: u64,
    /// Sum of last-level cache misses read from this worker's
    /// `PerfCounters` during the dispatch.
    pub llc_misses: u64,
}

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
    /// **Panic boundary** (`cancellation.md` §4.1). The worker closure
    /// is wrapped in `catch_unwind`: a panic from `work` is caught at
    /// the worker boundary and converted to
    /// [`bqlite_core::BqliteError::OperatorPanic`] — it never escapes
    /// through Rayon's scope unwind. This is the per-(worker, morsel)
    /// boundary mandated by §4.1: when TASK-541 introduces per-morsel
    /// iteration, the boundary migrates inside the morsel loop in the
    /// same shape. The outer `catch_unwind` in
    /// `Engine::query_with_options` becomes defense-in-depth for the
    /// parse / plan / bind path that bypasses the scheduler.
    pub fn submit<T, F>(&self, work: F) -> bqlite_core::Result<T>
    where
        F: FnOnce() -> bqlite_core::Result<T> + Send,
        T: Send,
    {
        use std::panic::catch_unwind;

        let _permits = self.core_budget.acquire_n(self.query_threads);

        let result_slot: Mutex<Option<bqlite_core::Result<T>>> = Mutex::new(None);
        let result_ref = &result_slot;

        self.pool.scope(|s| {
            s.spawn(move |_| {
                // Catch panic inside the worker so the Rayon scope
                // does not see an unwinding panic and re-raise on
                // join. The conversion to OperatorPanic happens here;
                // peer workers (when TASK-541 lands per-morsel
                // iteration) observe the failure via the
                // QueryContext cancel_with_reason path. Currently
                // single-task — this is forward-compatible.
                let outcome = catch_unwind(AssertUnwindSafe(work));
                let mapped = match outcome {
                    Ok(r) => r,
                    Err(payload) => Err(bqlite_core::BqliteError::OperatorPanic {
                        message: panic_message(payload),
                        location: None,
                    }),
                };
                *result_ref.lock().expect("result slot poisoned") = Some(mapped);
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
            snapshot.windows.iter().any(|w| !w.segments.is_empty()),
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

    /// Multi-morsel dispatch (TASK-536, design §4.1 / §5).
    ///
    /// Build one [`super::morsel::MorselGenerator`] per non-empty
    /// `ShardSnapshot`, push every morsel into a single
    /// [`MorselQueue`], spawn `min(query_threads, snapshots.len())`
    /// Rayon workers each pulling morsels until the queue drains, and
    /// return the per-shard [`AccumulatorHandle`]s for the
    /// coordinator's cross-shard merge (§6.4).
    ///
    /// The closure `work` receives the [`WorkerMorselGuard`] for the
    /// pulled morsel plus a per-worker `&mut PerWorkerCtx` the engine
    /// folds into a [`crate::perf::WorkerMetricsSnapshot`]. The first
    /// `Err` from `work` cancels remaining work via `cancel`; the
    /// queue is drained without running further closures (guards still
    /// drop cleanly so outstanding-morsels accounting balances), and
    /// the first error propagates back to the caller. Subsequent
    /// errors are dropped — design §9.2 first-error-wins.
    ///
    /// **Panic isolation.** Each `work` call is wrapped in
    /// `catch_unwind` (design §9.2) — a panic in one shard's worker
    /// does not bring down the Rayon pool. The panic payload is
    /// converted to `BqliteError::OperatorPanic` and stored in the
    /// first-error slot; remaining workers observe `cancel` at the
    /// next morsel pull and unwind cleanly.
    ///
    /// **Cancellation.** Workers check `cancel.is_cancelled()` at the
    /// top of every pull iteration. Per design §9.1 this is the
    /// "between morsels" yield point; per-batch cancellation is the
    /// caller's responsibility inside `work`.
    ///
    /// **Permits.** `query_threads` permits are acquired from the
    /// [`CoreBudget`] once at the start of the call and released when
    /// the dispatch completes — identical to [`Self::submit`]'s
    /// permit story.
    ///
    /// With zero snapshots the method returns `Ok(Vec::new())` without
    /// touching the worker pool.
    pub fn run_per_shard<F>(
        &self,
        snapshots: &[ShardSnapshot],
        cancel: &CancellationToken,
        collect_cpu_metrics: bool,
        work: F,
    ) -> std::result::Result<PerShardOutcome, BqliteError>
    where
        F: Fn(&mut WorkerMorselGuard, &mut PerWorkerCtx) -> std::result::Result<(), BqliteError>
            + Send
            + Sync,
    {
        if snapshots.is_empty() {
            return Ok(PerShardOutcome::default());
        }
        let _permits = self.core_budget.acquire_n(self.query_threads);

        // Per-shard bookkeeping: one AccumulatorHandle + one generator
        // per shard. The handles are returned to the caller for the
        // cross-shard merge.
        let accumulators: Vec<Arc<AccumulatorHandle>> = snapshots
            .iter()
            .map(|s| Arc::new(AccumulatorHandle::new(s.shard_id, None)))
            .collect();
        let by_shard: std::collections::HashMap<u32, Arc<AccumulatorHandle>> = accumulators
            .iter()
            .map(|h| (h.shard_id(), Arc::clone(h)))
            .collect();

        // Sized to hold every morsel the single-producer enumeration
        // below pushes plus a small safety margin. v1 generator emits
        // exactly one morsel per shard; sub-shard halving is not yet
        // dispatched through this path.
        let queue = Arc::new(MorselQueue::new(snapshots.len().max(1)));

        for snap in snapshots {
            let mut gen_ = super::morsel::MorselGenerator::degenerate(snap.clone());
            while let Some(m) = gen_.take_next() {
                queue.push(m).expect("queue capacity sized for all morsels");
            }
            let total_for_shard = gen_.total_emitted().unwrap_or(0);
            if let Some(handle) = by_shard.get(&snap.shard_id) {
                handle.mark_total_emitted(total_for_shard);
            }
        }
        queue.mark_drained();

        let num_workers = self.query_threads.min(snapshots.len()).max(1);
        let first_error: Mutex<Option<BqliteError>> = Mutex::new(None);
        let first_error_ref = &first_error;
        let queue_for_workers = Arc::clone(&queue);
        let by_shard_for_workers = &by_shard;
        let work_ref = &work;
        let cancel_for_workers = cancel.clone();
        let per_worker_ctxs: Mutex<Vec<PerWorkerCtx>> = Mutex::new(Vec::new());
        let per_worker_ctxs_ref = &per_worker_ctxs;
        // Whether *any* worker successfully opened a real perf-counter
        // group. Folded into `PerShardOutcome::cpu_counters_available`
        // for the CLI render path (TASK-537 CP3).
        let cpu_counters_available: Mutex<bool> = Mutex::new(false);
        let cpu_counters_available_ref = &cpu_counters_available;

        self.pool.scope(|s| {
            for _ in 0..num_workers {
                let q = Arc::clone(&queue_for_workers);
                let cancel_w = cancel_for_workers.clone();
                s.spawn(move |_| {
                    let mut ctx = PerWorkerCtx {
                        rayon_thread_index: rayon::current_thread_index(),
                        ..PerWorkerCtx::default()
                    };
                    // Open one perf-counter group per worker thread.
                    // Disabled when the engine did not opt into CPU
                    // metrics, on platforms without an integration, or
                    // when the kernel refuses (`CAP_PERFMON` missing).
                    // TASK-537 scope (c) / (d).
                    let mut perf = if collect_cpu_metrics {
                        crate::perf::PerfCounters::open_or_disabled()
                    } else {
                        crate::perf::PerfCounters::disabled()
                    };
                    if perf.is_enabled() {
                        *cpu_counters_available_ref
                            .lock()
                            .expect("cpu_counters_available poisoned") = true;
                    }
                    loop {
                        // Cancellation between morsels (design §9.1).
                        if cancel_w.is_cancelled() {
                            // Drain the queue with empty guards so
                            // outstanding-morsels accounting balances.
                            while let Some(m) = q.pop() {
                                let acc = by_shard_for_workers
                                    .get(&m.shard_id)
                                    .cloned()
                                    .expect("morsel shard has accumulator");
                                let on_done: ShardDoneCallback = Arc::new(|_, _| {});
                                let g = WorkerMorselGuard::new(m, acc, on_done);
                                drop(g);
                            }
                            break;
                        }
                        // Idle sample: time spent inside `pop_or_park`.
                        // Includes pulls that hand back `Drained` so a
                        // worker that fails to find work does not make
                        // its idle row look artificially zero.
                        let pre_pull = std::time::Instant::now();
                        let morsel = match q.pop_or_park(Duration::from_millis(10)) {
                            Ok(m) => {
                                ctx.worker_idle_ns =
                                    ctx.worker_idle_ns.saturating_add(elapsed_ns(pre_pull));
                                m
                            }
                            Err(_drained) => {
                                ctx.worker_idle_ns =
                                    ctx.worker_idle_ns.saturating_add(elapsed_ns(pre_pull));
                                break;
                            }
                        };
                        let shard = morsel.shard_id;
                        let acc = by_shard_for_workers
                            .get(&shard)
                            .cloned()
                            .expect("morsel shard has accumulator");
                        let on_done: ShardDoneCallback = Arc::new(|_, _| {});
                        let mut guard = WorkerMorselGuard::new(morsel, acc, on_done);
                        ctx.morsels_dispatched = ctx.morsels_dispatched.saturating_add(1);
                        // Busy sample: wall time from morsel pull to
                        // guard drop, per design §8.3 (one Instant
                        // pair per morsel).
                        let pre_busy = std::time::Instant::now();
                        let perf_pre = perf.snapshot_counters();
                        // Snapshot the events vec length so we can
                        // detect whether the closure pushed an entry —
                        // a closure that errors out before recording
                        // its row count gets a 0-sample.
                        let events_before = ctx.processed_events_per_morsel.len();
                        // Panic isolation per design §9.2: convert
                        // panic payloads into BqliteError::OperatorPanic
                        // and propagate via the first-error slot.
                        let unwound = std::panic::catch_unwind(AssertUnwindSafe(|| {
                            work_ref(&mut guard, &mut ctx)
                        }));
                        let outcome: std::result::Result<(), BqliteError> = match unwound {
                            Ok(r) => r,
                            Err(payload) => Err(BqliteError::OperatorPanic {
                                message: panic_message(payload),
                                location: None,
                            }),
                        };
                        if ctx.processed_events_per_morsel.len() == events_before {
                            // Closure did not record an event count
                            // (error path or omitted call). Push 0 so
                            // the per-morsel sample count matches
                            // `morsels_dispatched`.
                            ctx.processed_events_per_morsel.push(0);
                        }
                        // Guard drop runs before we look at the result
                        // slot — outstanding-morsels accounting must
                        // balance even on the error path.
                        drop(guard);
                        let perf_delta = perf.delta_since(perf_pre);
                        ctx.total_cpu_cycles =
                            ctx.total_cpu_cycles.saturating_add(perf_delta.cpu_cycles);
                        ctx.branch_misses =
                            ctx.branch_misses.saturating_add(perf_delta.branch_misses);
                        ctx.llc_misses = ctx.llc_misses.saturating_add(perf_delta.llc_misses);
                        ctx.worker_busy_ns =
                            ctx.worker_busy_ns.saturating_add(elapsed_ns(pre_busy));
                        if let Err(e) = outcome {
                            let mut slot = first_error_ref.lock().expect("first_error poisoned");
                            if slot.is_none() {
                                *slot = Some(e);
                            }
                            cancel_w.cancel();
                            // Don't break — let the cancel-check at the
                            // top of the loop drain remaining morsels.
                        }
                    }
                    per_worker_ctxs_ref
                        .lock()
                        .expect("per_worker_ctxs poisoned")
                        .push(ctx);
                });
            }
        });

        if let Some(err) = first_error.into_inner().expect("first_error poisoned") {
            return Err(err);
        }
        let per_worker = per_worker_ctxs
            .into_inner()
            .expect("per_worker_ctxs poisoned");
        let cpu_counters_available = cpu_counters_available
            .into_inner()
            .expect("cpu_counters_available poisoned");
        Ok(PerShardOutcome {
            accumulators,
            per_worker,
            cpu_counters_available,
        })
    }
}

/// Outcome of a successful [`MorselScheduler::run_per_shard`] dispatch.
///
/// Carries both the per-shard [`AccumulatorHandle`]s the coordinator
/// merges in design §6.4 and the per-worker scratch the engine folds
/// into the query metrics aggregate (one [`crate::perf::WorkerMetricsSnapshot`]
/// per Rayon thread that pulled at least one morsel).
#[derive(Debug, Default)]
pub struct PerShardOutcome {
    /// One handle per `ShardSnapshot` that was passed in.
    pub accumulators: Vec<Arc<AccumulatorHandle>>,
    /// One context per worker thread that contributed (zero
    /// contexts for an empty input).
    pub per_worker: Vec<PerWorkerCtx>,
    /// `true` iff at least one worker successfully opened a real
    /// `PerfCounters` group (i.e. the kernel honoured the
    /// `perf_event_open` syscall). Disambiguates "user opted in but
    /// kernel refused" from "user did not opt in" for the
    /// `--explain-perf` CLI rendering. See TASK-537 scope (f).
    pub cpu_counters_available: bool,
}

/// Convert an `Instant`'s elapsed time to a saturating `u64` of
/// nanoseconds. Used by the worker driver loop to record per-morsel
/// busy / idle samples without panicking on the (impossible) overflow.
fn elapsed_ns(since: std::time::Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Extract a human-readable message from a `catch_unwind` payload —
/// matches `query.rs::panic_message` so the conversion is identical
/// across both the engine-level catch and the per-worker catch.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
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

    // ── run_per_shard (TASK-536) ─────────────────────────────────────

    #[test]
    fn run_per_shard_dispatches_one_task_per_morsel() {
        let sched = MorselScheduler::new(4).expect("scheduler builds");
        let snaps: Vec<ShardSnapshot> = (0..4).map(fake_snapshot).collect();
        let calls = AtomicU32::new(0);
        let observed_shards: Mutex<Vec<u32>> = Mutex::new(Vec::new());

        let cancel = bqlite_operators::CancellationToken::new();
        let outcome = sched
            .run_per_shard(&snaps, &cancel, false, |guard, _ctx| {
                calls.fetch_add(1, Ordering::Relaxed);
                observed_shards.lock().unwrap().push(guard.morsel.shard_id);
                Ok(())
            })
            .expect("run_per_shard returns OK");

        assert_eq!(calls.load(Ordering::Relaxed), 4, "one call per shard");
        let mut got = observed_shards.lock().unwrap().clone();
        got.sort();
        assert_eq!(
            got,
            vec![0, 1, 2, 3],
            "every shard must be processed exactly once"
        );
        assert_eq!(outcome.accumulators.len(), 4);
        for h in &outcome.accumulators {
            assert!(h.is_done(), "every accumulator must signal done");
        }
        // Per-worker contexts populated for every worker that pulled a
        // morsel; total morsels across all workers equals the input
        // shard count.
        let total: u64 = outcome
            .per_worker
            .iter()
            .map(|c| c.morsels_dispatched)
            .sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn run_per_shard_with_zero_shards_returns_empty_outcome() {
        let sched = MorselScheduler::new(2).expect("scheduler builds");
        let cancel = bqlite_operators::CancellationToken::new();
        let outcome = sched
            .run_per_shard(&[] as &[ShardSnapshot], &cancel, false, |_, _| {
                unreachable!("must not be called");
            })
            .expect("empty dispatch ok");
        assert!(outcome.accumulators.is_empty());
        assert!(outcome.per_worker.is_empty());
        // Permits are released even on the empty path.
        assert_eq!(sched.available_permits(), 2);
    }

    #[test]
    fn run_per_shard_propagates_first_worker_error() {
        let sched = MorselScheduler::new(2).expect("scheduler builds");
        let snaps: Vec<ShardSnapshot> = (0..2).map(fake_snapshot).collect();
        let cancel = bqlite_operators::CancellationToken::new();
        let result = sched.run_per_shard(&snaps, &cancel, false, |guard, _ctx| {
            if guard.morsel.shard_id == 1 {
                Err(BqliteError::Execution("boom".into()))
            } else {
                Ok(())
            }
        });
        let err = result.unwrap_err();
        assert!(
            matches!(err, BqliteError::Execution(ref m) if m == "boom"),
            "{err:?}"
        );
        // Cancel was set on first error.
        assert!(cancel.is_cancelled());
    }

    #[test]
    fn run_per_shard_panic_in_worker_surfaces_as_operator_panic() {
        let sched = MorselScheduler::new(2).expect("scheduler builds");
        let snaps: Vec<ShardSnapshot> = (0..2).map(fake_snapshot).collect();
        let cancel = bqlite_operators::CancellationToken::new();
        let result = sched.run_per_shard(&snaps, &cancel, false, |guard, _ctx| {
            if guard.morsel.shard_id == 0 {
                panic!("synthetic shard-0 panic");
            }
            Ok(())
        });
        let err = result.unwrap_err();
        match err {
            BqliteError::OperatorPanic { message, .. } => {
                assert!(
                    message.contains("synthetic shard-0 panic"),
                    "unexpected panic message: {message}"
                );
            }
            other => panic!("expected OperatorPanic, got {other:?}"),
        }
    }

    #[test]
    fn run_per_shard_observes_external_cancellation() {
        // Pre-cancel the token before dispatch — workers must
        // observe it at the morsel pull and exit without running
        // the closure for any shard.
        let sched = MorselScheduler::new(2).expect("scheduler builds");
        let snaps: Vec<ShardSnapshot> = (0..3).map(fake_snapshot).collect();
        let calls = AtomicU32::new(0);
        let cancel = bqlite_operators::CancellationToken::new();
        cancel.cancel();
        let outcome = sched
            .run_per_shard(&snaps, &cancel, false, |_g, _ctx| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .expect("pre-cancel does not error");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            0,
            "no closure must run after pre-cancel"
        );
        // Outstanding-morsels accounting balanced — every shard's
        // accumulator transitioned to done.
        for h in &outcome.accumulators {
            assert!(
                h.is_done(),
                "accumulator for shard {} not done",
                h.shard_id()
            );
        }
    }

    #[test]
    fn run_per_shard_records_per_morsel_timing() {
        // The worker driver loop instrumentation must populate
        // `worker_busy_ns` to a non-zero value when the closure body
        // takes measurable wall-clock time. TASK-537.
        let sched = MorselScheduler::new(2).expect("scheduler builds");
        let snaps: Vec<ShardSnapshot> = (0..2).map(fake_snapshot).collect();
        let cancel = bqlite_operators::CancellationToken::new();
        let outcome = sched
            .run_per_shard(&snaps, &cancel, false, |_g, ctx_w| {
                std::thread::sleep(Duration::from_millis(5));
                ctx_w.processed_events_per_morsel.push(42);
                Ok(())
            })
            .expect("ok");
        let total_busy_ns: u64 = outcome.per_worker.iter().map(|c| c.worker_busy_ns).sum();
        assert!(
            total_busy_ns > 1_000_000,
            "worker_busy_ns must be ≥ 1ms after a 5ms sleep per morsel; got {total_busy_ns} ns"
        );
        // Per-morsel events vec sized to one entry per morsel pulled.
        for c in &outcome.per_worker {
            assert_eq!(
                c.processed_events_per_morsel.len() as u64,
                c.morsels_dispatched,
                "every morsel must record an events sample"
            );
        }
    }

    #[test]
    fn run_per_shard_records_rayon_thread_index() {
        let sched = MorselScheduler::new(2).expect("scheduler builds");
        let snaps: Vec<ShardSnapshot> = (0..4).map(fake_snapshot).collect();
        let cancel = bqlite_operators::CancellationToken::new();
        let outcome = sched
            .run_per_shard(&snaps, &cancel, false, |_g, _ctx| Ok(()))
            .expect("ok");
        // Every per-worker context was populated by a Rayon worker, so
        // rayon_thread_index must be Some(_).
        for c in &outcome.per_worker {
            assert!(
                c.rayon_thread_index.is_some(),
                "rayon_thread_index missing — was the closure dispatched on a Rayon worker?"
            );
        }
    }

    #[test]
    fn submit_returns_worker_result() {
        // Sanity that the new Result-returning shape round-trips a
        // success.
        let cfg = crate::EngineConfig::default();
        let scheduler = build_from_config(&cfg).expect("scheduler builds");
        let r: bqlite_core::Result<u32> = scheduler.submit(|| Ok(42));
        assert_eq!(r.unwrap(), 42);
        assert_eq!(scheduler.available_permits(), scheduler.query_threads());
    }

    #[test]
    fn submit_propagates_worker_error_unchanged() {
        // A non-panic Err from the closure round-trips unchanged.
        let cfg = crate::EngineConfig::default();
        let scheduler = build_from_config(&cfg).expect("scheduler builds");
        let r: bqlite_core::Result<u32> = scheduler.submit(|| {
            Err(bqlite_core::BqliteError::Execution(
                "synthetic error".to_string(),
            ))
        });
        match r {
            Err(bqlite_core::BqliteError::Execution(msg)) => {
                assert_eq!(msg, "synthetic error");
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn submit_catches_worker_panic_as_operator_panic() {
        // A panic from the closure is caught at the worker boundary
        // and converted to BqliteError::OperatorPanic — it never
        // re-raises through Rayon's scope unwind. cancellation.md §4.1.
        let cfg = crate::EngineConfig::default();
        let scheduler = build_from_config(&cfg).expect("scheduler builds");
        let result: bqlite_core::Result<()> = scheduler.submit(|| {
            panic!("synthetic worker panic for test");
        });
        match result {
            Err(bqlite_core::BqliteError::OperatorPanic { message, .. }) => {
                assert!(
                    message.contains("synthetic worker panic for test"),
                    "panic payload should be in OperatorPanic message: {message}"
                );
            }
            other => panic!("expected OperatorPanic, got {other:?}"),
        }
        // Permits released cleanly — the worker boundary's catch did
        // not poison the CoreBudget.
        assert_eq!(
            scheduler.available_permits(),
            scheduler.query_threads(),
            "permits must be released even after a worker panic"
        );
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
