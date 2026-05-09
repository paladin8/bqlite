//! Per-query runtime context: memory budget, cancellation, and tile size.
//!
//! Per [`docs/design/engine/memory-budget.md`](../../../docs/design/engine/memory-budget.md)
//! § 3.2, the engine constructs exactly one [`QueryContext`] per
//! `Engine::query` invocation and threads it into every operator
//! through the bind step. The context owns the
//! [`Arc<dyn MemoryBudget>`] for the query and the shared
//! [`CancellationToken`] that future timeout / Ctrl-C work
//! (TASK-505) will signal.
//!
//! ## Wave 5 scope (TASK-510)
//!
//! This module ships the scaffold:
//!
//! - [`EngineConfig`] — host-level configuration. Wraps the per-query
//!   default budget and (placeholder) compaction / ingest budgets so
//!   `Engine::with_config` can override them.
//! - [`QueryOptions`] — per-`execute()` overrides. Currently exposes
//!   `memory_budget_bytes` only; later waves add timeout, warnings,
//!   etc.
//! - [`QueryContext`] — runtime handle threaded into bind. Owns the
//!   tracker `Arc` and the shared cancellation token.
//!
//! Operator constructors that allocate dynamically will accept
//! `Arc<dyn MemoryBudget>` (read from the context) in TASK-512/513/514.
//! TASK-510 stops at making the context available so those tasks have a
//! single seam to wire against.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use bqlite_core::metrics::{AtomicMetrics, Metrics};
use bqlite_core::spill::{SpillFs, SpillQueryId, TempSpillFile};
use bqlite_core::{memory::MemoryTracker, BqliteError, MemoryBudget, Result, UnboundedMemory};
use bqlite_operators::CancellationToken;

use crate::perf::{QueryMetrics, WorkerMetricsSnapshot};

// ─────────────────────────────────────────────────────────────────────────────
// Defaults & constants (design doc § 2.2 / § 8)
// ─────────────────────────────────────────────────────────────────────────────

/// Default per-query memory budget — 3 GiB.
///
/// Per `docs/design/engine/memory-budget.md` § 2.2. The engine-wide
/// aggregate (~4 GiB) is the sum of this plus the compaction (800 MiB)
/// and ingest (256 MiB) defaults; those budgets enforce themselves
/// independently and are not contended through `QueryContext`.
pub const DEFAULT_QUERY_BUDGET_BYTES: u64 = 3 * 1024 * 1024 * 1024;

/// Default compaction memory budget — 800 MiB.
///
/// Stored on `EngineConfig` for symmetry; not consumed by
/// `QueryContext` (compaction does not run inside a query).
pub const DEFAULT_COMPACTION_BUDGET_BYTES: u64 = 800 * 1024 * 1024;

/// Default ingest-partitioner memory budget — 256 MiB.
pub const DEFAULT_INGEST_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

/// Floor for per-query budget overrides — 512 MiB.
///
/// Below this, the fixed per-worker working set (k-way merge buffers
/// plus current batch) leaves no room for tracked allocations and the
/// query cannot make forward progress on small / medium hosts. See
/// design doc § 8.2 for the derivation.
pub const MIN_QUERY_BUDGET_BYTES: u64 = 512 * 1024 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// CancelReason — first-fire attribution for cooperative cancel paths.
// (engine/cancellation.md §3.1)
// ─────────────────────────────────────────────────────────────────────────────

/// Why a query was cancelled. Set exactly once per [`QueryContext`] via
/// the first-fire CAS on [`QueryContext::cancel_with_reason`]; subsequent
/// fires lose the race and are silently dropped. Read at result
/// collection in `Engine::query_with_options` to discriminate
/// `BqliteError::Cancelled` from `BqliteError::Timeout`.
///
/// `LimitHit` is included for completeness with the design doc — the
/// `LimitOperator` calls `cancel_with_reason(LimitHit)` to short-circuit
/// once it has produced the requested rows. Per cancellation.md §3.1
/// case 4, the driver maps `LimitHit` to `Ok(...)` at result collection
/// (the in-flight rows were already collected before the token fired).
/// TASK-538 records the variant but does not yet rewire `LimitOperator`
/// to use `cancel_with_reason` — the existing operator calls
/// `cancellation().cancel()` directly, and the driver's "no reason"
/// path still produces the right answer because the rows are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CancelReason {
    /// No cancellation has fired.
    None = 0,
    /// Caller-initiated cancel via `QueryOptions::cancel`.
    Cancelled = 1,
    /// Per-query timeout timer fired.
    Timeout = 2,
    /// `LimitOperator` short-circuited after producing the requested rows.
    LimitHit = 3,
}

impl CancelReason {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => CancelReason::Cancelled,
            2 => CancelReason::Timeout,
            3 => CancelReason::LimitHit,
            _ => CancelReason::None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EngineConfig
// ─────────────────────────────────────────────────────────────────────────────

/// Host-level configuration for an [`crate::Engine`] instance.
///
/// All byte fields are bytes. Defaults match § 2.2 of the design doc. The
/// compaction and ingest fields are placeholders for the schedulers that
/// own those budgets; `QueryContext` consumes only `query_memory_budget_bytes`.
#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
    /// Default per-query budget. Overridable per submission via
    /// [`QueryOptions::memory_budget_bytes`].
    pub query_memory_budget_bytes: u64,
    /// Compaction worker pool ceiling (out of scope for the query
    /// pipeline — kept here so a host can configure all three sub-
    /// budgets through a single struct).
    pub compaction_memory_budget_bytes: u64,
    /// Ingest partitioner ceiling (out of scope for the query
    /// pipeline; consumed by `Partitioner::new`).
    pub ingest_memory_budget_bytes: u64,
    /// Worker pool size for the engine's morsel scheduler
    /// (engine/morsel-scheduler.md §5.1). `None` defaults to
    /// `available_parallelism()`, falling back to 4 when the platform
    /// cannot report. Capped at the underlying [`bqlite_storage::CoreBudget`]
    /// permit count, which is initialised to `num_cores`.
    pub query_threads: Option<usize>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            query_memory_budget_bytes: DEFAULT_QUERY_BUDGET_BYTES,
            compaction_memory_budget_bytes: DEFAULT_COMPACTION_BUDGET_BYTES,
            ingest_memory_budget_bytes: DEFAULT_INGEST_BUDGET_BYTES,
            query_threads: None,
        }
    }
}

impl EngineConfig {
    /// Resolve the effective worker count, applying the
    /// `available_parallelism` default when `query_threads` is unset.
    /// Per design §5.1 the fallback is 4 when the platform cannot
    /// report.
    pub fn resolve_query_threads(&self) -> usize {
        self.query_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryOptions
// ─────────────────────────────────────────────────────────────────────────────

/// Per-submission overrides for a single `Engine::query` call.
///
/// `Default` produces an empty option set (all `None` / `false`), so
/// existing callers that pass nothing still get the engine-level
/// defaults. Constructed with struct update syntax
/// (`..QueryOptions::default()`) so additive field changes do not
/// break existing call sites.
///
/// `Copy` was previously implemented but is no longer because
/// [`bqlite_operators::CancellationToken`] (an `Arc<AtomicBool>`
/// under the hood) is not `Copy`. The struct remains `Clone`.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Override the per-query memory budget. Validated against
    /// [`MIN_QUERY_BUDGET_BYTES`] at submission time.
    pub memory_budget_bytes: Option<u64>,
    /// Opt this query into CPU-cost sampling per
    /// `docs/design/execution-model.md` §14.3. The CLI's
    /// `--explain-perf` flag sets this; normal queries leave it
    /// `false` so the perf-counter overhead never lands on the
    /// hot path. Today the platform integration is a stub
    /// ([`crate::perf::PerfCounters`]); the flag still propagates
    /// through to `QueryMetrics::cpu_metrics_enabled` so callers can
    /// distinguish "CPU counters were sampled but were zero" from
    /// "CPU counters were never enabled".
    pub collect_cpu_metrics: bool,
    /// External cancellation handle. When `Some`, the token is
    /// installed as the per-query [`bqlite_operators::CancellationToken`]
    /// — operators see the same flag the caller set. When `None`, the
    /// engine constructs a fresh token (the original Wave 1 default).
    /// Per `cancellation.md` §3.1 source 1 (caller-initiated cancel).
    pub cancel: Option<bqlite_operators::CancellationToken>,
    /// Per-query timeout. When `Some(d)`, the engine spawns a timer
    /// thread that fires `cancel_with_reason(Timeout)` after `d`
    /// elapses; the driver maps the resulting `BqliteError::Cancelled`
    /// to `BqliteError::Timeout { elapsed_ms }` at result collection.
    /// Per `cancellation.md` §3.1 source 2 (query timeout).
    ///
    /// Latency target: the timer signal is observed at the next yield
    /// point — batch / sub-batch / morsel boundary — per
    /// `cancellation.md` §3.2.
    ///
    /// `Some(Duration::ZERO)` is a deterministic-fire shorthand: the
    /// timer fires synchronously at spawn time, before any operator
    /// runs. Useful for tests and for callers that want to fail fast.
    pub timeout: Option<std::time::Duration>,
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryContext
// ─────────────────────────────────────────────────────────────────────────────

/// Per-query runtime context.
///
/// Constructed once per `Engine::query` invocation and threaded into
/// every operator via the bind step. Operators clone the
/// [`CancellationToken`] cheaply (it shares an `Arc<AtomicBool>`) and
/// hold an `Arc<dyn MemoryBudget>` for the duration of the operator's
/// life.
///
/// The context is intentionally `Clone` (cheap — every field is
/// already `Arc` or zero-cost-clone) so bind helpers can hand sub-trees
/// their own context references without lifetime games.
///
/// ## Spill cleanup
///
/// When the *last* clone drops, the inner `SpillCleanup` guard runs
/// `SpillFs::cleanup_query(qid)` so any spill files that survived
/// their owning [`TempSpillFile`] guard's `Drop` (e.g. an `EBUSY` we
/// did not log) are reclaimed. This is the belt-and-braces sweep
/// mandated by `engine/spill.md` § 8.3, ordered after the
/// operator-tree drop because `Engine::query` drops the operator
/// tree first and the context last.
#[derive(Clone)]
pub struct QueryContext {
    cancellation: CancellationToken,
    memory: Arc<dyn MemoryBudget>,
    /// Concrete tracker handle when the context owns a real
    /// [`MemoryTracker`]. `None` for [`UnboundedMemory`]. Used at
    /// query teardown to surface `peak_bytes()` through
    /// `ExecutionResult::peak_memory_bytes` without exposing
    /// `peak_bytes` on the public trait.
    tracker: Option<Arc<MemoryTracker>>,
    /// Per-query warning sink. Operators publish per-entity
    /// diagnostics here through the EntityOperatorAdapter; the engine
    /// drains it into `ExecutionResult.warnings` on success or
    /// `ExecutionFailure.warnings` on failure. See
    /// `docs/design/engine/cancellation.md` §7.
    warnings: crate::warning_sink::WarningSink,
    /// Per-database spill filesystem. Cloned from the [`Database`] at
    /// `Engine::query` construction time. `None` for unbounded /
    /// test-only contexts that never spill.
    spill_fs: Option<Arc<SpillFs>>,
    /// Per-query identifier paired with `spill_fs`. Allocated lazily
    /// the first time a `SpillFs` is attached to the context.
    spill_query_id: Option<SpillQueryId>,
    /// RAII guard whose `Drop` runs `cleanup_query` on the spill tree
    /// once the last clone of this context goes out of scope. `None`
    /// when no `spill_fs` is attached.
    _cleanup: Option<Arc<SpillCleanup>>,
    /// Per-query operator-counter handle. Shared across every operator
    /// in the tree so a single `snapshot()` at teardown captures the
    /// query-level totals. Default is a fresh [`AtomicMetrics`] so the
    /// shape is non-zero even for unbounded / test contexts.
    metrics: Arc<dyn Metrics>,
    /// Cross-worker aggregate accumulating `WorkerMetricsSnapshot`s
    /// recorded through [`QueryContext::record_worker_snapshot`]. The
    /// single-threaded driver records exactly one snapshot today; the
    /// morsel-scheduler follow-up will record one per worker.
    worker_aggregate: Arc<Mutex<QueryMetrics>>,
    /// First-fire reason for the cooperative cancel path
    /// (`engine/cancellation.md` §3.1). Stored as a `u8` because
    /// `AtomicEnum` is not in std; values map through
    /// [`CancelReason::from_u8`].
    cancel_reason: Arc<AtomicU8>,
    /// Whether CPU-cost sampling is opted in for this query
    /// (`PerfCounters::open_or_disabled` returns enabled only when
    /// this flag is set and the platform supports it). Default
    /// `false` per `execution-model.md` §14.3 — perf collection is
    /// off in normal queries.
    collect_cpu_metrics: bool,
}

/// Belt-and-braces sweep: when the last `Arc<SpillCleanup>` drops, the
/// per-query subdirectory is reclaimed. Operators' `TempSpillFile`
/// guards delete individual files on drop already; this is the
/// safety net for guards whose `Drop` failed silently (e.g. EBUSY
/// not logged). See `docs/design/engine/spill.md` § 8.3.
struct SpillCleanup {
    spill_fs: Arc<SpillFs>,
    query_id: SpillQueryId,
}

impl Drop for SpillCleanup {
    fn drop(&mut self) {
        self.spill_fs.cleanup_query(self.query_id);
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryContext")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("cancel_reason", &self.cancel_reason())
            .field("memory_used_bytes", &self.memory.used_bytes())
            .field("memory_budget_bytes", &self.memory.budget_bytes())
            .field("has_tracker", &self.tracker.is_some())
            .field("has_spill_fs", &self.spill_fs.is_some())
            .field("spill_query_id", &self.spill_query_id)
            .finish()
    }
}

impl QueryContext {
    /// Build a context backed by a real per-query [`MemoryTracker`].
    pub fn new(budget_bytes: u64) -> Self {
        let tracker = MemoryTracker::new(budget_bytes);
        Self {
            cancellation: CancellationToken::new(),
            memory: tracker.clone(),
            tracker: Some(tracker),
            warnings: crate::warning_sink::WarningSink::new(),
            spill_fs: None,
            spill_query_id: None,
            _cleanup: None,
            metrics: Arc::new(AtomicMetrics::new()),
            worker_aggregate: Arc::new(Mutex::new(QueryMetrics::zero())),
            cancel_reason: Arc::new(AtomicU8::new(CancelReason::None as u8)),
            collect_cpu_metrics: false,
        }
    }

    /// Build a tracker-backed context whose cancellation flag is
    /// driven by an externally-supplied [`CancellationToken`]. The
    /// caller retains a clone of the same token and observes any
    /// internal cancel signal (timeout fire, panic peer-shutdown) by
    /// reading it.
    ///
    /// This is a constructor (not a builder) so the token is in place
    /// before any clone of `self` can be made — no observer ever sees
    /// the about-to-be-replaced default token.
    pub fn new_with_external_cancellation(budget_bytes: u64, token: CancellationToken) -> Self {
        let tracker = MemoryTracker::new(budget_bytes);
        Self {
            cancellation: token,
            memory: tracker.clone(),
            tracker: Some(tracker),
            warnings: crate::warning_sink::WarningSink::new(),
            spill_fs: None,
            spill_query_id: None,
            _cleanup: None,
            metrics: Arc::new(AtomicMetrics::new()),
            worker_aggregate: Arc::new(Mutex::new(QueryMetrics::zero())),
            cancel_reason: Arc::new(AtomicU8::new(CancelReason::None as u8)),
            collect_cpu_metrics: false,
        }
    }

    /// Build an unbounded context — every reservation succeeds, no
    /// peak is reported. Intended for tests and any path that has not
    /// yet been wired against the real tracker.
    pub fn unbounded() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            memory: Arc::new(UnboundedMemory::new()),
            tracker: None,
            warnings: crate::warning_sink::WarningSink::new(),
            spill_fs: None,
            spill_query_id: None,
            _cleanup: None,
            metrics: Arc::new(AtomicMetrics::new()),
            worker_aggregate: Arc::new(Mutex::new(QueryMetrics::zero())),
            cancel_reason: Arc::new(AtomicU8::new(CancelReason::None as u8)),
            collect_cpu_metrics: false,
        }
    }

    /// Attach a per-database `SpillFs` to this context, allocating a
    /// fresh per-query identifier and registering the belt-and-braces
    /// `cleanup_query` sweep that fires when the last clone drops.
    ///
    /// Idempotent at most once per context — the engine constructs a
    /// fresh `QueryContext` for every `Engine::query` call, so this is
    /// only ever called from the engine's query path.
    pub fn with_spill_fs(mut self, spill_fs: Arc<SpillFs>) -> Self {
        let query_id = spill_fs.new_query_id();
        let cleanup = Arc::new(SpillCleanup {
            spill_fs: Arc::clone(&spill_fs),
            query_id,
        });
        self.spill_fs = Some(spill_fs);
        self.spill_query_id = Some(query_id);
        self._cleanup = Some(cleanup);
        self
    }

    /// Cancellation token shared across every operator in this query.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Read the current first-fire cancel reason. `None` until any
    /// cooperative source CAS-installs a reason via
    /// [`Self::cancel_with_reason`].
    pub fn cancel_reason(&self) -> CancelReason {
        CancelReason::from_u8(self.cancel_reason.load(Ordering::Acquire))
    }

    /// Mark the query cancelled with a structured reason. CAS-installs
    /// `reason` from `None`; second fires lose the race and are
    /// silently dropped. After the CAS (won or lost) the cancellation
    /// token is set so operators stop at their next yield point.
    ///
    /// `reason` must not be [`CancelReason::None`] — that variant
    /// represents the "no cancellation fired" sentinel and installing
    /// it would defeat the first-fire rule. Callers that want to flip
    /// the cancellation token without recording a reason should use
    /// `cancellation().cancel()` directly (the external-cancel path
    /// in `Engine::query_with_options` does exactly this).
    pub fn cancel_with_reason(&self, reason: CancelReason) {
        debug_assert_ne!(
            reason as u8,
            CancelReason::None as u8,
            "cancel_with_reason must record a real reason"
        );
        let _ = self.cancel_reason.compare_exchange(
            CancelReason::None as u8,
            reason as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.cancellation.cancel();
    }

    /// Memory budget for this query. Operators that allocate dynamically
    /// hold an `Arc<dyn MemoryBudget>` clone via this accessor.
    pub fn memory(&self) -> &Arc<dyn MemoryBudget> {
        &self.memory
    }

    /// Per-database spill filesystem, when attached. `None` for
    /// unbounded / test contexts.
    pub fn spill_fs(&self) -> Option<&Arc<SpillFs>> {
        self.spill_fs.as_ref()
    }

    /// Per-query spill identifier, when a [`SpillFs`] is attached.
    pub fn spill_query_id(&self) -> Option<SpillQueryId> {
        self.spill_query_id
    }

    /// Open a fresh spill file under the per-query subdirectory,
    /// tagged with `purpose`. Returns `None` if the context has no
    /// attached `SpillFs` (test contexts).
    ///
    /// Lazily creates the per-query subdirectory on the first call.
    pub fn open_spill(&self, purpose: &str) -> Option<Result<TempSpillFile>> {
        match (&self.spill_fs, self.spill_query_id) {
            (Some(fs), Some(qid)) => Some(fs.open_spill(qid, purpose)),
            _ => None,
        }
    }

    /// Peak `used_bytes()` observed since this context was constructed.
    ///
    /// Returns `None` if the context is backed by [`UnboundedMemory`]
    /// (no peak is tracked). Read once at query teardown for
    /// `ExecutionResult::peak_memory_bytes`.
    pub fn peak_memory_bytes(&self) -> Option<u64> {
        self.tracker.as_ref().map(|t| t.peak_bytes())
    }

    /// Per-query warning sink. Bind helpers clone this into adapters
    /// that need to publish per-entity diagnostics. Cloning is cheap
    /// (an `Arc<Mutex<...>>` clone). See
    /// `docs/design/engine/cancellation.md` §7.
    pub fn warnings(&self) -> &crate::warning_sink::WarningSink {
        &self.warnings
    }

    /// Per-query operator-counter handle. Operators clone the `Arc` at
    /// construction time so a single `snapshot()` at query teardown
    /// captures the query-level totals.
    pub fn metrics(&self) -> &Arc<dyn Metrics> {
        &self.metrics
    }

    /// Set the CPU-cost collection opt-in flag. `false` (the default)
    /// keeps perf-event sampling off entirely; `true` arms the
    /// platform integration (today still a stub — see
    /// [`crate::perf::PerfCounters`]). Returns the modified context
    /// for builder-style chaining.
    pub fn collect_cpu_metrics(mut self, enabled: bool) -> Self {
        self.collect_cpu_metrics = enabled;
        self
    }

    /// Whether CPU-cost sampling was opted into for this query.
    pub fn cpu_metrics_enabled(&self) -> bool {
        self.collect_cpu_metrics
    }

    /// Record one worker's contribution to the per-query metrics
    /// aggregate. The single-threaded driver calls this exactly once
    /// at query teardown; the morsel scheduler will call it per
    /// worker. Calls beyond the first fold the snapshot in via
    /// [`QueryMetrics::record_worker`].
    pub fn record_worker_snapshot(&self, snap: WorkerMetricsSnapshot) {
        let mut guard = self
            .worker_aggregate
            .lock()
            .expect("query worker_aggregate mutex poisoned");
        guard.record_worker(&snap);
    }

    /// Record the per-shard morsel counts and add to `morsels_dispatched`
    /// after a per-shard dispatch returns. The morsel scheduler emits
    /// exactly one morsel per shard in v1; sub-shard halving will
    /// expand this to per-`(shard, generator)` totals.
    pub fn record_morsels_per_shard(&self, counts: &[u64]) {
        let mut guard = self
            .worker_aggregate
            .lock()
            .expect("query worker_aggregate mutex poisoned");
        guard.morsels_dispatched = guard
            .morsels_dispatched
            .saturating_add(counts.iter().sum::<u64>());
        guard.record_morsels_per_shard(counts);
    }

    /// Snapshot the per-query metrics: read the shared operator
    /// counters, fold them into the worker aggregate, stamp the
    /// wall-clock duration and the CPU-metrics flag, and return.
    ///
    /// Non-draining: subsequent calls return identical values modulo
    /// any later `record_*` activity. The engine drops the operator
    /// tree before calling this so no operator clones still publish
    /// into the shared metrics handle.
    pub fn take_query_metrics(&self, wall_clock_ns: u64) -> QueryMetrics {
        let operator_snapshot = self.metrics.snapshot();
        let mut guard = self
            .worker_aggregate
            .lock()
            .expect("query worker_aggregate mutex poisoned");
        guard.operator = operator_snapshot;
        guard.wall_clock_ns = wall_clock_ns;
        guard.cpu_metrics_enabled = self.collect_cpu_metrics;
        *guard
    }
}

impl Default for QueryContext {
    fn default() -> Self {
        Self::unbounded()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Validation helper
// ─────────────────────────────────────────────────────────────────────────────

/// Resolve the effective per-query budget from an `EngineConfig` plus
/// optional `QueryOptions` override. Rejects overrides below
/// [`MIN_QUERY_BUDGET_BYTES`] per design doc § 8.2.
pub(crate) fn resolve_query_budget(config: &EngineConfig, options: &QueryOptions) -> Result<u64> {
    let budget = options
        .memory_budget_bytes
        .unwrap_or(config.query_memory_budget_bytes);
    if budget < MIN_QUERY_BUDGET_BYTES {
        return Err(BqliteError::Execution(format!(
            "query memory budget too small: {budget} bytes \
             (minimum {MIN_QUERY_BUDGET_BYTES} bytes per \
             docs/design/engine/memory-budget.md §8.2)"
        )));
    }
    Ok(budget)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_config_defaults_match_design_doc() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.query_memory_budget_bytes, 3 * 1024 * 1024 * 1024);
        assert_eq!(cfg.compaction_memory_budget_bytes, 800 * 1024 * 1024);
        assert_eq!(cfg.ingest_memory_budget_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn query_options_default_is_empty() {
        let opts = QueryOptions::default();
        assert!(opts.memory_budget_bytes.is_none());
    }

    #[test]
    fn unbounded_context_reports_no_peak() {
        let ctx = QueryContext::unbounded();
        assert!(ctx.peak_memory_bytes().is_none());
    }

    #[test]
    fn tracked_context_reports_peak_after_reservation() {
        let ctx = QueryContext::new(1_000_000);
        // Make a reservation directly against the budget surface so we
        // can verify peak round-trips through the context without
        // needing operator wiring (TASK-512/513/514 land that).
        let r = ctx.memory().try_reserve(2048).unwrap();
        assert_eq!(ctx.memory().used_bytes(), 2048);
        drop(r);
        assert_eq!(ctx.memory().used_bytes(), 0);
        assert_eq!(ctx.peak_memory_bytes(), Some(2048));
    }

    #[test]
    fn tracked_context_overflow_surfaces_typed_error() {
        // Overshoot must surface as the structured variant landed by
        // TASK-511. The helper folds the requested-bytes count into
        // `used` (per `BqliteError::MemoryBudgetExceeded` shape — see
        // `docs/design/engine/cancellation.md` §4.3), so the test
        // observes `used == requested + live_used == 2048 + 0`.
        let ctx = QueryContext::new(1_024);
        let err = ctx
            .memory()
            .try_reserve(2 * 1024)
            .expect_err("must overshoot");
        match err {
            BqliteError::MemoryBudgetExceeded { used, budget } => {
                assert_eq!(budget, 1_024);
                assert_eq!(used, 2 * 1024);
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
    }

    #[test]
    fn resolve_budget_uses_engine_default_when_no_override() {
        let cfg = EngineConfig::default();
        let opts = QueryOptions::default();
        let budget = resolve_query_budget(&cfg, &opts).expect("default must validate");
        assert_eq!(budget, cfg.query_memory_budget_bytes);
    }

    #[test]
    fn resolve_budget_honors_override() {
        let cfg = EngineConfig::default();
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
            ..Default::default()
        };
        let budget = resolve_query_budget(&cfg, &opts).expect("at-floor must validate");
        assert_eq!(budget, MIN_QUERY_BUDGET_BYTES);
    }

    #[test]
    fn resolve_budget_rejects_below_floor() {
        let cfg = EngineConfig::default();
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES - 1),
            ..Default::default()
        };
        let err = resolve_query_budget(&cfg, &opts).expect_err("must reject");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("query memory budget too small"));
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── Per-query metrics surface (TASK-524) ────────────────────────

    #[test]
    fn unbounded_context_has_metrics_handle() {
        let ctx = QueryContext::unbounded();
        // Default `AtomicMetrics` is wired even on the unbounded path.
        let m = ctx.metrics();
        m.record_rows_in(7);
        assert_eq!(m.snapshot().rows_in, 7);
    }

    #[test]
    fn collect_cpu_metrics_flag_round_trips_through_clone() {
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES).collect_cpu_metrics(true);
        assert!(ctx.cpu_metrics_enabled());
        let clone = ctx.clone();
        assert!(clone.cpu_metrics_enabled());
    }

    #[test]
    fn collect_cpu_metrics_default_is_off() {
        let ctx = QueryContext::unbounded();
        assert!(!ctx.cpu_metrics_enabled());
    }

    #[test]
    fn record_worker_snapshot_folds_into_take_query_metrics() {
        let ctx = QueryContext::unbounded();
        ctx.record_worker_snapshot(WorkerMetricsSnapshot {
            worker_busy_ns: 1_000,
            worker_idle_ns: 100,
            morsels_dispatched: 4,
            ..Default::default()
        });
        ctx.record_worker_snapshot(WorkerMetricsSnapshot {
            worker_busy_ns: 2_000,
            worker_idle_ns: 50,
            morsels_dispatched: 6,
            ..Default::default()
        });
        let snap = ctx.take_query_metrics(123_456);
        assert_eq!(snap.num_workers, 2);
        assert_eq!(snap.worker_busy_ns_min, 1_000);
        assert_eq!(snap.worker_busy_ns_max, 2_000);
        assert_eq!(snap.morsels_dispatched, 10);
        assert_eq!(snap.wall_clock_ns, 123_456);
    }

    #[test]
    fn take_query_metrics_is_non_draining() {
        // Calling twice must produce identical values (modulo any
        // intervening `record_*` activity, of which there is none).
        let ctx = QueryContext::unbounded();
        ctx.record_worker_snapshot(WorkerMetricsSnapshot {
            worker_busy_ns: 500,
            ..Default::default()
        });
        let first = ctx.take_query_metrics(1_000);
        let second = ctx.take_query_metrics(1_000);
        assert_eq!(first, second);
    }

    #[test]
    fn take_query_metrics_includes_operator_snapshot() {
        let ctx = QueryContext::unbounded();
        ctx.metrics().record_rows_in(42);
        ctx.metrics().record_spill_bytes_written(1024);
        let snap = ctx.take_query_metrics(0);
        assert_eq!(snap.operator.rows_in, 42);
        assert_eq!(snap.operator.spill_bytes_written, 1024);
    }

    #[test]
    fn take_query_metrics_with_no_workers_yields_zero_aggregate() {
        // DDL / DELETE paths construct a QueryContext but never invoke
        // `record_worker_snapshot`. Calling `take_query_metrics` on
        // that context must produce a sensible all-zero aggregate
        // rather than panicking or leaking uninitialised state.
        let ctx = QueryContext::unbounded();
        let snap = ctx.take_query_metrics(500);
        assert_eq!(snap.num_workers, 0);
        assert_eq!(snap.worker_busy_ns_min, 0);
        assert_eq!(snap.worker_busy_ns_max, 0);
        assert_eq!(snap.wall_clock_ns, 500);
        // Derived metrics return None when num_workers is zero.
        assert!(snap.gb_per_sec_scanned().is_none());
    }

    #[test]
    fn take_query_metrics_stamps_cpu_flag() {
        let ctx = QueryContext::unbounded().collect_cpu_metrics(true);
        let snap = ctx.take_query_metrics(0);
        assert!(snap.cpu_metrics_enabled);
    }

    // ── CancelReason + external cancellation (TASK-538) ────────────

    #[test]
    fn cancel_reason_default_is_none() {
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
        assert_eq!(ctx.cancel_reason(), CancelReason::None);
        assert!(!ctx.cancellation().is_cancelled());
    }

    #[test]
    fn cancel_with_reason_installs_first_fire_only() {
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
        ctx.cancel_with_reason(CancelReason::Timeout);
        assert_eq!(ctx.cancel_reason(), CancelReason::Timeout);
        assert!(ctx.cancellation().is_cancelled());
        // Second fire is silently dropped — Timeout wins because it
        // was first. The cancellation flag stays set across the
        // losing fire (pin this behaviorally so a refactor that
        // moves `self.cancellation.cancel()` inside the CAS Ok arm
        // fails loudly).
        ctx.cancel_with_reason(CancelReason::Cancelled);
        assert_eq!(ctx.cancel_reason(), CancelReason::Timeout);
        assert!(ctx.cancellation().is_cancelled());
    }

    #[test]
    fn cancel_with_reason_propagates_through_clones() {
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
        let clone = ctx.clone();
        ctx.cancel_with_reason(CancelReason::Cancelled);
        assert_eq!(clone.cancel_reason(), CancelReason::Cancelled);
        assert!(clone.cancellation().is_cancelled());
    }

    #[test]
    fn external_cancellation_token_is_observed_by_context() {
        let external = CancellationToken::new();
        let ctx =
            QueryContext::new_with_external_cancellation(MIN_QUERY_BUDGET_BYTES, external.clone());
        assert!(!ctx.cancellation().is_cancelled());
        external.cancel();
        assert!(
            ctx.cancellation().is_cancelled(),
            "cancel on the externally-supplied token must be observed by the context"
        );
        // The external-cancel path does not pre-install a reason; the
        // engine driver records it at the boundary because the
        // user-supplied token has no `cancel_with_reason` API.
        assert_eq!(ctx.cancel_reason(), CancelReason::None);
    }

    #[test]
    fn query_options_default_includes_cancel_and_timeout_as_none() {
        let opts = QueryOptions::default();
        assert!(opts.cancel.is_none());
        assert!(opts.timeout.is_none());
    }

    #[test]
    fn query_options_with_cancel_clones_token() {
        let token = CancellationToken::new();
        let opts = QueryOptions {
            cancel: Some(token.clone()),
            ..QueryOptions::default()
        };
        let cloned = opts.clone();
        // Mutating through one observable handle must reach the other —
        // they are clones of the same `Arc<AtomicBool>`.
        token.cancel();
        assert!(cloned.cancel.as_ref().unwrap().is_cancelled());
    }

    #[test]
    fn cancellation_token_propagates_through_clones() {
        // A clone of QueryContext must observe the cancellation set on
        // the original — the bind step relies on this when threading
        // sub-trees.
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
        let clone = ctx.clone();
        assert!(!ctx.cancellation().is_cancelled());
        assert!(!clone.cancellation().is_cancelled());
        ctx.cancellation().cancel();
        assert!(clone.cancellation().is_cancelled());
    }

    // ── SpillFs plumbing (TASK-513 CP1) ─────────────────────────────

    fn scratch_db_root(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering as O};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, O::Relaxed);
        let pid = std::process::id();
        let mut p = std::env::temp_dir();
        p.push(format!("bqlite-engine-context-{label}-{pid}-{seq}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn unbounded_context_has_no_spill_fs() {
        let ctx = QueryContext::unbounded();
        assert!(ctx.spill_fs().is_none());
        assert!(ctx.spill_query_id().is_none());
        assert!(ctx.open_spill("sort-run").is_none());
    }

    #[test]
    fn with_spill_fs_attaches_query_id_and_handle() {
        let db_root = scratch_db_root("attach");
        let spill_root = db_root.join("spill");
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(fs);
        assert!(ctx.spill_fs().is_some());
        assert!(ctx.spill_query_id().is_some());
        let _ = std::fs::remove_dir_all(&db_root);
    }

    #[test]
    fn open_spill_returns_writer_under_per_query_subdir() {
        let db_root = scratch_db_root("open-spill");
        let spill_root = db_root.join("spill");
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(fs);

        let qid = ctx.spill_query_id().expect("qid attached");
        let guard = ctx
            .open_spill("sort-run")
            .expect("spill_fs attached")
            .expect("open_spill must succeed");
        let expected_dir = spill_root.join(qid.to_string());
        assert_eq!(guard.path().parent(), Some(expected_dir.as_path()));
        assert!(guard
            .path()
            .to_string_lossy()
            .ends_with("sort-run-000000.spill"));
        let _ = std::fs::remove_dir_all(&db_root);
    }

    #[test]
    fn dropping_last_clone_runs_belt_and_braces_cleanup() {
        let db_root = scratch_db_root("cleanup");
        let spill_root = db_root.join("spill");
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let qid = ctx.spill_query_id().unwrap();

        // Open and *forget* a guard to simulate a leaked TempSpillFile
        // (the file system path stays on disk past Drop).
        let guard = ctx.open_spill("sort-run").unwrap().unwrap();
        let leaked_path = guard.path().to_path_buf();
        std::mem::forget(guard);
        assert!(leaked_path.exists());

        // Cloned context: cleanup should NOT fire while a clone lives.
        let clone = ctx.clone();
        drop(ctx);
        assert!(leaked_path.exists(), "cleanup must wait for last clone");
        drop(clone);
        // Last clone dropped — belt-and-braces sweep deletes the
        // per-query subdir, taking the leaked file with it.
        let qdir = spill_root.join(qid.to_string());
        assert!(!qdir.exists(), "per-query subdir must be reclaimed");
        let _ = std::fs::remove_dir_all(&db_root);
    }
}
