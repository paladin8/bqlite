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

use std::sync::Arc;

use bqlite_core::spill::{SpillFs, SpillQueryId, TempSpillFile};
use bqlite_core::{memory::MemoryTracker, BqliteError, MemoryBudget, Result, UnboundedMemory};
use bqlite_operators::CancellationToken;

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
/// `Default` produces an empty option set (all `None`), so existing
/// callers that pass nothing still get the engine-level defaults.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryOptions {
    /// Override the per-query memory budget. Validated against
    /// [`MIN_QUERY_BUDGET_BYTES`] at submission time.
    pub memory_budget_bytes: Option<u64>,
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
/// When the *last* clone drops, the inner [`SpillCleanup`] guard runs
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
        };
        let budget = resolve_query_budget(&cfg, &opts).expect("at-floor must validate");
        assert_eq!(budget, MIN_QUERY_BUDGET_BYTES);
    }

    #[test]
    fn resolve_budget_rejects_below_floor() {
        let cfg = EngineConfig::default();
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES - 1),
        };
        let err = resolve_query_budget(&cfg, &opts).expect_err("must reject");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("query memory budget too small"));
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
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
