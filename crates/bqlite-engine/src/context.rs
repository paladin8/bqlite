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
/// All fields are bytes. Defaults match § 2.2 of the design doc. The
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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            query_memory_budget_bytes: DEFAULT_QUERY_BUDGET_BYTES,
            compaction_memory_budget_bytes: DEFAULT_COMPACTION_BUDGET_BYTES,
            ingest_memory_budget_bytes: DEFAULT_INGEST_BUDGET_BYTES,
        }
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
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryContext")
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("memory_used_bytes", &self.memory.used_bytes())
            .field("memory_budget_bytes", &self.memory.budget_bytes())
            .field("has_tracker", &self.tracker.is_some())
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
        }
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
}
