//! Size-tiered compaction (TASK-408).
//!
//! Implements `docs/design/storage/compaction-concurrency.md` §§2–9 for
//! a single `(table, window, shard)` job at a time.
//!
//! # Layering (CP1 surface)
//!
//! - [`CompactionConfig`] — user-facing thresholds and pool sizing.
//! - [`CoreBudget`] — the §4 semaphore. Compaction acquires one permit
//!   per job; queries (TASK-438, future) will acquire `worker_count`
//!   permits on start. Until TASK-438 lands, the budget is uncontested
//!   and the acquire/release pair is a cheap no-op.
//! - [`CompactionMetrics`] — observable backlog, exposed per
//!   compaction-concurrency.md §5 ("Observability requirement").
//!
//! # What this module deliberately does NOT do
//!
//! - It does not consult `tombstones.json` — TASK-434 / TASK-435 own
//!   the tombstone-aware filtering and reclamation extension.
//! - It does not run a 10-second `Arc::strong_count` reclamation sweep
//!   — superseded segment files are deleted immediately because today's
//!   `Database` does not hand out `Arc<Manifest>` snapshots; see the
//!   design doc's §12 implementation status.
//!
//! Later checkpoints (CP2–CP5) layer on the executor (`compact_one`),
//! the synchronous `Database::compact_now` API, and the background
//! scheduler. CP1 is intentionally limited to the configuration,
//! semaphore, and metric surfaces so they can be reused without
//! pulling in the executor's dependency graph.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

// ── Configuration ───────────────────────────────────────────────────────────

/// User-facing tunables for the compaction subsystem.
///
/// All fields have production-sensible defaults via
/// [`CompactionConfig::default`]; tests override individual fields with
/// the struct-update syntax. Defaults match
/// `docs/design/storage/compaction-concurrency.md` §3.1, §3.2, and
/// §8.3.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// L0 segment count above which a `(window, shard)` becomes
    /// eligible. Matches compaction-concurrency.md §3.2 default.
    pub l0_count_trigger: u32,
    /// L0 total byte size above which a `(window, shard)` becomes
    /// eligible. Matches compaction-concurrency.md §3.2 default
    /// (256 MiB).
    pub l0_size_trigger_bytes: u64,
    /// Background scheduler pool size. Default `max(1, num_cores / 4)`
    /// per §3.1.
    pub pool_size: usize,
    /// Total core-budget permits. Default `num_cores` per §4.1.
    pub core_budget_permits: usize,
    /// Cooldown after a failed job before the same `(window, shard)`
    /// becomes eligible to retry. Matches §8.3 (60 s).
    pub retry_cooldown: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self {
            l0_count_trigger: 4,
            l0_size_trigger_bytes: 256 * 1024 * 1024,
            pool_size: (cores / 4).max(1),
            core_budget_permits: cores.max(1),
            retry_cooldown: Duration::from_secs(60),
        }
    }
}

// ── Core-budget semaphore ───────────────────────────────────────────────────

/// Counting semaphore from compaction-concurrency.md §4.
///
/// Compaction acquires permits before starting work; queries (when
/// TASK-438 lands) will acquire `worker_count` permits at start and
/// release on finalization. Built on `Mutex` + `Condvar` so we don't
/// take a new dependency.
///
/// The §4 design calls for the compaction worker to acquire one permit
/// per row-group boundary. The CP1 surface ships the type; the v1
/// executor in CP3 acquires one permit per job because it materialises
/// the whole merge in one pass (see §12.1 in the design doc for the
/// streaming follow-on that hoists the acquire/release back inside the
/// row-group loop).
#[derive(Debug)]
pub struct CoreBudget {
    state: Mutex<usize>,
    cv: Condvar,
}

/// RAII guard for one acquired permit. Releasing happens on drop.
#[derive(Debug)]
pub struct CoreBudgetPermit<'a> {
    budget: &'a CoreBudget,
}

impl CoreBudget {
    /// Construct a budget pre-loaded with `permits` and return it
    /// behind an `Arc` so it can be shared across the scheduler's
    /// worker threads.
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(permits),
            cv: Condvar::new(),
        })
    }

    /// Acquire one permit, blocking until one is available.
    pub fn acquire(&self) -> CoreBudgetPermit<'_> {
        let mut g = self.state.lock().expect("CoreBudget mutex poisoned");
        while *g == 0 {
            g = self.cv.wait(g).expect("CoreBudget condvar poisoned");
        }
        *g -= 1;
        CoreBudgetPermit { budget: self }
    }

    /// Currently available permits. Test/observability helper; the
    /// hot path acquires permits via [`Self::acquire`].
    pub fn available(&self) -> usize {
        *self.state.lock().expect("CoreBudget mutex poisoned")
    }
}

impl Drop for CoreBudgetPermit<'_> {
    fn drop(&mut self) {
        let mut g = self.budget.state.lock().expect("CoreBudget mutex poisoned");
        *g += 1;
        self.budget.cv.notify_one();
    }
}

// ── Metrics ─────────────────────────────────────────────────────────────────

/// Observable counters the operator can read at any time.
///
/// Surfaced per compaction-concurrency.md §5 ("Observability
/// requirement"). Backed by a `Mutex<HashMap>` because the
/// per-`(table, window, shard)` backlog set is sparse and small; an
/// atomic-per-key map would over-engineer a surface no hot path
/// consults.
#[derive(Debug, Default)]
pub struct CompactionMetrics {
    inner: Mutex<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    /// Per-`(table, window_id, shard_id)` L0 segment count, refreshed
    /// on every scheduler eligibility evaluation pass.
    backlog: HashMap<(String, u32, u32), u64>,
}

impl CompactionMetrics {
    /// Construct a fresh, empty metrics handle wrapped in an `Arc` so
    /// the scheduler and external observers can share it.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Replace the per-key L0 count for one bucket. Used by the
    /// scheduler's eligibility pass. A count of zero removes the
    /// entry so the snapshot stays compact.
    pub fn set_backlog(&self, table: &str, window_id: u32, shard_id: u32, l0_count: u64) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let key = (table.to_string(), window_id, shard_id);
        if l0_count == 0 {
            inner.backlog.remove(&key);
        } else {
            inner.backlog.insert(key, l0_count);
        }
    }

    /// Snapshot of every non-zero bucket, ordered arbitrarily.
    /// Allocates; intended for metrics scrape paths and tests, not the
    /// hot path.
    pub fn backlog_snapshot(&self) -> Vec<(String, u32, u32, u64)> {
        let inner = self.inner.lock().expect("metrics mutex poisoned");
        inner
            .backlog
            .iter()
            .map(|((t, w, s), c)| (t.clone(), *w, *s, *c))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_thresholds_match_design_doc() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.l0_count_trigger, 4);
        assert_eq!(cfg.l0_size_trigger_bytes, 256 * 1024 * 1024);
        assert!(cfg.pool_size >= 1);
        assert!(cfg.core_budget_permits >= 1);
        assert_eq!(cfg.retry_cooldown, Duration::from_secs(60));
    }

    #[test]
    fn core_budget_acquire_release_round_trip() {
        let budget = CoreBudget::new(2);
        assert_eq!(budget.available(), 2);
        let p1 = budget.acquire();
        assert_eq!(budget.available(), 1);
        let p2 = budget.acquire();
        assert_eq!(budget.available(), 0);
        drop(p1);
        assert_eq!(budget.available(), 1);
        drop(p2);
        assert_eq!(budget.available(), 2);
    }

    #[test]
    fn core_budget_blocks_until_permit_available() {
        let budget = CoreBudget::new(1);
        let p1 = budget.acquire();
        let b2 = budget.clone();
        let handle = std::thread::spawn(move || {
            let _p = b2.acquire();
            // Permit acquired; thread exits, releasing it.
        });
        // Give the spawned thread time to actually block on the cv.
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(budget.available(), 0);
        drop(p1);
        handle.join().expect("acquirer thread panicked");
        assert_eq!(budget.available(), 1);
    }

    #[test]
    fn metrics_set_and_snapshot_round_trip() {
        let m = CompactionMetrics::new();
        m.set_backlog("events", 0, 0, 5);
        m.set_backlog("events", 0, 1, 7);
        m.set_backlog("events", 1, 0, 0); // zero -> never inserted
        let mut snap = m.backlog_snapshot();
        snap.sort();
        assert_eq!(
            snap,
            vec![
                ("events".to_string(), 0, 0, 5),
                ("events".to_string(), 0, 1, 7),
            ]
        );
        // Setting an existing entry to zero removes it.
        m.set_backlog("events", 0, 0, 0);
        let snap = m.backlog_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0], ("events".to_string(), 0, 1, 7));
    }
}
