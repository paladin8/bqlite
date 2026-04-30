//! Morsel size policy and per-query halving state (design §3.4).
//!
//! Per design §3.4, the morsel-size target adapts to worker idle
//! pressure: an initial high-water target (default 4M rows) that
//! halves whenever the running query's `worker_idle_ns_p99` exceeds
//! the configured threshold at a halving-decision point. Halving is
//! sticky — the target never grows back — so transient idle blips
//! cannot cause oscillation.
//!
//! The v1 degenerate generator does not consult [`MorselSizeState`]
//! (the single whole-shard morsel ignores size targets). The state
//! is wired here so the per-entity-range generator landing in a
//! follow-on task can read it without further plumbing.

use std::sync::atomic::{AtomicU64, Ordering};

/// Tunable defaults driving morsel sizing (design §3.4).
///
/// Default values match the Wave 5 design doc; override only for
/// benchmarking / regression tuning.
#[derive(Debug, Clone, Copy)]
pub struct MorselSizePolicy {
    /// High-water target row count. Default 4_000_000 per design §3.4.
    pub high_target_rows: u64,
    /// Low-water target row count (one row group). Default 65_536.
    pub low_target_rows: u64,
    /// Adaptive halving threshold in nanoseconds — query-wide
    /// `worker_idle_ns_p99` above this triggers halving at the next
    /// decision point. Default 5 ms.
    pub halve_idle_threshold_ns: u64,
    /// Minimum gap, in morsels emitted across all shards, between
    /// consecutive halving decisions. The first decision happens
    /// after the first `halving_warmup_morsels` morsels. Default
    /// `4 × num_workers`; the per-query default substituted by the
    /// scheduler is `4 × query_threads` (resolved when the query
    /// builds its [`MorselSizeState`]).
    pub halving_warmup_morsels: u64,
}

impl MorselSizePolicy {
    /// Default policy per design §3.4. Callers that want to override
    /// individual fields should construct via the struct-update
    /// syntax: `MorselSizePolicy { low_target_rows: 1024, ..MorselSizePolicy::default() }`.
    pub const DEFAULT_HIGH_TARGET_ROWS: u64 = 4_000_000;
    pub const DEFAULT_LOW_TARGET_ROWS: u64 = 65_536;
    pub const DEFAULT_HALVE_IDLE_THRESHOLD_NS: u64 = 5_000_000;
    pub const DEFAULT_HALVING_WARMUP_MORSELS_PER_WORKER: u64 = 4;
}

impl Default for MorselSizePolicy {
    fn default() -> Self {
        Self {
            high_target_rows: Self::DEFAULT_HIGH_TARGET_ROWS,
            low_target_rows: Self::DEFAULT_LOW_TARGET_ROWS,
            halve_idle_threshold_ns: Self::DEFAULT_HALVE_IDLE_THRESHOLD_NS,
            halving_warmup_morsels: Self::DEFAULT_HALVING_WARMUP_MORSELS_PER_WORKER,
        }
    }
}

/// Per-query, runtime sticky-halving state (design §3.4).
///
/// Workers (when the per-entity-range generator lands) read
/// `current_target_rows` at the start of every morsel emission. The
/// coordinator's drain pump is the only writer, calling
/// [`MorselSizeState::halve`] when `worker_idle_ns_p99` exceeds the
/// policy threshold at a halving decision point.
#[derive(Debug)]
pub struct MorselSizeState {
    policy: MorselSizePolicy,
    current_target_rows: AtomicU64,
    morsels_emitted_total: AtomicU64,
}

impl MorselSizeState {
    /// Construct fresh state for one query. The initial target is
    /// `policy.high_target_rows`.
    pub fn new(policy: MorselSizePolicy) -> Self {
        Self {
            current_target_rows: AtomicU64::new(policy.high_target_rows),
            morsels_emitted_total: AtomicU64::new(0),
            policy,
        }
    }

    /// Snapshot the policy this state was built with.
    pub fn policy(&self) -> &MorselSizePolicy {
        &self.policy
    }

    /// Current target row count. Workers (per entity-range generator)
    /// read this at the start of every morsel emission.
    pub fn current_target_rows(&self) -> u64 {
        self.current_target_rows.load(Ordering::Acquire)
    }

    /// Total morsels emitted across all shards for this query.
    pub fn morsels_emitted_total(&self) -> u64 {
        self.morsels_emitted_total.load(Ordering::Acquire)
    }

    /// Increment the emitted-morsel counter by 1. Returns the new
    /// total. Called by the morsel generator each time it produces a
    /// morsel.
    pub fn record_morsel_emitted(&self) -> u64 {
        self.morsels_emitted_total.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Halve the current target. Sticky: the target never grows back
    /// and never falls below `policy.low_target_rows`. Returns the
    /// new target.
    ///
    /// The halving control loop (per design §3.4) is the
    /// coordinator's responsibility — it decides *when* to call this
    /// based on `worker_idle_ns_p99`; this method only enforces the
    /// "monotonic non-increasing, floor at `low_target_rows`"
    /// invariant.
    pub fn halve(&self) -> u64 {
        let mut current = self.current_target_rows.load(Ordering::Acquire);
        loop {
            // Already at floor — halving is a no-op.
            if current <= self.policy.low_target_rows {
                return current;
            }
            let halved = (current / 2).max(self.policy.low_target_rows);
            match self.current_target_rows.compare_exchange_weak(
                current,
                halved,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return halved,
                Err(observed) => current = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_matches_design_doc() {
        let p = MorselSizePolicy::default();
        assert_eq!(p.high_target_rows, 4_000_000);
        assert_eq!(p.low_target_rows, 65_536);
        assert_eq!(p.halve_idle_threshold_ns, 5_000_000);
    }

    #[test]
    fn state_starts_at_high_target() {
        let s = MorselSizeState::new(MorselSizePolicy::default());
        assert_eq!(s.current_target_rows(), 4_000_000);
        assert_eq!(s.morsels_emitted_total(), 0);
    }

    #[test]
    fn halving_is_sticky_and_floors_at_low_target() {
        let policy = MorselSizePolicy {
            high_target_rows: 4_000_000,
            low_target_rows: 100_000,
            ..MorselSizePolicy::default()
        };
        let s = MorselSizeState::new(policy);
        assert_eq!(s.halve(), 2_000_000);
        assert_eq!(s.halve(), 1_000_000);
        assert_eq!(s.halve(), 500_000);
        assert_eq!(s.halve(), 250_000);
        assert_eq!(s.halve(), 125_000);
        assert_eq!(s.halve(), 100_000); // capped at low_target
        assert_eq!(s.halve(), 100_000); // sticky floor
        assert_eq!(s.halve(), 100_000);
    }

    #[test]
    fn halving_never_grows_back() {
        let s = MorselSizeState::new(MorselSizePolicy::default());
        s.halve();
        let after_halve = s.current_target_rows();
        // No public method grows the target. We assert that no
        // sequence of `halve` calls ever crosses the original target.
        for _ in 0..5 {
            s.halve();
        }
        assert!(s.current_target_rows() <= after_halve);
    }

    #[test]
    fn record_morsel_emitted_increments() {
        let s = MorselSizeState::new(MorselSizePolicy::default());
        assert_eq!(s.record_morsel_emitted(), 1);
        assert_eq!(s.record_morsel_emitted(), 2);
        assert_eq!(s.morsels_emitted_total(), 2);
    }

    #[test]
    fn halving_is_safe_under_concurrent_callers() {
        // Many threads call halve() simultaneously; the state should
        // converge to floor without panic / lost write.
        let policy = MorselSizePolicy {
            high_target_rows: 1 << 20,
            low_target_rows: 1,
            ..MorselSizePolicy::default()
        };
        let s = std::sync::Arc::new(MorselSizeState::new(policy));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let s = std::sync::Arc::clone(&s);
            handles.push(std::thread::spawn(move || {
                for _ in 0..32 {
                    s.halve();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(s.current_target_rows(), 1);
    }
}
