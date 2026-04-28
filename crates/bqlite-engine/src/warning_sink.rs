//! Per-query warning sink shared across operators.
//!
//! The [`WarningSink`] wraps a [`WorkerContext`] behind an
//! `Arc<Mutex<...>>` so the single-threaded driver and (eventually)
//! parallel workers can share the same per-query warning buffer. The
//! mutex is on the cold path — operators only acquire it after
//! `finish_entity`, never inside the per-event loop. See
//! `docs/design/engine/cancellation.md` §7.2 / §7.3 for the full
//! protocol.
//!
//! **Single-threaded today, multi-worker tomorrow.** TASK-541 (morsel
//! scheduler) will spawn parallel workers; the same `WarningSink`
//! handle is cloned into each. Until then there is exactly one writer
//! at a time and the mutex is uncontended.

use std::sync::{Arc, Mutex};

use bqlite_core::QueryWarning;

/// Per-worker warning slot. Owns the bounded `Vec<QueryWarning>` and
/// the suppressed-warning counter.
///
/// See `docs/design/engine/cancellation.md` §7.2.
#[derive(Debug, Default)]
pub struct WorkerContext {
    pub warnings: Vec<QueryWarning>,
    pub warning_overflow: u64,
}

impl WorkerContext {
    /// Per-`cancellation.md` §7.2: each worker silently drops further
    /// warnings once the cap is reached, incrementing the overflow
    /// counter so the coordinator can surface a final
    /// `WarningsOverflow` aggregating across workers.
    pub const PER_WORKER_WARNING_CAP: usize = 1_000;

    /// Push a warning, respecting the per-worker cap.
    pub fn record_warning(&mut self, warning: QueryWarning) {
        if self.warnings.len() < Self::PER_WORKER_WARNING_CAP {
            self.warnings.push(warning);
        } else {
            self.warning_overflow = self.warning_overflow.saturating_add(1);
        }
    }
}

/// Shared handle to a [`WorkerContext`]. Cloneable; clones share the
/// same underlying buffer.
///
/// `EntityOperator` implementors do **not** see this type — they
/// emit `Vec<QueryWarning>` from `take_pending_warnings`, and the
/// engine-side adapter forwards each warning into the sink.
#[derive(Debug, Clone, Default)]
pub struct WarningSink {
    inner: Arc<Mutex<WorkerContext>>,
}

impl WarningSink {
    /// Construct an empty sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a single warning. Acquires the mutex.
    pub fn record(&self, warning: QueryWarning) {
        // Mutex poisoning would mean a worker panicked while holding
        // the lock. Recover the inner value and continue — panic
        // propagation flows through `OperatorPanic`, not through the
        // warning sink.
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        guard.record_warning(warning);
    }

    /// Record many warnings in one mutex acquisition. Used by the
    /// adapter forwarding path so an entity that produced multiple
    /// warnings does not lock-unlock per warning.
    pub fn record_many<I: IntoIterator<Item = QueryWarning>>(&self, iter: I) {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        for w in iter {
            guard.record_warning(w);
        }
    }

    /// Drain the assembled warning list per `cancellation.md` §7.3:
    /// concatenated buffer in record order, with a final
    /// `QueryWarning::WarningsOverflow` appended when any warnings
    /// were suppressed.
    ///
    /// Calling this on a clone is safe — the inner buffer is moved
    /// out via `mem::take`, so subsequent observers see an empty
    /// state and return `Vec::new()`.
    pub fn into_warnings(self) -> Vec<QueryWarning> {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let WorkerContext {
            mut warnings,
            warning_overflow,
        } = std::mem::take(&mut *guard);
        if warning_overflow > 0 {
            warnings.push(QueryWarning::WarningsOverflow {
                suppressed_count: warning_overflow,
            });
        }
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_under_cap_is_collected() {
        let sink = WarningSink::new();
        sink.record(QueryWarning::EntityEventLimitExceeded {
            entity_id: "u1".into(),
            count: 1,
            limit: 1,
        });
        let out = sink.into_warnings();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn record_over_cap_emits_single_overflow_at_end() {
        let sink = WarningSink::new();
        for i in 0..(WorkerContext::PER_WORKER_WARNING_CAP + 5) {
            sink.record(QueryWarning::EntityEventLimitExceeded {
                entity_id: format!("e{i}"),
                count: 1,
                limit: 1,
            });
        }
        let out = sink.into_warnings();
        assert_eq!(out.len(), WorkerContext::PER_WORKER_WARNING_CAP + 1);
        match out.last().unwrap() {
            QueryWarning::WarningsOverflow { suppressed_count } => {
                assert_eq!(*suppressed_count, 5);
            }
            other => panic!("expected WarningsOverflow at end, got {other:?}"),
        }
    }

    #[test]
    fn into_warnings_empty_when_nothing_recorded() {
        let sink = WarningSink::new();
        assert!(sink.into_warnings().is_empty());
    }

    #[test]
    fn clones_share_buffer() {
        let sink = WarningSink::new();
        let clone = sink.clone();
        sink.record(QueryWarning::EntityEventLimitExceeded {
            entity_id: "a".into(),
            count: 1,
            limit: 1,
        });
        clone.record(QueryWarning::EntityEventLimitExceeded {
            entity_id: "b".into(),
            count: 1,
            limit: 1,
        });
        let out = sink.into_warnings();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn record_many_acquires_lock_once() {
        let sink = WarningSink::new();
        sink.record_many([
            QueryWarning::SessionEventCapExceeded {
                entity_id: "x".into(),
                event_count: 100,
                cap: 99,
            },
            QueryWarning::AttributeTouchpointCapExceeded {
                entity_id: "x".into(),
                touchpoint_count: 5,
                cap: 4,
            },
        ]);
        let out = sink.into_warnings();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn second_drain_returns_empty() {
        let sink = WarningSink::new();
        let clone = sink.clone();
        sink.record(QueryWarning::ActiveStateLimitExceeded {
            entity_id: "u".into(),
            active_states: 1,
            cap: 1,
        });
        let first = sink.into_warnings();
        assert_eq!(first.len(), 1);
        let second = clone.into_warnings();
        assert!(second.is_empty());
    }
}
