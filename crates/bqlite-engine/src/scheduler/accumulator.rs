//! Per-shard accumulator handle (design §6.2 / §4.3).
//!
//! Each query maintains one [`AccumulatorHandle`] per shard it
//! touches. Workers running morsels for shard *S* lock the handle's
//! inner [`Mutex<Box<dyn Accumulator>>`] at fused-entity-operator
//! `finish_entity_into` boundaries (per design §6.3). The drop hook
//! on [`super::worker::WorkerMorselGuard`] decrements
//! [`AccumulatorHandle::outstanding_morsels`]; once `outstanding == 0`
//! and `total_emitted` is set by the generator, the coordinator
//! observes the shard as ready for the cross-shard merge (§6.4).
//!
//! Per design §6.2, the per-shard mutex grain is the right tradeoff:
//! contention is per-`finish_entity` (microsecond-scale hold time),
//! and one mutex per shard bounds memory at `num_shards` locks
//! regardless of group cardinality.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use bqlite_operators::aggregate::Accumulator;

use super::morsel::ShardId;

/// Why the shard transitioned to "done" — used by tests and the
/// coordinator's diagnostic logs to disambiguate the two write
/// orderings (last morsel finishing before generator marks
/// `total_emitted`, vs. the other way around).
///
/// Per design §4.3, the shard-done condition is conjunctive
/// (`outstanding_morsels == 0 && total_emitted.is_set()`); this enum
/// records which side of the conjunction was *last* to flip and so
/// tripped the signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardDoneSignal {
    /// The generator's `mark_total_emitted` was the last write.
    GeneratorClosed,
    /// The last in-flight morsel's guard was the last write.
    LastGuardDropped,
}

/// Per-shard accumulator handle.
///
/// One instance per shard per query, owned by the query coordinator
/// (held inside an `Arc` so workers can borrow against the
/// coordinator's lifetime). `Debug` is not derived because
/// `dyn Accumulator` is not `Debug` — the [`std::fmt::Debug`] impl
/// below renders the bookkeeping fields and elides the inner.
pub struct AccumulatorHandle {
    shard_id: ShardId,
    /// Per-shard accumulator. `None` for shards that are still being
    /// initialised before the first morsel runs, or after the
    /// coordinator takes the accumulator out at finalize-time.
    inner: Mutex<Option<Box<dyn Accumulator>>>,
    /// Number of morsels still in flight for this shard.
    outstanding_morsels: AtomicU64,
    /// Total morsels emitted by this shard's generator. Set when the
    /// generator drains; the shard is "done" when
    /// `outstanding_morsels == 0 && total_emitted` is set.
    total_emitted: OnceLock<u64>,
    /// Records whether the shard has signaled "done" already, and
    /// which side of the conjunction tripped the signal. Set
    /// exactly once.
    done_signal: OnceLock<ShardDoneSignal>,
}

impl std::fmt::Debug for AccumulatorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccumulatorHandle")
            .field("shard_id", &self.shard_id)
            .field("outstanding_morsels", &self.outstanding_morsels())
            .field("total_emitted", &self.total_emitted())
            .field("done_signal", &self.done_signal())
            .finish_non_exhaustive()
    }
}

impl AccumulatorHandle {
    /// Construct a handle for one shard, optionally seeded with a
    /// fresh accumulator. Pass `None` for selection queries that
    /// have no aggregate to merge.
    pub fn new(shard_id: ShardId, accumulator: Option<Box<dyn Accumulator>>) -> Self {
        Self {
            shard_id,
            inner: Mutex::new(accumulator),
            outstanding_morsels: AtomicU64::new(0),
            total_emitted: OnceLock::new(),
            done_signal: OnceLock::new(),
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Lock the per-shard accumulator. Workers hold the mutex only at
    /// `finish_entity_into` boundaries — design §6.2 / §6.3.
    ///
    /// # Panics
    ///
    /// Panics on `PoisonError` — per design §9.2, a poisoned mutex
    /// means an upstream worker panicked while holding the lock and
    /// the query is unrecoverable. Callers that want to surface
    /// `BqliteError::OperatorPanic` instead should call
    /// [`Self::lock_or_poisoned`].
    pub fn lock(&self) -> std::sync::MutexGuard<'_, Option<Box<dyn Accumulator>>> {
        self.inner.lock().expect("accumulator mutex poisoned")
    }

    /// Lock the per-shard accumulator, surfacing the poisoned-error
    /// case to the caller. Used by the worker pool's morsel runner
    /// to convert poison into a typed `OperatorPanic` per design §9.2.
    pub fn lock_or_poisoned(
        &self,
    ) -> std::sync::LockResult<std::sync::MutexGuard<'_, Option<Box<dyn Accumulator>>>> {
        self.inner.lock()
    }

    /// Increment `outstanding_morsels` to mark a morsel for this
    /// shard as in flight. Called when the worker pulls a morsel for
    /// this shard.
    pub fn record_morsel_pulled(&self) -> u64 {
        self.outstanding_morsels.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Decrement `outstanding_morsels` and check whether the shard
    /// has just transitioned to "done". Returns `Some(signal)`
    /// exactly once (for the write that flipped the conjunction);
    /// returns `None` on every subsequent call. Called by the morsel
    /// guard's `Drop`.
    pub fn record_morsel_finished(&self) -> Option<ShardDoneSignal> {
        let prev = self.outstanding_morsels.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "record_morsel_finished underflow");
        // Conjunctive shard-done check; either ordering of the
        // generator's `mark_total_emitted` and the last guard's
        // decrement works.
        if prev == 1 && self.total_emitted.get().is_some() {
            self.done_signal
                .set(ShardDoneSignal::LastGuardDropped)
                .ok()?;
            return Some(ShardDoneSignal::LastGuardDropped);
        }
        None
    }

    /// Generator marks the total morsel count for this shard. May
    /// also flip the conjunction to "done" if every morsel already
    /// finished before the generator drained.
    pub fn mark_total_emitted(&self, total: u64) -> Option<ShardDoneSignal> {
        let _ = self.total_emitted.set(total);
        if self.outstanding_morsels.load(Ordering::Acquire) == 0 {
            self.done_signal
                .set(ShardDoneSignal::GeneratorClosed)
                .ok()?;
            return Some(ShardDoneSignal::GeneratorClosed);
        }
        None
    }

    /// True iff the shard has already signaled done.
    pub fn is_done(&self) -> bool {
        self.done_signal.get().is_some()
    }

    /// Inspect the done signal, if set. Returns `None` while the
    /// shard is still active.
    pub fn done_signal(&self) -> Option<ShardDoneSignal> {
        self.done_signal.get().copied()
    }

    /// Take ownership of the inner accumulator for the cross-shard
    /// merge step (design §6.4). Caller must guarantee the shard has
    /// signaled done before calling — otherwise a worker may still
    /// be holding the lock.
    pub fn take_accumulator(&self) -> Option<Box<dyn Accumulator>> {
        self.inner
            .lock()
            .expect("accumulator mutex poisoned")
            .take()
    }

    /// Outstanding morsel count. Test/observability helper.
    pub fn outstanding_morsels(&self) -> u64 {
        self.outstanding_morsels.load(Ordering::Acquire)
    }

    /// Total morsels emitted, if the generator has drained.
    pub fn total_emitted(&self) -> Option<u64> {
        self.total_emitted.get().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_done_via_last_guard_drop() {
        // Generator publishes total before any morsels pulled, then
        // workers run; the *last* guard drop fires the signal.
        let h = AccumulatorHandle::new(0, None);
        h.record_morsel_pulled();
        h.record_morsel_pulled();
        // Generator sees that there's still outstanding work — no
        // signal at this write.
        assert!(h.mark_total_emitted(2).is_none());
        // First guard drop: outstanding goes from 2 → 1, no signal.
        assert!(h.record_morsel_finished().is_none());
        // Second guard drop: outstanding 1 → 0 with total set → signal.
        assert_eq!(
            h.record_morsel_finished(),
            Some(ShardDoneSignal::LastGuardDropped)
        );
        assert!(h.is_done());
    }

    #[test]
    fn shard_done_via_generator_close_when_all_morsels_already_finished() {
        // All morsels complete before the generator marks total —
        // the generator's write is the trigger.
        let h = AccumulatorHandle::new(1, None);
        h.record_morsel_pulled();
        // Worker finishes; outstanding back to 0 but total not set.
        assert!(h.record_morsel_finished().is_none());
        assert!(!h.is_done(), "no total_emitted yet");
        // Generator drains; signal fires now.
        assert_eq!(
            h.mark_total_emitted(1),
            Some(ShardDoneSignal::GeneratorClosed)
        );
        assert!(h.is_done());
    }

    #[test]
    fn empty_shard_signals_done_at_generator_close() {
        let h = AccumulatorHandle::new(2, None);
        // Empty shard — outstanding is already 0.
        assert_eq!(
            h.mark_total_emitted(0),
            Some(ShardDoneSignal::GeneratorClosed)
        );
        assert!(h.is_done());
    }

    #[test]
    fn done_signal_fires_exactly_once() {
        // Both write orderings exercised above; this test mixes them
        // and asserts the second potential firing is suppressed.
        let h = AccumulatorHandle::new(3, None);
        h.record_morsel_pulled();
        // Generator first.
        assert!(h.mark_total_emitted(1).is_none());
        // Last guard fires the signal.
        assert_eq!(
            h.record_morsel_finished(),
            Some(ShardDoneSignal::LastGuardDropped)
        );
        // A redundant `mark_total_emitted` returns None (`OnceLock::set`
        // would fail anyway).
        assert!(h.mark_total_emitted(1).is_none());
    }

    #[test]
    fn outstanding_underflow_is_caught_in_debug() {
        // record_morsel_finished without a paired pull underflows —
        // debug_assert! catches it. In release this would wrap, so
        // we only assert in debug builds.
        if cfg!(debug_assertions) {
            let h = AccumulatorHandle::new(4, None);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                h.record_morsel_finished();
            }));
            assert!(result.is_err(), "underflow must panic in debug");
        }
    }

    #[test]
    fn take_accumulator_yields_inner() {
        // None => take returns None.
        let h = AccumulatorHandle::new(5, None);
        assert!(h.take_accumulator().is_none());
    }
}
