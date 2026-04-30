//! Per-query lock-free MPMC morsel queue (design §4.1).
//!
//! One [`MorselQueue`] per query, shared across every shard's
//! generator (producer side, fan-in) and every worker pulling from
//! the engine's worker pool (consumer side, fan-out). The queue is
//! built on `crossbeam_queue::ArrayQueue` (a lock-free fixed-capacity
//! MPMC structure) plus a pair of `Condvar` for producer/consumer
//! wakeups.
//!
//! Per design §3.5 the queue's capacity is `2 × num_workers`; the
//! constructor exposes the capacity as an explicit parameter so the
//! engine can pick it from `query_threads` at submit time.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;

use super::morsel::Morsel;

/// Returned by [`MorselQueue::push`] when the queue is at capacity.
/// The producer holds the rejected morsel locally and retries after
/// being woken on `pop_notify`.
#[derive(Debug)]
pub struct MorselQueueFull(pub Morsel);

/// Returned by [`MorselQueue::pop_or_park`] when every per-shard
/// generator for the query has drained and the queue is empty —
/// workers using this signal stop pulling from the query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselQueueDrained;

/// Lock-free MPMC morsel queue with producer/consumer wake notifications.
///
/// Producers (the per-query coordinator drain pump) call
/// [`push`](Self::push) and on `Err(Full)` park on `pop_notify` until
/// a worker pop frees capacity. Consumers (workers) call
/// [`pop`](Self::pop) (non-blocking) or [`pop_or_park`](Self::pop_or_park)
/// (blocks until a morsel is available or the queue drains).
///
/// Design §4.1 specifies single-producer (the coordinator drain
/// pump) and multiple-consumer (the engine's worker pool); the
/// underlying `ArrayQueue` is MPMC, so concurrent producers would be
/// safe — the contract just isn't exercised by v1.
#[derive(Debug)]
pub struct MorselQueue {
    inner: ArrayQueue<Morsel>,
    /// Set when every per-shard generator has drained. Workers that
    /// observe `inner.is_empty() && all_generators_drained` stop
    /// pulling and return `Drained`.
    all_generators_drained: AtomicBool,
    /// Lock guarding the condvars. Held only across short
    /// `cv.wait_timeout` calls; never held while doing real work.
    park: Mutex<()>,
    /// Coordinator-side wake. Workers signal after every successful
    /// `pop()` so a parked drain-pump producer can retry a previously
    /// rejected push.
    pop_notify: Condvar,
    /// Worker-side wake. The coordinator signals after every
    /// successful `push()` and when `all_generators_drained` flips,
    /// so any parked worker can re-check.
    push_notify: Condvar,
}

impl MorselQueue {
    /// Construct a queue with the given fixed capacity. Per design
    /// §3.5, callers pass `2 × num_workers`; this constructor does
    /// not impose that policy so test fixtures can use small caps.
    ///
    /// # Panics
    ///
    /// Panics if `capacity == 0` — `ArrayQueue::new` itself panics on
    /// zero, and the panic is caller error.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: ArrayQueue::new(capacity),
            all_generators_drained: AtomicBool::new(false),
            park: Mutex::new(()),
            pop_notify: Condvar::new(),
            push_notify: Condvar::new(),
        }
    }

    /// Capacity the queue was constructed with. The fixed capacity
    /// of the underlying `ArrayQueue` does not change at runtime.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Current depth (number of morsels in flight). Test helper.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// True iff every per-shard generator has drained and the queue
    /// is empty. Workers that observe this stop pulling. Coordinator
    /// signals draining via [`mark_drained`](Self::mark_drained).
    pub fn is_drained(&self) -> bool {
        self.all_generators_drained.load(Ordering::Acquire) && self.inner.is_empty()
    }

    /// Coordinator marks every per-shard generator as drained. Wakes
    /// every parked worker so they observe the terminal state.
    pub fn mark_drained(&self) {
        self.all_generators_drained.store(true, Ordering::Release);
        self.push_notify.notify_all();
    }

    /// Producer push. Returns the rejected morsel back to the caller
    /// when the queue is at capacity. On success, signals every
    /// parked worker.
    pub fn push(&self, morsel: Morsel) -> Result<(), MorselQueueFull> {
        match self.inner.push(morsel) {
            Ok(()) => {
                self.push_notify.notify_one();
                Ok(())
            }
            Err(rejected) => Err(MorselQueueFull(rejected)),
        }
    }

    /// Producer park-and-retry helper. Used by the drain pump after
    /// a `push` returned `Full` — sleeps until a consumer pops, with
    /// a fixed timeout so cancellation can break the wait.
    pub fn park_for_push(&self, timeout: Duration) {
        let g = self.park.lock().expect("MorselQueue park mutex poisoned");
        let _ = self.pop_notify.wait_timeout(g, timeout);
    }

    /// Non-blocking consumer pop. Returns `None` when the queue is
    /// empty (regardless of drain state). On success, signals the
    /// coordinator drain pump.
    pub fn pop(&self) -> Option<Morsel> {
        let m = self.inner.pop()?;
        self.pop_notify.notify_one();
        Some(m)
    }

    /// Blocking consumer pop. Returns `Ok(morsel)` when a morsel is
    /// available, `Err(Drained)` when every generator has drained
    /// *and* the queue is empty. Polls with the given `park_timeout`
    /// so the worker can periodically re-check cancellation
    /// (cancellation is a per-`QueryContext` concern; this queue
    /// just provides the wake-up cadence).
    ///
    /// **Missed-wakeup safety net.** There is a small window between
    /// the post-pop drain check and the `park.lock()` below where the
    /// coordinator could call [`Self::mark_drained`] without observing
    /// any parked worker. The `wait_timeout` here — *not*
    /// `wait_forever` — is the safety net that bounds the worst-case
    /// observation latency to one `park_timeout`. Do not change this
    /// to an unbounded wait without first introducing an explicit
    /// re-check after parking.
    pub fn pop_or_park(&self, park_timeout: Duration) -> Result<Morsel, MorselQueueDrained> {
        loop {
            if let Some(m) = self.pop() {
                return Ok(m);
            }
            if self.all_generators_drained.load(Ordering::Acquire) && self.inner.is_empty() {
                return Err(MorselQueueDrained);
            }
            let g = self.park.lock().expect("MorselQueue park mutex poisoned");
            // Timed wait: workers re-check the drain flag and the
            // queue depth on every wakeup so cancellation /
            // late-drain transitions are observed within the
            // configured cadence.
            let _ = self.push_notify.wait_timeout(g, park_timeout);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::morsel::{EntityRange, Morsel};
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn fake_morsel(shard: u32) -> Morsel {
        Morsel {
            shard_id: shard,
            entity_range: EntityRange::All,
            segments: Vec::new(),
            estimated_rows: 0,
            is_shard_final: true,
        }
    }

    #[test]
    fn push_then_pop_round_trip() {
        let q = MorselQueue::new(4);
        assert_eq!(q.capacity(), 4);
        assert!(q.is_empty());
        q.push(fake_morsel(1)).unwrap();
        q.push(fake_morsel(2)).unwrap();
        assert_eq!(q.len(), 2);
        assert_eq!(q.pop().unwrap().shard_id, 1);
        assert_eq!(q.pop().unwrap().shard_id, 2);
        assert!(q.pop().is_none());
    }

    #[test]
    fn push_returns_full_at_capacity() {
        let q = MorselQueue::new(2);
        q.push(fake_morsel(0)).unwrap();
        q.push(fake_morsel(1)).unwrap();
        match q.push(fake_morsel(2)) {
            Err(MorselQueueFull(m)) => assert_eq!(m.shard_id, 2),
            Ok(()) => panic!("expected Full"),
        }
        // Drain one; push succeeds again.
        q.pop().unwrap();
        q.push(fake_morsel(3)).unwrap();
    }

    #[test]
    fn pop_or_park_returns_drained_when_marked_and_empty() {
        let q = MorselQueue::new(1);
        q.mark_drained();
        match q.pop_or_park(Duration::from_millis(10)) {
            Err(MorselQueueDrained) => {}
            Ok(_) => panic!("expected Drained"),
        }
    }

    #[test]
    fn pop_or_park_pops_morsel_pushed_after_park() {
        let q = Arc::new(MorselQueue::new(1));
        let q2 = Arc::clone(&q);
        let h = thread::spawn(move || {
            q2.pop_or_park(Duration::from_millis(50))
                .expect("morsel must arrive")
                .shard_id
        });
        // Give the worker time to park.
        thread::sleep(Duration::from_millis(10));
        q.push(fake_morsel(7)).unwrap();
        assert_eq!(h.join().unwrap(), 7);
    }

    #[test]
    fn mark_drained_wakes_parked_workers() {
        let q = Arc::new(MorselQueue::new(1));
        let q2 = Arc::clone(&q);
        let h = thread::spawn(move || q2.pop_or_park(Duration::from_secs(5)).is_err());
        thread::sleep(Duration::from_millis(10));
        q.mark_drained();
        assert!(h.join().unwrap(), "worker observes drain");
    }

    #[test]
    fn is_drained_requires_both_flag_and_empty() {
        let q = MorselQueue::new(2);
        assert!(!q.is_drained());
        q.push(fake_morsel(0)).unwrap();
        q.mark_drained();
        // Drained flag is set but queue is non-empty.
        assert!(!q.is_drained());
        q.pop();
        assert!(q.is_drained());
    }

    #[test]
    fn capacity_zero_panics() {
        let result = std::panic::catch_unwind(|| MorselQueue::new(0));
        assert!(result.is_err(), "ArrayQueue::new(0) must panic");
    }
}
