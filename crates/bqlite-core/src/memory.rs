//! Memory budget trait and reservation API.
//!
//! This module defines the [`MemoryBudget`] trait — the interface that
//! operators use to request, track, and release memory allocations within
//! a query's memory budget.
//!
//! # Design
//!
//! Operators that grow with data size (aggregation hash tables, sort buffers,
//! decoded column data) call [`MemoryBudget::try_reserve`] before each
//! allocation. On budget exhaustion the method returns
//! [`Err(BqliteError::MemoryBudgetExceeded { .. })`], at which point the operator
//! should attempt to spill (see [`SpillNotification`]) before propagating the
//! error.
//!
//! [`MemoryReservation`] is a RAII guard: when it is dropped the reserved
//! bytes are automatically returned to the budget.
//!
//! # Implementations
//!
//! - [`UnboundedMemory`] — the Wave 1 stub. Always succeeds, performs no
//!   accounting. Used in unit tests and any path that has not yet been
//!   wired to a real query budget.
//! - [`MemoryTracker`] — the Wave 5 enforcement scaffold (TASK-510). One
//!   tracker per active query, shared via `Arc` across operators. Backed
//!   by a single `AtomicU64` for the success path and a `Mutex` only on
//!   the slow / failure path. Spill handlers iterate per § 4.1 of
//!   [`docs/design/engine/memory-budget.md`](../../../../docs/design/engine/memory-budget.md).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::error::BqliteError;
use crate::Result;

// ---------------------------------------------------------------------------
// MemoryReservation
// ---------------------------------------------------------------------------

/// RAII guard for a memory reservation.
///
/// Created by [`MemoryBudget::try_reserve`]. When dropped, the reserved
/// bytes are returned to the budget by invoking the release callback.
///
/// Reservations must not outlive the budget that created them.
pub struct MemoryReservation {
    bytes: u64,
    /// Release callback. `None` for no-op budgets such as [`UnboundedMemory`].
    release: Option<Box<dyn Fn(u64) + Send + Sync>>,
}

impl MemoryReservation {
    /// The number of bytes held by this reservation.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Consume the reservation and return its bytes without invoking the
    /// release callback. Use this when transferring ownership of the
    /// reserved memory to a new reservation.
    pub fn forget(mut self) -> u64 {
        self.release = None;
        self.bytes
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if let Some(f) = &self.release {
            f(self.bytes);
        }
    }
}

impl std::fmt::Debug for MemoryReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// SpillNotification
// ---------------------------------------------------------------------------

/// Callback invoked when the memory budget is under pressure.
///
/// Operators that can spill their state to disk implement this trait and
/// register themselves with [`MemoryBudget::register_spill_handler`].
/// When a [`MemoryBudget::try_reserve`] call cannot be satisfied, the
/// budget iterates over registered handlers before returning an error,
/// giving operators a chance to free memory first.
///
/// # Return value
///
/// Return the number of bytes freed. If no bytes could be freed, return 0.
/// The budget may call handlers in registration order until `bytes_needed`
/// have been freed or all handlers have been exhausted.
///
/// # Wave 1 status
///
/// The spill invocation sequence and retry logic are defined in Wave 5.
/// Operators should implement this trait now so that Wave 5 can wire
/// the invocation without changing operator code.
pub trait SpillNotification: Send + Sync {
    /// Called when the budget needs the operator to free at least
    /// `bytes_needed` bytes. Returns the number of bytes actually freed.
    fn on_pressure(&self, bytes_needed: u64) -> u64;
}

// ---------------------------------------------------------------------------
// MemoryBudget trait
// ---------------------------------------------------------------------------

/// Byte-accounting interface shared by all bqlite operators.
///
/// Every operator that allocates memory proportional to data size should
/// call [`try_reserve`] before the allocation and hold the returned
/// [`MemoryReservation`] for the lifetime of the allocation. Releasing
/// the reservation (by dropping it) returns the bytes to the budget.
///
/// # Implementing this trait
///
/// Implementations must be [`Send`] + [`Sync`] so they can be shared
/// across shard-task threads via [`Arc`].
///
/// # Wave 1 note
///
/// The enforcement model (hierarchical budget trees, per-shard limits,
/// spill retry protocol) is finalized in Wave 5. Until then, use
/// [`UnboundedMemory`] in tests and in the engine stub.
pub trait MemoryBudget: Send + Sync {
    /// Attempt to reserve `bytes` of memory.
    ///
    /// Returns a [`MemoryReservation`] on success. On failure returns
    /// `Err(BqliteError::MemoryBudgetExceeded { .. })` indicating that
    /// the budget would be exceeded. Callers should try to spill before
    /// propagating the error.
    fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation>;

    /// Register a spill handler that the budget may call when under pressure.
    ///
    /// A budget may hold multiple handlers. They are called in registration
    /// order when [`try_reserve`] cannot be satisfied. The real invocation
    /// protocol is defined in Wave 5.
    fn register_spill_handler(&self, handler: Arc<dyn SpillNotification>);

    /// Returns the number of bytes currently reserved from this budget.
    fn used_bytes(&self) -> u64;

    /// Returns the total byte budget (maximum that may be reserved at once).
    ///
    /// Returns `u64::MAX` for unbounded budgets such as [`UnboundedMemory`].
    fn budget_bytes(&self) -> u64;
}

// ---------------------------------------------------------------------------
// UnboundedMemory — stub implementation
// ---------------------------------------------------------------------------

/// A no-op [`MemoryBudget`] that always succeeds and performs no tracking.
///
/// Use this in tests and in Wave 1 operator stubs where actual memory
/// enforcement is not yet required. Reservations are zero-cost and the
/// release callback is a no-op.
///
/// Wave 5 replaces this with a real hierarchical `MemoryTracker`.
#[derive(Default)]
pub struct UnboundedMemory {
    // Handlers are stored but never invoked — Wave 5 adds the invocation loop.
    _handlers: Mutex<Vec<Arc<dyn SpillNotification>>>,
}

impl std::fmt::Debug for UnboundedMemory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnboundedMemory").finish_non_exhaustive()
    }
}

impl UnboundedMemory {
    /// Create a new unbounded (no-op) budget.
    pub fn new() -> Self {
        Self::default()
    }
}

impl MemoryBudget for UnboundedMemory {
    fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation> {
        Ok(MemoryReservation {
            bytes,
            release: None,
        })
    }

    fn register_spill_handler(&self, handler: Arc<dyn SpillNotification>) {
        self._handlers.lock().unwrap().push(handler);
    }

    fn used_bytes(&self) -> u64 {
        0
    }

    fn budget_bytes(&self) -> u64 {
        u64::MAX
    }
}

// ---------------------------------------------------------------------------
// MemoryTracker — enforcement scaffold (TASK-510)
// ---------------------------------------------------------------------------

/// Query-scoped [`MemoryBudget`] implementation backed by a single
/// `AtomicU64` counter.
///
/// One `MemoryTracker` is constructed per executing query and shared via
/// `Arc` between every operator in the tree. Reservations on the success
/// path cost two atomic operations (one `fetch_add` to charge the
/// reservation, one CAS loop to update the peak); the spill-handler
/// mutex is acquired only on the failure path.
///
/// See `docs/design/engine/memory-budget.md` § 4 for the full contract,
/// including the single-retry spill protocol and the re-entrancy
/// guarantees handlers may rely on.
///
/// # Construction
///
/// Use [`MemoryTracker::new`] with the per-query budget in bytes. The
/// engine constructs one tracker per `Engine::query` invocation; tests
/// that don't care about enforcement should keep using
/// [`UnboundedMemory`].
///
/// # Accounting model
///
/// - `used` — the live byte count. Updated atomically on every
///   reservation lifecycle event (charge on `try_reserve` success,
///   release on `MemoryReservation::drop`).
/// - `peak` — the high-water mark observed since construction. Updated
///   under a relaxed CAS loop on every successful reservation; read
///   once at query teardown for instrumentation per § 10 of the design
///   doc.
/// - `budget` — the per-query ceiling. Set once at construction; never
///   resized.
///
/// # Spill handler protocol
///
/// On a failed reservation the tracker:
/// 1. Releases the optimistic charge (`fetch_sub`) so other reservers
///    are not blocked by the overshoot.
/// 2. Clones the registered handler `Arc`s out from under the
///    `spill_handlers` mutex and drops the guard. Handlers are then
///    invoked in registration order; each may itself call back into
///    `try_reserve` without deadlocking.
/// 3. Retries the reservation **once** after each handler that
///    reported freed bytes. If the retry still overshoots, the bytes
///    are released again and the next handler is consulted.
/// 4. Returns `Err(budget_exceeded_error(...))` if no handler frees
///    enough.
///
/// This bounds the wall-time impact of a near-budget query to one
/// spill round per `try_reserve`, regardless of how many handlers are
/// registered — see § 4.1 ("A single retry is the contract").
pub struct MemoryTracker {
    /// Live total of all currently-held reservations.
    used: AtomicU64,
    /// High-water mark observed since construction.
    peak: AtomicU64,
    /// Per-query budget ceiling. Fixed at construction.
    budget: u64,
    /// Spill handlers consulted on the slow path. The mutex is dropped
    /// before any handler is invoked so handlers may call back into
    /// `try_reserve` without deadlocking (§ 4.2).
    spill_handlers: Mutex<Vec<Arc<dyn SpillNotification>>>,
    /// Captured at construction via `Arc::new_cyclic` so the release
    /// callback in [`MemoryReservation`] can hold an `Arc<MemoryTracker>`
    /// without forcing operator-side code to reach behind the
    /// [`MemoryBudget`] trait. Upgrading is infallible while any caller
    /// holds an `Arc` to this tracker — which is the contract with the
    /// engine: the reservation is dropped before `QueryContext`
    /// teardown.
    weak_self: Weak<MemoryTracker>,
}

impl std::fmt::Debug for MemoryTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryTracker")
            .field("budget", &self.budget)
            .field("used", &self.used.load(Ordering::Relaxed))
            .field("peak", &self.peak.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl MemoryTracker {
    /// Construct a new tracker with the given per-query budget (bytes).
    ///
    /// Returns an `Arc<Self>` so callers can directly coerce to
    /// `Arc<dyn MemoryBudget>` when storing on the engine's
    /// `QueryContext`. The tracker captures a `Weak<Self>` internally;
    /// dropping the last `Arc` invalidates outstanding reservations'
    /// release callbacks (which become no-ops). The engine guarantees
    /// the tracker outlives every operator and therefore every
    /// reservation.
    pub fn new(budget_bytes: u64) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            budget: budget_bytes,
            spill_handlers: Mutex::new(Vec::new()),
            weak_self: weak.clone(),
        })
    }

    /// Peak `used_bytes()` value observed since construction.
    ///
    /// Read at query teardown for the `peak_memory_bytes` field in
    /// `ExecutionResult` (per § 10 of the design doc). Always returns
    /// `>= used_bytes()` for any *outstanding* reservations.
    pub fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }

    /// Slow-path release. Used both by the optimistic-overshoot rollback
    /// inside [`Self::try_reserve_inner`] and by the
    /// [`MemoryReservation`] drop callback.
    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        // `Release` ordering pairs with the `Acquire` on `used_bytes()`
        // so observers see a consistent counter after a reservation has
        // been freed.
        let prev = self.used.fetch_sub(bytes, Ordering::Release);
        debug_assert!(
            prev >= bytes,
            "MemoryTracker::release underflow: used={prev}, releasing={bytes}"
        );
    }

    /// Optimistic `fetch_add` followed by the post-add bound check.
    ///
    /// Returns `Some(post_add_used)` if the reservation fits, otherwise
    /// rolls the counter back and returns `None`.
    fn charge(&self, bytes: u64) -> Option<u64> {
        // `Acquire` so we observe any prior `Release`-ordered free.
        let prev = self.used.fetch_add(bytes, Ordering::AcqRel);
        let post = prev.saturating_add(bytes);
        if post <= self.budget {
            // Update the peak under a bounded relaxed CAS loop. We
            // tolerate a spurious lag here — `peak` is a metric, not a
            // correctness invariant.
            let mut current_peak = self.peak.load(Ordering::Relaxed);
            while post > current_peak {
                match self.peak.compare_exchange_weak(
                    current_peak,
                    post,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(observed) => current_peak = observed,
                }
            }
            Some(post)
        } else {
            self.release(bytes);
            None
        }
    }

    /// Build a `MemoryReservation` whose drop callback releases bytes
    /// back to this tracker. The closure holds an `Arc<MemoryTracker>`
    /// upgraded from `weak_self` so the tracker is kept alive at least
    /// as long as the reservation. If the upgrade fails (because the
    /// engine has already torn down the tracker — a programming error,
    /// since reservations must not outlive their tracker), the
    /// reservation degrades to a no-op release.
    fn make_reservation(&self, bytes: u64) -> MemoryReservation {
        match self.weak_self.upgrade() {
            Some(tracker) => MemoryReservation {
                bytes,
                release: Some(Box::new(move |b| tracker.release(b))),
            },
            None => MemoryReservation {
                bytes,
                release: None,
            },
        }
    }
}

impl MemoryBudget for MemoryTracker {
    fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation> {
        // Fast path: success in two atomic operations (one `fetch_add`
        // + one bounded peak CAS loop). No mutex acquisition.
        if self.charge(bytes).is_some() {
            return Ok(self.make_reservation(bytes));
        }

        // Slow path: clone the handler list out of the mutex so any
        // handler that itself calls `try_reserve` does not deadlock
        // (§ 4.2 re-entrancy contract).
        let handlers: Vec<Arc<dyn SpillNotification>> = {
            let guard = self
                .spill_handlers
                .lock()
                .expect("MemoryTracker spill_handlers mutex poisoned");
            guard.clone()
        };

        for handler in handlers {
            let freed = handler.on_pressure(bytes);
            if freed == 0 {
                continue;
            }
            // Single retry per handler that reports freed bytes (§ 4.1).
            if self.charge(bytes).is_some() {
                return Ok(self.make_reservation(bytes));
            }
        }

        Err(budget_exceeded_error(
            bytes,
            self.budget,
            self.used.load(Ordering::Acquire),
        ))
    }

    fn register_spill_handler(&self, handler: Arc<dyn SpillNotification>) {
        self.spill_handlers
            .lock()
            .expect("MemoryTracker spill_handlers mutex poisoned")
            .push(handler);
    }

    fn used_bytes(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    fn budget_bytes(&self) -> u64 {
        self.budget
    }
}

// ---------------------------------------------------------------------------
// BudgetExceededError helper
// ---------------------------------------------------------------------------

/// Construct a [`BqliteError::MemoryBudgetExceeded`] with the standard
/// shape used by every memory-budget-aware operator. `requested` is
/// folded into `used` (the variant only exposes `{ used, budget }` per
/// `docs/design/engine/cancellation.md` §4.3); callers that need to
/// surface the requested-bytes diagnostic should log it before
/// propagating the error.
pub fn budget_exceeded_error(requested: u64, budget: u64, used: u64) -> BqliteError {
    BqliteError::MemoryBudgetExceeded {
        used: used.saturating_add(requested),
        budget,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn unbounded_reserve_succeeds() {
        let budget = UnboundedMemory::new();
        let r = budget.try_reserve(1_000_000).expect("should succeed");
        assert_eq!(r.bytes(), 1_000_000);
    }

    #[test]
    fn unbounded_reported_bytes() {
        let budget = UnboundedMemory::new();
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(budget.budget_bytes(), u64::MAX);
    }

    #[test]
    fn reservation_drop_is_safe() {
        let budget = UnboundedMemory::new();
        {
            let _r = budget.try_reserve(512).expect("should succeed");
            // drop here — must not panic
        }
    }

    #[test]
    fn reservation_forget_suppresses_release() {
        // Verify that forget() does not invoke the callback.
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();
        let r = MemoryReservation {
            bytes: 100,
            release: Some(Box::new(move |b| {
                counter_clone.fetch_add(b, Ordering::SeqCst);
            })),
        };
        let bytes = r.forget();
        assert_eq!(bytes, 100);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "release should not be called after forget"
        );
    }

    #[test]
    fn reservation_release_callback_fires_on_drop() {
        let released = Arc::new(AtomicU64::new(0));
        let released_clone = released.clone();
        {
            let _r = MemoryReservation {
                bytes: 42,
                release: Some(Box::new(move |b| {
                    released_clone.fetch_add(b, Ordering::SeqCst);
                })),
            };
        }
        assert_eq!(released.load(Ordering::SeqCst), 42);
    }

    #[test]
    fn register_spill_handler_does_not_panic() {
        struct NoopSpill;
        impl SpillNotification for NoopSpill {
            fn on_pressure(&self, _: u64) -> u64 {
                0
            }
        }
        let budget = UnboundedMemory::new();
        budget.register_spill_handler(Arc::new(NoopSpill));
    }

    #[test]
    fn budget_exceeded_error_yields_structured_variant() {
        let err = budget_exceeded_error(100, 200, 150);
        match err {
            BqliteError::MemoryBudgetExceeded { used, budget } => {
                // requested(100) is folded into used(150) → 250.
                assert_eq!(used, 250);
                assert_eq!(budget, 200);
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
        // Display still names the budget and the post-attempt usage.
        let msg = budget_exceeded_error(100, 200, 150).to_string();
        assert!(msg.contains("memory budget exceeded"));
        assert!(msg.contains("250"));
        assert!(msg.contains("200"));
    }

    // ─────────────────────────────────────────────────────────────────
    // MemoryTracker tests (TASK-510, design § 4)
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn tracker_initial_state_is_zero() {
        let tracker = MemoryTracker::new(1_000);
        assert_eq!(tracker.used_bytes(), 0);
        assert_eq!(tracker.peak_bytes(), 0);
        assert_eq!(tracker.budget_bytes(), 1_000);
    }

    #[test]
    fn tracker_reserve_then_drop_releases_bytes() {
        let tracker = MemoryTracker::new(1_000);
        {
            let r = tracker.try_reserve(400).expect("must fit");
            assert_eq!(r.bytes(), 400);
            assert_eq!(tracker.used_bytes(), 400);
        }
        assert_eq!(tracker.used_bytes(), 0, "drop must release reserved bytes");
    }

    #[test]
    fn tracker_reservations_sum_correctly() {
        let tracker = MemoryTracker::new(10_000);
        let a = tracker.try_reserve(100).unwrap();
        let b = tracker.try_reserve(250).unwrap();
        let c = tracker.try_reserve(50).unwrap();
        assert_eq!(tracker.used_bytes(), 400);
        drop(b);
        assert_eq!(tracker.used_bytes(), 150);
        drop(a);
        drop(c);
        assert_eq!(tracker.used_bytes(), 0);
    }

    #[test]
    fn tracker_overshoot_returns_typed_error() {
        let tracker = MemoryTracker::new(500);
        let _r = tracker.try_reserve(400).unwrap();
        let err = tracker.try_reserve(200).unwrap_err();
        match err {
            BqliteError::MemoryBudgetExceeded { used, budget } => {
                // Tracker rolls back the failed optimistic charge;
                // budget_exceeded_error folds requested+used (200+400=600).
                assert_eq!(used, 600);
                assert_eq!(budget, 500);
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
        // The failed reservation must roll back the optimistic charge.
        assert_eq!(tracker.used_bytes(), 400);
    }

    #[test]
    fn tracker_peak_tracks_high_water_mark() {
        let tracker = MemoryTracker::new(1_000);
        let a = tracker.try_reserve(400).unwrap();
        let b = tracker.try_reserve(300).unwrap();
        assert_eq!(tracker.peak_bytes(), 700);
        drop(b);
        assert_eq!(
            tracker.peak_bytes(),
            700,
            "peak does not regress on release"
        );
        drop(a);
        let c = tracker.try_reserve(200).unwrap();
        assert_eq!(tracker.peak_bytes(), 700, "peak is monotonic");
        drop(c);
    }

    #[test]
    fn tracker_zero_byte_reservation_is_legal() {
        // Operators may opportunistically reserve zero bytes to test the
        // surface. The tracker accepts it as a no-op.
        let tracker = MemoryTracker::new(100);
        let r = tracker.try_reserve(0).unwrap();
        assert_eq!(r.bytes(), 0);
        assert_eq!(tracker.used_bytes(), 0);
    }

    #[test]
    fn tracker_forget_does_not_release() {
        let tracker = MemoryTracker::new(1_000);
        let r = tracker.try_reserve(300).unwrap();
        assert_eq!(tracker.used_bytes(), 300);
        let bytes = r.forget();
        assert_eq!(bytes, 300);
        // forget() suppresses the release callback — bytes are
        // permanently charged to this tracker. Operators use this when
        // transferring reservation ownership across an operator
        // boundary; nothing in the tracker can detect the leak.
        assert_eq!(tracker.used_bytes(), 300);
    }

    #[test]
    fn tracker_zero_freed_handler_does_not_grant_retry() {
        struct ZeroSpill;
        impl SpillNotification for ZeroSpill {
            fn on_pressure(&self, _: u64) -> u64 {
                0
            }
        }
        let tracker = MemoryTracker::new(500);
        let _r = tracker.try_reserve(400).unwrap();
        tracker.register_spill_handler(Arc::new(ZeroSpill));
        // Handler reports zero freed → no retry → fail.
        assert!(tracker.try_reserve(200).is_err());
    }

    #[test]
    fn tracker_spill_handler_freeing_bytes_unblocks_reservation() {
        // The handler "frees" 200 bytes by dropping a reservation it
        // captured. The retry then fits.
        let tracker = MemoryTracker::new(500);

        struct DroppingSpill {
            reservation: Mutex<Option<MemoryReservation>>,
        }
        impl SpillNotification for DroppingSpill {
            fn on_pressure(&self, _bytes_needed: u64) -> u64 {
                let mut guard = self.reservation.lock().unwrap();
                if let Some(r) = guard.take() {
                    let bytes = r.bytes();
                    drop(r);
                    bytes
                } else {
                    0
                }
            }
        }

        let to_free = tracker.try_reserve(300).unwrap();
        let _hold = tracker.try_reserve(150).unwrap();
        tracker.register_spill_handler(Arc::new(DroppingSpill {
            reservation: Mutex::new(Some(to_free)),
        }));

        let r = tracker
            .try_reserve(250)
            .expect("handler should free enough to fit the retry");
        assert_eq!(r.bytes(), 250);
    }

    #[test]
    fn tracker_spill_handler_may_reenter_try_reserve() {
        // § 4.2: handlers must be re-entrant-safe relative to
        // try_reserve. The implementation drops the spill_handlers
        // mutex before invoking handlers, so a handler that calls back
        // into try_reserve must not deadlock. This test would hang on
        // a regression that holds the guard across the invocation.
        struct ReentrantSpill {
            tracker: Mutex<Option<Arc<MemoryTracker>>>,
            held: Mutex<Option<MemoryReservation>>,
        }
        impl SpillNotification for ReentrantSpill {
            fn on_pressure(&self, _: u64) -> u64 {
                let tracker = match self.tracker.lock().unwrap().clone() {
                    Some(t) => t,
                    None => return 0,
                };
                // Re-enter try_reserve from inside a spill handler.
                // The reservation succeeds against the same tracker
                // and is held in the spill struct so we can verify
                // the budget remained accurate.
                if let Ok(r) = tracker.try_reserve(0) {
                    *self.held.lock().unwrap() = Some(r);
                }
                0
            }
        }

        let tracker = MemoryTracker::new(500);
        let _r = tracker.try_reserve(400).unwrap();
        tracker.register_spill_handler(Arc::new(ReentrantSpill {
            tracker: Mutex::new(Some(Arc::clone(&tracker))),
            held: Mutex::new(None),
        }));
        // Failure path invokes the handler, which calls try_reserve
        // re-entrantly without deadlocking. The outer try_reserve
        // still fails (handler reports zero freed) but completes.
        assert!(tracker.try_reserve(200).is_err());
    }

    #[test]
    fn tracker_handlers_iterate_in_registration_order() {
        let tracker = MemoryTracker::new(500);

        // Records the order in which handlers were called.
        struct RecordingSpill {
            id: u32,
            order: Arc<Mutex<Vec<u32>>>,
            // freed bytes the handler should claim. Returning 0 short-
            // circuits the retry; non-zero forces a retry that may
            // still fail because no bytes were actually released.
            claim: u64,
        }
        impl SpillNotification for RecordingSpill {
            fn on_pressure(&self, _: u64) -> u64 {
                self.order.lock().unwrap().push(self.id);
                self.claim
            }
        }

        let order = Arc::new(Mutex::new(Vec::new()));
        tracker.register_spill_handler(Arc::new(RecordingSpill {
            id: 1,
            order: Arc::clone(&order),
            claim: 0,
        }));
        tracker.register_spill_handler(Arc::new(RecordingSpill {
            id: 2,
            order: Arc::clone(&order),
            claim: 0,
        }));

        let _r = tracker.try_reserve(400).unwrap();
        let _ = tracker.try_reserve(200); // overshoots budget
        let observed = order.lock().unwrap().clone();
        assert_eq!(observed, vec![1, 2], "handlers fire in registration order");
    }

    #[test]
    fn tracker_reservation_outlives_dropped_arc() {
        // The reservation degrades to a no-op release if the tracker
        // is torn down before the reservation is dropped. This is a
        // programming error per the design doc, but the tracker must
        // not crash.
        let tracker = MemoryTracker::new(500);
        let r = tracker.try_reserve(100).unwrap();
        drop(tracker);
        drop(r); // must not panic
    }

    #[test]
    fn tracker_concurrent_reservations_preserve_invariant() {
        use std::sync::Barrier;
        use std::thread;

        // N threads each try to reserve `STEP` bytes from a budget
        // that admits exactly ADMIT simultaneous reservations. Each
        // thread holds its reservation until every other thread has
        // finished its `try_reserve` call (via a `Barrier`), so a
        // buggy implementation that lets reservations through serially
        // cannot mask the bug by releasing before the next thread
        // attempts.
        const STEP: u64 = 1_000;
        const ADMIT: u64 = 8;
        const THREADS: u64 = 16;

        let tracker = MemoryTracker::new(STEP * ADMIT);
        let success = Arc::new(AtomicU64::new(0));
        let failure = Arc::new(AtomicU64::new(0));
        let barrier = Arc::new(Barrier::new(THREADS as usize));
        let mut handles = Vec::with_capacity(THREADS as usize);

        for _ in 0..THREADS {
            let tracker = Arc::clone(&tracker);
            let success = Arc::clone(&success);
            let failure = Arc::clone(&failure);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let result = tracker.try_reserve(STEP);
                match &result {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        failure.fetch_add(1, Ordering::SeqCst);
                    }
                }
                // Hold the reservation (or the failure) until every
                // other thread is past try_reserve. `result` drops
                // after the barrier, releasing successful
                // reservations.
                barrier.wait();
                drop(result);
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let s = success.load(Ordering::SeqCst);
        let f = failure.load(Ordering::SeqCst);
        assert_eq!(s + f, THREADS, "every attempt is counted exactly once");
        assert_eq!(
            s, ADMIT,
            "exactly ADMIT reservations fit; the rest must surface budget-exceeded"
        );
        assert!(
            tracker.peak_bytes() <= STEP * ADMIT,
            "peak never exceeds budget: peak={} budget={}",
            tracker.peak_bytes(),
            STEP * ADMIT
        );
        assert_eq!(
            tracker.used_bytes(),
            0,
            "all reservations released after threads exited"
        );
    }

    #[test]
    fn tracker_dyn_dispatch_through_arc_dyn_memorybudget() {
        // The engine wires the tracker as `Arc<dyn MemoryBudget>` on
        // QueryContext. Ensure dispatch through the trait object
        // produces working reservations whose drop releases bytes.
        let tracker = MemoryTracker::new(1_000);
        let budget: Arc<dyn MemoryBudget> = tracker.clone();
        {
            let r = budget.try_reserve(400).unwrap();
            assert_eq!(r.bytes(), 400);
            assert_eq!(budget.used_bytes(), 400);
            assert_eq!(tracker.used_bytes(), 400);
        }
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(tracker.peak_bytes(), 400);
    }
}
