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
//! [`Err(BqliteError::Execution(...))`], at which point the operator should
//! attempt to spill (see [`SpillNotification`]) before propagating the error.
//!
//! [`MemoryReservation`] is a RAII guard: when it is dropped the reserved
//! bytes are automatically returned to the budget.
//!
//! # Wave 1 status
//!
//! This module ships the trait surface so every operator crate can implement
//! the interface from day one. The concrete enforcement model — hierarchical
//! budget trees, per-shard limits, spill-to-disk file management — is
//! designed and implemented in Wave 5. The stub implementation
//! ([`UnboundedMemory`]) always succeeds and performs no tracking.

use std::sync::{Arc, Mutex};

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
    /// `Err(BqliteError::Execution(...))` indicating that the budget
    /// would be exceeded. Callers should try to spill before propagating
    /// the error.
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
// BudgetExceededError helper
// ---------------------------------------------------------------------------

/// Construct a [`BqliteError::Execution`] indicating a memory budget
/// overflow. Centralised here so the error message is consistent.
pub fn budget_exceeded_error(requested: u64, budget: u64, used: u64) -> BqliteError {
    BqliteError::Execution(format!(
        "memory budget exceeded: requested {requested} bytes, \
         {used} bytes already in use, budget is {budget} bytes"
    ))
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
    fn budget_exceeded_error_message() {
        let err = budget_exceeded_error(100, 200, 150);
        let msg = err.to_string();
        assert!(msg.contains("memory budget exceeded"));
        assert!(msg.contains("100"));
        assert!(msg.contains("200"));
        assert!(msg.contains("150"));
    }
}
