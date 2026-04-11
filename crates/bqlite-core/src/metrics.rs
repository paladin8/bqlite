//! Per-operator metric counters.
//!
//! Every physical operator increments small counters as it processes
//! batches: rows in, rows out, bytes decoded or written, wall-time
//! nanoseconds. After a query completes the engine aggregates per-operator
//! snapshots into per-query totals and attaches them to the result.
//!
//! # Wave 1 scope
//!
//! This module ships the operator-facing counter surface ([`Metrics`]
//! trait + two default implementors + a RAII timer) and a
//! [`MetricsSnapshot`] value type with a query-level aggregation hook.
//! The full set of per-shard counters enumerated in
//! `docs/design/execution-model.md` §14 (rows_scanned, rows_after_filter,
//! segments_pruned, spill_bytes_written, ...) is the engine's concern and
//! lives in a richer `QueryMetrics` struct in `bqlite-engine`; the
//! operator-level trait here is intentionally narrower.
//!
//! The metrics-to-span bridge mentioned in
//! [`crate::telemetry`] will wire `MetricsSnapshot` values into
//! `tracing::Span` attributes in a later wave. No `tracing` dependency
//! is added here — snapshots are plain data.
//!
//! # Why a trait rather than a plain struct?
//!
//! Operators hold an `Arc<dyn Metrics>` handed to them at construction
//! time (mirroring the [`crate::MemoryBudget`] pattern). The trait lets
//! tests and stub operators swap in the zero-cost [`NoopMetrics`] without
//! changing operator code, and leaves room for a future
//! `TracingMetrics` impl that also emits events to spans without
//! breaking existing callers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

// ---------------------------------------------------------------------------
// MetricsSnapshot
// ---------------------------------------------------------------------------

/// Immutable point-in-time view of a [`Metrics`] counter set.
///
/// Snapshots are `Copy` so callers can pass them around freely. The
/// engine takes one snapshot per operator at query completion and
/// [`merge`](Self::merge)s them into a per-query total.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    /// Rows fed into the operator (or its child, for scans).
    pub rows_in: u64,
    /// Rows produced by the operator.
    pub rows_out: u64,
    /// Bytes of column data decoded or written by the operator.
    pub bytes: u64,
    /// Wall-time nanoseconds consumed by the operator.
    pub elapsed_ns: u64,
}

impl MetricsSnapshot {
    /// Construct a zeroed snapshot. Equivalent to `MetricsSnapshot::default()`.
    pub fn zero() -> Self {
        Self::default()
    }

    /// Fold `other` into `self` component-wise (saturating add).
    ///
    /// Used by the engine's query-level aggregation hook to merge a
    /// per-operator snapshot into the running total for the query.
    /// Saturating add means pathological counter overflow clamps at
    /// `u64::MAX` rather than wrapping — the total stays monotonic.
    pub fn merge(&mut self, other: &MetricsSnapshot) {
        self.rows_in = self.rows_in.saturating_add(other.rows_in);
        self.rows_out = self.rows_out.saturating_add(other.rows_out);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.elapsed_ns = self.elapsed_ns.saturating_add(other.elapsed_ns);
    }

    /// Return the component-wise sum of `self` and `other` without
    /// mutating either operand.
    pub fn add(&self, other: &MetricsSnapshot) -> MetricsSnapshot {
        let mut out = *self;
        out.merge(other);
        out
    }

    /// True if every counter is zero.
    pub fn is_zero(&self) -> bool {
        self.rows_in == 0 && self.rows_out == 0 && self.bytes == 0 && self.elapsed_ns == 0
    }
}

// ---------------------------------------------------------------------------
// Metrics trait
// ---------------------------------------------------------------------------

/// Per-operator metric counter surface.
///
/// Operators receive an `Arc<dyn Metrics>` at construction time (the
/// engine clones one counter set per operator in the tree) and call the
/// `record_*` methods in their hot paths. Every call must be:
///
/// - **Zero-allocation.** Implementations use atomic counters, not a
///   heap data structure.
/// - **Branch-minimal.** Fetch-and-add is the only operation expected on
///   the hot path.
/// - **Thread-safe.** A single counter set is shared across the shard-task
///   threads that run one operator; the trait's `Send + Sync` bound is
///   what makes this sound.
///
/// [`snapshot`](Self::snapshot) is called off the hot path (once per
/// operator at query completion, or periodically by a debug dump) and
/// returns a plain-data [`MetricsSnapshot`] the engine can merge and
/// serialize.
pub trait Metrics: Send + Sync {
    /// Add `n` to the input-row counter.
    fn record_rows_in(&self, n: u64);

    /// Add `n` to the output-row counter.
    fn record_rows_out(&self, n: u64);

    /// Add `n` to the bytes counter.
    fn record_bytes(&self, n: u64);

    /// Add `ns` wall-time nanoseconds to the elapsed counter.
    ///
    /// Typically called via a [`MetricsTimer`] RAII guard rather than
    /// directly, so the elapsed value is computed with a single
    /// `Instant::now()` call per scope.
    fn record_elapsed_ns(&self, ns: u64);

    /// Take a point-in-time snapshot of the current counter values.
    ///
    /// Not called on the hot path. The returned snapshot is a value
    /// type and does not share state with the underlying counters.
    fn snapshot(&self) -> MetricsSnapshot;
}

// ---------------------------------------------------------------------------
// AtomicMetrics — default production implementor
// ---------------------------------------------------------------------------

/// Default [`Metrics`] implementation backed by atomic counters.
///
/// Every counter is an `AtomicU64` using relaxed ordering. Relaxed is
/// sufficient because:
///
/// 1. Counters carry no cross-thread happens-before relationships; a
///    later read may observe a stale sum but never a torn one on any
///    architecture Rust supports.
/// 2. Query completion flushes the counters through a separate
///    synchronization point (shard-task join), so the final
///    [`snapshot`](Self::snapshot) taken after the join observes every
///    prior write.
#[derive(Debug, Default)]
pub struct AtomicMetrics {
    rows_in: AtomicU64,
    rows_out: AtomicU64,
    bytes: AtomicU64,
    elapsed_ns: AtomicU64,
}

impl AtomicMetrics {
    /// Construct a zeroed counter set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Metrics for AtomicMetrics {
    #[inline]
    fn record_rows_in(&self, n: u64) {
        self.rows_in.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    fn record_rows_out(&self, n: u64) {
        self.rows_out.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    fn record_bytes(&self, n: u64) {
        self.bytes.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    fn record_elapsed_ns(&self, ns: u64) {
        self.elapsed_ns.fetch_add(ns, Ordering::Relaxed);
    }

    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            rows_in: self.rows_in.load(Ordering::Relaxed),
            rows_out: self.rows_out.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            elapsed_ns: self.elapsed_ns.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// NoopMetrics — zero-cost stub for tests and operators that don't care
// ---------------------------------------------------------------------------

/// Zero-cost [`Metrics`] implementation that records nothing.
///
/// Used by tests and by operator stubs that have no meaningful counters
/// yet. Every method is an empty body; `snapshot` returns
/// [`MetricsSnapshot::zero`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl NoopMetrics {
    /// Construct a no-op metrics handle.
    pub fn new() -> Self {
        Self
    }
}

impl Metrics for NoopMetrics {
    #[inline]
    fn record_rows_in(&self, _n: u64) {}
    #[inline]
    fn record_rows_out(&self, _n: u64) {}
    #[inline]
    fn record_bytes(&self, _n: u64) {}
    #[inline]
    fn record_elapsed_ns(&self, _ns: u64) {}

    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot::zero()
    }
}

// ---------------------------------------------------------------------------
// MetricsTimer — RAII helper for wall-time recording
// ---------------------------------------------------------------------------

/// RAII guard that records wall-time nanoseconds to a [`Metrics`]
/// implementor when dropped.
///
/// Usage:
///
/// ```rust
/// # use std::sync::Arc;
/// # use std::time::Duration;
/// # use bqlite_core::metrics::{AtomicMetrics, Metrics, MetricsTimer};
/// let metrics: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
/// {
///     let _timer = MetricsTimer::new(&*metrics);
///     std::thread::sleep(Duration::from_micros(1));
/// } // timer drops here, elapsed ns is recorded on `metrics`
/// assert!(metrics.snapshot().elapsed_ns > 0);
/// ```
///
/// Overflow handling: an elapsed duration that exceeds `u64::MAX`
/// nanoseconds (584+ years) clamps at `u64::MAX`. This cannot happen
/// in practice but is documented so the saturating conversion is
/// explicit rather than implicit.
pub struct MetricsTimer<'m> {
    metrics: &'m dyn Metrics,
    start: Instant,
}

impl<'m> MetricsTimer<'m> {
    /// Start a new timer. The elapsed duration is recorded when the
    /// returned guard is dropped.
    pub fn new(metrics: &'m dyn Metrics) -> Self {
        Self {
            metrics,
            start: Instant::now(),
        }
    }
}

impl Drop for MetricsTimer<'_> {
    fn drop(&mut self) {
        let nanos_u128 = self.start.elapsed().as_nanos();
        let ns = u64::try_from(nanos_u128).unwrap_or(u64::MAX);
        self.metrics.record_elapsed_ns(ns);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::*;

    // ── MetricsSnapshot ──────────────────────────────────────────────────

    #[test]
    fn snapshot_zero_is_default() {
        assert_eq!(MetricsSnapshot::zero(), MetricsSnapshot::default());
        assert!(MetricsSnapshot::zero().is_zero());
    }

    #[test]
    fn snapshot_is_zero_reflects_all_counters() {
        let mut s = MetricsSnapshot::zero();
        assert!(s.is_zero());
        s.rows_in = 1;
        assert!(!s.is_zero());
        s.rows_in = 0;
        s.bytes = 1;
        assert!(!s.is_zero());
    }

    #[test]
    fn snapshot_merge_sums_components() {
        let mut a = MetricsSnapshot {
            rows_in: 10,
            rows_out: 5,
            bytes: 100,
            elapsed_ns: 1_000,
        };
        let b = MetricsSnapshot {
            rows_in: 3,
            rows_out: 1,
            bytes: 20,
            elapsed_ns: 500,
        };
        a.merge(&b);
        assert_eq!(a.rows_in, 13);
        assert_eq!(a.rows_out, 6);
        assert_eq!(a.bytes, 120);
        assert_eq!(a.elapsed_ns, 1_500);
    }

    #[test]
    fn snapshot_merge_is_commutative() {
        let a = MetricsSnapshot {
            rows_in: 7,
            rows_out: 2,
            bytes: 99,
            elapsed_ns: 42,
        };
        let b = MetricsSnapshot {
            rows_in: 3,
            rows_out: 8,
            bytes: 1,
            elapsed_ns: 58,
        };
        let mut a_into_b = b;
        a_into_b.merge(&a);
        let mut b_into_a = a;
        b_into_a.merge(&b);
        assert_eq!(a_into_b, b_into_a);
    }

    #[test]
    fn snapshot_add_does_not_mutate() {
        let a = MetricsSnapshot {
            rows_in: 1,
            rows_out: 2,
            bytes: 3,
            elapsed_ns: 4,
        };
        let b = MetricsSnapshot {
            rows_in: 10,
            rows_out: 20,
            bytes: 30,
            elapsed_ns: 40,
        };
        let sum = a.add(&b);
        // Neither operand mutated.
        assert_eq!(a.rows_in, 1);
        assert_eq!(b.rows_in, 10);
        // Sum correct.
        assert_eq!(sum.rows_in, 11);
        assert_eq!(sum.rows_out, 22);
        assert_eq!(sum.bytes, 33);
        assert_eq!(sum.elapsed_ns, 44);
    }

    #[test]
    fn snapshot_merge_saturates_on_overflow() {
        let mut a = MetricsSnapshot {
            rows_in: u64::MAX,
            rows_out: u64::MAX - 1,
            bytes: u64::MAX,
            elapsed_ns: 100,
        };
        let b = MetricsSnapshot {
            rows_in: 1,
            rows_out: 10,
            bytes: u64::MAX,
            elapsed_ns: 100,
        };
        a.merge(&b);
        assert_eq!(a.rows_in, u64::MAX);
        assert_eq!(a.rows_out, u64::MAX);
        assert_eq!(a.bytes, u64::MAX);
        assert_eq!(a.elapsed_ns, 200);
    }

    // ── AtomicMetrics ────────────────────────────────────────────────────

    #[test]
    fn atomic_metrics_new_is_zero() {
        let m = AtomicMetrics::new();
        assert_eq!(m.snapshot(), MetricsSnapshot::zero());
    }

    #[test]
    fn atomic_metrics_record_accumulates() {
        let m = AtomicMetrics::new();
        m.record_rows_in(10);
        m.record_rows_in(5);
        m.record_rows_out(3);
        m.record_bytes(128);
        m.record_elapsed_ns(1_234);

        let s = m.snapshot();
        assert_eq!(s.rows_in, 15);
        assert_eq!(s.rows_out, 3);
        assert_eq!(s.bytes, 128);
        assert_eq!(s.elapsed_ns, 1_234);
    }

    #[test]
    fn atomic_metrics_snapshot_does_not_reset() {
        let m = AtomicMetrics::new();
        m.record_rows_in(10);
        let _first = m.snapshot();
        let second = m.snapshot();
        assert_eq!(second.rows_in, 10); // snapshot is a read, not a drain
    }

    #[test]
    fn atomic_metrics_is_thread_safe() {
        // Exercise concurrent increments — the final total must equal the
        // sum of all per-thread contributions without torn writes.
        const THREADS: u64 = 8;
        const PER_THREAD: u64 = 10_000;

        let metrics = Arc::new(AtomicMetrics::new());
        let mut handles = Vec::with_capacity(THREADS as usize);
        for _ in 0..THREADS {
            let m = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    m.record_rows_in(1);
                    m.record_rows_out(2);
                    m.record_bytes(3);
                    m.record_elapsed_ns(4);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let s = metrics.snapshot();
        assert_eq!(s.rows_in, THREADS * PER_THREAD);
        assert_eq!(s.rows_out, 2 * THREADS * PER_THREAD);
        assert_eq!(s.bytes, 3 * THREADS * PER_THREAD);
        assert_eq!(s.elapsed_ns, 4 * THREADS * PER_THREAD);
    }

    // ── NoopMetrics ──────────────────────────────────────────────────────

    #[test]
    fn noop_metrics_snapshot_is_zero() {
        let m = NoopMetrics::new();
        m.record_rows_in(100);
        m.record_rows_out(200);
        m.record_bytes(300);
        m.record_elapsed_ns(400);
        assert_eq!(m.snapshot(), MetricsSnapshot::zero());
    }

    #[test]
    fn noop_metrics_is_default() {
        let a = NoopMetrics::new();
        let b = NoopMetrics;
        // Both discard writes equally.
        a.record_rows_in(1);
        b.record_rows_in(1);
        assert_eq!(a.snapshot(), b.snapshot());
    }

    // ── MetricsTimer ─────────────────────────────────────────────────────

    #[test]
    fn metrics_timer_records_on_drop() {
        let metrics = AtomicMetrics::new();
        {
            let _timer = MetricsTimer::new(&metrics);
            // Spin long enough that the platform monotonic clock reports a
            // non-zero elapsed — a no-op call pair is sufficient because
            // Instant resolution is nanosecond-scale on every supported
            // platform.
            let mut acc: u64 = 0;
            for i in 0..10_000 {
                acc = acc.wrapping_add(i);
            }
            std::hint::black_box(acc);
        }
        assert!(metrics.snapshot().elapsed_ns > 0);
    }

    #[test]
    fn metrics_timer_noop_is_harmless() {
        let metrics = NoopMetrics::new();
        let _timer = MetricsTimer::new(&metrics);
        drop(_timer);
        // Snapshot still zero.
        assert_eq!(metrics.snapshot(), MetricsSnapshot::zero());
    }

    // ── Trait object coercion ────────────────────────────────────────────

    #[test]
    fn metrics_is_object_safe() {
        let atomic: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
        let noop: Arc<dyn Metrics> = Arc::new(NoopMetrics::new());
        atomic.record_rows_in(5);
        noop.record_rows_in(5);
        assert_eq!(atomic.snapshot().rows_in, 5);
        assert_eq!(noop.snapshot().rows_in, 0);
    }

    #[test]
    fn metrics_timer_via_trait_object() {
        let metrics: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
        {
            let _timer = MetricsTimer::new(&*metrics);
            let mut acc: u64 = 0;
            for i in 0..10_000 {
                acc = acc.wrapping_add(i);
            }
            std::hint::black_box(acc);
        }
        assert!(metrics.snapshot().elapsed_ns > 0);
    }
}
