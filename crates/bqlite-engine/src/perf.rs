//! Per-query metrics aggregation and `--explain-perf` rendering.
//!
//! Implements the per-query rows from
//! `docs/design/execution-model.md` §14. The struct is a plain-data
//! aggregation surface populated by the engine at query teardown
//! (operator-tree counters via [`MetricsSnapshot`], plus engine-side
//! rows: worker spread, morsels dispatched, optional CPU counters).
//!
//! ## Wave 5 scope (TASK-524)
//!
//! Wave 5 ships:
//!
//! - All throughput / shape rows that already accumulate into
//!   [`MetricsSnapshot`] (rows, bytes, copy-budget, fused-segment
//!   selection-vector counters). These flow through
//!   [`QueryMetrics::operator`].
//! - `spill_bytes_written` — populated by the
//!   [`bqlite_core::spill::TempSpillFile`] drop hook landed in CP1
//!   when the engine attaches a metrics handle to the spill guard
//!   (CP3).
//! - Morsel / skew / worker rows — present as fields, all-zero today.
//!   They become non-zero once the morsel scheduler (TASK-523
//!   follow-up) records per-worker snapshots through
//!   [`crate::context::QueryContext::record_worker_snapshot`]. This
//!   matches `execution-model.md` §14.1: "Metrics that depend on a
//!   feature not yet shipped report zero until the feature lands."
//! - CPU-cost rows — present as fields, all-zero unless
//!   `QueryContext::collect_cpu_metrics(true)` is set *and* the
//!   platform integration plugs in real counters. The Wave 5 surface
//!   lands the seam ([`PerfCounters::open_or_disabled`]); the
//!   concrete `perf_event_open` / `kpc` integration is a follow-up.
//!
//! ## Surface
//!
//! - [`QueryMetrics`] — flat aggregate. Attached to
//!   [`crate::ExecutionResult`] in CP3.
//! - [`WorkerMetricsSnapshot`] — what a single worker contributes; the
//!   morsel scheduler will hand one of these per worker to
//!   [`QueryMetrics::record_worker`].
//! - [`PerfCounters`] — per-worker CPU counter handle. Stub today.
//! - [`format_perf_explain`] — render the aggregate as a labelled
//!   multi-section text block for `bqlite query --explain-perf`.

use bqlite_core::metrics::MetricsSnapshot;

/// Per-(worker, shard) sampling slot the morsel scheduler folds into
/// the query-level aggregate at query completion.
///
/// Today the single-threaded driver records exactly one of these (in
/// CP3 wiring). The shape matches what one worker can plausibly
/// contribute under `execution-model.md` §14.1; the morsel scheduler
/// follow-up (post-TASK-523) will produce one per worker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WorkerMetricsSnapshot {
    /// Wall-time the worker spent waiting on its morsel queue.
    pub worker_idle_ns: u64,
    /// Wall-time the worker spent actively executing morsels.
    pub worker_busy_ns: u64,
    /// 99th-percentile event count for any entity inside this
    /// worker's morsels. Zero until the scheduler samples it.
    pub entity_event_skew_p99: u64,
    /// Morsels dispatched to this worker.
    pub morsels_dispatched: u64,
    /// Branch-prediction misses (`PERF_COUNT_HW_BRANCH_MISSES`).
    pub branch_misses: u64,
    /// Last-level cache misses.
    pub llc_misses: u64,
    /// CPU cycles consumed by this worker's morsels.
    pub total_cpu_cycles: u64,
    /// Events processed (rows × per-event work) — the denominator for
    /// `cycles_per_event`.
    pub events_processed: u64,
}

/// Aggregated per-query metrics.
///
/// Attached to [`crate::ExecutionResult`] (CP3) and rendered by
/// [`format_perf_explain`] when the query is invoked with
/// `--explain-perf`. Plain data — every field is `Copy`-able and
/// summable, so the struct is cheap to clone and pass through the
/// engine boundary.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryMetrics {
    /// Operator-tree counters summed across every operator in this
    /// query. Populated from the shared `Arc<dyn Metrics>` snapshot.
    pub operator: MetricsSnapshot,

    // ── Worker / scheduler aggregates (fold of WorkerMetricsSnapshot) ──
    /// 50th-percentile worker idle time. For `num_workers == 1`
    /// degenerates to the single observation.
    pub worker_idle_ns_p50: u64,
    /// 99th-percentile worker idle time.
    pub worker_idle_ns_p99: u64,
    /// Highest per-worker total busy time — the "tail worker" signal.
    pub worker_busy_ns_max: u64,
    /// Lowest per-worker total busy time. The spread between
    /// `worker_busy_ns_max` and `_min` is the headline skew signal.
    pub worker_busy_ns_min: u64,
    /// Worst per-worker `entity_event_skew_p99`. Zero until the
    /// scheduler samples per-entity event counts.
    pub entity_event_skew_p99: u64,
    /// Total morsels dispatched across every worker.
    pub morsels_dispatched: u64,
    /// Largest morsel count for any single shard. Zero until the
    /// scheduler enumerates per-shard morsels.
    pub morsels_per_shard_max: u64,
    /// Smallest morsel count for any single shard.
    pub morsels_per_shard_min: u64,

    // ── CPU-cost rows (sampled, opt-in) ──
    /// Branch-prediction miss count from `perf_event_open` when
    /// available; zero on platforms without
    /// `PERF_COUNT_HW_BRANCH_MISSES` or when CPU collection is off.
    pub branch_misses: u64,
    /// Last-level cache miss count.
    pub llc_misses: u64,
    /// Total CPU cycles. Denominator for [`cycles_per_event`].
    pub total_cpu_cycles: u64,
    /// Events processed across all workers — numerator for derived
    /// throughput metrics.
    pub events_processed: u64,

    // ── Compaction interaction ──
    /// Wall time during which any compaction was active in the worker
    /// pool. Zero until the compaction scheduler exposes this through
    /// [`QueryMetrics::record_compaction_active_ns`].
    pub compaction_active_ns: u64,

    // ── Engine-side stamps ──
    /// Wall-clock duration of the entire `Engine::query` call.
    /// Distinct from `operator.elapsed_ns`, which sums per-operator
    /// wall-time.
    pub wall_clock_ns: u64,
    /// Number of workers that contributed a snapshot. The
    /// single-threaded driver always records 1 in CP3; the morsel
    /// scheduler will record `num_cores` once it lands.
    pub num_workers: u32,
    /// Whether CPU-cost sampling was opted into for this query.
    /// `true` only when [`crate::QueryContext::collect_cpu_metrics`]
    /// returned a context with the flag set.
    pub cpu_metrics_enabled: bool,
}

impl QueryMetrics {
    /// Construct a zeroed aggregate. Equivalent to
    /// `QueryMetrics::default()`.
    pub fn zero() -> Self {
        Self::default()
    }

    /// Fold a single [`WorkerMetricsSnapshot`] into the running
    /// aggregate.
    ///
    /// Idle/busy aggregates use a streaming min/max protocol:
    /// `worker_busy_ns_min` / `_max` straddle the per-worker busy
    /// values; `worker_idle_ns_p50` / `_p99` are the running min and
    /// max of idle samples (degenerate to identical values when
    /// `num_workers == 1`, which is the v1 single-threaded shape).
    /// The morsel-scheduler follow-up will switch to a real
    /// percentile estimator once it has more than two samples to
    /// work with.
    pub fn record_worker(&mut self, snap: &WorkerMetricsSnapshot) {
        self.morsels_dispatched = self
            .morsels_dispatched
            .saturating_add(snap.morsels_dispatched);
        self.branch_misses = self.branch_misses.saturating_add(snap.branch_misses);
        self.llc_misses = self.llc_misses.saturating_add(snap.llc_misses);
        self.total_cpu_cycles = self.total_cpu_cycles.saturating_add(snap.total_cpu_cycles);
        self.events_processed = self.events_processed.saturating_add(snap.events_processed);
        self.entity_event_skew_p99 = self.entity_event_skew_p99.max(snap.entity_event_skew_p99);

        if self.num_workers == 0 {
            // First sample: seed every min/max with this snapshot's
            // values rather than the default zeroes (which would make
            // the min stick at 0 forever).
            self.worker_busy_ns_min = snap.worker_busy_ns;
            self.worker_busy_ns_max = snap.worker_busy_ns;
            self.worker_idle_ns_p50 = snap.worker_idle_ns;
            self.worker_idle_ns_p99 = snap.worker_idle_ns;
        } else {
            self.worker_busy_ns_min = self.worker_busy_ns_min.min(snap.worker_busy_ns);
            self.worker_busy_ns_max = self.worker_busy_ns_max.max(snap.worker_busy_ns);
            self.worker_idle_ns_p50 = self.worker_idle_ns_p50.min(snap.worker_idle_ns);
            self.worker_idle_ns_p99 = self.worker_idle_ns_p99.max(snap.worker_idle_ns);
        }
        self.num_workers = self.num_workers.saturating_add(1);
    }

    /// Record per-shard morsel counts after every worker has finished.
    /// Stored as min/max so the rendered output answers "did every
    /// shard get a fair share?" without exposing the full distribution.
    pub fn record_morsels_per_shard(&mut self, counts: &[u64]) {
        if let (Some(&min), Some(&max)) = (counts.iter().min(), counts.iter().max()) {
            self.morsels_per_shard_min = min;
            self.morsels_per_shard_max = max;
        }
    }

    /// Record the wall time during which any compaction overlapped
    /// this query. Zero today; populated by the compaction scheduler
    /// follow-up.
    pub fn record_compaction_active_ns(&mut self, ns: u64) {
        self.compaction_active_ns = self.compaction_active_ns.saturating_add(ns);
    }

    // ── Derived metrics (computed at render time) ────────────────────

    /// `bytes_scanned_total / wall_clock_ns / num_workers` expressed in
    /// GB/s/core. Returns `None` when `wall_clock_ns` or `num_workers`
    /// is zero — the render path treats `None` as "—".
    pub fn gb_per_sec_scanned(&self) -> Option<f64> {
        if self.wall_clock_ns == 0 || self.num_workers == 0 {
            return None;
        }
        let bytes = self.operator.bytes_scanned as f64;
        let secs = self.wall_clock_ns as f64 / 1_000_000_000.0;
        let cores = self.num_workers as f64;
        Some(bytes / secs / cores / (1024.0 * 1024.0 * 1024.0))
    }

    /// `total_cpu_cycles / events_processed`. Returns `None` when
    /// CPU counters are disabled or `events_processed` is zero.
    pub fn cycles_per_event(&self) -> Option<f64> {
        if !self.cpu_metrics_enabled || self.events_processed == 0 {
            return None;
        }
        Some(self.total_cpu_cycles as f64 / self.events_processed as f64)
    }

    /// `bytes_decoded_to_scanned` from the operator snapshot —
    /// `operator.bytes / operator.bytes_scanned`. Lower is better;
    /// well below 1 means late materialization is paying off. Returns
    /// `None` when `bytes_scanned` is zero.
    pub fn bytes_decoded_to_scanned(&self) -> Option<f64> {
        if self.operator.bytes_scanned == 0 {
            return None;
        }
        Some(self.operator.bytes as f64 / self.operator.bytes_scanned as f64)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PerfCounters
// ─────────────────────────────────────────────────────────────────────────────

/// Per-worker CPU counter handle. Wave 5 surface; concrete platform
/// integration (Linux `perf_event_open`, macOS `kpc`) is a follow-up
/// to TASK-524.
///
/// The seam exists so the morsel scheduler can call
/// [`PerfCounters::open_or_disabled`] in a worker's `WorkerContext`
/// constructor without conditional compilation in the scheduler
/// itself. Today every platform returns the disabled variant; counter
/// reads are no-ops.
#[derive(Debug, Default)]
pub struct PerfCounters {
    enabled: bool,
}

impl PerfCounters {
    /// Open a perf-event group for the current worker, or return a
    /// disabled placeholder when the platform / build does not
    /// support it or when the engine has not opted into CPU-cost
    /// metrics. Today every platform returns disabled.
    pub fn open_or_disabled() -> Self {
        Self { enabled: false }
    }

    /// Construct an explicit disabled handle. Used by code paths that
    /// know they will not be sampling.
    pub fn disabled() -> Self {
        Self { enabled: false }
    }

    /// Whether this handle is connected to a real counter source.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Read the underlying counters into `out`. No-op when disabled —
    /// the snapshot's `branch_misses` / `llc_misses` /
    /// `total_cpu_cycles` fields stay at their pre-call value.
    pub fn read_into(&self, _out: &mut WorkerMetricsSnapshot) {
        if !self.enabled {
            // Platform integration lands later; today this is a
            // deliberate no-op so callers can issue the read
            // unconditionally on every morsel boundary.
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Rendering
// ─────────────────────────────────────────────────────────────────────────────

/// Render `metrics` as the human-readable footer printed by
/// `bqlite query --explain-perf`.
///
/// Output is a labelled multi-section text block grouped into
/// `Throughput`, `CPU`, `Skew`, and `Spill` sections. Every section
/// header appears even when its rows are zero — absence-of-row is
/// itself a useful signal ("did the optimizer pick the fused path?")
/// and removing zero rows would force callers to remember which
/// sections exist.
///
/// ## Intentionally elided (Wave 5)
///
/// The `MetricsSnapshot` fields below flow through
/// [`QueryMetrics::operator`] but are not rendered by the v1 footer:
/// `rows_after_pushdown`, `rows_after_filter`, `entities_*`,
/// `segments_scanned`, `segments_pruned`, `row_groups_pruned`,
/// `marks_pruned`. These are addressable through programmatic access
/// to `result.metrics.operator`; later waves promote them into the
/// rendered output once the operators that populate them are wired.
/// `decode_to_operator_ratio` from `execution-model.md` §14.1 is
/// deferred until the §14.3 sampling protocol lands real perf
/// counters — without sampled wall fractions the ratio is undefined.
pub fn format_perf_explain(metrics: &QueryMetrics) -> String {
    let mut out = String::new();
    out.push_str("bqlite query perf metrics\n");
    out.push_str("=========================\n");

    out.push_str("\nThroughput\n");
    out.push_str(&format!(
        "  rows_in                    : {}\n",
        metrics.operator.rows_in
    ));
    out.push_str(&format!(
        "  rows_out                   : {}\n",
        metrics.operator.rows_out
    ));
    out.push_str(&format!(
        "  bytes_scanned              : {}\n",
        metrics.operator.bytes_scanned
    ));
    out.push_str(&format!(
        "  bytes_decoded              : {}\n",
        metrics.operator.bytes
    ));
    out.push_str(&format!(
        "  bytes_decoded_to_scanned   : {}\n",
        format_ratio(metrics.bytes_decoded_to_scanned())
    ));
    out.push_str(&format!(
        "  selection_vector_materializations: {}\n",
        metrics.operator.selection_vector_materializations
    ));
    out.push_str(&format!(
        "  selection_vector_dropped_rows    : {}\n",
        metrics.operator.selection_vector_dropped_rows
    ));
    out.push_str(&format!(
        "  wall_clock_ns              : {}\n",
        metrics.wall_clock_ns
    ));
    out.push_str(&format!(
        "  gb_per_sec_scanned         : {}\n",
        format_ratio(metrics.gb_per_sec_scanned())
    ));

    out.push_str("\nCPU\n");
    out.push_str(&format!(
        "  cpu_metrics_enabled        : {}\n",
        metrics.cpu_metrics_enabled
    ));
    out.push_str(&format!(
        "  total_cpu_cycles           : {}\n",
        metrics.total_cpu_cycles
    ));
    out.push_str(&format!(
        "  events_processed           : {}\n",
        metrics.events_processed
    ));
    out.push_str(&format!(
        "  cycles_per_event           : {}\n",
        format_ratio(metrics.cycles_per_event())
    ));
    out.push_str(&format!(
        "  branch_misses              : {}\n",
        metrics.branch_misses
    ));
    out.push_str(&format!(
        "  llc_misses                 : {}\n",
        metrics.llc_misses
    ));

    out.push_str("\nSkew\n");
    out.push_str(&format!(
        "  num_workers                : {}\n",
        metrics.num_workers
    ));
    out.push_str(&format!(
        "  morsels_dispatched         : {}\n",
        metrics.morsels_dispatched
    ));
    out.push_str(&format!(
        "  morsels_per_shard_min      : {}\n",
        metrics.morsels_per_shard_min
    ));
    out.push_str(&format!(
        "  morsels_per_shard_max      : {}\n",
        metrics.morsels_per_shard_max
    ));
    out.push_str(&format!(
        "  worker_idle_ns_p50         : {}\n",
        metrics.worker_idle_ns_p50
    ));
    out.push_str(&format!(
        "  worker_idle_ns_p99         : {}\n",
        metrics.worker_idle_ns_p99
    ));
    out.push_str(&format!(
        "  worker_busy_ns_min         : {}\n",
        metrics.worker_busy_ns_min
    ));
    out.push_str(&format!(
        "  worker_busy_ns_max         : {}\n",
        metrics.worker_busy_ns_max
    ));
    out.push_str(&format!(
        "  entity_event_skew_p99      : {}\n",
        metrics.entity_event_skew_p99
    ));

    out.push_str("\nSpill\n");
    out.push_str(&format!(
        "  spill_bytes_written        : {}\n",
        metrics.operator.spill_bytes_written
    ));
    out.push_str(&format!(
        "  compaction_active_ns       : {}\n",
        metrics.compaction_active_ns
    ));

    out
}

/// Format an `Option<f64>` ratio with three decimal places, or `—`
/// when the input is `None`.
fn format_ratio(v: Option<f64>) -> String {
    match v {
        Some(x) => format!("{:.3}", x),
        None => "—".to_string(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── QueryMetrics::record_worker ──────────────────────────────────

    #[test]
    fn record_worker_seeds_min_max_on_first_sample() {
        let mut m = QueryMetrics::zero();
        m.record_worker(&WorkerMetricsSnapshot {
            worker_idle_ns: 50,
            worker_busy_ns: 100,
            ..Default::default()
        });
        assert_eq!(m.worker_idle_ns_p50, 50);
        assert_eq!(m.worker_idle_ns_p99, 50);
        assert_eq!(m.worker_busy_ns_min, 100);
        assert_eq!(m.worker_busy_ns_max, 100);
        assert_eq!(m.num_workers, 1);
    }

    #[test]
    fn record_worker_updates_min_max_on_second_sample() {
        let mut m = QueryMetrics::zero();
        m.record_worker(&WorkerMetricsSnapshot {
            worker_idle_ns: 50,
            worker_busy_ns: 100,
            ..Default::default()
        });
        m.record_worker(&WorkerMetricsSnapshot {
            worker_idle_ns: 150,
            worker_busy_ns: 200,
            ..Default::default()
        });
        // P50 = min, P99 = max for the two-sample case.
        assert_eq!(m.worker_idle_ns_p50, 50);
        assert_eq!(m.worker_idle_ns_p99, 150);
        assert_eq!(m.worker_busy_ns_min, 100);
        assert_eq!(m.worker_busy_ns_max, 200);
        assert_eq!(m.num_workers, 2);
    }

    #[test]
    fn record_worker_sums_morsels_and_cpu_counters() {
        let mut m = QueryMetrics::zero();
        m.record_worker(&WorkerMetricsSnapshot {
            morsels_dispatched: 4,
            branch_misses: 10,
            llc_misses: 5,
            total_cpu_cycles: 1_000,
            events_processed: 256,
            ..Default::default()
        });
        m.record_worker(&WorkerMetricsSnapshot {
            morsels_dispatched: 6,
            branch_misses: 2,
            llc_misses: 1,
            total_cpu_cycles: 500,
            events_processed: 128,
            ..Default::default()
        });
        assert_eq!(m.morsels_dispatched, 10);
        assert_eq!(m.branch_misses, 12);
        assert_eq!(m.llc_misses, 6);
        assert_eq!(m.total_cpu_cycles, 1_500);
        assert_eq!(m.events_processed, 384);
    }

    #[test]
    fn record_worker_takes_max_of_entity_event_skew() {
        let mut m = QueryMetrics::zero();
        m.record_worker(&WorkerMetricsSnapshot {
            entity_event_skew_p99: 100,
            ..Default::default()
        });
        m.record_worker(&WorkerMetricsSnapshot {
            entity_event_skew_p99: 250,
            ..Default::default()
        });
        m.record_worker(&WorkerMetricsSnapshot {
            entity_event_skew_p99: 150,
            ..Default::default()
        });
        assert_eq!(m.entity_event_skew_p99, 250);
    }

    #[test]
    fn record_morsels_per_shard_picks_min_max() {
        let mut m = QueryMetrics::zero();
        m.record_morsels_per_shard(&[3, 7, 1, 5]);
        assert_eq!(m.morsels_per_shard_min, 1);
        assert_eq!(m.morsels_per_shard_max, 7);
    }

    #[test]
    fn record_morsels_per_shard_empty_is_noop() {
        let mut m = QueryMetrics::zero();
        m.record_morsels_per_shard(&[]);
        assert_eq!(m.morsels_per_shard_min, 0);
        assert_eq!(m.morsels_per_shard_max, 0);
    }

    // ── Derived metrics ──────────────────────────────────────────────

    #[test]
    fn derived_metrics_return_none_when_inputs_zero() {
        let m = QueryMetrics::zero();
        assert!(m.gb_per_sec_scanned().is_none());
        assert!(m.cycles_per_event().is_none());
        assert!(m.bytes_decoded_to_scanned().is_none());
    }

    #[test]
    fn cycles_per_event_requires_cpu_metrics_enabled() {
        let mut m = QueryMetrics::zero();
        m.total_cpu_cycles = 1_000;
        m.events_processed = 100;
        // Flag off: returns None even with non-zero counters.
        assert!(m.cycles_per_event().is_none());
        m.cpu_metrics_enabled = true;
        assert_eq!(m.cycles_per_event(), Some(10.0));
    }

    #[test]
    fn gb_per_sec_scanned_computes_from_bytes_and_wall_clock() {
        let mut m = QueryMetrics::zero();
        m.operator.bytes_scanned = 1_073_741_824; // 1 GiB
        m.wall_clock_ns = 1_000_000_000; // 1 second
        m.num_workers = 1;
        let v = m.gb_per_sec_scanned().expect("ratio defined");
        assert!((v - 1.0).abs() < 1e-9, "got {v}");
    }

    #[test]
    fn bytes_decoded_to_scanned_below_one_means_late_materialization() {
        let mut m = QueryMetrics::zero();
        m.operator.bytes_scanned = 1000;
        m.operator.bytes = 200;
        let v = m.bytes_decoded_to_scanned().expect("ratio defined");
        assert!((v - 0.2).abs() < 1e-9);
    }

    // ── PerfCounters ─────────────────────────────────────────────────

    #[test]
    fn perf_counters_open_returns_disabled_today() {
        let p = PerfCounters::open_or_disabled();
        assert!(!p.is_enabled());
    }

    #[test]
    fn perf_counters_read_into_disabled_is_noop() {
        let p = PerfCounters::disabled();
        let mut snap = WorkerMetricsSnapshot {
            branch_misses: 99,
            llc_misses: 7,
            total_cpu_cycles: 42,
            ..Default::default()
        };
        p.read_into(&mut snap);
        // Disabled handle leaves the snapshot untouched.
        assert_eq!(snap.branch_misses, 99);
        assert_eq!(snap.llc_misses, 7);
        assert_eq!(snap.total_cpu_cycles, 42);
    }

    // ── format_perf_explain ──────────────────────────────────────────

    #[test]
    fn format_perf_explain_includes_every_section() {
        let m = QueryMetrics::zero();
        let out = format_perf_explain(&m);
        for header in ["Throughput", "CPU", "Skew", "Spill"] {
            assert!(out.contains(header), "missing section {header}: {out}");
        }
    }

    #[test]
    fn format_perf_explain_renders_em_dash_for_undefined_ratios() {
        let m = QueryMetrics::zero();
        let out = format_perf_explain(&m);
        // gb_per_sec_scanned is None when wall_clock_ns is zero.
        assert!(
            out.contains("gb_per_sec_scanned         : —"),
            "expected em-dash for undefined ratio: {out}"
        );
    }

    #[test]
    fn format_perf_explain_renders_concrete_values_when_populated() {
        let mut m = QueryMetrics::zero();
        m.operator.rows_out = 12_345;
        m.operator.bytes_scanned = 1_073_741_824;
        m.wall_clock_ns = 1_000_000_000;
        m.num_workers = 1;
        let out = format_perf_explain(&m);
        assert!(out.contains("rows_out                   : 12345"), "{out}");
        // GB/s/core should be ~1.000 for 1 GiB / 1 second / 1 core.
        assert!(out.contains("gb_per_sec_scanned         : 1.000"), "{out}");
    }

    #[test]
    fn format_perf_explain_surfaces_spill_bytes() {
        let mut m = QueryMetrics::zero();
        m.operator.spill_bytes_written = 4096;
        let out = format_perf_explain(&m);
        assert!(
            out.contains("spill_bytes_written        : 4096"),
            "spill row missing: {out}"
        );
    }
}
