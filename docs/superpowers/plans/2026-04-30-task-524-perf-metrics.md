# TASK-524 — CPU/skew metrics + `--explain-perf` surface

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax. Each checkpoint must pass `scripts/local-ci.sh` and be reviewed by a code-review subagent before merging to main.

**Goal:** Implement Wave 5-only per-query metrics rows from `docs/design/execution-model.md` §14 — fused-segment selection-vector materializations, morsel skew, worker idle/busy spread, spill bytes, and sampled CPU-cost metrics — and surface them through `bqlite query --explain-perf` without making perf collection mandatory in normal queries.

**Architecture:**
- `QueryMetrics` struct in `bqlite-engine` aggregates per-query totals from `MetricsSnapshot` plus engine-side rows (worker spread, morsels, CPU counters, spill bytes).
- A single `Arc<dyn Metrics>` is shared across the operator tree via `QueryContext`. Operators that already increment `MetricsSnapshot` counters (the fused stateless segment) start contributing to the aggregate immediately; everything else (morsel/skew/CPU) reports zero per the design doc's "report zero until the feature lands" rule.
- `QueryContext::collect_cpu_metrics(true)` opt-in flag toggles the CPU-cost sampling path. The platform integration is a stub that returns zero counters today (`PerfCounters::open_or_disabled`) — wire is in place so the morsel scheduler (TASK-523 follow-on) can plug real counters in without a surface change.
- `bqlite query --explain-perf <bql>` runs the query, drops the row output, and emits the formatted metrics table. EXPLAIN itself stays unchanged.

**Tech Stack:** Rust 2021, Arrow, internal `Metrics` trait surface (`bqlite-core`), engine bind step (`bqlite-engine::bind`), CLI argument parser (`bqlite-cli`).

---

## File Structure

| Crate | File | Action | Responsibility |
|---|---|---|---|
| `bqlite-core` | `src/metrics.rs` | Modify | Add `record_spill_bytes_written` to `Metrics` trait + `AtomicMetrics`; add `spill_bytes_written` field to `MetricsSnapshot`. |
| `bqlite-engine` | `src/perf.rs` | Create | `QueryMetrics`, `WorkerMetricsSnapshot`, `PerfCounters`, derived-metric computation, `format_perf_explain`. |
| `bqlite-engine` | `src/lib.rs` | Modify | Re-export `QueryMetrics`, `WorkerMetricsSnapshot`, `PerfCounters`. |
| `bqlite-engine` | `src/context.rs` | Modify | Add `metrics: Arc<dyn Metrics>` (default `AtomicMetrics`), `collect_cpu_metrics` flag, `record_worker_snapshot`, `take_query_metrics`. |
| `bqlite-engine` | `src/query.rs` | Modify | `ExecutionResult` gains `metrics: QueryMetrics` field; `run_query_inner` snapshots `ctx` metrics into `ExecutionResult`. |
| `bqlite-engine` | `src/bind.rs` | Modify | Replace `NoopMetrics::new()` in fused-segment bind with `ctx.metrics().clone()`. |
| `bqlite-cli` | `src/main.rs` | Modify | Add `--explain-perf` flag to `query` subcommand; render perf footer when set. |
| `docs/design` | `execution-model.md` | Modify | Reconcile §14.1 / §14.3 with field names actually shipped. |

---

## Self-Review notes (post-review revisions)

- Coverage check: §14 metric rows mapped onto `QueryMetrics` fields. Morsel/CPU rows present but zero — design doc explicitly allows.
- Single-shared-metrics decision: simpler than per-operator collection because the engine is single-threaded today; the per-worker shape sits behind `WorkerMetricsSnapshot` so the morsel-scheduler task that lands real workers folds in via `record_worker_snapshot` without touching this surface.
- Spill bytes: `TempSpillFile::record_bytes_written` already exists; CP1 wires a `Metrics::record_spill_bytes_written` flush in `TempSpillFile::Drop` so bytes channel into the per-query total without each operator having to remember.

**Review-driven revisions (incorporated below):**
- B1: `QueryMetrics::elapsed_ns` renamed to `wall_clock_ns` to avoid collision with the operator-side `MetricsSnapshot::elapsed_ns` that already aggregates per-operator wall time.
- B2: `take_query_metrics` is non-draining (`snapshot()` is read-only); CP3 explicitly drops the operator tree before reading the snapshot to ensure no clones are still publishing.
- B4: `--explain-perf` test asserts on the *positive* presence of section headers and the absence of a row-table heading line (e.g. `(0 rows)`). CP1 explicitly adds the `TempSpillFile::Drop` flush hook with a unit test.

---

## Checkpoint 1 — Snapshot & trait surface + spill flush hook

**Files:**
- Modify: `crates/bqlite-core/src/metrics.rs`
- Modify: `crates/bqlite-core/src/spill.rs` (`TempSpillFile::Drop` flush)

- [ ] **Step 1.1: Add `spill_bytes_written` field + plumbing**

Add field to `MetricsSnapshot`, include in `merge`, `is_zero`, and the snapshot constructor; add `record_spill_bytes_written` method to `Metrics` trait (default no-op body) and to `AtomicMetrics`.

- [ ] **Step 1.2: Tests for `MetricsSnapshot`**

Mirror existing copy-budget / segment-counter tests: `snapshot_is_zero_includes_spill_counter`, `snapshot_merge_sums_spill_counter`, `atomic_metrics_spill_counter_accumulates`, `noop_metrics_default_body_accepts_spill_writes`.

- [ ] **Step 1.3: Wire `TempSpillFile::Drop` to flush bytes**

Extend `TempSpillFile` with an optional `metrics: Option<Arc<dyn Metrics>>` handle and a constructor that accepts it. On `Drop`, if set, call `metrics.record_spill_bytes_written(self.bytes_written)`.

- [ ] **Step 1.4: Tests for spill flush hook**

```rust
#[test]
fn spill_drop_flushes_bytes_to_metrics() {
    let metrics: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
    {
        let mut guard = TempSpillFile::for_test_with_metrics(Arc::clone(&metrics));
        guard.record_bytes_written(2048);
    }
    assert_eq!(metrics.snapshot().spill_bytes_written, 2048);
}

#[test]
fn spill_drop_without_metrics_is_a_noop() { /* default-constructed guard does not panic */ }
```

(Use whatever `for_test_with_metrics` shape best fits the existing `TempSpillFile` test scaffolding — match its constructor signature.)

- [ ] **Step 1.5: Run local-ci**

```
scripts/local-ci.sh
```

Expected: pass.

- [ ] **Step 1.6: Subagent review**

Spawn a code-review subagent over the staged diff. Must approve with no blocking issues.

- [ ] **Step 1.7: Commit + merge to main**

Commit message: `TASK-524: Add spill_bytes_written counter and TempSpillFile flush hook`.

---

## Checkpoint 2 — `QueryMetrics`, `PerfCounters`, perf module

**Files:**
- Create: `crates/bqlite-engine/src/perf.rs`
- Modify: `crates/bqlite-engine/src/lib.rs` (re-exports)

- [ ] **Step 2.1: Create `perf.rs` with `QueryMetrics` shape**

```rust
//! Per-query metrics aggregation and `--explain-perf` rendering.
//!
//! Implements the per-query rows from
//! `docs/design/execution-model.md` §14. The struct is a plain-data
//! aggregation surface populated by the engine at query teardown
//! (operator-tree counters via [`MetricsSnapshot`], plus engine-side
//! rows: worker spread, morsels dispatched, optional CPU counters).
//!
//! Wave 5 ships:
//! - all throughput / shape rows (live as soon as operators wire counters)
//! - selection_vector_materializations (already wired by TASK-518)
//! - spill_bytes_written (TASK-524 CP1)
//! - morsel/skew/worker rows — present as fields, all-zero until the
//!   morsel scheduler lands (TASK-523 follow-up)
//! - CPU-cost rows — present as fields, all-zero unless
//!   `QueryContext::collect_cpu_metrics(true)` and the platform
//!   integration plugs real counters in.

use bqlite_core::metrics::MetricsSnapshot;

/// Per-(worker, shard) sampling slot the morsel scheduler folds into
/// `QueryMetrics::worker_*` aggregates at query completion. A single-
/// threaded driver records exactly one of these.
#[derive(Debug, Clone, Copy, Default)]
pub struct WorkerMetricsSnapshot {
    pub worker_idle_ns: u64,
    pub worker_busy_ns: u64,
    pub entity_event_skew_p99: u64,
    pub morsels_dispatched: u64,
    pub branch_misses: u64,
    pub llc_misses: u64,
    pub total_cpu_cycles: u64,
    pub events_processed: u64,
}

/// Aggregated per-query metrics, attached to `ExecutionResult` and
/// rendered by `bqlite query --explain-perf`.
#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    pub operator: MetricsSnapshot,
    pub worker_idle_ns_p50: u64,
    pub worker_idle_ns_p99: u64,
    pub worker_busy_ns_max: u64,
    pub worker_busy_ns_min: u64,
    pub entity_event_skew_p99: u64,
    pub morsels_dispatched: u64,
    pub morsels_per_shard_max: u64,
    pub morsels_per_shard_min: u64,
    pub branch_misses: u64,
    pub llc_misses: u64,
    pub total_cpu_cycles: u64,
    pub events_processed: u64,
    pub compaction_active_ns: u64,
    /// Wall-clock duration of the entire query (Engine::query call).
    /// Distinct from `operator.elapsed_ns`, which sums per-operator
    /// wall-time for the operator tree.
    pub wall_clock_ns: u64,
    pub num_workers: u32,
    pub cpu_metrics_enabled: bool,
}

impl QueryMetrics {
    /// Fold a single `WorkerMetricsSnapshot` into the running
    /// aggregates. Idempotent for an empty snapshot.
    pub fn record_worker(&mut self, snap: &WorkerMetricsSnapshot) { /* ... */ }

    /// `bytes_scanned_total / wall_clock_ns / num_cores`. Returns
    /// `None` when `wall_clock_ns` or `num_workers` are zero.
    pub fn gb_per_sec_scanned(&self) -> Option<f64> { /* ... */ }

    /// `total_cpu_cycles / events_processed`. Returns `None` when
    /// `events_processed` is zero or CPU counters disabled.
    pub fn cycles_per_event(&self) -> Option<f64> { /* ... */ }

    /// `bytes_decoded / bytes_scanned` from the operator snapshot.
    pub fn bytes_decoded_to_scanned(&self) -> Option<f64> { /* ... */ }
}

/// Stub platform integration for `perf_event_open` / `kpc`. The Wave 5
/// surface lands the seam; concrete platform code lands with the
/// morsel scheduler. `open_or_disabled` always returns the disabled
/// variant today.
#[derive(Debug, Default)]
pub struct PerfCounters {
    enabled: bool,
}

impl PerfCounters {
    /// Open a perf-event group for the current worker, or return a
    /// disabled placeholder when the platform / build does not
    /// support it. Today every platform returns disabled.
    pub fn open_or_disabled() -> Self {
        Self { enabled: false }
    }

    pub fn is_enabled(&self) -> bool { self.enabled }

    /// Read counters into `out`. No-op when disabled.
    pub fn read_into(&self, _out: &mut WorkerMetricsSnapshot) {}
}

/// Format `metrics` as the human-readable footer printed by
/// `bqlite query --explain-perf`.
pub fn format_perf_explain(metrics: &QueryMetrics) -> String { /* ... */ }
```

Implement all bodies. `record_worker` updates min/max/p50/p99 (worker_idle uses two samples ⇒ p50 = min, p99 = max for a single-worker driver). `format_perf_explain` produces a labelled multi-line block grouped by section: throughput / CPU / skew / spill.

- [ ] **Step 2.2: Tests in `perf.rs`**

```rust
#[test]
fn record_worker_updates_min_max() { /* min/max correct after two snaps */ }

#[test]
fn derived_metrics_return_none_when_inputs_zero() { /* gb_per_sec, cycles_per_event */ }

#[test]
fn format_perf_explain_includes_every_section() {
    let m = QueryMetrics::default();
    let out = format_perf_explain(&m);
    for header in ["Throughput", "Skew", "Spill"] {
        assert!(out.contains(header), "missing section {header}: {out}");
    }
}

#[test]
fn perf_counters_open_returns_disabled_today() {
    assert!(!PerfCounters::open_or_disabled().is_enabled());
}
```

- [ ] **Step 2.3: Re-export from `lib.rs`**

```rust
pub mod perf;
pub use perf::{format_perf_explain, PerfCounters, QueryMetrics, WorkerMetricsSnapshot};
```

- [ ] **Step 2.4: local-ci + review + merge**

Commit message: `TASK-524: Add QueryMetrics aggregation surface in bqlite-engine`.

---

## Checkpoint 3 — Wire `Arc<dyn Metrics>` through `QueryContext` + `ExecutionResult`

**Files:**
- Modify: `crates/bqlite-engine/src/context.rs`
- Modify: `crates/bqlite-engine/src/query.rs`
- Modify: `crates/bqlite-engine/src/bind.rs`

- [ ] **Step 3.1: Extend `QueryContext`**

Add field `metrics: Arc<dyn bqlite_core::metrics::Metrics>` defaulting to `Arc::new(AtomicMetrics::new())`. Add `collect_cpu_metrics: bool` field. Public methods:

```rust
pub fn metrics(&self) -> &Arc<dyn bqlite_core::metrics::Metrics> { &self.metrics }

pub fn collect_cpu_metrics(mut self, enabled: bool) -> Self {
    self.collect_cpu_metrics = enabled;
    self
}

pub fn cpu_metrics_enabled(&self) -> bool { self.collect_cpu_metrics }

/// Record a worker's contribution to the query-level metrics. The
/// single-threaded driver calls this exactly once at query teardown.
pub fn record_worker_snapshot(&self, snap: WorkerMetricsSnapshot) {
    let mut g = self.worker_aggregate.lock().expect("worker aggregate poisoned");
    g.record_worker(&snap);
    g.num_workers = g.num_workers.saturating_add(1);
}

/// Drain the query-level metrics, folding the operator snapshot in.
/// Called at `Engine::query` teardown.
pub fn take_query_metrics(&self, elapsed_ns: u64) -> QueryMetrics { /* ... */ }
```

Internally hold `worker_aggregate: Arc<Mutex<QueryMetrics>>` for cross-worker folding (today single-thread).

- [ ] **Step 3.2: `bind_fused_segment` uses `ctx.metrics()`**

Replace the `NoopMetrics` line with `let metrics = ctx.metrics().clone();`. Drop the comment about the wiring being deferred.

- [ ] **Step 3.3: `ExecutionResult` gains `metrics: QueryMetrics`**

Add the field. Update every constructor site (the `run_query_inner` happy path, `execute_explain_statement`, `execute_delete_statement`, and any test helpers). DELETE / EXPLAIN return `QueryMetrics::default()` since they bypass the drive loop.

- [ ] **Step 3.4: Snapshot wiring in `run_query_inner`**

```rust
let start = std::time::Instant::now();
// ... existing pipeline ...
// Drop operator tree FIRST so adapter clones of the metrics handle are
// released before we read the aggregate. Same ordering rule the
// warning-sink drain follows.
drop(operator);
let wall_clock_ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
let metrics = ctx.take_query_metrics(wall_clock_ns);
Ok(ExecutionResult { schema, rows, rows_affected: None, peak_memory_bytes: ctx.peak_memory_bytes(), warnings: ..., metrics })
```

`take_query_metrics` is non-draining: it calls `Arc::<dyn Metrics>::snapshot()` (a read, not a move) and copies into `QueryMetrics::operator`, then folds the worker aggregate behind a `Mutex`, stamps `wall_clock_ns` / `cpu_metrics_enabled`. Calling it twice on the same context is safe and produces the same value.

- [ ] **Step 3.5: Engine tests**

Add to `query.rs` tests:
- `query_reports_zero_metrics_on_empty_table` — every counter zero, `wall_clock_ns > 0`.
- `query_records_wall_clock_and_num_workers` — assert `metrics.num_workers == 1` (single-threaded driver records exactly one worker snapshot at teardown) and `metrics.wall_clock_ns > 0`.
- `query_with_cpu_metrics_enabled_propagates_flag` — call `Engine::query_with_options` with `collect_cpu_metrics: true`, assert `result.metrics.cpu_metrics_enabled == true`.

Skip a "selection_vector_materializations > 0" test today: the optimizer flip that routes through the fused segment lands in TASK-519 and the assertion would be flaky against the current planner shape. The metric path is exercised end-to-end by CP4's CLI test (asserting the section renders) and by the unit test on `format_perf_explain`.

- [ ] **Step 3.6: local-ci + review + merge**

Commit message: `TASK-524: Thread per-query metrics aggregate through QueryContext`.

---

## Checkpoint 4 — `bqlite query --explain-perf` CLI surface

**Files:**
- Modify: `crates/bqlite-cli/src/main.rs`

- [ ] **Step 4.1: Argument parser**

Add `explain_perf: bool` to `QueryArgs`. Accept `--explain-perf` flag (no value). Reject combination with `--no-limit`/`--limit` only if it's awkward — simplest is to allow both: `--explain-perf` discards rows anyway, but the engine still runs to completion. Document the flag in the top-level `USAGE` block.

```rust
"--explain-perf" => {
    if explain_perf { return Err(CliError::Usage("--explain-perf specified more than once".into())); }
    explain_perf = true;
    i += 1;
}
```

- [ ] **Step 4.2: `run_query` branch**

When `parsed.explain_perf` is true:
1. Skip auto-limit injection (the user wants the full pipeline run, but only the perf footer rendered).
2. Set `QueryOptions { collect_cpu_metrics: true, .. }` — *this requires extending `QueryOptions`*. Add the flag mirroring `memory_budget_bytes`.
3. After `engine.query_with_options`, write `format_perf_explain(&result.metrics)` to `out`. Do not write the row table.

- [ ] **Step 4.3: `QueryOptions::collect_cpu_metrics`**

Already-blessed-as-additive change in `context.rs`:

```rust
pub struct QueryOptions {
    pub memory_budget_bytes: Option<u64>,
    pub collect_cpu_metrics: bool,
}
```

`Engine::query_with_options` calls `ctx.collect_cpu_metrics(options.collect_cpu_metrics)` before threading the context into `run_query_inner`.

- [ ] **Step 4.4: CLI tests**

```rust
#[test]
fn query_with_explain_perf_emits_metrics_footer() {
    let scratch = Scratch::new("explain-perf");
    init_db_with_events(&scratch);
    let db = scratch.path().to_string_lossy().to_string();
    let args = sv(&["query", "events", "--db", &db, "--explain-perf"]);
    let mut out = Vec::new();
    let mut err = Vec::new();
    run(&args, &mut out, &mut err).expect("query --explain-perf must succeed");
    let text = String::from_utf8(out).unwrap();
    for header in ["Throughput", "Skew", "Spill"] {
        assert!(text.contains(header), "perf footer missing {header}: {text}");
    }
    // Row-table output should be suppressed: the CLI's row renderer
    // always emits a `(N rows)` footer for the table path, so its
    // absence is the load-bearing negative assertion.
    assert!(!text.contains("(0 rows)"), "perf-only path must not render row table: {text}");
    assert!(!text.contains("(1 rows)"));
}

#[test]
fn parse_query_args_accepts_explain_perf_flag() { /* mirrors --no-limit test */ }

#[test]
fn parse_query_args_rejects_duplicate_explain_perf() { /* mirrors --no-limit dup */ }
```

- [ ] **Step 4.5: local-ci + review + merge**

Commit message: `TASK-524: Surface per-query perf metrics via bqlite query --explain-perf`.

---

## Checkpoint 5 — Documentation reconciliation

**Files:**
- Modify: `docs/design/execution-model.md`

- [ ] **Step 5.1: Reconcile §14**

Adjust §14.1 / §14.3 prose to match the shipped surface: replace
"opt-in via `QueryContext::collect_cpu_metrics`" reference with the now-actual API surface (no shape change since this is what we built). Add a sentence in §14.3 explaining that the platform integration is a stub returning zero today and that the morsel-scheduler follow-up plugs in real `perf_event_open` / `kpc`.

- [ ] **Step 5.2: Cross-reference**

Add to the §14.1 table footer: `--explain-perf` (CLI surface) renders these rows; `QueryContext::collect_cpu_metrics(true)` opts the query into CPU-cost sampling.

- [ ] **Step 5.3: local-ci + commit**

Doc-only change; clippy/test still run. Commit message: `TASK-524: Reconcile execution-model.md §14 with shipped --explain-perf surface`.

---

## Completion

- [ ] **Step C.1: Move lock**

```bash
git mv tasks/active/TASK-524.lock tasks/completed/TASK-524.done
```

- [ ] **Step C.2: Stamp `completed_at`**

Edit the `.done` file: add `"completed_at": "<UTC ISO-8601>"`.

- [ ] **Step C.3: Commit + push**

```bash
git commit -m "TASK-524: completed" && git push origin main
```
