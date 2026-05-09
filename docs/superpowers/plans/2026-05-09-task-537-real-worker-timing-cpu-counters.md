# TASK-537 — Real worker timing + CPU counter integration

**Date**: 2026-05-09
**Owner**: agent-1
**Branch**: `task/TASK-537`
**Refs**: TASK-524 closure; `docs/design/execution-model.md` §14, `docs/design/engine/morsel-scheduler.md` §8.

## Goal

Make the `--explain-perf` rows from TASK-524 carry real values rather than zeroed placeholders. After this task lands:

- `worker_busy_ns_min/_max` reflects per-morsel `Instant::now()` deltas summed per-worker.
- `worker_idle_ns_p50/_p99` reflects time spent in `pop_or_park` per worker.
- `entity_event_skew_p99` reflects the per-worker p99-vs-p50 spread of per-morsel processed-event counts.
- `branch_misses` / `llc_misses` / `total_cpu_cycles` are read from `perf_event_open` on Linux when `CAP_PERFMON` is granted, and gracefully disabled (with clear CLI labelling) otherwise.
- The `morsel_skew` bench asserts on `entity_event_skew_p99 > 0` and a sane `worker_idle_ns_p50` bound.

## Out of scope

- DDSketch-based merge across workers (per-worker p99 / per-query max is enough for v1; sketches can land later).
- macOS `kpc` integration (gated behind `cfg(target_os = "macos")` with a `not collected` label).
- Sub-shard morsel halving (already noted in design as future work).
- Compaction `compaction_active_ns` polling — separate row, owned by storage compaction work.

## Checkpoints

### CP1 — Per-worker timing + skew in scheduler

Files touched:

- `crates/bqlite-engine/src/scheduler/engine_pool.rs`
- `crates/bqlite-engine/src/scheduler/mod.rs` (re-export `PerWorkerCtx` if needed)
- `crates/bqlite-engine/src/perf.rs` (add helper for percentile of Vec<u64>)
- `crates/bqlite-engine/src/query.rs` (fold per-worker stats into `WorkerMetricsSnapshot`)

Changes:

1. Extend `PerWorkerCtx` with:
   - `worker_busy_ns: u64` — sum of `Instant::now()` deltas across morsels.
   - `worker_idle_ns: u64` — sum across pop waits.
   - `processed_events_per_morsel: Vec<u64>` — one entry per morsel (used for `entity_event_skew_p99`).
   - **Drops `Copy`; keeps `Clone + Default + Debug`.** Construction sites change from
     `PerWorkerCtx { rayon_thread_index, ..PerWorkerCtx::default() }` to use the same syntax —
     the struct-update form still works without `Copy`. The `Mutex<Vec<PerWorkerCtx>>` collector
     continues to work (it owns by value). No other call sites depend on `Copy`.
2. The `work` closure already receives `&mut PerWorkerCtx`. The closure body simply writes
   `ctx_w.processed_events_per_morsel.push(rows as u64)` after running the morsel — no
   wrapper setter, no return-type change. The `Fn + Send + Sync` bound is preserved because
   the closure still does not capture mutable state by reference.
3. In `run_per_shard`, instrument the worker driver loop:
   - Capture `pre_pull = Instant::now()` before `pop_or_park`.
   - On successful pop: `idle_ns += now - pre_pull; pre_busy = now`.
   - After guard drop: `busy_ns += now - pre_busy`.
4. Compute `entity_event_skew_p99` per worker: `p99(events) - p50(events)`, saturating to 0
   when fewer than 2 samples. This is a v1 *per-morsel* spread, not the per-entity sample
   the design doc names. Reconciliation: update `morsel-scheduler.md` §8.2 to record that
   v1 uses per-morsel processed-event counts as a proxy until the entity-operator
   `finish_entity` hook lands; update `execution-model.md` §14.1 in the same checkpoint.
   This matches the literal scope-(b) wording in the TASK-537 description.
5. In `query.rs`, for `run_per_shard_concat` and `run_per_shard_aggregate`:
   - Each closure body counts the rows it produced (concat: `rows.iter().map(|b| b.num_rows()).sum()`;
     aggregate: sum of `batch.num_rows()` across the drive loop) and pushes into
     `ctx_w.processed_events_per_morsel`.
   - After dispatch returns, fold each `PerWorkerCtx` into a `WorkerMetricsSnapshot`
     (compute p99-vs-p50, fill `worker_busy_ns/_idle_ns/morsels_dispatched/entity_event_skew_p99`)
     and call `ctx.record_worker_snapshot(snap)` exactly once per worker that pulled at least
     one morsel.
6. Drop the legacy `for _ in 0..dispatch.num_workers { record_worker_snapshot(default()) }`
   seed in `run_query_inner` for the per-shard paths — those paths now record real
   per-worker snapshots themselves. `SingleTask` keeps the legacy single default snapshot
   because it has no per-worker observations; document this in a brief comment.

Tests added:
- Unit test for `PerWorkerCtx::record_morsel_events` and percentile helper.
- Unit test verifying `worker_busy_ns > 0` when `run_per_shard` actually runs work that takes measurable time (use a `std::thread::sleep` in the test closure).
- Unit test verifying `entity_event_skew_p99 > 0` when the per-morsel events vary.
- The existing `wave5_acceptance::multi_shard_stats_under_floor_budget_matches_hand_computed` continues to pass (the new metrics are additive).

Reconciliation:
- Update `morsel-scheduler.md` §8.2 to record that v1 collapses the DDSketch protocol for
  `worker_idle_ns_p50/_p99` to per-worker totals + cross-worker min/max via the existing
  `QueryMetrics::record_worker` protocol; DDSketch-merged percentiles land when sub-shard
  morsel halving makes per-worker sample counts large enough to need a sketch.
- Update `morsel-scheduler.md` §8.2 row for `entity_event_skew_p99` to record that v1
  reports per-worker p99-vs-p50 spread of *per-morsel processed-event counts*, not
  per-entity event counts; the per-entity sample lands when the `EntityOperatorAdapter`
  exposes a `finish_entity` metrics hook. Mirror this in `execution-model.md` §14.1.

CI gate: `scripts/local-ci.sh` passes; subagent review of staged diff.

### CP2 — Linux `perf_event_open` integration

Files touched:

- `crates/bqlite-engine/Cargo.toml` (add `[target.'cfg(target_os = "linux")'.dependencies] libc = "0.2"`)
- `Cargo.toml` workspace deps (add `libc = "0.2"`)
- `crates/bqlite-engine/src/perf.rs` (replace `PerfCounters::open_or_disabled()` stub)
- New module `crates/bqlite-engine/src/perf/linux.rs` (Linux-only)

Changes:

1. Add `libc = "0.2"` as workspace dep, gated `target_os = "linux"` in the engine crate.
2. New `perf::linux` module:
   - Inline `struct perf_event_attr` (libc provides `libc::perf_event_attr` on Linux ≥ a recent kernel — verify).
   - Open three counters: `PERF_COUNT_HW_CPU_CYCLES`, `PERF_COUNT_HW_BRANCH_MISSES`, `PERF_COUNT_HW_CACHE_LL` (using read-format-group so a single `read` returns all three).
   - Use `libc::syscall(SYS_perf_event_open, &attr, pid=0, cpu=-1, group_fd=-1, flags=0)`.
   - On `EACCES` / `EPERM` (paranoid disabled or `CAP_PERFMON` missing), return disabled.
   - RAII close-on-drop.
3. `PerfCounters::open_or_disabled()` now does `#[cfg(target_os = "linux")] linux::open_or_disabled()` else stub.
4. `read_into` actually reads when enabled; sums into `WorkerMetricsSnapshot::{branch_misses, llc_misses, total_cpu_cycles}`.
5. The per-worker context in `engine_pool::run_per_shard` opens one `PerfCounters` per worker
   thread (lazily on first morsel pull) and reads it twice per morsel — once at `pre_busy`
   immediately after `pop_or_park`, once after the guard drop. The handle owns
   `last_read: [u64; 3]` shadow values so the per-morsel delta is `current - last; last = current`.
   Deltas are added to the running `PerWorkerCtx` totals (`branch_misses` / `llc_misses` /
   `total_cpu_cycles`). The handle's first read at open time seeds `last_read` to zero so the
   first morsel's delta is the running total since `perf_event_open` — correct because the
   counters are reset at open and we want every CPU-cycle count between open and the first
   read attributed to the first morsel's busy span.
6. Wire `cpu_metrics_enabled` flag from `QueryContext` into the worker — `PerfCounters`
   opens only when the flag is set; otherwise the worker holds a `PerfCounters::disabled()`
   handle whose `read_into` is a no-op.
7. Add a `cpu_counters_available` flag (or reuse `PerfCounters::is_enabled` queried at
   read-back time) so the CLI can distinguish "user opted in but kernel refused" from
   "user did not opt in". Cleanest implementation: the worker that successfully opens a
   real `PerfCounters` ORs `true` into a `Mutex<bool>` shared across workers; that bool
   is folded into `QueryMetrics::cpu_counters_available` at finalize. Disabled stub leaves
   the bool at its `false` default.

Tests added:
- Unit test on Linux: `PerfCounters::open_or_disabled()` returns either enabled or disabled — both are valid; the test asserts the call never panics. (CI containers typically lack `CAP_PERFMON` so this exercises the graceful-disable path.)
- Existing tests: `perf_counters_open_returns_disabled_today` is removed; replaced with one that asserts the disabled handle's `read_into` is a no-op (still valid).
- Integration: a test that runs an `--explain-perf`-equivalent query and asserts `cpu_metrics_enabled == true` round-trips through `QueryMetrics`. Whether `total_cpu_cycles > 0` depends on the runner; the test only asserts the flag.

Reconciliation:
- `execution-model.md` §14.3 already documents Wave 5 status. Update its "Wave 5 implementation status" paragraph to reflect that Linux now lands real counters and macOS remains stub.

### CP3 — CLI `--explain-perf` labelling for disabled CPU rows

Files touched:

- `crates/bqlite-engine/src/perf.rs` (`format_perf_explain` rendering)
- `crates/bqlite-cli/src/main.rs` — already calls `format_perf_explain`; no CLI change unless we expose new label.

Changes:

1. When `cpu_metrics_enabled == false`, render CPU rows as `branch_misses              : not collected (cpu metrics disabled)` etc.
2. When `cpu_metrics_enabled == true` but `total_cpu_cycles == 0` *and* the platform integration is the disabled stub (Linux without `CAP_PERFMON`, or macOS), render `not collected (no CAP_PERFMON)` for branch/LLC/cycles. Use a `cpu_counters_available: bool` field on `QueryMetrics` (new) to discriminate, set by the worker when it opens `PerfCounters` and sees enabled vs disabled.
3. Update tests in `perf.rs::tests::format_perf_explain_*` to cover the new labels.

Tests added:
- `format_perf_explain_labels_disabled_cpu_rows` — `cpu_metrics_enabled == false` produces "not collected (cpu metrics disabled)" rows.
- `format_perf_explain_labels_unavailable_perf_counters` — `cpu_metrics_enabled == true` with `cpu_counters_available == false` produces "not collected (no CAP_PERFMON)".

### CP4 — Upgrade `benches/wave5/morsel_skew.rs`

Files touched:

- `benches/wave5/morsel_skew.rs`

Changes:

1. After the probe queries, assert:
   - `result.metrics.entity_event_skew_p99 > 0` on the skewed fixture (the dominant entity should produce a wide spread).
   - `result.metrics.worker_idle_ns_p50` is bounded — define a sane ceiling (e.g. ≤ 1s) so a stuck-on-pop regression fails the bench.
   - The previous `skew_tax_ratio` collector entry remains, with the same target.
2. Update the file-level docstring (lines 5–48) to reflect the new metric assertions and remove the "wall-clock-only tripwire" framing.

Tests:
- Bench is a Criterion target; the assertions run in the `bench_morsel_skew` body before the timed loop.
- Verify `cargo bench --bench wave5_morsel_skew -- --quick` succeeds (or run via `BENCH_MODE=ci` to keep timing tight).

### Completion

- Move `tasks/active/TASK-537.lock` → `tasks/completed/TASK-537.done` with `completed_at`.
- Final commit & push.

## Risk register

- **`perf_event_open` syscall compatibility.** Older Linux kernels lack `PERF_COUNT_HW_CACHE_LL` etc. The graceful-disable path covers this, but we should use the modern `perf_event_attr` size field correctly to avoid `E2BIG`. Use `attr.size = std::mem::size_of::<libc::perf_event_attr>() as u32`.
- **Overhead.** Per-morsel `Instant::now()` is ~tens of ns, well under the 1% per-batch budget at default morsel size.
- **Bench environment variance.** The `worker_idle_ns_p50` ceiling needs headroom — pick
  100 ms (10 × the `pop_or_park` interval × small buffer). Larger ceilings would mask a
  genuine stuck-on-pop bug; smaller ceilings risk flake on noisy CI machines.
- **Vec allocations in `PerWorkerCtx`.** Per-morsel events vec grows linearly with morsel count; today v1 emits one per shard, so the vec is ≤ 32 entries. Reuse via `SmallVec<[u64; 32]>` if hot.

## Decision points captured up front

- **Where p99/p50 live.** Per-worker `WorkerMetricsSnapshot::entity_event_skew_p99` carries the per-worker spread; the cross-worker `QueryMetrics::entity_event_skew_p99` is the running max. This matches the existing `record_worker` protocol — no API shape change.
- **Why no DDSketch.** v1 morsel count is small (≤ 32 per shard); a sort-and-pick percentile is O(n log n) on tiny n and avoids a new dep. DDSketch lands when sub-shard halving turns morsel count into the hundreds-thousands.
- **`libc` vs `perf-event` crate.** `libc` is the standard low-level dep with the stable `perf_event_attr` definition; the `perf-event` crate adds a higher-level Rust API but more code surface. We need ~30 lines, so `libc` direct is the lighter choice.
