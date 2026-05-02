# TASK-526 — Wave 5 benchmark suite + regression-gate refresh

**Branch:** `task/TASK-526`
**Author:** agent-1
**Date:** 2026-05-02
**Output:** `benches/wave5/`

## 1. Scope

The task description (TASKS.md §Wave 5):

> Add benchmark groups and CI baselines for the new execution path:
> zero-copy scan/filter copy budget, fused stateless segment, fused
> stateful-to-aggregate fusion, morsel-scheduler skew behavior, spill
> overhead, and cohort pushdown savings. **Extends the existing bench
> gate** rather than creating a one-off suite.

So this is mostly *new bench files under `benches/wave5/`* plus
`[[bench]]` registrations in `benches/Cargo.toml`. The Wave 2 fused
segment microbenches (`benches/wave2/fused_segment.rs`) already cover
the in-memory `FusedStatelessSegment` driver — we won't duplicate
that. Wave 5's new ground:

| Area | Bench file | What it measures |
|---|---|---|
| Zero-copy scan/filter copy budget | `zero_copy_scan.rs` | `bytes_decompressed` / `bytes_materialized_before_filter` per `bytes_scanned` on `ScanOperator` over a written segment, dictionary + LZ4 column profiles |
| Stateful-to-aggregate fusion | `stateful_aggregate_fusion.rs` | Throughput of `Sessionize/EventSelect/Attribute → HashAggregate` vs the partial-aggregate handoff path (TASK-520) |
| Morsel-scheduler skew | `morsel_skew.rs` | End-to-end `Engine::query` wall-clock under a deliberately skewed entity-event distribution (1 dominant entity, long tail) vs balanced |
| Spill overhead | `spill_overhead.rs` | `SortOperator::with_spill` throughput under an oversubscribed memory budget (forces spill) vs `SortOperator::new` headroom-fits-in-memory baseline |
| Cohort pushdown savings | `cohort_pushdown.rs` | `bytes_scanned` and rows produced by an `IN (cohort)` query when the cohort fits the pushdown gate (< 65,536) vs when it doesn't |

The regression gate (`scripts/bench-compare.sh`) is generic — it picks
up any `target/criterion/<group>/<fn>/new/estimates.json` Criterion
writes, plus any `target/bench-results.json` rows. So the "refresh" is
just: add hard targets through `BenchResultCollector::record(...)`
where the design docs pin a number, and document them in
`benches/wave5/README.md` so the matrix is auditable.

We do **not** add net-new gate machinery: the existing Wave 4 README
documents the contract and the script already enforces both the
Criterion +10% rule and the JSON hard-target rule. TASK-526 just plugs
the new metrics in.

## 2. Reconciliation against design docs

Each bench cites the relevant design-doc section. Targets fall into
two categories per Wave 4 conventions:

- **[spec]** — pinned numerically in a design doc. Revising the target
  requires updating the doc in the same checkpoint.
- **[floor]** — chosen by this bench suite as a regression tripwire.

Pinned numerical targets in Wave 5 design docs:

| Target | Source |
|---|---|
| `bytes_materialized_before_filter == 0` on uncompressed dict / RLE / constant scan | `zero-copy-scan-filter.md` §3 (also `metrics.rs` doc comment) |
| `bytes_decompressed == payload_bytes` on LZ4-wrapped scan | same |
| Cohort pushdown gate: `cohort.len() < 65_536` | `cohort_pushdown.rs` const + `optimizer-direction.md` §7 row 9 (the *gate*, not a savings ratio) |
| Sparsity short-circuit: `<= 10%` materialization trigger | `operator-fusion.md` §3.4 (already covered by `wave2/fused_segment.rs`) |
| Sort spill: must drop on cancellation/timeout | `spill.md` §6.1 (correctness — covered by tests, not benches) |

No design-doc *numerical* floor exists for: stateful-to-aggregate
fusion throughput, morsel-skew wall-clock variance, sort-spill
throughput tax, or cohort pushdown bytes-savings ratio
(`optimizer-direction.md` §7 row 9 makes a *qualitative* claim only —
"a non-trivial fraction of row groups must be skippable"). All of
these get **[floor]** tripwires picked by this suite. If a future
task sets a spec target, the floor row is replaced.

No design-doc edits are required for this task: every metric we
publish is already documented in the cited doc, and we only add the
bench-side measurements. If a measurement reveals a doc gap we'll
amend the doc in the same checkpoint, but I do not anticipate one.

## 3. Decomposition into checkpoints

Each checkpoint lands a complete bench file (compiles, runs in CI
mode, panics in reference mode if the target is missed) plus its
`[[bench]]` registration. The README is updated incrementally so the
"single source for `(file → metric → target)`" contract is preserved
on every commit.

### CP1 — scaffolding + zero-copy scan/filter copy budget bench

- New file `benches/wave5/zero_copy_scan.rs`. One Criterion group
  `wave5/zero_copy_scan/` with two functions:
  - `low_card_dict/copy_budget` — write a segment whose `event_type`
    column lands as Dictionary-encoded, scan with a predicate that
    selects ~10% of rows, assert `bytes_materialized_before_filter ==
    0` post-iteration. Reports `bytes_materialized_before_filter /
    bytes_scanned` ratio with a `[spec]` target of `≤ 0.0`
    (effectively `== 0`).
  - `lz4_payload/decompress_ratio` — write a segment with a
    high-cardinality string column that lands as Plain+LZ4. Scan with
    a single-column projection. Reports
    `bytes_decompressed / bytes_scanned` ratio. `[floor]` target `≥
    1.0` (one full payload-sized decompression copy is expected and
    acceptable; no second materialization copy).
- Wire `BenchResultCollector` results so `target/bench-results.json`
  picks them up.
- Add `benches/wave5/` directory + `benches/wave5/README.md`. The
  README is seeded with the full Wave 4-style structure on day one:
  - Coverage matrix table (CP1 row populated; CP2–CP5 rows added as
    they land — each follow-up checkpoint fills its own row).
  - "Reference-machine targets" table (same incremental shape).
  - "Not covered (intentional)" section seeded immediately with:
    *"In-memory `FusedStatelessSegment` driver — covered by
    `benches/wave2/fused_segment.rs` (TASK-519, §7.2). The Wave 5
    fusion bench (`stateful_aggregate_fusion.rs`) measures the
    aggregate-handoff path landed by TASK-520, which is the only
    new fusion surface introduced this wave."*
  - "Parent docs" section.
- `[[bench]]` entry in `benches/Cargo.toml`.

### CP2 — stateful-to-aggregate fusion bench

- New file `benches/wave5/stateful_aggregate_fusion.rs`. Group
  `wave5/stateful_aggregate_fusion/` with sub-bench:
  - `sessionize_to_count/throughput` — drives a
    `SessionizeOperator → HashAggregateOperator` chain over an
    in-memory pre-built `RecordBatch` of N entities × M events.
    Reports rows / sec.
- This is the lighter-weight of the two TASK-520 paths to bench
  cleanly without engine-side bind. EventSelect and Attribute fusion
  are *correctness-tested* in Wave 4 benches already (`wave4/`
  attribute / event_select); the Wave 5 bench focuses on the
  *aggregate boundary* throughput, which is the new TASK-520 path.
- `[floor]` target on `rows_per_sec` chosen from a dry-run baseline
  measurement. Documented inline as a regression tripwire.
- Update `benches/wave5/README.md` matrix.
- `[[bench]]` entry.

### CP3 — morsel scheduler skew bench

- New file `benches/wave5/morsel_skew.rs`. Group `wave5/morsel_skew/`
  with two functions:
  - `balanced/throughput` — N entities × M events each, run a
    representative analytical query through `Engine::query`. Reports
    wall-clock + `bytes_scanned`.
  - `skewed/throughput` — same total event count but 70% of events
    on a single entity, 30% spread across the long tail. Reports
    wall-clock + `bytes_scanned`. The Criterion comparison surfaces
    the skew tax; the regression gate catches future regressions.
- **Why this isn't a `entity_event_skew_p99` assertion bench.**
  TASK-526's prerequisites include TASK-523 / TASK-524, both of
  which landed (see `tasks/completed/`). Their v1 surface, however,
  is what `crates/bqlite-engine/src/perf.rs` §"Wave 5 scope" line
  21–25 already documents: *"Morsel / skew / worker rows — present
  as fields, all-zero today. They become non-zero once the morsel
  scheduler (TASK-523 follow-up) records per-worker snapshots
  through `QueryContext::record_worker_snapshot`."* The single-task
  driver in `query.rs:487` records exactly one
  `WorkerMetricsSnapshot::default()` per query — by design, not by
  oversight. So today's morsel-skew bench is a wall-clock
  regression tripwire, not a metric-assertion bench. Document this
  inline; when the per-worker sampling lands, this bench upgrades.
- **Sanity row.** Both functions also report `bytes_scanned` so the
  wall-clock comparison can be cross-checked: balanced and skewed
  fixtures generate the same total event count and therefore should
  produce near-identical `bytes_scanned`. A regression in
  scan/filter that disproportionately hits the dominant entity
  shows up as a `bytes_scanned` divergence rather than a
  scheduler-side wall-clock blowup.
- `[floor]` target: `skewed/throughput` wall-clock should not exceed
  `balanced/throughput * 4`. Picked as a tripwire — a >4× skew tax
  signals a morsel-generation regression. The 4× number is calibrated
  off a baseline measurement at the start of CP3 (per §4 protocol);
  if the dry-run shows the steady-state ratio is already close to
  4×, the floor lifts to keep ~1.5× headroom.
- README + `[[bench]]` entry.

### CP4 — spill overhead bench

- New file `benches/wave5/spill_overhead.rs`. Group
  `wave5/spill_overhead/` with two functions:
  - `sort_no_spill/throughput` — `SortOperator::new` over an
    in-memory N-row batch, generous max-rows budget. Baseline.
  - `sort_with_spill/throughput` — `SortOperator::with_spill` with a
    `MemoryBudget` whose limit is below the input's working-set
    size, plus a `SpillFs` rooted at a `ScratchDir`. Reports
    `spill_bytes_written` (via a snapshot of `AtomicMetrics` after
    the run) and rows / sec.
- **Memory-budget construction.** The benches crate constructs the
  budget directly via `bqlite_core::memory::MemoryTracker::new(N)`
  (already `pub`, see `crates/bqlite-core/src/memory.rs:304`). No
  engine wiring needed — the `SortOperator::with_spill` constructor
  accepts an `Arc<dyn MemoryBudget>` straight through.
- The bench asserts `spill_bytes_written > 0` in the spill case
  (correctness-of-the-bench guard — if the budget is set too
  generously and spill never fires, the bench is meaningless and
  should fail loudly) but uses Criterion's timing for the regression
  signal.
- `[floor]` target: `sort_with_spill/throughput` ≥ `sort_no_spill /
  3`. **Pure tripwire** — `engine/spill.md` does not pin a numerical
  throughput tax (verified — §10.3 covers `try_reserve` semantics, not
  throughput), so this is a `[floor]` regression guard, not a `[spec]`
  derivation. The 3× headroom matches Wave 4's tripwire discipline
  and is calibrated off a CP4-start dry-run measurement.
- README + `[[bench]]` entry.

### CP5 — cohort pushdown savings bench + README finalisation + completion

- New file `benches/wave5/cohort_pushdown.rs`. Group
  `wave5/cohort_pushdown/` with two functions:
  - `pushdown_eligible/bytes_scanned` — issue an `Engine::query`
    with an `IN (SELECT ...)` whose cohort materialises to ~1024
    entities (well under the 65,536 gate). Reports
    `result.metrics.operator.bytes_scanned`.
  - `pushdown_disabled/bytes_scanned` — same query shape but a
    cohort that exceeds the 65,536 gate. Reports same.
- The savings ratio
  `pushdown_eligible.bytes_scanned / pushdown_disabled.bytes_scanned`
  is the headline metric. **`[floor]` target**: ratio `≤ 0.5` —
  pushdown reduces scanned bytes by at least 2× when eligible.
  `optimizer-direction.md` §7 row 9 says only that "a non-trivial
  fraction of row groups must be skippable" — qualitative, not a
  pinned 2× number — so this is a regression tripwire, not a `[spec]`
  derivation. The 2× headroom is calibrated against a CP5-start
  dry-run measurement on the chosen fixture; if pushdown's *real*
  steady-state savings on the fixture are <4× the floor, lift the
  floor to keep 1.5× headroom.
- **Prerequisite check at CP5 start**: the cohort-pushdown engine
  path (TASK-522) is on `main` already (`tasks/completed/TASK-522.done`),
  so both query branches are end-to-end-runnable. CP5 confirms by
  running the bench once before pinning targets — if the
  measurement comes back as `≈ 1.0` ratio, that means the engine
  isn't actually applying the pushdown for the fixture and the
  bench needs to use a different query shape (or surface a real
  bug worth a `[NEEDS INPUT]` ticket).
- Finalise `benches/wave5/README.md` with the full coverage matrix,
  reference-machine targets table, and "Not covered (intentional)"
  section.
- Move `tasks/active/TASK-526.lock` →
  `tasks/completed/TASK-526.done`, push to main.

## 4. Per-CP local-ci + review + merge protocol

For each checkpoint:

1. **Implement.** Convention reminders that apply to every bench
   file:
   - All fixture construction (segment writes, batch generation,
     scratch dirs) lives **outside** the `iter_custom` closure.
     Inside the closure: only the measured operator-tree drive.
     Mirrors the Wave 2/3/4 pattern and avoids per-iteration
     allocation noise.
   - One Criterion `benchmark_group` per file with two-tier names
     (`wave5/<area>/<scenario>`).
   - Wrap measured outputs in `criterion::black_box(...)`.
   - Reference-mode size scales through `BenchSizing::for_mode`
     where applicable; bench-local sizing constants live next to
     the bench (and only get hoisted into `common/mod.rs` if a
     second bench reuses them).
   - Each `[floor]` target is calibrated against a dry-run
     measurement at the start of its checkpoint — pick the floor
     to leave ~1.5× headroom over the steady-state observed value.
2. `scripts/local-ci.sh` from a clean working tree.
3. `cargo bench -p bqlite-benches --bench <name> -- --quick` — does
   the bench actually run end-to-end? (CI's `--test` mode runs each
   bench function once; this confirms it.)
4. Spawn a code-review subagent on the staged diff. Block on any
   blocking finding.
5. Commit with `TASK-526: <message>`. Fast-forward merge to main.

## 5. Risks and unknowns

- **Engine-side bench dependency.** CP3 (morsel skew) and CP5 (cohort
  pushdown) drive `Engine::query` end-to-end. If the engine path
  surface I'm planning to use isn't actually accessible to the
  benches crate (e.g. a private item), I'll discover it at build time
  and either expose it or fall back to operator-layer benches. The
  exploration in §1 confirms `Engine`, `Engine::query`,
  `ExecutionResult.metrics.operator.bytes_scanned` are all `pub` from
  `bqlite_engine::*`, and the benches crate already depends on
  `bqlite-engine`.
- **`SortOperator::with_spill` budget wiring.** Resolved during
  planning: `bqlite_core::memory::MemoryTracker::new(N)` is `pub`
  (memory.rs:304) and implements the `MemoryBudget` trait, so CP4
  constructs the budget directly without engine routing.
- **Reference-mode dataset sizing.** All five benches need a
  reference-mode and CI-mode size. The Wave 2/3/4 benches use
  `BenchSizing::for_mode`. Wave 5 sizing constants will live in the
  bench files (or be added to `common/mod.rs`) — small targeted
  additions, no shared-file lockstep concerns.

## 6. Out of scope

- New CI workflow steps. The existing `bench-gate` job already runs
  `scripts/bench-compare.sh` on `target/criterion` + `target/bench-results.json`.
- Wave 5 acceptance gate (TASK-528 owns end-to-end correctness).
- Per-operator benches that already exist in Wave 2/3/4 — we extend,
  not duplicate.
- `entity_event_skew_p99` assertions — gated on a follow-on
  TASK-523/524 surface that populates real per-worker snapshots
  through `record_worker_snapshot`.
