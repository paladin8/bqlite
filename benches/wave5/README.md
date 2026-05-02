# Wave 5 Benchmark Suite (TASK-526)

Criterion benches covering the Wave 5 execution-path story:
zero-copy scan/filter copy budget, fused stateful-to-aggregate
fusion, morsel-scheduler skew behavior, spill overhead, and cohort
pushdown savings. Each bench reports metrics via
`BenchResultCollector` so that `scripts/bench-compare.sh` promotes
its hard targets into the existing CI bench gate (`bench-gate` job in
`.github/workflows/bench.yml`). No new gate machinery ships with
TASK-526 — every Wave 5 bench plugs into the gate the Wave 2 / Wave 4
suites already wired.

This README is the single source for which `(file → metric → target)`
the CI gate enforces. Adding a bench means adding a row below.

## Layout

```
benches/wave5/
├── README.md                        # this file
├── zero_copy_scan.rs                # Zero-copy scan/filter copy budget (CP1)
├── stateful_aggregate_fusion.rs     # Stateful → aggregate fusion throughput (CP2)
├── morsel_skew.rs                   # Morsel scheduler skew wall-clock tripwire (CP3)
├── spill_overhead.rs                # Sort spill overhead (CP4)
└── cohort_pushdown.rs               # Cohort entity-id pushdown savings (CP5)
```

Adding a bench means adding a row to both the *Coverage matrix*
and *Reference-machine targets* tables below.

## Coverage matrix

| TASK-526 area | Bench file | Main API / metric |
|---|---|---|
| Zero-copy scan/filter copy budget | `zero_copy_scan.rs` | `SegmentScan::next_encoded_row_group` + `apply_encoded_eq` + `materialize_selected`; `MetricsSnapshot::{bytes_materialized_before_filter, bytes_decompressed, bytes_scanned}` |
| Stateful → aggregate fusion (TASK-520) | `stateful_aggregate_fusion.rs` | `Engine::query` end-to-end on `SESSIONIZE → STATS`; fused vs unfused wall-clock ratio |
| Morsel scheduler skew (TASK-523/524) | `morsel_skew.rs` | `Engine::query` end-to-end on `STATS COUNT(*) GROUP BY entity_id`; skewed-to-balanced wall-clock ratio |
| Sort spill overhead (TASK-502/513) | `spill_overhead.rs` | `SortOperator::with_spill` vs `SortOperator::new`; spill-tax wall-clock ratio against an in-memory baseline |
| Cohort pushdown savings (TASK-522) | `cohort_pushdown.rs` | `SegmentScan::scan` with vs without a `ScanPredicate { conjuncts: [EntityIn(...)] }`; `bytes_scanned` savings ratio |

## Reference-machine targets

Hard targets fire only under `BQLITE_BENCH_MODE=reference` (on the
pinned Apple M2 Max). CI mode runs the same benches at scaled-down
fixtures and relies on Criterion's statistical-regression gate. Every
target below is the value passed to `BenchResultCollector::record`
and picked up by `scripts/bench-compare.sh`.

Provenance labels:

- **[spec]** — pinned numerically in a design doc. Revising the
  target requires a design-doc change in the same checkpoint.
- **[floor]** — chosen by this bench suite as a regression tripwire.
  Not contractual; revising requires a commit-message note per
  AGENTS.md §5.

| Bench metric | Target | Source |
|---|---|---|
| `wave5/zero_copy_scan/low_card_dict/pre_filter_materialization_ratio` | ≤ 0.0 | **[spec]** `zero-copy-scan-filter.md` §3 + `MetricsSnapshot` doc comment ("`bytes_materialized_before_filter == 0` on uncompressed dictionary / RLE / constant-encoded scan paths") |
| `wave5/zero_copy_scan/lz4_payload/decompress_ratio` | ≥ 1.0 | **[spec]** `zero-copy-scan-filter.md` §3 + `MetricsSnapshot` doc comment ("`bytes_decompressed == payload_bytes` on LZ4-wrapped segments"); the ratio measures `bytes_decompressed / bytes_scanned` |
| `wave5/zero_copy_scan/lz4_payload/pre_filter_materialization_ratio` | ≤ 0.0 | **[spec]** same — pre-filter materialisation must remain zero on the encoded path even when LZ4 fired |
| `wave5/stateful_aggregate_fusion/fusion_speedup_ratio` | ≥ 0.95 | **[floor]** `engine/operator-fusion.md` does not pin a numerical ratio; this is a regression tripwire that catches the fusion pass silently no-op'ing or the inline-accumulator path landing on a slow branch. The 0.95 threshold absorbs CI runner noise around the no-effect point of 1.0. Fused query: `SESSIONIZE → STATS MAX(session_id)`; unfused baseline: `SESSIONIZE → STATS SUM(amount)` |
| `wave5/morsel_skew/skew_tax_ratio` | ≤ 4.0 | **[floor]** `engine/morsel-scheduler.md` does not pin a numerical ratio. v1's single-task driver should produce roughly identical wall-clock between balanced and skewed inputs (per-entity work is sequential anyway); a >4× skew tax signals a morsel-generation regression. The future per-shard scheduler should *narrow* the tax, not widen it |
| `wave5/spill_overhead/spill_tax_ratio` | ≤ 3.0 | **[floor]** `engine/spill.md` does not pin a numerical throughput tax (§10.3 covers `try_reserve` semantics, not throughput); this is a regression tripwire. `sort_with_spill_ns / sort_no_spill_ns ≤ 3.0` keeps headroom for the merger phase's startup overhead at small CI sizing while still catching a regression that pushes the spill drain into a slow branch |
| `wave5/cohort_pushdown/bytes_scanned_savings_ratio` | ≤ 0.5 | **[floor]** `optimizer-direction.md` §7 row 9 makes only a qualitative claim ("a non-trivial fraction of row groups must be skippable"), so this is a regression tripwire. The fixture (single-entity cohort, 100-row row groups) gives the gate its best case — observed ratio ≈ 0.002 in CI mode. A regression that turns the `EntityIn` conjunct into a no-op surfaces as a ratio close to 1.0 |

### Not covered (intentional)

- **In-memory `FusedStatelessSegment` driver** —
  `benches/wave2/fused_segment.rs` (TASK-519) already covers the
  §7.2 throughput and selection-vector materialisation count. The
  Wave 5 fusion bench (CP2 `stateful_aggregate_fusion.rs`) measures
  the aggregate-handoff path landed by TASK-520, which is the only
  *new* fusion surface introduced in this wave.
- **`entity_event_skew_p99` assertion benches.** The v1 morsel
  scheduler in `crates/bqlite-engine/src/query.rs` records exactly
  one `WorkerMetricsSnapshot::default()` per query, by design (see
  `perf.rs` "Wave 5 scope" notes). The CP3 morsel-skew bench is a
  wall-clock regression tripwire only; per-worker skew assertions
  upgrade once the scheduler populates real per-worker snapshots.
- **End-to-end engine benches for spill / cohort pushdown** beyond
  what the operator-layer benches cover. The Wave 5 acceptance gate
  (TASK-528) owns end-to-end correctness; this suite measures
  per-feature throughput and metric budgets in isolation so the
  numbers are stable and attributable.
- **`bytes_scanned` parity assertions through `Engine::query`.** The
  engine bind path (`crates/bqlite-engine/src/bind.rs:1376`)
  constructs `ScanOperator::with_tombstones` *without* an
  `attach_metrics` call — so `ExecutionResult.metrics.operator.bytes_scanned`
  is always zero today. The CP5 cohort-pushdown bench works around
  this by driving the storage-layer scan directly via
  `SegmentFileReader::scan` so it can attach `AtomicMetrics` and
  observe the counter. Once the engine threads its `Metrics` handle
  into the scan, both the morsel-skew (CP3) and cohort-pushdown
  (CP5) benches can add `bytes_scanned` parity rows at the engine
  layer.
- **Multi-column scan copy-budget assertions.** `zero_copy_scan.rs`
  projects a single column per scenario to isolate the metric.
  Multi-column scan copy budget is exercised end-to-end by Wave 2's
  `scan_encoded` bench (`benches/wave2/scan_encoded.rs`); revisiting
  it here would duplicate that coverage without strengthening the
  Wave 5 assertions.

## Running the suite

```bash
# Default CI mode — scaled-down fixtures, no hard targets enforced.
cargo bench -p bqlite-benches --bench zero_copy_scan
cargo bench -p bqlite-benches --bench stateful_aggregate_fusion
cargo bench -p bqlite-benches --bench morsel_skew
cargo bench -p bqlite-benches --bench spill_overhead
cargo bench -p bqlite-benches --bench cohort_pushdown

# Reference mode — full-scale fixtures with hard targets. Only
# meaningful on the pinned reference hardware.
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches --bench zero_copy_scan
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches --bench stateful_aggregate_fusion
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches --bench morsel_skew
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches --bench spill_overhead
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches --bench cohort_pushdown
```

Each bench also runs under `cargo test --all-targets` /
`cargo bench --bench <name> -- --test`, which executes one sample
per function. That path is wired into `scripts/local-ci.sh` so a
bench that panics at startup fails CI even if no one runs
`cargo bench`.

## How the bench gate reads these targets

`BenchResultCollector::write_json` writes to
`target/bench-results.json` on every run. The `bench-gate` job in
`.github/workflows/bench.yml` feeds that file plus the Criterion
baseline into `scripts/bench-compare.sh`; any result where
`target.pass == false` fails the job. Regression detection across
iterations continues to use Criterion's own sample-level comparison.

## Parent docs

- `benches/README.md` — bench-crate conventions (dual-mode dataset
  strategy, `[[bench]]` stanza shape, how to add a new bench).
- `docs/design/storage/zero-copy-scan-filter.md` §3 — copy-budget
  invariants underpinning `zero_copy_scan.rs`.
- `crates/bqlite-core/src/metrics.rs` — `MetricsSnapshot` doc
  comment with the same numerical claims, attached to the counters
  the bench reads.
- `docs/design/engine/operator-fusion.md` §7.2 — fused-stateless
  microbenches landed in `benches/wave2/fused_segment.rs` (out of
  scope for this README's coverage matrix; cross-linked here so the
  Wave 5 reader can find them). The TASK-520 stateful-to-aggregate
  fusion path (no §-pinned target) is what `stateful_aggregate_fusion.rs`
  measures.
