# `bench-perf` Performance Characterization Suite

**Owner.** TASK-546.
**Status.** Active (Wave 5+, does not block Wave 6).
**Scope.** Produce headline rows/s and GB/s numbers for bqlite at 100M,
1B, and 10B-row scales, suitable for marketing claims and capacity
planning. Identify opportunities for improvement where measured
performance falls short of design targets.

## 1. Why this exists

The existing benches under `benches/wave[2-5]/` are correctness and
regression tripwires sized for CI: each one runs in seconds and
reports relative-comparison metrics (3.39 µs / 1000 rows, etc.). They
were never intended to answer "what is bqlite's actual throughput on
real-world-scale data?" The Wave 5 audit (TASK-599) explicitly
flagged that **bench CI mode uses scaled-down datasets** and that the
100M reference-mode gate has never run on the actual reference
hardware.

`bench-perf` fills that gap with three deliverables:

1. **A scale ladder.** All benches accept a `BenchScale` enum
   (Small 1M / Medium 100M / Large 1B / XLarge 10B) controlled by
   the `BQLITE_BENCH_SCALE` env var. The same bench file runs at any
   scale; the only difference is fixture size and Criterion config.

2. **A persistent fixture cache.** Materialized `Database` instances
   are cached on disk by `(scale, seed, schema_version)` so the
   30-minute (1B) and multi-hour (10B) ingest steps run **once per
   fixture**, not once per bench iteration.

3. **A polished report.** `docs/perf/wave5-results.md` is the single
   public artefact: a headline numbers table, per-bench-group
   throughputs across all three scales, a multi-core scaling chart,
   and a methodology section citing seed, hardware, and Criterion
   config. The report is generated, not hand-written.

## 2. Scope

**In scope:**
- Scan / filter throughput across selectivity sweep.
- `COUNT(*) GROUP BY` aggregation throughput across group-cardinality
  sweep.
- Funnel pattern matching at varying depths.
- Sort + top-k including the spill regime.
- Ingest throughput for INSERT VALUES, JSONL, and Parquet.
- Multi-core scaling curve (1, 2, 4, 8, auto workers).
- Concurrent-query throughput (1, 4, 16 parallel queries).
- Memory-budget behaviour: when does each bench spill?
- 100M / 1B / 10B-row scales — all bench groups must run cleanly at
  100M; the 1B and 10B steps may be reduced for benches where the
  underlying algorithm has known complexity issues (e.g. funnels at
  10B may use a smaller fixture if pattern matching is the
  bottleneck rather than scan).

**Out of scope:**
- Comparison vs. external engines (DuckDB, ClickHouse, Polars).
  Deferred to a future task once raw numbers are established.
- Pretty HTML rendering. Markdown only.
- CI gating. Bench-compare.sh already gates regressions on the
  existing benches; `bench-perf` is **report-only** — it does not
  fail PRs.

## 3. Architecture

### 3.1 Scale ladder

```rust
pub enum BenchScale {
    Small,    // 1M rows — fits in seconds, validates wiring
    Medium,   // 100M rows — Wave 5 acceptance gate
    Large,    // 1B rows — stress
    XLarge,   // 10B rows — headline
}

impl BenchScale {
    pub fn from_env() -> Self { /* read BQLITE_BENCH_SCALE */ }
    pub fn rows(self) -> u64 { /* 1M, 100M, 1B, 10B */ }
    pub fn entity_count(self) -> usize { /* 10K, 100K, 1M, 10M */ }
}
```

The `BenchMode` enum (CI / Reference) is orthogonal to `BenchScale`:

- `Mode == CI` + `Scale == Small` → status-quo CI loop, sub-second per
  bench iteration.
- `Mode == Reference` + `Scale == Medium` → today's reference-mode
  acceptance gate with hard targets.
- `Mode == CI` + `Scale == Large` → development iteration on big
  fixtures without target panics.
- `Mode == Reference` + `Scale == XLarge` → the headline production
  run.

### 3.2 Streaming fixture generator

The existing `generate_events` produces `Vec<Event>` — at 10B events
× ~150 bytes each, that's 1.5 TiB of RAM. The streaming generator
produces events in chunks of 1M (configurable) and yields them via a
`for_each(|chunk| ...)` callback. Generation cost is ~constant memory
regardless of total row count.

Realism comes from a power-law entity-skew model: at the default
`entity_skew = 1.5`, the top 1% of entities own ~30% of the events
and the bottom 50% own < 1%, mimicking real behavioral workloads.
Event-type distribution stays at the existing 20-label profile;
property columns stay at the existing 7-column profile so this
generator is comparable to the legacy `generate_events`.

Seed: 64-bit deterministic seed (default 0xBEACA15E), recorded in
the report's methodology section.

### 3.3 Persistent fixture cache

```rust
pub struct PersistentFixture {
    pub db_path: PathBuf,
    pub manifest: FixtureManifest,
}

pub struct FixtureManifest {
    pub scale: BenchScale,
    pub seed: u64,
    pub schema_version: u32,    // bump when generator changes
    pub rows: u64,
    pub bytes_logical: u64,     // sum of compute_event_bytes
    pub built_at: SystemTime,
}
```

Layout:

```
$BQLITE_BENCH_CACHE_DIR/                     # default: target/bench-fixtures
  fixture-small-seed0xBEACA15E-v1/
    manifest.json
    db/                                       # the actual Database directory
  fixture-medium-seed0xBEACA15E-v1/
    ...
  fixture-large-seed0xBEACA15E-v1/
    ...
  fixture-xlarge-seed0xBEACA15E-v1/
    ...
```

On first access, `PersistentFixture::load_or_build(scale)` checks for
the manifest; if missing or stale (older than 90 days, mismatched
schema version), it runs the streaming generator into a fresh
`Database` and writes the manifest atomically (write to `tmp`,
rename). Stale fixtures are deleted before rebuild.

Forced rebuild: `BQLITE_BENCH_REGEN=1`.

### 3.4 Bench groups

Each group is a separate file under `benches/perf/` and a `[[bench]]`
entry in `benches/Cargo.toml`. They all share the same structure:

```rust
fn main() {
    let scale = BenchScale::from_env();
    let mode = BenchMode::from_env();
    let fixture = PersistentFixture::load_or_build(scale);
    let mut c = criterion_for_scale(scale);
    let mut collector = BenchResultCollector::new(mode);

    // ... bench definitions reading from fixture.db_path ...

    collector.finish();
    c.final_summary();
}
```

Each bench reports both Criterion's measurement (latency) and a
manually-recorded throughput metric via `BenchResultCollector` so the
JSON output has rows/s and GB/s numbers for the report generator to
consume.

Bench groups (one file each, ~8 files total):

| File | What it measures |
|---|---|
| `benches/perf/scan_selectivity.rs` | Filter throughput at 0.001 / 0.01 / 0.1 / 0.5 / 1.0 selectivity |
| `benches/perf/aggregation_cardinality.rs` | `COUNT(*) GROUP BY` at 10 / 1K / 1M / 100M groups |
| `benches/perf/funnel_depth.rs` | 2 / 5 / 10-step funnels with `WITHIN 7d` |
| `benches/perf/sort_topk.rs` | Full sort, sort+limit(1K), sort+limit(1M) |
| `benches/perf/ingest_throughput.rs` | INSERT VALUES, JSONL, Parquet |
| `benches/perf/mc_scaling.rs` | `query_threads = 1 / 2 / 4 / 8 / auto` |
| `benches/perf/concurrent_queries.rs` | 1 / 4 / 16 parallel queries |
| `benches/perf/memory_pressure.rs` | Same queries at 256 MB / 1 GB / 3 GB query budget |

### 3.5 Report generator

`benches/src/bin/perf_report.rs` reads `target/bench-results.json`
and writes `docs/perf/wave5-results.md`. The report layout:

```markdown
# bqlite Wave 5 performance characterization

## Headline numbers
[ small table with the 5 most impressive throughputs ]

## Methodology
- Hardware: ...
- Scale ladder: ...
- Seed: ...
- Criterion config: ...

## Scan / filter
[ rows/s, GB/s, p50/p99 latency tables, one row per scale × selectivity ]

## Aggregation
[ tables, one row per scale × group-cardinality ]

## Funnel / sequence
[ tables, one row per scale × depth ]

## Sort / top-k
[ tables, one row per scale × limit-fraction ]

## Ingest
[ tables, one row per scale × format ]

## Multi-core scaling
[ speedup table, one column per worker count ]

## Concurrent queries
[ aggregate throughput + per-query latency percentiles ]

## Memory pressure
[ how each query degrades at smaller budgets ]
```

### 3.6 Criterion configuration per scale

```rust
pub fn criterion_for_scale(scale: BenchScale) -> Criterion {
    match scale {
        BenchScale::Small  => Criterion::default().sample_size(50).warm_up(1s).measure(3s),
        BenchScale::Medium => Criterion::default().sample_size(20).warm_up(3s).measure(10s),
        BenchScale::Large  => Criterion::default().sample_size(10).warm_up(5s).measure(30s),
        BenchScale::XLarge => Criterion::default().sample_size(5).warm_up(10s).measure(60s),
    }
}
```

XLarge runs the full bench suite in approximately
**`(60s measure + 10s warm-up + 5 samples × per-bench overhead) × ~50 benches`**
— budget several hours.

## 4. Workflow

### 4.1 Development loop

```bash
# CI mode + Small scale: status quo, sub-second per iteration
cargo bench -p bqlite-benches --bench perf_scan_selectivity

# Reference mode + Medium scale: validates new benches at 100M rows
BQLITE_BENCH_MODE=reference BQLITE_BENCH_SCALE=medium \
  cargo bench -p bqlite-benches --bench perf_scan_selectivity
```

### 4.2 Headline run

```bash
# One-shot manual production run at 10B rows.
# Generates fixture (~hours), runs all benches (~hours), produces report.
BQLITE_BENCH_MODE=reference BQLITE_BENCH_SCALE=xlarge \
  cargo bench -p bqlite-benches \
  --bench perf_scan_selectivity \
  --bench perf_aggregation_cardinality \
  --bench perf_funnel_depth \
  --bench perf_sort_topk \
  --bench perf_ingest_throughput \
  --bench perf_mc_scaling \
  --bench perf_concurrent_queries \
  --bench perf_memory_pressure

cargo run -p bqlite-benches --release --bin perf_report
# writes docs/perf/wave5-results.md
```

## 5. Risks and mitigations

| Risk | Mitigation |
|---|---|
| 10B-row fixture exceeds disk | Document storage requirement up front (~500 GB at 50 B/row); document `BQLITE_BENCH_CACHE_DIR` so the cache can live on external storage |
| Streaming generator becomes the bottleneck rather than the engine | Generator runs in parallel with ingest (channel of chunks); generator throughput target ≥ 50M events/s on the M2 Max so it stays ahead of ingest |
| 10B sort spills exceed disk | Bench documents the spill-disk requirement; `sort_topk` at XLarge uses the limit variants only |
| Fixture corruption survives across runs | Manifest includes schema version + bytes_logical; integrity check on load verifies the manifest matches the database |
| Single 10B run takes too long to debug if anything fails | All benches validate at Medium (100M) first; XLarge is only run after Medium passes cleanly |

## 6. Acceptance

1. All 8 bench groups compile, run cleanly at Small scale, and
   produce JSON output.
2. All 8 bench groups run cleanly at Medium scale and report rows/s
   and GB/s.
3. `cargo run --bin perf_report` produces a well-formed
   `docs/perf/wave5-results.md` with all sections populated.
4. At least one full XLarge run completes and the report's headline
   numbers section reflects the 10B-row throughput.

## 7. Out of scope (future tasks)

- Comparison harness vs. DuckDB, ClickHouse, Polars (TASK-547).
- CI integration of `bench-perf` (TASK-548) — these are slow
  characterization benches, not regression gates.
- Continuous-perf-tracking dashboard reading `bench-results.json`
  across many runs.
