# Wave 4 Benchmark Suite (TASK-441)

Criterion benches covering the Wave 4 advanced analytics performance
story: advanced-encoding compression/decode comparisons, compaction
throughput and L0 fan-in reduction, JSONL / Parquet ingest, SAMPLE
pushdown, ATTRIBUTE latency, and cohort / joined-source query
overhead. Each bench reports metrics via `BenchResultCollector` so
that `scripts/bench-compare.sh` promotes its hard targets into the CI
bench gate.

This README is the single source for which `(file → metric → target)`
the CI gate enforces. Adding a bench means adding a row below.

## Layout

```
benches/wave4/
├── README.md            # this file
├── attribute.rs         # AttributeOperator throughput (TASK-431, TASK-441 ratio sweep)
├── compaction.rs        # Database::compact_now throughput + L0 reduction
├── cohort_join.rs       # SubqueryFilter probe + MergeSources k-way merge
├── encoding_matrix.rs   # ALP + head-to-head integer/string encodings
├── ingest.rs            # JSONL + Parquet end-to-end ingest
├── pfor.rs              # PFOR codec (TASK-450)
├── sample.rs            # SampleFilter per-row throughput + selectivity
└── sessionize.rs        # SessionizeOperator throughput (TASK-428)
```

## Coverage matrix

| TASK-441 area | Bench file | Main operator / API |
|---|---|---|
| Advanced-encoding compression/decode comparisons | `encoding_matrix.rs` | `Alp`, `Plain`, `BitPacking`, `Delta`, `DoubleDelta`, `ForEncoding`, `Dictionary`, `Rle` |
| Compaction throughput + read-amplification reduction | `compaction.rs` | `Database::compact_now`, `CompactionOutcome` |
| JSONL / Parquet ingest throughput | `ingest.rs` | `JsonlEventReader`, `ParquetEventReader`, `Partitioner`, `SegmentWriter` |
| SAMPLE pushdown savings | `sample.rs` | `SampleFilter::apply_to_array`, `ScanOperator::with_sample_filter` |
| ATTRIBUTE latency on realistic ratios | `attribute.rs` | `AttributeOperator` (existing §17.1 workloads + 10:1 / 100:1 / 1000:1 ratio sweep) |
| Cohort / joined-source query overhead | `cohort_join.rs` | `SubqueryFilterOperator`, `MergeSourcesOperator` |

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
| `encoding_matrix/alp/round_f64/compression_ratio` | ≤ 0.40 | **[spec]** `advanced-encodings.md` §8.2 |
| `encoding_matrix/alp/round_f64/decode_gb_per_sec` | ≥ 2.0 GB/s | **[floor]** derived from `advanced-encodings.md` §8.3 qualitative claim |
| `encoding_matrix/int_clustered/for_vs_bitpacking_payload_ratio` | ≤ 0.75 | **[spec]** `advanced-encodings.md` §5.2 |
| `ingest/jsonl/end_to_end/mb_per_sec` | ≥ 100 MB/s | **[floor]** parity with Wave 2 CSV ingest floor |
| `ingest/parquet/end_to_end/mb_per_sec` | ≥ 150 MB/s | **[floor]** columnar decode should beat JSONL parse |
| `compaction/throughput/l0_to_l1_mb_per_sec` | ≥ 200 MB/s | **[floor]** no MB/s number pinned; chosen as regression tripwire |
| `compaction/l0_reduction/5_to_1/ratio` | ≥ 5 | **[spec]** `compaction-concurrency.md` §3.2 (eligible when count > 4) |
| `sample/apply_to_array/fraction_0.10/entities_per_sec` | ≥ 50 M | **[spec]** `event-select-sample.md` §21.2 row 1 |
| `sample/selectivity/fraction_0.01/abs_deviation` | ≤ 3σ bound | **[floor]** Bernoulli 3σ: `3·sqrt(f·(1-f)/N)` |
| `sample/selectivity/fraction_0.10/abs_deviation` | ≤ 3σ bound | **[floor]** same |
| `cohort/semijoin/cohort_10000/rows_per_sec` | ≥ 10 M rows/sec | **[floor]** hash-set probe regression tripwire |
| `merge_sources/k2/rows_per_sec` | ≥ 10 M rows/sec | **[floor]** k-way merge regression tripwire |

### Not covered (intentional)

- **Frequency encoding** — `advanced-encodings.md` §9.5 is NO-GO; no
  shipping implementation to bench.
- **Engine-level benches for Wave 4 operators** — SAMPLE /
  MergeSources / Attribute / EventSelect / Sessionize were landed
  in the engine bind step under TASK-438. The TASK-441 benches
  measure at the operator layer directly so the numbers are stable
  and attributable to each operator in isolation. End-to-end
  `Engine::query` benches for the full Wave 4 feature set are a
  Wave 5 concern (query-optimizer fusion, memory budgets).

## Running the suite

```bash
# Default CI mode — scaled-down fixtures, no hard targets enforced.
cargo bench -p bqlite-benches --bench encoding_matrix
cargo bench -p bqlite-benches --bench wave4_ingest
cargo bench -p bqlite-benches --bench compaction
cargo bench -p bqlite-benches --bench sample
cargo bench -p bqlite-benches --bench cohort_join
cargo bench -p bqlite-benches --bench attribute
cargo bench -p bqlite-benches --bench sessionize
cargo bench -p bqlite-benches --bench pfor

# Reference mode — full-scale fixtures with hard targets. Only
# meaningful on the pinned reference hardware.
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches
```

Each bench also runs under `cargo test --all-targets` / `--bench … --
--test`, which executes one sample per function. That path is wired
into `scripts/local-ci.sh` so a bench that panics at startup fails
CI even if no one runs `cargo bench`.

## How the bench gate reads these targets

`BenchResultCollector` writes to `target/bench-results.json` on every
run. The `bench-gate` job in `.github/workflows/bench.yml` feeds that
file plus the Criterion baseline into `scripts/bench-compare.sh`; any
result where `target.pass == false` fails the job. Regression
detection across iterations continues to use Criterion's own
sample-level comparison. No new CI wiring ships with TASK-441 —
every Wave 4 bench plugs into the existing gate.

## Parent docs

- `benches/README.md` — bench-crate conventions (dual-mode dataset
  strategy, `[[bench]]` stanza shape, how to add a new bench).
- `docs/design/storage/advanced-encodings.md` — §2.1 column profiles
  and §6–§9 per-codec recommendations drive `encoding_matrix.rs`.
- `docs/design/storage/compaction-concurrency.md` §3.2 defines the
  L0 trigger used by `compaction.rs`.
- `docs/design/operators/event-select-sample.md` §21.2 defines the
  SAMPLE throughput floor used by `sample.rs`.
- `docs/design/operators/attribute.md` §17.1 defines the ATTRIBUTE
  workload shapes used by `attribute.rs`.
- `docs/design/language/cohorts-aliases-joins.md` §3.8 + §4.2
  define the cohort + merge-sources semantics underpinning
  `cohort_join.rs`.
