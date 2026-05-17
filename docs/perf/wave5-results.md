# bqlite Wave 5 performance characterization

Auto-generated from `target/bench-results.json` by `cargo run -p bqlite-benches --release --bin perf_report`. Do not edit by hand — every section is rendered from the bench-collector JSON the `perf_*` benches emit.

## Headline numbers

Best throughput observed across the run, per bench group. Higher is better; cells are blank when the bench did not run at that scale.

| Bench | small |
|---|---|
| Scan (peak rows/s) | 95.49 M/s |
| Aggregation (peak rows/s) | 44.52 M/s |
| Funnel (peak rows/s) | 1.02 M/s |
| Sort top-k (peak rows/s) | 85.75 K/s |
| Ingest JSONL (peak rows/s) | 320.64 K/s |
| MC-scaling (peak rows/s) | 23.50 M/s |
| Concurrent (peak queries/s) | 39.3/s |

## Methodology

- **Hardware**: the run-host's defaults — see the `cargo bench` invocation log alongside this report for the exact machine specifics.
- **Scale ladder** (`BQLITE_BENCH_SCALE`):
  - `small` — 1M rows / 10K entities (default; suitable for development)
  - `medium` — 100M rows / 100K entities
  - `large` — 1B rows / 1M entities
  - `xlarge` — 10B rows / 10M entities

  Scales present in this report: **small**.

- **Seed**: `0x00000000beaca15e` — fixed per `docs/design/perf-suite.md` §3.2.
- **Mode**: `BQLITE_BENCH_MODE=ci` (default) vs `reference` controls which bench-result records are emitted; both modes still drive Criterion the same way.
- **Criterion config** (`benches/common/mod.rs::criterion_for_scale`):
  - small: sample_size=50, warm_up=1s, measurement=3s
  - medium: sample_size=20, warm_up=3s, measurement=10s
  - large: sample_size=10, warm_up=5s, measurement=30s
  - xlarge: sample_size=10, warm_up=10s, measurement=60s
- **Reproducing this report**:

  ```bash
  BQLITE_BENCH_SCALE=<scale> cargo bench -p bqlite-benches --bench perf_scan_selectivity \
      --bench perf_aggregation_cardinality --bench perf_funnel_depth \
      --bench perf_sort_topk --bench perf_ingest_throughput \
      --bench perf_mc_scaling --bench perf_concurrent_queries \
      --bench perf_memory_pressure
  cargo run -p bqlite-benches --release --bin perf_report
  ```

## Scan / filter

Five selectivity targets driven through `purchases | where amount <= K`. `matched_ratio` is the actual fraction of rows the predicate passes (the streaming generator's amount distribution is not perfectly uniform, so this drifts from the K-derived target).

| Point | small rows/s | small GB/s | small ratio |
|---|---|---|---|
| `sel_0.001` | 79.04 M/s | 10.22 GB/s | 0.050 |
| `sel_0.01` | 88.53 M/s | 11.45 GB/s | 0.267 |
| `sel_0.1` | 75.53 M/s | 9.77 GB/s | 0.531 |
| `sel_0.5` | 95.49 M/s | 12.35 GB/s | 0.808 |
| `sel_1.0` | 53.82 M/s | 6.96 GB/s | 1.000 |

## Aggregation

`COUNT(*) GROUP BY <key>` across group-key shapes covering low / mid / composite / high group-cardinalities per `docs/design/perf-suite.md` §3.4.

| Point | small rows/s | small GB/s | small groups |
|---|---|---|---|
| `composite_category_region` | 25.77 M/s | 3.33 GB/s | 128 |
| `high_card_user_id` | 34.18 M/s | 4.42 GB/s | 10.0 K |
| `low_card_quantity` | 44.52 M/s | 5.76 GB/s | 10 |
| `mid_card_event_type` | 36.79 M/s | 4.76 GB/s | 20 |

## Funnel / sequence

`MATCH FIRST SEQUENCE(event_0 THEN event_1 …) WITHIN 7d` at depths 2 / 5 / 10. `matches` is the number of matched sequences from the probe run.

| Point | small rows/s | small GB/s | small matches |
|---|---|---|---|
| `depth_10` | 1.01 M/s | 0.13 GB/s | 5.8 K |
| `depth_2` | 984.46 K/s | 0.13 GB/s | 8.2 K |
| `depth_5` | 1.02 M/s | 0.13 GB/s | 7.4 K |

## Sort / top-k

`purchases | ORDER BY amount ASC [LIMIT k]`. Currently probe-only — `ORDER BY ... LIMIT` does not push the limit into a top-k heap, so all three points pay the full-sort cost; `peak_memory_bytes` reflects that.

| Point | small probe | small rows/s | small peak mem |
|---|---|---|---|
| `full_sort` | 11.66 s | 85.75 K/s | 2.39 GiB |
| `topk_1k` | 18.18 s | 55.00 K/s | 2.39 GiB |
| `topk_1m` | 24.08 s | 41.54 K/s | 2.39 GiB |

## Ingest

End-to-end ingest throughput for the three supported input shapes. `file_mb_per_sec` is the on-disk source size for JSONL/Parquet (and the SQL-string size for INSERT VALUES); `event_mb_per_sec` normalizes by the logical event byte count.

| Point | small rows/s | small file | small events |
|---|---|---|---|
| `insert_values` | 98.52 K/s | — | 13.0 MB/s |
| `jsonl` | 245.77 K/s | 42.8 MB/s | 32.5 MB/s |
| `parquet` | 320.64 K/s | 5.3 MB/s | 42.4 MB/s |

## Multi-core scaling

`purchases | STATS COUNT(*) GROUP BY category, region` driven at `query_threads = 1 / 2 / 4 / 8 / auto`. Speedup is relative to the 1-thread probe of the same bench run (so a non-zero figure even at `threads_1` reflects measurement noise vs. the warm-up probe).

| Point | small threads | small rows/s | small speedup |
|---|---|---|---|
| `threads_1` | 1 | 4.46 M/s | 1.03× |
| `threads_2` | 2 | 8.85 M/s | 2.04× |
| `threads_4` | 4 | 16.92 M/s | 3.90× |
| `threads_8` | 8 | 20.62 M/s | 4.75× |
| `threads_auto` | 12 | 23.50 M/s | 5.41× |

## Concurrent queries

1 / 4 / 16 client threads submit `purchases | STATS COUNT(*) GROUP BY category` against a shared `Arc<Mutex<Database>>` (`Database::open` holds a per-directory exclusive lock, so submissions serialize at the mutex). Per-query parallelism within the engine is unchanged — the bench varies only the submitter count.

| Point | small queries/s | small per-thread | small rows/s |
|---|---|---|---|
| `clients_1` | 39.3 | 25.5 ms | 39.28 M/s |
| `clients_16` | 35.7 | 448.0 ms | 35.72 M/s |
| `clients_4` | 35.9 | 111.4 ms | 35.90 M/s |

## Memory pressure

Same two queries (`sort_full`, `agg_high_card`) submitted at three `QueryOptions::memory_budget_bytes` budgets. `status` is 1 when the query completed under the budget, 0 when the engine rejected it (either under-floor or budget-exceeded). The 512 MiB floor reflects `docs/design/engine/memory-budget.md` §8.2.

| Point | small status | small probe | small peak mem | small rows/s |
|---|---|---|---|---|
| `agg_high_card/budget_1gb` | ok | 39.0 ms | 0 MiB | 25.67 M/s |
| `agg_high_card/budget_3gb` | ok | 41.8 ms | 0 MiB | 23.91 M/s |
| `agg_high_card/budget_512mb` | ok | 40.0 ms | 0 MiB | 24.99 M/s |
| `sort_full/budget_1gb` | fail | — | — | — |
| `sort_full/budget_3gb` | ok | 20.88 s | 2.39 GiB | 47.89 K/s |
| `sort_full/budget_512mb` | fail | — | — | — |

