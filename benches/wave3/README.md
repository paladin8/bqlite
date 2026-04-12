# Wave 3 Benchmark Suite (TASK-325)

Criterion benchmarks covering the Wave 3 operator surface: sequence matching,
hash aggregation, sort, distinct, and the end-to-end funnel pipeline.

## Benchmarks

| Bench target     | File            | Coverage                                                                    |
|------------------|-----------------|-----------------------------------------------------------------------------|
| `matcher`        | `matcher.rs`    | NFA vs step-counter fast path on 3-step linear funnel; MATCH ALL; windowed  |
| `aggregate`      | `aggregate.rs`  | HashAccumulator COUNT/SUM/AVG by group count (10, 1k, 1M); ungrouped       |
| `wave3_sort`     | `sort.rs`       | SortOperator by row count (10k, 100k, 500k); two-key; multi-batch          |
| `wave3_distinct` | `distinct.rs`   | DistinctOperator by dedup ratio (0%, 50%, 90%, 99%); by row count           |
| `funnel`         | `funnel.rs`     | End-to-end 3-step funnel (ingest + parse + plan + execute); CI + reference  |
| `percentile`     | `percentile.rs` | DDSketch insert/quantile/merge; AggState P99; grouped HashAccumulator       |

## Running

```bash
# Run all Wave 3 benches
cargo bench -p bqlite-benches \
  --bench matcher --bench aggregate --bench wave3_sort \
  --bench wave3_distinct --bench funnel --bench percentile

# Run a single bench
cargo bench -p bqlite-benches --bench matcher

# Reference mode (100M-row funnel dataset, hard targets enforced)
BQLITE_BENCH_MODE=reference cargo bench -p bqlite-benches --bench funnel
```

## Performance Budgets

### Matcher (TASK-302 validation)

The step-counter fast path must be significantly faster than the full NFA
on the same linear 3-step funnel pattern. This validates the TASK-302
performance expectation that the pattern classifier correctly routes
linear patterns to the cheaper strategy.

- **Step counter vs NFA speedup**: step counter should be >= 2x faster
  on the 1k-entity linear funnel (measured by Criterion comparison)

### Aggregate

| Metric                     | CI threshold  | Reference target |
|----------------------------|---------------|------------------|
| COUNT 10 groups, 1M rows   | regression <10% | n/a             |
| SUM 1M groups, 1M rows     | regression <10% | n/a             |

### Sort

| Metric                     | CI threshold  | Reference target |
|----------------------------|---------------|------------------|
| int sort 100k rows         | regression <10% | n/a             |
| two-key sort 100k rows     | regression <10% | n/a             |

### Distinct

| Metric                     | CI threshold  | Reference target |
|----------------------------|---------------|------------------|
| 100k rows, 0% dup          | regression <10% | n/a             |
| 100k rows, 99% dup         | regression <10% | n/a             |

### End-to-end Funnel (reference mode only)

| Metric               | Reference target |
|-----------------------|------------------|
| 100M-event query time | < 10 s           |

The 10s ceiling is a generous 10x multiplier over the Wave 2 scan-only
baseline (< 1s). The gap accounts for MATCH operator overhead, entity
sub-batch slicing, and CSV ingest cost. As optimizations mature (predicate
pushdown into the matcher, batch-level step counters), this target will
tighten.

## CI Integration

Wave 3 benches are included in the `bench.yml` workflow:

- **bench-baseline** (main pushes): captures Criterion output for all benches
- **bench-gate** (PRs): runs regression comparison via `scripts/bench-compare.sh`;
  fails on >10% regression across 3+ consecutive samples
- **bench-reference** (manual dispatch): enforces hard targets in reference mode
