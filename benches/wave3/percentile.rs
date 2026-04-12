//! DDSketch percentile throughput benchmark (TASK-327).
//!
//! Measures insert and quantile-query throughput for the DDSketch-based
//! percentile accumulators, both through the raw sketch API and through
//! the `AggState` / `HashAccumulator` integration path.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench percentile
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use bqlite_core::{AggFunction, BqlType, ScalarValue};
use bqlite_operators::aggregate::percentile::DDSketch;
use bqlite_operators::{Accumulator, AggState, HashAccumulator, DEFAULT_MAX_GROUPS};

/// Benchmark raw DDSketch insert throughput.
fn bench_ddsketch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentile/ddsketch_insert");
    for &n in &[1_000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut sketch = DDSketch::new();
                for i in 1..=n {
                    sketch.insert(black_box(i as f64));
                }
                black_box(&sketch);
            });
        });
    }
    group.finish();
}

/// Benchmark DDSketch quantile query after N inserts.
fn bench_ddsketch_quantile(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentile/ddsketch_quantile");
    for &n in &[1_000, 10_000, 100_000] {
        let mut sketch = DDSketch::new();
        for i in 1..=n {
            sketch.insert(i as f64);
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                black_box(sketch.quantile(black_box(0.99)));
            });
        });
    }
    group.finish();
}

/// Benchmark DDSketch merge throughput.
fn bench_ddsketch_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentile/ddsketch_merge");
    for &n in &[1_000, 10_000] {
        let mut a = DDSketch::new();
        let mut b = DDSketch::new();
        for i in 1..=n {
            a.insert(i as f64);
            b.insert((n + i) as f64);
        }
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |bench, _| {
            bench.iter(|| {
                let mut merged = a.clone();
                merged.merge(black_box(&b));
                black_box(&merged);
            });
        });
    }
    group.finish();
}

/// Benchmark AggState::Percentile update throughput (the path used by
/// HashAccumulator for ungrouped P99).
fn bench_agg_state_percentile_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentile/agg_state_update");
    for &n in &[1_000, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut state = AggState::new(AggFunction::P99, Some(&BqlType::Int));
                for i in 1..=n {
                    state.update(black_box(&ScalarValue::Int(i)));
                }
                black_box(state.finalize());
            });
        });
    }
    group.finish();
}

/// Benchmark HashAccumulator with grouped P50 — measures end-to-end
/// throughput including group-key hashing and state lookup.
fn bench_hash_accumulator_grouped_percentile(c: &mut Criterion) {
    let mut group = c.benchmark_group("percentile/hash_accumulator_grouped");
    for &num_groups in &[10, 100, 1000] {
        let schema = {
            use bqlite_core::ColumnDef;
            bqlite_core::OperatorSchema::new(vec![
                ColumnDef::required("grp", BqlType::Int),
                ColumnDef::nullable("p50", BqlType::Float),
            ])
            .unwrap()
        };
        group.bench_with_input(
            BenchmarkId::from_parameter(num_groups),
            &num_groups,
            |b, &num_groups| {
                b.iter(|| {
                    let mut acc = HashAccumulator::new(
                        vec![AggFunction::P50],
                        vec![Some(BqlType::Int)],
                        schema.clone(),
                        vec!["grp".into()],
                        vec![Some("val".into())],
                        DEFAULT_MAX_GROUPS,
                    );
                    for i in 0..10_000i64 {
                        let grp = i % num_groups;
                        acc.update(Some(&[ScalarValue::Int(grp)]), &[ScalarValue::Int(i)])
                            .unwrap();
                    }
                    black_box(acc.finish().unwrap());
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    percentile_benches,
    bench_ddsketch_insert,
    bench_ddsketch_quantile,
    bench_ddsketch_merge,
    bench_agg_state_percentile_update,
    bench_hash_accumulator_grouped_percentile,
);
criterion_main!(percentile_benches);
