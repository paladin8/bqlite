//! Hash aggregate throughput benchmarks (TASK-325).
//!
//! Measures `HashAccumulator` throughput by group count (10, 1k, 1M) using
//! COUNT, SUM, and AVG aggregate functions.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench aggregate
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bqlite_benches::common::{criterion_for_mode, BenchMode};
use bqlite_core::{AggFunction, BqlType, ColumnDef, OperatorSchema, ScalarValue};
use bqlite_operators::{Accumulator, HashAccumulator, DEFAULT_MAX_GROUPS};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build the output schema for a grouped COUNT aggregate.
fn count_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("grp", BqlType::Int),
        ColumnDef::required("cnt", BqlType::Int),
    ])
    .unwrap()
}

/// Build the output schema for a grouped SUM aggregate.
fn sum_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("grp", BqlType::Int),
        ColumnDef::nullable("total", BqlType::Int),
    ])
    .unwrap()
}

/// Build the output schema for a grouped AVG aggregate.
fn avg_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("grp", BqlType::Int),
        ColumnDef::nullable("average", BqlType::Float),
    ])
    .unwrap()
}

/// Build the output schema for an ungrouped multi-aggregate.
fn multi_agg_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("cnt", BqlType::Int),
        ColumnDef::nullable("total", BqlType::Int),
        ColumnDef::nullable("average", BqlType::Float),
    ])
    .unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// COUNT throughput by group count
// ─────────────────────────────────────────────────────────────────────────────

fn bench_aggregate_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/count_by_groups");
    let total_rows = 1_000_000u64;
    group.throughput(Throughput::Elements(total_rows));

    for &num_groups in &[10i64, 1_000, 1_000_000] {
        let schema = count_schema();
        group.bench_with_input(
            BenchmarkId::from_parameter(num_groups),
            &num_groups,
            |b, &num_groups| {
                b.iter(|| {
                    let mut acc = HashAccumulator::new(
                        vec![AggFunction::Count],
                        vec![None], // COUNT(*)
                        schema.clone(),
                        vec!["grp".into()],
                        vec![None],
                        DEFAULT_MAX_GROUPS,
                    );
                    for i in 0..total_rows as i64 {
                        let grp = i % num_groups;
                        acc.update(Some(&[ScalarValue::Int(grp)]), &[ScalarValue::Null])
                            .unwrap();
                    }
                    black_box(acc.finish().unwrap())
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// SUM throughput by group count
// ─────────────────────────────────────────────────────────────────────────────

fn bench_aggregate_sum(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/sum_by_groups");
    let total_rows = 1_000_000u64;
    group.throughput(Throughput::Elements(total_rows));

    for &num_groups in &[10i64, 1_000, 1_000_000] {
        let schema = sum_schema();
        group.bench_with_input(
            BenchmarkId::from_parameter(num_groups),
            &num_groups,
            |b, &num_groups| {
                b.iter(|| {
                    let mut acc = HashAccumulator::new(
                        vec![AggFunction::Sum],
                        vec![Some(BqlType::Int)],
                        schema.clone(),
                        vec!["grp".into()],
                        vec![Some("val".into())],
                        DEFAULT_MAX_GROUPS,
                    );
                    for i in 0..total_rows as i64 {
                        let grp = i % num_groups;
                        acc.update(Some(&[ScalarValue::Int(grp)]), &[ScalarValue::Int(i)])
                            .unwrap();
                    }
                    black_box(acc.finish().unwrap())
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// AVG throughput by group count
// ─────────────────────────────────────────────────────────────────────────────

fn bench_aggregate_avg(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/avg_by_groups");
    let total_rows = 1_000_000u64;
    group.throughput(Throughput::Elements(total_rows));

    for &num_groups in &[10i64, 1_000, 1_000_000] {
        let schema = avg_schema();
        group.bench_with_input(
            BenchmarkId::from_parameter(num_groups),
            &num_groups,
            |b, &num_groups| {
                b.iter(|| {
                    let mut acc = HashAccumulator::new(
                        vec![AggFunction::Avg],
                        vec![Some(BqlType::Float)],
                        schema.clone(),
                        vec!["grp".into()],
                        vec![Some("val".into())],
                        DEFAULT_MAX_GROUPS,
                    );
                    for i in 0..total_rows as i64 {
                        let grp = i % num_groups;
                        acc.update(
                            Some(&[ScalarValue::Int(grp)]),
                            &[ScalarValue::Float(i as f64 * 0.1)],
                        )
                        .unwrap();
                    }
                    black_box(acc.finish().unwrap())
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Ungrouped multi-aggregate throughput
// ─────────────────────────────────────────────────────────────────────────────

fn bench_aggregate_ungrouped(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregate/ungrouped_multi");
    let total_rows = 1_000_000u64;
    group.throughput(Throughput::Elements(total_rows));

    let schema = multi_agg_schema();
    group.bench_function("count_sum_avg", |b| {
        b.iter(|| {
            let mut acc = HashAccumulator::new(
                vec![AggFunction::Count, AggFunction::Sum, AggFunction::Avg],
                vec![None, Some(BqlType::Int), Some(BqlType::Float)],
                schema.clone(),
                vec![],
                vec![None, Some("val".into()), Some("val".into())],
                DEFAULT_MAX_GROUPS,
            );
            acc.ensure_default_group().unwrap();
            for i in 0..total_rows as i64 {
                acc.update(
                    None,
                    &[
                        ScalarValue::Null,
                        ScalarValue::Int(i),
                        ScalarValue::Float(i as f64 * 0.1),
                    ],
                )
                .unwrap();
            }
            black_box(acc.finish().unwrap())
        });
    });
    group.finish();
}

criterion_group! {
    name = aggregate_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_aggregate_count,
        bench_aggregate_sum,
        bench_aggregate_avg,
        bench_aggregate_ungrouped,
}
criterion_main!(aggregate_benches);
