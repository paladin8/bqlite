//! Distinct operator throughput benchmarks (TASK-325).
//!
//! Measures `DistinctOperator` throughput by dedup ratio: from 0% duplicates
//! (every row unique) to 99% duplicates (10x repeated keys).
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench distinct
//! ```

use std::sync::Arc;

use arrow::array::{Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bqlite_benches::common::*;
use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
use bqlite_operators::operator::{CancellationToken, PhysicalOperator};
use bqlite_operators::DistinctOperator;
use bqlite_planner::physical::DEFAULT_MAX_GROUPS;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn distinct_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("key", BqlType::String),
        ColumnDef::required("val", BqlType::Int),
    ])
    .unwrap()
}

/// Generate a batch with controlled dedup ratio.
///
/// `n` total rows, `unique_keys` distinct key values. The dedup ratio
/// is `1 - unique_keys/n`.
fn gen_dedup_batch(n: usize, unique_keys: usize) -> RecordBatch {
    let keys: Vec<String> = (0..n).map(|i| format!("k_{}", i % unique_keys)).collect();
    let values: Vec<i64> = (0..n as i64).collect();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Utf8View, false),
        Field::new("val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Drive a DistinctOperator to completion and return total output rows.
fn drain_distinct(op: &mut DistinctOperator) -> u64 {
    op.open().unwrap();
    let mut total = 0u64;
    while let Some(batch) = op.next_batch().unwrap() {
        total += batch.num_rows() as u64;
        black_box(&batch);
    }
    op.close().unwrap();
    total
}

// ─────────────────────────────────────────────────────────────────────────────
// Distinct by dedup ratio
// ─────────────────────────────────────────────────────────────────────────────

fn bench_distinct_by_dedup_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct/by_dedup_ratio");
    let n = 100_000usize;
    group.throughput(Throughput::Elements(n as u64));

    // (label, unique_keys) — dedup ratio = 1 - unique_keys/n
    let cases: Vec<(&str, usize)> = vec![
        ("0pct_dup", n),        // 0% duplicates (all unique)
        ("50pct_dup", n / 2),   // 50% duplicates
        ("90pct_dup", n / 10),  // 90% duplicates
        ("99pct_dup", n / 100), // 99% duplicates
    ];

    for (label, unique_keys) in &cases {
        let batch = gen_dedup_batch(n, *unique_keys);
        let schema = distinct_schema();

        group.bench_with_input(BenchmarkId::new("ratio", label), unique_keys, |b, _| {
            b.iter(|| {
                let child = Box::new(VecOp::new(schema.clone(), vec![batch.clone()]));
                let mut distinct =
                    DistinctOperator::new(child, DEFAULT_MAX_GROUPS, CancellationToken::new());
                black_box(drain_distinct(&mut distinct))
            });
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Distinct by row count (fixed 50% dedup ratio)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_distinct_by_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct/by_rows");

    for &n in &[10_000usize, 50_000, 200_000] {
        let unique_keys = n / 2; // 50% dedup ratio
        group.throughput(Throughput::Elements(n as u64));
        let batch = gen_dedup_batch(n, unique_keys);
        let schema = distinct_schema();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let child = Box::new(VecOp::new(schema.clone(), vec![batch.clone()]));
                let mut distinct =
                    DistinctOperator::new(child, DEFAULT_MAX_GROUPS, CancellationToken::new());
                black_box(drain_distinct(&mut distinct))
            });
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-batch distinct
// ─────────────────────────────────────────────────────────────────────────────

fn bench_distinct_multi_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct/multi_batch");
    let total_rows = 100_000usize;
    let batch_size = 10_000;
    let num_batches = total_rows / batch_size;
    let unique_keys = total_rows / 5; // 80% dedup when cross-batch
    group.throughput(Throughput::Elements(total_rows as u64));

    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|_| gen_dedup_batch(batch_size, unique_keys))
        .collect();
    let schema = distinct_schema();

    group.bench_function("10x10k_80pct_dup", |b| {
        b.iter(|| {
            let child = Box::new(VecOp::new(schema.clone(), batches.clone()));
            let mut distinct =
                DistinctOperator::new(child, DEFAULT_MAX_GROUPS, CancellationToken::new());
            black_box(drain_distinct(&mut distinct))
        });
    });
    group.finish();
}

criterion_group! {
    name = distinct_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_distinct_by_dedup_ratio,
        bench_distinct_by_rows,
        bench_distinct_multi_batch,
}
criterion_main!(distinct_benches);
