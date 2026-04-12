//! Sort operator throughput benchmarks (TASK-325).
//!
//! Measures `SortOperator` throughput by row count, covering the full
//! pipeline: accumulate → concatenate → lexsort → take → chunk emission.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench sort
//! ```

use std::sync::Arc;

use arrow::array::{Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bqlite_benches::common::*;
use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
use bqlite_operators::operator::{CancellationToken, PhysicalOperator};
use bqlite_operators::SortOperator;
use bqlite_planner::compiled::{CompiledExpr, CompiledNode};
use bqlite_planner::logical::SortDirection;
use bqlite_planner::physical::DEFAULT_SORT_MAX_ROWS;

// ─────────────────────────────────────────────────────────────────────────────
// Data generators
// ─────────────────────────────────────────────────────────────────────────────

fn int_sort_schema() -> OperatorSchema {
    OperatorSchema::new(vec![ColumnDef::required("v", BqlType::Int)]).unwrap()
}

fn two_col_sort_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("key", BqlType::String),
        ColumnDef::required("val", BqlType::Int),
    ])
    .unwrap()
}

/// Generate a single RecordBatch of `n` rows with descending i64 values
/// (worst case for ascending sort).
fn gen_int_batch_desc(n: usize) -> RecordBatch {
    let values: Vec<i64> = (0..n as i64).rev().collect();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "v",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

/// Generate a two-column batch: low-cardinality string key + i64 value.
fn gen_two_col_batch(n: usize, key_cardinality: usize) -> RecordBatch {
    let keys: Vec<String> = (0..n)
        .map(|i| format!("key_{}", i % key_cardinality))
        .collect();
    let values: Vec<i64> = (0..n as i64).rev().collect();
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

fn col0_expr() -> CompiledExpr {
    CompiledExpr {
        node: CompiledNode::Column {
            index: 0,
            name: "v".to_string(),
        },
        result_type: BqlType::Int,
        nullable: false,
    }
}

fn string_col0_expr() -> CompiledExpr {
    CompiledExpr {
        node: CompiledNode::Column {
            index: 0,
            name: "key".to_string(),
        },
        result_type: BqlType::String,
        nullable: false,
    }
}

fn int_col1_expr() -> CompiledExpr {
    CompiledExpr {
        node: CompiledNode::Column {
            index: 1,
            name: "val".to_string(),
        },
        result_type: BqlType::Int,
        nullable: false,
    }
}

/// Drive a SortOperator to completion and return total output rows.
fn drain_sort(sort: &mut SortOperator) -> u64 {
    sort.open().unwrap();
    let mut total = 0u64;
    while let Some(batch) = sort.next_batch().unwrap() {
        total += batch.num_rows() as u64;
        black_box(&batch);
    }
    sort.close().unwrap();
    total
}

// ─────────────────────────────────────────────────────────────────────────────
// Single-key i64 sort by row count
// ─────────────────────────────────────────────────────────────────────────────

fn bench_sort_int_by_rows(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort/int_by_rows");

    for &n in &[10_000usize, 100_000, 500_000] {
        group.throughput(Throughput::Elements(n as u64));
        let batch = gen_int_batch_desc(n);
        let schema = int_sort_schema();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let child = Box::new(VecOp::new(schema.clone(), vec![batch.clone()]));
                let mut sort = SortOperator::new(
                    child,
                    vec![(col0_expr(), SortDirection::Asc)],
                    DEFAULT_SORT_MAX_ROWS,
                    CancellationToken::new(),
                );
                black_box(drain_sort(&mut sort))
            });
        });
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Two-key sort (string + int)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_sort_two_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort/two_key_string_int");
    let n = 100_000usize;
    group.throughput(Throughput::Elements(n as u64));

    for &key_card in &[10, 100, 1000] {
        let batch = gen_two_col_batch(n, key_card);
        let schema = two_col_sort_schema();

        group.bench_with_input(
            BenchmarkId::new("key_cardinality", key_card),
            &key_card,
            |b, _| {
                b.iter(|| {
                    let child = Box::new(VecOp::new(schema.clone(), vec![batch.clone()]));
                    let mut sort = SortOperator::new(
                        child,
                        vec![
                            (string_col0_expr(), SortDirection::Asc),
                            (int_col1_expr(), SortDirection::Desc),
                        ],
                        DEFAULT_SORT_MAX_ROWS,
                        CancellationToken::new(),
                    );
                    black_box(drain_sort(&mut sort))
                });
            },
        );
    }
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-batch sort (simulates chunked pipeline input)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_sort_multi_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort/multi_batch");
    let total_rows = 100_000usize;
    let batch_size = 10_000;
    let num_batches = total_rows / batch_size;
    group.throughput(Throughput::Elements(total_rows as u64));

    let batches: Vec<RecordBatch> = (0..num_batches)
        .map(|_| gen_int_batch_desc(batch_size))
        .collect();
    let schema = int_sort_schema();

    group.bench_function("10x10k", |b| {
        b.iter(|| {
            let child = Box::new(VecOp::new(schema.clone(), batches.clone()));
            let mut sort = SortOperator::new(
                child,
                vec![(col0_expr(), SortDirection::Asc)],
                DEFAULT_SORT_MAX_ROWS,
                CancellationToken::new(),
            );
            black_box(drain_sort(&mut sort))
        });
    });
    group.finish();
}

criterion_group! {
    name = sort_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_sort_int_by_rows,
        bench_sort_two_keys,
        bench_sort_multi_batch,
}
criterion_main!(sort_benches);
