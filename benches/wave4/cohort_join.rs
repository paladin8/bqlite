//! Cohort semi-join + joined-source merge overhead (TASK-441).
//!
//! Two bench groups in one file:
//!
//! - `cohort/semijoin` — measures `SubqueryFilterOperator` probe
//!   throughput against pre-materialized `CohortHashSet` values of
//!   varying size (100 / 1 000 / 10 000 entities) over a 1 M-row
//!   entity-sorted input. Reference-mode target (plan `[floor]`):
//!   absolute probe throughput ≥ 10 M rows/sec at the 10 k-entity
//!   cohort size.
//!
//! - `merge_sources/k2` and `merge_sources/k4` — measures
//!   `MergeSourcesOperator` k-way merge throughput over 2 and 4
//!   entity-sorted sub-scans respectively. Reference-mode target
//!   (plan `[floor]`): absolute merged throughput ≥ 10 M rows/sec
//!   at k=2 (container-class floor; reference hardware comfortably
//!   exceeds this).
//!
//! The ratio-based targets called out in the plan (overhead vs scan
//! baseline) proved unreliable here because the `VecOp` baseline is
//! a trivial pre-built-batch iteration; the semijoin / merge cost
//! dominates by a huge margin and the resulting ratio carries no
//! signal. Absolute throughput numbers on the same synthetic fixture
//! give a cleaner regression tripwire, and the Criterion comparison
//! plot still shows the relative cost directly.
//!
//! Both operators run against synthetic `VecOp` inputs so the
//! measurement isolates the operator cost from storage I/O. TASK-438
//! (engine bind for merge-sources) has not landed yet, so the bench
//! operates at the operator layer directly — `SubqueryFilterOperator`
//! via engine binding is already covered by TASK-437's tests; the
//! bench here quantifies its per-row probe cost in isolation.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench cohort_join
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bqlite_ast::Span;
use bqlite_benches::common::*;
use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
use bqlite_operators::cohort::{CohortHashSet, SubqueryFilterOperator};
use bqlite_operators::operator::{CancellationToken, PhysicalOperator};
use bqlite_operators::scan::MergeSourcesOperator;
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::expr::TypedExpr;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Scale knobs
// ─────────────────────────────────────────────────────────────────────────────

/// Rows per batch emitted by the synthetic VecOps. Matches the
/// default merge batch size so per-row costs are directly comparable
/// to the engine-level merge path.
const BATCH_ROWS: usize = 65_536;

struct Scale {
    total_rows: usize,
    cohort_sizes: [usize; 3],
}

impl Scale {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => Self {
                // 131 072 rows across 2 batches for the semijoin
                // bench — enough to measure probe throughput without
                // tipping the shared CI runner into noise.
                total_rows: BATCH_ROWS * 2,
                cohort_sizes: [100, 1_000, 10_000],
            },
            BenchMode::Reference => Self {
                // 1 048 576 rows across 16 batches in reference mode —
                // the "1 M-row entity-sorted input" the plan calls
                // for in CP5.
                total_rows: BATCH_ROWS * 16,
                cohort_sizes: [100, 1_000, 10_000],
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schemas and column helpers
// ─────────────────────────────────────────────────────────────────────────────

fn three_col_schema() -> OperatorSchema {
    OperatorSchema::new(vec![
        ColumnDef::required("entity_id", BqlType::String),
        ColumnDef::required("ts", BqlType::Timestamp),
        ColumnDef::required("event_type", BqlType::String),
    ])
    .expect("three-col schema")
}

fn three_col_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8View, false),
    ]))
}

/// Build a `CompiledExpr` referencing a column by name in `schema`.
fn col_expr(name: &str, schema: &OperatorSchema) -> CompiledExpr {
    let (idx, col_def) = schema
        .column(name)
        .unwrap_or_else(|| panic!("column {name} not in schema"));
    let typed = TypedExpr::column(idx, col_def, Span::EMPTY);
    CompiledExpr::from_typed(&typed)
}

/// Build `n_batches` globally `(entity_id, ts)`-sorted batches for a
/// single sub-scan. Each entity gets `events_per_entity = total_rows
/// / unique_entities` rows with strictly monotonic timestamps, and
/// entities are emitted in lexicographic id order across batches —
/// this is the per-sub-scan invariant `MergeSourcesOperator` relies
/// on (each sub-scan's output must be `(entity_key, ts)`-ordered).
///
/// `build_event_stream(16, 10_000)` produces 16 × 65 536 = 1 048 576
/// rows with ~105 events per entity.
fn build_event_batches(n_batches: usize, unique_entities: usize) -> Vec<RecordBatch> {
    let total_rows = n_batches * BATCH_ROWS;
    let events_per_entity = (total_rows / unique_entities.max(1)).max(1);
    let base_ns: i64 = 1_700_000_000_000_000_000;
    let step_ns: i64 = 1_000_000; // 1 ms per event within an entity.

    // Generate the full stream first in entity-sorted order, then
    // slice into BATCH_ROWS-sized batches. Simpler than interleaving
    // the sort into the batch loop.
    let mut all_eids: Vec<String> = Vec::with_capacity(total_rows);
    let mut all_ts: Vec<i64> = Vec::with_capacity(total_rows);
    for entity_idx in 0..unique_entities {
        let entity = format!("user_{entity_idx:06}");
        for e in 0..events_per_entity {
            all_eids.push(entity.clone());
            all_ts.push(base_ns + (e as i64) * step_ns);
        }
    }
    // If total_rows isn't a clean multiple, pad the final entity
    // stream with extra rows so every batch is full.
    while all_eids.len() < total_rows {
        let last = all_eids
            .last()
            .cloned()
            .unwrap_or_else(|| "user_000000".into());
        let next_ts = all_ts.last().copied().unwrap_or(base_ns) + step_ns;
        all_eids.push(last);
        all_ts.push(next_ts);
    }

    let arrow = three_col_arrow_schema();
    let mut batches = Vec::with_capacity(n_batches);
    for b in 0..n_batches {
        let start = b * BATCH_ROWS;
        let end = start + BATCH_ROWS;
        let eids_chunk: Vec<String> = all_eids[start..end].to_vec();
        let ts_chunk: Vec<i64> = all_ts[start..end].to_vec();
        let events: Vec<&str> = (0..BATCH_ROWS).map(|_| "click").collect();

        let eid_col: ArrayRef = Arc::new(StringViewArray::from(eids_chunk));
        let ts_col: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(ts_chunk.iter().copied().map(Some).collect::<Vec<_>>())
                .with_timezone("UTC"),
        );
        let et_col: ArrayRef = Arc::new(StringViewArray::from(events));
        batches
            .push(RecordBatch::try_new(Arc::clone(&arrow), vec![eid_col, ts_col, et_col]).unwrap());
    }
    batches
}

/// Materialize a `CohortHashSet` of `n` entity ids using the public
/// `from_batches` constructor — the same path the engine bind step
/// uses at `crates/bqlite-engine/src/bind.rs`.
fn build_cohort(n: usize) -> Arc<CohortHashSet> {
    // Cohort subquery schema has one declared column: entity_id.
    let cohort_schema =
        OperatorSchema::new(vec![ColumnDef::required("entity_id", BqlType::String)]).unwrap();
    let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
        "entity_id",
        DataType::Utf8View,
        false,
    )]));
    let ids: Vec<String> = (0..n).map(|i| format!("user_{i:06}")).collect();
    let col: ArrayRef = Arc::new(StringViewArray::from(ids));
    let batch = RecordBatch::try_new(arrow, vec![col]).unwrap();
    Arc::new(CohortHashSet::from_batches(&cohort_schema, vec![batch]).expect("build cohort"))
}

/// Drain an operator to exhaustion, returning total rows produced.
fn drain(mut op: Box<dyn PhysicalOperator>) -> usize {
    op.open().unwrap();
    let mut rows = 0usize;
    while let Some(b) = op.next_batch().unwrap() {
        rows += b.num_rows();
    }
    op.close().unwrap();
    rows
}

// ─────────────────────────────────────────────────────────────────────────────
// Cohort semi-join bench
// ─────────────────────────────────────────────────────────────────────────────

fn bench_cohort_semijoin(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let scale = Scale::for_mode(mode);
    let n_batches = scale.total_rows / BATCH_ROWS;
    // Universe of possible entities is 10_000 so even the
    // 10 000-entity cohort fully matches; the cohort probe still
    // costs one hash-lookup per row, which is the regression target.
    let universe = 10_000usize;
    let batches = build_event_batches(n_batches, universe);
    let schema = three_col_schema();

    let mut collector = BenchResultCollector::new(mode);

    // Baseline: drain the raw VecOp (no cohort filter). Held once so
    // the ratio math uses the same scan-only timing across sizes.
    let baseline_rows = {
        let child: Box<dyn PhysicalOperator> =
            Box::new(VecOp::new(schema.clone(), batches.clone()));
        drain(child)
    };
    assert_eq!(baseline_rows, scale.total_rows);

    let mut group = c.benchmark_group("cohort/semijoin");
    group.throughput(Throughput::Elements(scale.total_rows as u64));

    // Baseline bench function — measures the no-cohort cost so
    // Criterion's comparison graph shows the semijoin overhead
    // directly.
    group.bench_function("baseline_vecop", |b| {
        b.iter(|| {
            let child: Box<dyn PhysicalOperator> =
                Box::new(VecOp::new(schema.clone(), batches.clone()));
            black_box(drain(child));
        })
    });

    for &cohort_size in &scale.cohort_sizes {
        let cohort = build_cohort(cohort_size);
        let lhs = vec![col_expr("entity_id", &schema)];

        group.bench_with_input(
            BenchmarkId::new("cohort_size", cohort_size),
            &cohort_size,
            |b, _| {
                b.iter(|| {
                    let child: Box<dyn PhysicalOperator> =
                        Box::new(VecOp::new(schema.clone(), batches.clone()));
                    let op = SubqueryFilterOperator::new(child, lhs.clone(), Arc::clone(&cohort))
                        .expect("subquery filter");
                    let rows = drain(Box::new(op));
                    black_box(rows);
                })
            },
        );

        // Standalone throughput measurement for the collector.
        let iters = 5;
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let child: Box<dyn PhysicalOperator> =
                Box::new(VecOp::new(schema.clone(), batches.clone()));
            let op = SubqueryFilterOperator::new(child, lhs.clone(), Arc::clone(&cohort))
                .expect("subquery filter");
            black_box(drain(Box::new(op)));
        }
        let per_run_s = start.elapsed().as_secs_f64() / iters as f64;
        let rows_per_sec = scale.total_rows as f64 / per_run_s.max(1e-12);

        // [floor] regression tripwire on the large cohort: hash
        // probe should clear 10 M rows/sec/core. Smaller cohorts are
        // reported for tracking.
        let target = if cohort_size == 10_000 {
            Some(BenchTarget::at_least(10_000_000.0))
        } else {
            None
        };
        collector.record(
            &format!("cohort/semijoin/cohort_{cohort_size}/rows_per_sec"),
            rows_per_sec,
            "rows/sec",
            target,
        );
    }

    group.finish();
    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Joined-source merge bench
// ─────────────────────────────────────────────────────────────────────────────

/// Combined OperatorSchema for a k-way join of `three_col_schema()`
/// sub-tables. Column names are qualified `<table>.<col>` for
/// non-system columns plus a non-nullable `__source_table_id`
/// discriminator, per `cohorts-aliases-joins.md` §3.8.
fn combined_schema(k: usize) -> OperatorSchema {
    let mut cols = Vec::new();
    for i in 0..k {
        let table = format!("t{i}");
        cols.push(ColumnDef::nullable(
            format!("{table}.entity_id"),
            BqlType::String,
        ));
        cols.push(ColumnDef::nullable(
            format!("{table}.ts"),
            BqlType::Timestamp,
        ));
        cols.push(ColumnDef::nullable(
            format!("{table}.event_type"),
            BqlType::String,
        ));
    }
    cols.push(ColumnDef::required("__source_table_id", BqlType::Int));
    OperatorSchema::new(cols).expect("combined schema")
}

fn bench_merge_sources(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let scale = Scale::for_mode(mode);
    // Half the total rows per sub-scan so the overall merged row
    // count matches the cohort bench's input size.
    let rows_per_sub = scale.total_rows / 2;
    let n_batches_per_sub = (rows_per_sub / BATCH_ROWS).max(1);

    let mut collector = BenchResultCollector::new(mode);

    let sub_schema = three_col_schema();
    let mut group = c.benchmark_group("merge_sources");
    group.throughput(Throughput::Elements(
        (n_batches_per_sub * BATCH_ROWS * 2) as u64,
    ));

    // Baseline: single VecOp scan over the concatenated input (same
    // row count as the merged output). Registered for the Criterion
    // comparison plot; the merge-side tripwires measure absolute
    // throughput rather than a ratio against this baseline — see the
    // file header for the rationale.
    let baseline_batches = build_event_batches(n_batches_per_sub * 2, 10_000);
    group.bench_function("baseline_single_scan", |b| {
        b.iter(|| {
            let op: Box<dyn PhysicalOperator> =
                Box::new(VecOp::new(sub_schema.clone(), baseline_batches.clone()));
            black_box(drain(op));
        })
    });

    for &k in &[2usize, 4] {
        // One batch set per sub-scan. Each sub-scan owns its own
        // entity-sorted stream; the k-way merge interleaves them.
        let sub_batch_sets: Vec<Vec<RecordBatch>> = (0..k)
            .map(|_| build_event_batches(n_batches_per_sub, 10_000))
            .collect();

        let combined = combined_schema(k);
        let table_id_map: Vec<String> = (0..k).map(|i| format!("t{i}")).collect();

        group.bench_with_input(BenchmarkId::new("k", k), &k, |b, _| {
            b.iter(|| {
                let sub_ops: Vec<Box<dyn PhysicalOperator>> = sub_batch_sets
                    .iter()
                    .map(|batches| {
                        let op: Box<dyn PhysicalOperator> =
                            Box::new(VecOp::new(sub_schema.clone(), batches.clone()));
                        op
                    })
                    .collect();
                let op = MergeSourcesOperator::new(
                    sub_ops,
                    vec!["entity_id".to_string(); k],
                    vec!["ts".to_string(); k],
                    combined.clone(),
                    table_id_map.clone(),
                    CancellationToken::new(),
                )
                .expect("merge sources ctor");
                black_box(drain(Box::new(op)));
            })
        });

        // Standalone throughput measurement for the collector.
        let iters = 5;
        let start = std::time::Instant::now();
        for _ in 0..iters {
            let sub_ops: Vec<Box<dyn PhysicalOperator>> = sub_batch_sets
                .iter()
                .map(|batches| {
                    let op: Box<dyn PhysicalOperator> =
                        Box::new(VecOp::new(sub_schema.clone(), batches.clone()));
                    op
                })
                .collect();
            let op = MergeSourcesOperator::new(
                sub_ops,
                vec!["entity_id".to_string(); k],
                vec!["ts".to_string(); k],
                combined.clone(),
                table_id_map.clone(),
                CancellationToken::new(),
            )
            .expect("merge sources ctor");
            black_box(drain(Box::new(op)));
        }
        let per_run_s = start.elapsed().as_secs_f64() / iters as f64;
        let merged_rows = (n_batches_per_sub * BATCH_ROWS * k) as f64;
        let rows_per_sec = merged_rows / per_run_s.max(1e-12);

        // [floor] regression tripwire on k=2: k-way merge of two
        // entity-sorted streams should clear 10 M merged rows/sec on
        // a conservative shared-runner floor. Reference-mode hardware
        // should comfortably exceed this. k=4 is tracking only.
        let target = if k == 2 {
            Some(BenchTarget::at_least(10_000_000.0))
        } else {
            None
        };
        collector.record(
            &format!("merge_sources/k{k}/rows_per_sec"),
            rows_per_sec,
            "rows/sec",
            target,
        );
    }

    group.finish();
    collector.finish();
}

criterion_group! {
    name = cohort_join_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_cohort_semijoin,
        bench_merge_sources,
}
criterion_main!(cohort_join_benches);
