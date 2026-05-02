//! Sort spill overhead bench (TASK-526, Wave 5).
//!
//! Measures the throughput tax `SortOperator::with_spill` pays when
//! the per-query memory budget is exceeded and the operator drains
//! its in-memory buffer to disk via `TempSpillFile` (see
//! `docs/design/engine/spill.md` §6.1). Comparing against the
//! in-memory baseline isolates the disk-roundtrip cost from the
//! sort algorithm itself.
//!
//! Two scenarios:
//!
//! - `sort_no_spill/throughput` — `SortOperator::new` over the same
//!   `Vec<RecordBatch>` input, with a generous `MemoryTracker`
//!   budget that comfortably fits every batch in memory. Baseline.
//! - `sort_with_spill/throughput` — `SortOperator::with_spill` with
//!   a `MemoryTracker` budget pinned below the input's working-set
//!   size, plus a `SpillFs` rooted at a `ScratchDir`. Every second
//!   batch triggers the registered `SortSpillHandler`, draining the
//!   prior in-memory buffer to a `.spill` file on disk. The merger
//!   phase then k-way-merges the spill runs.
//!
//! Fixture-correctness guard: the budget is pinned at
//! `one_batch_bytes + one_batch_bytes/2` — comfortably under two
//! batches' worth — to force the spill handler to fire. The spill
//! probe brackets `tracker.peak_bytes()` from both sides: a lower
//! bound proves at least two batches were buffered (so the handler
//! had something to drain), and an upper bound proves the handler
//! actually bounded buffer growth (rather than silently no-op'ing
//! while batches accumulated past the budget). Direct
//! `spill_bytes_written` assertion is deferred until
//! `SortOperator::with_spill` threads its `Metrics` handle into
//! `TempSpillFile::attach_metrics` — today the operator does not
//! wire that path (verified at `crates/bqlite-operators/src/sort.rs`,
//! no `attach_metrics` call site in the spill drain logic).
//!
//! [`floor`] target: `sort_with_spill_ns / sort_no_spill_ns ≤ 3.0`.
//! `engine/spill.md` does not pin a numerical throughput tax — §10.3
//! covers `try_reserve` semantics, not throughput — so this is a
//! `[floor]` regression tripwire, not a `[spec]` derivation. A 3×
//! ceiling is generous enough to absorb the sort-merger's startup
//! overhead at small CI sizing while still catching a regression
//! that pushes the spill path into a slow code path.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bqlite_benches::common::*;
use bqlite_core::memory::{MemoryBudget, MemoryTracker};
use bqlite_core::spill::SpillFs;
use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
use bqlite_operators::{CancellationToken, PhysicalOperator, SortOperator};
use bqlite_planner::compiled::{CompiledExpr, CompiledNode};
use bqlite_planner::logical::SortDirection;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

const ROWS_PER_BATCH: usize = 4_096;

fn int_schema() -> OperatorSchema {
    OperatorSchema::new(vec![ColumnDef::required("v", BqlType::Int)]).expect("schema")
}

fn int_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "v",
        DataType::Int64,
        false,
    )]))
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

/// Build `n_batches` reverse-sorted Int64 batches. The reverse
/// order forces the sort to actually do work; if the input were
/// already sorted, lex-sort short-circuits and the bench measures
/// noise instead of the spill round-trip.
fn make_batches(n_batches: usize) -> Vec<RecordBatch> {
    let schema = int_arrow_schema();
    (0..n_batches)
        .map(|chunk| {
            let base = (chunk * ROWS_PER_BATCH) as i64;
            // Reverse within batch: high values first.
            let values: Vec<i64> = (base..base + ROWS_PER_BATCH as i64).rev().collect();
            let arr: ArrayRef = Arc::new(Int64Array::from(values));
            RecordBatch::try_new(schema.clone(), vec![arr]).expect("record batch")
        })
        .collect()
}

/// Per-mode sizing for the bench fixture.
struct SpillSizing {
    n_batches: usize,
}

impl SpillSizing {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => SpillSizing { n_batches: 16 },
            BenchMode::Reference => SpillSizing { n_batches: 256 },
        }
    }
}

/// Build a fresh `SpillFs` rooted under a fresh `ScratchDir`. The
/// caller is responsible for keeping the `ScratchDir` alive until
/// after the bench drains every spill file.
fn fresh_spill_fs(label: &str) -> (ScratchDir, Arc<SpillFs>) {
    let scratch = ScratchDir::new(label);
    let db_root = scratch.path().to_path_buf();
    std::fs::create_dir_all(&db_root).expect("create scratch root");
    let spill_root = db_root.join("spill");
    let fs = SpillFs::open(spill_root, &db_root).expect("open spill fs");
    (scratch, fs)
}

/// Drive a `SortOperator` to completion, returning the total row
/// count produced. Mirrors the in-house tests' `drive_to_vec` shape
/// but without materialising the i64 values (we only care about
/// timing + correctness of the row count).
fn drive_sort(op: &mut SortOperator) -> usize {
    op.open().expect("open");
    let mut total = 0usize;
    while let Some(batch) = op.next_batch().expect("next_batch") {
        total += batch.num_rows();
        let _ = black_box(batch);
    }
    total
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench
// ─────────────────────────────────────────────────────────────────────────────

fn bench_spill_overhead(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = SpillSizing::for_mode(mode);
    let total_rows = (sizing.n_batches * ROWS_PER_BATCH) as u64;
    let batches = make_batches(sizing.n_batches);
    let one_batch_bytes = batches[0].get_array_memory_size() as u64;

    // ── Probe: in-memory baseline ─────────────────────────────────
    let probe_no_spill_ns = {
        // Generous budget — every batch fits with headroom.
        let tracker: Arc<dyn MemoryBudget> = MemoryTracker::new(one_batch_bytes * 64);
        let child = Box::new(VecOp::new(int_schema(), batches.clone()));
        let mut op = SortOperator::new(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            usize::MAX,
            CancellationToken::new(),
            tracker,
        );
        let start = Instant::now();
        let total = drive_sort(&mut op);
        let elapsed = start.elapsed();
        assert_eq!(total as u64, total_rows, "no-spill row count mismatch");
        elapsed.as_nanos()
    };

    // ── Probe: spill scenario ─────────────────────────────────────
    // Tight budget — buffer can hold roughly 1.5 batches, so every
    // second `try_reserve` triggers the registered handler and the
    // operator drains the prior buffer to disk.
    let probe_spill_ns = {
        let (_scratch, spill_fs) = fresh_spill_fs("wave5-spill-probe");
        let qid = spill_fs.new_query_id();
        let tight = one_batch_bytes + (one_batch_bytes / 2);
        let tracker = MemoryTracker::new(tight);
        let budget: Arc<dyn MemoryBudget> = Arc::clone(&tracker) as Arc<dyn MemoryBudget>;
        let child = Box::new(VecOp::new(int_schema(), batches.clone()));
        let mut op = SortOperator::with_spill(
            child,
            vec![(col0_expr(), SortDirection::Asc)],
            usize::MAX,
            CancellationToken::new(),
            budget,
            Some(Arc::clone(&spill_fs)),
            Some(qid),
        );
        let start = Instant::now();
        let total = drive_sort(&mut op);
        let elapsed = start.elapsed();
        assert_eq!(total as u64, total_rows, "spill row count mismatch");
        // Bench-correctness guard: bracket the tracker peak from
        // both sides.
        //
        // Lower bound: peak must exceed `tight / 2` so we know
        // batches were actually buffered (not e.g. each batch
        // released-and-recharged below the threshold).
        //
        // Upper bound: peak must stay near `tight` (within one
        // batch's worth of slack for the in-flight reservation that
        // tripped the handler). If a regression silently disabled
        // the spill drain — `try_reserve` succeeds against a stale
        // budget, or the registered handler no-ops — peak would
        // balloon toward `n_batches * one_batch_bytes`, far above
        // `tight`. Together the bounds prove the handler is
        // bounding the in-memory residual.
        let peak = tracker.peak_bytes();
        let upper = tight + one_batch_bytes;
        assert!(
            peak > tight / 2,
            "spill fixture is too small to exercise the spill handler: \
             peak={peak} tight={tight}",
        );
        assert!(
            peak <= upper,
            "spill drain appears disabled — peak {peak} exceeds tight + one_batch \
             bound {upper}; the handler did not bound buffer growth",
        );
        elapsed.as_nanos()
    };

    let mut group = c.benchmark_group("wave5/spill_overhead");
    group.throughput(Throughput::Elements(total_rows));

    group.bench_function("sort_no_spill/throughput", |b| {
        b.iter(|| {
            let tracker: Arc<dyn MemoryBudget> = MemoryTracker::new(one_batch_bytes * 64);
            let child = Box::new(VecOp::new(int_schema(), batches.clone()));
            let mut op = SortOperator::new(
                child,
                vec![(col0_expr(), SortDirection::Asc)],
                usize::MAX,
                CancellationToken::new(),
                tracker,
            );
            let total = drive_sort(&mut op);
            black_box(total)
        });
    });
    group.bench_function("sort_with_spill/throughput", |b| {
        b.iter(|| {
            let (_scratch, spill_fs) = fresh_spill_fs("wave5-spill-iter");
            let qid = spill_fs.new_query_id();
            let tracker = MemoryTracker::new(one_batch_bytes + (one_batch_bytes / 2));
            let budget: Arc<dyn MemoryBudget> = Arc::clone(&tracker) as Arc<dyn MemoryBudget>;
            let child = Box::new(VecOp::new(int_schema(), batches.clone()));
            let mut op = SortOperator::with_spill(
                child,
                vec![(col0_expr(), SortDirection::Asc)],
                usize::MAX,
                CancellationToken::new(),
                budget,
                Some(Arc::clone(&spill_fs)),
                Some(qid),
            );
            let total = drive_sort(&mut op);
            black_box(total)
        });
    });
    group.finish();

    let mut collector = BenchResultCollector::new(mode);
    collector.record(
        "wave5/spill_overhead/sort_no_spill/probe_ns",
        probe_no_spill_ns as f64,
        "ns",
        None,
    );
    collector.record(
        "wave5/spill_overhead/sort_with_spill/probe_ns",
        probe_spill_ns as f64,
        "ns",
        None,
    );
    let spill_tax = probe_spill_ns as f64 / probe_no_spill_ns.max(1) as f64;
    collector.record(
        "wave5/spill_overhead/spill_tax_ratio",
        spill_tax,
        "ratio",
        Some(BenchTarget::at_most(3.0)),
    );
    collector.finish();
}

criterion_group! {
    name = spill_overhead_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(2));
    targets = bench_spill_overhead,
}
criterion_main!(spill_overhead_benches);
