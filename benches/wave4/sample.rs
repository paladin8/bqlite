//! Entity-level SAMPLE pushdown savings (TASK-441).
//!
//! The SAMPLE pushdown path at `crates/bqlite-storage/src/sample.rs`
//! is the hot loop: the planner's `pushdown_sample` optimization
//! hands a `SampleFilter` to `ScanOperator` so the per-row hash +
//! threshold test runs on the entity-id column before any other
//! column is materialized. That's what this bench measures —
//! `SampleFilter::apply_to_array` throughput and selectivity across
//! representative fractions.
//!
//! Two bench groups:
//!
//! - `sample/apply_to_array` — per-row `xxHash64 + threshold` cost
//!   on a 65 536-row `StringViewArray` of entity ids at fractions
//!   0.01 / 0.10 / 0.50 / 1.00. Reference-mode floor (at
//!   fraction 0.10): ≥ 50 M entities/sec/core, per
//!   `event-select-sample.md` §21.2 row 1.
//!
//! - `sample/selectivity` — ratio of `(rows_accepted /
//!   rows_scanned)` at fractions 0.01 and 0.10, asserting the
//!   law-of-large-numbers 3σ deviation bound at 65 536-entity
//!   scale. Derived [floor] targets per the plan.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench sample
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, BooleanArray, StringViewArray};
use bqlite_benches::common::*;
use bqlite_core::BqlType;
use bqlite_operators::operator::PhysicalOperator;
use bqlite_operators::scan::ScanOperator;
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use bqlite_storage::SampleFilter;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Number of rows per sample iteration — one entity per row. 65 536
/// is the default row-group size, so the bench mirrors the per-row-
/// group pushdown unit.
const N_ENTITIES: usize = 65_536;

/// Fixed seed so the selectivity math is deterministic and the bench
/// is reproducible across runs.
const SAMPLE_SEED: i64 = 0x4441; // "DA" (0x44 = 'D', 0x41 = 'A') — arbitrary.

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

/// Build a `StringViewArray` of 65 536 distinct entity ids that match
/// the `generate_events` profile (`user_000000`..). One row per
/// entity so `accepts_str` is the dominant per-row cost.
fn entity_id_column() -> Arc<StringViewArray> {
    let values: Vec<String> = (0..N_ENTITIES).map(|i| format!("user_{i:06}")).collect();
    Arc::new(StringViewArray::from(values))
}

/// Count `true` bits in a `BooleanArray` selectivity mask.
fn count_accepted(mask: &BooleanArray) -> usize {
    mask.true_count()
}

// ─────────────────────────────────────────────────────────────────────────────
// apply_to_array throughput
// ─────────────────────────────────────────────────────────────────────────────

/// Per-row SAMPLE pushdown cost — the same xxHash64 + threshold
/// compare the scan operator runs on every row when SAMPLE is pushed
/// into the scan.
fn bench_apply_to_array(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let array = entity_id_column();
    let array_ref: &dyn Array = array.as_ref();

    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("sample/apply_to_array");
    group.throughput(Throughput::Elements(N_ENTITIES as u64));

    for &fraction in &[0.01f64, 0.10, 0.50, 1.00] {
        // NB: fraction=1.00 hits `SampleFilter::is_pass_through`'s
        // fast path (every row accepted without hashing), so its
        // number is expected to be noticeably higher than the 0.01 /
        // 0.10 / 0.50 points. It is reported for completeness, not
        // for direct comparison.
        let filter =
            SampleFilter::new(fraction, SAMPLE_SEED, "entity_id", BqlType::String).unwrap();
        // Rendering floats into `BenchmarkId` — use integer percents
        // so Criterion's CLI filter stays user-friendly.
        let bp = (fraction * 100.0) as u64;
        group.bench_with_input(BenchmarkId::new("pct", bp), &fraction, |b, _| {
            b.iter(|| {
                let mask = filter.apply_to_array(black_box(array_ref)).unwrap();
                black_box(mask);
            })
        });
    }
    group.finish();

    // Hard-target measurement: fraction=0.10 throughput floor at
    // 50 M entities/sec/core per event-select-sample.md §21.2.
    if mode.is_reference() {
        let filter = SampleFilter::new(0.10, SAMPLE_SEED, "entity_id", BqlType::String).unwrap();
        let runs: u64 = 100;
        let start = Instant::now();
        for _ in 0..runs {
            let mask = filter.apply_to_array(array_ref).unwrap();
            black_box(mask);
        }
        let elapsed_s = start.elapsed().as_secs_f64();
        let entities_per_sec = (N_ENTITIES as f64 * runs as f64) / elapsed_s;
        collector.record(
            "sample/apply_to_array/fraction_0.10/entities_per_sec",
            entities_per_sec,
            "entities/sec",
            Some(BenchTarget::at_least(50_000_000.0)),
        );
    }

    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Selectivity
// ─────────────────────────────────────────────────────────────────────────────

/// Population-level selectivity — records the empirical
/// `(rows_accepted / rows_scanned)` deviation from the configured
/// fraction. The hard-target ceiling is the 3σ law-of-large-numbers
/// bound at `N_ENTITIES` entities.
fn bench_selectivity(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let array = entity_id_column();
    let array_ref: &dyn Array = array.as_ref();
    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("sample/selectivity");
    group.throughput(Throughput::Elements(N_ENTITIES as u64));

    for &fraction in &[0.01f64, 0.10] {
        let filter =
            SampleFilter::new(fraction, SAMPLE_SEED, "entity_id", BqlType::String).unwrap();
        let bp = (fraction * 100.0) as u64;

        group.bench_with_input(BenchmarkId::new("pct", bp), &fraction, |b, _| {
            b.iter(|| {
                let mask = filter.apply_to_array(black_box(array_ref)).unwrap();
                black_box(count_accepted(&mask));
            })
        });

        // Standalone selectivity measurement (one run; the number
        // is deterministic given the fixed seed, so repetition is
        // unnecessary).
        let mask = filter.apply_to_array(array_ref).unwrap();
        let accepted = count_accepted(&mask);
        let observed = accepted as f64 / N_ENTITIES as f64;
        let abs_deviation = (observed - fraction).abs();

        // 3σ bound of a Bernoulli(fraction) sum over N trials:
        // σ = sqrt(fraction * (1-fraction) / N).
        // For fraction=0.01, N=65 536: σ ≈ 0.00039 → 3σ ≈ 0.00118.
        // For fraction=0.10, N=65 536: σ ≈ 0.00117 → 3σ ≈ 0.00352.
        let sigma = (fraction * (1.0 - fraction) / N_ENTITIES as f64).sqrt();
        let bound = 3.0 * sigma;

        collector.record(
            &format!("sample/selectivity/fraction_{fraction}/abs_deviation"),
            abs_deviation,
            "abs",
            Some(BenchTarget::at_most(bound)),
        );
        collector.record(
            &format!("sample/selectivity/fraction_{fraction}/observed"),
            observed,
            "ratio",
            None,
        );
    }
    group.finish();
    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Pushdown savings — ScanOperator with vs without a SampleFilter
// ─────────────────────────────────────────────────────────────────────────────

/// Drive a `ScanOperator` to completion and return `(row_count,
/// batch_count)` for accounting.
fn drain_scan(mut op: ScanOperator) -> (usize, usize) {
    op.open().expect("open scan");
    let mut row_count = 0usize;
    let mut batch_count = 0usize;
    while let Some(batch) = op.next_batch().expect("next_batch") {
        row_count += batch.num_rows();
        batch_count += 1;
    }
    (row_count, batch_count)
}

/// Pushdown-savings bench: ScanOperator with SampleFilter attached
/// (the planner's `pushdown_sample` output) compared against a plain
/// full scan. The ratio shows the fraction of rows the scan layer
/// actually emits — the "savings" vs materializing every row and
/// filtering downstream.
fn bench_scan_pushdown(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);

    // Build a real database once and reuse it across Criterion
    // samples — the scan is read-only, so no per-iteration setup is
    // needed.
    let scratch = ScratchDir::new("sample-scan");
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch.path(), "purchases", schema);
    let events = generate_events(sizing.scan_events, sizing.scan_entities);
    let batch_id = db.allocate_batch_id("purchases").unwrap();
    let mut partitioner = Partitioner::new(1, 30, batch_id, 512 * 1024 * 1024).unwrap();
    for ev in &events {
        partitioner.push_event(ev.clone()).unwrap();
    }
    let mut writer = SegmentWriter::new(&mut db);
    let _metas = writer.write_partitioner("purchases", partitioner).unwrap();
    let reader = Arc::from(db.segment_reader("purchases").unwrap());

    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("sample/scan_pushdown");
    group.throughput(Throughput::Elements(events.len() as u64));

    // Baseline: full scan with no SampleFilter. Every row materialized.
    group.bench_function("baseline_no_sample", |b| {
        b.iter(|| {
            let op = ScanOperator::full_scan(Arc::clone(&reader)).unwrap();
            let (rows, batches) = drain_scan(op);
            black_box((rows, batches));
        })
    });

    // Pushed-down SAMPLE at fractions 0.01 and 0.10. The scan layer
    // runs the threshold test on the entity-id column before
    // materializing other columns.
    for &fraction in &[0.01f64, 0.10] {
        let bp = (fraction * 100.0) as u64;
        group.bench_with_input(BenchmarkId::new("pushdown_pct", bp), &fraction, |b, &f| {
            b.iter(|| {
                let filter = Arc::new(
                    SampleFilter::new(f, SAMPLE_SEED, "user_id", BqlType::String).unwrap(),
                );
                let mut op = ScanOperator::full_scan(Arc::clone(&reader)).unwrap();
                op.with_sample_filter(filter);
                let (rows, batches) = drain_scan(op);
                black_box((rows, batches));
            })
        });

        // Standalone pushdown-savings ratio: emitted rows / baseline
        // rows. Recorded without a hard target because the fraction-
        // selectivity ceiling is already enforced by
        // `bench_selectivity` above; this is a cross-check that the
        // scan path respects the same math.
        let filter =
            Arc::new(SampleFilter::new(fraction, SAMPLE_SEED, "user_id", BqlType::String).unwrap());
        let mut op_sampled = ScanOperator::full_scan(Arc::clone(&reader)).unwrap();
        op_sampled.with_sample_filter(filter);
        let (sampled_rows, _) = drain_scan(op_sampled);
        let op_baseline = ScanOperator::full_scan(Arc::clone(&reader)).unwrap();
        let (baseline_rows, _) = drain_scan(op_baseline);
        let emitted_ratio = sampled_rows as f64 / baseline_rows.max(1) as f64;
        collector.record(
            &format!("sample/scan_pushdown/fraction_{fraction}/emitted_ratio"),
            emitted_ratio,
            "ratio",
            None,
        );
    }

    group.finish();
    collector.finish();
}

criterion_group! {
    name = sample_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_apply_to_array,
        bench_selectivity,
        bench_scan_pushdown,
}
criterion_main!(sample_benches);
