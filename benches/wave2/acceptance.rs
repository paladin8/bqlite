//! Full acceptance query benchmark on the reference dataset.
//!
//! Covers the Wave 2 performance gate "acceptance" line: the full
//! ingest → scan → decode round-trip that the 100M-row acceptance
//! test exercises. The bench uses a scaled-down dataset (50k rows)
//! to keep CI runtimes reasonable while still exercising every layer
//! of the storage pipeline end-to-end.
//!
//! The acceptance bench measures:
//! - Full round-trip: ingest events → write segments → read segments
//! - Compression ratio: segment bytes / raw event bytes
//! - Zone-map pruning rate on a selective predicate
//!
//! Per execution-model.md §14.1, the bench reports `gb_per_sec_scanned`
//! and `bytes_decoded_to_scanned` per iteration.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::property::PropertyValue;
use bqlite_core::schema::TableSchema;
use bqlite_core::storage::{ColumnProjection, ScanConjunct, ScanPredicate, SegmentScan};
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::segment::reader::SegmentFileReader;
use bqlite_storage::writer::SegmentWriter;
use bqlite_storage::zone_map::accepts_row_group;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

/// Ingest events into a database, return (scratch dir, segment file paths,
/// schema, raw event byte estimate).
fn setup_acceptance_db(
    n: usize,
    entity_count: usize,
) -> (ScratchDir, Vec<PathBuf>, TableSchema, u64) {
    let schema = purchases_schema();
    let scratch = ScratchDir::new("acceptance");
    let mut db = open_db_with_table(scratch.path(), "purchases", schema.clone());

    let events = generate_events(n, entity_count);
    let raw_bytes: u64 = events.len() as u64 * 120;

    let batch_id = db.allocate_batch_id("purchases").unwrap();
    let mut partitioner = Partitioner::new(4, 30, batch_id, 512 * 1024 * 1024).unwrap();
    for event in &events {
        partitioner.push_event(event.clone()).unwrap();
    }

    let mut writer = SegmentWriter::new(&mut db);
    let _metas = writer.write_partitioner("purchases", partitioner).unwrap();

    let seg_paths = find_segment_files(scratch.path());
    assert!(!seg_paths.is_empty(), "no segment files written");

    (scratch, seg_paths, schema, raw_bytes)
}

// ── Full round-trip benchmark ────────────────────────────────────────────────

fn bench_acceptance_round_trip(c: &mut Criterion) {
    let (_scratch, seg_paths, schema, raw_bytes) = setup_acceptance_db(50_000, 500);

    let total_segment_bytes: u64 = seg_paths
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();

    if raw_bytes > 0 {
        let ratio = (total_segment_bytes as f64) / (raw_bytes as f64);
        eprintln!(
            "  acceptance: compression_ratio = {ratio:.3} \
             (segment_bytes={total_segment_bytes}, raw_bytes={raw_bytes})"
        );
    }

    let mut group = c.benchmark_group("acceptance/round_trip");
    group.throughput(Throughput::Bytes(total_segment_bytes));

    group.bench_function("full_scan", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut total_rows = 0u64;
                for path in &seg_paths {
                    let bytes = std::fs::read(path).unwrap();
                    let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
                    let projection = ColumnProjection::all();
                    let mut scan = reader.scan(&projection, None).unwrap();
                    while let Some(batch) = scan.next_row_group().unwrap() {
                        total_rows += batch.num_rows() as u64;
                        black_box(&batch);
                    }
                }
                black_box(total_rows);
            }
            let elapsed = start.elapsed();
            report_metrics(
                total_segment_bytes * iters,
                total_segment_bytes * iters,
                elapsed,
            );
            elapsed
        })
    });

    group.finish();
}

// ── Zone-map pruning effectiveness ───────────────────────────────────────────

fn bench_acceptance_pruning(c: &mut Criterion) {
    let (_scratch, seg_paths, schema, _raw_bytes) = setup_acceptance_db(50_000, 500);

    let total_segment_bytes: u64 = seg_paths
        .iter()
        .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();

    let predicate = Arc::new(ScanPredicate::new(vec![ScanConjunct::Range {
        column: "amount".to_string(),
        op: bqlite_core::storage::RangeOp::Gt,
        value: PropertyValue::Int(4500),
    }]));

    let mut group = c.benchmark_group("acceptance/pruning");
    group.throughput(Throughput::Bytes(total_segment_bytes));

    {
        let mut total_rgs = 0usize;
        let mut accepted_rgs = 0usize;
        for path in &seg_paths {
            let bytes = std::fs::read(path).unwrap();
            let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
            let projection = ColumnProjection::all();
            let scan = reader.scan(&projection, None).unwrap();
            let rg_count = scan.row_group_count();
            total_rgs += rg_count;
            for idx in 0..rg_count {
                let zones = scan.row_group_zone_maps(idx).unwrap();
                if accepts_row_group(predicate.as_ref(), &zones) {
                    accepted_rgs += 1;
                }
            }
        }
        let pruning_rate = if total_rgs > 0 {
            1.0 - (accepted_rgs as f64 / total_rgs as f64)
        } else {
            0.0
        };
        eprintln!(
            "  acceptance: zone_map_pruning_rate = {pruning_rate:.2} \
             (accepted={accepted_rgs}/{total_rgs})"
        );
    }

    group.bench_function("selective_scan", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut total_rows = 0u64;
                for path in &seg_paths {
                    let bytes = std::fs::read(path).unwrap();
                    let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
                    let projection = ColumnProjection::all();
                    let mut scan = reader.scan(&projection, Some(predicate.clone())).unwrap();
                    while let Some(batch) = scan.next_row_group().unwrap() {
                        total_rows += batch.num_rows() as u64;
                        black_box(&batch);
                    }
                }
                black_box(total_rows);
            }
            let elapsed = start.elapsed();
            report_metrics(
                total_segment_bytes * iters,
                total_segment_bytes * iters,
                elapsed,
            );
            elapsed
        })
    });

    group.finish();
}

// ── Ingest throughput ────────────────────────────────────────────────────────

fn bench_acceptance_ingest(c: &mut Criterion) {
    let events = generate_events(50_000, 500);
    let event_bytes: u64 = events.len() as u64 * 120;

    let mut group = c.benchmark_group("acceptance/ingest");
    group.throughput(Throughput::Bytes(event_bytes));

    group.bench_function("50k_events", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let scratch = ScratchDir::new("acceptance-ingest");
                let schema = purchases_schema();
                let mut db = open_db_with_table(scratch.path(), "purchases", schema);

                let batch_id = db.allocate_batch_id("purchases").unwrap();
                let mut partitioner = Partitioner::new(4, 30, batch_id, 512 * 1024 * 1024).unwrap();
                for event in &events {
                    partitioner.push_event(event.clone()).unwrap();
                }

                let mut writer = SegmentWriter::new(&mut db);
                let metas = writer.write_partitioner("purchases", partitioner).unwrap();
                black_box(metas.len());
            }
            let elapsed = start.elapsed();
            report_metrics(event_bytes * iters, event_bytes * iters, elapsed);
            elapsed
        })
    });

    group.finish();
}

criterion_group! {
    name = acceptance_benches;
    config = wave2_criterion();
    targets =
        bench_acceptance_round_trip,
        bench_acceptance_pruning,
        bench_acceptance_ingest,
}
criterion_main!(acceptance_benches);
