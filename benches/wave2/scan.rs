//! Columnar decode throughput benchmarks for the segment reader.
//!
//! Covers the Wave 2 performance gate "scan" line:
//! - Columnar decode throughput for int64, string, and float columns
//! - With and without zone-map pruning
//!
//! Each benchmark writes a segment via the writer, reads it back
//! through the SegmentFileReader, and measures end-to-end decode
//! throughput. The `gb_per_sec_scanned` and `bytes_decoded_to_scanned`
//! metrics from execution-model.md §14.1 are reported per iteration.

use std::sync::Arc;
use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::property::PropertyValue;
use bqlite_core::storage::{ColumnProjection, ScanConjunct, ScanPredicate, SegmentScan};
use bqlite_storage::segment::reader::SegmentFileReader;
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

/// Write a segment of `n` events, return the segment bytes for reading.
fn write_test_segment(
    n: usize,
    entity_count: usize,
) -> (Vec<u8>, bqlite_core::schema::TableSchema) {
    let schema = purchases_schema();
    let scratch = ScratchDir::new("scan");
    let mut db = open_db_with_table(scratch.path(), "purchases", schema.clone());

    let events = generate_events(n, entity_count);
    let batch_id = db.allocate_batch_id("purchases").unwrap();

    let mut writer = SegmentWriter::new(&mut db);
    let _meta = writer
        .write_bucket("purchases", 0, 0, batch_id, &events)
        .unwrap();

    // Find and read the segment file.
    let seg_paths = find_segment_files(scratch.path());
    assert!(!seg_paths.is_empty(), "no segment files written");
    let bytes = std::fs::read(&seg_paths[0]).unwrap();
    (bytes, schema)
}

// ── Full-scan decode throughput ──────────────────────────────────────────────

fn bench_scan_full(c: &mut Criterion) {
    let (bytes, schema) = write_test_segment(50_000, 500);
    let file_bytes = bytes.len() as u64;

    let mut group = c.benchmark_group("scan/full");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("all_columns", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
                let projection = ColumnProjection::all();
                let mut scan = reader.scan(&projection, None).unwrap();
                let mut total_rows = 0u64;
                while let Some(batch) = scan.next_row_group().unwrap() {
                    total_rows += batch.num_rows() as u64;
                    black_box(&batch);
                }
                black_box(total_rows);
            }
            let elapsed = start.elapsed();
            report_metrics(file_bytes * iters, file_bytes * iters, elapsed);
            elapsed
        })
    });

    group.finish();
}

fn bench_scan_projected(c: &mut Criterion) {
    let (bytes, schema) = write_test_segment(50_000, 500);
    let file_bytes = bytes.len() as u64;

    let mut group = c.benchmark_group("scan/projected");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("two_columns", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
                let projection = ColumnProjection::with_columns(["user_id", "amount"]);
                let mut scan = reader.scan(&projection, None).unwrap();
                while let Some(batch) = scan.next_row_group().unwrap() {
                    black_box(&batch);
                }
            }
            let elapsed = start.elapsed();
            report_metrics(file_bytes * iters, file_bytes * iters / 5, elapsed);
            elapsed
        })
    });

    group.finish();
}

// ── Zone-map pruning throughput ──────────────────────────────────────────────

fn bench_scan_with_zone_map_pruning(c: &mut Criterion) {
    let (bytes, schema) = write_test_segment(50_000, 500);
    let file_bytes = bytes.len() as u64;

    let mut group = c.benchmark_group("scan/zone_map_pruning");
    group.throughput(Throughput::Bytes(file_bytes));

    let predicate = Arc::new(ScanPredicate::new(vec![ScanConjunct::Range {
        column: "amount".to_string(),
        op: bqlite_core::storage::RangeOp::Gt,
        value: PropertyValue::Int(4000),
    }]));

    group.bench_function("range_predicate", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
                let projection = ColumnProjection::all();
                let mut scan = reader.scan(&projection, Some(predicate.clone())).unwrap();
                let mut total_rows = 0u64;
                while let Some(batch) = scan.next_row_group().unwrap() {
                    total_rows += batch.num_rows() as u64;
                    black_box(&batch);
                }
                black_box(total_rows);
            }
            let elapsed = start.elapsed();
            report_metrics(file_bytes * iters, file_bytes * iters, elapsed);
            elapsed
        })
    });

    group.bench_function("pruning_decision_only", |b| {
        let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
        let projection = ColumnProjection::all();
        let scan = reader.scan(&projection, None).unwrap();
        let rg_count = scan.row_group_count();

        b.iter(|| {
            let mut accepted = 0usize;
            for idx in 0..rg_count {
                let zones = scan.row_group_zone_maps(idx).unwrap();
                if bqlite_storage::zone_map::accepts_row_group(predicate.as_ref(), &zones) {
                    accepted += 1;
                }
            }
            black_box(accepted)
        })
    });

    group.finish();
}

// ── Per-type decode throughput ───────────────────────────────────────────────

fn bench_scan_int64_column(c: &mut Criterion) {
    let (bytes, schema) = write_test_segment(50_000, 500);
    let file_bytes = bytes.len() as u64;

    let mut group = c.benchmark_group("scan/column_type/int64");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("amount", |b| {
        b.iter(|| {
            let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
            let projection = ColumnProjection::with_columns(["amount"]);
            let mut scan = reader.scan(&projection, None).unwrap();
            while let Some(batch) = scan.next_row_group().unwrap() {
                black_box(&batch);
            }
        })
    });

    group.finish();
}

fn bench_scan_string_column(c: &mut Criterion) {
    let (bytes, schema) = write_test_segment(50_000, 500);
    let file_bytes = bytes.len() as u64;

    let mut group = c.benchmark_group("scan/column_type/string");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("category", |b| {
        b.iter(|| {
            let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
            let projection = ColumnProjection::with_columns(["category"]);
            let mut scan = reader.scan(&projection, None).unwrap();
            while let Some(batch) = scan.next_row_group().unwrap() {
                black_box(&batch);
            }
        })
    });

    group.finish();
}

fn bench_scan_float64_column(c: &mut Criterion) {
    let (bytes, schema) = write_test_segment(50_000, 500);
    let file_bytes = bytes.len() as u64;

    let mut group = c.benchmark_group("scan/column_type/float64");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("price", |b| {
        b.iter(|| {
            let reader = SegmentFileReader::from_bytes(bytes.clone(), schema.clone()).unwrap();
            let projection = ColumnProjection::with_columns(["price"]);
            let mut scan = reader.scan(&projection, None).unwrap();
            while let Some(batch) = scan.next_row_group().unwrap() {
                black_box(&batch);
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = scan_benches;
    config = wave2_criterion();
    targets =
        bench_scan_full,
        bench_scan_projected,
        bench_scan_with_zone_map_pruning,
        bench_scan_int64_column,
        bench_scan_string_column,
        bench_scan_float64_column,
}
criterion_main!(scan_benches);
