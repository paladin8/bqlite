//! CSV ingest throughput benchmark end-to-end.
//!
//! Covers the Wave 2 performance gate "ingest" line:
//! - Partitioner throughput (event routing to `(window, shard)` buckets)
//! - End-to-end ingest pipeline (partition → sort → encode → write)
//!
//! The reference dataset profile uses 10k entities, 20 event types,
//! monotonic-within-entity timestamps, and 7 mixed-type property
//! columns. The benchmark reports `gb_per_sec_scanned` and
//! `bytes_decoded_to_scanned` per execution-model.md §14.1.
//!
//! ## Hard targets (reference mode only, TASK-246)
//!
//! - Ingest throughput (parse → sort → encode → write): >= 100 MB/s

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ── Partitioner throughput ───────────────────────────────────────────────────

fn bench_partitioner(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_small, sizing.ingest_entities_small);
    let event_bytes: u64 = events
        .iter()
        .map(|e| {
            let prop_bytes: usize = e
                .properties
                .iter()
                .map(|(k, v)| {
                    k.len()
                        + match v {
                            bqlite_core::property::PropertyValue::Int(_) => 8,
                            bqlite_core::property::PropertyValue::Float(_) => 8,
                            bqlite_core::property::PropertyValue::String(s) => s.len(),
                            bqlite_core::property::PropertyValue::Bool(_) => 1,
                            _ => 8,
                        }
                })
                .sum();
            (32 + 8 + e.event_type.len() + prop_bytes) as u64
        })
        .sum();

    let mut group = c.benchmark_group("ingest/partitioner");
    group.throughput(Throughput::Bytes(event_bytes));

    let label = format!("push_{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter(|| {
            let mut partitioner = Partitioner::new(4, 30, 1, 512 * 1024 * 1024).unwrap();
            for event in &events {
                partitioner.push_event(event.clone()).unwrap();
            }
            black_box(partitioner.buffered_events());
        })
    });

    group.bench_function("push_and_drain", |b| {
        b.iter(|| {
            let mut partitioner = Partitioner::new(4, 30, 1, 512 * 1024 * 1024).unwrap();
            for event in &events {
                partitioner.push_event(event.clone()).unwrap();
            }
            let drained: Vec<_> = partitioner.drain_sorted().collect();
            black_box(drained.len());
        })
    });

    group.finish();
}

// ── End-to-end ingest pipeline ───────────────────────────────────────────────

fn bench_ingest_end_to_end(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_small, sizing.ingest_entities_small);
    let event_bytes: u64 = events.len() as u64 * 120;

    let mut group = c.benchmark_group("ingest/end_to_end");
    group.throughput(Throughput::Bytes(event_bytes));

    let label = format!("{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let scratch = ScratchDir::new("ingest-e2e");
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

// ── Larger ingest batch ──────────────────────────────────────────────────────

fn bench_ingest_larger(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_large, sizing.ingest_entities_large);
    let event_bytes: u64 = events.len() as u64 * 120;

    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("ingest/larger_batch");
    group.throughput(Throughput::Bytes(event_bytes));

    let label = format!("{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let scratch = ScratchDir::new("ingest-large");
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

    // In reference mode, measure ingest throughput and enforce >= 100 MB/s.
    if mode.is_reference() {
        let start = Instant::now();
        let scratch = ScratchDir::new("ingest-target");
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

        let elapsed_secs = start.elapsed().as_secs_f64();
        let mb_per_sec = (event_bytes as f64) / elapsed_secs / (1024.0 * 1024.0);
        collector.record(
            "ingest/throughput_mb_per_sec",
            mb_per_sec,
            "MB/s",
            Some(BenchTarget::at_least(100.0)),
        );
    }

    collector.finish();
}

criterion_group! {
    name = ingest_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_partitioner,
        bench_ingest_end_to_end,
        bench_ingest_larger,
}
criterion_main!(ingest_benches);
