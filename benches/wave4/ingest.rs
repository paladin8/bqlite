//! JSONL + Parquet ingest throughput (TASK-441).
//!
//! Exercises the Wave 4 ingest readers end-to-end: reader →
//! `Partitioner` → `SegmentWriter`. The benches mirror the structure
//! of `wave2/ingest.rs` (CSV) so the throughput numbers are directly
//! comparable across formats. Parquet is expected to beat JSONL
//! because its column chunks decode without per-row JSON parsing.
//!
//! Reference-mode targets (per the TASK-441 plan):
//! - `ingest/jsonl/end_to_end` throughput ≥ 100 MB/s  [floor]
//! - `ingest/parquet/end_to_end` throughput ≥ 150 MB/s  [floor]
//!
//! Both targets are regression tripwires; no Wave 4 design doc pins a
//! specific MB/s number for these readers. CI mode runs the same
//! fixtures at smaller scales and relies on Criterion's statistical
//! comparison — hard targets only fire when
//! `BQLITE_BENCH_MODE=reference`.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bqlite_benches::common::*;
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_storage::ingest::json::JsonlEventReader;
use bqlite_storage::ingest::parquet::ParquetEventReader;
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use parquet::arrow::ArrowWriter;

const SHARD_COUNT: u16 = 4;
const PARTITION_BUDGET_BYTES: usize = 512 * 1024 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture writers
// ─────────────────────────────────────────────────────────────────────────────

/// Serialize a slice of events into a JSONL file whose keys match the
/// `purchases_schema()` column names. Returns the file path and the
/// file's byte size so callers can compute MB/s.
fn write_jsonl_fixture(path: &Path, events: &[Event]) -> u64 {
    let mut file = File::create(path).expect("create jsonl fixture");
    for event in events {
        let entity = match &event.entity {
            EntityId::String(s) => format!("\"{s}\""),
            EntityId::Int(i) => i.to_string(),
        };
        // Pre-size a single line buffer. The literal JSON object
        // layout mirrors the purchases_schema property set exactly.
        let mut line = String::with_capacity(256);
        line.push_str("{\"user_id\":");
        line.push_str(&entity);
        line.push_str(",\"ts\":");
        line.push_str(&event.timestamp.as_nanos().to_string());
        line.push_str(",\"event_type\":\"");
        line.push_str(&event.event_type);
        line.push('"');
        for (key, value) in &event.properties {
            line.push_str(",\"");
            line.push_str(key);
            line.push_str("\":");
            match value {
                PropertyValue::Int(i) => line.push_str(&i.to_string()),
                PropertyValue::Float(f) => line.push_str(&f.to_string()),
                PropertyValue::String(s) => {
                    line.push('"');
                    line.push_str(s);
                    line.push('"');
                }
                PropertyValue::Bool(b) => line.push_str(if *b { "true" } else { "false" }),
                PropertyValue::Null => line.push_str("null"),
                PropertyValue::Timestamp(t) => line.push_str(&t.to_string()),
                // The reference `generate_events` fixture never emits
                // List or Map properties; this arm exists only for
                // exhaustiveness. Serialize as JSON null rather than
                // expanding the writer with a full recursive encoder.
                PropertyValue::List(_) | PropertyValue::Map(_) => line.push_str("null"),
            }
        }
        line.push_str("}\n");
        file.write_all(line.as_bytes()).expect("write jsonl line");
    }
    file.sync_all().ok();
    std::fs::metadata(path).expect("stat jsonl fixture").len()
}

/// Build one Arrow `RecordBatch` containing every event in `events`
/// in the column shape `purchases_schema()` expects.
fn events_to_batch(events: &[Event]) -> RecordBatch {
    let n = events.len();
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, true),
        Field::new("price", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("quantity", DataType::Int64, true),
        Field::new("discount", DataType::Float64, true),
        Field::new("region", DataType::Utf8, true),
        Field::new("flag", DataType::Int64, true),
    ]));

    let mut user_ids: Vec<String> = Vec::with_capacity(n);
    let mut ts: Vec<i64> = Vec::with_capacity(n);
    let mut event_types: Vec<String> = Vec::with_capacity(n);
    let mut amount: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut price: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut category: Vec<Option<String>> = Vec::with_capacity(n);
    let mut quantity: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut discount: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut region: Vec<Option<String>> = Vec::with_capacity(n);
    let mut flag: Vec<Option<i64>> = Vec::with_capacity(n);

    for ev in events {
        user_ids.push(match &ev.entity {
            EntityId::String(s) => s.clone(),
            EntityId::Int(i) => i.to_string(),
        });
        ts.push(ev.timestamp.as_nanos());
        event_types.push(ev.event_type.clone());
        amount.push(ev.get("amount").and_then(|v| match v {
            PropertyValue::Int(i) => Some(*i),
            _ => None,
        }));
        price.push(ev.get("price").and_then(|v| match v {
            PropertyValue::Float(f) => Some(*f),
            _ => None,
        }));
        category.push(ev.get("category").and_then(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        }));
        quantity.push(ev.get("quantity").and_then(|v| match v {
            PropertyValue::Int(i) => Some(*i),
            _ => None,
        }));
        discount.push(ev.get("discount").and_then(|v| match v {
            PropertyValue::Float(f) => Some(*f),
            _ => None,
        }));
        region.push(ev.get("region").and_then(|v| match v {
            PropertyValue::String(s) => Some(s.clone()),
            _ => None,
        }));
        // Parquet's reader expects `flag` as Int64 because the
        // purchases_schema declares `Bool`, and the reader-side
        // coercion tolerates 0/1 integers. Encoding as i64 keeps the
        // fixture writer simple and the parser round-trip exact.
        flag.push(ev.get("flag").and_then(|v| match v {
            PropertyValue::Bool(b) => Some(if *b { 1 } else { 0 }),
            _ => None,
        }));
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(user_ids)),
            Arc::new(TimestampNanosecondArray::from(ts)),
            Arc::new(StringArray::from(event_types)),
            Arc::new(Int64Array::from(amount)),
            Arc::new(Float64Array::from(price)),
            Arc::new(StringArray::from(category)),
            Arc::new(Int64Array::from(quantity)),
            Arc::new(Float64Array::from(discount)),
            Arc::new(StringArray::from(region)),
            Arc::new(Int64Array::from(flag)),
        ],
    )
    .expect("build events batch")
}

/// Write a single-file Parquet fixture. Returns the file size in bytes.
fn write_parquet_fixture(path: &Path, events: &[Event]) -> u64 {
    let batch = events_to_batch(events);
    let file = File::create(path).expect("create parquet fixture");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer");
    writer.write(&batch).expect("parquet write");
    writer.close().expect("parquet close");
    std::fs::metadata(path).expect("stat parquet fixture").len()
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end ingest drivers
// ─────────────────────────────────────────────────────────────────────────────

/// Drive a fresh `JsonlEventReader` over `path`, partition into shards,
/// and write segments. Returns the number of segments produced.
fn ingest_jsonl_end_to_end(jsonl_path: &Path, scratch: &Path) -> usize {
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch, "purchases", schema.clone());

    let batch_id = db.allocate_batch_id("purchases").unwrap();
    let mut partitioner =
        Partitioner::new(SHARD_COUNT, 30, batch_id, PARTITION_BUDGET_BYTES).unwrap();
    let mut reader = JsonlEventReader::open(jsonl_path, &schema, &[]).expect("jsonl open");
    while let Some(event) = reader.next_event().expect("jsonl next_event") {
        partitioner.push_event(event).unwrap();
    }

    let mut writer = SegmentWriter::new(&mut db);
    let metas = writer.write_partitioner("purchases", partitioner).unwrap();
    metas.len()
}

/// Drive a fresh `ParquetEventReader` over `path`, partition, and
/// write segments. Returns the number of segments produced.
fn ingest_parquet_end_to_end(parquet_path: &Path, scratch: &Path) -> usize {
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch, "purchases", schema.clone());

    let batch_id = db.allocate_batch_id("purchases").unwrap();
    let mut partitioner =
        Partitioner::new(SHARD_COUNT, 30, batch_id, PARTITION_BUDGET_BYTES).unwrap();
    let mut reader = ParquetEventReader::open(parquet_path, &schema, &[]).expect("parquet open");
    while let Some(event) = reader.next_event().expect("parquet next_event") {
        partitioner.push_event(event).unwrap();
    }

    let mut writer = SegmentWriter::new(&mut db);
    let metas = writer.write_partitioner("purchases", partitioner).unwrap();
    metas.len()
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixture paths — captured in a tiny holder so the Criterion iterator
// can re-use the on-disk file across iterations without regenerating.
// ─────────────────────────────────────────────────────────────────────────────

struct Fixture {
    _scratch: ScratchDir,
    data_path: PathBuf,
    /// File size (bytes). Used as the `Throughput::Bytes` value so
    /// Criterion reports on-disk bytes consumed per second.
    file_bytes: u64,
}

fn build_jsonl_fixture(events: &[Event]) -> Fixture {
    let scratch = ScratchDir::new("jsonl-bench");
    std::fs::create_dir_all(scratch.path()).expect("mkdir jsonl scratch");
    let data_path = scratch.path().join("events.jsonl");
    let file_bytes = write_jsonl_fixture(&data_path, events);
    Fixture {
        _scratch: scratch,
        data_path,
        file_bytes,
    }
}

fn build_parquet_fixture(events: &[Event]) -> Fixture {
    let scratch = ScratchDir::new("parquet-bench");
    std::fs::create_dir_all(scratch.path()).expect("mkdir parquet scratch");
    let data_path = scratch.path().join("events.parquet");
    let file_bytes = write_parquet_fixture(&data_path, events);
    Fixture {
        _scratch: scratch,
        data_path,
        file_bytes,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// JSONL benches
// ─────────────────────────────────────────────────────────────────────────────

fn bench_jsonl_reader(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_small, sizing.ingest_entities_small);
    let fixture = build_jsonl_fixture(&events);
    let schema = purchases_schema();

    let mut group = c.benchmark_group("ingest/jsonl/reader");
    group.throughput(Throughput::Bytes(fixture.file_bytes));

    let label = format!("{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter(|| {
            let mut reader =
                JsonlEventReader::open(&fixture.data_path, &schema, &[]).expect("jsonl open");
            let mut count = 0usize;
            while let Some(event) = reader.next_event().expect("jsonl next") {
                count += 1;
                black_box(event);
            }
            count
        })
    });

    group.finish();
}

fn bench_jsonl_end_to_end(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_large, sizing.ingest_entities_large);
    let fixture = build_jsonl_fixture(&events);
    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("ingest/jsonl/end_to_end");
    group.throughput(Throughput::Bytes(fixture.file_bytes));

    let label = format!("{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let out_scratch = ScratchDir::new("jsonl-e2e-out");
                let segs = ingest_jsonl_end_to_end(&fixture.data_path, out_scratch.path());
                black_box(segs);
            }
            let elapsed = start.elapsed();
            report_metrics(
                fixture.file_bytes * iters,
                fixture.file_bytes * iters,
                elapsed,
            );
            elapsed
        })
    });

    group.finish();

    // Hard-target measurement (reference mode only).
    if mode.is_reference() {
        let out_scratch = ScratchDir::new("jsonl-target");
        let start = Instant::now();
        let segs = ingest_jsonl_end_to_end(&fixture.data_path, out_scratch.path());
        let elapsed_s = start.elapsed().as_secs_f64();
        black_box(segs);
        let mb_per_sec = fixture.file_bytes as f64 / elapsed_s / (1024.0 * 1024.0);
        collector.record(
            "ingest/jsonl/end_to_end/mb_per_sec",
            mb_per_sec,
            "MB/s",
            Some(BenchTarget::at_least(100.0)),
        );
    }

    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Parquet benches
// ─────────────────────────────────────────────────────────────────────────────

fn bench_parquet_reader(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_small, sizing.ingest_entities_small);
    let fixture = build_parquet_fixture(&events);
    let schema = purchases_schema();

    let mut group = c.benchmark_group("ingest/parquet/reader");
    group.throughput(Throughput::Bytes(fixture.file_bytes));

    let label = format!("{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter(|| {
            let mut reader =
                ParquetEventReader::open(&fixture.data_path, &schema, &[]).expect("parquet open");
            let mut count = 0usize;
            while let Some(event) = reader.next_event().expect("parquet next") {
                count += 1;
                black_box(event);
            }
            count
        })
    });

    group.finish();
}

fn bench_parquet_end_to_end(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let events = generate_events(sizing.ingest_events_large, sizing.ingest_entities_large);
    let fixture = build_parquet_fixture(&events);
    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("ingest/parquet/end_to_end");
    group.throughput(Throughput::Bytes(fixture.file_bytes));

    let label = format!("{}k_events", events.len() / 1000);
    group.bench_function(&label, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let out_scratch = ScratchDir::new("parquet-e2e-out");
                let segs = ingest_parquet_end_to_end(&fixture.data_path, out_scratch.path());
                black_box(segs);
            }
            let elapsed = start.elapsed();
            report_metrics(
                fixture.file_bytes * iters,
                fixture.file_bytes * iters,
                elapsed,
            );
            elapsed
        })
    });

    group.finish();

    if mode.is_reference() {
        let out_scratch = ScratchDir::new("parquet-target");
        let start = Instant::now();
        let segs = ingest_parquet_end_to_end(&fixture.data_path, out_scratch.path());
        let elapsed_s = start.elapsed().as_secs_f64();
        black_box(segs);
        let mb_per_sec = fixture.file_bytes as f64 / elapsed_s / (1024.0 * 1024.0);
        collector.record(
            "ingest/parquet/end_to_end/mb_per_sec",
            mb_per_sec,
            "MB/s",
            Some(BenchTarget::at_least(150.0)),
        );
    }

    collector.finish();
}

criterion_group! {
    name = wave4_ingest_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_jsonl_reader,
        bench_jsonl_end_to_end,
        bench_parquet_reader,
        bench_parquet_end_to_end,
}
criterion_main!(wave4_ingest_benches);
