//! Ingest throughput across INSERT VALUES, JSONL, and Parquet
//! formats (TASK-546).
//!
//! Materialises a fresh event batch (independent of the
//! `bench-perf` persistent fixture) into three on-disk shapes, then
//! drives end-to-end ingest for each:
//!
//! - `insert_values` — multi-row `INSERT INTO purchases VALUES (...)`
//!   pushed through `Engine::query`. Stresses the parser + planner +
//!   ingest path. Capped at a small row count because the parser is
//!   the bottleneck — measuring INSERT VALUES throughput at 10M rows
//!   would just be measuring SQL text parsing.
//! - `jsonl` — `JsonlEventReader` → `Partitioner` → `SegmentWriter`.
//! - `parquet` — `ParquetEventReader` → `Partitioner` → `SegmentWriter`.
//!
//! Per-scale row counts deliberately diverge from
//! `BenchScale::rows()` because ingest is destructive (creates a fresh
//! DB per iteration) and we want each iteration to finish in
//! Criterion-friendly wall-clock time:
//!
//! | scale  | insert_values | jsonl / parquet |
//! |--------|---------------|------------------|
//! | small  | 10 000        | 100 000          |
//! | medium | 50 000        | 1 000 000        |
//! | large  | 50 000        | 10 000 000       |
//! | xlarge | 50 000        | 10 000 000       |
//!
//! This bench is intentionally not gated on
//! `PersistentFixture::load_or_build` because the fixture's pre-built
//! database is not useful for measuring fresh-DB ingest throughput.

use std::fs::File;
use std::io::{BufWriter, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bqlite_benches::common::*;
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_engine::Engine;
use bqlite_storage::ingest::json::JsonlEventReader;
use bqlite_storage::ingest::parquet::ParquetEventReader;
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, Criterion, Throughput};
use parquet::arrow::ArrowWriter;

const SHARD_COUNT: u16 = 4;
const PARTITION_BUDGET_BYTES: usize = 512 * 1024 * 1024;
const INSERT_VALUES_CAP: usize = 50_000;

fn ingest_rows_for_scale(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Small => 100_000,
        BenchScale::Medium => 1_000_000,
        BenchScale::Large | BenchScale::XLarge => 10_000_000,
    }
}

fn insert_values_rows_for_scale(scale: BenchScale) -> usize {
    match scale {
        BenchScale::Small => 10_000,
        _ => INSERT_VALUES_CAP,
    }
}

fn materialize_events(count: u64, entity_count: u64) -> Vec<Event> {
    let cfg = StreamingConfig {
        total_events: count,
        entity_count,
        event_type_count: 20,
        entity_skew: STREAMING_DEFAULT_SKEW,
        seed: STREAMING_DEFAULT_SEED,
        chunk_size: STREAMING_DEFAULT_CHUNK.min(count as usize).max(1),
    };
    let generator = StreamingEventGenerator::with_config(cfg);
    let mut events: Vec<Event> = Vec::with_capacity(count as usize);
    generator.for_each_chunk(|chunk| events.extend_from_slice(chunk));
    events
}

// ─────────────────────────────────────────────────────────────────────────────
// Fixture builders
// ─────────────────────────────────────────────────────────────────────────────

fn write_jsonl_fixture(path: &Path, events: &[Event]) -> u64 {
    let file = File::create(path).expect("create jsonl fixture");
    let mut writer = BufWriter::new(file);
    for event in events {
        let entity = match &event.entity {
            EntityId::String(s) => format!("\"{s}\""),
            EntityId::Int(i) => i.to_string(),
        };
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
                PropertyValue::List(_) | PropertyValue::Map(_) => line.push_str("null"),
            }
        }
        line.push_str("}\n");
        writer.write_all(line.as_bytes()).expect("write jsonl line");
    }
    writer.flush().expect("flush jsonl");
    std::fs::metadata(path).expect("stat jsonl").len()
}

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
        flag.push(ev.get("flag").and_then(|v| match v {
            PropertyValue::Bool(b) => Some(i64::from(*b)),
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

fn write_parquet_fixture(path: &Path, events: &[Event]) -> u64 {
    let batch = events_to_batch(events);
    let file = File::create(path).expect("create parquet fixture");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("parquet writer");
    writer.write(&batch).expect("parquet write");
    writer.close().expect("parquet close");
    std::fs::metadata(path).expect("stat parquet").len()
}

/// Build a single multi-row `INSERT INTO purchases VALUES (...)` SQL
/// string. The parser does not currently accept an explicit column
/// list after the table name, so the VALUES tuples must be positioned
/// exactly in the order declared by `purchases_schema()`:
/// `(user_id, ts, event_type, amount, price, category, quantity,
/// discount, region, flag)`.
fn build_insert_values_sql(events: &[Event]) -> String {
    let mut sql =
        String::with_capacity(128 * events.len());
    sql.push_str("INSERT INTO purchases VALUES ");
    for (i, ev) in events.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push('(');
        match &ev.entity {
            EntityId::String(s) => {
                sql.push('\'');
                sql.push_str(s);
                sql.push('\'');
            }
            EntityId::Int(n) => sql.push_str(&n.to_string()),
        }
        sql.push(',');
        sql.push_str(&ev.timestamp.as_nanos().to_string());
        sql.push_str(",'");
        sql.push_str(&ev.event_type);
        sql.push('\'');

        for key in [
            "amount", "price", "category", "quantity", "discount", "region", "flag",
        ] {
            sql.push(',');
            match ev.get(key) {
                Some(PropertyValue::Int(i)) => sql.push_str(&i.to_string()),
                Some(PropertyValue::Float(f)) => sql.push_str(&f.to_string()),
                Some(PropertyValue::String(s)) => {
                    sql.push('\'');
                    sql.push_str(s);
                    sql.push('\'');
                }
                Some(PropertyValue::Bool(b)) => sql.push_str(if *b { "true" } else { "false" }),
                _ => sql.push_str("NULL"),
            }
        }
        sql.push(')');
    }
    sql
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end ingest drivers
// ─────────────────────────────────────────────────────────────────────────────

fn ingest_jsonl(jsonl_path: &Path, scratch: &Path) -> usize {
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch, "purchases", schema.clone());
    let batch_id = db.allocate_batch_id("purchases").unwrap();
    let mut partitioner =
        Partitioner::new(SHARD_COUNT, 30, batch_id, PARTITION_BUDGET_BYTES).unwrap();
    let mut reader = JsonlEventReader::open(jsonl_path, &schema, &[]).expect("jsonl open");
    while let Some(event) = reader.next_event().expect("jsonl next") {
        partitioner.push_event(event).unwrap();
    }
    let mut writer = SegmentWriter::new(&mut db);
    writer
        .write_partitioner("purchases", partitioner)
        .unwrap()
        .len()
}

fn ingest_parquet(parquet_path: &Path, scratch: &Path) -> usize {
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch, "purchases", schema.clone());
    let batch_id = db.allocate_batch_id("purchases").unwrap();
    let mut partitioner =
        Partitioner::new(SHARD_COUNT, 30, batch_id, PARTITION_BUDGET_BYTES).unwrap();
    let mut reader = ParquetEventReader::open(parquet_path, &schema, &[]).expect("parquet open");
    while let Some(event) = reader.next_event().expect("parquet next") {
        partitioner.push_event(event).unwrap();
    }
    let mut writer = SegmentWriter::new(&mut db);
    writer
        .write_partitioner("purchases", partitioner)
        .unwrap()
        .len()
}

fn ingest_insert_values(sql: &str, scratch: &Path) -> usize {
    let schema = purchases_schema();
    let engine = Engine::new();
    let mut db = open_db_with_table(scratch, "purchases", schema);
    let result = engine.query(sql, &mut db).expect("insert values query");
    result.rows_affected.unwrap_or(0) as usize
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench bodies
// ─────────────────────────────────────────────────────────────────────────────

fn bench_jsonl(
    c: &mut Criterion,
    scale: BenchScale,
    events: &[Event],
    jsonl_path: &Path,
    file_bytes: u64,
    event_bytes: u64,
    collector: &mut BenchResultCollector,
) {
    let mut group = c.benchmark_group(format!("perf/ingest_throughput/{}/jsonl", scale.label()));
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("end_to_end", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let out = ScratchDir::new("perf-ingest-jsonl");
                let segs = ingest_jsonl(jsonl_path, out.path());
                black_box(segs);
            }
            start.elapsed()
        });
    });

    // Probe pass with a fresh DB for the report's rows/s + MB/s estimate.
    let probe_scratch = ScratchDir::new("perf-ingest-jsonl-probe");
    let probe_start = Instant::now();
    let segs = ingest_jsonl(jsonl_path, probe_scratch.path());
    let probe_elapsed = probe_start.elapsed();
    let secs = probe_elapsed.as_secs_f64().max(1e-9);
    let mb_per_sec = (file_bytes as f64) / secs / (1024.0 * 1024.0);
    let rows_per_sec = events.len() as f64 / secs;
    let event_mb_per_sec = (event_bytes as f64) / secs / (1024.0 * 1024.0);
    let base = format!("perf/ingest_throughput/{}/jsonl", scale.label());
    collector.record(&format!("{base}/file_mb_per_sec"), mb_per_sec, "MB/s", None);
    collector.record(
        &format!("{base}/event_mb_per_sec"),
        event_mb_per_sec,
        "MB/s",
        None,
    );
    collector.record(
        &format!("{base}/rows_per_sec"),
        rows_per_sec,
        "rows/s",
        None,
    );
    collector.record(&format!("{base}/segments"), segs as f64, "count", None);

    group.finish();
}

fn bench_parquet(
    c: &mut Criterion,
    scale: BenchScale,
    events: &[Event],
    parquet_path: &Path,
    file_bytes: u64,
    event_bytes: u64,
    collector: &mut BenchResultCollector,
) {
    let mut group = c.benchmark_group(format!("perf/ingest_throughput/{}/parquet", scale.label()));
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("end_to_end", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let out = ScratchDir::new("perf-ingest-parquet");
                let segs = ingest_parquet(parquet_path, out.path());
                black_box(segs);
            }
            start.elapsed()
        });
    });

    let probe_scratch = ScratchDir::new("perf-ingest-parquet-probe");
    let probe_start = Instant::now();
    let segs = ingest_parquet(parquet_path, probe_scratch.path());
    let probe_elapsed = probe_start.elapsed();
    let secs = probe_elapsed.as_secs_f64().max(1e-9);
    let mb_per_sec = (file_bytes as f64) / secs / (1024.0 * 1024.0);
    let rows_per_sec = events.len() as f64 / secs;
    let event_mb_per_sec = (event_bytes as f64) / secs / (1024.0 * 1024.0);
    let base = format!("perf/ingest_throughput/{}/parquet", scale.label());
    collector.record(&format!("{base}/file_mb_per_sec"), mb_per_sec, "MB/s", None);
    collector.record(
        &format!("{base}/event_mb_per_sec"),
        event_mb_per_sec,
        "MB/s",
        None,
    );
    collector.record(
        &format!("{base}/rows_per_sec"),
        rows_per_sec,
        "rows/s",
        None,
    );
    collector.record(&format!("{base}/segments"), segs as f64, "count", None);

    group.finish();
}

fn bench_insert_values(
    c: &mut Criterion,
    scale: BenchScale,
    sql: &str,
    row_count: usize,
    event_bytes: u64,
    collector: &mut BenchResultCollector,
) {
    let sql_bytes = sql.len() as u64;
    let mut group = c.benchmark_group(format!(
        "perf/ingest_throughput/{}/insert_values",
        scale.label()
    ));
    group.throughput(Throughput::Bytes(sql_bytes));

    let label = format!("{}rows", row_count);
    group.bench_function(&label, |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let out = ScratchDir::new("perf-ingest-insert");
                let n = ingest_insert_values(sql, out.path());
                black_box(n);
            }
            start.elapsed()
        });
    });

    let probe_scratch = ScratchDir::new("perf-ingest-insert-probe");
    let probe_start = Instant::now();
    let _ = ingest_insert_values(sql, probe_scratch.path());
    let probe_elapsed = probe_start.elapsed();
    let secs = probe_elapsed.as_secs_f64().max(1e-9);
    let rows_per_sec = row_count as f64 / secs;
    let sql_mb_per_sec = sql_bytes as f64 / secs / (1024.0 * 1024.0);
    let event_mb_per_sec = event_bytes as f64 / secs / (1024.0 * 1024.0);
    let base = format!("perf/ingest_throughput/{}/insert_values", scale.label());
    collector.record(
        &format!("{base}/rows_per_sec"),
        rows_per_sec,
        "rows/s",
        None,
    );
    collector.record(
        &format!("{base}/sql_mb_per_sec"),
        sql_mb_per_sec,
        "MB/s",
        None,
    );
    collector.record(
        &format!("{base}/event_mb_per_sec"),
        event_mb_per_sec,
        "MB/s",
        None,
    );

    group.finish();
}

fn main() {
    let scale = BenchScale::from_env();
    let mode = BenchMode::from_env();
    let mut collector = BenchResultCollector::new(mode);

    let main_count = ingest_rows_for_scale(scale);
    let insert_count = insert_values_rows_for_scale(scale).min(main_count);
    let entity_count = scale.entity_count().min(main_count as u64).max(1);

    eprintln!(
        "  [perf_ingest] scale={} jsonl/parquet rows={} insert_values rows={} entities={}",
        scale.label(),
        main_count,
        insert_count,
        entity_count,
    );

    eprintln!("  [perf_ingest] generating {} events in memory...", main_count);
    let events = materialize_events(main_count as u64, entity_count);
    let event_bytes = compute_event_bytes(&events);

    // INSERT VALUES uses a prefix of the same event vector so the
    // three paths ingest comparable rows.
    let insert_events = &events[..insert_count];
    let insert_event_bytes = compute_event_bytes(insert_events);
    let insert_sql = build_insert_values_sql(insert_events);
    eprintln!(
        "  [perf_ingest] insert_values SQL = {} bytes ({} rows)",
        insert_sql.len(),
        insert_count,
    );

    // Persist the on-disk fixtures once.
    let fixture_scratch = ScratchDir::new("perf-ingest-fixtures");
    std::fs::create_dir_all(fixture_scratch.path()).expect("mkdir perf-ingest fixtures");
    let jsonl_path: PathBuf = fixture_scratch.path().join("events.jsonl");
    let parquet_path: PathBuf = fixture_scratch.path().join("events.parquet");

    eprintln!("  [perf_ingest] writing JSONL fixture...");
    let jsonl_bytes = write_jsonl_fixture(&jsonl_path, &events);
    eprintln!("  [perf_ingest] writing Parquet fixture...");
    let parquet_bytes = write_parquet_fixture(&parquet_path, &events);
    eprintln!(
        "  [perf_ingest] jsonl={} MiB parquet={} MiB event_bytes={} MiB",
        jsonl_bytes / (1024 * 1024),
        parquet_bytes / (1024 * 1024),
        event_bytes / (1024 * 1024),
    );

    let mut criterion = criterion_for_scale(scale).configure_from_args();

    bench_jsonl(
        &mut criterion,
        scale,
        &events,
        &jsonl_path,
        jsonl_bytes,
        event_bytes,
        &mut collector,
    );
    bench_parquet(
        &mut criterion,
        scale,
        &events,
        &parquet_path,
        parquet_bytes,
        event_bytes,
        &mut collector,
    );
    bench_insert_values(
        &mut criterion,
        scale,
        &insert_sql,
        insert_count,
        insert_event_bytes,
        &mut collector,
    );

    criterion.final_summary();
    collector.finish();
}
