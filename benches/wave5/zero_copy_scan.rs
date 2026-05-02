//! Zero-copy scan/filter copy-budget bench (TASK-526, Wave 5).
//!
//! Measures the per-scan copy-budget counters introduced by the
//! encoded-preserving read path (see
//! `docs/design/storage/zero-copy-scan-filter.md` §3 and the
//! `MetricsSnapshot` doc comment in `bqlite-core::metrics`):
//!
//! - `bytes_materialized_before_filter` must remain `0` on the
//!   encoded scan path. Materialisation only happens *after* the
//!   filter selects rows.
//! - `bytes_decompressed` reports the LZ4 decompression copy when
//!   the writer chose `CompressionType::Lz4`. A non-zero value is
//!   the expected single-copy cost of decompressing the column
//!   chunk in place; a zero value when LZ4 was chosen would mean a
//!   regression that bypassed decompression.
//!
//! The bench has two scenarios:
//!
//! - `low_card_dict/copy_budget` — the `event_type` column is
//!   dictionary-encoded (low cardinality + sorted-friendly cycling).
//!   Drives a dictionary-aware equality filter through the encoded
//!   path and asserts every iteration contributes exactly zero
//!   pre-filter materialisation bytes. `[spec]` target on
//!   `bytes_materialized_before_filter / bytes_scanned ≤ 0.0`.
//! - `lz4_payload/decompress_ratio` — a hand-built segment whose
//!   `amount` column is `Plain + Lz4` (the fixture is constructed
//!   via `encode_segment` rather than the writer's encoding selector
//!   because the selector picks FSST for natural high-cardinality
//!   string fixtures, and FSST falls back to
//!   `EncodedColumn::Materialized` per
//!   `bqlite-storage::segment::encoded` §6.5). Drives a scan over
//!   the LZ4-wrapped column and reports
//!   `bytes_decompressed / bytes_scanned`. `[floor]` target on
//!   ratio `≥ 1.0` — at least one full decompression copy is
//!   expected; zero would mean LZ4 stopped firing or the encoded
//!   path lost its `record_bytes_decompressed` call site.
//!
//! Both scenarios exercise the same `next_encoded_row_group` →
//! `apply_encoded_eq` → `materialize_selected` chain that
//! `ScanOperator` drives in production (see Wave 2's `scan_encoded`
//! bench for the same plumbing on the timing axis).

use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use bqlite_benches::common::*;
use bqlite_core::encoded::{RowRun, RowSelection};
use bqlite_core::event::Event;
use bqlite_core::metrics::{AtomicMetrics, Metrics, MetricsSnapshot};
use bqlite_core::property::PropertyValue;
use bqlite_core::schema::{ColumnDef, TableSchema};
use bqlite_core::storage::{ColumnProjection, SegmentScan};
use bqlite_core::{BqlType, Result};
use bqlite_operators::encoded_filter::{apply_encoded_eq, EncodedEqShape};
use bqlite_operators::materialize_selected;
use bqlite_storage::encoding::{CompressionType, Encoding, Plain};
use bqlite_storage::segment::reader::SegmentFileReader;
use bqlite_storage::segment::writer::{
    encode_segment, PreparedColumnChunk, PreparedRowGroup, SegmentWriteRequest,
};
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Sizing for the zero-copy bench fixtures. Wave 5 sticks with
/// `BenchSizing::scan_*` for parity with Wave 2's `scan_encoded`
/// bench; per-scenario tweaks scope to local constants below.
fn bench_sizing() -> BenchSizing {
    BenchSizing::for_mode(BenchMode::from_env())
}

/// Schema for the LZ4 fixture: entity id, timestamp, event type, and
/// an int `amount` property column whose Plain-encoded payload is
/// hand-wrapped with `CompressionType::Lz4` so the encoded path
/// observes a non-zero `bytes_decompressed` per
/// `zero-copy-scan-filter.md` §3. Bypassing the writer's encoding
/// selector is the only deterministic way to land an
/// `EncodedColumn::Encoded` chunk that is also LZ4-compressed: the
/// natural string-column candidates that LZ4 would compress well
/// (long URL-like values) get FSST-encoded by the selector, and
/// `EncodingType::Fsst` falls back to `EncodedColumn::Materialized`
/// per `bqlite-storage::segment::encoded` §6.5. Hand-building the
/// `SegmentWriteRequest` with `Plain + Lz4` keeps the on-disk shape
/// identical to a real segment while pinning the encoding choice.
fn lz4_schema() -> TableSchema {
    TableSchema::new(
        "lz4_payload",
        vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
        ],
        "user_id",
        "ts",
        "event_type",
    )
    .expect("lz4 schema")
}

/// Encode a string array as Plain (uncompressed). Helper for the
/// LZ4 fixture's non-target columns.
fn plain_string_chunk(
    values: &[&str],
) -> bqlite_core::Result<bqlite_storage::encoding::EncodedChunk> {
    use arrow::array::StringArray;
    let arr = StringArray::from(values.to_vec());
    Plain.encode(&arr)
}

/// Encode an i64 array as Plain.
fn plain_int_chunk(values: &[i64]) -> bqlite_core::Result<bqlite_storage::encoding::EncodedChunk> {
    let arr = Int64Array::from(values.to_vec());
    Plain.encode(&arr)
}

/// Encode an i64 timestamp array as Plain.
fn plain_ts_chunk(values: &[i64]) -> bqlite_core::Result<bqlite_storage::encoding::EncodedChunk> {
    use arrow::array::TimestampNanosecondArray;
    let arr = TimestampNanosecondArray::from(values.to_vec()).with_timezone("UTC");
    Plain.encode(&arr)
}

/// Build a single-row-group segment whose `amount` column is
/// `Plain + Lz4`. The other key columns are `Plain + None`.
///
/// `n_rows` is held below 256 entity values so the entity column
/// stays tiny and the bench scope is dominated by the `amount`
/// chunk's LZ4 footprint.
fn build_lz4_segment(n_rows: usize) -> Vec<u8> {
    let schema = lz4_schema();
    // Entity ids cycle through a tiny set so the entity column is
    // small and Plain-encoded; the bench is about `amount`.
    let entity_pool = ["a", "b", "c", "d", "e", "f", "g", "h"];
    let entity_values: Vec<&str> = (0..n_rows)
        .map(|i| entity_pool[i % entity_pool.len()])
        .collect();
    let ts_values: Vec<i64> = (0..n_rows as i64)
        .map(|i| 1_700_000_000_000_000_000 + i)
        .collect();
    let event_values: Vec<&str> = vec!["view"; n_rows];
    // Highly-compressible int values: cycle through a small set so
    // the high bytes of the i64 representation are repeated. LZ4
    // comfortably clears the 10 % `LZ4_RATIO_THRESHOLD`.
    let amount_values: Vec<i64> = (0..n_rows).map(|i| (i % 64) as i64).collect();

    let entity_chunk = plain_string_chunk(&entity_values).unwrap();
    let ts_chunk = plain_ts_chunk(&ts_values).unwrap();
    let event_chunk = plain_string_chunk(&event_values).unwrap();
    let amount_chunk = plain_int_chunk(&amount_values).unwrap();

    // Null bitmap for the nullable amount column: every row is
    // valid. Per `segment-format-v1.md` §8 the null bitmap is bit-
    // packed, so `n_rows` bits round up to bytes.
    let bitmap_bytes = n_rows.div_ceil(8);
    let amount_bitmap = vec![0xFFu8; bitmap_bytes];

    let request = SegmentWriteRequest {
        schema: schema.clone(),
        schema_version: 0,
        row_groups: vec![PreparedRowGroup {
            row_count: n_rows as u64,
            columns: vec![
                PreparedColumnChunk {
                    column_ordinal: 0,
                    null_bitmap: None,
                    encoded: entity_chunk,
                    compression: CompressionType::None,
                    null_count: 0,
                    zone_min: Some(PropertyValue::String(entity_pool[0].into())),
                    zone_max: Some(PropertyValue::String(
                        entity_pool[entity_pool.len() - 1].into(),
                    )),
                },
                PreparedColumnChunk {
                    column_ordinal: 1,
                    null_bitmap: None,
                    encoded: ts_chunk,
                    compression: CompressionType::None,
                    null_count: 0,
                    zone_min: Some(PropertyValue::Timestamp(ts_values[0])),
                    zone_max: Some(PropertyValue::Timestamp(*ts_values.last().unwrap())),
                },
                PreparedColumnChunk {
                    column_ordinal: 2,
                    null_bitmap: None,
                    encoded: event_chunk,
                    compression: CompressionType::None,
                    null_count: 0,
                    zone_min: Some(PropertyValue::String("view".into())),
                    zone_max: Some(PropertyValue::String("view".into())),
                },
                PreparedColumnChunk {
                    column_ordinal: 3,
                    null_bitmap: Some(amount_bitmap),
                    encoded: amount_chunk,
                    compression: CompressionType::Lz4,
                    null_count: 0,
                    zone_min: Some(PropertyValue::Int(0)),
                    zone_max: Some(PropertyValue::Int(63)),
                },
            ],
        }],
        dictionaries: vec![],
        creation_timestamp_ns: 1_700_000_000_000_000_000,
        seq_id_range: (0, n_rows as u64 - 1),
        batch_id: 1,
        compaction_level: 0,
        fsst_symbol_tables: vec![],
        format_version: 1,
    };
    encode_segment(&request).expect("encode_segment")
}

/// Write a segment from `events` into a scratch dir and return the
/// raw bytes plus the schema. Mirrors the Wave 2 `scan_encoded`
/// helper but parameterised on the schema so we can reuse it for
/// both scenarios.
fn write_segment(events: &[Event], schema: TableSchema) -> Vec<u8> {
    let scratch = ScratchDir::new("zero_copy_scan");
    let mut db = open_db_with_table(scratch.path(), schema.name(), schema.clone());
    let batch_id = db.allocate_batch_id(schema.name()).unwrap();
    let mut writer = SegmentWriter::new(&mut db);
    let _meta = writer
        .write_bucket(schema.name(), 0, 0, batch_id, events)
        .unwrap();
    let seg_paths = find_segment_files(scratch.path());
    assert!(!seg_paths.is_empty(), "no segment files written");
    std::fs::read(&seg_paths[0]).unwrap()
}

/// Decode the first row group of a segment to capture the
/// `arrow::datatypes::Schema` `materialize_selected` needs for the
/// requested projection. The `SegmentFileScan` does not expose the
/// cached arrow schema publicly, so we re-derive it from a single
/// materialised batch over the same projection.
fn projection_arrow_schema(
    bytes: &[u8],
    schema: &TableSchema,
    projection: &ColumnProjection,
) -> Arc<arrow::datatypes::Schema> {
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let mut scan = reader.scan(projection, None).unwrap();
    let batch = scan
        .next_row_group()
        .unwrap()
        .expect("segment has at least one row group");
    batch.schema()
}

/// Resolve the `BqlType` list for a column-name projection. Order
/// matches `ColumnProjection::columns()` so the output lines up with
/// the encoded batch's column array.
fn types_for_projection(schema: &TableSchema, projection: &ColumnProjection) -> Vec<BqlType> {
    projection
        .columns()
        .iter()
        .map(|name| {
            schema
                .columns()
                .iter()
                .find(|c| &c.name == name)
                .unwrap_or_else(|| panic!("column {name} not in schema {}", schema.name()))
                .bql_type
                .clone()
        })
        .collect()
}

/// Drive the encoded scan path with a single-literal equality
/// predicate against the *first* column of `projection` and return
/// the per-iteration metric snapshot. Both scenarios share this
/// driver. The projection narrows the read to the column under
/// test so the copy-budget assertion isolates the encoded path
/// rather than aggregating across columns whose encodings might
/// fall back to materialisation.
fn run_encoded_scan(
    bytes: &[u8],
    schema: &TableSchema,
    projection: &ColumnProjection,
    literal: PropertyValue,
    types: &[BqlType],
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> Result<MetricsSnapshot> {
    let metrics: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone())?;
    let mut scan = reader.scan(projection, None)?;
    scan.attach_metrics(Arc::clone(&metrics));
    let shape = EncodedEqShape {
        col_index: 0,
        literals: vec![literal],
    };
    let col_type = types[0].clone();
    while let Some(encoded) = scan.next_encoded_row_group()? {
        let rows = encoded.row_count;
        let input_sel = RowSelection::from_runs(vec![RowRun {
            start: 0,
            len: rows,
        }]);
        let sel = apply_encoded_eq(&shape, &encoded, &input_sel, &col_type)?;
        let fb = materialize_selected(&encoded, Some(&sel), types, arrow_schema.clone())?;
        let _: &RecordBatch = black_box(&fb.batch);
    }
    Ok(metrics.snapshot())
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 1: low-cardinality dictionary copy budget
// ─────────────────────────────────────────────────────────────────────────────

fn bench_low_card_dict_copy_budget(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = bench_sizing();
    let n = sizing.scan_events;
    let entities = sizing.scan_entities;

    let schema = purchases_schema();
    let events = generate_events(n, entities);
    let bytes = write_segment(&events, schema.clone());

    // Project only the dict-encoded column under test. Other
    // columns (e.g. nullable `amount`) may select encodings whose
    // encoded-path branch falls back to materialisation per
    // `bqlite-storage::segment::encoded`; those would mask the
    // copy-budget assertion if included.
    let projection = ColumnProjection::with_columns(["event_type"]);
    let types = types_for_projection(&schema, &projection);
    let arrow_schema = projection_arrow_schema(&bytes, &schema, &projection);

    // Hits ~5% of rows (the cycling 1-of-20 bucket). The encoded
    // dictionary-equality kernel only needs to materialise the
    // selected rows — the full Arrow array is never built.
    let literal = PropertyValue::String("event_3".into());

    // Sanity probe: run the chain once outside the timed loop so we
    // can pin the assertions before Criterion starts measuring.
    let probe = run_encoded_scan(
        &bytes,
        &schema,
        &projection,
        literal.clone(),
        &types,
        arrow_schema.clone(),
    )
    .expect("probe scan");
    assert!(
        probe.bytes_scanned > 0,
        "encoded scan produced bytes_scanned == 0; fixture is empty?"
    );
    assert_eq!(
        probe.bytes_materialized_before_filter, 0,
        "encoded path must not materialise into Arrow before the filter; \
         observed {} bytes",
        probe.bytes_materialized_before_filter
    );

    let mut group = c.benchmark_group("wave5/zero_copy_scan/low_card_dict");
    group.throughput(Throughput::Elements(events.len() as u64));

    group.bench_function(BenchmarkId::from_parameter("copy_budget"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut last_snapshot = MetricsSnapshot::default();
            for _ in 0..iters {
                last_snapshot = run_encoded_scan(
                    &bytes,
                    &schema,
                    &projection,
                    literal.clone(),
                    &types,
                    arrow_schema.clone(),
                )
                .expect("scan iter");
            }
            // Pin the invariant on every Criterion sample so a
            // regression cannot hide behind warm-up noise.
            assert_eq!(
                last_snapshot.bytes_materialized_before_filter, 0,
                "encoded scan path materialised {} bytes before filter",
                last_snapshot.bytes_materialized_before_filter,
            );
            start.elapsed()
        })
    });
    group.finish();

    let mut collector = BenchResultCollector::new(mode);
    let pre_filter_ratio = if probe.bytes_scanned == 0 {
        0.0
    } else {
        probe.bytes_materialized_before_filter as f64 / probe.bytes_scanned as f64
    };
    collector.record(
        "wave5/zero_copy_scan/low_card_dict/pre_filter_materialization_ratio",
        pre_filter_ratio,
        "ratio",
        Some(BenchTarget::at_most(0.0)),
    );
    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench 2: LZ4 payload decompression copy
// ─────────────────────────────────────────────────────────────────────────────

fn bench_lz4_payload_decompress_ratio(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = bench_sizing();
    // Hand-built segments are the only way to deterministically pin
    // a `Plain + Lz4` chunk on the encoded path (see
    // `build_lz4_segment` doc); cap row count at a CI-friendly
    // ceiling so the fixture build stays fast.
    let n_rows = sizing.scan_events.min(50_000);

    let schema = lz4_schema();
    let bytes = build_lz4_segment(n_rows);

    // Project only the LZ4-wrapped `amount` column so the
    // `bytes_decompressed` counter isolates the LZ4 chunk and is
    // not diluted by the other (uncompressed) columns.
    let projection = ColumnProjection::with_columns(["amount"]);
    let types = types_for_projection(&schema, &projection);
    let arrow_schema = projection_arrow_schema(&bytes, &schema, &projection);

    // Match a value present in the cycle. Selectivity does not
    // affect the per-chunk decompression metric, but a real literal
    // exercises the encoded equality kernel.
    let literal = PropertyValue::Int(7);

    let probe = run_encoded_scan(
        &bytes,
        &schema,
        &projection,
        literal.clone(),
        &types,
        arrow_schema.clone(),
    )
    .expect("probe scan");
    assert!(
        probe.bytes_scanned > 0,
        "encoded scan produced bytes_scanned == 0; fixture is empty?"
    );
    assert!(
        probe.bytes_decompressed > 0,
        "lz4 fixture must record bytes_decompressed; \
         observed bytes_decompressed == 0 — encoded path lost its \
         `record_bytes_decompressed` call site or the writer wrote \
         the chunk uncompressed",
    );
    // Pre-filter materialisation must still be zero on the encoded
    // path even when LZ4 fired; decompression and materialisation
    // are independent counters per `MetricsSnapshot` doc comments.
    assert_eq!(
        probe.bytes_materialized_before_filter, 0,
        "encoded path materialised {} bytes before filter despite LZ4 chunking",
        probe.bytes_materialized_before_filter,
    );

    let mut group = c.benchmark_group("wave5/zero_copy_scan/lz4_payload");
    group.throughput(Throughput::Elements(n_rows as u64));

    group.bench_function(BenchmarkId::from_parameter("decompress_ratio"), |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let mut last_snapshot = MetricsSnapshot::default();
            for _ in 0..iters {
                last_snapshot = run_encoded_scan(
                    &bytes,
                    &schema,
                    &projection,
                    literal.clone(),
                    &types,
                    arrow_schema.clone(),
                )
                .expect("scan iter");
            }
            assert!(
                last_snapshot.bytes_decompressed > 0,
                "encoded scan recorded zero bytes_decompressed on the LZ4 fixture",
            );
            start.elapsed()
        })
    });
    group.finish();

    let mut collector = BenchResultCollector::new(mode);
    let decompress_ratio = if probe.bytes_scanned == 0 {
        0.0
    } else {
        probe.bytes_decompressed as f64 / probe.bytes_scanned as f64
    };
    collector.record(
        "wave5/zero_copy_scan/lz4_payload/decompress_ratio",
        decompress_ratio,
        "ratio",
        Some(BenchTarget::at_least(1.0)),
    );
    let pre_filter_ratio = if probe.bytes_scanned == 0 {
        0.0
    } else {
        probe.bytes_materialized_before_filter as f64 / probe.bytes_scanned as f64
    };
    collector.record(
        "wave5/zero_copy_scan/lz4_payload/pre_filter_materialization_ratio",
        pre_filter_ratio,
        "ratio",
        Some(BenchTarget::at_most(0.0)),
    );
    collector.finish();
}

criterion_group! {
    name = zero_copy_scan_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(2));
    targets =
        bench_low_card_dict_copy_budget,
        bench_lz4_payload_decompress_ratio,
}
criterion_main!(zero_copy_scan_benches);
