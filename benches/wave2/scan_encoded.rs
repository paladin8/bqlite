//! Encoded-vs-materialized filter path comparison.
//!
//! The zero-copy scan/filter feature (`docs/design/storage/zero-copy-scan-filter.md`)
//! ships an encoded read path (`next_encoded_row_group` +
//! `materialize_encoded_batch`) as an alternative to the dense
//! `next_row_group` path. This bench measures end-to-end filter cost
//! for both paths on the same workload so we can judge whether the
//! encoded path is worth wiring into `ScanOperator`.
//!
//! Two scenarios:
//!
//! - `constant_rg`: small row groups (one entity per RG) so `user_id`
//!   lands as `Constant`-encoded. The encoded path hits
//!   [`ConstantEqKernel`] — its best case.
//! - `realistic_rg`: default row groups so every low-cardinality string
//!   column is Dictionary-encoded. No kernel fires today; the encoded
//!   path falls back to `EncodedColumn::Materialized` for the filter
//!   column. Expected: ≈ parity with the materialized path.
//!
//! The comparison isolates the filter-application cost: both paths do
//! the same segment read, they differ only in how they apply the
//! predicate + narrow the result.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Scalar, StringViewArray};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp;
use bqlite_benches::common::*;
use bqlite_core::encoded::RowSelection;
use bqlite_core::scalar::ScalarValue;
use bqlite_core::storage::{ColumnProjection, SegmentScan};
use bqlite_core::BqlType;
use bqlite_operators::encoded_filter::{ConstantEqKernel, EncodedPredicateKernel};
use bqlite_operators::materialize_selected;
use bqlite_storage::segment::reader::SegmentFileReader;
use bqlite_storage::writer::{SegmentWriter, WriterConfig};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

/// Write a segment using a custom `row_group_size`. Small sizes are the
/// trick that makes `user_id` land as Constant-encoded per row group.
fn write_segment_with_rg(
    n_events: usize,
    entity_count: usize,
    row_group_size: usize,
) -> (Vec<u8>, bqlite_core::schema::TableSchema) {
    let schema = purchases_schema();
    let scratch = ScratchDir::new("scan_encoded");
    let mut db = open_db_with_table(scratch.path(), "purchases", schema.clone());
    let events = generate_events(n_events, entity_count);
    let batch_id = db.allocate_batch_id("purchases").unwrap();

    let mut writer = SegmentWriter::with_config(&mut db, WriterConfig { row_group_size });
    let _meta = writer
        .write_bucket("purchases", 0, 0, batch_id, &events)
        .unwrap();

    let seg_paths = find_segment_files(scratch.path());
    assert!(!seg_paths.is_empty(), "no segment files written");
    let bytes = std::fs::read(&seg_paths[0]).unwrap();
    (bytes, schema)
}

fn types_for(schema: &bqlite_core::schema::TableSchema) -> Vec<BqlType> {
    schema
        .columns()
        .iter()
        .map(|c| c.bql_type.clone())
        .collect()
}

/// Decode the first row group of a segment just to capture its
/// [`arrow::datatypes::Schema`]. `SegmentFileScan` does not expose the
/// cached arrow schema publicly, so we re-derive it from a single
/// materialized batch.
fn first_batch_schema(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
) -> Arc<arrow::datatypes::Schema> {
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
    let batch = scan
        .next_row_group()
        .unwrap()
        .expect("segment has at least one row group");
    batch.schema()
}

/// Materialized baseline: `next_row_group` → `cmp::eq` → `filter_record_batch`.
fn run_materialized(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
    col_idx: usize,
    literal: &str,
) -> u64 {
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let projection = ColumnProjection::all();
    let mut scan = reader.scan(&projection, None).unwrap();
    let mut total = 0u64;
    while let Some(batch) = scan.next_row_group().unwrap() {
        let col = batch.column(col_idx);
        let lit = Scalar::new(StringViewArray::from(vec![literal]));
        let mask = cmp::eq(&col.as_ref(), &lit).unwrap();
        let filtered = filter_record_batch(&batch, &mask).unwrap();
        total += filtered.num_rows() as u64;
        black_box(&filtered);
    }
    total
}

/// Encoded kernel path: `next_encoded_row_group` → [`ConstantEqKernel`] →
/// `materialize_selected`.
///
/// Uses the kernel when the filter column is Constant-encoded; otherwise
/// falls back to materializing the column and running `cmp::eq` on it
/// (the same arrow-compute path the materialized baseline uses) — this
/// models the current encoded path when no kernel exists.
fn run_encoded(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
    col_idx: usize,
    literal: &str,
    types: &[BqlType],
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> u64 {
    use bqlite_core::encoded::EncodedColumn;
    use bqlite_storage::materialize_encoded_column;

    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let projection = ColumnProjection::all();
    let mut scan = reader.scan(&projection, None).unwrap();
    let kernel = ConstantEqKernel::new(ScalarValue::String(literal.to_string()));
    let mut total = 0u64;
    while let Some(encoded) = scan.next_encoded_row_group().unwrap() {
        let rows = encoded.row_count;
        let input_sel = RowSelection::from_runs(vec![bqlite_core::encoded::RowRun {
            start: 0,
            len: rows,
        }]);
        let col = &encoded.columns[col_idx];
        let sel = match col {
            EncodedColumn::Encoded { .. } => kernel.apply(&col.view(), &input_sel),
            EncodedColumn::Materialized { .. } => {
                // Fallback path: no kernel yet — materialize the filter
                // column and produce a boolean mask via arrow-compute.
                let dense = materialize_encoded_column(col, &BqlType::String).unwrap();
                let lit = Scalar::new(StringViewArray::from(vec![literal]));
                let mask = cmp::eq(&dense.as_ref(), &lit).unwrap();
                let mask_bool = mask
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .unwrap();
                bqlite_operators::encoded_filter::apply_materialized_mask(mask_bool, &input_sel)
            }
        };
        let fb = materialize_selected(&encoded, Some(&sel), types, arrow_schema.clone()).unwrap();
        total += fb.batch.num_rows() as u64;
        black_box(&fb);
    }
    total
}

// ── Bench 1: Constant-encoded column (kernel sweet spot) ────────────────────

fn bench_constant_column_filter(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let n = sizing.scan_events;
    let entities = sizing.scan_entities;
    let events_per_entity = n / entities;
    // Row groups aligned to one entity each → user_id is Constant per RG.
    let rg = events_per_entity.max(1);
    let (bytes, schema) = write_segment_with_rg(n, entities, rg);
    let types = types_for(&schema);
    let file_bytes = bytes.len() as u64;

    // Build a schema the encoded path can hand to materialize_selected.
    let arrow_schema = first_batch_schema(&bytes, &schema);

    let user_id_col = schema
        .columns()
        .iter()
        .position(|c| c.name == "user_id")
        .unwrap();
    let literal = "user_0";

    // Sanity: both paths agree on the row count so we're comparing
    // apples to apples.
    let m_rows = run_materialized(&bytes, &schema, user_id_col, literal);
    let e_rows = run_encoded(
        &bytes,
        &schema,
        user_id_col,
        literal,
        &types,
        arrow_schema.clone(),
    );
    assert_eq!(m_rows, e_rows, "encoded and materialized paths disagree");

    let mut group = c.benchmark_group("scan/filter_compare/constant_rg");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("materialized_arrow", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(run_materialized(&bytes, &schema, user_id_col, literal));
            }
            start.elapsed()
        })
    });

    group.bench_function("encoded_kernel", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(run_encoded(
                    &bytes,
                    &schema,
                    user_id_col,
                    literal,
                    &types,
                    arrow_schema.clone(),
                ));
            }
            start.elapsed()
        })
    });

    group.finish();
}

// ── Bench 2: Realistic row groups (no kernel → fallback path) ────────────────

fn bench_realistic_column_filter(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let n = sizing.scan_events;
    let entities = sizing.scan_entities;
    // Default row group size — multi-entity per RG → user_id is
    // Dictionary-encoded (no ConstantEqKernel). This is the realistic
    // "no kernel yet" case.
    let rg = bqlite_storage::writer::DEFAULT_ROW_GROUP_SIZE;
    let (bytes, schema) = write_segment_with_rg(n, entities, rg);
    let types = types_for(&schema);
    let file_bytes = bytes.len() as u64;

    let arrow_schema = first_batch_schema(&bytes, &schema);

    let event_col = schema
        .columns()
        .iter()
        .position(|c| c.name == "event_type")
        .unwrap();
    let literal = "event_0";

    let m_rows = run_materialized(&bytes, &schema, event_col, literal);
    let e_rows = run_encoded(
        &bytes,
        &schema,
        event_col,
        literal,
        &types,
        arrow_schema.clone(),
    );
    assert_eq!(m_rows, e_rows, "encoded and materialized paths disagree");

    let mut group = c.benchmark_group("scan/filter_compare/realistic_rg");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("materialized_arrow", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(run_materialized(&bytes, &schema, event_col, literal));
            }
            start.elapsed()
        })
    });

    group.bench_function("encoded_fallback", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(run_encoded(
                    &bytes,
                    &schema,
                    event_col,
                    literal,
                    &types,
                    arrow_schema.clone(),
                ));
            }
            start.elapsed()
        })
    });

    group.finish();
}

criterion_group! {
    name = scan_encoded_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_constant_column_filter,
        bench_realistic_column_filter,
}
criterion_main!(scan_encoded_benches);
