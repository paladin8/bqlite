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
//!   column is Dictionary-encoded. [`DictionaryEqKernel`] fires here —
//!   binary-searches the literal in the sorted dict, then scans the
//!   bit-packed code stream. Expected: ≈ parity with or faster than
//!   the materialized path (the win scales with row count and dict
//!   selectivity).
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
use bqlite_core::storage::{ColumnProjection, SegmentScan};
use bqlite_core::{BqlType, PropertyValue};
use bqlite_operators::encoded_filter::{apply_encoded_eq, EncodedEqShape};
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
    // Mirrors the segment reader's `ColumnProjection::all()`
    // expansion (declared columns + implicit `__seq_id` /
    // `__batch_id`) per `docs/design/storage/system-columns.md` §3.
    schema
        .logical_columns()
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

/// Encoded kernel path: `next_encoded_row_group` → `apply_encoded_eq`
/// (dispatches to ConstantEq / RleIntEq / DictionaryEq or falls back
/// to arrow-compute on Materialized columns) → `materialize_selected`.
///
/// `apply_encoded_eq` is the same entry point the real ScanOperator
/// drives, so the bench measures what production runs.
fn run_encoded(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
    col_idx: usize,
    literal: &str,
    types: &[BqlType],
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> u64 {
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let projection = ColumnProjection::all();
    let mut scan = reader.scan(&projection, None).unwrap();
    let shape = EncodedEqShape {
        col_index: col_idx,
        literals: vec![PropertyValue::String(literal.into())],
    };
    let col_type = types[col_idx].clone();
    let mut total = 0u64;
    while let Some(encoded) = scan.next_encoded_row_group().unwrap() {
        let rows = encoded.row_count;
        let input_sel = RowSelection::from_runs(vec![bqlite_core::encoded::RowRun {
            start: 0,
            len: rows,
        }]);
        let sel = apply_encoded_eq(&shape, &encoded, &input_sel, &col_type).unwrap();
        let fb = materialize_selected(&encoded, Some(&sel), types, arrow_schema.clone()).unwrap();
        total += fb.batch.num_rows() as u64;
        black_box(&fb);
    }
    total
}

/// Materialized baseline for a multi-literal `IN` predicate. Per-literal
/// `cmp::eq` masks Kleene-OR'd into a single mask, then
/// `filter_record_batch`. Mirrors the structure of `apply_fallback_eq`
/// for the OR-fold path so bench numbers are comparable between the
/// encoded and materialized sides.
fn run_materialized_in(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
    col_idx: usize,
    literals: &[&str],
) -> u64 {
    use arrow::array::BooleanArray;
    use arrow::compute::kernels::boolean;
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let projection = ColumnProjection::all();
    let mut scan = reader.scan(&projection, None).unwrap();
    let mut total = 0u64;
    while let Some(batch) = scan.next_row_group().unwrap() {
        let col = batch.column(col_idx);
        let mut combined: Option<BooleanArray> = None;
        for &literal in literals {
            let lit = Scalar::new(StringViewArray::from(vec![literal]));
            let mask = cmp::eq(&col.as_ref(), &lit).unwrap();
            let mask_bool = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .clone();
            combined = Some(match combined {
                None => mask_bool,
                Some(acc) => boolean::or_kleene(&acc, &mask_bool).unwrap(),
            });
        }
        let mask = combined.expect("literals non-empty");
        let filtered = filter_record_batch(&batch, &mask).unwrap();
        total += filtered.num_rows() as u64;
        black_box(&filtered);
    }
    total
}

/// Encoded kernel path for a multi-literal IN predicate. Uses the new
/// `EncodedEqShape::literals` vector to drive the dictionary IN kernel.
fn run_encoded_in(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
    col_idx: usize,
    literals: &[&str],
    types: &[BqlType],
    arrow_schema: Arc<arrow::datatypes::Schema>,
) -> u64 {
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone()).unwrap();
    let projection = ColumnProjection::all();
    let mut scan = reader.scan(&projection, None).unwrap();
    let shape = EncodedEqShape {
        col_index: col_idx,
        literals: literals
            .iter()
            .map(|s| PropertyValue::String((*s).into()))
            .collect(),
    };
    let col_type = types[col_idx].clone();
    let mut total = 0u64;
    while let Some(encoded) = scan.next_encoded_row_group().unwrap() {
        let rows = encoded.row_count;
        let input_sel = RowSelection::from_runs(vec![bqlite_core::encoded::RowRun {
            start: 0,
            len: rows,
        }]);
        let sel = apply_encoded_eq(&shape, &encoded, &input_sel, &col_type).unwrap();
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

// ── Bench 2: Realistic row groups (Dictionary kernel) ───────────────────────

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

    group.bench_function("encoded_kernel", |b| {
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

// ── Bench 3: Dictionary IN-list (TASK-516 CP1) ──────────────────────────────
//
// Dictionary-encoded `event_type` filtered by a 2-literal `IN` predicate.
// The encoded path goes through `recognize_encoded_eq`'s IN arm, the
// `apply_encoded_eq` dispatcher, and `DictionaryEqKernel` with both
// literals — no row-group-wide materialization before filter. The
// materialized baseline OR-folds two `cmp::eq` masks before
// `filter_record_batch`, mirroring `apply_fallback_eq`'s shape so the
// comparison isolates kernel cost vs. arrow-compute cost on the same
// workload.

fn bench_dictionary_in_list_filter(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = BenchSizing::for_mode(mode);
    let n = sizing.scan_events;
    let entities = sizing.scan_entities;
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
    let literals: &[&str] = &["event_0", "event_1"];

    let m_rows = run_materialized_in(&bytes, &schema, event_col, literals);
    let e_rows = run_encoded_in(
        &bytes,
        &schema,
        event_col,
        literals,
        &types,
        arrow_schema.clone(),
    );
    assert_eq!(m_rows, e_rows, "encoded and materialized IN paths disagree");

    let mut group = c.benchmark_group("scan/filter_compare/dictionary_in");
    group.throughput(Throughput::Bytes(file_bytes));

    group.bench_function("materialized_arrow", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(run_materialized_in(&bytes, &schema, event_col, literals));
            }
            start.elapsed()
        })
    });

    group.bench_function("encoded_kernel", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                black_box(run_encoded_in(
                    &bytes,
                    &schema,
                    event_col,
                    literals,
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
        bench_dictionary_in_list_filter,
}
criterion_main!(scan_encoded_benches);
