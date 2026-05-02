//! Tombstone scan filter microbenchmark (TASK-534).
//!
//! Two benchmark groups:
//!
//! 1. **`tombstone_scan/query_time`** — measures
//!    [`bqlite_storage::TombstoneFilter::filter_batch_with_index`]
//!    throughput on 65 536-row batches across five tombstone
//!    configurations (entity-delete sets of 0 / 100 / 10 000 entries,
//!    a 10%-covering time-range delete, and a mixed-granularity case
//!    with entity + time-range + row tombstones simultaneously).
//!    Reports surviving rows/second and a `[floor]` regression
//!    tripwire per configuration.
//!
//! 2. **`tombstone_scan/compaction`** — measures
//!    [`Database::compact_now`] throughput with entity-tombstoned
//!    segments at 1% / 5% / 10% entity density and asserts the
//!    `[floor]` that the tombstoned-to-clean throughput ratio at 10%
//!    density does not exceed 2×.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench tombstone_scan
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_benches::common::*;
use bqlite_core::schema::SEQ_ID_COLUMN;
use bqlite_core::ScalarValue;
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::tombstone::{
    EntityDeleteIndex, TimeRangeDelete, TombstoneFile, TombstoneFilter,
};
use bqlite_storage::writer::SegmentWriter;
use bqlite_storage::{tombstone_file_path, write_tombstone_atomic, Database};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Row count for the query-time filter batches (spec: 65 536).
const BATCH_ROWS: usize = 65_536;

/// Distinct entity IDs cycled in the filter batch. 1 024 gives each
/// entity ~64 rows, producing realistic hit-rate distributions for
/// both small (100-entry) and large (10 000-entry) delete sets.
const BATCH_ENTITIES: usize = 1_024;

/// Base timestamp for synthetic event data (2025-01-01 00:00:00 UTC).
const BASE_NS: i64 = 1_735_689_600_000_000_000;

/// Per-row timestamp step (1 second in ns). Keeps BATCH_ROWS
/// timestamps non-overlapping without approaching i64 overflow.
const STEP_NS: i64 = 1_000_000_000;

/// Single shard so all segment data lands in one (window, shard) bucket.
const SHARD_COUNT: u16 = 1;

/// L0 segment count for compaction benches (matches compaction.rs baseline).
const N_SEGMENTS: usize = 5;

/// Distinct entities per segment (matches seed_l0_stack in compaction.rs).
const ENTITIES_PER_SEGMENT: usize = 64;

const PARTITION_BUDGET_BYTES: usize = 512 * 1024 * 1024;

// ─────────────────────────────────────────────────────────────────────────────
// Batch builders
// ─────────────────────────────────────────────────────────────────────────────

/// Build a `BATCH_ROWS`-row RecordBatch for query-time filter benches.
///
/// Schema: `entity_id` (Utf8View), `ts` (Timestamp Nanosecond UTC),
/// `event_type` (Utf8View). Entity IDs cycle through `BATCH_ENTITIES`
/// distinct labels; timestamps are sequential from `BASE_NS`.
fn make_filter_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8View, false),
    ]));

    let labels: Vec<String> = (0..BATCH_ENTITIES)
        .map(|i| format!("entity_{i:06}"))
        .collect();
    let entities: Vec<&str> = (0..BATCH_ROWS)
        .map(|i| labels[i % BATCH_ENTITIES].as_str())
        .collect();
    let tss: Vec<Option<i64>> = (0..BATCH_ROWS as i64)
        .map(|i| Some(BASE_NS + i * STEP_NS))
        .collect();
    let evts: Vec<&str> = vec!["click"; BATCH_ROWS];

    let id_arr: ArrayRef = Arc::new(StringViewArray::from(entities));
    let ts_arr: ArrayRef = Arc::new(TimestampNanosecondArray::from(tss).with_timezone("UTC"));
    let evt_arr: ArrayRef = Arc::new(StringViewArray::from(evts));

    RecordBatch::try_new(schema, vec![id_arr, ts_arr, evt_arr]).unwrap()
}

/// Same as `make_filter_batch` plus a `__seq_id` column (Int64,
/// sequential from 0). Required for the mixed-granularity case which
/// includes row-level tombstones: `TombstoneFilter::apply_row_deletes`
/// reads `__seq_id` from the batch.
fn make_filter_batch_with_seq_id() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8View, false),
        Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
    ]));

    let labels: Vec<String> = (0..BATCH_ENTITIES)
        .map(|i| format!("entity_{i:06}"))
        .collect();
    let entities: Vec<&str> = (0..BATCH_ROWS)
        .map(|i| labels[i % BATCH_ENTITIES].as_str())
        .collect();
    let tss: Vec<Option<i64>> = (0..BATCH_ROWS as i64)
        .map(|i| Some(BASE_NS + i * STEP_NS))
        .collect();
    let evts: Vec<&str> = vec!["click"; BATCH_ROWS];
    let seqs: Vec<i64> = (0..BATCH_ROWS as i64).collect();

    let id_arr: ArrayRef = Arc::new(StringViewArray::from(entities));
    let ts_arr: ArrayRef = Arc::new(TimestampNanosecondArray::from(tss).with_timezone("UTC"));
    let evt_arr: ArrayRef = Arc::new(StringViewArray::from(evts));
    let seq_arr: ArrayRef = Arc::new(Int64Array::from(seqs));

    RecordBatch::try_new(schema, vec![id_arr, ts_arr, evt_arr, seq_arr]).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Tombstone builders
// ─────────────────────────────────────────────────────────────────────────────

/// Build an entity-delete tombstone file with `n` entries.
///
/// The first `min(n, BATCH_ENTITIES)` entries target entity IDs that
/// ARE present in the filter batch (real HashSet hits), while entries
/// beyond `BATCH_ENTITIES` target phantom IDs not in the batch
/// (probes that find no match). This stresses both hit and miss paths
/// as `n` grows beyond the batch's entity cardinality.
fn entity_tombstones(n: usize) -> TombstoneFile {
    let deletes: HashSet<ScalarValue> = (0..n)
        .map(|i| ScalarValue::String(format!("entity_{i:06}")))
        .collect();
    TombstoneFile {
        entity_deletes: deletes,
        ..Default::default()
    }
}

/// Build a time-range tombstone file covering the first `pct` fraction
/// of `BATCH_ROWS` rows by timestamp.
///
/// With `STEP_NS = 1 s` and `BASE_NS = 2025-01-01`, a 10% cutoff
/// covers the first 6 554 of 65 536 rows.
fn time_range_tombstone(pct: f64) -> TombstoneFile {
    let cutoff = BASE_NS + (pct * (BATCH_ROWS - 1) as f64 * STEP_NS as f64) as i64;
    TombstoneFile::for_time_range(TimeRangeDelete {
        min_ts: Some(BASE_NS),
        min_inclusive: true,
        max_ts: Some(cutoff),
        max_inclusive: true,
    })
}

/// Build a mixed-granularity tombstone file combining:
/// - 100 entity-level deletes (entities 0–99 from the batch)
/// - 1 time-range delete covering the first 10% of rows
/// - 100 row-level seq_id deletes (seq_ids 0–99)
fn mixed_tombstones() -> TombstoneFile {
    let entity_deletes: HashSet<ScalarValue> = (0u64..100)
        .map(|i| ScalarValue::String(format!("entity_{i:06}")))
        .collect();
    let cutoff = BASE_NS + (0.10 * (BATCH_ROWS - 1) as f64 * STEP_NS as f64) as i64;
    TombstoneFile {
        entity_deletes,
        row_deletes: (0u64..100).collect(),
        time_range_deletes: vec![TimeRangeDelete {
            min_ts: Some(BASE_NS),
            min_inclusive: true,
            max_ts: Some(cutoff),
            max_inclusive: true,
        }],
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1: TombstoneFilter::filter_batch_with_index
// ─────────────────────────────────────────────────────────────────────────────

/// Descriptor for one query-time filter bench case.
struct FilterCase {
    name: &'static str,
    tombstones: TombstoneFile,
    /// Whether this case needs the extended batch (with `__seq_id`).
    needs_seq_id: bool,
    /// Reference-mode floor target (M rows/sec, at_least).
    floor_m_rows_per_sec: f64,
}

fn bench_query_time_filter(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let mut collector = BenchResultCollector::new(mode);

    let batch = make_filter_batch();
    let batch_seq = make_filter_batch_with_seq_id();

    let cases = vec![
        FilterCase {
            name: "entity_0",
            tombstones: TombstoneFile::default(),
            needs_seq_id: false,
            floor_m_rows_per_sec: 1_000.0, // [floor] short-circuit passthrough
        },
        FilterCase {
            name: "entity_100",
            tombstones: entity_tombstones(100),
            needs_seq_id: false,
            floor_m_rows_per_sec: 100.0, // [floor]
        },
        FilterCase {
            name: "entity_10000",
            tombstones: entity_tombstones(10_000),
            needs_seq_id: false,
            floor_m_rows_per_sec: 50.0, // [floor]
        },
        FilterCase {
            name: "time_range_10pct",
            tombstones: time_range_tombstone(0.10),
            needs_seq_id: false,
            floor_m_rows_per_sec: 100.0, // [floor]
        },
        FilterCase {
            name: "mixed",
            tombstones: mixed_tombstones(),
            needs_seq_id: true,
            floor_m_rows_per_sec: 50.0, // [floor] all granularities active
        },
    ];

    let mut group = c.benchmark_group("tombstone_scan/query_time");

    for case in &cases {
        let filter = TombstoneFilter::new(&case.tombstones, "entity_id", "ts");
        let index: EntityDeleteIndex = case.tombstones.entity_delete_index();
        let src_batch: &RecordBatch = if case.needs_seq_id {
            &batch_seq
        } else {
            &batch
        };

        group.bench_with_input(
            BenchmarkId::new("filter_batch_with_index", case.name),
            &case.name,
            |b, _| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        // RecordBatch::clone is a shallow Arc-clone; all data
                        // buffers are shared. The filter itself may produce a
                        // new batch via arrow::compute::filter_record_batch
                        // when rows are removed.
                        let out = filter
                            .filter_batch_with_index(src_batch.clone(), &index)
                            .unwrap();
                        black_box(out);
                    }
                    start.elapsed()
                })
            },
        );

        // Standalone throughput measurement for BenchResultCollector.
        let standalone_iters: u64 = if mode.is_reference() { 1_000 } else { 100 };
        let t = Instant::now();
        for _ in 0..standalone_iters {
            let out = filter
                .filter_batch_with_index(src_batch.clone(), &index)
                .unwrap();
            black_box(out);
        }
        let elapsed_s = t.elapsed().as_secs_f64();
        let rows_per_sec_m =
            (BATCH_ROWS as f64 * standalone_iters as f64) / elapsed_s / 1_000_000.0;

        let target = if mode.is_reference() {
            Some(BenchTarget::at_least(case.floor_m_rows_per_sec))
        } else {
            None
        };
        collector.record(
            &format!("tombstone_scan/query_time/{}/rows_per_sec_m", case.name),
            rows_per_sec_m,
            "M rows/sec",
            target,
        );
    }

    group.finish();
    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Compaction helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Seed `N_SEGMENTS` L0 segments and optionally write entity tombstones
/// covering `tombstone_entity_pct` of the total unique entities.
///
/// Returns `(db, pre_compaction_bytes)`. When `tombstone_entity_pct`
/// is positive, tombstone files are written to every active
/// `(window_id, shard_id)` pair discovered in the post-seed manifest.
fn seed_tombstoned_l0_stack(
    scratch: &ScratchDir,
    events_per_segment: usize,
    tombstone_entity_pct: f64,
) -> (Database, u64) {
    let schema = purchases_schema();
    let mut db = open_db_with_table(scratch.path(), "purchases", schema);

    let mut pre_bytes: u64 = 0;
    for seg_idx in 0..N_SEGMENTS {
        // Match the entity-tagging pattern from compaction.rs's
        // seed_l0_stack so the entity key space is identical.
        let events: Vec<bqlite_core::event::Event> =
            generate_events(events_per_segment, ENTITIES_PER_SEGMENT)
                .into_iter()
                .map(|mut e| {
                    if let bqlite_core::event::EntityId::String(ref mut s) = e.entity {
                        *s = format!("{s}_seg{seg_idx}");
                    }
                    e
                })
                .collect();

        let batch_id = db.allocate_batch_id("purchases").unwrap();
        let mut partitioner =
            Partitioner::new(SHARD_COUNT, 30, batch_id, PARTITION_BUDGET_BYTES).unwrap();
        for event in events {
            partitioner.push_event(event).unwrap();
        }
        let mut writer = SegmentWriter::new(&mut db);
        let metas = writer.write_partitioner("purchases", partitioner).unwrap();
        pre_bytes += metas.iter().map(|m| m.byte_size).sum::<u64>();
    }

    if tombstone_entity_pct > 0.0 {
        let total_entities = N_SEGMENTS * ENTITIES_PER_SEGMENT;
        // Round up so even 1% of a small fixture tombstones at least one entity.
        let n_tombstone =
            ((tombstone_entity_pct * total_entities as f64).ceil() as usize).min(total_entities);

        let mut entity_deletes: HashSet<ScalarValue> = HashSet::with_capacity(n_tombstone);
        let mut count = 0;
        'outer: for seg_idx in 0..N_SEGMENTS {
            for entity_idx in 0..ENTITIES_PER_SEGMENT {
                entity_deletes.insert(ScalarValue::String(format!(
                    "user_{entity_idx:06}_seg{seg_idx}"
                )));
                count += 1;
                if count >= n_tombstone {
                    break 'outer;
                }
            }
        }

        let tf = TombstoneFile {
            entity_deletes,
            ..Default::default()
        };

        // Write to every active (window_id, shard_id) bucket in the manifest.
        // With SHARD_COUNT=1 and all events on the same 30-day window this is
        // always exactly one file, but we stay manifest-driven for correctness.
        let manifest = db.manifest();
        if let Some(table_entry) = manifest.tables.get("purchases") {
            for window in &table_entry.windows {
                for (shard_id, segments) in window.shards.iter().enumerate() {
                    if !segments.is_empty() {
                        let path = tombstone_file_path(
                            scratch.path(),
                            "purchases",
                            window.window_id,
                            shard_id as u16,
                        );
                        write_tombstone_atomic(&path, &tf).unwrap();
                    }
                }
            }
        }
    }

    (db, pre_bytes)
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2: CompactionTombstoneScan via Database::compact_now
// ─────────────────────────────────────────────────────────────────────────────

fn bench_compaction_tombstone(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let events_per_segment = match mode {
        BenchMode::Ci => 10_000,
        BenchMode::Reference => 500_000,
    };
    let mut collector = BenchResultCollector::new(mode);

    // ── Criterion group for timing ─────────────────────────────────────────
    let mut group = c.benchmark_group("tombstone_scan/compaction");

    // ── Clean-segment baseline (no tombstones) ─────────────────────────────
    group.bench_function("compact_now_clean", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let iter_scratch = ScratchDir::new("compact-ts-clean");
                let (mut db, _) = seed_tombstoned_l0_stack(&iter_scratch, events_per_segment, 0.0);
                let out = db.compact_now("purchases").expect("compact_now clean");
                black_box(&out);
            }
            start.elapsed()
        })
    });

    // ── Tombstone-density sweep ────────────────────────────────────────────
    for &pct in &[0.01_f64, 0.05, 0.10] {
        let label = format!("{:02.0}pct", pct * 100.0);
        group.bench_with_input(
            BenchmarkId::new("compact_now_tombstoned", &label),
            &pct,
            |b, &p| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let iter_scratch = ScratchDir::new("compact-ts-iter");
                        let (mut db, _) =
                            seed_tombstoned_l0_stack(&iter_scratch, events_per_segment, p);
                        let out = db.compact_now("purchases").expect("compact_now tombstoned");
                        black_box(&out);
                    }
                    start.elapsed()
                })
            },
        );
    }

    group.finish();

    // ── Standalone MB/s measurements for BenchResultCollector ─────────────
    // Run once per configuration outside Criterion to get a stable
    // single-shot MB/s number. Criterion's `iter_custom` loop is the
    // regression signal; this path drives the collector targets.

    let clean_scratch = ScratchDir::new("compact-ts-standalone-clean");
    let (mut clean_db, clean_bytes) =
        seed_tombstoned_l0_stack(&clean_scratch, events_per_segment, 0.0);
    let t = Instant::now();
    let out = clean_db
        .compact_now("purchases")
        .expect("compact_now standalone clean");
    let clean_s = t.elapsed().as_secs_f64();
    black_box(&out);
    let clean_mb_per_sec = clean_bytes as f64 / clean_s / (1024.0 * 1024.0);
    collector.record(
        "tombstone_scan/compaction/clean_baseline/mb_per_sec",
        clean_mb_per_sec,
        "MB/s",
        None,
    );

    let mut tombstoned_10pct_mb_per_sec = 0.0_f64;

    for &pct in &[0.01_f64, 0.05, 0.10] {
        let label = format!("{:02.0}pct", pct * 100.0);
        let scratch = ScratchDir::new("compact-ts-standalone");
        let (mut db, bytes) = seed_tombstoned_l0_stack(&scratch, events_per_segment, pct);
        let t = Instant::now();
        let out = db
            .compact_now("purchases")
            .expect("compact_now standalone tombstoned");
        let elapsed_s = t.elapsed().as_secs_f64();
        black_box(&out);

        let mb_per_sec = bytes as f64 / elapsed_s / (1024.0 * 1024.0);
        collector.record(
            &format!("tombstone_scan/compaction/tombstoned_{label}/mb_per_sec"),
            mb_per_sec,
            "MB/s",
            None,
        );

        if (pct - 0.10).abs() < 1e-9 {
            tombstoned_10pct_mb_per_sec = mb_per_sec;
        }
    }

    // [floor] clean-to-tombstoned MB/s ratio at 10% entity density must
    // not exceed 2.0 — tombstoning at most doubles compaction overhead.
    let ratio = if tombstoned_10pct_mb_per_sec > 0.0 {
        clean_mb_per_sec / tombstoned_10pct_mb_per_sec
    } else {
        f64::INFINITY
    };
    let ratio_target = if mode.is_reference() {
        Some(BenchTarget::at_most(2.0))
    } else {
        None
    };
    collector.record(
        "tombstone_scan/compaction/tombstoned_10pct/clean_to_tombstoned_ratio",
        ratio,
        "ratio",
        ratio_target,
    );

    collector.finish();
}

criterion_group! {
    name = tombstone_scan_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets = bench_query_time_filter, bench_compaction_tombstone,
}
criterion_main!(tombstone_scan_benches);
