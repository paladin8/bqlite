//! Cohort pushdown savings bench (TASK-526, Wave 5).
//!
//! Measures the byte-savings the post-cohort entity-id pushdown
//! gate (`crates/bqlite-engine/src/cohort_pushdown.rs`,
//! `optimizer-direction.md` §7 row 9) extracts on a realistic
//! `(entity, ts, event_type, ...)` segment. When a cohort fits the
//! 65,536-entity gate, the engine folds it into a
//! [`ScanConjunct::EntityIn`] that zone-map-prunes row groups whose
//! entity range does not overlap the cohort's `[set_min, set_max]`.
//!
//! Two scenarios drive the same `SegmentFileReader::scan` over the
//! same on-disk segment (storage-layer comparison so the bench can
//! observe `MetricsSnapshot::bytes_scanned` directly — the engine's
//! `ScanOperator` does not currently thread its `Metrics` handle
//! into the reader, so the operator-layer comparison would observe
//! zero counters today):
//!
//! - `pushdown_eligible` — `reader.scan(projection, Some(predicate))`
//!   with a `ScanPredicate { conjuncts: [EntityIn { user_id, [v] }] }`.
//!   The zone-map check at `ScanConjunct::accepts_zone` accepts only
//!   row groups whose entity-zone overlaps the cohort's
//!   `[set_min, set_max]`.
//! - `pushdown_baseline` — `reader.scan(projection, None)`. Every
//!   row group is decoded.
//!
//! Both calls attach an `AtomicMetrics` handle and drive the scan
//! to exhaustion via `next_row_group`.
//!
//! [`floor`] target:
//! `pushdown_eligible/bytes_scanned ≤ 0.5 * pushdown_baseline/bytes_scanned`.
//! Pushdown should at least halve bytes scanned when the cohort
//! spans a small entity range. `optimizer-direction.md` §7 row 9
//! makes only a qualitative claim ("a non-trivial fraction of row
//! groups must be skippable"), so this is a `[floor]` regression
//! tripwire, not a `[spec]` derivation. The 2× savings is
//! calibrated against a fixture (single-entity cohort, small row
//! groups so each row-group's entity-zone is tight) that gives the
//! gate its best case; a regression that turns the conjunct into a
//! no-op surfaces as a ratio close to 1.0.

use std::sync::Arc;
use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::metrics::{AtomicMetrics, Metrics, MetricsSnapshot};
use bqlite_core::property::PropertyValue;
use bqlite_core::storage::{ColumnProjection, Predicate, ScanConjunct, ScanPredicate, SegmentScan};
use bqlite_core::Result;
use bqlite_storage::segment::reader::SegmentFileReader;
use bqlite_storage::writer::{SegmentWriter, WriterConfig};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

struct PushdownSizing {
    n_events: usize,
    n_entities: usize,
    /// Rows per row group. Small values keep each row group's
    /// entity-zone tight so EntityIn pruning can isolate a single
    /// entity to a single row group.
    row_group_size: usize,
}

impl PushdownSizing {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => PushdownSizing {
                n_events: 50_000,
                n_entities: 500,
                row_group_size: 100,
            },
            BenchMode::Reference => PushdownSizing {
                n_events: 1_000_000,
                n_entities: REF_ENTITY_COUNT,
                row_group_size: 100,
            },
        }
    }
}

/// Write a segment with `n_events` events spread over `n_entities`
/// entities, packed at `row_group_size` rows per row-group.
/// Returns the on-disk bytes plus the table schema.
fn write_segment(
    sizing: &PushdownSizing,
) -> (Vec<u8>, bqlite_core::schema::TableSchema, ScratchDir) {
    let schema = purchases_schema();
    let scratch = ScratchDir::new("wave5-cohort-pushdown");
    let mut db = open_db_with_table(scratch.path(), "purchases", schema.clone());
    let events = generate_events(sizing.n_events, sizing.n_entities);
    let batch_id = db.allocate_batch_id("purchases").unwrap();
    {
        let mut writer = SegmentWriter::with_config(
            &mut db,
            WriterConfig {
                row_group_size: sizing.row_group_size,
            },
        );
        writer
            .write_bucket("purchases", 0, 0, batch_id, &events)
            .unwrap();
    }
    let seg_paths = find_segment_files(scratch.path());
    assert!(!seg_paths.is_empty(), "no segment files written");
    let bytes = std::fs::read(&seg_paths[0]).unwrap();
    (bytes, schema, scratch)
}

/// Drive a storage-layer scan with the supplied predicate (or
/// `None`) and return the metrics snapshot plus the row count.
fn drive_scan(
    bytes: &[u8],
    schema: &bqlite_core::schema::TableSchema,
    predicate: Option<Arc<dyn Predicate>>,
) -> Result<(MetricsSnapshot, usize)> {
    let metrics: Arc<dyn Metrics> = Arc::new(AtomicMetrics::new());
    let reader = SegmentFileReader::from_bytes(bytes.to_vec(), schema.clone())?;
    let mut scan = reader.scan(&ColumnProjection::all(), predicate)?;
    scan.attach_metrics(Arc::clone(&metrics));
    let mut total = 0usize;
    while let Some(batch) = scan.next_row_group()? {
        total += batch.num_rows();
        let _ = black_box(batch);
    }
    Ok((metrics.snapshot(), total))
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench
// ─────────────────────────────────────────────────────────────────────────────

fn bench_cohort_pushdown(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = PushdownSizing::for_mode(mode);
    let (bytes, schema, _scratch) = write_segment(&sizing);

    // Cohort = a single entity. The EntityIn conjunct's
    // [set_min, set_max] then collapses to a single value, giving
    // the zone-map check its best case for pruning. The entity is
    // picked from the middle of the lexicographic range so its row
    // group is interior (rules out the trivial leading-row-group
    // case).
    let cohort_entity = format!("user_{:06}", sizing.n_entities / 2);
    let entity_in_conjunct = ScanConjunct::entity_in(
        "user_id".to_string(),
        vec![PropertyValue::String(cohort_entity.clone())],
    )
    .expect("entity_in conjunct must build for non-empty values");
    let pushdown_predicate: Arc<dyn Predicate> =
        Arc::new(ScanPredicate::new(vec![entity_in_conjunct]));

    // Probe both scans once outside the timed loop. The probes
    // pin the bench-correctness invariants and feed the [floor]
    // target.
    let (probe_pushdown, pushdown_rows) =
        drive_scan(&bytes, &schema, Some(Arc::clone(&pushdown_predicate))).expect("pushdown probe");
    let (probe_baseline, baseline_rows) =
        drive_scan(&bytes, &schema, None).expect("baseline probe");

    assert!(
        baseline_rows > 0,
        "baseline scan produced zero rows; fixture is empty?"
    );
    assert!(
        baseline_rows >= pushdown_rows,
        "baseline scan ({baseline_rows} rows) cannot produce fewer rows than pushdown scan \
         ({pushdown_rows} rows) — fixture invariant violated",
    );
    assert!(
        probe_pushdown.bytes_scanned > 0 && probe_baseline.bytes_scanned > 0,
        "scan path did not record bytes_scanned for the probe — \
         attach_metrics wiring regressed",
    );
    assert!(
        probe_pushdown.bytes_scanned <= probe_baseline.bytes_scanned,
        "pushdown scan should not read MORE bytes than the baseline; \
         {} > {}",
        probe_pushdown.bytes_scanned,
        probe_baseline.bytes_scanned,
    );

    let mut group = c.benchmark_group("wave5/cohort_pushdown");
    group.throughput(Throughput::Elements(sizing.n_events as u64));

    let pred_for_iter = Arc::clone(&pushdown_predicate);
    group.bench_function("pushdown_eligible/scan_wallclock", |b| {
        b.iter(|| {
            let (snap, total) =
                drive_scan(&bytes, &schema, Some(Arc::clone(&pred_for_iter))).expect("scan iter");
            black_box((snap.bytes_scanned, total))
        });
    });
    group.bench_function("pushdown_baseline/scan_wallclock", |b| {
        b.iter(|| {
            let (snap, total) = drive_scan(&bytes, &schema, None).expect("scan iter");
            black_box((snap.bytes_scanned, total))
        });
    });
    group.finish();

    let mut collector = BenchResultCollector::new(mode);
    collector.record(
        "wave5/cohort_pushdown/pushdown_eligible/bytes_scanned",
        probe_pushdown.bytes_scanned as f64,
        "bytes",
        None,
    );
    collector.record(
        "wave5/cohort_pushdown/pushdown_baseline/bytes_scanned",
        probe_baseline.bytes_scanned as f64,
        "bytes",
        None,
    );
    let savings_ratio =
        probe_pushdown.bytes_scanned as f64 / probe_baseline.bytes_scanned.max(1) as f64;
    collector.record(
        "wave5/cohort_pushdown/bytes_scanned_savings_ratio",
        savings_ratio,
        "ratio",
        Some(BenchTarget::at_most(0.5)),
    );
    // Sanity row: probe wall-clock isn't a hard target, but feeds
    // trend analysis. Single-iteration estimate; Criterion's
    // per-sample comparison is the primary signal.
    let probe_pushdown_ns = {
        let start = Instant::now();
        let _ = drive_scan(&bytes, &schema, Some(Arc::clone(&pushdown_predicate))).unwrap();
        start.elapsed().as_nanos() as f64
    };
    let probe_baseline_ns = {
        let start = Instant::now();
        let _ = drive_scan(&bytes, &schema, None).unwrap();
        start.elapsed().as_nanos() as f64
    };
    collector.record(
        "wave5/cohort_pushdown/pushdown_eligible/probe_ns",
        probe_pushdown_ns,
        "ns",
        None,
    );
    collector.record(
        "wave5/cohort_pushdown/pushdown_baseline/probe_ns",
        probe_baseline_ns,
        "ns",
        None,
    );
    collector.finish();
}

criterion_group! {
    name = cohort_pushdown_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(2));
    targets = bench_cohort_pushdown,
}
criterion_main!(cohort_pushdown_benches);
