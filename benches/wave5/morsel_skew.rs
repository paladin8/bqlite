//! Morsel scheduler skew bench (TASK-526, Wave 5).
//!
//! Measures `Engine::query` wall-clock under a deliberately skewed
//! entity-event distribution. Today's bqlite morsel scheduler
//! dispatches one degenerate "whole-database" task per query and
//! records exactly one `WorkerMetricsSnapshot::default()` per query
//! at `crates/bqlite-engine/src/query.rs:487` — by design, not
//! oversight. The Wave 5 perf module documents this in
//! `crates/bqlite-engine/src/perf.rs` "Wave 5 scope":
//! *"Morsel / skew / worker rows — present as fields, all-zero
//! today. They become non-zero once the morsel scheduler
//! (TASK-523 follow-up) records per-worker snapshots through
//! `QueryContext::record_worker_snapshot`."*
//!
//! So the bench is a wall-clock regression tripwire, not a metric
//! assertion bench. Once the scheduler populates real per-worker
//! snapshots, this bench upgrades to assert on
//! `entity_event_skew_p99` / `worker_busy_ns_max - worker_busy_ns_min`
//! directly.
//!
//! Two scenarios:
//!
//! - `balanced/throughput` — total events split evenly across all
//!   entities. The reference workload point.
//! - `skewed/throughput` — 70 % of events concentrated on one
//!   entity, the remaining 30 % spread across the long tail. Same
//!   total event count and same query, so any wall-clock difference
//!   is attributable to the per-entity work distribution.
//!
//! [`floor`] target: `skewed_ns / balanced_ns ≤ 4.0`. A skew tax
//! beyond 4× signals a morsel-generation regression — today's
//! single-task driver should produce roughly identical wall-clock
//! between balanced and skewed fixtures because per-entity work is
//! sequential anyway. The 4× ceiling leaves headroom for the future
//! per-shard/per-morsel scheduler that should *narrow* the skew
//! tax, not widen it. `engine/morsel-scheduler.md` does not pin a
//! numerical ratio.
//!
//! Sanity row: both scenarios also report the per-query result row
//! count so a divergence that can't be explained by scheduler skew
//! (e.g. an upstream scan regression that misses rows on the skewed
//! input) shows up as a row-count delta rather than a wall-clock
//! blowup. `bytes_scanned` is *not* asserted on today: the engine
//! `ExecutionResult.metrics.operator.bytes_scanned` is zero because
//! `ScanOperator` is constructed via `ScanOperator::with_tombstones`
//! at `bqlite-engine::bind` line ~1376 *without* an
//! `attach_metrics` call, so the scan path does not contribute to
//! the per-query aggregate yet. Once that wiring lands, the sanity
//! row upgrades to assert on `bytes_scanned` parity.

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::time::Timestamp;
use bqlite_engine::{Database, Engine};
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

const CREATE_EVENTS: &str = "\
    CREATE TABLE events (\
        entity_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE, \
        amount INT\
    )";

/// Representative analytical query: per-entity row count. Forces
/// the engine to materialise a hash aggregate keyed on entity_id,
/// which surfaces per-entity work imbalance under skewed input.
const QUERY: &str = "events | STATS rows = COUNT(*) GROUP BY entity_id";

struct SkewSizing {
    total_events: usize,
    entity_count: usize,
}

impl SkewSizing {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => SkewSizing {
                total_events: 200_000,
                entity_count: 1_000,
            },
            BenchMode::Reference => SkewSizing {
                total_events: 10_000_000,
                entity_count: REF_ENTITY_COUNT,
            },
        }
    }
}

/// Generate `total` events spread evenly across `entity_count`
/// entities. Each entity gets the same per-entity event budget.
fn generate_balanced(total: usize, entity_count: usize) -> Vec<Event> {
    let entity_count = entity_count.max(1);
    let events_per_entity = total / entity_count;
    let base_ns: i64 = 1_735_689_600_000_000_000;
    let step_ns: i64 = 60_000_000_000;
    let mut events = Vec::with_capacity(total);
    for entity_idx in 0..entity_count {
        let entity = EntityId::String(format!("user_{entity_idx:06}"));
        let count = if entity_idx < entity_count - 1 {
            events_per_entity
        } else {
            total - events_per_entity * (entity_count - 1)
        };
        for ev_idx in 0..count {
            let ts = Timestamp::from_nanos(base_ns + (ev_idx as i64) * step_ns);
            events.push(Event::with_properties(
                entity.clone(),
                ts,
                "view".to_string(),
                vec![("amount".into(), PropertyValue::Int((ev_idx as i64) % 64))],
            ));
        }
    }
    events
}

/// Generate `total` events with 70 % on one dominant entity and the
/// remaining 30 % spread evenly across the long tail. Total event
/// count and `entity_count` match the balanced fixture so wall-clock
/// comparisons are apples-to-apples.
fn generate_skewed(total: usize, entity_count: usize) -> Vec<Event> {
    let entity_count = entity_count.max(2);
    let dominant_count = (total * 7) / 10;
    let tail_count = total - dominant_count;
    let tail_entities = entity_count - 1;
    let tail_per_entity = tail_count / tail_entities.max(1);
    let base_ns: i64 = 1_735_689_600_000_000_000;
    let step_ns: i64 = 60_000_000_000;
    let mut events = Vec::with_capacity(total);

    // Dominant entity — `user_000000` so it sorts first.
    {
        let entity = EntityId::String("user_000000".to_string());
        for ev_idx in 0..dominant_count {
            let ts = Timestamp::from_nanos(base_ns + (ev_idx as i64) * step_ns);
            events.push(Event::with_properties(
                entity.clone(),
                ts,
                "view".to_string(),
                vec![("amount".into(), PropertyValue::Int((ev_idx as i64) % 64))],
            ));
        }
    }

    // Tail entities — `user_000001` .. `user_<entity_count-1>`.
    for entity_idx in 1..entity_count {
        let entity = EntityId::String(format!("user_{entity_idx:06}"));
        let count = if entity_idx < entity_count - 1 {
            tail_per_entity
        } else {
            tail_count - tail_per_entity * (tail_entities - 1)
        };
        for ev_idx in 0..count {
            let ts = Timestamp::from_nanos(base_ns + (ev_idx as i64) * step_ns);
            events.push(Event::with_properties(
                entity.clone(),
                ts,
                "view".to_string(),
                vec![("amount".into(), PropertyValue::Int((ev_idx as i64) % 64))],
            ));
        }
    }
    events
}

/// Build a single-shard scratch database, ingest `events`, and
/// return the handle. The `ScratchDir` cleans up on drop.
fn setup_db(label: &str, events: Vec<Event>) -> (ScratchDir, Database, Engine) {
    let engine = Engine::new();
    let scratch = ScratchDir::new(label);
    let mut db =
        Database::create_with_shards(scratch.path(), 1).expect("Database::create_with_shards");
    engine.query(CREATE_EVENTS, &mut db).expect("CREATE TABLE");
    let total_events = events.len();
    let batch_id = db.allocate_batch_id("events").expect("allocate batch_id");
    let shard_count = db.manifest().shard_count;
    let budget = (total_events * 200).max(256 * 1024 * 1024);
    let mut partitioner =
        Partitioner::new(shard_count, 30, batch_id, budget).expect("create partitioner");
    for event in events {
        partitioner.push_event(event).expect("push event");
    }
    {
        let mut writer = SegmentWriter::new(&mut db);
        writer
            .write_partitioner("events", partitioner)
            .expect("write partitioner");
    }
    (scratch, db, engine)
}

// ─────────────────────────────────────────────────────────────────────────────
// Bench
// ─────────────────────────────────────────────────────────────────────────────

fn bench_morsel_skew(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = SkewSizing::for_mode(mode);

    let (_balanced_dir, mut balanced_db, balanced_engine) = setup_db(
        "wave5-skew-balanced",
        generate_balanced(sizing.total_events, sizing.entity_count),
    );
    let (_skewed_dir, mut skewed_db, skewed_engine) = setup_db(
        "wave5-skew-skewed",
        generate_skewed(sizing.total_events, sizing.entity_count),
    );

    // Probe both fixtures once. Capture wall-clock + per-query
    // group cardinality for the regression-gate JSON; the timed
    // loop is the primary signal.
    let probe_balanced = {
        let start = Instant::now();
        let result = balanced_engine
            .query(QUERY, &mut balanced_db)
            .expect("balanced probe");
        let rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
        (start.elapsed().as_nanos(), rows)
    };
    let probe_skewed = {
        let start = Instant::now();
        let result = skewed_engine
            .query(QUERY, &mut skewed_db)
            .expect("skewed probe");
        let rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
        (start.elapsed().as_nanos(), rows)
    };
    assert!(
        probe_balanced.0 > 0 && probe_skewed.0 > 0,
        "probes should produce non-zero wall-clock"
    );
    assert_eq!(
        probe_balanced.1, probe_skewed.1,
        "balanced and skewed fixtures must emit the same group cardinality \
         (entity_count). A divergence here would mean an upstream regression \
         that misses rows on one of the inputs and would pollute the skew tax \
         signal"
    );

    let mut group = c.benchmark_group("wave5/morsel_skew");
    group.throughput(Throughput::Elements(sizing.total_events as u64));

    group.bench_function("balanced/throughput", |b| {
        b.iter(|| {
            let result = balanced_engine
                .query(QUERY, &mut balanced_db)
                .expect("balanced query");
            let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
            black_box(total)
        });
    });
    group.bench_function("skewed/throughput", |b| {
        b.iter(|| {
            let result = skewed_engine
                .query(QUERY, &mut skewed_db)
                .expect("skewed query");
            let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
            black_box(total)
        });
    });
    group.finish();

    let mut collector = BenchResultCollector::new(mode);
    collector.record(
        "wave5/morsel_skew/balanced/probe_ns",
        probe_balanced.0 as f64,
        "ns",
        None,
    );
    collector.record(
        "wave5/morsel_skew/skewed/probe_ns",
        probe_skewed.0 as f64,
        "ns",
        None,
    );
    let skew_tax = probe_skewed.0 as f64 / probe_balanced.0.max(1) as f64;
    collector.record(
        "wave5/morsel_skew/skew_tax_ratio",
        skew_tax,
        "ratio",
        Some(BenchTarget::at_most(4.0)),
    );
    collector.finish();
}

criterion_group! {
    name = morsel_skew_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(3));
    targets = bench_morsel_skew,
}
criterion_main!(morsel_skew_benches);
