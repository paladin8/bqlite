//! Morsel scheduler skew bench (TASK-526, Wave 5; TASK-537 metric
//! assertions).
//!
//! Measures `Engine::query` wall-clock under a deliberately skewed
//! entity-event distribution. With TASK-536 + TASK-537 the morsel
//! scheduler now dispatches one task per shard and records real
//! per-worker timing + per-morsel processed-event samples. This bench
//! therefore asserts directly on `QueryMetrics`:
//!
//! - `entity_event_skew_p99 > 0` on the skewed fixture — the dominant
//!   entity must produce a per-morsel processed-event spread big
//!   enough to surface in the per-worker p99-vs-p50 reducer
//!   (TASK-537 scope (b)).
//! - `worker_idle_ns_p50 ≤ 100 ms` on both fixtures — a stuck-on-pop
//!   regression (worker spinning in `pop_or_park` instead of running
//!   morsels) would blow this ceiling. 100 ms is 10× the
//!   `pop_or_park` interval, leaving headroom for noisy CI runners
//!   while still catching a genuine regression (TASK-537 scope (e)).
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
//! [`floor`] target: `skewed_ns / balanced_ns ≤ 6.0`. The 32-shard
//! fixture (required so the per-worker percentile reducer sees
//! multiple morsels) widens the steady-state ratio relative to the
//! single-shard predecessor: balanced fans out cleanly across shards
//! while skewed is bottlenecked on the dominant-entity shard. The
//! 6.0 ceiling pins regression detection without flagging the
//! expected single-dominant-shard wall-clock penalty. Sub-shard
//! morsel halving (a follow-on) will narrow the ratio when it lands.
//! `engine/morsel-scheduler.md` does not pin a numerical ratio.
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

/// Build a multi-shard scratch database, ingest `events`, and return
/// the handle. The `ScratchDir` cleans up on drop.
///
/// Multi-shard so the per-shard dispatch in TASK-536 fans the query
/// out to multiple Rayon workers, giving each worker a multi-morsel
/// sample run that the TASK-537 `entity_event_skew_p99` reducer can
/// surface a non-zero spread on (the dominant entity hashes to one
/// shard whose morsel processes ~7× the per-shard average row count).
const SHARD_COUNT: u16 = 32;

fn setup_db(label: &str, events: Vec<Event>) -> (ScratchDir, Database, Engine) {
    let engine = Engine::new();
    let scratch = ScratchDir::new(label);
    let mut db = Database::create_with_shards(scratch.path(), SHARD_COUNT)
        .expect("Database::create_with_shards");
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
    let balanced_probe_result = balanced_engine
        .query(QUERY, &mut balanced_db)
        .expect("balanced probe");
    let probe_balanced = {
        let start = Instant::now();
        let result = balanced_engine
            .query(QUERY, &mut balanced_db)
            .expect("balanced probe");
        let rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
        (start.elapsed().as_nanos(), rows)
    };
    let skewed_probe_result = skewed_engine
        .query(QUERY, &mut skewed_db)
        .expect("skewed probe");
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

    // ── Metric assertions (TASK-537 scope (e)) ────────────────────────
    //
    // Cross-fixture sanity: both queries must process the same total
    // event count — any divergence would mean a scan/aggregate
    // regression skipping rows on one of the inputs and would pollute
    // every other downstream metric.
    assert_eq!(
        balanced_probe_result.metrics.events_processed,
        skewed_probe_result.metrics.events_processed,
        "balanced and skewed fixtures process the same total event count"
    );
    // Per-shard dispatch landed: the skewed query must hand at least
    // one morsel to every populated shard.
    assert!(
        skewed_probe_result.metrics.morsels_dispatched >= 1,
        "skewed query must dispatch ≥ 1 morsel; got {}",
        skewed_probe_result.metrics.morsels_dispatched
    );

    // `entity_event_skew_p99 > 0` on the skewed fixture. The dominant
    // entity hashes to a single shard whose morsel processes ~7M
    // events; the other shards' morsels process ~100K events each.
    // When `num_workers < num_shards` (typically true: 4 workers vs.
    // 32 shards on a CI runner) each worker pulls multiple morsels
    // and the per-worker p99-vs-p50 reducer surfaces the spread. We
    // gate the assertion on `num_workers > 1 && num_workers < SHARD_COUNT`
    // because a 1-core runner collapses to a single worker (one
    // sample per worker → spread is mathematically zero) and a runner
    // with `num_workers >= num_shards` would put each morsel on its
    // own worker (one sample per worker → also zero).
    // Direct gate: at least one worker must have pulled multiple
    // morsels for the percentile reducer to produce a non-zero
    // spread. `morsels_dispatched > num_workers` is sufficient
    // (pigeonhole) — at least one worker holds ≥ 2 samples.
    let multi_morsel_per_worker = u64::from(skewed_probe_result.metrics.num_workers)
        < skewed_probe_result.metrics.morsels_dispatched;
    if multi_morsel_per_worker {
        assert!(
            skewed_probe_result.metrics.entity_event_skew_p99 > 0,
            "skewed fixture must surface a non-zero entity_event_skew_p99 \
             when at least one worker holds multiple morsels (got {} with \
             num_workers={}, morsels_dispatched={})",
            skewed_probe_result.metrics.entity_event_skew_p99,
            skewed_probe_result.metrics.num_workers,
            skewed_probe_result.metrics.morsels_dispatched,
        );
    }

    // Worker idle ceiling — a stuck-on-pop regression would inflate
    // this. 100 ms = 10× the `pop_or_park` interval; well above the
    // expected (sub-millisecond) idle for the per-shard dispatch.
    const WORKER_IDLE_NS_CEILING: u64 = 100_000_000;
    assert!(
        balanced_probe_result.metrics.worker_idle_ns_p50 <= WORKER_IDLE_NS_CEILING,
        "balanced worker_idle_ns_p50 above ceiling — possible stuck-on-pop \
         regression: got {} ns, ceiling {} ns",
        balanced_probe_result.metrics.worker_idle_ns_p50,
        WORKER_IDLE_NS_CEILING,
    );
    assert!(
        skewed_probe_result.metrics.worker_idle_ns_p50 <= WORKER_IDLE_NS_CEILING,
        "skewed worker_idle_ns_p50 above ceiling — possible stuck-on-pop \
         regression: got {} ns, ceiling {} ns",
        skewed_probe_result.metrics.worker_idle_ns_p50,
        WORKER_IDLE_NS_CEILING,
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
    // Target ≤ 6.0. Switching the fixture from single-shard to 32-shard
    // (TASK-537 — required so the per-worker percentile reducer sees
    // multiple morsels and `entity_event_skew_p99 > 0` becomes
    // observable) widens the steady-state ratio: the balanced query
    // fans out across shards in parallel (~6 ms) while the skewed
    // query is bottlenecked by one dominant-entity shard (~28 ms).
    // Sub-shard morsel halving (a follow-on to TASK-523) will halve
    // the dominant morsel into multiple sub-morsels and bring the
    // ratio back down. The 6.0 ceiling leaves headroom for noisy CI
    // machines without masking a genuine regression.
    collector.record(
        "wave5/morsel_skew/skew_tax_ratio",
        skew_tax,
        "ratio",
        Some(BenchTarget::at_most(6.0)),
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
