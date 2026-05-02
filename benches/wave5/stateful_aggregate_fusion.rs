//! Stateful-to-aggregate fusion bench (TASK-526, Wave 5).
//!
//! Measures the throughput of the `SESSIONIZE → STATS` pipeline
//! through the engine end-to-end. The TASK-520 planner pass
//! (`bqlite-planner::opt::fuse_match_aggregate`) folds an aggregate
//! whose inputs are all in the sessionize operator's *native*
//! output schema into the sessionize descriptor's
//! `fused_aggregate` slot, letting the operator's per-entity finish
//! step update the accumulator inline instead of materialising a
//! per-entity `RecordBatch` for the downstream `HashAggregate` to
//! re-scan. See `docs/design/engine/operator-fusion.md` and the
//! TASK-520 path in `bqlite-engine::bind`.
//!
//! Two scenarios:
//!
//! - `sessionize_to_count_max/fused/throughput` — `SESSIONIZE | STATS
//!   sessions = MAX(session_id) GROUP BY entity_id`. Both `session_id`
//!   and `entity_id` are in the sessionize native output, so the
//!   planner fuses the aggregate into the sessionize descriptor.
//!   This is the "good" path the bench wants to keep fast.
//! - `sessionize_to_sum_amount/unfused/throughput` — `SESSIONIZE |
//!   STATS total = SUM(amount) GROUP BY entity_id`. `amount` is a
//!   scan column not present in sessionize's native output, so the
//!   fusion pass leaves the aggregate at the root and the operator
//!   tree falls back to the unfused chain. This is the regression
//!   baseline: a Criterion delta between fused and unfused that
//!   shrinks would mean fusion is no longer paying off.
//!
//! Both queries materialise the same number of group-by keys
//! (`entity_count` per fixture) and run over the same row count, so
//! the difference between them is dominated by the per-entity
//! materialisation cost the fused path avoids.
//!
//! [`floor`] target: `unfused/fused ≥ 0.95`. The fused path must
//! not be measurably slower than the unfused fallback. A ratio that
//! drops below 0.95 means either (a) the fusion pass stopped firing
//! for the fused query (the pass returns the input plan unchanged),
//! or (b) the inline-accumulator path hit a slow-path that the
//! unfused chain avoids. The 0.95 threshold absorbs CI runner
//! noise around the no-effect point of 1.0 — the standard "1.5×
//! headroom" calibration rule does not translate cleanly to a ratio
//! whose meaningful boundary is fixed at 1.0. The *primary*
//! regression signal remains Criterion's sample-level comparison of
//! the fused-only function. `engine/operator-fusion.md` does not
//! pin a numerical ratio, so this is a `[floor]` regression
//! tripwire rather than a `[spec]` derivation.

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

const FUSED_QUERY: &str = "\
    events | SELECT entity_id, ts, event_type \
           | SESSIONIZE(gap: 2h) \
           | STATS sessions = MAX(session_id) GROUP BY entity_id";

const UNFUSED_QUERY: &str = "\
    events | SESSIONIZE(gap: 2h) \
           | STATS total = SUM(amount) GROUP BY entity_id";

struct FusionSizing {
    total_events: usize,
    entity_count: usize,
}

impl FusionSizing {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => FusionSizing {
                total_events: 100_000,
                entity_count: 1_000,
            },
            BenchMode::Reference => FusionSizing {
                total_events: 10_000_000,
                entity_count: REF_ENTITY_COUNT,
            },
        }
    }
}

/// Generate `total` events spread evenly across `entity_count`
/// entities. Every event carries an `amount` Int property so the
/// unfused `SUM(amount)` query has a defined input.
fn generate_fusion_events(total: usize, entity_count: usize) -> Vec<Event> {
    let events_per_entity = total / entity_count.max(1);
    // 2025-01-01T00:00:00Z.
    let base_ns: i64 = 1_735_689_600_000_000_000;
    // ~30 minute step inside an entity, with one ~3h jump every 5
    // events so SESSIONIZE produces multiple sessions per entity.
    let step_ns: i64 = 30 * 60_000_000_000;
    let gap_jump_ns: i64 = 3 * 60 * 60_000_000_000;

    let mut events = Vec::with_capacity(total);
    for entity_idx in 0..entity_count {
        let entity = EntityId::String(format!("user_{entity_idx:06}"));
        let count = if entity_idx < entity_count - 1 {
            events_per_entity
        } else {
            total - events_per_entity * (entity_count - 1)
        };
        let mut ts_cursor: i64 = base_ns;
        for ev_idx in 0..count {
            ts_cursor += step_ns;
            if ev_idx > 0 && ev_idx % 5 == 0 {
                ts_cursor += gap_jump_ns;
            }
            let properties = vec![("amount".into(), PropertyValue::Int((ev_idx as i64) % 64))];
            events.push(Event::with_properties(
                entity.clone(),
                Timestamp::from_nanos(ts_cursor),
                "view".to_string(),
                properties,
            ));
        }
    }
    events
}

/// Build a single-shard scratch database and ingest the fusion
/// fixture. The `ScratchDir` cleans up on drop.
fn setup_db(sizing: &FusionSizing) -> (ScratchDir, Database, Engine) {
    let engine = Engine::new();
    let scratch = ScratchDir::new("wave5-fusion");
    let mut db =
        Database::create_with_shards(scratch.path(), 1).expect("Database::create_with_shards");
    engine.query(CREATE_EVENTS, &mut db).expect("CREATE TABLE");

    let events = generate_fusion_events(sizing.total_events, sizing.entity_count);
    let batch_id = db.allocate_batch_id("events").expect("allocate batch_id");
    let shard_count = db.manifest().shard_count;
    let budget = (sizing.total_events * 200).max(256 * 1024 * 1024);
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

/// Run a query end-to-end and return wall-clock nanos plus the
/// total row count produced. Used as a probe outside the timed
/// loop — Criterion times its own closure, so the per-iteration
/// path inlines `engine.query` directly to avoid an extra clock
/// read inside the measurement.
fn run_query(engine: &Engine, db: &mut Database, sql: &str) -> (u128, usize) {
    let start = Instant::now();
    let result = engine.query(sql, db).expect("query");
    let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
    (start.elapsed().as_nanos(), total)
}

// ─────────────────────────────────────────────────────────────────────────────
// Benches
// ─────────────────────────────────────────────────────────────────────────────

fn bench_sessionize_aggregate_fusion(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = FusionSizing::for_mode(mode);
    let (_scratch, mut db, engine) = setup_db(&sizing);

    // Probe both queries once outside the timed loop. The probes
    // pin the row counts the bench expects and feed the [floor]
    // target calibration in `BenchResultCollector`.
    let (fused_probe_ns, fused_rows) = run_query(&engine, &mut db, FUSED_QUERY);
    let (unfused_probe_ns, unfused_rows) = run_query(&engine, &mut db, UNFUSED_QUERY);
    assert!(
        fused_rows > 0,
        "fused query produced zero rows; fixture is empty?"
    );
    assert!(
        unfused_rows > 0,
        "unfused query produced zero rows; fixture is empty?"
    );
    // Both queries group by `entity_id`, so each emits exactly
    // `entity_count` rows. A divergence here would mean the
    // baselines aren't comparable.
    assert_eq!(
        fused_rows, unfused_rows,
        "fused and unfused queries must emit the same group cardinality"
    );

    let mut group = c.benchmark_group("wave5/stateful_aggregate_fusion");
    group.throughput(Throughput::Elements(sizing.total_events as u64));

    group.bench_function("sessionize_to_count_max/fused", |b| {
        b.iter(|| {
            // Skip `run_query`'s `Instant::now()` — Criterion times
            // this closure end-to-end, the extra clock read inside is
            // pure overhead.
            let result = engine.query(FUSED_QUERY, &mut db).expect("fused query");
            let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
            black_box(total)
        });
    });
    group.bench_function("sessionize_to_sum_amount/unfused", |b| {
        b.iter(|| {
            let result = engine.query(UNFUSED_QUERY, &mut db).expect("unfused query");
            let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
            black_box(total)
        });
    });
    group.finish();

    // Record per-bench wall-clock plus the fusion savings ratio
    // for the regression gate. Probes are single-iteration estimates
    // — Criterion's per-sample timing remains the primary signal;
    // these rows surface the design-doc invariant in JSON.
    let mut collector = BenchResultCollector::new(mode);
    collector.record(
        "wave5/stateful_aggregate_fusion/fused/probe_ns",
        fused_probe_ns as f64,
        "ns",
        None,
    );
    collector.record(
        "wave5/stateful_aggregate_fusion/unfused/probe_ns",
        unfused_probe_ns as f64,
        "ns",
        None,
    );
    let fusion_speedup = unfused_probe_ns as f64 / fused_probe_ns.max(1) as f64;
    collector.record(
        "wave5/stateful_aggregate_fusion/fusion_speedup_ratio",
        fusion_speedup,
        "ratio",
        Some(BenchTarget::at_least(0.95)),
    );
    collector.finish();
}

criterion_group! {
    name = stateful_aggregate_fusion_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(3));
    targets = bench_sessionize_aggregate_fusion,
}
criterion_main!(stateful_aggregate_fusion_benches);
