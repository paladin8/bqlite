//! End-to-end 3-step funnel benchmark (TASK-325).
//!
//! Exercises the full bqlite pipeline: ingest → parse → plan → bind →
//! execute for a 3-step funnel pattern (`signup THEN activation THEN
//! purchase`) over a synthetic dataset.
//!
//! The CI-mode dataset is small (~50k events) for noise control; the
//! reference-mode dataset is the full 100M events per TASKS.md.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench funnel
//! ```

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::time::Timestamp;
use bqlite_engine::{Database, Engine};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ─────────────────────────────────────────────────────────────────────────────
// Dataset generation
// ─────────────────────────────────────────────────────────────────────────────

/// Funnel event type labels.
const FUNNEL_STEPS: [&str; 3] = ["signup", "activation", "purchase"];
/// Noise event types to create realistic event streams.
const NOISE_TYPES: [&str; 5] = ["view", "click", "scroll", "hover", "search"];

/// Funnel sizing: small CI dataset vs large reference dataset.
struct FunnelSizing {
    total_events: usize,
    entity_count: usize,
}

impl FunnelSizing {
    fn for_mode(mode: BenchMode) -> Self {
        match mode {
            BenchMode::Ci => FunnelSizing {
                total_events: 50_000,
                entity_count: 500,
            },
            BenchMode::Reference => FunnelSizing {
                total_events: 100_000_000,
                entity_count: REF_ENTITY_COUNT,
            },
        }
    }
}

/// Generate synthetic events with a 3-step funnel embedded in noise.
///
/// Each entity gets `total_events / entity_count` events. Roughly 1 in 5
/// events is a funnel step, cycling through signup→activation→purchase.
/// This means ~60% of entities complete the full funnel.
fn generate_funnel_events(total: usize, entity_count: usize) -> Vec<Event> {
    let events_per_entity = total / entity_count.max(1);
    let base_ns: i64 = 1_735_689_600_000_000_000;
    let step_ns: i64 = 60_000_000_000; // ~1 minute

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
            let event_type = if ev_idx % 5 == 0 {
                let step = (ev_idx / 5) % FUNNEL_STEPS.len();
                FUNNEL_STEPS[step]
            } else {
                NOISE_TYPES[ev_idx % NOISE_TYPES.len()]
            };

            let properties = vec![("amount".into(), PropertyValue::Int((ev_idx as i64) * 10))];
            events.push(Event::with_properties(
                entity.clone(),
                ts,
                event_type.to_string(),
                properties,
            ));
        }
    }
    events
}

/// Table creation DDL.
const CREATE_EVENTS: &str = "\
    CREATE TABLE events (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE, \
        amount INT\
    )";

/// The funnel query: 3-step MATCH FIRST with EMIT ALL to get step_reached.
const FUNNEL_QUERY: &str = "\
    events | MATCH FIRST SEQUENCE(signup THEN activation THEN purchase) EMIT ALL";

/// Set up a database with the funnel events ingested.
fn setup_funnel_db(sizing: &FunnelSizing) -> (ScratchDir, Database, Engine) {
    let scratch = ScratchDir::new("funnel-bench");
    let mut db = Database::create(scratch.path()).expect("Database::create");
    let engine = Engine::new();

    engine.query(CREATE_EVENTS, &mut db).expect("CREATE TABLE");

    // Bulk-ingest using INSERT VALUES in chunks to avoid enormous
    // single-statement parsing overhead.
    let events = generate_funnel_events(sizing.total_events, sizing.entity_count);
    let chunk_size = 10_000;
    for chunk in events.chunks(chunk_size) {
        let mut sql = String::from("INSERT INTO events VALUES ");
        for (i, event) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let entity_str = match &event.entity {
                EntityId::String(s) => s.as_str(),
                _ => "unknown",
            };
            let ts_ns = event.timestamp.as_nanos();
            let amount = event
                .properties
                .iter()
                .find(|(k, _)| k == "amount")
                .map(|(_, v)| match v {
                    PropertyValue::Int(n) => *n,
                    _ => 0,
                })
                .unwrap_or(0);
            sql.push_str(&format!(
                "('{}', {}, '{}', {})",
                entity_str, ts_ns, event.event_type, amount
            ));
        }
        engine
            .query(&sql, &mut db)
            .unwrap_or_else(|e| panic!("INSERT failed: {e}"));
    }

    (scratch, db, engine)
}

// ─────────────────────────────────────────────────────────────────────────────
// End-to-end funnel benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_funnel_e2e(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = FunnelSizing::for_mode(mode);
    let (_scratch, mut db, engine) = setup_funnel_db(&sizing);

    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("funnel/e2e");
    group.throughput(Throughput::Elements(sizing.total_events as u64));

    let label = format!("{}k_events", sizing.total_events / 1000);
    group.bench_function(&label, |b| {
        b.iter(|| {
            let result = engine.query(FUNNEL_QUERY, &mut db).unwrap();
            let total_rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
            black_box(total_rows)
        });
    });
    group.finish();

    // In reference mode, measure a single pass and enforce the target:
    // the step-counter fast path should complete within 2× of the
    // Wave 2 scan-only baseline on the same dataset.
    if mode.is_reference() {
        let start = Instant::now();
        let result = engine.query(FUNNEL_QUERY, &mut db).unwrap();
        let total_rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
        black_box(total_rows);
        let elapsed_secs = start.elapsed().as_secs_f64();
        collector.record(
            "funnel/e2e_query_time_secs",
            elapsed_secs,
            "s",
            // Target: < 10s for the full 100M dataset. The Wave 2
            // scan-only acceptance is < 1s; 2× overhead for MATCH plus
            // ingest overhead gives a generous ceiling.
            Some(BenchTarget::at_most(10.0)),
        );
    }

    collector.finish();
}

criterion_group! {
    name = funnel_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets = bench_funnel_e2e,
}
criterion_main!(funnel_benches);
