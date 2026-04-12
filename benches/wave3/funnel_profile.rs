//! Standalone profiling harness for the funnel query (TASK-331).
//!
//! Runs the 3-step funnel query under pprof and writes flamegraph +
//! top-stacks output. Uses a medium-sized dataset (configurable via
//! `FUNNEL_EVENTS` env var, default 500k) for quick iteration.
//!
//! ```bash
//! RUSTFLAGS="-C force-frame-pointers=yes" \
//!     cargo run -p bqlite-benches --release --example funnel_profile
//! ```

use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::time::Timestamp;
use bqlite_engine::{Database, Engine};
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;

const FUNNEL_STEPS: [&str; 3] = ["signup", "activation", "purchase"];
const NOISE_TYPES: [&str; 5] = ["view", "click", "scroll", "hover", "search"];

const CREATE_EVENTS: &str = "\
    CREATE TABLE events (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE, \
        amount INT\
    )";

const FUNNEL_QUERY: &str = "\
    events | MATCH FIRST SEQUENCE(signup THEN activation THEN purchase) EMIT ALL";

fn generate_funnel_events(total: usize, entity_count: usize) -> Vec<Event> {
    let events_per_entity = total / entity_count.max(1);
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

fn main() {
    let total_events: usize = std::env::var("FUNNEL_EVENTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500_000);
    let entity_count = (total_events / 10_000).max(10);
    let iterations: usize = std::env::var("FUNNEL_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    eprintln!(
        "Funnel profile: {} events, {} entities, {} iterations",
        total_events, entity_count, iterations
    );

    // Phase 1: Setup (not profiled).
    let tmpdir = std::env::temp_dir().join(format!("funnel-profile-{}", std::process::id()));
    eprintln!("DB path: {}", tmpdir.display());
    let mut db = Database::create_with_shards(&tmpdir, 1).expect("create db");
    let engine = Engine::new();
    engine.query(CREATE_EVENTS, &mut db).expect("CREATE TABLE");

    let events = generate_funnel_events(total_events, entity_count);
    let ingest_start = Instant::now();
    let batch_id = db.allocate_batch_id("events").expect("allocate batch_id");
    let shard_count = db.manifest().shard_count;
    let budget = (total_events * 200).max(256 * 1024 * 1024);
    let mut partitioner = Partitioner::new(shard_count, 30, batch_id, budget).expect("partitioner");
    for event in events {
        partitioner.push_event(event).expect("push event");
    }
    let mut writer = SegmentWriter::new(&mut db);
    writer
        .write_partitioner("events", partitioner)
        .expect("write partitioner");
    eprintln!("Ingest: {:.2}s", ingest_start.elapsed().as_secs_f64());

    // Phase 2: Warm-up query.
    let warmup_start = Instant::now();
    let result = engine.query(FUNNEL_QUERY, &mut db).unwrap();
    let row_count: usize = result.rows.iter().map(|b| b.num_rows()).sum();
    eprintln!(
        "Warm-up query: {:.3}s, {} rows",
        warmup_start.elapsed().as_secs_f64(),
        row_count
    );

    // Phase 3: Profiled queries.
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(997)
        .build()
        .expect("pprof profiler");

    let query_start = Instant::now();
    for i in 0..iterations {
        let iter_start = Instant::now();
        let result = engine.query(FUNNEL_QUERY, &mut db).unwrap();
        let rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
        black_box(rows);
        eprintln!(
            "  iter {}: {:.3}s, {} rows",
            i,
            iter_start.elapsed().as_secs_f64(),
            rows
        );
    }
    let total_query_secs = query_start.elapsed().as_secs_f64();
    let per_query_secs = total_query_secs / iterations as f64;
    let throughput = (total_events as f64) / per_query_secs / 1_000_000.0;
    eprintln!(
        "Query avg: {:.3}s ({:.2} Mev/s)",
        per_query_secs, throughput
    );

    // Phase 4: Write pprof artifacts.
    match guard.report().build() {
        Ok(report) => {
            let stacks = report.data.len();
            eprintln!("pprof: {stacks} unique stacks");

            let target_dir = PathBuf::from("target");
            let _ = std::fs::create_dir_all(&target_dir);

            // Flamegraph SVG.
            let svg_path = target_dir.join("funnel-flamegraph.svg");
            if let Ok(mut file) = std::fs::File::create(&svg_path) {
                if let Err(e) = report.flamegraph(&mut file) {
                    eprintln!("Flamegraph error: {e}");
                } else {
                    eprintln!("Flamegraph: {}", svg_path.display());
                }
            }

            // Top stacks text.
            let txt_path = target_dir.join("funnel-pprof-stacks.txt");
            let mut entries: Vec<_> = report.data.iter().collect();
            entries.sort_by(|a, b| b.1.cmp(a.1));
            let mut lines = Vec::new();
            for (key, count) in entries.iter().take(30) {
                let frames: Vec<String> = key
                    .frames
                    .iter()
                    .flat_map(|syms| syms.iter().map(|s| format!("{s}")))
                    .collect();
                lines.push(format!("samples={count}:"));
                for frame in frames.iter().rev().take(15) {
                    lines.push(format!("  {frame}"));
                }
                lines.push(String::new());
            }
            if let Ok(mut file) = std::fs::File::create(&txt_path) {
                use std::io::Write;
                let _ = file.write_all(lines.join("\n").as_bytes());
                eprintln!("Top stacks: {}", txt_path.display());
            }
        }
        Err(e) => eprintln!("pprof report error: {e}"),
    }

    // Cleanup.
    let _ = std::fs::remove_dir_all(&tmpdir);
}
