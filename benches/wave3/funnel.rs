//! End-to-end 3-step funnel benchmark (TASK-325, TASK-331).
//!
//! Exercises the full bqlite pipeline: ingest -> parse -> plan -> bind ->
//! execute for a 3-step funnel pattern (`signup THEN activation THEN
//! purchase`) over a synthetic dataset.
//!
//! The CI-mode dataset is small (~50k events) for noise control; the
//! reference-mode dataset is the full 100M events per TASKS.md.
//!
//! ## pprof integration (TASK-331)
//!
//! When `BQLITE_BENCH_PPROF=1` is set, the reference-mode single-pass
//! measurement wraps the query execution in a `pprof` profiler guard
//! and writes a flamegraph SVG to `target/funnel-flamegraph.svg`. This
//! allows profiling the query execution path in isolation, with ingest
//! and fixture setup excluded from the timed region.
//!
//! ## Warm fixture support (TASK-331)
//!
//! When `BQLITE_BENCH_DB` is set to a directory path, the benchmark
//! attempts to open an existing database there instead of generating
//! and ingesting a fresh dataset. If the directory does not exist or
//! does not contain a valid database, a new one is created and
//! populated at that path (without the `ScratchDir` cleanup). This
//! allows reusing a prepared database across profiling runs to focus
//! measurement on query execution.
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench funnel
//! ```

use std::path::PathBuf;
use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::time::Timestamp;
use bqlite_engine::{Database, Engine};
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// ---------------------------------------------------------------------------
// Dataset generation
// ---------------------------------------------------------------------------

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
/// events is a funnel step, cycling through signup->activation->purchase.
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

// ---------------------------------------------------------------------------
// Database setup with warm-fixture support (TASK-331)
// ---------------------------------------------------------------------------

/// Prepared database handle. When backed by a `ScratchDir`, the directory
/// is cleaned up on drop. When backed by a persistent path (warm fixture),
/// the directory survives.
#[allow(dead_code)] // ScratchDir field held for RAII cleanup, not read directly.
enum DbHandle {
    Scratch(ScratchDir, Database),
    Persistent(Database),
}

impl DbHandle {
    fn db_mut(&mut self) -> &mut Database {
        match self {
            DbHandle::Scratch(_, db) => db,
            DbHandle::Persistent(db) => db,
        }
    }
}

/// Set up a database with the funnel events ingested.
///
/// If `BQLITE_BENCH_DB` is set, attempts to reuse a database at that path.
/// Otherwise creates a temporary `ScratchDir`-backed database.
fn setup_funnel_db(sizing: &FunnelSizing) -> (DbHandle, Engine) {
    let engine = Engine::new();

    // Check for a warm fixture path.
    if let Ok(db_path) = std::env::var("BQLITE_BENCH_DB") {
        let path = PathBuf::from(&db_path);
        if path.exists() {
            if let Ok(db) = Database::open(&path) {
                eprintln!("[funnel] Reusing warm fixture at {db_path}");
                return (DbHandle::Persistent(db), engine);
            }
        }
        // Create and populate at the requested path. Use 1 shard for
        // query-focused benchmarking.
        eprintln!("[funnel] Creating warm fixture at {db_path}");
        let mut db = Database::create_with_shards(&path, 1).expect("Database::create_with_shards");
        populate_db(&mut db, &engine, sizing);
        return (DbHandle::Persistent(db), engine);
    }

    // Default: scratch directory. Use 1 shard to minimize k-way merge
    // overhead — the funnel benchmark measures query execution, not
    // shard-parallel scan.
    let scratch = ScratchDir::new("funnel-bench");
    let mut db =
        Database::create_with_shards(scratch.path(), 1).expect("Database::create_with_shards");
    populate_db(&mut db, &engine, sizing);
    (DbHandle::Scratch(scratch, db), engine)
}

/// Populate a database with the funnel event dataset.
///
/// Uses the direct storage API (`Partitioner` + `SegmentWriter`) to
/// ingest all events in a single batch, producing the minimum number
/// of segments. This avoids the per-INSERT-chunk segment proliferation
/// that artificially inflates k-way merge overhead at query time.
fn populate_db(db: &mut Database, engine: &Engine, sizing: &FunnelSizing) {
    engine.query(CREATE_EVENTS, db).expect("CREATE TABLE");

    let events = generate_funnel_events(sizing.total_events, sizing.entity_count);
    let batch_id = db.allocate_batch_id("events").expect("allocate batch_id");
    let shard_count = db.manifest().shard_count;
    // Budget large enough for the full dataset in a single batch. At ~200
    // bytes/event (estimated_event_size overhead), 100M events require ~20 GB.
    // The `.max()` sets a floor of 256 MiB for small CI datasets. Reference
    // mode intentionally requires large memory to avoid multi-batch ingest.
    let budget = (sizing.total_events * 200).max(256 * 1024 * 1024);
    let mut partitioner =
        Partitioner::new(shard_count, 30, batch_id, budget).expect("create partitioner");

    for event in events {
        partitioner.push_event(event).expect("push event");
    }

    let mut writer = SegmentWriter::new(db);
    writer
        .write_partitioner("events", partitioner)
        .expect("write partitioner");
}

// ---------------------------------------------------------------------------
// End-to-end funnel benchmark
// ---------------------------------------------------------------------------

fn bench_funnel_e2e(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = FunnelSizing::for_mode(mode);
    let (mut handle, engine) = setup_funnel_db(&sizing);

    let mut collector = BenchResultCollector::new(mode);

    let mut group = c.benchmark_group("funnel/e2e");
    group.throughput(Throughput::Elements(sizing.total_events as u64));

    let label = format!("{}k_events", sizing.total_events / 1000);
    let db = handle.db_mut();
    group.bench_function(&label, |b| {
        b.iter(|| {
            let result = engine.query(FUNNEL_QUERY, db).unwrap();
            let total_rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
            black_box(total_rows)
        });
    });
    group.finish();

    // In reference mode, measure a single pass and enforce the target.
    // Also optionally capture a pprof flamegraph.
    if mode.is_reference() {
        let pprof_enabled = std::env::var("BQLITE_BENCH_PPROF")
            .map(|v| v == "1")
            .unwrap_or(false);

        let guard = if pprof_enabled {
            Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(997)
                    .build()
                    .expect("pprof profiler guard"),
            )
        } else {
            None
        };

        let start = Instant::now();
        let result = engine.query(FUNNEL_QUERY, handle.db_mut()).unwrap();
        let total_rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
        black_box(total_rows);
        let elapsed_secs = start.elapsed().as_secs_f64();

        // Write pprof flamegraph if profiling was enabled.
        if let Some(guard) = guard {
            write_pprof_artifacts(guard);
        }

        collector.record(
            "funnel/e2e_query_time_secs",
            elapsed_secs,
            "s",
            // Target: < 10s for the full 100M dataset. The Wave 2
            // scan-only acceptance is < 1s; 2x overhead for MATCH plus
            // ingest overhead gives a generous ceiling.
            Some(BenchTarget::at_most(10.0)),
        );
        collector.record(
            "funnel/e2e_throughput_mevents_s",
            (sizing.total_events as f64) / elapsed_secs / 1_000_000.0,
            "Mev/s",
            // Inverse of the 10s ceiling: >= 10 Mev/s.
            Some(BenchTarget::at_least(10.0)),
        );
    }

    collector.finish();
}

/// Write pprof flamegraph SVG and top-stacks text report.
fn write_pprof_artifacts(guard: pprof::ProfilerGuard) {
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .or_else(|_| {
            std::env::var("CARGO_MANIFEST_DIR").map(|m| {
                PathBuf::from(m)
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join("target")
            })
        })
        .unwrap_or_else(|_| PathBuf::from("target"));

    match guard.report().build() {
        Ok(report) => {
            let stacks = report.data.len();
            eprintln!("[pprof] Collected {stacks} unique stack samples");

            // Write flamegraph SVG.
            let svg_path = target_dir.join("funnel-flamegraph.svg");
            if let Err(e) = std::fs::create_dir_all(&target_dir) {
                eprintln!("[pprof] Cannot create target dir: {e}");
                return;
            }
            match std::fs::File::create(&svg_path) {
                Ok(mut file) => {
                    if let Err(e) = report.flamegraph(&mut file) {
                        eprintln!("[pprof] Failed to write flamegraph: {e}");
                    } else {
                        eprintln!("[pprof] Flamegraph written to {}", svg_path.display());
                    }
                }
                Err(e) => eprintln!("[pprof] Cannot create {}: {e}", svg_path.display()),
            }

            // Write top-stacks text report.
            let txt_path = target_dir.join("funnel-pprof-stacks.txt");
            let mut lines = Vec::new();
            let mut entries: Vec<_> = report.data.iter().collect();
            entries.sort_by(|a, b| b.1.cmp(a.1));
            for (key, count) in entries.iter().take(30) {
                let frames: Vec<String> = key
                    .frames
                    .iter()
                    .flat_map(|syms| syms.iter().map(|s| s.to_string()))
                    .collect();
                lines.push(format!("samples={count}:"));
                for frame in frames.iter().take(10) {
                    lines.push(format!("  {frame}"));
                }
                lines.push(String::new());
            }
            match std::fs::File::create(&txt_path) {
                Ok(mut file) => {
                    use std::io::Write;
                    if let Err(e) = file.write_all(lines.join("\n").as_bytes()) {
                        eprintln!("[pprof] Failed to write stacks: {e}");
                    } else {
                        eprintln!("[pprof] Top stacks written to {}", txt_path.display());
                    }
                }
                Err(e) => eprintln!("[pprof] Cannot create {}: {e}", txt_path.display()),
            }
        }
        Err(e) => eprintln!("[pprof] Report build failed: {e}"),
    }
}

criterion_group! {
    name = funnel_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets = bench_funnel_e2e,
}
criterion_main!(funnel_benches);
