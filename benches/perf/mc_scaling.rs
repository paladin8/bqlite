//! Multi-core scaling curve (TASK-546).
//!
//! Drives the same scan/aggregation query against the persistent
//! `purchases` fixture with `query_threads` set to 1, 2, 4, 8, and
//! `auto` (let the engine read `available_parallelism()`). Per
//! `docs/design/perf-suite.md` §3.4 the bench reports the per-worker
//! speedup curve so the report generator can render the multi-core
//! scaling chart.
//!
//! Each worker count uses a *separate* `Engine` instance because
//! `EngineConfig::query_threads` is baked into the morsel scheduler
//! at construction time. The same `Database` handle is reused across
//! engines — `Database` is `&mut`-borrowed per query, not owned by
//! any one engine.
//!
//! The workload is intentionally CPU-bound rather than I/O-bound: an
//! aggregation query that fully consumes the fixture and produces a
//! small number of groups. A `LIMIT 1` scan would saturate at very
//! few cores because the scheduler can short-circuit; a full
//! aggregation forces every worker to scan its share.

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine, EngineConfig};
use criterion::{black_box, Criterion, Throughput};

// NOTE: the design-doc sweep originally specified `GROUP BY category,
// region`, which exercises the multi-key composite path. That query
// currently surfaces a planner-pruning bug (column-resolve mismatch
// when STATS references property columns); see the
// `aggregation_cardinality` bench docstring for the full trace.
// `GROUP BY user_id, event_type` is the closest workload still
// supported — composite key, 20 × entity_count groups, but uses only
// role columns and avoids the pruning bug.
const QUERY: &str = "purchases | STATS n = COUNT(*) GROUP BY user_id, event_type";

struct ThreadPoint {
    label: &'static str,
    query_threads: Option<usize>,
}

const POINTS: &[ThreadPoint] = &[
    ThreadPoint {
        label: "threads_1",
        query_threads: Some(1),
    },
    ThreadPoint {
        label: "threads_2",
        query_threads: Some(2),
    },
    ThreadPoint {
        label: "threads_4",
        query_threads: Some(4),
    },
    ThreadPoint {
        label: "threads_8",
        query_threads: Some(8),
    },
    ThreadPoint {
        label: "threads_auto",
        query_threads: None,
    },
];

fn engine_with_threads(threads: Option<usize>) -> Engine {
    let cfg = EngineConfig {
        query_threads: threads,
        ..EngineConfig::default()
    };
    Engine::with_config(cfg)
}

fn bench_mc_scaling(
    c: &mut Criterion,
    db: &mut Database,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;
    let bytes_logical = fixture.manifest.bytes_logical;

    let mut group = c.benchmark_group(format!("perf/mc_scaling/{}", scale.label()));
    group.throughput(Throughput::Elements(total_rows));

    // Probe at 1 thread for the speedup denominator. Recorded in the
    // collector so the report can render absolute throughput too.
    let baseline_engine = engine_with_threads(Some(1));
    let baseline_start = Instant::now();
    let baseline_result = baseline_engine
        .query(QUERY, db)
        .expect("mc_scaling baseline probe");
    let baseline_elapsed = baseline_start.elapsed();
    let baseline_rows: usize = baseline_result.rows.iter().map(|b| b.num_rows()).sum();
    let baseline_secs = baseline_elapsed.as_secs_f64().max(1e-9);
    eprintln!(
        "  [mc_scaling] baseline threads=1 rows={} elapsed={:.1}ms",
        baseline_rows,
        baseline_secs * 1000.0,
    );

    for point in POINTS {
        let engine = engine_with_threads(point.query_threads);
        let resolved = engine.config().resolve_query_threads();
        let id = format!("{}/{}", point.label, resolved);

        // Per-point probe for the report's per-thread throughput.
        let probe_start = Instant::now();
        let probe_result = engine
            .query(QUERY, db)
            .unwrap_or_else(|e| panic!("mc_scaling probe {}: {e}", point.label));
        let probe_elapsed = probe_start.elapsed();
        let probe_rows: usize = probe_result.rows.iter().map(|b| b.num_rows()).sum();
        let probe_secs = probe_elapsed.as_secs_f64().max(1e-9);
        let speedup = baseline_secs / probe_secs;
        eprintln!(
            "  [mc_scaling] {} resolved={} rows={} elapsed={:.1}ms speedup={:.2}x",
            point.label,
            resolved,
            probe_rows,
            probe_secs * 1000.0,
            speedup,
        );

        group.bench_function(&id, |b| {
            b.iter(|| {
                let result = engine.query(QUERY, db).expect("mc_scaling query");
                let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
                black_box(total)
            });
        });

        let rows_per_sec = total_rows as f64 / probe_secs;
        let gb_per_sec = (bytes_logical as f64 / probe_secs) / (1u64 << 30) as f64;
        let base = format!("perf/mc_scaling/{}/{}", scale.label(), point.label);
        collector.record(
            &format!("{base}/resolved_threads"),
            resolved as f64,
            "threads",
            None,
        );
        collector.record(
            &format!("{base}/rows_per_sec"),
            rows_per_sec,
            "rows/s",
            None,
        );
        collector.record(&format!("{base}/gb_per_sec"), gb_per_sec, "GB/s", None);
        collector.record(&format!("{base}/speedup_vs_1"), speedup, "ratio", None);
    }

    group.finish();
}

fn main() {
    let scale = BenchScale::from_env();
    let mode = BenchMode::from_env();
    let fixture = PersistentFixture::load_or_build(scale);
    let mut db = fixture.open_db();
    let mut collector = BenchResultCollector::new(mode);

    let mut criterion = criterion_for_scale(scale).configure_from_args();
    bench_mc_scaling(&mut criterion, &mut db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
