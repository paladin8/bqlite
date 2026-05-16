//! Concurrent-query throughput (TASK-546).
//!
//! Measures aggregate throughput when 1, 4, or 16 client threads
//! submit queries against the same `Database`. Because
//! `Engine::query` requires `&mut Database` and `Database::open`
//! acquires an exclusive per-directory file lock, concurrent
//! submissions against a single fixture are necessarily serialized
//! through an `Arc<Mutex<Database>>`. The bench therefore measures
//! end-to-end *interleaved* throughput — the realistic shape for a
//! single bqlite process serving many client threads — rather than
//! parallel parallelism on independent databases.
//!
//! Each Engine spawned here shares a single morsel-scheduler-sized
//! worker pool sized to the platform's `available_parallelism()`.
//! Per-query parallelism is unaffected: within one query the engine
//! still spreads work across worker threads. The bench varies *only*
//! the count of concurrent submitters.
//!
//! Reported metrics:
//! - Aggregate `queries_per_sec` across all threads.
//! - Per-thread average latency (wall-clock / queries per thread).

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine};
use criterion::{black_box, Criterion, Throughput};

// NOTE: original sweep spec used `GROUP BY category` (a property
// column) — currently blocked by the same planner-pruning bug
// documented in the `aggregation_cardinality` bench. `event_type` is
// the role-column substitute that keeps a similar group cardinality
// (20 groups) without triggering the bug.
const QUERY: &str = "purchases | STATS n = COUNT(*) GROUP BY event_type";

struct ConcurrencyPoint {
    label: &'static str,
    threads: usize,
}

const POINTS: &[ConcurrencyPoint] = &[
    ConcurrencyPoint {
        label: "clients_1",
        threads: 1,
    },
    ConcurrencyPoint {
        label: "clients_4",
        threads: 4,
    },
    ConcurrencyPoint {
        label: "clients_16",
        threads: 16,
    },
];

/// Number of completed queries per client thread inside one bench
/// iteration. Larger values amortize thread-spawn overhead at the
/// cost of longer iterations; smaller values respond faster to
/// per-query latency regressions. 8 is a compromise that keeps each
/// iteration under a few seconds at Small scale.
const QUERIES_PER_CLIENT: usize = 8;

fn run_concurrent_round(
    engine: &Engine,
    db: &Arc<Mutex<Database>>,
    threads: usize,
) -> (Duration, usize) {
    let start = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let engine = engine.clone();
        let db = Arc::clone(db);
        handles.push(thread::spawn(move || {
            let mut local_rows = 0usize;
            for _ in 0..QUERIES_PER_CLIENT {
                let mut guard = db.lock().expect("db mutex not poisoned");
                let result = engine.query(QUERY, &mut guard).expect("concurrent query");
                local_rows += result.rows.iter().map(|b| b.num_rows()).sum::<usize>();
            }
            local_rows
        }));
    }
    let mut total_rows = 0usize;
    for handle in handles {
        total_rows += handle.join().expect("client thread join");
    }
    (start.elapsed(), total_rows)
}

fn bench_concurrent(
    c: &mut Criterion,
    engine: &Engine,
    db: Arc<Mutex<Database>>,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;

    let mut group = c.benchmark_group(format!("perf/concurrent_queries/{}", scale.label()));
    // Each iteration submits `threads * QUERIES_PER_CLIENT` queries;
    // the Criterion throughput axis is the queries completed.
    for point in POINTS {
        let queries_per_iter = (point.threads * QUERIES_PER_CLIENT) as u64;
        group.throughput(Throughput::Elements(queries_per_iter));

        // Probe pass for the report-side metrics.
        let (probe_elapsed, probe_rows) = run_concurrent_round(engine, &db, point.threads);
        let secs = probe_elapsed.as_secs_f64().max(1e-9);
        let queries_per_sec = queries_per_iter as f64 / secs;
        let per_thread_avg_ms = (secs / QUERIES_PER_CLIENT as f64) * 1000.0;
        eprintln!(
            "  [concurrent] {} threads={} probe={:.2}s queries/s={:.1} \
             per-thread-avg={:.1}ms rows={}",
            point.label,
            point.threads,
            secs,
            queries_per_sec,
            per_thread_avg_ms,
            probe_rows,
        );

        group.bench_function(point.label, |b| {
            b.iter(|| {
                let (_elapsed, rows) = run_concurrent_round(engine, &db, point.threads);
                black_box(rows)
            });
        });

        let base = format!("perf/concurrent_queries/{}/{}", scale.label(), point.label);
        collector.record(
            &format!("{base}/queries_per_sec"),
            queries_per_sec,
            "queries/s",
            None,
        );
        collector.record(
            &format!("{base}/per_thread_avg_ms"),
            per_thread_avg_ms,
            "ms",
            None,
        );
        // Total scan throughput across all clients on the probe pass
        // (each client scans the whole fixture per query).
        let scan_rows_per_sec =
            (queries_per_iter as f64) * (total_rows as f64) / secs;
        collector.record(
            &format!("{base}/aggregate_rows_per_sec"),
            scan_rows_per_sec,
            "rows/s",
            None,
        );
    }

    group.finish();
}

fn main() {
    let scale = BenchScale::from_env();
    let mode = BenchMode::from_env();
    let fixture = PersistentFixture::load_or_build(scale);
    let engine = Engine::new();
    let db = Arc::new(Mutex::new(fixture.open_db()));
    let mut collector = BenchResultCollector::new(mode);

    let mut criterion = criterion_for_scale(scale).configure_from_args();
    bench_concurrent(&mut criterion, &engine, db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
