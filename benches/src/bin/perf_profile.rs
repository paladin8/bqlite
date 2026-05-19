//! Per-core profiling driver for headline benchmark queries.
//!
//! Runs a fixed set of queries against the persistent `small` fixture
//! (1M rows) at both 1-thread and auto-thread configurations, then
//! prints per-query throughput plus the relevant `QueryMetrics`
//! signals. Used to pinpoint which stage of the engine pipeline is
//! holding back per-core throughput.
//!
//! Run:
//!     cargo run -p bqlite-benches --release --bin perf_profile
//!
//! Pair with samply:
//!     samply record -- target/release/perf_profile

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine, EngineConfig};

struct ProfileQuery {
    label: &'static str,
    sql: &'static str,
}

const QUERIES: &[ProfileQuery] = &[
    ProfileQuery {
        label: "scan_only",
        sql: "purchases",
    },
    ProfileQuery {
        label: "scan_filter_1pct",
        sql: "purchases | where amount <= 50",
    },
    ProfileQuery {
        label: "scan_filter_50pct",
        sql: "purchases | where amount <= 2500",
    },
    ProfileQuery {
        label: "scan_filter_project",
        sql: "purchases | where amount <= 2500 | select user_id, amount",
    },
    ProfileQuery {
        label: "agg_lowcard",
        sql: "purchases | STATS n = COUNT(*) GROUP BY category",
    },
    ProfileQuery {
        label: "agg_composite",
        sql: "purchases | STATS n = COUNT(*) GROUP BY category, region",
    },
    ProfileQuery {
        label: "filter_agg_1pct",
        sql: "purchases | where amount <= 50 | STATS n = COUNT(*) GROUP BY category",
    },
    ProfileQuery {
        label: "filter_agg_50pct",
        sql: "purchases | where amount <= 2500 | STATS n = COUNT(*) GROUP BY category",
    },
];

fn run_one(engine: &Engine, db: &mut Database, sql: &str, iters: usize) -> RunStats {
    // Warm-up: prime the page cache and any per-query lazy state.
    let _ = engine.query(sql, db).expect("warmup");

    let start = Instant::now();
    let mut last = None;
    for _ in 0..iters {
        last = Some(engine.query(sql, db).expect("query"));
    }
    let elapsed = start.elapsed();
    let last = last.expect("at least one iter");
    let rows_out: usize = last.rows.iter().map(|b| b.num_rows()).sum();
    RunStats {
        elapsed_ns: elapsed.as_nanos() as u64 / iters as u64,
        rows_out,
        metrics: last.metrics,
    }
}

struct RunStats {
    elapsed_ns: u64,
    rows_out: usize,
    metrics: bqlite_engine::QueryMetrics,
}

fn print_row(label: &str, threads_label: &str, total_rows: u64, stats: &RunStats) {
    let secs = stats.elapsed_ns as f64 / 1e9;
    let rows_per_sec = total_rows as f64 / secs;
    let m = &stats.metrics;
    let bytes_scanned = m.operator.bytes_scanned;
    let bytes_decompressed = m.operator.bytes_decompressed;
    let bytes_mat_pre = m.operator.bytes_materialized_before_filter;
    let bytes_mat_post = m.operator.bytes_materialized_after_filter;
    let gb_per_sec_in = (bytes_scanned as f64 / secs) / (1u64 << 30) as f64;
    let workers = m.num_workers.max(1) as f64;
    let rows_per_sec_per_core = rows_per_sec / workers;
    let ns_per_row_per_core = (stats.elapsed_ns as f64 * workers) / total_rows.max(1) as f64;
    let busy_min_ms = m.worker_busy_ns_min as f64 / 1e6;
    let busy_max_ms = m.worker_busy_ns_max as f64 / 1e6;
    let idle_p99_ms = m.worker_idle_ns_p99 as f64 / 1e6;
    let mat_pre_ratio = if bytes_scanned == 0 {
        0.0
    } else {
        bytes_mat_pre as f64 / bytes_scanned as f64
    };

    eprintln!(
        "  {label:24} {threads_label:>6} | {:.2}ms | {:>7.2} M rows/s | {:>5.2} M r/s/core | {:>5.2} ns/row/core | {:>4.2} GB/s in | mat_pre/scan={:.2} | rows_out={}",
        secs * 1000.0,
        rows_per_sec / 1e6,
        rows_per_sec_per_core / 1e6,
        ns_per_row_per_core,
        gb_per_sec_in,
        mat_pre_ratio,
        stats.rows_out,
    );
    eprintln!(
        "  {empty:24} {threads_label:>6} | workers={} busy_min={:.1}ms busy_max={:.1}ms idle_p99={:.1}ms morsels={} scanned={} decompr={} mat_pre={} mat_post={} sel_drops={}",
        m.num_workers,
        busy_min_ms,
        busy_max_ms,
        idle_p99_ms,
        m.morsels_dispatched,
        bytes_scanned,
        bytes_decompressed,
        bytes_mat_pre,
        bytes_mat_post,
        m.operator.selection_vector_dropped_rows,
        empty = "",
    );
}

fn main() {
    let scale = BenchScale::from_env();
    let fixture = PersistentFixture::load_or_build(scale);
    let total_rows = fixture.manifest.rows;
    let bytes_logical = fixture.manifest.bytes_logical;
    eprintln!(
        "fixture: scale={} rows={} bytes_logical={} ({:.2} GiB)",
        scale.label(),
        total_rows,
        bytes_logical,
        bytes_logical as f64 / (1u64 << 30) as f64,
    );

    let iters_env = std::env::var("BQLITE_PROFILE_ITERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5);

    // Allow profiling one specific query for samply runs:
    //   BQLITE_PROFILE_ONLY=agg_composite samply record ./target/release/perf_profile
    let only = std::env::var("BQLITE_PROFILE_ONLY").ok();
    let threads_only = std::env::var("BQLITE_PROFILE_THREADS").ok();

    let engine_1t = Engine::with_config(EngineConfig {
        query_threads: Some(1),
        ..EngineConfig::default()
    });
    let engine_auto = Engine::new();
    let auto_threads = engine_auto.config().resolve_query_threads();
    eprintln!(
        "engine: 1-thread + auto={} (M2 Max → expect 8P+4E)\n",
        auto_threads,
    );

    let mut db = fixture.open_db();

    for q in QUERIES {
        if let Some(filter) = &only {
            if q.label != filter {
                continue;
            }
        }
        eprintln!("Query: {} -- {}", q.label, q.sql);
        if threads_only.as_deref() != Some("auto") {
            let s1 = run_one(&engine_1t, &mut db, q.sql, iters_env);
            print_row(q.label, "1t", total_rows, &s1);
        }
        if threads_only.as_deref() != Some("1") {
            let sa = run_one(&engine_auto, &mut db, q.sql, iters_env);
            print_row(q.label, "auto", total_rows, &sa);
        }
        eprintln!();
    }
}
