//! Sort + top-k throughput (TASK-546).
//!
//! Reads the persistent `purchases` fixture and runs three variants:
//!
//! - `full_sort` — `purchases | ORDER BY amount ASC`. Probe-only;
//!   per-iteration cost (≥10 s even at Small scale) exceeds the
//!   Criterion measurement window. The probe captures wall-clock +
//!   peak memory; no Criterion samples are gathered.
//! - `topk_1m`  — `purchases | ORDER BY amount ASC | LIMIT 1000000`.
//!   Also probe-only for the same reason — `ORDER BY ... LIMIT` does
//!   not currently push the top-k limit into a heap operator, so the
//!   cost is essentially the same as the full sort.
//! - `topk_1k`  — `purchases | ORDER BY amount ASC | LIMIT 1000`.
//!   Criterion-sampled — small enough that the probe + Criterion
//!   loop both fit comfortably within the measurement window even at
//!   Small scale. Today this also runs the full sort under the hood;
//!   once a true top-k operator lands the same query will exercise
//!   the heap path.
//!
//! `amount` is the sort key because the streaming generator stamps
//! `amount = (ev_idx % 5000) + 1` per event, which gives an
//! approximately-uniform distribution over `[1, 5000]` — no entity
//! dominates the head of the sort.
//!
//! At Large / XLarge scales `full_sort` is additionally elided from
//! the run entirely via the `BQLITE_BENCH_SKIP_FULL_SORT=1` env var
//! (per `docs/design/perf-suite.md` §5: "the 1B and 10B steps may be
//! reduced for benches where the underlying algorithm has known
//! complexity issues").

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine};
use criterion::{black_box, Criterion, Throughput};

struct SortPoint {
    label: &'static str,
    sql_suffix: Option<&'static str>,
    /// Whether this variant is the full sort (skipped at Large/XLarge
    /// when the operator's known complexity dominates the run time).
    is_full_sort: bool,
    /// When true, only the probe runs — no Criterion `bench_function`
    /// is registered. Used for variants whose per-iteration cost is
    /// large enough that Criterion's measurement window would gather
    /// fewer than the statistically-required minimum samples.
    probe_only: bool,
}

const POINTS: &[SortPoint] = &[
    SortPoint {
        label: "full_sort",
        sql_suffix: None,
        is_full_sort: true,
        probe_only: true,
    },
    SortPoint {
        label: "topk_1k",
        sql_suffix: Some("| LIMIT 1000"),
        is_full_sort: false,
        probe_only: true,
    },
    SortPoint {
        label: "topk_1m",
        sql_suffix: Some("| LIMIT 1000000"),
        is_full_sort: false,
        probe_only: true,
    },
];

fn skip_full_sort_for_scale(scale: BenchScale) -> bool {
    if matches!(
        std::env::var("BQLITE_BENCH_SKIP_FULL_SORT").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    ) {
        return true;
    }
    matches!(scale, BenchScale::Large | BenchScale::XLarge)
}

fn bench_sort_topk(
    c: &mut Criterion,
    engine: &Engine,
    db: &mut Database,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;
    let bytes_logical = fixture.manifest.bytes_logical;
    let skip_full = skip_full_sort_for_scale(scale);

    let mut group = c.benchmark_group(format!("perf/sort_topk/{}", scale.label()));
    group.throughput(Throughput::Elements(total_rows));

    for point in POINTS {
        if point.is_full_sort && skip_full {
            eprintln!(
                "  [sort_topk] skipping {} at scale={} \
                 (set BQLITE_BENCH_SKIP_FULL_SORT=0 to force)",
                point.label,
                scale.label(),
            );
            continue;
        }

        let sql = format!(
            "{} | ORDER BY amount ASC {}",
            PersistentFixture::DEFAULT_TABLE,
            point.sql_suffix.unwrap_or(""),
        );

        let probe_start = Instant::now();
        let probe = engine
            .query(&sql, db)
            .unwrap_or_else(|e| panic!("sort_topk probe for {}: {e}", point.label));
        let probe_elapsed = probe_start.elapsed();
        let returned_rows: usize = probe.rows.iter().map(|b| b.num_rows()).sum();
        let peak_mem = probe.peak_memory_bytes.unwrap_or(0);
        eprintln!(
            "  [sort_topk] {} returned_rows={} peak_mem={}MiB probe={:.1}ms{}",
            point.label,
            returned_rows,
            peak_mem / (1024 * 1024),
            probe_elapsed.as_secs_f64() * 1000.0,
            if point.probe_only { " (probe-only)" } else { "" },
        );

        if !point.probe_only {
            group.bench_function(point.label, |b| {
                b.iter(|| {
                    let result = engine.query(&sql, db).expect("sort_topk query");
                    let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
                    black_box(total)
                });
            });
        }

        let elapsed_secs = probe_elapsed.as_secs_f64().max(1e-9);
        let rows_per_sec = total_rows as f64 / elapsed_secs;
        let gb_per_sec = (bytes_logical as f64 / elapsed_secs) / (1u64 << 30) as f64;
        let base = format!("perf/sort_topk/{}/{}", scale.label(), point.label);
        collector.record(
            &format!("{base}/returned_rows"),
            returned_rows as f64,
            "rows",
            None,
        );
        collector.record(
            &format!("{base}/peak_memory_bytes"),
            peak_mem as f64,
            "bytes",
            None,
        );
        collector.record(
            &format!("{base}/rows_per_sec"),
            rows_per_sec,
            "rows/s",
            None,
        );
        collector.record(&format!("{base}/gb_per_sec"), gb_per_sec, "GB/s", None);
        collector.record(
            &format!("{base}/probe_ms"),
            elapsed_secs * 1000.0,
            "ms",
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
    let mut db = fixture.open_db();
    let mut collector = BenchResultCollector::new(mode);

    let mut criterion = criterion_for_scale(scale).configure_from_args();
    bench_sort_topk(&mut criterion, &engine, &mut db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
