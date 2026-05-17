//! Scan / filter throughput across a selectivity sweep (TASK-546).
//!
//! Reads the persistent `purchases` fixture and measures the engine's
//! end-to-end throughput on `purchases | where amount <= K` at five
//! selectivity targets. The streaming generator sets `amount =
//! (ev_idx % 5000) + 1` per event, which produces an amount
//! distribution dominated by each entity's own event-count
//! (per-entity events ≤ 5000 trace out `1..N`). The reported
//! `matched_ratio` is whatever the fixture actually yields — the
//! K values land in the *order* low → high, not at fixed percentile
//! targets.
//!
//! The pipeline appends `| select user_id, amount` so the bench
//! measures filter + project end-to-end (the prior workaround that
//! dropped the SELECT existed only while the column-index remap pass
//! was missing — see `opt::remap`).
//!
//! See `docs/design/perf-suite.md` §3.4. The bench is intentionally
//! report-only (TASK-546 §2 "out of scope" — no CI gating); per-scale
//! Criterion config comes from `criterion_for_scale`.

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine};
use criterion::{black_box, Criterion, Throughput};

/// One bench point: selectivity in [0,1] plus the BQL `amount <= K`
/// threshold the generator's uniform-[1,5000] property yields that
/// selectivity for.
struct SelectivityPoint {
    label: &'static str,
    selectivity: f64,
    amount_threshold: i64,
}

const POINTS: &[SelectivityPoint] = &[
    SelectivityPoint {
        label: "0.001",
        selectivity: 0.001,
        amount_threshold: 5,
    },
    SelectivityPoint {
        label: "0.01",
        selectivity: 0.01,
        amount_threshold: 50,
    },
    SelectivityPoint {
        label: "0.1",
        selectivity: 0.1,
        amount_threshold: 500,
    },
    SelectivityPoint {
        label: "0.5",
        selectivity: 0.5,
        amount_threshold: 2500,
    },
    SelectivityPoint {
        label: "1.0",
        selectivity: 1.0,
        amount_threshold: 5000,
    },
];

fn bench_scan_selectivity(
    c: &mut Criterion,
    engine: &Engine,
    db: &mut Database,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;
    let bytes_logical = fixture.manifest.bytes_logical;

    let mut group = c.benchmark_group(format!("perf/scan_selectivity/{}", scale.label()));
    group.throughput(Throughput::Elements(total_rows));

    for point in POINTS {
        let sql = format!(
            "{} | where amount <= {} | select user_id, amount",
            PersistentFixture::DEFAULT_TABLE,
            point.amount_threshold,
        );

        // Probe once before the timed loop so the report has a
        // matched-row count and an estimated rows-out / GB-out
        // unaffected by Criterion's variable iteration count.
        let probe_start = Instant::now();
        let probe = engine
            .query(&sql, db)
            .unwrap_or_else(|e| panic!("scan_selectivity probe for {}: {e}", point.label));
        let probe_elapsed = probe_start.elapsed();
        let matched_rows: usize = probe.rows.iter().map(|b| b.num_rows()).sum();
        let matched_ratio = matched_rows as f64 / total_rows.max(1) as f64;
        eprintln!(
            "  [scan_selectivity] target={} matched={}/{} ratio={:.4} probe={:.2}ms",
            point.label,
            matched_rows,
            total_rows,
            matched_ratio,
            probe_elapsed.as_secs_f64() * 1000.0,
        );

        group.bench_function(point.label, |b| {
            b.iter(|| {
                let result = engine.query(&sql, db).expect("scan_selectivity query");
                let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
                black_box(total)
            });
        });

        // Convert the probe latency into a rows/s and GB/s estimate.
        // Criterion's per-sample numbers remain the primary signal; the
        // collector rows give the report generator stable values to
        // index into.
        let elapsed_secs = probe_elapsed.as_secs_f64().max(1e-9);
        let rows_per_sec = total_rows as f64 / elapsed_secs;
        let gb_per_sec = (bytes_logical as f64 / elapsed_secs) / (1u64 << 30) as f64;
        let base = format!(
            "perf/scan_selectivity/{}/sel_{}",
            scale.label(),
            point.label
        );
        collector.record(&format!("{base}/matched_rows"), matched_rows as f64, "rows", None);
        collector.record(
            &format!("{base}/matched_ratio"),
            matched_ratio,
            "ratio",
            None,
        );
        collector.record(
            &format!("{base}/rows_per_sec"),
            rows_per_sec,
            "rows/s",
            None,
        );
        collector.record(&format!("{base}/gb_per_sec"), gb_per_sec, "GB/s", None);
        let _ = point.selectivity; // kept for documentation parity
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
    bench_scan_selectivity(&mut criterion, &engine, &mut db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
