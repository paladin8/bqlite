//! Funnel / sequence matching throughput across funnel depth
//! (TASK-546).
//!
//! Reads the persistent `purchases` fixture and runs
//! `purchases | MATCH FIRST SEQUENCE(event_0 THEN event_1 ... THEN event_{depth-1}) WITHIN 7d`
//! at depths 2, 5, and 10. The streaming generator stamps
//! `event_type = event_{(entity_idx + ev_idx) % 20}` so every entity
//! has a deterministic cycle through the 20 labels — the 2 / 5 / 10
//! step funnels each have non-zero match rates without requiring a
//! handcrafted fixture.
//!
//! `WITHIN 7d` was picked from `docs/design/perf-suite.md` §3.4. The
//! generator's 1-minute inter-event step means even a 10-step funnel
//! easily completes inside a 7-day window for entities that visit the
//! ordered prefix.

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine};
use criterion::{black_box, Criterion, Throughput};

struct DepthPoint {
    label: &'static str,
    depth: usize,
}

const POINTS: &[DepthPoint] = &[
    DepthPoint {
        label: "depth_2",
        depth: 2,
    },
    DepthPoint {
        label: "depth_5",
        depth: 5,
    },
    DepthPoint {
        label: "depth_10",
        depth: 10,
    },
];

fn build_funnel_query(depth: usize) -> String {
    let steps: Vec<String> = (0..depth).map(|i| format!("event_{i}")).collect();
    format!(
        "{} | MATCH FIRST SEQUENCE({}) WITHIN 7d",
        PersistentFixture::DEFAULT_TABLE,
        steps.join(" THEN "),
    )
}

fn bench_funnel(
    c: &mut Criterion,
    engine: &Engine,
    db: &mut Database,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;
    let bytes_logical = fixture.manifest.bytes_logical;

    let mut group = c.benchmark_group(format!("perf/funnel_depth/{}", scale.label()));
    group.throughput(Throughput::Elements(total_rows));

    for point in POINTS {
        let sql = build_funnel_query(point.depth);

        let probe_start = Instant::now();
        let probe = engine
            .query(&sql, db)
            .unwrap_or_else(|e| panic!("funnel_depth probe for {}: {e}", point.label));
        let probe_elapsed = probe_start.elapsed();
        let match_rows: usize = probe.rows.iter().map(|b| b.num_rows()).sum();
        eprintln!(
            "  [funnel] {} matches={} probe={:.1}ms",
            point.label,
            match_rows,
            probe_elapsed.as_secs_f64() * 1000.0,
        );

        group.bench_function(point.label, |b| {
            b.iter(|| {
                let result = engine.query(&sql, db).expect("funnel query");
                let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
                black_box(total)
            });
        });

        let elapsed_secs = probe_elapsed.as_secs_f64().max(1e-9);
        let rows_per_sec = total_rows as f64 / elapsed_secs;
        let gb_per_sec = (bytes_logical as f64 / elapsed_secs) / (1u64 << 30) as f64;
        let base = format!("perf/funnel_depth/{}/{}", scale.label(), point.label);
        collector.record(&format!("{base}/matches"), match_rows as f64, "matches", None);
        collector.record(
            &format!("{base}/rows_per_sec"),
            rows_per_sec,
            "rows/s",
            None,
        );
        collector.record(&format!("{base}/gb_per_sec"), gb_per_sec, "GB/s", None);
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
    bench_funnel(&mut criterion, &engine, &mut db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
