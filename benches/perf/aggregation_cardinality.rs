//! `COUNT(*) GROUP BY` throughput across a group-cardinality sweep
//! (TASK-546).
//!
//! Reads the persistent `purchases` fixture and runs
//! `purchases | STATS n = COUNT(*) GROUP BY <key>` for two group-key
//! shapes (`docs/design/perf-suite.md` §3.4):
//!
//! - `mid_card_event_type` — 20 distinct values from the generator's
//!   `event_type` profile.
//! - `high_card_user_id` — one group per entity. Scales with
//!   `BenchScale::entity_count`: 10K at Small, 100K Medium, 1M Large,
//!   10M XLarge.
//!
//! The design-doc sweep originally called for low-card (`quantity`,
//! 10 groups) and composite (`category, region`, 128 groups) points
//! too. Both are skipped here because aggregation that references a
//! *property* column — either as the GROUP BY key or via
//! `SUM`/`AVG`/etc — surfaces a planner bug today:
//!
//!     Execution error: column `quantity` has index 6 but
//!     batch only has 3 columns
//!
//! The column-pruning rule in `opt::prune` reduces the scan to the
//! role-column subset (entity_id, ts, event_type) for STATS, but the
//! HashAggregateOperator still resolves property columns by full-
//! schema index. Filed separately; once fixed, restore
//! `low_card_quantity` (`GROUP BY quantity`) and
//! `composite_category_region` (`GROUP BY category, region`).
//!
//! The 100M-group point from the design doc remains omitted: the
//! generator's natural cardinalities top out at the entity count
//! (`scale.entity_count()` ≤ 10M), and synthesising 100M *distinct*
//! group keys from a 10M-entity fixture would require an artificial
//! row-id column that doesn't model any real workload.

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine};
use criterion::{black_box, Criterion, Throughput};

struct GroupPoint {
    label: &'static str,
    group_by: &'static str,
}

const POINTS: &[GroupPoint] = &[
    GroupPoint {
        label: "mid_card_event_type",
        group_by: "event_type",
    },
    GroupPoint {
        label: "high_card_user_id",
        group_by: "user_id",
    },
];

fn bench_aggregation(
    c: &mut Criterion,
    engine: &Engine,
    db: &mut Database,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;
    let bytes_logical = fixture.manifest.bytes_logical;

    let mut group = c.benchmark_group(format!("perf/aggregation_cardinality/{}", scale.label()));
    group.throughput(Throughput::Elements(total_rows));

    for point in POINTS {
        let sql = format!(
            "{} | STATS n = COUNT(*) GROUP BY {}",
            PersistentFixture::DEFAULT_TABLE,
            point.group_by,
        );

        // Probe once so the JSON has a definite group count and a
        // throughput estimate the report generator can index by label.
        let probe_start = Instant::now();
        let probe = engine
            .query(&sql, db)
            .unwrap_or_else(|e| panic!("aggregation_cardinality probe for {}: {e}", point.label));
        let probe_elapsed = probe_start.elapsed();
        let group_count: usize = probe.rows.iter().map(|b| b.num_rows()).sum();
        eprintln!(
            "  [aggregation] {} groups={} probe={:.1}ms",
            point.label,
            group_count,
            probe_elapsed.as_secs_f64() * 1000.0,
        );

        group.bench_function(point.label, |b| {
            b.iter(|| {
                let result = engine.query(&sql, db).expect("aggregation query");
                let total: usize = result.rows.iter().map(|b| b.num_rows()).sum();
                black_box(total)
            });
        });

        let elapsed_secs = probe_elapsed.as_secs_f64().max(1e-9);
        let rows_per_sec = total_rows as f64 / elapsed_secs;
        let gb_per_sec = (bytes_logical as f64 / elapsed_secs) / (1u64 << 30) as f64;
        let base = format!(
            "perf/aggregation_cardinality/{}/{}",
            scale.label(),
            point.label,
        );
        collector.record(&format!("{base}/group_count"), group_count as f64, "groups", None);
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
    bench_aggregation(&mut criterion, &engine, &mut db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
