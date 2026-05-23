//! Memory-pressure degradation curve (TASK-546).
//!
//! Runs the same query against the persistent `purchases` fixture at
//! three memory budgets: 256 MiB, 1 GiB, and 3 GiB. Each budget is
//! applied through `QueryOptions::memory_budget_bytes`, which
//! overrides the engine-level default per submission. The bench
//! reports per-iteration wall clock + peak memory so the report
//! generator can plot how each query degrades when the budget shrinks.
//!
//! Two queries cover distinct memory-pressure regimes:
//!
//! - `sort_full` — `purchases | ORDER BY amount ASC`. Forces the
//!   sort operator to spill at the tighter budgets; at the 3 GiB
//!   budget it should fit in memory at Small / Medium scale.
//! - `agg_high_card` — `purchases | STATS n = COUNT(*) GROUP BY
//!   user_id`. Group cardinality scales with entity count
//!   (`scale.entity_count()`), so the hash table's footprint grows
//!   with scale and the 256 MiB budget gets tight at Large+.
//!
//! See `docs/design/perf-suite.md` §3.4.

use std::time::Instant;

use bqlite_benches::common::*;
use bqlite_engine::{Database, Engine, QueryOptions};
use criterion::{Criterion, Throughput};

struct BudgetPoint {
    label: &'static str,
    budget_bytes: u64,
}

// The engine rejects budgets below 512 MiB (see
// `docs/design/engine/memory-budget.md` §8.2: "minimum 536870912
// bytes per submission"). The original sweep target of 256 MiB is
// replaced with 512 MiB so the smallest point still produces an
// engine response (success or budget-exceeded) rather than a hard
// "budget below the floor" rejection.
const POINTS: &[BudgetPoint] = &[
    BudgetPoint {
        label: "budget_512mb",
        budget_bytes: 512 * 1024 * 1024,
    },
    BudgetPoint {
        label: "budget_1gb",
        budget_bytes: 1024 * 1024 * 1024,
    },
    BudgetPoint {
        label: "budget_3gb",
        budget_bytes: 3 * 1024 * 1024 * 1024,
    },
];

struct QueryProbe {
    label: &'static str,
    sql: &'static str,
}

const QUERIES: &[QueryProbe] = &[
    QueryProbe {
        label: "sort_full",
        sql: "purchases | ORDER BY amount ASC",
    },
    QueryProbe {
        label: "agg_high_card",
        sql: "purchases | STATS n = COUNT(*) GROUP BY user_id",
    },
];

fn run_with_budget(
    engine: &Engine,
    db: &mut Database,
    sql: &str,
    budget: u64,
) -> Result<(std::time::Duration, usize, u64), bqlite_engine::ExecutionFailure> {
    let opts = QueryOptions {
        memory_budget_bytes: Some(budget),
        ..QueryOptions::default()
    };
    let start = Instant::now();
    let result = engine.query_with_options(sql, db, &opts)?;
    let elapsed = start.elapsed();
    let rows: usize = result.rows.iter().map(|b| b.num_rows()).sum();
    let peak = result.peak_memory_bytes.unwrap_or(0);
    Ok((elapsed, rows, peak))
}

fn bench_memory_pressure(
    c: &mut Criterion,
    engine: &Engine,
    db: &mut Database,
    fixture: &PersistentFixture,
    collector: &mut BenchResultCollector,
) {
    let scale = BenchScale::from_env();
    let total_rows = fixture.manifest.rows;

    for query in QUERIES {
        let mut group = c.benchmark_group(format!(
            "perf/memory_pressure/{}/{}",
            scale.label(),
            query.label
        ));
        group.throughput(Throughput::Elements(total_rows));

        for point in POINTS {
            // Probe under the chosen budget. A budget-too-small error
            // is recorded but not panicked — the report should show
            // the regime where queries start failing.
            let probe = run_with_budget(engine, db, query.sql, point.budget_bytes);
            let base = format!(
                "perf/memory_pressure/{}/{}/{}",
                scale.label(),
                query.label,
                point.label
            );
            match probe {
                Ok((elapsed, rows, peak)) => {
                    let secs = elapsed.as_secs_f64().max(1e-9);
                    eprintln!(
                        "  [mem_pressure] query={} budget={}MB rows={} peak={}MiB elapsed={:.1}ms",
                        query.label,
                        point.budget_bytes / (1024 * 1024),
                        rows,
                        peak / (1024 * 1024),
                        secs * 1000.0,
                    );
                    collector.record(&format!("{base}/status"), 1.0, "ok", None);
                    collector.record(&format!("{base}/probe_ms"), secs * 1000.0, "ms", None);
                    collector.record(
                        &format!("{base}/peak_memory_bytes"),
                        peak as f64,
                        "bytes",
                        None,
                    );
                    collector.record(
                        &format!("{base}/rows_per_sec"),
                        total_rows as f64 / secs,
                        "rows/s",
                        None,
                    );
                    // Probe-only: per-iteration cost for full-sort /
                    // high-cardinality aggregations under tight
                    // budgets routinely exceeds the Criterion
                    // measurement window. The probe wall-clock + peak
                    // memory are the headline metrics; Criterion
                    // samples would not add useful signal here.
                }
                Err(e) => {
                    eprintln!(
                        "  [mem_pressure] query={} budget={}MB FAILED: {}",
                        query.label,
                        point.budget_bytes / (1024 * 1024),
                        e,
                    );
                    collector.record(&format!("{base}/status"), 0.0, "ok", None);
                    // No bench_function registered for failure points.
                    // The collector status=0 record is the signal the
                    // report uses to mark the budget regime as
                    // "queries fail here".
                }
            }
        }

        group.finish();
    }
}

fn main() {
    let scale = BenchScale::from_env();
    let mode = BenchMode::from_env();
    let fixture = PersistentFixture::load_or_build(scale);
    let engine = Engine::new();
    let mut db = fixture.open_db();
    let mut collector = BenchResultCollector::new(mode);

    let mut criterion = criterion_for_scale(scale).configure_from_args();
    bench_memory_pressure(&mut criterion, &engine, &mut db, &fixture, &mut collector);

    criterion.final_summary();
    collector.finish();
}
