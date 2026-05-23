//! `perf_report` — collect TASK-546 bench output and emit a single
//! markdown characterization report.
//!
//! Reads `$CARGO_TARGET_DIR/bench-results.json` (or `./target/...`)
//! and writes `docs/perf/wave5-results.md` per
//! `docs/design/perf-suite.md` §3.5. The JSON contract:
//!
//! ```text
//! "<mode>/perf/<bench>/<scale>/<point...>/<metric>": {
//!     "mode": "ci" | "reference",
//!     "unit": "rows/s" | "GB/s" | ...,
//!     "value": <f64>
//! }
//! ```
//!
//! The number of `<point>` segments varies — `scan_selectivity`
//! has one (`sel_0.001`); `memory_pressure` has two
//! (`sort_full/budget_3gb`). The parser handles arbitrary depth by
//! treating everything between `<scale>` and the trailing `<metric>`
//! as the point label.
//!
//! Output sections follow the perf-suite layout: headline,
//! methodology, then one section per bench group. Tables include
//! every scale present in the JSON (Small / Medium / Large / XLarge).
//! Missing data is rendered as `—`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct RawEntry {
    #[allow(dead_code)]
    mode: String,
    unit: String,
    value: f64,
}

/// Parsed key: `<mode>/perf/<bench>/<scale>/<point>/<metric>`.
#[derive(Debug, Clone)]
struct ParsedKey {
    bench: String,
    scale: String,
    point: String,
    metric: String,
}

fn parse_key(key: &str) -> Option<ParsedKey> {
    let parts: Vec<&str> = key.split('/').collect();
    // mode + "perf" + bench + scale + ≥1 point segment + metric  →  ≥6 parts.
    if parts.len() < 6 || parts[1] != "perf" {
        return None;
    }
    let bench = parts[2].to_string();
    let scale = parts[3].to_string();
    let metric = parts[parts.len() - 1].to_string();
    let point = parts[4..parts.len() - 1].join("/");
    Some(ParsedKey {
        bench,
        scale,
        point,
        metric,
    })
}

/// `bench -> scale -> point -> metric -> (value, unit)`.
type Db = BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, (f64, String)>>>>;

/// A value formatter (e.g. `fmt_rows_per_sec`).
type Fmt = fn(f64) -> String;

/// Row for the headline table: `(bench_key, display_label, metric, formatter)`.
type HeadlineRow = (&'static str, &'static str, &'static str, Fmt);

/// Column spec for `render_per_point_table`: `(metric_key, header, formatter)`.
type MetricCol = (&'static str, &'static str, Fmt);

fn load_db(path: &Path) -> Db {
    let raw = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let parsed: BTreeMap<String, RawEntry> =
        serde_json::from_str(&raw).expect("parse bench-results.json");
    let mut db: Db = BTreeMap::new();
    for (key, entry) in parsed {
        let Some(pk) = parse_key(&key) else {
            continue;
        };
        db.entry(pk.bench)
            .or_default()
            .entry(pk.scale)
            .or_default()
            .entry(pk.point)
            .or_default()
            .insert(pk.metric, (entry.value, entry.unit));
    }
    db
}

fn scales_present(db: &Db) -> Vec<String> {
    let mut seen: BTreeMap<&str, ()> = BTreeMap::new();
    for scales in db.values() {
        for scale in scales.keys() {
            seen.insert(scale.as_str(), ());
        }
    }
    // Order: small < medium < large < xlarge (alphabetic happens to
    // not produce this), so impose an explicit ladder.
    let ladder = ["small", "medium", "large", "xlarge"];
    let mut out = Vec::new();
    for s in ladder {
        if seen.contains_key(s) {
            out.push(s.to_string());
        }
    }
    // Append any non-ladder scales last so unknown values still show.
    for s in seen.keys() {
        if !ladder.contains(s) {
            out.push((*s).to_string());
        }
    }
    out
}

fn fmt_rows_per_sec(v: f64) -> String {
    if v >= 1e9 {
        format!("{:.2} B/s", v / 1e9)
    } else if v >= 1e6 {
        format!("{:.2} M/s", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.2} K/s", v / 1e3)
    } else {
        format!("{v:.0}/s")
    }
}

fn fmt_gb_per_sec(v: f64) -> String {
    format!("{v:.2} GB/s")
}

fn fmt_mb_per_sec(v: f64) -> String {
    format!("{v:.1} MB/s")
}

fn fmt_ms(v: f64) -> String {
    if v >= 1000.0 {
        format!("{:.2} s", v / 1000.0)
    } else {
        format!("{v:.1} ms")
    }
}

fn fmt_mib_from_bytes(v: f64) -> String {
    let mib = v / (1024.0 * 1024.0);
    if mib >= 1024.0 {
        format!("{:.2} GiB", mib / 1024.0)
    } else {
        format!("{mib:.0} MiB")
    }
}

fn fmt_count(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.2} M", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.1} K", v / 1e3)
    } else {
        format!("{v:.0}")
    }
}

/// Pull a metric from the nested map, or "—" if absent.
fn cell<F: FnOnce(f64) -> String>(
    db: &Db,
    bench: &str,
    scale: &str,
    point: &str,
    metric: &str,
    fmt: F,
) -> String {
    db.get(bench)
        .and_then(|m| m.get(scale))
        .and_then(|m| m.get(point))
        .and_then(|m| m.get(metric))
        .map(|(v, _)| fmt(*v))
        .unwrap_or_else(|| "—".to_string())
}

fn raw_value(db: &Db, bench: &str, scale: &str, point: &str, metric: &str) -> Option<f64> {
    db.get(bench)
        .and_then(|m| m.get(scale))
        .and_then(|m| m.get(point))
        .and_then(|m| m.get(metric))
        .map(|(v, _)| *v)
}

fn point_labels(db: &Db, bench: &str) -> Vec<String> {
    let mut all: BTreeMap<String, ()> = BTreeMap::new();
    let Some(scales) = db.get(bench) else {
        return Vec::new();
    };
    for points in scales.values() {
        for p in points.keys() {
            all.insert(p.clone(), ());
        }
    }
    all.keys().cloned().collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Section renderers — each appends to a single output string.
// ─────────────────────────────────────────────────────────────────────────────

fn render_headline(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Headline numbers\n\n");
    out.push_str("Best throughput observed across the run, per bench group. ");
    out.push_str("Higher is better; cells are blank when the bench did not run at that scale.\n\n");

    let cols = std::iter::once("Bench").chain(scales.iter().map(String::as_str));
    out.push_str("| ");
    out.push_str(&cols.collect::<Vec<_>>().join(" | "));
    out.push_str(" |\n|");
    for _ in 0..scales.len() + 1 {
        out.push_str("---|");
    }
    out.push('\n');

    let rows: &[HeadlineRow] = &[
        (
            "scan_selectivity",
            "Scan (peak rows/s)",
            "rows_per_sec",
            fmt_rows_per_sec,
        ),
        (
            "aggregation_cardinality",
            "Aggregation (peak rows/s)",
            "rows_per_sec",
            fmt_rows_per_sec,
        ),
        (
            "funnel_depth",
            "Funnel (peak rows/s)",
            "rows_per_sec",
            fmt_rows_per_sec,
        ),
        (
            "sort_topk",
            "Sort top-k (peak rows/s)",
            "rows_per_sec",
            fmt_rows_per_sec,
        ),
        (
            "ingest_throughput",
            "Ingest JSONL (peak rows/s)",
            "rows_per_sec",
            fmt_rows_per_sec,
        ),
        (
            "mc_scaling",
            "MC-scaling (peak rows/s)",
            "rows_per_sec",
            fmt_rows_per_sec,
        ),
        (
            "concurrent_queries",
            "Concurrent (peak queries/s)",
            "queries_per_sec",
            |v| format!("{v:.1}/s"),
        ),
    ];

    for (bench, label, metric, fmt) in rows {
        out.push_str("| ");
        out.push_str(label);
        for scale in scales {
            out.push_str(" | ");
            // For each scale, take the max value across all points.
            let mut best: Option<f64> = None;
            if let Some(scales_map) = db.get(*bench) {
                if let Some(points) = scales_map.get(scale) {
                    for metrics in points.values() {
                        if let Some((v, _)) = metrics.get(*metric) {
                            best = Some(best.map_or(*v, |b: f64| b.max(*v)));
                        }
                    }
                }
            }
            out.push_str(&best.map(fmt).unwrap_or_else(|| "—".to_string()));
        }
        out.push_str(" |\n");
    }
    out.push('\n');
}

fn render_methodology(scales: &[String], out: &mut String) {
    out.push_str("## Methodology\n\n");
    out.push_str("- **Hardware**: the run-host's defaults — see the `cargo bench` invocation log alongside this report for the exact machine specifics.\n");
    out.push_str("- **Scale ladder** (`BQLITE_BENCH_SCALE`):\n");
    out.push_str("  - `small` — 1M rows / 10K entities (default; suitable for development)\n");
    out.push_str("  - `medium` — 100M rows / 100K entities\n");
    out.push_str("  - `large` — 1B rows / 1M entities\n");
    out.push_str("  - `xlarge` — 10B rows / 10M entities\n");
    let present = scales.join(", ");
    out.push_str(&format!(
        "\n  Scales present in this report: **{present}**.\n"
    ));
    out.push_str(
        "\n- **Seed**: `0x00000000beaca15e` — fixed per `docs/design/perf-suite.md` §3.2.\n",
    );
    out.push_str("- **Mode**: `BQLITE_BENCH_MODE=ci` (default) vs `reference` controls which bench-result records are emitted; both modes still drive Criterion the same way.\n");
    out.push_str("- **Criterion config** (`benches/common/mod.rs::criterion_for_scale`):\n");
    out.push_str("  - small: sample_size=50, warm_up=1s, measurement=3s\n");
    out.push_str("  - medium: sample_size=20, warm_up=3s, measurement=10s\n");
    out.push_str("  - large: sample_size=10, warm_up=5s, measurement=30s\n");
    out.push_str("  - xlarge: sample_size=10, warm_up=10s, measurement=60s\n");
    out.push_str("- **Reproducing this report**:\n\n");
    out.push_str("  ```bash\n");
    out.push_str("  BQLITE_BENCH_SCALE=<scale> cargo bench -p bqlite-benches --bench perf_scan_selectivity \\\n");
    out.push_str("      --bench perf_aggregation_cardinality --bench perf_funnel_depth \\\n");
    out.push_str("      --bench perf_sort_topk --bench perf_ingest_throughput \\\n");
    out.push_str("      --bench perf_mc_scaling --bench perf_concurrent_queries \\\n");
    out.push_str("      --bench perf_memory_pressure\n");
    out.push_str("  cargo run -p bqlite-benches --release --bin perf_report\n");
    out.push_str("  ```\n\n");
}

/// Render a per-bench table where rows are points and columns are scales.
/// `metrics` is the list of metric keys (and their renderers) to show as
/// subcolumns within each scale.
fn render_per_point_table(
    db: &Db,
    bench: &str,
    scales: &[String],
    metrics: &[MetricCol],
    out: &mut String,
) {
    let points = point_labels(db, bench);
    if points.is_empty() {
        out.push_str("_No data collected._\n\n");
        return;
    }

    // Header row 1: scale spans (one cell per metric per scale).
    out.push_str("| Point |");
    for scale in scales {
        for (_, hdr, _) in metrics {
            out.push_str(&format!(" {scale} {hdr} |"));
        }
    }
    out.push_str("\n|---|");
    for _ in 0..(scales.len() * metrics.len()) {
        out.push_str("---|");
    }
    out.push('\n');

    for point in &points {
        out.push_str("| `");
        out.push_str(point);
        out.push('`');
        for scale in scales {
            for (metric, _, fmt) in metrics {
                out.push_str(" | ");
                out.push_str(&cell(db, bench, scale, point, metric, *fmt));
            }
        }
        out.push_str(" |\n");
    }
    out.push('\n');
}

fn render_scan_filter(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Scan / filter\n\n");
    out.push_str("Five selectivity targets driven through `purchases | where amount <= K`. ");
    out.push_str("`matched_ratio` is the actual fraction of rows the predicate passes (the streaming generator's amount distribution is not perfectly uniform, so this drifts from the K-derived target).\n\n");
    render_per_point_table(
        db,
        "scan_selectivity",
        scales,
        &[
            ("rows_per_sec", "rows/s", fmt_rows_per_sec),
            ("gb_per_sec", "GB/s", fmt_gb_per_sec),
            ("matched_ratio", "ratio", |v| format!("{v:.3}")),
        ],
        out,
    );
}

fn render_aggregation(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Aggregation\n\n");
    out.push_str("`COUNT(*) GROUP BY <key>` across group-key shapes covering low / mid / composite / high group-cardinalities per `docs/design/perf-suite.md` §3.4.\n\n");
    render_per_point_table(
        db,
        "aggregation_cardinality",
        scales,
        &[
            ("rows_per_sec", "rows/s", fmt_rows_per_sec),
            ("gb_per_sec", "GB/s", fmt_gb_per_sec),
            ("group_count", "groups", fmt_count),
        ],
        out,
    );
}

fn render_funnel(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Funnel / sequence\n\n");
    out.push_str("`MATCH FIRST SEQUENCE(event_0 THEN event_1 …) WITHIN 7d` at depths 2 / 5 / 10. ");
    out.push_str("`matches` is the number of matched sequences from the probe run.\n\n");
    render_per_point_table(
        db,
        "funnel_depth",
        scales,
        &[
            ("rows_per_sec", "rows/s", fmt_rows_per_sec),
            ("gb_per_sec", "GB/s", fmt_gb_per_sec),
            ("matches", "matches", fmt_count),
        ],
        out,
    );
}

fn render_sort_topk(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Sort / top-k\n\n");
    out.push_str("`purchases | ORDER BY amount ASC [LIMIT k]`. Currently probe-only — `ORDER BY ... LIMIT` does not push the limit into a top-k heap, so all three points pay the full-sort cost; `peak_memory_bytes` reflects that.\n\n");
    render_per_point_table(
        db,
        "sort_topk",
        scales,
        &[
            ("probe_ms", "probe", fmt_ms),
            ("rows_per_sec", "rows/s", fmt_rows_per_sec),
            ("peak_memory_bytes", "peak mem", fmt_mib_from_bytes),
        ],
        out,
    );
}

fn render_ingest(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Ingest\n\n");
    out.push_str("End-to-end ingest throughput for the three supported input shapes. ");
    out.push_str("`file_mb_per_sec` is the on-disk source size for JSONL/Parquet (and the SQL-string size for INSERT VALUES); `event_mb_per_sec` normalizes by the logical event byte count.\n\n");
    render_per_point_table(
        db,
        "ingest_throughput",
        scales,
        &[
            ("rows_per_sec", "rows/s", fmt_rows_per_sec),
            ("file_mb_per_sec", "file", fmt_mb_per_sec),
            ("event_mb_per_sec", "events", fmt_mb_per_sec),
        ],
        out,
    );
    // insert_values has sql_mb_per_sec instead of file_mb_per_sec;
    // the per-point table renders "—" in that cell. That's fine —
    // the rows/s and event_mb_per_sec axes still compare directly.
}

fn render_mc_scaling(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Multi-core scaling\n\n");
    out.push_str("`purchases | STATS COUNT(*) GROUP BY category, region` driven at `query_threads = 1 / 2 / 4 / 8 / auto`. ");
    out.push_str("Speedup is relative to the 1-thread probe of the same bench run (so a non-zero figure even at `threads_1` reflects measurement noise vs. the warm-up probe).\n\n");
    render_per_point_table(
        db,
        "mc_scaling",
        scales,
        &[
            ("resolved_threads", "threads", |v| format!("{v:.0}")),
            ("rows_per_sec", "rows/s", fmt_rows_per_sec),
            ("speedup_vs_1", "speedup", |v| format!("{v:.2}×")),
        ],
        out,
    );
}

fn render_concurrent(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Concurrent queries\n\n");
    out.push_str("1 / 4 / 16 client threads submit `purchases | STATS COUNT(*) GROUP BY category` against a shared `Arc<Mutex<Database>>` (`Database::open` holds a per-directory exclusive lock, so submissions serialize at the mutex). Per-query parallelism within the engine is unchanged — the bench varies only the submitter count.\n\n");
    render_per_point_table(
        db,
        "concurrent_queries",
        scales,
        &[
            ("queries_per_sec", "queries/s", |v| format!("{v:.1}")),
            ("per_thread_avg_ms", "per-thread", fmt_ms),
            ("aggregate_rows_per_sec", "rows/s", fmt_rows_per_sec),
        ],
        out,
    );
}

fn render_memory_pressure(db: &Db, scales: &[String], out: &mut String) {
    out.push_str("## Memory pressure\n\n");
    out.push_str("Same two queries (`sort_full`, `agg_high_card`) submitted at three `QueryOptions::memory_budget_bytes` budgets. ");
    out.push_str("`status` is 1 when the query completed under the budget, 0 when the engine rejected it (either under-floor or budget-exceeded). The 512 MiB floor reflects `docs/design/engine/memory-budget.md` §8.2.\n\n");

    let bench = "memory_pressure";
    let points = point_labels(db, bench);
    if points.is_empty() {
        out.push_str("_No data collected._\n\n");
        return;
    }

    out.push_str("| Point |");
    let metric_cols = ["status", "probe", "peak mem", "rows/s"];
    for scale in scales {
        for hdr in metric_cols {
            out.push_str(&format!(" {scale} {hdr} |"));
        }
    }
    out.push_str("\n|---|");
    for _ in 0..(scales.len() * metric_cols.len()) {
        out.push_str("---|");
    }
    out.push('\n');

    for point in &points {
        out.push_str("| `");
        out.push_str(point);
        out.push('`');
        for scale in scales {
            // status: render "ok" / "fail" / "—"
            let status_cell = raw_value(db, bench, scale, point, "status")
                .map(|v| if v >= 1.0 { "ok" } else { "fail" }.to_string())
                .unwrap_or_else(|| "—".to_string());
            out.push_str(&format!(" | {status_cell}"));
            // probe_ms — only populated for status=ok runs.
            out.push_str(" | ");
            out.push_str(&cell(db, bench, scale, point, "probe_ms", fmt_ms));
            // peak_memory_bytes — only populated for status=ok runs.
            out.push_str(" | ");
            out.push_str(&cell(
                db,
                bench,
                scale,
                point,
                "peak_memory_bytes",
                fmt_mib_from_bytes,
            ));
            // rows_per_sec
            out.push_str(" | ");
            out.push_str(&cell(
                db,
                bench,
                scale,
                point,
                "rows_per_sec",
                fmt_rows_per_sec,
            ));
        }
        out.push_str(" |\n");
    }
    out.push('\n');
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

fn resolve_results_path() -> PathBuf {
    // Match the BenchResultCollector::finish() location resolution.
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir).join("bench-results.json");
    }
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let mut p = PathBuf::from(manifest);
        // benches/ is a child of the workspace root.
        p.pop();
        return p.join("target/bench-results.json");
    }
    PathBuf::from("target/bench-results.json")
}

fn resolve_report_path() -> PathBuf {
    if let Ok(manifest) = std::env::var("CARGO_MANIFEST_DIR") {
        let mut p = PathBuf::from(manifest);
        p.pop();
        return p.join("docs/perf/wave5-results.md");
    }
    PathBuf::from("docs/perf/wave5-results.md")
}

fn main() {
    let input = resolve_results_path();
    let output = resolve_report_path();

    eprintln!("[perf_report] reading {}", input.display());
    let db = load_db(&input);
    let scales = scales_present(&db);
    eprintln!(
        "[perf_report] {} benches, scales: {}",
        db.len(),
        scales.join(", ")
    );

    let mut report = String::new();
    report.push_str("# bqlite Wave 5 performance characterization\n\n");
    report.push_str(
        "Auto-generated from `target/bench-results.json` by `cargo run -p bqlite-benches \
         --release --bin perf_report`. Do not edit by hand — every section is rendered from \
         the bench-collector JSON the `perf_*` benches emit.\n\n",
    );

    render_headline(&db, &scales, &mut report);
    render_methodology(&scales, &mut report);
    render_scan_filter(&db, &scales, &mut report);
    render_aggregation(&db, &scales, &mut report);
    render_funnel(&db, &scales, &mut report);
    render_sort_topk(&db, &scales, &mut report);
    render_ingest(&db, &scales, &mut report);
    render_mc_scaling(&db, &scales, &mut report);
    render_concurrent(&db, &scales, &mut report);
    render_memory_pressure(&db, &scales, &mut report);

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|e| panic!("create {}: {e}", parent.display()));
    }
    fs::write(&output, report).unwrap_or_else(|e| panic!("write {}: {e}", output.display()));
    eprintln!("[perf_report] wrote {}", output.display());
}
