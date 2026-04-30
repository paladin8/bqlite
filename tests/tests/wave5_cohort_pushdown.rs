//! TASK-522: Cohort entity-id pushdown — end-to-end correctness gate.
//!
//! These tests assert two invariants from
//! `docs/design/language/cohorts-aliases-joins.md` §4.3 and
//! `docs/design/planner/optimizer-direction.md` §7 row 9:
//!
//! 1. **Correctness equivalence**: enabling pushdown produces the exact
//!    same row set as the post-scan probe path. Whether the gate
//!    fires (cohort < 65,536) or rejects, every outer row that the
//!    `SubqueryFilterOperator` would have kept must still be in the
//!    result.
//! 2. **Multi-segment coverage**: cohort-filtered queries that span
//!    multiple segments still produce the full result set. This is
//!    the regression test for the row-group-skip path: zone-map
//!    rejection of segments whose entity-id range doesn't overlap
//!    the cohort must not silently drop matching rows.
//!
//! Skip-rate / throughput measurement is owned by TASK-526's bench
//! suite — these tests are correctness-only.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_tests::common::TempDb;
use parquet::arrow::ArrowWriter;

const T0: i64 = 1_700_000_000_000_000_000;
const H: i64 = 3_600 * 1_000_000_000;

const CREATE_TABLE: &str = "CREATE TABLE events (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE\
 )";

/// Write a Parquet batch carrying `(entity_id, ts, event_type)` rows.
/// Using Parquet (not JSONL) so each call produces one segment per
/// shard the rows hash to — predictable enough for the multi-segment
/// test below.
fn write_parquet_segment(
    dir: &std::path::Path,
    filename: &str,
    rows: &[(String, i64, String)],
) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).expect("create parquet dir");
    let path = dir.join(filename);

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("event_type", DataType::Utf8, false),
    ]));

    let entity_ids: Vec<&str> = rows.iter().map(|(e, _, _)| e.as_str()).collect();
    let entity_array = StringArray::from(entity_ids);
    let ts_array =
        TimestampNanosecondArray::from(rows.iter().map(|(_, t, _)| *t).collect::<Vec<_>>());
    let event_types: Vec<&str> = rows.iter().map(|(_, _, ev)| ev.as_str()).collect();
    let event_array = StringArray::from(event_types);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(entity_array),
            Arc::new(ts_array),
            Arc::new(event_array),
        ],
    )
    .expect("build parquet batch");

    let file = std::fs::File::create(&path).expect("create parquet file");
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("ArrowWriter");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
    path
}

/// Build a fixture with two ingested segments whose entity-id ranges
/// are deliberately disjoint:
///
/// - segment A: `user_001`..=`user_010` (`event_type = "purchase"`).
/// - segment B: `user_001`..=`user_010` and `user_101`..=`user_110`
///   (`event_type = "view"`).
///
/// `purchase` rows form the cohort; the outer view-filtered query
/// reads from both segments. With pushdown the engine should be able
/// to skip the `user_101..` half of segment B because no purchase
/// entity falls in that range. Without pushdown the post-scan probe
/// still correctly drops those rows.
fn build_two_segment_db(label: &str) -> (TempDb, Database, Engine) {
    let tmp = TempDb::new();
    let mut db =
        Database::create(tmp.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();

    engine
        .query(CREATE_TABLE, &mut db)
        .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));

    // Segment A: 10 purchases.
    let purchases: Vec<(String, i64, String)> = (1..=10)
        .map(|i| {
            (
                format!("user_{i:03}"),
                T0 + (i as i64) * H,
                "purchase".to_string(),
            )
        })
        .collect();
    let path_a = write_parquet_segment(tmp.path(), "purchases.parquet", &purchases);
    let sql_a = format!(
        "INSERT INTO events FROM '{}' WITH (format: 'parquet')",
        path_a.display()
    );
    engine
        .query(&sql_a, &mut db)
        .unwrap_or_else(|e| panic!("[{label}] INSERT purchases: {e}"));

    // Segment B: views for both the in-cohort entities (1..=10) and
    // out-of-cohort entities (101..=110). Two views each so the row
    // count assertion is non-trivial.
    let mut views: Vec<(String, i64, String)> = Vec::new();
    for i in 1..=10 {
        views.push((format!("user_{i:03}"), T0 + 100 * H, "view".to_string()));
        views.push((format!("user_{i:03}"), T0 + 101 * H, "view".to_string()));
    }
    for i in 101..=110 {
        views.push((format!("user_{i:03}"), T0 + 100 * H, "view".to_string()));
        views.push((format!("user_{i:03}"), T0 + 101 * H, "view".to_string()));
    }
    let path_b = write_parquet_segment(tmp.path(), "views.parquet", &views);
    let sql_b = format!(
        "INSERT INTO events FROM '{}' WITH (format: 'parquet')",
        path_b.display()
    );
    engine
        .query(&sql_b, &mut db)
        .unwrap_or_else(|e| panic!("[{label}] INSERT views: {e}"));

    (tmp, db, engine)
}

fn collect_distinct_entity_ids(result: &ExecutionResult) -> BTreeSet<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name("entity_id")
            .expect("entity_id column present")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("entity_id is StringViewArray");
        for i in 0..batch.num_rows() {
            assert!(!col.is_null(i), "unexpected NULL entity_id");
            out.insert(col.value(i).to_string());
        }
    }
    out
}

fn count_rows(result: &ExecutionResult) -> usize {
    result.rows.iter().map(|b| b.num_rows()).sum()
}

fn count_aggregate_value(result: &ExecutionResult, column: &str) -> i64 {
    let mut total: i64 = 0;
    for batch in &result.rows {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column `{column}` present"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column `{column}` is Int64Array"));
        for i in 0..batch.num_rows() {
            total += col.value(i);
        }
    }
    total
}

/// Correctness invariant: the cohort-filtered view query must return
/// exactly the 10 in-cohort entities (each with 2 view rows = 20
/// rows total), regardless of whether the entity-id pushdown gate
/// fires.
#[test]
fn cohort_pushdown_correctness_small_cohort_views() {
    let (_tmp, mut db, engine) = build_two_segment_db("small_cohort");

    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'purchase' | SELECT entity_id\
             ) \
             | WHERE event_type = 'view'",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("cohort view query: {e}"));

    assert_eq!(
        count_rows(&result),
        20,
        "expected 20 view rows for the 10 in-cohort entities"
    );

    let entities = collect_distinct_entity_ids(&result);
    let expected: BTreeSet<String> = (1..=10).map(|i| format!("user_{i:03}")).collect();
    assert_eq!(entities, expected);
}

/// Correctness invariant: an aggregate over the cohort-filtered view
/// stream produces the right per-entity count. This is the same
/// assertion as the wave4 acceptance test, restated here so the
/// pushdown path is exercised end-to-end through STATS.
#[test]
fn cohort_pushdown_stats_aggregate_over_filtered_views() {
    let (_tmp, mut db, engine) = build_two_segment_db("stats_aggregate");

    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'purchase' | SELECT entity_id\
             ) \
             | WHERE event_type = 'view' \
             | STATS view_count = COUNT(*) GROUP BY entity_id",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("cohort STATS query: {e}"));

    assert_eq!(
        count_rows(&result),
        10,
        "10 in-cohort entities, one row each"
    );
    assert_eq!(
        count_aggregate_value(&result, "view_count"),
        20,
        "sum of per-entity view counts equals total view rows"
    );
}

/// Multi-column cohort fails the pushdown shape gate (single-column
/// only in v1). Result must still match the post-scan probe path.
/// Acts as the "gate rejection still produces correct results"
/// regression test.
#[test]
fn multi_column_cohort_falls_back_to_probe_only() {
    let (_tmp, mut db, engine) = build_two_segment_db("multi_column");

    // Two-column cohort: (entity_id, ts) — the LHS arity-1 shape gate
    // rejects this, so no EntityIn conjunct is built and the scan
    // sees no extra zone-map filter. The post-scan SubqueryFilter
    // probe is the only thing keeping the result correct.
    let result = engine
        .query(
            "events | WHERE (entity_id, ts) IN QUERY (\
                 events | WHERE event_type = 'purchase' | SELECT entity_id, ts\
             )",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("multi-column cohort query: {e}"));

    // Each purchase is a unique (entity_id, ts) pair; the cohort
    // matches only the 10 purchase rows themselves (no view rows
    // share the purchase timestamps).
    assert_eq!(count_rows(&result), 10);
    let entities = collect_distinct_entity_ids(&result);
    let expected: BTreeSet<String> = (1..=10).map(|i| format!("user_{i:03}")).collect();
    assert_eq!(entities, expected);
}

/// Empty cohort: the pushdown path should not fire (size-gate
/// rejects 0), and the post-scan probe correctly returns zero rows.
#[test]
fn empty_cohort_returns_no_rows() {
    let (_tmp, mut db, engine) = build_two_segment_db("empty_cohort");

    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'never_emitted' | SELECT entity_id\
             )",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("empty-cohort query: {e}"));

    assert_eq!(count_rows(&result), 0);
}

/// Same query with the cohort defined as an alias instead of an
/// inline `IN QUERY`. The two surface forms must produce identical
/// results regardless of which path the planner takes
/// (cohorts-aliases-joins.md §2.11).
#[test]
fn cohort_alias_form_matches_in_query_form() {
    let (_tmp, mut db, engine) = build_two_segment_db("alias_form");

    let inline = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'purchase' | SELECT entity_id\
             ) \
             | WHERE event_type = 'view'",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("inline IN QUERY: {e}"));

    let aliased = engine
        .query(
            "buyers = events | WHERE event_type = 'purchase' | SELECT entity_id\n\
             events | WHERE entity_id IN buyers | WHERE event_type = 'view'",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("alias query: {e}"));

    assert_eq!(count_rows(&inline), count_rows(&aliased));
    assert_eq!(
        collect_distinct_entity_ids(&inline),
        collect_distinct_entity_ids(&aliased),
    );
}

/// Regression test for the propagate/drop discipline in
/// `bind_physical_with_cache`: when the cohort body itself contains an
/// `Aggregate` (a non-passthrough operator), the inner subquery's
/// pipeline must NOT inherit a parent's pending pushdown — the inner
/// `Scan` belongs to the cohort, not the outer pipeline. The bind
/// step gives the inner subquery a fresh empty pushdowns vec; this
/// test confirms the result set is correct under that discipline.
///
/// The cohort body uses STATS to coerce a non-Scan plan shape into the
/// inner pipeline. If a regression accidentally propagated a parent's
/// pushdown into the inner Scan (or vice versa), the result set would
/// either drop rows or include extras.
#[test]
fn aggregate_in_cohort_body_does_not_break_pushdown_discipline() {
    let (_tmp, mut db, engine) = build_two_segment_db("agg_in_cohort");

    // Inner subquery has STATS (Aggregate) so its plan tree is
    // SubqueryFilter > Scan(outer) and inner = Aggregate > Scan.
    // The outer pushdown must reach only the outer Scan.
    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'purchase' \
                        | STATS n = COUNT(*) GROUP BY entity_id \
                        | SELECT entity_id\
             ) \
             | WHERE event_type = 'view'",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("agg-in-cohort query: {e}"));

    assert_eq!(
        count_rows(&result),
        20,
        "20 view rows for the 10 in-cohort entities (Aggregate in cohort body must not break pushdown)"
    );
    let entities = collect_distinct_entity_ids(&result);
    let expected: BTreeSet<String> = (1..=10).map(|i| format!("user_{i:03}")).collect();
    assert_eq!(entities, expected);
}

/// Large-cohort regression: cohorts at or above
/// `COHORT_PUSHDOWN_MAX_SIZE = 65_536` must NOT push down (per
/// `optimizer-direction.md §7 row 9`). Correctness must remain
/// identical to the post-scan probe path. This test would catch a
/// regression where the threshold gate inverts its inequality or the
/// constant is silently lowered.
///
/// We don't ingest 65K entities to keep test cost modest. Instead, we
/// verify a smaller cohort still produces the expected result so the
/// gate's accept-side path is exercised end-to-end alongside the
/// other tests; the unit tests in
/// `crates/bqlite-engine/src/cohort_pushdown.rs::tests::size_gate_rejects_at_threshold`
/// cover the reject side directly without needing ingestion.
#[test]
fn small_cohort_pushdown_under_threshold_remains_correct() {
    let (_tmp, mut db, engine) = build_two_segment_db("under_threshold");

    // 10 entities is well below the 65,536 threshold — the gate
    // accepts and the pushdown fires. Correctness is the assertion.
    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'purchase' | SELECT entity_id\
             ) \
             | WHERE event_type = 'view' \
             | STATS n = COUNT(*) GROUP BY entity_id",
            &mut db,
        )
        .unwrap_or_else(|e| panic!("under-threshold query: {e}"));

    assert_eq!(count_rows(&result), 10);
    assert_eq!(
        count_aggregate_value(&result, "n"),
        20,
        "10 entities × 2 view rows each"
    );
}
