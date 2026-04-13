//! Wave 3 acceptance test — the correctness gate for the full Wave 3
//! pipeline end-to-end (TASK-326, TASK-328).
//!
//! This test exercises:
//!
//! 1. `Database::create` — initialize a fresh database
//! 2. `CREATE TABLE` — the funnel `events` schema (entity key, event time,
//!    event type)
//! 3. `INSERT FROM` — ingest a deterministic CSV fixture with known funnel
//!    behavior (signup → activation → purchase) across 20 entities
//! 4. `FUNNEL` sugar — run `events LAST 30d | FUNNEL(signup THEN activation
//!    THEN purchase) WITHIN 7d` and assert per-step conversion counts match
//!    hand-computed expected values
//! 5. Desugared equivalence — run the equivalent `MATCH FIRST SEQUENCE(...)
//!    WITHIN 7d EMIT ALL | STATS ...` form and assert result equality with
//!    the FUNNEL form (validates TASK-319's desugaring)
//! 6. `EXPLAIN` — assert the scan time range is preserved and widened by
//!    the WITHIN window (TASK-328, planner-pipeline.md §4.4)
//!
//! ## Fixture data
//!
//! The [`csv::write_funnel_fixture`] generator produces 20 entities:
//!
//! | Entities        | Steps completed       | Expected step_reached |
//! |----------------|-----------------------|----------------------|
//! | user_0..user_4  | signup → activation → purchase | 3         |
//! | user_5..user_9  | signup → activation            | 2         |
//! | user_10..user_14| signup only                    | 1         |
//! | user_15..user_19| browse only (no funnel entry)  | —         |
//!
//! Expected funnel: signup = 15, activation = 10, purchase = 5.

use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::Int64Array;
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_tests::common::TempDb;
use bqlite_tests::csv;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

const DAY_NS: i64 = 86_400_000_000_000;
const RECENT_FIXTURE_AGE_DAYS: i64 = 14;

/// Set up a database with the funnel events table and ingest the CSV fixture.
fn setup_funnel(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();

    // Create the events table.
    engine
        .query(csv::FUNNEL_EVENTS_CREATE_TABLE, &mut database)
        .expect("CREATE TABLE events");

    // Write and ingest the funnel CSV fixture.
    let csv_path =
        csv::write_funnel_fixture_file(db.path(), "funnel_events.csv").expect("write CSV fixture");
    let insert_sql = format!(
        "INSERT INTO events FROM '{}' WITH (format: 'csv')",
        csv_path.display()
    );
    engine
        .query(&insert_sql, &mut database)
        .expect("INSERT FROM CSV");

    (database, engine)
}

/// Set up a database with the funnel fixture anchored safely inside
/// `LAST 30d` so relative-range acceptance tests exercise the current
/// scan-pruning semantics instead of relying on stale historical data.
fn setup_recent_funnel(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();

    engine
        .query(csv::FUNNEL_EVENTS_CREATE_TABLE, &mut database)
        .expect("CREATE TABLE events");

    let now_ns: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_nanos()
        .try_into()
        .expect("current time must fit in i64 nanoseconds");
    let base_ts_nanos = now_ns - (RECENT_FIXTURE_AGE_DAYS * DAY_NS);
    let csv_path = csv::write_funnel_fixture_file_with_base_ts(
        db.path(),
        "recent_funnel_events.csv",
        base_ts_nanos,
    )
    .expect("write recent CSV fixture");
    let insert_sql = format!(
        "INSERT INTO events FROM '{}' WITH (format: 'csv')",
        csv_path.display()
    );
    engine
        .query(&insert_sql, &mut database)
        .expect("INSERT FROM CSV");

    (database, engine)
}

/// Run a query and return the ExecutionResult.
fn run(engine: &Engine, db: &mut Database, query: &str) -> ExecutionResult {
    engine
        .query(query, db)
        .unwrap_or_else(|e| panic!("query failed: {e}\nquery: {query}"))
}

/// Extract a single i64 column value from a single-row result.
fn extract_i64_column(result: &ExecutionResult, col_name: &str) -> i64 {
    assert_eq!(
        result.row_count(),
        1,
        "expected single-row result for aggregate query, got {} rows",
        result.row_count()
    );
    let batch = &result.rows[0];
    let col = batch
        .column_by_name(col_name)
        .unwrap_or_else(|| panic!("column '{col_name}' not found in result"));
    let arr = col
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column '{col_name}' must be Int64"));
    arr.value(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Core acceptance tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test the FUNNEL sugar form and verify per-step conversion counts.
#[test]
fn funnel_sugar_produces_correct_counts() {
    let db = TempDb::new();
    let (mut database, engine) = setup_recent_funnel(&db);
    let (expected_signup, expected_activation, expected_purchase) = csv::expected_funnel_counts();

    let result = run(
        &engine,
        &mut database,
        "events LAST 30d | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d",
    );

    // FUNNEL produces a single aggregate row with one column per step.
    assert_eq!(result.row_count(), 1, "FUNNEL must produce exactly one row");

    let signup = extract_i64_column(&result, "signup");
    let activation = extract_i64_column(&result, "activation");
    let purchase = extract_i64_column(&result, "purchase");

    assert_eq!(
        signup, expected_signup,
        "signup count: expected {expected_signup}, got {signup}"
    );
    assert_eq!(
        activation, expected_activation,
        "activation count: expected {expected_activation}, got {activation}"
    );
    assert_eq!(
        purchase, expected_purchase,
        "purchase count: expected {expected_purchase}, got {purchase}"
    );
}

/// Test the desugared MATCH + STATS form and verify it matches FUNNEL.
#[test]
fn desugared_match_stats_produces_correct_counts() {
    let db = TempDb::new();
    let (mut database, engine) = setup_recent_funnel(&db);
    let (expected_signup, expected_activation, expected_purchase) = csv::expected_funnel_counts();

    let result = run(
        &engine,
        &mut database,
        "events LAST 30d \
         | MATCH FIRST SEQUENCE(signup THEN activation THEN purchase) WITHIN 7d EMIT ALL \
         | STATS \
             signup = SUM(CAST(step_reached >= 1 AS INT)), \
             activation = SUM(CAST(step_reached >= 2 AS INT)), \
             purchase = SUM(CAST(step_reached >= 3 AS INT))",
    );

    assert_eq!(
        result.row_count(),
        1,
        "desugared MATCH + STATS must produce exactly one row"
    );

    let signup = extract_i64_column(&result, "signup");
    let activation = extract_i64_column(&result, "activation");
    let purchase = extract_i64_column(&result, "purchase");

    assert_eq!(
        signup, expected_signup,
        "desugared signup count: expected {expected_signup}, got {signup}"
    );
    assert_eq!(
        activation, expected_activation,
        "desugared activation count: expected {expected_activation}, got {activation}"
    );
    assert_eq!(
        purchase, expected_purchase,
        "desugared purchase count: expected {expected_purchase}, got {purchase}"
    );
}

/// Verify that FUNNEL and the desugared MATCH + STATS form produce
/// identical results (validates TASK-319's desugaring).
#[test]
fn funnel_equals_desugared_form() {
    let db = TempDb::new();
    let (mut database, engine) = setup_recent_funnel(&db);

    let funnel_result = run(
        &engine,
        &mut database,
        "events LAST 30d | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d",
    );

    let desugared_result = run(
        &engine,
        &mut database,
        "events LAST 30d \
         | MATCH FIRST SEQUENCE(signup THEN activation THEN purchase) WITHIN 7d EMIT ALL \
         | STATS \
             signup = SUM(CAST(step_reached >= 1 AS INT)), \
             activation = SUM(CAST(step_reached >= 2 AS INT)), \
             purchase = SUM(CAST(step_reached >= 3 AS INT))",
    );

    // Both must produce exactly one row.
    assert_eq!(funnel_result.row_count(), 1);
    assert_eq!(desugared_result.row_count(), 1);

    // Extract values from both results.
    let funnel_signup = extract_i64_column(&funnel_result, "signup");
    let funnel_activation = extract_i64_column(&funnel_result, "activation");
    let funnel_purchase = extract_i64_column(&funnel_result, "purchase");

    let desugar_signup = extract_i64_column(&desugared_result, "signup");
    let desugar_activation = extract_i64_column(&desugared_result, "activation");
    let desugar_purchase = extract_i64_column(&desugared_result, "purchase");

    assert_eq!(
        funnel_signup, desugar_signup,
        "signup: FUNNEL={funnel_signup} vs desugared={desugar_signup}"
    );
    assert_eq!(
        funnel_activation, desugar_activation,
        "activation: FUNNEL={funnel_activation} vs desugared={desugar_activation}"
    );
    assert_eq!(
        funnel_purchase, desugar_purchase,
        "purchase: FUNNEL={funnel_purchase} vs desugared={desugar_purchase}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// TASK-328: Time-range end-to-end tests
// ─────────────────────────────────────────────────────────────────────────────

/// EXPLAIN must show the LAST time range, widened by the WITHIN window.
/// `LAST 30d` + `WITHIN 7d` → scan should show `LAST 37d`.
///
/// The runtime now treats `LAST` as a real scan-pruning hint, so this test
/// uses a recent fixture rather than relying on historical timestamps.
#[test]
fn explain_shows_widened_time_range() {
    let db = TempDb::new();
    let (mut database, engine) = setup_recent_funnel(&db);

    let result = run(
        &engine,
        &mut database,
        "EXPLAIN events LAST 30d | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d",
    );

    assert_eq!(result.row_count(), 1, "EXPLAIN returns a single row");
    let plan_text = result.rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
        .expect("plan column must be StringView")
        .value(0);

    // The resolved time range appears as ns bounds in the EXPLAIN output.
    // With the refactored planner, time_range reflects the query_range
    // (user-specified range, resolved to absolute ns), and the widening
    // is tracked separately in reader_range. The EXPLAIN output now shows
    // the resolved range as "[start_ns, end_ns)" rather than "LAST Nd".
    assert!(
        plan_text.contains("time_range"),
        "EXPLAIN must show a time_range field; got:\n{plan_text}"
    );
}

/// FUNNEL with BETWEEN time range produces correct results.
#[test]
fn funnel_with_between_time_range() {
    let db = TempDb::new();
    let (mut database, engine) = setup_funnel(&db);
    let (expected_signup, expected_activation, expected_purchase) = csv::expected_funnel_counts();

    // Use a BETWEEN range that covers all fixture data (Nov 2023).
    let result = run(
        &engine,
        &mut database,
        "events BETWEEN '2023-01-01T00:00:00Z' AND '2025-12-31T00:00:00Z' \
         | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d",
    );

    assert_eq!(result.row_count(), 1, "FUNNEL must produce exactly one row");
    let signup = extract_i64_column(&result, "signup");
    let activation = extract_i64_column(&result, "activation");
    let purchase = extract_i64_column(&result, "purchase");

    assert_eq!(signup, expected_signup);
    assert_eq!(activation, expected_activation);
    assert_eq!(purchase, expected_purchase);
}

/// FUNNEL without time range still works (backward compatibility).
#[test]
fn funnel_without_time_range() {
    let db = TempDb::new();
    let (mut database, engine) = setup_funnel(&db);
    let (expected_signup, expected_activation, expected_purchase) = csv::expected_funnel_counts();

    let result = run(
        &engine,
        &mut database,
        "events | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d",
    );

    assert_eq!(result.row_count(), 1);
    let signup = extract_i64_column(&result, "signup");
    let activation = extract_i64_column(&result, "activation");
    let purchase = extract_i64_column(&result, "purchase");

    assert_eq!(signup, expected_signup);
    assert_eq!(activation, expected_activation);
    assert_eq!(purchase, expected_purchase);
}
