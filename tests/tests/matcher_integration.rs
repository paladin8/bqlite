//! Integration tests for the matcher (TASK-324).
//!
//! Each test exercises the full engine pipeline: parse → plan → bind → execute.
//! Small CSV fixtures are ingested via `INSERT VALUES`, and the MATCH query
//! result is asserted against exact expected values.
//!
//! Coverage matrix from `sequence-matching.md`:
//!
//! - Linear patterns (basic two-step, three-step, interleaved noise)
//! - MATCH FIRST (default) and MATCH ALL
//! - WITHOUT negation (eager kill within negation scope)
//! - $var variable bindings (single, multiple, NULL short-circuit)
//! - IMMEDIATELY modifier (consecutive matching)
//! - Time-window expiry (before, at boundary, after)
//! - EMIT ALL (partial matches with step_reached)
//! - Alternation on steps (branching NFA)
//! - One-or-more (+) and zero-or-more (*) repetition
//! - Entity sub-batch streaming (multiple entities)

use arrow::array::{Array, Int64Array, StringViewArray};
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_tests::common::TempDb;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Events-only table: entity_id, ts, event_type — the minimal schema for
/// MATCH patterns that don't need property columns.
const CREATE_EVENTS: &str = "\
    CREATE TABLE events (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE\
    )";

/// Events table with a property column for WHERE predicates and variable
/// bindings.
const CREATE_EVENTS_WITH_PROPS: &str = "\
    CREATE TABLE events (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE, \
        amount INT, \
        category STRING\
    )";

/// Create a fresh database with the events table and return the handles.
fn setup_events(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS, &mut database)
        .expect("CREATE TABLE events");
    (database, engine)
}

/// Create a fresh database with the events-with-properties table.
fn setup_events_with_props(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS_WITH_PROPS, &mut database)
        .expect("CREATE TABLE events");
    (database, engine)
}

/// Run a query and return the ExecutionResult.
fn run(engine: &Engine, db: &mut Database, query: &str) -> ExecutionResult {
    engine
        .query(query, db)
        .unwrap_or_else(|e| panic!("query failed: {e}\nquery: {query}"))
}

/// Collect entity_id values from a result set as sorted strings.
fn entity_ids_sorted(result: &ExecutionResult) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for batch in &result.rows {
        let col = batch.column_by_name("entity_id").expect("entity_id column");
        let arr = col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        for i in 0..arr.len() {
            ids.push(arr.value(i).to_string());
        }
    }
    ids.sort();
    ids
}

/// Collect match_duration values from a result set.
fn match_durations(result: &ExecutionResult) -> Vec<Option<i64>> {
    let mut durations: Vec<Option<i64>> = Vec::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name("match_duration")
            .expect("match_duration column");
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..arr.len() {
            if arr.is_null(i) {
                durations.push(None);
            } else {
                durations.push(Some(arr.value(i)));
            }
        }
    }
    durations
}

/// Collect step_reached values from a result set.
fn step_reached_values(result: &ExecutionResult) -> Vec<i64> {
    let mut values: Vec<i64> = Vec::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name("step_reached")
            .expect("step_reached column");
        let arr = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..arr.len() {
            values.push(arr.value(i));
        }
    }
    values
}

/// Total row count across all batches.
fn total_rows(result: &ExecutionResult) -> usize {
    result.rows.iter().map(|b| b.num_rows()).sum()
}

/// Insert events for entity u1: signup at t=100, view at t=200, purchase at t=300.
const INSERT_BASIC: &str = "\
    INSERT INTO events VALUES \
    ('u1', 100, 'signup'), \
    ('u1', 200, 'view'), \
    ('u1', 300, 'purchase')";

// ─────────────────────────────────────────────────────────────────────────────
// Linear simple patterns
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn linear_two_step_match_first() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(&engine, &mut database, INSERT_BASIC);

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
    assert_eq!(entity_ids_sorted(&result), vec!["u1"]);
    // duration = 300 - 100 = 200
    assert_eq!(match_durations(&result), vec![Some(200)]);
}

#[test]
fn linear_three_step_match_first() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(&engine, &mut database, INSERT_BASIC);
    // view at t=200 is noise — non-consecutive matching skips it.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN view THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
    // duration = 300 - 100 = 200
    assert_eq!(match_durations(&result), vec![Some(200)]);
}

#[test]
fn no_match_returns_empty() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(&engine, &mut database, INSERT_BASIC);

    // No "checkout" event exists.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN checkout)",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn wrong_order_no_match() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // purchase before signup — THEN requires strictly increasing timestamps.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'purchase'), \
         ('u1', 200, 'signup')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn multiple_entities_independent_matching() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase'), \
         ('u2', 100, 'signup'), \
         ('u2', 200, 'view'), \
         ('u3', 100, 'signup'), \
         ('u3', 200, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    // u1 and u3 match, u2 does not.
    assert_eq!(total_rows(&result), 2);
    assert_eq!(entity_ids_sorted(&result), vec!["u1", "u3"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// MATCH ALL
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn match_all_returns_multiple_non_overlapping() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase'), \
         ('u1', 300, 'signup'), \
         ('u1', 400, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH ALL SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 2);
    // Both matches for the same entity.
    let durations = match_durations(&result);
    assert_eq!(durations, vec![Some(100), Some(100)]);
}

#[test]
fn match_first_stops_after_first() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase'), \
         ('u1', 300, 'signup'), \
         ('u1', 400, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
    assert_eq!(match_durations(&result), vec![Some(100)]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Time windows
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn time_window_within_passes() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 500, 'purchase')",
    );

    // Window of 1000 ns — match should succeed (duration = 400).
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 1000ns",
    );
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn time_window_expired_no_match() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 1200, 'purchase')",
    );

    // Window of 1000 ns — match fails because 1200 - 100 > 1000.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 1000ns",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn time_window_boundary_exact() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 1100, 'purchase')",
    );

    // Window is exactly 1000 ns. The match at t=1100 has duration 1000.
    // The WITHIN window check is anchor_ts + window ≥ event_ts, so
    // 100 + 1000 = 1100 ≥ 1100 → should match (boundary inclusive).
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 1000ns",
    );
    // The window comparison is `event_ts <= anchor_ts + window` so boundary
    // events pass. If the engine uses strict < instead, this test catches it.
    assert_eq!(total_rows(&result), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// WITHOUT negation
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn without_negation_kills_match() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'churn'), \
         ('u1', 300, 'purchase')",
    );

    // churn between signup and purchase kills the match.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup WITHOUT churn THEN purchase)",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn without_negation_no_poison_event_matches() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'view'), \
         ('u1', 300, 'purchase')",
    );

    // No churn event — match should succeed.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup WITHOUT churn THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
    assert_eq!(match_durations(&result), vec![Some(200)]);
}

// ─────────────────────────────────────────────────────────────────────────────
// EMIT ALL (partial matches)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn emit_all_partial_match_reports_step_reached() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'view')",
    );

    // Pattern: signup → purchase → checkout. Entity only reaches step 1.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase THEN checkout) EMIT ALL",
    );
    assert_eq!(total_rows(&result), 1);
    let steps = step_reached_values(&result);
    // step_reached = 1: the entity matched step 1 (signup) but not step 2.
    assert_eq!(steps, vec![1]);
}

#[test]
fn emit_all_completed_match_reports_full_steps() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase'), \
         ('u1', 300, 'checkout')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase THEN checkout) EMIT ALL",
    );
    assert_eq!(total_rows(&result), 1);
    let steps = step_reached_values(&result);
    // step_reached = 3: all three steps matched.
    assert_eq!(steps, vec![3]);
}

#[test]
fn emit_all_two_of_three_steps() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase')",
    );

    // Pattern: signup → purchase → checkout. Entity reaches step 2 of 3.
    // The NFA drains all in-progress candidates at entity end: one at
    // state 1 (step_reached=1) and one at state 2 (step_reached=2).
    // This is correct for funnel analysis — each row shows a step the
    // entity entered.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase THEN checkout) EMIT ALL",
    );
    assert_eq!(total_rows(&result), 2);
    let steps = step_reached_values(&result);
    assert_eq!(steps, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE predicates on steps
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn step_where_predicate_filters_events() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_props(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'purchase', 50, 'books'), \
         ('u1', 200, 'purchase', 200, 'electronics'), \
         ('u1', 300, 'refund', NULL, NULL)",
    );

    // Only purchase with amount > 100 satisfies step 1.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(purchase WHERE amount > 100 THEN refund)",
    );
    assert_eq!(total_rows(&result), 1);
    // The qualifying purchase is at t=200, refund at t=300 → duration=100.
    assert_eq!(match_durations(&result), vec![Some(100)]);
}

#[test]
fn step_where_predicate_no_qualifying_event() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_props(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'purchase', 50, 'books'), \
         ('u1', 200, 'refund', NULL, NULL)",
    );

    // No purchase with amount > 100 → no match.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(purchase WHERE amount > 100 THEN refund)",
    );
    assert_eq!(total_rows(&result), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Alternation (branching NFA)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn alternation_on_first_step() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup_web'), \
         ('u1', 200, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE((signup_web OR signup_mobile) THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn alternation_on_second_step() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'subscription')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN (purchase OR subscription))",
    );
    assert_eq!(total_rows(&result), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Repetition
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn one_or_more_repetition_single_occurrence() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'view'), \
         ('u1', 300, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN view+ THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn one_or_more_repetition_multiple_occurrences() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'view'), \
         ('u1', 300, 'view'), \
         ('u1', 400, 'view'), \
         ('u1', 500, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN view+ THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn one_or_more_repetition_zero_occurrences_fails() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase')",
    );

    // view+ requires at least one view between signup and purchase.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN view+ THEN purchase)",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn zero_or_more_repetition_zero_occurrences_passes() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase')",
    );

    // view* allows zero views.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN view* THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// IMMEDIATELY modifier (consecutive matching)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn immediately_consecutive_match() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // Timestamps at t=100, t=101 — consecutive nanosecond events.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 101, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN IMMEDIATELY purchase)",
    );
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn immediately_with_only_relevant_events_matches() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // The NFA only sees events matching relevant event types. The "view"
    // event is filtered out by the scan because it doesn't appear in the
    // pattern. From the NFA's perspective, purchase immediately follows
    // signup in the filtered stream.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'view'), \
         ('u1', 300, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN IMMEDIATELY purchase)",
    );
    assert_eq!(total_rows(&result), 1);
}

#[test]
fn immediately_reanchors_after_intervening_relevant_event() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // A second signup between signup and purchase breaks IMMEDIATELY
    // because signup IS a relevant event type for the pattern.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'signup'), \
         ('u1', 300, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN IMMEDIATELY purchase)",
    );
    // The second signup at t=200 causes the IMMEDIATELY constraint to
    // fail for the first signup at t=100. The second signup at t=200
    // re-anchors and finds purchase at t=300, which IS immediately next
    // in the stream. So this actually matches.
    assert_eq!(total_rows(&result), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Variable bindings ($var)
// ─────────────────────────────────────────────────────────────────────────────

/// Create a table with a `plan` column for variable binding tests.
const CREATE_EVENTS_WITH_PLAN: &str = "\
    CREATE TABLE events (\
        user_id STRING NOT NULL ENTITY KEY, \
        ts TIMESTAMP NOT NULL EVENT TIME, \
        event_type STRING NOT NULL EVENT TYPE, \
        plan STRING, \
        category STRING\
    )";

fn setup_events_with_plan(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS_WITH_PLAN, &mut database)
        .expect("CREATE TABLE events");
    (database, engine)
}

/// Collect string column values from a result set.
fn string_column_values(result: &ExecutionResult, col_name: &str) -> Vec<String> {
    let mut vals = Vec::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name(col_name)
            .unwrap_or_else(|| panic!("column `{col_name}` not found"));
        let arr = col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap_or_else(|| panic!("column `{col_name}` is not StringViewArray"));
        for i in 0..arr.len() {
            vals.push(arr.value(i).to_string());
        }
    }
    vals
}

#[test]
fn single_variable_binding_filters_by_plan() {
    // signup(plan=free) THEN purchase(plan=free) → track $plan=free matches.
    // signup(plan=pro) THEN purchase(plan=pro) → track $plan=pro matches.
    // Cross-plan (signup free, purchase pro) does NOT match.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', 'free', NULL), \
         ('u1', 200, 'purchase', 'free', NULL), \
         ('u2', 100, 'signup', 'pro', NULL), \
         ('u2', 200, 'purchase', 'free', NULL)",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(\
             signup WHERE plan = $plan \
             THEN purchase WHERE plan = $plan\
         )",
    );
    // u1 matches (free→free), u2 does NOT (pro→free is a cross-plan mismatch).
    assert_eq!(total_rows(&result), 1);
    assert_eq!(entity_ids_sorted(&result), vec!["u1"]);
    // Output should include the $plan binding column.
    assert_eq!(string_column_values(&result, "$plan"), vec!["free"]);
}

#[test]
fn single_binding_commuted_form() {
    // Test `$plan = plan` (variable on the left side).
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', 'free', NULL), \
         ('u1', 200, 'purchase', 'free', NULL)",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(\
             signup WHERE $plan = plan \
             THEN purchase WHERE $plan = plan\
         )",
    );
    assert_eq!(total_rows(&result), 1);
    assert_eq!(string_column_values(&result, "$plan"), vec!["free"]);
}

#[test]
fn single_binding_multiple_entities_different_plans() {
    // Multiple entities with different plan values.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', 'free', NULL), \
         ('u1', 200, 'purchase', 'free', NULL), \
         ('u2', 100, 'signup', 'pro', NULL), \
         ('u2', 200, 'purchase', 'pro', NULL), \
         ('u3', 100, 'signup', 'enterprise', NULL), \
         ('u3', 200, 'purchase', 'starter', NULL)",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(\
             signup WHERE plan = $plan \
             THEN purchase WHERE plan = $plan\
         )",
    );
    // u1 matches (free→free), u2 matches (pro→pro), u3 does NOT (enterprise→starter).
    assert_eq!(total_rows(&result), 2);
    let ids = entity_ids_sorted(&result);
    assert_eq!(ids, vec!["u1", "u2"]);
}

#[test]
fn multi_variable_binding() {
    // Two variables: plan = $plan AND category = $cat.
    // Both must match across steps.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', 'free', 'web'), \
         ('u1', 200, 'purchase', 'free', 'web'), \
         ('u2', 100, 'signup', 'free', 'web'), \
         ('u2', 200, 'purchase', 'free', 'mobile')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(\
             signup WHERE plan = $plan AND category = $cat \
             THEN purchase WHERE plan = $plan AND category = $cat\
         )",
    );
    // u1 matches (both plan and category match), u2 does NOT (category differs).
    assert_eq!(total_rows(&result), 1);
    assert_eq!(entity_ids_sorted(&result), vec!["u1"]);
    assert_eq!(string_column_values(&result, "$plan"), vec!["free"]);
    assert_eq!(string_column_values(&result, "$cat"), vec!["web"]);
}

#[test]
fn null_binding_short_circuits() {
    // When the binding column is NULL, the step predicate evaluates to NULL
    // under three-valued logic and the step does NOT match.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', NULL, NULL), \
         ('u1', 200, 'purchase', 'free', NULL)",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(\
             signup WHERE plan = $plan \
             THEN purchase WHERE plan = $plan\
         )",
    );
    // signup has plan=NULL → no binding created → no match.
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn match_all_with_variable_rebinding() {
    // MATCH ALL: after first match completes, the track resets and can
    // match again with the same or different binding values.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', 'free', NULL), \
         ('u1', 200, 'purchase', 'free', NULL), \
         ('u1', 300, 'signup', 'free', NULL), \
         ('u1', 400, 'purchase', 'free', NULL)",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH ALL SEQUENCE(\
             signup WHERE plan = $plan \
             THEN purchase WHERE plan = $plan\
         )",
    );
    // Two matches for the $plan=free track: (100,200) and (300,400).
    assert_eq!(total_rows(&result), 2);
}

#[test]
fn mixed_binding_and_negation() {
    // Variable binding combined with WITHOUT negation.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup', 'free', NULL), \
         ('u1', 200, 'purchase', 'free', NULL), \
         ('u2', 100, 'signup', 'pro', NULL), \
         ('u2', 150, 'churn', NULL, NULL), \
         ('u2', 200, 'purchase', 'pro', NULL)",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(\
             signup WHERE plan = $plan \
             WITHOUT churn \
             THEN purchase WHERE plan = $plan\
         )",
    );
    // u1 matches (no churn), u2 killed by churn poison transition.
    assert_eq!(total_rows(&result), 1);
    assert_eq!(entity_ids_sorted(&result), vec!["u1"]);
    assert_eq!(string_column_values(&result, "$plan"), vec!["free"]);
}

#[test]
fn variable_in_non_equality_context_is_rejected() {
    // $var must only appear in `column = $var` or `$var = column`.
    // Using $plan in a non-equality context (e.g., $plan > 'free') is an error.
    let db = TempDb::new();
    let (mut database, engine) = setup_events_with_plan(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES ('u1', 100, 'signup', 'free', NULL)",
    );

    let err = engine
        .query(
            "events | MATCH FIRST SEQUENCE(\
                 signup WHERE $plan > plan \
                 THEN purchase WHERE plan = $plan\
             )",
            &mut database,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("equality"),
        "error should mention equality constraint: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Entity sub-batch streaming (oversized entities, multiple sub-batches)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn many_events_per_entity_match_across_inserts() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);

    // Insert events in multiple batches to exercise sub-batch streaming.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup')",
    );
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 200, 'view')",
    );
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 300, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 1);
    assert_eq!(match_durations(&result), vec![Some(200)]);
}

#[test]
fn many_entities_across_inserts() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);

    // Insert entities across multiple INSERT statements.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase')",
    );
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u2', 100, 'signup'), \
         ('u2', 200, 'purchase')",
    );
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u3', 100, 'signup')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 2);
    assert_eq!(entity_ids_sorted(&result), vec!["u1", "u2"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge cases
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn single_entity_single_event_no_match() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES ('u1', 100, 'signup')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn same_timestamp_events_do_not_match_then() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // Both events at the same timestamp.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 100, 'purchase')",
    );

    // THEN requires strictly increasing timestamps — same-ts doesn't satisfy.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 0);
}

#[test]
fn empty_table_returns_empty() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// MATCH ALL + EMIT ALL combination
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn match_all_emit_all_is_parse_error() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES ('u1', 100, 'signup'), ('u1', 200, 'purchase')",
    );

    // MATCH ALL EMIT ALL is intentionally unsupported — the parser rejects
    // this combination per query-language.md §4.3 (EMIT ALL only with FIRST).
    let err = engine
        .query(
            "events | MATCH ALL SEQUENCE(signup THEN purchase) EMIT ALL",
            &mut database,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("EMIT"),
        "error should mention EMIT: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Time window + EMIT ALL interaction
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn time_window_expiry_with_emit_all() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 2000, 'purchase')",
    );

    // Window of 1000 ns. signup@100 enters the NFA but purchase@2000
    // exceeds the window (2000 - 100 = 1900 > 1000). With EMIT ALL,
    // the expired candidate is emitted as a partial with step_reached=1.
    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 1000ns EMIT ALL",
    );
    assert_eq!(total_rows(&result), 1);
    let steps = step_reached_values(&result);
    assert_eq!(steps, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────────
// WITHOUT negation with multiple candidates
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn without_kills_all_active_candidates() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // MATCH ALL: two signup entries create two candidate tracks.
    // churn at t=250 poisons both candidates.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'signup'), \
         ('u1', 250, 'churn'), \
         ('u1', 300, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH ALL SEQUENCE(signup WITHOUT churn THEN purchase)",
    );
    // Both signup candidates at t=100 and t=200 are poisoned by churn at t=250.
    // No matches should survive.
    assert_eq!(total_rows(&result), 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// MATCH ALL with rebinding
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn match_all_rebinds_after_first_match() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // Three signup-purchase pairs: should produce 3 matches with MATCH ALL.
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase'), \
         ('u1', 300, 'signup'), \
         ('u1', 400, 'purchase'), \
         ('u1', 500, 'signup'), \
         ('u1', 600, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH ALL SEQUENCE(signup THEN purchase)",
    );
    assert_eq!(total_rows(&result), 3);
    let durations = match_durations(&result);
    assert_eq!(durations, vec![Some(100), Some(100), Some(100)]);
}

#[test]
fn match_all_multiple_entities_each_gets_all_matches() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    run(
        &engine,
        &mut database,
        "INSERT INTO events VALUES \
         ('u1', 100, 'signup'), \
         ('u1', 200, 'purchase'), \
         ('u1', 300, 'signup'), \
         ('u1', 400, 'purchase'), \
         ('u2', 100, 'signup'), \
         ('u2', 200, 'purchase')",
    );

    let result = run(
        &engine,
        &mut database,
        "events | MATCH ALL SEQUENCE(signup THEN purchase)",
    );
    // u1: 2 matches, u2: 1 match → total 3.
    assert_eq!(total_rows(&result), 3);
}

// ─────────────────────────────────────────────────────────────────────────────
// Multiple entity sub-batch streaming edge case
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn sub_batch_streaming_preserves_entity_boundary() {
    let db = TempDb::new();
    let (mut database, engine) = setup_events(&db);
    // Insert events from multiple entities in separate batches.
    // The SequenceMatchAdapter must correctly detect entity-id transitions
    // and finalize each entity's state independently.
    for i in 0..10 {
        let entity = format!("u{i}");
        run(
            &engine,
            &mut database,
            &format!(
                "INSERT INTO events VALUES \
                 ('{entity}', 100, 'signup'), \
                 ('{entity}', 200, 'purchase')"
            ),
        );
    }

    let result = run(
        &engine,
        &mut database,
        "events | MATCH FIRST SEQUENCE(signup THEN purchase)",
    );
    // All 10 entities should match.
    assert_eq!(total_rows(&result), 10);
}
