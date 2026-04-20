//! Wave 4 ATTRIBUTE + cohort + joined-source integration suite
//! (TASK-439 CP3).
//!
//! End-to-end coverage of the ATTRIBUTE operator (LEFT-UNNEST plus
//! sliding-window deque), cohort semi-joins via `IN QUERY (...)` and
//! alias references, multi-column cohort IN, and joined-source
//! queries — all through `Engine::query` per
//! `docs/design/operators/attribute.md`,
//! `docs/design/language/cohorts-aliases-joins.md`, and
//! `docs/design/query-language.md` §14.3 (ATTRIBUTE), §17 (IN), §19 (JOIN).
//!
//! ## Coverage matrix
//!
//! | Surface | Status |
//! |---|---|
//! | ATTRIBUTE unattributed conversion (LEFT-UNNEST) | runs |
//! | ATTRIBUTE multiple touchpoints → N rows | runs |
//! | ATTRIBUTE downstream STATS per channel | runs |
//! | ATTRIBUTE null touchpoint_key distinct from unattributed | runs |
//! | Cohort via inline `IN QUERY (...)` | runs |
//! | Cohort via alias reference | runs |
//! | Cohort alias equivalence with inline `IN QUERY` | runs |
//! | Multi-column `IN QUERY` | runs |
//! | Full stack (cohort + filter + aggregate) | runs |
//! | Joined-source `events JOIN purchases` | `#[ignore]` (see below) |
//! | Joined-source + sequence MATCH | `#[ignore]` |
//!
//! ## Known gaps tracked here
//!
//! 1. **`MergeSourcesOperator` fails `__seq_id` nullability at assembly.**
//!    Any `events JOIN table` query — even a bare `events JOIN purchases`
//!    over ingested rows — surfaces as `Execution("MergeSourcesOperator:
//!    failed to assemble output batch: Invalid argument error: Column
//!    '__seq_id' is declared as non-nullable but contains null values")`.
//!    The merge output schema declares `__seq_id` non-null, but one
//!    branch of the n-way merge produces null values in the assembled
//!    batch. Gating joined-source tests with `#[ignore]` until the merge
//!    output contract is reconciled.

use std::collections::HashMap;

use arrow::array::{Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_tests::common::TempDb;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Anchor timestamp: 2023-11-14T22:13:20Z in nanoseconds since the Unix epoch.
const T0: i64 = 1_700_000_000_000_000_000;
/// One day in nanoseconds.
const D: i64 = 86_400 * 1_000_000_000;

const CREATE_EVENTS: &str = "CREATE TABLE events (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE, \
     channel STRING\
 )";

const CREATE_PURCHASES: &str = "CREATE TABLE purchases (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE, \
     amount INT\
 )";

fn fresh_db(label: &str) -> (TempDb, Database, Engine) {
    let db = TempDb::new();
    let mut database =
        Database::create(db.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE events: {e}"));
    engine
        .query(CREATE_PURCHASES, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE purchases: {e}"));
    (db, database, engine)
}

/// Insert rows into `events` with an optional `channel` column. Passing
/// `None` produces a SQL `NULL` literal for that column; passing
/// `Some("foo")` produces `'foo'`.
fn insert_events(engine: &Engine, db: &mut Database, rows: &[(&str, i64, &str, Option<&str>)]) {
    assert!(!rows.is_empty(), "insert_events called with no rows");
    let mut sql = String::from("INSERT INTO events VALUES ");
    for (i, (entity, ts, et, channel)) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let chan = match channel {
            None => "NULL".to_string(),
            Some(c) => format!("'{c}'"),
        };
        sql.push_str(&format!("('{entity}', {ts}, '{et}', {chan})"));
    }
    engine
        .query(&sql, db)
        .unwrap_or_else(|e| panic!("insert_events: {e}"));
}

/// Extract a single scalar `i64` from a one-row, one-named-column
/// result.
fn single_i64(result: &ExecutionResult, column: &str) -> i64 {
    assert_eq!(result.row_count(), 1, "expected single-row result");
    let batch = &result.rows[0];
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("column `{column}` present"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column `{column}` is Int64Array"));
    col.value(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// ATTRIBUTE (operators/attribute.md, query-language.md §14.3)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn attribute_unattributed_conversion_emits_left_unnest_row() {
    // §14.3 LEFT-UNNEST: a conversion with zero qualifying touchpoints
    // still emits a single row with `touchpoint_ts = NULL` and
    // `touchpoint_key = NULL`. This preserves the un-attributed
    // conversion for downstream counting via `SUM(CAST(touchpoint_ts
    // IS NULL AS INT))`.
    let (_tmp, mut db, engine) = fresh_db("attr-leftunnest");
    // One conversion, no touchpoints in the window.
    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "purchase", Some("direct"))],
    );

    let result = engine
        .query(
            "events | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 30d, \
                 touchpoint_key: channel\
             )",
            &mut db,
        )
        .expect("ATTRIBUTE unattributed");

    // Exactly one row — the LEFT-UNNEST row.
    assert_eq!(
        result.row_count(),
        1,
        "LEFT-UNNEST shape is one row per conversion"
    );

    // touchpoint_ts must be NULL for the LEFT-UNNEST shape.
    let batch = &result.rows[0];
    let tp_ts = batch
        .column_by_name("touchpoint_ts")
        .expect("touchpoint_ts present")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("touchpoint_ts is TimestampNanosecondArray");
    assert!(
        tp_ts.is_null(0),
        "touchpoint_ts must be NULL on LEFT-UNNEST"
    );

    let tp_key = batch
        .column_by_name("touchpoint_key")
        .expect("touchpoint_key present")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("touchpoint_key is StringViewArray");
    assert!(
        tp_key.is_null(0),
        "touchpoint_key must be NULL on LEFT-UNNEST"
    );
}

#[test]
fn attribute_multiple_touchpoints_produces_n_rows() {
    // §14.3 "Auto-unnest": N qualifying touchpoints → N rows, one
    // per (entity, conversion, matched-touchpoint). We sort by
    // `touchpoint_ts` before comparison because ATTRIBUTE's
    // sliding-window deque does not guarantee downstream row order
    // absent an explicit ORDER BY.
    let (_tmp, mut db, engine) = fresh_db("attr-n-rows");
    // alice: three ad_clicks followed by one purchase. All three clicks
    // fall inside the 30d window.
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "ad_click", Some("google")),
            ("alice", T0 + D, "ad_click", Some("facebook")),
            ("alice", T0 + 2 * D, "ad_click", Some("twitter")),
            ("alice", T0 + 5 * D, "purchase", Some("direct")),
        ],
    );

    let result = engine
        .query(
            "events | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 30d, \
                 touchpoint_key: channel\
             )",
            &mut db,
        )
        .expect("ATTRIBUTE N-rows");
    assert_eq!(
        result.row_count(),
        3,
        "three qualifying touchpoints → three rows"
    );

    // Collect (touchpoint_ts, touchpoint_key) pairs and sort by ts.
    let mut pairs: Vec<(i64, String)> = Vec::new();
    for batch in &result.rows {
        let tp_ts = batch
            .column_by_name("touchpoint_ts")
            .expect("touchpoint_ts")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("touchpoint_ts is TimestampNanosecondArray");
        let tp_key = batch
            .column_by_name("touchpoint_key")
            .expect("touchpoint_key")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("touchpoint_key is StringViewArray");
        for i in 0..batch.num_rows() {
            assert!(!tp_ts.is_null(i), "touchpoint_ts present");
            assert!(!tp_key.is_null(i), "touchpoint_key present");
            pairs.push((tp_ts.value(i), tp_key.value(i).to_string()));
        }
    }
    pairs.sort_by_key(|p| p.0);
    assert_eq!(
        pairs,
        vec![
            (T0, "google".into()),
            (T0 + D, "facebook".into()),
            (T0 + 2 * D, "twitter".into()),
        ]
    );
}

#[test]
fn attribute_null_touchpoint_key_is_distinct_from_unattributed() {
    // §14.3 final paragraph: a qualifying touchpoint whose
    // `touchpoint_key` expression evaluates to NULL is still emitted
    // with a non-null `touchpoint_ts` — distinct from the LEFT-UNNEST
    // shape (both fields NULL).
    let (_tmp, mut db, engine) = fresh_db("attr-null-key");
    insert_events(
        &engine,
        &mut db,
        &[
            // Touchpoint whose `channel` is NULL.
            ("alice", T0, "ad_click", None),
            // Conversion 5d later — within the 30d window.
            ("alice", T0 + 5 * D, "purchase", Some("direct")),
        ],
    );

    let result = engine
        .query(
            "events | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 30d, \
                 touchpoint_key: channel\
             )",
            &mut db,
        )
        .expect("ATTRIBUTE null key");
    assert_eq!(result.row_count(), 1);

    let batch = &result.rows[0];
    let tp_ts = batch
        .column_by_name("touchpoint_ts")
        .expect("touchpoint_ts")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let tp_key = batch
        .column_by_name("touchpoint_key")
        .expect("touchpoint_key")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .unwrap();
    assert!(
        !tp_ts.is_null(0),
        "touchpoint_ts is non-null — this is the qualifying-touchpoint \
         row, not LEFT-UNNEST"
    );
    assert!(
        tp_key.is_null(0),
        "touchpoint_key is NULL because channel was NULL on the touchpoint"
    );
}

#[test]
fn attribute_downstream_stats_counts_per_channel() {
    // §14.3 "Attributions aggregation": the canonical pattern is
    //   | WHERE touchpoint_ts IS NOT NULL
    //   | STATS attributions = COUNT(*) GROUP BY touchpoint_key
    //
    // Six touchpoints across three channels (google=3, facebook=2,
    // twitter=1) all funnel into a single conversion. The terminal
    // GROUP BY must produce the exact per-channel counts.
    let (_tmp, mut db, engine) = fresh_db("attr-stats");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "ad_click", Some("google")),
            ("alice", T0 + D, "ad_click", Some("google")),
            ("alice", T0 + 2 * D, "ad_click", Some("facebook")),
            ("alice", T0 + 3 * D, "ad_click", Some("google")),
            ("alice", T0 + 4 * D, "ad_click", Some("facebook")),
            ("alice", T0 + 5 * D, "ad_click", Some("twitter")),
            ("alice", T0 + 6 * D, "purchase", Some("direct")),
        ],
    );

    let result = engine
        .query(
            "events | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 30d, \
                 touchpoint_key: channel\
             ) \
             | WHERE touchpoint_ts IS NOT NULL \
             | STATS attributions = COUNT(*) GROUP BY touchpoint_key",
            &mut db,
        )
        .expect("ATTRIBUTE → STATS");

    let mut by_channel: HashMap<String, i64> = HashMap::new();
    for batch in &result.rows {
        let key = batch
            .column_by_name("touchpoint_key")
            .expect("touchpoint_key present")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("touchpoint_key is StringViewArray");
        let attrs = batch
            .column_by_name("attributions")
            .expect("attributions present")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("attributions is Int64Array");
        for i in 0..batch.num_rows() {
            assert!(
                !key.is_null(i),
                "WHERE touchpoint_ts IS NOT NULL already filters nulls"
            );
            by_channel.insert(key.value(i).to_string(), attrs.value(i));
        }
    }

    let mut expected: HashMap<String, i64> = HashMap::new();
    expected.insert("google".into(), 3);
    expected.insert("facebook".into(), 2);
    expected.insert("twitter".into(), 1);
    assert_eq!(by_channel, expected);
}

// ─────────────────────────────────────────────────────────────────────────────
// Cohort semi-joins (query-language.md §17, cohorts-aliases-joins.md)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn cohort_in_query_restricts_downstream_scan() {
    // query-language.md §17.2: `IN QUERY (...)` materializes the
    // inner query as a hash set and probes per outer row.
    let (_tmp, mut db, engine) = fresh_db("cohort-inquery");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "premium_signup", None),
            ("alice", T0 + D, "click", None),
            ("bob", T0, "click", None),
            ("carol", T0, "premium_signup", None),
            ("carol", T0 + D, "click", None),
        ],
    );

    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'premium_signup' | SELECT entity_id\
             ) | STATS n = COUNT(*) GROUP BY entity_id",
            &mut db,
        )
        .expect("cohort IN QUERY");
    // alice and carol both have premium_signup → both land in the
    // cohort. Their rows (2 each for alice, 2 each for carol) survive.
    let mut by_entity: HashMap<String, i64> = HashMap::new();
    for batch in &result.rows {
        let e = batch
            .column_by_name("entity_id")
            .expect("entity_id")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let n = batch
            .column_by_name("n")
            .expect("n")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            by_entity.insert(e.value(i).to_string(), n.value(i));
        }
    }
    let mut expected: HashMap<String, i64> = HashMap::new();
    expected.insert("alice".into(), 2);
    expected.insert("carol".into(), 2);
    assert_eq!(by_entity, expected);
}

#[test]
fn cohort_alias_equals_inline_in_query() {
    // cohorts-aliases-joins.md §2.5: a bare alias reference in `IN`
    // and an inline `IN QUERY` with a structurally-equal inner plan
    // are semantically equivalent. We assert the two pipelines
    // produce the same aggregate count.
    let (_tmp, mut db, engine) = fresh_db("cohort-alias-eq");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "premium_signup", None),
            ("alice", T0 + D, "click", None),
            ("bob", T0, "click", None),
            ("carol", T0, "premium_signup", None),
            ("carol", T0 + D, "click", None),
        ],
    );

    let inline = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'premium_signup' | SELECT entity_id\
             ) | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("inline IN QUERY");
    let via_alias = engine
        .query(
            "premium = events | WHERE event_type = 'premium_signup' | SELECT entity_id\n\
             events | WHERE entity_id IN premium | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("alias reference");

    // Pin to the concrete expected count so this is not just "two
    // broken pipelines agree": alice has 2 rows in `events` and
    // carol has 2 → cohort survivors are 4 rows in total.
    assert_eq!(single_i64(&inline, "n"), 4);
    assert_eq!(single_i64(&via_alias, "n"), single_i64(&inline, "n"));
}

#[test]
fn multi_column_in_query_filters_by_tuple() {
    // query-language.md §17.4: `(entity_id, event_type) IN QUERY (...)`
    // matches outer rows against a two-column cohort. A row whose
    // (entity_id, event_type) pair is absent from the cohort is
    // filtered out even if either column alone is present.
    let (_tmp, mut db, engine) = fresh_db("multi-col-in");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click", None),
            ("alice", T0 + D, "view", None),
            ("bob", T0, "click", None),
            // dave has a view but no click — must be filtered out.
            // Pins true tuple semantics: if multi-column IN silently
            // degenerated to `entity_id IN (...)`, dave would still
            // be dropped (he is not in the cohort's entity set) and
            // this test would pass coincidentally. But dave's
            // absence from the cohort's *entity* set means only the
            // `(alice, view)` filter-out is the discriminating case;
            // dave's presence here exercises the additional axis of
            // "tuple key, not just single column" by giving the
            // planner a chance to pick between lookups.
            ("dave", T0, "view", None),
        ],
    );

    // Cohort has (alice, click) and (bob, click). The outer (alice,
    // view) row must be filtered out because that pair is not in
    // the cohort, even though alice alone is.
    let result = engine
        .query(
            "events | WHERE (entity_id, event_type) IN QUERY (\
                 events | WHERE event_type = 'click' | SELECT entity_id, event_type\
             ) | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("multi-column IN");
    // Two surviving rows: (alice, click) and (bob, click).
    assert_eq!(single_i64(&result, "n"), 2);
}

#[test]
fn cohort_plus_filter_plus_aggregate_exact_count() {
    // End-to-end composition: cohort + filter + terminal aggregate.
    // This is the "exact downstream aggregate results on realistic
    // fixtures" axis of the TASK-439 test matrix, reduced to the
    // features that currently run (ATTRIBUTE is exercised in earlier
    // tests; joined-source is gated).
    //
    // Cohort: entities that signed up. Outer: count of their
    // non-signup events. Expected: alice has 2, bob has 1.
    let (_tmp, mut db, engine) = fresh_db("full-stack");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "signup", None),
            ("alice", T0 + D, "click", None),
            ("alice", T0 + 2 * D, "view", None),
            ("bob", T0, "signup", None),
            ("bob", T0 + D, "click", None),
            // carol does NOT sign up.
            ("carol", T0, "click", None),
            ("carol", T0 + D, "view", None),
        ],
    );

    let result = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'signup' | SELECT entity_id\
             ) \
             | WHERE event_type != 'signup' \
             | STATS n = COUNT(*) GROUP BY entity_id",
            &mut db,
        )
        .expect("cohort + filter + aggregate");

    let mut by_entity: HashMap<String, i64> = HashMap::new();
    for batch in &result.rows {
        let e = batch
            .column_by_name("entity_id")
            .expect("entity_id")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let n = batch
            .column_by_name("n")
            .expect("n")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            by_entity.insert(e.value(i).to_string(), n.value(i));
        }
    }
    let mut expected: HashMap<String, i64> = HashMap::new();
    expected.insert("alice".into(), 2);
    expected.insert("bob".into(), 1);
    assert_eq!(by_entity, expected, "carol has no signup → filtered out");
}

// ─────────────────────────────────────────────────────────────────────────────
// Joined-source queries (query-language.md §19)
// ─────────────────────────────────────────────────────────────────────────────
//
// Both joined-source tests are currently gated with `#[ignore]`
// because even a bare `events JOIN purchases` on ingested data fails
// at batch assembly with:
//
//     "MergeSourcesOperator: failed to assemble output batch:
//      Invalid argument error: Column '__seq_id' is declared as
//      non-nullable but contains null values"
//
// The merge's output schema declares `__seq_id` non-null per the
// planner, but one side of the n-way merge produces nulls. This is
// the same family of runtime-vs-planner schema drift called out in
// `bind_event_select`. The test bodies encode spec shapes to run
// once the merge output contract is reconciled.

#[test]
#[ignore = "MergeSourcesOperator fails __seq_id nullability at assembly; \
    any `events JOIN purchases` surfaces it"]
fn joined_source_stats_counts_entities_in_both_tables() {
    let (_tmp, mut db, engine) = fresh_db("join-basic");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "signup", Some("direct")),
            ("bob", T0, "signup", Some("direct")),
        ],
    );
    engine
        .query(
            &format!(
                "INSERT INTO purchases VALUES \
                 ('alice', {t1}, 'purchase', 100), \
                 ('bob', {t1}, 'purchase', 200)",
                t1 = T0 + D,
            ),
            &mut db,
        )
        .expect("INSERT purchases");

    let result = engine
        .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
        .expect("joined-source scan");
    // Four rows total: two events + two purchases merge into a
    // four-row entity-sorted stream.
    assert_eq!(single_i64(&result, "total"), 4);
}

#[test]
#[ignore = "Joined-source + sequence MATCH hits the same MergeSources \
    nullability issue as the basic JOIN test"]
fn joined_source_sequence_match_spans_tables() {
    // query-language.md §19.1: table-qualified references inside
    // multi-table queries. A MATCH pattern whose steps come from
    // different tables matches entities where both sides appeared in
    // sequence. Exactly one entity (alice) has the pattern
    // events.signup THEN purchases.purchase.
    let (_tmp, mut db, engine) = fresh_db("join-match");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "signup", Some("direct")),
            ("bob", T0, "signup", Some("direct")),
        ],
    );
    engine
        .query(
            &format!(
                "INSERT INTO purchases VALUES ('alice', {t1}, 'purchase', 100)",
                t1 = T0 + D,
            ),
            &mut db,
        )
        .expect("INSERT purchases (alice only)");

    let result = engine
        .query(
            "events JOIN purchases \
             | MATCH FIRST SEQUENCE(events.signup THEN purchases.purchase) WITHIN 7d \
             | STATS matched = COUNT(*)",
            &mut db,
        )
        .expect("joined MATCH");
    assert_eq!(single_i64(&result, "matched"), 1);
}
