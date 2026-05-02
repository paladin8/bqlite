//! Wave 4 ATTRIBUTE integration coverage (TASK-532).
//!
//! Exercises the axes added by TASK-532 that are not covered by the
//! existing `wave4_advanced_analytics_attribute_cohort_join.rs` suite:
//!
//! | Test | Spec reference |
//! |---|---|
//! | Scan-extension for near-start conversions (A4) | attribute.md §12 |
//! | Multi-type `conversion:` and `touchpoints:` lists (A5) | attribute.md §3 + §10.2 |
//! | `SESSIONIZE \| ATTRIBUTE` composition (A6) | attribute.md §14.1, sessionize.md §12.3 |
//!
//! These are end-to-end tests that exercise the full engine path:
//! BQL text → parser → planner → operators → Arrow output.

use arrow::array::{Array, StringViewArray, TimestampNanosecondArray};
use bqlite_engine::{Database, Engine};
use bqlite_tests::common::TempDb;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Anchor timestamp: 2023-11-14T22:13:20Z in nanoseconds since Unix epoch.
const T0: i64 = 1_700_000_000_000_000_000;
/// One day in nanoseconds.
const D: i64 = 86_400 * 1_000_000_000;
/// One hour in nanoseconds.
const H: i64 = 3_600 * 1_000_000_000;
/// One minute in nanoseconds.
const M: i64 = 60 * 1_000_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture helpers
// ─────────────────────────────────────────────────────────────────────────────

const CREATE_EVENTS: &str = "CREATE TABLE events (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE, \
     channel STRING\
 )";

fn fresh_db(label: &str) -> (TempDb, Database, Engine) {
    let db = TempDb::new();
    let mut database =
        Database::create(db.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));
    (db, database, engine)
}

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

// ─────────────────────────────────────────────────────────────────────────────
// A4: Scan-extension for near-start conversions (attribute.md §12)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn attribute_scan_extension_widens_for_near_start_conversions() {
    // attribute.md §12: when `events LAST <d>` constrains the outer range to
    // `[now - d, now)`, the planner extends the scan backward by `window`
    // so the operator sees touchpoints from the lookback zone
    // `[now - d - window, now - d)` that qualify for conversions near `now - d`.
    //
    // Setup (all timestamps relative to "now"):
    //   - touchpoint at now - 35d  →  5 days before the LAST 30d window
    //   - conversion  at now - 15d  →  inside the LAST 30d window
    //
    // With `window: 30d` the scan extends to `now - 60d`, capturing the
    // touchpoint.  Without extension the scan would stop at `now - 30d` and
    // the touchpoint would be invisible; the operator would emit a LEFT-UNNEST
    // row (null touchpoint_ts) instead of an attributed row.
    //
    // `LAST 30d` is anchored at wall-clock "now" at query time, so we anchor
    // the fixture to SystemTime::now() just before querying — the same pattern
    // used by `first_with_lookback_widens_scan_range` in
    // `wave4_advanced_analytics_event_select.rs`.
    let (_tmp, mut db, engine) = fresh_db("attr-scan-ext");

    let now_ns: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
        .try_into()
        .expect("now fits in i64 ns");

    insert_events(
        &engine,
        &mut db,
        &[
            // Touchpoint 35 days ago — 5 days before the LAST 30d window start.
            // With scan extension (window=30d) the scan reaches now-60d, so this
            // touchpoint is captured and qualifies for the conversion.
            ("alice", now_ns - 35 * D, "ad_click", Some("google")),
            // Conversion 15 days ago — inside the LAST 30d window.
            ("alice", now_ns - 15 * D, "purchase", None),
        ],
    );

    let result = engine
        .query(
            "events LAST 30d | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 30d, \
                 touchpoint_key: channel\
             )",
            &mut db,
        )
        .expect("ATTRIBUTE scan extension query");

    // One attributed row.  If scan extension failed, the ad_click would be
    // outside the (un-extended) scan window and the operator would fall back
    // to emitting a LEFT-UNNEST row (null touchpoint_ts).
    assert_eq!(
        result.row_count(),
        1,
        "scan extension must surface the lookback-zone ad_click"
    );

    let batch = &result.rows[0];
    let tp_ts = batch
        .column_by_name("touchpoint_ts")
        .expect("touchpoint_ts column")
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("touchpoint_ts is TimestampNanosecondArray");
    assert!(
        !tp_ts.is_null(0),
        "touchpoint from the lookback zone must produce a non-null touchpoint_ts; \
         a null here means the scan failed to extend backward"
    );

    let tp_key = batch
        .column_by_name("touchpoint_key")
        .expect("touchpoint_key column")
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("touchpoint_key is StringViewArray");
    assert_eq!(tp_key.value(0), "google");
}

// ─────────────────────────────────────────────────────────────────────────────
// A5: Multi-type conversion: and touchpoints: lists (attribute.md §3, §10.2)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn attribute_multi_type_conversion_and_touchpoints_both_produce_rows() {
    // attribute.md §3: both `conversion:` and `touchpoints:` accept a
    // parenthesized comma-separated list of event types.  §10.2: all listed
    // conversion types share the same forwarded-column namespace; a touchpoint
    // that precedes a conversion of either type qualifies.
    //
    // Fixture:
    //   - ad_click  at T0       (touchpoint type 1)
    //   - email_open at T0+2d   (touchpoint type 2)
    //   - purchase  at T0+5d    (conversion type 1)
    //   - subscription at T0+7d (conversion type 2)
    //
    // For purchase  (T0+5d, window=30d): ad_click and email_open both qualify.
    // For subscription (T0+7d, window=30d): ad_click and email_open both qualify.
    // Total expected rows: 2 + 2 = 4.
    let (_tmp, mut db, engine) = fresh_db("attr-multi-type");

    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "ad_click", Some("google")),
            ("alice", T0 + 2 * D, "email_open", Some("newsletter")),
            ("alice", T0 + 5 * D, "purchase", None),
            ("alice", T0 + 7 * D, "subscription", None),
        ],
    );

    let result = engine
        .query(
            "events | ATTRIBUTE(\
                 conversion: (purchase, subscription), \
                 touchpoints: (ad_click, email_open), \
                 window: 30d, \
                 touchpoint_key: channel\
             )",
            &mut db,
        )
        .expect("ATTRIBUTE multi-type query");

    assert_eq!(
        result.row_count(),
        4,
        "two conversions × two touchpoints = 4 attributed rows"
    );

    // All emitted rows must be attributed (non-null touchpoint_ts), and both
    // touchpoint channels must appear in the output.
    let mut channels: std::collections::HashSet<String> = std::collections::HashSet::new();
    for batch in &result.rows {
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
        for i in 0..batch.num_rows() {
            assert!(
                !tp_ts.is_null(i),
                "every emitted row must be attributed (non-null touchpoint_ts)"
            );
            if !tp_key.is_null(i) {
                channels.insert(tp_key.value(i).to_string());
            }
        }
    }
    assert!(
        channels.contains("google"),
        "ad_click touchpoints must appear in output"
    );
    assert!(
        channels.contains("newsletter"),
        "email_open touchpoints must appear in output"
    );
}

#[test]
fn attribute_multi_type_conversion_each_type_contributes_left_unnest_when_no_touchpoints() {
    // §4.1 LEFT-UNNEST guarantee applies per conversion regardless of type:
    // if no qualifying touchpoints exist for a given conversion, that
    // conversion still emits exactly one LEFT-UNNEST row.
    //
    // Fixture: purchase at T0 (no touchpoints), subscription at T0+D (no touchpoints).
    // Expected: 2 LEFT-UNNEST rows, one per conversion.
    let (_tmp, mut db, engine) = fresh_db("attr-multi-left-unnest");

    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "purchase", None),
            ("alice", T0 + D, "subscription", None),
        ],
    );

    let result = engine
        .query(
            "events | ATTRIBUTE(\
                 conversion: (purchase, subscription), \
                 touchpoints: (ad_click, email_open), \
                 window: 30d, \
                 touchpoint_key: channel\
             )",
            &mut db,
        )
        .expect("ATTRIBUTE multi-type left-unnest");

    assert_eq!(
        result.row_count(),
        2,
        "one LEFT-UNNEST row per conversion, two conversions"
    );

    for batch in &result.rows {
        let tp_ts = batch
            .column_by_name("touchpoint_ts")
            .expect("touchpoint_ts")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(
                tp_ts.is_null(i),
                "LEFT-UNNEST rows must have null touchpoint_ts"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// A6: SESSIONIZE | ATTRIBUTE composition (attribute.md §14.1, sessionize.md §12.3)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn sessionize_then_attribute_composition_runs_and_attributes_across_sessions() {
    // attribute.md §14.1: SESSIONIZE is a valid upstream of ATTRIBUTE.
    // In v1, the ATTRIBUTE deque spans session boundaries — attribution is NOT
    // restricted to within a single session by default.  Users who want
    // within-session attribution must express it explicitly downstream.
    //
    // Fixture (gap=1h):
    //   - ad_click  at T0       → session 1 (first event)
    //   - ad_click  at T0+30m   → session 1 (delta = 30m <= 1h gap, same session)
    //   - purchase  at T0+2h30m → session 2 (delta from T0+30m = 2h > 1h gap)
    //
    // With window=30d, both session-1 ad_clicks are within the lookback window
    // for the session-2 purchase.  Expected: 2 attributed rows (deque spans
    // sessions, both ad_clicks qualify).
    //
    // touchpoint_key uses `event_type` (a NOT NULL column that SESSIONIZE always
    // buffers) rather than `channel` (nullable) to avoid the v1 demand-analysis
    // limitation where nullable columns are null-padded in SESSIONIZE output
    // unless demand propagation has run.  The channel-specific attribution
    // semantics are covered by `attribute_downstream_stats_counts_per_channel`
    // in `wave4_advanced_analytics_attribute_cohort_join.rs`.
    //
    // Note: SESSIONIZE requires the entity-key column to be literally named
    // `entity_id` (tracked as a follow-up issue in sessionize.rs).
    let (_tmp, mut db, engine) = fresh_db("ses-attr");

    insert_events(
        &engine,
        &mut db,
        &[
            // Session 1: two ad_clicks within the 1h gap.
            ("alice", T0, "ad_click", Some("google")),
            ("alice", T0 + 30 * M, "ad_click", Some("facebook")),
            // Session 2: purchase — delta from T0+30m to T0+2h30m is 2h > 1h gap.
            ("alice", T0 + 150 * M, "purchase", None),
        ],
    );

    let result = engine
        .query(
            "events | SESSIONIZE(gap: 1h) \
             | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 30d, \
                 touchpoint_key: event_type\
             )",
            &mut db,
        )
        .expect("SESSIONIZE | ATTRIBUTE composition");

    // Both session-1 ad_clicks must be attributed to the session-2 purchase.
    // A result of 1 would mean only one ad_click was visible; a LEFT-UNNEST
    // row would mean the deque was incorrectly cleared at the session boundary.
    assert_eq!(
        result.row_count(),
        2,
        "both session-1 ad_clicks must be attributed to the session-2 purchase"
    );

    // All emitted rows must be attributed (non-null touchpoint_ts).
    for batch in &result.rows {
        let tp_ts = batch
            .column_by_name("touchpoint_ts")
            .expect("touchpoint_ts")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(
                !tp_ts.is_null(i),
                "ATTRIBUTE deque spans sessions — all rows must be attributed"
            );
        }
    }

    // Both rows' touchpoint_key must be "ad_click" (that's the event_type of
    // the touchpoint events).
    for batch in &result.rows {
        let tp_key = batch
            .column_by_name("touchpoint_key")
            .expect("touchpoint_key")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(
                !tp_key.is_null(i),
                "event_type is NOT NULL so touchpoint_key must be non-null"
            );
            assert_eq!(
                tp_key.value(i),
                "ad_click",
                "touchpoint_key must match the touchpoint event's event_type"
            );
        }
    }
}

#[test]
fn sessionize_then_attribute_pipeline_does_not_error_on_multi_session_entity() {
    // Smoke test: SESSIONIZE | ATTRIBUTE on a multi-session, multi-entity
    // fixture must not panic or return an error.  This exercises the schema
    // plumbing between the two operators (SESSIONIZE appends session_id /
    // session_duration; ATTRIBUTE must handle the enriched input schema).
    let (_tmp, mut db, engine) = fresh_db("ses-attr-smoke");

    insert_events(
        &engine,
        &mut db,
        &[
            // alice: 3 sessions, conversions in sessions 1 and 3.
            ("alice", T0, "ad_click", Some("search")),
            ("alice", T0 + 10 * M, "purchase", None), // session 1
            ("alice", T0 + 2 * H, "ad_click", Some("email")), // session 2
            ("alice", T0 + 4 * H, "purchase", None),  // session 3
            // bob: single session, no conversion.
            ("bob", T0, "ad_click", Some("social")),
            ("bob", T0 + 5 * M, "ad_click", Some("social")),
        ],
    );

    // Use touchpoint_key: event_type (NOT NULL) rather than channel (nullable)
    // to avoid the v1 demand-propagation limitation where nullable columns are
    // null-padded in SESSIONIZE output. This smoke test only asserts row counts;
    // channel-specific attribution semantics are covered by the primary A6 test
    // in wave4_advanced_analytics_attribute_cohort_join.rs.
    let result = engine
        .query(
            "events | SESSIONIZE(gap: 30m) \
             | ATTRIBUTE(\
                 conversion: purchase, \
                 touchpoints: ad_click, \
                 window: 7d, \
                 touchpoint_key: event_type\
             )",
            &mut db,
        )
        .expect("multi-session SESSIONIZE | ATTRIBUTE must not error");

    // alice has 2 conversions; each has ≥1 attributed row.
    // bob has no conversions; he should produce no rows.
    let alice_rows: usize = result
        .rows
        .iter()
        .map(|batch| {
            let eid = batch
                .column_by_name("entity_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            (0..batch.num_rows())
                .filter(|&i| eid.value(i) == "alice")
                .count()
        })
        .sum();
    let bob_rows: usize = result
        .rows
        .iter()
        .map(|batch| {
            let eid = batch
                .column_by_name("entity_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            (0..batch.num_rows())
                .filter(|&i| eid.value(i) == "bob")
                .count()
        })
        .sum();

    assert!(alice_rows >= 2, "alice has 2 conversions → at least 2 rows");
    assert_eq!(bob_rows, 0, "bob has no conversions → no rows");
}
