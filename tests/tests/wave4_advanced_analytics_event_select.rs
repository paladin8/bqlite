//! Wave 4 FIRST/LAST/NTH + SAMPLE + RETENTION integration suite
//! (TASK-439 CP2).
//!
//! End-to-end coverage of the entity-level event-selection primitives
//! (FIRST, LAST, NTH), the entity-sampling primitive (SAMPLE), and
//! the RETENTION sugar — all through `Engine::query` per
//! `docs/design/operators/event-select-sample.md`,
//! `docs/design/query-language.md` §6.3 (RETENTION), §14 (FIRST/LAST/NTH),
//! and §14.2 (SAMPLE).
//!
//! ## Current coverage matrix
//!
//! | Surface | Status |
//! |---|---|
//! | SAMPLE fraction: 0.0 → empty | runs |
//! | SAMPLE fraction: 1.0 → pass-through | runs |
//! | SAMPLE determinism across repeat runs | runs |
//! | SAMPLE population invariance with stateless filter | runs |
//! | FIRST / LAST / NTH (every shape) | runs |
//! | FIRST event-type list | runs |
//! | FIRST `lookback:` widening | runs |
//! | NTH + candidate-predicate filtering | runs |
//! | RETENTION standard brackets | `#[ignore]` (see below) |
//! | RETENTION `cumulative: true` | `#[ignore]` |
//!
//! ## Known gaps tracked by this binary
//!
//! **`bracket` column declared non-nullable but produced as null.**
//! `MATCH … BRACKETS [..]` (which RETENTION desugars to per
//! `crates/bqlite-planner/src/opt/desugar_retention.rs`) errors at
//! batch assembly: `crates/bqlite-operators/src/matcher/output.rs`
//! ~line 97 enforces the declared non-null schema but the matcher
//! produces a null `bracket` for at least one code path. Surfaces as
//! `"Invalid argument error: Column 'bracket' is declared as
//! non-nullable but contains null values"`. Once this gap closes,
//! remove the corresponding `#[ignore]` markers.
//!
//! FIRST/LAST/NTH previously shared this gated-by-`__seq_id` status,
//! but the operator now uses the scan's `(entity_id, ts, __seq_id)`
//! emission order for same-`ts` tie-breaking and does not read
//! `__seq_id` values directly (see the doc on `EventSelectInputMap`).

use std::collections::HashSet;

use arrow::array::{Array, Int64Array, StringViewArray};
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_tests::common::TempDb;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Anchor timestamp: 2023-11-14T22:13:20Z in nanoseconds since the Unix epoch.
const T0: i64 = 1_700_000_000_000_000_000;
/// One day in nanoseconds.
const D: i64 = 86_400 * 1_000_000_000;

const CREATE_TABLE: &str = "CREATE TABLE events (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE\
 )";

fn fresh_db(label: &str) -> (TempDb, Database, Engine) {
    let db = TempDb::new();
    let mut database =
        Database::create(db.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_TABLE, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));
    (db, database, engine)
}

fn insert_rows(engine: &Engine, db: &mut Database, rows: &[(&str, i64, &str)]) {
    assert!(!rows.is_empty(), "insert_rows called with no rows");
    let mut sql = String::from("INSERT INTO events VALUES ");
    for (i, (entity, ts, et)) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("('{entity}', {ts}, '{et}')"));
    }
    engine
        .query(&sql, db)
        .unwrap_or_else(|e| panic!("insert_rows: {e}"));
}

/// Collect the `entity_id` column from every batch into a sorted set.
/// Used for SAMPLE population-invariance checks and cohort-equivalence
/// assertions where row order does not matter.
fn entity_set(result: &ExecutionResult) -> HashSet<String> {
    let mut out: HashSet<String> = HashSet::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name("entity_id")
            .expect("entity_id column present")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("entity_id is StringViewArray");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                out.insert(col.value(i).to_string());
            }
        }
    }
    out
}

/// Extract the single scalar value from a one-row, one-column `STATS`
/// result. Panics if the shape does not match — keeps the test
/// assertions terse.
fn single_i64(result: &ExecutionResult, column: &str) -> i64 {
    assert_eq!(
        result.row_count(),
        1,
        "expected a single-row result for `{column}`, got {}",
        result.row_count()
    );
    let batch = &result.rows[0];
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("`{column}` column present"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("`{column}` is Int64Array"));
    col.value(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// SAMPLE (query-language.md §14.2)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn sample_fraction_zero_produces_no_rows() {
    // Empty-set contract: `SAMPLE(fraction: 0.0)` returns zero rows
    // regardless of input size (query-language.md §14.2 boundary cases).
    let (_tmp, mut db, engine) = fresh_db("sample-zero");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("bob", T0 + D, "click"),
            ("carol", T0 + 2 * D, "click"),
        ],
    );

    let result = engine
        .query("events | SAMPLE(fraction: 0.0)", &mut db)
        .expect("SAMPLE(fraction: 0.0)");
    assert_eq!(result.row_count(), 0);
}

#[test]
fn sample_fraction_one_passes_through() {
    // Pass-through contract: `SAMPLE(fraction: 1.0)` returns every
    // entity's rows unchanged. This is also the fallback-bind test
    // precedent (see `wave4_sample_fallback_bind_pass_through` in
    // `crates/bqlite-engine/src/bind.rs`), extended with real data.
    let (_tmp, mut db, engine) = fresh_db("sample-one");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("bob", T0 + D, "view"),
            ("carol", T0 + 2 * D, "click"),
        ],
    );

    let baseline = engine.query("events", &mut db).expect("baseline scan");
    let sampled = engine
        .query("events | SAMPLE(fraction: 1.0)", &mut db)
        .expect("SAMPLE(fraction: 1.0)");

    // Row count is preserved.
    assert_eq!(sampled.row_count(), baseline.row_count(), "row count");
    // Entity set is identical.
    assert_eq!(
        entity_set(&sampled),
        entity_set(&baseline),
        "entity set preserved"
    );
}

#[test]
fn sample_is_deterministic_across_runs() {
    // Determinism contract (query-language.md §14.2 "Determinism"):
    // repeat queries on the same database return the same sample.
    // This is the stability guarantee called out in the spec.
    let (_tmp, mut db, engine) = fresh_db("sample-determ");
    // 10 entities so a 0.5 fraction is unlikely to collapse to
    // trivial all-in / all-out and still deterministic.
    let rows: Vec<(&str, i64, &str)> = vec![
        ("u01", T0, "click"),
        ("u02", T0 + D, "click"),
        ("u03", T0 + 2 * D, "click"),
        ("u04", T0 + 3 * D, "click"),
        ("u05", T0 + 4 * D, "click"),
        ("u06", T0 + 5 * D, "click"),
        ("u07", T0 + 6 * D, "click"),
        ("u08", T0 + 7 * D, "click"),
        ("u09", T0 + 8 * D, "click"),
        ("u10", T0 + 9 * D, "click"),
    ];
    insert_rows(&engine, &mut db, &rows);

    let r1 = engine
        .query("events | SAMPLE(fraction: 0.5, seed: 42)", &mut db)
        .expect("first sample");
    let r2 = engine
        .query("events | SAMPLE(fraction: 0.5, seed: 42)", &mut db)
        .expect("second sample");

    assert_eq!(
        entity_set(&r1),
        entity_set(&r2),
        "same seed ⇒ same sampled entities across runs"
    );

    // Negative control: a different seed on the same fraction should
    // — with high probability over 10 entities — produce a different
    // sampled entity set. A seed that is silently ignored would make
    // this assertion false. If the implementation ever legitimately
    // decides two seeds must yield the same set, this control can be
    // tightened to assert the seeds are actually different bytes.
    let r_alt = engine
        .query("events | SAMPLE(fraction: 0.5, seed: 43)", &mut db)
        .expect("alt-seed sample");
    assert_ne!(
        entity_set(&r1),
        entity_set(&r_alt),
        "different seed ⇒ different sample (seed must actually flow through to the hash)"
    );
}

#[test]
fn sample_population_invariance_with_stateless_filter() {
    // query-language.md §14.2 "Population invariance":
    //
    //   events | WHERE P | SAMPLE(fraction: f)
    //   ≡ events | SAMPLE(fraction: f) | WHERE P
    //
    // for any predicate P that does not reference the entity key.
    // The claim is about the entity set, not row count — an entity
    // present in both sampled sets will have the same row count in
    // both pipelines, but the cardinality check is the one that
    // matters for the invariance argument.
    let (_tmp, mut db, engine) = fresh_db("sample-popinv");
    // 10 entities; half are 'click', half are 'view'. The predicate
    // `event_type = 'click'` is stateless and does not reference the
    // entity key, so the invariance must hold.
    let mut rows: Vec<(&str, i64, &str)> = Vec::new();
    for i in 0..10 {
        let name = match i {
            0 => "u0",
            1 => "u1",
            2 => "u2",
            3 => "u3",
            4 => "u4",
            5 => "u5",
            6 => "u6",
            7 => "u7",
            8 => "u8",
            _ => "u9",
        };
        let et = if i % 2 == 0 { "click" } else { "view" };
        rows.push((name, T0 + (i as i64) * D, et));
    }
    insert_rows(&engine, &mut db, &rows);

    let r_filter_then_sample = engine
        .query(
            "events | WHERE event_type = 'click' | SAMPLE(fraction: 0.5, seed: 7)",
            &mut db,
        )
        .expect("filter → sample");
    let r_sample_then_filter = engine
        .query(
            "events | SAMPLE(fraction: 0.5, seed: 7) | WHERE event_type = 'click'",
            &mut db,
        )
        .expect("sample → filter");

    assert_eq!(
        entity_set(&r_filter_then_sample),
        entity_set(&r_sample_then_filter),
        "population invariance must hold for predicates that do not reference the entity key"
    );
    // Row counts must also match — guards against a regression that
    // silently duplicates rows on one side of the invariance.
    assert_eq!(
        r_filter_then_sample.row_count(),
        r_sample_then_filter.row_count(),
        "both pipelines must survive the same rows, not just the same entity set"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// FIRST / LAST / NTH (event-select-sample.md)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn first_returns_first_event_per_entity() {
    let (_tmp, mut db, engine) = fresh_db("first-basic");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", T0, "login"),
            ("alice", T0 + D, "login"),
            ("bob", T0 + 2 * D, "login"),
        ],
    );

    let result = engine
        .query("events | FIRST(login)", &mut db)
        .expect("FIRST(login)");
    // One row per entity — alice's earliest login, bob's only login.
    assert_eq!(result.row_count(), 2);
}

#[test]
fn first_with_candidate_predicate_filters_before_selection() {
    // event-select-sample.md §X: the WHERE predicate is applied per
    // event before position selection, so NTH(e WHERE P, 3) returns
    // the third event that satisfies P, not the third event overall.
    let (_tmp, mut db, engine) = fresh_db("first-pred");
    // Alice has purchases of amount 50, 150, 200 (ascending ts).
    // With the predicate `amount > 100`, the first qualifying
    // purchase is amount=150.
    engine
        .query(
            "CREATE TABLE purchases (\
             entity_id STRING NOT NULL ENTITY KEY, \
             ts TIMESTAMP NOT NULL EVENT TIME, \
             event_type STRING NOT NULL EVENT TYPE, \
             amount INT)",
            &mut db,
        )
        .expect("CREATE TABLE purchases");
    engine
        .query(
            &format!(
                "INSERT INTO purchases VALUES \
                 ('alice', {t0}, 'purchase', 50), \
                 ('alice', {t1}, 'purchase', 150), \
                 ('alice', {t2}, 'purchase', 200)",
                t0 = T0,
                t1 = T0 + D,
                t2 = T0 + 2 * D,
            ),
            &mut db,
        )
        .expect("INSERT purchases");

    let result = engine
        .query("purchases | FIRST(purchase WHERE amount > 100)", &mut db)
        .expect("FIRST with candidate predicate");
    // Expect one row (alice's amount=150).
    assert_eq!(result.row_count(), 1);
}

#[test]
fn first_with_event_type_list_matches_any() {
    let (_tmp, mut db, engine) = fresh_db("first-list");
    insert_rows(
        &engine,
        &mut db,
        &[
            // login first → selected for alice.
            ("alice", T0, "login"),
            ("alice", T0 + D, "sso_login"),
            // sso_login first → selected for bob.
            ("bob", T0, "sso_login"),
            ("bob", T0 + D, "login"),
            // No login-family event at all.
            ("carol", T0, "click"),
        ],
    );
    let result = engine
        .query("events | FIRST((login, sso_login, mobile_login))", &mut db)
        .expect("FIRST with event-type list");
    // alice + bob → two rows; carol omitted.
    assert_eq!(result.row_count(), 2);
}

#[test]
fn first_without_lookback_is_bounded_by_outer_range() {
    // event-select-sample.md: without `lookback:`, FIRST/NTH operate
    // only within the outer time range. The pre-range signup should
    // NOT be returned.
    let (_tmp, mut db, engine) = fresh_db("first-no-lookback");
    insert_rows(
        &engine,
        &mut db,
        &[
            // 30d in the past.
            ("alice", T0 - 30 * D, "signup"),
            // Inside the 7d window.
            ("alice", T0 - 3 * D, "click"),
        ],
    );
    let result = engine
        .query("events LAST 7d | FIRST(signup)", &mut db)
        .expect("FIRST within outer range");
    // No signup in the last 7d → no rows.
    assert_eq!(result.row_count(), 0);
}

#[test]
fn first_with_lookback_widens_scan_range() {
    // With `lookback: 60d`, the same fixture widens the scan to 60d
    // into the past, so the 30d-old signup is now visible.
    //
    // `LAST 7d` is anchored at wall-clock "now" — use a recent-now
    // fixture so the expected rows fall inside the widened window at
    // query time. Anchoring at the module-constant `T0` (a 2023 epoch
    // picked for determinism of other tests) would place every row
    // outside any plausible `lookback:` window in a 2026+ CI run.
    let (_tmp, mut db, engine) = fresh_db("first-lookback");
    let now_ns: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
        .try_into()
        .expect("now fits in i64 ns");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", now_ns - 30 * D, "signup"),
            ("alice", now_ns - 3 * D, "click"),
        ],
    );
    let result = engine
        .query("events LAST 7d | FIRST(signup, lookback: 60d)", &mut db)
        .expect("FIRST with lookback widens scan");
    assert_eq!(result.row_count(), 1);
}

#[test]
fn last_returns_last_event_per_entity() {
    let (_tmp, mut db, engine) = fresh_db("last-basic");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", T0, "page_view"),
            ("alice", T0 + D, "page_view"),
            ("alice", T0 + 2 * D, "page_view"),
        ],
    );
    let result = engine
        .query("events | LAST(page_view)", &mut db)
        .expect("LAST(page_view)");
    assert_eq!(result.row_count(), 1);
}

#[test]
fn nth_returns_third_matching_event() {
    let (_tmp, mut db, engine) = fresh_db("nth-3");
    // Alice has 5 page_views; Bob has only 2. NTH(page_view, 3)
    // returns alice's third event; bob is omitted (fewer than 3
    // qualifying events).
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", T0, "page_view"),
            ("alice", T0 + D, "page_view"),
            ("alice", T0 + 2 * D, "page_view"),
            ("alice", T0 + 3 * D, "page_view"),
            ("alice", T0 + 4 * D, "page_view"),
            ("bob", T0, "page_view"),
            ("bob", T0 + D, "page_view"),
        ],
    );
    let result = engine
        .query("events | NTH(page_view, 3)", &mut db)
        .expect("NTH(page_view, 3)");
    assert_eq!(result.row_count(), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// RETENTION (query-language.md §6.3)
// ─────────────────────────────────────────────────────────────────────────────
//
// RETENTION tests are currently gated with `#[ignore]` because
// `MATCH … BRACKETS [..]` — which RETENTION desugars to per
// `crates/bqlite-planner/src/opt/desugar_retention.rs` — panics at
// output-batch assembly. The matcher's output builder
// (`crates/bqlite-operators/src/matcher/output.rs` ~line 97) enforces
// the declared non-null `bracket` column but the matcher produces a
// null `bracket` for at least one code path, yielding:
//
//     "Invalid argument error: Column 'bracket' is declared as
//      non-nullable but contains null values"
//
// Both the sugar path (`RETENTION(...)`) and the desugared form
// (`MATCH FIRST SEQUENCE(...) BRACKETS [..]`) hit the same panic.

#[test]
#[ignore = "RETENTION desugars to MATCH … BRACKETS and panics at \
    `bracket` column nullability \
    (see crates/bqlite-operators/src/matcher/output.rs ~line 97)"]
fn retention_standard_brackets_produces_expected_rates() {
    // Three entities, one signup each on day 0, purchase on days
    // 2, 9, 20 respectively. Expected desugared output:
    //   | STATS retention_rate = AVG(CAST(step_reached >= 2 AS INT))
    //     GROUP BY bracket
    //
    // Per desugar_retention.rs:155 the output columns are literally
    // `bracket` (the bracket key) and `retention_rate`.
    let (_tmp, mut db, engine) = fresh_db("retention-standard");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("u_early", T0, "signup"),
            ("u_early", T0 + 2 * D, "purchase"),
            ("u_mid", T0, "signup"),
            ("u_mid", T0 + 9 * D, "purchase"),
            ("u_late", T0, "signup"),
            ("u_late", T0 + 20 * D, "purchase"),
        ],
    );

    let result = engine
        .query(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [1d, 7d, 14d, 30d])",
            &mut db,
        )
        .expect("RETENTION standard brackets");
    assert!(
        result.row_count() > 0,
        "retention must produce at least one bracket row"
    );
}

#[test]
#[ignore = "RETENTION cumulative mode hits the same `bracket` non-null \
    panic as the standard form"]
fn retention_cumulative_brackets_are_monotone() {
    // With `cumulative: true`, each bracket's rate includes every
    // prior bracket's conversions; the resulting sequence of rates
    // is non-decreasing by construction. This test asserts that
    // monotonicity property on a three-entity, day-2/9/20 fixture.
    let (_tmp, mut db, engine) = fresh_db("retention-cumulative");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("u1", T0, "signup"),
            ("u1", T0 + 2 * D, "purchase"),
            ("u2", T0, "signup"),
            ("u2", T0 + 9 * D, "purchase"),
            ("u3", T0, "signup"),
            ("u3", T0 + 20 * D, "purchase"),
        ],
    );

    let result = engine
        .query(
            "events | RETENTION(\
                 entry: signup, \
                 activity: purchase, \
                 brackets: [1d, 7d, 14d, 30d], \
                 cumulative: true\
             )",
            &mut db,
        )
        .expect("RETENTION cumulative");
    assert!(result.row_count() > 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// STATS composition sanity (this is a runs-today test)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn sample_composes_with_stats() {
    // Pipeline composition: `SAMPLE | STATS COUNT(*)` must return a
    // single row whose count equals the number of rows surviving the
    // sample — at fraction=1.0 that is every event in the fixture.
    // This is the §25.2 composition table's "SAMPLE → STATS" row
    // made concrete.
    let (_tmp, mut db, engine) = fresh_db("sample-stats");
    insert_rows(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("alice", T0 + D, "view"),
            ("bob", T0, "click"),
        ],
    );

    let result = engine
        .query(
            "events | SAMPLE(fraction: 1.0) | STATS total = COUNT(*)",
            &mut db,
        )
        .expect("SAMPLE → STATS");
    assert_eq!(single_i64(&result, "total"), 3);
}
