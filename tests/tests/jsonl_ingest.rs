//! JSONL ingest acceptance test (TASK-410).
//!
//! Exercises the full Wave 4 JSONL ingest path end-to-end:
//!
//! 1. `INSERT FROM ... WITH (format: 'jsonl')` — flat JSON objects
//! 2. Nested `"properties"` sub-object flattening
//! 3. Column mapping (`map: (...)`)
//! 4. Query pipeline after ingest verifies real rows from disk
//! 5. Empty-file no-op
//! 6. Row-numbered error diagnostics
//!
//! The `purchases` schema is shared with the Wave 2 CSV acceptance test
//! so the two ingest paths are exercised against the same schema and
//! counting logic.

use bqlite_engine::{Database, Engine};
use bqlite_tests::common::TempDb;
use bqlite_tests::jsonl::{self, FixtureConfig};

/// Scale for the CI gate: enough rows to exercise multi-entity
/// partitioning and the purchase/refund split without slowing down CI.
const CI_ROW_COUNT: u64 = 1_000;

/// Scale for the optional large-scale variant (behind `#[ignore]`).
const LARGE_ROW_COUNT: u64 = 1_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn setup_purchases(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();
    engine
        .query(jsonl::PURCHASES_CREATE_TABLE, &mut database)
        .expect("CREATE TABLE purchases");
    (database, engine)
}

fn manifest_row_count(db: &Database, table: &str) -> u64 {
    db.manifest().tables[table]
        .windows
        .iter()
        .flat_map(|w| &w.shards)
        .flatten()
        .map(|seg| seg.row_count)
        .sum()
}

// ─────────────────────────────────────────────────────────────────────────────
// Flat JSONL ingest
// ─────────────────────────────────────────────────────────────────────────────

fn flat_ingest_at_scale(row_count: u64) {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let config = FixtureConfig::new(row_count);
    let jsonl_path =
        jsonl::write_fixture_file(&config, db.path(), "purchases.jsonl").expect("write fixture");

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
        jsonl_path.display()
    );
    let result = engine
        .query(&sql, &mut database)
        .expect("INSERT FROM JSONL");
    assert!(result.is_empty(), "INSERT produces no rows");

    assert_eq!(
        manifest_row_count(&database, "purchases"),
        row_count,
        "manifest must contain exactly {row_count} rows"
    );
}

#[test]
fn flat_jsonl_ingest_ci_scale() {
    flat_ingest_at_scale(CI_ROW_COUNT);
}

#[test]
#[ignore]
fn flat_jsonl_ingest_large_scale() {
    flat_ingest_at_scale(LARGE_ROW_COUNT);
}

// ─────────────────────────────────────────────────────────────────────────────
// Nested-properties flattening
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn nested_properties_ingest_matches_flat_row_count() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let row_count = CI_ROW_COUNT;
    let config = FixtureConfig::new(row_count);
    let jsonl_path = jsonl::write_fixture_nested_file(&config, db.path(), "purchases_nested.jsonl")
        .expect("write nested fixture");

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
        jsonl_path.display()
    );
    engine
        .query(&sql, &mut database)
        .expect("nested JSONL ingest");

    assert_eq!(
        manifest_row_count(&database, "purchases"),
        row_count,
        "nested-properties ingest must land the same row count as flat ingest"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Query after ingest verifies real rows from disk
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn query_after_jsonl_ingest_returns_correct_rows() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let row_count = CI_ROW_COUNT;
    let config = FixtureConfig::new(row_count);
    let jsonl_path =
        jsonl::write_fixture_file(&config, db.path(), "purchases.jsonl").expect("write fixture");

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
        jsonl_path.display()
    );
    engine
        .query(&sql, &mut database)
        .expect("INSERT FROM JSONL");

    // Full table scan must return all rows.
    let scan = engine.query("purchases", &mut database).expect("bare scan");
    assert_eq!(
        scan.row_count(),
        row_count as usize,
        "bare scan must return all {row_count} rows"
    );

    // Filtered query: count purchase rows.
    let purchase_result = engine
        .query("purchases | where event_type = 'purchase'", &mut database)
        .expect("purchase filter");
    assert_eq!(
        purchase_result.row_count(),
        jsonl::expected_purchase_count(row_count) as usize,
        "purchase filter must match expected count"
    );

    // Filtered query: count refund rows.
    let refund_result = engine
        .query("purchases | where event_type = 'refund'", &mut database)
        .expect("refund filter");
    assert_eq!(
        refund_result.row_count(),
        jsonl::expected_refund_count(row_count) as usize,
        "refund filter must match expected count"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Column mapping
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn jsonl_ingest_with_column_map() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    // Write a JSONL file with non-standard key names.
    // Note: `time` and `timestamp` are reserved keywords in the BQL grammar,
    // so we use `tstamp` as the source key for the event-time field.
    let jsonl_content = r#"{"uid":"alice","tstamp":1700000000000000000,"evt":"purchase","amt":42,"cat":"books"}
{"uid":"bob","tstamp":1700000001000000000,"evt":"refund"}
"#;
    let jsonl_path = db.path().join("data.jsonl");
    std::fs::write(&jsonl_path, jsonl_content).unwrap();

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (\
             format: 'jsonl', \
             map: (uid AS user_id, tstamp AS ts, evt AS event_type, amt AS amount, cat AS category)\
         )",
        jsonl_path.display()
    );
    engine
        .query(&sql, &mut database)
        .expect("JSONL insert with column map must succeed");

    assert_eq!(manifest_row_count(&database, "purchases"), 2);
}

// ─────────────────────────────────────────────────────────────────────────────
// Empty file is a no-op
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn empty_jsonl_file_is_noop() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let jsonl_path = db.path().join("empty.jsonl");
    std::fs::write(&jsonl_path, "").unwrap();

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
        jsonl_path.display()
    );
    engine
        .query(&sql, &mut database)
        .expect("empty JSONL must succeed");
    assert!(
        database.manifest().tables["purchases"].windows.is_empty(),
        "empty JSONL must not create any segments"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Error diagnostics
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn jsonl_invalid_json_error_carries_row_number() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    // Line 2 is malformed.
    let content =
        "{\"user_id\":\"alice\",\"ts\":1700000000000000000,\"event_type\":\"click\"}\nnot json\n";
    let jsonl_path = db.path().join("bad.jsonl");
    std::fs::write(&jsonl_path, content).unwrap();

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
        jsonl_path.display()
    );
    let err = engine
        .query(&sql, &mut database)
        .expect_err("invalid JSON must error");
    let msg = err.to_string();
    assert!(
        msg.contains("row 2"),
        "error must identify row 2; got: {msg}"
    );
}

#[test]
fn parquet_format_is_rejected_with_clear_error() {
    // TASK-449 is the Parquet ingest task; until then the planner must
    // reject `format: 'parquet'` with a clear diagnostic.
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);
    let dummy_path = db.path().join("data.parquet");
    std::fs::write(&dummy_path, "not parquet").unwrap();

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'parquet')",
        dummy_path.display()
    );
    let err = engine
        .query(&sql, &mut database)
        .expect_err("parquet must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("parquet") || msg.contains("TASK-449"),
        "error must mention parquet or TASK-449; got: {msg}"
    );
}
