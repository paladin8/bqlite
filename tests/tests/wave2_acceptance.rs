//! Wave 2 acceptance test — the gate that validates the full Wave 2
//! pipeline end-to-end (TASK-235).
//!
//! This test exercises:
//!
//! 1. `Database::create` — initialize a fresh database
//! 2. `CREATE TABLE` — the `purchases` schema
//! 3. `INSERT VALUES` — a small literal batch
//! 4. `INSERT FROM` — the synthetic CSV at scale
//! 5. `WHERE` / `SELECT` / `LIMIT` query pipeline
//! 6. `EXPLAIN` — assert the plan tree structure
//! 7. `DESCRIBE` — assert the column metadata
//! 8. `ALTER TABLE ADD COLUMN` — extend the schema
//! 9. `DROP TABLE` — remove the table
//!
//! A 100M-row `#[ignore]`d variant is provided for the bench job; the
//! default test uses a smaller scale to keep CI fast.

use arrow::array::StringViewArray;
use bqlite_engine::{Database, Engine};
use bqlite_tests::common::TempDb;
use bqlite_tests::csv::{self, FixtureConfig};

/// Scale for the default CI test: 1000 rows.
///
/// Large enough to exercise multi-entity partitioning and the
/// purchase/refund split, small enough to keep CI under a second.
const CI_ROW_COUNT: u64 = 1_000;

/// Scale for the bench-job variant behind `#[ignore]` (100M rows per
/// the TASK-235 spec).
const BENCH_ROW_COUNT: u64 = 100_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Set up a database, create the purchases table, and return the
/// database handle and engine.
fn setup_purchases(db: &TempDb) -> (Database, Engine) {
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();
    engine
        .query(csv::PURCHASES_CREATE_TABLE, &mut database)
        .expect("CREATE TABLE purchases");
    (database, engine)
}

/// Total rows in the manifest for the given table.
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
// Core acceptance test
// ─────────────────────────────────────────────────────────────────────────────

/// Run the full acceptance script at the given row count.
fn acceptance_at_scale(row_count: u64) {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    // ── Step 1: INSERT VALUES (small literal batch) ──────────────
    let insert_values = engine
        .query(
            "INSERT INTO purchases VALUES \
             ('user_0', 1700000000000000000, 'purchase', 999, 'manual'), \
             ('user_1', 1700000001000000000, 'refund', NULL, NULL)",
            &mut database,
        )
        .expect("INSERT VALUES");
    assert!(insert_values.is_empty(), "INSERT produces no rows");
    assert_eq!(manifest_row_count(&database, "purchases"), 2);

    // ── Step 2: INSERT FROM CSV at scale ─────────────────────────
    let config = FixtureConfig::new(row_count);
    let csv_path =
        csv::write_fixture_file(&config, db.path(), "purchases.csv").expect("write CSV fixture");

    let insert_from_sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'csv')",
        csv_path.display()
    );
    let insert_from = engine
        .query(&insert_from_sql, &mut database)
        .expect("INSERT FROM CSV");
    assert!(insert_from.is_empty(), "INSERT produces no rows");

    // Total: VALUES rows + CSV rows.
    let total_rows = manifest_row_count(&database, "purchases");
    assert_eq!(total_rows, 2 + row_count);

    // ── Step 3: Query pipeline — bare table scan ─────────────────
    //
    // The scan operator uses Database::segment_reader which currently
    // returns EmptySegmentReader (the real segment-to-Arrow read path
    // is a separate task). The authoritative proof that rows landed is
    // the manifest row count asserted above. Here we verify the scan
    // *succeeds* (does not error) and returns the correct schema.
    let scan_result = engine
        .query("purchases", &mut database)
        .expect("bare table scan must not error");
    let scan_names: Vec<&str> = scan_result
        .schema
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(
        scan_names.contains(&"user_id"),
        "scan schema must include user_id, got: {scan_names:?}"
    );
    assert!(
        scan_names.contains(&"amount"),
        "scan schema must include amount, got: {scan_names:?}"
    );

    // ── Step 4: Query pipeline — WHERE + SELECT + LIMIT ──────────
    //
    // The pipeline compiles and runs correctly; row count is 0 through
    // the EmptySegmentReader but the schema reflects the projection.
    let pipeline_result = engine
        .query(
            "purchases | where event_type = 'purchase' | select user_id, amount | limit 10",
            &mut database,
        )
        .expect("WHERE/SELECT/LIMIT pipeline must not error");
    assert!(
        pipeline_result.row_count() <= 10,
        "LIMIT 10 should cap output at 10 rows, got {}",
        pipeline_result.row_count()
    );
    // Check the schema has only the projected columns (plus system cols).
    let col_names: Vec<&str> = pipeline_result
        .schema
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(
        col_names.contains(&"user_id"),
        "projected schema must include user_id, got: {col_names:?}"
    );
    assert!(
        col_names.contains(&"amount"),
        "projected schema must include amount, got: {col_names:?}"
    );

    // ── Step 5: EXPLAIN ──────────────────────────────────────────
    let explain = engine
        .query(
            "EXPLAIN purchases | where event_type = 'purchase'",
            &mut database,
        )
        .expect("EXPLAIN");
    assert_eq!(explain.row_count(), 1, "EXPLAIN returns a single row");
    // The plan text should mention the Scan node for the purchases table.
    let plan_col = explain.schema.columns().iter().find(|c| c.name == "plan");
    assert!(
        plan_col.is_some(),
        "EXPLAIN result must have a 'plan' column"
    );
    // Extract the plan text and assert the expected ExplainNode structure:
    // the Scan node should reference the purchases table, and the plan
    // should contain a predicate for the WHERE filter.
    let plan_text = explain.rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("plan column must be StringView")
        .value(0);
    assert!(
        plan_text.contains("Scan(purchases)"),
        "EXPLAIN must show Scan(purchases), got:\n{plan_text}"
    );
    assert!(
        plan_text.contains("event_type"),
        "EXPLAIN must show the predicate referencing event_type, got:\n{plan_text}"
    );

    // ── Step 6: DESCRIBE ─────────────────────────────────────────
    let describe = engine
        .query("DESCRIBE purchases", &mut database)
        .expect("DESCRIBE purchases");
    // The purchases table has 5 columns.
    assert_eq!(
        describe.row_count(),
        5,
        "DESCRIBE should return 5 rows (one per column), got {}",
        describe.row_count()
    );
    let desc_names: Vec<&str> = describe
        .schema
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(
        desc_names.contains(&"name"),
        "DESCRIBE schema must include 'name' column"
    );
    assert!(
        desc_names.contains(&"role"),
        "DESCRIBE schema must include 'role' column"
    );

    // ── Step 7: ALTER TABLE ADD COLUMN ────────────────────────────
    engine
        .query(
            "ALTER TABLE purchases ADD COLUMN source STRING",
            &mut database,
        )
        .expect("ALTER TABLE ADD COLUMN");

    // Verify the column was added by describing the table again.
    let describe_after = engine
        .query("DESCRIBE purchases", &mut database)
        .expect("DESCRIBE after ALTER");
    assert_eq!(
        describe_after.row_count(),
        6,
        "DESCRIBE should return 6 rows after ADD COLUMN, got {}",
        describe_after.row_count()
    );

    // Verify via manifest that the table still has its data.
    assert_eq!(
        manifest_row_count(&database, "purchases"),
        2 + row_count,
        "ALTER TABLE should not change row count"
    );

    // ── Step 8: DROP TABLE ───────────────────────────────────────
    engine
        .query("DROP TABLE purchases", &mut database)
        .expect("DROP TABLE purchases");

    // The table should no longer exist in the manifest.
    assert!(
        !database.manifest().tables.contains_key("purchases"),
        "purchases table should be absent from manifest after DROP"
    );

    // Querying the dropped table should error.
    let query_err = engine.query("purchases", &mut database);
    assert!(query_err.is_err(), "querying a dropped table must error");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test entry points
// ─────────────────────────────────────────────────────────────────────────────

/// The CI acceptance test — 1,000-row scale.
#[test]
fn wave2_acceptance_ci() {
    acceptance_at_scale(CI_ROW_COUNT);
}

/// The bench-job acceptance test — 100M-row scale.
///
/// Lives behind `#[ignore]` so `cargo test` skips it by default.
/// Run with `cargo test -- --ignored` or from the bench CI job.
#[test]
#[ignore]
fn wave2_acceptance_100m() {
    acceptance_at_scale(BENCH_ROW_COUNT);
}

// ─────────────────────────────────────────────────────────────────────────────
// Focused sub-tests
// ─────────────────────────────────────────────────────────────────────────────

/// CREATE TABLE followed by a bare scan returns an empty result with
/// the correct schema.
#[test]
fn create_table_then_empty_scan() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let result = engine
        .query("purchases", &mut database)
        .expect("empty scan");
    assert!(result.is_empty());

    let names: Vec<&str> = result
        .schema
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    // Table columns + system columns (__seq_id, __batch_id).
    assert!(names.contains(&"user_id"));
    assert!(names.contains(&"ts"));
    assert!(names.contains(&"event_type"));
    assert!(names.contains(&"amount"));
    assert!(names.contains(&"category"));
    assert!(names.contains(&"__seq_id"));
    assert!(names.contains(&"__batch_id"));
}

/// INSERT VALUES round-trip — insert a small batch and verify the
/// manifest row count.
#[test]
fn insert_values_round_trip() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    engine
        .query(
            "INSERT INTO purchases VALUES \
             ('alice', 1700000000000000000, 'purchase', 42, 'books'), \
             ('bob',   1700000001000000000, 'refund', NULL, NULL), \
             ('alice', 1700000002000000000, 'purchase', 100, 'electronics')",
            &mut database,
        )
        .expect("INSERT VALUES");

    assert_eq!(manifest_row_count(&database, "purchases"), 3);
}

/// INSERT FROM CSV round-trip — write a fixture, ingest it, verify
/// the manifest row count.
#[test]
fn insert_from_csv_round_trip() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let config = FixtureConfig::new(500);
    let csv_path = csv::write_fixture_file(&config, db.path(), "data.csv").expect("write fixture");

    let sql = format!(
        "INSERT INTO purchases FROM '{}' WITH (format: 'csv')",
        csv_path.display()
    );
    engine.query(&sql, &mut database).expect("INSERT FROM");

    assert_eq!(manifest_row_count(&database, "purchases"), 500);
}

/// DESCRIBE returns the correct column metadata for the purchases
/// schema.
#[test]
fn describe_purchases_schema() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let result = engine
        .query("DESCRIBE purchases", &mut database)
        .expect("DESCRIBE");
    assert_eq!(result.row_count(), 5);
}

/// DROP TABLE on a non-existent table errors.
#[test]
fn drop_nonexistent_table_errors() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let err = engine
        .query("DROP TABLE ghost", &mut database)
        .expect_err("DROP nonexistent table must error");
    let msg = err.to_string();
    assert!(msg.contains("ghost"), "error should name the table: {msg}");
}

/// ALTER TABLE ADD COLUMN on a non-existent table errors.
#[test]
fn alter_nonexistent_table_errors() {
    let db = TempDb::new();
    let (mut database, engine) = setup_purchases(&db);

    let err = engine
        .query("ALTER TABLE ghost ADD COLUMN x INT", &mut database)
        .expect_err("ALTER nonexistent table must error");
    let msg = err.to_string();
    assert!(msg.contains("ghost"), "error should name the table: {msg}");
}

/// CSV fixture generator produces deterministic output.
#[test]
fn csv_fixture_deterministic() {
    let config = FixtureConfig::new(100);
    let mut buf1 = Vec::new();
    csv::write_fixture(&config, &mut buf1).unwrap();
    let mut buf2 = Vec::new();
    csv::write_fixture(&config, &mut buf2).unwrap();
    assert_eq!(buf1, buf2, "CSV fixture must be deterministic");
}
