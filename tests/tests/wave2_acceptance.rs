//! Wave 2 acceptance test — the gate that validates the full Wave 2
//! pipeline end-to-end (TASK-235).
//!
//! This test exercises:
//!
//! 1. `Database::create` — initialize a fresh database
//! 2. `CREATE TABLE` — the `purchases` schema
//! 3. `INSERT VALUES` — a small literal batch
//! 4. `INSERT FROM` — the synthetic CSV at scale
//! 5. `WHERE` / `SELECT` / `LIMIT` query pipeline — real rows from disk
//! 6. `EXPLAIN` — assert pushed predicates and pruned columns in the plan
//! 7. `DESCRIBE` — assert exact column metadata (names, types, nullable, roles)
//! 8. `ALTER TABLE ADD COLUMN` — extend the schema
//! 9. `DROP TABLE` — remove the table
//!
//! A 100M-row `#[ignore]`d variant is provided for the bench job; the
//! default test uses a smaller scale to keep CI fast.
//!
//! TASK-244 shipped the manifest-backed `SegmentReader`; every scan in
//! this test reads real segment data from disk.

use arrow::array::{BooleanArray, StringViewArray};
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
    // TASK-244 shipped the manifest-backed SegmentReader: a bare scan
    // now reads every segment from disk and returns real rows.
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
    // The scan reads all ingested rows from disk.
    assert_eq!(
        scan_result.row_count(),
        (2 + row_count) as usize,
        "bare scan must return all ingested rows; expected {}, got {}",
        2 + row_count,
        scan_result.row_count()
    );

    // ── Step 4: Query pipeline — WHERE + SELECT + LIMIT ──────────
    //
    // The scan drives through the ManifestSegmentReader, which reads
    // real segment data from disk; the WHERE filter and LIMIT then
    // operate on those real rows.
    //
    // The fixture produces 2 rows from VALUES, but the literal
    // purchase has amount=999 and therefore does not satisfy the
    // acceptance predicate. CSV purchase rows have amount `i * 100 + 1`,
    // so the first matching row is i=45 (amount=4501). LIMIT 10 caps
    // the output once enough matching CSV rows exist.
    let purchase_rows_before_threshold = csv::expected_purchase_count(45);
    let matching_purchase_from_csv =
        csv::expected_purchase_count(row_count).saturating_sub(purchase_rows_before_threshold);
    let expected_limit_result = matching_purchase_from_csv.min(10) as usize;

    let pipeline_result = engine
        .query(
            "purchases | where event_type = 'purchase' AND amount > 4500 | select user_id, amount | limit 10",
            &mut database,
        )
        .expect("WHERE/SELECT/LIMIT pipeline must not error");
    assert_eq!(
        pipeline_result.row_count(),
        expected_limit_result,
        "WHERE/SELECT/LIMIT should return {} rows (high-value purchase count capped at 10), got {}",
        expected_limit_result,
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
    //
    // Run EXPLAIN on the full WHERE/SELECT/LIMIT pipeline and assert
    // that the optimizer pushed the predicate into the Scan (no Filter
    // node remains) and pruned columns to only what the Project demands
    // plus what the pushed predicate references.
    let explain = engine
        .query(
            "EXPLAIN purchases | where event_type = 'purchase' AND amount > 4500 | select user_id, amount | limit 10",
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
    // Extract the plan text.
    let plan_text = explain.rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("plan column must be StringView")
        .value(0);

    // The predicate must be visible in the Scan node (pushed down from
    // the Filter; the Filter node itself is elided when all conjuncts
    // are pushed).
    assert!(
        plan_text.contains("Scan(purchases)"),
        "EXPLAIN must show Scan(purchases), got:\n{plan_text}"
    );
    assert!(
        plan_text.contains("event_type = 'purchase'"),
        "EXPLAIN must show the pushed predicate in Scan, got:\n{plan_text}"
    );
    assert!(
        plan_text.contains("amount > 4500"),
        "EXPLAIN must show the pushed amount predicate in Scan, got:\n{plan_text}"
    );
    // The Filter node must be absent — the optimizer elided it after
    // pushing all conjuncts into the Scan.
    assert!(
        !plan_text.contains("Filter"),
        "EXPLAIN must not show a Filter node after predicate pushdown, got:\n{plan_text}"
    );
    // The Project node is present and lists only the demanded columns.
    assert!(
        plan_text.contains("Project"),
        "EXPLAIN must show a Project node, got:\n{plan_text}"
    );
    assert!(
        plan_text.contains("user_id") && plan_text.contains("amount"),
        "EXPLAIN must list user_id and amount in the Project, got:\n{plan_text}"
    );
    // The Limit node is present with count 10.
    assert!(
        plan_text.contains("Limit 10"),
        "EXPLAIN must show Limit 10, got:\n{plan_text}"
    );
    // Pruned columns: the Scan should only list the demanded columns:
    // - user_id (entity key — always included for k-way merge + in SELECT)
    // - ts (timestamp — always included for k-way merge, even if not in SELECT)
    // - event_type (referenced by the pushed-down predicate)
    // - amount (referenced by both the pushed predicate and SELECT)
    // category must be pruned (not referenced by any operator).
    let scan_start = plan_text
        .find("Scan(purchases)")
        .expect("Scan node missing");
    let scan_section = &plan_text[scan_start..];
    // The "columns" line within the Scan section.
    let columns_line = scan_section
        .lines()
        .find(|l| l.contains("columns"))
        .expect("Scan must have a columns line");
    assert!(
        !columns_line.contains("category"),
        "Scan columns must not include category (pruned), got:\n{columns_line}"
    );
    assert!(
        columns_line.contains("user_id") && columns_line.contains("amount"),
        "Scan columns must include user_id and amount, got:\n{columns_line}"
    );
    assert!(
        columns_line.contains("ts"),
        "Scan columns must include ts (required for k-way merge), got:\n{columns_line}"
    );
    assert!(
        columns_line.contains("event_type"),
        "Scan columns must include event_type (pushed predicate), got:\n{columns_line}"
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
    // The result schema has four columns: name, type, nullable, role.
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
        desc_names.contains(&"type"),
        "DESCRIBE schema must include 'type' column"
    );
    assert!(
        desc_names.contains(&"nullable"),
        "DESCRIBE schema must include 'nullable' column"
    );
    assert!(
        desc_names.contains(&"role"),
        "DESCRIBE schema must include 'role' column"
    );

    // Assert the actual column data for the first batch.
    let batch = &describe.rows[0];
    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("name column must be StringView");
    let type_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("type column must be StringView");
    let nullable_col = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("nullable column must be Boolean");
    let role_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("role column must be StringView");

    // Collect into vecs for easy assertion.
    let names: Vec<&str> = (0..batch.num_rows()).map(|i| name_col.value(i)).collect();
    let types: Vec<&str> = (0..batch.num_rows()).map(|i| type_col.value(i)).collect();
    let nullables: Vec<bool> = (0..batch.num_rows())
        .map(|i| nullable_col.value(i))
        .collect();
    let roles: Vec<&str> = (0..batch.num_rows()).map(|i| role_col.value(i)).collect();

    // Expected schema for purchases:
    //   user_id   STRING      NOT NULL  ENTITY KEY
    //   ts        TIMESTAMP   NOT NULL  EVENT TIME
    //   event_type STRING     NOT NULL  EVENT TYPE
    //   amount    INT         nullable  (no role)
    //   category  STRING      nullable  (no role)
    assert_eq!(
        names,
        vec!["user_id", "ts", "event_type", "amount", "category"]
    );
    assert_eq!(
        types,
        vec!["STRING", "TIMESTAMP", "STRING", "INT", "STRING"]
    );
    assert_eq!(nullables, vec![false, false, false, true, true]);
    assert_eq!(
        roles,
        vec!["ENTITY KEY", "EVENT TIME", "EVENT TYPE", "", ""]
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

    // Verify the new column appears at the end with the correct type.
    let after_batch = &describe_after.rows[0];
    let after_names = after_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("name col");
    assert_eq!(
        after_names.value(5),
        "source",
        "new column must be 'source'"
    );
    let after_types = after_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("type col");
    assert_eq!(
        after_types.value(5),
        "STRING",
        "new column type must be 'STRING'"
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
/// Exercises the same real segment-scan path as the CI test (TASK-244).
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
/// manifest row count and that the scan returns real rows.
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

    // The scan must return all 3 inserted rows from disk.
    let result = engine
        .query("purchases", &mut database)
        .expect("scan after INSERT VALUES");
    assert_eq!(
        result.row_count(),
        3,
        "scan must return all 3 inserted rows, got {}",
        result.row_count()
    );
}

/// INSERT FROM CSV round-trip — write a fixture, ingest it, verify
/// the manifest row count and that the scan returns real rows.
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

    // The scan must return all 500 rows from disk.
    let result = engine
        .query("purchases", &mut database)
        .expect("scan after INSERT FROM");
    assert_eq!(
        result.row_count(),
        500,
        "scan must return all 500 rows, got {}",
        result.row_count()
    );
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

/// TASK-519 — selection-vector materialization counter increments through
/// the fused stateless path. Builds the `FusedStatelessSegment` directly
/// over hand-built batches so we can read the metric snapshot without
/// piping a `Metrics` handle through `Engine::query`.
#[test]
fn fused_stateless_segment_records_selection_vector_metrics() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::metrics::{AtomicMetrics, Metrics};
    use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
    use bqlite_operators::{
        CancellationToken, FilterKernel, FusedStatelessSegment, KernelStep, PhysicalOperator,
        StatelessKernel,
    };
    use bqlite_planner::compiled::CompiledExpr;
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    let op_schema = OperatorSchema::new(vec![
        ColumnDef::required("id", BqlType::Int),
        ColumnDef::required("value", BqlType::Int),
    ])
    .unwrap();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // 100-row batch. First filter keeps 5 rows (5%, below the 10%
    // sparsity threshold), forcing an intra-chain materialization at
    // the second filter's entry point.
    let ids: ArrayRef = Arc::new(Int64Array::from((0..100i64).collect::<Vec<_>>()));
    let values: ArrayRef = Arc::new(Int64Array::from((0..100i64).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), vec![ids, values]).unwrap();

    fn sp<T>(node: T) -> Spanned<T> {
        Spanned::new(node, Span::EMPTY)
    }
    fn col(name: &str) -> Spanned<Expr> {
        sp(Expr::Column(Name::synthetic(name)))
    }
    fn int_lit(i: i64) -> Spanned<Expr> {
        sp(Expr::Literal(Literal::Int(i)))
    }
    fn compile_expr(ast: Spanned<Expr>, schema: &OperatorSchema) -> CompiledExpr {
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("type check");
        CompiledExpr::from_typed(&typed)
    }

    // Filter 1: value < 5 → keeps 5 rows of 100 (sparse).
    let f1 = compile_expr(
        sp(Expr::Compare {
            op: CompareOp::Less,
            left: Box::new(col("value")),
            right: Box::new(int_lit(5)),
        }),
        &op_schema,
    );
    // Filter 2: id >= 0 (always true) — runs over the materialized
    // dense input produced by the sparsity boundary.
    let f2 = compile_expr(
        sp(Expr::Compare {
            op: CompareOp::GreaterOrEqual,
            left: Box::new(col("id")),
            right: Box::new(int_lit(0)),
        }),
        &op_schema,
    );

    // A child operator that emits the single hand-built batch then None.
    struct OneShot {
        schema: OperatorSchema,
        batch: Option<RecordBatch>,
    }
    impl PhysicalOperator for OneShot {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next_batch(&mut self) -> bqlite_core::Result<Option<RecordBatch>> {
            Ok(self.batch.take())
        }
    }

    let kernels: Vec<KernelStep> = vec![
        KernelStep::Filter(
            Arc::new(FilterKernel::with_default_tile_size(f1, op_schema.clone()))
                as Arc<dyn StatelessKernel>,
        ),
        KernelStep::Filter(
            Arc::new(FilterKernel::with_default_tile_size(f2, op_schema.clone()))
                as Arc<dyn StatelessKernel>,
        ),
    ];
    let metrics = Arc::new(AtomicMetrics::new());
    let mut seg = FusedStatelessSegment::new(
        Box::new(OneShot {
            schema: op_schema.clone(),
            batch: Some(batch),
        }),
        kernels,
        op_schema,
        None,
        Arc::clone(&metrics) as Arc<dyn Metrics>,
        CancellationToken::new(),
    );

    let out = seg.next_batch().expect("next_batch ok").expect("non-empty");
    assert_eq!(out.num_rows(), 5);

    let snap = metrics.snapshot();
    // One mid-chain materialization at the sparsity boundary; the outer
    // boundary is a full-cover Some(sv) (5 of 5) which short-circuits
    // without incrementing the counter (per `materialize_filtered_batch`).
    assert_eq!(snap.selection_vector_materializations, 1);
    // Filter 1 dropped 95 rows from a dense input; its accounting is
    // recorded at filter 2's entry. Total: 95.
    assert_eq!(snap.selection_vector_dropped_rows, 95);
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
