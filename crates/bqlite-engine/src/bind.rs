//! Physical-plan bind step — plain-data → executable operator tree.
//!
//! Per [`docs/design/planner-pipeline.md`](../../../docs/design/planner-pipeline.md)
//! §15, the planner emits a plain-data [`PhysicalPlan`] tree, not
//! `Box<dyn PhysicalOperator>` — the trait object lives in
//! `bqlite-operators`, which sits above `bqlite-planner` in the crate
//! dependency graph. Binding is the engine's responsibility: it
//! consumes the descriptor and materializes one concrete operator per
//! descriptor node, wiring in the runtime handles (segment readers,
//! cancellation tokens, memory budgets) that only the engine has
//! visibility into.
//!
//! ## Wave 2 scope (TASK-232)
//!
//! The bind step handles the full Wave 2 descriptor set:
//!
//! - **Data-plane** (`Scan`, `Filter`, `Project`, `Limit`):
//!   recursively bind children and construct the corresponding
//!   operator from `bqlite-operators`.
//! - **DDL** (`CreateTable`, `DropTable`, `AlterTableAddColumn`):
//!   execute the mutation against the manifest during bind and
//!   return an empty [`crate::ddl::ResultOperator`].
//! - **Metadata** (`Describe`, `Explain`): compute the result
//!   batch during bind and return a
//!   [`crate::ddl::ResultOperator`] wrapping the batch.
//! - **INSERT** (`From`): execute via the CSV ingest pipeline
//!   (TASK-233). `Values` deferred to TASK-238.

use std::sync::Arc;

use bqlite_core::{Result, SegmentReader};
use bqlite_operators::{
    CancellationToken, FilterOperator, LimitOperator, PhysicalOperator, ProjectOperator,
    ScanOperator,
};
use bqlite_planner::{PhysicalPlan, ScanPhysical};
use bqlite_storage::Database;

use crate::ddl::{
    build_describe_batch, build_explain_batch, execute_alter_table_add_column,
    execute_create_table, execute_drop_table, ResultOperator,
};

/// Bind a plain-data [`PhysicalPlan`] into an executable
/// `Box<dyn PhysicalOperator>` tree rooted at the plan's top node.
///
/// Each descriptor arm is responsible for wiring in runtime handles
/// (`Database` for segment readers, the shared
/// [`CancellationToken`], later the memory budget and metrics sink).
/// The returned operator is ready to drive with `open → next_batch* →
/// close` per the [`PhysicalOperator`] lifecycle contract.
///
/// ## Data-plane descriptors
///
/// `Scan`, `Filter`, `Project`, `Limit` — recursively bind children
/// and construct the corresponding operator.
///
/// ## DDL descriptors
///
/// `CreateTable`, `DropTable`, `AlterTableAddColumn` — execute the
/// DDL mutation against the manifest during bind and return an empty
/// [`ResultOperator`].
///
/// ## Metadata descriptors
///
/// `Describe` — look up the table and build a four-column result
/// batch. `Explain` — format the plan tree as a single-column batch.
///
/// # Errors
///
/// Propagates any error from operator construction, DDL execution,
/// or catalog lookup.
pub fn bind_physical(plan: &PhysicalPlan, db: &mut Database) -> Result<Box<dyn PhysicalOperator>> {
    match plan {
        // ── Data-plane operators ─────────────────────────────────
        PhysicalPlan::Scan(scan) => bind_scan(scan, db),

        PhysicalPlan::Filter(filter) => {
            let child = bind_physical(&filter.input, db)?;
            Ok(Box::new(FilterOperator::new(
                child,
                filter.predicate.clone(),
                filter.tile_size,
            )))
        }

        PhysicalPlan::Project(project) => {
            let child = bind_physical(&project.input, db)?;
            Ok(Box::new(ProjectOperator::from_physical_items(
                child,
                project.expressions.clone(),
                project.output_schema.clone(),
            )))
        }

        PhysicalPlan::Limit(limit) => {
            let child = bind_physical(&limit.input, db)?;
            Ok(Box::new(LimitOperator::new(child, limit.count)))
        }

        // ── DDL ──────────────────────────────────────────────────
        PhysicalPlan::CreateTable(ct) => {
            execute_create_table(ct, db)?;
            Ok(Box::new(ResultOperator::empty(ct.output_schema.clone())))
        }

        PhysicalPlan::DropTable(dt) => {
            execute_drop_table(dt, db)?;
            Ok(Box::new(ResultOperator::empty(dt.output_schema.clone())))
        }

        PhysicalPlan::AlterTableAddColumn(alter) => {
            execute_alter_table_add_column(alter, db)?;
            Ok(Box::new(ResultOperator::empty(alter.output_schema.clone())))
        }

        // ── Metadata ─────────────────────────────────────────────
        PhysicalPlan::Describe(desc) => {
            let batch = build_describe_batch(desc, db)?;
            Ok(Box::new(ResultOperator::new(
                desc.output_schema.clone(),
                vec![batch],
            )))
        }

        PhysicalPlan::Explain(explain) => {
            let batch = build_explain_batch(explain)?;
            Ok(Box::new(ResultOperator::new(
                explain.output_schema.clone(),
                vec![batch],
            )))
        }

        // ── DML ──────────────────────────────────────────────────
        PhysicalPlan::Insert(insert) => {
            crate::ingest::execute_insert(insert, db)?;
            Ok(Box::new(ResultOperator::empty(
                insert.output_schema.clone(),
            )))
        }
    }
}

fn bind_scan(scan: &ScanPhysical, db: &Database) -> Result<Box<dyn PhysicalOperator>> {
    // `Database::segment_reader` returns a manifest-backed
    // `ManifestSegmentReader` (TASK-244) that enumerates live segments
    // and opens them from disk. We hand it to the scan as an `Arc` so
    // later waves can share ownership across parallel shard-tasks.
    let reader_box: Box<dyn SegmentReader> = db.segment_reader(&scan.table)?;
    let reader: Arc<dyn SegmentReader> = Arc::from(reader_box);

    // Thread the descriptor's projection and pushed predicates into
    // the scan operator. Both fields are empty at lowering time in
    // Wave 2 (TASK-228's projection pruning and TASK-227's predicate
    // pushdown populate them during optimization), so this reduces
    // to `ColumnProjection::all()` with no zone-map predicate until
    // those passes run.
    let op = ScanOperator::new(
        reader,
        &scan.projected_columns,
        scan.scan_predicates.clone(),
        CancellationToken::new(),
    )?;
    Ok(Box::new(op))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow::array::Array;
    use bqlite_core::OperatorSchema;
    use bqlite_planner::{plan, PhysicalPlan, ScanPhysical};
    use bqlite_storage::{bootstrap_events_schema, Database};

    use super::*;

    /// Per-test unique temp directory. Mirrors the pattern used in
    /// `bqlite_storage::database::tests` — process PID + monotonic
    /// counter is enough for in-process uniqueness without pulling
    /// `tempfile` into the dev-dependency closure.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-engine-bind-{label}-{pid}-{seq}"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn create_db_with_bootstrap(path: &Path) -> Database {
        let mut db = Database::create(path).expect("create db");
        db.create_table("events".into(), bootstrap_events_schema())
            .expect("create events");
        db
    }

    fn bootstrap_scan_descriptor() -> PhysicalPlan {
        PhysicalPlan::Scan(ScanPhysical {
            table: "events".to_string(),
            time_range: None,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema: OperatorSchema::from_table(&bootstrap_events_schema()),
        })
    }

    #[test]
    fn bind_scan_produces_a_drivable_operator() {
        let scratch = Scratch::new("happy");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = bootstrap_scan_descriptor();

        let mut op = bind_physical(&descriptor, &mut db).expect("bind must succeed");

        // Full PhysicalOperator lifecycle — the smoke test (TASK-123)
        // will drive this exact path through `Engine::query`.
        op.open().expect("open should succeed");
        assert!(
            op.next_batch()
                .expect("next_batch should succeed")
                .is_none(),
            "bootstrap events table has zero segments so the first pull must exhaust"
        );
        // Exhaustion is sticky.
        assert!(op.next_batch().unwrap().is_none());
        op.close().expect("close should succeed");
    }

    #[test]
    fn bind_scan_output_schema_reflects_declared_columns() {
        // The descriptor's `output_schema` widens to
        // `OperatorSchema::from_table` (declared columns + implicit
        // `__seq_id` / `__batch_id` system columns) because that's
        // the shape the planner uses to compose downstream
        // operators. The Wave 2 scan operator, however, narrows to
        // **declared columns only** — the segment reader does not
        // yet materialize system columns, and the k-way merge would
        // reject batches whose schema did not match the one passed
        // at construction. This test pins both facts explicitly so a
        // regression in either side surfaces as a clean assertion.
        let scratch = Scratch::new("schema");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = bootstrap_scan_descriptor();

        let op = bind_physical(&descriptor, &mut db).expect("bind must succeed");

        let op_names: Vec<&str> = op
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(op_names, vec!["entity_id", "ts", "event_type"]);

        let descriptor_names: Vec<&str> = descriptor
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            descriptor_names,
            vec!["entity_id", "ts", "event_type", "__seq_id", "__batch_id"]
        );
    }

    #[test]
    fn bind_scan_reports_unknown_table_through_plan_error() {
        let scratch = Scratch::new("unknown");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = PhysicalPlan::Scan(ScanPhysical {
            table: "ghost".to_string(),
            time_range: None,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema: OperatorSchema::from_table(&bootstrap_events_schema()),
        });

        match bind_physical(&descriptor, &mut db) {
            Err(bqlite_core::BqliteError::Plan(msg)) => {
                assert!(msg.contains("ghost"), "error should name the table: {msg}");
            }
            Err(other) => panic!("expected Plan error for unknown table, got {other:?}"),
            Ok(_) => panic!("expected Plan error for unknown table, got Ok"),
        }
    }

    #[test]
    fn bind_physical_composes_with_planner_output() {
        // End-to-end spot check: run the Wave 1 pipeline (parse ->
        // plan -> bind) against a real bootstrap database and confirm
        // every stage hands off the expected shape. This duplicates
        // the smoke test coverage (TASK-123) in miniature so that a
        // regression in the bind step is localized to *this* file
        // rather than surfacing as a generic smoke-test failure.
        let scratch = Scratch::new("compose");
        let mut db = create_db_with_bootstrap(scratch.path());
        let stmt = bqlite_parser::parse("events").expect("parse events");
        let physical = {
            let catalog = db.catalog();
            plan(stmt, &catalog).expect("plan events")
        };

        let mut op = bind_physical(&physical, &mut db).expect("bind succeeds");
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
        op.close().unwrap();
    }

    // ── TASK-244: end-to-end real-rows through manifest-backed reader ────

    /// Create a database with a 4-column events table suitable for
    /// INSERT VALUES round-trip tests.
    fn create_db_with_events_table(path: &Path) -> (Database, crate::Engine) {
        let mut db = Database::create(path).expect("create db");
        let engine = crate::Engine::new();
        engine
            .query(
                "CREATE TABLE events (\
                     user_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE, \
                     amount INT\
                 )",
                &mut db,
            )
            .expect("create events table");
        (db, engine)
    }

    #[test]
    fn insert_values_then_query_returns_real_rows() {
        // TASK-244 end-to-end: CREATE TABLE → INSERT VALUES → query
        // must return actual row data through the manifest-backed
        // reader, not zero rows from the old EmptySegmentReader stub.
        let scratch = Scratch::new("real-rows");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 42), \
                 ('bob',   1700000000100000000, 'view',  NULL)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");

        // Query the table — the scan operator drives through the
        // ManifestSegmentReader and returns real rows.
        let result = engine.query("events", &mut db).expect("query must succeed");
        assert_eq!(
            result.row_count(),
            2,
            "query must return 2 rows, not zero — the scan reads real segments"
        );

        // Collect entity names across all batches (rows may be split
        // across segments/shards).
        let mut entities: Vec<String> = Vec::new();
        let mut found_alice_amount = false;
        let mut found_bob_null = false;
        for batch in &result.rows {
            let entity_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("user_id column should be StringView");
            let amount_col = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("amount column should be Int64");
            for i in 0..batch.num_rows() {
                let name = entity_col.value(i).to_string();
                if name == "alice" {
                    assert_eq!(amount_col.value(i), 42);
                    found_alice_amount = true;
                }
                if name == "bob" {
                    assert!(amount_col.is_null(i), "bob's amount must be NULL");
                    found_bob_null = true;
                }
                entities.push(name);
            }
        }
        entities.sort();
        assert_eq!(entities, vec!["alice", "bob"]);
        assert!(found_alice_amount, "alice's amount=42 must round-trip");
        assert!(found_bob_null, "bob's NULL amount must round-trip");
    }

    #[test]
    fn insert_values_then_where_returns_filtered_rows() {
        let scratch = Scratch::new("real-rows-filter");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 10), \
                 ('alice', 1700000000100000000, 'view',  20), \
                 ('bob',   1700000000200000000, 'click', 30)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");

        // BQL pipe syntax: table | where ...
        let result = engine
            .query("events | where event_type = 'click'", &mut db)
            .expect("filtered query");
        assert_eq!(
            result.row_count(),
            2,
            "WHERE event_type = 'click' should return 2 rows (alice + bob)"
        );
    }

    #[test]
    fn insert_values_then_limit_returns_bounded_rows() {
        let scratch = Scratch::new("real-rows-limit");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 10), \
                 ('bob',   1700000000100000000, 'view',  20), \
                 ('carol', 1700000000200000000, 'click', 30)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");

        let result = engine
            .query("events | limit 2", &mut db)
            .expect("query with LIMIT");
        assert_eq!(result.row_count(), 2, "LIMIT 2 should cap at 2 rows");
    }
}
