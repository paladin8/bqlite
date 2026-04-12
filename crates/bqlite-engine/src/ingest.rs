//! INSERT execution — binds an [`InsertPhysical`] plan node into a
//! concrete write path that streams rows through the partitioner and
//! segment writer.
//!
//! # Wave 2 scope (TASK-233)
//!
//! Two `InsertBody` variants:
//!
//! - **`From`** — opens a CSV file via the streaming
//!   [`CsvEventReader`], pushes each row into the [`Partitioner`],
//!   then drains sorted buckets through the [`SegmentWriter`].
//! - **`Values`** — deferred to TASK-238, which feeds literal tuples
//!   through the same partitioner + writer pipeline.
//!
//! The function returns `()` on success (INSERT produces no result
//! rows); the caller wraps it in an empty [`ResultOperator`].

use std::path::Path;

use bqlite_core::error::{BqliteError, Result};
use bqlite_planner::logical::{InsertFromDescriptor, InsertLogicalBody};
use bqlite_planner::InsertPhysical;
use bqlite_storage::ingest::csv_reader::{CsvEventReader, CsvReaderOptions};
use bqlite_storage::ingest::partitioner::Partitioner;
use bqlite_storage::writer::SegmentWriter;
use bqlite_storage::Database;

/// Default memory budget for the ingest partitioner (256 MB).
///
/// Wave 2 uses a fixed budget; per-query memory management (TASK-501)
/// will make this configurable.
const DEFAULT_INGEST_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Default window-days for the partitioner (30 days).
///
/// Matches the default in `storage-format.md` §4.1.
const DEFAULT_WINDOW_DAYS: u32 = 30;

/// Execute an `INSERT` physical plan against the database.
///
/// Opens the source file (for `FROM`) or converts literal rows (for
/// `VALUES`), streams them through the partitioner, writes segments,
/// and registers them in the manifest.
///
/// # Errors
///
/// - File not found / I/O errors from the CSV reader.
/// - Type-coercion failures with row numbers.
/// - Partitioner memory-budget overflow.
/// - Segment-writer errors (encoding, disk I/O, manifest update).
pub fn execute_insert(plan: &InsertPhysical, db: &mut Database) -> Result<()> {
    match &plan.body {
        InsertLogicalBody::From(descriptor) => execute_insert_from(descriptor, &plan.table, db),
        InsertLogicalBody::Values(_) => Err(BqliteError::Plan(
            "engine: INSERT VALUES execution lands in TASK-238".into(),
        )),
    }
}

/// Execute `INSERT INTO <table> FROM '<path>' WITH (...)`.
fn execute_insert_from(
    descriptor: &InsertFromDescriptor,
    table: &bqlite_core::schema::TableSchema,
    db: &mut Database,
) -> Result<()> {
    // 1. Parse CSV reader options from the planner's resolved options.
    let csv_options = CsvReaderOptions::from_options(&descriptor.options)?;

    // 2. Open the CSV reader and resolve column mapping.
    let path = Path::new(&descriptor.path);
    let mut csv_reader = CsvEventReader::open(path, table, &descriptor.column_map, &csv_options)?;

    // 3. Allocate a batch_id for this ingest call.
    let batch_id = db.allocate_batch_id(table.name())?;

    // 4. Construct the partitioner with the database's shard count.
    let shard_count = db.manifest().shard_count;
    let mut partitioner = Partitioner::new(
        shard_count,
        DEFAULT_WINDOW_DAYS,
        batch_id,
        DEFAULT_INGEST_BUDGET_BYTES,
    )?;

    // 5. Stream CSV rows into the partitioner.
    let mut row_count: u64 = 0;
    while let Some(event) = csv_reader.next_event()? {
        partitioner.push_event(event)?;
        row_count += 1;
    }

    if row_count == 0 {
        // Nothing to write — the CSV had a header but no data rows.
        return Ok(());
    }

    // 6. Drain sorted buckets through the segment writer.
    let mut writer = SegmentWriter::new(db);
    writer.write_partitioner(table.name(), partitioner)?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use bqlite_core::property::PropertyValue;
    use bqlite_core::schema::TableSchema;
    use bqlite_core::OperatorSchema;
    use bqlite_planner::logical::{IngestFormat, InsertFromDescriptor, InsertLogicalBody};
    use bqlite_planner::InsertPhysical;
    use bqlite_storage::Database;

    use super::*;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-engine-ingest-{label}-{pid}-{seq}"));
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

    /// Create a database and use DDL to create the test events table.
    fn create_db_with_events(db_path: &Path) -> Database {
        let mut db = Database::create(db_path).expect("create db");
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
        db
    }

    /// Write CSV text to a temp file and return the path.
    fn write_csv_file(scratch: &Scratch, name: &str, content: &str) -> PathBuf {
        let csv_path = scratch.path().join(name);
        let mut f = fs::File::create(&csv_path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        csv_path
    }

    fn empty_schema() -> OperatorSchema {
        OperatorSchema::new(Vec::new()).expect("empty is valid")
    }

    fn insert_from_plan(
        table: TableSchema,
        csv_path: &Path,
        column_map: Vec<(String, String)>,
    ) -> InsertPhysical {
        InsertPhysical {
            table,
            body: InsertLogicalBody::From(InsertFromDescriptor {
                path: csv_path.to_string_lossy().into_owned(),
                format: IngestFormat::Csv,
                options: Vec::new(),
                column_map,
            }),
            output_schema: empty_schema(),
        }
    }

    fn events_table_schema(db: &Database) -> TableSchema {
        db.manifest().tables["events"].schema.clone()
    }

    // ── Happy path: INSERT FROM CSV ────────────────────────────────

    #[test]
    fn insert_from_csv_writes_segments_and_registers_in_manifest() {
        let scratch = Scratch::new("insert-happy");
        let mut db = create_db_with_events(scratch.path());
        let csv_path = write_csv_file(
            &scratch,
            "data.csv",
            "user_id,ts,event_type,amount\n\
             alice,1700000000000000000,click,42\n\
             bob,1700000000100000000,view,\n\
             charlie,1700000000200000000,purchase,10\n",
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_plan(schema, &csv_path, vec![]);
        execute_insert(&plan, &mut db).expect("insert must succeed");

        // The manifest should have segments registered.
        let table_entry = &db.manifest().tables["events"];
        assert!(
            !table_entry.windows.is_empty(),
            "manifest must have at least one window after insert"
        );

        // Total rows across all segments must equal 3.
        let total_rows: u64 = table_entry
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 3);
    }

    // ── INSERT FROM CSV with column mapping ────────────────────────

    #[test]
    fn insert_from_csv_with_column_map_remaps_columns() {
        let scratch = Scratch::new("insert-map");
        let mut db = create_db_with_events(scratch.path());
        let csv_path = write_csv_file(
            &scratch,
            "data.csv",
            "uid,time,evt,val\n\
             alice,1700000000000000000,click,42\n",
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_plan(
            schema,
            &csv_path,
            vec![
                ("uid".into(), "user_id".into()),
                ("time".into(), "ts".into()),
                ("evt".into(), "event_type".into()),
                ("val".into(), "amount".into()),
            ],
        );
        execute_insert(&plan, &mut db).expect("insert with map must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 1);
    }

    // ── Empty CSV (header only) produces no segments ───────────────

    #[test]
    fn insert_from_empty_csv_is_noop() {
        let scratch = Scratch::new("insert-empty");
        let mut db = create_db_with_events(scratch.path());
        let csv_path = write_csv_file(&scratch, "data.csv", "user_id,ts,event_type,amount\n");

        let schema = events_table_schema(&db);
        let plan = insert_from_plan(schema, &csv_path, vec![]);
        execute_insert(&plan, &mut db).expect("empty insert must succeed");

        // No windows should be created.
        assert!(db.manifest().tables["events"].windows.is_empty());
    }

    // ── File not found errors clearly ──────────────────────────────

    #[test]
    fn insert_from_missing_file_errors() {
        let scratch = Scratch::new("insert-missing");
        let mut db = create_db_with_events(scratch.path());
        let csv_path = scratch.path().join("nonexistent.csv");

        let schema = events_table_schema(&db);
        let plan = insert_from_plan(schema, &csv_path, vec![]);
        let err = execute_insert(&plan, &mut db).expect_err("missing file must error");
        assert!(matches!(err, BqliteError::Io(_)));
    }

    // ── Type mismatch in CSV data errors with row number ───────────

    #[test]
    fn insert_from_csv_type_mismatch_errors_with_row() {
        let scratch = Scratch::new("insert-type-err");
        let mut db = create_db_with_events(scratch.path());
        let csv_path = write_csv_file(
            &scratch,
            "data.csv",
            "user_id,ts,event_type,amount\n\
             alice,1700000000000000000,click,not_a_number\n",
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_plan(schema, &csv_path, vec![]);
        let err = execute_insert(&plan, &mut db).expect_err("type mismatch must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("row 2"), "got: {msg}");
                assert!(msg.contains("not_a_number"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── VALUES body returns a forward-compat error ─────────────────

    #[test]
    fn insert_values_returns_task_238_stub_error() {
        let scratch = Scratch::new("insert-values");
        let mut db = create_db_with_events(scratch.path());
        let schema = events_table_schema(&db);
        let plan = InsertPhysical {
            table: schema,
            body: InsertLogicalBody::Values(vec![vec![
                PropertyValue::String("alice".into()),
                PropertyValue::Timestamp(1_700_000_000_000_000_000),
                PropertyValue::String("click".into()),
                PropertyValue::Int(42),
            ]]),
            output_schema: empty_schema(),
        };
        let err = execute_insert(&plan, &mut db).expect_err("VALUES must error");
        match err {
            BqliteError::Plan(msg) => {
                assert!(msg.contains("TASK-238"), "got: {msg}");
            }
            other => panic!("expected Plan, got {other:?}"),
        }
    }

    // ── End-to-end: INSERT FROM via Engine::query ──────────────────

    #[test]
    fn engine_query_insert_from_csv_end_to_end() {
        let scratch = Scratch::new("insert-e2e");
        let mut db = create_db_with_events(scratch.path());
        let csv_path = write_csv_file(
            &scratch,
            "data.csv",
            "user_id,ts,event_type,amount\n\
             alice,1700000000000000000,click,42\n\
             bob,1700000000100000000,view,\n",
        );

        let engine = crate::Engine::new();
        let insert_sql = format!(
            "INSERT INTO events FROM '{}' WITH (format: 'csv')",
            csv_path.display()
        );
        let result = engine.query(&insert_sql, &mut db).expect("e2e insert");

        // INSERT produces no output rows.
        assert!(result.is_empty());

        // Verify the data landed in the manifest.
        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 2);
    }
}
