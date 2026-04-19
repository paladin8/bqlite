//! INSERT execution — binds an [`InsertPhysical`] plan node into a
//! concrete write path that streams rows through the partitioner and
//! segment writer.
//!
//! # Scope
//!
//! Two `InsertBody` variants:
//!
//! - **`From`** — opens a source file, dispatches to the appropriate
//!   format reader, pushes each row into the [`Partitioner`], then
//!   drains sorted buckets through the [`SegmentWriter`].
//!   - CSV: [`CsvEventReader`] (TASK-233)
//!   - JSONL: [`JsonlEventReader`] (TASK-410)
//!   - Parquet: [`ParquetEventReader`] (TASK-449)
//! - **`Values`** — converts planner-coerced literal tuples into
//!   [`Event`] records using the resolved `TableSchema` role indices,
//!   then feeds them through the same partitioner + writer pipeline
//!   (TASK-238).
//!
//! The function returns `()` on success (INSERT produces no result
//! rows); the caller wraps it in an empty [`ResultOperator`].

use std::path::Path;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::PropertyValue;
use bqlite_core::schema::TableSchema;
use bqlite_core::time::Timestamp;
use bqlite_planner::logical::{IngestFormat, InsertFromDescriptor, InsertLogicalBody};
use bqlite_planner::InsertPhysical;
use bqlite_storage::ingest::csv_reader::{CsvEventReader, CsvReaderOptions};
use bqlite_storage::ingest::json::JsonlEventReader;
use bqlite_storage::ingest::parquet::ParquetEventReader;
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
        InsertLogicalBody::Values(rows) => execute_insert_values(rows, &plan.table, db),
    }
}

/// Execute `INSERT INTO <table> FROM '<path>' WITH (...)`.
///
/// Dispatches to the format-specific reader (`csv` or `jsonl`),
/// streams rows into the partitioner, then drains sorted buckets
/// through the segment writer.
fn execute_insert_from(
    descriptor: &InsertFromDescriptor,
    table: &bqlite_core::schema::TableSchema,
    db: &mut Database,
) -> Result<()> {
    let path = Path::new(&descriptor.path);

    // 1. Allocate a batch_id and construct the partitioner.
    let batch_id = db.allocate_batch_id(table.name())?;
    let shard_count = db.manifest().shard_count;
    let mut partitioner = Partitioner::new(
        shard_count,
        DEFAULT_WINDOW_DAYS,
        batch_id,
        DEFAULT_INGEST_BUDGET_BYTES,
    )?;

    // 2. Stream events into the partitioner using the appropriate reader.
    let row_count = match descriptor.format {
        IngestFormat::Csv => {
            let csv_options = CsvReaderOptions::from_options(&descriptor.options)?;
            let mut reader =
                CsvEventReader::open(path, table, &descriptor.column_map, &csv_options)?;
            stream_reader_into_partitioner(&mut reader, &mut partitioner)?
        }
        IngestFormat::JsonL => {
            let mut reader = JsonlEventReader::open(path, table, &descriptor.column_map)?;
            stream_reader_into_partitioner(&mut reader, &mut partitioner)?
        }
        IngestFormat::Parquet => {
            let mut reader = ParquetEventReader::open(path, table, &descriptor.column_map)?;
            stream_reader_into_partitioner(&mut reader, &mut partitioner)?
        }
    };

    if row_count == 0 {
        // Nothing to write — the source had no data rows.
        return Ok(());
    }

    // 3. Drain sorted buckets through the segment writer.
    let mut writer = SegmentWriter::new(db);
    writer.write_partitioner(table.name(), partitioner)?;

    Ok(())
}

/// Drain a pull-based event reader into a [`Partitioner`], returning
/// the number of events pushed.
///
/// The reader must implement `next_event() -> Result<Option<Event>>`.
/// This helper avoids duplicating the streaming loop for each format.
fn stream_reader_into_partitioner<R>(reader: &mut R, partitioner: &mut Partitioner) -> Result<u64>
where
    R: EventReader,
{
    let mut count: u64 = 0;
    while let Some(event) = reader.next_event()? {
        partitioner.push_event(event)?;
        count += 1;
    }
    Ok(count)
}

/// Trait alias for the pull-based reader interface shared by
/// [`CsvEventReader`] and [`JsonlEventReader`].
///
/// Both readers expose `next_event() -> Result<Option<Event>>` but
/// live in separate modules and carry format-specific state. This
/// private trait lets `stream_reader_into_partitioner` be generic
/// without a public `EventReader` abstraction in the library surface.
trait EventReader {
    fn next_event(&mut self) -> Result<Option<Event>>;
}

impl<R: std::io::BufRead> EventReader for CsvEventReader<R> {
    fn next_event(&mut self) -> Result<Option<Event>> {
        CsvEventReader::next_event(self)
    }
}

impl<R: std::io::BufRead> EventReader for JsonlEventReader<R> {
    fn next_event(&mut self) -> Result<Option<Event>> {
        JsonlEventReader::next_event(self)
    }
}

impl EventReader for ParquetEventReader {
    fn next_event(&mut self) -> Result<Option<Event>> {
        ParquetEventReader::next_event(self)
    }
}

/// Execute `INSERT INTO <table> VALUES (row), ...`.
///
/// Converts each planner-coerced row into an [`Event`] using the
/// table schema's role indices (entity key, timestamp, event type),
/// then feeds all events through the partitioner + segment writer
/// pipeline — the same path `execute_insert_from` uses for CSV rows.
///
/// Returns immediately (no-op) when `rows` is empty.
fn execute_insert_values(
    rows: &[Vec<PropertyValue>],
    table: &TableSchema,
    db: &mut Database,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let entity_key_idx = table.entity_key_index();
    let timestamp_idx = table.timestamp_index();
    let event_type_idx = table.event_type_index();

    let batch_id = db.allocate_batch_id(table.name())?;
    let shard_count = db.manifest().shard_count;
    let mut partitioner = Partitioner::new(
        shard_count,
        DEFAULT_WINDOW_DAYS,
        batch_id,
        DEFAULT_INGEST_BUDGET_BYTES,
    )?;

    for (row_idx, row) in rows.iter().enumerate() {
        let entity = values_entity_id(&row[entity_key_idx], row_idx)?;
        let timestamp = values_timestamp(&row[timestamp_idx], row_idx)?;
        let event_type = values_event_type(&row[event_type_idx], row_idx)?;

        // Collect non-role columns as the event's property bag.
        let mut properties: Vec<(String, PropertyValue)> =
            Vec::with_capacity(row.len().saturating_sub(3));
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx != entity_key_idx && col_idx != timestamp_idx && col_idx != event_type_idx {
                properties.push((table.columns()[col_idx].name.clone(), value.clone()));
            }
        }

        let event = Event::with_properties(entity, timestamp, event_type, properties);
        partitioner.push_event(event)?;
    }

    let mut writer = SegmentWriter::new(db);
    writer.write_partitioner(table.name(), partitioner)?;

    Ok(())
}

/// Extract an [`EntityId`] from a planner-coerced entity-key value.
///
/// The planner guarantees the entity-key column is `String` or `Int`
/// and non-null; any other variant indicates an internal inconsistency.
fn values_entity_id(value: &PropertyValue, row_idx: usize) -> Result<EntityId> {
    match value {
        PropertyValue::String(s) => Ok(EntityId::String(s.clone())),
        PropertyValue::Int(n) => Ok(EntityId::Int(*n)),
        other => Err(BqliteError::Execution(format!(
            "INSERT VALUES row {row_idx}: entity-key has unexpected runtime type {other:?} \
             (planner should have coerced or rejected this)"
        ))),
    }
}

/// Extract a [`Timestamp`] from a planner-coerced event-time value.
fn values_timestamp(value: &PropertyValue, row_idx: usize) -> Result<Timestamp> {
    match value {
        PropertyValue::Timestamp(ns) => Ok(Timestamp::from_nanos(*ns)),
        other => Err(BqliteError::Execution(format!(
            "INSERT VALUES row {row_idx}: timestamp has unexpected runtime type {other:?} \
             (planner should have coerced or rejected this)"
        ))),
    }
}

/// Extract an event-type string from a planner-coerced event-type value.
fn values_event_type(value: &PropertyValue, row_idx: usize) -> Result<String> {
    match value {
        PropertyValue::String(s) => Ok(s.clone()),
        other => Err(BqliteError::Execution(format!(
            "INSERT VALUES row {row_idx}: event-type has unexpected runtime type {other:?} \
             (planner should have coerced or rejected this)"
        ))),
    }
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

    // ── INSERT FROM JSONL: helpers ─────────────────────────────────

    /// Write JSONL text to a temp file and return the path.
    fn write_jsonl_file(scratch: &Scratch, name: &str, content: &str) -> PathBuf {
        let path = scratch.path().join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    fn insert_from_jsonl_plan(
        table: TableSchema,
        jsonl_path: &Path,
        column_map: Vec<(String, String)>,
    ) -> InsertPhysical {
        InsertPhysical {
            table,
            body: InsertLogicalBody::From(InsertFromDescriptor {
                path: jsonl_path.to_string_lossy().into_owned(),
                format: IngestFormat::JsonL,
                options: Vec::new(),
                column_map,
            }),
            output_schema: empty_schema(),
        }
    }

    // ── INSERT FROM JSONL: happy path ──────────────────────────────

    #[test]
    fn insert_from_jsonl_writes_segments_and_registers_in_manifest() {
        let scratch = Scratch::new("jsonl-happy");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = write_jsonl_file(
            &scratch,
            "data.jsonl",
            r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","amount":42}
{"user_id":"bob","ts":1700000000100000000,"event_type":"view","amount":null}
{"user_id":"charlie","ts":1700000000200000000,"event_type":"purchase","amount":10}
"#,
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_jsonl_plan(schema, &jsonl_path, vec![]);
        execute_insert(&plan, &mut db).expect("JSONL insert must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 3);
    }

    // ── INSERT FROM JSONL: nested "properties" object ──────────────

    #[test]
    fn insert_from_jsonl_nested_properties_object() {
        let scratch = Scratch::new("jsonl-nested");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = write_jsonl_file(
            &scratch,
            "data.jsonl",
            r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","properties":{"amount":99}}
"#,
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_jsonl_plan(schema, &jsonl_path, vec![]);
        execute_insert(&plan, &mut db).expect("JSONL nested properties must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 1);
    }

    // ── INSERT FROM JSONL: column mapping ─────────────────────────

    #[test]
    fn insert_from_jsonl_with_column_map() {
        let scratch = Scratch::new("jsonl-map");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = write_jsonl_file(
            &scratch,
            "data.jsonl",
            r#"{"uid":"alice","time":1700000000000000000,"evt":"click","val":42}
"#,
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_jsonl_plan(
            schema,
            &jsonl_path,
            vec![
                ("uid".into(), "user_id".into()),
                ("time".into(), "ts".into()),
                ("evt".into(), "event_type".into()),
                ("val".into(), "amount".into()),
            ],
        );
        execute_insert(&plan, &mut db).expect("JSONL with map must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 1);
    }

    // ── INSERT FROM JSONL: empty file is no-op ─────────────────────

    #[test]
    fn insert_from_empty_jsonl_is_noop() {
        let scratch = Scratch::new("jsonl-empty");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = write_jsonl_file(&scratch, "data.jsonl", "");

        let schema = events_table_schema(&db);
        let plan = insert_from_jsonl_plan(schema, &jsonl_path, vec![]);
        execute_insert(&plan, &mut db).expect("empty JSONL must succeed");

        assert!(db.manifest().tables["events"].windows.is_empty());
    }

    // ── INSERT FROM JSONL: missing file errors ─────────────────────

    #[test]
    fn insert_from_missing_jsonl_errors() {
        let scratch = Scratch::new("jsonl-missing");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = scratch.path().join("nonexistent.jsonl");

        let schema = events_table_schema(&db);
        let plan = insert_from_jsonl_plan(schema, &jsonl_path, vec![]);
        let err = execute_insert(&plan, &mut db).expect_err("missing file must error");
        assert!(matches!(err, BqliteError::Io(_)));
    }

    // ── INSERT FROM JSONL: type error carries row number ───────────

    #[test]
    fn insert_from_jsonl_type_error_has_row_number() {
        let scratch = Scratch::new("jsonl-type-err");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = write_jsonl_file(
            &scratch,
            "data.jsonl",
            r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","amount":42}
{"user_id":"bob","ts":"not_a_timestamp","event_type":"view"}
"#,
        );

        let schema = events_table_schema(&db);
        let plan = insert_from_jsonl_plan(schema, &jsonl_path, vec![]);
        let err = execute_insert(&plan, &mut db).expect_err("type error must propagate");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("row 2"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── End-to-end: INSERT FROM JSONL via Engine::query ───────────

    #[test]
    fn engine_query_insert_from_jsonl_end_to_end() {
        let scratch = Scratch::new("jsonl-e2e");
        let mut db = create_db_with_events(scratch.path());
        let jsonl_path = write_jsonl_file(
            &scratch,
            "data.jsonl",
            r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","amount":42}
{"user_id":"bob","ts":1700000000100000000,"event_type":"view"}
"#,
        );

        let engine = crate::Engine::new();
        let insert_sql = format!(
            "INSERT INTO events FROM '{}' WITH (format: 'jsonl')",
            jsonl_path.display()
        );
        let result = engine
            .query(&insert_sql, &mut db)
            .expect("e2e JSONL insert");
        assert!(result.is_empty());

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 2);
    }

    // ── INSERT VALUES: happy path ──────────────────────────────────

    #[test]
    fn insert_values_writes_rows_to_manifest() {
        let scratch = Scratch::new("insert-values-happy");
        let mut db = create_db_with_events(scratch.path());
        let schema = events_table_schema(&db);
        let plan = InsertPhysical {
            table: schema,
            body: InsertLogicalBody::Values(vec![
                vec![
                    PropertyValue::String("alice".into()),
                    PropertyValue::Timestamp(1_700_000_000_000_000_000),
                    PropertyValue::String("click".into()),
                    PropertyValue::Int(42),
                ],
                vec![
                    PropertyValue::String("bob".into()),
                    PropertyValue::Timestamp(1_700_000_000_100_000_000),
                    PropertyValue::String("view".into()),
                    PropertyValue::Null,
                ],
            ]),
            output_schema: empty_schema(),
        };
        execute_insert(&plan, &mut db).expect("VALUES insert must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 2);
    }

    // ── INSERT VALUES: non-standard role column ordering ──────────

    /// Exercises the property-bag exclusion path when the role columns
    /// are NOT at positions 0, 1, 2. Table has a regular column first,
    /// then entity key, then timestamp, then event type — so
    /// `entity_key_index` = 1, `timestamp_index` = 2, `event_type_index` = 3.
    #[test]
    fn insert_values_non_default_role_column_ordering() {
        let scratch = Scratch::new("insert-values-reordered");
        let mut db = Database::create(scratch.path()).expect("create db");
        let engine = crate::Engine::new();
        engine
            .query(
                "CREATE TABLE readings (\
                     value FLOAT, \
                     device_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     kind STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .expect("create readings table");

        let schema = db.manifest().tables["readings"].schema.clone();
        // Confirm the non-standard ordering is what we expect.
        assert_eq!(schema.entity_key_index(), 1);
        assert_eq!(schema.timestamp_index(), 2);
        assert_eq!(schema.event_type_index(), 3);

        let plan = InsertPhysical {
            table: schema,
            body: InsertLogicalBody::Values(vec![
                // value, device_id, ts, kind
                vec![
                    PropertyValue::Float(23.5),
                    PropertyValue::String("sensor_1".into()),
                    PropertyValue::Timestamp(1_700_000_000_000_000_000),
                    PropertyValue::String("temperature".into()),
                ],
                vec![
                    PropertyValue::Null, // nullable float
                    PropertyValue::String("sensor_2".into()),
                    PropertyValue::Timestamp(1_700_000_000_100_000_000),
                    PropertyValue::String("humidity".into()),
                ],
            ]),
            output_schema: empty_schema(),
        };
        execute_insert(&plan, &mut db).expect("VALUES with reordered roles must succeed");

        let total_rows: u64 = db.manifest().tables["readings"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 2);
    }

    // ── INSERT VALUES: empty rows is a no-op ───────────────────────

    #[test]
    fn insert_values_empty_rows_is_noop() {
        let scratch = Scratch::new("insert-values-empty");
        let mut db = create_db_with_events(scratch.path());
        let schema = events_table_schema(&db);
        let plan = InsertPhysical {
            table: schema,
            body: InsertLogicalBody::Values(vec![]),
            output_schema: empty_schema(),
        };
        execute_insert(&plan, &mut db).expect("empty VALUES must succeed");
        assert!(db.manifest().tables["events"].windows.is_empty());
    }

    // ── INSERT VALUES: multiple entities in one batch ──────────────

    #[test]
    fn insert_values_multiple_entities_all_land() {
        let scratch = Scratch::new("insert-values-multi-entity");
        let mut db = create_db_with_events(scratch.path());
        let schema = events_table_schema(&db);

        // Three different entity ids; three events each.
        let mut rows = Vec::new();
        for entity in ["alice", "bob", "charlie"] {
            for ts_offset in [0_i64, 1_000_000, 2_000_000] {
                rows.push(vec![
                    PropertyValue::String(entity.into()),
                    PropertyValue::Timestamp(1_700_000_000_000_000_000 + ts_offset),
                    PropertyValue::String("action".into()),
                    PropertyValue::Null,
                ]);
            }
        }
        let plan = InsertPhysical {
            table: schema,
            body: InsertLogicalBody::Values(rows),
            output_schema: empty_schema(),
        };
        execute_insert(&plan, &mut db).expect("multi-entity VALUES must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 9);
    }

    // ── End-to-end: INSERT VALUES via Engine::query ───────────────

    /// Round-trip: `CREATE TABLE → INSERT VALUES → assert rows in manifest`.
    ///
    /// This test verifies the manifest row count after ingest. For the
    /// full scan-path proof that queries return real rows through the
    /// manifest-backed `ManifestSegmentReader` (TASK-244), see
    /// `bind::tests::insert_values_then_query_returns_real_rows`.
    #[test]
    fn engine_query_insert_values_round_trip() {
        let scratch = Scratch::new("insert-values-e2e");
        let mut db = create_db_with_events(scratch.path());
        let engine = crate::Engine::new();

        // INSERT two rows via the text query path.
        let result = engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 42), \
                 ('bob',   1700000000100000000, 'view',  NULL)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");
        assert!(result.is_empty(), "INSERT produces no rows");

        // Verify the rows landed in the manifest.
        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 2);
    }

    // ── Engine error paths: planner rejects bad VALUES ─────────────

    #[test]
    fn engine_query_insert_values_wrong_arity_errors() {
        let scratch = Scratch::new("insert-values-arity");
        let mut db = create_db_with_events(scratch.path());
        let engine = crate::Engine::new();

        // events table has 4 columns; supply only 3.
        let err = engine
            .query(
                "INSERT INTO events VALUES ('alice', 1700000000000000000, 'click')",
                &mut db,
            )
            .expect_err("wrong arity must error");
        let msg = err.to_string();
        assert!(
            msg.contains("arity") || msg.contains("3") || msg.contains("4"),
            "got: {msg}"
        );
    }

    #[test]
    fn engine_query_insert_values_null_into_not_null_errors() {
        let scratch = Scratch::new("insert-values-null");
        let mut db = create_db_with_events(scratch.path());
        let engine = crate::Engine::new();

        // user_id is NOT NULL — NULL in that slot must be rejected.
        let err = engine
            .query(
                "INSERT INTO events VALUES (NULL, 1700000000000000000, 'click', 42)",
                &mut db,
            )
            .expect_err("NULL into NOT NULL must error");
        let msg = err.to_string();
        assert!(
            msg.contains("NULL") || msg.contains("NOT NULL"),
            "got: {msg}"
        );
    }

    // ── INSERT FROM Parquet: helpers ──────────────────────────────

    /// Write a Parquet file from a single Arrow RecordBatch and return
    /// the path. Uses `parquet::arrow::ArrowWriter`.
    fn write_parquet_file(
        scratch: &Scratch,
        name: &str,
        batch: arrow::record_batch::RecordBatch,
    ) -> PathBuf {
        use parquet::arrow::ArrowWriter;
        let path = scratch.path().join(name);
        let file = fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    fn insert_from_parquet_plan(
        table: TableSchema,
        parquet_path: &Path,
        column_map: Vec<(String, String)>,
    ) -> InsertPhysical {
        InsertPhysical {
            table,
            body: InsertLogicalBody::From(InsertFromDescriptor {
                path: parquet_path.to_string_lossy().into_owned(),
                format: IngestFormat::Parquet,
                options: Vec::new(),
                column_map,
            }),
            output_schema: empty_schema(),
        }
    }

    // ── INSERT FROM Parquet: happy path ───────────────────────────

    #[test]
    fn insert_from_parquet_writes_segments_and_registers_in_manifest() {
        use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let scratch = Scratch::new("parquet-happy");
        let mut db = create_db_with_events(scratch.path());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                    1_700_000_000_100_000_000_i64,
                    1_700_000_000_200_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click", "view", "purchase"])),
                Arc::new(Int64Array::from(vec![Some(42), None, Some(10)])),
            ],
        )
        .unwrap();

        let parquet_path = write_parquet_file(&scratch, "data.parquet", batch);
        let schema = events_table_schema(&db);
        let plan = insert_from_parquet_plan(schema, &parquet_path, vec![]);
        execute_insert(&plan, &mut db).expect("Parquet insert must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 3);
    }

    // ── INSERT FROM Parquet: column mapping ───────────────────────

    #[test]
    fn insert_from_parquet_with_column_map_remaps_columns() {
        use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let scratch = Scratch::new("parquet-map");
        let mut db = create_db_with_events(scratch.path());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("uid", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("evt", DataType::Utf8, false),
            Field::new("val", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(Int64Array::from(vec![Some(42)])),
            ],
        )
        .unwrap();

        let parquet_path = write_parquet_file(&scratch, "data.parquet", batch);
        let schema = events_table_schema(&db);
        let plan = insert_from_parquet_plan(
            schema,
            &parquet_path,
            vec![
                ("uid".into(), "user_id".into()),
                ("time".into(), "ts".into()),
                ("evt".into(), "event_type".into()),
                ("val".into(), "amount".into()),
            ],
        );
        execute_insert(&plan, &mut db).expect("Parquet insert with map must succeed");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 1);
    }

    // ── INSERT FROM Parquet: empty file is no-op ──────────────────

    #[test]
    fn insert_from_empty_parquet_is_noop() {
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let scratch = Scratch::new("parquet-empty");
        let mut db = create_db_with_events(scratch.path());

        // Write a Parquet file with schema but zero rows.
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let empty_batch = RecordBatch::new_empty(arrow_schema);
        let parquet_path = write_parquet_file(&scratch, "data.parquet", empty_batch);

        let schema = events_table_schema(&db);
        let plan = insert_from_parquet_plan(schema, &parquet_path, vec![]);
        execute_insert(&plan, &mut db).expect("empty Parquet must succeed");

        assert!(db.manifest().tables["events"].windows.is_empty());
    }

    // ── INSERT FROM Parquet: missing file errors ───────────────────

    #[test]
    fn insert_from_missing_parquet_errors() {
        let scratch = Scratch::new("parquet-missing");
        let mut db = create_db_with_events(scratch.path());
        let parquet_path = scratch.path().join("nonexistent.parquet");

        let schema = events_table_schema(&db);
        let plan = insert_from_parquet_plan(schema, &parquet_path, vec![]);
        let err = execute_insert(&plan, &mut db).expect_err("missing file must error");
        assert!(matches!(err, BqliteError::Io(_)));
    }

    // ── End-to-end: INSERT FROM Parquet via Engine::query ─────────

    #[test]
    fn engine_query_insert_from_parquet_end_to_end() {
        use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let scratch = Scratch::new("parquet-e2e");
        let mut db = create_db_with_events(scratch.path());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                    1_700_000_000_100_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click", "view"])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
            ],
        )
        .unwrap();

        let parquet_path = write_parquet_file(&scratch, "data.parquet", batch);

        let engine = crate::Engine::new();
        let insert_sql = format!(
            "INSERT INTO events FROM '{}' WITH (format: 'parquet')",
            parquet_path.display()
        );
        let result = engine
            .query(&insert_sql, &mut db)
            .expect("e2e Parquet insert");
        assert!(result.is_empty());

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 2);
    }

    // ── Width-consolidation: Int32 Parquet column → BQL Int ────────

    #[test]
    fn insert_from_parquet_int32_column_consolidates_to_i64() {
        use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let scratch = Scratch::new("parquet-int32");
        let mut db = create_db_with_events(scratch.path());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, true), // Int32, not Int64
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(Int32Array::from(vec![Some(99_i32)])),
            ],
        )
        .unwrap();

        let parquet_path = write_parquet_file(&scratch, "data.parquet", batch);
        let schema = events_table_schema(&db);
        let plan = insert_from_parquet_plan(schema, &parquet_path, vec![]);
        // Int32 column must be accepted and consolidated to i64.
        execute_insert(&plan, &mut db).expect("Int32 column must be accepted");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 1);
    }

    // ── Timestamp normalization: microseconds → nanoseconds ────────

    #[test]
    fn insert_from_parquet_microsecond_timestamp_normalizes_to_nanos() {
        use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let scratch = Scratch::new("parquet-micros");
        let mut db = create_db_with_events(scratch.path());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
        ]));
        let micros = 1_700_000_000_000_000_i64;
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampMicrosecondArray::from(vec![micros])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ],
        )
        .unwrap();

        let parquet_path = write_parquet_file(&scratch, "data.parquet", batch);
        let schema = events_table_schema(&db);
        let plan = insert_from_parquet_plan(schema, &parquet_path, vec![]);
        execute_insert(&plan, &mut db).expect("microsecond timestamp must be accepted");

        let total_rows: u64 = db.manifest().tables["events"]
            .windows
            .iter()
            .flat_map(|w| &w.shards)
            .flatten()
            .map(|seg| seg.row_count)
            .sum();
        assert_eq!(total_rows, 1);
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
