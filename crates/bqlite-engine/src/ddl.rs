//! DDL execution and immediate-result operators (TASK-232).
//!
//! DDL statements (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`)
//! execute directly during the bind step — they mutate the manifest via
//! TASK-217's atomic update API and return an empty result. `DESCRIBE`
//! and `EXPLAIN` similarly produce their result at bind time and wrap
//! it in a [`ResultOperator`] that the engine drives normally.
//!
//! The [`ResultOperator`] is a trivial `PhysicalOperator` that yields
//! zero or more pre-computed batches and then exhausts. It unifies the
//! return type of `bind_physical` across data-plane and DDL paths so
//! the engine's drive loop does not need to branch.

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

use bqlite_core::{BqliteError, Catalog, OperatorSchema, Result};
use bqlite_operators::PhysicalOperator;
use bqlite_planner::explain::{build_explain_node, format_explain};
use bqlite_planner::physical::{
    AlterTableAddColumnPhysical, CreateTablePhysical, DescribePhysical, DropTablePhysical,
    ExplainPhysical,
};
use bqlite_storage::Database;

use bqlite_core::schema::TableSchema;

// ─────────────────────────────────────────────────────────────────────────────
// ResultOperator
// ─────────────────────────────────────────────────────────────────────────────

/// A trivial `PhysicalOperator` that yields pre-computed batches.
///
/// Used for DDL (empty result), DESCRIBE (one batch), and EXPLAIN
/// (one batch). The operator does no work — it just hands out the
/// batches it was given at construction, then exhausts.
pub struct ResultOperator {
    schema: OperatorSchema,
    batches: Vec<RecordBatch>,
    next_idx: usize,
}

impl ResultOperator {
    /// Wrap pre-computed batches in an operator.
    pub fn new(schema: OperatorSchema, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches,
            next_idx: 0,
        }
    }

    /// An empty-result operator (DDL statements).
    pub fn empty(schema: OperatorSchema) -> Self {
        Self::new(schema, Vec::new())
    }
}

impl PhysicalOperator for ResultOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.next_idx >= self.batches.len() {
            return Ok(None);
        }
        let batch = self.batches[self.next_idx].clone();
        self.next_idx += 1;
        Ok(Some(batch))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DDL execution
// ─────────────────────────────────────────────────────────────────────────────

/// Execute `CREATE TABLE` — validate schema, error on name collision,
/// atomically register the new table.
pub fn execute_create_table(plan: &CreateTablePhysical, db: &mut Database) -> Result<()> {
    let schema = TableSchema::new(
        &plan.name,
        plan.columns.clone(),
        &plan.entity_key,
        &plan.event_time,
        &plan.event_type,
    )?;
    db.create_table(plan.name.clone(), schema)
}

/// Execute `DROP TABLE` — error on missing table, atomically remove
/// the table entry and its segment inventory from the manifest.
pub fn execute_drop_table(plan: &DropTablePhysical, db: &mut Database) -> Result<()> {
    db.drop_table(&plan.name)
}

/// Execute `ALTER TABLE ADD COLUMN` — error on missing table or
/// duplicate column name, append the column, bump schema_version.
pub fn execute_alter_table_add_column(
    plan: &AlterTableAddColumnPhysical,
    db: &mut Database,
) -> Result<()> {
    db.alter_table_add_column(&plan.name, plan.column.clone())
}

// ─────────────────────────────────────────────────────────────────────────────
// DESCRIBE batch builder
// ─────────────────────────────────────────────────────────────────────────────

/// Build the four-column result batch for `DESCRIBE <table>`.
///
/// Columns: `(name: String, type: String, nullable: Bool, role: String)`
/// per query-language.md §20.5.
pub fn build_describe_batch(plan: &DescribePhysical, db: &Database) -> Result<RecordBatch> {
    let catalog = db.catalog();
    let table_schema = catalog.resolve_table(&plan.name)?;

    let columns = table_schema.columns();
    let entity_key_idx = table_schema.entity_key_index();
    let timestamp_idx = table_schema.timestamp_index();
    let event_type_idx = table_schema.event_type_index();

    let mut names: Vec<&str> = Vec::with_capacity(columns.len());
    let mut types: Vec<String> = Vec::with_capacity(columns.len());
    let mut nullables: Vec<bool> = Vec::with_capacity(columns.len());
    let mut roles: Vec<&str> = Vec::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        names.push(&col.name);
        types.push(col.bql_type.to_string());
        nullables.push(col.nullable);
        if i == entity_key_idx {
            roles.push("ENTITY KEY");
        } else if i == timestamp_idx {
            roles.push("EVENT TIME");
        } else if i == event_type_idx {
            roles.push("EVENT TYPE");
        } else {
            roles.push("");
        }
    }

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("name", DataType::Utf8View, false),
        Field::new("type", DataType::Utf8View, false),
        Field::new("nullable", DataType::Boolean, false),
        Field::new("role", DataType::Utf8View, false),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(StringViewArray::from(names)),
            Arc::new(StringViewArray::from(
                types.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(nullables)),
            Arc::new(StringViewArray::from(roles)),
        ],
    )
    .map_err(BqliteError::Arrow)?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPLAIN batch builder
// ─────────────────────────────────────────────────────────────────────────────

/// Build the single-column result batch for `EXPLAIN <pipeline>`.
///
/// Column: `(plan: String)` — one row containing the formatted plan
/// tree as a multi-line string.
pub fn build_explain_batch(plan: &ExplainPhysical) -> Result<RecordBatch> {
    let node = build_explain_node(&plan.plan);
    let text = format_explain(&node);

    let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "plan",
        DataType::Utf8View,
        false,
    )]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(StringViewArray::from(vec![text.as_str()]))],
    )
    .map_err(BqliteError::Arrow)?;

    Ok(batch)
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

    use bqlite_core::schema::ColumnDef;
    use bqlite_core::BqlType;

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
            path.push(format!("bqlite-engine-ddl-{label}-{pid}-{seq}"));
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

    // ── ResultOperator ────────────────────────────────────────────────

    #[test]
    fn result_operator_empty_exhausts_immediately() {
        let schema = OperatorSchema::new(Vec::new()).unwrap();
        let mut op = ResultOperator::empty(schema);
        assert!(op.next_batch().unwrap().is_none());
        // Idempotent.
        assert!(op.next_batch().unwrap().is_none());
    }

    #[test]
    fn result_operator_yields_precomputed_batches() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "x",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(StringViewArray::from(vec!["hello"]))],
        )
        .unwrap();
        let schema = OperatorSchema::new(vec![ColumnDef::required("x", BqlType::String)]).unwrap();
        let mut op = ResultOperator::new(schema, vec![batch]);

        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 1);
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── CREATE TABLE ─────────────────────────────────────────────────

    #[test]
    fn create_table_happy_path() {
        let scratch = Scratch::new("create");
        let mut db = Database::create(scratch.path()).unwrap();

        let plan = CreateTablePhysical {
            name: "clicks".into(),
            columns: vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event", BqlType::String),
            ],
            entity_key: "user_id".into(),
            event_time: "ts".into(),
            event_type: "event".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_create_table(&plan, &mut db).unwrap();

        let catalog = db.catalog();
        let schema = catalog.resolve_table("clicks").unwrap();
        assert_eq!(schema.name(), "clicks");
        assert_eq!(schema.columns().len(), 3);
    }

    #[test]
    fn create_table_name_collision_errors() {
        let scratch = Scratch::new("create-dup");
        let mut db = Database::create(scratch.path()).unwrap();

        // Create `events` table first so the duplicate attempt errors.
        let first = CreateTablePhysical {
            name: "events".into(),
            columns: vec![
                ColumnDef::required("e", BqlType::String),
                ColumnDef::required("t", BqlType::Timestamp),
                ColumnDef::required("ev", BqlType::String),
            ],
            entity_key: "e".into(),
            event_time: "t".into(),
            event_type: "ev".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_create_table(&first, &mut db).unwrap();

        // Now attempt to create the same table again.
        let plan = CreateTablePhysical {
            name: "events".into(),
            columns: vec![
                ColumnDef::required("e", BqlType::String),
                ColumnDef::required("t", BqlType::Timestamp),
                ColumnDef::required("ev", BqlType::String),
            ],
            entity_key: "e".into(),
            event_time: "t".into(),
            event_type: "ev".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        let err = execute_create_table(&plan, &mut db).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("already exists"), "{msg}"),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── DROP TABLE ───────────────────────────────────────────────────

    #[test]
    fn drop_table_happy_path() {
        let scratch = Scratch::new("drop");
        let mut db = Database::create(scratch.path()).unwrap();

        // Create a table so we can drop it.
        let create_plan = CreateTablePhysical {
            name: "clicks".into(),
            columns: vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event", BqlType::String),
            ],
            entity_key: "user_id".into(),
            event_time: "ts".into(),
            event_type: "event".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_create_table(&create_plan, &mut db).unwrap();

        let plan = DropTablePhysical {
            name: "clicks".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_drop_table(&plan, &mut db).unwrap();

        let catalog = db.catalog();
        assert!(catalog.resolve_table("clicks").is_err());
    }

    #[test]
    fn drop_table_missing_errors() {
        let scratch = Scratch::new("drop-missing");
        let mut db = Database::create(scratch.path()).unwrap();

        let plan = DropTablePhysical {
            name: "ghost".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        let err = execute_drop_table(&plan, &mut db).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("ghost"), "{msg}"),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── ALTER TABLE ADD COLUMN ──────────────────────────────────────

    #[test]
    fn alter_table_add_column_happy_path() {
        let scratch = Scratch::new("alter");
        let mut db = Database::create(scratch.path()).unwrap();

        // Create events table first.
        let create_plan = CreateTablePhysical {
            name: "events".into(),
            columns: vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            entity_key: "entity_id".into(),
            event_time: "ts".into(),
            event_type: "event_type".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_create_table(&create_plan, &mut db).unwrap();

        let plan = AlterTableAddColumnPhysical {
            name: "events".into(),
            column: ColumnDef::nullable("amount", BqlType::Float),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_alter_table_add_column(&plan, &mut db).unwrap();

        let catalog = db.catalog();
        let schema = catalog.resolve_table("events").unwrap();
        assert_eq!(schema.columns().len(), 4); // 3 original + 1 new
        assert!(schema.column("amount").is_some());
        assert_eq!(schema.version(), 1);
    }

    #[test]
    fn alter_table_missing_table_errors() {
        let scratch = Scratch::new("alter-missing");
        let mut db = Database::create(scratch.path()).unwrap();

        let plan = AlterTableAddColumnPhysical {
            name: "ghost".into(),
            column: ColumnDef::nullable("x", BqlType::Int),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        let err = execute_alter_table_add_column(&plan, &mut db).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("ghost"), "{msg}"),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn alter_table_duplicate_column_errors() {
        let scratch = Scratch::new("alter-dup");
        let mut db = Database::create(scratch.path()).unwrap();

        // Create events table first.
        let create_plan = CreateTablePhysical {
            name: "events".into(),
            columns: vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            entity_key: "entity_id".into(),
            event_time: "ts".into(),
            event_type: "event_type".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_create_table(&create_plan, &mut db).unwrap();

        let plan = AlterTableAddColumnPhysical {
            name: "events".into(),
            column: ColumnDef::required("entity_id", BqlType::String),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        let err = execute_alter_table_add_column(&plan, &mut db).unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("already exists"), "{msg}"),
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    // ── DESCRIBE ────────────────────────────────────────────────────

    #[test]
    fn describe_happy_path() {
        let scratch = Scratch::new("describe");
        let mut db = Database::create(scratch.path()).unwrap();

        // Create events table first.
        let create_plan = CreateTablePhysical {
            name: "events".into(),
            columns: vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            entity_key: "entity_id".into(),
            event_time: "ts".into(),
            event_type: "event_type".into(),
            output_schema: OperatorSchema::new(Vec::new()).unwrap(),
        };
        execute_create_table(&create_plan, &mut db).unwrap();

        let plan = DescribePhysical {
            name: "events".into(),
            output_schema: OperatorSchema::new(vec![
                ColumnDef::required("name", BqlType::String),
                ColumnDef::required("type", BqlType::String),
                ColumnDef::required("nullable", BqlType::Bool),
                ColumnDef::required("role", BqlType::String),
            ])
            .unwrap(),
        };
        let batch = build_describe_batch(&plan, &db).unwrap();

        // Events table has 3 columns.
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 4);

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(names.value(0), "entity_id");
        assert_eq!(names.value(1), "ts");
        assert_eq!(names.value(2), "event_type");

        let roles = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(roles.value(0), "ENTITY KEY");
        assert_eq!(roles.value(1), "EVENT TIME");
        assert_eq!(roles.value(2), "EVENT TYPE");

        let nullable_arr = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        // All three bootstrap columns are NOT NULL.
        assert!(!nullable_arr.value(0));
        assert!(!nullable_arr.value(1));
        assert!(!nullable_arr.value(2));
    }

    #[test]
    fn describe_missing_table_errors() {
        let scratch = Scratch::new("describe-missing");
        let db = Database::create(scratch.path()).unwrap();

        let plan = DescribePhysical {
            name: "ghost".into(),
            output_schema: OperatorSchema::new(vec![
                ColumnDef::required("name", BqlType::String),
                ColumnDef::required("type", BqlType::String),
                ColumnDef::required("nullable", BqlType::Bool),
                ColumnDef::required("role", BqlType::String),
            ])
            .unwrap(),
        };
        let err = build_describe_batch(&plan, &db).unwrap_err();
        match err {
            BqliteError::Plan(msg) => assert!(msg.contains("ghost"), "{msg}"),
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── EXPLAIN ─────────────────────────────────────────────────────

    #[test]
    fn explain_produces_single_row_batch() {
        use bqlite_planner::physical::{PhysicalPlan, ScanPhysical};

        let plan = ExplainPhysical {
            plan: Box::new(PhysicalPlan::Scan(ScanPhysical {
                table: "events".into(),
                time_range: None,
                scan_predicates: Vec::new(),
                projected_columns: Vec::new(),
                output_schema: OperatorSchema::new(vec![
                    ColumnDef::required("entity_id", BqlType::String),
                    ColumnDef::required("ts", BqlType::Timestamp),
                    ColumnDef::required("event_type", BqlType::String),
                ])
                .unwrap(),
                entity_key_col: "entity_id".to_string(),
                timestamp_col: "ts".to_string(),
            })),
            output_schema: OperatorSchema::new(vec![ColumnDef::required("plan", BqlType::String)])
                .unwrap(),
        };
        let batch = build_explain_batch(&plan).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0);
        assert!(text.contains("Scan(events)"), "explain text: {text}");
    }
}
