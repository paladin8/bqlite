//! `TableSchema` and `OperatorSchema` — the two schema types that
//! describe declared event tables and the output of each pipe stage.
//!
//! See `docs/design/type-system.md` §5 for the authoritative specification.
//!
//! - [`TableSchema`] is the shape of a declared table: a name, a list of
//!   [`ColumnDef`]s in DDL order, and indices identifying the three
//!   mandatory column roles (entity key, event time, event type). It is
//!   returned by the catalog (TASK-125) and constructed by `CREATE TABLE`
//!   execution in later waves. It validates the §5.1 schema-creation-time
//!   rules on construction.
//! - [`OperatorSchema`] is the contract between piped operators: an
//!   ordered `Vec<ColumnDef>` with name-based lookup, Arrow schema
//!   conversion, and a compatibility check against a required shape.
//!
//! Both types are foundational — `TableSchema` is what the catalog hands
//! to the planner, and `OperatorSchema` is what the planner propagates
//! through the logical and physical plan trees. See TASK-106 in
//! `TASKS.md` for the task definition.

use std::collections::HashSet;

use ::arrow::datatypes::{Field, Schema as ArrowSchema};
use serde::{Deserialize, Serialize};

use crate::arrow::bql_type_to_arrow;
use crate::error::{BqliteError, Result};
use crate::property::{BqlType, PropertyValue};

// ─────────────────────────────────────────────────────────────────────────────
// System columns
// ─────────────────────────────────────────────────────────────────────────────

/// Prefix reserved for bqlite system columns. User-declared columns may
/// not start with this prefix (see §5.1 rule 8).
pub const SYSTEM_COLUMN_PREFIX: &str = "__";

/// Name of the implicit system column that uniquely identifies a row at
/// ingest time. Always `Int`, non-nullable.
pub const SEQ_ID_COLUMN: &str = "__seq_id";

/// Name of the implicit system column that identifies the ingest batch a
/// row was written in. Always `Int`, non-nullable.
pub const BATCH_ID_COLUMN: &str = "__batch_id";

// ─────────────────────────────────────────────────────────────────────────────
// ColumnDef
// ─────────────────────────────────────────────────────────────────────────────

/// Definition of a single column.
///
/// See docs/design/type-system.md §3.1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name. Must be a valid identifier for user-declared columns;
    /// system columns use the reserved [`SYSTEM_COLUMN_PREFIX`].
    pub name: String,
    /// Static type.
    pub bql_type: BqlType,
    /// When true, the column's Arrow array may carry NULLs; when false,
    /// the null bitmap is guaranteed empty.
    pub nullable: bool,
    /// Default value applied when reading segments that predate this
    /// column. `None` means NULL — requires `nullable == true`.
    pub default_value: Option<PropertyValue>,
}

impl ColumnDef {
    /// Shortcut for a nullable column with no default.
    pub fn nullable(name: impl Into<String>, bql_type: BqlType) -> Self {
        Self {
            name: name.into(),
            bql_type,
            nullable: true,
            default_value: None,
        }
    }

    /// Shortcut for a non-nullable column.
    pub fn required(name: impl Into<String>, bql_type: BqlType) -> Self {
        Self {
            name: name.into(),
            bql_type,
            nullable: false,
            default_value: None,
        }
    }

    /// Attach a default value, enabling nullable-with-default semantics
    /// for schema evolution.
    pub fn with_default(mut self, default: PropertyValue) -> Self {
        self.default_value = Some(default);
        self
    }

    /// True if this is a reserved system column (name starts with `__`).
    pub fn is_system(&self) -> bool {
        self.name.starts_with(SYSTEM_COLUMN_PREFIX)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TableSchema
// ─────────────────────────────────────────────────────────────────────────────

/// Schema for a declared event table.
///
/// Encodes the declared columns plus metadata identifying the three
/// mandatory column roles (entity key, event time, event type). The
/// constructor enforces every rule listed in
/// docs/design/type-system.md §5.1; a `TableSchema` value is therefore
/// provably valid.
///
/// The declared column list does **not** include the implicit system
/// columns `__seq_id` and `__batch_id`. Use
/// [`TableSchema::logical_columns`] for the logical shape that includes
/// them — this is what the planner sees when resolving column
/// references on a scanned table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    name: String,
    columns: Vec<ColumnDef>,
    entity_key_index: usize,
    timestamp_index: usize,
    event_type_index: usize,
    version: u32,
}

impl TableSchema {
    /// Construct a `TableSchema`, applying the §5.1 validation rules.
    ///
    /// - `columns` is the declared column list in DDL order.
    /// - `entity_key`, `timestamp`, `event_type` are the names of the
    ///   three mandatory role columns; each must appear in `columns` and
    ///   all three must be distinct columns.
    ///
    /// Returns `BqliteError::Schema` on any violation.
    pub fn new(
        name: impl Into<String>,
        columns: Vec<ColumnDef>,
        entity_key: &str,
        timestamp: &str,
        event_type: &str,
    ) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(BqliteError::Schema("table name must not be empty".into()));
        }

        // §5.1 rules 4, 5, 8: valid identifiers, unique names, and no
        // `__` prefix on user-declared columns.
        let mut seen: HashSet<&str> = HashSet::with_capacity(columns.len());
        for col in &columns {
            if !is_valid_identifier(&col.name) {
                return Err(BqliteError::Schema(format!(
                    "column name `{}` is not a valid identifier",
                    col.name
                )));
            }
            if col.name.starts_with(SYSTEM_COLUMN_PREFIX) {
                return Err(BqliteError::Schema(format!(
                    "user-declared column `{}` may not start with `{SYSTEM_COLUMN_PREFIX}` (reserved for system columns)",
                    col.name
                )));
            }
            if !seen.insert(col.name.as_str()) {
                return Err(BqliteError::Schema(format!(
                    "duplicate column name `{}` in table `{name}`",
                    col.name
                )));
            }
        }

        // §5.1 rule 6: at least the three mandatory columns — enforced
        // by requiring each role name to resolve to an existing column.
        let entity_key_index = index_of_column(&columns, entity_key, "entity key")?;
        let timestamp_index = index_of_column(&columns, timestamp, "timestamp")?;
        let event_type_index = index_of_column(&columns, event_type, "event type")?;

        // The three role columns must be three distinct columns.
        // Collapsing two roles onto one column would confuse every
        // operator that reads role metadata.
        if entity_key_index == timestamp_index
            || entity_key_index == event_type_index
            || timestamp_index == event_type_index
        {
            return Err(BqliteError::Schema(format!(
                "entity key, timestamp, and event type must be three distinct columns in table `{name}`"
            )));
        }

        // §5.1 rules 1 and 7: entity key must be STRING or INT,
        // non-nullable, never List/Map.
        {
            let ek = &columns[entity_key_index];
            match ek.bql_type {
                BqlType::String | BqlType::Int => {}
                _ => {
                    return Err(BqliteError::Schema(format!(
                        "entity key column `{}` must be STRING or INT, found {}",
                        ek.name, ek.bql_type
                    )));
                }
            }
            if ek.nullable {
                return Err(BqliteError::Schema(format!(
                    "entity key column `{}` must be NOT NULL",
                    ek.name
                )));
            }
        }

        // §5.1 rules 2 and 7: event-time column must be TIMESTAMP,
        // non-nullable.
        {
            let ts = &columns[timestamp_index];
            if ts.bql_type != BqlType::Timestamp {
                return Err(BqliteError::Schema(format!(
                    "timestamp column `{}` must be TIMESTAMP, found {}",
                    ts.name, ts.bql_type
                )));
            }
            if ts.nullable {
                return Err(BqliteError::Schema(format!(
                    "timestamp column `{}` must be NOT NULL",
                    ts.name
                )));
            }
        }

        // §5.1 rules 3 and 7: event type column must be STRING,
        // non-nullable.
        {
            let et = &columns[event_type_index];
            if et.bql_type != BqlType::String {
                return Err(BqliteError::Schema(format!(
                    "event type column `{}` must be STRING, found {}",
                    et.name, et.bql_type
                )));
            }
            if et.nullable {
                return Err(BqliteError::Schema(format!(
                    "event type column `{}` must be NOT NULL",
                    et.name
                )));
            }
        }

        // Any ColumnDef with a default value must be self-consistent:
        // NULL defaults are only allowed on nullable columns, and
        // scalar defaults must match the column's declared type.
        for col in &columns {
            if let Some(default) = &col.default_value {
                validate_default_for_column(col, default)?;
            }
        }

        Ok(Self {
            name,
            columns,
            entity_key_index,
            timestamp_index,
            event_type_index,
            version: 0,
        })
    }

    /// Table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Declared columns in DDL order. Does **not** include the implicit
    /// `__seq_id` / `__batch_id` system columns.
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// The entity key column.
    pub fn entity_key_column(&self) -> &ColumnDef {
        &self.columns[self.entity_key_index]
    }

    /// The event-time column.
    pub fn timestamp_column(&self) -> &ColumnDef {
        &self.columns[self.timestamp_index]
    }

    /// The event type column.
    pub fn event_type_column(&self) -> &ColumnDef {
        &self.columns[self.event_type_index]
    }

    /// Index of the entity key column within [`Self::columns`].
    pub fn entity_key_index(&self) -> usize {
        self.entity_key_index
    }

    /// Index of the timestamp column within [`Self::columns`].
    pub fn timestamp_index(&self) -> usize {
        self.timestamp_index
    }

    /// Index of the event type column within [`Self::columns`].
    pub fn event_type_index(&self) -> usize {
        self.event_type_index
    }

    /// Monotonically increasing schema version. Starts at 0; `ALTER
    /// TABLE ADD COLUMN` bumps it.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Return a copy of this schema with a different version number.
    ///
    /// Used by `ALTER TABLE ADD COLUMN` to bump the version after
    /// appending a column. The version is metadata-only — it does not
    /// re-validate the column list.
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Look up a declared column by name. System columns are not
    /// included — call [`Self::logical_columns`] if those are needed.
    pub fn column(&self, name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
    }

    /// Iterate the logical columns: declared columns followed by the
    /// implicit `__seq_id` and `__batch_id` system columns.
    pub fn logical_columns(&self) -> impl Iterator<Item = ColumnDef> + '_ {
        self.columns
            .iter()
            .cloned()
            .chain(std::iter::once(ColumnDef::required(
                SEQ_ID_COLUMN,
                BqlType::Int,
            )))
            .chain(std::iter::once(ColumnDef::required(
                BATCH_ID_COLUMN,
                BqlType::Int,
            )))
    }
}

fn index_of_column(columns: &[ColumnDef], name: &str, role: &str) -> Result<usize> {
    columns.iter().position(|c| c.name == name).ok_or_else(|| {
        BqliteError::Schema(format!(
            "{role} column `{name}` not present in declared columns"
        ))
    })
}

fn validate_default_for_column(col: &ColumnDef, default: &PropertyValue) -> Result<()> {
    if matches!(default, PropertyValue::Null) {
        if !col.nullable {
            return Err(BqliteError::Schema(format!(
                "column `{}` is NOT NULL but has a NULL default",
                col.name
            )));
        }
        return Ok(());
    }
    // Type-check the default against the column's declared type for the
    // scalar cases. Full `PropertyValue` → `BqlType` inference (including
    // List/Map element checks) is shared with the Arrow mapping task
    // (TASK-107); for now List/Map defaults are only shape-checked.
    let type_matches = matches!(
        (&col.bql_type, default),
        (BqlType::Bool, PropertyValue::Bool(_))
            | (BqlType::Int, PropertyValue::Int(_))
            | (BqlType::Float, PropertyValue::Float(_))
            | (BqlType::String, PropertyValue::String(_))
            | (BqlType::Timestamp, PropertyValue::Timestamp(_))
            | (BqlType::List(_), PropertyValue::List(_))
            | (BqlType::Map(_), PropertyValue::Map(_))
    );
    if !type_matches {
        return Err(BqliteError::Schema(format!(
            "default value for column `{}` does not match declared type {}",
            col.name, col.bql_type
        )));
    }
    Ok(())
}

fn is_valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        None => false,
        Some(c) if !c.is_ascii_alphabetic() && c != '_' => false,
        _ => chars.all(|c| c.is_ascii_alphanumeric() || c == '_'),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OperatorSchema
// ─────────────────────────────────────────────────────────────────────────────

/// Output schema of a plan node — the contract between piped operators.
///
/// See docs/design/type-system.md §5.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSchema {
    columns: Vec<ColumnDef>,
}

impl OperatorSchema {
    /// Construct an `OperatorSchema` from an ordered list of columns.
    /// Column names must be unique.
    pub fn new(columns: Vec<ColumnDef>) -> Result<Self> {
        let mut seen: HashSet<&str> = HashSet::with_capacity(columns.len());
        for col in &columns {
            if !seen.insert(col.name.as_str()) {
                return Err(BqliteError::Schema(format!(
                    "duplicate column `{}` in operator schema",
                    col.name
                )));
            }
        }
        Ok(Self { columns })
    }

    /// Build the operator schema for a scan over `table`. Includes the
    /// declared columns and the implicit `__seq_id` / `__batch_id`
    /// system columns in that order.
    pub fn from_table(table: &TableSchema) -> Self {
        // `TableSchema` already enforces uniqueness and the system
        // columns are fixed, so the result is valid by construction.
        Self {
            columns: table.logical_columns().collect(),
        }
    }

    /// The columns in output order.
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// True if this schema has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Look up a column by name. Returns its position and definition.
    pub fn column(&self, name: &str) -> Option<(usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
    }

    /// Check that `self` satisfies every column required by `required`:
    /// each required column must be present by name with the same
    /// `BqlType`, and a `NOT NULL` requirement may not be satisfied by a
    /// nullable source column. Extra columns in `self` are allowed.
    ///
    /// Used by the planner to validate operator composition — e.g. a
    /// `FILTER` operator validates that its child produces every column
    /// referenced by the predicate.
    pub fn validate_against(&self, required: &OperatorSchema) -> Result<()> {
        for req in &required.columns {
            let Some((_, have)) = self.column(&req.name) else {
                return Err(BqliteError::Schema(format!(
                    "required column `{}` missing from operator schema",
                    req.name
                )));
            };
            if have.bql_type != req.bql_type {
                return Err(BqliteError::Schema(format!(
                    "column `{}`: required {}, found {}",
                    req.name, req.bql_type, have.bql_type
                )));
            }
            if !req.nullable && have.nullable {
                return Err(BqliteError::Schema(format!(
                    "column `{}` required NOT NULL but the available column is nullable",
                    req.name
                )));
            }
        }
        Ok(())
    }

    /// Convert to an Arrow [`ArrowSchema`].
    ///
    /// Delegates to [`crate::arrow::bql_type_to_arrow`] — the single
    /// source of truth for the `BqlType → Arrow` mapping specified in
    /// docs/design/type-system.md §7.1.
    pub fn to_arrow_schema(&self) -> ArrowSchema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(&c.name, bql_type_to_arrow(&c.bql_type), c.nullable))
            .collect();
        ArrowSchema::new(fields)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use ::arrow::datatypes::{DataType, TimeUnit};

    fn minimal_events() -> Vec<ColumnDef> {
        vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ]
    }

    fn minimal_schema() -> TableSchema {
        TableSchema::new("events", minimal_events(), "entity_id", "ts", "event_type")
            .expect("minimal events schema is valid")
    }

    #[test]
    fn minimal_schema_is_accepted() {
        let s = minimal_schema();
        assert_eq!(s.name(), "events");
        assert_eq!(s.columns().len(), 3);
        assert_eq!(s.entity_key_column().name, "entity_id");
        assert_eq!(s.timestamp_column().name, "ts");
        assert_eq!(s.event_type_column().name, "event_type");
        assert_eq!(s.entity_key_index(), 0);
        assert_eq!(s.timestamp_index(), 1);
        assert_eq!(s.event_type_index(), 2);
        assert_eq!(s.version(), 0);
    }

    #[test]
    fn int_entity_key_is_accepted() {
        let cols = vec![
            ColumnDef::required("uid", BqlType::Int),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ];
        let s = TableSchema::new("events", cols, "uid", "ts", "event_type").unwrap();
        assert_eq!(s.entity_key_column().bql_type, BqlType::Int);
    }

    #[test]
    fn property_columns_may_be_nullable() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Float),
            ColumnDef::nullable("tags", BqlType::List(Box::new(BqlType::String))),
        ];
        let s = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap();
        assert_eq!(s.columns().len(), 5);
        let (idx, col) = s.column("amount").unwrap();
        assert_eq!(idx, 3);
        assert!(col.nullable);
    }

    #[test]
    fn rejects_empty_table_name() {
        let err =
            TableSchema::new("", minimal_events(), "entity_id", "ts", "event_type").unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn rejects_duplicate_column_names() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("ts", BqlType::Int), // duplicate of timestamp column
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("duplicate"), "error = {msg}");
    }

    #[test]
    fn rejects_invalid_identifier() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("1bad", BqlType::Int),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("valid identifier"), "error = {msg}");
    }

    #[test]
    fn rejects_system_prefix_on_user_column() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("__hidden", BqlType::Int),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("reserved"), "error = {msg}");
    }

    #[test]
    fn rejects_missing_role_column() {
        let err = TableSchema::new("events", minimal_events(), "missing", "ts", "event_type")
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("entity key"), "error = {msg}");
    }

    #[test]
    fn rejects_nullable_entity_key() {
        let cols = vec![
            ColumnDef::nullable("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("NOT NULL"), "error = {msg}");
    }

    #[test]
    fn rejects_non_string_non_int_entity_key() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::Float),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("STRING or INT"), "error = {msg}");
    }

    #[test]
    fn rejects_list_entity_key() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::List(Box::new(BqlType::String))),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("STRING or INT"), "error = {msg}");
    }

    #[test]
    fn rejects_non_timestamp_event_time() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Int),
            ColumnDef::required("event_type", BqlType::String),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("TIMESTAMP"), "error = {msg}");
    }

    #[test]
    fn rejects_non_string_event_type() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::Int),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("STRING"), "error = {msg}");
    }

    #[test]
    fn rejects_collapsed_roles() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ];
        // Point entity key and event type at the same column.
        let err = TableSchema::new("events", cols, "entity_id", "ts", "entity_id").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("distinct"), "error = {msg}");
    }

    #[test]
    fn rejects_null_default_on_required_column() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String).with_default(PropertyValue::Null),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("NOT NULL"), "error = {msg}");
    }

    #[test]
    fn rejects_type_mismatched_default() {
        let cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Float)
                .with_default(PropertyValue::String("zero".into())),
        ];
        let err = TableSchema::new("events", cols, "entity_id", "ts", "event_type").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("default value"), "error = {msg}");
    }

    #[test]
    fn column_lookup_excludes_system_columns() {
        let s = minimal_schema();
        assert!(s.column("entity_id").is_some());
        assert!(s.column("__seq_id").is_none());
    }

    #[test]
    fn logical_columns_appends_system_columns() {
        let s = minimal_schema();
        let cols: Vec<ColumnDef> = s.logical_columns().collect();
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[3].name, SEQ_ID_COLUMN);
        assert_eq!(cols[3].bql_type, BqlType::Int);
        assert!(!cols[3].nullable);
        assert!(cols[3].is_system());
        assert_eq!(cols[4].name, BATCH_ID_COLUMN);
        assert!(cols[4].is_system());
    }

    #[test]
    fn serde_round_trip_preserves_indices() {
        let s = minimal_schema();
        let json = serde_json::to_string(&s).unwrap();
        let back: TableSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
        assert_eq!(back.entity_key_index(), 0);
        assert_eq!(back.timestamp_index(), 1);
        assert_eq!(back.event_type_index(), 2);
    }

    // ── OperatorSchema tests ────────────────────────────────────────────────

    #[test]
    fn operator_schema_column_lookup() {
        let s = OperatorSchema::new(vec![
            ColumnDef::required("a", BqlType::Int),
            ColumnDef::nullable("b", BqlType::String),
        ])
        .unwrap();
        assert_eq!(s.len(), 2);
        assert!(!s.is_empty());
        let (idx, col) = s.column("b").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.bql_type, BqlType::String);
        assert!(s.column("missing").is_none());
    }

    #[test]
    fn operator_schema_rejects_duplicates() {
        let err = OperatorSchema::new(vec![
            ColumnDef::required("a", BqlType::Int),
            ColumnDef::required("a", BqlType::String),
        ])
        .unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)));
    }

    #[test]
    fn operator_schema_empty_is_valid() {
        let s = OperatorSchema::new(vec![]).unwrap();
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        assert!(s.column("anything").is_none());
        assert_eq!(s.to_arrow_schema().fields().len(), 0);
    }

    #[test]
    fn operator_schema_from_table_has_system_columns() {
        let s = OperatorSchema::from_table(&minimal_schema());
        assert_eq!(s.len(), 5);
        assert!(s.column(SEQ_ID_COLUMN).is_some());
        assert!(s.column(BATCH_ID_COLUMN).is_some());
    }

    #[test]
    fn validate_against_accepts_superset() {
        let have = OperatorSchema::new(vec![
            ColumnDef::required("a", BqlType::Int),
            ColumnDef::required("b", BqlType::String),
            ColumnDef::nullable("c", BqlType::Float),
        ])
        .unwrap();
        let required = OperatorSchema::new(vec![
            ColumnDef::required("a", BqlType::Int),
            ColumnDef::required("b", BqlType::String),
        ])
        .unwrap();
        have.validate_against(&required).unwrap();
    }

    #[test]
    fn validate_against_rejects_missing_column() {
        let have = OperatorSchema::new(vec![ColumnDef::required("a", BqlType::Int)]).unwrap();
        let required = OperatorSchema::new(vec![ColumnDef::required("b", BqlType::Int)]).unwrap();
        let err = have.validate_against(&required).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("missing"), "error = {msg}");
    }

    #[test]
    fn validate_against_rejects_type_mismatch() {
        let have = OperatorSchema::new(vec![ColumnDef::required("a", BqlType::Int)]).unwrap();
        let required = OperatorSchema::new(vec![ColumnDef::required("a", BqlType::Float)]).unwrap();
        let err = have.validate_against(&required).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("required"), "error = {msg}");
    }

    #[test]
    fn validate_against_rejects_nullable_for_required_not_null() {
        let have = OperatorSchema::new(vec![ColumnDef::nullable("a", BqlType::Int)]).unwrap();
        let required = OperatorSchema::new(vec![ColumnDef::required("a", BqlType::Int)]).unwrap();
        let err = have.validate_against(&required).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("NOT NULL"), "error = {msg}");
    }

    #[test]
    fn validate_against_allows_not_null_for_required_nullable() {
        let have = OperatorSchema::new(vec![ColumnDef::required("a", BqlType::Int)]).unwrap();
        let required = OperatorSchema::new(vec![ColumnDef::nullable("a", BqlType::Int)]).unwrap();
        have.validate_against(&required).unwrap();
    }

    // ── Arrow conversion tests ──────────────────────────────────────────────

    #[test]
    fn to_arrow_schema_maps_scalars() {
        let s = OperatorSchema::new(vec![
            ColumnDef::required("b", BqlType::Bool),
            ColumnDef::required("i", BqlType::Int),
            ColumnDef::required("f", BqlType::Float),
            ColumnDef::required("s", BqlType::String),
            ColumnDef::required("t", BqlType::Timestamp),
        ])
        .unwrap();
        let arrow = s.to_arrow_schema();
        assert_eq!(arrow.fields().len(), 5);
        assert_eq!(arrow.field(0).data_type(), &DataType::Boolean);
        assert_eq!(arrow.field(1).data_type(), &DataType::Int64);
        assert_eq!(arrow.field(2).data_type(), &DataType::Float64);
        assert_eq!(arrow.field(3).data_type(), &DataType::Utf8View);
        assert_eq!(
            arrow.field(4).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert!(!arrow.field(0).is_nullable());
    }

    #[test]
    fn to_arrow_schema_maps_list() {
        let s = OperatorSchema::new(vec![ColumnDef::nullable(
            "tags",
            BqlType::List(Box::new(BqlType::String)),
        )])
        .unwrap();
        let arrow = s.to_arrow_schema();
        match arrow.field(0).data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.name(), "item");
                assert_eq!(inner.data_type(), &DataType::Utf8View);
                assert!(inner.is_nullable());
            }
            other => panic!("expected List, got {other:?}"),
        }
        assert!(arrow.field(0).is_nullable());
    }

    #[test]
    fn to_arrow_schema_maps_map() {
        let s = OperatorSchema::new(vec![ColumnDef::nullable(
            "props",
            BqlType::Map(Box::new(BqlType::Float)),
        )])
        .unwrap();
        let arrow = s.to_arrow_schema();
        let DataType::Map(entries, _keys_sorted) = arrow.field(0).data_type() else {
            panic!("expected Map, got {:?}", arrow.field(0).data_type());
        };
        assert_eq!(entries.name(), "entries");
        let DataType::Struct(inner) = entries.data_type() else {
            panic!("expected Struct entries, got {:?}", entries.data_type());
        };
        assert_eq!(inner.len(), 2);
        assert_eq!(inner[0].name(), "key");
        assert_eq!(inner[0].data_type(), &DataType::Utf8View);
        assert!(!inner[0].is_nullable());
        assert_eq!(inner[1].name(), "value");
        assert_eq!(inner[1].data_type(), &DataType::Float64);
        assert!(inner[1].is_nullable());
    }

    // ── ColumnDef smoke tests ──────────────────────────────────────────────

    #[test]
    fn column_def_is_system_flag() {
        assert!(!ColumnDef::required("user", BqlType::String).is_system());
        assert!(ColumnDef::required(SEQ_ID_COLUMN, BqlType::Int).is_system());
    }

    #[test]
    fn is_valid_identifier_rules() {
        assert!(is_valid_identifier("user_id"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("a1"));
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("1abc"));
        assert!(!is_valid_identifier("has-dash"));
        assert!(!is_valid_identifier("with space"));
    }
}
