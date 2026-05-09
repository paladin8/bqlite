//! Streaming JSONL reader — converts a newline-delimited JSON file into
//! a stream of [`Event`] records ready for the ingest partitioner.
//!
//! # Responsibilities
//!
//! 1. Open a `.jsonl` / `.ndjson` file and read it line by line.
//! 2. Parse each non-empty line as a flat JSON object
//!    (`serde_json::Map<String, Value>`).
//! 3. Conditionally flatten a nested `"properties"` sub-object into the
//!    row's field map (without overwriting top-level keys). This supports
//!    the common event-tracking convention where event metadata lives
//!    under a `properties` key:
//!    ```text
//!    {"user_id": "alice", "ts": 1700000000000000000, "event_type":
//!     "click", "properties": {"amount": 42}}
//!    ```
//!    is treated identically to:
//!    ```text
//!    {"user_id": "alice", "ts": 1700000000000000000, "event_type":
//!     "click", "amount": 42}
//!    ```
//!    **Exception**: flattening is skipped when the resolved column
//!    mapping contains a source key literally named `"properties"`. This
//!    preserves the value for a target column such as
//!    `properties MAP(STRING)` and prevents silent data loss.
//! 4. Apply the column mapping from the planner's
//!    `InsertFromDescriptor` to resolve each source key to its target
//!    column in the [`TableSchema`].
//! 5. For each row, coerce JSON values to [`PropertyValue`]s matching
//!    the target column types, populate the entity id / timestamp /
//!    event-type role columns, and emit an [`Event`].
//! 6. Report errors with 1-based row numbers so the caller can surface
//!    a useful diagnostic.
//!
//! # Supported JSON value coercions
//!
//! | JSON type | Target BQL type | Notes |
//! |-----------|-----------------|-------|
//! | `null`    | any (nullable)  | `PropertyValue::Null` |
//! | `bool`    | BOOL            | direct |
//! | `number`  | INT             | integer JSON numbers |
//! | `number`  | FLOAT           | any JSON number |
//! | `number`  | TIMESTAMP       | nanosecond integer |
//! | `string`  | STRING          | direct |
//! | `string`  | INT / FLOAT / TIMESTAMP / BOOL | parsed from string repr |
//! | `number`  | STRING          | converted to decimal string repr |
//! | `bool`    | STRING          | `"true"` / `"false"` |
//! | `object`  | MAP(V)          | recursive coercion of values |
//! | `array`   | LIST(T)         | recursive coercion of elements |
//!
//! ## Loose string coercion
//!
//! For `STRING`-typed columns, JSON numbers and booleans are accepted and
//! converted to their string representations (`42` → `"42"`, `true` →
//! `"true"`). This includes entity-key columns: `{"user_id": 42}` ingest
//! into a `user_id STRING` column produces entity key `"42"`. Callers
//! that need stricter type enforcement should validate source data before
//! ingestion.
//!
//! # Wave 4 scope (TASK-410)
//!
//! This is the Wave 4 JSONL reader. Parquet ingest is TASK-449.

use std::io::{self, BufRead};
use std::path::Path;

use serde_json::Value;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::{BqlType, PropertyValue};
use bqlite_core::schema::TableSchema;
use bqlite_core::time::Timestamp;

// ─────────────────────────────────────────────────────────────────────────────
// Internal types
// ─────────────────────────────────────────────────────────────────────────────

/// Resolved mapping from a source JSON key to a target column in the
/// [`TableSchema`].
#[derive(Debug)]
struct ResolvedColumn {
    /// Key name to look up in the flattened JSON object.
    source_key: String,
    /// Name of the target column in the table schema.
    target_name: String,
    /// Type of the target column.
    target_type: BqlType,
    /// Whether the target column is nullable.
    nullable: bool,
    /// Which schema role this column fills, if any.
    role: ColumnRole,
}

/// Schema role of a mapped column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnRole {
    EntityKey,
    Timestamp,
    EventType,
    Property,
}

// ─────────────────────────────────────────────────────────────────────────────
// JsonlEventReader
// ─────────────────────────────────────────────────────────────────────────────

/// A pull-based JSONL event reader.
///
/// Constructed with a file path (or arbitrary `BufRead`), the target
/// table schema, and the resolved column mapping. Each call to
/// [`JsonlEventReader::next_event`] reads the next non-empty line,
/// parses and coerces it, and returns one [`Event`]; returns `None` at
/// EOF.
///
/// Empty lines and lines consisting only of whitespace are skipped
/// silently (standard JSONL practice). The reader is generic over `R:
/// BufRead` so tests can inject in-memory cursors.
#[derive(Debug)]
pub struct JsonlEventReader<R: BufRead> {
    reader: R,
    line_buf: String,
    /// Resolved column mappings — one entry per target column.
    columns: Vec<ResolvedColumn>,
    /// 1-based row number for error messages (counts non-empty lines).
    row_number: u64,
    /// Cached count of `Property` role entries, used to pre-size the
    /// per-event property `Vec` without re-scanning on every row.
    property_count: usize,
    /// Whether to flatten a top-level `"properties"` sub-object.
    ///
    /// Set to `false` when the resolved column mapping contains a source
    /// key named `"properties"`, so that the column's actual value is
    /// preserved rather than being treated as a flattening envelope.
    flatten_properties: bool,
}

impl JsonlEventReader<io::BufReader<std::fs::File>> {
    /// Open a JSONL file on disk and resolve the column mapping.
    ///
    /// # Arguments
    ///
    /// * `path` — filesystem path to the `.jsonl` / `.ndjson` file.
    /// * `table` — target table schema.
    /// * `column_map` — `(source_key, target_column)` pairs from the
    ///   planner. Empty means "match JSON object keys to table column
    ///   names directly."
    pub fn open(path: &Path, table: &TableSchema, column_map: &[(String, String)]) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(|e| {
            BqliteError::Io(io::Error::new(
                e.kind(),
                format!("JSONL reader: failed to open '{}': {e}", path.display()),
            ))
        })?;
        let buf = io::BufReader::new(file);
        Self::from_reader(buf, table, column_map)
    }
}

impl<R: BufRead> JsonlEventReader<R> {
    /// Construct a reader from an arbitrary `BufRead` source.
    ///
    /// Used by [`JsonlEventReader::open`] (file) and by tests
    /// (in-memory cursor).
    pub fn from_reader(
        reader: R,
        table: &TableSchema,
        column_map: &[(String, String)],
    ) -> Result<Self> {
        let mut me = Self {
            reader,
            line_buf: String::new(),
            columns: Vec::new(),
            row_number: 0,
            property_count: 0,
            // Initialized after resolve_columns — see below.
            flatten_properties: false,
        };
        me.resolve_columns(table, column_map)?;
        me.property_count = me
            .columns
            .iter()
            .filter(|c| c.role == ColumnRole::Property)
            .count();
        // Enable properties-flattening only when no resolved column claims
        // `"properties"` — checked on both the source key (passthrough mode:
        // schema column literally named `"properties"`) and the target name
        // (explicit-map mode: `map: (metadata AS properties)`). Either way
        // the `"properties"` JSON value must be preserved as column data
        // rather than consumed as a flattening envelope.
        me.flatten_properties = !me
            .columns
            .iter()
            .any(|c| c.source_key == "properties" || c.target_name == "properties");
        Ok(me)
    }

    /// Resolve the source-to-target column mapping from the schema and
    /// optional explicit map clause.
    fn resolve_columns(
        &mut self,
        table: &TableSchema,
        column_map: &[(String, String)],
    ) -> Result<()> {
        if column_map.is_empty() {
            // Passthrough mode: match JSON key names to table column
            // names. Required (non-nullable) columns must be present in
            // each JSON object at runtime; nullable columns may be absent.
            for col_def in table.columns() {
                let role = role_for(table, &col_def.name);
                self.columns.push(ResolvedColumn {
                    source_key: col_def.name.clone(),
                    target_name: col_def.name.clone(),
                    target_type: col_def.bql_type.clone(),
                    nullable: col_def.nullable,
                    role,
                });
            }
        } else {
            // Explicit map clause: add the mapped pairs first, then
            // auto-include unambiguous passthrough matches for remaining
            // columns.
            for (src, tgt) in column_map {
                let (_, col_def) = table.column(tgt).ok_or_else(|| {
                    BqliteError::Execution(format!(
                        "JSONL reader: map target '{tgt}' is not a column on table '{}' \
                         (this should have been caught by the planner)",
                        table.name()
                    ))
                })?;
                let role = role_for(table, tgt);
                self.columns.push(ResolvedColumn {
                    source_key: src.clone(),
                    target_name: tgt.clone(),
                    target_type: col_def.bql_type.clone(),
                    nullable: col_def.nullable,
                    role,
                });
            }

            // Also pick up passthrough columns: table columns whose name
            // matches a JSON key and that are not already covered by the
            // explicit map.
            let mapped_targets: std::collections::HashSet<&str> =
                column_map.iter().map(|(_, t)| t.as_str()).collect();
            for col_def in table.columns() {
                if mapped_targets.contains(col_def.name.as_str()) {
                    continue;
                }
                // Include this column under its own name as a passthrough.
                let role = role_for(table, &col_def.name);
                self.columns.push(ResolvedColumn {
                    source_key: col_def.name.clone(),
                    target_name: col_def.name.clone(),
                    target_type: col_def.bql_type.clone(),
                    nullable: col_def.nullable,
                    role,
                });
            }
        }

        // Validate that all three role columns are resolved.
        let has_entity = self.columns.iter().any(|c| c.role == ColumnRole::EntityKey);
        let has_ts = self.columns.iter().any(|c| c.role == ColumnRole::Timestamp);
        let has_et = self.columns.iter().any(|c| c.role == ColumnRole::EventType);
        if !has_entity {
            return Err(BqliteError::Execution(format!(
                "JSONL reader: entity-key column '{}' is not mapped from any JSON key",
                table.columns()[table.entity_key_index()].name
            )));
        }
        if !has_ts {
            return Err(BqliteError::Execution(format!(
                "JSONL reader: event-time column '{}' is not mapped from any JSON key",
                table.columns()[table.timestamp_index()].name
            )));
        }
        if !has_et {
            return Err(BqliteError::Execution(format!(
                "JSONL reader: event-type column '{}' is not mapped from any JSON key",
                table.columns()[table.event_type_index()].name
            )));
        }

        Ok(())
    }

    /// Read the next non-empty line and convert it into an [`Event`].
    ///
    /// Returns `Ok(None)` at EOF. Empty / whitespace-only lines are
    /// skipped. Errors are annotated with the 1-based row number.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            self.line_buf.clear();
            let bytes_read = self.reader.read_line(&mut self.line_buf).map_err(|e| {
                BqliteError::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "JSONL reader: I/O error near row {}: {e}",
                        self.row_number + 1
                    ),
                ))
            })?;

            if bytes_read == 0 {
                // EOF
                return Ok(None);
            }

            let trimmed = self.line_buf.trim();
            if trimmed.is_empty() {
                // Skip blank lines (valid JSONL practice).
                continue;
            }

            self.row_number += 1;
            return self.parse_line(trimmed).map(Some);
        }
    }

    /// Parse one non-empty, trimmed line into an [`Event`].
    fn parse_line(&self, line: &str) -> Result<Event> {
        let raw: Value = serde_json::from_str(line).map_err(|e| {
            BqliteError::Execution(format!(
                "JSONL reader row {}: invalid JSON: {e}",
                self.row_number
            ))
        })?;

        let mut obj = match raw {
            Value::Object(m) => m,
            other => {
                return Err(BqliteError::Execution(format!(
                    "JSONL reader row {}: expected a JSON object, got {}",
                    self.row_number,
                    json_type_name(&other),
                )));
            }
        };

        // Conditionally flatten a nested "properties" sub-object.
        // Only performed when no resolved column claims "properties" as
        // its source key, so that a literal `properties` column on the
        // target schema (e.g. `properties MAP(STRING)`) is not silently
        // discarded. Without this guard the column's value would be
        // removed before lookup, producing a misleading NULL or "missing"
        // error rather than the actual data.
        if self.flatten_properties {
            if let Some(Value::Object(props)) = obj.remove("properties") {
                for (k, v) in props {
                    obj.entry(k).or_insert(v);
                }
            }
        }

        let mut entity: Option<EntityId> = None;
        let mut timestamp: Option<Timestamp> = None;
        let mut event_type: Option<String> = None;
        let mut properties: Vec<(String, PropertyValue)> = Vec::with_capacity(self.property_count);

        for col in &self.columns {
            let json_val = obj.get(&col.source_key);

            // Handle missing / null values.
            let json_val = match json_val {
                None | Some(Value::Null) => {
                    if col.nullable {
                        if col.role == ColumnRole::Property {
                            properties.push((col.target_name.clone(), PropertyValue::Null));
                        }
                        // For role columns (entity/ts/type): `TableSchema::new`
                        // enforces NOT NULL on all three, so `col.nullable` is
                        // always `false` for them and this branch is unreachable
                        // in practice. We `continue` here to let the
                        // `ok_or_else` guards below catch any schema-level
                        // violation with a clear error.
                        continue;
                    } else {
                        return Err(BqliteError::Execution(format!(
                            "JSONL reader row {}: NOT NULL column '{}' is missing or null",
                            self.row_number, col.target_name
                        )));
                    }
                }
                Some(v) => v,
            };

            match col.role {
                ColumnRole::EntityKey => {
                    entity = Some(coerce_entity_id(
                        json_val,
                        &col.target_type,
                        &col.target_name,
                        self.row_number,
                    )?);
                }
                ColumnRole::Timestamp => {
                    timestamp = Some(coerce_timestamp(
                        json_val,
                        &col.target_name,
                        self.row_number,
                    )?);
                }
                ColumnRole::EventType => {
                    event_type = Some(coerce_event_type(
                        json_val,
                        &col.target_name,
                        self.row_number,
                    )?);
                }
                ColumnRole::Property => {
                    let value = coerce_property_value(
                        json_val,
                        &col.target_type,
                        &col.target_name,
                        self.row_number,
                    )?;
                    properties.push((col.target_name.clone(), value));
                }
            }
        }

        let entity = entity.ok_or_else(|| {
            BqliteError::Execution(format!(
                "JSONL reader row {}: entity-key column was not populated",
                self.row_number
            ))
        })?;
        let timestamp = timestamp.ok_or_else(|| {
            BqliteError::Execution(format!(
                "JSONL reader row {}: timestamp column was not populated",
                self.row_number
            ))
        })?;
        let event_type = event_type.ok_or_else(|| {
            BqliteError::Execution(format!(
                "JSONL reader row {}: event-type column was not populated",
                self.row_number
            ))
        })?;

        Ok(Event::with_properties(
            entity, timestamp, event_type, properties,
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Column-role helper
// ─────────────────────────────────────────────────────────────────────────────

fn role_for(table: &TableSchema, target_name: &str) -> ColumnRole {
    let cols = table.columns();
    if cols[table.entity_key_index()].name == target_name {
        ColumnRole::EntityKey
    } else if cols[table.timestamp_index()].name == target_name {
        ColumnRole::Timestamp
    } else if cols[table.event_type_index()].name == target_name {
        ColumnRole::EventType
    } else {
        ColumnRole::Property
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Coercion helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Coerce a JSON value to an [`EntityId`].
///
/// Entity keys are `STRING` or `INT` by schema design; any other type
/// is a schema violation caught at table-creation time.
fn coerce_entity_id(
    val: &Value,
    target_type: &BqlType,
    col_name: &str,
    row: u64,
) -> Result<EntityId> {
    match target_type {
        BqlType::String => {
            let s = coerce_to_string(val, col_name, row)?;
            Ok(EntityId::String(s))
        }
        BqlType::Int => {
            let n = coerce_to_i64(val, col_name, row)?;
            Ok(EntityId::Int(n))
        }
        other => Err(BqliteError::Execution(format!(
            "JSONL reader row {row}: entity-key column '{col_name}' has unsupported type \
             {other:?}; only STRING and INT are valid entity-key types"
        ))),
    }
}

/// Coerce a JSON value to a [`Timestamp`].
///
/// Accepts:
/// - JSON number (integer) → nanoseconds since Unix epoch
/// - JSON string (integer text) → parsed as nanoseconds
fn coerce_timestamp(val: &Value, col_name: &str, row: u64) -> Result<Timestamp> {
    let nanos = coerce_to_i64(val, col_name, row)?;
    Ok(Timestamp::from_nanos(nanos))
}

/// Coerce a JSON value to an event-type string.
fn coerce_event_type(val: &Value, col_name: &str, row: u64) -> Result<String> {
    coerce_to_string(val, col_name, row)
}

/// Coerce a JSON value to a [`PropertyValue`] for the given target type.
///
/// Supports:
/// - Scalar types: Bool, Int, Float, String, Timestamp
/// - Collection types: List(T), Map(V)
///
/// JSON strings are parsed into non-String types when the schema
/// requires it (e.g., `"42"` → `PropertyValue::Int(42)`).
fn coerce_property_value(
    val: &Value,
    target_type: &BqlType,
    col_name: &str,
    row: u64,
) -> Result<PropertyValue> {
    match target_type {
        BqlType::Bool => {
            let b = coerce_to_bool(val, col_name, row)?;
            Ok(PropertyValue::Bool(b))
        }
        BqlType::Int => {
            let n = coerce_to_i64(val, col_name, row)?;
            Ok(PropertyValue::Int(n))
        }
        BqlType::Float => {
            let f = coerce_to_f64(val, col_name, row)?;
            Ok(PropertyValue::Float(f))
        }
        BqlType::String => {
            let s = coerce_to_string(val, col_name, row)?;
            Ok(PropertyValue::String(s))
        }
        BqlType::Timestamp => {
            let nanos = coerce_to_i64(val, col_name, row)?;
            Ok(PropertyValue::Timestamp(nanos))
        }
        BqlType::List(elem_type) => {
            let arr = match val {
                Value::Array(a) => a,
                other => {
                    return Err(BqliteError::Execution(format!(
                        "JSONL reader row {row}: column '{col_name}' expects a JSON array, \
                         got {}",
                        json_type_name(other)
                    )));
                }
            };
            let mut items = Vec::with_capacity(arr.len());
            for (i, item) in arr.iter().enumerate() {
                let pv = if matches!(item, Value::Null) {
                    PropertyValue::Null
                } else {
                    coerce_property_value(item, elem_type, &format!("{col_name}[{i}]"), row)?
                };
                items.push(pv);
            }
            Ok(PropertyValue::List(items))
        }
        BqlType::Map(val_type) => {
            let map_obj = match val {
                Value::Object(m) => m,
                other => {
                    return Err(BqliteError::Execution(format!(
                        "JSONL reader row {row}: column '{col_name}' expects a JSON object, \
                         got {}",
                        json_type_name(other)
                    )));
                }
            };
            let mut pairs = Vec::with_capacity(map_obj.len());
            for (k, v) in map_obj {
                let pv = if matches!(v, Value::Null) {
                    PropertyValue::Null
                } else {
                    coerce_property_value(v, val_type, &format!("{col_name}.{k}"), row)?
                };
                pairs.push((k.clone(), pv));
            }
            Ok(PropertyValue::Map(pairs))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Primitive coercions
// ─────────────────────────────────────────────────────────────────────────────

fn coerce_to_bool(val: &Value, col_name: &str, row: u64) -> Result<bool> {
    match val {
        Value::Bool(b) => Ok(*b),
        Value::Number(n) => {
            if n.as_i64() == Some(1) {
                Ok(true)
            } else if n.as_i64() == Some(0) {
                Ok(false)
            } else {
                Err(BqliteError::Execution(format!(
                    "JSONL reader row {row}: cannot coerce number {n} to BOOL \
                     for column '{col_name}' — only 0 and 1 are accepted"
                )))
            }
        }
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => Err(BqliteError::Execution(format!(
                "JSONL reader row {row}: cannot parse '{s}' as BOOL for column '{col_name}'"
            ))),
        },
        other => Err(BqliteError::Execution(format!(
            "JSONL reader row {row}: cannot coerce {} to BOOL for column '{col_name}'",
            json_type_name(other)
        ))),
    }
}

fn coerce_to_i64(val: &Value, col_name: &str, row: u64) -> Result<i64> {
    match val {
        Value::Number(n) => n.as_i64().ok_or_else(|| {
            BqliteError::Execution(format!(
                "JSONL reader row {row}: number {n} cannot be represented as INT \
                 for column '{col_name}'"
            ))
        }),
        Value::String(s) => s.parse::<i64>().map_err(|_| {
            BqliteError::Execution(format!(
                "JSONL reader row {row}: cannot parse '{s}' as INT for column '{col_name}'"
            ))
        }),
        other => Err(BqliteError::Execution(format!(
            "JSONL reader row {row}: cannot coerce {} to INT for column '{col_name}'",
            json_type_name(other)
        ))),
    }
}

fn coerce_to_f64(val: &Value, col_name: &str, row: u64) -> Result<f64> {
    match val {
        Value::Number(n) => n.as_f64().ok_or_else(|| {
            BqliteError::Execution(format!(
                "JSONL reader row {row}: number {n} cannot be represented as FLOAT \
                 for column '{col_name}'"
            ))
        }),
        Value::String(s) => s.parse::<f64>().map_err(|_| {
            BqliteError::Execution(format!(
                "JSONL reader row {row}: cannot parse '{s}' as FLOAT for column '{col_name}'"
            ))
        }),
        other => Err(BqliteError::Execution(format!(
            "JSONL reader row {row}: cannot coerce {} to FLOAT for column '{col_name}'",
            json_type_name(other)
        ))),
    }
}

fn coerce_to_string(val: &Value, col_name: &str, row: u64) -> Result<String> {
    match val {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        other => Err(BqliteError::Execution(format!(
            "JSONL reader row {row}: cannot coerce {} to STRING for column '{col_name}'",
            json_type_name(other)
        ))),
    }
}

/// Human-readable name for a JSON value's type (for error messages).
fn json_type_name(val: &Value) -> &'static str {
    match val {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::io;

    use bqlite_core::event::EntityId;
    use bqlite_core::property::{BqlType, PropertyValue};
    use bqlite_core::schema::{ColumnDef, TableSchema};
    use proptest::prelude::*;

    use super::*;

    // ── Schema helpers ─────────────────────────────────────────────

    fn events_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Int),
                ColumnDef::nullable("country", BqlType::String),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .expect("schema")
    }

    fn reader_from_str(
        jsonl: &str,
        table: &TableSchema,
        column_map: &[(String, String)],
    ) -> JsonlEventReader<io::BufReader<io::Cursor<Vec<u8>>>> {
        let cursor = io::Cursor::new(jsonl.as_bytes().to_vec());
        let buf = io::BufReader::new(cursor);
        JsonlEventReader::from_reader(buf, table, column_map).unwrap()
    }

    fn collect_events(
        reader: &mut JsonlEventReader<io::BufReader<io::Cursor<Vec<u8>>>>,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        while let Some(ev) = reader.next_event().unwrap() {
            events.push(ev);
        }
        events
    }

    // ── Happy path: flat objects ────────────────────────────────────

    #[test]
    fn flat_objects_passthrough_mode() {
        let jsonl = r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","amount":42,"country":"US"}
{"user_id":"bob","ts":1700000000100000000,"event_type":"view","amount":null,"country":"UK"}
"#;
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[0].timestamp_nanos(), 1_700_000_000_000_000_000);
        assert_eq!(events[0].event_type, "click");
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(42)));
        assert_eq!(
            events[0].get("country"),
            Some(&PropertyValue::String("US".into()))
        );

        assert_eq!(events[1].entity, EntityId::from("bob"));
        assert_eq!(events[1].get("amount"), Some(&PropertyValue::Null));
    }

    // ── Nested "properties" object is flattened ────────────────────

    #[test]
    fn nested_properties_object_is_flattened() {
        let jsonl = r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","properties":{"amount":42,"country":"US"}}
"#;
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(42)));
        assert_eq!(
            events[0].get("country"),
            Some(&PropertyValue::String("US".into()))
        );
    }

    // ── Top-level keys take precedence over "properties" ───────────

    #[test]
    fn top_level_key_wins_over_properties() {
        // Top-level `amount` = 99 should win over `properties.amount` = 42.
        let jsonl = r#"{"user_id":"alice","ts":1700000000000000000,"event_type":"click","amount":99,"properties":{"amount":42}}
"#;
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(99)));
    }

    // ── Blank lines are skipped ────────────────────────────────────

    #[test]
    fn blank_lines_are_skipped() {
        let jsonl =
            "\n\n{\"user_id\":\"alice\",\"ts\":1700000000000000000,\"event_type\":\"click\"}\n\n";
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let events = collect_events(&mut reader);
        assert_eq!(events.len(), 1);
    }

    // ── Column mapping ─────────────────────────────────────────────

    #[test]
    fn explicit_column_map_remaps_keys() {
        let jsonl = r#"{"uid":"alice","time":1700000000000000000,"evt":"click","val":42}
"#;
        let schema = events_schema();
        let column_map: Vec<(String, String)> = vec![
            ("uid".into(), "user_id".into()),
            ("time".into(), "ts".into()),
            ("evt".into(), "event_type".into()),
            ("val".into(), "amount".into()),
        ];
        let mut reader = reader_from_str(jsonl, &schema, &column_map);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(42)));
    }

    // ── EOF on empty input ─────────────────────────────────────────

    #[test]
    fn empty_input_returns_none() {
        let schema = events_schema();
        let mut reader = reader_from_str("", &schema, &[]);
        assert!(reader.next_event().unwrap().is_none());
    }

    // ── Error: non-JSON line ───────────────────────────────────────

    #[test]
    fn non_json_line_errors_with_row_number() {
        let jsonl = "{\"user_id\":\"alice\",\"ts\":1700000000000000000,\"event_type\":\"click\"}\nnot json\n";
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        reader.next_event().unwrap(); // first line succeeds
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("row 2"), "got: {msg}");
                assert!(msg.contains("invalid JSON"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: NOT NULL column absent ─────────────────────────────

    #[test]
    fn missing_not_null_column_errors_with_row_number() {
        // Missing "event_type" (NOT NULL) → should error on row 1
        let jsonl = "{\"user_id\":\"alice\",\"ts\":1700000000000000000}\n";
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("row 1"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: JSON array at top level ────────────────────────────

    #[test]
    fn json_array_at_top_level_errors() {
        let jsonl = "[1, 2, 3]\n";
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("expected a JSON object"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Nullable column absent → PropertyValue::Null ──────────────

    #[test]
    fn absent_nullable_column_becomes_null() {
        // `amount` and `country` are nullable; omitting them should
        // produce Null property values.
        let jsonl = "{\"user_id\":\"alice\",\"ts\":1700000000000000000,\"event_type\":\"click\"}\n";
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let events = collect_events(&mut reader);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Null));
        assert_eq!(events[0].get("country"), Some(&PropertyValue::Null));
    }

    // ── String coercion for timestamp field ───────────────────────

    #[test]
    fn string_timestamp_is_coerced() {
        let jsonl =
            "{\"user_id\":\"alice\",\"ts\":\"1700000000000000000\",\"event_type\":\"click\"}\n";
        let schema = events_schema();
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        let events = collect_events(&mut reader);
        assert_eq!(events[0].timestamp_nanos(), 1_700_000_000_000_000_000);
    }

    // ── Multiple rows, row counter advances ───────────────────────

    #[test]
    fn row_counter_advances_for_error_messages() {
        // Three valid rows, then an error on row 4.
        let valid = "{\"user_id\":\"alice\",\"ts\":1700000000000000000,\"event_type\":\"click\"}\n";
        let invalid = "oops\n";
        let jsonl = format!("{valid}{valid}{valid}{invalid}");
        let schema = events_schema();
        let mut reader = reader_from_str(&jsonl, &schema, &[]);
        for _ in 0..3 {
            reader.next_event().unwrap();
        }
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => assert!(msg.contains("row 4"), "got: {msg}"),
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── "properties" column-name collision ────────────────────────
    //
    // When the schema contains a column literally named `"properties"`,
    // `flatten_properties` must be `false` and the column's JSON value
    // must be preserved rather than being removed and merged as a
    // flattening envelope.

    #[test]
    fn schema_column_named_properties_is_preserved_not_flattened() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("properties", BqlType::Map(Box::new(BqlType::Int))),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .expect("schema");

        // The `"properties"` value is an object — identical in shape to
        // the nested-flattening convention — but must NOT be flattened
        // because the schema has a column literally named `"properties"`.
        let jsonl = "{\"user_id\":\"alice\",\"ts\":1700000000000000000,\"event_type\":\"click\",\"properties\":{\"amount\":42}}\n";
        let mut reader = reader_from_str(jsonl, &schema, &[]);
        assert!(
            !reader.flatten_properties,
            "flatten_properties must be false when schema has a 'properties' column"
        );
        let events = collect_events(&mut reader);
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].get("properties"),
            Some(&PropertyValue::Map(vec![(
                "amount".into(),
                PropertyValue::Int(42)
            )]))
        );
    }

    // ── Proptest: primitive coercions ──────────────────────────────
    //
    // Exercises the coercion helpers with arbitrarily generated inputs to
    // catch edge cases that hand-crafted examples miss. Each test covers
    // one clear invariant: a value that can be losslessly represented in
    // JSON survives the round-trip through the coerce_* helpers.

    proptest! {
        /// Any i64 encoded as a JSON number must round-trip through
        /// `coerce_to_i64` without loss.
        #[test]
        fn prop_coerce_to_i64_from_number(n in any::<i64>()) {
            let val = Value::Number(n.into());
            prop_assert_eq!(coerce_to_i64(&val, "col", 1).unwrap(), n);
        }

        /// Any i64 formatted as a decimal string must parse correctly
        /// via `coerce_to_i64` (the string-coercion path).
        #[test]
        fn prop_coerce_to_i64_from_string(n in any::<i64>()) {
            let val = Value::String(n.to_string());
            prop_assert_eq!(coerce_to_i64(&val, "col", 1).unwrap(), n);
        }

        /// Any bool survives the round-trip through `coerce_to_bool`.
        #[test]
        fn prop_coerce_to_bool_from_bool(b in any::<bool>()) {
            let val = Value::Bool(b);
            prop_assert_eq!(coerce_to_bool(&val, "col", 1).unwrap(), b);
        }

        /// Any string survives the direct round-trip through
        /// `coerce_to_string`.
        #[test]
        fn prop_coerce_to_string_from_string(s in ".*") {
            let val = Value::String(s.clone());
            prop_assert_eq!(coerce_to_string(&val, "col", 1).unwrap(), s);
        }

        /// `coerce_property_value` with target type `INT` must produce
        /// `PropertyValue::Int(n)` for any JSON number representable as i64.
        #[test]
        fn prop_coerce_property_value_int_roundtrip(n in any::<i64>()) {
            let val = Value::Number(n.into());
            let pv = coerce_property_value(&val, &BqlType::Int, "col", 1).unwrap();
            prop_assert_eq!(pv, PropertyValue::Int(n));
        }

        /// `coerce_property_value` with target type `BOOL` must produce
        /// `PropertyValue::Bool(b)` for any JSON boolean.
        #[test]
        fn prop_coerce_property_value_bool_roundtrip(b in any::<bool>()) {
            let val = Value::Bool(b);
            let pv = coerce_property_value(&val, &BqlType::Bool, "col", 1).unwrap();
            prop_assert_eq!(pv, PropertyValue::Bool(b));
        }

        /// `coerce_property_value` with target type `STRING` must produce
        /// `PropertyValue::String(s)` for any JSON string.
        #[test]
        fn prop_coerce_property_value_string_roundtrip(s in ".*") {
            let val = Value::String(s.clone());
            let pv = coerce_property_value(&val, &BqlType::String, "col", 1).unwrap();
            prop_assert_eq!(pv, PropertyValue::String(s));
        }
    }
}
