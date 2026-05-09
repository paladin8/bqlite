//! Parquet event reader — converts a Parquet file into a stream of
//! [`Event`] records ready for the ingest partitioner.
//!
//! # Responsibilities
//!
//! 1. Open a `.parquet` file and read it into Arrow [`RecordBatch`]es
//!    via the `parquet` crate's Arrow integration.
//! 2. Apply the column mapping from the planner's
//!    `InsertFromDescriptor` to resolve each Parquet field name to its
//!    target column in the [`TableSchema`].
//! 3. Apply width-consolidation: any integer width collapses to `i64`
//!    (BQL `Int`), any float width collapses to `f64` (BQL `Float`),
//!    and timestamp precision is normalized to nanoseconds regardless of
//!    the stored unit.
//! 4. For each row, coerce Arrow values to [`PropertyValue`]s, populate
//!    the entity id / timestamp / event-type role columns, and emit an
//!    [`Event`].
//! 5. Report errors with 1-based row numbers so the caller can surface
//!    a useful diagnostic.
//!
//! # Width-consolidation rules (type-system.md §2.1)
//!
//! | Parquet / Arrow physical type           | BQL type  | Notes |
//! |-----------------------------------------|-----------|-------|
//! | `Boolean`                               | `Bool`    | |
//! | `Int8 / Int16 / Int32 / Int64`          | `Int`     | direct cast to i64 |
//! | `UInt8 / UInt16 / UInt32 / UInt64`      | `Int`     | checked widening; UInt64 errors on overflow |
//! | `Float32`                               | `Float`   | widened to f64 |
//! | `Float64`                               | `Float`   | direct |
//! | `Utf8 / LargeUtf8 / Utf8View`           | `String`  | |
//! | `Timestamp(Nanosecond, _)`              | `Timestamp` | nanoseconds pass through |
//! | `Timestamp(Microsecond, _)`             | `Timestamp` | × 1 000 |
//! | `Timestamp(Millisecond, _)`             | `Timestamp` | × 1 000 000 |
//! | `Timestamp(Second, _)`                  | `Timestamp` | × 1 000 000 000 |
//! | `Date32` (days since epoch)             | `Timestamp` | × 86 400 000 000 000 |
//! | `Date64` (milliseconds since epoch)     | `Timestamp` | × 1 000 000 |
//! | `Null`                                  | `Null`    | always null |
//!
//! # Wave 4 scope (TASK-449)
//!
//! This is the Wave 4 Parquet reader. JSONL ingest is TASK-410.

use std::fs::File;
use std::io;
use std::path::Path;

use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::{BqlType, PropertyValue};
use bqlite_core::schema::TableSchema;
use bqlite_core::time::Timestamp;

// ─────────────────────────────────────────────────────────────────────────────
// Internal types
// ─────────────────────────────────────────────────────────────────────────────

/// Resolved mapping from a Parquet Arrow column index to a target BQL column.
#[derive(Debug)]
struct ResolvedColumn {
    /// Index into the Arrow [`RecordBatch`] columns slice.
    /// `None` means the column is absent from the Parquet file (only
    /// valid when `nullable == true`; the reader emits `Null` for it).
    arrow_col_idx: Option<usize>,
    /// Name of the target column in the table schema.
    target_name: String,
    /// BQL type for coercion.
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
// ParquetEventReader
// ─────────────────────────────────────────────────────────────────────────────

/// A pull-based Parquet event reader.
///
/// Opened with a file path (or pre-loaded batches in tests), the target
/// table schema, and the resolved column mapping.  Each call to
/// [`ParquetEventReader::next_event`] advances one row through the Arrow
/// [`RecordBatch`] stream and returns one [`Event`]; returns `None` at EOF.
///
/// All batches are read eagerly on construction.  Lazy / streaming reads
/// are a Wave 5 concern.
#[derive(Debug)]
pub struct ParquetEventReader {
    batches: Vec<RecordBatch>,
    batch_idx: usize,
    row_idx: usize,
    columns: Vec<ResolvedColumn>,
    /// Cached count of `Property`-role entries, used to pre-size the
    /// per-event property `Vec` without re-scanning on every row.
    property_count: usize,
    /// 1-based row number for error messages (counts all rows processed).
    row_number: u64,
}

impl ParquetEventReader {
    /// Open a Parquet file on disk and resolve the column mapping.
    ///
    /// # Arguments
    ///
    /// * `path` — filesystem path to the `.parquet` file.
    /// * `table` — target table schema.
    /// * `column_map` — `(source_field, target_column)` pairs from the
    ///   planner.  Empty means "match Parquet field names to table column
    ///   names directly."
    pub fn open(path: &Path, table: &TableSchema, column_map: &[(String, String)]) -> Result<Self> {
        let file = File::open(path).map_err(|e| {
            BqliteError::Io(io::Error::new(
                e.kind(),
                format!("Parquet reader: failed to open '{}': {e}", path.display()),
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            BqliteError::Execution(format!(
                "Parquet reader: failed to parse metadata in '{}': {e}",
                path.display()
            ))
        })?;

        let arrow_schema = builder.schema().clone();

        let reader = builder.build().map_err(|e| {
            BqliteError::Execution(format!(
                "Parquet reader: failed to build record-batch reader for '{}': {e}",
                path.display()
            ))
        })?;

        // Collect all batches eagerly.
        let mut batches: Vec<RecordBatch> = Vec::new();
        for batch_result in reader {
            let batch = batch_result.map_err(|e| {
                BqliteError::Execution(format!(
                    "Parquet reader: I/O error reading batch from '{}': {e}",
                    path.display()
                ))
            })?;
            batches.push(batch);
        }

        Self::from_batches(batches, &arrow_schema, table, column_map)
    }

    /// Construct a reader from pre-loaded Arrow batches.
    ///
    /// Used by unit tests to avoid disk I/O.  Not part of the public engine
    /// API — callers outside this crate should use [`Self::open`] instead.
    pub(crate) fn from_batches(
        batches: Vec<RecordBatch>,
        arrow_schema: &Schema,
        table: &TableSchema,
        column_map: &[(String, String)],
    ) -> Result<Self> {
        let columns = resolve_columns(arrow_schema, table, column_map)?;
        let property_count = columns
            .iter()
            .filter(|c| c.role == ColumnRole::Property)
            .count();
        Ok(Self {
            batches,
            batch_idx: 0,
            row_idx: 0,
            columns,
            property_count,
            row_number: 0,
        })
    }

    /// Read the next row and convert it into an [`Event`].
    ///
    /// Returns `Ok(None)` at EOF.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            let Some(batch) = self.batches.get(self.batch_idx) else {
                return Ok(None);
            };
            if self.row_idx >= batch.num_rows() {
                self.batch_idx += 1;
                self.row_idx = 0;
                continue;
            }

            self.row_number += 1;
            let event = self.extract_row(batch, self.row_idx)?;
            self.row_idx += 1;
            return Ok(Some(event));
        }
    }

    /// Extract a single row from a batch into an [`Event`].
    fn extract_row(&self, batch: &RecordBatch, row: usize) -> Result<Event> {
        let mut entity: Option<EntityId> = None;
        let mut timestamp: Option<Timestamp> = None;
        let mut event_type: Option<String> = None;
        let mut properties: Vec<(String, PropertyValue)> = Vec::with_capacity(self.property_count);

        for col in &self.columns {
            // Column absent from the Parquet file (arrow_col_idx is None).
            let Some(arrow_idx) = col.arrow_col_idx else {
                if col.role == ColumnRole::Property {
                    if col.nullable {
                        properties.push((col.target_name.clone(), PropertyValue::Null));
                    } else {
                        // NOT NULL property column is absent from the file — error.
                        return Err(BqliteError::Execution(format!(
                            "Parquet reader row {}: NOT NULL column '{}' is absent \
                             from the Parquet file",
                            self.row_number, col.target_name
                        )));
                    }
                }
                // Role columns (entity/ts/type) with None are caught at construction
                // time by the role-column validation; reaching here would indicate an
                // internal inconsistency.
                continue;
            };

            let array = batch.column(arrow_idx);

            // Handle null values at the Arrow level.
            if array.is_null(row) {
                if col.nullable {
                    if col.role == ColumnRole::Property {
                        properties.push((col.target_name.clone(), PropertyValue::Null));
                    }
                    // Role columns (entity/ts/type) are NOT NULL by schema
                    // invariant; if a null appears anyway, the guards below
                    // will surface a "column not populated" error.
                    continue;
                } else {
                    return Err(BqliteError::Execution(format!(
                        "Parquet reader row {}: NOT NULL column '{}' contains a null value",
                        self.row_number, col.target_name
                    )));
                }
            }

            match col.role {
                ColumnRole::EntityKey => {
                    entity = Some(extract_entity_id(
                        array.as_ref(),
                        row,
                        &col.target_type,
                        &col.target_name,
                        self.row_number,
                    )?);
                }
                ColumnRole::Timestamp => {
                    timestamp = Some(extract_timestamp(
                        array.as_ref(),
                        row,
                        &col.target_name,
                        self.row_number,
                    )?);
                }
                ColumnRole::EventType => {
                    event_type = Some(extract_utf8(
                        array.as_ref(),
                        row,
                        &col.target_name,
                        self.row_number,
                    )?);
                }
                ColumnRole::Property => {
                    let value = extract_property_value(
                        array.as_ref(),
                        row,
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
                "Parquet reader row {}: entity-key column was not populated",
                self.row_number
            ))
        })?;
        let timestamp = timestamp.ok_or_else(|| {
            BqliteError::Execution(format!(
                "Parquet reader row {}: timestamp column was not populated",
                self.row_number
            ))
        })?;
        let event_type = event_type.ok_or_else(|| {
            BqliteError::Execution(format!(
                "Parquet reader row {}: event-type column was not populated",
                self.row_number
            ))
        })?;

        Ok(Event::with_properties(
            entity, timestamp, event_type, properties,
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Column resolution
// ─────────────────────────────────────────────────────────────────────────────

/// Build the resolved column list from the Arrow schema, BQL table schema,
/// and optional explicit column map.
fn resolve_columns(
    arrow_schema: &Schema,
    table: &TableSchema,
    column_map: &[(String, String)],
) -> Result<Vec<ResolvedColumn>> {
    let mut columns: Vec<ResolvedColumn> = Vec::new();

    if column_map.is_empty() {
        // Passthrough mode: match Parquet field names to BQL column names.
        for col_def in table.columns() {
            let arrow_col_idx = arrow_schema
                .column_with_name(&col_def.name)
                .map(|(idx, _)| idx);

            if arrow_col_idx.is_none() && !col_def.nullable {
                return Err(BqliteError::Execution(format!(
                    "Parquet reader: NOT NULL column '{}' is not present in the Parquet file",
                    col_def.name
                )));
            }

            columns.push(ResolvedColumn {
                arrow_col_idx,
                target_name: col_def.name.clone(),
                target_type: col_def.bql_type.clone(),
                nullable: col_def.nullable,
                role: role_for(table, &col_def.name),
            });
        }
    } else {
        // Explicit map mode: add explicitly-mapped pairs first, then pick
        // up passthrough columns for the remainder.
        let mapped_targets: std::collections::HashSet<&str> =
            column_map.iter().map(|(_, t)| t.as_str()).collect();

        for (src, tgt) in column_map {
            let (_, col_def) = table.column(tgt).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "Parquet reader: map target '{tgt}' is not a column on table '{}' \
                     (this should have been caught by the planner)",
                    table.name()
                ))
            })?;
            let arrow_idx = arrow_schema
                .column_with_name(src)
                .map(|(idx, _)| idx)
                .ok_or_else(|| {
                    BqliteError::Execution(format!(
                        "Parquet reader: source field '{src}' is not present in the Parquet file"
                    ))
                })?;
            let role = role_for(table, tgt);
            columns.push(ResolvedColumn {
                arrow_col_idx: Some(arrow_idx),
                target_name: tgt.clone(),
                target_type: col_def.bql_type.clone(),
                nullable: col_def.nullable,
                role,
            });
        }

        // Passthrough for unmapped BQL columns present in the Parquet file.
        for col_def in table.columns() {
            if mapped_targets.contains(col_def.name.as_str()) {
                continue;
            }
            let arrow_col_idx = arrow_schema
                .column_with_name(&col_def.name)
                .map(|(idx, _)| idx);

            // Non-role columns absent from both the map and the file are
            // simply skipped when nullable; non-nullable absent columns that
            // are not covered by the explicit map would produce a missing-
            // entity-key / timestamp / event-type error at row extraction
            // time, which gives the user a clear message.
            columns.push(ResolvedColumn {
                arrow_col_idx,
                target_name: col_def.name.clone(),
                target_type: col_def.bql_type.clone(),
                nullable: col_def.nullable,
                role: role_for(table, &col_def.name),
            });
        }
    }

    // Validate that all three role columns are resolved (index is Some).
    let entity_col = columns.iter().find(|c| c.role == ColumnRole::EntityKey);
    let ts_col = columns.iter().find(|c| c.role == ColumnRole::Timestamp);
    let et_col = columns.iter().find(|c| c.role == ColumnRole::EventType);

    if entity_col.is_none_or(|c| c.arrow_col_idx.is_none()) {
        return Err(BqliteError::Execution(format!(
            "Parquet reader: entity-key column '{}' is not mapped from any Parquet field",
            table.columns()[table.entity_key_index()].name
        )));
    }
    if ts_col.is_none_or(|c| c.arrow_col_idx.is_none()) {
        return Err(BqliteError::Execution(format!(
            "Parquet reader: event-time column '{}' is not mapped from any Parquet field",
            table.columns()[table.timestamp_index()].name
        )));
    }
    if et_col.is_none_or(|c| c.arrow_col_idx.is_none()) {
        return Err(BqliteError::Execution(format!(
            "Parquet reader: event-type column '{}' is not mapped from any Parquet field",
            table.columns()[table.event_type_index()].name
        )));
    }

    Ok(columns)
}

/// Map a BQL column name to its schema role.
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
// Value extraction helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Extract an [`EntityId`] from an Arrow array at the given row.
///
/// Accepts any integer or UTF-8 string Arrow type and coerces to the
/// target BQL entity-key type (`String` or `Int`).
fn extract_entity_id(
    array: &dyn Array,
    row: usize,
    target_type: &BqlType,
    col_name: &str,
    row_number: u64,
) -> Result<EntityId> {
    match target_type {
        BqlType::String => {
            let s = extract_utf8(array, row, col_name, row_number)?;
            Ok(EntityId::String(s))
        }
        BqlType::Int => {
            let n = extract_i64(array, row, col_name, row_number)?;
            Ok(EntityId::Int(n))
        }
        other => Err(BqliteError::Execution(format!(
            "Parquet reader row {row_number}: entity-key column '{col_name}' has \
             unsupported BQL type {other:?}; only STRING and INT are valid"
        ))),
    }
}

/// Extract a nanosecond [`Timestamp`] from an Arrow array at the given row.
///
/// Accepts any temporal Arrow type (Timestamp with any unit, Date32, Date64)
/// or any integer type (interpreted as epoch nanoseconds).
fn extract_timestamp(
    array: &dyn Array,
    row: usize,
    col_name: &str,
    row_number: u64,
) -> Result<Timestamp> {
    let nanos = extract_nanos(array, row, col_name, row_number)?;
    Ok(Timestamp::from_nanos(nanos))
}

/// Convert an Arrow temporal or integer column value to epoch nanoseconds.
fn extract_nanos(array: &dyn Array, row: usize, col_name: &str, row_number: u64) -> Result<i64> {
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row)),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(row)
            .saturating_mul(1_000)),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(row)
            .saturating_mul(1_000_000)),
        DataType::Timestamp(TimeUnit::Second, _) => Ok(array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(row)
            .saturating_mul(1_000_000_000)),
        DataType::Date32 => Ok((array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row) as i64)
            .saturating_mul(86_400_000_000_000)),
        DataType::Date64 => Ok(array
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap()
            .value(row)
            .saturating_mul(1_000_000)),
        // Integer columns → interpreted directly as epoch nanoseconds.
        _ => extract_i64(array, row, col_name, row_number),
    }
}

/// Extract a UTF-8 string value from an Arrow array.
///
/// Accepts `Utf8`, `LargeUtf8`, and `Utf8View` arrays. Also accepts any
/// integer or float array and converts to a decimal/float string, matching
/// the JSONL reader's loose-string-coercion semantics.
fn extract_utf8(array: &dyn Array, row: usize, col_name: &str, row_number: u64) -> Result<String> {
    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_owned()),
        DataType::LargeUtf8 => Ok(array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(row)
            .to_owned()),
        DataType::Utf8View => Ok(array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(row)
            .to_owned()),
        // Integer / float → decimal / float string.
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)
            .to_string()),
        DataType::Boolean => {
            let b = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row);
            Ok(if b { "true" } else { "false" }.to_owned())
        }
        other => Err(BqliteError::Execution(format!(
            "Parquet reader row {row_number}: column '{col_name}': \
             cannot coerce Arrow type {other:?} to STRING"
        ))),
    }
}

/// Extract an `i64` value from an Arrow numeric array (width-consolidation).
///
/// Accepts all signed and unsigned integer widths. UInt64 is checked
/// for overflow. Float types are truncated toward zero (matching
/// `CAST(float AS INT)` semantics from type-system.md §4.2).
fn extract_i64(array: &dyn Array, row: usize, col_name: &str, row_number: u64) -> Result<i64> {
    match array.data_type() {
        DataType::Int8 => Ok(array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .value(row) as i64),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(row) as i64),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row) as i64),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)),
        DataType::UInt8 => Ok(array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .value(row) as i64),
        DataType::UInt16 => Ok(array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .value(row) as i64),
        DataType::UInt32 => Ok(array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(row) as i64),
        DataType::UInt64 => {
            let val = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            i64::try_from(val).map_err(|_| {
                BqliteError::Execution(format!(
                    "Parquet reader row {row_number}: column '{col_name}': \
                     UInt64 value {val} overflows i64"
                ))
            })
        }
        DataType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row) as i64),
        DataType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row) as i64),
        other => Err(BqliteError::Execution(format!(
            "Parquet reader row {row_number}: column '{col_name}': \
             cannot coerce Arrow type {other:?} to INT"
        ))),
    }
}

/// Extract a `f64` value from an Arrow numeric array (width-consolidation).
fn extract_f64(array: &dyn Array, row: usize, col_name: &str, row_number: u64) -> Result<f64> {
    match array.data_type() {
        DataType::Float32 => Ok(array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .value(row) as f64),
        DataType::Float64 => Ok(array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(row)),
        // Integer widths → widened to f64.
        _ => extract_i64(array, row, col_name, row_number).map(|n| n as f64),
    }
}

/// Extract a `bool` value from an Arrow boolean array.
fn extract_bool(array: &dyn Array, row: usize, col_name: &str, row_number: u64) -> Result<bool> {
    match array.data_type() {
        DataType::Boolean => Ok(array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(row)),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => {
            let n = extract_i64(array, row, col_name, row_number)?;
            match n {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(BqliteError::Execution(format!(
                    "Parquet reader row {row_number}: column '{col_name}': \
                     cannot coerce integer {n} to BOOL — only 0 and 1 are accepted"
                ))),
            }
        }
        other => Err(BqliteError::Execution(format!(
            "Parquet reader row {row_number}: column '{col_name}': \
             cannot coerce Arrow type {other:?} to BOOL"
        ))),
    }
}

/// Extract a [`PropertyValue`] from an Arrow array for the given target BQL type.
///
/// Applies width-consolidation (integer widths → `i64`, float widths → `f64`)
/// and common cross-type coercions (int → float, string numeric/bool parsing).
fn extract_property_value(
    array: &dyn Array,
    row: usize,
    target_type: &BqlType,
    col_name: &str,
    row_number: u64,
) -> Result<PropertyValue> {
    match target_type {
        BqlType::Bool => extract_bool(array, row, col_name, row_number).map(PropertyValue::Bool),
        BqlType::Int => extract_i64(array, row, col_name, row_number).map(PropertyValue::Int),
        BqlType::Float => extract_f64(array, row, col_name, row_number).map(PropertyValue::Float),
        BqlType::String => {
            extract_utf8(array, row, col_name, row_number).map(PropertyValue::String)
        }
        BqlType::Timestamp => {
            extract_nanos(array, row, col_name, row_number).map(PropertyValue::Timestamp)
        }
        BqlType::List(_) | BqlType::Map(_) => {
            // Nested collection types are not supported in the Parquet ingest
            // path.  Parquet's nested representation (LIST / MAP logical
            // types) would require substantially more complex decoding, and
            // bqlite's current schema design does not require them for the
            // event property bag.  Arrow's LIST / MAP arrays are not handled
            // by any existing Arrow downcast path in this module.
            Err(BqliteError::Execution(format!(
                "Parquet reader row {row_number}: column '{col_name}': \
                 LIST and MAP property columns are not supported in the \
                 Parquet ingest path"
            )))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray, TimestampNanosecondArray, UInt32Array,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use bqlite_core::event::EntityId;
    use bqlite_core::property::{BqlType, PropertyValue};
    use bqlite_core::schema::{ColumnDef, TableSchema};

    use super::*;

    // ── Schema helpers ─────────────────────────────────────────────────────────

    fn events_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Int),
                ColumnDef::nullable("score", BqlType::Float),
                ColumnDef::nullable("country", BqlType::String),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .expect("schema")
    }

    /// Build an Arrow schema matching the events table (passthrough mode).
    fn events_arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("country", DataType::Utf8, true),
        ])
    }

    fn collect_events(reader: &mut ParquetEventReader) -> Vec<Event> {
        let mut events = Vec::new();
        while let Some(ev) = reader.next_event().unwrap() {
            events.push(ev);
        }
        events
    }

    // ── Happy path: passthrough mode ───────────────────────────────────────────

    #[test]
    fn passthrough_mode_reads_all_rows() {
        let schema = events_schema();
        let arrow_schema = events_arrow_schema();

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                    1_700_000_000_100_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click", "view"])),
                Arc::new(Int64Array::from(vec![Some(42), None])),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(StringArray::from(vec![Some("US"), None])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[0].timestamp_nanos(), 1_700_000_000_000_000_000);
        assert_eq!(events[0].event_type, "click");
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(42)));
        assert_eq!(events[0].get("score"), Some(&PropertyValue::Float(1.5)));
        assert_eq!(
            events[0].get("country"),
            Some(&PropertyValue::String("US".into()))
        );

        assert_eq!(events[1].entity, EntityId::from("bob"));
        assert_eq!(events[1].get("amount"), Some(&PropertyValue::Null));
        assert_eq!(events[1].get("score"), Some(&PropertyValue::Null));
        assert_eq!(events[1].get("country"), Some(&PropertyValue::Null));
    }

    // ── Width-consolidation: Int32 → i64 ──────────────────────────────────────

    #[test]
    fn int32_column_width_consolidates_to_i64() {
        let schema = events_schema();
        let arrow_schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, true), // Int32, not Int64
            Field::new("score", DataType::Float64, true),
            Field::new("country", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(Int32Array::from(vec![Some(99)])),
                Arc::new(Float64Array::from(vec![None::<f64>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(99)));
    }

    // ── Width-consolidation: Float32 → f64 ───────────────────────────────────

    #[test]
    fn float32_column_width_consolidates_to_f64() {
        let schema = events_schema();
        let arrow_schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new("score", DataType::Float32, true), // Float32, not Float64
            Field::new("country", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Float32Array::from(vec![Some(1.25_f32)])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        // Float32 1.25 widened to f64: 1.25 is exactly representable, so
        // the result must match exactly after widening.
        if let Some(PropertyValue::Float(v)) = events[0].get("score") {
            assert!((v - 1.25_f32 as f64).abs() < 1e-10, "score = {v}");
        } else {
            panic!("expected Float, got {:?}", events[0].get("score"));
        }
    }

    // ── Width-consolidation: UInt32 → i64 ────────────────────────────────────

    #[test]
    fn uint32_column_width_consolidates_to_i64() {
        let schema = events_schema();
        let arrow_schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::UInt32, true),
            Field::new("score", DataType::Float64, true),
            Field::new("country", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(UInt32Array::from(vec![Some(u32::MAX)])),
                Arc::new(Float64Array::from(vec![None::<f64>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].get("amount"),
            Some(&PropertyValue::Int(u32::MAX as i64))
        );
    }

    // ── Timestamp unit normalization ───────────────────────────────────────────

    #[test]
    fn timestamp_microseconds_normalized_to_nanos() {
        let schema = events_schema();
        let arrow_schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("country", DataType::Utf8, true),
        ]);

        let micros = 1_700_000_000_000_000_i64; // 1 700 000 seconds in µs
        let expected_nanos = micros * 1_000;

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampMicrosecondArray::from(vec![micros])),
                Arc::new(StringArray::from(vec!["click"])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Float64Array::from(vec![None::<f64>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].timestamp_nanos(), expected_nanos);
    }

    // ── Explicit column mapping ────────────────────────────────────────────────

    #[test]
    fn explicit_column_map_remaps_fields() {
        let schema = events_schema();
        // Arrow schema with different field names than the BQL schema.
        let arrow_schema = Schema::new(vec![
            Field::new("uid", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("evt", DataType::Utf8, false),
            Field::new("val", DataType::Int64, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
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

        let column_map = vec![
            ("uid".to_owned(), "user_id".to_owned()),
            ("time".to_owned(), "ts".to_owned()),
            ("evt".to_owned(), "event_type".to_owned()),
            ("val".to_owned(), "amount".to_owned()),
        ];

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &column_map)
                .unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(42)));
    }

    // ── Multiple batches ───────────────────────────────────────────────────────

    #[test]
    fn multiple_batches_are_concatenated() {
        let schema = events_schema();
        let arrow_schema = events_arrow_schema();

        let make_batch = |entity: &str, ts: i64, event: &str| {
            RecordBatch::try_new(
                Arc::new(arrow_schema.clone()),
                vec![
                    Arc::new(StringArray::from(vec![entity])),
                    Arc::new(TimestampNanosecondArray::from(vec![ts])),
                    Arc::new(StringArray::from(vec![event])),
                    Arc::new(Int64Array::from(vec![None::<i64>])),
                    Arc::new(Float64Array::from(vec![None::<f64>])),
                    Arc::new(StringArray::from(vec![None::<&str>])),
                ],
            )
            .unwrap()
        };

        let batches = vec![
            make_batch("alice", 1_700_000_000_000_000_000, "click"),
            make_batch("bob", 1_700_000_000_100_000_000, "view"),
            make_batch("charlie", 1_700_000_000_200_000_000, "purchase"),
        ];

        let mut reader =
            ParquetEventReader::from_batches(batches, &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[1].entity, EntityId::from("bob"));
        assert_eq!(events[2].entity, EntityId::from("charlie"));
    }

    // ── Empty file (zero batches) → no events ─────────────────────────────────

    #[test]
    fn empty_batches_returns_none() {
        let schema = events_schema();
        let arrow_schema = events_arrow_schema();
        let mut reader =
            ParquetEventReader::from_batches(vec![], &arrow_schema, &schema, &[]).unwrap();
        assert!(reader.next_event().unwrap().is_none());
    }

    // ── Boolean column ─────────────────────────────────────────────────────────

    #[test]
    fn boolean_column_reads_correctly() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("active", BqlType::Bool),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .unwrap();
        let arrow_schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                    1_700_000_000_100_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click", "view"])),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &[]).unwrap();
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].get("active"), Some(&PropertyValue::Bool(true)));
        assert_eq!(events[1].get("active"), Some(&PropertyValue::Null));
    }

    // ── Error: NOT NULL column contains null ──────────────────────────────────

    #[test]
    fn not_null_column_containing_null_errors_with_row_number() {
        let bql_schema = events_schema();

        // Arrow schema where event_type is *nullable* at the Arrow level — this
        // lets the batch be constructed.  Our reader must still error because the
        // BQL schema marks event_type as NOT NULL.
        let arrow_schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, true), // nullable at Arrow level
            Field::new("amount", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("country", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                    1_700_000_000_100_000_000_i64,
                ])),
                // Row 2 has a null event_type.
                Arc::new(StringArray::from(vec![Some("click"), None])),
                Arc::new(Int64Array::from(vec![None::<i64>, None::<i64>])),
                Arc::new(Float64Array::from(vec![None::<f64>, None::<f64>])),
                Arc::new(StringArray::from(vec![None::<&str>, None::<&str>])),
            ],
        )
        .unwrap();

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &bql_schema, &[]).unwrap();
        reader.next_event().unwrap(); // row 1 succeeds
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("row 2"), "got: {msg}");
                assert!(
                    msg.contains("NOT NULL") || msg.contains("null"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: entity-key column absent from Parquet (passthrough mode) ────────

    #[test]
    fn missing_not_null_column_in_passthrough_mode_errors_at_construction() {
        let schema = events_schema();
        // Arrow schema missing `user_id`.
        let arrow_schema = Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
        ]);

        let err =
            ParquetEventReader::from_batches(vec![], &arrow_schema, &schema, &[]).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("user_id") || msg.contains("NOT NULL"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: NOT NULL property column absent in explicit map mode ────────────
    //
    // In explicit map mode, unmapped BQL columns are added as passthrough entries
    // with arrow_col_idx = None when absent from the Parquet file.  If such a
    // column is NOT NULL, the reader must error (not silently emit Null).

    #[test]
    fn not_null_property_column_absent_in_explicit_map_mode_errors_at_row_read() {
        // BQL schema has `amount INT NOT NULL` (via `ColumnDef::required`).
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::required("amount", BqlType::Int), // NOT NULL
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .expect("schema");

        // Arrow file has only the three role columns — `amount` is absent.
        let arrow_schema = Schema::new(vec![
            Field::new("uid", DataType::Utf8, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("evt", DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema.clone()),
            vec![
                Arc::new(StringArray::from(vec!["alice"])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    1_700_000_000_000_000_000_i64,
                ])),
                Arc::new(StringArray::from(vec!["click"])),
            ],
        )
        .unwrap();

        let column_map = vec![
            ("uid".to_owned(), "user_id".to_owned()),
            ("time".to_owned(), "ts".to_owned()),
            ("evt".to_owned(), "event_type".to_owned()),
            // "amount" is NOT in the explicit map and NOT in the Parquet file.
        ];

        let mut reader =
            ParquetEventReader::from_batches(vec![batch], &arrow_schema, &schema, &column_map)
                .unwrap();

        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("amount") || msg.contains("NOT NULL"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }
}
