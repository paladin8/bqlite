//! Streaming CSV reader — converts a CSV file into a stream of
//! [`Event`] records ready for the ingest partitioner.
//!
//! # Responsibilities
//!
//! 1. Open a CSV file and read its header to discover source column
//!    names.
//! 2. Apply the column mapping from the planner's
//!    `InsertFromDescriptor` to resolve each source column to its
//!    target column in the [`TableSchema`].
//! 3. For each CSV row, parse and coerce field values into
//!    [`PropertyValue`]s matching the target column types, populate
//!    the entity id / timestamp / event-type role columns from the
//!    mapped fields, and emit an [`Event`].
//! 4. Report errors with row numbers so the caller can surface a
//!    useful diagnostic.
//!
//! # Wave 2 scope (TASK-233)
//!
//! - Delimiter defaults to `,`; overridable via the `delimiter`
//!   option in `WITH (...)`.
//! - Header defaults to `true`; overridable via the `header` option.
//! - Only the `Csv` ingest format is supported. `JsonL` and `Parquet`
//!   land in Wave 4 (TASK-410).
//! - The reader is pull-based: `CsvEventReader::next_event()` returns
//!   one [`Event`] at a time for the caller to push into the
//!   partitioner. No batch accumulation inside this module.

use std::io::{self, BufRead};
use std::path::Path;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::event::{EntityId, Event};
use bqlite_core::property::{BqlType, PropertyValue};
use bqlite_core::schema::TableSchema;
use bqlite_core::time::Timestamp;

/// Resolved mapping from a source CSV column index to its target
/// column in the [`TableSchema`], plus the target column's metadata
/// needed for parsing.
#[derive(Debug)]
struct ResolvedColumn {
    /// 0-based index into the CSV row's field list.
    source_index: usize,
    /// Name of the target column in the table schema.
    target_name: String,
    /// Type of the target column.
    target_type: BqlType,
    /// Whether the target column is nullable.
    nullable: bool,
    /// Which role-column (if any) this mapping satisfies.
    role: ColumnRole,
}

/// What schema role a mapped column fills, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnRole {
    EntityKey,
    Timestamp,
    EventType,
    Property,
}

/// A pull-based CSV event reader.
///
/// Constructed with a file path, the target table schema, the
/// resolved column mapping, and any format options (`delimiter`,
/// `header`). Each call to [`CsvEventReader::next_event`] returns
/// the next row as an [`Event`], or `None` at EOF.
#[derive(Debug)]
pub struct CsvEventReader<R: BufRead> {
    /// The underlying csv crate reader.
    csv_reader: csv::Reader<R>,
    /// Reusable string record to avoid per-row allocation.
    record: csv::StringRecord,
    /// Resolved column mappings, one per target column we populate.
    columns: Vec<ResolvedColumn>,
    /// 1-based row number for error messages (header is row 1 when
    /// present, data rows start at 2).
    row_number: u64,
    /// Cached count of `ColumnRole::Property` entries in `columns`,
    /// used to pre-allocate the per-event property `Vec` without
    /// re-scanning the mapping on every row.
    property_count: usize,
}

/// Configuration extracted from the planner's `WITH (...)` options.
#[derive(Debug)]
pub struct CsvReaderOptions {
    /// Field delimiter byte. Default: `,`.
    pub delimiter: u8,
    /// Whether the first row is a header. Default: `true`.
    pub has_header: bool,
}

impl Default for CsvReaderOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_header: true,
        }
    }
}

impl CsvReaderOptions {
    /// Build options from the planner's resolved `(key, value)` list.
    ///
    /// Recognized keys: `delimiter` (single-char string), `header`
    /// (bool-like string: `"true"` / `"false"`). Unknown keys are
    /// silently ignored — the planner already validated them.
    pub fn from_options(options: &[(String, PropertyValue)]) -> Result<Self> {
        let mut opts = Self::default();
        for (key, value) in options {
            match key.as_str() {
                "delimiter" => {
                    let s = match value {
                        PropertyValue::String(s) => s.as_str(),
                        other => {
                            return Err(BqliteError::Execution(format!(
                                "CSV reader: `delimiter` must be a string, got {other:?}"
                            )));
                        }
                    };
                    let bytes = s.as_bytes();
                    if bytes.len() != 1 {
                        return Err(BqliteError::Execution(format!(
                            "CSV reader: `delimiter` must be a single byte, got {s:?}"
                        )));
                    }
                    opts.delimiter = bytes[0];
                }
                "header" => {
                    opts.has_header = match value {
                        PropertyValue::Bool(b) => *b,
                        PropertyValue::String(s) => match s.to_ascii_lowercase().as_str() {
                            "true" | "1" | "yes" => true,
                            "false" | "0" | "no" => false,
                            _ => {
                                return Err(BqliteError::Execution(format!(
                                    "CSV reader: `header` must be a boolean, got {s:?}"
                                )));
                            }
                        },
                        other => {
                            return Err(BqliteError::Execution(format!(
                                "CSV reader: `header` must be a boolean, got {other:?}"
                            )));
                        }
                    };
                }
                _ => {} // planner already validated; ignore extras
            }
        }
        Ok(opts)
    }
}

impl CsvEventReader<io::BufReader<std::fs::File>> {
    /// Open a CSV file on disk and resolve the column mapping.
    ///
    /// # Arguments
    ///
    /// * `path` — filesystem path to the CSV file.
    /// * `table` — the target table schema.
    /// * `column_map` — `(source, target)` pairs from the planner.
    ///   Empty means "match source header names to table columns by
    ///   name."
    /// * `options` — delimiter, header, etc.
    pub fn open(
        path: &Path,
        table: &TableSchema,
        column_map: &[(String, String)],
        options: &CsvReaderOptions,
    ) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(|e| {
            BqliteError::Io(io::Error::new(
                e.kind(),
                format!("CSV reader: failed to open '{}': {e}", path.display()),
            ))
        })?;
        let buf = io::BufReader::new(file);
        Self::from_reader(buf, table, column_map, options)
    }
}

impl<R: BufRead> CsvEventReader<R> {
    /// Construct a reader from an arbitrary `BufRead` source.
    ///
    /// This is the implementation used by both `open` (file) and
    /// tests (in-memory).
    pub fn from_reader(
        reader: R,
        table: &TableSchema,
        column_map: &[(String, String)],
        options: &CsvReaderOptions,
    ) -> Result<Self> {
        let csv_reader = csv::ReaderBuilder::new()
            .has_headers(options.has_header)
            .delimiter(options.delimiter)
            .from_reader(reader);

        let mut me = Self {
            csv_reader,
            record: csv::StringRecord::new(),
            columns: Vec::new(),
            row_number: if options.has_header { 1 } else { 0 },
            property_count: 0,
        };

        me.resolve_columns(table, column_map, options.has_header)?;
        me.property_count = me
            .columns
            .iter()
            .filter(|c| c.role == ColumnRole::Property)
            .count();
        Ok(me)
    }

    /// Resolve the source-to-target column mapping.
    fn resolve_columns(
        &mut self,
        table: &TableSchema,
        column_map: &[(String, String)],
        has_header: bool,
    ) -> Result<()> {
        // Build an index from source header name → source column index.
        let headers: Vec<String> = if has_header {
            let h = self.csv_reader.headers().map_err(|e| {
                BqliteError::Execution(format!("CSV reader: failed to read header: {e}"))
            })?;
            h.iter().map(|s| s.to_string()).collect()
        } else {
            // No header — source names are positional: "0", "1", "2", …
            // The map clause must use these positional names.
            Vec::new()
        };

        let source_index_of = |name: &str| -> Result<usize> {
            if has_header {
                headers.iter().position(|h| h == name).ok_or_else(|| {
                    BqliteError::Execution(format!(
                        "CSV reader: source column '{name}' not found in CSV header; \
                             available columns: {headers:?}"
                    ))
                })
            } else {
                name.parse::<usize>().map_err(|_| {
                    BqliteError::Execution(format!(
                        "CSV reader: headerless CSV requires numeric source column \
                         names in the map clause, got '{name}'"
                    ))
                })
            }
        };

        if column_map.is_empty() {
            // Passthrough mode: match CSV header names to table columns
            // by name. Every required role column must be present.
            if !has_header {
                return Err(BqliteError::Execution(
                    "CSV reader: headerless CSV requires a `map` clause to \
                     associate positional columns with table columns"
                        .into(),
                ));
            }
            for col_def in table.columns() {
                let src_idx = match headers.iter().position(|h| h == &col_def.name) {
                    Some(i) => i,
                    None if col_def.nullable => continue,
                    None => {
                        return Err(BqliteError::Execution(format!(
                            "CSV reader: required column '{}' not found in CSV header; \
                             available columns: {headers:?}",
                            col_def.name
                        )));
                    }
                };
                let role = role_for(table, &col_def.name);
                self.columns.push(ResolvedColumn {
                    source_index: src_idx,
                    target_name: col_def.name.clone(),
                    target_type: col_def.bql_type.clone(),
                    nullable: col_def.nullable,
                    role,
                });
            }
        } else {
            // Explicit map clause: resolve each (source, target) pair.
            for (src, tgt) in column_map {
                let src_idx = source_index_of(src)?;
                let (_, col_def) = table.column(tgt).ok_or_else(|| {
                    BqliteError::Execution(format!(
                        "CSV reader: map target '{tgt}' is not a column on table '{}' \
                         (this should have been caught by the planner)",
                        table.name()
                    ))
                })?;
                let role = role_for(table, tgt);
                self.columns.push(ResolvedColumn {
                    source_index: src_idx,
                    target_name: tgt.clone(),
                    target_type: col_def.bql_type.clone(),
                    nullable: col_def.nullable,
                    role,
                });
            }

            // Also pick up passthrough columns: CSV header columns whose
            // name exactly matches a table column not already in the map.
            if has_header {
                let mapped_targets: std::collections::HashSet<&str> =
                    column_map.iter().map(|(_, t)| t.as_str()).collect();
                for col_def in table.columns() {
                    if mapped_targets.contains(col_def.name.as_str()) {
                        continue;
                    }
                    if let Some(src_idx) = headers.iter().position(|h| h == &col_def.name) {
                        let role = role_for(table, &col_def.name);
                        self.columns.push(ResolvedColumn {
                            source_index: src_idx,
                            target_name: col_def.name.clone(),
                            target_type: col_def.bql_type.clone(),
                            nullable: col_def.nullable,
                            role,
                        });
                    }
                }
            }
        }

        // Validate that all three role columns are resolved.
        let has_entity = self.columns.iter().any(|c| c.role == ColumnRole::EntityKey);
        let has_ts = self.columns.iter().any(|c| c.role == ColumnRole::Timestamp);
        let has_et = self.columns.iter().any(|c| c.role == ColumnRole::EventType);
        if !has_entity {
            return Err(BqliteError::Execution(format!(
                "CSV reader: entity-key column '{}' is not mapped from any CSV column",
                table.columns()[table.entity_key_index()].name
            )));
        }
        if !has_ts {
            return Err(BqliteError::Execution(format!(
                "CSV reader: event-time column '{}' is not mapped from any CSV column",
                table.columns()[table.timestamp_index()].name
            )));
        }
        if !has_et {
            return Err(BqliteError::Execution(format!(
                "CSV reader: event-type column '{}' is not mapped from any CSV column",
                table.columns()[table.event_type_index()].name
            )));
        }

        Ok(())
    }

    /// Read the next CSV row and convert it into an [`Event`].
    ///
    /// Returns `Ok(None)` at EOF. Errors include parse failures and
    /// type-coercion mismatches, all annotated with the 1-based row
    /// number.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        let has_record = self.csv_reader.read_record(&mut self.record).map_err(|e| {
            BqliteError::Execution(format!(
                "CSV reader: error reading row {}: {e}",
                self.row_number + 1
            ))
        })?;
        if !has_record {
            return Ok(None);
        }
        self.row_number += 1;

        let mut entity: Option<EntityId> = None;
        let mut timestamp: Option<Timestamp> = None;
        let mut event_type: Option<String> = None;
        let mut properties: Vec<(String, PropertyValue)> = Vec::with_capacity(self.property_count);

        for col in &self.columns {
            let field = self.record.get(col.source_index).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "CSV reader row {}: column index {} out of bounds \
                     (row has {} fields)",
                    self.row_number,
                    col.source_index,
                    self.record.len()
                ))
            })?;

            // Empty fields on nullable columns become NULL.
            if field.is_empty() && col.nullable {
                if col.role == ColumnRole::Property {
                    properties.push((col.target_name.clone(), PropertyValue::Null));
                } else {
                    // Role columns are NOT NULL by schema design; an empty
                    // field for a role column is an error.
                    return Err(BqliteError::Execution(format!(
                        "CSV reader row {}: required column '{}' has empty value",
                        self.row_number, col.target_name
                    )));
                }
                continue;
            }

            if field.is_empty() && !col.nullable {
                return Err(BqliteError::Execution(format!(
                    "CSV reader row {}: NOT NULL column '{}' has empty value",
                    self.row_number, col.target_name
                )));
            }

            match col.role {
                ColumnRole::EntityKey => {
                    entity = Some(parse_entity_id(field, &col.target_type, self.row_number)?);
                }
                ColumnRole::Timestamp => {
                    timestamp = Some(parse_timestamp(field, self.row_number)?);
                }
                ColumnRole::EventType => {
                    event_type = Some(field.to_string());
                }
                ColumnRole::Property => {
                    let value = parse_property_value(
                        field,
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
                "CSV reader row {}: entity-key column was not populated",
                self.row_number
            ))
        })?;
        let timestamp = timestamp.ok_or_else(|| {
            BqliteError::Execution(format!(
                "CSV reader row {}: timestamp column was not populated",
                self.row_number
            ))
        })?;
        let event_type = event_type.ok_or_else(|| {
            BqliteError::Execution(format!(
                "CSV reader row {}: event-type column was not populated",
                self.row_number
            ))
        })?;

        Ok(Some(Event::with_properties(
            entity, timestamp, event_type, properties,
        )))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Determine the schema role for a target column name.
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

/// Parse a CSV field into an [`EntityId`].
fn parse_entity_id(field: &str, target_type: &BqlType, row: u64) -> Result<EntityId> {
    match target_type {
        BqlType::String => Ok(EntityId::String(field.to_string())),
        BqlType::Int => {
            let n: i64 = field.parse().map_err(|_| {
                BqliteError::Execution(format!(
                    "CSV reader row {row}: cannot parse entity key '{field}' as Int"
                ))
            })?;
            Ok(EntityId::Int(n))
        }
        other => Err(BqliteError::Execution(format!(
            "CSV reader row {row}: entity-key column has unsupported type {other:?}"
        ))),
    }
}

/// Parse a CSV field into a [`Timestamp`].
///
/// Accepts raw nanosecond integers. RFC 3339 / ISO 8601 date-time
/// parsing is deferred to a later wave.
fn parse_timestamp(field: &str, row: u64) -> Result<Timestamp> {
    let nanos: i64 = field.parse().map_err(|_| {
        BqliteError::Execution(format!(
            "CSV reader row {row}: cannot parse timestamp '{field}' as nanosecond integer"
        ))
    })?;
    Ok(Timestamp::from_nanos(nanos))
}

/// Parse a CSV field into a [`PropertyValue`] for the given target type.
fn parse_property_value(
    field: &str,
    target_type: &BqlType,
    col_name: &str,
    row: u64,
) -> Result<PropertyValue> {
    match target_type {
        BqlType::Bool => {
            if field.eq_ignore_ascii_case("true")
                || field == "1"
                || field.eq_ignore_ascii_case("yes")
            {
                Ok(PropertyValue::Bool(true))
            } else if field.eq_ignore_ascii_case("false")
                || field == "0"
                || field.eq_ignore_ascii_case("no")
            {
                Ok(PropertyValue::Bool(false))
            } else {
                Err(BqliteError::Execution(format!(
                    "CSV reader row {row}: cannot parse '{field}' as Bool for column '{col_name}'"
                )))
            }
        }
        BqlType::Int => {
            let n: i64 = field.parse().map_err(|_| {
                BqliteError::Execution(format!(
                    "CSV reader row {row}: cannot parse '{field}' as Int for column '{col_name}'"
                ))
            })?;
            Ok(PropertyValue::Int(n))
        }
        BqlType::Float => {
            let f: f64 = field.parse().map_err(|_| {
                BqliteError::Execution(format!(
                    "CSV reader row {row}: cannot parse '{field}' as Float for column '{col_name}'"
                ))
            })?;
            Ok(PropertyValue::Float(f))
        }
        BqlType::String => Ok(PropertyValue::String(field.to_string())),
        BqlType::Timestamp => {
            let nanos: i64 = field.parse().map_err(|_| {
                BqliteError::Execution(format!(
                    "CSV reader row {row}: cannot parse '{field}' as Timestamp for column '{col_name}'"
                ))
            })?;
            Ok(PropertyValue::Timestamp(nanos))
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "CSV reader row {row}: nested type {target_type:?} for column '{col_name}' \
             is not supported in Wave 2"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::schema::ColumnDef;

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

    fn reader_from_csv(
        csv_text: &str,
        table: &TableSchema,
        column_map: &[(String, String)],
        options: &CsvReaderOptions,
    ) -> CsvEventReader<io::BufReader<io::Cursor<Vec<u8>>>> {
        let cursor = io::Cursor::new(csv_text.as_bytes().to_vec());
        let buf = io::BufReader::new(cursor);
        CsvEventReader::from_reader(buf, table, column_map, options).unwrap()
    }

    fn collect_events(
        reader: &mut CsvEventReader<io::BufReader<io::Cursor<Vec<u8>>>>,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        while let Some(ev) = reader.next_event().unwrap() {
            events.push(ev);
        }
        events
    }

    // ── Passthrough mode (no map clause) ───────────────────────────

    #[test]
    fn passthrough_reads_all_columns_by_name() {
        let csv = "user_id,ts,event_type,amount,country\n\
                   alice,1700000000000000000,click,42,US\n\
                   bob,1700000000100000000,view,,UK\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 2);

        // First row
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[0].timestamp_nanos(), 1_700_000_000_000_000_000);
        assert_eq!(events[0].event_type, "click");
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(42)));
        assert_eq!(
            events[0].get("country"),
            Some(&PropertyValue::String("US".into()))
        );

        // Second row — empty amount → null
        assert_eq!(events[1].entity, EntityId::from("bob"));
        assert_eq!(events[1].get("amount"), Some(&PropertyValue::Null));
        assert_eq!(
            events[1].get("country"),
            Some(&PropertyValue::String("UK".into()))
        );
    }

    // ── Explicit map clause ────────────────────────────────────────

    #[test]
    fn explicit_map_remaps_source_to_target_columns() {
        let csv = "uid,time,evt,val\n\
                   charlie,1700000000200000000,purchase,100\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let map = vec![
            ("uid".into(), "user_id".into()),
            ("time".into(), "ts".into()),
            ("evt".into(), "event_type".into()),
            ("val".into(), "amount".into()),
        ];
        let mut reader = reader_from_csv(csv, &schema, &map, &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, EntityId::from("charlie"));
        assert_eq!(events[0].timestamp_nanos(), 1_700_000_000_200_000_000);
        assert_eq!(events[0].event_type, "purchase");
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(100)));
    }

    // ── Map + passthrough ──────────────────────────────────────────

    #[test]
    fn map_plus_passthrough_picks_up_unmapped_matching_columns() {
        // CSV has `uid`, `ts`, `event_type`, `amount`.
        // Map remaps `uid → user_id`. `ts`, `event_type`, `amount`
        // should pass through because their names match table columns.
        let csv = "uid,ts,event_type,amount\n\
                   dave,1700000000300000000,click,7\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let map = vec![("uid".into(), "user_id".into())];
        let mut reader = reader_from_csv(csv, &schema, &map, &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, EntityId::from("dave"));
        assert_eq!(events[0].event_type, "click");
        assert_eq!(events[0].get("amount"), Some(&PropertyValue::Int(7)));
    }

    // ── Custom delimiter ───────────────────────────────────────────

    #[test]
    fn custom_delimiter_tab() {
        let csv = "user_id\tts\tevent_type\n\
                   alice\t1700000000000000000\tclick\n";
        let schema = events_schema();
        let opts = CsvReaderOptions {
            delimiter: b'\t',
            has_header: true,
        };
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, EntityId::from("alice"));
    }

    // ── Int entity key ─────────────────────────────────────────────

    #[test]
    fn int_entity_key_parses_correctly() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("uid", BqlType::Int),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("ev", BqlType::String),
            ],
            "uid",
            "ts",
            "ev",
        )
        .unwrap();
        let csv = "uid,ts,ev\n42,1000,click\n";
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, EntityId::Int(42));
    }

    // ── Error: missing required column in passthrough ──────────────

    #[test]
    fn passthrough_errors_on_missing_required_column() {
        let csv = "user_id,ts\nalice,1000\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let cursor = io::Cursor::new(csv.as_bytes().to_vec());
        let buf = io::BufReader::new(cursor);
        let err = CsvEventReader::from_reader(buf, &schema, &[], &opts).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("event_type"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: missing source column in map ────────────────────────

    #[test]
    fn map_errors_on_missing_source_column() {
        let csv = "x,y,z\n1,2,3\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let map = vec![("nonexistent".into(), "user_id".into())];
        let cursor = io::Cursor::new(csv.as_bytes().to_vec());
        let buf = io::BufReader::new(cursor);
        let err = CsvEventReader::from_reader(buf, &schema, &map, &opts).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("nonexistent"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: type mismatch at parse time ─────────────────────────

    #[test]
    fn type_mismatch_errors_with_row_number() {
        let csv = "user_id,ts,event_type,amount\n\
                   alice,1000,click,not_a_number\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("row 2"), "got: {msg}");
                assert!(msg.contains("not_a_number"), "got: {msg}");
                assert!(msg.contains("amount"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Error: NOT NULL column with empty field ────────────────────

    #[test]
    fn not_null_column_errors_on_empty_field() {
        let csv = "user_id,ts,event_type\n,1000,click\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let err = reader.next_event().unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(
                    msg.contains("empty value") || msg.contains("NOT NULL"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    // ── Empty CSV returns no events ────────────────────────────────

    #[test]
    fn empty_csv_returns_none_immediately() {
        let csv = "user_id,ts,event_type\n";
        let schema = events_schema();
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        assert!(reader.next_event().unwrap().is_none());
    }

    // ── Bool property parsing ──────────────────────────────────────

    #[test]
    fn bool_property_parsing() {
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
        let csv = "user_id,ts,event_type,active\n\
                   alice,1000,click,true\n\
                   bob,2000,click,false\n\
                   charlie,3000,click,1\n";
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].get("active"), Some(&PropertyValue::Bool(true)));
        assert_eq!(events[1].get("active"), Some(&PropertyValue::Bool(false)));
        assert_eq!(events[2].get("active"), Some(&PropertyValue::Bool(true)));
    }

    // ── Float property parsing ─────────────────────────────────────

    #[test]
    fn float_property_parsing() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("price", BqlType::Float),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .unwrap();
        let csv = "user_id,ts,event_type,price\nalice,1000,buy,12.99\n";
        let opts = CsvReaderOptions::default();
        let mut reader = reader_from_csv(csv, &schema, &[], &opts);
        let events = collect_events(&mut reader);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].get("price"), Some(&PropertyValue::Float(12.99)));
    }

    // ── CsvReaderOptions::from_options ──────────────────────────────

    #[test]
    fn options_from_planner_parses_delimiter_and_header() {
        let opts = CsvReaderOptions::from_options(&[
            ("delimiter".into(), PropertyValue::String("\t".into())),
            ("header".into(), PropertyValue::String("false".into())),
        ])
        .unwrap();
        assert_eq!(opts.delimiter, b'\t');
        assert!(!opts.has_header);
    }

    #[test]
    fn options_default_is_comma_with_header() {
        let opts = CsvReaderOptions::default();
        assert_eq!(opts.delimiter, b',');
        assert!(opts.has_header);
    }

    #[test]
    fn options_rejects_multi_byte_delimiter() {
        let err = CsvReaderOptions::from_options(&[(
            "delimiter".into(),
            PropertyValue::String(";;".into()),
        )])
        .unwrap_err();
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    // ── Headerless CSV ─────────────────────────────────────────────

    #[test]
    fn headerless_csv_with_positional_map_works() {
        // No header row — the map clause uses positional indices "0", "1", "2".
        let csv = "alice,1000,click\nbob,2000,view\n";
        let schema = events_schema();
        let opts = CsvReaderOptions {
            delimiter: b',',
            has_header: false,
        };
        let map = vec![
            ("0".into(), "user_id".into()),
            ("1".into(), "ts".into()),
            ("2".into(), "event_type".into()),
        ];
        let cursor = io::Cursor::new(csv.as_bytes().to_vec());
        let buf = io::BufReader::new(cursor);
        let mut reader = CsvEventReader::from_reader(buf, &schema, &map, &opts).unwrap();
        let mut events = Vec::new();
        while let Some(ev) = reader.next_event().unwrap() {
            events.push(ev);
        }

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].entity, EntityId::from("alice"));
        assert_eq!(events[0].timestamp_nanos(), 1000);
        assert_eq!(events[0].event_type, "click");
        assert_eq!(events[1].entity, EntityId::from("bob"));
    }

    #[test]
    fn headerless_csv_without_map_errors() {
        let csv = "alice,1000,click\n";
        let schema = events_schema();
        let opts = CsvReaderOptions {
            delimiter: b',',
            has_header: false,
        };
        let cursor = io::Cursor::new(csv.as_bytes().to_vec());
        let buf = io::BufReader::new(cursor);
        let err = CsvEventReader::from_reader(buf, &schema, &[], &opts).unwrap_err();
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("headerless"), "got: {msg}");
                assert!(msg.contains("map"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }
}
