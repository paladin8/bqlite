//! Bidirectional conversion between bqlite types and Apache Arrow
//! types. This module is the single source of truth for the Arrow
//! type mapping specified in `docs/design/type-system.md` §7.
//!
//! The forward direction (`BqlType → DataType`, `TableSchema →
//! ArrowSchema`) emits the canonical Arrow representation bqlite uses
//! internally — `Utf8View` for strings, `Timestamp(Nanosecond, "UTC")`
//! for timestamps, homogeneous `List(item)` and `Map(key, value)` for
//! nested types.
//!
//! The reverse direction (`DataType → BqlType`) is used at the ingest
//! boundary when bqlite receives Arrow data from external sources
//! (Parquet, CSV readers, user-provided RecordBatches). It widens
//! narrower integer types, accepts any UTF-8 string variant, and
//! normalizes any timestamp / date / duration encoding to the
//! canonical BQL representation, per §7.2. Arrow types that have no
//! BQL equivalent (decimals, binary, time-of-day, fixed-size list,
//! structs outside a Map entry, unions, null, run-end-encoded) are
//! rejected with a typed error.
//!
//! **Round-trip guarantee (§7.3).** For every `BqlType` variant,
//! `arrow_to_bql_type(&bql_type_to_arrow(&t)) == Ok(t)`. This is
//! verified exhaustively by the `round_trip_all_variants` test below.

use std::sync::Arc;

use ::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};

use crate::error::{BqliteError, Result};
use crate::property::{BqlType, PropertyValue};
use crate::schema::{ColumnDef, TableSchema};

// ─────────────────────────────────────────────────────────────────────────────
// Forward: BqlType → Arrow
// ─────────────────────────────────────────────────────────────────────────────

/// Canonical Arrow `DataType` for a `BqlType`, per §7.1.
///
/// This is the exact shape bqlite emits for declared column types,
/// intermediate operator outputs, and schema round-tripping through
/// the catalog. The reverse direction [`arrow_to_bql_type`] is
/// intentionally broader (widens narrower Arrow types) so
/// `bql → arrow → bql` is stable but `arrow → bql → arrow` is not.
pub fn bql_type_to_arrow(ty: &BqlType) -> DataType {
    match ty {
        BqlType::Bool => DataType::Boolean,
        BqlType::Int => DataType::Int64,
        BqlType::Float => DataType::Float64,
        BqlType::String => DataType::Utf8View,
        BqlType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        BqlType::List(elem) => {
            // List elements are always nullable in the canonical
            // encoding — a `NULL` element inside a list is distinct
            // from the list itself being absent.
            DataType::List(Arc::new(Field::new("item", bql_type_to_arrow(elem), true)))
        }
        BqlType::Map(value) => {
            let entries = DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8View, false),
                Field::new("value", bql_type_to_arrow(value), true),
            ]));
            // Second arg (`keys_sorted`) is false — we make no
            // ordering promise on map keys in the canonical form.
            DataType::Map(Arc::new(Field::new("entries", entries, false)), false)
        }
    }
}

/// Arrow `Field` for a declared `ColumnDef`, preserving the
/// `nullable` flag.
pub fn column_def_to_field(col: &ColumnDef) -> Field {
    Field::new(&col.name, bql_type_to_arrow(&col.bql_type), col.nullable)
}

/// Arrow `Schema` for a `TableSchema`, including the implicit system
/// columns (`__seq_id`, `__batch_id`) at the tail.
///
/// This is the physical shape of a row-group read from storage: the
/// declared columns in DDL order followed by the two system columns.
/// For just the declared (user-visible) columns, iterate
/// `table.columns()` and call [`column_def_to_field`] directly.
pub fn table_schema_to_arrow(table: &TableSchema) -> ArrowSchema {
    let fields: Vec<Field> = table
        .logical_columns()
        .map(|c| column_def_to_field(&c))
        .collect();
    ArrowSchema::new(fields)
}

/// Arrow `DataType` corresponding to the dynamic type of a
/// `PropertyValue`, or `None` for `PropertyValue::Null` (whose type
/// is context-dependent).
///
/// **Not for ingest.** This delegates to `PropertyValue::bql_type`,
/// whose list/map element inference silently defaults to `String`
/// when every element is `Null`. Callers at the ingest boundary know
/// the target column type and should call [`bql_type_to_arrow`]
/// directly — this helper is for test fixtures and debug formatting
/// only.
pub fn property_value_to_arrow_type(pv: &PropertyValue) -> Option<DataType> {
    pv.bql_type().as_ref().map(bql_type_to_arrow)
}

// ─────────────────────────────────────────────────────────────────────────────
// Reverse: Arrow → BqlType
// ─────────────────────────────────────────────────────────────────────────────

/// Widen an Arrow `DataType` to its canonical `BqlType`, per §7.2.
///
/// Widening summary:
///
/// - Every signed/unsigned integer width → `BqlType::Int` (`i64`)
/// - `Float16 / Float32 / Float64` → `BqlType::Float` (`f64`)
/// - `Utf8 / LargeUtf8 / Utf8View` → `BqlType::String`
/// - `Dictionary<_, Utf8*>` → `BqlType::String`
/// - `Timestamp(_, _)` / `Date32` / `Date64` → `BqlType::Timestamp`
/// - `Duration(_)` → `BqlType::Int` (nanoseconds)
/// - `List(f)` / `LargeList(f)` → `BqlType::List(arrow_to_bql_type(f))`
/// - `Map(entries, _)` → `BqlType::Map(arrow_to_bql_type(value_field))` —
///   requires `entries` to be `Struct { key: Utf8*, value: _ }`
///
/// Unsupported Arrow types (`Null`, `Binary*`, `Decimal*`, `Time32/64`,
/// `Interval`, `FixedSizeList`, `FixedSizeBinary`, `Struct` outside a
/// Map entry, `Union`, `RunEndEncoded`, `ListView`, `LargeListView`,
/// and nested `Dictionary` with a non-string value) return
/// `BqliteError::Schema` with a descriptive message.
pub fn arrow_to_bql_type(ty: &DataType) -> Result<BqlType> {
    match ty {
        DataType::Boolean => Ok(BqlType::Bool),

        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(BqlType::Int),

        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(BqlType::Float),

        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(BqlType::String),

        DataType::Dictionary(_key_type, value_type) => match value_type.as_ref() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(BqlType::String),
            other => Err(BqliteError::Schema(format!(
                "Arrow Dictionary value type {other:?} is not representable as a BqlType (only string values are supported)"
            ))),
        },

        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => Ok(BqlType::Timestamp),

        DataType::Duration(_) => Ok(BqlType::Int),

        DataType::List(field) | DataType::LargeList(field) => Ok(BqlType::List(Box::new(
            arrow_to_bql_type(field.data_type())?,
        ))),

        DataType::Map(entries_field, _keys_sorted) => {
            // Canonical Arrow Map shape: a non-nullable Struct with
            // exactly two fields — `key` (UTF-8 string, non-nullable)
            // and `value`. We accept any reasonable variant here
            // because external Arrow producers sometimes emit slight
            // variations (nullable entries, alternative field names).
            let DataType::Struct(fields) = entries_field.data_type() else {
                return Err(BqliteError::Schema(format!(
                    "Arrow Map entries must be a Struct, found {:?}",
                    entries_field.data_type()
                )));
            };
            if fields.len() != 2 {
                return Err(BqliteError::Schema(format!(
                    "Arrow Map entries Struct must have exactly 2 fields (key, value), found {}",
                    fields.len()
                )));
            }
            let key_field = &fields[0];
            let value_field = &fields[1];
            match key_field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {}
                other => {
                    return Err(BqliteError::Schema(format!(
                        "Arrow Map key must be a UTF-8 string type, found {other:?}"
                    )));
                }
            }
            // The canonical Arrow Map shape (and BQL semantics) require
            // a non-nullable key — a NULL key has no meaningful lookup
            // behavior. Forward emission sets `nullable = false`, so we
            // reject nullable keys on the reverse path to keep the two
            // directions symmetric.
            if key_field.is_nullable() {
                return Err(BqliteError::Schema(
                    "Arrow Map key must be non-nullable".into(),
                ));
            }
            Ok(BqlType::Map(Box::new(arrow_to_bql_type(
                value_field.data_type(),
            )?)))
        }

        other => Err(BqliteError::Schema(format!(
            "Arrow type {other:?} has no BqlType equivalent"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use ::arrow::datatypes::IntervalUnit;

    use crate::schema::{OperatorSchema, BATCH_ID_COLUMN, SEQ_ID_COLUMN};

    // ── Forward: scalars ──────────────────────────────────────────────────────

    #[test]
    fn forward_scalars() {
        assert_eq!(bql_type_to_arrow(&BqlType::Bool), DataType::Boolean);
        assert_eq!(bql_type_to_arrow(&BqlType::Int), DataType::Int64);
        assert_eq!(bql_type_to_arrow(&BqlType::Float), DataType::Float64);
        assert_eq!(bql_type_to_arrow(&BqlType::String), DataType::Utf8View);
        assert_eq!(
            bql_type_to_arrow(&BqlType::Timestamp),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }

    #[test]
    fn forward_list_has_nullable_item_field() {
        let dt = bql_type_to_arrow(&BqlType::List(Box::new(BqlType::Int)));
        let DataType::List(inner) = dt else {
            panic!("expected List");
        };
        assert_eq!(inner.name(), "item");
        assert_eq!(inner.data_type(), &DataType::Int64);
        assert!(inner.is_nullable());
    }

    #[test]
    fn forward_map_has_string_keys_and_nullable_values() {
        let dt = bql_type_to_arrow(&BqlType::Map(Box::new(BqlType::Float)));
        let DataType::Map(entries, keys_sorted) = dt else {
            panic!("expected Map");
        };
        assert!(!keys_sorted);
        assert_eq!(entries.name(), "entries");
        assert!(!entries.is_nullable());
        let DataType::Struct(inner) = entries.data_type() else {
            panic!("expected Struct inside Map entries");
        };
        assert_eq!(inner.len(), 2);
        assert_eq!(inner[0].name(), "key");
        assert_eq!(inner[0].data_type(), &DataType::Utf8View);
        assert!(!inner[0].is_nullable());
        assert_eq!(inner[1].name(), "value");
        assert_eq!(inner[1].data_type(), &DataType::Float64);
        assert!(inner[1].is_nullable());
    }

    #[test]
    fn forward_nested_list_of_map() {
        // List<Map<Utf8View, Utf8View>>
        let bql = BqlType::List(Box::new(BqlType::Map(Box::new(BqlType::String))));
        let dt = bql_type_to_arrow(&bql);
        let DataType::List(item) = &dt else {
            panic!("expected List, found {dt:?}");
        };
        let DataType::Map(entries, _) = item.data_type() else {
            panic!("expected Map inside List, found {:?}", item.data_type());
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected Struct, found {:?}", entries.data_type());
        };
        assert_eq!(fields[1].data_type(), &DataType::Utf8View);
    }

    // ── Reverse: scalars ──────────────────────────────────────────────────────

    #[test]
    fn reverse_scalars() {
        assert_eq!(
            arrow_to_bql_type(&DataType::Boolean).unwrap(),
            BqlType::Bool
        );
        assert_eq!(arrow_to_bql_type(&DataType::Int64).unwrap(), BqlType::Int);
        assert_eq!(
            arrow_to_bql_type(&DataType::Float64).unwrap(),
            BqlType::Float
        );
        assert_eq!(
            arrow_to_bql_type(&DataType::Utf8View).unwrap(),
            BqlType::String
        );
        assert_eq!(
            arrow_to_bql_type(&DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("UTC".into())
            ))
            .unwrap(),
            BqlType::Timestamp
        );
    }

    #[test]
    fn reverse_widens_integer_widths() {
        for dt in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ] {
            assert_eq!(arrow_to_bql_type(&dt).unwrap(), BqlType::Int, "dt = {dt:?}");
        }
    }

    #[test]
    fn reverse_widens_float_widths() {
        for dt in [DataType::Float16, DataType::Float32, DataType::Float64] {
            assert_eq!(
                arrow_to_bql_type(&dt).unwrap(),
                BqlType::Float,
                "dt = {dt:?}"
            );
        }
    }

    #[test]
    fn reverse_accepts_every_utf8_variant() {
        for dt in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            assert_eq!(
                arrow_to_bql_type(&dt).unwrap(),
                BqlType::String,
                "dt = {dt:?}"
            );
        }
    }

    #[test]
    fn reverse_dictionary_with_string_value() {
        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        assert_eq!(arrow_to_bql_type(&dt).unwrap(), BqlType::String);
        let dt = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
        assert_eq!(arrow_to_bql_type(&dt).unwrap(), BqlType::String);
        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8));
        assert_eq!(arrow_to_bql_type(&dt).unwrap(), BqlType::String);
    }

    #[test]
    fn reverse_dictionary_with_non_string_value_errors() {
        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64));
        let err = arrow_to_bql_type(&dt).unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)), "{err}");
    }

    #[test]
    fn reverse_normalizes_any_timestamp_unit_and_timezone() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let naive = DataType::Timestamp(unit, None);
            assert_eq!(arrow_to_bql_type(&naive).unwrap(), BqlType::Timestamp);
            let tz = DataType::Timestamp(unit, Some("America/Los_Angeles".into()));
            assert_eq!(arrow_to_bql_type(&tz).unwrap(), BqlType::Timestamp);
        }
    }

    #[test]
    fn reverse_date_types_become_timestamp() {
        assert_eq!(
            arrow_to_bql_type(&DataType::Date32).unwrap(),
            BqlType::Timestamp
        );
        assert_eq!(
            arrow_to_bql_type(&DataType::Date64).unwrap(),
            BqlType::Timestamp
        );
    }

    #[test]
    fn reverse_duration_becomes_int() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            assert_eq!(
                arrow_to_bql_type(&DataType::Duration(unit)).unwrap(),
                BqlType::Int
            );
        }
    }

    #[test]
    fn reverse_list_recursive() {
        let dt = DataType::List(Arc::new(Field::new("x", DataType::Int32, true)));
        assert_eq!(
            arrow_to_bql_type(&dt).unwrap(),
            BqlType::List(Box::new(BqlType::Int))
        );
    }

    #[test]
    fn reverse_large_list_recursive() {
        let dt = DataType::LargeList(Arc::new(Field::new("x", DataType::Utf8, true)));
        assert_eq!(
            arrow_to_bql_type(&dt).unwrap(),
            BqlType::List(Box::new(BqlType::String))
        );
    }

    #[test]
    fn reverse_map_recursive() {
        // Build a well-formed Arrow Map via the forward direction.
        let dt = bql_type_to_arrow(&BqlType::Map(Box::new(BqlType::Int)));
        assert_eq!(
            arrow_to_bql_type(&dt).unwrap(),
            BqlType::Map(Box::new(BqlType::Int))
        );
    }

    #[test]
    fn reverse_map_rejects_non_struct_entries() {
        let entries = Arc::new(Field::new("entries", DataType::Int32, false));
        let dt = DataType::Map(entries, false);
        let err = arrow_to_bql_type(&dt).unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)), "{err}");
    }

    #[test]
    fn reverse_map_rejects_wrong_field_count() {
        let entries_struct =
            DataType::Struct(Fields::from(vec![Field::new("key", DataType::Utf8, false)]));
        let dt = DataType::Map(
            Arc::new(Field::new("entries", entries_struct, false)),
            false,
        );
        let err = arrow_to_bql_type(&dt).unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)), "{err}");
    }

    #[test]
    fn reverse_map_rejects_non_string_key() {
        let entries_struct = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]));
        let dt = DataType::Map(
            Arc::new(Field::new("entries", entries_struct, false)),
            false,
        );
        let err = arrow_to_bql_type(&dt).unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)), "{err}");
    }

    #[test]
    fn reverse_map_rejects_nullable_key() {
        // A nullable key is not representable in BQL and the forward
        // direction always emits `nullable = false` — reject on reverse
        // to keep the two directions symmetric.
        let entries_struct = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let dt = DataType::Map(
            Arc::new(Field::new("entries", entries_struct, false)),
            false,
        );
        let err = arrow_to_bql_type(&dt).unwrap_err();
        let msg = format!("{err}");
        assert!(
            matches!(err, BqliteError::Schema(_)) && msg.contains("non-nullable"),
            "{msg}"
        );
    }

    #[test]
    fn reverse_dictionary_with_nested_value_errors() {
        // Dictionary<_, List<_>> is not Utf8-valued, so reject.
        let value = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let dt = DataType::Dictionary(Box::new(DataType::Int32), Box::new(value));
        let err = arrow_to_bql_type(&dt).unwrap_err();
        assert!(matches!(err, BqliteError::Schema(_)), "{err}");
    }

    #[test]
    fn reverse_rejects_unsupported_types() {
        use ::arrow::datatypes::{UnionFields, UnionMode};

        let union_fields = UnionFields::try_new(
            vec![0_i8, 1],
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ],
        )
        .expect("valid union fields");

        let cases: Vec<DataType> = vec![
            DataType::Null,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
            DataType::BinaryView,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Decimal128(10, 2),
            DataType::Decimal256(40, 10),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
            DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
            DataType::Union(union_fields, UnionMode::Dense),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Int64, true)),
            ),
            DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true))),
        ];
        for dt in cases {
            let err = arrow_to_bql_type(&dt).unwrap_err();
            assert!(matches!(err, BqliteError::Schema(_)), "{dt:?}: {err}");
        }
    }

    // ── Round-trip guarantee (§7.3) ───────────────────────────────────────────

    /// Compile-time exhaustiveness check: adding a new `BqlType`
    /// variant forces this match to be updated, which in turn forces
    /// the author to add a covering case to [`round_trip_all_variants`]
    /// (since both this function and the test below fail together).
    #[allow(dead_code)]
    fn _round_trip_exhaustiveness_guard(t: &BqlType) {
        match t {
            BqlType::Bool
            | BqlType::Int
            | BqlType::Float
            | BqlType::String
            | BqlType::Timestamp
            | BqlType::List(_)
            | BqlType::Map(_) => {}
        }
    }

    #[test]
    fn round_trip_all_variants() {
        // Exhaustive over the canonical BqlType shape: every scalar,
        // every list-of-scalar, every map-of-scalar, plus deep
        // nesting on both collection sides. The companion
        // `_round_trip_exhaustiveness_guard` above forces a compile
        // error when a new BqlType variant is added without also
        // being considered here.
        let cases: Vec<BqlType> = vec![
            BqlType::Bool,
            BqlType::Int,
            BqlType::Float,
            BqlType::String,
            BqlType::Timestamp,
            BqlType::List(Box::new(BqlType::Bool)),
            BqlType::List(Box::new(BqlType::Int)),
            BqlType::List(Box::new(BqlType::Float)),
            BqlType::List(Box::new(BqlType::String)),
            BqlType::List(Box::new(BqlType::Timestamp)),
            BqlType::Map(Box::new(BqlType::Bool)),
            BqlType::Map(Box::new(BqlType::Int)),
            BqlType::Map(Box::new(BqlType::Float)),
            BqlType::Map(Box::new(BqlType::String)),
            BqlType::Map(Box::new(BqlType::Timestamp)),
            // Nesting.
            BqlType::List(Box::new(BqlType::List(Box::new(BqlType::Int)))),
            BqlType::Map(Box::new(BqlType::List(Box::new(BqlType::Float)))),
            BqlType::List(Box::new(BqlType::Map(Box::new(BqlType::String)))),
            BqlType::List(Box::new(BqlType::Map(Box::new(BqlType::List(Box::new(
                BqlType::Int,
            )))))),
        ];
        for bql in cases {
            let dt = bql_type_to_arrow(&bql);
            let back = arrow_to_bql_type(&dt).expect("round trip must succeed");
            assert_eq!(back, bql, "round trip mismatch for {bql}");
        }
    }

    // ── TableSchema → ArrowSchema ─────────────────────────────────────────────

    fn minimal_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Float),
                ColumnDef::nullable("tags", BqlType::List(Box::new(BqlType::String))),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    #[test]
    fn table_schema_to_arrow_includes_system_columns_and_preserves_nullability() {
        let t = minimal_schema();
        let arrow = table_schema_to_arrow(&t);
        // 5 declared + 2 system
        assert_eq!(arrow.fields().len(), 7);
        // Declared columns preserve nullability.
        assert_eq!(arrow.field(0).name(), "entity_id");
        assert_eq!(arrow.field(0).data_type(), &DataType::Utf8View);
        assert!(!arrow.field(0).is_nullable());
        assert_eq!(arrow.field(3).name(), "amount");
        assert_eq!(arrow.field(3).data_type(), &DataType::Float64);
        assert!(arrow.field(3).is_nullable());
        // System columns at the tail, non-nullable Int64.
        assert_eq!(arrow.field(5).name(), SEQ_ID_COLUMN);
        assert_eq!(arrow.field(5).data_type(), &DataType::Int64);
        assert!(!arrow.field(5).is_nullable());
        assert_eq!(arrow.field(6).name(), BATCH_ID_COLUMN);
        assert_eq!(arrow.field(6).data_type(), &DataType::Int64);
        assert!(!arrow.field(6).is_nullable());
    }

    #[test]
    fn table_schema_to_arrow_matches_operator_schema_path() {
        // The two entry points `table_schema_to_arrow(&t)` and
        // `OperatorSchema::from_table(&t).to_arrow_schema()` must
        // produce equal Arrow schemas. Locking this in with an
        // equality check prevents the two code paths from drifting.
        let t = minimal_schema();
        let via_table = table_schema_to_arrow(&t);
        let via_operator = OperatorSchema::from_table(&t).to_arrow_schema();
        assert_eq!(via_table, via_operator);
    }

    #[test]
    fn column_def_to_field_preserves_nullable_flag() {
        let required = ColumnDef::required("x", BqlType::Int);
        let nullable = ColumnDef::nullable("x", BqlType::Int);
        assert!(!column_def_to_field(&required).is_nullable());
        assert!(column_def_to_field(&nullable).is_nullable());
    }

    // ── PropertyValue → Arrow type ────────────────────────────────────────────

    #[test]
    fn property_value_to_arrow_type_null_returns_none() {
        assert!(property_value_to_arrow_type(&PropertyValue::Null).is_none());
    }

    #[test]
    fn property_value_to_arrow_type_scalars() {
        assert_eq!(
            property_value_to_arrow_type(&PropertyValue::Bool(true)),
            Some(DataType::Boolean)
        );
        assert_eq!(
            property_value_to_arrow_type(&PropertyValue::Int(7)),
            Some(DataType::Int64)
        );
        assert_eq!(
            property_value_to_arrow_type(&PropertyValue::Timestamp(0)),
            Some(DataType::Timestamp(
                TimeUnit::Nanosecond,
                Some("UTC".into())
            ))
        );
    }

    #[test]
    fn property_value_to_arrow_type_list_infers_from_first_non_null() {
        let pv = PropertyValue::List(vec![
            PropertyValue::Null,
            PropertyValue::Int(1),
            PropertyValue::Int(2),
        ]);
        let dt = property_value_to_arrow_type(&pv).unwrap();
        let DataType::List(item) = dt else {
            panic!("expected List");
        };
        assert_eq!(item.data_type(), &DataType::Int64);
    }
}
