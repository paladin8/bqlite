//! `BqlType` and `PropertyValue` — the type enum and dynamic value type
//! used at the ingest boundary and in test fixtures.
//!
//! `PropertyValue` does **not** appear in the query execution hot path.
//! Once data reaches columnar Arrow arrays the engine operates purely on
//! Arrow batches. `PropertyValue` is a boundary type: ingest, schema
//! evolution defaults, and test fixtures.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};

// ──────────────────────────────────────────────────────────────────────────────
// BqlType
// ──────────────────────────────────────────────────────────────────────────────

/// The static type of a BQL column or expression.
///
/// This enum is small and `Copy`-friendly once `Box` is acceptable for the
/// recursive variants; discriminant comparisons compile to direct integer
/// comparisons (no heap access).
///
/// See docs/design/type-system.md §2 for rationale on each variant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BqlType {
    /// SQL `BOOL` — Rust `bool`, Arrow `Boolean`.
    Bool,
    /// SQL `INT` — Rust `i64`, Arrow `Int64`.
    ///
    /// Duration literals (`7d`, `30m`) are represented as `Int` (nanoseconds).
    Int,
    /// SQL `FLOAT` — Rust `f64`, Arrow `Float64`.
    Float,
    /// SQL `STRING` — UTF-8, Arrow `Utf8View`.
    String,
    /// SQL `TIMESTAMP` — epoch nanoseconds UTC, Arrow `Timestamp(Nanosecond, Some("UTC"))`.
    Timestamp,
    /// Homogeneously-typed list, Arrow `List(elem.to_arrow())`.
    List(Box<BqlType>),
    /// String-keyed map with homogeneous values, Arrow `Map(Utf8View, value.to_arrow())`.
    Map(Box<BqlType>),
}

impl fmt::Display for BqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BqlType::Bool => write!(f, "BOOL"),
            BqlType::Int => write!(f, "INT"),
            BqlType::Float => write!(f, "FLOAT"),
            BqlType::String => write!(f, "STRING"),
            BqlType::Timestamp => write!(f, "TIMESTAMP"),
            BqlType::List(elem) => write!(f, "LIST({elem})"),
            BqlType::Map(val) => write!(f, "MAP({val})"),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// PropertyValue
// ──────────────────────────────────────────────────────────────────────────────

/// A dynamically-typed scalar value used at the ingest boundary.
///
/// Variants map 1:1 to `BqlType` variants plus `Null`.
///
/// Ordering is defined for use in sort/comparison operations on dynamic data:
/// `Null` sorts before everything; within a type, natural ordering applies;
/// across types the discriminant order (Bool < Int < Float < String <
/// Timestamp < List < Map) is used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PropertyValue {
    /// SQL `NULL` — absent value.
    Null,
    /// SQL `BOOL`.
    Bool(bool),
    /// SQL `INT` — stored as `i64`.
    Int(i64),
    /// SQL `FLOAT` — stored as `f64`.
    Float(f64),
    /// SQL `STRING` — UTF-8.
    String(std::string::String),
    /// SQL `TIMESTAMP` — epoch nanoseconds UTC.
    Timestamp(i64),
    /// SQL `LIST(T)`.
    List(Vec<PropertyValue>),
    /// SQL `MAP(V)` — ordered key-value pairs with `String` keys.
    Map(Vec<(std::string::String, PropertyValue)>),
}

impl PropertyValue {
    /// Returns the `BqlType` for this value, or `None` for `Null`.
    pub fn bql_type(&self) -> Option<BqlType> {
        match self {
            PropertyValue::Null => None,
            PropertyValue::Bool(_) => Some(BqlType::Bool),
            PropertyValue::Int(_) => Some(BqlType::Int),
            PropertyValue::Float(_) => Some(BqlType::Float),
            PropertyValue::String(_) => Some(BqlType::String),
            PropertyValue::Timestamp(_) => Some(BqlType::Timestamp),
            PropertyValue::List(elems) => {
                // Infer element type from the first non-null element.
                let elem_type = elems
                    .iter()
                    .find_map(|e| e.bql_type())
                    .unwrap_or(BqlType::String);
                Some(BqlType::List(Box::new(elem_type)))
            }
            PropertyValue::Map(pairs) => {
                let val_type = pairs
                    .iter()
                    .find_map(|(_, v)| v.bql_type())
                    .unwrap_or(BqlType::String);
                Some(BqlType::Map(Box::new(val_type)))
            }
        }
    }

    /// Attempts to coerce this value to the target `BqlType`.
    ///
    /// Returns `Some(coerced)` on success, `None` if coercion is impossible.
    /// Follows the TRY_CAST semantics described in docs/design/type-system.md §4.2:
    /// failed coercions return `None` (callers map to `Null`) rather than
    /// propagating an error.
    pub fn coerce_to(&self, target: &BqlType) -> Option<PropertyValue> {
        use PropertyValue as PV;

        // Null coerces to any type as Null.
        if let PV::Null = self {
            return Some(PV::Null);
        }

        match (self, target) {
            // Identity coercions.
            (PV::Bool(_), BqlType::Bool) => Some(self.clone()),
            (PV::Int(_), BqlType::Int) => Some(self.clone()),
            (PV::Float(_), BqlType::Float) => Some(self.clone()),
            (PV::String(_), BqlType::String) => Some(self.clone()),
            (PV::Timestamp(_), BqlType::Timestamp) => Some(self.clone()),
            (PV::List(_), BqlType::List(_)) => Some(self.clone()),
            (PV::Map(_), BqlType::Map(_)) => Some(self.clone()),

            // Int → Float (implicit widening).
            (PV::Int(n), BqlType::Float) => Some(PV::Float(*n as f64)),

            // Float → Int (truncation, explicit).
            (PV::Float(f), BqlType::Int) => {
                if f.is_finite() {
                    Some(PV::Int(*f as i64))
                } else {
                    None
                }
            }

            // Bool → Int.
            (PV::Bool(b), BqlType::Int) => Some(PV::Int(if *b { 1 } else { 0 })),

            // Bool → String.
            (PV::Bool(b), BqlType::String) => {
                Some(PV::String(if *b { "true" } else { "false" }.to_owned()))
            }

            // Int → String.
            (PV::Int(n), BqlType::String) => Some(PV::String(n.to_string())),

            // Float → String.
            (PV::Float(f), BqlType::String) => Some(PV::String(f.to_string())),

            // Timestamp → String (RFC 3339 / ISO-8601 UTC).
            //
            // Canonical form is `YYYY-MM-DDTHH:MM:SS(.fraction)?Z` with
            // `chrono::SecondsFormat::AutoSi` — trailing zero fractional
            // digits are trimmed, so `1_700_000_000_000_000_000` ns renders
            // as `2023-11-14T22:13:20Z` rather than `...22:13:20.000000000Z`.
            // The output always parses back via the `String → Timestamp`
            // arm below; see `docs/design/type-system.md §4.2`.
            (PV::Timestamp(ns), BqlType::String) => {
                use chrono::{DateTime, SecondsFormat, Utc};
                let dt: DateTime<Utc> = DateTime::from_timestamp_nanos(*ns);
                Some(PV::String(dt.to_rfc3339_opts(SecondsFormat::AutoSi, true)))
            }

            // Timestamp → Int (epoch nanos).
            (PV::Timestamp(ns), BqlType::Int) => Some(PV::Int(*ns)),

            // Int → Timestamp (interpret as epoch nanos).
            (PV::Int(n), BqlType::Timestamp) => Some(PV::Timestamp(*n)),

            // String → Timestamp (RFC 3339 / ISO-8601).
            //
            // Accepts any RFC 3339 form — UTC `Z`, fractional seconds at
            // any precision, or non-UTC offsets (e.g. `+05:30`) which are
            // normalized to UTC nanoseconds. Returns `None` on parse failure
            // *or* if the parsed instant falls outside the nanosecond-
            // representable range (chrono's `timestamp_nanos_opt` returns
            // `None` outside ~1677-09-21 .. 2262-04-11). Both failure modes
            // collapse to `NULL` per TRY_CAST semantics in type-system.md §4.2.
            (PV::String(s), BqlType::Timestamp) => {
                use chrono::DateTime;
                DateTime::parse_from_rfc3339(s.trim())
                    .ok()
                    .and_then(|dt| dt.timestamp_nanos_opt())
                    .map(PV::Timestamp)
            }

            // String → Int.
            (PV::String(s), BqlType::Int) => s.trim().parse::<i64>().ok().map(PV::Int),

            // String → Float.
            (PV::String(s), BqlType::Float) => s.trim().parse::<f64>().ok().map(PV::Float),

            // String → Bool.
            (PV::String(s), BqlType::Bool) => match s.trim().to_ascii_lowercase().as_str() {
                "true" => Some(PV::Bool(true)),
                "false" => Some(PV::Bool(false)),
                _ => None,
            },

            // Unsupported coercion.
            _ => None,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Equality
// ──────────────────────────────────────────────────────────────────────────────

impl PartialEq for PropertyValue {
    fn eq(&self, other: &Self) -> bool {
        use PropertyValue as PV;
        match (self, other) {
            (PV::Null, PV::Null) => true,
            (PV::Bool(a), PV::Bool(b)) => a == b,
            (PV::Int(a), PV::Int(b)) => a == b,
            // Use total ordering for floats: NaN == NaN for equality purposes.
            (PV::Float(a), PV::Float(b)) => a.total_cmp(b) == Ordering::Equal,
            (PV::String(a), PV::String(b)) => a == b,
            (PV::Timestamp(a), PV::Timestamp(b)) => a == b,
            (PV::List(a), PV::List(b)) => a == b,
            (PV::Map(a), PV::Map(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for PropertyValue {}

// ──────────────────────────────────────────────────────────────────────────────
// Ordering
// ──────────────────────────────────────────────────────────────────────────────

/// Numeric discriminant for cross-type ordering: Null < Bool < Int < Float <
/// String < Timestamp < List < Map.
fn type_rank(v: &PropertyValue) -> u8 {
    match v {
        PropertyValue::Null => 0,
        PropertyValue::Bool(_) => 1,
        PropertyValue::Int(_) => 2,
        PropertyValue::Float(_) => 3,
        PropertyValue::String(_) => 4,
        PropertyValue::Timestamp(_) => 5,
        PropertyValue::List(_) => 6,
        PropertyValue::Map(_) => 7,
    }
}

impl PartialOrd for PropertyValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PropertyValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use PropertyValue as PV;

        let rank_cmp = type_rank(self).cmp(&type_rank(other));
        if rank_cmp != Ordering::Equal {
            return rank_cmp;
        }

        match (self, other) {
            (PV::Null, PV::Null) => Ordering::Equal,
            (PV::Bool(a), PV::Bool(b)) => a.cmp(b),
            (PV::Int(a), PV::Int(b)) => a.cmp(b),
            (PV::Float(a), PV::Float(b)) => a.total_cmp(b),
            (PV::String(a), PV::String(b)) => a.cmp(b),
            (PV::Timestamp(a), PV::Timestamp(b)) => a.cmp(b),
            (PV::List(a), PV::List(b)) => a.cmp(b),
            (PV::Map(a), PV::Map(b)) => {
                // Compare map entries lexicographically by (key, value).
                let len_cmp = a.len().cmp(&b.len());
                if len_cmp != Ordering::Equal {
                    return len_cmp;
                }
                for ((ka, va), (kb, vb)) in a.iter().zip(b.iter()) {
                    let k = ka.cmp(kb);
                    if k != Ordering::Equal {
                        return k;
                    }
                    let v = va.cmp(vb);
                    if v != Ordering::Equal {
                        return v;
                    }
                }
                Ordering::Equal
            }
            // Same rank but different variants can't happen; guarded above.
            _ => unreachable!(),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Display
// ──────────────────────────────────────────────────────────────────────────────

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyValue::Null => write!(f, "NULL"),
            PropertyValue::Bool(b) => write!(f, "{b}"),
            PropertyValue::Int(n) => write!(f, "{n}"),
            PropertyValue::Float(v) => write!(f, "{v}"),
            PropertyValue::String(s) => write!(f, "{s}"),
            PropertyValue::Timestamp(ns) => {
                let secs = ns / 1_000_000_000;
                let subsec = (ns % 1_000_000_000).unsigned_abs();
                write!(f, "{secs}.{subsec:09}")
            }
            PropertyValue::List(elems) => {
                write!(f, "[")?;
                for (i, e) in elems.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{e}")?;
                }
                write!(f, "]")
            }
            PropertyValue::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── bql_type ──────────────────────────────────────────────────────────────

    #[test]
    fn bql_type_null_returns_none() {
        assert_eq!(PropertyValue::Null.bql_type(), None);
    }

    #[test]
    fn bql_type_scalars() {
        assert_eq!(PropertyValue::Bool(true).bql_type(), Some(BqlType::Bool));
        assert_eq!(PropertyValue::Int(0).bql_type(), Some(BqlType::Int));
        assert_eq!(PropertyValue::Float(0.0).bql_type(), Some(BqlType::Float));
        assert_eq!(
            PropertyValue::String("x".into()).bql_type(),
            Some(BqlType::String)
        );
        assert_eq!(
            PropertyValue::Timestamp(0).bql_type(),
            Some(BqlType::Timestamp)
        );
    }

    #[test]
    fn bql_type_list_infers_from_first_non_null() {
        let list = PropertyValue::List(vec![
            PropertyValue::Null,
            PropertyValue::Int(1),
            PropertyValue::Int(2),
        ]);
        assert_eq!(list.bql_type(), Some(BqlType::List(Box::new(BqlType::Int))));
    }

    #[test]
    fn bql_type_empty_list_defaults_to_string() {
        let list = PropertyValue::List(vec![]);
        assert_eq!(
            list.bql_type(),
            Some(BqlType::List(Box::new(BqlType::String)))
        );
    }

    #[test]
    fn bql_type_map_infers_from_first_non_null_value() {
        let map = PropertyValue::Map(vec![
            ("a".into(), PropertyValue::Null),
            ("b".into(), PropertyValue::Float(2.5)),
        ]);
        assert_eq!(map.bql_type(), Some(BqlType::Map(Box::new(BqlType::Float))));
    }

    // ── coerce_to ─────────────────────────────────────────────────────────────

    #[test]
    fn coerce_null_to_any_is_null() {
        for target in [
            BqlType::Bool,
            BqlType::Int,
            BqlType::Float,
            BqlType::String,
            BqlType::Timestamp,
        ] {
            assert_eq!(
                PropertyValue::Null.coerce_to(&target),
                Some(PropertyValue::Null)
            );
        }
    }

    #[test]
    fn coerce_identity() {
        assert_eq!(
            PropertyValue::Bool(true).coerce_to(&BqlType::Bool),
            Some(PropertyValue::Bool(true))
        );
        assert_eq!(
            PropertyValue::Int(42).coerce_to(&BqlType::Int),
            Some(PropertyValue::Int(42))
        );
        assert_eq!(
            PropertyValue::Float(1.5).coerce_to(&BqlType::Float),
            Some(PropertyValue::Float(1.5))
        );
    }

    #[test]
    fn coerce_int_to_float() {
        assert_eq!(
            PropertyValue::Int(7).coerce_to(&BqlType::Float),
            Some(PropertyValue::Float(7.0))
        );
    }

    #[test]
    fn coerce_float_to_int_truncates() {
        assert_eq!(
            PropertyValue::Float(3.9).coerce_to(&BqlType::Int),
            Some(PropertyValue::Int(3))
        );
        assert_eq!(
            PropertyValue::Float(f64::INFINITY).coerce_to(&BqlType::Int),
            None
        );
        assert_eq!(
            PropertyValue::Float(f64::NAN).coerce_to(&BqlType::Int),
            None
        );
    }

    #[test]
    fn coerce_bool_to_int() {
        assert_eq!(
            PropertyValue::Bool(true).coerce_to(&BqlType::Int),
            Some(PropertyValue::Int(1))
        );
        assert_eq!(
            PropertyValue::Bool(false).coerce_to(&BqlType::Int),
            Some(PropertyValue::Int(0))
        );
    }

    #[test]
    fn coerce_string_to_int_and_float() {
        assert_eq!(
            PropertyValue::String("42".into()).coerce_to(&BqlType::Int),
            Some(PropertyValue::Int(42))
        );
        assert_eq!(
            PropertyValue::String("not_a_number".into()).coerce_to(&BqlType::Int),
            None
        );
        assert_eq!(
            PropertyValue::String("1.5".into()).coerce_to(&BqlType::Float),
            Some(PropertyValue::Float(1.5))
        );
    }

    #[test]
    fn coerce_string_to_bool() {
        assert_eq!(
            PropertyValue::String("true".into()).coerce_to(&BqlType::Bool),
            Some(PropertyValue::Bool(true))
        );
        assert_eq!(
            PropertyValue::String("FALSE".into()).coerce_to(&BqlType::Bool),
            Some(PropertyValue::Bool(false))
        );
        assert_eq!(
            PropertyValue::String("yes".into()).coerce_to(&BqlType::Bool),
            None
        );
    }

    #[test]
    fn coerce_timestamp_int_roundtrip() {
        let ns = 1_700_000_000_000_000_000_i64;
        let ts = PropertyValue::Timestamp(ns);
        let as_int = ts.coerce_to(&BqlType::Int).unwrap();
        assert_eq!(as_int, PropertyValue::Int(ns));
        let back = as_int.coerce_to(&BqlType::Timestamp).unwrap();
        assert_eq!(back, PropertyValue::Timestamp(ns));
    }

    // ── Timestamp ↔ String casts (type-system.md §4.2) ────────────────────────

    #[test]
    fn coerce_timestamp_to_string_emits_rfc3339_utc() {
        // Whole seconds — AutoSi trims trailing zero fractional digits.
        assert_eq!(
            PropertyValue::Timestamp(1_700_000_000_000_000_000_i64).coerce_to(&BqlType::String),
            Some(PropertyValue::String("2023-11-14T22:13:20Z".into()))
        );
        // Epoch.
        assert_eq!(
            PropertyValue::Timestamp(0).coerce_to(&BqlType::String),
            Some(PropertyValue::String("1970-01-01T00:00:00Z".into()))
        );
        // Pre-epoch.
        assert_eq!(
            PropertyValue::Timestamp(-1_000_000_000).coerce_to(&BqlType::String),
            Some(PropertyValue::String("1969-12-31T23:59:59Z".into()))
        );
        // Nanosecond precision survives in the rendered form.
        assert_eq!(
            PropertyValue::Timestamp(1_700_000_000_123_456_789_i64).coerce_to(&BqlType::String),
            Some(PropertyValue::String(
                "2023-11-14T22:13:20.123456789Z".into()
            ))
        );
    }

    #[test]
    fn coerce_string_to_timestamp_parses_rfc3339() {
        assert_eq!(
            PropertyValue::String("2023-11-14T22:13:20Z".into()).coerce_to(&BqlType::Timestamp),
            Some(PropertyValue::Timestamp(1_700_000_000_000_000_000_i64))
        );
        // Nanosecond-precision fractional seconds.
        assert_eq!(
            PropertyValue::String("2023-11-14T22:13:20.123456789Z".into())
                .coerce_to(&BqlType::Timestamp),
            Some(PropertyValue::Timestamp(1_700_000_000_123_456_789_i64))
        );
        // Surrounding whitespace is trimmed, matching the sibling String→Int /
        // String→Float arms above.
        assert_eq!(
            PropertyValue::String("  1970-01-01T00:00:00Z  ".into()).coerce_to(&BqlType::Timestamp),
            Some(PropertyValue::Timestamp(0))
        );
    }

    #[test]
    fn coerce_string_to_timestamp_normalizes_non_utc_offset_to_utc() {
        // Midnight in +05:30 is 18:30 the previous day in UTC. Both forms
        // must resolve to the same UTC nanosecond instant.
        let utc =
            PropertyValue::String("2024-01-01T18:30:00Z".into()).coerce_to(&BqlType::Timestamp);
        let local = PropertyValue::String("2024-01-02T00:00:00+05:30".into())
            .coerce_to(&BqlType::Timestamp);
        assert_eq!(utc, local);
        assert!(matches!(utc, Some(PropertyValue::Timestamp(_))));
    }

    #[test]
    fn coerce_string_to_timestamp_returns_none_on_parse_failure() {
        for malformed in [
            "",
            "not a date",
            "2024-01-01",          // RFC 3339 requires the time component
            "2024-01-01 12:00:00", // space separator not accepted by RFC 3339 strict parser
            "garbage",
        ] {
            assert_eq!(
                PropertyValue::String(malformed.into()).coerce_to(&BqlType::Timestamp),
                None,
                "malformed input should return None: {malformed:?}"
            );
        }
    }

    #[test]
    fn coerce_timestamp_string_roundtrip_preserves_nanos() {
        // The chrono AutoSi formatter emits enough precision to round-trip.
        for ns in [
            0_i64,
            1_000_000_000,             // 1 second
            -1_000_000_000,            // 1 second before epoch
            1_700_000_000_000_000_000, // 2023-ish
            1_700_000_000_123_456_789, // with nanos
        ] {
            let as_str = PropertyValue::Timestamp(ns)
                .coerce_to(&BqlType::String)
                .expect("ts → string");
            let back = as_str.coerce_to(&BqlType::Timestamp).expect("string → ts");
            assert_eq!(
                back,
                PropertyValue::Timestamp(ns),
                "round trip failed for ns={ns}"
            );
        }
    }

    // ── equality ─────────────────────────────────────────────────────────────

    #[test]
    fn null_equals_null() {
        assert_eq!(PropertyValue::Null, PropertyValue::Null);
    }

    #[test]
    fn null_not_equal_to_non_null() {
        assert_ne!(PropertyValue::Null, PropertyValue::Int(0));
    }

    #[test]
    fn float_nan_equals_nan() {
        assert_eq!(
            PropertyValue::Float(f64::NAN),
            PropertyValue::Float(f64::NAN)
        );
    }

    // ── ordering ─────────────────────────────────────────────────────────────

    #[test]
    fn null_sorts_first() {
        assert!(PropertyValue::Null < PropertyValue::Bool(false));
        assert!(PropertyValue::Null < PropertyValue::Int(i64::MIN));
    }

    #[test]
    fn int_ordering() {
        assert!(PropertyValue::Int(-1) < PropertyValue::Int(0));
        assert!(PropertyValue::Int(0) < PropertyValue::Int(1));
    }

    #[test]
    fn cross_type_ordering() {
        assert!(PropertyValue::Bool(true) < PropertyValue::Int(0));
        assert!(PropertyValue::Int(i64::MAX) < PropertyValue::Float(0.0));
        assert!(PropertyValue::Float(f64::MAX) < PropertyValue::String("".into()));
    }

    #[test]
    fn list_ordering_lexicographic() {
        let a = PropertyValue::List(vec![PropertyValue::Int(1), PropertyValue::Int(2)]);
        let b = PropertyValue::List(vec![PropertyValue::Int(1), PropertyValue::Int(3)]);
        assert!(a < b);
    }

    // ── display ──────────────────────────────────────────────────────────────

    #[test]
    fn display_null() {
        assert_eq!(PropertyValue::Null.to_string(), "NULL");
    }

    #[test]
    fn display_bool() {
        assert_eq!(PropertyValue::Bool(true).to_string(), "true");
        assert_eq!(PropertyValue::Bool(false).to_string(), "false");
    }

    #[test]
    fn display_list() {
        let v = PropertyValue::List(vec![PropertyValue::Int(1), PropertyValue::Int(2)]);
        assert_eq!(v.to_string(), "[1, 2]");
    }

    #[test]
    fn display_map() {
        let v = PropertyValue::Map(vec![
            ("a".into(), PropertyValue::Int(1)),
            ("b".into(), PropertyValue::Int(2)),
        ]);
        assert_eq!(v.to_string(), "{a: 1, b: 2}");
    }

    // ── BqlType display ───────────────────────────────────────────────────────

    #[test]
    fn bql_type_display() {
        assert_eq!(BqlType::Bool.to_string(), "BOOL");
        assert_eq!(BqlType::Int.to_string(), "INT");
        assert_eq!(
            BqlType::List(Box::new(BqlType::String)).to_string(),
            "LIST(STRING)"
        );
        assert_eq!(
            BqlType::Map(Box::new(BqlType::Float)).to_string(),
            "MAP(FLOAT)"
        );
    }
}
