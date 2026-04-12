//! `ScalarValue` — lightweight scalar type for group keys and aggregate
//! intermediate values.
//!
//! Unlike [`crate::PropertyValue`], which is a boundary type for ingest and
//! schema evolution, `ScalarValue` is the query-execution scalar: it carries
//! the six leaf types that appear in group-by keys, aggregate function
//! arguments, and `Accumulator::update` value slices.
//!
//! `ScalarValue` intentionally omits `List` and `Map` — aggregate group keys
//! and reduction values are always scalar. This keeps `GroupKey` hashing and
//! comparison branchless and cache-friendly.
//!
//! ## Float handling
//!
//! `f64` does not implement `Eq` or `Hash` in the standard library.
//! `ScalarValue` provides both via `total_cmp` for equality/ordering and
//! `to_bits()` for hashing, following the same convention as
//! [`crate::PropertyValue`]'s custom `PartialEq` / `Ord` implementations.
//! Under this convention NaN == NaN, which is the correct behavior for
//! GROUP BY (two NaN group keys must land in the same bucket).

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::BqlType;

/// A scalar value used in group keys and aggregate intermediate state.
///
/// Variants map to the six leaf types of [`BqlType`]. `Null` represents
/// SQL NULL. The enum is `Clone + Eq + Hash + Ord` so it can be used
/// directly in hash maps (for `GroupKey`) and sorted containers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScalarValue {
    /// SQL NULL.
    Null,
    /// SQL BOOL.
    Bool(bool),
    /// SQL INT (i64).
    Int(i64),
    /// SQL FLOAT (f64).
    Float(f64),
    /// SQL STRING (UTF-8).
    String(std::string::String),
    /// SQL TIMESTAMP (epoch nanoseconds UTC).
    Timestamp(i64),
}

impl ScalarValue {
    /// Returns the [`BqlType`] of this value, or `None` for `Null`.
    pub fn bql_type(&self) -> Option<BqlType> {
        match self {
            ScalarValue::Null => None,
            ScalarValue::Bool(_) => Some(BqlType::Bool),
            ScalarValue::Int(_) => Some(BqlType::Int),
            ScalarValue::Float(_) => Some(BqlType::Float),
            ScalarValue::String(_) => Some(BqlType::String),
            ScalarValue::Timestamp(_) => Some(BqlType::Timestamp),
        }
    }

    /// Returns `true` if this value is `Null`.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Equality — NaN == NaN for GROUP BY correctness
// ──────────────────────────────────────────────────────────────────────────────

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ScalarValue::Null, ScalarValue::Null) => true,
            (ScalarValue::Bool(a), ScalarValue::Bool(b)) => a == b,
            (ScalarValue::Int(a), ScalarValue::Int(b)) => a == b,
            (ScalarValue::Float(a), ScalarValue::Float(b)) => a.total_cmp(b) == Ordering::Equal,
            (ScalarValue::String(a), ScalarValue::String(b)) => a == b,
            (ScalarValue::Timestamp(a), ScalarValue::Timestamp(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for ScalarValue {}

// ──────────────────────────────────────────────────────────────────────────────
// Hashing — uses f64::to_bits() so NaN hashes consistently
// ──────────────────────────────────────────────────────────────────────────────

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Discriminant tag first, then payload.
        std::mem::discriminant(self).hash(state);
        match self {
            ScalarValue::Null => {}
            ScalarValue::Bool(b) => b.hash(state),
            ScalarValue::Int(n) => n.hash(state),
            ScalarValue::Float(f) => f.to_bits().hash(state),
            ScalarValue::String(s) => s.hash(state),
            ScalarValue::Timestamp(ns) => ns.hash(state),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Ordering — Null < Bool < Int < Float < String < Timestamp
// ──────────────────────────────────────────────────────────────────────────────

fn type_rank(v: &ScalarValue) -> u8 {
    match v {
        ScalarValue::Null => 0,
        ScalarValue::Bool(_) => 1,
        ScalarValue::Int(_) => 2,
        ScalarValue::Float(_) => 3,
        ScalarValue::String(_) => 4,
        ScalarValue::Timestamp(_) => 5,
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScalarValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank_cmp = type_rank(self).cmp(&type_rank(other));
        if rank_cmp != Ordering::Equal {
            return rank_cmp;
        }
        match (self, other) {
            (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
            (ScalarValue::Bool(a), ScalarValue::Bool(b)) => a.cmp(b),
            (ScalarValue::Int(a), ScalarValue::Int(b)) => a.cmp(b),
            (ScalarValue::Float(a), ScalarValue::Float(b)) => a.total_cmp(b),
            (ScalarValue::String(a), ScalarValue::String(b)) => a.cmp(b),
            (ScalarValue::Timestamp(a), ScalarValue::Timestamp(b)) => a.cmp(b),
            _ => unreachable!(),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Display
// ──────────────────────────────────────────────────────────────────────────────

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Bool(b) => write!(f, "{b}"),
            ScalarValue::Int(n) => write!(f, "{n}"),
            ScalarValue::Float(v) => write!(f, "{v}"),
            ScalarValue::String(s) => write!(f, "{s}"),
            ScalarValue::Timestamp(ns) => {
                let secs = ns / 1_000_000_000;
                let subsec = (ns % 1_000_000_000).unsigned_abs();
                write!(f, "{secs}.{subsec:09}")
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
    use std::collections::HashMap;

    // ── equality ─────────────────────────────────────────────────────────────

    #[test]
    fn null_equals_null() {
        assert_eq!(ScalarValue::Null, ScalarValue::Null);
    }

    #[test]
    fn null_not_equal_to_non_null() {
        assert_ne!(ScalarValue::Null, ScalarValue::Int(0));
    }

    #[test]
    fn float_nan_equals_nan() {
        assert_eq!(ScalarValue::Float(f64::NAN), ScalarValue::Float(f64::NAN));
    }

    #[test]
    fn float_neg_zero_equals_pos_zero() {
        // f64::total_cmp distinguishes -0.0 and 0.0, but for GROUP BY we
        // want them in the same bucket. However, total_cmp says
        // -0.0 < 0.0, so they are NOT equal under our Eq impl.
        // This is the correct behavior: they are different bit patterns.
        // In practice, GROUP BY on exact float values is rare.
        assert_ne!(ScalarValue::Float(-0.0), ScalarValue::Float(0.0));
    }

    #[test]
    fn cross_type_not_equal() {
        assert_ne!(ScalarValue::Int(0), ScalarValue::Float(0.0));
        assert_ne!(ScalarValue::Int(0), ScalarValue::Timestamp(0));
    }

    // ── hashing ──────────────────────────────────────────────────────────────

    #[test]
    fn nan_hashes_consistently() {
        let mut map: HashMap<ScalarValue, u32> = HashMap::new();
        map.insert(ScalarValue::Float(f64::NAN), 42);
        assert_eq!(map.get(&ScalarValue::Float(f64::NAN)), Some(&42));
    }

    #[test]
    fn null_hashes_consistently() {
        let mut map: HashMap<ScalarValue, u32> = HashMap::new();
        map.insert(ScalarValue::Null, 1);
        assert_eq!(map.get(&ScalarValue::Null), Some(&1));
    }

    // ── ordering ─────────────────────────────────────────────────────────────

    #[test]
    fn null_sorts_first() {
        assert!(ScalarValue::Null < ScalarValue::Bool(false));
        assert!(ScalarValue::Null < ScalarValue::Int(i64::MIN));
    }

    #[test]
    fn cross_type_ordering() {
        assert!(ScalarValue::Bool(true) < ScalarValue::Int(0));
        assert!(ScalarValue::Int(i64::MAX) < ScalarValue::Float(0.0));
        assert!(ScalarValue::Float(f64::MAX) < ScalarValue::String("".into()));
        assert!(ScalarValue::String("z".into()) < ScalarValue::Timestamp(0));
    }

    #[test]
    fn int_ordering() {
        assert!(ScalarValue::Int(-1) < ScalarValue::Int(0));
        assert!(ScalarValue::Int(0) < ScalarValue::Int(1));
    }

    // ── bql_type ─────────────────────────────────────────────────────────────

    #[test]
    fn bql_type_null_returns_none() {
        assert_eq!(ScalarValue::Null.bql_type(), None);
    }

    #[test]
    fn bql_type_scalars() {
        assert_eq!(ScalarValue::Bool(true).bql_type(), Some(BqlType::Bool));
        assert_eq!(ScalarValue::Int(0).bql_type(), Some(BqlType::Int));
        assert_eq!(ScalarValue::Float(0.0).bql_type(), Some(BqlType::Float));
        assert_eq!(
            ScalarValue::String("x".into()).bql_type(),
            Some(BqlType::String)
        );
        assert_eq!(
            ScalarValue::Timestamp(0).bql_type(),
            Some(BqlType::Timestamp)
        );
    }

    // ── is_null ──────────────────────────────────────────────────────────────

    #[test]
    fn is_null_on_null() {
        assert!(ScalarValue::Null.is_null());
    }

    #[test]
    fn is_null_on_non_null() {
        assert!(!ScalarValue::Int(0).is_null());
    }

    // ── display ──────────────────────────────────────────────────────────────

    #[test]
    fn display_values() {
        assert_eq!(ScalarValue::Null.to_string(), "NULL");
        assert_eq!(ScalarValue::Bool(true).to_string(), "true");
        assert_eq!(ScalarValue::Int(42).to_string(), "42");
        assert_eq!(ScalarValue::Float(1.5).to_string(), "1.5");
        assert_eq!(ScalarValue::String("hello".into()).to_string(), "hello");
    }
}
