//! Aggregate function enumeration shared across planner and operator crates.
//!
//! `AggFunction` is the resolved, typed representation of an aggregate
//! function. The parser produces `AggItem { function: Name, ... }` (a
//! string); the planner resolves the string to an `AggFunction` variant
//! during type-checking and carries it through `FusableAggregate` and
//! `CompiledAgg` into the physical plan. The operator crate uses it to
//! select the right `AggState` variant inside `HashAccumulator`.
//!
//! Living in `bqlite-core` (no internal deps) keeps the enum importable
//! by both `bqlite-planner` and `bqlite-operators` without adding a
//! dependency edge.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::BqlType;

/// A resolved aggregate function.
///
/// The Wave 3 set is fixed by query-language.md §7.1. Percentile
/// accumulators (`P50`–`P99`) are listed here but their runtime
/// implementation (DDSketch) lands in TASK-327.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggFunction {
    /// `COUNT(*)` — counts all rows (ignores NULLs only when applied to
    /// a column, but the star form counts every row).
    Count,
    /// `COUNT(col)` — counts non-NULL values.
    CountColumn,
    /// `COUNT_DISTINCT(col)` — counts distinct non-NULL values.
    CountDistinct,
    /// `SUM(col)` — sum of non-NULL values. Accepts `Int` and `Float`.
    Sum,
    /// `MIN(col)` — minimum non-NULL value. Accepts `Int`, `Float`,
    /// `String`, `Timestamp`.
    Min,
    /// `MAX(col)` — maximum non-NULL value. Accepts `Int`, `Float`,
    /// `String`, `Timestamp`.
    Max,
    /// `AVG(col)` — average of non-NULL values. Accepts `Int` and
    /// `Float`. Always returns `Float`.
    Avg,
    /// `P50(col)` — 50th percentile via DDSketch (TASK-327).
    P50,
    /// `P90(col)` — 90th percentile via DDSketch (TASK-327).
    P90,
    /// `P95(col)` — 95th percentile via DDSketch (TASK-327).
    P95,
    /// `P99(col)` — 99th percentile via DDSketch (TASK-327).
    P99,
    // NOTE: `VARIANCE` / `STDDEV` are tracked in `AggState::Variance`
    // (execution-model.md §9.5) but are not yet exposed as BQL surface
    // functions. They will be added to this enum when the language
    // spec (query-language.md §7.1) includes them in a future wave.
}

impl AggFunction {
    /// Returns the output [`BqlType`] for this function given the input type.
    ///
    /// Returns `None` if the input type is not accepted (e.g. `SUM(String)`).
    /// See type-system.md §6.4 for the full type-rule table.
    ///
    /// All aggregate outputs are nullable except `COUNT(*)`, `COUNT(col)`,
    /// and `COUNT_DISTINCT(col)`, which always produce a non-null `Int`.
    /// Callers use [`output_nullable`](Self::output_nullable) to determine
    /// nullability.
    pub fn output_type(&self, input_type: Option<&BqlType>) -> Option<BqlType> {
        match self {
            // COUNT(*) — no input column, always Int.
            AggFunction::Count => Some(BqlType::Int),
            // COUNT(col) — any input type, always Int. Requires an argument.
            AggFunction::CountColumn => {
                input_type?;
                Some(BqlType::Int)
            }
            // COUNT_DISTINCT(col) — any input type, always Int. Requires an argument.
            AggFunction::CountDistinct => {
                input_type?;
                Some(BqlType::Int)
            }
            // SUM — preserves Int, preserves Float. Rejects others.
            AggFunction::Sum => match input_type? {
                BqlType::Int => Some(BqlType::Int),
                BqlType::Float => Some(BqlType::Float),
                _ => None,
            },
            // AVG — always Float regardless of input.
            AggFunction::Avg => match input_type? {
                BqlType::Int | BqlType::Float => Some(BqlType::Float),
                _ => None,
            },
            // MIN / MAX — preserves input type. Accepts Int, Float,
            // String, Timestamp.
            AggFunction::Min | AggFunction::Max => match input_type? {
                BqlType::Int => Some(BqlType::Int),
                BqlType::Float => Some(BqlType::Float),
                BqlType::String => Some(BqlType::String),
                BqlType::Timestamp => Some(BqlType::Timestamp),
                _ => None,
            },
            // Percentiles — always Float regardless of input.
            AggFunction::P50 | AggFunction::P90 | AggFunction::P95 | AggFunction::P99 => {
                match input_type? {
                    BqlType::Int | BqlType::Float => Some(BqlType::Float),
                    _ => None,
                }
            }
        }
    }

    /// Whether this function's output is nullable.
    ///
    /// COUNT variants always produce a non-null `Int`. All other
    /// aggregates are nullable (NULL when all inputs are NULL or the
    /// group is empty for non-count aggregates).
    pub fn output_nullable(&self) -> bool {
        !matches!(
            self,
            AggFunction::Count | AggFunction::CountColumn | AggFunction::CountDistinct
        )
    }

    /// Whether this function requires a column argument.
    ///
    /// `COUNT(*)` takes no argument; all others require exactly one.
    pub fn requires_argument(&self) -> bool {
        !matches!(self, AggFunction::Count)
    }

    /// Whether this function is incrementally computable (can be updated
    /// one entity/match/session at a time without seeing all input at
    /// once).
    ///
    /// All Wave 3 aggregates are incremental — this is a design
    /// invariant (see execution-model.md §8.4). This method exists so
    /// the fusion eligibility check in the planner can call it without
    /// maintaining a separate list.
    pub fn is_incremental(&self) -> bool {
        // All Wave 3 aggregates are incremental by design.
        true
    }
}

impl fmt::Display for AggFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggFunction::Count => write!(f, "COUNT(*)"),
            AggFunction::CountColumn => write!(f, "COUNT"),
            AggFunction::CountDistinct => write!(f, "COUNT_DISTINCT"),
            AggFunction::Sum => write!(f, "SUM"),
            AggFunction::Min => write!(f, "MIN"),
            AggFunction::Max => write!(f, "MAX"),
            AggFunction::Avg => write!(f, "AVG"),
            AggFunction::P50 => write!(f, "P50"),
            AggFunction::P90 => write!(f, "P90"),
            AggFunction::P95 => write!(f, "P95"),
            AggFunction::P99 => write!(f, "P99"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_star_needs_no_argument() {
        assert!(!AggFunction::Count.requires_argument());
    }

    #[test]
    fn all_others_need_argument() {
        for f in [
            AggFunction::CountColumn,
            AggFunction::CountDistinct,
            AggFunction::Sum,
            AggFunction::Min,
            AggFunction::Max,
            AggFunction::Avg,
            AggFunction::P50,
            AggFunction::P90,
            AggFunction::P95,
            AggFunction::P99,
        ] {
            assert!(f.requires_argument(), "{f} should require an argument");
        }
    }

    #[test]
    fn count_variants_are_non_nullable() {
        assert!(!AggFunction::Count.output_nullable());
        assert!(!AggFunction::CountColumn.output_nullable());
        assert!(!AggFunction::CountDistinct.output_nullable());
    }

    #[test]
    fn non_count_aggregates_are_nullable() {
        for f in [
            AggFunction::Sum,
            AggFunction::Min,
            AggFunction::Max,
            AggFunction::Avg,
            AggFunction::P50,
        ] {
            assert!(f.output_nullable(), "{f} should be nullable");
        }
    }

    #[test]
    fn sum_output_type_preserves_input() {
        assert_eq!(
            AggFunction::Sum.output_type(Some(&BqlType::Int)),
            Some(BqlType::Int)
        );
        assert_eq!(
            AggFunction::Sum.output_type(Some(&BqlType::Float)),
            Some(BqlType::Float)
        );
        assert_eq!(AggFunction::Sum.output_type(Some(&BqlType::String)), None);
    }

    #[test]
    fn avg_always_returns_float() {
        assert_eq!(
            AggFunction::Avg.output_type(Some(&BqlType::Int)),
            Some(BqlType::Float)
        );
        assert_eq!(
            AggFunction::Avg.output_type(Some(&BqlType::Float)),
            Some(BqlType::Float)
        );
    }

    #[test]
    fn min_max_accept_comparable_types() {
        for f in [AggFunction::Min, AggFunction::Max] {
            assert!(f.output_type(Some(&BqlType::Int)).is_some());
            assert!(f.output_type(Some(&BqlType::Float)).is_some());
            assert!(f.output_type(Some(&BqlType::String)).is_some());
            assert!(f.output_type(Some(&BqlType::Timestamp)).is_some());
            assert!(f.output_type(Some(&BqlType::Bool)).is_none());
        }
    }

    #[test]
    fn count_star_output_type_ignores_input() {
        assert_eq!(AggFunction::Count.output_type(None), Some(BqlType::Int));
    }

    #[test]
    fn count_column_rejects_none_input() {
        assert_eq!(AggFunction::CountColumn.output_type(None), None);
    }

    #[test]
    fn count_distinct_rejects_none_input() {
        assert_eq!(AggFunction::CountDistinct.output_type(None), None);
    }

    #[test]
    fn count_column_accepts_any_type() {
        for t in [
            BqlType::Bool,
            BqlType::Int,
            BqlType::Float,
            BqlType::String,
            BqlType::Timestamp,
        ] {
            assert_eq!(
                AggFunction::CountColumn.output_type(Some(&t)),
                Some(BqlType::Int)
            );
        }
    }

    #[test]
    fn percentile_accepts_numeric_only() {
        for f in [
            AggFunction::P50,
            AggFunction::P90,
            AggFunction::P95,
            AggFunction::P99,
        ] {
            assert!(f.output_type(Some(&BqlType::Int)).is_some());
            assert!(f.output_type(Some(&BqlType::Float)).is_some());
            assert!(f.output_type(Some(&BqlType::String)).is_none());
        }
    }

    #[test]
    fn all_functions_are_incremental() {
        for f in [
            AggFunction::Count,
            AggFunction::CountColumn,
            AggFunction::CountDistinct,
            AggFunction::Sum,
            AggFunction::Min,
            AggFunction::Max,
            AggFunction::Avg,
            AggFunction::P50,
            AggFunction::P90,
            AggFunction::P95,
            AggFunction::P99,
        ] {
            assert!(f.is_incremental(), "{f} should be incremental");
        }
    }

    #[test]
    fn display_formatting() {
        assert_eq!(AggFunction::Count.to_string(), "COUNT(*)");
        assert_eq!(AggFunction::Sum.to_string(), "SUM");
        assert_eq!(AggFunction::CountDistinct.to_string(), "COUNT_DISTINCT");
    }
}
