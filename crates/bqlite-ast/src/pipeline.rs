//! Pipeline AST — the top-level structure of a BQL query.
//!
//! A BQL query is a `Source` followed by a flat list of pipe operators:
//!
//! ```text
//! events WHERE x > 0 | SELECT user_id, ts | LIMIT 100
//! ```
//!
//! The parser produces a [`Pipeline`] containing an ordered
//! `Vec<PipelineStage>`. The planner converts the flat list into a
//! tree during logical-plan lowering
//! (docs/design/planner-pipeline.md §3.3, §4.2).

use serde::{Deserialize, Serialize};

use crate::operator::PipelineStage;
use crate::span::{Name, Span};

/// A BQL pipeline — a source and an ordered list of pipe operators.
///
/// `Pipeline` is what a `Statement::Query` wraps. Pipelines are also
/// reachable from inside expressions via `Expr::In { rhs: InRhs::Query(_), .. }`,
/// which creates a mutual recursion with [`crate::expr::Expr`]. That
/// recursion is broken by boxing pipelines inside `InRhs::Query`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    /// The data source — one or more joined tables, plus optional
    /// time-range filter (query-language.md §3).
    pub source: Source,
    /// Pipe operators applied in order after the source.
    pub stages: Vec<PipelineStage>,
    pub span: Span,
}

/// The source (FROM-like) clause of a pipeline.
///
/// BQL does not use the word FROM — the pipeline starts with a bare
/// table name or a joined table list. Sources may optionally carry a
/// time-range predicate applied before any subsequent pipe stage, per
/// query-language.md §16.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    /// The primary source table.
    pub primary: TableRef,
    /// Additional tables joined by `JOIN`. Empty for single-table
    /// queries. Join semantics are entity-keyed — the planner validates
    /// that all joined tables share an entity-id column.
    pub joins: Vec<TableRef>,
    /// Optional time range applied to the source — `LAST 7d` or
    /// `BETWEEN 'ts1' AND 'ts2'`.
    pub time_range: Option<TimeRange>,
    pub span: Span,
}

/// A reference to a source table.
///
/// BQL v1 does not support table aliases in the source expression —
/// the grammar is `source := name time_range? (JOIN name)*` (§26
/// line 1492) and §17 line 1107 explicitly rules out self-joins
/// because "v1 does not introduce table aliases for the source
/// expression". Qualified column references (`orders.total`) use the
/// raw table name as the qualifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    /// The declared table name from the catalog.
    pub name: Name,
    pub span: Span,
}

/// A `LAST <duration>` or `BETWEEN <ts> AND <ts>` time-range filter
/// applied to a source.
///
/// The grammar (§26 line 1492) accepts exactly these two forms — no
/// `BEFORE` / `AFTER` shortcuts. Users needing one-sided ranges spell
/// them as a WHERE predicate on the timestamp column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeRange {
    /// `LAST <duration>` — relative to the query's evaluation time.
    /// Duration is stored in nanoseconds.
    Last(i64),
    /// `BETWEEN <start> AND <end>` — inclusive absolute range. Values
    /// are stored as raw source strings and parsed into timestamps by
    /// the planner so that timezone and format errors surface with
    /// proper source spans.
    Between { start: String, end: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::PipelineStage;

    fn table(name: &str) -> TableRef {
        TableRef {
            name: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    #[test]
    fn minimal_pipeline_construction() {
        let p = Pipeline {
            source: Source {
                primary: table("events"),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![],
            span: Span::EMPTY,
        };
        assert_eq!(p.source.primary.name.text, "events");
        assert!(p.stages.is_empty());
    }

    #[test]
    fn source_with_time_range_last() {
        let s = Source {
            primary: table("events"),
            joins: vec![],
            time_range: Some(TimeRange::Last(7 * 86_400_000_000_000)),
            span: Span::EMPTY,
        };
        match s.time_range {
            Some(TimeRange::Last(ns)) => assert_eq!(ns, 7 * 86_400_000_000_000),
            _ => panic!("expected Last"),
        }
    }

    #[test]
    fn source_with_between_time_range() {
        let s = Source {
            primary: table("events"),
            joins: vec![],
            time_range: Some(TimeRange::Between {
                start: "2024-01-01T00:00:00Z".into(),
                end: "2024-02-01T00:00:00Z".into(),
            }),
            span: Span::EMPTY,
        };
        assert!(s.time_range.is_some());
    }

    #[test]
    fn pipeline_with_limit_stage() {
        let p = Pipeline {
            source: Source {
                primary: table("events"),
                joins: vec![],
                time_range: None,
                span: Span::EMPTY,
            },
            stages: vec![PipelineStage::Limit {
                count: 100,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        assert_eq!(p.stages.len(), 1);
    }

    #[test]
    fn source_joins_allows_multiple_tables() {
        let s = Source {
            primary: table("a"),
            joins: vec![table("b"), table("c")],
            time_range: None,
            span: Span::EMPTY,
        };
        assert_eq!(s.joins.len(), 2);
    }
}
