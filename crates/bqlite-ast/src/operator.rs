//! Pipeline operator (pipe stage) AST nodes.
//!
//! Each variant of [`PipelineStage`] corresponds to one pipe stage in
//! BQL source text: `| WHERE …`, `| SELECT …`, `| MATCH …`, etc.
//! Stages are stored in an ordered `Vec<PipelineStage>` on [`crate::pipeline::Pipeline`];
//! the planner walks the vector to build a tree during logical-plan
//! lowering (docs/design/planner-pipeline.md §3.3).
//!
//! The AST preserves FUNNEL and RETENTION as distinct stages even
//! though the planner desugars them to `MATCH + STATS` — keeping the
//! user's original shape makes error messages reference the right
//! source text (query-language.md §6, §6.3).

use serde::{Deserialize, Serialize};

use crate::expr::{Expr, Literal, OrderItem, Spanned};
use crate::pattern::{BracketSpec, EventRef, MatchPattern, Step};
use crate::span::{Name, Span};

/// One stage in a BQL pipeline. See module docs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PipelineStage {
    /// `| WHERE <predicate>` — row filter (query-language.md §9).
    Where {
        predicate: Spanned<Expr>,
        span: Span,
    },

    /// `| SELECT [DISTINCT] <items>` — projection
    /// (query-language.md §10).
    Select {
        distinct: bool,
        items: Vec<SelectItem>,
        span: Span,
    },

    /// `| LET <name> = <expr>` — introduce a named expression reusable
    /// in later stages (query-language.md §11).
    Let {
        name: Name,
        expr: Spanned<Expr>,
        span: Span,
    },

    /// `| MATCH <pattern>` — sequence match (query-language.md §4).
    Match { pattern: MatchPattern, span: Span },

    /// `| FUNNEL steps: […] within: <duration>` — funnel sugar form
    /// (query-language.md §6). Desugared to MATCH + STATS by the
    /// planner.
    Funnel(Funnel),

    /// `| RETENTION entry: <e>, activity: <e>, brackets: […]` —
    /// retention sugar form (query-language.md §6.3). Desugared by
    /// the planner.
    Retention(Retention),

    /// `| SESSIONIZE gap: <d> [end: <e>]` — session assignment
    /// (query-language.md §8).
    Sessionize(Sessionize),

    /// `| STATS <aggregates> [BY <group>]` — aggregation with optional
    /// grouping (query-language.md §7).
    Stats {
        aggregates: Vec<AggItem>,
        group_by: Vec<GroupItem>,
        span: Span,
    },

    /// `| ORDER BY <items>` — total ordering (query-language.md §13).
    OrderBy { items: Vec<OrderItem>, span: Span },

    /// `| LIMIT <count>` — row cap (query-language.md §13).
    Limit { count: u64, span: Span },

    /// `| PIVOT <pivot_column> ON <value_column> [IN (values)]` —
    /// tall-to-wide reshape (query-language.md §16; grammar §26 line
    /// 1579 `pivot_op := PIVOT name ON name (IN "(" literal_list ")")?`).
    Pivot {
        /// The column whose distinct values become new column names.
        /// Spelled as the first token after `PIVOT`.
        pivot_column: Name,
        /// The measure column filled into each new column. Spelled
        /// as the token after the `ON` keyword.
        value_column: Name,
        /// Optional explicit list of pivot values: `IN (1, 2, 3)`.
        /// Reserved by the grammar — in v1 the list is optional and
        /// pivot values are inferred from the upstream operator
        /// (e.g. BRACKETS produces a fixed count). §26.1 line 1671.
        values: Option<Vec<Literal>>,
        span: Span,
    },

    /// `| FIRST | LAST | NTH(<n>) <event> [WHERE <predicate>]` —
    /// per-entity event sub-selection (query-language.md §14.1).
    EventSelect(EventSelect),

    /// `| SAMPLE <spec>` — random row sampling (query-language.md §14.3).
    Sample(Sample),

    /// `| ATTRIBUTE conversion: <e> touchpoints: <e> window: <d>` —
    /// attribution operator (query-language.md §14.4).
    Attribute(Attribute),
}

/// A single projection item in `| SELECT …`.
///
/// Bare column references and star expansions have no alias and use
/// the column's own name for output. Computed expressions require an
/// explicit `AS alias` — the parser rejects bare expressions without
/// an alias (query-language.md §10). At the AST level the alias is
/// always `Option<Name>`: the parser enforces the "required when
/// computed" rule and the planner trusts the AST shape it receives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectItem {
    pub kind: SelectItemKind,
    pub alias: Option<Name>,
    pub span: Span,
}

/// The shape of a `SELECT` item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SelectItemKind {
    /// `*` — all source columns. Mutually exclusive with other items
    /// at the planner level but the AST does not enforce this.
    Wildcard,
    /// `table.*` — wildcard qualified by a joined-source name.
    QualifiedWildcard(Name),
    /// Any expression, including bare columns, function calls, and
    /// arithmetic. The parser demands `AS alias` when the expression
    /// is anything other than a column reference.
    Expr(Spanned<Expr>),
}

/// A `GROUP BY` term in `| STATS … BY …`.
///
/// Like [`SelectItem`], the term can be any expression with an
/// optional alias; the parser requires the alias when the term is not
/// a bare column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupItem {
    pub expr: Spanned<Expr>,
    pub alias: Option<Name>,
    pub span: Span,
}

/// A single aggregate item inside `| STATS …`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggItem {
    /// Aggregate function identifier — keywords like `count`, `sum`,
    /// `avg`, `min`, `max`, or any extension function name. Stored as
    /// a `Name` rather than an enum so future aggregates can be added
    /// without changing the AST.
    pub function: Name,
    /// Aggregate arguments — most built-ins take exactly one expression
    /// (or zero for `count(*)`). `Vec` accommodates multi-argument
    /// extensions like `quantile(x, 0.95)`.
    pub args: Vec<Spanned<Expr>>,
    /// `DISTINCT` modifier — `count(DISTINCT user_id)`.
    pub distinct: bool,
    /// Output column name — required by the parser
    /// (query-language.md §7.1), so stored as a non-optional `Name`.
    pub alias: Name,
    pub span: Span,
}

/// The `| FUNNEL` sugar form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Funnel {
    /// Funnel steps — same shape as MATCH steps since FUNNEL is a
    /// MATCH sugar form.
    pub steps: Vec<Step>,
    /// `within: <duration>` — completion window in nanoseconds.
    pub window: Option<i64>,
    pub span: Span,
}

/// The `| RETENTION` sugar form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Retention {
    /// `entry: <event>` — the entry (cohort-defining) event.
    pub entry: EventRef,
    /// `activity: <event>` — the event that counts as "retained".
    pub activity: EventRef,
    /// `brackets: [...]` — retention time slices.
    pub brackets: BracketSpec,
    pub span: Span,
}

/// The `| SESSIONIZE` stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sessionize {
    /// `gap: <duration>` — inactivity gap in nanoseconds that ends a
    /// session.
    pub gap: i64,
    /// `end: <event>` — optional event that forcibly ends a session.
    pub end: Option<EventRef>,
    pub span: Span,
}

/// The `| FIRST`, `| LAST`, or `| NTH` stage — per-entity event
/// sub-selection (query-language.md §14.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventSelect {
    pub kind: EventSelectKind,
    /// Event type(s) the selector applies to.
    pub event: EventRef,
    /// Optional predicate scoping which events qualify.
    pub predicate: Option<Spanned<Expr>>,
    pub span: Span,
}

/// Which per-entity position to select.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventSelectKind {
    First,
    Last,
    /// `NTH(n)` — 1-indexed position of the target event for each entity.
    Nth(u64),
}

/// The `| SAMPLE` stage — random row sampling (query-language.md §14.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub spec: SampleSpec,
    /// Optional explicit RNG seed for reproducible sampling.
    pub seed: Option<i64>,
    pub span: Span,
}

/// What to sample.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SampleSpec {
    /// `FRACTION 0.1` — keep roughly this fraction of rows in `[0.0, 1.0]`.
    Fraction(f64),
    /// `COUNT 1000` — keep at most this many rows.
    Count(u64),
}

/// The `| ATTRIBUTE` stage — attribution (query-language.md §14.3).
///
/// Four mandatory parameters, matching the grammar at §26 line
/// 1566–1569: `conversion`, `touchpoints`, `window`, `touchpoint_key`.
/// The v1 language deliberately omits a `model:` parameter — credit
/// distribution (first-touch, last-touch, linear, time-decay,
/// positional) is expressed by follow-on window functions and
/// aggregates on the flat `(entity, conversion, touchpoint)` rows
/// emitted by this operator (§14.3, paragraph beginning "Why one key
/// column, not a list").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    /// The conversion event being attributed.
    pub conversion: EventRef,
    /// The touchpoint event type credited to conversions.
    pub touchpoints: EventRef,
    /// Lookback window in nanoseconds.
    pub window: i64,
    /// Expression evaluated against each touchpoint event to produce
    /// the `touchpoint_key` output column. Required — the grammar has
    /// no default (§14.3: "Use `CAST(… AS STRING)` if the source
    /// column isn't already a string"). The expression cannot
    /// reference conversion-event properties; the planner enforces
    /// this rule.
    pub touchpoint_key: Spanned<Expr>,
    pub span: Span,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Literal;

    fn lit_true() -> Spanned<Expr> {
        Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY)
    }

    fn column(name: &str) -> Spanned<Expr> {
        Spanned::new(Expr::Column(Name::synthetic(name)), Span::EMPTY)
    }

    #[test]
    fn where_stage_carries_predicate() {
        let stage = PipelineStage::Where {
            predicate: lit_true(),
            span: Span::EMPTY,
        };
        match stage {
            PipelineStage::Where { .. } => (),
            _ => panic!("expected Where"),
        }
    }

    #[test]
    fn select_items_expr_and_wildcard() {
        let items = [
            SelectItem {
                kind: SelectItemKind::Wildcard,
                alias: None,
                span: Span::EMPTY,
            },
            SelectItem {
                kind: SelectItemKind::Expr(column("user_id")),
                alias: None,
                span: Span::EMPTY,
            },
            SelectItem {
                kind: SelectItemKind::Expr(column("amount")),
                alias: Some(Name::synthetic("total")),
                span: Span::EMPTY,
            },
        ];
        assert_eq!(items.len(), 3);
        // Verify the alias carries through on the computed item.
        assert_eq!(items[2].alias.as_ref().unwrap().text, "total");
    }

    #[test]
    fn stats_with_group_by() {
        let stage = PipelineStage::Stats {
            aggregates: vec![AggItem {
                function: Name::synthetic("count"),
                args: vec![],
                distinct: false,
                alias: Name::synthetic("n"),
                span: Span::EMPTY,
            }],
            group_by: vec![GroupItem {
                expr: column("country"),
                alias: None,
                span: Span::EMPTY,
            }],
            span: Span::EMPTY,
        };
        match stage {
            PipelineStage::Stats {
                aggregates,
                group_by,
                ..
            } => {
                assert_eq!(aggregates.len(), 1);
                assert_eq!(group_by.len(), 1);
            }
            _ => panic!("expected Stats"),
        }
    }

    #[test]
    fn limit_stage_count() {
        let stage = PipelineStage::Limit {
            count: 100,
            span: Span::EMPTY,
        };
        match stage {
            PipelineStage::Limit { count, .. } => assert_eq!(count, 100),
            _ => panic!("expected Limit"),
        }
    }

    #[test]
    fn event_select_nth_carries_position() {
        let sel = EventSelect {
            kind: EventSelectKind::Nth(3),
            event: EventRef {
                table: None,
                event: Name::synthetic("click"),
                span: Span::EMPTY,
            },
            predicate: None,
            span: Span::EMPTY,
        };
        assert_eq!(sel.kind, EventSelectKind::Nth(3));
    }

    #[test]
    fn sample_fraction_vs_count_distinct() {
        let f = SampleSpec::Fraction(0.1);
        let c = SampleSpec::Count(100);
        // Smoke test that both variants can be constructed and matched.
        match f {
            SampleSpec::Fraction(v) => assert_eq!(v, 0.1),
            _ => panic!("expected Fraction"),
        }
        match c {
            SampleSpec::Count(n) => assert_eq!(n, 100),
            _ => panic!("expected Count"),
        }
    }

    #[test]
    fn attribute_stage_requires_touchpoint_key() {
        // `touchpoint_key` is non-optional — §14.3 line 881 says it is
        // required and has no default. A constructed Attribute always
        // carries a real expression.
        let attr = Attribute {
            conversion: EventRef {
                table: None,
                event: Name::synthetic("purchase"),
                span: Span::EMPTY,
            },
            touchpoints: EventRef {
                table: None,
                event: Name::synthetic("ad_click"),
                span: Span::EMPTY,
            },
            window: 7 * 86_400_000_000_000,
            touchpoint_key: column("channel"),
            span: Span::EMPTY,
        };
        assert_eq!(attr.conversion.event.text, "purchase");
        assert_eq!(attr.touchpoints.event.text, "ad_click");
    }

    #[test]
    fn pivot_stage_has_optional_values_list() {
        let without_values = PipelineStage::Pivot {
            pivot_column: Name::synthetic("bracket"),
            value_column: Name::synthetic("retention"),
            values: None,
            span: Span::EMPTY,
        };
        let with_values = PipelineStage::Pivot {
            pivot_column: Name::synthetic("country"),
            value_column: Name::synthetic("signups"),
            values: Some(vec![
                Literal::String("US".into()),
                Literal::String("UK".into()),
            ]),
            span: Span::EMPTY,
        };
        match without_values {
            PipelineStage::Pivot { values, .. } => assert!(values.is_none()),
            _ => panic!("expected Pivot"),
        }
        match with_values {
            PipelineStage::Pivot { values, .. } => {
                assert_eq!(values.as_ref().unwrap().len(), 2);
            }
            _ => panic!("expected Pivot"),
        }
    }
}
