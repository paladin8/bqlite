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

    /// `| STATS <aggregates> [GROUP BY <group>]` — aggregation with
    /// optional grouping (query-language.md §7). The two-keyword form
    /// `GROUP BY` is required; bare `BY` is a parse error (§7.2).
    Stats {
        aggregates: Vec<AggItem>,
        group_by: Vec<GroupItem>,
        span: Span,
    },

    /// `| ORDER BY <items>` or `| SORT <items>` — total ordering
    /// (query-language.md §15). `SORT` is a parser-level alias; both
    /// forms produce this variant.
    OrderBy { items: Vec<OrderItem>, span: Span },

    /// `| LIMIT <count>` — row cap (query-language.md §15).
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

    /// `| SAMPLE(fraction: …)` — deterministic entity sampling
    /// (query-language.md §14.2).
    Sample(Sample),

    /// `| ATTRIBUTE conversion: <e> touchpoints: <e> window: <d>` —
    /// attribution operator (query-language.md §14.4).
    Attribute(Attribute),
}

impl PipelineStage {
    /// The source span covering this stage from its leading keyword
    /// through the last token the stage owns.
    ///
    /// Provided as a single entry point so consumers (parser, planner,
    /// diagnostics) do not have to pattern-match on every variant when
    /// they only need the span. Adding a new variant without extending
    /// this match is a compile error, which keeps the helper honest.
    pub fn span(&self) -> Span {
        match self {
            Self::Where { span, .. } => *span,
            Self::Select { span, .. } => *span,
            Self::Let { span, .. } => *span,
            Self::Match { span, .. } => *span,
            Self::Funnel(f) => f.span,
            Self::Retention(r) => r.span,
            Self::Sessionize(s) => s.span,
            Self::Stats { span, .. } => *span,
            Self::OrderBy { span, .. } => *span,
            Self::Limit { span, .. } => *span,
            Self::Pivot { span, .. } => *span,
            Self::EventSelect(e) => e.span,
            Self::Sample(s) => s.span,
            Self::Attribute(a) => a.span,
        }
    }
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
    /// Reserved for future use. The parser always sets this to `false`
    /// because `COUNT(DISTINCT col)` is a parse error per §7.1 —
    /// the correct distinct-count form is `COUNT_DISTINCT(col)`.
    /// Builder APIs may set this field; the planner validates it.
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
    /// `end: <event_list>` — optional event type(s) that forcibly end a
    /// session. Accepts either a single event ref or a parenthesised list
    /// `(e1, e2, …)`. The `Vec` always has length ≥ 1 when `Some`.
    /// Duplicate names within the list are rejected at parse time
    /// (sessionize.md §5.4).
    pub end: Option<Vec<EventRef>>,
    pub span: Span,
}

/// The `| FIRST`, `| LAST`, or `| NTH` stage — per-entity event
/// sub-selection (query-language.md §14.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventSelect {
    pub kind: EventSelectKind,
    /// Event type(s) the selector applies to. One or more `EventRef`s;
    /// the single-arg form `FIRST(purchase)` produces a one-element vec and
    /// the parenthesized list form `FIRST((login, sso_login))` produces a
    /// longer one. Duplicate names are rejected at parse time.
    pub events: Vec<EventRef>,
    /// Optional predicate scoping which events qualify (the `WHERE` clause).
    /// Applied per-event before position selection.
    pub predicate: Option<Spanned<Expr>>,
    /// Scan-range backward extension in nanoseconds, from `lookback: <dur>`.
    /// Only `FIRST` and `NTH` accept this parameter; the parser rejects it
    /// on `LAST` (query-language.md §14.1).
    pub lookback: Option<i64>,
    pub span: Span,
}

/// Which per-entity position to select.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventSelectKind {
    First,
    Last,
    /// `NTH(n)` — 1-indexed, >= 1. The parser rejects `n == 0`. Using `u32`
    /// matches the physical descriptor shape in `event-select-sample.md §4`
    /// and bounds the max position to ~4 billion events, well beyond any
    /// practical per-entity event count.
    Nth(u32),
}

/// The `| SAMPLE` stage — deterministic entity-level sampling
/// (query-language.md §14.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Fraction of entities to include, in `[0.0, 1.0]` (inclusive both
    /// ends). Values outside this range are rejected at parse time.
    pub fraction: f64,
    /// Optional explicit RNG seed for reproducible sampling. Without it the
    /// seed is derived from the database identity (query-language.md §30.9).
    pub seed: Option<i64>,
    pub span: Span,
}

/// The `| ATTRIBUTE` stage — attribution (query-language.md §14.3).
///
/// Four mandatory parameters, matching the grammar at §26 line
/// 1638–1642: `conversion`, `touchpoints`, `window`, `touchpoint_key`.
/// The v1 language deliberately omits a `model:` parameter — credit
/// distribution (first-touch, last-touch, linear, time-decay,
/// positional) is expressed by follow-on window functions and
/// aggregates on the flat `(entity, conversion, touchpoint)` rows
/// emitted by this operator (§14.3, paragraph beginning "Why one key
/// column, not a list").
///
/// Both `conversion` and `touchpoints` accept either a single event
/// reference or a parenthesised comma-separated list (attribute.md §3,
/// "List extension"). The `Vec` always has length ≥ 1 when the AST
/// is produced by the parser.  Duplicate names within each list are
/// rejected at parse time (TASK-422). Overlap between the two lists
/// is permitted — the emit-before-add rule (attribute.md §6) handles
/// the overlap at runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Attribute {
    /// The conversion event type(s) that trigger attribution emission.
    /// Single event ref or parenthesised list; length ≥ 1.
    pub conversion: Vec<EventRef>,
    /// The touchpoint event type(s) eligible for attribution credit.
    /// Single event ref or parenthesised list; length ≥ 1.
    pub touchpoints: Vec<EventRef>,
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
    fn pipeline_stage_span_helper_returns_variant_span() {
        // Smoke-test the inherent `span()` method for a representative
        // mix of variants — the exhaustive match makes adding a new
        // variant without a span accessor a compile error.
        let named_span = Span::new(10, 20, 3, 5);
        let where_stage = PipelineStage::Where {
            predicate: lit_true(),
            span: named_span,
        };
        assert_eq!(where_stage.span(), named_span);

        let select_stage = PipelineStage::Select {
            distinct: false,
            items: vec![],
            span: named_span,
        };
        assert_eq!(select_stage.span(), named_span);

        let limit_stage = PipelineStage::Limit {
            count: 1,
            span: named_span,
        };
        assert_eq!(limit_stage.span(), named_span);

        let attr_stage = PipelineStage::Attribute(Attribute {
            conversion: vec![EventRef {
                table: None,
                event: Name::synthetic("conv"),
                span: Span::EMPTY,
            }],
            touchpoints: vec![EventRef {
                table: None,
                event: Name::synthetic("tp"),
                span: Span::EMPTY,
            }],
            window: 0,
            touchpoint_key: column("k"),
            span: named_span,
        });
        assert_eq!(attr_stage.span(), named_span);
    }

    #[test]
    fn event_select_nth_carries_position() {
        let sel = EventSelect {
            kind: EventSelectKind::Nth(3),
            events: vec![EventRef {
                table: None,
                event: Name::synthetic("click"),
                span: Span::EMPTY,
            }],
            predicate: None,
            lookback: None,
            span: Span::EMPTY,
        };
        assert_eq!(sel.kind, EventSelectKind::Nth(3));
        assert_eq!(sel.events.len(), 1);
        assert_eq!(sel.events[0].event.text, "click");
    }

    #[test]
    fn event_select_multi_event_list() {
        // FIRST((login, sso_login)) — two-element event list.
        let sel = EventSelect {
            kind: EventSelectKind::First,
            events: vec![
                EventRef {
                    table: None,
                    event: Name::synthetic("login"),
                    span: Span::EMPTY,
                },
                EventRef {
                    table: None,
                    event: Name::synthetic("sso_login"),
                    span: Span::EMPTY,
                },
            ],
            predicate: None,
            lookback: None,
            span: Span::EMPTY,
        };
        assert_eq!(sel.events.len(), 2);
        assert_eq!(sel.events[0].event.text, "login");
        assert_eq!(sel.events[1].event.text, "sso_login");
    }

    #[test]
    fn event_select_lookback_optional() {
        // FIRST only — lookback is Some; LAST — lookback is None.
        let with_lb = EventSelect {
            kind: EventSelectKind::First,
            events: vec![EventRef {
                table: None,
                event: Name::synthetic("signup"),
                span: Span::EMPTY,
            }],
            predicate: None,
            lookback: Some(90 * 86_400_000_000_000_i64),
            span: Span::EMPTY,
        };
        assert_eq!(with_lb.lookback, Some(90 * 86_400_000_000_000_i64));

        let without_lb = EventSelect {
            kind: EventSelectKind::Last,
            events: vec![EventRef {
                table: None,
                event: Name::synthetic("page_view"),
                span: Span::EMPTY,
            }],
            predicate: None,
            lookback: None,
            span: Span::EMPTY,
        };
        assert_eq!(without_lb.lookback, None);
    }

    #[test]
    fn sample_fraction_only() {
        // `count:` is removed; only `fraction:` is valid in v1.
        let s = Sample {
            fraction: 0.1,
            seed: None,
            span: Span::EMPTY,
        };
        assert_eq!(s.fraction, 0.1);
        assert_eq!(s.seed, None);

        let s_with_seed = Sample {
            fraction: 0.5,
            seed: Some(42),
            span: Span::EMPTY,
        };
        assert_eq!(s_with_seed.seed, Some(42));
    }

    #[test]
    fn attribute_stage_requires_touchpoint_key() {
        // `touchpoint_key` is non-optional — §14.3 line 881 says it is
        // required and has no default. A constructed Attribute always
        // carries a real expression.
        //
        // Both `conversion` and `touchpoints` are now `Vec<EventRef>`,
        // accepting a single event ref or a parenthesised list
        // (attribute.md §3, "List extension", TASK-422).
        let attr = Attribute {
            conversion: vec![EventRef {
                table: None,
                event: Name::synthetic("purchase"),
                span: Span::EMPTY,
            }],
            touchpoints: vec![EventRef {
                table: None,
                event: Name::synthetic("ad_click"),
                span: Span::EMPTY,
            }],
            window: 7 * 86_400_000_000_000,
            touchpoint_key: column("channel"),
            span: Span::EMPTY,
        };
        assert_eq!(attr.conversion[0].event.text, "purchase");
        assert_eq!(attr.touchpoints[0].event.text, "ad_click");
    }

    #[test]
    fn attribute_accepts_multi_event_lists() {
        // Both `conversion:` and `touchpoints:` accept a list of ≥ 1 event refs.
        let attr = Attribute {
            conversion: vec![
                EventRef {
                    table: None,
                    event: Name::synthetic("purchase"),
                    span: Span::EMPTY,
                },
                EventRef {
                    table: None,
                    event: Name::synthetic("subscription"),
                    span: Span::EMPTY,
                },
            ],
            touchpoints: vec![
                EventRef {
                    table: None,
                    event: Name::synthetic("ad_click"),
                    span: Span::EMPTY,
                },
                EventRef {
                    table: None,
                    event: Name::synthetic("email_open"),
                    span: Span::EMPTY,
                },
            ],
            window: 30 * 86_400_000_000_000,
            touchpoint_key: column("channel"),
            span: Span::EMPTY,
        };
        assert_eq!(attr.conversion.len(), 2);
        assert_eq!(attr.touchpoints.len(), 2);
        assert_eq!(attr.conversion[0].event.text, "purchase");
        assert_eq!(attr.conversion[1].event.text, "subscription");
        assert_eq!(attr.touchpoints[0].event.text, "ad_click");
        assert_eq!(attr.touchpoints[1].event.text, "email_open");
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
