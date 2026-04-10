//! Expression AST — the syntactic shape of BQL expressions.
//!
//! Expressions here are **untyped**: the parser produces them directly
//! from the grammar without consulting a schema. The planner later
//! resolves them against the catalog and wraps them in a separate
//! `TypedExpr` (see docs/design/planner-pipeline.md §4.6). Keeping the
//! AST free of type metadata is deliberate — it is what lets us reuse
//! these same nodes from the Rust and Python builder APIs before any
//! schema is known.
//!
//! See docs/design/query-language.md §21 (lines 1243–1339) for the
//! expression language surface and §26 grammar for the concrete
//! productions each variant corresponds to.

use bqlite_core::BqlType;
use serde::{Deserialize, Serialize};

use crate::pipeline::Pipeline;
use crate::span::{Name, Span};

/// A BQL expression node.
///
/// `Expr` is the union of every syntactic expression form BQL accepts.
/// Variants are deliberately coarse: the parser only classifies the
/// expression by its surface shape — the planner is responsible for
/// further type resolution.
///
/// Every variant that the parser can emit is expected to carry an
/// enclosing `Spanned<Expr>` wrapper (see [`Spanned`]). Nested
/// sub-expressions are boxed so the enum stays a fixed size.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// A literal constant — see [`Literal`].
    Literal(Literal),

    /// An unqualified column reference: `user_id`, `amount`.
    Column(Name),

    /// A qualified column reference: `users.name`, `a.b`.
    ///
    /// The parser emits this whenever a dot-qualified identifier is
    /// seen; the planner resolves `table` against the source list.
    Qualified { table: Name, column: Name },

    /// A pattern-bound variable reference: `$product`.
    ///
    /// Variable scoping is enforced by the planner, not the parser —
    /// the AST simply records that the user wrote a `$`-prefixed name.
    Variable(Name),

    /// A scalar or window function call: `upper(name)`, `rank() over (…)`.
    ///
    /// `over` is `Some` only when the parser sees an `OVER` clause;
    /// presence of `over` turns the call into a window function.
    FunctionCall {
        name: Name,
        args: Vec<Spanned<Expr>>,
        over: Option<OverClause>,
    },

    /// A binary arithmetic operation: `a + b`, `x * 2`.
    Binary {
        op: BinaryOp,
        left: Box<Spanned<Expr>>,
        right: Box<Spanned<Expr>>,
    },

    /// A unary operation: `-x`, `+y`.
    Unary {
        op: UnaryOp,
        operand: Box<Spanned<Expr>>,
    },

    /// A comparison: `a = b`, `x < 10`, `a != b`.
    Compare {
        op: CompareOp,
        left: Box<Spanned<Expr>>,
        right: Box<Spanned<Expr>>,
    },

    /// Logical `AND` of two or more operands.
    ///
    /// The parser is free to flatten chains of `AND` into a single
    /// `And(Vec<_>)` for easier inspection by the planner.
    And(Vec<Spanned<Expr>>),

    /// Logical `OR` of two or more operands. See [`Expr::And`].
    Or(Vec<Spanned<Expr>>),

    /// Logical `NOT`.
    Not(Box<Spanned<Expr>>),

    /// `expr IS NULL` (when `negated` is false) or `expr IS NOT NULL`.
    IsNull {
        expr: Box<Spanned<Expr>>,
        negated: bool,
    },

    /// `expr BETWEEN low AND high` (when `negated` is false) or
    /// `expr NOT BETWEEN low AND high`.
    Between {
        expr: Box<Spanned<Expr>>,
        low: Box<Spanned<Expr>>,
        high: Box<Spanned<Expr>>,
        negated: bool,
    },

    /// `expr LIKE 'pattern'` (or `NOT LIKE`).
    ///
    /// The pattern is stored as the literal string from the source —
    /// interpretation of `%` / `_` lives in the planner.
    Like {
        expr: Box<Spanned<Expr>>,
        pattern: String,
        negated: bool,
    },

    /// `expr ~= 'regex'`. Regex compilation is deferred to plan time.
    Regex {
        expr: Box<Spanned<Expr>>,
        pattern: String,
    },

    /// `expr CONTAINS 'substring'` — substring membership
    /// (query-language.md §9.1 line 689, grammar §26 line 1594).
    ///
    /// Grammar has no `NOT CONTAINS` form — unlike `LIKE`, `IN`, and
    /// `BETWEEN`, CONTAINS has no negation modifier. `WHERE NOT (tag
    /// CONTAINS 'premium')` is spelled as a wrapping `Expr::Not`.
    Contains {
        expr: Box<Spanned<Expr>>,
        pattern: String,
    },

    /// `expr IN (…)` or `expr NOT IN (…)` — set membership.
    ///
    /// `lhs` is a 1-tuple for single-column predicates and may be a
    /// multi-element `Vec` for compound-key predicates
    /// (`(a, b) IN QUERY (…)`) — see query-language.md §17.4.
    In {
        lhs: Vec<Spanned<Expr>>,
        rhs: InRhs,
        negated: bool,
    },

    /// `CASE WHEN ... THEN ... [ELSE ...] END`.
    Case {
        arms: Vec<CaseArm>,
        else_expr: Option<Box<Spanned<Expr>>>,
    },

    /// `CAST(expr AS TYPE)` — explicit type conversion
    /// (query-language.md §21.7, grammar §26 line 1609).
    ///
    /// BQL has a single `CAST` form — failed casts produce NULL
    /// (type-system.md §4.2, "TRY_CAST semantics by default"), so
    /// there is no separate `TRY_CAST` keyword. Target types reuse
    /// [`BqlType`] from `bqlite-core` rather than redefining a parallel
    /// type enum.
    Cast {
        expr: Box<Spanned<Expr>>,
        target: BqlType,
    },

    /// Parenthesized sub-expression.
    ///
    /// Preserved in the AST rather than erased so explain-plan output
    /// can reproduce the user's original text verbatim.
    Paren(Box<Spanned<Expr>>),
}

/// An expression paired with its source span.
///
/// Spans live on this wrapper instead of on every `Expr` variant so
/// we do not pay per-variant storage cost; every place the grammar
/// produces an expression is expected to wrap it in a `Spanned`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Spanned<T> {
    pub node: T,
    pub span: Span,
}

impl<T> Spanned<T> {
    pub fn new(node: T, span: Span) -> Self {
        Self { node, span }
    }
}

/// A literal constant value in source text.
///
/// The parser converts duration literals (`7d`, `30m`) to i64 nanoseconds
/// at lex time. Duration literals are stored as [`Literal::Duration`]
/// rather than [`Literal::Int`] so the planner can distinguish the
/// user's intent — even though both have `BqlType::Int`, durations carry
/// different semantics when used as window bounds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    /// SQL `NULL`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// Integer literal: `42`, `-3`, `0x1F`.
    Int(i64),
    /// Floating-point literal: `3.14`, `1e6`.
    Float(f64),
    /// String literal: `'hello'`, `"quoted"`.
    String(String),
    /// Duration literal in nanoseconds: `7d` → `Duration(604800 * 1_000_000_000)`.
    ///
    /// The lexer recognizes units (`ns`, `us`/`μs`, `ms`, `s`, `m`, `h`,
    /// `d`, `w`) and converts them to nanoseconds at token production
    /// time.
    Duration(i64),
    /// Timestamp literal: `@2024-01-01T00:00:00Z` → epoch nanoseconds.
    Timestamp(i64),
}

/// Arithmetic binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

/// Unary prefix operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    /// Arithmetic negation: `-x`.
    Negate,
    /// Arithmetic identity: `+x`. Preserved for source fidelity.
    Plus,
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompareOp {
    /// `=` — SQL equality (NULL-unsafe).
    Equal,
    /// `!=` or `<>`.
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

/// The right-hand side of an `IN` / `NOT IN` expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InRhs {
    /// Inline list of values: `IN (1, 2, 3)`.
    List(Vec<Spanned<Expr>>),
    /// `IN QUERY (<pipeline>)` — set membership against a subquery.
    ///
    /// Pipelines are boxed to break the mutual recursion between
    /// `Expr` and `Pipeline` (a pipeline's WHERE clause may contain an
    /// `IN QUERY` referencing another pipeline).
    Query(Box<Pipeline>),
    /// `IN <alias>` — set membership against a named intermediate
    /// result declared elsewhere in the statement (see
    /// query-language.md §22 Aliases).
    Alias(Name),
}

/// A `WHEN condition THEN value` arm of a `CASE` expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaseArm {
    pub condition: Spanned<Expr>,
    pub value: Spanned<Expr>,
    pub span: Span,
}

/// An `OVER (...)` clause attached to a window-function call.
///
/// `partition_by` and `order_by` are both optional at parse time — an
/// empty `OVER ()` is permitted and places the window function over
/// the entire input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OverClause {
    pub partition_by: Vec<Spanned<Expr>>,
    pub order_by: Vec<OrderItem>,
    pub span: Span,
}

/// A single `ORDER BY` term — an expression and a direction.
///
/// Shared by `OVER (ORDER BY ...)` and the top-level `ORDER BY`
/// pipeline operator, so it lives in `expr` rather than `operator` to
/// avoid a circular dependency.
///
/// BQL v1 has no `NULLS FIRST` / `NULLS LAST` clause — NULL ordering
/// is fixed by convention (query-language.md §15): ASC sorts NULLs
/// last, DESC sorts NULLs first. The grammar does not accept a nulls
/// clause (§26 line 1573: `order_item := expr (ASC | DESC)?`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderItem {
    pub expr: Spanned<Expr>,
    pub direction: SortDir,
    pub span: Span,
}

/// Ascending or descending sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SortDir {
    Asc,
    Desc,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit_int(n: i64) -> Spanned<Expr> {
        Spanned::new(Expr::Literal(Literal::Int(n)), Span::EMPTY)
    }

    #[test]
    fn construct_binary_expr() {
        // 1 + 2
        let e = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(lit_int(1)),
            right: Box::new(lit_int(2)),
        };
        match e {
            Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::Add),
            _ => panic!("expected Binary"),
        }
    }

    #[test]
    fn literal_variants_distinct() {
        assert_ne!(Literal::Int(0), Literal::Float(0.0));
        assert_ne!(Literal::Int(1), Literal::Bool(true));
        assert_ne!(Literal::Null, Literal::String(String::new()));
    }

    #[test]
    fn duration_and_int_are_distinct_literals() {
        // Even though both encode to i64 nanoseconds, the AST keeps
        // them separate so the planner can detect duration usage.
        assert_ne!(
            Literal::Duration(1_000_000_000),
            Literal::Int(1_000_000_000)
        );
    }

    #[test]
    fn column_ref_is_case_sensitive() {
        let a = Expr::Column(Name::synthetic("User_ID"));
        let b = Expr::Column(Name::synthetic("user_id"));
        assert_ne!(a, b);
    }

    #[test]
    fn cast_target_uses_bql_type() {
        let e = Expr::Cast {
            expr: Box::new(lit_int(1)),
            target: BqlType::Float,
        };
        match e {
            Expr::Cast { target, .. } => assert_eq!(target, BqlType::Float),
            _ => panic!("expected Cast"),
        }
    }

    #[test]
    fn case_expression_arms() {
        let case = Expr::Case {
            arms: vec![CaseArm {
                condition: Spanned::new(Expr::Literal(Literal::Bool(true)), Span::EMPTY),
                value: lit_int(1),
                span: Span::EMPTY,
            }],
            else_expr: Some(Box::new(lit_int(0))),
        };
        match case {
            Expr::Case { arms, else_expr } => {
                assert_eq!(arms.len(), 1);
                assert!(else_expr.is_some());
            }
            _ => panic!("expected Case"),
        }
    }

    #[test]
    fn contains_expression_variant() {
        let e = Expr::Contains {
            expr: Box::new(Spanned::new(
                Expr::Column(Name::synthetic("tag")),
                Span::EMPTY,
            )),
            pattern: "premium".into(),
        };
        match e {
            Expr::Contains { pattern, .. } => assert_eq!(pattern, "premium"),
            _ => panic!("expected Contains"),
        }
    }

    #[test]
    fn in_rhs_list_variant() {
        let rhs = InRhs::List(vec![lit_int(1), lit_int(2)]);
        let e = Expr::In {
            lhs: vec![Spanned::new(
                Expr::Column(Name::synthetic("x")),
                Span::EMPTY,
            )],
            rhs,
            negated: false,
        };
        match e {
            Expr::In { negated, .. } => assert!(!negated),
            _ => panic!("expected In"),
        }
    }

    #[test]
    fn serde_round_trip_simple_expr() {
        // Smoke test that the derived Serde impls cover every variant
        // used here — catches any accidental #[serde(skip)] regressions.
        let e = Spanned::new(
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(lit_int(1)),
                right: Box::new(lit_int(2)),
            },
            Span::EMPTY,
        );
        let json = serde_json::to_string(&e).unwrap();
        let back: Spanned<Expr> = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }
}
