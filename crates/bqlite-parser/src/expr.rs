//! Expression grammar.
//!
//! Design: `docs/design/language/grammar-framework.md` §7.3 and
//! `docs/design/query-language.md` §26 `expr` subgrammar.
//!
//! Wave 2 CP2 lands the expression subset pinned by TASK-220:
//! literals (int, float, string, bool, null, duration), column
//! references, qualified column references (`table.col`), variable
//! references (`$var`), parenthesized sub-expressions, prefix unary
//! minus and `NOT`, binary arithmetic (`+ - * / %`), comparisons
//! (`= != <> < <= > >=`), `IS NULL` / `IS NOT NULL`, and boolean
//! `AND` / `OR` (flattened into `Expr::And` / `Expr::Or` vectors).
//!
//! Deferred to later Wave 2 / Wave 3 productions:
//!
//! - Function calls (`UPPER(x)`, `COUNT(*)`, `LAG(..) OVER (..)`)
//! - `CAST(expr AS type)` and `CASE` expressions
//! - `BETWEEN`, `LIKE`, `~=`, `CONTAINS`, `IN`, `NOT IN`
//! - Timestamp literals (`@2024-…`)
//!
//! # Implementation
//!
//! The grammar is implemented as straight recursive descent with one
//! function per precedence level, matching §26's `or_expr → and_expr →
//! not_expr → comparison → addition → multiplication → unary → primary`
//! ladder. This is operationally equivalent to Pratt precedence climbing
//! with statically-fixed binding powers; both produce the same parse
//! trees for BQL's grammar and descent is less opaque when stepped
//! through in a debugger. Each level is a `&mut Parser` method taking
//! no precedence argument — the call graph encodes the precedence.
//!
//! Associativity:
//!
//! - `OR` and `AND` are left-associative at the source level but stored
//!   as flattened `Vec` nodes (`Expr::And` / `Expr::Or`) so the planner
//!   can rewrite without rebalancing an associativity tree.
//! - `+ - * / %` are left-associative and produce nested `Expr::Binary`
//!   trees.
//! - Comparisons are **non-associative** per §26 line 1588 — exactly
//!   zero or one comparison operator is allowed between two additions.
//!   Chained comparisons like `a = b = c` are rejected at parse time.
//! - Unary `-` and `NOT` are right-associative.

// Every function in this module is `pub(crate)` and called only from
// TASK-223's pipeline grammar (WHERE predicate expressions), which has
// not landed yet. The module-level allow prevents `dead_code` warnings
// from tripping `clippy -D warnings` until the consumer lands.
#![allow(dead_code)]

use bqlite_ast::{BinaryOp, CompareOp, Expr, Literal, Name, Spanned, UnaryOp};

use crate::error::{Expected, NameRole, ParseError};
use crate::lex::{token_span, Keyword, TokenKind};
use crate::parser::Parser;

/// Parse one BQL expression and return it wrapped in its source span.
///
/// Consumes tokens up to (but not including) the first token that is
/// not part of the expression. Callers are responsible for consuming
/// the terminator (e.g. `)`, `;`, another keyword).
pub(crate) fn parse_expression(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    parse_or(p)
}

fn parse_or(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let mut lhs = parse_and(p)?;
    while p.try_kw(Keyword::Or).is_some() {
        let rhs = parse_and(p)?;
        let span = lhs.span.merged(rhs.span);
        lhs = match lhs.node {
            Expr::Or(mut items) => {
                items.push(rhs);
                Spanned::new(Expr::Or(items), span)
            }
            _ => Spanned::new(Expr::Or(vec![lhs, rhs]), span),
        };
    }
    Ok(lhs)
}

fn parse_and(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let mut lhs = parse_not(p)?;
    while p.try_kw(Keyword::And).is_some() {
        let rhs = parse_not(p)?;
        let span = lhs.span.merged(rhs.span);
        lhs = match lhs.node {
            Expr::And(mut items) => {
                items.push(rhs);
                Spanned::new(Expr::And(items), span)
            }
            _ => Spanned::new(Expr::And(vec![lhs, rhs]), span),
        };
    }
    Ok(lhs)
}

fn parse_not(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    if let Some(tok) = p.try_kw(Keyword::Not) {
        let start_span = token_span(&tok);
        // NOT is right-associative and chains: `NOT NOT x` is legal.
        // The inner parse goes through `parse_not` so `NOT NOT x`
        // produces a nested `Expr::Not` tree.
        let inner = parse_not(p)?;
        let span = start_span.merged(inner.span);
        return Ok(Spanned::new(Expr::Not(Box::new(inner)), span));
    }
    parse_comparison(p)
}

fn parse_comparison(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let lhs = parse_addition(p)?;

    // IS NULL / IS NOT NULL — handled inside `comparison` so `a = b IS
    // NULL` is a parse error (non-associative with other comparisons).
    // Note: `NULL` is tokenized as the literal `TokenKind::Null`, not
    // as a keyword, so we match the token kind directly.
    if p.try_kw(Keyword::Is).is_some() {
        let negated = p.try_kw(Keyword::Not).is_some();
        if !matches!(p.peek_kind(), TokenKind::Null) {
            return Err(p.error_unexpected(
                Expected::Keyword("NULL"),
                Some("`IS` and `IS NOT` must be followed by NULL"),
            ));
        }
        let null_tok = p.bump();
        let span = lhs.span.merged(token_span(&null_tok));
        return Ok(Spanned::new(
            Expr::IsNull {
                expr: Box::new(lhs),
                negated,
            },
            span,
        ));
    }

    // Optional single comparison operator.
    let op = match p.peek_kind() {
        TokenKind::Eq => Some(CompareOp::Equal),
        TokenKind::NotEq => Some(CompareOp::NotEqual),
        TokenKind::Lt => Some(CompareOp::Less),
        TokenKind::LtEq => Some(CompareOp::LessOrEqual),
        TokenKind::Gt => Some(CompareOp::Greater),
        TokenKind::GtEq => Some(CompareOp::GreaterOrEqual),
        _ => None,
    };
    let Some(op) = op else {
        return Ok(lhs);
    };
    p.bump();
    let rhs = parse_addition(p)?;
    let span = lhs.span.merged(rhs.span);
    Ok(Spanned::new(
        Expr::Compare {
            op,
            left: Box::new(lhs),
            right: Box::new(rhs),
        },
        span,
    ))
}

fn parse_addition(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let mut lhs = parse_multiplication(p)?;
    loop {
        let op = match p.peek_kind() {
            TokenKind::Plus => BinaryOp::Add,
            TokenKind::Minus => BinaryOp::Subtract,
            _ => break,
        };
        p.bump();
        let rhs = parse_multiplication(p)?;
        let span = lhs.span.merged(rhs.span);
        lhs = Spanned::new(
            Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            },
            span,
        );
    }
    Ok(lhs)
}

fn parse_multiplication(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let mut lhs = parse_unary(p)?;
    loop {
        let op = match p.peek_kind() {
            TokenKind::Star => BinaryOp::Multiply,
            TokenKind::Slash => BinaryOp::Divide,
            TokenKind::Percent => BinaryOp::Modulo,
            _ => break,
        };
        p.bump();
        let rhs = parse_unary(p)?;
        let span = lhs.span.merged(rhs.span);
        lhs = Spanned::new(
            Expr::Binary {
                op,
                left: Box::new(lhs),
                right: Box::new(rhs),
            },
            span,
        );
    }
    Ok(lhs)
}

fn parse_unary(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    // §26 defines unary as `"-"? primary` — only minus, not plus.
    if let Some(minus) = p.try_kind(&TokenKind::Minus) {
        let start = token_span(&minus);
        let operand = parse_unary(p)?;
        let span = start.merged(operand.span);
        return Ok(Spanned::new(
            Expr::Unary {
                op: UnaryOp::Negate,
                operand: Box::new(operand),
            },
            span,
        ));
    }
    parse_primary(p)
}

fn parse_primary(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let tok = p.peek().clone();
    let span = token_span(&tok);
    match &tok.kind {
        TokenKind::Int(v) => {
            let v = *v;
            p.bump();
            Ok(Spanned::new(Expr::Literal(Literal::Int(v)), span))
        }
        TokenKind::Number(v) => {
            let v = *v;
            p.bump();
            Ok(Spanned::new(Expr::Literal(Literal::Float(v)), span))
        }
        TokenKind::String(s) => {
            let s = s.clone();
            p.bump();
            Ok(Spanned::new(Expr::Literal(Literal::String(s)), span))
        }
        TokenKind::Bool(b) => {
            let b = *b;
            p.bump();
            Ok(Spanned::new(Expr::Literal(Literal::Bool(b)), span))
        }
        TokenKind::Null => {
            p.bump();
            Ok(Spanned::new(Expr::Literal(Literal::Null), span))
        }
        TokenKind::Duration(ns) => {
            let ns = *ns;
            p.bump();
            Ok(Spanned::new(Expr::Literal(Literal::Duration(ns)), span))
        }
        TokenKind::Variable(name) => {
            let name = name.clone();
            p.bump();
            Ok(Spanned::new(Expr::Variable(Name::new(name, span)), span))
        }
        TokenKind::Ident(_) | TokenKind::QuotedName(_) => parse_name_or_qualified(p),
        TokenKind::LParen => {
            p.bump();
            let inner = parse_expression(p)?;
            let rparen = p.expect_punct(&TokenKind::RParen, ")")?;
            let full_span = span.merged(token_span(&rparen));
            Ok(Spanned::new(Expr::Paren(Box::new(inner)), full_span))
        }
        _ => Err(p.error_unexpected(Expected::Expression, None)),
    }
}

/// Parse a bare or qualified name — `col` or `table.col`.
///
/// The first token is already known to be an `Ident` or `QuotedName`,
/// so `expect_name` cannot fail except via `ReservedKeyword` for a
/// bare keyword, which the caller's `peek` has already ruled out.
fn parse_name_or_qualified(p: &mut Parser) -> Result<Spanned<Expr>, ParseError> {
    let first = p.expect_name(NameRole::ColumnName)?;
    let first_span = first.span;

    // `.` here means a qualified column reference `table.col`. The
    // column portion may also be a backtick-quoted name.
    if p.try_kind(&TokenKind::Dot).is_some() {
        let column = p.expect_name(NameRole::ColumnName)?;
        let full_span = first_span.merged(column.span);
        return Ok(Spanned::new(
            Expr::Qualified {
                table: first,
                column,
            },
            full_span,
        ));
    }

    Ok(Spanned::new(Expr::Column(first), first_span))
}

/// Helper for exhaustive-match tests — parses one expression and
/// requires the input to be fully consumed.
#[cfg(test)]
fn expr_of(source: &str) -> Result<Spanned<Expr>, ParseError> {
    let mut p = Parser::new(source)?;
    let expr = parse_expression(&mut p)?;
    p.expect_eof()?;
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit_int(e: &Spanned<Expr>) -> i64 {
        match e.node {
            Expr::Literal(Literal::Int(v)) => v,
            _ => panic!("expected Int literal, got {:?}", e.node),
        }
    }

    fn parse_ok(source: &str) -> Spanned<Expr> {
        expr_of(source).unwrap_or_else(|e| panic!("parse failed for `{source}`: {e:?}"))
    }

    // ------------------------------------------------------------------
    // Literals
    // ------------------------------------------------------------------

    #[test]
    fn parses_int_literal() {
        assert_eq!(lit_int(&parse_ok("42")), 42);
    }

    #[test]
    fn parses_float_literal() {
        let e = parse_ok("2.5");
        match e.node {
            Expr::Literal(Literal::Float(v)) => assert_eq!(v, 2.5),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn parses_string_literal() {
        let e = parse_ok("'hello'");
        match e.node {
            Expr::Literal(Literal::String(s)) => assert_eq!(s, "hello"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn parses_string_with_doubled_quote() {
        let e = parse_ok("'it''s'");
        match e.node {
            Expr::Literal(Literal::String(s)) => assert_eq!(s, "it's"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn parses_bool_literals() {
        assert!(matches!(
            parse_ok("true").node,
            Expr::Literal(Literal::Bool(true))
        ));
        assert!(matches!(
            parse_ok("FALSE").node,
            Expr::Literal(Literal::Bool(false))
        ));
    }

    #[test]
    fn parses_null_literal() {
        assert!(matches!(
            parse_ok("NULL").node,
            Expr::Literal(Literal::Null)
        ));
    }

    #[test]
    fn parses_duration_literals() {
        let cases = &[
            ("7d", 7 * 86_400 * 1_000_000_000_i64),
            ("500ms", 500 * 1_000_000),
            ("30m", 30 * 60 * 1_000_000_000),
        ];
        for (src, nanos) in cases {
            match parse_ok(src).node {
                Expr::Literal(Literal::Duration(ns)) => assert_eq!(ns, *nanos, "for {src}"),
                other => panic!("expected Duration for {src}, got {other:?}"),
            }
        }
    }

    // ------------------------------------------------------------------
    // Names: column references, qualified, variables
    // ------------------------------------------------------------------

    #[test]
    fn parses_bare_column_reference() {
        let e = parse_ok("amount");
        match e.node {
            Expr::Column(name) => assert_eq!(name.text, "amount"),
            other => panic!("expected Column, got {other:?}"),
        }
    }

    #[test]
    fn parses_qualified_column_reference() {
        let e = parse_ok("purchases.amount");
        match e.node {
            Expr::Qualified { table, column } => {
                assert_eq!(table.text, "purchases");
                assert_eq!(column.text, "amount");
            }
            other => panic!("expected Qualified, got {other:?}"),
        }
    }

    #[test]
    fn parses_qualified_reference_with_backtick_sides() {
        let e = parse_ok("`weird table`.`weird col`");
        match e.node {
            Expr::Qualified { table, column } => {
                assert_eq!(table.text, "weird table");
                assert_eq!(column.text, "weird col");
            }
            other => panic!("expected Qualified, got {other:?}"),
        }
    }

    #[test]
    fn parses_variable_reference() {
        let e = parse_ok("$plan");
        match e.node {
            Expr::Variable(name) => assert_eq!(name.text, "plan"),
            other => panic!("expected Variable, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Parens and unary minus
    // ------------------------------------------------------------------

    #[test]
    fn parses_parenthesized_expression() {
        let e = parse_ok("(1 + 2)");
        match e.node {
            Expr::Paren(inner) => match inner.node {
                Expr::Binary { op, .. } => assert_eq!(op, BinaryOp::Add),
                other => panic!("expected Binary inside Paren, got {other:?}"),
            },
            other => panic!("expected Paren, got {other:?}"),
        }
    }

    #[test]
    fn parses_unary_minus() {
        let e = parse_ok("-5");
        match e.node {
            Expr::Unary {
                op: UnaryOp::Negate,
                operand,
            } => assert_eq!(lit_int(&operand), 5),
            other => panic!("expected Unary Negate, got {other:?}"),
        }
    }

    #[test]
    fn parses_double_unary_minus() {
        // `- -5` with a separating space — `--5` without a space would
        // lex as a line comment (`--`) per §26 line 1663 followed by
        // `5`, so the two unary minuses need to be tokenized separately.
        let e = parse_ok("- -5");
        match e.node {
            Expr::Unary { op, operand } => {
                assert_eq!(op, UnaryOp::Negate);
                match operand.node {
                    Expr::Unary { op, operand } => {
                        assert_eq!(op, UnaryOp::Negate);
                        assert_eq!(lit_int(&operand), 5);
                    }
                    other => panic!("expected nested Unary, got {other:?}"),
                }
            }
            other => panic!("expected Unary, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unary_plus() {
        // §26 line 1603 defines only `"-"? primary`, so unary `+` is
        // not a grammar production.
        match expr_of("+5") {
            Err(ParseError::Unexpected {
                offset, expected, ..
            }) => {
                assert_eq!(offset, 0);
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected Unexpected/Expression, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Arithmetic precedence
    // ------------------------------------------------------------------

    #[test]
    fn multiplication_binds_tighter_than_addition() {
        // 1 + 2 * 3 → Binary Add { 1, Binary Mul { 2, 3 } }
        let e = parse_ok("1 + 2 * 3");
        match e.node {
            Expr::Binary {
                op: BinaryOp::Add,
                left,
                right,
            } => {
                assert_eq!(lit_int(&left), 1);
                match right.node {
                    Expr::Binary {
                        op: BinaryOp::Multiply,
                        left,
                        right,
                    } => {
                        assert_eq!(lit_int(&left), 2);
                        assert_eq!(lit_int(&right), 3);
                    }
                    other => panic!("expected Binary Multiply, got {other:?}"),
                }
            }
            other => panic!("expected Binary Add, got {other:?}"),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        // (1 + 2) * 3 → Binary Mul { Paren { Binary Add { 1, 2 } }, 3 }
        let e = parse_ok("(1 + 2) * 3");
        match e.node {
            Expr::Binary {
                op: BinaryOp::Multiply,
                left,
                right,
            } => {
                assert!(matches!(left.node, Expr::Paren(_)));
                assert_eq!(lit_int(&right), 3);
            }
            other => panic!("expected Binary Multiply, got {other:?}"),
        }
    }

    #[test]
    fn subtraction_is_left_associative() {
        // 10 - 3 - 2 → ((10 - 3) - 2)
        let e = parse_ok("10 - 3 - 2");
        match e.node {
            Expr::Binary {
                op: BinaryOp::Subtract,
                left,
                right,
            } => {
                assert_eq!(lit_int(&right), 2);
                match left.node {
                    Expr::Binary {
                        op: BinaryOp::Subtract,
                        left,
                        right,
                    } => {
                        assert_eq!(lit_int(&left), 10);
                        assert_eq!(lit_int(&right), 3);
                    }
                    other => panic!("expected nested Binary Subtract, got {other:?}"),
                }
            }
            other => panic!("expected Binary Subtract, got {other:?}"),
        }
    }

    #[test]
    fn division_and_modulo_left_associative() {
        let e = parse_ok("100 / 4 % 3");
        match e.node {
            Expr::Binary {
                op: BinaryOp::Modulo,
                left,
                right,
            } => {
                assert_eq!(lit_int(&right), 3);
                assert!(matches!(
                    left.node,
                    Expr::Binary {
                        op: BinaryOp::Divide,
                        ..
                    }
                ));
            }
            other => panic!("expected Binary Modulo, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Comparisons
    // ------------------------------------------------------------------

    #[test]
    fn parses_every_comparison_operator() {
        let cases = &[
            ("a = 1", CompareOp::Equal),
            ("a != 1", CompareOp::NotEqual),
            ("a <> 1", CompareOp::NotEqual),
            ("a < 1", CompareOp::Less),
            ("a <= 1", CompareOp::LessOrEqual),
            ("a > 1", CompareOp::Greater),
            ("a >= 1", CompareOp::GreaterOrEqual),
        ];
        for (src, expected_op) in cases {
            match parse_ok(src).node {
                Expr::Compare { op, .. } => assert_eq!(&op, expected_op, "for {src}"),
                other => panic!("expected Compare for {src}, got {other:?}"),
            }
        }
    }

    #[test]
    fn comparison_is_non_associative() {
        // `a = b = c` must reject — exactly one comparison allowed.
        match expr_of("a = b = c") {
            Err(ParseError::Unexpected { .. }) => {}
            other => panic!("expected non-associative error, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // IS NULL / IS NOT NULL
    // ------------------------------------------------------------------

    #[test]
    fn parses_is_null() {
        let e = parse_ok("amount IS NULL");
        match e.node {
            Expr::IsNull { expr, negated } => {
                assert!(!negated);
                assert!(matches!(expr.node, Expr::Column(_)));
            }
            other => panic!("expected IsNull, got {other:?}"),
        }
    }

    #[test]
    fn parses_is_not_null() {
        let e = parse_ok("amount IS NOT NULL");
        match e.node {
            Expr::IsNull { expr, negated } => {
                assert!(negated);
                assert!(matches!(expr.node, Expr::Column(_)));
            }
            other => panic!("expected IsNull, got {other:?}"),
        }
    }

    #[test]
    fn is_without_null_errors() {
        match expr_of("amount IS 5") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("NULL"));
                assert!(detail.is_some());
            }
            other => panic!("expected Unexpected/NULL, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Boolean AND / OR / NOT
    // ------------------------------------------------------------------

    #[test]
    fn and_flattens_into_single_node() {
        // `a AND b AND c` → Expr::And(vec![a, b, c])
        let e = parse_ok("a AND b AND c");
        match e.node {
            Expr::And(items) => assert_eq!(items.len(), 3),
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn or_flattens_into_single_node() {
        let e = parse_ok("a OR b OR c");
        match e.node {
            Expr::Or(items) => assert_eq!(items.len(), 3),
            other => panic!("expected Or, got {other:?}"),
        }
    }

    #[test]
    fn and_binds_tighter_than_or() {
        // `a OR b AND c` → Or(a, And(b, c))
        let e = parse_ok("a OR b AND c");
        match e.node {
            Expr::Or(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0].node, Expr::Column(_)));
                assert!(matches!(items[1].node, Expr::And(_)));
            }
            other => panic!("expected Or, got {other:?}"),
        }
    }

    #[test]
    fn comparison_binds_tighter_than_and() {
        // `a > 1 AND b < 2` → And(Compare(a, 1), Compare(b, 2))
        let e = parse_ok("a > 1 AND b < 2");
        match e.node {
            Expr::And(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0].node, Expr::Compare { .. }));
                assert!(matches!(items[1].node, Expr::Compare { .. }));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn not_binds_tighter_than_and_and_or() {
        // `NOT a AND b` → And(Not(a), b)  — because NOT is above AND
        let e = parse_ok("NOT a AND b");
        match e.node {
            Expr::And(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0].node, Expr::Not(_)));
                assert!(matches!(items[1].node, Expr::Column(_)));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn not_below_comparison_per_sql_standard() {
        // `NOT x = 5` parses as `NOT (x = 5)` — SQL standard precedence
        // (see query-language.md §29 line 1922).
        let e = parse_ok("NOT x = 5");
        match e.node {
            Expr::Not(inner) => {
                assert!(matches!(inner.node, Expr::Compare { .. }));
            }
            other => panic!("expected Not, got {other:?}"),
        }
    }

    #[test]
    fn not_is_right_associative_chainable() {
        // `NOT NOT x` → Not(Not(x))
        let e = parse_ok("NOT NOT x");
        match e.node {
            Expr::Not(inner) => match inner.node {
                Expr::Not(inner2) => assert!(matches!(inner2.node, Expr::Column(_))),
                other => panic!("expected nested Not, got {other:?}"),
            },
            other => panic!("expected Not, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Compound / real-world expressions from the Wave 2 acceptance
    // script in docs/design/query-language.md and TASKS.md Wave 2 goal.
    // ------------------------------------------------------------------

    #[test]
    fn parses_wave2_acceptance_predicate() {
        // The Wave 2 acceptance script's WHERE predicate:
        // `event = 'checkout' AND amount > 100`.
        //
        // `event` is listed as a reserved keyword in query-language.md
        // §26.2 (from `EVENT TIME` / `EVENT TYPE` column modifiers), so
        // it must be backtick-quoted when used as a column name at the
        // parser level. The relaxation — allowing `event` as a bare
        // column name by making `EVENT` contextual — is a language-doc
        // decision that belongs to TASK-221 (DDL parser) or a later
        // spec fix, not to TASK-220. We exercise the same predicate
        // shape here with the quoted form.
        let e = parse_ok("`event` = 'checkout' AND amount > 100");
        match e.node {
            Expr::And(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0].node, Expr::Compare { .. }));
                assert!(matches!(items[1].node, Expr::Compare { .. }));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn parses_nested_parens_with_mixed_operators() {
        // `(a + b) * (c - d) / e`
        let e = parse_ok("(a + b) * (c - d) / e");
        match e.node {
            Expr::Binary {
                op: BinaryOp::Divide,
                left,
                ..
            } => {
                assert!(matches!(
                    left.node,
                    Expr::Binary {
                        op: BinaryOp::Multiply,
                        ..
                    }
                ));
            }
            other => panic!("expected Binary Divide, got {other:?}"),
        }
    }

    #[test]
    fn parses_duration_in_arithmetic() {
        // `ts - prev_ts > 1h` — duration literal in a comparison RHS.
        let e = parse_ok("ts - prev_ts > 1h");
        match e.node {
            Expr::Compare {
                op: CompareOp::Greater,
                right,
                ..
            } => match right.node {
                Expr::Literal(Literal::Duration(ns)) => {
                    assert_eq!(ns, 3_600 * 1_000_000_000);
                }
                other => panic!("expected Duration RHS, got {other:?}"),
            },
            other => panic!("expected Compare, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Span tracking
    // ------------------------------------------------------------------

    #[test]
    fn spans_cover_full_expression() {
        // `a + b * c` — the outer span should cover byte 0 through
        // the last byte of `c`.
        let e = parse_ok("a + b * c");
        assert_eq!(e.span.start, 0);
        assert_eq!(e.span.end, "a + b * c".len());
    }

    #[test]
    fn spans_preserve_inner_subexpression_positions() {
        // In `a + b * c`, the RHS `b * c` should have a span starting
        // at the `b` character and ending at the `c` character.
        let e = parse_ok("a + b * c");
        match e.node {
            Expr::Binary { right, .. } => {
                assert_eq!(right.span.start, 4);
                assert_eq!(right.span.end, 9);
            }
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Error paths
    // ------------------------------------------------------------------

    #[test]
    fn unclosed_paren_errors() {
        match expr_of("(1 + 2") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(")"));
            }
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(")"));
            }
            other => panic!("expected unclosed paren error, got {other:?}"),
        }
    }

    #[test]
    fn bare_comma_errors() {
        match expr_of(",") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected Unexpected/Expression, got {other:?}"),
        }
    }

    #[test]
    fn empty_input_errors() {
        match expr_of("") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn trailing_operator_errors() {
        match expr_of("1 +") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn trailing_and_errors() {
        // `a AND` — missing RHS of boolean conjunction.
        match expr_of("a AND") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn trailing_or_errors() {
        match expr_of("a OR") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn qualified_missing_column_errors() {
        // `purchases.` — qualified reference without a column part.
        match expr_of("purchases.") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected UnexpectedEof/Name, got {other:?}"),
        }
    }
}
