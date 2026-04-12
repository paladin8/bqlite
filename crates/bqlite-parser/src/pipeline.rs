//! Pipeline stage productions.
//!
//! Design: docs/design/query-language.md §26 (grammar) and
//! docs/design/language/grammar-framework.md §7.
//!
//! This module implements the `|`-separated continuation of a pipeline
//! after the source expression. Wave 2 / TASK-223 lands the Wave 2
//! verbs:
//!
//! - `| WHERE <predicate>` — row filter (§9).
//! - `| SELECT [DISTINCT] <items>` — projection (§10).
//! - `| LIMIT <integer>` — row cap (§13).
//!
//! The grammar lives in §26 lines 1520–1601. The precise productions:
//!
//! ```text
//! pipeline    := source ("|" operator)*
//! operator    := where_op | select_op | limit_op | ...
//!
//! where_op    := WHERE predicate
//! select_op   := SELECT DISTINCT? select_list
//! select_list := select_item ("," select_item)*
//! select_item := "*"
//!              | name                    -- bare column
//!              | name "." name           -- qualified column
//!              | expr AS identifier      -- computed expression
//! limit_op    := LIMIT integer
//! ```
//!
//! Every other Wave 2+ pipeline verb (`MATCH`, `FUNNEL`, `STATS`, …)
//! lives in later tasks and produces a `PipelineStage::…`-returning
//! production function sitting alongside the ones here.
//!
//! The module surface is crate-private. [`parse_pipeline_stages`] is
//! called from `crate::parser::parse_pipeline`; outside callers reach
//! pipelines via [`crate::parse`] in `lib.rs`.

#![allow(dead_code)] // TASK-221 / TASK-222 productions reach this module later.

use bqlite_ast::{AggItem, Expr, GroupItem, Name, PipelineStage, SelectItem, SelectItemKind};

use crate::error::{Expected, NameRole, ParseError};
use crate::expr::parse_expression;
use crate::lex::{token_span, Keyword, TokenKind};
use crate::parser::Parser;

/// Parse the `("|" stage)*` tail of a pipeline, returning the ordered
/// stage list. Stops at the first token that is not a `|`. The caller
/// is responsible for the source expression that precedes the tail.
pub(crate) fn parse_pipeline_stages(p: &mut Parser) -> Result<Vec<PipelineStage>, ParseError> {
    let mut stages = Vec::new();
    while matches!(p.peek_kind(), TokenKind::Pipe) {
        p.bump(); // consume `|`
        stages.push(parse_stage(p)?);
    }
    Ok(stages)
}

/// Dispatch on the first keyword after `|` and parse the matching stage.
fn parse_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    match p.peek_kind() {
        TokenKind::Kw(Keyword::Where) => parse_where_stage(p),
        TokenKind::Kw(Keyword::Select) => parse_select_stage(p),
        TokenKind::Kw(Keyword::Limit) => parse_limit_stage(p),
        TokenKind::Kw(Keyword::Stats) => parse_stats_stage(p),

        // Every other first token is either a later-wave verb that
        // TASK-223 does not yet implement, or an error. The error
        // message names `PipelineStage` so the user sees `"expected
        // pipeline stage"` rather than a bare `"unexpected token"`.
        _ => Err(p.error_unexpected(
            Expected::PipelineStage,
            Some("expected a pipeline stage keyword after `|`"),
        )),
    }
}

// ----------------------------------------------------------------------
// WHERE
// ----------------------------------------------------------------------

/// `WHERE predicate` — §26 line 1525.
///
/// The `WHERE` keyword has already been peeked by the dispatcher; this
/// function consumes it and then parses a single expression via the
/// existing expression grammar.
fn parse_where_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let where_tok = p.expect_kw(Keyword::Where)?;
    let start_span = token_span(&where_tok);
    let predicate = parse_expression(p)?;
    let span = start_span.merged(predicate.span);
    Ok(PipelineStage::Where { predicate, span })
}

// ----------------------------------------------------------------------
// LIMIT
// ----------------------------------------------------------------------

/// `LIMIT integer` — §26 line 1601.
///
/// `integer` is a bare non-negative integer literal. The lexer never
/// produces negative `Int` tokens (see `docs/design/query-language.md`
/// §30 "negative numeric literals" and `expr.rs` `parses_unary_minus`),
/// so `LIMIT -5` parses as `LIMIT` followed by a `-` token and fails
/// the `expect_int` check with an `Expected::Integer` error.
fn parse_limit_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let limit_tok = p.expect_kw(Keyword::Limit)?;
    let start_span = token_span(&limit_tok);
    let (value, int_span) = p.expect_int()?;
    // The lexer emits only non-negative Int tokens; casting is safe.
    debug_assert!(value >= 0, "lexer emits non-negative Int tokens");
    let count = value as u64;
    let span = start_span.merged(int_span);
    Ok(PipelineStage::Limit { count, span })
}

// ----------------------------------------------------------------------
// SELECT
// ----------------------------------------------------------------------

/// `SELECT DISTINCT? select_list` — §26 line 1528.
///
/// ```text
/// select_list := select_item ("," select_item)*
/// select_item := "*"
///              | name                    -- bare column
///              | name "." name           -- qualified column
///              | expr AS identifier      -- computed expression
/// ```
fn parse_select_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let select_tok = p.expect_kw(Keyword::Select)?;
    let start_span = token_span(&select_tok);

    // Optional DISTINCT modifier — applies to the whole output row, not
    // individual items (query-language.md §10 line 736).
    let distinct = p.try_kw(Keyword::Distinct).is_some();

    // At least one select item. An empty list is a parse error.
    let mut items = Vec::new();
    items.push(parse_select_item(p)?);
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        items.push(parse_select_item(p)?);
    }

    // Span covers SELECT through the last parsed select_item's span.
    let last_span = items
        .last()
        .expect("loop above pushes at least one item")
        .span;
    let span = start_span.merged(last_span);

    Ok(PipelineStage::Select {
        distinct,
        items,
        span,
    })
}

/// Parse one `select_item`. The grammar has three shapes:
///
/// 1. `*` — wildcard, no alias.
/// 2. A bare or qualified column reference — no alias required.
/// 3. Any other expression — an `AS identifier` alias is **required**
///    per §10 line 730, which is enforced here.
fn parse_select_item(p: &mut Parser) -> Result<SelectItem, ParseError> {
    // Case 1: `*` wildcard. The `*` token here is the multiplication
    // operator in the expression grammar, so we have to match it before
    // calling `parse_expression` — otherwise the expression parser would
    // see a leading `*` and emit `Expected::Expression`.
    if matches!(p.peek_kind(), TokenKind::Star) {
        let tok = p.bump();
        let span = token_span(&tok);
        // A wildcard cannot carry an alias (§26 line 1530:
        // `select_item := "*" | ...`). Intercept `SELECT * AS foo`
        // here so the user sees a specific error rather than the
        // generic `expected end of input` produced when the trailing
        // `AS` is left to `expect_eof`.
        if matches!(p.peek_kind(), TokenKind::Kw(Keyword::As)) {
            let as_tok = p.peek();
            return Err(ParseError::Unexpected {
                offset: as_tok.start,
                line: as_tok.line,
                column: as_tok.column,
                expected: Expected::Punct(","),
                found: "AS".to_string(),
                detail: Some("wildcard `*` select items cannot have an alias"),
            });
        }
        return Ok(SelectItem {
            kind: SelectItemKind::Wildcard,
            alias: None,
            span,
        });
    }

    // Remember where the expression starts so error spans are precise.
    let expr_start = token_span(p.peek());

    // Case 2 + 3: parse an expression and then decide whether an alias
    // is required based on the expression shape.
    let expr = parse_expression(p)?;
    let expr_span = expr.span;

    if p.try_kw(Keyword::As).is_some() {
        // Alias is always a bare identifier per §26 line 1533
        // (`expr AS identifier`, not `expr AS name`). `expect_ident`
        // returns a plain `(String, Span)`, which we promote to a
        // `Name`. The `NameRole::AliasName` role routes reserved-keyword
        // errors (e.g. `AS where`) to the right diagnostic slot.
        let (alias_text, alias_span) = p.expect_ident(NameRole::AliasName)?;
        let alias = Name::new(alias_text, alias_span);
        let span = expr_span.merged(alias_span);
        return Ok(SelectItem {
            kind: SelectItemKind::Expr(expr),
            alias: Some(alias),
            span,
        });
    }

    // No alias — the expression must be a bare or qualified column
    // reference. Anything else is a parse error per §10 line 730
    // ("computed expressions never auto-generate names").
    if is_bare_or_qualified_column(&expr.node) {
        return Ok(SelectItem {
            kind: SelectItemKind::Expr(expr),
            alias: None,
            span: expr_span,
        });
    }

    // Error: computed expression without an alias. Point at the start
    // of the expression so the user sees the offending term rather
    // than the `|` or `,` that precedes it. We fabricate a `ParseError`
    // manually rather than going through `Parser::error_unexpected`
    // because the cursor has already moved past the expression and we
    // want the error to reference the *expression's* start, not the
    // current token.
    Err(ParseError::Unexpected {
        offset: expr_start.start,
        line: expr_start.line,
        column: expr_start.column,
        expected: Expected::Keyword("AS"),
        found: "computed expression".to_string(),
        detail: Some("computed SELECT items must have an `AS <alias>` name"),
    })
}

/// True when `expr` is a bare column reference (`col`) or qualified
/// column reference (`table.col`), both of which are legal select items
/// without an `AS` alias.
fn is_bare_or_qualified_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Qualified { .. })
}

// ----------------------------------------------------------------------
// STATS
// ----------------------------------------------------------------------

/// `STATS agg_list [GROUP BY group_list]` — §26 line 1576.
///
/// Grammar:
/// ```text
/// stats_op   := STATS agg_list (GROUP BY group_list)?
/// agg_list   := agg_item ("," agg_item)*
/// agg_item   := identifier "=" agg_expr
/// agg_expr   := agg_func "(" (expr | "*") ")"
/// agg_func   := COUNT | COUNT_DISTINCT | SUM | AVG | MIN | MAX
///             | P50 | P90 | P95 | P99
/// group_list := group_item ("," group_item)*
/// group_item := name
///             | expr AS identifier
/// ```
///
/// Per §7.2, `GROUP BY` is the required two-keyword form. A bare `BY`
/// without `GROUP` is rejected with a helpful error. Per §7.1,
/// `COUNT(DISTINCT col)` is a parse error — use `COUNT_DISTINCT(col)`.
fn parse_stats_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let stats_tok = p.expect_kw(Keyword::Stats)?;
    let start_span = token_span(&stats_tok);

    let aggregates = parse_agg_list(p)?;

    // Optional GROUP BY clause.
    //
    // Bare `BY` without `GROUP` is a syntax error: BQL always uses
    // `GROUP BY` (two keywords), never a standalone `BY`
    // (query-language.md §7.2 and the grammar at §26 line 1576).
    let group_by = match p.peek_kind() {
        TokenKind::Kw(Keyword::By) => {
            let by_tok = p.peek().clone();
            return Err(ParseError::Unexpected {
                offset: by_tok.start,
                line: by_tok.line,
                column: by_tok.column,
                expected: Expected::Keyword("GROUP"),
                found: "BY".to_string(),
                detail: Some("STATS uses `GROUP BY` (two keywords), not bare `BY`"),
            });
        }
        TokenKind::Kw(Keyword::Group) => {
            p.bump(); // consume GROUP
            p.expect_kw(Keyword::By)?;
            parse_group_list(p)?
        }
        _ => vec![],
    };

    // `parse_agg_list` always returns at least one item — enforce the
    // invariant explicitly so the `unwrap_or` below is clearly a
    // dead-code safety net, not a real fallback.
    debug_assert!(
        !aggregates.is_empty(),
        "agg list must have at least one item"
    );

    // Span: STATS keyword through the last token consumed.
    let last_span = group_by
        .last()
        .map(|g: &GroupItem| g.span)
        .or_else(|| aggregates.last().map(|a: &AggItem| a.span))
        .map(|s| start_span.merged(s))
        .unwrap_or(start_span);

    Ok(PipelineStage::Stats {
        aggregates,
        group_by,
        span: last_span,
    })
}

/// Parse the required aggregate item list: at least one `agg_item`,
/// comma-separated.
fn parse_agg_list(p: &mut Parser) -> Result<Vec<AggItem>, ParseError> {
    let mut items = Vec::new();
    items.push(parse_agg_item(p)?);
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        items.push(parse_agg_item(p)?);
    }
    Ok(items)
}

/// Parse one `identifier "=" agg_expr`.
///
/// The output alias (`identifier`) is required per §7.1 — bare
/// aggregate expressions without an alias are a parse error.
fn parse_agg_item(p: &mut Parser) -> Result<AggItem, ParseError> {
    // Output alias — bare identifier (not a backtick name; §26 line 1578
    // uses `identifier`, not `name`).
    let (alias_text, alias_span) = p.expect_ident(NameRole::AliasName)?;
    let alias = Name::new(alias_text, alias_span);

    // `=` assignment separator.
    p.expect_punct(&TokenKind::Eq, "=")?;

    // Aggregate expression.
    parse_agg_expr(p, alias)
}

/// True when `kw` is one of the ten supported aggregate function
/// keywords per §7.1 and §26 line 1580.
fn is_agg_func(kw: Keyword) -> bool {
    matches!(
        kw,
        Keyword::Count
            | Keyword::CountDistinct
            | Keyword::Sum
            | Keyword::Avg
            | Keyword::Min
            | Keyword::Max
            | Keyword::P50
            | Keyword::P90
            | Keyword::P95
            | Keyword::P99
    )
}

/// Parse `agg_func "(" (expr | "*") ")"` and assemble an [`AggItem`].
///
/// `alias` is the output name already parsed by the caller. The
/// function name is stored as the lowercase canonical keyword string
/// in `AggItem::function` (e.g. `"count"`, `"count_distinct"`, `"p95"`).
///
/// `COUNT(DISTINCT col)` is explicitly rejected here per §7.1 —
/// `DISTINCT` is not a valid argument modifier for any aggregate. Use
/// `COUNT_DISTINCT(col)` instead.
fn parse_agg_expr(p: &mut Parser, alias: Name) -> Result<AggItem, ParseError> {
    // Match the aggregate function keyword.
    let (kw, func_span) = match p.peek_kind() {
        TokenKind::Kw(k) if is_agg_func(*k) => {
            let k = *k;
            let tok = p.bump();
            (k, token_span(&tok))
        }
        _ => {
            return Err(p.error_unexpected(
                Expected::Expression,
                Some(
                    "expected an aggregate function: \
                     COUNT, COUNT_DISTINCT, SUM, AVG, MIN, MAX, P50, P90, P95, P99",
                ),
            ));
        }
    };

    // Opening paren.
    p.expect_punct(&TokenKind::LParen, "(")?;

    // `COUNT(DISTINCT col)` is a parse error per §7.1 — `DISTINCT` is
    // not valid inside any aggregate expression. The correct form for
    // distinct counting is `COUNT_DISTINCT(col)`.
    if matches!(p.peek_kind(), TokenKind::Kw(Keyword::Distinct)) {
        let tok = p.peek().clone();
        return Err(ParseError::Unexpected {
            offset: tok.start,
            line: tok.line,
            column: tok.column,
            expected: Expected::Expression,
            found: "DISTINCT".to_string(),
            detail: Some(
                "`DISTINCT` is not valid inside aggregate expressions; \
                 use `COUNT_DISTINCT(col)` for distinct counting",
            ),
        });
    }

    // Argument: `*` (star form — maps to empty args list, e.g. COUNT(*))
    // or a regular expression. The `*` token is the same as the
    // multiplication token; we intercept it here before delegating to
    // `parse_expression` which would interpret a leading `*` as an error.
    let args = if matches!(p.peek_kind(), TokenKind::Star) {
        p.bump(); // consume `*`
        vec![]
    } else {
        let expr = parse_expression(p)?;
        vec![expr]
    };

    // Closing paren.
    let close_tok = p.expect_punct(&TokenKind::RParen, ")")?;

    // Span covers from the alias name through the closing `)`.
    // `alias.span` precedes `func_span`, so the merge correctly extends
    // from the alias start to the `)` end.
    let item_span = alias.span.merged(func_span.merged(token_span(&close_tok)));

    Ok(AggItem {
        // Store lowercase canonical name: "count", "count_distinct", etc.
        function: Name::new(kw.canonical().to_lowercase(), func_span),
        args,
        // `COUNT(DISTINCT col)` is rejected above — the `distinct` flag
        // on AggItem is always false for parser-produced nodes.
        distinct: false,
        alias,
        span: item_span,
    })
}

/// Parse the GROUP BY item list: at least one `group_item`,
/// comma-separated.
fn parse_group_list(p: &mut Parser) -> Result<Vec<GroupItem>, ParseError> {
    let mut items = Vec::new();
    items.push(parse_group_item(p)?);
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        items.push(parse_group_item(p)?);
    }
    Ok(items)
}

/// Parse one GROUP BY item.
///
/// ```text
/// group_item := name                    -- bare column reference
///             | expr AS identifier      -- computed group key
/// ```
///
/// Bare column references do not require an `AS` alias. Computed
/// expressions **must** carry an `AS alias` per §7.2 — the same rule
/// as SELECT computed items (§10).
fn parse_group_item(p: &mut Parser) -> Result<GroupItem, ParseError> {
    let item_start = token_span(p.peek());

    let expr = parse_expression(p)?;
    let expr_span = expr.span;

    // Optional `AS identifier` alias.
    if p.try_kw(Keyword::As).is_some() {
        let (alias_text, alias_span) = p.expect_ident(NameRole::AliasName)?;
        let alias = Name::new(alias_text, alias_span);
        let span = expr_span.merged(alias_span);
        return Ok(GroupItem {
            expr,
            alias: Some(alias),
            span,
        });
    }

    // No alias — the expression must be a bare or qualified column
    // reference. Computed expressions require `AS alias` in GROUP BY
    // (query-language.md §7.2).
    if is_bare_or_qualified_column(&expr.node) {
        return Ok(GroupItem {
            expr,
            alias: None,
            span: expr_span,
        });
    }

    Err(ParseError::Unexpected {
        offset: item_start.start,
        line: item_start.line,
        column: item_start.column,
        expected: Expected::Keyword("AS"),
        found: "computed expression".to_string(),
        detail: Some("computed GROUP BY expressions must have an `AS <alias>` name"),
    })
}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::{BinaryOp, CompareOp, Literal, Spanned, Statement};

    // --- helpers ------------------------------------------------------

    fn parse_stmt(src: &str) -> Statement {
        crate::parse(src).unwrap_or_else(|e| panic!("parse failed for `{src}`: {e:?}"))
    }

    fn stages_of(stmt: &Statement) -> &[PipelineStage] {
        match stmt {
            Statement::Query(p) => &p.stages,
            other => panic!("expected Query, got {other:?}"),
        }
    }

    // --- WHERE --------------------------------------------------------

    #[test]
    fn where_stage_with_equality() {
        let stmt = parse_stmt("events | WHERE amount = 100");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 1);
        match &stages[0] {
            PipelineStage::Where { predicate, .. } => match &predicate.node {
                Expr::Compare {
                    op: CompareOp::Equal,
                    ..
                } => (),
                other => panic!("expected Compare Equal, got {other:?}"),
            },
            other => panic!("expected Where, got {other:?}"),
        }
    }

    #[test]
    fn where_stage_with_and_predicate() {
        // The Wave 2 acceptance predicate shape:
        //   `WHERE \`event\` = 'checkout' AND amount > 100`
        let stmt = parse_stmt("purchases | WHERE `event` = 'checkout' AND amount > 100");
        let stages = stages_of(&stmt);
        match &stages[0] {
            PipelineStage::Where { predicate, .. } => match &predicate.node {
                Expr::And(items) => assert_eq!(items.len(), 2),
                other => panic!("expected And, got {other:?}"),
            },
            other => panic!("expected Where, got {other:?}"),
        }
    }

    #[test]
    fn where_stage_case_insensitive_keyword() {
        let stmt = parse_stmt("events | where amount > 0");
        assert!(matches!(stages_of(&stmt)[0], PipelineStage::Where { .. }));
    }

    #[test]
    fn where_stage_missing_predicate_errors() {
        match crate::parse("events | WHERE") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn where_stage_carries_span_from_keyword_through_expression() {
        let src = "events | WHERE amount > 100";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::Where { span, .. } => {
                // Span starts at `WHERE` and ends at the last `0`.
                let w_start = src.find("WHERE").unwrap();
                assert_eq!(span.start, w_start);
                assert_eq!(span.end, src.len());
            }
            other => panic!("expected Where, got {other:?}"),
        }
    }

    // --- LIMIT --------------------------------------------------------

    #[test]
    fn limit_stage_with_literal() {
        let stmt = parse_stmt("events | LIMIT 100");
        match &stages_of(&stmt)[0] {
            PipelineStage::Limit { count, .. } => assert_eq!(*count, 100),
            other => panic!("expected Limit, got {other:?}"),
        }
    }

    #[test]
    fn limit_zero_is_accepted() {
        let stmt = parse_stmt("events | LIMIT 0");
        match &stages_of(&stmt)[0] {
            PipelineStage::Limit { count, .. } => assert_eq!(*count, 0),
            other => panic!("expected Limit, got {other:?}"),
        }
    }

    #[test]
    fn limit_rejects_negative_literal() {
        // `LIMIT -5` — the lexer tokenizes as Kw(Limit), Minus, Int(5).
        // `expect_int` sees the `-` and errors with Expected::Integer.
        match crate::parse("events | LIMIT -5") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Integer);
            }
            other => panic!("expected Unexpected/Integer, got {other:?}"),
        }
    }

    #[test]
    fn limit_rejects_non_integer() {
        match crate::parse("events | LIMIT foo") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Integer);
            }
            other => panic!("expected Unexpected/Integer, got {other:?}"),
        }
    }

    // --- SELECT -------------------------------------------------------

    #[test]
    fn select_single_bare_column() {
        let stmt = parse_stmt("events | SELECT user_id");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select {
                distinct, items, ..
            } => {
                assert!(!distinct);
                assert_eq!(items.len(), 1);
                assert!(items[0].alias.is_none());
                match &items[0].kind {
                    SelectItemKind::Expr(e) => match &e.node {
                        Expr::Column(n) => assert_eq!(n.text, "user_id"),
                        other => panic!("expected Column, got {other:?}"),
                    },
                    other => panic!("expected Expr item, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_wildcard() {
        let stmt = parse_stmt("events | SELECT *");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { items, .. } => {
                assert_eq!(items.len(), 1);
                assert!(matches!(items[0].kind, SelectItemKind::Wildcard));
                assert!(items[0].alias.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_multi_column_list() {
        let stmt = parse_stmt("events | SELECT user_id, ts, amount");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { items, .. } => {
                assert_eq!(items.len(), 3);
                for it in items {
                    assert!(matches!(it.kind, SelectItemKind::Expr(_)));
                    assert!(it.alias.is_none());
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_qualified_column_reference() {
        let stmt = parse_stmt("purchases | SELECT purchases.amount");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { items, .. } => {
                assert_eq!(items.len(), 1);
                match &items[0].kind {
                    SelectItemKind::Expr(e) => match &e.node {
                        Expr::Qualified { table, column } => {
                            assert_eq!(table.text, "purchases");
                            assert_eq!(column.text, "amount");
                        }
                        other => panic!("expected Qualified, got {other:?}"),
                    },
                    other => panic!("expected Expr item, got {other:?}"),
                }
                assert!(items[0].alias.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_expression_with_alias() {
        let stmt = parse_stmt("events | SELECT amount * 2 AS doubled");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { items, .. } => {
                assert_eq!(items.len(), 1);
                match &items[0].kind {
                    SelectItemKind::Expr(e) => match &e.node {
                        Expr::Binary {
                            op: BinaryOp::Multiply,
                            ..
                        } => (),
                        other => panic!("expected Binary Multiply, got {other:?}"),
                    },
                    other => panic!("expected Expr item, got {other:?}"),
                }
                assert_eq!(items[0].alias.as_ref().unwrap().text, "doubled");
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_mixed_wildcard_and_aliased_expression() {
        // `SELECT *, amount * 1.1 AS adjusted` — the AST permits
        // wildcard alongside other items per query-language.md §10.
        let stmt = parse_stmt("events | SELECT *, amount * 1.1 AS adjusted");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { items, .. } => {
                assert_eq!(items.len(), 2);
                assert!(matches!(items[0].kind, SelectItemKind::Wildcard));
                assert!(matches!(items[1].kind, SelectItemKind::Expr(_)));
                assert_eq!(items[1].alias.as_ref().unwrap().text, "adjusted");
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_distinct_sets_flag() {
        let stmt = parse_stmt("events | SELECT DISTINCT user_id, device");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select {
                distinct, items, ..
            } => {
                assert!(distinct);
                assert_eq!(items.len(), 2);
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_distinct_wildcard_is_legal() {
        // `SELECT DISTINCT *` — grammar-legal combination; neither the
        // DISTINCT keyword nor the wildcard branch blocks the other.
        let stmt = parse_stmt("events | SELECT DISTINCT *");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select {
                distinct, items, ..
            } => {
                assert!(distinct);
                assert_eq!(items.len(), 1);
                assert!(matches!(items[0].kind, SelectItemKind::Wildcard));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_qualified_column_with_alias() {
        // The qualified-column form takes an AS alias via the general
        // `expr AS identifier` rule (not a separate grammar branch).
        let stmt = parse_stmt("purchases | SELECT purchases.amount AS amt");
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { items, .. } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].alias.as_ref().unwrap().text, "amt");
                match &items[0].kind {
                    SelectItemKind::Expr(e) => {
                        assert!(matches!(e.node, Expr::Qualified { .. }));
                    }
                    other => panic!("expected Expr item, got {other:?}"),
                }
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn select_wildcard_with_alias_errors_with_specific_message() {
        // `SELECT * AS foo` — the wildcard cannot carry an alias.
        match crate::parse("events | SELECT * AS foo") {
            Err(ParseError::Unexpected {
                found,
                detail,
                expected,
                ..
            }) => {
                assert_eq!(found, "AS");
                assert_eq!(expected, Expected::Punct(","));
                assert!(detail
                    .unwrap_or("")
                    .contains("wildcard `*` select items cannot have an alias"));
            }
            other => panic!("expected specific wildcard/alias error, got {other:?}"),
        }
    }

    #[test]
    fn select_alias_that_shadows_keyword_errors_as_alias_name() {
        // `SELECT amount AS where` — reserved-keyword alias. The
        // diagnostic should name the slot as "alias name", not
        // "table name".
        match crate::parse("events | SELECT amount AS where") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "WHERE");
                assert_eq!(role, NameRole::AliasName);
            }
            other => panic!("expected ReservedKeyword(AliasName), got {other:?}"),
        }
    }

    #[test]
    fn select_computed_without_alias_errors_pointing_at_expression() {
        // `SELECT amount * 1.1` without an AS alias is a parse error.
        match crate::parse("events | SELECT amount * 1.1") {
            Err(ParseError::Unexpected {
                offset,
                expected,
                detail,
                ..
            }) => {
                assert_eq!(expected, Expected::Keyword("AS"));
                assert!(detail
                    .unwrap_or("")
                    .contains("must have an `AS <alias>` name"));
                // Offset points at the start of the expression (`amount`),
                // not at the `|` or the `SELECT` keyword.
                let src = "events | SELECT amount * 1.1";
                assert_eq!(offset, src.find("amount").unwrap());
            }
            other => panic!("expected Unexpected(AS) error, got {other:?}"),
        }
    }

    #[test]
    fn select_trailing_comma_errors() {
        match crate::parse("events | SELECT user_id,") {
            Err(ParseError::UnexpectedEof { .. }) => {}
            Err(ParseError::Unexpected { .. }) => {}
            other => panic!("expected trailing-comma error, got {other:?}"),
        }
    }

    #[test]
    fn select_literal_without_alias_errors() {
        // `SELECT 42` — bare integer literal without `AS alias`.
        match crate::parse("events | SELECT 42") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("AS"));
                assert!(detail.is_some());
            }
            other => panic!("expected Unexpected/AS error, got {other:?}"),
        }
    }

    // --- multi-stage pipelines ---------------------------------------

    #[test]
    fn pipeline_with_where_select_limit_chain() {
        // The Wave 2 acceptance pipeline:
        //   purchases | where event = 'checkout' AND amount > 100
        //             | select user_id, ts, amount
        //             | limit 100
        let src = "purchases \
                   | where `event` = 'checkout' AND amount > 100 \
                   | select user_id, ts, amount \
                   | limit 100";
        let stmt = parse_stmt(src);
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 3);
        assert!(matches!(stages[0], PipelineStage::Where { .. }));
        match &stages[1] {
            PipelineStage::Select { items, .. } => assert_eq!(items.len(), 3),
            other => panic!("expected Select, got {other:?}"),
        }
        match &stages[2] {
            PipelineStage::Limit { count, .. } => assert_eq!(*count, 100),
            other => panic!("expected Limit, got {other:?}"),
        }
    }

    #[test]
    fn pipeline_with_trailing_semicolon_on_last_stage() {
        let stmt = parse_stmt("events | LIMIT 10;");
        assert_eq!(stages_of(&stmt).len(), 1);
    }

    #[test]
    fn pipeline_preserves_multiple_where_stages_in_order() {
        // Two WHERE stages should survive parse in order — the parser
        // does not fold them. (§25.2 allows WHERE after anything that
        // produces rows, including another WHERE.)
        let stmt = parse_stmt("events | WHERE amount > 0 | WHERE amount < 100");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 2);
        assert!(matches!(stages[0], PipelineStage::Where { .. }));
        assert!(matches!(stages[1], PipelineStage::Where { .. }));
    }

    #[test]
    fn limit_is_not_pipeline_terminal() {
        // Per §25.2 and §13, LIMIT is not a terminal stage — WHERE /
        // SELECT / LET / ORDER BY can follow. The parser should accept
        // `| LIMIT 100 | SELECT user_id` without complaint.
        let stmt = parse_stmt("events | LIMIT 100 | SELECT user_id");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 2);
        match &stages[0] {
            PipelineStage::Limit { count, .. } => assert_eq!(*count, 100),
            other => panic!("expected Limit first, got {other:?}"),
        }
        assert!(matches!(stages[1], PipelineStage::Select { .. }));
    }

    #[test]
    fn pipe_followed_by_eof_errors_with_pipeline_stage() {
        match crate::parse("events |") {
            Err(ParseError::Unexpected { expected, .. })
            | Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::PipelineStage);
            }
            other => panic!("expected PipelineStage error, got {other:?}"),
        }
    }

    #[test]
    fn pipe_followed_by_non_stage_keyword_errors() {
        // `INTO` is a reserved keyword but not a pipeline stage verb.
        match crate::parse("events | INTO stuff") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::PipelineStage);
            }
            other => panic!("expected Unexpected/PipelineStage, got {other:?}"),
        }
    }

    #[test]
    fn pipe_followed_by_identifier_errors() {
        // Identifiers are not pipeline-stage starts; only keywords are.
        match crate::parse("events | foo bar") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::PipelineStage);
            }
            other => panic!("expected Unexpected/PipelineStage, got {other:?}"),
        }
    }

    // Spans on individual stages should cover from the stage keyword
    // through the last token the stage consumes, not leak past the
    // trailing `|`.
    #[test]
    fn select_span_ends_at_last_item() {
        // The source uses `amount` as the trailing column so the test
        // does not collide with the substring `ts` inside `events`.
        let src = "events | SELECT user_id, amount | LIMIT 10";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::Select { span, .. } => {
                // Span starts at SELECT, ends at end of `amount` (not the `|`).
                let sel = src.find("SELECT").unwrap();
                let amount_end = src.find("amount").unwrap() + "amount".len();
                assert_eq!(span.start, sel);
                assert_eq!(span.end, amount_end);
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    // Bare literal sanity — confirms the `Expr::Literal` path out of
    // `parse_expression` is correctly flagged as needing an alias.
    #[test]
    fn bare_literal_in_select_detected_as_computed() {
        match crate::parse("events | SELECT 'x'") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("AS"));
            }
            other => panic!("expected AS error on bare literal, got {other:?}"),
        }
        // Confirm the same for null / bool to make sure we don't accidentally
        // accept them due to `Expr::Literal` pattern matching weirdness.
        assert!(matches!(
            crate::parse("events | SELECT NULL"),
            Err(ParseError::Unexpected { .. })
        ));
        assert!(matches!(
            crate::parse("events | SELECT true"),
            Err(ParseError::Unexpected { .. })
        ));
    }

    // Confirms that, past `_` rejections, the `Literal` variant that
    // triggers the error doesn't accidentally match `is_bare_or_qualified_column`.
    #[test]
    fn is_bare_or_qualified_column_rejects_literals_and_binary() {
        let lit = Expr::Literal(Literal::Int(1));
        assert!(!is_bare_or_qualified_column(&lit));
        let bin = Expr::Binary {
            op: BinaryOp::Add,
            left: Box::new(Spanned::new(lit.clone(), bqlite_ast::Span::EMPTY)),
            right: Box::new(Spanned::new(lit, bqlite_ast::Span::EMPTY)),
        };
        assert!(!is_bare_or_qualified_column(&bin));
    }

    // --- STATS --------------------------------------------------------

    // Helper: extract AggItems from the first Stats stage.
    fn agg_items_of(stages: &[PipelineStage]) -> &[AggItem] {
        match &stages[0] {
            PipelineStage::Stats { aggregates, .. } => aggregates.as_slice(),
            other => panic!("expected Stats, got {other:?}"),
        }
    }

    // Helper: extract GroupItems from the first Stats stage.
    fn group_items_of(stages: &[PipelineStage]) -> &[GroupItem] {
        match &stages[0] {
            PipelineStage::Stats { group_by, .. } => group_by.as_slice(),
            other => panic!("expected Stats, got {other:?}"),
        }
    }

    #[test]
    fn stats_count_star_single_aggregate() {
        // `| STATS total = COUNT(*)` — COUNT(*) maps to empty args list.
        let stmt = parse_stmt("events | STATS total = COUNT(*)");
        let items = agg_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].alias.text, "total");
        assert_eq!(items[0].function.text, "count");
        assert!(items[0].args.is_empty(), "COUNT(*) should have no args");
        assert!(!items[0].distinct);
    }

    #[test]
    fn stats_count_with_column_argument() {
        let stmt = parse_stmt("events | STATS n = COUNT(user_id)");
        let items = agg_items_of(stages_of(&stmt));
        assert_eq!(items[0].function.text, "count");
        assert_eq!(items[0].args.len(), 1);
        match &items[0].args[0].node {
            Expr::Column(c) => assert_eq!(c.text, "user_id"),
            other => panic!("expected Column arg, got {other:?}"),
        }
    }

    #[test]
    fn stats_count_distinct() {
        // `COUNT_DISTINCT(col)` is the valid distinct-count form.
        let stmt = parse_stmt("events | STATS uv = COUNT_DISTINCT(user_id)");
        let items = agg_items_of(stages_of(&stmt));
        assert_eq!(items[0].function.text, "count_distinct");
        assert_eq!(items[0].args.len(), 1);
        assert!(!items[0].distinct);
    }

    #[test]
    fn stats_all_ten_aggregate_functions() {
        // Smoke-test that each aggregate function keyword is accepted
        // and stored with the correct lowercase name.
        let cases: &[(&str, &str)] = &[
            ("n = COUNT(*)", "count"),
            ("u = COUNT_DISTINCT(x)", "count_distinct"),
            ("s = SUM(amount)", "sum"),
            ("a = AVG(amount)", "avg"),
            ("lo = MIN(amount)", "min"),
            ("hi = MAX(amount)", "max"),
            ("q50 = P50(latency)", "p50"),
            ("q90 = P90(latency)", "p90"),
            ("q95 = P95(latency)", "p95"),
            ("q99 = P99(latency)", "p99"),
        ];
        for (agg_src, expected_fn) in cases {
            let src = format!("events | STATS {agg_src}");
            let stmt = parse_stmt(&src);
            let items = agg_items_of(stages_of(&stmt));
            assert_eq!(
                items[0].function.text, *expected_fn,
                "function name mismatch for `{agg_src}`"
            );
        }
    }

    #[test]
    fn stats_multiple_aggregates() {
        let stmt =
            parse_stmt("events | STATS total = COUNT(*), avg_amt = AVG(amount), mx = MAX(amount)");
        let items = agg_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].alias.text, "total");
        assert_eq!(items[1].alias.text, "avg_amt");
        assert_eq!(items[2].alias.text, "mx");
        assert_eq!(items[0].function.text, "count");
        assert_eq!(items[1].function.text, "avg");
        assert_eq!(items[2].function.text, "max");
    }

    #[test]
    fn stats_with_group_by_single_bare_column() {
        let stmt = parse_stmt("events | STATS n = COUNT(*) GROUP BY device");
        let groups = group_items_of(stages_of(&stmt));
        assert_eq!(groups.len(), 1);
        assert!(groups[0].alias.is_none());
        match &groups[0].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "device"),
            other => panic!("expected Column in GROUP BY, got {other:?}"),
        }
    }

    #[test]
    fn stats_with_group_by_multiple_bare_columns() {
        let stmt = parse_stmt("events | STATS n = COUNT(*) GROUP BY device, plan");
        let groups = group_items_of(stages_of(&stmt));
        assert_eq!(groups.len(), 2);
        match &groups[0].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "device"),
            other => panic!("expected first GROUP BY column, got {other:?}"),
        }
        match &groups[1].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "plan"),
            other => panic!("expected second GROUP BY column, got {other:?}"),
        }
    }

    #[test]
    fn stats_with_group_by_computed_expression_requires_alias() {
        // Computed expressions in GROUP BY must have an AS alias.
        // `amount * 2` is an arithmetic expression — a computed expression.
        // (Function calls like QUANTIZE are deferred to a later wave task.)
        let stmt = parse_stmt("events | STATS n = COUNT(*) GROUP BY amount * 2 AS doubled");
        let groups = group_items_of(stages_of(&stmt));
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].alias.as_ref().unwrap().text, "doubled");
        match &groups[0].expr.node {
            Expr::Binary {
                op: BinaryOp::Multiply,
                ..
            } => {}
            other => panic!("expected Binary Multiply in GROUP BY, got {other:?}"),
        }
    }

    #[test]
    fn stats_with_group_by_mixed_bare_and_computed() {
        let stmt = parse_stmt("events | STATS n = COUNT(*) GROUP BY device, amount * 2 AS doubled");
        let groups = group_items_of(stages_of(&stmt));
        assert_eq!(groups.len(), 2);
        assert!(groups[0].alias.is_none()); // bare column
        assert!(groups[1].alias.is_some()); // computed
        assert_eq!(groups[1].alias.as_ref().unwrap().text, "doubled");
    }

    #[test]
    fn stats_case_insensitive_keywords() {
        // Parser is case-insensitive per §2.2.
        let stmt = parse_stmt("events | stats total = count(*) group by device");
        let stages = stages_of(&stmt);
        assert!(matches!(stages[0], PipelineStage::Stats { .. }));
        let items = agg_items_of(stages);
        assert_eq!(items[0].function.text, "count");
    }

    #[test]
    fn stats_without_group_by_has_empty_group_list() {
        let stmt = parse_stmt("events | STATS n = COUNT(*)");
        match &stages_of(&stmt)[0] {
            PipelineStage::Stats { group_by, .. } => assert!(group_by.is_empty()),
            other => panic!("expected Stats, got {other:?}"),
        }
    }

    #[test]
    fn stats_span_covers_keyword_through_last_token() {
        let src = "events | STATS total = COUNT(*)";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::Stats { span, .. } => {
                let stats_start = src.find("STATS").unwrap();
                assert_eq!(span.start, stats_start);
                // Span end should be at or past the closing `)`.
                assert!(span.end > stats_start);
            }
            other => panic!("expected Stats, got {other:?}"),
        }
    }

    #[test]
    fn stats_span_covers_group_by_when_present() {
        let src = "events | STATS n = COUNT(*) GROUP BY device";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::Stats { span, .. } => {
                let stats_start = src.find("STATS").unwrap();
                let device_end = src.find("device").unwrap() + "device".len();
                assert_eq!(span.start, stats_start);
                assert_eq!(span.end, device_end);
            }
            other => panic!("expected Stats, got {other:?}"),
        }
    }

    // --- STATS error cases -------------------------------------------

    #[test]
    fn stats_count_distinct_syntax_rejected() {
        // `COUNT(DISTINCT col)` is a parse error per §7.1 — use
        // `COUNT_DISTINCT(col)` instead.
        match crate::parse("events | STATS n = COUNT(DISTINCT user_id)") {
            Err(ParseError::Unexpected {
                found,
                detail,
                expected,
                ..
            }) => {
                assert_eq!(found, "DISTINCT");
                assert_eq!(expected, Expected::Expression);
                assert!(detail.unwrap_or("").contains("COUNT_DISTINCT"));
            }
            other => panic!("expected DISTINCT-inside-agg error, got {other:?}"),
        }
    }

    #[test]
    fn stats_bare_by_without_group_rejected() {
        // `STATS n = COUNT(*) BY device` — bare BY must be rejected.
        match crate::parse("events | STATS n = COUNT(*) BY device") {
            Err(ParseError::Unexpected {
                found,
                expected,
                detail,
                ..
            }) => {
                assert_eq!(found, "BY");
                assert_eq!(expected, Expected::Keyword("GROUP"));
                assert!(detail.unwrap_or("").contains("GROUP BY"));
            }
            other => panic!("expected bare-BY error, got {other:?}"),
        }
    }

    #[test]
    fn stats_missing_alias_errors_on_agg_function() {
        // `STATS COUNT(*)` without `alias =` is a parse error — the
        // parser expects an identifier (the alias) before the `=`.
        // The aggregate function keyword (COUNT) is reserved and so
        // the parser will see it where it expects an identifier and
        // emit `ReservedKeyword`.
        match crate::parse("events | STATS COUNT(*)") {
            Err(ParseError::ReservedKeyword { keyword, .. }) => {
                assert_eq!(keyword, "COUNT");
            }
            other => panic!("expected ReservedKeyword(COUNT) error, got {other:?}"),
        }
    }

    #[test]
    fn stats_missing_equals_errors() {
        // `STATS total COUNT(*)` — missing `=` between alias and agg expr.
        match crate::parse("events | STATS total COUNT(*)") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Punct("="));
            }
            other => panic!("expected Punct(=) error, got {other:?}"),
        }
    }

    #[test]
    fn stats_unknown_function_errors() {
        // `STATS n = MEDIAN(x)` — MEDIAN is not a supported agg function
        // keyword; it would be tokenised as an `Ident("MEDIAN")` which
        // is not an agg keyword.
        match crate::parse("events | STATS n = MEDIAN(x)") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected Expression error for unknown function, got {other:?}"),
        }
    }

    #[test]
    fn stats_group_by_computed_without_alias_errors() {
        // Computed GROUP BY item without `AS alias` is a parse error.
        match crate::parse("events | STATS n = COUNT(*) GROUP BY amount * 2") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("AS"));
                assert!(detail.unwrap_or("").contains("GROUP BY"));
            }
            other => panic!("expected AS error on computed GROUP BY, got {other:?}"),
        }
    }

    #[test]
    fn stats_in_multi_stage_pipeline() {
        // STATS followed by a WHERE stage (post-aggregation filter).
        let stmt = parse_stmt("events | STATS n = COUNT(*) GROUP BY device | WHERE n > 100");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 2);
        assert!(matches!(stages[0], PipelineStage::Stats { .. }));
        assert!(matches!(stages[1], PipelineStage::Where { .. }));
    }

    #[test]
    fn stats_span_does_not_include_trailing_pipe() {
        // The span of a STATS stage must end before the `|` of the next stage.
        let src = "events | STATS n = COUNT(*) | LIMIT 10";
        let stmt = parse_stmt(src);
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 2);
        let stats_span = stages[0].span();
        let pipe_pos = src.rfind('|').unwrap(); // position of the last `|`
        assert!(
            stats_span.end <= pipe_pos,
            "stats span end ({}) should not reach the `|` at {pipe_pos}",
            stats_span.end
        );
    }

    #[test]
    fn stats_agg_item_with_expression_argument() {
        // AVG of a binary expression `amount * rate`.
        let stmt = parse_stmt("orders | STATS wa = AVG(amount * rate)");
        let items = agg_items_of(stages_of(&stmt));
        assert_eq!(items[0].function.text, "avg");
        assert_eq!(items[0].args.len(), 1);
        match &items[0].args[0].node {
            Expr::Binary {
                op: BinaryOp::Multiply,
                ..
            } => {}
            other => panic!("expected Binary Multiply arg, got {other:?}"),
        }
    }

    #[test]
    fn stats_missing_agg_list_errors_on_reserved_keyword() {
        // `STATS GROUP BY device` — no aggregate items before GROUP BY.
        // `parse_agg_item` sees `GROUP` (a reserved keyword) where it
        // expects an identifier (the aggregate alias), so the error is
        // `ReservedKeyword { keyword: "GROUP", role: AliasName }`.
        match crate::parse("events | STATS GROUP BY device") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "GROUP");
                assert_eq!(role, NameRole::AliasName);
            }
            other => panic!("expected ReservedKeyword(GROUP) error, got {other:?}"),
        }
    }

    #[test]
    fn stats_group_by_qualified_column_reference() {
        // GROUP BY accepts a qualified column reference (`table.col`)
        // via the `name` branch of `group_item`. No alias is required
        // because `is_bare_or_qualified_column` returns true for
        // `Expr::Qualified`.
        let stmt = parse_stmt("events | STATS n = COUNT(*) GROUP BY events.device");
        let groups = group_items_of(stages_of(&stmt));
        assert_eq!(groups.len(), 1);
        assert!(groups[0].alias.is_none());
        match &groups[0].expr.node {
            Expr::Qualified { table, column } => {
                assert_eq!(table.text, "events");
                assert_eq!(column.text, "device");
            }
            other => panic!("expected Qualified in GROUP BY, got {other:?}"),
        }
    }
}
