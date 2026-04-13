//! Pipeline stage productions.
//!
//! Design: docs/design/query-language.md §26 (grammar) and
//! docs/design/language/grammar-framework.md §7.
//!
//! This module implements the `|`-separated continuation of a pipeline
//! after the source expression. Implemented verbs (in grammar-section order):
//!
//! - `| WHERE <predicate>` — row filter (§9, TASK-223).
//! - `| SELECT [DISTINCT] <items>` — projection (§10, TASK-223).
//! - `| LIMIT <integer>` — row cap (§15, TASK-223).
//! - `| STATS <agg_list> [GROUP BY <group_list>]` — aggregation (§7, TASK-314).
//! - `| ORDER BY <items>` / `| SORT <items>` — ordering (§15, TASK-315).
//! - `| MATCH (FIRST|ALL) SEQUENCE(…) [modifiers]` — sequence pattern (§4, TASK-313).
//! - `| FUNNEL "(" step_list ")" (WITHIN duration)?` — funnel sugar (§6.1, TASK-316).
//!
//! The grammar lives in §26 (query-language.md). Every other pipeline
//! verb lives in later tasks and produces a
//! `PipelineStage::…`-returning production function alongside these.
//!
//! The module surface is crate-private. [`parse_pipeline_stages`] is
//! called from `crate::parser::parse_pipeline`; outside callers reach
//! pipelines via [`crate::parse`] in `lib.rs`.

#![allow(dead_code)] // TASK-221 / TASK-222 productions reach this module later.

use bqlite_ast::{
    AggItem, Attribute, BracketSpec, EventRef, Expr, Funnel, GroupItem, MatchMode, MatchPattern,
    Name, OrderItem, PipelineStage, Retention, SelectItem, SelectItemKind, Sessionize, SortDir,
    Spanned,
};

use crate::pattern::parse_step_list;

use crate::error::{Expected, NameRole, ParseError};
use crate::expr::parse_expression;
use crate::lex::{token_span, Keyword, TokenKind};
use crate::parser::Parser;
use crate::pattern::{parse_event_ref, parse_match_modifiers, parse_sequence};

/// Parse the `("|" stage)*` tail of a pipeline, returning the ordered
/// stage list. Stops at the first token that is not a `|`. The caller
/// is responsible for the source expression that precedes the tail.
///
/// FUNNEL and RETENTION are terminal stages — if a `|` appears
/// immediately after them the parser emits an error rather than
/// continuing (query-language.md §6.1, §6.3, §25.2).
pub(crate) fn parse_pipeline_stages(p: &mut Parser) -> Result<Vec<PipelineStage>, ParseError> {
    let mut stages = Vec::new();
    while matches!(p.peek_kind(), TokenKind::Pipe) {
        p.bump(); // consume `|`
        let stage = parse_stage(p)?;
        let terminal_name: Option<&'static str> = match &stage {
            PipelineStage::Funnel(_) => Some("FUNNEL"),
            PipelineStage::Retention(_) => Some("RETENTION"),
            _ => None,
        };
        stages.push(stage);
        // Terminal stages cannot be followed by another pipe stage.
        if let Some(name) = terminal_name {
            if matches!(p.peek_kind(), TokenKind::Pipe) {
                let tok = p.peek().clone();
                return Err(ParseError::Unexpected {
                    offset: tok.start,
                    line: tok.line,
                    column: tok.column,
                    expected: Expected::Eof,
                    found: "|".to_string(),
                    detail: Some(if name == "FUNNEL" {
                        "FUNNEL is a terminal stage and cannot be followed by another pipe stage; \
                         write the desugared MATCH + STATS form explicitly if you need downstream operators"
                    } else {
                        "RETENTION is a terminal stage and cannot be followed by another pipe stage; \
                         write the desugared MATCH + STATS form explicitly if you need downstream operators"
                    }),
                });
            }
        }
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
        // `ORDER BY …` and its `SORT …` alias (query-language.md §15).
        TokenKind::Kw(Keyword::Order) | TokenKind::Kw(Keyword::Sort) => parse_order_by_stage(p),
        // `MATCH (FIRST | ALL) SEQUENCE(…) [modifiers]` — sequence pattern (§4).
        TokenKind::Kw(Keyword::Match) => parse_match_stage(p),
        // `FUNNEL(…) (WITHIN duration)?` — terminal funnel sugar (§6.1, TASK-316).
        TokenKind::Kw(Keyword::Funnel) => parse_funnel_stage(p),
        // `RETENTION(entry: …, activity: …, brackets: …)` — terminal retention sugar (§6.3, TASK-420).
        TokenKind::Kw(Keyword::Retention) => parse_retention_stage(p),
        // `SESSIONIZE(gap: …, end: …)` — session assignment (§8, TASK-420).
        TokenKind::Kw(Keyword::Sessionize) => parse_sessionize_stage(p),
        // `ATTRIBUTE(conversion: …, touchpoints: …, window: …, touchpoint_key: …)` —
        // attribution operator (§14.3 / §26 line 1638, TASK-422).
        TokenKind::Kw(Keyword::Attribute) => parse_attribute_stage(p),

        // Every other first token is either a later-wave verb that
        // is not yet implemented, or an error. The error message names
        // `PipelineStage` so the user sees `"expected pipeline stage"`
        // rather than a bare `"unexpected token"`.
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
// ORDER BY
// ----------------------------------------------------------------------

/// `ORDER BY <items>` or `SORT <items>` — §15 / §26 line 1601.
///
/// Both keywords produce an identical `PipelineStage::OrderBy` node.
/// `SORT` is a convenience alias recognised by the parser only — the AST
/// and planner see no distinction (query-language.md §15).
///
/// Grammar:
/// ```text
/// order_op  := ORDER BY order_item ("," order_item)*
///            | SORT    order_item ("," order_item)*
/// order_item := expr (ASC | DESC)?
/// ```
///
/// Default direction is `ASC` when the keyword is absent (§15: "default
/// direction is ascending").
fn parse_order_by_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    // `SORT` is a single keyword; `ORDER BY` is a two-keyword form.
    let start_span = if matches!(p.peek_kind(), TokenKind::Kw(Keyword::Sort)) {
        let tok = p.bump(); // consume SORT
        token_span(&tok)
    } else {
        let tok = p.expect_kw(Keyword::Order)?;
        let start = token_span(&tok);
        // `ORDER` must be followed by `BY` — bare `ORDER` is a syntax
        // error (query-language.md §15). The error message points at the
        // token after `ORDER` so the user sees what was found instead.
        p.expect_kw(Keyword::By)?;
        start
    };

    // At least one order item is required.
    let mut items = Vec::new();
    items.push(parse_order_item(p)?);
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        items.push(parse_order_item(p)?);
    }

    let last_span = items
        .last()
        .expect("loop above pushes at least one item")
        .span;
    let span = start_span.merged(last_span);

    Ok(PipelineStage::OrderBy { items, span })
}

/// Parse one `order_item`: `expr (ASC | DESC)?`.
///
/// The direction keyword is optional; the default is [`SortDir::Asc`]
/// when omitted (query-language.md §15). `NULLS FIRST` / `NULLS LAST`
/// are not part of the v1 grammar (§26 line 1602).
fn parse_order_item(p: &mut Parser) -> Result<OrderItem, ParseError> {
    let expr = parse_expression(p)?;
    let expr_span = expr.span;

    // Optional `ASC` or `DESC` direction keyword. Track the token span
    // so the item span correctly covers keyword-through-direction.
    let (direction, span) = match p.peek_kind() {
        TokenKind::Kw(Keyword::Asc) => {
            let tok = p.bump();
            (SortDir::Asc, expr_span.merged(token_span(&tok)))
        }
        TokenKind::Kw(Keyword::Desc) => {
            let tok = p.bump();
            (SortDir::Desc, expr_span.merged(token_span(&tok)))
        }
        // No direction keyword — default is ASC; span is the expression.
        _ => (SortDir::Asc, expr_span),
    };

    Ok(OrderItem {
        expr,
        direction,
        span,
    })
}

// MATCH
// ----------------------------------------------------------------------

/// `MATCH (FIRST | ALL) SEQUENCE(…) [WITHIN …] [BRACKETS …] [EMIT ALL]`
/// — §4 / §26 line ~1555.
///
/// Grammar:
/// ```text
/// match_op        := MATCH match_mode SEQUENCE "(" step_list ")" match_modifiers
/// match_mode      := FIRST | ALL
/// match_modifiers := (WITHIN (duration | SESSION))?
///                    (BRACKETS CUMULATIVE? "[" duration_list "]")?
///                    (EMIT ALL)?
/// ```
///
/// Delegates `SEQUENCE(…)` and modifier parsing to `crate::pattern`.
/// Emits `PipelineStage::Match { pattern: MatchPattern { … }, span }`.
///
/// Error sites:
/// - Neither `FIRST` nor `ALL` after `MATCH` → `Expected::Keyword("FIRST or ALL")`
/// - Missing `SEQUENCE` after mode → `Expected::Keyword("SEQUENCE")`
/// - Empty `SEQUENCE()` → `Expected::Keyword("step")`
/// - Modifier order violations → `Expected::EndOfModifiers`
fn parse_match_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    // Consume `MATCH` keyword — span anchor (start of the whole stage).
    let match_tok = p.expect_kw(Keyword::Match)?;
    let start_span = token_span(&match_tok);

    // Parse match mode: FIRST or ALL. Both are required; anything else is
    // a parse error (pattern-grammar.md §3.2).
    let base_mode = match p.peek_kind() {
        TokenKind::Kw(Keyword::First) => {
            p.bump();
            MatchMode::First
        }
        TokenKind::Kw(Keyword::All) => {
            p.bump();
            MatchMode::All
        }
        _ => {
            return Err(p.error_unexpected(
                Expected::Keyword("FIRST or ALL"),
                Some("MATCH requires a mode: MATCH FIRST ... or MATCH ALL ..."),
            ));
        }
    };

    // Guard: emit a user-friendly hint when SEQUENCE is missing.
    // `parse_sequence` itself emits `Expected::Keyword("SEQUENCE")` but
    // without a detail message. Intercepting here lets us add the hint
    // from pattern-grammar.md §3.1 before delegating to `parse_sequence`.
    if !matches!(p.peek_kind(), TokenKind::Kw(Keyword::Sequence)) {
        return Err(p.error_unexpected(
            Expected::Keyword("SEQUENCE"),
            Some("MATCH requires SEQUENCE(...) — did you mean MATCH FIRST SEQUENCE(...) ?"),
        ));
    }

    // Parse `SEQUENCE "(" step_list ")"` — delegates to crate::pattern.
    // Errors if `(`, step list, or `)` are missing.
    let (steps, seq_span) = parse_sequence(p)?;

    // Parse optional modifiers in canonical order: WITHIN, BRACKETS, EMIT ALL.
    // `modifier_end` is the span of the last modifier token consumed, or
    // `Span::EMPTY` when no modifiers are present.
    let (window, brackets, emit_all, modifier_end) = parse_match_modifiers(p, base_mode)?;

    // Full stage span: from `MATCH` through the last modifier (or through the
    // closing `)` of SEQUENCE when no modifiers are present).
    // `Span::merged` treats EMPTY as a no-op, so `modifier_end = EMPTY`
    // correctly leaves the span ending at `seq_span`.
    let span = start_span.merged(seq_span).merged(modifier_end);

    let pattern = MatchPattern {
        steps,
        mode: base_mode,
        emit_all,
        window,
        brackets,
        span,
    };

    Ok(PipelineStage::Match { pattern, span })
}

// ----------------------------------------------------------------------
// FUNNEL
// ----------------------------------------------------------------------

/// `FUNNEL "(" step_list ")" (WITHIN duration)?` — §26 line 1570.
///
/// Grammar:
/// ```text
/// funnel_op        := FUNNEL "(" step_list ")" funnel_modifiers
/// funnel_modifiers := (WITHIN duration)?
/// ```
///
/// FUNNEL is **terminal sugar**: it cannot be followed by further pipe
/// stages. The `parse_pipeline_stages` caller enforces this rule after
/// receiving the `PipelineStage::Funnel` value — the stage parser itself
/// does not need to peek ahead past its own production.
///
/// The step list reuses the full MATCH step sub-grammar (`parse_step_list`
/// from `crate::pattern`), so named steps, property constraints, variable
/// bindings, WITHOUT exclusions, alternation, repetition, and IMMEDIATELY
/// are all accepted. The planner desugars the node to
/// `MATCH FIRST … EMIT ALL | STATS …` before type-checking
/// (query-language.md §6.1, planner-pipeline.md §4.3).
///
/// Error cases:
/// - `FUNNEL()` — empty step list → `Expected::Keyword("step")`
/// - `WITHIN SESSION` instead of a duration → explicit detail message;
///   `WITHIN SESSION` is a MATCH-only modifier (query-language.md §4.4).
fn parse_funnel_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let funnel_tok = p.expect_kw(Keyword::Funnel)?;
    let start_span = token_span(&funnel_tok);

    p.expect_punct(&TokenKind::LParen, "(")?;

    // Empty step list is a parse error — at least one step is required.
    if matches!(p.peek_kind(), TokenKind::RParen) {
        return Err(p.error_unexpected(
            Expected::Keyword("step"),
            Some("FUNNEL requires at least one step"),
        ));
    }

    let steps = parse_step_list(p)?;

    let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
    let rparen_span = token_span(&rparen_tok);

    // Optional `WITHIN <duration>`. Note: `WITHIN SESSION` is valid for
    // MATCH but NOT for FUNNEL — the grammar at §26 line 1571 spells the
    // modifier as `(WITHIN duration)?` (no SESSION alternative).
    let window = if p.try_kw(Keyword::Within).is_some() {
        // Reject WITHIN SESSION — SESSION is only valid in MATCH.
        if matches!(p.peek_kind(), TokenKind::Kw(Keyword::Session)) {
            return Err(p.error_unexpected(
                Expected::Literal,
                Some(
                    "FUNNEL WITHIN requires a duration literal (e.g. 7d), not SESSION; \
                     use MATCH … WITHIN SESSION for session-scoped patterns",
                ),
            ));
        }
        // Expect a duration literal. `i64` is `Copy`, so the borrow of
        // the peeked token ends before the mutable `bump()` call (NLL).
        if let TokenKind::Duration(ns) = p.peek().kind {
            let dur_tok = p.bump();
            Some((ns, token_span(&dur_tok)))
        } else {
            return Err(p.error_unexpected(
                Expected::Literal,
                Some("expected a duration literal (e.g. 7d) after WITHIN"),
            ));
        }
    } else {
        None
    };

    // Span: from FUNNEL keyword through the last consumed token.
    let span = match &window {
        Some((_, win_span)) => start_span.merged(*win_span),
        None => start_span.merged(rparen_span),
    };

    Ok(PipelineStage::Funnel(Funnel {
        steps,
        window: window.map(|(ns, _)| ns),
        span,
    }))
}

// ----------------------------------------------------------------------
// RETENTION
// ----------------------------------------------------------------------

/// Parse `| RETENTION(entry: event, activity: event, brackets: [d, …] [, cumulative: bool])`.
///
/// Named arguments are accepted in any order. Required: `entry:`,
/// `activity:`, `brackets:`. Optional: `cumulative:` (default `false`).
/// Duplicate argument names are a parse error.
///
/// RETENTION is terminal: the caller rejects a subsequent `|` stage
/// (query-language.md §25.2).
fn parse_retention_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let ret_tok = p.expect_kw(Keyword::Retention)?;
    let start_span = token_span(&ret_tok);

    p.expect_punct(&TokenKind::LParen, "(")?;

    let mut entry: Option<EventRef> = None;
    let mut activity: Option<EventRef> = None;
    let mut brackets_durations: Option<Vec<i64>> = None;
    let mut brackets_span: Option<bqlite_ast::Span> = None;
    let mut cumulative: Option<bool> = None;

    let mut first = true;
    loop {
        if matches!(p.peek_kind(), TokenKind::RParen | TokenKind::Eof) {
            break;
        }
        if !first {
            p.expect_punct(&TokenKind::Comma, ",")?;
            // After the comma the loop-top guard re-checks for `)`, which breaks
            // out of the loop. A trailing comma (e.g. `brackets: [7d],)`) is
            // therefore caught by the downstream missing-arg checks, not here.
        }
        first = false;

        // Dispatch on the argument keyword or identifier.
        match p.peek_kind().clone() {
            TokenKind::Ident(ref name) if name == "entry" => {
                if entry.is_some() {
                    return Err(p.error_unexpected(
                        Expected::Keyword("activity:"),
                        Some("duplicate `entry:` argument — each argument appears exactly once in RETENTION"),
                    ));
                }
                p.bump(); // consume "entry"
                p.expect_punct(&TokenKind::Colon, ":")?;
                entry = Some(parse_event_ref(p)?);
            }
            TokenKind::Ident(ref name) if name == "activity" => {
                if activity.is_some() {
                    return Err(p.error_unexpected(
                        Expected::Keyword("brackets:"),
                        Some("duplicate `activity:` argument — each argument appears exactly once in RETENTION"),
                    ));
                }
                p.bump(); // consume "activity"
                p.expect_punct(&TokenKind::Colon, ":")?;
                activity = Some(parse_event_ref(p)?);
            }
            TokenKind::Kw(Keyword::Brackets) => {
                if brackets_durations.is_some() {
                    return Err(p.error_unexpected(
                        Expected::Keyword("cumulative:"),
                        Some("duplicate `brackets:` argument — each argument appears exactly once in RETENTION"),
                    ));
                }
                let brack_tok = p.bump(); // consume "brackets"
                let brack_start = token_span(&brack_tok);
                p.expect_punct(&TokenKind::Colon, ":")?;
                let (durations, end_span) = parse_bracket_duration_list(p)?;
                brackets_durations = Some(durations);
                brackets_span = Some(brack_start.merged(end_span));
            }
            TokenKind::Kw(Keyword::Cumulative) => {
                if cumulative.is_some() {
                    return Err(p.error_unexpected(
                        Expected::Punct(")"),
                        Some("duplicate `cumulative:` argument — each argument appears exactly once in RETENTION"),
                    ));
                }
                p.bump(); // consume "cumulative"
                p.expect_punct(&TokenKind::Colon, ":")?;
                cumulative = Some(parse_bool_literal(p)?);
            }
            _ => {
                return Err(p.error_unexpected(
                    Expected::Keyword("entry:"),
                    Some("unknown RETENTION argument; expected entry, activity, brackets, or cumulative"),
                ));
            }
        }
    }

    // Validate required arguments are present (point at `)` or EOF for the
    // error location — callers see which arg is missing from the detail).
    let entry = entry.ok_or_else(|| {
        p.error_unexpected(
            Expected::Keyword("entry:"),
            Some("missing required `entry:` argument in RETENTION"),
        )
    })?;
    let activity = activity.ok_or_else(|| {
        p.error_unexpected(
            Expected::Keyword("activity:"),
            Some("missing required `activity:` argument in RETENTION"),
        )
    })?;
    let durations = brackets_durations.ok_or_else(|| {
        p.error_unexpected(
            Expected::Keyword("brackets:"),
            Some("missing required `brackets:` argument in RETENTION"),
        )
    })?;

    let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
    let rparen_span = token_span(&rparen_tok);
    let span = start_span.merged(rparen_span);

    Ok(PipelineStage::Retention(Retention {
        entry,
        activity,
        brackets: BracketSpec {
            durations,
            cumulative: cumulative.unwrap_or(false),
            span: brackets_span.unwrap_or(bqlite_ast::Span::EMPTY),
        },
        span,
    }))
}

/// Parse `[d1, d2, …]` — the bracket duration list for RETENTION.
///
/// Returns the parsed durations (nanoseconds) and the span covering the
/// `[…]` delimiters. At least one duration is required.
fn parse_bracket_duration_list(p: &mut Parser) -> Result<(Vec<i64>, bqlite_ast::Span), ParseError> {
    let lbracket_tok = p.expect_punct(&TokenKind::LBracket, "[")?;
    let lbracket_span = token_span(&lbracket_tok);

    if matches!(p.peek_kind(), TokenKind::RBracket) {
        return Err(p.error_unexpected(
            Expected::Literal,
            Some("RETENTION brackets list requires at least one duration (e.g. [7d, 14d, 30d])"),
        ));
    }

    let mut durations = Vec::new();
    loop {
        let tok = p.peek();
        if let TokenKind::Duration(ns) = tok.kind {
            p.bump();
            durations.push(ns);
        } else {
            return Err(p.error_unexpected(
                Expected::Literal,
                Some("expected a duration literal (e.g. 7d) in RETENTION brackets list"),
            ));
        }
        if p.try_kind(&TokenKind::Comma).is_none() {
            break;
        }
    }

    let rbracket_tok = p.expect_punct(&TokenKind::RBracket, "]")?;
    let rbracket_span = token_span(&rbracket_tok);

    Ok((durations, lbracket_span.merged(rbracket_span)))
}

/// Parse a bare `true` or `false` boolean literal.
fn parse_bool_literal(p: &mut Parser) -> Result<bool, ParseError> {
    match p.peek().kind {
        TokenKind::Bool(b) => {
            p.bump();
            Ok(b)
        }
        _ => Err(p.error_unexpected(Expected::Literal, Some("expected `true` or `false`"))),
    }
}

// ----------------------------------------------------------------------
// SESSIONIZE
// ----------------------------------------------------------------------

/// Parse `| SESSIONIZE(gap: duration [, end: event_ref_list])`.
///
/// Named arguments are accepted in any order. Required: `gap:`.
/// Optional: `end:` (accepts a single event ref or a parenthesised
/// list). Duplicate argument names are a parse error. Duplicate event
/// names within an `end:` list are also a parse error.
///
/// Per sessionize.md §5.4, `end: (logout, logout)` is rejected.
fn parse_sessionize_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let sess_tok = p.expect_kw(Keyword::Sessionize)?;
    let start_span = token_span(&sess_tok);

    p.expect_punct(&TokenKind::LParen, "(")?;

    let mut gap_ns: Option<i64> = None;
    let mut end_events: Option<Vec<EventRef>> = None;

    let mut first = true;
    loop {
        if matches!(p.peek_kind(), TokenKind::RParen | TokenKind::Eof) {
            break;
        }
        if !first {
            p.expect_punct(&TokenKind::Comma, ",")?;
        }
        first = false;

        match p.peek_kind().clone() {
            TokenKind::Ident(ref name) if name == "gap" => {
                if gap_ns.is_some() {
                    return Err(p.error_unexpected(
                        Expected::Keyword("end:"),
                        Some("duplicate `gap:` argument — each argument appears exactly once in SESSIONIZE"),
                    ));
                }
                p.bump(); // consume "gap"
                p.expect_punct(&TokenKind::Colon, ":")?;
                let tok = p.peek();
                if let TokenKind::Duration(ns) = tok.kind {
                    p.bump();
                    gap_ns = Some(ns);
                } else {
                    return Err(p.error_unexpected(
                        Expected::Literal,
                        Some("expected a duration literal (e.g. 30m) after `gap:`"),
                    ));
                }
            }
            TokenKind::Kw(Keyword::End) => {
                if end_events.is_some() {
                    return Err(p.error_unexpected(
                        Expected::Punct(")"),
                        Some("duplicate `end:` argument — each argument appears exactly once in SESSIONIZE"),
                    ));
                }
                p.bump(); // consume "end"
                p.expect_punct(&TokenKind::Colon, ":")?;
                end_events = Some(parse_end_event_list(p)?);
            }
            _ => {
                return Err(p.error_unexpected(
                    Expected::Keyword("gap:"),
                    Some("unknown SESSIONIZE argument; expected gap or end"),
                ));
            }
        }
    }

    // Validate required `gap:` argument.
    let gap_ns = gap_ns.ok_or_else(|| {
        p.error_unexpected(
            Expected::Keyword("gap:"),
            Some("missing required `gap:` argument in SESSIONIZE"),
        )
    })?;

    let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
    let rparen_span = token_span(&rparen_tok);
    let span = start_span.merged(rparen_span);

    Ok(PipelineStage::Sessionize(Sessionize {
        gap: gap_ns,
        end: end_events,
        span,
    }))
}

/// Parse `event_ref_list = event_ref | "(" event_ref ("," event_ref)* ")"`.
///
/// Duplicate event names within a list are rejected (sessionize.md §5.4).
/// Returns a `Vec<EventRef>` with length ≥ 1.
fn parse_end_event_list(p: &mut Parser) -> Result<Vec<EventRef>, ParseError> {
    // Parenthesised list form: `(logout, timeout, session_end)`.
    if p.try_kind(&TokenKind::LParen).is_some() {
        let mut events: Vec<EventRef> = Vec::new();

        // At least one event ref is required inside the parens.
        if matches!(p.peek_kind(), TokenKind::RParen) {
            return Err(p.error_unexpected(
                Expected::EventRef,
                Some("SESSIONIZE end: list requires at least one event name"),
            ));
        }

        loop {
            let ev = parse_event_ref(p)?;
            // Reject duplicate (table, event) pairs (case-sensitive).
            // Two refs are duplicates only when both the table qualifier AND the
            // event name are identical. `events.logout` and `purchases.logout`
            // are different qualified refs and are not considered duplicates here.
            let ev_table = ev.table.as_ref().map(|t| t.text.as_str());
            if events.iter().any(|e| {
                e.event.text == ev.event.text
                    && e.table.as_ref().map(|t| t.text.as_str()) == ev_table
            }) {
                return Err(ParseError::Unexpected {
                    offset: ev.span.start,
                    line: ev.span.line,
                    column: ev.span.column,
                    expected: Expected::Punct(","),
                    found: ev.event.text.to_string(),
                    detail: Some(
                        "duplicate event name in SESSIONIZE end: list — each event type must appear at most once",
                    ),
                });
            }
            events.push(ev);
            if p.try_kind(&TokenKind::Comma).is_none() {
                break;
            }
        }

        p.expect_punct(&TokenKind::RParen, ")")?;
        Ok(events)
    } else {
        // Single event ref (no parentheses).
        Ok(vec![parse_event_ref(p)?])
    }
}

// ----------------------------------------------------------------------
// ATTRIBUTE
// ----------------------------------------------------------------------

/// `ATTRIBUTE "(" key ":" value "," … ")"` — attribution operator.
///
/// Grammar (§14.3 / §26 line 1638):
/// ```text
/// attribute_op     := ATTRIBUTE "(" "conversion"     ":" event_ref_list ","
///                                   "touchpoints"    ":" event_ref_list ","
///                                   "window"         ":" duration       ","
///                                   "touchpoint_key" ":" expr           ")"
/// event_ref_list   := event_ref | "(" event_ref ("," event_ref)* ")"
/// ```
///
/// All four parameters are **required** and may appear in any order.
/// The grammar above shows the canonical order for documentation; the parser
/// accepts any permutation so users can write parameters in their preferred order
/// without memorising a fixed sequence. A trailing comma before `)` is accepted.
///
/// **Diagnostics emitted here (all halt-on-first):**
///
/// | Condition | Error |
/// |---|---|
/// | Missing required parameter | `Expected::Keyword("…")` pointing at `)` |
/// | Duplicate parameter key | `Expected::Keyword("…")` pointing at duplicate key token |
/// | Unknown parameter key | `Expected::Keyword("…")` pointing at the unknown key token |
/// | Duplicate event in `conversion:` list | `Expected::EventRef` pointing at duplicate |
/// | Duplicate event in `touchpoints:` list | `Expected::EventRef` pointing at duplicate |
/// | `window:` value is not a duration literal | `Expected::Literal` pointing at the bad token |
fn parse_attribute_stage(p: &mut Parser) -> Result<PipelineStage, ParseError> {
    let attr_tok = p.expect_kw(Keyword::Attribute)?;
    let start_span = token_span(&attr_tok);

    p.expect_punct(&TokenKind::LParen, "(")?;

    // Accumulate the four required parameters — each must appear exactly once.
    let mut conversion: Option<Vec<EventRef>> = None;
    let mut touchpoints: Option<Vec<EventRef>> = None;
    let mut window: Option<i64> = None;
    let mut touchpoint_key: Option<Spanned<Expr>> = None;

    let mut first = true;
    while !matches!(p.peek_kind(), TokenKind::RParen | TokenKind::Eof) {
        if !first {
            p.expect_punct(&TokenKind::Comma, ",")?;
            // Allow trailing comma: stop if `)` follows the comma.
            if matches!(p.peek_kind(), TokenKind::RParen) {
                break;
            }
        }
        first = false;

        // Parse the parameter key — must be a bare identifier (not a keyword).
        let key_tok = p.peek().clone();
        let key_span = token_span(&key_tok);
        let (key, _) = p.expect_ident(NameRole::OptionKey)?;
        p.expect_punct(&TokenKind::Colon, ":")?;

        match key.as_str() {
            "conversion" => {
                if conversion.is_some() {
                    return Err(ParseError::Unexpected {
                        offset: key_span.start,
                        line: key_span.line,
                        column: key_span.column,
                        expected: Expected::Keyword("conversion"),
                        found: key,
                        detail: Some("duplicate `conversion:` parameter in ATTRIBUTE"),
                    });
                }
                let dup_msg = "duplicate event type in ATTRIBUTE `conversion:` list; \
                               each event type may appear at most once";
                conversion = Some(parse_attr_event_ref_list(p, dup_msg)?);
            }
            "touchpoints" => {
                if touchpoints.is_some() {
                    return Err(ParseError::Unexpected {
                        offset: key_span.start,
                        line: key_span.line,
                        column: key_span.column,
                        expected: Expected::Keyword("touchpoints"),
                        found: key,
                        detail: Some("duplicate `touchpoints:` parameter in ATTRIBUTE"),
                    });
                }
                let dup_msg = "duplicate event type in ATTRIBUTE `touchpoints:` list; \
                               each event type may appear at most once";
                touchpoints = Some(parse_attr_event_ref_list(p, dup_msg)?);
            }
            "window" => {
                if window.is_some() {
                    return Err(ParseError::Unexpected {
                        offset: key_span.start,
                        line: key_span.line,
                        column: key_span.column,
                        expected: Expected::Keyword("window"),
                        found: key,
                        detail: Some("duplicate `window:` parameter in ATTRIBUTE"),
                    });
                }
                // The value must be a duration literal (e.g. `30d`).
                if let TokenKind::Duration(ns) = p.peek().kind {
                    p.bump();
                    window = Some(ns);
                } else {
                    return Err(p.error_unexpected(
                        Expected::Literal,
                        Some(
                            "ATTRIBUTE `window:` requires a duration literal \
                             (e.g. 30d, 7d, 1h); found a non-duration token",
                        ),
                    ));
                }
            }
            "touchpoint_key" => {
                if touchpoint_key.is_some() {
                    return Err(ParseError::Unexpected {
                        offset: key_span.start,
                        line: key_span.line,
                        column: key_span.column,
                        expected: Expected::Keyword("touchpoint_key"),
                        found: key,
                        detail: Some("duplicate `touchpoint_key:` parameter in ATTRIBUTE"),
                    });
                }
                touchpoint_key = Some(parse_expression(p)?);
            }
            _ => {
                return Err(ParseError::Unexpected {
                    offset: key_span.start,
                    line: key_span.line,
                    column: key_span.column,
                    expected: Expected::Keyword(
                        "conversion, touchpoints, window, or touchpoint_key",
                    ),
                    found: key,
                    detail: Some(
                        "unknown ATTRIBUTE parameter; expected one of: \
                         conversion, touchpoints, window, touchpoint_key",
                    ),
                });
            }
        }
    }

    // All four parameters are required — report missing ones before consuming `)`.
    // At this point the cursor is at `)` (or Eof), so `error_unexpected` points there.
    if conversion.is_none() {
        return Err(p.error_unexpected(
            Expected::Keyword("conversion"),
            Some("ATTRIBUTE requires a `conversion:` parameter (missing from the argument list)"),
        ));
    }
    if touchpoints.is_none() {
        return Err(p.error_unexpected(
            Expected::Keyword("touchpoints"),
            Some("ATTRIBUTE requires a `touchpoints:` parameter (missing from the argument list)"),
        ));
    }
    if window.is_none() {
        return Err(p.error_unexpected(
            Expected::Keyword("window"),
            Some("ATTRIBUTE requires a `window:` parameter (missing from the argument list)"),
        ));
    }
    if touchpoint_key.is_none() {
        return Err(p.error_unexpected(
            Expected::Keyword("touchpoint_key"),
            Some(
                "ATTRIBUTE requires a `touchpoint_key:` parameter (missing from the argument list)",
            ),
        ));
    }

    let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
    let end_span = token_span(&rparen_tok);
    let span = start_span.merged(end_span);

    // All four are guaranteed Some by the checks above; unwrap is infallible.
    Ok(PipelineStage::Attribute(Attribute {
        conversion: conversion.unwrap(),
        touchpoints: touchpoints.unwrap(),
        window: window.unwrap(),
        touchpoint_key: touchpoint_key.unwrap(),
        span,
    }))
}

/// Parse `event_ref | "(" event_ref ("," event_ref)* ")"` for an ATTRIBUTE
/// `conversion:` or `touchpoints:` value.
///
/// `dup_detail` is the `ParseError::Unexpected::detail` string to include
/// when a duplicate event ref is found within the list.
///
/// Returns a `Vec<EventRef>` of length ≥ 1.
fn parse_attr_event_ref_list(
    p: &mut Parser,
    dup_detail: &'static str,
) -> Result<Vec<EventRef>, ParseError> {
    if matches!(p.peek_kind(), TokenKind::LParen) {
        // Parenthesised list: "(" event_ref ("," event_ref)* ")"
        p.bump(); // consume `(`

        // At least one event ref is required inside the parens.
        let first = parse_event_ref(p)?;
        let mut refs = vec![first];

        while matches!(p.peek_kind(), TokenKind::Comma) {
            p.bump(); // consume `,`
            refs.push(parse_event_ref(p)?);
        }

        p.expect_punct(&TokenKind::RParen, ")")?;

        // Validate: no duplicate (table, event) pairs within this list.
        check_no_duplicate_event_refs(&refs, dup_detail)?;

        Ok(refs)
    } else {
        // Single bare event ref — no duplicate check needed (list of length 1).
        let er = parse_event_ref(p)?;
        Ok(vec![er])
    }
}

/// Validate that no two `EventRef`s in `refs` share the same `(table, event)`
/// key. Comparison is case-sensitive (identifier semantics per §2.2).
///
/// `dup_detail` is the `ParseError::Unexpected::detail` string to use when a
/// duplicate is found — the caller supplies the parameter-specific message.
///
/// On the first duplicate found, returns a `ParseError::Unexpected` pointing at
/// the duplicate entry's span.
fn check_no_duplicate_event_refs(
    refs: &[EventRef],
    dup_detail: &'static str,
) -> Result<(), ParseError> {
    use std::collections::HashSet;
    // Set of (table_text, event_text) keys seen so far.
    let mut seen: HashSet<(Option<&str>, &str)> = HashSet::new();

    for er in refs {
        let key = (
            er.table.as_ref().map(|t| t.text.as_str()),
            er.event.text.as_str(),
        );
        if !seen.insert(key) {
            // Point the error at the duplicate EventRef's own span.
            let dup_span = er.span;
            return Err(ParseError::Unexpected {
                offset: dup_span.start,
                line: dup_span.line,
                column: dup_span.column,
                expected: Expected::EventRef,
                found: er.event.text.clone(),
                detail: Some(dup_detail),
            });
        }
    }
    Ok(())
}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::{BinaryOp, CompareOp, Literal, OrderItem, SortDir, Spanned, Statement};

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

    // --- ORDER BY -----------------------------------------------------

    /// Helper: extract OrderItems from the first OrderBy stage.
    fn order_items_of(stages: &[PipelineStage]) -> &[OrderItem] {
        match &stages[0] {
            PipelineStage::OrderBy { items, .. } => items.as_slice(),
            other => panic!("expected OrderBy, got {other:?}"),
        }
    }

    #[test]
    fn order_by_single_column_default_asc() {
        // Default direction is ASC when no keyword is supplied (§15).
        let stmt = parse_stmt("events | ORDER BY amount");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].direction, SortDir::Asc);
        match &items[0].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "amount"),
            other => panic!("expected Column, got {other:?}"),
        }
    }

    #[test]
    fn order_by_explicit_asc() {
        let stmt = parse_stmt("events | ORDER BY amount ASC");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].direction, SortDir::Asc);
    }

    #[test]
    fn order_by_explicit_desc() {
        let stmt = parse_stmt("events | ORDER BY amount DESC");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].direction, SortDir::Desc);
    }

    #[test]
    fn order_by_mixed_directions() {
        // `ORDER BY device ASC, amount DESC` — multiple items, mixed direction.
        let stmt = parse_stmt("events | ORDER BY device ASC, amount DESC");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].direction, SortDir::Asc);
        assert_eq!(items[1].direction, SortDir::Desc);
        match &items[0].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "device"),
            other => panic!("expected Column[0], got {other:?}"),
        }
        match &items[1].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "amount"),
            other => panic!("expected Column[1], got {other:?}"),
        }
    }

    #[test]
    fn order_by_multiple_items_default_directions() {
        // All directions absent — all default to ASC.
        let stmt = parse_stmt("events | ORDER BY a, b, c");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 3);
        for item in items {
            assert_eq!(item.direction, SortDir::Asc);
        }
    }

    #[test]
    fn sort_alias_produces_identical_stage_to_order_by() {
        // `SORT col` is the alias for `ORDER BY col` — both produce
        // `PipelineStage::OrderBy` (query-language.md §15).
        let stmt_order = parse_stmt("events | ORDER BY amount DESC");
        let stmt_sort = parse_stmt("events | SORT amount DESC");

        let items_order = order_items_of(stages_of(&stmt_order));
        let items_sort = order_items_of(stages_of(&stmt_sort));

        assert_eq!(items_order.len(), items_sort.len());
        assert_eq!(items_order[0].direction, items_sort[0].direction);
        match (&items_order[0].expr.node, &items_sort[0].expr.node) {
            (Expr::Column(a), Expr::Column(b)) => assert_eq!(a.text, b.text),
            other => panic!("expression mismatch: {other:?}"),
        }
    }

    #[test]
    fn sort_alias_single_column_default_asc() {
        // `SORT col` without direction keyword defaults to ASC.
        let stmt = parse_stmt("events | SORT user_id");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].direction, SortDir::Asc);
        match &items[0].expr.node {
            Expr::Column(c) => assert_eq!(c.text, "user_id"),
            other => panic!("expected Column, got {other:?}"),
        }
    }

    #[test]
    fn sort_alias_multiple_items() {
        let stmt = parse_stmt("events | SORT device ASC, amount DESC");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].direction, SortDir::Asc);
        assert_eq!(items[1].direction, SortDir::Desc);
    }

    #[test]
    fn order_by_case_insensitive() {
        // Parser accepts keywords case-insensitively (§2.2).
        let stmt = parse_stmt("events | order by amount desc");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items[0].direction, SortDir::Desc);
    }

    #[test]
    fn sort_alias_case_insensitive() {
        let stmt = parse_stmt("events | sort amount asc");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items[0].direction, SortDir::Asc);
    }

    #[test]
    fn order_by_span_covers_keyword_through_last_item() {
        let src = "events | ORDER BY amount DESC";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::OrderBy { span, .. } => {
                let order_start = src.find("ORDER").unwrap();
                let desc_end = src.find("DESC").unwrap() + "DESC".len();
                assert_eq!(span.start, order_start);
                assert_eq!(span.end, desc_end);
            }
            other => panic!("expected OrderBy, got {other:?}"),
        }
    }

    #[test]
    fn order_by_bare_without_by_errors() {
        // `ORDER amount` — missing the `BY` keyword. The parser expects
        // `BY` after `ORDER` and emits a keyword error.
        match crate::parse("events | ORDER amount") {
            Err(ParseError::Unexpected { .. }) | Err(ParseError::ReservedKeyword { .. }) => {}
            other => panic!("expected parse error for ORDER without BY, got {other:?}"),
        }
    }

    #[test]
    fn order_by_missing_expression_errors() {
        match crate::parse("events | ORDER BY") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn sort_missing_expression_errors() {
        match crate::parse("events | SORT") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Expression);
            }
            other => panic!("expected UnexpectedEof/Expression, got {other:?}"),
        }
    }

    #[test]
    fn order_by_in_multi_stage_pipeline() {
        // `STATS n = COUNT(*) GROUP BY device | ORDER BY n DESC | LIMIT 10`
        let stmt =
            parse_stmt("events | STATS n = COUNT(*) GROUP BY device | ORDER BY n DESC | LIMIT 10");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 3);
        assert!(matches!(stages[0], PipelineStage::Stats { .. }));
        assert!(matches!(stages[1], PipelineStage::OrderBy { .. }));
        assert!(matches!(stages[2], PipelineStage::Limit { .. }));
    }

    #[test]
    fn order_by_with_expression_not_just_column() {
        // ORDER BY can sort on any expression, not just bare columns
        // (§26 line 1602: `order_item := expr (ASC | DESC)?`).
        let stmt = parse_stmt("events | ORDER BY amount * 2 DESC");
        let items = order_items_of(stages_of(&stmt));
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].direction, SortDir::Desc);
        match &items[0].expr.node {
            Expr::Binary {
                op: BinaryOp::Multiply,
                ..
            } => {}
            other => panic!("expected Binary Multiply expression in ORDER BY, got {other:?}"),
        }
    }

    #[test]
    fn order_by_trailing_comma_errors() {
        // `ORDER BY amount,` — trailing comma must be a parse error.
        // `parse_expression` errors when it sees EOF or `|` after the comma.
        match crate::parse("events | ORDER BY amount,") {
            Err(ParseError::UnexpectedEof { .. }) | Err(ParseError::Unexpected { .. }) => {}
            other => panic!("expected trailing-comma error in ORDER BY, got {other:?}"),
        }
    }

    #[test]
    fn sort_alias_span_covers_keyword_through_last_item() {
        // For the SORT form, the stage span must start at the `S` of
        // `SORT` and end at the last token consumed (the direction keyword
        // or the final expression token, whichever is last).
        let src = "events | SORT amount DESC";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::OrderBy { span, .. } => {
                let sort_start = src.find("SORT").unwrap();
                let desc_end = src.find("DESC").unwrap() + "DESC".len();
                assert_eq!(span.start, sort_start);
                assert_eq!(span.end, desc_end);
            }
            other => panic!("expected OrderBy, got {other:?}"),
        }
    }

    // --- MATCH --------------------------------------------------------

    use bqlite_ast::{MatchMode, MatchWindow};

    /// Extract the `MatchPattern` from the first stage (must be `Match`).
    fn match_pattern_of(stages: &[PipelineStage]) -> &bqlite_ast::MatchPattern {
        match &stages[0] {
            PipelineStage::Match { pattern, .. } => pattern,
            other => panic!("expected Match stage, got {other:?}"),
        }
    }

    // --- FUNNEL -------------------------------------------------------

    /// Extract the `Funnel` from a single-stage pipeline, panicking on mismatch.
    fn funnel_of(stmt: &Statement) -> &bqlite_ast::Funnel {
        let stages = stages_of(stmt);
        assert_eq!(stages.len(), 1, "expected exactly one pipeline stage");
        match &stages[0] {
            PipelineStage::Funnel(f) => f,
            other => panic!("expected Funnel stage, got {other:?}"),
        }
    }

    #[test]
    fn match_first_single_step() {
        // `MATCH FIRST SEQUENCE(signup)` — one step, no modifiers.
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(signup)");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::First);
        assert_eq!(pat.steps.len(), 1);
        assert!(pat.window.is_none());
        assert!(pat.brackets.is_none());
    }

    #[test]
    fn match_all_mode_multi_step() {
        // `MATCH ALL SEQUENCE(a THEN b THEN c)` — three steps.
        let stmt = parse_stmt("events | MATCH ALL SEQUENCE(a THEN b THEN c)");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::All);
        assert_eq!(pat.steps.len(), 3);
    }

    #[test]
    fn match_first_with_within_duration() {
        // `MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 7d`
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 7d");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::First);
        assert_eq!(pat.steps.len(), 2);
        assert_eq!(
            pat.window,
            Some(MatchWindow::Within(7 * 24 * 3_600_000_000_000))
        );
        assert!(pat.brackets.is_none());
    }

    #[test]
    fn match_first_with_within_session() {
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(a) WITHIN SESSION");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.window, Some(MatchWindow::WithinSession));
    }

    #[test]
    fn match_first_emit_all_sets_flag() {
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(signup THEN purchase) EMIT ALL");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::First);
        assert!(pat.emit_all);
        assert!(pat.window.is_none());
    }

    #[test]
    fn match_first_with_within_and_emit_all() {
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(signup) WITHIN 30d EMIT ALL");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::First);
        assert!(pat.emit_all);
        assert_eq!(
            pat.window,
            Some(MatchWindow::Within(30 * 24 * 3_600_000_000_000))
        );
    }

    #[test]
    fn match_all_without_emit_all_stays_all() {
        let stmt = parse_stmt("events | MATCH ALL SEQUENCE(a THEN b)");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::All);
        assert!(!pat.emit_all);
    }

    #[test]
    fn match_all_emit_all_is_allowed() {
        let stmt = parse_stmt("events | MATCH ALL SEQUENCE(signup) EMIT ALL");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::All);
        assert!(pat.emit_all);
    }

    #[test]
    fn match_missing_mode_errors() {
        // `MATCH SEQUENCE(signup)` — missing FIRST or ALL.
        // The parser sees SEQUENCE where it expects FIRST/ALL and
        // emits `Expected::Keyword("FIRST or ALL")`.
        match crate::parse("events | MATCH SEQUENCE(signup)") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("FIRST or ALL"));
            }
            other => panic!("expected Unexpected(FIRST or ALL) error, got {other:?}"),
        }
    }

    #[test]
    fn match_missing_sequence_keyword_errors() {
        // `MATCH FIRST signup` — missing SEQUENCE.
        // The parser intercepts this and emits a user-friendly detail hint
        // per pattern-grammar.md §3.1.
        match crate::parse("events | MATCH FIRST signup") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("SEQUENCE"));
                assert!(
                    detail
                        .unwrap_or("")
                        .contains("MATCH requires SEQUENCE(...)"),
                    "detail message should contain hint about SEQUENCE"
                );
            }
            other => panic!("expected Unexpected(SEQUENCE) error, got {other:?}"),
        }
    }

    #[test]
    fn match_empty_sequence_errors() {
        // `MATCH FIRST SEQUENCE()` — empty step list is rejected.
        match crate::parse("events | MATCH FIRST SEQUENCE()") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("step"));
            }
            other => panic!("expected empty-step-list error, got {other:?}"),
        }
    }

    #[test]
    fn match_emit_all_before_within_is_out_of_order_error() {
        // Modifiers must appear in canonical order: WITHIN then EMIT ALL.
        match crate::parse("events | MATCH FIRST SEQUENCE(a) EMIT ALL WITHIN 7d") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EndOfModifiers);
                assert!(detail
                    .unwrap_or("")
                    .contains("WITHIN must appear before EMIT ALL"));
            }
            other => panic!("expected out-of-order modifier error, got {other:?}"),
        }
    }

    #[test]
    fn match_brackets_after_emit_all_is_out_of_order_error() {
        // `EMIT ALL BRACKETS [1d, 7d]` — BRACKETS must appear before EMIT ALL
        // per the canonical modifier order in pattern-grammar.md §3.11.
        match crate::parse("events | MATCH FIRST SEQUENCE(a) EMIT ALL BRACKETS [1d, 7d]") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EndOfModifiers);
                assert!(detail
                    .unwrap_or("")
                    .contains("BRACKETS must appear before EMIT ALL"));
            }
            other => panic!("expected out-of-order BRACKETS error, got {other:?}"),
        }
    }

    #[test]
    fn match_case_insensitive_keywords() {
        // All keywords are case-insensitive per §2.2.
        let stmt = parse_stmt("events | match first sequence(signup then purchase)");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.mode, MatchMode::First);
        assert_eq!(pat.steps.len(), 2);
    }

    #[test]
    fn match_span_starts_at_match_keyword() {
        // The stage span must begin at the `M` of `MATCH`.
        let src = "events | MATCH FIRST SEQUENCE(signup)";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::Match { span, .. } => {
                let match_start = src.find("MATCH").unwrap();
                assert_eq!(span.start, match_start);
                // Span must end at or after the closing `)`.
                let rparen = src.rfind(')').unwrap();
                assert!(span.end > match_start);
                assert_eq!(span.end, rparen + 1);
            }
            other => panic!("expected Match, got {other:?}"),
        }
    }

    #[test]
    fn match_span_extends_through_modifier_when_present() {
        // When WITHIN is present the span should extend past the sequence `)`.
        let src = "events | MATCH FIRST SEQUENCE(signup) WITHIN 7d";
        let stmt = parse_stmt(src);
        match &stages_of(&stmt)[0] {
            PipelineStage::Match { span, .. } => {
                // Span must end at end of `7d`, not at `)` of SEQUENCE.
                let rparen = src.rfind(')').unwrap();
                assert!(
                    span.end > rparen + 1,
                    "span.end ({}) should extend past `)` at {}",
                    span.end,
                    rparen + 1
                );
            }
            other => panic!("expected Match, got {other:?}"),
        }
    }

    #[test]
    fn match_in_multi_stage_pipeline() {
        // MATCH followed by STATS — common Wave 3 pipeline shape.
        let stmt =
            parse_stmt("events | MATCH FIRST SEQUENCE(signup THEN purchase) | STATS n = COUNT(*)");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 2);
        assert!(matches!(stages[0], PipelineStage::Match { .. }));
        assert!(matches!(stages[1], PipelineStage::Stats { .. }));
    }

    #[test]
    fn match_step_name_is_parsed() {
        // Named step: `myname: event_type`.
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(s: signup THEN p: purchase)");
        let pat = match_pattern_of(stages_of(&stmt));
        assert_eq!(pat.steps.len(), 2);
        assert_eq!(pat.steps[0].name.as_ref().unwrap().text, "s");
        assert_eq!(pat.steps[1].name.as_ref().unwrap().text, "p");
    }

    #[test]
    fn match_arrow_separator_is_equivalent_to_then() {
        // `->` is an alias for `THEN` in step separators (pattern-grammar.md §3.4).
        let stmt_then = parse_stmt("events | MATCH FIRST SEQUENCE(a THEN b)");
        let stmt_arrow = parse_stmt("events | MATCH FIRST SEQUENCE(a -> b)");
        let pat_then = match_pattern_of(stages_of(&stmt_then));
        let pat_arrow = match_pattern_of(stages_of(&stmt_arrow));
        assert_eq!(pat_then.steps.len(), pat_arrow.steps.len());
        assert_eq!(pat_then.mode, pat_arrow.mode);
    }

    #[test]
    fn match_with_brackets_modifier() {
        let stmt = parse_stmt("events | MATCH FIRST SEQUENCE(signup) BRACKETS [1d, 7d, 30d]");
        let pat = match_pattern_of(stages_of(&stmt));
        let b = pat.brackets.as_ref().expect("brackets should be Some");
        assert_eq!(b.durations.len(), 3);
        assert!(!b.cumulative);
        assert!(pat.window.is_none());
    }

    // --- FUNNEL -------------------------------------------------------

    #[test]
    fn funnel_two_step_no_window() {
        // `events | FUNNEL(signup THEN purchase)` — basic 2-step without WITHIN.
        let stmt = parse_stmt("events | FUNNEL(signup THEN purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        assert!(f.window.is_none());
        // Step names should be absent (no `name:` prefix).
        assert!(f.steps[0].name.is_none());
        assert!(f.steps[1].name.is_none());
        // Event types come through as bare names.
        let event_name = |step: &bqlite_ast::Step| match &step.event {
            bqlite_ast::StepEvent::Single(er) => er.event.text.as_str().to_string(),
            _ => panic!("expected Single event ref"),
        };
        assert_eq!(event_name(&f.steps[0]), "signup");
        assert_eq!(event_name(&f.steps[1]), "purchase");
    }

    #[test]
    fn funnel_three_step_with_window() {
        // `events | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d`
        let stmt = parse_stmt("events | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 3);
        // 7 days in nanoseconds.
        let seven_days_ns = 7 * 24 * 60 * 60 * 1_000_000_000_i64;
        assert_eq!(f.window, Some(seven_days_ns));
    }

    #[test]
    fn funnel_named_steps() {
        // `events | FUNNEL(s1: signup THEN s2: purchase)`
        let stmt = parse_stmt("events | FUNNEL(s1: signup THEN s2: purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        assert_eq!(
            f.steps[0].name.as_ref().map(|n| n.text.as_str()),
            Some("s1")
        );
        assert_eq!(
            f.steps[1].name.as_ref().map(|n| n.text.as_str()),
            Some("s2")
        );
    }

    #[test]
    fn funnel_step_with_where_predicate() {
        // `events | FUNNEL(signup WHERE plan = 'pro' THEN purchase)`
        let stmt = parse_stmt("events | FUNNEL(signup WHERE plan = 'pro' THEN purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        assert!(
            f.steps[0].predicate.is_some(),
            "first step must carry WHERE predicate"
        );
        assert!(f.steps[1].predicate.is_none());
    }

    #[test]
    fn funnel_step_with_without_exclusion() {
        // `events | FUNNEL(signup WITHOUT refund THEN purchase)`
        let stmt = parse_stmt("events | FUNNEL(signup WITHOUT refund THEN purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        assert!(
            f.steps[0].without_next.is_some(),
            "first step must carry WITHOUT exclusion"
        );
    }

    #[test]
    fn funnel_step_with_alternation() {
        // `events | FUNNEL((add_to_cart OR add_to_wishlist) THEN purchase)`
        let stmt = parse_stmt("events | FUNNEL((add_to_cart OR add_to_wishlist) THEN purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        assert!(
            matches!(f.steps[0].event, bqlite_ast::StepEvent::Alternation(_)),
            "first step must be an alternation"
        );
    }

    #[test]
    fn funnel_step_with_immediately() {
        // `events | FUNNEL(signup THEN IMMEDIATELY purchase)`
        let stmt = parse_stmt("events | FUNNEL(signup THEN IMMEDIATELY purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        // IMMEDIATELY flag is stored on the *preceding* step.
        assert!(
            f.steps[0].immediately_next,
            "first step must have immediately_next = true"
        );
    }

    #[test]
    fn funnel_within_optional_is_absent() {
        // No WITHIN → window is None.
        let stmt = parse_stmt("events | FUNNEL(a THEN b)");
        let f = funnel_of(&stmt);
        assert!(f.window.is_none());
    }

    #[test]
    fn funnel_within_30d() {
        let stmt = parse_stmt("events | FUNNEL(a THEN b) WITHIN 30d");
        let f = funnel_of(&stmt);
        let thirty_days_ns = 30 * 24 * 60 * 60 * 1_000_000_000_i64;
        assert_eq!(f.window, Some(thirty_days_ns));
    }

    #[test]
    fn funnel_case_insensitive_keyword() {
        // FUNNEL keyword is case-insensitive per query-language.md §26.3.
        let stmt = parse_stmt("events | funnel(signup THEN purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
    }

    #[test]
    fn funnel_span_covers_keyword_through_rparen_when_no_window() {
        // When no WITHIN clause is present, the stage span runs from the
        // `F` of `FUNNEL` through the closing `)`.
        let src = "events | FUNNEL(a THEN b)";
        let stmt = parse_stmt(src);
        let stages = stages_of(&stmt);
        match &stages[0] {
            PipelineStage::Funnel(f) => {
                let funnel_start = src.find("FUNNEL").unwrap();
                let rparen_end = src.rfind(')').unwrap() + 1;
                assert_eq!(f.span.start, funnel_start);
                assert_eq!(f.span.end, rparen_end);
            }
            other => panic!("expected Funnel, got {other:?}"),
        }
    }

    #[test]
    fn funnel_span_covers_keyword_through_window_when_within_present() {
        // With a WITHIN clause, the stage span runs through the duration token.
        let src = "events | FUNNEL(a THEN b) WITHIN 7d";
        let stmt = parse_stmt(src);
        let stages = stages_of(&stmt);
        match &stages[0] {
            PipelineStage::Funnel(f) => {
                let funnel_start = src.find("FUNNEL").unwrap();
                // Span end must be at least past `7d`.
                let dur_end = src.rfind("7d").unwrap() + "7d".len();
                assert_eq!(f.span.start, funnel_start);
                assert_eq!(f.span.end, dur_end);
            }
            other => panic!("expected Funnel, got {other:?}"),
        }
    }

    #[test]
    fn funnel_empty_step_list_errors() {
        // `FUNNEL()` with no steps must be a parse error.
        match crate::parse("events | FUNNEL()") {
            Err(ParseError::Unexpected { .. }) | Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected parse error for empty FUNNEL(), got {other:?}"),
        }
    }

    #[test]
    fn funnel_missing_lparen_errors() {
        match crate::parse("events | FUNNEL signup THEN purchase") {
            Err(ParseError::Unexpected { .. }) | Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected parse error for FUNNEL without opening `(`, got {other:?}"),
        }
    }

    #[test]
    fn funnel_missing_rparen_errors() {
        match crate::parse("events | FUNNEL(signup THEN purchase") {
            Err(ParseError::Unexpected { .. }) | Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected parse error for FUNNEL without closing `)`, got {other:?}"),
        }
    }

    #[test]
    fn funnel_within_session_is_rejected() {
        // `WITHIN SESSION` is a MATCH-only modifier — FUNNEL must reject it.
        match crate::parse("events | FUNNEL(signup THEN purchase) WITHIN SESSION") {
            Err(ParseError::Unexpected { detail, found, .. }) => {
                assert_eq!(found, "SESSION", "error should point at SESSION token");
                assert!(
                    detail.map(|d| d.contains("SESSION")).unwrap_or(false),
                    "detail should mention SESSION"
                );
            }
            other => {
                panic!("expected Unexpected error for WITHIN SESSION in FUNNEL, got {other:?}")
            }
        }
    }

    #[test]
    fn funnel_within_non_duration_errors() {
        // `WITHIN` without a valid duration literal must error.
        match crate::parse("events | FUNNEL(a THEN b) WITHIN notaduration") {
            Err(ParseError::Unexpected { expected, .. })
            | Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Literal);
            }
            other => panic!("expected Expected::Literal error after WITHIN, got {other:?}"),
        }
    }

    #[test]
    fn funnel_is_terminal_pipe_after_errors() {
        // FUNNEL cannot be followed by another pipe stage — trying to do
        // so must produce a parse error with a helpful message.
        match crate::parse("events | FUNNEL(signup THEN purchase) | WHERE x = 1") {
            Err(ParseError::Unexpected {
                detail, expected, ..
            }) => {
                assert_eq!(expected, Expected::Eof);
                assert!(
                    detail.map(|d| d.contains("FUNNEL")).unwrap_or(false),
                    "error detail should mention FUNNEL terminal constraint"
                );
            }
            other => panic!("expected parse error when piping after FUNNEL, got {other:?}"),
        }
    }

    #[test]
    fn funnel_is_terminal_pipe_after_with_window_errors() {
        // The terminal rule applies even when WITHIN is present.
        match crate::parse("events | FUNNEL(signup THEN purchase) WITHIN 7d | LIMIT 10") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Eof);
            }
            other => {
                panic!("expected parse error when piping after FUNNEL with WITHIN, got {other:?}")
            }
        }
    }

    #[test]
    fn funnel_single_step_is_valid() {
        // A single-step FUNNEL is syntactically valid (semantic constraints
        // live in the planner, not the parser).
        let stmt = parse_stmt("events | FUNNEL(signup)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 1);
    }

    #[test]
    fn funnel_qualified_event_refs_accepted() {
        // query-language.md §19.3: FUNNEL accepts table-qualified event
        // references in multi-table queries.
        let stmt = parse_stmt("events | FUNNEL(events.signup THEN events.purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 2);
        // Verify the qualifier was captured on the first step's event ref.
        match &f.steps[0].event {
            bqlite_ast::StepEvent::Single(er) => {
                assert!(
                    er.table.is_some(),
                    "qualified event ref must carry table name"
                );
                assert_eq!(er.event.text.as_str(), "signup");
            }
            _ => panic!("expected Single event ref"),
        }
    }

    #[test]
    fn funnel_step_repetition_accepted() {
        // The full MATCH step sub-grammar is accepted, including repetition
        // modifiers (`+`, `*`). The parser accepts them; semantic constraints
        // on repetition in FUNNEL are enforced by the planner.
        let stmt = parse_stmt("events | FUNNEL(signup THEN browse+ THEN purchase)");
        let f = funnel_of(&stmt);
        assert_eq!(f.steps.len(), 3);
        assert!(
            f.steps[1].repetition.is_some(),
            "middle step with `+` must carry a repetition"
        );
    }

    #[test]
    fn funnel_is_terminal_stats_after_errors() {
        // The most natural user mistake: writing `FUNNEL(…) | STATS …`.
        // This must be caught at parse time with a clear error message.
        match crate::parse("events | FUNNEL(signup THEN purchase) | STATS n = COUNT(*)") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Eof);
                assert!(
                    detail.map(|d| d.contains("FUNNEL")).unwrap_or(false),
                    "error detail should mention FUNNEL"
                );
            }
            other => panic!("expected parse error for FUNNEL followed by STATS, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // RETENTION tests
    // -----------------------------------------------------------------------

    fn retention_of(stmt: &Statement) -> &bqlite_ast::Retention {
        match stages_of(stmt)
            .iter()
            .find(|s| matches!(s, PipelineStage::Retention(_)))
        {
            Some(PipelineStage::Retention(r)) => r,
            _ => panic!("expected a Retention stage"),
        }
    }

    #[test]
    fn retention_basic() {
        // Canonical named-arg order: entry, activity, brackets.
        let stmt = parse_stmt(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [7d, 14d, 30d])",
        );
        let r = retention_of(&stmt);
        assert_eq!(r.entry.event.text.as_str(), "signup");
        assert_eq!(r.activity.event.text.as_str(), "purchase");
        assert_eq!(
            r.brackets.durations,
            vec![
                7 * 86_400_000_000_000i64,
                14 * 86_400_000_000_000i64,
                30 * 86_400_000_000_000i64,
            ]
        );
        assert!(!r.brackets.cumulative, "cumulative defaults to false");
    }

    #[test]
    fn retention_with_cumulative_true() {
        let stmt = parse_stmt(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [7d, 30d], cumulative: true)",
        );
        let r = retention_of(&stmt);
        assert!(r.brackets.cumulative);
    }

    #[test]
    fn retention_with_cumulative_false() {
        let stmt = parse_stmt(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [7d, 30d], cumulative: false)",
        );
        let r = retention_of(&stmt);
        assert!(!r.brackets.cumulative);
    }

    #[test]
    fn retention_parameter_ordering_brackets_first() {
        // Named args in a non-canonical order: brackets before entry/activity.
        let stmt =
            parse_stmt("events | RETENTION(brackets: [7d], entry: signup, activity: purchase)");
        let r = retention_of(&stmt);
        assert_eq!(r.entry.event.text.as_str(), "signup");
        assert_eq!(r.activity.event.text.as_str(), "purchase");
        assert_eq!(r.brackets.durations.len(), 1);
    }

    #[test]
    fn retention_parameter_ordering_cumulative_first() {
        // cumulative: before the required args.
        let stmt = parse_stmt(
            "events | RETENTION(cumulative: true, entry: signup, activity: purchase, brackets: [7d])",
        );
        let r = retention_of(&stmt);
        assert!(r.brackets.cumulative);
    }

    #[test]
    fn retention_qualified_event_refs() {
        // query-language.md §19.3: RETENTION accepts table-qualified event refs.
        let stmt = parse_stmt(
            "events | RETENTION(entry: users.signup, activity: users.purchase, brackets: [30d])",
        );
        let r = retention_of(&stmt);
        assert_eq!(
            r.entry.table.as_ref().map(|t| t.text.as_str()),
            Some("users")
        );
        assert_eq!(
            r.activity.table.as_ref().map(|t| t.text.as_str()),
            Some("users")
        );
    }

    #[test]
    fn retention_is_terminal_pipe_after_errors() {
        // RETENTION must not be followed by another pipe stage.
        match crate::parse(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [7d]) | STATS n = COUNT(*)",
        ) {
            Err(ParseError::Unexpected { expected, detail, .. }) => {
                assert_eq!(expected, Expected::Eof);
                assert!(
                    detail.map(|d| d.contains("RETENTION")).unwrap_or(false),
                    "error detail should mention RETENTION"
                );
            }
            other => panic!("expected terminal error for RETENTION | STATS, got {other:?}"),
        }
    }

    #[test]
    fn retention_duplicate_entry_key_errors() {
        match crate::parse(
            "events | RETENTION(entry: signup, entry: login, activity: purchase, brackets: [7d])",
        ) {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail
                        .map(|d| d.contains("duplicate") && d.contains("entry"))
                        .unwrap_or(false),
                    "error detail should mention duplicate entry"
                );
            }
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
    }

    #[test]
    fn retention_duplicate_activity_key_errors() {
        match crate::parse(
            "events | RETENTION(entry: signup, activity: purchase, activity: login, brackets: [7d])",
        ) {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("duplicate") && d.contains("activity")).unwrap_or(false),
                    "error detail should mention duplicate activity"
                );
            }
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
    }

    #[test]
    fn retention_duplicate_brackets_key_errors() {
        match crate::parse(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [7d], brackets: [30d])",
        ) {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("duplicate") && d.contains("brackets")).unwrap_or(false),
                    "error detail should mention duplicate brackets"
                );
            }
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
    }

    #[test]
    fn retention_duplicate_cumulative_key_errors() {
        match crate::parse(
            "events | RETENTION(entry: signup, activity: purchase, brackets: [7d], cumulative: true, cumulative: false)",
        ) {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("duplicate") && d.contains("cumulative")).unwrap_or(false),
                    "error detail should mention duplicate cumulative"
                );
            }
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
    }

    #[test]
    fn retention_missing_entry_errors() {
        match crate::parse("events | RETENTION(activity: purchase, brackets: [7d])") {
            Err(ParseError::Unexpected { detail, .. })
            | Err(ParseError::UnexpectedEof { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("entry")).unwrap_or(false),
                    "error detail should mention missing entry"
                );
            }
            other => panic!("expected missing-entry error, got {other:?}"),
        }
    }

    #[test]
    fn retention_missing_activity_errors() {
        match crate::parse("events | RETENTION(entry: signup, brackets: [7d])") {
            Err(ParseError::Unexpected { detail, .. })
            | Err(ParseError::UnexpectedEof { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("activity")).unwrap_or(false),
                    "error detail should mention missing activity"
                );
            }
            other => panic!("expected missing-activity error, got {other:?}"),
        }
    }

    #[test]
    fn retention_missing_brackets_errors() {
        match crate::parse("events | RETENTION(entry: signup, activity: purchase)") {
            Err(ParseError::Unexpected { detail, .. })
            | Err(ParseError::UnexpectedEof { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("brackets")).unwrap_or(false),
                    "error detail should mention missing brackets"
                );
            }
            other => panic!("expected missing-brackets error, got {other:?}"),
        }
    }

    #[test]
    fn retention_empty_brackets_list_errors() {
        match crate::parse("events | RETENTION(entry: signup, activity: purchase, brackets: [])") {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("brackets")).unwrap_or(false),
                    "error should mention brackets requirement"
                );
            }
            other => panic!("expected empty-brackets error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // SESSIONIZE tests
    // -----------------------------------------------------------------------

    fn sessionize_of(stmt: &Statement) -> &bqlite_ast::Sessionize {
        match stages_of(stmt)
            .iter()
            .find(|s| matches!(s, PipelineStage::Sessionize(_)))
        {
            Some(PipelineStage::Sessionize(s)) => s,
            _ => panic!("expected a Sessionize stage"),
        }
    }

    #[test]
    fn sessionize_gap_only() {
        // Simplest form: only the required gap parameter.
        let stmt = parse_stmt("events | SESSIONIZE(gap: 30m)");
        let s = sessionize_of(&stmt);
        assert_eq!(s.gap, 30 * 60 * 1_000_000_000i64, "30m in nanoseconds");
        assert!(s.end.is_none(), "no end events in gap-only mode");
    }

    #[test]
    fn sessionize_gap_and_single_end_event() {
        // Single end event — no parentheses required.
        let stmt = parse_stmt("events | SESSIONIZE(gap: 30m, end: logout)");
        let s = sessionize_of(&stmt);
        assert_eq!(s.gap, 30 * 60 * 1_000_000_000i64);
        let end = s.end.as_ref().expect("end should be set");
        assert_eq!(end.len(), 1);
        assert_eq!(end[0].event.text.as_str(), "logout");
    }

    #[test]
    fn sessionize_end_list_parenthesised() {
        // Multiple end events in a parenthesised list.
        let stmt = parse_stmt("events | SESSIONIZE(gap: 30m, end: (logout, timeout, session_end))");
        let s = sessionize_of(&stmt);
        let end = s.end.as_ref().expect("end should be set");
        assert_eq!(end.len(), 3);
        assert_eq!(end[0].event.text.as_str(), "logout");
        assert_eq!(end[1].event.text.as_str(), "timeout");
        assert_eq!(end[2].event.text.as_str(), "session_end");
    }

    #[test]
    fn sessionize_parameter_ordering_end_before_gap() {
        // Named args in reverse order: end before gap.
        let stmt = parse_stmt("events | SESSIONIZE(end: logout, gap: 1h)");
        let s = sessionize_of(&stmt);
        assert_eq!(s.gap, 3_600_000_000_000i64, "1h in nanoseconds");
        let end = s.end.as_ref().expect("end should be set");
        assert_eq!(end[0].event.text.as_str(), "logout");
    }

    #[test]
    fn sessionize_duplicate_gap_key_errors() {
        match crate::parse("events | SESSIONIZE(gap: 30m, gap: 1h)") {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail
                        .map(|d| d.contains("duplicate") && d.contains("gap"))
                        .unwrap_or(false),
                    "error detail should mention duplicate gap"
                );
            }
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_duplicate_end_key_errors() {
        match crate::parse("events | SESSIONIZE(gap: 30m, end: logout, end: timeout)") {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail
                        .map(|d| d.contains("duplicate") && d.contains("end"))
                        .unwrap_or(false),
                    "error detail should mention duplicate end"
                );
            }
            other => panic!("expected duplicate-key error, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_duplicate_names_in_end_list_errors() {
        // Duplicate event names inside the end: list are rejected at parse time
        // (sessionize.md §5.4).
        match crate::parse("events | SESSIONIZE(gap: 30m, end: (logout, timeout, logout))") {
            Err(ParseError::Unexpected { detail, found, .. }) => {
                assert_eq!(found, "logout", "found should be the duplicate event name");
                assert!(
                    detail.map(|d| d.contains("duplicate")).unwrap_or(false),
                    "error detail should mention duplicate"
                );
            }
            other => panic!("expected duplicate-in-list error, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_missing_gap_errors() {
        match crate::parse("events | SESSIONIZE(end: logout)") {
            Err(ParseError::Unexpected { detail, .. })
            | Err(ParseError::UnexpectedEof { detail, .. }) => {
                assert!(
                    detail.map(|d| d.contains("gap")).unwrap_or(false),
                    "error detail should mention missing gap"
                );
            }
            other => panic!("expected missing-gap error, got {other:?}"),
        }
    }

    #[test]
    fn sessionize_within_session_surface_accepted() {
        // A downstream MATCH using WITHIN SESSION is valid BQL after SESSIONIZE.
        // The parser level surface for this is that the pipeline parses without
        // error and both stages are present.
        let stmt = parse_stmt(
            "events | SESSIONIZE(gap: 30m) | MATCH FIRST SEQUENCE(search THEN checkout) WITHIN SESSION",
        );
        let stages = stages_of(&stmt);
        assert!(
            stages
                .iter()
                .any(|s| matches!(s, PipelineStage::Sessionize(_))),
            "SESSIONIZE stage must be present"
        );
        assert!(
            stages
                .iter()
                .any(|s| matches!(s, PipelineStage::Match { .. })),
            "MATCH stage must be present after SESSIONIZE"
        );
    }

    #[test]
    fn sessionize_qualified_end_event_ref() {
        // End event ref can be table-qualified in multi-table queries.
        let stmt = parse_stmt("events | SESSIONIZE(gap: 30m, end: users.logout)");
        let s = sessionize_of(&stmt);
        let end = s.end.as_ref().expect("end should be set");
        assert_eq!(
            end[0].table.as_ref().map(|t| t.text.as_str()),
            Some("users")
        );
        assert_eq!(end[0].event.text.as_str(), "logout");
    }

    #[test]
    fn sessionize_single_element_parenthesised_end_list() {
        // `end: (logout)` — single event in a parenthesised list is valid.
        let stmt = parse_stmt("events | SESSIONIZE(gap: 30m, end: (logout))");
        let s = sessionize_of(&stmt);
        let end = s.end.as_ref().expect("end should be set");
        assert_eq!(end.len(), 1);
        assert_eq!(end[0].event.text.as_str(), "logout");
    }

    #[test]
    fn sessionize_cross_table_qualified_end_refs_not_duplicate() {
        // `end: (events.logout, purchases.logout)` — same bare name but different
        // table qualifiers: NOT a duplicate (duplicate detection considers the
        // full (table, event) pair, not the bare name alone).
        let stmt =
            parse_stmt("events | SESSIONIZE(gap: 30m, end: (events.logout, purchases.logout))");
        let s = sessionize_of(&stmt);
        let end = s.end.as_ref().expect("end should be set");
        assert_eq!(end.len(), 2);
        assert_eq!(end[0].event.text.as_str(), "logout");
        assert_eq!(end[1].event.text.as_str(), "logout");
        assert_ne!(
            end[0].table.as_ref().map(|t| t.text.as_str()),
            end[1].table.as_ref().map(|t| t.text.as_str()),
            "table qualifiers should differ"
        );
    }

    // --- ATTRIBUTE --------------------------------------------------------

    /// Extract the `Attribute` from the first stage (must be `Attribute`).
    fn attribute_of(stmt: &Statement) -> &Attribute {
        let stages = stages_of(stmt);
        match &stages[0] {
            PipelineStage::Attribute(a) => a,
            other => panic!("expected Attribute stage, got {other:?}"),
        }
    }

    #[test]
    fn attribute_single_event_refs() {
        // Basic form: single event ref for each of conversion and touchpoints.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(conversion: purchase, touchpoints: ad_click, \
             window: 30d, touchpoint_key: channel)",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion.len(), 1);
        assert_eq!(attr.conversion[0].event.text, "purchase");
        assert_eq!(attr.touchpoints.len(), 1);
        assert_eq!(attr.touchpoints[0].event.text, "ad_click");
        // 30d in nanoseconds: 30 * 24 * 60 * 60 * 1_000_000_000
        assert_eq!(attr.window, 30 * 24 * 60 * 60 * 1_000_000_000i64);
        // touchpoint_key must be a non-empty expression (column reference).
        match &attr.touchpoint_key.node {
            Expr::Column(name) => assert_eq!(name.text, "channel"),
            other => panic!("expected Column for touchpoint_key, got {other:?}"),
        }
    }

    #[test]
    fn attribute_multi_event_lists() {
        // Parenthesised lists for both conversion and touchpoints.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: (purchase, subscription), \
             touchpoints: (ad_click, email_open), \
             window: 7d, \
             touchpoint_key: channel\
             )",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion.len(), 2);
        assert_eq!(attr.conversion[0].event.text, "purchase");
        assert_eq!(attr.conversion[1].event.text, "subscription");
        assert_eq!(attr.touchpoints.len(), 2);
        assert_eq!(attr.touchpoints[0].event.text, "ad_click");
        assert_eq!(attr.touchpoints[1].event.text, "email_open");
    }

    #[test]
    fn attribute_single_item_parenthesised_list() {
        // Parenthesised list with a single event ref is valid.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: (purchase), \
             touchpoints: (ad_click), \
             window: 30d, \
             touchpoint_key: channel\
             )",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion.len(), 1);
        assert_eq!(attr.conversion[0].event.text, "purchase");
        assert_eq!(attr.touchpoints.len(), 1);
        assert_eq!(attr.touchpoints[0].event.text, "ad_click");
    }

    #[test]
    fn attribute_overlap_between_lists_is_allowed() {
        // The same event type may appear in both conversion and touchpoints
        // (attribute.md §3: "Lists may overlap").
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: login, \
             touchpoints: login, \
             window: 7d, \
             touchpoint_key: channel\
             )",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion[0].event.text, "login");
        assert_eq!(attr.touchpoints[0].event.text, "login");
    }

    #[test]
    fn attribute_overlap_multi_list_allowed() {
        // Overlap with multi-event lists is also permitted.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: (purchase, signup), \
             touchpoints: (ad_click, signup), \
             window: 30d, \
             touchpoint_key: source\
             )",
        );
        let attr = attribute_of(&stmt);
        // signup appears in both lists — not an error.
        assert_eq!(attr.conversion.len(), 2);
        assert_eq!(attr.touchpoints.len(), 2);
    }

    #[test]
    fn attribute_parameters_any_order() {
        // Parameters may appear in any order.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             window: 14d, \
             touchpoint_key: campaign, \
             touchpoints: email_click, \
             conversion: signup\
             )",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion[0].event.text, "signup");
        assert_eq!(attr.touchpoints[0].event.text, "email_click");
        assert_eq!(attr.window, 14 * 24 * 60 * 60 * 1_000_000_000i64);
    }

    #[test]
    fn attribute_trailing_comma_accepted() {
        // A trailing comma before `)` is syntactically valid.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: ad_click, \
             window: 30d, \
             touchpoint_key: channel,\
             )",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion[0].event.text, "purchase");
    }

    #[test]
    fn attribute_touchpoint_key_arithmetic_expression() {
        // `touchpoint_key` accepts any scalar expression supported by the
        // Wave 2 expression parser — tested here with binary arithmetic.
        // (Function calls like CONCAT are a later-wave expression feature.)
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: ad_click, \
             window: 30d, \
             touchpoint_key: amount + 1\
             )",
        );
        let attr = attribute_of(&stmt);
        // Verify the expression is a binary operation, not just a column.
        match &attr.touchpoint_key.node {
            Expr::Binary { op, .. } => assert_eq!(*op, bqlite_ast::BinaryOp::Add),
            other => panic!("expected Binary Add for touchpoint_key, got {other:?}"),
        }
    }

    #[test]
    fn attribute_touchpoint_key_column_reference() {
        // `touchpoint_key: channel` — bare column reference is the common case.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: ad_click, \
             window: 30d, \
             touchpoint_key: channel\
             )",
        );
        let attr = attribute_of(&stmt);
        match &attr.touchpoint_key.node {
            Expr::Column(name) => assert_eq!(name.text, "channel"),
            other => panic!("expected Column for touchpoint_key, got {other:?}"),
        }
    }

    #[test]
    fn attribute_qualified_event_refs_accepted() {
        // Event refs may be table-qualified (e.g. `events.purchase`).
        let stmt = parse_stmt(
            "events | ATTRIBUTE(\
             conversion: events.purchase, \
             touchpoints: events.ad_click, \
             window: 30d, \
             touchpoint_key: channel\
             )",
        );
        let attr = attribute_of(&stmt);
        assert_eq!(attr.conversion[0].table.as_ref().unwrap().text, "events");
        assert_eq!(attr.conversion[0].event.text, "purchase");
        assert_eq!(attr.touchpoints[0].table.as_ref().unwrap().text, "events");
        assert_eq!(attr.touchpoints[0].event.text, "ad_click");
    }

    #[test]
    fn attribute_span_covers_keyword_through_rparen() {
        // The stage span should start at `ATTRIBUTE` and end at `)`.
        let src = "events | ATTRIBUTE(conversion: purchase, touchpoints: ad_click, window: 30d, touchpoint_key: channel)";
        let stmt = parse_stmt(src);
        let attr = attribute_of(&stmt);
        // The span start should be at the `A` of `ATTRIBUTE`; end at `)`.
        assert!(
            attr.span.start < attr.span.end,
            "span must be non-empty: {:?}",
            attr.span
        );
        let attr_kw_pos = src.find("ATTRIBUTE").unwrap();
        assert_eq!(
            attr.span.start, attr_kw_pos,
            "span.start must point at ATTRIBUTE keyword"
        );
        let rparen_pos = src.rfind(')').unwrap();
        assert_eq!(
            attr.span.end,
            rparen_pos + 1,
            "span.end must be one past `)`"
        );
    }

    // --- ATTRIBUTE error cases ------------------------------------------

    #[test]
    fn attribute_error_empty_conversion_list() {
        // `conversion: ()` — empty parenthesised list is a parse error;
        // `parse_event_ref` calls `expect_name` and sees `)`, producing
        // `Expected::Name` (the role used by `parse_event_ref` internally).
        match crate::parse(
            "events | ATTRIBUTE(conversion: (), touchpoints: ad_click, window: 30d, touchpoint_key: channel)",
        ) {
            Err(ParseError::Unexpected { expected, .. }) => {
                // `parse_event_ref` → `expect_name` → Expected::Name on empty list.
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected empty-list error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_missing_conversion() {
        match crate::parse(
            "events | ATTRIBUTE(touchpoints: ad_click, window: 30d, touchpoint_key: channel)",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("conversion"));
                assert!(
                    detail.map(|d| d.contains("conversion")).unwrap_or(false),
                    "detail should mention conversion"
                );
            }
            other => panic!("expected missing-conversion error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_missing_touchpoints() {
        match crate::parse(
            "events | ATTRIBUTE(conversion: purchase, window: 30d, touchpoint_key: channel)",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("touchpoints"));
                assert!(
                    detail.map(|d| d.contains("touchpoints")).unwrap_or(false),
                    "detail should mention touchpoints"
                );
            }
            other => panic!("expected missing-touchpoints error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_missing_window() {
        match crate::parse(
            "events | ATTRIBUTE(conversion: purchase, touchpoints: ad_click, touchpoint_key: channel)",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("window"));
                assert!(
                    detail.map(|d| d.contains("window")).unwrap_or(false),
                    "detail should mention window"
                );
            }
            other => panic!("expected missing-window error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_missing_touchpoint_key() {
        match crate::parse(
            "events | ATTRIBUTE(conversion: purchase, touchpoints: ad_click, window: 30d)",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("touchpoint_key"));
                assert!(
                    detail
                        .map(|d| d.contains("touchpoint_key"))
                        .unwrap_or(false),
                    "detail should mention touchpoint_key"
                );
            }
            other => panic!("expected missing-touchpoint_key error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_duplicate_conversion_key() {
        match crate::parse(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             conversion: signup, \
             touchpoints: ad_click, \
             window: 30d, \
             touchpoint_key: channel\
             )",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("conversion"));
                assert!(
                    detail.map(|d| d.contains("duplicate")).unwrap_or(false),
                    "detail should mention duplicate"
                );
            }
            other => panic!("expected duplicate-conversion error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_duplicate_window_key() {
        match crate::parse(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: ad_click, \
             window: 30d, \
             window: 7d, \
             touchpoint_key: channel\
             )",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("window"));
                assert!(
                    detail.map(|d| d.contains("duplicate")).unwrap_or(false),
                    "detail should mention duplicate"
                );
            }
            other => panic!("expected duplicate-window error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_unknown_parameter_key() {
        match crate::parse(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: ad_click, \
             window: 30d, \
             touchpoint_key: channel, \
             model: last_touch\
             )",
        ) {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(
                    detail
                        .map(|d| d.contains("unknown ATTRIBUTE parameter"))
                        .unwrap_or(false),
                    "detail should mention unknown parameter"
                );
            }
            other => panic!("expected unknown-parameter error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_duplicate_event_in_conversion_list() {
        // Duplicate within conversion: list is rejected.
        match crate::parse(
            "events | ATTRIBUTE(\
             conversion: (purchase, purchase), \
             touchpoints: ad_click, \
             window: 30d, \
             touchpoint_key: channel\
             )",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EventRef);
                assert!(
                    detail.map(|d| d.contains("duplicate")).unwrap_or(false),
                    "detail should mention duplicate"
                );
            }
            other => panic!("expected duplicate-event error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_duplicate_event_in_touchpoints_list() {
        // Duplicate within touchpoints: list is rejected.
        match crate::parse(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: (ad_click, ad_click), \
             window: 30d, \
             touchpoint_key: channel\
             )",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EventRef);
                assert!(
                    detail.map(|d| d.contains("duplicate")).unwrap_or(false),
                    "detail should mention duplicate"
                );
            }
            other => panic!("expected duplicate-event error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_error_window_not_duration() {
        // `window:` must be a duration literal; an integer is rejected.
        match crate::parse(
            "events | ATTRIBUTE(\
             conversion: purchase, \
             touchpoints: ad_click, \
             window: 30, \
             touchpoint_key: channel\
             )",
        ) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Literal);
                assert!(
                    detail.map(|d| d.contains("duration")).unwrap_or(false),
                    "detail should mention duration"
                );
            }
            other => panic!("expected window-not-duration error, got {other:?}"),
        }
    }

    #[test]
    fn attribute_piped_with_stats() {
        // ATTRIBUTE can be followed by further pipe stages (non-terminal).
        let stmt = parse_stmt("events | ATTRIBUTE(conversion: purchase, touchpoints: ad_click, window: 30d, touchpoint_key: channel) | STATS n = COUNT(*)");
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 2);
        assert!(matches!(stages[0], PipelineStage::Attribute(_)));
        assert!(matches!(stages[1], PipelineStage::Stats { .. }));
    }

    #[test]
    fn attribute_piped_with_where_then_stats() {
        // ATTRIBUTE | WHERE | STATS composition.
        let stmt = parse_stmt(
            "events | ATTRIBUTE(conversion: purchase, touchpoints: ad_click, window: 30d, touchpoint_key: channel) | WHERE touchpoint_ts IS NOT NULL | STATS n = COUNT(*)",
        );
        let stages = stages_of(&stmt);
        assert_eq!(stages.len(), 3);
        assert!(matches!(stages[0], PipelineStage::Attribute(_)));
        assert!(matches!(stages[1], PipelineStage::Where { .. }));
        assert!(matches!(stages[2], PipelineStage::Stats { .. }));
    }
}
