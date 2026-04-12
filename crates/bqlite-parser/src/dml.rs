//! DML (Data Manipulation Language) productions.
//!
//! Design: `docs/design/query-language.md` §20.1 (INSERT) and §26
//! (`insert_stmt` / `insert_body` / `insert_option` grammar). TASK-237
//! defined the `InsertBody::From::map` AST field that this module
//! populates.
//!
//! ## Productions landed here
//!
//! - `INSERT INTO <table> FROM '<path>'` — the minimal form. No options.
//! - `INSERT INTO <table> FROM '<path>' WITH ( ... )` — with a flat
//!   `key: literal` option list and an optional structured
//!   `map: (src AS dst, ...)` column-rename clause.
//! - `INSERT INTO <table> VALUES (lit, ...), (lit, ...), ...` — the
//!   literal-tuple form (TASK-238). Positional only, no column list,
//!   literals only — matches the AST's `Vec<Vec<Literal>>` shape and
//!   the §20.1 v1 restriction.
//!
//! ## Not landed here
//!
//! - `DELETE FROM ... WHERE ...` — Wave 4 (tombstones).
//!
//! ## Grammar conformance
//!
//! The grammar in §26 historically carried `insert_option := identifier
//! ":" literal`, which cannot represent the `map` clause — TASK-237
//! added the AST field out of band and §20.1 prose documents the
//! structured form. TASK-222 updates §26 so the grammar matches the
//! prose and the landed parser:
//!
//! ```text
//! insert_option    := identifier ":" literal
//!                   | MAP ":" "(" column_mapping ("," column_mapping)* ")"
//! column_mapping   := identifier AS identifier
//! ```
//!
//! The grammar doc bump lands in the same commit as the parser.

#![allow(dead_code)] // The dispatcher routes to this module from parser.rs.

use bqlite_ast::{ColumnMapping, InsertBody, InsertOption, InsertStmt, Literal, Name, Statement};

use crate::error::{Expected, NameRole, ParseError};
use crate::lex::{token_span, Keyword, TokenKind};
use crate::parser::Parser;

/// Parse an `INSERT INTO ...` statement.
///
/// Dispatches on the body form:
/// - `FROM '<path>' [WITH (...)]` (TASK-222)
/// - `VALUES (lit, ...), ...` (TASK-238)
pub(crate) fn parse_insert(p: &mut Parser) -> Result<Statement, ParseError> {
    let insert_tok = p.expect_kw(Keyword::Insert)?;
    let start_span = token_span(&insert_tok);
    p.expect_kw(Keyword::Into)?;
    let table = p.expect_name(NameRole::TableName)?;

    match p.peek_kind() {
        TokenKind::Kw(Keyword::From) => {
            let (body, body_end_span) = parse_insert_from_body(p)?;
            let span = start_span.merged(body_end_span);
            Ok(Statement::Insert(InsertStmt { table, body, span }))
        }
        TokenKind::Kw(Keyword::Values) => {
            let (body, body_end_span) = parse_insert_values_body(p)?;
            let span = start_span.merged(body_end_span);
            Ok(Statement::Insert(InsertStmt { table, body, span }))
        }
        _ => Err(p.error_unexpected(
            Expected::Keyword("FROM or VALUES"),
            Some("INSERT INTO <table> must be followed by `FROM <path>` or `VALUES (...)`"),
        )),
    }
}

// ----------------------------------------------------------------------
// INSERT ... FROM
// ----------------------------------------------------------------------

/// Parse `FROM string_lit (WITH "(" insert_option ("," insert_option)* ")")?`
/// and return the resulting [`InsertBody`] plus the span of the last
/// token the body consumed (used by [`parse_insert`] to compute the
/// outer statement span).
fn parse_insert_from_body(p: &mut Parser) -> Result<(InsertBody, bqlite_ast::Span), ParseError> {
    p.expect_kw(Keyword::From)?;

    // The grammar pins the path as a `string_lit` terminal (§26 line
    // 1659). Any non-string literal (number, duration, boolean) is a
    // parse error at this position.
    let path_tok = p.peek().clone();
    let path_span = token_span(&path_tok);
    let path = match &path_tok.kind {
        TokenKind::String(s) => {
            let owned = s.clone();
            p.bump();
            owned
        }
        _ => {
            return Err(p.error_unexpected(
                Expected::Literal,
                Some("INSERT ... FROM requires a string literal path (e.g. 'data.csv')"),
            ));
        }
    };

    // Optional `WITH (...)` clause. Missing WITH means no options and
    // no map clause — the body's end-span is the path.
    if !matches!(p.peek_kind(), TokenKind::Kw(Keyword::With)) {
        let body = InsertBody::From {
            path,
            options: vec![],
            map: None,
        };
        return Ok((body, path_span));
    }

    p.bump(); // consume WITH
    p.expect_punct(&TokenKind::LParen, "(")?;

    // `WITH ()` is a parse error per §26's non-optional first option.
    // Catch it explicitly so the user sees a helpful message instead
    // of the generic "expected identifier" that would fall out of the
    // first `parse_option_key` attempt.
    if matches!(p.peek_kind(), TokenKind::RParen) {
        return Err(p.error_unexpected(
            Expected::Identifier,
            Some("WITH option list requires at least one `key: value` entry"),
        ));
    }

    let mut options = Vec::new();
    let mut map: Option<Vec<ColumnMapping>> = None;

    // Parse the first insert_option; then a comma-separated tail.
    parse_one_with_option(p, &mut options, &mut map)?;
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        parse_one_with_option(p, &mut options, &mut map)?;
    }

    let rparen = p.expect_punct(&TokenKind::RParen, ")")?;
    let end_span = token_span(&rparen);

    let body = InsertBody::From { path, options, map };
    Ok((body, end_span))
}

/// Parse one `insert_option` and append it to `options` or `map`,
/// depending on the key. See the module-level grammar note for the
/// two shapes.
fn parse_one_with_option(
    p: &mut Parser,
    options: &mut Vec<InsertOption>,
    map: &mut Option<Vec<ColumnMapping>>,
) -> Result<(), ParseError> {
    // Lookahead: if the key token is the `MAP` keyword, dispatch into
    // the structured `map: (src AS dst, ...)` branch. Otherwise the
    // key must be a bare identifier and the value must be a literal.
    if matches!(p.peek_kind(), TokenKind::Kw(Keyword::Map)) {
        parse_map_option(p, map)?;
        return Ok(());
    }

    // Literal-valued option: identifier ":" literal.
    let key = parse_option_key(p)?;
    p.expect_punct(&TokenKind::Colon, ":")?;
    let (value, value_span) = parse_option_literal(p)?;
    let span = key.span.merged(value_span);
    options.push(InsertOption { key, value, span });
    Ok(())
}

/// Parse a flat option key — always a bare identifier per §26
/// `insert_option := identifier ":" literal`. Keywords are not
/// permitted here (backtick-wrapping would not help either — option
/// keys are a parser-owned vocabulary, not a user namespace).
fn parse_option_key(p: &mut Parser) -> Result<Name, ParseError> {
    let (text, span) = p.expect_ident(NameRole::OptionKey)?;
    Ok(Name::new(text, span))
}

/// Parse the literal value of a flat option.
fn parse_option_literal(p: &mut Parser) -> Result<(Literal, bqlite_ast::Span), ParseError> {
    let tok = p.peek().clone();
    let span = token_span(&tok);
    match &tok.kind {
        TokenKind::Int(v) => {
            let v = *v;
            p.bump();
            Ok((Literal::Int(v), span))
        }
        TokenKind::Number(v) => {
            let v = *v;
            p.bump();
            Ok((Literal::Float(v), span))
        }
        TokenKind::String(s) => {
            let s = s.clone();
            p.bump();
            Ok((Literal::String(s), span))
        }
        TokenKind::Bool(b) => {
            let b = *b;
            p.bump();
            Ok((Literal::Bool(b), span))
        }
        TokenKind::Null => {
            p.bump();
            Ok((Literal::Null, span))
        }
        TokenKind::Duration(ns) => {
            let ns = *ns;
            p.bump();
            Ok((Literal::Duration(ns), span))
        }
        _ => Err(p.error_unexpected(
            Expected::Literal,
            Some("WITH option value must be a literal"),
        )),
    }
}

// ----------------------------------------------------------------------
// INSERT ... VALUES
// ----------------------------------------------------------------------

/// Parse `VALUES "(" literal ("," literal)* ")" ("," "(" ... ")")*`.
///
/// Per §20.1 the VALUES form is positional-only and accepts the same
/// literal grammar as WITH option values: `Null`, `Bool`, `Int`,
/// `Float`, `String`, `Duration`. Returns the [`InsertBody::Values`]
/// and the span of the last token consumed.
fn parse_insert_values_body(p: &mut Parser) -> Result<(InsertBody, bqlite_ast::Span), ParseError> {
    let values_tok = p.expect_kw(Keyword::Values)?;
    let start_span = token_span(&values_tok);

    // Parse the first tuple; at least one is required.
    let (first_row, mut end_span) = parse_values_tuple(p)?;
    let mut rows = vec![first_row];

    // Parse additional tuples separated by commas.
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        let (row, row_end) = parse_values_tuple(p)?;
        rows.push(row);
        end_span = row_end;
    }

    let span = start_span.merged(end_span);
    Ok((InsertBody::Values(rows), span))
}

/// Parse one `"(" literal ("," literal)* ")"` tuple.
///
/// An empty tuple `()` is a parse error — the grammar requires at
/// least one literal per tuple per §20.1.
fn parse_values_tuple(p: &mut Parser) -> Result<(Vec<Literal>, bqlite_ast::Span), ParseError> {
    p.expect_punct(&TokenKind::LParen, "(")?;

    if matches!(p.peek_kind(), TokenKind::RParen) {
        return Err(p.error_unexpected(
            Expected::Literal,
            Some("VALUES tuple requires at least one literal value"),
        ));
    }

    let (first_lit, _) = parse_option_literal(p)?;
    let mut values = vec![first_lit];

    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        let (lit, _) = parse_option_literal(p)?;
        values.push(lit);
    }

    let rparen = p.expect_punct(&TokenKind::RParen, ")")?;
    let end_span = token_span(&rparen);
    Ok((values, end_span))
}

// ----------------------------------------------------------------------
// MAP clause — structured `(src AS dst, ...)` column-rename list
// ----------------------------------------------------------------------

/// Parse `MAP ":" "(" column_mapping ("," column_mapping)* ")"`.
///
/// The `MAP` keyword is already peeked by [`parse_one_with_option`];
/// this function consumes it. A repeat `map` clause (the user wrote
/// `WITH (map: (...), ..., map: (...))`) is rejected as a parse error
/// so the "grammar produces at most one `map`" invariant documented
/// in §20.1 holds at the parser layer.
fn parse_map_option(
    p: &mut Parser,
    map_out: &mut Option<Vec<ColumnMapping>>,
) -> Result<(), ParseError> {
    let map_tok = p.expect_kw(Keyword::Map)?;

    if map_out.is_some() {
        // Repeat `map:` is a parse error. The error points at the
        // already-consumed `map` keyword so the user can find the
        // duplicate — `Parser::error_unexpected` would anchor at the
        // current token (now past `map`), which would hide the
        // diagnostic from the user. Construct the error by hand in
        // this one spot.
        return Err(ParseError::Unexpected {
            offset: map_tok.start,
            line: map_tok.line,
            column: map_tok.column,
            expected: Expected::Identifier,
            found: "map".to_string(),
            detail: Some("`map` clause may appear at most once in a WITH option list"),
        });
    }

    p.expect_punct(&TokenKind::Colon, ":")?;
    p.expect_punct(&TokenKind::LParen, "(")?;

    // `map: ()` is a parse error per §20.1 ("an empty parenthesized
    // list is a parse error"). Catch it explicitly so the user sees a
    // helpful "map clause requires at least one entry" instead of the
    // generic "expected identifier" that would fall out of the first
    // `parse_column_mapping` call below.
    if matches!(p.peek_kind(), TokenKind::RParen) {
        return Err(p.error_unexpected(
            Expected::Identifier,
            Some("map clause requires at least one `src AS dst` entry"),
        ));
    }

    let mut entries = Vec::new();
    entries.push(parse_column_mapping(p)?);
    while matches!(p.peek_kind(), TokenKind::Comma) {
        p.bump(); // consume `,`
        entries.push(parse_column_mapping(p)?);
    }

    p.expect_punct(&TokenKind::RParen, ")")?;
    *map_out = Some(entries);
    Ok(())
}

/// Parse one `column_mapping := identifier AS identifier`.
///
/// Both sides are bare identifiers per the grammar; backtick names are
/// rejected here because column names flowing through the AS alias
/// rule are always bare per §26 line 1533's `expr AS identifier`
/// shape. The span covers both sides plus the intervening `AS`.
fn parse_column_mapping(p: &mut Parser) -> Result<ColumnMapping, ParseError> {
    let (source_text, source_span) = p.expect_ident(NameRole::ColumnName)?;
    let source = Name::new(source_text, source_span);
    p.expect_kw(Keyword::As)?;
    let (target_text, target_span) = p.expect_ident(NameRole::ColumnName)?;
    let target = Name::new(target_text, target_span);
    let span = source_span.merged(target_span);
    Ok(ColumnMapping {
        source,
        target,
        span,
    })
}

// ----------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::{InsertBody, Statement};

    fn parse_ok(src: &str) -> Statement {
        crate::parse(src).unwrap_or_else(|e| panic!("parse failed for `{src}`: {e:?}"))
    }

    fn insert_from_of(stmt: &Statement) -> (&str, &[InsertOption], &Option<Vec<ColumnMapping>>) {
        match stmt {
            Statement::Insert(i) => match &i.body {
                InsertBody::From { path, options, map } => (path.as_str(), options.as_slice(), map),
                other => panic!("expected InsertBody::From, got {other:?}"),
            },
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    // --- minimal FROM without WITH -----------------------------------

    #[test]
    fn insert_from_minimal_no_options() {
        let stmt = parse_ok("INSERT INTO events FROM 'data.csv'");
        let (path, options, map) = insert_from_of(&stmt);
        assert_eq!(path, "data.csv");
        assert!(options.is_empty());
        assert!(map.is_none());
    }

    #[test]
    fn insert_from_parquet_without_options() {
        let stmt = parse_ok("INSERT INTO events FROM 'data.parquet'");
        let (path, _, _) = insert_from_of(&stmt);
        assert_eq!(path, "data.parquet");
    }

    #[test]
    fn insert_from_case_insensitive_keywords() {
        let stmt = parse_ok("insert into events from 'data.csv'");
        let (path, _, _) = insert_from_of(&stmt);
        assert_eq!(path, "data.csv");
    }

    #[test]
    fn insert_from_accepts_backtick_table_name() {
        let stmt = parse_ok("INSERT INTO `weird table` FROM 'data.csv'");
        match stmt {
            Statement::Insert(i) => assert_eq!(i.table.text, "weird table"),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_accepts_trailing_semicolon() {
        let stmt = parse_ok("INSERT INTO events FROM 'data.csv';");
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn insert_from_span_covers_entire_statement() {
        let src = "INSERT INTO events FROM 'data.csv'";
        let stmt = parse_ok(src);
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.span.start, 0);
                assert_eq!(i.span.end, src.len());
            }
            _ => panic!("expected Insert"),
        }
    }

    // --- WITH flat options -------------------------------------------

    #[test]
    fn insert_from_with_format_option() {
        let stmt = parse_ok("INSERT INTO events FROM 'data.csv' WITH (format: 'csv')");
        let (_, options, map) = insert_from_of(&stmt);
        assert!(map.is_none());
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].key.text, "format");
        assert_eq!(options[0].value, Literal::String("csv".into()));
    }

    #[test]
    fn insert_from_with_delimiter_and_header() {
        let stmt =
            parse_ok("INSERT INTO events FROM 'data.csv' WITH (delimiter: ',', header: true)");
        let (_, options, _) = insert_from_of(&stmt);
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].key.text, "delimiter");
        assert_eq!(options[0].value, Literal::String(",".into()));
        assert_eq!(options[1].key.text, "header");
        assert_eq!(options[1].value, Literal::Bool(true));
    }

    #[test]
    fn insert_from_with_numeric_option_value() {
        let stmt = parse_ok("INSERT INTO events FROM 'data.csv' WITH (batch_size: 1024)");
        let (_, options, _) = insert_from_of(&stmt);
        assert_eq!(options[0].value, Literal::Int(1024));
    }

    #[test]
    fn insert_from_unknown_options_are_accepted_by_parser() {
        // §20.1: "Unknown keys are a plan-time error." The parser
        // records them verbatim and leaves the error to the planner.
        let stmt = parse_ok("INSERT INTO events FROM 'data.csv' WITH (foo: 'bar')");
        let (_, options, _) = insert_from_of(&stmt);
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].key.text, "foo");
    }

    // --- MAP clause --------------------------------------------------

    #[test]
    fn insert_from_with_map_clause_only() {
        let stmt = parse_ok(
            "INSERT INTO events FROM 'data.csv' \
             WITH (map: (uid AS user_id, event_ts AS ts, evt AS event_type))",
        );
        let (_, options, map) = insert_from_of(&stmt);
        assert!(options.is_empty());
        let mappings = map.as_ref().expect("map clause present");
        assert_eq!(mappings.len(), 3);
        assert_eq!(mappings[0].source.text, "uid");
        assert_eq!(mappings[0].target.text, "user_id");
        assert_eq!(mappings[1].source.text, "event_ts");
        assert_eq!(mappings[1].target.text, "ts");
        assert_eq!(mappings[2].source.text, "evt");
        assert_eq!(mappings[2].target.text, "event_type");
    }

    #[test]
    fn insert_from_with_format_and_map_clauses() {
        // The §20.1 example shape. The canonical example uses
        // `event_ts` rather than `time` because `time` is a reserved
        // keyword and the map clause requires bare identifiers.
        let stmt = parse_ok(
            "INSERT INTO events \
             FROM 'data.csv' \
             WITH (format: 'csv', map: (uid AS user_id, event_ts AS ts, evt AS event_type))",
        );
        let (_, options, map) = insert_from_of(&stmt);
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].key.text, "format");
        let mappings = map.as_ref().unwrap();
        assert_eq!(mappings.len(), 3);
    }

    #[test]
    fn map_clause_rejects_reserved_keyword_as_source() {
        // `time` is a reserved keyword (Keyword::Time, part of the
        // `EVENT TIME` column modifier). The parser rejects it in the
        // map source slot per the grammar's "bare identifier" rule.
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: (time AS ts))") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "TIME");
                assert_eq!(role, NameRole::ColumnName);
            }
            other => panic!("expected ReservedKeyword(TIME), got {other:?}"),
        }
    }

    #[test]
    fn map_clause_rejects_reserved_keyword_as_target() {
        // `event` is a reserved keyword. The parser rejects it in the
        // map target slot the same way it rejects a keyword source.
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: (evt AS event))") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "EVENT");
                assert_eq!(role, NameRole::ColumnName);
            }
            other => panic!("expected ReservedKeyword(EVENT), got {other:?}"),
        }
    }

    #[test]
    fn insert_from_map_clause_before_format() {
        // Options may appear in any order.
        let stmt = parse_ok(
            "INSERT INTO events FROM 'data.csv' \
             WITH (map: (uid AS user_id), format: 'csv')",
        );
        let (_, options, map) = insert_from_of(&stmt);
        assert_eq!(options.len(), 1);
        assert_eq!(options[0].key.text, "format");
        assert_eq!(map.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn insert_from_map_single_entry() {
        let stmt = parse_ok("INSERT INTO events FROM 'data.csv' WITH (map: (uid AS user_id))");
        let (_, _, map) = insert_from_of(&stmt);
        assert_eq!(map.as_ref().unwrap().len(), 1);
    }

    // --- error paths -------------------------------------------------

    #[test]
    fn insert_without_into_errors() {
        match crate::parse("INSERT events VALUES (1)") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("INTO"));
            }
            other => panic!("expected Unexpected/INTO, got {other:?}"),
        }
    }

    #[test]
    fn insert_missing_table_errors() {
        match crate::parse("INSERT INTO") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected UnexpectedEof/Name, got {other:?}"),
        }
    }

    #[test]
    fn insert_missing_from_or_values_errors() {
        match crate::parse("INSERT INTO events") {
            Err(ParseError::UnexpectedEof { expected, .. })
            | Err(ParseError::Unexpected { expected, .. }) => {
                // After TASK-238, the error names both accepted body forms.
                assert_eq!(expected, Expected::Keyword("FROM or VALUES"));
            }
            other => panic!("expected Unexpected/FROM or VALUES, got {other:?}"),
        }
    }

    // --- VALUES body -------------------------------------------------

    #[test]
    fn insert_values_single_row() {
        let stmt =
            parse_ok("INSERT INTO events VALUES ('alice', 1700000000000000000, 'click', 42)");
        match stmt {
            Statement::Insert(i) => match i.body {
                InsertBody::Values(rows) => {
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 4);
                    assert_eq!(rows[0][0], Literal::String("alice".into()));
                    assert_eq!(rows[0][1], Literal::Int(1700000000000000000_i64));
                    assert_eq!(rows[0][2], Literal::String("click".into()));
                    assert_eq!(rows[0][3], Literal::Int(42));
                }
                other => panic!("expected Values body, got {other:?}"),
            },
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_multi_row() {
        let stmt = parse_ok(
            "INSERT INTO events VALUES ('alice', 1700000000000000000, 'click', 1), \
             ('bob', 1700000000100000000, 'view', NULL)",
        );
        match stmt {
            Statement::Insert(i) => match i.body {
                InsertBody::Values(rows) => {
                    assert_eq!(rows.len(), 2);
                    assert_eq!(rows[0][0], Literal::String("alice".into()));
                    assert_eq!(rows[1][0], Literal::String("bob".into()));
                    assert_eq!(rows[1][3], Literal::Null);
                }
                other => panic!("expected Values body, got {other:?}"),
            },
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_span_covers_entire_statement() {
        let src = "INSERT INTO events VALUES ('alice', 1700000000000000000, 'click')";
        let stmt = parse_ok(src);
        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.span.start, 0);
                assert_eq!(i.span.end, src.len());
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn insert_values_with_bool_and_float_literals() {
        let stmt = parse_ok("INSERT INTO t VALUES (true, 1.5, NULL)");
        match stmt {
            Statement::Insert(i) => match i.body {
                InsertBody::Values(rows) => {
                    assert_eq!(rows[0][0], Literal::Bool(true));
                    assert_eq!(rows[0][1], Literal::Float(1.5));
                    assert_eq!(rows[0][2], Literal::Null);
                }
                other => panic!("expected Values, got {other:?}"),
            },
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_with_trailing_semicolon() {
        let stmt = parse_ok("INSERT INTO events VALUES ('alice', 0, 'click');");
        assert!(matches!(stmt, Statement::Insert(_)));
    }

    #[test]
    fn insert_values_empty_tuple_errors() {
        match crate::parse("INSERT INTO events VALUES ()") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Literal);
            }
            other => panic!("expected Unexpected/Literal, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_missing_open_paren_errors() {
        match crate::parse("INSERT INTO events VALUES 1, 2") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Punct("("));
            }
            other => panic!("expected Unexpected/`(`, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_missing_close_paren_errors() {
        match crate::parse("INSERT INTO events VALUES ('alice', 0, 'click'") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(")"));
            }
            other => panic!("expected UnexpectedEof/`)`, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_trailing_comma_in_row_errors() {
        match crate::parse("INSERT INTO events VALUES ('alice', 0, 'click',)") {
            Err(ParseError::Unexpected { .. }) => {}
            Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected error on trailing comma in row, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_non_literal_in_tuple_errors() {
        // Bare identifier is not a literal.
        match crate::parse("INSERT INTO events VALUES (alice, 0, 'click')") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Literal);
            }
            other => panic!("expected Unexpected/Literal, got {other:?}"),
        }
    }

    #[test]
    fn insert_values_negative_literal_errors() {
        // §26 line 1706: no negative-literal token; `-5` is unary minus
        // on a positive integer, which is not accepted in a terminal
        // `literal` position (the grammar's `literal` production is a
        // single token). The `-` will be seen as `Minus` punctuation
        // where a literal token is expected.
        match crate::parse("INSERT INTO events VALUES (-1, 0, 'click')") {
            Err(_) => {} // any parse error is correct
            Ok(stmt) => {
                panic!("negative literal in VALUES should be a parse error, got {stmt:?}")
            }
        }
    }

    #[test]
    fn insert_from_missing_path_errors() {
        match crate::parse("INSERT INTO events FROM") {
            Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_non_string_path_errors() {
        // §26 pins the path as `string_lit`. Every non-string literal
        // shape flows through the same error branch.
        let cases = &[
            "INSERT INTO events FROM 42",
            "INSERT INTO events FROM 1.5",
            "INSERT INTO events FROM true",
            "INSERT INTO events FROM 7d",
            "INSERT INTO events FROM NULL",
        ];
        for src in cases {
            match crate::parse(src) {
                Err(ParseError::Unexpected {
                    expected, detail, ..
                }) => {
                    assert_eq!(expected, Expected::Literal, "for {src}");
                    assert!(
                        detail.unwrap_or("").contains("string literal path"),
                        "missing detail for {src}"
                    );
                }
                other => panic!("expected Unexpected/Literal for {src}, got {other:?}"),
            }
        }
    }

    #[test]
    fn insert_from_with_missing_open_paren_errors() {
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Punct("("));
            }
            other => panic!("expected UnexpectedEof/`(`, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_with_missing_close_paren_errors() {
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (format: 'csv'") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(")"));
            }
            other => panic!("expected UnexpectedEof/`)`, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_with_trailing_comma_errors() {
        // `WITH (format: 'csv',)` — the trailing comma triggers another
        // option parse, which then fails at the `)`.
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (format: 'csv',)") {
            Err(ParseError::Unexpected { .. }) => {}
            Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected error on trailing comma, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_with_missing_colon_errors() {
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (format 'csv')") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(":"));
            }
            other => panic!("expected Unexpected/`:`, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_with_non_literal_value_errors() {
        // WITH (format: foo) — foo is a bare identifier, not a literal.
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (format: foo)") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Literal);
                assert!(detail
                    .unwrap_or("")
                    .contains("WITH option value must be a literal"));
            }
            other => panic!("expected Unexpected/Literal, got {other:?}"),
        }
    }

    #[test]
    fn insert_from_with_empty_parens_errors_with_helpful_detail() {
        // `WITH ()` — at least one option required. The parser emits a
        // specific "at least one key: value entry" detail so users do
        // not get a generic "expected identifier".
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH ()") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Identifier);
                assert!(detail
                    .unwrap_or("")
                    .contains("WITH option list requires at least one"));
            }
            other => panic!("expected Unexpected/Identifier with detail, got {other:?}"),
        }
    }

    // --- MAP clause errors -------------------------------------------

    #[test]
    fn map_clause_empty_parens_errors_with_helpful_detail() {
        // §20.1: "an empty parenthesized list is a parse error". The
        // parser emits a specific detail pointing at the map clause
        // rather than the bare "expected identifier" it would
        // otherwise produce.
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: ())") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Identifier);
                assert!(detail
                    .unwrap_or("")
                    .contains("map clause requires at least one"));
            }
            other => panic!("expected Unexpected/Identifier with detail, got {other:?}"),
        }
    }

    #[test]
    fn map_clause_missing_as_errors() {
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: (uid user_id))") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("AS"));
            }
            other => panic!("expected Unexpected/AS, got {other:?}"),
        }
    }

    #[test]
    fn map_clause_missing_target_errors() {
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: (uid AS))") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Identifier);
            }
            other => panic!("expected Unexpected/Identifier, got {other:?}"),
        }
    }

    #[test]
    fn map_clause_missing_source_errors() {
        // `map: (AS user_id)` — `AS` is a reserved keyword used in an
        // identifier slot.
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: (AS user_id))") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "AS");
                assert_eq!(role, NameRole::ColumnName);
            }
            other => panic!("expected ReservedKeyword(AS), got {other:?}"),
        }
    }

    #[test]
    fn map_clause_trailing_comma_errors() {
        match crate::parse("INSERT INTO events FROM 'data.csv' WITH (map: (uid AS user_id,))") {
            Err(ParseError::Unexpected { .. }) => {}
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn map_clause_repeat_key_errors() {
        // Two `map:` clauses in one WITH list. The invariant from
        // §20.1 is that `map` appears at most once.
        match crate::parse(
            "INSERT INTO events FROM 'data.csv' \
             WITH (map: (uid AS user_id), map: (evt AS event_type))",
        ) {
            Err(ParseError::Unexpected { detail, .. }) => {
                assert!(detail
                    .unwrap_or("")
                    .contains("`map` clause may appear at most once"));
            }
            other => panic!("expected Unexpected/repeat-map, got {other:?}"),
        }
    }

    #[test]
    fn map_clause_with_backtick_source_errors() {
        // The grammar requires bare identifiers on both sides.
        match crate::parse(
            "INSERT INTO events FROM 'data.csv' WITH (map: (`weird col` AS user_id))",
        ) {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Identifier);
            }
            other => panic!("expected Unexpected/Identifier, got {other:?}"),
        }
    }

    #[test]
    fn map_clause_source_with_duplicate_names_accepted_at_parse_time() {
        // §20.1: "a map list with duplicate target names is a
        // plan-time error" — the parser does NOT flag duplicates.
        // Pin the shape so a future regression inside the parser is
        // caught.
        let stmt = parse_ok(
            "INSERT INTO events FROM 'data.csv' \
             WITH (map: (uid AS user_id, uid AS user_id_alt))",
        );
        let (_, _, map) = insert_from_of(&stmt);
        let mappings = map.as_ref().unwrap();
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].source.text, "uid");
        assert_eq!(mappings[1].source.text, "uid");
    }
}
