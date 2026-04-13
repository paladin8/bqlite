//! # bqlite-parser
//!
//! BQL text parser.
//!
//! Parses BQL query text into the AST types defined in `bqlite-ast`.
//! Design: `docs/design/query-language.md` (grammar §26, error strategy
//! §27, error-recovery policy §30.10) and
//! `docs/design/language/grammar-framework.md` (this crate's
//! implementation technology — hand-rolled recursive descent plus a
//! hand-written lexer).
//!
//! ## Public surface
//!
//! - [`parse`] — lex and parse a BQL source string into a
//!   [`bqlite_ast::Statement`].
//! - [`ParseError`] — the typed error returned when lexing or parsing
//!   fails. Every variant carries a byte offset and, where available,
//!   a line/column pair and a hand-tuned suggestion string so callers
//!   can render a caret with the `§27.1` common-error heuristics.
//!
//! Internal modules ([`lex`], [`parser`], [`error`]) are kept
//! crate-private; only `parse` and `ParseError` are exported from this
//! crate. This matches the Wave 1 stub's public surface so the engine
//! integration (see `crates/bqlite-engine/src/query.rs`) keeps working
//! without changes.
//!
//! ## Wave 2 status
//!
//! TASK-220 lands the framework scaffolding (lexer, error types,
//! parser cursor) and the expression grammar covering every precedence
//! level from §26's `or_expr → and_expr → not_expr → comparison →
//! addition → multiplication → unary → primary` ladder for the
//! literal / column / paren / unary / binary / compare / IS NULL /
//! AND / OR / NOT subset.
//!
//! TASK-223 wires the `|` pipeline operator into the top-level
//! dispatcher and lands the Wave 2 pipeline verbs — `WHERE`,
//! `SELECT [DISTINCT]`, and `LIMIT` — via the `pipeline` module.
//!
//! TASK-221 lands the schema DDL surface (`CREATE TABLE`,
//! `ALTER TABLE ADD COLUMN`, `DROP TABLE`, `DESCRIBE`) and the
//! `EXPLAIN pipeline` wrapper via the `ddl` module.
//!
//! Later wave tasks add the remaining productions:
//!
//! TASK-222 adds the `INSERT ... FROM '<path>' [WITH (...)]` DML
//! production via the `dml` module, including the structured
//! `map: (src AS dst, ...)` column-rename clause. `INSERT ... VALUES`
//! (TASK-238) and `DELETE` (Wave 4) are deferred.
//!
//! Expression features deferred to later tasks: function calls,
//! `CAST`, `CASE`, `BETWEEN`, `LIKE`, `~=`, `CONTAINS`, `IN` / `NOT
//! IN`, and `@`-prefixed timestamp literals.

mod ddl;
mod dml;
mod error;
mod expr;
mod lex;
mod parser;
mod pattern;
mod pipeline;

use bqlite_ast::Statement;

pub use crate::error::{Expected, LiteralKind, NameRole, ParseError, UnterminatedKind};

/// Parse a BQL submission into an ordered list of statements.
///
/// A BQL submission follows the grammar (query-language.md §26):
///
/// ```text
/// query     := (alias_def)* terminal
/// alias_def := identifier "=" pipeline_body
/// ```
///
/// The returned [`Vec`] always contains at least one element:
/// - Zero or more [`Statement::DefineAlias`] items appear first (in
///   source order).
/// - The last element is the **terminal statement** — a query pipeline,
///   DDL, or DML statement.
///
/// Alias execution semantics (forward-reference validation, cycle
/// detection, per-submission caching) are planner-level concerns
/// deferred to TASK-425. The parser records definition order without
/// validating reference or shadowing rules.
///
/// # Examples
///
/// ```
/// use bqlite_parser::parse;
/// use bqlite_ast::Statement;
///
/// // A plain pipeline with no aliases — returns a one-element Vec.
/// let stmts = parse("events").unwrap();
/// assert_eq!(stmts.len(), 1);
/// match &stmts[0] {
///     Statement::Query(p) => assert_eq!(p.source.primary.name.text, "events"),
///     _ => unreachable!("bare-source parses to a Query statement"),
/// }
///
/// // A submission with one alias definition and a terminal query.
/// let stmts = parse("buyers = purchases | LIMIT 10\nevents").unwrap();
/// assert_eq!(stmts.len(), 2);
/// assert!(matches!(&stmts[0], Statement::DefineAlias { .. }));
/// assert!(matches!(&stmts[1], Statement::Query(_)));
/// ```
pub fn parse(text: &str) -> Result<Vec<Statement>, ParseError> {
    let mut p = parser::Parser::new(text)?;
    let stmts = parser::parse_script(&mut p)?;
    p.expect_eof()?;
    Ok(stmts)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Extract the single terminal statement from a no-alias submission.
    ///
    /// Most tests exercise single-statement input; this helper avoids
    /// repeating `.into_iter().last().unwrap()` in every test.
    fn parse_one(text: &str) -> Result<Statement, ParseError> {
        parse(text).map(|mut v| {
            assert_eq!(
                v.len(),
                1,
                "expected exactly one statement, got {}",
                v.len()
            );
            v.remove(0)
        })
    }

    fn name_of(stmt: &Statement) -> &str {
        match stmt {
            Statement::Query(p) => &p.source.primary.name.text,
            other => panic!("expected Query, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Happy path — identifier acceptance
    // ------------------------------------------------------------------

    #[test]
    fn parses_bare_identifier() {
        let stmt = parse_one("events").unwrap();
        assert_eq!(name_of(&stmt), "events");
        match stmt {
            Statement::Query(pipe) => {
                assert!(pipe.stages.is_empty());
                assert!(pipe.source.joins.is_empty());
                assert!(pipe.source.time_range.is_none());
                assert_eq!(pipe.source.primary.span.start, 0);
                assert_eq!(pipe.source.primary.span.end, 6);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parses_identifier_with_underscores_and_digits() {
        let stmt = parse_one("_user_events_42").unwrap();
        assert_eq!(name_of(&stmt), "_user_events_42");
    }

    #[test]
    fn parses_leading_whitespace() {
        let stmt = parse_one("   events").unwrap();
        assert_eq!(name_of(&stmt), "events");
        match stmt {
            Statement::Query(pipe) => {
                assert_eq!(pipe.source.primary.span.start, 3);
                assert_eq!(pipe.source.primary.span.end, 9);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parses_trailing_whitespace() {
        assert_eq!(name_of(&parse_one("events   ").unwrap()), "events");
    }

    #[test]
    fn parses_both_leading_and_trailing_whitespace() {
        assert_eq!(name_of(&parse_one("  events\n").unwrap()), "events");
    }

    #[test]
    fn parses_trailing_semicolon() {
        // Framework-doc §12 #3: the parser tolerates a single trailing
        // `;` as a statement terminator for REPL ergonomics.
        assert_eq!(name_of(&parse_one("events;").unwrap()), "events");
        assert_eq!(name_of(&parse_one("events ;").unwrap()), "events");
    }

    #[test]
    fn parses_backtick_quoted_identifier() {
        let stmt = parse_one("`weird name`").unwrap();
        assert_eq!(name_of(&stmt), "weird name");
        match stmt {
            Statement::Query(pipe) => {
                assert_eq!(pipe.source.primary.span.start, 0);
                assert_eq!(pipe.source.primary.span.end, "`weird name`".len());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parses_backtick_with_symbols_inside() {
        assert_eq!(
            name_of(&parse_one("`table.with.dots`").unwrap()),
            "table.with.dots"
        );
    }

    #[test]
    fn parses_backtick_with_doubled_backtick() {
        // `` `a``b` `` → literal identifier `a`b`
        assert_eq!(name_of(&parse_one("`a``b`").unwrap()), "a`b");
    }

    // ------------------------------------------------------------------
    // Error paths — updated to the new ParseError variants
    // ------------------------------------------------------------------

    #[test]
    fn rejects_empty_input_as_eof() {
        match parse("") {
            Err(ParseError::UnexpectedEof {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Name);
                assert_eq!(detail, Some("expected a table name"));
            }
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn rejects_whitespace_only_input_as_eof() {
        match parse("   \t\n  ") {
            Err(ParseError::UnexpectedEof { .. }) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn rejects_leading_digit_with_trailing_token_error() {
        // `42events` lexes as Int(42) followed by Ident("events"). The
        // first token is the Int, which is not a valid source name, so
        // the dispatcher raises Unexpected/Name.
        match parse("42events") {
            Err(ParseError::Unexpected {
                offset, expected, ..
            }) => {
                assert_eq!(offset, 0);
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_leading_symbol_as_lexer_error() {
        // `@` is not a valid lexer byte and produces a lex-level
        // Unexpected error before the parser runs.
        match parse("@foo") {
            Err(ParseError::Unexpected { offset, found, .. }) => {
                assert_eq!(offset, 0);
                assert_eq!(found, "@");
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_two_identifiers_as_trailing_input() {
        match parse("events extra") {
            Err(ParseError::Unexpected {
                offset,
                expected,
                found,
                ..
            }) => {
                assert_eq!(offset, 7);
                assert_eq!(expected, Expected::Eof);
                assert_eq!(found, "extra");
            }
            other => panic!("expected Unexpected/Eof, got {other:?}"),
        }
    }

    #[test]
    fn rejects_dotted_name_as_trailing_input() {
        // The new lexer tokenizes `schema.events` as Ident, Dot, Ident.
        // The dispatcher accepts the first Ident and expect_eof rejects
        // the `.` with `Expected::Eof`. Wave 2 does not yet support
        // qualified source references (§26 line 1492 is single-name).
        match parse("schema.events") {
            Err(ParseError::Unexpected {
                offset, expected, ..
            }) => {
                assert_eq!(offset, 6);
                assert_eq!(expected, Expected::Eof);
            }
            other => panic!("expected Unexpected/Eof, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_pipeline_stage_keyword() {
        // `events | filter` — the `|` is accepted (TASK-223), but
        // `filter` is an identifier, not a pipeline-stage keyword, so
        // the dispatcher errors with `Expected::PipelineStage` pointing
        // at the `filter` token.
        match parse("events | filter") {
            Err(ParseError::Unexpected {
                offset,
                expected,
                found,
                ..
            }) => {
                assert_eq!(offset, "events | ".len());
                assert_eq!(expected, Expected::PipelineStage);
                assert_eq!(found, "filter");
            }
            other => panic!("expected Unexpected/PipelineStage, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_ident_char_immediately_after_identifier() {
        match parse("events!") {
            Err(ParseError::Unexpected { offset, found, .. }) => {
                assert_eq!(offset, 6);
                assert_eq!(found, "!");
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_trailing_garbage_after_backtick_identifier() {
        let input = "`weird name` extra";
        match parse(input) {
            Err(ParseError::Unexpected {
                offset,
                expected,
                found,
                ..
            }) => {
                assert_eq!(offset, 13);
                assert_eq!(expected, Expected::Eof);
                assert_eq!(found, "extra");
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unterminated_backtick() {
        match parse("`never_closed") {
            Err(ParseError::Unterminated { offset, kind }) => {
                assert_eq!(offset, 0);
                assert_eq!(kind, UnterminatedKind::BacktickIdent);
            }
            other => panic!("expected Unterminated, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_backtick_as_invalid_literal() {
        match parse("``") {
            Err(ParseError::InvalidLiteral { offset, kind, .. }) => {
                assert_eq!(offset, 0);
                assert_eq!(kind, LiteralKind::String);
            }
            other => panic!("expected InvalidLiteral, got {other:?}"),
        }
    }

    #[test]
    fn rejects_bare_reserved_keyword_as_source() {
        match parse("MATCH") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "MATCH");
                assert_eq!(role, NameRole::TableName);
            }
            other => panic!("expected ReservedKeyword, got {other:?}"),
        }
    }

    #[test]
    fn rejects_bare_reserved_keyword_case_insensitively() {
        assert!(matches!(
            parse("where"),
            Err(ParseError::ReservedKeyword {
                keyword: "WHERE",
                ..
            })
        ));
    }

    #[test]
    fn unicode_non_ascii_is_rejected_as_lex_error() {
        match parse("événements") {
            Err(ParseError::Unexpected { offset, .. }) => assert_eq!(offset, 0),
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn backtick_wrapped_keyword_is_accepted_by_parser() {
        // §26.3 rule 3 defers keyword-shadowing rejection to the
        // planner; the parser accepts `` `MATCH` `` as a name.
        let stmt = parse_one("`MATCH`").unwrap();
        assert_eq!(name_of(&stmt), "MATCH");
    }

    #[test]
    fn trailing_snippet_is_truncated_for_huge_input() {
        // A 200-char trailing identifier — the `found` field in the
        // error must truncate to ~32 chars + ellipsis so error messages
        // stay printable even when callers pass a pathological input.
        let huge = format!("events {}", "x".repeat(200));
        match parse(&huge) {
            Err(ParseError::Unexpected { found, .. }) => {
                assert!(
                    found.chars().count() <= 33,
                    "snippet should be truncated, got {} chars",
                    found.chars().count()
                );
                assert!(found.ends_with('…'), "snippet should be ellipsized");
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // Regression / sanity
    // ------------------------------------------------------------------

    #[test]
    fn parse_result_is_cloneable_and_comparable() {
        // The returned Vec should be comparable and cloneable.
        let a = parse("events").unwrap();
        let b = a.clone();
        assert_eq!(a, b);
    }

    // ------------------------------------------------------------------
    // Alias definition parsing (TASK-423)
    // ------------------------------------------------------------------

    #[test]
    fn parses_single_alias_def_and_terminal_pipeline() {
        // `buyers = purchases LAST 30d\nevents` → [DefineAlias, Query]
        let stmts = parse("buyers = purchases LAST 30d\nevents").unwrap();
        assert_eq!(stmts.len(), 2, "expected alias + terminal");
        match &stmts[0] {
            Statement::DefineAlias { name, body, .. } => {
                assert_eq!(name.text, "buyers");
                assert_eq!(body.source.primary.name.text, "purchases");
            }
            other => panic!("expected DefineAlias, got {other:?}"),
        }
        match &stmts[1] {
            Statement::Query(p) => assert_eq!(p.source.primary.name.text, "events"),
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parses_multiple_alias_defs_before_terminal() {
        // Two alias defs, then the terminal pipeline.
        let src = "a = events\nb = purchases\nresults";
        let stmts = parse(src).unwrap();
        assert_eq!(stmts.len(), 3, "expected two aliases + terminal");
        assert!(matches!(&stmts[0], Statement::DefineAlias { name, .. } if name.text == "a"));
        assert!(matches!(&stmts[1], Statement::DefineAlias { name, .. } if name.text == "b"));
        assert!(matches!(&stmts[2], Statement::Query(_)));
    }

    #[test]
    fn alias_body_may_include_pipeline_stages() {
        // Alias body is a full pipeline: `buyers = events | LIMIT 5`
        let stmts = parse("buyers = events | LIMIT 5\nresults").unwrap();
        assert_eq!(stmts.len(), 2);
        match &stmts[0] {
            Statement::DefineAlias { name, body, .. } => {
                assert_eq!(name.text, "buyers");
                assert_eq!(body.stages.len(), 1);
            }
            other => panic!("expected DefineAlias, got {other:?}"),
        }
    }

    #[test]
    fn alias_with_backtick_name_is_accepted() {
        // Backtick-wrapped alias names are accepted by the parser per
        // §26.3 rule 3; planner rejects reserved names at bind time.
        let stmts = parse("`my alias` = events\nresults").unwrap();
        assert_eq!(stmts.len(), 2);
        match &stmts[0] {
            Statement::DefineAlias { name, .. } => assert_eq!(name.text, "my alias"),
            other => panic!("expected DefineAlias, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_alias_name_allowed_last_wins() {
        // Shadowing is permitted; the planner applies last-wins resolution.
        // cohorts-aliases-joins.md §2.2.
        let stmts = parse("x = events\nx = purchases\nresults").unwrap();
        assert_eq!(stmts.len(), 3);
        // Both DefineAlias items are preserved in source order.
        assert!(matches!(&stmts[0], Statement::DefineAlias { name, .. } if name.text == "x"));
        assert!(matches!(&stmts[1], Statement::DefineAlias { name, .. } if name.text == "x"));
        match &stmts[0] {
            Statement::DefineAlias { body, .. } => {
                assert_eq!(body.source.primary.name.text, "events")
            }
            _ => unreachable!(),
        }
        match &stmts[1] {
            Statement::DefineAlias { body, .. } => {
                assert_eq!(body.source.primary.name.text, "purchases")
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn alias_reserved_keyword_name_is_rejected() {
        // A bare keyword in alias-name position produces ReservedKeyword.
        match parse("WHERE = events\nresults") {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "WHERE");
                assert_eq!(role, NameRole::AliasName);
            }
            other => panic!("expected ReservedKeyword, got {other:?}"),
        }
    }

    #[test]
    fn alias_only_input_missing_terminal_is_error() {
        // A submission with only an alias def but no terminal pipeline
        // is a grammar error — the parser hits EOF where it expects a
        // statement.
        match parse("buyers = events") {
            Err(ParseError::UnexpectedEof { expected, .. }) => {
                // After the alias body `events`, EOF is reached where a
                // terminal statement is required.
                assert_eq!(expected, Expected::Name);
            }
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn alias_span_covers_name_through_body() {
        // The span on DefineAlias covers from the alias name to the end
        // of the pipeline body.
        let stmts = parse("ab = events\nresults").unwrap();
        match &stmts[0] {
            Statement::DefineAlias { name, span, .. } => {
                // Alias name starts at byte 0, body ends after "events" (byte 11).
                assert_eq!(name.span.start, 0);
                assert_eq!(span.start, 0);
                assert_eq!(span.end, "ab = events".len());
            }
            other => panic!("expected DefineAlias, got {other:?}"),
        }
    }

    #[test]
    fn no_alias_input_returns_single_element_vec() {
        // A plain pipeline returns a Vec with exactly one element.
        let stmts = parse("events").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Query(_)));
    }
}
