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
//! `SELECT [DISTINCT]`, and `LIMIT` — via the [`pipeline`] module.
//!
//! Later wave tasks add the remaining productions:
//!
//! - **TASK-221** — DDL (`CREATE`, `ALTER`, `DROP`, `DESCRIBE`, `EXPLAIN`).
//! - **TASK-222** — `INSERT ... FROM` with the `WITH (...)` option list.
//! - **TASK-238** — `INSERT ... VALUES`.
//!
//! Expression features deferred to later tasks: function calls,
//! `CAST`, `CASE`, `BETWEEN`, `LIKE`, `~=`, `CONTAINS`, `IN` / `NOT
//! IN`, and `@`-prefixed timestamp literals.

mod error;
mod expr;
mod lex;
mod parser;
mod pipeline;

use bqlite_ast::Statement;

pub use crate::error::{Expected, LiteralKind, NameRole, ParseError, UnterminatedKind};

/// Parse a BQL query text into a [`Statement`].
///
/// Wave 2 CP1 only accepts the Wave 1 surface — a bare table name,
/// optionally followed by a trailing semicolon. Every other form is
/// rejected at parse time and will land in a later checkpoint.
///
/// # Examples
///
/// ```
/// use bqlite_parser::parse;
/// use bqlite_ast::Statement;
///
/// let stmt = parse("events").unwrap();
/// match stmt {
///     Statement::Query(p) => assert_eq!(p.source.primary.name.text, "events"),
///     _ => unreachable!("Wave 2 CP1 parser only emits Query statements"),
/// }
/// ```
pub fn parse(text: &str) -> Result<Statement, ParseError> {
    let mut p = parser::Parser::new(text)?;
    let stmt = parser::statement(&mut p)?;
    p.expect_eof()?;
    Ok(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let stmt = parse("events").unwrap();
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
        let stmt = parse("_user_events_42").unwrap();
        assert_eq!(name_of(&stmt), "_user_events_42");
    }

    #[test]
    fn parses_leading_whitespace() {
        let stmt = parse("   events").unwrap();
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
        assert_eq!(name_of(&parse("events   ").unwrap()), "events");
    }

    #[test]
    fn parses_both_leading_and_trailing_whitespace() {
        assert_eq!(name_of(&parse("  events\n").unwrap()), "events");
    }

    #[test]
    fn parses_trailing_semicolon() {
        // Framework-doc §12 #3: the parser tolerates a single trailing
        // `;` as a statement terminator for REPL ergonomics.
        assert_eq!(name_of(&parse("events;").unwrap()), "events");
        assert_eq!(name_of(&parse("events ;").unwrap()), "events");
    }

    #[test]
    fn parses_backtick_quoted_identifier() {
        let stmt = parse("`weird name`").unwrap();
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
            name_of(&parse("`table.with.dots`").unwrap()),
            "table.with.dots"
        );
    }

    #[test]
    fn parses_backtick_with_doubled_backtick() {
        // `` `a``b` `` → literal identifier `a`b`
        assert_eq!(name_of(&parse("`a``b`").unwrap()), "a`b");
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
        let stmt = parse("`MATCH`").unwrap();
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
        // The returned Statement should be comparable and cloneable
        // like every other AST node.
        let a = parse("events").unwrap();
        let b = a.clone();
        assert_eq!(a, b);
    }
}
