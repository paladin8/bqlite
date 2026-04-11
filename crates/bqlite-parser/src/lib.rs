//! # bqlite-parser
//!
//! BQL text parser.
//!
//! Parses BQL query text into the AST types defined in `bqlite-ast`.
//! The full language (design: docs/design/query-language.md) eventually
//! handles:
//! - Pattern expressions: `match(A -> B -> C)`
//! - Time windows: `within 7d`
//! - Entity grouping: `by user_id`
//! - Property predicates: `WHERE amount > 50`, `WHERE query ~= ".*shoes.*"`
//! - Pipe operators: `| stats count, avg(duration)`
//! - Convenience wrappers: `funnel(...)`, `retention(...)`, `sessionize(...)`
//! - DDL/DML: `CREATE TABLE`, `INSERT INTO`, `DELETE FROM`
//!
//! The parser produces the same AST as the Rust and Python builder APIs.
//!
//! ## Wave 1 status — deliberately minimal
//!
//! Wave 1 (TASK-114) only needs to parse a single bare identifier — a
//! table name like `events` — so the end-to-end smoke test
//! `bqlite query "events"` can drive parser → planner → operators →
//! engine against an empty database and return an empty result set.
//!
//! The grammar accepted here is therefore:
//!
//! ```text
//! Statement  := Whitespace? Identifier Whitespace?
//! Identifier := PlainIdent | BacktickIdent
//! PlainIdent := [A-Za-z_] [A-Za-z0-9_]*
//! BacktickIdent := '`' ( any char except '`' )+ '`'
//! ```
//!
//! The parse result is a `Statement::Query` wrapping a `Pipeline` whose
//! `source.primary` is the identifier and whose `stages` list is empty.
//! Anything else — keywords, operators, whitespace inside the name,
//! multiple tokens, numeric literals — is a parse error. There is no
//! error recovery: the parser halts on the first failure.
//!
//! This implementation is explicitly throwaway scaffolding. The real
//! grammar framework (hand-rolled vs. parser generator, error recovery
//! strategy, span tracking, CSV `WITH (...)` remap clause, etc.) is the
//! Wave 2 design task **TASK-203**, and every real production lives
//! downstream of that. The surface exposed here — a single `parse`
//! entry point returning `Result<Statement, ParseError>` — is chosen
//! so the Wave 2 implementation can replace the body without touching
//! any caller in `bqlite-engine`.

use bqlite_ast::{Name, Pipeline, Source, Span, Statement, TableRef};
use thiserror::Error;

/// An error raised while parsing BQL source text.
///
/// Wave 1 only recognizes a single identifier, so the errors are
/// correspondingly coarse. Every variant carries a byte offset into the
/// original source text so later stages (and Wave 2's real parser) can
/// render diagnostics with a caret.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum ParseError {
    /// The input was empty or contained only whitespace.
    #[error("expected a table name, found empty input")]
    Empty,

    /// A character was encountered that cannot start a valid identifier.
    #[error("syntax error at byte {offset}: {reason}")]
    Syntax {
        /// Byte offset into the original source text.
        offset: usize,
        /// Human-readable explanation of the failure.
        reason: String,
    },

    /// Input remained after the first identifier. Wave 1 accepts a
    /// single token only — keywords and operators are Wave 2.
    #[error("unexpected trailing input at byte {offset}: `{rest}`")]
    TrailingInput {
        /// Byte offset of the unexpected text in the original source.
        offset: usize,
        /// The trailing source text (trimmed to a short snippet).
        rest: String,
    },

    /// A backtick-quoted identifier was opened but never closed.
    #[error("unterminated backtick identifier at byte {offset}")]
    UnterminatedBacktick {
        /// Byte offset of the opening backtick.
        offset: usize,
    },

    /// A backtick-quoted identifier contained no characters between
    /// the delimiters.
    #[error("empty backtick identifier at byte {offset}")]
    EmptyBacktick {
        /// Byte offset of the opening backtick.
        offset: usize,
    },
}

/// Parse a BQL query text into a [`Statement`].
///
/// Wave 1 only accepts a single identifier — a bare table name. The
/// result is `Statement::Query(Pipeline { source, stages: [] })` where
/// `source.primary` carries the parsed name.
///
/// See the crate-level documentation for the exact grammar and for the
/// Wave 2 upgrade path (TASK-203).
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
///     _ => unreachable!("wave 1 parser only emits Query statements"),
/// }
/// ```
pub fn parse(text: &str) -> Result<Statement, ParseError> {
    // Skip leading ASCII whitespace. `is_ascii_whitespace` on bytes is
    // safe: every ASCII-whitespace byte is a complete UTF-8 codepoint,
    // so truncating on the first non-whitespace byte lands on a valid
    // char boundary.
    let leading_ws: usize = text.bytes().take_while(|b| b.is_ascii_whitespace()).count();
    let (start_offset, body) = (leading_ws, &text[leading_ws..]);

    if body.is_empty() {
        return Err(ParseError::Empty);
    }

    // Extract the identifier token and its byte length within `body`.
    let (name_text, name_len) = parse_identifier(body, start_offset)?;

    // Skip any trailing whitespace after the identifier.
    let tail = &body[name_len..];
    let tail_ws: usize = tail.bytes().take_while(|b| b.is_ascii_whitespace()).count();
    let after = &tail[tail_ws..];

    if !after.is_empty() {
        let offset = start_offset + name_len + tail_ws;
        return Err(ParseError::TrailingInput {
            offset,
            rest: snippet(after),
        });
    }

    // Wave 1 assumes single-line input — we do not scan the leading
    // whitespace for newlines, so `line` is hard-coded to 1 and `column`
    // is the byte offset + 1. For the Wave 1 smoke test (`events`) that
    // is exact; for multi-line inputs it is a deliberate
    // simplification. Proper line/column tracking arrives with the real
    // grammar framework in TASK-203.
    let ident_span = Span::new(
        start_offset,
        start_offset + name_len,
        1,
        (start_offset as u32).saturating_add(1),
    );

    let primary = TableRef {
        name: Name::new(name_text, ident_span),
        span: ident_span,
    };
    let source = Source {
        primary,
        joins: vec![],
        time_range: None,
        span: ident_span,
    };
    let pipeline = Pipeline {
        source,
        stages: vec![],
        span: ident_span,
    };
    Ok(Statement::Query(pipeline))
}

/// Parse a single identifier from the start of `s`, returning the
/// identifier text and the number of bytes consumed within `s`.
///
/// `global_offset` is the byte offset of `s` within the original source
/// text; it is used only to populate error offsets.
fn parse_identifier(s: &str, global_offset: usize) -> Result<(String, usize), ParseError> {
    let first = s.chars().next().ok_or(ParseError::Empty)?;

    if first == '`' {
        // Backtick-quoted identifier. Skip the leading backtick and scan
        // for the closing one. Wave 1 does not support escaped backticks
        // (`` ` ``) inside a quoted identifier — that, too, is Wave 2.
        let mut closing: Option<usize> = None;
        for (i, c) in s[1..].char_indices() {
            if c == '`' {
                closing = Some(1 + i);
                break;
            }
        }
        let close_idx = closing.ok_or(ParseError::UnterminatedBacktick {
            offset: global_offset,
        })?;
        let inner = &s[1..close_idx];
        if inner.is_empty() {
            return Err(ParseError::EmptyBacktick {
                offset: global_offset,
            });
        }
        Ok((inner.to_string(), close_idx + 1))
    } else if is_ident_start(first) {
        // Plain [A-Za-z_][A-Za-z0-9_]* identifier. Advance until the
        // first character that cannot continue an identifier.
        let mut end = first.len_utf8();
        for c in s[end..].chars() {
            if is_ident_continue(c) {
                end += c.len_utf8();
            } else {
                break;
            }
        }
        Ok((s[..end].to_string(), end))
    } else {
        Err(ParseError::Syntax {
            offset: global_offset,
            reason: format!("unexpected character `{first}`"),
        })
    }
}

#[inline]
fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

#[inline]
fn is_ident_continue(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Return a short, display-friendly snippet of trailing text for the
/// `TrailingInput` error payload so errors stay printable even if a
/// caller passes a huge string.
fn snippet(s: &str) -> String {
    const MAX: usize = 32;
    let trimmed = s.trim_end();
    if trimmed.chars().count() <= MAX {
        return trimmed.to_string();
    }
    let cut: String = trimmed.chars().take(MAX).collect();
    format!("{cut}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::Statement;

    fn name_of(stmt: &Statement) -> &str {
        match stmt {
            Statement::Query(p) => &p.source.primary.name.text,
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parses_bare_identifier() {
        let stmt = parse("events").unwrap();
        assert_eq!(name_of(&stmt), "events");
        match stmt {
            Statement::Query(p) => {
                assert!(p.stages.is_empty(), "Wave 1 pipeline has no stages");
                assert!(p.source.joins.is_empty());
                assert!(p.source.time_range.is_none());
                // Span points into the input at offset 0..6.
                assert_eq!(p.source.primary.span.start, 0);
                assert_eq!(p.source.primary.span.end, 6);
                assert_eq!(p.source.primary.name.text, "events");
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
            Statement::Query(p) => {
                assert_eq!(p.source.primary.span.start, 3);
                assert_eq!(p.source.primary.span.end, 9);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parses_trailing_whitespace() {
        let stmt = parse("events   ").unwrap();
        assert_eq!(name_of(&stmt), "events");
    }

    #[test]
    fn parses_both_leading_and_trailing_whitespace() {
        let stmt = parse("  events\n").unwrap();
        assert_eq!(name_of(&stmt), "events");
    }

    #[test]
    fn parses_backtick_quoted_identifier() {
        let stmt = parse("`weird name`").unwrap();
        assert_eq!(name_of(&stmt), "weird name");
        match stmt {
            Statement::Query(p) => {
                // Span covers the backticks.
                assert_eq!(p.source.primary.span.start, 0);
                assert_eq!(p.source.primary.span.end, "`weird name`".len());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn parses_backtick_with_symbols_inside() {
        let stmt = parse("`table.with.dots`").unwrap();
        assert_eq!(name_of(&stmt), "table.with.dots");
    }

    #[test]
    fn rejects_empty_input() {
        assert_eq!(parse(""), Err(ParseError::Empty));
    }

    #[test]
    fn rejects_whitespace_only_input() {
        assert_eq!(parse("   \t\n  "), Err(ParseError::Empty));
    }

    #[test]
    fn rejects_leading_digit() {
        match parse("42events") {
            Err(ParseError::Syntax { offset, .. }) => assert_eq!(offset, 0),
            other => panic!("expected Syntax error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_leading_symbol() {
        match parse("@foo") {
            Err(ParseError::Syntax { offset, reason }) => {
                assert_eq!(offset, 0);
                assert!(reason.contains('@'), "reason should mention the char");
            }
            other => panic!("expected Syntax error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_two_identifiers() {
        match parse("events extra") {
            Err(ParseError::TrailingInput { offset, rest }) => {
                assert_eq!(offset, 7);
                assert_eq!(rest, "extra");
            }
            other => panic!("expected TrailingInput, got {other:?}"),
        }
    }

    #[test]
    fn rejects_dotted_name() {
        // Wave 2 will handle qualified references; Wave 1 treats `.` as
        // unexpected trailing input after the first identifier.
        match parse("schema.events") {
            Err(ParseError::TrailingInput { offset, rest }) => {
                assert_eq!(offset, 6);
                assert_eq!(rest, ".events");
            }
            other => panic!("expected TrailingInput, got {other:?}"),
        }
    }

    #[test]
    fn rejects_pipeline_stage() {
        // `| filter ...` is a Wave 2 production, not Wave 1.
        match parse("events | filter") {
            Err(ParseError::TrailingInput { offset, rest }) => {
                assert_eq!(offset, 7);
                assert_eq!(rest, "| filter");
            }
            other => panic!("expected TrailingInput, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_ident_char_immediately_after_identifier() {
        // Guards the off-by-one case where the rejected character has
        // no intervening whitespace between it and the name.
        match parse("events!") {
            Err(ParseError::TrailingInput { offset, rest }) => {
                assert_eq!(offset, 6);
                assert_eq!(rest, "!");
            }
            other => panic!("expected TrailingInput, got {other:?}"),
        }
    }

    #[test]
    fn rejects_trailing_garbage_after_backtick_identifier() {
        // Guards the `close_idx + 1` offset arithmetic for the backtick
        // path by confirming trailing input is flagged at the correct
        // byte offset (just past the closing backtick).
        let input = "`weird name` extra";
        let closing_backtick_plus_one = "`weird name`".len(); // 12
        match parse(input) {
            Err(ParseError::TrailingInput { offset, rest }) => {
                assert_eq!(offset, closing_backtick_plus_one + 1); // 13, past the space
                assert_eq!(rest, "extra");
            }
            other => panic!("expected TrailingInput, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unterminated_backtick() {
        match parse("`never_closed") {
            Err(ParseError::UnterminatedBacktick { offset }) => assert_eq!(offset, 0),
            other => panic!("expected UnterminatedBacktick, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_backtick() {
        match parse("``") {
            Err(ParseError::EmptyBacktick { offset }) => assert_eq!(offset, 0),
            other => panic!("expected EmptyBacktick, got {other:?}"),
        }
    }

    #[test]
    fn trailing_snippet_is_truncated_for_huge_input() {
        let huge = format!("events {}", "x".repeat(200));
        match parse(&huge) {
            Err(ParseError::TrailingInput { rest, .. }) => {
                // Snippet capped at ~32 chars + ellipsis.
                assert!(
                    rest.chars().count() <= 33,
                    "snippet should be truncated, got {} chars",
                    rest.chars().count()
                );
                assert!(rest.ends_with('…'), "snippet should be ellipsized");
            }
            other => panic!("expected TrailingInput, got {other:?}"),
        }
    }

    #[test]
    fn stub_is_round_trippable_via_debug() {
        // The returned Statement should be comparable and cloneable
        // like every other AST node.
        let a = parse("events").unwrap();
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn unicode_non_ascii_is_rejected_as_syntax_error() {
        // Wave 1 only accepts ASCII identifiers. Unicode identifiers
        // are Wave 2 (and may never be added — the Wave 0 design doc
        // pins identifiers to ASCII).
        match parse("événements") {
            Err(ParseError::Syntax { offset, .. }) => assert_eq!(offset, 0),
            other => panic!("expected Syntax error, got {other:?}"),
        }
    }
}
