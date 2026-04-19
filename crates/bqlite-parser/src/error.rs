//! Parser error types.
//!
//! Design: docs/design/language/grammar-framework.md §4.
//!
//! The parser halts on the first error (query-language.md §30.10), so
//! every production function returns `Result<T, ParseError>` and
//! propagates failures unchanged. `ParseError` itself is plain data —
//! nothing in this module reaches back into the lexer or parser state.

use std::fmt;

use thiserror::Error;

/// An error raised while parsing BQL source text.
///
/// Every variant carries a byte offset into the original source text so
/// downstream stages (CLI, builder APIs) can render a caret. `Unexpected`
/// and `UnexpectedEof` additionally carry an optional hand-tuned
/// `detail` string that implements the §27.1 suggestion heuristics —
/// e.g. `"missing pipe \`|\` between source and WHERE"`. Detail strings
/// are `&'static str` so each call site picks a canned message rather
/// than formatting freely.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParseError {
    /// Hit end of input while a production still expected more tokens.
    #[error("unexpected end of input at byte {offset}: expected {expected}{}", detail.map(|d| format!(" — {d}")).unwrap_or_default())]
    UnexpectedEof {
        /// Byte offset of the end of the source text.
        offset: usize,
        /// What the production was looking for.
        expected: Expected,
        /// Optional hand-tuned suggestion for §27.1 common-error
        /// categories. `None` means the default `expected` message is
        /// enough.
        detail: Option<&'static str>,
    },

    /// A token was seen that the current production did not expect.
    #[error("unexpected token at byte {offset}: expected {expected}, found `{found}`{}", detail.map(|d| format!(" — {d}")).unwrap_or_default())]
    Unexpected {
        /// Byte offset of the offending token's first byte.
        offset: usize,
        /// 1-indexed line number of `offset`. `0` is used for the
        /// end-of-input pseudo-position to match the `Span::EMPTY`
        /// convention.
        line: u32,
        /// 1-indexed column number of `offset`. See `line`.
        column: u32,
        /// What the production was looking for.
        expected: Expected,
        /// Source text of the offending token, truncated to a short
        /// snippet so errors stay printable.
        found: String,
        /// Optional hand-tuned suggestion — see `UnexpectedEof::detail`.
        detail: Option<&'static str>,
    },

    /// A literal of a known kind was lexed but its body was malformed
    /// (integer overflow, invalid escape inside a string, duration unit
    /// not recognized, …).
    #[error("invalid {kind} literal at byte {offset}: {reason}")]
    InvalidLiteral {
        /// Byte offset of the literal's first byte.
        offset: usize,
        /// Which literal kind failed to parse.
        kind: LiteralKind,
        /// Human-readable explanation of the failure.
        reason: String,
    },

    /// A multi-character lexical token (string literal, backtick
    /// identifier, block comment) ran off the end of the input without
    /// a closing delimiter.
    #[error("unterminated {kind} at byte {offset}")]
    Unterminated {
        /// Byte offset of the opening delimiter.
        offset: usize,
        /// Which kind of token was left open.
        kind: UnterminatedKind,
    },

    /// A reserved keyword was used as a bare identifier in a position
    /// that requires a user-defined name. Backtick-wrapped keyword
    /// names (`` `MATCH` ``) are not caught here — per query-language.md
    /// §26.3 rule 3 the lexer accepts them and the planner rejects
    /// them during catalog resolution.
    #[error("reserved keyword `{keyword}` cannot be used as a {role}")]
    ReservedKeyword {
        /// Byte offset of the keyword token.
        offset: usize,
        /// The keyword's canonical uppercase form.
        keyword: &'static str,
        /// Which name slot the keyword was used in.
        role: NameRole,
    },

    /// A table name appears more than once in a source expression's JOIN list.
    ///
    /// Self-joins are forbidden at parse time because BQL v1 has no
    /// table-alias syntax — `events JOIN events` would leave all
    /// `events.signup` references ambiguous. For multi-pass analysis over
    /// a single table, use variable bindings inside a single MATCH or use
    /// aliases (query-language.md §19.2; cohorts-aliases-joins.md §8.2).
    #[error("table `{name}` cannot appear more than once in a source JOIN list")]
    SelfJoin {
        /// Byte offset of the duplicate table name token.
        offset: usize,
        /// The table name that was repeated.
        name: String,
    },
}

/// A closed vocabulary of parser-facing expectations used in error
/// messages. Having a finite set (rather than free-form strings) keeps
/// error categories consistent across call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// A specific keyword — e.g., `Expected::Keyword("WHERE")`.
    Keyword(&'static str),
    /// A specific punctuation token — e.g., `Expected::Punct("|")`.
    Punct(&'static str),
    /// Any bare `[A-Za-z_][A-Za-z0-9_]*` identifier.
    Identifier,
    /// A table/column name: bare identifier or backtick-quoted name.
    Name,
    /// The beginning of any valid token. Used by lexer-level errors
    /// where a raw byte could not start any known token shape.
    Token,
    /// Any literal: integer, number, string, bool, null, duration, timestamp.
    Literal,
    /// An integer literal specifically (e.g., `LIMIT N`).
    Integer,
    /// Any BQL expression.
    Expression,
    /// A pipeline stage — `WHERE`, `SELECT`, `LIMIT`, `MATCH`, etc.
    PipelineStage,
    /// A column definition inside `CREATE TABLE (...)`.
    ColumnDef,
    /// An event reference: `event_name` or `table.event_name`.
    EventRef,
    /// The end of the match-modifier sequence; signals an out-of-order
    /// modifier (e.g. `WITHIN` appearing after `EMIT ALL`).
    EndOfModifiers,
    /// End of input (no more tokens).
    Eof,
}

impl fmt::Display for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Keyword(kw) => write!(f, "keyword `{kw}`"),
            Self::Punct(p) => write!(f, "`{p}`"),
            Self::Identifier => f.write_str("identifier"),
            Self::Name => f.write_str("name"),
            Self::Token => f.write_str("token"),
            Self::Literal => f.write_str("literal"),
            Self::Integer => f.write_str("integer literal"),
            Self::Expression => f.write_str("expression"),
            Self::PipelineStage => f.write_str("pipeline stage"),
            Self::ColumnDef => f.write_str("column definition"),
            Self::EventRef => f.write_str("event reference"),
            Self::EndOfModifiers => f.write_str("end of match modifiers"),
            Self::Eof => f.write_str("end of input"),
        }
    }
}

/// The kind of literal that failed to parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralKind {
    Int,
    Float,
    String,
    Duration,
    Timestamp,
}

impl fmt::Display for LiteralKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Int => "integer",
            Self::Float => "number",
            Self::String => "string",
            Self::Duration => "duration",
            Self::Timestamp => "timestamp",
        })
    }
}

/// The kind of multi-character token that was left open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnterminatedKind {
    /// A backtick-quoted identifier `` `... ``.
    BacktickIdent,
    /// A single-quoted string literal `'...`.
    StringLit,
    /// A block comment `/* ...`.
    BlockComment,
}

impl fmt::Display for UnterminatedKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::BacktickIdent => "backtick identifier",
            Self::StringLit => "string literal",
            Self::BlockComment => "block comment",
        })
    }
}

/// Which user-name slot a keyword was attempted in. Used only by
/// `ParseError::ReservedKeyword`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameRole {
    TableName,
    ColumnName,
    AliasName,
    VariableName,
    StepName,
    OptionKey,
}

impl fmt::Display for NameRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::TableName => "table name",
            Self::ColumnName => "column name",
            Self::AliasName => "alias name",
            Self::VariableName => "variable name",
            Self::StepName => "step name",
            Self::OptionKey => "WITH option key",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_display_renders_keyword_with_backticks() {
        assert_eq!(Expected::Keyword("WHERE").to_string(), "keyword `WHERE`");
    }

    #[test]
    fn expected_display_renders_punct_with_backticks() {
        assert_eq!(Expected::Punct("|").to_string(), "`|`");
    }

    #[test]
    fn expected_display_identifier_is_plain() {
        assert_eq!(Expected::Identifier.to_string(), "identifier");
    }

    #[test]
    fn parse_error_unexpected_display_has_offset() {
        let err = ParseError::Unexpected {
            offset: 7,
            line: 1,
            column: 8,
            expected: Expected::Punct("|"),
            found: "where".into(),
            detail: None,
        };
        let text = err.to_string();
        assert!(text.contains("byte 7"));
        assert!(text.contains("`|`"));
        assert!(text.contains("`where`"));
    }

    #[test]
    fn parse_error_unexpected_display_includes_detail_when_present() {
        let err = ParseError::Unexpected {
            offset: 7,
            line: 1,
            column: 8,
            expected: Expected::Punct("|"),
            found: "where".into(),
            detail: Some("missing pipe `|` between source and WHERE"),
        };
        assert!(err
            .to_string()
            .contains("missing pipe `|` between source and WHERE"));
    }

    #[test]
    fn parse_error_unexpected_display_omits_detail_when_none() {
        let err = ParseError::Unexpected {
            offset: 0,
            line: 1,
            column: 1,
            expected: Expected::Identifier,
            found: "42".into(),
            detail: None,
        };
        let text = err.to_string();
        assert!(!text.contains(" — "));
    }

    #[test]
    fn parse_error_reserved_keyword_display_names_both_fields() {
        let err = ParseError::ReservedKeyword {
            offset: 0,
            keyword: "MATCH",
            role: NameRole::TableName,
        };
        let text = err.to_string();
        assert!(text.contains("MATCH"));
        assert!(text.contains("table name"));
    }

    #[test]
    fn parse_error_unterminated_display_names_kind_and_offset() {
        let err = ParseError::Unterminated {
            offset: 12,
            kind: UnterminatedKind::BacktickIdent,
        };
        let text = err.to_string();
        assert!(text.contains("byte 12"));
        assert!(text.contains("backtick identifier"));
    }

    #[test]
    fn parse_error_invalid_literal_display_includes_reason() {
        let err = ParseError::InvalidLiteral {
            offset: 4,
            kind: LiteralKind::Int,
            reason: "value exceeds i64::MAX".into(),
        };
        let text = err.to_string();
        assert!(text.contains("integer literal"));
        assert!(text.contains("byte 4"));
        assert!(text.contains("value exceeds i64::MAX"));
    }

    #[test]
    fn parse_error_self_join_display_names_table() {
        let err = ParseError::SelfJoin {
            offset: 12,
            name: "events".into(),
        };
        let text = err.to_string();
        assert!(
            text.contains("events"),
            "table name should appear in message"
        );
    }
}
