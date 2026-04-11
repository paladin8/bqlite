//! Parser cursor and top-level dispatch.
//!
//! Design: docs/design/language/grammar-framework.md §7.
//!
//! Production modules ([`crate::expr`] in a later checkpoint, DDL, DML,
//! pipeline) build on the `Parser<'s>` cursor defined here. The Wave 2
//! dispatcher is intentionally narrow — until TASK-221 / TASK-222 /
//! TASK-223 land their productions, `statement` only recognizes the
//! Wave 1 single-bare-identifier source form and produces the same
//! `Statement::Query(Pipeline { ... })` the Wave 1 stub returned so the
//! engine's smoke test keeps passing end-to-end.

use bqlite_ast::{Name, Pipeline, Source, Span, Statement, TableRef};

use crate::error::{Expected, NameRole, ParseError};
use crate::lex::{lex, token_span, Keyword, Token, TokenKind};

/// Parser cursor over a pre-lexed token stream.
///
/// A `Parser` is cheap to construct — it calls the lexer once up front
/// and then walks the resulting `Vec<Token>` with arbitrary lookahead.
/// Production functions take `&mut Parser` and use the `peek_*` /
/// `expect_*` helpers to consume tokens; they never touch the cursor
/// directly.
pub(crate) struct Parser<'s> {
    #[allow(dead_code)] // Used by future productions for detail snippets.
    source: &'s str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl<'s> Parser<'s> {
    /// Lex `source` and return a cursor positioned at the first token.
    pub(crate) fn new(source: &'s str) -> Result<Self, ParseError> {
        let tokens = lex(source)?;
        Ok(Self {
            source,
            tokens,
            cursor: 0,
        })
    }

    // ------------------------------------------------------------------
    // Lookahead helpers — never advance the cursor.
    // ------------------------------------------------------------------

    /// The current token. Always safe to call — the token vector ends
    /// in an `Eof` terminator so `peek()` returns it past the last real
    /// token.
    pub(crate) fn peek(&self) -> &Token {
        &self.tokens[self.cursor]
    }

    /// The current token kind.
    pub(crate) fn peek_kind(&self) -> &TokenKind {
        &self.peek().kind
    }

    /// The token `n` positions ahead of the cursor. Used for productions
    /// that need up to §7.4's 3-token lookahead bound.
    #[allow(dead_code)] // Used by TASK-221/222/223 productions.
    pub(crate) fn peek_at(&self, n: usize) -> &Token {
        let idx = (self.cursor + n).min(self.tokens.len() - 1);
        &self.tokens[idx]
    }

    /// True if the parser has consumed every real token (the cursor
    /// points at `Eof`).
    pub(crate) fn at_eof(&self) -> bool {
        matches!(self.peek().kind, TokenKind::Eof)
    }

    // ------------------------------------------------------------------
    // Mutating helpers — advance the cursor.
    // ------------------------------------------------------------------

    /// Unconditionally consume the current token and return it.
    #[allow(dead_code)] // Used by TASK-220 CP2 expression grammar and later.
    pub(crate) fn bump(&mut self) -> Token {
        let t = self.tokens[self.cursor].clone();
        if !matches!(t.kind, TokenKind::Eof) {
            self.cursor += 1;
        }
        t
    }

    /// If the current token is the given keyword, consume it and return
    /// it. Otherwise leave the cursor alone and return `None`.
    #[allow(dead_code)] // Used by later production tasks.
    pub(crate) fn try_kw(&mut self, k: Keyword) -> Option<Token> {
        if matches!(self.peek().kind, TokenKind::Kw(got) if got == k) {
            Some(self.bump())
        } else {
            None
        }
    }

    /// If the current token is `kind`, consume it. Useful for trailing
    /// optional punctuation like `;`.
    pub(crate) fn try_kind(&mut self, kind: &TokenKind) -> Option<Token> {
        if std::mem::discriminant(&self.peek().kind) == std::mem::discriminant(kind) {
            Some(self.bump())
        } else {
            None
        }
    }

    /// Require the current token to be the given keyword.
    #[allow(dead_code)] // Used by later production tasks.
    pub(crate) fn expect_kw(&mut self, k: Keyword) -> Result<Token, ParseError> {
        if let Some(t) = self.try_kw(k) {
            Ok(t)
        } else {
            Err(self.error_unexpected(Expected::Keyword(k.canonical()), None))
        }
    }

    /// Require a punctuation token of the given discriminant. The
    /// `punct_label` describes the expected token for error messages
    /// (e.g. `")"`, `","`, `"."`).
    #[allow(dead_code)] // Used by CP2 expression grammar and later.
    pub(crate) fn expect_punct(
        &mut self,
        kind: &TokenKind,
        punct_label: &'static str,
    ) -> Result<Token, ParseError> {
        if let Some(t) = self.try_kind(kind) {
            Ok(t)
        } else {
            Err(self.error_unexpected(Expected::Punct(punct_label), None))
        }
    }

    /// Require a bare identifier (not a keyword, not a quoted name).
    /// Used for grammar positions where a quoted identifier would not
    /// be valid (e.g., `$identifier` variable references, alias names).
    #[allow(dead_code)] // Used by later production tasks.
    pub(crate) fn expect_ident(&mut self) -> Result<(String, Span), ParseError> {
        let tok = self.peek();
        let span = token_span(tok);
        match &tok.kind {
            TokenKind::Ident(text) => {
                let text = text.clone();
                self.bump();
                Ok((text, span))
            }
            TokenKind::Kw(kw) => Err(ParseError::ReservedKeyword {
                offset: tok.start,
                keyword: kw.canonical(),
                role: NameRole::TableName,
            }),
            _ => Err(self.error_unexpected(Expected::Identifier, None)),
        }
    }

    /// Require a user-defined name — a bare identifier or a backtick
    /// name. Returns the AST `Name` node with its span already set.
    ///
    /// Bare keywords are rejected with `ReservedKeyword`; backtick
    /// keyword shadowing is accepted here (per §26.3 rule 3 the
    /// planner rejects it at name resolution).
    pub(crate) fn expect_name(&mut self, role: NameRole) -> Result<Name, ParseError> {
        let tok = self.peek();
        let span = token_span(tok);
        match &tok.kind {
            TokenKind::Ident(text) => {
                let text = text.clone();
                self.bump();
                Ok(Name::new(text, span))
            }
            TokenKind::QuotedName(text) => {
                let text = text.clone();
                self.bump();
                Ok(Name::new(text, span))
            }
            TokenKind::Kw(kw) => Err(ParseError::ReservedKeyword {
                offset: tok.start,
                keyword: kw.canonical(),
                role,
            }),
            _ => Err(self.error_unexpected(Expected::Name, None)),
        }
    }

    /// Require a non-negative integer literal. Used for `LIMIT N` and
    /// similar terminals.
    #[allow(dead_code)] // Used by later production tasks.
    pub(crate) fn expect_int(&mut self) -> Result<(i64, Span), ParseError> {
        let tok = self.peek();
        let span = token_span(tok);
        if let TokenKind::Int(v) = tok.kind {
            self.bump();
            Ok((v, span))
        } else {
            Err(self.error_unexpected(Expected::Integer, None))
        }
    }

    /// Require the current position to be end-of-input. Accepts an
    /// optional trailing `;` (framework-doc §12 #3).
    pub(crate) fn expect_eof(&mut self) -> Result<(), ParseError> {
        // Accept an optional trailing `;`.
        let _ = self.try_kind(&TokenKind::Semicolon);
        if self.at_eof() {
            Ok(())
        } else {
            Err(self.error_unexpected(Expected::Eof, None))
        }
    }

    // ------------------------------------------------------------------
    // Diagnostics
    // ------------------------------------------------------------------

    /// Construct a `ParseError::Unexpected` or `UnexpectedEof` for the
    /// current token. Production functions call this when a call to
    /// `expect_*` fails.
    pub(crate) fn error_unexpected(
        &self,
        expected: Expected,
        detail: Option<&'static str>,
    ) -> ParseError {
        let tok = self.peek();
        if matches!(tok.kind, TokenKind::Eof) {
            ParseError::UnexpectedEof {
                offset: tok.start,
                expected,
                detail,
            }
        } else {
            ParseError::Unexpected {
                offset: tok.start,
                line: tok.line,
                column: tok.column,
                expected,
                found: token_source_text(self.source, tok),
                detail,
            }
        }
    }
}

/// Render a token as a short display snippet for error messages.
fn token_source_text(source: &str, tok: &Token) -> String {
    const MAX: usize = 32;
    let slice = &source[tok.start..tok.end];
    let trimmed = slice.trim_end();
    if trimmed.chars().count() <= MAX {
        trimmed.to_string()
    } else {
        let head: String = trimmed.chars().take(MAX).collect();
        format!("{head}…")
    }
}

/// Top-level statement dispatcher.
///
/// Wave 2 CP1 recognizes only a single bare identifier / backtick name
/// as a source expression and returns a `Statement::Query(Pipeline)`
/// with an empty stage list — matching Wave 1's surface behavior. Later
/// CP2 / TASK-221 / TASK-222 / TASK-223 extend this dispatcher with
/// DDL, DML, expression, and pipeline productions.
pub(crate) fn statement(p: &mut Parser) -> Result<Statement, ParseError> {
    // Empty input is a user-visible error: the parser should say
    // "expected a source table name" rather than a bare "unexpected
    // end of input."
    if p.at_eof() {
        return Err(ParseError::UnexpectedEof {
            offset: p.peek().start,
            expected: Expected::Name,
            detail: Some("expected a table name"),
        });
    }

    // Dispatch on the first token. Wave 2 CP1 only implements the
    // source-expression case; every other first-token variant is either
    // a later-wave statement (CREATE, DROP, ALTER, DESCRIBE, EXPLAIN,
    // INSERT, DELETE) or an error.
    match p.peek_kind() {
        TokenKind::Ident(_) | TokenKind::QuotedName(_) => parse_bare_source_pipeline(p),

        TokenKind::Kw(kw) => {
            // Reserved keyword in source position — the planner-facing
            // error is `ReservedKeyword` so the user sees the specific
            // keyword name rather than a generic "unexpected token."
            let tok = p.peek();
            Err(ParseError::ReservedKeyword {
                offset: tok.start,
                keyword: kw.canonical(),
                role: NameRole::TableName,
            })
        }

        _ => Err(p.error_unexpected(Expected::Name, Some("expected a table name"))),
    }
}

/// Minimal Wave 2 CP1 pipeline: a bare source name with no stages.
fn parse_bare_source_pipeline(p: &mut Parser) -> Result<Statement, ParseError> {
    let name = p.expect_name(NameRole::TableName)?;
    let primary_span = name.span;
    let primary = TableRef {
        name,
        span: primary_span,
    };
    let source = Source {
        primary,
        joins: vec![],
        time_range: None,
        span: primary_span,
    };
    let pipeline = Pipeline {
        source,
        stages: vec![],
        span: primary_span,
    };
    Ok(Statement::Query(pipeline))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_new_lexes_source() {
        let p = Parser::new("events").unwrap();
        assert!(matches!(p.peek().kind, TokenKind::Ident(ref s) if s == "events"));
    }

    #[test]
    fn peek_at_out_of_range_returns_eof() {
        let p = Parser::new("events").unwrap();
        // Only one real token plus Eof; peek_at(99) clamps to Eof.
        assert!(matches!(p.peek_at(99).kind, TokenKind::Eof));
    }

    #[test]
    fn bump_stops_at_eof() {
        let mut p = Parser::new("a").unwrap();
        let _ = p.bump();
        assert!(p.at_eof());
        let _ = p.bump();
        assert!(p.at_eof());
    }

    #[test]
    fn expect_kw_consumes_matching_keyword() {
        let mut p = Parser::new("WHERE").unwrap();
        let tok = p.expect_kw(Keyword::Where).unwrap();
        assert!(matches!(tok.kind, TokenKind::Kw(Keyword::Where)));
        assert!(p.at_eof());
    }

    #[test]
    fn expect_kw_rejects_wrong_keyword() {
        let mut p = Parser::new("SELECT").unwrap();
        match p.expect_kw(Keyword::Where) {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("WHERE"));
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn expect_name_accepts_bare_identifier() {
        let mut p = Parser::new("events").unwrap();
        let name = p.expect_name(NameRole::TableName).unwrap();
        assert_eq!(name.text, "events");
        assert_eq!(name.span.start, 0);
        assert_eq!(name.span.end, 6);
    }

    #[test]
    fn expect_name_accepts_backtick_name() {
        let mut p = Parser::new("`weird name`").unwrap();
        let name = p.expect_name(NameRole::TableName).unwrap();
        assert_eq!(name.text, "weird name");
    }

    #[test]
    fn expect_name_rejects_bare_keyword_with_reserved_error() {
        let mut p = Parser::new("MATCH").unwrap();
        match p.expect_name(NameRole::TableName) {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "MATCH");
                assert_eq!(role, NameRole::TableName);
            }
            other => panic!("expected ReservedKeyword, got {other:?}"),
        }
    }

    #[test]
    fn expect_name_accepts_backtick_wrapped_keyword() {
        // Per §26.3 rule 3, backtick-wrapped keyword names are accepted
        // by the parser; the planner rejects them at name resolution.
        let mut p = Parser::new("`MATCH`").unwrap();
        let name = p.expect_name(NameRole::TableName).unwrap();
        assert_eq!(name.text, "MATCH");
    }

    #[test]
    fn expect_int_consumes_integer() {
        let mut p = Parser::new("100").unwrap();
        let (v, span) = p.expect_int().unwrap();
        assert_eq!(v, 100);
        assert_eq!(span.start, 0);
        assert_eq!(span.end, 3);
    }

    #[test]
    fn expect_int_rejects_non_integer() {
        let mut p = Parser::new("foo").unwrap();
        match p.expect_int() {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Integer);
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn expect_eof_accepts_clean_end() {
        let mut p = Parser::new("events").unwrap();
        let _ = p.expect_name(NameRole::TableName).unwrap();
        p.expect_eof().unwrap();
    }

    #[test]
    fn expect_eof_accepts_trailing_semicolon() {
        let mut p = Parser::new("events;").unwrap();
        let _ = p.expect_name(NameRole::TableName).unwrap();
        p.expect_eof().unwrap();
    }

    #[test]
    fn expect_eof_rejects_trailing_garbage() {
        let mut p = Parser::new("events junk").unwrap();
        let _ = p.expect_name(NameRole::TableName).unwrap();
        match p.expect_eof() {
            Err(ParseError::Unexpected {
                expected, found, ..
            }) => {
                assert_eq!(expected, Expected::Eof);
                assert_eq!(found, "junk");
            }
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn statement_produces_empty_pipeline_for_bare_name() {
        let mut p = Parser::new("events").unwrap();
        let stmt = statement(&mut p).unwrap();
        match stmt {
            Statement::Query(pipe) => {
                assert_eq!(pipe.source.primary.name.text, "events");
                assert!(pipe.stages.is_empty());
                assert!(pipe.source.joins.is_empty());
                assert!(pipe.source.time_range.is_none());
            }
            _ => panic!("expected Query, got {stmt:?}"),
        }
    }

    #[test]
    fn statement_rejects_empty_input_with_helpful_error() {
        let mut p = Parser::new("").unwrap();
        match statement(&mut p) {
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
    fn statement_rejects_bare_keyword_as_source() {
        let mut p = Parser::new("MATCH").unwrap();
        match statement(&mut p) {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "MATCH");
                assert_eq!(role, NameRole::TableName);
            }
            other => panic!("expected ReservedKeyword, got {other:?}"),
        }
    }
}
