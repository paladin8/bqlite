//! BQL lexer.
//!
//! Design: docs/design/language/grammar-framework.md §6.
//!
//! The lexer is a pure function: given a `&str`, it either returns the
//! full stream of tokens (ending in a `TokenKind::Eof`) or a
//! [`crate::error::ParseError`] describing the first failure. The parser
//! then walks the `Vec<Token>` with arbitrary lookahead, never revisiting
//! the source string.

use bqlite_ast::Span;

use crate::error::{LiteralKind, ParseError, UnterminatedKind};

/// A lexed token with its source range.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Token {
    pub kind: TokenKind,
    /// Byte offset of the first byte (inclusive).
    pub start: usize,
    /// Byte offset one past the last byte (exclusive).
    pub end: usize,
    /// 1-indexed line number of `start`.
    pub line: u32,
    /// 1-indexed column number of `start`.
    pub column: u32,
}

/// Convert a lexed token into a `bqlite_ast::Span` for use on AST nodes.
#[inline]
pub(crate) fn token_span(t: &Token) -> Span {
    Span::new(t.start, t.end, t.line, t.column)
}

/// The semantic class of a lexed token.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TokenKind {
    // Literals
    /// A non-negative integer literal. Negative numbers are parsed as
    /// unary minus on a positive `Int` token (query-language.md §26.1
    /// line 1676).
    Int(i64),
    /// A decimal floating-point literal.
    Number(f64),
    /// A fused duration literal (`7d`, `500ms`). Value is nanoseconds.
    Duration(i64),
    /// A single-quoted string literal with `''` escapes resolved.
    String(String),
    /// Boolean literal `true` / `false`.
    Bool(bool),
    /// The `NULL` literal.
    Null,

    // Names
    /// A bare `[A-Za-z_][A-Za-z0-9_]*` identifier (non-keyword).
    Ident(String),
    /// A backtick-quoted name with `` `` `` doubling resolved.
    QuotedName(String),
    /// A `$`-prefixed variable reference. Value is the name after `$`.
    Variable(String),

    // Keywords
    Kw(Keyword),

    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
    Colon,
    Semicolon,
    Pipe,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    /// `~=` — regex-match operator.
    RegexMatch,
    /// `->` — MATCH step separator alias.
    Arrow,

    /// Virtual end-of-input terminator emitted after the last real token.
    Eof,
}

/// Reserved BQL keywords, per query-language.md §26.2. The enum has
/// one variant per canonical keyword — case-insensitive matches live in
/// `keyword_from_upper`.
///
/// Scalar type names (`BOOL`, `INT`, `FLOAT`, `STRING`) carry a
/// trailing underscore on the variant names to avoid collisions with
/// Rust primitive type identifiers.
#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Keyword {
    Add,
    All,
    Alter,
    And,
    As,
    Asc,
    Attribute,
    Avg,
    Between,
    Brackets,
    By,
    Case,
    Cast,
    Coalesce,
    Column,
    Contains,
    Count,
    CountDistinct,
    Create,
    Cumulative,
    Default,
    Delete,
    Desc,
    Describe,
    Distinct,
    Drop,
    Else,
    Emit,
    End,
    Entity,
    Event,
    Explain,
    False,
    First,
    From,
    Funnel,
    Group,
    If,
    Immediately,
    In,
    Insert,
    Into,
    Is,
    Join,
    Key,
    Lag,
    Last,
    Lead,
    Length,
    Let,
    Like,
    Limit,
    List,
    Map,
    Match,
    Max,
    Min,
    Not,
    Nth,
    Null,
    On,
    Or,
    Order,
    Over,
    P50,
    P90,
    P95,
    P99,
    Partition,
    Pivot,
    Query,
    Rank,
    Retention,
    RowNumber,
    Sample,
    Sequence,
    Select,
    Session,
    Sessionize,
    Stats,
    Sum,
    Table,
    Then,
    Time,
    Timestamp,
    True,
    Type,
    Upper,
    Values,
    When,
    Where,
    With,
    Within,
    Without,
    // Scalar type keywords — variant names carry trailing underscore to
    // avoid colliding with Rust primitive type names inside this crate.
    Bool_,
    Int_,
    Float_,
    String_,
}

impl Keyword {
    /// The canonical uppercase spelling used in error messages.
    pub(crate) const fn canonical(self) -> &'static str {
        match self {
            Self::Add => "ADD",
            Self::All => "ALL",
            Self::Alter => "ALTER",
            Self::And => "AND",
            Self::As => "AS",
            Self::Asc => "ASC",
            Self::Attribute => "ATTRIBUTE",
            Self::Avg => "AVG",
            Self::Between => "BETWEEN",
            Self::Brackets => "BRACKETS",
            Self::By => "BY",
            Self::Case => "CASE",
            Self::Cast => "CAST",
            Self::Coalesce => "COALESCE",
            Self::Column => "COLUMN",
            Self::Contains => "CONTAINS",
            Self::Count => "COUNT",
            Self::CountDistinct => "COUNT_DISTINCT",
            Self::Create => "CREATE",
            Self::Cumulative => "CUMULATIVE",
            Self::Default => "DEFAULT",
            Self::Delete => "DELETE",
            Self::Desc => "DESC",
            Self::Describe => "DESCRIBE",
            Self::Distinct => "DISTINCT",
            Self::Drop => "DROP",
            Self::Else => "ELSE",
            Self::Emit => "EMIT",
            Self::End => "END",
            Self::Entity => "ENTITY",
            Self::Event => "EVENT",
            Self::Explain => "EXPLAIN",
            Self::False => "FALSE",
            Self::First => "FIRST",
            Self::From => "FROM",
            Self::Funnel => "FUNNEL",
            Self::Group => "GROUP",
            Self::If => "IF",
            Self::Immediately => "IMMEDIATELY",
            Self::In => "IN",
            Self::Insert => "INSERT",
            Self::Into => "INTO",
            Self::Is => "IS",
            Self::Join => "JOIN",
            Self::Key => "KEY",
            Self::Lag => "LAG",
            Self::Last => "LAST",
            Self::Lead => "LEAD",
            Self::Length => "LENGTH",
            Self::Let => "LET",
            Self::Like => "LIKE",
            Self::Limit => "LIMIT",
            Self::List => "LIST",
            Self::Map => "MAP",
            Self::Match => "MATCH",
            Self::Max => "MAX",
            Self::Min => "MIN",
            Self::Not => "NOT",
            Self::Nth => "NTH",
            Self::Null => "NULL",
            Self::On => "ON",
            Self::Or => "OR",
            Self::Order => "ORDER",
            Self::Over => "OVER",
            Self::P50 => "P50",
            Self::P90 => "P90",
            Self::P95 => "P95",
            Self::P99 => "P99",
            Self::Partition => "PARTITION",
            Self::Pivot => "PIVOT",
            Self::Query => "QUERY",
            Self::Rank => "RANK",
            Self::Retention => "RETENTION",
            Self::RowNumber => "ROW_NUMBER",
            Self::Sample => "SAMPLE",
            Self::Sequence => "SEQUENCE",
            Self::Select => "SELECT",
            Self::Session => "SESSION",
            Self::Sessionize => "SESSIONIZE",
            Self::Stats => "STATS",
            Self::Sum => "SUM",
            Self::Table => "TABLE",
            Self::Then => "THEN",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::True => "TRUE",
            Self::Type => "TYPE",
            Self::Upper => "UPPER",
            Self::Values => "VALUES",
            Self::When => "WHEN",
            Self::Where => "WHERE",
            Self::With => "WITH",
            Self::Within => "WITHIN",
            Self::Without => "WITHOUT",
            Self::Bool_ => "BOOL",
            Self::Int_ => "INT",
            Self::Float_ => "FLOAT",
            Self::String_ => "STRING",
        }
    }
}

/// Given an ASCII-uppercased identifier, return its `Keyword` if any.
///
/// The lexer uppercases the bare identifier token text before calling
/// this helper, so every entry in the match is the canonical form.
fn keyword_from_upper(upper: &str) -> Option<Keyword> {
    // A linear match is fine — BQL has ~90 keywords and the typical
    // input has short identifiers. A hash table or perfect-hash would
    // save a few tens of nanoseconds per keyword at the cost of build
    // complexity.
    Some(match upper {
        "ADD" => Keyword::Add,
        "ALL" => Keyword::All,
        "ALTER" => Keyword::Alter,
        "AND" => Keyword::And,
        "AS" => Keyword::As,
        "ASC" => Keyword::Asc,
        "ATTRIBUTE" => Keyword::Attribute,
        "AVG" => Keyword::Avg,
        "BETWEEN" => Keyword::Between,
        "BRACKETS" => Keyword::Brackets,
        "BY" => Keyword::By,
        "CASE" => Keyword::Case,
        "CAST" => Keyword::Cast,
        "COALESCE" => Keyword::Coalesce,
        "COLUMN" => Keyword::Column,
        "CONTAINS" => Keyword::Contains,
        "COUNT" => Keyword::Count,
        "COUNT_DISTINCT" => Keyword::CountDistinct,
        "CREATE" => Keyword::Create,
        "CUMULATIVE" => Keyword::Cumulative,
        "DEFAULT" => Keyword::Default,
        "DELETE" => Keyword::Delete,
        "DESC" => Keyword::Desc,
        "DESCRIBE" => Keyword::Describe,
        "DISTINCT" => Keyword::Distinct,
        "DROP" => Keyword::Drop,
        "ELSE" => Keyword::Else,
        "EMIT" => Keyword::Emit,
        "END" => Keyword::End,
        "ENTITY" => Keyword::Entity,
        "EVENT" => Keyword::Event,
        "EXPLAIN" => Keyword::Explain,
        "FALSE" => Keyword::False,
        "FIRST" => Keyword::First,
        "FROM" => Keyword::From,
        "FUNNEL" => Keyword::Funnel,
        "GROUP" => Keyword::Group,
        "IF" => Keyword::If,
        "IMMEDIATELY" => Keyword::Immediately,
        "IN" => Keyword::In,
        "INSERT" => Keyword::Insert,
        "INTO" => Keyword::Into,
        "IS" => Keyword::Is,
        "JOIN" => Keyword::Join,
        "KEY" => Keyword::Key,
        "LAG" => Keyword::Lag,
        "LAST" => Keyword::Last,
        "LEAD" => Keyword::Lead,
        "LENGTH" => Keyword::Length,
        "LET" => Keyword::Let,
        "LIKE" => Keyword::Like,
        "LIMIT" => Keyword::Limit,
        "LIST" => Keyword::List,
        "MAP" => Keyword::Map,
        "MATCH" => Keyword::Match,
        "MAX" => Keyword::Max,
        "MIN" => Keyword::Min,
        "NOT" => Keyword::Not,
        "NTH" => Keyword::Nth,
        "NULL" => Keyword::Null,
        "ON" => Keyword::On,
        "OR" => Keyword::Or,
        "ORDER" => Keyword::Order,
        "OVER" => Keyword::Over,
        "P50" => Keyword::P50,
        "P90" => Keyword::P90,
        "P95" => Keyword::P95,
        "P99" => Keyword::P99,
        "PARTITION" => Keyword::Partition,
        "PIVOT" => Keyword::Pivot,
        "QUERY" => Keyword::Query,
        "RANK" => Keyword::Rank,
        "RETENTION" => Keyword::Retention,
        "ROW_NUMBER" => Keyword::RowNumber,
        "SAMPLE" => Keyword::Sample,
        "SEQUENCE" => Keyword::Sequence,
        "SELECT" => Keyword::Select,
        "SESSION" => Keyword::Session,
        "SESSIONIZE" => Keyword::Sessionize,
        "STATS" => Keyword::Stats,
        "SUM" => Keyword::Sum,
        "TABLE" => Keyword::Table,
        "THEN" => Keyword::Then,
        "TIME" => Keyword::Time,
        "TIMESTAMP" => Keyword::Timestamp,
        "TRUE" => Keyword::True,
        "TYPE" => Keyword::Type,
        "UPPER" => Keyword::Upper,
        "VALUES" => Keyword::Values,
        "WHEN" => Keyword::When,
        "WHERE" => Keyword::Where,
        "WITH" => Keyword::With,
        "WITHIN" => Keyword::Within,
        "WITHOUT" => Keyword::Without,
        "BOOL" => Keyword::Bool_,
        "INT" => Keyword::Int_,
        "FLOAT" => Keyword::Float_,
        "STRING" => Keyword::String_,
        _ => return None,
    })
}

/// Lex an entire BQL source string into a token stream.
///
/// The returned `Vec` always ends with a single `TokenKind::Eof` token
/// whose span points at the end of the source. Comments are stripped.
pub(crate) fn lex(source: &str) -> Result<Vec<Token>, ParseError> {
    let mut scanner = Scanner::new(source);
    let mut out: Vec<Token> = Vec::new();
    loop {
        scanner.skip_trivia()?;
        if scanner.eof() {
            out.push(Token {
                kind: TokenKind::Eof,
                start: scanner.pos,
                end: scanner.pos,
                line: scanner.line,
                column: scanner.column,
            });
            return Ok(out);
        }
        let tok = scanner.next_token()?;
        out.push(tok);
    }
}

/// Private single-pass scanner backing `lex`.
struct Scanner<'s> {
    source: &'s str,
    bytes: &'s [u8],
    pos: usize,
    line: u32,
    column: u32,
}

impl<'s> Scanner<'s> {
    fn new(source: &'s str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            pos: 0,
            line: 1,
            column: 1,
        }
    }

    #[inline]
    fn eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    #[inline]
    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    #[inline]
    fn peek_byte_at(&self, n: usize) -> Option<u8> {
        self.bytes.get(self.pos + n).copied()
    }

    /// Advance one byte and bump `line`/`column`.
    ///
    /// Caller must ensure the byte is a complete ASCII character; this
    /// method must not be used inside string-literal or backtick
    /// scanning where multi-byte UTF-8 may appear.
    fn bump_ascii(&mut self) -> u8 {
        let b = self.bytes[self.pos];
        self.pos += 1;
        if b == b'\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        b
    }

    /// Advance by `n` ASCII bytes in a single step, known to contain
    /// no newlines (the caller must have verified this).
    fn bump_ascii_run(&mut self, n: usize) {
        debug_assert!(self.pos + n <= self.bytes.len());
        debug_assert!(!self.bytes[self.pos..self.pos + n].contains(&b'\n'));
        self.pos += n;
        self.column += n as u32;
    }

    fn skip_trivia(&mut self) -> Result<(), ParseError> {
        loop {
            match self.peek_byte() {
                Some(b) if b.is_ascii_whitespace() => {
                    self.bump_ascii();
                }
                Some(b'-') if self.peek_byte_at(1) == Some(b'-') => {
                    // Line comment. Consume until newline or EOF.
                    while let Some(b) = self.peek_byte() {
                        if b == b'\n' {
                            break;
                        }
                        self.bump_ascii();
                    }
                }
                Some(b'/') if self.peek_byte_at(1) == Some(b'*') => {
                    let start = self.pos;
                    self.bump_ascii();
                    self.bump_ascii();
                    // Consume until `*/`. Nested block comments are
                    // not supported per framework-doc §6.6.
                    loop {
                        match self.peek_byte() {
                            Some(b'*') if self.peek_byte_at(1) == Some(b'/') => {
                                self.bump_ascii();
                                self.bump_ascii();
                                break;
                            }
                            Some(_) => {
                                // Block comments may span lines and may
                                // contain non-ASCII bytes. Advance by
                                // the current char's UTF-8 width.
                                let remaining = &self.source[self.pos..];
                                let ch = remaining.chars().next().unwrap();
                                let ch_len = ch.len_utf8();
                                if ch == '\n' {
                                    self.line += 1;
                                    self.column = 1;
                                } else {
                                    self.column += 1;
                                }
                                self.pos += ch_len;
                            }
                            None => {
                                return Err(ParseError::Unterminated {
                                    offset: start,
                                    kind: UnterminatedKind::BlockComment,
                                });
                            }
                        }
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        let start = self.pos;
        let line = self.line;
        let column = self.column;
        let first = self.peek_byte().expect("caller skipped EOF");

        // Dispatch on the first byte.
        match first {
            b'(' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::LParen, start, line, column))
            }
            b')' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::RParen, start, line, column))
            }
            b'[' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::LBracket, start, line, column))
            }
            b']' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::RBracket, start, line, column))
            }
            b',' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Comma, start, line, column))
            }
            b'.' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Dot, start, line, column))
            }
            b':' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Colon, start, line, column))
            }
            b';' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Semicolon, start, line, column))
            }
            b'|' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Pipe, start, line, column))
            }
            b'+' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Plus, start, line, column))
            }
            b'*' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Star, start, line, column))
            }
            b'%' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Percent, start, line, column))
            }
            b'/' => {
                // Block comments already skipped in skip_trivia — a raw
                // `/` here means division.
                self.bump_ascii();
                Ok(self.token(TokenKind::Slash, start, line, column))
            }
            b'=' => {
                self.bump_ascii();
                Ok(self.token(TokenKind::Eq, start, line, column))
            }
            b'-' => {
                // `-` is unary/binary minus, or the start of a `->`.
                if self.peek_byte_at(1) == Some(b'>') {
                    self.bump_ascii();
                    self.bump_ascii();
                    Ok(self.token(TokenKind::Arrow, start, line, column))
                } else {
                    self.bump_ascii();
                    Ok(self.token(TokenKind::Minus, start, line, column))
                }
            }
            b'<' => {
                self.bump_ascii();
                if self.peek_byte() == Some(b'=') {
                    self.bump_ascii();
                    Ok(self.token(TokenKind::LtEq, start, line, column))
                } else if self.peek_byte() == Some(b'>') {
                    self.bump_ascii();
                    Ok(self.token(TokenKind::NotEq, start, line, column))
                } else {
                    Ok(self.token(TokenKind::Lt, start, line, column))
                }
            }
            b'>' => {
                self.bump_ascii();
                if self.peek_byte() == Some(b'=') {
                    self.bump_ascii();
                    Ok(self.token(TokenKind::GtEq, start, line, column))
                } else {
                    Ok(self.token(TokenKind::Gt, start, line, column))
                }
            }
            b'!' => {
                if self.peek_byte_at(1) == Some(b'=') {
                    self.bump_ascii();
                    self.bump_ascii();
                    Ok(self.token(TokenKind::NotEq, start, line, column))
                } else {
                    self.lex_error_unexpected(start, line, column, "!")
                }
            }
            b'~' => {
                if self.peek_byte_at(1) == Some(b'=') {
                    self.bump_ascii();
                    self.bump_ascii();
                    Ok(self.token(TokenKind::RegexMatch, start, line, column))
                } else {
                    self.lex_error_unexpected(start, line, column, "~")
                }
            }
            b'$' => {
                // `$ident` — variable reference.
                self.bump_ascii();
                let name = self.scan_ident_body_or_err(start, line, column)?;
                Ok(self.token(TokenKind::Variable(name), start, line, column))
            }
            b'`' => self.lex_backtick_name(start, line, column),
            b'\'' => self.lex_string_literal(start, line, column),
            b'0'..=b'9' => self.lex_number_or_duration(start, line, column),
            b if is_ident_start_byte(b) => self.lex_ident_or_keyword(start, line, column),
            b if b >= 0x80 => self.lex_error_non_ascii(start, line, column),
            b'"' => self.lex_error_unexpected(start, line, column, "\""),
            _ => {
                // Any other ASCII byte is junk.
                let snippet: String = char::from(first).to_string();
                self.lex_error_unexpected(start, line, column, &snippet)
            }
        }
    }

    fn token(&self, kind: TokenKind, start: usize, line: u32, column: u32) -> Token {
        Token {
            kind,
            start,
            end: self.pos,
            line,
            column,
        }
    }

    fn lex_error_unexpected(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
        found: &str,
    ) -> Result<Token, ParseError> {
        // Advance past the offending byte so repeated calls make progress;
        // the parser halts after the first error anyway so this mostly
        // matters in tests.
        if !self.eof() {
            let b = self.bytes[self.pos];
            if b < 0x80 {
                self.bump_ascii();
            }
        }
        Err(ParseError::Unexpected {
            offset: start,
            line,
            column,
            // Lex-level junk — the byte cannot start any valid token
            // shape, so the best we can say is "expected some token."
            // The `detail` field carries the user-actionable hint.
            expected: crate::error::Expected::Token,
            found: found.to_string(),
            detail: Some("unrecognized character"),
        })
    }

    fn lex_error_non_ascii(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
    ) -> Result<Token, ParseError> {
        // Take the offending character for the error snippet.
        let remaining = &self.source[self.pos..];
        let ch = remaining.chars().next().unwrap();
        Err(ParseError::Unexpected {
            offset: start,
            line,
            column,
            expected: crate::error::Expected::Identifier,
            found: ch.to_string(),
            detail: Some("BQL identifiers must be ASCII"),
        })
    }

    fn scan_ident_body_or_err(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
    ) -> Result<String, ParseError> {
        // A `$` has just been consumed; at least one ident char must follow.
        let first = self.peek_byte();
        match first {
            Some(b) if is_ident_start_byte(b) => {}
            _ => {
                return Err(ParseError::Unexpected {
                    offset: start,
                    line,
                    column,
                    expected: crate::error::Expected::Identifier,
                    found: "$".to_string(),
                    detail: Some("`$` must be followed by a variable name"),
                });
            }
        }
        let body_start = self.pos;
        while let Some(b) = self.peek_byte() {
            if is_ident_cont_byte(b) {
                self.bump_ascii();
            } else {
                break;
            }
        }
        Ok(self.source[body_start..self.pos].to_string())
    }

    fn lex_ident_or_keyword(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
    ) -> Result<Token, ParseError> {
        while let Some(b) = self.peek_byte() {
            if is_ident_cont_byte(b) {
                self.bump_ascii();
            } else {
                break;
            }
        }
        let text = &self.source[start..self.pos];
        // Special case: bare `true`/`false`/`null` are literal tokens,
        // not keyword tokens, so the parser does not have to pattern
        // match on Kw(True) / Kw(False) / Kw(Null) everywhere.
        let upper = text.to_ascii_uppercase();
        let kind = match upper.as_str() {
            "TRUE" => TokenKind::Bool(true),
            "FALSE" => TokenKind::Bool(false),
            "NULL" => TokenKind::Null,
            _ => match keyword_from_upper(upper.as_str()) {
                Some(kw) => TokenKind::Kw(kw),
                None => TokenKind::Ident(text.to_string()),
            },
        };
        Ok(self.token(kind, start, line, column))
    }

    fn lex_backtick_name(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
    ) -> Result<Token, ParseError> {
        // Skip the opening backtick.
        self.bump_ascii();
        let mut buf = String::new();
        loop {
            let remaining = &self.source[self.pos..];
            let Some(ch) = remaining.chars().next() else {
                return Err(ParseError::Unterminated {
                    offset: start,
                    kind: UnterminatedKind::BacktickIdent,
                });
            };
            if ch == '\n' {
                // §23.1: newline not allowed inside backtick names.
                return Err(ParseError::Unterminated {
                    offset: start,
                    kind: UnterminatedKind::BacktickIdent,
                });
            }
            let ch_len = ch.len_utf8();
            if ch == '`' {
                // Doubled backtick = literal; single backtick = close.
                if self.bytes.get(self.pos + 1) == Some(&b'`') {
                    buf.push('`');
                    self.pos += 2;
                    self.column += 2;
                    continue;
                }
                // Close.
                self.pos += 1;
                self.column += 1;
                break;
            }
            buf.push(ch);
            self.pos += ch_len;
            self.column += 1;
        }
        if buf.is_empty() {
            return Err(ParseError::InvalidLiteral {
                offset: start,
                kind: LiteralKind::String,
                reason: "empty backtick identifier".into(),
            });
        }
        Ok(self.token(TokenKind::QuotedName(buf), start, line, column))
    }

    fn lex_string_literal(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
    ) -> Result<Token, ParseError> {
        // Skip the opening quote.
        self.bump_ascii();
        let mut buf = String::new();
        loop {
            let remaining = &self.source[self.pos..];
            let Some(ch) = remaining.chars().next() else {
                return Err(ParseError::Unterminated {
                    offset: start,
                    kind: UnterminatedKind::StringLit,
                });
            };
            let ch_len = ch.len_utf8();
            if ch == '\'' {
                // `''` → literal single quote. Single `'` → close.
                if self.bytes.get(self.pos + 1) == Some(&b'\'') {
                    buf.push('\'');
                    self.pos += 2;
                    self.column += 2;
                    continue;
                }
                self.pos += 1;
                self.column += 1;
                break;
            }
            if ch == '\n' {
                // Multi-line string literals are not supported in v1.
                // Users needing newlines must encode them at ingest.
                return Err(ParseError::Unterminated {
                    offset: start,
                    kind: UnterminatedKind::StringLit,
                });
            }
            buf.push(ch);
            self.pos += ch_len;
            self.column += 1;
        }
        Ok(self.token(TokenKind::String(buf), start, line, column))
    }

    fn lex_number_or_duration(
        &mut self,
        start: usize,
        line: u32,
        column: u32,
    ) -> Result<Token, ParseError> {
        // Consume the run of ASCII digits.
        while let Some(b) = self.peek_byte() {
            if b.is_ascii_digit() {
                self.bump_ascii();
            } else {
                break;
            }
        }
        let int_end = self.pos;

        // Try duration suffix (longest match: 2-char ns/us/ms before 1-char).
        if let Some(unit_len) = self.peek_duration_unit() {
            let int_text = &self.source[start..int_end];
            let int_val: i64 = int_text.parse().map_err(|_| ParseError::InvalidLiteral {
                offset: start,
                kind: LiteralKind::Duration,
                reason: format!("integer part `{int_text}` does not fit in i64"),
            })?;
            let unit_slice = &self.source[int_end..int_end + unit_len];
            let nanos_per_unit = match unit_slice {
                "ns" => 1i64,
                "us" => 1_000,
                "ms" => 1_000_000,
                "s" => 1_000_000_000,
                "m" => 60 * 1_000_000_000,
                "h" => 3_600 * 1_000_000_000,
                "d" => 86_400 * 1_000_000_000,
                _ => unreachable!("peek_duration_unit returned invalid length"),
            };
            let nanos =
                int_val
                    .checked_mul(nanos_per_unit)
                    .ok_or_else(|| ParseError::InvalidLiteral {
                        offset: start,
                        kind: LiteralKind::Duration,
                        reason: format!("{int_val}{unit_slice} overflows i64 nanoseconds"),
                    })?;
            self.bump_ascii_run(unit_len);
            return Ok(self.token(TokenKind::Duration(nanos), start, line, column));
        }

        // Try float: `<digits>.<digits>`.
        if self.peek_byte() == Some(b'.')
            && self
                .peek_byte_at(1)
                .map(|b| b.is_ascii_digit())
                .unwrap_or(false)
        {
            self.bump_ascii(); // consume '.'
            while let Some(b) = self.peek_byte() {
                if b.is_ascii_digit() {
                    self.bump_ascii();
                } else {
                    break;
                }
            }
            let text = &self.source[start..self.pos];
            let value: f64 = text.parse().map_err(|_| ParseError::InvalidLiteral {
                offset: start,
                kind: LiteralKind::Float,
                reason: format!("`{text}` is not a valid floating-point literal"),
            })?;
            return Ok(self.token(TokenKind::Number(value), start, line, column));
        }

        // Plain integer.
        let text = &self.source[start..int_end];
        let value: i64 = text.parse().map_err(|_| ParseError::InvalidLiteral {
            offset: start,
            kind: LiteralKind::Int,
            reason: format!("`{text}` does not fit in i64"),
        })?;
        Ok(self.token(TokenKind::Int(value), start, line, column))
    }

    /// If the current byte starts a valid duration unit suffix, return
    /// its byte length. `ns`/`us`/`ms` are preferred over bare `s`/`m`
    /// per longest-match.
    fn peek_duration_unit(&self) -> Option<usize> {
        let a = self.peek_byte()?;
        let b = self.peek_byte_at(1);
        // 2-char units first.
        match (a, b) {
            (b'n', Some(b's')) => {
                if !self.ident_continues_after(2) {
                    return Some(2);
                }
            }
            (b'u', Some(b's')) => {
                if !self.ident_continues_after(2) {
                    return Some(2);
                }
            }
            (b'm', Some(b's')) => {
                if !self.ident_continues_after(2) {
                    return Some(2);
                }
            }
            _ => {}
        }
        // 1-char units.
        match a {
            b's' | b'm' | b'h' | b'd' => {
                if !self.ident_continues_after(1) {
                    return Some(1);
                }
            }
            _ => {}
        }
        None
    }

    /// True if the byte at `pos + n` is an ident-continue byte, meaning
    /// the candidate unit prefix is actually part of a longer identifier
    /// (e.g., `5day_count` is not a duration, it's `5` followed by the
    /// identifier `day_count`).
    fn ident_continues_after(&self, n: usize) -> bool {
        match self.bytes.get(self.pos + n) {
            Some(b) => is_ident_cont_byte(*b),
            None => false,
        }
    }
}

#[inline]
fn is_ident_start_byte(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

#[inline]
fn is_ident_cont_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_kinds(s: &str) -> Vec<TokenKind> {
        lex(s).unwrap().into_iter().map(|t| t.kind).collect()
    }

    #[test]
    fn lexes_bare_identifier() {
        let toks = lex("events").unwrap();
        assert_eq!(toks.len(), 2);
        assert!(matches!(toks[0].kind, TokenKind::Ident(ref s) if s == "events"));
        assert!(matches!(toks[1].kind, TokenKind::Eof));
        assert_eq!(toks[0].start, 0);
        assert_eq!(toks[0].end, 6);
        assert_eq!(toks[0].line, 1);
        assert_eq!(toks[0].column, 1);
    }

    #[test]
    fn lexes_identifier_case_preserved() {
        let toks = lex_kinds("UserID");
        assert!(matches!(toks[0], TokenKind::Ident(ref s) if s == "UserID"));
    }

    #[test]
    fn lexes_reserved_keyword_case_insensitive() {
        // lowercase keyword still becomes Kw(Where)
        assert_eq!(
            lex_kinds("where"),
            vec![TokenKind::Kw(Keyword::Where), TokenKind::Eof]
        );
        assert_eq!(
            lex_kinds("WHERE"),
            vec![TokenKind::Kw(Keyword::Where), TokenKind::Eof]
        );
        assert_eq!(
            lex_kinds("WheRe"),
            vec![TokenKind::Kw(Keyword::Where), TokenKind::Eof]
        );
    }

    #[test]
    fn lexes_true_false_null_as_literals_not_keywords() {
        assert_eq!(lex_kinds("true")[0], TokenKind::Bool(true));
        assert_eq!(lex_kinds("FALSE")[0], TokenKind::Bool(false));
        assert_eq!(lex_kinds("null")[0], TokenKind::Null);
    }

    #[test]
    fn lexes_count_distinct_as_single_keyword() {
        assert_eq!(
            lex_kinds("COUNT_DISTINCT")[0],
            TokenKind::Kw(Keyword::CountDistinct)
        );
        assert_eq!(
            lex_kinds("ROW_NUMBER")[0],
            TokenKind::Kw(Keyword::RowNumber)
        );
    }

    #[test]
    fn lexes_integer_literal() {
        assert_eq!(lex_kinds("42")[0], TokenKind::Int(42));
        assert_eq!(lex_kinds("0")[0], TokenKind::Int(0));
    }

    #[test]
    fn lexes_float_literal() {
        assert_eq!(lex_kinds("2.5")[0], TokenKind::Number(2.5));
        assert_eq!(lex_kinds("0.125")[0], TokenKind::Number(0.125));
    }

    #[test]
    fn lexes_every_duration_unit() {
        assert_eq!(
            lex_kinds("7d")[0],
            TokenKind::Duration(7 * 86_400 * 1_000_000_000)
        );
        assert_eq!(
            lex_kinds("12h")[0],
            TokenKind::Duration(12 * 3_600 * 1_000_000_000)
        );
        assert_eq!(
            lex_kinds("30m")[0],
            TokenKind::Duration(30 * 60 * 1_000_000_000)
        );
        assert_eq!(lex_kinds("10s")[0], TokenKind::Duration(10 * 1_000_000_000));
        assert_eq!(lex_kinds("500ms")[0], TokenKind::Duration(500 * 1_000_000));
        assert_eq!(lex_kinds("100us")[0], TokenKind::Duration(100 * 1_000));
        assert_eq!(lex_kinds("50ns")[0], TokenKind::Duration(50));
    }

    #[test]
    fn duration_prefers_long_unit_over_short() {
        // "3ms" must be 3 ms, not "3m" + "s".
        assert_eq!(lex_kinds("3ms")[0], TokenKind::Duration(3 * 1_000_000));
    }

    #[test]
    fn digits_followed_by_identifier_are_not_duration() {
        // `5day_count` is junk: Int(5) then Ident("day_count").
        // The ident-continues-after check rules out `5d` as a duration.
        let toks = lex_kinds("5day_count");
        assert_eq!(toks[0], TokenKind::Int(5));
        assert!(matches!(toks[1], TokenKind::Ident(ref s) if s == "day_count"));
    }

    #[test]
    fn duration_overflow_errors() {
        match lex("99999999999999d") {
            Err(ParseError::InvalidLiteral {
                kind: LiteralKind::Duration,
                ..
            }) => {}
            other => panic!("expected duration overflow, got {other:?}"),
        }
    }

    #[test]
    fn lexes_string_literal() {
        let toks = lex_kinds("'hello'");
        assert!(matches!(toks[0], TokenKind::String(ref s) if s == "hello"));
    }

    #[test]
    fn lexes_string_literal_with_doubled_quote() {
        let toks = lex_kinds("'it''s'");
        assert!(matches!(toks[0], TokenKind::String(ref s) if s == "it's"));
    }

    #[test]
    fn unterminated_string_errors() {
        match lex("'never_closed") {
            Err(ParseError::Unterminated {
                kind: UnterminatedKind::StringLit,
                offset: 0,
            }) => {}
            other => panic!("expected unterminated string, got {other:?}"),
        }
    }

    #[test]
    fn string_literal_rejects_embedded_newline() {
        match lex("'line\nbreak'") {
            Err(ParseError::Unterminated {
                kind: UnterminatedKind::StringLit,
                ..
            }) => {}
            other => panic!("expected unterminated-string error, got {other:?}"),
        }
    }

    #[test]
    fn lexes_backtick_quoted_name() {
        let toks = lex_kinds("`weird name`");
        assert!(matches!(toks[0], TokenKind::QuotedName(ref s) if s == "weird name"));
    }

    #[test]
    fn lexes_backtick_doubling() {
        let toks = lex_kinds("`a``b`");
        assert!(matches!(toks[0], TokenKind::QuotedName(ref s) if s == "a`b"));
    }

    #[test]
    fn empty_backtick_errors() {
        match lex("``") {
            Err(ParseError::InvalidLiteral {
                kind: LiteralKind::String,
                ..
            }) => {}
            other => panic!("expected empty backtick error, got {other:?}"),
        }
    }

    #[test]
    fn unterminated_backtick_errors() {
        match lex("`never_closed") {
            Err(ParseError::Unterminated {
                kind: UnterminatedKind::BacktickIdent,
                ..
            }) => {}
            other => panic!("expected unterminated backtick, got {other:?}"),
        }
    }

    #[test]
    fn backtick_rejects_embedded_newline() {
        match lex("`line\nbreak`") {
            Err(ParseError::Unterminated {
                kind: UnterminatedKind::BacktickIdent,
                ..
            }) => {}
            other => panic!("expected unterminated backtick, got {other:?}"),
        }
    }

    #[test]
    fn lexes_variable_reference() {
        let toks = lex_kinds("$product");
        assert!(matches!(toks[0], TokenKind::Variable(ref s) if s == "product"));
    }

    #[test]
    fn lone_dollar_errors() {
        match lex("$") {
            Err(ParseError::Unexpected { detail, .. }) => {
                // Detail must survive refactors so users keep seeing
                // the variable-name hint.
                assert_eq!(detail, Some("`$` must be followed by a variable name"));
            }
            other => panic!("expected unexpected error for lone $, got {other:?}"),
        }
    }

    #[test]
    fn lexes_punctuation() {
        use TokenKind::*;
        assert_eq!(
            lex_kinds("(),.;:|+-*/%"),
            vec![
                LParen, RParen, Comma, Dot, Semicolon, Colon, Pipe, Plus, Minus, Star, Slash,
                Percent, Eof
            ]
        );
    }

    #[test]
    fn lexes_comparison_operators() {
        use TokenKind::*;
        assert_eq!(
            lex_kinds("= != <> < <= > >="),
            vec![Eq, NotEq, NotEq, Lt, LtEq, Gt, GtEq, Eof]
        );
    }

    #[test]
    fn lexes_arrow_and_regex_match() {
        use TokenKind::*;
        assert_eq!(lex_kinds("-> ~="), vec![Arrow, RegexMatch, Eof]);
    }

    #[test]
    fn skips_line_comment() {
        let toks = lex_kinds("events -- this is a comment\nWHERE");
        assert!(matches!(toks[0], TokenKind::Ident(ref s) if s == "events"));
        assert_eq!(toks[1], TokenKind::Kw(Keyword::Where));
    }

    #[test]
    fn skips_block_comment() {
        let toks = lex_kinds("events /* block\ncomment */ WHERE");
        assert!(matches!(toks[0], TokenKind::Ident(ref s) if s == "events"));
        assert_eq!(toks[1], TokenKind::Kw(Keyword::Where));
    }

    #[test]
    fn line_comment_without_trailing_newline_is_accepted() {
        // `events --trailing` with no \n before EOF — the comment must
        // still terminate at EOF, leaving just the `events` token.
        let toks = lex("events -- trailing").unwrap();
        assert_eq!(toks.len(), 2);
        assert!(matches!(toks[0].kind, TokenKind::Ident(ref s) if s == "events"));
        assert_eq!(toks[1].kind, TokenKind::Eof);
    }

    #[test]
    fn comments_without_surrounding_whitespace() {
        // `a/*mid*/b` and `a--tail\nb` — comments adjacent to idents.
        let toks = lex_kinds("a/*mid*/b");
        assert!(matches!(toks[0], TokenKind::Ident(ref s) if s == "a"));
        assert!(matches!(toks[1], TokenKind::Ident(ref s) if s == "b"));
        let toks = lex_kinds("a--tail\nb");
        assert!(matches!(toks[0], TokenKind::Ident(ref s) if s == "a"));
        assert!(matches!(toks[1], TokenKind::Ident(ref s) if s == "b"));
    }

    #[test]
    fn line_comment_at_start_of_input() {
        // Leading `--` comment, then a token.
        let toks = lex_kinds("-- header\nevents");
        assert!(matches!(toks[0], TokenKind::Ident(ref s) if s == "events"));
    }

    #[test]
    fn unknown_byte_errors_with_token_category() {
        match lex("@") {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, crate::error::Expected::Token);
                assert_eq!(detail, Some("unrecognized character"));
            }
            other => panic!("expected Unexpected with Token category, got {other:?}"),
        }
    }

    #[test]
    fn unterminated_block_comment_errors() {
        match lex("events /* never closed") {
            Err(ParseError::Unterminated {
                kind: UnterminatedKind::BlockComment,
                ..
            }) => {}
            other => panic!("expected unterminated block comment, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_punctuation() {
        match lex("events @ 5") {
            Err(ParseError::Unexpected { found, .. }) => assert_eq!(found, "@"),
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_lone_bang() {
        match lex("!foo") {
            Err(ParseError::Unexpected { found, .. }) => assert_eq!(found, "!"),
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_lone_tilde() {
        match lex("~foo") {
            Err(ParseError::Unexpected { found, .. }) => assert_eq!(found, "~"),
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_ascii_identifier() {
        match lex("événements") {
            Err(ParseError::Unexpected { found, .. }) => {
                assert!(!found.is_empty());
                assert!(found.chars().next().unwrap() as u32 >= 0x80);
            }
            other => panic!("expected Unexpected for non-ASCII, got {other:?}"),
        }
    }

    #[test]
    fn rejects_double_quote_outside_string() {
        match lex("x \"y\"") {
            Err(ParseError::Unexpected { found, .. }) => assert_eq!(found, "\""),
            other => panic!("expected Unexpected, got {other:?}"),
        }
    }

    #[test]
    fn tracks_line_column_through_newlines() {
        let toks = lex("a\n\n  b").unwrap();
        // First token: `a` at (1, 1)
        assert_eq!((toks[0].line, toks[0].column), (1, 1));
        // Second token: `b` at (3, 3)
        assert_eq!((toks[1].line, toks[1].column), (3, 3));
    }

    #[test]
    fn token_span_helper_reports_correct_span() {
        let toks = lex("events").unwrap();
        let span = token_span(&toks[0]);
        assert_eq!(span.start, 0);
        assert_eq!(span.end, 6);
        assert_eq!(span.line, 1);
        assert_eq!(span.column, 1);
    }

    #[test]
    fn eof_token_points_at_source_end() {
        let toks = lex("events ").unwrap();
        let eof = toks.last().unwrap();
        assert_eq!(eof.kind, TokenKind::Eof);
        assert_eq!(eof.start, 7);
        assert_eq!(eof.end, 7);
    }

    #[test]
    fn empty_input_produces_only_eof() {
        let toks = lex("").unwrap();
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].kind, TokenKind::Eof);
    }

    #[test]
    fn whitespace_only_input_produces_only_eof() {
        let toks = lex("   \t\n  ").unwrap();
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].kind, TokenKind::Eof);
    }
}
