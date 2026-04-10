//! Source spans for AST nodes.
//!
//! Every AST node carries a [`Span`] pointing into the original BQL source
//! text so that error messages from the planner and later stages can quote
//! the offending fragment. See docs/design/query-language.md §27
//! (Error strategy) — every error must include a source span.
//!
//! The parser populates spans as it produces nodes; synthetic nodes
//! produced by desugaring or tests use [`Span::EMPTY`].

use serde::{Deserialize, Serialize};

/// A byte range in the source text, with line/column for diagnostics.
///
/// `start..end` is a byte-offset half-open range into the original BQL
/// source text (`end` is exclusive). `line` and `column` are 1-indexed
/// and reflect the position of `start`; they are redundant with the byte
/// offsets but cached here so diagnostics do not need to re-scan the
/// source to render a caret.
///
/// `EMPTY` (all zeros) is the conventional placeholder for synthetic
/// nodes that do not correspond to any source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Span {
    /// Byte offset of the first character (inclusive).
    pub start: usize,
    /// Byte offset one past the last character (exclusive).
    pub end: usize,
    /// 1-indexed line number of `start`. Zero for synthetic nodes.
    pub line: u32,
    /// 1-indexed column number of `start`. Zero for synthetic nodes.
    pub column: u32,
}

impl Span {
    /// A placeholder span for synthetic nodes — all fields zero.
    pub const EMPTY: Self = Self {
        start: 0,
        end: 0,
        line: 0,
        column: 0,
    };

    /// Construct a span from explicit byte offsets and (line, column).
    #[inline]
    pub fn new(start: usize, end: usize, line: u32, column: u32) -> Self {
        Self {
            start,
            end,
            line,
            column,
        }
    }

    /// The length in bytes covered by this span. Zero for synthetic/empty spans.
    #[inline]
    pub fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// Whether the span covers zero bytes (synthetic or placeholder).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.end <= self.start
    }

    /// Merge two spans into the smallest span covering both.
    ///
    /// Line and column are inherited from whichever span starts earlier.
    /// If either operand is [`Span::EMPTY`], the other span is returned.
    pub fn merged(&self, other: Span) -> Span {
        if *self == Span::EMPTY {
            return other;
        }
        if other == Span::EMPTY {
            return *self;
        }
        let (earlier, later) = if self.start <= other.start {
            (*self, other)
        } else {
            (other, *self)
        };
        Span {
            start: earlier.start,
            end: later.end.max(earlier.end),
            line: earlier.line,
            column: earlier.column,
        }
    }
}

impl Default for Span {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// A (possibly backtick-quoted) identifier from the source text.
///
/// Stores the canonical string with backticks stripped. Identifier
/// comparison is case-sensitive (query-language.md §2.2) — `User_ID`
/// and `user_id` are distinct names. Case-insensitive matching is
/// reserved for keywords, which are not stored as `Name` values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Name {
    /// The canonical identifier text (backticks stripped if quoted).
    pub text: String,
    /// Source span of the original token (including backticks, if any).
    pub span: Span,
}

impl Name {
    /// Construct a `Name` with an explicit span.
    pub fn new(text: impl Into<String>, span: Span) -> Self {
        Self {
            text: text.into(),
            span,
        }
    }

    /// Construct a `Name` with an [`Span::EMPTY`] placeholder span.
    /// Useful in tests and synthetic nodes.
    pub fn synthetic(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            span: Span::EMPTY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_span_is_default() {
        assert_eq!(Span::default(), Span::EMPTY);
        assert!(Span::EMPTY.is_empty());
        assert_eq!(Span::EMPTY.len(), 0);
    }

    #[test]
    fn span_len_and_empty() {
        let s = Span::new(10, 20, 1, 11);
        assert_eq!(s.len(), 10);
        assert!(!s.is_empty());
    }

    #[test]
    fn merged_two_real_spans() {
        let a = Span::new(5, 10, 1, 6);
        let b = Span::new(12, 15, 1, 13);
        let m = a.merged(b);
        assert_eq!(m.start, 5);
        assert_eq!(m.end, 15);
        assert_eq!(m.line, 1);
        assert_eq!(m.column, 6);
    }

    #[test]
    fn merged_respects_earliest_start_regardless_of_order() {
        let a = Span::new(5, 10, 1, 6);
        let b = Span::new(12, 15, 1, 13);
        assert_eq!(a.merged(b), b.merged(a));
    }

    #[test]
    fn merged_with_empty_returns_other() {
        let a = Span::new(5, 10, 1, 6);
        assert_eq!(a.merged(Span::EMPTY), a);
        assert_eq!(Span::EMPTY.merged(a), a);
    }

    #[test]
    fn merged_nested_span_extends_end() {
        // a fully contains b — merged should still use a's end.
        let a = Span::new(5, 20, 1, 6);
        let b = Span::new(10, 15, 1, 11);
        let m = a.merged(b);
        assert_eq!(m.start, 5);
        assert_eq!(m.end, 20);
    }

    #[test]
    fn name_synthetic_has_empty_span() {
        let n = Name::synthetic("user_id");
        assert_eq!(n.text, "user_id");
        assert_eq!(n.span, Span::EMPTY);
    }

    #[test]
    fn name_case_sensitive_equality() {
        assert_ne!(Name::synthetic("User_ID"), Name::synthetic("user_id"));
    }
}
