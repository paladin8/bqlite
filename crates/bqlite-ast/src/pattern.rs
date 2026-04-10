//! MATCH pattern AST — sequence, alternation, negation, repetition, bindings.
//!
//! Pattern types mirror the BQL `MATCH` surface from
//! docs/design/query-language.md §4 and docs/design/sequence-matching.md.
//! The AST is the same whether the pattern was written as a top-level
//! `MATCH`, a `FUNNEL` sugar form (§6), or a `RETENTION` sugar form
//! (§6.3) — the planner desugars sugar into the same node shapes.
//!
//! Variable bindings (`$product`) appear inside step predicates as
//! [`crate::expr::Expr::Variable`]; the pattern module does not track
//! binding scopes — the planner validates scoping at plan time.

use serde::{Deserialize, Serialize};

use crate::expr::{Expr, Spanned};
use crate::span::{Name, Span};

/// A compiled `MATCH` pattern.
///
/// A pattern is a linear sequence of steps plus an optional global
/// time window and optional retention brackets. Match mode controls
/// whether the operator returns the first match per entity, every
/// non-overlapping match, or every match (including overlapping ones)
/// — see query-language.md §4.14.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatchPattern {
    /// Ordered sequence of steps. Empty patterns are rejected by the
    /// planner but the AST does not enforce non-emptiness.
    pub steps: Vec<Step>,
    /// Match mode: `FIRST`, `ALL`, or `EMIT ALL`.
    pub mode: MatchMode,
    /// Optional global time window: `WITHIN 7d` / `WITHIN SESSION`.
    pub window: Option<MatchWindow>,
    /// Optional retention brackets: `BRACKETS [1d, 7d, 30d]`.
    pub brackets: Option<BracketSpec>,
    pub span: Span,
}

/// Match mode (query-language.md §4.14).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MatchMode {
    /// `MATCH FIRST` — return at most one match per entity.
    First,
    /// `MATCH ALL` — return every non-overlapping match.
    All,
    /// `EMIT ALL` — return every possible match including overlaps.
    /// Used internally when lowering `FUNNEL` to `MATCH`.
    EmitAll,
}

/// A single step in a pattern.
///
/// A step matches an event (or one of several alternatives), optionally
/// filtered by a per-step predicate, optionally bound to a step name
/// for downstream reference, and optionally annotated with a repetition
/// quantifier or transition modifier to the next step.
///
/// The grammar at §26 line 1518–1519 has two alternatives:
///
/// ```text
/// step := unqualified_step repetition?
///       | "(" step ")" repetition?     -- parenthesized group (required for WHERE + repetition)
/// ```
///
/// The parenthesized alternative exists **solely** to disambiguate
/// `WHERE predicate + repetition` from expression-level `+` inside
/// the predicate (§4.9 line 249: "The grammar resolves this by
/// requiring parentheses around any step that combines a WHERE clause
/// with a repetition suffix."). The parser strips the parens and
/// produces a single flat `Step` with both a predicate and a
/// repetition — this AST intentionally does not model the
/// parenthesized form as a distinct variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Step {
    /// Optional user-provided step name: `s: signup`. Used in
    /// downstream `STATS` to reference per-step properties. Auto-named
    /// (e.g. `step_0`, `step_1`) by the planner when `None` —
    /// query-language.md §4.4.
    pub name: Option<Name>,
    /// The event(s) this step matches — single type or alternation.
    pub event: StepEvent,
    /// Optional per-step predicate: `signup WHERE country = 'US'`.
    pub predicate: Option<Spanned<Expr>>,
    /// Optional repetition quantifier: `*` or `+`.
    pub repetition: Option<Repetition>,
    /// `IMMEDIATELY` modifier: true when the next step must follow this
    /// one with no intervening events.
    pub immediately_next: bool,
    /// `WITHOUT` exclusion between this step and the next — set of
    /// event types that must not appear in the gap.
    pub without_next: Option<Exclusion>,
    pub span: Span,
}

/// The event (or set of alternatives) a [`Step`] matches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StepEvent {
    /// A single event type, optionally qualified by source table for
    /// cross-table MATCH.
    Single(EventRef),
    /// Alternation: `(signup OR login)`. Order is significant for
    /// source fidelity but the planner treats the set as unordered.
    Alternation(Vec<EventRef>),
}

/// A reference to an event type, optionally qualified by source table.
///
/// The table qualifier is used for cross-table MATCH across joined
/// sources (query-language.md §17). For single-table patterns the
/// parser emits `table = None`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventRef {
    pub table: Option<Name>,
    pub event: Name,
    pub span: Span,
}

/// A `WITHOUT (a, b, …)` exclusion clause attached to a step transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Exclusion {
    pub events: Vec<EventRef>,
    pub span: Span,
}

/// Step repetition quantifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Repetition {
    /// `*` — zero or more.
    ZeroOrMore,
    /// `+` — one or more.
    OneOrMore,
}

/// A pattern-global time window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MatchWindow {
    /// `WITHIN <duration>` — duration in nanoseconds.
    Within(i64),
    /// `WITHIN SESSION` — window is bounded by the enclosing session.
    WithinSession,
}

/// `BRACKETS [d1, d2, …]` — retention time slicing per
/// query-language.md §4.13.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BracketSpec {
    /// Bracket durations in nanoseconds. Stored in user-declared order.
    pub durations: Vec<i64>,
    /// `CUMULATIVE` modifier: each bracket includes all earlier brackets.
    pub cumulative: bool,
    pub span: Span,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Literal;

    fn evt(name: &str) -> EventRef {
        EventRef {
            table: None,
            event: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    fn step(name: &str) -> Step {
        Step {
            name: None,
            event: StepEvent::Single(evt(name)),
            predicate: None,
            repetition: None,
            immediately_next: false,
            without_next: None,
            span: Span::EMPTY,
        }
    }

    #[test]
    fn linear_three_step_pattern() {
        let p = MatchPattern {
            steps: vec![step("view"), step("cart"), step("purchase")],
            mode: MatchMode::First,
            window: Some(MatchWindow::Within(86_400_000_000_000)),
            brackets: None,
            span: Span::EMPTY,
        };
        assert_eq!(p.steps.len(), 3);
        assert_eq!(p.mode, MatchMode::First);
    }

    #[test]
    fn step_with_alternation_event() {
        let s = Step {
            event: StepEvent::Alternation(vec![evt("a"), evt("b")]),
            ..step("ignored")
        };
        match s.event {
            StepEvent::Alternation(evs) => assert_eq!(evs.len(), 2),
            _ => panic!("expected alternation"),
        }
    }

    #[test]
    fn step_with_predicate_and_name() {
        let s = Step {
            name: Some(Name::synthetic("signup")),
            predicate: Some(Spanned::new(
                Expr::Literal(Literal::Bool(true)),
                Span::EMPTY,
            )),
            ..step("signup_event")
        };
        assert_eq!(s.name.as_ref().unwrap().text, "signup");
        assert!(s.predicate.is_some());
    }

    #[test]
    fn step_with_immediately_and_without() {
        let s = Step {
            immediately_next: true,
            without_next: Some(Exclusion {
                events: vec![evt("noise")],
                span: Span::EMPTY,
            }),
            ..step("a")
        };
        assert!(s.immediately_next);
        assert_eq!(s.without_next.as_ref().unwrap().events.len(), 1);
    }

    #[test]
    fn pattern_with_cumulative_brackets() {
        let p = MatchPattern {
            steps: vec![step("signup")],
            mode: MatchMode::EmitAll,
            window: None,
            brackets: Some(BracketSpec {
                durations: vec![86_400_000_000_000, 7 * 86_400_000_000_000],
                cumulative: true,
                span: Span::EMPTY,
            }),
            span: Span::EMPTY,
        };
        let b = p.brackets.as_ref().unwrap();
        assert!(b.cumulative);
        assert_eq!(b.durations.len(), 2);
    }

    #[test]
    fn event_ref_qualified_and_unqualified_distinct() {
        let a = EventRef {
            table: None,
            event: Name::synthetic("click"),
            span: Span::EMPTY,
        };
        let b = EventRef {
            table: Some(Name::synthetic("web")),
            event: Name::synthetic("click"),
            span: Span::EMPTY,
        };
        assert_ne!(a, b);
    }
}
