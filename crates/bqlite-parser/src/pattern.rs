//! Pattern sub-grammar productions.
//!
//! Implements the recursive-descent parser for BQL `SEQUENCE` patterns,
//! individual steps, step separators, event references, exclusions,
//! repetitions, and match modifiers.
//!
//! Design: `docs/design/language/pattern-grammar.md`
//!
//! ## Public surface (crate-internal)
//!
//! - [`parse_sequence`] — `SEQUENCE "(" step_list ")"` (used by MATCH parser, TASK-313)
//! - [`parse_step_list`] — inner step list without surrounding parens (used by FUNNEL, TASK-316)
//! - [`parse_match_modifiers`] — optional `WITHIN`/`BRACKETS`/`EMIT ALL` modifiers (TASK-313)
//! - [`parse_event_ref`] — `(name ".")? name` event reference (TASK-316, RETENTION)

// All pub(crate) functions in this module are consumed by the MATCH, FUNNEL,
// and RETENTION pipeline stage parsers (TASK-313, TASK-316) which have not
// landed yet. Suppress dead_code until they do.
#![allow(dead_code)]

use bqlite_ast::pattern::{
    BracketSpec, EventRef, Exclusion, MatchMode, MatchWindow, Repetition, Step, StepEvent,
};
use bqlite_ast::{Name, Span};

use crate::error::{Expected, NameRole, ParseError};
use crate::lex::{token_span, Keyword, TokenKind};
use crate::parser::Parser;

// ──────────────────────────────────────────────────────────────────────────────
// Public crate-internal API
// ──────────────────────────────────────────────────────────────────────────────

/// Parse `SEQUENCE "(" step_list ")"`.
///
/// Expects the cursor to be positioned on the `SEQUENCE` keyword.
/// Returns the ordered step list and the span from `SEQUENCE` through
/// the closing `)`.
///
/// Error sites (pattern-grammar.md §3.1):
/// - Missing `(` after `SEQUENCE` → `Expected::Punct("(")`
/// - Empty step list `SEQUENCE()` → `Expected::Keyword("step")`
/// - Missing `)` after step list → `Expected::Punct(")")`
pub(crate) fn parse_sequence(p: &mut Parser) -> Result<(Vec<Step>, Span), ParseError> {
    let seq_tok = p.expect_kw(Keyword::Sequence)?;
    let seq_span = token_span(&seq_tok);

    p.expect_punct(&TokenKind::LParen, "(")?;

    // Empty step list is a parse error — at least one step is required.
    if matches!(p.peek_kind(), TokenKind::RParen) {
        return Err(p.error_unexpected(
            Expected::Keyword("step"),
            Some("SEQUENCE requires at least one step"),
        ));
    }

    let steps = parse_step_list(p)?;

    let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
    let rparen_span = token_span(&rparen_tok);

    Ok((steps, seq_span.merged(rparen_span)))
}

/// Parse the step list inner content `step (step_sep step)*`.
///
/// The surrounding parens (`(` and `)`) are handled by the caller.
/// At least one step must already be confirmed non-empty by the caller
/// before invoking this function, unless the caller wishes to accept
/// an empty list (FUNNEL callers may prefer to error themselves).
///
/// Step separator fields (`without_next`, `immediately_next`) are
/// applied to the **preceding** step per pattern-grammar.md §3.3.
pub(crate) fn parse_step_list(p: &mut Parser) -> Result<Vec<Step>, ParseError> {
    let mut steps = Vec::new();

    // Parse the first step unconditionally.
    steps.push(parse_step(p)?);

    // Each iteration: check for a step separator, parse it, parse the
    // next step, then apply separator metadata to steps[len-2].
    loop {
        if !is_step_sep_start(p) {
            break;
        }

        // Optional WITHOUT clause — must come before THEN / ->.
        let without = if p.try_kw(Keyword::Without).is_some() {
            Some(parse_exclusion(p)?)
        } else {
            None
        };

        // Mandatory THEN or -> separator token.
        if p.try_kw(Keyword::Then).is_none() && p.try_kind(&TokenKind::Arrow).is_none() {
            return Err(p.error_unexpected(
                Expected::Keyword("THEN"),
                Some("step separator THEN or -> expected between steps"),
            ));
        }

        // Optional IMMEDIATELY after THEN/->. Per pattern-grammar.md §3.4,
        // IMMEDIATELY is stored on the *preceding* step (not the one
        // being parsed next), describing the gap *after* that step.
        let immediately = p.try_kw(Keyword::Immediately).is_some();

        // Parse the next step.
        steps.push(parse_step(p)?);

        // Apply separator fields to the step *before* the one just pushed.
        let prev = steps.len() - 2;
        steps[prev].without_next = without;
        steps[prev].immediately_next = immediately;
    }

    Ok(steps)
}

/// Parse `(WITHIN (duration | SESSION))? (BRACKETS CUMULATIVE? "[" duration_list "]")? (EMIT ALL)?`
/// in canonical order.
///
/// `base_mode` is the already-parsed `MatchMode::First` or `MatchMode::All`.
/// Returns `(window, brackets, final_mode)`.
///
/// Errors if modifiers appear out of canonical order or if the mode /
/// modifier combination is unsupported (e.g. `MATCH ALL EMIT ALL`).
pub(crate) fn parse_match_modifiers(
    p: &mut Parser,
    base_mode: MatchMode,
) -> Result<(Option<MatchWindow>, Option<BracketSpec>, MatchMode), ParseError> {
    // 1. Optional WITHIN clause.
    let window = parse_within(p)?;

    // 2. Optional BRACKETS clause.
    // Check for out-of-order: if BRACKETS appears here, WITHIN must have
    // already been consumed (or absent). No ordering problem at this point.
    let brackets = parse_brackets(p)?;

    // Guard: WITHIN after BRACKETS is an out-of-order modifier.
    // Pattern-grammar.md §3.11 requires WITHIN before BRACKETS.
    // Example: `BRACKETS [1d] WITHIN SESSION` → error.
    if brackets.is_some() && matches!(p.peek_kind(), TokenKind::Kw(Keyword::Within)) {
        return Err(p.error_unexpected(
            Expected::EndOfModifiers,
            Some("WITHIN must appear before BRACKETS"),
        ));
    }

    // 3. Optional EMIT ALL.
    let final_mode = parse_emit_all(p, base_mode)?;

    Ok((window, brackets, final_mode))
}

/// Parse `(name ".")? name` into an [`EventRef`].
///
/// Uses `NameRole::ColumnName` for error messages (there is no dedicated
/// `EventName` role). Backtick-quoted event names are accepted here;
/// the planner rejects reserved-keyword event names at name resolution.
///
/// Error site: any non-name token → `Expected::Name` (from `expect_name`).
pub(crate) fn parse_event_ref(p: &mut Parser) -> Result<EventRef, ParseError> {
    let first_name = p.expect_name(NameRole::ColumnName)?;
    let first_span = first_name.span;

    // Dot-qualified form: `table.event_name`.
    if matches!(p.peek_kind(), TokenKind::Dot) {
        p.bump(); // consume "."
        let event_name = p.expect_name(NameRole::ColumnName)?;
        let event_span = event_name.span;
        return Ok(EventRef {
            table: Some(first_name),
            event: event_name,
            span: first_span.merged(event_span),
        });
    }

    Ok(EventRef {
        table: None,
        event: first_name,
        span: first_span,
    })
}

// ──────────────────────────────────────────────────────────────────────────────
// Private helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Returns `true` if the current token could start a step separator.
/// Pattern-grammar.md §3.4: separators start with `WITHOUT`, `THEN`, or `->`.
fn is_step_sep_start(p: &Parser) -> bool {
    matches!(
        p.peek_kind(),
        TokenKind::Kw(Keyword::Without) | TokenKind::Kw(Keyword::Then) | TokenKind::Arrow
    )
}

/// Returns `true` if a `LParen` at the current cursor position opens an
/// alternation expression `"(" event_ref (OR event_ref)+ ")"` rather
/// than a parenthesized step group.
///
/// Implements the lookahead rule from pattern-grammar.md §3.5 / §3.7:
///
/// - `( ident OR …)` → alternation (unqualified event ref followed by OR).
/// - `( ident . ident OR …)` → alternation (qualified event ref).
/// - Anything else → parenthesized step group.
///
/// This is the *exact* lookahead from the design doc, not a general
/// "scan for OR" — so `(name: event OR other)` is correctly identified
/// as a parenthesized step group (the `Colon` at peek_at(2) is not `Or`).
fn is_alternation_lparen(p: &Parser) -> bool {
    debug_assert!(
        matches!(p.peek_kind(), TokenKind::LParen),
        "caller must verify LParen at current position"
    );

    // Unqualified alternation: ( (ident | backtick-quoted) OR ...
    if matches!(
        p.peek_at(1).kind,
        TokenKind::Ident(_) | TokenKind::QuotedName(_)
    ) && matches!(p.peek_at(2).kind, TokenKind::Kw(Keyword::Or))
    {
        return true;
    }

    // Qualified alternation: ( (ident | backtick-quoted) . (ident | backtick-quoted) OR ...
    if matches!(
        p.peek_at(1).kind,
        TokenKind::Ident(_) | TokenKind::QuotedName(_)
    ) && matches!(p.peek_at(2).kind, TokenKind::Dot)
        && matches!(
            p.peek_at(3).kind,
            TokenKind::Ident(_) | TokenKind::QuotedName(_)
        )
        && matches!(p.peek_at(4).kind, TokenKind::Kw(Keyword::Or))
    {
        return true;
    }

    false
}

/// Parse one step.
///
/// ```text
/// step := unqualified_step repetition?
///       | "(" step ")" repetition?   -- parenthesized group for WHERE + repetition
/// ```
///
/// See pattern-grammar.md §3.5 for the disambiguation between
/// alternation parens and parenthesized step groups.
fn parse_step(p: &mut Parser) -> Result<Step, ParseError> {
    if matches!(p.peek_kind(), TokenKind::LParen) && !is_alternation_lparen(p) {
        // Parenthesized step group: consume `(`, parse the inner step,
        // then expect `)`, then optionally a repetition suffix.
        let lparen_tok = p.bump();
        let lparen_span = token_span(&lparen_tok);

        let mut inner = parse_unqualified_step(p)?;

        let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
        let rparen_span = token_span(&rparen_tok);

        // Repetition applies to the parenthesized group.
        let (rep, rep_span) = parse_repetition_with_span(p);
        inner.repetition = rep;
        inner.span = lparen_span.merged(rparen_span).merged(rep_span);
        Ok(inner)
    } else {
        // Bare event ref or alternation (alternation `(` stays unconsumed
        // so parse_step_event inside parse_unqualified_step sees it).
        let mut s = parse_unqualified_step(p)?;
        let (rep, rep_span) = parse_repetition_with_span(p);
        s.repetition = rep;
        if s.repetition.is_some() {
            s.span = s.span.merged(rep_span);
        }
        Ok(s)
    }
}

/// Parse `(identifier ":")? step_event (WHERE predicate)?`.
///
/// Pattern-grammar.md §3.6. Step name disambiguation uses 2-token
/// lookahead: `Ident Colon` → step name; anything else → no name.
fn parse_unqualified_step(p: &mut Parser) -> Result<Step, ParseError> {
    let step_start = token_span(p.peek());

    // Optional step name: `identifier ":"` — 2-token lookahead.
    let name = if matches!(p.peek_kind(), TokenKind::Ident(_))
        && matches!(p.peek_at(1).kind, TokenKind::Colon)
    {
        let ident_tok = p.bump(); // consume the identifier
        p.bump(); // consume ":"
        let name_span = token_span(&ident_tok);
        let text = match ident_tok.kind {
            TokenKind::Ident(s) => s,
            _ => unreachable!("matched Ident above"),
        };
        Some(Name::new(text, name_span))
    } else if matches!(p.peek_kind(), TokenKind::Kw(_))
        && matches!(p.peek_at(1).kind, TokenKind::Colon)
    {
        // Reserved keyword followed by `:` — the user is trying to use
        // a keyword as a step name, which is a parse error per §26.3.
        let kw_tok = p.peek();
        let kw = match &kw_tok.kind {
            TokenKind::Kw(kw) => kw.canonical(),
            _ => unreachable!("matched Kw above"),
        };
        return Err(ParseError::ReservedKeyword {
            offset: kw_tok.start,
            keyword: kw,
            role: NameRole::StepName,
        });
    } else {
        None
    };

    // Event reference — single or alternation.
    let (event, event_span) = parse_step_event(p)?;

    // Optional WHERE predicate.
    let predicate = if p.try_kw(Keyword::Where).is_some() {
        Some(crate::expr::parse_expression(p)?)
    } else {
        None
    };

    // Span: from step start (name or event) through end of predicate or event.
    let end_span = predicate.as_ref().map(|pr| pr.span).unwrap_or(event_span);
    let span = step_start.merged(end_span);

    Ok(Step {
        name,
        event,
        predicate,
        repetition: None,
        immediately_next: false,
        without_next: None,
        span,
    })
}

/// Parse `event_ref | "(" event_ref (OR event_ref)+ ")"`.
///
/// Returns the `StepEvent` and its source span.
///
/// Pattern-grammar.md §3.7. At this call site the leading `(` (if
/// present) always denotes alternation — parenthesized step group form
/// consumes the `(` before calling `parse_unqualified_step`.
fn parse_step_event(p: &mut Parser) -> Result<(StepEvent, Span), ParseError> {
    if matches!(p.peek_kind(), TokenKind::LParen) {
        // Alternation form: `"(" event_ref (OR event_ref)+ ")"`.
        let lparen_tok = p.bump();
        let start_span = token_span(&lparen_tok);

        let first = parse_event_ref(p)?;
        let mut refs = vec![first];

        // At least one OR is required (single-event alternation is rejected).
        if !matches!(p.peek_kind(), TokenKind::Kw(Keyword::Or)) {
            return Err(p.error_unexpected(
                Expected::Keyword("OR"),
                Some("alternation requires at least two event types: (a OR b)"),
            ));
        }

        while p.try_kw(Keyword::Or).is_some() {
            refs.push(parse_event_ref(p)?);
        }

        let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
        let end_span = token_span(&rparen_tok);

        Ok((StepEvent::Alternation(refs), start_span.merged(end_span)))
    } else {
        let event_ref = parse_event_ref(p)?;
        let span = event_ref.span;
        Ok((StepEvent::Single(event_ref), span))
    }
}

/// Parse `event_ref | "(" event_ref (OR event_ref)+ ")"` into an [`Exclusion`].
///
/// Pattern-grammar.md §3.9. Unlike `parse_step_event`, bare single-event
/// exclusions (`WITHOUT cancel`) are accepted. A parenthesized single
/// event (`WITHOUT (cancel)`) is also accepted per §3.9 error table.
fn parse_exclusion(p: &mut Parser) -> Result<Exclusion, ParseError> {
    if matches!(p.peek_kind(), TokenKind::LParen) {
        let lparen_tok = p.bump();
        let start_span = token_span(&lparen_tok);

        // Empty `WITHOUT ()` is an error.
        if matches!(p.peek_kind(), TokenKind::RParen) {
            return Err(p.error_unexpected(
                Expected::EventRef,
                Some("WITHOUT requires at least one event type"),
            ));
        }

        let first = parse_event_ref(p)?;
        let mut events = vec![first];

        // Zero or more additional OR-separated event refs.
        while p.try_kw(Keyword::Or).is_some() {
            events.push(parse_event_ref(p)?);
        }

        let rparen_tok = p.expect_punct(&TokenKind::RParen, ")")?;
        let end_span = token_span(&rparen_tok);

        Ok(Exclusion {
            events,
            span: start_span.merged(end_span),
        })
    } else {
        let event_ref = parse_event_ref(p)?;
        let span = event_ref.span;
        Ok(Exclusion {
            events: vec![event_ref],
            span,
        })
    }
}

/// Try to consume `*` or `+`, returning both the [`Repetition`] value
/// and the token span. Returns `(None, Span::EMPTY)` if neither is present.
fn parse_repetition_with_span(p: &mut Parser) -> (Option<Repetition>, Span) {
    if let Some(tok) = p.try_kind(&TokenKind::Star) {
        (Some(Repetition::ZeroOrMore), token_span(&tok))
    } else if let Some(tok) = p.try_kind(&TokenKind::Plus) {
        (Some(Repetition::OneOrMore), token_span(&tok))
    } else {
        (None, Span::EMPTY)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Match modifier sub-parsers
// ──────────────────────────────────────────────────────────────────────────────

/// Parse the optional `WITHIN (duration | SESSION)` clause.
fn parse_within(p: &mut Parser) -> Result<Option<MatchWindow>, ParseError> {
    if p.try_kw(Keyword::Within).is_none() {
        return Ok(None);
    }

    // WITHIN SESSION
    if p.try_kw(Keyword::Session).is_some() {
        return Ok(Some(MatchWindow::WithinSession));
    }

    // WITHIN <duration>
    let tok = p.peek();
    if let TokenKind::Duration(ns) = tok.kind {
        p.bump();
        Ok(Some(MatchWindow::Within(ns)))
    } else {
        Err(p.error_unexpected(
            Expected::Literal,
            Some("expected a duration (e.g. 7d) or SESSION after WITHIN"),
        ))
    }
}

/// Parse the optional `BRACKETS CUMULATIVE? "[" duration_list "]"` clause.
fn parse_brackets(p: &mut Parser) -> Result<Option<BracketSpec>, ParseError> {
    let brackets_tok = match p.try_kw(Keyword::Brackets) {
        Some(tok) => tok,
        None => return Ok(None),
    };
    let brackets_span = token_span(&brackets_tok);

    let cumulative = p.try_kw(Keyword::Cumulative).is_some();

    p.expect_punct(&TokenKind::LBracket, "[")?;

    // Empty bracket list is an error.
    if matches!(p.peek_kind(), TokenKind::RBracket) {
        return Err(p.error_unexpected(
            Expected::Literal,
            Some("BRACKETS requires at least one duration"),
        ));
    }

    // Parse comma-separated duration list.
    let mut durations = Vec::new();
    loop {
        let tok = p.peek();
        if let TokenKind::Duration(ns) = tok.kind {
            p.bump();
            durations.push(ns);
        } else {
            return Err(p.error_unexpected(
                Expected::Literal,
                Some("expected a duration (e.g. 7d) in BRACKETS list"),
            ));
        }
        if p.try_kind(&TokenKind::Comma).is_none() {
            break;
        }
    }

    let rbracket_tok = p.expect_punct(&TokenKind::RBracket, "]")?;
    let rbracket_span = token_span(&rbracket_tok);

    Ok(Some(BracketSpec {
        durations,
        cumulative,
        span: brackets_span.merged(rbracket_span),
    }))
}

/// Parse the optional `EMIT ALL` modifier and derive the final `MatchMode`.
///
/// `base_mode` is the `MatchMode::First` or `MatchMode::All` that was
/// parsed before the step list.
///
/// Errors:
/// - `EMIT` not followed by `ALL` → `Expected::Keyword("ALL")`
/// - `MATCH ALL ... EMIT ALL` → `Expected::EndOfModifiers` (unsupported)
/// - `EMIT ALL` followed by `WITHIN` or `BRACKETS` → out-of-order error
fn parse_emit_all(p: &mut Parser, base_mode: MatchMode) -> Result<MatchMode, ParseError> {
    let emit_tok = match p.try_kw(Keyword::Emit) {
        Some(tok) => tok,
        None => return Ok(base_mode),
    };
    let emit_span = token_span(&emit_tok);

    // `EMIT` must be followed by `ALL`.
    p.expect_kw(Keyword::All)?;

    // Detect out-of-order modifiers after EMIT ALL.
    match p.peek_kind() {
        TokenKind::Kw(Keyword::Within) => {
            return Err(p.error_unexpected(
                Expected::EndOfModifiers,
                Some("WITHIN must appear before EMIT ALL"),
            ));
        }
        TokenKind::Kw(Keyword::Brackets) => {
            return Err(p.error_unexpected(
                Expected::EndOfModifiers,
                Some("BRACKETS must appear before EMIT ALL"),
            ));
        }
        _ => {}
    }

    // `MATCH ALL EMIT ALL` is not representable in the current AST.
    // See pattern-grammar.md §7.1 for the encoding decision.
    match base_mode {
        MatchMode::First | MatchMode::EmitAll => Ok(MatchMode::EmitAll),
        MatchMode::All => Err(ParseError::Unexpected {
            offset: emit_span.start,
            line: emit_span.line,
            column: emit_span.column,
            expected: Expected::EndOfModifiers,
            found: "EMIT".to_string(),
            detail: Some(
                "MATCH ALL EMIT ALL is not supported in this version; \
                 use MATCH FIRST EMIT ALL or MATCH ALL without EMIT ALL",
            ),
        }),
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_ast::pattern::MatchMode;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_parser(src: &'static str) -> crate::parser::Parser<'static> {
        // All callers pass string literals (`'static`), so no allocation needed.
        crate::parser::Parser::new(src).expect("lex failed")
    }

    // -----------------------------------------------------------------------
    // event_ref
    // -----------------------------------------------------------------------

    #[test]
    fn event_ref_unqualified() {
        let mut p = make_parser("signup");
        let er = parse_event_ref(&mut p).unwrap();
        assert!(er.table.is_none());
        assert_eq!(er.event.text, "signup");
        assert_eq!(er.span.start, 0);
        assert_eq!(er.span.end, 6);
    }

    #[test]
    fn event_ref_qualified() {
        let mut p = make_parser("events.signup");
        let er = parse_event_ref(&mut p).unwrap();
        assert_eq!(er.table.as_ref().unwrap().text, "events");
        assert_eq!(er.event.text, "signup");
        assert_eq!(er.span.start, 0);
        assert_eq!(er.span.end, "events.signup".len());
    }

    #[test]
    fn event_ref_rejects_reserved_keyword() {
        let mut p = make_parser("MATCH");
        match parse_event_ref(&mut p) {
            Err(ParseError::ReservedKeyword { keyword, .. }) => {
                assert_eq!(keyword, "MATCH");
            }
            other => panic!("expected ReservedKeyword, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // exclusion
    // -----------------------------------------------------------------------

    #[test]
    fn exclusion_bare_single_event() {
        let mut p = make_parser("refund");
        let ex = parse_exclusion(&mut p).unwrap();
        assert_eq!(ex.events.len(), 1);
        assert_eq!(ex.events[0].event.text, "refund");
    }

    #[test]
    fn exclusion_parenthesized_single_event() {
        // `WITHOUT (refund)` — single element in parens is accepted per §3.9.
        let mut p = make_parser("(refund)");
        let ex = parse_exclusion(&mut p).unwrap();
        assert_eq!(ex.events.len(), 1);
    }

    #[test]
    fn exclusion_multiple_events() {
        let mut p = make_parser("(refund OR churn)");
        let ex = parse_exclusion(&mut p).unwrap();
        assert_eq!(ex.events.len(), 2);
        assert_eq!(ex.events[0].event.text, "refund");
        assert_eq!(ex.events[1].event.text, "churn");
    }

    #[test]
    fn exclusion_empty_parens_is_error() {
        let mut p = make_parser("()");
        match parse_exclusion(&mut p) {
            Err(ParseError::UnexpectedEof {
                expected, detail, ..
            })
            | Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EventRef);
                assert!(detail.unwrap_or("").contains("WITHOUT requires"));
            }
            other => panic!("expected EventRef error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // step_event
    // -----------------------------------------------------------------------

    #[test]
    fn step_event_bare_single() {
        let mut p = make_parser("signup");
        let (ev, _span) = parse_step_event(&mut p).unwrap();
        assert!(matches!(ev, StepEvent::Single(_)));
    }

    #[test]
    fn step_event_alternation() {
        let mut p = make_parser("(signup OR login)");
        let (ev, _span) = parse_step_event(&mut p).unwrap();
        match ev {
            StepEvent::Alternation(refs) => {
                assert_eq!(refs.len(), 2);
                assert_eq!(refs[0].event.text, "signup");
                assert_eq!(refs[1].event.text, "login");
            }
            other => panic!("expected Alternation, got {other:?}"),
        }
    }

    #[test]
    fn step_event_alternation_three_events() {
        let mut p = make_parser("(a OR b OR c)");
        let (ev, _span) = parse_step_event(&mut p).unwrap();
        match ev {
            StepEvent::Alternation(refs) => assert_eq!(refs.len(), 3),
            other => panic!("expected Alternation, got {other:?}"),
        }
    }

    #[test]
    fn step_event_single_paren_no_or_errors() {
        // `(signup)` inside alternation position — rejected since there is
        // no OR after the first event ref.
        let mut p = make_parser("(signup)");
        match parse_step_event(&mut p) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            })
            | Err(ParseError::UnexpectedEof {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("OR"));
                assert!(detail.unwrap_or("").contains("alternation requires"));
            }
            other => panic!("expected OR error, got {other:?}"),
        }
    }

    #[test]
    fn step_event_alternation_with_backtick_quoted_names() {
        // Backtick-quoted names in alternation must be correctly identified
        // as alternation (not parenthesized step group) by is_alternation_lparen.
        let mut p = make_parser("(`signup` OR login)");
        let (ev, _span) = parse_step_event(&mut p).unwrap();
        match ev {
            StepEvent::Alternation(refs) => {
                assert_eq!(refs.len(), 2);
                assert_eq!(refs[0].event.text, "signup");
                assert_eq!(refs[1].event.text, "login");
            }
            other => panic!("expected Alternation, got {other:?}"),
        }
    }

    #[test]
    fn step_with_name_colon_not_misidentified_as_alternation() {
        // `(name: event)` — the `Colon` at peek_at(2) prevents
        // is_alternation_lparen from returning true.
        // The `(` is consumed as a parenthesized step group, not alternation.
        let mut p = make_parser("(s: signup)");
        let s = parse_step(&mut p).unwrap();
        assert_eq!(s.name.as_ref().unwrap().text, "s");
        assert!(matches!(s.event, StepEvent::Single(_)));
    }

    // -----------------------------------------------------------------------
    // unqualified_step
    // -----------------------------------------------------------------------

    #[test]
    fn step_bare_event() {
        let mut p = make_parser("signup");
        let s = parse_unqualified_step(&mut p).unwrap();
        assert!(s.name.is_none());
        assert!(matches!(s.event, StepEvent::Single(_)));
        assert!(s.predicate.is_none());
    }

    #[test]
    fn step_with_name() {
        let mut p = make_parser("s: signup");
        let s = parse_unqualified_step(&mut p).unwrap();
        assert_eq!(s.name.as_ref().unwrap().text, "s");
        match &s.event {
            StepEvent::Single(er) => assert_eq!(er.event.text, "signup"),
            other => panic!("expected Single, got {other:?}"),
        }
    }

    #[test]
    fn step_with_where_predicate() {
        let mut p = make_parser("signup WHERE country = 'US'");
        let s = parse_unqualified_step(&mut p).unwrap();
        assert!(s.predicate.is_some());
    }

    #[test]
    fn step_reserved_keyword_as_name_errors() {
        // `WHERE: signup` — WHERE is a reserved keyword.
        let mut p = make_parser("WHERE: signup");
        match parse_unqualified_step(&mut p) {
            Err(ParseError::ReservedKeyword { keyword, role, .. }) => {
                assert_eq!(keyword, "WHERE");
                assert_eq!(role, NameRole::StepName);
            }
            other => panic!("expected ReservedKeyword, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // parse_step (with repetition and parenthesized form)
    // -----------------------------------------------------------------------

    #[test]
    fn step_with_zero_or_more_repetition() {
        let mut p = make_parser("page_view*");
        let s = parse_step(&mut p).unwrap();
        assert_eq!(s.repetition, Some(Repetition::ZeroOrMore));
    }

    #[test]
    fn step_with_one_or_more_repetition() {
        let mut p = make_parser("page_view+");
        let s = parse_step(&mut p).unwrap();
        assert_eq!(s.repetition, Some(Repetition::OneOrMore));
    }

    #[test]
    fn step_parenthesized_with_repetition() {
        // `(page_view WHERE category = 'shop')+`
        let mut p = make_parser("(page_view WHERE category = 'shop')+");
        let s = parse_step(&mut p).unwrap();
        assert_eq!(s.repetition, Some(Repetition::OneOrMore));
        assert!(s.predicate.is_some());
    }

    #[test]
    fn step_alternation_event() {
        let mut p = make_parser("(signup OR login)");
        let s = parse_step(&mut p).unwrap();
        assert!(matches!(s.event, StepEvent::Alternation(_)));
    }

    // -----------------------------------------------------------------------
    // parse_step_list / step separators
    // -----------------------------------------------------------------------

    #[test]
    fn step_list_single_step() {
        let mut p = make_parser("signup");
        let steps = parse_step_list(&mut p).unwrap();
        assert_eq!(steps.len(), 1);
    }

    #[test]
    fn step_list_three_steps() {
        let mut p = make_parser("signup THEN activation THEN purchase");
        let steps = parse_step_list(&mut p).unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(step_event_name(&steps[0]), "signup");
        assert_eq!(step_event_name(&steps[1]), "activation");
        assert_eq!(step_event_name(&steps[2]), "purchase");
    }

    #[test]
    fn step_list_arrow_separator() {
        let mut p = make_parser("signup -> activation");
        let steps = parse_step_list(&mut p).unwrap();
        assert_eq!(steps.len(), 2);
    }

    #[test]
    fn step_list_immediately_applied_to_preceding_step() {
        let mut p = make_parser("signup THEN IMMEDIATELY purchase");
        let steps = parse_step_list(&mut p).unwrap();
        // IMMEDIATELY on separator between signup and purchase → signup.immediately_next
        assert!(steps[0].immediately_next);
        assert!(!steps[1].immediately_next);
    }

    #[test]
    fn step_list_without_applied_to_preceding_step() {
        let mut p = make_parser("signup WITHOUT refund THEN purchase");
        let steps = parse_step_list(&mut p).unwrap();
        // WITHOUT on separator between signup and purchase → signup.without_next
        assert!(steps[0].without_next.is_some());
        assert_eq!(
            steps[0].without_next.as_ref().unwrap().events[0].event.text,
            "refund"
        );
        assert!(steps[1].without_next.is_none());
    }

    #[test]
    fn step_list_without_multiple_exclusions() {
        let mut p = make_parser("signup WITHOUT (refund OR churn) THEN purchase");
        let steps = parse_step_list(&mut p).unwrap();
        let excl = steps[0].without_next.as_ref().unwrap();
        assert_eq!(excl.events.len(), 2);
    }

    #[test]
    fn step_list_separator_missing_then_errors() {
        // WITHOUT but no THEN/-> after: should error.
        let mut p = make_parser("signup WITHOUT refund purchase");
        match parse_step_list(&mut p) {
            Err(ParseError::Unexpected { expected, .. })
            | Err(ParseError::UnexpectedEof { expected, .. }) => {
                assert_eq!(expected, Expected::Keyword("THEN"));
            }
            other => panic!("expected THEN error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // parse_sequence
    // -----------------------------------------------------------------------

    #[test]
    fn sequence_two_steps() {
        let mut p = make_parser("SEQUENCE(signup THEN purchase)");
        let (steps, span) = parse_sequence(&mut p).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(span.start, 0);
        assert_eq!(span.end, "SEQUENCE(signup THEN purchase)".len());
    }

    #[test]
    fn sequence_empty_is_error() {
        let mut p = make_parser("SEQUENCE()");
        match parse_sequence(&mut p) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            })
            | Err(ParseError::UnexpectedEof {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Keyword("step"));
                assert!(detail.unwrap_or("").contains("at least one step"));
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn sequence_missing_lparen_errors() {
        let mut p = make_parser("SEQUENCE signup THEN purchase)");
        match parse_sequence(&mut p) {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Punct("("));
            }
            other => panic!("expected Punct error, got {other:?}"),
        }
    }

    #[test]
    fn sequence_missing_rparen_errors() {
        let mut p = make_parser("SEQUENCE(signup THEN purchase");
        match parse_sequence(&mut p) {
            Err(ParseError::UnexpectedEof { expected, .. })
            | Err(ParseError::Unexpected { expected, .. }) => {
                assert_eq!(expected, Expected::Punct(")"));
            }
            other => panic!("expected Punct error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // parse_match_modifiers
    // -----------------------------------------------------------------------

    #[test]
    fn modifiers_none() {
        let mut p = make_parser("");
        let (window, brackets, mode) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        assert!(window.is_none());
        assert!(brackets.is_none());
        assert_eq!(mode, MatchMode::First);
    }

    #[test]
    fn modifiers_within_duration() {
        let mut p = make_parser("WITHIN 7d");
        let (window, _, mode) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        assert_eq!(
            window,
            Some(MatchWindow::Within(7 * 24 * 3_600_000_000_000))
        );
        assert_eq!(mode, MatchMode::First);
    }

    #[test]
    fn modifiers_within_session() {
        let mut p = make_parser("WITHIN SESSION");
        let (window, _, _) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        assert_eq!(window, Some(MatchWindow::WithinSession));
    }

    #[test]
    fn modifiers_emit_all_with_first_produces_emit_all_mode() {
        let mut p = make_parser("EMIT ALL");
        let (_, _, mode) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        assert_eq!(mode, MatchMode::EmitAll);
    }

    #[test]
    fn modifiers_all_mode_no_emit_all_stays_all() {
        let mut p = make_parser("");
        let (_, _, mode) = parse_match_modifiers(&mut p, MatchMode::All).unwrap();
        assert_eq!(mode, MatchMode::All);
    }

    #[test]
    fn modifiers_all_with_emit_all_is_error() {
        let mut p = make_parser("EMIT ALL");
        match parse_match_modifiers(&mut p, MatchMode::All) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EndOfModifiers);
                assert!(detail
                    .unwrap_or("")
                    .contains("MATCH ALL EMIT ALL is not supported"));
            }
            other => panic!("expected EndOfModifiers error, got {other:?}"),
        }
    }

    #[test]
    fn modifiers_within_and_emit_all() {
        let mut p = make_parser("WITHIN 7d EMIT ALL");
        let (window, _, mode) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        assert!(window.is_some());
        assert_eq!(mode, MatchMode::EmitAll);
    }

    #[test]
    fn modifiers_emit_all_before_within_is_out_of_order() {
        // `EMIT ALL WITHIN 7d` — WITHIN must appear before EMIT ALL.
        let mut p = make_parser("EMIT ALL WITHIN 7d");
        match parse_match_modifiers(&mut p, MatchMode::First) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EndOfModifiers);
                assert!(detail
                    .unwrap_or("")
                    .contains("WITHIN must appear before EMIT ALL"));
            }
            other => panic!("expected EndOfModifiers error, got {other:?}"),
        }
    }

    #[test]
    fn modifiers_brackets() {
        let mut p = make_parser("BRACKETS [1d, 7d, 30d]");
        let (_, brackets, _) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        let b = brackets.unwrap();
        assert_eq!(b.durations.len(), 3);
        assert!(!b.cumulative);
    }

    #[test]
    fn modifiers_brackets_cumulative() {
        let mut p = make_parser("BRACKETS CUMULATIVE [1d, 7d]");
        let (_, brackets, _) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        let b = brackets.unwrap();
        assert!(b.cumulative);
        assert_eq!(b.durations.len(), 2);
    }

    #[test]
    fn modifiers_within_and_brackets() {
        let mut p = make_parser("WITHIN SESSION BRACKETS [1d, 7d]");
        let (window, brackets, _) = parse_match_modifiers(&mut p, MatchMode::First).unwrap();
        assert_eq!(window, Some(MatchWindow::WithinSession));
        assert!(brackets.is_some());
    }

    #[test]
    fn modifiers_brackets_empty_is_error() {
        let mut p = make_parser("BRACKETS []");
        match parse_match_modifiers(&mut p, MatchMode::First) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            })
            | Err(ParseError::UnexpectedEof {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::Literal);
                assert!(detail.unwrap_or("").contains("at least one duration"));
            }
            other => panic!("expected Literal error, got {other:?}"),
        }
    }

    #[test]
    fn modifiers_brackets_before_within_is_out_of_order() {
        // `BRACKETS [1d] WITHIN SESSION` — WITHIN must appear before BRACKETS.
        let mut p = make_parser("BRACKETS [1d] WITHIN SESSION");
        match parse_match_modifiers(&mut p, MatchMode::First) {
            Err(ParseError::Unexpected {
                expected, detail, ..
            }) => {
                assert_eq!(expected, Expected::EndOfModifiers);
                assert!(detail
                    .unwrap_or("")
                    .contains("WITHIN must appear before BRACKETS"));
            }
            other => panic!("expected EndOfModifiers error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Span tracking
    // -----------------------------------------------------------------------

    #[test]
    fn event_ref_span_covers_qualified_name() {
        let src = "events.purchase";
        let mut p = make_parser(src);
        let er = parse_event_ref(&mut p).unwrap();
        assert_eq!(er.span.start, 0);
        assert_eq!(er.span.end, src.len());
    }

    #[test]
    fn sequence_span_includes_parens() {
        let src = "SEQUENCE(a THEN b)";
        let mut p = make_parser(src);
        let (_, span) = parse_sequence(&mut p).unwrap();
        assert_eq!(span.start, 0);
        assert_eq!(span.end, src.len());
    }

    // -----------------------------------------------------------------------
    // Integration: full step list shapes
    // -----------------------------------------------------------------------

    #[test]
    fn step_list_qualified_event_refs() {
        let mut p = make_parser("events.signup THEN events.purchase");
        let steps = parse_step_list(&mut p).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(
            step_single_event_ref(&steps[0])
                .unwrap()
                .table
                .as_ref()
                .unwrap()
                .text,
            "events"
        );
    }

    #[test]
    fn step_list_complex_separators() {
        // signup WITHOUT (refund OR churn) THEN IMMEDIATELY activation THEN purchase
        let src = "signup WITHOUT (refund OR churn) THEN IMMEDIATELY activation THEN purchase";
        let mut p = make_parser(src);
        let steps = parse_step_list(&mut p).unwrap();
        assert_eq!(steps.len(), 3);
        // signup has WITHOUT exclusion
        assert!(steps[0].without_next.is_some());
        assert_eq!(steps[0].without_next.as_ref().unwrap().events.len(), 2);
        // signup→activation separator has IMMEDIATELY
        assert!(steps[0].immediately_next);
        // activation has no exclusion
        assert!(steps[1].without_next.is_none());
        // purchase is last — no following separator
        assert!(!steps[2].immediately_next);
        assert!(steps[2].without_next.is_none());
    }

    // -----------------------------------------------------------------------
    // Test helpers — free functions (can't define inherent impls on foreign types)
    // -----------------------------------------------------------------------

    /// Return the event name of a single-event step (panics on alternation).
    fn step_event_name(s: &Step) -> &str {
        match &s.event {
            StepEvent::Single(er) => &er.event.text,
            StepEvent::Alternation(_) => panic!("step_event_name called on alternation step"),
        }
    }

    /// Return the `EventRef` of a single-event step, or `None` for alternation.
    fn step_single_event_ref(s: &Step) -> Option<&EventRef> {
        match &s.event {
            StepEvent::Single(er) => Some(er),
            StepEvent::Alternation(_) => None,
        }
    }
}
