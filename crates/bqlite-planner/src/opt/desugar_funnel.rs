//! FUNNEL → MATCH + STATS desugaring pass.
//!
//! Implements the AST-level rewrite described in
//! `docs/design/planner-pipeline.md` §4.3 and `query-language.md` §6.1.
//!
//! `PipelineStage::Funnel` is syntactic sugar that cannot be lowered
//! directly to a logical plan because desugaring requires schema access
//! (to know how many steps exist and what names to assign them). The
//! rewrite happens here, before TASK-318's logical lowering sees the
//! pipeline. Downstream lowering then processes the two resulting stages —
//! `Match` and `Stats` — through their ordinary lowering paths.
//!
//! ## Desugaring rules (planner-pipeline.md §4.3)
//!
//! ```text
//! FUNNEL(A THEN B THEN C) WITHIN 7d
//! ```
//! becomes:
//! ```text
//! MATCH FIRST SEQUENCE(A THEN B THEN C) WITHIN 7d EMIT ALL
//! | STATS
//!     <name_A> = SUM(CAST(step_reached >= 1 AS INT)),
//!     <name_B> = SUM(CAST(step_reached >= 2 AS INT)),
//!     <name_C> = SUM(CAST(step_reached >= 3 AS INT))
//! ```
//!
//! ## Output name derivation (query-language.md §6.1)
//!
//! 1. Named step (`s: signup`) → step name (`s`).
//! 2. Bare event type (`signup`) → event type name (`signup`).
//! 3. Backtick-quoted event name (`` `page view` ``) → backticks stripped
//!    (this is handled transparently by `Name::text`, which stores the canonical
//!    form; no special case is required in this function).
//! 4. Duplicate output names → [`BqliteError::Plan`] with a `NameCollision`
//!    message directing the user to add explicit step names.
//!
//! **Alternation steps** (e.g. `(signup | login) THEN purchase`) are not
//! covered by query-language.md §6.1. As a planner-level extension, an
//! alternation step without an explicit step name uses the first event type in
//! the alternation as its output name. If this produces a collision, rule 4
//! catches it and the user must add an explicit step name.
//!
//! ## Placement in the compilation pipeline
//!
//! `desugar_funnel` is called from `logical::fold_stage` when it encounters a
//! `PipelineStage::Funnel`. It is **not** an optimizer pass — it runs during
//! the AST-to-logical-plan lowering phase, before any plan node exists. The
//! resulting `Match` and `Stats` stages are folded immediately into the
//! accumulated plan in order, so no separate pipeline-level rewrite pass is
//! needed. It lives under `opt/` as a sibling to the structural optimizer
//! passes for discoverability, but its calling convention is different: it
//! takes an AST node and returns two AST nodes, rather than rewriting a
//! `PhysicalPlan` tree.

use std::collections::HashSet;

use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
use bqlite_ast::operator::{AggItem, Funnel, PipelineStage};
use bqlite_ast::pattern::{MatchMode, MatchPattern, MatchWindow, StepEvent};
use bqlite_ast::span::{Name, Span};
use bqlite_core::{BqlType, BqliteError, Result};

/// Desugar a `FUNNEL` pipeline stage into the equivalent `Match` + `Stats`
/// pair of pipeline stages.
///
/// Returns `(match_stage, stats_stage)`. The caller is responsible for folding
/// each stage into the accumulated logical plan in order.
///
/// # Errors
///
/// Returns [`BqliteError::Plan`] when:
/// - The funnel has zero steps (empty funnel).
/// - Two or more steps produce the same output name without explicit step names
///   (`TypeError::NameCollision` described in query-language.md §6.1 rule 4).
pub fn desugar_funnel(funnel: Funnel) -> Result<(PipelineStage, PipelineStage)> {
    let span = funnel.span;

    if funnel.steps.is_empty() {
        return Err(BqliteError::Plan(
            "FUNNEL must have at least one step".into(),
        ));
    }

    // ── Derive output names for each step ───────────────────────────────────
    // query-language.md §6.1 rules:
    // 1. Named step (`s: signup`)          → step name (`s`)
    // 2. Bare event type (`signup`)         → event type name (`signup`)
    // 3. Alternation without step name      → first event in the alternation
    // 4. Duplicate names                    → NameCollision error

    let mut output_names: Vec<String> = Vec::with_capacity(funnel.steps.len());
    let mut seen: HashSet<String> = HashSet::new();

    for step in &funnel.steps {
        let name = step_output_name(step);
        if !seen.insert(name.clone()) {
            return Err(BqliteError::Plan(format!(
                "FUNNEL step output name collision: `{name}` — add explicit step names \
                 to disambiguate (e.g. `s1: {name} THEN s2: {name}`)"
            )));
        }
        output_names.push(name);
    }

    // ── Build the MATCH stage ────────────────────────────────────────────────
    // FUNNEL desugars to MATCH FIRST ... EMIT ALL, which uses MatchMode::EmitAll.
    // The window from the Funnel (in nanoseconds) becomes MatchWindow::Within.

    let match_stage = PipelineStage::Match {
        pattern: MatchPattern {
            steps: funnel.steps,
            mode: MatchMode::EmitAll,
            window: funnel.window.map(MatchWindow::Within),
            brackets: None,
            span,
        },
        span,
    };

    // ── Build the STATS stage ────────────────────────────────────────────────
    // One SUM(CAST(step_reached >= N AS INT)) per step, 1-indexed.
    //
    // Expression tree for step N (1-indexed):
    //
    //   AggItem {
    //     function: "sum",
    //     args: [ CAST(step_reached >= N AS INT) ],
    //     alias: <output_name>,
    //   }

    let aggregates: Vec<AggItem> = output_names
        .into_iter()
        .enumerate()
        .map(|(idx, output_name)| {
            // N is 1-indexed
            let n = (idx + 1) as i64;

            // step_reached >= N
            let cmp = Expr::Compare {
                op: CompareOp::GreaterOrEqual,
                left: Box::new(Spanned::new(
                    Expr::Column(Name::synthetic("step_reached")),
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(Expr::Literal(Literal::Int(n)), Span::EMPTY)),
            };

            // CAST(step_reached >= N AS INT)
            let cast = Expr::Cast {
                expr: Box::new(Spanned::new(cmp, Span::EMPTY)),
                target: BqlType::Int,
            };

            AggItem {
                function: Name::synthetic("sum"),
                args: vec![Spanned::new(cast, Span::EMPTY)],
                // FUNNEL counts every row that reached step N, not distinct
                // values — distinct: true would incorrectly count at most 1
                // per unique CAST result (which is 0 or 1, so it would
                // collapse to a boolean count rather than an entity count).
                distinct: false,
                alias: Name::synthetic(output_name),
                span,
            }
        })
        .collect();

    let stats_stage = PipelineStage::Stats {
        aggregates,
        group_by: vec![],
        span,
    };

    Ok((match_stage, stats_stage))
}

/// Derive the aggregate output name for a funnel step.
///
/// Precedence (query-language.md §6.1 + planner extension for alternations):
/// 1. Explicit step name (`s: signup`) → `"s"`.
/// 2. Single event type (`signup`)     → `"signup"`.
/// 3. Alternation without step name    → first event type in the alternation
///    (planner extension; user must add a step name if this collides).
fn step_output_name(step: &bqlite_ast::pattern::Step) -> String {
    // Rule 1: explicit step name wins.
    if let Some(name) = &step.name {
        return name.text.clone();
    }
    // Rule 2 / planner-extension rule 3: fall back to the event type.
    match &step.event {
        StepEvent::Single(event_ref) => event_ref.event.text.clone(),
        StepEvent::Alternation(events) => {
            // No step name on an alternation: use the first event type.
            // An empty alternation is an invariant violation — the parser
            // never produces one — so we treat it as unreachable.
            debug_assert!(
                !events.is_empty(),
                "alternation must have at least one event"
            );
            events
                .first()
                .expect("alternation must have at least one event (parser invariant)")
                .event
                .text
                .clone()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bqlite_ast::pattern::{EventRef, MatchMode, MatchWindow, Step};
    use bqlite_ast::span::{Name, Span};

    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn event_ref(name: &str) -> EventRef {
        EventRef {
            table: None,
            event: Name::synthetic(name),
            span: Span::EMPTY,
        }
    }

    fn bare_step(event_name: &str) -> Step {
        Step {
            name: None,
            event: StepEvent::Single(event_ref(event_name)),
            predicate: None,
            repetition: None,
            immediately_next: false,
            without_next: None,
            span: Span::EMPTY,
        }
    }

    fn named_step(step_name: &str, event_name: &str) -> Step {
        Step {
            name: Some(Name::synthetic(step_name)),
            event: StepEvent::Single(event_ref(event_name)),
            predicate: None,
            repetition: None,
            immediately_next: false,
            without_next: None,
            span: Span::EMPTY,
        }
    }

    fn funnel(steps: Vec<Step>, window: Option<i64>) -> Funnel {
        Funnel {
            steps,
            window,
            span: Span::EMPTY,
        }
    }

    // ── Happy path ───────────────────────────────────────────────────────────

    #[test]
    fn two_step_funnel_produces_match_and_stats() {
        let f = funnel(
            vec![bare_step("signup"), bare_step("purchase")],
            Some(604_800_000_000_000), // 7d in ns
        );
        let (match_stage, stats_stage) = desugar_funnel(f).expect("two-step funnel");

        // Match stage
        let PipelineStage::Match { pattern, .. } = match_stage else {
            panic!("expected Match stage");
        };
        assert_eq!(pattern.mode, MatchMode::EmitAll);
        assert_eq!(pattern.steps.len(), 2);
        assert_eq!(
            pattern.window,
            Some(MatchWindow::Within(604_800_000_000_000))
        );
        assert!(pattern.brackets.is_none());

        // Stats stage
        let PipelineStage::Stats {
            aggregates,
            group_by,
            ..
        } = stats_stage
        else {
            panic!("expected Stats stage");
        };
        assert!(group_by.is_empty(), "FUNNEL STATS has no GROUP BY");
        assert_eq!(aggregates.len(), 2);

        // First aggregate: signup = SUM(CAST(step_reached >= 1 AS INT))
        assert_eq!(aggregates[0].alias.text, "signup");
        assert_eq!(aggregates[0].function.text, "sum");
        assert_eq!(aggregates[0].args.len(), 1);

        // Second aggregate: purchase = SUM(CAST(step_reached >= 2 AS INT))
        assert_eq!(aggregates[1].alias.text, "purchase");
        assert_eq!(aggregates[1].function.text, "sum");
        assert_eq!(aggregates[1].args.len(), 1);
    }

    #[test]
    fn three_step_funnel_produces_three_aggregates() {
        let f = funnel(
            vec![
                bare_step("signup"),
                bare_step("activation"),
                bare_step("purchase"),
            ],
            None,
        );
        let (_match_stage, stats_stage) = desugar_funnel(f).expect("three-step funnel");

        let PipelineStage::Stats { aggregates, .. } = stats_stage else {
            panic!("expected Stats stage");
        };
        assert_eq!(aggregates.len(), 3);
        assert_eq!(aggregates[0].alias.text, "signup");
        assert_eq!(aggregates[1].alias.text, "activation");
        assert_eq!(aggregates[2].alias.text, "purchase");
    }

    #[test]
    fn three_step_funnel_no_window() {
        let f = funnel(vec![bare_step("a"), bare_step("b"), bare_step("c")], None);
        let (match_stage, _) = desugar_funnel(f).expect("no-window funnel");
        let PipelineStage::Match { pattern, .. } = match_stage else {
            panic!("expected Match");
        };
        assert!(pattern.window.is_none());
    }

    #[test]
    fn named_steps_use_step_names_as_output_names() {
        // s: signup THEN p: purchase
        let f = funnel(
            vec![named_step("s", "signup"), named_step("p", "purchase")],
            None,
        );
        let (_match_stage, stats_stage) = desugar_funnel(f).expect("named-step funnel");

        let PipelineStage::Stats { aggregates, .. } = stats_stage else {
            panic!("expected Stats stage");
        };
        assert_eq!(aggregates[0].alias.text, "s", "step name takes precedence");
        assert_eq!(aggregates[1].alias.text, "p", "step name takes precedence");
    }

    #[test]
    fn step_reached_comparison_uses_one_indexed_n() {
        // Single step: step_reached >= 1
        let f = funnel(vec![bare_step("signup")], None);
        let (_match_stage, stats_stage) = desugar_funnel(f).expect("single step");

        let PipelineStage::Stats { aggregates, .. } = stats_stage else {
            panic!("expected Stats stage");
        };
        assert_eq!(aggregates.len(), 1);

        // Inspect the AST expression: CAST(step_reached >= 1 AS INT)
        let arg = &aggregates[0].args[0].node;
        let Expr::Cast { expr, target } = arg else {
            panic!("expected Cast, got {arg:?}");
        };
        assert_eq!(*target, BqlType::Int);

        let Expr::Compare { op, left: _, right } = &expr.node else {
            panic!("expected Compare inside Cast");
        };
        assert_eq!(*op, CompareOp::GreaterOrEqual);
        let Expr::Literal(Literal::Int(n)) = &right.node else {
            panic!("expected Int literal for N");
        };
        assert_eq!(*n, 1, "first step is 1-indexed");
    }

    #[test]
    fn three_step_n_values_are_one_two_three() {
        let f = funnel(vec![bare_step("a"), bare_step("b"), bare_step("c")], None);
        let (_, stats_stage) = desugar_funnel(f).unwrap();
        let PipelineStage::Stats { aggregates, .. } = stats_stage else {
            panic!();
        };

        for (i, agg) in aggregates.iter().enumerate() {
            let Expr::Cast { expr, .. } = &agg.args[0].node else {
                panic!("expected Cast at index {i}");
            };
            let Expr::Compare { right, .. } = &expr.node else {
                panic!("expected Compare at index {i}");
            };
            let Expr::Literal(Literal::Int(n)) = &right.node else {
                panic!("expected Int literal at index {i}");
            };
            assert_eq!(*n, (i + 1) as i64, "step {i} should use N={}", i + 1);
        }
    }

    // ── Error cases ──────────────────────────────────────────────────────────

    #[test]
    fn duplicate_event_type_names_raise_name_collision_error() {
        // signup THEN signup — both would produce "signup" as output name.
        let f = funnel(vec![bare_step("signup"), bare_step("signup")], None);
        let err = desugar_funnel(f).expect_err("collision expected");

        let BqliteError::Plan(msg) = err else {
            panic!("expected Plan error, got {err:?}");
        };
        assert!(
            msg.contains("signup"),
            "error should mention the colliding name; got: {msg}"
        );
        assert!(
            msg.contains("collision") || msg.contains("disambiguate"),
            "error should suggest fix; got: {msg}"
        );
    }

    #[test]
    fn named_steps_avoid_collision_on_same_event_type() {
        // s1: signup THEN s2: signup — step names differ, no collision.
        let f = funnel(
            vec![named_step("s1", "signup"), named_step("s2", "signup")],
            None,
        );
        let (_match, stats) = desugar_funnel(f).expect("named steps avoid collision");
        let PipelineStage::Stats { aggregates, .. } = stats else {
            panic!("expected Stats");
        };
        assert_eq!(aggregates[0].alias.text, "s1");
        assert_eq!(aggregates[1].alias.text, "s2");
    }

    #[test]
    fn empty_funnel_returns_error() {
        let f = funnel(vec![], None);
        let err = desugar_funnel(f).expect_err("empty funnel should error");
        assert!(matches!(err, BqliteError::Plan(_)));
    }

    #[test]
    fn alternation_step_without_name_uses_first_event_type() {
        // (signup | purchase) THEN click — alternation step has no name.
        let alt_step = Step {
            name: None,
            event: StepEvent::Alternation(vec![event_ref("signup"), event_ref("purchase")]),
            predicate: None,
            repetition: None,
            immediately_next: false,
            without_next: None,
            span: Span::EMPTY,
        };
        let f = funnel(vec![alt_step, bare_step("click")], None);
        let (_, stats) = desugar_funnel(f).expect("alternation without name");
        let PipelineStage::Stats { aggregates, .. } = stats else {
            panic!("expected Stats");
        };
        assert_eq!(aggregates[0].alias.text, "signup"); // first event in alternation
        assert_eq!(aggregates[1].alias.text, "click");
    }
}
