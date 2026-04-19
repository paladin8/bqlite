//! RETENTION → MATCH + STATS desugaring pass.
//!
//! Implements the AST-level rewrite described in
//! `docs/design/query-language.md` §6.3.
//!
//! `PipelineStage::Retention` is syntactic sugar over `MATCH FIRST … BRACKETS
//! … EMIT ALL` followed by `STATS retention_rate = AVG(…) GROUP BY bracket`.
//! The rewrite happens here, before logical lowering sees the pipeline.
//! Downstream lowering then processes the two resulting stages — `Match` and
//! `Stats` — through their ordinary lowering paths.
//!
//! ## Desugaring rule (query-language.md §6.3)
//!
//! ```text
//! RETENTION(entry: signup, activity: purchase, brackets: [1d, 7d, 14d, 30d])
//! ```
//! becomes:
//! ```text
//! MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
//! | STATS retention_rate = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket
//! ```
//!
//! For `cumulative: true` the `BRACKETS` spec carries the flag into the
//! `BracketSpec` on the `MatchPattern`; the match operator interprets it as
//! a partial-sum accumulation and the STATS expression remains identical.
//!
//! ## Scan-range widening
//!
//! The planner's `lower_match` already extends the scan's upper time bound by
//! `max(brackets)` when a `MatchPattern` carries a `BracketSpec`
//! (query-language.md §3.2, logical.rs `lower_match` lines ~2028-2032).
//! No additional logic is needed here; the bracket spec is forwarded as-is
//! into the generated `Match` pattern.
//!
//! ## EXPLAIN output
//!
//! The two desugared stages (`Match` and `Stats`) carry the original
//! `Retention` source span so that EXPLAIN and error messages reference the
//! correct user-visible location.
//!
//! ## Placement in the compilation pipeline
//!
//! `desugar_retention` is called from `logical::fold_stage` when it encounters
//! a `PipelineStage::Retention`. It returns an AST `(Match, Stats)` pair;
//! `fold_stage` then folds each through the normal lowering path. This
//! mirrors exactly how `desugar_funnel` works for `PipelineStage::Funnel`.

use bqlite_ast::expr::{CompareOp, Expr, Literal, Spanned};
use bqlite_ast::operator::{AggItem, GroupItem, PipelineStage, Retention};
use bqlite_ast::pattern::{MatchMode, MatchPattern, Step, StepEvent};
use bqlite_ast::span::{Name, Span};
use bqlite_core::{BqlType, BqliteError, Result};

/// Desugar a `RETENTION` pipeline stage into the equivalent `Match` + `Stats`
/// pair of pipeline stages.
///
/// Returns `(match_stage, stats_stage)`. The caller is responsible for folding
/// each stage into the accumulated logical plan in order.
///
/// # Errors
///
/// Returns [`BqliteError::Plan`] when:
/// - The bracket list is empty.
pub fn desugar_retention(retention: Retention) -> Result<(PipelineStage, PipelineStage)> {
    let span = retention.span;

    if retention.brackets.durations.is_empty() {
        return Err(BqliteError::Plan(
            "RETENTION brackets list must have at least one duration".into(),
        ));
    }

    // ── Build the MATCH stage ────────────────────────────────────────────────
    // RETENTION desugars to MATCH FIRST (entry THEN activity) BRACKETS [...] EMIT ALL.
    //
    // The entry and activity are bare EventRefs (the sugar intentionally does
    // not support per-step predicates, variable bindings, or WITHOUT —
    // query-language.md §6.3 "Limitation").  We wrap each EventRef in a
    // minimal Step with all optional fields set to None/false.
    //
    // Each step carries its EventRef's own source span so that error messages
    // and future EXPLAIN renderings can point at the specific entry/activity
    // token rather than the whole RETENTION expression.

    let entry_span = retention.entry.span;
    let activity_span = retention.activity.span;

    let entry_step = Step {
        name: None,
        event: StepEvent::Single(retention.entry),
        predicate: None,
        repetition: None,
        immediately_next: false,
        without_next: None,
        span: entry_span,
    };

    let activity_step = Step {
        name: None,
        event: StepEvent::Single(retention.activity),
        predicate: None,
        repetition: None,
        immediately_next: false,
        without_next: None,
        span: activity_span,
    };

    let match_stage = PipelineStage::Match {
        pattern: MatchPattern {
            steps: vec![entry_step, activity_step],
            mode: MatchMode::First,
            emit_all: true,
            // BRACKETS and WITHIN are mutually exclusive (query-language.md §4.12).
            window: None,
            brackets: Some(retention.brackets),
            span,
        },
        span,
    };

    // ── Build the STATS stage ────────────────────────────────────────────────
    // retention_rate = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket
    //
    // The activity is always step 2 (1-indexed) in the two-step sequence.
    //
    // Expression tree:
    //
    //   AggItem {
    //     function: "avg",
    //     args: [ CAST(step_reached >= 2 AS INT) ],
    //     alias: "retention_rate",
    //   }
    //   GROUP BY bracket

    // step_reached >= 2
    let cmp = Expr::Compare {
        op: CompareOp::GreaterOrEqual,
        left: Box::new(Spanned::new(
            Expr::Column(Name::synthetic("step_reached")),
            Span::EMPTY,
        )),
        right: Box::new(Spanned::new(Expr::Literal(Literal::Int(2)), Span::EMPTY)),
    };

    // CAST(step_reached >= 2 AS INT)
    let cast = Expr::Cast {
        expr: Box::new(Spanned::new(cmp, Span::EMPTY)),
        target: BqlType::Int,
    };

    let aggregate = AggItem {
        function: Name::synthetic("avg"),
        args: vec![Spanned::new(cast, Span::EMPTY)],
        distinct: false,
        alias: Name::synthetic("retention_rate"),
        span,
    };

    // GROUP BY bracket
    let group_by_bracket = GroupItem {
        expr: Spanned::new(Expr::Column(Name::synthetic("bracket")), Span::EMPTY),
        alias: None,
        span,
    };

    let stats_stage = PipelineStage::Stats {
        aggregates: vec![aggregate],
        group_by: vec![group_by_bracket],
        span,
    };

    Ok((match_stage, stats_stage))
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use bqlite_ast::pattern::{BracketSpec, EventRef, MatchMode, MatchWindow};
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

    fn brackets(durations: Vec<i64>, cumulative: bool) -> BracketSpec {
        BracketSpec {
            durations,
            cumulative,
            span: Span::EMPTY,
        }
    }

    fn retention(entry: &str, activity: &str, durations: Vec<i64>, cumulative: bool) -> Retention {
        Retention {
            entry: event_ref(entry),
            activity: event_ref(activity),
            brackets: brackets(durations, cumulative),
            span: Span::EMPTY,
        }
    }

    // ── Happy path ───────────────────────────────────────────────────────────

    #[test]
    fn basic_retention_produces_match_and_stats() {
        let r = retention(
            "signup",
            "purchase",
            vec![
                86_400_000_000_000,      // 1d
                7 * 86_400_000_000_000,  // 7d
                14 * 86_400_000_000_000, // 14d
                30 * 86_400_000_000_000, // 30d
            ],
            false,
        );
        let (match_stage, stats_stage) = desugar_retention(r).expect("basic retention");

        // ── Match stage ──
        let PipelineStage::Match { pattern, .. } = match_stage else {
            panic!("expected Match stage");
        };
        assert_eq!(pattern.mode, MatchMode::First);
        assert!(pattern.emit_all);
        assert!(pattern.window.is_none(), "BRACKETS implies no WITHIN");
        assert_eq!(pattern.steps.len(), 2);

        let brackets = pattern.brackets.expect("brackets must be set");
        assert_eq!(brackets.durations.len(), 4);
        assert!(!brackets.cumulative);

        // Entry step: bare signup
        assert!(pattern.steps[0].name.is_none());
        let StepEvent::Single(ref entry_ref) = pattern.steps[0].event else {
            panic!("entry must be a single event ref");
        };
        assert_eq!(entry_ref.event.text, "signup");

        // Activity step: bare purchase
        assert!(pattern.steps[1].name.is_none());
        let StepEvent::Single(ref activity_ref) = pattern.steps[1].event else {
            panic!("activity must be a single event ref");
        };
        assert_eq!(activity_ref.event.text, "purchase");

        // ── Stats stage ──
        let PipelineStage::Stats {
            aggregates,
            group_by,
            ..
        } = stats_stage
        else {
            panic!("expected Stats stage");
        };

        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0].alias.text, "retention_rate");
        assert_eq!(aggregates[0].function.text, "avg");
        assert!(!aggregates[0].distinct);
        assert_eq!(aggregates[0].args.len(), 1);

        assert_eq!(group_by.len(), 1);
        let Expr::Column(ref col_name) = group_by[0].expr.node else {
            panic!("GROUP BY must be a column ref");
        };
        assert_eq!(col_name.text, "bracket");
    }

    #[test]
    fn cumulative_brackets_flag_is_preserved() {
        let r = retention(
            "signup",
            "purchase",
            vec![86_400_000_000_000, 7 * 86_400_000_000_000],
            true, // cumulative
        );
        let (match_stage, _stats) = desugar_retention(r).expect("cumulative retention");
        let PipelineStage::Match { pattern, .. } = match_stage else {
            panic!("expected Match");
        };
        let brackets = pattern.brackets.expect("brackets must be set");
        assert!(
            brackets.cumulative,
            "cumulative flag must be forwarded from Retention"
        );
    }

    #[test]
    fn step_reached_comparison_uses_value_two() {
        // AVG aggregate must compare step_reached >= 2 (activity is step 2).
        let r = retention("signup", "purchase", vec![86_400_000_000_000], false);
        let (_match, stats_stage) = desugar_retention(r).expect("step_reached >= 2");
        let PipelineStage::Stats { aggregates, .. } = stats_stage else {
            panic!("expected Stats");
        };

        let arg = &aggregates[0].args[0].node;
        let Expr::Cast { expr, target } = arg else {
            panic!("expected Cast, got {arg:?}");
        };
        assert_eq!(*target, BqlType::Int);

        let Expr::Compare { op, right, .. } = &expr.node else {
            panic!("expected Compare inside Cast");
        };
        assert_eq!(*op, CompareOp::GreaterOrEqual);
        let Expr::Literal(Literal::Int(n)) = &right.node else {
            panic!("expected Int literal");
        };
        assert_eq!(*n, 2, "activity is step 2 (1-indexed)");
    }

    #[test]
    fn aggregate_function_is_avg_not_sum() {
        // RETENTION uses AVG (rate), FUNNEL uses SUM (count).
        let r = retention("install", "purchase", vec![86_400_000_000_000], false);
        let (_, stats_stage) = desugar_retention(r).expect("avg check");
        let PipelineStage::Stats { aggregates, .. } = stats_stage else {
            panic!("expected Stats");
        };
        assert_eq!(aggregates[0].function.text, "avg");
    }

    #[test]
    fn no_window_on_match_when_brackets_present() {
        // BRACKETS and WITHIN are mutually exclusive — desugared Match must
        // have window = None even when the original brackets list is non-empty.
        let r = retention(
            "signup",
            "activation",
            vec![86_400_000_000_000, 7 * 86_400_000_000_000],
            false,
        );
        let (match_stage, _) = desugar_retention(r).expect("no window");
        let PipelineStage::Match { pattern, .. } = match_stage else {
            panic!("expected Match");
        };
        assert!(
            pattern.window.is_none(),
            "BRACKETS and WITHIN are mutually exclusive"
        );
        // Specifically: no Within variant.
        assert!(!matches!(pattern.window, Some(MatchWindow::Within(_))));
    }

    #[test]
    fn table_qualified_event_refs_are_preserved() {
        // query-language.md §19.3: event refs may carry a table qualifier.
        let entry = EventRef {
            table: Some(Name::synthetic("events")),
            event: Name::synthetic("signup"),
            span: Span::EMPTY,
        };
        let activity = EventRef {
            table: Some(Name::synthetic("purchases")),
            event: Name::synthetic("purchase"),
            span: Span::EMPTY,
        };
        let r = Retention {
            entry,
            activity,
            brackets: BracketSpec {
                durations: vec![86_400_000_000_000],
                cumulative: false,
                span: Span::EMPTY,
            },
            span: Span::EMPTY,
        };
        let (match_stage, _) = desugar_retention(r).expect("qualified refs");
        let PipelineStage::Match { pattern, .. } = match_stage else {
            panic!("expected Match");
        };
        let StepEvent::Single(ref entry_ref) = pattern.steps[0].event else {
            panic!("entry must be Single");
        };
        assert_eq!(
            entry_ref.table.as_ref().map(|n| n.text.as_str()),
            Some("events"),
            "table qualifier on entry must be preserved"
        );
        let StepEvent::Single(ref act_ref) = pattern.steps[1].event else {
            panic!("activity must be Single");
        };
        assert_eq!(
            act_ref.table.as_ref().map(|n| n.text.as_str()),
            Some("purchases"),
            "table qualifier on activity must be preserved"
        );
    }

    // ── Error cases ──────────────────────────────────────────────────────────

    #[test]
    fn empty_brackets_returns_error() {
        let r = Retention {
            entry: event_ref("signup"),
            activity: event_ref("purchase"),
            brackets: BracketSpec {
                durations: vec![],
                cumulative: false,
                span: Span::EMPTY,
            },
            span: Span::EMPTY,
        };
        let err = desugar_retention(r).expect_err("empty brackets must error");
        assert!(matches!(err, BqliteError::Plan(_)));
    }
}
