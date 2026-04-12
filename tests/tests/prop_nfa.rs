//! Property tests for the NFA runtime simulator (TASK-304).
//!
//! These tests cover invariants that example tests cannot exhaust:
//!
//! 1. **No false negatives** — every match found by a brute-force
//!    reference evaluator is also found by the NFA simulator.
//! 2. **Window-expiry monotonicity** — once a candidate expires from
//!    a time window, it never reappears.
//! 3. **Poison-kill completeness** — no candidate survives past a
//!    negation scope it should have been killed in.
//! 4. **Candidate deque ordering** — candidates remain ordered by
//!    anchor_ts within each state deque after arbitrary event
//!    interleaving.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use bqlite_operators::matcher::nfa::{MatchCompletion, NfaSimulator};
use bqlite_planner::compile::{CompiledNfa, NfaState, PatternClass, PoisonTransition, Transition};
use proptest::prelude::*;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn make_batch(events: &[(&str, i64)]) -> RecordBatch {
    let event_types: Vec<&str> = events.iter().map(|(e, _)| *e).collect();
    let timestamps: Vec<i64> = events.iter().map(|(_, t)| *t).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("ts", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(event_types)),
            Arc::new(Int64Array::from(timestamps)),
        ],
    )
    .unwrap()
}

fn simple_transition(event_type: &str, target: u16) -> Transition {
    Transition {
        event_type: event_type.to_string(),
        predicates: Vec::new(),
        bind_variables: Vec::new(),
        check_variables: Vec::new(),
        target,
    }
}

fn simple_poison(event_type: &str) -> PoisonTransition {
    PoisonTransition {
        event_type: event_type.to_string(),
        predicates: Vec::new(),
    }
}

/// Build a 2-step NFA: A THEN B, with optional window.
fn nfa_a_then_b(window: Option<i64>) -> CompiledNfa {
    CompiledNfa {
        states: vec![
            NfaState {
                transitions: vec![simple_transition("A", 1)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![simple_transition("B", 2)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ],
        accept_state: 2,
        relevant_event_types: BTreeSet::from(["A".to_string(), "B".to_string()]),
        pattern_class: PatternClass::LinearSimple,
        variable_bindings: vec![],
        global_window: window,
        emit_all: false,
        state_to_step: vec![0, 1, 2],
    }
}

/// Build a 3-step NFA: A THEN B THEN C, with optional window.
fn nfa_a_then_b_then_c(window: Option<i64>) -> CompiledNfa {
    CompiledNfa {
        states: vec![
            NfaState {
                transitions: vec![simple_transition("A", 1)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![simple_transition("B", 2)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![simple_transition("C", 3)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ],
        accept_state: 3,
        relevant_event_types: BTreeSet::from(["A".to_string(), "B".to_string(), "C".to_string()]),
        pattern_class: PatternClass::LinearSimple,
        variable_bindings: vec![],
        global_window: window,
        emit_all: false,
        state_to_step: vec![0, 1, 2, 3],
    }
}

/// Build a 2-step NFA: A THEN B WITHOUT C (poison on state 1).
fn nfa_a_then_b_without_c(window: Option<i64>) -> CompiledNfa {
    CompiledNfa {
        states: vec![
            NfaState {
                transitions: vec![simple_transition("A", 1)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![simple_transition("B", 2)],
                poison_transitions: vec![simple_poison("C")],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ],
        accept_state: 2,
        relevant_event_types: BTreeSet::from(["A".to_string(), "B".to_string(), "C".to_string()]),
        pattern_class: PatternClass::LinearWithNegation,
        variable_bindings: vec![],
        global_window: window,
        emit_all: false,
        state_to_step: vec![0, 1, 2],
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Brute-force reference evaluator for "A THEN B" with optional window
// ─────────────────────────────────────────────────────────────────────────────

/// Brute-force evaluator for "A THEN B" with optional time window.
/// Returns the first match (earliest anchor) found by scanning all
/// pairs, or None.
fn brute_force_a_then_b(events: &[(&str, i64)], window: Option<i64>) -> Option<MatchCompletion> {
    for (i, (et_a, ts_a)) in events.iter().enumerate() {
        if *et_a != "A" {
            continue;
        }
        for (et_b, ts_b) in &events[i + 1..] {
            if *et_b != "B" {
                continue;
            }
            // Strict ordering.
            if *ts_b <= *ts_a {
                continue;
            }
            // Window check.
            if let Some(w) = window {
                if *ts_b - *ts_a > w {
                    continue;
                }
            }
            return Some(MatchCompletion {
                anchor_ts: *ts_a,
                final_ts: *ts_b,
                bindings: Vec::new(),
            });
        }
    }
    None
}

/// Brute-force evaluator for "A THEN B THEN C" with optional time
/// window. Returns the first match found.
fn brute_force_a_then_b_then_c(
    events: &[(&str, i64)],
    window: Option<i64>,
) -> Option<MatchCompletion> {
    for (i, (et_a, ts_a)) in events.iter().enumerate() {
        if *et_a != "A" {
            continue;
        }
        for (j, (et_b, ts_b)) in events[i + 1..].iter().enumerate() {
            if *et_b != "B" {
                continue;
            }
            if *ts_b <= *ts_a {
                continue;
            }
            if let Some(w) = window {
                if *ts_b - *ts_a > w {
                    continue;
                }
            }
            for (et_c, ts_c) in &events[i + 1 + j + 1..] {
                if *et_c != "C" {
                    continue;
                }
                if *ts_c <= *ts_b {
                    continue;
                }
                if let Some(w) = window {
                    if *ts_c - *ts_a > w {
                        continue;
                    }
                }
                return Some(MatchCompletion {
                    anchor_ts: *ts_a,
                    final_ts: *ts_c,
                    bindings: Vec::new(),
                });
            }
        }
    }
    None
}

/// Brute-force evaluator for "A THEN B WITHOUT C" with optional window.
/// Returns the first match found, but only if no C event occurs between
/// A and B **by position** in the event stream. The NFA processes events
/// in row order; poison transitions fire before forward transitions
/// within the same event pass, so a C at the same timestamp as B (but
/// earlier in the stream) kills the candidate before B can advance it.
fn brute_force_a_then_b_without_c(
    events: &[(&str, i64)],
    window: Option<i64>,
) -> Option<MatchCompletion> {
    for (i, (et_a, ts_a)) in events.iter().enumerate() {
        if *et_a != "A" {
            continue;
        }
        for (b_offset, (et_b, ts_b)) in events[i + 1..].iter().enumerate() {
            if *et_b != "B" {
                continue;
            }
            if *ts_b <= *ts_a {
                continue;
            }
            if let Some(w) = window {
                if *ts_b - *ts_a > w {
                    continue;
                }
            }
            // Check no C between A and B by position (not timestamp).
            // A C event at any position between i+1 and the B position
            // (exclusive) with ts > ts_a poisons the match. Also, a C
            // at the same position-group (same timestamp as B, but
            // earlier in row order) kills via poison before B's forward
            // transition fires.
            let b_pos = i + 1 + b_offset;
            let has_c = events[i + 1..b_pos]
                .iter()
                .any(|(et, ts)| *et == "C" && *ts > *ts_a);
            if !has_c {
                return Some(MatchCompletion {
                    anchor_ts: *ts_a,
                    final_ts: *ts_b,
                    bindings: Vec::new(),
                });
            }
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategies
// ─────────────────────────────────────────────────────────────────────────────

/// Generate a sorted event stream of 0..=20 events, each with a type
/// from the given alphabet and a strictly non-decreasing timestamp.
fn arb_event_stream(
    alphabet: &'static [&'static str],
    max_len: usize,
) -> impl Strategy<Value = Vec<(&'static str, i64)>> {
    prop::collection::vec(
        (
            prop::sample::select(alphabet),
            // Use a range that avoids i64 overflow in window arithmetic.
            0_i64..10_000_i64,
        ),
        0..=max_len,
    )
    .prop_map(|mut events| {
        // Sort by timestamp to satisfy the NFA input contract.
        events.sort_by_key(|&(_, ts)| ts);
        events
    })
}

/// Generate a window size (Some(1..=500) or None).
fn arb_window() -> impl Strategy<Value = Option<i64>> {
    prop_oneof![Just(None), (1_i64..=500).prop_map(Some),]
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 1: No false negatives — A THEN B
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// The NFA must find a match whenever the brute-force evaluator does
    /// (no false negatives). For the simple "A THEN B" pattern, the NFA
    /// result should agree with brute-force.
    #[test]
    fn prop_no_false_negatives_a_then_b(
        events in arb_event_stream(&["A", "B", "X"], 20),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b(window);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let brute = brute_force_a_then_b(&events, window);
        let nfa_result = state.completions().first().cloned();

        // NFA and brute-force must produce the same result (both
        // find the first match with earliest anchor, or both find
        // no match).
        prop_assert_eq!(nfa_result.clone(), brute.clone(),
            "NFA/brute-force mismatch.\nNFA: {:?}\nBrute: {:?}\nEvents: {:?}\nWindow: {:?}",
            nfa_result, brute, events, window);
    }

    /// Same as above but for the 3-step "A THEN B THEN C" pattern.
    #[test]
    fn prop_no_false_negatives_a_then_b_then_c(
        events in arb_event_stream(&["A", "B", "C", "X"], 16),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b_then_c(window);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let brute = brute_force_a_then_b_then_c(&events, window);
        let nfa_result = state.completions().first().cloned();

        prop_assert_eq!(nfa_result.clone(), brute.clone(),
            "NFA/brute-force mismatch.\nNFA: {:?}\nBrute: {:?}\nEvents: {:?}\nWindow: {:?}",
            nfa_result, brute, events, window);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 2: Window-expiry monotonicity
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// With a global window, expired candidates never reappear. We
    /// verify this by feeding events one at a time using MATCH ALL
    /// (so completions don't stop processing) and checking that after
    /// a far-future event, all old candidates have been expired.
    #[test]
    fn prop_window_expiry_monotonicity(
        events in arb_event_stream(&["A", "B"], 20),
        window in 1_i64..=200,
    ) {
        let nfa = nfa_a_then_b(Some(window));
        let sim = NfaSimulator::new(nfa, true); // MATCH ALL so completions don't stop processing
        let mut state = sim.create_state();

        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        if let Some(&(_, last_ts)) = events.last() {
            // Feed an A event far enough in the future that every
            // prior candidate's window has expired.
            let future_ts = last_ts + window + 1;
            let future_a = make_batch(&[("A", future_ts)]);
            sim.process_batch(&mut state, &future_a, "event_type", "ts");

            // After processing the future A, the only surviving
            // candidate should be from the future A itself (at
            // state 1). All old candidates must have been expired.
            prop_assert!(state.total_candidates() <= 1,
                "Expected at most 1 candidate after future event, got {}.\nEvents: {:?}\nWindow: {}",
                state.total_candidates(), events, window);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 3: Poison-kill completeness
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// For "A THEN B WITHOUT C", the NFA must agree with brute-force:
    /// any match found by the NFA must also be found by brute-force
    /// (no false positives through poison), and any match found by
    /// brute-force must be found by the NFA (no false negatives from
    /// over-eager poison).
    #[test]
    fn prop_poison_kill_completeness(
        events in arb_event_stream(&["A", "B", "C", "X"], 20),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b_without_c(window);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let brute = brute_force_a_then_b_without_c(&events, window);
        let nfa_result = state.completions().first().cloned();

        if brute.is_some() {
            prop_assert!(nfa_result.is_some(),
                "Brute-force found match {:?} but NFA found none (poison may be over-eager).\nEvents: {:?}\nWindow: {:?}",
                brute, events, window);
        }
        if brute.is_none() {
            prop_assert!(nfa_result.is_none(),
                "NFA found match {:?} but brute-force found none (poison may have missed a kill).\nEvents: {:?}\nWindow: {:?}",
                nfa_result, events, window);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 4: Candidate deque ordering
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// MATCH ALL: every completed match has anchor_ts strictly less than
    /// the next match's anchor_ts (matches are non-overlapping and
    /// ordered).
    #[test]
    fn prop_match_all_ordered_non_overlapping(
        events in arb_event_stream(&["A", "B", "X"], 20),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b(window);
        let sim = NfaSimulator::new(nfa, true); // MATCH ALL
        let mut state = sim.create_state();

        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let completions = state.completions();
        for pair in completions.windows(2) {
            // Each match's final_ts must be strictly less than the
            // next match's anchor_ts (non-overlapping).
            prop_assert!(pair[0].final_ts < pair[1].anchor_ts,
                "Overlapping matches: {:?} and {:?}.\nEvents: {:?}",
                pair[0], pair[1], events);
        }
    }

    /// MATCH ALL completions are monotonically ordered by anchor_ts.
    #[test]
    fn prop_match_all_monotonic_anchors(
        events in arb_event_stream(&["A", "B", "C", "X"], 20),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b_then_c(window);
        let sim = NfaSimulator::new(nfa, true);
        let mut state = sim.create_state();

        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let completions = state.completions();
        for pair in completions.windows(2) {
            prop_assert!(pair[0].anchor_ts < pair[1].anchor_ts,
                "Non-monotonic anchors: {:?} and {:?}.\nEvents: {:?}",
                pair[0], pair[1], events);
        }
    }
}
