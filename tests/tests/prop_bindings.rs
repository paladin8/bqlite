//! Property tests for variable binding tracks (TASK-306).
//!
//! These tests validate binding-track invariants that example tests
//! cannot exhaust:
//!
//! 1. **Track identity stability** — the same binding value combination
//!    always maps to the same track, and distinct combinations map to
//!    distinct tracks.
//! 2. **NULL short-circuit** — NULL binding values prevent track
//!    creation; no match is produced for events with NULL in binding
//!    columns.
//! 3. **Active-track cap enforcement** — the number of active tracks
//!    never exceeds the configured limit.
//! 4. **Bind-once-then-check semantics** — matches produced by the
//!    binding-aware NFA agree with a brute-force reference evaluator
//!    that checks each `(entity, binding value)` pair independently.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use bqlite_operators::matcher::bindings::{BindingValue, EntityBindingState};
use bqlite_operators::matcher::nfa::{MatchCompletion, NfaSimulator};
use bqlite_planner::compile::{
    CompiledNfa, NfaState, PatternClass, Transition, VariableBindingDef,
};
use proptest::prelude::*;

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build a RecordBatch with event_type, ts, and a string "plan" column.
/// `plan` values may be None (NULL).
fn make_batch_with_plan(events: &[(&str, i64, Option<&str>)]) -> RecordBatch {
    let event_types: Vec<&str> = events.iter().map(|(e, _, _)| *e).collect();
    let timestamps: Vec<i64> = events.iter().map(|(_, t, _)| *t).collect();
    let plans: Vec<Option<&str>> = events.iter().map(|(_, _, p)| *p).collect();

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("plan", DataType::Utf8View, true),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(event_types)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringViewArray::from(plans)),
        ],
    )
    .unwrap()
}

fn binding_transition(
    event_type: &str,
    target: u16,
    bind_vars: Vec<usize>,
    check_vars: Vec<usize>,
) -> Transition {
    Transition {
        event_type: event_type.to_string(),
        predicates: Vec::new(),
        bind_variables: bind_vars,
        check_variables: check_vars,
        target,
    }
}

/// Build a 2-step NFA with binding: A(bind $plan) THEN B(check $plan)
fn nfa_a_then_b_with_binding(window: Option<i64>) -> CompiledNfa {
    CompiledNfa {
        states: vec![
            NfaState {
                transitions: vec![binding_transition("A", 1, vec![0], vec![])],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![binding_transition("B", 2, vec![], vec![0])],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ],
        accept_state: 2,
        relevant_event_types: BTreeSet::from(["A".to_string(), "B".to_string()]),
        pattern_class: PatternClass::LinearWithBindings,
        variable_bindings: vec![VariableBindingDef {
            name: "plan".to_string(),
            source_column: "plan".to_string(),
            column_index: 2,
            bind_step: 0,
        }],
        global_window: window,
        session_window: false,
        emit_all: false,
        state_to_step: vec![0, 1, 2],
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Brute-force reference evaluator for binding patterns
// ─────────────────────────────────────────────────────────────────────────────

/// Brute-force evaluator for "A(bind $plan) THEN B(check $plan)"
/// with optional time window and per-plan-value independent matching.
///
/// Returns completions keyed by binding value. Each distinct binding
/// value is an independent track: the first match (earliest anchor)
/// for each track is returned.
fn brute_force_with_binding(
    events: &[(&str, i64, Option<&str>)],
    window: Option<i64>,
) -> HashMap<String, MatchCompletion> {
    let mut results: HashMap<String, MatchCompletion> = HashMap::new();

    for (i, (et_a, ts_a, plan_a)) in events.iter().enumerate() {
        if *et_a != "A" {
            continue;
        }
        // NULL plan → no track (§6.2).
        let plan = match plan_a {
            Some(p) => *p,
            None => continue,
        };
        // Skip if this plan already has a match (MATCH FIRST per track).
        if results.contains_key(plan) {
            continue;
        }
        for (et_b, ts_b, plan_b) in &events[i + 1..] {
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
            // Binding check: B's plan must equal A's plan.
            match plan_b {
                Some(p) if *p == plan => {}
                _ => continue,
            }
            results.insert(
                plan.to_string(),
                MatchCompletion {
                    anchor_ts: *ts_a,
                    final_ts: *ts_b,
                    bindings: Vec::new(),
                },
            );
            break; // First match for this anchor+plan.
        }
    }

    results
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategies
// ─────────────────────────────────────────────────────────────────────────────

/// Generate an event type from a small alphabet.
fn arb_event_type() -> impl Strategy<Value = &'static str> {
    prop::sample::select(&["A", "B", "X"][..])
}

/// Generate a plan value (Some string or None for NULL).
fn arb_plan() -> impl Strategy<Value = Option<&'static str>> {
    prop_oneof![
        Just(None),
        Just(Some("free")),
        Just(Some("pro")),
        Just(Some("enterprise")),
    ]
}

/// Higher-cardinality plan strategy for active-track cap testing.
/// Includes 10 distinct non-NULL values so limits in the 1..=8 range
/// are meaningfully exercised.
fn arb_plan_high_cardinality() -> impl Strategy<Value = Option<&'static str>> {
    prop_oneof![
        Just(None),
        Just(Some("p0")),
        Just(Some("p1")),
        Just(Some("p2")),
        Just(Some("p3")),
        Just(Some("p4")),
        Just(Some("p5")),
        Just(Some("p6")),
        Just(Some("p7")),
        Just(Some("p8")),
        Just(Some("p9")),
    ]
}

/// Generate a sorted event stream with plan values.
fn arb_binding_event_stream(
    max_len: usize,
) -> impl Strategy<Value = Vec<(&'static str, i64, Option<&'static str>)>> {
    prop::collection::vec((arb_event_type(), 0_i64..1000_i64, arb_plan()), 0..=max_len).prop_map(
        |mut events| {
            events.sort_by_key(|&(_, ts, _)| ts);
            events
        },
    )
}

/// High-cardinality variant for track-cap tests.
fn arb_binding_event_stream_high_cardinality(
    max_len: usize,
) -> impl Strategy<Value = Vec<(&'static str, i64, Option<&'static str>)>> {
    prop::collection::vec(
        (
            arb_event_type(),
            0_i64..1000_i64,
            arb_plan_high_cardinality(),
        ),
        0..=max_len,
    )
    .prop_map(|mut events| {
        events.sort_by_key(|&(_, ts, _)| ts);
        events
    })
}

/// Generate a window size (Some(1..=500) or None).
fn arb_window() -> impl Strategy<Value = Option<i64>> {
    prop_oneof![Just(None), (1_i64..=500).prop_map(Some),]
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 1: Track identity stability
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// Same binding value always maps to the same track. Different
    /// binding values always map to different tracks.
    #[test]
    fn prop_track_identity_stability(
        events in arb_binding_event_stream(20),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b_with_binding(window);
        let sim = NfaSimulator::new(nfa, false);
        let mut binding_state = EntityBindingState::new(1, 3);

        let batch = make_batch_with_plan(&events);
        sim.process_batch_with_bindings(
            &mut binding_state, &batch, "event_type", "ts",
        );

        // Verify: tracks have distinct binding keys.
        let tracks = binding_state.tracks();
        let mut seen_keys = HashMap::new();
        for (idx, track) in tracks.iter().enumerate() {
            // Extract the key from bound values.
            let key: Vec<_> = track.bound_values.iter().collect();
            if let Some(prev_idx) = seen_keys.insert(format!("{key:?}"), idx) {
                prop_assert!(false,
                    "Tracks {} and {} have the same binding key: {:?}",
                    prev_idx, idx, key);
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 2: NULL short-circuit
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// Events with NULL in the binding column at step 0 never create
    /// a track and never produce a match.
    #[test]
    fn prop_null_short_circuit(
        // Generate a stream where ALL A events have NULL plan.
        non_a_events in prop::collection::vec(
            (prop::sample::select(&["B", "X"][..]), 0_i64..1000_i64, arb_plan()),
            0..=10,
        ),
        null_a_count in 0_usize..=5,
    ) {
        // Insert null_a_count A events with NULL plan.
        let mut events = non_a_events;
        for i in 0..null_a_count {
            events.push(("A", i as i64 * 100, None));
        }
        events.sort_by_key(|&(_, ts, _)| ts);

        let nfa = nfa_a_then_b_with_binding(None);
        let sim = NfaSimulator::new(nfa, false);
        let mut binding_state = EntityBindingState::new(1, 3);

        let batch = make_batch_with_plan(&events);
        sim.process_batch_with_bindings(
            &mut binding_state, &batch, "event_type", "ts",
        );

        // No tracks should exist since only NULL A's were present.
        prop_assert_eq!(binding_state.num_tracks(), 0,
            "Expected 0 tracks with all-NULL bindings, got {}.\nEvents: {:?}",
            binding_state.num_tracks(), events);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 3: Active-track cap enforcement
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// The number of tracks never exceeds the configured limit.
    /// Uses the high-cardinality plan strategy (10 distinct values)
    /// so limits 1..=8 are meaningfully exercised.
    #[test]
    fn prop_active_track_cap(
        events in arb_binding_event_stream_high_cardinality(30),
        limit in 1_usize..=8,
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b_with_binding(window);
        let sim = NfaSimulator::new(nfa, false);
        let mut binding_state =
            EntityBindingState::new(1, 3).with_active_track_limit(limit);

        let batch = make_batch_with_plan(&events);
        sim.process_batch_with_bindings(
            &mut binding_state, &batch, "event_type", "ts",
        );

        prop_assert!(binding_state.num_tracks() <= limit,
            "Track count {} exceeds limit {}.\nEvents: {:?}",
            binding_state.num_tracks(), limit, events);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 4: Bind-once-then-check — NFA agrees with brute-force
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// The binding-aware NFA produces exactly the same match results
    /// as the brute-force evaluator for every binding track.
    #[test]
    fn prop_binding_nfa_agrees_with_brute_force(
        events in arb_binding_event_stream(20),
        window in arb_window(),
    ) {
        let nfa = nfa_a_then_b_with_binding(window);
        let sim = NfaSimulator::new(nfa, false);
        let mut binding_state = EntityBindingState::new(1, 3);

        let batch = make_batch_with_plan(&events);
        sim.process_batch_with_bindings(
            &mut binding_state, &batch, "event_type", "ts",
        );

        let brute = brute_force_with_binding(&events, window);

        // Collect NFA completions keyed by binding value.
        let mut nfa_results: HashMap<String, MatchCompletion> = HashMap::new();
        for (key, completion) in binding_state.all_completions() {
            // Extract the plan string from the key.
            if let Some(BindingValue::String(plan)) = key.first() {
                nfa_results.entry(plan.to_string()).or_insert(completion);
            }
        }

        // Every brute-force match must appear in NFA results.
        for (plan, brute_match) in &brute {
            let nfa_match = nfa_results.get(plan);
            prop_assert!(nfa_match.is_some(),
                "Brute-force found match for plan={:?}: {:?} but NFA found none.\nEvents: {:?}\nWindow: {:?}",
                plan, brute_match, events, window);
            prop_assert_eq!(nfa_match.unwrap(), brute_match,
                "Match mismatch for plan={:?}.\nNFA: {:?}\nBrute: {:?}\nEvents: {:?}\nWindow: {:?}",
                plan, nfa_match.unwrap(), brute_match, events, window);
        }

        // Every NFA match must appear in brute-force results.
        for (plan, nfa_match) in &nfa_results {
            prop_assert!(brute.contains_key(plan),
                "NFA found match for plan={:?}: {:?} but brute-force found none (false positive).\nEvents: {:?}\nWindow: {:?}",
                plan, nfa_match, events, window);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property 5: Multiple binding values produce independent tracks
// ─────────────────────────────────────────────────────────────────────────────

proptest! {
    /// Events can participate in matches across different binding
    /// tracks simultaneously. If a B event with plan="free" matches
    /// track free, and a B event with plan="pro" matches track pro,
    /// both matches should be found independently.
    #[test]
    fn prop_independent_tracks(
        // Use a controlled event stream with exactly 2 plan values.
        ts_a1 in 0_i64..500,
        ts_a2 in 0_i64..500,
        ts_b in 0_i64..1000,
    ) {
        // Generate events: A(free), A(pro), B(free), B(pro)
        let mut events = vec![
            ("A", ts_a1, Some("free")),
            ("A", ts_a2, Some("pro")),
            ("B", ts_b, Some("free")),
            ("B", ts_b, Some("pro")),
        ];
        events.sort_by_key(|&(_, ts, _)| ts);

        let nfa = nfa_a_then_b_with_binding(None);
        let sim = NfaSimulator::new(nfa, false);
        let mut binding_state = EntityBindingState::new(1, 3);

        let batch = make_batch_with_plan(&events);
        sim.process_batch_with_bindings(
            &mut binding_state, &batch, "event_type", "ts",
        );

        let brute = brute_force_with_binding(&events, None);

        // Check each plan value independently.
        for plan in &["free", "pro"] {
            let brute_match = brute.get(*plan);
            let nfa_completions = binding_state.all_completions();
            let nfa_match = nfa_completions.iter().find(|(key, _)| {
                key.first() == Some(&BindingValue::String((*plan).into()))
            });

            match (brute_match, nfa_match) {
                (Some(b), Some((_, n))) => {
                    prop_assert_eq!(n, b,
                        "Match mismatch for plan={:?}", plan);
                }
                (None, None) => {}
                (Some(b), None) => {
                    prop_assert!(false,
                        "Brute found match for plan={:?}: {:?} but NFA found none.\nEvents: {:?}",
                        plan, b, events);
                }
                (None, Some((_, n))) => {
                    prop_assert!(false,
                        "NFA found match for plan={:?}: {:?} but brute found none.\nEvents: {:?}",
                        plan, n, events);
                }
            }
        }
    }
}
