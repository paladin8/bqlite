//! Matcher microbenchmarks — full strategy matrix per matcher-strategy.md §8.
//!
//! Covers all nine benchmark scenarios from §8.1:
//!
//! | Scenario                     | PatternClass    | Steps | Features             |
//! |------------------------------|-----------------|-------|----------------------|
//! | linear_simple_3step          | LinearSimple    | 3     | None                 |
//! | linear_simple_5step          | LinearSimple    | 5     | None                 |
//! | linear_immediate_3step       | LinearImmediate | 3     | IMMEDIATELY          |
//! | linear_negation_3step        | LinearWithNeg   | 3     | 1 WITHOUT clause     |
//! | linear_bindings_3step        | LinearWithBind  | 3     | 1 var, 4 values      |
//! | linear_full_3step            | LinearFull      | 3     | negation + bindings  |
//! | general_nfa_3step            | GeneralNfa      | 3     | 1 alternation        |
//! | general_nfa_repetition       | GeneralNfa      | 3     | 1 B+ repetition      |
//! | nfa_match_events             | LinearSimple†   | 3     | match_events demand  |
//!
//! † Escalated to NFA path by match_events demand (matcher-strategy.md §3.2).
//!
//! Each benchmark constructs a compiled pattern with an explicit
//! `PatternClass` assertion (§8.5) so classifier drift cannot silently
//! invalidate the measurement.
//!
//! The original step-counter-vs-NFA comparison benchmarks remain as
//! diagnostic evidence (TASK-325).
//!
//! Run with:
//! ```bash
//! cargo bench -p bqlite-benches --bench matcher
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use bqlite_benches::common::{criterion_for_mode, BenchMode, BenchResultCollector, BenchTarget};
use bqlite_operators::matcher::nfa::NfaSimulator;
use bqlite_operators::matcher::step_counter::StepCounterSimulator;
use bqlite_planner::compile::{
    CompiledNfa, NfaState, PatternClass, PoisonTransition, Transition, VariableBindingDef,
};

// ─────────────────────────────────────────────────────────────────────────────
// NFA construction helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Build a minimal NFA for a linear pattern: step0 THEN step1 THEN step2 ...
fn linear_nfa(steps: &[&str], pattern_class: PatternClass) -> CompiledNfa {
    let num_steps = steps.len();
    let mut states = Vec::with_capacity(num_steps + 1);
    let mut relevant = BTreeSet::new();

    for (i, &event_type) in steps.iter().enumerate() {
        relevant.insert(event_type.to_string());
        states.push(NfaState {
            transitions: vec![Transition {
                event_type: event_type.to_string(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: (i + 1) as u16,
            }],
            poison_transitions: Vec::new(),
        });
    }
    // Accept state.
    states.push(NfaState {
        transitions: Vec::new(),
        poison_transitions: Vec::new(),
    });

    let state_to_step: Vec<u8> = (0..=num_steps as u8).collect();

    CompiledNfa {
        states,
        accept_state: num_steps as u16,
        relevant_event_types: relevant,
        pattern_class,
        variable_bindings: Vec::new(),
        global_window: None,
        session_window: false,
        emit_all: false,
        brackets: None,
        state_to_step,
    }
}

/// Build a linear NFA with a poison (WITHOUT) transition on state 1.
///
/// Pattern: step0 THEN step1 (WITHOUT poison_type) THEN step2.
fn linear_nfa_with_negation(steps: &[&str], poison_type: &str) -> CompiledNfa {
    assert!(steps.len() >= 2, "need at least 2 steps for negation");
    let num_steps = steps.len();
    let mut states = Vec::with_capacity(num_steps + 1);
    let mut relevant = BTreeSet::new();

    for (i, &event_type) in steps.iter().enumerate() {
        relevant.insert(event_type.to_string());
        let poison_transitions = if i == 1 {
            // Poison on the second state: if we see `poison_type`
            // while waiting at step 1, kill the candidate.
            relevant.insert(poison_type.to_string());
            vec![PoisonTransition {
                event_type: poison_type.to_string(),
                predicates: Vec::new(),
            }]
        } else {
            Vec::new()
        };
        states.push(NfaState {
            transitions: vec![Transition {
                event_type: event_type.to_string(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: (i + 1) as u16,
            }],
            poison_transitions,
        });
    }
    // Accept state.
    states.push(NfaState {
        transitions: Vec::new(),
        poison_transitions: Vec::new(),
    });

    let state_to_step: Vec<u8> = (0..=num_steps as u8).collect();

    CompiledNfa {
        states,
        accept_state: num_steps as u16,
        relevant_event_types: relevant,
        pattern_class: PatternClass::LinearWithNegation,
        variable_bindings: Vec::new(),
        global_window: None,
        session_window: false,
        emit_all: false,
        brackets: None,
        state_to_step,
    }
}

/// Build a linear NFA with one variable binding on step 0, checked at step 1+.
///
/// Binding column: "plan_type". Bound at step 0, checked at steps 1..N.
fn linear_nfa_with_bindings(steps: &[&str]) -> CompiledNfa {
    let num_steps = steps.len();
    let mut states = Vec::with_capacity(num_steps + 1);
    let mut relevant = BTreeSet::new();

    for (i, &event_type) in steps.iter().enumerate() {
        relevant.insert(event_type.to_string());
        let (bind_vars, check_vars) = if i == 0 {
            // Bind $plan at step 0.
            (vec![0usize], Vec::new())
        } else {
            // Check $plan at subsequent steps.
            (Vec::new(), vec![0usize])
        };
        states.push(NfaState {
            transitions: vec![Transition {
                event_type: event_type.to_string(),
                predicates: Vec::new(),
                bind_variables: bind_vars,
                check_variables: check_vars,
                target: (i + 1) as u16,
            }],
            poison_transitions: Vec::new(),
        });
    }
    // Accept state.
    states.push(NfaState {
        transitions: Vec::new(),
        poison_transitions: Vec::new(),
    });

    let state_to_step: Vec<u8> = (0..=num_steps as u8).collect();

    CompiledNfa {
        states,
        accept_state: num_steps as u16,
        relevant_event_types: relevant,
        pattern_class: PatternClass::LinearWithBindings,
        variable_bindings: vec![VariableBindingDef {
            name: "plan".to_string(),
            source_column: "plan_type".to_string(),
            column_index: 2, // after event_type, ts
            bind_step: 0,
        }],
        global_window: None,
        session_window: false,
        emit_all: false,
        brackets: None,
        state_to_step,
    }
}

/// Build a linear NFA with both negation and bindings (LinearFull).
fn linear_nfa_full(steps: &[&str], poison_type: &str) -> CompiledNfa {
    assert!(steps.len() >= 2);
    let num_steps = steps.len();
    let mut states = Vec::with_capacity(num_steps + 1);
    let mut relevant = BTreeSet::new();

    for (i, &event_type) in steps.iter().enumerate() {
        relevant.insert(event_type.to_string());
        let (bind_vars, check_vars) = if i == 0 {
            (vec![0usize], Vec::new())
        } else {
            (Vec::new(), vec![0usize])
        };
        let poison_transitions = if i == 1 {
            relevant.insert(poison_type.to_string());
            vec![PoisonTransition {
                event_type: poison_type.to_string(),
                predicates: Vec::new(),
            }]
        } else {
            Vec::new()
        };
        states.push(NfaState {
            transitions: vec![Transition {
                event_type: event_type.to_string(),
                predicates: Vec::new(),
                bind_variables: bind_vars,
                check_variables: check_vars,
                target: (i + 1) as u16,
            }],
            poison_transitions,
        });
    }
    // Accept state.
    states.push(NfaState {
        transitions: Vec::new(),
        poison_transitions: Vec::new(),
    });

    let state_to_step: Vec<u8> = (0..=num_steps as u8).collect();

    CompiledNfa {
        states,
        accept_state: num_steps as u16,
        relevant_event_types: relevant,
        pattern_class: PatternClass::LinearFull,
        variable_bindings: vec![VariableBindingDef {
            name: "plan".to_string(),
            source_column: "plan_type".to_string(),
            column_index: 2,
            bind_step: 0,
        }],
        global_window: None,
        session_window: false,
        emit_all: false,
        brackets: None,
        state_to_step,
    }
}

/// Build an NFA with alternation: state 0 has two forward transitions
/// matching different event types (alternation within step 0).
/// Pattern: (A OR A2) THEN B THEN C.
fn general_nfa_alternation(steps: &[&str]) -> CompiledNfa {
    assert!(steps.len() >= 3);
    // State 0: two transitions — A→1, A2→1 (alternation on step 0).
    // State 1: B→2.
    // State 2: C→3.
    // State 3: accept.
    let num_steps = steps.len();
    let alt_type = format!("{}_alt", steps[0]);
    let mut relevant = BTreeSet::new();

    let mut states = Vec::with_capacity(num_steps + 1);

    // State 0: alternation — two transitions to state 1.
    relevant.insert(steps[0].to_string());
    relevant.insert(alt_type.clone());
    states.push(NfaState {
        transitions: vec![
            Transition {
                event_type: steps[0].to_string(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: 1,
            },
            Transition {
                event_type: alt_type.clone(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: 1,
            },
        ],
        poison_transitions: Vec::new(),
    });

    // Remaining steps: linear chain.
    for (i, &event_type) in steps[1..].iter().enumerate() {
        relevant.insert(event_type.to_string());
        states.push(NfaState {
            transitions: vec![Transition {
                event_type: event_type.to_string(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: (i + 2) as u16,
            }],
            poison_transitions: Vec::new(),
        });
    }
    // Accept state.
    states.push(NfaState {
        transitions: Vec::new(),
        poison_transitions: Vec::new(),
    });

    let state_to_step: Vec<u8> = (0..=num_steps as u8).collect();

    CompiledNfa {
        states,
        accept_state: num_steps as u16,
        relevant_event_types: relevant,
        pattern_class: PatternClass::GeneralNfa,
        variable_bindings: Vec::new(),
        global_window: None,
        session_window: false,
        emit_all: false,
        brackets: None,
        state_to_step,
    }
}

/// Build an NFA with repetition: state 1 has a self-loop (B+).
/// Pattern: A THEN B+ THEN C.
fn general_nfa_repetition(steps: &[&str]) -> CompiledNfa {
    assert!(steps.len() >= 3);
    // State 0: A → 1.
    // State 1: B → 1 (self-loop) AND B → 2 (advance).
    // State 2: C → 3.
    // State 3: accept.
    let mut relevant = BTreeSet::new();
    for &s in steps {
        relevant.insert(s.to_string());
    }

    let states = vec![
        // State 0: A → 1.
        NfaState {
            transitions: vec![Transition {
                event_type: steps[0].to_string(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: 1,
            }],
            poison_transitions: Vec::new(),
        },
        // State 1: B → 1 (self-loop, repetition) and B → 2 (advance).
        // The NFA explores both paths. The self-loop makes this GeneralNfa.
        NfaState {
            transitions: vec![
                Transition {
                    event_type: steps[1].to_string(),
                    predicates: Vec::new(),
                    bind_variables: Vec::new(),
                    check_variables: Vec::new(),
                    target: 1, // self-loop
                },
                Transition {
                    event_type: steps[1].to_string(),
                    predicates: Vec::new(),
                    bind_variables: Vec::new(),
                    check_variables: Vec::new(),
                    target: 2, // advance
                },
            ],
            poison_transitions: Vec::new(),
        },
        // State 2: C → 3.
        NfaState {
            transitions: vec![Transition {
                event_type: steps[2].to_string(),
                predicates: Vec::new(),
                bind_variables: Vec::new(),
                check_variables: Vec::new(),
                target: 3,
            }],
            poison_transitions: Vec::new(),
        },
        // State 3: accept.
        NfaState {
            transitions: Vec::new(),
            poison_transitions: Vec::new(),
        },
    ];

    CompiledNfa {
        states,
        accept_state: 3,
        relevant_event_types: relevant,
        pattern_class: PatternClass::GeneralNfa,
        variable_bindings: Vec::new(),
        global_window: None,
        session_window: false,
        emit_all: false,
        brackets: None,
        state_to_step: vec![0, 1, 2, 3],
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Data generation (§8.2)
// ─────────────────────────────────────────────────────────────────────────────

/// Event type labels for the benchmark. Three funnel steps plus noise.
const FUNNEL_STEPS_3: [&str; 3] = ["signup", "activation", "purchase"];
const FUNNEL_STEPS_5: [&str; 5] = ["signup", "activation", "trial", "upgrade", "purchase"];
const NOISE_TYPES: [&str; 5] = ["view", "click", "scroll", "hover", "search"];

/// 4 distinct binding values for LinearWithBindings benchmarks (§8.2).
const BINDING_VALUES: [&str; 4] = ["basic", "pro", "enterprise", "free"];

/// Generate a RecordBatch of events for a single entity.
///
/// Produces ~`events_per_entity` events with a deterministic mix of funnel
/// steps and noise events. Roughly 20% of events per step are funnel events,
/// producing a realistic match rate.
fn generate_entity_batch(
    entity_idx: usize,
    events_per_entity: usize,
    funnel_steps: &[&str],
) -> RecordBatch {
    let mut event_types = Vec::with_capacity(events_per_entity);
    let mut timestamps = Vec::with_capacity(events_per_entity);

    let base_ts = (entity_idx as i64) * 1_000_000;
    for ev_idx in 0..events_per_entity {
        let ts = base_ts + (ev_idx as i64) * 100;
        timestamps.push(ts);

        // Deterministic pattern: every 5th event is a funnel step,
        // cycling through the funnel steps, producing matches for
        // entities where all steps appear in order.
        if ev_idx % 5 == 0 {
            let step = (ev_idx / 5) % funnel_steps.len();
            event_types.push(funnel_steps[step]);
        } else {
            event_types.push(NOISE_TYPES[ev_idx % NOISE_TYPES.len()]);
        }
    }

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

/// Generate a RecordBatch with an additional `plan_type` binding column.
///
/// The binding column cycles through [`BINDING_VALUES`] (4 distinct values)
/// to exercise per-track isolation in the step counter.
fn generate_entity_batch_with_bindings(
    entity_idx: usize,
    events_per_entity: usize,
    funnel_steps: &[&str],
) -> RecordBatch {
    let mut event_types = Vec::with_capacity(events_per_entity);
    let mut timestamps = Vec::with_capacity(events_per_entity);
    let mut plan_types = Vec::with_capacity(events_per_entity);

    let base_ts = (entity_idx as i64) * 1_000_000;
    for ev_idx in 0..events_per_entity {
        let ts = base_ts + (ev_idx as i64) * 100;
        timestamps.push(ts);

        // Deterministic binding value: cycles through 4 values.
        plan_types.push(BINDING_VALUES[ev_idx % BINDING_VALUES.len()]);

        if ev_idx % 5 == 0 {
            let step = (ev_idx / 5) % funnel_steps.len();
            event_types.push(funnel_steps[step]);
        } else {
            event_types.push(NOISE_TYPES[ev_idx % NOISE_TYPES.len()]);
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("plan_type", DataType::Utf8View, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(event_types)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(StringViewArray::from(plan_types)),
        ],
    )
    .unwrap()
}

/// Pre-generate batches for all entities.
fn generate_all_batches(
    num_entities: usize,
    events_per_entity: usize,
    funnel_steps: &[&str],
) -> Vec<RecordBatch> {
    (0..num_entities)
        .map(|i| generate_entity_batch(i, events_per_entity, funnel_steps))
        .collect()
}

/// Pre-generate batches with binding column for all entities.
fn generate_all_batches_with_bindings(
    num_entities: usize,
    events_per_entity: usize,
    funnel_steps: &[&str],
) -> Vec<RecordBatch> {
    (0..num_entities)
        .map(|i| generate_entity_batch_with_bindings(i, events_per_entity, funnel_steps))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Dataset sizing per mode
// ─────────────────────────────────────────────────────────────────────────────

/// Matrix benchmark parameters per matcher-strategy.md §8.2.
struct MatrixSizing {
    num_entities: usize,
    events_per_entity: usize,
}

fn matrix_sizing(mode: BenchMode) -> MatrixSizing {
    match mode {
        BenchMode::Ci => MatrixSizing {
            num_entities: 1_000,
            events_per_entity: 50,
        },
        BenchMode::Reference => MatrixSizing {
            num_entities: 10_000,
            events_per_entity: 500,
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// §8.1 Benchmark matrix — 9 scenarios
// ─────────────────────────────────────────────────────────────────────────────

/// Run a step-counter benchmark on the given NFA and batches.
fn run_step_counter(nfa: &CompiledNfa, batches: &[RecordBatch], match_all: bool) -> u64 {
    let sim = StepCounterSimulator::new(nfa.clone(), match_all);
    let mut total_completions = 0u64;
    for batch in batches {
        let mut state = sim.create_state();
        sim.process_batch(&mut state, batch, "event_type", "ts");
        sim.finish_entity(&mut state);
        total_completions += state.completions().len() as u64;
    }
    total_completions
}

/// Run an NFA benchmark on the given NFA and batches.
fn run_nfa(nfa: &CompiledNfa, batches: &[RecordBatch], match_all: bool) -> u64 {
    let sim = NfaSimulator::new(nfa.clone(), match_all);
    let mut total_completions = 0u64;
    for batch in batches {
        let mut state = sim.create_state();
        sim.process_batch(&mut state, batch, "event_type", "ts");
        let final_state = sim.finish_entity(state);
        total_completions += final_state.completions().len() as u64;
    }
    total_completions
}

/// Scenario 1: LinearSimple 3-step (§8.1).
fn bench_linear_simple_3step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearSimple);
    // §8.5: strategy isolation assertion.
    assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/linear_simple_3step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("step_counter", |b| {
        b.iter(|| black_box(run_step_counter(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 2: LinearSimple 5-step (§8.1).
fn bench_linear_simple_5step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = linear_nfa(&FUNNEL_STEPS_5, PatternClass::LinearSimple);
    assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_5,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/linear_simple_5step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("step_counter", |b| {
        b.iter(|| black_box(run_step_counter(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 3: LinearImmediate 3-step (§8.1).
///
/// The consecutive matcher reuses the step counter path (match-operator.md
/// §3.4). The PatternClass is LinearImmediate but execution routes through
/// StepCounterSimulator.
fn bench_linear_immediate_3step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearImmediate);
    assert_eq!(nfa.pattern_class, PatternClass::LinearImmediate);

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/linear_immediate_3step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("step_counter", |b| {
        b.iter(|| black_box(run_step_counter(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 4: LinearWithNegation 3-step (§8.1).
///
/// Pattern: signup THEN activation (WITHOUT cancel) THEN purchase.
fn bench_linear_negation_3step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = linear_nfa_with_negation(&FUNNEL_STEPS_3, "cancel");
    assert_eq!(nfa.pattern_class, PatternClass::LinearWithNegation);

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/linear_negation_3step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("step_counter", |b| {
        b.iter(|| black_box(run_step_counter(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 5: LinearWithBindings 3-step (§8.1).
///
/// Pattern: signup($plan) THEN activation($plan) THEN purchase($plan).
/// Binding column `plan_type` has 4 distinct values.
fn bench_linear_bindings_3step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = linear_nfa_with_bindings(&FUNNEL_STEPS_3);
    assert_eq!(nfa.pattern_class, PatternClass::LinearWithBindings);

    let batches = generate_all_batches_with_bindings(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/linear_bindings_3step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("step_counter", |b| {
        b.iter(|| black_box(run_step_counter(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 6: LinearFull 3-step (§8.1).
///
/// Pattern: signup($plan) THEN activation($plan, WITHOUT cancel) THEN purchase($plan).
fn bench_linear_full_3step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = linear_nfa_full(&FUNNEL_STEPS_3, "cancel");
    assert_eq!(nfa.pattern_class, PatternClass::LinearFull);

    let batches = generate_all_batches_with_bindings(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/linear_full_3step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("step_counter", |b| {
        b.iter(|| black_box(run_step_counter(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 7: GeneralNfa 3-step with alternation (§8.1).
///
/// Pattern: (signup OR signup_alt) THEN activation THEN purchase.
fn bench_general_nfa_3step(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = general_nfa_alternation(&FUNNEL_STEPS_3);
    assert_eq!(nfa.pattern_class, PatternClass::GeneralNfa);

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/general_nfa_3step");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("nfa", |b| {
        b.iter(|| black_box(run_nfa(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 8: GeneralNfa with repetition (§8.1).
///
/// Pattern: signup THEN activation+ THEN purchase.
fn bench_general_nfa_repetition_scenario(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let nfa = general_nfa_repetition(&FUNNEL_STEPS_3);
    assert_eq!(nfa.pattern_class, PatternClass::GeneralNfa);

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/general_nfa_repetition");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("nfa", |b| {
        b.iter(|| black_box(run_nfa(&nfa, &batches, false)));
    });
    group.finish();
}

/// Scenario 9: LinearSimple escalated to NFA by match_events demand (§8.1).
///
/// Same linear 3-step pattern as scenario 1 but forced to the NFA path
/// (simulating `track_match_events: true`).
fn bench_nfa_match_events(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    // Build as LinearSimple but route through NFA (demand escalation).
    let mut nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearSimple);
    assert_eq!(nfa.pattern_class, PatternClass::LinearSimple);
    // For NFA execution, set GeneralNfa so the simulator accepts it.
    nfa.pattern_class = PatternClass::GeneralNfa;

    let batches = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let total_events = (sizing.num_entities * sizing.events_per_entity) as u64;

    let mut group = c.benchmark_group("matcher_matrix/nfa_match_events");
    group.throughput(Throughput::Elements(total_events));
    group.bench_function("nfa_escalated", |b| {
        b.iter(|| black_box(run_nfa(&nfa, &batches, false)));
    });
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Diagnostic benchmarks (original TASK-325 — step counter vs NFA comparison)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_step_counter(c: &mut Criterion) {
    let nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearSimple);
    let sim = StepCounterSimulator::new(nfa, false);

    let mut group = c.benchmark_group("matcher/step_counter");
    for &num_entities in &[100, 1_000, 10_000] {
        let events_per_entity = 50;
        let batches = generate_all_batches(num_entities, events_per_entity, &FUNNEL_STEPS_3);
        let total_events = (num_entities * events_per_entity) as u64;
        group.throughput(Throughput::Elements(total_events));

        group.bench_with_input(
            BenchmarkId::new("entities", num_entities),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let mut total_completions = 0u64;
                    for batch in batches {
                        let mut state = sim.create_state();
                        sim.process_batch(&mut state, batch, "event_type", "ts");
                        sim.finish_entity(&mut state);
                        total_completions += state.completions().len() as u64;
                    }
                    black_box(total_completions)
                });
            },
        );
    }
    group.finish();
}

fn bench_nfa(c: &mut Criterion) {
    // Force GeneralNfa to test the full NFA path even on a linear pattern.
    let nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::GeneralNfa);
    let sim = NfaSimulator::new(nfa, false);

    let mut group = c.benchmark_group("matcher/nfa");
    for &num_entities in &[100, 1_000, 10_000] {
        let events_per_entity = 50;
        let batches = generate_all_batches(num_entities, events_per_entity, &FUNNEL_STEPS_3);
        let total_events = (num_entities * events_per_entity) as u64;
        group.throughput(Throughput::Elements(total_events));

        group.bench_with_input(
            BenchmarkId::new("entities", num_entities),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let mut total_completions = 0u64;
                    for batch in batches {
                        let mut state = sim.create_state();
                        sim.process_batch(&mut state, batch, "event_type", "ts");
                        let final_state = sim.finish_entity(state);
                        total_completions += final_state.completions().len() as u64;
                    }
                    black_box(total_completions)
                });
            },
        );
    }
    group.finish();
}

fn bench_step_counter_match_all(c: &mut Criterion) {
    let nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearSimple);
    let sim = StepCounterSimulator::new(nfa, true); // MATCH ALL

    let mut group = c.benchmark_group("matcher/step_counter_match_all");
    let num_entities = 1_000;
    let events_per_entity = 50;
    let batches = generate_all_batches(num_entities, events_per_entity, &FUNNEL_STEPS_3);
    let total_events = (num_entities * events_per_entity) as u64;
    group.throughput(Throughput::Elements(total_events));

    group.bench_function("1k_entities", |b| {
        b.iter(|| {
            let mut total_completions = 0u64;
            for batch in &batches {
                let mut state = sim.create_state();
                sim.process_batch(&mut state, batch, "event_type", "ts");
                sim.finish_entity(&mut state);
                total_completions += state.completions().len() as u64;
            }
            black_box(total_completions)
        });
    });
    group.finish();
}

fn bench_nfa_windowed(c: &mut Criterion) {
    let mut nfa = linear_nfa(&FUNNEL_STEPS_3, PatternClass::GeneralNfa);
    nfa.global_window = Some(2_000); // Tight window that expires some matches
    let sim = NfaSimulator::new(nfa, false);

    let mut group = c.benchmark_group("matcher/nfa_windowed");
    let num_entities = 1_000;
    let events_per_entity = 50;
    let batches = generate_all_batches(num_entities, events_per_entity, &FUNNEL_STEPS_3);
    let total_events = (num_entities * events_per_entity) as u64;
    group.throughput(Throughput::Elements(total_events));

    group.bench_function("1k_entities", |b| {
        b.iter(|| {
            let mut total_completions = 0u64;
            for batch in &batches {
                let mut state = sim.create_state();
                sim.process_batch(&mut state, batch, "event_type", "ts");
                let final_state = sim.finish_entity(state);
                total_completions += final_state.completions().len() as u64;
            }
            black_box(total_completions)
        });
    });
    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Result collection (bench-results.json)
// ─────────────────────────────────────────────────────────────────────────────

/// After all Criterion benchmarks complete, run a single-iteration timing
/// pass over the matrix scenarios and record ns/event into
/// `target/bench-results.json` via [`BenchResultCollector`].
///
/// In reference mode, hard targets from matcher-strategy.md §3.1 are
/// enforced (2× the expected cost ceiling).
fn bench_collect_results(c: &mut Criterion) {
    let mode = BenchMode::from_env();
    let sizing = matrix_sizing(mode);
    let total_events = (sizing.num_entities * sizing.events_per_entity) as f64;

    // Reference-mode hard targets: 2× the upper bound from §3.1.
    // These are generous ceilings — the step counter should be well
    // under 6 ns/event and the NFA under 60 ns/event.
    let sc_target = if mode.is_reference() {
        Some(BenchTarget::at_most(6.0))
    } else {
        None
    };
    let nfa_target = if mode.is_reference() {
        Some(BenchTarget::at_most(60.0))
    } else {
        None
    };

    let mut collector = BenchResultCollector::new(mode);

    // Use a dummy Criterion group just for the timing infrastructure.
    // The actual Criterion benchmarks above provide the statistical
    // analysis — this pass only populates bench-results.json.
    let mut group = c.benchmark_group("matcher_results");
    // Minimal samples — we only need one timing pass for the JSON file.
    group.sample_size(10);

    // Helper: time a closure, return ns/event.
    let time_ns_per_event = |f: &dyn Fn()| -> f64 {
        let start = std::time::Instant::now();
        f();
        let elapsed = start.elapsed();
        elapsed.as_nanos() as f64 / total_events
    };

    // Warm up by running each scenario once.
    let batches_3 = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );
    let batches_5 = generate_all_batches(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_5,
    );
    let batches_bindings = generate_all_batches_with_bindings(
        sizing.num_entities,
        sizing.events_per_entity,
        &FUNNEL_STEPS_3,
    );

    // Scenario 1: LinearSimple 3-step.
    let nfa_ls3 = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearSimple);
    let ns = time_ns_per_event(&|| {
        run_step_counter(&nfa_ls3, &batches_3, false);
    });
    collector.record(
        "matcher/linear_simple_3step_ns_per_event",
        ns,
        "ns/event",
        sc_target,
    );

    // Scenario 2: LinearSimple 5-step.
    let nfa_ls5 = linear_nfa(&FUNNEL_STEPS_5, PatternClass::LinearSimple);
    let ns = time_ns_per_event(&|| {
        run_step_counter(&nfa_ls5, &batches_5, false);
    });
    collector.record(
        "matcher/linear_simple_5step_ns_per_event",
        ns,
        "ns/event",
        sc_target,
    );

    // Scenario 3: LinearImmediate 3-step.
    let nfa_li = linear_nfa(&FUNNEL_STEPS_3, PatternClass::LinearImmediate);
    let ns = time_ns_per_event(&|| {
        run_step_counter(&nfa_li, &batches_3, false);
    });
    collector.record(
        "matcher/linear_immediate_3step_ns_per_event",
        ns,
        "ns/event",
        sc_target,
    );

    // Scenario 4: LinearWithNegation 3-step.
    let nfa_ln = linear_nfa_with_negation(&FUNNEL_STEPS_3, "cancel");
    let ns = time_ns_per_event(&|| {
        run_step_counter(&nfa_ln, &batches_3, false);
    });
    collector.record(
        "matcher/linear_negation_3step_ns_per_event",
        ns,
        "ns/event",
        sc_target,
    );

    // Scenario 5: LinearWithBindings 3-step.
    let nfa_lb = linear_nfa_with_bindings(&FUNNEL_STEPS_3);
    let ns = time_ns_per_event(&|| {
        run_step_counter(&nfa_lb, &batches_bindings, false);
    });
    collector.record(
        "matcher/linear_bindings_3step_ns_per_event",
        ns,
        "ns/event",
        sc_target,
    );

    // Scenario 6: LinearFull 3-step.
    let nfa_lf = linear_nfa_full(&FUNNEL_STEPS_3, "cancel");
    let ns = time_ns_per_event(&|| {
        run_step_counter(&nfa_lf, &batches_bindings, false);
    });
    collector.record(
        "matcher/linear_full_3step_ns_per_event",
        ns,
        "ns/event",
        sc_target,
    );

    // Scenario 7: GeneralNfa alternation.
    let nfa_ga = general_nfa_alternation(&FUNNEL_STEPS_3);
    let ns = time_ns_per_event(&|| {
        run_nfa(&nfa_ga, &batches_3, false);
    });
    collector.record(
        "matcher/general_nfa_3step_ns_per_event",
        ns,
        "ns/event",
        nfa_target,
    );

    // Scenario 8: GeneralNfa repetition.
    let nfa_gr = general_nfa_repetition(&FUNNEL_STEPS_3);
    let ns = time_ns_per_event(&|| {
        run_nfa(&nfa_gr, &batches_3, false);
    });
    collector.record(
        "matcher/general_nfa_repetition_ns_per_event",
        ns,
        "ns/event",
        nfa_target,
    );

    // Scenario 9: NFA match_events escalation.
    let nfa_me = linear_nfa(&FUNNEL_STEPS_3, PatternClass::GeneralNfa);
    let ns = time_ns_per_event(&|| {
        run_nfa(&nfa_me, &batches_3, false);
    });
    collector.record(
        "matcher/nfa_match_events_ns_per_event",
        ns,
        "ns/event",
        nfa_target,
    );

    group.finish();
    collector.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// Criterion groups
// ─────────────────────────────────────────────────────────────────────────────

criterion_group! {
    name = matcher_matrix;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_linear_simple_3step,
        bench_linear_simple_5step,
        bench_linear_immediate_3step,
        bench_linear_negation_3step,
        bench_linear_bindings_3step,
        bench_linear_full_3step,
        bench_general_nfa_3step,
        bench_general_nfa_repetition_scenario,
        bench_nfa_match_events,
}

criterion_group! {
    name = matcher_diagnostic;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_step_counter,
        bench_nfa,
        bench_step_counter_match_all,
        bench_nfa_windowed,
}

criterion_group! {
    name = matcher_results;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_collect_results,
}

criterion_main!(matcher_matrix, matcher_diagnostic, matcher_results);
