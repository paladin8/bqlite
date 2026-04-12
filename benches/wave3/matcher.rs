//! Matcher microbenchmarks: NFA vs. step-counter fast path (TASK-325).
//!
//! Compares the two sequence-matching strategies on the same linear 3-step
//! funnel pattern (`signup THEN activation THEN purchase`) to validate
//! the TASK-302 performance expectation that the step-counter fast path
//! is significantly faster than the full NFA for linear patterns.
//!
//! Benchmark matrix:
//! - Strategy: `StepCounterSimulator` vs `NfaSimulator`
//! - Entity count: 100, 1k, 10k
//! - Events per entity: ~50 (20% match rate per step)
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

use bqlite_benches::common::{criterion_for_mode, BenchMode};
use bqlite_operators::matcher::nfa::NfaSimulator;
use bqlite_operators::matcher::step_counter::StepCounterSimulator;
use bqlite_planner::compile::{CompiledNfa, NfaState, PatternClass, Transition};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
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
        emit_all: false,
        state_to_step,
    }
}

/// Event type labels for the benchmark. Three funnel steps plus noise.
const FUNNEL_STEPS: [&str; 3] = ["signup", "activation", "purchase"];
const NOISE_TYPES: [&str; 5] = ["view", "click", "scroll", "hover", "search"];

/// Generate a RecordBatch of events for a single entity.
///
/// Produces ~`events_per_entity` events with a deterministic mix of funnel
/// steps and noise events. Roughly 20% of events per step are funnel events,
/// producing a realistic match rate.
fn generate_entity_batch(entity_idx: usize, events_per_entity: usize) -> RecordBatch {
    let mut event_types = Vec::with_capacity(events_per_entity);
    let mut timestamps = Vec::with_capacity(events_per_entity);

    let base_ts = (entity_idx as i64) * 1_000_000;
    for ev_idx in 0..events_per_entity {
        let ts = base_ts + (ev_idx as i64) * 100;
        timestamps.push(ts);

        // Deterministic pattern: every 5th event is a funnel step,
        // cycling through the 3 funnel steps, producing matches for
        // entities where all 3 steps appear in order.
        if ev_idx % 5 == 0 {
            let step = (ev_idx / 5) % FUNNEL_STEPS.len();
            event_types.push(FUNNEL_STEPS[step]);
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

/// Pre-generate batches for all entities.
fn generate_all_batches(num_entities: usize, events_per_entity: usize) -> Vec<RecordBatch> {
    (0..num_entities)
        .map(|i| generate_entity_batch(i, events_per_entity))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step counter benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_step_counter(c: &mut Criterion) {
    let nfa = linear_nfa(&FUNNEL_STEPS, PatternClass::LinearSimple);
    let sim = StepCounterSimulator::new(nfa, false);

    let mut group = c.benchmark_group("matcher/step_counter");
    for &num_entities in &[100, 1_000, 10_000] {
        let events_per_entity = 50;
        let batches = generate_all_batches(num_entities, events_per_entity);
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

// ─────────────────────────────────────────────────────────────────────────────
// NFA benchmark (same pattern, forced to NFA path)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_nfa(c: &mut Criterion) {
    // Force GeneralNfa to test the full NFA path even on a linear pattern.
    let nfa = linear_nfa(&FUNNEL_STEPS, PatternClass::GeneralNfa);
    let sim = NfaSimulator::new(nfa, false);

    let mut group = c.benchmark_group("matcher/nfa");
    for &num_entities in &[100, 1_000, 10_000] {
        let events_per_entity = 50;
        let batches = generate_all_batches(num_entities, events_per_entity);
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

// ─────────────────────────────────────────────────────────────────────────────
// MATCH ALL mode benchmark (step counter)
// ─────────────────────────────────────────────────────────────────────────────

fn bench_step_counter_match_all(c: &mut Criterion) {
    let nfa = linear_nfa(&FUNNEL_STEPS, PatternClass::LinearSimple);
    let sim = StepCounterSimulator::new(nfa, true); // MATCH ALL

    let mut group = c.benchmark_group("matcher/step_counter_match_all");
    let num_entities = 1_000;
    let events_per_entity = 50;
    let batches = generate_all_batches(num_entities, events_per_entity);
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

// ─────────────────────────────────────────────────────────────────────────────
// Time-windowed NFA benchmark
// ─────────────────────────────────────────────────────────────────────────────

fn bench_nfa_windowed(c: &mut Criterion) {
    let mut nfa = linear_nfa(&FUNNEL_STEPS, PatternClass::GeneralNfa);
    nfa.global_window = Some(2_000); // Tight window that expires some matches
    let sim = NfaSimulator::new(nfa, false);

    let mut group = c.benchmark_group("matcher/nfa_windowed");
    let num_entities = 1_000;
    let events_per_entity = 50;
    let batches = generate_all_batches(num_entities, events_per_entity);
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

criterion_group! {
    name = matcher_benches;
    config = criterion_for_mode(BenchMode::from_env());
    targets =
        bench_step_counter,
        bench_nfa,
        bench_step_counter_match_all,
        bench_nfa_windowed,
}
criterion_main!(matcher_benches);
