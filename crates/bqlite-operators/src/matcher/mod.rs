//! Sequence matcher module — NFA runtime, step counter, variable bindings,
//! and the top-level `SequenceMatchOperator`.
//!
//! This module tree implements the per-event simulation engine that
//! evaluates MATCH pipeline stages. The compiled NFA program
//! ([`bqlite_planner::CompiledNfa`]) is produced by the pattern compiler
//! (TASK-311) and consumed here at runtime.
//!
//! ## Sub-modules
//!
//! - [`nfa`] — General-path NFA runtime simulator (TASK-304). Thompson's
//!   algorithm with candidate-deque propagation, poison transitions,
//!   global time-window enforcement, and EMIT ALL support.
//! - [`step_counter`] — Step counter fast path for linear patterns (TASK-305).
//! - [`bindings`] — Variable binding tracks and extraction (TASK-306).
//! - [`output`] — Arrow `RecordBatch` construction from match results.
//!
//! ## `SequenceMatchOperator` (TASK-321)
//!
//! The top-level `EntityOperator` implementation that ties together the
//! NFA simulator, step counter, and variable bindings into a single
//! operator driven by the engine's entity-aligned sub-batch protocol.

pub mod bindings;
pub mod nfa;
pub mod output;
pub mod step_counter;

use std::collections::{HashMap, HashSet};

use arrow::record_batch::RecordBatch;

use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema, TimeRange};
use bqlite_planner::compile::{CompiledNfa, MatchStrategy};
use bqlite_planner::demand::{CompiledFusableAggregate, DemandCapabilities};
use bqlite_planner::physical::SequenceMatchPhysical;
use bqlite_planner::{BracketSpec, PhysicalPlan};

use crate::aggregate::Accumulator;
use crate::operator::EntityOperator;

use self::bindings::{BindingValue, EntityBindingState};
use self::nfa::{MatchCompletion, NfaSimulator, PartialMatch};
use self::output::build_output_batch;
use self::step_counter::StepCounterSimulator;

// ─────────────────────────────────────────────────────────────────────────────
// SequenceMatchOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Physical `EntityOperator` for MATCH pipeline stages.
///
/// Constructed from a [`SequenceMatchPhysical`] descriptor by the engine
/// bind step (TASK-323). Dispatches between the step counter fast path
/// and the full NFA simulator based on the pattern classification and
/// downstream demand determined at plan time.
///
/// See `docs/design/operators/match-operator.md` for the full spec.
#[derive(Debug)]
pub struct SequenceMatchOperator {
    /// The execution strategy driver.
    strategy: StrategyDriver,
    /// Whether this is MATCH ALL mode (reset on match, keep scanning).
    /// Passed to the simulator at construction; retained for diagnostics
    /// and future use by the engine bind step.
    #[allow(dead_code)]
    match_all: bool,
    /// Whether EMIT ALL is enabled (emit partials at entity end).
    emit_all: bool,
    /// Number of pattern steps (= accept state index).
    num_steps: u8,
    /// Output schema for the operator's results.
    output_schema: OperatorSchema,
    /// Column names required from the input batch.
    required_column_names: Vec<String>,
    /// Number of variable bindings (0 for no-binding patterns).
    num_variables: usize,
    /// Fused aggregate descriptor (if match-aggregate fusion is active).
    ///
    /// When set, `finish_entity_into` builds intermediate match-output
    /// batches using `match_output_schema` and feeds them into the
    /// accumulator via `update_batch`.
    fused_aggregate: Option<CompiledFusableAggregate>,
    /// The original match output schema, preserved when fusion replaces
    /// `output_schema` with the aggregate schema. Used by the fused
    /// `finish_entity_into` path to build intermediate batches.
    match_output_schema: Option<OperatorSchema>,
}

/// The underlying strategy driver selected at plan time.
#[derive(Debug)]
enum StrategyDriver {
    /// Step counter fast path for linear patterns.
    StepCounter(StepCounterSimulator),
    /// Full NFA simulator for general patterns or match-detail demand.
    Nfa(NfaSimulator),
}

// ─────────────────────────────────────────────────────────────────────────────
// Construction
// ─────────────────────────────────────────────────────────────────────────────

impl SequenceMatchOperator {
    /// Construct from a physical plan descriptor.
    ///
    /// The descriptor is produced by the physical planner and carried on
    /// [`SequenceMatchPhysical`]. This constructor resolves column indices,
    /// selects the strategy, and freezes configuration.
    pub fn new(desc: &SequenceMatchPhysical) -> Self {
        let match_all = desc.match_all;
        let emit_all = desc.compiled_nfa.emit_all;
        let num_steps = desc.compiled_nfa.accept_state as u8;
        let num_variables = desc.compiled_nfa.variable_bindings.len();
        let entry_range = Self::source_entry_range(&desc.input);

        // Build required column names from the NFA's relevant event types
        // and variable bindings. At minimum we need entity_id, ts, event_type.
        let mut required_column_names = vec![
            "entity_id".to_string(),
            "ts".to_string(),
            "event_type".to_string(),
        ];
        // `WITHIN SESSION` reads the upstream SESSIONIZE column at
        // runtime; declare it required so the engine bind step does not
        // project it away. The parser/planner mutual-exclusion check
        // guarantees `session_window` only fires when SESSIONIZE is
        // upstream, so the column will always exist.
        if desc.compiled_nfa.session_window {
            required_column_names.push("session_id".to_string());
        }
        // Add columns referenced by variable bindings.
        for vb in &desc.compiled_nfa.variable_bindings {
            if !required_column_names.contains(&vb.source_column) {
                required_column_names.push(vb.source_column.clone());
            }
        }

        let strategy = Self::build_strategy(
            desc.compiled_nfa.clone(),
            desc.strategy,
            &desc.execution_config,
            match_all,
            entry_range,
        );

        // When fused, `desc.output_schema` has been replaced with the
        // aggregate schema. Build the original match output schema so
        // `finish_entity_into` can construct intermediate batches.
        let match_output_schema = if desc.fused_aggregate.is_some() {
            Some(Self::build_match_output_schema(
                emit_all,
                num_steps,
                desc.compiled_nfa.brackets.as_ref(),
            ))
        } else {
            None
        };

        // Demand-pruning sanity (TASK-529): if `BRACKETS` is set but the
        // unfused output schema does not carry the `bracket` column,
        // demand analysis stripped a column the matcher must populate.
        // Catch that mismatch here rather than producing silently empty
        // bracket rows downstream. The fused path replaces
        // `output_schema` with the aggregate schema, so we only check
        // when fusion is off.
        debug_assert!(
            desc.fused_aggregate.is_some()
                || desc.compiled_nfa.brackets.is_none()
                || desc.output_schema.column("bracket").is_some(),
            "BRACKETS set but `bracket` column pruned from match output schema",
        );

        Self {
            strategy,
            match_all,
            emit_all,
            num_steps,
            output_schema: desc.output_schema.clone(),
            required_column_names,
            num_variables,
            fused_aggregate: desc.fused_aggregate.clone(),
            match_output_schema,
        }
    }

    /// Construct from a pre-built `CompiledNfa` and configuration.
    ///
    /// Convenience constructor for tests and direct usage without a full
    /// physical plan descriptor.
    pub fn from_compiled_nfa(
        compiled_nfa: CompiledNfa,
        match_all: bool,
        output_schema: OperatorSchema,
    ) -> Self {
        let emit_all = compiled_nfa.emit_all;
        let num_steps = compiled_nfa.accept_state as u8;
        let num_variables = compiled_nfa.variable_bindings.len();
        let exec_config = bqlite_planner::compile::MatchExecutionConfig::default();

        let strategy_kind =
            bqlite_planner::compile::select_strategy(compiled_nfa.pattern_class, &exec_config);

        let mut required_column_names = vec![
            "entity_id".to_string(),
            "ts".to_string(),
            "event_type".to_string(),
        ];
        for vb in &compiled_nfa.variable_bindings {
            if !required_column_names.contains(&vb.source_column) {
                required_column_names.push(vb.source_column.clone());
            }
        }

        let strategy =
            Self::build_strategy(compiled_nfa, strategy_kind, &exec_config, match_all, None);

        Self {
            strategy,
            match_all,
            emit_all,
            num_steps,
            output_schema,
            required_column_names,
            num_variables,
            fused_aggregate: None,
            match_output_schema: None,
        }
    }

    fn build_strategy(
        compiled_nfa: CompiledNfa,
        strategy_kind: MatchStrategy,
        _exec_config: &bqlite_planner::compile::MatchExecutionConfig,
        match_all: bool,
        entry_range: Option<TimeRange>,
    ) -> StrategyDriver {
        match strategy_kind {
            MatchStrategy::StepCounter => StrategyDriver::StepCounter(
                StepCounterSimulator::new(compiled_nfa, match_all).with_entry_range(entry_range),
            ),
            MatchStrategy::ConsecutiveMatcher => {
                // ConsecutiveMatcher uses the step counter with the same
                // interface — the consecutive constraint is encoded in the
                // NFA transitions (IMMEDIATELY transitions require
                // `last_step_ts + 1 == event_ts`). For now, route through
                // the step counter.
                StrategyDriver::StepCounter(
                    StepCounterSimulator::new(compiled_nfa, match_all)
                        .with_entry_range(entry_range),
                )
            }
            MatchStrategy::FullNfa => StrategyDriver::Nfa(
                NfaSimulator::new(compiled_nfa, match_all).with_entry_range(entry_range),
            ),
        }
    }

    /// Discover the source time range that gates sequence entry.
    ///
    /// The scan may read a widened `reader_range` so already-entered
    /// sequences can finish after the source-range end, but only
    /// step-0 events inside the original scan `query_range` may start
    /// a new sequence.
    fn source_entry_range(plan: &PhysicalPlan) -> Option<TimeRange> {
        match plan {
            PhysicalPlan::Scan(scan) => scan.query_range,
            PhysicalPlan::FusedSegment(node) => Self::source_entry_range(&node.input),
            PhysicalPlan::SequenceMatch(node) => Self::source_entry_range(&node.input),
            PhysicalPlan::Aggregate(node) => Self::source_entry_range(&node.input),
            PhysicalPlan::Sort(node) => Self::source_entry_range(&node.input),
            PhysicalPlan::Distinct(node) => Self::source_entry_range(&node.input),
            _ => None,
        }
    }

    /// Borrow the brackets spec from the underlying compiled NFA, if any.
    /// Returned as `Option<&BracketSpec>` so the matcher's output layer
    /// can decide whether to expand into per-bracket rows.
    fn brackets(&self) -> Option<&BracketSpec> {
        match &self.strategy {
            StrategyDriver::StepCounter(sim) => sim.nfa().brackets.as_ref(),
            StrategyDriver::Nfa(sim) => sim.nfa().brackets.as_ref(),
        }
    }

    /// Build the minimal match output schema needed by the fused
    /// `finish_entity_into` path to construct intermediate batches.
    ///
    /// When the fusion optimizer replaces `output_schema` with the
    /// aggregate schema, we can no longer call `build_output_batch`
    /// using `self.output_schema`. This method builds the match-level
    /// schema with the columns that `build_output_batch` knows how to
    /// populate: `entity_id`, `match_duration`, `step_reached`, and the
    /// `bracket` / `bracket_end` pair when BRACKETS is active.
    fn build_match_output_schema(
        emit_all: bool,
        _num_steps: u8,
        brackets: Option<&BracketSpec>,
    ) -> OperatorSchema {
        let mut cols = vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "match_duration".into(),
                bql_type: BqlType::Int,
                nullable: true,
                default_value: None,
            },
        ];
        if emit_all {
            cols.push(ColumnDef {
                name: "step_reached".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            });
        }
        if brackets.is_some() {
            cols.push(ColumnDef {
                name: "bracket".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            });
            cols.push(ColumnDef {
                name: "bracket_end".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            });
        }
        OperatorSchema::new(cols).expect("match output schema must be valid")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-entity state
// ─────────────────────────────────────────────────────────────────────────────

/// Per-entity mutable state. Variant selected at plan time based on
/// `PatternClass` and demand (match-operator.md §4.1).
pub enum SequenceMatchState {
    /// Step counter fast path for linear patterns.
    /// Boxed because `StepCounterState` (~496 bytes) is significantly
    /// larger than the NFA variants; boxing keeps the enum at pointer
    /// size for the non-step-counter paths.
    StepCounter(Box<step_counter::StepCounterState>),
    /// Full NFA with binding tracks for general patterns or when variable
    /// bindings are present.
    NfaWithBindings(EntityBindingState),
    /// Full NFA without binding tracks (single default track).
    NfaSingle(nfa::EntityNfaState),
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityOperator implementation
// ─────────────────────────────────────────────────────────────────────────────

impl EntityOperator for SequenceMatchOperator {
    type State = SequenceMatchState;

    fn create_state(&self, _entity_id: &EntityId) -> Self::State {
        match &self.strategy {
            StrategyDriver::StepCounter(sim) => {
                SequenceMatchState::StepCounter(Box::new(sim.create_state()))
            }
            StrategyDriver::Nfa(sim) => {
                if self.num_variables > 0 {
                    SequenceMatchState::NfaWithBindings(EntityBindingState::new(
                        self.num_variables,
                        sim.nfa().states.len(),
                    ))
                } else {
                    SequenceMatchState::NfaSingle(sim.create_state())
                }
            }
        }
    }

    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn process_sub_batch(&self, state: &mut Self::State, batch: &RecordBatch) {
        match (state, &self.strategy) {
            (SequenceMatchState::StepCounter(sc_state), StrategyDriver::StepCounter(sim)) => {
                sim.process_batch(sc_state, batch, "event_type", "ts");
            }
            (SequenceMatchState::NfaSingle(nfa_state), StrategyDriver::Nfa(sim)) => {
                sim.process_batch(nfa_state, batch, "event_type", "ts");
            }
            (SequenceMatchState::NfaWithBindings(binding_state), StrategyDriver::Nfa(sim)) => {
                sim.process_batch_with_bindings(binding_state, batch, "event_type", "ts");
            }
            _ => {
                // State/strategy mismatch — programming error.
                debug_assert!(false, "state/strategy variant mismatch");
            }
        }
    }

    fn finish_entity(&self, state: Self::State) -> Option<RecordBatch> {
        let (completions, partials, _dropped_count) = self.finalize_state(state);

        if completions.is_empty() && partials.is_empty() {
            return None;
        }

        Some(build_output_batch(
            &self.output_schema,
            &completions,
            &partials,
            self.emit_all,
            self.num_steps,
            self.brackets(),
        ))
    }

    fn finish_entity_into(
        &self,
        state: Self::State,
        accumulator: &mut dyn Accumulator,
    ) -> bqlite_core::Result<()> {
        // Fused path: when the match-aggregate fusion optimizer is active,
        // `self.output_schema` has been replaced with the aggregate schema.
        // We use the saved `match_output_schema` to build an intermediate
        // match-output batch and feed it into the accumulator.
        if self.fused_aggregate.is_some() {
            if let Some(match_schema) = &self.match_output_schema {
                let (completions, partials, _dropped) = self.finalize_state(state);
                if !completions.is_empty() || (self.emit_all && !partials.is_empty()) {
                    let batch = build_output_batch(
                        match_schema,
                        &completions,
                        &partials,
                        self.emit_all,
                        self.num_steps,
                        self.brackets(),
                    );
                    accumulator.update_batch(&batch)?;
                }
            }
            return Ok(());
        }
        // Non-fused path: materialize match output batch and feed into accumulator.
        if let Some(batch) = self.finish_entity(state) {
            accumulator.update_batch(&batch)?;
        }
        Ok(())
    }

    fn required_columns(&self) -> &[String] {
        &self.required_column_names
    }

    fn supported_demands(&self) -> DemandCapabilities {
        DemandCapabilities {
            supports_step_reached: true,
            supports_match_count: true,
            supports_full_detail: true,
            supports_aggregation_fusion: true,
            supports_step_property_forwarding: true,
            supports_forwarded_columns: false,
            supports_eager_group_emit: false,
        }
    }

    fn take_pending_warnings(
        &self,
        state: &mut Self::State,
        entity_id: &EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        // Only the StepCounter strategy carries an active-state cap
        // today (sequence-matching.md §16.1). The NFA paths inherit
        // unbounded growth and will land their own cap in a later wave;
        // when they do, this branch needs to learn about their state.
        let SequenceMatchState::StepCounter(sc_state) = state else {
            return Vec::new();
        };
        if !sc_state.cap_exceeded {
            return Vec::new();
        }
        // Resolve the cap up front. A `StepCounter` state paired with a
        // non-StepCounter strategy is a programming error, but defending
        // against it here avoids leaving the latch reset without
        // emitting (which would silently swallow the warning).
        let StrategyDriver::StepCounter(sim) = &self.strategy else {
            debug_assert!(false, "StepCounter state with non-StepCounter strategy");
            return Vec::new();
        };
        let cap = sim.active_state_limit() as u64;
        sc_state.cap_exceeded = false;
        let active = sc_state.tracks.len() as u64 + sc_state.dropped_count;
        vec![bqlite_core::QueryWarning::ActiveStateLimitExceeded {
            entity_id: entity_id.to_string(),
            active_states: active,
            cap,
        }]
    }
}

impl SequenceMatchOperator {
    /// Finalize per-entity state, collecting completions and partials.
    ///
    /// Returns `(completions, partials, dropped_count)`.
    fn finalize_state(
        &self,
        state: SequenceMatchState,
    ) -> (Vec<MatchCompletion>, Vec<PartialMatch>, u64) {
        let (mut completions, mut partials, dropped) = match (state, &self.strategy) {
            (SequenceMatchState::StepCounter(mut sc_state), StrategyDriver::StepCounter(sim)) => {
                sim.finish_entity(&mut sc_state);
                let completions = sc_state.completions().to_vec();
                let partials = sc_state.partials().to_vec();
                let dropped = sc_state.dropped_count();
                (completions, partials, dropped)
            }
            (SequenceMatchState::NfaSingle(nfa_state), StrategyDriver::Nfa(_sim)) => {
                let dropped = nfa_state.dropped_count();
                let final_state = _sim.finish_entity(nfa_state);
                let completions = final_state.completions().to_vec();
                let partials = final_state.partials().to_vec();
                (completions, partials, dropped)
            }
            (SequenceMatchState::NfaWithBindings(mut binding_state), StrategyDriver::Nfa(sim)) => {
                let mut completions = Vec::new();
                let mut partials = Vec::new();
                let mut total_dropped: u64 = binding_state.dropped_track_count();

                for (key, mut completion) in binding_state.all_completions() {
                    // Populate binding values from the track's binding key.
                    completion.bindings = key.to_vec();
                    completions.push(completion);
                }

                // Collect already-emitted partials (from window expiry).
                for (key, mut partial) in binding_state.all_partials() {
                    if self.emit_all {
                        partial.bindings = key.to_vec();
                        partials.push(partial);
                    }
                }

                // Drain remaining in-progress candidates as partials
                // (EMIT ALL at entity end per match-operator.md §6.3).
                // Must finish each track's NFA state to convert active
                // candidates into partial matches.
                for track in binding_state.tracks_mut() {
                    total_dropped += track.nfa_state.dropped_count();
                    if self.emit_all {
                        // Extract binding values from this track before
                        // draining its NFA state.
                        // Use a default for unbound variables (late-bind
                        // steps not yet reached) to keep positional alignment.
                        let track_bindings: Vec<_> = track
                            .bound_values
                            .iter()
                            .map(|v| v.clone().unwrap_or(BindingValue::String("".into())))
                            .collect();
                        let final_state = sim.finish_entity(std::mem::replace(
                            &mut track.nfa_state,
                            nfa::EntityNfaState::new(0),
                        ));
                        for partial in final_state.partials() {
                            let mut p = partial.clone();
                            p.bindings = track_bindings.clone();
                            partials.push(p);
                        }
                    }
                }

                (completions, partials, total_dropped)
            }
            _ => {
                debug_assert!(false, "state/strategy variant mismatch");
                (Vec::new(), Vec::new(), 0)
            }
        };

        self.normalize_emit_all_rows(&mut completions, &mut partials);
        (completions, partials, dropped)
    }

    fn normalize_emit_all_rows(
        &self,
        completions: &mut [MatchCompletion],
        partials: &mut Vec<PartialMatch>,
    ) {
        if !self.emit_all {
            partials.clear();
            return;
        }

        if self.match_all {
            let completion_keys: HashSet<_> = completions
                .iter()
                .map(|c| (c.bindings.clone(), c.anchor_ts))
                .collect();
            let mut best_by_entry: HashMap<(Vec<BindingValue>, i64), PartialMatch> = HashMap::new();

            for partial in partials.drain(..) {
                let key = (partial.bindings.clone(), partial.anchor_ts);
                if completion_keys.contains(&key) {
                    continue;
                }
                match best_by_entry.get_mut(&key) {
                    Some(existing) if partial.step_reached > existing.step_reached => {
                        *existing = partial;
                    }
                    None => {
                        best_by_entry.insert(key, partial);
                    }
                    _ => {}
                }
            }

            let mut normalized: Vec<_> = best_by_entry.into_values().collect();
            normalized.sort_by_key(|p| (p.anchor_ts, p.step_reached));
            *partials = normalized;
            return;
        }

        // MATCH FIRST + EMIT ALL returns exactly one row per binding track:
        // a completion if one exists, otherwise the farthest partial.
        let completed_bindings: HashSet<_> =
            completions.iter().map(|c| c.bindings.clone()).collect();

        let mut best_by_binding: HashMap<Vec<BindingValue>, PartialMatch> = HashMap::new();
        for partial in partials.drain(..) {
            let key = partial.bindings.clone();
            if completed_bindings.contains(&key) {
                continue;
            }
            match best_by_binding.get_mut(&key) {
                Some(existing)
                    if partial.step_reached > existing.step_reached
                        || (partial.step_reached == existing.step_reached
                            && partial.anchor_ts < existing.anchor_ts) =>
                {
                    *existing = partial;
                }
                None => {
                    best_by_binding.insert(key, partial);
                }
                _ => {}
            }
        }

        let mut normalized: Vec<_> = best_by_binding.into_values().collect();
        normalized.sort_by_key(|p| (p.anchor_ts, p.step_reached));
        *partials = normalized;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{BqlType, ColumnDef};
    use bqlite_planner::compile::{CompiledNfa, NfaState, PatternClass, Transition};

    /// Helper: build a simple RecordBatch with event_type and ts columns.
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

    /// Build a minimal NFA for a linear pattern: A THEN B THEN C.
    fn linear_nfa(steps: &[&str]) -> CompiledNfa {
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
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: Vec::new(),
            global_window: None,
            session_window: false,
            emit_all: false,
            brackets: None,
            state_to_step,
        }
    }

    fn match_output_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "match_duration".into(),
                bql_type: BqlType::Int,
                nullable: true,
                default_value: None,
            },
        ])
        .unwrap()
    }

    fn emit_all_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "step_reached".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            },
        ])
        .unwrap()
    }

    // ── Basic match tests ────────────────────────────────────────────

    #[test]
    fn simple_match_first_completes() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(
            nfa,
            false, // MATCH FIRST
            match_output_schema(),
        );

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        let batch = make_batch(&[("signup", 100), ("view", 200), ("purchase", 300)]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);

        // match_duration = 300 - 100 = 200
        let durations = batch
            .column_by_name("match_duration")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(durations.value(0), 200);
    }

    #[test]
    fn no_match_returns_none() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        let batch = make_batch(&[
            ("signup", 100),
            ("view", 200),
            // No purchase event.
        ]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_none());
    }

    #[test]
    fn match_all_returns_multiple() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(
            nfa,
            true, // MATCH ALL
            match_output_schema(),
        );

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        let batch = make_batch(&[
            ("signup", 100),
            ("purchase", 200),
            ("signup", 300),
            ("purchase", 400),
        ]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn match_across_sub_batches() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        // First sub-batch: step 1.
        let batch1 = make_batch(&[("signup", 100)]);
        op.process_sub_batch(&mut state, &batch1);

        // Second sub-batch: step 2.
        let batch2 = make_batch(&[("purchase", 300)]);
        op.process_sub_batch(&mut state, &batch2);

        let result = op.finish_entity(state);
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);

        let durations = batch
            .column_by_name("match_duration")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(durations.value(0), 200);
    }

    #[test]
    fn emit_all_includes_partials() {
        let mut nfa = linear_nfa(&["signup", "purchase", "activate"]);
        nfa.emit_all = true;
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, emit_all_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        // Only completes 2 of 3 steps.
        let batch = make_batch(&[("signup", 100), ("purchase", 200)]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_some());
        let batch = result.unwrap();
        // Should have exactly one partial match row.
        assert_eq!(batch.num_rows(), 1);

        let steps = batch
            .column_by_name("step_reached")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // step_reached is 1-indexed: reaching step 2 of 3 → step_reached = 2.
        assert_eq!(steps.value(0), 2);
    }

    #[test]
    fn emit_all_nfa_path_keeps_only_farthest_partial() {
        let mut nfa = linear_nfa(&["signup", "purchase", "activate"]);
        nfa.pattern_class = bqlite_planner::compile::PatternClass::GeneralNfa;
        nfa.emit_all = true;
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, emit_all_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        let batch = make_batch(&[("signup", 100), ("purchase", 200)]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);

        let steps = batch
            .column_by_name("step_reached")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(steps.value(0), 2);
    }

    #[test]
    fn three_step_match_with_window() {
        let mut nfa = linear_nfa(&["signup", "activate", "purchase"]);
        nfa.global_window = Some(1000); // 1000 ns window
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let entity = EntityId::String("user1".into());

        // Within window.
        let mut state = op.create_state(&entity);
        let batch = make_batch(&[("signup", 100), ("activate", 500), ("purchase", 900)]);
        op.process_sub_batch(&mut state, &batch);
        let result = op.finish_entity(state);
        assert!(result.is_some());

        // Outside window.
        let mut state2 = op.create_state(&entity);
        let batch2 = make_batch(&[("signup", 100), ("activate", 500), ("purchase", 1200)]);
        op.process_sub_batch(&mut state2, &batch2);
        let result2 = op.finish_entity(state2);
        assert!(result2.is_none());
    }

    #[test]
    fn empty_batch_no_panic() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        let batch = make_batch(&[]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_none());
    }

    #[test]
    fn take_pending_warnings_emits_active_state_warning_for_step_counter() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        // Manually flip the StepCounter state's cap-exceeded latch and
        // dropped count to simulate a runtime cap fire — exercising the
        // operator-level conversion to QueryWarning without needing the
        // full LinearWithBindings setup that the step_counter unit test
        // already covers.
        if let SequenceMatchState::StepCounter(sc) = &mut state {
            sc.cap_exceeded = true;
            sc.dropped_count = 7;
        } else {
            panic!("LinearSimple should pick StepCounter strategy");
        }

        let warnings = op.take_pending_warnings(&mut state, &entity);
        assert_eq!(warnings.len(), 1);
        match &warnings[0] {
            bqlite_core::QueryWarning::ActiveStateLimitExceeded {
                entity_id,
                active_states,
                cap,
            } => {
                assert_eq!(entity_id, "user1");
                // active_states = tracks.len() (0 here) + dropped_count (7).
                assert_eq!(*active_states, 7);
                // Default `with_active_state_limit` is 10_000.
                assert_eq!(*cap, 10_000);
            }
            other => panic!("expected ActiveStateLimitExceeded, got {other:?}"),
        }

        // Latch reset.
        let again = op.take_pending_warnings(&mut state, &entity);
        assert!(again.is_empty());
    }

    #[test]
    fn take_pending_warnings_empty_when_cap_not_exceeded() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());
        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);
        let warnings = op.take_pending_warnings(&mut state, &entity);
        assert!(warnings.is_empty());
    }

    #[test]
    fn nfa_strategy_simple_match() {
        let mut nfa = linear_nfa(&["signup", "purchase"]);
        // Force GeneralNfa to test the NFA path.
        nfa.pattern_class = PatternClass::GeneralNfa;
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let entity = EntityId::String("user1".into());
        let mut state = op.create_state(&entity);

        let batch = make_batch(&[("signup", 100), ("purchase", 200)]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state);
        assert!(result.is_some());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn required_columns_includes_basics() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, match_output_schema());

        let cols = op.required_columns();
        assert!(cols.contains(&"entity_id".to_string()));
        assert!(cols.contains(&"ts".to_string()));
        assert!(cols.contains(&"event_type".to_string()));
    }

    #[test]
    fn output_schema_matches() {
        let nfa = linear_nfa(&["signup", "purchase"]);
        let schema = match_output_schema();
        let op = SequenceMatchOperator::from_compiled_nfa(nfa, false, schema.clone());
        assert_eq!(op.output_schema().columns().len(), schema.columns().len());
    }

    /// Build a 2-step NFA with `$plan` binding on step 1 (signup) and a
    /// check on step 2 (purchase). Mirrors `prop_bindings.rs::nfa_a_then_b_with_binding`
    /// but uses signup/purchase event types so the test reads naturally.
    fn brackets_with_binding_nfa(brackets: BracketSpec, emit_all: bool) -> CompiledNfa {
        use bqlite_planner::compile::VariableBindingDef;
        let mut relevant = BTreeSet::new();
        relevant.insert("signup".to_string());
        relevant.insert("purchase".to_string());
        CompiledNfa {
            states: vec![
                NfaState {
                    transitions: vec![Transition {
                        event_type: "signup".into(),
                        predicates: Vec::new(),
                        bind_variables: vec![0],
                        check_variables: Vec::new(),
                        target: 1,
                    }],
                    poison_transitions: Vec::new(),
                },
                NfaState {
                    transitions: vec![Transition {
                        event_type: "purchase".into(),
                        predicates: Vec::new(),
                        bind_variables: Vec::new(),
                        check_variables: vec![0],
                        target: 2,
                    }],
                    poison_transitions: Vec::new(),
                },
                NfaState {
                    transitions: Vec::new(),
                    poison_transitions: Vec::new(),
                },
            ],
            accept_state: 2,
            relevant_event_types: relevant,
            pattern_class: PatternClass::GeneralNfa,
            variable_bindings: vec![VariableBindingDef {
                name: "plan".into(),
                source_column: "plan".into(),
                column_index: 2,
                bind_step: 0,
            }],
            global_window: None,
            session_window: false,
            emit_all,
            brackets: Some(brackets),
            state_to_step: vec![0, 1, 2],
        }
    }

    fn make_batch_with_plan(events: &[(&str, i64, &str)]) -> RecordBatch {
        let event_types: Vec<&str> = events.iter().map(|(e, _, _)| *e).collect();
        let timestamps: Vec<i64> = events.iter().map(|(_, t, _)| *t).collect();
        let plans: Vec<&str> = events.iter().map(|(_, _, p)| *p).collect();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("plan", DataType::Utf8View, false),
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

    fn brackets_bindings_output_schema(emit_all: bool) -> OperatorSchema {
        let mut cols = vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "match_duration".into(),
                bql_type: BqlType::Int,
                nullable: true,
                default_value: None,
            },
            ColumnDef {
                name: "$plan".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
        ];
        if emit_all {
            cols.push(ColumnDef {
                name: "step_reached".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            });
        }
        cols.push(ColumnDef {
            name: "bracket".into(),
            bql_type: BqlType::Int,
            nullable: false,
            default_value: None,
        });
        cols.push(ColumnDef {
            name: "bracket_end".into(),
            bql_type: BqlType::Int,
            nullable: false,
            default_value: None,
        });
        OperatorSchema::new(cols).unwrap()
    }

    /// BRACKETS × variable-binding composition (TASK-529 plan §30.6 /
    /// query-language.md §4.12 + §8). Two `$plan` values for one
    /// entity, exclusive brackets, EMIT ALL: each track must produce
    /// exactly N rows, the per-bracket `step_reached` must match the
    /// completion's bracket, and the `$plan` column must carry the
    /// correct binding value on every per-bracket row.
    #[test]
    fn brackets_compose_with_variable_bindings_emit_all_exclusive() {
        let durations = vec![
            86_400_000_000_000,      // 1d
            7 * 86_400_000_000_000,  // 7d
            14 * 86_400_000_000_000, // 14d
            30 * 86_400_000_000_000, // 30d
        ];
        let nfa = brackets_with_binding_nfa(
            BracketSpec {
                durations: durations.clone(),
                cumulative: false,
                span: bqlite_ast::span::Span::EMPTY,
            },
            true,
        );
        let op = SequenceMatchOperator::from_compiled_nfa(
            nfa,
            false, // MATCH FIRST
            brackets_bindings_output_schema(true),
        );

        let entity = EntityId::String("u1".into());
        let mut state = op.create_state(&entity);

        // free track: anchor=0, purchase at delta=2d → bracket 1 `(1d, 7d]`.
        // pro  track: anchor=0, purchase at delta=20d → bracket 3 `(14d, 30d]`.
        let day = 86_400_000_000_000_i64;
        let batch = make_batch_with_plan(&[
            ("signup", 0, "free"),
            ("signup", 0, "pro"),
            ("purchase", 2 * day, "free"),
            ("purchase", 20 * day, "pro"),
        ]);
        op.process_sub_batch(&mut state, &batch);

        let result = op.finish_entity(state).expect("must produce rows");
        // 2 tracks × 4 brackets = 8 rows under EMIT ALL.
        assert_eq!(result.num_rows(), 8);

        let plan_arr = result
            .column_by_name("$plan")
            .unwrap()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let bracket_arr = result
            .column_by_name("bracket")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bracket_end_arr = result
            .column_by_name("bracket_end")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let step_arr = result
            .column_by_name("step_reached")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Group rows by `$plan` to make the assertion order-independent —
        // track iteration order is implementation-defined in the
        // matcher's HashMap, and asserting positionally would be a
        // false-positive guard. Build a map of plan → ordered-by-bracket
        // step_reached values.
        let mut by_plan: HashMap<String, Vec<(i64, i64, i64)>> = HashMap::new();
        for i in 0..result.num_rows() {
            by_plan
                .entry(plan_arr.value(i).to_string())
                .or_default()
                .push((
                    bracket_arr.value(i),
                    bracket_end_arr.value(i),
                    step_arr.value(i),
                ));
        }
        assert_eq!(by_plan.len(), 2, "expected one entry per binding track");
        for entries in by_plan.values_mut() {
            entries.sort_by_key(|(b, _, _)| *b);
        }

        // free: completion at delta=2d → bracket 1; bracket 0 carries
        // anchor (step_reached=1); brackets 2, 3 are dropouts (0).
        assert_eq!(
            by_plan["free"],
            vec![
                (0, durations[0], 1),
                (1, durations[1], 2),
                (2, durations[2], 0),
                (3, durations[3], 0),
            ],
            "free track per-bracket step_reached and bracket_end"
        );
        // pro: completion at delta=20d → bracket 3; bracket 0 = anchor (1);
        // brackets 1, 2 dropouts (0).
        assert_eq!(
            by_plan["pro"],
            vec![
                (0, durations[0], 1),
                (1, durations[1], 0),
                (2, durations[2], 0),
                (3, durations[3], 2),
            ],
            "pro track per-bracket step_reached and bracket_end"
        );
    }
}
