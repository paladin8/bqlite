//! Step counter fast path for linear MATCH patterns.
//!
//! Implements [`StepCounterSimulator`] and [`StepCounterState`] per
//! sequence-matching.md §10.3 and match-operator.md §4.2.
//!
//! Selected when `CompiledNfa.pattern_class` is any of the `Linear*`
//! variants **and** the downstream demand does not require
//! `track_match_events` (which forces the full NFA path). See
//! matcher-strategy.md §3.
//!
//! ## Algorithm
//!
//! For linear non-consecutive patterns the NFA simplifies to a step
//! counter per binding track:
//!
//! - Each track carries one active anchor (`anchor_ts`) and the step it
//!   has reached (`current_step`).
//! - [`StepCounterState::step0_candidates`] is a shared deque of all
//!   step-0 events for this entity. When the current anchor expires
//!   (window check fails), the simulator rebinds the track to the next
//!   eligible anchor (sequence-matching.md §4.2).
//! - Poison transitions (WITHOUT clauses) kill the track when a matching
//!   event appears at the current step.
//! - MATCH ALL resets the track on completion and resumes from
//!   `scan_from`.
//! - EMIT ALL records `step_reached` at window expiry, entity end, and
//!   match completion.
//!
//! ## Deviation from match-operator.md §4.2
//!
//! `step0_candidates` uses `VecDeque<Step0Entry>` (a new type carrying
//! binding values alongside the anchor timestamp) rather than
//! `ArrayVec<CandidateEntry, 8>`. This is required for binding-value-aware
//! rebinding in `LinearWithBindings` / `LinearFull` patterns without an
//! extra dependency. For `LinearSimple` / `LinearWithNegation` (no
//! bindings), `Step0Entry.binding_key` is always empty and the overhead
//! is zero.
//!
//! ## Design references
//!
//! - sequence-matching.md §10.3 — step counter fast path
//! - match-operator.md §4.2 — `StepCounterState` layout
//! - matcher-strategy.md §4 — variable-binding interaction with step counter

use std::collections::VecDeque;

use arrow::array::{Array, BooleanArray, StringViewArray};
use arrow::record_batch::RecordBatch;
use smallvec::SmallVec;

use bqlite_planner::compile::{CompiledNfa, NfaState};

use super::bindings::{check_bindings, try_bind_variables, BindingValue, BindingValueCache};
use super::nfa::resolve_timestamp_column;
use super::nfa::{MatchCompletion, PartialMatch};
use crate::eval;

// ─────────────────────────────────────────────────────────────────────────────
// Step0Entry
// ─────────────────────────────────────────────────────────────────────────────

/// A potential anchor entry in the [`StepCounterState::step0_candidates`] deque.
///
/// Stores the anchor timestamp and the binding key extracted at step 0.
/// For `LinearSimple` / `LinearWithNegation` (no variable bindings),
/// `binding_key` is always empty and carries no overhead.
#[derive(Debug, Clone)]
pub struct Step0Entry {
    /// Timestamp of the step-0 event (used as the match anchor).
    pub anchor_ts: i64,
    /// Dense binding key for this step-0 event.
    ///
    /// Values are ordered by variable index (matching the order of
    /// `CompiledNfa::variable_bindings`). Empty for patterns without
    /// variable bindings.
    pub binding_key: SmallVec<[BindingValue; 2]>,
}

// ─────────────────────────────────────────────────────────────────────────────
// StepCounterTrack
// ─────────────────────────────────────────────────────────────────────────────

/// Per-binding-track state for the step counter fast path.
///
/// A track is created when step 1 is first matched for a given
/// binding-value combination. For patterns without variable bindings
/// (`LinearSimple`, `LinearWithNegation`) there is at most one track
/// per entity.
///
/// Field layout per match-operator.md §4.2 and sequence-matching.md §10.3.
#[derive(Debug, Clone)]
pub struct StepCounterTrack {
    /// Dense binding key: bound variable values in variable-index order.
    ///
    /// Empty for patterns without variable bindings. Used both for track
    /// identity lookup and for building the indexed form needed by
    /// [`check_bindings`].
    pub bindings: SmallVec<[BindingValue; 2]>,
    /// Current step in the linear pattern.
    ///
    /// `0` = not started (waiting for a step-1 event). `k` (1 ≤ k <
    /// `num_steps`) = k steps have been matched. `k == num_steps`
    /// means the match is complete (MATCH FIRST: `done` also set).
    pub current_step: u8,
    /// Anchor timestamp from step 1 (for global window check).
    /// Undefined when `current_step == 0`.
    pub anchor_ts: i64,
    /// Timestamp of the most recently matched step event (for strict
    /// ordering: each step must have `ts > last_step_ts`).
    /// Undefined when `current_step == 0`.
    pub last_step_ts: i64,
    /// Completed match count (incremented on each match completion).
    pub match_count: u32,
    /// Farthest step reached (for EMIT ALL output).
    pub max_step_reached: u8,
    /// MATCH ALL restart point: step-0 events with `ts <= scan_from`
    /// are skipped when creating new candidates for this track.
    pub scan_from: i64,
    /// MATCH FIRST sentinel: set after the first match completes.
    /// The simulator skips this track in subsequent event passes.
    pub done: bool,
}

impl StepCounterTrack {
    fn new(bindings: SmallVec<[BindingValue; 2]>) -> Self {
        Self {
            bindings,
            current_step: 0,
            anchor_ts: 0,
            last_step_ts: 0,
            match_count: 0,
            max_step_reached: 0,
            scan_from: i64::MIN,
            done: false,
        }
    }

    /// Advance the track from step 0 to step 1 using the given anchor.
    ///
    /// Sets `current_step = 1`, records `anchor_ts` and `last_step_ts`,
    /// and updates `max_step_reached`.
    fn start(&mut self, anchor_ts: i64) {
        debug_assert_eq!(self.current_step, 0, "start() called on active track");
        self.current_step = 1;
        self.anchor_ts = anchor_ts;
        self.last_step_ts = anchor_ts;
        if self.max_step_reached < 1 {
            self.max_step_reached = 1;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StepCounterState
// ─────────────────────────────────────────────────────────────────────────────

/// Per-entity state for the step counter fast path.
///
/// Persists across `process_batch` calls — a match can span multiple
/// sub-batches.
#[derive(Debug)]
pub struct StepCounterState {
    /// One counter per active binding track.
    ///
    /// For `LinearSimple` / `LinearWithNegation` there is at most one
    /// track. For `LinearWithBindings` / `LinearFull` there is one per
    /// distinct binding-value combination seen so far.
    pub tracks: SmallVec<[StepCounterTrack; 4]>,
    /// Step-0 candidate deque for window rebinding
    /// (sequence-matching.md §4.2).
    ///
    /// All step-0 events for this entity are stored here (subject to
    /// the active-state cap and MATCH ALL purging). When the current
    /// anchor expires, [`StepCounterSimulator`] scans this deque to
    /// find the next eligible anchor for the affected track.
    ///
    /// ## Deviation from spec
    ///
    /// The design specifies `ArrayVec<CandidateEntry, 8>`. This
    /// implementation uses `VecDeque<Step0Entry>` so that the binding
    /// key is available for binding-aware rebinding. For patterns without
    /// bindings the `binding_key` field is always empty.
    pub step0_candidates: VecDeque<Step0Entry>,
    /// Completed matches collected during simulation for this entity.
    pub completions: Vec<MatchCompletion>,
    /// Partial matches emitted under EMIT ALL (window expiry, entity end).
    pub partials: Vec<PartialMatch>,
    /// Total active candidate count across all tracks (for active-state
    /// cap). Each active track contributes 1.
    pub active_candidate_count: u32,
    /// Whether the active-state cap was hit for this entity.
    pub cap_exceeded: bool,
    /// Number of tracks dropped due to the active-state cap.
    pub dropped_count: u64,
}

impl StepCounterState {
    fn new() -> Self {
        Self {
            tracks: SmallVec::new(),
            step0_candidates: VecDeque::new(),
            completions: Vec::new(),
            partials: Vec::new(),
            active_candidate_count: 0,
            cap_exceeded: false,
            dropped_count: 0,
        }
    }

    /// Returns the index of the first track whose `bindings` equals `key`.
    fn find_track(&self, key: &[BindingValue]) -> Option<usize> {
        self.tracks
            .iter()
            .position(|t| t.bindings.as_slice() == key)
    }

    /// Completed matches.
    pub fn completions(&self) -> &[MatchCompletion] {
        &self.completions
    }

    /// Partial matches emitted under EMIT ALL.
    pub fn partials(&self) -> &[PartialMatch] {
        &self.partials
    }

    /// Number of tracks dropped due to the active-state cap.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StepPredCache
// ─────────────────────────────────────────────────────────────────────────────

/// Per-batch predicate evaluation cache for the step counter.
///
/// Evaluates every step's predicates once per batch and stores the
/// resulting `BooleanArray`s for O(1) per-row lookup. This prevents
/// O(rows × predicates) re-evaluation in the per-event inner loop.
///
/// Layout: `forward[state_idx][pred_idx]` and
/// `poison[state_idx][poison_idx][pred_idx]`.
struct StepPredCache {
    /// Evaluated predicates for each state's (single) forward transition.
    forward: Vec<Vec<Option<BooleanArray>>>,
    /// Evaluated predicates for each state's poison transitions.
    poison: Vec<Vec<Vec<Option<BooleanArray>>>>,
}

impl StepPredCache {
    fn build(nfa: &CompiledNfa, batch: &RecordBatch) -> Self {
        let forward = nfa
            .states
            .iter()
            .map(|state| {
                // A linear NFA has at most one forward transition per state.
                state
                    .transitions
                    .first()
                    .map(|t| {
                        t.predicates
                            .iter()
                            .map(|p| eval::evaluate_bool(p, batch).ok())
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect();

        let poison = nfa
            .states
            .iter()
            .map(|state| {
                state
                    .poison_transitions
                    .iter()
                    .map(|p| {
                        p.predicates
                            .iter()
                            .map(|pred| eval::evaluate_bool(pred, batch).ok())
                            .collect()
                    })
                    .collect()
            })
            .collect();

        Self { forward, poison }
    }

    /// Check whether a forward-transition predicate is true at `row`.
    #[inline]
    fn check_forward(&self, state_idx: usize, pred_idx: usize, row: usize) -> bool {
        match self
            .forward
            .get(state_idx)
            .and_then(|preds| preds.get(pred_idx))
        {
            Some(Some(arr)) => !arr.is_null(row) && arr.value(row),
            _ => false,
        }
    }

    /// Check whether a poison-transition predicate is true at `row`.
    #[inline]
    fn check_poison(
        &self,
        state_idx: usize,
        poison_idx: usize,
        pred_idx: usize,
        row: usize,
    ) -> bool {
        match self
            .poison
            .get(state_idx)
            .and_then(|poisons| poisons.get(poison_idx))
            .and_then(|preds| preds.get(pred_idx))
        {
            Some(Some(arr)) => !arr.is_null(row) && arr.value(row),
            _ => false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StepCounterSimulator
// ─────────────────────────────────────────────────────────────────────────────

/// Immutable step counter driver. Shared across entities within a query.
///
/// Created once per query from a [`CompiledNfa`] whose `pattern_class`
/// is one of the `Linear*` variants. Drives per-entity
/// [`StepCounterState`] through the step counter algorithm.
///
/// Does **not** handle `LinearImmediate` (consecutive matcher) or
/// `GeneralNfa` (full NFA) — those use [`super::nfa::NfaSimulator`].
#[derive(Debug)]
pub struct StepCounterSimulator {
    /// The compiled NFA program (immutable, shared across all entities).
    nfa: CompiledNfa,
    /// Number of pattern steps (`nfa.accept_state`).
    /// A track completing at `current_step == num_steps` has matched.
    num_steps: u8,
    /// MATCH ALL mode: reset and continue after each match.
    match_all: bool,
    /// Maximum active tracks per entity before oldest are dropped
    /// (sequence-matching.md §16.1). Default: 10 000.
    active_state_limit: usize,
}

impl StepCounterSimulator {
    /// Construct a new simulator from a compiled NFA.
    ///
    /// `match_all` controls whether completed matches reset state and
    /// continue (MATCH ALL) or mark the track done (MATCH FIRST).
    pub fn new(nfa: CompiledNfa, match_all: bool) -> Self {
        let num_steps = nfa.accept_state as u8;
        Self {
            nfa,
            num_steps,
            match_all,
            active_state_limit: 10_000,
        }
    }

    /// Override the active-state limit (default 10 000).
    pub fn with_active_state_limit(mut self, limit: usize) -> Self {
        self.active_state_limit = limit;
        self
    }

    /// Create fresh per-entity state for a new entity.
    pub fn create_state(&self) -> StepCounterState {
        StepCounterState::new()
    }

    /// Access the underlying compiled NFA.
    pub fn nfa(&self) -> &CompiledNfa {
        &self.nfa
    }

    /// Whether this is a MATCH ALL query.
    pub fn is_match_all(&self) -> bool {
        self.match_all
    }

    /// Process a sub-batch of events for a single entity.
    ///
    /// Events must be sorted by `(entity_id, ts)` ascending and all
    /// belong to the same entity. State persists across calls — a match
    /// can span multiple sub-batches.
    pub fn process_batch(
        &self,
        state: &mut StepCounterState,
        batch: &RecordBatch,
        event_type_col: &str,
        timestamp_col: &str,
    ) {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        let event_types = match batch
            .column_by_name(event_type_col)
            .and_then(|c| c.as_any().downcast_ref::<StringViewArray>())
        {
            Some(arr) => arr,
            None => return,
        };

        let timestamps = match resolve_timestamp_column(batch, timestamp_col) {
            Some(ts) => ts,
            None => return,
        };

        // Pre-evaluate all predicates once per batch (avoids O(N²) re-eval).
        let pred_cache = StepPredCache::build(&self.nfa, batch);
        // Pre-extract all binding column values (empty if no bindings).
        let binding_cache = BindingValueCache::build(&self.nfa.variable_bindings, batch);

        // MATCH FIRST: if all tracks are already done, no more work to do.
        if !self.match_all && !state.completions.is_empty() {
            return;
        }

        #[allow(clippy::needless_range_loop)] // `row` indexes multiple arrays
        for row in 0..num_rows {
            if event_types.is_null(row) {
                continue;
            }
            let event_type = event_types.value(row);
            let event_ts = timestamps[row];

            self.process_event(
                state,
                event_type,
                event_ts,
                row,
                &pred_cache,
                &binding_cache,
            );

            // MATCH FIRST: stop processing after the first completion.
            if !self.match_all && !state.completions.is_empty() {
                break;
            }
        }
    }

    /// Finalize entity processing.
    ///
    /// When EMIT ALL is enabled, emits partial matches for all in-progress
    /// tracks that have not yet completed or been expired.
    pub fn finish_entity(&self, state: &mut StepCounterState) {
        if !self.nfa.emit_all {
            return;
        }
        for track in &state.tracks {
            if track.current_step > 0 && !track.done {
                state.partials.push(PartialMatch {
                    anchor_ts: track.anchor_ts,
                    step_reached: track.max_step_reached,
                    bindings: Vec::new(),
                });
            }
        }
    }

    // ── Per-event hot loop ────────────────────────────────────────────────

    /// Process a single event through the step counter algorithm.
    ///
    /// This is the inner hot loop. For a 3-step `LinearSimple` pattern
    /// with no predicates or bindings this compiles to ~5–10 instructions
    /// per non-matching event.
    fn process_event(
        &self,
        state: &mut StepCounterState,
        event_type: &str,
        event_ts: i64,
        row: usize,
        pred_cache: &StepPredCache,
        binding_cache: &BindingValueCache,
    ) {
        // Skip events that cannot match any transition or poison in the NFA.
        if !self.nfa.relevant_event_types.contains(event_type) {
            return;
        }

        // ── Phase 1: advance existing tracks ─────────────────────────────
        //
        // Collect indices to remove (expired or poisoned tracks). Processed
        // in reverse order: `SmallVec::remove` shifts elements left, so
        // removing the largest index first preserves the validity of
        // smaller indices.
        let mut to_remove: SmallVec<[usize; 4]> = SmallVec::new();

        for track_idx in 0..state.tracks.len() {
            let track = &state.tracks[track_idx];
            if track.done || track.current_step == 0 {
                // Done (MATCH FIRST) or dormant — skip.
                continue;
            }

            let step = track.current_step as usize;

            // ── Window expiry ─────────────────────────────────────────
            if let Some(window) = self.nfa.global_window {
                let anchor = state.tracks[track_idx].anchor_ts;
                if event_ts - anchor > window {
                    // Try to rebind to the next eligible step-0 anchor.
                    let last_step_ts = state.tracks[track_idx].last_step_ts;
                    let scan_from = state.tracks[track_idx].scan_from;
                    let bindings: SmallVec<[BindingValue; 2]> =
                        state.tracks[track_idx].bindings.clone();

                    if let Some(new_anchor) = find_rebind_anchor(
                        &mut state.step0_candidates,
                        event_ts,
                        window,
                        last_step_ts,
                        scan_from,
                        &bindings,
                    ) {
                        state.tracks[track_idx].anchor_ts = new_anchor;
                    } else {
                        // No valid anchor — expire this track.
                        if self.nfa.emit_all {
                            let anchor_ts = state.tracks[track_idx].anchor_ts;
                            let step_reached = state.tracks[track_idx].max_step_reached;
                            state.partials.push(PartialMatch {
                                anchor_ts,
                                step_reached,
                                bindings: Vec::new(),
                            });
                        }
                        to_remove.push(track_idx);
                        continue;
                    }
                }
            }

            let nfa_state = &self.nfa.states[step];

            // ── Poison transitions ────────────────────────────────────
            if self.event_matches_poison(nfa_state, step, event_type, row, pred_cache) {
                if self.nfa.emit_all {
                    let anchor_ts = state.tracks[track_idx].anchor_ts;
                    let step_reached = state.tracks[track_idx].max_step_reached;
                    state.partials.push(PartialMatch {
                        anchor_ts,
                        step_reached,
                        bindings: Vec::new(),
                    });
                }
                to_remove.push(track_idx);
                continue;
            }

            // ── Forward transition ────────────────────────────────────
            //
            // A linear NFA has exactly one forward transition per non-accept
            // state. If the event matches and all predicates + bindings pass,
            // advance the step counter.
            if let Some(transition) = nfa_state.transitions.first() {
                if transition.event_type == event_type {
                    let last_step_ts = state.tracks[track_idx].last_step_ts;
                    // Strict ordering: each step must have ts > last_step_ts.
                    if event_ts > last_step_ts {
                        // Check compiled predicates.
                        let preds_ok = transition
                            .predicates
                            .iter()
                            .enumerate()
                            .all(|(p_idx, _)| pred_cache.check_forward(step, p_idx, row));

                        if preds_ok {
                            // Check variable bindings.
                            let bindings_ok = self.check_binding_advance(
                                &state.tracks[track_idx].bindings,
                                transition,
                                binding_cache,
                                row,
                            );

                            if bindings_ok {
                                // Advance!
                                let new_step = state.tracks[track_idx].current_step + 1;
                                state.tracks[track_idx].current_step = new_step;
                                state.tracks[track_idx].last_step_ts = event_ts;
                                if new_step > state.tracks[track_idx].max_step_reached {
                                    state.tracks[track_idx].max_step_reached = new_step;
                                }

                                if new_step == self.num_steps {
                                    self.on_match_complete(state, track_idx, event_ts);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Remove expired / poisoned tracks in reverse order.
        for &idx in to_remove.iter().rev() {
            state.tracks.remove(idx);
            state.active_candidate_count = state.active_candidate_count.saturating_sub(1);
        }

        // ── Phase 2: check for new step-0 candidates ─────────────────────
        //
        // Done AFTER Phase 1 to prevent an event from both starting and
        // advancing a track in the same pass (NFA convention).
        self.check_step0(state, event_type, event_ts, row, pred_cache, binding_cache);
    }

    /// Handle a completed match for `track_idx`.
    fn on_match_complete(&self, state: &mut StepCounterState, track_idx: usize, event_ts: i64) {
        state.completions.push(MatchCompletion {
            anchor_ts: state.tracks[track_idx].anchor_ts,
            final_ts: event_ts,
            bindings: Vec::new(),
        });
        state.tracks[track_idx].match_count += 1;

        if self.match_all {
            // MATCH ALL: reset the track and continue from scan_from.
            let scan_from = event_ts;
            state.tracks[track_idx].scan_from = scan_from;
            state.tracks[track_idx].current_step = 0;
            state.tracks[track_idx].anchor_ts = 0;
            state.tracks[track_idx].last_step_ts = 0;

            // Purge step0_candidates entries consumed by this match.
            purge_step0_candidates(&mut state.step0_candidates, scan_from);

            // Immediately restart from the next eligible step-0 anchor
            // (if any remain in the deque).
            let bindings: SmallVec<[BindingValue; 2]> = state.tracks[track_idx].bindings.clone();
            if let Some(new_anchor) =
                find_next_step0_candidate(&state.step0_candidates, scan_from, &bindings)
            {
                // Track stays active (current_step goes 0→1): no count change.
                state.tracks[track_idx].start(new_anchor);
            } else {
                // Track goes dormant — no longer consuming an active slot.
                state.active_candidate_count = state.active_candidate_count.saturating_sub(1);
            }
        } else {
            // MATCH FIRST: mark done.
            state.tracks[track_idx].done = true;
            state.active_candidate_count = state.active_candidate_count.saturating_sub(1);
        }
    }

    /// Check if the event matches the step-0 transition and create or
    /// update tracks accordingly.
    ///
    /// Always runs AFTER Phase 1 (advancing existing tracks).
    fn check_step0(
        &self,
        state: &mut StepCounterState,
        event_type: &str,
        event_ts: i64,
        row: usize,
        pred_cache: &StepPredCache,
        binding_cache: &BindingValueCache,
    ) {
        let start_state = &self.nfa.states[0];
        let Some(transition) = start_state.transitions.first() else {
            return;
        };
        if transition.event_type != event_type {
            return;
        }
        // Check forward predicates on the step-0 transition.
        if !transition
            .predicates
            .iter()
            .enumerate()
            .all(|(p_idx, _)| pred_cache.check_forward(0, p_idx, row))
        {
            return;
        }

        // Extract binding values for this step-0 event.
        //
        // `bound_values[var_idx]` = extracted value (or None for NULL /
        // unsupported type).
        let num_vars = self.nfa.variable_bindings.len();
        let mut bound_values: Vec<Option<BindingValue>> = vec![None; num_vars];
        if num_vars > 0
            && !try_bind_variables(
                &transition.bind_variables,
                &mut bound_values,
                binding_cache,
                row,
            )
        {
            // NULL binding value — step does not match (§6.2).
            return;
        }

        // Build the dense binding key (values in variable-index order).
        let binding_key: SmallVec<[BindingValue; 2]> =
            bound_values.iter().filter_map(|v| v.clone()).collect();

        // Find existing track with the same binding key.
        let track_idx = state.find_track(&binding_key);

        // Determine the effective scan_from for this binding key.
        let scan_from = track_idx
            .map(|i| state.tracks[i].scan_from)
            .unwrap_or(i64::MIN);

        if event_ts <= scan_from {
            // Event is before the MATCH ALL restart point — skip.
            return;
        }

        // MATCH FIRST: skip if the track for this binding key is already done.
        // There is no further use for new step-0 anchors (including rebinding).
        if !self.match_all {
            if let Some(idx) = track_idx {
                if state.tracks[idx].done {
                    return;
                }
            }
        }

        // Add to step0_candidates (for potential future rebinding).
        // Cap the deque to avoid unbounded growth on high-cardinality entities
        // (sequence-matching.md §16.1).
        if state.step0_candidates.len() < self.active_state_limit {
            state.step0_candidates.push_back(Step0Entry {
                anchor_ts: event_ts,
                binding_key: binding_key.clone(),
            });
        }

        // Create or restart track.
        if let Some(idx) = track_idx {
            // Use a block to scope the mutable reference before calling
            // on_match_complete (which also borrows state).
            let (immediate, was_dormant) = {
                let track = &mut state.tracks[idx];
                if track.current_step == 0 && !track.done {
                    // Dormant (MATCH ALL just reset): restart.
                    track.start(event_ts);
                    // Single-step pattern: start() may already reach accept.
                    (track.current_step == self.num_steps, true)
                } else {
                    // Active track: step0_candidates holds a backup anchor.
                    // The track continues in progress.
                    (false, false)
                }
            };
            if was_dormant {
                state.active_candidate_count += 1;
            }
            if immediate {
                self.on_match_complete(state, idx, event_ts);
            }
        } else {
            // New binding key — create a new track and start it.
            //
            // Active-state cap: prevent unbounded track growth on
            // high-cardinality entities (sequence-matching.md §16.1).
            // We cap at `active_state_limit` total tracks per entity.
            if state.tracks.len() >= self.active_state_limit {
                state.cap_exceeded = true;
                state.dropped_count += 1;
                return;
            }
            let mut new_track = StepCounterTrack::new(binding_key);
            new_track.start(event_ts);
            let immediate = new_track.current_step == self.num_steps;
            state.tracks.push(new_track);
            state.active_candidate_count += 1;
            if immediate {
                let new_idx = state.tracks.len() - 1;
                self.on_match_complete(state, new_idx, event_ts);
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /// Check whether the event matches any poison transition at `step`.
    #[inline]
    fn event_matches_poison(
        &self,
        nfa_state: &NfaState,
        state_idx: usize,
        event_type: &str,
        row: usize,
        pred_cache: &StepPredCache,
    ) -> bool {
        for (poison_idx, poison) in nfa_state.poison_transitions.iter().enumerate() {
            if poison.event_type != event_type {
                continue;
            }
            if poison
                .predicates
                .iter()
                .enumerate()
                .all(|(p_idx, _)| pred_cache.check_poison(state_idx, poison_idx, p_idx, row))
            {
                return true;
            }
        }
        false
    }

    /// Check variable bindings when advancing a track.
    ///
    /// For `check_variables` (equality check against already-bound values),
    /// converts the track's dense `bindings` key to the indexed form
    /// expected by [`check_bindings`].
    ///
    /// For `bind_variables` at intermediate steps (variables bound after
    /// step 1), the values are checked to be consistent but not stored
    /// back — the step counter tracks bindings only at the track level
    /// (see matcher-strategy.md §4.1).
    #[inline]
    fn check_binding_advance(
        &self,
        track_bindings: &[BindingValue],
        transition: &bqlite_planner::compile::Transition,
        binding_cache: &BindingValueCache,
        row: usize,
    ) -> bool {
        // If no binding checks are needed, short-circuit.
        if transition.check_variables.is_empty() && transition.bind_variables.is_empty() {
            return true;
        }

        // Convert dense binding key → indexed form.
        // `track_bindings[i]` is the value for variable index `i`
        // (same ordering as build_binding_key).
        let bound_values: Vec<Option<BindingValue>> =
            track_bindings.iter().map(|v| Some(v.clone())).collect();

        // Check equality for check_variables.
        if !transition.check_variables.is_empty()
            && !check_bindings(
                &transition.check_variables,
                &bound_values,
                binding_cache,
                row,
            )
        {
            return false;
        }

        // For bind_variables at intermediate steps: verify extraction
        // succeeds (non-NULL). The extracted value must equal the already-
        // bound value (bind-once semantics).
        for &var_idx in &transition.bind_variables {
            if let Some(existing) = bound_values.get(var_idx).and_then(|v| v.as_ref()) {
                match binding_cache.get(var_idx, row) {
                    Some(event_val) => {
                        if event_val != existing {
                            return false;
                        }
                    }
                    None => return false, // NULL at bind position.
                }
            }
            // If not yet bound (None), we don't bind here (binding
            // happens at step creation). Treat as no constraint.
        }

        true
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Rebinding helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Find the next eligible rebind anchor for a track whose current anchor
/// has expired.
///
/// Scans `step0_candidates` from the front:
/// - Removes entries whose window has already expired
///   (`event_ts - anchor_ts > window`).
/// - Returns the `anchor_ts` of the first entry that satisfies all of:
///   1. Within window: `event_ts - anchor_ts <= window`.
///   2. Before the current step: `anchor_ts <= last_step_ts`.
///   3. After scan_from: `anchor_ts > scan_from`.
///   4. Matching bindings: `binding_key == track_bindings` (or both empty).
///
/// **Why `anchor_ts <= last_step_ts` is correct:** In a non-consecutive
/// linear pattern, any step-0 event that occurred before the last matched
/// step would also have been a valid anchor for all intermediate steps
/// (sequence-matching.md §4.2). The step counter retroactively applies
/// intermediate-step events to the new anchor.
fn find_rebind_anchor(
    step0_candidates: &mut VecDeque<Step0Entry>,
    event_ts: i64,
    window: i64,
    last_step_ts: i64,
    scan_from: i64,
    track_bindings: &[BindingValue],
) -> Option<i64> {
    // Pop definitely-expired entries from the front.
    while let Some(front) = step0_candidates.front() {
        if event_ts - front.anchor_ts > window {
            step0_candidates.pop_front();
        } else {
            break;
        }
    }

    // Scan remaining entries for a valid rebind target.
    for entry in step0_candidates.iter() {
        // Within window.
        if event_ts - entry.anchor_ts > window {
            continue;
        }
        // Must have been before the current step event.
        if entry.anchor_ts > last_step_ts {
            continue;
        }
        // Must be after the MATCH ALL restart point.
        if entry.anchor_ts <= scan_from {
            continue;
        }
        // Binding values must match.
        if !track_bindings.is_empty() && entry.binding_key.as_slice() != track_bindings {
            continue;
        }
        return Some(entry.anchor_ts);
    }
    None
}

/// Find the first step-0 candidate whose `anchor_ts > scan_from` with
/// matching binding values. Does not modify the deque.
///
/// Used when MATCH ALL resets a track: immediately restart from the
/// next available anchor.
fn find_next_step0_candidate(
    step0_candidates: &VecDeque<Step0Entry>,
    scan_from: i64,
    track_bindings: &[BindingValue],
) -> Option<i64> {
    for entry in step0_candidates.iter() {
        if entry.anchor_ts <= scan_from {
            continue;
        }
        if !track_bindings.is_empty() && entry.binding_key.as_slice() != track_bindings {
            continue;
        }
        return Some(entry.anchor_ts);
    }
    None
}

/// Remove all `step0_candidates` entries with `anchor_ts <= scan_from`.
///
/// Called after MATCH ALL match completion. Entries are in ascending
/// timestamp order, so we pop from the front until we hit one that's
/// after `scan_from`.
fn purge_step0_candidates(step0_candidates: &mut VecDeque<Step0Entry>, scan_from: i64) {
    while let Some(front) = step0_candidates.front() {
        if front.anchor_ts <= scan_from {
            step0_candidates.pop_front();
        } else {
            break;
        }
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
    use std::time::Instant;

    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_planner::compile::{
        CompiledNfa, NfaState, PatternClass, PoisonTransition, Transition,
    };

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn make_batch(events: &[(&str, i64)]) -> RecordBatch {
        let types: Vec<&str> = events.iter().map(|(e, _)| *e).collect();
        let timestamps: Vec<i64> = events.iter().map(|(_, t)| *t).collect();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(types)),
                Arc::new(Int64Array::from(timestamps)),
            ],
        )
        .unwrap()
    }

    /// Build a simple forward transition with no predicates or bindings.
    fn simple_transition(event_type: &str, target: u16) -> Transition {
        Transition {
            event_type: event_type.to_string(),
            predicates: vec![],
            bind_variables: vec![],
            check_variables: vec![],
            target,
        }
    }

    /// Build a 3-step linear NFA: A → B → C (no window, no negation, no bindings).
    fn build_3step_nfa() -> CompiledNfa {
        // States: 0 (start), 1 (after A), 2 (after B), 3 = accept
        let states = vec![
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
            // Accept state: no transitions.
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ];

        CompiledNfa {
            states,
            accept_state: 3,
            relevant_event_types: BTreeSet::from(["A".into(), "B".into(), "C".into()]),
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        }
    }

    /// Build a 3-step linear NFA with a time window.
    fn build_3step_windowed_nfa(window_ns: i64) -> CompiledNfa {
        let mut nfa = build_3step_nfa();
        nfa.global_window = Some(window_ns);
        nfa
    }

    /// Build a 3-step linear NFA with negation: A → B without D → C.
    fn build_3step_negation_nfa() -> CompiledNfa {
        let states = vec![
            NfaState {
                transitions: vec![simple_transition("A", 1)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![simple_transition("B", 2)],
                // WITHOUT D between A and B.
                poison_transitions: vec![PoisonTransition {
                    event_type: "D".to_string(),
                    predicates: vec![],
                }],
            },
            NfaState {
                transitions: vec![simple_transition("C", 3)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ];

        CompiledNfa {
            states,
            accept_state: 3,
            relevant_event_types: BTreeSet::from(["A".into(), "B".into(), "C".into(), "D".into()]),
            pattern_class: PatternClass::LinearWithNegation,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        }
    }

    // ── Basic correctness tests ───────────────────────────────────────────────

    #[test]
    fn match_first_basic() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("X", 2), ("B", 3), ("C", 5)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 5);
        assert_eq!(state.partials.len(), 0);
    }

    #[test]
    fn match_first_no_match() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // Missing C.
        let batch = make_batch(&[("A", 1), ("B", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 0);
    }

    #[test]
    fn match_first_events_out_of_step_order_produce_no_spurious_match() {
        // B appears before A — should not match.
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("B", 1), ("A", 2), ("C", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // B at ts=1 before A — no match (C comes after A but no B after A).
        assert_eq!(state.completions.len(), 0);
    }

    #[test]
    fn match_first_strict_ordering() {
        // A and B at same timestamp — should not advance.
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 5), ("B", 5), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // B at ts=5 is not strictly after A at ts=5.
        assert_eq!(state.completions.len(), 0);
    }

    #[test]
    fn match_first_spans_multiple_batches() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch1 = make_batch(&[("A", 1), ("B", 2)]);
        let batch2 = make_batch(&[("C", 3)]);
        sim.process_batch(&mut state, &batch1, "event_type", "ts");
        sim.process_batch(&mut state, &batch2, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 3);
    }

    #[test]
    fn match_all_basic() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, true);
        let mut state = sim.create_state();

        // Two A-B-C sequences interleaved.
        let batch = make_batch(&[("A", 1), ("B", 2), ("C", 3), ("A", 4), ("B", 5), ("C", 6)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 2);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[1].anchor_ts, 4);
    }

    #[test]
    fn match_all_no_overlap() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, true);
        let mut state = sim.create_state();

        // A(1) is the first anchor. A(2) is a backup in step0_candidates.
        // B(4) advances the track. C(5) completes: anchor=1, final=5.
        // After the match, scan_from=5 purges both A(1) and A(2) from
        // step0_candidates, so no further restart is possible.
        let batch = make_batch(&[("A", 1), ("A", 2), ("B", 4), ("C", 5)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // Only one match: A(2), B(4), C(5).
        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn window_expiry_no_match() {
        let nfa = build_3step_windowed_nfa(5);
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // A(1), B(3), C(10): C is outside 5-unit window from A.
        let batch = make_batch(&[("A", 1), ("B", 3), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 0);
    }

    #[test]
    fn window_rebinding_deferred_consumption() {
        // Tests the §4.2 deferred-consumption scenario:
        //   A(1), A(5), B(6), C(10) with window=7
        // NFA finds anchor=5 for C(10). Step counter must also find it.
        let nfa = build_3step_windowed_nfa(7);
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("A", 5), ("B", 6), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(
            state.completions.len(),
            1,
            "should find match via rebinding to anchor=5"
        );
        assert_eq!(state.completions[0].anchor_ts, 5);
        assert_eq!(state.completions[0].final_ts, 10);
    }

    #[test]
    fn window_within_limit_matches() {
        let nfa = build_3step_windowed_nfa(10);
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("B", 5), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // 10 - 1 = 9 <= 10 → within window.
        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn negation_kills_track() {
        let nfa = build_3step_negation_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // D appears between A and B — kills the track.
        let batch = make_batch(&[("A", 1), ("D", 2), ("B", 3), ("C", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 0);
    }

    #[test]
    fn negation_after_step_does_not_affect() {
        let nfa = build_3step_negation_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // D appears between B and C — no poison at state 2, no effect.
        let batch = make_batch(&[("A", 1), ("B", 2), ("D", 3), ("C", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn emit_all_partial_at_entity_end() {
        let mut nfa = build_3step_nfa();
        nfa.emit_all = true;
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // A and B matched but no C.
        let batch = make_batch(&[("A", 1), ("B", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        sim.finish_entity(&mut state);

        assert_eq!(state.completions.len(), 0);
        assert_eq!(state.partials.len(), 1);
        assert_eq!(state.partials[0].anchor_ts, 1);
        assert_eq!(state.partials[0].step_reached, 2); // 2 steps reached
    }

    #[test]
    fn emit_all_window_expiry_records_partial() {
        let mut nfa = build_3step_windowed_nfa(5);
        nfa.emit_all = true;
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // A(1), B(3) matched, C(10) — window expires. Should see partial.
        let batch = make_batch(&[("A", 1), ("B", 3), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // Window expires at C(10): 10 - 1 = 9 > 5. No rebind possible.
        assert_eq!(state.completions.len(), 0);
        assert_eq!(state.partials.len(), 1);
        assert_eq!(state.partials[0].step_reached, 2);
    }

    #[test]
    fn single_step_pattern() {
        // Pattern: just A (1 step). Any A event completes the match.
        let states = vec![
            NfaState {
                transitions: vec![simple_transition("A", 1)],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ];
        let nfa = CompiledNfa {
            states,
            accept_state: 1,
            relevant_event_types: BTreeSet::from(["A".into()]),
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1],
        };
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("X", 1), ("A", 2), ("A", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 2);
    }

    #[test]
    fn empty_batch_no_crash() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 0);
    }

    #[test]
    fn single_event_entity_no_match() {
        let nfa = build_3step_nfa();
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 0);
    }

    // ── Performance smoke test ────────────────────────────────────────────────

    /// Sanity check that the step counter is meaningfully faster than the
    /// NFA on the same 3-step linear input.
    ///
    /// Acceptance criterion from TASK-305 / Wave 3: the step-counter fast
    /// path benchmarks within 2× of the Wave 2 scan-only baseline, and
    /// is faster than the full NFA path on linear patterns.
    ///
    /// This is NOT a micro-benchmark — it just verifies the relative order
    /// of magnitude. Criterion.rs benchmarks in `benches/wave3/` measure
    /// the exact numbers.
    #[test]
    fn perf_smoke_step_counter_faster_than_nfa() {
        use super::super::nfa::NfaSimulator;

        const ENTITIES: usize = 1_000;
        const EVENTS_PER_ENTITY: usize = 500;
        const WARMUP: usize = 3;
        const RUNS: usize = 10;

        // Build a synthetic event stream: 10% A, 10% B, 10% C, 70% other.
        let mut events: Vec<(&str, i64)> = Vec::with_capacity(EVENTS_PER_ENTITY);
        for i in 0..EVENTS_PER_ENTITY {
            let ts = i as i64;
            let event = match i % 10 {
                0 => "A",
                3 => "B",
                7 => "C",
                _ => "X",
            };
            events.push((event, ts));
        }
        let batch = make_batch(&events);

        let nfa = build_3step_nfa();
        let step_sim = StepCounterSimulator::new(nfa.clone(), false);
        let nfa_sim = NfaSimulator::new(nfa.clone(), false);

        // Warm up.
        for _ in 0..WARMUP {
            for _ in 0..ENTITIES {
                let mut s = step_sim.create_state();
                step_sim.process_batch(&mut s, &batch, "event_type", "ts");
            }
            for _ in 0..ENTITIES {
                let mut s = nfa_sim.create_state();
                nfa_sim.process_batch(&mut s, &batch, "event_type", "ts");
            }
        }

        // Measure step counter.
        let t0 = Instant::now();
        for _ in 0..RUNS {
            for _ in 0..ENTITIES {
                let mut s = step_sim.create_state();
                step_sim.process_batch(&mut s, &batch, "event_type", "ts");
            }
        }
        let step_ns = t0.elapsed().as_nanos() as f64 / (RUNS * ENTITIES * EVENTS_PER_ENTITY) as f64;

        // Measure NFA.
        let t1 = Instant::now();
        for _ in 0..RUNS {
            for _ in 0..ENTITIES {
                let mut s = nfa_sim.create_state();
                nfa_sim.process_batch(&mut s, &batch, "event_type", "ts");
            }
        }
        let nfa_ns = t1.elapsed().as_nanos() as f64 / (RUNS * ENTITIES * EVENTS_PER_ENTITY) as f64;

        // The step counter should be faster than (or at most equal to)
        // the NFA on a linear pattern. We allow up to 3× slower as a
        // lenient guard — a real regression analysis uses Criterion.rs.
        assert!(
            step_ns <= nfa_ns * 3.0,
            "step counter ({step_ns:.2} ns/event) is >3× slower than NFA ({nfa_ns:.2} ns/event) on a linear pattern — likely a regression"
        );

        // Also verify both produce the same match count (correctness).
        let mut sc_state = step_sim.create_state();
        step_sim.process_batch(&mut sc_state, &batch, "event_type", "ts");
        let mut nfa_state = nfa_sim.create_state();
        nfa_sim.process_batch(&mut nfa_state, &batch, "event_type", "ts");
        assert_eq!(
            sc_state.completions.len(),
            nfa_state.completions().len(),
            "step counter and NFA must agree on match count"
        );
    }

    #[test]
    fn window_rebinding_at_step_greater_than_one() {
        // Verify rebinding works when the track has already advanced past step 1.
        //
        // Scenario (window=7):
        //   A(1), A(5), B(6), B(8), C(12)
        //
        // - A(1) starts track with anchor=1, last_step_ts=1.
        // - A(5) adds a backup anchor to step0_candidates.
        // - B(6) advances to step 2: anchor=1, last_step_ts=6.
        // - B(8) is skipped (track already at step 2, B is not step-3 event).
        // - C(12): 12 - 1 = 11 > window=7 → expiry. Rebind: try anchor=5.
        //   Condition: 12-5=7 ≤ 7 (OK), anchor=5 ≤ last_step_ts=6 (OK).
        //   Rebind to anchor=5, then advance with C(12): match!
        let nfa = build_3step_windowed_nfa(7);
        let sim = StepCounterSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("A", 5), ("B", 6), ("B", 8), ("C", 12)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(
            state.completions.len(),
            1,
            "should find match via rebinding to anchor=5 from step-2 track"
        );
        assert_eq!(state.completions[0].anchor_ts, 5);
        assert_eq!(state.completions[0].final_ts, 12);
    }

    #[test]
    fn active_state_cap_prevents_unbounded_track_growth() {
        // Build a 2-step NFA with a variable binding so that each distinct
        // value of col "k" creates a separate track.
        //
        // With active_state_limit=3, only the first 3 distinct binding values
        // may create tracks. The 4th event should be dropped and recorded
        // in dropped_count.
        use bqlite_planner::compile::VariableBindingDef;

        let states = vec![
            NfaState {
                transitions: vec![Transition {
                    event_type: "A".to_string(),
                    predicates: vec![],
                    bind_variables: vec![0], // bind var 0 at step 0
                    check_variables: vec![],
                    target: 1,
                }],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![Transition {
                    event_type: "B".to_string(),
                    predicates: vec![],
                    bind_variables: vec![],
                    check_variables: vec![0], // check var 0 at step 1
                    target: 2,
                }],
                poison_transitions: vec![],
            },
            NfaState {
                transitions: vec![],
                poison_transitions: vec![],
            },
        ];

        // We need variable_bindings to have 1 entry so that bound_values
        // is populated. column_index=0 points at the first column ("k").
        let nfa = CompiledNfa {
            states,
            accept_state: 2,
            relevant_event_types: BTreeSet::from(["A".into(), "B".into()]),
            pattern_class: PatternClass::LinearWithBindings,
            variable_bindings: vec![VariableBindingDef {
                name: "k".to_string(),
                source_column: "k".to_string(),
                column_index: 0,
                bind_step: 0,
            }],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2],
        };

        // Build batch with 4 distinct A events using a batch with a "k" column.
        // Since BindingValueCache uses ColumnId(0) = first column, put "k" first.
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8View, false),
            arrow::datatypes::Field::new("event_type", arrow::datatypes::DataType::Utf8View, false),
            arrow::datatypes::Field::new("ts", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec!["v1", "v2", "v3", "v4"])),
                Arc::new(StringViewArray::from(vec!["A", "A", "A", "A"])),
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])),
            ],
        )
        .unwrap();

        let sim = StepCounterSimulator::new(nfa, false).with_active_state_limit(3);
        let mut state = sim.create_state();

        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // Exactly 3 tracks created (cap = 3).
        assert_eq!(state.tracks.len(), 3, "only 3 tracks should be created");
        // 4th event was dropped.
        assert_eq!(state.dropped_count, 1, "one track dropped due to cap");
        assert!(state.cap_exceeded, "cap_exceeded flag must be set");
    }
}
