//! General-path NFA runtime simulator per sequence-matching.md §3–§9.
//!
//! Consumes the [`CompiledNfa`] produced by the pattern compiler
//! (TASK-311) and implements the three-phase per-event simulation:
//!
//! 1. **Phase 1** — process existing candidates in reverse state order:
//!    time-window expiry, poison-transition kills, forward propagation.
//! 2. **Phase 2** — start new candidates when the current event matches
//!    a step-0 transition.
//! 3. **Phase 3** — deduplicate per `(state, track)` and check accept.
//!
//! This module owns the runtime data structures (`NfaSimulator`,
//! `EntityNfaState`, `CandidateEntry`) and the per-event simulation
//! loop. It does **not** handle variable bindings (TASK-306) or the
//! step-counter fast path (TASK-305).
//!
//! ## Step indexing convention
//!
//! `PartialMatch::step_reached` and `state_to_step` use **0-indexed**
//! step numbers (matching the compiled NFA from TASK-311). The
//! user-facing output schema (match-operator.md §12.1) specifies
//! 1-indexed `step_reached`. The translation from 0-indexed to
//! 1-indexed is the responsibility of the operator integration layer
//! (TASK-321).
//!
//! ## Design references
//!
//! - sequence-matching.md §3.3 (transition algorithm)
//! - sequence-matching.md §6 (candidate propagation model)
//! - sequence-matching.md §7 (time-window semantics)
//! - sequence-matching.md §16.1 (active state limit)
//! - operators/match-operator.md §3–§13 (operator integration)

use std::collections::VecDeque;

use arrow::array::{Array, BooleanArray, StringViewArray};
use arrow::record_batch::RecordBatch;

use bqlite_planner::compile::{CompiledNfa, NfaState, Transition};

use crate::eval;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Default maximum number of candidate entries across all states for a
/// single entity (sequence-matching.md §16.1).
pub const DEFAULT_ACTIVE_STATE_LIMIT: usize = 10_000;

// ─────────────────────────────────────────────────────────────────────────────
// CandidateEntry
// ─────────────────────────────────────────────────────────────────────────────

/// A single in-flight NFA candidate.
///
/// Tracks both the original anchor timestamp (for global window checks)
/// and the timestamp of the event that last advanced it (for strict
/// ordering per sequence-matching.md §15.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateEntry {
    /// First step's timestamp — for window check:
    /// `event.ts - anchor_ts <= global_window`.
    pub anchor_ts: i64,
    /// Timestamp of the event that advanced this candidate to its
    /// current state. Forward transitions require
    /// `event.ts > last_step_ts` (strict ordering).
    pub last_step_ts: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// StateCandidates
// ─────────────────────────────────────────────────────────────────────────────

/// Per-state candidate deque within a single entity (no binding tracks
/// in this module — TASK-306 adds binding-track partitioning on top).
///
/// Uses `VecDeque` so that expiry from the front is O(1).
/// The design doc's `PendingTimestamps` inline/spill split (§6.1) is
/// a follow-up micro-optimisation; `VecDeque` is correct and gives us
/// the right algorithmic complexity now.
#[derive(Debug, Clone)]
struct StateCandidates {
    candidates: VecDeque<CandidateEntry>,
}

impl StateCandidates {
    fn new() -> Self {
        Self {
            candidates: VecDeque::new(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.candidates.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    /// Remove all candidates whose window has expired relative to
    /// `event_ts`. Returns how many were removed.
    fn expire_window(&mut self, event_ts: i64, global_window: i64) -> usize {
        let mut removed = 0;
        while let Some(front) = self.candidates.front() {
            if event_ts - front.anchor_ts > global_window {
                self.candidates.pop_front();
                removed += 1;
            } else {
                break;
            }
        }
        removed
    }

    /// Remove all candidates (poison kill). Returns how many were killed.
    fn kill_all(&mut self) -> usize {
        let n = self.candidates.len();
        self.candidates.clear();
        n
    }

    /// Insert a candidate, deduplicating by `anchor_ts`: if an entry
    /// with the same `anchor_ts` already exists, keep the one with
    /// the later `last_step_ts`.
    fn insert_dedup(&mut self, entry: CandidateEntry) {
        // Candidates are generally ordered by anchor_ts (since events
        // arrive in timestamp order). A new propagated candidate's
        // anchor_ts usually matches the latest entry — check from the
        // back for O(1) amortised.
        for existing in self.candidates.iter_mut().rev() {
            if existing.anchor_ts == entry.anchor_ts {
                if entry.last_step_ts > existing.last_step_ts {
                    existing.last_step_ts = entry.last_step_ts;
                }
                return;
            }
            if existing.anchor_ts < entry.anchor_ts {
                break;
            }
        }
        self.candidates.push_back(entry);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MatchResult
// ─────────────────────────────────────────────────────────────────────────────

/// Result from completing a match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchCompletion {
    /// The anchor timestamp of the completed match.
    pub anchor_ts: i64,
    /// The timestamp of the final step.
    pub final_ts: i64,
}

/// Information about an incomplete candidate emitted under EMIT ALL.
///
/// `step_reached` is **0-indexed** (matches `CompiledNfa::state_to_step`).
/// The operator integration layer (TASK-321) adds 1 when producing the
/// user-facing output column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartialMatch {
    /// The anchor timestamp.
    pub anchor_ts: i64,
    /// The farthest logical step reached (0-indexed).
    pub step_reached: u8,
}

// ─────────────────────────────────────────────────────────────────────────────
// PredicateCache
// ─────────────────────────────────────────────────────────────────────────────

/// Per-batch cache of evaluated predicate results.
///
/// Evaluates each `CompiledExpr` once per batch and stores the resulting
/// `BooleanArray` for O(1) per-row lookups. Without this cache, every
/// transition check would re-evaluate the full batch — O(rows²) for
/// batches with predicates.
struct PredicateCache {
    /// Cached `BooleanArray` results, indexed to match the order
    /// predicates appear in NFA states. Stored as `Option` because
    /// evaluation can fail (in which case the predicate is treated as
    /// not matching).
    results: Vec<Option<BooleanArray>>,
}

impl PredicateCache {
    /// Build a cache by evaluating all unique predicates in the NFA
    /// against the given batch. The returned cache maps each predicate
    /// (identified by a flat index across all transitions and poison
    /// transitions) to its evaluated `BooleanArray`.
    fn build(nfa: &CompiledNfa, batch: &RecordBatch) -> Self {
        let mut results = Vec::new();
        for state in &nfa.states {
            for transition in &state.transitions {
                for predicate in &transition.predicates {
                    let result = eval::evaluate_bool(predicate, batch).ok();
                    results.push(result);
                }
            }
            for poison in &state.poison_transitions {
                for predicate in &poison.predicates {
                    let result = eval::evaluate_bool(predicate, batch).ok();
                    results.push(result);
                }
            }
        }
        Self { results }
    }

    /// Check if a specific predicate result is true at `row`.
    /// Returns `false` if the evaluation failed or the value is null.
    #[inline]
    fn check(&self, predicate_idx: usize, row: usize) -> bool {
        match &self.results[predicate_idx] {
            Some(arr) => !arr.is_null(row) && arr.value(row),
            None => false,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityNfaState
// ─────────────────────────────────────────────────────────────────────────────

/// Per-entity mutable NFA state. Persists across sub-batches.
///
/// Without variable bindings (TASK-306), there is exactly one "track"
/// per entity — the state deques here are the single default track.
#[derive(Debug, Clone)]
pub struct EntityNfaState {
    /// Per-NFA-state candidate deques. Length == `nfa.states.len()`.
    state_deques: Vec<StateCandidates>,
    /// Completed matches collected during simulation.
    completions: Vec<MatchCompletion>,
    /// Partial matches emitted via EMIT ALL (window expiry).
    partials: Vec<PartialMatch>,
    /// Number of candidates dropped due to the active state limit.
    dropped_count: u64,
    /// Farthest step reached across all candidates (for EMIT ALL at
    /// entity-end).
    max_step_reached: u8,
    /// For MATCH ALL: timestamp of the last consumed match. New
    /// candidates are only created from events with `ts > scan_from`.
    scan_from: Option<i64>,
    /// Reusable propagation scratch buffer (avoids per-event heap
    /// allocation).
    propagation_buf: Vec<(u16, CandidateEntry)>,
}

impl EntityNfaState {
    /// Create fresh state for a new entity.
    pub fn new(num_states: usize) -> Self {
        let mut state_deques = Vec::with_capacity(num_states);
        for _ in 0..num_states {
            state_deques.push(StateCandidates::new());
        }
        Self {
            state_deques,
            completions: Vec::new(),
            partials: Vec::new(),
            dropped_count: 0,
            max_step_reached: 0,
            scan_from: None,
            propagation_buf: Vec::new(),
        }
    }

    /// Total number of active candidate entries across all states.
    pub fn total_candidates(&self) -> usize {
        self.state_deques.iter().map(|s| s.len()).sum()
    }

    /// Completed matches found so far.
    pub fn completions(&self) -> &[MatchCompletion] {
        &self.completions
    }

    /// Partial matches emitted under EMIT ALL (from window expiry).
    pub fn partials(&self) -> &[PartialMatch] {
        &self.partials
    }

    /// Number of candidates dropped due to the active state limit.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    /// Emit remaining candidates as partials (for EMIT ALL) and clear
    /// all deques. Used by MATCH ALL reset and finish_entity.
    fn drain_as_partials(&mut self, state_to_step: &[u8]) {
        for (idx, deque) in self.state_deques.iter_mut().enumerate() {
            let step = state_to_step[idx];
            for entry in deque.candidates.drain(..) {
                self.partials.push(PartialMatch {
                    anchor_ts: entry.anchor_ts,
                    step_reached: step,
                });
            }
        }
    }

    /// Reset state for MATCH ALL after a completed match.
    fn reset_for_match_all(&mut self, consumed_ts: i64, emit_all: bool, state_to_step: &[u8]) {
        if emit_all {
            self.drain_as_partials(state_to_step);
        } else {
            for deque in &mut self.state_deques {
                deque.candidates.clear();
            }
        }
        self.scan_from = Some(consumed_ts);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate index mapping
// ─────────────────────────────────────────────────────────────────────────────

/// Pre-computed mapping from (state_idx, transition_kind, transition_idx,
/// predicate_idx) to the flat predicate cache index.
#[derive(Debug)]
struct PredicateIndexMap {
    /// For each state: (forward_start, poison_start, next_state_start).
    /// forward predicates for state S start at offsets[S].0
    /// poison predicates for state S start at offsets[S].1
    offsets: Vec<(usize, usize)>,
}

impl PredicateIndexMap {
    fn build(nfa: &CompiledNfa) -> Self {
        let mut offsets = Vec::with_capacity(nfa.states.len());
        let mut idx = 0;
        for state in &nfa.states {
            let forward_start = idx;
            for transition in &state.transitions {
                idx += transition.predicates.len();
            }
            let poison_start = idx;
            for poison in &state.poison_transitions {
                idx += poison.predicates.len();
            }
            offsets.push((forward_start, poison_start));
        }
        Self { offsets }
    }

    /// Get the flat predicate cache index for a forward transition's
    /// predicate.
    fn forward_predicate_idx(
        &self,
        state_idx: usize,
        nfa_state: &NfaState,
        transition_idx: usize,
        pred_idx: usize,
    ) -> usize {
        let base = self.offsets[state_idx].0;
        let mut offset = 0;
        for t in &nfa_state.transitions[..transition_idx] {
            offset += t.predicates.len();
        }
        base + offset + pred_idx
    }

    /// Get the flat predicate cache index for a poison transition's
    /// predicate.
    fn poison_predicate_idx(
        &self,
        state_idx: usize,
        nfa_state: &NfaState,
        poison_idx: usize,
        pred_idx: usize,
    ) -> usize {
        let base = self.offsets[state_idx].1;
        let mut offset = 0;
        for p in &nfa_state.poison_transitions[..poison_idx] {
            offset += p.predicates.len();
        }
        base + offset + pred_idx
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NfaSimulator
// ─────────────────────────────────────────────────────────────────────────────

/// Immutable NFA runtime driver. Shared across entities.
///
/// Created once per query from a [`CompiledNfa`] and used to drive
/// per-entity [`EntityNfaState`] through the three-phase simulation.
#[derive(Debug)]
pub struct NfaSimulator {
    /// The compiled NFA program.
    nfa: CompiledNfa,
    /// Maximum candidates per entity before oldest are dropped.
    active_state_limit: usize,
    /// Whether this is a MATCH ALL query (repeated first-match with
    /// reset).
    match_all: bool,
    /// Pre-computed predicate index mapping.
    pred_map: PredicateIndexMap,
}

impl NfaSimulator {
    /// Construct a new simulator from a compiled NFA.
    ///
    /// `match_all` controls whether completed matches reset state
    /// (MATCH ALL) or stop processing the track (MATCH FIRST).
    pub fn new(nfa: CompiledNfa, match_all: bool) -> Self {
        let pred_map = PredicateIndexMap::build(&nfa);
        Self {
            nfa,
            active_state_limit: DEFAULT_ACTIVE_STATE_LIMIT,
            match_all,
            pred_map,
        }
    }

    /// Override the active state limit (default 10,000).
    pub fn with_active_state_limit(mut self, limit: usize) -> Self {
        self.active_state_limit = limit;
        self
    }

    /// Create fresh per-entity state.
    pub fn create_state(&self) -> EntityNfaState {
        EntityNfaState::new(self.nfa.states.len())
    }

    /// Access the underlying compiled NFA.
    pub fn nfa(&self) -> &CompiledNfa {
        &self.nfa
    }

    /// Process a sub-batch of events for a single entity.
    ///
    /// Events must be sorted by timestamp ascending and belong to
    /// exactly one entity. The `event_type_col` column name is used
    /// to locate the event type string in the batch.
    pub fn process_batch(
        &self,
        state: &mut EntityNfaState,
        batch: &RecordBatch,
        event_type_col: &str,
        timestamp_col: &str,
    ) {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        // Resolve event_type and timestamp columns.
        let event_types = match batch.column_by_name(event_type_col) {
            Some(col) => match col.as_any().downcast_ref::<StringViewArray>() {
                Some(arr) => arr,
                None => return, // Wrong type — skip batch.
            },
            None => return,
        };

        let timestamps = match resolve_timestamp_column(batch, timestamp_col) {
            Some(ts) => ts,
            None => return,
        };

        // For MATCH FIRST: if we already have a completion and this is
        // not MATCH ALL, skip processing.
        if !self.match_all && !state.completions.is_empty() {
            return;
        }

        // Pre-evaluate all predicates once per batch (avoids O(N²)
        // per-row re-evaluation).
        let pred_cache = PredicateCache::build(&self.nfa, batch);

        #[allow(clippy::needless_range_loop)] // `row` indexes into multiple arrays + batch
        for row in 0..num_rows {
            if event_types.is_null(row) {
                continue;
            }
            let event_type = event_types.value(row);
            let event_ts = timestamps[row];

            self.process_event(state, event_type, event_ts, row, &pred_cache);

            // For MATCH FIRST, stop after first completion.
            if !self.match_all && !state.completions.is_empty() {
                break;
            }
        }
    }

    /// Process a single event through the three-phase NFA simulation.
    ///
    /// This is the core hot loop per sequence-matching.md §3.3.
    fn process_event(
        &self,
        state: &mut EntityNfaState,
        event_type: &str,
        event_ts: i64,
        row: usize,
        pred_cache: &PredicateCache,
    ) {
        let accept = self.nfa.accept_state as usize;

        // Quick check: if the event type is irrelevant to any
        // transition or poison in the NFA, we can skip the full
        // simulation. Window expiry is deferred to the next relevant
        // event (safe because events arrive in timestamp order — any
        // candidate that would have expired now will also expire then).
        if !self.nfa.relevant_event_types.contains(event_type) {
            return;
        }

        // ── Phase 1: Process existing active states (reverse order) ──
        //
        // Reverse order prevents cascading: a candidate propagated from
        // state S to S+1 won't be re-evaluated for S+1's transitions in
        // the same event pass.
        //
        // We collect propagations into a reusable scratch buffer to
        // avoid mutating state_deques while iterating.
        state.propagation_buf.clear();

        let num_states = self.nfa.states.len();
        for state_idx in (0..num_states).rev() {
            if state_idx == accept {
                continue; // Accept state has no outgoing transitions.
            }

            let nfa_state = &self.nfa.states[state_idx];

            // Phase 1a: Expire candidates whose window has passed.
            if let Some(window) = self.nfa.global_window {
                let deque = &mut state.state_deques[state_idx];
                if self.nfa.emit_all {
                    // Record step_reached for expired candidates before
                    // dropping them.
                    let step = self.nfa.state_to_step[state_idx];
                    while let Some(front) = deque.candidates.front() {
                        if event_ts - front.anchor_ts > window {
                            let entry = deque.candidates.pop_front().unwrap();
                            state.partials.push(PartialMatch {
                                anchor_ts: entry.anchor_ts,
                                step_reached: step,
                            });
                        } else {
                            break;
                        }
                    }
                } else {
                    deque.expire_window(event_ts, window);
                }
            }

            // Phase 1b: Check poison transitions.
            if self.event_matches_poison(state_idx, nfa_state, event_type, row, pred_cache) {
                let deque = &mut state.state_deques[state_idx];
                if self.nfa.emit_all {
                    let step = self.nfa.state_to_step[state_idx];
                    for entry in deque.candidates.drain(..) {
                        state.partials.push(PartialMatch {
                            anchor_ts: entry.anchor_ts,
                            step_reached: step,
                        });
                    }
                } else {
                    deque.kill_all();
                }
            }

            // Phase 1c: Check forward transitions.
            for (t_idx, transition) in nfa_state.transitions.iter().enumerate() {
                if !self.event_matches_transition(
                    state_idx, nfa_state, t_idx, transition, event_type, row, pred_cache,
                ) {
                    continue;
                }

                // Propagate eligible candidates.
                let deque = &state.state_deques[state_idx];
                for candidate in &deque.candidates {
                    // Strict timestamp ordering (§15.1).
                    if event_ts <= candidate.last_step_ts {
                        continue;
                    }
                    // Global window check.
                    if let Some(window) = self.nfa.global_window {
                        if event_ts - candidate.anchor_ts > window {
                            continue;
                        }
                    }
                    // Variable binding checks are deferred to TASK-306.
                    state.propagation_buf.push((
                        transition.target,
                        CandidateEntry {
                            anchor_ts: candidate.anchor_ts,
                            last_step_ts: event_ts,
                        },
                    ));
                }
            }
        }

        // Apply propagations from the scratch buffer.
        for i in 0..state.propagation_buf.len() {
            let (target, entry) = state.propagation_buf[i];
            state.state_deques[target as usize].insert_dedup(entry);
        }

        // ── Phase 2: Start new candidates ──
        //
        // New candidates are created AFTER processing existing states.
        // This prevents the same event from both starting and advancing
        // a candidate.
        let start_state = &self.nfa.states[0];
        for (t_idx, transition) in start_state.transitions.iter().enumerate() {
            if !self.event_matches_transition(
                0,
                start_state,
                t_idx,
                transition,
                event_type,
                row,
                pred_cache,
            ) {
                continue;
            }
            // For MATCH ALL: skip if event is at or before scan_from.
            if let Some(scan_from) = state.scan_from {
                if event_ts <= scan_from {
                    continue;
                }
            }

            let new_entry = CandidateEntry {
                anchor_ts: event_ts,
                last_step_ts: event_ts,
            };
            state.state_deques[transition.target as usize].insert_dedup(new_entry);

            // Track step reached for EMIT ALL.
            let step = self.nfa.state_to_step[transition.target as usize];
            if step > state.max_step_reached {
                state.max_step_reached = step;
            }
        }

        // ── Phase 3: Dedup and check accept ──
        //
        // Dedup was applied during insert_dedup. Check accept state.
        let accept_deque = &mut state.state_deques[accept];
        if !accept_deque.is_empty() {
            // Consume earliest anchor (§4.3).
            if let Some(entry) = accept_deque.candidates.pop_front() {
                let completion = MatchCompletion {
                    anchor_ts: entry.anchor_ts,
                    final_ts: entry.last_step_ts,
                };
                state.completions.push(completion);

                // Update max step reached.
                let num_steps = self.nfa.state_to_step[accept];
                if num_steps > state.max_step_reached {
                    state.max_step_reached = num_steps;
                }

                if self.match_all {
                    // MATCH ALL: emit remaining candidates as partials
                    // (if EMIT ALL) and reset.
                    state.reset_for_match_all(
                        entry.last_step_ts,
                        self.nfa.emit_all,
                        &self.nfa.state_to_step,
                    );
                } else {
                    // MATCH FIRST: clear everything — we're done.
                    for deque in &mut state.state_deques {
                        deque.candidates.clear();
                    }
                }
            }
        }

        // ── Active state limit enforcement (§16.1) ──
        self.enforce_active_state_limit(state);

        // Track max step reached from surviving candidates.
        for (idx, deque) in state.state_deques.iter().enumerate() {
            if !deque.is_empty() {
                let step = self.nfa.state_to_step[idx];
                if step > state.max_step_reached {
                    state.max_step_reached = step;
                }
            }
        }
    }

    /// Check if an event at `row` matches a forward transition.
    #[allow(clippy::too_many_arguments)]
    fn event_matches_transition(
        &self,
        state_idx: usize,
        nfa_state: &NfaState,
        transition_idx: usize,
        transition: &Transition,
        event_type: &str,
        row: usize,
        pred_cache: &PredicateCache,
    ) -> bool {
        if transition.event_type != event_type {
            return false;
        }
        for (p_idx, _) in transition.predicates.iter().enumerate() {
            let cache_idx =
                self.pred_map
                    .forward_predicate_idx(state_idx, nfa_state, transition_idx, p_idx);
            if !pred_cache.check(cache_idx, row) {
                return false;
            }
        }
        true
    }

    /// Check if an event at `row` matches any poison transition at a state.
    fn event_matches_poison(
        &self,
        state_idx: usize,
        nfa_state: &NfaState,
        event_type: &str,
        row: usize,
        pred_cache: &PredicateCache,
    ) -> bool {
        for (poison_idx, poison) in nfa_state.poison_transitions.iter().enumerate() {
            if poison.event_type != event_type {
                continue;
            }
            let mut all_pass = true;
            for (p_idx, _) in poison.predicates.iter().enumerate() {
                let cache_idx = self
                    .pred_map
                    .poison_predicate_idx(state_idx, nfa_state, poison_idx, p_idx);
                if !pred_cache.check(cache_idx, row) {
                    all_pass = false;
                    break;
                }
            }
            if all_pass {
                return true;
            }
        }
        false
    }

    /// Enforce the active state limit by dropping oldest candidates.
    fn enforce_active_state_limit(&self, state: &mut EntityNfaState) {
        let mut total = state.total_candidates();
        if total <= self.active_state_limit {
            return;
        }

        // Drop from the front of deques, starting with the earliest
        // anchors across all states. We repeatedly find the state with
        // the oldest front anchor and drop it.
        while total > self.active_state_limit {
            let mut oldest_state: Option<usize> = None;
            let mut oldest_anchor = i64::MAX;

            for (idx, deque) in state.state_deques.iter().enumerate() {
                if let Some(front) = deque.candidates.front() {
                    if front.anchor_ts < oldest_anchor {
                        oldest_anchor = front.anchor_ts;
                        oldest_state = Some(idx);
                    }
                }
            }

            if let Some(idx) = oldest_state {
                state.state_deques[idx].candidates.pop_front();
                state.dropped_count += 1;
                total -= 1;
            } else {
                break;
            }
        }
    }

    /// Finalize entity processing. Collects remaining in-progress
    /// candidates as partial matches when EMIT ALL is enabled.
    pub fn finish_entity(&self, mut state: EntityNfaState) -> EntityNfaState {
        if self.nfa.emit_all {
            state.drain_as_partials(&self.nfa.state_to_step);
        }
        state
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Resolve a timestamp column from the batch as an `i64` slice.
///
/// Supports both `TimestampNanosecondArray` and plain `Int64Array`.
fn resolve_timestamp_column<'a>(batch: &'a RecordBatch, col_name: &str) -> Option<&'a [i64]> {
    use arrow::array::{Int64Array, TimestampNanosecondArray};

    let col = batch.column_by_name(col_name)?;
    // Try TimestampNanosecondArray first (canonical schema type).
    if let Some(ts_arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(ts_arr.values().as_ref());
    }
    // Fall back to Int64Array for test convenience.
    if let Some(i64_arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Some(i64_arr.values().as_ref());
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_planner::compile::{
        CompiledNfa, NfaState, PatternClass, PoisonTransition, Transition,
    };

    /// Helper: build a simple RecordBatch with event_type (StringView)
    /// and ts (Int64) columns.
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

    /// Helper: build a RecordBatch with event_type, ts, and an extra
    /// f64 "amount" column.
    fn make_batch_with_amount(events: &[(&str, i64, f64)]) -> RecordBatch {
        let event_types: Vec<&str> = events.iter().map(|(e, _, _)| *e).collect();
        let timestamps: Vec<i64> = events.iter().map(|(_, t, _)| *t).collect();
        let amounts: Vec<f64> = events.iter().map(|(_, _, a)| *a).collect();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(event_types)),
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(Float64Array::from(amounts)),
            ],
        )
        .unwrap()
    }

    /// Helper: build a simple Transition with no predicates or bindings.
    fn simple_transition(event_type: &str, target: u16) -> Transition {
        Transition {
            event_type: event_type.to_string(),
            predicates: Vec::new(),
            bind_variables: Vec::new(),
            check_variables: Vec::new(),
            target,
        }
    }

    /// Helper: build a simple PoisonTransition with no predicates.
    fn simple_poison(event_type: &str) -> PoisonTransition {
        PoisonTransition {
            event_type: event_type.to_string(),
            predicates: Vec::new(),
        }
    }

    /// Build a 2-step linear NFA: A THEN B (no window).
    fn nfa_a_then_b() -> CompiledNfa {
        // State 0: start -- [A] --> State 1
        // State 1: -- [B] --> State 2 (accept)
        // State 2: accept
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
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2],
        }
    }

    /// Build a 3-step linear NFA: A THEN B THEN C.
    fn nfa_a_then_b_then_c() -> CompiledNfa {
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
            relevant_event_types: BTreeSet::from([
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
            ]),
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        }
    }

    // ── Basic matching ──────────────────────────────────────────────

    #[test]
    fn test_simple_two_step_match() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("B", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 2);
    }

    #[test]
    fn test_no_match_wrong_order() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("B", 1), ("A", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert!(state.completions.is_empty());
    }

    #[test]
    fn test_no_match_same_timestamp() {
        // Same-timestamp events do not satisfy THEN (§15.1).
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 5), ("B", 5)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert!(state.completions.is_empty());
    }

    #[test]
    fn test_non_consecutive_match() {
        // Events between matched steps are allowed (§15.2).
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("X", 2), ("Y", 3), ("B", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 4);
    }

    #[test]
    fn test_three_step_match() {
        let nfa = nfa_a_then_b_then_c();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("B", 5), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 10);
    }

    // ── MATCH FIRST semantics ───────────────────────────────────────

    #[test]
    fn test_match_first_stops_after_one() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false); // MATCH FIRST
        let mut state = sim.create_state();

        // Two possible matches: (1,2) and (3,4).
        let batch = make_batch(&[("A", 1), ("B", 2), ("A", 3), ("B", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
    }

    // ── MATCH ALL semantics ─────────────────────────────────────────

    #[test]
    fn test_match_all_finds_multiple() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, true); // MATCH ALL
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("B", 2), ("A", 3), ("B", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 2);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 2);
        assert_eq!(state.completions[1].anchor_ts, 3);
        assert_eq!(state.completions[1].final_ts, 4);
    }

    #[test]
    fn test_match_all_non_overlapping() {
        // MATCH ALL produces non-overlapping matches.
        // Events: A(1), A(2), B(3) — only one match should be produced
        // (the one with the earliest anchor).
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, true);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("A", 2), ("B", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // First match: anchor=1, final=3. Then reset with scan_from=3.
        // The A(2) candidate is cleared during reset.
        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
    }

    // ── Time window ─────────────────────────────────────────────────

    #[test]
    fn test_window_allows_within() {
        let mut nfa = nfa_a_then_b();
        nfa.global_window = Some(10); // 10 ns window
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 100), ("B", 105)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn test_window_rejects_expired() {
        let mut nfa = nfa_a_then_b();
        nfa.global_window = Some(10);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 100), ("B", 111)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert!(state.completions.is_empty());
    }

    #[test]
    fn test_window_boundary_exact() {
        // event_ts - anchor_ts == window should still pass (<=).
        let mut nfa = nfa_a_then_b();
        nfa.global_window = Some(10);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 100), ("B", 110)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn test_window_rebinding() {
        // From §4.2: A(1), A(5), B(6), C(10) with WITHIN 7.
        // A(1) expires (10-1=9>7), A(5) survives (10-5=5<=7).
        let mut nfa = nfa_a_then_b_then_c();
        nfa.global_window = Some(7);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("A", 5), ("B", 6), ("C", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 5);
        assert_eq!(state.completions[0].final_ts, 10);
    }

    // ── Poison transitions (negation) ───────────────────────────────

    #[test]
    fn test_poison_kills_candidates() {
        // A THEN B WITHOUT C: if C occurs between A and B, the match
        // is killed.
        let nfa = CompiledNfa {
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
            relevant_event_types: BTreeSet::from([
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
            ]),
            pattern_class: PatternClass::LinearWithNegation,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2],
        };

        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("C", 2), ("B", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // C killed the candidate at state 1.
        assert!(state.completions.is_empty());
    }

    #[test]
    fn test_poison_does_not_kill_wrong_event() {
        // A THEN B WITHOUT C: if event D occurs (not C), match succeeds.
        let nfa = CompiledNfa {
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
            relevant_event_types: BTreeSet::from([
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
            ]),
            pattern_class: PatternClass::LinearWithNegation,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2],
        };

        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("D", 2), ("B", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn test_poison_multi_state_scope() {
        // A THEN B THEN C WITHOUT D between A and C
        // (poison on states 1 and 2).
        let nfa = CompiledNfa {
            states: vec![
                NfaState {
                    transitions: vec![simple_transition("A", 1)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![simple_transition("B", 2)],
                    poison_transitions: vec![simple_poison("D")],
                },
                NfaState {
                    transitions: vec![simple_transition("C", 3)],
                    poison_transitions: vec![simple_poison("D")],
                },
                NfaState {
                    transitions: vec![],
                    poison_transitions: vec![],
                },
            ],
            accept_state: 3,
            relevant_event_types: BTreeSet::from([
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
            ]),
            pattern_class: PatternClass::LinearWithNegation,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        };

        let sim = NfaSimulator::new(nfa, false);

        // D before B — kills at state 1.
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("D", 2), ("B", 3), ("C", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert!(state.completions.is_empty());

        // D after B but before C — kills at state 2.
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("B", 2), ("D", 3), ("C", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert!(state.completions.is_empty());

        // No D — match succeeds.
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("B", 2), ("C", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);
    }

    // ── Repetition (self-loops) ─────────────────────────────────────

    #[test]
    fn test_repetition_one_or_more() {
        // A THEN B+ THEN C
        // State 0: --[A]--> State 1
        // State 1: --[B]--> State 2
        // State 2: --[B]--> State 2 (self-loop)
        // State 2: --[C]--> State 3 (accept)
        let nfa = CompiledNfa {
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
                    transitions: vec![
                        simple_transition("B", 2), // self-loop
                        simple_transition("C", 3),
                    ],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![],
                    poison_transitions: vec![],
                },
            ],
            accept_state: 3,
            relevant_event_types: BTreeSet::from([
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
            ]),
            pattern_class: PatternClass::GeneralNfa,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        };

        let sim = NfaSimulator::new(nfa.clone(), false);

        // One B.
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("B", 2), ("C", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);

        // Multiple B's.
        let sim = NfaSimulator::new(nfa.clone(), false);
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("B", 2), ("B", 3), ("B", 4), ("C", 5)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);

        // Zero B's — should NOT match (B+ requires at least one).
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("C", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert!(state.completions.is_empty());
    }

    // ── Alternation ─────────────────────────────────────────────────

    #[test]
    fn test_alternation() {
        // A THEN (B OR C) THEN D
        // State 0: --[A]--> State 1
        // State 1: --[B]--> State 2, --[C]--> State 2
        // State 2: --[D]--> State 3 (accept)
        let nfa = CompiledNfa {
            states: vec![
                NfaState {
                    transitions: vec![simple_transition("A", 1)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![simple_transition("B", 2), simple_transition("C", 2)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![simple_transition("D", 3)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![],
                    poison_transitions: vec![],
                },
            ],
            accept_state: 3,
            relevant_event_types: BTreeSet::from([
                "A".to_string(),
                "B".to_string(),
                "C".to_string(),
                "D".to_string(),
            ]),
            pattern_class: PatternClass::GeneralNfa,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        };

        let sim = NfaSimulator::new(nfa.clone(), false);

        // Match via B path.
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("B", 2), ("D", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);

        // Match via C path.
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("C", 2), ("D", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);
    }

    // ── EMIT ALL ────────────────────────────────────────────────────

    #[test]
    fn test_emit_all_completed_match() {
        let mut nfa = nfa_a_then_b();
        nfa.emit_all = true;
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("B", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
    }

    #[test]
    fn test_emit_all_partial_at_entity_end() {
        let mut nfa = nfa_a_then_b_then_c();
        nfa.emit_all = true;
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // Only match A and B, not C — entity ends.
        let batch = make_batch(&[("A", 1), ("B", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let state = sim.finish_entity(state);

        // Should have partial matches from remaining candidates.
        assert!(state.completions.is_empty());
        assert!(!state.partials.is_empty());
        // The candidate at state 2 (step 2) should be emitted.
        let max_step = state.partials.iter().map(|p| p.step_reached).max().unwrap();
        assert_eq!(max_step, 2);
    }

    #[test]
    fn test_emit_all_window_expiry_emits_partial() {
        let mut nfa = nfa_a_then_b_then_c();
        nfa.emit_all = true;
        nfa.global_window = Some(5);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // A(1), B(3), then event at ts=10 triggers window expiry for
        // the candidate from A(1) (10-1=9 > 5).
        let batch = make_batch(&[("A", 1), ("B", 3), ("A", 10)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // The candidate at state 2 (after B match) should have been
        // emitted as partial when it expired.
        assert!(state.completions.is_empty());
        assert!(!state.partials.is_empty());
    }

    #[test]
    fn test_match_all_emit_all_interaction() {
        // MATCH ALL + EMIT ALL: intermediate candidates at the time
        // of a match reset should be emitted as partials (§5.3).
        let mut nfa = nfa_a_then_b();
        nfa.emit_all = true;
        let sim = NfaSimulator::new(nfa, true); // MATCH ALL + EMIT ALL
        let mut state = sim.create_state();

        // A(1), A(2): two candidates at state 1.
        // B(3): both reach accept, earliest (anchor=1) is consumed.
        //   Remaining candidate (anchor=2) at state 1 should be
        //   emitted as a partial with step_reached=1.
        // A(4), B(5): second match.
        let batch = make_batch(&[("A", 1), ("A", 2), ("B", 3), ("A", 4), ("B", 5)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 2);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[1].anchor_ts, 4);
        // The A(2) candidate at state 1 should have been emitted as
        // a partial during the first match's reset.
        assert!(!state.partials.is_empty());
        let partial_steps: Vec<u8> = state.partials.iter().map(|p| p.step_reached).collect();
        assert!(partial_steps.contains(&1));
    }

    // ── Cross sub-batch persistence ─────────────────────────────────

    #[test]
    fn test_cross_sub_batch() {
        let nfa = nfa_a_then_b_then_c();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // First sub-batch: A occurs.
        let batch1 = make_batch(&[("A", 1)]);
        sim.process_batch(&mut state, &batch1, "event_type", "ts");
        assert!(state.completions.is_empty());

        // Second sub-batch: B occurs.
        let batch2 = make_batch(&[("B", 5)]);
        sim.process_batch(&mut state, &batch2, "event_type", "ts");
        assert!(state.completions.is_empty());

        // Third sub-batch: C occurs.
        let batch3 = make_batch(&[("C", 10)]);
        sim.process_batch(&mut state, &batch3, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[0].final_ts, 10);
    }

    // ── Multiple candidates / deferred consumption ──────────────────

    #[test]
    fn test_multiple_anchors_kept() {
        // Multiple anchors must be kept — the earlier anchor may be the
        // only one whose subsequent steps arrive in time (§6.4).
        //
        // Pattern: A THEN B THEN C, WITHIN 10
        // Events: A(1), A(5), B(8), C(14)
        //
        // Anchor 1: B(8) — 8-1=7<=10 ok. C(14) — 14-1=13>10 expired.
        // Anchor 5: B(8) — 8-5=3<=10 ok. C(14) — 14-5=9<=10 ok. Match!
        let mut nfa = nfa_a_then_b_then_c();
        nfa.global_window = Some(10);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("A", 5), ("B", 8), ("C", 14)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 5);
        assert_eq!(state.completions[0].final_ts, 14);
    }

    #[test]
    fn test_earliest_anchor_consumed() {
        // With multiple possible anchors, the earliest is consumed
        // (§4.3).
        let mut nfa = nfa_a_then_b();
        nfa.global_window = Some(100);
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        // A(1), A(2), B(3) — both anchors reach accept.
        let batch = make_batch(&[("A", 1), ("A", 2), ("B", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 1);
        assert_eq!(state.completions[0].anchor_ts, 1); // Earliest.
    }

    // ── Active state limit ──────────────────────────────────────────

    #[test]
    fn test_active_state_limit() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false).with_active_state_limit(3);
        let mut state = sim.create_state();

        // Create many A events, each generating a candidate at state 1.
        let events: Vec<(&str, i64)> = (1..=10).map(|i| ("A", i as i64)).collect();
        let batch = make_batch(&events);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        // Should be capped at 3 candidates total.
        assert!(state.total_candidates() <= 3);
        assert!(state.dropped_count > 0);
    }

    // ── Empty / edge cases ──────────────────────────────────────────

    #[test]
    fn test_empty_batch() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert!(state.completions.is_empty());
        assert_eq!(state.total_candidates(), 0);
    }

    #[test]
    fn test_single_event_entity() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert!(state.completions.is_empty());
        // Should have one candidate at state 1.
        assert_eq!(state.total_candidates(), 1);
    }

    #[test]
    fn test_irrelevant_events_only() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("X", 1), ("Y", 2), ("Z", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert!(state.completions.is_empty());
        assert_eq!(state.total_candidates(), 0);
    }

    #[test]
    fn test_match_all_scan_from_prevents_reuse() {
        // After MATCH ALL reset, events at or before scan_from should
        // not start new candidates.
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, true);
        let mut state = sim.create_state();

        // A(1), B(2) -> match, reset scan_from=2.
        // A(2) at ts=2 <= scan_from=2 -> should not start new candidate.
        // A(3), B(4) -> second match.
        let batch = make_batch(&[("A", 1), ("B", 2), ("A", 2), ("A", 3), ("B", 4)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        assert_eq!(state.completions.len(), 2);
        assert_eq!(state.completions[0].anchor_ts, 1);
        assert_eq!(state.completions[1].anchor_ts, 3);
    }

    // ── Anti-cascading ──────────────────────────────────────────────

    #[test]
    fn test_no_cascading_same_event_type() {
        // Pattern: A THEN A THEN A — a single "A" event must not
        // cascade through multiple states in one pass.
        let nfa = CompiledNfa {
            states: vec![
                NfaState {
                    transitions: vec![simple_transition("A", 1)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![simple_transition("A", 2)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![simple_transition("A", 3)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![],
                    poison_transitions: vec![],
                },
            ],
            accept_state: 3,
            relevant_event_types: BTreeSet::from(["A".to_string()]),
            pattern_class: PatternClass::LinearSimple,
            variable_bindings: vec![],
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2, 3],
        };

        let sim = NfaSimulator::new(nfa, false);

        // Three A events at different timestamps should complete.
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("A", 2), ("A", 3)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);

        // Two A events should NOT complete (need three distinct A events).
        let mut state = sim.create_state();
        let batch = make_batch(&[("A", 1), ("A", 2)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert!(state.completions.is_empty());
        // Should have candidates at state 1 and 2, but not at accept.
        assert!(state.state_deques[3].is_empty());
    }

    // ── Property predicates ─────────────────────────────────────────

    #[test]
    fn test_predicate_filters_transition() {
        // A THEN B WHERE amount > 100.0
        // Build a predicate: column("amount") > literal(100.0)
        use bqlite_core::{BqlType, PropertyValue};
        use bqlite_planner::compiled::{ArrowKernelId, CompareKernel, CompiledExpr, CompiledNode};

        let predicate = CompiledExpr {
            node: CompiledNode::Compare {
                left: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 2, // "amount" column at index 2
                        name: "amount".to_string(),
                    },
                    result_type: BqlType::Float,
                    nullable: false,
                }),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Float(100.0)),
                    result_type: BqlType::Float,
                    nullable: false,
                }),
                op: bqlite_ast::expr::CompareOp::Greater,
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtFloat),
            },
            result_type: BqlType::Bool,
            nullable: false,
        };

        let nfa = CompiledNfa {
            states: vec![
                NfaState {
                    transitions: vec![simple_transition("A", 1)],
                    poison_transitions: vec![],
                },
                NfaState {
                    transitions: vec![Transition {
                        event_type: "B".to_string(),
                        predicates: vec![predicate],
                        bind_variables: vec![],
                        check_variables: vec![],
                        target: 2,
                    }],
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
            global_window: None,
            emit_all: false,
            state_to_step: vec![0, 1, 2],
        };

        let sim = NfaSimulator::new(nfa, false);

        // B with amount=50.0 — predicate fails, no match.
        let mut state = sim.create_state();
        let batch = make_batch_with_amount(&[("A", 1, 0.0), ("B", 2, 50.0)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert!(state.completions.is_empty());

        // B with amount=200.0 — predicate passes, match.
        let mut state = sim.create_state();
        let batch = make_batch_with_amount(&[("A", 1, 0.0), ("B", 2, 200.0)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");
        assert_eq!(state.completions.len(), 1);
    }

    // ── Finish entity ───────────────────────────────────────────────

    #[test]
    fn test_finish_entity_without_emit_all() {
        let nfa = nfa_a_then_b();
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let state = sim.finish_entity(state);
        // Without EMIT ALL, partials should be empty.
        assert!(state.partials.is_empty());
    }

    #[test]
    fn test_finish_entity_with_emit_all() {
        let mut nfa = nfa_a_then_b_then_c();
        nfa.emit_all = true;
        let sim = NfaSimulator::new(nfa, false);
        let mut state = sim.create_state();

        let batch = make_batch(&[("A", 1), ("B", 5)]);
        sim.process_batch(&mut state, &batch, "event_type", "ts");

        let state = sim.finish_entity(state);
        // Should have partial matches from remaining candidates.
        // Candidates at state 1 (from A(1), step 1) and
        // state 2 (from B(5), step 2).
        assert!(!state.partials.is_empty());
    }
}
