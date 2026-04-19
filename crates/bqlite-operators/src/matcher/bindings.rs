//! Variable binding tracks per sequence-matching.md §8.
//!
//! This module implements:
//!
//! - [`BindingValue`] — compact, comparison-optimized enum for bound
//!   variable values (§6.2).
//! - [`BindingKey`] — track identity formed by the combination of all
//!   bound variable values (§8.2).
//! - [`BindingValueCache`] — per-batch pre-extraction of variable columns
//!   from Arrow arrays (§8.5).
//! - Binding check/extract utilities for NFA transition processing.
//!
//! ## Bind-on-first-reference / Check-on-subsequent-reference
//!
//! Each [`VariableBindingDef`] has a `bind_step` indicating the first step
//! that references the variable. At that step, the variable's value is
//! extracted from the event and stored in the binding key. Subsequent steps
//! with `check_variables` referencing the same variable index compare the
//! event's value against the stored binding.
//!
//! ## NULL handling
//!
//! If a column value to bind is NULL, no binding occurs and no track is
//! created for that value (sequence-matching.md §6.2). NULL in a check
//! position also fails the predicate.
//!
//! ## Design deviations
//!
//! - Custom `OrderedFloat` instead of `FloatOrd` (float-ord crate):
//!   avoids a dependency; same semantics (NaN == NaN for binding
//!   purposes per §6.2).
//!
//! ## CompactString adoption
//!
//! `BindingValue::String` uses [`compact_str::CompactString`] (TASK-454)
//! rather than `Box<str>`. For strings ≤ 24 bytes (typical analytics
//! binding values: plan names, countries, categories), clone is a
//! 24-byte stack memcpy with zero allocator interaction — roughly 10×
//! faster than `Box<str>` on the dominant binding-clone path. See
//! `docs/design/operators/compactstring-evaluation.md` §4.3 for the
//! benchmark evidence.
//!
//! ## Design references
//!
//! - sequence-matching.md §6.2 (BindingValue)
//! - sequence-matching.md §8 (variable bindings)
//! - sequence-matching.md §16.1 (active state limit, shared across tracks)

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use compact_str::CompactString;

use arrow::array::{
    Array, BooleanArray, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray,
};
use arrow::record_batch::RecordBatch;
use smallvec::SmallVec;

use bqlite_planner::compile::VariableBindingDef;

use super::nfa::{CandidateEntry, EntityNfaState, MatchCompletion, NfaSimulator, PartialMatch};

// ─────────────────────────────────────────────────────────────────────────────
// OrderedFloat
// ─────────────────────────────────────────────────────────────────────────────

/// Ordered `f64` wrapper providing `Eq` and `Hash`.
///
/// NaN == NaN for binding purposes — two NaN-valued bindings join the
/// same track (sequence-matching.md §6.2). This deviates from IEEE 754
/// but is the desired behavior for grouping.
///
/// `-0.0 == +0.0` follows IEEE 754 semantics (they represent the same
/// value conceptually and should join the same binding track).
#[derive(Debug, Clone, Copy)]
pub struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            return true;
        }
        self.0 == other.0
    }
}

impl Eq for OrderedFloat {}

impl Hash for OrderedFloat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_nan() {
            // All NaN variants hash identically.
            0x7ff8_0000_0000_0000_u64.hash(state);
        } else if self.0 == 0.0 {
            // Both +0.0 and −0.0 must hash identically since they're equal.
            0_u64.hash(state);
        } else {
            self.0.to_bits().hash(state);
        }
    }
}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0.is_nan(), other.0.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => {
                if self.0 == 0.0 && other.0 == 0.0 {
                    Ordering::Equal
                } else {
                    self.0.total_cmp(&other.0)
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BindingValue
// ────────────────────────────────────────────────────────────────────────��────

/// Compact representation of a bound variable value for NFA comparisons.
///
/// Only scalar types — List and Map are not valid binding targets.
/// The variant is selected during extraction based on the Arrow column
/// type.
///
/// This is distinct from `PropertyValue` (bqlite-core) — it is a
/// compact, comparison-optimized enum for the NFA hot path.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BindingValue {
    Bool(bool),
    Int(i64),
    Float(OrderedFloat),
    /// Stored as [`CompactString`]: strings ≤ 24 bytes are inline (no
    /// heap allocation). Clone is a 24-byte stack memcpy — ~10× faster
    /// than `Box<str>` for the typical short binding values seen in
    /// analytics workloads (see compactstring-evaluation.md §4.3).
    String(CompactString),
    /// Epoch nanoseconds.
    Timestamp(i64),
}

// ─────────────────────────────────────────────────────────────────────────────
// BindingKey
// ─────────────────────────────────────────────────────────────────────────────

/// Track identity: the combination of all bound variable values.
///
/// `SmallVec<[BindingValue; 2]>` keeps the common case (1–2 bindings)
/// stack-allocated. Patterns with more bindings spill to heap.
pub type BindingKey = SmallVec<[BindingValue; 2]>;

/// Build a [`BindingKey`] from a slice of bound variable values.
///
/// Returns `None` if any variable is not yet bound (should not happen
/// after all bind-steps have been processed for a track).
pub fn build_binding_key(bound_values: &[Option<BindingValue>]) -> Option<BindingKey> {
    bound_values.iter().cloned().collect::<Option<BindingKey>>()
}

// ─────────────────────────────────────────────────────────────────────────────
// BindingValueCache
// ─────────────────────────────────────────────────────────────────────────────

/// Pre-extracted binding values from a [`RecordBatch`].
///
/// Evaluates each variable's source column once per batch and stores
/// the resulting `Option<BindingValue>` per row. Without this cache,
/// every transition check would re-extract from the Arrow column —
/// O(rows × transitions) dynamic dispatch instead of O(rows × vars).
pub struct BindingValueCache {
    /// `columns[var_idx][row]` = extracted value for variable `var_idx`
    /// at `row`. `None` if the column value is NULL or the column is
    /// missing from the batch.
    columns: Vec<Vec<Option<BindingValue>>>,
}

impl BindingValueCache {
    /// Build a cache by extracting all variable binding columns from
    /// the batch.
    pub fn build(variable_bindings: &[VariableBindingDef], batch: &RecordBatch) -> Self {
        let num_rows = batch.num_rows();
        let columns = variable_bindings
            .iter()
            .map(|def| match batch.column_by_name(&def.source_column) {
                Some(col) => (0..num_rows)
                    .map(|row| extract_binding_value(col.as_ref(), row))
                    .collect(),
                None => vec![None; num_rows],
            })
            .collect();
        Self { columns }
    }

    /// Get the extracted value for variable `var_idx` at `row`.
    #[inline]
    pub fn get(&self, var_idx: usize, row: usize) -> Option<&BindingValue> {
        self.columns[var_idx][row].as_ref()
    }

    /// Returns `true` if there are no variable bindings.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Extraction
// ─────────────────────────────────────────────────────────────────────────────

/// Extract a [`BindingValue`] from an Arrow array at the given row.
///
/// Returns `None` if the value is NULL or the array type is not a
/// supported binding type.
///
/// Supports: `BooleanArray`, `Int64Array`, `Float64Array`,
/// `StringViewArray`, `TimestampNanosecondArray`.
fn extract_binding_value(array: &dyn Array, row: usize) -> Option<BindingValue> {
    if array.is_null(row) {
        return None;
    }

    // Try each supported type. The order reflects expected frequency
    // in analytics workloads (strings > ints > timestamps > floats > bools).
    if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
        return Some(BindingValue::String(CompactString::from(arr.value(row))));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        return Some(BindingValue::Int(arr.value(row)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Some(BindingValue::Timestamp(arr.value(row)));
    }
    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        return Some(BindingValue::Float(OrderedFloat(arr.value(row))));
    }
    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
        return Some(BindingValue::Bool(arr.value(row)));
    }

    None
}

// ─────────────────────────────────────────────────────────────────────────────
// Binding check/extract utilities
// ─────────────────────────────────────────────────────────────────────────────

/// Check whether all `check_variables` on a transition match the
/// bound values in the current track.
///
/// Returns `true` if all check variables match. Returns `false` if:
/// - Any event value is NULL (per §6.2: NULL fails predicate)
/// - Any event value doesn't equal the bound value
/// - Any required variable is not yet bound
#[inline]
pub fn check_bindings(
    check_variables: &[usize],
    bound_values: &[Option<BindingValue>],
    cache: &BindingValueCache,
    row: usize,
) -> bool {
    for &var_idx in check_variables {
        let bound = match &bound_values[var_idx] {
            Some(v) => v,
            None => return false,
        };
        let event_val = match cache.get(var_idx, row) {
            Some(v) => v,
            None => return false,
        };
        if bound != event_val {
            return false;
        }
    }
    true
}

/// Attempt to bind variables for `bind_variables` on a transition.
///
/// For each variable in `bind_variables`:
/// - If already bound (bind-once semantics), skip.
/// - If the event value is NULL, return `false` (no track created
///   per §6.2).
/// - Otherwise, store the extracted value.
///
/// Returns `true` on success, `false` if any required extraction
/// produced NULL. On `false`, `bound_values` may be partially
/// modified — the caller should discard the track.
pub fn try_bind_variables(
    bind_variables: &[usize],
    bound_values: &mut [Option<BindingValue>],
    cache: &BindingValueCache,
    row: usize,
) -> bool {
    for &var_idx in bind_variables {
        if bound_values[var_idx].is_some() {
            continue;
        }
        match cache.get(var_idx, row) {
            Some(v) => {
                bound_values[var_idx] = Some(v.clone());
            }
            None => return false,
        }
    }
    true
}

// ─────────────────────────────────────────────────────────────────────────────
// BindingTrack
// ─────────────────────────────────────────────────────────────────────────────

/// A single binding track: independent NFA state for one binding value
/// combination.
///
/// Per sequence-matching.md §8.2: each distinct binding value creates
/// an independent match track. Conceptually `(entity_id, binding_key)`
/// is the effective entity key for the NFA.
#[derive(Debug)]
pub struct BindingTrack {
    /// Bound variable values for this track. Indexed by variable
    /// binding index. `Some` once bound, `None` until bound.
    pub bound_values: Vec<Option<BindingValue>>,
    /// Per-track NFA candidate state.
    pub nfa_state: EntityNfaState,
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityBindingState
// ─────────────────────────────────────────────────────────────────────────────

/// Default maximum number of binding tracks per entity.
///
/// Independent from the per-track candidate active-state limit
/// (`DEFAULT_ACTIVE_STATE_LIMIT`). This bounds the number of distinct
/// binding value combinations tracked simultaneously.
pub const DEFAULT_ACTIVE_TRACK_LIMIT: usize = 1_000;

/// Per-entity binding track state. Manages all tracks for one entity.
///
/// For patterns without variable bindings, there is exactly one default
/// track. The `EntityBindingState` degenerates to a single
/// `EntityNfaState` in that case, keeping overhead minimal.
#[derive(Debug)]
pub struct EntityBindingState {
    /// Active tracks, keyed by binding value combination.
    tracks: Vec<BindingTrack>,
    /// Fast lookup from binding key to track index in `tracks`.
    track_index: HashMap<BindingKey, usize>,
    /// Maximum tracks per entity.
    active_track_limit: usize,
    /// Number of variable bindings (length of bound_values per track).
    num_variables: usize,
    /// Number of NFA states (for creating new EntityNfaState).
    num_nfa_states: usize,
    /// Tracks dropped due to the active track limit.
    dropped_track_count: u64,
}

impl EntityBindingState {
    /// Create state for a new entity.
    pub fn new(num_variables: usize, num_nfa_states: usize) -> Self {
        Self {
            tracks: Vec::new(),
            track_index: HashMap::new(),
            active_track_limit: DEFAULT_ACTIVE_TRACK_LIMIT,
            num_variables,
            num_nfa_states,
            dropped_track_count: 0,
        }
    }

    /// Override the active track limit.
    pub fn with_active_track_limit(mut self, limit: usize) -> Self {
        self.active_track_limit = limit;
        self
    }

    /// Number of active tracks.
    pub fn num_tracks(&self) -> usize {
        self.tracks.len()
    }

    /// Number of tracks dropped due to the limit.
    pub fn dropped_track_count(&self) -> u64 {
        self.dropped_track_count
    }

    /// Total candidates across all tracks.
    pub fn total_candidates(&self) -> usize {
        self.tracks
            .iter()
            .map(|t| t.nfa_state.total_candidates())
            .sum()
    }

    /// Iterate over all tracks.
    pub fn tracks(&self) -> &[BindingTrack] {
        &self.tracks
    }

    /// Iterate over all tracks mutably.
    pub fn tracks_mut(&mut self) -> &mut [BindingTrack] {
        &mut self.tracks
    }

    /// Look up or create a track for the given binding key.
    ///
    /// Returns `None` if the track limit has been reached and this is a
    /// new key.
    pub fn get_or_create_track(&mut self, key: &BindingKey) -> Option<&mut BindingTrack> {
        if let Some(&idx) = self.track_index.get(key) {
            return Some(&mut self.tracks[idx]);
        }

        if self.tracks.len() >= self.active_track_limit {
            self.dropped_track_count += 1;
            return None;
        }

        let idx = self.tracks.len();
        self.tracks.push(BindingTrack {
            bound_values: vec![None; self.num_variables],
            nfa_state: EntityNfaState::new(self.num_nfa_states),
        });
        self.track_index.insert(key.clone(), idx);
        Some(&mut self.tracks[idx])
    }

    /// Find an existing track by binding key.
    pub fn find_track(&self, key: &BindingKey) -> Option<&BindingTrack> {
        self.track_index.get(key).map(|&idx| &self.tracks[idx])
    }

    /// Find an existing track by binding key (mutable).
    pub fn find_track_mut(&mut self, key: &BindingKey) -> Option<&mut BindingTrack> {
        self.track_index
            .get(key)
            .copied()
            .map(move |idx| &mut self.tracks[idx])
    }

    /// Collect completions from all tracks.
    pub fn all_completions(&self) -> Vec<(BindingKey, MatchCompletion)> {
        let mut result = Vec::new();
        for (key, &idx) in &self.track_index {
            for completion in self.tracks[idx].nfa_state.completions() {
                result.push((key.clone(), completion.clone()));
            }
        }
        result
    }

    /// Collect partial matches from all tracks (for EMIT ALL).
    pub fn all_partials(&self) -> Vec<(BindingKey, PartialMatch)> {
        let mut result = Vec::new();
        for (key, &idx) in &self.track_index {
            for partial in self.tracks[idx].nfa_state.partials() {
                result.push((key.clone(), partial.clone()));
            }
        }
        result
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Binding-aware NFA processing
// ─────────────────────────────────────────────────────────────────────────────

impl NfaSimulator {
    /// Process a sub-batch with variable binding track routing.
    ///
    /// This is the binding-aware counterpart of [`NfaSimulator::process_batch`].
    /// For each event:
    /// 1. Process existing tracks (Phase 1 + Phase 3 per track, with
    ///    binding checks on transitions).
    /// 2. Handle new candidate creation (Phase 2) with binding extraction
    ///    and track routing.
    pub fn process_batch_with_bindings(
        &self,
        binding_state: &mut EntityBindingState,
        batch: &RecordBatch,
        event_type_col: &str,
        timestamp_col: &str,
    ) {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        let event_types = match batch.column_by_name(event_type_col) {
            Some(col) => match col.as_any().downcast_ref::<StringViewArray>() {
                Some(arr) => arr,
                None => return,
            },
            None => return,
        };

        let timestamps = match super::nfa::resolve_timestamp_column(batch, timestamp_col) {
            Some(ts) => ts,
            None => return,
        };

        let pred_cache = self.build_predicate_cache(batch);
        let binding_cache = BindingValueCache::build(&self.nfa().variable_bindings, batch);

        #[allow(clippy::needless_range_loop)] // `row` indexes into multiple arrays + batch
        for row in 0..num_rows {
            if event_types.is_null(row) {
                continue;
            }
            let event_type = event_types.value(row);
            let event_ts = timestamps[row];

            self.process_event_with_bindings(
                binding_state,
                event_type,
                event_ts,
                row,
                &pred_cache,
                &binding_cache,
            );
        }
    }

    /// Process one event through all binding tracks.
    fn process_event_with_bindings(
        &self,
        binding_state: &mut EntityBindingState,
        event_type: &str,
        event_ts: i64,
        row: usize,
        pred_cache: &PredicateCache,
        binding_cache: &BindingValueCache,
    ) {
        let nfa = self.nfa();

        if !nfa.relevant_event_types.contains(event_type) {
            return;
        }

        // ── Phase 1 + Phase 3: process existing tracks ──
        for track in binding_state.tracks.iter_mut() {
            Self::process_event_for_track(
                self,
                &mut track.nfa_state,
                &mut track.bound_values,
                event_type,
                event_ts,
                row,
                pred_cache,
                binding_cache,
            );
        }

        // ── Phase 2: start new candidates with binding routing ──
        let start_state = &nfa.states[0];
        for (t_idx, transition) in start_state.transitions.iter().enumerate() {
            if transition.event_type != event_type {
                continue;
            }
            if !self.check_transition_predicates(0, t_idx, transition, row, pred_cache) {
                continue;
            }

            // Extract binding values for this transition.
            let mut new_bound = vec![None; binding_state.num_variables];
            if !try_bind_variables(
                &transition.bind_variables,
                &mut new_bound,
                binding_cache,
                row,
            ) {
                continue; // NULL → no track
            }

            // Check variables that need checking at step 0 (rare but possible).
            if !check_bindings(&transition.check_variables, &new_bound, binding_cache, row) {
                continue;
            }
            if !self.can_start_entry(event_ts) {
                continue;
            }

            // Build positional key from all variable slots. Variables
            // that bind at later steps are represented as a sentinel
            // value (`BindingValue::Int(0)`) to ensure each step-0
            // binding combination produces a distinct key. Late-bound
            // variables will be filled in by `try_bind_variables`
            // during Phase 1c of subsequent events.
            let key: BindingKey = new_bound
                .iter()
                .map(|v| v.clone().unwrap_or(BindingValue::Int(0)))
                .collect();

            let track = match binding_state.get_or_create_track(&key) {
                Some(t) => t,
                None => continue, // Track limit reached
            };

            // Set bound values on track (idempotent for existing tracks).
            for (i, val) in new_bound.into_iter().enumerate() {
                if val.is_some() && track.bound_values[i].is_none() {
                    track.bound_values[i] = val;
                }
            }

            // Check scan_from for MATCH ALL.
            if let Some(scan_from) = track.nfa_state.scan_from() {
                if event_ts <= scan_from {
                    continue;
                }
            }

            let new_entry = CandidateEntry {
                anchor_ts: event_ts,
                last_step_ts: event_ts,
            };
            track
                .nfa_state
                .insert_candidate(transition.target as usize, new_entry);

            let step = nfa.state_to_step[transition.target as usize];
            track.nfa_state.update_max_step_reached(step);
        }
    }

    /// Process one event for a single binding track (Phase 1 + Phase 3).
    ///
    /// This handles expiry, poison, forward transitions with binding
    /// checks (and late binding for `bind_variables` on intermediate
    /// steps), accept state handling, and active state limit enforcement.
    ///
    /// `bound_values` is mutable to support late binding: variables
    /// whose `bind_step` is beyond step 0 are bound when the first
    /// matching transition at that step fires.
    #[allow(clippy::too_many_arguments)]
    fn process_event_for_track(
        &self,
        state: &mut EntityNfaState,
        bound_values: &mut [Option<BindingValue>],
        event_type: &str,
        event_ts: i64,
        row: usize,
        pred_cache: &PredicateCache,
        binding_cache: &BindingValueCache,
    ) {
        let nfa = self.nfa();
        let accept = nfa.accept_state as usize;

        // For MATCH FIRST: if already completed, skip.
        if !self.is_match_all() && !state.completions().is_empty() {
            return;
        }

        // ── Phase 1: Process existing active states (reverse order) ──
        state.clear_propagation_buf();

        let num_states = nfa.states.len();
        for state_idx in (0..num_states).rev() {
            if state_idx == accept {
                continue;
            }

            let nfa_state = &nfa.states[state_idx];

            // Phase 1a: Expire.
            if let Some(window) = nfa.global_window {
                self.expire_candidates(state, state_idx, event_ts, window);
            }

            // Phase 1b: Poison.
            if self.check_poison_transitions(state_idx, nfa_state, event_type, row, pred_cache) {
                self.kill_candidates(state, state_idx);
            }

            // Phase 1c: Forward transitions with binding checks.
            for (t_idx, transition) in nfa_state.transitions.iter().enumerate() {
                if transition.event_type != event_type {
                    continue;
                }
                if !self.check_transition_predicates(state_idx, t_idx, transition, row, pred_cache)
                {
                    continue;
                }
                // Variable binding check.
                if !check_bindings(
                    &transition.check_variables,
                    bound_values,
                    binding_cache,
                    row,
                ) {
                    continue;
                }
                // Late binding: bind variables at intermediate steps.
                if !try_bind_variables(&transition.bind_variables, bound_values, binding_cache, row)
                {
                    continue; // NULL in bind column → no match
                }

                // Propagate eligible candidates.
                state.propagate_candidates(
                    state_idx,
                    transition.target,
                    event_ts,
                    nfa.global_window,
                );
            }
        }

        state.apply_propagations();

        // ── Phase 3: Check accept ──
        self.check_accept_state(state);

        // ── Active state limit ──
        self.enforce_active_state_limit(state);

        // Track max step reached.
        state.update_max_step_from_deques(&nfa.state_to_step);
    }
}

// Re-import PredicateCache from nfa module.
use super::nfa::PredicateCache;

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    // ── OrderedFloat tests ──────────────────────────────────────────────

    #[test]
    fn ordered_float_nan_eq() {
        let a = OrderedFloat(f64::NAN);
        let b = OrderedFloat(f64::NAN);
        assert_eq!(a, b);
    }

    #[test]
    fn ordered_float_nan_hash_consistent() {
        use std::hash::DefaultHasher;

        let hash = |f: OrderedFloat| {
            let mut h = DefaultHasher::new();
            f.hash(&mut h);
            h.finish()
        };

        assert_eq!(hash(OrderedFloat(f64::NAN)), hash(OrderedFloat(f64::NAN)));
        // Different NaN bit patterns should also hash the same.
        let neg_nan = OrderedFloat(f64::from_bits(0xfff8_0000_0000_0000));
        assert!(neg_nan.0.is_nan());
        assert_eq!(hash(OrderedFloat(f64::NAN)), hash(neg_nan));
    }

    #[test]
    fn ordered_float_zero_eq() {
        let pos = OrderedFloat(0.0);
        let neg = OrderedFloat(-0.0);
        assert_eq!(pos, neg);
    }

    #[test]
    fn ordered_float_zero_hash_consistent() {
        use std::hash::DefaultHasher;

        let hash = |f: OrderedFloat| {
            let mut h = DefaultHasher::new();
            f.hash(&mut h);
            h.finish()
        };

        assert_eq!(hash(OrderedFloat(0.0)), hash(OrderedFloat(-0.0)));
    }

    #[test]
    fn ordered_float_normal_values() {
        assert_eq!(OrderedFloat(1.0), OrderedFloat(1.0));
        assert_ne!(OrderedFloat(1.0), OrderedFloat(2.0));
        assert!(OrderedFloat(1.0) < OrderedFloat(2.0));
    }

    #[test]
    fn ordered_float_nan_sorts_last() {
        assert!(OrderedFloat(f64::MAX) < OrderedFloat(f64::NAN));
        assert!(OrderedFloat(f64::NEG_INFINITY) < OrderedFloat(f64::NAN));
    }

    // ── BindingValue tests ──────────────────────────────────────────────

    #[test]
    fn binding_value_eq_different_types() {
        assert_ne!(BindingValue::Int(1), BindingValue::Timestamp(1));
        assert_ne!(BindingValue::Int(1), BindingValue::Float(OrderedFloat(1.0)));
        assert_ne!(BindingValue::String("1".into()), BindingValue::Int(1),);
    }

    #[test]
    fn binding_value_hash_consistency() {
        use std::hash::DefaultHasher;

        let hash = |v: &BindingValue| {
            let mut h = DefaultHasher::new();
            v.hash(&mut h);
            h.finish()
        };

        let a = BindingValue::String("hello".into());
        let b = BindingValue::String("hello".into());
        assert_eq!(a, b);
        assert_eq!(hash(&a), hash(&b));

        let c = BindingValue::Float(OrderedFloat(f64::NAN));
        let d = BindingValue::Float(OrderedFloat(f64::NAN));
        assert_eq!(c, d);
        assert_eq!(hash(&c), hash(&d));
    }

    #[test]
    fn binding_value_as_hashmap_key() {
        let mut map: HashMap<BindingValue, i32> = HashMap::new();
        map.insert(BindingValue::String("free".into()), 1);
        map.insert(BindingValue::String("pro".into()), 2);
        map.insert(BindingValue::Int(42), 3);

        assert_eq!(map.get(&BindingValue::String("free".into())), Some(&1));
        assert_eq!(map.get(&BindingValue::String("pro".into())), Some(&2));
        assert_eq!(map.get(&BindingValue::Int(42)), Some(&3));
        assert_eq!(map.get(&BindingValue::Int(99)), None);
    }

    // ── BindingKey tests ────────────────────────────────────────────────

    #[test]
    fn binding_key_identity() {
        let key1: BindingKey =
            smallvec::smallvec![BindingValue::String("free".into()), BindingValue::Int(1),];
        let key2: BindingKey =
            smallvec::smallvec![BindingValue::String("free".into()), BindingValue::Int(1),];
        let key3: BindingKey =
            smallvec::smallvec![BindingValue::String("pro".into()), BindingValue::Int(1),];

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn binding_key_in_hashmap() {
        let mut map: HashMap<BindingKey, i32> = HashMap::new();
        let key: BindingKey = smallvec::smallvec![BindingValue::String("free".into())];
        map.insert(key.clone(), 42);
        assert_eq!(map.get(&key), Some(&42));
    }

    #[test]
    fn build_binding_key_all_bound() {
        let values = vec![
            Some(BindingValue::Int(1)),
            Some(BindingValue::String("test".into())),
        ];
        let key = build_binding_key(&values);
        assert!(key.is_some());
        assert_eq!(key.unwrap().len(), 2);
    }

    #[test]
    fn build_binding_key_missing_value() {
        let values = vec![Some(BindingValue::Int(1)), None];
        assert!(build_binding_key(&values).is_none());
    }

    // ── BindingValueCache tests ─────────────────────────────────────────

    fn make_binding_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("plan", DataType::Utf8View, true),
            Field::new("amount", DataType::Int64, true),
            Field::new("score", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec![Some("free"), None, Some("pro")])),
                Arc::new(Int64Array::from(vec![Some(100), Some(200), None])),
                Arc::new(Float64Array::from(vec![1.5, 2.5, f64::NAN])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![1000, 2000, 3000]).with_timezone("UTC"),
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn cache_extracts_string_values() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        assert_eq!(cache.get(0, 0), Some(&BindingValue::String("free".into())));
        assert_eq!(cache.get(0, 1), None); // NULL
        assert_eq!(cache.get(0, 2), Some(&BindingValue::String("pro".into())));
    }

    #[test]
    fn cache_extracts_int_values() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "amount".into(),
            source_column: "amount".into(),
            column_index: 1,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        assert_eq!(cache.get(0, 0), Some(&BindingValue::Int(100)));
        assert_eq!(cache.get(0, 1), Some(&BindingValue::Int(200)));
        assert_eq!(cache.get(0, 2), None); // NULL
    }

    #[test]
    fn cache_extracts_float_values() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "score".into(),
            source_column: "score".into(),
            column_index: 2,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        assert_eq!(
            cache.get(0, 0),
            Some(&BindingValue::Float(OrderedFloat(1.5)))
        );
        assert_eq!(
            cache.get(0, 2),
            Some(&BindingValue::Float(OrderedFloat(f64::NAN)))
        );
    }

    #[test]
    fn cache_extracts_bool_values() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "active".into(),
            source_column: "active".into(),
            column_index: 3,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        assert_eq!(cache.get(0, 0), Some(&BindingValue::Bool(true)));
        assert_eq!(cache.get(0, 1), Some(&BindingValue::Bool(false)));
    }

    #[test]
    fn cache_extracts_timestamp_values() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "ts".into(),
            source_column: "ts".into(),
            column_index: 4,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        assert_eq!(cache.get(0, 0), Some(&BindingValue::Timestamp(1000)));
    }

    #[test]
    fn cache_missing_column() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "nonexistent".into(),
            source_column: "nonexistent".into(),
            column_index: 99,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        assert_eq!(cache.get(0, 0), None);
        assert_eq!(cache.get(0, 1), None);
    }

    // ── check_bindings tests ────────────────────────────────────────────

    #[test]
    fn check_bindings_match() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let bound = vec![Some(BindingValue::String("free".into()))];
        assert!(check_bindings(&[0], &bound, &cache, 0));
    }

    #[test]
    fn check_bindings_mismatch() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let bound = vec![Some(BindingValue::String("pro".into()))];
        assert!(!check_bindings(&[0], &bound, &cache, 0)); // row 0 is "free"
    }

    #[test]
    fn check_bindings_null_event_value() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let bound = vec![Some(BindingValue::String("free".into()))];
        assert!(!check_bindings(&[0], &bound, &cache, 1)); // row 1 is NULL
    }

    #[test]
    fn check_bindings_unbound_variable() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let bound = vec![None];
        assert!(!check_bindings(&[0], &bound, &cache, 0));
    }

    #[test]
    fn check_bindings_empty_variables() {
        let batch = make_binding_batch();
        let defs = vec![];
        let cache = BindingValueCache::build(&defs, &batch);
        let bound: Vec<Option<BindingValue>> = vec![];

        assert!(check_bindings(&[], &bound, &cache, 0));
    }

    // ── try_bind_variables tests ────────────────────────────────────────

    #[test]
    fn try_bind_success() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let mut bound = vec![None];
        assert!(try_bind_variables(&[0], &mut bound, &cache, 0));
        assert_eq!(bound[0], Some(BindingValue::String("free".into())));
    }

    #[test]
    fn try_bind_null_fails() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let mut bound = vec![None];
        assert!(!try_bind_variables(&[0], &mut bound, &cache, 1)); // row 1 NULL
    }

    #[test]
    fn try_bind_once_semantics() {
        let batch = make_binding_batch();
        let defs = vec![VariableBindingDef {
            name: "plan".into(),
            source_column: "plan".into(),
            column_index: 0,
            bind_step: 0,
        }];
        let cache = BindingValueCache::build(&defs, &batch);

        let mut bound = vec![Some(BindingValue::String("free".into()))];
        // Already bound — should skip and succeed.
        assert!(try_bind_variables(&[0], &mut bound, &cache, 2));
        // Value unchanged (still "free", not "pro").
        assert_eq!(bound[0], Some(BindingValue::String("free".into())));
    }

    // ── EntityBindingState tests ────────────────────────────────────────

    #[test]
    fn entity_state_create_track() {
        let mut state = EntityBindingState::new(1, 3);
        let key: BindingKey = smallvec::smallvec![BindingValue::String("free".into())];

        let track = state.get_or_create_track(&key);
        assert!(track.is_some());
        assert_eq!(state.num_tracks(), 1);
    }

    #[test]
    fn entity_state_reuse_track() {
        let mut state = EntityBindingState::new(1, 3);
        let key: BindingKey = smallvec::smallvec![BindingValue::String("free".into())];

        let _ = state.get_or_create_track(&key);
        let _ = state.get_or_create_track(&key);
        // Same key → same track, not two tracks.
        assert_eq!(state.num_tracks(), 1);
    }

    #[test]
    fn entity_state_separate_tracks() {
        let mut state = EntityBindingState::new(1, 3);
        let key1: BindingKey = smallvec::smallvec![BindingValue::String("free".into())];
        let key2: BindingKey = smallvec::smallvec![BindingValue::String("pro".into())];

        let _ = state.get_or_create_track(&key1);
        let _ = state.get_or_create_track(&key2);
        assert_eq!(state.num_tracks(), 2);
    }

    #[test]
    fn entity_state_track_limit() {
        let mut state = EntityBindingState::new(1, 3).with_active_track_limit(2);

        let key1: BindingKey = smallvec::smallvec![BindingValue::Int(1)];
        let key2: BindingKey = smallvec::smallvec![BindingValue::Int(2)];
        let key3: BindingKey = smallvec::smallvec![BindingValue::Int(3)];

        assert!(state.get_or_create_track(&key1).is_some());
        assert!(state.get_or_create_track(&key2).is_some());
        // Third track exceeds limit.
        assert!(state.get_or_create_track(&key3).is_none());
        assert_eq!(state.num_tracks(), 2);
        assert_eq!(state.dropped_track_count(), 1);
    }

    #[test]
    fn entity_state_find_track() {
        let mut state = EntityBindingState::new(1, 3);
        let key: BindingKey = smallvec::smallvec![BindingValue::Int(42)];

        assert!(state.find_track(&key).is_none());
        let _ = state.get_or_create_track(&key);
        assert!(state.find_track(&key).is_some());
    }
}
