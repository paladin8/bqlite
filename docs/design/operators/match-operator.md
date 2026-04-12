# MATCH Operator Architecture

**Wave**: 3
**Task**: TASK-301
**Status**: draft
**Depends on**: sequence-matching.md, execution-model.md, operator-traits.md, type-system.md, planner-pipeline.md
**Depended on by**: TASK-302 (strategy selection), TASK-304 (NFA runtime), TASK-306 (variable bindings), TASK-309 (lowering), TASK-311 (pattern compiler), TASK-321 (SequenceMatchOperator)

---

## 1. Purpose

This document connects the algorithmic specification in sequence-matching.md to a concrete `EntityOperator` implementation. It pins down:

- The operator state layout — per-entity NFA candidate deques and step-counter tracks.
- `EntityOperator` trait integration — entity boundary detection, `finish_entity()` semantics, sub-batch streaming.
- Output schema — `entity_id`, `$var` binding columns, step-property columns, `step_reached`, `match_duration`, `match_events`.
- Emission points — match completion, window expiry, entity end.
- Active-state cap — the safety valve, default limit, and typed error on overflow.
- Demand-driven column reduction — how the planner avoids materializing unreferenced step properties.

The document does **not** cover strategy selection (TASK-302), pattern compilation (TASK-311), or the NFA transition algorithm details (sequence-matching.md §3.3) — those are owned by their respective tasks and design docs.

---

## 2. Operator Identity

The MATCH operator is the `SequenceMatchOperator`, a stateful per-entity operator that implements `EntityOperator` (operator-traits.md §6). It is the single operator that evaluates MATCH pipeline stages. FUNNEL and RETENTION desugar into MATCH + STATS compositions during logical planning (query-language.md §4, type-system.md §6.2) and never instantiate a separate operator.

**Crate**: `bqlite-operators` (in `src/matcher/` module tree).

**Trait implementation**:

```rust
impl EntityOperator for SequenceMatchOperator {
    type State = SequenceMatchState;
    // ...
}
```

The operator itself (`&self`) is immutable — it carries the compiled pattern, execution configuration, and output schema. All mutable state lives in `SequenceMatchState`, created fresh per entity.

---

## 3. Operator Construction

`SequenceMatchOperator` is constructed by the engine bind step from a `SequenceMatchPhysical` descriptor (a plain-data struct produced by the physical planner, living in `bqlite-planner`). The constructor receives:

```rust
pub struct SequenceMatchOperator {
    /// Compiled NFA program (immutable, shared across shard-tasks via Arc).
    compiled_nfa: Arc<CompiledNfa>,

    /// Pattern classification (determines which state variant to use).
    pattern_class: PatternClass,

    /// Match mode: FIRST or ALL.
    match_mode: MatchMode,

    /// Whether EMIT ALL is enabled.
    emit_all: bool,

    /// Execution configuration derived from demand propagation.
    exec_config: MatchExecutionConfig,

    /// Output schema for this operator's results.
    output_schema: OperatorSchema,

    /// Column indices into the input batch, resolved once at construction.
    input_columns: InputColumnMap,

    /// Active state limit per entity (default 10,000).
    active_state_limit: u32,
}
```

### 3.1 InputColumnMap

Column indices are resolved once at construction from the input's `OperatorSchema`, not per-batch:

```rust
pub struct InputColumnMap {
    /// Index of `entity_id` in the input batch.
    pub entity_id_idx: usize,
    /// Index of `ts` (timestamp) in the input batch.
    pub ts_idx: usize,
    /// Index of `event_type` in the input batch.
    pub event_type_idx: usize,
    /// Indices for columns referenced by step predicates.
    pub predicate_columns: Vec<PredicateColumnRef>,
    /// Indices for columns referenced by variable bindings.
    pub binding_columns: Vec<BindingColumnRef>,
    /// Indices for columns referenced by step-property forwarding.
    pub step_property_columns: Vec<StepPropertyColumnRef>,
}

pub struct PredicateColumnRef {
    pub column_idx: usize,
    pub step_indices: Vec<u8>,
}

pub struct BindingColumnRef {
    pub column_idx: usize,
    pub variable_idx: usize,
    pub bql_type: BqlType,
}

pub struct StepPropertyColumnRef {
    pub column_idx: usize,
    pub step_index: u8,
    pub output_column_name: String,
    pub bql_type: BqlType,
}
```

Columns are looked up by name through `OperatorSchema` (execution-model.md §3.7) at construction, never by position at runtime.

### 3.2 MatchExecutionConfig

The layered extraction configuration (execution-model.md §4.2) controls which optional outputs the operator materializes:

```rust
pub struct MatchExecutionConfig {
    /// Whether to track and emit `match_duration`.
    pub track_match_duration: bool,
    /// Whether to track and emit `match_events`.
    pub track_match_events: bool,
    /// Per-(step, column) extractions for named step property forwarding.
    pub step_properties: Vec<StepPropertyExtraction>,
    /// Fused accumulator, if aggregation fusion is active.
    pub fused_accumulator: Option<Box<dyn Accumulator>>,
}

pub struct StepPropertyExtraction {
    /// Step index in the pattern (0-indexed).
    pub step_index: u8,
    /// Column name in the source event's schema.
    pub column_name: String,
    /// BQL type of the extracted value.
    pub bql_type: BqlType,
}
```

The physical planner populates `MatchExecutionConfig` during demand propagation (planner-pipeline.md §7.5, §9.4). Toggle flags are evaluated only at match/session completion, never in the per-event hot loop (execution-model.md §4.2).

---

## 4. Per-Entity State Layout

### 4.1 State Variant Selection

The state variant is fixed at plan time based on pattern classification and demand:

```rust
/// Per-entity state. Variant selected at plan time based on PatternClass + demand.
pub enum SequenceMatchState {
    /// Step counter fast path: linear patterns without match detail demand.
    StepCounter(StepCounterState),
    /// Full NFA simulation: general patterns or match detail demand.
    Nfa(NfaEntityState),
}
```

All entities within a single query use the same variant. The variant is determined by the `PatternClass` (sequence-matching.md §10.1) and the `MatchExecutionConfig`:

| Condition | Variant |
|---|---|
| `PatternClass::LinearSimple` and no match-detail demand | `StepCounter` |
| `PatternClass::LinearWithNegation` and no match-detail demand | `StepCounter` |
| `PatternClass::LinearWithBindings` and no match-detail demand | `StepCounter` |
| `PatternClass::LinearFull` and no match-detail demand | `StepCounter` |
| `PatternClass::LinearImmediate` | `StepCounter` (dedicated consecutive variant) |
| `PatternClass::GeneralNfa` | `Nfa` |
| Any pattern class with `track_match_events: true` | `Nfa` |
| Any linear class with `track_match_duration: true` but no step properties | `StepCounter` (duration is `last_step_ts - anchor_ts`) |

TASK-302 (matcher-strategy.md) specifies the full strategy selection matrix. This document pins down the state layouts that those strategies populate.

### 4.2 StepCounterState

For linear patterns (the common funnel case). See sequence-matching.md §10.3.

```rust
pub struct StepCounterState {
    /// One counter per active binding track.
    tracks: SmallVec<[StepCounterTrack; 4]>,
    /// Step 0 candidate deque for window rebinding (Section 4.2 of
    /// sequence-matching.md). Shared across tracks before bindings are
    /// established.
    step0_candidates: ArrayVec<CandidateEntry, 8>,
    /// Total active candidate count across all tracks (for active-state cap).
    active_candidate_count: u32,
    /// Whether the active-state cap was hit for this entity.
    cap_exceeded: bool,
    /// Number of candidates dropped due to the active-state cap (for warning).
    dropped_count: u64,
}

pub struct StepCounterTrack {
    /// Bound variable values for this track.
    bindings: SmallVec<[BindingValue; 2]>,
    /// Current step in the linear pattern (0 = not started, 1..num_steps).
    current_step: u8,
    /// Anchor timestamp from the first step (for global window check).
    anchor_ts: i64,
    /// Timestamp of the event at the current step (for strict ordering).
    last_step_ts: i64,
    /// Completed match count (for fused counting / MATCH ALL).
    match_count: u32,
    /// Farthest step reached (for EMIT ALL).
    max_step_reached: u8,
    /// Restart point for MATCH ALL: ignore events with ts <= scan_from.
    scan_from: i64,
    /// Retained step-property values for demanded extractions.
    /// Indexed by position in `MatchExecutionConfig::step_properties`.
    /// Populated lazily — only when the corresponding step fires.
    retained_properties: SmallVec<[Option<PropertySlot>; 4]>,
}
```

**Size estimate**: Without bindings or step properties, `StepCounterTrack` is ~48 bytes. With two `BindingValue::String` entries and two step properties, ~200 bytes. For a single-track entity with no bindings, the entire `StepCounterState` fits in two cache lines.

### 4.3 NfaEntityState

For general patterns or match-detail demand. See sequence-matching.md §6.

```rust
pub struct NfaEntityState {
    /// One track per distinct binding value combination.
    tracks: SmallVec<[BindingTrack; 4]>,
    /// Total active candidate count across all tracks (for active-state cap).
    active_candidate_count: u32,
    /// Whether the active-state cap was hit for this entity.
    cap_exceeded: bool,
    /// Number of candidates dropped due to the active-state cap (for warning).
    dropped_count: u64,
}

pub struct BindingTrack {
    /// Bound variable values for this track.
    bindings: SmallVec<[BindingValue; 2]>,
    /// Per-state candidate deques.
    state_candidates: SmallVec<[StateCandidates; 8]>,
    /// Completed match count.
    match_count: u32,
    /// Farthest step reached (for EMIT ALL).
    max_step_reached: u8,
    /// Restart point for MATCH ALL.
    scan_from: i64,
    /// Retained step-property values. In the NFA path, properties are
    /// retained per completed match (not per candidate) — extracted at
    /// match completion from the matched event references.
    retained_properties: SmallVec<[Option<PropertySlot>; 4]>,
}

pub struct StateCandidates {
    pub state_id: u16,
    pub candidates: PendingTimestamps,
}

pub enum PendingTimestamps {
    /// Stack-allocated for the common case (<=4 candidates per state).
    Inline(ArrayVec<CandidateEntry, 4>),
    /// Heap-allocated for rare power-law entities.
    Spilled(VecDeque<CandidateEntry>),
}

#[derive(Debug, Clone, Copy)]
pub struct CandidateEntry {
    /// First step's timestamp (for window check).
    pub anchor_ts: i64,
    /// Timestamp of the event that advanced this candidate to current state.
    pub last_step_ts: i64,
}
```

### 4.4 BindingValue

Compact, comparison-optimized enum for the NFA hot path. Distinct from `bqlite_core::PropertyValue` (used at ingest).

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BindingValue {
    Bool(bool),
    Int(i64),
    Float(FloatOrd<f64>),
    String(CompactString),
    Timestamp(i64),
}
```

Lives in `bqlite-operators` (not `bqlite-core`) because it is used only by sequence match execution. See sequence-matching.md §6.2 for NULL/NaN handling.

### 4.5 PropertySlot

Retained step-property values for demand-driven extraction:

```rust
pub enum PropertySlot {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(CompactString),
    Timestamp(i64),
    Null,
}
```

`PropertySlot` differs from `BindingValue` in two ways: (1) it allows `Null` (source columns may be nullable), and (2) `Float` uses raw `f64` (no ordering wrapper needed — property slots are stored and forwarded, never hashed or compared).

---

## 5. EntityOperator Integration

### 5.1 create_state

```rust
fn create_state(&self, entity_id: &EntityId) -> SequenceMatchState {
    match self.pattern_class {
        PatternClass::GeneralNfa => SequenceMatchState::Nfa(NfaEntityState {
            tracks: SmallVec::new(),
            active_candidate_count: 0,
            cap_exceeded: false,
        }),
        _ if self.exec_config.track_match_events => {
            // Match-detail demand forces the NFA path even for linear patterns.
            SequenceMatchState::Nfa(NfaEntityState {
                tracks: SmallVec::new(),
                active_candidate_count: 0,
                cap_exceeded: false,
            })
        }
        _ => SequenceMatchState::StepCounter(StepCounterState {
            tracks: SmallVec::new(),
            step0_candidates: ArrayVec::new(),
            active_candidate_count: 0,
            cap_exceeded: false,
        }),
    }
}
```

State starts empty — no tracks, no candidates. Tracks are created lazily when step 1 matches. The `entity_id` argument is not stored in the state (operators that need per-entity warning attribution capture it in the warning channel, not in state).

### 5.2 process_sub_batch

For each sub-batch (one row-group, entity-aligned, up to 64K rows):

1. **Extract columns** from the input `RecordBatch` using `InputColumnMap` indices:
   - `event_type`: If `DictionaryArray<Int32, Utf8View>`, resolve pattern event-type strings against the batch's dictionary once to build a per-batch code lookup table. If decoded (`StringViewArray`), use string comparison.
   - `ts`: `Int64Array` (nanosecond timestamps).
   - Predicate columns, binding columns, step-property columns as needed.

2. **Per-row loop** (the innermost hot loop):
   - For the `StepCounter` variant: load event type, compare against current step's expected type, check `ts > last_step_ts`, check window, advance step counter. ~5-10 instructions per non-matching event.
   - For the `Nfa` variant: run the three-phase NFA transition algorithm (sequence-matching.md §3.3) — expire, check poison, check forward transitions, start new candidates, dedup, check accept.

3. **Match completion handling**: When a candidate reaches the accept state:
   - **MATCH FIRST**: Set an early-exit flag for this track (stop processing events for this track). If single binding track, set entity-level early-exit.
   - **MATCH ALL**: Prune candidates that shared the consumed events, set
     `scan_from = anchor_ts`, increment `match_count`, and keep later
     unmatched entries alive.
   - In both modes, run the layered extraction hooks (§7.2).

4. **Active-state cap check**: After processing each event, if `active_candidate_count > active_state_limit`, drop oldest candidates from the front of deques until under the limit. Set `cap_exceeded = true`.

The per-row loop has **zero demand-related branches** (execution-model.md §4.2). All layered-extraction decisions (match duration, match events, step properties, fused accumulator updates) run only at match completion.

### 5.3 Sub-Batch Streaming

The `EntityOperatorAdapter` (execution-model.md §4.1) guarantees:
- Sub-batches for one entity arrive consecutively, no interleaving.
- Rows within a sub-batch are sorted by `(entity_id, ts)` ascending.
- The adapter drops each sub-batch's `RecordBatch` data before producing the next.

The `SequenceMatchState` persists across sub-batches. A match can span multiple sub-batches — step A may occur in sub-batch 1 and step B in sub-batch 5. NFA state is compact (tens to hundreds of bytes for typical patterns) and lives entirely in the `State` type.

For oversized entities (millions of events), the streaming contract ensures memory stays bounded: only one sub-batch is resident at a time, and per-entity state is bounded by the active-state cap (§8).

**Cancellation**: The MATCH operator does not check cancellation internally. Per operator-traits.md §5.2, cancellation is checked by the wrapping `EntityOperatorAdapter` between sub-batches. This keeps the per-event inner loop branch-free — the worst-case cancellation latency is one sub-batch (~64K rows).

### 5.4 finish_entity

Called exactly once per entity after all sub-batches have been processed. Consumes state.

```rust
fn finish_entity(&self, state: SequenceMatchState) -> Option<RecordBatch> {
    // 1. Emit remaining in-progress candidates (EMIT ALL only).
    // 2. Build output RecordBatch from completed matches and/or
    //    partial results.
    // 3. Return None if no output rows.
}
```

**Without EMIT ALL**: Returns `None` for entities with no completed matches. Returns a `RecordBatch` with one row per completed match — one row per binding track for MATCH FIRST, potentially multiple rows per binding track for MATCH ALL.

**With EMIT ALL**: Returns a `RecordBatch` for every binding track that entered the NFA (matched step 1). In-progress candidates that haven't expired or completed are emitted here with their current `step_reached`. Candidates that expired during `process_sub_batch` were already counted/emitted at expiry time (§6.3).

The return type is always `Option<RecordBatch>` with the demand-reduced output schema (§9).

### 5.5 finish_entity_into (Aggregation Fusion)

When the demand is aggregate-only and `fused_accumulator` is set, the adapter calls `finish_entity_into` instead of `finish_entity`, bypassing `RecordBatch` construction entirely:

```rust
fn finish_entity_into(&self, state: SequenceMatchState, acc: &mut dyn Accumulator) {
    match state {
        SequenceMatchState::StepCounter(sc) => {
            for track in sc.tracks {
                acc.update(group_key(&track).as_deref(), &track.reduced_values());
            }
        }
        SequenceMatchState::Nfa(nfa) => {
            for track in nfa.tracks {
                acc.update(group_key(&track).as_deref(), &track.reduced_values());
            }
        }
    }
}
```

This is the zero-materialization path. No per-entity `RecordBatch` is constructed. The `group_key` is constructed from bound variables or other GROUP BY fields. The `reduced_values` slice is laid out in `FusableAggregate::functions` order (see sequence-matching.md §13.4).

**Internal bookkeeping for fused paths** (sequence-matching.md §13.4):
- **Count of matches per entity**: `track.match_count`, passed as the sole value.
- **Funnel step counts**: Internal `[u64; num_steps]` array, incremented at match completion, window expiry (EMIT ALL), and entity end. Replayed into the accumulator as one `update` call per step.
- **Grouped counts**: Internal `HashMap<GroupKey, Counts>`, drained into `acc.update`.

### 5.6 required_columns

Returns the union of all input columns the operator reads:

```rust
fn required_columns(&self) -> &[String] {
    &self.required_column_names
}
```

The set is computed once at construction:
- `entity_id` — always (output).
- `event_type` — always (transition matching).
- `ts` — always (ordering and window checks).
- Columns referenced by step predicates.
- Columns referenced by variable bindings.
- Columns referenced by negation predicates.
- Columns referenced by demanded step-property extractions.

This drives demand propagation upstream — the scan layer only decodes these columns (late materialization).

### 5.7 supported_demands

```rust
fn supported_demands(&self) -> DemandCapabilities {
    // All pattern classes support all demand levels.
    // Strategy selection (TASK-302) determines the implementation.
    DemandCapabilities {
        supports_step_reached: true,
        supports_match_count: true,
        supports_full_detail: true,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: true,
    }
}
```

The `DemandCapabilities` type lives in `bqlite-planner` (or `bqlite-core` if the scaffold is already there). The operator advertises uniform capabilities; the physical planner uses these to decide whether to request fusion, step-property forwarding, or full match detail.

---

## 6. Emission Points

The operator emits output at three distinct points, each serving a different purpose:

### 6.1 Match Completion

When a candidate reaches the accept state. This is the primary emission point for both MATCH FIRST and MATCH ALL.

**What happens**:
1. Record `step_reached = num_steps` for this track.
2. Run layered extraction hooks (§7.2): compute `match_duration` if demanded, extract step properties if demanded, build `match_events` map if demanded.
3. **MATCH FIRST**: Mark the binding track as completed. No further events are processed for this track.
4. **MATCH ALL**: Prune only the candidates that used the consumed events,
   set `scan_from = anchor_ts`, increment `match_count`, and keep later
   unmatched entries alive for the next match.
5. In the fused path, update the accumulator directly. In the non-fused path, buffer the result row for `finish_entity`.

**Event consumption**: At match completion, consume the earliest eligible
anchor and the events that participated in that completed sequence
(sequence-matching.md §4.3). Newer unmatched entries must remain eligible.

### 6.2 Window Expiry

When a candidate's global window expires during the expire phase (sequence-matching.md §3.3, Phase 1). Only produces output when EMIT ALL is enabled.

**What happens**:
1. Compute `step_reached = state_to_step[current_state]` for the expiring candidate.
2. Update the track's `max_step_reached` if this candidate reached farther.
3. Drop the candidate from the deque.
4. In the fused path, increment `step_counts[step_reached]` directly — no buffering.

**Why emit at expiry**: Emitting at expiry enables incremental fused aggregation without holding state for expired candidates. The fused funnel path increments step counts at the moment of expiry, avoiding any retained state for expired candidates. This is the primary advantage of eager expiry emission.

### 6.3 Entity End

When `finish_entity()` is called. Handles remaining in-progress candidates.

**What happens**:
1. **Without EMIT ALL**: Only completed matches (buffered during match completion) are returned. In-progress candidates are silently dropped.
2. **With EMIT ALL**: Remaining in-progress candidates are normalized to the
   contract row shape before emission: one farthest partial per binding track
   for MATCH FIRST, or one farthest partial per surviving step-1 entry for
   MATCH ALL. For the fused path, accumulated counts plus these final
   partials are replayed into the accumulator.

---

## 7. Layered Extraction

### 7.1 Principle

The inner per-event loop has zero demand-related branches (execution-model.md §4.2). All feature toggles are evaluated only at match completion (§6.1), which is infrequent compared to the per-event transition hot path. This is load-bearing for the step-counter fast path: ~1-3 ns/event cannot afford per-event `if` checks for optional materialization.

### 7.2 Extraction Hooks

At match completion, the operator runs these hooks in order. The operator itself is `&self` (immutable, shared via `Arc`), so all mutable state — the output buffer, accumulated values — lives in the per-entity `State` or is passed as a parameter by the adapter:

```rust
/// Called by the per-event loop when a candidate reaches the accept state.
/// `config` is &self.exec_config (immutable). `track` and `output_buffer`
/// live in the per-entity State. `acc` is passed by the adapter when fused.
fn on_match_complete(
    config: &MatchExecutionConfig,
    track: &mut BindingTrack, // or StepCounterTrack
    output_buffer: &mut Vec<OutputRow>,
    acc: Option<&mut dyn Accumulator>,
) {
    // 1. match_duration (if demanded)
    if config.track_match_duration {
        let duration = track.last_step_ts - track.anchor_ts;
        // Store in output row
    }

    // 2. match_events (if demanded)
    if config.track_match_events {
        // Build Map(String -> Timestamp) from retained event references
        // Only available in the NFA path (requires path tracking)
    }

    // 3. Step properties (if any demanded)
    for extraction in &config.step_properties {
        // Read from track.retained_properties[extraction.step_index]
        // Store in output row
    }

    // 4. Fused accumulator (if active)
    if let Some(acc) = acc {
        // Reduced values are laid out in FusableAggregate::functions order;
        // see sequence-matching.md §13.4 for the concrete MATCH version.
        acc.update(group_key.as_deref(), &reduced_values);
    } else {
        output_buffer.push(/* ... */);
    }
}
```

### 7.3 Step-Property Retention

Step-property values are retained at the moment the corresponding step fires, not at match completion. This is because the matched event's `RecordBatch` may have been dropped by the time `finish_entity` runs (sub-batch streaming).

**In the StepCounter path**: When step N fires and step N has demanded properties, extract the values from the current sub-batch row and store them in `track.retained_properties[N]`. Values are `PropertySlot` (§4.5). Only demanded properties are extracted — the set is known at plan time.

**In the NFA path**: When a candidate transitions to a state that maps to a step with demanded properties, the row index within the current sub-batch is recorded in the candidate. At match completion, the property values are extracted from the retained row references. If the sub-batch has been dropped (match spans multiple sub-batches), the NFA path retains the property values eagerly at transition time, same as the StepCounter path.

**Implementation note**: Both paths use the same eager-retention strategy. The NFA path could theoretically defer extraction to match completion and retain row indices, but sub-batch streaming invalidates row indices across sub-batch boundaries. Eager retention at step-fire time is the only correct approach.

---

## 8. Active-State Cap

### 8.1 Design

Configurable maximum on the total number of `CandidateEntry` instances per entity, across all states and binding tracks. Default: **10,000**. See sequence-matching.md §16.1 for the motivating design and §16.2 for the entity event limit (a separate, upstream defense).

The limit is stored as a plain `u32` in `SequenceMatchOperator::active_state_limit` (§3), defaulting to `10_000` when not overridden by the physical plan descriptor.

### 8.2 Enforcement

After processing each event in the per-row loop, if `active_candidate_count > limit`:

1. Drop the oldest candidates from the front of deques, starting with the earliest anchors across all binding tracks. Iterate tracks by creation order, within each track iterate states by index, pop from the front of each deque.
2. Continue dropping until `active_candidate_count <= limit`.
3. Set `cap_exceeded = true` on the entity state.

### 8.3 Warning

When `cap_exceeded` is true at `finish_entity` time, the operator records a warning in the `WorkerContext` (execution-model.md §3.3):

```rust
QueryWarning::ActiveStateLimitExceeded {
    entity_id: String,
    dropped_count: u64,
    limit: u64,
}
```

The query continues with potentially degraded accuracy for that entity. This is a non-fatal warning, not an error — the query completes and the warning is surfaced in result metadata.

### 8.4 Memory Bound

Worst case: 10,000 candidates x 16 bytes (`CandidateEntry`) = 160 KB per entity for the deque model. With binding tracks and step properties, a realistic upper bound is ~500 KB per entity. This is well within per-thread memory bounds and is not tracked individually by `MemoryTracker`.

The `PendingTimestamps::Spilled` variant (heap `VecDeque`) is bounded by the same active-state limit — total candidates across all deques and tracks cannot exceed the cap.

---

## 9. Output Schema

### 9.1 Full Output Schema

The operator's output schema is the demand-reduced version of the full schema defined in type-system.md §6.1 and sequence-matching.md §12:

| Column | Arrow Type | Nullable | Present | Description |
|---|---|---|---|---|
| `entity_id` | `Utf8View` or `Int64` | no | Always | Entity key (type matches table schema) |
| `$var` | (per variable type) | no | When variables are bound | One column per bound variable, named by the variable |
| *step-property columns* | (resolved from source schema) | follows source | When downstream references `step_name.column` | One column per referenced named-step property |
| `step_reached` | `Int64` | no | When EMIT ALL is enabled | 1-indexed step number of the farthest step matched |
| `match_duration` | `Int64` | yes | When demanded | Nanoseconds between first and last matched step (NULL if `step_reached == 1`) |
| `match_events` | `Map(Utf8View, Timestamp(Nanosecond, UTC))` | yes | When demanded | Step name -> timestamp for each matched step |

### 9.2 Output by Mode

**MATCH FIRST (no EMIT ALL)**: One row per `(entity_id, bindings)` that completed the sequence. `step_reached` is omitted (implicitly equals `num_steps`).

**MATCH FIRST EMIT ALL**: One row per `(entity_id, bindings)` that entered the NFA. Includes both completed and incomplete sequences.

**MATCH ALL (no EMIT ALL)**: One row per completed match. An `(entity_id, bindings)` pair can produce multiple rows.

**MATCH ALL EMIT ALL**: One row per NFA entry (step 1 match) within each binding track. Entries whose window expired are accounted for at expiry time (§6.2).

### 9.3 Demand-Driven Schema Reduction

The physical planner propagates downstream `DemandSet` (planner-pipeline.md §9.3) to strip columns that are never read:

| Downstream demand | Schema reduction | Strategy implication |
|---|---|---|
| Only `entity_id` + aggregate | No match detail columns | Step counter or boolean match |
| `entity_id` + `step_reached` | Step counter, no match trace | Funnel fast path |
| `entity_id` + `step_reached` + step properties | Step counter + step-property forwarding | Retain only demanded properties |
| `match_events` | Full NFA with path tracking | Most expensive path |
| `match_duration` | Can use step counter (duration = `last_step_ts - anchor_ts`) | No full NFA needed |

The `finish_entity()` return batch's schema is always the demand-reduced version. Unreferenced columns are never computed or stored.

---

## 10. Dictionary Optimization for Event Type Matching

### 10.1 Per-Batch Dictionary Resolution

When the `event_type` column arrives as `DictionaryArray<Int32, Utf8View>` (the common single-segment case), the operator resolves pattern event-type strings against the batch's dictionary once at the start of `process_sub_batch`:

```rust
struct DictionaryCodeCache {
    /// For each NFA transition, the dictionary code that matches its
    /// event_type string. `None` if the event type is not in this
    /// batch's dictionary (transition can never fire for this batch).
    transition_codes: SmallVec<[Option<i32>; 8]>,
    /// For each poison transition, the matching dictionary code.
    poison_codes: SmallVec<[Option<i32>; 4]>,
}
```

In the per-row loop, event-type comparison is an integer comparison against cached dictionary codes — no string comparison in the hot path. This is reset per sub-batch because different segments may have different dictionary encodings.

### 10.2 Decoded String Fallback

When the `event_type` column arrives as `StringViewArray` (post k-way merge across segments), event-type comparison uses the 4-byte prefix in the view header for short-circuit (execution-model.md §3.7). Full string comparison only fires when the prefix matches, which is rare for distinct event type names.

---

## 11. Interaction with Other Operators

### 11.1 Upstream: Scan + Filter

The MATCH operator receives entity-aligned, timestamp-sorted `RecordBatch`es from the scan layer (via optional filter/project operators). The scan layer pushes event-type and property-predicate filters (sequence-matching.md §9) so the MATCH operator sees the minimum possible event stream.

### 11.2 Downstream: Stats / Select / OrderBy

The MATCH operator's output feeds into STATS (aggregation), SELECT (projection), WHERE (post-match filter), or ORDER BY. When the immediate downstream is STATS with fusable aggregates and no intervening row-level operator, the optimizer fuses the aggregate into the MATCH operator (§5.5).

### 11.3 FUNNEL and RETENTION Desugaring

FUNNEL and RETENTION are syntactic sugar that desugar into MATCH + STATS compositions during logical planning (type-system.md §6.2, query-language.md §4). The MATCH operator is unaware of whether it was instantiated from a direct MATCH query or from desugared FUNNEL/RETENTION. The desugared plan uses MATCH FIRST EMIT ALL + STATS step counts for funnels, and repeated MATCH queries across time brackets for retention.

---

## 12. Module Layout

The MATCH operator implementation lives in `crates/bqlite-operators/src/matcher/`:

```
matcher/
  mod.rs              -- Module root, SequenceMatchOperator struct + EntityOperator impl
  state.rs            -- SequenceMatchState, StepCounterState, NfaEntityState
  bindings.rs         -- BindingValue, BindingTrack (TASK-306)
  nfa.rs              -- NFA runtime simulator (TASK-304)
  step_counter.rs     -- Step counter fast path (TASK-305)
  output.rs           -- Output RecordBatch construction from state
  dictionary.rs       -- DictionaryCodeCache for per-batch event type resolution
```

Types that cross the planner-operator boundary:
- `CompiledNfa`, `PatternClass`, `NfaState`, `Transition`, `PoisonTransition` — live in `bqlite-planner` (plan-time compilation, TASK-311).
- `SequenceMatchPhysical` — the plain-data physical plan descriptor, lives in `bqlite-planner`.
- `MatchExecutionConfig`, `StepPropertyExtraction` — live in `bqlite-operators`. The physical planner in `bqlite-planner` produces an intermediate representation (`SequenceMatchPhysical`) containing demand flags and step-property descriptors as plain data. The engine bind step (in `bqlite-engine`, which depends on both `bqlite-planner` and `bqlite-operators`) converts this descriptor into the concrete `MatchExecutionConfig` when constructing the operator. This avoids a `bqlite-planner -> bqlite-operators` dependency.
- `DemandCapabilities`, `DemandSet` — live in `bqlite-planner` (plan-time demand propagation).

This respects the dependency direction: `bqlite-operators -> bqlite-planner` (for `CompiledNfa`, `PatternClass`), not the reverse.

---

## 13. Empty Entity Streams

Entities with zero events after scan-level filtering are skipped without initializing state. The `EntityOperatorAdapter` never calls `create_state()` for them — the entity boundary detection advances past the empty span (sequence-matching.md §16.4).

---

## 14. Decision Summary

| Aspect | Decision | Rationale |
|---|---|---|
| State variant selection | Plan-time, based on PatternClass + demand | No runtime branching between strategies |
| State layout | `StepCounterState` (linear) vs `NfaEntityState` (general) | Step counter is cache-friendly for the common case |
| Binding tracks | `SmallVec<[BindingTrack; 4]>` per entity | Most entities have 1-4 binding tracks; avoids heap allocation |
| Candidate storage | `ArrayVec<CandidateEntry, 4>` inline, `VecDeque` spill | 4 inline candidates = 64 bytes = one cache line |
| Step-property retention | Eager at step-fire time | Sub-batch streaming invalidates row indices across boundaries |
| Layered extraction | Branch only at match completion, never per-event | Preserves ~1-3 ns/event step counter performance |
| Active-state cap | 10,000 per entity, drop oldest | Bounds memory without aborting query |
| Dictionary optimization | Per-batch code cache, integer comparison in hot loop | Avoids string comparison per event |
| Output schema | Demand-reduced at plan time | Unreferenced columns never computed |
| Entity-id in state | Not stored | Warning attribution uses the warning channel, not state |
| Module tree | `matcher/` under `bqlite-operators/src/` | Clean separation from existing stateless operators |

---

## 15. Open Questions Deferred to Other Tasks

- **Strategy selection matrix**: Full matrix including IMMEDIATELY patterns, fusion combinations, and performance targets. Owned by TASK-302 (matcher-strategy.md).
- **NFA transition algorithm details**: Phase ordering, candidate dedup rules, poison transition evaluation. Specified in sequence-matching.md §3.3, implemented by TASK-304.
- **Pattern compiler pipeline**: AST -> NFA graph -> classification -> optimization. Owned by TASK-311.
- **Variable binding implementation**: Track creation/lookup, binding extraction, check semantics. Owned by TASK-306.
- **Logical lowering**: How the planner lowers `PipelineStage::Match` into a `SequenceMatchPhysical`. Owned by TASK-309.
- **`DemandCapabilities` real protocol**: The Wave 1 scaffold returns `DemandCapabilities::None`. The full protocol that populates the struct above is a Wave 4+ design task, with TASK-302 specifying the interim strategy-selection approach.
