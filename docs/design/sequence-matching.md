# Sequence Matching Design

> **Status**: DRAFT
> **Task**: TASK-004
> **Depends on**: TASK-005 (type system), TASK-001 (storage format), TASK-003 (execution model)
> **Depended on by**: TASK-002 (query language)

---

## 1. Design Goals

The sequence matcher is the most technically novel component of bqlite. It serves three constraints from [core-beliefs.md](../core-beliefs.md):

**Performance (Belief 1).** The matcher must process events at >100M events/sec for common linear patterns. NFA simulation uses Thompson's algorithm — O(n × m) time, no backtracking, no pathological cases. Tiered execution strategies ensure that simple patterns (the common case) pay no overhead for features they don't use. Cache-aware memory layout keeps the hot loop in L1.

**Powerful primitives (Belief 2).** The sequence matcher is the primitive that funnels, retention, and cohort analysis compose on top of. It supports ordered steps, time windows, negation, repetition, variable bindings, and two match modes. These compose freely — a funnel is a first-match sequence with per-step counting demand; retention is a sequence with bracket-based time windows; a conversion path is a sequence match with event materialization. Variable bindings partition the match space into independent tracks, enabling per-segment analysis without separate operators.

**Entity-first data model (Belief 3).** The matcher operates per-entity via the `EntityOperator` interface (execution-model.md Section 4). Entity-sorted data from the storage layer guarantees that all events for an entity arrive in timestamp order. The matcher maintains compact per-entity state that fits in L1 cache for typical entities.

---

## 2. Feature Requirements

The sequence matcher must support:

1. **Ordered steps.** A THEN B THEN C — events must occur in strictly increasing timestamp order. Events at the same timestamp do not satisfy THEN (same-timestamp events are not conversions).
2. **Time windows.** Global window from the first step: A THEN B THEN C WITHIN 7d means all steps must complete within 7 days of the first step.
3. **Negation (exclusions).** A THEN B WITHOUT C in between — modeled as poison transitions that eagerly kill candidate paths.
4. **Repetition.** A THEN B+ THEN C (one or more B's) — self-loops in the NFA.
5. **Property constraints.** A WHERE price > 100 — per-step predicates evaluated during NFA transitions.
6. **Variable bindings.** `$plan = signup.plan` — bind a property value on the first step that references the variable, check equality on subsequent steps. Each distinct binding value creates an **independent match track** (Section 8).
7. **Event consumption.** Matched events are consumed within a binding track, deferred to match completion. Events CAN participate in matches across different binding tracks.
8. **Two match modes.** FIRST (one match per entity/binding track) and ALL (all non-overlapping matches per entity/binding track).
9. **EMIT ALL flag.** Optionally emit all entities/tracks that enter the NFA (match step 1), with the farthest step they reached. Incomplete sequences still appear in output (Section 5.3).
10. **Non-consecutive matching (default).** Events between matched steps are allowed and not consumed. Consecutive matching (IMMEDIATELY) is an opt-in modifier.

---

## 3. NFA Design

### 3.1 Thompson's NFA Simulation

The matcher uses Thompson's algorithm: simulate all active NFA states in parallel for each event. This gives O(n × m) time where n = events and m = pattern states. Predictable, no pathological cases.

**Never backtracking.** A backtracking engine has exponential worst-case time and is a footgun for a query engine operating on untrusted data. Thompson's simulation guarantees bounded work per event regardless of pattern complexity.

**Why not lazy DFA.** Flink CEP uses a lazy DFA-like approach. For bqlite, DFA state explosion with variable bindings and time windows makes eager DFA construction impractical. The tiered strategy system (Section 10) already provides DFA-equivalent performance for linear patterns via the step counter fast path.

The compiled NFA is immutable and shared across all shard-tasks (it is the "program"). Active instances are the per-entity "execution state."

### 3.2 NFA State Graph

A compiled pattern produces a directed graph of states with labeled transitions:

```rust
/// Compiled NFA shared across all shard-tasks.
pub struct CompiledNfa {
    /// States in the NFA graph. State 0 is the start state.
    states: Vec<NfaState>,
    /// Index of the accept state.
    accept_state: u16,
    /// Event types referenced by any step or negation (for scan pushdown).
    /// Deduplicated.
    relevant_event_types: HashSet<String>,
    /// Pattern classification (Section 10).
    pattern_class: PatternClass,
    /// Variable binding definitions.
    variable_bindings: Vec<VariableBindingDef>,
    /// Global time window, if any (nanos).
    global_window: Option<i64>,
    /// Whether EMIT ALL is enabled.
    emit_all: bool,
    /// Map from NFA state index to logical step number (1-indexed).
    /// For linear patterns, state N maps to step N.
    /// For branching patterns, each state maps to the number of forward
    /// transitions on the shortest path from the start state.
    state_to_step: Vec<u8>,
}

pub struct NfaState {
    /// Forward transitions: event predicate → target state.
    transitions: Vec<Transition>,
    /// Poison transitions: event predicate → kills candidates at this state.
    poison_transitions: Vec<PoisonTransition>,
}

pub struct Transition {
    /// Event type to match. Compared as string — different segments may have
    /// different dictionary encodings, and the k-way merge across segments
    /// produces decoded values. For single-segment batches that arrive as
    /// DictionaryArray, the NFA resolves event type strings against the
    /// batch's dictionary once and caches codes for fast comparison.
    event_type: String,
    /// Additional property predicates (evaluated after event type match).
    predicates: Vec<StepPredicate>,
    /// Variables to bind at this transition (index into variable_bindings).
    /// Binding occurs only if the variable is not yet bound for this candidate.
    bind_variables: Vec<usize>,
    /// Variables to check at this transition (index into variable_bindings).
    /// Candidate only transitions if the event's value matches the bound value.
    check_variables: Vec<usize>,
    /// Target state index.
    target: u16,
}

pub struct PoisonTransition {
    /// Event type that kills candidates at this state.
    event_type: String,
    /// Optional additional predicates on the poison event.
    predicates: Vec<StepPredicate>,
}
```

### 3.3 NFA Transition Algorithm

For each event in an entity's stream, the NFA executes three phases:

**Phase 1: Process existing active states (reverse order).** Iterate states from highest index to lowest. Reverse order prevents cascading — a candidate propagated from state S to state S+1 won't be re-evaluated for S+1's transitions in the same event pass, since S+1 was already processed before S:

   a. **Expire.** Remove candidates whose time window has passed (pop from front of deque where `event.ts - candidate.anchor_ts > window`). When EMIT ALL is enabled, record `step_reached` for expired candidates before dropping them (Section 5.3).
   b. **Check poison transitions.** If the event matches a poison transition's predicate, remove all affected candidates at this state.
   c. **Check forward transitions.** If the event matches a forward transition's predicate AND `event.ts > candidate.last_step_ts` (strict timestamp ordering — Section 15.1) AND the global time window is satisfied AND variable binding checks pass, propagate eligible candidates to the target state. Propagated candidates carry `last_step_ts = event.ts`.

**Phase 2: Start new candidates.** If the event matches any first-step predicate (state 0's forward transitions), create a new candidate at the transition's target state with `anchor_ts = event.ts` and `last_step_ts = event.ts`. For variables bound at step 1, extract and store the binding values. Each distinct binding value set joins (or creates) an independent binding track (Section 8).

New candidates are created AFTER processing existing states. This prevents the same event from both starting a candidate and advancing it — an event advances a candidate by at most one step per pass.

**Phase 3: Dedup and check accept.** Deduplicate candidates per `(state, binding track)` — same `anchor_ts` keeps one entry. If the accept state has any candidates, a match has completed (Section 5 for mode-specific behavior).

### 3.4 Negation: Poison Transitions

"A THEN B WITHOUT C" is modeled as an explicit poison transition on all states between the negation's scope:

```
State 0 --[A]--> State 1
State 1 --[B]--> ACCEPT
State 1 --[C]--> DEAD (poison transition)
```

When the NFA is at state 1 and sees event C, all candidates at state 1 are removed.

- Evaluated eagerly as events arrive, keeping the state set pruned.
- Preferred over guard conditions (which defer the check) because it prunes dead paths immediately.
- **Negation target event types must be included in scan-level event type filtering** — the NFA needs to see them to fire poison transitions.

For negation spanning multiple steps ("WITHOUT C" between step A and step D in a 4-step pattern), poison transitions for C are added to every state between A's target state and D's source state.

### 3.5 Repetition

"A THEN B+ THEN C" uses an intermediate state to track whether at least one B has been seen:

```
State 0 --[A]--> State 1
State 1 --[B]--> State 2       (first B — transitions to "B seen" state)
State 2 --[B]--> State 2       (self-loop — additional B's)
State 2 --[C]--> ACCEPT        (C — only after at least one B)
```

The state distinction (1 vs 2) encodes whether at least one B has been seen — no per-candidate flag needed.

- `+` (one or more): requires the intermediate state before the self-loop.
- `*` (zero or more): epsilon transition from state 1 directly to state 2 (skip the B requirement). Epsilon transitions are resolved at compile time via epsilon-closure computation — the NFA eliminates them during construction by precomputing reachable states and adding direct transitions. No epsilon transitions exist at runtime.
- Combined with time windows, repetition's state growth is bounded by window expiration and candidate dedup.

### 3.6 Alternation

"A THEN (B OR C) THEN D" produces branching transitions from the same source state:

```
State 0 --[A]--> State 1
State 1 --[B]--> State 2
State 1 --[C]--> State 2
State 2 --[D]--> ACCEPT
```

Thompson's construction for alternation introduces epsilon transitions in the general case. These are eliminated during compilation via epsilon-closure (Section 3.5), producing the simplified graph above with direct transitions.

---

## 4. Event Consumption Semantics

### 4.1 Core Rule

Events that are assigned to a specific step in a completed match are **consumed** — they cannot participate in any other match within the same binding track. Events between matched steps (non-consecutive matching) are NOT consumed and remain available. Events CAN participate in matches across different binding tracks (Section 8.2).

### 4.2 Deferred Consumption

Consumption is **deferred to match completion**, not applied at intermediate steps. This is critical for correctness with time windows.

**Why eager consumption fails.** Consider pattern "A THEN B THEN C WITHIN 7" with events `A(ts:1), A(ts:5), B(ts:6), C(ts:10)`:

- **Eager:** B(6) consumes A(1) (earliest). C(10) checks window: 10 - 1 = 9 > 7. Expired. No match. **Wrong.**
- **Deferred:** B(6) propagates both candidate anchors [1, 5] to state 1. C(10) checks: anchor 1 expired (10 - 1 = 9 > 7), anchor 5 eligible (10 - 5 = 5 ≤ 7). Match with anchor 5. **Correct.**

Eager consumption at intermediate steps commits to an anchor before knowing if the full match will complete within that anchor's window.

### 4.3 Anchor Consumption Strategy

At match completion, consume the **earliest eligible anchor**. This maximizes the chance of future matches by preserving newer events.

---

## 5. Match Modes

### 5.1 MATCH FIRST

Find the first complete match for each `(entity, binding track)` pair, then stop processing that track.

- Simplest and fastest mode.
- Covers the common funnel use case ("did the user convert?").
- Step counter fast path applies directly (Section 10.3).
- `finish_entity()` returns one row per `(entity, binding track)` that completed the sequence, or `None` if no match and EMIT ALL is disabled.

### 5.2 MATCH ALL (Non-Overlapping)

Find all matches where no event participates in more than one match within the
same binding track. After a match completes, remove only the candidates that
used the consumed events and keep later unmatched entries alive.

- Reduces to **repeated first-match with a moving start point** per binding track.
- After each match completion: advance the track's restart point to the
  consumed anchor and prune only candidates that share consumed events. New
  step-1 entries for this track are created only from events with `ts > scan_from`.
- Adds almost zero complexity over first-match — a loop around first-match logic with a reset.
- Covers the common "count conversions per user" use case.

### 5.3 EMIT ALL

When the EMIT ALL flag is set on a MATCH operator, the output includes **all binding tracks that enter the NFA** (match step 1), not just those that complete the full sequence. Each output row includes `step_reached` indicating the farthest step matched.

**Emission points.** Partial results are emitted at three points:

1. **Match completion.** `step_reached = num_steps`. Normal completed match.
2. **Window expiry.** When a candidate's global window expires during Phase 1 of the transition algorithm (Section 3.3), record `step_reached = state_to_step[current_state]` and emit (or fuse into the aggregation accumulator) before dropping the candidate. This avoids retaining any state for expired candidates.
3. **Entity end.** When `finish_entity()` is called, emit rows for any
   remaining in-progress candidates that haven't expired or completed.
   `MATCH FIRST EMIT ALL` collapses those live candidates to the single
   farthest partial for the binding track; `MATCH ALL EMIT ALL` collapses to
   the farthest partial per surviving step-1 entry.

**Fused aggregation at expiry.** For the common funnel use case (EMIT ALL + STATS step counts), the fused aggregation path increments `step_counts[step_reached]` directly at the moment of expiry. No buffering, no retained state for expired candidates. This is the primary advantage of emitting at expiry — the sequence match operator can fuse aggregations incrementally without accumulating partial results.

**MATCH FIRST EMIT ALL:** Each `(entity, binding track)` pair produces exactly one row. Either completed (`step_reached = num_steps`) or incomplete (farthest step before entity end or window expiry).

**MATCH ALL EMIT ALL:** Each NFA entry (step 1 match) within a binding track
produces a row. After a match completes (or a window expires), that entry
closes and later unmatched step-1 events continue as new entries. This is the
funnel use case: "how many times did the user start checkout, and how far did
they get each time?"

### 5.4 BQL Syntax

```
events | MATCH FIRST SEQUENCE(A THEN B THEN C)
events | MATCH ALL SEQUENCE(A THEN B THEN C)
events | MATCH FIRST SEQUENCE(A THEN B THEN C) EMIT ALL
events | MATCH ALL SEQUENCE(A THEN B THEN C) EMIT ALL
```

---

## 6. Candidate Propagation Model

The candidate deque model is the **general-purpose execution model** for the NFA. It handles all pattern shapes, match modes, and demand sets correctly. Compact state models (Section 11) are optimizations used under specific conditions (typically when downstream aggregation demand allows fusion).

### 6.1 Core Data Structures

Each binding track maintains a set of per-state candidate deques:

```rust
/// Per-(entity, binding track) state. One instance per distinct binding value
/// combination. For patterns without variable bindings, there is exactly one
/// track per entity.
struct BindingTrack {
    /// Bound variable values for this track.
    bindings: SmallVec<[BindingValue; 2]>,
    /// Per-state candidate deques.
    state_candidates: SmallVec<[StateCandidates; 8]>,
    /// Completed match count (for fused counting).
    match_count: u32,
    /// Farthest step reached (for EMIT ALL).
    max_step_reached: u8,
    /// Restart point for MATCH ALL: ignore events with ts ≤ scan_from
    /// when creating new candidates for this track.
    scan_from: i64,
}

struct StateCandidates {
    state_id: u16,
    candidates: PendingTimestamps,
}

/// Each candidate entry tracks both the original anchor (for window checks)
/// and the timestamp of the event that last advanced it (for strict ordering).
#[derive(Debug, Clone, Copy)]
struct CandidateEntry {
    /// First step's timestamp (for window check: event.ts - anchor_ts ≤ window).
    anchor_ts: i64,
    /// Timestamp of the event that advanced this candidate to its current state.
    /// Forward transitions require event.ts > last_step_ts (strict ordering).
    last_step_ts: i64,
}

enum PendingTimestamps {
    /// Stack-allocated for the common case (≤4 candidates per state).
    Inline(ArrayVec<CandidateEntry, 4>),    // 64 bytes (4 × 16)
    /// Heap-allocated for rare power-law entities.
    Spilled(VecDeque<CandidateEntry>),
}
```

The top-level per-entity state:

```rust
struct EntityMatchState {
    /// One track per distinct binding value combination.
    tracks: SmallVec<[BindingTrack; 4]>,
}
```

### 6.2 BindingValue

```rust
/// Compact representation of a bound variable value for NFA comparisons.
/// Only scalar types — List and Map are not valid binding targets.
/// Type is known at plan time; the variant is selected during compilation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BindingValue {
    Bool(bool),
    Int(i64),
    Float(FloatOrd<f64>),   // ordered wrapper for Eq/Hash (float-ord crate)
    String(CompactString),   // inline small strings, heap for large (compact_str crate)
    Timestamp(i64),          // epoch nanos
}
```

`BindingValue` is distinct from `PropertyValue` (bqlite-core, used for ingest) — it is a compact, comparison-optimized enum for the NFA hot path.

**NULL handling.** If a column value to bind is NULL, the WHERE predicate containing the `$variable` evaluates to NULL under SQL three-valued logic (e.g., `NULL = $plan` is NULL, not true). The event does not match the step, no binding occurs, and no track is created for that value. Entities with NULL in binding columns silently don't match that step.

**NaN handling.** `FloatOrd<f64>` treats NaN == NaN for binding purposes — two NaN-valued bindings join the same track. This deviates from IEEE 754 but is the desired behavior for grouping.

### 6.3 Propagation Algorithm

When an event matches the transition predicate for state S → state S+1 within a binding track:

1. **Expire** old candidates at state S (front of deque, where `event.ts - candidate.anchor_ts > window`). When EMIT ALL, record `step_reached` before dropping.
2. **Check strict ordering**: only candidates where `event.ts > candidate.last_step_ts`.
3. **Propagate** ALL remaining eligible candidates to state S+1 (copied), with `last_step_ts = event.ts`. Anchor is preserved.
4. **Dedup** at state S+1 (same `anchor_ts` = keep one, preferring the entry with the latest `last_step_ts`).
5. Candidates at state S are **not removed** (deferred consumption).
6. When a candidate reaches the accept state: match completed. Consume the
   earliest eligible anchor. For MATCH ALL: remove candidates that share the
   consumed events, preserve later unmatched entries, and set `scan_from` to
   the consumed anchor so earlier step-1 events are not reconsidered.

### 6.4 Why All Candidates Must Be Kept

Cannot keep only the latest candidate. Counterexample:

```
Pattern: A THEN B THEN C, WITHIN 7
Events:  A(1), A(100), B(3), C(5)

Latest-only: state 1 = [(anchor=100, last=100)]. B(3): 3 > 100? No. No match. WRONG.
All:         state 1 = [(anchor=1, last=1), (anchor=100, last=100)].
             B(3): 3 > 1? Yes. Window 3-1=2≤7.
             State 2 = [(anchor=1, last=3)]. C(5): 5 > 3? Yes. 5-1=4≤7. Match! CORRECT.
```

An earlier anchor may be the only one whose subsequent steps have arrived in time.

### 6.5 Candidate Deque Bounds

Candidates are bounded by:
- **Time window expiration** — old candidates expire from the front of the deque.
- **Dedup** — each `anchor_ts` appears at most once per `(state, track)`.
- **MATCH ALL mode** — per-completion pruning of consumed-event candidates.
- **Active state limit** (Section 16.1) — hard cap on total candidates per entity.

For a typical entity with a few hundred events over 30 days and a 7-day window, each state's deque has ~10-50 entries. The inline `ArrayVec<CandidateEntry, 4>` (64 bytes) handles most cases; rare spills to heap for power-law entities.

### 6.6 Sub-Batch Persistence

The `EntityMatchState` (including all binding tracks) persists across `process_sub_batch()` calls, per the `EntityOperator` contract (execution-model.md Section 4). A match can span multiple sub-batches — step A may occur in sub-batch 1 and step B in sub-batch 5. The NFA state is compact (a few hundred bytes for typical patterns) and lives in the operator's `State` type, which the `EntityOperatorAdapter` maintains across sub-batch boundaries.

---

## 7. Time Window Semantics

### 7.1 Global Window

All time windows are global — measured from the first step. "A THEN B THEN C WITHIN 7d" means all steps must complete within 7 days of step A's timestamp.

Each candidate carries `anchor_ts` (the first step's timestamp), unchanged through all state transitions. At each transition, the check is: `event.ts - candidate.anchor_ts ≤ global_window`.

This is the only window mode. Per-step windows (e.g., "B WITHIN 1d of A THEN C WITHIN 30d of B") are not supported — they add significant complexity to both the language and the NFA implementation (candidates at intermediate states would need per-candidate step anchors, breaking the step counter fast path and doubling candidate state size). The global window covers the vast majority of real analytics queries.

### 7.2 Window Expiration

Candidates are expired eagerly at the front of each deque whenever a new event arrives (Phase 1 of Section 3.3). Since events are timestamp-ordered, once a candidate's anchor is older than `current_event.ts - window`, it can never satisfy the window constraint for any future event. Expiration is O(1) amortized — each candidate is expired exactly once.

When EMIT ALL is enabled, expiring a candidate records `step_reached` for it (Section 5.3). In the fused aggregation path, this directly increments the step count array — no buffered state.

For patterns without a time window (`global_window = None`), candidates never expire from windows. The active state limit (Section 16.1) provides the bound.

---

## 8. Variable Bindings

### 8.1 Syntax and Semantics

Variables are denoted with the `$` prefix and bound via equality predicates in step WHERE clauses:

```
events | MATCH FIRST SEQUENCE(
    signup WHERE plan = $plan
    THEN purchase WHERE plan = $plan
) WITHIN 30d
```

The first step where `$plan` appears (`signup`) **binds** the variable — extracts the value of `plan` from the matched event. Subsequent steps (`purchase`) **check** the variable — the event's `plan` value must equal the bound value.

### 8.2 Independent Match Tracks

Each distinct binding value creates an **independent match track**. Conceptually, `(entity_id, binding_values)` is the effective entity key for the NFA. If a user has signup events with `plan=free` and `plan=pro`, two independent tracks run simultaneously:

- Track `$plan=free`: looking for `signup(plan=free) THEN purchase(plan=free)`
- Track `$plan=pro`: looking for `signup(plan=pro) THEN purchase(plan=pro)`

Tracks are independent — events can participate in matches across different binding tracks. Consumption only applies within a single binding track. This means one event can contribute to matches in multiple tracks simultaneously.

In MATCH FIRST mode, each track produces at most one match. In MATCH ALL mode, each track produces all non-overlapping matches independently. With EMIT ALL, each track produces one row per NFA entry (Section 5.3).

### 8.3 Binding Definition

```rust
pub struct VariableBindingDef {
    /// Variable name (e.g., "$plan").
    name: String,
    /// Column name to extract from the event batch.
    column: String,
    /// BqlType of this variable (known at plan time, validated against table schema).
    bql_type: BqlType,
    /// Index of the step where binding occurs (first step referencing this variable).
    bind_step: u16,
    /// Steps that check (enforce equality with) this binding.
    check_steps: Vec<u16>,
}
```

### 8.4 Implementation

In the candidate deque model, each `BindingTrack` operates independently (Section 6.1). When an event matches step 1 with a new binding value, a new `BindingTrack` is created and added to the entity's track list. When an event matches step 1 with an existing binding value, the candidate is added to the existing track.

For the step counter fast path (Section 10.3), each binding track has its own step counter. Multiple tracks may be active simultaneously for a single entity.

### 8.5 Type-Safe Extraction

Since the BQL type of each variable is known at plan time (Belief 8 — strongly-typed pipelines), the compiled NFA includes extraction functions monomorphized per type:

- `BqlType::Int` → extract `i64` from `Int64Array`, store as `BindingValue::Int`.
- `BqlType::String` → extract `&str` from `StringViewArray` or decode from `DictionaryArray`, store as `BindingValue::String`.
- Etc.

For dictionary-encoded columns (common for string properties), the NFA decodes the dictionary value at extraction time. This is a single dictionary lookup per binding event, not per comparison.

Comparison at check steps is a `BindingValue::eq()` call — single enum match, no dynamic dispatch.

### 8.6 State Space

The state space is `num_binding_tracks × num_states × candidates_per_state`. For a single entity, the cardinality of bound variables is almost always small (one user has one country, a few plan types). The active state limit (Section 16.1) bounds the total candidate count across all binding tracks, preventing state explosion from high-cardinality bindings.

---

## 9. Filter Pushdown Before the NFA

### 9.1 Principle

Every filter that can be evaluated before the NFA should be. The NFA should see the minimum possible event stream.

### 9.2 Level 1: Event Type Filtering

The pattern references event types A, B, C (and negation target D). Push to the scan layer:

```
event_type IN {A, B, C, D}
```

With dictionary encoding on `event_type` (storage-format.md Section 10.2), this is a bitset check on dictionary codes at the scan layer — essentially free. **Negation targets must be included** — the NFA needs to see them to fire poison transitions.

### 9.3 Level 2: Property Predicate Pushdown

If step predicates include property constraints, push a disjunctive filter:

```
(event_type = A AND price > 100)
OR (event_type = B AND region = 'US')
OR (event_type = C)                     -- no property constraint on this step
OR (event_type = D)                     -- negation target, no property filtering
```

Any event failing ALL step predicates can never participate in a match. For "purchase WHERE amount > 1000", if 95% of purchases are under $1000, this eliminates 95% of purchase events before the NFA.

### 9.4 Level 3: Predicate Extraction During Compilation

During physical planning, the sequence match operator examines its compiled pattern and produces a **scan predicate** that the scan layer evaluates in vectorized fashion on columnar data. Only surviving events reach the NFA.

The NFA still evaluates per-step predicates (the disjunctive pushdown is coarser), but on a much smaller event stream.

### 9.5 Step Predicate Bitmask (Optional Optimization)

Annotate each event with which step predicates it satisfied during scan-phase evaluation, as a small bitmask. NFA transition checks become bitmask tests rather than predicate re-evaluation. Worth implementing for expensive predicates (regex on strings); probably not worth it for simple comparisons. Deferred to implementation — measure first.

---

## 10. Tiered Execution Strategies

### 10.1 Pattern Shape Classification

At compile time, classify the pattern:

```rust
pub enum PatternClass {
    /// Ordered steps, no negation, no repetition, no variable bindings.
    LinearSimple,
    /// Linear with one or more IMMEDIATELY transitions.
    LinearImmediate,
    /// Linear with poison transitions.
    LinearWithNegation,
    /// Linear with variable bindings.
    LinearWithBindings,
    /// Negation + bindings.
    LinearFull,
    /// Branching, repetition, alternation — requires general NFA.
    GeneralNfa,
}
```

### 10.2 Strategy Selection Matrix

Combined with match mode and demand set, select the optimal execution strategy at plan time:

| Pattern Shape | Match Mode | Demand | Strategy | Approx. Speed |
|---|---|---|---|---|
| Linear | FIRST | Any | Step counter + candidate deque at step 0 | ~1-3 ns/event |
| Linear + IMMEDIATELY | Any | Any | Dedicated consecutive matcher with previous-event slot | ~1-3 ns/event |
| Linear | FIRST | Entity presence only | Step counter, stop at first accept | ~1-2 ns/event |
| Linear + negation | FIRST | Any | Step counter + poison flags + candidate deque | ~2-5 ns/event |
| Linear + bindings | FIRST | Any | Step counter per track + candidate deque | ~3-7 ns/event |
| Linear | ALL | Count (fused) | Repeated step counter with reset | ~1-3 ns/event |
| Linear | ALL | Step counts (fused funnel) | Step counter with `[u64; N]` array | ~1-3 ns/event |
| LinearFull | Any | Any | Step counter + poison + bindings | ~3-7 ns/event |
| General | FIRST | Any | Full NFA with candidate propagation | ~5-15 ns/event |
| General | ALL | Any | Repeated NFA first-match with reset | ~5-15 ns/event |
| Any | Any | Match details | Full NFA with path tracking | ~10-30 ns/event |

The strategy is a **compile-time decision** — no runtime branching between strategies. The operator inspects the compiled pattern shape and demand set during physical planning.

**Fusion is orthogonal to strategy selection.** When the planner fuses a downstream aggregate into the MATCH operator (planner-pipeline.md §7), the strategy chosen from this matrix does not change. A fused MATCH can be a step counter, a dedicated consecutive matcher, or a full NFA — fusion only affects what happens at match completion (update an accumulator instead of emitting a row). The per-event inner loop of each strategy is identical whether fusion is enabled or not.

Patterns containing `IMMEDIATELY` do not use the general non-consecutive NFA transition logic. They compile to a dedicated consecutive matcher because adjacency is a positional constraint, not a timestamp-only check.

### 10.3 Step Counter Fast Path (Linear Patterns)

For linear patterns without branching or repetition, the NFA simplifies to a step counter per binding track. The candidate deque is only needed at step 0 (to handle the window rebinding problem from Section 4.2). Subsequent steps carry forward one anchor at a time.

```rust
struct StepCounterTrack {
    /// Bound variable values for this track.
    bindings: SmallVec<[BindingValue; 2]>,
    /// Current step in the linear pattern (1..num_steps, or 0 if not started).
    current_step: u8,
    /// Anchor timestamp from the first step (for global window).
    anchor_ts: i64,
    /// Timestamp of the event at the current step (for strict ordering).
    last_step_ts: i64,
    /// Completed match count (for fused counting / MATCH ALL).
    match_count: u32,
    /// Farthest step reached (for EMIT ALL).
    max_step_reached: u8,
    /// Restart point for MATCH ALL.
    scan_from: i64,
}

struct StepCounterState {
    /// One counter per active binding track.
    tracks: SmallVec<[StepCounterTrack; 4]>,
    /// Step 0 candidate deque (for window rebinding on expiration).
    /// Shared across tracks before bindings are established.
    step0_candidates: ArrayVec<CandidateEntry, 8>,
}
```

This fast path is for non-consecutive linear patterns only. `IMMEDIATELY` patterns use the dedicated consecutive matcher described above.

For MATCH FIRST, when the window expires at an intermediate step, fall back to `step0_candidates` and rebind to the next eligible anchor. If no eligible anchors remain, reset to step 0.

For MATCH ALL, prune only the consumed entry on match completion and continue.

With EMIT ALL, when a window expires or the entity stream ends, emit the
farthest surviving partial for that track/entry. In the fused path, increment
`step_counts[current_step]` directly.

This is the hot path for funnel queries. A 5-step funnel with no negation or bindings: check event type (string comparison), check window (integer subtraction + comparison), increment step counter. ~1-3 ns/event.

### 10.4 Single-Event Pattern Bypass

A pattern matching a single event type ("A WHERE price > 100") doesn't need the NFA at all — it is a simple filter. The pattern classifier detects this and emits a filter operator instead, avoiding NFA construction entirely.

---

## 11. Memory Layout and Cache Optimization

### 11.1 Design Principle

The candidate deque model (Section 6) is the general-purpose representation. Compact state models are used under **specific conditions** — typically when downstream aggregation demand allows the operator to fuse counting or step tracking internally, eliminating the need for full candidate tracking.

### 11.2 Compact Step Counter (Fused Aggregation Path)

When the demand is fused step counts (the funnel fast path), the per-track state collapses to `StepCounterTrack` (Section 10.3, ~50 bytes per track with bindings, ~28 bytes without). For a single-binding entity, the entire state fits in one cache line.

The step counter still needs `step0_candidates` for window rebinding (Section 4.2). For patterns without a time window, `step0_candidates` is empty and the state is even smaller.

### 11.3 Full NFA Instance (Match Detail Path)

```rust
struct FullNfaInstance {
    anchor_ts: i64,
    last_step_ts: i64,
    match_trace: Vec<EventRef>,  // which events matched at each step
}

/// Reference to a matched event, used only for match detail output.
struct EventRef {
    timestamp: i64,
    /// Row index within the sub-batch (for extracting property values if needed).
    row_index: u32,
}
```

Used within a `BindingTrack` only when demand requires match details (match_events, match_duration). Heap-allocated, rare path.

### 11.4 Memory Budget Interaction

Per-entity NFA state is compact (tens to hundreds of bytes) and is not tracked individually by `MemoryTracker` (execution-model.md Section 10.1). The active state limit (Section 16.1, default 10,000 candidates) bounds the worst case: 10,000 × 16 bytes = 160 KB per entity for the deque model, well within per-thread memory bounds. The `PendingTimestamps::Spilled` variant (heap `VecDeque`) is bounded by the same active state limit.

---

## 12. Output Schema

### 12.1 Base Output Schema

The sequence match operator produces a `RecordBatch` with the following columns (see also type-system.md Section 6.1):

| Column | Type | Nullable | Present | Description |
|---|---|---|---|---|
| `entity_id` | String or Int | no | Always | Entity key (type matches table schema) |
| `$var` | (per variable type) | no | When variables are bound | One column per bound variable, named by the variable |
| *step-property columns* | (resolved from source schema) | follows source | When downstream references `step_name.column` | One column per referenced named-step property |
| `step_reached` | Int | no | When EMIT ALL is enabled | 1-indexed step number of the farthest step matched |
| `match_duration` | Int | yes | When demanded | Nanoseconds between first and last matched step (NULL if `step_reached == 1`) |
| `match_events` | Map(Timestamp) | yes | When demanded | Step name → timestamp for each matched step (partial map if incomplete) |

**Step-property columns.** When a pattern contains named steps (`s: signup THEN p: purchase`) and a downstream operator references a per-step property (`s.plan`, `p.amount`), the planner adds that property as a first-class column in the output schema. The column's name is `step_name.column_name` internally and its type is resolved from the source table's schema for the step's event type. Step-property demand is per-(step, column) — see planner-pipeline.md §8.2 and type-system.md §6.1. Only the demanded properties are retained by the operator; `match_events` is *not* materialized as a side effect of step-property access.

Without EMIT ALL, only completed matches appear in output. `step_reached` is omitted (implicitly equals `num_steps`). `match_duration` and `match_events` are non-NULL for completed matches.

With EMIT ALL, all binding tracks that enter the NFA appear. `step_reached` ranges from 1 to `num_steps`. Tracks with `step_reached == 1` have NULL `match_duration` and single-entry `match_events`. Tracks with `step_reached < num_steps` have partial `match_events`.

### 12.2 Output by Mode

**MATCH FIRST (no EMIT ALL):** One row per `(entity_id, bindings)` that completed the sequence.

**MATCH FIRST EMIT ALL:** One row per `(entity_id, bindings)` that entered the NFA. Includes both completed and incomplete sequences.

**MATCH ALL (no EMIT ALL):** One row per completed match. An `(entity_id, bindings)` pair can produce multiple rows (one per non-overlapping match).

**MATCH ALL EMIT ALL:** One row per NFA entry (step 1 match) within each binding track. Each entry either completed, had its window expire, or reached entity end. Entries whose window expired are emitted at expiry time (Section 5.3).

### 12.3 Demand-Driven Schema Reduction

The planner propagates downstream demand to strip columns that are never read:

- If downstream only needs `entity_id` + aggregate → no match detail columns materialized.
- If downstream needs `step_reached` for funnel counting → step counter strategy, no match trace.
- If downstream needs named step properties (`s.plan`, `p.amount`) → per-(step, column) layered extraction; no `match_events` materialized.
- If downstream needs `match_events` explicitly → full NFA with path tracking.

The `finish_entity()` return type is always `Option<RecordBatch>`, but the batch's schema is the demand-reduced version. Demand propagation uses the **`DemandSet`** type formally defined in planner-pipeline.md §9.3 — this is the downstream-needs struct propagated backward through the plan by the physical planner (execution-model.md §8.2).

---

## 13. Interaction With EntityOperator Interface

The sequence matcher implements `EntityOperator` (execution-model.md Section 4).

### 13.1 State Type

```rust
impl EntityOperator for SequenceMatchOperator {
    type State = SequenceMatchState;
    // ...
}

/// Per-entity state. Variant selected at plan time based on pattern class + demand.
enum SequenceMatchState {
    /// Step counter fast path (linear patterns, non-detail demand).
    StepCounter(StepCounterState),
    /// Full NFA simulation (general patterns or match detail demand).
    Nfa(EntityMatchState),
}
```

The variant is fixed at plan time — all entities use the same variant within a query.

### 13.2 process_sub_batch()

For each sub-batch (one row-group, up to 64K rows):

1. Extract the `event_type` column. If dictionary-encoded (`DictionaryArray`), resolve pattern event type strings against the batch's dictionary once to get code-based comparison for this batch. If decoded (post k-way merge), use string comparison.
2. Extract timestamp column.
3. Extract any columns needed by step predicates or variable bindings. Columns arrive in whatever encoding the storage layer produces (dictionary-encoded, RLE, decoded). Variable extraction decodes values at bind time (Section 8.5).
4. For each row in the sub-batch, run the NFA transition algorithm (Section 3.3).
5. If a match completes and mode is MATCH FIRST with a single binding track, set an early-exit flag.

The per-row loop is the innermost hot loop. For the step counter fast path, it compiles down to: load event type → compare against step's expected type → branch on match → check `ts > last_step_ts` → check window → increment step. ~5-10 instructions per event.

### 13.3 finish_entity()

Called after all sub-batches for the entity (execution-model.md Section 4). Returns `Option<RecordBatch>` with the output schema from Section 12.

- **Without EMIT ALL:** Returns `None` for entities with no completed matches. Returns a `RecordBatch` with one row per completed match (one row for MATCH FIRST, potentially multiple for MATCH ALL), across all binding tracks.
- **With EMIT ALL:** Returns a `RecordBatch` for every binding track that entered the NFA. Remaining in-progress candidates are emitted with their current `step_reached`. Completed matches have `step_reached = num_steps`. Previously expired candidates were already counted/emitted at expiry time (Section 5.3); for the fused path, the accumulated counts are emitted here.

### 13.4 finish_entity_into() (Aggregation Fusion)

When the demand is aggregate-only, the sequence matcher overrides `finish_entity_into()` and calls the `Accumulator` (execution-model.md §9.4) directly rather than going through a per-entity `RecordBatch`:

```rust
fn finish_entity_into(&self, state: Self::State, acc: &mut dyn Accumulator) {
    match state {
        SequenceMatchState::StepCounter(sc) => {
            for track in sc.tracks {
                // values[] carries the reduced values the fused aggregate reads
                // (step counts, match counts, extracted step properties, etc).
                acc.update(group_key(&track).as_deref(), &track.reduced_values());
            }
            // Separate path for expired candidates already counted via EMIT ALL
            // (see Section 5.3).
        }
        SequenceMatchState::Nfa(ems) => { /* analogous walk over binding tracks */ }
    }
}
```

The `group_key` is constructed from bound variables, quantized timestamps, or other group-by columns the planner resolved into the fused aggregate. The `values` slice is laid out in the same order as `FusableAggregate::functions` (planner-pipeline.md §5.3), so the accumulator updates each `AggState` slot in one pass. Per-operator internal bookkeeping:

- **Count of matches per entity:** `track.match_count` across all binding tracks, passed as the sole value.
- **Funnel step counts:** Internal `[u64; num_steps]` array, incremented at match completion, window expiry (EMIT ALL), and entity end. Replayed into the accumulator as one `update` call per step at entity end.
- **Grouped counts:** Internal `HashMap<GroupKey, Counts>`, keyed by bound variable values or other GROUP BY fields. Drained into `acc.update(Some(&key), &counts)` at entity end.

Zero intermediate materialization — no per-entity rows emitted between the sequence match and aggregation operators. The `Accumulator::update` path bypasses `RecordBatch` construction entirely, which is the main reason `finish_entity_into` exists as a separate hot-path entry point.

### 13.5 supported_demands()

The operator advertises its capabilities to the planner. The planner-side type carried through the backward pass is `DemandSet` (planner-pipeline.md §9.3). The operator's **capability advertisement** — a distinct type that describes which demand shapes the operator can satisfy — is `DemandCapabilities`. Both types live in `bqlite-planner`; `SequenceMatchOperator` in `bqlite-operators` imports and returns them, which respects the dependency direction (`bqlite-operators → bqlite-planner`).

The two types are **dual** concepts:

- `DemandSet` (downstream side): "the downstream needs these columns, these match details, these step properties, and optionally this fused aggregate". Constructed during the backward pass.
- `DemandCapabilities` (operator side): "this operator supports step counts, match counts, full match detail, and aggregation fusion". Advertised once per operator via `supported_demands()`.

The physical planner matches the `DemandSet` for each plan node against the upstream operator's `DemandCapabilities` to decide whether the operator can satisfy the demand directly, and if so, which strategy from Section 10.2 to use.

```rust
fn supported_demands(&self) -> DemandCapabilities {
    // All pattern classes support all demand levels.
    // The strategy selection (Section 10.2) determines the implementation,
    // but the capability set is uniform.
    DemandCapabilities {
        supports_step_reached: true,
        supports_match_count: true,
        supports_full_detail: true,
        supports_aggregation_fusion: true,
        supports_step_property_forwarding: true,   // per-(step, column) forwarding
        supports_forwarded_columns: false,         // MATCH-specific, not generic forwarding
        supports_eager_group_emit: false,          // reserved for Wave 5
    }
}
```

See `docs/design/planner/demand-protocol.md` §2–§3 for the canonical field list and §6 for the `const DEMAND_CAPS` planner-side declaration that must stay in sync.

For general NFA patterns, `step_reached` is computed using the `state_to_step` mapping (Section 3.2): each NFA state maps to a logical step number based on the shortest path from the start state. For branching patterns (A THEN (B OR C) THEN D), both B and C states map to step 2, and D maps to step 3.

### 13.6 required_columns()

Returns the union of:
- `entity_id` column (always needed for output).
- `event_type` column (always needed for transition matching).
- `timestamp` column (always needed for ordering and window checks).
- Columns referenced by step predicates.
- Columns referenced by variable bindings.
- Columns referenced by negation predicates.
- **Columns referenced by named step property forwarding** — each `StepPropertyRef` in the planner's `DemandSet` (e.g., `s.plan` → column `plan` from the `signup` event type) adds the underlying source column to the required set. These columns are decoded only for the relevant event-type rows, but scan-level decoding is columnar so the whole column must be read.

This drives demand propagation upstream — the scan layer only decodes these columns (late materialization).

---

## 14. Pattern Compilation Pipeline

### 14.1 Overview

```
BQL text: MATCH FIRST SEQUENCE(
    signup WHERE plan = $plan
    THEN purchase WHERE plan = $plan
    WITHOUT churn
) WITHIN 7d
    ↓
AST: SequencePattern {
    mode: First,
    emit_all: false,
    steps: [
        Step { event: "signup", predicates: [plan = $plan] },
        Step { event: "purchase", predicates: [plan = $plan] },
    ],
    negations: [Negation { event: "churn", between: (0, 1) }],
    window: GlobalWindow(7d),
    variables: [$plan],
}
    ↓
NFA Graph:
    State 0 --[signup, bind $plan]--> State 1
    State 1 --[purchase, check $plan]--> ACCEPT
    State 1 --[churn]--> DEAD (poison)
    state_to_step: [0, 1, 2]  (state 0 → step 0, state 1 → step 1, ACCEPT → step 2)
    ↓
Pattern Classification: LinearFull (linear + negation + bindings)
    ↓
Strategy Selection (depends on demand):
    demand = count → step counter + poison flags + bindings
    demand = match_details → full NFA with path tracking
```

### 14.2 Compilation Steps

1. **Parse** the pattern into an AST (steps, predicates, time window, negations, repetitions, variable references). Handled by `bqlite-parser`.
2. **Validate** against the table schema — event type column exists, property columns referenced in predicates and variable bindings exist with correct types. Handled by `bqlite-planner`.
3. **Resolve variable binding order** — for each `$variable`, determine which step binds (first reference) and which steps check.
4. **Convert** AST to NFA graph using Thompson's construction (extended for temporal features). Each step becomes a state; transitions carry predicates, bind instructions, and check instructions.
5. **Eliminate epsilon transitions** — compute epsilon-closure for `*` (zero-or-more) and alternation constructs. Produce an epsilon-free NFA.
6. **Add poison transitions** for negation clauses across their specified scope.
7. **Compute `state_to_step` mapping** — BFS from start state, assigning each state a step number based on shortest forward-transition distance from start.
8. **Classify** the pattern shape (Section 10.1).
9. **Optimize** the NFA: merge equivalent states, compute transition priority order.
10. **Extract** the set of relevant event types for scan-level pushdown (Section 9).
11. **Package** the compiled NFA, scan predicates, and output schema into the physical plan.

### 14.3 Crate Placement

| Component | Crate | Rationale |
|---|---|---|
| `SequencePattern` AST node | `bqlite-ast` | Shared by parser and planner |
| Pattern validation, variable resolution | `bqlite-planner` | Schema access needed |
| `CompiledNfa`, `PatternClass` | `bqlite-planner` | Plan-time compilation |
| `DemandCapabilities`, `DemandSet` | `bqlite-planner` | Plan-time demand propagation |
| `SequenceMatchOperator` | `bqlite-operators` | Implements `EntityOperator` |
| NFA state types (`EntityMatchState`, etc.) | `bqlite-operators` | Execution-time only |
| `BindingValue` | `bqlite-operators` | Used only by sequence match execution |

---

## 15. Matching Semantics Details

### 15.1 Strict Timestamp Ordering

THEN requires **strictly increasing timestamps**. Two events at the same timestamp do not satisfy THEN, even if they have different `__seq_id` values. Same-timestamp events are considered simultaneous and cannot form a conversion.

Implementation: the forward transition check `event.ts > candidate.last_step_ts` enforces this. `last_step_ts` tracks the timestamp of the event that advanced the candidate to its current state. An event at the same timestamp as the previous step's event fails this check.

### 15.2 Non-Consecutive (Default)

Events between matched steps are allowed and not consumed. "A THEN B" matches even with unrelated events between A and B. This is the overwhelmingly common case for behavioral analytics.

### 15.3 Consecutive (Opt-In)

Events must be adjacent — no events between matched steps (considering only events that pass scan-level filtering). Supported as a modifier:

```
events | MATCH FIRST SEQUENCE(A THEN IMMEDIATELY B THEN IMMEDIATELY C)
```

Implementation: patterns with `IMMEDIATELY` compile to a dedicated consecutive matcher rather than the general non-consecutive NFA. The matcher keeps only the previous filtered event (per binding track where relevant) and advances the pattern iff the current event is the very next filtered event after the prior matched step. This is substantially simpler than trying to retrofit adjacency onto the general NFA state model.

### 15.4 Greedy/Lazy for MATCH ALL

In MATCH ALL mode, the algorithm naturally finds the first complete match starting from each scan position. With deferred consumption and earliest-anchor-at-completion, this produces greedy-from-the-left behavior: each match uses the earliest possible anchor.

---

## 16. Safety Valves

### 16.1 Active State Limit

Configurable maximum on the total number of candidate entries per entity across all states and binding tracks (default: 10,000). If exceeded, the oldest candidates are dropped (front of deques, starting with the earliest anchors). This prevents pathological patterns or extreme power-law entities from consuming unbounded memory.

When triggered, a warning is recorded in the shard-task's `ShardTaskContext` (execution-model.md Section 3.3). The `QueryWarning` enum in execution-model.md should be extended with:

```rust
ActiveStateLimitExceeded {
    entity_id: String,
    dropped_count: u64,
    limit: u64,
}
```

The query continues with potentially degraded accuracy for that entity.

### 16.2 Entity Event Limit

The execution model's entity event limiter (execution-model.md Section 5.3, default 10M events) caps the event stream before it reaches the NFA. This is the primary defense against pathological entities; the active state limit (Section 16.1) is a secondary defense for patterns that amplify state.

### 16.3 Repetition and Consumption Interaction

Patterns with repetition and consumption ("A THEN B+ THEN C" where each B is consumed across matches in MATCH ALL mode) have complex interactions. The consumed B's can't participate in the B+ self-loop for another match path. For v1, this is handled by the general NFA with full instance tracking. Optimization deferred to v2.

### 16.4 Empty Entity Streams

Entities with zero events after scan-level filtering are skipped without initializing NFA state. The `EntityOperatorAdapter` never calls `create_state()` for them — the entity boundary detection (execution-model.md Section 4.1) simply advances past the empty span.

---

## 17. Decision Summary

| Aspect | Decision | Rationale |
|---|---|---|
| NFA algorithm | Thompson's simulation | O(n×m), no pathological cases, never backtracking |
| Why not lazy DFA | Variable bindings + time windows cause state explosion | Step counter gives DFA-equivalent speed for linear patterns |
| Event consumption | Deferred to match completion, earliest anchor consumed | Correctness with time windows (Section 4.2) |
| Match modes | FIRST and ALL (non-overlapping) | Covers funnel, retention, conversion counting |
| EMIT ALL | Optional flag; outputs all tracks entering the NFA with `step_reached` | Enables funnel analysis without completing the full sequence |
| EMIT ALL expiry | Emit at window expiry, not buffered | Enables fused aggregation without holding state |
| Negation | Poison transitions (eager kill) | Prunes dead paths immediately |
| Variable bindings | `$var` notation; each `(entity, bindings)` is an independent track | Clean model for per-segment analysis; tracks can overlap |
| Time windows | Global only (from first step) | Per-step windows add complexity with limited benefit |
| Same-timestamp events | Do not satisfy THEN (`event.ts > candidate.last_step_ts`) | Simplifies NFA; simultaneous events are not conversions |
| Candidate tracking | `CandidateEntry { anchor_ts, last_step_ts }` per `(state, track)` | Handles deferred consumption and strict ordering correctly |
| Entity state model | One `BindingTrack` per `(entity, binding values)` | Independent tracks with clean consumption semantics |
| Compact instances | Used for fused aggregation paths (step counter per track) | L1 cache friendly; demand-driven optimization |
| step_reached | Supported for all pattern classes via `state_to_step` mapping | EMIT ALL works uniformly across linear and general NFA patterns |
| Strategy selection | Compile-time based on pattern shape + demand | No runtime branching, optimal code path per query |
| Filter pushdown | Event type + property predicates pushed to scan layer | NFA sees minimal event stream |
| Event type comparison | String comparison; per-batch dictionary code optimization | Segments may have different dictionary encodings |
| Binding value type | `BindingValue` enum (5 scalar variants); NULL fails predicate | Compact, Eq/Hash, not `PropertyValue` |
| Active state limit | 10,000 per entity (configurable), drop oldest | Bounds memory without aborting query |
| Epsilon transitions | Eliminated at compile time via epsilon-closure | No runtime epsilon handling |

---

## 18. Open Questions for Other Design Docs

These questions are intentionally deferred to the design docs that own them:

- **Query Language (TASK-002):** Exact BQL grammar for MATCH FIRST/ALL, SEQUENCE, THEN, IMMEDIATELY, WITHOUT, WITHIN, EMIT ALL, and `$variable` binding. How do nested groups / alternation appear in the grammar (e.g., `A THEN (B OR C) THEN D`)? Can patterns reference properties from earlier steps beyond variable bindings (e.g., `B.amount > A.amount` — general cross-step predicates)?
- **Query Language (TASK-002):** Interaction between SEQUENCE and sessions. Does "A THEN B within the same session" use session boundaries as an event annotation, or does the NFA need to be session-aware?
- **Query Language (TASK-002):** Semantics of variable bindings with repetition — must the bound variable be constant across ALL B's in B+, or just between the first B and subsequent non-B steps?
