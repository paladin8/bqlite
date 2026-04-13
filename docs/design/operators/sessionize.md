# SESSIONIZE Operator Architecture

**Wave**: 4
**Task**: TASK-405
**Status**: draft
**Depends on**: execution-model.md, operator-traits.md, type-system.md, planner-pipeline.md, query-language.md
**Depended on by**: TASK-420 (parser: SESSIONIZE), TASK-424 (planner: Sessionize logical/physical node), TASK-428 (SessionizeOperator implementation)

---

## 1. Purpose

This document specifies the operator-level architecture for `SESSIONIZE(gap: ..., end: ...)`. It pins down:

- Session boundary rules — gap-exclusive boundary, end-event membership, gap-vs-end precedence.
- Output schema — `session_id` / `session_duration` column types, nullability, per-entity semantics.
- Per-entity state layout — session buffer, emission timing, entity-boundary flush.
- `EntityOperator` trait integration — `create_state`, `process_sub_batch`, `finish_entity` contracts.
- `WITHIN SESSION` interaction with downstream MATCH.
- Demand-driven column forwarding — which input columns are physically buffered.
- State caps for pathological entities — per-entity event cap, diagnostic channel.
- Fused aggregate shapes — what is deferred to Wave 5.
- Benchmark and edge-case matrix the implementation must satisfy.

The document does **not** cover:

- Parser surface syntax for SESSIONIZE — owned by TASK-420 and query-language.md §8.
- Logical/physical plan node shapes — owned by TASK-424 and planner/logical-plan-nodes.md.
- AST-to-logical lowering rules — owned by TASK-425.
- `DemandCapabilities` protocol — owned by TASK-427.

---

## 2. Relationship to Other Docs

| Topic | Authoritative doc | Role here |
|---|---|---|
| `EntityOperator` trait surface, lifecycle | operators/operator-traits.md | SESSIONIZE implements this trait. |
| Entity-aligned batching, sub-batch streaming | execution-model.md §3.5, §5 | SESSIONIZE relies on entity-sorted input. |
| Output schema types | type-system.md §6.3 | `session_id: Int64 NOT NULL`, `session_duration: Int64 NOT NULL`. |
| SESSIONIZE surface syntax | query-language.md §8 | `gap:`, `end:` parameters, `WITHIN SESSION`. |
| Pipeline composition rules | query-language.md §25.2 | Valid downstream operators after SESSIONIZE. |
| Demand propagation, fusion | planner-pipeline.md §7.4.2, §8.3, §9 | Fusion shapes and demand-driven column forwarding. |
| `PhysicalOperator` trait | operators/operator-traits.md §4 | Wrapped by `EntityOperatorAdapter`. |
| Layered extraction pattern | execution-model.md §4.2 | Branch only at session completion, not per-event. |
| Warning channel | execution-model.md §12.2 | `QueryWarning` for per-entity event cap. |

### 2.1 Deviations from execution-model.md §2.1

execution-model.md §2.1 summarizes SESSIONIZE's state as "Current session ID + last event timestamp." This was a Wave 0 approximation. The real state is richer: SESSIONIZE buffers the rows of the currently-open session (§5) because `session_duration` cannot be known until the session closes. The state table in execution-model.md §2.1 should be updated when TASK-428 ships; this doc is authoritative for the operator's actual state layout.

### 2.2 Fused aggregate deferral

execution-model.md §8.6 and planner-pipeline.md §7.4.2 describe SESSIONIZE fusion shapes (session-fold with internal accumulator). **These are deferred to Wave 5** per the task note §10. SESSIONIZE in v1 emits full per-session rows to a downstream STATS operator. The fusion table in §7.4.2 remains as documentation of future opportunities, not as a v1 requirement. See §10 of this document.

---

## 3. Operator Identity

The SESSIONIZE operator is the `SessionizeOperator`, a stateful per-entity operator that implements `EntityOperator` (operator-traits.md §6). It annotates each input event with `session_id` and `session_duration` columns, grouping events into sessions based on inactivity gaps and optional explicit end events.

**Crate**: `bqlite-operators` (in `src/sessionize.rs` or `src/sessionize/` module tree).

**Trait implementation**:

```rust
impl EntityOperator for SessionizeOperator {
    type State = SessionizeState;
    // ...
}
```

The operator itself (`&self`) is immutable — it carries the gap threshold, end-event set, execution configuration, and output schema. All mutable state lives in `SessionizeState`, created fresh per entity.

---

## 4. Operator Construction

`SessionizeOperator` is constructed by the engine bind step from a `SessionizePhysical` descriptor (a plain-data struct produced by the physical planner, living in `bqlite-planner`):

```rust
/// Physical descriptor for Sessionize — carried on the physical plan.
/// Materialized into a `SessionizeOperator` instance by the engine bind step.
pub struct SessionizePhysical {
    /// Minimum inactivity gap (nanoseconds) that triggers a new session.
    /// Boundary is exclusive: new session iff delta > gap_ns.
    pub gap_ns: i64,
    /// Event types that explicitly end a session. Empty = gap-only mode.
    pub end_events: Vec<String>,
    /// Demand set from downstream operators.
    pub demand: DemandSet,
    /// Columns that downstream operators need forwarded through the session buffer.
    pub forwarded_columns: Vec<ColumnId>,
    /// Fused aggregate specification. Always `None` in v1 (see §10).
    pub fused_aggregate: Option<FusableAggregate>,
}
```

The operator struct:

```rust
pub struct SessionizeOperator {
    /// Minimum inactivity gap in nanoseconds.
    /// A new session starts iff delta_ns(event_i, event_{i-1}) > gap_ns.
    gap_ns: i64,

    /// Set of event types that explicitly end a session.
    /// Empty means gap-only mode (no explicit end events).
    /// Stored as a HashSet for O(1) membership checks.
    end_events: HashSet<String>,

    /// Output schema: input columns + session_id + session_duration.
    output_schema: OperatorSchema,

    /// Column indices into the input batch, resolved once at construction.
    input_columns: SessionizeInputMap,

    /// Indices of columns to physically buffer in the session buffer.
    /// Derived from downstream demand (§9). Columns not in this set are
    /// not buffered — they are logically present in the output schema
    /// but physically dropped from the per-session buffer.
    buffered_column_indices: Vec<usize>,

    /// Per-entity open-session event cap. Default: 1,000,000.
    session_event_cap: usize,
}
```

### 4.1 SessionizeInputMap

Column indices are resolved once at construction from the input's `OperatorSchema`:

```rust
pub struct SessionizeInputMap {
    /// Index of `entity_id` in the input batch.
    pub entity_id_idx: usize,
    /// Index of `ts` (timestamp) in the input batch.
    pub ts_idx: usize,
    /// Index of `event_type` in the input batch. Required when end_events
    /// is non-empty; unused in gap-only mode.
    pub event_type_idx: Option<usize>,
}
```

`event_type_idx` is `Some` only when `end_events` is non-empty. In gap-only mode, the operator never inspects event types and does not require the `event_type` column.

---

## 5. Session Boundary Rules

These rules are authoritative. They override any conflicting statements in earlier design docs.

### 5.1 Gap Boundary — Exclusive (Strict `>`)

A new session opens if and only if the time delta between consecutive events exceeds the gap:

```
new_session = delta_ns(event_i, event_{i-1}) > gap_ns
```

At exactly `delta == gap_ns`, both events belong to the **same** session. The gap boundary itself is excluded from the set of deltas that trigger a break.

**Rationale**: "Maximum inactivity" reads naturally as "up to and including the gap is still same-session." Matches the standard sessionization convention used by analytics tools.

### 5.2 End-Event Membership — End Event Belongs to the Session It Closes

When an event `E` matches an end-event type (`E.event_type IN end_events`), `E` is the **last event of the current session**, not the first event of a new session.

This means a MATCH pattern of shape `search THEN logout` inside `WITHIN SESSION` matches within the session containing the logout, because the logout is part of the same session as the search.

**Rationale**: "End event" reads as "the event that ends the session." Avoids the awkward case where every end event is its own 1-event session.

### 5.3 Gap vs End Precedence — Gap Closes First

If event `E` arrives with `delta > gap_ns` **and** `E.event_type` is in `end_events`:

1. The prior session closes due to gap **before** `E` is considered.
2. `E` starts a fresh session.
3. `E` is an end event, so the fresh session closes immediately — `E` becomes a 1-event session.

**Rationale**: Inactivity is a property of the interval preceding `E` and is independent of `E`'s type. Once the gap has elapsed, the prior session is logically closed — `E` cannot retroactively extend it. This gives a single deterministic rule without special-casing end events.

### 5.4 End-Event List

The `end:` parameter accepts either a single event type or a parenthesized list:

```bql
| SESSIONIZE(gap: 30m, end: logout)
| SESSIONIZE(gap: 30m, end: (logout, timeout, session_end))
```

Any event whose type is in the list closes the current session (same membership rule as §5.2). The AST node carries a `Vec<EventRef>` (length >= 1; length 1 covers the single form). Duplicate names within the list are rejected at parse time (TASK-420).

### 5.5 Entity Boundary — Flush and Reset

When the entity-sorted input stream crosses from entity A to entity B:

1. A's currently-open session is closed as "end-of-entity" (same treatment as gap closure for the final session).
2. A's session buffer is flushed downstream.
3. `session_id` counter resets to `1` for B.
4. No session can span two entities.

This is a consequence of the per-entity semantics. The `EntityOperatorAdapter` (execution-model.md §4.1) detects entity boundaries and calls `finish_entity` for entity A before `create_state` for entity B.

---

## 6. Output Schema

### 6.1 Schema Definition

The output schema is `input schema UNION {session_id, session_duration}` — all input columns flow through logically (type-system.md §6.3):

| Column | Arrow Type | Nullable | Description |
|---|---|---|---|
| *(all input columns)* | *(unchanged)* | *(unchanged)* | Passed through from input |
| `session_id` | `Int64` | no | Per-entity session counter, starting at 1 |
| `session_duration` | `Int64` | no | Session duration in nanoseconds (`max_ts - min_ts`) |

### 6.2 `session_id` Semantics

- Type: `Int64`, not nullable.
- Starts at `1` for each entity's first session. Monotonically increasing by 1 within an entity.
- Resets to `1` at every entity boundary — **not globally unique**.
- `COUNT_DISTINCT(session_id)` without `GROUP BY entity_id` almost never means what a user wants — it collapses across entities that share the same session numbers.

**User-facing caveat (must be documented in query-language.md §8)**: `COUNT_DISTINCT(session_id)` without `GROUP BY entity_id` is misleading. The idiomatic per-entity session count is `COUNT_DISTINCT(session_id) GROUP BY entity_id`.

### 6.3 `session_duration` Semantics

- Type: `Int64`, not nullable. Value is nanoseconds.
- Computed as `max_ts_in_session - min_ts_in_session`.
- Single-event sessions have `session_duration == 0`.
- Trailing idle time up to the gap boundary is **not** included.

**Rationale**: Matches standard sessionization tools. Zero duration for single-event sessions is semantically correct (zero activity spanned). `AVG(session_duration)` behaves sensibly.

### 6.4 Single-Event Sessions — Emitted

Every input event belongs to exactly one session. Entities whose events are all separated by more than `gap` produce one 1-event session per event. SESSIONIZE does not drop singletons.

**Rationale**: SESSIONIZE annotates, it does not filter. Users who want to drop singletons write `WHERE session_duration > 0` downstream.

---

## 7. Per-Entity State Layout

### 7.1 SessionizeState

```rust
pub struct SessionizeState {
    /// Current session ID (starts at 1, incremented at each session boundary).
    current_session_id: i64,

    /// Timestamp of the first event in the current open session.
    session_start_ts: i64,

    /// Timestamp of the most recent event in the current open session.
    session_last_ts: i64,

    /// Buffered rows for the currently-open session.
    /// Each entry is a `RecordBatch` slice (or row-level buffer) containing
    /// only the demanded columns (§9).
    session_buffer: SessionBuffer,

    /// Number of events in the current open session (for cap enforcement).
    session_event_count: usize,

    /// Total events processed for this entity (for cap enforcement).
    entity_event_count: usize,

    /// Whether the per-entity event cap was exceeded.
    cap_exceeded: bool,

    /// Whether we are in skip-to-entity-boundary mode after cap exceeded.
    skipping: bool,
}
```

### 7.2 SessionBuffer

The session buffer holds rows for the currently-open session. Rows are buffered because `session_duration` cannot be known until the session closes.

```rust
pub struct SessionBuffer {
    /// Accumulated RecordBatch slices for the current session.
    /// Each slice retains only the columns listed in
    /// SessionizeOperator::buffered_column_indices.
    batches: Vec<RecordBatch>,

    /// Total row count across all buffered batches.
    total_rows: usize,
}
```

**Memory model**: Because input is entity-sorted, at most one entity's open session is in memory at a time. The buffer holds rows for a single session within a single entity — not across entities, not across sessions. When a session closes, the buffer is flushed and reset.

**Why buffer instead of late-patch**: `session_duration` cannot be known until the session closes. Alternatives like emitting rows immediately and patching `session_duration` later would break downstream non-null guarantees and require either random-access updates to already-emitted batches (unsafe) or sentinel values (violates schema contract).

### 7.3 Size Estimate

For a typical session of 50 events with 10 demanded columns at ~100 bytes per row: 50 x 100 = 5 KB per open session. For the cap-limited worst case (1M events): 1M x 100 = 100 MB. The cap exists specifically to bound this — see §11.

---

## 8. EntityOperator Integration

### 8.1 create_state

```rust
fn create_state(&self, entity_id: &EntityId) -> SessionizeState {
    SessionizeState {
        current_session_id: 1,
        session_start_ts: i64::MIN,  // sentinel: no events seen yet
        session_last_ts: i64::MIN,
        session_buffer: SessionBuffer::new(),
        session_event_count: 0,
        entity_event_count: 0,
        cap_exceeded: false,
        skipping: false,
    }
}
```

State starts with `session_id = 1` and sentinel timestamps. The first event initializes `session_start_ts` and `session_last_ts`.

### 8.2 process_sub_batch

For each sub-batch (one row-group, entity-aligned, up to 64K rows):

1. **Extract columns** from the input `RecordBatch` using `SessionizeInputMap` indices:
   - `ts`: `Int64Array` (nanosecond timestamps).
   - `event_type`: Only extracted when `end_events` is non-empty. If `DictionaryArray<Int32, Utf8View>`, resolve end-event strings against the batch dictionary once to build a per-batch code lookup set.

2. **Per-row loop**:

```
for each row in sub_batch:
    if state.skipping:
        continue  // skip remaining events after cap exceeded

    ts = row.ts
    event_type = row.event_type  // only if end_events non-empty

    if state.session_start_ts == i64::MIN:
        // First event for this entity — initialize session
        state.session_start_ts = ts
        state.session_last_ts = ts
        buffer_row(row)
        check_end_event(event_type)  // may close immediately
        continue

    delta = ts - state.session_last_ts

    // Step 1: Check gap boundary (§5.1)
    if delta > self.gap_ns:
        flush_session(state)          // close prior session
        start_new_session(state, ts)  // new session starts with this event

    // Step 2: Update current session
    state.session_last_ts = ts
    state.session_event_count += 1
    state.entity_event_count += 1
    buffer_row(row)

    // Step 3: Check end-event (§5.2, §5.3)
    // If gap already closed prior session (Step 1), this event started
    // a new session. If it is also an end event, this new session
    // closes immediately — producing a 1-event session.
    if self.end_events.contains(event_type):
        flush_session(state)

    // Step 4: Check per-entity event cap (§11)
    // The cap limits events buffered in a single open session.
    // When exceeded, the partial session is flushed and all remaining
    // events for this entity are skipped.
    if state.session_event_count > self.session_event_cap:
        flush_partial_session(state)
        state.skipping = true
```

3. **`flush_session`**: Emits all buffered rows with `session_id` and `session_duration` filled in:
   - `session_id` = `state.current_session_id`
   - `session_duration` = `state.session_last_ts - state.session_start_ts`
   - Appends the annotated batch to the output buffer.
   - Resets: `state.current_session_id += 1`, clears `session_buffer`, resets `session_event_count`.

4. **`start_new_session`**: Sets `session_start_ts = ts`, `session_last_ts = ts`, resets `session_event_count = 0`.

**Dictionary optimization for end-event matching**: When `event_type` arrives as `DictionaryArray<Int32, Utf8View>`, end-event membership is resolved once per sub-batch against the dictionary:

```rust
struct EndEventCodeSet {
    /// Dictionary codes that match end-event types in this batch.
    /// Empty if no end-event types appear in this batch's dictionary.
    matching_codes: HashSet<i32>,
}
```

Per-row end-event check is then an integer set lookup, not a string comparison.

### 8.3 Sub-Batch Streaming

The `EntityOperatorAdapter` guarantees:
- Sub-batches for one entity arrive consecutively, no interleaving.
- Rows within a sub-batch are sorted by `(entity_id, ts)` ascending.
- The adapter drops each sub-batch's `RecordBatch` data before producing the next.

The `SessionizeState` persists across sub-batches. A session can span multiple sub-batches — the session buffer accumulates rows across sub-batch boundaries. This is safe because the buffer only holds demanded columns (§9) and is bounded by the event cap (§11).

**Cancellation**: The SESSIONIZE operator does not check cancellation internally. Per operator-traits.md §5.2, cancellation is checked by the wrapping `EntityOperatorAdapter` between sub-batches. The worst-case cancellation latency is one sub-batch (~64K rows).

### 8.4 finish_entity

Called exactly once per entity after all sub-batches have been processed. Consumes state.

```rust
fn finish_entity(&self, state: SessionizeState) -> Option<RecordBatch> {
    // 1. If state.skipping, the entity was truncated — no final session to flush.
    //    Return whatever was already emitted (buffered output batches).
    // 2. If session_buffer is non-empty, flush the final open session
    //    (end-of-entity closes the last session — §5.5).
    // 3. Concatenate all emitted session batches into a single output RecordBatch.
    // 4. Return None if no output rows (should not happen — every event
    //    produces output unless cap-skipped).
}
```

**Multi-row output**: Unlike MATCH (which may emit 0 or 1 rows per entity), SESSIONIZE always emits as many rows as it received (minus any cap-skipped events). The `finish_entity` return is a multi-row `RecordBatch` containing all events for the entity annotated with their session columns. The `EntityOperatorAdapter` handles multi-row returns naturally (execution-model.md §7.3).

**Output accumulation**: SESSIONIZE emits completed sessions as they close during `process_sub_batch` (pushed into an output buffer). `finish_entity` flushes the final open session and returns the concatenated output. This means the output buffer grows with the entity's total event count (minus cap-skipped events), not with the session count.

### 8.5 finish_entity_into (Aggregation Fusion)

**Deferred to Wave 5** (§10). The default implementation calls `finish_entity` and feeds the result into the accumulator:

```rust
fn finish_entity_into(&self, state: SessionizeState, acc: &mut dyn Accumulator) {
    if let Some(batch) = self.finish_entity(state) {
        acc.update_batch(&batch);
    }
}
```

### 8.6 required_columns

Returns the set of input columns the operator reads:

```rust
fn required_columns(&self) -> &[String] {
    &self.required_column_names
}
```

The set is computed once at construction:
- `entity_id` — always (output, entity boundary detection is handled by the adapter).
- `ts` — always (gap computation, `session_start_ts`, `session_last_ts`).
- `event_type` — only when `end_events` is non-empty.
- All downstream-demanded forwarded columns (§9).

### 8.7 supported_demands

```rust
fn supported_demands(&self) -> DemandCapabilities {
    DemandCapabilities {
        supports_column_forwarding: true,
        supports_aggregation_fusion: false,  // v1: no fusion (§10)
    }
}
```

---

## 9. Demand-Driven Column Forwarding

### 9.1 Logical vs Physical Schema

The **output schema** advertised by SESSIONIZE is `input schema UNION {session_id, session_duration}` — all input columns flow through logically. Downstream operators see the full schema and can reference any input column.

The **physical per-session buffer** only materializes columns that downstream operators demand via `DemandSet`. Columns that are not downstream-demanded are dropped from the buffer, not from the schema.

### 9.2 Forwarding Mechanics

The physical planner propagates downstream `DemandSet` (planner-pipeline.md §9) to determine which input columns SESSIONIZE must buffer:

1. Walk the downstream demand backward from the consumer.
2. Strip `session_id` and `session_duration` (produced by SESSIONIZE, not needed from upstream).
3. The remaining demanded columns plus `ts` (always needed for gap computation) and `event_type` (if end-events configured) form `forwarded_columns`.
4. The scan layer decodes only these columns.

```
Downstream demands: {entity_id, ts, event_type, page, session_id, session_duration}
                                                                    ↓ strip produced columns
SESSIONIZE buffers:  {entity_id, ts, event_type, page}
                                                    ↓ propagate upstream
Scan decodes:        {entity_id, ts, event_type, page}
```

### 9.3 Memory Benefit

Sessions can be long (thousands of events in a 30-minute gap window). Buffering only demanded columns reduces per-session memory proportionally to the fraction of columns demanded. For a table with 50 property columns where downstream demands only 3, the buffer is ~6% of the full-column cost.

---

## 10. Fused Aggregate Shapes — Deferred to Wave 5

SESSIONIZE in v1 emits full per-session rows to the downstream STATS operator. No `SESSIONIZE -> STATS` fusion.

### 10.1 Candidates for Wave 5

These are documented as future fusion opportunities, not v1 requirements:

| Downstream pattern | Fused strategy | What's avoided |
|---|---|---|
| `STATS sessions = COUNT_DISTINCT(session_id) GROUP BY entity_id` | Per-entity session counter | No `session_id` on every event |
| `STATS avg = AVG(session_duration)` | Running `(sum, count)` of session durations | No per-event annotation |
| `STATS events = COUNT(*) GROUP BY session_id` | Events-per-session counter | No column materialization |
| `STATS first_page = FIRST_VALUE(page) GROUP BY session_id` | Per-session accumulator with column forwarding (planner-pipeline.md §8.3) | Full `session_id` materialization |

### 10.2 Rationale for Deferral

Fusion is a cross-cutting Wave 5 concern per planner-pipeline.md §5.3. The `FusedDownstream` annotation is explicitly deferred. Getting SESSIONIZE's emission semantics, state cap, and boundary rules right matters more than shaving per-session row construction in v1. The non-fused path (§8.5 default `finish_entity_into`) always works as the fallback (execution-model.md §8.7).

---

## 11. Per-Entity Session Event Cap

### 11.1 Design

Per-entity open-session state is capped at **1,000,000 events** (configurable via a future engine setting, hard default in v1). The cap limits the total number of events buffered in a single session for a single entity.

### 11.2 Enforcement

When `session_event_count` exceeds the cap during `process_sub_batch`:

1. The partially-buffered session is **flushed** with `session_duration` computed from events seen so far.
2. **Remaining events for the same entity are discarded** — SESSIONIZE sets `state.skipping = true` and skips all subsequent events until the entity boundary, then resumes normally with the next entity.
3. The operator records a per-query diagnostic (§11.3).

### 11.3 Diagnostic

The diagnostic uses the same `QueryWarning` shape used by the existing entity event limit (execution-model.md §12.2):

```rust
QueryWarning::SessionEventCapExceeded {
    entity_id: String,
    event_count: u64,
    cap: u64,
}
```

The query succeeds; it does not error. The warning is attached to the query result metadata and surfaced to the caller. Per-worker warning accumulation is capped at 1,000 entries (execution-model.md §12.2).

### 11.4 Diagnostic Channel

The operator needs access to the per-query diagnostic channel (`WorkerContext::warnings`). If this channel does not yet exist at the operator boundary when TASK-428 implements the operator, TASK-428 must either:

1. Thread `WorkerContext` (or a warning sink reference) into the `EntityOperator` interface, or
2. Use the existing `QueryContext`-based warning accumulation that other operators use, or
3. File a follow-up for the shared plumbing.

The preferred approach is option (2) — the same mechanism used by the entity event limit in the `EntityOperatorAdapter`.

### 11.5 Memory Bound

Worst case before cap fires: 1,000,000 events x ~100 bytes (demanded columns) = ~100 MB for a single session buffer. This is within the 3 GB query budget (execution-model.md §10.1). The cap exists to prevent pathological entities (malicious, buggy producer, or mega-user) from consuming unbounded memory. Spill-to-disk is out of scope for v1 (Wave 5, TASK-502).

---

## 12. WITHIN SESSION Interaction

### 12.1 Mechanism

`WITHIN SESSION` constrains a downstream MATCH to session boundaries. The implementation is simple: SESSIONIZE emits `session_id` as a monotonically increasing integer per entity. MATCH with `WITHIN SESSION` observes the `session_id` column in its input schema and expires all active NFA candidates when `session_id` increments.

```
SESSIONIZE adds session_id column (1, 1, 1, 2, 2, 3, 3, 3, ...)
                                          ↑       ↑
MATCH observes session_id changes ───────┘       └── expire all active candidates
```

No sentinel events, no window annotations, no session-aware NFA. The correctness argument: within a single session, `session_id` is constant; at the boundary between sessions, it changes by exactly one; the increment serves as the expiry trigger (query-language.md §30.2).

### 12.2 Mutual Exclusivity

`WITHIN SESSION` is mutually exclusive with `WITHIN <duration>` and `BRACKETS`. The three window forms are mutually exclusive — a MATCH expression may use at most one (query-language.md §8.1). This is enforced at parse time (TASK-420) and at plan time (TASK-425).

### 12.3 Composition

`SESSIONIZE | MATCH ... WITHIN SESSION` is the canonical composition. The pipeline composition rules (query-language.md §25.2) allow MATCH as a valid downstream of SESSIONIZE. Other valid compositions after SESSIONIZE: `WHERE`, `SELECT`, `LET`, `FIRST/LAST/NTH`, `ATTRIBUTE`, `STATS`.

`SESSIONIZE | FIRST/LAST/NTH` and `SESSIONIZE | ATTRIBUTE` remain entity-level compositions in v1 — SESSIONIZE does not implicitly make those downstream operators session-scoped (query-language.md §25.2).

---

## 13. Emission Timing

### 13.1 Rows Emitted When Sessions Close

SESSIONIZE buffers rows for the currently-open session. When a session closes (gap boundary, end event, or end-of-entity), all buffered rows are emitted downstream with their final `session_id` and `session_duration` filled in.

Emission triggers:
1. **Gap boundary**: `delta > gap_ns` on the next event. Prior session's buffer is flushed.
2. **End event**: Event matches `end_events`. Current session's buffer (including the end event) is flushed.
3. **End-of-entity**: `finish_entity` called. Final open session's buffer is flushed.
4. **Event cap exceeded**: Partial session flushed with duration computed from events seen so far (§11).

### 13.2 Output Row Construction

For each flushed session, the operator constructs an output `RecordBatch` by:

1. Taking the buffered column data (demanded columns only).
2. Appending a `session_id: Int64` column — constant value for all rows in the session.
3. Appending a `session_duration: Int64` column — constant value for all rows in the session.

Both `session_id` and `session_duration` are constant within a session, so they can be constructed as constant-valued arrays (or Arrow `ConstantArray` / repeated scalar) for the batch.

### 13.3 No Per-Event Emission

SESSIONIZE does **not** emit rows one-at-a-time as events arrive. It must buffer because `session_duration` is unknown until the session closes. This is the minimum buffering compatible with the output schema — alternatives like late-patching rows would break downstream non-null guarantees for `session_duration`.

---

## 14. Benchmark and Edge-Case Matrix

### 14.1 Edge Cases the Implementation Must Handle

| Case | Expected behavior | Test priority |
|---|---|---|
| Empty entity (0 events after filter) | No output, no state created | High |
| Single-event entity | 1 session, `session_id=1`, `session_duration=0` | High |
| All events in one session | 1 session spanning all events | High |
| All events in separate sessions (each gap > threshold) | N 1-event sessions, all with `session_duration=0` | High |
| Delta exactly equal to gap | Same session (exclusive boundary, §5.1) | High |
| Delta = gap + 1 ns | New session | High |
| End event as first event for entity | 1-event session that closes immediately | Medium |
| End event as last event for entity | Closes current session; `finish_entity` has nothing to flush | Medium |
| Gap + end event on same event (§5.3) | Gap closes prior session; event starts new 1-event session that closes via end event | High |
| Multiple end events in sequence | Each closes its session; next event starts a new one | Medium |
| Entity boundary mid-batch | Prior entity's last session flushed; new entity starts at session_id=1 | High |
| Session spanning multiple sub-batches | State persists across sub-batches; session_duration computed correctly | High |
| Sub-batch boundary mid-session | Buffer accumulates across sub-batches | High |
| Per-entity event cap exceeded | Partial flush, remaining events skipped, warning recorded | High |
| Event cap exceeded exactly at cap boundary | Cap event is included in partial flush | Medium |
| End event in end-event list vs not in list | Only listed events close sessions | Medium |
| Multiple end-event types (§5.4) | Any listed type closes the session | Medium |
| Gap-only mode (no end events) | Only gap boundaries trigger session breaks | High |
| `session_id` counter overflow | `i64` overflow at 2^63 sessions per entity — effectively impossible | Low |
| Downstream `WITHIN SESSION` with MATCH | MATCH expires candidates at `session_id` changes | High |

### 14.2 Benchmark Requirements

The following benchmarks must be satisfied by the TASK-428 implementation:

| Benchmark | Description | Target |
|---|---|---|
| **Throughput: gap-only** | 10M events, 100K entities, avg 100 events/entity, gap=30min | >100M events/sec/core |
| **Throughput: gap + end-event** | Same as above, with 1 end-event type | <5% overhead vs gap-only |
| **Throughput: gap + end-event list** | Same as above, with 5 end-event types | <10% overhead vs gap-only |
| **Memory: typical session** | 50 events/session, 10 columns, gap=30min | <10 KB per open session |
| **Memory: large session** | 100K events in one session, 10 columns | <15 MB per open session |
| **Latency: entity boundary** | Time to flush final session at entity boundary | <1 us per session |
| **Latency: session close** | Time to flush session at gap/end-event boundary | <1 us per session |
| **Dictionary event-type matching** | End-event membership via dictionary codes | Integer comparison, no string alloc |

### 14.3 Property Test Candidates

SESSIONIZE has clear invariants suitable for property testing:

1. **Session coverage**: Every input event appears in exactly one output session. No events are dropped (except by cap), no events are duplicated.
2. **Session ordering**: `session_id` is strictly monotonically increasing within an entity. For any two events in the same session, `session_id` values are equal.
3. **Duration correctness**: For every session, `session_duration == max(ts) - min(ts)` across the session's events.
4. **Gap invariant**: For any two consecutive events in the same session, `delta <= gap_ns`. For the first event of any session (except session 1), `delta > gap_ns` or the prior event was an end event.
5. **End-event invariant**: If an event matches an end-event type, it is the last event in its session.
6. **Entity isolation**: No session spans two entities. `session_id` resets to 1 at entity boundaries.

Use the `tests/src/strategies.rs` Arrow-shaped generators to produce entity-sorted event streams with varying gap distributions, entity sizes, and end-event frequencies.

---

## 15. Module Layout

The SESSIONIZE operator implementation lives in `crates/bqlite-operators/src/sessionize.rs` (or `src/sessionize/` if implementation complexity warrants a module tree):

```
sessionize.rs (or sessionize/)
  mod.rs          -- SessionizeOperator struct + EntityOperator impl
  state.rs        -- SessionizeState, SessionBuffer
  dictionary.rs   -- EndEventCodeSet for per-batch end-event resolution
```

Types that cross the planner-operator boundary:
- `SessionizePhysical` — the plain-data physical plan descriptor, lives in `bqlite-planner`.
- `SessionizeInputMap` — lives in `bqlite-operators`. The engine bind step converts `SessionizePhysical` into the concrete `SessionizeOperator` at construction time.
- `DemandSet`, `DemandCapabilities` — live in `bqlite-planner` (plan-time demand propagation).

This respects the dependency direction: `bqlite-operators -> bqlite-planner` (for plan-time types), not the reverse.

---

## 16. Decision Summary

| Aspect | Decision | Rationale |
|---|---|---|
| Gap boundary | Exclusive (`>`, not `>=`) | "Maximum inactivity" includes the boundary; standard convention |
| End-event membership | Belongs to the session it closes | "End event" = "event that ends the session"; avoids 1-event-session pathology |
| Gap vs end precedence | Gap closes first | Inactivity is independent of event type; deterministic single rule |
| `session_id` type | `Int64`, per-entity counter starting at 1 | No global uniqueness needed; user intuition ("session #1") |
| `session_duration` | `max_ts - min_ts`, nanos | Standard; single-event sessions = 0; no trailing idle time |
| Single-event sessions | Emitted | SESSIONIZE annotates, does not filter |
| Emission timing | Buffer until session closes | `session_duration` requires closed session; minimum buffering |
| Column forwarding | Demand-driven physical buffer | Sessions can be long; buffer only needed columns |
| Fused aggregates | None in v1 (Wave 5) | Getting boundary semantics right first; fusion is cross-cutting |
| Per-entity event cap | 1M events, flush partial + skip entity | Matches existing entity-event-limit failure mode |
| `WITHIN SESSION` | MATCH observes `session_id` increments | Simple, cheap, correct; no session-aware NFA needed |
| End-event list | `Vec<EventRef>`, length >= 1 | Real systems produce multiple end-of-session event types |
| Entity boundary | Flush and reset; session_id restarts at 1 | No cross-entity sessions; per-entity isolation |

---

## 17. Follow-On Implications

### 17.1 Doc Updates Required When TASK-428 Ships

- **query-language.md §8**: Add `COUNT_DISTINCT(session_id)` caveat (§6.2), document end-event list form (§5.4), document per-entity event cap behavior (§11), pin gap-exclusive rule (§5.1) and end-event membership (§5.2).
- **type-system.md §6.3**: Confirm `session_id: Int64 NOT NULL`, `session_duration: Int64 NOT NULL` (nanos).
- **execution-model.md §2.1**: Update SESSIONIZE state summary from "Current session ID + last event timestamp" to reflect actual per-session buffer.

### 17.2 Downstream Task Implications

- **TASK-420 (parser)**: `session_params` grammar accepts single `event_ref` and parenthesized list. AST carries `Vec<EventRef>` (length >= 1). Duplicate-name diagnostic within the list.
- **TASK-424 (planner)**: Logical node carries `gap`, `end_events: Vec<String>`, `forwarded_columns` (demand-driven), output schema per §6.
- **TASK-427 (DemandCapabilities)**: SESSIONIZE advertises `supports_column_forwarding: true`, `supports_aggregation_fusion: false`.
- **TASK-428 (SessionizeOperator)**: Owns the per-entity buffer, gap rule (§5.1), end-event rule (§5.2, §5.3), duration computation (§6.3), per-entity event cap with diagnostic + entity skip (§11), entity-boundary flush (§5.5). Must thread the per-query diagnostic channel.
- **Wave 5 fusion task**: Add the two fusion candidates from §10.1 to the fusion-opportunities list.

---

## 18. Open Questions Deferred to Other Tasks

- **Logical lowering rules**: How the planner lowers `PipelineStage::Sessionize` into a `Sessionize` logical node and then to `SessionizePhysical`. Owned by TASK-425.
- **`DemandCapabilities` real protocol**: The Wave 1 scaffold returns `DemandCapabilities::None`. The full protocol is a Wave 4 task (TASK-427).
- **Spill-to-disk for session buffers**: Deferred to Wave 5 (TASK-502). V1 uses the event cap as the only defense.
- **Cross-stateful-operator fusion**: `SESSIONIZE -> MATCH -> STATS` fuses MATCH with STATS but not SESSIONIZE with MATCH. Cross-operator fusion is a potential v2 enhancement (planner-pipeline.md §7.7).
