# EventSelect and SAMPLE Operator Architecture

**Wave**: 4
**Task**: TASK-411
**Status**: draft
**Depends on**: execution-model.md, operator-traits.md, type-system.md, planner-pipeline.md, query-language.md, planner/demand-protocol.md
**Depended on by**: TASK-421 (parser: FIRST/LAST/NTH + SAMPLE), TASK-424 (planner: EventSelect + Sample logical/physical nodes), TASK-425 (lowering), TASK-429 (EventSelectOperator), TASK-430 (SAMPLE pushdown)

---

## 1. Purpose

This document specifies the operator-level architecture for two independent operators that share a Wave 4 design task:

- **Block A: EventSelect** — `FIRST`, `LAST`, and `NTH` operators for per-entity event selection.
- **Block B: SAMPLE** — entity-level deterministic sampling with scan-pushdown.
- **Block C: Composition** — rules for combining these operators with each other and with other pipeline stages.

For each operator, the document pins down:

- Selection / sampling semantics and boundary rules.
- Per-event candidate filtering order and tie-breaking.
- Output schema and omitted-entity rules.
- Per-entity state layout and `EntityOperator` integration.
- Demand-driven column forwarding via `DemandCapabilities`.
- Scan-pushdown contract (SAMPLE) and scan-range extension (EventSelect `lookback:`).
- Fused aggregate shapes deferred to Wave 5.
- Edge-case matrix and benchmark targets.

The document does **not** cover:

- Parser surface syntax — owned by TASK-421 and query-language.md Section 14.1 / Section 14.2.
- Logical/physical plan node shapes — owned by TASK-424 and planner/logical-plan-nodes.md.
- AST-to-logical lowering rules — owned by TASK-425.
- `DemandCapabilities` protocol — owned by TASK-409 / TASK-427.
- SAMPLE scan-pushdown implementation — owned by TASK-430.

---

## 2. Relationship to Other Docs

| Topic | Authoritative doc | Role here |
|---|---|---|
| `EntityOperator` trait surface, lifecycle | operators/operator-traits.md | EventSelect implements this trait. |
| Entity-aligned batching, sub-batch streaming | execution-model.md Section 3.5, Section 5 | EventSelect relies on entity-sorted input. |
| Output schema types | type-system.md Section 6.7 (EventSelect), Section 6.11 (SAMPLE) | One row per entity; source schema pass-through. |
| FIRST/LAST/NTH surface syntax | query-language.md Section 14.1 | Event-type lists, WHERE predicates, `lookback:`. |
| SAMPLE surface syntax | query-language.md Section 14.2 | `fraction:`, `seed:`, determinism. |
| Pipeline composition rules | query-language.md Section 25.2 | Valid upstream/downstream operators. |
| Demand propagation, fusion | planner-pipeline.md Section 7.4.2, Section 8.3, Section 9 | Demand-driven forwarded columns. |
| `PhysicalOperator` trait | operators/operator-traits.md Section 4 | Wrapped by `EntityOperatorAdapter`. |
| Layered extraction pattern | execution-model.md Section 4.2 | Branch only at entity completion. |
| SAMPLE scan-level implementation | execution-model.md Section 7.6 | Entity-id hash filter at scan layer. |
| Hash function + stability | query-language.md Section 30.9 | xxHash64, database-UUID-derived default seed. |

### 2.1 Deviations from execution-model.md Section 2.1

execution-model.md Section 2.1 summarizes EventSelect state as "At most one retained event." This is accurate for the v1 design — EventSelect's per-entity state is a single candidate row (or nothing), which is the minimum possible state for position selection. No correction needed.

### 2.2 Fused aggregate deferral

execution-model.md Section 8.6 and planner-pipeline.md Section 7.4.2 describe EventSelect fusion shapes. **These are deferred to Wave 5** per Section 12 of this document. EventSelect in v1 emits full per-entity rows to a downstream STATS operator.

---

# Block A: EventSelect (FIRST / LAST / NTH)

## 3. Operator Identity

The `FIRST`, `LAST`, and `NTH` operators are implemented as a single `EventSelectOperator`, a stateful per-entity operator that implements `EntityOperator` (operator-traits.md Section 6). The operator is parameterized by `EventSelectKind` to distinguish the three selection modes.

**Crate**: `bqlite-operators` (in `src/event_select.rs`).

**Trait implementation**:

```rust
impl EntityOperator for EventSelectOperator {
    type State = EventSelectState;
    // ...
}
```

The operator itself (`&self`) is immutable — it carries the selection kind, event-type set, compiled predicate, execution configuration, and output schema. All mutable state lives in `EventSelectState`, created fresh per entity.

---

## 4. Operator Construction

`EventSelectOperator` is constructed by the engine bind step from an `EventSelectPhysical` descriptor (a plain-data struct produced by the physical planner, living in `bqlite-planner`):

```rust
/// Physical descriptor for EventSelect — carried on the physical plan.
/// Materialized into an `EventSelectOperator` instance by the engine bind step.
pub struct EventSelectPhysical {
    /// Selection mode: FIRST, LAST, or NTH(n).
    pub kind: EventSelectKind,
    /// Event types eligible for selection. Length >= 1.
    pub event_types: Vec<String>,
    /// Optional per-event predicate, compiled from WHERE clause.
    pub predicate: Option<CompiledExpr>,
    /// Scan-range backward extension for FIRST/NTH. None for LAST.
    pub lookback: Option<i64>,
    /// Columns that downstream operators need forwarded through the candidate row.
    pub forwarded_columns: Vec<ColumnId>,
    /// Fused aggregate specification. Always `None` in v1 (see Section 12).
    pub fused_aggregate: Option<FusableAggregate>,
}

pub enum EventSelectKind {
    First,
    Last,
    Nth(u32),
}
```

The operator struct:

```rust
pub struct EventSelectOperator {
    /// Selection mode.
    kind: EventSelectKind,

    /// Set of event types eligible for selection.
    /// Stored as a HashSet for O(1) membership checks.
    event_types: HashSet<String>,

    /// Optional compiled per-event predicate (from WHERE clause).
    /// Evaluated per-event on candidates that pass the event-type filter.
    predicate: Option<CompiledExpr>,

    /// Output schema: source-table columns (one row per entity).
    output_schema: OperatorSchema,

    /// Column indices into the input batch, resolved once at construction.
    input_columns: EventSelectInputMap,

    /// Indices of columns to physically retain in the candidate row.
    /// Derived from downstream demand (Section 10). Columns not in this set
    /// are logically present in the output schema but physically dropped.
    forwarded_column_indices: Vec<usize>,
}
```

### 4.1 EventSelectInputMap

Column indices are resolved once at construction from the input's `OperatorSchema`:

```rust
pub struct EventSelectInputMap {
    /// Index of `entity_id` in the input batch.
    pub entity_id_idx: usize,
    /// Index of `ts` (timestamp) in the input batch.
    pub ts_idx: usize,
    /// Index of `__seq_id` in the input batch.
    pub seq_id_idx: usize,
    /// Index of `event_type` in the input batch.
    pub event_type_idx: usize,
}
```

Unlike SESSIONIZE (where `event_type_idx` is optional in gap-only mode), EventSelect always requires `event_type` for the event-type membership check and `__seq_id` for deterministic tie-breaking.

### 4.2 EventSelectKind Validation

`NTH(n)` requires `n >= 1`. The parser rejects `n == 0` and negative values at parse time with a clear diagnostic. The `EventSelectKind::Nth(u32)` type enforces non-negativity; the `>= 1` invariant is validated during plan construction.

---

## 5. Selection Semantics

### 5.1 Event-Type Filter

The operator selects events whose `event_type` is in the configured event-type set. The set is populated from the grammar's `event_ref_list` production, which accepts either a single event type or a parenthesized comma-separated list.

```bql
events | FIRST(purchase)                          -- single type
events | FIRST((login, sso_login, mobile_login))  -- type list
```

The per-entity candidate loop tests `event.event_type IN event_types_set` using a `HashSet<EventTypeId>` (or dictionary-code lookup for `DictionaryArray` inputs; see Section 8.2).

Duplicate event types within the list are rejected at parse time (TASK-421).

### 5.2 WHERE Predicate — Per-Event, Before Position Selection

When a WHERE predicate is present, it is applied **per-event** on candidates that pass the event-type filter, **before** position selection:

```bql
events | NTH(purchase WHERE amount > 100, 3)
```

This returns the third `purchase` event where `amount > 100`, not the third `purchase` event overall that happens to satisfy the predicate. The filtering order is:

1. Check `event_type IN event_types_set`.
2. If (1) passes, evaluate the WHERE predicate.
3. If (2) passes (or no predicate), the event is a **qualifying event**.
4. Apply position selection (FIRST/LAST/NTH) over qualifying events.

The predicate is type-checked against the source schema once at plan time. At runtime, it is evaluated only on rows that pass the event-type filter.

**Column resolution model**: Same as TASK-406 Section 11 and Section A4 of the task note. Columns resolve against the source table's schema, not per-event-type. If some listed event types carry different column sets, missing columns resolve to NULL on those rows.

### 5.3 Position Selection Rules

**FIRST**: Selects the qualifying event with the smallest `(ts, __seq_id)` — the earliest qualifying event.

**LAST**: Selects the qualifying event with the largest `(ts, __seq_id)` — the latest qualifying event.

**NTH(n)**: Selects the `n`-th qualifying event in ascending `(ts, __seq_id)` order. `n` is 1-indexed: `NTH(event, 1)` is equivalent to `FIRST(event)`.

### 5.4 Same-`ts` Tie-Breaking by `__seq_id`

When multiple qualifying events share a `ts`:
- **FIRST** selects the event with the smallest `(ts, __seq_id)`.
- **LAST** selects the event with the largest `(ts, __seq_id)`.
- **NTH** selects the `n`-th event in `(ts, __seq_id)` ascending order.

**Rationale**: Determinism is load-bearing for test reproducibility and for repeat queries to return stable answers. `__seq_id` is already the canonical per-`ts` tiebreaker elsewhere in the codebase (matched-event ordering; TASK-407 B1 JOIN tiebreaking).

### 5.5 Omitted Entities

If an entity has **no qualifying events** (no events pass the event-type filter and WHERE predicate), the entity produces **no output row**. The entity is omitted from the result.

For **NTH(n)**: if an entity has fewer than `n` qualifying events, the entity also produces no output row. This is the natural extension of the "no matching event" rule — `NTH` promises "the n-th qualifying event"; if there isn't one, the entity has no answer.

**Rationale**: Omission is the cleanest representation. Users who want to distinguish "no events at all" from "fewer than n" can compose `NTH(e, 2)` and `NTH(e, 3)` as separate pipelines and compare counts.

---

## 6. Event-Type List Extension

### 6.1 Grammar

The `event_ref` position in all three operators accepts a parenthesized list of event types:

```
event_ref | "(" event_ref ("," event_ref)* ")"
```

### 6.2 Semantics

The operator selects the first / last / n-th event whose type is **in the list**. The optional WHERE predicate applies across all listed event types, evaluated per-event in the common (source-schema) column namespace.

```bql
events | FIRST((login, sso_login, mobile_login))
events | LAST((purchase, subscription) WHERE amount > 0)
events | NTH((page_view, mobile_page_view) WHERE url LIKE '/checkout%', 3)
```

### 6.3 Column Resolution

bqlite resolves columns against the source table's schema, not per-event-type. The WHERE predicate is type-checked against the source schema once; at runtime it only evaluates on rows whose `event_type` is in the list. If some listed event types carry different column sets, missing columns resolve to NULL on those rows.

**Consistency**: Same column resolution model as TASK-405 Section 11 (SESSIONIZE `end:` list) and TASK-406 Section 8 (ATTRIBUTE `conversion:` / `touchpoints:` lists).

### 6.4 Duplicate Diagnostics

Duplicate event types within the list are rejected at parse time (TASK-421) with a diagnostic identifying the duplicate name and its position.

---

## 7. `lookback:` Parameter

### 7.1 Applicability

`FIRST` and `NTH` (not `LAST`) accept an optional `lookback: <duration>` parameter:

```bql
events LAST 30d | FIRST(signup, lookback: 90d)
events LAST 30d | FIRST(signup WHERE plan = 'pro', lookback: 90d)
events LAST 30d | NTH(purchase WHERE amount > 0, 3, lookback: 1y)
events LAST 30d | FIRST((signup, premium_signup), lookback: 90d)
```

### 7.2 Semantics

The planner extends the source scan range backward by `lookback` from the outer time range's start. The operator observes events in the widened range. The selected event's actual `ts` is returned as-is — it may be before the outer range's start. WHERE predicates apply within the widened range. Downstream operators see the output row as any other — no "row is from the lookback zone" marker.

### 7.3 No Default — Explicit Opt-In

`FIRST` / `NTH` without `lookback:` operates only on the outer time range (current behavior preserved). No hidden widening. Users who want the true-first semantic must write `lookback:` explicitly.

**Rationale**: BQL's principle is "no hidden magic" — the outer time range is explicit user intent, and silently widening would surprise users who wrote `LAST 30d` expecting their scan bounds respected. The "right" default is use-case-dependent (new-user onboarding wants 90d, fraud detection wants years); picking any fixed default is a footgun.

### 7.4 Unbounded Lookback

There is no `lookback: ALL` sentinel. Users who want "scan all time" omit the outer time range entirely (`events | FIRST(signup)`). `lookback:` is always a bounded, relative-duration extension.

**Rationale**: Keeps the surface small; avoids introducing an `ALL` / `unbounded` enum value that would then want to propagate to other duration-typed parameters across the language.

### 7.5 LAST Does Not Accept `lookback:`

LAST's natural bound is the outer range's end (or "now"); there is no forward-looking analog because time's arrow points forward and future events don't exist at query time. Users who want "last signup through a specific past deadline" bound the outer range with `BETWEEN`. The parser rejects `lookback:` on LAST at parse time with a diagnostic.

### 7.6 Operator Transparency

No operator-side change for `lookback:` — the widening is transparent at the operator boundary. The operator simply sees a wider event stream from the scan. The output row carries the real `ts` of the selected event regardless of whether it falls inside or outside the outer range.

---

## 8. Per-Entity State Layout

### 8.1 EventSelectState

```rust
pub struct EventSelectState {
    /// The current best candidate row for FIRST / LAST, or the qualifying
    /// event counter + candidate for NTH.
    candidate: EventSelectCandidate,
}

enum EventSelectCandidate {
    /// FIRST: retains the first qualifying event seen.
    /// Once set, no further updates needed (input is ts-ascending).
    First {
        row: Option<CandidateRow>,
    },
    /// LAST: continuously updated to the latest qualifying event.
    Last {
        row: Option<CandidateRow>,
    },
    /// NTH(n): counts qualifying events; retains the n-th.
    Nth {
        n: u32,
        qualifying_count: u32,
        row: Option<CandidateRow>,
    },
}
```

### 8.2 CandidateRow

The candidate row holds only the columns demanded by downstream operators (Section 10):

```rust
struct CandidateRow {
    /// Retained column values for the selected event.
    /// One `ScalarValue` per forwarded column index.
    values: Vec<ScalarValue>,
}
```

**Memory model**: At most one `CandidateRow` exists per entity at any time. This is the minimum possible per-entity state for position selection — far smaller than SESSIONIZE (which buffers an entire session) or ATTRIBUTE (which maintains a deque). For a typical entity with 10 demanded columns at ~100 bytes per column, the per-entity state is ~1 KB.

### 8.3 FIRST Optimization

Because input is entity-sorted and `(ts, __seq_id)`-ascending within each entity, the first qualifying event is always the first one encountered. Once `First { row: Some(_) }` is set, all subsequent events for the same entity can be skipped without evaluation. The operator sets an internal `done` flag per entity.

This is a significant optimization: for entities with millions of events, FIRST processes only events up to (and including) the first qualifying one.

### 8.4 NTH Early Termination

Similarly, once `Nth { qualifying_count == n }`, the n-th qualifying event has been found and retained. All subsequent events for the entity can be skipped.

### 8.5 LAST — Full Scan Required

LAST must scan all events for the entity because the latest qualifying event is only known after all events are processed. The candidate row is overwritten each time a new qualifying event is encountered.

**Performance implication**: LAST is inherently O(events-per-entity) regardless of selectivity. This is unavoidable without reverse-scan support (which is not in the v1 storage contract). The per-event cost is low: one event-type set lookup, one optional predicate evaluation, and one conditional candidate-row overwrite.

---

## 9. EntityOperator Integration

### 9.1 create_state

```rust
fn create_state(&self, _entity_id: &EntityId) -> EventSelectState {
    EventSelectState {
        candidate: match self.kind {
            EventSelectKind::First => EventSelectCandidate::First { row: None },
            EventSelectKind::Last => EventSelectCandidate::Last { row: None },
            EventSelectKind::Nth(n) => EventSelectCandidate::Nth {
                n,
                qualifying_count: 0,
                row: None,
            },
        },
    }
}
```

### 9.2 process_sub_batch

For each sub-batch (one row-group, entity-aligned, up to 64K rows):

1. **Extract columns** from the input `RecordBatch` using `EventSelectInputMap` indices:
   - `ts`: `Int64Array` (nanosecond timestamps).
   - `__seq_id`: `Int64Array`.
   - `event_type`: If `DictionaryArray<Int32, Utf8View>`, resolve event-type strings against the batch dictionary once to build a per-batch code lookup set.

2. **Per-row loop**:

```
for each row in sub_batch:
    // Early termination for FIRST and NTH (Section 8.3, 8.4)
    if state.is_done():
        break  // skip remaining events for this entity

    event_type = row.event_type

    // Step 1: Event-type filter (Section 5.1)
    if event_type NOT IN self.event_types:
        continue

    // Step 2: WHERE predicate (Section 5.2)
    if self.predicate is Some(pred):
        if not pred.evaluate(row):
            continue

    // Event is a qualifying event.

    match state.candidate:
        First { row: ref mut candidate }:
            if candidate.is_none():
                *candidate = Some(extract_candidate_row(row))
                // Mark done — first qualifying event found (Section 8.3)
                state.set_done()

        Last { row: ref mut candidate }:
            // Always overwrite — last qualifying event so far
            *candidate = Some(extract_candidate_row(row))

        Nth { n, ref mut qualifying_count, ref mut row }:
            *qualifying_count += 1
            if *qualifying_count == n:
                *row = Some(extract_candidate_row(row))
                // Mark done — n-th qualifying event found (Section 8.4)
                state.set_done()
```

3. **`extract_candidate_row`**: Extracts only the forwarded column values from the input batch row into a `CandidateRow`. Uses the `forwarded_column_indices` resolved at construction.

**Dictionary optimization for event-type matching**: Same pattern as SESSIONIZE (sessionize.md Section 8.2). When `event_type` arrives as `DictionaryArray<Int32, Utf8View>`, event-type membership is resolved once per sub-batch against the dictionary:

```rust
struct EventTypeCodeSet {
    /// Dictionary codes that match configured event types in this batch.
    matching_codes: HashSet<i32>,
}
```

Per-row event-type check is then an integer set lookup, not a string comparison.

### 9.3 Sub-Batch Streaming

The `EntityOperatorAdapter` guarantees:
- Sub-batches for one entity arrive consecutively, no interleaving.
- Rows within a sub-batch are sorted by `(entity_id, ts, __seq_id)` ascending.
- The adapter drops each sub-batch's `RecordBatch` data before producing the next.

The `EventSelectState` persists across sub-batches. A qualifying event may be in any sub-batch.

**Cancellation**: The EventSelect operator does not check cancellation internally. Per operator-traits.md Section 5.2, cancellation is checked by the wrapping `EntityOperatorAdapter` between sub-batches. For FIRST/NTH with early termination, the effective cancellation latency is minimal since the operator breaks out of the per-row loop once done.

### 9.4 finish_entity

Called exactly once per entity after all sub-batches have been processed. Consumes state.

```rust
fn finish_entity(&self, state: EventSelectState) -> Option<RecordBatch> {
    // Extract the candidate row from state.
    let candidate_row = match state.candidate {
        EventSelectCandidate::First { row } => row,
        EventSelectCandidate::Last { row } => row,
        EventSelectCandidate::Nth { row, .. } => row,
    };

    // If no candidate (no qualifying event, or fewer than n for NTH):
    // return None — entity is omitted (Section 5.5).
    let row = candidate_row?;

    // Construct a single-row RecordBatch from the candidate row's
    // forwarded column values, conforming to the output schema.
    Some(build_output_batch(row))
}
```

**Single-row output**: Unlike SESSIONIZE (which emits as many rows as it received), EventSelect always emits exactly 0 or 1 rows per entity. The `EntityOperatorAdapter` handles both naturally (execution-model.md Section 7.3).

### 9.5 finish_entity_into (Aggregation Fusion)

**Deferred to Wave 5** (Section 12). The default implementation calls `finish_entity` and feeds the result into the accumulator:

```rust
fn finish_entity_into(&self, state: EventSelectState, acc: &mut dyn Accumulator) {
    if let Some(batch) = self.finish_entity(state) {
        acc.update_batch(&batch);
    }
}
```

### 9.6 required_columns

Returns the set of input columns the operator reads:

```rust
fn required_columns(&self) -> &[String] {
    &self.required_column_names
}
```

The set is computed once at construction:
- `entity_id` — always (output column, entity boundary detection handled by adapter).
- `ts` — always (output column, tie-breaking).
- `__seq_id` — always (tie-breaking, Section 5.4).
- `event_type` — always (event-type membership check).
- All downstream-demanded forwarded columns (Section 10).
- All columns referenced by the WHERE predicate (if present).

### 9.7 supported_demands

```rust
fn supported_demands(&self) -> DemandCapabilities {
    DemandCapabilities {
        supports_step_reached: false,
        supports_match_count: false,
        supports_full_detail: false,
        supports_aggregation_fusion: false,           // v1: no fusion (Section 12)
        supports_step_property_forwarding: false,
        supports_forwarded_columns: true,             // generic column forwarding (§10)
        supports_eager_group_emit: false,             // reserved for Wave 5
    }
}
```

See `docs/design/planner/demand-protocol.md` §6.3 for the canonical capability table.

---

## 10. Demand-Driven Column Forwarding

### 10.1 Logical vs Physical Schema

The **output schema** advertised by EventSelect is the full source-table schema — all source columns flow through logically (type-system.md Section 6.7). Downstream operators see the full schema and can reference any source column.

The **physical per-entity candidate row** only materializes columns that downstream operators demand via `DemandSet`. Columns not downstream-demanded are dropped from the candidate row, not from the schema.

### 10.2 Forwarding Mechanics

The physical planner propagates downstream `DemandSet` (planner-pipeline.md Section 9) to determine which source columns EventSelect must retain:

1. Walk the downstream demand backward from the consumer.
2. The demanded columns plus `ts`, `__seq_id`, `event_type`, and `entity_id` (always needed) form `forwarded_columns`.
3. Columns referenced by the WHERE predicate are added to the required set for scan but are not necessarily forwarded to output (only if also demanded downstream).
4. The scan layer decodes only required columns.

```
Downstream demands: {entity_id, ts, event_type, amount}
                                                            |  plus always-needed
EventSelect retains: {entity_id, ts, __seq_id, event_type, amount}
                                                            |  propagate upstream
Scan decodes:        {entity_id, ts, __seq_id, event_type, amount}
```

### 10.3 Memory Benefit

EventSelect's per-entity state is a single candidate row, so the absolute memory wins from demand-driven forwarding are smaller than for SESSIONIZE or ATTRIBUTE. However, the convention should be uniform across stateful operators (consistent with SESSIONIZE Section 9 and ATTRIBUTE Section 10), and fewer retained columns also reduce the candidate-row extraction cost per qualifying event.

---

## 11. Scan-Range Extension for `lookback:`

When a query constrains the outer time range (e.g., `events LAST 30d | FIRST(signup, lookback: 90d)`), the planner **extends the scan range backward by `lookback`** from the outer time range's start:

- Scan range: `[outer_start - lookback, outer_end)`
- The operator sees all events in the widened range.
- The selected event's `ts` is returned as-is, even if it falls before `outer_start`.

**No operator-side change**: The widening is transparent at the operator boundary. The operator simply sees a wider event stream and selects from it. This parallels the ATTRIBUTE Section 12 scan-range extension for `window:` and the MATCH lookback widening for RETENTION brackets.

**Difference from ATTRIBUTE**: ATTRIBUTE restricts conversion emission to `[outer_start, outer_end)` using an internal conversion-range filter, because touchpoints from the extended range are not triggers. EventSelect has no such restriction — the selected event may legitimately be in the lookback zone (that's the point of `lookback:`).

---

## 12. Fused Aggregate Shapes — Deferred to Wave 5

EventSelect in v1 emits full per-entity rows to the downstream STATS operator. No `EventSelect -> STATS` fusion.

### 12.1 Candidates for Wave 5

These are documented as future fusion opportunities, not v1 requirements:

| Downstream pattern | Fused strategy | What's avoided |
|---|---|---|
| `FIRST(event) \| STATS COUNT(*)` | Per-entity presence boolean aggregated into a single counter | No per-entity row materialization |
| `FIRST(event) \| STATS AVG(property) GROUP BY group_key` | Single-row-per-entity extraction fed directly into grouped aggregate | Intermediate RecordBatch construction |

### 12.2 Rationale for Deferral

Same rationale as SESSIONIZE Section 10 and ATTRIBUTE Section 13. `FusedDownstream` is an explicit Wave 5 concern per planner-pipeline.md Section 5.3. Unfused EventSelect is cheap — per-entity state is a single candidate row; typical output row counts are per-entity (millions, not billions). The overhead of materializing a single-row RecordBatch per entity and feeding it to STATS is minimal.

---

## 13. Per-Entity Event Cap — Not Required

Unlike SESSIONIZE (sessionize.md Section 11, 1M-event cap) and ATTRIBUTE (attribute.md Section 9, 1M-touchpoint cap), EventSelect does **not** require a per-entity event cap. The per-entity state is O(1) regardless of entity size — a single `CandidateRow` (Section 8.2). Even for an entity with billions of events, the operator holds at most one candidate row in memory at any time. FIRST and NTH benefit from early termination (Sections 8.3, 8.4), which bounds processing time. LAST processes all events but holds constant memory.

No cap, no diagnostic channel, no skip-to-entity-boundary logic needed.

---

# Block B: SAMPLE

## 14. Operator Identity

SAMPLE is **not** an `EntityOperator`. It is a scan-level filter that operates on entity IDs, not on event rows. The SAMPLE filter is pushed down to the storage layer (TASK-430) to avoid reading segments for non-sampled entities.

**Execution model**: SAMPLE is implemented as a predicate on the entity-id column, evaluated at the scan layer before events are materialized. It does not maintain per-entity state, does not buffer rows, and does not implement the `EntityOperator` trait.

**Crate placement**:
- The hash-based filter logic lives in `bqlite-operators` (scan module) or `bqlite-storage` (reader), depending on TASK-430's implementation.
- The `SamplePhysical` descriptor lives in `bqlite-planner`.

---

## 15. SAMPLE Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `fraction` | Float literal | yes | Fraction of entities to include, in `[0.0, 1.0]` inclusive both ends. |
| `seed` | Integer literal | no | Explicit seed for deterministic sampling. Default: database-UUID-derived. |

### 15.1 `fraction:` Range

`fraction: 0.0` (empty output) and `fraction: 1.0` (pass-through) are both legal and not special-cased. Values outside `[0.0, 1.0]` are parse-time errors.

**Rationale**: Both boundaries have legitimate uses — `0.0` for test fixtures that verify empty-cohort handling, `1.0` as a dev-time toggle that flips sampling off without rewriting the pipeline. No reason to reject either.

### 15.2 No `count:` Parameter

SAMPLE accepts only `fraction:`. The `count:` parameter is **removed** from the v1 surface.

Revised grammar:

```
sample_op := SAMPLE "(" "fraction" ":" number ("," "seed" ":" integer)? ")"
```

Users who need a target count:
- For approximate-N sampling, compute `fraction = N / entity_count_estimate` manually.
- For exact-N sampling, use `ORDER BY <deterministic_expr> | LIMIT N` on an entity-level projection.

**Rationale**: `count:` was semantically fraught under the scan-pushdown contract (TASK-430):
- Approximate semantics (convert to fraction using catalog entity count; actual output is `N +/- sqrt(N)`) is surprising.
- Two-pass execution (count entities, then sample) doubles scan cost.
- Reservoir sampling defeats the pushdown entirely.

Rather than paper over the tradeoff, drop the parameter and make available modes (`fraction:` for pushdown, `ORDER BY ... LIMIT N` for exact count) explicit and composable.

---

## 16. Hash Function and Determinism

### 16.1 xxHash64 — Pinned Forever

SAMPLE hashes entity-id values with **xxHash64**, seeded per-query with either the explicit `seed:` parameter value or the database-UUID-derived default seed (per query-language.md Section 30.9).

The hash function is pinned with an explicit stability contract: **repeat queries on the same database with the same seed always return the same sampled entity set, indefinitely**.

- Bumping the hash function to a different algorithm is a user-visible breaking change gated behind a major version bump with a prominent migration note.
- The specific seeding protocol (how the explicit seed combines with the hash input) is the same across all bqlite versions.

**Rationale**: xxHash64 is widely used, fast, has good distribution, and has stable Rust implementations (the `xxhash-rust` crate). Pinning a specific hash is required for reproducibility; `xxHash64` matches industry convention (BigQuery, Presto, Spark use it in analogous sampling paths).

### 16.2 Entity-ID Byte Serialization

The hash input is the canonical byte representation of the entity-id value:
- **String entity keys**: UTF-8 bytes.
- **Int entity keys**: little-endian 8-byte representation.

This serialization is documented as part of the stability contract — the hash output depends on it.

### 16.3 Threshold Test

An entity is included in the sample iff:

```
xxhash64(entity_id_bytes, seed) < fraction * u64::MAX
```

**Boundary cases**: `fraction: 0.0` produces a threshold of `0` — no entity passes (strict `<`). `fraction: 1.0` is short-circuited: the implementation includes all entities without computing the hash. This avoids a theoretical `1/2^64` probability of excluding an entity whose hash is exactly `u64::MAX` and ensures `fraction: 1.0` is a true pass-through as documented in Section 15.1.

---

## 17. Population Semantics

### 17.1 Source-Table Invariant

SAMPLE's "population" is the **source-table entity set**, regardless of which filters sit upstream in the pipeline. Formally, for any entity-key-independent predicate `P`:

```
events | WHERE P | SAMPLE(fraction: f) === events | SAMPLE(fraction: f) | WHERE P
```

The hash is computed over `entity_id` alone — whether an entity has any events matching `P` doesn't affect whether the entity is "in" the sampled set.

**Rationale**: SAMPLE's semantic promise is "pick f% of entities" (query-language.md Section 14.2). Making SAMPLE's population depend on upstream WHERE state would mean two logically equivalent queries produce different sampled sets, breaking both user intuition and the pushdown contract.

### 17.2 Output Interaction with Upstream Filters

The output row set is `sampled_entities INTERSECT filtered_events`. A sampled entity with no events matching upstream WHERE contributes zero rows to output but is still "in" the sampled set.

**Must be documented in query-language.md Section 14.2**.

### 17.3 SAMPLE + `IN alias` / `IN QUERY` Cohorts

```bql
events | WHERE entity_id IN churned | SAMPLE(fraction: 0.1)
```

Produces `(churned INTERSECT sampled)` — 10% of the **full source population**, further filtered to only those in `churned`. Expected output size is `|churned| * 0.1` (assuming the cohort's entity-id distribution matches the source's).

Users who want "10% of the churned cohort" (sampling *within* the cohort) write:

```bql
sampled_churned = churned | SAMPLE(fraction: 0.1)
events | WHERE entity_id IN sampled_churned
```

or equivalently:

```bql
events | SAMPLE(fraction: 0.1) | WHERE entity_id IN churned
```

which, per Section 17.1, is identical to the first form because SAMPLE's population is source-invariant.

**Rationale**: Strict consequence of the source-table invariant (Section 17.1). SAMPLE is not cohort-aware; it samples the population.

---

## 18. Scan Pushdown Contract

SAMPLE is designed to be pushed down to the scan layer so non-sampled entities are never read. The pushdown path is owned by TASK-430; this section specifies the contract.

### 18.1 Pushdown Mechanics

Because SAMPLE is population-invariant under stateless upstream filters (Section 17.1), the planner can always push the sample filter into the scan regardless of what sits between `source` and `SAMPLE` (as long as intermediate stages are stateless — WHERE, SELECT, LET).

The scan-layer filter computes:

```
xxhash64(entity_id_bytes, seed) < fraction * u64::MAX
```

...for each entity encountered. Entities that fail the threshold test are skipped entirely — their events are never read from storage.

### 18.2 Pre-Pushdown Fallback

If the planner cannot push SAMPLE into the scan (e.g., a stateful operator sits between the source and SAMPLE), SAMPLE falls back to an entity-level filter operator above the scan. The filter evaluates the same hash threshold per entity and drops all events for non-sampled entities. This is semantically correct but loses the I/O savings of pushdown.

### 18.3 Physical Descriptor

```rust
pub struct SamplePhysical {
    pub fraction: f64,
    pub seed: Option<i64>,
}
```

When the seed is `None`, the engine resolves it from the database manifest's UUID at execution time.

---

# Block C: Composition

## 19. Composition Rules

### 19.1 `SESSIONIZE | FIRST/LAST/NTH` — Allowed, Entity-Level

query-language.md Section 25.2's downstream table is updated to list FIRST/LAST/NTH as valid downstreams of SESSIONIZE. The composition produces **entity-level** (not session-level) selection — the operator picks the first/last/n-th event per *entity*, not per session.

`session_id` and `session_duration` flow through as forwarded columns (demand-driven per Section 10) when referenced downstream. Users who want per-session selection express it explicitly using a window function or MATCH.

**Rationale**: Consistent with TASK-406 Section 14.1's `SESSIONIZE | ATTRIBUTE` decision — allow the composition without adding operator-side session awareness. Per-session selection is a substantive semantic feature that deserves its own design pass.

### 19.2 Chained EventSelects — Allowed, Semantically Empty

`events | FIRST(signup) | LAST(purchase)` is legal. The planner does not reject it, and runtime returns empty for every entity (after FIRST(signup), each entity has exactly one row which is a signup and cannot be a purchase). No special rule, no warning.

**Rationale**: The composition is semantically well-defined even if practically useless. Rejecting at plan time requires a "no second EventSelect after an EventSelect" rule with edge cases around intermediate WHERE/SELECT. Warning channels don't exist in Wave 4. Empty output is self-explanatory.

### 19.3 Valid Downstreams of EventSelect

Per query-language.md Section 25.2:

| Downstream | Notes |
|---|---|
| `WHERE` | Filter the selected event's properties |
| `SELECT` / `LET` | Project / compute derived columns |
| `STATS` | Aggregate over per-entity selected events |
| `ORDER BY` | Sort the entity-level output |
| `LIMIT` | Truncate output |

### 19.4 Valid Upstreams of SAMPLE

SAMPLE is typically the first operator after the source because it is a scan-level operator that works best when pushed all the way down. Per query-language.md Section 25.2:

| Upstream | Downstream of SAMPLE |
|---|---|
| Source (table) | WHERE, SELECT, LET, MATCH, FUNNEL, RETENTION, SESSIONIZE, FIRST/LAST/NTH, ATTRIBUTE, STATS |

### 19.5 `SAMPLE | FIRST/LAST/NTH` — Allowed

The sampled entity set feeds into EventSelect. Per Section 17.1, the sampled set is population-invariant. EventSelect operates on the (smaller) sampled entity stream.

### 19.6 `MATCH | FIRST/LAST/NTH` — Valid Pipeline Composition

MATCH emits per-match rows with a different schema (step-property columns, `match_duration`, etc.). EventSelect downstream of MATCH selects from the match output rows, not from raw events. This is semantically well-defined: "first match row per entity" is a valid query (e.g., `MATCH ALL SEQUENCE(...) | FIRST(purchase)` selects the first match-row whose matched event type is `purchase`).

---

## 20. Edge-Case Matrix

### 20.1 EventSelect Edge Cases

The implementation (TASK-429) must handle and test:

| Case | Expected behavior | Test priority |
|---|---|---|
| **Empty entity** (0 events after filter) | No output row (entity omitted, Section 5.5) | High |
| **Single-event entity, matching** | One output row with that event | High |
| **Single-event entity, non-matching type** | No output row | High |
| **Single-event entity, WHERE predicate fails** | No output row | High |
| **FIRST with all events qualifying** | Selects the first event `(min ts, min __seq_id)` | High |
| **LAST with all events qualifying** | Selects the last event `(max ts, max __seq_id)` | High |
| **NTH(3) with exactly 3 qualifying events** | Selects the third qualifying event | High |
| **NTH(3) with only 2 qualifying events** | No output row (Section 5.5) | High |
| **NTH(1) equivalence with FIRST** | Same output as FIRST for the same event-type set and predicate | Medium |
| **Same-`ts` tie-breaking** (Section 5.4) | FIRST picks smallest `__seq_id`; LAST picks largest; NTH counts by `__seq_id` order | High |
| **Event-type list with multiple types** | Qualifying events from any listed type are considered | High |
| **Event-type list with non-overlapping column sets** | WHERE predicate evaluates NULL for missing columns; output row has NULL for missing properties | Medium |
| **WHERE predicate rejects all candidates of one type** | Only events of other types in the list qualify | Medium |
| **FIRST with early termination** (Section 8.3) | Remaining events for the entity are skipped after first qualifying event found | High |
| **NTH with early termination** (Section 8.4) | Remaining events skipped after n-th qualifying event found | High |
| **LAST full scan** (Section 8.5) | All events processed; last qualifying event selected | High |
| **Entity boundary mid-batch** | Prior entity's candidate emitted (or omitted); next entity starts fresh | High |
| **Candidate spanning multiple sub-batches** | State persists across sub-batches; selection is correct | High |
| **`lookback:` extends scan range** | Events before outer time range are visible to the operator; selected event may have `ts < outer_start` | Medium |
| **Dictionary-encoded event_type** | Event-type matching uses dictionary codes, not string comparison | Medium |
| **Large entity with millions of events, FIRST** | Only processes events up to first qualifying event (early termination) | High |
| **Large entity with millions of events, LAST** | Processes all events (no early termination); candidate row overwritten per qualifying event | High |
| **Chained EventSelects** (Section 19.2) | Second EventSelect sees single-row entities from first; likely produces empty output | Low |

### 20.2 SAMPLE Edge Cases

The implementation (TASK-430) must handle and test:

| Case | Expected behavior | Test priority |
|---|---|---|
| **`fraction: 0.0`** | Empty output — no entity passes threshold | High |
| **`fraction: 1.0`** | Pass-through — all entities included | High |
| **`fraction: 0.5`** | Approximately half of entities included | High |
| **Explicit `seed:`** | Same seed + same entity set = same sampled entities across runs | High |
| **Default seed (database-UUID-derived)** | Deterministic within same database; different across database clones | Medium |
| **Different seeds, same fraction** | Different entity sets selected | Medium |
| **String entity keys** | Hash computed over UTF-8 bytes | High |
| **Int entity keys** | Hash computed over little-endian 8 bytes | High |
| **Empty table** | Empty output | Medium |
| **Single-entity table, fraction: 0.5** | Entity either included or not (deterministic based on hash) | Medium |
| **SAMPLE after WHERE** | Same sampled entity set as SAMPLE before WHERE (Section 17.1) | High |
| **SAMPLE + `IN alias` cohort** | Sampled from full population, then intersected with cohort (Section 17.3) | Medium |
| **Pushdown: entities skipped at scan** | Non-sampled entities never read from storage | High |
| **Fallback: non-pushdown path** | Same results as pushdown, just without I/O savings | Medium |

---

## 21. Benchmark Targets

### 21.1 EventSelect Benchmarks

TASK-429 must include benchmarks for:

| Benchmark | Description | Target |
|---|---|---|
| **FIRST throughput** | 10M events, 100K entities, 100 events/entity, single event type | >200M events/sec/core (early termination dominates) |
| **LAST throughput** | Same as above | >100M events/sec/core (full scan, cheap per-event work) |
| **NTH(5) throughput** | Same as above, 10 qualifying events per entity on average | >150M events/sec/core (early termination after 5th) |
| **FIRST with WHERE predicate** | Same as above, predicate filters 50% of events | >150M events/sec/core |
| **Event-type list matching** | 5 event types in list, dictionary-encoded input | Integer comparison, no string alloc |
| **Memory per entity** | 10 demanded columns | <2 KB per entity (single candidate row) |
| **Entity boundary overhead** | 100K entities, state creation + teardown per entity | <500 ns per entity |

### 21.2 SAMPLE Benchmarks

TASK-430 must include benchmarks for:

| Benchmark | Description | Target |
|---|---|---|
| **Pushdown throughput** | 1M entities, fraction: 0.1, at scan layer | >50M entities/sec/core (hash + threshold only) |
| **Hash computation** | xxHash64 over 32-byte string entity IDs | <10 ns per entity |
| **Determinism verification** | Same seed, same entity set, 3 runs | Bit-identical entity sets |

---

## 22. Property Test Candidates

### 22.1 EventSelect Properties

EventSelect has clear invariants suitable for property testing:

1. **Output cardinality**: For any entity event stream, the operator emits exactly 0 or 1 rows per entity. Never more than 1.
2. **FIRST correctness**: If the operator emits a row, that row's `(ts, __seq_id)` is the minimum among all qualifying events for the entity.
3. **LAST correctness**: If the operator emits a row, that row's `(ts, __seq_id)` is the maximum among all qualifying events for the entity.
4. **NTH correctness**: If the operator emits a row for NTH(n), there are exactly `n - 1` qualifying events with smaller `(ts, __seq_id)`.
5. **Omission invariant**: If the operator emits no row, the entity has fewer than `n` qualifying events (where `n = 1` for FIRST/LAST).
6. **Entity isolation**: No output row contains data from a different entity.
7. **NTH(1) == FIRST equivalence**: `NTH(event, 1)` and `FIRST(event)` produce identical output for the same event-type set and predicate.

### 22.2 SAMPLE Properties

1. **Determinism**: For any entity set and seed, repeated evaluation produces the same sampled set.
2. **Monotonicity**: If entity E is sampled at `fraction: f1`, it is also sampled at any `fraction: f2 > f1` (with the same seed).
3. **Boundary**: `fraction: 0.0` produces empty output; `fraction: 1.0` includes all entities.
4. **Population invariance**: The sampled entity set is independent of upstream stateless filters (Section 17.1).

Use the `tests/src/strategies.rs` Arrow-shaped generators to produce entity-sorted event streams with varying event-type distributions, entity sizes, and predicate selectivities.

---

## 23. Module Layout

### 23.1 EventSelect

The EventSelect operator implementation lives in `crates/bqlite-operators/src/event_select.rs`:

```
event_select.rs
  EventSelectOperator    -- struct + EntityOperator impl
  EventSelectState       -- per-entity state
  EventSelectCandidate   -- First/Last/Nth variant
  CandidateRow           -- forwarded column values
  EventSelectInputMap    -- resolved column indices
  EventTypeCodeSet       -- per-batch dictionary code lookup
```

Types that cross the planner-operator boundary:
- `EventSelectPhysical`, `EventSelectKind` — plain-data physical plan descriptors, live in `bqlite-planner`.
- `EventSelectInputMap` — lives in `bqlite-operators`. The engine bind step converts `EventSelectPhysical` into the concrete `EventSelectOperator`.
- `DemandSet`, `DemandCapabilities` — live in `bqlite-planner` (plan-time demand propagation).

### 23.2 SAMPLE

SAMPLE has no dedicated operator module. The hash-threshold filter lives in the scan path:

- `SamplePhysical` — lives in `bqlite-planner`.
- Hash filter implementation — lives in `bqlite-operators` (scan module) or `bqlite-storage` (reader), per TASK-430.

---

## 24. Decision Summary

| Aspect | Decision | Rationale |
|---|---|---|
| Single operator for FIRST/LAST/NTH | `EventSelectOperator` parameterized by `EventSelectKind` | Same state shape, same event-type filter, same predicate evaluation; only position selection differs |
| NTH `n` range | `n >= 1`, positive integer literal only | No reverse indexing; `LAST` covers `-1`; `n == 0` is parse-time error |
| WHERE position | `NTH(event WHERE pred, n)` — predicate binds to event, not to position | Consistent with MATCH step syntax; grammar production in Section 26 |
| Same-`ts` tie-breaking | `__seq_id` ascending | Determinism for reproducibility; consistent with MATCH, JOIN tiebreaking |
| Fewer-than-n entities | Omitted (no output row) | Clean representation; composable with downstream operators |
| Event-type list | Parenthesized comma-separated list, `Vec<EventRef>` | Consistent with SESSIONIZE `end:`, ATTRIBUTE `conversion:`/`touchpoints:` |
| Column resolution | Source-schema level, not per-event-type | Same model as ATTRIBUTE; missing columns resolve to NULL |
| `lookback:` | FIRST/NTH only; explicit opt-in, no default widening | No hidden magic; use-case-dependent default is a footgun |
| `lookback:` unbounded | No `ALL` sentinel; omit outer range for full scan | Keeps surface small |
| LAST rejects `lookback:` | Parse-time error | No forward-looking analog |
| Demand-driven forwarding | Physical candidate row retains only demanded columns | Uniform convention across stateful operators |
| Fused aggregates | None in v1 (Wave 5) | Per-entity state is one row; overhead is minimal |
| FIRST early termination | Break out of per-row loop after first qualifying event | Significant perf win for large entities |
| NTH early termination | Break after n-th qualifying event found | Same optimization |
| SAMPLE `fraction:` range | `[0.0, 1.0]` inclusive | Both boundaries have legitimate uses |
| SAMPLE `count:` parameter | Removed entirely | Semantically fraught under pushdown; explicit alternatives exist |
| SAMPLE hash function | xxHash64, pinned forever | Fast, good distribution, industry convention, stable Rust impl |
| SAMPLE entity-id serialization | UTF-8 for String, LE 8-byte for Int | Documented stability contract |
| SAMPLE population | Source-table entity set, invariant under upstream stateless filters | Pushdown compatibility; user intuition |
| SESSIONIZE -> EventSelect | Allowed, entity-level (not session-level) | Consistent with ATTRIBUTE composition rules |
| Chained EventSelects | Allowed, no special case | Semantically well-defined; empty output is self-explanatory |

---

## 25. Follow-On Implications

### 25.1 Doc Updates Required

- **query-language.md Section 14.1**: Document event-type list form (Section 6), `lookback:` parameter with rationale + worked example (Section 7), "fewer than n" rule (Section 5.5), tie-breaking (Section 5.4). Reconcile TASK-421's prose about NTH's WHERE position.
- **query-language.md Section 14.2**: Remove `count:` (Section 15.2); document boundary values (Section 15.1); document hash function + stability guarantee (Section 16); document population-invariance with the `sampled INTERSECT filtered` output rule (Section 17).
- **query-language.md Section 25.2**: Add `SESSIONIZE -> FIRST/LAST/NTH` with the "no session-scoped selection" caveat (Section 19.1).
- **query-language.md Section 26 grammar**: Update `first_last_op` and `nth_op` to accept event-type lists and trailing `lookback:` param; update `sample_op` to remove `count:`.
- **type-system.md Section 6.7**: Restate tie-breaking (Section 5.4); note that FIRST/LAST/NTH output may fall outside the outer time range when `lookback:` is present (Section 7).
- **type-system.md Section 6.11**: Remove `count:` mention and example; keep `fraction:`-only surface.

### 25.2 Downstream Task Implications

- **TASK-421 (parser: FIRST/LAST/NTH + SAMPLE)**:
  - `first_last_op` and `nth_op` accept parenthesized event-type list (Section 6).
  - `nth_op` grammar keeps the `event_ref (WHERE predicate)? "," integer` form; reconcile TASK-421's prose description about NTH WHERE position.
  - `first_last_op` and `nth_op` accept optional `, lookback: <duration>` (Section 7). LAST rejects `lookback:` at parse time.
  - `sample_op` grammar: `SAMPLE "(" "fraction" ":" number ("," "seed" ":" integer)? ")"` — drop the `count:` alternative (Section 15.2).
  - Duplicate-name diagnostic required for event-type lists (Section 6.4).

- **TASK-424 (planner: EventSelect + Sample physical nodes)**:
  - `EventSelect { kind: FIRST|LAST|NTH(n), event_types: Vec<EventType>, predicate: Option<TypedExpr>, lookback: Option<Duration>, forwarded_columns: Vec<ColumnId>, fused_downstream: Option<FusedDownstream>, input, output_schema }`.
  - `Sample { fraction: f64, seed: Option<i64>, input, output_schema }` — no `SampleSpec` enum; `count:` gone.

- **TASK-425 (lowering)**:
  - Thread `lookback` into upstream `Scan` time-range widening for FIRST/NTH (Section 11). Integrates with TASK-407 B2 uniform widening across joined tables.
  - Drop any `count:` desugaring / handling path.

- **TASK-429 (EventSelectOperator)**:
  - Per-entity candidate loop tests against `event_types_set` (Section 5.1).
  - "Fewer than n" -> omit entity (Section 5.5).
  - Same-`ts` tie-breaking by `__seq_id` (Section 5.4).
  - Demand-driven forwarded columns (Section 10).
  - FIRST/NTH early termination (Sections 8.3, 8.4).
  - Dictionary-code event-type matching (Section 9.2).
  - No operator-side change for `lookback:` (Section 7.6).

- **TASK-430 (SAMPLE pushdown)**:
  - Fraction-only pushdown — no count-to-fraction translation path (Section 15.2).
  - xxHash64 over canonical entity-id byte serialization (Section 16).
  - Population invariance (Section 17.1) enables pushdown through WHERE / SELECT / LET chains.

- **Wave 5 fusion task (TASK-503 ecosystem)**:
  - Add Section 12.1 EventSelect -> STATS fusion candidates to the fusion-opportunities list.
  - Per-session FIRST/LAST/NTH (v2 operator variant) is a separate design task, not a fusion opportunity (Section 19.1).

---

## 26. Open Questions Deferred to Other Tasks

- **Logical lowering rules**: How the planner lowers `PipelineStage::FirstLastNth` and `PipelineStage::Sample` into logical nodes and then to physical descriptors. Owned by TASK-425.
- **`DemandCapabilities` real protocol**: The full demand protocol is owned by TASK-409 / TASK-427.
- **SAMPLE scan pushdown implementation**: Owned by TASK-430. This document specifies the contract; TASK-430 owns the implementation in storage/scan.
- **Per-session FIRST/LAST/NTH**: A v2 feature requiring its own design. Session-scoped selection is not supported in v1 (Section 19.1).
- **Reverse-scan for LAST optimization**: A potential future storage feature that would allow LAST to terminate early by scanning entities in reverse `(ts, __seq_id)` order. Not in v1 storage contract.
