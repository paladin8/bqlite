# ATTRIBUTE Operator Architecture

> **Status**: DRAFT
> **Task**: TASK-406
> **Wave**: 4
> **Depends on**: execution-model.md, operator-traits.md, type-system.md, planner-pipeline.md, query-language.md
> **Depended on by**: TASK-422 (parser ATTRIBUTE stage), TASK-424 (planner logical + physical nodes), TASK-425 (lowering), TASK-431 (AttributeOperator implementation)

---

## 1. Purpose

This document connects the surface-syntax specification in query-language.md Section 14.3, the output schema in type-system.md Section 6.14, and the planner integration in planner-pipeline.md Section 13 to a concrete `EntityOperator` implementation. It pins down:

- The sliding-window deque per-entity state layout and lifecycle.
- Window boundary rules — inclusive at lookback edge, strict at conversion timestamp.
- Same-`ts` ordering — the rule is stated in `ts`-space; `__seq_id` is a traversal tiebreaker, not a window-boundary participant.
- Per-event processing order — emit-before-add when an event matches both `conversion:` and `touchpoints:` lists.
- Output schema — `entity_id`, `conversion_ts`, demand-forwarded conversion properties, `touchpoint_ts?`, `touchpoint_key: String?`.
- Three-way row-shape distinction: normal attribution, touchpoint-present-key-null, and LEFT-UNNEST (no qualifying touchpoint).
- `touchpoint_key` expression surface — any scalar expression resolving against the source schema, typed to `String`.
- Per-entity deque cap — 1M touchpoints, flush + skip entity + diagnostic.
- Scan-range extension — planner widens backward by `window`.
- Demand-driven conversion property forwarding.
- Fused aggregate shapes — none in v1 (Wave 5 target).
- Composition rules — `SESSIONIZE | ATTRIBUTE` allowed; `MATCH | ATTRIBUTE` rejected.
- Edge-case matrix and benchmark targets the implementation must satisfy.

The document does **not** cover parser grammar changes (TASK-422), AST-to-logical lowering (TASK-425), or planner plan-node definitions (TASK-424) — those are owned by their respective tasks.

---

## 2. Operator Identity

The ATTRIBUTE operator is the `AttributeOperator`, a stateful per-entity operator that implements `EntityOperator` (operator-traits.md Section 6). It finds touchpoint events preceding each conversion event within a time window and auto-unnests them into flat rows — one row per `(entity, conversion, matched-touchpoint)` triple.

**Crate**: `bqlite-operators` (in `src/attribute.rs`).

**Execution category**: Stateful temporal operator (execution-model.md Section 2.1). Pull-based via `EntityOperatorAdapter`.

**Physical plan node**: `AttributePhysical` (planner-pipeline.md Section 9.5).

---

## 3. Parameters

All parameters are required. The operator accepts:

| Parameter | Type | Description |
|---|---|---|
| `conversion` | `Vec<EventRef>` (length >= 1) | Event type(s) that trigger conversion emission. Single event ref or parenthesized list. |
| `touchpoints` | `Vec<EventRef>` (length >= 1) | Event type(s) eligible as touchpoints. Single event ref or parenthesized list. |
| `window` | Duration (nanoseconds internally) | Lookback window before each conversion in which touchpoints qualify. |
| `touchpoint_key` | Scalar expression -> `String` | Expression evaluated per qualifying touchpoint; result becomes the `touchpoint_key` output column. |

**List extension (Section 8 of task note).** Both `conversion:` and `touchpoints:` accept either a single `event_ref` or a parenthesized comma-separated list:

```
event_ref | "(" event_ref ("," event_ref)* ")"
```

The two lists may overlap (see Section 6 below for the emit-before-add rule). Duplicates within each list are rejected at parse time (TASK-422).

---

## 4. Output Schema

One row per `(entity_id, conversion, matched-touchpoint)`. ATTRIBUTE auto-unnests — it emits flat rows, not a list column.

| Column | Type | Nullable | Present | Description |
|---|---|---|---|---|
| `entity_id` | String or Int (matches entity key) | no | Always | Entity identifier |
| `conversion_ts` | Timestamp | no | Always | Conversion event's timestamp |
| *conversion properties* | (resolved from source schema) | follows source | When downstream references `<conversion_event_type>.<column>` for any event type named in `conversion:` | Demand-driven forwarded conversion properties |
| `touchpoint_ts` | Timestamp | **yes** | Always | Touchpoint timestamp; `NULL` only for LEFT-UNNEST rows |
| `touchpoint_key` | String | **yes** | Always | Pre-computed `touchpoint_key` expression result; `NULL` for LEFT-UNNEST rows or when the expression evaluates to `NULL` on a qualifying touchpoint |

### 4.1 Three-Way Row-Shape Distinction

The operator emits three distinguishable row shapes:

| Row shape | `touchpoint_ts` | `touchpoint_key` | Meaning |
|---|---|---|---|
| Normal attributed row | non-null | non-null | Touchpoint matched and key expression produced a value |
| Qualifying touchpoint, key null | **non-null** | null | Touchpoint matched but `touchpoint_key` expression evaluated to NULL |
| No qualifying touchpoint (LEFT-UNNEST) | null | null | Un-attributed conversion — zero touchpoints in window |

**Design rationale.** Collapsing "touchpoint present, key missing" into "no touchpoint" silently loses signal — users cannot count attributed-but-un-keyed touchpoints. The three-way distinction lets users choose their semantics downstream:

- **All conversions (attributed + unattributed):** no filter — default output.
- **Only attributed conversions (INNER-join):** `WHERE touchpoint_ts IS NOT NULL`.
- **Only attributed with a real key:** `WHERE touchpoint_key IS NOT NULL` (implies non-null `touchpoint_ts`).

### 4.2 Row Cardinality

For an entity with K conversions and, on average, N qualifying touchpoints per conversion (within `window`), the operator emits `K * max(N, 1)` rows. The `max(N, 1)` accounts for the LEFT-UNNEST row emitted for un-attributed conversions.

---

## 5. Window Boundary Rules

### 5.1 Qualification Rule

A touchpoint at `touchpoint_ts` qualifies for a conversion at `conversion_ts` iff:

```
conversion_ts - window <= touchpoint_ts < conversion_ts
```

- The `conversion_ts - window` boundary is **inclusive** — a touchpoint exactly on the lookback edge counts.
- `conversion_ts` itself is **strict** — a touchpoint at the same instant as the conversion does not count.

**Design rationale.** "Last 30d" intuition reads as "including the boundary nanosecond." A click at the exact instant of a purchase is either clock-skew noise or co-ingestion; not crediting it avoids racy attribution that depends on sub-nanosecond event ordering.

### 5.2 Same-`ts` Ordering

If a touchpoint and a conversion share the same `ts`, the strict-at-conversion rule (Section 5.1) excludes the touchpoint regardless of `__seq_id` arrival order. The operator processes events in `(ts, __seq_id)` order (the storage invariant), but the window rule is stated purely in `ts`-space — `__seq_id` is a tiebreaker for deterministic traversal, not a window-boundary participant.

**Design rationale.** A single consistent rule. Reaching into `__seq_id` would create a second semantic dimension users would have to reason about when composing ATTRIBUTE downstream of upstream operators that might or might not preserve the `__seq_id` field.

---

## 6. Per-Event Processing Order

The operator processes events in `(ts, __seq_id)` order within each entity. For each arriving event:

1. **Deque pruning.** Drop entries from the front of the deque where `entry.ts < event.ts - window`. Since events arrive in ascending `ts` order, any future conversion has `conversion_ts >= event.ts`, so its lookback edge is `conversion_ts - window >= event.ts - window`. Entries strictly below `event.ts - window` can never qualify again and are safe to discard. The inclusive boundary (`>=`) in Section 5.1 means entries at exactly `event.ts - window` are kept.

2. **Conversion emission.** If the arriving event's type is in the `conversion:` list, run the conversion emission step against the current deque:
   - Walk the deque and collect entries satisfying `conversion_ts - window <= entry.ts < conversion_ts`.
   - If qualifying entries exist, emit one row per entry in ascending `touchpoint_ts` order (Section 7).
   - If no qualifying entries exist, emit one LEFT-UNNEST row with `touchpoint_ts = NULL` and `touchpoint_key = NULL`.
   - Attach forwarded conversion properties (demand-driven, Section 10) to every emitted row.

3. **Touchpoint add.** If the arriving event's type is in the `touchpoints:` list, evaluate the `touchpoint_key` expression and add a `TouchpointDequeEntry { ts, key }` to the back of the deque.

Steps 2 and 3 run in that order — **emission before self-add** — so a conversion does not attribute to itself. With Section 5.1's strict-at-`conversion_ts` rule, this is already correct for the equal-`ts` case; the ordering rule is the generalization for the single-event-type case where `conversion == touchpoints`.

**Self-type attribution.** The grammar and semantics permit `conversion: E, touchpoints: E` (same event type). Every E-event is both a potential conversion trigger and a potential touchpoint. The emit-before-add rule ensures a conversion does not attribute to itself. "Logins that follow logins within 7d" is a legitimate query.

**Overlap with multi-list.** If an event matches both the `conversion:` and `touchpoints:` lists (either via overlap or an explicit shared event type), emission runs first (step 2), then deque add (step 3). The generalization is consistent across all list configurations.

---

## 7. Emission Order

When a conversion emits N qualifying rows, they are emitted in **ascending `touchpoint_ts` order** (oldest first — FIFO from the deque).

**Design rationale.** Deterministic, cheap (deque-natural), and documented so downstream code doesn't silently depend on reverse order. Window functions that need last-touch order (`ROW_NUMBER() OVER (... ORDER BY touchpoint_ts DESC)`) re-sort explicitly; the operator's own order is the baseline users can reason about.

---

## 8. Per-Entity State Layout

### 8.1 Deque Element

```rust
struct TouchpointDequeEntry {
    ts: i64,     // always — needed for window check and output
    key: String, // always — pre-computed touchpoint_key expression result (may be logically NULL)
}
```

The `key` field stores the pre-computed `touchpoint_key` expression result. A NULL expression result is represented as an empty sentinel or an `Option<String>` — the implementation chooses whichever avoids per-entry heap overhead. The choice is internal; the output column is always nullable `String`.

The deque entry does not carry raw touchpoint row data. That's the point of the auto-unnest design: by collapsing the "per-touchpoint structured payload" into a single pre-computed String, the state per touchpoint is minimal and has no type-system dependency on `List(Map)` / `List(Struct)`.

### 8.2 Per-Entity Operator State

```rust
struct AttributeEntityState {
    deque: VecDeque<TouchpointDequeEntry>,
}
```

The deque is cleared (or dropped) at entity boundaries. The operator maintains no cross-entity state.

### 8.3 Conversion-Side Retained State

When a conversion event arrives, the operator extracts the demanded conversion properties (Section 10) from the current input batch row. These values are held temporarily — only for the duration of the emission loop for that conversion — and are not stored in the deque. The implementation pre-sizes a scratch buffer for the forwarded column values:

```rust
struct ConversionScratch {
    conversion_ts: i64,
    forwarded_values: Vec<ScalarValue>, // one per demanded conversion property
}
```

This scratch is reused across conversions within the same entity (clear + refill, no reallocation).

---

## 9. Per-Entity Deque Cap

Per-entity deque state is capped at **1,000,000 touchpoints** (same convention as SESSIONIZE Section 8; configurable via a future engine setting, hard default in v1). On exceeding the cap:

1. The in-flight deque is **flushed** — the conversion currently being processed (if any) emits its qualifying rows from the deque seen so far.
2. **Remaining events for the same entity are discarded** — ATTRIBUTE skips forward until the entity boundary, then resumes normally with the next entity. Conversions that would have arrived after the skip-point are not emitted.
3. The engine records a **per-query diagnostic** of the same shape used by the existing "entity event limit" and by SESSIONIZE Section 8 (affected entity id, event count, operator). The query succeeds; it does not error.

**Design rationale.** Matches SESSIONIZE's failure mode and the existing per-entity-event-limit convention — pathological entities don't take down the whole query, but users learn about the truncation. Spill-to-disk is Wave 5 (TASK-502).

**Diagnostic channel.** ATTRIBUTE shares the per-query diagnostic channel with SESSIONIZE (per TASK-405 Section 8). If that channel does not yet exist at the operator boundary when TASK-431 ships, coordinate with TASK-428's plumbing rather than duplicating. The diagnostic shape:

```rust
struct EntityCapDiagnostic {
    entity_id: String,    // or the entity's canonical representation
    event_count: u64,     // number of events seen before cap
    operator: &'static str, // "ATTRIBUTE"
    cap: u64,             // the active cap value (1_000_000)
}
```

---

## 10. Demand-Driven Conversion Property Forwarding

Output schema advertises `entity_id`, `conversion_ts`, all conversion-property names referenced downstream, `touchpoint_ts`, `touchpoint_key`. The **physical per-conversion materialization** only computes and forwards conversion properties that downstream operators demand via `DemandSet` (the planner's backward demand propagation, planner-pipeline.md Section 9.2). Undemanded columns are not read from the source batch.

### 10.1 Demand Analysis

The planner walks downstream and records which conversion properties are referenced (as `<conversion_event_type>.<column>` expressions for event types named in `conversion:`). These become `forwarded_conversion_columns` on the `AttributePhysical` node.

**Example:**

```bql
events
| ATTRIBUTE(
    conversion: purchase,
    touchpoints: ad_click,
    window: 30d,
    touchpoint_key: channel
)
| STATS revenue = SUM(purchase.amount) GROUP BY touchpoint_key
```

Demand analysis yields:
- **Conversion forwarding**: `amount` from `purchase` (referenced downstream as `purchase.amount`).
- **Touchpoint-key expression**: needs `channel` from `ad_click` (becomes part of `required_columns()`).

Only those columns are read from the scan. `amount` is retained on the operator's per-entity state at the moment each `purchase` event is consumed and attached to every touchpoint row emitted for that conversion. `channel` is read only when evaluating the `touchpoint_key` expression on each `ad_click` event; no retention is needed because the expression's String result is stored directly in the deque entry.

### 10.2 Multi-Conversion-Type Forwarding

When `conversion:` is a list (e.g., `conversion: (purchase, subscription)`), any listed event type may be used as a prefix downstream. All types in the list share the same forwarded-column namespace. A downstream reference to `purchase.amount` is resolved against the source schema — if a `subscription` event does not carry `amount`, that column is NULL on rows where the conversion was a `subscription`.

---

## 11. `touchpoint_key` Expression Surface

The `touchpoint_key` expression must type-check to `String`. The allowed expression surface is **any scalar expression** valid elsewhere in BQL:

- Column references (e.g., `channel`)
- Literals (e.g., `'organic'`)
- Arithmetic, `CAST`, `CONCAT`, `CASE`
- Built-in scalar functions (`DATE_TRUNC`, string functions, etc.)
- Nested expressions (e.g., `CONCAT(channel, ':', campaign)`)

**Explicitly rejected at plan time:**

| Rejected form | Reason |
|---|---|
| Aggregate functions (`COUNT`, `SUM`, etc.) | Nonsensical in a per-row expression |
| Window functions (`ROW_NUMBER`, `LAG`, etc.) | Nonsensical pre-aggregation |
| Subqueries | Not permitted in scalar-expression contexts anywhere in BQL |
| References to conversion-event properties | Expression is evaluated in the touchpoint's context only |

Non-String expression results require an explicit `CAST(... AS STRING)`. NULL results are handled per Section 4.1 (three-way row-shape distinction).

**Column validation.** bqlite's column resolution is per-source-schema, not per-event-type. The `touchpoint_key` expression resolves against the source table's schema; at runtime it is only evaluated for rows whose `event_type` is in the `touchpoints:` list. There is no "column must exist on each touchpoint event type" requirement — the operator treats touchpoint types as a runtime predicate, not a static type constraint. Users whose touchpoint types have disjoint column surfaces (e.g., `ad_click.campaign_id` but `email_open` has no `campaign_id`) get NULL on the missing rows, which Section 4.1 handles.

**Design rationale.** The motivating example (`CONCAT(channel, ':', campaign)`) already implies the full scalar surface. Bucketing by `DATE_TRUNC('day', ts)` or `CASE WHEN` over property values is natural and valuable. The operator-side cost is a single per-touchpoint scalar eval regardless of expression shape, so there is no implementation reason to restrict.

---

## 12. Scan-Range Extension

When a query constrains the outer time range (e.g., `events LAST 30d | ATTRIBUTE(window: 30d, ...)`), the planner **extends the scan range backward by `window`** so the operator sees touchpoints from the lookback zone that qualify for conversions near the start of the outer range.

- Scan range: `[outer_start - window, outer_end)`
- Conversion emission is restricted to conversions with `conversion_ts` in the original `[outer_start, outer_end)` — touchpoints from the extended range are deque material only, never a trigger.

The conversion-emission filter is internal to the operator. The planner threads the original `[outer_start, outer_end)` as `conversion_range` into the `AttributePhysical` node so the operator can distinguish "in-range conversion" from "touchpoint-only event in the extended zone."

**Design rationale.** `events LAST 30d | ATTRIBUTE(window: 30d)` should just work. The existing doc examples widening manually (`events LAST 60d | ATTRIBUTE(window: 30d)`) is a footgun; mechanical planner widening removes it. Same spirit as MATCH lookback widening for RETENTION brackets (TASK-426).

**Implication for TASK-425 (lowering).** `Attribute` lowering must thread `window` into the upstream `Scan`'s time-range so it widens before the logical `Filter(ts >= outer_start)` gate is applied. Conversion emission filtering happens inside the operator, not as an upstream WHERE.

---

## 13. Fused Aggregate Shapes

ATTRIBUTE in v1 emits flat per-touchpoint rows to downstream STATS. **No `ATTRIBUTE -> STATS` fusion in v1.**

The three shapes enumerated in planner-pipeline.md Section 7.4.4 remain on the Wave 5 fusion menu:

| Fusion pattern | Description | Benefit |
|---|---|---|
| `STATS COUNT(*) GROUP BY touchpoint_key` | Direct per-key counting | Eliminates flat-row materialization |
| `STATS SUM(<conv.prop>) GROUP BY touchpoint_key` | Aggregated conversion property per key | Same |
| `WHERE touchpoint_ts IS NOT NULL \| STATS COUNT(*) GROUP BY touchpoint_key` | LEFT-UNNEST rows filtered at emission time; fused counter | LEFT-UNNEST rows never leave the operator |

**Design rationale.** Same rationale as SESSIONIZE Section 10 — `FusedDownstream` is an explicit Wave 5 concern per planner-pipeline.md Section 5.3. ATTRIBUTE's unfused path is already efficient because the operator emits flat rows (no list to UNNEST, no intermediate structure to collapse). Getting boundary rules, scan widening, and state cap right matters more than shaving per-row materialization now.

---

## 14. Composition Rules

### 14.1 `SESSIONIZE | ATTRIBUTE` — Allowed

`SESSIONIZE` is a valid upstream of `ATTRIBUTE`. The operator itself does **not** treat session boundaries specially — the per-entity deque spans sessions, and attribution can cross session boundaries freely.

`session_id` and `session_duration` flow through ATTRIBUTE as forwarded columns (demand-driven per Section 10) when referenced downstream.

Users who want within-session attribution express it explicitly downstream. v1 composes the operators as independent stages; within-session attribution is a v2 feature gated on dedicated design work.

### 14.2 `MATCH | ATTRIBUTE` — Rejected

`MATCH | ATTRIBUTE` is rejected — MATCH emits per-match rows, not raw event rows; there is no meaningful input shape for ATTRIBUTE to consume. The planner rejects this composition at plan time with a `TypeError`.

### 14.3 Valid Downstreams of ATTRIBUTE

Per query-language.md Section 25.2:

| Downstream | Notes |
|---|---|
| `WHERE` | Filter attributed rows |
| `SELECT` / `LET` | Project / compute derived columns |
| `STATS` | Aggregate over attributed rows |
| `ORDER BY` | Sort the flat output |
| `LIMIT` | Truncate output |

---

## 15. `EntityOperator` Integration

### 15.1 Trait Methods

The `AttributeOperator` implements the `EntityOperator` trait (operator-traits.md Section 6):

| Method | Behavior |
|---|---|
| `open(schema)` | Validate source schema, compile `touchpoint_key` expression, resolve forwarded conversion column indices. Store the set of conversion event types and touchpoint event types as `HashSet<String>` for O(1) membership checks. |
| `process_sub_batch(batch)` | Process events in `(ts, __seq_id)` order per Section 6. For each event: prune deque, check conversion, check touchpoint. Accumulate output rows in a batch builder. |
| `finish_entity()` | Flush any remaining state. Since ATTRIBUTE only emits on conversion events (not on entity end), `finish_entity()` clears the deque and resets the entity state. No additional rows are emitted. |
| `output_schema()` | Return the schema described in Section 4. |

### 15.2 Sub-Batch Streaming

ATTRIBUTE supports sub-batch streaming (operator-traits.md Section 6.3). An entity's events may arrive across multiple sub-batches. The deque persists across sub-batch boundaries within the same entity. Entity boundary detection is handled by `EntityOperatorAdapter`.

### 15.3 Output Batch Construction

The operator uses Arrow builders pre-sized to the expected output cardinality (the number of rows emitted for the current sub-batch). Columns:

- `entity_id`: copied from the input batch's entity column (same value for all rows in the entity).
- `conversion_ts`: `TimestampNanosecondBuilder` — set to the conversion event's timestamp for every row emitted from that conversion.
- Forwarded conversion properties: one builder per demanded column, populated from the conversion event's row.
- `touchpoint_ts`: `TimestampNanosecondBuilder` — nullable; set from `TouchpointDequeEntry.ts`, or NULL for LEFT-UNNEST rows.
- `touchpoint_key`: `StringViewBuilder` — nullable; set from `TouchpointDequeEntry.key`, or NULL for LEFT-UNNEST rows or NULL key values.

---

## 16. Edge Cases and Invariants

### 16.1 Edge-Case Matrix

The implementation must handle and test:

| Case | Expected behavior |
|---|---|
| **Empty entity** (no events) | No rows emitted. |
| **Entity with only touchpoints, no conversions** | No rows emitted — touchpoints are deque-only; emission requires a conversion trigger. |
| **Entity with only conversions, no touchpoints** | One LEFT-UNNEST row per conversion (`touchpoint_ts = NULL`, `touchpoint_key = NULL`). |
| **Conversion with exactly one touchpoint at `conversion_ts - window`** (inclusive edge) | One row emitted — the touchpoint qualifies (inclusive boundary). |
| **Touchpoint at exactly `conversion_ts`** | Not qualified — strict-at-conversion rule (Section 5.1). LEFT-UNNEST row emitted if no other touchpoint qualifies. |
| **Same event type for conversion and touchpoints** (`conversion: E, touchpoints: E`) | Emit-before-add rule (Section 6). Each E-event triggers emission against the current deque, then adds itself. No self-attribution. |
| **Multiple conversions sharing the same touchpoints** | Touchpoints are not consumed. The same deque entries qualify for multiple conversions as long as they remain within each conversion's window. |
| **Touchpoint with NULL `touchpoint_key` expression** | Row emitted with non-null `touchpoint_ts` and null `touchpoint_key` (Section 4.1, row shape 2). |
| **Deque cap exceeded (> 1M touchpoints for one entity)** | Flush in-flight conversion, skip remaining entity events, emit diagnostic (Section 9). |
| **Single-event entity (one conversion, no touchpoints)** | One LEFT-UNNEST row. |
| **Single-event entity (one touchpoint, no conversion)** | No rows emitted. |
| **`window: 0s`** (zero-duration window) | No touchpoint can satisfy `conversion_ts - 0 <= touchpoint_ts < conversion_ts` (empty interval). Every conversion emits a LEFT-UNNEST row. This is semantically valid but useless — not rejected at plan time. |
| **Conversion at the boundary of the scan extension zone** | Conversion with `conversion_ts < outer_start` is not emitted (conversion-range filter, Section 12). Only conversions in `[outer_start, outer_end)` produce output. |
| **Touchpoints across sub-batch boundaries** | Deque persists across sub-batches within the same entity (Section 15.2). |
| **Multi-type conversion list with overlapping touchpoints** | Both conversion types trigger emission; both touchpoint types add to deque. A single event matching both lists follows emit-before-add (Section 6). |
| **Forwarded conversion property missing on one conversion event type** | NULL for that column on rows from that conversion type (Section 10.2). |

### 16.2 Invariants

The implementation must maintain:

1. **Entity ordering preserved.** Output rows are grouped by entity, with entities appearing in the same order as the input.
2. **Per-entity conversion ordering preserved.** Within an entity, rows are grouped by conversion, with conversions appearing in ascending `conversion_ts` order.
3. **Per-conversion touchpoint ordering.** Within a conversion's emitted rows, touchpoints appear in ascending `touchpoint_ts` order (Section 7).
4. **LEFT-UNNEST exactly once.** A conversion with zero qualifying touchpoints emits exactly one LEFT-UNNEST row, never zero and never more than one.
5. **No self-attribution.** An event matching both `conversion:` and `touchpoints:` does not attribute to itself, regardless of `ts`/`__seq_id` values.
6. **Window rule is `ts`-space only.** The `__seq_id` field never participates in the qualification predicate.
7. **Deque cap is per-entity.** Hitting the cap on entity A does not affect entity B.
8. **Conversion-range filter is internal.** Events from the scan extension zone are visible to the deque but never trigger conversion emission outside `[outer_start, outer_end)`.

---

## 17. Benchmark and Test Targets

### 17.1 Benchmark Coverage

TASK-431 must include benchmarks for:

| Benchmark | What it measures |
|---|---|
| **Single-entity, many touchpoints** | Deque throughput: 100K touchpoints, 100 conversions per entity, 10d window. Measures per-event deque-add and per-conversion deque-walk cost. |
| **Many entities, sparse attribution** | Entity-boundary overhead: 10K entities, 5 events each (1 conversion + 4 touchpoints). Measures state setup/teardown. |
| **High fan-out** | Emission throughput: 1 entity, 10K touchpoints per conversion, 10 conversions. Measures batch-builder performance under high cardinality. |
| **LEFT-UNNEST dominant** | Conversions with zero touchpoints: 1K entities, 100 conversions each, 0 touchpoints. Measures LEFT-UNNEST fast path. |
| **Multi-type attribution** | Mixed `conversion: (A, B)`, `touchpoints: (C, D, E)`. Measures event-type dispatch overhead. |

### 17.2 Property Test Targets

| Property | Invariant |
|---|---|
| For any entity event stream with mixed conversion/touchpoint events, every conversion emits at least one row (LEFT-UNNEST guarantee). | `count(output rows where conversion_ts = c.ts) >= 1` for every conversion `c`. |
| No emitted row has `touchpoint_ts >= conversion_ts`. | Window strict-at-conversion rule. |
| No emitted row has `touchpoint_ts < conversion_ts - window`. | Window inclusive-at-lookback rule. |
| For `conversion: E, touchpoints: E`, no row has `touchpoint_ts == conversion_ts` from the same event. | No self-attribution. |
| Total output rows = `sum over conversions of max(qualifying_touchpoints, 1)`. | Cardinality invariant. |

---

## 18. Downstream Task Implications

These consequences are called out for downstream implementation tasks:

- **TASK-422 (parser: ATTRIBUTE)** — Accepts `conversion:` and `touchpoints:` as single `event_ref` or parenthesized list (Section 3); emits `Vec<EventRef>` for each in the AST; rejects duplicates within each list. Lists may overlap across the two parameters.

- **TASK-424 (planner: Attribute logical + physical node)** — Carries `conversion_events: Vec<EventType>`, `touchpoint_events: Vec<EventType>`, `window`, `touchpoint_key: TypedExpr`, `forwarded_conversion_columns` (demand-driven per Section 10), output schema per type-system.md Section 6.14. Must include `conversion_range: Option<(i64, i64)>` for scan-extension-aware emission filtering (Section 12).

- **TASK-425 (lowering)** — Threads `window` into upstream `Scan` time-range for Section 12 backward widening; type-checks `touchpoint_key` against the source schema per Section 11; emits `Attribute` with widened scan range + internal conversion-emission filter.

- **TASK-431 (AttributeOperator)** — Owns the per-entity deque, Section 5 window rules, Section 5.2 `ts`-space ordering, Section 4.1 three-way row-shape emission, Section 9 deque cap with diagnostic + entity skip, Section 6 emit-before-add ordering, Section 7 chronological ascending emission, Section 10 demand-driven forwarded columns. Shares the per-query diagnostic channel with TASK-428 (SESSIONIZE).

- **`query-language.md` Section 14.3** — Must document Section 5.1 window boundary rule, Section 4.1 three-way row-shape distinction with worked example, Section 12 scan-range widening, Section 6 self-type-attribution rule, Section 7 emission order, Section 3 list extension.

- **`query-language.md` Section 25.2** — Add `ATTRIBUTE` to valid downstreams of `SESSIONIZE` (Section 14.1), with the "no automatic session restriction" caveat.

- **`type-system.md` Section 6.14** — Update "Conversion property access" paragraph for Section 3 list form; restate Section 4.1 three-way output row distinction.

- **`planner-pipeline.md` Sections 5.2 / 7.4.4 / 8.4** — No immediate changes required. Section 7.4.4's fusion table stands as the Wave 5 target per Section 13. Section 8.4 (ATTRIBUTE column forwarding) stands.

- **Wave 5 fusion task (TASK-503 ecosystem)** — Retain the planner-pipeline.md Section 7.4.4 three shapes as v5 ATTRIBUTE fusion targets.
