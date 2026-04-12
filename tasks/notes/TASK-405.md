# TASK-405 — SESSIONIZE operator semantics

Human-assisted semantics decisions for `docs/design/operators/sessionize.md`. These decisions are authoritative and override conflicting guesses drawn from `TASKS.md` or `query-language.md` §8 / §30.2. Reconcile those docs in the same checkpoint as any code change that contradicts them.

## Already pinned by existing docs (not re-litigated here)

- Parameters: `gap: <duration>` required, `end: <event_type>` optional (see §11 below for the list extension). `query-language.md` §8.
- Output schema = input columns + `session_id: Int` + `session_duration: Int` (nanos). §8, type-system.md §6.3.
- `session_id` monotonically increasing per entity; `WITHIN SESSION` is implemented by MATCH watching `session_id` increments. §30.2.
- SESSIONIZE is entity-streaming over entity-sorted input; downstream operators valid per §25.2 (WHERE, SELECT, LET, MATCH, STATS).
- `WITHIN SESSION` is mutually exclusive with `WITHIN <duration>` and `BRACKETS`. §8.1.

## Decisions

### 1. Gap boundary — exclusive (strict `>`)

A new session opens iff `delta_ns(event_i, event_{i-1}) > gap_ns`. At exactly `delta == gap`, both events belong to the **same** session.

**Why:** "Maximum inactivity" reads naturally as "up to and including the gap is still inactive-but-same-session." Matches the standard sessionization convention. The gap boundary itself is excluded from the set of deltas that trigger a break — hence "exclusive boundary."

### 2. `end:` event membership — end event belongs to the session it closes

When an event `E` matches an `end:` event type, `E` is the **last event of the current session**, not the first event of a new session. A MATCH of shape `search THEN logout` inside `WITHIN SESSION` therefore matches within the session containing the logout.

**Why:** "End event" reads as "the event that ends the session." Lets `WITHIN SESSION` patterns that include the terminator work naturally. Avoids the awkward case where every `end:` event is its own 1-event session.

### 3. Gap vs `end:` precedence on the same event — gap closes first

If event `E` arrives with `delta > gap` **and** `E.event_type` matches an `end:` type, the prior session closes due to gap **before** `E` is considered. `E` then starts a fresh session and is immediately an end-event, so it becomes a 1-event session that closes on itself.

**Why:** Inactivity is a property of the interval preceding `E` and is independent of `E`'s type. Once the gap has elapsed, the prior session is logically closed — `E` cannot retroactively extend it. This gives a single deterministic rule without special-casing end events.

### 4. `session_duration` — `max_ts - min_ts` within the session

`session_duration_ns = max_ts_in_session - min_ts_in_session`. Single-event sessions have `session_duration == 0`.

**Why:** Matches standard sessionization tools. Semantically correct (zero activity spanned). `AVG(session_duration)` behaves sensibly. Trailing idle time up to the gap boundary is explicitly **not** included.

### 5. Single-event sessions — emitted

Every input event belongs to exactly one session, including entities whose events are all separated by more than `gap` (each event becomes its own 1-event session). SESSIONIZE does not drop singletons.

**Why:** SESSIONIZE annotates, it does not filter. Users who want to drop singletons write `WHERE session_duration > 0` downstream.

### 6. `session_id` encoding — per-entity `Int64` counter starting at 1

`session_id` is `Int64`, not nullable. Starts at `1` for each entity's first session. Monotonically increasing by 1 within an entity. Resets to `1` at every entity boundary — **not globally unique**.

**Why:** Global uniqueness is a cost we don't need. `COUNT_DISTINCT(session_id) GROUP BY entity_id` is the idiomatic per-entity session count and works with per-entity counters. Starting at `1` matches user intuition ("session #1"). Matches §30.2's "monotonically increasing integer per entity."

**User-facing caveat (must be documented):** `COUNT_DISTINCT(session_id)` **without** `GROUP BY entity_id` is almost never what a user wants — it collapses across entities that share the same session numbers. Document this with an example in `query-language.md` §8.

### 7. Emission timing — session rows emitted when the session closes

SESSIONIZE buffers the rows of the currently-open session for the entity it is processing. When the session closes (gap boundary, end event, or end-of-entity), all buffered rows are emitted downstream with their final `session_id` and `session_duration` filled in.

Because input is entity-sorted, at most one entity's open session is in memory at a time. An entity's final session closes at the entity boundary (the first event of the next entity, or end-of-stream).

**Why:** `session_duration` cannot be known until the session closes. Buffering is per-entity-at-a-time; no cross-entity state. This is the minimum buffering compatible with the output schema — alternatives like late-patch rows would break downstream non-null guarantees for `session_duration`.

### 8. Per-entity session cap — 1M events, truncate entity with a diagnostic

Per-entity open-session state is capped at **1,000,000 events** (configurable via a future engine setting, hard default in v1). On exceeding the cap:

- The partially-buffered session is **flushed** with `session_duration` computed from events seen so far.
- **Remaining events for the same entity are discarded** — SESSIONIZE skips forward until the entity boundary, then resumes normally with the next entity.
- The engine records a per-query diagnostic of the same shape used for the existing "entity event limit" (affected entity id, event count, operator). The query succeeds; it does not error.

**Why:** Matches the existing per-entity-event-limit failure mode — a single pathological entity (malicious, buggy producer, or a legitimate mega-user) should not error-out the whole query, but users need to know it happened. Flushing the partial session preserves whatever signal was already there. Spill-to-disk is out of scope for v1 (Wave 5, TASK-502).

**Implication for TASK-428:** the operator needs access to the same per-query diagnostic channel used by the existing entity event limit. If that channel does not yet exist at the operator boundary, TASK-428 must either thread it through or file a follow-up for the shared plumbing.

### 9. Forwarded columns — logically all-pass, physically demand-driven

The **output schema** advertised by SESSIONIZE is `input schema ∪ {session_id, session_duration}` — all input columns flow through logically.

The **physical per-session buffer** only materializes columns that downstream operators demand via `DemandCapabilities`. Columns that are not downstream-demanded are dropped from the buffer, not from the schema.

**Why:** Consistent with the demand-driven approach used by other stateful operators (SequenceMatch, EventSelect, Attribute per TASK-409). Sessions can be long; unbuffered columns reduce per-session memory materially. The schema stays clean so downstream planning doesn't need session-specific rules.

### 10. Fused aggregate shapes — none in v1

SESSIONIZE in v1 emits full per-session rows to the downstream STATS operator. No `SESSIONIZE → STATS` fusion.

**Candidates for later (documented as Wave 5 fusion opportunities, not implemented):**
- `SESSIONIZE | STATS sessions = COUNT_DISTINCT(session_id), avg_sd = AVG(session_duration) GROUP BY entity_id` → fused per-entity (session_count, total_session_duration) counters.
- `SESSIONIZE | STATS AVG(session_duration)` (no group by) → fused two-counter running average.

**Why:** Fusion is a cross-cutting Wave 5 concern per planner-pipeline.md §5.3 and the `FusedDownstream` annotation is explicitly deferred. Getting SESSIONIZE's emission semantics, state cap, and boundary rules right matters more than shaving per-session row construction now.

### 11. `end:` accepts a list of event types

Grammar extension: `end: event_ref` becomes `end: event_ref | "(" event_ref ("," event_ref)* ")"`. Any event whose type is in the list closes the current session (same membership rule as §2 — the end event belongs to the session it closes).

```bql
| SESSIONIZE(gap: 30m, end: logout)
| SESSIONIZE(gap: 30m, end: (logout, timeout, session_end))
```

**Why:** Real systems produce multiple end-of-session event types (logout, forced timeout, crash-flush). Forcing users to synthesize a column or filter upstream is friction for a common case. Single-event form is still legal.

**Implication for TASK-420 (parser):** `session_params` grammar accepts both the single `event_ref` and the parenthesized list. The AST node carries a `Vec<EventRef>` (length ≥ 1; length 1 covers the single form). Duplicate-name diagnostic required within the list.

### 12. Entity boundary — flush and reset

When the entity-sorted input stream crosses from entity A to entity B:

- A's currently-open session is closed as "end-of-entity" (same treatment as gap closure for the final session).
- A's session buffer is flushed downstream.
- `session_id` counter resets to `1` for B.
- No session can span two entities.

This is a consequence of the per-entity semantics, listed explicitly so the implementation has an unambiguous rule at the boundary.

## Follow-on implications to propagate

These are not decisions, just consequences worth calling out for downstream tasks:

- **TASK-420 (parser: SESSIONIZE)** — accepts `end:` as single `event_ref` **or** parenthesized list of event refs; emits a `Vec<EventRef>` in the AST; rejects duplicates within the list (§11).
- **TASK-424 (planner: Sessionize logical + physical node)** — carries `gap`, `end_events: Vec<EventType>`, `forwarded_columns` (demand-driven per §9), output schema per §6 and type-system.md §6.3.
- **TASK-428 (SessionizeOperator)** — owns the per-entity buffer, the §1 gap rule, §2/§3 end-event rule, §4 duration computation, §8 per-entity event cap with diagnostic + entity skip, and the §12 entity-boundary flush. Must thread the per-query diagnostic channel used by the entity event limit.
- **`query-language.md` §8** — add the caveat from §6 about `COUNT_DISTINCT(session_id)` without `GROUP BY entity_id`, document the `end:` list form from §11, document the per-entity event cap behavior from §8, and pin the gap-exclusive rule (§1) and end-event membership (§2) explicitly.
- **`type-system.md` §6.3** — confirm `session_id: Int64 NOT NULL`, `session_duration: Int64 NOT NULL` (nanos).
- **Wave 5 fusion task (TASK-503 ecosystem)** — add the two fusion candidates from §10 to the fusion-opportunities list.
