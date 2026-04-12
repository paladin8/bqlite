# TASK-406 — ATTRIBUTE operator semantics

Human-assisted semantics decisions for `docs/design/operators/attribute.md`. These decisions are authoritative and override conflicting guesses drawn from `TASKS.md`, `query-language.md` §14.3, `type-system.md` §6.14, or `planner-pipeline.md` §5.2 / §7.4.4 / §8.4. Reconcile those docs in the same checkpoint as any code change that contradicts them.

## Already pinned by existing docs (not re-litigated here)

- Parameters: `conversion`, `touchpoints`, `window`, `touchpoint_key`, all required (see §8 below for the list extension). `query-language.md` §14.3.
- Output schema = `entity_id`, `conversion_ts`, demand-forwarded conversion properties, `touchpoint_ts?`, `touchpoint_key: String?`. `type-system.md` §6.14.
- LEFT-UNNEST: a conversion with zero qualifying touchpoints emits exactly one row with `touchpoint_ts = NULL` and `touchpoint_key = NULL`.
- Auto-unnest — flat rows, not a list column, no Struct type required.
- Conversion properties are accessed downstream as `<conversion_event_type>.<column>`; name collision with source columns is `TypeError::NameCollision` at plan time.
- Touchpoints are not consumed — a single touchpoint can attribute to multiple conversions.
- Execution model (sliding-window deque per entity): on each conversion arrival, drop deque entries older than `conversion_ts - window`, emit one row per remaining entry.

## Decisions

### 1. Window boundary — inclusive at lookback edge, strict at conversion

A touchpoint at `touchpoint_ts` qualifies for a conversion at `conversion_ts` iff:

```
conversion_ts - window <= touchpoint_ts < conversion_ts
```

The `conversion_ts - window` boundary is **inclusive** (a touchpoint exactly on the lookback edge counts). `conversion_ts` itself is **strict** (a touchpoint at the same instant as the conversion does not count).

**Why:** "last 30d" intuition reads as "including the boundary nanosecond." A click at the exact instant of a purchase is either clock-skew noise or co-ingestion; not crediting it avoids racy attribution that depends on sub-nanosecond event ordering.

### 2. Same-`ts` ordering — `ts`-space rule; `__seq_id` is not a second gate

If a touchpoint and a conversion share the same `ts`, §1's strict-at-conversion rule excludes the touchpoint regardless of `__seq_id` arrival order. The operator processes events in `(ts, __seq_id)` order (the storage invariant), but the window rule is stated purely in `ts`-space — `__seq_id` is a tiebreaker for deterministic traversal, not a window-boundary participant.

**Why:** a single consistent rule. Reaching into `__seq_id` would create a second semantic dimension users would have to reason about when composing ATTRIBUTE downstream of upstream operators that might or might not preserve the `__seq_id` field.

### 3. `touchpoint_key = NULL` — row emitted, `touchpoint_ts` stays non-null

If the `touchpoint_key` expression evaluates to NULL for a qualifying touchpoint, the operator **still emits a row**. `touchpoint_ts` carries the real touchpoint timestamp (non-null), and `touchpoint_key` carries NULL.

This is deliberately distinguishable from the LEFT-UNNEST row:

| Row shape                          | `touchpoint_ts` | `touchpoint_key` | Meaning                                |
| ---------------------------------- | --------------- | ---------------- | -------------------------------------- |
| Qualifying touchpoint, key non-null | non-null        | non-null         | Normal attributed row                  |
| Qualifying touchpoint, key null     | **non-null**    | null             | Touchpoint matched but key was missing |
| No qualifying touchpoint (LEFT-UNNEST) | null         | null             | Un-attributed conversion                |

**Why:** collapsing "touchpoint present, key missing" into "no touchpoint" silently loses signal (users can't count attributed-but-un-keyed touchpoints). Users who want INNER-join semantics write `WHERE touchpoint_ts IS NOT NULL`; users who want "attributed with a real key" write `WHERE touchpoint_key IS NOT NULL` (which implies non-null `touchpoint_ts`).

**Must be documented in `query-language.md` §14.3:** the three-way distinction above, with a worked example.

### 4. Per-entity deque cap — 1M touchpoints, flush + skip entity + diagnostic

Per-entity deque state is capped at **1,000,000 touchpoints** (same convention as SESSIONIZE §8; configurable via a future engine setting, hard default in v1). On exceeding the cap:

- The in-flight deque is **flushed** — the conversion currently being processed (if any) emits its qualifying rows from the deque seen so far.
- **Remaining events for the same entity are discarded** — ATTRIBUTE skips forward until the entity boundary, then resumes normally with the next entity. Conversions that would have arrived after the skip-point are not emitted.
- The engine records a per-query diagnostic of the same shape used by the existing "entity event limit" and by SESSIONIZE §8 (affected entity id, event count, operator). The query succeeds; it does not error.

**Why:** matches SESSIONIZE's failure mode and the existing per-entity-event-limit convention — pathological entities don't take down the whole query, but users learn about the truncation. Spill-to-disk is Wave 5 (TASK-502).

**Implication for TASK-431:** shares the per-query diagnostic channel with SESSIONIZE (per TASK-405 §8). If that channel does not yet exist at the operator boundary, coordinate with TASK-428's plumbing rather than duplicating.

### 5. Scan-range extension — planner widens backward by `window`

When a query constrains the outer time range (e.g. `events LAST 30d | ATTRIBUTE(window: 30d, ...)`), the planner **extends the scan range backward by `window`** so the operator sees touchpoints from the lookback zone that qualify for conversions near the start of the outer range.

- Scan range: `[outer_start - window, outer_end)`
- Conversion emission is restricted to conversions with `conversion_ts` in the original `[outer_start, outer_end)` — touchpoints from the extended range are deque material only, never a trigger.

**Why:** `events LAST 30d | ATTRIBUTE(window: 30d)` should just work. The existing doc examples widening manually (`events LAST 60d | ATTRIBUTE(window: 30d)`) is a footgun; mechanical planner widening removes it. Same spirit as MATCH lookback widening for RETENTION brackets (TASK-426).

**Implication for TASK-425 (lowering):** `Attribute` lowering must thread `window` into the upstream `Scan`'s time-range so it widens before the logical `Filter(ts >= outer_start)` gate is applied. Conversion emission filtering happens inside the operator, not as an upstream WHERE.

**Must be documented in `query-language.md` §14.3:** the scan-range widening rule, so users understand that `ATTRIBUTE` reads data from outside their nominal range.

### 6. `conversion == touchpoints` same event type — allowed; emit-before-add ordering

The grammar and semantics permit `conversion: E, touchpoints: E` (same event type). Every `E`-event is both a potential conversion trigger and a potential touchpoint. The operator's per-event rule is:

1. If the arriving event's type is in `conversion:`, run the conversion emission step against the current deque.
2. If the arriving event's type is in `touchpoints:`, add it to the deque.

Steps 1 and 2 run in that order — **emission before self-add** — so a conversion does not attribute to itself. With Q1's strict-at-`conversion_ts` rule, this is already correct for the equal-`ts` case; the ordering rule is the generalization for the single-event-type case.

**Why:** "logins that follow logins within 7d" is a legitimate query. Rejecting self-type attribution would push users to ugly workarounds. The emit-before-add rule is deterministic and documented.

### 7. Emitted rows per conversion — chronological ascending

When a conversion emits N qualifying rows, they are emitted in **ascending `touchpoint_ts` order** (oldest first — FIFO from the deque).

**Why:** deterministic, cheap (deque-natural), and documented so downstream code doesn't silently depend on reverse order. Window functions that need last-touch order (`ROW_NUMBER() OVER (... ORDER BY touchpoint_ts DESC)`) re-sort explicitly; the operator's own order is the baseline users can reason about.

### 8. `conversion:` and `touchpoints:` both accept a list

Grammar extension — both parameters become:

```
event_ref | "(" event_ref ("," event_ref)* ")"
```

Semantics: any event whose type is in the `conversion:` list can trigger an emission; any event whose type is in the `touchpoints:` list can be added to the deque. The §6 emit-before-add rule generalizes — if an event matches both lists (either via overlap or an explicit shared event type), emission runs first, then deque add.

```bql
| ATTRIBUTE(
    conversion: purchase,
    touchpoints: (ad_click, email_open, social_share),
    window: 30d,
    touchpoint_key: channel
  )

| ATTRIBUTE(
    conversion: (purchase, subscription),
    touchpoints: (ad_click, email_open),
    window: 30d,
    touchpoint_key: channel
  )
```

**Column validation.** bqlite's column resolution is per-source-schema, not per-event-type. The `touchpoint_key` expression resolves against the source table's schema; at runtime it is only evaluated for rows whose `event_type` is in the `touchpoints:` list. There is no "column must exist on each touchpoint event type" requirement — the operator treats touchpoint types as a runtime predicate, not a static type constraint. Users whose touchpoint types have disjoint column surfaces (e.g., `ad_click.campaign_id` but `email_open` has no `campaign_id`) get NULL on the missing rows, which §3 handles.

Forwarded conversion properties work the same way — `purchase.amount` resolves against the source schema, and is NULL on conversion rows whose `event_type` does not carry that column.

**Why:** multi-touchpoint / multi-conversion attribution is a common case (ad_click + email_open + social_share; purchase + subscription as unified conversions). The per-event-type column-typing complication that held TASK-405 §11 to just the `end:` list doesn't apply here because bqlite already resolves columns globally over the source schema.

**Implication for TASK-422 (parser):** `attribute_op` grammar accepts both single `event_ref` and parenthesized lists for both `conversion:` and `touchpoints:`. The AST node carries `Vec<EventRef>` for each (length ≥ 1; length 1 covers the single form). Duplicate-name diagnostic required within each list. The two lists may overlap (see §6).

**Implication for `type-system.md` §6.14:** update the "Conversion property access" paragraph to reflect that `<conversion_event_type>.<column>` is legal for any event type in the `conversion:` list. Within a single query, all types in the list share the same forwarded-column namespace.

### 9. Forwarded conversion columns — logically typed; physically demand-driven

Output schema advertises `entity_id`, `conversion_ts`, all conversion-property names referenced downstream, `touchpoint_ts`, `touchpoint_key`. The **physical per-conversion materialization** only computes and forwards conversion properties that downstream operators demand via `DemandCapabilities`. Undemanded columns are not read from the source batch.

**Why:** consistent with SESSIONIZE §9 and the wider demand-driven approach in TASK-409 / TASK-427. Conversion property lookup touches the source row; demand-pruning keeps that cost proportional to actual usage.

### 10. Fused aggregate shapes — none in v1

ATTRIBUTE in v1 emits flat per-touchpoint rows to downstream STATS. No `ATTRIBUTE → STATS` fusion. The three shapes already enumerated in `planner-pipeline.md` §7.4.4 — `COUNT(*) GROUP BY touchpoint_key`, `SUM(<conv.prop>) GROUP BY touchpoint_key`, and the LEFT-UNNEST-filtered count — remain on the Wave 5 fusion menu.

**Why:** same rationale as TASK-405 §10 — `FusedDownstream` is an explicit Wave 5 concern per `planner-pipeline.md` §5.3. ATTRIBUTE's unfused path is already efficient because the operator emits flat rows (no list to UNNEST, no intermediate structure to collapse). Getting boundary rules, scan widening, and state cap right matters more than shaving per-row materialization now.

**Wave 5 fusion task (TASK-503 ecosystem)** — retain the `planner-pipeline.md` §7.4.4 table as the v5 target.

### 11. `touchpoint_key` expression — any scalar expression that resolves against the source schema

The `touchpoint_key` expression must type-check to `String`. The allowed expression surface is **any scalar expression** valid elsewhere in BQL: column references, literals, arithmetic, `CAST`, `CONCAT`, `CASE`, built-in scalar functions (`DATE_TRUNC`, string functions, etc.), nested expressions.

**Explicitly rejected at plan time:**
- Aggregate functions (`COUNT`, `SUM`, etc.) — nonsensical in a per-row expression.
- Window functions (`ROW_NUMBER`, `LAG`, etc.) — nonsensical pre-aggregation.
- Subqueries — not permitted in scalar-expression contexts anywhere in BQL.
- References to conversion-event properties — the expression is evaluated in the touchpoint's context only. Already pinned by `query-language.md` §14.3; restated here.

Non-String expression results require an explicit `CAST(... AS STRING)`. NULL results are handled per §3.

**Why:** the motivating example (`CONCAT(channel, ':', campaign)`) already implies the full scalar surface. Bucketing by `DATE_TRUNC('day', ts)` or `CASE WHEN` over property values is natural and valuable. The operator-side cost is a single per-touchpoint scalar eval regardless of expression shape, so there is no implementation reason to restrict.

### 12. `SESSIONIZE | ATTRIBUTE` composition — allowed; no session-awareness

`query-language.md` §25.2 is updated to allow `SESSIONIZE` as a valid upstream of `ATTRIBUTE`. The operator itself does **not** treat session boundaries specially — the per-entity deque spans sessions, and attribution can cross session boundaries freely.

`session_id` and `session_duration` flow through ATTRIBUTE as forwarded columns (demand-driven per §9) when referenced downstream. Users who want within-session attribution express it explicitly downstream:

```bql
events LAST 60d
| SESSIONIZE(gap: 30m)
| ATTRIBUTE(conversion: purchase, touchpoints: ad_click, window: 30m, touchpoint_key: channel)
-- Further filter: only count attribution within the same session as the conversion.
-- (Requires LET-binding the conversion's session_id upstream or using a join pattern;
--  not a feature the operator provides by default.)
```

**Why:** allowing the composition keeps the §25.2 grammar unsurprising without adding operator-side complexity. Treating session boundaries as deque resets would be a substantive semantic feature (what does "within-session attribution" mean when a conversion and a touchpoint share a session vs. don't?) that deserves its own design pass. v1 composes the operators as independent stages; within-session attribution is a v2 feature gated on that design work.

**MATCH | ATTRIBUTE** remains rejected — MATCH emits per-match rows, not raw event rows; there is no meaningful input shape for ATTRIBUTE to consume.

**Must be documented in `query-language.md` §25.2:** add `ATTRIBUTE` to the downstream list of `SESSIONIZE`. Add a user-facing note that ATTRIBUTE does not automatically restrict to within-session touchpoints; v2 may add explicit session-aware modes.

## Follow-on implications to propagate

These are consequences worth calling out for downstream tasks:

- **TASK-422 (parser: ATTRIBUTE)** — accepts `conversion:` and `touchpoints:` as single `event_ref` or parenthesized list (§8); emits `Vec<EventRef>` for each in the AST; rejects duplicates within each list. Lists may overlap across the two parameters.
- **TASK-424 (planner: Attribute logical + physical node)** — carries `conversion_events: Vec<EventType>`, `touchpoint_events: Vec<EventType>`, `window`, `touchpoint_key: TypedExpr`, `forwarded_conversion_columns` (demand-driven per §9), output schema per `type-system.md` §6.14.
- **TASK-425 (lowering)** — threads `window` into upstream `Scan` time-range for §5 backward widening; type-checks `touchpoint_key` against the source schema per §11; emits `Attribute` with widened scan range + internal conversion-emission filter.
- **TASK-431 (AttributeOperator)** — owns the per-entity deque, the §1 window rule, §2 `ts`-space ordering rule, §3 three-way row-shape emission, §4 deque cap with diagnostic + entity skip, §6 emit-before-add ordering, §7 chronological ascending emission, §9 demand-driven forwarded columns. Shares the per-query diagnostic channel with TASK-428 (SESSIONIZE).
- **`query-language.md` §14.3** — update for §1 window boundary rule, §3 three-way row-shape distinction, §5 scan-range widening, §6 self-type-attribution rule, §7 emission order, §8 list extension for both `conversion:` and `touchpoints:`.
- **`query-language.md` §25.2** — add `ATTRIBUTE` to the valid downstreams of `SESSIONIZE` (§12), with the "no automatic session restriction" caveat.
- **`type-system.md` §6.14** — update "Conversion property access" paragraph for §8 list form; restate §3 three-way output row distinction.
- **`planner-pipeline.md` §5.2 / §7.4.4 / §8.4** — §7.4.4 fusion table stands as the Wave 5 target per §10; no immediate change. §8.4 stands.
- **Wave 5 fusion task (TASK-503 ecosystem)** — retain the §7.4.4 three shapes as v5 ATTRIBUTE fusion targets.
