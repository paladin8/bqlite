# TASK-411 — EventSelect (FIRST/LAST/NTH) and SAMPLE operator semantics

Human-assisted semantics decisions for `docs/design/operators/event-select-sample.md`. These decisions are authoritative and override conflicting guesses drawn from `TASKS.md`, `query-language.md` §14.1 / §14.2 / §25.2 / §26 / §30.9, or `type-system.md` §6.7 / §6.11. Reconcile those docs in the same checkpoint as any code change that contradicts them.

The design doc covers two independent operators that share a Wave 4 task slot:

- **Block A** — FIRST / LAST / NTH (entity-streaming per-entity event selection).
- **Block B** — SAMPLE (entity-level sampling, scan-pushdown-able).
- **Block C** — composition rules for both.

## Already pinned by existing docs (not re-litigated here)

- FIRST / LAST / NTH: per-entity; output is one row per entity with full event columns; entities with no matching event are **omitted**. `query-language.md` §14.1, `type-system.md` §6.7.
- WHERE predicate is applied **per-event before position selection** — `NTH(e WHERE p, 3)` returns the third `p`-satisfier, not the third `e` (that happens to satisfy `p`). §14.1.
- SAMPLE is entity-level, not event-level — a sampled entity's full event stream is included; non-sampled entities contribute zero events. §14.2.
- SAMPLE is deterministic via entity-id hash; default seed derived from the database UUID. §14.2, §30.9.
- SAMPLE's standard pushdown goal: filter entities at the scan layer so unsampled entities never reach the merge/read hot loop. TASK-430 is the implementation.

## Decisions — Block A: FIRST / LAST / NTH

### A1. `n` range for NTH — positive integer literal only

`NTH(event, n)` requires `n >= 1` (integer literal). `n == 0` and negative values are **parse-time** errors with clear diagnostics (e.g., `TypeError::InvalidNth { n: 0, reason: "n must be >= 1" }`).

**Why:** reverse indexing via negative `n` has a viable syntactic niche (`-1 == LAST`, `-2 == "second-to-last"`) but introduces a second indexing convention inside a single operator. `LAST` already covers `-1` trivially, and "second-to-last" is rare enough that users can `ORDER BY ts DESC | LIMIT n` downstream when they need it. Keeping `n >= 1` is a smaller and less surprising surface.

### A2. NTH syntax — WHERE attached to event_ref, before the comma and `n`

Canonical syntax:

```bql
NTH(event_ref (WHERE predicate)?, n)
```

So `NTH(purchase WHERE amount > 100, 3)` — **not** `NTH(purchase, 3 WHERE amount > 100)`. Matches `query-language.md` §14.1's example and §26's formal grammar production for `nth_op`.

**Why:** the grammar production and §14.1's example both already use this form; `event WHERE pred` as a conceptual unit also matches MATCH step syntax (`s: event WHERE pred`), keeping BQL's "predicate binds to the event" convention consistent.

**Implication for TASK-421 parser:** the TASKS.md description for TASK-421 incorrectly writes `NTH(event, n [WHERE ...])` (WHERE trailing). Reconcile TASK-421's description to the formal grammar in the same checkpoint that lands TASK-421's parser changes.

### A3. "Fewer than `n` qualifying events" — entity omitted

If an entity has fewer than `n` events satisfying the (optional) WHERE predicate, that entity produces **no output row**. Consistent with §14.1's "entities with no matching event are omitted" extended naturally.

**Why:** `NTH` promises "the n-th qualifying event"; if there isn't one, the entity has no answer. Omission is the cleanest representation. Users who want to distinguish "no events at all" from "fewer than n" can compose `NTH(e, 2)` and `NTH(e, 3)` as separate pipelines and compare counts.

### A4. Event type list extension for FIRST / LAST / NTH

Grammar extension — the `event_ref` position in all three operators accepts a parenthesized list of event types:

```
event_ref | "(" event_ref ("," event_ref)* ")"
```

Semantics: the operator selects the first / last / n-th event whose type is **in the list**. The optional WHERE predicate applies across all listed event types, evaluated per-event in the common (source-schema) column namespace.

```bql
events | FIRST((login, sso_login, mobile_login))
events | LAST((purchase, subscription) WHERE amount > 0)
events | NTH((page_view, mobile_page_view) WHERE url LIKE '/checkout%', 3)
```

**Why:** consistent with the pattern established by TASK-405 §11 (SESSIONIZE `end:` list) and TASK-406 §8 (ATTRIBUTE `conversion:` / `touchpoints:` lists). "First of any login-ish event" is a real use case; forcing users to `WHERE event_type IN (...) | FIRST(ANY_EVENT)` fights BQL's event-type-as-discriminator model.

**Column resolution model (same as TASK-406 §8):** bqlite resolves columns against the source table's schema, not per-event-type. The WHERE predicate is type-checked against the source schema once; at runtime it only evaluates on rows whose `event_type` is in the list. If some listed event types carry different column sets, missing columns resolve to NULL on those rows.

**Implication for TASK-421 parser:** `first_last_op` and `nth_op` productions accept both single `event_ref` and parenthesized list forms. AST node carries `Vec<EventRef>` (length ≥ 1). Duplicate-name diagnostic required within the list.

**Implication for TASK-429 (EventSelectOperator):** the per-entity candidate loop tests `event.event_type ∈ event_types_set` (small `HashSet<EventTypeId>`) rather than a single-type equality check.

### A5. Same-`ts` tie-breaking by `__seq_id`

When multiple qualifying events share a `ts`:
- **FIRST** selects the event with the smallest `(ts, __seq_id)`.
- **LAST** selects the event with the largest `(ts, __seq_id)`.
- **NTH** selects the `n`-th event in `(ts, __seq_id)` ascending order.

**Why:** determinism is load-bearing for test reproducibility and for repeat queries to return stable answers. `__seq_id` is already the canonical per-`ts` tiebreaker elsewhere in the codebase (matched-event ordering; TASK-407 B1 JOIN tiebreaking).

### A6. Forwarded columns — demand-driven via `DemandCapabilities`

The output schema advertises all source-table columns (`type-system.md` §6.7). The **physical per-entity candidate buffer** materializes only columns that downstream operators demand via `DemandCapabilities`. Undemanded columns are not copied into the candidate row.

**Why:** consistent with SESSIONIZE §9 / ATTRIBUTE §9 and the wider demand-driven approach (TASK-409 / TASK-427). EventSelect's per-entity state is a single candidate row; the absolute memory wins are smaller than for SESSIONIZE, but the convention should be uniform across stateful operators.

### A7. Fused aggregate shapes — none in v1

EventSelect emits full per-entity rows to downstream STATS. No `FIRST/LAST/NTH → STATS` fusion in v1.

**Candidates for later (documented as Wave 5 fusion opportunities, not implemented):**
- `FIRST(event) | STATS COUNT(*)` → per-entity presence boolean aggregated into a single counter.
- `FIRST(event) | STATS AVG(property) GROUP BY group_key` → single-row-per-entity extraction fed directly into grouped aggregate.

**Why:** same rationale as SESSIONIZE §10 / ATTRIBUTE §10. `FusedDownstream` is explicitly Wave 5 per `planner-pipeline.md` §5.3. Unfused EventSelect is cheap — per-entity state is a single candidate row; typical output row counts are per-entity (millions, not billions).

### A8. `lookback:` parameter for FIRST / NTH — scan-range widening

Grammar extension — FIRST and NTH (not LAST) accept an optional `lookback: <duration>` parameter:

```bql
events LAST 30d | FIRST(signup, lookback: 90d)
events LAST 30d | FIRST(signup WHERE plan = 'pro', lookback: 90d)
events LAST 30d | NTH(purchase WHERE amount > 0, 3, lookback: 1y)
events LAST 30d | FIRST((signup, premium_signup), lookback: 90d)
```

**Semantics.** The planner extends the source scan range backward by `lookback` from the outer time range's start. The operator observes events in the widened range. The selected event's actual `ts` is returned as-is — it may be before the outer range's start. WHERE predicates apply within the widened range. Downstream operators see the output row as any other — no "row is from the lookback zone" marker.

**Why this exists.** The naive composition `events LAST 30d | FIRST(signup)` returns "first signup within the last 30 days", which is rarely what analytics queries want. The true-first-ever semantic (for onboarding analysis, new-user cohort identification, first-touch attribution without ATTRIBUTE) requires scanning farther back than the outer range. Making this an explicit named parameter matches the convention established for ATTRIBUTE's `window:` and aligns with the planner-level scan-extension mechanism already in place.

**Default — A8a: no default, explicit opt-in.** FIRST/NTH without `lookback:` operates only on the outer time range (current behavior preserved). No hidden widening. Users who want the true-first semantic must write `lookback:` explicitly.

**Rationale:** BQL's principle is "no hidden magic" — the outer time range is explicit user intent, and silently widening would surprise users who wrote `LAST 30d` expecting their scan bounds respected. The "right" default is use-case-dependent (new-user onboarding wants 90d, fraud detection wants years); picking any fixed default is a footgun.

**Unbounded lookback — A8b: no special keyword; drop the outer range.** Users who want "scan all time" omit the outer time range entirely (`events | FIRST(signup)`). There is no `lookback: ALL` sentinel. `lookback:` is always a bounded, relative-duration extension.

**Rationale:** keeps the surface small; avoids introducing an `ALL` / `unbounded` enum value that would then want to propagate to other duration-typed parameters across the language.

**LAST does not take `lookback:` — A8c.** LAST's natural bound is the outer range's end (or "now"); there is no forward-looking analog because time's arrow points forward and future events don't exist at query time. Users who want "last signup through a specific past deadline" bound the outer range with `BETWEEN`.

**Implication for TASK-421 (parser):** `first_last_op` and `nth_op` productions accept an optional `, lookback: <duration>` parameter (trailing, after the existing args). The AST carries `lookback: Option<Duration>`. LAST's production rejects `lookback:` with a parse-time error.

**Implication for TASK-425 (lowering):** `EventSelect` logical → physical lowering threads `lookback` into the upstream `Scan`'s time-range widening, parallel to the planner's existing per-operator widening (ATTRIBUTE `window`, MATCH WITHIN, RETENTION BRACKETS). Integrates with TASK-407 B2's uniform widening across joined tables.

**Implication for TASK-429 (EventSelectOperator):** no operator-side change — the widening is transparent; the operator simply sees a wider event stream. The output row carries the real `ts` of the selected event regardless of whether it falls inside or outside the outer range.

**Must be documented in `query-language.md` §14.1:** the `lookback:` parameter, the "no hidden default" semantic, and a worked example showing the true-first-signup-ever pattern via `events | FIRST(signup)` with no outer range, vs the bounded-lookback pattern with `LAST 30d | FIRST(signup, lookback: 90d)`.

## Decisions — Block B: SAMPLE

### B1. `fraction:` range — `[0.0, 1.0]` inclusive both ends

`fraction: 0.0` (empty output) and `fraction: 1.0` (pass-through) are both legal and not special-cased. Values outside `[0.0, 1.0]` are parse-time errors.

**Why:** both boundaries have legitimate uses — `0.0` for test fixtures that verify empty-cohort handling, `1.0` as a dev-time toggle that flips sampling off without rewriting the pipeline. No reason to reject either.

### B2 / B3. **Remove `count:` parameter entirely from SAMPLE**

SAMPLE accepts only `fraction:`. The `count:` parameter (originally specified in `query-language.md` §14.2, `type-system.md` §6.11, and the grammar's `sample_param` production) is **removed** before v1 ships.

Revised grammar:

```
sample_op        := SAMPLE "(" "fraction" ":" number ("," "seed" ":" integer)? ")"
```

Users who need a target count:
- For approximate-N sampling, compute `fraction = N / entity_count_estimate` manually and pass as `fraction:`.
- For exact-N sampling, use `ORDER BY <deterministic_expr> | LIMIT N` on an entity-level projection (semantics and cost are explicit and user-controlled).

**Why:** `count:` was a semantically fraught parameter. Making it work under the scan-pushdown contract (TASK-430) requires either:
- Approximate semantics (convert to `fraction` using catalog-level entity count; actual output is `N ± √N`) — which is surprising to users who wrote `count: 10000` expecting exactly 10,000 rows.
- Two-pass execution (count entities, then sample) — doubles the scan cost on large tables.
- Reservoir sampling — defeats the pushdown entirely; single-threaded bottleneck.

Every option has a significant correctness-vs-performance tradeoff that the design doc would have to expose to users. Rather than paper over the tradeoff with "approximate count," drop the parameter and make the available modes (`fraction:` for pushdown sampling, `ORDER BY ... LIMIT N` for exact count) explicit and composable.

**Implications:**
- **TASK-421 (parser):** `sample_param` grammar becomes `"fraction" ":" number` only. Remove the `count:` alternative and its AST variant.
- **TASK-424 (planner):** `Sample { fraction: f64, seed: Option<i64>, input, output_schema }` — drop the `SampleSpec` enum's count variant.
- **TASK-430 (pushdown):** simpler pushdown path — always a fraction-threshold against the hashed entity id.
- **`query-language.md` §14.2:** drop the `count:` bullet; drop the `count: 10000` example.
- **`type-system.md` §6.11:** drop the `count:` mention and the `count: 10000` example.

### B4. Hash function — xxHash64, stable across bqlite versions forever

SAMPLE hashes entity-id values with **xxHash64**, seeded per-query with either the explicit `seed:` parameter value or the database-UUID-derived default seed (per §30.9). The hash function is pinned in the design doc with an explicit stability contract: **repeat queries on the same database with the same seed always return the same sampled entity set, indefinitely**.

- Bumping the hash function to a different algorithm is a user-visible breaking change that would be gated behind a major version bump with a prominent migration note.
- The specific seeding protocol (how the explicit seed combines with the hash input) is the same across all bqlite versions.

**Why:** xxHash64 is widely used, fast, has good distribution, has stable Rust implementations (the `xxhash-rust` crate is the de facto canonical one — verify at implementation time). Pinning a specific hash is required for reproducibility; pinning `xxHash64` specifically matches industry convention (BigQuery, Presto, Spark all use it in analogous sampling paths).

**Implication for TASK-430 (pushdown):** the storage-layer sample filter computes `xxhash64(entity_id_bytes, seed) < fraction * u64::MAX` — fraction thresholding against the hash output. Entity-id bytes are the canonical serialization (for `String`, UTF-8 bytes; for `Int`, little-endian 8 bytes) — document the serialization explicitly in the design doc since the hash output depends on it.

### B5. SAMPLE population — source-table invariant; commutes with upstream stateless filters

SAMPLE's "population" is the **source-table entity set**, regardless of which filters sit upstream in the pipeline. Formally: for any entity-key-independent predicate `P`:

```
events | WHERE P | SAMPLE(fraction: f) ≡ events | SAMPLE(fraction: f) | WHERE P
```

The hash is computed over `entity_id` alone — whether an entity has any events matching `P` doesn't affect whether the entity is "in" the sampled set.

**Why:** SAMPLE's semantic promise is "pick 10% of entities" (§14.2). Making SAMPLE's population depend on upstream WHERE state would mean two logically equivalent queries produce different sampled sets, breaking both user intuition and the pushdown contract.

**Implication for TASK-430 (pushdown):** because SAMPLE is population-invariant under stateless upstream filters, the planner can always push the sample filter into the scan regardless of what sits between `source` and `SAMPLE` (as long as the intermediate stages are stateless — WHERE, SELECT, LET). §25.2's composition rules are about pipeline grammar, not about population definition.

**User-facing caveat (must be documented in `query-language.md` §14.2):** the output row set is `sampled_entities ∩ filtered_events` — a sampled entity with no events matching upstream WHERE contributes zero rows to output, but is still "in" the sampled set.

### B6. SAMPLE + `IN alias` / `IN QUERY` cohorts — strict B5 consequence

```bql
events | WHERE entity_id IN churned | SAMPLE(fraction: 0.1)
```

produces `(churned ∩ sampled)` — 10% of the **full source population**, further filtered to only those in `churned`. Expected output size is `|churned| * 0.1` (assuming the cohort's entity-id distribution matches the source's, which it does by construction since the cohort is a subset).

Users who want "10% of the churned cohort" (i.e., sampling *within* the cohort rather than across the whole source) write:

```bql
sampled_churned = churned | SAMPLE(fraction: 0.1)
events | WHERE entity_id IN sampled_churned
```

or equivalently:

```bql
events | SAMPLE(fraction: 0.1) | WHERE entity_id IN churned
```

which, per B5, is identical to the first form because SAMPLE's population is source-invariant.

**Why:** strict consequence of B5(a). SAMPLE is not cohort-aware; it samples the population.

## Decisions — Block C: composition

### C1. `SESSIONIZE | FIRST/LAST/NTH` — allowed with entity-level semantics

`query-language.md` §25.2's downstream table is updated to list FIRST/LAST/NTH as valid downstreams of SESSIONIZE. The composition produces **entity-level** (not session-level) selection — the operator picks the first/last/n-th event per *entity*, not per session.

`session_id` and `session_duration` flow through as forwarded columns (demand-driven per A6) when referenced downstream. Users who want per-session selection express it explicitly downstream using a window function or MATCH — the operator does not provide session-scoped selection natively in v1.

**Why:** consistent with TASK-406 §12's `SESSIONIZE | ATTRIBUTE` decision — allow the composition without adding operator-side session awareness. Per-session selection is a substantive semantic feature (what does "first per session per entity" mean for entities with many sessions? do you emit N rows per entity or one?) that deserves its own design pass.

**Must be documented in `query-language.md` §25.2:** add `SESSIONIZE` → `FIRST/LAST/NTH` to the valid-downstream table with the "no automatic session-scoped selection" caveat.

### C2. Chained EventSelects — allowed; semantically empty but not a special case

`events | FIRST(signup) | LAST(purchase)` is legal. Plan-time check passes, runtime returns empty for every entity (after FIRST(signup), each entity has exactly one row, which is a signup, which cannot be a purchase). No special rule, no warning.

**Why:** the composition is semantically well-defined even if practically useless. Rejecting at plan time requires a "no second EventSelect after an EventSelect" rule with edge cases around intermediate WHERE/SELECT that would proliferate. Warning channels don't exist in Wave 4 (per TASK-404 §9). Empty output is self-explanatory when users run the query.

## Follow-on implications to propagate

These are consequences worth calling out for downstream tasks:

- **TASK-421 (parser: FIRST/LAST/NTH + SAMPLE)**:
  - `first_last_op` and `nth_op` accept parenthesized event-type list (§A4).
  - `nth_op` grammar keeps the `event_ref (WHERE predicate)? "," integer` form (§A2); reconcile TASK-421's prose description.
  - `first_last_op` and `nth_op` accept optional `, lookback: <duration>` (§A8). LAST rejects `lookback:` at parse time.
  - `sample_op` grammar: `SAMPLE "(" "fraction" ":" number ("," "seed" ":" integer)? ")"` — drop the `count:` alternative (§B2/B3).
  - Duplicate-name diagnostic required for event-type lists.
- **TASK-424 (planner: EventSelect + Sample physical nodes)**:
  - `EventSelect { kind: FIRST|LAST|NTH(n), event_types: Vec<EventType>, predicate: Option<TypedExpr>, lookback: Option<Duration>, forwarded_columns: Vec<ColumnId>, fused_downstream: Option<FusedDownstream>, input, output_schema }`.
  - `Sample { fraction: f64, seed: Option<i64>, input, output_schema }` — no `SampleSpec` enum; `count:` gone.
- **TASK-425 (lowering)**:
  - Thread `lookback` into upstream `Scan` time-range widening for FIRST/NTH (§A8).
  - Integrates with TASK-407 B2 uniform widening across joined tables.
  - Drop the `count:` desugaring / handling path.
- **TASK-429 (EventSelectOperator)**:
  - Per-entity candidate loop tests against `event_types_set` (§A4).
  - §A3 "fewer than n" → omit entity.
  - §A5 same-`ts` tie-breaking by `__seq_id`.
  - §A6 demand-driven forwarded columns.
  - No operator-side change for `lookback:` (§A8) — widening is transparent at the operator boundary.
- **TASK-430 (SAMPLE pushdown)**:
  - Fraction-only pushdown — no count-to-fraction translation path (§B2/B3).
  - xxHash64 over canonical entity-id byte serialization (§B4).
  - B5 commutativity with upstream stateless filters enables pushdown through WHERE / SELECT / LET chains.
- **`query-language.md`**:
  - **§14.1**: document event-type list form (§A4), `lookback:` parameter with rationale + worked example (§A8), §A3 "fewer than n" rule, §A5 tie-breaking. Reconcile TASK-421's prose about NTH's WHERE position (§A2).
  - **§14.2**: remove `count:` (§B2/B3); document §B1 boundary values; document §B4 hash function + stability guarantee; document §B5 population-invariance with the `sampled ∩ filtered` output rule.
  - **§25.2**: add `SESSIONIZE → FIRST/LAST/NTH` with the "no session-scoped selection" caveat (§C1).
  - **§26 grammar**: update `first_last_op` and `nth_op` to accept event-type lists and trailing `lookback:` param; update `sample_op` to remove `count:`.
- **`type-system.md`**:
  - **§6.7**: restate §A5 tie-breaking; note that FIRST/LAST/NTH output may fall outside the outer time range when `lookback:` is present (§A8).
  - **§6.11**: remove `count:` mention and example; keep `fraction:`-only surface.
- **Wave 5 fusion task (TASK-503 ecosystem)**:
  - Add §A7 EventSelect → STATS fusion candidates to the fusion-opportunities list.
  - Reminder: per-session FIRST/LAST/NTH (v2 operator variant) is a separate design task, not a fusion opportunity (§C1).
