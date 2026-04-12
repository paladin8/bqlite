# Query Language Design

> **Status**: DRAFT
> **Task**: TASK-002
> **Depends on**: TASK-005 (type system), TASK-004 (sequence matching), TASK-003 (execution model), TASK-001 (storage format)
> **Depended on by**: TASK-006 (planner pipeline), all Wave 2+ implementation

---

## 1. Design Goals

BQL is the surface language users write to query bqlite. It serves three constraints from [core-beliefs.md](../core-beliefs.md):

**Powerful primitives (Belief 2).** BQL exposes a small set of operators that compose freely via pipes. Funnels, retention, cohort analysis, and path analysis are all expressible as compositions of the same primitives — MATCH, STATS, WHERE, SELECT, SESSIONIZE. Specialized operators (FUNNEL, RETENTION) exist only as sugar over the primitives.

**Entity-first data model (Belief 3).** Every query operates over entity event streams. The entity key is implicit — declared once in the table schema, never repeated in queries. Time range is prominent in the source expression because time-window pruning is the dominant cost optimization at the storage layer.

**Strongly-typed pipelines (Belief 8).** Every operator has a precise output schema (type-system.md Section 6). Pipe composition is schema-verified at plan time. Type errors are reported with enough context to locate the problem in the source text. There are no runtime type errors during query execution.

The language is designed for humans writing behavioral analytics, not for generated SQL. It is terser than SQL for common patterns (no mandatory `SELECT ... FROM` boilerplate), explicit where SQL is ambiguous (pipe order matches execution order), and specialized where behavior matters (MATCH, SESSIONIZE, BRACKETS).

---

## 2. Language Philosophy

### 2.1 Pipeline-First

BQL uses typed pipelines with the `|` operator, similar to KQL/Kusto and PRQL. Data flows left to right through a chain of operators. This is the right model for behavioral queries — they are naturally sequential: "start with events, filter, match a pattern, aggregate."

SQL-style nested `SELECT` trees invert the reading order and obscure data flow. A pipe-first syntax makes the execution order visible and eliminates the need for CTEs in the common case.

### 2.2 Naming Convention

All BQL keywords, operators, and functions are **ALL-CAPS** (MATCH, STATS, WHERE, SELECT, SEQUENCE, WITHIN, GROUP BY, ORDER BY, CAST, COALESCE, etc.). Event names, column names, and table names are user-defined and typically lowercase. This makes it easy to distinguish language constructs from data in a query.

The parser accepts keywords case-insensitively for compatibility with external tooling, but canonical examples in documentation always use uppercase for keywords.

**User identifiers are case-sensitive.** Table names, column names, event names, alias names, variable names (`$plan`), and step names (`s:`) are all matched case-sensitively. `User_ID` and `user_id` are distinct columns. This matches type-system.md Section 5.1 which requires column names to be unique under case-sensitive comparison. The full reserved-keyword list is in Section 26.2.

### 2.3 Relationship to Other Designs

This document specifies the surface syntax and the composition rules between operators. It does not specify:

- **Type semantics**: owned by type-system.md (BqlType, operator output schemas, scalar function signatures, coercion rules, null handling, schema DDL).
- **Sequence matching internals**: owned by sequence-matching.md (NFA construction, candidate propagation, variable binding tracks, EMIT ALL semantics, tiered execution).
- **Execution semantics**: owned by execution-model.md (pipeline composition, demand propagation, operator fusion, parallelism, memory budget).
- **Planning and optimization**: owned by planner-pipeline.md / TASK-006 (logical plan construction, optimizer rules, physical plan selection, DemandCapabilities propagation).

Where this document refers to these concerns, it does so through explicit cross-references. The surface syntax and composition rules are the contract this document owns.

---

## 3. Source Expression

Every query starts with a table reference. The source expression also names the time range of interest, which is the dominant pruning hint for the storage layer.

### 3.1 Table Reference and Time Range

```bql
events                                              -- all data
events LAST 30d                                     -- relative range
events BETWEEN '2025-01-01' AND '2025-06-30'        -- explicit UTC range
```

`LAST <duration>` is relative to query execution time. `BETWEEN '<start>' AND '<end>'` takes ISO-8601 timestamp literals; both bounds are treated as UTC if timezone is not specified. The range is **closed-open**: `[start, end)`.

If no time range is given, the scan reads the entire table. This is legal but usually a mistake for large tables; the planner emits a warning.

### 3.2 Time Range as Entry Scope

The stated time range scopes the **entry** of any downstream pattern — it bounds which events can start a match, not how far matches can extend. The planner automatically extends the scan's upper time bound when MATCH has a `WITHIN` or `BRACKETS` clause:

- `events LAST 30d | MATCH ... WITHIN 7d` → scan range extends 7d beyond the stated range so entries near the boundary can still match.
- `events LAST 90d | MATCH ... BRACKETS [1d, 7d, 14d, 30d]` → scan extends 30d beyond (the maximum bracket).

The entry-qualifying events are filtered to the user's stated range at the scan layer. Events beyond the stated range are visible only to the matcher for completing matches that started inside the range. This is transparent to users and must be documented clearly: "the range you specify is the range of possible match starts, not the range of data read."

This rule exists because the alternative — requiring users to widen the range manually and then filter — is both error-prone and defeats time-window pruning in the storage layer.

### 3.3 No BY Clause

BQL has no `BY <column>` clause anywhere. The entity key is always the column declared as `ENTITY KEY` in the table schema (type-system.md Section 5.1). Partitioning or grouping by a non-entity-key column is meaningless for the temporal operators (MATCH, SESSIONIZE, FIRST/LAST/NTH) because they require entity-sorted input to operate correctly — switching the partition axis to an arbitrary column would require re-sorting the entire stream, which BQL does not do.

For aggregate grouping, use `GROUP BY` on STATS (Section 7.2). For filtering to a subset of entities, use `WHERE entity_id IN <alias>` (Section 17). These two mechanisms together cover every legitimate use case that a `BY` clause might have been proposed for.

### 3.4 Cross-Table Entity Join

A source expression may join multiple tables with the `JOIN` keyword, provided all tables share the same entity-key type. The join is a streaming merge on pre-sorted entity-ordered data.

```bql
events LAST 30d JOIN purchases
| MATCH FIRST SEQUENCE(events.signup THEN purchases.purchase) WITHIN 7d
```

Cross-table joins are covered in Section 19.

---

## 4. MATCH — Sequence Pattern Matching

MATCH is the central primitive of BQL. It compiles to the NFA engine defined in sequence-matching.md.

### 4.1 Basic Syntax

```bql
events | MATCH FIRST SEQUENCE(step1 THEN step2 THEN step3) WITHIN 7d
```

`THEN` is the primary step separator. `->` is accepted as an alias for readability in dense patterns:

```bql
events | MATCH FIRST SEQUENCE(signup THEN purchase)
events | MATCH FIRST SEQUENCE(signup -> purchase)
```

The two forms are semantically identical; mixing them in a single pattern is permitted but discouraged by style.

### 4.2 Match Modes

```bql
MATCH FIRST SEQUENCE(...)           -- first match per (entity, binding track)
MATCH ALL SEQUENCE(...)             -- all non-overlapping matches per (entity, binding track)
```

FIRST is the default semantics for funnels and conversion analysis. ALL is the default for enumerating every occurrence of a pattern. Overlapping match mode is intentionally not supported — variable bindings (Section 4.11) are the escape hatch for patterns that need to share events across multiple logical matches.

See sequence-matching.md Section 5 for the full match mode specification.

### 4.3 EMIT ALL

```bql
MATCH FIRST SEQUENCE(...) EMIT ALL  -- one row per (entity, binding track) with step_reached
MATCH ALL SEQUENCE(...) EMIT ALL    -- one row per NFA entry within a binding track, with step_reached
```

EMIT ALL adds a `step_reached` column to the output schema. With FIRST + EMIT ALL, each `(entity, binding track)` pair produces exactly one row — either the completed match or the farthest partial. With ALL + EMIT ALL, each step 1 match within a binding track produces a row, each with its own `step_reached` value. This is essential for funnel analysis — without EMIT ALL, the output contains only completed matches and step-wise dropoff is invisible.

See sequence-matching.md Section 5.3 for emission rules (MATCH FIRST EMIT ALL produces one row per `(entity, binding track)`; MATCH ALL EMIT ALL produces one row per step 1 match within a binding track) and Section 12 for the output schema.

### 4.4 Named Steps

Steps can be given explicit names for referencing matched event properties downstream:

```bql
MATCH FIRST SEQUENCE(s: signup THEN p: purchase) WITHIN 7d
| SELECT s.ts AS signup_time, p.amount AS purchase_amount
```

Step names are prefixed to the step event with a colon. They are required when a pattern contains repeated event types so that downstream operators can disambiguate:

```bql
MATCH FIRST SEQUENCE(v1: page_view THEN v2: page_view) WITHIN 1h
| SELECT v1.page AS from_page, v2.page AS to_page
```

Without explicit names, the parser auto-generates names from the event type. Repeated event types in the pattern produce auto-generated names with numeric suffixes (`page_view_0`, `page_view_1`).

Step name scoping: step names are local to a single MATCH expression. They may be referenced by subsequent WHERE, SELECT, LET, STATS stages but do not leak across aliases.

**Cross-step comparisons inside pattern predicates must use variable bindings.** A step's WHERE clause cannot reference another step by name (`v2: page_view WHERE page != v1.page` is not supported). To express "step 2 has a different page than step 1", use a variable binding:

```bql
MATCH FIRST SEQUENCE(
    v1: page_view WHERE page = $p
    THEN v2: page_view WHERE page != $p
  ) WITHIN 1h
```

Note that each distinct value of `$p` creates its own independent match track (Section 4.11), which means some semantics that a fully general cross-step comparison form could express cannot be expressed with bindings alone. See Section 30.11 for the tradeoff and examples.

### 4.5 Property Constraints

Each step may attach a `WHERE` predicate on the event's properties:

```bql
MATCH FIRST SEQUENCE(signup THEN purchase WHERE amount > 100)
MATCH FIRST SEQUENCE(signup WHERE plan = 'pro' THEN purchase WHERE amount > 100)
```

The predicate is an arbitrary expression over the event's property columns, evaluated in the context of that single event. It must return `Bool`. NULL predicate results fail the step (SQL three-valued logic, type-system.md Section 3.2).

Property predicates are pushed down to the scan layer when possible — the planner combines them with event-type filters to minimize the rows the NFA sees. See sequence-matching.md Section 9 for pushdown rules.

### 4.6 Time Windows

Global window from first step (the only window mode):

```bql
MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 7d
MATCH FIRST SEQUENCE(signup THEN purchase THEN checkout) WITHIN 30d
```

`WITHIN <duration>` enforces that all steps complete within the given duration of the first step. Same-timestamp events do not satisfy THEN — the NFA uses strict `>` comparison for ordering (sequence-matching.md Section 2, requirement 1).

Per-step time windows (e.g., "step 2 within 1d of step 1, step 3 within 7d of step 2") are explicitly not supported in v1. They add substantial complexity to the candidate propagation model for limited additional expressiveness. Global windows plus post-match filtering on named step timestamps cover the motivating use cases:

```bql
MATCH FIRST SEQUENCE(s: signup THEN p: purchase THEN r: refund) WITHIN 37d
| WHERE p.ts - s.ts < 7d AND r.ts - p.ts < 30d
```

### 4.7 Negation (WITHOUT)

`WITHOUT` is interleaved between steps. It binds to the gap between the previous step and the next step. Each step optionally declares what must NOT appear in the gap leading to it:

```bql
-- No churn between signup and purchase
MATCH FIRST SEQUENCE(signup WITHOUT churn THEN purchase)

-- No refund between signup and purchase, no churn between purchase and checkout
MATCH FIRST SEQUENCE(
    signup WITHOUT refund THEN purchase WITHOUT churn THEN checkout
)

-- Multiple exclusion types in one gap
MATCH FIRST SEQUENCE(signup WITHOUT (refund OR churn) THEN purchase)
```

Grammar: `step (WITHOUT exclusion THEN step)*`. WITHOUT attaches to the step before it as a constraint on the gap between that step and the next. Trailing WITHOUT (after the last step) is a parse error.

Exclusion targets are included in scan-level event-type filtering — the NFA needs to see them to fire the poison transitions. See sequence-matching.md Section 3.4 for the poison transition model.

### 4.8 Alternation on Steps

```bql
-- Either signup type counts
MATCH FIRST SEQUENCE((signup_web OR signup_mobile) THEN purchase) WITHIN 7d

-- Alternation on any step
MATCH FIRST SEQUENCE(signup THEN (purchase OR subscription)) WITHIN 7d
```

Alternation is a step-level construct — it substitutes for a single event type position. A WHERE clause on an alternation step applies to whichever alternative matched. Variable bindings referenced in the WHERE clause work across alternatives provided both alternatives share the referenced property column (planner-enforced).

### 4.9 Repetition

```bql
MATCH FIRST SEQUENCE(signup THEN page_view+ THEN purchase)    -- one or more
MATCH FIRST SEQUENCE(signup THEN page_view* THEN purchase)    -- zero or more
```

Repetition is implemented as NFA self-loops (sequence-matching.md Section 3.5). It composes with WHERE predicates by attaching to the outside of the step:

```bql
-- The repetition suffix applies to the whole step (event type + predicate)
MATCH FIRST SEQUENCE(signup THEN (page_view WHERE category = 'shop')+ THEN purchase)
```

**Parenthesizing repetition around a WHERE-qualified step is required.** The bare form `page_view WHERE category = 'shop' +` is ambiguous: the expression parser could consume `+` as a trailing arithmetic operator inside the predicate. The grammar resolves this by requiring parentheses around any step that combines a WHERE clause with a repetition suffix. This keeps the expression parser decoupled from the step-level repetition token.

Variable bindings interact with repetition in a subtle way: a variable first bound inside a repeated step must remain constant across all iterations of that step (see Section 4.11, "Variables under repetition"). This rule is checked at plan time.

### 4.10 Consecutive Matching (IMMEDIATELY)

```bql
MATCH FIRST SEQUENCE(A THEN IMMEDIATELY B THEN IMMEDIATELY C)
```

Non-consecutive matching is the default: events between matched steps are allowed and ignored by the NFA. IMMEDIATELY opts into consecutive matching for a specific step transition — there must be no intervening events between the prior step and the IMMEDIATELY-marked step.

IMMEDIATELY is rare in practice. Most behavioral queries allow arbitrary events between steps (users do many things between signing up and purchasing). It exists for tightly-coupled event sequences like protocol state machines.

Patterns containing IMMEDIATELY are planned to a dedicated consecutive matcher rather than the general non-consecutive NFA path. Adjacency is positional, so the implementation uses a simpler specialized state machine.

### 4.11 Variable Bindings

Variables bind on the first step that references them, check equality on subsequent steps:

```bql
MATCH FIRST SEQUENCE(
    signup WHERE plan = $plan
    THEN purchase WHERE plan = $plan
) WITHIN 30d
```

Each distinct binding value creates an **independent match track**. The effective entity key becomes `(entity_id, binding_values)`. Two tracks for the same entity with different `$plan` values are treated as separate match streams — they do not interfere with each other, and events can participate in matches across different tracks.

See sequence-matching.md Section 8 for the binding track model and Section 11 of type-system.md for variable type inference rules.

**Variable naming.** `$` followed by a bare identifier — the grammar is `"$" identifier` where `identifier := [a-zA-Z_][a-zA-Z0-9_]*`. Quoted identifiers (Section 23) are not allowed after `$`; only bare identifiers. Variables are scoped to a single MATCH expression — they do not leak across pipe stages. Two MATCH expressions in different aliases may reuse the same variable name without conflict.

**Variable output columns.** Each bound variable becomes an output column of MATCH, named by the variable (without the `$`). This enables grouping downstream:

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(
    signup WHERE plan = $plan
    THEN purchase WHERE plan = $plan
  ) WITHIN 7d EMIT ALL
| STATS conversion = AVG(CAST(step_reached >= 2 AS INT))
  GROUP BY plan
```

**Variables under repetition.** A variable first bound inside a repeated step (`(B WHERE prop = $x)+`) holds across all iterations of that step and remains bound when the NFA exits the loop. The NFA enforces equality on every iteration. If a subsequent non-repeated step references `$x`, it compares against the bound value.

**NULL binding values.** If the binding expression evaluates to NULL, the step predicate fails (three-valued logic). No track is created for a NULL value.

### 4.12 BRACKETS (Retention Time Slicing)

BRACKETS runs a two-step (or multi-step) match against multiple time windows simultaneously. It is the primary mechanism for retention analysis.

```bql
MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
```

**Exclusive brackets (default).** `BRACKETS [1d, 7d, 14d, 30d]` means: did step 2 occur during `[0-1d]`, `(1d-7d]`, `(7d-14d]`, or `(14d-30d]` from the anchor? Each bracket is a distinct time slice with no overlap. Every matching event falls in exactly one bracket.

**Cumulative brackets (opt-in).** Backward partial sum over exclusive brackets. "Retained at bracket N" means active in ANY bracket 0..N.

```bql
-- Exclusive (default): which slice did the step 2 fall in?
MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL

-- Cumulative: did step 2 fall in bracket N or any earlier bracket?
MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS CUMULATIVE [1d, 7d, 14d, 30d] EMIT ALL
```

**Output schema.** BRACKETS extends the MATCH output with two columns:

| Column | Type | Nullable | Description |
|---|---|---|---|
| `bracket` | Int | no | Bracket index (0-indexed) |
| `bracket_end` | Int | no | Bracket upper bound (nanos) for display |

One row per `(entity, binding track, bracket)`. Without EMIT ALL, only brackets where the step completed are emitted. With EMIT ALL, every bracket is emitted regardless of completion — `step_reached` distinguishes completed brackets from dropouts.

**Bracket index semantics.** Brackets are 0-indexed from the anchor event. `bracket = 0` means "between anchor time and first bracket boundary." `bracket = 1` means "between first and second boundaries." The highest index is the final bracket (`N-1` for N boundaries).

**BRACKETS and WITHIN are mutually exclusive** — BRACKETS defines its own time structure. The planner rejects queries that specify both.

**Planner scan extension.** The planner extends the scan range by `max(brackets)` beyond the user's stated time range (Section 3.2).

**MATCH FIRST with BRACKETS.** Only the first entry per entity/binding track counts as cohort entry. This is the standard cohort-retention definition.

**MATCH ALL with BRACKETS.** Every qualifying entry starts a new retention window. Each entry produces a separate set of per-bracket rows.

### 4.13 Pattern Composition Summary

A MATCH expression has the structure:

```
MATCH <mode> SEQUENCE(<step_list>) <modifiers>
```

Where `<mode>` is FIRST or ALL, `<step_list>` is steps joined by THEN (with optional WITHOUT between steps), and `<modifiers>` are any combination of:

- `WITHIN <duration>` — global time window
- `WITHIN SESSION` — window scoped to the enclosing session (requires upstream SESSIONIZE)
- `BRACKETS [<durations>]` — bracket time windows (mutually exclusive with WITHIN)
- `BRACKETS CUMULATIVE [<durations>]` — cumulative brackets
- `EMIT ALL` — emit all NFA entries including dropouts

Modifiers must appear in this canonical order: `WITHIN` or `BRACKETS` first, then `EMIT ALL`. The parser enforces this ordering — modifiers written out of order are a parse error. A fixed order keeps the grammar unambiguous and matches how users naturally read match expressions.

---

## 5. MATCH Output Schema and Downstream Access

### 5.1 Output Columns

From sequence-matching.md Section 12 and type-system.md Section 6.1:

| Column | Type | Nullable | Present | Description |
|---|---|---|---|---|
| `entity_id` | String or Int (matches entity key) | no | Always | Entity identifier |
| `$var` columns | per variable type | no | When variables bound | One column per bound variable |
| `step_reached` | Int | no | When EMIT ALL | 1-indexed step number of farthest step matched |
| `bracket` | Int | no | When BRACKETS | Bracket index |
| `bracket_end` | Int | no | When BRACKETS | Bracket upper bound in nanos |
| `match_duration` | Int | yes | When demanded | First-to-last matched event time (NULL if `step_reached == 1`) |
| `match_events` | Map(Timestamp) | yes | When demanded | Step name → timestamp |

**Entity ID column naming.** The output column is always named `entity_id`, regardless of the source table's entity-key column name. For example, if the source table declares `user_id STRING ENTITY KEY`, MATCH still outputs a column called `entity_id`. Downstream operators that filter by entity (e.g., `WHERE entity_id IN converted`) use this canonical name. This rename is transparent in single-table queries but must be kept in mind when piping MATCH output back into a source stream via an IN subquery — the subquery must `SELECT entity_id`, not `SELECT user_id`.

**Entity ID column type.** The type is fixed at plan time: it matches the entity-key column type of the source table. In cross-table joins, all tables must agree on the entity-key type (Section 19.3).

**Variable binding columns.** Variable columns are named by the variable without the `$` prefix (`plan`, not `$plan`). This is the form referenced in downstream GROUP BY and WHERE clauses. Per type-system.md Section 6.1, variable columns are non-nullable because only events with non-NULL binding values pass the step predicate (Section 4.11 "NULL binding values").

**`match_events` type notation.** Per type-system.md Section 2.1, all BqlType Maps have String keys, so `Map(Timestamp)` denotes `String → Timestamp`. The single-argument form is the canonical notation used throughout the bqlite design docs.

### 5.2 Named Step Property Access

After MATCH, use named step aliases with dot notation to access properties of matched events:

```bql
MATCH FIRST SEQUENCE(s: signup THEN p: purchase) WITHIN 7d
| SELECT s.ts AS signup_time, p.amount AS purchase_amount, p.ts - s.ts AS time_to_convert
```

Step property access is driven by **per-(step, column) demand bits**. When a downstream operator references `s.ts`, `p.amount`, or any other step-property expression, the planner records a `(step_name, column_name)` entry on the MATCH operator's demand set. MATCH retains exactly those properties — no full `match_events` map, no non-referenced columns from the matched events. The `match_events` map (and `match_duration`) are only materialized when a downstream expression references those specific names. This is specified in planner-pipeline.md §8.2 and §9.3; see Section 30.1 for the decision record.

For filtering on match structure:

```bql
MATCH FIRST SEQUENCE(s: signup THEN p: purchase THEN r: refund) WITHIN 37d
| WHERE p.ts - s.ts < 7d AND r.ts - p.ts < 30d
```

**Property access beyond timestamps.** `s.ts` always works — the step timestamp is always available. Non-timestamp properties (e.g., `p.amount`) require the engine to retain the referenced column value at the moment that step's event is consumed. This is a compile-time demand declared by the MATCH operator when it sees downstream references, set per-(step, column) — only the referenced pairs are carried, everything else is discarded.

### 5.3 MATCH Does Not Preserve the Event Stream

MATCH transforms the event stream into match results — it does not preserve the original event stream for downstream operators. MATCH → MATCH chaining is not supported.

To filter entities by a match AND do further analysis on their event stream, use `WHERE entity_id IN` with an aliased subquery:

```bql
converted = events LAST 30d
  | MATCH FIRST SEQUENCE(signup THEN purchase) WITHIN 7d
  | SELECT entity_id

events LAST 30d
| WHERE entity_id IN converted
| WHERE event_type = 'support_ticket'
| STATS support_tickets = COUNT(*)
```

This is slightly more verbose than a hypothetical `WHERE MATCHED(...)` sugar, but it composes naturally with the rest of the language. The alias is reusable, the intent is explicit, and there is no special-case syntax. See Section 18 on aliases.

### 5.4 No WHERE MATCHED

There is no special `WHERE MATCHED(...)` syntax. Entity filtering by pattern is accomplished through aliases and `IN` (Section 5.3). Having two ways to express the same thing would create confusion without adding power.

### 5.5 Empty Results

An empty event stream, an entity with no events matching any step, or a pattern that matches zero times all produce **zero rows** in the MATCH output. This is the default behavior everywhere in BQL: if no rows match, the result set is empty. Downstream aggregates over an empty MATCH output produce one row with `COUNT(*) = 0` and NULL for other aggregates (type-system.md Section 6.4, standard SQL semantics). No synthetic "empty" sentinel row is emitted. Sequence-matching.md Section 16.4 covers the per-entity empty-stream case in more detail.

---

## 6. FUNNEL and RETENTION — Convenience Sugar

FUNNEL and RETENTION are syntactic sugar over MATCH + EMIT ALL + STATS. They are desugared during logical planning — the planner rewrites them into primitive operations before type checking and optimization (planner-pipeline.md TASK-006 specifies the exact rewrite pass).

### 6.1 FUNNEL

```bql
-- Basic sugar:
events LAST 30d
| FUNNEL(signup THEN add_to_cart THEN purchase) WITHIN 7d

-- Desugars to:
events LAST 30d
| MATCH FIRST SEQUENCE(signup THEN add_to_cart THEN purchase) WITHIN 7d EMIT ALL
| STATS
    signup = SUM(CAST(step_reached >= 1 AS INT)),
    add_to_cart = SUM(CAST(step_reached >= 2 AS INT)),
    purchase = SUM(CAST(step_reached >= 3 AS INT))
```

**FUNNEL accepts the full MATCH step grammar.** Named steps, property constraints, variable bindings, WITHOUT exclusions, alternation, repetition, and IMMEDIATELY are all valid inside a FUNNEL. The sugar layer only fixes the *match mode* (always `FIRST`), forces `EMIT ALL`, and generates the step-reached STATS — everything else is inherited from MATCH. This is intentional: a funnel is "count how many entities reach each step of a pattern", and that definition is useful for any pattern, not just linear bare-event sequences.

```bql
-- Funnel with property constraints, bindings, and an exclusion:
events LAST 30d
| FUNNEL(
    s: signup WHERE s.country = 'US'
    THEN a: add_to_cart WHERE a.cart_value > 50
    WITHOUT churn
    THEN p: purchase WHERE p.plan = $plan AND p.plan = s.signup_plan
  ) WITHIN 7d

-- Desugars to (the planner fills in the STATS automatically):
events LAST 30d
| MATCH FIRST SEQUENCE(
    s: signup WHERE s.country = 'US'
    THEN a: add_to_cart WHERE a.cart_value > 50
    WITHOUT churn
    THEN p: purchase WHERE p.plan = $plan AND p.plan = s.signup_plan
  ) WITHIN 7d EMIT ALL
| STATS
    s = SUM(CAST(step_reached >= 1 AS INT)),
    a = SUM(CAST(step_reached >= 2 AS INT)),
    p = SUM(CAST(step_reached >= 3 AS INT))
```

**Step naming in the desugared STATS.** The output aggregate names follow these rules:

1. If the step is named (`s: signup`), the aggregate output name is the step name (`s`).
2. If the step is a bare event type (`signup`), the aggregate output name is the event type (`signup`).
3. If the step is a backtick-quoted name, the backticks are stripped for the output name.
4. If two steps produce the same output name (e.g. `signup THEN signup` without step names), the planner raises `TypeError::NameCollision { name: "signup", context: "FUNNEL step outputs" }` (type-system.md §12) — the user must add step names to disambiguate (e.g. `s1: signup THEN s2: signup`).

**Note on the aggregation idiom.** Per type-system.md Section 6.4, `COUNT(col)` counts non-null values of `col`. Since `step_reached` is non-nullable, `COUNT(step_reached >= N)` would count every row regardless of the predicate's value — which is wrong. The correct pattern for "count rows where predicate is true" is `SUM(CAST(predicate AS INT))`, which leverages `CAST(Bool AS INT)` producing 0 or 1 (type-system.md Section 4.2). This same idiom is used in the RETENTION desugaring (Section 6.3) and all funnel examples in Section 28.

### 6.2 FUNNEL With GROUP BY

```bql
-- Funnel conversion by day — use MATCH directly for GROUP BY control
events LAST 30d
| MATCH FIRST SEQUENCE(s: signup THEN p: purchase) WITHIN 7d EMIT ALL
| STATS
    entered = COUNT(*),
    converted = SUM(CAST(step_reached >= 2 AS INT))
  GROUP BY QUANTIZE(s.ts, 1d) AS day
```

FUNNEL sugar does not support GROUP BY directly — the desugared STATS clause is already fixed. For grouped funnels, write the MATCH + STATS explicitly. This is intentional: FUNNEL is a shortcut for the simple case, not a general-purpose operator.

### 6.3 RETENTION

```bql
-- Sugar:
events LAST 180d
| RETENTION(entry: signup, activity: purchase, brackets: [1d, 7d, 14d, 30d])

-- Desugars to:
events LAST 180d
| MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
| STATS retention_rate = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket
```

RETENTION is a named-argument form: `entry:` is the cohort-forming step, `activity:` is the retention-defining step, `brackets:` is the bracket list. Optional `cumulative: true` selects cumulative brackets.

**Limitation.** The RETENTION sugar is restricted to bare event types for `entry:` and `activity:` — property constraints (e.g., `entry: signup WHERE plan = 'pro'`), variable bindings, and WITHOUT exclusions are not expressible in the sugar. For any retention query that needs these features, write the equivalent MATCH + BRACKETS + STATS explicitly. The sugar is intentionally scoped to the common case so the grammar stays simple; the escape hatch is always the underlying primitives.

### 6.4 Retention Over Time

```bql
-- Retention by signup week — use MATCH directly for cohort grouping
events LAST 180d
| MATCH FIRST SEQUENCE(s: signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
| STATS retention_rate = AVG(CAST(step_reached >= 2 AS INT))
  GROUP BY QUANTIZE(s.ts, 7d) AS cohort_week, bracket
```

As with FUNNEL, RETENTION sugar does not support GROUP BY. For cohort-over-time retention, write the primitives explicitly.

### 6.5 Why Sugar and Not Operators

FUNNEL and RETENTION do not have their own output schemas — they inherit the schema of whatever the desugared primitives produce (type-system.md Section 6.2). The rationale:

1. **No special operator to implement.** The execution engine only needs to handle MATCH + STATS. FUNNEL and RETENTION add no code paths.
2. **Users see the primitives.** Errors point to the desugared form, teaching users how to write more general queries.
3. **One source of truth for semantics.** The correctness of FUNNEL follows from the correctness of MATCH and STATS, not from a separate proof.

The planner desugars FUNNEL and RETENTION during logical plan construction, before schema validation. Error messages can optionally reference the original source span for better user feedback.

---

## 7. STATS — Aggregation

STATS computes aggregate functions, optionally grouped:

```bql
-- Single aggregate
| STATS total = COUNT(*)

-- Multiple aggregates
| STATS
    total = COUNT(*),
    avg_amount = AVG(amount),
    p95_latency = P95(latency)

-- Grouped
| STATS
    event_count = COUNT(*),
    avg_amount = AVG(amount)
  GROUP BY device, plan

-- Grouped with time bucketing (computed group key requires AS)
| STATS event_count = COUNT(*) GROUP BY QUANTIZE(ts, 1d) AS day, device
```

Aggregate function signatures and type rules are defined in type-system.md Section 6.4.

### 7.1 Aggregate Expressions

```
agg_item := identifier "=" agg_expr
agg_expr := agg_func "(" (expr | "*") ")"
```

Aggregate functions: `COUNT`, `COUNT_DISTINCT`, `SUM`, `AVG`, `MIN`, `MAX`, `P50`, `P90`, `P95`, `P99`. Type restrictions are in type-system.md Section 6.4.

**Output column names are required.** Every aggregate expression must have an explicit `name = expr` assignment. `| STATS COUNT(*)` without a name is a parse error; write `| STATS total = COUNT(*)` instead. This rule exists because auto-generated names from expressions are ambiguous at scale (two different pipelines producing `count_star` but meaning different things in context) and because downstream operators (`| WHERE count > 100`) need a stable reference.

**Distinct counting.** Use `COUNT_DISTINCT(col)` to count distinct values. `COUNT(DISTINCT col)` is not supported — the `DISTINCT` keyword is reserved for `SELECT DISTINCT` (Section 10). Using `DISTINCT` inside an aggregate expression is a parse error. No other aggregate (`SUM`, `AVG`, `MIN`, `MAX`, percentiles) accepts `DISTINCT`; use a preceding `SELECT DISTINCT` if you need to pre-deduplicate.

### 7.2 GROUP BY

GROUP BY supports:

- **Bare column references**: `GROUP BY device`
- **Multiple bare columns**: `GROUP BY device, plan`
- **Computed expressions with a required name**: `GROUP BY QUANTIZE(ts, 1d) AS day`
- **Mixed**: `GROUP BY device, QUANTIZE(ts, 1d) AS day`
- **Variable binding columns** (after MATCH): `GROUP BY plan`

Bare references keep the source column's name in the output. Computed expressions **must** have an explicit `AS name` suffix — the same rationale as aggregate naming (Section 7.1). `GROUP BY QUANTIZE(ts, 1d)` without `AS day` is a parse error.

Group-by columns retain their input type and nullability. Group keys with NULL values are grouped together (SQL semantics); use `WHERE <col> IS NOT NULL` before STATS to exclude them.

**Groupable types.** All scalar types (`Bool`, `Int`, `Float`, `String`, `Timestamp`) are groupable — grouping uses value equality. `List` and `Map` values are also groupable by structural equality: two `List(T)` values are equal if they contain the same elements in the same order; two `Map` values are equal if they have the same key-value pairs. Note that `List`/`Map` are not *orderable* (type-system.md Section 6.12) — they cannot appear in ORDER BY — but equality-based grouping is well-defined.

### 7.3 No HAVING

There is no HAVING keyword. `WHERE` after STATS is unambiguous in a pipeline and sufficient:

```bql
events | STATS count = COUNT(*) GROUP BY device | WHERE count > 100
```

In SQL, HAVING exists because WHERE semantically precedes GROUP BY — a separate keyword is needed for post-aggregation filtering. In BQL, the pipe order IS the execution order: `| WHERE` after `| STATS` is post-aggregation filtering, unambiguously.

---

## 8. SESSIONIZE

SESSIONIZE groups events into sessions based on inactivity gaps:

```bql
-- Gap-based sessions
| SESSIONIZE(gap: 30m)

-- With explicit end event
| SESSIONIZE(gap: 30m, end: logout)
```

**Parameters:**
- `gap: <duration>` — maximum inactivity between events in a session. Required.
- `end: <event_type>` — explicit session-terminating event. Optional. When set, sessions end at the first occurrence of the event type even if the gap has not elapsed.

Output: passes through all input columns and adds `session_id` (Int) and `session_duration` (Int, nanos). See type-system.md Section 6.3.

### 8.1 Sessions as MATCH Context

```bql
-- Match within sessions
| SESSIONIZE(gap: 30m)
| MATCH FIRST SEQUENCE(search THEN click) WITHIN SESSION
```

`WITHIN SESSION` constrains a downstream MATCH to session boundaries rather than a fixed duration. The MATCH operator detects `session_id` in its input schema and treats session boundary changes as candidate expiry events, effectively scoping all matches to within a single session.

**Implementation sketch:** SESSIONIZE adds a `session_id` column. MATCH with `WITHIN SESSION` uses a virtual window that ends when `session_id` increments — on each event, if `session_id != current_session_id`, all active candidates in the NFA for that binding track are expired. This is simpler than threading session annotations through the match state.

`WITHIN SESSION` is a distinct form of the `WITHIN` modifier and cannot be combined with a duration argument or with `BRACKETS`. The three forms (`WITHIN <duration>`, `WITHIN SESSION`, `BRACKETS [...]`) are mutually exclusive — a MATCH expression may use at most one.

---

## 9. WHERE — Filtering

WHERE works at multiple positions in the pipeline:

```bql
-- Before MATCH: filters the event stream
events | WHERE event_type IN ('signup', 'purchase') | MATCH ...

-- After MATCH: filters match results
events | MATCH ... | WHERE step_reached >= 2

-- After STATS: filters aggregated rows (replaces HAVING)
events | STATS ... GROUP BY device | WHERE count > 100
```

The predicate is an arbitrary boolean expression. Type rules: the predicate must evaluate to `Bool`. NULL predicate results are treated as FALSE for filtering (type-system.md Section 3.3). Output schema is unchanged.

**What the predicate can reference depends on WHERE's position.** Before MATCH/STATS, the predicate sees the event stream's columns (table columns plus system columns like `__seq_id`). After MATCH, it sees the MATCH output schema (`entity_id`, `$var` columns, `step_reached`, etc. — Section 5.1). After STATS, it sees the STATS output schema — aggregate result columns referenced by the `alias =` name assigned in the STATS clause (e.g., `WHERE count > 100` in the example above references the aggregate named `count` in the preceding STATS). This replaces SQL's HAVING.

### 9.1 Predicate Operators

```bql
-- Comparison
WHERE amount > 100
WHERE plan = 'pro'
WHERE ts > '2025-01-01'

-- Boolean logic
WHERE (event_type = 'purchase' OR event_type = 'refund') AND amount > 0

-- Null handling
WHERE amount IS NOT NULL
WHERE amount IS NULL

-- Set membership
WHERE event_type IN ('signup', 'purchase', 'checkout')
WHERE event_type NOT IN ('heartbeat', 'ping')

-- Range
WHERE amount BETWEEN 100 AND 500

-- Regex
WHERE url ~= 'checkout/step[0-9]+'

-- LIKE (SQL-style, % and _ wildcards)
WHERE name LIKE 'John%'

-- Substring check
WHERE tag CONTAINS 'premium'
```

### 9.2 Operator Precedence

Standard SQL precedence (highest to lowest):

1. Unary `-`
2. `*`, `/`, `%`
3. `+`, `-`
4. Comparisons: `=`, `!=`, `<`, `>`, `<=`, `>=`
5. `IS NULL`, `IS NOT NULL`, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `LIKE`, `NOT LIKE`, `~=`, `CONTAINS`
6. Unary `NOT`
7. `AND`
8. `OR`

This matches SQL's convention: `NOT` is a boolean operator applied after comparisons, so `NOT x = 5` parses as `NOT (x = 5)`, not `(NOT x) = 5`. The `NOT` inside `NOT IN` / `NOT BETWEEN` / `NOT LIKE` is part of a single compound operator and is distinct from the general unary `NOT` at level 6.

Parentheses override precedence. The parser does not accept mixed implicit `AND`/`OR` without parentheses when intent is ambiguous — e.g., `A OR B AND C` is parsed as `A OR (B AND C)` per standard precedence, but the linter may recommend explicit parentheses.

---

## 10. SELECT — Projection

```bql
-- Specific columns
| SELECT entity_id, amount, device

-- With expressions
| SELECT entity_id, amount * 1.1 AS adjusted_amount

-- All columns plus computed
| SELECT *, amount * 1.1 AS adjusted_amount

-- Explicit system column selection
| SELECT __seq_id, amount

-- Distinct
| SELECT DISTINCT user_id, device
```

SELECT rewrites the output schema. Each item is either a column reference (bare or qualified), a `*` wildcard, or a computed expression with a **required** `AS` alias. Writing `SELECT amount * 1.1` without `AS adjusted_amount` is a parse error — computed expressions never auto-generate names. This rule keeps downstream column references stable and explicit; there is no normalization scheme to memorize.

`SELECT *` preserves all non-system input columns. The implicit system columns `__seq_id` and `__batch_id` (type-system.md Section 5.1) are excluded from wildcard expansion but may be selected explicitly. `SELECT *, <expr> AS <name>` adds a computed column alongside the existing non-system columns (equivalent to LET for a single column).

**No column-name collisions.** Within a single SELECT's output schema, every column name must be unique. `SELECT *, amount * 1.1 AS amount` where the input already has a column named `amount` is a planner error — use a different alias. The same rule applies to LET (Section 11).

`SELECT DISTINCT` deduplicates the projected rows. It is a post-projection operator — distinctness is computed on the output columns, not the input columns.

---

## 11. LET — Computed Columns

LET adds a computed column without a full SELECT. Syntactic sugar for `SELECT *, expr AS name`:

```bql
events
| LET time_of_day = QUANTIZE(ts, 1h)
| STATS event_count = COUNT(*) GROUP BY time_of_day

events
| LET price_bucket = QUANTIZE(amount, 100)
| STATS event_count = COUNT(*) GROUP BY price_bucket
```

Multiple LET clauses can be chained:

```bql
events
| LET day = QUANTIZE(ts, 1d)
| LET morning = CASE WHEN QUANTIZE(ts, 1h) < day + 12h THEN TRUE ELSE FALSE END
| STATS event_count = COUNT(*) GROUP BY morning
```

LET is preferred over SELECT when adding a single computed column without dropping others — it makes the intent explicit and keeps the pipeline shorter.

**No rebinding.** The new column name must not collide with an existing column in the input schema. `| LET amount = amount * 1.1` is a planner error even though it's superficially a legal LET form — the input already has a column named `amount`. Pick a different name (`adjusted_amount`, `amount_v2`, etc.). This matches the collision rule in SELECT (Section 10) and keeps column references unambiguous.

Because LET rewrites through `SELECT *`, it inherits the same wildcard rule: implicit system columns are not projected unless named explicitly.

---

## 12. CASE Expressions

Conditional logic usable in SELECT, LET, STATS, and WHERE:

```bql
events
| LET tier = CASE
    WHEN amount > 1000 THEN 'high'
    WHEN amount > 100 THEN 'medium'
    ELSE 'low'
  END
| STATS event_count = COUNT(*) GROUP BY tier
```

CASE follows SQL semantics:

- Evaluated top to bottom, first matching WHEN wins.
- ELSE is optional — returns NULL if no WHEN matches and no ELSE is given.
- All THEN/ELSE expressions must be type-compatible (same type or implicitly coercible to a common type). The result type is the common type (type-system.md Section 4.1).
- WHEN predicates must produce `Bool`.

CASE is an expression, not a statement. It can appear wherever an expression is expected, including inside aggregate functions:

```bql
| STATS high_value_count = COUNT(CASE WHEN amount > 1000 THEN 1 END)
```

---

## 13. Window Functions (OVER)

Window functions compute values across the entity's ordered event stream without collapsing rows:

```bql
-- Time since previous event
| SELECT *, LAG(ts, 1) OVER (ORDER BY ts) AS prev_ts

-- Running count
| SELECT *, ROW_NUMBER() OVER (ORDER BY ts) AS event_num

-- Running sum partitioned by group
| SELECT *, SUM(amount) OVER (PARTITION BY category ORDER BY ts) AS running_total
```

`OVER` supports `ORDER BY` (defaults to the timestamp column) and `PARTITION BY`. Within BQL pipelines, the entity key is always an implicit partition — window functions operate per-entity regardless of whether PARTITION BY is written. Additional PARTITION BY columns subdivide the entity stream further.

See type-system.md Section 6.8 for the full function list and output types.

---

## 14. Entity Operators

These operators run per-entity and are specialized enough to get their own syntax.

### 14.1 FIRST / LAST / NTH

Per-entity event selection:

```bql
events | FIRST(purchase)               -- first purchase per entity
events | LAST(page_view)               -- last page view per entity
events | NTH(page_view, 3)             -- third page view per entity
```

Output: one row per entity with the selected event's full columns. Entities with no matching event are omitted. See type-system.md Section 6.7.

The event argument is an event type identifier. A WHERE clause may be attached to filter which events are candidates for selection:

```bql
events | FIRST(purchase WHERE amount > 100)            -- first high-value purchase per entity
events | LAST(page_view WHERE url LIKE '/checkout%')
events | NTH(purchase WHERE amount > 100, 3)           -- third high-value purchase per entity
```

All three operators (FIRST, LAST, NTH) accept an optional WHERE clause. The predicate is applied per-event before the position selection, so `NTH(e WHERE p, 3)` returns the third event that satisfies `p`, not the third event overall if it happens to satisfy `p`.

### 14.2 SAMPLE

Random sampling of entities:

```bql
events | SAMPLE(fraction: 0.1)         -- 10% random sample of entities
events | SAMPLE(count: 10000)          -- fixed sample size
```

**Parameters:** exactly one of `fraction:` (Float in [0, 1]) or `count:` (Int).

Output: passes through input schema unchanged. SAMPLE is a scan-level operator that filters entities early — it's pushed down to the storage layer to avoid reading segments for non-sampled entities (type-system.md Section 6.11).

Sampling is entity-level, not event-level. A sampled entity's full event stream is included; non-sampled entities contribute zero events.

**Determinism.** SAMPLE uses a hash of the entity ID, making results deterministic across runs with the same seed. An optional `seed: <int>` parameter fixes the seed for reproducibility; without it, the seed is derived from the database identity so repeat queries on the same database produce the same sample.

### 14.3 ATTRIBUTE

Find touchpoint events preceding each conversion event within a time window and emit one row per `(entity, conversion, touchpoint)` triple:

```bql
events | ATTRIBUTE(
    conversion: purchase,
    touchpoints: ad_click,
    window: 30d,
    touchpoint_key: channel
)
```

**Parameters:**
- `conversion: <event_type>` — the conversion-defining event. Required.
- `touchpoints: <event_type>` — the touchpoint event type whose occurrences are credited to conversions. Required.
- `window: <duration>` — the lookback window before each conversion in which touchpoints count. Required.
- `touchpoint_key: <expr>` — an expression evaluated against the touchpoint event's schema that produces a `String`. The result appears as the `touchpoint_key` output column. Required. Use `CAST(… AS STRING)` if the source column isn't already a string. The expression cannot reference conversion properties — it is evaluated purely in the touchpoint's context.

**Output schema.** One row per `(entity_id, conversion, matched-touchpoint)`. See type-system.md Section 6.14.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | String or Int | no | Entity |
| `conversion_ts` | Timestamp | no | Conversion event's timestamp |
| *conversion properties* | (resolved from source) | follows source | Accessed as `<conversion_event_type>.<column>` downstream; demand-driven |
| `touchpoint_ts` | Timestamp | yes | Timestamp of the matched touchpoint. NULL when no touchpoint qualified. |
| `touchpoint_key` | String | yes | Result of the `touchpoint_key` expression. NULL when no touchpoint qualified. |

**Auto-unnest semantics.** ATTRIBUTE emits flat rows, not a list column. A conversion with N qualifying touchpoints produces N rows. This makes attribution aggregation straightforward: `STATS attributions = COUNT(*) GROUP BY touchpoint_key` directly gives you per-channel counts without an intermediate list materialization step.

**Un-attributed conversions (LEFT-UNNEST).** A conversion with zero qualifying touchpoints still emits **one row**, with `touchpoint_ts = NULL` and `touchpoint_key = NULL`. This preserves un-attributed conversions so the user can count them (`STATS unattributed = SUM(CAST(touchpoint_ts IS NULL AS INT))`). For INNER-join semantics — drop un-attributed conversions entirely — append `| WHERE touchpoint_ts IS NOT NULL`.

**Conversion property access.** Forwarded conversion properties are accessed downstream with the conversion event type as a prefix, parallel to MATCH's bare-step property access (Section 5.2). For `conversion: purchase`, downstream writes `purchase.amount`. Conversion property forwarding is demand-driven: only referenced properties are retained. If the conversion event type shares its name with a column on the source table, the planner raises `TypeError::NameCollision`.

```bql
-- Last-touch attribution by channel
events LAST 60d
| ATTRIBUTE(
    conversion: purchase,
    touchpoints: ad_click,
    window: 30d,
    touchpoint_key: channel
)
| LET rn = ROW_NUMBER() OVER (PARTITION BY entity_id, conversion_ts ORDER BY touchpoint_ts DESC)
| WHERE rn = 1
| STATS revenue = SUM(purchase.amount) GROUP BY touchpoint_key
```

```bql
-- Equal-weight attribution: every touchpoint gets one count
events LAST 60d
| ATTRIBUTE(
    conversion: purchase,
    touchpoints: ad_click,
    window: 30d,
    touchpoint_key: channel
)
| WHERE touchpoint_ts IS NOT NULL
| STATS attributions = COUNT(*) GROUP BY touchpoint_key
```

```bql
-- Computed key: channel + campaign concatenated
events LAST 60d
| ATTRIBUTE(
    conversion: purchase,
    touchpoints: ad_click,
    window: 30d,
    touchpoint_key: CONCAT(channel, ':', campaign)
)
| WHERE touchpoint_ts IS NOT NULL
| STATS revenue = SUM(purchase.amount) GROUP BY touchpoint_key
```

**Why one key column, not a list.** Earlier designs of ATTRIBUTE emitted a `List(Struct)` of touchpoints per conversion, which required a companion `UNNEST` operator to flatten for analysis. The list form also hit a dead end in BQL's type system: BQL has no `Struct` type, and `Map(V)` has a single value type, so a list of heterogeneously-typed touchpoint fields has no natural representation. Auto-unnesting sidesteps both problems: flat rows compose naturally with STATS and window functions, multi-column attribution keys are handled via the `touchpoint_key` expression (CONCAT, CASE, etc.), and the language needs no Struct type or UNNEST operator. Credit distribution policies (first-touch, last-touch, equal, time-decay, position-based) are all expressible with window functions and standard aggregates on the flat row form.

**Touchpoint consumption.** A touchpoint can contribute to multiple conversions — there is no consumption. Consumption is a potential v2 modifier.

**Execution model.** Entity-streaming operator: maintain a sliding-window deque of qualifying touchpoints per entity. When a conversion event arrives, drop touchpoints older than `conversion_ts - window`, then emit one row per remaining touchpoint. If the deque is empty after pruning, emit one row with NULL touchpoint fields.

---

## 15. Ordering and Limiting

```bql
| ORDER BY amount DESC
| ORDER BY device ASC, amount DESC
| LIMIT 100
| ORDER BY count DESC | LIMIT 10      -- top-N pattern

-- SORT is a single-keyword alias for ORDER BY:
| SORT amount DESC
| SORT device ASC, amount DESC
```

`SORT` is a convenience alias for `ORDER BY`. Both forms accept the same item list and produce identical AST nodes (`PipelineStage::OrderBy`). The planner sees no distinction between them. `SORT` is slightly more concise for interactive use; `ORDER BY` is the canonical SQL-compatible spelling.

ORDER BY / SORT requires orderable types (all scalar types). List and Map are not orderable (type-system.md Section 6.12). NULL ordering follows the convention: NULLs sort **last** in ASC, **first** in DESC. This is the same convention Oracle, DuckDB, and BigQuery use by default (Postgres defaults differ — NULLS LAST regardless of direction — so queries ported from Postgres that depend on NULL position should be reviewed).

The direction keyword (`ASC` or `DESC`) is optional on each item; the default is **ascending** when omitted.

LIMIT takes a non-negative integer literal. There is no OFFSET in v1 — pagination over analytical queries is typically a client concern, and large OFFSETs on sorted data defeat the sort optimization.

**LIMIT is not terminal.** Operators may follow LIMIT — the LIMIT output is still a row stream with a well-defined schema, and downstream operations like `| SELECT col1, col2` or `| WHERE col > 0` work normally. This is useful for composing top-N subresults into larger pipelines (e.g., top 100 by count, then filter or project). The composition table in Section 25.2 lists the valid downstreams.

---

## 16. PIVOT

Reshapes long-form results into wide-form by turning values of a pivot column into separate output columns:

```bql
-- Wide-form retention table
events LAST 90d
| MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
| STATS retention = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket
| PIVOT bracket ON retention
```

PIVOT has two arguments: the pivot column (whose values become new column names) and the value column (whose values fill the new columns). The set of distinct pivot values must be known at plan time — either from a literal list in the PIVOT clause (future feature) or inferred from the upstream operator (e.g., BRACKETS produces a fixed bracket count).

See type-system.md Section 6.10 for schema rules.

---

## 17. IN — Set Membership

`IN` has three distinct forms on its right-hand side:

### 17.1 Inline Literal List

```bql
| WHERE event_type IN ('signup', 'purchase', 'checkout')
| WHERE amount IN (100, 200, 500)
```

A parenthesized comma-separated list of literals. Used for "column is one of these fixed values." The list elements must be type-compatible with the left-hand side.

### 17.2 Inline Subquery With `IN QUERY`

```bql
events | WHERE entity_id IN QUERY (
    events | WHERE event_type = 'premium_signup' | SELECT entity_id
)
```

The `QUERY` keyword is **required** before the parenthesized pipeline. It disambiguates subqueries from literal lists — without it, `IN (events)` could mean either "the literal list with one element" or "the pipeline reading from the `events` table." The keyword is deliberately short and SQL-adjacent (most engines use `IN (SELECT ...)` which works because the subquery starts with `SELECT`; bqlite pipelines can start with any source, so we need an explicit marker).

The subquery must produce exactly one column (or a tuple of columns for multi-column IN) whose type matches the left-hand side.

### 17.3 Alias Reference

```bql
premium_users = events | WHERE event_type = 'premium_signup' | SELECT entity_id
events | WHERE entity_id IN premium_users
```

A bare identifier on the right of `IN` is **always** an alias reference, never a column reference. Column-valued set membership must use one of the other two forms. See Section 18 for alias semantics.

### 17.4 Multi-Column IN

```bql
events | WHERE (user_id, day) IN QUERY (
    events | STATS first_active = MIN(ts) GROUP BY user_id, QUANTIZE(ts, 1d) AS day
           | SELECT user_id, day
)
```

Multi-column IN uses a parenthesized tuple on the left. The tuple arity and column types must match the subquery output.

Type rules: see type-system.md Section 6.9.

---

## 18. Aliases

Session-scoped named query results:

```bql
-- Define
churned = events LAST 90d | WHERE event_type = 'churn' | SELECT entity_id

-- Use in multiple places (branching)
events | WHERE entity_id IN churned | STATS churned_count = COUNT(*) GROUP BY plan
events | WHERE entity_id IN churned | MATCH FIRST SEQUENCE(support_ticket THEN churn) WITHIN 7d
```

### 18.1 Alias Semantics

- **Session-scoped.** Aliases live for the duration of a REPL session or a multi-statement query submission. They do not persist across sessions.
- **Lazy evaluation.** An alias is not executed when defined. It is executed when referenced. Multiple references in a single session may be evaluated once and cached, or re-evaluated per reference, at the planner's discretion (TASK-006 cost model).
- **Composable.** Aliases may reference other aliases. The planner resolves the dependency graph.
- **No cycles.** Circular references are a planner error.
- **Lexically scoped naming.** An alias name must be a valid identifier and must not shadow a keyword or table name. Shadowing other aliases is permitted — the most recent definition wins.

### 18.2 Alias Resolution in IN

Per Section 17.3, a bare identifier on the right side of `IN` is **always** an alias reference. The parser resolves the identifier against the set of aliases defined in the current query submission — if the name is not a defined alias, the planner rejects the query. There is no fallback to column lookup. To test whether a scalar column's value is in a set of literal values, use `IN (lit1, lit2, ...)`. To test against a dynamic set computed by a query, use `IN QUERY (...)`.

### 18.3 Aliases vs CTEs

Aliases play the role CTEs play in SQL, but they are top-level definitions rather than embedded `WITH` clauses. This keeps the pipeline the main visual element of a query and makes alias reuse across multiple queries (in a REPL session) natural.

Persistent aliases — named views, materialized results — are a v2 feature and explicitly out of scope for v1.

### 18.4 Alias Top-Level Structure

A BQL script is a sequence of zero or more alias definitions followed by a terminal pipeline. The formal production `query := (alias_def)* pipeline` is in Section 26. The terminal pipeline is what executes and produces output; alias definitions on their own do nothing — they are inert until referenced.

---

## 19. Cross-Table Entity Joins

Only entity-aligned joins are supported in v1. Multiple tables are joined with the explicit `JOIN` keyword in the source expression. The join is a streaming merge on pre-sorted entity-ordered data within each shard — cheap because bqlite pre-shards every table using the same shard function (storage-format.md: database-level shard count fixed at initialization).

```bql
-- Explicit JOIN in the source expression
events LAST 30d JOIN purchases
| MATCH FIRST SEQUENCE(
    events.signup THEN purchases.purchase WHERE purchases.amount > 100
  ) WITHIN 7d
```

Multiple tables can be joined by repeating JOIN:

```bql
events JOIN purchases JOIN support_tickets
| ...
```

The time range (`LAST 30d`, `BETWEEN ...`) applies to the first table; other tables in the join are implicitly time-bounded by the same range plus the planner's scan-extension rule (Section 3.2).

### 19.1 Table-Qualified References Are Required Inside JOINs

Inside a joined query, **every event type and every column reference must be prefixed with the source table**: `events.signup`, `purchases.amount`, `support_tickets.priority`. The dot notation disambiguates references even when they are unique to a single table.

```bql
events JOIN purchases
| MATCH FIRST SEQUENCE(
    events.signup WHERE events.plan = 'pro'
    THEN purchases.purchase WHERE purchases.amount > 100
  ) WITHIN 7d
```

In single-table queries (no JOIN), the table prefix is never written — bare event types and column names resolve against the single source table. In multi-table queries (with JOIN), bare references are **planner errors**, even when they would be unambiguous by schema lookup. This rule keeps queries self-documenting: readers see which table each reference comes from without tracing schema lookups, and adding a new table to a JOIN cannot silently rebind references in a downstream operator.

**Inside a MATCH step**, the step-name prefix (`s:`) is written before the table-qualified event: `s: events.signup`. Both prefixes are optional individually but have fixed order when both are present.

### 19.2 No Self-Joins

A table cannot appear more than once in a source expression. `events JOIN events` is a planner error. Self-joins would require table aliases to disambiguate `events.signup` (which events?), and v1 does not introduce table aliases for the source expression. If you need multiple independent views of the same table (e.g., to find pairs of events within one entity's stream), use variable bindings inside a single MATCH or use aliases.

### 19.3 FUNNEL and RETENTION Inside JOINs

FUNNEL and RETENTION sugar **both accept table-qualified event references** in multi-table queries. The grammar's `retention_args` uses `event_ref` (which allows the `table.event` form), and `funnel_op` takes a full `step_list` so step events already support qualifiers.

```bql
-- Retention across tables
events LAST 180d JOIN purchases
| RETENTION(
    entry: events.signup,
    activity: purchases.purchase,
    brackets: [1d, 7d, 14d, 30d]
  )
```

The sugar desugars into the same MATCH/STATS forms as single-table use, with the qualifiers preserved through the rewrite.

### 19.4 Why Only Entity-Aligned Joins

General joins (joins between tables where entities don't align one-to-one) are out of scope for v1. Behavioral analytics rarely needs them — the entity event stream is the unit of analysis, and having multiple "event tables" per entity (e.g., separate tables for web events and mobile events, or events and purchases) is the dominant use case for joins.

General joins would require substantially more planner and execution machinery (hash joins, merge joins across different sort orders) and are deferred until v2.

### 19.5 Schema Requirements

For tables `A` and `B` to be joinable:

1. Both must have the same entity-key **type** (`String` or `Int`). The entity-key column **names** can differ — e.g., `events.user_id STRING ENTITY KEY` and `purchases.customer_id STRING ENTITY KEY` are joinable, and the engine joins on entity-key value regardless of column name.
2. Both must have been created in the same database. Cross-database joins are out of scope.
3. Both must use the same shard function and count. Because shard count is a database-level property set at initialization (storage-format.md Section 5.1), this is guaranteed by construction — all tables in a database share the same sharding.
4. Each table's own event-time column (declared with the `EVENT TIME` role) governs ordering of events from that table. When events from different tables are merged into a single per-entity stream, each event carries its own timestamp. Time window semantics (WITHIN, BRACKETS) treat these timestamps uniformly — the window anchors to the first matched step's timestamp regardless of which table it came from.
5. Event type columns are table-local — events are distinguished by `(table, event_type)`.
6. **Neither table can appear more than once in a single source expression** (Section 19.2).

**Output schema of joined MATCH.** MATCH output still has a single `entity_id` column (Section 5.1) — its value is the shared entity-key value that both tables' entity-key columns contained. The column is named `entity_id` regardless of whether the underlying tables called it `user_id`, `customer_id`, or anything else.

---

## 20. Data Manipulation

### 20.1 INSERT

```bql
-- Insert literal values (for small tests and REPL work)
INSERT INTO events VALUES
    ('user_1', '2025-01-15T10:00:00Z', 'signup', NULL, 'mobile'),
    ('user_1', '2025-01-15T10:05:00Z', 'page_view', NULL, 'mobile')

-- Insert from file
INSERT INTO events FROM 'data.parquet'
INSERT INTO events FROM 'data.csv' WITH (delimiter: ',', header: true)
INSERT INTO events FROM 'events.jsonl' WITH (format: 'jsonl')

-- Insert from file with column remapping: rename `uid` to `user_id`,
-- `event_ts` to `ts`, `evt` to `event_type`; other source columns
-- pass through by name match. The map clause requires **bare**
-- identifiers on both sides — reserved keywords like `time`, `type`,
-- or `event` are rejected at parse time, so source or target columns
-- with such names need to be renamed upstream (or targeted via the
-- passthrough rule against differently-named catalog columns).
INSERT INTO events
FROM 'data.csv'
WITH (format: 'csv', map: (uid AS user_id, event_ts AS ts, evt AS event_type))
```

Literal INSERT takes positional tuples matching the table's column order. Column lists for named inserts are out of scope for v1 — they can be added later without breaking existing queries.

INSERT targets only user-declared columns. The implicit system columns `__seq_id` and `__batch_id` are assigned by the storage layer and are not valid insert targets.

File INSERT reads from a path (local filesystem in v1) and ingests rows into the target table. Supported formats: Parquet (default), CSV, JSONL. Format is inferred from the file extension or explicit `WITH (format: ...)`. Each file ingest is a single batch; see storage-format.md Section 6 for the ingest pipeline details.

**`WITH (...)` options.** All options use the colon-separated `key: value` form (§26). The recognized keys for `INSERT ... FROM` are:

| Key | Value shape | Meaning |
|---|---|---|
| `format` | string literal (`'csv'`, `'jsonl'`, `'parquet'`) | Explicit format override. Defaults to the file extension. |
| `delimiter` | single-character string literal | CSV field separator. CSV format only. |
| `header` | boolean literal | Whether the first CSV row is a header. CSV format only. Defaults to `true`. |
| `map` | parenthesized `(src AS dst, ...)` list | Column rename clause, see below. |

Options may appear in any order and any combination. Unknown keys are a plan-time error.

**The `map` clause.** `map: (src AS dst, ...)` renames columns from the source file to target columns in the destination table. Each entry is a pair of bare identifiers separated by the keyword `AS`; the left-hand side is the source column name as it appears in the input file, and the right-hand side is the target table column. Source columns *not* named in the `map` list default to passthrough when the source name already matches a target table column name; source columns that are neither mapped nor name-matched cause a plan-time error. A `map` list with duplicate target names is a plan-time error. The parentheses are mandatory, and an empty parenthesized list is a parse error.

The `map` clause is the only `WITH` option whose right-hand side is not a literal — the grammar's other keys take strings, numbers, or booleans, but `map` takes a list of identifier pairs. This is a deliberate choice: the syntactic shape `src AS dst` mirrors SELECT aliasing (§8.4), and making `map` a structured clause avoids string-parsing identifiers out of a literal at plan time. The AST carries it in a dedicated field on `InsertBody::From` rather than as an entry in the flat option list — see the AST-shape note at the bottom of this subsection.

**AST-shape note (informs TASK-222 / TASK-233).** The `InsertBody::From` AST variant in `bqlite-ast` carries three fields: `path: String`, `options: Vec<InsertOption>`, and `map: Option<Vec<ColumnMapping>>`. The `map` field is out of band from the flat option list because `InsertOption { key: Name, value: Literal }` cannot hold a list of identifier pairs without widening its `value` type — and widening would either (a) force every consumer to pattern-match on an option-value enum even when they only care about literal keys, or (b) bury the mapping list inside a string and force the parser to re-parse it at plan time. The structured field is cheaper: the parser emits `ColumnMapping { source, target, span }` values directly, consumers see a single `Option<Vec<ColumnMapping>>` to walk, and the type system enforces that `map`'s value shape is distinct from every other option's. The alternative considered was an `InsertOptionValue` enum with `Literal` and `ColumnMappings(Vec<ColumnMapping>)` variants; it is recorded here for posterity but not chosen. `None` means the clause was absent; the parser never produces `Some(vec![])` (grammar requires at least one entry inside the parentheses), and downstream code may assume the list is non-empty whenever it is `Some`.

### 20.2 DELETE

```bql
-- Delete by entity
DELETE FROM events WHERE user_id = 'user_123'

-- Delete by time range
DELETE FROM events WHERE ts < '2024-01-01'

-- Delete specific events
DELETE FROM events WHERE __seq_id IN (123, 456, 789)

-- Delete a bad ingest batch
DELETE FROM events WHERE __batch_id = 42
```

DELETE supports a WHERE clause over indexed columns (entity key, timestamp, `__seq_id`, `__batch_id`). Arbitrary-predicate DELETE is supported but may require a full scan to materialize the row set for tombstoning; the planner warns when this happens.

Deletes are implemented as tombstones at the storage layer (storage-format.md on tombstones). They are visible immediately to new queries but do not rewrite segments until compaction.

### 20.3 No UPDATE

There is no UPDATE statement. Events are immutable facts; mutating historical data is inconsistent with the analytics model. If data needs correction, the appropriate pattern is: DELETE the bad rows and INSERT the corrected ones in a new batch.

### 20.4 Schema DDL

```bql
CREATE TABLE events (
    user_id STRING ENTITY KEY,
    ts TIMESTAMP EVENT TIME,
    event_type STRING EVENT TYPE,
    amount FLOAT,
    device STRING
)

ALTER TABLE events ADD COLUMN category STRING
ALTER TABLE events ADD COLUMN score FLOAT NOT NULL DEFAULT 0.0

DROP TABLE events_tmp

DESCRIBE events
```

Full schema DDL is specified in type-system.md Section 9. This document only lists the statements supported at the parser level. DROP TABLE and DESCRIBE are added here (not covered in type-system.md).

### 20.5 DESCRIBE

```bql
DESCRIBE events
```

Output columns: `name`, `type`, `nullable`, `role`. Role values are:

| Role | Meaning |
|---|---|
| `entity_key` | The declared `ENTITY KEY` column |
| `event_time` | The declared `EVENT TIME` column |
| `event_type` | The declared `EVENT TYPE` column |
| `property` | A user-declared property column |
| `system` | An implicit system column (`__seq_id`, `__batch_id` — type-system.md Section 5.1) |

DESCRIBE always lists the system columns alongside user-declared columns so users can discover them for use in DELETE (Section 20.2) or explicit SELECT. DESCRIBE does not read data — it reads table metadata only.

### 20.6 EXPLAIN (Plan Output)

```bql
EXPLAIN <pipeline>
```

Shows the logical plan, optimized plan, and physical plan for a query without executing it. EXPLAIN applies to **pipelines only** — the `explain_stmt := EXPLAIN pipeline` production does not accept DDL or DML. To preview what an INSERT or DELETE would do, run the equivalent `WHERE` first.

The exact output format is deferred to the CLI/REPL implementation (Wave 6); the planner emits structured plan data that the REPL formats. See Section 30.7.

---

## 21. Expression Language

BQL expressions compose arithmetic, comparisons, boolean logic, CASE, CAST, and scalar function calls. This section covers the syntax and lists the common forms; the full scalar function catalog — temporal (QUANTIZE, EPOCH_NANOS, EPOCH_SECONDS), string (LENGTH, SUBSTRING, CONCAT, LOWER, UPPER, TRIM), numeric (ABS, FLOOR, CEIL, ROUND, EXP, LN, POW), and null-handling (COALESCE, IF) — is specified in **type-system.md Section 10.2**. Every function listed there is available in any BQL expression.

### 21.1 Arithmetic

```bql
amount * 1.1
ts - prev_ts                    -- produces Int (nanos)
ts + 7d                         -- Timestamp + Int duration
price / quantity
count % 10
```

See type-system.md Section 4.4 for arithmetic type rules, including the accepted ambiguity in `Timestamp + Int`.

### 21.2 Comparisons

```bql
amount > 100
plan = 'pro'
amount != 0
amount >= 100 AND amount <= 500
amount BETWEEN 100 AND 500      -- equivalent to above
```

### 21.3 Boolean Logic

```bql
(A OR B) AND C
NOT deleted
event_type IN ('a', 'b', 'c')
event_type NOT IN ('x', 'y')
```

### 21.4 Null Handling

```bql
amount IS NULL
amount IS NOT NULL
COALESCE(amount, 0.0)
IF(amount IS NULL, 0.0, amount)
```

`IF` and `COALESCE` signatures are in type-system.md Section 10.2.

### 21.5 String Operations

```bql
url ~= 'checkout/step[0-9]+'       -- regex match (left operand must be String)
name LIKE 'John%'                   -- SQL LIKE with % and _ wildcards
tag CONTAINS 'premium'              -- substring check
CONCAT(first_name, ' ', last_name)
LOWER(email)
LENGTH(name)
SUBSTRING(description, 1, 20)
```

Regex syntax is the RE2 dialect (no backreferences, no lookaround — these are intrinsically excluded from RE2). This prevents pathological regex runtime — bqlite should not accept untrusted input that can cause exponential matching. Compilation happens at plan time.

### 21.6 CASE

```bql
CASE
    WHEN amount > 1000 THEN 'high'
    WHEN amount > 100 THEN 'medium'
    ELSE 'low'
END
```

See Section 12.

### 21.7 CAST

```bql
CAST(amount AS INT)
CAST(step_reached >= 2 AS INT)     -- Bool to Int for AVG
CAST(ts AS STRING)                  -- RFC 3339 / ISO-8601 UTC, e.g. 2023-11-14T22:13:20.123456789Z
CAST('2023-11-14T22:13:20Z' AS TIMESTAMP)  -- RFC 3339, non-UTC offsets accepted and normalized to UTC
```

Type-system.md Section 4.2 defines the full cast table and failure semantics (failed casts produce NULL, not errors).

### 21.8 QUANTIZE

Bucket a value by a given width:

```bql
QUANTIZE(ts, 1d)                                -- UTC day buckets
QUANTIZE(ts, 1h, 'America/New_York')            -- timezone-aware hour buckets
QUANTIZE(amount, 100)                            -- numeric buckets: 0-99, 100-199, etc.
```

QUANTIZE is overloaded: `(Timestamp, Int)`, `(Timestamp, Int, String)` for timezone, and `(Int|Float, Int|Float)` for numeric bucketing. Signatures in type-system.md Section 10.2.

Timezone-aware bucketing aligns buckets to local midnight/boundaries in the given timezone but returns UTC timestamps. The timezone string is validated at plan time — invalid IANA names produce a planner error.

---

## 22. Duration Literals

Single-unit durations, parsed to i64 nanoseconds at plan time:

```bql
7d          -- days
12h         -- hours
30m         -- minutes
10s         -- seconds
500ms       -- milliseconds
100us       -- microseconds
50ns        -- nanoseconds
```

Duration literals are a lexical token — `7d` is one token, not two. The parser recognizes them in time-range contexts (`LAST 7d`, `WITHIN 30m`, `BRACKETS [1d, 7d]`) and in arithmetic (`ts + 7d`, `p.ts - s.ts > 1h`).

Compound durations: use arithmetic (`1d + 12h`), not compound literals. This keeps the lexer simple and makes the intent of compound durations explicit. The constant folder reduces `1d + 12h` to a single i64 at plan time, so there is no runtime cost.

Type: duration literals have BqlType `Int` (see type-system.md Section 4.1). The duration interpretation is context-dependent — `WITHIN 7d` treats the int as a window duration, `ts + 7d` treats it as a timestamp offset.

---

## 23. Quoted Event Names and Identifiers

Bare event names and column names must be valid identifiers: `[a-zA-Z_][a-zA-Z0-9_]*`. Real-world event data often violates this — event types like `page view`, `user.signed_up`, `order/refund`, or CJK characters are common. BQL supports **backtick-quoted event names** to cover these cases:

```bql
events
| MATCH FIRST SEQUENCE(
    `page view` THEN `purchase.completed`
  ) WITHIN 1h
```

### 23.1 Quoting Rules

- **Syntax**: a backtick-quoted name is any sequence of characters between two backticks (`` ` ``) — any character except backtick itself and newline is allowed.
- **Applies to**: event names (in MATCH steps, FIRST/LAST/NTH, SESSIONIZE `end:`, WITHOUT exclusions) and column names (in WHERE, SELECT, LET, GROUP BY, ORDER BY).
- **Table names**: also backtick-quotable in the source expression (`` `events.raw` LAST 30d JOIN `purchases/eu` ``).
- **Never needed for keywords**: keywords cannot be shadowed by user names regardless of quoting — `` `MATCH` `` as a column name is accepted by the lexer but rejected at name resolution as a reserved identifier. Use a different column name.
- **Canonical form**: bare identifiers and backtick-quoted identifiers that happen to match the bare-identifier pattern are equivalent. `` `user_id` `` and `user_id` refer to the same column.

### 23.2 Literal Backticks in Names

A backtick inside a name is represented by doubling it: `` `weird``name` `` refers to a name literally containing a backtick. This is rare enough in practice that doubling (rather than backslash escaping) is acceptable.

### 23.3 String Comparison

At the NFA layer and in scan-level filters, event types are compared as strings after the lexer strips the surrounding backticks. A bare identifier and its quoted form produce the same internal representation — no runtime overhead for using quoted names (sequence-matching.md Section 9.2 on event-type filtering).

### 23.4 When to Use Quoted Names

Quoted names should be a last resort — prefer renaming events at ingest time to valid identifiers when possible, because:

- Bare identifiers are easier to read.
- Error messages are less noisy.
- Cross-tool compatibility is better (dashboards, exports, etc.).

But for data you don't control (e.g., third-party event streams with mandated naming conventions), quoted names are the escape hatch.

---

## 24. Comments

```bql
-- Line comment
events
| WHERE event_type = 'purchase'  -- inline comment
/* Block comment */
| STATS purchase_count = COUNT(*)
```

Line comments start with `--` and extend to end of line. Block comments are `/* ... */` and do not nest. Comments are stripped by the lexer and do not appear in the AST.

---

## 25. Pipeline Composition Rules

### 25.1 Operator Categories

| Category | Operators | Entity semantics |
|---|---|---|
| Source | table reference with optional time range | Produces entity event stream |
| Filter/Transform | WHERE, SELECT, LET | Passes through entity boundaries |
| Entity ops | MATCH, FUNNEL, RETENTION, SESSIONIZE, FIRST/LAST/NTH, SAMPLE, ATTRIBUTE | Operates per entity |
| Post-entity | STATS, window functions (OVER) | Aggregates across entities or per entity |
| Post-agg | WHERE (on aggregates), ORDER BY, LIMIT, PIVOT | Operates on aggregated rows |

FUNNEL and RETENTION appear as entity ops in the category table, but the planner desugars them into MATCH + STATS compositions during logical plan construction (Section 6). Their composition rules follow from the desugared form.

### 25.2 What Can Follow What

| Upstream | Valid Downstream |
|---|---|
| Source (table) | WHERE, SELECT, LET, MATCH, FUNNEL, RETENTION, SESSIONIZE, FIRST/LAST/NTH, SAMPLE, ATTRIBUTE, STATS |
| SAMPLE | WHERE, SELECT, LET, MATCH, FUNNEL, RETENTION, SESSIONIZE, FIRST/LAST/NTH, ATTRIBUTE, STATS |
| WHERE (pre-aggregate) | WHERE, SELECT, LET, MATCH, FUNNEL, RETENTION, SESSIONIZE, FIRST/LAST/NTH, ATTRIBUTE, STATS |
| SELECT / LET (pre-aggregate) | WHERE, SELECT, LET, MATCH, FUNNEL, RETENTION, SESSIONIZE, ATTRIBUTE, STATS, ORDER BY, LIMIT |
| MATCH | WHERE, SELECT, LET, STATS, ORDER BY, LIMIT |
| FUNNEL | (terminal after desugaring — produces aggregated rows; follows STATS rules) |
| RETENTION | (terminal after desugaring — produces aggregated rows; follows STATS rules) |
| SESSIONIZE | WHERE, SELECT, LET, MATCH, STATS |
| FIRST/LAST/NTH | WHERE, SELECT, LET, STATS, ORDER BY, LIMIT |
| ATTRIBUTE | WHERE, SELECT, LET, STATS, ORDER BY, LIMIT |
| STATS | WHERE, SELECT, LET, ORDER BY, LIMIT, PIVOT |
| PIVOT | WHERE, SELECT, LET, ORDER BY, LIMIT |
| ORDER BY | WHERE, SELECT, LET, LIMIT |
| LIMIT | WHERE, SELECT, LET, ORDER BY |

**Notes on the table:**
- **SAMPLE placement**: SAMPLE is typically the first operator after the source because it is a scan-level operator and works best when pushed down all the way. In general, SAMPLE is only meaningful on the event stream (before any aggregation), so it follows the same downstream rules as the source.
- **WHERE after STATS**: the "pre-aggregate" WHERE row above applies before STATS. WHERE after STATS follows the STATS row (WHERE on aggregated columns; behaves like SQL HAVING).
- **LET and SELECT after STATS / ORDER BY**: allowed, enabling post-aggregation computed columns (e.g., window functions in a top-N query — Section 28.9). The planner treats SELECT / LET after STATS as running on aggregated rows, so expressions may reference aggregate result columns.
- **LIMIT is not terminal**: LIMIT output is still a row stream with a well-defined schema, and WHERE / SELECT / LET / ORDER BY may follow. A common pattern is `... | ORDER BY count DESC | LIMIT 100 | SELECT col1, col2` to project after taking the top N.

### 25.3 Invalid Compositions

- **MATCH → MATCH**: not supported. MATCH transforms the event stream into match results; you can't re-match on match rows. Use aliases + `IN` for multi-pattern analysis (Section 5.3).
- **STATS → MATCH**: aggregated results are not event streams.
- **STATS → SESSIONIZE**: same reason.
- **FUNNEL → anything**, **RETENTION → anything**: these are terminal sugar forms that desugar into `... | MATCH ... | STATS ...`. Downstream operators after FUNNEL/RETENTION are rejected at parse time; to continue processing, write the desugared MATCH + STATS form explicitly.

The planner enforces these rules at plan construction time. Invalid compositions produce a clear error pointing to the offending operator and explaining the restriction.

### 25.4 Schema Validation

Each operator's output schema is validated against the next operator's input requirements at plan time (type-system.md Section 12). Type errors include column names, expected vs. actual types, and enough context to locate the problem in the source.

---

## 26. Grammar

The grammar below is schematic: terminals like `duration`, `integer`, `number`, `string_lit`, and `identifier` are lexer tokens, not parser productions. In particular, `duration` is a single fused token in the lexer (`7d` is one token, not `7` followed by `d` — see Section 22). Keyword terminals (e.g., `MATCH`, `WHERE`) are also lexer tokens; the parser is case-insensitive per Section 2.2.

```
-- Top-level
statement        := query
                  | insert_stmt
                  | delete_stmt
                  | create_table
                  | alter_table
                  | drop_table
                  | describe_stmt
                  | explain_stmt

query            := (alias_def)* pipeline

alias_def        := identifier "=" pipeline

pipeline         := source ("|" operator)*

source           := name time_range? (JOIN name)*
time_range       := LAST duration
                  | BETWEEN string_lit AND string_lit

operator         := where_op | select_op | let_op | match_op | funnel_op | retention_op
                  | sessionize_op | stats_op | order_op | limit_op | pivot_op
                  | first_last_op | nth_op | sample_op | attribute_op

-- WHERE
where_op         := WHERE predicate

-- SELECT (computed expressions must have an AS alias — see Section 10)
select_op        := SELECT DISTINCT? select_list
select_list      := select_item ("," select_item)*
select_item      := "*"
                  | name                                 -- bare column reference
                  | name "." name                        -- qualified column reference
                  | expr AS identifier                   -- computed expression, name required

-- LET
let_op           := LET identifier "=" expr

-- MATCH
match_op         := MATCH match_mode SEQUENCE "(" step_list ")" match_modifiers
match_mode       := FIRST | ALL
step_list        := step (step_sep step)*
step_sep         := (WITHOUT exclusion)? (THEN | "->") IMMEDIATELY?
step             := unqualified_step repetition?
                  | "(" step ")" repetition?            -- parenthesized group (required for WHERE + repetition)
unqualified_step := (identifier ":")? step_event (WHERE predicate)?
step_event       := event_ref
                  | "(" event_ref (OR event_ref)+ ")"
event_ref        := (name ".")? name                    -- table.event_type in multi-table queries
exclusion        := event_ref
                  | "(" event_ref (OR event_ref)+ ")"
repetition       := "*" | "+"
match_modifiers  := (WITHIN (duration | SESSION))?
                    (BRACKETS CUMULATIVE? "[" duration_list "]")?
                    (EMIT ALL)?
duration_list    := duration ("," duration)*

-- FUNNEL (sugar; BRACKETS and EMIT ALL are implicit in the desugaring)
funnel_op        := FUNNEL "(" step_list ")" funnel_modifiers
funnel_modifiers := (WITHIN duration)?

-- RETENTION (sugar)
retention_op     := RETENTION "(" retention_args ")"
retention_args   := "entry" ":" event_ref "," "activity" ":" event_ref ","
                    "brackets" ":" "[" duration_list "]"
                    ("," "cumulative" ":" bool_lit)?

-- SESSIONIZE
sessionize_op    := SESSIONIZE "(" session_params ")"
session_params   := "gap" ":" duration ("," "end" ":" event_ref)?

-- STATS (aggregate expressions must have an `alias =` assignment — see Section 7.1)
stats_op         := STATS agg_list (GROUP BY group_list)?
agg_list         := agg_item ("," agg_item)*
agg_item         := identifier "=" agg_expr              -- output name is required
agg_expr         := agg_func "(" (expr | "*") ")"
agg_func         := COUNT | COUNT_DISTINCT | SUM | AVG | MIN | MAX
                  | P50 | P90 | P95 | P99
group_list       := group_item ("," group_item)*
group_item       := name                                 -- bare column reference
                  | expr AS identifier                   -- computed group key, name required

-- FIRST / LAST / NTH
first_last_op    := (FIRST | LAST) "(" event_ref (WHERE predicate)? ")"
nth_op           := NTH "(" event_ref (WHERE predicate)? "," integer ")"

-- SAMPLE
sample_op        := SAMPLE "(" sample_param ("," "seed" ":" integer)? ")"
sample_param     := "fraction" ":" number | "count" ":" integer

-- ATTRIBUTE
attribute_op     := ATTRIBUTE "(" "conversion" ":" event_ref ","
                                  "touchpoints" ":" event_ref ","
                                  "window" ":" duration ","
                                  "touchpoint_key" ":" expr ")"

-- ORDER BY / SORT (SORT is a single-keyword alias for ORDER BY)
order_op         := ORDER BY order_item ("," order_item)*
                  | SORT    order_item ("," order_item)*
order_item       := expr (ASC | DESC)?

-- LIMIT
limit_op         := LIMIT integer

-- PIVOT (reserves syntax space for future literal value list)
pivot_op         := PIVOT name ON name (IN "(" literal_list ")")?
literal_list     := literal ("," literal)*

-- Expressions
expr             := or_expr
or_expr          := and_expr (OR and_expr)*
and_expr         := not_expr (AND not_expr)*
not_expr         := NOT? comparison
comparison       := addition (comp_op addition)?
                  | addition IS NOT? NULL
                  | addition NOT? IN in_rhs
                  | tuple_expr NOT? IN in_rhs           -- multi-column IN (compound keys)
                  | addition NOT? BETWEEN addition AND addition
                  | addition NOT? LIKE string_lit
                  | addition "~=" string_lit
                  | addition CONTAINS string_lit
in_rhs           := "(" arg_list ")"                    -- inline literal list
                  | QUERY "(" pipeline ")"              -- inline subquery (explicit marker)
                  | identifier                          -- alias reference
tuple_expr       := "(" addition ("," addition)+ ")"    -- 2+ columns
comp_op          := "=" | "!=" | "<" | ">" | "<=" | ">="
addition         := multiplication (("+"|"-") multiplication)*
multiplication   := unary (("*"|"/"|"%") unary)*
unary            := "-"? primary
primary          := literal
                  | name                                    -- column reference
                  | name "." name                           -- table.column or step.column access
                  | "$" identifier                          -- variable binding reference
                  | func_call
                  | "(" expr ")"
                  | case_expr
func_call        := identifier "(" arg_list? ")"
                  | identifier "(" arg_list? ")" OVER "(" window_spec ")"
arg_list         := expr ("," expr)*
case_expr        := CASE (WHEN predicate THEN expr)+ (ELSE expr)? END
window_spec      := (PARTITION BY arg_list)?
                    (ORDER BY order_item ("," order_item)*)?
predicate        := expr                                    -- must type-check to Bool

-- Identifiers and quoted names
--
-- `identifier` is the bare token used for alias names, step names after `:`,
-- variable names after `$`, column aliases after `AS`, named parameters
-- (`gap:`, `entry:`, etc.), and function names — places where bqlite controls
-- the name space.
--
-- `name` is the general form used for user-defined tables, columns, and event
-- types, which may contain characters not allowed in bare identifiers. See
-- Section 23 for quoting rules.
name             := identifier | quoted_name
quoted_name      := "`" [^`]+ "`"

-- DML
insert_stmt      := INSERT INTO name insert_body
insert_body      := VALUES literal_tuple ("," literal_tuple)*
                  | FROM string_lit (WITH "(" insert_option ("," insert_option)* ")")?
literal_tuple    := "(" literal ("," literal)* ")"
insert_option    := identifier ":" literal
                  | MAP ":" "(" column_mapping ("," column_mapping)* ")"
column_mapping   := identifier AS identifier
delete_stmt      := DELETE FROM name WHERE predicate

-- DDL
create_table     := CREATE TABLE name "(" column_def ("," column_def)* ")"
alter_table      := ALTER TABLE name ADD COLUMN name type_expr alter_modifier*
drop_table       := DROP TABLE name
describe_stmt    := DESCRIBE name
explain_stmt     := EXPLAIN pipeline
column_def       := name type_expr column_modifier*
type_expr        := scalar_type | composite_type
scalar_type      := BOOL | INT | FLOAT | STRING | TIMESTAMP
composite_type   := LIST "(" type_expr ")" | MAP "(" type_expr ")"
column_modifier  := ENTITY KEY | EVENT TYPE | EVENT TIME | NOT NULL | NULL | DEFAULT literal
alter_modifier   := NOT NULL | NULL | DEFAULT literal

-- Literals (terminals)
literal          := integer | number | string_lit | bool_lit | NULL | duration
bool_lit         := TRUE | FALSE
duration         := -- single fused lexer token matching [0-9]+("d"|"h"|"m"|"s"|"ms"|"us"|"ns")
string_lit       := "'" [^']* "'"
integer          := [0-9]+
number           := [0-9]+ ("." [0-9]+)?
identifier       := [a-zA-Z_][a-zA-Z0-9_]*

-- Comments (stripped by lexer)
comment          := "--" [^\n]* | "/*" .* "*/"
```

### 26.1 Grammar Notes

- **`statement`** is the top-level entry point accepted by the parser. A `query` (pipeline-producing) statement, DML (INSERT / DELETE), or DDL (CREATE / ALTER / DROP / DESCRIBE) all parse through this rule.
- **INSERT literal tuples** are restricted to `literal` values (no arbitrary expressions). This matches the "literal INSERT" prose in Section 20.1 and defers computed-value inserts to a later version.
- **Multi-column IN** uses a `tuple_expr` as the left-hand side. A single scalar `addition` remains the normal form.
- **Window functions** make `ORDER BY` optional inside `window_spec` — when omitted, the default is the timestamp column (Section 13, type-system.md Section 6.8).
- **`literal_list`** is reserved on `PIVOT` for a future `IN (...)` form that names the pivot values explicitly. In v1, the list is optional and pivot values must be inferrable from the upstream operator (e.g., BRACKETS).
- **Match modifier ordering** is fixed (canonical order) per the grammar: `WITHIN` or `BRACKETS` first, then `EMIT ALL`. Additionally, `WITHIN`, `WITHIN SESSION`, and `BRACKETS` are semantically mutually exclusive — the parser enforces the order, and the planner rejects queries that specify more than one (since all three occupy the same time-window role).
- **`duration`** is a single lexer token. The grammar uses the symbol `duration` as a terminal, not as a two-part production. The lexer resolves the latent ambiguity between `integer` and `duration` by longest-match.
- **Numeric literal disambiguation.** `integer` and `number` overlap on tokens like `42`. The lexer applies longest-match: if the token contains a decimal point it is `number`, otherwise `integer`. Duration literals (`7d`) are tried first by longest-match — a trailing unit suffix after digits always wins.
- **Negative numeric literals.** There is no negative-literal token. `-5` parses as unary minus applied to the integer literal `5` via `unary := "-"? primary`. This means contexts that require a terminal token (e.g., `LIMIT integer`) do not accept a leading `-`, and `LIMIT -5` is a parse error.
- **`IN` subquery vs literal list.** `in_rhs` has three distinct forms: an inline literal list `IN (a, b, c)`, an inline subquery `IN QUERY (pipeline)`, and an alias reference `IN alias_name`. The `QUERY` keyword is required for inline subqueries to disambiguate them from one-element literal lists. `WHERE x IN (events)` is a literal list containing a single value reference; `WHERE x IN QUERY (events | SELECT entity_id)` is a subquery.
- **`NOT` in comparisons.** `NOT IN`, `NOT BETWEEN`, and `NOT LIKE` are parsed as single operators inside the `comparison` production and are distinct from the general unary `NOT` that sits at `not_expr`. This matches SQL semantics (`NOT x IN (a, b)` and `x NOT IN (a, b)` are equivalent).

### 26.2 Reserved Keywords

The parser is case-insensitive for keywords (Section 2.2), and the following identifiers are reserved — they cannot be used as table names, column names, event names, alias names, or variable names (without quoting, see Section 23):

```
ADD AND ALL ALTER AS ASC ATTRIBUTE AVG BETWEEN BRACKETS BY
CASE CAST COALESCE COLUMN CONTAINS COUNT COUNT_DISTINCT CREATE CUMULATIVE
DEFAULT DELETE DESC DESCRIBE DISTINCT DROP
ELSE EMIT END ENTITY EVENT EXPLAIN
FALSE FIRST FROM FUNNEL
GROUP
IF IMMEDIATELY IN INSERT INTO IS
JOIN
KEY
LAG LAST LEAD LENGTH LET LIKE LIMIT LIST
MAP MATCH MAX MIN
NOT NTH NULL
ON OR ORDER OVER
P50 P90 P95 P99 PARTITION PIVOT
QUERY
RANK RETENTION ROW_NUMBER
SAMPLE SEQUENCE SELECT SESSION SESSIONIZE STATS SUM
TABLE THEN TIME TIMESTAMP TRUE TYPE
UPPER
VALUES
WHEN WHERE WITH WITHIN WITHOUT
BOOL INT FLOAT STRING                            -- scalar type keywords
```

Scalar function names (e.g., `QUANTIZE`, `EPOCH_NANOS`, `CONCAT`, `LOWER`, `ABS`, `FLOOR`, `CEIL`, `ROUND`, `TRIM`, `SUBSTRING`, `EXP`, `LN`, `POW`; see type-system.md Section 10.2) are case-insensitive and are not reserved — they live in a function namespace separate from user identifiers. A column named `round` is legal; a function call `round(x)` is also legal; name resolution uses the function registry for calls and the column schema for bare references.

Aggregate-function names (`COUNT`, `COUNT_DISTINCT`, `SUM`, `AVG`, `MIN`, `MAX`, `P50`, `P90`, `P95`, `P99`) are reserved because they can appear as bare tokens in contexts where a column reference would otherwise parse, and the parser needs to disambiguate syntactically. The same applies to window-function names used with `OVER` (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`).

### 26.3 Identifier Resolution

- **Case sensitivity.** User-defined identifiers — table names, column names, event names, alias names, variable names, step names — are **case-sensitive**. `User_ID` and `user_id` are distinct. This matches the case-sensitivity rule for column names in type-system.md Section 5.1.
- **Bare vs quoted equivalence.** A quoted identifier that happens to match the bare-identifier pattern is equivalent to the bare form. `` `user_id` `` and `user_id` refer to the same column; `` `user id` `` (with a space) cannot have a bare form.
- **Keyword shadowing is rejected regardless of quoting.** Using `` `MATCH` `` as a column name is accepted by the lexer (backticks can contain any character except backtick) but rejected by the planner because the resulting identifier collides with a reserved keyword. To use a literal keyword as a column name, rename the column at ingest time.
- **Alias resolution in `IN`.** The bare-identifier form of `in_rhs` always resolves to an alias, never a column. Column-valued `IN` comparisons must use the inline literal-list form (`IN (a, b, c)`) or the subquery form (`IN QUERY (...)`).

---

## 27. Error Message Strategy

Error messages are a user-facing product. BQL error messages should follow three principles:

1. **Point to the problem.** Every error includes a source span (line and column) and a snippet of the offending text.
2. **Explain the expected vs. actual.** For type errors: "column `amount` is FLOAT, but SUM requires INT or FLOAT — did you mean `AVG`?"
3. **Suggest a fix when possible.** Common mistakes have canned suggestions: missing THEN between steps, `SELECT` missing `AS`, using HAVING instead of `| WHERE`.

### 27.1 Common Error Categories

| Category | Examples | Suggestion heuristic |
|---|---|---|
| Unknown column | `amount` doesn't exist | List available columns; did-you-mean for similar names |
| Type mismatch | comparing String to Int | Suggest explicit CAST |
| Invalid composition | MATCH after STATS | Explain why the composition is invalid; suggest alias + IN |
| Missing keyword | events WHERE ... (no pipe) | "missing pipe `\|` between source and WHERE" |
| Unknown operator | SLECT | did-you-mean for keyword typos |
| Missing THEN | `MATCH(A, B)` | "step separator `THEN` or `->` expected between steps" |
| Trailing WITHOUT | `MATCH(A THEN B WITHOUT C)` | "WITHOUT must appear between two steps" |
| Mutually exclusive modifiers | `WITHIN 7d BRACKETS [1d]` | "WITHIN and BRACKETS cannot both be specified" |
| Shadowed keyword | `stats = ...` | "alias name `stats` conflicts with keyword" |

### 27.2 Error Severity Levels

- **Error**: query cannot execute. Reported and halts execution.
- **Warning**: query can execute but may have undesired behavior (e.g., Int → Float coercion with loss of precision, untyped time range, SAMPLE without a seed).
- **Hint**: optional style suggestions (e.g., "use LET instead of SELECT * + computed column").

Warnings and hints are opt-in at the REPL layer — they can be silenced by flag.

---

## 28. Complete Examples

### 28.1 Basic Funnel

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(signup THEN add_to_cart THEN purchase) WITHIN 7d EMIT ALL
| STATS
    signup = SUM(CAST(step_reached >= 1 AS INT)),
    add_to_cart = SUM(CAST(step_reached >= 2 AS INT)),
    purchase = SUM(CAST(step_reached >= 3 AS INT))
```

### 28.2 Funnel Over Time

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(s: signup THEN p: purchase) WITHIN 7d EMIT ALL
| STATS
    entered = COUNT(*),
    converted = SUM(CAST(step_reached >= 2 AS INT))
  GROUP BY QUANTIZE(s.ts, 1d) AS day
```

### 28.3 Retention

```bql
events LAST 180d
| MATCH FIRST SEQUENCE(signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
| STATS retention_rate = AVG(CAST(step_reached >= 2 AS INT)) GROUP BY bracket
```

### 28.4 Retention by Cohort Week

```bql
events LAST 180d
| MATCH FIRST SEQUENCE(s: signup THEN purchase) BRACKETS [1d, 7d, 14d, 30d] EMIT ALL
| STATS retention_rate = AVG(CAST(step_reached >= 2 AS INT))
  GROUP BY QUANTIZE(s.ts, 7d) AS cohort_week, bracket
```

### 28.5 Funnel With Held Properties

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(
    s: signup WHERE plan = $plan
    THEN p: purchase WHERE plan = $plan
  ) WITHIN 7d EMIT ALL
| STATS conversion = AVG(CAST(step_reached >= 2 AS INT))
  GROUP BY plan
```

### 28.6 Funnel With Exclusion

```bql
events LAST 30d
| MATCH FIRST SEQUENCE(
    signup
    WITHOUT churn THEN add_to_cart
    WITHOUT churn THEN purchase
  ) WITHIN 7d EMIT ALL
| STATS
    signup = SUM(CAST(step_reached >= 1 AS INT)),
    add_to_cart = SUM(CAST(step_reached >= 2 AS INT)),
    purchase = SUM(CAST(step_reached >= 3 AS INT))
```

### 28.7 Session Analysis

```bql
events LAST 30d
| SESSIONIZE(gap: 30m)
| STATS
    sessions = COUNT_DISTINCT(session_id),
    avg_session_duration = AVG(session_duration)
  GROUP BY entity_id
```

### 28.8 Cohort Analysis With Aliases

```bql
-- Define cohort
power_users = events LAST 90d
  | WHERE event_type = 'purchase'
  | STATS purchase_count = COUNT(*) GROUP BY entity_id
  | WHERE purchase_count > 10
  | SELECT entity_id

-- Analyze power users
events LAST 90d
| WHERE entity_id IN power_users
| MATCH FIRST SEQUENCE(s: search THEN purchase) WITHIN 1d EMIT ALL
| STATS conversion = AVG(CAST(step_reached >= 2 AS INT))
  GROUP BY QUANTIZE(s.ts, 7d) AS week
```

### 28.9 Top N Per Group

```bql
events LAST 30d
| WHERE event_type = 'purchase'
| STATS purchase_count = COUNT(*) GROUP BY category, product
| LET rank = ROW_NUMBER() OVER (PARTITION BY category ORDER BY purchase_count DESC)
| WHERE rank <= 3
```

### 28.10 Cross-Table Join

```bql
events LAST 30d JOIN purchases
| MATCH FIRST SEQUENCE(
    events.signup
    THEN purchases.purchase WHERE purchases.amount > 100
  ) WITHIN 30d EMIT ALL
| STATS high_value_conversion_rate = AVG(CAST(step_reached >= 2 AS INT))
```

### 28.11 WITHIN SESSION

```bql
events LAST 30d
| SESSIONIZE(gap: 30m)
| MATCH FIRST SEQUENCE(search THEN product_view THEN add_to_cart) WITHIN SESSION EMIT ALL
| STATS
    searches = SUM(CAST(step_reached >= 1 AS INT)),
    viewed_product = SUM(CAST(step_reached >= 2 AS INT)),
    added_to_cart = SUM(CAST(step_reached >= 3 AS INT))
```

---

## 29. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Syntax family | Pipe-first (KQL/PRQL style) | Matches execution order; natural for sequential behavioral queries |
| Keyword case | ALL CAPS canonical, case-insensitive parser | Visual distinction between language and data |
| Time range | Source-level with LAST or BETWEEN | Drives storage pruning; most critical optimization hint |
| Scan extension for MATCH | Automatic; user sees stated range | Prevents manual widening; preserves pruning |
| Step separator | THEN primary, `->` alias | Readability for both verbose and dense patterns |
| Match modes | FIRST, ALL (non-overlapping) | Variable bindings cover overlapping cases |
| EMIT ALL | Modifier flag, adds step_reached | Essential for funnel analysis |
| Time windows | Global only (WITHIN) | Per-step deferred; post-match WHERE covers most cases |
| Negation | WITHOUT between steps | Intuitive gap-binding semantics |
| Variable bindings | `$var` syntax, MATCH-scoped | Separate from step names; independent match tracks |
| Named steps | `s: signup` prefix | Required for repeated event types; optional otherwise |
| BRACKETS | Exclusive by default, CUMULATIVE opt-in | Standard retention semantics |
| FUNNEL / RETENTION | Sugar desugared at planner | One set of primitives; no separate operator code paths |
| HAVING | No — use WHERE after STATS | Pipe order matches execution order; no need for a separate keyword |
| Aliases | Session-scoped, lazy, no persistence in v1 | Covers CTE use cases without special syntax |
| Cohorts | Aliases + IN | No cohort type; just queries returning entity IDs |
| Joins | Entity-aligned only in v1 | Dominant use case; general joins deferred |
| Join syntax | Explicit `JOIN` keyword + `table.event` references | Self-documenting multi-table queries |
| Join schema requirement | Entity-key type must match; column names may differ | Shard function is database-level, so entity alignment is by hash value |
| MATCH → MATCH | Not supported | Stream transformation; use alias + IN |
| IN set form | Three forms: literal list `IN (...)`, subquery `IN QUERY (...)`, alias `IN name` | Explicit `QUERY` keyword disambiguates pipelines from one-element literal lists |
| Self-join | Not supported | Would require table aliases; not needed for v1 use cases |
| Database init | CLI-only (`bqlite init`) | No BQL statement for creating a database; shard count and UUID set at initialization. **Wave 1 shortcut:** until `CREATE TABLE` DDL lands in Wave 2, `Database::open_or_create` seeds a default `events` table into the manifest so `bqlite query "events"` can parse-plan-execute against a freshly created database (the Wave 1 acceptance gate). The seeded entry carries a `bootstrap_events_table: true` flag in `TableEntry` (storage-format.md §12.3) that later waves check to retire the shortcut cleanly once user DDL takes over — see TASK-125. |
| BY clause | Not supported anywhere | Entity key is declared once in the table schema; no per-query override |
| LIMIT | Not terminal — may be followed by more operators | LIMIT output is still a row stream; composability matters |
| EXPLAIN | `EXPLAIN <pipeline>` for plan output (pipelines only) | Standard SQL term; DESCRIBE remains for table schemas |
| Event name quoting | Backtick-quoted (`` `page view` ``) | Supports data with spaces/dots/CJK/etc. |
| Case sensitivity | User identifiers case-sensitive; keywords case-insensitive | SQL convention; keeps collision rules simple |
| Reserved keywords | Canonical list in Section 26.2 | Prevents aliasing over language constructs |
| Output column names | Required for STATS aggregates, SELECT/LET computed columns, GROUP BY expressions | No auto-generated names; stable downstream references |
| LET rebinding | Not allowed — new name must not collide with existing column | Matches SELECT collision rule; unambiguous column refs |
| DISTINCT in aggregates | `COUNT_DISTINCT(x)` only; no `COUNT(DISTINCT x)`, no `SUM(DISTINCT ...)` | One canonical form; minimal surface |
| Operator precedence | SQL standard: `NOT` below comparisons, above `AND` | Standard SQL semantics — `NOT x = 5` parses as `NOT (x = 5)` |
| PATHS operator | Not in v1 | Can be added later if users need it; MATCH + STATS covers most analyses |
| Comment syntax | `--` and `/* */` | SQL-familiar |
| Regex dialect | RE2 | Prevents pathological runtime (no backreferences, no lookaround) |
| UPDATE | Not supported | Analytics model: facts are immutable |
| Duration literals | Single-unit only, compound via arithmetic | Simple lexer; explicit compound expressions |
| Schema DDL | CREATE, ALTER ADD COLUMN, DROP, DESCRIBE | Mirrors type-system.md Section 9 |

---

## 30. Resolved Design Questions

These are design questions that were raised and then resolved during the v1 design process. They are kept in this section for context and traceability.

### 30.1 Cross-Step Property Access Demand Model

**Resolved.** Non-timestamp property access (`p.amount`, `s.device`) from a named step downstream sets **per-(step, property) demand bits** on the MATCH operator. When the planner sees a downstream reference to `step_name.column`, it adds `(step_index, column_name)` to MATCH's required output set. MATCH carries only the referenced (step, property) pairs in its per-candidate state — unreferenced properties are not materialized.

This is the most granular option (Option 1 from the prior discussion). It avoids the memory blowup of an all-or-nothing match_events map and is strictly more ergonomic than restricting downstream access to timestamps. The demand bits flow through the DemandCapabilities protocol (execution-model.md Section 8.2, planner-pipeline.md TASK-006).

### 30.2 WITHIN SESSION Propagation

**Resolved.** `WITHIN SESSION` is implemented by having MATCH **observe the `session_id` column** in its input schema. SESSIONIZE emits `session_id` as a monotonically increasing integer per entity (guaranteed sequential within an entity's event stream). MATCH with `WITHIN SESSION` tracks the current `session_id` and expires all active candidates for that binding track when the id increments.

No sentinel events, no window annotations, no session-aware NFA. The correctness argument reduces to: within a single session, the `session_id` column is constant; at the boundary between sessions, it changes by exactly one; the increment serves as the expiry trigger. This is simple to implement and cheap to execute.

### 30.3 Variable Bindings Under Repetition

**Resolved.** A variable first bound inside a repeated step `(B WHERE prop = $x)+` is established on the **first iteration** of the repeated step and held constant across all subsequent iterations and non-repeated steps. Any iteration that does not match the held value is rejected by the step predicate (so the NFA does not re-enter the self-loop for that candidate). This rule is the only one consistent with per-track variable bindings — a single track cannot hold two different values for the same variable. Sequence-matching.md owns the implementation detail; this document specifies the user-visible semantics.

### 30.4 FUNNEL/RETENTION Desugaring Placement

**Resolved.** FUNNEL and RETENTION are desugared in the **planner** during logical plan construction (not in the parser). This gives the desugaring access to the schema for error-message fidelity and makes the rewrite visible in `EXPLAIN` output. The AST preserves the original FUNNEL/RETENTION nodes so source-span error messages can reference the user's original text.

### 30.5 Parameterized Aliases

**Resolved: out of scope for v1.** Aliases do not take parameters in v1. Syntax like `cohort(event) = events | WHERE event_type = event | SELECT entity_id` is effectively user-defined functions and is deferred to v2. The v1 alias syntax is `identifier = pipeline` only.

### 30.6 BRACKETS × Variable Bindings

**Resolved.** When both BRACKETS and variable bindings are specified, **each `(entity, binding track)` gets its own bracket evaluation**. Each track produces its own set of per-bracket rows, and brackets are computed relative to that track's own anchor event. This is consistent with sequence-matching.md Section 8's rule that every `(entity, binding track)` pair is treated as an independent match stream.

### 30.7 EXPLAIN Output Format

**Deferred.** The exact output format for the `EXPLAIN <pipeline>` statement (text tree? JSON? both?) is a UX decision deferred to the CLI/REPL implementation (Wave 6). The planner will emit structured plan data (tree-of-nodes with per-node operator kind, schema, demand bits, row-count estimates if cost-based planning is added) and the REPL is responsible for rendering it. TASK-006 (planner pipeline) owns the structured plan data format.

### 30.8 INSERT Batch Semantics

**Resolved.** Each `INSERT` statement produces **one batch**. Multi-statement coalescing is not supported — two sequential `INSERT INTO events VALUES (...)` statements produce two segments, not one. This keeps the ingest semantics predictable and matches the crash-safety guarantee (reliability.md: each ingest call produces complete segment files). Batch-level coalescing would add complexity with limited benefit for v1.

### 30.9 Sample Determinism and the Database Identity Seed

**Resolved.** SAMPLE without an explicit `seed:` parameter uses a seed derived from the **database UUID**, which is generated at database creation time and stored in the manifest. The UUID is never rotated for the lifetime of the database. This makes unseeded SAMPLE deterministic for repeat queries on the same database but different across database clones. Storage-format.md owns the UUID generation and storage details.

### 30.10 Error Recovery in Parser

**Resolved.** The parser **halts on first error** in v1. This is the simplest option, gives users a clear error location, and avoids cascading phantom errors from recovery state. Multi-error diagnostics (panic-mode recovery) can be added in v2 if users report single-error diagnostics are inadequate for iterative query authoring.

### 30.11 Cross-Step Predicates Inside Patterns

**Resolved.** Cross-step comparisons inside pattern predicates are supported **only via variable bindings**, not as a general "earlier step reference" form. The form `B WHERE B.amount > A.amount` is not supported; users must express the intent with a binding.

**Consequence: some semantics are not expressible.** Variable binding creates an **independent match track per distinct binding value**. This means `(v1: page_view WHERE page = $p) THEN (v2: page_view WHERE page != $p)` does not mean "v2 is any page_view whose page differs from v1's page" — it means "each distinct page value `$p` creates a separate track; within that track, v2 is any page_view whose page is not `$p`." The two are subtly different:

- With a general cross-step form, a single entity's stream `[A, B, A]` would produce one match `(v1=A, v2=B)` (and possibly `(v1=B, v2=A)` in MATCH ALL).
- With the binding-only form, the same stream produces one match for the `$p=A` track and one for the `$p=B` track. Each track only sees the v2 events with a different page than its bound `$p`. The effect is the same in this simple example, but diverges when subsequent steps reference `$p` — the binding is held for the whole track, so later steps also see the page-A vs page-B distinction.

The restriction is accepted because:
1. The general form requires the NFA to carry earlier-step events in active candidate state, which blows up memory and complicates the deferred-consumption correctness argument (sequence-matching.md Section 4).
2. Post-match WHERE on named step properties (Section 5.2) covers the cases where the comparison should be applied *after* the match rather than pruning during it — `| WHERE v2.page != v1.page` filters out matches where the two pages are equal, without affecting which matches are discovered.
3. The common behavioral-analytics use cases (funnels with held properties, retention with dimension splitting) are naturally expressed with bindings.

Users who need the general form can fall back to post-match WHERE filtering (accepting that the match enumeration runs first) or restructure their query with aliases.

---

## 31. Crate Placement

BQL parsing and AST live in `bqlite-parser` and `bqlite-ast`:

- `bqlite-ast`: AST node types (`Query`, `Pipeline`, `Operator`, `MatchExpr`, `Step`, `Expr`, etc.)
- `bqlite-parser`: BQL text → AST, including lexer, parser, and error reporting with source spans.

The parser depends on `bqlite-ast` for AST types. The AST crate depends only on `bqlite-core` for BqlType.

Desugaring (FUNNEL/RETENTION → MATCH + STATS) happens in `bqlite-planner` during logical plan construction, not in the parser. The AST preserves the original FUNNEL/RETENTION nodes so error messages can reference the source form.
