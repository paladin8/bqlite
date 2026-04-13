# TASK-407 — Cohorts, aliases, and entity-aligned source JOINs

Human-assisted semantics decisions for `docs/design/language/cohorts-aliases-joins.md`. These decisions are authoritative and override conflicting guesses drawn from `TASKS.md`, `query-language.md` §17 / §18 / §19 / §30.5, `type-system.md` §6.9, or `planner-pipeline.md` §5.2 (`SubqueryFilter`). Reconcile those docs in the same checkpoint as any code change that contradicts them.

The task design anchor covers two surfaces that share binding/planning infrastructure but are otherwise independent:

- **Cohorts, aliases, and `IN QUERY` / `IN alias`** — Block A and Block C.
- **Entity-aligned source `JOIN`** — Block B.

## Already pinned by existing docs (not re-litigated here)

- Grammar: `query := (alias_def)* pipeline`; `alias_def := identifier "=" pipeline`. `query-language.md` §18, §26.
- `IN` has three forms: literal list, `IN QUERY (pipeline)`, bare `IN alias`. Bare identifier on RHS of `IN` is **always** an alias reference, never a column. §17, §18.2.
- Aliases are lazy, composable, session-scoped, cycle-free. Alias-on-alias shadowing allowed (last wins); shadowing keywords or table names forbidden. §18.1.
- Parameterized aliases, persistent aliases/views are v2. §18.3, §30.5.
- `JOIN` is the entity-aligned source-expression keyword; same entity-key **type** required, column **names** may differ; same shard function/count required by construction; table-qualified refs mandatory inside JOIN; no self-joins; time range applies to first table. §19, §19.1, §19.2, §19.5.
- FUNNEL/RETENTION inside JOIN accept table-qualified event refs. §19.3.
- MATCH output retains a single `entity_id` column regardless of underlying entity-key column names. §19.5.

## Decisions — Block A: Aliases and cohorts (language)

### A1. Alias shadowing — keep §18.1 as-is; do not add event-type restrictions

Aliases may not shadow keywords or table names (status quo). Shadowing event type names is **not** forbidden, because event types are runtime string values inside a table rather than grammar-reserved tokens — the grammar has no clean hook for preventing the collision, and disambiguation always occurs through the table-qualified form (`events.signup`) in multi-table contexts or through the lack-of-alias-in-pattern-positions in single-table contexts.

Column names are likewise not forbidden because column references are never bare in alias-reference positions.

**Why:** §18.1's existing rule already covers the real collision surface (keywords + tables). Event-type names are arguably *values* rather than identifiers — forcing the grammar to know the catalog's event-type vocabulary at alias-def time is a layering violation.

### A2. Alias forward references — forbidden; top-down order required

Aliases must be defined before they are referenced, in source order within a submission. The binder's first pass walks alias definitions top-down and resolves each RHS against the aliases already bound.

**Why:** matches users' visual mental model, simplifies cycle detection to a trivial stack check (A7), and removes the "mutual recursion" edge case which would have to be rejected anyway. No compelling use case for forward refs in v1.

### A3. Submission boundary — one `execute` call; engine is alias-stateless

An engine `execute` call takes a single text string containing `(alias_def)* pipeline`. Aliases defined in submission N are **not** visible in submission N+1. The engine holds no alias session state.

Any cross-submission alias persistence (REPL history, multi-statement scripts) is a CLI-layer concern implemented by prepending buffered alias definitions to the next submission's text. The CLI can expose whatever UX model it wants without changing the engine contract.

**Why:** keeps the engine stateless and the alias-scope semantics trivially reproducible. Persistent aliases / views are already deferred to v2 (§18.3).

### A4. Per-submission alias caching — always cached within a submission

If an alias is referenced two or more times within a single submission, its pipeline is **executed exactly once** and the materialized result is reused across all references. Caching is not optional and not cost-based in v1.

**Why:** users reach for aliases specifically to reuse expensive work. Always-cached is the least surprising default and matches CTE caching behavior in most SQL engines. The memory bound comes from A6.

### A5. Cross-submission caching — none

Cohort materializations live for the duration of a single `execute` call and are dropped when the call returns. Submission N+1 on the same logical session does not inherit materializations from submission N.

**Why:** follows A3 — the engine is alias-stateless, so there is nowhere to hold cross-submission state. CLI-layer caching (if ever added) is orthogonal.

### A6. Cohort size cap — no operator-specific cap in v1; defer to the general memory budget

Cohort materialization (both alias caches per A4 and inline `IN QUERY` subqueries per C2) does **not** have a dedicated size cap in v1. Memory pressure is enforced by the general memory budget layer (TASK-501).

**Why:** adding a cohort-specific cap would either (a) duplicate the cross-cutting memory-budget work or (b) force a pre-mature "magic number" for cohort size that the broader budget system will subsume anyway. The user explicitly chose to defer to the general mechanism rather than layering a local cap.

**User-facing caveat (must be documented):** a cohort that exceeds the memory budget causes the whole query to fail with the standard out-of-budget error. There is no "silently truncated cohort" failure mode — the only possible outcomes are "cohort fits, query succeeds" and "cohort doesn't fit, query errors."

**Implication for TASK-501:** the memory-budget model must account for the cohort's materialized `HashSet` as a single-cost node; document it as a named memory consumer.

### A7. Alias cycle detection — bind-time DFS

Cycle detection runs at bind time, before any planning or execution. The binder walks the alias reference graph in DFS order starting from the first alias used by the terminal pipeline; on re-entry into a node currently on the DFS stack, the binder emits `TypeError::AliasCycle { path: Vec<String> }` naming the cycle path.

**Why:** cycles are reported before any real work starts, with a clean user-facing error message that names the offending names. Given A2 (top-down order), the DFS stack is exactly the current alias's transitive-reference chain.

### A8. Alias result shape — no requirement at definition; checked at use site

An alias is a named pipeline with no shape requirement at definition time. The shape check is deferred to the use site:

- `x IN alias` requires the alias's pipeline to produce exactly one column whose type is compatible with `x`.
- `(x, y) IN alias` requires exactly two columns matching the tuple arity and types.
- Hypothetical future non-`IN` alias uses (e.g., alias-as-source in v2) will impose their own shape constraints.

The alias's output is whatever its terminal pipe produces. Aliases **do not have to result in entity keys** — they can return any tuple shape as long as the use site is compatible. This is important because C1's cohort hash key is the whole tuple, not specifically an entity id.

**Why:** the cohort concept is a *use* of aliases; aliases themselves are just named pipelines. Keeping definition-time shape-agnostic lets the same alias be reused across different `IN` shapes and across future non-`IN` use sites without redefinition.

### A9. Multi-column `IN` matching — positional

The tuple on the LHS of `IN` binds to the subquery output **positionally**: first LHS element to first output column, second to second, etc. Column names in the subquery output are ignored for matching purposes.

**Why:** matches standard SQL multi-column IN semantics, handles computed LHS tuples (`(user_id, DATE_TRUNC(ts, '1d'))`) where the LHS has no natural name, and removes a cross-check between user-written expressions.

**Must be documented in `query-language.md` §17.4:** the positional rule explicitly, with an example where LHS and subquery output use different column names and the binding still works.

### A10. `IN alias` vs `IN QUERY (pipeline)` — semantically equivalent

The two forms are semantically identical:

```
x = <pipeline>
events | WHERE y IN x
-- is equivalent to:
events | WHERE y IN QUERY (<pipeline>)
```

The caching rule from A4 applies to both: when the planner detects multiple references to the same logical cohort (whether via alias or via identical-subquery CSE), it materializes once and reuses. The only user-visible difference between the two forms is the syntactic reuse convenience of the alias name.

**Why:** users should be able to refactor between the two forms without worrying about semantic changes. Caching is orthogonal to surface form.

**Implication for the planner:** common-subexpression elimination over `IN QUERY` subqueries is a consequence of A4+A10 being consistent; the planner should normalize both forms to the same internal cohort representation before caching decisions.

## Decisions — Block B: Entity-aligned source JOIN

### B1. Same-`ts` cross-table event ordering — source-expression order + `__seq_id`

When events from different tables share the same `ts` for the same entity, the merged stream orders them by:

```
(ts, table_order_in_source_expression, __seq_id)
```

`table_order_in_source_expression` is the 0-indexed position the table appears in the JOIN clause (leftmost table = 0). `__seq_id` is the per-table sequence identifier already used to break ties within a single table.

**Why:** matches what the user wrote, remains stable under catalog-level renames, and avoids the surprise of alphabetical ordering silently flipping when a new table is introduced.

### B2. Time-range widening across joined tables — uniform across all tables

The planner's scan-extension rule (aggregate of operator-driven lookbacks: SESSIONIZE gap, MATCH lookback, ATTRIBUTE window per TASK-406 §5, RETENTION bracket max) is computed once for the whole pipeline and applied **uniformly** to every joined table's scan range.

**Why:** simplest rule that preserves correctness. Per-operator-arg widening (only the table actually named in the operator's event ref gets widened) is an optimization deferrable to Wave 5 without semantic change. The extra data read is bounded by `max(window, gap, bracket_max)` which is small relative to overall query scope.

### B3. JOIN + SAMPLE — hash the entity-key value, not the column name

SAMPLE's entity hash is computed over the **value** of the entity key, not over any particular column's name. Because the join merges on entity-key value across tables (§19.5 pt 1 allows the column names to differ but requires the values to match by definition), the hash result is identical for both tables' rows belonging to the same entity. SAMPLE keeps or drops the whole cross-table entity stream atomically.

**Why:** SAMPLE's definition (§14.2) is "entity-level, not event-level" — the entity unit is the shared key *value*. Hashing the value is the only consistent rule; hashing a specific column name would require picking one table's column arbitrarily.

### B4. JOIN + DELETE — disallowed in v1

`DELETE FROM events JOIN purchases WHERE ...` is a parser error in v1. Cross-table DELETE with a joined source expression is not supported. Users express cross-table deletes as sequential single-table DELETEs using the `IN QUERY` / `IN alias` forms:

```
-- Tombstone purchase rows for users who churned
DELETE FROM purchases
WHERE user_id IN QUERY (events | WHERE event_type = 'churn' | SELECT user_id)
ALLOW SCAN
```

**Why:** cross-table DELETE is semantically murky (which table's rows get tombstoned for a predicate spanning both?); every case users actually need is expressible as a sequence of single-table DELETEs. Keep the v1 surface tight.

**Implication for TASK-433 (DELETE parser):** reject the `JOIN` keyword after `DELETE FROM <table>` with a clear error message.

### B5. Entity-key type mismatch — plan-time error

A JOIN between tables whose entity-key columns have different `BqlType`s (e.g., `String` vs `Int`) is a **plan-time** error. The binder has the catalog and can check this before any execution.

**Why:** every piece of information needed is available at bind time. Runtime discovery is strictly worse for UX; fail early.

### B6. JOIN physical operator shape — n-ary `MergeSources` operator

JOIN is implemented as a single **n-ary `MergeSources` operator** that reads from N independent sorted per-shard scans (one per joined table) and emits a unified entity-sorted event stream. Downstream operators see a single logical event stream and remain table-agnostic.

Per-shard execution:
1. Each joined table contributes one scan per shard, producing an entity-sorted stream.
2. `MergeSources` performs a per-shard k-way merge over the N scans, ordered by `(entity_id, ts, table_order, __seq_id)` per B1.
3. The merged stream feeds whatever operator pipeline follows (MATCH, SESSIONIZE, etc.).

This is **not** a binary merge chained left-to-right (extra buffer layers), and **not** a "operators directly consume multiple sorted streams" model (pollutes every downstream operator with multi-stream awareness).

**Why:** models the actual physical reality (n-way merge at the source layer), isolates multi-table complexity in one place, keeps downstream operators table-agnostic. Reuses the same k-way merge algorithm already present in the compaction path.

**Implication for TASK-425 (lowering):** source expression with K tables → a `MergeSources { tables: Vec<ScanDesc>, order: Vec<(ColumnRef, Direction)> }` physical node. Single-table source expression → ordinary `Scan` (no wrapping).

### B7. Source-table discriminator — `__source_table_id: i8` enum with dictionary registry

Merged rows carry a discriminator column named `__source_table_id` typed as `Int8` (or Arrow `Int8` natively). The mapping from `i8` values to table names lives in an out-of-band dictionary registry that the planner builds for the pipeline and attaches to the physical plan — downstream operators look up table names by id when they need them (e.g., for display in EXPLAIN).

Table-qualified references (`events.signup`, `purchases.amount`) resolve through the registry: the planner rewrites them into `(table_id = N) AND <column>` predicates internally.

Dictionary size is bounded by table count in the JOIN, practically ≤ 4.

**Why:** `i8` is the smallest representation that comfortably covers realistic JOIN widths. A dictionary registry (vs `Dictionary<Int8, Utf8>` per column) avoids duplicating the mapping in every batch. Keeps the row-level per-byte cost minimal in the hot path while preserving display / EXPLAIN ergonomics through registry lookup.

**Implication for TASK-424 (planner):** `MergeSources` carries the `table_id → name` map in its descriptor; `OperatorSchema` exposes `__source_table_id` as a non-nullable `Int8` column. EXPLAIN renders the map alongside the merge node.

### B8. `__source_table_id` absent in single-table queries

Single-table source expressions produce events without a `__source_table_id` column. The column is introduced exclusively by `MergeSources`. Operators must branch on schema presence when behavior depends on the column; this is a schema-level (not hot-path) branch.

**Why:** avoids polluting the single-table common case with a constant column that is memory and schema noise. The grep-able invariant "`__source_table_id` only exists after `MergeSources`" simplifies downstream operator reasoning.

### B9. Aliases referencing joined-source pipelines — no new rules

An alias defined against a joined-source pipeline and consumed via `IN` in another query (single- or multi-table) works through the existing machinery:

- The alias's inner pipeline is compiled with its own JOIN (producing `__source_table_id` etc. internally).
- Its output is a materialized cohort matching the shape the RHS of `IN` demands.
- The outer query consumes it via SubqueryFilter — exactly as for any other cohort. The fact that the alias internally joined multiple tables is opaque.

**Why:** alias result shape is an output-only concern (A8); the inner pipeline's source shape is invisible to consumers.

### B10. FUNNEL / RETENTION inside JOIN — step-name before table qualifier

Within a MATCH step (or the desugared forms of FUNNEL/RETENTION), when both a step name and a table qualifier appear, the order is `step_name: table.event_type`:

```
s: events.signup
purchase_step: purchases.purchase WHERE purchases.amount > 100
```

This restates §19.1 for clarity; both prefixes are individually optional but the relative order is fixed when both are present.

**Why:** already pinned by §19.1; the design doc should simply cite.

## Decisions — Block C: SubqueryFilter physical execution

### C1. SubqueryFilter execution — hash-set probe

SubqueryFilter materializes the subquery result into a `HashSet<Tuple>` keyed by the LHS shape (single-element tuple for `x IN (...)`, N-element tuple for `(x1, ..., xN) IN (...)`). The outer stream is then probed against the set row-by-row.

The tuple hash key is whatever shape the RHS produces per A8 — there is no requirement that the key be an entity id specifically. Multi-column cohorts with mixed types (e.g., `(user_id: String, day: Timestamp)`) hash over the composite tuple.

**Why:** simple, fast for cohorts bounded by the memory budget (A6). Streaming semi-join (both sides sorted) is a Wave 5 optimization if benchmarks show hash-set probe pinching on very large cohorts.

**Implication for TASK-424 (`SubqueryFilter` physical node):** the physical node carries the materialized cohort representation (`Arc<HashSet<Tuple>>` or equivalent) populated at query start per C2.

### C2. Cohort materialization timing — at query start

All cohorts referenced by the terminal pipeline (via alias expansion or `IN QUERY`) are materialized **at query start**, before any outer-query scan begins. This enables two properties:

1. Cohort sizes are known before downstream planning decisions commit.
2. The entity-id component of a cohort predicate can be pushed into the source scan as a pre-probe filter (C3), letting the storage layer skip whole shards or segments that contain no cohort entities.

Cohorts that are mutually independent (no reference chain between them) may materialize in parallel at the planner's discretion.

**Why:** the pushdown benefit is significant for cohort-filtered queries over large tables. Lazy materialization offers no compensating benefit — the cohort has to be materialized before the outer query can filter on it regardless.

**Implication for TASK-425 (lowering):** the logical → physical lowering identifies all cohort references in the pipeline, materializes them as a DAG of `SubqueryFilter` inputs, and wires them into the outer pipeline's filter operators. The planner's overall execution graph starts with a cohort-materialization phase that fully completes before the main pipeline runs.

### C3. Entity-id component pushdown for multi-column cohorts

When a multi-column `IN` predicate includes the entity-key column (e.g., `(entity_id, day) IN QUERY (...)`), the **entity-id component** is extracted and pushed down to the source scan as a hash-set filter. Other tuple components (`day` in this example) are applied at the filter operator after the scan.

- Full shard skipping: shards containing no cohort entities are skipped entirely.
- Segment skipping: segments whose entity-id range doesn't overlap the cohort set are skipped.
- Post-scan filtering: rows that survive the entity-id filter are then probed against the full tuple for the other components.

Full multi-column pushdown via tuple-bloom or multi-key predicate is a Wave 5 optimization; not v1.

**Why:** entity-id pushdown is the high-value case (storage-layer shard/segment pruning). Multi-component pushdown adds complexity disproportionate to its gain for typical cohort shapes.

## Follow-on implications to propagate

These are consequences worth calling out for downstream tasks:

- **TASK-423 (parser: alias definitions)** — accepts `(alias_def)* pipeline` top-level grammar per A2; rejects forward references with span-accurate diagnostic; duplicate alias name resolution follows §18.1's "last wins" (shadow-permitted) rule.
- **TASK-425 (AST → logical lowering, Wave 4)** — implements A2 top-down binding; A7 cycle detection via DFS producing `TypeError::AliasCycle`; A4 / A10 cohort normalization (aliases and subqueries with identical inner plans share one materialized cohort); B6 `MergeSources` lowering for JOIN source expressions; B7 `__source_table_id` injection; B2 uniform scan-range widening across joined tables; C2 query-start cohort materialization phase.
- **TASK-424 (planner: plan variants for Wave 4)** — new `MergeSources { tables, order, table_id_map }` physical node; `SubqueryFilter { lhs_tuple, cohort: Arc<HashSetCohort>, output_schema }` physical node per C1.
- **TASK-451 (engine: `IN QUERY` / bare `IN alias`)** — owns A4 caching as a planner-level materialization; A8 shape-check at use site producing `TypeError::IncompatibleCohortShape`; A9 positional multi-column binding; A10 equivalence between `IN alias` and `IN QUERY`; C1 hash-set probe execution; C3 entity-id pushdown signaling.
- **TASK-452 (engine: entity-aligned source JOIN)** — implements B6 `MergeSourcesOperator`; B1 cross-table tie-breaking; B3 value-based SAMPLE hash; B5 plan-time entity-key-type check (wired in TASK-425); B7 discriminator column semantics; B10 step-name + table qualifier ordering.
- **TASK-433 (DELETE parser)** — rejects `JOIN` after `DELETE FROM <table>` per B4 with a clear error message pointing users to `IN QUERY` / `IN alias`.
- **TASK-430 (SAMPLE pushdown)** — honors B3's value-based hash when the source is a `MergeSources`; extended for joined sources as noted in the task description (`Joined-source SAMPLE correctness is extended by TASK-436`).
- **TASK-501 (memory budget)** — accounts for the cohort `HashSet` as a named memory consumer per A6; documents the cohort-exceeds-budget error path user-facing.
- **`query-language.md`**:
  - **§17.4**: document A9's positional multi-column binding with an example where LHS and subquery column names differ.
  - **§18.1**: restate A1 (event-type and column names are not reserved against alias shadowing; only keywords and table names are) and A2 (forward refs forbidden).
  - **§18.1**: document A3 (submission = one `execute` call; engine is alias-stateless) and A4 (always-cached within a submission).
  - **§18.3**: A5 (no cross-submission caching) as the corollary of §18.3's "session-scoped" line.
  - **§17 / §18**: add the A6 caveat — exceeding the memory budget errors the whole query.
  - **§18**: add A10 equivalence statement between `IN alias` and `IN QUERY`.
  - **§19.2**: restate B4 disallowing DELETE+JOIN with the `IN QUERY` workaround example.
  - **§19.5**: cite B5 plan-time error on entity-key-type mismatch.
- **`type-system.md`**:
  - **§6.9**: document A8 (alias shape is use-site enforced) and A9 positional tuple binding.
  - **New or extended section** for `__source_table_id: Int8` non-nullable column introduced by `MergeSources` per B7/B8.
- **`planner-pipeline.md`** §5.2: `SubqueryFilter` physical shape per C1 / C2 (carries `Arc<HashSetCohort>`, materialized at query start, with entity-id pushdown per C3).
