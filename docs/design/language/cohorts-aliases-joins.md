# Cohort Materialization, Alias Binding, and Entity Joins

> **Status**: DRAFT
> **Task**: TASK-407
> **Depends on**: none
> **Depended on by**: TASK-423 (parser alias defs), TASK-424 (plan variants), TASK-425 (Wave 4 lowering), TASK-430 (SAMPLE pushdown), TASK-433 (DELETE parser), TASK-436 (joined-source scan runtime), TASK-437 (cohort subquery runtime), TASK-451 (parser `IN QUERY` / `IN alias`), TASK-452 (parser entity-aligned source JOIN), TASK-501 (memory budget)

---

## 1. Scope and Purpose

This document is the design anchor for three language/runtime features that share binding and planning infrastructure but are otherwise independent:

- **Block A — Aliases and cohorts (language):** `alias = pipeline`, alias scoping, forward-reference rules, cycle detection, per-submission caching, and cohort size accounting.
- **Block B — Entity-aligned source `JOIN`:** multi-table source expressions, merged event ordering, scan-range widening, discriminator columns, and interaction with SAMPLE / DELETE.
- **Block C — `SubqueryFilter` physical execution:** cohort materialization timing, hash-set probe semantics, and entity-id pushdown for multi-column cohorts.

Blocks A and C together define how `IN QUERY (...)` and bare `IN alias` resolve from surface syntax through planning to runtime. Block B defines how `JOIN` in the source expression produces a unified entity-sorted event stream for downstream operators.

### 1.1 Cross-References

Surface syntax for all three blocks is specified in `query-language.md`:
- **Aliases**: `query-language.md` § 18 (alias semantics), § 18.1--18.4 (scoping, resolution, structure)
- **`IN` forms**: `query-language.md` § 17 (set membership), § 17.1--17.4 (literal list, `IN QUERY`, alias reference, multi-column)
- **Source `JOIN`**: `query-language.md` § 19 (cross-table entity joins), § 19.1--19.5 (qualification, self-joins, FUNNEL/RETENTION, rationale, schema)
- **Grammar**: `query-language.md` § 26 (`query := (alias_def)* pipeline`, `in_rhs`, `source`, `tuple_expr`)
- **Parameterized aliases**: `query-language.md` § 30.5 (out of scope for v1)

Type rules:
- `type-system.md` § 6.9 (`IN` subquery filtering output schema and type rules)

Planner integration:
- `planner-pipeline.md` § 4.7 (validation sequence: source resolution, alias resolution, operator folding)
- `planner-pipeline.md` § 4.8 (alias resolution: top-down binding, cycle detection, normalization)
- `planner-pipeline.md` § 5.1 (`SubqueryFilter` logical plan node)
- `planner-pipeline.md` § 5.4 (schema computation: `SubqueryFilter` output identical to outer input)

Execution model:
- `execution-model.md` § 7.3 (cohort execution: hash-set materialization, entity-level semi-join)

---

## 2. Block A: Aliases and Cohorts (Language)

### 2.1 Grammar Recap

From `query-language.md` § 26:

```
query     := (alias_def)* pipeline
alias_def := identifier "=" pipeline
```

A BQL submission is zero or more alias definitions followed by a terminal pipeline. Alias definitions are inert until referenced. The terminal pipeline is what executes and produces output.

### 2.2 Alias Shadowing (A1)

Aliases may not shadow reserved keywords or table names (`query-language.md` § 18.1). Shadowing other aliases is permitted -- the most recent definition wins (last-wins resolution).

Event-type names are **not** forbidden as alias names. Event types are runtime string values inside a table rather than grammar-reserved tokens. The grammar has no clean hook for preventing the collision, and disambiguation always occurs through the table-qualified form (`events.signup`) in multi-table contexts or through the lack-of-alias-in-pattern-positions in single-table contexts.

Column names are likewise not forbidden because column references are never bare in alias-reference positions.

**Rationale.** `query-language.md` § 18.1's existing rule already covers the real collision surface (keywords + tables). Event-type names are values rather than identifiers -- forcing the grammar to know the catalog's event-type vocabulary at alias-def time would be a layering violation.

### 2.3 Forward References Forbidden (A2)

Aliases must be defined before they are referenced, in source order within a submission. The binder's first pass walks alias definitions top-down and resolves each RHS against the aliases already bound. A reference to an alias not yet defined is a bind-time error.

**Rationale.** Top-down order matches users' visual mental model, simplifies cycle detection to a trivial stack check (§ 2.8), and removes the "mutual recursion" edge case which would have to be rejected anyway. No compelling use case for forward references in v1.

### 2.4 Submission Boundary (A3)

An engine `execute` call takes a single text string containing `(alias_def)* pipeline`. Aliases defined in submission N are **not** visible in submission N+1. The engine holds no alias session state.

Any cross-submission alias persistence (REPL history, multi-statement scripts) is a CLI-layer concern implemented by prepending buffered alias definitions to the next submission's text. The CLI can expose whatever UX model it wants without changing the engine contract.

**Rationale.** Keeps the engine stateless and the alias-scope semantics trivially reproducible. Persistent aliases / views are already deferred to v2 (`query-language.md` § 18.3, § 30.5).

### 2.5 Per-Submission Alias Caching (A4)

If an alias is referenced two or more times within a single submission, its pipeline is **executed exactly once** and the materialized result is reused across all references. Caching is not optional and not cost-based in v1.

**Rationale.** Users reach for aliases specifically to reuse expensive work. Always-cached is the least surprising default and matches CTE caching behavior in most SQL engines. The memory bound comes from the general memory budget (§ 2.7).

### 2.6 Cross-Submission Caching (A5)

Cohort materializations live for the duration of a single `execute` call and are dropped when the call returns. Submission N+1 on the same logical session does not inherit materializations from submission N.

Follows directly from § 2.4 -- the engine is alias-stateless, so there is nowhere to hold cross-submission state. CLI-layer caching (if ever added) is orthogonal.

### 2.7 Cohort Size Accounting (A6)

Cohort materialization (both alias caches per § 2.5 and inline `IN QUERY` subqueries per § 4.1) does **not** have a dedicated size cap in v1. Memory pressure is enforced by the general memory budget layer (TASK-501).

A cohort that exceeds the memory budget causes the whole query to fail with the standard out-of-budget error. There is no "silently truncated cohort" failure mode -- the only possible outcomes are "cohort fits, query succeeds" and "cohort doesn't fit, query errors."

**Implication for TASK-501:** the memory-budget model must account for the cohort's materialized `HashSet` as a named memory consumer.

**User-facing documentation requirement:** the exceeds-budget error path must be documented in `query-language.md` § 17 / § 18.

### 2.8 Alias Cycle Detection (A7)

Cycle detection runs at bind time, before any planning or execution. The binder walks the alias reference graph in DFS order starting from the first alias used by the terminal pipeline. On re-entry into a node currently on the DFS stack, the binder emits:

```rust
TypeError::AliasCycle { path: Vec<String> }
```

naming the cycle path (e.g., `["a", "b", "a"]`).

Given § 2.3 (top-down order, forward references forbidden), the DFS stack is exactly the current alias's transitive-reference chain. A cycle can only arise when alias `X` references alias `Y` which references `X`, and both were defined before they are used -- but the forward-reference prohibition means the second reference would already have failed at bind time. The DFS check is a belt-and-suspenders safety net.

**Rationale.** Cycles are reported before any real work starts, with a clean user-facing error message that names the offending names.

### 2.9 Alias Result Shape (A8)

An alias is a named pipeline with no shape requirement at definition time. The shape check is deferred to the use site:

- `x IN alias` requires the alias's pipeline to produce exactly one column whose type is compatible with `x`.
- `(x, y) IN alias` requires exactly two columns matching the tuple arity and types.
- Hypothetical future non-`IN` alias uses (e.g., alias-as-source in v2) will impose their own shape constraints.

The alias's output is whatever its terminal pipe produces. Aliases **do not have to result in entity keys** -- they can return any tuple shape as long as the use site is compatible. This is important because the cohort hash key (§ 4.1) is the whole tuple, not specifically an entity id.

**Rationale.** The cohort concept is a *use* of aliases; aliases themselves are just named pipelines. Keeping definition-time shape-agnostic lets the same alias be reused across different `IN` shapes and across future non-`IN` use sites without redefinition.

### 2.10 Multi-Column `IN` Matching (A9)

The tuple on the LHS of `IN` binds to the subquery output **positionally**: first LHS element to first output column, second to second, etc. Column names in the subquery output are ignored for matching purposes.

**Rationale.** Matches standard SQL multi-column `IN` semantics, handles computed LHS tuples (`(user_id, DATE_TRUNC(ts, '1d'))`) where the LHS has no natural name, and removes a cross-check between user-written expressions.

**Documentation requirement for `query-language.md` § 17.4:** the positional rule must be stated explicitly, with an example where LHS and subquery output use different column names and the binding still works.

### 2.11 `IN alias` vs `IN QUERY (pipeline)` Equivalence (A10)

The two forms are semantically identical:

```bql
x = <pipeline>
events | WHERE y IN x
-- is equivalent to:
events | WHERE y IN QUERY (<pipeline>)
```

The caching rule from § 2.5 applies to both: when the planner detects multiple references to the same logical cohort (whether via alias or via identical-subquery CSE), it materializes once and reuses. The only user-visible difference between the two forms is the syntactic reuse convenience of the alias name.

**Rationale.** Users should be able to refactor between the two forms without worrying about semantic changes. Caching is orthogonal to surface form.

**Implication for the planner:** common-subexpression elimination over `IN QUERY` subqueries is a consequence of § 2.5 + § 2.11 being consistent; the planner should normalize both forms to the same internal cohort representation before caching decisions.

---

## 3. Block B: Entity-Aligned Source JOIN

### 3.1 Grammar Recap

From `query-language.md` § 26:

```
source := name time_range? (JOIN name)*
```

Multiple tables join by repeating `JOIN`. The time range applies to the first table; other tables are implicitly time-bounded by the same range plus the planner's scan-extension rule (`query-language.md` § 3.2).

Table-qualified references are mandatory inside JOINs (`query-language.md` § 19.1). Self-joins are forbidden (§ 19.2). Both tables must have the same entity-key **type** (§ 19.5).

### 3.2 Same-Timestamp Cross-Table Event Ordering (B1)

When events from different tables share the same `ts` for the same entity, the merged stream orders them by:

```
(ts, table_order_in_source_expression, __seq_id)
```

`table_order_in_source_expression` is the 0-indexed position the table appears in the JOIN clause (leftmost table = 0). `__seq_id` is the per-table sequence identifier already used to break ties within a single table.

**Rationale.** Matches what the user wrote, remains stable under catalog-level renames, and avoids the surprise of alphabetical ordering silently flipping when a new table is introduced.

### 3.3 Time-Range Widening Across Joined Tables (B2)

The planner's scan-extension rule (aggregate of operator-driven lookbacks: SESSIONIZE gap, MATCH lookback, ATTRIBUTE window per `operators/attribute.md` § 5, RETENTION bracket max) is computed once for the whole pipeline and applied **uniformly** to every joined table's scan range.

**Rationale.** Simplest rule that preserves correctness. Per-operator-arg widening (only the table actually named in the operator's event ref gets widened) is an optimization deferrable to Wave 5 without semantic change. The extra data read is bounded by `max(window, gap, bracket_max)` which is small relative to overall query scope.

### 3.4 JOIN + SAMPLE Interaction (B3)

SAMPLE's entity hash is computed over the **value** of the entity key, not over any particular column's name. Because the join merges on entity-key value across tables (`query-language.md` § 19.5 pt 1 allows the column names to differ but requires the values to match by definition), the hash result is identical for both tables' rows belonging to the same entity. SAMPLE keeps or drops the whole cross-table entity stream atomically.

**Rationale.** SAMPLE's definition (`query-language.md` § 14.2) is "entity-level, not event-level" -- the entity unit is the shared key *value*. Hashing the value is the only consistent rule; hashing a specific column name would require picking one table's column arbitrarily.

### 3.5 JOIN + DELETE Disallowed (B4)

`DELETE FROM events JOIN purchases WHERE ...` is a parser error in v1. Cross-table DELETE with a joined source expression is not supported. Users express cross-table deletes as sequential single-table DELETEs using the `IN QUERY` / `IN alias` forms:

```bql
-- Tombstone purchase rows for users who churned
DELETE FROM purchases
WHERE user_id IN QUERY (events | WHERE event_type = 'churn' | SELECT user_id)
ALLOW SCAN
```

**Rationale.** Cross-table DELETE is semantically murky (which table's rows get tombstoned for a predicate spanning both?). Every case users actually need is expressible as a sequence of single-table DELETEs.

**Implication for TASK-433 (DELETE parser):** reject the `JOIN` keyword after `DELETE FROM <table>` with a clear error message pointing users to `IN QUERY` / `IN alias`.

### 3.6 Entity-Key Type Mismatch (B5)

A JOIN between tables whose entity-key columns have different `BqlType`s (e.g., `String` vs `Int`) is a **plan-time** error. The binder has the catalog and can check this before any execution.

**Rationale.** Every piece of information needed is available at bind time. Runtime discovery is strictly worse for UX; fail early.

### 3.7 Physical Operator Shape: N-ary `MergeSources` (B6)

JOIN is implemented as a single **n-ary `MergeSources` operator** that reads from N independent sorted per-shard scans (one per joined table) and emits a unified entity-sorted event stream. Downstream operators see a single logical event stream and remain table-agnostic.

**Per-shard execution:**
1. Each joined table contributes one scan per shard, producing an entity-sorted stream.
2. `MergeSources` performs a per-shard k-way merge over the N scans, ordered by `(entity_id, ts, table_order, __seq_id)` per § 3.2.
3. The merged stream feeds whatever operator pipeline follows (MATCH, SESSIONIZE, etc.).

This is **not** a binary merge chained left-to-right (extra buffer layers), and **not** a "operators directly consume multiple sorted streams" model (pollutes every downstream operator with multi-stream awareness).

**Rationale.** Models the actual physical reality (n-way merge at the source layer), isolates multi-table complexity in one place, keeps downstream operators table-agnostic. Reuses the same k-way merge algorithm already present in the compaction path.

**Implication for TASK-425 (lowering):** source expression with K tables produces a `MergeSources { tables: Vec<ScanDesc>, order: Vec<(ColumnRef, Direction)> }` physical node. Single-table source expression produces an ordinary `Scan` (no wrapping).

### 3.8 Source-Table Discriminator Column (B7)

Merged rows carry a discriminator column:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `__source_table_id` | `Int8` | no | 0-indexed position in the JOIN clause |

The mapping from `i8` values to table names lives in an out-of-band dictionary registry that the planner builds for the pipeline and attaches to the physical plan. Downstream operators look up table names by id when needed (e.g., for display in EXPLAIN).

Table-qualified references (`events.signup`, `purchases.amount`) resolve through the registry: the planner rewrites them into `(table_id = N) AND <column>` predicates internally.

Dictionary size is bounded by table count in the JOIN, practically <= 4.

**Rationale.** `i8` is the smallest representation that comfortably covers realistic JOIN widths. A dictionary registry (vs `Dictionary<Int8, Utf8>` per column) avoids duplicating the mapping in every batch. Keeps the row-level per-byte cost minimal in the hot path while preserving display / EXPLAIN ergonomics through registry lookup.

**Implication for TASK-424 (planner):** `MergeSources` carries the `table_id -> name` map in its descriptor; `OperatorSchema` exposes `__source_table_id` as a non-nullable `Int8` column. EXPLAIN renders the map alongside the merge node.

### 3.9 `__source_table_id` Absent in Single-Table Queries (B8)

Single-table source expressions produce events without a `__source_table_id` column. The column is introduced exclusively by `MergeSources`. Operators must branch on schema presence when behavior depends on the column; this is a schema-level (not hot-path) branch.

**Rationale.** Avoids polluting the single-table common case with a constant column that is memory and schema noise. The grep-able invariant "`__source_table_id` only exists after `MergeSources`" simplifies downstream operator reasoning.

### 3.10 Aliases Referencing Joined-Source Pipelines (B9)

An alias defined against a joined-source pipeline and consumed via `IN` in another query (single- or multi-table) works through the existing machinery:

- The alias's inner pipeline is compiled with its own JOIN (producing `__source_table_id` etc. internally).
- Its output is a materialized cohort matching the shape the RHS of `IN` demands.
- The outer query consumes it via SubqueryFilter -- exactly as for any other cohort. The fact that the alias internally joined multiple tables is opaque.

**Rationale.** Alias result shape is an output-only concern (§ 2.9); the inner pipeline's source shape is invisible to consumers.

### 3.11 FUNNEL / RETENTION Inside JOIN: Step-Name Before Table Qualifier (B10)

Within a MATCH step (or the desugared forms of FUNNEL/RETENTION), when both a step name and a table qualifier appear, the order is `step_name: table.event_type`:

```bql
s: events.signup
purchase_step: purchases.purchase WHERE purchases.amount > 100
```

This restates `query-language.md` § 19.1 for clarity; both prefixes are individually optional but the relative order is fixed when both are present.

---

## 4. Block C: SubqueryFilter Physical Execution

### 4.1 Hash-Set Probe Execution (C1)

`SubqueryFilter` materializes the subquery result into a `HashSet<Tuple>` keyed by the LHS shape:
- Single-element tuple for `x IN (...)`.
- N-element tuple for `(x1, ..., xN) IN (...)`.

The outer stream is then probed against the set row-by-row.

The tuple hash key is whatever shape the RHS produces per § 2.9 -- there is no requirement that the key be an entity id specifically. Multi-column cohorts with mixed types (e.g., `(user_id: String, day: Timestamp)`) hash over the composite tuple.

**Rationale.** Simple, fast for cohorts bounded by the memory budget (§ 2.7). Streaming semi-join (both sides sorted) is a Wave 5 optimization if benchmarks show hash-set probe pinching on very large cohorts.

**Implication for TASK-424 (`SubqueryFilter` physical node):** the physical node carries the materialized cohort representation (`Arc<HashSet<Tuple>>` or equivalent) populated at query start per § 4.2.

### 4.2 Cohort Materialization Timing (C2)

All cohorts referenced by the terminal pipeline (via alias expansion or `IN QUERY`) are materialized **at query start**, before any outer-query scan begins. This enables two properties:

1. Cohort sizes are known before downstream planning decisions commit.
2. The entity-id component of a cohort predicate can be pushed into the source scan as a pre-probe filter (§ 4.3), letting the storage layer skip whole shards or segments that contain no cohort entities.

Cohorts that are mutually independent (no reference chain between them) may materialize in parallel at the planner's discretion.

**Rationale.** The pushdown benefit is significant for cohort-filtered queries over large tables. Lazy materialization offers no compensating benefit -- the cohort has to be materialized before the outer query can filter on it regardless.

**Implication for TASK-425 (lowering):** the logical-to-physical lowering identifies all cohort references in the pipeline, materializes them as a DAG of `SubqueryFilter` inputs, and wires them into the outer pipeline's filter operators. The planner's overall execution graph starts with a cohort-materialization phase that fully completes before the main pipeline runs.

### 4.3 Entity-ID Component Pushdown for Multi-Column Cohorts (C3)

When a multi-column `IN` predicate includes the entity-key column (e.g., `(entity_id, day) IN QUERY (...)`), the **entity-id component** is extracted and pushed down to the source scan as a hash-set filter. Other tuple components (`day` in this example) are applied at the filter operator after the scan.

Pushdown levels:

| Level | Mechanism |
|-------|-----------|
| Full shard skipping | Shards containing no cohort entities are skipped entirely |
| Segment skipping | Segments whose entity-id range doesn't overlap the cohort set are skipped |
| Post-scan filtering | Rows surviving the entity-id filter are probed against the full tuple |

Full multi-column pushdown via tuple-bloom or multi-key predicate is a Wave 5 optimization; not v1.

**Rationale.** Entity-id pushdown is the high-value case (storage-layer shard/segment pruning). Multi-component pushdown adds complexity disproportionate to its gain for typical cohort shapes.

---

## 5. Planner Integration

This section summarizes how the three blocks integrate into the planner's existing pipeline.

### 5.1 Validation Sequence Extension

The planner's validation sequence (`planner-pipeline.md` § 4.7) extends as follows for Wave 4:

1. **Resolve source.** Look up the primary table and every `JOIN` table in the catalog. For joins:
   - Validate entity-key type compatibility (§ 3.6).
   - Build the `table_id -> name` registry (§ 3.8).
   - Produce a `MergeSources` physical node (§ 3.7) instead of a plain `Scan`.
   - Inject `__source_table_id` into the combined schema (§ 3.8).
2. **Resolve aliases** top-down (§ 2.3). For each alias:
   - Recursively plan the alias body through the same validation sequence.
   - Check for cycles via DFS (§ 2.8).
   - Cache the planned alias body for reuse (§ 2.5).
3. **Fold operators left to right.** For `WHERE` operators containing `IN` expressions:
   - Normalize `IN alias` and `IN QUERY (pipeline)` to the same internal cohort representation (§ 2.11).
   - Validate shape compatibility at use site (§ 2.9).
   - Validate positional type matching for multi-column `IN` (§ 2.10).
   - Register the cohort for query-start materialization (§ 4.2).

### 5.2 Cohort Materialization Phase

Before the main pipeline executes, the planner orchestrates a cohort-materialization phase:

1. Collect all cohort references from the planned pipeline.
2. Deduplicate: aliases referenced multiple times share one materialization; identical `IN QUERY` subqueries (after normalization) also share.
3. Build a DAG of cohort dependencies (alias A may reference alias B in its body).
4. Execute the DAG bottom-up. Independent cohorts may execute in parallel.
5. Wire each materialized `HashSet<Tuple>` into the corresponding `SubqueryFilter` physical node.
6. For cohorts that include the entity-key column, extract the entity-id set and attach it to the scan as a pushdown predicate (§ 4.3).

### 5.3 Schema Computation for New Nodes

| Node | Output Schema |
|------|--------------|
| `MergeSources` | Union of all joined tables' schemas, plus `__source_table_id: Int8 NOT NULL` |
| `SubqueryFilter` | Identical to outer input (filter, not transform) |

These follow the schema computation rules in `planner-pipeline.md` § 5.4.

For `MergeSources`, the output entity-key column is always named `entity_id` regardless of the underlying tables' entity-key column names (`query-language.md` § 19.5).

---

## 6. Downstream Task Implications

### 6.1 Parser Tasks

**TASK-423 (parser: alias definitions):**
- Accept `(alias_def)* pipeline` top-level grammar per § 2.3.
  The public `parse()` function returns `Vec<Statement>` where zero or more
  `Statement::DefineAlias` items appear first (in source order) and the
  terminal statement is last; the Vec always has ≥ 1 element.
- Duplicate alias name resolution follows `query-language.md` § 18.1's "last wins"
  (shadow-permitted) rule; the parser records both definitions in source order and
  emits no diagnostic — last-wins enforcement is a bind-time responsibility (TASK-425).
- Bare reserved keywords in alias-name position produce `ParseError::ReservedKeyword`
  with `NameRole::AliasName` at parse time (detectable via 2-token lookahead `kw "="`).
- Forward-reference validation (`TypeError::UndefinedAlias`, error table § 8.1) is a
  **bind-time** concern deferred to TASK-425 — the parser records definitions in source
  order without checking whether alias bodies reference names not yet defined.

**TASK-451 (parser: `IN QUERY` / bare `IN alias`):**
- Add `IN QUERY (pipeline)` and bare `IN alias` forms to the expression grammar.
- Support single-column and tuple cohort keys at the syntax level.
- Emit a diagnostic for empty RHS lists (`x IN ()` is rejected).
- Empty LHS tuples `() IN …` are prevented by the grammar (tuple requires ≥ 2 elements).
- Structural-duplicate detection for LHS tuples (e.g. `(a, a) IN alias`) is
  **deferred to TASK-425** (planner/bind-time). Full name resolution is not
  available at parse time, and the `Name` AST node embeds source spans that
  prevent simple structural equality from working across positions.
- Semantic resolution is downstream in TASK-425 / TASK-437.

**TASK-452 (parser: entity-aligned source JOIN):**
- Parse `source := name time_range? (JOIN name)*` per `query-language.md` § 19.
- Reject self-joins at the syntactic level.
- Accept table-qualified reference surface syntax.
- Planner-level validation of qualification rules is TASK-425.

### 6.2 Planner Tasks

**TASK-424 (planner: plan variants for Wave 4):**
- New `MergeSources { tables, order, table_id_map }` physical node (§ 3.7, § 3.8).
- `SubqueryFilter { lhs_tuple, cohort: Arc<HashSetCohort>, output_schema }` physical node (§ 4.1).
- `OperatorSchema` exposes `__source_table_id` as non-nullable `Int8`.
- EXPLAIN renders the table-id map alongside the merge node.

**TASK-425 (AST-to-logical lowering, Wave 4):**
- Top-down alias binding (§ 2.3).
- Forward-reference detection: reject references to alias names not yet defined
  in source order, producing `TypeError::UndefinedAlias` with span-accurate
  diagnostic (error table § 8.1).
- Cycle detection via DFS producing `TypeError::AliasCycle` (§ 2.8).
- Cohort normalization: aliases and subqueries with identical inner plans share one materialized cohort (§ 2.5, § 2.11).
- `MergeSources` lowering for JOIN source expressions (§ 3.7).
- `__source_table_id` injection (§ 3.8).
- Uniform scan-range widening across joined tables (§ 3.3).
- Query-start cohort materialization phase (§ 4.2).

### 6.3 Runtime Tasks

**TASK-436 (joined-source scan runtime):**
- Implement `MergeSourcesOperator` (§ 3.7).
- Cross-table tie-breaking per § 3.2.
- Value-based SAMPLE hash per § 3.4.
- Discriminator column injection per § 3.8.

**TASK-437 (cohort subquery runtime):**
- Execute `SubqueryFilterPhysical` via hash-set probe (§ 4.1).
  Implemented as `SubqueryFilterOperator` in
  `crates/bqlite-operators/src/cohort.rs`; engine bind step
  materializes the inner subquery at query start in
  `crates/bqlite-engine/src/bind.rs::bind_subquery_filter`.
- Cache alias results per submission (§ 2.5). Implemented as
  `CohortCache` in the engine bind step; cohorts are keyed by
  structural equality of the inner `PhysicalPlan` so two `IN alias`
  references that resolve to the same alias body — and two
  `IN QUERY (...)` whose inner pipelines lower to identical physical
  plans — share one `Arc<CohortHashSet>` per top-level execution
  (§ 2.11). Pinned by the
  `cohort_cache_get_returns_arc_for_equal_plan` test.
- Detect alias cycles (§ 2.8). Owned by the planner's
  `resolve_alias` in `crates/bqlite-planner/src/logical.rs`; by the
  time a `PhysicalPlan` reaches the engine bind step it is
  guaranteed cycle-free, so the runtime walk does not re-check.
- Support single-column and tuple cohort keys (§ 4.1). Single and
  two-column tuple cases are pinned by `cohort.rs` unit tests and
  by end-to-end tests in `bind.rs#tests`.
- Shape-check at use site producing `TypeError::IncompatibleCohortShape` (§ 2.9).
  Owned by the planner (`apply_subquery_filter` in
  `bqlite-planner/src/logical.rs`); the runtime asserts arity
  consistency at construction as a defense-in-depth check.
- Positional multi-column binding (§ 2.10). The runtime evaluates
  LHS expressions in source order and probes the cohort tuple
  positionally; column names in the cohort's output schema are
  ignored by the probe.
- ~~Entity-id pushdown signaling to the scan layer (§ 4.3).~~
  **Deferred** — see § 6.3.1 for the rationale and follow-up
  plan. The post-scan probe is correct without pushdown; the
  pushdown is purely a performance optimization that requires
  storage-layer scan-predicate vocabulary to grow a
  `column IN literal_set` shape.

#### 6.3.1 Entity-id pushdown deferral (post-TASK-437)

The entity-id-component pushdown described in § 4.3 has not been
wired into the scan layer in TASK-437. The cohort runtime is
correct without it — every outer row is probed against the
materialized hash set after the scan emits it — and the missing
optimization affects throughput on large outer tables filtered by
small cohorts, not correctness.

The work involves:

1. Extending the storage-layer `ScanPredicate` taxonomy
   (`docs/design/storage/predicate-pushdown.md`) with a
   `column IN <Arc<HashSet<EntityId>>>` shape that the scan can
   evaluate cheaply.
2. Teaching `bqlite-storage`'s shard / segment skip logic to
   reject shards whose `(entity_min, entity_max)` zone-map
   doesn't overlap the cohort.
3. Having the engine bind step extract the entity-id component
   from a multi-column cohort and attach it as a pushdown
   predicate on the outer scan, after the cohort is materialized
   but before scan binding.

This crosses the operators ↔ storage boundary and is deliberately
scoped as a follow-up rather than wedged into TASK-437. The
follow-up should be filed against the same `cohorts-aliases-joins.md`
spec; semantics are unchanged.

### 6.4 Other Tasks

**TASK-433 (DELETE parser):** reject `JOIN` after `DELETE FROM <table>` per § 3.5 with a clear error message.

**TASK-430 (SAMPLE pushdown):** honor § 3.4's value-based hash when the source is a `MergeSources`.

**TASK-501 (memory budget):** account for cohort `HashSet` as a named memory consumer per § 2.7.

---

## 7. Documentation Reconciliation

The following spec sections require updates to align with the decisions in this document. Each update should be applied in the same checkpoint as the code change that depends on it.

### 7.1 `query-language.md` Updates

| Section | Change |
|---------|--------|
| § 17.4 | Document A9's positional multi-column binding with an example where LHS and subquery column names differ |
| § 17 / § 18 | Add the A6 caveat: exceeding the memory budget errors the whole query |
| § 18.1 | Restate A1 (event-type and column names not reserved against alias shadowing) |
| § 18.1 | Document A2 (forward refs forbidden, top-down order required) |
| § 18.1 | Document A3 (submission = one `execute` call; engine is alias-stateless) and A4 (always-cached within submission) |
| § 18.3 | A5 (no cross-submission caching) as corollary of "session-scoped" |
| § 18 | Add A10 equivalence statement between `IN alias` and `IN QUERY` |
| § 19.2 | Restate B4 disallowing DELETE+JOIN with `IN QUERY` workaround example |
| § 19.5 | Cite B5 plan-time error on entity-key-type mismatch |

### 7.2 `type-system.md` Updates

| Section | Change |
|---------|--------|
| § 6.9 | Document A8 (alias shape is use-site enforced) and A9 (positional tuple binding) |
| New section | `__source_table_id: Int8` non-nullable column introduced by `MergeSources` per B7/B8 |

### 7.3 `planner-pipeline.md` Updates

| Section | Change |
|---------|--------|
| § 5.1 | `SubqueryFilter` physical shape per C1/C2: carries `Arc<HashSetCohort>`, materialized at query start, with entity-id pushdown per C3 |

---

## 8. Edge Cases and Error Conditions

### 8.1 Alias Errors

| Condition | Error | Phase |
|-----------|-------|-------|
| Alias name shadows keyword | `ParseError::ReservedKeyword` | Parse |
| Alias name shadows table name | `TypeError::AliasNameCollision` | Bind |
| Forward reference to undefined alias | `TypeError::UndefinedAlias` | Bind |
| Alias cycle (via transitive reference) | `TypeError::AliasCycle { path }` | Bind |
| `IN alias` shape mismatch (arity) | `TypeError::IncompatibleCohortShape` | Bind |
| `IN alias` type mismatch (positional) | `TypeError::IncompatibleCohortType` | Bind |
| Cohort exceeds memory budget | `ExecutionError::MemoryBudgetExceeded` | Runtime |

### 8.2 JOIN Errors

| Condition | Error | Phase |
|-----------|-------|-------|
| Self-join (`events JOIN events`) | `ParseError::SelfJoin` | Parse |
| Unknown table in JOIN | `TypeError::UnknownTable` | Bind |
| Entity-key type mismatch across tables | `TypeError::EntityKeyTypeMismatch` | Bind |
| `DELETE FROM ... JOIN ...` | `ParseError::DeleteJoinNotSupported` | Parse |
| Bare (unqualified) reference in JOIN context | `TypeError::UnqualifiedReferenceInJoin` | Bind |

### 8.3 SubqueryFilter Errors

| Condition | Error | Phase |
|-----------|-------|-------|
| `IN QUERY` output has zero columns | `TypeError::EmptyCohortOutput` | Bind |
| `IN QUERY` output arity != LHS tuple arity | `TypeError::IncompatibleCohortShape` | Bind |
| `IN QUERY` output column type incompatible | `TypeError::IncompatibleCohortType` | Bind |

---

## 9. Worked Examples

### 9.1 Alias-Based Cohort Filtering

```bql
-- Find support tickets from users who purchased in January
buyers = events BETWEEN '2025-01-01' AND '2025-02-01'
       | WHERE event_type = 'purchase'
       | SELECT entity_id

events LAST 90d
| WHERE event_type = 'support_ticket'
| WHERE entity_id IN buyers
| STATS ticket_count = COUNT(*) GROUP BY entity_id
| ORDER BY ticket_count DESC
| LIMIT 100
```

**Execution flow:**
1. `buyers` alias is materialized at query start into a `HashSet<String>` of entity IDs.
2. Entity-ID pushdown filters the outer scan: shards/segments with no buyer entities are skipped.
3. Post-scan, each row's `entity_id` is probed against the hash set.
4. Surviving rows flow through the rest of the pipeline.

### 9.2 Multi-Column Cohort

```bql
-- Events from (user, day) pairs that had high activity
active_days = events LAST 30d
            | STATS n = COUNT(*) GROUP BY entity_id, QUANTIZE(ts, 1d) AS day
            | WHERE n > 50
            | SELECT entity_id, day

events LAST 30d
| WHERE (entity_id, QUANTIZE(ts, 1d)) IN active_days
| MATCH FIRST SEQUENCE(page_view THEN purchase) WITHIN 1d
```

**Execution flow:**
1. `active_days` materializes into a `HashSet<(String, Timestamp)>`.
2. Entity-ID component is extracted for scan pushdown.
3. Post-scan, each row's `(entity_id, QUANTIZE(ts, 1d))` is probed against the full tuple.

### 9.3 Cross-Table JOIN with FUNNEL

```bql
events LAST 30d JOIN purchases
| FUNNEL(
    events.signup
    THEN events.add_to_cart
    THEN purchases.purchase WHERE purchases.amount > 50
  ) WITHIN 7d
```

**Execution flow:**
1. `MergeSources` merges entity-sorted streams from `events` and `purchases`.
2. Events are ordered by `(entity_id, ts, table_order, __seq_id)`.
3. `__source_table_id` is injected: `0` for `events`, `1` for `purchases`.
4. The desugared MATCH/STATS pipeline operates on the unified stream.
5. Table-qualified event references resolve through the registry: `events.signup` becomes `__source_table_id = 0 AND event_type = 'signup'`.

### 9.4 Alias Reuse with Caching

```bql
-- Single materialization despite two references
churned = events LAST 90d | WHERE event_type = 'churn' | SELECT entity_id

-- Query 1: churn rate by plan
events | WHERE entity_id IN churned | STATS n = COUNT(*) GROUP BY plan

-- Note: only the terminal pipeline executes. To run two queries using
-- the same alias, submit them as separate `execute` calls with the
-- alias definition prepended to each.
```

The above submission has one terminal pipeline (the last line). The alias `churned` is referenced once, materialized once. To reuse across queries, the CLI prepends the alias definition to each submission.
