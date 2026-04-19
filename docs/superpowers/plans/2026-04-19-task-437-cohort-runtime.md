# TASK-437 — Cohort subquery runtime + alias execution cache

## Scope

Implement the runtime execution of `PhysicalPlan::SubqueryFilter`:

1. A `SubqueryFilterOperator` that probes its child against a pre-materialized
   `CohortHashSet` of LHS-shaped tuples.
2. An engine-side cohort materialization phase that walks the `PhysicalPlan`
   tree before the main bind, executes each unique inner subquery exactly
   once, and wires the resulting `Arc<CohortHashSet>` into the operator at
   bind time.
3. Per-submission cohort caching keyed by structural equality of the inner
   `PhysicalPlan` (so two `IN alias` references with the same alias body, and
   two identical `IN QUERY (...)` subqueries, share one materialization per
   §2.5 + §2.11 of `docs/design/language/cohorts-aliases-joins.md`).

Out of scope (explicitly deferred to follow-up tasks):

- **Entity-id pushdown to the scan layer (§4.3).** A storage-side
  optimization that requires extending the scan predicate vocabulary with a
  `column IN literal_set` shape and adding shard/segment skipping. The
  runtime probe operator handles correctness independently; this is purely
  a perf path. Documented as a follow-up note in `cohorts-aliases-joins.md`
  §6.3.
- **`MergeSources` runtime (TASK-436).** Already on its own task; the
  Wave 4 stub remains in `bind.rs`.
- **Engine bind extension for the other Wave 4 nodes (TASK-438).**

Cycle detection is already handled at plan time by `resolve_alias` in
`crates/bqlite-planner/src/logical.rs`; the runtime simply executes the
already-cycle-free plan.

## Checkpoints

### CP1 — Cohort operator + hash-set type (operators-only, additive)

New file `crates/bqlite-operators/src/cohort.rs` containing:

- `CohortKey` (newtype around `Vec<ScalarValue>` with the same hashing
  story as `GroupKey`). Uses `compact_str` only for the string-typed
  arms when it cleanly reduces allocations — otherwise plain `String`,
  matching the rest of `bqlite-operators`.
- `CohortHashSet` — `HashSet<CohortKey>` plus a small builder API
  (`from_batches(...)`) that consumes `RecordBatch`es from the
  materialized inner subquery and inserts one tuple per non-system row.
  Ignores system columns (`__seq_id`, `__batch_id`) by name match —
  the planner already projects only declared columns into cohort
  output schemas (cohorts-aliases-joins.md §4.1).
- `SubqueryFilterOperator` — wraps a child `Box<dyn PhysicalOperator>`,
  evaluates the LHS expression list per batch, builds row-keyed tuples,
  and emits a filtered batch via `arrow::compute::filter_record_batch`
  for rows whose tuple is in the cohort.

Tests:
- single-column probe (Int and String keys)
- two-column tuple probe with mixed types
- empty cohort → empty output
- empty input batch passes through
- mid-stream all-rejected batch → re-pull
- NULL on LHS → row is dropped (NULL never matches in IN per the
  three-valued-logic convention; matches existing `FilterOperator`
  null semantics)
- lifecycle forwards open/close to child

CP1 only adds files; no public API of any other crate moves. Pure
additive merge, zero conflict risk.

### CP2 — Engine bind wiring + end-to-end correctness

In `crates/bqlite-engine/src/bind.rs`:

- Replace the `PhysicalPlan::SubqueryFilter` stub arm.
- Add a small `CohortCache` collected in a single pre-bind walk of the
  plan tree. Keyed by the inner subquery `PhysicalPlan` (compared via
  `PartialEq`); each unique subquery materializes once into an
  `Arc<CohortHashSet>`. Walk visits children of every plan variant.
- Materialization runs the inner subquery via the existing
  `bind_physical` + `open / next_batch / close` pattern (recursive use
  of `bind_physical` for the inner plan is fine — inner plans don't
  contain `SubqueryFilter` at v1 because alias bodies cannot reference
  other aliases that themselves cohort-filter… actually they can; the
  cache walk must handle nested SubqueryFilter inside the inner plan).
- Wire `SubqueryFilterOperator` with the cached `Arc<CohortHashSet>`.

Tests in `bind.rs` (or new file `bind_cohort.rs` if it grows):
- end-to-end `IN QUERY (...)` single-column over real `Engine::query`
- end-to-end `IN QUERY (...)` two-column tuple
- end-to-end `IN alias` (with alias used once and twice — verify
  single-materialization via a recording counter on a wrapper around
  the inner pipeline, or via observable side effect)
- alias chain (alias `b = ... | WHERE x IN a`) executes correctly

CP2 needs `cohort.rs` from CP1 to be on `main` first.

### CP3 — Doc reconciliation + completion

- Update `docs/design/language/cohorts-aliases-joins.md` §6.3 to flag
  the entity-id-pushdown deferral with a one-line follow-up note.
- Move the lock file to `tasks/completed/TASK-437.done` with
  `completed_at`, commit, and push (per AGENTS.md *Completion Protocol*).

## Risks / open questions

- **Subquery `PartialEq` cost.** Plans are small (≤ a few dozen nodes
  in realistic queries). Linear scan over the cohort cache is fine
  for v1; if it shows up in a profile, Wave 5 can hash plan trees.
- **Cohort materialization re-uses the existing `bind_physical` path.**
  This is correct because the inner subquery is just another
  `PhysicalPlan` from the planner, but it means cohort materialization
  is single-threaded for now (matches the rest of Wave 4).
- **`update_batch` empty-cohort handling.** If the cohort is empty,
  the operator can short-circuit and return zero rows for every
  batch — included as a microbench-friendly path but tested with the
  empty-cohort case.
