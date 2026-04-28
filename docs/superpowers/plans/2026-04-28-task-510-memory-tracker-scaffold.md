# TASK-510 — Memory tracker enforcement scaffold

> Plan author: agent-3 (2026-04-28). Tracks the work to land the real
> query-scoped memory tracker per
> [`docs/design/engine/memory-budget.md`](../../design/engine/memory-budget.md).

## Goal

Replace the Wave 1 `UnboundedMemory` stub with a real `MemoryTracker`
implementation, plumb it into a new engine-side `QueryContext`, and
expose the configuration knobs (`EngineConfig::query_memory_budget_bytes`,
`QueryOptions::memory_budget_bytes`). No spill — failed reservations
produce a typed-friendly `BqliteError::Execution` with the existing
`budget_exceeded_error` formatting (TASK-511 promotes the variant).

The scaffold also threads the cancellation token through `bind_physical`
from a single per-query source, retiring the per-bind-site
`CancellationToken::new()` calls so future cancel/timeout work
(TASK-505) does not have to repeat the plumbing.

Operator-side reservation calls (sort/aggregate/distinct/cohort) are
explicitly **out of scope** — those are TASK-512/513/514. The job here
is the scaffold: tracker, context, configuration, and the seam for
operators to read `Arc<dyn MemoryBudget>` from a single per-query
source.

## Reference: design doc

`docs/design/engine/memory-budget.md` is the canonical spec. Sections
4 (`MemoryTracker`), 6 (per-query budget), 8 (config & validation),
9 (error surface), 10 (instrumentation) drive the implementation.

## Checkpoints

### CP1 — `MemoryTracker` in `bqlite-core::memory`

Add a real `MemoryTracker` struct implementing `MemoryBudget` per § 4
of the design doc:

- Atomic `used` counter; success path is one `fetch_add` + bounded
  CAS loop on `peak`.
- Mutex-guarded `spill_handlers: Vec<Arc<dyn SpillNotification>>`,
  consulted only on the failure path. Handlers are cloned out before
  invocation so handlers may themselves call `try_reserve` (§ 4.2
  re-entrancy contract).
- One retry per `try_reserve` after spill rounds (§ 4.1). Failure
  returns `Err(budget_exceeded_error(...))`.
- `peak_bytes()` accessor on the concrete type for instrumentation
  (§ 10). Trait surface unchanged.

Existing `UnboundedMemory` stays for tests that want zero accounting.
The trait shape is unchanged.

Tests:
- Reserve / drop returns bytes (RAII works).
- Budget overshoot → `Err`.
- Successful reservations sum correctly across multiple holders.
- Peak tracks the high-water mark and does not regress on release.
- Spill handler that returns 0 → still fails.
- Spill handler that frees enough → `try_reserve` succeeds on retry.
- Multiple handlers iterated in registration order; second handler
  may fix what the first could not.
- `forget()` does not release.
- Concurrent reservations (a small thread-spawn test) preserve the
  invariant `used_bytes() <= budget_bytes()` for any successful
  reservation.

Out of scope: switching the `MemoryReservation` Box-closure to a
non-allocating release path (§ 11.1 deferred to TASK-525/526 per the
doc).

### CP2 — `EngineConfig`, `QueryOptions`, `QueryContext`, plumbing

Add the engine-side wiring per § 8.1, § 8.2, § 3.2 of the design doc.

1. **`EngineConfig`** in `bqlite-engine` with
   `query_memory_budget_bytes: u64` (default 3 GiB). Other budgets
   (compaction / ingest) are placeholders documented as out of scope.
2. **`QueryOptions`** with `memory_budget_bytes: Option<u64>`. Floor
   validation (`MIN_QUERY_BUDGET_BYTES = 512 MiB`, § 8.2). Constructed
   via `QueryOptions::default()` so existing callers compile unchanged.
3. **`QueryContext`** — a private engine struct holding the
   per-query `Arc<dyn MemoryBudget>` and `CancellationToken`. Built
   per `Engine::query` invocation. Threaded into `bind_physical` so
   every operator that takes a `CancellationToken` reads the same
   instance.
4. **`Engine` carries `EngineConfig`.** `Engine::new()` keeps the
   default config; `Engine::with_config(...)` lets a host pin a
   custom budget. `Engine::query_with_options` lets a caller override
   on a single submission. `Engine::query` keeps its current
   signature by delegating to the override path with default options.
5. **`bind_physical(plan, db)` becomes
   `bind_physical(plan, db, &QueryContext)`**. Internal callers are
   updated; the public re-export keeps backwards compatibility by
   threading a default-budget `QueryContext` through if no caller
   passed one (the engine's own callers always have a context).
6. **Operator construction** — `bind_physical_with_cache` reads the
   token from `QueryContext` instead of synthesising fresh ones at
   each site. The `MemoryBudget` is *available* on the context but
   no operator constructor yet consumes it; that lands in
   TASK-512/513/514. The seam exists.
7. **`ExecutionResult::peak_memory_bytes`** — surfaced from the
   tracker's `peak_bytes()` so callers can observe accounting work
   (§ 10). `None` for `UnboundedMemory`.

Tests:
- `EngineConfig::default()` produces 3 GiB.
- Floor rejection for too-small per-query overrides.
- A query under the default budget completes with a positive
  `peak_memory_bytes` once any operator opts into reservations
  (none yet — assertion deferred to first wired operator).
- A pathological reservation exceeding budget surfaces as an
  execution error string matching `budget_exceeded_error()`.
- `bind_physical` threads one `CancellationToken` through every
  bound operator (reflection / token-equality test).

## Reconciliation

Per § 13 of the design doc, the following docs are reconciled
**inside this task** in CP2:

- `core-beliefs.md` Belief 6 wording (3 GiB query / ~4 GiB engine).
- `reliability.md` § Memory Budget — point at the new design doc.
- `storage-format.md` § 13 — reword 200 MB → 256 MB ingest, frame
  percentages as illustrative.
- `execution-model.md` § 10.1 — drop "hierarchical memory tracker"
  language, refer to the new design doc.
- `operators/operator-traits.md` § 2 / § 7 / § 5.3 — clarify the
  `Arc<dyn MemoryBudget>` plumbing pattern alongside the existing
  `CancellationToken` pattern.
- `operators/sort-distinct.md`, `operators/aggregate-operator.md`,
  `operators/sessionize.md`, `operators/match-operator.md` —
  forward references TASK-111 → TASK-510 per
  `engine/memory-budget.md`.
- `language/cohorts-aliases-joins.md` § 2.7 / § 7 — point cohort
  out-of-budget caveat at the new design doc.
- `INDEX.md` — `engine/memory-budget.md` already listed (TASK-501).

If any of these reconciliations balloon the diff past one
checkpoint's worth of review, the documentation reconciliation is
deferred to a follow-on doc-only commit on the same branch and
merged before completion.

## Risks

- **Breaking the `bind_physical` signature.** The engine's `query.rs`
  is the only direct caller; `bqlite-cli` and the tests go through
  `Engine::query`. Adding a `&QueryContext` parameter to
  `bind_physical` is a public-API change but the existing tests in
  `bqlite-engine` cover the surface, and we can keep a
  `bind_physical(plan, db)` shim that builds a default context for
  any external caller. The CP2 diff confirms this directly.
- **Trait surface drift.** Resist adding `peak_bytes` to the
  `MemoryBudget` trait — the design says only the concrete tracker
  exposes it. Adding it would also force `UnboundedMemory` to
  return something meaningless.
- **Doc reconciliation scope creep.** Limit the doc edits to the
  exact phrasing changes called out in § 13. Anything broader is a
  separate PR.
