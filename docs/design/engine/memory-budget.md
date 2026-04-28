# Memory Budget Enforcement Model

> **Status**: draft (TASK-501, Wave 5).
> **Owns**: query-time memory accounting, the reservation/release contract,
> per-operator spill-vs-fail policy, and the `MemoryBudget` ↔ `QueryContext`
> wiring.
> **Depended on by**: TASK-502 (spill protocol), TASK-510 (memory tracker
> enforcement scaffold), TASK-511 (structured execution errors),
> TASK-512 (ingest partitioner external spill — outside `QueryContext`,
> see § 1 / § 13), TASK-513 (operator-side sort spill), TASK-514
> (operator-side cohort/`IN`-subquery spill), TASK-525 (memory-pressure
> stress suite).
> **Reconciles**: `core-beliefs.md` Belief 6, `reliability.md` § Memory
> Budget, `storage-format.md` § 13, `execution-model.md` § 10,
> `operators/operator-traits.md` § 5/§ 7, the per-operator notes in
> `operators/sort-distinct.md` / `operators/aggregate-operator.md` /
> `operators/sessionize.md` / `language/cohorts-aliases-joins.md`.

---

## 1. Scope

This note covers **query-time memory accounting** and is the single source
of truth for:

- The default budget numbers and how they relate to one another.
- Which allocation classes count against the budget and which do not.
- How a query gets its budget, how operators reserve from it, and how
  reservations are released.
- How `bqlite-core::memory::MemoryBudget` (TASK-111) is wired into the
  engine's `QueryContext`.
- Per-operator spill-vs-fail policy and how a failing reservation surfaces
  to the caller.
- Configuration surface, instrumentation, and the items intentionally
  deferred past Wave 5.

Out of scope:

- The byte layout, naming, and cleanup protocol for spilled state — that
  is `engine/spill.md` (TASK-502).
- The cancellation/timeout/warning protocol — `engine/cancellation.md`
  (TASK-505). This doc references that protocol but does not freeze it.
- Compaction-side memory accounting — `storage-format.md` § 13 covers
  that share of the engine budget at the operational level; the storage
  layer's compaction memory bound is documented there and is independent
  of `QueryContext`.
- Ingest-buffer memory accounting (the `(shard, window)` partitioner) —
  documented in `storage-format.md` § 13 and refined in TASK-512. The
  partitioner has its own budget, separate from the per-query budget,
  because ingest does not run inside `QueryContext`.

---

## 2. Default Budget Numbers (Reconciliation)

### 2.1 The drift this note retires

Two threads of doc drift accumulated through Waves 0–4:

1. **Engine total vs. query share.** `core-beliefs.md` Belief 6 and
   `reliability.md` say the *default memory budget is 4 GB*. Older
   Wave 5 task drafts repeated that figure as if it were the *query*
   budget. `storage-format.md` § 13 and `execution-model.md` § 10
   instead split that 4 GB across query / compaction / ingest, with
   the query share landing at **3 GB**. Every Wave 3/4 operator note
   that quotes a number quotes 3 GB.
2. **Per-subsystem percentages vs. shipped defaults.** § 13 derives
   the ingest share as 5 % of 4 GB = 200 MB, but the engine ships
   `DEFAULT_INGEST_BUDGET_BYTES = 256 MB`. The percentages are
   illustrative; the absolute defaults are authoritative.

### 2.2 Canonical defaults

The **query memory budget** is **3 GiB** (`3 * 1024 * 1024 * 1024` bytes
= 3 221 225 472 bytes) by default. That is the only number this doc
binds; every other doc must use this value when describing query memory.

The **engine-wide aggregate ceiling** is **4 GiB** by default — but it is
*not* a single allocator. The 4 GiB number is a capacity-planning
sum-of-defaults across three independent budgets:

| Subsystem  | Default budget | Owner trait / type                        | Spec |
|------------|---------------:|-------------------------------------------|------|
| Query      | **3 GiB**      | `bqlite-core::MemoryBudget` per query     | This doc (§ 6, § 7) |
| Compaction | **800 MiB**    | Compaction worker pool ceiling            | `storage-format.md` § 13, `storage/compaction-concurrency.md` § 4 |
| Ingest     | **256 MiB**    | `Partitioner::new(.., budget_bytes)`      | `storage-format.md` § 13, TASK-512 |

The three sub-budgets do **not** share an allocator and do **not**
contend for bytes. Each subsystem enforces its own ceiling. The 4 GiB
"engine ceiling" is the user-facing summary number ("bqlite uses up to
~4 GB by default") and is not a runtime invariant — the actual peak is
*at most* `3 GiB + 800 MiB + 256 MiB ≈ 4.06 GiB` if all three subsystems
are simultaneously at their ceilings, plus the small fixed-size
overheads in § 5.

Why three independent budgets rather than one parent allocator:

- **Different lifetimes.** A query budget lives for the duration of one
  `execute()`; the ingest budget lives for the duration of one INSERT;
  compaction runs in a long-lived background pool. Sharing one
  allocator would either require coarse arbitration (which queries
  should we starve to give compaction headroom?) or no arbitration at
  all, which is what we already have.
- **Different failure modes.** A query that exceeds its budget aborts
  with `MemoryBudgetExceeded`. An ingest partitioner that exceeds its
  budget triggers external spill (TASK-512). A compaction merge that
  exceeds its ceiling pauses on the core-budget semaphore. None of
  these failure modes generalize cleanly.
- **Isolation guarantee.** A pathological query cannot starve
  compaction; a pathological compaction cannot starve queries. This
  matters for an embeddable engine that is expected to keep the host
  process responsive.

### 2.3 Authoritative reconciliation list

This note is the canonical reference. Every other doc that mentions a
default budget number must read either as:

- "the query memory budget (default 3 GiB; see `engine/memory-budget.md`)",
- "the per-subsystem default (3 GiB query / 800 MiB compaction / 256 MiB ingest; see `engine/memory-budget.md`)",
- or "the engine's aggregate default (~4 GiB; see `engine/memory-budget.md`)",

depending on which figure is contextually relevant. Strings that referred
to "the 4 GB query budget" or "the default memory budget is 4 GB" without
distinguishing sub-budgets are updated in the same checkpoint as this
file (see § 13).

### 2.4 Configuration surface

The defaults are settable per `Engine` instance (see TASK-510 for the
engine-side wiring):

| Field | Default | Notes |
|-------|--------:|-------|
| `query_memory_budget_bytes` | `3 GiB` | Applied to every query unless overridden per-query. |
| `compaction_memory_budget_bytes` | `800 MiB` | Read by the compaction scheduler; out of scope here. |
| `ingest_memory_budget_bytes` | `256 MiB` | Read by `Partitioner::new`; § 13 ingest entry. |

Per-query overrides (`QueryOptions::memory_budget_bytes`) are wired in
TASK-510. Validation rules (§ 8) reject overrides below a documented
floor.

---

## 3. The `MemoryBudget` ↔ `QueryContext` Wiring

### 3.1 Trait surface (already shipped)

The trait is defined in `bqlite-core::memory` (TASK-111) with this
shape:

```rust
pub trait MemoryBudget: Send + Sync {
    fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation>;
    fn register_spill_handler(&self, handler: Arc<dyn SpillNotification>);
    fn used_bytes(&self) -> u64;
    fn budget_bytes(&self) -> u64;
}

pub struct MemoryReservation { /* RAII guard, drop releases */ }

pub trait SpillNotification: Send + Sync {
    fn on_pressure(&self, bytes_needed: u64) -> u64;
}
```

The Wave 1 stub `UnboundedMemory` always succeeds. TASK-510 lands the
real `MemoryTracker` implementation (§ 4).

### 3.2 Mapping into `QueryContext`

`QueryContext` is the engine-side per-query wrapper (currently sketched
in `execution-model.md` § 3.3). The Wave 5 shape:

```rust
pub struct QueryContext {
    cancelled: CancellationToken,        // §3.3, TASK-505
    timeout: Option<Duration>,           // TASK-505
    memory: Arc<dyn MemoryBudget>,       // THIS DOC
    tile_size: usize,                    // §3.6
    metrics: Arc<QueryMetricsSink>,      // TASK-511 / TASK-524
    warnings: Arc<WarningChannel>,       // TASK-505 / TASK-511
    // ... query-id / scheduling fields owned by TASK-506
}
```

Key invariants:

1. **One budget per query.** The engine constructs exactly one
   `Arc<dyn MemoryBudget>` per `execute()` call and stores it on the
   `QueryContext`. Every operator reachable from that context shares
   that one budget via `Arc` clones.
2. **No per-worker sub-budgets.** Workers draining morsels for the
   same query reserve from the same `MemoryTracker`. Contention is on
   one `AtomicU64` (used-bytes counter), which is the cheapest possible
   shared coordination point. Per-worker sub-budgets were considered
   and rejected — see § 6.3.
3. **Operators receive the trait, not the concrete type.** Operator
   constructors take `Arc<dyn MemoryBudget>` so the engine can hand
   them the real tracker in production and `Arc::new(UnboundedMemory)`
   in unit tests. This matches how `CancellationToken` is already
   distributed (see `operators/operator-traits.md` § 5.3).
4. **The `QueryContext` is plumbed into operators on construction.**
   Operators that allocate dynamically take `Arc<dyn MemoryBudget>` as
   a constructor argument; stateless kernels that allocate nothing
   beyond fixed scratch ignore it. The `EntityOperatorAdapter` forwards
   the same `Arc` to its inner `EntityOperator` if the operator
   declares it cares.

### 3.3 Engine ↔ query relationship

There is **no** parent/child tracker hierarchy in v1. The "hierarchical
memory tracker" wording in `execution-model.md` § 10.1 was forward-
looking; v1 ships:

- One concrete `MemoryTracker` per active query.
- No engine-wide tracker. Concurrent queries each carry their own
  tracker; the morsel scheduler (TASK-506) bounds in-flight work by
  the worker pool, not by a shared byte ceiling.
- No NUMA / thread-local sub-trackers.

The "hierarchical" phrasing is dropped from `execution-model.md` § 10.1
in this checkpoint (§ 13). If a future wave needs cross-query global
accounting, it lands as TASK-5xx with a parent-tracker addendum to this
doc. The trait shape already accommodates it (a parent tracker can wrap
a child's `try_reserve` call), so the engine code does not have to
change to add it later.

---

## 4. The `MemoryTracker` Implementation (TASK-510 preview)

This section freezes the contract; the code lands in TASK-510.

```rust
pub struct MemoryTracker {
    /// Total bytes currently reserved by this query. All reads/writes
    /// use Acquire/Release on the operator-side `try_reserve`/release
    /// path so the byte total observed by `used_bytes()` is always
    /// consistent with the most recent reservation observable on the
    /// calling thread.
    used: AtomicU64,
    /// Maximum bytes this query may reserve. Set once at construction.
    budget: u64,
    /// Peak observed `used` value. Updated on each successful
    /// reservation; read at query teardown for metrics.
    peak: AtomicU64,
    /// Registered spill handlers. Iterated under a `Mutex` only on the
    /// failure path of `try_reserve` — the success path is lock-free.
    spill_handlers: Mutex<Vec<Arc<dyn SpillNotification>>>,
}

impl MemoryBudget for MemoryTracker {
    fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation> { /* §4.1 */ }
    fn register_spill_handler(&self, handler: Arc<dyn SpillNotification>) { /* §4.2 */ }
    fn used_bytes(&self) -> u64 { self.used.load(Ordering::Acquire) }
    fn budget_bytes(&self) -> u64 { self.budget }
}
```

### 4.1 `try_reserve` algorithm

```text
1. Atomically `fetch_add(bytes)` on `used`.
2. If the post-add value ≤ budget:
     update `peak` (relaxed CAS loop), and
     return Ok(MemoryReservation { bytes, release: <release callback> })
3. Otherwise (overshoot):
     a. Atomically `fetch_sub(bytes)` to restore the counter.
     b. Acquire the `spill_handlers` mutex, clone out the handler `Arc`s
        into a local `Vec`, and drop the guard. (Only on this slow path.)
        Cloning out before invocation is what allows handlers to
        themselves call `try_reserve` without deadlocking — see § 4.2.
     c. For each handler in registration order:
            freed = handler.on_pressure(bytes)
            if freed > 0:
                retry the fetch_add once.
                if it now fits: succeed.
                if it still overshoots: fetch_sub and continue to the next handler.
     d. If no handler frees enough bytes, return Err(MemoryBudgetExceeded).
```

Properties this gives us:

- The success path is **two atomic operations on the uncontended path
  plus one allocation** (the `MemoryReservation` callback closure):
  one `fetch_add` to charge the reservation and one CAS to update
  `peak`. The peak CAS may iterate under contention but is bounded by
  the number of concurrent reservations and is observably cheap in
  benchmarks (TASK-525 / TASK-526). No mutex acquisition. No heap
  allocation past the closure box; the Wave 5 implementation may
  switch to a non-allocating closure if benchmarks demand it (see
  § 11).
- The failure path is **slow on purpose** — taking a mutex is fine
  because we are about to either spill (which is dramatically more
  expensive) or fail the query. Callers must not depend on per-call
  `try_reserve` timing.
- Spill handlers run in registration order; this is a stable
  contract. Operators that register late can rely on earlier-
  registered handlers having already had a chance to free memory.
- A single retry is the contract. If after one round of spilling we
  still cannot fit, the query fails. This bounds the wall-time impact
  of a near-budget query and prevents pathological "spill-spill-spill"
  loops.

### 4.2 `register_spill_handler` semantics

Operators that can spill register a handler at construction time and
hold onto an `Arc` of themselves (or a dedicated handle struct that
wraps the relevant state) so the handler can call back into operator
state without lifetime games. Handlers must be:

- **Idempotent in the small.** Calling `on_pressure(0)` is allowed and
  must return 0.
- **Honest about freed bytes.** The handler must return the *actual*
  bytes released to the budget, measured by the same accounting the
  reservation lifecycle uses. Returning more than was actually
  released invalidates the budget invariant.
- **Re-entrant-safe relative to `try_reserve`.** A handler that
  internally calls `try_reserve` (e.g., to allocate a spill scratch
  buffer) must not deadlock. The implementation drops the
  `spill_handlers` mutex before invoking handlers to make this safe.

### 4.3 `MemoryReservation` lifecycle

The RAII guard is already shipped. Three lifecycle patterns operators
should follow:

1. **Static allocation.** Reserve once, hold until the operator's
   `close()`. Drop the reservation in `close()` — the engine guarantees
   `close()` runs on every path including errors and panics.
2. **Per-batch allocation.** Reserve at the top of `next_batch`,
   release implicitly at function return. Drop happens on the unwind
   path automatically.
3. **Growing buffer.** Drop the prior reservation, reserve a new
   (larger) one. There is no in-place `grow()` — the byte total is the
   only thing being tracked, so dropping and re-reserving is exactly
   equivalent and avoids growing the trait surface. The `forget()`
   helper exists for handing reservation ownership across operator
   boundaries (e.g., from a sub-batch builder into a final
   `RecordBatch` whose reservation is owned by the parent operator).

Operators must **not** reserve speculatively. Allocate then release in a
loop is the right pattern only when paired with a concrete "what would
I do with this memory" computation. Otherwise the budget reports
inflated peak usage.

---

## 5. Tracked vs. Untracked Allocation Classes

The budget tracks every allocation class that **grows with the data**.
It does not track class-bounded fixed-size state.

### 5.1 Tracked

These all reserve before allocating and hold the reservation for the
allocation's lifetime:

| Allocation class | Owner | Lifetime | Wave 5 spill behaviour |
|------------------|-------|----------|-----------------------|
| Hash-aggregate state (`HashAccumulator` groups + values) | Aggregate operator (per-shard) | Operator open → close | Hard cap (`max_groups`); fail (§ 7) |
| Sort buffer (rows + `take` indices + output batch) | `SortOperator` | Open → close | Spill (TASK-513; layout per `engine/spill.md` § 6.1) |
| Distinct hash set | `DistinctOperator` | Open → close | Hard cap; fail (§ 7) |
| IN-subquery / cohort hash set | Cohort materialization (`MergeSources` / `SubqueryFilter`) | Outer query lifetime | Fail (`engine/spill.md` § 4.3); TASK-514 wires the budget check |
| K-way merge read buffers | Scan layer (per active worker × k inputs) | Per-morsel | None — fail-fast on construction (these are fixed-size) |
| Decoded column payloads materialized past the scan/filter boundary | Stateless kernels (`materialize_filtered_batch`) | Per-batch | Fail (§ 7) |
| `FilteredBatch` payload buffers | Stateless segment driver | Per-batch | Fail |
| SequenceMatch output-row buffer (inside `MatchExecutionConfig`) | Match operator | Per-entity → flushed at `finish_entity` | Fail (per-entity active-state cap is the existing line of defence) |
| Sessionize per-entity session-event buffer (when `track_match_events` or end-event lists demand retention) | Sessionize operator | Per-entity → flushed at `finish_entity` | Hard cap (entity-event limit, `operators/sessionize.md` § 11); fail |
| EventSelect candidate-row state (FIRST/LAST/NTH) | EventSelect operator | Per-entity → flushed at `finish_entity` | Negligible per entity; charged for completeness |
| Attribute sliding-window deque payload | Attribute operator | Per-entity → flushed at `finish_entity` | Hard cap; fail |

The reservation is for the **logical owned bytes**: the Arrow buffer
length, the hash-table backing array length plus probe overhead, etc.
For Arrow `RecordBatch`es flowing through, the reservation is held by
the operator that materialized the batch and released when the batch is
dropped at the next operator boundary (or sooner — if the receiving
operator borrows column views without copying, it does not re-reserve).

### 5.2 Untracked (fixed-size or class-bounded)

These do **not** go through the budget:

- Compiled operator state — NFA programs, compiled predicates, schemas,
  `OperatorSchema`, demand-capability vectors. All `Arc`'d once at
  plan time and shared across workers.
- Per-entity state inside an `EntityOperator` — typical sizes are tens
  to hundreds of bytes per entity (`MatchState`, `SessionState`,
  step-counter byte, candidate-row `EventSelectState`).
  `operators/match-operator.md` § 8.4 documents the worst-case
  ~500 KB / entity ceiling under the active-state cap; this is not
  worth tracking individually.
- Per-tile scratch buffers (~32 KB, sized once at plan time as a
  function of `tile_size`).
- `CancellationToken`, metrics counters, warning collectors.
- The `MemoryReservation` closure box itself (one allocation per
  reservation; charged to the budget at zero cost and re-evaluated
  in benchmarks per § 11).
- `QueryContext`, `WorkerContext`, and the morsel queue.
- All Arc-shared compiled artefacts (planner output, `PhysicalPlan`).

The line is drawn at "would the worst-case size of this allocation
class be visibly correlated with input cardinality, group cardinality,
or per-entity event cardinality?" If yes, it is tracked. If no, it is
not.

### 5.3 Why this line

Tracking per-entity 16-byte state across millions of entities means
millions of `try_reserve` calls per query. Each call is two atomics and
a small allocation; under a 32-core morsel scheduler, that is ~30M
reservations/sec of contention on the budget atomic on the hot path.
The benefit is bounding ~16 bytes × N entities × ~few states ≈ ~1 GB
*peak* state — already covered by the existing entity-level caps
(active-state, max sessions, etc.). The cost-benefit is wrong.

The same logic applies to per-tile scratch: capping `tile_size` at
4096 (`execution-model.md` § 3.6) caps the per-tile scratch. Tracking
it would charge fixed bytes against the budget for every batch and add
no observable safety.

---

## 6. Per-Query vs. Per-Worker Budget

### 6.1 Single shared budget

There is one `MemoryTracker` per query. All workers draining morsels
for that query share it via `Arc`. This is the simplest correct design:
budget enforcement is exact, contention is bounded to one atomic, and
no worker can be "starved" by another worker's runaway reservation
because all reservations land in the same counter.

### 6.2 Per-worker working set as a planning hint

`execution-model.md` § 10.2 documents a per-worker working-set table
(`~29 MB` for k-way merge buffers + current batch on a 32-core machine).
That table stays — it is a *planning* tool the morsel generator uses to
decide morsel size and the planner uses to decide tile size. It is not
a runtime invariant.

The per-worker working set is just an arithmetic decomposition of the
total query budget:

```text
expected_peak_query_bytes
  = num_active_workers × per_worker_working_set
  + num_shards × per_shard_partial_aggregate_bytes
  + per_query_owned_state (sort buffer, cohort hash set, etc.)
```

The morsel scheduler checks the inequality at admission time
(TASK-506). Operators do **not** check it. Operators only care that
their own `try_reserve` calls succeed.

### 6.3 Why no per-worker sub-budget

Considered and rejected:

- **Sub-budget overshoot is hard to recover.** A worker that exhausts
  its sub-budget would have to either spill (cheap to do per-query;
  expensive to do per-worker, because most spillable state is shared
  across the morsel pool) or fail (which means a single skewed entity
  fails the whole query unnecessarily).
- **Contention is already bounded.** One `AtomicU64::fetch_add` per
  reservation is cheap. Sharding the counter into per-worker
  counters with a roll-up step adds complexity and saves nothing on
  the success path.
- **Power-law skew.** Behavioral data is power-law distributed
  (`execution-model.md` § 9). One worker draining the unlucky shard
  legitimately needs more than `1/num_cores` of the budget. A
  hard per-worker carve-out would cap the wrong worker.

The arithmetic-decomposition view in § 6.2 gives planning teeth without
the enforcement cost.

---

## 7. Per-Operator Spill-vs-Fail Policy

This is the v1 policy table. TASK-502 freezes the spill *protocol*
(file layout, naming, cleanup); this table fixes which operators
participate.

| Operator | Tracks budget? | Overflow behaviour (v1) | Hard cap? | Spec |
|----------|:-------------:|-------------------------|-----------|------|
| `ScanOperator` | No (k-way merge buffers are fixed-size; charged once at construction; nothing grows with data) | Fail at construction if buffers don't fit | No | This doc § 5.1 |
| `FilterOperator` (Wave 2 / fused) | Per-batch only | Fail | No | `execution-model.md` § 3.8 |
| `ProjectOperator` | Per-batch only | Fail | No | `execution-model.md` § 3.8 |
| `LimitOperator` | No (selection-vector slicing) | n/a | n/a | — |
| `SortOperator` | Yes | **Spill** (TASK-513; on-disk layout per `engine/spill.md` § 6.1) | `max_rows` (10M) as last-resort backstop | `operators/sort-distinct.md`, `engine/spill.md`, TASK-513 |
| `DistinctOperator` | Yes | **Fail** | `max_groups` (1M) | `operators/sort-distinct.md` |
| `HashAccumulator` (aggregate) | Yes | **Fail** with `MaxGroupsExceeded` | `max_groups` (1M) | `operators/aggregate-operator.md` |
| `MatchOperator` (sequence matching) | Per-entity output buffer + step-property retention | **Fail** (per-entity active-state cap is the v1 line of defence; budget overflow is fatal) | Active-state cap (10K candidates), entity-event limit (10M) | `operators/match-operator.md` § 8, `sequence-matching.md` § 16 |
| `SessionizeOperator` | Yes (per-entity event buffer when downstream demands `match_events` etc.) | **Fail** | Per-entity event cap (1M) | `operators/sessionize.md` § 14 |
| `EventSelectOperator` | Yes (per-entity candidate state, negligible) | **Fail** | Bounded by k for NTH | `operators/event-select-sample.md` |
| `AttributeOperator` | Yes (sliding-window deque) | **Fail** | Per-entity deque cap | `operators/attribute.md` |
| `MergeSources` / `SubqueryFilter` (cohort) | Yes (hash set) | **Fail** with `MemoryBudgetExceeded` per `engine/spill.md` § 4.3 (no on-disk hash-set in v1; spill deferred past Wave 5) | None (cohort size is unbounded) | `language/cohorts-aliases-joins.md` § 2.7, `engine/spill.md` § 4.3, TASK-514 |

**Spill is the preferred response only for operators in the table above
that explicitly say "Spill".** Every other operator's response to a
failed `try_reserve` is to abort with `MemoryBudgetExceeded`. This is
deliberate — most operator state is not cheaply spillable, and forcing
all operators to implement a spill path would balloon the engine for
negligible practical gain.

The hard caps remain in place even after the budget tracker lands. They
are not redundant — the cap is a per-operator semantic guard
(`max_groups`, active-state cap) that fires before the budget fires for
the common-case "this query is fine, but one entity is pathological"
scenario. The budget catches the orthogonal case "many concurrent
small allocations sum past the global ceiling".

When **both** a cap and the budget would trip, the operator surfaces the
more specific error: `MaxGroupsExceeded` over `MemoryBudgetExceeded` for
the aggregate case. The error variant is determined at the call site,
not by inspecting both bounds — the operator checks the cap first
(it's the cheaper test) and only calls `try_reserve` once the cap is
satisfied. This keeps the typed-error mapping unambiguous.

---

## 8. Configuration & Validation

### 8.1 Engine-level configuration

Set on `Engine` construction (TASK-510 wires the field):

```rust
pub struct EngineConfig {
    pub query_memory_budget_bytes: u64,       // default: 3 * 1024 * 1024 * 1024
    pub compaction_memory_budget_bytes: u64,  // default: 800 * 1024 * 1024 (out of scope)
    pub ingest_memory_budget_bytes: u64,      // default: 256 * 1024 * 1024 (out of scope)
    // ... other fields
}
```

### 8.2 Per-query override

A future `QueryOptions` struct (TASK-510) will accept
`memory_budget_bytes: Option<u64>`. When set, it replaces the
engine-level default for that query only. Validation:

- **Floor.** A query budget below `MIN_QUERY_BUDGET_BYTES = 512 MiB`
  is rejected at submission time. This is enough to hold the
  aggregate fixed working set across the worker pool — `num_cores ×
  ~29 MB` for k-way merge buffers + current batch (≈ 464 MB on a
  16-core machine, ~ 928 MB on a 32-core machine; see
  `execution-model.md` § 10.2) — for the small / medium target
  hardware, plus headroom for at least one tracked allocation
  (smallest aggregate state, smallest sort buffer). Below that
  number, no real query can run; below ~ 256 MiB even single-shard
  queries on a small machine cannot make forward progress, so this
  cuts off the worst latency-of-failure case (a query that submits,
  drains its budget on the first row-group fetch, and aborts).
  Hosts that explicitly want to run only on tiny machines can
  recompile with a different floor; this is a configuration
  invariant, not a per-query knob.
- **No upper bound from the engine.** The host process is responsible
  for not configuring a budget larger than its address space.
- **Single budget per query.** Setting the override after the query
  has started executing is a programming error and is rejected.

### 8.3 Why these specific defaults

3 GiB query budget:

- The fixed per-worker working set on a 32-core machine is
  ~32 × 29 MB ≈ 1 GB (`execution-model.md` § 10.2).
- Aggregate / sort / distinct caps add up to ~200–500 MB in the worst
  case (1M groups × 100 bytes + 10M sort rows × 100 bytes).
- A worst-case cohort hash set sits on top.
- 3 GiB leaves ~1.5 GiB of headroom for materialization scratch,
  decoded payloads, and the spillable IN-subquery / cohort cases
  before TASK-502 lands. That headroom is what justifies "fail"
  rather than "spill" for the majority of operators.

256 MiB ingest budget: matches the shipped `Partitioner::new` call.
The `storage-format.md` § 13 figure of 200 MiB is updated to 256 MiB
in this checkpoint.

---

## 9. Error Surface

### 9.1 Typed error

`try_reserve` returns `Result<MemoryReservation, BqliteError>` (the
`bqlite-core::Result` alias). The error path constructs:

```rust
budget_exceeded_error(requested, budget, used)
    -> BqliteError::Execution(<formatted message>)
```

This is the v1 surface. TASK-505 (`engine/cancellation.md`) and
TASK-511 (structured execution errors) freeze the strongly-typed
variant the engine returns to the caller — likely
`ExecutionError::MemoryBudgetExceeded { requested, used, budget }` —
and update the `bqlite-core::Error` enum accordingly. Until then, a
match on the error message is the v1 contract for tests; structured
matching becomes the contract once TASK-511 lands.

### 9.2 Surfacing path

1. Operator calls `try_reserve(bytes)`.
2. On `Err`, the operator either:
   - retries via the spill notification (handled by the budget
     itself in § 4.1, transparent to the operator), or
   - propagates the error by returning it from `next_batch` /
     `process_sub_batch`'s associated error channel.
3. Inside `EntityOperatorAdapter`, the per-entity processing path
   (`process_sub_batch` → `()`) catches the error via the explicit
   `QueryContext` error flag (per `execution-model.md` § 4 design
   choice — the hot path is branch-free, errors are flagged
   asynchronously).
4. The engine drains in-flight workers, calls `close()` on every
   operator (which releases held reservations), and returns the
   error to the caller.

### 9.3 Error vs. cancellation

Memory failure and cancellation are separate signals. A query that
hits its budget after cancellation has fired returns the
cancellation, not the budget error. (TASK-505 owns the precedence
order; the design here defers to it.)

---

## 10. Instrumentation

The tracker exposes:

| Metric | Source | Surface |
|--------|--------|---------|
| `MemoryTracker::used_bytes()` | live counter | EXPLAIN, debug |
| `MemoryTracker::peak_bytes()` | post-query | `QueryMetrics` (TASK-511) |
| `MemoryTracker::budget_bytes()` | static | EXPLAIN |
| Spill-handler invocations | counted on the slow path | `QueryMetrics` |
| Bytes freed per spill round | counted on the slow path | `QueryMetrics` |

These map to the Wave 5-only metrics rows in `execution-model.md` § 14
and surface through `--explain-perf` (TASK-524). They are not collected
during normal queries unless the caller opts in — peak tracking is the
only counter that runs unconditionally, and it costs one CAS per
reservation.

The query result envelope (TASK-511) includes a `peak_memory_bytes`
field so callers can size their host-process budget without parsing
metrics text.

---

## 11. Performance Notes

The reservation hot path is on every sort / aggregate / distinct /
sequence-match output emission. Two performance constraints follow:

1. **No allocation past the closure box.** The current
   `MemoryReservation` boxes a release callback — one heap allocation
   per reservation. Wave 5 benchmarks (TASK-525 / TASK-526) measure the
   amortized cost. If it shows up in profiles, the alternative is to
   shrink the reservation to a `(bytes, *const MemoryTracker)` tuple
   and call `tracker.release(bytes)` manually on drop — keeping the
   trait but adding a non-allocating impl. The trait surface does not
   change.
2. **Reservations are coarse, not fine.** Operators reserve "enough
   for this batch" or "enough for the next 1024 rows", not "enough
   for the next row". The morsel scheduler bounds in-flight work, so
   any operator can amortize its reservations across at least a tile
   (default 2 048 rows). Per-row reservation is forbidden in any new
   operator.

The spill slow path is allowed to allocate freely — it is already a
disk-bound operation by orders of magnitude.

---

## 12. Non-Goals & Open Questions (Beyond Wave 5)

- **Cross-query global accounting.** No engine-wide ceiling is
  enforced in v1; concurrent queries each carry their own 3 GiB
  budget. A future wave can add a parent tracker without changing the
  trait. Until then, callers wanting a global ceiling must enforce it
  themselves at the API surface.
- **Dynamic budget resizing.** Per-query budget is fixed at submission
  time. Mid-query resizing is not supported.
- **OS RSS reconciliation.** The budget tracks logical bytes
  (Arrow buffer lengths, hash-table allocations). It does not track
  RSS or fragmentation. Process-level overhead is the host's
  responsibility.
- **NUMA-aware sub-budgets.** Same answer; the trait could
  accommodate it later.
- **Compaction / ingest unification under one parent budget.** Out of
  scope. § 2.2 explains why.
- **Soft-pressure spilling.** Operators only spill on hard `try_reserve`
  failure. A future wave could add a "soft-pressure" hook that fires
  before the budget is exhausted, letting operators proactively spill.
  This requires runtime coordination across operators (who spills
  first?) and is deferred.

---

## 13. Reconciliation Checklist

Updated in the same checkpoint as this file:

- `core-beliefs.md` Belief 6 — clarify that "default 4 GB" is the
  engine-wide aggregate; query share is 3 GB.
- `reliability.md` § Memory Budget — same clarification, point at this
  doc.
- `storage-format.md` § 13 — keep the table, add a leading paragraph
  pointing at this doc as the canonical query-budget owner; bump
  ingest from 200 MB to 256 MB to match the engine; reword the
  percentage column to clarify the percentages are illustrative, not
  enforced ratios.
- `execution-model.md` § 10.1 — drop the "hierarchical memory tracker"
  wording, rewrite as a short cross-reference to this doc, keep the
  per-worker working-set table in § 10.2 as the planning artefact it
  was always intended to be, and update the "3 GB query budget"
  references to read "3 GiB query budget (see
  `engine/memory-budget.md`)".
- `operators/operator-traits.md` § 2 (deferred-types table) and § 7
  (Wave 1 deferral 6) — clarify the `Arc<dyn MemoryBudget>` plumbing
  now that it has an owner. § 5.3 (cancellation-token distribution)
  is the symmetric pattern and is referenced rather than rewritten.
- `operators/sort-distinct.md`, `operators/aggregate-operator.md`,
  `operators/sessionize.md`, `operators/match-operator.md` — replace
  forward references like "`MemoryBudget` integration: TASK-111" with
  "`MemoryBudget` integration: TASK-510 per
  `engine/memory-budget.md`".
- `language/cohorts-aliases-joins.md` § 2.7 / § 7 — point the cohort
  out-of-budget caveat at this doc; flag the spill-vs-fail decision as
  TASK-502.
- `INDEX.md` — add the `engine/` section with this entry.

The detailed text changes land in the same diff as this file.

---

## 14. References

1. `bqlite-core::memory` — `MemoryBudget`, `MemoryReservation`,
   `SpillNotification`, `UnboundedMemory` (TASK-111).
2. `execution-model.md` § 3.3 (`QueryContext`), § 4 (`EntityOperator`
   error channels), § 9 (morsel scheduler), § 10 (memory management
   summary).
3. `storage-format.md` § 13 (engine-wide budget split).
4. `operators/operator-traits.md` § 5 (`CancellationToken`
   distribution pattern), § 7 (Wave 1 deferrals).
5. `operators/sort-distinct.md` (sort/distinct caps and spill
   forward-reference).
6. `operators/aggregate-operator.md` (hash-aggregate hard cap).
7. `operators/sessionize.md`, `operators/event-select-sample.md`,
   `operators/attribute.md`, `operators/match-operator.md`
   (per-entity caps).
8. `language/cohorts-aliases-joins.md` § 2.7 (cohort out-of-budget
   semantics).
9. TASK-502, TASK-505, TASK-510, TASK-511, TASK-512, TASK-513,
   TASK-514, TASK-525 (downstream implementation tasks).
