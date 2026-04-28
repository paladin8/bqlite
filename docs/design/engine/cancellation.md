# Cancellation, timeout, and warning protocol

**Wave**: 5
**Task**: TASK-505
**Status**: draft — frozen for Wave 5; consumed directly by TASK-511 (structured execution errors + warning channel) and TASK-513 (sort spill cleanup).

## 1. Scope

This note freezes the runtime contract for everything that takes a query off
its happy path *without corrupting state*. It covers:

- Caller cancellation (Ctrl-C, drop-on-shutdown, programmatic cancel handle).
- Query timeouts.
- Worker panics inside `process_sub_batch` / `next_batch`.
- Cleanup of memory reservations and spill files on every exit path.
- Non-fatal operator warnings — the `QueryWarning` channel.
- The typed errors callers see, and the `BqliteError` mapping that produces
  them.
- The latency bounds an operator must honour at every yield point.

The doc is a *protocol* spec. It does not introduce new operator algorithms;
it pins down how the existing operators, the `EntityOperatorAdapter`, and the
engine orchestration layer must behave at error / cancellation / warning
boundaries so that downstream tasks (TASK-511 structured errors, TASK-513
sort spill, TASK-510 memory tracker, TASK-541 morsel scheduler) can plug
into a stable contract.

It is **not** a re-design of `CancellationToken`. The existing token type
(`bqlite_operators::CancellationToken`) is the canonical signal carrier and
this doc only reinforces the rules for using it. It is also **not** a
replacement for `execution-model.md` §3.3, §3.4, §10.3, §12 — it is the
implementation note that turns those Wave 0 sections into a protocol the
engine can implement.

## 2. Existing surface today

What landed in earlier waves and whose semantics this doc must preserve:

- `bqlite_operators::CancellationToken` — `Arc<AtomicBool>` clone-shared
  across operators, set by `cancel()`, polled by `is_cancelled()`. Wave 1
  scaffold (TASK-108). Already integrated into `ScanOperator`,
  `ProjectOperator`, `LimitOperator`, `SortOperator`, `DistinctOperator`,
  and the segment-merge driver. Wave 5 keeps the type unchanged.
- `bqlite_core::BqliteError::Cancelled` — the unified variant every operator
  returns when the token is set. Wave 1 (TASK-102). No timeout variant
  exists yet; this doc adds one.
- `Engine::query` / `Engine::execute` — single-threaded driver in
  `bqlite-engine`. Wave 1 (TASK-118). Holds no `QueryContext`, no timeout
  timer, no warning sink, no metrics.
- `SessionizeOperator` and `AttributeOperator` — already record per-entity
  cap overflow on their `State` (sessionize.md §11.3, attribute.md §10).
  They have nowhere to publish those events; the warning channel in §5
  picks them up.
- Sort and IN-subquery spill (TASK-513, TASK-514) — not yet implemented.
  This doc fixes the cleanup contract those tasks must honour.

The execution-model.md §12 sketch of `OperatorError` / `ExecutionError` is
**superseded** by this note: per `operator-traits.md` §2, all operator-side
errors are unified under `bqlite_core::BqliteError`, and the engine surfaces
them through `BqliteError::Timeout` rather than introducing a separate
`ExecutionError` enum. The mechanical update to execution-model.md §12 is
**owned by TASK-511**, which ships the new `BqliteError` variants in the
same checkpoint as the doc rewrite. This note (TASK-505) freezes the
protocol but does not edit execution-model.md, because the variants it
references do not exist on `main` yet — landing the doc rewrite ahead of
the variants would leave a self-inconsistent sister doc.

## 3. Cancellation signal flow

### 3.1 Sources

A query enters the cancellation path through exactly four sources. They are
unified by the same `CancellationToken` so operators only ever observe
"cancelled / not cancelled":

| Source | Mechanism | Maps to terminal error |
|---|---|---|
| Caller-initiated cancel | Caller invokes `QueryHandle::cancel()` (TASK-541); engine calls `token.cancel()` | `BqliteError::Cancelled` |
| Query timeout | Engine's per-query timer fires after `QueryContext::timeout`; sets a `timed_out` flag *before* `token.cancel()` | `BqliteError::Timeout { elapsed_ms }` |
| LIMIT short-circuit | The `LimitOperator` calls `token.cancel()` once it has produced the requested rows | `Ok` — propagates up as success once the in-flight `next_batch()` returns |
| Worker panic | A worker's `catch_unwind` boundary (see §4) calls `token.cancel()` so peer workers stop quickly | `BqliteError::OperatorPanic { message, location }` |

There is exactly one boolean signal because "race-free single-writer" is too
restrictive — multiple sources can race to set the flag and the engine must
still produce a deterministic error. The race is resolved by **first-fire
attribution**, with one strictly higher-priority slot for panics:

```rust
#[repr(u8)]
pub enum CancelReason {
    None     = 0,
    Cancelled = 1,
    Timeout   = 2,
    LimitHit  = 3,
}

pub struct QueryContext {
    cancelled: Arc<AtomicBool>,
    /// First-fire reason for the cooperative cancel paths. Stored as a
    /// `u8` because `AtomicEnum` is not in the standard library; the
    /// discriminants above are the canonical mapping.
    reason: AtomicU8,
    /// Panic always wins. Set by the worker's `catch_unwind` handler
    /// (§4) before flipping `cancelled`. A non-empty slot here overrides
    /// `reason` at result collection.
    panic_payload: Mutex<Option<PanicPayload>>,
    // ... timeout, memory tracker, ... (existing fields from
    // execution-model.md §3.3)
}
```

Each cooperative source CAS-installs its reason (`None → Self`) before
flipping `token.cancel()`. A second fire (e.g. caller cancels a query that
already timed out) loses the CAS and is silently discarded. Panics use a
separate slot because a panic during teardown of a timed-out query must
still surface as `OperatorPanic` — bugs are always more important than
the cancellation reason that preceded them.

**Precedence rule (single source of truth).** At result collection the
engine checks slots in this order:

1. `panic_payload` is `Some` → `BqliteError::OperatorPanic`.
2. `reason` is `Cancelled` → `BqliteError::Cancelled`.
3. `reason` is `Timeout` → `BqliteError::Timeout { elapsed_ms }`.
4. `reason` is `LimitHit` → `Ok(...)` (LIMIT short-circuit; the in-flight
   results were already collected before the token fired).
5. `reason` is `None` and `cancelled` is `false` → normal completion.

Cases 1–4 cover every cooperative-cancel exit path; case 5 is the happy
path. The CAS rule + this precedence rule are exhaustive — operators
never read `reason` themselves.

### 3.2 Yield points

Operators check `token.is_cancelled()` at three boundaries, each with a
documented latency target:

| Boundary | Where it is checked | Latency target |
|---|---|---|
| Batch | Top of `PhysicalOperator::next_batch()`, before pulling input | ≤ one batch's processing time (default ~10 ms for a 64K-row batch on the reference machine) |
| Sub-batch | `EntityOperatorAdapter` between `process_sub_batch()` calls (i.e. between sub-batches of one entity, or after `finish_entity()`) | best-effort, bounded by sub-batch size — concrete target ≤ ~10 ms for the default 65,536-row sub-batch on the reference machine, scaling linearly with sub-batch row count |
| Morsel | `MorselGenerator::next_morsel()` and at every worker handoff | ≤ one morsel's processing time |

These three boundaries are exhaustive. Operators do **not** poll
`is_cancelled()` inside the per-event loop, inside the per-tile kernel
loop, or inside the per-row materialization helpers. A `Relaxed` atomic
load is cheap but the resulting branch perturbs autovectorization on
hot loops, and the latency win is negligible — a tile is at most 4,096
rows and runs in tens of microseconds, well below any user-visible
cancel response budget.

The MATCH operator's NFA fast path (`sequence-matching.md` §10) and the
fused stateless segment (`execution-model.md` §3.8) both honour this rule:
they expose an outer `next_batch()` boundary and an inner per-event /
per-tile loop, and only the outer boundary checks the token.

**Exception — long-running spill writes.** Sort and IN-subquery spill
writers (TASK-513, TASK-514) check `is_cancelled()` between row-groups
inside the spill loop. The sort writer's row-group is the same 64K-row unit
the in-memory sort uses, so the latency target is identical to a
`next_batch()` boundary. This exception is documented per-operator in the
relevant design note (`sort-distinct.md` §6 once TASK-513 lands), not
encoded in the trait.

### 3.3 Required check sites — current + Wave 5 additions

The current operator set (Wave 4) already polls the token at the right
boundaries. The audit:

| Operator | Boundary | Site |
|---|---|---|
| `ScanOperator` | batch | top of every `next_batch()` (segment scan, k-way merge, joined-source scan) |
| `ProjectOperator` | batch | top of `next_batch()` |
| `LimitOperator` | batch | top of `next_batch()`; also *fires* `token.cancel()` when the row budget is satisfied (sets `CancelReason::LimitHit` per §3.1) |
| `SortOperator` | batch + per-pull | top of `next_batch()` and inside the input-drain loop (between child pulls) |
| `DistinctOperator` | batch | top of `next_batch()` |
| `EntityOperatorAdapter` | sub-batch | not yet wired — see TASK-511 acceptance criteria below |

Wave 5 adds two more required sites:

1. **`EntityOperatorAdapter::next_batch()`** — polls between sub-batches
   per `execution-model.md` §4.1. Must fire after `finish_entity()` and
   before `create_state()` for the next entity, so a slow per-entity
   operator cannot trap inside one entity for an unbounded period.
2. **`MorselGenerator::next_morsel()`** — polls before handing the morsel
   to the worker, so a query that cancels mid-shard does not start a fresh
   morsel.

`AggregateOperator`, `MatchOperator`, `SessionizeOperator`,
`AttributeOperator`, and the `EventSelectOperator` family are stateful —
they live behind the adapter and inherit its sub-batch poll. They do not
need their own poll site.

## 4. Panic handling

### 4.1 Catch boundary

The engine wraps every worker's per-morsel execution in `catch_unwind`.
The boundary lives in the morsel runner that drives a `(worker, shard)`
session, not in `Engine::query`:

```rust
let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
    self.run_morsel(morsel)
}));
match result {
    Ok(Ok(())) => { /* normal */ }
    Ok(Err(e)) => self.fail_query(e),
    Err(payload) => self.fail_query(BqliteError::OperatorPanic {
        // `panic_message` extracts a `String` from the `Box<dyn Any>`
        // payload by trying `downcast_ref::<&'static str>()` then
        // `downcast_ref::<String>()`, falling back to "<non-string panic
        // payload>". `panic_location` reads the most recent panic
        // location captured by a project-local panic hook installed by
        // the engine at startup (the standard panic hook discards it
        // before `catch_unwind` returns). Both helpers ship in
        // `bqlite-engine` alongside the worker runner — TASK-541.
        message: panic_message(&payload),
        location: panic_location(),
    }),
}
```

`AssertUnwindSafe` is correct here because (a) the worker owns its
`WorkerContext` exclusively, (b) `QueryContext` is `Sync` and shared via
`Arc` but is never interior-mutated by operators (operators read the
cancel flag and reserve/release through `MemoryTracker`'s atomics —
neither is observed-broken-on-unwind), and (c) operator-side state
lives on `EntityOperator::State` which the worker also owns. The worker
takes responsibility for keeping its operator state `UnwindSafe`-by-
construction; an operator that needs interior mutation across
`process_sub_batch` calls already has to choose `Cell` / `RefCell` /
`Mutex` deliberately, and unwind-safety is an additional consideration
in that choice.

A panic that escapes the morsel boundary is treated as a bug — the panic
payload becomes the structured error the user sees, but the database
state itself remains consistent because every panic-prone operator
(sort buffer, hash accumulator, sessionize state) holds RAII guards
that release on unwind (§5.2).

### 4.2 Peer worker shutdown

When a worker observes a panic, the catch handler calls `token.cancel()`
*before* publishing the failure to the coordinator. This wakes peer
workers at their next yield point so they do not waste CPU on a query
that is already dead. The CAS in §3.1 records `CancelReason::Panic`, so
subsequent peer-worker errors are coalesced into the panic instead of
masking it.

### 4.3 Mapping to `BqliteError`

```rust
pub enum BqliteError {
    // ... existing variants ...
    /// The query was cancelled by the caller via QueryHandle::cancel().
    Cancelled,
    /// The query exceeded its configured timeout. Carries the elapsed
    /// time in milliseconds for diagnostics.
    Timeout { elapsed_ms: u64 },
    /// A worker panicked while executing the query. The `message` is the
    /// panic payload (best-effort `Display`), and `location` is the
    /// `file:line:column` of the panic site when the standard panic hook
    /// captured it.
    OperatorPanic { message: String, location: Option<String> },
}
```

`Timeout` and `OperatorPanic` are new variants that this doc commits the
engine to. TASK-511 lands them, with the caveat that `OperatorPanic`
propagation through the worker pool depends on TASK-541 (morsel scheduler).
Until TASK-541 ships, the single-threaded `Engine::query` driver from
TASK-118 wraps its `bind_physical → next_batch` loop in the same
`catch_unwind` and surfaces `OperatorPanic` directly.

## 5. Resource cleanup

### 5.1 Cleanup ordering

Every query exit path — success, `BqliteError::Cancelled`,
`BqliteError::Timeout`, `BqliteError::OperatorPanic`, or any other typed
error — runs the same teardown sequence in deterministic order:

1. **Operators.** The root operator's `Drop` impl tears down the operator
   tree top-down. Every operator's `Drop` closes its child reference,
   which cascades. `PhysicalOperator::close()` is reserved for resources
   that *must* be released before drop (rare in the Rust idiom where
   `Drop` is sufficient); operators that override `close()` re-run the
   logic in their `Drop` to keep cancel/panic paths correct, because the
   engine's normal driver invokes `close()` only on success.
2. **Spill files.** Every spill writer holds a `TempSpillFile` RAII guard
   (§5.2) — its `Drop` impl deletes the on-disk file. Because `Drop` runs
   on every exit path, including unwinding from a panic, no spill file
   survives a query failure. The order is leaf-operator-first because
   spill files hang off operator state.
3. **Memory reservations.** `MemoryReservation` is also RAII (TASK-510).
   It releases bytes back to `MemoryTracker` on drop. By the time
   step 4 runs, the query's tracked bytes have returned to zero.
4. **Worker contexts.** Each worker drains its `WorkerContext` (metrics,
   warnings) into the coordinator. On panic / cancel, this drain still
   happens — partial metrics and partial warnings are published. The
   coordinator then assembles `ExecutionResult { warnings, metrics, ... }`
   when the query succeeds, or `BqliteError` when it fails. Either way,
   the warnings the operators have already recorded are not silently
   dropped: they are passed through the engine's failure surface (§5.4).
5. **Cancellation token.** The token is dropped with the
   `QueryContext`. Subsequent observers see "cancelled" until the entire
   `Arc` chain is freed; this is harmless because the query is over.

This ordering is enforced structurally (RAII), not by an explicit
"cleanup function". Operators must not invent their own teardown
sequences.

### 5.2 `TempSpillFile` RAII guard

```rust
/// Owns an on-disk spill file and removes it when dropped.
///
/// Created via `QueryContext::open_spill(purpose)` (which delegates to
/// `bqlite_core::spill::SpillFs::open_spill`) so the engine controls
/// the directory layout and the per-query subdirectory lifecycle. Drop
/// is best-effort: a deletion failure is silently swallowed (because
/// Drop must not unwind during another unwind); residual files are
/// reclaimed by the belt-and-braces per-query sweep
/// (`engine/spill.md` § 8.3) and the engine-open root sweep
/// (`engine/spill.md` § 5.4 / § 9.1).
pub struct TempSpillFile {
    path: PathBuf,
    file: File,
    bytes_written: u64,
}

impl Drop for TempSpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
```

Spill writers (sort, IN-subquery, ingest partitioner) hold a
`TempSpillFile` per active run / partition. Drop deletes the file on
every exit path. The guard is the single source of truth for spill-file
lifetime — operators may not call `remove_file` directly.

**Crash safety.** A process crash mid-query leaves spill files on disk
because `Drop` does not run. The engine recovers by reclaiming the
entire spill root at engine open under the existing database flock —
see [`engine/spill.md`](spill.md) § 5.4 / § 9 for the crash-recovery
model and the rationale (the database lock guarantees no other live
process can be using the spill tree, and spill files have no
cross-startup meaning). The pid-pattern filename sweep originally
sketched here is superseded by that simpler model.

**No filesystem fsync on the spill path.** Spill files are temporary by
construction — durability is unnecessary, and the sync cost would
dominate small spills.

### 5.3 Spill-directory layout

All spill files for a given query land under
`<spill_root>/<query_id>/<purpose>-<seq>.spill`, where `<spill_root>`
defaults to `<db_root>/spill/` and is configurable via
`Engine::with_spill_root`. The per-query subdirectory makes cleanup
trivial: on any exit path, the engine `rm -rf`s the per-query
subdirectory after the operator-tree drop has run, as a belt-and-braces
sweep against any guard that failed to delete its file. The subdirectory
is created lazily on the first spill so cancel-before-spill paths leave
no trace. The full path scheme, the `<purpose>` tags
(`sort-run`, `ingest-part-w<window>-s<shard>`), and the per-purpose
file payload (Arrow IPC stream) are owned by
[`engine/spill.md`](spill.md) § 6 / § 7.

### 5.4 Warnings on the failure path

Warnings recorded *before* a query fails are surfaced even when the
final result is an error. `Engine::query` returns
`Result<ExecutionResult, BqliteError>` today, and `BqliteError` cannot
carry a vec of warnings without breaking pattern-match arms in every
caller. TASK-511 owns the API change; this doc commits to the shape.

**Migration path (TASK-511).** The `Engine::query` signature changes to
`Result<ExecutionResult, ExecutionFailure>`:

```rust
pub struct ExecutionResult {
    pub schema: OperatorSchema,
    pub rows: Vec<RecordBatch>,
    pub rows_affected: Option<u64>,
    /// Warnings collected during execution. Empty for queries that
    /// produce no diagnostics. See §7.
    pub warnings: Vec<QueryWarning>,
}

/// Wrapper attached when the engine wants to publish partial diagnostics
/// alongside a fatal error. CLI and Python bindings format the warnings
/// after the error message.
pub struct ExecutionFailure {
    pub error: BqliteError,
    pub warnings: Vec<QueryWarning>,
}

impl From<BqliteError> for ExecutionFailure {
    fn from(error: BqliteError) -> Self {
        Self { error, warnings: Vec::new() }
    }
}

impl ExecutionFailure {
    /// Pattern-friendly extraction for callers that only want the error.
    pub fn into_error(self) -> BqliteError { self.error }
}
```

This is a breaking change for callers that match on `Err(BqliteError)`,
which TASK-511 absorbs in the same checkpoint:

- The internal driver code keeps using `BqliteError`; the wrapper is
  produced once at `Engine::query`'s outermost return.
- The CLI and Python bindings (the only callers today) update to
  pattern-match on `Err(ExecutionFailure { error, warnings })`.
- A `From<BqliteError>` impl makes the engine's `?` propagation continue
  to work; the failure case wraps with `warnings: Vec::new()` and the
  driver stitches the partial warnings in at the boundary before
  returning.

A non-breaking alternative would be a sidecar method
(`Engine::take_warnings(&mut self) -> Vec<QueryWarning>`), but that
hides warnings behind a separate API surface and forces every caller to
know about it. The signature change is one-shot and keeps the surface
self-documenting.

## 6. Typed errors

### 6.1 Reconciliation with execution-model.md §12

execution-model.md §12.1 sketched two enums (`OperatorError`,
`ExecutionError`). Operators and the engine have since unified on
`bqlite_core::BqliteError` (operator-traits.md §2), so the §12 sketch is
no longer accurate. This doc settles the reconciliation:

- Drop the `OperatorError` / `ExecutionError` distinction. All errors
  are `BqliteError`.
- `BqliteError::Cancelled` covers caller cancellation and
  LIMIT-short-circuit (the LIMIT case never reaches the user — it is
  observed as `Ok` by the driver).
- Add `BqliteError::Timeout { elapsed_ms }` for timeout-triggered
  cancellation.
- Add `BqliteError::OperatorPanic { message, location }` for panic
  propagation.
- Existing variants (`Io`, `Arrow`, `Schema`, `Plan`, `Execution`,
  `Corruption`) keep their current semantics. TASK-511 also replaces
  several `BqliteError::Execution(String)` sites with structured
  variants (e.g. `MemoryBudgetExceeded { used, budget }`,
  `MaxGroupsExceeded { limit }`), per the dependency from TASK-510.

The execution-model.md §12 rewrite — replacing the stale
`OperatorError` / `ExecutionError` sketch with a pointer to this note —
is owned by TASK-511 (§2 above and §8 below), not this checkpoint. The
two cannot be split: the doc rewrite references variants
(`BqliteError::Timeout`, `BqliteError::OperatorPanic`) that TASK-511
introduces, so the doc and the variants must land together.

### 6.2 Cancellation vs. timeout precedence

The first-fire CAS rule and the panic-always-wins precedence are
specified once in §3.1; this subsection is intentionally brief to avoid
duplicating the rule. The user-visible behaviour follows: "why did this
query stop?" answers "the timer expired" *or* "I asked for it to stop"
*or* "an operator panicked", not a combination. A timed-out query that
then panics during teardown surfaces the *panic*, not the timeout —
panics are bugs and must be visible.

## 7. Warning protocol

### 7.1 The `QueryWarning` enum

**Crate placement.** `QueryWarning` lives in `bqlite-core` alongside
`BqliteError` so that both `ExecutionResult::warnings` (engine surface)
and `EntityOperator::take_pending_warnings` (operators surface) can
reference it without violating the dependency direction
(`core → operators → engine`). This mirrors the placement of
`BqliteError` itself: protocol types that cross the operators/engine
boundary belong in `bqlite-core`.

```rust
#[derive(Debug, Clone)]
pub enum QueryWarning {
    /// The entity event limit (default 10M) was reached for one entity;
    /// remaining events for that entity were dropped. Per
    /// execution-model.md §5.3.
    EntityEventLimitExceeded {
        entity_id: String,
        count: u64,
        limit: u64,
    },
    /// Sessionize per-entity event cap (default 1M, sessionize.md §11.3)
    /// was reached; remaining events for that entity were dropped.
    SessionEventCapExceeded {
        entity_id: String,
        event_count: u64,
        cap: u64,
    },
    /// Attribute per-entity touchpoint cap (attribute.md §10) was
    /// reached; remaining touchpoints for that entity were dropped.
    AttributeTouchpointCapExceeded {
        entity_id: String,
        touchpoint_count: u64,
        cap: u64,
    },
    /// Match operator's active-state cap (match-operator.md §13.3) was
    /// reached for one entity; further state expansion was suppressed.
    ActiveStateLimitExceeded {
        entity_id: String,
        active_states: u64,
        cap: u64,
    },
    /// One or more workers exceeded the per-worker warning cap and
    /// silently dropped further warnings. Aggregated by the coordinator
    /// — the user sees a single `WarningsOverflow` even when many
    /// workers hit the cap.
    WarningsOverflow { suppressed_count: u64 },
}
```

The enum is exhaustive at Wave 5 entry. Future operators that need a new
warning variant add a case (no `#[non_exhaustive]` attribute — exhaustive
matching is part of the published API so callers can render every variant
with full context).

### 7.2 Per-worker channel

Each `WorkerContext` owns a `Vec<QueryWarning>` and a counter:

```rust
pub struct WorkerContext {
    /// ... existing fields ...
    pub warnings: Vec<QueryWarning>,
    pub warning_overflow: u64,
}

impl WorkerContext {
    pub fn record_warning(&mut self, warning: QueryWarning) {
        if self.warnings.len() < Self::PER_WORKER_WARNING_CAP {
            self.warnings.push(warning);
        } else {
            self.warning_overflow = self.warning_overflow.saturating_add(1);
        }
    }
    pub const PER_WORKER_WARNING_CAP: usize = 1_000;
}
```

The cap is **1,000 entries per worker**, matching execution-model.md
§12.2. The cap exists because some workloads — bot-heavy datasets, runaway
sessions, or pathological matchers — can produce one warning per entity
across millions of entities. An unbounded `Vec` would either OOM the
query or starve the warning consumer.

The cap is per-worker, not per-query, so the visible total is at most
`num_cores * 1_000 = 32_000` warnings on a 32-core machine. This is
deliberate: per-query capping would force atomic coordination in the
hot path; per-worker capping needs no synchronisation. The coordinator
sums the suppressed counts across workers when assembling the final
warning list (§7.3).

### 7.3 Coordinator merge

When every worker for a shard finishes its last morsel, the coordinator
drains each `WorkerContext` and folds the warnings into a per-query
`Vec<QueryWarning>`. The merge:

1. Concatenates `worker.warnings` from every worker into a flat vec, in
   worker-id order. Within one worker the order is the order in which
   warnings were recorded; across workers there is no ordering guarantee
   beyond worker-id stability.
2. Sums every worker's `warning_overflow` into a single `total_overflow`.
   If `total_overflow > 0`, the coordinator appends a final
   `QueryWarning::WarningsOverflow { suppressed_count: total_overflow }`.
   `WarningsOverflow` MUST be the last element of the assembled vec —
   the CLI's "N further warnings suppressed" rendering depends on this
   ordering, and `EntityOperator` implementors MUST NOT emit
   `WarningsOverflow` themselves (only the coordinator does).
3. The resulting vec is attached to `ExecutionResult::warnings` (success)
   or `ExecutionFailure::warnings` (failure).

No deduplication, no merging of repeated `EntityEventLimitExceeded`
warnings for the same entity — the per-entity attribution is the user's
diagnostic signal and folding it would lose information.

### 7.4 Recording from operators

Stateful operators record warnings through the adapter, not directly:

- `EntityOperator` implementors stash the *warning trigger* on their
  `Self::State`. `SessionizeState`, `AttributeState`, and `MatchState`
  already store these triggers today (e.g. `SessionizeState`'s
  `event_cap_exceeded` boolean per sessionize.md §11.3, plus the
  observed event count). What they do **not** yet have is an accessor
  to publish those triggers as fully-formed `QueryWarning` values — the
  enum did not exist, so the trigger had nowhere to go.
- TASK-511 adds the missing accessor:
  `EntityOperator::take_pending_warnings(&mut state, entity_id) ->
  Vec<QueryWarning>`. The default implementation returns an empty vec;
  Sessionize, Attribute, and MATCH override it to convert their stashed
  trigger into the matching `QueryWarning` variant, attaching the
  `entity_id` the adapter passes in.
- `EntityOperatorAdapter` calls `take_pending_warnings()` after
  `finish_entity()` and forwards each warning to
  `WorkerContext::record_warning()`. This is the single edge between
  operator state and the warning channel. Operators do **not** see
  `WorkerContext`.
- Stateless operators (filter, project, scan) do not produce warnings in
  Wave 5. Future memory-pressure diagnostics from the memory tracker
  (TASK-510) flow through the same adapter forwarding path: the operator
  stores a warning on its `Self::State` (or, for stateless operators, on
  a dedicated slot exposed by the engine), the adapter or driver picks
  it up, and it lands in `WorkerContext`.

This indirection keeps the operator hot path free of `&mut WorkerContext`
threading. Operators see only the cancellation token and (eventually)
`MemoryReservation` — no engine-orchestration types leak across the
crate boundary.

### 7.5 Result surfacing

`ExecutionResult::warnings` is part of the public surface. The CLI prints
warnings after the result body, formatted as:

```
3 warnings:
  - entity event limit exceeded: entity=u_42, count=10000001, limit=10000000
  - session event cap exceeded: entity=u_99, count=1000001, cap=1000000
  - 12 further warnings suppressed
```

The Python binding exposes warnings as a list of dicts on the result
object so they can be inspected programmatically.

The `WarningsOverflow` variant is always last in the list when present —
the CLI's "N further warnings suppressed" rendering depends on this
ordering.

## 8. Implementation breakdown

This doc is the prerequisite for several Wave 5 implementation tasks. The
mapping:

| Task | Section | Scope |
|---|---|---|
| TASK-510 (memory tracker) | §5.1, §5.2 | Defines the `MemoryReservation` RAII guard; this doc fixes its drop-order relative to spill files. |
| TASK-511 (structured errors + warnings) | §6, §7 | Adds `BqliteError::Timeout`, `BqliteError::OperatorPanic`, the `QueryWarning` enum, the `WorkerContext` warning slot, and the `ExecutionResult::warnings` field. Updates execution-model.md §12 in the same checkpoint. |
| TASK-512 (ingest spill) | §5.2, §5.3 | Uses `TempSpillFile` for partitioner spill; honours the per-query subdirectory layout and the startup orphan sweep. |
| TASK-513 (sort spill) | §3.2 exception, §5.2, §5.3 | Sort spill writer polls cancellation between row-groups; spill runs use `TempSpillFile`; merge pass reads through OS cache. |
| TASK-514 (cohort spill / fail) | §5.1 | Cohort materialization either spills (using §5.3 layout) or fails fast with `BqliteError::MemoryBudgetExceeded`; either way honours the cleanup ordering. |
| TASK-541 (morsel scheduler) | §3.1, §3.2, §4 | Implements per-query timeout timer, the morsel-boundary panic catch, the peer-worker shutdown via `token.cancel()`, and the `CancelReason` CAS. |

TASK-505 itself ships only this design note — no code change. The
implementation tasks above are gated on this doc landing on `main`.

## 9. Open questions

- **CTRL-C in the CLI vs. embedded use.** The CLI installs a SIGINT
  handler that calls `QueryHandle::cancel()`. Embedded callers (Python,
  C FFI) get a programmatic cancel handle but not an automatic SIGINT
  hook — they must register their own. This is documented but not
  enforced. Open question: should the engine refuse to install a
  default SIGINT handler so embedders can choose? Resolution path:
  TASK-541 will pick a default; this doc does not commit either way.
- **Warning surfacing for streaming results.** Wave 5 ships only the
  fully-materialized result API. If a future wave introduces a
  streaming iterator (`QueryStream`), warnings will need to flush at
  stream end, not at first batch. This is out of scope for TASK-511.
- **Compaction interaction.** Compaction failures do not interact with
  query cancellation — they are reported through a separate channel
  (`compaction-concurrency.md` §9). A query that runs concurrently
  with a failing compaction is unaffected.

These do not block any Wave 5 implementation work. They are cataloged so
later waves do not surprise themselves.

## 10. Decision summary

| Question | Decision | Rationale |
|---|---|---|
| Cancellation signal carrier | `Arc<AtomicBool>` token; first-fire CAS for reason | One signal, deterministic attribution, branch-free hot path |
| Yield-point granularity | Batch / sub-batch / morsel only — never per-event or per-tile | Atomic load is cheap but the branch perturbs vectorization; per-batch latency is already acceptable |
| Timeout mechanism | Per-query timer thread sets `CancelReason::Timeout`, then `token.cancel()` | No polling overhead; reuses the existing token plumbing |
| Panic boundary | `catch_unwind` per `(worker, morsel)` | Bounded scope; peer workers exit at next yield via cascading `token.cancel()` |
| Spill-file lifetime | `TempSpillFile` RAII; per-query subdirectory under `<spill_root>` | Drop runs on every exit path including unwind; per-query subdir simplifies belt-and-braces cleanup |
| Crash recovery | Startup sweep of `<spill_root>` for non-live PIDs | Mirrors compaction-concurrency.md §6 orphan cleanup |
| Warning channel | Per-worker `Vec<QueryWarning>` cap of 1000; coordinator sums overflow | No hot-path synchronisation; bounded memory; explicit suppressed count for visibility |
| Warning recording site | Operator `Self::State` → adapter forwards on `finish_entity` | Keeps `WorkerContext` out of operator hot path |
| Result surface | `ExecutionResult::warnings` on success, `ExecutionFailure::warnings` on failure | Warnings recorded before failure are not silently dropped |
| Error enum | Extend `BqliteError` with `Timeout` and `OperatorPanic`; retire `OperatorError` / `ExecutionError` from execution-model.md §12 | Single error type matches operator-traits.md §2 reconciliation |
| Cancellation vs timeout precedence | First-fire CAS wins; panic always overrides | Matches user-facing "why did this stop?" expectation |
