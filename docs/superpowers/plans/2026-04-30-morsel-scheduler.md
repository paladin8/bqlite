# TASK-523: Morsel scheduler + partial-aggregate handoff — implementation plan

**Date**: 2026-04-30
**Task**: TASK-523 (Wave 5, [HARD][IMPL])
**Design doc**: `docs/design/engine/morsel-scheduler.md` (TASK-506)

## Scope

Land the scaffolding contract described by the morsel-scheduler design doc:

- `CoreBudget::acquire_n` atomic batch acquisition (§7.1).
- `Morsel`, `MorselGenerator`, `MorselQueue`, `MorselSizePolicy`,
  `MorselSizeState`, `WorkerContext`, `AccumulatorHandle`, `WorkerHandle`,
  and the `MorselScheduler` engine-level coordinator (§3, §4, §5, §6).
- Engine-level FIFO query queue + Rayon worker pool, sized at
  `query_threads = num_cores` (§5.1, §5.2).
- Replace `Engine::query`'s single-threaded `drive_to_completion`
  with a morsel-scheduler entry point that:
    - Acquires `query_threads` permits from the shared `CoreBudget`.
    - Dispatches the query as **one degenerate whole-query morsel**
      via the morsel queue to a Rayon worker.
    - Returns the worker's result back to the calling thread.
- Reconcile `execution-model.md` §9 / §11 / §14, plus
  `compaction-concurrency.md` §4.4 / §12, and add this scheduler doc
  to `docs/design/INDEX.md`.

## Out of scope

The design doc anticipates that the implementation lands in stages.
Per-operator morsel-awareness — that is, parameterizing scan / filter /
aggregate so each operator instance only sees a single
`(shard_id, [entity_lo, entity_hi))` morsel — is **not** in this
checkpoint. Operators continue to walk every shard end-to-end inside
the worker. The scheduler infrastructure is built so the conversion
to genuine multi-morsel parallelism is purely additive in follow-on
tasks (i.e. swapping the degenerate one-morsel generator for the
real per-shard generator and adding a per-shard scan-restriction
parameter).

This boundary is consistent with the design doc's §11 migration plan
and the v2 deferrals listed in §13.

## Dependencies / coordination

- TASK-506 design doc — already in tree.
- TASK-501 / TASK-510 (memory tracker) — already wired through
  `QueryContext`.
- TASK-505 / cancellation, panic isolation — already in
  `QueryContext.cancellation()`.
- `crossbeam` + `rayon` — new workspace dependencies; both are
  industry-standard, well-maintained, and explicitly named by the
  design doc.

## Checkpoints

### CP1 — `CoreBudget::acquire_n` (storage)

- New file: none. Edit `crates/bqlite-storage/src/compaction.rs`:
  - Add `pub struct CoreBudgetPermitBatch<'a>` RAII guard holding `n`.
  - Add `pub fn acquire_n(&self, n: usize) -> CoreBudgetPermitBatch<'_>`.
  - Make FIFO real with an explicit ticket queue. State becomes
    `{available: usize, waiters: VecDeque<u64>, next_ticket: u64}`.
    Each waiter pushes its ticket on entry, then loops waiting for
    `waiters.front() == Some(&my_ticket) && available >= n`.
    Existing `acquire()` is rewired to call `acquire_n(1)` so
    compaction permits also respect the FIFO queue (otherwise a
    busy compaction stream could starve a queued query).
  - Tests:
    - `acquire_n(2)` while two permits are free succeeds immediately.
    - `acquire_n(3)` blocks while one permit is taken; releasing the
      one wakes it; assertion via thread-handle join.
    - Two concurrent `acquire_n(2)` callers on a 2-permit budget
      serialize (FIFO).
    - The drop releases all `n` permits at once.

CI gate: `scripts/local-ci.sh`. Subagent review of staged diff.
Merge to main. Reconciliation against
`compaction-concurrency.md` happens in CP4 alongside the rest of the
doc work.

### CP2 — Scheduler types + queue mechanics (engine, additive)

- New module tree in `crates/bqlite-engine/src/scheduler/`:
  - `mod.rs` — public re-exports + module-level docs that point at
    `engine/morsel-scheduler.md`.
  - `morsel.rs` — `Morsel`, `WindowSegments`, `MorselGenerator` with
    a `degenerate(...)` constructor that emits exactly one
    "whole-query" morsel for the v1 path. The metadata-only
    contract (§3.2) is preserved by accepting an
    `Arc<[ShardSnapshot]>` of pre-pruned segment lists.
  - `policy.rs` — `MorselSizePolicy` + `MorselSizeState` with
    `current_target_rows: AtomicU64`. The halving control loop is
    the §3.4 spec; sticky halving is enforced by storing a "step
    count" alongside the target so `halve` always moves down.
  - `queue.rs` — `MorselQueue` wrapping `crossbeam::queue::ArrayQueue`
    with the §4.1 push/pop + condvar wake protocol.
  - `accumulator.rs` — `AccumulatorHandle { inner:
    Mutex<Option<Box<dyn Accumulator>>>, outstanding_morsels:
    AtomicU64, total_emitted: OnceLock<u64> }`. Provides
    `pop_finalized()` for the coordinator merge step.
  - `worker.rs` — `WorkerContext`, `WorkerMorselGuard` with the
    §4.3 RAII drop hook. `take_morsel` is the public pull entry.
  - `coordinator.rs` — `QueryCoordinator` owning every
    per-shard `Arc<AccumulatorHandle>`, the `MorselQueue`, and the
    cross-shard merge driver.
  - `engine_pool.rs` — `MorselScheduler` carrying the Rayon thread
    pool, the active-queries list, and the `Arc<CoreBudget>`. The
    public surface is `submit(query_fn) -> ExecutionResult`, where
    the `query_fn` is the operator-tree driver (today: the closure
    that runs `drive_to_completion`).
  - Cargo.toml additions: `crossbeam = "0.8"`, `rayon = "1.10"` in
    workspace, `bqlite-engine` depends on both.
- Tests (per design §10.1 unit-test list, scoped to the scaffolding):
  - Morsel boundary correctness for the degenerate generator: one
    morsel out, `is_shard_final = true`, then `None`.
  - `MorselQueue` push/pop round-trip; `pop` returns `None` when
    drained; `push` returns `Err(Full)` at capacity.
  - `MorselSizeState::halve()` is sticky: full → half → quarter,
    never grows back; bottoms out at `low_target_rows`.
  - `AccumulatorHandle::shard_done()` triggers exactly once when
    `outstanding_morsels == 0` and `total_emitted` is set, in either
    write order.
  - `WorkerMorselGuard` drop decrements `outstanding_morsels`.
  - `MorselScheduler::submit` runs a closure on the worker pool
    and forwards its `Result`. Two concurrent submissions serialize
    on the `CoreBudget` (when `query_threads == num_cores` and the
    pool is saturated by the first).

### CP3 — Engine integration (replaces drive_to_completion)

- `crates/bqlite-engine/src/lib.rs` exports the scheduler module
  publicly under `pub mod scheduler`.
- `Engine` gains a `Arc<MorselScheduler>` field. `Engine::new()`
  builds one with `query_threads = available_parallelism()`. A
  fresh override path `Engine::with_scheduler(...)` is added for tests.
- `Engine::query_with_options` now:
  1. Parses + plans + binds (unchanged).
  2. For DDL / DELETE / INSERT — runs unchanged on the calling
     thread (§5.4 bypass).
  3. For SELECT-shaped queries: builds a `QueryCoordinator` with one
     degenerate morsel covering the entire query and submits it to
     `MorselScheduler::submit`. The submission acquires
     `CoreBudget::acquire_n(query_threads)` permits, dispatches the
     morsel to the Rayon thread pool via
     `ThreadPool::scope(|s| s.spawn(|_| ...))` (which gives the
     closure non-`'static` borrow of `&mut Database` through scoped
     lifetimes and joins on scope exit), runs the existing operator
     tree inside that worker (with the current `QueryContext` and
     `catch_unwind` boundary), and writes the resulting
     `Result<Vec<RecordBatch>>` into a `Mutex<Option<...>>` slot
     captured by the closure. No channel is needed — the scope's
     join already orders the read-after-write.
- The result-collection contract is identical to the previous
  drive_to_completion path (warnings stitched in on success and
  failure paths). Property test asserts the SELECT path produces
  byte-identical output to the pre-migration code path on a small
  fixture.
- Cancellation: the worker's `catch_unwind` runs around the
  drive_to_completion call (matches §9.2). The engine still sees
  `BqliteError::OperatorPanic` on worker panics and `BqliteError::
  Cancelled` on cancellation tokens.
- Tests:
  - All existing `query.rs` tests pass with the new path.
  - Two concurrent queries from two threads run sequentially under
    the `CoreBudget` semaphore (assertion: total wall time ≥ sum of
    individual wall times).
  - A panicking operator (synthetic) surfaces as
    `BqliteError::OperatorPanic` exactly as before.
  - A cancelled query returns `BqliteError::Cancelled`.

### CP4 — Doc reconciliation

- Per design doc §11.1 reconciliation block:
  - `docs/design/execution-model.md` §9.1 / §9.4 / §9.5 / §11.1 —
    correct the work-stealing, per-shard-queue, and thread-local
    accumulator phrasing.
  - `docs/design/execution-model.md` §14 ordering — note the
    rationalization is owned by TASK-524 (do not move §14.2/§14.3
    here; just leave a short forward-reference if the wording is
    actively misleading).
  - `docs/design/storage/compaction-concurrency.md` §4.4 / §12 —
    update the TASK-438 caveat to reference TASK-523.
  - `docs/design/INDEX.md` — verify the morsel-scheduler entry is
    present (it already is from TASK-506).

## Risks / open questions

- Replacing the engine's single-threaded driver with a Rayon-thread
  driver puts the operator tree on a thread that is **not** the one
  that owns `&mut Database`. We side-step this with
  `rayon::ThreadPool::scope`, which lets the spawned closure borrow
  `&mut Database` through a scoped lifetime, and which blocks the
  calling thread until the closure has joined. `Database: Send`
  because every owned field is `Send` (`PathBuf`, `File`, `Mutex`,
  `Arc`); a `static_assertions::assert_impl_all!` line in the
  scheduler module pins this so a future addition of a non-`Send`
  field surfaces as a compile error.
- `query_threads = available_parallelism()` may oversubscribe small
  hosts; the configurable knob is `EngineConfig::query_threads`,
  which exists today as the design contract. If unset we default
  to 4 per the design doc when `available_parallelism()` is `Err`.
- `crossbeam` and `rayon` are new deps; both are widely used (the
  storage crate already pulls in `crossbeam` indirectly via tests)
  and are explicitly required by the design doc.
- Per the design doc, follow-on `[IMPL]` tasks (TASK-524, TASK-525,
  TASK-526) and the per-operator morsel-awareness work depend on
  this scaffold. The deferral of operator-side morsel awareness is
  documented in CP3's commit message and the module-level doc.
