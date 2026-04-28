# TASK-513 Plan — Sort Spill Runs + On-Disk Merge

**Status:** draft
**Owner:** agent-1
**Spec:** `docs/design/engine/spill.md` §5–§11, `docs/design/operators/sort-distinct.md` §3, §8
**Depends on:** TASK-502 (merged), TASK-510 (merged)

## Goal

Implement v1 sort spill: when the in-memory sort buffer outgrows the per-query
`MemoryBudget`, `SortOperator` spills sorted runs to disk as Arrow IPC streams
and performs a final k-way merge across all runs (spilled + in-memory residual).
Cancellation/teardown must match `engine/cancellation.md` §5; outputs must match
the in-memory path bit-for-bit on every fixture.

## Architecture

| Concern | Where it lives |
|---|---|
| `TempSpillFile` RAII guard | `bqlite-core::spill` (operators consume it) |
| `SpillFs` (root path + per-query subdir + seq counters) | `bqlite-core::spill` |
| Spill-root sweep on database open | `bqlite-storage::Database::{open, create}` |
| `QueryContext::open_spill(purpose)` | `bqlite-engine::context` |
| `SortOperator` budget+spill participation | `bqlite-operators::sort` |
| K-way merge of sorted Arrow IPC runs | `bqlite-operators::sort::merge` (private) |

`SpillFs` lives in `bqlite-core` (not `bqlite-engine`) so operators can own
`Arc<SpillFs>` directly. `bqlite-engine::QueryContext` exposes
`open_spill(purpose)` as a convenience wrapper that resolves a per-query
subdirectory via the engine-side `Arc<SpillFs>`.

`Database` owns the `Arc<SpillFs>` because the spill-root lifecycle is gated by
the database lock (per spill.md §5.3). `Engine::query` reads
`db.spill_fs()` and threads it into the `QueryContext`.

## Reconciliation with Existing Code

- `Engine` is stateless; the spill design's `Engine::open` maps to `Database::open`
  in this codebase. The startup `rm_rf(<spill_root>)` happens there, scoped to
  the configured spill root.
- `Engine::close` maps to `Database::drop`. Best-effort `rm_rf` on drop.
- `EngineConfig::spill_root: Option<PathBuf>` is **landed in this task** per
  spill.md §5.2 / §12. Validation rules from §12.2 (must be absolute; must not
  equal `<db_root>`; must not be a child of `<db_root>` unless exactly
  `<db_root>/spill/`) are enforced when constructing `Database` from a config.
  An optional `Database::with_spill_root(path)` builder lets tests override
  without an `Engine` round-trip.
- The process-global spill-root registry from §5.3 lives as a private
  `Mutex<HashSet<PathBuf>>` static inside `bqlite-core::spill`. `SpillFs::new`
  inserts its canonicalised root and panics-via-typed-error if it is already
  present; `Drop` removes it. v1's default is auto-scoped by the database
  lock, so the registry guards only the override path, but it must be
  present from CP1 because the override surface ships in CP1.
- TASK-512 (ingest spill) is in flight on a peer agent's branch and will
  also need `TempSpillFile` / `SpillFs`. This task lands them first; if that
  branch lands first, CP1 rebases and reuses what's there.

## Checkpoint Plan

### CP1 — Spill-fs scaffolding (NEW FILES + thin Database/Engine wiring)

Mostly additive, low conflict risk.

**Files:**
- `crates/bqlite-core/src/spill.rs` — new module
- `crates/bqlite-core/src/lib.rs` — re-export `TempSpillFile`, `SpillFs`,
  `SpillQueryId`
- `crates/bqlite-storage/src/database.rs` — add `spill_fs: Arc<SpillFs>` field;
  resolve spill root from config + db root, run `rm_rf` + `mkdir` in `open` /
  `create`; expose `spill_fs()` accessor; `Drop` runs best-effort `rm_rf` and
  removes the registry entry
- `crates/bqlite-engine/src/context.rs` — store `Arc<SpillFs>` and a
  per-query `SpillQueryId`; `QueryContext::open_spill(purpose)` returns a
  `TempSpillFile`; on `Drop` of the *last* clone, `cleanup_query(qid)` runs
  (post-operator-drop because the operator-tree drop happens before the
  context drops in `Engine::query`)
- `crates/bqlite-engine/src/query.rs` — pull `db.spill_fs()` into
  `QueryContext::new`
- Reconcile `docs/design/engine/cancellation.md` §5.2's stale
  `MemoryTracker::open_spill(path_hint)` reference to point at
  `QueryContext::open_spill` / `engine/spill.md` §8.1.
- Tests in `spill.rs`, `database.rs`, `context.rs`.

**Public surface (bqlite-core):**

```rust
pub struct TempSpillFile { /* path, file, bytes_written */ }
impl TempSpillFile {
    pub fn path(&self) -> &Path;
    pub fn file_mut(&mut self) -> &mut File;
    pub fn bytes_written(&self) -> u64;
    pub fn record_bytes_written(&mut self, n: u64);
    /// Convert into the underlying `File` for write paths that own the
    /// guard separately. Drop of the returned `TempSpillFile` still
    /// deletes `path`.
}
impl Drop for TempSpillFile { /* remove_file best-effort */ }

pub struct SpillFs { /* root, query counter, per-(qid,purpose) seq, registry */ }
impl SpillFs {
    /// Validate `root` per spill.md §12.2 vs. `db_root`, register
    /// `canonicalize(root)` in the process-global registry, run rm_rf
    /// + mkdir(0o700) on POSIX. Returns Err if the registry already
    /// holds `root` or validation fails.
    pub fn open(root: PathBuf, db_root: &Path) -> Result<Arc<Self>>;
    pub fn root(&self) -> &Path;
    pub fn new_query_id(&self) -> SpillQueryId;
    pub fn open_spill(&self, qid: SpillQueryId, purpose: &str)
        -> Result<TempSpillFile>;
    pub fn cleanup_query(&self, qid: SpillQueryId);
}
impl Drop for SpillFs { /* best-effort rm_rf(root) + registry.remove */ }
```

**SpillQueryId:** wraps `u64`; `Display` renders zero-padded 9-digit
decimal (`000000042`) per spill.md §7. Wrapper makes a future swap to
UUIDv7 (TASK-541) source-compatible at the call sites.

**Path scheme:** `<root>/<qid>/<purpose>-<seq>.spill` (zero-padded 6-digit seq).
Validate `purpose` against `[a-z0-9-]+` to keep filenames safe.

**Lazy subdir:** `open_spill` creates `<root>/<qid>/` on first call.

**EngineConfig surface change:**

```rust
pub struct EngineConfig {
    pub query_memory_budget_bytes: u64,
    pub compaction_memory_budget_bytes: u64,
    pub ingest_memory_budget_bytes: u64,
    /// Override for the spill root. `None` → `<db_root>/spill/`.
    pub spill_root: Option<PathBuf>,
}
```

`Database::create_with_config(path, config)` and an analogous `open_with_config`
take the override. The existing `Database::create(path)` /
`Database::open(path)` call them with `EngineConfig::default()` (no override).

**Tests:**
- Path scheme matches `<root>/<qid>/<purpose>-NNNNNN.spill`.
- `TempSpillFile::Drop` removes the file.
- Sequence counter is monotone within `(qid, purpose)`.
- Per-purpose counters are independent.
- Per-query counters are independent.
- `SpillFs::open` reclaims a populated spill root.
- `SpillFs::open` rejects relative override paths.
- `SpillFs::open` rejects `<db_root>` itself as the spill root.
- `SpillFs::open` rejects a non-`spill/` child of `<db_root>`.
- `SpillFs::open` rejects a duplicate canonicalised root via the registry.
- `SpillFs::Drop` removes the registry entry so a re-open succeeds.
- `cleanup_query` only deletes its own subdir.
- `Database::open` reclaims a stale spill root.
- `Database::create` initializes an empty spill root.
- `QueryContext::open_spill` lazily creates the per-query subdir.
- `QueryContext::Drop` (last clone) runs `cleanup_query(qid)`.
- Belt-and-braces: simulate a leaked guard (via `mem::forget`); the
  per-query subdir still gets swept on context drop.

**Not in this CP:** SortOperator changes, `MemoryBudget` reservations,
`Utf8View` round-trip tests (those land with the actual writer in CP2b).

**Verification:** `scripts/local-ci.sh` clean; subagent code review approved.

### CP2a — SortOperator plumbing (BUDGET + SPILLFS THREAD-THROUGH, NO BEHAVIOUR CHANGE)

Pure plumbing; mergeable independently. No spill is performed.

**Files:**
- `crates/bqlite-operators/src/sort.rs` — extend `SortOperator::new` to
  accept `Arc<dyn MemoryBudget>` + `Option<Arc<SpillFs>>` +
  `Option<SpillQueryId>` (the option pair is `Some` for live queries,
  `None` for tests that want pure-in-memory). Reserve through the budget
  per accumulated batch; on `Err`, propagate (no spill yet — handler is a
  no-op stub registered at construction). Existing behaviour is preserved
  byte-for-byte for the unbounded path.
- `crates/bqlite-engine/src/bind.rs` — pass `ctx.memory()`, `ctx.spill_fs()`,
  `ctx.query_id()` into `SortOperator::new`.
- Existing tests updated to pass `UnboundedMemory` + `None` for the spill
  pair; behaviour identical to today.
- New tests:
  - `sort_reservations_charged_to_budget` — sort N batches, assert
    `peak_bytes() ≈ Σ batch.get_array_memory_size()`.
  - `sort_overflow_returns_typed_budget_error` — tiny budget, no spill
    handler frees anything; the operator surfaces
    `BqliteError::MemoryBudgetExceeded`.

**Verification:** local-ci clean; subagent review.

### CP2b — Sort spill writer + k-way merge (THE WORK)

**Files:**
- `crates/bqlite-operators/src/sort.rs` — register a real spill handler in
  the constructor; phase 2 takes the merge path when runs exist
- `crates/bqlite-operators/src/sort/merge.rs` — k-way merge module (private)
- `crates/bqlite-operators/Cargo.toml` — add `arrow-ipc`, `arrow-row`,
  `arrow-select` deps if not already in tree
- Reconcile `docs/design/operators/sort-distinct.md` §3.6 + §8 forward
  references in the same checkpoint

**Locking discipline (handler reentrancy):**

Spillable state lives in `Arc<Mutex<SortSpillState>>`. The operator holds
one Arc; the spill handler holds a clone. The operator **never** holds the
state lock across `MemoryBudget::try_reserve` — it computes the request,
releases the lock, calls `try_reserve`, and re-acquires the lock to push
the resulting `(batch, reservation)`. The handler runs synchronously from
inside `try_reserve`'s slow path on the operator's thread; it acquires the
lock, performs the spill, releases the lock, and returns. The handler
never re-enters `try_reserve`.

**Algorithm (per spill.md §6.1, §10):**

Phase 1 — Accumulation:
- Each child batch arrives → reserve `batch.get_array_memory_size() as u64`
  bytes via `try_reserve` *without holding the state lock*.
  - On `Ok(reservation)` → push `(batch, reservation)` into the buffer.
  - On `Err(MemoryBudgetExceeded)` → propagate (the budget already
    consulted the handler under its single-retry rule per
    memory-budget.md §4.1).
- The `max_rows` hard cap is preserved as an absolute upper bound across
  *all* observed input rows (in-memory + spilled).

Spill handler (`SortSpillHandler`):
- On `on_pressure(_bytes_needed)`:
  1. Lock the state. If `buffer` is empty → return 0.
  2. Concat the buffer, evaluate sort keys, `lexsort_to_indices` + `take`
     to produce a sorted batch.
  3. Open a `TempSpillFile` via `SpillFs::open_spill(qid, "sort-run")`.
  4. Wrap the file in `arrow_ipc::writer::StreamWriter` (default config —
     dictionary tracking on, no compression). Write the sorted batch in
     `DEFAULT_OUTPUT_BATCH_SIZE`-row chunks; poll `is_cancelled()`
     between chunks (matches spill.md §11).
  5. Compute `freed = Σ reservation.bytes()` from the dropped buffer
     entries. Drop the in-memory batch + reservations (release flows
     back to the tracker). Push the `TempSpillFile` guard onto
     `state.runs`.
  6. Return `freed` (reservation bytes, **not** on-disk bytes per
     spill.md §10.2 step 6).

Phase 2 — Merge:
- When the child returns `None`:
  - If `state.runs.is_empty()` → existing single-buffer path
    (`sort_and_split`); zero-spill case is unchanged behaviorally.
  - Else → k-way merge over spilled runs + (optionally) the sorted
    in-memory residual exposed as a virtual "run-N" through the same
    `RunCursor` interface. No temp file is written for the residual.

K-way merge:
- Build a `RowConverter` once from `Vec<SortField>` matching the
  operator's `(SortDirection, nulls)` rules.
- For each run, lazily pull one batch at a time. Convert each pulled
  batch's key columns to `OwnedRow`s and cache the row representation
  on the cursor (one allocation per pulled batch, none per row).
- `BinaryHeap<Reverse<RunCursor>>` (min-heap) picks the next-smallest
  row across runs. Tie-break on `run_idx` so equal-key rows globally
  keep input order — runs are assigned increasing indices in spill
  order, with the in-memory residual as the highest index. This
  preserves stability without a per-row sequence number.
- Inside one output batch's worth of work, accumulate
  `Vec<(run_idx, row_idx)>` of length up to
  `DEFAULT_OUTPUT_BATCH_SIZE`. Materialize the output via
  `arrow::compute::interleave_record_batch` on the live array views
  from each run's current batch — one kernel call per output batch, no
  per-row builder allocation, native `Utf8View` / dictionary
  preservation.
- When a run's current batch is exhausted, pull the next; if its
  reader returns `None`, drop the cursor (which drops the
  `TempSpillFile` guard → file deleted promptly).
- Cancellation: top-of-`next_batch` poll suffices — the per-output-batch
  boundary is the natural yield point per spill.md §11.

**Stability proof sketch:** within a single run, `lexsort_to_indices` is
stable. Across runs, the heap's `run_idx` tie-break forces equal-key rows
to emit in run order; runs are spilled in input arrival order, with the
residual indexed last, so equal-key rows globally appear in input order.
The test asserts this with an all-equal sort key and a synthetic
monotone tag column.

**Tests:**
- `sort_with_unbounded_budget_matches_in_memory_path` — `proptest`:
  arbitrary batch sequences sorted with a small budget produce the same
  output as the in-memory path.
- `sort_spills_when_budget_exceeded` — small budget vs. larger input
  with a real `MemoryTracker`; assert ≥ 1 spill file existed during
  execution (sample mid-merge by hooking `SpillFs`'s sequence) and the
  spill subdir is gone after teardown.
- `sort_multi_run_merge_preserves_total_order` — 3+ runs across mixed
  types.
- `sort_spill_with_nulls_preserves_null_ordering` — ASC + DESC.
- `sort_spill_is_stable_across_runs` — equal sort keys, synthetic
  monotone tag, spilled+merged output preserves input order.
- `sort_spill_cancelled_during_write_returns_cancelled` — token
  cancelled between batch writes inside the handler; per-query subdir
  is empty after.
- `sort_spill_handler_returns_reservation_bytes_not_disk_bytes` — unit:
  buffer entries with known reservation totals; assert `on_pressure`
  return value matches reservation total even when on-disk byte count
  differs.
- `sort_spill_preserves_utf8view` — `Utf8View` column round-trips
  through spill+merge as `Utf8View` (not `Utf8`).
- `sort_spill_drop_without_close_cleans_up` — drop the operator
  mid-execution; spill files removed.
- `sort_spill_residual_only` — child exhausts exactly when the last
  reservation triggers a spill; the merge sees only spilled runs.

**Verification:** local-ci clean; subagent review; reconcile against
`docs/design/operators/sort-distinct.md` §3.6 + §8 forward references and
`docs/design/engine/spill.md` §6.1 + §10.

## Reconciliation Checklist

- `docs/design/operators/sort-distinct.md` §3.6 — update the "operator does
  not register with a `MemoryBudget` tracker in Wave 3" sentence: TASK-513
  ships the registration and the in-memory + spill paths share the same
  algorithm.
- `docs/design/operators/sort-distinct.md` §8 — strike the "Sort spill: no
  per-physical-descriptor opt-in field" forward reference (now landed).
- `docs/design/engine/cancellation.md` §5.2 — replace the stale
  `MemoryTracker::open_spill(path_hint)` sketch with a pointer to
  `QueryContext::open_spill` and `engine/spill.md` §8.1 (CP1).

## Risks / Open Questions

- **Arrow row comparators:** confirm `arrow::row::RowConverter` exposes a
  stable API in our pinned arrow version. If not, fall back to per-key cell
  comparison using `arrow::compute::kernels::cmp::*` — this is a private
  module concern and does not change the operator surface.
- **Memory accounting precision:** `RecordBatch::get_array_memory_size`
  underestimates dictionary overhead for nested types. Sort inputs are
  primitives + Utf8View in our pipeline today; the underestimate is bounded.
  If a future input shape blows the bound, the spill handler frees the
  *reservation* bytes (which the tracker has, by definition, accepted), so
  the budget invariant holds even if on-disk bytes differ from in-memory
  bytes.
- **Coordination with TASK-512:** if agent-3's TASK-512 lands the
  `TempSpillFile` / `SpillFs` types first, this plan rebases CP1 to
  reuse them rather than redefining.
