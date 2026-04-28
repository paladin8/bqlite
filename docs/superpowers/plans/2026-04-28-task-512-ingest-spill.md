# TASK-512 — Ingest partitioner external spill

> Plan author: agent-3 (2026-04-28). Replaces the Wave 2 "error loudly
> when the buffer exceeds budget" path in `bqlite-storage::ingest::partitioner`
> with external spill + k-way merge per
> [`docs/design/engine/spill.md`](../../design/engine/spill.md) § 6.2.

## Goal

Land external spill so that an ingest whose total event footprint
exceeds `Partitioner::budget_bytes` succeeds by writing the largest
in-memory `(window, shard)` buckets to per-ingest temp files,
preserves `(entity_id, ts)` ordering and the partitioner's `batch_id`
stamping at drain, and cleans up every temp file on every exit path
(success / mid-ingest error / process crash).

`MemoryBudget` participation is **out of scope**: per spill.md § 4.2
the partitioner is its own allocator and does not call
`register_spill_handler`. Sort spill (TASK-513) and per-`Engine` spill-
root configuration / process-startup sweep (the `Engine`-level open
hook in spill.md § 5.4) are also out of scope — TASK-513 will reuse
the `<db_root>/spill/` reclamation hook this task lands at the
`Database` boundary.

## Reference: design doc

`docs/design/engine/spill.md` is canonical. § 4.2 (why ingest spills),
§ 5 (spill root + reclamation), § 6.2 (per-bucket file layout), § 7
(path scheme), § 8 (RAII cleanup), § 9 (crash recovery), § 11
(cancellation) drive the implementation.

## Format deviation (postcard, not Arrow IPC)

§ 6.2 calls for "the same Arrow IPC stream format as sort runs (§ 6.1)
using the ingest event schema". The partitioner does not own a
`TableSchema` and its input is `Vec<Event>`, not `RecordBatch` — using
Arrow IPC would require either threading the table schema down or
synthesising a wrapper schema with a postcard-encoded property blob
column anyway. I use **postcard** directly: length-delimited
`postcard::to_stdvec(&Event)` records written through a `BufWriter`,
read back through a `BufReader` + `postcard::from_bytes`. This keeps
every Event-shape semantic intact (properties bag, `EntityId`
String/Int variants), reuses the postcard dependency
`bqlite-storage` already pulls in for the segment writer, and avoids a
round-trip through Arrow that would add code without changing the
on-disk size class.

The doc reconciliation lands in CP2 alongside the partitioner change
so the spec and the code agree at every commit.

## Checkpoints

### CP1 — `<db_root>/spill/` lifecycle on the `Database` handle

Database open / create now own the spill-root directory's lifecycle.

- `Database::create_with_shards` and `Database::open` both call a new
  `reclaim_spill_root(&root)` helper after the database flock is
  held, which `rm_rf`s `<root>/spill/` (best-effort; logs on a non-
  `NotFound` error) and recreates it `mkdir_p` with mode `0o700` on
  POSIX.
- `Database::spill_root(&self) -> &Path` accessor returns the
  cached `<root>/spill/`.
- The flock is the exclusivity guarantee per spill.md § 5.3 — no
  process-global registry yet (deferred to TASK-513 / TASK-525 once a
  user-overridable spill_root surface lands).

Tests (storage-side):
- `Database::create` populates `<root>/spill/` empty.
- `Database::open` reclaims a pre-existing non-empty `<root>/spill/`.
- `Database::open` after a clean `Database::close`-equivalent leaves
  `<root>/spill/` empty.
- The reclamation tolerates a missing `<root>/spill/` (NotFound is
  not an error).

Out of scope: cross-process registry, per-`Engine` `with_spill_root`
override, the validation rules in § 12.2 (those land in TASK-525 with
`EngineConfig.spill_root`). All of those are layered on top without
changing this task's hook.

### CP2 — Partitioner external spill + k-way merge

The partitioner gains an opt-in spill directory plus the spill loop
and the merge pass. The existing constructor (`Partitioner::new(...)`)
keeps the Wave 2 fail-fast contract — every existing caller compiles
unchanged.

**Module shape (`bqlite-storage::ingest::partitioner` only):**

- New private `SpillRunFile` RAII guard owning a `PathBuf` and an
  `enum SpillHandle { Writing(BufWriter<File>), Closed }` state. The
  invariant: as long as a `SpillRunFile` exists, its `Drop`
  unconditionally `remove_file`s the path (best-effort; logged on
  non-`NotFound` failure). The write→read transition is explicit:
  the writer is flushed and dropped via `finish_writing(&mut self)`
  (which transitions to `Closed`), and a separate read pass opens a
  fresh `File` for read by re-opening the path. We do not use
  `File::try_clone()` — a cloned descriptor would not see writer-
  buffered bytes, and explicit flush+close is clearer than a shared-
  fd dance. The `PathBuf` is the single source of truth for both
  Drop and the read open.
- New private `SpillStream` wrapping a `BufReader<File>` (opened
  fresh from the `SpillRunFile.path` after `finish_writing`) and
  yielding `Result<Option<Event>>` via `postcard::take_from_bytes`
  over a reusable read buffer.
- `Partitioner` gains:
  - `spill_dir: Option<PathBuf>` field. None → fail-fast (existing
    behaviour). Some → spill enabled.
  - `spilled_runs: BTreeMap<BucketKey, Vec<SpillRunFile>>` — runs are
    produced in insertion-order, which is also `(entity, ts)`-sort
    order *within* each run.
  - `spill_seq: u64` — monotone counter for filenames inside the
    per-ingest dir.
- New constructor `Partitioner::with_spill_dir(shard_count,
  window_days, batch_id, budget_bytes, spill_dir)` returning the same
  `Self`.

**Spill loop (`push_event`):**

When `projected = buffered_bytes + size > budget_bytes` and
`spill_dir.is_some()`:

1. Pick the bucket with the largest in-memory footprint. (Estimate: a
   running per-bucket byte count maintained alongside `buckets`. This
   is updated on every `push_event` add and on every spill subtraction.)
2. Sort the bucket in place by `(entity_id, ts)` (stable, identical
   to `drain_sorted`).
3. Stream the events to a fresh
   `<spill_dir>/ingest-part-w<window>-s<shard>-<seq>.spill`
   via length-delimited postcard. `<seq>` is zero-padded to six
   decimal digits per spill.md § 7 so lexicographic order matches
   creation order. `<window>` is the literal `window_id` integer
   and `<shard>` is the literal `shard_id` integer with no padding
   (matches spill.md § 7 example
   `ingest-part-w19782-s0007-000003.spill` — note that example
   uses zero-padded shard, but the doc body says "encoding the
   bucket key in the filename for debuggability" without
   prescribing padding; this plan zero-pads the shard to four
   digits to match the example).
4. Drop the in-memory `Vec<Event>` and subtract its byte estimate
   from `buffered_bytes`. Append the `SpillRunFile` to
   `spilled_runs[(window, shard)]`.
5. Re-evaluate `projected`. If it still overshoots, repeat from step 1
   on the next-largest bucket. If `buckets` becomes empty and the
   single event still does not fit, return
   `BqliteError::Execution("partitioner: oversized event ...")` per
   spill.md § 6.2's "single event bigger than the entire budget"
   case.
6. Append the event to its bucket and update `buffered_bytes`.

Cancellation: the spill writer polls `is_cancelled()` between
chunks of 65,536 events per spill.md § 11. The partitioner does not
yet hold a `CancellationToken` (engine-side ingest cancellation is
TASK-525); the field is wired through `with_spill_dir` as
`Option<CancellationToken>` defaulting to `None` so TASK-525 has a
single seam to flip on. **Decision-guard:** if pulling the token in
adds non-trivial wiring this checkpoint defers it and leaves a
TODO with a TASK-525 cross-reference; the spill loop chunks at
65,536 events regardless so the latency target is met without the
poll.

**Drain pass (`drain_sorted`):**

```text
for each (window, shard) in BTreeMap-order:
  let runs = (in-memory residual, sorted) :: (each spilled run as a SpillStream)
  k-way merge by (entity_id, ts) using BinaryHeap<RunHead>
  yield (BucketKey, Vec<Event>)  // collected per bucket because the
                                  // signature returns full Vec<Event>;
                                  // see "drain shape" below
```

**Drain shape note.** The current return type is
`impl Iterator<Item = (BucketKey, Vec<Event>)>`. The merge accumulates
a full `Vec<Event>` per bucket before yielding — same as today. A
streaming variant (yield event-by-event) is a future-wave concern;
it would change the writer's API too (TASK-214 expects a sorted
`Vec<Event>` per bucket). The merge writes the result into a fresh
`Vec` so the spilled bytes never aggregate in memory beyond one
bucket at a time, which is the property the writer needs.

**Doc reconciliation (CP2):**

- `docs/design/engine/spill.md` § 6.2 — replace "Arrow IPC stream
  format as sort runs … using the ingest event schema" with a
  postcard-stream description; cross-reference the `SpillRunFile`
  guard. Note the deviation rationale (no `TableSchema` available;
  Event-level serialisation is sufficient because the spill is
  private).
- `docs/design/engine/spill.md` § 6.2 — already says "spill files
  for ingest live under the *query* spill subdirectory" — confirm
  the per-ingest UUID subdirectory model. The `query_id` becomes
  `ingest-<uuid>` per the same paragraph.
- `crates/bqlite-storage/src/ingest/partitioner.rs` module docstring
  — drop the "Wave 2 error-loudly" language; point at spill.md.

Tests (CP2):
- A small budget that admits ≤ 1 event in memory still drains every
  pushed event in `(entity, ts)` order across many buckets and one
  bucket.
- Spilled file naming follows
  `ingest-part-w<window>-s<shard:04>-<seq:06>.spill` and lex-sorts
  in creation order (asserted by listing the per-ingest dir).
- Single event larger than budget surfaces
  `BqliteError::Execution("oversized event …")` and leaves the
  partitioner state untouched.
- Drain order and per-bucket `(entity, ts)` ordering are identical to
  the no-spill path on the same input.
- Stable-sort tie-break preserved (events with identical
  `(entity, ts)` retain insertion order across spill+merge).
- Dropping the partitioner before drain removes every spill file
  (RAII).
- proptest: for a random `Vec<Event>` with random budgets ≥
  `min_event_size`, drain yields the same `(BucketKey, Vec<Event>)`
  sequence as a no-spill partitioner with `budget_bytes = usize::MAX`.

### CP3 — Engine wiring + Integration

`crates/bqlite-engine/src/ingest.rs` `execute_insert_from` and
`execute_insert_values` resolve the per-ingest spill subdirectory
and pass it to the partitioner.

- Per-ingest id: a UUIDv7 rendered as a directory name. Created
  lazily on the first spill (the partitioner mkdir_ps the dir before
  the first write, or the engine creates it up-front so the engine
  also owns the cleanup on success). Easier-to-reason path:
  **engine creates the dir up-front under
  `<db.spill_root()>/ingest-<uuid>/`**, passes `Some(dir)` to the
  partitioner, and `rm_rf`s the dir after the writer drains
  successfully (belt-and-braces sweep per spill.md § 8.3 — the
  partitioner's `SpillRunFile` drops also delete files, so the sweep
  only catches a stuck handle).
- The sweep runs on every exit path via a small
  `IngestScratchDir` RAII guard owned in `execute_insert_from` /
  `execute_insert_values`.
- The default ingest budget stays at 256 MiB. For tests we use a
  much smaller budget to force spill paths.

Integration tests (CP3):
- INSERT VALUES with a 16 KiB budget on >100 K-row VALUES list
  (synthetic) commits the segments and produces correct `seq_id` /
  `batch_id` ranges.
- INSERT FROM CSV / JSONL with a budget that forces multiple bucket
  spills produces the same segment row count as a wide-budget run.
- Crash-equivalent test: after a successful ingest, the per-ingest
  scratch dir does not exist on disk.
- `Database::open` after manually-injected stale spill files clears
  them (already covered by CP1 — re-asserted as an integration
  smoke).

**Doc reconciliation (CP3):**

- `docs/design/storage-format.md` § 13 — add the one sentence noted
  in spill.md § 13 about ingest partitioner spill landing under the
  per-ingest subdirectory.
- `crates/bqlite-engine/src/ingest.rs` module docstring — replace
  the `DEFAULT_INGEST_BUDGET_BYTES` "Wave 2 fixed budget; per-query
  memory management lands in TASK-501" comment with one that points
  at spill.md and TASK-512.

## Risks

- **Partitioner ↔ writer coupling.** The writer's
  `write_partitioner` consumes `partitioner.drain_sorted()` and
  expects buckets in `(window, shard)` order with sorted events.
  Spill must preserve both. The merge pass loops the existing
  BTreeMap order, so the bucket-level order is unchanged. The
  per-bucket `(entity, ts)` order is the merge invariant.
- **Event size estimate drift.** `estimated_event_size` is a
  monotonic best-effort estimate. The spill loop subtracts
  estimates, not exact bytes — so `buffered_bytes` may diverge from
  reality after many spills. As long as the divergence is bounded
  (it is: each bucket's contribution drops to 0 atomically when
  spilled), the budget invariant holds. Tests stress this with
  variable-size events.
- **Postcard format stability.** Spill files are private to one
  process and crash-recovered by reclamation; format stability
  across versions is not required. We pin the encoding via
  `postcard::to_stdvec` / `from_bytes` and document the contract.
- **TempSpillFile naming inside the partitioner.** Naming the
  guard `SpillRunFile` (private to the partitioner module) avoids
  colliding with the future `TempSpillFile` engine surface
  (cancellation.md § 5.2). When TASK-513 lands, the engine
  surface and the partitioner-private guard can be unified or kept
  separate without changing this task's contract.
- **Doc deviation (postcard vs. Arrow IPC).** Documented inline in
  spill.md § 6.2 and the partitioner docstring. Open question
  whether sort spill should also adopt postcard; out of scope for
  this task — TASK-513 makes its own decision.

## Out-of-scope checklist

- `register_spill_handler` integration with `MemoryTracker`
  (partitioner is its own allocator; spill.md § 4.2).
- `EngineConfig.spill_root` override / per-`Engine` validation
  (TASK-525 / future).
- Process-global spill-root registry (spill.md § 5.3 — TASK-525 /
  future).
- Compression of spill payloads (spill.md § 14).
- `--explain-perf` spill metrics (TASK-524).
- Sort spill (TASK-513).
