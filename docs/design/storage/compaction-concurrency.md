# Compaction Concurrency Protocol

**Wave**: 4
**Task**: TASK-403
**Status**: draft
**Depends on**: none (design doc; TASK-408 implements)
**Depended on by**: TASK-404 (tombstone semantics), TASK-408 (compaction executor), TASK-434 (tombstone-aware scan), TASK-435 (tombstone reclamation), TASK-438 (engine bind step)

---

## 1. Scope

This document freezes the compaction concurrency protocol for bqlite's
size-tiered compaction engine: how compaction jobs are scheduled, how
they coexist with concurrent queries without read-path locks, how
manifest publication and old-segment reclamation work, and how failures
are recovered.

It does **not** cover:

- The compaction merge algorithm itself (k-way merge, re-encoding) --
  that is TASK-408 implementation scope, guided by
  [storage-format.md](../storage-format.md) SS7.1--SS7.3.
- Tombstone semantics beyond the compaction interaction points --
  TASK-404 owns the full delete design.
- Backpressure on ingest -- explicitly deferred to Wave 5 (SS5).
- Segment format v2 / encoding selection -- TASK-401 / TASK-402.

### 1.1 Relationship to existing design docs

This document refines and supersedes the concurrency-related sketches in
[storage-format.md](../storage-format.md):

| storage-format.md section | This document | Change |
|---|---|---|
| SS7.1 scheduler sketch | SS3 (scheduler model) | Replaces "background process" with dedicated thread pool + synchronous API |
| SS7.4 atomicity (4-step) | SS6 (5-step manifest publication) | Adds in-memory `Arc` swap as explicit step; deferred deletion via SS7 |
| SS7.6 query snapshots | SS7 (old-segment reclamation) | Replaces "refcount wait" with periodic 10s sweep on `Arc::strong_count` |
| SS7.7 subcompaction cooperation | SS4 (core-budget semaphore) | Adds semaphore-gated pause at row-group boundaries |
| SS14.2 queries + compaction | SS4, SS7 | Forward-references to this document for the full cooperative-gating model |

Where this document and `storage-format.md` conflict, this document
wins. `storage-format.md` is updated in the same checkpoint to add
forward-references.

---

## 2. Unit of Work

Each compaction job covers exactly one `(window, shard)` pair. Jobs are
independent and embarrassingly parallel across different `(window,
shard)` pairs.

Subcompaction (entity-range slicing per
[storage-format.md](../storage-format.md) SS7.7) is a within-job
parallelism mechanism -- the job still publishes all N subcompaction
outputs atomically as a single manifest update.

**Why:** Matches the existing sharding model. Keeps manifest-update
contention to one table at a time. Any larger unit (multi-shard,
multi-window) would lose the embarrassing parallelism.

---

## 3. Scheduler Model

v1 ships both a background thread pool and a synchronous trigger API.

### 3.1 Background Thread Pool

A `CompactionScheduler` owns N dedicated worker threads, distinct from
the query worker pool. Default `N = max(1, num_cores / 4)`.

The scheduler dequeues jobs from a priority queue keyed by:

1. Highest L0 segment count first.
2. Ties broken by total L0 size (largest first).

### 3.2 Trigger Thresholds

Both thresholds are engine configuration with these defaults:

| Threshold | Default | Condition |
|---|---|---|
| L0 segment count | 4 | Eligible when count > 4 |
| L0 total size | 256 MB | Eligible when total size > 256 MB |

Any `(window, shard)` that satisfies **either** condition becomes
eligible and is enqueued. An already-enqueued `(window, shard)` is not
re-enqueued; re-evaluation happens on job completion.

**Why:** Matches the defaults already sketched in `storage-format.md`
SS7.1. Configurability matters because deployments with very different
ingest volumes (1 MB/day vs 1 TB/day) need different thresholds.

### 3.3 Synchronous API

`Database::compact_now(table: &str)` runs on the caller's thread and
blocks until every eligible `(window, shard)` for the named table has
been compacted. It ignores the active-count gating from SS4 -- the
caller is explicitly opting in.

Primary consumers: tests, CLI, operator scripts.

**Semaphore bypass:** `compact_now` does **not** acquire from the
core-budget semaphore (SS4). It runs on the caller's thread and counts
against whatever permits the caller already holds (typically zero for a
CLI invocation). This means:

- If invoked from within a query, it will not be throttled by that
  query's permits.
- If invoked during heavy background compaction, it races the background
  workers for I/O but not for permits.

**Why:** The user asked for it explicitly; throttling a synchronous API
against the same gate that throttles background work defeats its
purpose. Tests and CLI operators rely on `compact_now` completing
deterministically.

---

## 4. Concurrency Gate: Core-Budget Semaphore

The concurrency policy is **semaphore-gated with cooperative pause at
row-group boundaries**.

### 4.1 Protocol

- The engine owns a `core_budget: Semaphore` initialized with
  `num_cores` permits at startup.
- **Queries** acquire permits according to their parallelism -- a query
  running with worker count `w` acquires `w` permits on start and
  releases them on finalization.
- **Compaction workers** acquire **one permit** before each row-group of
  work and release it at the row-group boundary. The acquire/release
  pair is cheap (one atomic per row-group, not per row).
- When queries hold all permits, compaction workers block in `acquire()`
  -- they **pause** mid-job at the current row-group boundary. When a
  query releases, the first waiting compaction worker resumes.

Net effective compaction concurrency at any instant:

```
max(0, num_cores - active_query_permits)
```

Clamped by the scheduler pool size from SS3.1.

### 4.2 Mid-Job Pause, Not Abort

In-flight compaction jobs keep their temp outputs and internal state.
Pausing at a row-group boundary is free -- the row-group is already a
natural checkpoint in the merge pipeline. No subcompaction output is
discarded; no partial output is committed. When permits return, the
worker resumes the same job at the same row-group boundary where it
paused.

### 4.3 Scheduler Pool Cap

The pool size from SS3.1 is a hard ceiling on concurrent compaction
workers regardless of available permits. A 64-core machine with 16
compaction threads never runs more than 16 compactions simultaneously
even if no queries are active.

### 4.4 TASK-408 Integration

The scheduler exposes `CompactionScheduler::acquire_core_budget(&semaphore) -> Permit`
as a sub-call in the worker's row-group loop. Query execution (TASK-523)
acquires its permits on query start via `CoreBudget::acquire_n(query_threads)`
on the same semaphore — see `engine/morsel-scheduler.md` §7.1 for the
atomic batch-acquisition contract that resolves the partial-acquisition
deadlock between concurrent queries on a saturated worker pool.

**Why this shape:** A plain active-count check either over- or
under-provisions compaction. Semaphore-based gating naturally interleaves
compaction and query work at the granularity of one core x one row-group,
without explicit signaling and without per-row overhead. Row-group
boundaries are already checkpoint points in the merge pipeline, so
"pause" costs nothing to implement.

---

## 5. Backpressure

**v1 ships no backpressure.** If ingest outpaces compaction, L0 segments
accumulate and scans slow down. v1 does not throttle ingest, does not
spawn synchronous in-line compactions, and does not refuse writes.

**Why:** We have no bench evidence yet for what the safe L0 ceiling is,
and a wrong default for hard/soft thresholds causes more operator
surprise than slow scans. Wave 4 benches (TASK-441) will measure the
scan-degradation curve; the right backpressure policy is a Wave 5
decision informed by that data.

**Observability requirement:** The scheduler exposes a metric
`compaction_backlog_l0_segments` per `(window, shard)` so operators can
monitor the backlog themselves. Surfacing the metric is TASK-408 scope.

**Documented limitation:** "Running without backpressure means that
sustained ingest rates exceeding compaction throughput will cause L0
segment accumulation, increasing scan latency proportionally to the
number of uncompacted segments per `(window, shard)`."

---

## 6. Manifest Publication: 5-Step Atomic-Rename Protocol

Compaction publishes results via the following protocol:

```
1. Write all new segment temp files; fsync each.
2. Write manifest.json.tmp; fsync.
3. rename(manifest.json.tmp, manifest.json).   // atomic on POSIX
4. Atomically swap the in-memory Arc<Manifest>.
5. Schedule deferred deletion of old segment files (via SS7).
```

No directory fsync after the rename. POSIX `rename(2)` atomicity covers
us -- the metadata update is durable before rename returns. Targets
POSIX; Windows (`ReplaceFile`) parity is a future concern per
`storage-format.md` SS7.4.

### 6.1 Manifest Lock Scope

Compaction holds the per-table manifest lock only for steps 2--4
(manifest write through in-memory swap). Steps 1 (segment writes) and 5
(deferred deletion) are lock-free.

Lock hold time is milliseconds, not job duration, so concurrent ingest
to the same table is unblocked for the vast majority of the compaction.

### 6.2 Manifest Version Monotonicity

`Manifest.version: u64` is serialized by the per-table lock. Compaction
computes `new_version = old_version + 1` while holding the lock; no CAS
retry path needed.

CAS-based lock-free publication is a Wave 5+ candidate if lock
contention becomes a measured problem.

---

## 7. Old-Segment Reclamation

Query snapshot isolation uses plain `Arc<Manifest>` refcounting
([storage-format.md](../storage-format.md) SS14.2):

- Queries take `Arc::clone(&current_manifest)` at start; the refcount
  drops when the query finalizes.
- The `CompactionScheduler` retains a reference to every superseded
  manifest version in a `retired_versions: Vec<Arc<Manifest>>` list.
- A sweep runs every **10 seconds** (configurable) and, for each
  retired manifest, checks `Arc::strong_count` -- if it equals 1 (only
  the scheduler's own reference remains), the manifest's orphaned
  segment files are deleted and the entry is removed from
  `retired_versions`.

### 7.1 Why Periodic Over Event-Driven

Event-driven reclamation (sweep on every `Arc::drop`) requires a custom
`Arc` wrapper with a drop hook and atomic coordination. Periodic sweep
is simpler, has predictable worst-case latency (10s), and adds
negligible overhead. Reclamation latency is not user-visible.

### 7.2 Long-Running-Query Policy

A query that holds an `Arc<Manifest>` for hours defers reclamation of
its old segments for that long. v1 takes no action -- no warnings, no
timeouts, no forced invalidation.

**Documented limitation:** "Running a long query during active
compaction can temporarily double the disk footprint for the affected
`(window, shard)` pairs. Reclamation resumes once the query completes."

**Why no timeout:** Forcing a snapshot invalidation mid-query is a
correctness surprise -- the query suddenly sees
`ExecutionError::SnapshotInvalidated` from an operator that had been
happily running for 45 minutes. That is worse than deferred reclamation.
Real-world bqlite query lifetimes are minutes at most; disk bloat
concerns are a Wave 5+ operational topic.

---

## 8. Failure Recovery

All failure modes are recoverable via a single startup sweep plus a
retry-after-cooldown rule. No separate "pending-deletion sidecar file"
or "partial-output checkpoint" needed.

**Invariant:** The manifest is the source of truth. Anything not
referenced is either a temp file (delete) or an orphan (delete).
Anything referenced is assumed valid until proven otherwise (checksum on
first read).

### 8.1 Crash Before Manifest Rename (SS6 Step 3)

Temp segment files + `manifest.json.tmp` exist; `manifest.json`
unchanged. Startup sweep deletes every `.tmp` file silently. The
compaction is retried on the next trigger evaluation. No recovery of
the aborted compaction -- it just reruns.

### 8.2 Crash After Manifest Rename, Before Old-Segment Deletion (SS6 Step 5)

New manifest is live; old segment files are physically present but
unreferenced. Startup sweep compares on-disk files in each `(window,
shard)` directory against the manifest's active segment list and deletes
the remainder.

### 8.3 Mid-Subcompaction Failure

One of N subcompactions errors. The whole job aborts: any subcompaction
temp outputs already written are deleted; the input segments are
untouched (never deleted until after publish); an error is logged; the
`(window, shard)` is marked for retry after a **60-second cooldown** to
avoid busy-looping on a persistently failing job.

This matches the all-or-nothing publish guarantee from SS2 (implemented via SS6).

### 8.4 Corrupt Input Segment

Checksum failure on read is a loud error -- the segment is already
referenced by the manifest, so the corruption predates compaction. The
job aborts; the corrupt segment remains referenced by the manifest.

No `.corrupt` rename, no silent removal, no "best effort continue with
remaining inputs."

**Why:** Silent removal is data loss we cannot distinguish from a
checksum-implementation bug. Aborting and surfacing the error preserves
the operator's ability to diagnose (real disk corruption? a bqlite
encoding bug?) and act (restore from backup, re-ingest, file a bug).
Auto-recovery for corruption is Wave 5+ if it ever lands.

### 8.5 Startup Orphan-Cleanup Policy

**Conservative.** The sweep deletes only:

- Files matching `*.tmp` in any `(window, shard)` directory the manifest
  knows about.
- Unreferenced `segment_*.seg` files in `(window, shard)` directories
  the manifest knows about.

Directories the manifest does not know about are **not** touched -- they
may belong to another tool, a renamed table, or a debug dump. Aggressive
cleanup (delete-anything-unreferenced) risks removing user-placed files;
the conservative rule has negligible disk-footprint cost.

---

## 9. Tombstone Interaction

### 9.1 Tombstone Snapshot Ordering

Compaction snapshots the `tombstones.json` file at job start and uses
that snapshot for filtering throughout the job. Deletes issued
mid-compaction write a new tombstone file; that new file applies to
subsequent reads (they snapshot it at query start) but does not affect
the in-flight compaction's output.

### 9.2 Tombstone Reclamation Ordering

**Manifest-first, then tombstone.** After SS6 step 4 (in-memory `Arc`
swap):

```
6. Write new tombstones.json.tmp with reclaimed tombstones removed; fsync.
7. rename(tombstones.json.tmp, tombstones.json).
8. Schedule old-segment deletion (SS7).
```

If the process crashes between step 4 and step 7, the new manifest is
live but the tombstone file still lists entries that the new segment has
already physically removed. This is **harmless**: the stale tombstones
filter rows that are not in the new segment anyway, so they are no-ops.
The next compaction of the same `(window, shard)` -- or a manual
operator action -- will rewrite the tombstone file.

**Why manifest-first:** The reverse order (tombstone-first) is a
**correctness bug**. If we cleared tombstones before publishing the new
manifest, and crashed in between, the old segments would still be
referenced by the current manifest but their tombstones would be gone --
readers would see previously-deleted rows. Never reorder these steps.

---

## 10. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Unit of work | One `(window, shard)` per job | Embarrassingly parallel; minimal manifest contention |
| Scheduler model | Dedicated thread pool (`num_cores / 4`) + `compact_now` sync API | Decouples compaction from query latency; deterministic testing |
| Trigger thresholds | L0 count > 4 OR L0 size > 256 MB, configurable | Matches existing defaults; configurability for varied deployments |
| Concurrency gate | Core-budget semaphore, pause at row-group boundaries | Natural yield; no per-row overhead; zero-cost pause |
| `compact_now` gating | Bypasses semaphore | Synchronous API must complete deterministically |
| Backpressure | None in v1 | No bench data yet; wrong defaults worse than slow scans |
| Manifest publication | 5-step atomic-rename (write, fsync, rename, Arc swap, deferred delete) | POSIX atomicity; millisecond lock hold time |
| Manifest versioning | Lock-serialized increment, no CAS | Simple; CAS is Wave 5+ if contention measured |
| Old-segment reclamation | `Arc::strong_count` check every 10s | Simpler than event-driven; predictable latency |
| Long-query policy | No timeout, no forced invalidation | Correctness > disk footprint; real queries are short |
| Failure recovery | Startup orphan sweep + 60s retry cooldown | Single mechanism covers all crash points |
| Corrupt-segment policy | Abort job, do not auto-quarantine | Silent removal is indistinguishable from bugs |
| Tombstone snapshot | At job start; manifest-first reclamation ordering | Crash-safe ordering; stale tombstones are harmless no-ops |
| Orphan cleanup | Conservative: only `.tmp` and unreferenced `.seg` in known dirs | Avoids deleting user-placed files |

---

## 11. Follow-On Implications

These cross-references document which downstream tasks consume decisions
from this protocol:

- **TASK-408 (compaction executor + scheduler)** -- implements SS2--SS9.
  Owns: priority queue, background thread pool, `compact_now` entry
  point, `core_budget: Semaphore`, `acquire_core_budget()` sub-call,
  manifest publish protocol, per-table lock acquisition, retired-manifest
  list + 10s reclamation sweep, startup orphan sweep, mid-job abort + 60s
  retry cooldown, `compaction_backlog_l0_segments` metric.
- **TASK-438 (engine bind step)** -- engine-level wiring of cohort /
  subquery materialization at query start. The query-side
  `core_budget` permit acquisition itself is TASK-523's responsibility
  via `CoreBudget::acquire_n(query_threads)` (`engine/morsel-scheduler.md`
  §7.1).
- **TASK-523 (morsel scheduler + partial-aggregate handoff)** --
  extends `core_budget` with `acquire_n` (atomic batch acquisition,
  head-of-line FIFO) and acquires `query_threads` permits on query
  start through the engine-side `MorselScheduler::submit` path (SS4
  symmetry).
- **TASK-404 (tombstone semantics)** -- SS9 pins tombstone
  snapshot-at-job-start and manifest-first reclamation ordering.
  TASK-404 must reflect these in `docs/design/storage/deletes.md`.
- **TASK-434 (tombstone-aware scan + merge)** -- query-side tombstone
  snapshot is symmetric: query snapshots both `Arc<Manifest>` and the
  tombstone file at start.
- **TASK-435 (tombstone reclamation during compaction)** -- implements
  SS9.2 steps 6--7; depends on the manifest publication protocol from
  SS6.
- **Engine configuration surface** -- SS3.1 (pool size N), SS3.2 (L0
  count/size thresholds), SS7 (sweep period), SS8.3 (retry cooldown)
  become user-visible config. Exact config module placement is TASK-408
  scope.

---

## 12. Implementation status (TASK-408)

TASK-408 lands the executor, scheduler, `compact_now`, the
`core_budget` semaphore surface, the backlog metric, the manifest
publication primitive (one closure per job, see SS6), the 60-second
mid-job retry cooldown, and reuses the startup orphan sweep already
shipped by TASK-239 (`reconcile_segments`) without modification.

Manifest publication uses a new
`Manifest::replace_segments(table, window, shard, removed_ids,
new_meta)` mutation invoked through the existing
`Database::update_manifest` closure path (promoted to `pub(crate)` in
the same checkpoint). One closure removes every input and adds the
output, so the on-disk manifest never observes a half-state -- the
SS6 all-or-nothing publish guarantee is preserved.

**Deferred items, kept honest with this doc:**

- SS6 step 4 in-memory `Arc<Manifest>` swap and SS7 10-second
  `Arc::strong_count` reclamation sweep wait on the `Arc<Manifest>`
  migration that TASK-438 (engine bind step) is the natural place to
  land. Until then, `Database` owns its `Manifest` by value; there are
  no concurrent readers holding a stale snapshot, so superseded
  segment files are deleted immediately after the manifest update
  succeeds. The `retired_versions` hook is intentionally absent -- it
  would have nothing to track.
- SS9 tombstone snapshot at job start and the manifest-first
  reclamation ordering are implemented by TASK-435 inside
  `compact_one` (`crates/bqlite-storage/src/compaction.rs`): the job
  reads `tombstones.json` once, wraps every input scan in
  `CompactionTombstoneScan` to drop row/batch/entity/time-range
  matches during the merge, and rewrites the tombstone file under
  the per-shard mutex after publish. The zero-surviving-rows path
  publishes a "remove-only" manifest update via
  `Database::remove_segments_atomic` and still runs reclamation to
  clear the entries that caused the full drop. Query-time
  scan-wrapping is TASK-434.
- SS4 query-side permit acquisition lands with TASK-523. TASK-408
  shipped the `CoreBudget` type and the per-job acquire/release
  inside the worker; TASK-523 (`engine/morsel-scheduler.md` §7.1)
  extends `CoreBudget` with `acquire_n` (atomic batch, head-of-line
  FIFO) so the engine acquires `query_threads` permits on query start
  through `MorselScheduler::submit`. Sharing one `CoreBudget` instance
  between the engine and the storage compaction scheduler — so a
  running query actually pre-empts new compaction permit acquisitions
  — is forward-compatible follow-on work; the v1 engine constructs
  its own `CoreBudget`, and the public `acquire_n` contract is
  identical regardless of who owns the underlying instance.

### 12.1 Streaming row-group writer (Wave 5 follow-on)

The TASK-408 executor materialises the merged stream into one
in-memory `RecordBatch` via `arrow::compute::concat_batches` before
encoding. With the v1 256 MiB L0 size threshold and `pool_size =
num_cores / 4`, peak per-worker memory is approximately
`2 x L0_total_bytes` (input + concat double-buffer); on a 16-core
machine that is roughly 512 MiB x 4 workers, ~2 GiB. This is the
deliberate v1 cap.

A streaming row-group writer that encodes one row group at a time and
flushes to disk before pulling the next merged batch is filed as a
Wave 5 follow-on. TASK-441 (advanced-analytics benchmarks) will
measure the actual peak and decide whether the streaming rewrite is
worth shipping; the per-row-group `core_budget` acquire/release model
already in place for the streaming variant is preserved by the v1
executor for forward compatibility.
