# TASK-403 — Compaction concurrency protocol

Human-assisted semantics decisions for `docs/design/storage/compaction-concurrency.md`. These decisions are authoritative and override conflicting guesses drawn from `docs/design/storage-format.md §7 / §12 / §14` or `TASKS.md`. Reconcile those docs in the same checkpoint as any code change that contradicts them. Downstream consumers include TASK-408 (compaction executor + scheduler) and TASK-404 / TASK-434 / TASK-435 (tombstone semantics).

## Already pinned by existing docs (not re-litigated here)

- Strategy: size-tiered within each `(window, shard)`. `storage-format.md §7.1`.
- Levels (L0 → L1 → …) and the merge-4-per-level invariant. `storage-format.md §7.2`.
- Subcompaction: entity-range slices for large merges, atomic multi-output publish, boundary picker snaps to entity transitions. `storage-format.md §7.7`.
- Query snapshot isolation via `Arc<Manifest>`. `storage-format.md §12.3 / §14.2`.
- Tombstones live in per-shard `tombstones.json` files, not the manifest. `storage-format.md §7.5 / §12.4`.
- Database lock file at `<db_root>/.lock`. `storage-format.md §14.1`.
- Manifest atomicity via write-temp → fsync → rename. `storage-format.md §7.4 / §12.3`.

## Decisions

### 1. Unit of work — one job = one `(window, shard)`

Each compaction job covers exactly one `(window, shard)` pair. Jobs are independent and embarrassingly parallel across different `(window, shard)` pairs. Subcompaction (entity-range slicing per `storage-format.md §7.7`) is a within-job parallelism mechanism — the job still publishes all N subcompaction outputs atomically as a single manifest update.

**Why:** matches the existing sharding model; keeps manifest-update contention to one table at a time; any larger unit (multi-shard, multi-window) would lose the embarrassing parallelism.

### 2. Scheduler model — dedicated thread pool + explicit synchronous trigger API

v1 ships both:

- **Background thread pool.** A `CompactionScheduler` owns N worker threads, distinct from the query worker pool. Default `N = max(1, num_cores / 4)`. The scheduler dequeues jobs from a priority queue keyed "highest L0 segment count first, ties broken by total L0 size."
- **Synchronous API.** `Database::compact_now(table: &str)` runs on the caller's thread and blocks until every eligible `(window, shard)` for the named table has been compacted. Ignores the active-count gating from §4 — the caller is explicitly opting in. Primary consumers: tests, CLI, operator scripts.

**Why:** dedicated pool decouples compaction latency from query concurrency; synchronous trigger gives tests and CLI a deterministic compaction step without poking at scheduler internals.

### 3. Trigger thresholds — L0 count > 4 OR L0 total size > 256 MB, configurable

Both thresholds are engine configuration with the defaults above. Any `(window, shard)` that satisfies either condition becomes eligible and is enqueued. An already-enqueued `(window, shard)` is not re-enqueued; re-evaluation happens on job completion.

**Why:** matches the defaults already sketched in `storage-format.md §7.1`. Configurability matters because deployments with very different ingest volumes (1 MB/day vs 1 TB/day) need different thresholds; hard-coding them forces workarounds. The defaults are a starting point validated by Wave 4 benches (TASK-441).

### 4. Concurrency gate — core-budget semaphore with mid-job pause

The concurrency policy is **semaphore-gated with cooperative pause at row-group boundaries** — replacing the `(a)/(b)/(c)` options originally offered for D1/D2.

**Protocol:**

- The engine owns a `core_budget: Semaphore` initialized with `num_cores` permits at startup.
- **Queries** acquire permits according to their parallelism — a query running with worker count `w` acquires `w` permits on start and releases them on finalization.
- **Compaction workers** acquire **one permit** before each row-group of work and release it at the row-group boundary. The acquire/release pair is cheap (one atomic per row-group, not per row).
- When queries hold all permits, compaction workers block in `acquire()` — they **pause** mid-job at the current row-group boundary. When a query releases, the first waiting compaction worker resumes. This is a natural yield model; no preemption primitive needed.
- Net effective compaction concurrency at any instant = `max(0, num_cores - active_query_permits)`, clamped by the scheduler pool size from §2.

**Mid-job pause, not abort.** In-flight compaction jobs keep their temp outputs and internal state; pausing at a row-group boundary is free (the row-group is already a natural checkpoint in the merge pipeline). No subcompaction output is discarded; no partial output is committed. When permits return, the worker resumes the same job on the same row-group boundary where it paused.

**Scheduler pool cap is the upper bound.** The pool size from §2 is a hard ceiling on concurrent compaction workers regardless of available permits — a 64-core machine with 16 compaction threads never runs more than 16 compactions simultaneously even if no queries are active.

**Why this shape:** a plain active-count check (the original D1 options) either over- or under-provisions compaction. Semaphore-based gating naturally interleaves compaction and query work at the granularity of one core × one row-group, without explicit signaling and without per-row overhead. Row-group boundaries are already checkpoint points in the merge pipeline, so "pause" costs nothing to implement.

**Implication for TASK-408:** the scheduler exposes `CompactionScheduler::acquire_core_budget(&semaphore) -> Permit` as a sub-call in the worker's row-group loop. Query execution (TASK-438 onwards) acquires its permits on query start via the same semaphore.

### 5. User-driven `compact_now` bypasses the semaphore

`Database::compact_now(table)` does **not** acquire from the core-budget semaphore. It runs on the caller's thread and counts against whatever permits the caller already holds (typically zero, if the caller is a CLI invocation). This means:

- If invoked from within a query, it will not be throttled by that query's permits.
- If invoked during heavy background compaction, it races the background workers for I/O but not for permits.

**Why:** the user asked for it explicitly; throttling a synchronous API against the same gate that throttles background work defeats its purpose. Tests and CLI operators rely on `compact_now` completing deterministically.

### 6. Backpressure — none in v1, documented limitation

If ingest outpaces compaction, L0 segments accumulate and scans slow down. v1 does not throttle ingest, does not spawn synchronous in-line compactions, and does not refuse writes.

**Why:** we have no bench evidence yet for what the safe L0 ceiling is, and a wrong default for hard/soft thresholds causes more operator surprise than slow scans. Wave 4 benches (TASK-441) will measure the scan-degradation curve; the right backpressure policy is a Wave 5 decision informed by that data.

**Observability requirement:** the scheduler exposes a metric `compaction_backlog_l0_segments` per `(window, shard)` so operators can monitor the backlog themselves. Surfacing the metric is TASK-408 scope.

### 7. Manifest publication — 5-step atomic-rename protocol (no directory fsync)

```
1. Write all new segment temp files; fsync each.
2. Write manifest.json.tmp; fsync.
3. rename(manifest.json.tmp, manifest.json).   // atomic on POSIX
4. Atomically swap the in-memory Arc<Manifest>.
5. Schedule deferred deletion of old segment files (via §8 reclamation sweep).
```

No directory fsync after the rename. POSIX `rename(2)` atomicity covers us — the metadata update is durable before rename returns. Targets POSIX; Windows (`ReplaceFile`) parity is a future concern per `storage-format.md §7.4`.

**Manifest lock scope:** compaction holds the per-table manifest lock only for steps 2–4 (manifest write through in-memory swap). Steps 1 (segment writes) and 5 (deferred deletion) are lock-free. Lock hold time is milliseconds, not job duration, so concurrent ingest to the same table is unblocked for the vast majority of the compaction.

**Manifest version monotonicity:** `Manifest.version: u64` is serialized by the per-table lock. Compaction computes `new_version = old_version + 1` while holding the lock; no CAS retry path needed. CAS-based lock-free publication is a Wave 5+ candidate if lock contention becomes a measured problem.

**Why:** matches the existing `storage-format.md §7.4` sketch. POSIX `rename(2)` durability without directory fsync is the common DBMS convention; the engineering cost of adding the extra fsync exceeds the marginal robustness benefit in v1.

### 8. Old-segment reclamation — Arc refcount + periodic 10s sweep

Query snapshot isolation uses plain `Arc<Manifest>` refcounting (`storage-format.md §14.2`):

- Queries take `Arc::clone(&current_manifest)` at start; the refcount drops when the query finalizes.
- The `CompactionScheduler` retains a reference to every superseded manifest version in a `retired_versions: Vec<Arc<Manifest>>` list.
- A sweep runs every **10 seconds** (configurable) and, for each retired manifest, checks `Arc::strong_count` — if it equals 1 (only the scheduler's own reference remains), the manifest's orphaned segment files are deleted and the entry is removed from `retired_versions`.

**Why periodic over event-driven:** event-driven reclamation (sweep on every `Arc::drop`) requires a custom `Arc` wrapper with a drop hook and atomic coordination. Periodic sweep is simpler, has predictable worst-case latency (10s), and adds negligible overhead. Reclamation latency isn't user-visible.

**Long-running-query policy:** a query that holds an `Arc<Manifest>` for hours defers reclamation of its old segments for that long. v1 takes no action — no warnings, no timeouts, no forced invalidation. Document as: "Running a long query during active compaction can temporarily double the disk footprint for the affected `(window, shard)` pairs. Reclamation resumes once the query completes."

**Why no timeout:** forcing a snapshot invalidation mid-query is a correctness surprise — the query suddenly sees `ExecutionError::SnapshotInvalidated` from an operator that had been happily running for 45 minutes. That's worse than deferred reclamation. Real-world bqlite query lifetimes are minutes at most; disk bloat concerns are a Wave 5+ operational topic.

### 9. Failure recovery — startup orphan sweep handles all crash points

**Crash before manifest rename (§7 step 3).** Temp segment files + `manifest.json.tmp` exist; `manifest.json` unchanged. Startup sweep deletes every `.tmp` file silently. The compaction is retried on the next trigger evaluation. No recovery of the aborted compaction — it just reruns.

**Crash after manifest rename, before old-segment deletion (§7 step 5).** New manifest is live; old segment files are physically present but unreferenced. Startup sweep compares on-disk files in each `(window, shard)` directory against the manifest's active segment list and deletes the remainder.

**Mid-subcompaction failure (one of N subcompactions errors).** The whole job aborts: any subcompaction temp outputs already written are deleted; the input segments are untouched (never deleted until after publish); an error is logged; the `(window, shard)` is marked for retry after a 60-second cooldown to avoid busy-looping on a persistently failing job. This matches §1's all-or-nothing publish guarantee.

**Corrupt input segment discovered during compaction.** Checksum failure on read is a loud error — the segment is already referenced by the manifest, so the corruption predates compaction. The job aborts; the corrupt segment remains referenced (compaction does not quarantine). Operator intervention is required to recover: restore from backup or issue an appropriate `DELETE`. v1 does not auto-quarantine — silently removing a segment from the manifest would silently lose committed data.

**Startup orphan-cleanup policy.** Conservative. The sweep deletes only:

- Files matching `*.tmp` in any `(window, shard)` directory the manifest knows about.
- Unreferenced `segment_*.seg` files in `(window, shard)` directories the manifest knows about.

Directories the manifest doesn't know about are **not** touched — they may belong to another tool, a renamed table, or a debug dump. Aggressive cleanup (delete-anything-unreferenced) risks removing user-placed files; the conservative rule has negligible disk-footprint cost.

**Why:** all four failure modes are recoverable via a single startup sweep + a single retry-after-cooldown rule. No separate "pending-deletion sidecar file" or "partial-output checkpoint" needed. The invariant is: "the manifest is the source of truth; anything not referenced is either a temp file (delete) or an orphan (delete). Anything referenced is assumed valid until proven otherwise (checksum on first read)."

### 10. Tombstone interaction — snapshot at job start; clear tombstone file after manifest swap

**Tombstone snapshot ordering.** Compaction snapshots the `tombstones.json` file at job start and uses that snapshot for filtering throughout the job. Deletes issued mid-compaction write a new tombstone file; that new file applies to subsequent reads (they snapshot it at query start) but does not affect the in-flight compaction's output.

**Tombstone reclamation ordering** (manifest-first, then tombstone):

```
After §7 step 4 (in-memory Arc swap):
  6. Write new tombstones.json.tmp with reclaimed tombstones removed; fsync.
  7. rename(tombstones.json.tmp, tombstones.json).
  8. Schedule old-segment deletion (§8).
```

If the process crashes between step 4 and step 7, the new manifest is live but the tombstone file still lists entries that the new segment has already physically removed. This is **harmless**: the stale tombstones filter rows that aren't in the new segment anyway, so they're no-ops. The next compaction of the same `(window, shard)` — or a manual operator action — will rewrite the tombstone file.

**Why manifest-first:** the reverse order (tombstone-first) is a correctness bug. If we cleared tombstones before publishing the new manifest, and crashed in between, the old segments would still be referenced by the current manifest but their tombstones would be gone — readers would see previously-deleted rows. Never reorder these steps.

### 11. Corrupted-segment policy — abort job, do not auto-quarantine

Restated from §9 for emphasis: when compaction discovers a corrupt input segment, the job aborts cleanly and the corrupt segment remains referenced by the manifest. No `.corrupt` rename, no silent removal, no "best effort continue with remaining inputs."

**Why:** silent removal is data loss we can't distinguish from a checksum-implementation bug. Aborting and surfacing the error preserves the operator's ability to diagnose (is this real disk corruption? a bqlite encoding bug? a cosmic ray?) and act (restore, re-ingest, file a bug). Auto-recovery for corruption is Wave 5+ if it ever lands.

## Follow-on implications to propagate

- **TASK-408 (compaction executor + scheduler)** — implements §1–§11. Owns:
  - Priority queue, background thread pool, `compact_now` synchronous entry point (§2).
  - The `core_budget: Semaphore` per §4; the `acquire_core_budget()` sub-call used between row-groups; the pause/resume protocol at row-group boundaries.
  - Manifest publish protocol from §7; per-table lock acquisition for steps 2–4.
  - Retired-manifest list + 10-second reclamation sweep from §8.
  - Startup orphan sweep from §9.
  - Mid-job abort + 60-second retry cooldown on subcompaction failures from §9.
  - `compaction_backlog_l0_segments` metric from §6.
- **TASK-438 (engine bind step)** — must acquire `core_budget` permits for query workers on query start and release on finalization (§4 symmetry).
- **TASK-404 (tombstone semantics)** — §10 pins tombstone snapshot-at-job-start and manifest-first reclamation ordering. TASK-404 must reflect these in `docs/design/storage/deletes.md` (specifically the "compaction-time reclamation" section referenced by TASKS.md TASK-404 description).
- **TASK-434 (tombstone-aware scan + merge)** — query-side tombstone snapshot is already symmetric: query snapshots both `Arc<Manifest>` and the tombstone file at start; compaction's post-swap tombstone rewrite is a separate file so in-flight queries see a consistent view.
- **TASK-435 (tombstone reclamation during compaction)** — implements §10 steps 6–7; depends on the manifest publication protocol from §7.
- **`docs/design/storage-format.md §7`** — reconcile with this note: §7.1 scheduler sketch becomes §2 here; §7.4 atomicity sketch becomes §7 here (explicit 5 steps); §7.6 query-snapshot sketch becomes §8 here (periodic 10s sweep, no timeout policy); §7.7 subcompaction gets a cross-reference to §4 for pause protocol.
- **`docs/design/storage-format.md §14.2–§14.3`** — update to reference `compaction-concurrency.md` for the full cooperative-gating model; the "cooperates with query load" prose becomes a forward-reference to §4.
- **Engine configuration surface** — §2 (pool size N), §3 (L0 count / size thresholds), §8 (sweep period), §9 (retry cooldown) become user-visible config. Exact config module placement is TASK-408 scope.
