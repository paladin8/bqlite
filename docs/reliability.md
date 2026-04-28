# Reliability

Operational requirements that all components must satisfy.

## Memory Budget

All operators must respect the configured memory budget. When an operator's intermediate state exceeds its allocation, it must either spill to disk or fail with a typed `MemoryBudgetExceeded` error per the per-operator policy in `docs/design/engine/memory-budget.md` § 7. The default query memory budget is 3 GiB; the engine-wide aggregate (query + compaction + ingest sub-budgets) defaults to ~4 GiB. The three sub-budgets are independent allocators, not a single shared pool — see `docs/design/engine/memory-budget.md` § 2 for the canonical numbers and the rationale.

## Typed, Recoverable Errors

All errors must be typed and recoverable. Panics are not acceptable error handling. Callers must be able to distinguish between different failure modes and take appropriate action.

## Crash Safety

Ingestion is batch-only — each ingest call produces complete segment files directly on disk. A write is durable once the segment file is fsynced and the manifest is atomically updated. There is no WAL or memtable. If the process crashes mid-ingest, partially written segment files (identified by `.tmp` suffix) are cleaned up on next startup. No data that was acknowledged can be lost; no recovery replay is needed.

## Concurrent Write Access Prevention

A lock file prevents concurrent write access to the same database from multiple processes. Attempting to open a database for writing when another process holds the lock returns a clear error. Multiple read-only opens are allowed. Within a single process, multiple queries and ingest operations can run concurrently — concurrency is managed by the execution engine's thread pool and manifest locking, not the lock file.

## Non-blocking Compaction

Compaction must not block reads. Background compaction merges segments while active queries continue to read from the pre-compaction state. Segment switching is atomic.

## Versioning

Version everything (data format, schema format, API format, table schema, etc) that can change in the future and cause backwards-compatibility issues. During most of development, the version will be 1.

## Migration Notes

### Wave 1 → Wave 2: Bootstrap events table retirement (TASK-240)

Wave 1 databases carry a manifest with `bootstrap_events_table: true` on the auto-seeded `events` table entry. Wave 2's `Database::create` no longer seeds this table — callers use explicit `CREATE TABLE` DDL instead. The `bootstrap_events_table` field remains in the manifest schema for read-compatibility: a Wave 1 manifest opened by Wave 2 code works without modification. Wave 2 databases never set this flag to `true`.
