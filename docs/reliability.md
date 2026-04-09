# Reliability

Operational requirements that all components must satisfy.

## Memory Budget

All operators must respect the configured memory budget. When an operator's intermediate state exceeds its allocation, it must spill to disk rather than grow unbounded. The default memory budget is 4 GB.

## Typed, Recoverable Errors

All errors must be typed and recoverable. Panics are not acceptable error handling. Callers must be able to distinguish between different failure modes and take appropriate action.

## Crash Safety

Ingestion is batch-only — each ingest call produces complete segment files directly on disk. A write is durable once the segment file is fsynced and the manifest is atomically updated. There is no WAL or memtable. If the process crashes mid-ingest, partially written segment files (identified by `.tmp` suffix) are cleaned up on next startup. No data that was acknowledged can be lost; no recovery replay is needed.

## Concurrent Access Prevention

A lock file prevents concurrent access to the same database directory. Attempting to open a database that is already locked returns a clear error rather than corrupting data.

## Non-blocking Compaction

Compaction must not block reads. Background compaction merges segments while active queries continue to read from the pre-compaction state. Segment switching is atomic.

## Versioning

Version everything (data format, schema format, API format, table schema, etc) that can change in the future and cause backwards-compatibility issues. During most of development, the version will be 1.
