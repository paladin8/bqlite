# Reliability

Operational requirements that all components must satisfy.

## I/O Timeouts

All I/O operations must have timeouts. No unbounded waits on disk, network, or any external resource.

## Memory Budget

All operators must respect the configured memory budget. When an operator's intermediate state exceeds its allocation, it must spill to disk rather than grow unbounded. The default memory budget is 4 GB.

## Typed, Recoverable Errors

All errors must be typed and recoverable. Panics are not acceptable error handling. Callers must be able to distinguish between different failure modes and take appropriate action.

## Entity Event Limit

An entity event limit must prevent pathological entities from consuming unbounded resources. Entities with event counts exceeding the configured limit are skipped and flagged in the result metadata — not silently dropped, not allowed to blow up memory.

## Crash Safety

The WAL (write-ahead log) ensures crash safety for writes. Any write that has been acknowledged is durable. Recovery on startup replays the WAL to restore consistent state.

## Concurrent Access Prevention

A lock file prevents concurrent access to the same database directory. Attempting to open a database that is already locked returns a clear error rather than corrupting data.

## Non-blocking Compaction

Compaction must not block reads. Background compaction merges segments while active queries continue to read from the pre-compaction state. Segment switching is atomic.
