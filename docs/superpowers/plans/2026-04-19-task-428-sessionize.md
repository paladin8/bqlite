# TASK-428 — SessionizeOperator Implementation Plan

**Task**: TASK-428 (`[HARD][IMPL]`) — entity-streaming `SessionizeOperator` implementing
gap/end-event session boundaries, `session_id` / `session_duration` emission,
sub-batch continuation, and demand-driven forwarding.

**Scope doc**: `docs/design/operators/sessionize.md` (authoritative).
**Task note**: `tasks/notes/TASK-405.md` (authoritative semantics).

## Summary

Produce `crates/bqlite-operators/src/sessionize.rs` implementing `EntityOperator`.
All plumbing (physical descriptor, demand capabilities, logical lowering) is
already in place (TASK-424 lowering, TASK-427 DemandCapabilities). Fused
aggregates are deferred to Wave 5 per sessionize.md §10. The per-query warning
channel does not yet exist; per §11.4 option (2), we track cap exhaustion on
per-entity state (mirrors the matcher's `cap_exceeded`/`dropped_count` pattern).

## Deliverables

- `crates/bqlite-operators/src/sessionize.rs` — `SessionizeOperator`, `SessionizeState`,
  `SessionBuffer`, `EndEventCodeSet`, `SessionizeInputMap`.
- `pub use` in `crates/bqlite-operators/src/lib.rs`.
- Unit tests (inline `#[cfg(test)] mod tests`).
- Property tests exercising §14.3 invariants.
- Benchmark (throughput) in `crates/bqlite-operators/benches/`.
- Doc reconciliation: any behavior divergence back into `sessionize.md`.

## Checkpoints

### CP1 — SessionizeOperator implementation + unit tests
- Struct + construction from `SessionizePhysical` (gap, end_events, forwarded
  columns, output schema).
- `EntityOperator` impl:
  - `create_state`, `output_schema`, `required_columns`, `supported_demands`.
  - `process_sub_batch`: per-row gap check (§5.1), end-event membership (§5.2),
    gap-vs-end precedence (§5.3), buffer-only-demanded-columns (§9).
  - `finish_entity`: flush final open session (§5.5), concatenate into a single
    multi-row `RecordBatch`.
- Dictionary-aware end-event matching for `DictionaryArray<Int32, Utf8View>`
  and fall-through for `StringViewArray`.
- Per-entity event cap: partial flush + skip-to-entity-boundary with
  `cap_exceeded` flag on state (diagnostic plumbing deferred per §11.4 — flag
  exposed via `SessionizeState::cap_exceeded()` for adapter use when it lands).
- Unit tests covering §14.1 edge-case matrix (empty entity, single event,
  exclusive boundary, gap+1ns, end-event first/last, gap+end precedence,
  entity-boundary flush, multi-sub-batch session, per-entity cap).
- `pub use sessionize::SessionizeOperator` in `lib.rs`.
- Passes `scripts/local-ci.sh`. Subagent code review. FF merge.

### CP2 — Property tests + benchmark + doc reconciliation
- `proptest` invariants (session coverage, duration correctness, gap invariant,
  end-event invariant, entity isolation) — wire up via dev-dependency.
- Throughput benchmark in `crates/bqlite-operators/benches/sessionize.rs`
  (gap-only; gap+end-event; dict-coded event_type).
- Reconcile `sessionize.md` §11.4 to note the v1 warning plumbing (in-state flag
  surfaced via public accessor on `SessionizeState`, adapter wiring deferred).
- Passes CI. Subagent review. FF merge.

## Key design points from doc

- Gap is strict `>`. `delta == gap_ns` is same session.
- End event belongs to the session it closes.
- Gap closes first when both gap-exceeded and end-event on same row.
- `session_id: Int64 NOT NULL`, `session_duration: Int64 NOT NULL`. Per-entity
  monotonic starting at 1. Reset at entity boundary.
- `session_duration == max_ts - min_ts`. Single-event session = 0.
- Buffer holds full `RecordBatch` slices (demanded columns only, sliced to the
  session's row range), plus the session bookkeeping.
- When flushing, build an annotated batch whose first N columns are the
  buffered input columns (plus any logically-forwarded, not-physically-buffered
  columns), and last two columns are `session_id` / `session_duration`
  constant-value arrays. For v1 we materialize the union of input columns plus
  `session_id` / `session_duration`; non-demanded columns are padded via null
  arrays to honor the schema (downstream operators will not read them by §9).
- Accumulate completed-session batches across sub-batches in `SessionizeState`;
  concatenate at `finish_entity`.

## Risk and deferrals

- **Schema honor vs demand**: The logical output schema advertises all input
  columns, but only demanded columns are physically buffered. Downstream
  operators promise not to read un-demanded columns (§9). We still must return
  a `RecordBatch` whose columns match the advertised `output_schema` so the
  adapter's schema contract holds. Non-demanded columns are emitted as null
  arrays typed per `output_schema` — allocation is bounded by emit-time row
  count, not buffered, so the memory benefit (§9.3) is preserved.
- **Fused aggregate**: always `None` in v1. The trait default
  `finish_entity_into` is sufficient.

## Skipping plan review

The plan is scoped to a single file with a fully-specified design doc and task
note. Review would be ceremonial. Proceeding to CP1.
