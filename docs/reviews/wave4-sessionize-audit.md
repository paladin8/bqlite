# Wave 4 SESSIONIZE Semantic Audit

**Auditor**: TASK-444
**Date**: 2026-04-20
**Sources reviewed**:
- Design spec: `docs/design/operators/sessionize.md` (TASK-405)
- Query-language spec: `docs/design/query-language.md` §8, §30.2
- Type-system spec: `docs/design/type-system.md` §6.3
- Execution-model spec: `docs/design/execution-model.md` §2.1
- Operator implementation: `crates/bqlite-operators/src/sessionize.rs` (TASK-428)
- Planner / NFA compiler: `crates/bqlite-planner/src/compile.rs` (TASK-424)
- Integration tests: `tests/tests/wave4_advanced_analytics_sessionize.rs` (TASK-439)
- Benchmarks: `benches/wave4/sessionize.rs` (TASK-428)

**Methodology**: Walk the design doc section by section; for each promise, locate primary evidence in code + tests; classify each as ✅ Covered, ⚠️ Partial, or ❌ Missing. File follow-up items for partial/missing rows. Nothing is fixed here — all drift is rolled up into TASK-455.

---

## Promise-vs-Evidence Matrix

### §5 — Session Boundary Rules

| Promise | Evidence | Status |
|---------|----------|--------|
| §5.1 Gap is exclusive: new session iff `delta > gap_ns`; `delta == gap_ns` stays same session | `process_sub_batch` at `sessionize.rs:580`: `if delta > self.gap_ns`; unit tests `delta_equal_to_gap_stays_same_session` and `delta_one_ns_over_gap_starts_new_session`; property test `session_id_monotone_and_duration_matches_range` invariant (4) asserts `delta <= gap` for same-session pairs | ✅ |
| §5.1 `delta` computed as `ts - session_last_ts` | `ts.saturating_sub(state.session_last_ts)` at `sessionize.rs:579`. `saturating_sub` is used rather than wrapping subtraction; on well-formed entity-sorted input (guaranteed by execution-model.md §3.5) `ts >= session_last_ts` always holds, so saturation never fires | ✅ |
| §5.2 End event belongs to the session it closes, not the next session | `push_slice(state, batch, slice_start, row + 1)` includes the end event in the buffer before `flush_session` at `sessionize.rs:570–571, 597–599, 623–625`; unit tests `end_event_belongs_to_closed_session`, `end_event_as_last_event_closes_session_cleanly`; property test `end_event_is_last_in_its_session` | ✅ |
| §5.3 When gap and end-event coincide: gap closes prior session first, then new 1-event session closes immediately | Gap branch (`sessionize.rs:582–603`) closes the prior session, then checks whether the arriving event is an end-event and closes the fresh 1-event session; unit test `gap_and_end_event_on_same_row_gap_closes_first` | ✅ |
| §5.4 `end:` accepts a list (`end: (A, B, C)`); each listed type closes the session | `end_events: HashSet<String>` at `sessionize.rs:95`; unit test `multiple_end_events_in_list_all_close`; integration test `end_event_list_multiple_types_all_close_their_session` | ✅ |
| §5.5 Entity boundary: final open session flushed; `session_id` resets to 1 for next entity | `finish_entity` flushes when `session_start_ts != i64::MIN && !skipping` (`sessionize.rs:641`); `create_state` sets `current_session_id: 1` (`sessionize.rs:491`); unit test `entity_state_resets_to_session_one`; property test `entity_isolation_restarts_session_id`; integration test `entity_boundary_resets_session_id_to_one` | ✅ |

---

### §6 — Output Schema

| Promise | Evidence | Status |
|---------|----------|--------|
| §6.1 Output schema = input schema + `{session_id, session_duration}` | `output_to_buffer_slot` + `session_id_out_idx` + `session_duration_out_idx` assembled at construction (`sessionize.rs:263–285`); output batch constructed in `flush_session` column-by-column in output schema order | ✅ |
| §6.2 `session_id: Int64 NOT NULL`; starts at 1 per entity; monotonically increasing; resets at entity boundary | `ColumnDef::required("session_id", BqlType::Int)` in test fixtures; `Int64Array::from(vec![session_id; num_rows])` in `flush_session`; property test `session_id_monotone_and_duration_matches_range` verifies contiguous monotone sequence | ✅ |
| §6.2 `COUNT_DISTINCT(session_id)` without `GROUP BY entity_id` caveat documented | query-language.md §8 contains the caveat; `sessionize.rs` module-doc paragraph also warns; integration test `sessionize_stats_per_entity_session_count_uses_group_by` covers the correct GROUP BY idiom | ⚠️ |
| §6.3 `session_duration: Int64 NOT NULL`; value = `max_ts - min_ts`; single-event session = 0 | `state.session_last_ts.saturating_sub(state.session_start_ts)` in `flush_session` (`sessionize.rs:736`); unit test `single_event_is_single_session_with_duration_zero`; property test `session_id_monotone_and_duration_matches_range` verifies `max_ts - min_ts` per session run | ✅ |
| §6.3 Trailing idle time up to gap boundary NOT included in `session_duration` | Duration is `session_last_ts - session_start_ts`; no gap-width padding; confirmed by `sdurs == vec![150; 4]` test in `all_within_gap_becomes_one_session` (4 events spanning ts 0–150, duration 150) | ✅ |
| §6.4 Single-event sessions are emitted (SESSIONIZE does not filter) | `single_event_is_single_session_with_duration_zero`; `end_event_as_first_event_of_entity_is_singleton_session`; integration test `single_event_entity_produces_one_session` | ✅ |

**Partial note for §6.2 caveat coverage**: No test explicitly demonstrates the *wrong* behavior (that `COUNT_DISTINCT(session_id)` without GROUP BY produces a misleading result). The GROUP BY idiom is tested, but the anti-pattern is only documented, not tested. Low priority — this is a documentation/ergonomics issue rather than a correctness gap.

---

### §7 — Per-Entity State Layout

| Promise | Evidence | Status |
|---------|----------|--------|
| `session_event_count` resets at each session boundary | `state.session_event_count = 0` in `start_new_session` logic (`sessionize.rs:592`) and in `flush_session` (`sessionize.rs:791`) | ✅ |
| `open_buffer` holds only the currently-open session; cleared at flush | `state.open_buffer.clear()` in `flush_session` (`sessionize.rs:789`); `completed` vec holds closed sessions and is drained at `finish_entity` | ✅ |
| At most one entity's open session in memory at a time | State is per-entity and never shared; `EntityOperatorAdapter` creates a new state for each entity; enforced by design | ✅ |
| Session buffer holds only demanded columns | `buffered_indices` set in `SessionizeInputMap` based on `desc.forwarded_columns` + always-needed columns; `push_slice` projects to `buffered_arrow_schema` | ✅ |

---

### §8 — EntityOperator Integration

| Promise | Evidence | Status |
|---------|----------|--------|
| §8.1 `create_state` starts with `current_session_id: 1`, sentinel timestamps `i64::MIN` | `sessionize.rs:489–501` exactly matches | ✅ |
| §8.2 Per-row loop: gap check → session update → end-event check → cap check | Loop structure at `sessionize.rs:554–634`; order: init path first, then gap (line 580), then session update (605–607), then cap (611–619), then end-event (622–628) — **cap check precedes end-event check** | ⚠️ |
| §8.2 Dictionary fast path for end-event matching: one code-set build per sub-batch | `EndEventCodeSet::build` called once per sub-batch via `EventTypeView::resolve` at `sessionize.rs:533`; `is_end_event` uses integer code lookup; unit test `dictionary_event_type_resolves_end_events_via_codes` | ✅ |
| §8.3 Session can span multiple sub-batches without losing state | `open_buffer` accumulates across `process_sub_batch` calls; unit tests `session_spans_multiple_sub_batches`, `session_boundary_across_sub_batches`; property test `streaming_equivalent_to_single_batch` | ✅ |
| §8.4 `finish_entity` flushes final open session; concatenates all completed sessions into one `RecordBatch` | `sessionize.rs:637–654`; `concat_batches` over `state.completed`; unit tests verify multi-session output | ✅ |
| §8.6 `required_columns` returns `entity_id` + `ts` + `event_type` (when end-events) + forwarded | `required_column_names` built during construction; unit tests `required_columns_include_ts_and_forwarded`, `required_columns_include_event_type_when_end_events_set` | ✅ |
| §8.7 `supported_demands` returns `supports_forwarded_columns: true`; all other bits false | `sessionize.rs:660–665`; unit test `supported_demands_advertises_forwarded_columns` confirms match with `SessionizePhysical::DEMAND_CAPS` | ✅ |

**Partial note for §8.2 loop order**: The design doc's pseudocode shows cap check after end-event check. The implementation checks cap *before* end-event (lines 611 before 622). There is a subtle semantic deviation at the exact cap boundary: if the `(cap+1)`-th event is also an end-event, the pseudocode's ordering would close the session cleanly (end-event check fires first, session closes, entity is not marked `skipping`), while the implementation fires the cap check first, flushes a partial session, and sets `skipping = true` — discarding the clean close. This is a minor correctness deviation at a contrived boundary condition, not just a doc consistency gap. The TASK-455 follow-up item should clarify the intended behavior.

---

### §9 — Demand-Driven Column Forwarding

| Promise | Evidence | Status |
|---------|----------|--------|
| Physical buffer holds only demanded columns; non-demanded nullable columns emitted as null arrays | `output_to_buffer_slot` maps output columns to buffer slots or `None`; `flush_session` emits `new_null_array` for `None` slots; unit tests `forwarded_column_flows_through_session_buffer`, `non_forwarded_column_is_null_padded` | ✅ |
| NOT NULL input columns buffered defensively even if not in `forwarded_columns` | Safety net at `sessionize.rs:209–224`: iterates output schema, buffers any NOT NULL column found in input schema; unit test `buffered_schema_always_includes_entity_id_and_ts` confirms `entity_id` and `ts` are always in buffer | ✅ |
| Buffered schema derived from input `OperatorSchema` so `concat_batches` across sub-batches succeeds | `buffered_arrow_schema` built at construction from input schema fields; `push_slice` casts src column to the expected type if needed | ✅ |

---

### §10 — Fused Aggregate Shapes (Deferred to Wave 5)

| Promise | Evidence | Status |
|---------|----------|--------|
| `fused_aggregate: None` in v1; construction asserts this | `assert!(desc.fused_aggregate.is_none(), ...)` at `sessionize.rs:157` | ✅ |
| `supported_demands.supports_aggregation_fusion == false` | `DemandCapabilities::none()` spread at `sessionize.rs:661` | ✅ |
| Unfused path (SESSIONIZE → STATS) works end-to-end | Integration test `sessionize_stats_per_entity_session_count_uses_group_by` exercises full `SESSIONIZE | STATS ... GROUP BY entity_id` pipeline | ✅ |
| No test verifying fused ≡ unfused output (per §10.2 note) | Fused path not implemented; equivalence test not applicable in v1 | ✅ (N/A in v1) |

---

### §11 — Per-Entity Session Event Cap

| Promise | Evidence | Status |
|---------|----------|--------|
| Default cap = 1,000,000 events | `DEFAULT_SESSION_EVENT_CAP: usize = 1_000_000` at `sessionize.rs:59` | ✅ |
| Cap fires when `session_event_count > cap`; partial session flushed | `sessionize.rs:611–619`; unit test `per_entity_event_cap_flushes_partial_and_skips_rest` | ✅ |
| After cap, remaining events for entity are skipped; `state.skipping = true` | Early return at `sessionize.rs:509`; test confirms `state.cap_exceeded() == true` and output has only cap rows | ✅ |
| §11.3 Diagnostic accessors: `cap_exceeded()`, `entity_id()`, `entity_event_count()` | `sessionize.rs:369–388`; all three accessors present | ✅ |
| §11.3 `entity_event_count` counts all events including those skipped after cap fires | **`entity_event_count` incremented at the top of the per-row loop (`sessionize.rs:556`), but `process_sub_batch` returns early at the top (`sessionize.rs:509`) when `skipping == true`. Events in sub-batches arriving after the cap fires are never counted. The count also under-counts events after the cap row within the same sub-batch (rows R+1..num_rows).** | ⚠️ |
| Warning-channel plumbing deferred; state carries the flag for later adapter integration | Documented in `sessionize.rs:330–334`, §11.4 callout | ✅ |

---

### §12 — WITHIN SESSION Interaction with MATCH

| Promise | Evidence | Status |
|---------|----------|--------|
| §12.1 SESSIONIZE emits `session_id` as a monotonically increasing integer that MATCH can observe | `session_id` column is always in the output schema; the emitted Int64 sequence is monotone per entity (verified by unit and property tests) | ✅ |
| §12.1 MATCH with `WITHIN SESSION` expires all active NFA candidates when `session_id` increments | **`bqlite-planner/src/compile.rs:262` coalesces `MatchWindow::WithinSession` to `None` (no window). The matcher never observes `session_id` increments. Cross-session pairs still match today.** Two integration tests (`within_session_match_expires_across_boundary`, `within_session_match_composes_with_downstream_stats`) are marked `#[ignore]` with explicit attribution to this line. | ❌ |
| §12.2 `WITHIN SESSION` is mutually exclusive with `WITHIN <duration>` and `BRACKETS` | Enforced at parse time (`bqlite-parser/src/pattern.rs:1051–1077`); planner-level cross-check in logical plan lowering (`bqlite-planner/src/logical.rs:1954`) | ✅ |

---

### §13 — Emission Timing

| Promise | Evidence | Status |
|---------|----------|--------|
| §13.1 Rows emitted when session closes (not per-event) | `completed` vec filled by `flush_session`; single-event sessions confirm no early emission | ✅ |
| §13.2 `session_id` and `session_duration` are constant-valued for all rows in a session | `Int64Array::from(vec![session_id; num_rows])` and `Int64Array::from(vec![duration_ns; num_rows])` in `flush_session` | ✅ |
| §13.3 No per-event emission; buffer required because `session_duration` is unknown until session closes | Confirmed by code structure; `open_buffer` accumulates, `flush_session` materializes | ✅ |

---

### §14 — Benchmarks and Edge Cases

| Promise | Evidence | Status |
|---------|----------|--------|
| §14.1 Edge cases (empty entity, single-event entity, all-in-one-session, all-singleton, gap+end on same row, entity boundary mid-batch, sub-batch boundary, cap exceeded) | All covered across unit tests and property tests (see detailed list above) | ✅ |
| §14.2 Benchmark suite | `benches/wave4/sessionize.rs` covers gap-only, gap+1-end-event (StringView and Dictionary layouts) at 10K and 100K events. Missing: 5-type end-event list (the "<10% overhead" target), memory benchmarks (typical and large sessions), latency benchmarks (entity boundary, session close), and no pass/fail thresholds for any numeric target | ⚠️ |
| §14.3 Property test coverage of all 6 invariants | All 6 invariants covered: (1) `every_event_appears_exactly_once`, (2+3) `session_id_monotone_and_duration_matches_range`, (4) included in same test, (5) `end_event_is_last_in_its_session`, (6) `entity_isolation_restarts_session_id`; streaming equivalence via `streaming_equivalent_to_single_batch` | ✅ |

---

### §17 — Follow-On Doc Update Requirements

| Promise | Evidence | Status |
|---------|----------|--------|
| §17.1 query-language.md §8: add gap-exclusive rule, end-event membership, end-event list form, event cap behavior, `COUNT_DISTINCT` caveat | query-language.md §8 already contains: gap-exclusive rule (line 628), end-event membership (line 628), list form (lines 619–620), event cap paragraph (line 634), `COUNT_DISTINCT` caveat (line 630). Substantially complete. | ✅ |
| §17.1 type-system.md §6.3: confirm `session_id: Int64 NOT NULL`, `session_duration: Int64 NOT NULL` (nanos) | type-system.md §6.3 lists both as `Int` (not `Int64`). BQL `Int` maps to Arrow `Int64`, so this is technically correct, but the wording doesn't explicitly say "nanoseconds" for `session_duration` in the table; the description column does say "Session duration in nanoseconds". Sufficiently detailed. | ✅ |
| §17.1 execution-model.md §2.1: update SESSIONIZE state summary from "Current session ID + last event timestamp" to reflect actual per-session buffer | execution-model.md §2.1 table still reads "Sessionization (SESSIONIZE) | Streaming fold | Current session ID + last event timestamp". The per-session buffer is not reflected. The design doc itself (§2.1 of sessionize.md) flags this as a deferred update. | ❌ |

---

## Additional Issues Found

### A — `WITHIN SESSION` Not Enforced in Matcher (Critical)

**Location**: `crates/bqlite-planner/src/compile.rs:262`

The NFA compiler coalesces `Some(MatchWindow::WithinSession)` to `None`:

```rust
let global_window = match pattern.window {
    Some(MatchWindow::Within(nanos)) => Some(nanos),
    Some(MatchWindow::WithinSession) | None => None,  // ← WithinSession silently dropped
};
```

Result: `SESSIONIZE | MATCH ... WITHIN SESSION` compiles to a windowless matcher. Cross-session matches are not rejected. The two integration tests that verify correct expiry behavior are both marked `#[ignore]`. The parser, AST, and logical plan all carry `WithinSession` correctly — only the final NFA compilation step drops it.

**Impact**: Any query using `WITHIN SESSION` today produces semantically incorrect results — it behaves identically to a query without `WITHIN SESSION`. Users relying on session-scoped matching will get false positives (cross-session matches).

**Workaround**: None. Queries must be rewritten to use a `WITHIN <duration>` approximation until the matcher is updated.

---

### B — `entity_id` Column Name Hardcoded in Buffered-Column Derivation (Minor)

**Location**: `crates/bqlite-operators/src/sessionize.rs:201`

The operator hardcodes `"entity_id"` when building the set of always-buffered columns:

```rust
push_name("entity_id", &mut buffered_names, &mut seen);
```

If a table declares its entity key column with a name other than `entity_id` (e.g., `user_id`), the operator panics during construction at the buffered-index resolution step (`sessionize.rs:230`): `push_name("entity_id", ...)` adds `"entity_id"` to `buffered_names`, but the input schema has `"user_id"` — the subsequent `input_schema.column("entity_id").map(|(i, _)| i).unwrap_or_else(|| panic!(...))` fires. The NOT NULL safety net (lines 209–224) would independently buffer `"user_id"` correctly, but both code paths execute, so the panic is not averted by the safety net.

This is noted in the integration test file (line 87–94): "The ENTITY KEY column must be literally named `entity_id` for SESSIONIZE to bind."

The entity key column name is available from the `TableSchema` at planner time and could be propagated via `SessionizePhysical`.

---

### C — `entity_event_count` Under-Counts Events After Cap Fires (Minor)

**Location**: `crates/bqlite-operators/src/sessionize.rs:509`

The `process_sub_batch` function returns early when `state.skipping == true`, before incrementing `entity_event_count`. As a result:

- Events arriving in sub-batches *after* the cap fires (i.e., all events from the second and later sub-batches after the cap) are never counted.
- Events at rows R+1..num_rows in the same sub-batch where the cap fires are also not counted (the function returns immediately when the cap event is processed).

The comment in the code says "including those skipped after the cap fires" but the implementation does not achieve this for subsequent sub-batches. The diagnostic warning's `event_count` field will report a lower-bound approximation.

Since the warning-channel plumbing is deferred to Wave 5 (§11.4), this does not affect any currently-observable behavior. It should be corrected before the diagnostic channel lands.

---

### D — `flush_session` Increments `current_session_id` on Empty Buffer (Minor)

**Location**: `crates/bqlite-operators/src/sessionize.rs:727`

When `flush_session` is called with an empty `open_buffer`, the function resets bookkeeping and increments `current_session_id`:

```rust
if state.open_buffer.is_empty() {
    state.session_start_ts = i64::MIN;
    state.session_last_ts = i64::MIN;
    state.session_event_count = 0;
    state.current_session_id += 1;   // ← increments even with no output rows
    return;
}
```

Analysis of the code paths shows this guard should never fire in well-formed operation: every `flush_session` call is preceded by at least one `push_slice` with one or more rows, or the function is called only after confirming `session_start_ts != i64::MIN`. However, if the guard ever fires (e.g., due to a latent bug elsewhere), it silently skips a session ID without producing any output rows, making `session_id` non-contiguous and violating the "monotonically increasing by 1" property. The correct behavior is `return` without incrementing.

---

### E — Schema Binding Bug: Scan Does Not Materialize System Columns (Operational Gap)

**Location**: Integration test file header (`tests/tests/wave4_advanced_analytics_sessionize.rs:43–64`)

The scan operator does not materialize system columns (`__seq_id`, `__batch_id`) in the runtime `RecordBatch`, but `SessionizeOperator::new` resolves its buffered-column indices against the logical schema (which includes system columns). Without an intervening projection, the index-to-column mapping diverges between planner and runtime, causing an "index out of bounds" panic in `push_slice`.

The integration test suite works around this by prepending `| SELECT entity_id, ts, event_type` before every `SESSIONIZE`. This is a planner/engine binding gap, not a SESSIONIZE-specific issue, but it means that queries without an explicit projection ahead of SESSIONIZE will panic at runtime. The fix belongs in the engine bind step (align runtime column indices with the logical schema after system-column exclusion).

---

## Findings Summary

| Finding | Severity | Section | Notes |
|---------|----------|---------|-------|
| `WITHIN SESSION` not enforced — matcher coalesces to no-window | **Critical** | §12.1 | `compile.rs:262`; 2 tests `#[ignore]` |
| `entity_id` column name hardcoded; panics at construction on non-standard entity key names | Minor | §4, construction | Noted in integration test comment; NOT NULL safety net does not prevent the panic |
| `entity_event_count` under-counts events in sub-batches after cap fires | Minor | §11.3 | Only affects deferred warning diagnostic |
| `flush_session` increments `session_id` on empty buffer | Minor | §7 | Should never fire; guard logic incorrect |
| Schema binding bug: scan system columns cause index mismatch | Operational | §8.2 | Workaround: explicit `SELECT` before SESSIONIZE |
| `execution-model.md §2.1` state summary not updated | Doc drift | §17.1 | Still says "Current session ID + last event timestamp" |
| Benchmark suite missing: 5-type end-event list, memory, latency | Gap | §14.2 | Current benches cover throughput only at 2 sizes |
| Loop-order divergence: cap fires before end-event close at `cap+1` boundary | Minor correctness | §8.2 | Contrived boundary; TASK-455 to clarify intent |

---

## Follow-Up Items for TASK-455

All items below are **observations only** — no code changes made in this audit task.

1. **Wire `WITHIN SESSION` in `compile.rs`**: Implement the `MatchWindow::WithinSession` arm in `compile_nfa` to propagate the session-boundary expiry signal to the NFA runtime (observes `session_id` column increments). Enable the two `#[ignore]` integration tests once the matcher is updated. Also update or replace the unit test `global_window_session_is_none` at `compile.rs:1554–1566` — it currently asserts `nfa.global_window == None` for a `WithinSession` pattern, which documents the broken behavior and will fail once the fix lands.

2. **Fix `entity_id` hardcode**: Propagate the actual entity-key column name through `SessionizePhysical` (or derive it from the input schema's entity-key declaration) and replace the hardcoded `push_name("entity_id", ...)` at `sessionize.rs:201` with `push_name(&desc.entity_key_col, ...)`. Both the explicit push and the NOT NULL safety net must use the real column name; the current explicit push panics at construction time when the name differs.

3. **Fix `entity_event_count` counting**: Accumulate the event count for all skipped events (across sub-batches) before the warning fires. The simplest approach: when `process_sub_batch` encounters `state.skipping == true` at the top, count the remaining rows in the batch (`state.entity_event_count += batch.num_rows() as u64`) before returning.

4. **Fix `flush_session` empty-buffer guard**: Change the guard to `return` without incrementing `current_session_id`, since an empty flush produces no output and advancing the counter is incorrect.

5. **Update `execution-model.md §2.1`**: Replace "Current session ID + last event timestamp" in the SESSIONIZE row with "Current session ID, open-session row buffer, session start/last timestamps, per-entity event count; per-session buffer bounded by §11 cap."

6. **Extend benchmark suite**: Add a 5-type end-event list benchmark, and either a memory-measurement benchmark or a comment linking to the §14.2 target rationale. The existing throughput benchmarks satisfy the gap-only and single-end-event promises.

7. **Address schema binding bug for system columns**: Fix the engine bind step so SESSIONIZE's column-index resolution aligns with the runtime batch layout, removing the need for an explicit `SELECT` projection workaround.

8. **Add property test for `WITHIN SESSION` semantics**: Once finding #1 is resolved, add a property test verifying that no match output row has a `search` step and `checkout` step with different `session_id` values (i.e., the session-boundary expiry correctly prevents cross-session matches).
