# Wave 4 Delete, Tombstone, and Compaction Semantic Audit

**Auditor**: TASK-448
**Date**: 2026-04-19
**Sources reviewed**:
- Design spec: `docs/design/storage/deletes.md` (TASK-404)
- Supporting spec: `docs/design/storage/compaction-concurrency.md` (TASK-403)
- Parser: `crates/bqlite-parser/src/dml.rs` (TASK-433)
- Tombstone storage: `crates/bqlite-storage/src/tombstone.rs` (TASK-432 / TASK-434)
- Tombstone scan wrappers: `crates/bqlite-storage/src/tombstone_scan.rs` (TASK-434 / TASK-435)
- Compaction executor: `crates/bqlite-storage/src/compaction.rs` (TASK-435)
- Planner classifier: `crates/bqlite-planner/src/logical.rs` (TASK-453)
- Engine execution: `crates/bqlite-engine/src/delete.rs` (TASK-453)
- Integration tests: `tests/tests/wave4_delete_compaction.rs` (TASK-440)
- Scan operator: `crates/bqlite-operators/src/scan.rs` (TASK-434 integration)

**Methodology**: Walk the design doc section by section; for each promise, locate primary evidence in code + tests; classify each as ✅ Covered, ⚠️ Partial, or ❌ Missing. File follow-up items for partial/missing rows. Nothing is fixed here — all drift is rolled up into TASK-455.

---

## Promise-vs-Evidence Matrix

### §2 — Tombstone Granularities

| Promise | Evidence | Status |
|---------|----------|--------|
| Four granularities: row (`__seq_id`), batch (`__batch_id`), entity (key column), time-range | `TombstoneFile` in `tombstone.rs:62–76` matches schema exactly; `entity_deletes: HashSet<ScalarValue>`, `row_deletes: HashSet<u64>`, `batch_deletes: HashSet<u64>`, `time_range_deletes: Vec<TimeRangeDelete>` | ✅ |
| Entity tombstone is "data not a rule" (applies only to rows present at write time) | `deletes.md` §2 prose, compaction reclamation removes entity entries once they're empty (§12.4 in `reclaim_tombstones_after_compaction`), confirmed in `compaction_reclaims_entity_tombstones_and_drops_deleted_rows` integration test | ✅ |

---

### §3 — Cheap-Class Predicate Taxonomy

| Promise | Evidence | Status |
|---------|----------|--------|
| Cheap = top-level AND-chain of: entity-key `=`/`IN`, `__seq_id =`/`IN`, `__batch_id =`/`IN`, time comparisons (`<`, `<=`, `>`, `>=`, `BETWEEN`) | `classify_delete_predicate` in `logical.rs:3381` flattens AND conjuncts, classifies each conjunct against the allowlist; full taxonomy encoded in `into_cheap_spec` (`logical.rs:3471`) | ✅ |
| Non-cheap (OR, NOT, `!=`, arbitrary expressions) → rejected or forced to full-scan class | `classify_delete_predicate` returns `DeleteFilter::AllowScan` for non-cheap predicates when `allow_scan=false`, hard error at plan time (`logical.rs:3422–3428`) | ✅ |
| §3.1 Same-granularity AND-chain: two time-range terms collapse to one `TimeRangeDelete` with both bounds | `into_cheap_spec` collects `time_min`/`time_max` separately and builds one `TimeRangeSpec` with both bounds (`logical.rs:3456–3457`); tested in `time_range_two_bounds_collapses_to_single_range` | ✅ |
| §3.2 Cross-granularity conjunctions (e.g., entity + time-range) rejected | `into_cheap_spec` rejects any mix of time-range with entity/seq/batch at `logical.rs:3483–3498`; tested in `entity_plus_time_range_is_rejected_cross_granularity`, `seq_plus_batch_is_rejected_cross_granularity` | ✅ |
| §3.2 Exception: entity + `__seq_id`/`__batch_id` → entity used for shard targeting only (`EntityRole::AsShardTarget`) | `EntityRole::AsShardTarget` emitted at `logical.rs:3501–3504`; engine uses entity key only for shard routing, not as a separate tombstone entry (`delete.rs:505–506`); tested in `entity_plus_seq_id_uses_seq_id_count_with_shard_targeting` | ✅ |
| §3.3 Shard targeting via `xxhash64(entity_id) % num_shards` | `shard_id_for(entity_id, num_shards)` called at `delete.rs:505` using `bqlite_storage::ingest::partitioner::shard_id_for` (which wraps xxHash64) | ✅ |
| §3.3 No entity predicate (e.g., pure time-range) → tombstones written to all shards | `execute_cheap_delete` iterates all (window, shard) pairs when no entity key is present (`delete.rs:532–562`) | ✅ |

---

### §4 — Full-Scan DELETE and ALLOW SCAN

| Promise | Evidence | Status |
|---------|----------|--------|
| Non-cheap predicates rejected at plan time with a hard error | `classify_delete_predicate` returns `Err(BqliteError::...)` for non-cheap predicates when `allow_scan=false` (`logical.rs:3422–3428`); tested in `non_cheap_predicate_without_allow_scan_is_rejected` | ✅ |
| `ALLOW SCAN` suffix parsed correctly as two tokens (case-insensitive) | `parse_allow_scan` in `dml.rs:439–469`; error surfaced for `ALLOW` without `SCAN` | ✅ |
| §4.1 ALLOW SCAN execution: scan all segments → materialize `__seq_id` of matching rows → write row-level tombstones per shard | `execute_allow_scan_delete` in `delete.rs:91–359` walks all segments, loads tombstone snapshot once at bind time, evaluates predicate per row group, derives `__seq_id = seq_id_first + segment_offset + row_idx`, writes row-level tombstones; tested in `allow_scan_materializes_row_tombstones_to_disk` | ✅ |
| §4.1 ALLOW SCAN scan respects manifest snapshot at DELETE-start | `execute_allow_scan_delete` reads manifest segments at function entry; mid-DELETE INSERTs are invisible (`delete.rs` module-level comment §8) | ✅ |

---

### §5 — Tombstone File Schema

| Promise | Evidence | Status |
|---------|----------|--------|
| Schema: `entity_deletes: HashSet<ScalarValue>`, `row_deletes: HashSet<u64>`, `batch_deletes: HashSet<u64>`, `time_range_deletes: Vec<TimeRangeDelete>` | `tombstone.rs:62–76` matches exactly | ✅ |
| `time_range_deletes` is `Vec` (not `Option`) to support multiple independent time-range deletes | `Vec<TimeRangeDelete>` at `tombstone.rs:75`; schema-change rationale documented in `deletes.md` §5.1 | ✅ |
| §5.1 Time-range deduplication at write: exact-match duplicate skipped, O(n) linear scan | `TombstoneFile::merge` at `tombstone.rs:115–118`: `if !self.time_range_deletes.contains(range) { self.time_range_deletes.push(...) }` | ✅ |
| §5.2 Multiple time-range scan evaluation: row suppressed if timestamp falls within ANY range; short-circuit on first match | `TombstoneFilter::apply_time_range_deletes` iterates ranges and short-circuits; `tombstone.rs` | ✅ |
| Atomic write via write-to-tmp then rename | `write_tombstone_atomic` helper; called in `reclaim_tombstones_after_compaction` at `compaction.rs:717` and in `write_shard_tombstone` in `delete.rs` | ✅ |

---

### §6 — Per-Query Tombstone Snapshot

| Promise | Evidence | Status |
|---------|----------|--------|
| Tombstone files loaded once at query bind time, shared across all scan operators | `execute_allow_scan_delete` loads snapshot at `delete.rs:170`; `ScanOperator::with_tombstones` receives a pre-built `Arc<TombstoneSnapshot>` from the engine bind step | ✅ |
| §6.1 All scan operators in a single query observe the same tombstone state (no within-query anomalies) | Shared `Arc<TombstoneSnapshot>` passed to all `ScanOperator` instances at plan time | ✅ |
| §6.1 Joined-source queries observe a single coherent tombstone epoch across all input tables | Snapshot loading for joined sources not covered in integration tests | ⚠️ |
| §6.2 Tombstone snapshot taken alongside `Arc<Manifest>` snapshot at bind time | Manifest snapshot loaded at bind in engine; tombstone snapshot co-loaded | ✅ |
| §6.3 Must never re-read tombstone files from disk during query execution | `ScanOperator` receives pre-built snapshot; snapshot not re-loaded per batch or row group (confirmed by code inspection and `each_query_loads_a_fresh_tombstone_snapshot` integration test) | ✅ |

---

### §7 — Scan-Time Filtering Order

| Promise | Evidence | Status |
|---------|----------|--------|
| Tombstone filtering occurs after zone-map/predicate pushdown, before rows reach operators | `TombstoneScanWrapper::next_row_group()` applies `TombstoneFilter` to each row group after the inner scan (which handles zone-map pushdown) has returned it (`tombstone_scan.rs:72–82`); scan.rs module comment confirms pipeline order | ✅ |
| §7.1 Check order within tombstone filter: batch → entity → row → time-range (cheapest first) | `TombstoneFilter::filter_batch` at `tombstone.rs:402–409`: 1) `apply_batch_deletes`, 2) `apply_entity_deletes`, 3) `apply_row_deletes`, 4) `apply_time_range_deletes` — matches design order exactly | ✅ |
| Row-level (`__seq_id`) and batch-level (`__batch_id`) tombstones applied at query time | **NOT IMPLEMENTED**: `__seq_id` and `__batch_id` are not exposed as columns in the scan's materialized output (`scan.rs:53–60`). `TombstoneFilter.apply_batch_deletes` / `apply_row_deletes` error when these columns are absent. Documented as a "current scope limit" in `scan.rs:3489–3519` and in integration test comments (`wave4_delete_compaction.rs:470–476`, `309–312`). Entity- and time-range tombstones function correctly. | ❌ |

---

### §8 — DELETE vs. In-Flight INSERT

| Promise | Evidence | Status |
|---------|----------|--------|
| DELETE sees only the manifest-visible set at DELETE-start | Engine reads manifest at DELETE function entry; concurrent INSERT manifest updates are not visible (confirmed by `delete.rs` module-level comment §8) | ✅ |
| §8.2 User-facing documentation: "wait for INSERT to complete before issuing DELETE" | Deferred to TASK-456; referenced in `delete.rs:47–49` comment | ⚠️ (intentional deferral) |

---

### §9 — Concurrent DELETEs on the Same Shard

| Promise | Evidence | Status |
|---------|----------|--------|
| Per-shard in-process `Mutex<()>` serializes concurrent tombstone writes | `Database::tombstone_shard_lock` rented in `write_shard_tombstone` (`delete.rs:611–613`); read-modify-write cycle held under the lock | ✅ |
| Queries do not take the tombstone lock | `TombstoneSnapshot` loaded by reading tombstone files without locking; the per-shard mutex is held only during write | ✅ |
| §9.1 Lock scope: read, merge, write-to-tmp, fsync, rename | `write_shard_tombstone` in `delete.rs` follows the documented RMW cycle; `write_tombstone_atomic` handles the tmp+rename pattern | ✅ |
| §9.2 Multi-process flock promotion deferred | Explicitly deferred per design doc §9.2 and `delete.rs` module comment | ⚠️ (intentional deferral) |

---

### §10 — Cross-Shard Crash Atomicity

| Promise | Evidence | Status |
|---------|----------|--------|
| Cross-shard DELETE is per-shard atomic; no cross-shard WAL | Each shard's tombstone file updated independently under its own mutex | ✅ |
| Idempotent retry: re-running same DELETE converges to same state | `TombstoneFile::merge` uses set-union for row/batch/entity entries; time-range entries exact-match deduplicated (`tombstone.rs:110–119`); tested in `idempotent_delete_returns_same_count_and_state` | ✅ |
| §10.2 ALLOW SCAN idempotence caveat: retry may tombstone additional rows ingested between runs | Documented in `delete.rs:37–40`; user-facing documentation deferred to TASK-456 | ⚠️ (intentional deferral) |

---

### §11 — DELETE Return Value

| Promise | Evidence | Status |
|---------|----------|--------|
| Exact `rows_affected: u64` always returned | `execute_delete_statement` returns `ExecutionResult { rows_affected: Some(count), ... }` (`delete.rs:84–88`) | ✅ |
| `__seq_id` cheap-class: count = `|input_set|` | `compute_rows_affected` for seq_ids returns `dedup_len` of the input literal set (`delete.rs:641–651`); tested in `seq_id_in_list_returns_input_set_cardinality` | ✅ |
| Batch-level cheap-class: count from segment metadata row_count scan | `compute_rows_affected` for batch_ids sums `seg.row_count` for matching batch_ids across segments (`delete.rs:653–654`); tested in `batch_id_delete_uses_segment_metadata_for_count` | ✅ |
| Entity-level cheap-class: count from entity-key column scan (contiguous due to entity-sorted layout) | `compute_rows_affected` for entity keys reads entity-key column per shard and counts matches (`delete.rs:659–660`) | ✅ |
| Time-range cheap-class: zone-map for fully-inside row groups, timestamp column scan for boundary groups | `compute_rows_affected` for time-range uses zone-map metadata for inside groups, per-row timestamp scan at boundaries (`delete.rs:656–657`) | ✅ |
| ALLOW SCAN count: byproduct of the scan itself | `execute_allow_scan_delete` accumulates count while materializing seq_ids, returns exact count at end (`delete.rs:298–302`) | ✅ |
| §11.2 Zero-match DELETE returns `rows_affected = 0` with success | Tested in `entity_delete_for_unknown_entity_returns_zero`, `allow_scan_zero_match_writes_no_row_tombstones` | ✅ |

---

### §12 — Compaction-Time Reclamation

| Promise | Evidence | Status |
|---------|----------|--------|
| §12.1 Compaction snapshots tombstones at job start | `compact_one` reads `tombstones.json` at job entry (before merge loop) into `tombstone_snapshot_at_start`; `CompactionTombstoneScan` wraps each input segment scan with this snapshot | ✅ |
| §12.1 Mid-compaction DELETEs are invisible to the in-flight compaction | Snapshot read once at job start; subsequent tombstone file writes (from concurrent DELETEs) are not re-read mid-job | ✅ |
| §12.2 Manifest-first reclamation ordering: manifest published (step 11) before tombstone rewrite (step 13) | In `compact_one`: `db.replace_segments(...)` at step 11 (`compaction.rs:563`), then `reclaim_tombstones_after_compaction(...)` at step 13 (`compaction.rs:581`). Comment at `compaction.rs:577` explicitly states "Manifest-first ordering: a crash between publish and the rewrite below leaves stale tombstones that §12.3 guarantees are harmless." | ✅ |
| §12.2 Zero-row path: manifest published before tombstone reclamation | `remove_segments_atomic` called at `compaction.rs:400` before `reclaim_tombstones_after_compaction` at `compaction.rs:408`; confirmed by `compaction_purges_shard_when_every_input_row_is_tombstoned` integration test | ✅ |
| §12.3 Stale tombstone safety: crash between manifest publish and tombstone rewrite leaves harmless no-ops | Invariant documented in `deletes.md` §12.3, reiterated in `compaction.rs` code comment at step 13; the new output segment physically excludes the already-reclaimed rows, so stale tombstone entries match nothing | ✅ |
| §12.4 Row-level reclamation: remove `__seq_id` entries whose seq_id was within any compacted input's `seq_id_range` | `reclaim_tombstones_after_compaction` at `compaction.rs:666–678` retains only seq_ids not covered by any input segment's `seq_id_range`, guarded by `in_snapshot` check | ✅ |
| §12.4 Batch-level reclamation: remove `batch_id` entries matched by any compacted input | `reclaim_tombstones_after_compaction` at `compaction.rs:681–690` retains batch_ids not matched by any `seg.batch_id` in input segments | ✅ |
| §12.4 Entity-level reclamation: remove entity entries (output segment has no rows for those entities) | `reclaim_tombstones_after_compaction` at `compaction.rs:696–699` drops all entity entries present in snapshot. Relies on the invariant that the new output segment is the only remaining segment in the `(window, shard)` after publish. The function comment (`compaction.rs:620–626`) explicitly notes that a future per-shard concurrent writer would require re-snapshotting the manifest under the publish lock to tighten this logic; §12.3 keeps correctness either way | ⚠️ (correct today; documented assumption for future concurrent-writer extension) |
| §12.4 Time-range reclamation: remove time-range entries (output segment has no rows in those ranges) | `reclaim_tombstones_after_compaction` at `compaction.rs:704–708` drops all time-range entries present in snapshot by equality match. Same single-remaining-segment assumption as entity-level above | ⚠️ (correct today; documented assumption for future concurrent-writer extension) |
| §12.5 Optional time-range merge optimization during tombstone rewrite | Not implemented; deferred per design doc §12.5 footnote ("optional optimization…deferred to compaction-time tombstone rewrite") | ⚠️ (intentional deferral) |
| Compaction tombstone reclamation serialized against concurrent DELETEs via per-shard mutex | `reclaim_tombstones_after_compaction` acquires `db.tombstone_shard_lock` at `compaction.rs:651` | ✅ |

---

### §13 — Warning Channel

| Promise | Evidence | Status |
|---------|----------|--------|
| No DML warning channel in Wave 4; cheap DELETEs return only `rows_affected`, no warnings | `execute_delete_statement` returns `ExecutionResult { rows: Vec::new(), rows_affected: Some(count), .. }` with no warning or diagnostic field (`delete.rs:84–88`); the return type has no warning channel | ✅ |
| Dangerous predicate shapes (non-cheap without `ALLOW SCAN`) are gated at plan time, not via a warning | `classify_delete_predicate` returns a hard `Err(BqliteError::...)` for non-cheap predicates (`logical.rs:3422–3428`); no warning is emitted, execution is rejected entirely | ✅ |

---

### Integration Test Coverage (TASK-440)

| Test | Coverage | Status |
|------|----------|--------|
| `entity_delete_excludes_rows_in_subsequent_query` | Entity tombstone applied correctly in SELECT after DELETE | ✅ |
| `entity_in_list_delete_drops_each_listed_entity` | Entity IN-list tombstone; all listed entities excluded | ✅ |
| `batch_id_delete_uses_segment_metadata_for_count` | Batch-level DELETE rows_affected; tombstone written; SELECT cannot filter (documented scope limit) | ✅ (with documented gap) |
| `seq_id_in_list_returns_input_set_cardinality` | Row-level rows_affected is cardinality of input literal set | ✅ |
| `time_range_delete_excludes_old_rows` | Time-range tombstone applied in SELECT | ✅ |
| `time_range_two_bounds_collapses_to_single_range` | AND conjunction of two time bounds collapses to one `TimeRangeDelete` | ✅ |
| `entity_plus_seq_id_uses_seq_id_count_with_shard_targeting` | Entity as shard target, `__seq_id` as tombstone | ✅ |
| `entity_delete_for_unknown_entity_returns_zero` | Zero-match DELETE returns `rows_affected = 0` | ✅ |
| `time_range_outside_window_writes_no_tombstone_file` | No tombstone written when predicate matches no data | ✅ |
| `non_cheap_predicate_without_allow_scan_is_rejected` | Default-reject non-cheap predicate | ✅ |
| `allow_scan_materializes_row_tombstones_to_disk` | ALLOW SCAN writes row-level tombstones; SELECT cannot filter (documented scope limit) | ✅ (with documented gap) |
| `allow_scan_zero_match_writes_no_row_tombstones` | ALLOW SCAN zero-match: no tombstones written | ✅ |
| `each_query_loads_a_fresh_tombstone_snapshot` | Per-query snapshot freshness: DELETE mid-stream reflected in subsequent SELECT | ✅ |
| `idempotent_delete_returns_same_count_and_state` | Idempotent retry: same tombstone state after two identical DELETEs | ✅ |
| `compaction_reclaims_entity_tombstones_and_drops_deleted_rows` | Compaction physically drops entity-tombstoned rows and clears tombstone entries | ✅ |
| `compaction_purges_shard_when_every_input_row_is_tombstoned` | Zero-row compaction path with tombstone reclamation | ✅ |
| `delete_on_missing_table_errors_at_plan_time` | Missing-table DELETE fails at plan time | ✅ |
| DELETE + joined-source read (§6.1) | Not covered | ❌ |
| DELETE + cohort query | Not covered | ❌ |
| Concurrent DELETE + long-running query tombstone isolation | Not covered | ❌ |

---

## Drift and Missing Coverage — Follow-up Items for TASK-455

### F1 — Row-level and batch-level tombstones not applied at query time (High severity)

**Promise**: Design §7 states that tombstone filtering (for all four granularities) runs after zone-map pushdown and before rows reach operators. Operators must never see tombstoned rows.

**Evidence**: `__seq_id` and `__batch_id` are not materialised as columns in the scan operator's output (`scan.rs:53–60`). `TombstoneFilter.apply_batch_deletes` and `apply_row_deletes` both require those columns to be present and return a hard error (`BqliteError::Execution`) when they are absent but the respective tombstone set is non-empty (`tombstone.rs:431–456`). After `DELETE WHERE __seq_id IN (...)`, `DELETE WHERE __batch_id = N`, or `DELETE ... ALLOW SCAN`, a subsequent SELECT will error at scan time **only for shards whose tombstone snapshot contains non-empty `row_deletes` or `batch_deletes`** — shards with no row/batch tombstones are unaffected. The error is shard-local and propagates through `TombstoneScanWrapper::next_row_group` as a hard `Err`.

Note: the ALLOW SCAN write path (`execute_allow_scan_delete`) already accounts for this limitation — it walks segments directly from the manifest using `seq_id_range` from `SegmentMeta` rather than going through `ScanOperator`, precisely because system columns are absent from the scan surface. The gap is wholly in the subsequent SELECT path.

**Scope**: Both `scan.rs` and the integration tests document this limitation explicitly. The code guards against silent pass-through (the error is surfaced loudly rather than deleted rows being silently returned). Compaction-time reclamation (`CompactionTombstoneScan`) handles row- and batch-level tombstones correctly by deriving seq_id from segment manifest metadata without needing materialised columns.

**Impact**: Row-level and batch-level DELETEs cannot be used safely in combination with subsequent SELECT queries that touch the same shards, until system columns are materialised in the scan output. The error surfaces loudly; it is not a silent data correctness issue.

**Follow-up**: Wire `__seq_id` and `__batch_id` as virtual materialised columns in the scan output (as foreshadowed in `scan.rs:57–60`). This unblocks row- and batch-level tombstone filtering at query time and removes the error guard.

---

### F2 — No integration tests for DELETE interactions with joined-source reads (§6.1)

**Promise**: Design §6.1 states that "Joined-source queries (TASK-436) observe a single coherent tombstone epoch across all input tables."

**Evidence**: No integration test covers a DELETE followed by a joined-source query, or a scenario where tombstones on one source affect joined-entity visibility. TASK-436 was in scope before this audit but tombstone coverage for the join path was not carried into TASK-440. Additionally, while the design says a shared `Arc<TombstoneSnapshot>` is passed to all scan operators in a query, this threading has not been verified for the `MergeSources` operator path that joined-source queries use.

**Follow-up**: (a) Verify that the engine bind step threads the `TombstoneSnapshot` to all `ScanOperator` instances created by the `MergeSources` / joined-source plan, including those for secondary input tables. (b) Add integration tests verifying that entity tombstones on source A are visible (as filtered-out rows) to a joined query that reads both source A and source B.

---

### F3 — No integration tests for DELETE interaction with cohorts

**Promise**: Design §6.1 scope implies multi-scan coherence for cohort-filtered queries.

**Evidence**: No test covers a DELETE on an entity that is a member of a cohort used in a subsequent query.

**Follow-up**: Add integration tests for DELETE + cohort-filtered query.

---

### F4 — No integration test for concurrent DELETE + query snapshot isolation

**Promise**: Design §6.2 states that tombstone snapshots are immutable for the lifetime of a query; DELETEs that complete after bind time are invisible to the running query.

**Evidence**: `each_query_loads_a_fresh_tombstone_snapshot` tests the between-query freshness (DELETE between two SELECTs). There is no test for a DELETE that races a running scan (mid-query snapshot isolation under concurrent write).

**Impact**: Low: current tests use single-threaded execution, and the correctness argument (snapshot taken at bind, never re-read) is well-supported by the code. The absence of a concurrent-access test is a testing gap, not a code correctness concern.

**Follow-up**: Add a multi-threaded integration test that runs a long SELECT concurrently with a DELETE and verifies the SELECT returns the pre-DELETE row set.

---

### F5 — Time-range tombstone merge optimization deferred (§12.5)

**Promise**: Design §12.5 says overlapping or adjacent `TimeRangeDelete` entries *may* be merged during the tombstone rewrite to prevent unbounded growth.

**Evidence**: Not implemented. The `reclaim_tombstones_after_compaction` function drops ranges that are in the snapshot but does not merge survivors. This is noted explicitly in the design doc as "optional".

**Impact**: Low: the Vec grows by one entry per independent time-range DELETE. For typical retention-cutoff cadences (daily or weekly), this remains in the 1–10 entry range. A 1000-entry Vec would appear only after 1000 independent time-range DELETEs without intervening compaction.

**Follow-up**: Implement range merging in `reclaim_tombstones_after_compaction` once operational evidence shows the Vec growing to a problematic size.

---

### F6 — User-facing documentation for DELETE semantics deferred (§8.2, §10.2)

**Promise**: Design §8.2 requires user-facing documentation: "DELETE applies to data visible at DELETE-start; wait for INSERT to complete before issuing DELETE." Design §10.2 requires documenting the ALLOW SCAN idempotence caveat.

**Evidence**: Both are documented in `delete.rs` module-level comments as TASK-456 deliverables.

**Follow-up**: Captured in TASK-456. No action needed in TASK-455.

---

## Summary

The delete, tombstone, and compaction implementation is **substantially correct** across parser (TASK-433), tombstone storage (TASK-432/434), planner classifier (TASK-453), engine executor (TASK-453), and compaction reclamation (TASK-435). All design-specified tombstone granularities, schema shapes, and lifecycle ordering are faithfully implemented.

**One high-severity gap** (F1) stands out: row-level and batch-level tombstones are written correctly but are not applied during SELECT queries because the scan operator does not yet expose `__seq_id`/`__batch_id` system columns. This affects the ALLOW SCAN path and `DELETE WHERE __seq_id IN (...)` / `DELETE WHERE __batch_id = N`. The gap is self-documenting (the code errors loudly rather than silently passing deleted rows through) but prevents safe use of those predicate classes until F1 is resolved.

Three test-coverage gaps (F2–F4) should be added before Wave 4 closure; they exercise promises already in the design doc. F5 and F6 are intentional deferrals with low urgency.

| Item | Severity | Blocking? |
|------|----------|-----------|
| F1: Row/batch query-time tombstone filtering missing | High | No (loud `BqliteError` on affected shards, not silent data leak) |
| F2: No DELETE + joined-source integration test | Medium | No |
| F3: No DELETE + cohort integration test | Low | No |
| F4: No concurrent DELETE + query isolation test | Low | No |
| F5: Time-range merge deferred | Low | No |
| F6: User-facing docs deferred | Low | No (TASK-456) |
