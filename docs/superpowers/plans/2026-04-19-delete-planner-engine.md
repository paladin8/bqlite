# TASK-453 — DELETE planner + engine tombstone writer

**Owner**: agent-4
**Date**: 2026-04-19
**Outputs (per TASKS.md)**: `crates/bqlite-planner/src/logical.rs`, `crates/bqlite-engine/src/query.rs`
**Spec**: `docs/design/storage/deletes.md` (TASK-404)
**Depends on**: TASK-432 (`tombstone.rs` storage), TASK-433 (parser),
TASK-434 (tombstone-aware scan)

---

## 1. Scope reminder

The parser (TASK-433) emits `Statement::Delete(DeleteStmt {
table, predicate, allow_scan, span })`. The planner currently rejects
this statement. The storage layer already has `TombstoneFile`,
`TimeRangeDelete`, `write_tombstone_atomic`, `tombstone_file_path`, and
`load_tombstone_snapshot`. The scan path filters out tombstoned rows.

What this task delivers:

1. Lower the parsed DELETE into the planner's logical/physical
   pipeline.
2. Classify the predicate against the cheap-class taxonomy
   (`deletes.md` SS3) at plan time. Reject non-cheap predicates unless
   `ALLOW SCAN` is set.
3. Engine execution path:
   - **Cheap-class:** compute target shards, atomically RMW
     `tombstones.json` per shard, return exact `rows_affected`.
   - **`ALLOW SCAN`:** scan the table, evaluate the predicate,
     materialize `__seq_id`s, write row-level tombstones to every
     shard that contributed matching rows, return the count.
4. Honor SS9 (per-shard serialization), SS10 (idempotent retry),
   SS11 (exact `rows_affected`), SS8 (manifest snapshot at
   DELETE-start).

DELETE produces no result rows but **does** carry an exact
`rows_affected: u64` per SS11. Add an additive
`rows_affected: Option<u64>` field on `ExecutionResult`, defaulting
to `None` for SELECT/DDL/INSERT and `Some(count)` for DELETE. This
is non-breaking — the field defaults via `Option::default()` and
existing callers can ignore it. The CLI render output (TASK-456
scope) and Python bindings (Wave 6) consume it later; the field
is in place now so the engine contract matches SS11 from day one.

Out of scope: integration test suite (TASK-440), CLI examples
(TASK-456), `flock`-based multi-process serialization (deferred per
SS9.2).

---

## 2. Architectural decisions

### 2.1 Per-shard mutex (SS9)

The design specifies an in-process per-shard `Mutex<()>` to serialize
concurrent DELETE writes on the same shard. While `Engine::query`
currently takes `&mut Database` and therefore serializes today, the
SS9 contract is *per-shard* (different shards proceed in parallel)
and is the foundation for SS10's idempotent retry guarantee. Relying
on the `&mut` borrow is a stricter, lossier contract that silently
breaks the moment any future task introduces interior-mutable
execution.

**Decision:** add the literal per-shard lock now. Add a
`tombstone_locks: Mutex<HashMap<(String, u32, u16),
Arc<Mutex<()>>>>` field on `Database`, keyed by
`(table, window_id, shard_id)` (matching the on-disk tombstone-file
granularity), with a helper:

```rust
impl Database {
    pub fn tombstone_shard_lock(
        &self,
        table: &str,
        window_id: u32,
        shard_id: u16,
    ) -> Arc<Mutex<()>> { /* lazy insert, return clone */ }
}
```

The cheap-class and ALLOW SCAN paths call this once per affected
shard, take the lock, RMW the file, drop the lock. Today the lock is
a no-op (the outer `&mut Database` borrow already serializes), but
the locking discipline is in the code and the tests exercise it.
Promotion to `flock` for multi-process writers (SS9.2) is still
deferred.

This adds the field to `Database` — a small change to a shared file.
Schedule it as the first commit in Checkpoint 2 (no need for a
separate checkpoint because `tombstone_locks` is additive and other
agents do not depend on its API surface).

### 2.2 Where the Delete plan node lives

Per task scope (`logical.rs` + `query.rs`), and per
`logical-plan-nodes.md`, `LogicalPlan::Delete` is added directly to
the existing `LogicalPlan` enum. A matching `PhysicalPlan::Delete` is
added because the engine bind step works off `PhysicalPlan` and we
want to keep the lowering pipeline uniform. Engine execution lives in
a new `delete.rs` module under `bqlite-engine/src/` (so `query.rs`
stays focused on the public surface) and is wired into `bind_physical`.

### 2.3 Predicate classifier shape

The classifier returns one of:

```
DeleteFilter::Cheap(CheapDeleteSpec)
DeleteFilter::AllowScan(TypedExpr)   // when allow_scan = true
```

`CheapDeleteSpec` carries the **decomposed** predicate so the engine
does not re-walk the AST:

```rust
pub struct CheapDeleteSpec {
    /// Entity-key values from the predicate. Plays one of two roles
    /// (see `entity_role`).
    pub entity_keys: Vec<ScalarValue>,
    pub entity_role: EntityRole,        // AsTombstone | AsShardTarget

    /// __seq_id literals from the predicate.
    pub seq_ids: Vec<u64>,
    /// __batch_id literals from the predicate.
    pub batch_ids: Vec<u64>,
    /// Collapsed time-range from one or two ts comparisons / BETWEEN.
    pub time_range: Option<TimeRangeDelete>,
}

pub enum EntityRole {
    /// Entity equality / IN written directly as an entity-level tombstone.
    AsTombstone,
    /// Entity equality / IN paired with __seq_id or __batch_id —
    /// used only to narrow which shards the DELETE writes to.
    AsShardTarget,
}
```

Plan-time classification rejects:
- Top-level `OR`, `NOT`, `!=`.
- Mixed-granularity conjunctions outside the entity+row/batch
  exception.
- Any term outside the SS3 allowlist.

**Invariant on `CheapDeleteSpec`:** at least one tombstone-producing
field must be non-empty:
- `entity_role == AsTombstone` and `entity_keys` non-empty, **or**
- `seq_ids` non-empty, **or**
- `batch_ids` non-empty, **or**
- `time_range` is `Some`.

If `entity_role == AsShardTarget`, `entity_keys` must be non-empty
**and** at least one of `seq_ids` / `batch_ids` must be non-empty
(the shard-target role is meaningless without a paired
row/batch tombstone). The classifier panics on a bug-class violation
(internal invariant) and rejects an empty IN-list at plan time as
`BqliteError::Plan` with a clear message — even though the parser is
likely to reject `IN ()` first, the defensive check keeps the
classifier total.

When the predicate is rejected and `allow_scan == false`, the planner
returns `BqliteError::Plan` with the SS4 message text
("Use ALLOW SCAN at the end of the statement to opt in").

### 2.4 Rows-affected accounting

Per SS11, every DELETE returns an exact `rows_affected: u64`:

| Class | Source | Implementation |
|---|---|---|
| `__seq_id` cheap | `\|seq_ids\|` | Trust the input set per the design table |
| `__batch_id` cheap | `\|batch_ids\|` | Same |
| Entity-level cheap | Per-shard scan of the entity-key column on segments overlapping the affected entities | Use `Database::segment_reader_for_time_range(unbounded)`, project `entity_key_col`, count matches via Arrow filter |
| Time-range cheap | Walk the manifest's segment metadata for affected shards: zone-map fully-inside → `seg.row_count`; boundary segments → scan the timestamp column | Same reader, project `ts_col` only |
| `ALLOW SCAN` | Byproduct of the materialization scan | Already counting `__seq_id`s, the count *is* `rows_affected` |

Counts skip tombstoned rows already in `tombstones.json` snapshot at
DELETE-start (SS8) — re-running an idempotent DELETE produces the
same `rows_affected` only if the tombstone snapshot is identical, but
SS10.2 already documents this caveat for `ALLOW SCAN`. For cheap
classes the count is set-cardinality / row-count metadata, neither
of which depends on the tombstone snapshot.

### 2.5 Idempotent retry (SS10)

The cheap-class write path is **always read-modify-write**:

1. Read existing `tombstones.json` for the shard.
2. Merge the new entries via `TombstoneFile::merge` (which already
   does set-union for row/batch/entity and dedup-append for
   time-range).
3. Atomically write via `write_tombstone_atomic`.

Re-running a DELETE that previously committed is a no-op on the
on-disk tombstone state. `rows_affected` is computed from the same
inputs, so it stays stable across retries (modulo the SS10.2 caveat).

### 2.6 ALLOW SCAN execution path

The engine's ALLOW SCAN path constructs a `ScanPhysical` descriptor
**directly** (rather than running through `plan()`), with
`projected_columns = [entity_key_col, ts_col, __seq_id]` and the
classified predicate as a `scan_predicate`. The `ScanPhysical` doc
on `projected_columns` explicitly carves out this path: "Empty means
'decode every declared column'; the pruning pass replaces the empty
list with the minimal set demanded by downstream operators." A
manually-constructed descriptor with an explicit projection list is
the documented usage when not going through the optimizer; it is not
"overriding" the prune pass — the prune pass simply does not run on a
hand-built descriptor. The Insert ingest path uses the same
"manually-build a physical descriptor for an internal pipeline"
pattern.

Wiring:

1. The planner's `lower_delete` produces a
   `DeletePhysical::AllowScan { predicate: TypedExpr,
   table_name, entity_key_col, ts_col, query_range }`.
2. The engine's `execute_delete_allow_scan` builds a
   `ScanPhysical` with the explicit projection and the predicate
   (compiled from `TypedExpr` via `physical::lower_expr`) as a
   `scan_predicate`. It then binds and drives that scan via the
   normal `bind_physical` → `ScanOperator` path so tombstone-aware
   filtering, pushdown, and zone-map skipping all apply.
3. For each batch: route each row to its shard via
   `partitioner::shard_id_for(entity, shard_count)`, accumulate
   `__seq_id`s per shard, count total matching rows.
4. For every shard with non-empty `__seq_id`s, take the per-shard
   lock from §2.1, RMW the shard's `tombstones.json` (same helper
   as cheap-class).
5. Return the total row count.

The scan honors the manifest snapshot at engine-bind time (SS8) by
construction — `Database::segment_reader_for_time_range` captures the
current manifest, and any concurrent INSERTs that publish later do
not affect the in-flight reader.

---

## 3. Checkpoints

Each checkpoint passes `scripts/local-ci.sh` in isolation, gets a
subagent code review, and is fast-forward merged to `main` before the
next starts.

### Checkpoint 1 — Plan-side: Delete nodes + classifier (planner only)

Files:
- `crates/bqlite-planner/src/logical.rs` — add `LogicalPlan::Delete`,
  `DeleteFilter`, `CheapDeleteSpec`, `EntityRole`, the
  `lower_delete` function and the predicate classifier
  `classify_delete_predicate`. Replace the current "deferred to
  Wave 4" rejection in `lower_statement`.
- `crates/bqlite-planner/src/physical.rs` — add `PhysicalPlan::Delete`
  and `DeletePhysical` (which carries the same `DeleteFilter` plus the
  resolved table name and entity-key/timestamp column names). Extend
  `lower_physical` to translate `LogicalPlan::Delete` to
  `PhysicalPlan::Delete`. Make sure `output_schema()` returns an empty
  schema (DELETE has no row output).
- `crates/bqlite-planner/src/lib.rs` — re-export the new types.
- `crates/bqlite-planner/src/explain.rs` — render the Delete node
  (one-line `DELETE FROM <table> WHERE <classified-summary>`).
- `crates/bqlite-planner/src/opt/{pushdown,prune}.rs` — pass through
  Delete unchanged (it is a leaf with respect to the data plane;
  pushdown/prune do not apply).

Tests in `logical.rs` and `physical.rs`:
- Each cheap-class shape is correctly classified
  (entity =, entity IN, seq_id =, seq_id IN, batch_id =, batch_id IN,
  ts <, ts <=, ts >, ts >=, ts BETWEEN, multi-conjunct same-granularity,
  entity + seq_id mixed, entity + batch_id mixed).
- Each non-cheap shape is rejected without `ALLOW SCAN` and accepted
  as `AllowScan` with it: `OR`, `NOT`, `!=`, function call, arbitrary
  user column predicate, mixed entity + ts.
- Unknown table → `BqliteError::Plan`.
- The reject error message contains the literal "ALLOW SCAN" so users
  see the suggested fix.

Risk: shared-file changes to `lib.rs` re-exports + the explain
module. Bundle them all into this one checkpoint to minimize the
window where downstream agents see partial APIs.

### Checkpoint 2 — Engine cheap-class execution

Files:
- `crates/bqlite-engine/src/delete.rs` (new) — `execute_delete_cheap`
  function. Resolves shards via
  `bqlite_storage::ingest::partitioner::shard_id_for`, RMWs the
  per-shard tombstone file, computes `rows_affected` via the SS11
  count strategy.
- `crates/bqlite-engine/src/bind.rs` — extend `bind_physical` to
  bind `PhysicalPlan::Delete` cheap-class to a new `DeleteOperator`
  in `delete.rs` that wraps the executed result. The operator's
  `next_batch` always returns `None` (DELETE produces no rows).
- `crates/bqlite-engine/src/lib.rs` — add `mod delete;`.
- `crates/bqlite-engine/src/query.rs` — small touch only: documentation
  comment that DELETE flows through the standard bind+drive path.

Tests in `delete.rs` and `bind.rs` (mirroring INSERT VALUES tests):
- `DELETE FROM events WHERE user_id = 'alice'` — entity-level write
  to the targeted shard; only that shard's tombstone file changes;
  `rows_affected` counts alice's rows.
- `DELETE FROM events WHERE user_id IN ('alice', 'bob')` — multiple
  entities, possibly different shards.
- `DELETE FROM events WHERE __seq_id IN (1, 2, 3)` — row-level
  tombstones written to all shards (no entity targeting); count = 3.
- `DELETE FROM events WHERE __batch_id = 0` — batch-level; count
  derived from segment metadata.
- `DELETE FROM events WHERE ts < <literal>` — time-range; verify
  bounds match `TimeRangeDelete`; count via zone-maps.
- `DELETE FROM events WHERE ts BETWEEN a AND b` — collapsed range.
- `DELETE FROM events WHERE user_id = 'alice' AND __seq_id IN (10)` —
  entity narrows shard targeting, only that shard gets the row
  tombstone; count = 1.
- Idempotent retry: run the same DELETE twice, on-disk tombstone
  state is identical after both calls, `rows_affected` matches.
- DELETE that affects zero rows succeeds with `rows_affected = 0`.
- End-to-end via `Engine::query`: insert rows, DELETE, then query
  the table and confirm filtered rows are no longer visible (relies
  on the existing tombstone-aware scan in TASK-434).

### Checkpoint 3 — Engine ALLOW SCAN execution

Files:
- `crates/bqlite-engine/src/delete.rs` — add `execute_delete_allow_scan`
  function. Builds the inner Filter+Scan plan, drives it batch by
  batch, partitions `__seq_id`s by shard, then RMWs each shard's
  tombstone file via the helper from C2.
- `crates/bqlite-engine/src/bind.rs` — extend the Delete bind arm to
  dispatch to `execute_delete_cheap` or `execute_delete_allow_scan`.

Tests:
- `DELETE ... WHERE event_type = 'spam' ALLOW SCAN` — the predicate
  cannot be cheaply classified, ALLOW SCAN forces the scan; only
  matching rows get row-level tombstones.
- `DELETE ... WHERE user_id != 'bot' ALLOW SCAN` — top-level `!=`
  case.
- ALLOW SCAN that matches zero rows: succeeds, no tombstone files
  changed (or empty tombstone files, depending on impl — verify the
  `is_empty` skip path so we do not write empty files).
- End-to-end via `Engine::query`: insert rows, ALLOW SCAN delete,
  query — filtered rows are gone.

Risk lower than C2: only a new code path in one file, no shared API
changes.

---

## 4. Reconciliation against the design doc

| Spec section | Plan covers it via |
|---|---|
| SS3 cheap-class taxonomy | C1 classifier matrix; tested per shape |
| SS3.1 same-granularity collapse | C1 (entity IN-list collapse, time-range two-bound collapse) |
| SS3.2 cross-granularity rejection + entity+row/batch exception | C1 entity-role split |
| SS3.3 shard targeting | C2 + C3 via `shard_id_for` |
| SS4 default-reject + ALLOW SCAN opt-in | C1 reject path + C3 allow-scan execution |
| SS5 schema | Already implemented in TASK-432; reused |
| SS6 per-query snapshot | Out of scope (TASK-434 owns scan-time consumption) |
| SS7 scan-time filter order | Already in TASK-434's `TombstoneScanWrapper` |
| SS8 DELETE-vs-INSERT manifest snapshot | Inherited from `Database::segment_reader_for_time_range` semantics |
| SS9 per-shard mutex | Trivially satisfied by `&mut Database` borrow; documented |
| SS10 idempotent retry | C2 RMW-merge path; tested |
| SS10.2 ALLOW SCAN idempotence caveat | Documented in `delete.rs` module comment |
| SS11 exact `rows_affected` | C2 + C3 per-class strategy |
| SS11.2 zero-match | Tested in C2 + C3 |
| SS12 compaction reclamation | Out of scope (TASK-435 owns compaction) |
| SS13 no warning channel | Documented; nothing to do |

No spec changes anticipated. If the reviewer flags a gap, surface
via `[NEEDS INPUT]` rather than guessing.

---

## 5. Risk register

1. **`rows_affected` for entity-level deletes is more expensive than
   the design doc implies.** SS11 says "Per-shard entity-key column
   scan across affected shards; reads entity-key column only
   (contiguous due to entity-sorted layout)". To do this exactly we
   need to read the entity_key column from segments via the existing
   scan pipeline. The first-cut implementation will use
   `Database::segment_reader_for_time_range(unbounded)`, project the
   entity-key column, and count matching rows. This is a column-only
   read, so it matches the design intent, but it is a real scan —
   not a metadata read. Performance is acceptable for Wave 4 because
   the read is column-only and entity-sorted.

2. **Shard targeting for entity IN-list with ints vs strings.** The
   `shard_id_for` function accepts `EntityId` (String or Int). The
   classifier carries `ScalarValue` and the table schema carries the
   entity-key type. The conversion is mechanical; tested per shape.

3. **`__seq_id IN` with literals** — the classifier accepts only
   integer literals. Negative literals are rejected (seq_ids are
   `u64`). Tested.

4. **Empty tombstone write.** If the cheap-class spec produces an
   empty TombstoneFile after merge (unusual but possible if the
   existing on-disk file already covered everything), skip the write
   so we do not churn fsyncs. Same for ALLOW SCAN with zero matches.

5. **Cross-shard atomicity (SS10).** Per-shard atomic only;
   acknowledged by the design. Document the partial-state behavior in
   the module comment and rely on the idempotent retry contract.

6. **TimeRangeDelete dedup at write time (SS5.1).** Verified:
   `TombstoneFile::merge` in `tombstone.rs` lines 110–120 already
   does exact-bound dedup on append. The DELETE writer reuses
   `merge` rather than `time_range_deletes.push(...)` so the dedup
   guarantee holds for repeated idempotent retries.

7. **Cross-link TASK-456 (SS8.2).** The `delete.rs` module comment
   includes a forward pointer to TASK-456, the user-facing
   documentation task, so the SS8.2 documentation requirement
   surfaces in the right place.

---

## 6. Verification

For each checkpoint:
1. `scripts/local-ci.sh` end-to-end (fmt, dep-direction, clippy, build,
   test).
2. Subagent code review on the staged diff.
3. Re-read `deletes.md` to confirm the implementation matches the
   spec sections claimed in §4 of this plan.
4. `git merge task/TASK-453 --ff-only` into `main`; push.
