# Tombstone and Delete Semantics

**Wave**: 4
**Task**: TASK-404
**Status**: draft
**Depends on**: TASK-403 (compaction concurrency protocol)
**Depended on by**: TASK-432 (tombstone file storage), TASK-433 (DELETE parser), TASK-434 (tombstone-aware scan), TASK-435 (tombstone reclamation), TASK-453 (DELETE planner + engine)

---

## 1. Scope

This document freezes the complete semantics for bqlite's DELETE
statement and its storage-layer tombstone representation: which
predicates are recognized cheaply, how a DELETE maps to shard-local
tombstone files, visibility rules for concurrent queries, scan-time
filtering order, compaction-time reclamation, and error behavior for
deletes that require a full scan.

It does **not** cover:

- The tombstone file I/O implementation (atomic read/write helpers,
  shard targeting) -- TASK-432 scope.
- DELETE statement parsing -- TASK-433 scope.
- Scan-time tombstone application (operator integration, merge path) --
  TASK-434 scope.
- Compaction-time physical row reclamation -- TASK-435 scope.
- DELETE planner lowering and engine execution -- TASK-453 scope.

### 1.1 Relationship to Existing Design Docs

This document refines and supersedes the tombstone-related sketches in
[storage-format.md](../storage-format.md) and the DELETE surface syntax
in [query-language.md](../query-language.md):

| Source | Section | This document | Change |
|---|---|---|---|
| storage-format.md | SS7.5 (tombstone file schema) | SS5 (tombstone file schema) | Elevates `time_range_delete` from `Option<TimeRangeDelete>` to `Vec<TimeRangeDelete>` to support multiple independent time-range deletes |
| storage-format.md | SS7.5 (tombstone lifecycle) | SS6, SS7, SS8, SS10, SS12 | Expands lifecycle into per-query snapshots, concurrent-delete serialization, crash atomicity, and compaction reclamation ordering |
| query-language.md | SS20.2 (DELETE) | SS3, SS4 | Formalizes the cheap-class predicate taxonomy and `ALLOW SCAN` opt-in semantics |
| compaction-concurrency.md | SS9 (tombstone interaction) | SS12 | Adopts tombstone-snapshot-at-job-start and manifest-first reclamation ordering verbatim |

Where this document and the source docs conflict, this document wins.
Source docs are updated in the same checkpoint to add forward-references.

---

## 2. Tombstone Granularities

Four granularities, matching [storage-format.md](../storage-format.md)
SS7.5:

| Granularity | Tombstone field | Use case | Example |
|---|---|---|---|
| Row-level | `__seq_id` | Delete specific events | `DELETE FROM events WHERE __seq_id IN (123, 456)` |
| Batch-level | `__batch_id` | Undo a bad ingest | `DELETE FROM events WHERE __batch_id = 42` |
| Entity-level | entity key column | GDPR right-to-erasure | `DELETE FROM events WHERE user_id = 'alice'` |
| Time-range | `min_ts` / `max_ts` + inclusivity | Retention cutoff, rolling window drop | `DELETE FROM events WHERE ts < '2024-01-01'` |

**Entity-level deletes are data, not rules.** An entity-level tombstone
suppresses existing rows for that entity. If the entity is re-ingested
after the delete, the new rows are *not* affected -- the tombstone
applies only to data present at the time the tombstone is written. This
is enforced by the compaction reclamation lifecycle: once compaction
physically removes the tombstoned rows and clears the tombstone entry,
re-ingested data for the same entity lands in fresh segments with no
tombstone covering them.

---

## 3. Cheap-Class Predicate Taxonomy

A DELETE predicate is **cheap** -- routed directly to tombstone writes
with no data scan -- iff its top-level form is a conjunction (`AND`-chain)
of terms drawn exclusively from this allowlist:

| Term shape | Maps to granularity |
|---|---|
| `<entity_key_col> = <literal>` | Entity-level |
| `<entity_key_col> IN (<literal>, ...)` | Entity-level |
| `__seq_id = <literal>` | Row-level |
| `__seq_id IN (<literal>, ...)` | Row-level |
| `__batch_id = <literal>` | Batch-level |
| `__batch_id IN (<literal>, ...)` | Batch-level |
| `<event_time_col> < <literal>` | Time-range |
| `<event_time_col> <= <literal>` | Time-range |
| `<event_time_col> > <literal>` | Time-range |
| `<event_time_col> >= <literal>` | Time-range |
| `<event_time_col> BETWEEN <literal> AND <literal>` | Time-range |

**Not cheap (full-scan class):** any predicate containing top-level `OR`,
`NOT`, `!=`, arbitrary expressions on user-defined columns, functions,
subqueries, or any term not in the allowlist above.

### 3.1 Same-Granularity Conjunctions

A cheap-class conjunction may combine multiple terms of the **same**
granularity:

```bql
-- Cheap: two time-range terms → single TimeRangeDelete with both bounds
DELETE FROM events WHERE ts >= '2024-01-01' AND ts < '2025-01-01'

-- Cheap: entity IN-list (single granularity)
DELETE FROM events WHERE user_id IN ('alice', 'bob')
```

Multiple time-range terms are collapsed into a single `TimeRangeDelete`
with both bounds set. Multiple entity-key terms in a single `AND` are
collapsed into a single `IN`-list.

### 3.2 Cross-Granularity Conjunctions

A conjunction that mixes terms from **different** granularities is **not
cheap** because the tombstone format cannot represent the intersection.
Each tombstone entry operates independently at scan time (OR semantics),
so decomposing a conjunction into per-granularity entries would delete
more rows than the predicate specifies:

```bql
-- NOT cheap: entity + time-range cross-granularity conjunction.
-- Decomposing into an entity tombstone for 'alice' PLUS a time-range
-- tombstone for ts < '2024-01-01' would delete ALL of alice's rows
-- AND all rows before 2024 for every entity — far broader than intended.
DELETE FROM events WHERE user_id = 'alice' AND ts < '2024-01-01'
-- Error: predicate crosses tombstone granularities. Use ALLOW SCAN.
```

The exception is entity-key terms combined with `__seq_id` or
`__batch_id` terms: the entity predicate is used only for **shard
targeting** (narrowing which shards to write to), while the row/batch
IDs are the actual tombstone entries. The entity term is not written as a
separate entity-level tombstone:

```bql
-- Cheap: entity narrows shard targeting, __seq_id is the tombstone
DELETE FROM events WHERE user_id = 'alice' AND __seq_id IN (100, 200)
-- Writes row-level tombstones {100, 200} to alice's shard only
```

### 3.3 Shard Targeting

When the predicate includes an entity-key equality or IN-list, the
planner resolves the target shards via `xxhash64(entity_id) % num_shards`
and writes tombstones only to those shards. When no entity predicate is
present (e.g., a pure time-range delete), tombstones are written to
*all* shards.

---

## 4. Full-Scan DELETE and ALLOW SCAN

A DELETE whose predicate is not in the cheap class is **rejected at plan
time** by default. The planner emits a hard error:

```
Error: DELETE predicate is not in the cheap class and would require a
full table scan. Use ALLOW SCAN at the end of the statement to opt in:

  DELETE FROM events WHERE user_id != 'bot' ALLOW SCAN
```

If the predicate is reformulable as a cheap-class expression, the error
message suggests the reformulation when one is obvious (e.g., `!= 'bot'`
cannot be reformulated cheaply, but `status = 'inactive'` on a
user-defined column cannot either -- no suggestion in that case).

### 4.1 ALLOW SCAN Execution

With `ALLOW SCAN`, the engine:

1. Runs a scan over all segments in the table, applying the predicate.
2. Materializes the `__seq_id` of every matching row.
3. Writes the materialized `__seq_id` set as row-level tombstones to
   each affected shard's tombstone file.

The scan respects the manifest snapshot at DELETE start (SS8). Rows
from in-flight INSERTs that have not yet published are invisible to the
scan and will not be tombstoned.

### 4.2 Why Default-Reject

The default path must be safe from accidental large deletes. A predicate
like `WHERE user_id != 'bot'` can silently wipe the table. Requiring
explicit `ALLOW SCAN` matches the analytics-DB convention of opt-in for
expensive DML and ensures the user acknowledges the cost.

---

## 5. Tombstone File Schema

Each shard has one tombstone file at
`<table>/windows/<window>/<shard>/tombstones.json`. The file is a
complete snapshot of the shard's active tombstones, updated atomically
via write + rename (same pattern as manifest updates).

```rust
/// Serialized as JSON. Updated atomically via write + rename.
#[derive(Serialize, Deserialize)]
pub struct TombstoneFile {
    /// Entity-level deletes: all events for these entities are deleted.
    pub entity_deletes: HashSet<ScalarValue>,

    /// Row-level deletes: specific sequence IDs.
    pub row_deletes: HashSet<u64>,

    /// Batch-level deletes: specific batch IDs.
    pub batch_deletes: HashSet<u64>,

    /// Time-range deletes: all events whose timestamps fall within any
    /// of the configured bounds are dropped. Multiple ranges support
    /// independent time-range DELETE operations (e.g., a retention
    /// cutoff plus a separate window drop).
    pub time_range_deletes: Vec<TimeRangeDelete>,
}

#[derive(Serialize, Deserialize)]
pub struct TimeRangeDelete {
    /// Lower bound (nanoseconds since epoch). None = unbounded below.
    pub min_ts: Option<i64>,
    pub min_inclusive: bool,

    /// Upper bound (nanoseconds since epoch). None = unbounded above.
    pub max_ts: Option<i64>,
    pub max_inclusive: bool,
}
```

### 5.1 Schema Changes from storage-format.md SS7.5

The `time_range_delete` field is promoted from `Option<TimeRangeDelete>`
to `time_range_deletes: Vec<TimeRangeDelete>`. A single optional range
cannot represent two independent time-range deletes (e.g., `ts < '2023-01-01'`
followed by `ts BETWEEN '2024-06-01' AND '2024-07-01'`). The Vec allows
each DELETE to append its range without merging with prior ranges.

**Deduplication at write time:** When appending a new `TimeRangeDelete`,
the DELETE write path checks for an exact-match duplicate (same bounds
and inclusivity) and skips the append if one exists. This is a cheap
O(n) scan over the expected-small Vec (1-3 entries) and prevents
unbounded growth from repeated idempotent retries (SS10). General
merging of overlapping ranges is deferred to compaction-time tombstone
rewrite (SS12.5).

### 5.2 Scan-Time Evaluation of Multiple Time Ranges

A row is tombstoned if its timestamp falls within **any** range in the
`time_range_deletes` Vec. The scan path evaluates ranges in order and
short-circuits on the first match. For the expected case (1-3 ranges),
linear scan is optimal; no index structure is needed.

---

## 6. Per-Query Tombstone Snapshot

The engine loads tombstone files for every `(window, shard)` the
physical plan will touch **once at query bind time** and shares the
snapshot across every scan operator in that query.

### 6.1 Snapshot Consistency

All scan operators within a single query observe the same tombstone
state. This prevents "same query, different answers across sub-scans"
anomalies. Joined-source queries (TASK-436) observe a single coherent
tombstone epoch across all input tables.

### 6.2 Relationship to Manifest Snapshot

The tombstone snapshot is taken alongside the `Arc<Manifest>` snapshot
at query bind time. Together they define the complete visibility state
for the query:

- **Manifest snapshot:** which segments exist.
- **Tombstone snapshot:** which rows within those segments are logically
  deleted.

Both are immutable for the lifetime of the query. DELETEs and compactions
that complete after bind time are invisible to the running query.

### 6.3 Implementation Constraint for TASK-434

The tombstone-aware scan path must receive its snapshot from the query
context (engine bind step), not re-read from disk. This is a hard
requirement -- re-reading would break snapshot consistency for queries
with multiple scan operators.

---

## 7. Scan-Time Filtering Order

Tombstone checks are applied in the read path at a specific point in
the scan pipeline:

```
1. Column projection (read only needed columns from segment)
2. Zone-map / predicate pushdown (skip row groups)
3. Dictionary-aware filtering (if applicable)
4. Tombstone filtering  ← here
5. Rows delivered to operators
```

**Why after pushdown, before operators:** Pushdown reduces the row set
cheaply using segment metadata. Tombstone checks are per-row operations
(HashSet lookups + timestamp comparisons) and should only run on rows
that survive pushdown. Operators must never see tombstoned rows --
tombstones are transparent to the operator layer.

### 7.1 Tombstone Check Order

Within tombstone filtering, checks are applied in this order for
early-exit efficiency:

1. **Batch-level:** `batch_deletes.contains(&row.__batch_id)` -- cheapest
   check, highest expected hit rate for bulk undo operations.
2. **Entity-level:** `entity_deletes.contains(&row.entity_key)` --
   second cheapest, common for GDPR.
3. **Row-level:** `row_deletes.contains(&row.__seq_id)` -- HashSet
   lookup, variable hit rate.
4. **Time-range:** iterate `time_range_deletes`, check
   `min_ts..max_ts` bounds -- bounded comparison, expected to be rare
   (1-3 ranges).

A row is suppressed if **any** check matches. The order is a performance
hint, not a semantic requirement -- any ordering produces the same
result.

---

## 8. DELETE vs. In-Flight INSERT

DELETE operates on the **manifest-visible set at DELETE-start**. Rows
from an INSERT batch that has begun but not yet published via a manifest
update are invisible to the DELETE and will land alive in the table.

### 8.1 User-Facing Consequence

A user scripting "ingest then immediately GDPR-delete" must sequence
explicitly: wait for the INSERT to return before issuing the DELETE.
This is consistent with (a) the "tombstones are data, not rules"
principle and (b) the per-query snapshot model.

### 8.2 Documentation Requirement

This behavior must be documented in user-facing DELETE documentation
(TASK-456 scope): "DELETE applies to data that is visible at the time
the DELETE statement begins. If an INSERT is in progress, its rows will
not be affected. Wait for the INSERT to complete before issuing a DELETE
to ensure all rows are covered."

---

## 9. Concurrent DELETEs on the Same Shard

Serialized by an **in-process per-shard `Mutex<()>`** held for the
duration of the read-modify-write on that shard's `tombstones.json`.

- DELETEs to different shards proceed in parallel.
- Queries do not take this lock -- they open the tombstone file normally
  and the per-query snapshot (SS6) isolates them from concurrent writes.

### 9.1 Lock Scope

The mutex protects only the tombstone file read-modify-write cycle:

```
1. Lock per-shard mutex.
2. Read current tombstones.json.
3. Merge new tombstone entries into the in-memory set.
4. Write tombstones.json.tmp; fsync.
5. rename(tombstones.json.tmp, tombstones.json).
6. Unlock per-shard mutex.
```

Queries never contend on this lock. The lock hold time is bounded by a
single file read + write + rename, which is milliseconds.

### 9.2 Single-Process Scope

The database is a single-process embedded writer today. If multi-process
writer support is added in the future, this in-process mutex must be
promoted to `flock`/`fcntl` on the tombstone file. That promotion is a
follow-up task at that point; `flock` is not added speculatively now.

---

## 10. Cross-Shard Crash Atomicity

A DELETE that touches multiple shards is **per-shard atomic only**. The
engine walks shards in some deterministic order, fsyncs each
`tombstones.json` write, and returns success once all shards have
committed. If the process crashes mid-DELETE, partial state is visible:
some shards tombstoned, others not.

### 10.1 Recovery: Idempotent Retry

The caller re-runs the DELETE. DELETE is a documented **idempotent
contract** -- re-running the same predicate converges to the same final
state over the tombstone set.

**Why idempotent:** Tombstones are set-based. `HashSet::insert` of an
already-present `__seq_id` or `entity_id` is a no-op. Time-range
entries that duplicate an existing range are harmless (they match the
same rows). A cross-shard WAL is heavyweight for a case the idempotence
contract already covers.

### 10.2 ALLOW SCAN Idempotence Caveat

Cheap-class DELETEs are trivially idempotent over the tombstone set.
`ALLOW SCAN` DELETEs are idempotent over the tombstone set but may
tombstone **additional** rows on retry if new data matching the predicate
has been ingested between runs. This is intended behavior, not a bug,
and must be documented user-facing.

---

## 11. DELETE Return Value

DELETE returns an **exact `rows_affected: u64` count**, always.

| Predicate class | Count source | Cost |
|---|---|---|
| `__seq_id` / `__batch_id` cheap-class | `\|input_set\|` | Free (known from the predicate literal) |
| Entity-level cheap-class | Per-shard entity-key column scan across affected shards | Bounded by number of affected shards; reads entity-key column only (contiguous due to entity-sorted layout) |
| Time-range cheap-class | Per-shard row-group metadata scan (zone maps + row counts) | Bounded by number of row groups; row-groups fully inside the range contribute exact counts, boundary row-groups require timestamp column scan |
| `ALLOW SCAN` | Materialization scan | Free (count is a byproduct of the scan that already ran) |

### 11.1 Why Exact

SQL convention is load-bearing: tooling and humans rely on
`rows_affected` to verify that a DELETE did what they expected.

- **Entity-level:** The entity-sorted storage layout means all rows for
  a given entity are contiguous within a shard. Counting them requires
  scanning the entity-key column (not the full row data) to find the
  entity's row range. This is a column-only scan, not a full-row scan.
- **Time-range:** Row-group zone maps on the timestamp column identify
  row groups fully inside, fully outside, or straddling the range.
  Fully-inside row groups contribute their row count directly from
  metadata. Boundary row groups require a timestamp column scan to get
  exact counts.

### 11.2 Zero-Match DELETE

A DELETE whose predicate matches no rows succeeds silently with
`rows_affected = 0`. This is required by the SQL convention and by the
idempotence contract in SS10. No error, no warning.

---

## 12. Compaction-Time Reclamation

Compaction physically removes tombstoned rows from segment data and
clears the resolved tombstone entries. The ordering follows
[compaction-concurrency.md](compaction-concurrency.md) SS9:

### 12.1 Tombstone Snapshot at Job Start

Compaction snapshots `tombstones.json` at job start and uses that
snapshot for filtering throughout the job. DELETEs issued mid-compaction
write a new tombstone file; the new file applies to subsequent queries
but does not affect the in-flight compaction's output.

### 12.2 Manifest-First Reclamation Ordering

After the 5-step manifest publication protocol
([compaction-concurrency.md](compaction-concurrency.md) SS6), tombstone
cleanup follows:

```
Steps 1-5: Manifest publication (write segments, publish manifest, Arc swap)
Step 6: Write new tombstones.json.tmp with reclaimed entries removed; fsync.
Step 7: rename(tombstones.json.tmp, tombstones.json).
Step 8: Schedule old-segment deletion.
```

**Manifest-first is a correctness invariant.** If tombstones were
cleared before the new manifest was published and the process crashed in
between, the old segments would still be referenced by the manifest but
their tombstones would be gone -- readers would see previously-deleted
rows. This ordering is never reordered.

### 12.3 Stale Tombstone Safety

If the process crashes between step 5 (manifest published) and step 7
(tombstone rewrite), the new manifest is live but the tombstone file
still lists entries that the new segment has already physically removed.
This is **harmless**: the stale tombstones filter rows that are not in
the new segment, so they are no-ops. The next compaction of the same
`(window, shard)` will rewrite the tombstone file.

### 12.4 What Gets Reclaimed

Since compaction merges *all* segments within a `(window, shard)`, a
single compaction pass can resolve every tombstone for that scope:

- **Row-level (`__seq_id`):** Removed from `row_deletes` if the
  `__seq_id` was present in a compacted input segment.
- **Batch-level (`__batch_id`):** Removed from `batch_deletes` if no
  remaining segment in the `(window, shard)` contains any row with that
  `__batch_id`.
- **Entity-level:** Removed from `entity_deletes` if no remaining
  segment contains any row for that entity.
- **Time-range:** Removed from `time_range_deletes` if no remaining
  segment contains any row whose timestamp falls within the range.

### 12.5 Time-Range Merge During Reclamation

During the tombstone rewrite in step 6, overlapping or adjacent
`TimeRangeDelete` entries *may* be merged into a single entry. This is
an optional optimization that keeps the Vec from growing unboundedly
across many retention-cutoff deletes. The merge is semantics-preserving
(the union of the original ranges equals the merged range).

---

## 13. Warning Channel

**No DML warning channel in Wave 4.** Cheap-class DELETEs return only
the `rows_affected` count. Dangerous predicate shapes are already
rejected at plan time by the `ALLOW SCAN` requirement (SS4).

If a general statement-warning channel lands later (e.g., SELECT
"scanned N segments" warnings), DELETE adopts it at that point. This is
not a Wave 4 deliverable.

---

## 14. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Tombstone granularities | Row (`__seq_id`), batch (`__batch_id`), entity (key column), time-range (`min_ts`/`max_ts` + inclusivity) | Covers the four canonical delete patterns |
| Cheap-class taxonomy | Same-granularity AND-chain of entity key `=`/`IN`, `__seq_id`/`__batch_id` `=`/`IN`, time comparisons; cross-granularity conjunctions are full-scan class | Tombstone entries use OR semantics; cross-granularity decomposition would delete more rows than intended |
| Non-cheap default | Reject at plan time | Safe from accidental large deletes |
| Non-cheap opt-in | `ALLOW SCAN` suffix | Explicit acknowledgment of scan cost |
| Time-range schema | `Vec<TimeRangeDelete>` with `min_ts`/`max_ts` + inclusivity | Supports multiple independent time-range deletes |
| Snapshot granularity | Per-query, loaded at bind time | No within-query anomalies |
| Scan-time filter order | After pushdown, before operators; batch > entity > row > time-range | Cheapest checks first; operators never see deleted rows |
| DELETE vs. in-flight INSERT | DELETE sees manifest-visible set at DELETE-start only | Consistent with tombstones-as-data and per-query snapshots |
| Concurrent DELETE serialization | Per-shard in-process `Mutex<()>` | Simple; writes are rare and short |
| Cross-shard crash atomicity | Per-shard atomic; idempotent retry, no WAL | Set-based tombstones make re-run a no-op |
| Return value | Exact `rows_affected: u64`, always | SQL convention; tooling relies on it |
| Zero-match DELETE | `rows_affected = 0`, success | SQL convention; idempotence contract |
| Compaction reclamation | Snapshot at job start; manifest-first ordering | Crash-safe; stale tombstones are harmless no-ops |
| Warning channel | None in Wave 4 | Dangerous shapes already gated by `ALLOW SCAN` |
| Multi-process writers | Not supported; in-process mutex only | Single-process embedded DB today; `flock` deferred |

---

## 15. Follow-On Implications

These cross-references document which downstream tasks consume decisions
from this design:

- **TASK-432 (tombstone file storage)** -- implements the
  `TombstoneFile` / `TimeRangeDelete` schema from SS5, the atomic
  read/write protocol from SS9.1, and the per-shard mutex from SS9.
  Must model `time_range_deletes` as `Vec<TimeRangeDelete>`, not
  `Option<TimeRangeDelete>`.
- **TASK-433 (DELETE parser)** -- parses the `ALLOW SCAN` suffix per
  SS4 and attaches it to the DELETE AST node. The cheap-class taxonomy
  (SS3) is a planner concern, not a parser concern -- the parser accepts
  any predicate expression.
- **TASK-434 (tombstone-aware scan)** -- consumes the per-query snapshot
  from the query context per SS6.3. Implements the scan-time filtering
  order from SS7. Must never re-read tombstone files from disk during
  query execution.
- **TASK-435 (tombstone reclamation during compaction)** -- implemented
  by `compact_one` and `reclaim_tombstones_after_compaction` in
  `crates/bqlite-storage/src/compaction.rs`, using the
  `CompactionTombstoneScan` wrapper in
  `crates/bqlite-storage/src/tombstone_scan.rs`. Manifest-first
  reclamation order (SS12.2) and per-granularity reclamation rules
  (SS12.4) are covered; optional time-range merge (SS12.5) is deferred
  to a follow-on task once bench data justifies it.
- **TASK-453 (DELETE planner + engine)** -- enforces cheap-class
  rejection (SS4), implements exact `rows_affected` (SS11), owns the
  per-shard mutex lifecycle (SS9), and the idempotent retry contract
  (SS10). Must document the `ALLOW SCAN` idempotence caveat (SS10.2)
  and the DELETE-vs-INSERT sequencing requirement (SS8.2).
- **storage-format.md SS7.5** -- must be updated to reference this
  document and to change `time_range_delete: Option<TimeRangeDelete>`
  to `time_range_deletes: Vec<TimeRangeDelete>`.
- **query-language.md SS20.2** -- already reflects the `ALLOW SCAN`
  syntax and idempotence note; no further updates needed.
