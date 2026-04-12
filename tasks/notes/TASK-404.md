# TASK-404 — Tombstone and delete semantics

Human-assisted semantics decisions for `docs/design/storage/deletes.md`. These decisions are authoritative and override conflicting guesses drawn from `TASKS.md`, `storage-format.md` §7.5, or `query-language.md` §20.2. Reconcile those docs in the same checkpoint as any code change that contradicts them.

## Already pinned by existing docs (not re-litigated here)

- Per-shard `tombstones.json`, write+rename atomicity, JSON format per `storage-format.md` §7.5.
- Four granularities: row (`__seq_id`), batch (`__batch_id`), entity key, time-range.
- Reclaimed during compaction of the owning `(window, shard)`.
- Entity-level delete does not persist across re-ingestion — tombstones are data, not rules.
- No UPDATE.
- Tombstone snapshot is loaded at scan setup (refined below as per-query).
- DELETE is visible immediately to new queries; segments are not rewritten until compaction.

## Decisions

### 1. Cheap-class predicate taxonomy

A DELETE predicate is "cheap" — routed directly to tombstone writes with no data scan — iff its top-level is a conjunction (`AND`-chain, no top-level `OR`, no `NOT`) of terms drawn from this allowlist:

- `<entity_key_col> = <literal>`
- `<entity_key_col> IN (<literal>, ...)`
- `__seq_id = <literal>` or `__seq_id IN (<literal>, ...)`
- `__batch_id = <literal>` or `__batch_id IN (<literal>, ...)`
- Time-range terms: `ts < X`, `ts <= X`, `ts > X`, `ts >= X`, `ts BETWEEN a AND b` (see §3)

Anything else — top-level OR, arbitrary expressions on user columns, functions, `!=`, subqueries — is **full-scan class**.

### 2. Full-scan DELETE requires explicit opt-in

A DELETE whose predicate is not in the cheap class is **rejected at plan time** by default. The user opts in with the keyword `ALLOW SCAN` at the end of the statement:

```bql
DELETE FROM events WHERE user_id != 'bot' ALLOW SCAN
```

Without `ALLOW SCAN`, the planner emits a hard error explaining why the predicate is non-cheap and suggesting the cheap-class reformulation if one is obvious. With `ALLOW SCAN`, the engine runs a scan, materializes matching `__seq_id`s, and writes them as row-level tombstones.

**Why:** the default path should be safe from accidental large deletes (`WHERE user_id != 'bot'` wiping the table); users who genuinely need arbitrary deletes declare intent explicitly. Matches the analytics-DB convention of explicit opt-in for expensive DML.

### 3. Time-range tombstone schema extension

Extend the tombstone file's time-range representation beyond today's single `max_ts` field to a `min_ts` / `max_ts` pair with inclusivity flags (or equivalent encoding). The cheap class accepts `ts </<=/>/>=` against a literal and `ts BETWEEN a AND b` without falling into `ALLOW SCAN`.

**Why:** time-based cleanup (retention cutoff, rolling window drop) is one of the two canonical DELETE shapes alongside entity-level GDPR. A one-nanosecond rewrite away from `ts < X` shouldn't require `ALLOW SCAN`.

**Implication for TASK-432:** tombstone file schema and loader must model both bounds plus inclusivity. Update `storage-format.md` §7.5 in the same checkpoint.

### 4. Snapshot granularity — per-query

The engine loads tombstone files for every `(window, shard)` the physical plan will touch **once at query bind time** and shares the snapshot across every scan op in that query. Joined-source queries (TASK-436) observe a single coherent tombstone epoch across all input tables.

**Why:** "same query, different answers across sub-scans" is a class of bug we don't want. Matches the manifest-snapshot-per-query model already implied by `storage-format.md` §7.6. Tombstone files are small; loading them once at bind time is cheap.

**Implication for TASK-434:** the tombstone-aware scan path must receive its snapshot from the query context, not re-read from disk.

### 5. DELETE vs. in-flight INSERT

DELETE operates on the **manifest-visible set at DELETE-start**. Rows from an INSERT batch that has begun but not yet published via a manifest update are invisible to the DELETE and will land alive in the table.

**Why:** only choice consistent with (a) the already-locked "tombstones are data, not rules" principle and (b) the per-query snapshot model. A user scripting "ingest then immediately GDPR-delete" must sequence explicitly — wait for the INSERT to return before issuing the DELETE. This must be documented in the user-facing DELETE docs.

### 6. Concurrent DELETEs on the same shard

Serialized by an **in-process per-shard `Mutex<()>`** held for the duration of the read-modify-write on that shard's `tombstones.json`. DELETEs to different shards proceed in parallel. Queries do not take this lock — they open the file normally and the per-query snapshot isolates them.

**Why:** tombstone writes are rare, short, and the contention pattern doesn't warrant optimistic CAS. Simple, matches the per-shard locality goal.

**Scope caveat:** the database is a single-process embedded writer today. If we ever support multi-process writers, this in-process mutex must be promoted to `flock`/`fcntl` on the tombstone file. File a follow-up task at that point; do not add flock speculatively now.

### 7. Cross-shard crash atomicity — idempotent retry, no WAL

A DELETE that touches multiple shards is **per-shard atomic only**. The engine walks shards in some order, fsyncs each `tombstones.json` write, and returns success once all shards have committed. If the process crashes mid-DELETE, partial state is visible (some shards tombstoned, others not).

Recovery: the caller re-runs the DELETE. DELETE is a documented **idempotent contract** — re-running the same predicate converges to the same final state over the tombstone set.

**Why:** tombstones are set-based (HashSet-of-seq_ids, HashSet-of-entity_ids, time bounds); applying the same DELETE twice is a no-op at the file level. A cross-shard WAL is heavyweight for a case the idempotence contract already covers.

**Idempotence caveat (must be documented user-facing):** cheap-class DELETEs are trivially idempotent over the tombstone set. `ALLOW SCAN` DELETEs are idempotent over the tombstone set but may tombstone **additional** rows on retry if new data matching the predicate has been ingested between runs. This is intended behavior, not a bug.

### 8. DELETE return value

DELETE returns an **exact `rows_affected: u64` count**, always.

- `__seq_id` / `__batch_id` cheap-class: count is `|input_set|` (free).
- Entity-level cheap-class: count comes from per-shard bloom + column metadata scan across affected shards.
- Time-range cheap-class: count comes from segment manifest metadata.
- `ALLOW SCAN`: count comes from the materialization scan (free).

**Why:** SQL convention is load-bearing; tooling and humans rely on it. The metadata cost for entity and time-range counts is bounded, small, and uses infrastructure that already exists.

### 9. Warning channel

**No DML warning channel in Wave 4.** Cheap-class DELETEs return only the rows-affected count; dangerous shapes are already rejected at plan time by the `ALLOW SCAN` requirement. If a general statement-warning channel lands later (e.g. SELECT "scanned N segments" warnings), DELETE adopts it at that point.

### 10. Zero-match DELETE

A DELETE whose predicate matches no rows succeeds silently with `rows_affected = 0`. Required by the SQL convention and by the idempotence contract in §7.

## Follow-on implications to propagate

These are not decisions, just consequences worth calling out for downstream tasks:

- **TASK-432 (tombstone file storage)** — schema must model time-range bounds with inclusivity per §3.
- **TASK-433 (DELETE parser)** — must accept the `ALLOW SCAN` suffix and attach it to the DELETE AST node.
- **TASK-434 (tombstone-aware scan)** — consumes the per-query snapshot from the query context per §4.
- **TASK-453 (DELETE planner + engine)** — enforces §2 (reject non-cheap without `ALLOW SCAN`), implements §8 (exact count), owns the per-shard mutex from §6 and the idempotent retry contract from §7.
- **`storage-format.md` §7.5 / `query-language.md` §20.2** — must be updated in the same checkpoints as the code changes to reflect §3 schema, `ALLOW SCAN` syntax, idempotence contract, and per-query snapshot statement.
