# Spill-to-Disk Protocol

> **Status**: draft (TASK-502, Wave 5).
> **Owns**: the v1 spill surface — which structures spill, the on-disk
> file layout and naming, the spill-root directory and its lifecycle,
> the cleanup ordering on success / cancel / timeout / panic / crash,
> the operator-to-budget participation protocol, and the per-operator
> fail-fast list. Freezes the cross-doc conflict between
> `execution-model.md` § 10.3 (sort + IN-subquery spill, aggregate no
> spill) and the newer `engine/memory-budget.md` § 7 (cohort policy
> deferred to TASK-502).
> **Depended on by**: TASK-512 (ingest partitioner external spill),
> TASK-513 (sort spill runs + on-disk merge), TASK-514 (cohort
> materialization memory enforcement), TASK-525 (memory-pressure /
> spill / cancellation stress suite).
> **Reconciles**: `execution-model.md` § 10.3, `engine/memory-budget.md`
> § 5 / § 7, `engine/cancellation.md` § 5.2 / § 5.3,
> `operators/sort-distinct.md`, `operators/aggregate-operator.md`,
> `language/cohorts-aliases-joins.md` § 2.7 / § 7,
> `storage-format.md` § 13 / § 14.1.

---

## 1. Scope

This note is the single source of truth for everything about
**how query-time intermediate state moves to disk and back**. Concretely:

- Which operators spill in v1 and which fail fast (§ 3, § 4).
- How the spill root directory is configured, created, and reclaimed
  across process lifetimes (§ 5).
- The on-disk path scheme and per-purpose file format for each
  spill source (§ 6, § 7).
- The cleanup contract on every query exit path — success,
  cancellation, timeout, operator panic, host crash — and how it
  composes with the RAII guards introduced in
  `engine/cancellation.md` (§ 8, § 9).
- The operator-to-budget protocol: when an operator registers a spill
  handler, what the handler does on `try_reserve` failure, and how the
  single-retry rule from `engine/memory-budget.md` § 4.1 propagates
  through the spilling operator (§ 10).
- The cancellation discipline inside spill loops, refining the yield
  rules in `engine/cancellation.md` § 3.2 (§ 11).
- The configuration surface and validation rules (§ 12).
- The cross-doc reconciliation list updated in the same checkpoint
  as this file (§ 13).

**Out of scope:**

- The `MemoryBudget` trait, the reservation lifecycle, and the
  per-operator spill-vs-fail policy at the *budget* level —
  `engine/memory-budget.md` (TASK-501).
- Cancellation, timeout, panic propagation, and the `TempSpillFile`
  RAII guard's *trait* shape — `engine/cancellation.md` (TASK-505).
  This doc consumes that contract; it does not redefine it.
- Compaction-side temporary files (output segments built incrementally
  during merge) — `storage/compaction-concurrency.md`. Compaction
  writes its outputs into the segment store, not the spill tree.
- The byte-level v1/v2 segment formats — they are not the spill
  format. Spill files are a private, per-query, per-process artefact;
  segment files are durable.
- Aggregate spill — explicitly deferred past v1 (`execution-model.md`
  § 10.4, `engine/memory-budget.md` § 7).

---

## 2. The Conflict This Note Retires

Three threads of doc drift around spill accumulated across waves:

1. **`execution-model.md` § 10.3** (Wave 0) listed sort spill + IN
   subquery spill as v1 features; aggregate explicitly does not spill.
2. **`engine/memory-budget.md` § 7** (TASK-501, Wave 5) updated the
   policy table to mark sort spill (TASK-513) as the only confirmed
   spilling operator and explicitly deferred the cohort / IN-subquery
   decision to TASK-502.
3. **Older task text** in `operators/sort-distinct.md` / earlier task
   drafts pointed at "broader spill work" (originally aggregate spill
   was on the radar) without naming the operators precisely.

This note pins the v1 answer and updates the upstream docs in the
same checkpoint (§ 13). The retired drift, in one line each:

- **Aggregate spill is not in v1.** Hard cap (`max_groups` = 1M);
  fail with `MaxGroupsExceeded`. Confirmed by `engine/memory-budget.md`
  § 7 and `operators/aggregate-operator.md`.
- **Sort spill is in v1.** TASK-513 implements the protocol below.
- **Ingest partitioner spill is in v1.** TASK-512 implements it.
  (Outside `QueryContext` — see § 4.2.)
- **Cohort / IN-subquery spill is *not* in v1.** Fails fast with
  `MemoryBudgetExceeded`. TASK-514 wires the budget integration only;
  no on-disk hash-set is built. Rationale: § 4.3.

---

## 3. The v1 Spill Surface

The v1 list is short on purpose: every spilling component is a place
where keeping the operator simple would silently degrade large-input
queries, and where the on-disk shape is a near-mechanical reflection
of the in-memory shape.

| Source | Spills in v1? | Trigger | Owner | Implementation task |
|---|:---:|---|---|---|
| `SortOperator` (ORDER BY) | **Yes** | `MemoryBudget::try_reserve` returns `Err` after the in-memory sort buffer would otherwise grow past the remaining budget | `bqlite-operators::sort` | TASK-513 |
| Ingest partitioner (`Partitioner` in `bqlite-storage::ingest`) | **Yes** | Buffered bytes would exceed `Partitioner::budget_bytes` (256 MiB default) | `bqlite-storage::ingest::partitioner` | TASK-512 |
| Cohort / IN-subquery materialization (`MergeSources`, `SubqueryFilter`) | **No (fails fast)** | n/a — handler is not registered | `bqlite-operators::cohort` | TASK-514 (budget wiring only) |
| `HashAccumulator` (STATS) | No | n/a | `bqlite-operators::aggregate` | — |
| `DistinctOperator` | No | n/a | `bqlite-operators::distinct` | — |
| `MatchOperator` (sequence matching) | No | n/a | `bqlite-operators::matcher` | — |
| `SessionizeOperator` | No | n/a | `bqlite-operators::sessionize` | — |
| `AttributeOperator` | No | n/a | `bqlite-operators::attribute` | — |
| `EventSelectOperator` (FIRST/LAST/NTH/SAMPLE) | No | n/a | `bqlite-operators::event_select` | — |
| Stateless kernels (filter/project/limit, fused stateless segment) | No | n/a | `bqlite-operators::filter`/`project`/`limit` and TASK-518 | — |
| Scan / k-way merge buffers | No | Fixed-size; charged once at construction | `bqlite-operators::scan` | — |

The "Owner" column names the crate path that owns the spill-handler
registration call. Operators that do not spill have **no**
`register_spill_handler` call in their constructors — it is a
deliberate omission, not an oversight.

The two spilling operators are intentionally far apart in the
runtime topology:

- **Sort** is per-query, lives inside `QueryContext`, reserves through
  `Arc<dyn MemoryBudget>`, and registers a spill handler.
- **Ingest partitioner** is per-`INSERT`, has its own budget
  (`Partitioner::budget_bytes`, defaults to 256 MiB —
  `engine/memory-budget.md` § 2.2), does not run inside
  `QueryContext`, and self-triggers spill against its own
  internal threshold. It does **not** call `try_reserve` on the query
  budget.

This split mirrors the budget split in `engine/memory-budget.md`
§ 2.2: query and ingest are independent allocators with independent
spill paths. They share *only* the spill-root directory and the
`TempSpillFile` RAII guard (§ 8.1).

---

## 4. Why These Choices

### 4.1 Why sort spills

A sort buffer is by definition unbounded in input size. Ordering
billions of rows by a single column has a legitimate use case (export,
top-N over a large window after `LIMIT` pushdown is impossible, etc.).
The on-disk shape is identical to the in-memory shape — a sequence of
sorted rows — so the spill format is just "the same `RecordBatch`,
serialized" (§ 6.1). The merge pass is a k-way merge of sorted runs,
the same algorithm the storage layer already uses for compaction.

### 4.2 Why ingest spills

The `(window_id, shard_id)` partitioner sorts each bucket by
`(entity_id, ts)` before flushing to L0 segments
(`crates/bqlite-storage/src/ingest/partitioner.rs`). For a
multi-billion-row CSV import on a host with a 256 MiB ingest budget,
holding every event in memory is impossible. External spill + merge
preserves the same `(entity_id, ts)` ordering and `batch_id`
assignment that downstream operators rely on. Without spill the
partitioner can only refuse the import (the current Wave 2 behaviour,
which is an explicit "loud failure" — see
`partitioner.rs::push_event`).

The ingest partitioner is **not** part of `QueryContext`. It uses its
own `Partitioner::budget_bytes` ceiling and does not interact with
`MemoryBudget`. It still uses the same `TempSpillFile` RAII guard
and the same spill root directory as the query side, because the
crash-recovery contract (§ 9) is process-wide, not query-scoped.

### 4.3 Why cohort / IN-subquery does *not* spill

`execution-model.md` § 10.3 originally listed IN-subquery spill as a
v1 feature ("Write the hash set to a temporary on-disk hash table
(sorted file with binary search). Probe the on-disk table for each
entity during the outer query scan."). After Wave 4's
`language/cohorts-aliases-joins.md` and Wave 5's
`engine/memory-budget.md` landed, the decision was deferred to
TASK-502. v1 ships **without cohort spill**, for the following
reasons.

**1. The on-disk probe is a different data path, not a swap-out.**
A hash set is O(1)-probe; a sorted file with binary search is
O(log n) random IO per probe. The outer scan probes the cohort
once per row in v1 (per-row in the worst case; per-entity once
TASK-522 ships entity-id pushdown). For a 100M-entity cohort and
a billion-row outer scan, an on-disk binary-search probe is
unworkable: each probe is a disk seek, the kernel's page cache
loses to random access patterns, and the query becomes
disk-bound on a structure that exists only as a query-time
optimisation.

A spill that is dramatically slower than failing the query is not
a useful spill. The "spill" then becomes "the user's query runs
for hours and they kill it anyway" — strictly worse than a
typed-error-at-the-boundary they can react to.

**2. The 3 GiB query budget already holds a large cohort.** Entity
IDs are ~12 bytes on average (string heap + length); a `HashSet`
with ~30 bytes of overhead per entry holds roughly
`3 GiB / 30 ≈ 100M` entity IDs. The Wave 5 ship target accommodates
analytics workloads up to that scale; cohorts above 100M entities
are an order of magnitude larger than any documented use case in
the project.

**3. TASK-522 (entity-id pushdown into scan) is the right answer for
big cohorts.** When the cohort fits, the planner can push it into
the outer scan as a `ScanPredicate` with shard / segment skipping.
A pushed-down cohort produces a *smaller* outer scan, not a slower
outer probe. Pushdown wins where spill loses; the engine should not
build the spill path at the cost of the pushdown path.

**4. Consistency with aggregate.** Aggregate is already fail-fast
(`engine/memory-budget.md` § 7) for the same hash-table-doesn't-spill
reason. Cohort is the same shape — a hash set keyed by an entity ID
or compound key. Treating the two consistently keeps the v1 fail-vs-
spill story easy to teach.

**5. Implementation cost.** Adding cohort spill in v1 means: an
on-disk hash data structure with non-trivial layout (sorted file,
key directory, probe protocol), a probe code path in the outer scan
that materialises through cached IO, and integration tests for both
the spill and the merge boundaries. None of that pays off until a
real workload demands it.

The user-facing contract is: a query whose cohort exceeds the budget
fails with `BqliteError::MemoryBudgetExceeded`. The CLI prints a
hint pointing at the ways to make the cohort fit (filter the inner
query, narrow the time range, raise the budget). This is documented
in `query-language.md` § 17 / § 18 and `language/cohorts-aliases-joins.md`
§ 2.7 in the same checkpoint as this doc.

A future wave may revisit this decision once entity-id pushdown
(TASK-522) is in production and we have evidence about the cohort
sizes real users hit. The trait surface in
`engine/memory-budget.md` § 4 already accommodates a cohort spill
handler if a future wave adds one — no engine code change is
required to revisit; only TASK-514's implementation grows a
spill-handler arm.

### 4.4 Why aggregate, distinct, sessionize, attribute, match, event-select do *not* spill

Documented in `engine/memory-budget.md` § 7. This doc does not
re-litigate. The summary: each has a per-operator hard cap that
fires before the budget would, and the cap is the right
diagnostic — "your one entity has too much state" or "your
group cardinality is too high" is more actionable than a spill
that masks the underlying skew.

---

## 5. The Spill Root Directory

### 5.1 Default location

```
<spill_root>  defaults to  <db_root>/spill/
```

Where `<db_root>` is the path the engine was opened with. Spill files
are scoped to a single database open; they have no cross-startup
meaning (§ 9).

This default makes the spill tree automatically scoped to the
database's directory lock (`storage-format.md` § 14.1). Holding the
flock on `<db_root>/.lock` is the single guarantee that no other
bqlite process is operating on this database; locating the spill
tree under `<db_root>` extends that guarantee to the spill files
without introducing a second lock.

### 5.2 Configuration surface

```rust
impl Engine {
    /// Override the spill root for this engine instance. The path
    /// must be an absolute filesystem path. If the directory does
    /// not exist, the engine creates it on `open` (§ 5.4); if it
    /// does exist, the engine reclaims it (§ 9.1).
    pub fn with_spill_root(self, spill_root: PathBuf) -> Self;
}

pub struct EngineConfig {
    pub query_memory_budget_bytes: u64,
    pub compaction_memory_budget_bytes: u64,
    pub ingest_memory_budget_bytes: u64,
    /// `None` → `<db_root>/spill/`. `Some(p)` → `p`.
    pub spill_root: Option<PathBuf>,
}
```

The host configures this once per `Engine`; per-query overrides are
not supported in v1.

### 5.3 Constraint: the spill root must be exclusively owned by one database open

A single `<spill_root>` may only be used by one bqlite process at a
time, scoped to one database. The default (`<db_root>/spill/`)
satisfies this automatically because of the database lock; a
user-configured spill root must do so by construction.

If the host configures `spill_root = /tmp/bqlite-spill` and opens two
databases out of `/db/foo` and `/db/bar` from the same process, the
two engines would race: both think they own the spill tree, both
would call `rm_rf(spill_root)` on open, both would recreate it empty,
and queries against one database could clobber the other's live
spill writes — silent wrong answers, not a loud error.

To prevent this, the engine maintains a process-global registry of
canonicalised spill roots already in use by a live `Engine`. A
second `Engine::open(...)` whose canonicalised `spill_root` matches
an entry in the registry fails with a typed configuration error
naming both databases. The registry is a `Mutex<HashSet<PathBuf>>`
held by an engine-internal `static`; entries are inserted at the end
of `open` (after the spill-root sweep succeeds) and removed at the
end of `close` / `Drop`. The cost is negligible (one path
canonicalisation and one set probe per `open`), and the check fires
*before* the second engine's `rm_rf` runs (§ 5.4 step 1), so the
first engine's spill tree is never reclaimed by an interloping
peer.

Cross-process duplicate spill roots remain undefined behaviour: two
distinct bqlite processes pointed at the same `spill_root` (different
databases, no shared flock) will silently clobber each other. The
host must keep the spill root unique per database for that case; the
default (`<db_root>/spill/`) does so automatically because of the
database lock. A future wave may add a per-database sentinel file
inside `<spill_root>` that the engine writes at open and another
process refuses to nuke if it is fresh; deferred until cross-process
sharing of spill roots is shown to be a real configuration.

### 5.4 Engine open

On `Engine::open(...)`, after taking the database lock and before
serving any queries:

1. `rm_rf(<spill_root>)`. Best-effort; logs a warning if it fails for
   reasons other than `NotFound`.
2. `mkdir_p(<spill_root>)` with mode `0o700` on POSIX.
3. The spill tree is now empty.

This single sweep replaces the per-pid filename scan suggested in
`engine/cancellation.md` § 5.2. The reason it is safe: the database
lock guarantees no other live process is using this spill tree;
every prior process has already exited (cleanly or via crash);
spill files have no cross-startup meaning. § 9 expands on the
crash-recovery argument.

### 5.5 Engine close

On `Engine::close`, after every query has finished and every ingest
has completed:

1. `rm_rf(<spill_root>)`. Same best-effort semantics.
2. The flock on `<db_root>/.lock` is released.

A clean shutdown leaves an empty (or absent) spill tree.

---

## 6. Per-Purpose File Layouts

Each spill source has its own on-disk layout. All layouts share the
same path scheme (§ 7) and the same `TempSpillFile` RAII guard
(§ 8.1) — they only differ in the byte payload.

### 6.1 Sort spill runs

Source: `SortOperator` (TASK-513).

**File contents:** a sequence of Arrow IPC stream-format batches,
written through `arrow_ipc::writer::StreamWriter`. Each stream
contains one *run* — a contiguous, in-memory-sorted slice of input
rows. The schema is the operator's *output* schema (which equals
the input schema for sort — sort never adds or drops columns).
Streams use no compression in v1; the spill path is disk-bound,
not CPU-bound, and the OS page cache absorbs short-lived writes.

**Why Arrow IPC stream format:**

- Already a dependency (`arrow-ipc` crate). No new format to
  maintain.
- Self-describing: the schema lands in the file's footer, so the
  merge pass does not need to round-trip schema metadata through
  operator state.
- Each `RecordBatch` boundary is a natural row-group analogue, so
  the merge pass can stream batch-by-batch without buffering whole
  runs.
- The merge pass uses `arrow_ipc::reader::StreamReader` to lazily
  pull batches.

**Run boundaries:** one file per run. A run is one in-memory sort
buffer that grew until the spill handler fired (or the operator's
input was exhausted, in which case the final run stays in memory
unless the merge pass decides otherwise).

**Sort key:** the run is sorted before being written — the writer
does not re-sort. The sort key is the operator's `OrderBy` key list
(`operators/sort-distinct.md` § 4). Null ordering follows the same
operator's rules.

**Merge pass:** after the input is fully drained, `SortOperator`
opens every spilled run plus the in-memory residual run and runs a
k-way merge using `BinaryHeap` over the run heads. The output is
materialised as `RecordBatch`es of the operator's batch size and
emitted through the normal `next_batch()` boundary. The merge does
not write back to disk.

### 6.2 Ingest partitioner spill

Source: `Partitioner` in `bqlite-storage::ingest::partitioner`
(TASK-512).

**File contents:** one file per spilled `(window_id, shard_id)`
bucket. Each file contains the events in that bucket, sorted in
place by `(entity_id, ts)` before being written, then serialised as
a length-prefixed [`postcard`](https://docs.rs/postcard) stream of
[`bqlite_core::event::Event`] records: a 4-byte little-endian
length followed by that many postcard bytes, repeated until end of
file. The format is private to the partitioner; the run files are
read back only by the partitioner's own merge pass on the same
process during the same ingest call.

This is the one deliberate format split between sort and ingest
spill. Sort operates on `RecordBatch` values that already carry an
Arrow schema, so Arrow IPC stream is the natural fit (§ 6.1). The
partitioner's input is `Event` values, and the partitioner does not
own a [`bqlite_core::schema::TableSchema`] at this layer (the
schema lives one crate up in the engine, in
`bqlite-engine::ingest`). Round-tripping through a synthetic Arrow
schema would either thread the table schema down through the
partitioner constructor — adding a parameter every existing caller
would need to update — or wrap the postcard payload inside a
single-column Arrow Binary array, which gains no debuggability over
postcard alone. The postcard direct path uses an existing
`bqlite-storage` dependency, preserves every Event semantic
(properties bag, `EntityId::String`/`Int` discriminants), and
keeps the partitioner self-contained. Spill files are private,
per-process artefacts crash-recovered by reclamation (§ 9), so
format stability across versions is not a contract.

**Filename scheme:** spill files live at
`<spill_root>/<query_id>/ingest-part-w<window>-s<shard:04>-<seq:06>.spill`,
matching the worked example in § 7. The shard slot is zero-padded
to four digits and the seq slot to six digits so a `read_dir` walk
in lexicographic order matches the spill creation order — useful
for ad-hoc inspection and required by the design's "lex order
matches creation order" rule (§ 7).

The partitioner already builds buckets keyed by `(window_id, shard_id)`
and sorts each bucket at drain. The spill protocol changes drain
into a two-phase process:

1. **Pre-drain spill phase.** When `push_event` would take
   `buffered_bytes` past `budget_bytes`, the partitioner picks the
   largest in-memory bucket, sorts it, writes it to
   `<spill_root>/<query_id>/ingest-part-w<window>-s<shard>-<seq>.spill`,
   drops the in-memory copy, and decrements `buffered_bytes` by the
   spilled estimate. The push that triggered the spill is then
   retried (the same single-retry rule the budget protocol uses,
   `engine/memory-budget.md` § 4.1). If the retry still overshoots,
   the partitioner spills the next-largest bucket. If after spilling
   every in-memory bucket the push still overshoots — i.e. one
   single event is bigger than the entire budget — the partitioner
   returns `BqliteError::Execution("…oversized event…")`. That
   failure mode is the same as today's "refuse to push" error, only
   now after the partitioner has done what it can.
2. **Drain phase.** `drain_sorted` becomes a k-way merge across
   every spilled bucket file *for the same `(window, shard)`* plus
   the in-memory residual. The merge yields events in
   `(entity_id, ts)` order per bucket, preserving the contract
   downstream operators rely on. Different `(window, shard)`
   buckets are still drained in ascending `(window_id, shard_id)`
   order at the bucket level — the partitioner's outer iterator
   shape does not change.

**`batch_id`:** the partitioner already stamps a single `batch_id`
across every event it sees (`Partitioner::batch_id`). Spilling does
not change that — the `batch_id` is captured at construction and
does not interact with disk lifetime.

**Spill is not visible outside `bqlite-storage::ingest`.** The
partitioner returns the same `(BucketKey, sorted events)` pairs to
its caller; the segment writer does not know whether the events
came from an in-memory bucket or a spilled-and-merged stream.

**Spill files for ingest live under the *query* spill subdirectory
even though ingest does not run inside `QueryContext`.** The
partitioner asks the engine for a per-ingest "query id" at
construction (a UUIDv7 or monotone counter scoped to the ingest
call); the spill subdirectory is created under that id. This keeps
every spill path uniform — one cleanup model — and avoids
sprouting an `ingest/` subtree in parallel to `query/`. The id is
purely a directory name; it has no other semantic meaning.

### 6.3 Cohort / IN-subquery — not applicable

Cohort hash sets do **not** spill in v1 (§ 4.3). There is no
on-disk format for them. TASK-514 wires the cohort hash set into
`MemoryBudget` reservation; on `try_reserve` failure, the
materialisation aborts the query with
`BqliteError::MemoryBudgetExceeded`. No spill handler is registered.

If a future wave reverses this decision, a new § 6.3 is added that
specifies the on-disk hash-set format. The trait surfaces above do
not need to change.

---

## 7. Path and Naming Scheme

```
<spill_root>/<query_id>/<purpose>-<seq>.spill
```

Where:

- `<spill_root>` — § 5; defaults to `<db_root>/spill/`.
- `<query_id>` — the engine-assigned per-query identifier. Format
  owned by TASK-541 (the morsel scheduler / query handle layer);
  this doc only requires that it is a filesystem-safe ASCII string
  and unique within the lifetime of one `Engine`. For the
  pre-TASK-541 single-threaded driver, `query_id` is a monotone
  counter rendered as a zero-padded decimal (e.g. `000000042`);
  TASK-513 starts here, TASK-541 swaps to UUIDv7. The directory
  name is opaque to the spill protocol; it is just a unique
  per-query container.
- `<purpose>` — short ASCII tag identifying the spill source:
  - `sort-run` — `SortOperator` runs.
  - `ingest-part-w<window>-s<shard>` — partitioner buckets,
    encoding the bucket key in the filename for debuggability.
- `<seq>` — monotone counter within `(query_id, <purpose>)`,
  zero-padded to six digits. Lexicographic order matches creation
  order, which matters for the merge pass when it walks the
  subdirectory. Note that the bucket key is already encoded inside
  `<purpose>` for the partitioner case (e.g.
  `ingest-part-w19782-s0007`), so two writes against the same
  `(window, shard)` bucket get distinct `<seq>` values; two writes
  against *different* buckets get distinct `<purpose>` tags so their
  `<seq>` values may coincide without collision.

Examples:

```
<db_root>/spill/000000042/sort-run-000001.spill
<db_root>/spill/ingest-7f3c-…/ingest-part-w19782-s0007-000003.spill
```

**Why the per-query subdirectory.** Cleanup at query end becomes a
single `rm_rf(<spill_root>/<query_id>)`. No filename pattern matching;
no risk of deleting a sibling query's files. The startup sweep
similarly reduces to a single `rm_rf(<spill_root>)`. Filesystems
handle thousands of files per directory comfortably; the per-query
container caps the worst case at one query's spill set.

**Why no `<pid>` in the filename.** `engine/cancellation.md` § 5.2
sketched a filename pattern with `<pid>` so a startup sweep could
pid-test each file. This doc supersedes that with the simpler
"nuke `<spill_root>` on engine open" model (§ 5.4, § 9.1). Without
the per-pid filename scan there is no need to embed `<pid>` in the
filename, and removing it makes the names smaller and easier to
grep in operational logs.

**Why no `.tmp` extension.** `.spill` is more specific to bqlite's
on-disk artefacts and aligns the spill tree's contents with their
purpose. `.tmp` suggests "any short-lived file"; `.spill` makes the
sweep target unambiguous if the spill root is ever shared with a
debugging tool. (The default spill root is not shared, but the
extension is a cheap diagnostic improvement either way.)

**Per-query subdirectory creation is lazy.** The directory is
created on the first spill write inside that query, not at query
start. A query that never overflows the budget leaves no on-disk
trace, even an empty directory. This matches
`engine/cancellation.md` § 5.3.

---

## 8. RAII Cleanup Path

### 8.1 `TempSpillFile` is the single guard

Every spilled file is owned by a `TempSpillFile` RAII guard, defined
in `engine/cancellation.md` § 5.2 and shipped by TASK-510:

```rust
pub struct TempSpillFile {
    path: PathBuf,
    file: File,
    bytes_written: u64,
}

impl Drop for TempSpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
```

The trait shape is owned by `engine/cancellation.md`; this doc
freezes how it is used.

**Construction.** Spill writers obtain a `TempSpillFile` via a new
engine-side helper:

```rust
impl QueryContext {
    /// Open a fresh spill file under this query's subdirectory.
    /// `purpose` is the `<purpose>` tag from § 7. The engine creates
    /// the per-query subdirectory lazily and assigns a fresh `<seq>`.
    pub fn open_spill(&self, purpose: &str) -> Result<TempSpillFile>;
}
```

The ingest partitioner has its own `Partitioner::open_spill`
that reuses the same engine-side helper; the partitioner does not
have a `QueryContext` but is given an `Arc<EngineSpillFs>` at
construction. `EngineSpillFs` is the small engine-internal struct
that owns the spill root path and the per-id sequence counters; it
is not part of any public surface.

**Operators must not call `remove_file` directly.** The guard's
`Drop` is the single deletion site.

**Guard ownership.** A spill writer holds a `Vec<TempSpillFile>` —
one entry per run / per partition. Dropping the writer drops every
guard, which deletes every file. The merge pass takes ownership of
each guard for the duration of its read pass; when a run is fully
consumed, the guard is dropped and the file deleted.

### 8.2 Drop order vs. memory release

`engine/cancellation.md` § 5.1 freezes the cleanup ordering for
every query exit path:

1. Operators (top-down via `Drop`).
2. Spill files (`TempSpillFile::Drop` deletes them).
3. Memory reservations (`MemoryReservation::Drop` releases bytes).
4. Worker contexts.
5. Cancellation token.

Spill files drop *before* memory reservations because the spill
guard is held inside the operator's state; releasing operator state
releases the guard, which deletes the file, which lets the
reservation release the bytes the file had been "freeing" through
the spill handler. The order is enforced structurally (RAII
holds nothing beyond what the operator owns); operators do not
implement a teardown order themselves.

### 8.3 Per-query belt-and-braces sweep

After the operator tree's `Drop` has run for a query, the engine
calls `rm_rf(<spill_root>/<query_id>)` as a belt-and-braces sweep
against any guard whose `Drop` failed silently (e.g. a deletion
EBUSY we did not log). This runs on success, error, cancel, timeout,
and panic exit paths, gated only by "we got past the operator-tree
drop".

The sweep is best-effort. An EBUSY or EPERM failure logs a warning
and is not fatal — the next engine open will reclaim the directory
anyway.

### 8.4 Cancellation, timeout, panic — same path

`engine/cancellation.md` § 5.1 already establishes that all three
exit through the same RAII teardown. This doc reaffirms it: the
spill protocol does **not** introduce a separate cancellation /
timeout / panic codepath. Whatever fired, the operator tree drops,
guards delete files, the engine sweeps the per-query subdirectory.

---

## 9. Crash Recovery

### 9.1 The recovery model

bqlite has a single recovery rule for spill: **on engine open, the
spill root is reclaimed and recreated empty** (§ 5.4). After this
sweep, no spill artefact from a previous process can affect the
current open.

The argument:

- Spill files are by construction private to one process. The
  database flock guarantees only one bqlite process at a time.
  When this process opens, every prior process has exited.
- A clean shutdown (`Engine::close`) already nukes the spill root
  (§ 5.5). A crash skips that, leaving spill files behind.
- The crash-residual files have no cross-process meaning — they
  were owned by a `TempSpillFile` guard that no longer exists,
  pointing at memory reservations that no longer exist, in a query
  that no longer exists.
- Therefore the safe answer is: delete them. No state is salvageable
  and no caller is waiting for it.

This is the simplest recovery model and the cheapest at runtime
(one `rm_rf` per engine open, not per-file pid checks).

### 9.2 Comparison with `engine/cancellation.md` § 5.2

`engine/cancellation.md` § 5.2 sketched a per-pid filename pattern
and a "scan the spill root for non-live PIDs and delete them"
recovery path, modeled on `compaction-concurrency.md` § 6's orphan
sweep. This doc supersedes that with the simpler spill-root nuke,
for two reasons:

- The compaction sweep is more sophisticated because compaction is
  long-running, may be running concurrent with the open (across
  processes if the user mounts the same database elsewhere — out of
  scope for v1 anyway), and writes to a tree that *also* contains
  durable segments. The spill tree contains nothing durable.
- The pid-pattern sweep solves a problem we do not have: the
  database lock prevents the "live other-process owns part of the
  spill root" case in the first place.

The cancellation.md update is mechanical (drop the `<pid>` slot from
the filename pattern; replace the "scan and pid-check" wording with
"the spill root is reclaimed at engine open per `engine/spill.md`
§ 5.4"). It is part of this checkpoint's reconciliation list (§ 13).

### 9.3 What if `rm_rf` fails?

If the startup sweep cannot reclaim the spill root (e.g. EACCES
because the directory was created with a different uid), engine
open fails with a typed error pointing at the spill root and the
underlying IO error. The host is expected to fix the filesystem
state and retry. The engine does not start with a half-reclaimed
spill tree.

---

## 10. Operator Participation Protocol

The following details how a spilling operator interacts with
`MemoryBudget` (`engine/memory-budget.md` § 4). The contract is
identical for every spilling operator, with one concrete query-side
instance in v1 (sort) plus the partitioner's parallel self-managed
protocol. Cohort / IN-subquery deliberately does *not* register a
handler — it fails fast (§ 4.3).

### 10.1 Registration

A spilling operator constructs itself with an `Arc<dyn MemoryBudget>`
and an `Arc<EngineSpillFs>` (the engine-side spill filesystem
helper, § 8.1). At the end of the constructor, it registers a
spill handler with the budget:

```rust
let handler: Arc<dyn SpillNotification> =
    Arc::new(SortSpillHandler { state: state.clone() });
budget.register_spill_handler(handler);
```

The handler holds an `Arc` of the operator's spillable state — not
of the operator itself, which would create a reference cycle. The
state struct exposes the methods the handler needs to choose what
to spill and how much.

Registration order is significant (`engine/memory-budget.md` § 4.1):
handlers are invoked in the order they registered. In v1, only sort
registers a handler per query, so order does not matter; with cohort
fail-fast and the partitioner outside `QueryContext`, there is at
most one spilling registrant per `MemoryTracker`.

### 10.2 What the handler does on `on_pressure`

`SpillNotification::on_pressure(&self, bytes_needed: u64) -> u64`
(`engine/memory-budget.md` § 4.2) returns the bytes actually freed.
A spilling operator implements it as:

1. Pick a unit to spill — for sort, the in-memory run; for ingest,
   the largest bucket. The unit is large enough to amortise the
   write cost (a multi-megabyte run, not a single batch).
2. Open a `TempSpillFile` via `QueryContext::open_spill(...)`.
3. Write the unit to disk (Arrow IPC stream). The write checks
   `is_cancelled()` between batches per § 11.
4. Drop the in-memory representation and any `MemoryReservation`
   it held. The dropped reservation calls back into the tracker,
   subtracting its bytes from `used`.
5. Append the `TempSpillFile` guard to the operator's
   `Vec<TempSpillFile>` so it lives until the merge pass consumes
   it.
6. Return the *exact* bytes freed — measured by the same accounting
   the reservation lifecycle uses, **not** the bytes written to disk.
   Returning more than was actually released invalidates the
   budget invariant. Concretely: a 100 MiB sort run held a 100 MiB
   `MemoryReservation`; the spill writer may emit a 70 MiB Arrow IPC
   stream because of dictionary encoding, but the handler returns
   100 MiB (the reservation's bytes), not 70 MiB (the on-disk size).
   The two numbers are tracked separately by design.

**Single-retry contract.** The budget retries the failed
reservation once after the handler returns (`engine/memory-budget.md`
§ 4.1). If the retry still overshoots, the budget moves on to the
next handler (no other handler in v1) and ultimately returns
`MemoryBudgetExceeded`. The handler must therefore free *enough*
on the first call — not "free a little, hope the next caller frees
more". For sort this means picking the run size large enough that
freeing one run usually gives back at least the requested bytes.

**Re-entrancy.** The handler may itself call `try_reserve` while
spilling (e.g. to allocate a small write buffer). The budget drops
the spill-handlers mutex before invoking handlers
(`engine/memory-budget.md` § 4.2), so re-entrant `try_reserve` does
not deadlock.

### 10.3 What the operator sees on `try_reserve` success after spill

From the operator's perspective, the only thing that distinguishes
a "freshly reserved" buffer from one that was reclaimed via spill is
the existence of the `TempSpillFile` guards in operator state. The
return value of `try_reserve` is identical. The merge pass at end-
of-input is the operator's job to plumb in — the budget does not
know about runs.

If a sort operator never spilled (the in-memory run fit), there are
no guards, `Vec::is_empty()` is true, and the merge pass collapses
to "emit the in-memory run". This is the same code path as the
zero-spill case, parameterised by the number of runs.

### 10.4 The partitioner's parallel protocol

The ingest partitioner does **not** call `register_spill_handler` —
it does not have a `MemoryBudget`. Its spill is self-triggered:
`push_event` checks `projected > budget_bytes` and, if true, runs
the spill loop in § 6.2 directly. The spill loop calls
`open_spill(...)` on `EngineSpillFs` and the resulting
`TempSpillFile` lifecycle is identical to the query side.

Why two protocols: the partitioner has one allocator (its
`buffered_bytes` counter), one bucket map, one purpose. There is
nothing to coordinate with a peer, so the indirection through a
`SpillNotification` trait would add complexity for no benefit.

---

## 11. Cancellation Discipline Inside Spill Loops

`engine/cancellation.md` § 3.2 specifies three yield points (batch
/ sub-batch / morsel) and explicitly lists "long-running spill
writes" as an exception that polls inside the spill loop. This doc
freezes the rules for that exception:

- **Sort spill writer** polls `is_cancelled()` between
  `RecordBatch`es it writes to a run file. The sort operator
  already operates on 64K-row batches, so this is a per-batch poll
  inside the spill loop (latency target identical to a `next_batch`
  boundary, ~10 ms).
- **Ingest partitioner spill writer** polls `is_cancelled()`
  between sorted-event chunks within one bucket flush. The
  partitioner sorts the bucket once before writing; the chunked
  write yields the slice in fixed-size pieces (default 65 536
  events per chunk, matching the storage row-group size).
- **Sort merge pass** polls `is_cancelled()` between output
  `RecordBatch`es it emits, which is the same check the operator's
  outer `next_batch()` already does. No additional yield site
  inside the merge loop is required because the merge naturally
  yields at `next_batch()` boundaries.

A cancelled spill write fails fast with `BqliteError::Cancelled`.
The operator's `Drop` runs the standard teardown path; every
`TempSpillFile` guard the operator still holds is deleted; the
per-query subdirectory is swept; nothing is salvageable.

Spill files are not synced (`fsync`) on the spill path. They are
temporary by construction, durability is unnecessary, and the
`fsync` cost dominates short spills. This matches
`engine/cancellation.md` § 5.2.

---

## 12. Configuration & Validation

### 12.1 Engine configuration

```rust
pub struct EngineConfig {
    pub query_memory_budget_bytes: u64,
    pub compaction_memory_budget_bytes: u64,
    pub ingest_memory_budget_bytes: u64,
    /// Override for the spill root. `None` resolves to
    /// `<db_root>/spill/`. The path must be absolute when set.
    pub spill_root: Option<PathBuf>,
}
```

### 12.2 Validation

- `spill_root`, when set, must be absolute. Relative paths are
  rejected at engine construction.
- The directory does not need to exist at config time. The engine
  creates it on `open` (§ 5.4).
- The path must not equal `<db_root>` itself. (A misconfiguration
  that causes the engine to nuke its own data directory at open is
  one we refuse to make possible.) Equality is checked against the
  canonicalised paths; symlinks are resolved.
- The path must not be a child of `<db_root>` *unless* it is exactly
  `<db_root>/spill/`. Any other in-database location risks colliding
  with segment storage. If the user explicitly opts in via a future
  knob, the engine does not have to care; in v1 the constraint is
  enforced.
- The spill root may be on a different filesystem than `<db_root>`
  (e.g. a tmpfs / RAM disk, or a faster local SSD when the database
  lives on a slower volume). No filesystem-type check is performed.

### 12.3 No per-query override

The spill root is fixed for the lifetime of an `Engine`. Per-query
overrides would require per-query directory creation and reclamation
logic that v1 does not implement. A future wave can add it without
changing the file-format contract.

---

## 13. Reconciliation Checklist

Updated in the same checkpoint as this file:

- **`docs/design/execution-model.md` § 10.3** — rewrite the
  IN-subquery spill subsection: cohort / IN-subquery does not spill
  in v1 per `engine/spill.md` § 4.3; the user-facing failure mode is
  `MemoryBudgetExceeded`; the previous "sorted file with binary
  search" sketch is dropped. Sort spill subsection updated to point
  at `engine/spill.md` § 6.1 for the on-disk layout. The aggregation
  no-spill paragraph already aligns with this doc; no rewrite
  needed.
- **`docs/design/engine/cancellation.md` § 5.2 / § 5.3** — drop the
  per-pid filename-pattern startup sweep; replace the wording with
  "the spill root is reclaimed at engine open per
  `engine/spill.md` § 5.4 / § 9.1". The `TempSpillFile` trait shape
  is unchanged. The filename-pattern sentence is removed; the
  per-query subdirectory layout is unchanged but now cross-references
  this doc as the canonical owner.
- **`docs/design/engine/memory-budget.md` § 7** — update the cohort
  row: change "Per TASK-502 / TASK-514 — either spill … or fail" to
  "**Fail** with `MemoryBudgetExceeded` per
  `engine/spill.md` § 4.3". Sort row updated to point at
  `engine/spill.md` § 6.1 alongside its existing TASK-513 reference.
- **`docs/design/operators/sort-distinct.md`** — the future-work
  table entry for TASK-502 updated to point at this doc; the body
  text of § 4 / § 6 already aligns. The forward reference
  ("`SortPhysical` will gain a `spill_dir: Option<PathBuf>` field")
  is rewritten to drop the field — the spill path does not need a
  per-physical-descriptor opt-in because every `SortOperator`
  participates by construction once TASK-513 lands; the engine's
  `spill_root` is the single configuration point.
- **`docs/design/operators/aggregate-operator.md`** — already
  aligned ("no spill in v1"); a one-line cross-reference to
  `engine/spill.md` § 3 is added to the no-spill subsection so a
  reader landing on the aggregate doc finds the canonical cross-
  operator policy.
- **`docs/design/language/cohorts-aliases-joins.md` § 2.7 / § 7** —
  rewrite "Until TASK-502 lands, the user-visible behaviour is
  'fail with `MemoryBudgetExceeded`'" as the permanent v1 contract;
  TASK-514 implements the budget integration (no spill code path).
  The "spill-vs-fail decision … owned by TASK-502" note is replaced
  with "v1 fails fast per `engine/spill.md` § 4.3; spill may be
  revisited in a future wave".
- **`docs/design/storage-format.md` § 13** — already points at
  `engine/memory-budget.md`; no spill-specific change needed beyond
  one sentence noting that ingest partitioner spill (TASK-512)
  honours `engine/spill.md` for file layout and cleanup.
- **`docs/design/storage-format.md` § 14.1** — no change. The
  database lock paragraph already documents the exclusivity guarantee
  this doc relies on; the spill-root rationale (§ 5.1) cross-references
  it.
- **`docs/design/INDEX.md`** — add an `engine/spill.md` entry under
  the Engine section.
- **`docs/design/operators/operator-traits.md`** — no change. The
  `register_spill_handler` plumbing is already documented as a
  budget concern (`memory-budget.md` § 4) and the operator-traits
  doc deliberately defers the spill plumbing to that note.

The detailed text changes land in the same diff as this file.
TASK-512, TASK-513, and TASK-514 are downstream implementation
tasks; their commits update the docs they touch (e.g. the partitioner
docstring) but do not move the design doc.

---

## 14. Non-Goals

- **Aggregate spill.** Deferred past v1
  (`engine/memory-budget.md` § 7).
- **Spill from inside a fused stateless segment.** The fused
  stateless-segment driver (`engine/operator-fusion.md`, TASK-503)
  never spills — it materialises through the boundary into a
  stateful operator (sort, aggregate) which is the spilling
  participant. Stateless kernels reserve per-batch and fail fast on
  budget pressure; the spill points are always at stateful-operator
  boundaries.
- **Cohort spill.** Deferred past v1 (§ 4.3). May be revisited
  alongside or after TASK-522 (entity-id pushdown) once production
  data on cohort sizes is available.
- **Per-query spill-root override.** Not in v1 (§ 12.3).
- **Spill-file durability.** Spill files are not `fsync`ed.
  Crash-recovery handles them by reclaiming the spill root (§ 9).
- **Compression of spill payloads.** Spill is disk-bound on the
  reference workloads; LZ4 / Zstd would add CPU for marginal IO
  savings on short-lived data. Future-wave knob if benchmarks
  motivate it.
- **Cross-process spill sharing.** Two bqlite processes opening
  the same database is already prevented by the database lock; the
  spill protocol relies on that exclusivity and does not attempt
  cross-process coordination.
- **Soft-pressure pre-emptive spill.** Operators only spill on hard
  `try_reserve` failure. A future wave could add a soft-pressure
  hook (the trait surface in `engine/memory-budget.md` § 4
  accommodates it) without changing the on-disk format.
- **Spill metrics in v1.** `engine/memory-budget.md` § 10 already
  enumerates the metrics surface (spill-handler invocations, bytes
  freed); TASK-524 (`--explain-perf`) lands them. No additional
  metrics are introduced here.

---

## 15. References

1. `engine/memory-budget.md` (TASK-501) — the budget trait, the
   reservation lifecycle, the `try_reserve` / spill-handler call
   protocol (§ 4), the per-operator policy table (§ 7).
2. `engine/cancellation.md` (TASK-505) — `TempSpillFile` RAII guard
   (§ 5.2), the per-query subdirectory layout (§ 5.3, refined here),
   the cleanup ordering (§ 5.1), the cancellation yield rules
   (§ 3.2).
3. `execution-model.md` § 10.3 — original sort + IN-subquery spill
   sketch; reconciled by this doc.
4. `operators/sort-distinct.md` — sort operator semantics; the
   in-memory-only path is the same algorithm the spill path
   degenerates to when no run is spilled.
5. `operators/aggregate-operator.md` — aggregate hard-cap fail
   policy.
6. `language/cohorts-aliases-joins.md` § 2.7 / § 7 — cohort size
   accounting; the user-visible failure mode this doc commits to.
7. `storage-format.md` § 13 (memory budget split) and § 14.1
   (database lock).
8. `crates/bqlite-storage/src/ingest/partitioner.rs` — the current
   "fail loudly on budget overflow" path that TASK-512 replaces.
9. TASK-502 (this design), TASK-512 (ingest spill), TASK-513 (sort
   spill), TASK-514 (cohort budget integration), TASK-525 (memory-
   pressure stress suite).
