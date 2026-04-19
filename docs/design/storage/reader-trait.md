# SegmentReader trait

**Wave**: 1
**Task**: TASK-109
**Status**: draft — frozen for Wave 1, extended by later waves

## 1. Scope

This note defines the **v0 trait surface** that scan operators use to
read segments out of the storage layer. Concretely:

- The trait hierarchy — `SegmentReader` (per-table catalog of
  segments) and `SegmentScan` (streaming iterator over one segment's
  row-groups).
- The supporting types the trait traffics in — `SegmentHandle`,
  `ColumnProjection`, `ZoneMap`, and the `Predicate` hook.
- How the five pushdown hooks the task description calls out
  — segment enumeration, column projection, row-group iteration,
  zone-map access, and predicate pushdown — project onto the Wave 1
  trait surface.
- Where the traits live in the dependency graph and why.

It does **not** design the native segment format (already covered by
[storage-format.md](../storage-format.md) §9), the k-way merge layer
that composes multiple `SegmentScan`s across shards and windows
(execution-model.md §3), the full predicate IR (a Wave 2 [DESIGN]
task), or the scan operator itself (TASK-117). Wave 1's job is to
ship a trait stable enough that Wave 2+ storage and scan work never
has to rebase behind a trait change. After Wave 1 the trait surface
is frozen; any later change requires a high-priority `[TRAIT]` task.

The authoritative background for the storage format is
[storage-format.md](../storage-format.md). This note is narrower: it
is the contract scan operators hold to against the storage layer,
not the full storage story.

## 2. Relationship to the existing design docs

The surface defined here is a **minimal v0 compatible projection** of
the richer model documented in storage-format.md §8–§11. Specifically:

| storage-format.md feature | Wave 1 trait surface | Rationale |
|---|---|---|
| Segment enumeration from the per-table manifest (§12.3, §7.6) | `SegmentReader::segments()` returns a lazy iterator of `SegmentHandle`. | Matches the manifest's `segments: []` inventory — one `SegmentHandle` per live entry. Stable iteration order for the lifetime of the reader (it is anchored to the snapshot taken at reader construction). |
| Row-group iteration within a segment file (§3.2, §9.2) | `SegmentScan::next_row_group()` yields one `RecordBatch` per row-group. | Preserves the 1:1 row-group ↔ batch alignment that execution-model.md §3.6 relies on: one row-group produces one batch with no splitting or buffering inside a single-segment read. |
| Column projection (§8.2 "lazy column reading") | `ColumnProjection` passed to `open_segment()`. | Keeps decode work proportional to the columns an operator actually references. An empty projection is interpreted as "all columns", matching the default-passthrough case. |
| Zone map pushdown (§11.1) | `SegmentScan::row_group_zone_maps(idx)` returns per-column `ZoneMap`s for row-group `idx`. | Scan operators consult these before calling `next_row_group()` and can skip row-groups whose zone maps are ruled out by the predicate. Kept as a hook — the concrete pruning decision lives in the scan operator, not the reader. |
| Predicate pushdown (§8.2) | `Option<Arc<dyn Predicate>>` passed to `open_segment()`, with a single `accepts_zone(column, zone)` method. | The narrowest possible v0 surface. A real predicate IR (evaluatable against whole batches, dictionary-aware for FSST and dict-encoded columns) is a Wave 2 `[DESIGN]` task; `Predicate` is intentionally one method wide so extending it is additive. |
| Dictionary filter bitsets (§8.2, execution-model.md §3.7) | **Deferred.** | Requires a real predicate IR and the encoding layer from storage-format.md §10. Wave 1 readers treat `Predicate` as a zone-map-only hint. |
| K-way merge across segments (§8.1) | **Not this trait.** | The merge sits above the per-segment reader and composes many `SegmentScan`s. It is a scan-operator concern (TASK-117 in Wave 1 reads a single segment at a time; the real merge lands in Wave 2 alongside the segment format). |
| Schema evolution (§6.4) | `SegmentReader::schema()` returns the *current* table schema; readers are responsible for filling missing columns with NULL/default values in the `RecordBatch` they return. | Matches §12.2 "schema authority": the manifest's schema is the output schema, not the per-segment schema at write time. Wave 1 stubs have nothing to evolve, but the trait surface commits to this direction. |
| Query snapshot refcounting (§7.6) | The `SegmentReader` is obtained for the lifetime of a query and holds a snapshot of the manifest. Iteration results never change across calls for the same reader. | The scan operator keeps the reader alive for the duration of execution; dropping the reader releases the snapshot. Wave 1 stubs have no compaction, so there is nothing to refcount yet. |

### 2.1 Planner-pipeline doc consistency

No changes to `planner-pipeline.md` or `storage-format.md` are
required for this task. The existing docs already describe the
pieces this trait hooks into; this note adds the glue.

## 3. Crate placement

`bqlite-operators` already depends on `bqlite-storage`
(architecture.md, `crates/bqlite-operators/Cargo.toml`), so placing
`SegmentReader` in `bqlite-storage` would not create a dependency
cycle — the scan operator could name it directly. The decision to
put the trait in `bqlite-core` is **not** about cycle avoidance; it
is about giving impls that are not the real storage layer a home.
Specifically:

- The **`Predicate`** trait lives in `bqlite-core` (it is a
  placeholder that a later-wave predicate IR will implement from
  `bqlite-planner`). `bqlite-planner` does not depend on
  `bqlite-storage` — placing `Predicate` in storage would force the
  planner to take a storage dep for a type that carries no storage
  concerns. Keeping `Predicate` in core is the only option, and
  keeping `SegmentReader` next to it preserves the one-module story.
- **In-memory fakes** in `bqlite-core`'s own unit tests need to
  implement `SegmentReader` to exercise the trait surface before
  TASK-116 lands. `bqlite-core` cannot depend on `bqlite-storage`
  (that direction is forbidden by the dep graph), so any trait the
  core tests want to implement has to live in core.
- `ZoneMap` carries `PropertyValue` (`bqlite-core`). `TableSchema`
  returned by `SegmentReader::schema()` also lives in core. Placing
  the trait in core keeps the type closure within one crate — no
  re-exports across the storage boundary.

The concrete layout is:

| Item | Crate | Why |
|---|---|---|
| `SegmentReader` trait | `bqlite-core` | Colocated with `Predicate` and the data types it traffics in; lets core unit tests exercise the trait with in-memory fakes. |
| `SegmentScan` trait | `bqlite-core` | Companion to `SegmentReader`; same module. |
| `Predicate` trait | `bqlite-core` | Placeholder. A real predicate IR in `bqlite-planner` will implement this trait without depending on `bqlite-storage`. |
| `SegmentHandle`, `ZoneMap`, `ColumnProjection` | `bqlite-core` | Pure data. `ZoneMap` carries `PropertyValue`, which already lives in core. |
| Concrete `SegmentReader` impl | `bqlite-storage` (`Database::segment_reader()` in TASK-116) | Real segments and the manifest live here. The Wave 1 stub returns an empty iterator. |

`bqlite-core`'s current `Cargo.toml` already depends on
`arrow`, `thiserror`, `serde`, and `tracing` — this task does not
widen that dep set. `RecordBatch` lives in `arrow::record_batch`,
which is re-exported from the top-level `arrow` crate we already
use in `crate::arrow`.

## 4. SegmentReader trait

### 4.1 Definition

```rust
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::schema::TableSchema;

/// Read-side API for a table's segments.
///
/// A `SegmentReader` is a per-query, per-table snapshot of the
/// manifest's live segment inventory. Scan operators obtain one from
/// the engine at query start and iterate it via
/// `segments()` → `open_segment()` → `SegmentScan::next_row_group`.
///
/// Implementations live in `bqlite-storage` (real segments) and in
/// test code (in-memory fakes). The trait is deliberately small —
/// the performance-critical work happens inside `SegmentScan`.
pub trait SegmentReader: Send + Sync {
    /// The table schema this reader produces rows against.
    ///
    /// This is the *current* schema from the manifest
    /// (storage-format.md §12.2), not any individual segment's
    /// write-time schema. Implementations are responsible for
    /// filling columns missing from older segments with NULL or the
    /// column's default value before returning a `RecordBatch`.
    fn schema(&self) -> &TableSchema;

    /// Enumerate segments visible to this reader's snapshot.
    ///
    /// Iteration order is implementation-defined but stable for the
    /// lifetime of the reader. Scan operators drive this iterator to
    /// completion; each element is opened separately via
    /// `open_segment()`.
    ///
    /// Returning an iterator (rather than a `Vec`) lets large
    /// manifests stream lazily and keeps the Wave 1 stub trivial —
    /// the empty-iterator case is `Box::new(std::iter::empty())`.
    fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_>;

    /// Open a streaming scan over a segment.
    ///
    /// - `handle` must be a value returned by `segments()` on the
    ///   same reader; opening a stale or unknown handle returns
    ///   `BqliteError::Execution`.
    /// - `projection` names the columns to decode, in the desired
    ///   output order. `ColumnProjection::all()` means "every
    ///   declared column plus the `__seq_id` and `__batch_id` system
    ///   columns, in table-schema order".
    /// - `predicate` is an optional pushdown hint. Passing `None`
    ///   disables zone-map and dictionary pruning for this scan.
    ///
    /// The returned `SegmentScan` holds the OS resources for one
    /// segment (a memory map, a file handle, or an in-memory buffer
    /// in the Wave 1 stub). Dropping it releases those resources.
    fn open_segment(
        &self,
        handle: &SegmentHandle,
        projection: &ColumnProjection,
        predicate: Option<Arc<dyn Predicate>>,
    ) -> Result<Box<dyn SegmentScan>>;
}
```

### 4.2 Lifecycle

- **Construction.** Engine asks `Database` for a reader at query
  start. The reader captures a manifest snapshot and holds a refcount
  until drop (§7.6 query snapshots). Wave 1 has no compaction, so
  this is a no-op.
- **Enumeration.** Scan operator calls `segments()` once and drives
  the iterator lazily. An error during enumeration aborts the query.
- **Per-segment scan.** Scan operator calls `open_segment()`, iterates
  the returned `SegmentScan` via `next_row_group()`, then drops it.
  Closing by drop keeps the API simple — no explicit `close()` call
  is required.
- **Error recovery.** An error from `next_row_group()` aborts the
  segment scan. The scan operator may drop the `SegmentScan` and
  continue with the next segment from `segments()`, or propagate the
  error upward. The `SegmentReader` itself remains valid.
- **Cancellation.** `SegmentScan` does not take a cancellation token.
  Scan operators poll the engine's cancellation flag between
  `next_row_group()` calls (the cancellation token model from
  TASK-108). Since `next_row_group()` returns one row-group at a
  time, latency is bounded by one row-group decode — typically
  10–100 ms for a 64K-row group, well under the 1-second target
  from execution-model.md §3.3.

### 4.3 Error propagation

All errors travel as `bqlite_core::BqliteError` (TASK-102). The
relevant variants:

| Variant | Meaning | Who raises it |
|---|---|---|
| `Io` | Failed file read, memory map failure, or similar | Real readers in `bqlite-storage` |
| `Arrow` | Arrow batch construction failed (e.g. type mismatch during schema-evolution backfill) | Any reader |
| `Schema` | A column referenced by `projection` is not in the reader's `schema()` | Every reader |
| `Execution` | Unknown or stale `SegmentHandle` passed to `open_segment()` | Every reader |

## 5. SegmentScan trait

### 5.1 Definition

```rust
/// Streaming read over a single segment's row-groups.
///
/// One-shot: obtain from `SegmentReader::open_segment`, iterate to
/// completion (or drop early), and request a fresh scan for another
/// pass. Dropping releases any OS resources the scan holds.
pub trait SegmentScan: Send {
    /// Number of row-groups in this segment — known before iteration
    /// starts. Used by scan operators to preallocate per-row-group
    /// scratch buffers and to drive zone-map pruning loops.
    fn row_group_count(&self) -> usize;

    /// Per-column zone maps for row-group `idx`.
    ///
    /// Returned as a name-keyed map because column presence is not
    /// guaranteed — zone maps for a column are absent when the
    /// row-group contains only nulls, when the column was added
    /// after the segment was written (schema evolution), or when the
    /// storage layer chose not to maintain zone maps for the column.
    /// Callers check presence per column.
    ///
    /// Returning `Ok(HashMap::new())` is legal and means "no zone
    /// maps available for this row-group" — the scan operator must
    /// then assume the row-group is not prunable and read it.
    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>>;

    /// Yield the next row-group as a `RecordBatch`, or `Ok(None)`
    /// when the segment is exhausted.
    ///
    /// Column order in the returned batch matches the
    /// `ColumnProjection` passed to `open_segment`. Nullable columns
    /// carry Arrow null bitmaps; non-nullable columns do not
    /// (execution-model.md §3.7).
    fn next_row_group(&mut self) -> Result<Option<RecordBatch>>;

    /// Yield the next row-group as an encoded-preserving `EncodedBatch`,
    /// or `Ok(None)` when the segment is exhausted.
    ///
    /// This is the encoded-path read hook added for the zero-copy
    /// scan/filter design (`docs/design/storage/zero-copy-scan-filter.md`).
    /// It lets scan operators consume encoded column chunks directly
    /// and run selection-first predicate kernels over them without
    /// eagerly materializing each row-group into an Arrow array.
    ///
    /// Default body: delegates to `next_row_group()` and wraps each
    /// produced Arrow column as `EncodedColumn::Materialized`. Every
    /// existing implementor therefore works on the encoded surface
    /// with no behavior change until it overrides this method with a
    /// real encoded reader.
    fn next_encoded_row_group(&mut self) -> Result<Option<EncodedBatch>> { /* default */ }
}
```

### 5.2 Ordering and batch shape

- Row-groups are yielded in the order they appear in the segment
  file. Within a single segment, this is `(entity_id, timestamp)`
  sort order (storage-format.md §3.1).
- Each returned `RecordBatch` contains exactly one row-group worth
  of rows — no concatenation, no splitting.
- An empty `RecordBatch` (zero rows) is legal for row-groups the
  predicate fully pruned; consumers must tolerate them. Implementations
  may also return `Ok(None)` early if they know every remaining row
  group is pruned.

### 5.3 Encoded read path (additive extension)

`next_encoded_row_group()` is an **additive extension** to the trait,
not a replacement for `next_row_group()`. The two paths coexist:

- The **materialized path** (`next_row_group`) remains the
  compatibility default. Every pre-existing `SegmentScan` implementor
  works without changes, and the default body of
  `next_encoded_row_group` wraps each Arrow column as
  `EncodedColumn::Materialized` so encoded-aware operators can still
  consume the legacy path uniformly.
- The **encoded path** (`next_encoded_row_group`) is opt-in for
  readers that can produce `EncodedBatch` directly from segment bytes.
  Selection-first predicate kernels (`bqlite-operators`) consume the
  encoded batch without materializing every row-group first.

Implementors must pick one path for the lifetime of a single scan —
mixing encoded and materialized iteration on one scan instance is
disallowed. Scan operators select the path via `ScanPath` (see
`bqlite-operators::ScanPath`), overridable via the
`BQLITE_SCAN_PATH=materialized|encoded|auto` environment variable.

## 6. Supporting types

### 6.1 SegmentHandle

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHandle {
    /// Monotonically increasing segment identifier from the manifest
    /// (storage-format.md §5.2, §6.2). Unique across the database.
    pub segment_id: u64,
    /// Shard index within the database's fixed shard count
    /// (storage-format.md §5.1).
    pub shard_id: u32,
    /// Identifier of the time window this segment belongs to
    /// (storage-format.md §4.1). `0` is legal and is used by the
    /// Wave 1 stub, which has no window partitioning yet.
    pub window_id: u64,
    /// Total rows in the segment across all row-groups.
    pub row_count: u64,
    /// Schema version the segment was written against
    /// (type-system.md §5, storage-format.md §6.4). Used by the
    /// reader to backfill columns added after the segment was
    /// written.
    pub schema_version: u32,
}
```

`SegmentHandle` is deliberately cheap to clone — scan operators may
hold many handles at once (one per shard per window in the k-way
merge) without worrying about allocation cost. It carries no pointers
into the segment file, so a handle that outlives its reader is
useless but not unsafe.

### 6.2 ZoneMap

```rust
#[derive(Debug, Clone, Default)]
pub struct ZoneMap {
    /// Column minimum, or `None` if the row-group is all-null for
    /// this column.
    pub min: Option<PropertyValue>,
    /// Column maximum, or `None` if the row-group is all-null.
    pub max: Option<PropertyValue>,
    /// Number of nulls in the row-group for this column.
    pub null_count: u64,
    /// Number of rows in the row-group (the same value is available
    /// from `SegmentScan` itself; duplicated here so a `ZoneMap` is
    /// self-contained for pruning loops).
    pub row_count: u64,
}
```

`PropertyValue` is the boundary type from `bqlite-core`; using it
here keeps the zone-map surface free of Arrow type juggling. Readers
populate `min`/`max` from the segment footer (storage-format.md §9.4
per-column-chunk metadata) by converting the stored Arrow value into
`PropertyValue` once at load time, so the predicate's `accepts_zone`
check does not touch Arrow.

### 6.3 ColumnProjection

```rust
#[derive(Debug, Clone, Default)]
pub struct ColumnProjection {
    names: Vec<String>,
}

impl ColumnProjection {
    /// Projection naming every column in table-schema order,
    /// including the implicit `__seq_id` / `__batch_id` system
    /// columns. Equivalent to `ColumnProjection::default()`.
    pub fn all() -> Self { … }

    /// Projection naming an explicit column list in the desired
    /// output order.
    pub fn with_columns<I, S>(columns: I) -> Self
    where I: IntoIterator<Item = S>, S: Into<String> { … }

    /// True if this projection means "all columns".
    pub fn is_all(&self) -> bool { … }

    /// Column names in projection order. Empty when `is_all()`.
    pub fn columns(&self) -> &[String] { … }
}
```

The empty-vec-means-all convention keeps the common case (`scan
events`) a single allocation cheaper than carrying an explicit
`Projection::All` variant, and matches how the planner currently
represents the "no projection pruning" case elsewhere.

### 6.4 Predicate

```rust
pub trait Predicate: Send + Sync + std::fmt::Debug {
    /// True if the predicate **might** accept at least one row in a
    /// range described by `zone` for column `column`. Returning
    /// `false` tells the reader to skip the range entirely.
    ///
    /// Conservative implementations may always return `true` — this
    /// disables zone-map pruning but is always safe.
    fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool;
}
```

One method wide on purpose. Wave 2's `[DESIGN]` task will add a
proper predicate IR with whole-batch evaluation, dictionary-aware
filtering, and fusion with the scan. This trait is the narrow hook
Wave 1 readers can honour cheaply while still giving the scan
operator a place to plug in a real predicate later.

## 7. v0 vs later waves

| Concern | Wave 1 | Later wave |
|---|---|---|
| Segment enumeration | Empty iterator from the stub; real manifest-backed iterator in TASK-116. | Unchanged — the trait surface is stable. |
| Row-group iteration | Wave 1 stub returns zero row-groups; TASK-117 drives the iterator. | Real segment format decoding lands in Wave 2 alongside the segment writer. |
| Column projection | `ColumnProjection` type lands; the stub honours `is_all()` trivially since it returns no rows. | Real projection pushdown in Wave 2 — the reader decodes only the columns in the projection and returns them in the requested order. |
| Zone map access | Trait method lands returning an empty `HashMap` in the stub. | Real zone maps are loaded from the segment footer when the segment is opened; `row_group_zone_maps()` becomes a cheap lookup. |
| Predicate pushdown | `Predicate` trait lands with one method. The stub ignores the argument. | Wave 2 [DESIGN] task introduces a predicate IR; existing readers implementing `SegmentReader` extend their `open_segment` impls to honour it. The trait itself does not need to change. |
| Dictionary filter bitsets (§8.2) | Not represented in the trait. | Added as an extension hook when the encoding layer lands. Not a trait method — the scan operator calls a segment-local API the reader exposes alongside `SegmentScan`. |
| Schema evolution | `schema()` returns the manifest's current schema; the stub's schema is the bootstrap `events` table from TASK-125. | Real evolution handled inside the reader; trait surface unchanged. |
| Cancellation | Polled by the scan operator between `next_row_group()` calls. | Same. No per-row cancellation — latency bounded by row-group decode time. |

## 8. Open questions

These are issues we consciously defer without locking in an answer.
None of them block Wave 1.

1. **Batch-size granularity for large row-groups.** Row-groups are
   a fixed 65,536 rows (storage-format.md §3.3). If later analysis
   shows operators prefer smaller batches for cache reasons, the
   trait may grow a `max_batch_size` hint or allow `SegmentScan` to
   split row-groups internally. Extending `next_row_group` to stream
   sub-batches for an entity that spans row-groups is an
   execution-model.md §5 concern and belongs in the scan operator,
   not this trait.

2. **Async vs sync.** The trait is synchronous — `next_row_group()`
   returns `Result<Option<RecordBatch>>`, not a future. If we later
   want to overlap I/O with compute at the segment level (not just
   the thread-pool level from execution-model.md §6), this trait will
   grow an async variant. Wave 1 stays synchronous because the
   single-core benchmark in core-beliefs.md §1 is the thing we
   actually measure.

3. **Predicate cost model.** A real predicate has a cost (dictionary
   lookups are cheap; re-evaluation per row is not). The Wave 2
   predicate IR will need to expose a cost so the scan operator can
   decide whether to enable pushdown. The Wave 1 `Predicate` has no
   cost hook; adding one is additive.

4. **Shared reader state.** A reader may want to cache open segment
   files across `open_segment()` calls (segment header parsing,
   memory maps, decompression state). The trait does not mandate or
   forbid this — implementations are free to reference-count
   internally. If sharing turns out to be load-bearing for
   performance, a later wave may add a `prefetch` hook.
