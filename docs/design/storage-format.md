# Storage Format Design

> **Status**: DRAFT
> **Task**: TASK-001
> **Depends on**: TASK-005 (type system)
> **Depended on by**: TASK-003 (execution model)

---

## 1. Design Goals

The storage engine serves three constraints from [core-beliefs.md](../core-beliefs.md):

**Performance (Belief 1).** The dominant workload is aggregate queries across all entities — retention, funnels, sequence matching. The storage format must deliver >1 GB/s scan throughput on a single core. This requires columnar encoding, compression that the CPU can decode faster than disk can deliver, and a layout that enables predicate pushdown to skip irrelevant data before it reaches operators.

**Entity-first data model (Belief 3).** Every query implicitly operates per-entity. Data is sorted by `(entity_id, timestamp)` so that all events for a given entity are contiguous on disk. Temporal operators consume entity-complete streams via sequential scan — no random I/O, optimal cache behavior.

**Embeddable, not a server (Belief 5).** No background daemon. Ingestion is a batch call that produces segment files directly. Compaction runs inline or as an explicit maintenance operation. The database is a directory that can be opened, queried, and closed without lifecycle management.

**Memory-conscious (Belief 6).** Scans, compaction, and ingestion all operate within the configured memory budget (default 4 GB). Buffer sizes are bounded and explicit. The read path streams entity batches rather than materializing full result sets.

---

## 2. Conventions

**Terminology.** A *segment* is the logical unit of storage — a sorted, encoded collection of rows for a single `(window, shard)` at a given compaction level. A *segment file* is its on-disk representation (e.g., `segment_1001.seg`; the naming scheme is specified in Section 5.2). This document uses "segment" for the logical concept and "segment file" when referring to the physical file.

**Byte order.** All multi-byte integers in the segment file format are **little-endian**. This matches the native byte order of x86 and ARM (the target platforms) and avoids byte-swapping on the read path.

---

## 3. Data Layout

### 3.1 Sort Order

Data within each segment file is sorted by `(entity_id, timestamp)`. This provides:

- **Entity locality.** All events for an entity are contiguous. Temporal operators (sequence matching, sessionization) scan one entity's events front-to-back with no seeking.
- **Timestamp ordering within entity.** Events arrive in causal order, which is the natural consumption order for pattern matching.
- **Efficient merge.** Merging multiple sorted segments is a k-way merge on the same key — standard, well-optimized, and streaming.

### 3.2 Row-Groups

Segments are divided into fixed-size columnar **row-groups**. Each row-group contains a fixed number of rows (default: 65,536) encoded column-by-column.

Entity boundaries do **not** align with row-group boundaries. This is an intentional tradeoff:

- **Pro:** Standard columnar infrastructure. Row-groups are uniform size, encoding and compression operate on predictable chunks, and existing columnar techniques (dictionary encoding, delta encoding, zone maps) apply directly.
- **Con:** A single-entity lookup at a row-group boundary may touch two row-groups.
- **Why acceptable:** The dominant access pattern is full-scan aggregate queries, not single-entity lookups. The sort order means that an entity's data is still contiguous on disk — the row-group boundary is a logical split, not a physical discontinuity. For the rare single-entity case, zone maps on `entity_id` skip directly to the relevant row-groups.

### 3.3 Row-Group Size

Default: **65,536 rows** (64K). This balances:

- **Encoding efficiency.** Dictionary encoding, delta encoding, and bit-packing all benefit from larger chunks. Below ~8K rows, dictionary overhead dominates for low-cardinality columns.
- **Zone map selectivity.** Larger row-groups mean coarser zone maps. At 64K rows with `(entity_id, timestamp)` sort order, each row-group covers a contiguous entity range — zone maps on `entity_id` remain highly selective.
- **Memory footprint.** A single decoded row-group with 10 columns at 8 bytes each is ~5 MB. This fits comfortably in L3 cache and leaves headroom for multiple active worker threads inside the owning process.

### 3.4 Segment-Level Data Structures

Some data structures are shared across all row-groups within a segment rather than duplicated per row-group:

- **Dictionaries.** For dictionary-encoded columns, the dictionary (sorted distinct values) is stored once in the segment footer, and each row-group's column chunk stores only the codes array. This avoids repeating the same dictionary in every row-group, reduces segment size, and enables cross-row-group dictionary pushdown — a single dictionary lookup produces a code that can be compared against all row-groups without re-resolving.
- **FSST symbol tables.** The 256-entry symbol table for an FSST-encoded column is stored once per segment, shared by all row-groups. Since the symbol table is built from a sample of the full column data (not per row-group), this produces better compression than per-row-group tables.

Per-row-group data structures: encoded column chunks (codes, deltas, packed bits, etc.), null bitmaps, and zone maps. These vary per row-group by definition.

---

## 4. Partitioning: Time Windows

### 4.1 Window-Based Partitioning

Data is partitioned into time windows based on **event timestamp**, not ingestion time. This is critical: historical backfills route events to the correct window based on the event's own timestamp.

Each time window is a directory containing one or more segment files per shard. Queries specify a time range, and the scan layer prunes windows that fall entirely outside the range before opening any files.

### 4.2 Window Granularity

Configurable as **N days** (single integer parameter, no calendar complexity). Default: **30 days**.

| Configuration | Use case | k for 6-month query |
|---|---|---|
| 30 days (default) | General analytics | 6 |
| 7 days | High-volume ingest, faster compaction cycles | 26 |
| 1 day | Very high volume, narrow time-range queries | 180 |

The 30-day default keeps k small for cross-window merges. A 6-month retention query merges k=6 sorted streams — trivial. Daily windows (k=180) create significant merge overhead and should only be used when window-level pruning is the dominant performance factor.

Window boundaries are aligned to UTC day boundaries. A 30-day window starting 2025-03-01 covers `[2025-03-01T00:00:00Z, 2025-03-31T00:00:00Z)`. Window directories are named by their window ID — the number of days since epoch (1970-01-01) for the window start, zero-padded to six digits. `2025-03-01` is day 20148, so this window's directory is `w_020148/` (see Section 5.2 for the full layout).

### 4.3 Backfill Behavior

Backfilling historical data writes new L0 segments into the corresponding old windows. Each window tracks a **last write timestamp** so the compaction scheduler knows to revisit windows that received new data after initial compaction.

---

## 5. Sharding

### 5.1 Entity Hash Sharding

Within each time window, data is hash-partitioned by entity: `shard = xxhash64(entity_id) % num_shards`. xxHash64 is chosen for speed, good distribution, and consistency with the checksum hash. Each shard is an independent file.

**Shard count is a database-level property.** It is set once when the database is initialized and is fixed for the lifetime of the database — no resharding, no per-table override. Default: **32 shards**, configurable via CLI options at database initialization (`bqlite init --shards N`). There is no BQL statement for creating a database; initialization is a CLI-only operation. The default matches the core count of modern hardware so that one shard-task per core keeps all cores busy during query execution (see execution-model.md Section 9.3).

**Why database-level, not per-table.** Cross-table entity joins (query-language.md Section 19) rely on the fact that a given entity hashes to the same shard in every table. If tables could choose their own shard counts, a join between `events` (32 shards) and `purchases` (16 shards) could require hash resharding at query time, which defeats the merge-join performance model. By fixing shard count at the database level, cross-table entity alignment is guaranteed by construction: shard N of table A and shard N of table B both contain entities with `xxhash64(entity_id) % 32 == N`, so the streaming merge join reads one shard per table with no resharding.

**Database UUID.** A randomly generated UUID is also assigned at database initialization and stored in the database-level `db.json` file (Section 12.1). It never rotates. The UUID serves as the default seed for SAMPLE determinism (query-language.md Section 14.2) and can be used by external tools for database identity.

### 5.2 On-Disk Layout

```
db_root/
  .lock                              ← file lock (POSIX flock / Windows equivalent)
  db.json                            ← database-wide metadata (UUID, engine version)
  table_name/
    manifest.json                    ← per-table manifest (Section 12)
    windows/
      w_000000/                      ← window_id = days since epoch for window start
        shard_00/
          segment_1001.seg
          segment_1002.seg
          tombstones.json
        shard_01/
          segment_1003.seg
          tombstones.json
        ...
        shard_31/
          ...
      w_000030/                      ← next 30-day window
        shard_00/
          ...
```

**Naming conventions.**

- **Windows.** `w_<6-digit zero-padded days-since-epoch>/`. Days-since-epoch is a stable integer that doesn't depend on calendar formatting and sorts lexicographically in creation order. Window 0 is `1970-01-01`.
- **Segments.** `segment_<segment_id>.seg`. The `segment_id` is assigned from the manifest's monotonically-increasing counter; the compaction level is *not* encoded in the filename because a segment's level can change during compaction. The level lives in `SegmentMeta` inside the manifest (Section 12.3).
- **Tombstones.** `tombstones.json` — one file per shard, not per segment. Section 7.5 defines the contents.

Orphaned files (present on disk but not referenced by the manifest) are candidates for GC on startup and after compaction (Section 7.4).

### 5.3 Benefits

**Parallel writes.** Each shard's ingestion path is independent — no cross-shard coordination or locking.

**Parallel reads.** Aggregate queries fan out across all shards in the relevant windows. One thread per shard is the natural parallelism unit.

**Entity locality across windows.** An entity hashes to the same shard in every window. Cross-window entity reconstruction reads `shard_N` from each window — a k-way merge of sorted streams from a single shard per window.

**Distributed-ready.** Shards are the natural unit of distribution. A future distributed version can assign shards to different nodes without changing the format.

### 5.4 Single-Entity Queries

Hash the entity ID to determine its shard. Read only that shard across relevant windows. Entity ID zone maps (Section 11.1) skip segments and row-groups where the entity is absent.

---

## 6. Ingestion (Batch-Only)

### 6.1 No WAL, No Memtable

Each ingest call takes a batch of events and produces segment files directly:

1. Receive batch (from CSV, JSON, Parquet, or programmatic API).
2. Validate against `TableSchema` — type check property values, reject malformed rows.
3. Sort by `(entity_id, timestamp)`.
4. Partition by `(time_window, shard)` based on event timestamp and entity hash.
5. Encode columns, compress, write L0 segment files.

There is no write-ahead log and no in-memory memtable. This eliminates recovery complexity (WAL replay) and simplifies the architecture.

**Tradeoff:** Very small writes produce many tiny L0 files. Compaction handles this. "Please batch your writes" is a reasonable expectation for an analytics engine — the target ingest pattern is bulk loading (CSV/Parquet files), not single-event streaming.

### 6.2 Sequence ID and Batch ID (`__seq_id` / `__batch_id`)

**Sequence ID.** Every row gets a monotonically increasing **64-bit sequence ID** assigned at ingestion time and exposed through the implicit system column `__seq_id`.

- `(entity_id, timestamp, __seq_id)` is globally unique.
- Provides: unique row identity, tiebreaker for same-timestamp events, unambiguous delete targets.
- Assignment: per-table atomic counter. One `fetch_add` per batch reserves a range of IDs.
- Exposed to users only when selected explicitly (`SELECT __seq_id`) — it is excluded from `SELECT *`.
- **Ordering guarantee:** Sequence IDs are monotonically increasing per batch, but concurrent ingest batches may write segments with interleaved ID ranges (batch A reserves [1000, 2000), batch B reserves [2000, 3000), but B may finish writing first). The only guarantee is global uniqueness and within-batch monotonicity. No component should assume sequence ID order correlates with segment creation or visibility order.

**Batch ID.** Every ingest call is assigned a monotonically increasing **64-bit batch ID**, returned to the caller on successful ingest and exposed through the implicit system column `__batch_id`. A batch ID identifies all rows written in a single ingest call.

- Assignment: per-table atomic counter, separate from the sequence ID counter. One `fetch_add` per ingest call. Both the sequence ID counter and batch ID counter are persisted in the per-table manifest as `next_sequence_id` and `next_batch_id` (Section 12.3) so monotonicity survives restarts.
- Each segment file records the batch ID it was produced from (in the segment footer).
- The manifest tracks the batch ID range for each segment.
- **Batch-level deletion:** `DELETE FROM table WHERE __batch_id = N` tombstones all rows from that batch. This is useful for undoing a bad ingest — the caller retains the batch ID from the original insert and can use it to cleanly remove the data. Batch-level tombstones are stored alongside row-level and entity-level tombstones (Section 7.5).
- Batch IDs are excluded from `SELECT *`, but can be accessed explicitly via `SELECT __batch_id` for debugging.

### 6.3 Ingest Sources

| Source | Behavior |
|---|---|
| CSV | Parse rows, infer or validate against schema, convert to `PropertyValue`, then to columnar |
| JSON | Parse objects, validate against schema, same path |
| Parquet | Read Arrow record batches, apply Arrow-to-BqlType width consolidation (Section 7.2 of type-system.md), partition and write segments |
| Programmatic API | Accept `Vec<PropertyValue>` rows or Arrow `RecordBatch` directly |

For Parquet ingest, the Arrow-to-BqlType mapping performs width consolidation (e.g., Int32 -> Int64, Float32 -> Float64, non-UTC timestamps -> UTC nanoseconds) as defined in the type system design.

### 6.4 Schema Evolution on Ingest

Each segment records the schema version it was written with (see `TableSchema.version` in type-system.md). When reading a segment written before a column was added, the scan layer fills the missing column with NULL or the column's default value. No segment rewriting is required for `ALTER TABLE ADD COLUMN`.

---

## 7. Compaction

### 7.1 Strategy: Size-Tiered Within (Window, Shard)

Each `(window, shard)` compacts independently. This means compaction is embarrassingly parallel across shards and windows.

**Scheduling:** Compaction runs as a dedicated background process that activates when query and ingest load is light. It can also be triggered explicitly via API or CLI.

**Trigger:** When the number of L0 segments in a `(window, shard)` exceeds a threshold (default: 4), or the total size of L0 segments exceeds a limit (default: 256 MB).

**Process:**

1. Open all input segments for the `(window, shard)`.
2. K-way merge on `(entity_id, timestamp, __seq_id)`.
3. Apply tombstones — skip deleted rows.
4. Re-encode columns (fresh encoding analysis on the merged data).
5. Write a single output segment to a temp file.
6. Atomically update the manifest.
7. Delete old segments.

### 7.2 Levels

```
L0: Raw ingest segments (~10-50 MB each)
    ↓  merge 4 L0s
L1: ~40-200 MB per segment
    ↓  merge 4 L1s
L2: ~160-800 MB per segment
    ↓  ...
```

Eventually a window stabilizes at one or two large segments per shard. For a 30-day window with moderate volume, a single compacted segment per shard is typical.

### 7.3 Re-Encoding During Compaction

Compaction is an opportunity to re-analyze column distributions and pick optimal encodings. A column that was dictionary-encoded with 50 entries across 4 small L0 segments might benefit from a different strategy after merging. The encoding selector (Section 10.3) runs fresh on each output column chunk.

### 7.4 Atomicity

The manifest is the source of truth for which segments are live. Compaction atomicity:

1. Write new segment to a temp file. `fsync`.
2. Write new manifest to a temp file. `fsync`.
3. Rename new manifest over old manifest. Atomic on POSIX (`rename(2)`). On Windows, use `ReplaceFile` for equivalent semantics. v1 targets POSIX; Windows support is a future concern.
4. Delete old input segments.

If the process crashes at any point:
- Before step 3: the old manifest is still current; temp files are orphans (cleaned up on next startup).
- After step 3: the new manifest is current; old segments may still exist on disk (cleaned up on next startup).

**Orphan cleanup on startup.** When the database is opened, the startup routine scans each `(window, shard)` directory and compares files on disk against the manifest's active segment list. Files not referenced by the manifest and not currently being written (identified by a `.tmp` suffix) are deleted. This handles both crashed compactions (temp output files) and deferred segment deletions (old inputs from a completed compaction).

### 7.5 Deletes

Deletes are tracked via **tombstone files** per shard. Tombstones are *data*, not metadata, and are not stored in the manifest (Section 12.4).

| Granularity | Tombstone field | Use case |
|---|---|---|
| Row-level | `__seq_id` | Delete specific events |
| Batch-level | `__batch_id` | Undo a bad ingest |
| Entity-level | `entity_id` | GDPR right-to-erasure, remove all events for an entity |
| Time-range | `max_ts` | Drop everything before a given timestamp (retention cutoff) |

Each shard has one tombstone file at `<window>/<shard>/tombstones.json`. The contents:

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

    /// Time-range delete: all events with ts < this value are dropped.
    pub time_range_delete: Option<i64>,
}
```

Tombstone lifecycle:

1. A delete operation writes a new tombstone file (not append — write + rename for atomicity, same pattern as manifest updates). Each tombstone file is a complete snapshot of the shard's active tombstones.
2. During reads, the scan layer loads the tombstone file for each `(window, shard)` at scan setup (snapshotted alongside the manifest). The checks are all `HashSet` lookups (`entity_id`, `__seq_id`, `__batch_id`) plus one comparison (`ts < time_range_delete`), applied after column filtering but before rows reach operators.
3. During compaction, tombstoned rows are physically removed from the output segment.
4. After compaction completes for a `(window, shard)` and the output segment no longer contains any tombstoned rows, those tombstones are removed from the tombstone file. Since compaction merges *all* segments within a `(window, shard)`, a single compaction pass resolves every tombstone for that scope — there are no older segments still containing the deleted rows.

**Why not in the manifest.** See Section 12.4 for the full rationale. Briefly:

- Tombstones describe deleted *data*, not which segments exist.
- If tombstones lived in a (table-wide) manifest, a re-inserted entity after a delete would still be suppressed — the wrong semantics for tombstone-as-data.
- Per-shard files keep delete writes local: a delete to shard 3 does not block ingest on shards 0–2, 4–31.

Tombstone files are small (`__seq_id`, `__batch_id`, entity ID values, and one timestamp) and are loaded into memory at query time. Concurrent deletes and query execution within the owning process are safe because each query snapshots the tombstone file at start — a concurrent delete writes a new file that only subsequent queries will see.

### 7.6 Query Snapshots and Compaction

Queries acquire a reference to the current manifest at query start. Compaction publishes a new manifest and only deletes old segments after all in-process queries referencing them have released. This is a lightweight reference-counting scheme:

- Queries increment a per-manifest refcount on start, decrement on completion.
- Compaction waits for the old manifest's refcount to reach zero before deleting its segments.
- No locks on the query read path.

---

## 8. Query Read Path: K-Way Merge

### 8.1 Cross-Window Entity Reconstruction

A query spanning multiple windows (e.g., 6-month retention) requires merging an entity's events across windows. Since the entity hashes to the same shard in every window, this is a merge of sorted streams from `shard_N` across windows.

```
windows/w_020088/shard_03  ──┐    (2025-01-01 + 30d windows)
windows/w_020118/shard_03  ──┤
windows/w_020148/shard_03  ──┼──→  k-way merge on (entity_id, timestamp)  ──→  entity batches
windows/w_020178/shard_03  ──┤
windows/w_020208/shard_03  ──┤
windows/w_020238/shard_03  ──┘
```

### 8.2 Performance Design

**Sequential access.** Each merge input stream is read front-to-back in sorted order. OS readahead works in favor of this pattern.

**Bounded buffers.** 4 MB read buffer per merge input stream. Modern SSDs deliver peak throughput at ≥128 KB I/O sizes, but larger buffers (2-4 MB) reduce syscall overhead and amortize seek latency on spinning disks. With k=6 (30-day windows, 6-month query) and 32 shards, total buffer memory is 192 streams * 4 MB = 768 MB — within the 3 GB query budget. Note that not all shards read simultaneously — the thread pool (sized to num_cores) limits concurrency, so actual buffer usage at any instant is `num_cores * k * 4 MB`.

**Entity-batch handoff.** The merge produces entity batches: "here are all N events for entity X, in timestamp order." The temporal operator consumes the batch, produces a result, the merge advances. Only one entity's data is in memory at a time.

**Lazy column reading.** The merge drives on `entity_id` + `timestamp` only. Property columns are decoded lazily per entity batch, only when downstream operators request them. This dramatically reduces I/O for selective queries — a funnel query that only checks `event_type` and `amount` never decodes `device`, `query`, `tags`, etc.

**Predicate pushdown.** Filters (e.g., `WHERE event_type = 'purchase'`) are evaluated at the per-segment reader before events reach the merge. For dictionary-encoded columns, this is a dictionary lookup that produces a bitset — applied directly to the encoded data without decoding the full column. This can eliminate 90%+ of events before they touch the merge heap.

**Late materialization.** Encoded columns stay in their compressed representation as long as possible — they are only decoded to full Arrow arrays when an operator genuinely needs the expanded values. This applies to several encodings:

- **Dictionary-encoded columns** propagate as Arrow `DictionaryArray` through the pipeline. Filters, grouping, and equality joins all operate on integer codes. A `GROUP BY event_type` with a dictionary-encoded `event_type` groups by code, then resolves the dictionary for display. The full string values are never materialized unless the query explicitly outputs them.
- **RLE-encoded columns** propagate as Arrow `RunEndEncodedArray`. Aggregations that iterate per-entity (where `entity_id` is constant within a run) skip the run without repeated comparison. Counting events per entity is O(runs), not O(rows).
- **Constant-encoded columns** are never decoded at all during scans — the single value is used directly for comparisons and passed through to output.

Late materialization means the scan layer and merge layer operate on compressed representations. Decoding is deferred to the latest possible point — typically the output serialization stage. This reduces memory bandwidth and cache pressure throughout the pipeline.

### 8.3 Single-Entity Lookup Path

For debugging or entity-level inspection:

1. Hash entity ID to determine shard.
2. For each relevant window, use entity ID zone maps to skip segments where the entity's ID falls outside the segment's min/max range.
3. Within matching segments, use row-group-level entity ID zone maps to skip row-groups.
4. Decode only the matching row-groups.

This path is rare (the dominant workload is full scans) but should be fast when needed.

---

## 9. Segment File Format

### 9.1 Design Decision: Own the Container, Own the Encodings

bqlite uses its own segment format and encoding layer — no dependency on Parquet, Vortex, or other container formats.

**Rationale:** Full control over the read path, which is the performance-critical component. No deep transitive dependency on still-evolving libraries. The ability to add domain-specific optimizations (entity-aware encoding, temporal-pattern-aware prefetch) without working around container limitations.

### 9.2 Segment Structure

```
┌──────────────────────────────────────┐
│  Segment Header (fixed size, 6B)     │
│    magic bytes: "BQLT" (4B)          │
│    format version: u16               │
├──────────────────────────────────────┤
│  Row-Group 0                         │
│  ┌──────────────────────────────────┐│
│  │  Column Chunk 0 (encoded)        ││
│  │  Column Chunk 1 (encoded)        ││
│  │  ...                             ││
│  │  Column Chunk N (encoded)        ││
│  └──────────────────────────────────┘│
├──────────────────────────────────────┤
│  Row-Group 1 ... Row-Group K         │
│  (same structure as Row-Group 0)     │
├──────────────────────────────────────┤
│  Segment Footer                      │
│    table schema (serialized)         │
│    schema version: u32               │
│    row count: u64                    │
│    row-group count: u32              │
│    creation timestamp: i64           │
│    sequence ID range: (u64, u64)     │
│    batch ID: u64                     │
│    compaction level: u8              │
│    segment-level dictionaries        │
│    segment-level FSST symbol tables  │
│    per-column-chunk metadata:        │
│      byte offset: u64                │
│      byte length: u64                │
│      encoding type + params          │
│      compression type: u8            │
│      null count: u64                 │
│      row count: u64                  │
│      min/max (zone map values)       │
│    checksum: xxHash64                │
│    footer length: u32                │
│    magic bytes: "BQLT" (4B)         │
└──────────────────────────────────────┘
```

The footer contains all metadata — schema, row counts, column chunk offsets, zone maps, segment-level dictionaries, and FSST symbol tables. To open a segment, the reader reads the last 8 bytes of the file (trailing magic bytes for validation + footer length), computes the footer start position (`file_size - 8 - footer_length`), then reads the full footer in a single I/O. The header contains only magic bytes and format version (6 bytes, fixed size) for quick corruption detection without reading the footer.

### 9.3 Versioning

The format version field (`u16`) in the segment header defines the encoding set, compression options, zone map format, and footer layout for that segment.

- `version: 1` — initial format with the v1 encoding set.
- Readers support all versions up to current. Writers always produce the latest version.
- This provides forward-compatibility: a future bqlite version can read segments written by any prior version without migration.

### 9.4 Per-Column-Chunk Metadata

Stored in the segment footer for each column chunk within each row-group:

| Field | Type | Description |
|---|---|---|
| `byte_offset` | `u64` | Offset from segment start |
| `byte_length` | `u64` | Encoded + compressed size |
| `encoding` | `u8` (enum) | Encoding type (Section 10) |
| `encoding_params` | variable | Encoding-specific: dictionary size, bit width, base value, etc. |
| `compression` | `u8` (enum) | Post-encoding compression (None, LZ4) |
| `null_count` | `u64` | Number of null values |
| `row_count` | `u64` | Number of rows |
| `min_value` | variable | Zone map minimum (type-specific serialization) |
| `max_value` | variable | Zone map maximum |

### 9.5 Checksums

Per-segment **xxHash64** checksum covering the entire segment content (header through footer, exclusive of the checksum field itself). xxHash64 is chosen for speed — checksumming should not be a bottleneck on the read path.

Checksum verification is optional and configurable:
- **Default:** Verify on first read after ingest and after compaction.
- **Paranoid mode:** Verify on every read.
- **Off:** Skip verification (benchmarking, trusted environments).

Per-row-group checksums are deferred to v2. The segment-level checksum catches corruption; row-group-level granularity is only needed if partial-segment recovery is required.

**Segment validation on open.** Before trusting any segment, the reader validates:
1. Magic bytes ("BQLT") at offset 0 and at the last 4 bytes of the file.
2. Footer length (u32 at file_size - 8) is less than file_size, and the computed footer start position is within the file.
3. Footer is parseable and internally consistent (column chunk offsets within file bounds, row counts non-zero).
4. Checksum (if verification is enabled).

A segment that fails validation is treated as corrupt. The behavior depends on context: during startup orphan cleanup, corrupt `.tmp` files are deleted silently; corrupt segments referenced by the manifest produce an error on database open. This handles partial writes from crashes — a half-written segment will fail magic or footer validation.

---

## 10. Encoding Layer

### 10.1 Architecture

Trait-based encoding with cascading support:

```rust
pub trait ColumnEncoder {
    /// Encode an Arrow array into an encoded chunk.
    fn encode(&self, array: &dyn Array) -> Result<EncodedChunk, StorageError>;

    /// Decode an encoded chunk back to an Arrow array.
    fn decode(&self, chunk: &EncodedChunk) -> Result<ArrayRef, StorageError>;
}

pub struct EncodedChunk {
    /// The encoding used.
    pub encoding: EncodingType,
    /// Encoding-specific parameters needed for decoding.
    pub params: EncodingParams,
    /// The encoded data. May contain child chunks (for cascading).
    pub data: Vec<u8>,
    /// Optional child arrays (e.g., dictionary codes after dictionary encoding).
    pub children: Vec<EncodedChunk>,
}
```

**Cascading.** An `EncodedChunk` can contain child arrays that are themselves encodable. Example: dictionary encoding produces a dictionary (encoded as plain) and a codes array (encoded with bit-packing). The segment writer walks the encoding tree and serializes each node. The segment reader deserializes and decodes lazily. Nesting depth is limited to 2-3 levels in practice.

**Near-zero-copy Arrow decode.** The storage format is designed so that decoding produces Arrow arrays with minimal copying:

- **Dictionary encoding** decodes to Arrow `DictionaryArray` — the dictionary buffer and codes buffer are produced directly, not expanded to a flat string array. Operators that only need equality checks (filters, joins) operate on dictionary codes without ever materializing the full strings.
- **RLE** decodes to Arrow `RunEndEncodedArray` — run ends and values are produced directly. Aggregations over RLE columns (e.g., counting events per entity when `entity_id` is RLE) skip repeated values entirely.
- **BitPacking** uses SIMD-accelerated unpacking (via `bitpacking` crate) that writes directly into an aligned Arrow buffer. FastLanes interleaved layout (Section 10.4) enables single-pass SIMD decode with no intermediate buffer.
- **Plain** fixed-width columns decode as a zero-copy pointer cast from the mmap'd or read buffer into an Arrow `Buffer` (when alignment permits). No deserialization — the on-disk bytes *are* the Arrow buffer.
- **Delta** decodes via a prefix-sum pass that writes directly into an Arrow `Int64Array` buffer.

The goal is that the hot decode path allocates exactly one Arrow buffer per column chunk and fills it in a single pass. No intermediate `Vec<T>` that gets copied into an Arrow array.

### 10.2 Encoding Type Enum

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EncodingType {
    Plain         = 0,
    Dictionary    = 1,
    Delta         = 2,
    DoubleDelta   = 3,
    BitPacking    = 4,
    Rle           = 5,
    Constant      = 6,
    Fsst          = 7,
    For           = 8,   // Frame-of-Reference
    PFor          = 9,   // Patched Frame-of-Reference
    Alp           = 10,  // Adaptive Lossless floating-Point
    FreqEncoding  = 11,  // Frequency-reordered dictionary codes
}

/// Post-encoding compression, applied as an optional pass after the primary encoding.
/// Tracked separately from EncodingType in the per-column-chunk metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Lz4  = 1,
}
```

New encodings are added by extending `EncodingType` and implementing the `ColumnEncoder` trait. New compression codecs are added by extending `CompressionType`. The segment format version is incremented when new encodings or codecs are added.

**FastLanes layout** is not an encoding type — it is a data layout strategy applied *within* BitPacking, Delta, and RLE to enable SIMD-friendly access patterns. The `bitpacking` crate already uses a FastLanes-compatible interleaved layout. Future work can extend this to Delta and RLE decoders.

### 10.3 Encoding Selection (v1: Heuristic)

The v1 encoder uses multi-pass heuristics. The selector has access to the column's `BqlType`, position in the sort order, and basic statistics (cardinality, min, max, sortedness, average run length). For each column chunk:

```
Phase 1 — Trivial cases:
  1. If all values are identical → Constant

Phase 2 — String columns:
  2. If String and cardinality / row_count < 0.3 → Dictionary
     - Then: FreqEncoding on codes (if skewed) or BitPacking on codes
  3. If String and cardinality is high → FSST

Phase 3 — Integer / Timestamp columns:
  4. If sorted, monotonic, and deltas are near-constant → DoubleDelta + BitPacking
  5. If sorted and monotonically increasing → Delta + BitPacking
  6. If unsorted but range is narrow (max - min fits in fewer bits) → FOR + BitPacking
     - If > 5% of values are outliers beyond the narrow range → PFOR
  7. If cardinality / row_count < 0.3 → Dictionary + BitPacking on codes

Phase 4 — Float columns:
  8. If values are "round" (few significant digits, e.g. prices, scores) → ALP
  9. Otherwise → Plain + LZ4

Phase 5 — Boolean columns:
  10. If average run length > 2 → RLE
  11. Otherwise → Plain (bitpacked by Arrow, already compact)

Phase 6 — Fallback:
  12. Plain + LZ4 post-compression
```

The encoding selector is designed to be conservative — it picks the first matching rule. A future version can adopt BtrBlocks-style sampling selection (sample ~1% of the chunk, try candidate encodings, pick the winner by estimated compression ratio).

**Expected encodings by column role:**

| Column role | Expected encoding |
|---|---|
| `entity_id` (String, sorted) | Dictionary + FreqEncoding/RLE on codes |
| `timestamp` (Timestamp, sorted within entity) | DoubleDelta + BitPacking (near-constant intervals) or Delta + BitPacking |
| `event_type` (String, low cardinality) | Dictionary + FreqEncoding + BitPacking on codes |
| `__seq_id` (Int, monotonic) | Delta + BitPacking |
| Boolean properties | RLE |
| High-cardinality string properties (URLs, queries) | FSST |
| Numeric properties (clustered range) | FOR or PFOR + BitPacking |
| Numeric properties (round floats) | ALP |
| Numeric properties (random/high entropy) | Plain + LZ4 |

### 10.4 v1 Encodings

#### Plain

Uncompressed, zero overhead. Fixed-width values are stored contiguously. Variable-width values (strings) use a length-prefix encoding.

- **Use:** Fallback for all types, and as the base encoding for types where lightweight encodings don't help.
- **Decode cost:** Zero (memcpy).

#### Dictionary

Map distinct values to integer codes. Store the dictionary (sorted array of distinct values) and the codes array separately.

- **Use:** `event_type`, low-cardinality string properties, any column with cardinality < 30% of row count.
- **Predicate pushdown optimization:** Filter the dictionary values first, produce a bitset over codes. A `WHERE event_type = 'purchase'` against a dictionary-encoded column resolves to a single code comparison — no string comparisons at all. This single optimization is extremely valuable and must be in v1.

#### Delta

Store differences between consecutive values. The first value is stored as-is (the base), then each subsequent value is stored as `value[i] - value[i-1]`.

- **Use:** Timestamps within an entity (monotonically increasing, small deltas). Sequence IDs (strictly monotonic).
- **Typically cascaded** with BitPacking on the residuals.

#### Bit-Packing

Store integers using the minimum number of bits needed. Uses the `bitpacking` crate for SIMD-accelerated packing/unpacking.

- **Use:** Terminal encoding after Dictionary (codes) or Delta (residuals).
- **Bit width:** Determined by the maximum value in the chunk. Stored in encoding params.

#### RLE (Run-Length Encoding)

Store `(value, run_length)` pairs.

- **Use:** `entity_id` column (long runs of the same entity due to sort order), boolean columns, sort-correlated low-cardinality columns.
- **Break-even:** Effective when average run length > 2. The encoding selector checks this.

#### Constant

If every value in a chunk is identical, store the value once. Trivial to implement, surprisingly common — an entity's events within a single row-group all have the same `entity_id`.

- **Decode cost:** Broadcast a single value to fill the array.

#### Double-Delta

Store the delta of deltas: `dd[i] = (value[i] - value[i-1]) - (value[i-1] - value[i-2])`. The first two values are stored as-is.

- **Use:** Timestamps within an entity when events arrive at near-constant intervals (e.g., heartbeats, periodic pings). The double-deltas are near-zero and compress to 1-3 bits per value after BitPacking.
- **Typically cascaded** with BitPacking on the double-delta residuals.
- **When to prefer over Delta:** When the deltas themselves have low variance (near-constant spacing). The encoding selector computes the variance of the first-order deltas; if it is below a threshold, DoubleDelta is chosen over Delta.
- **Reference:** Used in Gorilla (Pelkonen et al., VLDB 2015).

#### FOR (Frame-of-Reference)

Store a base value per block, then encode each value as `value[i] - base` using the minimum bit width needed.

- **Use:** Integer columns with values clustered in a narrow range but not necessarily sorted — e.g., event counts, scores, small numeric properties.
- **Block size:** 128 or 256 values (aligned to SIMD register width).
- **Cascaded** with BitPacking on the offsets.

#### PFOR (Patched Frame-of-Reference)

FOR with an exception list for outliers. Values that fit in the narrow bit width are stored inline; outliers are stored in a separate exception array.

- **Use:** Same as FOR, but when a small fraction (≤5%) of values are outliers that would otherwise force a wider bit width for the entire block.
- **Reference:** Zukowski et al., ICDE 2006.

#### ALP (Adaptive Lossless floating-Point)

Encodes "round" floating-point values (values with few significant digits) by finding an integer mantissa and exponent such that `value = mantissa * 10^exponent`. The mantissa is stored as an integer (then FOR + BitPacking), achieving near-integer compression ratios on float data.

- **Use:** Prices (`9.99`, `29.95`), percentages (`0.15`, `0.92`), scores, and any float column where values have limited decimal precision.
- **Fallback:** For values that don't decompose cleanly (true random floats), ALP falls back to IEEE 754 bit-level compression. The encoding selector checks decomposability on a sample before committing.
- **Reference:** Afroozeh & Leis, SIGMOD 2023.

#### Frequency Encoding

Reorder dictionary codes so that the most frequent values get the smallest codes. After frequency reordering, BitPacking uses fewer bits because common values have small codes.

- **Use:** Applied as an optimization pass on Dictionary-encoded columns when the value distribution is skewed (top 10% of values account for >50% of occurrences).
- **Typically cascaded:** Dictionary → FreqEncoding → BitPacking on codes.

#### FSST (Fast Static Symbol Table)

String-specific encoding that builds a 256-entry symbol table of common substrings (1-8 bytes each), then re-encodes each string by replacing occurrences of those substrings with single-byte codes. Achieves 3-5x compression on typical string data with decode speeds exceeding 3 GB/s.

- **Use:** High-cardinality string columns where dictionary encoding is ineffective — URLs, search queries, referrer strings, user agents, free-text properties.
- **Symbol table:** Built once per segment by analyzing a sample of the full column's strings. Stored in the segment footer alongside dictionaries (Section 3.4), shared by all row-groups. The per-column-chunk encoding params reference the segment-level symbol table by index.
- **Decode:** Single-pass symbol substitution — each byte in the compressed output is either a literal or a symbol table index. No branches, highly SIMD-friendly.
- **Reference:** Boncz et al., "FSST: Fast Random Access String Compression," VLDB 2020.

Behavioral analytics data is string-heavy (entity IDs, event types, URLs, device names, query strings). For high-cardinality strings that fall through dictionary encoding, FSST is significantly better than Plain + LZ4: comparable decode speed but 2-3x better compression, and it preserves random access (LZ4 requires decompressing the entire block).

#### LZ4 (Post-Encoding Compression)

General-purpose compression applied as an optional post-encoding pass. LZ4 is chosen for decode speed over compression ratio — the read path must not be bottlenecked on decompression.

- **Use:** Applied after Plain encoding for columns where lightweight encodings are ineffective (random floats, high-entropy data that FSST and dictionary don't help with).
- **Minimum compression threshold:** LZ4 is only used if the compressed output is at least **10% smaller** than the input (ratio ≤ 0.9). Marginal compression (e.g., saving 2%) is not worth the decode cost — LZ4 decompression, while fast, is not free, and the saved bytes are negligible relative to disk I/O granularity. If the threshold is not met, store the data uncompressed (`CompressionType::None`).

### 10.5 Null Encoding

Null values are encoded as a separate null bitmap (Arrow-compatible) plus dense non-null values. The null bitmap is stored as a prefix of each column chunk. For non-nullable columns (as declared in the schema), the bitmap is omitted entirely — the decoder trusts the schema.

### 10.6 Deferred Encodings (Research / Future)

| Encoding | Description | Reference |
|---|---|---|
| PCodec | Byte-level numerical compression | Manos Athanassoulis group |
| Gorilla | XOR-based float compression for slowly-changing values | Pelkonen et al., VLDB 2015 |
| Prefix suppression | Store only differing suffix for sorted strings | — |
| Cross-column (Corra) | Encode column as residual relative to correlated column | Damme et al., arXiv 2024 |
| ANS/FSE | Asymmetric Numeral Systems entropy coding | Used internally by ZSTD |
| Roaring bitmaps | Compressed bitmap for sets of integers | `roaring-rs` crate |
| BtrBlocks sampling | Sample ~1% of chunk, try candidates, pick winner | Kuschewski et al., SIGMOD 2023 |

### 10.7 Compression Layer

**LZ4 only for v1.** Speed over size. The code is structured to allow other options (ZSTD for cold/archival windows) later, but v1 uses a single codec to minimize complexity.

LZ4 is applied as an optional **post-encoding** compression pass. If lightweight encodings (dictionary, delta+bitpack) are already effective, LZ4 on top adds minimal value. The minimum compression threshold (Section 10.4, LZ4) prevents wasting decode cycles for marginal gains.

---

## 11. Index Structures

### 11.1 Zone Maps

Per-row-group min/max metadata for key columns. Stored as `min_value`/`max_value` fields in the per-column-chunk metadata in the segment footer (Section 9.4).

**Entity ID zone maps** are the highest value. Given `(entity_id, timestamp)` sort order, each row-group covers a contiguous entity ID range. A single-entity lookup can skip directly to the correct row-group by comparing the target entity ID against min/max ranges. For aggregate scans, zone maps are irrelevant (every row-group is read).

**Timestamp zone maps** are useful for window-level pruning (skip entire windows) but less effective within a window — row-groups span many entities with diverse timestamp ranges. Still worth including because the cost is negligible (two values per row-group per column).

**Event type zone maps** have marginal benefit within a segment but are cheap to maintain. They may help skip row-groups that contain only a subset of event types.

Zone maps on all columns are stored in the segment footer metadata (Section 9.4, `min_value`/`max_value` fields).

### 11.2 Bloom Filters and Roaring Bitmaps (Deferred)

Bloom filters on `entity_id` and roaring bitmap indexes on `event_type` are deferred to v2. Zone maps on `entity_id` already provide effective segment-level and row-group-level pruning for single-entity lookups given the `(entity_id, timestamp)` sort order — each row-group covers a contiguous entity range, so min/max comparison is sufficient. Bloom filters add value only when entity distribution within segments is sparse, which is not expected after compaction.

### 11.3 v1 Index Strategy Summary

| Index | Target column | Purpose |
|---|---|---|
| Zone maps | `entity_id` | Skip segments and row-groups for single-entity lookups |
| Zone maps | `timestamp` | Skip entire windows |
| Zone maps | All columns | General min/max pruning (cheap to maintain) |
| Dictionary pushdown | `event_type`, low-cardinality strings | Filter at encoding level, skip rows |

---

## 12. Manifest / Catalog

### 12.1 Purpose

Each **table** has its own manifest file at `<db_root>/<table_name>/manifest.json`. The manifest is the source of truth for that table's state: the authoritative schema, table-level configuration, active segment inventory, compaction state, and the persisted sequence-ID / batch-ID counters.

A database also has a small `<db_root>/db.json` holding truly database-wide properties (database UUID, engine version), alongside the `.lock` file. Per-table configuration like shard count is stored in each table's manifest (Section 12.3), and is required to be identical across all tables in the database so that cross-table joins can rely on shard alignment (Section 5.1).

Per-table isolation has two important consequences:

- **Independent updates.** Ingest and compaction for table `A` never touch table `B`'s manifest, so there is no cross-table contention on the manifest lock.
- **Tombstones are not in the manifest.** Tombstones are data, not metadata, and live in separate files per shard — see Section 12.4 and Section 7.5. Keeping them out of the manifest ensures a re-inserted entity after a delete does not inherit the old delete.

### 12.2 Schema Authority

The manifest holds the *current* table schema. Each segment footer holds the schema *at the time it was written*. The scan layer uses the manifest's schema as the output schema, filling missing columns from older segments with NULL/default values (Section 6.4).

### 12.3 Format

JSON for v1. Each manifest is small enough to load entirely into memory at startup (even with thousands of segments across dozens of tables, each table's metadata is a few MB at most). JSON is human-readable and debuggable, and compilation time is dominated by execution, not metadata parsing.

**Concrete layout** — the fields the manifest serializer writes:

```rust
/// Per-table manifest. Serialized as JSON and updated atomically
/// (write manifest.json.tmp, fsync, rename to manifest.json).
#[derive(Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonically increasing version. Incremented on every manifest update.
    pub version: u64,

    /// Current table schema (latest version after any ALTER TABLE ADD COLUMN).
    pub schema: TableSchema,

    /// Table-level configuration.
    pub config: TableConfig,

    /// Active segments organized by (window, shard).
    pub windows: Vec<WindowManifest>,

    /// Next sequence_id to assign (persisted for monotonicity across restarts).
    pub next_sequence_id: u64,

    /// Next batch_id to assign.
    pub next_batch_id: u64,
}

#[derive(Serialize, Deserialize)]
pub struct TableConfig {
    /// Window size in days. See Section 4.2.
    pub window_days: u32,                    // default 30

    /// Number of shards. Database-wide constraint: every table in the same
    /// database must have the same value (see Section 5.1).
    pub num_shards: u16,                     // default 32
}

#[derive(Serialize, Deserialize)]
pub struct WindowManifest {
    /// Window identifier: days since epoch for the window start.
    pub window_id: u32,

    /// Active segments per shard. Outer index = shard_id.
    pub shards: Vec<Vec<SegmentMeta>>,
}

#[derive(Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Unique segment identifier (used for the filename: segment_{id}.seg).
    pub segment_id: u64,

    /// Compaction level (0 = freshly ingested, higher = more compacted).
    pub level: u8,

    /// Schema version this segment was written with. Used to determine
    /// which columns may be missing (filled with NULL/default at scan time).
    pub schema_version: u32,

    pub row_count: u64,
    pub byte_size: u64,

    /// (min_ts, max_ts) across all events in this segment.
    pub ts_range: (i64, i64),

    /// (min_entity, max_entity) — for entity zone map pruning (Section 11.1).
    /// String entities use lexicographic ordering.
    pub entity_range: (ScalarValue, ScalarValue),

    /// Per-column zone maps (min/max).
    pub column_stats: Vec<ColumnStats>,

    /// Creation timestamp (for compaction scheduling).
    pub created_at: i64,

    /// Batch that produced this segment (Section 6.2).
    pub batch_id: u64,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnStats {
    pub column_name: String,
    pub min: Option<ScalarValue>,
    pub max: Option<ScalarValue>,
    pub null_count: u64,
    /// Optional HyperLogLog estimate of distinct cardinality from ingest.
    pub distinct_count_estimate: Option<u64>,
}
```

**Why zone maps inline.** Zone maps are small — one `(min, max)` pair per column per segment, a few hundred bytes per segment at most. Keeping them inside the manifest lets the planner do segment-level pruning without opening any segment files. On a cold database, opening the manifest already happens at query start, and the zone maps come for free.

**Atomic update.** Write to `manifest.json.tmp`, `fsync` the file, then `rename` over `manifest.json`. Standard crash-safe pattern. No WAL needed for the manifest — the manifest itself is small and the rename is atomic on POSIX (`rename(2)`) and Windows (`ReplaceFile`).

**Reader isolation.** A query takes a snapshot of the manifest at start:

```rust
let manifest: Arc<Manifest> = table.current_manifest();   // atomic load
```

Compaction publishes new manifests by atomically swapping the `Arc`. Old manifests stay alive as long as any running query holds a snapshot. A background GC pass checks for orphaned segment files not referenced by the current manifest and not held by any active query's manifest snapshot, and deletes them once the refcount drops (Section 7.6).

**next_sequence_id / next_batch_id.** Both counters are persisted in the manifest so that ingest after a process restart still produces monotonically increasing IDs. They are loaded from the manifest on startup, incremented atomically during ingest, and persisted on the next manifest update.

### 12.4 Tombstones Are Not in the Manifest

Tombstones live in separate per-shard files (`tombstones.json` next to the segment files) and are updated independently from the manifest. The format is specified in Section 7.5. The reasoning:

1. **Tombstones are data, not metadata.** They describe which rows of which shard's data to skip, not which segments exist.
2. **New ingestion should not inherit old deletes.** If tombstones lived in the (global) manifest, a `DELETE FROM events WHERE entity_id = 'alice'` would continue to suppress alice's events after a subsequent re-insertion — that is the wrong semantics. Per-shard tombstone files that are garbage-collected after the next compaction have the right lifecycle.
3. **Writes are small and local.** Updating a tombstone touches one shard's file, not the table-wide manifest. This avoids blocking other shards' ingest or compaction on a delete operation.

### 12.5 Lifecycle

- **Startup:** Load every table's manifest into memory. Validate against files on disk (orphan cleanup — Section 7.4).
- **Ingest:** After writing new L0 segments, update the table's manifest atomically.
- **Compaction:** After merging segments, write a new manifest (Section 7.4).
- **Query:** Queries snapshot the manifest at query start. The snapshot is immutable for the query's lifetime.

A future version may switch to a binary manifest format if manifest load time becomes a bottleneck, but this is unlikely given the expected size.

---

## 13. Memory Budget Allocation

The default 4 GB memory budget is split across concurrent activities:

| Activity | Default allocation | Notes |
|---|---|---|
| Query execution | 75% (3 GB) | Shared across the fixed query worker pool |
| Compaction | 20% (800 MB) | One compaction at a time per table |
| Ingest buffering | 5% (200 MB) | Sort buffer for batch ingest |

Within query execution, the 3 GB budget is fixed for the process's query worker pool. Only shard-tasks that have actually started work reserve memory against it; queued tasks do not. Admission control and runtime reservations prevent the pool from exceeding the budget (returning a clear error rather than OOM when necessary).

**Per-shard read buffers** for the k-way merge: 4 MB per input stream. Active buffer usage is bounded by `num_concurrent_shard_tasks * k * 4 MB` since the thread pool limits how many shards read simultaneously.

**Compaction memory** is bounded by the size of the input segments being merged. The k-way merge during compaction streams rows and writes output incrementally — it does not materialize the full merged result in memory.

---

## 14. Concurrency

### 14.1 Database Lock File

A lock file at `<db_root>/.lock` (see the directory layout in Section 5.2) prevents multiple processes from opening the same database concurrently. Attempting to open a database when another process holds the lock returns a clear error.

The lock file uses `flock()` on POSIX systems. Queries, ingestion, and compaction may still run concurrently inside the owning process.

### 14.2 Queries and Compaction Within One Process

Compaction does not block query execution inside the owning process. The mechanism (Section 7.6):

1. Queries snapshot the manifest at query start.
2. Compaction writes new segments and publishes a new manifest.
3. Old segments are deleted only after all in-process queries referencing the old manifest have finished.

This is lock-free on the query read path. Queries never wait for compaction; compaction never waits for queries except to defer old-segment cleanup.

### 14.3 Concurrent Ingestion

Multiple ingest calls can run concurrently inside the owning process because they write to independent L0 segment files. The only coordination is:

- Sequence ID / batch ID reservation: atomic `fetch_add` on the per-table counters (Section 12.3).
- **Manifest update: serialized via a per-table manifest lock.** Ingest and compaction for the same table both need to update that table's manifest, so they contend on a table-scoped lock. Different tables never contend with each other — that is the main motivation for per-table manifests (Section 12.1). The actual segment writes are concurrent and lock-free; only the final manifest update is serialized. Manifest updates are fast (write JSON, fsync, rename), so the lock is held briefly and is not a bottleneck.

---

## 15. List and Map Column Encoding

`List` and `Map` columns require special treatment in the columnar format.

### 15.1 List Columns

Encoded as two components:
1. **Offsets array:** N+1 `i64` values (matching Arrow's List offset convention), where `offsets[i]` is the start index in the values array for row i, and `offsets[N]` is the total length. Encoded with Delta + BitPacking (offsets are monotonically increasing).
2. **Values array:** Flattened list elements, encoded with the standard encoding for the element type.

Null lists are represented by a null in the null bitmap; the offsets entry is equal to the next row's offset (zero-length span), but the null bitmap distinguishes "null list" from "empty list".

### 15.2 Map Columns

Encoded as three components:
1. **Offsets array:** Same as List — N+1 `i64` values for entry boundaries.
2. **Keys array:** Flattened string keys, encoded as a String column (Dictionary encoding is usually effective here since map keys repeat across rows).
3. **Values array:** Flattened values, encoded with the standard encoding for the value type.

### 15.3 Nested Type Limitations

- Maximum nesting depth: 2 (e.g., `List(Map(Int))` is allowed; `List(List(List(Int)))` is rejected at schema validation).
- Deeply nested types complicate encoding and provide negligible value in the behavioral analytics domain.

---

## 16. References

1. Kuschewski et al., "BtrBlocks: Efficient Columnar Compression for Data Lakes," SIGMOD 2023
2. Afroozeh & Leis, "FastLanes: Accelerating Encodings for Fun and Profit," VLDB 2023
3. Afroozeh & Leis, "ALP: Adaptive Lossless floating-Point Compression," SIGMOD 2023
4. Boncz et al., "FSST: Fast Random Access String Compression," VLDB 2020
5. Zukowski et al., "Super-Scalar RAM-CPU Cache Compression," ICDE 2006
6. Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series Database," VLDB 2015
7. Damme et al., "Corra: Correlation-Aware Column Compression," arXiv 2024
8. Lemire & Boytsov, "Decoding billions of integers per second through vectorization," Software: Practice and Experience 2015

---

## 17. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Data layout | Columnar row-groups, sorted by `(entity_id, timestamp)` | Standard columnar infra + entity locality from sort order |
| Row-group size | 65,536 rows | Balances encoding efficiency, zone map selectivity, memory footprint |
| Partitioning | Time windows (N days, default 30) | Window pruning for time-range queries; keeps merge k small |
| Sharding | Hash on entity_id (default 32 shards) | Parallel reads/writes, entity locality across windows, one shard per core |
| Ingestion model | Batch-only, no WAL/memtable | Eliminates recovery complexity; appropriate for analytics workload |
| Compaction strategy | Size-tiered within (window, shard) | Simple, effective, embarrassingly parallel |
| Segment format | Custom container + custom encodings | Full control over read path performance |
| v1 encodings | Plain, Dictionary, Delta, DoubleDelta, BitPacking, RLE, Constant, FSST, FOR, PFOR, ALP, FreqEncoding, LZ4 | Comprehensive encoding suite; each encoding targets a specific data pattern |
| Encoding selection | Multi-pass heuristics using statistics | Covers all common patterns; evolve to sampling in v2 |
| Compression | LZ4 only | Decode speed over ratio; ZSTD deferred |
| Index structures | Zone maps (all columns) | Nearly free; entity_id zone maps highly selective given sort order |
| Manifest format | JSON per-table + `db.json` for db-wide settings, atomic rename | Human-readable, debuggable; per-table isolation avoids cross-table lock contention |
| Manifest scope | One manifest per table (Section 12); db-wide UUID in `db.json` | Independent updates per table; tombstones stay out of metadata |
| Checksums | xxHash64 per segment | Fast; per-row-group deferred to v2 |
| Batch ID | 64-bit, returned to caller, supports batch-level deletion | Enables undo of bad ingests |
| Delete mechanism | Per-shard `tombstones.json` with row/batch/entity/time-range deletes; **not** in the manifest | Re-inserted entities get the right semantics; local writes for per-shard deletes |
| Decode strategy | Near-zero-copy to Arrow, late materialization | DictionaryArray/RunEndEncoded propagate through pipeline |
| Concurrency | Lock file for writes, manifest snapshots for reads | Lock-free read path |
| Memory budget split | 75% query / 20% compaction / 5% ingest | Query-dominant workload |

---

## 18. Open Questions for Other Design Docs

These questions are intentionally deferred to the design docs that own them:

- **Query Language (TASK-002):** Exact syntax for `INSERT INTO ... FROM` file paths. How does the `bqlite ingest` CLI command map to the programmatic API? Syntax for `DELETE` statements (row-level, batch-level, entity-level).
- **Execution Model (TASK-003):** How does the k-way merge iterator integrate with the pull-based operator protocol? How are entity batches handed off to temporal operators? Spill-to-disk strategy for intermediate results that exceed the query memory budget. **Entity event limit** — pathological entities with millions of events must be handled in the execution layer (not the storage layer). The execution engine should enforce a configurable per-entity event cap, skipping and flagging entities that exceed it.
- **Sequence Matching (TASK-004):** Does the sequence matcher operate on a single entity's event batch (produced by the merge layer), or does it need to see events across entities? How does predicate pushdown interact with pattern predicates — can `WHERE amount > 50` within a MATCH step be pushed down to the scan layer?
