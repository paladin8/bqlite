# Zero-Copy Encoded Scan and Filter

**Wave**: 5
**Task**: follow-up design (no task assigned yet)
**Status**: draft

## 1. Scope

This note designs the **read-path contract** that keeps segment data in
its encoded form from on-disk bytes through **scan, merge, and filter**.
The target is simple:

- **Uncompressed column chunks:** zero payload copies before the
  scan/filter segment finishes.
- **LZ4-wrapped column chunks:** exactly one unavoidable payload-sized
  copy, the decompression buffer.
- **No row-group-sized dense Arrow arrays** constructed before filter
  decides which rows survive.

The document refines the intent already present in:

- [execution-model.md](../execution-model.md) §3.7 and §3.8
  ("dictionary encoding preservation", selection vectors, late
  materialization),
- [predicate-pushdown.md](predicate-pushdown.md) §7
  (dictionary-code rewriting), and
- [segment-format-v1.md](segment-format-v1.md) /
  [segment-format-v2.md](segment-format-v2.md)
  (the concrete on-disk chunk layouts).

What this note does **not** do:

- Redesign the public query-facing `PhysicalOperator` trait.
- Require a v1/v2 format break. The initial design must work against
  existing segment bytes.
- Promise zero allocations. Selection vectors, small metadata indexes,
  and tile-local scratch are allowed.
- Optimize every downstream operator. The scope ends at the
  **scan/filter** boundary; aggregation, sort, MATCH, and rendering may
  still materialize when needed.

## 2. Problem Statement

The current Wave 2/Wave 4 storage reader normalizes too early:

1. Borrow chunk bytes.
2. Decode the chunk into a dense Arrow array.
3. Splice nulls into a second dense Arrow array.
4. Hand a `RecordBatch` to scan/filter.
5. Let merge and filter allocate again (`interleave`,
   `filter_record_batch`).

That pipeline defeats the "late materialization" story from
`execution-model.md` in exactly the cases where compression should help
most:

- **Dictionary** columns are expanded from bit-packed codes into full
  strings or ints before equality / `IN` filters run.
- **RLE** columns are broadcast into row-wise arrays before filters can
  take advantage of run structure.
- **Constant** columns are expanded even though the filter answer is
  often "all rows" or "no rows" from one scalar compare.
- **Delta / BitPacking / FOR / PFOR / DoubleDelta / ALP** pay a full
  row-group materialization cost even when the query only needs a
  selection vector.
- **Merge** currently interleaves full arrays into new arrays before the
  filter segment can narrow them.

This note makes the copy budget explicit and defines the internal
surfaces needed to hit it.

## 3. Copy Budget

### 3.1 What counts as a copy

A **copy** is any allocation that duplicates a column's value payload at
row-group or batch scale before scan/filter finishes.

Examples that **do count**:

- Expanding dictionary codes to a row-wise value buffer.
- Broadcasting RLE runs into one value per row.
- Rebuilding a flat `Int64Array` / `StringViewArray` for an entire row
  group before filter.
- `interleave`-ing merged rows into fresh column buffers.

Examples that **do not count**:

- Borrowing slices of the segment bytes.
- Building a `SelectionVector`.
- Building small per-row-group metadata, such as a string offset index
  or a dictionary code set.
- Tile-local scratch reused across batches.
- Constructing `Utf8View` headers for **selected rows only** at a
  materialization boundary.

### 3.2 Allowed copy budget

| Chunk kind | Allowed payload copies before scan/filter finishes |
|---|---|
| Uncompressed | 0 |
| LZ4-wrapped | 1 |

Tile-local decode scratch is allowed for codecs whose filter kernels
must reconstruct values in registers or in a small fixed-size buffer,
but that scratch must be:

- bounded by execution tile size,
- reused across batches, and
- dropped once the predicate kernel finishes.

The forbidden shape is "decode the entire row group into a new column
array, then filter."

## 4. Design Overview

The read path becomes a three-layer pipeline:

1. **Pinned chunk bytes.** The reader pins the on-disk bytes for a
   column chunk and, if necessary, decompresses the payload once.
2. **Borrowed encoded views.** The reader exposes a typed
   `EncodedColumnView` over those bytes plus its null bitmap and any
   segment-level side data (dictionary, FSST symbol table).
3. **Selection-first scan/filter.** Scan, merge, and filter operate on
   `EncodedBatch` + `RowSelection`, not on a fully materialized
   `RecordBatch`.

The load-bearing rule is:

> **The scan/filter segment produces a row selection over encoded
> sources, not a copied batch of values.**

Materialization becomes an explicit boundary crossing, not the default
read-path behavior.

## 5. Pinned Chunk Contract

Each opened column chunk is represented by a small pinned object. The
public surface uses Arc-backed byte handles so lifetimes do not leak
across crate boundaries; kernels derive a borrowed view from the pinned
form at dispatch time.

```rust
/// Cheaply cloneable handle over mmap-backed or decompressed segment
/// bytes. One `fetch_add` per chunk open; no per-row refcount traffic.
pub type ArcBytes = Arc<[u8]>;

/// Public, lifetime-free. Lives in `bqlite-core::encoded`.
pub struct PinnedChunk {
    /// Pinned segment bytes or the one-shot LZ4 decompression buffer.
    pub payload: ArcBytes,
    /// Arrow-style validity bits. Never spliced into the values
    /// before filter.
    pub nulls: Option<ArcBytes>,
    /// Encoding-specific params, pinned from the chunk header.
    pub params: ArcBytes,
}

/// Kernel-local borrowed view, constructed at kernel dispatch by
/// calling `PinnedChunk::view`. Never crosses a crate boundary.
pub struct PinnedChunkRef<'a> {
    pub payload: &'a [u8],
    pub nulls:   Option<NullMaskView<'a>>,
    pub params:  &'a [u8],
}

impl PinnedChunk {
    pub fn view(&self) -> PinnedChunkRef<'_> { /* ... */ }
}
```

Rules:

- The null bitmap is always kept **separate** from the values.
- `Dictionary` and `FSST` do not rebuild self-contained `params`
  payloads on the read path; they borrow the already-loaded
  segment-level dictionary / symbol table state directly (held as
  `Arc<DictionaryValues>` / `Arc<FsstSymbols>`).
- LZ4 decompression, when present, produces one owned payload buffer
  that is Arc-wrapped once. Every downstream view points into that
  same buffer. No second decode buffer is allowed before
  materialization.
- Arc handles are a pointer, not a payload copy. The §3 copy budget
  is unchanged: uncompressed chunks have **zero** payload copies;
  LZ4 chunks have **one**, for the decompression buffer.

## 6. Encoded Column IR

### 6.1 Why this layer is not just Arrow

Arrow is the right **materialization target**, but it is not the right
 **read-path IR** for every on-disk encoding:

- v1/v2 fixed-width payloads are not guaranteed to be aligned for
  zero-copy Arrow primitive arrays.
- Arrow has native `DictionaryArray` and `RunEndEncodedArray`, but no
  native array that matches on-disk Delta / FOR / PFOR / ALP / FSST
  payloads.
- Forcing every chunk through Arrow first recreates the row-group-sized
  copies this note is trying to eliminate.

The read path therefore uses a small internal IR. It has two forms: an
owned form that crosses crate boundaries (`EncodedColumn`), and a
kernel-local borrowed form (`EncodedColumnView<'a>`) derived at
dispatch.

```rust
// Public, lifetime-free. Lives in `bqlite-core::encoded`.
pub struct EncodedColumn {
    pub chunk: PinnedChunk,
    pub kind:  EncodedKind,
    pub rows:  u32,
}

pub enum EncodedKind {
    Bool,
    PlainFixed  { width: u8 },
    PlainString,
    Dictionary  { dict: Arc<DictionaryValues>, code_bit_width: u8 },
    Rle,
    Constant,
    Delta,
    DoubleDelta,
    BitPacking,
    For,
    PFor,
    Alp,
    Fsst        { symbols: Arc<FsstSymbols> },
}

impl EncodedColumn {
    pub fn view(&self) -> EncodedColumnView<'_> { /* ... */ }
}

// Kernel-local; constructed by `view()` and never stored across
// crate boundaries or await points.
pub enum EncodedColumnView<'a> {
    Bool(BoolView<'a>),
    PlainFixed(PlainFixedView<'a>),
    PlainString(PlainStringView<'a>),
    Dictionary(DictionaryView<'a>),
    Rle(RleView<'a>),
    Constant(ConstantView<'a>),
    Delta(DeltaView<'a>),
    DoubleDelta(DoubleDeltaView<'a>),
    BitPacking(BitPackingView<'a>),
    For(ForView<'a>),
    PFor(PForView<'a>),
    Alp(AlpView<'a>),
    Fsst(FsstView<'a>),
}
```

Every variant carries the borrowed null mask separately.

Ownership rule: `EncodedColumn` is what operators, schedulers, and any
future async/threaded executor hold. They never need a `'a`. The
borrowed `EncodedColumnView<'a>` exists strictly for the duration of a
kernel call so the hot loop can see `&[u8]` without refcount traffic.

### 6.2 Encoding-by-encoding contract

| Encoding family | Borrowed representation | Filter strategy | Materialization strategy |
|---|---|---|---|
| Bool | Bit-packed payload bytes + null mask | Bitwise predicate on the packed words; produce `RowSelection::Indices` | Materialize **selected rows only** to an Arrow `BooleanArray` |
| Plain fixed-width (`Int`, `Float`, `Timestamp`) | Raw payload bytes + type width + null mask | Unaligned loads or block decode straight from the borrowed bytes into registers / tile scratch | Materialize **selected rows only** |
| Plain string | Borrowed length-prefixed payload + null mask + optional lazy offset index | Sequential compare for scan/filter; build a lightweight offset index when repeated random access is needed | Build `Utf8View` headers for selected rows, reusing the payload bytes |
| Dictionary | Borrowed bit-packed code stream + borrowed segment dictionary + null mask | Rewrite literals to code sets once; filter on codes, not values | Preserve dictionary when the consumer supports it; otherwise decode selected rows only |
| RLE | Borrowed run-end slice + borrowed run values + null mask | Evaluate predicates per run, not per row; expand only the selection, not the values | Preserve `RunEndEncoded` when possible; otherwise materialize selected rows only |
| Constant | Borrowed literal from params + null mask | One scalar compare decides all/none/some-null behavior | Materialize only if a downstream consumer actually needs a flat column |
| Delta / DoubleDelta / BitPacking / FOR / PFOR / ALP | Borrowed compressed payload + params + null mask | Stream decode into tile scratch while producing a selection vector; never build a row-group-wide dense array | Materialize selected rows only |
| FSST | Borrowed compressed payload + borrowed symbol table + null mask | Encode the literal once when possible; otherwise decode values into reusable tile scratch, not a retained batch buffer | Build `Utf8View` for selected rows only |

Two important consequences:

- Some encodings remain encoded all the way through filter
  (`Dictionary`, `RLE`, `Constant`).
- Others still need decode work, but only into **bounded scratch**, not
  into a retained row-group-sized array (`Delta`, `FOR`, `ALP`, `FSST`,
  etc.).

That distinction is intentional: the goal is zero **payload copies**
through scan/filter, not "never decode anything at all."

## 7. Batch and Selection Surfaces

### 7.1 Single-source encoded batch

The storage reader yields one row-group as an owned `EncodedBatch`:

```rust
pub struct EncodedBatch {
    pub row_count: u32,
    pub columns:   Vec<EncodedColumn>,
}
```

No `RecordBatch` exists yet. Kernels that need borrowed access call
`EncodedColumn::view()` per column at dispatch.

### 7.2 Row selection

Filters narrow rows by producing a selection over the batch. To let
RLE-friendly kernels preserve run structure through filter — the
primary source of RLE's compression advantage — the kernel input and
output is a `RowSelection` that can carry either indices or runs.

```rust
pub struct SelectionVector {
    /// Sorted row indices into a single EncodedBatch.
    indices: Vec<u32>,
}

pub struct RowRun {
    pub start: u32,
    pub len: u32,
}

pub enum RowSelection {
    /// Point-selection form. Canonical for indices-friendly kernels
    /// (dictionary, plain, delta, FOR, etc.) and for the final
    /// boundary hand-off into materialization.
    Indices(SelectionVector),
    /// Run-compressed form. RLE kernels produce and consume this
    /// shape natively; run-preserving operations stay on runs
    /// through multiple predicate compositions.
    Runs(Vec<RowRun>),
}
```

Rules:

- **Runs survive composition.** `intersect(Runs, Runs)` stays in
  `Runs` form when the result is representable as runs. Only a
  kernel that cannot natively consume runs (e.g. a non-monotonic
  per-row predicate) is allowed to expand to `Indices`.
- **Kernels pick the output form that matches their source
  encoding.** RLE kernels emit `RowSelection::Runs`. All other
  kernels emit `RowSelection::Indices`. Mixed-encoding conjunctions
  coerce to `Indices` on combine.
- **The boundary materializer accepts either form.** A run-form
  selection hitting the materialization boundary is handled without
  a forced expand-then-materialize round trip: the materializer
  streams selected rows directly out of the encoded source.
- **Selection size is measured in logical rows, not variant
  cardinality.** A `Runs` selection with one run of 10 M rows is
  semantically identical to an `Indices` selection with 10 M
  sorted indices; kernels must not change result semantics based
  on the variant tag.

The semantic contract is always the same: **selected rows, still
backed by the original encoded source**.

### 7.3 Merged scan output

The current merge path copies via `interleave`. The zero-copy path does
not. Instead, the scan operator exposes a stitched view:

```rust
pub struct StitchedBatch {
    pub sources: Vec<EncodedBatch>,
    pub rows:    StitchedRows,
}

pub enum StitchedRows {
    SingleSource { source: u16, selection: Option<RowSelection> },
    Runs(Vec<SourceRun>),
    Indices(Vec<RowRef>),
}
```

Where:

- `SingleSource` is the common fast path.
- `Runs` covers contiguous ranges from one source.
- `Indices` is the fallback for truly interleaved merged output.

The merge stage therefore orders rows by `(entity_id, ts)` **without**
copying column payloads. It only produces row references into encoded
source batches.

## 8. Scan / Merge / Filter Pipeline

### 8.1 Segment-local scan

For each opened segment:

1. Consult zone maps.
2. Open only referenced column chunks.
3. Build borrowed `EncodedColumnView`s lazily.
4. Run whatever predicate kernels are possible directly on those views.
5. Produce an `EncodedBatch + Selection`.

Null handling stays in the null mask. No "dense decode, then splice
nulls back in" path is allowed.

### 8.2 Dictionary rewrite becomes mandatory

`predicate-pushdown.md` already defines `resolve_dictionary_codes`.
Under this design it is not a dormant hook; it is the normal path for
dictionary equality and `IN` filters:

- rewrite once per `(segment, column, predicate)` to a code set,
- scan the bit-packed codes directly,
- build a `SelectionVector`,
- do not materialize the dictionary values unless a surviving row must
  cross a materialization boundary.

If nothing resolves in the segment dictionary, the scan can reject the
entire chunk or row group without decoding values.

### 8.3 Residual row-level filtering moves into the fused scan segment

Row-local filters do not need to wait for a fully merged `RecordBatch`.
To preserve the copy budget, the physical pipeline fuses:

`scan -> merge -> filter`

into one **selection-first segment** whenever the filter references only
scan columns and has no row-shaping behavior.

That means:

- exact row-level filter semantics still hold,
- the filter runs over encoded views and stitched row selections, and
- the old "`filter_record_batch` over a materialized batch" path is no
  longer the default.

The planner may still expose separate logical nodes. This note is about
the physical handoff.

### 8.4 Tombstone application on the encoded path

> **Status:** implemented in TASK-517 as
> [`bqlite_storage::EncodedTombstoneSource`]
> (`crates/bqlite-storage/src/encoded_tombstone.rs`), wired into
> [`bqlite_operators::scan::ScanOperator::open`]
> (`crates/bqlite-operators/src/scan.rs`). On `ScanPath::Materialized`
> the scan keeps using `TombstoneScanWrapper`; on the encoded path each
> tombstoned segment is wrapped with `EncodedTombstoneSource`
> downstream of `KernelAppliedSource`, and a tombstoned single-segment
> scan now drops into `EncodedKWayMergeScan` instead of the
> single-segment fast path. Dictionary-code lowering for entity
> tombstones (final bullet below) is deferred as a follow-up.

`docs/design/storage/deletes.md` and
`docs/superpowers/plans/2026-04-13-tombstone-aware-scan.md` define the
per-query `TombstoneSnapshot`: the query opens each segment with an
immutable view of the shard's `TombstoneFile`, and the scan must drop
any row whose `(entity_id, ts)` is covered by an entity-, range-, or
row-level tombstone before it leaves the scan/filter segment.

The current storage-side implementation wraps `SegmentScan` with
`TombstoneScanWrapper`, which applies `TombstoneFilter::filter_batch`
to every `RecordBatch` returned by `next_row_group`. That wrapper is
correct for the materialized path but does **not** work for the
encoded path — it would force a materialization before filter and
defeat the copy budget.

The encoded path therefore uses an analogous, selection-first
wrapper:

1. The wrapper opens `next_encoded_row_group` on the inner scan and
   inspects the entity-key column's `EncodedColumnView`.
2. It produces a **tombstone selection** — a `RowSelection` whose
   rows are the ones *not* covered by any tombstone — using the same
   `TombstoneFile` predicates the materialized path uses
   (entity-level, (entity, range), (entity, row) granularities).
3. Entity-level tombstones produce `RowSelection::Runs` natively
   because they cover contiguous entity blocks in entity-sorted
   segments.
4. The tombstone selection is intersected with the encoded kernel
   output before the materialization boundary. The boundary
   therefore emits only rows that survive **both** the query's
   predicates and the tombstone snapshot.

Invariants:

- Tombstone application happens *inside* the scan/filter segment,
  before the boundary, on exactly the same code path as the
  predicate kernels. `FilteredBatch` never exposes a tombstoned row.
- The wrapper may reject whole row groups when the tombstone
  coverage subsumes the row group's zone-map entity range — this
  is the encoded analogue of the materialized-path pre-filter
  rejection.
- `TombstoneFile` is consulted once per query per segment; the
  derived code sets / range structures are reused across row
  groups. The per-row hot loop does a run-level or indices-level
  intersect, never a per-row `HashSet` lookup on entity strings
  (entity predicates are lowered to dictionary codes when the
  entity column is dictionary-encoded, via the same
  `resolve_dictionary_codes` path used by predicate pushdown).

### 8.5 Merge does not interleave payloads

The merge stage is allowed to decode only what it needs for ordering.
Concretely, the columns the current `KWayMergeScan` uses as its sort
key:

- `entity_id` (entity key comparison),
- `ts` (timestamp comparison),
- `__seq_id` (tie-break within equal `(entity_id, ts)`).

These three columns are always decoded to values — even when the query
does not project them — because the merge needs real values to
compare. Every other projected column stays encoded and is referenced
by row index through the stitched output.

It is not allowed to rebuild all projected columns into new arrays just
to pass them to filter. The merged result is a stitched row reference
set over encoded source batches.

## 9. Materialization Boundaries

### 9.1 Relationship to `FilteredBatch`

`execution-model.md` §3.8 specifies `FilteredBatch { RecordBatch,
Option<SelectionVector> }` as the stateless-segment IR for downstream
operators. At the time of this design that type is prose-only;
`bqlite-operators` does not yet define it, and the implementation of
this plan introduces it alongside the encoded IR. The encoded IR does
**not** replace `FilteredBatch`; it **layers under** it:

```
SegmentScan ──► EncodedBatch ──► [encoded kernels, selection-first]
            ──► materialization boundary
            ──► FilteredBatch { batch, selection: None }
            ──► downstream stateless ops (project/limit/residual filter)
            ──► PhysicalOperator consumers
```

Consequences:

- `EncodedBatch` is strictly internal to the fused scan/filter segment.
  It never escapes that segment.
- The boundary emits a `FilteredBatch` with `selection: None`, because
  it has already copied only the selected rows and projected columns.
  The execution-model.md §3.8 sparsity-and-push-segment rules continue
  to govern churn *after* the boundary, unchanged.
- `SelectionVector` is the single shared type referenced by both IRs.
  It lives in `bqlite-core::encoded` and is used by both
  `bqlite-operators::FilteredBatch` and the encoded kernels.

**Reconciliation with execution-model.md §3.8.3.** §3.8.3 says
"`FilteredBatch` is an *internal* shape inside a fused push segment
of stateless operators; it never crosses the segment boundary." That
statement describes the *post-boundary* fused push segment. The
scan/filter materialization boundary introduced here is upstream of
that segment: it produces the `FilteredBatch` that §3.8.3's fused
segment takes as input. The two segment boundaries are distinct, and
the §3.8.3 invariant continues to hold for the downstream fused
segment.

### 9.2 Required boundaries

Materialization happens only when a consumer cannot continue on encoded
views or stitched selections:

1. **Crossing out of the fused scan/filter segment** into a consumer
   that only accepts `FilteredBatch` / `RecordBatch`.
2. **Projection of selected rows** when the output schema drops columns
   or changes expression shape. In the layered design this happens at
   the same point as (1) — the boundary emits projected columns
   directly, so a downstream `Project` that only renames/drops columns
   becomes a no-op.
3. **FFI / CLI / pretty-print output** where the public contract is
   still Arrow `RecordBatch`.

**Projection layering note.** `SegmentReader::open_segment` already
takes a projection: column chunks outside the projected set are never
opened and never produce a `PinnedChunk`. That chunk-level pruning is
unchanged by this design. The materialization boundary in §9.2(2) is
concerned with *row-level* projection — shaping the surviving row set
into the declared output schema, including column ordering and Arrow
type coercion. No double-projection: a chunk excluded at
`open_segment` is excluded all the way through, and the materializer
only walks the columns that made it into the `EncodedBatch`.

### 9.3 Rules

- Materialize **selected rows only**.
- Materialize **projected columns only**.
- Strings materialize to `Utf8View`.
- Dictionary and RLE should materialize to native Arrow
  `DictionaryArray` / `RunEndEncodedArray` whenever the consumer can
  use them; flat expansion is the fallback, not the first choice.
- The materializer's output is `FilteredBatch { batch, selection:
  None }`, not a bare `RecordBatch`. This keeps the shared type one
  step wider than the public `PhysicalOperator` surface and lets
  downstream stateless ops stay on their existing contract.

## 10. Format Implications

### 10.1 What works with existing v1/v2 bytes

The initial design does **not** require rewriting existing segment
files. It works by:

- borrowing payload bytes directly for uncompressed chunks,
- decompressing once for LZ4 chunks,
- keeping nulls separate,
- using custom encoded views for compressed numeric/string codecs, and
- delaying Arrow materialization.

### 10.2 Optional future format upgrades

The following would make the zero-copy path cheaper, but they are not
required for the first implementation:

- **Aligned fixed-width payload starts** so plain numeric columns can be
  wrapped in Arrow primitive buffers without unaligned loads.
- **Optional plain-string offset sidecars** so random row access to
  plain string payloads avoids rescanning length prefixes.
- **Optional block skip metadata** for FSST / FOR-family codecs so
  highly selective filters can skip more compressed blocks.

These are v3-format candidates, not prerequisites for this note.

## 11. API Sketch

This note intentionally leaves the public `SegmentReader` and
`PhysicalOperator` traits alone. The new surfaces are internal to the
scan/filter segment:

```rust
pub trait EncodedPredicateKernel {
    /// Evaluate the predicate against `batch`, intersecting the result
    /// with `input` if provided. `out` is written in place; a kernel
    /// may widen it to `Indices` (e.g. when its source encoding is not
    /// run-native) or preserve `Runs` (e.g. RLE kernels over run-level
    /// predicates). Indices form is the safe fallback.
    fn evaluate(
        &self,
        batch: &StitchedBatch,
        input: Option<&RowSelection>,
        scratch: &mut PredicateScratch,
        out: &mut RowSelection,
    ) -> Result<()>;
}

pub trait MaterializeSelected {
    /// Materialize projected columns for the selected rows. Accepts
    /// either `RowSelection` variant so run-form selections from RLE
    /// kernels do not have to be expanded before the boundary.
    fn materialize_selected(
        &self,
        rows: &StitchedRows,
        projection: &Projection,
        scratch: &mut MaterializeScratch,
    ) -> Result<FilteredBatch>; // selection: None at the boundary
}
```

The important design choices are:

- the internal handoff is **encoded batch + selection**, not
  `RecordBatch`,
- the boundary-crossing output is **`FilteredBatch`** (selection
  already collapsed to `None`), so `execution-model.md` §3.8
  continues to describe what happens after, unchanged.

## 12. Staged Implementation Plan

This design is intended to land in small checkpoints:

1. **Copy-budget instrumentation.**
   Add metrics for `bytes_decompressed`, `bytes_materialized_before_filter`,
   `selected_rows_before_materialization`, and `materialized_rows`.
2. **Null-mask preservation.**
   Remove row-group-wide null splicing from the hot path; filters read
   validity bits directly.
3. **Dictionary zero-copy path.**
   Wire `resolve_dictionary_codes`, produce selection vectors from code
   streams, and stop expanding dictionary chunks before filter.
4. **RLE zero-copy path.**
   Preserve run structure through filter and selection.
5. **Selection-first fused scan/filter segment.**
   Replace `filter_record_batch` as the default scan-adjacent filter
   path.
6. **Merge without `interleave`.**
   Emit stitched row references over encoded sources.
7. **Late materialization boundary.**
   Materialize selected rows only when crossing into non-encoded-aware
   consumers.

Each checkpoint should preserve correctness while lowering
`bytes_materialized_before_filter`.

## 13. Open Questions

1. **Downstream encoded-aware consumers (future).**
   `EncodedBatch` / `StitchedBatch` are fused-scan-segment-internal in
   this design. A future task may widen the operator surface so
   specific downstream ops (e.g. aggregates on dictionary keys) can
   consume encoded views directly. That is out of scope here; the
   boundary stays at `FilteredBatch`.
2. **Merge selection representation.**
   Is `Vec<RowRef>` good enough for the interleaved fallback, or do we
   want a run-compressed representation by default?
3. **Plain-string indexing.**
   Is a lazy in-memory offset index sufficient, or is an on-disk sidecar
   worth the format complexity in v3?

## 14. Summary

The design target is not "convert every encoding into a dense Arrow
array more efficiently." The target is:

- borrow encoded bytes,
- evaluate predicates against encoded views or bounded decode scratch,
- carry surviving rows as selections over encoded sources, and
- materialize only at an explicit boundary.

With that contract, the best-case path becomes:

- **uncompressed:** 0 payload copies through scan/filter,
- **LZ4:** 1 payload copy through scan/filter,

and the worst current offenders — dictionary, RLE, constant, and merge
interleave — stop paying row-group-sized materialization costs before
the engine even knows which rows survive.
