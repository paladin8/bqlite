# Zero-Copy Scan/Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the encoded-preserving read path from [docs/design/storage/zero-copy-scan-filter.md](../../design/storage/zero-copy-scan-filter.md): zero payload copies for uncompressed chunks and exactly one payload copy for LZ4 chunks through the fused scan/filter segment.

**Architecture:** Introduce a shared encoded-batch IR plus selection/stitching types, teach the segment reader to produce borrowed encoded column views instead of eagerly materialized Arrow arrays, run scan/filter over selections rather than `RecordBatch`, then replace merge-time `interleave` with stitched row references. Materialization becomes an explicit boundary for non-encoded-aware consumers.

**Tech Stack:** Rust 2021, Arrow 54 (`Utf8View`, `DictionaryArray`, `RunEndEncodedArray` where useful), `bqlite-core` shared IR, `bqlite-storage` encoded readers/materializers, `bqlite-operators` selection-first scan/filter, existing Criterion benches in `benches/wave2/scan.rs`

**Primary design docs:**
- `docs/design/storage/zero-copy-scan-filter.md`
- `docs/design/execution-model.md` §3.7-§3.8
- `docs/design/storage/predicate-pushdown.md`
- `docs/design/storage/reader-trait.md`
- `docs/design/storage/segment-format-v1.md`
- `docs/design/storage/segment-format-v2.md`

---

## Assumptions and Decisions

These are the working assumptions this plan is built around. If any are rejected, re-plan before implementation begins.

1. **One additive trait extension is acceptable.**
   The current `SegmentReader` / `SegmentScan` trait pair only exposes materialized `RecordBatch` row groups. To let `bqlite-operators` consume encoded batches without concrete-type special-casing, we will add a narrow, additive extension hook in `bqlite-core`. This is the one structural decision that should be treated as a shared-file checkpoint and reconciled against `reader-trait.md` in the same commit.

2. **Shared encoded IR lives in `bqlite-core`, uses Arc-backed byte handles, and is lifetime-free.**
   Both `bqlite-storage` and `bqlite-operators` need to name the encoded-batch and stitching types. Keeping them in `bqlite-core` preserves dependency direction and avoids downcasting through storage-specific concrete types. The public IR (`PinnedChunk`, `EncodedColumn`, `EncodedBatch`, `StitchedBatch`) uses an `ArcBytes = Arc<[u8]>` handle and carries no `'a`. Borrowed views (`PinnedChunkRef<'a>`, `EncodedColumnView<'a>`) are constructed inside storage at kernel dispatch and do not cross crate boundaries. Arc cost is one `fetch_add` per chunk open, not per row — the §3 copy budget is unchanged.

3. **`EncodedBatch` layers under `FilteredBatch`; it does not replace it.**
   `FilteredBatch { RecordBatch, Option<SelectionVector> }` is the stateless-segment IR described by `execution-model.md` §3.8. Today it exists only in design prose — neither `FilteredBatch` nor `SelectionVector` is present in `bqlite-operators`; `FilterOperator`/`ScanOperator` operate on `RecordBatch` directly and use `compute::filter_record_batch`. This plan therefore **creates** both types. `SelectionVector` is newly introduced in `bqlite-core::encoded`; `FilteredBatch` is newly introduced in `bqlite-operators`, imports the shared `SelectionVector`, and follows the shape in `execution-model.md` §3.8. The encoded IR is strictly pre-materialization and internal to the fused scan/filter segment; the segment's materialization boundary emits `FilteredBatch { batch, selection: None }` — selection has already collapsed, and projection has already been applied.

   Note on today's scan hot path: there is no separate `FilterOperator` between scan and downstream operators for pushable predicates. Filters that are not pushed to storage live inside `ScanOperator::post_filters` and are evaluated in `apply_post_filters` via `compute::filter_record_batch`. Checkpoint 7's "scan-adjacent filter chooses the encoded path by default" therefore targets that inline post-filter, not a dedicated operator.

4. **The public query-facing `PhysicalOperator` trait stays unchanged in the first wave.**
   The encoded batch path is internal to the scan/filter segment. Downstream consumers still see `FilteredBatch` / `RecordBatch` at explicit materialization boundaries.

5. **Format changes are deferred.**
   The first implementation must work over existing v1/v2 segment bytes. Any future alignment sidecars or string offset sidecars are separate work after the end-to-end pipeline is proven.

6. **Dictionary / constant / RLE are phase 1 wins; compressed numeric codecs are phase 2.**
   The plan deliberately lands the encodings with the highest filter leverage first, then adds tile-scratch kernels for Delta / BitPacking / FOR / PFOR / DoubleDelta / ALP / FSST once the selection-first pipeline is stable.

7. **Tombstone integration is selection-first on the encoded path.**
   `bqlite-storage` today ships `TombstoneFile`, `TombstoneFilter`, `TombstoneSnapshot`, and a `TombstoneScanWrapper` that filters `RecordBatch`es produced by `next_row_group`. The operator/engine wiring (`ScanOperator::tombstone_snapshot` field, `bind.rs` plumbing) is specified by `docs/superpowers/plans/2026-04-13-tombstone-aware-scan.md` and is pending. The encoded path does not reuse `TombstoneScanWrapper` — it would force row-group-wide materialization — but it consumes the same `TombstoneFile` data and `TombstoneSnapshot` lifecycle. This plan introduces an encoded-aware sibling (`EncodedTombstoneScanWrapper`) that produces a `RowSelection` from the tombstone coverage and intersects it with predicate-kernel output before the materialization boundary. See Checkpoint 3 Step 6 and Risks #8 for the sequencing constraint with the tombstone-aware-scan plan.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/bqlite-core/src/storage.rs` | Modify | Add additive scan extension hook and any shared trait plumbing |
| `crates/bqlite-core/src/lib.rs` | Modify | Re-export shared encoded IR types |
| `crates/bqlite-core/src/encoded.rs` | Create | Shared `ArcBytes`, `PinnedChunk`, `EncodedColumn`/`EncodedKind`, `EncodedBatch`, `SelectionVector`, `RowRef`, `SourceRun`, `StitchedBatch` (lifetime-free, Arc-handle form) |
| `crates/bqlite-core/src/metrics.rs` | Modify | Add copy-budget counters and scan/filter metrics |
| `crates/bqlite-operators/src/filter.rs` | Modify | Import shared `SelectionVector` from `bqlite-core::encoded`; keep `FilteredBatch` as the stateless-segment IR |
| `crates/bqlite-storage/src/lib.rs` | Modify | Export new segment encoded-view modules |
| `crates/bqlite-storage/src/segment/encoded.rs` | Create | `PinnedChunk`, `NullMaskView`, reader-side encoded view construction |
| `crates/bqlite-storage/src/segment/materialize.rs` | Create | Materialize selected rows from encoded views into Arrow arrays |
| `crates/bqlite-storage/src/segment/reader.rs` | Modify | Build encoded batches, stop eager null splicing/materialization on new path |
| `crates/bqlite-storage/src/segment/merge.rs` | Modify | Add stitched merge path that avoids `interleave` |
| `crates/bqlite-storage/src/encoding/*.rs` | Modify | Expose borrowed metadata readers / tile-decode helpers instead of row-group materializers only |
| `crates/bqlite-operators/src/lib.rs` | Modify | Export selection-first scan/filter helpers |
| `crates/bqlite-operators/src/scan.rs` | Modify | Drive encoded-batch scan path, stitched merge, explicit materialization boundary |
| `crates/bqlite-operators/src/filter.rs` | Modify | Retain old materialized fallback; stop being the default for scan-adjacent filters |
| `crates/bqlite-operators/src/selection.rs` | Create | Selection combinators and row-stitch helpers |
| `crates/bqlite-operators/src/encoded_filter.rs` | Create | Encoded predicate kernels by encoding family |
| `crates/bqlite-storage/src/encoded_tombstone_scan.rs` | Create | Encoded-path tombstone wrapper: produces `RowSelection` from `TombstoneFile`, intersected with kernel output |
| `crates/bqlite-operators/src/materialize.rs` | Create | Shared `materialize_selected_*` helpers |
| `benches/wave2/scan.rs` | Modify | Add copy-budget and selected-row metrics |
| `tests/tests/wave2_acceptance.rs` | Modify | Assert results unchanged with encoded path enabled |
| `docs/design/storage/reader-trait.md` | Modify | Reconcile trait extension hook and new internal scan path |
| `docs/design/execution-model.md` | Modify | Reconcile actual implementation with `FilteredBatch` / encoded-preserving execution claims |

---

## Checkpoint 1: Shared Encoded IR + Trait Hook + Metrics Skeleton (shared-file changes)

This is the merge-first checkpoint. Every later implementation step depends on the shared types existing and both crates being able to name them.

**Files:**
- Modify: `crates/bqlite-core/src/storage.rs`
- Create: `crates/bqlite-core/src/encoded.rs`
- Modify: `crates/bqlite-core/src/lib.rs`
- Modify: `crates/bqlite-core/src/metrics.rs`
- Modify: `crates/bqlite-operators/src/filter.rs` (rewire `FilteredBatch` to use the shared `SelectionVector`)
- Modify: `docs/design/storage/reader-trait.md`
- Modify: `docs/design/execution-model.md` (add a §3.8.x subsection for the pre-boundary encoded path; leave existing §3.8 prose intact)

### Shared IR and hook

- [ ] **Step 1: Add a narrow additive encoded-scan hook to `SegmentScan`**

Preferred shape:

```rust
fn next_encoded_row_group(&mut self) -> Result<Option<EncodedBatch>> {
    // default fallback: call next_row_group() and wrap the result as
    // an EncodedBatch whose columns are all `MaterializedColumn`
    // fallback variants.
}
```

If that is too invasive for the existing trait, fall back to:

```rust
fn as_any(&self) -> &dyn Any;
```

plus a separate extension trait. The recommendation is the additive method with a default fallback because it keeps scan/operator code cleaner and avoids concrete-type branching.

- [ ] **Step 2: Add shared encoded read-path types in `bqlite-core/src/encoded.rs`**

Define the minimal cross-crate types, all lifetime-free:

- `ArcBytes = Arc<[u8]>` (thin alias; may wrap mmap-derived slices via `Arc<[u8]>::from(...)`)
- `PinnedChunk { payload: ArcBytes, nulls: Option<ArcBytes>, params: ArcBytes }`
- `EncodedColumn { chunk: PinnedChunk, kind: EncodedKind, rows: u32 }`
- `EncodedKind` enum (Bool, PlainFixed, PlainString, Dictionary, Rle, Constant, Delta, DoubleDelta, BitPacking, For, PFor, Alp, Fsst — each carrying any encoding-specific `Arc`-backed side data such as dictionary values or FSST symbols). `Bool` is distinct from `PlainFixed { width: 1 }` because its payload is bit-packed, not byte-per-row, and its kernels operate on packed words.
- `EncodedBatch { row_count: u32, columns: Vec<EncodedColumn> }`
- `SelectionVector` (sorted `Vec<u32>` indices)
- `RowRun { start: u32, len: u32 }`
- `RowSelection` enum: `Indices(SelectionVector)` | `Runs(Vec<RowRun>)`. This is the kernel input/output type. Runs form is the RLE fast path; indices form is the safe fallback. `intersect(Runs, Runs) -> Runs` when representable; mixed-variant combine coerces to `Indices`. See `zero-copy-scan-filter.md` §7.2 for the full contract.
- `RowRef`
- `SourceRun`
- `StitchedRows` (uses `Option<RowSelection>` on the `SingleSource` variant)
- `StitchedBatch { sources: Vec<EncodedBatch>, rows: StitchedRows }`
- `MaterializedColumn` fallback variant for transitional paths

Explicitly **do not** put a `'a` on any public IR type. Borrowed views (`PinnedChunkRef<'a>`, `EncodedColumnView<'a>`) are constructed inside `bqlite-storage` at kernel dispatch via `EncodedColumn::view()` and stay within one kernel call. Arc handles are pointers, not payload copies; the copy budget in `docs/design/storage/zero-copy-scan-filter.md` §3 still holds.

- [ ] **Step 2a: Create `SelectionVector` in `bqlite-core::encoded` and `FilteredBatch` in `bqlite-operators`**

Neither type exists in the codebase yet; both are introduced in this checkpoint. `SelectionVector` lives in `bqlite-core::encoded` (new module). `FilteredBatch { batch: RecordBatch, selection: Option<SelectionVector> }` is defined in `bqlite-operators::filter` (or a sibling `bqlite-operators::filtered_batch` module) and imports the shared `SelectionVector` from the start. Follow the contract in `execution-model.md` §3.8: filter narrows selection, project rewrites to post-selection row count, limit truncates selection (or slices the batch when `selection == None`). Downstream operators do not yet consume `FilteredBatch` on the hot path; that rewire happens in Checkpoint 7. In this checkpoint, `FilteredBatch` exists and is the return type of the (as-yet-unimplemented) materialization boundary.

- [ ] **Step 3: Keep nulls separate in the IR**

Every encoded column variant carries:
- logical row count,
- optional validity bits,
- encoding-specific params/state,
- a handle to the encoded payload bytes.

No variant stores a pre-spliced dense Arrow array.

- [ ] **Step 4: Add copy-budget metrics scaffolding**

Add counters to `bqlite-core::metrics` (or the existing query metrics surface) for:
- `bytes_scanned`
- `bytes_decompressed`
- `bytes_materialized_before_filter`
- `bytes_materialized_after_filter`
- `selected_rows_before_materialization`
- `materialized_rows`

Add docs stating that `bytes_materialized_before_filter == 0` is the target for uncompressed dictionary/RLE/constant paths.

- [ ] **Step 5: Add unit tests for the shared IR**

Test:
- selection vectors preserve sorted ascending indices,
- stitched row runs round-trip row counts,
- default fallback `next_encoded_row_group` preserves the old `next_row_group` semantics for existing fake scans,
- `MaterializedColumn` fallback can represent the pre-existing row-group path without behavior change,
- `PinnedChunk::view()` produces a `PinnedChunkRef<'_>` whose byte slices are equal to the ones backing the Arc handle,
- `FilteredBatch` still builds and behaves correctly after the `SelectionVector` import move.

- [ ] **Step 6: Reconcile docs in the same checkpoint**

- Update `reader-trait.md` to mention the additive encoded-scan hook and clarify that the materialized `RecordBatch` surface remains the compatibility fallback.
- Add a new subsection `execution-model.md` §3.8.x describing the pre-boundary encoded path (`SegmentScan → EncodedBatch → kernels → materialization boundary → FilteredBatch`). Leave the existing §3.8 prose about `FilteredBatch` unchanged: it continues to describe what happens after the boundary. Clarify §3.8.3's "FilteredBatch never crosses the segment boundary" — §3.8.3 is describing the *post-boundary* fused push segment of stateless operators; the materialization boundary introduced in this plan is *inside* the scan/filter segment and emits a `FilteredBatch` that becomes the input to §3.8.3's fused segment.

- [ ] **Step 7: Add a runtime scan-path selector**

Add a `ScanPath { Materialized, Encoded, Auto }` mode on `ScanOperator` (and a session-level default). `Auto` uses `Encoded` when every `post_filters` predicate is supported by an encoded kernel and the input scans can produce `EncodedBatch`, else falls back to `Materialized`. Expose a config / env-var override (suggested: `BQLITE_SCAN_PATH=materialized|encoded|auto`) so CI can run the full test suite in both modes and production can roll back by flipping the default. Ship the selector wired to `Materialized` in Checkpoint 1 (no behavior change yet); Checkpoint 3 enables `Auto`; Checkpoint 7 makes `Auto` the default. Until Checkpoint 8 lands copy-budget gates, CI must run `scripts/local-ci.sh` twice per PR (once forced `Materialized`, once forced `Encoded`) for any PR that touches `bqlite-storage` or `bqlite-operators`.

### Verification

- [ ] `cargo test -p bqlite-core`
- [ ] `cargo test -p bqlite-operators` (filtered-batch unit tests)
- [ ] Re-read `docs/design/storage/reader-trait.md` and `docs/design/execution-model.md` against the staged diff before commit

---

## Checkpoint 2: Reader Substrate - Pinned Chunks, Separate Nulls, No Eager Splicing

This checkpoint makes the storage reader capable of building encoded batches without changing query results yet. The old materialized path remains as the fallback.

**Files:**
- Create: `crates/bqlite-storage/src/segment/encoded.rs`
- Modify: `crates/bqlite-storage/src/segment/reader.rs`
- Create: `crates/bqlite-storage/src/segment/materialize.rs`
- Modify: `crates/bqlite-storage/src/lib.rs`

### Reader changes

- [ ] **Step 1: Add `PinnedChunk` / `NullMaskView` reader-side helpers**

Split current `decode_column_chunk` into:
- chunk parse + payload pinning (produce `PinnedChunk` with `ArcBytes` payload; LZ4 decompression happens here, once, Arc-wrapped),
- encoded view construction (per-encoding `*View<'a>` types constructed via `EncodedColumn::view()` for encoded-aware kernels),
- selected-row materialization.

**Relationship to existing `BorrowedEncodedChunk`:** `crates/bqlite-storage/src/encoding/mod.rs` already defines `BorrowedEncodedChunk<'a> { encoding, params, payload, row_count }`, consumed by every codec's `decode_borrowed(&self, chunk: BorrowedEncodedChunk, ty: &BqlType)`. The new `PinnedChunkRef<'a>` is the byte-level handle that carries a null-mask view; `BorrowedEncodedChunk<'a>` remains the kernel-side decode input for the materialized/fallback path. The kernel dispatcher converts between them: `EncodedColumn::view()` produces a `PinnedChunkRef`, and the materialized-path dispatcher wraps its `payload` / `params` / `row_count` into a `BorrowedEncodedChunk` before calling `decode_borrowed`. Encoded-aware kernels bypass `BorrowedEncodedChunk` entirely and consume the per-encoding `*View<'a>` types directly. Do **not** delete or refactor `BorrowedEncodedChunk` in this plan.

The parse step may decompress LZ4 once into an owned payload buffer, but it must not decode to dense values.

- [ ] **Step 2: Stop using eager null splicing on the new encoded path**

Current `splice_nulls` remains for the compatibility materialized path, but the encoded path carries the validity bitmap separately.

- [ ] **Step 3: Add encoded view constructors for these encodings first**

Required in this checkpoint:
- `Dictionary`
- `Rle`
- `Constant`
- `PlainFixed`
- `PlainString`

Transitional fallback for the rest:
- `Delta`
- `DoubleDelta`
- `BitPacking`
- `For`
- `PFor`
- `Alp`
- `Fsst`

The fallback may still materialize, but it must do so through the new `MaterializedColumn` IR rather than short-circuiting around the encoded pipeline.

- [ ] **Step 4: Add selected-row materializers**

Create `segment/materialize.rs` with helpers that take:
- one `EncodedColumn`,
- a `SelectionVector` or row-run selection,
- output type / projection info,
- reusable scratch.

Materialize only the selected rows. Do not provide a row-group-wide "materialize all rows" helper except as an explicitly test-only convenience.

- [ ] **Step 5: Add reader tests**

Tests to add:
- encoded batch build for dictionary string column carries payload + dictionary and no dense string array,
- nullable column on the encoded path preserves the null bitmap and does not allocate a spliced dense array,
- LZ4 path produces exactly one owned decompression buffer (assert `Arc::strong_count(&chunk.payload) == 1` at construction, then grows as views are cloned), and every column view derived from the same chunk shares that buffer,
- old `next_row_group` compatibility path still returns byte-for-byte equivalent materialized batches,
- differential fixture: for a handful of representative row groups, run both `next_row_group` and `next_encoded_row_group` (materializing the latter through the boundary), and assert the resulting `RecordBatch`es are Arrow-equal. Wire this into `wave2_acceptance` as a gate.

### Verification

- [ ] `cargo test -p bqlite-storage segment::reader`
- [ ] `cargo test --test wave2_acceptance`
- [ ] `cargo clippy -p bqlite-storage --all-targets -- -D warnings`

---

## Checkpoint 3: Dictionary / Constant / Null-Aware Selection-First Filter

This checkpoint lands the first real performance win. Single-segment scans with dictionary predicates should stop materializing full row groups before filter.

**Files:**
- Create: `crates/bqlite-operators/src/selection.rs`
- Create: `crates/bqlite-operators/src/encoded_filter.rs`
- Modify: `crates/bqlite-operators/src/scan.rs`
- Modify: `crates/bqlite-core/src/storage.rs` (if `resolve_dictionary_codes` or related helpers need additive convenience methods only)
- Modify: `crates/bqlite-storage/src/segment/materialize.rs`

### Filter path

- [ ] **Step 1: Implement `SelectionVector` and `RowSelection` combinators**

Required helpers on `SelectionVector`:
- `all_rows(len)`
- `intersect(lhs, rhs)`
- `from_bool_mask(mask)`
- `truncate(limit)`

Required helpers on `RowSelection`:
- `intersect(lhs, rhs)` — `(Runs, Runs) -> Runs` when representable as runs, else coerce to `Indices(Indices)`. `(Indices, Indices) -> Indices`. Mixed variants promote to `Indices`.
- `len()` — logical row count (sum of run lengths or index vector length)
- `into_indices()` / `as_indices()` — expand to `SelectionVector`, used only at kernels/consumers that cannot accept runs
- `from_runs(Vec<RowRun>)` — RLE kernel output constructor
- `truncate(limit)` — used by LIMIT pushdown; preserves variant where possible

Unit tests: `(Runs ∩ Runs)` over fixture patterns with and without run-boundary alignment, `(Indices ∩ Runs)` produces the same rows as expanding first, `into_indices` is a no-op when already `Indices`.

- [ ] **Step 2: Make dictionary rewrite mandatory on the encoded path**

Use `Predicate::resolve_dictionary_codes` for equality / `IN` filters on dictionary-encoded columns.

Required behavior:
- `NoRewrite` -> fall through to residual evaluation
- `EmptySet` -> reject the chunk / row group without value decode
- `Codes(set)` -> scan the code stream directly and emit a `SelectionVector`

- [ ] **Step 3: Implement encoded predicate kernels for**
- dictionary equality / `IN`
- constant equality / inequality / null checks
- null-aware `IS NULL` / `IS NOT NULL`
- plain fixed-width range compares on borrowed bytes
- bool equality (bit-packed payload)

Kernel signature matches `EncodedPredicateKernel::evaluate` in `zero-copy-scan-filter.md` §11: takes `Option<&RowSelection>` input and writes to `&mut RowSelection`. All kernels in this checkpoint emit `RowSelection::Indices` (runs-emitting kernels land in Checkpoint 4 with RLE). Do not materialize the source column to run these kernels.

- [ ] **Step 4: Add a single-segment encoded scan fast path**

When there is one scan input and every pushed predicate is handled by the encoded path:
- read `EncodedBatch` (via the new `next_encoded_row_group` hook on `SegmentScan`),
- produce `SelectionVector`,
- materialize only at the explicit boundary to old consumers.

**Plumbing note:** `ScanOperator` currently holds `merge: Option<KWayMergeScan>` and pulls `RecordBatch` from it. Introduce an encoded iteration path on `KWayMergeScan` (e.g. `next_encoded_row_group() -> Result<Option<EncodedBatch>>`) that short-circuits to the single-source case when there is one input scan; multi-source falls through to the existing `interleave`-based `next_batch` until Checkpoint 5 replaces it with stitched merge. `ScanOperator` gains a runtime mode (`ScanPath::Encoded` / `ScanPath::Materialized`) and routes per-query based on whether every `post_filters` predicate is encoded-supported. `apply_post_filters` is split: encoded-supported predicates run via the new kernels and produce a `SelectionVector`; unsupported predicates fall through to the existing `compute::filter_record_batch` path after materialization.

The old `filter_record_batch` path remains as the fallback for unsupported predicates.

- [ ] **Step 5: Add tests**

Tests to add:
- dictionary string equality on the scan path materializes zero bytes before filter,
- unresolved dictionary literal yields empty result without dense decode,
- nullable dictionary column honors null semantics without null splicing,
- constant-encoded column evaluates all/none behavior without materializing a flat array,
- results match the old materialized filter path exactly.

- [ ] **Step 6: Encoded-path tombstone integration**

Create `crates/bqlite-storage/src/encoded_tombstone_scan.rs` with an `EncodedTombstoneScanWrapper` that decorates a `SegmentScan`'s encoded path. The wrapper's `next_encoded_row_group` pulls the inner `EncodedBatch`, then:

1. **Derives a tombstone `RowSelection`** from the query's `TombstoneSnapshot` for this `(window, shard)`: inspect the batch's entity-key `EncodedColumnView` and timestamp column view; evaluate entity-level, `(entity, range)`, and `(entity, row)` tombstone predicates using the same `TombstoneFile` structures that `TombstoneFilter` uses for the materialized path.
2. **Emits `RowSelection::Runs`** when the coverage is contiguous entity blocks (the common case for entity-level tombstones in entity-sorted segments), else `RowSelection::Indices`.
3. **Lowers entity predicates to dictionary codes** when the entity column is dictionary-encoded, via `resolve_dictionary_codes`, mirroring the pushdown path. This avoids per-row string comparisons.
4. **Returns early with an empty selection** when the tombstone coverage subsumes the whole row group per its zone-map entity range.

Integration points:
- `ScanOperator::open()` wraps each segment's `SegmentScan` with `EncodedTombstoneScanWrapper` when the query's `TombstoneSnapshot` has non-empty tombstones for that `(window, shard)`. The materialized path continues to use the existing `TombstoneScanWrapper`; selection is per `ScanPath` mode.
- The encoded kernels intersect the tombstone `RowSelection` with their predicate result *before* the materialization boundary. `FilteredBatch` at the boundary never contains a tombstoned row.

**Sequencing with the tombstone-aware-scan plan:** `docs/superpowers/plans/2026-04-13-tombstone-aware-scan.md` delivers the `ScanOperator::tombstone_snapshot` field, the `bind.rs` plumbing that hands the per-query `TombstoneSnapshot` to scans, and the engine-side snapshot lifecycle. Those must land first (or be folded into this checkpoint) before `EncodedTombstoneScanWrapper` can be wired. If tombstone-aware-scan is still unlanded when Checkpoint 3 begins, build `EncodedTombstoneScanWrapper` and its unit tests against a synthetic `TombstoneSnapshot` and defer the `ScanOperator::open()` wire-up to the tombstone-aware-scan plan's completion. Do **not** flip `ScanPath::Auto` to default-`Encoded` (Checkpoint 7) until the encoded wrapper is wired end-to-end.

Tests to add:
- entity-level tombstone produces `RowSelection::Runs` spanning the covered entity's rows,
- `(entity, range)` tombstone produces indices (or runs) exactly covering the range,
- row-level tombstone removes the exact rows the materialized `TombstoneFilter` removes (differential test: run both wrappers over the same fixture; assert identical surviving rows),
- tombstone snapshot fully covering a row group returns empty without decoding predicate columns,
- dictionary-encoded entity column lowers to codes and does not materialize strings,
- intersection with a predicate kernel's `RowSelection` produces the expected row set.

### Verification

- [ ] `cargo test -p bqlite-operators scan`
- [ ] `cargo test -p bqlite-storage encoded_tombstone_scan`
- [ ] `cargo test --test wave2_acceptance`
- [ ] Add a benchmark assertion in `benches/wave2/scan.rs` that the pushed dictionary equality path drives `bytes_materialized_before_filter` to zero for uncompressed segments

---

## Checkpoint 4: RLE-Preserving Filter and Selected-Row Materialization

This checkpoint extends the encoded path to RLE instead of flattening runs before filter.

**Files:**
- Modify: `crates/bqlite-storage/src/encoding/rle.rs`
- Modify: `crates/bqlite-operators/src/encoded_filter.rs`
- Modify: `crates/bqlite-storage/src/segment/materialize.rs`
- Modify: `tests/tests/prop_encoding_rle.rs`

### RLE path

- [ ] **Step 1: Expose borrowed run metadata readers from `encoding/rle.rs`**

Add helpers to parse:
- run count,
- borrowed run-end slice,
- borrowed run-value view,

without broadcasting values into a dense Arrow array.

- [ ] **Step 2: Add run-level predicate kernels**

Implement:
- equality / `IN`
- range for fixed-width run values
- `IS NULL` / `IS NOT NULL`

**Output shape (performance-critical):** RLE kernels emit `RowSelection::Runs` *natively*. Do not widen to `RowSelection::Indices` internally — evaluate the predicate on each run value once, then emit a run for every surviving run (and a partial run for the intersection with any input `RowSelection`). Widening to indices is forced only when a downstream kernel in the same conjunction cannot accept runs; `RowSelection::intersect` handles that coercion. The kernel result is never a dense bool array.

- [ ] **Step 3: Materialize selected rows from RLE only at the boundary**

Selected-row materialization may expand runs, but only for the selected rows.

- [ ] **Step 4: Add tests**

Tests to add:
- long-run RLE equality filter stays in run space through filter — assert kernel output is `RowSelection::Runs` and `runs.len()` is O(surviving run count), not O(surviving row count),
- partially selected runs materialize only selected rows,
- RLE nullable columns preserve null handling without pre-splice,
- run-native intersect: `(Runs ∩ Runs)` output is still `Runs` unless coerced,
- mixed conjunction `(RLE predicate) AND (dictionary predicate)` coerces to `Indices` via `RowSelection::intersect` and produces the correct row set,
- results match old dense-RLE decode exactly.

### Verification

- [ ] `cargo test -p bqlite-storage rle`
- [ ] `cargo test -p bqlite-operators encoded_filter`
- [ ] Add a dedicated RLE scan benchmark variant or benchmark note in `benches/wave2/scan.rs`

---

## Checkpoint 5: Stitched Merge Without `interleave`

This is the most invasive structural checkpoint. It should land only after single-segment encoded filtering is already working and measured.

**Files:**
- Modify: `crates/bqlite-storage/src/segment/merge.rs`
- Modify: `crates/bqlite-operators/src/scan.rs`
- Modify: `crates/bqlite-operators/src/selection.rs`
- Modify: `crates/bqlite-storage/src/segment/materialize.rs`

### Merge changes

- [ ] **Step 1: Replace merged payload `interleave` with stitched row references**

Introduce `StitchedBatch` output from merge:
- `SingleSource`
- `Runs`
- `Indices`

The merge keeps ordering information as row references into encoded source batches.

- [ ] **Step 2: Restrict decode in merge to sort-key extraction only**

Merge may decode:
- entity key
- timestamp
- tie-break metadata

It may not rebuild every projected column.

- [ ] **Step 3: Teach scan filter to consume stitched rows**

Encoded predicate kernels must accept both:
- single-source selections (via the `SingleSource { selection: Option<RowSelection>, .. }` stitched variant — runs form survives when the upstream kernel produced it),
- stitched merged rows (`Runs` / `Indices`).

Start with the simplest path:
- dictionary / constant / plain fixed-width predicates over stitched rows,
- fallback materialize-then-filter for any unsupported stitched case.

- [ ] **Step 4: Add tests**

Tests to add:
- multi-segment merge of dictionary columns no longer uses `interleave` for projected payload columns,
- merged selection vectors produce the same row order and result set as the old path,
- single-source merged path still stays on the cheaper `SingleSource` representation.

### Verification

- [ ] `cargo test -p bqlite-storage segment::merge`
- [ ] `cargo test --test wave2_acceptance`
- [ ] Compare old/new merge benchmarks in `benches/wave2/scan.rs`

---

## Checkpoint 6: Tile-Scratch Kernels for Delta / BitPacking / FOR / PFOR / DoubleDelta / ALP / FSST

This checkpoint removes the biggest remaining pre-filter materialization sources among compressed numeric and string codecs.

**Files:**
- Modify: `crates/bqlite-storage/src/encoding/delta.rs`
- Modify: `crates/bqlite-storage/src/encoding/double_delta.rs`
- Modify: `crates/bqlite-storage/src/encoding/bitpacking.rs`
- Modify: `crates/bqlite-storage/src/encoding/for_encoding.rs`
- Modify: `crates/bqlite-storage/src/encoding/alp.rs`
- Modify: `crates/bqlite-storage/src/encoding/fsst.rs`
- Modify: `crates/bqlite-operators/src/encoded_filter.rs`

### Compressed numeric/string kernels

- [ ] **Step 1: Expose tile decode helpers instead of row-group materializers**

For each codec, add helpers that decode a row range or block range into reusable scratch buffers.

- [ ] **Step 2: Add range/equality kernels on tile scratch**

Implement encoded predicate kernels that:
- decode one tile / block into scratch,
- evaluate the predicate,
- append selected row ids,
- discard scratch before the next tile.

- [ ] **Step 3: FSST literal fast path**

Where possible:
- encode the literal once to token form,
- compare in compressed or partially decoded form.

If that is not yet practical, decode per tile into scratch only. Do not retain a row-group-wide decoded string array.

- [ ] **Step 4: Add tests**

Tests to add:
- compressed numeric filter path does not allocate a row-group-wide dense array,
- FSST path remains within the copy budget for uncompressed and LZ4-wrapped chunks,
- results match old decode-then-filter path exactly across randomized fixtures.

### Verification

- [ ] `cargo test --test prop_encoding_delta`
- [ ] `cargo test --test prop_encoding_double_delta`
- [ ] `cargo test --test prop_encoding_bitpacking`
- [ ] `cargo test --test prop_encoding_for`
- [ ] `cargo test --test prop_encoding_fsst`

---

## Checkpoint 7: Explicit Materialization Boundary + Fallback Cleanup

This checkpoint consolidates the new path so downstream operators get selected rows only, and the old eager-scan path becomes a compatibility fallback instead of the default.

**Files:**
- Create: `crates/bqlite-operators/src/materialize.rs`
- Modify: `crates/bqlite-operators/src/scan.rs`
- Modify: `crates/bqlite-operators/src/filter.rs`
- Modify: `crates/bqlite-storage/src/segment/materialize.rs`
- Modify: `docs/design/execution-model.md`

### Boundary behavior

- [ ] **Step 1: Add one shared `materialize_selected_*` path**

Every crossing from encoded scan/filter to old consumers must go through one implementation, not ad hoc array rebuilding in multiple operators. The return type is `FilteredBatch { batch, selection: None }`, not a bare `RecordBatch`: the boundary has already collapsed the selection and applied projection, so downstream stateless ops keep their existing `FilteredBatch` contract. `execution-model.md` §3.8's sparsity / push-segment materialization triggers apply *after* the boundary, unchanged.

- [ ] **Step 2: Make scan-adjacent filter choose the encoded path by default**

`filter_record_batch` remains:
- for the compatibility path,
- for non-scan children,
- for unsupported encoded predicates.

But scan + pushed filter should prefer encoded selection-first execution.

- [ ] **Step 3: Update execution-model docs to match actual behavior**

Once the implementation lands, reconcile any residual prose that still describes the old materialized filter path as the mainline. Keep the existing §3.8 `FilteredBatch` rules intact — they describe the post-boundary world. Extend the §3.8.x subsection (added in Checkpoint 1 Step 6) with any details that only become knowable after implementation (e.g. the exact set of predicates supported on the encoded path).

- [ ] **Step 4: Add acceptance and regression tests**

Tests to add:
- `scan -> filter -> project` materializes only selected rows,
- unsupported predicates still fall back correctly,
- no correctness regressions around null semantics, ordering, or projection.

### Verification

- [ ] `cargo test`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`

---

## Checkpoint 8: Benchmarks, Metrics, and Copy-Budget Gates

This checkpoint proves the refactor is paying for itself and prevents regressions.

**Files:**
- Modify: `benches/wave2/scan.rs`
- Modify: `benches/common/mod.rs`
- Modify: `docs/design/storage/zero-copy-scan-filter.md` (if measured reality changes any assumptions)

### Bench and gate work

- [ ] **Step 1: Report copy-budget metrics in scan benchmarks**

Add benchmark outputs for:
- `bytes_decompressed`
- `bytes_materialized_before_filter`
- `bytes_materialized_after_filter`
- `selected_rows_before_materialization`

- [ ] **Step 2: Add reference-mode assertions**

Suggested floor/ceiling checks:
- pushed dictionary equality on uncompressed segments:
  `bytes_materialized_before_filter == 0`
- same query on LZ4 segments:
  `bytes_materialized_before_filter == 0` and
  `bytes_decompressed == payload_bytes`
- RLE equality path:
  zero pre-filter materialization on uncompressed segments

- [ ] **Step 3: Compare throughput to old path**

Record before/after measurements for:
- full scan
- pushed dictionary equality
- RLE-heavy filter
- multi-segment merged scan

The goal is not necessarily to improve every full-scan number immediately, but the encoded-aware filtered paths should show clear wins and a lower ratio of `bytes_materialized_before_filter` to `bytes_scanned`.

### Verification

- [ ] `cargo bench --bench scan`
- [ ] Reconcile benchmark notes with `docs/design/storage/zero-copy-scan-filter.md`

---

## Recommended Execution Order

If implementation work is split across agents or checkpoints, use this order:

1. Checkpoint 1 - shared IR and trait hook
2. Checkpoint 2 - reader substrate
3. Checkpoint 3 - dictionary / constant / null-aware filter
4. Checkpoint 4 - RLE
5. Checkpoint 7 - explicit materialization boundary cleanup
6. Checkpoint 5 - stitched merge
7. Checkpoint 6 - compressed numeric / FSST tile kernels
8. Checkpoint 8 - benchmark gates

The deliberate reordering is that Checkpoint 7 can land before the stitched merge work if we want to prove the single-segment selection-first path first.

---

## Rough Effort Sizing

Estimates are for a single focused engineer, in engineer-days, including tests but excluding the Checkpoint 8 benchmark gates when they trail the code by more than one checkpoint. Treat as planning heuristic, not a commitment.

| Checkpoint | Estimate | Notes |
|-----------|----------|-------|
| 1 — Shared IR + trait hook | 2–3 | Mostly mechanical; the ScanPath selector and `FilteredBatch` create are the meaningful parts. |
| 2 — Reader substrate | 5–8 | Splitting `decode_column_chunk` inside a 4335-line file is the risk. Differential test is non-negotiable. |
| 3 — Dictionary / constant / null filter | 4–6 | Dictionary kernels + wiring `Auto` to actually choose `Encoded`. |
| 4 — RLE | 4–5 | Run-native `RowSelection::Runs` output (Option B); preserves run structure through filter for the RLE compression payoff. |
| 5 — Stitched merge | 8–12 | Most invasive. Consider splitting into "single-source short-circuit" and "true stitched merge" sub-checkpoints. |
| 6 — Compressed numeric / FSST tiles | 4–6 | Per-codec but each is fairly mechanical once the kernel dispatcher is solid. |
| 7 — Boundary + fallback cleanup | 3 | Small if earlier checkpoints held discipline. |
| 8 — Bench gates | 2 | Largely wiring `AtomicMetrics` snapshots into bench output. |

Total: ~30–43 engineer-days. Merge and reader-substrate dominate; everything else is predictable.

---

## Risks and Watchpoints

1. **Trait churn risk.**
   The encoded-scan hook is the one shared-file decision most likely to ripple. Land it first, document it in `reader-trait.md`, and keep the compatibility fallback in place.

2. **Lifetime / ownership discipline.**
   The shared IR is deliberately lifetime-free (Arc handles at the boundary, borrowed views inside kernels). In review, reject any PR that leaks `'a` into `bqlite-core` or `bqlite-operators` public signatures, and any PR that introduces a second decode buffer for LZ4 chunks. The `Arc::strong_count` test from Checkpoint 2 Step 5 is the regression gate.

3. **IR layering discipline.**
   `EncodedBatch` is fused-scan-segment-internal. `FilteredBatch` is the post-boundary IR. Reject any PR that surfaces `EncodedBatch` outside the scan/filter segment or that drops the `selection: None` invariant at the boundary. `SelectionVector` is a single shared type living in `bqlite-core::encoded`; both IRs import it.

4. **Merge complexity.**
   Stitched rows are the highest algorithmic risk in the plan. Do not block the single-segment wins on multi-segment perfection.

5. **Benchmark ambiguity.**
   Track copy-budget counters alongside throughput. Otherwise a refactor that shifts work around may look neutral in rows/sec while still eliminating materialization costs.

6. **Doc drift.**
   Reconcile `execution-model.md` and `reader-trait.md` in the same checkpoints as the code changes. Specifically: Checkpoint 1 adds a §3.8.x subsection for the pre-boundary encoded path but leaves the existing §3.8 `FilteredBatch` rules intact.

7. **Compaction read path shares `decode_column_chunk`.**
   The compaction rewriter reads encoded column chunks through the same reader surface. The refactor in Checkpoint 2 must leave compaction on a working path — the materialized fallback is acceptable if encoded-aware compaction is out of scope here. Compaction correctness is regression-tested by `wave2_acceptance`, but every PR that touches `reader.rs` or `encoding/*.rs` should also run `cargo test -p bqlite-storage compaction` explicitly.

8. **Tombstone application must move with the path, and the wrapper is distinct.**
   Any row set that leaves scan/filter has had its per-query tombstone snapshot applied. The existing `crates/bqlite-storage/src/tombstone_scan.rs` wraps `SegmentScan` and filters `RecordBatch` — correct for the materialized path, unusable for the encoded path (it would force row-group-wide materialization and defeat the copy budget). Checkpoint 3 Step 6 introduces an encoded-path sibling (`encoded_tombstone_scan.rs`) that produces a `RowSelection` and intersects it with kernel output before the boundary. The `ScanOperator::open()` callsite selects the wrapper matching `ScanPath`. **Sequencing:** `docs/superpowers/plans/2026-04-13-tombstone-aware-scan.md` delivers the engine/operator wiring of `TombstoneSnapshot` into `ScanOperator`; until that lands, `EncodedTombstoneScanWrapper` can be built and unit-tested against synthetic snapshots but not wired into the scan open path. **Gating:** Checkpoint 7's default flip to `ScanPath::Auto` is blocked until both (a) `EncodedTombstoneScanWrapper` is wired end-to-end, and (b) the differential test between materialized `TombstoneScanWrapper` and the encoded wrapper passes on `wave2_acceptance` fixtures.

---

## Final Verification for the Whole Plan

When the full sequence is complete:

- [ ] `scripts/local-ci.sh`
- [ ] `cargo bench --bench scan`
- [ ] Compare old/new `bytes_materialized_before_filter` on dictionary, RLE, and LZ4-filtered workloads
- [ ] Run a subagent code review focused on:
  correctness of null semantics,
  ordering stability through stitched merge,
  unsupported-predicate fallback behavior,
  copy-budget regressions,
  benchmark coverage
