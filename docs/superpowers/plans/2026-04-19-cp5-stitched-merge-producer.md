# CP5: Stitched Merge Without `interleave` — Implementation Plan

**Checkpoint**: 5 of the zero-copy scan/filter roadmap (`docs/design/storage/zero-copy-scan-filter.md`)
**Status**: plan, ready for implementation
**Depends on**: CP1–CP4 (shipped). Consumer `materialize_stitched` already in `crates/bqlite-operators/src/materialize.rs`.

---

## 1. Goal

Replace the multi-segment encoded fallback to `KWayMergeScan` with a new merge producer that emits `StitchedBatch` — zero column-payload copies, with decoded values only for the two sort-key columns (`entity_id`, `ts`). The existing consumer (`materialize_stitched`) remains unchanged; the scan operator wires the producer in as a drop-in replacement for the multi-segment encoded path that today falls through to the materialized merge. After CP5, requesting `ScanPath::Encoded` against any number of segments stays on the encoded pipeline end-to-end.

## 2. Design choice summary

**Integration shape (A/B/C): Option C — scan operator wraps per-source cursors**

The new merge type in `bqlite-storage` is predicate-free. It owns a vector of per-source cursors, where each cursor is a thin iterator of `(EncodedBatch, RowSelection)` pairs supplied by the scan operator (which already owns the per-source `apply_encoded_eq_all` logic for the single-segment path). Rationale:

- Mirrors the existing single-segment path at `crates/bqlite-operators/src/scan.rs:~411-459` exactly.
- Keeps `bqlite-storage` free of any `encoded_filter` / `CompiledExpr` dependency (`bqlite-operators` already depends on `bqlite-storage`, not the other way around — option B would invert that for zero benefit).
- Merge becomes a pure "pick next row by sort key across N encoded cursors" operator. No kernel dispatch inside it.

**Sort-key decoding strategy: eager per-batch**

When a source's current `EncodedBatch` is loaded, decode only its `entity_id` and `ts` columns to dense Arrow arrays via `materialize_encoded_column`. Every other column stays pinned. Reuse the existing `EntityKeyValue::extract` / `extract_ts_nanos` machinery. This honors design-doc §8.5 ("only sort-key columns decoded") and keeps the HeapEntry / `Ord` implementation identical to `KWayMergeScan`.

**Output batch shape**

`DEFAULT_STITCHED_BATCH_ROWS = 65_536` matching the materialized path. Emit when the budget is reached or all cursors drain.

**CP5 ships `Indices`-only. Defer all `SingleSource` / `Runs` compaction to a follow-up.**

- Always correct: `StitchedRows::Indices(Vec<RowRef>)`. Pick-order is preserved, tie-break is deterministic, consumer already handles it. This is the CP5 mandate.
- **Do NOT emit `StitchedRows::SingleSource { selection: None }` in CP5.** `materialize_stitched` routes that to `materialize_selected(src, None, ...)` which decodes the **full source batch** — correct only when the merge drained 100% of that batch's selected rows AND the upstream kernel pass produced an all-rows selection. Detecting that reliably is fiddly; a row-budget boundary or a kernel-narrowed selection breaks it silently. Ship `Indices` uniformly in CP5 and file `SingleSource { selection: Some(...) }` compaction as a follow-up that can emit either `RowSelection::Indices` (subset of source) or `RowSelection::Runs` (preserving RLE-kernel shape) explicitly.
- **Do NOT flatten `RowSelection::Runs` to a `Vec<u32>` at cursor-load time.** CP4 RLE kernels emit `Runs` specifically to preserve run shape through filter; flattening in the merge discards that shape and forces `take`-per-row downstream. Keep the native `RowSelection` on the cursor and advance with a small walker (see §3's `SelectionCursor`). Flattening is also a memory blow-up on long runs (a 65_536-row all-rows selection becomes a 256 KB `Vec<u32>`).
- Defer `StitchedRows::Runs` emission to a follow-up alongside `SingleSource` compaction.

## 3. File-by-file change list

### `crates/bqlite-storage/src/segment/merge.rs`
- Add a new public type `EncodedKWayMergeScan` alongside (not replacing) `KWayMergeScan`.
  - Fields: `sources: Vec<EncodedCursor>`, `schema: Arc<ArrowSchema>`, `entity_key_col: usize`, `ts_col: usize`, `batch_target_rows: usize`, `active_heap: BinaryHeap<Reverse<EncodedHeapEntry>>`, `exhausted: bool`.
  - `EncodedCursor`: wraps `Box<dyn EncodedBatchSource>` + current `Option<LoadedEncodedBatch>` + `scan_exhausted: bool`.
  - `LoadedEncodedBatch`: `{ batch: EncodedBatch, selection: SelectionCursor, entity_arr: ArrayRef, ts_arr: ArrayRef }`. No `Vec<u32>` flattening.
  - `SelectionCursor`: small walker that advances through a `RowSelection` without materializing indices up front. Two inner shapes mirroring `RowSelection`:
    - `Indices { indices: Vec<u32> from SelectionVector::into_vec(), pos: usize }` — cheap; the `SelectionVector` already owns a `Vec<u32>` we can `std::mem::take` out of the owned `RowSelection`.
    - `Runs { runs: Vec<RowRun>, run_idx: usize, run_offset: u32 }` — walks runs in place: current row is `runs[run_idx].start + run_offset`; `advance()` increments `run_offset` and moves to the next run when exhausted.
    - `fn current(&self) -> Option<u32>`; `fn advance(&mut self)`; `fn is_done(&self) -> bool`. Writing a native walker is ~30 lines and keeps RLE-emitted runs intact (no `O(total_rows)` flattening).
  - `EncodedBatchSource` trait: `fn next(&mut self) -> Result<Option<(EncodedBatch, RowSelection)>>`. This is the injection point from the scan operator.
  - `EncodedHeapEntry`: same structure as existing `HeapEntry` (`scan_idx, row_idx, entity_key: EntityKeyValue, ts_nanos: i64`) — reuse `EntityKeyValue` and `extract_ts_nanos` from the same module. Tie-break comparison order `entity_key → ts_nanos → scan_idx → row_idx` is preserved (matches existing `HeapEntry::cmp`), so lower-scan-idx wins on equal `(entity_id, ts)`.
  - `new(sources, schema, entity_key_col, ts_col) -> Result<Self>` and `with_batch_size(...)` constructors that validate `entity_key_col`/`ts_col` types the same way `KWayMergeScan` does (reuse `validate_key_types`).
  - `fn next_stitched_batch(&mut self) -> Result<Option<StitchedBatch>>` — the main driver:
    1. Reload any cursor whose `LoadedEncodedBatch` is empty. Reload loops past empty selections (skip fully-filtered batches).
    2. When loading, decode only the two sort-key columns of the new batch via `materialize_encoded_column` into `entity_arr` / `ts_arr`. Build a `SelectionCursor` from the incoming `RowSelection` — **no flattening**. Empty selections fall through to another reload.
    3. Push a heap entry pointing at `selection.current().unwrap()` of that cursor. If the cursor is already done (empty), skip the push.
    4. Drain the heap up to `batch_target_rows`, collecting `RowRef { source, row }`. After each pop, `cursor.selection.advance()`; re-push the heap entry with the new `current()` row if `!cursor.selection.is_done()`.
    5. At emit time, build the `StitchedBatch`: the `sources: Vec<EncodedBatch>` is cloned from each cursor's currently-loaded batch (cheap — `EncodedBatch` holds `Arc`-backed `PinnedChunk`s; `Clone` is refcount bumps). **CP5 always emits `StitchedRows::Indices(picks)`.**
    6. Clear fully-drained cursors (`selection.is_done()`) so step 1 reloads them next call.
- Keep `KWayMergeScan` untouched. It stays the materialized path.
- Add `DEFAULT_STITCHED_BATCH_ROWS` constant (value 65_536) — do not alias to `DEFAULT_MERGE_BATCH_ROWS`. Doc-comment: "Kept separate from `DEFAULT_MERGE_BATCH_ROWS` so the encoded-merge emit size can diverge from the materialized merge's without cross-path coupling."
- Add internal unit tests directly in the module (see §5).

### `crates/bqlite-storage/src/segment.rs`
- Add `pub use merge::{EncodedKWayMergeScan, EncodedBatchSource, DEFAULT_STITCHED_BATCH_ROWS};` as needed. No new module file.

### `crates/bqlite-storage/src/lib.rs`
- Re-export `EncodedKWayMergeScan`, `EncodedBatchSource` at crate root so `bqlite-operators::scan` can name them without a deep path. Follow the pattern of existing `pub use segment::materialize::…` lines.

### `crates/bqlite-operators/src/scan.rs`
- Add a new field on `ScanOperator`: `encoded_merge: Option<EncodedKWayMergeScan>`, mutually exclusive with `merge` and `encoded_scan`.
- In `open()` — explicit branch structure replaces the existing lines ~508-526:
  ```text
  if scan_path != Materialized && scans.len() == 1:
      encoded_scan = Some(scans.pop().unwrap())           // existing bypass, unchanged
  else if scan_path != Materialized:                       // scans.len() >= 2
      encoded_merge = Some(EncodedKWayMergeScan::new(
          scans.into_iter().map(|s| KernelAppliedSource::boxed(s, shapes.clone(), types.clone())).collect(),
          arrow_schema.clone(), entity_col, ts_col))
  else:                                                    // Materialized (any segment count)
      merge = Some(KWayMergeScan::new(scans, ...))
  debug_assert!(
      (encoded_scan.is_some() as u8 + encoded_merge.is_some() as u8 + merge.is_some() as u8) <= 1,
      "ScanOperator scan-holder invariant: at most one of encoded_scan/encoded_merge/merge may be Some"
  );
  ```
- Adapter: private struct `KernelAppliedSource { inner: Box<dyn SegmentScan>, shapes: Arc<[EncodedEqShape]>, types: Arc<[BqlType]> }`. Its `next()` pulls `inner.next_encoded_row_group()`, runs the same `apply_encoded_eq` loop over `self.shapes` that `encoded_next_batch` runs today, and returns `(EncodedBatch, RowSelection)`. Skip empty batches (loop on an empty `RowSelection` from a kernel rather than returning it — the merge would do the same skip, but pushing it up saves a reload round-trip); return `Ok(None)` when the inner scan is exhausted.
- Share `shapes` and `types` by `Arc` so every source borrows the same slices without per-batch clones.
- Add a new method `encoded_merge_next_batch(&mut self) -> Result<Option<RecordBatch>>`:
  - Check `self.cancel.is_cancelled()` at each loop iteration (mirrors `next_batch`'s entry check — a long merge over many sources must observe cancellation between stitched emits).
  - Loop: `self.encoded_merge.as_mut().unwrap().next_stitched_batch()?` → `materialize_stitched(&stitched, &self.types, self.arrow_schema.clone())?` → pull the `.batch`.
  - Apply the residual-predicate step exactly as `encoded_next_batch` does: `apply_compiled_filters(&self.encoded_residual, batch)?` when any residual remains, else pass through. If `encoded_shapes` is empty (encoded mode requested but no pushable equality), run `self.apply_post_filters(batch)?` instead so we stay correct on the mixed-residual fixture.
  - If the filtered batch is empty, loop again. On stitched `None`, set `exhausted = true` and return `None`.
- Update `next_batch()` to dispatch with explicit precedence: `if self.encoded_scan.is_some() → encoded_next_batch`, `else if self.encoded_merge.is_some() → encoded_merge_next_batch`, else the existing materialized merge loop. The `debug_assert!` in `open()` guarantees at most one is `Some`; if release builds somehow violate this (bug), the order above pins precedence: `encoded_scan` wins, then `encoded_merge`, then `merge`.
- Update `close()` to also clear `self.encoded_merge`.
- Update the `Debug` impl's `"open"` field to include `self.encoded_merge.is_some()` in the OR'd expression.

### `crates/bqlite-operators/src/scan.rs` tests (same file)
- Rename / replace `encoded_path_falls_back_to_merge_on_multi_segment` (line 1841). See §5.
- Add three new tests. See §5.

### `crates/bqlite-core/src/encoded.rs`
- No changes required. `StitchedBatch`/`StitchedRows` are frozen; the new producer uses them as-is.

### `benches/wave2/scan_encoded.rs`
- Optional / deferred. See §6.

## 4. Sequenced steps

Each step is independently buildable and testable.

**Step 1 — Scaffold `EncodedKWayMergeScan` (structural, no wiring).**
- What changes: add the type, constructors, `EncodedBatchSource` trait, and a stub `next_stitched_batch` that returns `Ok(None)` unconditionally. Reuse `validate_key_types`, `EntityKeyValue`, `extract_ts_nanos` from the module.
- Tests prove it: a unit test `new_rejects_out_of_range_entity_key_col` (mirrors existing `KWayMergeScan` test) and `new_validates_sort_key_types`. Compilation + construction validation.

**Step 2 — Implement the reload + single-source pick loop.**
- What changes: real `next_stitched_batch` implementation. Implement the decode-two-sort-columns helper, the heap-entry push, the pick loop, and emission. Initial cut always emits `StitchedRows::Indices`.
- Tests prove it: new unit test `encoded_merge_single_source_passes_through_indices` — feed one `MockEncodedSource` with a known batch + all-rows selection; assert the emitted `StitchedBatch` has one source, `StitchedRows::Indices` whose length equals the row count and whose `source` field is 0 throughout.
- This is a feature add but is gated behind the new type; no existing caller is touched.

**Step 3 — (REMOVED).** The original Step 3 emitted `StitchedRows::SingleSource { selection: None }`, which `materialize_stitched` routes to a full-source decode — unsound when the merge drained only a subset of the source's selection. `SingleSource`/`Runs` compaction is deferred to a follow-up that can emit the correct `selection: Some(...)` shape. CP5 emits `StitchedRows::Indices` uniformly.

**Step 4 — Two-source interleaved picking + tie-break.**
- What changes: none to producer code (picking already uses the heap). Just tests.
- Tests prove it: port the existing `two_scans_with_interleaved_entities` and `equal_keys_tie_break_to_lower_indexed_scan` from `KWayMergeScan` tests to the encoded type. Lower-scan-idx tie-break must survive because `EncodedHeapEntry::cmp` uses `scan_idx` as its third comparison key.

**Step 5 — Empty-selection sources + mid-merge reloads.**
- What changes: harden the reload loop to skip empty selections (loop on `Ok(Some((_, sel)))` with `sel.is_empty()` until a non-empty batch or `Ok(None)`).
- Tests prove it: `encoded_merge_skips_fully_filtered_source_batches` — one source returns a non-empty batch whose selection is empty; merge should still drain the other source deterministically.

**Step 6 — Wire `EncodedKWayMergeScan` into `ScanOperator::open` for the multi-segment encoded path.**
- What changes: build the `KernelAppliedSource` adapter, construct an `EncodedKWayMergeScan` when `scan_path != Materialized && scans.len() > 1`, drop the fallthrough to `KWayMergeScan`. Add `encoded_merge` field, update `next_batch`/`close`/`Debug`.
- Tests prove it: replace `encoded_path_falls_back_to_merge_on_multi_segment` with `encoded_path_multi_segment_preserves_entity_ts_order` (same fixture, same expected order — but now running through the new path). Make sure the existing single-segment encoded path tests still pass unchanged.

**Step 7 — Integration parity tests.**
- What changes: none to code; add three `ScanOperator`-level tests that run both `ScanPath::Materialized` and `ScanPath::Encoded` on the same fixture and assert row-for-row equality:
  - Two segments, pushable `col == literal`, half rows filter out.
  - Two segments with equal `(entity_id, ts)` tuples across sources (pin tie-break).
  - Two segments where one segment's rows are entirely filtered by the kernel.
- Tests prove it: if any parity test fails, the new merge has a correctness bug.

**Step 8 — (Deferred follow-up) `SingleSource { selection: Some(...) }` + `Runs` compaction.**
- Not in CP5 scope. Two follow-up tasks:
  1. Track per-emit source homogeneity. When `homogeneous`, emit `StitchedRows::SingleSource { source, selection: Some(RowSelection::Indices(sv)) }` where `sv` is the exact subset of source rows that were picked. Downstream materializer's `selection: Some` path already handles this correctly.
  2. For source-contiguous `Runs`-shaped kernel output consumed by a homogeneous emit, preserve `Runs` form on the way out — keeps RLE compression through the merge boundary.
- File a TODO in the `EncodedKWayMergeScan` doc comment pointing here.

## 5. Test plan

### Unit tests in `crates/bqlite-storage/src/segment/merge.rs` (new, co-located with `EncodedKWayMergeScan`)
- `encoded_new_rejects_out_of_range_entity_key_col`
- `encoded_new_rejects_non_nanosecond_ts`
- `encoded_new_rejects_schema_mismatch_across_sources` — parity with `KWayMergeScan`'s schema validation.
- `encoded_merge_empty_input_returns_none` — zero sources.
- `encoded_merge_empty_source_drains_others` — one source returns `Ok(None)` immediately; the other still drains fully.
- `encoded_merge_single_source_emits_indices` — one `MockEncodedSource`, assert `StitchedRows::Indices` length equals row count, every `RowRef.source == 0`.
- `encoded_merge_two_sources_interleaved_indices` — covers heap-driven picks across two sources.
- `encoded_merge_equal_keys_tie_break_to_lower_indexed_scan` — two sources share a `(u1, 10)` row; picked `RowRef.source` for that key is `0`.
- `encoded_merge_skips_fully_filtered_source_batches` — one source yields a non-empty batch whose `RowSelection` is empty; merge still drains the other source deterministically.
- `encoded_merge_reloads_across_row_group_boundaries` — source yields two back-to-back `EncodedBatch`es; merge reloads between them without dropping/reordering rows.
- `encoded_merge_small_batch_size_emits_multiple_output_batches` — `with_batch_size(…, 2)` over 5 merged rows emits three `StitchedBatch`es summing to 5.
- `encoded_merge_int64_entity_key` — integer entity-key column path (not just String).
- `encoded_merge_preserves_runs_shape_on_rle_selection` — source emits a batch whose `RowSelection::Runs` has a 50-row run; assert the `SelectionCursor` walker advances through the run correctly (no flattening regression).

Each uses a `MockEncodedSource` local to the test module, paralleling the existing `MockScan` helper in that file.

### Integration tests in `crates/bqlite-operators/src/scan.rs` (new/modified)
- **Delete**: `encoded_path_falls_back_to_merge_on_multi_segment` (line 1841). The fallback is gone.
- **Add**: `encoded_path_multi_segment_preserves_entity_ts_order` — same fixture (two segments, `(u1,u3)` and `(u2,u2,u4)`), asserts `["u1","u2","u2","u3","u4"]` via the encoded pipeline.
- **Add**: `encoded_and_materialized_paths_agree_on_multi_segment_with_pushable_eq` — two segments, a `col == literal` pushable predicate; drain both paths; assert equal row lists (entity_id, ts, event_type triples).
- **Add**: `encoded_multi_segment_preserves_tie_break_on_duplicate_entity_ts` — two segments with a shared `(u1, 10)` row **where each segment's row has a distinguishing `event_type` value** (e.g. `"a"` in seg 0, `"b"` in seg 1); assert the lower-indexed segment's `event_type == "a"` appears first in the merged output. Row-count-only would silently pass on a tie-break regression.
- **Add**: `encoded_multi_segment_with_fully_filtered_source` — two segments; predicate removes every row from segment 1; assert results equal segment 0's rows after the same filter applied to the materialized path.

### No changes
- `materialize_stitched` test module — frozen; still pins consumer behavior.
- Existing single-segment encoded tests (`encoded_path_is_byte_equivalent_to_materialized`, `encoded_path_preserves_non_pushable_residual`) — must pass without modification.

## 6. Benchmark note

**Deferred but noted.** `benches/wave2/scan_encoded.rs` only exercises single-segment today. A follow-up should add a `multi_segment_*` group that:
- Builds 2–8 pre-encoded segments over disjoint entity ranges.
- Builds 2–8 pre-encoded segments over overlapping entity ranges (forces interleaving).
- Benchmarks both `ScanPath::Materialized` and `ScanPath::Encoded` over each fixture with a pushable-eq predicate.

Expected wins: disjoint ranges should drop to ~0 interleave overhead because the `SingleSource` fast path kicks in per emit; overlapping ranges should show reduced `bytes_materialized_before_filter` via the `Metrics` counters that CP2 already wired.

Do not block CP5 merge on this.

## 7. Rollout / flag

No new flag. `ScanPath::Encoded` and `ScanPath::Auto` already exist; today they silently fall back to `KWayMergeScan` for multi-segment scans. After CP5, `ScanPath::Encoded` exercises the new merge end-to-end. `ScanPath::Auto` and its default flip remain CP7's concern — the CP5 plan does not change any default.

The `BQLITE_SCAN_PATH=encoded` env override becomes end-to-end meaningful across multi-segment scans once this lands.

## 8. Open questions

Resolved pre-implementation (from reviewer feedback):

- **Sort-key decode for fallback `Materialized` columns**: accept `materialize_encoded_column`'s clone cost across both encoded and materialized-fallback variants. `EntityKeyValue::extract` is already proven on the materialized path and parity with today's `KWayMergeScan` behavior is the bar.
- **`EncodedBatch` cloning per emit**: acceptable — `PinnedChunk` holds `Arc<[u8]>`, so `Clone` is `O(columns * sources)` refcount bumps, not payload copies. A `SharedEncodedBatch(Arc<EncodedBatch>)` wrapper is a wider IR change; defer.
- **Runs compression**: deferred. CP5 ships `Indices`-only. Follow-up (see Step 8) adds `SingleSource { selection: Some(...) }` and `Runs` emission together.

Still open (flag to user, non-blocking):

1. **`__seq_id` as sort key**: design doc §8.5 lists `entity_id`, `ts`, and `__seq_id` as merge sort keys. Both the current `KWayMergeScan` and this CP5 plan use only `(entity_id, ts, scan_idx)` as the tie-breaker, matching existing behavior. If `__seq_id`-based tie-break is needed, it must land as its own plan change that affects both merge variants — not CP5.
