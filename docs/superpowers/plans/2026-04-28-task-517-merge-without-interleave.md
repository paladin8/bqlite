---
task: TASK-517
title: Merge without `interleave` + late materialization boundary
spec: docs/design/storage/zero-copy-scan-filter.md (§8.4, §8.5, §9)
---

# TASK-517 — Plan

## Context

`docs/design/storage/zero-copy-scan-filter.md` §12 stages 6 and 7 require:

- **Stage 6 — merge without `interleave`.** The k-way merge must emit
  stitched row references over encoded sources rather than copy values
  through `arrow::compute::interleave`.
- **Stage 7 — late materialization boundary.** Materialization is a
  single, explicit boundary (`materialize_stitched`/`materialize_selected`)
  that emits `FilteredBatch { batch, selection: None }`.

CP5 of the zero-copy work (commit `fc9aa7d`, "stitched-merge producer for
multi-segment encoded scans") already landed:

- `EncodedKWayMergeScan` over `EncodedBatchSource`, emitting
  `StitchedBatch` (`crates/bqlite-storage/src/segment/merge.rs`).
- The boundary helper `materialize_stitched` in
  `crates/bqlite-operators/src/materialize.rs`.
- `ScanOperator::encoded_merge_next_batch` driving the encoded merge and
  collapsing to a dense `FilteredBatch`.
- `ScanPath::Auto` selecting the encoded path by default.

The remaining gap is **tombstone correctness on the encoded path**:
`ScanOperator::open` (around `crates/bqlite-operators/src/scan.rs:802`)
forces any segment that carries a non-empty `TombstoneFile` onto the
materialized `KWayMergeScan` path (which still calls `interleave`). The
design doc §8.4 spells out the alternative: a *selection-first*
tombstone wrapper at the `EncodedBatchSource` level that produces a
`RowSelection` (entity tombstones become `Runs` natively) and intersects
it with whatever selection upstream kernels produce, so tombstoned
segments stay on the encoded merge.

The other remaining `interleave` call site,
`MergeSourcesOperator::build_output_batch`
(`crates/bqlite-operators/src/scan.rs:1732`), is the n-ary
**joined-source** merge across already-materialized sub-operator
outputs. The zero-copy design (§11) is explicit that the public
`PhysicalOperator` boundary is unchanged; pushing encoded views across
`PhysicalOperator` is Wave 5 fused-segment scope (TASK-518/519). The
task line "must preserve … joined-source semantics" reads as
*correctness preservation*, not as folding `MergeSourcesOperator` into
the encoded merge. We keep that operator unchanged in this task.

## Goal

Land §8.4 of the zero-copy design: an `EncodedBatchSource` decorator
that applies the four `TombstoneFile` granularities as a `RowSelection`
intersect, and remove the `any_wrapped → fallback to materialized`
shortcut so tombstoned scans stay on the encoded merge. Result: every
read path that reaches a `RecordBatch` does so via
`materialize_stitched` / `materialize_selected`, never via
`KWayMergeScan::interleave_output`. `KWayMergeScan` itself remains as
the `BQLITE_SCAN_PATH=materialized` debug fallback.

## Out of scope

- `MergeSourcesOperator` (joined-source merge): kept unchanged. See
  Wave 5 fused-segment work.
- Removing `KWayMergeScan` outright. `ScanPath::Materialized` keeps it
  alive as a debug escape hatch.
- Dictionary-code lowering for entity tombstones (§8.4 final bullet).
  Worth noting as a follow-up; not required for correctness.

## Checkpoints

### CP1 — `EncodedTombstoneSource` adapter

**File**: new `crates/bqlite-storage/src/encoded_tombstone.rs`,
exported from `lib.rs`.

**Type**:
```rust
pub struct EncodedTombstoneSource {
    inner: Box<dyn EncodedBatchSource>,
    tombstones: TombstoneFile,
    entity_index: EntityDeleteIndex,
    entity_key_col: usize,
    ts_col: usize,
    entity_bql: BqlType,
    seq_id_first: u64,   // for synthesized __seq_id under row_deletes
    batch_id: u64,       // for synthesized __batch_id under batch_deletes
    next_row_offset: u64,
    all_batch_dropped: bool, // batch_id ∈ batch_deletes → drop everything
}
```

`EncodedBatchSource::next` algorithm:

1. Pull `(EncodedBatch, RowSelection)` from `inner`. `Ok(None)` →
   exhausted.
2. If `all_batch_dropped` → return an empty selection without decoding
   anything else; advance `next_row_offset` by `batch.row_count`.
3. Materialize **only** the entity-key column (and the `ts` column when
   `time_range_deletes` is non-empty) via `materialize_encoded_column`.
   The downstream `EncodedKWayMergeScan` re-decodes the same two columns
   for its sort key (`merge.rs:1114–1116`), so this introduces a
   *duplicate* decode of entity_id and ts when tombstones are present.
   Both decodes go from a pinned chunk into bounded scratch — no payload
   copy in the §3.2 sense — and the per-row-group cost is small relative
   to the merge's heap walk. We accept the duplicate decode for v1.
   Reusing the wrapper's decoded arrays in `EncodedKWayMergeScan::load_batch`
   is a future optimization (would need the source to attach decoded
   sort-key columns to the returned `EncodedBatch`); not in scope.
4. Build a `kept` bool mask (indexed by row in the batch, *not* the
   selection):
   - Entity deletes — probe `entity_index` per row of the entity-key
     array. Emit `RowSelection::Runs` natively when the column is
     entity-sorted (the common case in storage segments) by walking
     contiguous alive ranges; otherwise fall back to `Indices`.
   - Time-range deletes — probe each `TimeRangeDelete::contains_timestamp`
     per row of the ts array.
   - Row deletes — synthesize `__seq_id = seq_id_first + offset` per row
     in-place (no materialized column).
   - (Batch deletes are handled by `all_batch_dropped` short-circuit.)
5. Convert `kept` to a `RowSelection`. Pick the variant that is
   cheapest *for the wrapper itself*, not based on the upstream variant:
   Runs when the alive mask collapses to ≤K contiguous ranges (entity
   tombstones in entity-sorted segments are the common case); Indices
   otherwise. `RowSelection::intersect` (`encoded.rs:461`) handles the
   variant coercion — Indices on either side coerces the result.
6. Intersect with the upstream `RowSelection` via
   `RowSelection::intersect`. If the upstream selection is empty,
   short-circuit to an empty selection without applying tombstones.
7. Update `next_row_offset += batch.row_count`.
8. Return `(batch, intersected_selection)` — the batch payload is
   passed through unchanged (zero copies); only the selection narrows.

**Tests** (all in `#[cfg(test)] mod tests` inside the new file):
- Entity tombstones: contiguous block of entity rows is excluded;
  output selection is Runs and the EncodedBatch is unchanged.
- Row tombstones: rows whose synthesized `__seq_id` is in
  `row_deletes` are excluded.
- Batch tombstones: a tombstone covering the segment's `batch_id`
  drops the entire batch (empty selection) without materializing
  non-key columns.
- Time-range tombstones: rows with ts in the range are excluded.
- Composition: upstream selection (Runs) intersected with tombstone
  selection (Runs) stays in Runs form.
- Composition: upstream selection (Indices) intersected with
  tombstone selection.
- All four granularities composed in a single source.
- Empty `TombstoneFile` short-circuits to passthrough (no decode).
- Whole-batch entity tombstone (one entity covers the entire batch
  but `all_batch_dropped` is false) emits an empty selection without
  panicking the downstream merge heap reload (§S5 of CP1 review).
- Empty upstream selection short-circuits without decoding non-key
  columns.

**Validation gate**:
- `scripts/local-ci.sh` clean.
- Subagent code review with no blocking findings.

### CP2 — Wire into `ScanOperator::open`; drop the materialized fallback

**File edits**:

`crates/bqlite-operators/src/scan.rs`:
- Replace the `any_wrapped → fallback to KWayMergeScan` branch (around
  line 802) with an encoded-path-aware branch:
  - When `scan_path != Materialized` and any segment in `handles` has a
    non-empty tombstone file, wrap that segment's
    `KernelAppliedSource` with `EncodedTombstoneSource` (and *do not*
    wrap with `TombstoneScanWrapper`).
  - When `scan_path == Materialized`, retain the current
    `TombstoneScanWrapper`-based path so the materialized debug
    fallback still works correctly.
- Remove the `any_wrapped` boolean on the encoded path; the per-segment
  wrap decision is now local to each handle.
- Update the `merge` field comment so it accurately describes the new
  invariant: `merge` is `Some` only on the materialized debug path.
- Add an inline invariant comment near the wrap branches: "a segment
  is wrapped by exactly one tombstone adapter — `TombstoneScanWrapper`
  on `ScanPath::Materialized`, `EncodedTombstoneSource` on the encoded
  path; the two never compose."

**Test edits**:
- Existing tombstone scan tests in
  `crates/bqlite-operators/src/scan.rs` (search for "tombstone" in
  `mod tests`) — verify they pass under both `ScanPath::Auto` and
  `ScanPath::Materialized`. Add an assertion-pair test that exercises
  both paths and asserts row-equivalent output.
- Existing integration tests in `crates/bqlite-operators/tests/` and
  `crates/bqlite-storage/tests/` should run unchanged — that
  preservation is the regression bar.

**Doc edits**:
- `docs/design/storage/zero-copy-scan-filter.md` §8.4: append a short
  "Status: implemented (TASK-517)" note.
- `crates/bqlite-storage/src/segment/merge.rs` module header: replace
  the §8.4 "future" reference around line 99–101 with a pointer to
  `EncodedTombstoneSource`.
- `crates/bqlite-operators/src/materialize.rs` `materialize_stitched`
  doc: drop the "Non-goal for CP5" paragraph (it referenced the
  producer landing later — it's now landed and wired through tombstones).

**Validation gate**:
- `scripts/local-ci.sh` clean.
- Manual sanity: verify there is no remaining
  `KWayMergeScan::interleave_output` call on the *default* scan path,
  i.e. it can only be reached under `BQLITE_SCAN_PATH=materialized`.
- Subagent code review with no blocking findings.

## Risk register

- **TASK-516 in flight on a branch.** TASK-516 owns dictionary/RLE
  selection-first kernels. Its file footprint (`encoded_filter.rs`,
  predicate kernels) does not overlap with this task's scope. If
  TASK-516 lands first, our `EncodedTombstoneSource` composes naturally
  with whatever selection the kernels produce. If TASK-517 lands first,
  the kernels still see correct selections because intersect is the
  composition rule both directions.
- **Entity-sorted assumption.** Encoded segments are entity-sorted; if
  a future joined-source path produced merged-but-not-entity-sorted
  output to an `EncodedTombstoneSource`, the Runs optimization would
  silently miscount. Guard: when the alive mask is not contiguous,
  fall through to `Indices`. Tests cover both shapes.
- **Synthetic `__seq_id` overflow.** `next_row_offset` is `u64`; we
  carry the same overflow check `TombstoneScanWrapper` uses today.
- **Cancellation.** Each `EncodedBatchSource::next` call is a single
  row group; cancellation latency stays at row-group granularity, the
  same as today.
