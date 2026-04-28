# TASK-508: System-Column Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialize the implicit `__seq_id` and `__batch_id` system columns end-to-end so that they appear on every `ScanOperator` and `MergeSourcesOperator` output batch when projected, and make the system-column contract explicit in the design docs.

**Architecture:**
- The segment reader's `build_scan_plan` learns to recognise the two system column names by name (they are not in `current_schema.columns()`) and synthesises them from per-segment footer metadata: `__seq_id` from `seq_id_first + cumulative_offset` and `__batch_id` as a constant `Int64Array`. This matches the derivation already used by `CompactionTombstoneScan` for compaction-time tombstoning.
- `ColumnProjection::all()` keeps its current empty-projection sentinel meaning "every column", but the reader now expands it to include the two system columns at the end of the materialised batch (per `crates/bqlite-core/src/storage.rs:156-160` doc-comment).
- `ScanOperator::build_output_schema` mirrors the reader: empty projection → declared columns + `__seq_id`, `__batch_id`. Explicit projections may name the system columns and they pass through.
- `build_joined_scan` in `bqlite-planner::logical` adds the system columns (bare names, NOT NULL, Int) to the combined schema. Each sub-scan's projection is widened to include the system columns so `MergeSourcesOperator`'s existing bare-name resolution path populates them.
- A new design doc `docs/design/storage/system-columns.md` consolidates the contract that today is scattered across `storage-format.md`, `execution-model.md`, and `cohorts-aliases-joins.md`.

**Tech Stack:** Rust 2021, Arrow `Int64Array`, existing `bqlite-core` `OperatorSchema` / `ColumnProjection` / `SegmentReader` traits, `bqlite-storage` segment reader/writer/footer, `bqlite-operators::scan` ScanOperator + MergeSourcesOperator, `bqlite-planner::logical` joined-source lowering.

---

## Pre-implementation: re-read

Before touching code, the executing engineer should re-read:

- `docs/design/storage-format.md` §6.2 — the canonical `__seq_id` / `__batch_id` definition.
- `docs/design/execution-model.md` §3.7 — pipeline schema / null-bitmap rules for the system columns.
- `docs/design/planner/logical-plan-nodes.md` §Scan output schema — `OperatorSchema::from_table` shape.
- `docs/design/language/cohorts-aliases-joins.md` §3.8 — combined schema declaration for joined sources.
- `crates/bqlite-storage/src/tombstone_scan.rs` — `CompactionTombstoneScan` already does the `seq_id_first + offset` derivation; reuse its pattern.
- `crates/bqlite-operators/src/scan.rs` module-level docs — currently states system columns are NOT materialised. After this task they MUST be.
- `crates/bqlite-planner/src/logical.rs:1319-1329` — explicit comment carving out why system columns are omitted from the joined schema today. After this task they should be re-added; that comment must be rewritten.
- `crates/bqlite-planner/src/logical.rs:8068-8102` — `joined_pipeline_omits_system_columns_from_combined_schema` regression guard. The test expectation flips in CP4.
- `crates/bqlite-operators/src/scan.rs:3488-3520` — `row_tombstones_error_when_seq_id_column_missing`. Once CP3 lands, this test no longer documents the truth and is replaced.

---

## Checkpoint 1: Design Doc + INDEX entry

**Files:**
- Create: `docs/design/storage/system-columns.md`
- Modify: `docs/design/INDEX.md` (add Storage entry pointing at the new doc)

This is a pure-docs checkpoint that establishes the contract subsequent code changes will implement. It's small enough that the plan's review-by-subagent expectation can be a one-pass review, but it MUST land first because CP2/CP3/CP4 commit messages will cite it.

- [ ] **Step 1: Write the design doc**

Create `docs/design/storage/system-columns.md` with the following sections:

```markdown
# System Column Materialization

> Reconciles `storage-format.md` §6.2 (semantics), `execution-model.md` §3.7 (pipeline schema), `planner/logical-plan-nodes.md` §Scan (logical-plan output shape), and `language/cohorts-aliases-joins.md` §3.8 (joined-source combined schema). Where this doc and the older docs disagree, this doc wins; the older docs will be edited in lockstep with the implementation that lands TASK-508.

## 1. The two columns

| Column | Arrow type | Nullable | Origin | `SELECT *` |
|---|---|---|---|---|
| `__seq_id` | `Int64` | NO | per-row, segment-local | excluded |
| `__batch_id` | `Int64` | NO | per-segment constant | excluded |

`storage-format.md` §6.2 is the authoritative semantics doc. This doc covers materialization rules only.

## 2. Per-segment derivation

Every segment footer carries:

- `seq_id_range: (u64, u64)` — closed inclusive range; `seq_id_range.0` is the segment's first row's `__seq_id`. Compaction allocates a fresh contiguous range via `Database::allocate_sequence_id_range`, so the rule below holds for both ingest-written and compaction-written segments.
- `batch_id: u64` — the ingest batch (or compaction-output id) the segment belongs to.

For row `n` (0-indexed) in cumulative segment-storage order:

```
__seq_id(n) = seq_id_range.0 + n
__batch_id  = batch_id   (constant)
```

The `seq_id_range.0 + n` form is identical to what `CompactionTombstoneScan` (`crates/bqlite-storage/src/tombstone_scan.rs`) already uses to derive per-row `__seq_id` for compaction-time tombstone filtering. Both readers and the compaction wrapper share the same convention.

## 3. Reader contract

`SegmentFileReader::scan(projection, predicate)` resolves the projection as follows:

- **Empty / `ColumnProjection::all()`**: emit every declared column (in `current_schema.columns()` order) followed by `__seq_id` and `__batch_id` in that order. This matches the doc-comment on `ColumnProjection::all()` in `bqlite-core/src/storage.rs`.
- **Explicit projection**: each requested name is resolved against `TableSchema::logical_columns()` (declared + system). System column names are recognised by exact match. Unknown names error as before.

Synthesised arrays are non-nullable `Int64Array`s. The reader does NOT touch column-chunk bytes for these names — there is no on-disk chunk for them in the v1/v2 segment format.

The `ScanPlan` gains two new `PlannedColumnSource` variants (`SystemSeqId { seq_id_first }`, `SystemBatchId { batch_id }`) so the per-row-group decoder can build them inline. A `next_row_offset: u64` cursor on `SegmentFileScan` tracks the cumulative offset across row groups, advanced after each `decode_row_group` succeeds.

For the encoded path (`next_encoded_row_group`), system columns are emitted as `EncodedColumn::Materialized` wrapping the same `Int64Array` — the encoded merge / kernel layer treats them as opaque materialized columns. This keeps the encoded-batch contract intact without requiring a synthetic dictionary or RLE encoding for what is structurally a counter / constant.

## 4. Operator contract

### 4.1 ScanOperator

`build_output_schema` mirrors the reader:

- Empty `projected_columns` slice → output schema = `OperatorSchema::from_table(reader.schema())` (declared + `__seq_id` + `__batch_id`).
- Explicit projection → resolved against `TableSchema::logical_columns()`; system column names are first-class.

Existing tests that assert "scan output omits system columns" must be updated; this task includes that work.

### 4.2 MergeSourcesOperator

Per `cohorts-aliases-joins.md` §3.8, the combined schema for a joined source carries:

- `<table>.<col>` for every non-system column of every sub-table (nullable).
- `__source_table_id: Int64` non-nullable.
- `__seq_id: Int64` non-nullable.
- `__batch_id: Int64` non-nullable.

The system columns are bare-named (no `<table>.` qualifier) because they have identical semantics across every sub-table. The merge picks one row from one sub-scan at a time, and the picked sub-scan's `__seq_id` / `__batch_id` populate the output — never null — because every sub-scan emits them by the contract above.

`build_joined_scan` (`crates/bqlite-planner/src/logical.rs`) adds the two system columns to the combined schema. The previous comment about omitting them (added when the scan operator could not materialise them) is removed.

Sub-scan projections must include the system columns. The simplest and current-default form — pass an empty `projected_columns` slice — already gives the full set after CP2/CP3 (see §3 above and §4.1). Joined-source lowering relies on this default.

## 5. SELECT *

`SELECT *` excludes system columns (per `storage-format.md` §6.2 and `query-language.md`); the planner's project-expansion step is responsible for filtering them out of the star expansion. This task does not change that behaviour: the *operator* schemas carry the system columns, but the user-visible `SELECT *` projection drops them.

## 6. Reconciliation with prior docs

- `bqlite-operators::scan` module docs previously stated that `__seq_id` / `__batch_id` are NOT in the scan's output schema. That paragraph is rewritten in lockstep with CP3.
- `bqlite-planner::logical::build_joined_scan` previously documented an explicit omission of the two system columns. That comment is rewritten in lockstep with CP4 to point at this doc.
- `crates/bqlite-storage/src/segment/reader.rs::scan` doc-comment previously made no mention of system-column synthesis. Updated in lockstep with CP2.
- `bqlite-core/src/storage.rs::ColumnProjection::all()` already documents "all declared columns plus the implicit `__seq_id` and `__batch_id` system columns". After CP2 the implementation finally matches that doc.
```

- [ ] **Step 2: Add the INDEX entry**

In `docs/design/INDEX.md`, under `### Storage`, insert (alphabetically) a bullet:

```markdown
- **storage/system-columns.md** — `__seq_id` / `__batch_id` materialization contract: per-segment derivation rules (`seq_id_first + offset`, constant `batch_id`), reader projection semantics for system columns, ScanOperator + MergeSourcesOperator output-schema rules, reconciliation against pre-TASK-508 carve-outs in `bqlite-operators::scan` and `bqlite-planner::logical::build_joined_scan` (TASK-508, Wave 5)
```

- [ ] **Step 3: Validate**

```bash
scripts/local-ci.sh
```

Expected: passes (no code changed; markdown only).

- [ ] **Step 4: Subagent code review**

Spawn a subagent with the full diff. Reviewer must confirm:
- The doc reconciles with `storage-format.md` §6.2, `execution-model.md` §3.7, and `cohorts-aliases-joins.md` §3.8 without contradiction.
- The derivation formula matches `CompactionTombstoneScan`'s existing implementation.
- No verdict-blocking findings.

If `REVISE`, address findings and re-review.

- [ ] **Step 5: Commit**

```bash
git add docs/design/storage/system-columns.md docs/design/INDEX.md
git commit -m "TASK-508: System-column materialization contract design doc"
```

- [ ] **Step 6: Merge to main**

```bash
git checkout main
git pull origin main
git merge task/TASK-508 --ff-only
git push origin main
git checkout task/TASK-508
```

---

## Checkpoint 2: Reader-level synthesis of `__seq_id` / `__batch_id`

**Files:**
- Modify: `crates/bqlite-storage/src/segment/reader.rs`
  - `ScanPlan` struct + `PlannedColumnSource` enum
  - `build_scan_plan` function (system-column recognition path)
  - `SegmentFileScan` struct (add `next_row_offset` cursor)
  - `SegmentFileScan::decode_row_group` and `decode_encoded_row_group` (synthesis)
  - module-level / `scan` doc-comments (point at the new design doc)
- Modify: `crates/bqlite-core/src/storage.rs:154-160` (no behaviour change — the existing doc-comment already promises this; nothing to edit unless the comment needs sharpening; review and adjust if it overpromises today)
- Test: `crates/bqlite-storage/src/segment/reader.rs` (existing tests module — add new tests at the end)

- [ ] **Step 1: Write the failing reader-synthesis test for `ColumnProjection::all()`**

Append to the existing `#[cfg(test)] mod tests` in `crates/bqlite-storage/src/segment/reader.rs`. Reuse the `read_round_trip_table_helper` style already in that file (search for an existing round-trip test to confirm the helper name; the test below assumes a helper that writes a 1-segment file with three rows, `seq_id_range = (1000, 1002)`, `batch_id = 7`).

```rust
#[test]
fn scan_all_includes_system_columns_with_correct_values() {
    use arrow::array::Int64Array;
    use bqlite_core::ColumnProjection;

    // Reuse whichever existing helper writes a single-segment file
    // with a known seq_id_range / batch_id. If none exists, build one
    // inline that goes through `prepare_segment` + `write_segment`
    // with `seq_id_range = (1000, 1002)` and `batch_id = 7`.
    let (path, _tmp) = write_three_row_segment(/* seq_first */ 1000, /* batch */ 7);
    let reader = SegmentFileReader::open(&path).unwrap();
    let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();

    let batch = scan.next_row_group().unwrap().expect("one row group");
    let names: Vec<&str> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let n_decl = reader.schema().columns().len();
    assert_eq!(&names[n_decl..], &["__seq_id", "__batch_id"]);

    let seq = batch
        .column(n_decl)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("__seq_id is Int64");
    assert_eq!(seq.values(), &[1000, 1001, 1002]);

    let bid = batch
        .column(n_decl + 1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("__batch_id is Int64");
    assert_eq!(bid.values(), &[7, 7, 7]);

    // System columns must NOT have a null bitmap — they are
    // declared NOT NULL per execution-model.md §3.7.
    assert_eq!(seq.null_count(), 0);
    assert_eq!(bid.null_count(), 0);
    assert!(batch.schema().field(n_decl).is_nullable() == false);
    assert!(batch.schema().field(n_decl + 1).is_nullable() == false);
}
```

- [ ] **Step 2: Run the test, expect failure**

```bash
cargo test -p bqlite-storage --lib scan_all_includes_system_columns_with_correct_values
```

Expected: FAIL — current `ColumnProjection::all()` path produces only declared columns; either the schema length assertion or the column-count check trips.

- [ ] **Step 2.5: Verify `EncodedColumn::Materialized` field shape**

Before writing the encoded-path synthesis, confirm the variant shape:

```bash
grep -n "Materialized\s*{" crates/bqlite-core/src/encoded.rs
```

Expected: `Materialized { array: ArrayRef, rows: u32 }` (the variant currently has both fields per `crates/bqlite-core/src/encoded.rs:175-180`). If the shape differs, adjust the encoded-path code in Step 4.

- [ ] **Step 3: Update `ScanPlan` / `PlannedColumnSource` / `build_scan_plan`**

Edit `crates/bqlite-storage/src/segment/reader.rs`. In `PlannedColumnSource`, add two new variants:

```rust
enum PlannedColumnSource {
    FromSegment {
        write_time_ordinal: usize,
        write_time_col: ColumnDef,
    },
    BackfillAllNull,
    /// Synthesised `__seq_id` for this row group: every row gets
    /// `seq_id_first + cumulative_offset + row_idx`. The cumulative
    /// offset is tracked by the `SegmentFileScan`, not the plan.
    SystemSeqId,
    /// Synthesised `__batch_id` for this row group: a constant
    /// `Int64Array` of length `row_count` filled with `batch_id`.
    SystemBatchId,
}
```

In `build_scan_plan`, expand the projection-name resolution to recognise the two system-column names. Use the constants already exported from `bqlite-core::schema`:

```rust
use bqlite_core::schema::{BATCH_ID_COLUMN, SEQ_ID_COLUMN};

// ... inside build_scan_plan, after computing `column_names` for
// the explicit-projection branch, fold in the two system columns.

// For the `is_all` path: append the system columns at the end.
let mut planned_names: Vec<String> = if projection.is_all() {
    let mut v: Vec<String> = current_schema
        .columns()
        .iter()
        .map(|c| c.name.clone())
        .collect();
    v.push(SEQ_ID_COLUMN.to_string());
    v.push(BATCH_ID_COLUMN.to_string());
    v
} else {
    // Explicit projection: keep current behaviour for declared
    // columns (filter table.columns in schema order), then append
    // any system column names that were requested, preserving
    // request order.
    let projected: std::collections::HashSet<&str> =
        projection.columns().iter().map(String::as_str).collect();
    let mut v: Vec<String> = current_schema
        .columns()
        .iter()
        .filter(|c| projected.contains(c.name.as_str()))
        .map(|c| c.name.clone())
        .collect();
    for name in projection.columns() {
        if name == SEQ_ID_COLUMN || name == BATCH_ID_COLUMN {
            v.push(name.clone());
        }
    }
    v
};

// Validate: every requested name must resolve to either a declared
// column or one of the two system columns.
if !projection.is_all() {
    for name in projection.columns() {
        let is_declared = current_schema.columns().iter().any(|c| c.name == *name);
        let is_system = name == SEQ_ID_COLUMN || name == BATCH_ID_COLUMN;
        if !is_declared && !is_system {
            return Err(BqliteError::Schema(format!(
                "segment reader: column `{name}` not found in current schema `{}` \
                 and is not a recognised system column",
                current_schema.name()
            )));
        }
    }
}

for name in planned_names {
    if name == SEQ_ID_COLUMN {
        arrow_fields.push(Field::new(&name, DataType::Int64, false));
        entries.push(PlannedColumn {
            output_type: BqlType::Int,
            source: PlannedColumnSource::SystemSeqId,
        });
        continue;
    }
    if name == BATCH_ID_COLUMN {
        arrow_fields.push(Field::new(&name, DataType::Int64, false));
        entries.push(PlannedColumn {
            output_type: BqlType::Int,
            source: PlannedColumnSource::SystemBatchId,
        });
        continue;
    }
    // ... existing FromSegment / BackfillAllNull resolution ...
}
```

The `DataType::Int64` use needs `use arrow::datatypes::DataType;` at the top of the file if not already imported (verify).

- [ ] **Step 4: Add the cumulative-offset cursor and synthesis to the decoders**

Pruning correctness: zone-map pruning may skip a row group, but the `__seq_id` of rows in pruned groups STILL exists in the segment storage order. If we counted only emitted rows, we'd misalign `__seq_id` for rows in the row groups that come *after* a pruned one. The cumulative offset must advance by the row group's `row_count` regardless of whether we prune it.

Solution: compute a prefix-sum table once at scan construction. Although `next_idx` advances monotonically (so a single running cursor would work today), the prefix-sum form is robust against any future API that exposes row groups out of order (e.g. parallel decode), and keeps the synthesis call O(1) per row group.

Add to the `SegmentFileScan` struct:

```rust
pub struct SegmentFileScan {
    bytes: Arc<[u8]>,
    footer: Arc<SegmentFooter>,
    dictionaries: Arc<[DictionaryValues]>,
    dict_bytes: Arc<[bqlite_core::encoded::ArcBytes]>,
    plan: ScanPlan,
    predicate: Option<Arc<dyn Predicate>>,
    next_idx: usize,
    exhausted: bool,
    /// Prefix sum of `row_group.row_count` values: `row_group_start[i]`
    /// is the cumulative row offset of row group `i`. Computed once at
    /// construction so synthesised `__seq_id` is correct even when
    /// zone-map pruning skips earlier row groups
    /// (system-columns.md §3). Robust against future out-of-order
    /// row-group APIs; a single running cursor would also work today
    /// because `next_idx` is monotonic.
    row_group_start: Vec<u64>,
}
```

In `SegmentFileReader::scan`, compute the prefix sum:

```rust
let row_group_start: Vec<u64> = {
    let mut acc: u64 = 0;
    self.footer
        .row_groups()
        .iter()
        .map(|rg| {
            let s = acc;
            acc = acc.saturating_add(rg.row_count);
            s
        })
        .collect()
};
Ok(SegmentFileScan {
    bytes: self.bytes.clone(),
    footer: self.footer.clone(),
    dictionaries: self.dictionaries.clone(),
    dict_bytes: self.dict_bytes.clone(),
    plan,
    predicate,
    next_idx: 0,
    exhausted: false,
    row_group_start,
})
```

Update `decode_row_group` to synthesise:

```rust
fn decode_row_group(&self, idx: usize) -> Result<RecordBatch> {
    use arrow::array::Int64Array;

    let rg = &self.footer.row_groups()[idx];
    let row_count = rg.row_count as usize;
    let start_offset = self.row_group_start[idx];
    let seq_id_first = self.footer.seq_id_range().0;
    let batch_id_const = self.footer.batch_id() as i64;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.plan.entries.len());
    for entry in &self.plan.entries {
        let array: ArrayRef = match &entry.source {
            PlannedColumnSource::FromSegment { write_time_ordinal, write_time_col } => {
                // ... existing decode_column_chunk call ...
            }
            PlannedColumnSource::BackfillAllNull => {
                backfill_all_null(&entry.output_type, row_count)?
            }
            PlannedColumnSource::SystemSeqId => {
                // `seq_id_first + start_offset + i` — check the upper
                // bound once (B2 from plan review). We cast to i64
                // because Arrow's `Int64Array` is signed; per
                // type-system.md §7.1 our `BqlType::Int` maps to
                // `Int64`. A segment whose seq_ids overflow `i64::MAX`
                // would also overflow the per-table allocator (which
                // already errors in `allocate_sequence_id_range`), so
                // surfacing this here as `Execution` is defence in
                // depth.
                let base = seq_id_first
                    .checked_add(start_offset)
                    .and_then(|b| b.checked_add(row_count as u64).map(|_| b))
                    .ok_or_else(|| BqliteError::Execution(
                        "segment reader: __seq_id range overflows u64 \
                         (segment seq_id_first + cumulative_offset + row_count)".into(),
                    ))?;
                let mut buf: Vec<i64> = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    buf.push((base + i as u64) as i64);
                }
                Arc::new(Int64Array::from(buf)) as ArrayRef
            }
            PlannedColumnSource::SystemBatchId => {
                // `vec![batch_id; row_count]` allocates `row_count * 8`
                // bytes for a logically constant column. Arrow has no
                // first-class constant-array type and the existing
                // `BackfillAllNull` path uses the same materialised
                // shape, so accepting this allocation matches the rest
                // of the reader. Future optimization: dictionary-encode
                // a single-entry batch_id column at scan time. Not
                // worth doing today — `__batch_id` is rarely
                // projected outside DELETE-by-batch.
                let buf: Vec<i64> = vec![batch_id_const; row_count];
                Arc::new(Int64Array::from(buf)) as ArrayRef
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(self.plan.arrow_schema.clone(), columns).map_err(|e| {
        BqliteError::Execution(format!(
            "segment reader: failed to assemble row group {idx}: {e}"
        ))
    })
}
```

Update `decode_encoded_row_group` similarly — the system columns become `EncodedColumn::Materialized` wrapping the same `Int64Array`s:

```rust
PlannedColumnSource::SystemSeqId => {
    let base = seq_id_first
        .checked_add(start_offset)
        .ok_or_else(|| BqliteError::Execution(
            "segment reader: __seq_id base overflowed u64".into(),
        ))?;
    let mut buf: Vec<i64> = Vec::with_capacity(row_count);
    for i in 0..row_count {
        buf.push(base.wrapping_add(i as u64) as i64);
    }
    EncodedColumn::Materialized {
        array: Arc::new(Int64Array::from(buf)) as ArrayRef,
        rows: row_count as u32,
    }
}
PlannedColumnSource::SystemBatchId => EncodedColumn::Materialized {
    array: Arc::new(Int64Array::from(vec![batch_id_const; row_count])) as ArrayRef,
    rows: row_count as u32,
},
```

- [ ] **Step 5: Run the test, expect pass**

```bash
cargo test -p bqlite-storage --lib scan_all_includes_system_columns_with_correct_values
```

Expected: PASS.

- [ ] **Step 6: Add a multi-row-group test for `__seq_id` continuity across pruned groups**

Append:

```rust
#[test]
fn scan_seq_id_correct_across_multi_row_group_with_pruning() {
    // Segment with three row groups of 2 rows each, seq_id_first=100,
    // batch_id=3. A predicate that prunes the middle row group must
    // still produce __seq_id = [100, 101, 104, 105].
    use arrow::array::Int64Array;
    use bqlite_core::ColumnProjection;
    let (path, _tmp) = write_three_row_group_segment(/* seq_first */ 100, /* batch */ 3);
    let reader = SegmentFileReader::open(&path).unwrap();
    let pred = predicate_skipping_middle_group();
    let mut scan = reader.scan(&ColumnProjection::all(), Some(pred)).unwrap();

    let mut all_seq: Vec<i64> = Vec::new();
    while let Some(batch) = scan.next_row_group().unwrap() {
        let n_decl = reader.schema().columns().len();
        let seq = batch
            .column(n_decl)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        all_seq.extend(seq.values());
    }
    assert_eq!(all_seq, vec![100, 101, 104, 105]);
}
```

The helpers `write_three_row_group_segment` and `predicate_skipping_middle_group` may need to be authored in the same test module. Use existing fixtures in this file as a pattern; if no helper exists, build the segment inline using `prepare_segment` + `write_segment`, then construct a `ScanPredicate` whose `accepts_zone_group` returns false for one specific zone (e.g. an `Equal` on `entity_id` to a value only present in the middle group's zone-map gap).

- [ ] **Step 6.5: Add encoded-path test (S5 from review)**

```rust
#[test]
fn scan_encoded_path_synthesises_system_columns() {
    use arrow::array::Int64Array;
    use bqlite_core::encoded::EncodedColumn;
    use bqlite_core::ColumnProjection;

    let (path, _tmp) = write_three_row_segment(/* seq_first */ 1000, /* batch */ 7);
    let reader = SegmentFileReader::open(&path).unwrap();
    let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
    let encoded = scan.next_encoded_row_group().unwrap().expect("one row group");

    let n_decl = reader.schema().columns().len();
    // System columns are emitted as Materialized variants.
    let seq_col = &encoded.columns()[n_decl];
    let bid_col = &encoded.columns()[n_decl + 1];
    let EncodedColumn::Materialized { array: seq_arr, rows: seq_rows } = seq_col else {
        panic!("expected Materialized __seq_id, got {seq_col:?}");
    };
    let EncodedColumn::Materialized { array: bid_arr, rows: bid_rows } = bid_col else {
        panic!("expected Materialized __batch_id, got {bid_col:?}");
    };
    assert_eq!(*seq_rows, 3);
    assert_eq!(*bid_rows, 3);
    assert_eq!(
        seq_arr.as_any().downcast_ref::<Int64Array>().unwrap().values(),
        &[1000, 1001, 1002]
    );
    assert_eq!(
        bid_arr.as_any().downcast_ref::<Int64Array>().unwrap().values(),
        &[7, 7, 7]
    );
}
```

Run:
```bash
cargo test -p bqlite-storage --lib scan_encoded_path_synthesises_system_columns
```

Expected: PASS.

- [ ] **Step 7: Add an explicit-projection test**

```rust
#[test]
fn scan_explicit_projection_can_request_system_columns_only() {
    use arrow::array::Int64Array;
    use bqlite_core::ColumnProjection;

    let (path, _tmp) = write_three_row_segment(/* seq_first */ 1000, /* batch */ 7);
    let reader = SegmentFileReader::open(&path).unwrap();
    let proj = ColumnProjection::with_columns(["__seq_id", "__batch_id"]);
    let mut scan = reader.scan(&proj, None).unwrap();
    let batch = scan.next_row_group().unwrap().unwrap();
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "__seq_id");
    assert_eq!(batch.schema().field(1).name(), "__batch_id");
}

#[test]
fn scan_unknown_projection_name_still_errors() {
    use bqlite_core::ColumnProjection;
    let (path, _tmp) = write_three_row_segment(0, 0);
    let reader = SegmentFileReader::open(&path).unwrap();
    let proj = ColumnProjection::with_columns(["__nope"]);
    let err = reader.scan(&proj, None).unwrap_err();
    assert!(format!("{err}").contains("not found"));
}
```

- [ ] **Step 8: Run the new tests**

```bash
cargo test -p bqlite-storage --lib scan_seq_id_correct_across_multi_row_group_with_pruning \
                                   scan_explicit_projection_can_request_system_columns_only \
                                   scan_unknown_projection_name_still_errors
```

Expected: PASS.

- [ ] **Step 9: Run the whole storage test suite**

```bash
cargo test -p bqlite-storage
```

Expected: PASS — no existing test should regress. If any tests broke because they assumed `ColumnProjection::all()` returned only declared columns, fix them by either (a) updating the assertion to expect the system columns, or (b) switching to an explicit projection that names only the declared columns. Document each adjustment in the commit message.

- [ ] **Step 10: Update doc-comments**

In `crates/bqlite-storage/src/segment/reader.rs`, edit the `SegmentFileReader::scan` doc-block (around line 299–342) to add:

```text
/// ## System columns
///
/// `__seq_id` and `__batch_id` (see `bqlite_core::schema`) are
/// recognised by name and synthesised from segment-footer metadata
/// per `docs/design/storage/system-columns.md` §3. They are not
/// stored as on-disk column chunks. `ColumnProjection::all()`
/// includes them at the end of the materialised batch; an explicit
/// projection may name them and they pass through.
```

- [ ] **Step 11: Run local-ci**

```bash
scripts/local-ci.sh
```

Expected: PASS (fmt, dep-direction, clippy, build, test).

- [ ] **Step 12: Subagent code review**

Spawn a subagent. Reviewer must check:
- Correctness: `seq_id_first + cumulative_offset` derivation matches what `CompactionTombstoneScan` does. Pruned row groups don't break offset alignment.
- Performance: `Int64Array` construction reuses a pre-sized `Vec`, no per-row Arrow builder overhead. Cumulative-offset table is built once per scan, not per row group.
- API: `PlannedColumnSource` additions are non-breaking (it's `pub(crate)` — confirm).
- Test coverage: round-trip, pruning correctness, explicit-projection-only, unknown-name rejection.

If `REVISE`, address findings and re-review.

- [ ] **Step 13: Commit and merge**

```bash
git add crates/bqlite-storage/src/segment/reader.rs
git commit -m "TASK-508: Synthesise __seq_id/__batch_id in segment reader"

git checkout main
git pull origin main
git merge task/TASK-508 --ff-only
git push origin main
git checkout task/TASK-508
```

---

## Checkpoint 3: ScanOperator output schema includes system columns

**Files:**
- Modify: `crates/bqlite-operators/src/scan.rs`
  - `VecSegment` type alias (test fixtures) — add per-segment `seq_id_first` + `batch_id`
  - `make_batch` test helper — append synthesised `__seq_id` / `__batch_id` columns
  - `minimal_arrow_schema` test helper — include the two system-column fields
  - `VecScan` (test fake `SegmentScan`) — synthesise system columns per row group when the projection requests them
  - module-level docs (the "Output schema" section, lines ~48-60)
  - `build_output_schema` (lines ~1041-1073)
  - explicit-projection validation
  - replace `row_tombstones_error_when_seq_id_column_missing` test
  - add a new test that proves row tombstones now filter correctly via the materialised `__seq_id` column

### Why the test-fake updates come first

After Step 4 of this checkpoint changes `build_output_schema` to include system columns, `ScanOperator::arrow_schema` becomes a 5-column schema. The k-way merge (`KWayMergeScan::new` in `bqlite-storage::segment::merge`) validates each per-segment batch's schema against this. Today, `VecScan::next_row_group` returns 3-column batches built by `make_batch`; that mismatch will break every existing scan test that uses `VecReader`. The test-fake updates (Steps 1–3 below) make `VecScan` emit batches that match the new contract before we change the operator schema.

- [ ] **Step 0a: Verify the merge-validation expectation**

```bash
grep -n "fn new\|schema\|validate" crates/bqlite-storage/src/segment/merge.rs | head -30
```

Confirm that `KWayMergeScan::new` (or its per-batch loop) validates batch schema against the operator-supplied Arrow schema. If validation is per-batch column-count or per-batch schema equality, the test-fake updates below are necessary. If it only checks the entity-key/ts column types, the fake updates may be optional but are still recommended for honesty.

- [ ] **Step 0b: Update the `VecSegment` fixture and test helpers**

In the `mod tests` block of `crates/bqlite-operators/src/scan.rs`:

1. Extend `VecSegment` from a 3-tuple to a 5-tuple, adding per-segment `seq_id_first` and `batch_id`:

```rust
type VecSegment = (
    SegmentHandle,
    Vec<RecordBatch>,
    Vec<HashMap<String, ZoneMap>>,
    /// First `__seq_id` (synthesised per system-columns.md §3).
    u64,
    /// `__batch_id` for every row in this fixture's segment.
    u64,
);
```

2. Update `minimal_arrow_schema` to include the system columns:

```rust
fn minimal_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("entity_id", DataType::Utf8View, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("event_type", DataType::Utf8View, false),
        Field::new("__seq_id", DataType::Int64, false),
        Field::new("__batch_id", DataType::Int64, false),
    ]))
}
```

3. Update `make_batch` to take seq/batch and synthesise the columns:

```rust
fn make_batch(ids: &[&str], tss: &[i64], evts: &[&str]) -> RecordBatch {
    make_batch_at(ids, tss, evts, 0, 0)
}

fn make_batch_at(
    ids: &[&str],
    tss: &[i64],
    evts: &[&str],
    seq_id_first: u64,
    batch_id: u64,
) -> RecordBatch {
    use arrow::array::Int64Array;
    let n = ids.len();
    let ids: ArrayRef = Arc::new(StringViewArray::from(ids.to_vec()));
    let tss_arr: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(tss.iter().copied().map(Some).collect::<Vec<_>>())
            .with_timezone("UTC"),
    );
    let evts_arr: ArrayRef = Arc::new(StringViewArray::from(evts.to_vec()));
    let seq_arr: ArrayRef = Arc::new(Int64Array::from(
        (0..n).map(|i| (seq_id_first + i as u64) as i64).collect::<Vec<_>>()
    ));
    let bid_arr: ArrayRef = Arc::new(Int64Array::from(vec![batch_id as i64; n]));
    RecordBatch::try_new(
        minimal_arrow_schema(),
        vec![ids, tss_arr, evts_arr, seq_arr, bid_arr],
    )
    .unwrap()
}
```

The `make_batch` shim preserves backwards compatibility for existing call sites that don't care about specific seq_id/batch_id values; tests that DO care call `make_batch_at` directly.

- [ ] **Step 0c: Update every `VecSegment` literal in the file**

```bash
grep -n "VecSegment\|VecReader::with_segments\|with_segments(" crates/bqlite-operators/src/scan.rs | head -40
```

For each `(handle, batches, zones)` 3-tuple literal building a `VecSegment`, append the seq/batch arguments. Most existing tests don't care about specific values — pass `(0, 0)` and let the synthesised columns carry seq_ids `[0..row_count)` and batch_id `0`. For tests that were specifically testing seq_id-aware behaviour (the rewritten row-tombstone test, see Step 5), pass meaningful values.

The `MergeSourcesOperator` test fixtures (lines ~3793+) use a different helper `merge_sources_batch` and `make_merge_sources_sub` — apply the same upgrade there: those sub-scans must emit `__seq_id` / `__batch_id` columns once CP4 lands, so it's cheaper to do the test-fake plumbing here in CP3 than to spread it across two checkpoints. Update `merge_sources_table_schema` and `merge_sources_batch` to include the system columns; existing combined-schema test helpers (`combined_schema_two`) build their own combined schemas and are addressed in CP4.

- [ ] **Step 0d: Run the operators test suite to confirm fakes still produce coherent batches**

```bash
cargo test -p bqlite-operators
```

Expected: PASS — at this point `build_output_schema` still returns 3 columns (no operator change yet), and `VecScan` returns 5-column batches. The merge will reject the mismatch. Some tests SHOULD break here. That confirms the validation gate is real.

If tests pass unexpectedly, the merge does NOT validate per-batch schema — in which case the test-fake updates are honesty-only and Steps 1+ proceed without urgency. Document the finding in the commit message either way.

- [ ] **Step 1: Write the failing test for ScanOperator output schema**

Append to the existing tests module at the bottom of `crates/bqlite-operators/src/scan.rs`:

```rust
#[test]
fn scan_operator_output_schema_includes_system_columns_for_full_projection() {
    use bqlite_core::schema::{BATCH_ID_COLUMN, SEQ_ID_COLUMN};
    let (reader, _tmp) = single_segment_reader_three_rows(/* seq_first */ 1000, /* batch */ 7);
    let op = ScanOperator::full_scan(reader).unwrap();
    let schema = op.output_schema();
    assert!(
        schema.column(SEQ_ID_COLUMN).is_some(),
        "scan operator output schema must include __seq_id (system-columns.md §4.1)"
    );
    assert!(
        schema.column(BATCH_ID_COLUMN).is_some(),
        "scan operator output schema must include __batch_id (system-columns.md §4.1)"
    );
}
```

If `single_segment_reader_three_rows` doesn't exist, build a small helper that wraps an existing fixture (look for `VecReader::with_segments` usage already in this file; the test at line ~3470 has a working pattern).

- [ ] **Step 2: Run, expect failure**

```bash
cargo test -p bqlite-operators --lib scan_operator_output_schema_includes_system_columns_for_full_projection
```

Expected: FAIL — `build_output_schema` currently returns `table.columns().to_vec()`, omitting system columns.

- [ ] **Step 3: Update `build_output_schema`**

Edit `crates/bqlite-operators/src/scan.rs` `build_output_schema` (around line 1041):

```rust
fn build_output_schema(
    table: &TableSchema,
    projection: &ColumnProjection,
) -> Result<OperatorSchema> {
    use bqlite_core::schema::{BATCH_ID_COLUMN, SEQ_ID_COLUMN};

    let columns: Vec<ColumnDef> = if projection.is_all() {
        // Empty projection means "every column" — declared followed
        // by the two system columns. This must match the reader's
        // projection expansion in segment/reader.rs::build_scan_plan
        // because the k-way merge validates each per-segment batch's
        // schema against the operator-supplied one
        // (system-columns.md §4.1).
        table.logical_columns().collect()
    } else {
        // Validate every requested name first; system columns are
        // recognised in addition to declared columns.
        for name in projection.columns() {
            let is_declared = table.column(name).is_some();
            let is_system = name == SEQ_ID_COLUMN || name == BATCH_ID_COLUMN;
            if !is_declared && !is_system {
                return Err(BqliteError::Schema(format!(
                    "scan: projected column `{name}` not in table `{}` \
                     and is not a recognised system column",
                    table.name()
                )));
            }
        }
        // Output: declared columns in table-schema order (filtered),
        // then any explicitly requested system columns in request
        // order. Matches the reader's `build_scan_plan` ordering.
        let projected: std::collections::HashSet<&str> =
            projection.columns().iter().map(String::as_str).collect();
        let mut out: Vec<ColumnDef> = table
            .columns()
            .iter()
            .filter(|col| projected.contains(col.name.as_str()))
            .cloned()
            .collect();
        if projected.contains(SEQ_ID_COLUMN) {
            out.push(ColumnDef::required(SEQ_ID_COLUMN, BqlType::Int));
        }
        if projected.contains(BATCH_ID_COLUMN) {
            out.push(ColumnDef::required(BATCH_ID_COLUMN, BqlType::Int));
        }
        out
    };
    OperatorSchema::new(columns)
}
```

- [ ] **Step 4: Run the new test, expect PASS**

```bash
cargo test -p bqlite-operators --lib scan_operator_output_schema_includes_system_columns_for_full_projection
```

Expected: PASS.

- [ ] **Step 5: Replace the now-obsolete `row_tombstones_error_when_seq_id_column_missing` test**

Find the test at lines ~3488-3520 and replace its body to instead prove that row tombstones now apply because `__seq_id` is materialised. Rename if needed:

```rust
#[test]
fn row_tombstones_filter_via_materialised_seq_id() {
    // After TASK-508 the scan output carries __seq_id, so a row-level
    // tombstone (TombstoneFile::for_rows) filters out exactly the row
    // whose synthesised __seq_id matches.
    let segments: Vec<VecSegment> = vec![(
        handle_for(1, 0, 0, 1),
        vec![make_batch_at(&["alice"], &[100], &["e1"], /* seq_first */ 42, /* batch */ 1)],
        vec![zones_for("alice", "alice", 100, 100)],
        /* seq_id_first */ 42,
        /* batch_id    */ 1,
    )];
    let reader: Arc<dyn SegmentReader> =
        Arc::new(VecReader::with_segments(minimal_schema(), segments));
    // tombstone targets seq_id 42 (the only row in the segment).
    let snap = TombstoneSnapshot::from_map([((0, 0), TombstoneFile::for_rows([42]))]);
    let mut op = ScanOperator::with_tombstones(
        reader,
        &[],
        Vec::new(),
        CancellationToken::new(),
        Arc::new(snap),
    )
    .unwrap();
    op.open().unwrap();
    let mut total_rows: usize = 0;
    while let Some(b) = op.next_batch().unwrap() {
        total_rows += b.num_rows();
    }
    assert_eq!(total_rows, 0, "row tombstone for seq_id=42 should drop the only row");
}
```

After Step 0b, `make_batch_at(&["alice"], &[100], &["e1"], 42, 1)` already emits a 5-column batch where `__seq_id = [42]` and `__batch_id = [1]`. The test below uses that helper directly. `TombstoneScanWrapper::next_row_group` (in `crates/bqlite-storage/src/tombstone_scan.rs`) expects `__seq_id` and `__batch_id` columns to be present on each batch — that requirement is now satisfied by the test fake; verify by reading `TombstoneFilter::apply_row_deletes` (in `crates/bqlite-storage/src/tombstone.rs:474+`).

- [ ] **Step 6: Run the new test**

```bash
cargo test -p bqlite-operators --lib row_tombstones_filter_via_materialised_seq_id
```

Expected: PASS. If it fails because of the `VecReader` mock issue, fix the mock and re-run before proceeding.

- [ ] **Step 7: Update existing scan tests that assumed system columns were absent**

Search for other tests in `crates/bqlite-operators/src/scan.rs` that assert column counts or schema shapes against the declared-only set:

```bash
grep -n "minimal_schema().columns().to_vec()" crates/bqlite-operators/src/scan.rs
```

Tests that build an `OperatorSchema` from `minimal_schema().columns().to_vec()` and pass it to a sub-operator are exercising operators OTHER than scan; those keep working because they're not testing scan's schema shape. But the test `output_schema_reflects_declared_columns_for_full_scan` (line ~2198) names a contract that has now changed. Rename and update it:

```rust
#[test]
fn output_schema_reflects_declared_columns_plus_system_for_full_scan() {
    use bqlite_core::schema::{BATCH_ID_COLUMN, SEQ_ID_COLUMN};
    // Per system-columns.md §4.1, the empty-projection scan emits
    // every declared column followed by __seq_id and __batch_id.
    let table = minimal_schema();
    let reader: Arc<dyn SegmentReader> =
        Arc::new(VecReader::with_segments(table.clone(), Vec::new()));
    let op = ScanOperator::full_scan(reader).unwrap();
    let names: Vec<&str> = op
        .output_schema()
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let mut expected: Vec<&str> = table.columns().iter().map(|c| c.name.as_str()).collect();
    expected.push(SEQ_ID_COLUMN);
    expected.push(BATCH_ID_COLUMN);
    assert_eq!(names, expected);
}
```

- [ ] **Step 8: Update the module-level docs**

Replace the "Output schema" section in `crates/bqlite-operators/src/scan.rs` (lines ~48-60) with:

```text
//! ## Output schema
//!
//! The scan's [`OperatorSchema`] reflects the declared columns that
//! the reader materialises plus the implicit system columns
//! `__seq_id` and `__batch_id`, in that order. Empty
//! `projected_columns` produces the full set
//! (`OperatorSchema::from_table(table)` shape — see
//! `docs/design/storage/system-columns.md` §4.1). Explicit projections
//! may name the system columns and they pass through; `SegmentFileScan`
//! synthesises both columns from the segment footer's `seq_id_range`
//! and `batch_id` (storage-format.md §6.2).
```

- [ ] **Step 9: Run the full operators test suite**

```bash
cargo test -p bqlite-operators
```

Expected: PASS. If a test fails because it built an `OperatorSchema` from `minimal_schema().columns()` and then validated it against the scan's output, fix by either:
- Updating the assertion to include `__seq_id` / `__batch_id`, OR
- Switching the explicit projection to omit system columns when the test specifically wants the declared-only set.

Each adjustment must be deliberate; don't blanket-update.

- [ ] **Step 10: Run local-ci**

```bash
scripts/local-ci.sh
```

Expected: PASS.

- [ ] **Step 11: Subagent code review**

Spawn a subagent. Reviewer must check:
- The reader (CP2) and scan operator (CP3) agree on column ordering — the k-way merge validates `RecordBatch.schema()` against `OperatorSchema::to_arrow_schema()` and any divergence corrupts the merge.
- `MergeSourcesOperator::new`'s `validate_key_types` call (line ~1524) is unaffected — it looks up `entity_key_col` / `ts_col` by index, and those indices are computed against the sub-scan's output schema which now happens to be longer; both paths use `OperatorSchema::column(name)` for resolution so the lookup remains correct.
- Performance: `OperatorSchema::from_table` already exists and is used elsewhere; reusing it doesn't allocate per-batch.
- Test coverage: full-projection schema, explicit-projection-only, row-tombstone filtering via materialised `__seq_id`.

If `REVISE`, address findings and re-review.

- [ ] **Step 12: Commit and merge**

```bash
git add crates/bqlite-operators/src/scan.rs
git commit -m "TASK-508: ScanOperator emits __seq_id/__batch_id in output schema"

git checkout main
git pull origin main
git merge task/TASK-508 --ff-only
git push origin main
git checkout task/TASK-508
```

---

## Checkpoint 4: MergeSources combined schema includes system columns

**Files:**
- Modify: `crates/bqlite-planner/src/logical.rs`
  - `build_joined_scan` (around lines 1290-1342) — add system columns to combined schema, rewrite the comment
  - `joined_pipeline_omits_system_columns_from_combined_schema` test (lines ~8068-8102) — flip to assert presence, rename to `joined_pipeline_includes_system_columns_in_combined_schema`
- Verify: no change needed to `MergeSourcesOperator` — its `col_map` resolution at lines ~1473-1481 already handles bare-named system columns (`if sub_col.is_system()` branch).
- Verify: no change needed to `bqlite-planner::physical::lower_to_physical` for joined sources — the projected_columns it passes to each sub-`ScanPhysical` is `Vec::new()` (empty == all) per the existing logical-plan `build_joined_scan`, which already gives every sub-scan the full set after CP3.

- [ ] **Step 1: Write the failing test (flip the existing regression-guard)**

Replace the test `joined_pipeline_omits_system_columns_from_combined_schema` (line ~8068) with:

```rust
#[test]
fn joined_pipeline_includes_system_columns_in_combined_schema() {
    // Per `docs/design/storage/system-columns.md` §4.2, the joined
    // schema declares __seq_id and __batch_id as bare-named, NOT NULL,
    // Int columns. They are populated by MergeSourcesOperator's
    // bare-name resolution path against each sub-scan's emitted system
    // columns (now that ScanOperator materialises them as of TASK-508).
    let cat = InMemoryCatalog::default()
        .with(purchases_schema())
        .with(logins_schema());
    let mut pipeline = bare_pipeline("purchases");
    pipeline.source.joins.push(TableRef {
        name: Name::synthetic("logins"),
        span: Span::EMPTY,
    });
    let plan = lower_statement(Statement::Query(pipeline), &cat).unwrap();
    let LogicalPlan::Scan { output_schema, .. } = plan else {
        panic!("expected Scan");
    };
    let names: Vec<&str> = output_schema
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert!(names.contains(&"__seq_id"), "expected __seq_id in {names:?}");
    assert!(names.contains(&"__batch_id"), "expected __batch_id in {names:?}");
    let (_, seq) = output_schema.column("__seq_id").unwrap();
    assert_eq!(seq.bql_type, BqlType::Int);
    assert!(!seq.nullable);
    let (_, bid) = output_schema.column("__batch_id").unwrap();
    assert_eq!(bid.bql_type, BqlType::Int);
    assert!(!bid.nullable);
}
```

- [ ] **Step 2: Run, expect failure**

```bash
cargo test -p bqlite-planner --lib joined_pipeline_includes_system_columns_in_combined_schema
```

Expected: FAIL — `build_joined_scan` currently skips system columns and never appends them.

- [ ] **Step 3: Update `build_joined_scan`**

Edit `crates/bqlite-planner/src/logical.rs` around lines 1290-1342. Replace the existing comment about omitting system columns with:

```rust
    // Discriminator + system columns. Per
    // `docs/design/storage/system-columns.md` §4.2, the combined
    // schema declares `__source_table_id`, `__seq_id`, and
    // `__batch_id` as bare-named, NOT NULL, Int. The merge picks one
    // row from one sub-scan at a time, and that sub-scan's emitted
    // `__seq_id` / `__batch_id` populate the output (each sub-scan
    // materialises them as of TASK-508 — see `bqlite-operators::scan`
    // module docs).
    cols.push(ColumnDef::required(SOURCE_TABLE_ID_COLUMN, BqlType::Int));
    cols.push(ColumnDef::required(
        bqlite_core::schema::SEQ_ID_COLUMN,
        BqlType::Int,
    ));
    cols.push(ColumnDef::required(
        bqlite_core::schema::BATCH_ID_COLUMN,
        BqlType::Int,
    ));
```

- [ ] **Step 4: Run the new test, expect PASS**

```bash
cargo test -p bqlite-planner --lib joined_pipeline_includes_system_columns_in_combined_schema
```

Expected: PASS.

- [ ] **Step 5: Run all planner tests; investigate regressions**

```bash
cargo test -p bqlite-planner
```

Some tests that snapshot the joined-schema column count will break. Update each one to reflect the +2 columns. Pay particular attention to:
- `joined_pipeline_lowers_with_combined_schema_and_discriminator` (line ~8030) — assertion on existing names should still pass; if it asserts a length, bump the count.
- Any explain-format / golden-output snapshots in `explain.rs` tests.
- `crates/bqlite-planner/src/opt/sample_pushdown.rs` tests — see lines 345, 554 for `MergeSourcesPhysical` test fixtures; they may build combined schemas inline that need the +2 columns.

- [ ] **Step 6: Run all operator tests; investigate MergeSources regressions**

```bash
cargo test -p bqlite-operators
```

The `MergeSourcesOperator` tests at lines 3793+ build their own combined schemas via `combined_schema_two`. If those schemas don't include the system columns, the operator's `validate_key_types` and `col_map` should still work for the test's own schema, since the operator only looks for `__source_table_id` and bare-named system columns when they are in the schema. Confirm by reading `combined_schema_two` (line ~3876) and the tests' `OperatorSchema::new(...)` calls.

If a test does break, the fix depends on intent:
- Tests that simulate planner-produced combined schemas should be updated to include `__seq_id` / `__batch_id`.
- Tests that explicitly verify the degenerate "no system columns" case can keep their current schema if the operator gracefully handles it. The operator's existing `if sub_col.is_system()` resolution already passes when the combined schema OMITS them (the sub-scan column maps to `None`).

Document the fixes in the commit message.

- [ ] **Step 7: Run the full workspace tests**

```bash
cargo test --workspace
```

Investigate any cross-crate regressions. Common candidates:
- `tests/tests/wave4_acceptance.rs` and `tests/tests/wave4_advanced_analytics_*.rs` may snapshot joined-schema counts.
- `tests/tests/smoke.rs` may assume the wave-2 single-table scan output shape.

For each failing assertion, decide whether the test is correct (then update the assertion to include the system columns) or whether it captures a real regression (then fix the code, not the test). Per CLAUDE.md, fix code over tests when in doubt.

- [ ] **Step 8: Run local-ci**

```bash
scripts/local-ci.sh
```

Expected: PASS.

- [ ] **Step 9: Subagent code review**

Spawn a subagent. Reviewer must check:
- `build_joined_scan` correctly imports `bqlite_core::schema::{SEQ_ID_COLUMN, BATCH_ID_COLUMN}` (or via the existing prelude).
- The combined schema column ORDER matches what `MergeSourcesOperator` expects: discriminator before system columns is the existing pattern; document the chosen order in the comment.
- `MergeSourcesOperator::new` correctly resolves system columns via the `if sub_col.is_system()` branch (lines ~1473-1481) — no code change should be needed, but verify by reading it.
- Performance: no per-row allocation; the schema is built once at planner-time.
- All workspace tests pass.

If `REVISE`, address findings and re-review.

- [ ] **Step 10: Commit and merge**

```bash
git add crates/bqlite-planner/src/logical.rs \
        $(git diff --name-only)  # any test files updated for the +2 columns
git commit -m "TASK-508: Joined-source combined schema includes __seq_id/__batch_id"

git checkout main
git pull origin main
git merge task/TASK-508 --ff-only
git push origin main
git checkout task/TASK-508
```

---

## Checkpoint 5: Cross-cutting doc reconciliation

**Files:**
- Modify: `crates/bqlite-core/src/storage.rs:154-160` (sharpen if necessary)
- Modify: `crates/bqlite-planner/src/explain.rs` (verify EXPLAIN renders the new schema correctly; adjust column-count assertions in any snapshot tests)
- Modify: `docs/design/execution-model.md` §3.7 if the column-order rule there contradicts CP2/CP3 ordering
- Modify: `docs/quality-score.md` (only if the per-crate quality grade for `bqlite-storage` / `bqlite-operators` / `bqlite-planner` changed materially; otherwise skip)

This checkpoint reconciles design documents and code comments that previously documented the system-column carve-out. The following are mandatory (cannot be skipped):

- `crates/bqlite-operators/src/scan.rs` module docs — confirmed updated in CP3 Step 8. Re-verify.
- `crates/bqlite-planner/src/logical.rs:1319-1329` — confirmed rewritten in CP4 Step 3. Re-verify.
- `crates/bqlite-storage/src/segment/reader.rs::SegmentFileReader::scan` doc-block — confirmed updated in CP2 Step 10. Re-verify.

The rest of this checkpoint is optional cleanup (skip-by-decision OK if all `grep` output is already coherent post-CP4).

- [ ] **Step 1: Re-read every design doc mentioning system columns**

```bash
grep -rln "__seq_id\|__batch_id" docs/design/
```

For each file in the result, read the section that mentions the columns and confirm it agrees with `docs/design/storage/system-columns.md`. Update inline.

- [ ] **Step 2: Verify EXPLAIN output**

```bash
cargo test -p bqlite-planner --lib -- explain
```

If any EXPLAIN snapshot includes the joined combined schema and asserts column count, update.

- [ ] **Step 3: Run local-ci**

```bash
scripts/local-ci.sh
```

Expected: PASS.

- [ ] **Step 4: Subagent code review (if any non-trivial doc edit)**

Spawn a subagent only if material content changed. For pure markdown polish, skip.

- [ ] **Step 5: Commit and merge (if any change)**

```bash
git add docs/
git commit -m "TASK-508: Reconcile prior design docs with system-column contract"

git checkout main
git pull origin main
git merge task/TASK-508 --ff-only
git push origin main
git checkout task/TASK-508
```

---

## Completion Protocol

Per AGENTS.md:

```bash
git mv tasks/active/TASK-508.lock tasks/completed/TASK-508.done
# Edit the .done JSON to add `completed_at: "<UTC ISO-8601>"`
git add tasks/completed/TASK-508.done
git commit -m "TASK-508: completed"

git checkout main
git pull origin main
git merge task/TASK-508 --ff-only
git push origin main
```

End the turn. Do not claim another task.

---

## Risk register

- **R1: Compaction-output `seq_id_range` semantics.** The plan assumes every segment's `seq_id_range.0 + offset` formula holds for both ingest-written and compaction-written segments. CP1 §2 documents this assumption. The compaction code at `crates/bqlite-storage/src/compaction.rs:496` allocates a fresh contiguous range, so the assumption holds today. If a future compaction change breaks this, the system-column derivation breaks with it; the design doc is the single source of truth and any compactor change must preserve the invariant.
- **R2: Test churn from combined-schema +2 columns.** CP4 will probably break a non-trivial number of test snapshots. Each one should be updated deliberately. If the count of breakages exceeds ~10, consider splitting CP4 into "code change" + "test fixture update" sub-checkpoints to keep diffs reviewable.
- **R3: VecReader test fake.** CP3 assumes the in-test `VecReader` can carry `seq_id_range` / `batch_id` configuration to its synthesised batches. If `VecReader` is structurally hostile to that change, fall back to a real-segment fixture for the row-tombstone test instead of fighting the mock.
- **R4: Encoded path correctness.** CP2's encoded-path synthesis emits `EncodedColumn::Materialized`. Downstream encoded kernels may have invariants assuming an encoded-only batch; verify with `cargo test -p bqlite-operators` and inspect any kernel that walks `EncodedBatch::columns()` to ensure it tolerates `Materialized` variants for arbitrary columns. The existing `BackfillAllNull` path already uses `Materialized`, so this should be safe by construction.

---

## Self-review

- **Spec coverage:** Every requirement from TASK-508's description ("Materialize `__seq_id` and `__batch_id` end-to-end in ScanOperator and MergeSources, reconcile nullability/type rules, and make the system-column contract explicit in the docs") is covered. The "correctness unblocker for EventSelect runtime, joined-source scans, and row/batch tombstone filtering" follow-on impact is left to TASK-509 per the task index.
- **Placeholder scan:** No "TBD"/"TODO"/"implement later". Each step has the actual code/command needed.
- **Type consistency:** `SEQ_ID_COLUMN` / `BATCH_ID_COLUMN` referenced from `bqlite_core::schema` in both reader (CP2), scan operator (CP3), and joined-source planner (CP4). `BqlType::Int` → Arrow `Int64` mapping comes from `bqlite_core::arrow::bql_type_to_arrow` (already in use). `Int64Array` is the synthesised array type throughout.
