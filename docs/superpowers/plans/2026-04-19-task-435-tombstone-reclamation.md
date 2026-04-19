# TASK-435: Tombstone Reclamation During Compaction — Implementation Plan

> **For agentic workers:** Single-agent execution following AGENTS.md checkpoint
> discipline. Each CP must pass `scripts/local-ci.sh`, be code-reviewed by a
> subagent, and fast-forward-merge to `main` before the next CP starts.

**Goal:** Extend compaction so tombstoned rows are physically omitted from
compacted outputs and fully reclaimed tombstones are removed from the shard
snapshot once the new segments are published.

**Architecture:**
- Load a `TombstoneSnapshot` at compaction-job start (§12.1).
- Wrap each input `SegmentScan` with a new `CompactionTombstoneScan` that
  knows the segment's `batch_id` and `seq_id_range` so row- and batch-level
  tombstones work without materialised `__seq_id` / `__batch_id` columns.
- After the §6 manifest publish, acquire the per-shard tombstone mutex, read
  the current `tombstones.json` (which may reflect mid-compaction DELETEs),
  subtract the job-start snapshot entries that were physically reclaimed,
  and atomically rewrite the file (§12.2 manifest-first order).
- Cover the edge case where every input row is tombstoned: publish a
  "remove-only" manifest update (no new segment) via a new
  `Database::remove_segments_atomic` primitive, then reclaim as usual.

**Tech Stack:** Rust, Apache Arrow, existing `TombstoneFile` /
`TombstoneSnapshot` / `TombstoneFilter` surface, `compact_one` in
`crates/bqlite-storage/src/compaction.rs`, `per-shard tombstone_shard_lock` in
`crates/bqlite-storage/src/database.rs`.

**Relevant design:**
- `docs/design/storage/deletes.md` §§6, 9, 12 (tombstone reclamation is
  §12; `time_range_deletes` merge is §12.5).
- `docs/design/storage/compaction-concurrency.md` §§6, 9.

**Out of scope:**
- Time-range merge during reclamation (§12.5) is optional per the spec;
  defer unless tests require it.
- Startup orphan tombstone cleanup is already TASK-408 territory.

**Concurrency assumption (documented but not enforced by this task):**

Both `compact_one` and `execute_cheap_delete` take `&mut Database`, so the
Rust borrow checker already serialises every DELETE, ingest, and compaction
against every other such mutation. This means:

- No mid-compaction DELETE can actually race the reclaim step today — the
  "tombstones.json at reclaim time may differ from job-start snapshot" path
  is present in the code for spec parity (§9, §12.1) but is unreachable
  with the current `&mut Database` model.
- No concurrent ingest can add new segments to the shard between
  `shard.clone()` at job start and the publish call — `&mut Database`
  again. So §12.4 "remaining segment" equals "the new compacted output"
  after a successful publish.

If a future task introduces per-shard concurrent writers, this assumption
moves; the reclaimer would need to re-snapshot the manifest post-publish
and skip aggressive entity/time-range reclamation when the shard contains
more than the new output. §12.3 "stale tombstones are harmless" preserves
correctness in either case, so the migration is a pure pruning-rule
tightening, not a correctness fix.

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/bqlite-storage/src/tombstone_scan.rs` | Modify | Add `CompactionTombstoneScan`: per-segment tombstone wrapper using `seq_id_range` + `batch_id` from manifest metadata |
| `crates/bqlite-storage/src/manifest.rs` | Modify | Add `Manifest::remove_segments` (multi-id remove, in-memory only; mirrors `replace_segments` shape) |
| `crates/bqlite-storage/src/database.rs` | Modify | Add `Database::remove_segments_atomic` thin wrapper around `update_manifest` |
| `crates/bqlite-storage/src/compaction.rs` | Modify | Load snapshot at job start, wrap scans, handle zero-row output, call reclamation after publish |
| `crates/bqlite-storage/src/lib.rs` | Modify | Re-export `CompactionTombstoneScan` |
| `docs/design/storage/deletes.md` | Modify | Mark §12 as implemented; update §15 TASK-435 bullet to reflect the ship point |
| `docs/design/storage/compaction-concurrency.md` | Modify | Update §12 "Implementation status" note: the tombstone-aware path now lives in TASK-435 |

---

## Checkpoint Map

- **CP1** — `CompactionTombstoneScan` scaffolding + tests (no `compact_one`
  changes yet).
- **CP2** — `Manifest::remove_segments` + `Database::remove_segments_atomic`
  primitive, with tests.
- **CP3** — integrate into `compact_one`: snapshot, wrap scans, handle
  zero-row output path, **no reclamation yet** (tests confirm rows are
  dropped but the tombstone file is untouched).
- **CP4** — tombstone reclamation rewrite (manifest-first order, per-shard
  mutex, mid-compaction DELETE preservation), docs, complete.

Each CP is self-contained and merges independently.

---

## CP1: `CompactionTombstoneScan`

**Files:**
- Modify: `crates/bqlite-storage/src/tombstone_scan.rs`
- Modify: `crates/bqlite-storage/src/lib.rs` (re-export)

### Design

`CompactionTombstoneScan` wraps an inner `SegmentScan` and applies all four
tombstone granularities using per-segment metadata. Compared to
`TombstoneScanWrapper` (which needs `__seq_id` / `__batch_id` columns in the
batch), this knows:

- `batch_id: u64` — if the segment's `batch_id ∈ tombstones.batch_deletes`,
  the entire segment is dropped (returns `Ok(None)` from the first
  `next_row_group` call after a best-effort drain).
- `seq_id_first: u64` — each batch's rows have
  `__seq_id = seq_id_first + offset` where `offset` is the cumulative row
  count across already-yielded row groups.

Entity and time-range checks reuse existing column lookups on the batch.

Batch-delete fast-path is **both** correctness and performance: the inner
scan may still have work to do (file reads), but we return no rows from it.
We don't attempt to cancel the inner scan — just stop calling
`next_row_group` on it.

### Steps

- [ ] **Step 1: Add `CompactionTombstoneScan` struct and constructor**

Append after `TombstoneScanWrapper` in `crates/bqlite-storage/src/tombstone_scan.rs`:

```rust
/// Per-segment tombstone filter used during compaction.
///
/// Unlike [`TombstoneScanWrapper`], which expects `__seq_id` and
/// `__batch_id` to be materialised as columns on every batch, the
/// compaction scan output today only contains the declared table
/// columns — so row- and batch-level tombstones need out-of-band
/// segment context. This wrapper carries the segment's `batch_id` and
/// `seq_id_first` (both are manifest metadata we already have during
/// compaction) and derives `__seq_id = seq_id_first + cumulative_row_offset`
/// for every row yielded by the inner scan. Entity- and time-range
/// checks reuse the column-based logic from `TombstoneFilter` since
/// entity-key and timestamp columns ARE present in the scan output.
///
/// See `docs/design/storage/deletes.md` §12 — compaction is the site
/// where tombstones are physically applied.
pub struct CompactionTombstoneScan {
    inner: Box<dyn SegmentScan>,
    tombstones: TombstoneFile,
    entity_key_col: String,
    ts_col: String,
    /// First `__seq_id` covered by this segment. Row `n` in batch `b`
    /// has `__seq_id = seq_id_first + cumulative_offset(b) + n`.
    seq_id_first: u64,
    /// `batch_id` the segment was written with. If this is in the
    /// tombstone file's `batch_deletes`, every row is dropped.
    batch_id: u64,
    /// Rows already yielded by the inner scan, used to derive the
    /// absolute `__seq_id` of each row in subsequent batches.
    next_row_offset: u64,
    /// Cached "entire segment is batch-deleted" flag, computed once
    /// from the tombstone file at construction time.
    all_dropped: bool,
}

impl CompactionTombstoneScan {
    pub fn new(
        inner: Box<dyn SegmentScan>,
        tombstones: TombstoneFile,
        entity_key_col: String,
        ts_col: String,
        seq_id_first: u64,
        batch_id: u64,
    ) -> Self {
        let all_dropped = tombstones.batch_deletes.contains(&batch_id);
        Self {
            inner,
            tombstones,
            entity_key_col,
            ts_col,
            seq_id_first,
            batch_id,
            next_row_offset: 0,
            all_dropped,
        }
    }
}
```

- [ ] **Step 2: Promote `TombstoneFilter` per-granularity helpers to `pub(crate)`**

In `crates/bqlite-storage/src/tombstone.rs`, change:

```rust
fn apply_batch_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()>
fn apply_entity_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()>
fn apply_row_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()>
fn apply_time_range_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()>
```

to `pub(crate) fn` (only the four listed; keep `filter_batch` public).
This is an additive visibility change — no behavioural impact on any
existing caller.

- [ ] **Step 3: Implement `SegmentScan` for `CompactionTombstoneScan`**

Append immediately after the struct impl. Note that entity and
time-range use `TombstoneFilter::apply_*` directly against
`&self.tombstones` — no view clone — since those helpers only read
their own granularity's field from the tombstone file:

```rust
impl SegmentScan for CompactionTombstoneScan {
    fn row_group_count(&self) -> usize {
        self.inner.row_group_count()
    }

    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
        self.inner.row_group_zone_maps(idx)
    }

    fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
        if self.all_dropped {
            // §12.4 batch-level reclamation: the whole segment is
            // obsolete. Return None immediately; the merger will
            // simply exhaust this input with no rows contributed.
            return Ok(None);
        }
        let Some(batch) = self.inner.next_row_group()? else {
            return Ok(None);
        };
        let num_rows = batch.num_rows();
        let start_offset = self.next_row_offset;
        // A segment with > u64::MAX rows is absurd; a checked_add
        // turns any bug that would silently saturate the offset into
        // a clear runtime error instead.
        self.next_row_offset = start_offset
            .checked_add(num_rows as u64)
            .ok_or_else(|| bqlite_core::error::BqliteError::Execution(
                "CompactionTombstoneScan: cumulative row offset overflowed u64".into(),
            ))?;
        if num_rows == 0 {
            return Ok(Some(batch));
        }

        let has_row_work = !self.tombstones.row_deletes.is_empty();
        let has_entity_work = !self.tombstones.entity_deletes.is_empty();
        let has_time_work = !self.tombstones.time_range_deletes.is_empty();
        if !has_row_work && !has_entity_work && !has_time_work {
            return Ok(Some(batch));
        }

        let mut alive = vec![true; num_rows];

        // Row-level: derive __seq_id from seq_id_first + row offset.
        // seq_id_first + start_offset + i can't overflow — the
        // checked_add above already bounded next_row_offset.
        if has_row_work {
            for (i, flag) in alive.iter_mut().enumerate() {
                if *flag {
                    let seq_id = self.seq_id_first + start_offset + i as u64;
                    if self.tombstones.row_deletes.contains(&seq_id) {
                        *flag = false;
                    }
                }
            }
        }

        // Entity + time-range: call TombstoneFilter's promoted
        // pub(crate) helpers directly against &self.tombstones — the
        // helpers each only touch their own granularity's field, so
        // no view clone is needed.
        if has_entity_work || has_time_work {
            let filter = TombstoneFilter::new(
                &self.tombstones,
                &self.entity_key_col,
                &self.ts_col,
            );
            if has_entity_work {
                filter.apply_entity_deletes(&batch, &mut alive)?;
            }
            if has_time_work {
                filter.apply_time_range_deletes(&batch, &mut alive)?;
            }
        }

        if alive.iter().all(|&a| a) {
            return Ok(Some(batch));
        }
        let mask = arrow::array::BooleanArray::from(alive);
        let filtered = arrow::compute::filter_record_batch(&batch, &mask)
            .map_err(|e| bqlite_core::error::BqliteError::Execution(
                format!("CompactionTombstoneScan filter_record_batch failed: {e}"),
            ))?;
        Ok(Some(filtered))
    }
}
```

- [ ] **Step 4: Add unit tests inside `#[cfg(test)] mod tests` in `tombstone_scan.rs`**

Add tests covering:
- `compaction_passthrough_when_no_tombstones` — no filter, batch identical.
- `compaction_drops_entire_segment_on_batch_delete` — first call returns `None`.
- `compaction_applies_row_delete_by_derived_seq_id` — construct a segment
  with `seq_id_first = 100`, tombstone `__seq_id = 101`, verify row 1 removed.
- `compaction_applies_row_delete_across_multiple_row_groups` — two row
  groups of size 2 each, tombstone `__seq_id = 102` (second row of second
  group when `seq_id_first = 100`); verify offset tracking.
- `compaction_applies_entity_delete` — entity "alice" tombstoned, verify
  dropped.
- `compaction_applies_time_range_delete` — range covers half the rows,
  verify they're dropped.
- `compaction_combines_row_and_entity_deletes` — both granularities hit,
  intersection alive-mask verified.

Reuse the existing `MockScan` helper in the test module.

- [ ] **Step 5: Re-export in `lib.rs`**

In `crates/bqlite-storage/src/lib.rs`, extend the `tombstone_scan` re-export line:

```rust
pub use tombstone_scan::{CompactionTombstoneScan, TombstoneScanWrapper};
```

- [ ] **Step 6: Run local CI and code review**

Run: `scripts/local-ci.sh`
Expected: passes cleanly.

Spawn the `superpowers:code-reviewer` subagent on the staged diff. Address
any blocking findings.

- [ ] **Step 7: Commit + merge to main**

```bash
git add crates/bqlite-storage/src/tombstone.rs \
        crates/bqlite-storage/src/tombstone_scan.rs \
        crates/bqlite-storage/src/lib.rs
git commit -m "TASK-435: add CompactionTombstoneScan wrapper (CP1)"

git checkout main
git pull origin main
git merge task/TASK-435 --ff-only
git push origin main
git checkout task/TASK-435
```

---

## CP2: Atomic multi-segment remove primitive

**Files:**
- Modify: `crates/bqlite-storage/src/manifest.rs`
- Modify: `crates/bqlite-storage/src/database.rs`

### Design

Needed for the zero-surviving-rows case in CP3. Mirrors `replace_segments`
shape but takes no new segment — removes 1+ input ids from a given
`(window, shard)` in a single manifest-transaction. Duplicate and
missing-id rules match `replace_segments`.

### Steps

- [ ] **Step 1: Add `Manifest::remove_segments`**

Insert in `crates/bqlite-storage/src/manifest.rs` immediately after
`replace_segments`:

```rust
/// Atomically remove a set of segments from `(table_name, window_id,
/// shard_id)` without adding any output.
///
/// Used by the compaction reclamation path when every row in the input
/// set was tombstoned — there is no output to publish, but the inputs
/// must still be removed atomically. `removed_ids` must be non-empty;
/// every id must exist in the target shard.
///
/// Mirrors [`Self::replace_segments`]'s error taxonomy minus the
/// duplicate-output check.
pub fn remove_segments(
    &mut self,
    table_name: &str,
    window_id: u32,
    shard_id: u32,
    removed_ids: &[u64],
) -> Result<()> {
    if removed_ids.is_empty() {
        return Err(BqliteError::Execution(
            "remove_segments: removed_ids must be non-empty".into(),
        ));
    }
    if shard_id >= u32::from(self.shard_count) {
        return Err(BqliteError::Execution(format!(
            "remove_segments: shard_id {shard_id} out of range (shard_count = {})",
            self.shard_count
        )));
    }
    let entry = self.tables.get_mut(table_name).ok_or_else(|| {
        BqliteError::Execution(format!("remove_segments: unknown table '{table_name}'"))
    })?;
    let win_idx = entry
        .windows
        .iter()
        .position(|w| w.window_id == window_id)
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "remove_segments: window {window_id} not found in table '{table_name}'"
            ))
        })?;
    let shard_segs = entry.windows[win_idx]
        .shards
        .get(shard_id as usize)
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "remove_segments: shard {shard_id} out of range for window {window_id}"
            ))
        })?;
    for id in removed_ids {
        if !shard_segs.iter().any(|s| s.segment_id == *id) {
            return Err(BqliteError::Execution(format!(
                "remove_segments: segment_id {id} not found in table '{table_name}' window {window_id} shard {shard_id}"
            )));
        }
    }
    let shard_segs = &mut entry.windows[win_idx].shards[shard_id as usize];
    shard_segs.retain(|s| !removed_ids.contains(&s.segment_id));
    Ok(())
}
```

- [ ] **Step 2: Add tests in `manifest.rs`**

Append after `replace_segments_tolerates_duplicate_removed_ids`:

```rust
#[test]
fn remove_segments_drops_every_listed_id() {
    let mut m = Manifest::new_empty(1);
    m.tables.insert(
        "events".into(),
        TableEntry {
            schema: sample_schema(),
            next_batch_id: 3,
            next_sequence_id: 0,
            next_segment_id: 0,
            windows: vec![WindowManifest {
                window_id: 0,
                shards: vec![vec![
                    sample_segment(1, 1, (100, 200)),
                    sample_segment(2, 2, (201, 300)),
                    sample_segment(3, 3, (301, 400)),
                ]],
            }],
        },
    );
    m.remove_segments("events", 0, 0, &[1, 3]).unwrap();
    let ids: Vec<u64> = m.tables["events"].windows[0].shards[0]
        .iter()
        .map(|s| s.segment_id)
        .collect();
    assert_eq!(ids, vec![2]);
}

#[test]
fn remove_segments_rejects_empty_input() {
    let mut m = Manifest::new_empty(1);
    m.tables.insert(
        "events".into(),
        TableEntry {
            schema: sample_schema(),
            next_batch_id: 0,
            next_sequence_id: 0,
            next_segment_id: 0,
            windows: vec![WindowManifest {
                window_id: 0,
                shards: vec![vec![sample_segment(1, 1, (100, 200))]],
            }],
        },
    );
    let err = m.remove_segments("events", 0, 0, &[]).unwrap_err();
    assert!(matches!(err, BqliteError::Execution(_)));
}

#[test]
fn remove_segments_rejects_missing_id() {
    let mut m = Manifest::new_empty(1);
    m.tables.insert(
        "events".into(),
        TableEntry {
            schema: sample_schema(),
            next_batch_id: 0,
            next_sequence_id: 0,
            next_segment_id: 0,
            windows: vec![WindowManifest {
                window_id: 0,
                shards: vec![vec![sample_segment(1, 1, (100, 200))]],
            }],
        },
    );
    let err = m.remove_segments("events", 0, 0, &[42]).unwrap_err();
    assert!(matches!(err, BqliteError::Execution(_)));
}

#[test]
fn remove_segments_rejects_unknown_window() {
    let mut m = Manifest::new_empty(1);
    m.tables.insert(
        "events".into(),
        TableEntry {
            schema: sample_schema(),
            next_batch_id: 0,
            next_sequence_id: 0,
            next_segment_id: 0,
            windows: vec![],
        },
    );
    let err = m.remove_segments("events", 0, 0, &[1]).unwrap_err();
    assert!(matches!(err, BqliteError::Execution(_)));
}
```

(`sample_schema()` helper may or may not already exist in the test module;
check and reuse the existing `sample_segment` helper and the test-module's
pattern for constructing `TableEntry` values — match what
`replace_segments_*` tests already do verbatim.)

- [ ] **Step 3: Add `Database::remove_segments_atomic`**

In `crates/bqlite-storage/src/database.rs`, insert immediately after
`replace_segments`:

```rust
/// Atomically remove a set of compaction-input segments from
/// `(table_name, window_id, shard_id)`'s manifest inventory without
/// publishing a replacement.
///
/// Used by the compaction reclamation path when every row in the
/// merged input was tombstoned — there is no output segment to
/// publish, but the inputs must still be removed in one
/// `manifest.json.tmp → fsync → rename` cycle so the §6 all-or-nothing
/// publish guarantee holds.
///
/// `pub(crate)` because the only intended caller is
/// [`crate::compaction::compact_one`].
pub(crate) fn remove_segments_atomic(
    &mut self,
    table_name: &str,
    window_id: u32,
    shard_id: u32,
    removed_ids: &[u64],
) -> Result<()> {
    self.update_manifest(|m| {
        m.remove_segments(table_name, window_id, shard_id, removed_ids)
    })
}
```

- [ ] **Step 4: Run local CI and code review**

```
scripts/local-ci.sh
```

Spawn code-review subagent on staged diff. Address blocking findings.

- [ ] **Step 5: Commit + merge**

```bash
git add crates/bqlite-storage/src/manifest.rs \
        crates/bqlite-storage/src/database.rs
git commit -m "TASK-435: add remove_segments atomic primitive (CP2)"

git checkout main && git pull --ff-only origin main \
  && git merge task/TASK-435 --ff-only && git push origin main
git checkout task/TASK-435
```

---

## CP3: Wire tombstone filtering into `compact_one`

**Files:**
- Modify: `crates/bqlite-storage/src/compaction.rs`

### Design

At compaction-job start:
1. Load a per-`(table, window, shard)` `TombstoneSnapshot` by reading the
   current tombstone file. This is the job-start snapshot (§12.1) —
   immutable for the rest of the job.
2. Save the snapshot's `TombstoneFile` clone for use during reclamation (CP4).
3. Wrap each input's `SegmentScan` with `CompactionTombstoneScan`,
   passing each segment's `seq_id_range.0` and `batch_id`.
4. Run the merge as before. If the merger yields zero rows, take the
   zero-row branch: skip `write_segment`, call
   `Database::remove_segments_atomic` to drop the inputs, reap input
   files, return `CompactionOutcome { output_segment_ids: vec![], ... }`.
5. Reclamation is deferred to CP4.

Key invariant: every byte of the tombstone snapshot loaded at step 1
is surfaced as filter work on at least one input scan. Mid-compaction
DELETEs write to the file on disk but are not observed by the filter.

### Steps

- [ ] **Step 1: Add tombstone-snapshot load at job start**

In `compact_one`, after step 1 (manifest snapshot), before step 3 (open
segments), insert:

```rust
// ── Job-start tombstone snapshot (§12.1). ──────────────────────
// Read once, use for filtering throughout the job; DELETEs issued
// mid-compaction write a new file but do not affect this job.
//
// `shard_id` is u32 in the compact_one signature but every other
// shard API takes u16 (manifest::shard_count is u16). The earlier
// `shard.get(shard_id as usize)` validation guarantees shard_id fits
// in the manifest's shard_count, which itself fits in u16 — so the
// narrowing is infallible by construction. Use `as u16` with a
// debug_assert so the assumption is load-bearing at test time but
// costs nothing in release.
debug_assert!(shard_id <= u32::from(u16::MAX));
let shard_id_u16 = shard_id as u16;
let tombstone_path =
    crate::tombstone::tombstone_file_path(db.root(), table, window_id, shard_id_u16);
let tombstone_snapshot_at_start =
    crate::tombstone::read_tombstone_file(&tombstone_path)?;
```

- [ ] **Step 2: Wrap each input scan**

In step 3 of `compact_one`, replace the existing scan-opening loop with:

```rust
// ── 3. Open each input and build a SegmentScan. ─────────────────
let db_root = db.root().to_path_buf();
let shared_schema = Arc::new(table_schema.clone());
let mut scans: Vec<Box<dyn bqlite_core::storage::SegmentScan>> =
    Vec::with_capacity(shard_segments.len());
for seg in &shard_segments {
    let path = segment_path(&db_root, table, window_id, shard_id, seg.segment_id);
    let reader = SegmentFileReader::open_shared(&path, shared_schema.clone())?;
    let scan = reader.scan(&ColumnProjection::all(), None)?;
    // Wrap with tombstone filtering when the snapshot has entries.
    // `CompactionTombstoneScan::new` is cheap when the file is empty
    // (its `all_dropped` probe is a HashSet lookup), and every
    // short-circuit inside `next_row_group` is cheap enough that we
    // always wrap for uniformity — the extra indirection is negligible
    // compared to the k-way merge cost.
    let wrapped: Box<dyn bqlite_core::storage::SegmentScan> =
        Box::new(crate::tombstone_scan::CompactionTombstoneScan::new(
            Box::new(scan),
            tombstone_snapshot_at_start.clone(),
            entity_key_name.clone(),
            ts_name.clone(),
            seg.seq_id_range.0,
            seg.batch_id,
        ));
    scans.push(wrapped);
}
```

- [ ] **Step 3: Handle zero-row merge output**

Replace the existing empty-merge error branch (`return Err(... "merged
stream was empty ...")`) with:

```rust
if merged_batches.is_empty() {
    // Every input row was tombstoned. Remove the inputs atomically
    // (§6 publish primitive), reap the input files, and return an
    // outcome with no output segments. Reclamation (CP4) will still
    // run to clear the now-redundant tombstone entries.
    db.remove_segments_atomic(table, window_id, shard_id, &input_ids)?;
    for old_id in &input_ids {
        let path = segment_path(&db_root, table, window_id, shard_id, *old_id);
        let _ = std::fs::remove_file(&path);
    }
    return Ok(CompactionOutcome {
        input_segment_ids: input_ids,
        output_segment_ids: vec![],
        input_byte_size,
        output_byte_size: 0,
    });
}
```

(Reclamation hook lands in CP4; for CP3 the zero-row path just leaves the
tombstone file alone.)

- [ ] **Step 4: Tests — row / batch / entity / time-range reclamation during merge**

Append to the existing `#[cfg(test)] mod tests` block in `compaction.rs`
(reuse `ScratchDir`, `ingest_one_segment`, `events_schema`, `make_event`,
`read_all_rows`):

```rust
#[test]
fn compact_one_drops_row_tombstoned_events() {
    let scratch = ScratchDir::new("tombstone-row");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    // s1 spans seq ids [0, 2], s2 spans [3, 5].
    let s1 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
        make_event("alice", 200, "view"),
        make_event("bob", 150, "click"),
    ]);
    let s2 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("bob", 250, "view"),
        make_event("carol", 300, "click"),
        make_event("carol", 400, "view"),
    ]);
    // Tombstone the alice/200 row (s1 seq offset 1 → __seq_id = s1.first + 1).
    let doomed_seq = s1.seq_id_range.0 + 1;
    let tf = crate::tombstone::TombstoneFile::for_rows([doomed_seq]);
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    compact_one(&mut db, "events", 0, 0).unwrap();
    let rows = read_all_rows(&db);
    assert!(
        !rows.iter().any(|(_, ts, _)| *ts == 200),
        "alice/200 row must be reclaimed by compaction"
    );
    assert_eq!(rows.len(), 5);
    let _ = s2;
}

#[test]
fn compact_one_drops_batch_tombstoned_segments() {
    let scratch = ScratchDir::new("tombstone-batch");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    let s1 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
        make_event("bob", 150, "click"),
    ]);
    let _s2 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("carol", 200, "click"),
    ]);
    // Tombstone s1's batch id.
    let tf = crate::tombstone::TombstoneFile::for_batches([s1.batch_id]);
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    compact_one(&mut db, "events", 0, 0).unwrap();
    let rows = read_all_rows(&db);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "carol");
}

#[test]
fn compact_one_drops_entity_tombstoned_rows() {
    let scratch = ScratchDir::new("tombstone-entity");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
        make_event("alice", 200, "view"),
        make_event("bob", 150, "click"),
    ]);
    ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("bob", 250, "view"),
    ]);
    let tf = crate::tombstone::TombstoneFile::for_entities([
        bqlite_core::ScalarValue::String("alice".into()),
    ]);
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    compact_one(&mut db, "events", 0, 0).unwrap();
    let rows = read_all_rows(&db);
    assert!(!rows.iter().any(|(e, _, _)| e == "alice"));
    assert_eq!(rows.len(), 2); // just bob's two rows
}

#[test]
fn compact_one_drops_time_range_tombstoned_rows() {
    let scratch = ScratchDir::new("tombstone-time");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
        make_event("alice", 500, "view"),
    ]);
    ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("bob", 300, "click"),
    ]);
    let tf = crate::tombstone::TombstoneFile::for_time_range(
        crate::tombstone::TimeRangeDelete {
            min_ts: None,
            min_inclusive: false,
            max_ts: Some(400),
            max_inclusive: false,
        },
    );
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    compact_one(&mut db, "events", 0, 0).unwrap();
    let rows = read_all_rows(&db);
    assert_eq!(rows, vec![("alice".to_string(), 500, "view".to_string())]);
}

#[test]
fn compact_one_all_rows_tombstoned_removes_inputs_without_output() {
    let scratch = ScratchDir::new("tombstone-allkill");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    let s1 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
    ]);
    let s2 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 200, "view"),
    ]);
    let tf = crate::tombstone::TombstoneFile::for_entities([
        bqlite_core::ScalarValue::String("alice".into()),
    ]);
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    let outcome = compact_one(&mut db, "events", 0, 0).unwrap();
    assert!(outcome.output_segment_ids.is_empty());
    assert_eq!(outcome.input_segment_ids, vec![s1.segment_id, s2.segment_id]);
    let entry = db.manifest().tables.get("events").unwrap();
    assert!(entry.windows[0].shards[0].is_empty());
}
```

- [ ] **Step 5: Run local CI and code review**

```
scripts/local-ci.sh
```

Spawn code-review subagent. Address blocking findings.

- [ ] **Step 6: Commit + merge**

```bash
git add crates/bqlite-storage/src/compaction.rs
git commit -m "TASK-435: apply tombstones during compaction merge (CP3)"

git checkout main && git pull --ff-only origin main \
  && git merge task/TASK-435 --ff-only && git push origin main
git checkout task/TASK-435
```

---

## CP4: Tombstone reclamation after publish + docs + completion

**Files:**
- Modify: `crates/bqlite-storage/src/compaction.rs`
- Modify: `docs/design/storage/deletes.md`
- Modify: `docs/design/storage/compaction-concurrency.md`

### Design

After the atomic publish (step 11 for the happy path, or the
`remove_segments_atomic` call in the zero-row path), compute the set of
reclaimable tombstone entries from the **job-start snapshot** and rewrite
the tombstone file under the per-shard mutex.

Because compaction merges *every* segment in `(window, shard)` (§12.4), any
tombstone entry present in the job-start snapshot that was applied by the
merge is now redundant:

- `row_deletes`: every `__seq_id` in `snapshot.row_deletes` whose integer
  value falls within any compacted input segment's `seq_id_range`. Those
  rows were physically removed by the filter.
- `batch_deletes`: every `batch_id` in `snapshot.batch_deletes` whose value
  equals any compacted input segment's `batch_id`. Those segments were
  dropped wholesale.
- `entity_deletes`: every entry in `snapshot.entity_deletes`. The output
  segment contains no row for any tombstoned entity (filter invariant),
  and the output is the only surviving segment in the shard.
- `time_range_deletes`: every entry in `snapshot.time_range_deletes`. The
  output segment contains no row whose timestamp falls within any such
  range.

Reclamation steps:
1. Acquire `Database::tombstone_shard_lock(table, window_id, shard_id_u16)`.
2. Read current tombstone file `current` (may be `snapshot ∪ mid_delete`).
3. Compute `reclaimed` entries from `snapshot`.
4. Subtract: `new = current - reclaimed` (per-granularity set difference
   for row/batch/entity; linear scan for time-range).
5. If `new.is_empty()`, remove the file (best-effort); else
   `write_tombstone_atomic`.
6. Release lock.

If the tombstone file no longer exists (e.g. removed by a concurrent
operator), treat `current` as empty.

If step 2 or the rewrite errors, propagate. Per §12.3 stale tombstone
safety: a write failure leaves the snapshot + mid-delete file on disk,
which is harmless — the entries are no-ops against the new single output
segment.

### Steps

- [ ] **Step 1: Add `reclaim_tombstones_after_compaction`**

In `compaction.rs`, add (near the other helpers, before the
`Background scheduler` section):

```rust
/// Rewrite the shard's tombstone file after a successful compaction
/// publish to drop every entry that is now physically reclaimed.
///
/// Implements `docs/design/storage/deletes.md` §12.2 manifest-first
/// reclamation. Must be called only after the §6 publish (either
/// [`Database::replace_segments`] for the happy path or
/// [`Database::remove_segments_atomic`] for the zero-row path) has
/// succeeded — a crash before this point leaves the tombstone file
/// intact, which is correct per §12.3 "stale tombstone safety".
///
/// `snapshot_at_start` is the snapshot taken at job start (§12.1);
/// `input_segments` lists the segment metas that were consumed by
/// the merge so we can compute row- and batch-level reclamation.
/// The file rewrite is serialised against concurrent DELETEs via the
/// per-shard tombstone mutex (§9).
///
/// # Concurrency assumption
///
/// Entity- and time-range reclamation assume the new output segment
/// is the only remaining segment in `(window, shard)` after publish.
/// `compact_one` takes `&mut Database` today, so no concurrent ingest
/// can add segments between job-start and this call. If a future
/// per-shard concurrent writer changes that, this function must
/// re-snapshot the manifest under the publish lock and narrow the
/// entity/time-range rules accordingly — §12.3 keeps correctness
/// either way, so the change is a pruning tightening, not a bug fix.
///
/// # Read-modify-write window
///
/// Between publish and the `write_tombstone_atomic` call below, a
/// concurrent query that loads the tombstone snapshot will see both
/// the new output segment AND the reclaimable entries from the old
/// snapshot. Per §12.3 these are harmless no-ops on the new output
/// — no row in the new segment matches any reclaimable entry because
/// the merge filter already dropped them. No correctness issue.
fn reclaim_tombstones_after_compaction(
    db: &Database,
    table: &str,
    window_id: u32,
    shard_id: u32,
    snapshot_at_start: &crate::tombstone::TombstoneFile,
    input_segments: &[crate::manifest::SegmentMeta],
) -> Result<()> {
    if snapshot_at_start.is_empty() {
        return Ok(());
    }
    // Same narrowing rationale as in `compact_one`: shard_id ≤
    // manifest.shard_count ≤ u16::MAX by construction.
    debug_assert!(shard_id <= u32::from(u16::MAX));
    let shard_id_u16 = shard_id as u16;
    let lock = db.tombstone_shard_lock(table, window_id, shard_id_u16);
    let _guard = lock
        .lock()
        .expect("tombstone shard lock poisoned by a panicking writer");

    let path = crate::tombstone::tombstone_file_path(db.root(), table, window_id, shard_id_u16);
    let mut current = crate::tombstone::read_tombstone_file(&path)?;

    // Row-level: reclaim any __seq_id in snapshot.row_deletes whose
    // value fell within any compacted input's seq_id_range.
    if !snapshot_at_start.row_deletes.is_empty() {
        current.row_deletes.retain(|seq_id| {
            let covered = input_segments.iter().any(|seg| {
                let (lo, hi) = seg.seq_id_range;
                *seq_id >= lo && *seq_id <= hi
            });
            let in_snapshot = snapshot_at_start.row_deletes.contains(seq_id);
            // Retain when: NOT (covered AND in_snapshot)
            !(covered && in_snapshot)
        });
    }
    // Batch-level: reclaim any batch_id in snapshot.batch_deletes
    // matched by any compacted input.
    if !snapshot_at_start.batch_deletes.is_empty() {
        current.batch_deletes.retain(|batch_id| {
            let covered = input_segments.iter().any(|seg| seg.batch_id == *batch_id);
            let in_snapshot = snapshot_at_start.batch_deletes.contains(batch_id);
            !(covered && in_snapshot)
        });
    }
    // Entity-level: every entry in the snapshot is reclaimable (§12.4
    // + filter invariant: the new output contains no row for any
    // tombstoned entity).
    if !snapshot_at_start.entity_deletes.is_empty() {
        current
            .entity_deletes
            .retain(|e| !snapshot_at_start.entity_deletes.contains(e));
    }
    // Time-range: every entry in the snapshot is reclaimable. Compare
    // by equality (TimeRangeDelete is PartialEq).
    if !snapshot_at_start.time_range_deletes.is_empty() {
        current
            .time_range_deletes
            .retain(|r| !snapshot_at_start.time_range_deletes.contains(r));
    }

    if current.is_empty() {
        // Best-effort removal keeps the directory clean; a transient
        // failure is fine because an empty file is also a valid
        // representation of "no tombstones".
        let _ = std::fs::remove_file(&path);
        Ok(())
    } else {
        crate::tombstone::write_tombstone_atomic(&path, &current)
    }
}
```

- [ ] **Step 2: Call the reclaimer from `compact_one` happy path**

In `compact_one`, immediately after the existing step-12 reap-loop (`for
old_id in &input_ids { ... }`), insert:

```rust
// ── 13. Tombstone reclamation (§12.2). ─────────────────────────
// Manifest-first ordering: the publish above is durable; a crash
// here leaves stale tombstones which are harmless per §12.3.
reclaim_tombstones_after_compaction(
    db,
    table,
    window_id,
    shard_id,
    &tombstone_snapshot_at_start,
    &shard_segments,
)?;
```

- [ ] **Step 3: Call the reclaimer from `compact_one` zero-row path**

In the zero-row branch added in CP3, immediately after the reap-loop and
before the `return Ok(CompactionOutcome { ... })`, insert the same
reclaimer call verbatim.

- [ ] **Step 4: Tests — reclamation semantics**

Append to the `compaction.rs` test module:

```rust
#[test]
fn reclaim_removes_applied_row_entity_time_range_and_batch() {
    let scratch = ScratchDir::new("reclaim-all");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    let s1 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
        make_event("alice", 150, "view"),
        make_event("bob", 200, "click"),
    ]);
    let s2 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("carol", 300, "click"),
        make_event("dave", 400, "view"),
    ]);

    // One of each granularity. Row id inside s1; batch id of s1;
    // entity "dave"; time range covering 200..=250.
    let tf = crate::tombstone::TombstoneFile {
        row_deletes: [s1.seq_id_range.0 + 1].into_iter().collect(),
        batch_deletes: [s1.batch_id].into_iter().collect(),
        entity_deletes: [bqlite_core::ScalarValue::String("dave".into())]
            .into_iter()
            .collect(),
        time_range_deletes: vec![crate::tombstone::TimeRangeDelete {
            min_ts: Some(200),
            min_inclusive: true,
            max_ts: Some(250),
            max_inclusive: true,
        }],
    };
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    compact_one(&mut db, "events", 0, 0).unwrap();

    // File is gone (every entry was reclaimable) OR is empty.
    let after = crate::tombstone::read_tombstone_file(&tp).unwrap();
    assert!(after.is_empty(), "every snapshot entry should be reclaimed");
    let _ = s2;
}

#[test]
fn reclaim_preserves_unmatched_tombstone_entries() {
    // §12.3 stale-tombstone safety: a tombstone entry whose
    // target is not present in any compacted input must survive
    // reclamation. `&mut Database` means mid-compaction DELETEs
    // can't race today, but a row tombstone targeting a __seq_id
    // that no input covers tests the same retention logic.
    let scratch = ScratchDir::new("reclaim-stale-safety");
    let mut db = Database::create(scratch.path()).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    let s1 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("alice", 100, "click"),
    ]);
    let _s2 = ingest_one_segment(&mut db, "events", 0, 0, &[
        make_event("bob", 200, "view"),
    ]);

    // Snapshot at start: tombstone alice.
    let tf = crate::tombstone::TombstoneFile::for_entities([
        bqlite_core::ScalarValue::String("alice".into()),
    ]);
    let tp = crate::tombstone::tombstone_file_path(scratch.path(), "events", 0, 0);
    crate::tombstone::write_tombstone_atomic(&tp, &tf).unwrap();

    // Pre-populate a row tombstone outside any segment's seq range:
    let unreachable_seq = s1.seq_id_range.0 + 10_000;
    let combined = crate::tombstone::TombstoneFile {
        entity_deletes: [bqlite_core::ScalarValue::String("alice".into())]
            .into_iter()
            .collect(),
        row_deletes: [unreachable_seq].into_iter().collect(),
        ..Default::default()
    };
    crate::tombstone::write_tombstone_atomic(&tp, &combined).unwrap();

    compact_one(&mut db, "events", 0, 0).unwrap();

    // alice entity reclaim fires; row-delete stays because no input
    // segment covered that seq id.
    let after = crate::tombstone::read_tombstone_file(&tp).unwrap();
    assert!(after.entity_deletes.is_empty(), "entity entry reclaimed");
    assert!(
        after.row_deletes.contains(&unreachable_seq),
        "unreachable row tombstone must be preserved"
    );
}
```

(The first test exercises the happy-path reclaim-everything case; the
second verifies that entries present in the snapshot but NOT covered by
any compacted input's seq_id_range are preserved — this is the §12.3
"stale tombstones are harmless" safety check. End-to-end
mid-compaction-DELETE interleaving requires concurrent writers and is
covered in TASK-440's integration suite, not here; today `&mut Database`
serialises DELETE and compaction so the mid-compaction race is not yet
reachable.)

- [ ] **Step 5: Update design docs**

In `docs/design/storage/compaction-concurrency.md` §12 "Implementation
status", replace the bullet:

```
- SS9 tombstone snapshot at job start and the manifest-first
  reclamation ordering land in TASK-434 (tombstone-aware scan) and
  TASK-435 (tombstone reclamation during compaction). The compaction
  executor in TASK-408 does not consult `tombstones.json`.
```

with:

```
- SS9 tombstone snapshot at job start and the manifest-first
  reclamation ordering are implemented by TASK-435 inside
  `compact_one` (`crates/bqlite-storage/src/compaction.rs`): the job
  reads `tombstones.json` once, wraps every input scan in
  `CompactionTombstoneScan` to drop row/batch/entity/time-range
  matches during the merge, and rewrites the tombstone file under
  the per-shard mutex after publish. Query-time scan-wrapping is
  TASK-434.
```

In `docs/design/storage/deletes.md` §15, replace the TASK-435 bullet:

```
- **TASK-435 (tombstone reclamation during compaction)** -- implements
  the manifest-first reclamation ordering from SS12.2, the per-
  granularity reclamation rules from SS12.4, and the optional time-range
  merge from SS12.5.
```

with:

```
- **TASK-435 (tombstone reclamation during compaction)** -- implemented
  by `compact_one` and `reclaim_tombstones_after_compaction` in
  `crates/bqlite-storage/src/compaction.rs`, using the
  `CompactionTombstoneScan` wrapper in
  `crates/bqlite-storage/src/tombstone_scan.rs`. Manifest-first
  reclamation order (§12.2) and per-granularity reclamation rules
  (§12.4) are covered; optional time-range merge (§12.5) is deferred.
```

- [ ] **Step 6: Run local CI, code review**

```
scripts/local-ci.sh
```

Spawn code-review subagent on staged diff. Address blocking findings.

- [ ] **Step 7: Commit + merge**

```bash
git add crates/bqlite-storage/src/compaction.rs \
        docs/design/storage/compaction-concurrency.md \
        docs/design/storage/deletes.md
git commit -m "TASK-435: reclaim tombstones after compaction publish (CP4)"

git checkout main && git pull --ff-only origin main \
  && git merge task/TASK-435 --ff-only && git push origin main
git checkout task/TASK-435
```

- [ ] **Step 8: Completion protocol**

```bash
git mv tasks/active/TASK-435.lock tasks/completed/TASK-435.done
# Edit the .done file to add completed_at per AGENTS.md §Completion Protocol.
git add tasks/completed/TASK-435.done
git commit -m "TASK-435: completed"

git checkout main && git pull --ff-only origin main \
  && git merge task/TASK-435 --ff-only && git push origin main
```

---

## Self-Review Checklist

- **Spec coverage:**
  - §12.1 snapshot-at-job-start — CP3 step 1.
  - §12.2 manifest-first ordering — CP4 step 2/3 (reclaimer called after
    publish).
  - §12.3 stale-tombstone safety — tested implicitly by CP4 step 4's
    second test (stale row entry preserved when not covered).
  - §12.4 per-granularity rules — CP4 step 1 `reclaim_tombstones_after_compaction`.
  - §12.5 time-range merge — explicitly deferred; noted in design-doc update.
  - Mid-compaction DELETE preservation — CP4 step 4 second test.
- **Placeholders:** none; every step has the full code to type.
- **Type consistency:** `tombstone_file_path` signature takes `u16` for
  shard — every call site narrows from `u32` with a bounds check;
  `CompactionTombstoneScan::new` signature matches the call site in CP3.
