# TASK-536: Real Per-Shard Morsel Dispatch (TASK-523 Closure)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `Engine::query`'s degenerate single-task dispatch with real per-shard morsel parallelism: one morsel per non-empty shard, one Rayon task per morsel, per-shard `AccumulatorHandle`s merged by the coordinator, and `WorkerMetricsSnapshot` recorded once per worker.

**Architecture:** The engine inspects the bound plan and the manifest to identify the populated shards for the query. For aggregate-rooted plans (`PhysicalPlan::Aggregate(...)` at the root) it splits the bind+drive into **per-shard subplans** rooted at the aggregate's input — each dispatched as one morsel onto the morsel queue. Each worker drains its morsel via `WorkerMorselGuard`, drives the per-shard subplan to completion, feeds the resulting batches into a per-shard `HashAccumulator` parked on `AccumulatorHandle`, and records a per-worker `WorkerMetricsSnapshot`. The coordinator pairwise-merges per-shard accumulators on `ShardDoneSignal`, then materialises the final batch via `Accumulator::finish()`. For non-aggregate plans the same fan-out runs against the **whole bound tree per shard** with a `ShardScopedSegmentReader` that filters the manifest's segment list to one shard, and per-shard outputs are concatenated. DDL/DELETE/EXPLAIN remain on the §5.4 bypass. Plan shapes that cannot be safely split per shard (top-level Sort, **top-level Limit (would multiply the cap by num_shards)**, MergeSources/joins, SubqueryFilter, SequenceMatch, Distinct, EventSelect, Sessionize, Attribute, Sample) fall back to the existing whole-database path so the work-on-mainline never regresses correctness.

**Plan-review revisions (post-review, 2026-05-09).** Six blocking issues from the plan review are folded back here as inline overrides to the per-task instructions below. Implementers reading task text should treat these as authoritative even when a step's pseudocode predates the revision:

- **R1 (B1 — Limit):** `is_per_shard_safe_input` MUST NOT accept `Limit`. A query rooted at `Limit` falls back to `SingleTask` for v1. (A future task can lift Limit to the coordinator.)
- **R2 (B2 — accumulator setup duplication):** Add `HashAccumulator::for_aggregate(plan: &AggregatePhysical) -> HashAccumulator` in `bqlite-operators` and have `HashAggregateOperator::new` *and* the engine's per-shard dispatch both call it. No accumulator-setup code in `query.rs`.
- **R3 (B3 — merge correctness):** v1 aggregates (Count/Sum/Min/Max/Avg/CountDistinct/Percentile via DDSketch) are documented merge-correct in `aggregate/mod.rs:310`; the plan acknowledges this. Add a regression test that runs the same STATS query against a 1-thread baseline (`EngineConfig::query_threads = Some(1)`, which goes through the same dispatch path with `num_workers = 1`) and asserts row-for-row equality after sorting by group key.
- **R4 (B4 — bind plumbing):** Don't add an `Option<u32> shard_filter` parameter to free-form bind functions. Instead, introduce one struct `BindCtx<'a> { db: &'a mut Database, ctx: &'a QueryContext, shard_filter: Option<u32>, cohorts: ..., pending: ... }` that becomes the **single** recursion argument. Public entrypoints `bind_physical(...)` and `bind_physical_for_shard(...)` build a `BindCtx` and call into the recursion. This makes adding a new recursive call site fail at compile time if it doesn't thread `BindCtx`.
- **R5 (B5 — panic isolation):** Wrap each `work_ref(...)` call inside `run_per_shard` in `std::panic::catch_unwind(AssertUnwindSafe(...))`. Convert payloads to `BqliteError::OperatorPanic { message, location: None }` via the existing `panic_message` helper in `query.rs`. This bounds blast radius to one morsel per design §9.2; cleanup of the morsel guard runs in `Drop` and decrements outstanding-morsels even on panic.
- **R6 (B6 — cancellation):** Workers check `ctx.query.cancellation().is_cancelled()` at the top of every iteration in `run_per_shard`'s pull loop and on every batch boundary inside the per-shard `drive_to_completion`. The check is one atomic load — cheap. Adds the design-§9.1 between-morsels and between-batches yield points.
- **R7 (S1 — worker dedupe):** Use `rayon::current_thread_index()` (returns `Option<usize>` inside a Rayon pool worker) for `num_workers` accounting. Drop the `DefaultHasher`-on-`ThreadId` workaround.

**Tech Stack:** Rust 2021, Apache Arrow, crossbeam queues, Rayon thread pool, the existing `MorselScheduler`/`MorselQueue`/`AccumulatorHandle`/`WorkerMorselGuard` scaffold (TASK-523 CP2).

---

## File Structure

| File | Responsibility |
|------|----------------|
| `crates/bqlite-storage/src/database.rs` | Add `ShardScopedSegmentReader` and `Database::segment_reader_for_shard` (manifest-backed reader restricted to one shard). |
| `crates/bqlite-engine/src/scheduler/morsel.rs` | Convert `MorselGenerator::degenerate` into `for_shard` (already emits one morsel per shard) and add an engine-level helper `enumerate_shard_snapshots(db, table)` that returns one `ShardSnapshot` per non-empty shard. The morsel itself stays whole-shard (`EntityRange::All`); sub-shard halving is out of scope. |
| `crates/bqlite-engine/src/scheduler/engine_pool.rs` | Add `MorselScheduler::run_per_shard(...)` — the multi-morsel dispatch entry point that builds the queue, fans out one Rayon task per morsel, hooks each task to a `WorkerMorselGuard`, and joins on the coordinator condvar. Keeps `submit` / `run_degenerate` for callers that don't want fan-out. |
| `crates/bqlite-engine/src/query.rs` | Inspect the planner output; if the root is `PhysicalPlan::Aggregate` and the input is per-shard-safe, dispatch via `run_per_shard` with per-shard subplans + per-shard `HashAccumulator`s; coordinator merges and emits the final batch. For plain `Scan`/`Filter`/`Project`/`Limit` plans dispatch via `run_per_shard` with whole-tree-per-shard binding; concat outputs. For everything else (Sort, MergeSources, etc.), fall back to the existing single-task path. |
| `crates/bqlite-engine/src/perf.rs` | (No struct changes — the existing `record_worker_snapshot` path is reused.) |
| `tests/tests/wave5_acceptance.rs` | Strengthen `multi_shard_stats_under_floor_budget_matches_hand_computed` to assert `metrics.morsels_per_shard_min > 0` and `metrics.num_workers > 1`; add `multi_shard_scan_returns_full_row_count` strengthening for parallel scan. |
| `docs/design/engine/morsel-scheduler.md` | Add a §11.1 reconciliation note that TASK-536 lands per-shard dispatch with the fall-back-to-single-task pattern for sort/joins as a deliberate v1 simplification. |

---

## Task 1: ShardScopedSegmentReader in bqlite-storage

**Files:**
- Modify: `crates/bqlite-storage/src/database.rs` — add `Database::segment_reader_for_shard(table, shard_id, time_range)` that returns a `Box<dyn SegmentReader>` whose `segments()` iterator filters to one shard.
- Test: `crates/bqlite-storage/src/database.rs` (existing `tests` module) — three new unit tests.

- [ ] **Step 1: Write the failing test for `segment_reader_for_shard`**

Add to the `tests` module in `crates/bqlite-storage/src/database.rs`:

```rust
#[test]
fn segment_reader_for_shard_filters_to_one_shard() {
    use bqlite_core::SegmentReader as _;

    let scratch = test_dir("seg-reader-shard");
    let mut db = create_minimal_events_db(scratch.path());
    ingest_two_shard_fixture(&mut db);

    // Build the un-filtered reader so we can read off the populated
    // shard set from the manifest.
    let all = db.segment_reader("events").expect("reader");
    let mut shards: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for h in all.segments() {
        shards.insert(h.expect("handle").shard_id);
    }
    assert!(shards.len() >= 2, "fixture must populate >=2 shards");
    let target_shard = *shards.iter().next().unwrap();

    let scoped = db
        .segment_reader_for_shard("events", target_shard, bqlite_core::TimeRange::unbounded())
        .expect("scoped reader builds");
    let observed: Vec<u32> = scoped
        .segments()
        .map(|h| h.unwrap().shard_id)
        .collect();
    assert!(!observed.is_empty(), "scoped reader must yield segments");
    assert!(
        observed.iter().all(|&s| s == target_shard),
        "scoped reader yielded foreign shards: {observed:?}"
    );
}
```

(Helpers `test_dir`, `create_minimal_events_db`, `ingest_two_shard_fixture` mirror the existing tests in the same file. If they don't exist for "two-shard", reuse the smallest fixture builder already in the module — see existing test cases. Inline a minimal builder if needed using `Database::create` + `bqlite-tests::common`.)

- [ ] **Step 2: Run the test and confirm it fails on the missing function**

```bash
cd /workspace
cargo test -p bqlite-storage segment_reader_for_shard_filters_to_one_shard 2>&1 | tail -20
```

Expected: compile error — `segment_reader_for_shard` not found on `Database`.

- [ ] **Step 3: Implement `segment_reader_for_shard` and the wrapper reader**

In `crates/bqlite-storage/src/database.rs`, add a method on `Database` (next to `segment_reader_for_time_range`):

```rust
/// Open a manifest-backed segment reader restricted to a single
/// shard. The reader's iteration order is unchanged (window-major,
/// insertion-order within the shard), but every segment whose
/// `shard_id != shard` is filtered out at enumeration time.
///
/// Used by the engine's per-shard morsel dispatch (TASK-536):
/// each shard's worker binds a fresh operator tree against this
/// reader and drives it independently of the other shards'.
///
/// Returns `BqliteError::Plan` if `table_name` is unknown.
pub fn segment_reader_for_shard(
    &self,
    table_name: &str,
    shard: u32,
    time_range: TimeRange,
) -> Result<Box<dyn SegmentReader>> {
    let entry = self
        .manifest
        .tables
        .get(table_name)
        .ok_or_else(|| bqlite_core::catalog::unknown_table_error(table_name))?;

    let windows = if time_range == TimeRange::unbounded() {
        entry.windows.clone()
    } else {
        filter_windows_by_time_range(&entry.windows, time_range)
    };

    Ok(Box::new(ManifestSegmentReader {
        root: self.root.clone(),
        table_name: table_name.to_string(),
        schema: Arc::new(entry.schema.clone()),
        windows,
        shard_filter: Some(shard),
    }))
}
```

In the same file, extend `ManifestSegmentReader`:

```rust
struct ManifestSegmentReader {
    root: PathBuf,
    table_name: String,
    schema: Arc<TableSchema>,
    windows: Vec<WindowManifest>,
    /// When `Some(shard)`, `segments()` yields only handles whose
    /// `shard_id == shard`. Used by the morsel scheduler's per-shard
    /// dispatch (TASK-536). `None` preserves the historical
    /// "every live segment" enumeration.
    shard_filter: Option<u32>,
}
```

Update both constructor sites (`segment_reader_for_time_range` and any existing tests that build the struct directly) to set `shard_filter: None`. Update `segments()`:

```rust
fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
    let target = self.shard_filter;
    let iter = self.windows.iter().flat_map(move |win| {
        let window_id = win.window_id;
        win.shards
            .iter()
            .enumerate()
            .filter(move |(shard_idx, _)| {
                target.map_or(true, |s| *shard_idx as u32 == s)
            })
            .flat_map(move |(shard_idx, segs)| {
                let shard_id = shard_idx as u32;
                segs.iter().map(move |seg| {
                    Ok(SegmentHandle {
                        segment_id: seg.segment_id,
                        shard_id,
                        window_id: u64::from(window_id),
                        row_count: seg.row_count,
                        schema_version: seg.schema_version,
                        seq_id_first: seg.seq_id_range.0,
                        batch_id: seg.batch_id,
                    })
                })
            })
    });
    Box::new(iter)
}
```

`open_segment` is already shard-id-aware via the handle, no changes needed.

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cargo test -p bqlite-storage segment_reader_for_shard_filters_to_one_shard 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 5: Add a complementary "no shard filter ⇒ identical to base reader" test**

Append to the same module:

```rust
#[test]
fn segment_reader_for_shard_unbounded_matches_base_for_single_shard() {
    use bqlite_core::SegmentReader as _;

    let scratch = test_dir("seg-reader-shard-base");
    let mut db = create_minimal_events_db(scratch.path());
    ingest_two_shard_fixture(&mut db);

    let all = db.segment_reader("events").expect("reader");
    let by_shard: std::collections::BTreeMap<u32, Vec<u64>> = {
        let mut m: std::collections::BTreeMap<u32, Vec<u64>> = std::collections::BTreeMap::new();
        for h in all.segments() {
            let h = h.unwrap();
            m.entry(h.shard_id).or_default().push(h.segment_id);
        }
        m
    };
    for (shard, expected_segments) in &by_shard {
        let scoped = db
            .segment_reader_for_shard("events", *shard, bqlite_core::TimeRange::unbounded())
            .unwrap();
        let mut observed: Vec<u64> = scoped
            .segments()
            .map(|h| h.unwrap().segment_id)
            .collect();
        observed.sort();
        let mut expected = expected_segments.clone();
        expected.sort();
        assert_eq!(observed, expected, "shard {shard} segments must match");
    }
}

#[test]
fn segment_reader_for_shard_unknown_table_errors() {
    let scratch = test_dir("seg-reader-shard-unknown");
    let db = Database::create(scratch.path()).unwrap();
    let err = db
        .segment_reader_for_shard("nope", 0, bqlite_core::TimeRange::unbounded())
        .unwrap_err();
    assert!(matches!(err, bqlite_core::BqliteError::Plan(_)), "{err:?}");
}
```

- [ ] **Step 6: Run all the new tests, full storage crate test, plus clippy/format**

```bash
cargo test -p bqlite-storage 2>&1 | tail -20
cargo clippy -p bqlite-storage --all-targets --all-features -- -D warnings 2>&1 | tail -10
cargo fmt --check 2>&1 | tail -5
```

Expected: every test passes; clippy clean; fmt clean.

- [ ] **Step 7: Code-review the staged diff via subagent, then commit**

Spawn a code-review subagent (subagent_type=`superpowers:code-reviewer`) with the diff, the relevant design doc paths (`docs/design/engine/morsel-scheduler.md` §3.2 / §3.6, `docs/design/storage/storage-format.md` §5.2 if applicable), and ask: "Is the shard filter sound under tombstones, time-range pruning, and the existing enumeration order? Any missed call sites?"

After the review returns no blocking issues:

```bash
git add crates/bqlite-storage/src/database.rs
git commit -m "TASK-536: Add ShardScopedSegmentReader for per-shard morsel dispatch

Per docs/design/engine/morsel-scheduler.md §3.2: the morsel
generator is metadata-only and emits one morsel per shard. To
let the engine dispatch one Rayon task per shard, expose a
manifest-backed segment reader filtered to one shard via
Database::segment_reader_for_shard. The new shard_filter field
on ManifestSegmentReader is None for legacy callers (no behaviour
change) and Some(shard) for the per-shard dispatch path."
```

Then merge to main per AGENTS.md checkpoint protocol.

```bash
git checkout main && git pull origin main
git merge task/TASK-536 --ff-only
git push origin main
git checkout task/TASK-536
```

---

## Task 2: Engine-side shard enumeration helper

**Files:**
- Create: `crates/bqlite-engine/src/scheduler/shard_plan.rs`
- Modify: `crates/bqlite-engine/src/scheduler/mod.rs` — re-export.
- Test: inline `#[cfg(test)] mod tests` in the new file plus integration tests in `crates/bqlite-engine/src/query.rs` later (Task 5).

- [ ] **Step 1: Write the failing test for `enumerate_shard_snapshots`**

Create `crates/bqlite-engine/src/scheduler/shard_plan.rs` with the following skeleton (no real implementation yet — it intentionally fails to compile to drive TDD):

```rust
//! Engine-side helper that enumerates the populated shards of a
//! table from the manifest snapshot and constructs one
//! [`ShardSnapshot`] per shard.
//!
//! Used by the per-shard morsel dispatch (TASK-536). The helper is
//! manifest-only — it does not open segment files or decode zone maps.

use bqlite_core::{Result, TimeRange};
use bqlite_storage::Database;

use super::morsel::{ShardSnapshot, WindowSegments};

#[cfg(test)]
mod tests;

/// Enumerate one [`ShardSnapshot`] per shard that has at least one
/// live segment of `table` overlapping `time_range`. Empty shards are
/// elided — the design's empty-shard accounting (§3.6) is handled by
/// the morsel queue's drain logic, not by carrying zero-segment
/// snapshots through dispatch.
pub fn enumerate_shard_snapshots(
    _db: &Database,
    _table: &str,
    _time_range: TimeRange,
) -> Result<Vec<ShardSnapshot>> {
    todo!("implement in Task 2 step 3")
}
```

Create `crates/bqlite-engine/src/scheduler/shard_plan/tests.rs` (use a `mod tests;` link or inline at the bottom of `shard_plan.rs`; pick one and stick with it — inline is simpler):

Inline at the bottom of `shard_plan.rs`:

```rust
#[cfg(test)]
mod test_impl {
    use super::*;
    use bqlite_engine::Engine; // self-reference via crate path
    use bqlite_tests::common::TempDb;

    #[test]
    fn enumerate_returns_one_snapshot_per_populated_shard() {
        let tmp = TempDb::new();
        let mut db = bqlite_storage::Database::create(tmp.path()).unwrap();
        let engine = Engine::new();
        engine
            .query(
                "CREATE TABLE events (entity_id STRING NOT NULL ENTITY KEY, \
                                       ts TIMESTAMP NOT NULL EVENT TIME, \
                                       kind STRING NOT NULL EVENT TYPE)",
                &mut db,
            )
            .unwrap();
        // Tiny ingest: two entities that hash into different shards.
        // (Reuse a multi-entity ingest helper from bqlite-tests if present.)
        // …
        let snaps = enumerate_shard_snapshots(&db, "events", TimeRange::unbounded()).unwrap();
        assert!(!snaps.is_empty(), "must enumerate at least one shard");
        // Every snapshot must be non-empty.
        for s in &snaps {
            assert!(
                s.windows.iter().any(|w| !w.segments.is_empty()),
                "shard {} has no segments — empty shards must be elided",
                s.shard_id
            );
        }
        // Shard ids unique.
        let mut ids: Vec<u32> = snaps.iter().map(|s| s.shard_id).collect();
        ids.sort();
        let dedup_len = {
            let mut v = ids.clone();
            v.dedup();
            v.len()
        };
        assert_eq!(ids.len(), dedup_len, "shard_id duplicates: {ids:?}");
    }

    #[test]
    fn enumerate_empty_table_yields_empty_vec() {
        let tmp = TempDb::new();
        let mut db = bqlite_storage::Database::create(tmp.path()).unwrap();
        let engine = Engine::new();
        engine
            .query(
                "CREATE TABLE events (entity_id STRING NOT NULL ENTITY KEY, \
                                       ts TIMESTAMP NOT NULL EVENT TIME, \
                                       kind STRING NOT NULL EVENT TYPE)",
                &mut db,
            )
            .unwrap();
        let snaps = enumerate_shard_snapshots(&db, "events", TimeRange::unbounded()).unwrap();
        assert!(snaps.is_empty(), "no segments ⇒ no snapshots");
    }
}
```

(If `bqlite-tests::common` doesn't export a multi-entity ingest helper, inline the smallest one — write a few rows directly via the engine's `INSERT FROM` path, ingesting a small parquet fixture, OR mimic the pattern from `tests/tests/wave5_acceptance.rs`.)

In `crates/bqlite-engine/src/scheduler/mod.rs`:

```rust
pub mod shard_plan;
pub use shard_plan::enumerate_shard_snapshots;
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cargo test -p bqlite-engine enumerate_returns_one_snapshot_per_populated_shard 2>&1 | tail -20
```

Expected: PANIC (`todo!`) or compile error.

- [ ] **Step 3: Implement `enumerate_shard_snapshots`**

Replace the body in `shard_plan.rs`:

```rust
pub fn enumerate_shard_snapshots(
    db: &Database,
    table: &str,
    time_range: TimeRange,
) -> Result<Vec<ShardSnapshot>> {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bqlite_core::storage::SegmentHandle;
    use bqlite_core::SegmentReader as _;

    // Group every visible segment handle by `(shard_id, window_id)`.
    // Reuse the engine's existing time-range-filtered reader so the
    // snapshot covers exactly the segments the query will scan.
    let reader = db.segment_reader_for_time_range(table, time_range)?;
    let mut by_shard: BTreeMap<u32, BTreeMap<u64, Vec<SegmentHandle>>> = BTreeMap::new();
    for handle in reader.segments() {
        let h = handle?;
        by_shard
            .entry(h.shard_id)
            .or_default()
            .entry(h.window_id)
            .or_default()
            .push(h);
    }

    let mut out = Vec::with_capacity(by_shard.len());
    for (shard_id, windows) in by_shard {
        let mut win_vec: Vec<WindowSegments> = Vec::with_capacity(windows.len());
        for (window_id, handles) in windows {
            if handles.is_empty() {
                continue;
            }
            win_vec.push(WindowSegments {
                window_id,
                segments: Arc::from(handles),
            });
        }
        if win_vec.iter().any(|w| !w.segments.is_empty()) {
            out.push(ShardSnapshot {
                shard_id,
                windows: win_vec,
            });
        }
    }
    Ok(out)
}
```

- [ ] **Step 4: Run the tests, make them pass**

```bash
cargo test -p bqlite-engine shard_plan 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 5: Run clippy + fmt across the engine**

```bash
cargo clippy -p bqlite-engine --all-targets --all-features -- -D warnings 2>&1 | tail -10
cargo fmt --check
```

Expected: clean.

- [ ] **Step 6: Subagent review + commit + merge**

Subagent prompt: "Review `enumerate_shard_snapshots` for correctness vs. `ManifestSegmentReader::segments` ordering, time-range filtering equivalence, and any allocation that could be hoisted out of the hot path." Address any blocking findings.

```bash
git add crates/bqlite-engine/src/scheduler/shard_plan.rs crates/bqlite-engine/src/scheduler/mod.rs
git commit -m "TASK-536: Add enumerate_shard_snapshots helper for per-shard dispatch

Manifest-only enumeration that returns one ShardSnapshot per
populated shard, grouping live segments by (shard_id, window_id).
The morsel-scheduler dispatch path (CP3) builds one MorselGenerator
per snapshot. Empty shards are elided per design §3.6 — the morsel
queue's drain logic carries the empty-shard accounting, not the
snapshot list."
```

Merge to main per checkpoint protocol.

---

## Task 3: `MorselScheduler::run_per_shard` — the multi-morsel dispatch entry point

**Files:**
- Modify: `crates/bqlite-engine/src/scheduler/engine_pool.rs` — add `run_per_shard` method.
- Test: same file's `tests` module.

- [ ] **Step 1: Write the failing test for `run_per_shard`**

Append to the `tests` module of `engine_pool.rs`:

```rust
#[test]
fn run_per_shard_dispatches_one_task_per_morsel() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let sched = MorselScheduler::new(4).expect("scheduler builds");
    let snaps: Vec<ShardSnapshot> = (0..4).map(fake_snapshot).collect();
    let calls = AtomicU32::new(0);
    let observed_shards: Mutex<Vec<u32>> = Mutex::new(Vec::new());

    let _accumulators: Vec<Arc<AccumulatorHandle>> = sched
        .run_per_shard(&snaps, |guard, _ctx| {
            calls.fetch_add(1, Ordering::Relaxed);
            observed_shards.lock().unwrap().push(guard.morsel.shard_id);
            Ok::<_, bqlite_core::BqliteError>(())
        })
        .expect("run_per_shard returns OK");

    assert_eq!(calls.load(Ordering::Relaxed), 4, "one call per shard");
    let mut got = observed_shards.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![0, 1, 2, 3], "every shard must be processed exactly once");
}

#[test]
fn run_per_shard_returns_per_shard_accumulators_signaled_done() {
    let sched = MorselScheduler::new(2).expect("scheduler builds");
    let snaps: Vec<ShardSnapshot> = (0..2).map(fake_snapshot).collect();

    let handles = sched
        .run_per_shard(&snaps, |_guard, _ctx| {
            Ok::<_, bqlite_core::BqliteError>(())
        })
        .expect("run_per_shard ok");
    assert_eq!(handles.len(), 2);
    for h in &handles {
        assert!(h.is_done(), "every accumulator must signal done");
    }
}

#[test]
fn run_per_shard_with_zero_shards_returns_empty_vec_and_does_no_work() {
    let sched = MorselScheduler::new(2).expect("scheduler builds");
    let handles = sched
        .run_per_shard::<_, _>(&[] as &[ShardSnapshot], |_, _| {
            unreachable!("must not be called");
            #[allow(unreachable_code)]
            Ok::<_, bqlite_core::BqliteError>(())
        })
        .expect("empty dispatch ok");
    assert!(handles.is_empty());
}

#[test]
fn run_per_shard_propagates_first_worker_error() {
    let sched = MorselScheduler::new(2).expect("scheduler builds");
    let snaps: Vec<ShardSnapshot> = (0..2).map(fake_snapshot).collect();
    let result: Result<Vec<_>, _> = sched.run_per_shard(&snaps, |guard, _ctx| {
        if guard.morsel.shard_id == 1 {
            Err(bqlite_core::BqliteError::Execution("boom".into()))
        } else {
            Ok::<_, bqlite_core::BqliteError>(())
        }
    });
    let err = result.unwrap_err();
    assert!(matches!(err, bqlite_core::BqliteError::Execution(ref m) if m == "boom"));
}
```

- [ ] **Step 2: Run the tests and confirm compile failure**

```bash
cargo test -p bqlite-engine engine_pool 2>&1 | tail -20
```

Expected: error — `run_per_shard` not found. Also `WorkerContext` (the `_ctx` arg) — see step 3 for type.

- [ ] **Step 3: Implement `run_per_shard`**

In `crates/bqlite-engine/src/scheduler/engine_pool.rs`, add the new method on `MorselScheduler`:

```rust
/// Multi-morsel dispatch: build one [`MorselGenerator`] per
/// non-empty `ShardSnapshot`, push every morsel into a single
/// [`MorselQueue`], spawn `min(query_threads, snapshots.len())`
/// Rayon workers each pulling morsels until the queue drains, and
/// return the per-shard [`AccumulatorHandle`]s for the coordinator
/// to merge.
///
/// The closure `work` is called once per morsel inside the worker
/// pool. It receives the [`WorkerMorselGuard`] (which carries the
/// per-shard `AccumulatorHandle`) and a per-worker context the
/// caller can use to populate a [`crate::perf::WorkerMetricsSnapshot`].
/// The first error returned by any closure invocation cancels the
/// remaining work and propagates back to the caller; subsequent
/// errors are silently dropped (matches design §9.2 "first-error
/// wins").
///
/// Permits: `query_threads` are acquired from the [`CoreBudget`]
/// once at the start of the call and released when the dispatch
/// completes — identical to [`Self::submit`]'s permit story.
///
/// **Empty input.** With zero snapshots the method returns
/// `Ok(Vec::new())` without touching the worker pool. The caller
/// has no per-shard work to do and is responsible for short-
/// circuiting whatever post-merge step they were planning.
pub fn run_per_shard<F, E>(
    &self,
    snapshots: &[ShardSnapshot],
    work: F,
) -> std::result::Result<Vec<Arc<AccumulatorHandle>>, E>
where
    F: Fn(&mut WorkerMorselGuard, &mut PerWorkerCtx) -> std::result::Result<(), E> + Send + Sync,
    E: Send,
{
    if snapshots.is_empty() {
        return Ok(Vec::new());
    }
    let _permits = self.core_budget.acquire_n(self.query_threads);

    // Per-shard bookkeeping: one AccumulatorHandle + one generator per shard.
    let accumulators: Vec<Arc<AccumulatorHandle>> = snapshots
        .iter()
        .map(|s| Arc::new(AccumulatorHandle::new(s.shard_id, None)))
        .collect();
    let by_shard: std::collections::HashMap<u32, Arc<AccumulatorHandle>> = accumulators
        .iter()
        .map(|h| (h.shard_id(), Arc::clone(h)))
        .collect();

    let queue = Arc::new(MorselQueue::new(
        2.max(self.query_threads * 2).max(snapshots.len() * 2),
    ));

    // Single-producer push (this thread): enumerate every morsel
    // upfront. Sub-shard generation is out of scope; each generator
    // emits exactly one whole-shard morsel.
    let mut total_pushed: u64 = 0;
    for snap in snapshots {
        let mut gen = super::morsel::MorselGenerator::degenerate(snap.clone());
        while let Some(m) = gen.take_next() {
            queue
                .push(m)
                .expect("morsel queue capacity sized for all morsels");
            total_pushed += 1;
        }
        // Mark generator drained for this shard's accumulator handle.
        let total_for_shard = gen.total_emitted().unwrap_or(0);
        if let Some(handle) = by_shard.get(&snap.shard_id) {
            handle.mark_total_emitted(total_for_shard);
        }
    }
    queue.mark_drained();

    let num_workers = self.query_threads.min(snapshots.len()).max(1);
    let first_error: Mutex<Option<E>> = Mutex::new(None);
    let first_error_ref = &first_error;
    let queue_for_workers = Arc::clone(&queue);
    let by_shard_for_workers = &by_shard;
    let work_ref = &work;

    self.pool.scope(|s| {
        for _ in 0..num_workers {
            let q = Arc::clone(&queue_for_workers);
            s.spawn(move |_| {
                let mut ctx = PerWorkerCtx::default();
                loop {
                    let morsel = match q.pop_or_park(std::time::Duration::from_millis(10)) {
                        Ok(m) => m,
                        Err(_drained) => break,
                    };
                    let shard = morsel.shard_id;
                    let acc = by_shard_for_workers
                        .get(&shard)
                        .cloned()
                        .expect("morsel shard has an accumulator handle");
                    let on_done: ShardDoneCallback = Arc::new(|_, _| {});
                    let mut guard = WorkerMorselGuard::new(morsel, acc, on_done);
                    let r = work_ref(&mut guard, &mut ctx);
                    drop(guard);
                    if let Err(e) = r {
                        let mut slot = first_error_ref.lock().expect("first_error poisoned");
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                        // Drain the queue without running further work
                        // so the remaining morsels' guards still drop
                        // cleanly (decrement outstanding, signal done).
                        while let Some(m) = q.pop() {
                            let acc = by_shard_for_workers.get(&m.shard_id).cloned().unwrap();
                            let on_done: ShardDoneCallback = Arc::new(|_, _| {});
                            let g = WorkerMorselGuard::new(m, acc, on_done);
                            drop(g);
                        }
                        break;
                    }
                }
                ctx.morsels_dispatched_seen
                    // suppress unused-mut warning; ctx already used.
                    ;
                let _ = ctx;
            });
        }
    });

    let _ = total_pushed; // currently informational; future TASK-537 reads.

    if let Some(err) = first_error.into_inner().expect("first_error poisoned") {
        return Err(err);
    }
    Ok(accumulators)
}
```

Add a `PerWorkerCtx` struct in the same file (above `MorselScheduler` impl):

```rust
/// Per-worker scratch carried through one `run_per_shard` invocation.
///
/// Workers populate this from inside the `work` closure; the caller
/// gets one snapshot per worker after the dispatch returns through
/// the side channel they wire up themselves (e.g., a `Mutex<Vec<...>>`
/// captured in the closure). Today the only field is the per-worker
/// morsel count, which the engine uses to record one
/// [`crate::perf::WorkerMetricsSnapshot`] per worker.
#[derive(Debug, Default)]
pub struct PerWorkerCtx {
    /// Morsels this worker has pulled in the current dispatch.
    pub morsels_dispatched: u64,
    /// Hint for tests — flipped to true on every `morsels_dispatched`
    /// increment so the scheduler tests can sanity-check the field
    /// is wired without scraping the closure environment.
    pub morsels_dispatched_seen: bool,
}
```

Update the imports at the top of the file:

```rust
use super::morsel::{Morsel, ShardSnapshot};
```

becomes (add `MorselGenerator` not needed — it's path-qualified above; ensure `WorkerMorselGuard` and `ShardDoneCallback` are in scope):

```rust
use super::morsel::{Morsel, ShardSnapshot};
use super::queue::MorselQueue;
use super::worker::{ShardDoneCallback, WorkerMorselGuard};
```

(plus existing imports). Re-export `PerWorkerCtx` from `mod.rs`:

```rust
pub use engine_pool::{build_from_config, BuildError, MorselScheduler, PerWorkerCtx};
```

The work closure increments `morsels_dispatched` and flips `morsels_dispatched_seen`. Update the spawn loop:

```rust
let mut guard = WorkerMorselGuard::new(morsel, acc, on_done);
ctx.morsels_dispatched += 1;
ctx.morsels_dispatched_seen = true;
let r = work_ref(&mut guard, &mut ctx);
drop(guard);
```

(Re-arrange so the increment happens before `work_ref` so even on error paths the count reflects "morsels pulled" — matches §8.3 "morsels_dispatched is collected on every successful queue.push"; we count on every successful pop.)

- [ ] **Step 4: Run the tests, expect them to pass**

```bash
cargo test -p bqlite-engine engine_pool 2>&1 | tail -30
```

Expected: every test passes.

- [ ] **Step 5: Run clippy + fmt**

```bash
cargo clippy -p bqlite-engine --all-targets --all-features -- -D warnings 2>&1 | tail -20
cargo fmt --check
```

Expected: clean.

- [ ] **Step 6: Subagent review + commit + merge**

Subagent prompt: "Review `MorselScheduler::run_per_shard` for: (a) correctness of permit acquisition vs. concurrent queries, (b) panic safety of the worker scope (does a panicking closure leak permits / outstanding morsels?), (c) error propagation ordering — first error wins, others dropped — and whether the queue drain on error is leak-free." Iterate until APPROVE.

```bash
git add crates/bqlite-engine/src/scheduler/engine_pool.rs crates/bqlite-engine/src/scheduler/mod.rs
git commit -m "TASK-536: Add MorselScheduler::run_per_shard for multi-morsel dispatch

One Rayon task per morsel up to query_threads; per-shard
AccumulatorHandle returned for the coordinator's cross-shard
merge. The dispatch acquires query_threads permits at the start
of the call (per design §7.1 atomic acquire_n) and releases on
return. PerWorkerCtx carries a per-worker morsels_dispatched
count the engine folds into a WorkerMetricsSnapshot in CP4."
```

Merge per checkpoint protocol.

---

## Task 4: Engine — Plan classification + per-shard execution path

**Files:**
- Modify: `crates/bqlite-engine/src/query.rs` — add a plan classifier + `run_query_inner` rewrite to dispatch per-shard for the supported shapes.
- Modify: `crates/bqlite-engine/src/bind.rs` — expose a `bind_physical_against_reader` helper that lets callers swap the segment reader for a single-shard one (for non-aggregate per-shard binding) and a `bind_aggregate_input_against_reader` helper for the aggregate-rooted path.
- Test: keep existing test suite green; per-shard correctness asserted by the existing acceptance/runtime suites and strengthened in Task 5.

- [ ] **Step 1: Write the failing assertion against the existing acceptance fixture**

This is a verification-driven task — instead of a fresh unit test, run the existing acceptance band, observe the *current* output of `metrics.morsels_per_shard_min` and `metrics.num_workers`, and prove the values are still 0 / 1 (proving the new code path is needed). After Task 4 lands, the same query produces > 0 / > 1.

```bash
cargo test -p bqlite-tests multi_shard_stats_under_floor_budget_matches_hand_computed -- --nocapture 2>&1 | tail -10
```

This test should still pass (functional answer is correct) but the metrics it would assert (in Task 6) are still degenerate.

- [ ] **Step 2: Add a plan-shape classifier in `query.rs`**

Inside `crates/bqlite-engine/src/query.rs`, add (above `run_query_inner`):

```rust
/// Plan shapes that can be safely fanned out per shard. The
/// classifier walks the planner output and returns the
/// per-shard dispatch strategy:
///
/// - `Aggregate { input }` where `input` is per-shard-safe → run
///   the input per shard, accumulate into a per-shard
///   `HashAccumulator`, merge across shards on the coordinator.
/// - `Scan` / `Filter` / `Project` / `Limit` (any depth) → run
///   the whole tree per shard, concat outputs.
/// - Anything else (Sort, Distinct, MergeSources, SubqueryFilter,
///   SequenceMatch, EventSelect, Sessionize, Attribute,
///   FusedSegment with a top-level shape we cannot split) → fall
///   back to the legacy single-task path.
#[derive(Debug, Clone, Copy)]
enum DispatchShape {
    /// Per-shard whole-tree dispatch + concat.
    PerShardConcat,
    /// Per-shard execute-input + per-shard `HashAccumulator` +
    /// coordinator merge.
    PerShardAggregate,
    /// Whole-database single-task dispatch (legacy).
    SingleTask,
}

fn classify_dispatch(plan: &PhysicalPlan) -> DispatchShape {
    match plan {
        PhysicalPlan::Aggregate(agg) if is_per_shard_safe_input(&agg.input) => {
            DispatchShape::PerShardAggregate
        }
        // Pure data-plane shapes are safe to concat per shard.
        p if is_per_shard_safe_input(p) => DispatchShape::PerShardConcat,
        _ => DispatchShape::SingleTask,
    }
}

fn is_per_shard_safe_input(plan: &PhysicalPlan) -> bool {
    use bqlite_planner::PhysicalPlan as P;
    match plan {
        P::Scan(_) => true,
        P::Filter(f) => is_per_shard_safe_input(&f.input),
        P::Project(p) => is_per_shard_safe_input(&p.input),
        P::Limit(l) => is_per_shard_safe_input(&l.input),
        P::FusedSegment(fs) => is_per_shard_safe_input(&fs.input),
        // Cohort pushdown / SubqueryFilter / Sample / sort / distinct /
        // sequence / merge-sources / DDL / explain / delete / aggregate
        // (we only enter this from a parent scope) all force fallback.
        _ => false,
    }
}
```

(Confirm the variant names match `bqlite_planner::PhysicalPlan` — adjust where needed: e.g. `FilterPhysical` field names. Adapt the `match` arms to the actual enum shape.)

- [ ] **Step 3: Wire the dispatch in `run_query_inner`**

Replace the existing `scheduler.submit(...)` block in `run_query_inner` with:

```rust
let shape = classify_dispatch(&physical);
let (rows, num_worker_snapshots) = match shape {
    DispatchShape::SingleTask => run_single_task(scheduler, db, ctx, &physical)?,
    DispatchShape::PerShardConcat => {
        run_per_shard_concat(scheduler, db, ctx, &physical)?
    }
    DispatchShape::PerShardAggregate => {
        run_per_shard_aggregate(scheduler, db, ctx, &physical)?
    }
};

// One `WorkerMetricsSnapshot::default()` per worker that contributed
// — this seeds num_workers correctly for both the legacy single-task
// and the multi-morsel path. CPU/idle/busy fields are populated by
// TASK-537; here we only record the count.
for _ in 0..num_worker_snapshots {
    ctx.record_worker_snapshot(WorkerMetricsSnapshot::default());
}
```

Implement the three helpers as private functions. **`run_single_task`** is the existing logic (same `submit` pattern, returns `(rows, 1)` so a single snapshot is recorded):

```rust
fn run_single_task(
    scheduler: &MorselScheduler,
    db: &mut Database,
    ctx: &QueryContext,
    plan: &PhysicalPlan,
) -> bqlite_core::Result<(Vec<RecordBatch>, usize)> {
    let mut operator = bind_physical(plan, db, ctx)?;
    let rows = scheduler.submit(move || -> bqlite_core::Result<Vec<RecordBatch>> {
        let drive_result = drive_to_completion(operator.as_mut());
        let close_result = operator.close();
        let rows = drive_result?;
        close_result?;
        drop(operator);
        Ok(rows)
    })?;
    Ok((rows, 1))
}
```

**`run_per_shard_concat`** — bind one whole tree per shard with a shard-scoped reader:

```rust
fn run_per_shard_concat(
    scheduler: &MorselScheduler,
    db: &mut Database,
    ctx: &QueryContext,
    plan: &PhysicalPlan,
) -> bqlite_core::Result<(Vec<RecordBatch>, usize)> {
    let table = primary_table_name(plan)
        .ok_or_else(|| BqliteError::Execution("per-shard dispatch needs a primary table".into()))?;
    let reader_range = primary_table_time_range(plan).unwrap_or_else(TimeRange::unbounded);
    let snapshots = crate::scheduler::enumerate_shard_snapshots(db, &table, reader_range)?;
    if snapshots.is_empty() {
        // Empty fixture — fall through to single-task to preserve the
        // legacy "empty result, one snapshot" shape.
        return run_single_task(scheduler, db, ctx, plan);
    }
    // Pre-bind one operator tree per shard on the calling thread —
    // bind borrows `&mut Database` for cohort registration and is
    // not safe to interleave from worker threads.
    let mut bound: Vec<(u32, Box<dyn PhysicalOperator>)> = Vec::with_capacity(snapshots.len());
    for snap in &snapshots {
        let op = bind_physical_for_shard(plan, db, ctx, snap.shard_id)?;
        bound.push((snap.shard_id, op));
    }
    // Move bound trees into worker thread-safe slots keyed by shard.
    let by_shard: Mutex<std::collections::HashMap<u32, Box<dyn PhysicalOperator>>> = Mutex::new(
        bound.into_iter().collect(),
    );
    let collected: Mutex<Vec<RecordBatch>> = Mutex::new(Vec::new());
    let workers_used = Mutex::new(std::collections::HashSet::<usize>::new());

    let _ = scheduler.run_per_shard::<_, BqliteError>(&snapshots, |guard, ctx_w| {
        let shard = guard.morsel.shard_id;
        let mut op = by_shard
            .lock()
            .expect("per-shard tree slot poisoned")
            .remove(&shard)
            .expect("each shard has one bound tree");
        let drive_result = drive_to_completion(op.as_mut());
        let close_result = op.close();
        let rows = drive_result?;
        close_result?;
        collected
            .lock()
            .expect("collected poisoned")
            .extend(rows);
        // Track that this worker did at least one morsel.
        let tid = std::thread::current().id();
        let mut tids = workers_used.lock().expect("workers_used poisoned");
        // ThreadId hashing trick: use the std::thread::ThreadId hash.
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        tid.hash(&mut hasher);
        tids.insert(hasher.finish() as usize);
        let _ = ctx_w; // morsels_dispatched is incremented inside run_per_shard
        Ok(())
    })?;
    let rows = collected.into_inner().expect("collected poisoned");
    let workers = workers_used.into_inner().expect("workers_used poisoned").len().max(1);
    Ok((rows, workers))
}
```

**`run_per_shard_aggregate`** — bind only the aggregate's input per shard, accumulate per-shard, merge:

```rust
fn run_per_shard_aggregate(
    scheduler: &MorselScheduler,
    db: &mut Database,
    ctx: &QueryContext,
    plan: &PhysicalPlan,
) -> bqlite_core::Result<(Vec<RecordBatch>, usize)> {
    use bqlite_operators::aggregate::HashAccumulator;
    use bqlite_planner::PhysicalPlan as P;
    let agg = match plan {
        P::Aggregate(a) => a,
        _ => unreachable!("classify_dispatch guarantees aggregate root"),
    };
    let table = primary_table_name(&agg.input)
        .ok_or_else(|| BqliteError::Execution("aggregate-per-shard needs a primary table".into()))?;
    let reader_range = primary_table_time_range(&agg.input).unwrap_or_else(TimeRange::unbounded);
    let snapshots = crate::scheduler::enumerate_shard_snapshots(db, &table, reader_range)?;
    if snapshots.is_empty() {
        return run_single_task(scheduler, db, ctx, plan);
    }

    // Pre-bind the aggregate's INPUT per shard (one tree each). The
    // aggregate itself is materialised at the coordinator level
    // through one HashAccumulator per shard.
    let mut bound: Vec<(u32, Box<dyn PhysicalOperator>)> = Vec::with_capacity(snapshots.len());
    for snap in &snapshots {
        let op = bind_physical_for_shard(&agg.input, db, ctx, snap.shard_id)?;
        bound.push((snap.shard_id, op));
    }

    // Build a HashAccumulator per shard with the same aggregate spec.
    let agg_spec_for_shard = || -> HashAccumulator {
        // Identical to HashAggregateOperator::new's accumulator setup
        // (kept inline here so we don't need to expose a constructor
        // helper — the aggregate Physical carries every input we need).
        let functions = agg.aggregates.iter().map(|a| a.function).collect();
        let input_types = agg
            .aggregates
            .iter()
            .map(|a| a.arg.as_ref().map(|e| e.result_type.clone()))
            .collect();
        let group_by_col_names: Vec<String> =
            (0..agg.group_by.len()).map(|i| format!("__grp_{i}")).collect();
        let agg_arg_col_names: Vec<Option<String>> = agg
            .aggregates
            .iter()
            .enumerate()
            .map(|(i, a)| a.arg.as_ref().map(|_| format!("__agg_{i}")))
            .collect();
        HashAccumulator::new(
            functions,
            input_types,
            agg.output_schema.clone(),
            group_by_col_names,
            agg_arg_col_names,
            agg.max_groups,
        )
    };

    let by_shard_op: Mutex<std::collections::HashMap<u32, Box<dyn PhysicalOperator>>> = Mutex::new(
        bound.into_iter().collect(),
    );
    let workers_used = Mutex::new(std::collections::HashSet::<usize>::new());

    // Per-shard accumulators returned from run_per_shard.
    let handles = scheduler.run_per_shard::<_, BqliteError>(&snapshots, |guard, _ctx_w| {
        let shard = guard.morsel.shard_id;
        let mut op = by_shard_op
            .lock()
            .expect("per-shard subplan poisoned")
            .remove(&shard)
            .expect("subplan present");
        // Build the per-shard accumulator and seat it in the
        // AccumulatorHandle so the cross-shard merge can take it.
        let mut acc = agg_spec_for_shard();
        op.open()?;
        // Drive and feed every batch through the accumulator's
        // HashAggregateOperator-equivalent eval+update. The simplest
        // correct equivalent: re-use a HashAggregateOperator instance
        // wrapping a no-op pass-through child is overkill; we already
        // have raw input batches, so evaluate the group-by and
        // aggregate expressions inline.
        while let Some(batch) = op.next_batch()? {
            // Evaluate group-by + aggregate args via the helper
            // exposed by the aggregate operator.
            let (group_arrays, agg_arrays) =
                bqlite_operators::aggregate::evaluate_aggregate_inputs(
                    &agg.group_by,
                    &agg.aggregates,
                    &batch,
                )?;
            acc.update_evaluated(batch.num_rows(), &group_arrays, &agg_arrays)?;
        }
        op.close()?;
        // Park the per-shard accumulator on the handle so the
        // cross-shard merge picks it up.
        *guard.accumulator().lock_or_poisoned().unwrap() = Some(Box::new(acc));
        let tid = std::thread::current().id();
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        tid.hash(&mut hasher);
        workers_used
            .lock()
            .expect("workers_used poisoned")
            .insert(hasher.finish() as usize);
        Ok(())
    })?;

    // Cross-shard merge on the coordinator (single-threaded — design
    // §6.4). Take ownership of every shard's accumulator and pairwise
    // merge into the first one.
    let mut merged: Option<Box<dyn bqlite_operators::aggregate::Accumulator>> = None;
    for h in &handles {
        let acc = h
            .take_accumulator()
            .expect("shard accumulator parked by worker");
        match merged.as_mut() {
            None => merged = Some(acc),
            Some(m) => m.merge(acc)?,
        }
    }
    let final_batch = match merged {
        Some(m) => m.finish()?,
        None => return Ok((Vec::new(), 1)),
    };
    let workers = workers_used.into_inner().expect("workers_used poisoned").len().max(1);
    Ok((vec![final_batch], workers))
}
```

This requires a small helper exposed by `bqlite-operators::aggregate` — `evaluate_aggregate_inputs`. Add it next to `HashAggregateOperator::process_batch`:

```rust
/// Evaluate group-by and aggregate-argument expressions against
/// `batch`, returning the parallel arrays the [`HashAccumulator`]'s
/// `update_evaluated` consumes. Public so the engine's per-shard
/// dispatch path (TASK-536) can drive a per-shard accumulator
/// without instantiating a full [`HashAggregateOperator`] per shard.
pub fn evaluate_aggregate_inputs(
    group_by: &[(CompiledExpr, String)],
    aggregates: &[CompiledAgg],
    batch: &RecordBatch,
) -> Result<(Vec<arrow::array::ArrayRef>, Vec<Option<arrow::array::ArrayRef>>)> {
    let group_arrays: Vec<arrow::array::ArrayRef> = group_by
        .iter()
        .map(|(expr, _)| eval::evaluate(expr, batch))
        .collect::<Result<Vec<_>>>()?;
    let agg_arrays: Vec<Option<arrow::array::ArrayRef>> = aggregates
        .iter()
        .map(|agg| {
            agg.arg
                .as_ref()
                .map(|arg| eval::evaluate(arg, batch))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((group_arrays, agg_arrays))
}
```

Add helpers in `query.rs`:

```rust
fn primary_table_name(plan: &PhysicalPlan) -> Option<String> {
    use bqlite_planner::PhysicalPlan as P;
    match plan {
        P::Scan(s) => Some(s.table.clone()),
        P::Filter(f) => primary_table_name(&f.input),
        P::Project(p) => primary_table_name(&p.input),
        P::Limit(l) => primary_table_name(&l.input),
        P::FusedSegment(fs) => primary_table_name(&fs.input),
        _ => None,
    }
}

fn primary_table_time_range(plan: &PhysicalPlan) -> Option<TimeRange> {
    use bqlite_planner::PhysicalPlan as P;
    match plan {
        P::Scan(s) => s.reader_range,
        P::Filter(f) => primary_table_time_range(&f.input),
        P::Project(p) => primary_table_time_range(&p.input),
        P::Limit(l) => primary_table_time_range(&l.input),
        P::FusedSegment(fs) => primary_table_time_range(&fs.input),
        _ => None,
    }
}
```

And in `bind.rs`, expose `bind_physical_for_shard` that mirrors `bind_physical` but threads a `shard_filter: Option<u32>` through the recursive walk. The simplest approach: clone the plan tree, replace any `ScanPhysical::table` reader with a shard-restricted reader at bind time. Since `bind_scan` builds its own reader from `db.segment_reader_for_time_range`, branch internally:

```rust
pub fn bind_physical(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
) -> Result<Box<dyn PhysicalOperator>> {
    bind_physical_with_cache(plan, db, ctx, &mut Default::default(), &mut Vec::new())
}

pub fn bind_physical_for_shard(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
    shard_filter: u32,
) -> Result<Box<dyn PhysicalOperator>> {
    // Threads shard_filter into bind_scan via a thread-local override.
    // Simpler: pipe through a `BindOptions { shard_filter }` parameter.
    // We pick the parameter route to avoid global state.
    bind_physical_with_options(
        plan,
        db,
        ctx,
        BindOptions { shard_filter: Some(shard_filter) },
    )
}
```

Refactor `bind_physical_with_cache` to take a `BindOptions` parameter (default = no shard filter). In `bind_scan`, when `options.shard_filter.is_some()`, build the reader via `db.segment_reader_for_shard(...)` instead of `db.segment_reader_for_time_range(...)`. Be careful to thread `BindOptions` through every recursive call site (`MergeSources`, `Aggregate`, etc.); a missed call site silently undoes the shard filtering.

(For simplicity, factor a `BindCtx` struct with both the cache and the options. Make `BindOptions::default()` produce no shard filter so all existing callers stay unchanged.)

- [ ] **Step 4: Run the existing engine tests**

```bash
cargo test -p bqlite-engine 2>&1 | tail -30
cargo test -p bqlite-tests 2>&1 | tail -30
```

Expected: every test passes (functional answers preserved; metrics will be checked in Task 5).

- [ ] **Step 5: Run clippy + fmt**

```bash
cargo clippy --all-targets --all-features -- -D warnings 2>&1 | tail -20
cargo fmt --check
```

Expected: clean.

- [ ] **Step 6: Subagent review + commit + merge**

Subagent prompt: "Review the per-shard dispatch in `query.rs` for: (a) correctness of plan classification — does it cover every safe shape and conservatively fall back for everything else?, (b) per-shard binding — is `bind_physical_for_shard` shard-restricting at every Scan-leaf in the tree, including nested shapes (Filter→Project→FusedSegment)?, (c) aggregate path — is the per-shard `HashAccumulator` initialised identically to `HashAggregateOperator`'s accumulator and is the cross-shard merge order-stable for non-commutative operators?, (d) error propagation — does an error inside one shard's worker cleanly tear down the other shards' workers without leaking spill files?, (e) `WorkerMetricsSnapshot` count — `num_workers` reflects unique threads that ran morsels, not the morsel count." Iterate to APPROVE.

```bash
git add crates/bqlite-engine/src/query.rs crates/bqlite-engine/src/bind.rs crates/bqlite-operators/src/aggregate/mod.rs
git commit -m "TASK-536: Real per-shard morsel dispatch in Engine::query

Replace the degenerate single-task path with a plan-shape
classifier that fans out per-shard via MorselScheduler::run_per_shard
for aggregate-rooted and pure data-plane queries. Aggregate-rooted
plans bind only the input per shard, accumulate into a per-shard
HashAccumulator parked on the AccumulatorHandle, and cross-shard
merge on the coordinator per design §6.4. Sort, MergeSources,
SubqueryFilter, SequenceMatch, and the entity-operator family fall
back to the single-task path until later tasks generalise the
dispatch (TASK-541 timeout, TASK-545 sort merge)."
```

Merge per checkpoint protocol.

---

## Task 5: Strengthen `wave5_acceptance.rs` band 1

**Files:**
- Modify: `tests/tests/wave5_acceptance.rs:292-333` — add `morsels_per_shard_min > 0` and `num_workers > 1` assertions to the multi-shard band.

- [ ] **Step 1: Update the assertion block**

In `multi_shard_stats_under_floor_budget_matches_hand_computed`, after the existing answer + `peak_memory_bytes` assertions, add:

```rust
    // Per-shard morsel dispatch (TASK-536) must fan out to multiple
    // workers and emit at least one morsel per populated shard. The
    // single-task fallback used to seed `num_workers == 1` and every
    // morsels_per_shard_* field at zero; with the new dispatcher,
    // both numbers are real signals.
    assert!(
        result.metrics.num_workers > 1,
        "multi-shard query must fan out to >1 worker; got {}",
        result.metrics.num_workers
    );
    assert!(
        result.metrics.morsels_per_shard_min > 0,
        "every populated shard must produce at least one morsel; got {}",
        result.metrics.morsels_per_shard_min
    );
    assert!(
        result.metrics.morsels_per_shard_max > 0,
        "morsels_per_shard_max must be set; got {}",
        result.metrics.morsels_per_shard_max
    );
    assert_eq!(
        result.metrics.morsels_per_shard_min, result.metrics.morsels_per_shard_max,
        "v1 dispatch emits exactly one morsel per shard — min == max"
    );
    // morsels_dispatched must equal num populated shards.
    let pop_shards = populated_shards(&db, "events") as u64;
    assert_eq!(
        result.metrics.morsels_dispatched, pop_shards,
        "morsels_dispatched must equal populated shard count"
    );
```

This requires `record_morsels_per_shard` to be wired in `query.rs`. After `run_per_shard*` returns the per-shard accumulator handles in Task 4, also collect per-shard morsel counts (each generator emits 1 morsel for the v1 implementation, but the shape is uniform so we hand `&[1u64; n]` into `metrics.record_morsels_per_shard`):

In `query.rs`, after the dispatch, before `take_query_metrics`:

```rust
// Record per-shard morsel counts. v1: every populated shard
// emits exactly one morsel, so we hand a Vec of `1`s the same
// length as the populated-shard count. Once the per-entity-range
// generator lands (follow-on task), this becomes the per-shard
// generator's `total_emitted`.
let shard_count = match shape {
    DispatchShape::SingleTask => 0, // single-task path skips shard metrics
    _ => num_worker_snapshots, // we use the same number we recorded snapshots for
};
// More accurately: we tracked the populated-shard count when
// classifying, so plumb it back. Refactor the dispatch helpers to
// return (rows, workers, morsels_per_shard_counts).
```

Simpler refactor: change the dispatch helpers' return type to `(Vec<RecordBatch>, usize /* workers */, Vec<u64> /* morsels_per_shard */)`. Update each helper to populate the third value. Then:

```rust
let mut metrics_view = ctx.borrow_query_metrics_mut();
metrics_view.record_morsels_per_shard(&morsel_counts);
drop(metrics_view);
```

Or expose `QueryContext::record_morsels_per_shard` that wraps the same handle. (Pick whichever pattern matches the existing perf-recording API; check `record_worker_snapshot` for the established style.)

- [ ] **Step 2: Run the strengthened acceptance test**

```bash
cargo test -p bqlite-tests multi_shard_stats_under_floor_budget_matches_hand_computed -- --nocapture 2>&1 | tail -20
```

Expected: PASS — `num_workers > 1` (typically `>= 2` on the multi-shard fixture, capped at `query_threads`), `morsels_per_shard_min == morsels_per_shard_max == 1`, `morsels_dispatched == populated_shards`.

- [ ] **Step 3: Run the full Wave 5 + Wave 4 suite**

```bash
cargo test -p bqlite-tests 2>&1 | tail -30
```

Expected: every test passes (no regression in spill / cancellation / cohort / sort bands).

- [ ] **Step 4: Run the local-ci script end-to-end**

```bash
./scripts/local-ci.sh 2>&1 | tail -40
```

Expected: every stage passes — fmt, dep-direction, clippy, build, full test suite.

- [ ] **Step 5: Subagent review + commit + merge**

Subagent prompt: "Review the strengthened acceptance assertions for: (a) over-specification — are the values `morsels_per_shard_min == morsels_per_shard_max == 1` brittle to the future per-entity-range generator?, (b) flake risk — is `num_workers > 1` deterministic on a 1-core CI runner?" If the reviewer flags the 1-core risk, add a `populated_shards` lower bound check and a `query_threads.min(populated_shards) > 1` assertion path so the test is conditional on the platform.

If clean:

```bash
git add tests/tests/wave5_acceptance.rs
git commit -m "TASK-536: Assert real morsel dispatch in wave5_acceptance

Replace the Some(_) smoke check on result.metrics with concrete
assertions on num_workers, morsels_per_shard_{min,max}, and
morsels_dispatched. The multi-shard fixture populates >24 of 32
default shards; the dispatcher fans those out to query_threads
workers, so the metrics are non-degenerate post-TASK-536."
```

Merge per checkpoint protocol.

---

## Task 6: Reconcile `morsel-scheduler.md` §11 with the v1 dispatch shape

**Files:**
- Modify: `docs/design/engine/morsel-scheduler.md` — add a `§11.1 TASK-536 reconciliation` block.

- [ ] **Step 1: Append the reconciliation note**

After the existing §11.1 reconciliation list, append:

```markdown
**TASK-536 (real per-shard morsel dispatch).** The TASK-523 scaffold
landed the queue, accumulator handle, worker guard, and run_degenerate
stub but kept `Engine::query` on a single whole-database task.
TASK-536 closes that gap by:

- Adding `MorselScheduler::run_per_shard` — the multi-morsel dispatch
  entry point that pushes one morsel per non-empty shard onto the
  queue, spawns up to `query_threads` Rayon workers, returns the
  per-shard `AccumulatorHandle`s for the coordinator's cross-shard
  merge.
- Plan classification in `Engine::query`: aggregate-rooted plans
  use `run_per_shard` with per-shard `HashAccumulator`s + cross-shard
  merge per §6.4; pure data-plane plans (Scan / Filter / Project /
  Limit / FusedSegment) use `run_per_shard` with whole-tree-per-shard
  binding + concat; everything else (Sort, MergeSources, joins,
  subquery filters, sequence/event/sessionize/attribute) falls back
  to the legacy single-task path.
- `WorkerMetricsSnapshot` recorded once per worker (one default
  snapshot per unique worker thread that ran a morsel), so
  `num_workers`, `morsels_per_shard_*`, and the future `worker_busy_*`
  /`worker_idle_*` fields carry real values.

The v1 generator still emits exactly one morsel per shard
(`EntityRange::All`). Sub-shard morsel halving — the §3.4 control
loop reading `MorselSizeState::current_target_rows` — is wired but
not exercised by the v1 dispatch; it lands once the per-entity-range
generator and the operators that respect `(shard, entity_range)`
ship in a follow-on task.
```

- [ ] **Step 2: Verify the doc renders cleanly**

```bash
grep -n "§11.1" docs/design/engine/morsel-scheduler.md
```

Expected: The new section is present and section numbering does not collide.

- [ ] **Step 3: Commit + merge**

```bash
git add docs/design/engine/morsel-scheduler.md
git commit -m "TASK-536: Reconcile morsel-scheduler.md with real dispatch shape

Document the v1 per-shard dispatch lands one whole-shard morsel
per shard with run_per_shard; sub-shard halving and the entity-
range generator stay queued for a follow-on task. Documents the
fall-back-to-single-task list (Sort, MergeSources, etc.) so the
next reader does not have to re-derive the v1 boundary."
```

Merge per checkpoint protocol.

---

## Task 7: Move lock to completed/

**Files:**
- Move: `tasks/active/TASK-536.lock` → `tasks/completed/TASK-536.done`.

- [ ] **Step 1: Move the lock file with `git mv` and add the `completed_at` field**

```bash
git mv tasks/active/TASK-536.lock tasks/completed/TASK-536.done
# Edit the file to insert "completed_at" (use Edit tool, not sed)
```

The done file content:

```json
{
  "agent_id": "agent-1",
  "task_id": "TASK-536",
  "claimed_at": "2026-05-09T06:25:58Z",
  "completed_at": "<UTC ISO-8601 now>",
  "branch": "task/TASK-536",
  "description": "Real per-shard morsel dispatch (TASK-523 closure)"
}
```

- [ ] **Step 2: Commit + push**

```bash
git add tasks/active/TASK-536.lock tasks/completed/TASK-536.done
git commit -m "TASK-536: completed"
git checkout main && git pull origin main
git merge task/TASK-536 --ff-only
git push origin main
```

- [ ] **Step 3: End the agent turn.**
