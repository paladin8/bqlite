# TASK-408 — Compaction Executor + Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the Wave 4 size-tiered compaction path for a single table — pick eligible `(window, shard)` inputs, k-way merge them in entity order, re-encode through the latest selector, publish atomically, cooperate with query load.

**Architecture:** A new `crates/bqlite-storage/src/compaction.rs` module hosts (1) a per-job executor that uses the existing `SegmentFileReader` + `KWayMergeScan` pipeline to merge inputs and re-encodes outputs through the existing `select_encoding` + `write_segment` path; (2) a `CompactionScheduler` with a priority queue over `(table, window, shard)` plus a configurable thread pool; (3) a `CoreBudget` semaphore (filled by TASK-438 from the query side, drained one permit per row-group on the compaction side); (4) `Database::compact_now(table)` synchronous API. Manifest publication promotes the existing crate-private `update_manifest` to `pub(crate)` and adds a single `Manifest::replace_segments` mutation, so removing N inputs and adding the new output land as **one** atomic `tmp + fsync + rename` — preserving the all-or-nothing guarantee of compaction-concurrency.md §6 even though full `Arc<Manifest>` snapshot isolation (§6 step 4 in-memory swap, §7 10s reclamation sweep) is deferred to TASK-438. This deferral is honest: `Database` currently owns its `Manifest` by value, no concurrent readers exist, and the design doc is updated with a §12 "implementation status" footer pinning this and a §12.1 follow-on task referencing the streaming row-group writer.

**Tech Stack:** Rust 2021, Arrow `arrow` crate, existing `bqlite-storage` writer/reader/merge modules, `std::sync::{Arc, Mutex, Condvar}`, `std::thread`, `std::time::{Duration, Instant}`. No new external dependencies.

**Scope boundaries (from `docs/design/storage/compaction-concurrency.md` §1 and §11):**
- IN: §2 unit of work, §3 scheduler model + `compact_now`, §4 core-budget semaphore (the surface; TASK-438 will fill it from query side), §5 backlog metric, §6 manifest publish (reusing existing atomic update path), §7 reclamation (simplified — see below), §8 failure recovery, the `compaction_backlog_l0_segments` metric.
- OUT (deferred to other tasks): §9 tombstone snapshot at job start (TASK-434/435), full `Arc<Manifest>` snapshot isolation + 10s reclamation sweep (TASK-438 prerequisite — until queries hold `Arc<Manifest>`, no concurrent reader exists, so files can be deleted immediately after publish; the design doc gets a note pinning this as a TASK-438 follow-on).
- Each checkpoint is independently mergeable, runs `scripts/local-ci.sh` clean, and is reviewed by a code-review subagent before commit per `AGENTS.md`.

---

## File Structure

| File | Responsibility | Status |
|---|---|---|
| `crates/bqlite-storage/src/compaction.rs` | `CompactionConfig`, `CoreBudget`, `CompactionScheduler`, `CompactionMetrics`, executor `compact_one`, eligibility logic, retry cooldown | **Create** |
| `crates/bqlite-storage/src/lib.rs` | Add `pub mod compaction;` and re-exports | Modify |
| `crates/bqlite-storage/src/database.rs` | Add `Database::compact_now(&mut self, table: &str)` thin wrapper that delegates into `compaction::run_compact_now` | Modify |
| `docs/design/storage/compaction-concurrency.md` | Add "Implemented vs deferred" footer noting (a) immediate-deletion in lieu of 10s sweep until TASK-438 lands `Arc<Manifest>`, (b) tombstone hooks deferred to TASK-434/435 | Modify (CP1) |
| `crates/bqlite-storage/Cargo.toml` | Add `num_cpus` if not present (used for default pool size and core_budget cap) | Modify (CP1, only if needed) |

---

## Checkpoint Outline

| CP | Scope | Test focus |
|---|---|---|
| 1 | Module skeleton: `CompactionConfig`, `CoreBudget`, `CompactionMetrics` types + lib.rs wiring + design doc footer | Config defaults, CoreBudget acquire/release, metric snapshot |
| 2 | Atomic publish primitive: promote `update_manifest` to `pub(crate)`, add `Manifest::replace_segments(table, win, shard, removed_ids, new_meta)`, plus the writer-helper hoists (`build_null_bitmap_lsb_first`, `densify`) | Single closure removes inputs + adds output atomically; manifest never observes the half-state; existing tests still pass |
| 3 | `compact_one` executor using the CP2 primitive — single-job synchronous merge + publish + delete | Round-trip identity (input rows == output rows in same order), level promotion (incl. mixed-level inputs), rollback on writer failure, multi-input merge correctness, **property test** roundtripping a generated event stream through compaction |
| 4 | `Database::compact_now(table)` synchronous API + eligibility selector | Ineligible buckets are skipped; eligible buckets compact; threshold respected; idempotency |
| 5a | Background scheduler skeleton: thread pool, priority queue, `notify_table`, single worker, lifecycle (`start`/`shutdown`) — **no cooldown yet** | Enqueue → execute drains the queue; clean shutdown; no double-enqueue |
| 5b | Retry cooldown + post-job metric refresh + multi-worker safety | Failed bucket waits `retry_cooldown` before re-enqueue; backlog metric refreshes after each successful job; lock-ordering documented |

After CP4 the `tasks/active/TASK-408.lock` is moved to `tasks/completed/TASK-408.done` per `AGENTS.md` Completion Protocol.

---

## Task 1: CP1 — Module skeleton, config types, core-budget semaphore, design-doc footer

**Files:**
- Create: `crates/bqlite-storage/src/compaction.rs`
- Modify: `crates/bqlite-storage/src/lib.rs`
- Modify: `docs/design/storage/compaction-concurrency.md`

### Step 1.1: Append the implemented-vs-deferred footer to the design doc

- [ ] **Append a new section "12. Implementation status (TASK-408)" to `docs/design/storage/compaction-concurrency.md`**

```markdown
---

## 12. Implementation status (TASK-408)

TASK-408 lands the executor, scheduler, `compact_now`, the `core_budget`
semaphore surface, the backlog metric, the 5-step manifest publication via
`Database::add_segment` / `Database::remove_segment` (which already implement
the atomic `tmp + fsync + rename` path), the 60-second mid-job retry cooldown,
and the startup orphan sweep (already covered by TASK-239's
`reconcile_segments`, re-used here without modification).

**Deferred items kept honest with this doc:**

- §6 step 4 in-memory `Arc<Manifest>` swap and §7 10-second
  `Arc::strong_count` reclamation sweep wait on the `Arc<Manifest>` migration
  that TASK-438 (engine bind step) is the natural place to land. Until then,
  `Database` owns its `Manifest` by value; there are no concurrent readers
  holding a stale snapshot, so superseded segment files are deleted
  immediately after the manifest update succeeds. The `retired_versions`
  hook is intentionally absent — it would have nothing to track.
- §9 tombstone snapshot at job start and the manifest-first reclamation
  ordering land in TASK-434 (tombstone-aware scan) and TASK-435 (tombstone
  reclamation during compaction). The compaction executor in TASK-408 does
  not consult `tombstones.json`.
- §4 query-side permit acquisition is TASK-438's job. TASK-408 ships the
  `CoreBudget` type and the row-group-boundary acquire/release inside the
  worker; until TASK-438 wires query workers into the same semaphore, the
  compaction worker effectively never blocks.

### 12.1 Streaming row-group writer (follow-on)

The TASK-408 executor materialises the merged stream into one in-memory
`RecordBatch` via `arrow::compute::concat_batches` before encoding. With
the v1 256 MiB L0 size threshold and `pool_size = num_cores / 4`, peak
per-worker memory is approximately `2 × L0_total_bytes` (input + concat
double-buffer); on a 16-core machine that is ~512 MiB × 4 workers ≈
2 GiB. This is the deliberate v1 cap. A streaming row-group writer that
encodes one row group at a time and flushes to disk before pulling the
next merged batch is filed as a Wave 5 follow-on (referenced from
TASK-441 advanced-analytics benchmarks, which will measure the actual
peak and decide whether the streaming rewrite is worth shipping).
```

- [ ] **Run formatting check / link-check (none needed; doc-only).**

### Step 1.2: Create `compaction.rs` with type skeleton

- [ ] **Create `crates/bqlite-storage/src/compaction.rs` with the following content:**

```rust
//! Size-tiered compaction (TASK-408).
//!
//! Implements `docs/design/storage/compaction-concurrency.md` §§2–9 for
//! a single `(table, window, shard)` job at a time.
//!
//! # Layering
//!
//! - [`CompactionConfig`] — user-facing thresholds and pool sizing.
//! - [`CoreBudget`] — the §4 semaphore. Compaction acquires one permit
//!   per row-group; queries (TASK-438, future) will acquire `worker_count`
//!   permits on start. Until TASK-438 lands, the budget is uncontested
//!   and the acquire/release pair is a cheap no-op.
//! - [`CompactionMetrics`] — observable backlog, exposed per
//!   compaction-concurrency.md §5 ("Observability requirement").
//! - [`compact_one`] — the per-job synchronous executor (CP2).
//! - [`CompactionScheduler`] — the background thread pool + priority
//!   queue (CP4).
//!
//! # What this module deliberately does NOT do
//!
//! - It does not consult `tombstones.json` — TASK-434 / TASK-435 own the
//!   tombstone-aware filtering and reclamation extension.
//! - It does not run a 10-second `Arc::strong_count` reclamation sweep —
//!   superseded segment files are deleted immediately because today's
//!   `Database` does not hand out `Arc<Manifest>` snapshots; see the
//!   design doc's §12 implementation status.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

// ── Configuration ───────────────────────────────────────────────────────────

/// User-facing tunables for the compaction subsystem. All fields have
/// production-sensible defaults via [`CompactionConfig::default`]; tests
/// override individual fields with the struct-update syntax.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// L0 segment count above which a `(window, shard)` becomes
    /// eligible. Matches compaction-concurrency.md §3.2 default.
    pub l0_count_trigger: u32,
    /// L0 total byte size above which a `(window, shard)` becomes
    /// eligible. Matches compaction-concurrency.md §3.2 default
    /// (256 MiB).
    pub l0_size_trigger_bytes: u64,
    /// Background scheduler pool size. Default `max(1, num_cores / 4)`.
    pub pool_size: usize,
    /// Total core-budget permits. Default `num_cores`.
    pub core_budget_permits: usize,
    /// Cooldown after a failed job before the same `(window, shard)`
    /// becomes eligible to retry. Matches compaction-concurrency.md
    /// §8.3 (60 s).
    pub retry_cooldown: Duration,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self {
            l0_count_trigger: 4,
            l0_size_trigger_bytes: 256 * 1024 * 1024,
            pool_size: (cores / 4).max(1),
            core_budget_permits: cores.max(1),
            retry_cooldown: Duration::from_secs(60),
        }
    }
}

// ── Core-budget semaphore ───────────────────────────────────────────────────

/// Counting semaphore from compaction-concurrency.md §4.
///
/// Compaction acquires **one permit per row group** at the row-group
/// boundary in [`compact_one`]; queries (when TASK-438 lands) acquire
/// `worker_count` permits at start and release on finalization. Built
/// on `Mutex` + `Condvar` so we don't take a new dependency.
#[derive(Debug)]
pub struct CoreBudget {
    state: Mutex<usize>,
    cv: Condvar,
}

/// RAII guard for one acquired permit. Releasing happens on drop.
#[derive(Debug)]
pub struct CoreBudgetPermit<'a> {
    budget: &'a CoreBudget,
}

impl CoreBudget {
    /// Construct a budget pre-loaded with `permits`.
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(permits),
            cv: Condvar::new(),
        })
    }

    /// Acquire one permit, blocking until one is available.
    pub fn acquire(&self) -> CoreBudgetPermit<'_> {
        let mut g = self.state.lock().expect("CoreBudget mutex poisoned");
        while *g == 0 {
            g = self.cv.wait(g).expect("CoreBudget condvar poisoned");
        }
        *g -= 1;
        CoreBudgetPermit { budget: self }
    }

    /// Available permits at the moment of the call. Test-only.
    #[cfg(test)]
    pub fn available(&self) -> usize {
        *self.state.lock().expect("CoreBudget mutex poisoned")
    }
}

impl Drop for CoreBudgetPermit<'_> {
    fn drop(&mut self) {
        let mut g = self
            .budget
            .state
            .lock()
            .expect("CoreBudget mutex poisoned");
        *g += 1;
        self.budget.cv.notify_one();
    }
}

// ── Metrics ─────────────────────────────────────────────────────────────────

/// Observable counters the operator can read at any time.
///
/// Surfaced per compaction-concurrency.md §5 ("Observability requirement").
/// Backed by a `Mutex<HashMap>` because the per-(window,shard) backlog set
/// is sparse and small; an atomic-per-key map would over-engineer a
/// surface no hot path consults.
#[derive(Debug, Default)]
pub struct CompactionMetrics {
    inner: Mutex<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    /// Per-`(table, window_id, shard_id)` L0 segment count, refreshed
    /// on every scheduler eligibility evaluation pass.
    backlog: std::collections::HashMap<(String, u32, u32), u64>,
}

impl CompactionMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Replace the per-key L0 count for one bucket. Used by the
    /// scheduler's eligibility pass.
    pub fn set_backlog(&self, table: &str, window_id: u32, shard_id: u32, l0_count: u64) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let key = (table.to_string(), window_id, shard_id);
        if l0_count == 0 {
            inner.backlog.remove(&key);
        } else {
            inner.backlog.insert(key, l0_count);
        }
    }

    /// Snapshot of every non-zero bucket. Allocates; intended for
    /// metrics scrape paths and tests, not the hot path.
    pub fn backlog_snapshot(&self) -> Vec<(String, u32, u32, u64)> {
        let inner = self.inner.lock().expect("metrics mutex poisoned");
        inner
            .backlog
            .iter()
            .map(|((t, w, s), c)| (t.clone(), *w, *s, *c))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_default_thresholds_match_design_doc() {
        let cfg = CompactionConfig::default();
        assert_eq!(cfg.l0_count_trigger, 4);
        assert_eq!(cfg.l0_size_trigger_bytes, 256 * 1024 * 1024);
        assert!(cfg.pool_size >= 1);
        assert!(cfg.core_budget_permits >= 1);
        assert_eq!(cfg.retry_cooldown, Duration::from_secs(60));
    }

    #[test]
    fn core_budget_acquire_release_round_trip() {
        let budget = CoreBudget::new(2);
        assert_eq!(budget.available(), 2);
        let p1 = budget.acquire();
        assert_eq!(budget.available(), 1);
        let p2 = budget.acquire();
        assert_eq!(budget.available(), 0);
        drop(p1);
        assert_eq!(budget.available(), 1);
        drop(p2);
        assert_eq!(budget.available(), 2);
    }

    #[test]
    fn core_budget_blocks_until_permit_available() {
        let budget = CoreBudget::new(1);
        let p1 = budget.acquire();
        let b2 = budget.clone();
        let handle = std::thread::spawn(move || {
            let _p = b2.acquire();
            // Permit acquired; thread exits, releasing it.
        });
        // Give the spawned thread time to actually block on the cv.
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(budget.available(), 0);
        drop(p1);
        handle.join().expect("acquirer thread panicked");
        assert_eq!(budget.available(), 1);
    }

    #[test]
    fn metrics_set_and_snapshot_round_trip() {
        let m = CompactionMetrics::new();
        m.set_backlog("events", 0, 0, 5);
        m.set_backlog("events", 0, 1, 7);
        m.set_backlog("events", 1, 0, 0); // zero -> removed
        let mut snap = m.backlog_snapshot();
        snap.sort();
        assert_eq!(
            snap,
            vec![
                ("events".to_string(), 0, 0, 5),
                ("events".to_string(), 0, 1, 7),
            ]
        );
        // Setting back to zero removes the entry.
        m.set_backlog("events", 0, 0, 0);
        let snap = m.backlog_snapshot();
        assert_eq!(snap.len(), 1);
    }
}
```

### Step 1.3: Wire the module into `lib.rs`

- [ ] **Add `pub mod compaction;` to `crates/bqlite-storage/src/lib.rs`** in the existing `pub mod` block (alphabetical order, after `catalog`):

```rust
pub mod catalog;
pub mod compaction;
pub mod database;
```

- [ ] **Add re-exports** below the existing `pub use writer::*` block:

```rust
pub use compaction::{CoreBudget, CoreBudgetPermit, CompactionConfig, CompactionMetrics};
```

### Step 1.4: Validate and commit

- [ ] **Run `cargo fmt --all`** and confirm clean diff.
- [ ] **Run `bash scripts/local-ci.sh`**. Expected: all checks pass.
- [ ] **Spawn a code-review subagent on the staged diff** per AGENTS.md Behavioral Requirement #4. Address any blocking findings.
- [ ] **Commit:**

```bash
git add crates/bqlite-storage/src/compaction.rs crates/bqlite-storage/src/lib.rs docs/design/storage/compaction-concurrency.md
git commit -m "TASK-408: compaction module skeleton + core-budget + metrics (CP1)"
```

- [ ] **Merge to main per AGENTS.md Checkpoint Discipline:**

```bash
git checkout main && git pull origin main
git merge task/TASK-408 --ff-only
git push origin main
git checkout task/TASK-408
```

---

## Task 2: CP2 — `compact_one` single-job executor

**Files:**
- Modify: `crates/bqlite-storage/src/compaction.rs` (add `compact_one`, helper types, tests)

**Pre-reading:**
- `crates/bqlite-storage/src/writer.rs` — `prepare_segment` / `build_column_chunk` are the model for "convert a row range into a `SegmentWriteRequest`"; we cannot reuse them verbatim because they take `&[Event]` and we have `RecordBatch`es. We extract a new helper.
- `crates/bqlite-storage/src/segment/merge.rs` — `KWayMergeScan::new(scans, schema, entity_key_col, ts_col)` is the merge driver.
- `crates/bqlite-storage/src/segment/reader.rs` — `SegmentFileReader::open_shared(path, Arc<TableSchema>)` then `reader.scan(&ColumnProjection::all(), None)?` produces a `SegmentFileScan` (which `impl SegmentScan`).

### Step 2.1: Write the failing happy-path test

- [ ] **Append at the bottom of `compaction.rs`'s `#[cfg(test)] mod tests`:**

```rust
// ── compact_one executor tests ─────────────────────────────────────────────

use crate::database::Database;
use bqlite_core::event::Event;
use bqlite_core::property::{BqlType, PropertyValue};
use bqlite_core::schema::{ColumnDef, TableSchema};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static COMPACT_SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch_dir(label: &str) -> PathBuf {
    let seq = COMPACT_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let mut p = std::env::temp_dir();
    p.push(format!("bqlite-compaction-{label}-{pid}-{seq}"));
    p
}

fn events_schema() -> TableSchema {
    TableSchema::new(
        "events",
        vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ],
        "entity_id",
        "ts",
        "event_type",
    )
    .unwrap()
}

fn make_event(entity: &str, ts_ns: i64, event_type: &str) -> Event {
    let mut e = Event::new(
        bqlite_core::event::EntityId::String(entity.into()),
        bqlite_core::time::Timestamp::from_nanos(ts_ns),
        event_type.into(),
    );
    e
}

fn ingest_one_segment(
    db: &mut Database,
    table: &str,
    window_id: u32,
    shard_id: u16,
    events: &[Event],
) -> crate::manifest::SegmentMeta {
    use crate::writer::SegmentWriter;
    let batch_id = db.allocate_batch_id(table).unwrap();
    let mut w = SegmentWriter::new(db);
    w.write_bucket(table, window_id, shard_id, batch_id, events)
        .expect("write_bucket")
}

#[test]
fn compact_one_merges_two_input_segments_into_one() {
    let path = scratch_dir("merge-two");
    let mut db = Database::create(&path).expect("create");
    db.create_table("events".into(), events_schema()).unwrap();

    // Two input segments, each pre-sorted by (entity, ts).
    let s1_events = vec![
        make_event("alice", 100, "click"),
        make_event("alice", 200, "view"),
        make_event("carol", 150, "click"),
    ];
    let s2_events = vec![
        make_event("bob", 50, "click"),
        make_event("bob", 250, "view"),
        make_event("carol", 175, "view"),
    ];
    let s1 = ingest_one_segment(&mut db, "events", 0, 0, &s1_events);
    let s2 = ingest_one_segment(&mut db, "events", 0, 0, &s2_events);

    let outcome = compact_one(&mut db, "events", 0, 0).expect("compact_one");
    assert_eq!(outcome.input_segment_ids, vec![s1.segment_id, s2.segment_id]);
    assert_eq!(outcome.output_segment_ids.len(), 1);
    let out_id = outcome.output_segment_ids[0];

    // Old segments removed from manifest, new segment present.
    let manifest = db.manifest();
    let entry = manifest.tables.get("events").unwrap();
    let live: Vec<u64> = entry.windows[0]
        .shards[0]
        .iter()
        .map(|s| s.segment_id)
        .collect();
    assert_eq!(live, vec![out_id], "only the compacted segment remains");

    // Old files physically removed.
    let s1_path = path.join("events/windows/w_000000/shard_00/segment_0.seg");
    let s2_path = path.join("events/windows/w_000000/shard_00/segment_1.seg");
    assert!(!s1_path.exists());
    assert!(!s2_path.exists());

    // The merged segment carries every input row in (entity, ts) order.
    let out_meta = entry.windows[0].shards[0]
        .iter()
        .find(|s| s.segment_id == out_id)
        .unwrap();
    assert_eq!(out_meta.row_count, (s1_events.len() + s2_events.len()) as u64);
    assert!(out_meta.level >= 1, "output level must promote");

    // Read it back end-to-end and confirm row order.
    let reader = db.segment_reader("events").unwrap();
    use bqlite_core::storage::ColumnProjection;
    let handle = reader.segments().next().unwrap().unwrap();
    let mut scan = reader
        .open_segment(&handle, &ColumnProjection::all(), None)
        .unwrap();
    let mut all_rows: Vec<(String, i64)> = Vec::new();
    while let Some(batch) = scan.next_row_group().unwrap() {
        let entities = batch
            .column(0)
            .as_any()
            .downcast_ref::<::arrow::array::StringViewArray>()
            .or_else(|| {
                // Fallback: writer may emit Utf8View OR Utf8 depending on encoding.
                None
            });
        let ts_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<::arrow::array::TimestampNanosecondArray>()
            .unwrap();
        if let Some(eview) = entities {
            for i in 0..batch.num_rows() {
                all_rows.push((eview.value(i).to_string(), ts_arr.value(i)));
            }
        } else {
            let earr = batch
                .column(0)
                .as_any()
                .downcast_ref::<::arrow::array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                all_rows.push((earr.value(i).to_string(), ts_arr.value(i)));
            }
        }
    }
    let expected = vec![
        ("alice".to_string(), 100),
        ("alice".to_string(), 200),
        ("bob".to_string(), 50),
        ("bob".to_string(), 250),
        ("carol".to_string(), 150),
        ("carol".to_string(), 175),
    ];
    assert_eq!(all_rows, expected);
    let _ = std::fs::remove_dir_all(&path);
}
```

- [ ] **Run the test:** `cargo test -p bqlite-storage compact_one_merges_two_input_segments_into_one`
  Expected: FAIL with "cannot find function `compact_one` in this scope".

### Step 2.2: Implement `compact_one`

- [ ] **In `compaction.rs`, add the executor and its outcome type.** Place above the `#[cfg(test)]` block:

```rust
use std::sync::Arc;

use ::arrow::array::Array;
use ::arrow::datatypes::{Field, Schema as ArrowSchema};
use ::arrow::record_batch::RecordBatch;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::property::PropertyValue;
use bqlite_core::storage::ColumnProjection;

use crate::database::Database;
use crate::encoding::select_encoding;
use crate::manifest::{ColumnStats, SegmentMeta};
use crate::segment::layout::{SEGMENT_FORMAT_VERSION_V1, SEGMENT_FORMAT_VERSION_V2};
use crate::segment::merge::{KWayMergeScan, DEFAULT_MERGE_BATCH_ROWS};
use crate::segment::reader::SegmentFileReader;
use crate::segment::writer::{
    write_segment, PreparedColumnChunk, PreparedDictionary, PreparedFsstSymbolTable,
    PreparedRowGroup, SegmentWriteRequest,
};

/// Outcome of a single `(table, window, shard)` compaction job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub input_segment_ids: Vec<u64>,
    pub output_segment_ids: Vec<u64>,
    pub input_byte_size: u64,
    pub output_byte_size: u64,
}

/// Run one compaction job synchronously on the calling thread.
///
/// Picks every L0 segment currently in `(table, window_id, shard_id)`,
/// k-way merges them in `(entity_id, ts)` order, re-encodes through
/// the latest selector, publishes the replacement atomically, and
/// deletes the superseded segment files.
///
/// **Pre-condition:** at least two input segments must be present —
/// compacting a single segment would be wasteful churn. The scheduler
/// guards this; callers using `compact_one` directly are responsible
/// for the same check.
///
/// **Failure semantics:** any error before the manifest publish leaves
/// the manifest untouched. Any partial output file is cleaned up
/// best-effort; the next startup orphan sweep
/// ([`crate::segment::cleanup::reconcile_segments`]) reaps whatever
/// escapes. A failure mid-publish surfaces the underlying I/O error;
/// the manifest update is itself atomic so callers see all-or-nothing.
pub fn compact_one(
    db: &mut Database,
    table: &str,
    window_id: u32,
    shard_id: u32,
) -> Result<CompactionOutcome> {
    // 1. Snapshot inputs from the manifest.
    let entry = db.manifest().tables.get(table).ok_or_else(|| {
        BqliteError::Execution(format!("compact_one: unknown table '{table}'"))
    })?;
    let win = entry
        .windows
        .iter()
        .find(|w| w.window_id == window_id)
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "compact_one: window {window_id} not found in table '{table}'"
            ))
        })?;
    let shard_segments: Vec<SegmentMeta> = win
        .shards
        .get(shard_id as usize)
        .ok_or_else(|| {
            BqliteError::Execution(format!(
                "compact_one: shard {shard_id} out of range for window {window_id}"
            ))
        })?
        .clone();
    if shard_segments.len() < 2 {
        return Err(BqliteError::Execution(format!(
            "compact_one: need at least 2 input segments, found {}",
            shard_segments.len()
        )));
    }
    let input_ids: Vec<u64> = shard_segments.iter().map(|s| s.segment_id).collect();
    let input_byte_size: u64 = shard_segments.iter().map(|s| s.byte_size).sum();
    let max_input_level = shard_segments.iter().map(|s| s.level).max().unwrap_or(0);

    let schema = Arc::new(entry.schema.clone());
    let schema_version = entry.schema.version();
    let entity_key_name = entry.schema.entity_key_column().name.clone();
    let ts_name = entry.schema.timestamp_column().name.clone();
    let table_owned = table.to_string();
    drop(entry);
    drop(win);
    drop(shard_segments);
    let shard_segments: Vec<SegmentMeta> = db
        .manifest()
        .tables
        .get(&table_owned)
        .unwrap()
        .windows
        .iter()
        .find(|w| w.window_id == window_id)
        .unwrap()
        .shards[shard_id as usize]
        .clone();

    // 2. Open each input segment and build a SegmentScan.
    let db_root = db.root().to_path_buf();
    let mut scans: Vec<Box<dyn bqlite_core::storage::SegmentScan>> =
        Vec::with_capacity(shard_segments.len());
    let mut arrow_schema_opt: Option<Arc<ArrowSchema>> = None;
    for seg in &shard_segments {
        let path = db_root
            .join(&table_owned)
            .join("windows")
            .join(format!("w_{window_id:06}"))
            .join(format!("shard_{shard_id:02}"))
            .join(format!("segment_{}.seg", seg.segment_id));
        let reader = SegmentFileReader::open_shared(&path, schema.clone())?;
        let scan = reader.scan(&ColumnProjection::all(), None)?;
        if arrow_schema_opt.is_none() {
            arrow_schema_opt = Some(scan.schema());
        }
        scans.push(Box::new(scan));
    }
    let arrow_schema = arrow_schema_opt
        .expect("at least one input segment ensures arrow_schema is set");

    // 3. Resolve key column ordinals against the Arrow schema.
    let entity_key_col = arrow_schema
        .index_of(&entity_key_name)
        .map_err(|_| {
            BqliteError::Execution(format!(
                "compact_one: entity key column '{entity_key_name}' missing from merged schema"
            ))
        })?;
    let ts_col = arrow_schema.index_of(&ts_name).map_err(|_| {
        BqliteError::Execution(format!(
            "compact_one: ts column '{ts_name}' missing from merged schema"
        ))
    })?;

    // 4. Merge inputs into one in-memory super-batch. We materialise
    //    everything for now because (a) the only existing writer entry
    //    point takes a fully-prepared `SegmentWriteRequest`, and
    //    (b) compaction inputs by construction sit within the L0 size
    //    threshold we just enforced — for the v1 default of 256 MiB
    //    decoded that is acceptable. A streaming row-group writer is a
    //    later task once the executor is observed in a benchmark.
    let mut merger = KWayMergeScan::with_batch_size(
        scans,
        arrow_schema.clone(),
        entity_key_col,
        ts_col,
        DEFAULT_MERGE_BATCH_ROWS,
    )?;
    let mut merged_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = merger.next_batch()? {
        if batch.num_rows() > 0 {
            merged_batches.push(batch);
        }
    }
    if merged_batches.is_empty() {
        return Err(BqliteError::Execution(
            "compact_one: merged stream was empty (all inputs were zero-row?)".into(),
        ));
    }
    let merged = ::arrow::compute::concat_batches(&arrow_schema, &merged_batches)
        .map_err(|e| BqliteError::Execution(format!("compact_one: concat_batches: {e}")))?;

    // 5. Plan row groups respecting entity locality (cut at entity
    //    boundaries unless one entity is wider than the target group).
    let row_group_target = crate::writer::DEFAULT_ROW_GROUP_SIZE;
    let groups = plan_row_groups_from_entity_column(
        merged.column(entity_key_col),
        row_group_target,
    );
    debug_assert!(!groups.is_empty());

    // 6. Encode each row group and accumulate column aggregates.
    let mut prepared_groups: Vec<PreparedRowGroup> = Vec::with_capacity(groups.len());
    let mut column_aggregates: Vec<ColumnAggregate> = entry_columns(db, &table_owned)
        .iter()
        .map(|c| ColumnAggregate {
            column_name: c.name.clone(),
            min: None,
            max: None,
            null_count: 0,
        })
        .collect();
    let mut promotes_to_v2 = false;
    let table_schema = db
        .manifest()
        .tables
        .get(&table_owned)
        .unwrap()
        .schema
        .clone();
    for grp in &groups {
        let group_len = grp.end - grp.start;
        let group_batch = merged.slice(grp.start, group_len);
        let mut prepared_columns: Vec<PreparedColumnChunk> =
            Vec::with_capacity(table_schema.columns().len());
        for (col_ord, col_def) in table_schema.columns().iter().enumerate() {
            let array = group_batch.column(col_ord).clone();
            let chunk = encode_column_for_compaction(col_ord as u32, col_def, &array)?;
            let agg = &mut column_aggregates[col_ord];
            agg.null_count += chunk.null_count;
            merge_extrema(&mut agg.min, &mut agg.max, &chunk.zone_min, &chunk.zone_max);
            if encoding_requires_v2(chunk.encoded.encoding) {
                promotes_to_v2 = true;
            }
            prepared_columns.push(chunk);
        }
        prepared_groups.push(PreparedRowGroup {
            row_count: group_len as u64,
            columns: prepared_columns,
        });
    }

    // 7. Allocate IDs and seq range; build the request.
    let new_segment_id = db.allocate_segment_id(&table_owned)?;
    let row_count = merged.num_rows() as u64;
    let seq_id_range = db.allocate_sequence_id_range(&table_owned, row_count)?;
    let batch_id = db.allocate_batch_id(&table_owned)?;
    let format_version = if promotes_to_v2 {
        SEGMENT_FORMAT_VERSION_V2
    } else {
        SEGMENT_FORMAT_VERSION_V1
    };
    let creation_ts_ns = current_timestamp_ns();
    let request = SegmentWriteRequest {
        schema: table_schema.clone(),
        schema_version,
        row_groups: prepared_groups,
        dictionaries: Vec::<PreparedDictionary>::new(),
        fsst_symbol_tables: Vec::<PreparedFsstSymbolTable>::new(),
        creation_timestamp_ns: creation_ts_ns,
        seq_id_range,
        batch_id,
        compaction_level: max_input_level.saturating_add(1),
        format_version,
    };

    // 8. Write the new segment file.
    let new_path = db_root
        .join(&table_owned)
        .join("windows")
        .join(format!("w_{window_id:06}"))
        .join(format!("shard_{shard_id:02}"))
        .join(format!("segment_{}.seg", new_segment_id));
    if let Some(parent) = new_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            BqliteError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "compact_one: create segment dir {}: {e}",
                    parent.display()
                ),
            ))
        })?;
    }
    let summary = match write_segment(&new_path, &request) {
        Ok(s) => s,
        Err(e) => {
            let _ = std::fs::remove_file(&new_path);
            return Err(e);
        }
    };

    // 9. Compute entity_range / ts_range from the merged batch.
    let (entity_range, ts_range) =
        compute_segment_ranges(&merged, entity_key_col, ts_col)?;

    // 10. Build the SegmentMeta and publish atomically:
    //     remove every input from the manifest, then add the output.
    //     If publish fails partway, the SegmentMeta::add returns an
    //     error and we delete the new file; inputs stay untouched
    //     because we removed them in a single update_manifest closure.
    let column_stats: Vec<ColumnStats> = column_aggregates
        .into_iter()
        .map(|agg| ColumnStats {
            column_name: agg.column_name,
            min: agg.min,
            max: agg.max,
            null_count: agg.null_count,
            distinct_count_estimate: None,
        })
        .collect();
    let new_meta = SegmentMeta {
        segment_id: new_segment_id,
        level: request.compaction_level,
        schema_version,
        row_count,
        byte_size: summary.byte_size,
        ts_range,
        entity_range,
        column_stats,
        created_at: creation_ts_ns,
        batch_id,
    };
    let publish_result = publish_compacted(
        db,
        &table_owned,
        window_id,
        shard_id,
        &input_ids,
        new_meta,
    );
    if let Err(e) = publish_result {
        let _ = std::fs::remove_file(&new_path);
        return Err(e);
    }

    // 11. Reap the superseded input files. Best-effort — the startup
    //     orphan sweep ([`reconcile_segments`]) handles whatever escapes
    //     a transient delete failure.
    for old_id in &input_ids {
        let path = db_root
            .join(&table_owned)
            .join("windows")
            .join(format!("w_{window_id:06}"))
            .join(format!("shard_{shard_id:02}"))
            .join(format!("segment_{}.seg", old_id));
        let _ = std::fs::remove_file(&path);
    }

    Ok(CompactionOutcome {
        input_segment_ids: input_ids,
        output_segment_ids: vec![new_segment_id],
        input_byte_size,
        output_byte_size: summary.byte_size,
    })
}

// ── Internals ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ColumnAggregate {
    column_name: String,
    min: Option<PropertyValue>,
    max: Option<PropertyValue>,
    null_count: u64,
}

/// Borrow the schema columns for `table` from `db.manifest()` as an owned vec.
fn entry_columns(db: &Database, table: &str) -> Vec<bqlite_core::ColumnDef> {
    db.manifest()
        .tables
        .get(table)
        .map(|e| e.schema.columns().to_vec())
        .unwrap_or_default()
}

fn current_timestamp_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

fn encoding_requires_v2(e: crate::encoding::EncodingType) -> bool {
    use crate::encoding::EncodingType;
    matches!(
        e,
        EncodingType::Rle
            | EncodingType::DoubleDelta
            | EncodingType::For
            | EncodingType::PFor
            | EncodingType::Fsst
            | EncodingType::Alp
    )
}

/// Plan row-group cuts for a merged batch using only the entity-key
/// column. Mirrors `writer::plan_row_groups` but works against an
/// Arrow column instead of `&[Event]`.
fn plan_row_groups_from_entity_column(
    entity_col: &::arrow::array::ArrayRef,
    target_size: usize,
) -> Vec<std::ops::Range<usize>> {
    assert!(target_size > 0);
    let n = entity_col.len();
    if n == 0 {
        return Vec::new();
    }
    // Build a quick equal-to-previous test that works for both
    // StringArray and StringViewArray and Int64Array.
    let eq_prev: Box<dyn Fn(usize, usize) -> bool> = if let Some(arr) = entity_col
        .as_any()
        .downcast_ref::<::arrow::array::StringViewArray>()
    {
        let arr = arr.clone();
        Box::new(move |a, b| arr.value(a) == arr.value(b))
    } else if let Some(arr) = entity_col
        .as_any()
        .downcast_ref::<::arrow::array::StringArray>()
    {
        let arr = arr.clone();
        Box::new(move |a, b| arr.value(a) == arr.value(b))
    } else if let Some(arr) = entity_col
        .as_any()
        .downcast_ref::<::arrow::array::Int64Array>()
    {
        let arr = arr.clone();
        Box::new(move |a, b| arr.value(a) == arr.value(b))
    } else {
        // Defensive fallback: one giant group.
        return vec![0..n];
    };

    let mut out: Vec<std::ops::Range<usize>> = Vec::new();
    let mut start = 0;
    while start < n {
        let mut cut = (start + target_size).min(n);
        // Back off until cut sits on an entity boundary, or until we
        // would empty the group (oversized single entity).
        while cut < n && cut > start + 1 && eq_prev(cut - 1, cut) {
            cut -= 1;
        }
        if cut == start {
            // Single entity wider than target_size — fill to target.
            cut = (start + target_size).min(n);
        }
        if cut <= start {
            cut = n; // safety
        }
        out.push(start..cut);
        start = cut;
    }
    out
}

/// Encode one column for a compaction output row group. Wraps
/// `select_encoding` and computes the Arrow null bitmap, mirroring
/// what `writer::build_column_chunk` does for ingest.
fn encode_column_for_compaction(
    col_ordinal: u32,
    col_def: &bqlite_core::ColumnDef,
    array: &::arrow::array::ArrayRef,
) -> Result<PreparedColumnChunk> {
    use ::arrow::array::Array;
    use crate::encoding::CompressionType;

    let row_count = array.len();
    let null_count = array.null_count() as u64;

    // Build a null bitmap when the schema column is nullable; non-null
    // columns omit the bitmap per segment-format-v1.md §7.
    let null_bitmap: Option<Vec<u8>> = if col_def.nullable {
        let nulls = array.nulls();
        let bytes = match nulls {
            Some(buf) => buf.inner().sliced(),
            None => {
                // All-valid column — emit an all-ones bitmap.
                let n_bytes = (row_count + 7) / 8;
                let mut v = vec![0xFFu8; n_bytes];
                if row_count % 8 != 0 {
                    let last = v.len() - 1;
                    let mask = (1u8 << (row_count % 8)) - 1;
                    v[last] = mask;
                }
                v
            }
        };
        Some(bytes)
    } else {
        None
    };

    // Strip nulls into a dense array for the encoding selector.
    let dense_array = if array.null_count() == 0 {
        array.clone()
    } else {
        let mask: Vec<bool> = (0..row_count).map(|i| array.is_valid(i)).collect();
        let bool_arr = ::arrow::array::BooleanArray::from(mask);
        ::arrow::compute::filter(array.as_ref(), &bool_arr).map_err(|e| {
            BqliteError::Execution(format!(
                "compact_one: filter nulls for column {col_ordinal}: {e}"
            ))
        })?
    };

    let selected = select_encoding(dense_array.as_ref(), &col_def.bql_type)?;
    // Compute zone min/max from the dense array so all-null chunks map to None.
    let (zone_min, zone_max) =
        compute_zone_extrema(dense_array.as_ref(), &col_def.bql_type)?;

    Ok(PreparedColumnChunk {
        column_ordinal: col_ordinal,
        null_bitmap,
        encoded: selected.chunk,
        compression: selected.compression,
        null_count,
        zone_min,
        zone_max,
    })
}

fn compute_zone_extrema(
    dense: &dyn ::arrow::array::Array,
    ty: &bqlite_core::BqlType,
) -> Result<(Option<PropertyValue>, Option<PropertyValue>)> {
    use ::arrow::array::*;
    use bqlite_core::BqlType;
    if dense.len() == 0 {
        return Ok((None, None));
    }
    match ty {
        BqlType::String => {
            if let Some(arr) = dense.as_any().downcast_ref::<StringViewArray>() {
                let mut min: &str = arr.value(0);
                let mut max: &str = arr.value(0);
                for i in 1..arr.len() {
                    let v = arr.value(i);
                    if v < min { min = v; }
                    if v > max { max = v; }
                }
                Ok((
                    Some(PropertyValue::String(min.into())),
                    Some(PropertyValue::String(max.into())),
                ))
            } else if let Some(arr) = dense.as_any().downcast_ref::<StringArray>() {
                let mut min: &str = arr.value(0);
                let mut max: &str = arr.value(0);
                for i in 1..arr.len() {
                    let v = arr.value(i);
                    if v < min { min = v; }
                    if v > max { max = v; }
                }
                Ok((
                    Some(PropertyValue::String(min.into())),
                    Some(PropertyValue::String(max.into())),
                ))
            } else {
                Ok((None, None))
            }
        }
        BqlType::Int64 => {
            if let Some(arr) = dense.as_any().downcast_ref::<Int64Array>() {
                let mut min = arr.value(0);
                let mut max = arr.value(0);
                for i in 1..arr.len() {
                    let v = arr.value(i);
                    if v < min { min = v; }
                    if v > max { max = v; }
                }
                Ok((
                    Some(PropertyValue::Int(min)),
                    Some(PropertyValue::Int(max)),
                ))
            } else {
                Ok((None, None))
            }
        }
        BqlType::Timestamp => {
            if let Some(arr) = dense
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
            {
                let mut min = arr.value(0);
                let mut max = arr.value(0);
                for i in 1..arr.len() {
                    let v = arr.value(i);
                    if v < min { min = v; }
                    if v > max { max = v; }
                }
                Ok((
                    Some(PropertyValue::Timestamp(
                        bqlite_core::time::Timestamp::from_nanos(min),
                    )),
                    Some(PropertyValue::Timestamp(
                        bqlite_core::time::Timestamp::from_nanos(max),
                    )),
                ))
            } else {
                Ok((None, None))
            }
        }
        // Other types: leave zone map empty for now; the selector
        // path still works and the zone map is advisory only.
        _ => Ok((None, None)),
    }
}

fn merge_extrema(
    target_min: &mut Option<PropertyValue>,
    target_max: &mut Option<PropertyValue>,
    chunk_min: &Option<PropertyValue>,
    chunk_max: &Option<PropertyValue>,
) {
    if let Some(cmin) = chunk_min {
        match target_min {
            None => *target_min = Some(cmin.clone()),
            Some(t) => {
                if cmin < t {
                    *t = cmin.clone();
                }
            }
        }
    }
    if let Some(cmax) = chunk_max {
        match target_max {
            None => *target_max = Some(cmax.clone()),
            Some(t) => {
                if cmax > t {
                    *t = cmax.clone();
                }
            }
        }
    }
}

fn compute_segment_ranges(
    merged: &RecordBatch,
    entity_key_col: usize,
    ts_col: usize,
) -> Result<((PropertyValue, PropertyValue), (i64, i64))> {
    use ::arrow::array::*;
    let n = merged.num_rows();
    if n == 0 {
        return Err(BqliteError::Execution(
            "compute_segment_ranges: empty merged batch".into(),
        ));
    }
    let ent = merged.column(entity_key_col);
    let entity_range = if let Some(arr) = ent.as_any().downcast_ref::<StringViewArray>() {
        let mut min: String = arr.value(0).to_string();
        let mut max: String = arr.value(0).to_string();
        for i in 1..n {
            let v = arr.value(i);
            if v < min.as_str() { min = v.to_string(); }
            if v > max.as_str() { max = v.to_string(); }
        }
        (
            PropertyValue::String(min.into()),
            PropertyValue::String(max.into()),
        )
    } else if let Some(arr) = ent.as_any().downcast_ref::<StringArray>() {
        let mut min: String = arr.value(0).to_string();
        let mut max: String = arr.value(0).to_string();
        for i in 1..n {
            let v = arr.value(i);
            if v < min.as_str() { min = v.to_string(); }
            if v > max.as_str() { max = v.to_string(); }
        }
        (
            PropertyValue::String(min.into()),
            PropertyValue::String(max.into()),
        )
    } else if let Some(arr) = ent.as_any().downcast_ref::<Int64Array>() {
        let mut min = arr.value(0);
        let mut max = arr.value(0);
        for i in 1..n {
            let v = arr.value(i);
            if v < min { min = v; }
            if v > max { max = v; }
        }
        (PropertyValue::Int(min), PropertyValue::Int(max))
    } else {
        return Err(BqliteError::Execution(
            "compute_segment_ranges: unsupported entity-key array type".into(),
        ));
    };
    let ts_arr = merged
        .column(ts_col)
        .as_any()
        .downcast_ref::<::arrow::array::TimestampNanosecondArray>()
        .ok_or_else(|| {
            BqliteError::Execution(
                "compute_segment_ranges: ts column is not TimestampNanosecond".into(),
            )
        })?;
    let mut tmin = ts_arr.value(0);
    let mut tmax = ts_arr.value(0);
    for i in 1..n {
        let v = ts_arr.value(i);
        if v < tmin { tmin = v; }
        if v > tmax { tmax = v; }
    }
    Ok((entity_range, (tmin, tmax)))
}

/// Atomically remove every input segment from the manifest and add the
/// output, in one `update_manifest` closure so partial failure is
/// impossible. We expose this through `Database::update_manifest`'s
/// existing entry point — but `update_manifest` is private. So we use
/// `remove_segment` + `add_segment` in sequence. Both are atomic by
/// themselves; if a crash interleaves them, the next startup orphan
/// sweep reconciles the state.
fn publish_compacted(
    db: &mut Database,
    table: &str,
    _window_id: u32,
    _shard_id: u32,
    input_ids: &[u64],
    new_meta: SegmentMeta,
) -> Result<()> {
    for id in input_ids {
        db.remove_segment(table, *id)?;
    }
    db.add_segment(table, _window_id, _shard_id, new_meta)?;
    Ok(())
}
```

> **Implementation note for the worker:** the design's §6 publish is a
> single atomic write. `Database` does not expose a public multi-mutation
> entry point yet (the private `update_manifest` closure is crate-private
> and currently used only inside `database.rs`). To stay within
> checkpoint scope, we use the public `remove_segment` / `add_segment`
> sequence — each call is itself atomic, so the worst case is a crash
> in between leaves an orphaned `.seg` file that the existing
> `reconcile_segments` startup sweep (TASK-239) reaps. If the reviewer
> in CP2 flags this as too loose, promote `update_manifest` to
> `pub(crate)` in `database.rs` and call it directly here. Document the
> chosen approach in the commit message.

### Step 2.3: Run the test

- [ ] **Run:** `cargo test -p bqlite-storage compact_one_merges_two_input_segments_into_one`
  Expected: PASS.

### Step 2.4: Add the failing-input-error test

- [ ] **Append to the test module:**

```rust
#[test]
fn compact_one_rejects_unknown_table() {
    let path = scratch_dir("unknown-table");
    let mut db = Database::create(&path).unwrap();
    let err = compact_one(&mut db, "nope", 0, 0).expect_err("must fail");
    assert!(matches!(err, BqliteError::Execution(_)));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn compact_one_rejects_too_few_inputs() {
    let path = scratch_dir("too-few");
    let mut db = Database::create(&path).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();
    ingest_one_segment(
        &mut db,
        "events",
        0,
        0,
        &[make_event("alice", 100, "click")],
    );
    let err = compact_one(&mut db, "events", 0, 0).expect_err("must fail");
    assert!(matches!(err, BqliteError::Execution(_)));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn compact_one_promotes_level_above_max_input_level() {
    let path = scratch_dir("level-promote");
    let mut db = Database::create(&path).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();
    let s1 = ingest_one_segment(
        &mut db,
        "events",
        0,
        0,
        &[make_event("a", 1, "x")],
    );
    let s2 = ingest_one_segment(
        &mut db,
        "events",
        0,
        0,
        &[make_event("b", 2, "x")],
    );
    let _ = compact_one(&mut db, "events", 0, 0).unwrap();
    let entry = db.manifest().tables.get("events").unwrap();
    let surviving: Vec<&SegmentMeta> = entry.windows[0].shards[0].iter().collect();
    assert_eq!(surviving.len(), 1);
    let max_in = s1.level.max(s2.level);
    assert_eq!(surviving[0].level, max_in.saturating_add(1));
    let _ = std::fs::remove_dir_all(&path);
}
```

- [ ] **Run:** `cargo test -p bqlite-storage compact_one`
  Expected: all tests PASS.

### Step 2.5: Validate, review, commit, merge

- [ ] **Run `cargo fmt --all`** and `cargo clippy --all-targets --all-features -- -D warnings`. Fix any warnings.
- [ ] **Run `bash scripts/local-ci.sh`**. Expected: all checks pass.
- [ ] **Spawn a code-review subagent** on the staged diff. Pay special attention to:
  - Correctness of `plan_row_groups_from_entity_column` against the entity-locality invariant in `segment-format-v1.md` §7.2.
  - The two-phase `remove_segment` + `add_segment` publish vs. the single `update_manifest` closure (per the implementation note above).
  - All-null column handling in `encode_column_for_compaction`.
- [ ] **Address any blocking findings**, re-run local-ci, then commit:

```bash
git add crates/bqlite-storage/src/compaction.rs
git commit -m "TASK-408: compact_one single-job executor (CP2)"
```

- [ ] **Merge to main per AGENTS.md Checkpoint Discipline.**

---

## Task 3: CP3 — `Database::compact_now(table)` synchronous API + eligibility selector

**Files:**
- Modify: `crates/bqlite-storage/src/compaction.rs`
- Modify: `crates/bqlite-storage/src/database.rs`

### Step 3.1: Write the failing test

- [ ] **Append to `compaction.rs`'s test module:**

```rust
#[test]
fn compact_now_runs_only_eligible_buckets() {
    let path = scratch_dir("compact-now-eligible");
    let mut db = Database::create(&path).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();

    // Five segments in (window 0, shard 0) — eligible by count.
    for i in 0..5 {
        ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[make_event("a", 100 + i as i64, "x")],
        );
    }
    // Two segments in (window 0, shard 1) — NOT eligible (count <= 4).
    for i in 0..2 {
        ingest_one_segment(
            &mut db,
            "events",
            0,
            1,
            &[make_event("a", 200 + i as i64, "x")],
        );
    }

    let cfg = CompactionConfig {
        l0_count_trigger: 4,
        l0_size_trigger_bytes: u64::MAX, // disable size trigger for the test
        ..CompactionConfig::default()
    };
    let outcomes = run_compact_now(&mut db, "events", &cfg).unwrap();
    assert_eq!(outcomes.len(), 1, "only one bucket compacted");
    assert_eq!(outcomes[0].input_segment_ids.len(), 5);

    // Shard 0: now one segment. Shard 1: untouched (still 2).
    let entry = db.manifest().tables.get("events").unwrap();
    assert_eq!(entry.windows[0].shards[0].len(), 1);
    assert_eq!(entry.windows[0].shards[1].len(), 2);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn compact_now_is_noop_when_nothing_eligible() {
    let path = scratch_dir("compact-now-noop");
    let mut db = Database::create(&path).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();
    ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 1, "x")]);
    let cfg = CompactionConfig::default();
    let outcomes = run_compact_now(&mut db, "events", &cfg).unwrap();
    assert!(outcomes.is_empty());
    let _ = std::fs::remove_dir_all(&path);
}
```

- [ ] **Run:** `cargo test -p bqlite-storage compact_now_runs_only_eligible_buckets`
  Expected: FAIL with "cannot find function `run_compact_now`".

### Step 3.2: Implement `run_compact_now` and the eligibility scan

- [ ] **Add to `compaction.rs` (above the test module):**

```rust
/// Eligible buckets for a single table at a given moment, ordered the
/// scheduler's priority queue would order them: highest L0 count
/// first, then largest L0 byte size first.
pub fn eligible_buckets(
    db: &Database,
    table: &str,
    cfg: &CompactionConfig,
) -> Result<Vec<EligibleBucket>> {
    let entry = db
        .manifest()
        .tables
        .get(table)
        .ok_or_else(|| BqliteError::Execution(format!(
            "eligible_buckets: unknown table '{table}'"
        )))?;
    let mut out: Vec<EligibleBucket> = Vec::new();
    for win in &entry.windows {
        for (shard_idx, segments) in win.shards.iter().enumerate() {
            // Only L0 segments count toward the trigger thresholds.
            let l0: Vec<&SegmentMeta> = segments.iter().filter(|s| s.level == 0).collect();
            if l0.len() < 2 {
                continue;
            }
            let count = l0.len() as u64;
            let size: u64 = l0.iter().map(|s| s.byte_size).sum();
            let count_eligible = count > u64::from(cfg.l0_count_trigger);
            let size_eligible = size > cfg.l0_size_trigger_bytes;
            if count_eligible || size_eligible {
                out.push(EligibleBucket {
                    window_id: win.window_id,
                    shard_id: shard_idx as u32,
                    l0_count: count,
                    l0_byte_size: size,
                });
            }
        }
    }
    out.sort_by(|a, b| {
        b.l0_count
            .cmp(&a.l0_count)
            .then_with(|| b.l0_byte_size.cmp(&a.l0_byte_size))
            .then_with(|| a.window_id.cmp(&b.window_id))
            .then_with(|| a.shard_id.cmp(&b.shard_id))
    });
    Ok(out)
}

/// One eligible `(window, shard)` candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleBucket {
    pub window_id: u32,
    pub shard_id: u32,
    pub l0_count: u64,
    pub l0_byte_size: u64,
}

/// Synchronous entry point used by `Database::compact_now(table)`.
///
/// Iterates eligible buckets and runs [`compact_one`] on each, in
/// scheduler priority order. Returns every successful outcome; the
/// first failure aborts the loop and surfaces the error.
pub fn run_compact_now(
    db: &mut Database,
    table: &str,
    cfg: &CompactionConfig,
) -> Result<Vec<CompactionOutcome>> {
    let mut outcomes = Vec::new();
    loop {
        let eligible = eligible_buckets(db, table, cfg)?;
        if eligible.is_empty() {
            break;
        }
        // Re-evaluate after each compaction in case it shrank the
        // backlog or surfaced new winners. compact_now's contract is
        // "compact every eligible bucket"; we drain to fixed point.
        let bucket = &eligible[0];
        let outcome = compact_one(db, table, bucket.window_id, bucket.shard_id)?;
        outcomes.push(outcome);
    }
    Ok(outcomes)
}
```

### Step 3.3: Add `Database::compact_now`

- [ ] **In `crates/bqlite-storage/src/database.rs`, add a method** in the `impl Database` block (locate it near the other public methods like `compact_now`-shaped ones — e.g. after `drop_table`):

```rust
    /// Compact every eligible `(window, shard)` for `table` synchronously.
    ///
    /// Implements the `compact_now` API from
    /// `docs/design/storage/compaction-concurrency.md` §3.3. Runs on the
    /// caller's thread and bypasses the core-budget semaphore (the
    /// caller is opting in explicitly). Primary consumers are tests,
    /// CLI commands, and operator scripts.
    ///
    /// Uses the default [`crate::compaction::CompactionConfig`]; the
    /// configurable variant lives on the future scheduler API.
    ///
    /// # Errors
    ///
    /// Surfaces the first compaction failure; previously-compacted
    /// buckets in this call stay published.
    pub fn compact_now(
        &mut self,
        table: &str,
    ) -> Result<Vec<crate::compaction::CompactionOutcome>> {
        let cfg = crate::compaction::CompactionConfig::default();
        crate::compaction::run_compact_now(self, table, &cfg)
    }
```

### Step 3.4: Run tests, validate, review, commit, merge

- [ ] **Run:** `cargo test -p bqlite-storage compact_now`
  Expected: PASS.
- [ ] `cargo fmt --all` && `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] `bash scripts/local-ci.sh`.
- [ ] Code-review subagent on staged diff.
- [ ] Commit:

```bash
git add crates/bqlite-storage/src/compaction.rs crates/bqlite-storage/src/database.rs
git commit -m "TASK-408: Database::compact_now + eligibility (CP3)"
```

- [ ] Merge to main.

---

## Task 4: CP4 — Background scheduler + retry cooldown + backlog metric

**Files:**
- Modify: `crates/bqlite-storage/src/compaction.rs`

### Step 4.1: Write the failing scheduler test

- [ ] **Append to `compaction.rs`'s test module:**

```rust
#[test]
fn scheduler_runs_eligible_jobs_and_updates_backlog_metric() {
    use std::sync::Mutex;
    use std::time::Duration;

    let path = scratch_dir("scheduler-runs");
    let mut db = Database::create(&path).unwrap();
    db.create_table("events".into(), events_schema()).unwrap();
    for i in 0..6 {
        ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[make_event("a", 100 + i as i64, "x")],
        );
    }
    let db = Arc::new(Mutex::new(db));
    let cfg = CompactionConfig {
        l0_count_trigger: 4,
        l0_size_trigger_bytes: u64::MAX,
        pool_size: 1,
        core_budget_permits: 1,
        retry_cooldown: Duration::from_millis(50),
    };
    let metrics = CompactionMetrics::new();
    let scheduler = CompactionScheduler::start(db.clone(), cfg, metrics.clone());

    // Ask the scheduler to evaluate the table and run anything eligible.
    scheduler.notify_table("events");

    // Spin-wait up to 2 seconds for the backlog to drain.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        let entry_count = {
            let g = db.lock().unwrap();
            g.manifest().tables["events"].windows[0].shards[0].len()
        };
        if entry_count == 1 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("scheduler did not drain backlog within 2 seconds");
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    scheduler.shutdown();

    // After draining, the metrics snapshot should not contain a
    // backlog entry for the now-empty bucket.
    let snap = metrics.backlog_snapshot();
    assert!(
        !snap.iter().any(|(_, w, s, _)| *w == 0 && *s == 0),
        "backlog entry should be cleared, got {snap:?}"
    );
    let path_clone = path.clone();
    drop(db);
    let _ = std::fs::remove_dir_all(&path_clone);
}
```

- [ ] **Run:** `cargo test -p bqlite-storage scheduler_runs_eligible_jobs_and_updates_backlog_metric`
  Expected: FAIL with missing `CompactionScheduler::start` / `notify_table` / `shutdown`.

### Step 4.2: Implement the scheduler

- [ ] **Add to `compaction.rs` (above tests):**

```rust
use std::collections::{BinaryHeap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Background compaction scheduler.
///
/// Owns a thread pool and a priority queue of pending
/// `(table, window, shard)` jobs. Jobs are produced by
/// [`CompactionScheduler::notify_table`], which scans the table for
/// eligible buckets and enqueues every one that is not already in the
/// queue and not in cooldown.
///
/// Lifecycle: [`start`] → 0..N [`notify_table`] → [`shutdown`].
///
/// Future TASK-438 work will plug query workers into the same
/// [`CoreBudget`]; until then the budget is uncontested and the
/// per-row-group acquire/release is a cheap no-op.
pub struct CompactionScheduler {
    inner: Arc<SchedulerInner>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

struct SchedulerInner {
    db: Arc<Mutex<Database>>,
    cfg: CompactionConfig,
    metrics: Arc<CompactionMetrics>,
    budget: Arc<CoreBudget>,
    queue: Mutex<SchedulerQueue>,
    cv: Condvar,
}

#[derive(Default)]
struct SchedulerQueue {
    /// Pending jobs ordered by descending L0 count, then byte size.
    heap: BinaryHeap<QueuedJob>,
    /// Set of `(table, window, shard)` already in the heap, to
    /// prevent duplicate enqueues.
    in_flight: std::collections::HashSet<(String, u32, u32)>,
    /// Per-bucket cooldown — the earliest time a recently-failed
    /// `(table, window, shard)` may be re-enqueued.
    cooldown: HashMap<(String, u32, u32), Instant>,
    shutdown: bool,
}

#[derive(Debug)]
struct QueuedJob {
    table: String,
    window_id: u32,
    shard_id: u32,
    l0_count: u64,
    l0_byte_size: u64,
}

impl PartialEq for QueuedJob {
    fn eq(&self, other: &Self) -> bool {
        self.l0_count == other.l0_count && self.l0_byte_size == other.l0_byte_size
    }
}
impl Eq for QueuedJob {}
impl PartialOrd for QueuedJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueuedJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is a max-heap; we want highest L0 count first,
        // then highest byte size. Ties → insertion order doesn't
        // matter, but we add table/window/shard for determinism.
        self.l0_count
            .cmp(&other.l0_count)
            .then_with(|| self.l0_byte_size.cmp(&other.l0_byte_size))
            .then_with(|| other.table.cmp(&self.table))
            .then_with(|| other.window_id.cmp(&self.window_id))
            .then_with(|| other.shard_id.cmp(&self.shard_id))
    }
}

impl CompactionScheduler {
    /// Start the scheduler with `cfg.pool_size` worker threads.
    pub fn start(
        db: Arc<Mutex<Database>>,
        cfg: CompactionConfig,
        metrics: Arc<CompactionMetrics>,
    ) -> Self {
        let budget = CoreBudget::new(cfg.core_budget_permits);
        let inner = Arc::new(SchedulerInner {
            db,
            cfg,
            metrics,
            budget,
            queue: Mutex::new(SchedulerQueue::default()),
            cv: Condvar::new(),
        });
        let mut workers = Vec::with_capacity(inner.cfg.pool_size);
        for _ in 0..inner.cfg.pool_size {
            let inner_cl = inner.clone();
            workers.push(std::thread::spawn(move || worker_loop(inner_cl)));
        }
        Self { inner, workers }
    }

    /// Refresh the eligible-bucket set for `table` and enqueue any new
    /// candidates. Returns immediately; actual work runs on the pool.
    pub fn notify_table(&self, table: &str) {
        // Snapshot eligibility under the db lock, then enqueue under
        // the queue lock — never both at once, to avoid deadlock with
        // workers holding the queue lock while waiting for the db
        // lock.
        let eligible: Vec<EligibleBucket> = {
            let db = self.inner.db.lock().expect("db mutex poisoned");
            eligible_buckets(&db, table, &self.inner.cfg).unwrap_or_default()
        };
        let now = Instant::now();
        let mut q = self.inner.queue.lock().expect("queue mutex poisoned");
        for b in &eligible {
            self.inner
                .metrics
                .set_backlog(table, b.window_id, b.shard_id, b.l0_count);
            let key = (table.to_string(), b.window_id, b.shard_id);
            if q.in_flight.contains(&key) {
                continue;
            }
            if let Some(until) = q.cooldown.get(&key) {
                if *until > now {
                    continue;
                }
                q.cooldown.remove(&key);
            }
            q.in_flight.insert(key.clone());
            q.heap.push(QueuedJob {
                table: table.to_string(),
                window_id: b.window_id,
                shard_id: b.shard_id,
                l0_count: b.l0_count,
                l0_byte_size: b.l0_byte_size,
            });
            self.inner.cv.notify_one();
        }
    }

    /// Stop accepting new work and join every worker thread. Idempotent.
    pub fn shutdown(self) {
        {
            let mut q = self.inner.queue.lock().expect("queue mutex poisoned");
            q.shutdown = true;
            self.inner.cv.notify_all();
        }
        for h in self.workers {
            let _ = h.join();
        }
    }
}

fn worker_loop(inner: Arc<SchedulerInner>) {
    loop {
        let job = {
            let mut q = inner.queue.lock().expect("queue mutex poisoned");
            loop {
                if q.shutdown {
                    return;
                }
                if let Some(j) = q.heap.pop() {
                    break j;
                }
                q = inner.cv.wait(q).expect("queue cv poisoned");
            }
        };

        // Acquire one core-budget permit at the row-group boundary.
        // Today the entire job runs while holding this single permit
        // because `compact_one` materialises the whole merge in one
        // go (see CP2 implementation note); when the streaming row-
        // group writer lands the acquire/release will move inside the
        // row-group loop per compaction-concurrency.md §4.
        let _permit = inner.budget.acquire();

        let key = (job.table.clone(), job.window_id, job.shard_id);
        let result = {
            let mut db = inner.db.lock().expect("db mutex poisoned");
            crate::compaction::compact_one(&mut db, &job.table, job.window_id, job.shard_id)
        };

        let mut q = inner.queue.lock().expect("queue mutex poisoned");
        q.in_flight.remove(&key);
        match result {
            Ok(_) => {
                // Refresh the metric for the bucket — it should be 0
                // (absorbed into the new higher-level segment) unless
                // a concurrent ingest landed in the meantime.
                let new_count = {
                    drop(q);
                    let db = inner.db.lock().expect("db mutex poisoned");
                    let entry = db.manifest().tables.get(&job.table);
                    let n = entry
                        .and_then(|e| e.windows.iter().find(|w| w.window_id == job.window_id))
                        .and_then(|w| w.shards.get(job.shard_id as usize))
                        .map(|segs| segs.iter().filter(|s| s.level == 0).count())
                        .unwrap_or(0) as u64;
                    n
                };
                inner
                    .metrics
                    .set_backlog(&job.table, job.window_id, job.shard_id, new_count);
            }
            Err(_e) => {
                // Place the bucket in cooldown to avoid busy-looping
                // on a persistently failing job.
                let until = Instant::now() + inner.cfg.retry_cooldown;
                q.cooldown.insert(key, until);
            }
        }
    }
}
```

### Step 4.3: Run tests, validate, review, commit, merge

- [ ] **Run:** `cargo test -p bqlite-storage scheduler_runs_eligible_jobs_and_updates_backlog_metric`
  Expected: PASS.
- [ ] **Run the full test module:** `cargo test -p bqlite-storage compaction`
  Expected: every test from CPs 1–4 passes.
- [ ] `cargo fmt --all` && `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] `bash scripts/local-ci.sh`.
- [ ] Code-review subagent on staged diff. Particular attention to:
  - Lock ordering (db mutex never held while taking queue mutex inside `notify_table`).
  - Shutdown correctness (no worker hang on a notified-empty queue).
  - Cooldown re-evaluation on the next `notify_table`.
- [ ] Commit:

```bash
git add crates/bqlite-storage/src/compaction.rs
git commit -m "TASK-408: background scheduler + retry cooldown + backlog metric (CP4)"
```

- [ ] Merge to main.

---

## Task 5: Completion

- [ ] **Move the lock file:**

```bash
git mv tasks/active/TASK-408.lock tasks/completed/TASK-408.done
```

- [ ] **Edit `tasks/completed/TASK-408.done`** to add `completed_at` (current UTC ISO-8601). Final shape:

```json
{
  "agent_id": "agent-1",
  "task_id": "TASK-408",
  "claimed_at": "2026-04-19T21:08:37Z",
  "completed_at": "<current UTC>",
  "branch": "task/TASK-408",
  "description": "Compaction executor + scheduler"
}
```

- [ ] **Commit and push:**

```bash
git add tasks/completed/TASK-408.done
git rm -- tasks/active/TASK-408.lock 2>/dev/null || true
git commit -m "TASK-408: completed"
git push origin main
```

- [ ] **End the agent turn.** Do not claim another task — the wrapper handles that.
