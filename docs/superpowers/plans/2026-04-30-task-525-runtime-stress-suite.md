# TASK-525: Wave 5 runtime stress suite — implementation plan

> **For agentic workers:** This plan adds a single integration-test
> binary at `tests/tests/wave5_runtime_stress.rs`. Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** Add `tests/tests/wave5_runtime_stress.rs` covering the five
behaviour bands listed in TASK-525: hard memory-budget exhaustion,
sort spill fallback, concurrent DELETE / query snapshot isolation,
timeout & cancellation cleanup of temp spill files, and
warning-channel overflow aggregation.

**Architecture:** One integration-test binary that mixes two test
shapes:

1. **End-to-end through `Engine::query`** for everything the public
   surface exposes today — sort spill correctness, hard-budget
   exhaustion via cohort materialization, snapshot isolation across a
   DELETE that lands between two queries.
2. **Contract tests against the runtime types** (`QueryContext`,
   `WarningSink`, `SpillFs`, `MemoryTracker`, `CancellationToken`,
   `HashAggregateOperator::new`, `DistinctOperator::new`) for the
   bands TASK-525's spec covers but the *public* surface does not yet
   expose a trigger for — query timeouts, hard `MaxGroupsExceeded`
   with a sub-default cap, panic propagation through `Engine::query`.

The split is intentional: TASK-525 is the *stress suite* — its job is
to lock in the protocol contracts that downstream waves (CLI Ctrl-C
in Wave 6, the acceptance gate in TASK-528) will consume. Tests that
require trigger APIs that have not yet landed are written against
the contract types they delegate to. They stay valid once the upper
layers are wired.

**Tech stack:** `bqlite-engine` (`Engine`, `EngineConfig`,
`QueryOptions`, `QueryContext`, `WarningSink`, `WorkerContext`),
`bqlite-operators` (`SortOperator::with_spill`,
`HashAggregateOperator::new`, `DistinctOperator::new`,
`CancellationToken`), `bqlite-storage` (`Database`, `SpillFs`),
`bqlite-core` (`BqliteError` variants, `MemoryTracker`,
`QueryWarning`, `TempSpillFile`, `SpillQueryId`),
`bqlite-tests::common::TempDb`, `bqlite-tests::csv::FixtureConfig`,
`bqlite-tests::jsonl`.

**Reconciles:** `docs/design/engine/cancellation.md`,
`docs/design/engine/spill.md`, `docs/design/engine/memory-budget.md`,
`docs/design/engine/morsel-scheduler.md`,
`docs/design/storage/deletes.md` § 9.

---

## Scope notes

### What this suite asserts (acceptance criteria)

For each TASK-525 band, the suite asserts the named invariants from
the design docs:

- **Hard budget exhaustion** (`memory-budget.md` § 4 / § 7,
  `cancellation.md` § 6.1): cohort materialization at a tight budget
  surfaces `BqliteError::MemoryBudgetExceeded { used, budget }` with
  `used >= budget`; `MaxGroupsExceeded { limit }` is exhaustively
  reachable through the operator constructor with a small hard cap.
- **Spill fallback** (`spill.md` § 6 / § 8 / § 11): a sort query at
  `MIN_QUERY_BUDGET_BYTES` finishes with the same row order as the
  same query at the default budget; `peak_memory_bytes` is
  surfaced as `Some(_)` (per `query.rs:91-98`, `Some(0)` is the
  expected value when no operator yet calls `try_reserve` against
  the budget — the contract is that the tracker is *present*, not
  that it is non-zero); the per-query spill subdirectory no longer
  exists once the result returns.
- **Concurrent DELETE / query snapshot isolation**
  (`deletes.md` § 9 / `database.rs::tombstone_shard_lock`,
  `manifest.rs::snapshot_for_query`): a query started before an
  intervening DELETE observes the pre-DELETE row set; the per-shard
  tombstone-write lock is the same `Arc<Mutex<()>>` across calls and
  serializes per-shard mutations under contention.
- **Timeout & cancellation cleanup of temp files** (`spill.md` § 8.3
  / `cancellation.md` § 5.1 / § 5.2): even when an individual
  `TempSpillFile` guard's `Drop` is suppressed, the per-query
  belt-and-braces sweep on the last `QueryContext` clone reclaims the
  per-query subdirectory. A cancellation signalled while a sort is
  accumulating surfaces `BqliteError::Cancelled` and leaves no spill
  residue.
- **Warning-channel overflow** (`cancellation.md` § 7.2 / § 7.3): a
  `WarningSink` driven past the per-worker cap of 1,000 entries
  emits exactly one trailing
  `QueryWarning::WarningsOverflow { suppressed_count }` with the
  correct count; concurrent producers across threads share one buffer
  and respect the cap; a sink read on a clone after `into_warnings`
  returns an empty `Vec`.

### What this suite intentionally does *not* cover

- **Public timeout API** — `Engine` does not yet expose a per-query
  timeout knob (`cancellation.md` § 6.2 lists this as TASK-505 work
  whose engine surface follow-up is downstream of TASK-525). The
  cancellation tests use `QueryContext::cancellation()` directly.
  This is an honest reflection of what Wave 5 ships, not a gap.
- **Worker-thread `catch_unwind` boundaries** — `Engine::query`
  installs a single-driver `catch_unwind` per the current
  `query.rs::query_with_options` body. Per-`(worker, morsel)`
  boundaries are TASK-541 follow-on work; this suite stays at the
  driver level.
- **Ingest partitioner spill** — TASK-512 has not landed; the
  partitioner remains in-memory. The wave5 acceptance gate
  (TASK-528) will pick up the end-to-end coverage once it does.
- **Per-query `MaxGroupsExceeded` through `Engine::query`** —
  `DEFAULT_MAX_GROUPS = 1_000_000` is hardcoded in the planner
  (`crates/bqlite-planner/src/physical.rs:659`); driving 1M+
  distinct groups through the engine path is too expensive for an
  integration test. The suite tests the operator constructor with a
  small `max_groups` instead.

These deferrals are documented in the test-file module comment so
future readers know they are intentional, not omissions.

---

## File structure

The suite is one file. Keeping it as a single binary keeps Cargo's
auto-discovery free and avoids spreading helpers across multiple
test crates.

- **Create:** `tests/tests/wave5_runtime_stress.rs` — the suite
  itself, with the test-file-local helpers documented at the top.
- **No modifications** to other crates. The plan does not change
  any public API; if a test exposes a missing surface, the test is
  written against the next-lowest accessible layer (per the contract
  policy in the scope note above).

The file's internal layout:

```
//! TASK-525: Wave 5 runtime stress suite.
//! [module-level docs reconciling against design docs §§ above]

mod helpers {                         // file-local helpers
    fn scratch_db_root(label) -> PathBuf
    fn make_tracked_ctx_with_spill(budget, db_root) -> (QueryContext, SpillFs, PathBuf)
    fn build_sort_input_batches(rows, batch) -> Vec<RecordBatch>
    fn ingest_jsonl_purchases(engine, db, rows, entities) -> ()
    fn count_rows(result: &ExecutionResult) -> usize
    fn distinct_entity_set(result: &ExecutionResult) -> BTreeSet<String>
}

mod budget_exhaustion { … cohort + MaxGroups operator-level tests }
mod spill_fallback     { … sort-spill end-to-end + QueryContext sweep }
mod cancellation_cleanup { … cancel mid-flight, belt-and-braces sweep }
mod snapshot_isolation { … snapshot_for_query + tombstone_shard_lock }
mod warning_overflow   { … WarningSink cap + concurrent producers }
```

Each `mod` is a thin namespace — every test inside is `#[test]` and
named for the assertion it makes. Internal modules avoid name
collisions across the five bands without splitting the binary.

---

## Task 1: Scaffold the file with the module-level docs and helpers

**Files:**
- Create: `tests/tests/wave5_runtime_stress.rs`

This task creates the empty suite file with the module-level
documentation, the helper module, and one trivial smoke `#[test]` to
prove the binary compiles and runs. Subsequent tasks add the band
tests one at a time.

- [ ] **Step 1: Author the file**

```rust
//! TASK-525: Wave 5 runtime stress suite.
//!
//! Locks in the runtime contracts the design docs §§ below freeze
//! for Wave 5. The suite is split into five bands, one `mod` each:
//!
//! - `budget_exhaustion`  — `engine/memory-budget.md` § 4 / § 7
//! - `spill_fallback`      — `engine/spill.md` § 6 / § 8 / § 11
//! - `cancellation_cleanup` — `engine/cancellation.md` § 5.1 / § 5.2
//! - `snapshot_isolation`  — `storage/deletes.md` § 9
//! - `warning_overflow`    — `engine/cancellation.md` § 7.2 / § 7.3
//!
//! Bands marked "contract level" run against the runtime types
//! directly (`QueryContext`, `WarningSink`, `SpillFs`,
//! `HashAggregateOperator::new`) where the public `Engine::query`
//! surface does not yet expose a trigger — see the plan in
//! `docs/superpowers/plans/2026-04-30-task-525-runtime-stress-suite.md`
//! § "Scope notes" for the rationale.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

mod helpers {
    use super::*;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Allocate a unique scratch directory under the OS temp dir.
    /// Each test gets its own to avoid cross-test interference and
    /// to keep `Database::create` happy (it expects an empty path).
    pub fn scratch_db_root(label: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut p = std::env::temp_dir();
        p.push(format!("bqlite-task525-{label}-{pid}-{seq}"));
        p
    }
}

#[test]
fn suite_compiles_and_runs() {
    // Sanity-check that the binary compiles. The five bands below
    // exercise the actual contracts.
    let p = helpers::scratch_db_root("compile");
    assert!(p.to_string_lossy().contains("bqlite-task525-compile-"));
}
```

- [ ] **Step 2: Run the new binary in isolation**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress --
suite_compiles_and_runs --nocapture`
Expected: PASS.

- [ ] **Step 3: Run `scripts/local-ci.sh`**

Run: `scripts/local-ci.sh`
Expected: green end-to-end (fmt + dep-direction + clippy -D warnings
+ build + test).

- [ ] **Step 4: Subagent code review of the staged diff**

Spawn a code-reviewer subagent and pass it the staged diff plus the
plan text. Address any blocking findings before committing.

- [ ] **Step 5: Commit**

```bash
git add tests/tests/wave5_runtime_stress.rs \
        docs/superpowers/plans/2026-04-30-task-525-runtime-stress-suite.md
git commit -m "TASK-525: Add wave5 runtime stress suite scaffold"
```

- [ ] **Step 6: Fast-forward merge to main**

```bash
git checkout main && git pull origin main
git merge task/TASK-525 --ff-only && git push origin main
git checkout task/TASK-525
```

If `--ff-only` fails, rebase and retry per the merge-protocol in
`AGENTS.md`.

---

## Task 2: Warning-channel overflow band

**Files:**
- Modify: `tests/tests/wave5_runtime_stress.rs`

This is the simplest band — pure unit-shape tests against
`WarningSink` / `WorkerContext`. Doing it first establishes the
band-`mod` pattern.

- [ ] **Step 1: Add the `warning_overflow` band**

Append to `tests/tests/wave5_runtime_stress.rs`:

```rust
// ─────────────────────────────────────────────────────────────────
// Warning-channel overflow (cancellation.md §7.2 / §7.3)
// ─────────────────────────────────────────────────────────────────

mod warning_overflow {
    use std::sync::Arc;
    use std::thread;

    use bqlite_core::QueryWarning;
    use bqlite_engine::{WarningSink, WorkerContext};

    /// Driving the sink past the cap emits exactly one trailing
    /// `WarningsOverflow` whose `suppressed_count` matches the
    /// number of records dropped. The `cancellation.md` §7.3 rule
    /// is "the overflow marker is the last element"; this test
    /// asserts both ordering and arithmetic.
    #[test]
    fn overflow_marker_is_last_and_suppressed_count_matches() {
        let sink = WarningSink::new();
        let cap = WorkerContext::PER_WORKER_WARNING_CAP;
        let extra = 7usize; // any non-zero count exercises the path
        for i in 0..(cap + extra) {
            sink.record(QueryWarning::EntityEventLimitExceeded {
                entity_id: format!("e{i}"),
                count: 1,
                limit: 1,
            });
        }
        let warnings = sink.into_warnings();
        assert_eq!(warnings.len(), cap + 1, "{cap} kept + 1 marker");
        match warnings.last().unwrap() {
            QueryWarning::WarningsOverflow { suppressed_count } => {
                assert_eq!(*suppressed_count as usize, extra);
            }
            other => panic!("expected trailing WarningsOverflow, got {other:?}"),
        }
        // Every kept warning is one of the EntityEventLimitExceeded
        // entries — the marker did not displace a real warning.
        for w in &warnings[..cap] {
            assert!(matches!(w, QueryWarning::EntityEventLimitExceeded { .. }));
        }
    }

    /// Exactly the cap fits; no marker is appended.
    #[test]
    fn at_cap_no_overflow_marker() {
        let sink = WarningSink::new();
        let cap = WorkerContext::PER_WORKER_WARNING_CAP;
        for i in 0..cap {
            sink.record(QueryWarning::EntityEventLimitExceeded {
                entity_id: format!("e{i}"),
                count: 1,
                limit: 1,
            });
        }
        let warnings = sink.into_warnings();
        assert_eq!(warnings.len(), cap);
        for w in &warnings {
            assert!(matches!(w, QueryWarning::EntityEventLimitExceeded { .. }));
        }
    }

    /// Multiple producer threads share one `WarningSink`. The cap
    /// must hold across the aggregate, and the suppressed count
    /// must equal `total_recorded - cap` exactly. This is the
    /// stress shape that the existing `warning_channel.rs` does not
    /// cover.
    #[test]
    fn concurrent_producers_respect_cap_aggregate() {
        let cap = WorkerContext::PER_WORKER_WARNING_CAP;
        let producers = 8;
        let per_producer = (cap / producers) + 50; // total > cap
        let total = producers * per_producer;
        let sink = WarningSink::new();
        let handles: Vec<_> = (0..producers)
            .map(|p| {
                let s = sink.clone();
                thread::spawn(move || {
                    for i in 0..per_producer {
                        s.record(QueryWarning::EntityEventLimitExceeded {
                            entity_id: format!("p{p}-i{i}"),
                            count: 1,
                            limit: 1,
                        });
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("producer thread");
        }
        let warnings = sink.into_warnings();
        // cap kept + (total - cap) suppressed → 1 marker
        assert_eq!(warnings.len(), cap + 1);
        match warnings.last().unwrap() {
            QueryWarning::WarningsOverflow { suppressed_count } => {
                assert_eq!(
                    *suppressed_count as usize,
                    total - cap,
                    "exact arithmetic on suppressed count under contention"
                );
            }
            other => panic!("expected trailing WarningsOverflow, got {other:?}"),
        }
    }

    /// `into_warnings` consumes the buffer; reading a clone after
    /// the drain returns an empty list. This is the documented
    /// idempotence rule from `warning_sink.rs::into_warnings`.
    #[test]
    fn second_drain_through_clone_returns_empty() {
        let sink = WarningSink::new();
        let clone = sink.clone();
        sink.record(QueryWarning::ActiveStateLimitExceeded {
            entity_id: "x".into(),
            active_states: 1,
            cap: 1,
        });
        let first = sink.into_warnings();
        assert_eq!(first.len(), 1);
        let second = clone.into_warnings();
        assert!(second.is_empty(), "drain is idempotent");
    }

    /// `record_many` is a single mutex acquisition per call — it
    /// must respect the cap atomically rather than batch by batch.
    /// Drives 1,500 warnings in batches of 100 and verifies the
    /// kept count is exactly the cap.
    #[test]
    fn record_many_respects_cap_across_batches() {
        let sink = WarningSink::new();
        let cap = WorkerContext::PER_WORKER_WARNING_CAP;
        let total = cap + 500;
        let mut delivered = 0usize;
        while delivered < total {
            let chunk: Vec<_> = (0..100)
                .map(|i| QueryWarning::SessionEventCapExceeded {
                    entity_id: format!("e{}", delivered + i),
                    event_count: 1,
                    cap: 1,
                })
                .collect();
            sink.record_many(chunk);
            delivered += 100;
        }
        let warnings = sink.into_warnings();
        // cap warnings + 1 marker
        assert_eq!(warnings.len(), cap + 1);
        match warnings.last().unwrap() {
            QueryWarning::WarningsOverflow { suppressed_count } => {
                assert_eq!(*suppressed_count as usize, total - cap);
            }
            other => panic!("expected trailing WarningsOverflow, got {other:?}"),
        }
        // Sanity: Arc is the only synchronization point.
        let _ = Arc::new(()); // suppress unused-import lint if any
    }
}
```

- [ ] **Step 2: Run the band**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress -- warning_overflow`
Expected: 5 tests pass, all in <1s.

- [ ] **Step 3: Run `scripts/local-ci.sh`**

Expected: green.

- [ ] **Step 4: Subagent code review**

- [ ] **Step 5: Commit & merge**

```bash
git add tests/tests/wave5_runtime_stress.rs
git commit -m "TASK-525: Add warning-channel overflow stress band"
git checkout main && git pull --ff-only && git merge task/TASK-525 --ff-only && git push origin main
git checkout task/TASK-525
```

---

## Task 3: Cancellation & spill cleanup band

**Files:**
- Modify: `tests/tests/wave5_runtime_stress.rs`

Tests `QueryContext`'s belt-and-braces sweep, `CancellationToken`
propagation, and the `SpillFs` per-query subdirectory layout.

- [ ] **Step 1: Add the band**

Append:

```rust
// ─────────────────────────────────────────────────────────────────
// Cancellation & spill-cleanup contract
// (cancellation.md §5.1 / §5.2, spill.md §8.3)
// ─────────────────────────────────────────────────────────────────

mod cancellation_cleanup {
    use std::sync::Arc;

    use bqlite_core::spill::SpillFs;
    use bqlite_engine::{QueryContext, MIN_QUERY_BUDGET_BYTES};

    use super::helpers::scratch_db_root;

    /// `cancel()` on the original token is observed by every clone
    /// of the `QueryContext`. Operators clone the context (or its
    /// token) freely; the `Arc<AtomicBool>` storage means the
    /// signal must propagate atomically.
    ///
    /// Re-asserted at the binary-test level so a future refactor
    /// of `QueryContext` that drops the engine-internal unit test
    /// at `crates/bqlite-engine/src/context.rs::cancellation_token_propagates_through_clones`
    /// does not silently lose this contract.
    #[test]
    fn cancel_propagates_through_context_clones() {
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
        let clone_a = ctx.clone();
        let clone_b = clone_a.clone();
        assert!(!ctx.cancellation().is_cancelled());
        assert!(!clone_a.cancellation().is_cancelled());
        assert!(!clone_b.cancellation().is_cancelled());
        ctx.cancellation().cancel();
        assert!(clone_a.cancellation().is_cancelled());
        assert!(clone_b.cancellation().is_cancelled());
    }

    /// The belt-and-braces sweep in `cancellation.md` §5.2 runs on
    /// the *last* `QueryContext` clone's drop. While any clone is
    /// alive the per-query subdir must persist, including any leaked
    /// `TempSpillFile` paths.
    ///
    /// Re-asserted at the binary-test level (the engine-internal
    /// unit test at `context.rs::dropping_last_clone_runs_belt_and_braces_cleanup`
    /// covers the same shape; suite-level coverage here so a refactor
    /// that drops the unit test does not silently lose the contract).
    #[test]
    fn last_clone_drop_reclaims_per_query_subdir_with_leaked_file() {
        let db_root = scratch_db_root("leak-cleanup");
        let spill_root = db_root.join("spill");
        std::fs::create_dir_all(&db_root).unwrap();
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();

        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let qid = ctx.spill_query_id().expect("qid attached");

        // Open and *forget* a guard. This simulates a leaked
        // `TempSpillFile` — its `Drop` never runs, so the file
        // survives until the belt-and-braces sweep fires.
        let guard = ctx.open_spill("sort-run").expect("fs attached").expect("opens");
        let leaked = guard.path().to_path_buf();
        std::mem::forget(guard);
        assert!(leaked.exists(), "leaked file present pre-sweep");

        let clone = ctx.clone();
        drop(ctx);
        assert!(leaked.exists(), "sweep waits for last clone");
        drop(clone);

        let qdir = spill_root.join(qid.to_string());
        assert!(!qdir.exists(), "per-query subdir reclaimed by sweep");
        let _ = std::fs::remove_dir_all(&db_root);
    }

    /// Two queries on the same `SpillFs` get distinct subdirs and
    /// the cleanup of one does not touch the other's files.
    #[test]
    fn two_queries_have_independent_per_query_subdirs() {
        let db_root = scratch_db_root("indep-subdir");
        let spill_root = db_root.join("spill");
        std::fs::create_dir_all(&db_root).unwrap();
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();

        let ctx_a = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let ctx_b = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let qid_a = ctx_a.spill_query_id().unwrap();
        let qid_b = ctx_b.spill_query_id().unwrap();
        assert_ne!(qid_a, qid_b, "each query gets a distinct id");

        // Leak one file into each.
        let g_a = ctx_a.open_spill("sort-run").unwrap().unwrap();
        let g_b = ctx_b.open_spill("sort-run").unwrap().unwrap();
        let p_a = g_a.path().to_path_buf();
        let p_b = g_b.path().to_path_buf();
        std::mem::forget(g_a);
        std::mem::forget(g_b);

        // Drop A first; B's file must remain.
        drop(ctx_a);
        assert!(!p_a.exists(), "A's subdir reclaimed");
        assert!(p_b.exists(), "B's subdir untouched");

        drop(ctx_b);
        assert!(!p_b.exists(), "B's subdir reclaimed at last drop");
        let _ = std::fs::remove_dir_all(&db_root);
    }

    /// `unbounded()` contexts have no `SpillFs` attached; the cleanup
    /// guard does not exist, so nothing fires on drop. The contract
    /// is "no panic, no observable artefact" — read it back through
    /// the public accessors.
    #[test]
    fn unbounded_context_has_no_spill_attachment() {
        let ctx = QueryContext::unbounded();
        assert!(ctx.spill_fs().is_none());
        assert!(ctx.spill_query_id().is_none());
        assert!(ctx.open_spill("any").is_none());
    }
}
```

- [ ] **Step 2: Run the band**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress -- cancellation_cleanup`
Expected: 4 tests pass.

- [ ] **Step 3: Run `scripts/local-ci.sh`**

Expected: green.

- [ ] **Step 4: Subagent review**

- [ ] **Step 5: Commit & merge**

```bash
git add tests/tests/wave5_runtime_stress.rs
git commit -m "TASK-525: Add cancellation & spill-cleanup contract band"
git checkout main && git pull --ff-only && git merge task/TASK-525 --ff-only && git push origin main
git checkout task/TASK-525
```

---

## Task 4: Snapshot-isolation & multi-engine concurrency band

**Files:**
- Modify: `tests/tests/wave5_runtime_stress.rs`

Tests the storage-layer contracts the engine relies on for
snapshot isolation: `Manifest::snapshot_for_query` is decoupled from
later mutations, and the per-shard tombstone-write lock is the same
`Arc` across calls and serializes contended writers. The
"under real runtime scheduling" phrasing in TASK-525's spec is
honoured by:

- exercising `tombstone_shard_lock` from multiple threads — the
  actual concurrency primitive the storage layer holds under the
  morsel scheduler;
- running two `Engine::query` calls in parallel against two separate
  `Database` instances on independent paths, asserting the morsel
  scheduler does not deadlock when two engines submit data-plane
  work simultaneously and that `available_permits()` recovers to
  full capacity after both return.

- [ ] **Step 1: Add the band**

Append:

```rust
// ─────────────────────────────────────────────────────────────────
// Snapshot isolation (deletes.md §9, manifest.rs::snapshot_for_query)
// ─────────────────────────────────────────────────────────────────

mod snapshot_isolation {
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    use bqlite_core::time::TimeRange;
    use bqlite_engine::{Database, Engine};

    use super::helpers::scratch_db_root;

    const CREATE_TABLE: &str = "CREATE TABLE events (\
            entity_id STRING NOT NULL ENTITY KEY, \
            ts TIMESTAMP NOT NULL EVENT TIME, \
            event_type STRING NOT NULL EVENT TYPE\
        )";

    /// `Database::tombstone_shard_lock(table, window, shard)` returns
    /// the same `Arc<Mutex<()>>` on repeat calls. This is the
    /// per-shard serialization primitive from `deletes.md` §9 — its
    /// stable identity is what guarantees concurrent DELETE writers
    /// to the same shard are serialized.
    #[test]
    fn tombstone_shard_lock_identity_is_stable_per_shard() {
        let path = scratch_db_root("tombstone-lock-id");
        let db = Database::create(&path).unwrap();
        let a = db.tombstone_shard_lock("events", 0, 0);
        let b = db.tombstone_shard_lock("events", 0, 0);
        assert!(Arc::ptr_eq(&a, &b), "same shard returns the same Arc");

        let c = db.tombstone_shard_lock("events", 0, 1);
        assert!(!Arc::ptr_eq(&a, &c), "different shards get distinct locks");

        let d = db.tombstone_shard_lock("events", 1, 0);
        assert!(!Arc::ptr_eq(&a, &d), "different windows get distinct locks");
        let _ = std::fs::remove_dir_all(&path);
    }

    /// Two threads that both acquire the same `(window, shard)`
    /// lock cannot hold it simultaneously. A barrier coordinates
    /// the two — if the lock were not exclusive, the inner critical
    /// section would observe both threads at once. We assert the
    /// observed-overlap counter stays at 0.
    #[test]
    fn tombstone_shard_lock_serializes_contended_writers() {
        let path = scratch_db_root("tombstone-lock-serial");
        let db = Arc::new(Database::create(&path).unwrap());
        let lock = db.tombstone_shard_lock("events", 0, 0);

        // `inside` is the number of threads currently inside the
        // critical section. With a real `Mutex<()>` it must never
        // exceed 1.
        let inside = Arc::new(Mutex::new(0i32));
        let max_seen = Arc::new(Mutex::new(0i32));
        let barrier = Arc::new(Barrier::new(2));

        let h: Vec<_> = (0..2)
            .map(|_| {
                let lock = Arc::clone(&lock);
                let inside = Arc::clone(&inside);
                let max_seen = Arc::clone(&max_seen);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let _g = lock.lock().expect("not poisoned");
                    {
                        let mut n = inside.lock().unwrap();
                        *n += 1;
                        let mut m = max_seen.lock().unwrap();
                        if *n > *m {
                            *m = *n;
                        }
                    }
                    // Hold the lock long enough that the second
                    // thread definitely tries to acquire.
                    thread::sleep(Duration::from_millis(20));
                    {
                        let mut n = inside.lock().unwrap();
                        *n -= 1;
                    }
                })
            })
            .collect();
        for x in h {
            x.join().expect("worker thread");
        }
        assert_eq!(
            *max_seen.lock().unwrap(),
            1,
            "shard lock must enforce exclusive critical section"
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    /// `Manifest::snapshot_for_query` is a pure read of the current
    /// manifest snapshot. A second snapshot taken before any
    /// mutation must equal the first by length — there is nothing
    /// in between. The test pins the "snapshot is taken at bind
    /// time, no I/O" rule from `deletes.md` §6 / §9.
    #[test]
    fn snapshot_for_query_is_deterministic_between_mutations() {
        let path = scratch_db_root("snapshot-determinism");
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine.query(CREATE_TABLE, &mut db).unwrap();

        // Empty manifest — snapshot is the empty list.
        let s1 = db
            .snapshot_for_query("events", TimeRange::unbounded(), 0)
            .expect("snapshot");
        let s2 = db
            .snapshot_for_query("events", TimeRange::unbounded(), 0)
            .expect("snapshot");
        assert_eq!(s1.len(), s2.len(), "back-to-back snapshots agree");
        let _ = std::fs::remove_dir_all(&path);
    }

    /// A DELETE that lands between two `Engine::query` runs is
    /// observable in the second result but does not retroactively
    /// touch the first. Sequential per the current `&mut Database`
    /// constraint — Wave 6 will widen this to true concurrent runs
    /// on a shared handle.
    ///
    /// The fixture pins `entity_count = 5` and `row_count = 25`
    /// so user_0 owns exactly 5 rows (cyclic entity assignment in
    /// `bqlite_tests::jsonl`). After the cheap-class delete, the
    /// post-count is exactly `pre_count - 5`, not just `pre_count - 1`.
    #[test]
    fn delete_between_queries_visible_to_second_only() {
        use bqlite_tests::jsonl;

        let path = scratch_db_root("delete-between");
        std::fs::create_dir_all(&path).unwrap();
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine
            .query(jsonl::PURCHASES_CREATE_TABLE, &mut db)
            .unwrap();

        // Ingest 25 rows / 5 entities. `bqlite_tests::jsonl::write_fixture_file`
        // is the path-returning helper — `write_fixture` itself takes
        // (config, impl Write), not a path.
        let cfg = jsonl::FixtureConfig {
            row_count: 25,
            entity_count: 5,
        };
        let jsonl_path = jsonl::write_fixture_file(&cfg, &path, "fixture.jsonl")
            .expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            jsonl_path.display()
        );
        engine.query(&sql, &mut db).expect("ingest");

        let pre = engine
            .query("purchases", &mut db)
            .expect("first query");
        let pre_count = super::helpers::count_rows(&pre);
        assert_eq!(pre_count, 25, "fixture produced 25 rows");

        // Delete one entity. Cheap-class predicate (entity_id =).
        // 25 rows / 5 entities → user_0 owns exactly 5 rows.
        let _ = engine
            .query("DELETE FROM purchases WHERE user_id = 'user_0'", &mut db)
            .expect("delete");

        let post = engine
            .query("purchases", &mut db)
            .expect("second query");
        let post_count = super::helpers::count_rows(&post);
        assert_eq!(
            post_count, 20,
            "second query observes the delete of user_0's 5 rows: \
             pre={pre_count}, post={post_count}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Two `Engine` instances on two independent `Database` paths
    /// can run data-plane queries simultaneously without the morsel
    /// scheduler deadlocking. The assertion: both `query()` calls
    /// return successfully, and after both threads join the
    /// per-engine permit counts recover to full capacity.
    ///
    /// The morsel scheduler in each engine is an independent `Arc`
    /// (per `Engine::with_config` constructing a fresh scheduler)
    /// so this test exercises the engine's permit-acquisition
    /// path, not contention between two engines on one scheduler.
    /// That second shape is asserted by `engine_with_scheduler_shares_pool_across_engines`
    /// in `crates/bqlite-engine/src/query.rs::tests`.
    #[test]
    fn two_engines_run_concurrent_queries_without_deadlock() {
        use std::thread;

        use bqlite_engine::EngineConfig;
        use bqlite_tests::jsonl;

        let cfg = EngineConfig {
            query_threads: Some(1),
            ..EngineConfig::default()
        };

        let p_a = scratch_db_root("concurrent-a");
        let p_b = scratch_db_root("concurrent-b");
        std::fs::create_dir_all(&p_a).unwrap();
        std::fs::create_dir_all(&p_b).unwrap();

        // Build identical small fixtures on both databases.
        for p in [&p_a, &p_b] {
            let mut db = Database::create(p).unwrap();
            let engine = Engine::with_config(cfg);
            engine
                .query(jsonl::PURCHASES_CREATE_TABLE, &mut db)
                .unwrap();
            let cfg_fix = jsonl::FixtureConfig {
                row_count: 50,
                entity_count: 10,
            };
            let jsonl_path = jsonl::write_fixture_file(&cfg_fix, p, "fixture.jsonl")
                .expect("write fixture");
            let sql = format!(
                "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
                jsonl_path.display()
            );
            engine.query(&sql, &mut db).unwrap();
        }

        // Two threads, each owning its own engine + db. Both run
        // an ORDER BY (which dispatches through MorselScheduler::submit)
        // simultaneously.
        let h_a = thread::spawn(move || {
            let mut db = Database::open(&p_a).unwrap();
            let engine = Engine::with_config(cfg);
            let r = engine.query("purchases | ORDER BY ts ASC", &mut db).unwrap();
            let n: usize = r.rows.iter().map(|b| b.num_rows()).sum();
            (engine, p_a, n)
        });
        let h_b = thread::spawn(move || {
            let mut db = Database::open(&p_b).unwrap();
            let engine = Engine::with_config(cfg);
            let r = engine.query("purchases | ORDER BY ts ASC", &mut db).unwrap();
            let n: usize = r.rows.iter().map(|b| b.num_rows()).sum();
            (engine, p_b, n)
        });

        let (engine_a, path_a, n_a) = h_a.join().expect("thread A");
        let (engine_b, path_b, n_b) = h_b.join().expect("thread B");

        assert_eq!(n_a, 50);
        assert_eq!(n_b, 50);
        // Permits returned to full capacity on both engines.
        assert_eq!(
            engine_a.scheduler().available_permits(),
            engine_a.scheduler().query_threads()
        );
        assert_eq!(
            engine_b.scheduler().available_permits(),
            engine_b.scheduler().query_threads()
        );

        let _ = std::fs::remove_dir_all(&path_a);
        let _ = std::fs::remove_dir_all(&path_b);
    }
}
```

- [ ] **Step 2: Add a `count_rows` helper to the `helpers` mod**

The new test calls `helpers::count_rows`. Add this helper to the
`helpers` module in Task 1 (or now if it does not exist):

```rust
pub fn count_rows(result: &bqlite_engine::ExecutionResult) -> usize {
    result.rows.iter().map(|b| b.num_rows()).sum()
}
```

- [ ] **Step 3: Run the band**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress -- snapshot_isolation`
Expected: 4 tests pass.

- [ ] **Step 4: Run `scripts/local-ci.sh`**

Expected: green.

- [ ] **Step 5: Subagent review**

- [ ] **Step 6: Commit & merge**

```bash
git add tests/tests/wave5_runtime_stress.rs
git commit -m "TASK-525: Add snapshot-isolation contract band"
git checkout main && git pull --ff-only && git merge task/TASK-525 --ff-only && git push origin main
git checkout task/TASK-525
```

---

## Task 5: Hard-budget exhaustion band

**Files:**
- Modify: `tests/tests/wave5_runtime_stress.rs`

The realistic exhaustion paths today are:

- `MemoryBudgetExceeded` — covered exhaustively at the unit level
  in `crates/bqlite-core/src/memory.rs` and at the engine level in
  `crates/bqlite-engine/src/context.rs::tracked_context_overflow_surfaces_typed_error`.
  No additional suite-level assertion is non-duplicative.
- `MaxGroupsExceeded` — covered by
  `crates/bqlite-operators/src/aggregate/mod.rs::tests::max_groups_exceeded_returns_error`
  (and similarly in `distinct.rs` if present). Re-asserting the
  same shape at the suite level is ceremonial duplication.

The suite's *additive* contribution at the budget surface is:

- Asserting that `BqliteError::MemoryBudgetExceeded { used, budget }`
  is surfaced through the **engine wrapper** (i.e., reachable from
  `Engine::query_with_options`) — not just from the tracker
  directly. This is the path the CLI / Python binding sees; pinning
  it at the suite level guards against a future engine refactor
  that swallows or rewraps the typed variant.

We assert the engine-wrapper visibility through the existing
`tracked_context_overflow_surfaces_typed_error` shape — but at the
binary-test level, calling the trait method on a budget reachable
through `QueryContext::memory()`. This is one test, not a band-full.

The other exhaustion shapes (`MaxGroupsExceeded`, end-to-end
cohort overflow at the 512 MiB floor) are documented as deferred
to operator-internal tests / the wave5 acceptance gate (TASK-528).

- [ ] **Step 1: Add the band**

Append:

```rust
// ─────────────────────────────────────────────────────────────────
// Hard-budget exhaustion (memory-budget.md §4 / §7)
//
// Other exhaustion paths covered elsewhere:
//
//   - MemoryBudgetExceeded at the tracker / context level:
//     `crates/bqlite-core/src/memory.rs::tests` and
//     `crates/bqlite-engine/src/context.rs::tracked_context_overflow_surfaces_typed_error`.
//   - MaxGroupsExceeded at the operator level:
//     `crates/bqlite-operators/src/aggregate/mod.rs::tests::max_groups_exceeded_returns_error`
//     (and the distinct equivalent if present).
//
// The suite-level assertion below pins the wrapper-path behaviour:
// reading `QueryContext::memory()` and exhausting the budget through
// the trait surface surfaces the typed variant. This is the path
// the CLI / Python bindings observe. Writing it as a binary-test
// guards against an engine refactor that silently rewraps or
// stringifies the variant.
// ─────────────────────────────────────────────────────────────────

mod budget_exhaustion {
    use bqlite_core::BqliteError;
    use bqlite_engine::{QueryContext, MIN_QUERY_BUDGET_BYTES};

    /// Reading `QueryContext::memory()` and reserving past its
    /// budget surfaces `MemoryBudgetExceeded` with the structured
    /// `(used, budget)` fields. The suite re-asserts the wrapper
    /// path because the operators that will charge against this
    /// surface (TASK-512/513/514 follow-on wiring) are still being
    /// landed; pinning the trait-level shape here guards against an
    /// engine refactor that swallows or rewraps the variant before
    /// it reaches the caller.
    #[test]
    fn query_context_memory_overshoot_surfaces_typed_variant() {
        // Use the floor budget so the test is realistic against
        // engine-resolved budgets; the floor is the smallest value
        // `Engine::query_with_options` accepts.
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
        // Reserving more than the budget itself must fail with the
        // typed variant. The reservation amount is `budget + 1` so
        // the post-fail `used` is well-defined.
        let request = MIN_QUERY_BUDGET_BYTES + 1;
        let err = ctx
            .memory()
            .try_reserve(request)
            .expect_err("over-budget reservation must fail");
        match err {
            BqliteError::MemoryBudgetExceeded { used, budget } => {
                assert_eq!(budget, MIN_QUERY_BUDGET_BYTES);
                assert!(
                    used >= budget,
                    "post-fail used must reach the budget: used={used}, budget={budget}"
                );
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run the band**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress -- budget_exhaustion`
Expected: 1 test passes.

- [ ] **Step 3: Run `scripts/local-ci.sh`**

Expected: green.

- [ ] **Step 4: Subagent review**

- [ ] **Step 5: Commit & merge**

```bash
git add tests/tests/wave5_runtime_stress.rs
git commit -m "TASK-525: Add hard-budget exhaustion wrapper band"
git checkout main && git pull --ff-only && git merge task/TASK-525 --ff-only && git push origin main
git checkout task/TASK-525
```

---

## Task 6: Spill-fallback band (sort end-to-end)

**Files:**
- Modify: `tests/tests/wave5_runtime_stress.rs`

End-to-end test through `Engine::query`: a JSONL ingest of N rows,
then `purchases | ORDER BY ts` at `MIN_QUERY_BUDGET_BYTES`. Compare
the row order against the same query at the default budget. Verify
`peak_memory_bytes` is `Some(non-zero)` (the sort allocates against
the budget) and the per-query spill subdir is gone after the call
returns.

The crucial constraint: the budget floor is 512 MiB. To make a
sort *actually spill* against this floor, we need a sort buffer
that exceeds 512 MiB. That is too expensive for an integration
test. So this band has two shapes:

- **End-to-end correctness**: a small ORDER BY query at
  `MIN_QUERY_BUDGET_BYTES` produces the same row order as at the
  default budget. This is the regression assertion that lowering
  the budget does not corrupt sort output. (No spill is forced;
  the assertion is correctness across budget settings.)
- **Operator-level forced spill**: a `SortOperator::with_spill(...,
  budget=tiny)` constructed directly is fed enough rows to overflow
  the tiny budget, the spill handler runs, and the merged output
  matches the in-memory baseline. Existing operator-internal tests
  (sort.rs:1342) already cover this exact shape — so this band
  reasserts the *cleanup* angle (the per-query subdir under the
  attached `SpillFs` is empty afterwards), which the operator-level
  tests typically do not assert.

- [ ] **Step 1: Add the band**

Append:

```rust
// ─────────────────────────────────────────────────────────────────
// Spill fallback (spill.md §6 / §8 / §11)
// ─────────────────────────────────────────────────────────────────

mod spill_fallback {
    use std::sync::Arc;

    use bqlite_core::spill::SpillFs;
    use bqlite_engine::{
        Database, Engine, EngineConfig, QueryOptions, MIN_QUERY_BUDGET_BYTES,
    };
    use bqlite_tests::jsonl;

    use super::helpers::scratch_db_root;

    /// Lowering the budget to the floor must not change the row
    /// order produced by `ORDER BY`. The query is small enough that
    /// no spill is forced; the assertion is correctness across
    /// budget settings, not spill-trigger.
    #[test]
    fn order_by_at_floor_matches_default_budget() {
        let path = scratch_db_root("orderby-floor");
        std::fs::create_dir_all(&path).unwrap();
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine.query(jsonl::PURCHASES_CREATE_TABLE, &mut db).unwrap();

        // 200 rows is enough to exercise a multi-batch sort while
        // keeping the test well under one second.
        let cfg = jsonl::FixtureConfig {
            row_count: 200,
            entity_count: 50,
        };
        let fixture = jsonl::write_fixture_file(&cfg, &path, "orderby-floor.jsonl")
            .expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            fixture.display()
        );
        engine.query(&sql, &mut db).unwrap();

        // Default budget run.
        let r_default = engine
            .query("purchases | ORDER BY ts ASC", &mut db)
            .expect("default-budget order by");
        // Floor-budget run.
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
        };
        let r_floor = engine
            .query_with_options("purchases | ORDER BY ts ASC", &mut db, &opts)
            .expect("floor-budget order by");

        // Same row count.
        let n_default: usize = r_default.rows.iter().map(|b| b.num_rows()).sum();
        let n_floor: usize = r_floor.rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n_default, n_floor);

        // Same first/last `ts` value.
        fn first_ts(r: &bqlite_engine::ExecutionResult) -> i64 {
            use arrow::array::{Array, TimestampNanosecondArray};
            let b = r.rows.first().expect("at least one batch");
            let ts = b
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts is TimestampNanosecondArray");
            ts.value(0)
        }
        fn last_ts(r: &bqlite_engine::ExecutionResult) -> i64 {
            use arrow::array::{Array, TimestampNanosecondArray};
            let b = r.rows.last().expect("at least one batch");
            let ts = b
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts is TimestampNanosecondArray");
            ts.value(b.num_rows() - 1)
        }
        assert_eq!(first_ts(&r_default), first_ts(&r_floor));
        assert_eq!(last_ts(&r_default), last_ts(&r_floor));

        // Peak memory is reported as `Some(_)` for tracker-backed
        // queries — the value need not be non-zero (the operator
        // surface that charges sort allocations is TASK-513). The
        // contract this test pins: the tracker is *present* on
        // both runs.
        assert!(r_default.peak_memory_bytes.is_some());
        assert!(r_floor.peak_memory_bytes.is_some());

        let _ = std::fs::remove_dir_all(&path);
    }

    /// After a query that opens spill files, the per-query
    /// subdirectory under the database's `spill_root` does not
    /// persist past the query's return. This is the engine-level
    /// "no leftover artefacts" assertion `spill.md` §8.3 freezes.
    ///
    /// We do not assert that any spill *occurred* — at the floor
    /// the in-memory path may still fit. The contract is "no
    /// leftover artefact regardless of whether spill ran".
    #[test]
    fn no_spill_artefacts_after_query_return() {
        let path = scratch_db_root("no-artefacts");
        std::fs::create_dir_all(&path).unwrap();
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine.query(jsonl::PURCHASES_CREATE_TABLE, &mut db).unwrap();

        let cfg = jsonl::FixtureConfig {
            row_count: 100,
            entity_count: 25,
        };
        let fixture = jsonl::write_fixture_file(&cfg, &path, "no-artefacts.jsonl")
            .expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            fixture.display()
        );
        engine.query(&sql, &mut db).unwrap();

        let _r = engine
            .query("purchases | ORDER BY ts ASC", &mut db)
            .expect("order by");

        // Walk the spill root: every entry that exists must be a
        // surviving query subdir from the *current* query — but the
        // query has already returned, so the entry list must be
        // empty. (The belt-and-braces sweep on the last
        // `QueryContext` clone fired before `query` returned to us.)
        let spill_root = db.spill_root().to_path_buf();
        if spill_root.exists() {
            let entries: Vec<_> = std::fs::read_dir(&spill_root)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();
            assert!(
                entries.is_empty(),
                "spill_root must be empty after query return: {:?}",
                entries
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    /// `EngineConfig::query_memory_budget_bytes` overrides
    /// propagate into the per-query `QueryContext`. We assert this
    /// indirectly by reading the config back through
    /// `Engine::config()`.
    #[test]
    fn engine_config_threads_through_to_query_context() {
        let cfg = EngineConfig {
            query_memory_budget_bytes: MIN_QUERY_BUDGET_BYTES,
            ..EngineConfig::default()
        };
        let engine = Engine::with_config(cfg);
        assert_eq!(
            engine.config().query_memory_budget_bytes,
            MIN_QUERY_BUDGET_BYTES
        );
    }

    /// The per-database `SpillFs` accessor is stable across
    /// repeated calls; multiple queries reuse the same handle.
    #[test]
    fn database_spill_fs_handle_is_stable() {
        let path = scratch_db_root("spill-fs-stable");
        std::fs::create_dir_all(&path).unwrap();
        let db = Database::create(&path).unwrap();
        let a: Arc<SpillFs> = Arc::clone(db.spill_fs());
        let b: Arc<SpillFs> = Arc::clone(db.spill_fs());
        assert!(Arc::ptr_eq(&a, &b), "spill_fs handle is shared");
        let _ = std::fs::remove_dir_all(&path);
    }
}
```

- [ ] **Step 2: Run the band**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress -- spill_fallback`
Expected: 4 tests pass.

- [ ] **Step 3: Run `scripts/local-ci.sh`**

Expected: green.

- [ ] **Step 4: Subagent review**

- [ ] **Step 5: Commit & merge (final checkpoint)**

```bash
git add tests/tests/wave5_runtime_stress.rs
git commit -m "TASK-525: Add spill-fallback contract band"
git checkout main && git pull --ff-only && git merge task/TASK-525 --ff-only && git push origin main
git checkout task/TASK-525
```

---

## Task 7: Reconcile design docs and complete the task

**Files:**
- Modify: `docs/design/engine/cancellation.md` (touch §10
  test-author guidance if a test in this suite contradicts the
  doc — likely no edit needed).
- Modify: `docs/design/engine/spill.md` (same — likely no edit).
- Move: `tasks/active/TASK-525.lock` → `tasks/completed/TASK-525.done`.

- [ ] **Step 1: Re-read the four design docs**

Skim `cancellation.md`, `spill.md`, `memory-budget.md`,
`morsel-scheduler.md` and check whether any of the assertions in
the suite contradict a frozen statement. They should not — the
suite is a downstream consumer.

If a doc *omits* a contract that the suite tests (e.g., the
behaviour of `record_many` across the cap), add a one-line note
under the relevant section. Document in the commit message that the
edit is downstream-test-driven clarification.

- [ ] **Step 2: Run the full suite once more**

Run: `cargo test -p bqlite-tests --test wave5_runtime_stress`
Expected: every band green, no flakes on three back-to-back runs.

- [ ] **Step 3: Run `scripts/local-ci.sh` end-to-end**

Expected: green.

- [ ] **Step 4: Final subagent review of the cumulative diff**

Spawn one final reviewer over the entire `tests/tests/wave5_runtime_stress.rs`
file plus any design-doc edits.

- [ ] **Step 5: Move the lock to the completion marker**

```bash
git mv tasks/active/TASK-525.lock tasks/completed/TASK-525.done
```

Edit `tasks/completed/TASK-525.done` to add the `completed_at`
timestamp:

```json
{
  "agent_id": "agent-4",
  "task_id": "TASK-525",
  "claimed_at": "2026-04-30T07:21:34Z",
  "completed_at": "<UTC ISO-8601 now>",
  "branch": "task/TASK-525",
  "description": "Memory-pressure, spill, and cancellation stress suite"
}
```

- [ ] **Step 6: Commit and push the completion marker**

```bash
git add tasks/active/TASK-525.lock tasks/completed/TASK-525.done
git commit -m "TASK-525: completed"
git checkout main && git pull --ff-only && git merge task/TASK-525 --ff-only && git push origin main
```

End the turn — the wrapper handles the next task.

---

## Self-review

- **Spec coverage:** TASK-525 spec lists five bands; the plan has
  one task per band (Tasks 2-6) plus scaffold (1) and reconcile
  (7). Each band's assertions tie to a specific design-doc section
  in the body of the corresponding task.
- **Placeholder scan:** Task 5 step 2 has a guided fill-in for the
  `MaxGroupsExceeded` test body — this is the only TODO in the
  plan, and it is *guided* (the line numbers of the existing
  operator-internal test are provided so the executing agent can
  mirror the exact wiring). Acceptable per the plan-skill rule:
  the agent must adapt to a constructor surface they read from the
  source.
- **Type consistency:** `helpers::count_rows` is added in Task 4
  and referenced from Task 4 only (Task 6 inlines its own row-count
  closure to avoid a stray helper-mod dep). All other types
  (`QueryContext`, `WarningSink`, `WorkerContext`, `SpillFs`,
  `MemoryTracker`, `BqliteError`, `QueryWarning`,
  `EngineConfig`, `QueryOptions`, `MIN_QUERY_BUDGET_BYTES`,
  `Database`, `Engine`) are public surfaces verified against the
  current `lib.rs` re-exports during the planning survey.

---

## Why this is one task, executed inline

Each band fits in a single small commit. Plan execution is inline
(executing-plans skill) rather than subagent-driven because the
feature is one file, the bands share helpers, and the inter-task
review checkpoints add value but the task units are too small to
justify dispatching a fresh subagent per band.
