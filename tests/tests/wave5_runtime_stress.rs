//! TASK-525: Wave 5 runtime stress suite.
//!
//! Locks in the runtime contracts the design docs §§ below freeze
//! for Wave 5. The suite is split into five bands, one `mod` each:
//!
//! - `budget_exhaustion`     — `engine/memory-budget.md` § 4 / § 7
//! - `spill_fallback`        — `engine/spill.md` § 6 / § 8 / § 11
//! - `cancellation_cleanup`  — `engine/cancellation.md` § 5.1 / § 5.2
//! - `snapshot_isolation`    — `storage/deletes.md` § 9
//! - `warning_overflow`      — `engine/cancellation.md` § 7.2 / § 7.3
//!
//! Bands marked "contract level" run against the runtime types
//! directly (`QueryContext`, `WarningSink`, `SpillFs`) where the
//! public `Engine::query` surface does not yet expose a trigger —
//! see the plan in
//! `docs/superpowers/plans/2026-04-30-task-525-runtime-stress-suite.md`
//! § "Scope notes" for the rationale.
//!
//! Out of scope today (deferred to TASK-528 acceptance gate / Wave 6):
//!
//! - **Public timeout API** — `Engine` does not expose a per-query
//!   timeout knob today (`cancellation.md` §6.2). The cancellation
//!   tests use `QueryContext::cancellation()` directly.
//! - **End-to-end `MaxGroupsExceeded` through `Engine::query`** —
//!   `DEFAULT_MAX_GROUPS = 1_000_000` is hardcoded in the planner;
//!   suite-level coverage is non-additive against the operator-level
//!   tests in `crates/bqlite-operators/src/aggregate/mod.rs::tests`.
//! - **Ingest partitioner spill** — covered in `spill_fallback::ingest_partitioner_spill`.
//! - **Per-`(worker, morsel)` `catch_unwind`** — TASK-541 follow-on.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(dead_code)]
mod helpers {
    use super::*;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    /// Allocate a unique scratch directory under the OS temp dir.
    /// Each test gets its own to avoid cross-test interference and
    /// to keep `Database::create` happy (it expects a non-existent
    /// or empty path).
    pub fn scratch_db_root(label: &str) -> PathBuf {
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut p = std::env::temp_dir();
        p.push(format!("bqlite-task525-{label}-{pid}-{seq}"));
        p
    }

    /// Total row count across every batch in an `ExecutionResult`.
    pub fn count_rows(result: &bqlite_engine::ExecutionResult) -> usize {
        result.rows.iter().map(|b| b.num_rows()).sum()
    }
}

// ─────────────────────────────────────────────────────────────────
// Warning-channel overflow (cancellation.md §7.2 / §7.3)
// ─────────────────────────────────────────────────────────────────

mod warning_overflow {
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
        let extra = 7usize;
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
    /// must equal `total_recorded - cap` exactly.
    #[test]
    fn concurrent_producers_respect_cap_aggregate() {
        let cap = WorkerContext::PER_WORKER_WARNING_CAP;
        let producers = 8usize;
        let per_producer = (cap / producers) + 50;
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
        assert_eq!(warnings.len(), cap + 1);
        match warnings.last().unwrap() {
            QueryWarning::WarningsOverflow { suppressed_count } => {
                assert_eq!(
                    *suppressed_count as usize,
                    total - cap,
                    "exact suppressed-count arithmetic under contention"
                );
            }
            other => panic!("expected trailing WarningsOverflow, got {other:?}"),
        }
    }

    /// `into_warnings` consumes the buffer; reading a clone after
    /// the drain returns an empty list. Idempotence rule from
    /// `warning_sink.rs::into_warnings`.
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
    /// Drives `cap + 500` warnings in batches of 100 and verifies
    /// the kept count is exactly the cap.
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
        assert_eq!(warnings.len(), cap + 1);
        match warnings.last().unwrap() {
            QueryWarning::WarningsOverflow { suppressed_count } => {
                assert_eq!(*suppressed_count as usize, total - cap);
            }
            other => panic!("expected trailing WarningsOverflow, got {other:?}"),
        }
    }
}

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

    /// The belt-and-braces sweep in `spill.md` §8.3 (cross-referenced
    /// from `cancellation.md` §5.2) runs on the *last* `QueryContext`
    /// clone's drop. While any clone is alive the per-query subdir
    /// must persist, including any leaked `TempSpillFile` paths.
    ///
    /// Re-asserted at the binary-test level (the engine-internal
    /// unit test at `context.rs::dropping_last_clone_runs_belt_and_braces_cleanup`
    /// covers the same shape; suite-level coverage here so a refactor
    /// that drops the unit test does not silently lose the contract).
    #[test]
    fn last_clone_drop_reclaims_per_query_subdir_with_leaked_file() {
        let db_root = scratch_db_root("leak-cleanup");
        std::fs::create_dir_all(&db_root).unwrap();
        let spill_root = db_root.join("spill");
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();

        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let qid = ctx.spill_query_id().expect("qid attached");

        // Open and *forget* a guard — simulates a leaked
        // `TempSpillFile` whose `Drop` never runs.
        let guard = ctx
            .open_spill("sort-run")
            .expect("fs attached")
            .expect("opens");
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
        std::fs::create_dir_all(&db_root).unwrap();
        let spill_root = db_root.join("spill");
        let fs = SpillFs::open(spill_root.clone(), &db_root).unwrap();

        let ctx_a = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let ctx_b = QueryContext::new(MIN_QUERY_BUDGET_BYTES).with_spill_fs(Arc::clone(&fs));
        let qid_a = ctx_a.spill_query_id().unwrap();
        let qid_b = ctx_b.spill_query_id().unwrap();
        assert_ne!(qid_a, qid_b, "each query gets a distinct id");

        let g_a = ctx_a.open_spill("sort-run").unwrap().unwrap();
        let g_b = ctx_b.open_spill("sort-run").unwrap().unwrap();
        let p_a = g_a.path().to_path_buf();
        let p_b = g_b.path().to_path_buf();
        std::mem::forget(g_a);
        std::mem::forget(g_b);

        drop(ctx_a);
        assert!(!p_a.exists(), "A's subdir reclaimed");
        assert!(p_b.exists(), "B's subdir untouched");

        drop(ctx_b);
        assert!(!p_b.exists(), "B's subdir reclaimed at last drop");
        let _ = std::fs::remove_dir_all(&db_root);
    }

    /// `unbounded()` contexts have no `SpillFs` attached; the cleanup
    /// guard does not exist, so nothing fires on drop.
    #[test]
    fn unbounded_context_has_no_spill_attachment() {
        let ctx = QueryContext::unbounded();
        assert!(ctx.spill_fs().is_none());
        assert!(ctx.spill_query_id().is_none());
        assert!(ctx.open_spill("any").is_none());
    }
}

// ─────────────────────────────────────────────────────────────────
// Snapshot isolation & multi-engine concurrency
// (deletes.md §9, manifest.rs::snapshot_for_query)
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
    /// the same `Arc<Mutex<()>>` on repeat calls. Pins the per-shard
    /// serialization primitive from `deletes.md` §9.
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

    /// Two threads cannot hold the same `(window, shard)` lock
    /// simultaneously. The observed-overlap counter must stay at
    /// most 1.
    #[test]
    fn tombstone_shard_lock_serializes_contended_writers() {
        let path = scratch_db_root("tombstone-lock-serial");
        let db = Database::create(&path).unwrap();
        let lock = db.tombstone_shard_lock("events", 0, 0);

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

    /// `snapshot_for_query` against an empty table returns an empty
    /// segment list, and back-to-back snapshots agree. Pins the
    /// "snapshot is a pure read; no I/O" rule from `deletes.md` §6.
    #[test]
    fn snapshot_for_query_returns_empty_on_empty_table() {
        let path = scratch_db_root("snapshot-empty");
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine.query(CREATE_TABLE, &mut db).unwrap();

        let s1 = db
            .snapshot_for_query("events", TimeRange::unbounded(), 0)
            .expect("snapshot");
        assert!(s1.is_empty(), "fresh table has no segments");
        let s2 = db
            .snapshot_for_query("events", TimeRange::unbounded(), 0)
            .expect("snapshot");
        assert!(s2.is_empty(), "back-to-back snapshots agree");
        let _ = std::fs::remove_dir_all(&path);
    }

    /// A DELETE between two `Engine::query` runs is observable in
    /// the second result. With 25 rows / 5 entities, user_0 owns
    /// exactly 5 rows (cyclic entity assignment in
    /// `bqlite_tests::jsonl`); the post-count is `pre_count - 5`.
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

        let cfg = jsonl::FixtureConfig {
            row_count: 25,
            entity_count: 5,
        };
        let jsonl_path =
            jsonl::write_fixture_file(&cfg, &path, "fixture.jsonl").expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            jsonl_path.display()
        );
        engine.query(&sql, &mut db).expect("ingest");

        let pre = engine.query("purchases", &mut db).expect("first query");
        let pre_count = super::helpers::count_rows(&pre);
        assert_eq!(pre_count, 25, "fixture produced 25 rows");

        let _ = engine
            .query("DELETE FROM purchases WHERE user_id = 'user_0'", &mut db)
            .expect("delete");

        let post = engine.query("purchases", &mut db).expect("second query");
        let post_count = super::helpers::count_rows(&post);
        assert_eq!(
            post_count, 20,
            "second query observes the delete of user_0's 5 rows: \
             pre={pre_count}, post={post_count}"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Two `Engine` instances on two independent `Database` paths
    /// run data-plane queries simultaneously without the morsel
    /// scheduler deadlocking. Per-engine permits return to full
    /// capacity after both threads join.
    #[test]
    fn two_engines_run_concurrent_queries_without_deadlock() {
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
            let jsonl_path =
                jsonl::write_fixture_file(&cfg_fix, p, "fixture.jsonl").expect("write fixture");
            let sql = format!(
                "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
                jsonl_path.display()
            );
            engine.query(&sql, &mut db).unwrap();
        }

        let h_a = thread::spawn(move || {
            let mut db = Database::open(&p_a).unwrap();
            let engine = Engine::with_config(cfg);
            let r = engine
                .query("purchases | ORDER BY ts ASC", &mut db)
                .unwrap();
            let n = super::helpers::count_rows(&r);
            (engine, p_a, n)
        });
        let h_b = thread::spawn(move || {
            let mut db = Database::open(&p_b).unwrap();
            let engine = Engine::with_config(cfg);
            let r = engine
                .query("purchases | ORDER BY ts ASC", &mut db)
                .unwrap();
            let n = super::helpers::count_rows(&r);
            (engine, p_b, n)
        });

        let (engine_a, path_a, n_a) = h_a.join().expect("thread A");
        let (engine_b, path_b, n_b) = h_b.join().expect("thread B");

        assert_eq!(n_a, 50);
        assert_eq!(n_b, 50);
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

// ─────────────────────────────────────────────────────────────────
// Hard-budget exhaustion (memory-budget.md §4 / §7)
//
// Other exhaustion paths are covered elsewhere:
//
//   - MemoryBudgetExceeded at the tracker / context level:
//     `crates/bqlite-core/src/memory.rs::tests` and
//     `crates/bqlite-engine/src/context.rs::tracked_context_overflow_surfaces_typed_error`.
//   - MaxGroupsExceeded at the operator level:
//     `crates/bqlite-operators/src/aggregate/mod.rs::tests::max_groups_exceeded_returns_error`
//     and `crates/bqlite-operators/src/distinct.rs` if present.
//
// The suite-level assertion below pins the wrapper-path behaviour:
// reading `QueryContext::memory()` and exhausting the budget through
// the trait surface surfaces the typed variant. This is the path
// the CLI / Python bindings observe.
// ─────────────────────────────────────────────────────────────────

mod budget_exhaustion {
    use bqlite_core::BqliteError;
    use bqlite_engine::{QueryContext, MIN_QUERY_BUDGET_BYTES};

    /// Reading `QueryContext::memory()` and reserving past its
    /// budget surfaces `MemoryBudgetExceeded` with the structured
    /// `(used, budget)` fields.
    #[test]
    fn query_context_memory_overshoot_surfaces_typed_variant() {
        let ctx = QueryContext::new(MIN_QUERY_BUDGET_BYTES);
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

// ─────────────────────────────────────────────────────────────────
// Spill fallback (spill.md §6 / §8 / §11)
// ─────────────────────────────────────────────────────────────────

mod spill_fallback {
    use std::sync::Arc;

    use bqlite_core::spill::SpillFs;
    use bqlite_engine::{Database, Engine, EngineConfig, QueryOptions, MIN_QUERY_BUDGET_BYTES};
    use bqlite_tests::jsonl;

    use super::helpers::scratch_db_root;

    /// Lowering the budget to the floor must not change the row
    /// order produced by `ORDER BY`. The query is small enough that
    /// no spill is forced; the assertion is correctness across
    /// budget settings.
    #[test]
    fn order_by_at_floor_matches_default_budget() {
        let path = scratch_db_root("orderby-floor");
        std::fs::create_dir_all(&path).unwrap();
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine
            .query(jsonl::PURCHASES_CREATE_TABLE, &mut db)
            .unwrap();

        let cfg = jsonl::FixtureConfig {
            row_count: 200,
            entity_count: 50,
        };
        let fixture =
            jsonl::write_fixture_file(&cfg, &path, "orderby-floor.jsonl").expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            fixture.display()
        );
        engine.query(&sql, &mut db).unwrap();

        let r_default = engine
            .query("purchases | ORDER BY ts ASC", &mut db)
            .expect("default-budget order by");
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
            ..QueryOptions::default()
        };
        let r_floor = engine
            .query_with_options("purchases | ORDER BY ts ASC", &mut db, &opts)
            .expect("floor-budget order by");

        let n_default = super::helpers::count_rows(&r_default);
        let n_floor = super::helpers::count_rows(&r_floor);
        assert_eq!(n_default, n_floor);

        // Pull the `ts` column from the first/last non-empty batch.
        // Empty mid-stream batches are legal per the PhysicalOperator
        // contract; both endpoints scan past them.
        fn endpoint_ts(r: &bqlite_engine::ExecutionResult, last: bool) -> i64 {
            use arrow::array::{Array, TimestampNanosecondArray};
            let iter: Box<dyn Iterator<Item = _>> = if last {
                Box::new(r.rows.iter().rev())
            } else {
                Box::new(r.rows.iter())
            };
            for batch in iter {
                if batch.num_rows() == 0 {
                    continue;
                }
                let ts = batch
                    .column_by_name("ts")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("ts is TimestampNanosecondArray");
                let idx = if last { batch.num_rows() - 1 } else { 0 };
                return ts.value(idx);
            }
            unreachable!("non-empty result must have at least one non-empty batch");
        }
        assert_eq!(endpoint_ts(&r_default, false), endpoint_ts(&r_floor, false));
        assert_eq!(endpoint_ts(&r_default, true), endpoint_ts(&r_floor, true));

        // `peak_memory_bytes` is `Some(_)` on tracker-backed runs.
        // The value need not be non-zero — operator-side wiring
        // (TASK-512/513/514) is still landing; the contract this
        // test pins is "the tracker is *present* on both runs."
        // See `crates/bqlite-engine/src/query.rs:91-98`.
        assert!(r_default.peak_memory_bytes.is_some());
        assert!(r_floor.peak_memory_bytes.is_some());

        let _ = std::fs::remove_dir_all(&path);
    }

    /// After a query that may have opened spill files, the per-query
    /// subdirectory under the database's `spill_root` does not
    /// persist past the query's return.
    #[test]
    fn no_spill_artefacts_after_query_return() {
        let path = scratch_db_root("no-artefacts");
        std::fs::create_dir_all(&path).unwrap();
        let mut db = Database::create(&path).unwrap();
        let engine = Engine::new();
        engine
            .query(jsonl::PURCHASES_CREATE_TABLE, &mut db)
            .unwrap();

        let cfg = jsonl::FixtureConfig {
            row_count: 100,
            entity_count: 25,
        };
        let fixture =
            jsonl::write_fixture_file(&cfg, &path, "no-artefacts.jsonl").expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            fixture.display()
        );
        engine.query(&sql, &mut db).unwrap();

        let _r = engine
            .query("purchases | ORDER BY ts ASC", &mut db)
            .expect("order by");

        let spill_root = db.spill_root().to_path_buf();
        if spill_root.exists() {
            let entries: Vec<_> = std::fs::read_dir(&spill_root)
                .unwrap()
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .collect();
            assert!(
                entries.is_empty(),
                "spill_root must be empty after query return: {entries:?}"
            );
        }
        let _ = std::fs::remove_dir_all(&path);
    }

    /// `EngineConfig::query_memory_budget_bytes` overrides
    /// propagate into the engine's stored configuration.
    #[test]
    fn engine_config_threads_through_to_engine_state() {
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

    /// The per-database `SpillFs` accessor returns a stable
    /// `Arc<SpillFs>` — multiple queries on the same database share
    /// the same handle.
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

    /// Drives the ingest partitioner past its memory budget so spill
    /// files are created, verifies that `drain_sorted` merges them in
    /// `(entity_id, ts)` order across the spilled-vs-resident boundary,
    /// and confirms no spill artefacts persist after drain completes.
    ///
    /// Pins `docs/design/engine/spill.md` § 6.2 / § 8 at the suite
    /// level (TASK-539 closure). The full end-to-end correctness check
    /// — spill → `write_partitioner` → analytical query — lives in
    /// `wave5_acceptance.rs::ingest_spill_produces_correct_queryable_data`.
    #[test]
    fn ingest_partitioner_spill() {
        use bqlite_core::event::{EntityId, Event};
        use bqlite_core::time::Timestamp;
        use bqlite_storage::ingest::partitioner::Partitioner;

        let db_root = scratch_db_root("ingest-partitioner-spill");
        std::fs::create_dir_all(&db_root).unwrap();
        let spill_dir = db_root.join("spill");
        std::fs::create_dir_all(&spill_dir).unwrap();

        // Anchor timestamp inside a single 30-day window so all events
        // land in one bucket per shard (deterministic multi-shard spread).
        // Same T0 as the Wave 5 fixture so the shard distribution matches.
        const T0: i64 = 1_700_000_000_000_000_000;
        const S: i64 = 1_000_000_000; // 1 second in nanoseconds

        // 2 KiB budget forces spill after the first ~17-20 events for
        // minimal-property events (each Event is ~100-120 bytes per
        // `estimated_event_size`). Enough to exercise multiple spill
        // cycles without making the test slow.
        let tight_budget = 2048_usize;

        // 200 events across 50 entities in scrambled insertion order
        // so the merge has real cross-entity interleaving to handle.
        // Outer loop = event index so events for the same entity arrive
        // separated by events for other entities.
        let n_entities = 50_usize;
        let events_per_entity = 4_usize;
        let mut all_events: Vec<Event> = Vec::with_capacity(n_entities * events_per_entity);
        for ev_idx in 0..events_per_entity {
            for e_id in 0..n_entities {
                let entity = EntityId::from(format!("user_{e_id:04}"));
                let ts = Timestamp::from_nanos(T0 + (ev_idx as i64) * S);
                all_events.push(Event::new(entity, ts, "click"));
            }
        }

        // Reference: unlimited-budget no-spill drain.
        let mut ref_p = Partitioner::new(32, 30, 1, usize::MAX).unwrap();
        for e in &all_events {
            ref_p.push_event(e.clone()).unwrap();
        }
        let reference: Vec<_> = ref_p.drain_sorted().collect();

        // Spilling path: tight budget forces multiple spill cycles.
        let mut p =
            Partitioner::with_spill_dir(32, 30, 1, tight_budget, spill_dir.clone()).unwrap();
        for e in &all_events {
            p.push_event(e.clone()).unwrap();
        }
        assert!(
            p.spilled_run_count() > 0,
            "tight budget must trigger at least one spill; got runs={}",
            p.spilled_run_count()
        );

        let actual: Vec<_> = p.drain_sorted().collect();

        // (a) Ordering: every bucket must be sorted by (entity_id, ts)
        //     across the spilled-vs-resident boundary.
        for (key, events) in &actual {
            for w in events.windows(2) {
                let a = (&w[0].entity, &w[0].timestamp);
                let b = (&w[1].entity, &w[1].timestamp);
                assert!(
                    a <= b,
                    "bucket {key:?}: (entity_id, ts) ordering violated \
                     after spill+merge (spill.md §6.2)"
                );
            }
        }

        // (b) Content: spill+merge must produce the same output as the
        //     unlimited-budget no-spill drain on identical input.
        assert_eq!(
            actual.len(),
            reference.len(),
            "bucket count must match across spill and no-spill drain"
        );
        for ((ka, va), (kb, vb)) in actual.iter().zip(reference.iter()) {
            assert_eq!(ka, kb, "bucket keys diverged after spill+merge");
            assert_eq!(
                va, vb,
                "events diverged for bucket {ka:?} after spill+merge"
            );
        }

        // (c) Cleanup: drain removes all spill artefacts via
        //     `SpillRunFile` RAII (spill.md §8.1 / §8.3).
        let leftover: Vec<_> = std::fs::read_dir(&spill_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".spill"))
            .collect();
        assert!(
            leftover.is_empty(),
            "no spill artefacts must remain after drain; found {leftover:?}"
        );

        let _ = std::fs::remove_dir_all(&db_root);
    }
}
