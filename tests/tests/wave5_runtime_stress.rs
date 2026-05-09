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
//! - **End-to-end `MaxGroupsExceeded` through `Engine::query`** —
//!   `DEFAULT_MAX_GROUPS = 1_000_000` is hardcoded in the planner;
//!   suite-level coverage is non-additive against the operator-level
//!   tests in `crates/bqlite-operators/src/aggregate/mod.rs::tests`.
//! - **Ingest partitioner spill** — covered in `spill_fallback::ingest_partitioner_spill`.
//! - **Per-morsel iteration `catch_unwind`** — TASK-541 follow-on; the
//!   `MorselScheduler::submit` worker boundary catches panics today
//!   (cancellation.md §4.1; landed in TASK-538 CP1.3).

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

    /// Build a database with a multi-shard `events` fixture sized for
    /// long-enough scans to be observable. Used by the public-API
    /// cancellation tests (TASK-538) — the fixture is large enough
    /// that an `ORDER BY ts ASC` scan does observable work, but small
    /// enough to keep test runtime reasonable.
    ///
    /// Caller owns the returned `TempDb`; dropping it removes the
    /// temp directory.
    pub fn build_long_query_db(
        label: &str,
    ) -> (
        bqlite_tests::common::TempDb,
        bqlite_storage::Database,
        bqlite_engine::Engine,
    ) {
        use std::sync::Arc;

        use arrow::array::{StringArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use bqlite_engine::Engine;
        use bqlite_storage::Database;
        use bqlite_tests::common::TempDb;
        use parquet::arrow::ArrowWriter;

        const N_ENTITIES: usize = 5_000;
        const EVENTS_PER_ENTITY: usize = 10;
        const T0: i64 = 1_700_000_000_000_000_000;
        const S: i64 = 1_000_000_000;

        let tmp = TempDb::new();
        let mut db = Database::create(tmp.path())
            .unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
        let engine = Engine::new();
        engine
            .query(
                "CREATE TABLE events (\
                     entity_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("event_type", DataType::Utf8, false),
        ]));
        let mut entity_ids: Vec<String> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
        let mut tss: Vec<i64> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
        let mut event_types: Vec<&str> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
        for i in 0..N_ENTITIES {
            let eid = format!("user_{i:05}");
            for k in 0..EVENTS_PER_ENTITY {
                entity_ids.push(eid.clone());
                tss.push(T0 + (k as i64) * S);
                event_types.push(if k % 3 == 0 {
                    "view"
                } else if k % 3 == 1 {
                    "add_to_cart"
                } else {
                    "purchase"
                });
            }
        }
        let entity_id_array =
            StringArray::from(entity_ids.iter().map(String::as_str).collect::<Vec<_>>());
        let ts_array = TimestampNanosecondArray::from(tss);
        let event_type_array = StringArray::from(event_types);
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(entity_id_array),
                Arc::new(ts_array),
                Arc::new(event_type_array),
            ],
        )
        .expect("build fixture batch");

        let path = tmp.path().join(format!("{label}-fixture.parquet"));
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("ArrowWriter");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let sql = format!(
            "INSERT INTO events FROM '{}' WITH (format: 'parquet')",
            path.display()
        );
        engine
            .query(&sql, &mut db)
            .unwrap_or_else(|e| panic!("[{label}] INSERT FROM Parquet: {e}"));
        (tmp, db, engine)
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

    // ────────────────────────────────────────────────────────────
    // Public-surface tests (TASK-538): drive cancellation through
    // `Engine::query_with_options` and the new `QueryOptions`
    // surface rather than the contract-level `QueryContext`.
    // ────────────────────────────────────────────────────────────

    use bqlite_core::BqliteError;
    use bqlite_engine::{CancellationToken, ExecutionFailure, QueryOptions};

    use super::helpers::build_long_query_db;

    /// External cancel before the query starts produces
    /// `BqliteError::Cancelled` from the very first yield point.
    /// Deterministic: the token is pre-cancelled so any operator
    /// poll observes the flag immediately. Public-API counterpart of
    /// the contract-level `cancel_propagates_through_context_clones`.
    #[test]
    fn external_cancel_pre_query_returns_cancelled() {
        let (_tmp, mut db, engine) = build_long_query_db("ext-pre");
        let token = CancellationToken::new();
        token.cancel();
        let opts = QueryOptions {
            cancel: Some(token),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options("events | ORDER BY ts ASC", &mut db, &opts)
            .expect_err("pre-cancelled token must fire on first yield");
        assert!(
            matches!(
                &err,
                ExecutionFailure {
                    error: BqliteError::Cancelled,
                    ..
                }
            ),
            "expected Cancelled, got {err:?}"
        );
    }

    /// Per-query timeout fires deterministically when set to
    /// `Duration::ZERO` — `QueryTimer::spawn` synchronously calls
    /// `cancel_with_reason(Timeout)` before the query starts, so the
    /// driver maps the resulting `Cancelled` to `Timeout` at result
    /// collection (cancellation.md §3.1). No retry loop, no race.
    #[test]
    fn timeout_zero_duration_returns_timeout_deterministically() {
        let (_tmp, mut db, engine) = build_long_query_db("timeout-mid");
        let opts = QueryOptions {
            timeout: Some(std::time::Duration::ZERO),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options("events | ORDER BY ts ASC", &mut db, &opts)
            .expect_err("zero-duration timeout must fire");
        match err {
            ExecutionFailure {
                error: BqliteError::Timeout { .. },
                ..
            } => {}
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    /// Cancel mid-query through the public API leaves no spill
    /// artefacts under the database's spill root. The engine
    /// constructs (and drops) a fresh `QueryContext` per call that
    /// may have lazily created a per-query subdir; on every exit
    /// path — natural completion or external cancel —
    /// `SpillCleanup::Drop` on the last `QueryContext` clone reclaims
    /// the per-query subdir, leaving the spill root empty.
    ///
    /// Unlike the `Duration::ZERO` timeout (which fires before any
    /// operator runs), a pre-cancelled token deterministically lets
    /// bind run and the operator tree open before the very first
    /// yield-point check fires Cancelled — exercising the "operator
    /// state opened, then unwound" exit path. Re-pins
    /// `cancellation.md` § 5.1 / § 5.2 / `spill.md` § 8.3 through
    /// the public surface.
    #[test]
    fn cancel_via_query_options_leaves_no_spill_artefacts() {
        use bqlite_engine::MIN_QUERY_BUDGET_BYTES;

        let (_tmp, mut db, engine) = build_long_query_db("cancel-cleanup");
        let spill_root = db.spill_fs().root().to_path_buf();

        for _ in 0..16 {
            let token = CancellationToken::new();
            token.cancel();
            let opts = QueryOptions {
                cancel: Some(token),
                memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
                ..QueryOptions::default()
            };
            let result = engine.query_with_options("events | ORDER BY ts ASC", &mut db, &opts);
            assert!(
                matches!(
                    &result,
                    Err(ExecutionFailure {
                        error: BqliteError::Cancelled,
                        ..
                    })
                ),
                "expected Cancelled exit, got {result:?}"
            );
            // Spill root must be empty (or absent) after every call.
            // The per-query subdir is created lazily on first spill;
            // on the cancel exit path SpillCleanup::Drop reclaims it
            // before the engine returns.
            if spill_root.exists() {
                let entries: Vec<_> = std::fs::read_dir(&spill_root)
                    .expect("read spill root")
                    .filter_map(|e| e.ok())
                    .collect();
                assert!(
                    entries.is_empty(),
                    "spill root must be empty after cancel, found {} entries",
                    entries.len()
                );
            }
        }
    }

    /// Timeout exit through the public API runs the engine cleanup
    /// path successfully — `Engine::query_with_options` returns
    /// `BqliteError::Timeout` and no spill artefacts persist under
    /// the database's spill root.
    ///
    /// `Duration::ZERO` deterministically fires the timer before
    /// any operator runs (`QueryTimer::spawn` synchronous-fire
    /// branch). The complementary
    /// `cancel_via_query_options_leaves_no_spill_artefacts` exercises
    /// the operator-state-opened exit path. Together they pin
    /// "the public timeout / cancel surface returns cleanly without
    /// leaving artefacts" for every reachable interleaving today.
    #[test]
    fn timeout_via_query_options_exits_cleanly() {
        use bqlite_engine::MIN_QUERY_BUDGET_BYTES;

        let (_tmp, mut db, engine) = build_long_query_db("timeout-exit");
        let spill_root = db.spill_fs().root().to_path_buf();

        let opts = QueryOptions {
            timeout: Some(std::time::Duration::ZERO),
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
            ..QueryOptions::default()
        };
        let result = engine.query_with_options("events | ORDER BY ts ASC", &mut db, &opts);
        assert!(
            matches!(
                &result,
                Err(ExecutionFailure {
                    error: BqliteError::Timeout { .. },
                    ..
                })
            ),
            "expected Timeout exit, got {result:?}"
        );
        if spill_root.exists() {
            let entries: Vec<_> = std::fs::read_dir(&spill_root)
                .expect("read spill root")
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                entries.is_empty(),
                "spill root must be empty after timeout exit"
            );
        }
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
    use bqlite_engine::{Database, Engine, EngineConfig};
    use bqlite_tests::jsonl;

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

    // ─── helpers local to this mod ────────────────────────────────

    /// Create a fresh `purchases` database at `path`, ingest
    /// `row_count` rows across `entity_count` entities, and return
    /// the open `Database`.
    fn setup_purchases_db(
        path: &std::path::Path,
        engine: &Engine,
        row_count: u64,
        entity_count: u64,
    ) -> Database {
        std::fs::create_dir_all(path).unwrap();
        let mut db = Database::create(path).unwrap();
        engine
            .query(jsonl::PURCHASES_CREATE_TABLE, &mut db)
            .unwrap();
        let cfg = jsonl::FixtureConfig {
            row_count,
            entity_count,
        };
        let fixture_path =
            jsonl::write_fixture_file(&cfg, path, "fixture.jsonl").expect("write fixture");
        let sql = format!(
            "INSERT INTO purchases FROM '{}' WITH (format: 'jsonl')",
            fixture_path.display()
        );
        engine.query(&sql, &mut db).unwrap();
        db
    }

    /// Spawn a writer thread and a reader thread that race to acquire
    /// `Arc<Mutex<Database>>`.  Because `Engine::query` holds `&mut
    /// Database` for its entire duration, the two operations are
    /// serialized by the mutex — one completes fully before the other
    /// starts.  The function returns whichever count the reader
    /// observed.
    fn run_delete_query_race(db: Arc<Mutex<Database>>, engine: &Engine) -> usize {
        let db_w = Arc::clone(&db);
        let eng_w = engine.clone();
        let writer = thread::spawn(move || {
            let mut g = db_w.lock().expect("db mutex not poisoned");
            eng_w
                .query("DELETE FROM purchases WHERE user_id = 'user_0'", &mut g)
                .expect("DELETE must succeed");
        });

        let db_r = Arc::clone(&db);
        let eng_r = engine.clone();
        let reader = thread::spawn(move || {
            let mut g = db_r.lock().expect("db mutex not poisoned");
            let result = eng_r
                .query("purchases", &mut g)
                .expect("SELECT must succeed");
            super::helpers::count_rows(&result)
        });

        writer.join().expect("writer thread panicked");
        reader.join().expect("reader thread panicked")
    }

    /// DELETE / query ordering correctness on the **same** `Database`
    /// with `query_threads = 1`.
    ///
    /// Two threads race to acquire `Arc<Mutex<Database>>`:
    ///  - The **writer** deletes `user_0`'s rows.
    ///  - The **reader** fetches the total row count.
    ///
    /// Because `Engine::query` holds `&mut Database` for its entire
    /// duration, the mutex serializes the two operations — one
    /// completes before the other starts.  This verifies the ordering
    /// invariant from `deletes.md §9`: the result is always one of two
    /// consistent values (pre-delete 50 or post-delete 45), never an
    /// intermediate count.
    ///
    /// `query_threads = 1` routes all morsel work through a single
    /// Rayon worker, exercising the scheduler under the constraint that
    /// the cloned `Engine` instances share the same `Arc<MorselScheduler>`.
    #[test]
    fn delete_concurrent_with_query_on_same_db() {
        let path = scratch_db_root("del-conc-1t");
        let engine = Engine::with_config(EngineConfig {
            query_threads: Some(1),
            ..EngineConfig::default()
        });

        // 50 rows across 10 entities; user_0 owns rows at indices
        // 0,10,20,30,40 → 5 rows.
        let db = setup_purchases_db(&path, &engine, 50, 10);
        let pre_count: usize = 50;
        let post_count: usize = 45;

        let db = Arc::new(Mutex::new(db));
        let count = run_delete_query_race(Arc::clone(&db), &engine);

        assert!(
            count == pre_count || count == post_count,
            "snapshot-isolation violated (threads=1): \
             count={count} is neither pre ({pre_count}) nor post ({post_count})"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// Same ordering-correctness test as
    /// `delete_concurrent_with_query_on_same_db` but with
    /// `query_threads = 4`.
    ///
    /// Operations are still mutex-serialized (one runs at a time);
    /// the higher `query_threads` only affects the Rayon pool size
    /// for a single query's operator drive, not inter-query
    /// parallelism.  The test exercises the engine under a different
    /// scheduler configuration and is a regression guard for when
    /// TASK-536 lands real per-shard morsel dispatch.
    #[test]
    fn delete_concurrent_with_query_on_same_db_threads_4() {
        let path = scratch_db_root("del-conc-4t");
        let engine = Engine::with_config(EngineConfig {
            query_threads: Some(4),
            ..EngineConfig::default()
        });

        let db = setup_purchases_db(&path, &engine, 50, 10);
        let pre_count: usize = 50;
        let post_count: usize = 45;

        let db = Arc::new(Mutex::new(db));
        let count = run_delete_query_race(Arc::clone(&db), &engine);

        assert!(
            count == pre_count || count == post_count,
            "snapshot-isolation violated (threads=4): \
             count={count} is neither pre ({pre_count}) nor post ({post_count})"
        );

        let _ = std::fs::remove_dir_all(&path);
    }

    /// 1 000-iteration stability variant. Gated behind `cfg(stress)`
    /// for nightly CI only — the loop is too slow for `cargo test`.
    ///
    /// Each iteration spawns a writer + reader pair that race for
    /// `Arc<Mutex<Database>>`. The ordering invariant from
    /// `deletes.md §9` holds throughout: the count must be either
    /// `pre_count` (50) or `post_count` (45).
    ///
    /// **Note on iteration diversity.** After iteration 0 commits a
    /// delete, `user_0`'s entity tombstone is permanent. In iterations
    /// 1–999 the writer's DELETE is an idempotent no-op (set-union on
    /// an already-present entry) and the reader always observes 45.
    /// The loop's value is stress-testing the absence of panics,
    /// deadlocks, and mutex poisoning under repeated scheduler
    /// round-trips — not 1 000 distinct race outcomes.
    #[test]
    #[cfg(stress)]
    fn delete_concurrent_with_query_on_same_db_stress() {
        const ITERATIONS: usize = 1_000;

        let path = scratch_db_root("del-conc-stress");
        let engine = Engine::with_config(EngineConfig {
            query_threads: Some(1),
            ..EngineConfig::default()
        });

        let db = setup_purchases_db(&path, &engine, 50, 10);
        let pre_count: usize = 50;
        let post_count: usize = 45;

        let db = Arc::new(Mutex::new(db));

        for iter in 0..ITERATIONS {
            let count = run_delete_query_race(Arc::clone(&db), &engine);
            assert!(
                count == pre_count || count == post_count,
                "iteration {iter}: count={count} outside valid set \
                 {{pre={pre_count}, post={post_count}}}"
            );
        }

        let _ = std::fs::remove_dir_all(&path);
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
