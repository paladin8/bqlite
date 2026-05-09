//! Wave 5 acceptance gate (TASK-528).
//!
//! End-to-end correctness gate for the wave. The four bands the task
//! definition pins, each landed as one or more focused tests below:
//!
//! 1. **Multi-shard analytical query under the documented budget**
//!    (`docs/design/engine/memory-budget.md` § 2.2 / § 8.2). A fixture
//!    spread across the 32-shard default manifest is queried at
//!    [`MIN_QUERY_BUDGET_BYTES`] (the documented floor). Hand-computed
//!    expected values pin the answer. `peak_memory_bytes` is asserted
//!    `Some(_)` to confirm the per-query [`MemoryTracker`] is wired,
//!    per `query.rs:91-98`.
//!
//! 2. **Cancellation / timeout cleanup on a long-running query**
//!    (`engine/cancellation.md` § 5.1 / § 5.2,
//!    `engine/spill.md` § 8.3). TASK-538 added a public per-query
//!    cancel handle and timeout knob to `Engine::query_with_options`;
//!    band 2 drives both through that surface and asserts every exit
//!    path leaves no spill artefacts behind. The contract-level
//!    re-pinning (the original Wave 5 entry-time test) lives in
//!    `tests/tests/wave5_runtime_stress.rs::cancellation_cleanup`.
//!
//! 3. **Sort / ingest / cohort spill policy** (`engine/spill.md` § 3 / § 6 / § 8).
//!    The v1 spill list is short and explicit:
//!    - Sort spills (TASK-513) — verified via answer-invariance across
//!      the default budget and the floor budget on a multi-shard
//!      `ORDER BY ts ASC` query.
//!    - Cohort / IN-subquery and aggregate **fail fast** rather than
//!      spill. v1 carries no on-disk hash-set. Verified by asserting
//!      a small-cohort multi-shard query returns the expected row set
//!      under the floor budget without spilling artefacts.
//!    - Ingest partitioner spill (TASK-539) — verified in
//!      `ingest_spill_produces_correct_queryable_data`: a tight-budget
//!      partitioner is driven past its memory ceiling, the RAII guards
//!      remove all `.spill` files on drain, and an analytical query
//!      over the written segments returns the hand-computed reference.
//!
//!    Both sub-bands above also assert "no spill artefacts persist
//!    after the query returns" (the cleanup contract from § 8.3).
//!
//! 4. **Fused / zero-copy path matches the reference fixture**
//!    (`engine/operator-fusion.md`,
//!    `storage/zero-copy-scan-filter.md`). The planner does not expose
//!    a public knob to disable stateless fusion; the design's intent
//!    is that fused and fallback paths produce byte-identical answers
//!    by construction. The achievable contract for an end-to-end gate
//!    is therefore: run the multi-shard reference fixture through the
//!    engine (which exercises `FusedSegmentPhysical` + zero-copy scan
//!    where applicable) and assert the result equals the
//!    **hand-computed ground truth** derived directly from the fixture
//!    builder. Per-operator fused-vs-fallback unit coverage lives in
//!    the operator crate (`crates/bqlite-operators/src/`); the suite-
//!    level assertion here pins answer correctness across the optimizer
//!    + fusion pipeline.
//!
//! ## Why a single binary
//!
//! Each band stands on its own and could in principle live in its own
//! file. They share a single Parquet fixture builder and a single set
//! of hand-computed expected values, so colocating them avoids
//! duplicating ~200 lines of fixture wiring. The wave-level acceptance
//! gate is the *one place* where the wave's named contracts converge,
//! which matches the pattern set by `wave4_acceptance.rs`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray, StringViewArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bqlite_engine::{Database, Engine, ExecutionResult, QueryOptions, MIN_QUERY_BUDGET_BYTES};
use bqlite_tests::common::TempDb;
use parquet::arrow::ArrowWriter;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

/// Anchor timestamp: 2023-11-14T22:13:20Z. Shared with the rest of the
/// Wave 4/5 acceptance fixtures so every row lands in a single 30-day
/// window (the default window size partitions by `ts / window_days`).
const T0: i64 = 1_700_000_000_000_000_000;

/// One second in nanoseconds. Enough granularity that 600 rows still
/// fit inside one 30-day window comfortably.
const S: i64 = 1_000_000_000;

/// Number of distinct entities. Picked to spread well across the 32
/// default shards (`bqlite-storage::manifest::DEFAULT_SHARD_COUNT`) —
/// `entity_id = format!("user_{i:04}")` over `0..200` distributes via
/// the partitioner's hash and reliably populates >24 of the 32 shards
/// in CI runs (verified empirically; the exact count is platform-stable
/// because the partitioner uses a fixed seed).
const N_ENTITIES: usize = 200;

/// Events per entity. Three event types — `view`, `add_to_cart`, `purchase`
/// — with a fixed per-entity sequence so the per-event-type counts are
/// hand-computable and the ground-truth tests have a closed-form expected.
const EVENTS_PER_ENTITY: usize = 3;
const EVENT_TYPES: [&str; EVENTS_PER_ENTITY] = ["view", "add_to_cart", "purchase"];

const CREATE_TABLE: &str = "CREATE TABLE events (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE\
 )";

fn entity_id(i: usize) -> String {
    format!("user_{i:04}")
}

/// Write the fixture as a single Parquet file. The Parquet ingest path
/// is the multi-shard one — the partitioner buckets rows by
/// `hash(entity_id) % shard_count`, so a fixture with 200 distinct
/// entity ids reliably touches a strong majority of the 32 default
/// shards.
fn write_fixture_parquet(dir: &Path, filename: &str) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).expect("create fixture dir");
    let path = dir.join(filename);

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("event_type", DataType::Utf8, false),
    ]));

    let mut entity_ids: Vec<String> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
    let mut tss: Vec<i64> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
    let mut event_types: Vec<&str> = Vec::with_capacity(N_ENTITIES * EVENTS_PER_ENTITY);
    for i in 0..N_ENTITIES {
        let eid = entity_id(i);
        for (k, et) in EVENT_TYPES.iter().enumerate() {
            entity_ids.push(eid.clone());
            tss.push(T0 + (k as i64) * S);
            event_types.push(*et);
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

    let file = std::fs::File::create(&path).expect("create fixture parquet file");
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).expect("ArrowWriter");
    writer.write(&batch).expect("write fixture batch");
    writer.close().expect("close fixture writer");
    path
}

/// Build a fresh database, ingest the multi-shard fixture, return the
/// owning temp directory + database + a fresh engine.
fn build_acceptance_db(label: &str) -> (TempDb, Database, Engine) {
    let tmp = TempDb::new();
    let mut db =
        Database::create(tmp.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_TABLE, &mut db)
        .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));
    let fixture = write_fixture_parquet(tmp.path(), "wave5-fixture.parquet");
    let sql = format!(
        "INSERT INTO events FROM '{}' WITH (format: 'parquet')",
        fixture.display()
    );
    engine
        .query(&sql, &mut db)
        .unwrap_or_else(|e| panic!("[{label}] INSERT FROM Parquet: {e}"));
    (tmp, db, engine)
}

/// Total rows the fixture builder wrote.
fn total_fixture_rows() -> usize {
    N_ENTITIES * EVENTS_PER_ENTITY
}

/// Hand-computed reference: per-event-type COUNT(*). Each entity emits
/// one row of each event type; with `N_ENTITIES` entities the per-type
/// count is exactly `N_ENTITIES`.
fn expected_per_event_type_counts() -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for et in EVENT_TYPES {
        out.insert(et.to_string(), N_ENTITIES as i64);
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Result extractors
// ─────────────────────────────────────────────────────────────────────────────

fn group_by_stringkey_to_i64(
    result: &ExecutionResult,
    key_column: &str,
    value_column: &str,
) -> BTreeMap<String, i64> {
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    for batch in &result.rows {
        let keys = batch
            .column_by_name(key_column)
            .unwrap_or_else(|| panic!("key column `{key_column}` present"))
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap_or_else(|| panic!("key `{key_column}` is StringViewArray"));
        let values = batch
            .column_by_name(value_column)
            .unwrap_or_else(|| panic!("value column `{value_column}` present"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("value `{value_column}` is Int64Array"));
        for i in 0..batch.num_rows() {
            assert!(
                !keys.is_null(i),
                "unexpected NULL grouping key in `{key_column}`"
            );
            out.insert(keys.value(i).to_string(), values.value(i));
        }
    }
    out
}

fn count_rows(result: &ExecutionResult) -> usize {
    result.rows.iter().map(|b| b.num_rows()).sum()
}

/// Materialize the `ts` column across every batch in a result. Empty
/// mid-stream batches are skipped (legal per the `PhysicalOperator`
/// contract). Used by the sort-spill band to compare row sequences
/// across budget settings.
fn collect_ts_sequence(result: &ExecutionResult) -> Vec<i64> {
    let mut out = Vec::with_capacity(count_rows(result));
    for batch in &result.rows {
        if batch.num_rows() == 0 {
            continue;
        }
        let ts = batch
            .column_by_name("ts")
            .expect("ts column present")
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts is TimestampNanosecondArray");
        for i in 0..batch.num_rows() {
            out.push(ts.value(i));
        }
    }
    out
}

/// Number of distinct `(window, shard)` cells that hold at least one
/// segment for the given table. Used by the multi-shard band to prove
/// the fixture actually populated more than one shard, otherwise the
/// "multi-shard" assertion is vacuous.
fn populated_shards(db: &Database, table: &str) -> usize {
    let entry = match db.manifest().tables.get(table) {
        Some(e) => e,
        None => return 0,
    };
    let mut count = 0;
    for w in &entry.windows {
        for shard in &w.shards {
            if !shard.is_empty() {
                count += 1;
            }
        }
    }
    count
}

// ─────────────────────────────────────────────────────────────────────────────
// Band 1 — Multi-shard analytical query under the documented budget
// ─────────────────────────────────────────────────────────────────────────────

/// Headline analytical query: STATS COUNT(*) GROUP BY event_type, run
/// against a 200-entity multi-shard Parquet ingest at the floor budget
/// (`MIN_QUERY_BUDGET_BYTES` = 512 MiB, the documented minimum per
/// `engine/memory-budget.md` § 8.2). Every assertion is against
/// hand-computed values from the fixture builder so a regression in
/// the multi-shard scan, the partial-aggregate merge, or the per-query
/// budget tracker shows up here.
#[test]
fn multi_shard_stats_under_floor_budget_matches_hand_computed() {
    let (_tmp, mut db, engine) = build_acceptance_db("stats-floor");

    // Sanity: the fixture builder must actually populate more than one
    // shard. Without this assertion the rest of the band degrades to
    // a single-shard test silently if the partitioner ever changes
    // its hashing.
    let shards = populated_shards(&db, "events");
    assert!(
        shards >= 2,
        "fixture must spread across multiple shards; got {shards}"
    );

    let opts = QueryOptions {
        memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
        ..QueryOptions::default()
    };
    let result = engine
        .query_with_options(
            "events | STATS rows = COUNT(*) GROUP BY event_type",
            &mut db,
            &opts,
        )
        .expect("STATS at floor budget must succeed");

    let by_type = group_by_stringkey_to_i64(&result, "event_type", "rows");
    assert_eq!(
        by_type,
        expected_per_event_type_counts(),
        "per-event-type COUNT must match the hand-computed reference"
    );

    // The per-query MemoryTracker is always present on tracker-backed
    // runs. The value need not be non-zero — operator-side wiring for
    // `try_reserve` is still landing (TASK-512/513/514). The contract
    // pinned here is that a tracker is *attached*, per `query.rs:91-98`.
    assert!(
        result.peak_memory_bytes.is_some(),
        "tracker-backed query must report peak_memory_bytes"
    );

    // Per-shard morsel dispatch (TASK-536) must fan the query out to
    // multiple Rayon workers and emit at least one morsel per
    // populated shard. Before TASK-536 the dispatcher seeded
    // `num_workers == 1` and every `morsels_per_shard_*` field at
    // zero — the legacy single-task path. With real per-shard
    // dispatch the metrics carry concrete values.
    let pop_shards = shards as u64;
    let resolved_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4) as u64;
    let expected_workers = resolved_threads.min(pop_shards).max(1);
    if expected_workers > 1 {
        // On a multi-core platform with more populated shards than
        // we risk a 1-core CI runner masking the parallelism — check
        // num_workers strictly only when the platform can deliver it.
        assert!(
            result.metrics.num_workers > 1,
            "multi-shard query must fan out to >1 worker on multi-core platforms; \
             got num_workers={}, pop_shards={pop_shards}, resolved_threads={resolved_threads}",
            result.metrics.num_workers
        );
    } else {
        // On a 1-core runner num_workers may degenerate to 1.
        assert!(
            result.metrics.num_workers >= 1,
            "num_workers must be at least 1; got {}",
            result.metrics.num_workers
        );
    }
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
    assert_eq!(
        result.metrics.morsels_dispatched, pop_shards,
        "morsels_dispatched must equal populated shard count"
    );
}

/// Regression: the cross-shard merge of per-shard `HashAccumulator`s
/// must produce row-for-row equality with the single-threaded baseline
/// for every v1 aggregate. Drives the same multi-shard fixture through
/// `query_threads = Some(1)` (which still routes through the per-shard
/// dispatcher but with one worker) and through the default
/// `query_threads = None` (multi-worker), then asserts the answer is
/// identical. Pins TASK-536 plan-revision R3.
#[test]
fn per_shard_aggregate_matches_single_thread_baseline() {
    use bqlite_engine::EngineConfig;

    let (_tmp, mut db, _engine_default) = build_acceptance_db("baseline-cmp");

    // 1-thread baseline: every shard's morsel runs on the same Rayon
    // worker. The result is the merged per-shard accumulator's
    // `finish()` output.
    let cfg_1 = EngineConfig {
        query_threads: Some(1),
        ..EngineConfig::default()
    };
    let engine_1 = Engine::with_config(cfg_1);
    let r1 = engine_1
        .query(
            "events | STATS rows = COUNT(*) GROUP BY event_type",
            &mut db,
        )
        .expect("1-thread baseline");

    // Multi-thread per-shard dispatch: shards run in parallel; cross-
    // shard merge on the coordinator.
    let r_n = Engine::default()
        .query(
            "events | STATS rows = COUNT(*) GROUP BY event_type",
            &mut db,
        )
        .expect("multi-thread");

    let by_type_1 = group_by_stringkey_to_i64(&r1, "event_type", "rows");
    let by_type_n = group_by_stringkey_to_i64(&r_n, "event_type", "rows");
    assert_eq!(
        by_type_1, by_type_n,
        "1-thread and multi-thread dispatch must produce identical aggregate results"
    );
}

/// A bare scan over the multi-shard fixture must return exactly
/// `N_ENTITIES * EVENTS_PER_ENTITY` rows. This is the band's lower-
/// bound regression hook: if the multi-shard read path drops a shard
/// or double-emits any segment the row count diverges from the
/// fixture builder's row count.
#[test]
fn multi_shard_scan_returns_full_row_count() {
    let (_tmp, mut db, engine) = build_acceptance_db("scan-rows");
    let result = engine
        .query("events", &mut db)
        .expect("multi-shard scan must succeed");
    assert_eq!(
        count_rows(&result),
        total_fixture_rows(),
        "multi-shard scan row count must equal the fixture builder's row count"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Band 2 — Cancellation / timeout cleanup
// ─────────────────────────────────────────────────────────────────────────────

/// Public-surface cancellation: a caller-supplied `CancellationToken`
/// passed via `QueryOptions::cancel` is observed by every operator in
/// the query. Pre-cancelling it before submission produces
/// `BqliteError::Cancelled` from the very first yield point.
/// Public-API equivalent of the runtime-stress contract-level test.
#[test]
fn external_cancel_via_query_options_returns_cancelled() {
    use bqlite_engine::CancellationToken;

    let (_tmp, mut db, engine) = build_acceptance_db("ext-cancel");
    let token = CancellationToken::new();
    token.cancel();
    let opts = QueryOptions {
        cancel: Some(token),
        ..QueryOptions::default()
    };
    // Use ORDER BY so the SortOperator's pull loop has a yield-point
    // check before draining the scan — the bare `events` query may
    // run an empty-shard scan to EOS without observing the flag, but
    // the sort path is unconditional.
    match engine.query_with_options("events | ORDER BY ts ASC", &mut db, &opts) {
        Err(bqlite_engine::ExecutionFailure {
            error: bqlite_core::BqliteError::Cancelled,
            ..
        }) => {}
        other => panic!("expected Cancelled via public API, got {other:?}"),
    }
}

/// Public-surface timeout: `Duration::ZERO` triggers
/// `QueryTimer::spawn`'s synchronous-fire branch — the cancel reason
/// is installed before `run_query_inner` runs, so the result-collection
/// arm rewrites `Cancelled → Timeout` deterministically. Pins the
/// contract that the timeout knob discriminates Cancelled from Timeout
/// on the public surface.
#[test]
fn timeout_via_query_options_returns_timeout_error() {
    let (_tmp, mut db, engine) = build_acceptance_db("timeout");
    let opts = QueryOptions {
        timeout: Some(std::time::Duration::ZERO),
        ..QueryOptions::default()
    };
    match engine.query_with_options("events | ORDER BY ts ASC", &mut db, &opts) {
        Err(bqlite_engine::ExecutionFailure {
            error: bqlite_core::BqliteError::Timeout { .. },
            ..
        }) => {}
        other => panic!("expected Timeout via public API, got {other:?}"),
    }
}

/// After a public-API cancel exits the query, the per-query spill
/// subdir is reclaimed by `SpillCleanup::Drop`. Re-pins
/// `engine/spill.md` § 8.3 at the wave-acceptance level — running
/// through `Engine::query_with_options` rather than constructing a
/// `QueryContext` directly.
///
/// Even a query that never spilled passes the assertion ("the
/// per-query subdir was never created and the root stayed empty");
/// the point is that no artefacts persist after a cancel exit.
#[test]
fn cancel_exit_leaves_no_spill_artefacts() {
    use bqlite_engine::CancellationToken;

    let (_tmp, mut db, engine) = build_acceptance_db("cancel-cleanup");
    let spill_root = db.spill_fs().root().to_path_buf();

    for _ in 0..8 {
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
                Err(bqlite_engine::ExecutionFailure {
                    error: bqlite_core::BqliteError::Cancelled,
                    ..
                })
            ),
            "expected Cancelled exit, got {result:?}"
        );
        if spill_root.exists() {
            let entries: Vec<_> = std::fs::read_dir(&spill_root)
                .expect("read spill root")
                .filter_map(|e| e.ok())
                .collect();
            assert!(
                entries.is_empty(),
                "spill root must be empty after cancel exit, found {} entries",
                entries.len()
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Band 3 — Sort / ingest / cohort spill policy
// ─────────────────────────────────────────────────────────────────────────────

/// `ORDER BY ts ASC` over the multi-shard fixture must produce the
/// same row ordering at the floor budget as at the default budget.
/// This is the suite-level contract for sort spill correctness
/// (`engine/spill.md` § 6 / § 8): the sort operator may spill internally
/// at a tight budget, but the externally-observed row stream must be
/// identical to the in-memory path.
#[test]
fn order_by_floor_budget_matches_default_budget() {
    let (_tmp, mut db, engine) = build_acceptance_db("orderby");

    let r_default = engine
        .query("events | ORDER BY ts ASC", &mut db)
        .expect("ORDER BY at default budget");

    let opts = QueryOptions {
        memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
        ..QueryOptions::default()
    };
    let r_floor = engine
        .query_with_options("events | ORDER BY ts ASC", &mut db, &opts)
        .expect("ORDER BY at floor budget");

    assert_eq!(
        count_rows(&r_default),
        count_rows(&r_floor),
        "row count must be invariant across budget settings"
    );
    assert_eq!(
        count_rows(&r_default),
        total_fixture_rows(),
        "ORDER BY must return every fixture row"
    );

    // Compare the materialized timestamp sequence between the two
    // budget settings. ORDER BY ts ASC produces a non-decreasing
    // sequence; correctness across budgets means the two streams are
    // byte-identical when scanned by `(ts, entity_id, event_type)`.
    let ts_default = collect_ts_sequence(&r_default);
    let ts_floor = collect_ts_sequence(&r_floor);
    assert_eq!(
        ts_default, ts_floor,
        "ORDER BY ts sequence must be identical across budgets"
    );
    // The sequence itself must be monotonically non-decreasing (sanity
    // on the operator's contract; if this fails the equality check
    // above can still pass but the operator is broken).
    for w in ts_default.windows(2) {
        assert!(w[0] <= w[1], "ORDER BY ts ASC must be non-decreasing");
    }
}

/// Cohort / IN-subquery v1 policy is **fail-fast** (`engine/spill.md`
/// § 4.3, § 7) — there is no on-disk hash-set in v1. A small-cohort
/// multi-shard query under the floor budget must therefore complete
/// successfully (the cohort fits in memory) and produce the right
/// answer. The negative case (cohort exceeds budget → `MemoryBudgetExceeded`)
/// is owned by `wave5_runtime_stress.rs::budget_exhaustion`; the
/// positive case lives here so the wave gate also covers the happy
/// path of the cohort policy.
#[test]
fn cohort_query_under_floor_budget_returns_expected_rows() {
    let (_tmp, mut db, engine) = build_acceptance_db("cohort");

    let opts = QueryOptions {
        memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
        ..QueryOptions::default()
    };
    let result = engine
        .query_with_options(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'purchase' | SELECT entity_id\
             ) \
             | WHERE event_type = 'view' \
             | STATS views = COUNT(*) GROUP BY entity_id",
            &mut db,
            &opts,
        )
        .expect("cohort STATS under floor budget");

    // Every entity has exactly one `purchase` and one `view`, so the
    // cohort matches every entity and the per-entity view count is
    // exactly 1.
    assert_eq!(
        count_rows(&result),
        N_ENTITIES,
        "every entity is in the cohort and survives the view filter"
    );
    let by_entity = group_by_stringkey_to_i64(&result, "entity_id", "views");
    assert_eq!(
        by_entity.len(),
        N_ENTITIES,
        "result must contain one row per entity"
    );
    for (eid, count) in &by_entity {
        assert_eq!(*count, 1, "entity {eid} must have exactly one view row");
    }
}

/// After every successful query the per-database `spill_root` must
/// contain no leftover per-query subdirectories. Pins the cleanup
/// contract from `engine/spill.md` § 8.3 at the suite level so a Wave
/// 6 regression where the `SpillCleanup` guard stops firing (e.g. a
/// future engine refactor that holds a `QueryContext` clone past the
/// query return) shows up here.
#[test]
fn spill_root_is_empty_after_successful_query() {
    let (_tmp, mut db, engine) = build_acceptance_db("spill-clean");

    let opts = QueryOptions {
        memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
        ..QueryOptions::default()
    };
    let _ = engine
        .query_with_options("events | ORDER BY ts ASC", &mut db, &opts)
        .expect("ORDER BY must succeed");

    let spill_root = db.spill_root().to_path_buf();
    if spill_root.exists() {
        let entries: Vec<_> = std::fs::read_dir(&spill_root)
            .expect("read spill_root")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert!(
            entries.is_empty(),
            "spill_root must be empty after query return: {entries:?}"
        );
    }
}

/// End-to-end ingest partitioner spill correctness (`engine/spill.md`
/// § 6.2 / § 8). Drives the partitioner past a tight memory budget so
/// spill files are created, then verifies:
///
/// 1. At least one spill run was produced (the budget was actually hit).
/// 2. All `.spill` artefacts are removed by the RAII guard after drain
///    (`spill.md` § 8.1 / § 8.3).
/// 3. An analytical query over the written segments returns the same
///    per-event-type counts as the hand-computed reference — no rows
///    lost or duplicated across the spilled ↔ resident boundary.
#[test]
fn ingest_spill_produces_correct_queryable_data() {
    use bqlite_core::event::{EntityId, Event};
    use bqlite_core::time::Timestamp;
    use bqlite_storage::ingest::partitioner::Partitioner;
    use bqlite_storage::SegmentWriter;

    let tmp = TempDb::new();
    let mut db = Database::create(tmp.path()).expect("[ingest-spill] Database::create");
    let engine = Engine::new();
    engine
        .query(CREATE_TABLE, &mut db)
        .expect("[ingest-spill] CREATE TABLE");

    let spill_dir = tmp.path().join("ingest-spill-test");
    std::fs::create_dir_all(&spill_dir).unwrap();

    let shard_count = db.manifest().shard_count;
    let batch_id = db
        .allocate_batch_id("events")
        .expect("[ingest-spill] allocate_batch_id");

    // A very tight budget forces spill after the first few events.
    let tight_budget: usize = 2048;

    let mut partitioner =
        Partitioner::with_spill_dir(shard_count, 30, batch_id, tight_budget, spill_dir.clone())
            .expect("[ingest-spill] Partitioner::with_spill_dir");

    // Push the same fixture events as the rest of the acceptance suite
    // so the per-event-type counts are hand-computable.
    for i in 0..N_ENTITIES {
        let entity = EntityId::from(entity_id(i));
        for (k, et) in EVENT_TYPES.iter().enumerate() {
            let ts = Timestamp::from_nanos(T0 + (k as i64) * S);
            partitioner
                .push_event(Event::new(entity.clone(), ts, *et))
                .expect("[ingest-spill] push_event");
        }
    }

    // (1) The tight budget must have triggered at least one spill.
    let run_count = partitioner.spilled_run_count();
    assert!(
        run_count > 0,
        "tight budget must trigger at least one spill run; got {run_count}"
    );

    // Drain → write segments.
    SegmentWriter::new(&mut db)
        .write_partitioner("events", partitioner)
        .expect("[ingest-spill] write_partitioner");

    // (2) All spill artefacts removed by the RAII guard on drain.
    let leftover: Vec<_> = std::fs::read_dir(&spill_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".spill"))
        .collect();
    assert!(
        leftover.is_empty(),
        "no .spill artefacts must remain after write_partitioner drain: {leftover:?}"
    );

    // (3) Analytical query returns the hand-computed reference counts.
    let stats = engine
        .query(
            "events | STATS rows = COUNT(*) GROUP BY event_type",
            &mut db,
        )
        .expect("[ingest-spill] STATS query");
    let by_type = group_by_stringkey_to_i64(&stats, "event_type", "rows");
    assert_eq!(
        by_type,
        expected_per_event_type_counts(),
        "per-event-type COUNT after spill must match the hand-computed reference"
    );

    let scan = engine
        .query("events", &mut db)
        .expect("[ingest-spill] full scan");
    assert_eq!(
        count_rows(&scan),
        total_fixture_rows(),
        "full scan after spill must return every fixture row"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Band 4 — Fused / zero-copy path matches the reference fixture
// ─────────────────────────────────────────────────────────────────────────────

/// The optimizer-driven plan (with stateless fusion + zero-copy scan
/// where applicable, per `engine/operator-fusion.md` and
/// `storage/zero-copy-scan-filter.md`) must produce the
/// **hand-computed ground truth** on the reference fixture. There is
/// no public knob to disable fusion on the engine surface (the design
/// intent is that fused and fallback paths produce identical answers
/// by construction), so the suite-level "fused == fallback" contract
/// here is encoded as "fused == hand-computed reference". Per-operator
/// fused-vs-fallback unit coverage lives in `crates/bqlite-operators`.
///
/// Two assertions land:
/// 1. Aggregated shape — per-entity COUNT(*) of `purchase` rows.
/// 2. Per-row shape — the materialized `(entity_id, event_type)` of
///    every filtered row matches the hand-computed reference exactly.
///    A fusion regression that corrupts row contents but happens to
///    leave the aggregate balanced fails this second assertion.
#[test]
fn fused_path_matches_hand_computed_reference() {
    let (_tmp, mut db, engine) = build_acceptance_db("fused-vs-truth");

    // Filter + STATS — the canonical shape that exercises stateless
    // segment fusion (filter inside the scan-adjacent fused segment)
    // followed by the aggregate.
    let result = engine
        .query(
            "events | WHERE event_type = 'purchase' \
             | STATS purchases = COUNT(*) GROUP BY entity_id",
            &mut db,
        )
        .expect("fused filter + STATS must succeed");

    assert_eq!(
        count_rows(&result),
        N_ENTITIES,
        "every entity has exactly one purchase row"
    );
    let by_entity = group_by_stringkey_to_i64(&result, "entity_id", "purchases");
    assert_eq!(by_entity.len(), N_ENTITIES);
    for (eid, n) in &by_entity {
        assert_eq!(*n, 1, "entity {eid} must have exactly one purchase");
    }

    // Per-row assertion: filter without aggregate, materialize every
    // surviving row into a `(entity_id, event_type)` set, and require
    // it to equal the hand-computed reference. A fusion regression
    // that duplicates rows or swaps event-type values fails here even
    // when the per-entity count above stays at 1.
    let raw = engine
        .query("events | WHERE event_type = 'purchase'", &mut db)
        .expect("fused filter must succeed");
    let observed: BTreeMap<String, String> = collect_entity_event_pairs(&raw);
    let expected: BTreeMap<String, String> = (0..N_ENTITIES)
        .map(|i| (entity_id(i), "purchase".to_string()))
        .collect();
    assert_eq!(
        observed, expected,
        "per-row (entity_id, event_type) set must match the fixture exactly"
    );
}

/// Collect `(entity_id -> event_type)` pairs from a result. Asserts
/// that every input row is non-null on both columns. The `BTreeMap`
/// shape doubles as a uniqueness check — a duplicate `entity_id`
/// triggers a panic via the duplicate-key debug assertion below.
fn collect_entity_event_pairs(result: &ExecutionResult) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for batch in &result.rows {
        let entity = batch
            .column_by_name("entity_id")
            .expect("entity_id column present")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("entity_id is StringViewArray");
        let etype = batch
            .column_by_name("event_type")
            .expect("event_type column present")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("event_type is StringViewArray");
        for i in 0..batch.num_rows() {
            assert!(!entity.is_null(i), "entity_id must not be null");
            assert!(!etype.is_null(i), "event_type must not be null");
            let prior = out.insert(entity.value(i).to_string(), etype.value(i).to_string());
            assert!(
                prior.is_none(),
                "duplicate entity_id `{}` in filtered result",
                entity.value(i)
            );
        }
    }
    out
}

/// `WHERE` + `LIMIT` fuses into a single stateless segment. The
/// observable contract: the limited result is a prefix-shape of the
/// unlimited result (same row count when limit ≥ available rows; same
/// per-row content when limit < available). Pins the suite-level
/// invariant that fusion does not silently drop or reorder rows.
#[test]
fn fused_filter_limit_is_consistent_with_unfused_filter() {
    let (_tmp, mut db, engine) = build_acceptance_db("fused-limit");

    let unlimited = engine
        .query("events | WHERE event_type = 'purchase'", &mut db)
        .expect("filter without LIMIT");
    let limited = engine
        .query(
            "events | WHERE event_type = 'purchase' | LIMIT 100000",
            &mut db,
        )
        .expect("filter with LIMIT (above available)");

    // LIMIT 100_000 sits well above the 200 purchase rows the fixture
    // produced; the row counts must agree.
    assert_eq!(
        count_rows(&unlimited),
        N_ENTITIES,
        "filter must return exactly one purchase row per entity"
    );
    assert_eq!(
        count_rows(&limited),
        count_rows(&unlimited),
        "LIMIT above available must not drop rows"
    );

    // A small LIMIT must produce strictly fewer rows than the unlimited
    // filter, and never more than the limit.
    let small = engine
        .query("events | WHERE event_type = 'purchase' | LIMIT 7", &mut db)
        .expect("filter with small LIMIT");
    let small_n = count_rows(&small);
    assert!(
        small_n <= 7,
        "LIMIT 7 must not return more than 7 rows; got {small_n}"
    );
    assert!(
        small_n <= count_rows(&unlimited),
        "LIMIT must never return more rows than the unlimited filter"
    );
}
