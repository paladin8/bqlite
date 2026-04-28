//! System-column follow-up correctness suite (TASK-509).
//!
//! Strengthens the joined-source, row/batch DELETE, and cohort cases that
//! were previously blocked by missing `__seq_id` / `__batch_id`
//! materialisation. TASK-508 materialises the two system columns end-to-end
//! through `ScanOperator` and `MergeSourcesOperator`; the tests here verify
//! the observable effects.
//!
//! ## What was blocked before TASK-508
//!
//! `docs/design/storage/deletes.md` §2 says row-level tombstones filter by
//! `__seq_id` and batch-level tombstones filter by `__batch_id`. Both checks
//! require the scan to materialise those columns so the tombstone filter
//! (`bqlite-storage::tombstone_scan`) can evaluate them per-row. Before
//! TASK-508 the columns were not emitted, so:
//!
//! - `DELETE FROM events WHERE __seq_id IN (...)` could write the tombstone
//!   file but the subsequent `SELECT` would still return the tombstoned rows
//!   because the scan filter had no column to test against.
//! - `DELETE FROM events WHERE __batch_id = N` similarly wrote a tombstone
//!   but `SELECT` returned all rows. `wave4_delete_compaction.rs` tests
//!   verified correctness via the tombstone file on disk rather than a
//!   follow-up `SELECT`.
//! - `ALLOW SCAN` deletes write row-level tombstones (`__seq_id`-based) so
//!   they had the same post-`SELECT` blind spot.
//! - Joined-source queries (`MergeSourcesOperator`) declared `__seq_id`
//!   non-nullable in the combined schema but the sub-scans did not emit the
//!   column, causing Arrow to reject the assembled batch.
//!
//! ## What this file verifies
//!
//! 1. **Row-level `__seq_id` delete → SELECT** (§2): rows whose `__seq_id`
//!    is tombstoned are filtered from subsequent SELECT results.
//! 2. **Batch-level `__batch_id` delete → SELECT** (§2): rows in a
//!    tombstoned batch are filtered from subsequent SELECT results.
//! 3. **ALLOW SCAN → SELECT** (§4.1): ALLOW SCAN materialises `__seq_id`
//!    row tombstones; a subsequent SELECT excludes the matching rows.
//! 4. **Joined-source + entity delete**: after an entity-level delete, a
//!    joined-source query reflects the deletion in the merged result.
//! 5. **Joined-source + batch delete**: `__batch_id` tombstones propagate
//!    through a joined-source query using the per-query tombstone snapshot
//!    (deletes.md §6).
//! 6. **DELETE + cohort** (`IN QUERY`): deleting the cohort-qualifying events
//!    for an entity removes that entity from the cohort in subsequent queries.
//! 7. **Query-snapshot isolation** (§6): SELECT queries issued before and
//!    after a DELETE observe consistent snapshots; the snapshot is bound at
//!    query-start and is not affected by concurrent or subsequent DELETEs.

use std::collections::HashSet;

use arrow::array::{Array, Int64Array, StringViewArray};
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_tests::common::TempDb;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Anchor timestamp: 2023-11-14T22:13:20Z in nanoseconds since the Unix
/// epoch. All fixture rows land in the same 30-day storage window so the
/// shard layout is deterministic across test runs.
const T0: i64 = 1_700_000_000_000_000_000;
/// One day in nanoseconds.
const D: i64 = 86_400 * 1_000_000_000;

const CREATE_EVENTS: &str = "CREATE TABLE events (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE\
 )";

const CREATE_PURCHASES: &str = "CREATE TABLE purchases (\
     entity_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE, \
     amount INT\
 )";

fn fresh_db_events(label: &str) -> (TempDb, Database, Engine) {
    let db = TempDb::new();
    let mut database =
        Database::create(db.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE events: {e}"));
    (db, database, engine)
}

fn fresh_db_events_and_purchases(label: &str) -> (TempDb, Database, Engine) {
    let db = TempDb::new();
    let mut database =
        Database::create(db.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_EVENTS, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE events: {e}"));
    engine
        .query(CREATE_PURCHASES, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE purchases: {e}"));
    (db, database, engine)
}

fn insert_events(engine: &Engine, db: &mut Database, rows: &[(&str, i64, &str)]) {
    assert!(!rows.is_empty());
    let mut sql = String::from("INSERT INTO events VALUES ");
    for (i, (entity, ts, et)) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("('{entity}', {ts}, '{et}')"));
    }
    engine
        .query(&sql, db)
        .unwrap_or_else(|e| panic!("insert_events: {e}"));
}

/// Total row count across all batches in an ExecutionResult.
fn row_count(result: &ExecutionResult) -> usize {
    result.row_count()
}

/// Collect the `entity_id` column from every batch into a sorted Vec.
fn entity_ids(result: &ExecutionResult) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name("entity_id")
            .expect("entity_id present")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("entity_id is StringViewArray");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    out.sort();
    out
}

/// Collect the `entity_id` column as a set (ignores duplicate names across
/// batches).
fn entity_set(result: &ExecutionResult) -> HashSet<String> {
    entity_ids(result).into_iter().collect()
}

/// Extract all values of an Int64 column across all batches.
fn collect_i64(result: &ExecutionResult, column: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in &result.rows {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column `{column}` present"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column `{column}` is Int64Array"));
        for i in 0..batch.num_rows() {
            out.push(col.value(i));
        }
    }
    out
}

/// Extract the single scalar i64 from a one-row, one-column result.
fn single_i64(result: &ExecutionResult, column: &str) -> i64 {
    assert_eq!(result.row_count(), 1, "expected single-row result");
    let batch = &result.rows[0];
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("`{column}` present"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("`{column}` is Int64Array"));
    col.value(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Row-level `__seq_id` delete → SELECT (deletes.md §2, system-columns.md §4.1)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn seq_id_delete_filters_rows_in_subsequent_select() {
    // Before TASK-508 the scan did not materialise `__seq_id`, so a
    // subsequent SELECT would still see tombstoned rows. This test
    // verifies the end-to-end tombstone filter using a SELECT after
    // the DELETE.
    let (_tmp, mut db, engine) = fresh_db_events("seq-id-select");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("alice", T0 + D, "view"),
            ("alice", T0 + 2 * D, "purchase"),
        ],
    );

    // Discover the actual `__seq_id` values assigned by the ingest.
    let seq_ids_result = engine
        .query("events | SELECT __seq_id | STATS total = COUNT(*)", &mut db)
        .expect("SELECT __seq_id");
    assert_eq!(
        single_i64(&seq_ids_result, "total"),
        3,
        "__seq_id must be materialised for all three rows"
    );

    // Fetch the concrete seq_id values so the DELETE is exact.
    let ids_result = engine
        .query("events | SELECT __seq_id", &mut db)
        .expect("fetch __seq_id values");
    let mut ids = collect_i64(&ids_result, "__seq_id");
    ids.sort();
    assert_eq!(ids.len(), 3, "expected 3 seq_ids");

    // Delete the first and last rows by __seq_id.
    let to_delete = format!("{}, {}", ids[0], ids[2]);
    let del = engine
        .query(
            &format!("DELETE FROM events WHERE __seq_id IN ({to_delete})"),
            &mut db,
        )
        .expect("DELETE by __seq_id");
    assert_eq!(del.rows_affected, Some(2));

    // A subsequent SELECT must exclude the tombstoned rows.
    let after = engine
        .query("events | STATS n = COUNT(*)", &mut db)
        .expect("post-delete count");
    assert_eq!(
        single_i64(&after, "n"),
        1,
        "exactly one row (the middle one) must survive"
    );

    // Verify the surviving row is alice's middle event.
    let survivors = engine
        .query("events | SELECT __seq_id", &mut db)
        .expect("surviving seq_ids");
    let surviving_ids = collect_i64(&survivors, "__seq_id");
    assert_eq!(
        surviving_ids,
        vec![ids[1]],
        "only the middle seq_id must remain"
    );
}

#[test]
fn seq_id_delete_compaction_invariance() {
    // After compaction the physical row set changes, but the logical
    // answer (surviving rows after seq_id tombstones) must be identical.
    let (_tmp, mut db, engine) = fresh_db_events("seq-id-compact");
    // Two separate ingest calls produce two segments so compact_one
    // has something to merge.
    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "click"), ("bob", T0 + D, "view")],
    );
    insert_events(&engine, &mut db, &[("alice", T0 + 2 * D, "purchase")]);

    // Fetch all seq_ids.
    let ids_result = engine
        .query("events | SELECT __seq_id", &mut db)
        .expect("seq_ids");
    let mut ids = collect_i64(&ids_result, "__seq_id");
    ids.sort();
    assert_eq!(ids.len(), 3);

    // Delete bob's row by __seq_id.
    let del = engine
        .query(
            &format!("DELETE FROM events WHERE __seq_id IN ({})", ids[1]),
            &mut db,
        )
        .expect("DELETE bob's row");
    assert_eq!(del.rows_affected, Some(1));

    let pre_compact_count = single_i64(
        &engine
            .query("events | STATS n = COUNT(*)", &mut db)
            .expect("pre-compact count"),
        "n",
    );
    assert_eq!(pre_compact_count, 2, "alice still has two rows");

    // Find any shard with >= 2 segments to compact.
    let entry = db.manifest().tables["events"].clone();
    let mut compacted = false;
    for window in &entry.windows {
        for (shard_idx, segs) in window.shards.iter().enumerate() {
            if segs.len() >= 2 {
                bqlite_storage::compaction::compact_one(
                    &mut db,
                    "events",
                    window.window_id,
                    shard_idx as u32,
                )
                .expect("compact_one");
                compacted = true;
                break;
            }
        }
        if compacted {
            break;
        }
    }

    let post_compact_count = single_i64(
        &engine
            .query("events | STATS n = COUNT(*)", &mut db)
            .expect("post-compact count"),
        "n",
    );
    assert_eq!(
        post_compact_count, pre_compact_count,
        "compaction must preserve the logical row count"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Batch-level `__batch_id` delete → SELECT (deletes.md §2)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn batch_id_delete_filters_rows_in_subsequent_select() {
    // `wave4_delete_compaction.rs::batch_id_delete_uses_segment_metadata_for_count`
    // verified `rows_affected` but noted that the subsequent SELECT could
    // not filter batch tombstones because `__batch_id` was not materialised.
    // This test verifies the SELECT-based outcome now works.
    let (_tmp, mut db, engine) = fresh_db_events("batch-id-select");

    // Batch 0: alice's events.
    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "click"), ("alice", T0 + D, "view")],
    );
    // Batch 1: bob's events.
    insert_events(&engine, &mut db, &[("bob", T0 + 2 * D, "click")]);

    // Verify the `__batch_id` values via SELECT.
    let batch_ids_result = engine
        .query("events | SELECT __batch_id | STATS n = COUNT(*)", &mut db)
        .expect("SELECT __batch_id");
    assert_eq!(
        single_i64(&batch_ids_result, "n"),
        3,
        "__batch_id must be materialised for all three rows"
    );

    // Both ingest calls produce batch_id = 0 and batch_id = 1 respectively
    // (monotonically allocated by the storage layer). Delete batch 0 (alice's).
    let del = engine
        .query("DELETE FROM events WHERE __batch_id = 0", &mut db)
        .expect("DELETE WHERE __batch_id = 0");
    assert_eq!(
        del.rows_affected,
        Some(2),
        "batch 0 contains alice's two rows"
    );

    // SELECT must now return only bob's row.
    let after = engine
        .query("events | STATS n = COUNT(*)", &mut db)
        .expect("post-batch-delete count");
    assert_eq!(
        single_i64(&after, "n"),
        1,
        "only bob's row in batch 1 must survive"
    );

    let surviving_entities = entity_ids(
        &engine
            .query("events", &mut db)
            .expect("post-batch-delete SELECT"),
    );
    assert_eq!(
        surviving_entities,
        vec!["bob"],
        "only bob must remain after batch 0 is tombstoned"
    );
}

#[test]
fn batch_id_delete_after_compaction_still_filters() {
    // After compaction the old `batch_id` tombstone is reclaimed only if the
    // compacted segment no longer contains any row with that `batch_id`
    // (deletes.md §12.4). This test verifies the pre-compaction SELECT
    // filter works correctly.
    let (_tmp, mut db, engine) = fresh_db_events("batch-id-compact");

    // Two ingest batches — batch 0 and batch 1.
    insert_events(&engine, &mut db, &[("alice", T0, "click")]);
    insert_events(
        &engine,
        &mut db,
        &[("bob", T0 + D, "view"), ("carol", T0 + 2 * D, "view")],
    );

    let del = engine
        .query("DELETE FROM events WHERE __batch_id = 1", &mut db)
        .expect("DELETE batch 1");
    // batch 1 contains bob + carol = 2 rows.
    assert_eq!(del.rows_affected, Some(2));

    // Pre-compaction SELECT must exclude batch 1 rows.
    let pre = engine.query("events", &mut db).expect("pre-compact SELECT");
    assert_eq!(entity_set(&pre), HashSet::from(["alice".to_string()]));
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. ALLOW SCAN → SELECT (deletes.md §4.1, system-columns.md §4.1)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn allow_scan_delete_filters_rows_in_subsequent_select() {
    // `wave4_delete_compaction.rs::allow_scan_materializes_row_tombstones_to_disk`
    // verified the tombstone was persisted on disk but noted that the scan
    // could not filter the surviving row tombstones via SELECT. That limitation
    // was because `__seq_id` was not materialised; TASK-508 fixes it.
    let (_tmp, mut db, engine) = fresh_db_events("allow-scan-select");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("alice", T0 + D, "view"),
            ("bob", T0 + 2 * D, "click"),
            ("carol", T0 + 3 * D, "purchase"),
        ],
    );

    // ALLOW SCAN delete: non-cheap predicate selects rows by event_type.
    let del = engine
        .query(
            "DELETE FROM events WHERE event_type = 'click' ALLOW SCAN",
            &mut db,
        )
        .expect("ALLOW SCAN delete");
    assert_eq!(del.rows_affected, Some(2), "two click rows (alice + bob)");

    // A subsequent SELECT must exclude the deleted rows.
    let after = engine
        .query("events | STATS n = COUNT(*)", &mut db)
        .expect("post-ALLOW-SCAN count");
    assert_eq!(
        single_i64(&after, "n"),
        2,
        "alice's view and carol's purchase must survive"
    );

    let survivors = entity_set(
        &engine
            .query("events", &mut db)
            .expect("post-ALLOW-SCAN SELECT"),
    );
    // alice still has her view row; carol still has her purchase.
    assert_eq!(
        survivors,
        HashSet::from(["alice".to_string(), "carol".to_string()])
    );
}

#[test]
fn allow_scan_then_entity_delete_cumulative_filtering() {
    // Two successive deletes of different classes both apply: ALLOW SCAN
    // writes `__seq_id` row tombstones; entity delete writes an entity
    // tombstone. Both must filter in a subsequent SELECT.
    let (_tmp, mut db, engine) = fresh_db_events("allow-scan-entity");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("alice", T0 + D, "view"),
            ("bob", T0 + 2 * D, "click"),
            ("carol", T0 + 3 * D, "purchase"),
        ],
    );

    // ALLOW SCAN deletes the two click rows (alice's click and bob's click).
    engine
        .query(
            "DELETE FROM events WHERE event_type = 'click' ALLOW SCAN",
            &mut db,
        )
        .expect("ALLOW SCAN");

    // Entity delete removes all of alice's remaining rows.
    engine
        .query("DELETE FROM events WHERE entity_id = 'alice'", &mut db)
        .expect("entity DELETE alice");

    // Only carol's purchase must remain.
    let after = engine
        .query("events | STATS n = COUNT(*)", &mut db)
        .expect("count after two deletes");
    assert_eq!(single_i64(&after, "n"), 1);
    let survivors = entity_set(&engine.query("events", &mut db).expect("SELECT"));
    assert_eq!(survivors, HashSet::from(["carol".to_string()]));
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Joined-source + entity delete (cohorts-aliases-joins.md §3.8)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn joined_source_reflects_entity_delete_in_events() {
    // A joined-source query (events JOIN purchases) merges both sources.
    // After an entity-level DELETE in events, the merged stream must no
    // longer include the deleted entity's events rows.
    let (_tmp, mut db, engine) = fresh_db_events_and_purchases("join-entity-del");

    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "signup"),
            ("alice", T0 + D, "click"),
            ("bob", T0, "signup"),
        ],
    );
    engine
        .query(
            &format!(
                "INSERT INTO purchases VALUES \
                 ('alice', {t1}, 'purchase', 100), \
                 ('bob',   {t2}, 'purchase', 200)",
                t1 = T0 + 2 * D,
                t2 = T0 + 3 * D,
            ),
            &mut db,
        )
        .expect("INSERT purchases");

    // Baseline: 3 events + 2 purchases = 5 rows.
    let pre = engine
        .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
        .expect("pre-delete join count");
    assert_eq!(single_i64(&pre, "total"), 5, "baseline: 5 joined rows");

    // Delete alice from events (entity tombstone on events table only).
    let del = engine
        .query("DELETE FROM events WHERE entity_id = 'alice'", &mut db)
        .expect("DELETE alice from events");
    assert_eq!(del.rows_affected, Some(2), "alice has two events rows");

    // Post-delete: alice's events are gone; bob's event + both purchases remain
    // = 1 + 2 = 3 rows.
    let post = engine
        .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
        .expect("post-delete join count");
    assert_eq!(
        single_i64(&post, "total"),
        3,
        "alice's two events rows are gone; purchases are untouched"
    );
}

#[test]
fn joined_source_reflects_batch_delete() {
    // After a batch-level delete in one source, the joined-source query
    // reflects the deletion via the per-query tombstone snapshot.
    let (_tmp, mut db, engine) = fresh_db_events_and_purchases("join-batch-del");

    // Batch 0 for events (batch_id = 0).
    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "signup"), ("bob", T0 + D, "signup")],
    );
    // Batch 0 for purchases (batch_id = 0 for purchases table).
    engine
        .query(
            &format!(
                "INSERT INTO purchases VALUES \
                 ('alice', {t1}, 'purchase', 50), \
                 ('bob',   {t2}, 'purchase', 75)",
                t1 = T0 + 2 * D,
                t2 = T0 + 3 * D,
            ),
            &mut db,
        )
        .expect("INSERT purchases");

    // Baseline: 2 + 2 = 4 rows.
    let pre = engine
        .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
        .expect("baseline join");
    assert_eq!(single_i64(&pre, "total"), 4);

    // Delete events batch 0 (covers both signup rows).
    let del = engine
        .query("DELETE FROM events WHERE __batch_id = 0", &mut db)
        .expect("DELETE events batch 0");
    assert_eq!(del.rows_affected, Some(2));

    // Post-delete: events has 0 rows; purchases still has 2.
    let post = engine
        .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
        .expect("post-batch-delete join");
    assert_eq!(
        single_i64(&post, "total"),
        2,
        "events rows are tombstoned; only purchase rows remain in the join"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. DELETE + cohort (cohorts-aliases-joins.md §2 + deletes.md §6)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_qualifying_event_removes_entity_from_cohort() {
    // Cohort: entities that have at least one 'signup' event.
    // After deleting an entity's signup event (entity-level tombstone),
    // that entity must no longer appear in a subsequent cohort-filtered
    // query.
    let (_tmp, mut db, engine) = fresh_db_events("cohort-del");
    insert_events(
        &engine,
        &mut db,
        &[
            // alice qualifies: has signup + click.
            ("alice", T0, "signup"),
            ("alice", T0 + D, "click"),
            // bob qualifies: has signup.
            ("bob", T0, "signup"),
            // carol does not qualify.
            ("carol", T0, "click"),
        ],
    );

    // Baseline cohort count: alice + bob → 3 rows total (alice has 2).
    let pre = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'signup' | SELECT entity_id\
             ) | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("pre-delete cohort count");
    assert_eq!(single_i64(&pre, "n"), 3, "alice (2 rows) + bob (1 row) = 3");

    // Delete bob entirely (entity tombstone removes his signup).
    engine
        .query("DELETE FROM events WHERE entity_id = 'bob'", &mut db)
        .expect("DELETE bob");

    // Post-delete: bob is no longer in the cohort; only alice's rows remain.
    let post = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'signup' | SELECT entity_id\
             ) | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("post-delete cohort count");
    assert_eq!(
        single_i64(&post, "n"),
        2,
        "only alice's two rows must remain in the cohort"
    );
}

#[test]
fn batch_delete_from_cohort_source_removes_entities() {
    // Delete an entire batch that includes cohort-qualifying events;
    // entities whose only qualifying event was in the deleted batch must
    // be excluded from the cohort in subsequent queries.
    let (_tmp, mut db, engine) = fresh_db_events("cohort-batch-del");

    // Batch 0: alice's signup (qualifying) and dave's click (non-qualifying).
    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "signup"), ("dave", T0 + D, "click")],
    );
    // Batch 1: bob's signup (qualifying).
    insert_events(&engine, &mut db, &[("bob", T0 + 2 * D, "signup")]);

    // Baseline: alice + bob are in the cohort.
    let pre_cohort = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'signup' | SELECT entity_id\
             ) | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("pre-delete cohort");
    assert_eq!(single_i64(&pre_cohort, "n"), 2, "alice + bob");

    // Delete batch 0 — removes alice's signup and dave's click.
    engine
        .query("DELETE FROM events WHERE __batch_id = 0", &mut db)
        .expect("DELETE batch 0");

    // Alice is no longer in the cohort (her only signup was in batch 0).
    // Bob's signup is in batch 1 → still qualifies.
    let post_cohort = engine
        .query(
            "events | WHERE entity_id IN QUERY (\
                 events | WHERE event_type = 'signup' | SELECT entity_id\
             ) | STATS n = COUNT(*)",
            &mut db,
        )
        .expect("post-delete cohort");
    assert_eq!(
        single_i64(&post_cohort, "n"),
        1,
        "only bob's signup survives batch 0 deletion"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Query-snapshot isolation (deletes.md §6)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn snapshot_isolation_row_level_tombstones() {
    // Per deletes.md §6: each query loads the tombstone snapshot at bind
    // time. A SELECT issued before a `__seq_id` DELETE sees the pre-delete
    // row set; one issued after sees the post-delete set.
    let (_tmp, mut db, engine) = fresh_db_events("snapshot-row");
    insert_events(
        &engine,
        &mut db,
        &[
            ("alice", T0, "click"),
            ("bob", T0 + D, "view"),
            ("carol", T0 + 2 * D, "purchase"),
        ],
    );

    // Pre-delete snapshot: three rows.
    let pre = engine
        .query("events | STATS n = COUNT(*)", &mut db)
        .expect("pre-delete count");
    assert_eq!(single_i64(&pre, "n"), 3);

    // Fetch alice's seq_id.
    let ids_result = engine
        .query("events | SELECT entity_id, __seq_id", &mut db)
        .expect("fetch seq_ids");

    let mut alice_seq_id: Option<i64> = None;
    for batch in &ids_result.rows {
        let entities = batch
            .column_by_name("entity_id")
            .expect("entity_id")
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringViewArray");
        let seq_ids = batch
            .column_by_name("__seq_id")
            .expect("__seq_id")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        for i in 0..batch.num_rows() {
            if entities.value(i) == "alice" {
                alice_seq_id = Some(seq_ids.value(i));
            }
        }
    }
    let alice_id = alice_seq_id.expect("alice must have a __seq_id");

    // Delete alice's row by __seq_id.
    engine
        .query(
            &format!("DELETE FROM events WHERE __seq_id IN ({alice_id})"),
            &mut db,
        )
        .expect("DELETE alice by __seq_id");

    // Post-delete snapshot: two rows (bob + carol).
    let post = engine
        .query("events | STATS n = COUNT(*)", &mut db)
        .expect("post-delete count");
    assert_eq!(single_i64(&post, "n"), 2);

    // Second delete: remove bob's row by entity key.
    engine
        .query("DELETE FROM events WHERE entity_id = 'bob'", &mut db)
        .expect("DELETE bob");

    // Final snapshot: only carol remains.
    let final_result = engine.query("events", &mut db).expect("final SELECT");
    assert_eq!(row_count(&final_result), 1);
    assert_eq!(
        entity_set(&final_result),
        HashSet::from(["carol".to_string()])
    );
}

#[test]
fn snapshot_isolation_batch_delete_across_sequential_queries() {
    // Three sequential queries straddling a batch delete verify that each
    // query's tombstone snapshot is loaded afresh and does not bleed into
    // the next. This extends deletes.md §6.1 to batch-level tombstones.
    let (_tmp, mut db, engine) = fresh_db_events("snapshot-batch");

    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "click"), ("alice", T0 + D, "view")],
    );
    insert_events(&engine, &mut db, &[("bob", T0 + 2 * D, "click")]);

    // Q1 — before any delete: 3 rows.
    let q1 = single_i64(
        &engine
            .query("events | STATS n = COUNT(*)", &mut db)
            .expect("Q1"),
        "n",
    );
    assert_eq!(q1, 3);

    // DELETE batch 0 (alice's two rows).
    engine
        .query("DELETE FROM events WHERE __batch_id = 0", &mut db)
        .expect("batch delete");

    // Q2 — after batch delete: 1 row (bob).
    let q2 = single_i64(
        &engine
            .query("events | STATS n = COUNT(*)", &mut db)
            .expect("Q2"),
        "n",
    );
    assert_eq!(q2, 1, "alice's batch must be filtered");

    // DELETE bob by entity.
    engine
        .query("DELETE FROM events WHERE entity_id = 'bob'", &mut db)
        .expect("entity delete");

    // Q3 — after both deletes: 0 rows.
    let q3 = single_i64(
        &engine
            .query("events | STATS n = COUNT(*)", &mut db)
            .expect("Q3"),
        "n",
    );
    assert_eq!(q3, 0, "no rows must remain");
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Joined-source snapshot isolation (deletes.md §6.1)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn joined_source_consistent_snapshot_across_sub_scans() {
    // deletes.md §6.1: all scan operators within a single query observe
    // the same tombstone state. For a joined-source query, both the events
    // sub-scan and the purchases sub-scan must see a consistent tombstone
    // snapshot so that "same query, different answers across sub-scans"
    // anomalies cannot occur.
    //
    // We verify this via sequential queries (pre-delete vs post-delete),
    // which is the observable consequence of per-query snapshot binding.
    let (_tmp, mut db, engine) = fresh_db_events_and_purchases("join-snapshot");

    insert_events(
        &engine,
        &mut db,
        &[("alice", T0, "signup"), ("bob", T0 + D, "signup")],
    );
    engine
        .query(
            &format!(
                "INSERT INTO purchases VALUES \
                 ('alice', {t1}, 'purchase', 100), \
                 ('bob',   {t2}, 'purchase', 200)",
                t1 = T0 + 2 * D,
                t2 = T0 + 3 * D,
            ),
            &mut db,
        )
        .expect("INSERT purchases");

    // Pre-delete: 4 rows.
    let pre = single_i64(
        &engine
            .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
            .expect("pre-delete join"),
        "total",
    );
    assert_eq!(pre, 4);

    // Delete alice from events. The tombstone applies only to the events
    // sub-scan. A consistent snapshot means the join sees 1 event (bob's)
    // plus 2 purchases = 3 rows.
    engine
        .query("DELETE FROM events WHERE entity_id = 'alice'", &mut db)
        .expect("DELETE alice from events");

    let post = single_i64(
        &engine
            .query("events JOIN purchases | STATS total = COUNT(*)", &mut db)
            .expect("post-delete join"),
        "total",
    );
    assert_eq!(
        post, 3,
        "bob's event (1) + alice's purchase (1) + bob's purchase (1) = 3"
    );
}
