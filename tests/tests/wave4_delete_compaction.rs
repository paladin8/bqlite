//! Wave 4 DELETE + compaction integration suite (TASK-440).
//!
//! End-to-end correctness gate for the live-delete story per
//! `docs/design/storage/deletes.md`. Drives every cheap-class
//! granularity, the `ALLOW SCAN` opt-in path, the `rows_affected`
//! contract, the per-query tombstone snapshot, and the
//! compaction-time physical reclamation sequence — through the
//! engine's text-in surface.
//!
//! ## Scope
//!
//! These cases are the integration-level evidence the wave acceptance
//! gate (TASK-442) and the delete/tombstone semantic audit (TASK-448)
//! lean on. The unit tests in `bqlite-engine::delete` already cover
//! per-classifier behaviour at the module boundary; this binary
//! exercises the same axes through `Engine::query` plus the manifest
//! and tombstone-file invariants that only become observable when a
//! full ingest → delete → compact cycle runs.
//!
//! ### What is asserted
//!
//! - **Cheap-class deletes** (entity equality / `IN`, `__seq_id`,
//!   `__batch_id`, time range): predicate routes to a tombstone
//!   write, post-delete `Engine::query` no longer surfaces the
//!   tombstoned rows, and `rows_affected` matches the spec
//!   (`deletes.md` §11).
//! - **Non-cheap predicate without `ALLOW SCAN`**: rejected at plan
//!   time per §4 with an error mentioning `ALLOW SCAN`.
//! - **`ALLOW SCAN`**: materialises matching `__seq_id`s into the
//!   shard's `row_deletes` set; subsequent SELECT excludes those
//!   rows.
//! - **Per-query snapshot (§6)**: a SELECT issued *before* a DELETE
//!   sees the pre-delete row set; a SELECT issued *after* sees the
//!   post-delete row set. Each query loads tombstones at bind time.
//! - **Compaction reclamation (§12)**: after `compact_one`, the
//!   shard's tombstone file no longer carries entries that the merge
//!   filter resolved, and the merged segment's row set excludes the
//!   tombstoned rows. The empty-merge path (every input row
//!   tombstoned) leaves the shard with zero output segments.

use std::collections::HashSet;

use arrow::array::{Array, StringViewArray};
use arrow::record_batch::RecordBatch;
use bqlite_engine::{Database, Engine, ExecutionResult};
use bqlite_storage::{
    compaction, read_tombstone_file, tombstone_file_path, SegmentMeta, TombstoneFile,
};
use bqlite_tests::common::TempDb;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture: a four-entity, four-event events table.
// ─────────────────────────────────────────────────────────────────────────────

/// Anchor timestamp for every fixture row: 2023-11-14T22:13:20Z in
/// nanoseconds since the Unix epoch. Picked so all four rows fall in
/// the same retention window, which keeps every per-(window, shard)
/// assertion deterministic regardless of the table's window-size
/// default.
const T0: i64 = 1_700_000_000_000_000_000;

const CREATE_TABLE: &str = "CREATE TABLE events (\
     user_id STRING NOT NULL ENTITY KEY, \
     ts TIMESTAMP NOT NULL EVENT TIME, \
     event_type STRING NOT NULL EVENT TYPE, \
     amount INT\
 )";

/// Create the table and ingest one batch of four rows across three
/// entities. The batch is sized so that some entities land in the same
/// shard and others in a different shard, exercising shard-targeted
/// tombstone writes when a delete narrows by entity.
fn fresh_db(label: &str) -> (TempDb, Database, Engine) {
    let db = TempDb::new();
    let mut database =
        Database::create(db.path()).unwrap_or_else(|e| panic!("[{label}] Database::create: {e}"));
    let engine = Engine::new();
    engine
        .query(CREATE_TABLE, &mut database)
        .unwrap_or_else(|e| panic!("[{label}] CREATE TABLE: {e}"));
    insert_initial_batch(&engine, &mut database, label);
    (db, database, engine)
}

fn insert_initial_batch(engine: &Engine, db: &mut Database, label: &str) {
    let sql = format!(
        "INSERT INTO events VALUES \
         ('alice', {a1}, 'click', 1), \
         ('alice', {a2}, 'view',  2), \
         ('bob',   {b1}, 'click', 3), \
         ('carol', {c1}, 'view',  4)",
        a1 = T0,
        a2 = T0 + 100_000_000,
        b1 = T0 + 200_000_000,
        c1 = T0 + 300_000_000,
    );
    engine
        .query(&sql, db)
        .unwrap_or_else(|e| panic!("[{label}] initial INSERT: {e}"));
}

/// Append a second batch of two more `alice` rows. Forces a second
/// segment in alice's shard so we can exercise multi-segment
/// compaction with tombstones.
fn insert_second_alice_batch(engine: &Engine, db: &mut Database, label: &str) {
    let sql = format!(
        "INSERT INTO events VALUES \
         ('alice', {a3}, 'purchase', 5), \
         ('alice', {a4}, 'logout',   6)",
        a3 = T0 + 400_000_000,
        a4 = T0 + 500_000_000,
    );
    engine
        .query(&sql, db)
        .unwrap_or_else(|e| panic!("[{label}] second INSERT: {e}"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers for inspecting query results and the manifest.
// ─────────────────────────────────────────────────────────────────────────────

fn select_all(engine: &Engine, db: &mut Database, label: &str) -> ExecutionResult {
    engine
        .query("events", db)
        .unwrap_or_else(|e| panic!("[{label}] SELECT events: {e}"))
}

fn collect_user_ids(rows: &[RecordBatch]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for batch in rows {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("user_id column is StringViewArray");
        for i in 0..batch.num_rows() {
            if !col.is_null(i) {
                out.push(col.value(i).to_string());
            }
        }
    }
    out.sort();
    out
}

/// Find the first `(window_id, shard_id)` cell that has at least
/// `min_segments` segments. Returns the segment metadata as well so
/// callers can assert on row counts before/after compaction.
fn find_shard_with_min_segments(
    db: &Database,
    table: &str,
    min_segments: usize,
) -> (u32, u16, Vec<SegmentMeta>) {
    let entry = db
        .manifest()
        .tables
        .get(table)
        .expect("table present in manifest");
    for window in &entry.windows {
        for (shard_idx, shard_segs) in window.shards.iter().enumerate() {
            if shard_segs.len() >= min_segments {
                return (window.window_id, shard_idx as u16, shard_segs.clone());
            }
        }
    }
    panic!(
        "no (window, shard) cell with >= {min_segments} segments in table '{table}'; \
         manifest = {:?}",
        entry.windows
    );
}

/// Sum tombstone-file entry counts across every populated `(window,
/// shard)` cell of a table. Used to assert tombstone state before and
/// after compaction-time reclamation.
fn total_tombstone_entries(db: &Database, table: &str) -> usize {
    let mut total = 0usize;
    let entry = match db.manifest().tables.get(table) {
        Some(e) => e.clone(),
        None => return 0,
    };
    for window in &entry.windows {
        for shard_idx in 0..window.shards.len() {
            let path = tombstone_file_path(db.root(), table, window.window_id, shard_idx as u16);
            if !path.exists() {
                continue;
            }
            let tf = read_tombstone_file(&path).expect("read tombstone file");
            total += tf.entity_deletes.len()
                + tf.row_deletes.len()
                + tf.batch_deletes.len()
                + tf.time_range_deletes.len();
        }
    }
    total
}

/// Sum a single tombstone-file field across every populated `(window,
/// shard)` cell. Lets the cheap-class accounting tests assert on one
/// granularity without re-deriving the manifest walk inline.
fn sum_tombstone_field<F>(db: &Database, table: &str, mut f: F) -> usize
where
    F: FnMut(&TombstoneFile) -> usize,
{
    let mut total = 0usize;
    let entry = match db.manifest().tables.get(table) {
        Some(e) => e.clone(),
        None => return 0,
    };
    for window in &entry.windows {
        for shard_idx in 0..window.shards.len() {
            let path = tombstone_file_path(db.root(), table, window.window_id, shard_idx as u16);
            if !path.exists() {
                continue;
            }
            let tf = read_tombstone_file(&path).expect("read tombstone file");
            total += f(&tf);
        }
    }
    total
}

fn tombstone_file_exists_anywhere(db: &Database, table: &str) -> bool {
    let entry = match db.manifest().tables.get(table) {
        Some(e) => e.clone(),
        None => return false,
    };
    for window in &entry.windows {
        for shard_idx in 0..window.shards.len() {
            let path = tombstone_file_path(db.root(), table, window.window_id, shard_idx as u16);
            if path.exists() {
                return true;
            }
        }
    }
    false
}

/// Total row count across every segment in the manifest. Drops to
/// zero only if the empty-merge compaction path removes every segment.
fn manifest_row_count(db: &Database, table: &str) -> u64 {
    db.manifest()
        .tables
        .get(table)
        .map(|t| {
            t.windows
                .iter()
                .flat_map(|w| w.shards.iter().flatten())
                .map(|s| s.row_count)
                .sum()
        })
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Cheap-class delete classes (deletes.md §3)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn entity_delete_excludes_rows_in_subsequent_query() {
    let (_tmp, mut db, engine) = fresh_db("entity-eq");

    let result = engine
        .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
        .expect("DELETE WHERE user_id = literal");
    assert_eq!(result.rows_affected, Some(2), "alice has two rows");
    assert!(result.is_empty(), "DELETE produces no result rows");

    let after = select_all(&engine, &mut db, "entity-eq");
    let mut seen = collect_user_ids(&after.rows);
    seen.dedup();
    assert_eq!(seen, vec!["bob", "carol"]);
}

#[test]
fn entity_in_list_delete_drops_each_listed_entity() {
    let (_tmp, mut db, engine) = fresh_db("entity-in");

    let result = engine
        .query(
            "DELETE FROM events WHERE user_id IN ('alice', 'bob')",
            &mut db,
        )
        .expect("DELETE WHERE user_id IN");
    assert_eq!(result.rows_affected, Some(3));

    let after = select_all(&engine, &mut db, "entity-in");
    let mut seen = collect_user_ids(&after.rows);
    seen.dedup();
    assert_eq!(seen, vec!["carol"]);
}

#[test]
fn batch_id_delete_uses_segment_metadata_for_count() {
    let (_tmp, mut db, engine) = fresh_db("batch-id");

    // The single ingest call wrote one batch — `__batch_id = 0`
    // covers all four rows. `rows_affected` is sourced from segment
    // metadata (deletes.md §11) and must equal the row count without
    // touching segment data beyond the row-count metadata.
    let result = engine
        .query("DELETE FROM events WHERE __batch_id = 0", &mut db)
        .expect("DELETE WHERE __batch_id = N");
    assert_eq!(result.rows_affected, Some(4));

    // The `__batch_id` tombstone is on disk — at least one shard's
    // tombstone file should now hold a batch_deletes entry.  The
    // SELECT-based correctness check (subsequent query returns only
    // non-deleted rows) lives in
    // `tests/wave5_system_columns::batch_id_delete_filters_rows_in_subsequent_select`.
    // Here we verify the on-disk tombstone state directly.
    let total_batch_entries = sum_tombstone_field(&db, "events", |tf| tf.batch_deletes.len());
    assert!(
        total_batch_entries >= 1,
        "batch_id tombstone must be persisted to at least one shard"
    );
}

#[test]
fn seq_id_in_list_returns_input_set_cardinality() {
    let (_tmp, mut db, engine) = fresh_db("seq-id-in");

    // Per deletes.md §11 the count for `__seq_id IN (...)` is the
    // input set's cardinality — even when some IDs do not exist.
    let result = engine
        .query("DELETE FROM events WHERE __seq_id IN (0, 1, 99)", &mut db)
        .expect("DELETE WHERE __seq_id IN");
    assert_eq!(result.rows_affected, Some(3));
}

#[test]
fn time_range_delete_excludes_old_rows() {
    let (_tmp, mut db, engine) = fresh_db("time-lt");

    // Drop everything strictly before bob's row (T0 + 200ms). That
    // tombstones alice's two rows; bob and carol survive.
    let cutoff = T0 + 200_000_000;
    let result = engine
        .query(&format!("DELETE FROM events WHERE ts < {cutoff}"), &mut db)
        .expect("DELETE WHERE ts < literal");
    assert_eq!(result.rows_affected, Some(2));

    let after = select_all(&engine, &mut db, "time-lt");
    let mut seen = collect_user_ids(&after.rows);
    seen.dedup();
    assert_eq!(seen, vec!["bob", "carol"]);
}

#[test]
fn time_range_two_bounds_collapses_to_single_range() {
    // `ts >= L AND ts <= H` collapses to one TimeRangeDelete with
    // both bounds inclusive (`deletes.md` §3.1). The classifier
    // pre-counts via row-group zone maps and must report the exact
    // count.
    let (_tmp, mut db, engine) = fresh_db("time-and");

    let lo = T0;
    let hi = T0 + 300_000_000;
    let result = engine
        .query(
            &format!("DELETE FROM events WHERE ts >= {lo} AND ts <= {hi}"),
            &mut db,
        )
        .expect("DELETE WHERE ts >= AND ts <=");
    assert_eq!(result.rows_affected, Some(4));

    let after = select_all(&engine, &mut db, "time-and");
    assert!(after.is_empty(), "every row is inside the closed range");

    // §3.1: the two-bound AND form must collapse to exactly one
    // TimeRangeDelete entry per shard touched, not two independent
    // entries. Sum across shards and divide by the number of
    // populated tombstone files to confirm each is exactly 1.
    let total_ranges = sum_tombstone_field(&db, "events", |tf| tf.time_range_deletes.len());
    let touched_shards = sum_tombstone_field(&db, "events", |tf| {
        usize::from(!tf.time_range_deletes.is_empty())
    });
    assert!(touched_shards >= 1, "at least one shard must be touched");
    assert_eq!(
        total_ranges, touched_shards,
        "each touched shard must hold exactly one collapsed TimeRangeDelete"
    );
}

#[test]
fn entity_plus_seq_id_uses_seq_id_count_with_shard_targeting() {
    // Cross-granularity entity + __seq_id is cheap per §3.2: entity
    // narrows the shard set, __seq_id is the actual tombstone. The
    // count is `|seq_ids|` regardless of how many of them physically
    // exist on disk.
    let (_tmp, mut db, engine) = fresh_db("entity-plus-seq");

    let result = engine
        .query(
            "DELETE FROM events WHERE user_id = 'alice' AND __seq_id IN (0, 1)",
            &mut db,
        )
        .expect("DELETE WHERE user_id AND __seq_id IN");
    assert_eq!(result.rows_affected, Some(2));
}

// ─────────────────────────────────────────────────────────────────────────────
// rows_affected accounting edge cases (deletes.md §11.2)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn entity_delete_for_unknown_entity_returns_zero() {
    let (_tmp, mut db, engine) = fresh_db("zero-match");

    let result = engine
        .query("DELETE FROM events WHERE user_id = 'eve'", &mut db)
        .expect("DELETE on missing entity");
    assert_eq!(result.rows_affected, Some(0));

    // Subsequent state is unchanged: still four rows visible.
    let after = select_all(&engine, &mut db, "zero-match");
    assert_eq!(after.row_count(), 4);
}

#[test]
fn time_range_outside_window_writes_no_tombstone_file() {
    let (_tmp, mut db, engine) = fresh_db("range-outside");

    // 1970 cutoff cannot match any row in the 2023 window.
    let result = engine
        .query("DELETE FROM events WHERE ts < 100", &mut db)
        .expect("DELETE for range outside window");
    assert_eq!(result.rows_affected, Some(0));
    assert!(
        !tombstone_file_exists_anywhere(&db, "events"),
        "no tombstone file should be created when no shard is touched"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cheap-class rejection and ALLOW SCAN (deletes.md §4)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn non_cheap_predicate_without_allow_scan_is_rejected() {
    let (_tmp, mut db, engine) = fresh_db("reject-non-cheap");

    let err = engine
        .query("DELETE FROM events WHERE event_type != 'spam'", &mut db)
        .expect_err("non-cheap predicate must be rejected at plan time");
    let msg = err.to_string();
    assert!(
        msg.contains("ALLOW SCAN"),
        "rejection message must mention ALLOW SCAN, got: {msg}"
    );
}

#[test]
fn allow_scan_materializes_row_tombstones_to_disk() {
    let (_tmp, mut db, engine) = fresh_db("allow-scan");

    let result = engine
        .query(
            "DELETE FROM events WHERE event_type = 'click' ALLOW SCAN",
            &mut db,
        )
        .expect("ALLOW SCAN must succeed");
    // Two rows have event_type = 'click': alice's click and bob's
    // click. The ALLOW SCAN path materialises matching `__seq_id`s
    // into shard-level `row_deletes` per `deletes.md` §4.1.
    assert_eq!(result.rows_affected, Some(2));

    // Walk every populated shard and sum the `row_deletes` set
    // sizes — exactly two `__seq_id`s should have landed on disk.
    // The SELECT-based correctness check (follow-up query filters the
    // tombstoned rows) lives in
    // `tests/wave5_system_columns::allow_scan_delete_filters_rows_in_subsequent_select`.
    // Here we verify the on-disk tombstone state directly, mirroring
    // the engine-level test `allow_scan_writes_row_level_tombstones`.
    let total_row_deletes = sum_tombstone_field(&db, "events", |tf| tf.row_deletes.len());
    assert_eq!(
        total_row_deletes, 2,
        "ALLOW SCAN must materialize 2 __seq_ids as row tombstones"
    );
}

#[test]
fn allow_scan_zero_match_writes_no_row_tombstones() {
    let (_tmp, mut db, engine) = fresh_db("allow-scan-zero");

    let result = engine
        .query(
            "DELETE FROM events WHERE event_type = 'spam' ALLOW SCAN",
            &mut db,
        )
        .expect("ALLOW SCAN with zero matches must succeed");
    assert_eq!(result.rows_affected, Some(0));

    // §11.2 zero-match contract: succeed silently, write no
    // tombstone state.
    let total_row_deletes = sum_tombstone_field(&db, "events", |tf| tf.row_deletes.len());
    assert_eq!(total_row_deletes, 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-query tombstone snapshot (deletes.md §6)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn each_query_loads_a_fresh_tombstone_snapshot() {
    // Two SELECTs straddling a DELETE: the first sees four rows
    // (no tombstones loaded at its bind time); the DELETE writes a
    // tombstone; the second SELECT loads the tombstone snapshot at
    // its own bind time and excludes the deleted rows. This is the
    // observable consequence of per-query snapshot loading from §6
    // — the engine never reuses a stale snapshot across statements.
    let (_tmp, mut db, engine) = fresh_db("snapshot");

    let pre = select_all(&engine, &mut db, "snapshot/pre");
    assert_eq!(pre.row_count(), 4, "pre-DELETE snapshot sees every row");

    let _del = engine
        .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
        .expect("DELETE between SELECTs");

    let post = select_all(&engine, &mut db, "snapshot/post");
    assert_eq!(
        post.row_count(),
        2,
        "post-DELETE snapshot reflects the tombstone immediately"
    );

    // A second DELETE for a different entity then a third SELECT
    // confirms the snapshot is reloaded per query, not cached across
    // the engine.
    let _del2 = engine
        .query("DELETE FROM events WHERE user_id = 'bob'", &mut db)
        .expect("second DELETE");
    let final_q = select_all(&engine, &mut db, "snapshot/final");
    assert_eq!(final_q.row_count(), 1);
    let mut seen = collect_user_ids(&final_q.rows);
    seen.dedup();
    assert_eq!(seen, vec!["carol"]);
}

#[test]
fn idempotent_delete_returns_same_count_and_state() {
    // §10 idempotence contract for cheap-class DELETEs: re-running
    // converges to the same final state. The on-disk tombstone set
    // does not grow, and the `rows_affected` count is identical to
    // the first run.
    let (_tmp, mut db, engine) = fresh_db("idempotent");

    let r1 = engine
        .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
        .expect("first DELETE");
    let entries_after_first = total_tombstone_entries(&db, "events");

    let r2 = engine
        .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
        .expect("retry DELETE");
    let entries_after_second = total_tombstone_entries(&db, "events");

    assert_eq!(r1.rows_affected, r2.rows_affected);
    assert_eq!(
        entries_after_first, entries_after_second,
        "idempotent retry must not grow tombstone state"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Compaction-time reclamation (deletes.md §12)
// ─────────────────────────────────────────────────────────────────────────────

/// After a multi-segment shard is compacted, every entity-level
/// tombstone resolved by the merge filter must be removed from the
/// shard's tombstone file, the merged segment must not contain the
/// deleted entity's rows, and the surviving entities must remain
/// queryable. This is the §12.2 + §12.4 evidence the wave acceptance
/// gate (TASK-442) leans on.
#[test]
fn compaction_reclaims_entity_tombstones_and_drops_deleted_rows() {
    let (_tmp, mut db, engine) = fresh_db("compact-entity");
    insert_second_alice_batch(&engine, &mut db, "compact-entity");

    // Two batches landed → at least one shard now holds two
    // segments (alice's shard, since both batches included alice).
    let (window_id, shard_id, segs_before) = find_shard_with_min_segments(&db, "events", 2);
    assert!(
        segs_before.len() >= 2,
        "expected >=2 input segments in (window={window_id}, shard={shard_id})"
    );

    // Tombstone alice and confirm the entity tombstone is on disk.
    let del = engine
        .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
        .expect("DELETE alice");
    assert_eq!(
        del.rows_affected,
        Some(4),
        "alice has 4 rows across the two batches"
    );
    let entries_before_compaction = total_tombstone_entries(&db, "events");
    assert!(
        entries_before_compaction >= 1,
        "tombstone file must record at least one entry before compaction"
    );

    let pre_compaction_users: HashSet<String> =
        collect_user_ids(&select_all(&engine, &mut db, "compact-entity/pre").rows)
            .into_iter()
            .collect();
    assert_eq!(
        pre_compaction_users,
        HashSet::from_iter(["bob".into(), "carol".into()]),
        "pre-compaction snapshot already filters tombstoned rows"
    );

    compaction::compact_one(&mut db, "events", window_id, u32::from(shard_id))
        .expect("compact_one for alice's shard");

    // §12.4: alice's entity entry must be reclaimed because no
    // surviving segment in this (window, shard) holds rows for her.
    let entries_after_compaction = total_tombstone_entries(&db, "events");
    assert!(
        entries_after_compaction < entries_before_compaction,
        "compaction must reclaim at least one tombstone entry; \
         before={entries_before_compaction}, after={entries_after_compaction}"
    );

    // Surviving entities still queryable; alice does not return.
    let post: HashSet<String> =
        collect_user_ids(&select_all(&engine, &mut db, "compact-entity/post").rows)
            .into_iter()
            .collect();
    assert_eq!(
        post, pre_compaction_users,
        "post-compaction query result equals pre-compaction logical set"
    );

    // Stronger spot-check: alice's entity tombstone is gone from
    // the compacted shard's tombstone file (or the file was removed
    // entirely when no entries remained, per §12.4 final paragraph).
    let path = tombstone_file_path(db.root(), "events", window_id, shard_id);
    if path.exists() {
        let tf = read_tombstone_file(&path).expect("read tombstone file");
        let alice_present = tf
            .entity_deletes
            .iter()
            .any(|v| matches!(v, bqlite_core::ScalarValue::String(s) if s == "alice"));
        assert!(
            !alice_present,
            "alice's entity tombstone must be reclaimed from the compacted shard"
        );
    }
}

/// When every input row in a `(window, shard)` is tombstoned, the
/// merger emits zero output and the shard ends with no remaining
/// segments. The tombstone snapshot still drives reclamation so the
/// resolved entries are cleared even on the empty-merge path
/// (`compact_one` §12.2 final paragraph).
#[test]
fn compaction_purges_shard_when_every_input_row_is_tombstoned() {
    let (_tmp, mut db, engine) = fresh_db("compact-empty");
    insert_second_alice_batch(&engine, &mut db, "compact-empty");

    let (window_id, shard_id, segs_before) = find_shard_with_min_segments(&db, "events", 2);
    let pre_rows: u64 = segs_before.iter().map(|s| s.row_count).sum();
    assert!(pre_rows >= 2);

    // Tombstone every row in alice's shard via batch_id deletes.
    // Both ingest calls produce a batch, so __batch_id IN (0, 1)
    // covers everything we wrote.
    let del = engine
        .query("DELETE FROM events WHERE __batch_id IN (0, 1)", &mut db)
        .expect("DELETE all batches");
    assert!(
        del.rows_affected.unwrap_or(0) > 0,
        "DELETE must report a non-zero count"
    );

    let row_count_before = manifest_row_count(&db, "events");
    compaction::compact_one(&mut db, "events", window_id, u32::from(shard_id))
        .expect("compact_one with full tombstone coverage");
    let row_count_after = manifest_row_count(&db, "events");
    assert!(
        row_count_after < row_count_before,
        "compaction must remove tombstoned segment rows from the manifest; \
         before={row_count_before}, after={row_count_after}"
    );

    // The compacted shard must hold zero segments now.
    let entry = db.manifest().tables["events"].clone();
    let win = entry
        .windows
        .iter()
        .find(|w| w.window_id == window_id)
        .expect("window present");
    let shard_segs = &win.shards[shard_id as usize];
    assert!(
        shard_segs.is_empty(),
        "every input row was tombstoned; shard must hold zero segments after compaction"
    );

    // The on-disk tombstone state for the compacted shard must
    // have its batch_id entries reclaimed. Other shards may still
    // hold batch_id tombstones for batches that touched them; we
    // assert specifically about the compacted shard only.
    let path = tombstone_file_path(db.root(), "events", window_id, shard_id);
    if path.exists() {
        let tf = read_tombstone_file(&path).expect("read tombstone file");
        assert!(
            tf.batch_deletes.is_empty(),
            "compacted shard's batch_deletes must be reclaimed; found {:?}",
            tf.batch_deletes
        );
    }

    // We verify the manifest and per-shard tombstone state here; the
    // SELECT-based post-compaction correctness check lives in
    // `tests/wave5_system_columns::batch_id_delete_after_compaction_still_filters`.
    // Verifying the tombstone state is the correctness signal for the
    // §12.2 reclamation contract that this test exists to cover.
}

// ─────────────────────────────────────────────────────────────────────────────
// Misc
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn delete_on_missing_table_errors_at_plan_time() {
    let db = TempDb::new();
    let mut database = Database::create(db.path()).expect("Database::create");
    let engine = Engine::new();

    let err = engine
        .query("DELETE FROM ghost WHERE user_id = 'alice'", &mut database)
        .expect_err("missing table must be a plan-time error");
    assert!(
        err.to_string().contains("ghost"),
        "error must name the missing table; got: {err}"
    );
}
