//! DELETE statement execution (TASK-453, `docs/design/storage/deletes.md`).
//!
//! `Engine::query` dispatches `Statement::Delete` plans here rather than
//! through `bind_physical` because DELETE produces no result rows but
//! does carry an exact `rows_affected: u64` that the engine returns
//! out-of-band on `ExecutionResult` (`deletes.md` §11). Routing
//! through a dedicated module keeps the bind step focused on
//! producing query operators.
//!
//! ## Wave 4 scope (this checkpoint)
//!
//! - [`execute_delete_statement`] — top-level entry. Branches on the
//!   classified [`PhysicalDeleteFilter`] variant.
//! - [`execute_cheap_delete`] — direct per-shard tombstone writes for
//!   the cheap-class taxonomy (`deletes.md` §3): entity equality / IN,
//!   `__seq_id` equality / IN, `__batch_id` equality / IN, time-range
//!   comparisons / `BETWEEN`, and the `entity + __seq_id|__batch_id`
//!   shard-targeted mix from §3.2.
//! - `execute_allow_scan_delete` — landed by C3 (TASK-453).
//!
//! ## Per-shard serialization (`deletes.md` §9)
//!
//! Each `(table, window_id, shard_id)` tombstone-file mutation is
//! protected by an `Arc<Mutex<()>>` rented from
//! [`Database::tombstone_shard_lock`]. Today every mutation funnels
//! through `&mut Database` so the inner locks are uncontested no-ops;
//! the discipline is in the code so the contract holds when later
//! waves introduce shared-handle execution. SS9.2's `flock`
//! promotion for multi-process writers is still deferred.
//!
//! ## Idempotent retry (`deletes.md` §10)
//!
//! Cheap-class writes are read-modify-write through
//! [`TombstoneFile::merge`], which performs set-union for row / batch
//! / entity entries and exact-match deduplication for time-range
//! entries (SS5.1). Re-running the same DELETE is a no-op on the
//! on-disk tombstone state and returns the same `rows_affected`. The
//! `ALLOW SCAN` idempotence caveat (SS10.2) — that retries may
//! tombstone additional rows ingested between runs — is a
//! user-facing documentation responsibility owned by TASK-456.
//!
//! ## DELETE vs. in-flight INSERT (`deletes.md` §8)
//!
//! `rows_affected` counts and ALLOW SCAN materialization read the
//! manifest snapshot live at DELETE-start. Concurrent INSERTs that
//! publish a manifest update mid-DELETE are invisible — they cannot
//! be tombstoned by this DELETE. The user-facing implication
//! ("sequence ingest then DELETE explicitly") is documented in
//! TASK-456.

use std::collections::HashSet;

use arrow::array::{Array, Int64Array, StringViewArray, TimestampNanosecondArray};

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::storage::{ColumnProjection, SegmentHandle, SegmentReader};
use bqlite_core::{EntityId, ScalarValue};
use bqlite_planner::{
    CheapDeleteSpec, DeletePhysical, EntityRole, PhysicalDeleteFilter, TimeRangeSpec,
};
use bqlite_storage::ingest::partitioner::shard_id_for;
use bqlite_storage::{
    tombstone_file_path, write_tombstone_atomic, Database, SegmentMeta, TimeRangeDelete,
    TombstoneFile,
};

use crate::query::ExecutionResult;

/// Top-level DELETE execution entry point.
///
/// Returns an [`ExecutionResult`] with empty rows and
/// `rows_affected = Some(count)` per `deletes.md` §11.
pub fn execute_delete_statement(
    plan: &DeletePhysical,
    db: &mut Database,
) -> Result<ExecutionResult> {
    let count = match &plan.filter {
        PhysicalDeleteFilter::Cheap(spec) => execute_cheap_delete(plan, spec, db)?,
        PhysicalDeleteFilter::AllowScan { .. } => {
            return Err(BqliteError::Execution(
                "ALLOW SCAN DELETE execution is not yet implemented (TASK-453 C3)".into(),
            ));
        }
    };
    Ok(ExecutionResult {
        schema: plan.output_schema.clone(),
        rows: Vec::new(),
        rows_affected: Some(count),
    })
}

/// Execute a cheap-class DELETE by writing directly to per-shard
/// tombstone files.
///
/// Returns the exact `rows_affected: u64` per `deletes.md` §11.
pub fn execute_cheap_delete(
    plan: &DeletePhysical,
    spec: &CheapDeleteSpec,
    db: &mut Database,
) -> Result<u64> {
    let table_name = plan.table_name.as_str();

    let shard_count = db.manifest().shard_count;
    if shard_count == 0 {
        return Err(BqliteError::Execution(
            "DELETE: shard_count must be at least 1".into(),
        ));
    }

    // ── 1. Resolve target shards ───────────────────────────────────────────
    let target_shards = compute_target_shards(spec, shard_count, &plan.entity_key_col)?;

    // ── 2. Compute rows_affected against the manifest snapshot at start ───
    //
    // Done before writes so the count reflects the snapshot the
    // user sees (and so a tombstone we just wrote does not interfere
    // with counting). For seq_id / batch_id the count is metadata-only
    // and order doesn't matter, but for entity / time-range we read
    // segment data — so snapshot-before-write is the right discipline.
    let rows_affected = compute_rows_affected(table_name, spec, &target_shards, db)?;

    // ── 3. Walk every (window, shard) the table has segments in and
    // write the per-shard tombstone update.
    //
    // The manifest's `windows` vec for this table enumerates every
    // window that has segments. Empty windows do not appear, so we
    // never spuriously create empty window directories.
    let table_entry = match db.manifest().tables.get(table_name) {
        Some(t) => t.clone(),
        None => {
            // Table existed at plan time but is gone now. Treat as
            // "nothing to delete" — the SS11.2 zero-match contract.
            return Ok(0);
        }
    };

    for window in &table_entry.windows {
        if !time_range_overlaps_window(spec.time_range.as_ref(), window) {
            // Pure time-range delete that doesn't touch this window
            // — skip. Other granularities short-circuit per shard.
            continue;
        }
        for &shard_id in &target_shards {
            let shard_idx = shard_id as usize;
            // Skip shards that are out of range for this window.
            // `WindowManifest::shards` length equals `shard_count` at
            // window creation time; if `shard_count` was bumped
            // later, older windows may have a shorter shards vec.
            // Treat absent shards as empty (no segments), so writing
            // a tombstone there would be wasted I/O.
            if shard_idx >= window.shards.len() {
                continue;
            }
            // Skip shards with no segments in this window — there is
            // nothing for the tombstone to apply to in this
            // (window, shard).
            if window.shards[shard_idx].is_empty() {
                continue;
            }

            let new_entries = build_tombstone_for_shard(spec, shard_id, shard_count);
            if new_entries.is_empty() {
                continue;
            }

            write_shard_tombstone(db, table_name, window.window_id, shard_id, new_entries)?;
        }
    }

    Ok(rows_affected)
}

/// Build the per-`(window, shard)` tombstone entries that the
/// `CheapDeleteSpec` produces for a specific shard.
///
/// Filters `entity_keys` (when `AsTombstone`) so only the entities
/// hashing to `shard_id` are written into that shard's file. Row /
/// batch / time-range entries are not shard-specific (the design
/// targets them at all relevant shards via `target_shards`), so they
/// land verbatim.
fn build_tombstone_for_shard(
    spec: &CheapDeleteSpec,
    shard_id: u16,
    shard_count: u16,
) -> TombstoneFile {
    let mut tf = TombstoneFile {
        row_deletes: spec.seq_ids.iter().copied().collect(),
        batch_deletes: spec.batch_ids.iter().copied().collect(),
        ..Default::default()
    };
    if let Some(range) = spec.time_range.as_ref() {
        tf.time_range_deletes.push(time_range_spec_to_delete(range));
    }
    if matches!(spec.entity_role, EntityRole::AsTombstone) {
        for value in &spec.entity_keys {
            if let Some(entity) = scalar_to_entity_id(value) {
                if shard_id_for(&entity, shard_count) == shard_id {
                    tf.entity_deletes.insert(value.clone());
                }
            }
        }
    }
    tf
}

/// Read, merge, and atomically rewrite a single shard's tombstone
/// file under the per-shard mutex from `Database::tombstone_shard_lock`.
fn write_shard_tombstone(
    db: &mut Database,
    table: &str,
    window_id: u32,
    shard_id: u16,
    new_entries: TombstoneFile,
) -> Result<()> {
    let lock = db.tombstone_shard_lock(table, window_id, shard_id);
    let _guard = lock
        .lock()
        .expect("tombstone shard lock poisoned by a panicking DELETE caller");

    let path = tombstone_file_path(db.root(), table, window_id, shard_id);
    let mut current = bqlite_storage::read_tombstone_file(&path)?;
    current.merge(&new_entries);
    write_tombstone_atomic(&path, &current)
}

/// Compute the exact `rows_affected` count per `deletes.md` §11.
///
/// Per-granularity strategy:
/// - `__seq_id`: trust `|seq_ids|` (design-doc convention; even if
///   some IDs do not exist on disk, the user-supplied set size is
///   the contract). When entity narrows the shards via
///   `EntityRole::AsShardTarget`, `seq_ids` is still the count.
/// - `__batch_id`: walk segment metadata, sum `row_count` for
///   segments matching the predicate `batch_id`. Free — no data scan.
/// - Entity-level (`AsTombstone`): per-shard scan of the entity-key
///   column on segments overlapping the affected entities.
/// - Time-range: per-segment zone-map check; fully-inside segments
///   contribute `row_count`; boundary segments require a timestamp
///   column scan.
fn compute_rows_affected(
    table: &str,
    spec: &CheapDeleteSpec,
    target_shards: &[u16],
    db: &Database,
) -> Result<u64> {
    if !spec.seq_ids.is_empty() {
        // `__seq_id` count is exact by uniqueness invariant; per
        // SS11 it is "free (known from the predicate literal)".
        // Deduplicate first so a predicate like `__seq_id IN (5, 5)`
        // does not double-count — the spec wording "input set"
        // (§11) is set-cardinality, not multiset-cardinality.
        // When entity narrows the shards (AsShardTarget mode), the
        // count is still the seq_id set size — the entity term is
        // shard targeting, not a separate filter.
        let unique: HashSet<u64> = spec.seq_ids.iter().copied().collect();
        return Ok(unique.len() as u64);
    }
    if !spec.batch_ids.is_empty() {
        return Ok(count_rows_by_batch_id(table, spec, target_shards, db));
    }
    if let Some(range) = spec.time_range.as_ref() {
        return count_rows_by_time_range(table, range, db);
    }
    if matches!(spec.entity_role, EntityRole::AsTombstone) && !spec.entity_keys.is_empty() {
        return count_rows_by_entity(table, &spec.entity_keys, target_shards, db);
    }
    Ok(0)
}

/// Sum `row_count` for every segment whose `batch_id` is in
/// `spec.batch_ids` and whose shard is in `target_shards`.
fn count_rows_by_batch_id(
    table: &str,
    spec: &CheapDeleteSpec,
    target_shards: &[u16],
    db: &Database,
) -> u64 {
    let target: HashSet<u16> = target_shards.iter().copied().collect();
    let batches: HashSet<u64> = spec.batch_ids.iter().copied().collect();
    let mut total: u64 = 0;
    let Some(entry) = db.manifest().tables.get(table) else {
        return 0;
    };
    for window in &entry.windows {
        for (shard_idx, segs) in window.shards.iter().enumerate() {
            if !target.contains(&(shard_idx as u16)) {
                continue;
            }
            for seg in segs {
                if batches.contains(&seg.batch_id) {
                    total = total.saturating_add(seg.row_count);
                }
            }
        }
    }
    total
}

/// Count rows whose timestamp falls inside `range`.
///
/// Walks every segment in the table. Segments fully outside the
/// range contribute zero (cheap zone-map test); fully-inside
/// segments contribute their `row_count` straight from metadata;
/// boundary segments require a timestamp column scan.
fn count_rows_by_time_range(table: &str, range: &TimeRangeSpec, db: &Database) -> Result<u64> {
    let entry = match db.manifest().tables.get(table) {
        Some(e) => e,
        None => return Ok(0),
    };
    let ts_col = entry.schema.timestamp_column().name.clone();
    let mut total: u64 = 0;

    let reader = db.segment_reader(table)?;
    let segments: Vec<SegmentHandle> = reader
        .segments()
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let by_id: std::collections::HashMap<u64, &SegmentMeta> = entry
        .windows
        .iter()
        .flat_map(|w| w.shards.iter().flatten())
        .map(|s| (s.segment_id, s))
        .collect();

    for handle in &segments {
        let Some(meta) = by_id.get(&handle.segment_id) else {
            continue;
        };
        match classify_segment_against_range(meta, range) {
            SegmentRangeClass::Outside => continue,
            SegmentRangeClass::FullyInside => {
                total = total.saturating_add(meta.row_count);
            }
            SegmentRangeClass::Boundary => {
                total = total.saturating_add(scan_segment_count_in_range(
                    &*reader, handle, &ts_col, range,
                )?);
            }
        }
    }
    Ok(total)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentRangeClass {
    Outside,
    FullyInside,
    Boundary,
}

fn classify_segment_against_range(meta: &SegmentMeta, range: &TimeRangeSpec) -> SegmentRangeClass {
    let (seg_min, seg_max) = meta.ts_range; // inclusive-inclusive
    let above_min = match range.min_ts {
        None => true,
        Some(min) if range.min_inclusive => seg_min >= min,
        Some(min) => seg_min > min,
    };
    let below_max = match range.max_ts {
        None => true,
        Some(max) if range.max_inclusive => seg_max <= max,
        Some(max) => seg_max < max,
    };
    if above_min && below_max {
        return SegmentRangeClass::FullyInside;
    }
    // Outside: segment ends before the range begins or starts after it ends.
    let below_min = match range.min_ts {
        None => false,
        Some(min) if range.min_inclusive => seg_max < min,
        Some(min) => seg_max <= min,
    };
    let above_max = match range.max_ts {
        None => false,
        Some(max) if range.max_inclusive => seg_min > max,
        Some(max) => seg_min >= max,
    };
    if below_min || above_max {
        return SegmentRangeClass::Outside;
    }
    SegmentRangeClass::Boundary
}

/// Scan a single segment's timestamp column, counting rows whose
/// timestamp falls inside `range`.
fn scan_segment_count_in_range(
    reader: &dyn SegmentReader,
    handle: &SegmentHandle,
    ts_col: &str,
    range: &TimeRangeSpec,
) -> Result<u64> {
    let projection = ColumnProjection::with_columns([ts_col.to_string()]);
    let mut scan = reader.open_segment(handle, &projection, None)?;
    let mut total: u64 = 0;
    while let Some(batch) = scan.next_row_group()? {
        let col_idx = batch.schema().index_of(ts_col).map_err(|_| {
            BqliteError::Execution(format!(
                "DELETE rows_affected: timestamp column '{ts_col}' missing from scan output"
            ))
        })?;
        let arr = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                BqliteError::Execution(format!(
                    "DELETE rows_affected: '{ts_col}' is not TimestampNanosecondArray"
                ))
            })?;
        for i in 0..arr.len() {
            if !arr.is_null(i) && time_range_contains(range, arr.value(i)) {
                total = total.saturating_add(1);
            }
        }
    }
    Ok(total)
}

fn time_range_contains(range: &TimeRangeSpec, ts: i64) -> bool {
    let above_min = match range.min_ts {
        None => true,
        Some(min) if range.min_inclusive => ts >= min,
        Some(min) => ts > min,
    };
    let below_max = match range.max_ts {
        None => true,
        Some(max) if range.max_inclusive => ts <= max,
        Some(max) => ts < max,
    };
    above_min && below_max
}

/// Count entity-tombstone rows by scanning the entity-key column on
/// every segment in `target_shards`.
///
/// The entity-sorted layout means matching rows for one entity are
/// contiguous within a shard, but the linear scan still walks all
/// row-groups — Wave 4 does not yet plumb entity-zone-map skipping
/// here. Each row-group is a single column read; no full-row
/// materialization.
fn count_rows_by_entity(
    table: &str,
    entity_keys: &[ScalarValue],
    target_shards: &[u16],
    db: &Database,
) -> Result<u64> {
    let entry = match db.manifest().tables.get(table) {
        Some(e) => e,
        None => return Ok(0),
    };
    let entity_col = entry.schema.entity_key_column().name.clone();
    let entity_type = entry.schema.entity_key_column().bql_type.clone();

    let target: HashSet<u16> = target_shards.iter().copied().collect();
    let reader = db.segment_reader(table)?;

    // Build typed lookup sets to avoid per-row `ScalarValue` construction.
    let str_set: HashSet<String> = entity_keys
        .iter()
        .filter_map(|v| match v {
            ScalarValue::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    let int_set: HashSet<i64> = entity_keys
        .iter()
        .filter_map(|v| match v {
            ScalarValue::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    let projection = ColumnProjection::with_columns([entity_col.clone()]);
    let mut total: u64 = 0;
    for handle in reader.segments() {
        let handle = handle?;
        if !target.contains(&(handle.shard_id as u16)) {
            continue;
        }
        let mut scan = reader.open_segment(&handle, &projection, None)?;
        while let Some(batch) = scan.next_row_group()? {
            let col_idx = batch.schema().index_of(&entity_col).map_err(|_| {
                BqliteError::Execution(format!(
                    "DELETE rows_affected: entity-key column '{entity_col}' missing from scan output"
                ))
            })?;
            let col = batch.column(col_idx);
            total = total.saturating_add(count_entity_matches(
                col.as_ref(),
                &entity_type,
                &str_set,
                &int_set,
            )?);
        }
    }
    Ok(total)
}

fn count_entity_matches(
    col: &dyn Array,
    entity_type: &bqlite_core::BqlType,
    str_set: &HashSet<String>,
    int_set: &HashSet<i64>,
) -> Result<u64> {
    use bqlite_core::BqlType;
    let mut count: u64 = 0;
    match entity_type {
        BqlType::String => {
            let arr = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "DELETE rows_affected: expected StringViewArray for entity-key column"
                            .into(),
                    )
                })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) && str_set.contains(arr.value(i)) {
                    count += 1;
                }
            }
        }
        BqlType::Int => {
            let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution(
                    "DELETE rows_affected: expected Int64Array for entity-key column".into(),
                )
            })?;
            for i in 0..arr.len() {
                if !arr.is_null(i) && int_set.contains(&arr.value(i)) {
                    count += 1;
                }
            }
        }
        other => {
            return Err(BqliteError::Execution(format!(
                "DELETE rows_affected: unsupported entity-key type {other:?}"
            )));
        }
    }
    Ok(count)
}

/// Resolve the set of target shards for the DELETE's writes.
///
/// - When `entity_keys` is non-empty (any role), shards are the
///   unique image of `xxhash64(entity_id) % shard_count` over the
///   entity values.
/// - Otherwise (pure time-range / pure `__seq_id` / pure
///   `__batch_id`), shards span `0..shard_count`.
fn compute_target_shards(
    spec: &CheapDeleteSpec,
    shard_count: u16,
    entity_key_col: &str,
) -> Result<Vec<u16>> {
    if spec.entity_keys.is_empty() {
        return Ok((0..shard_count).collect());
    }
    let mut set: HashSet<u16> = HashSet::new();
    for value in &spec.entity_keys {
        let entity = scalar_to_entity_id(value).ok_or_else(|| {
            BqliteError::Execution(format!(
                "DELETE: entity-key column '{entity_key_col}' value {value:?} \
                 is not String/Int — cannot compute shard"
            ))
        })?;
        set.insert(shard_id_for(&entity, shard_count));
    }
    let mut out: Vec<u16> = set.into_iter().collect();
    out.sort_unstable();
    Ok(out)
}

fn scalar_to_entity_id(value: &ScalarValue) -> Option<EntityId> {
    match value {
        ScalarValue::String(s) => Some(EntityId::String(s.clone())),
        ScalarValue::Int(n) => Some(EntityId::Int(*n)),
        _ => None,
    }
}

fn time_range_spec_to_delete(spec: &TimeRangeSpec) -> TimeRangeDelete {
    TimeRangeDelete {
        min_ts: spec.min_ts,
        min_inclusive: spec.min_inclusive,
        max_ts: spec.max_ts,
        max_inclusive: spec.max_inclusive,
    }
}

/// True when the shard write should proceed for `window` given the
/// optional time-range bound. Pure time-range deletes skip windows
/// whose entire `[min_ts, max_ts]` lies outside the delete range —
/// no segment in such a window can match.
///
/// Conservative: when there is no time-range clause on the DELETE
/// (any other granularity), this returns `true` for every window.
/// Convenience wrapper used by tests outside this module to build a
/// `TombstoneFile` exactly like the cheap-class write path would,
/// without going through the full `execute_cheap_delete` driver.
#[cfg(test)]
pub(crate) fn build_tombstone_for_shard_for_tests(
    spec: &CheapDeleteSpec,
    shard_id: u16,
    shard_count: u16,
) -> TombstoneFile {
    build_tombstone_for_shard(spec, shard_id, shard_count)
}

fn time_range_overlaps_window(
    range: Option<&TimeRangeSpec>,
    window: &bqlite_storage::WindowManifest,
) -> bool {
    let Some(range) = range else {
        return true;
    };
    // Compute the window's actual ts span from its segments. An empty
    // window cannot match any range; outer caller already skips empty
    // (window, shard) pairs but we may be called for a window with
    // only some shards populated.
    let mut win_min: Option<i64> = None;
    let mut win_max: Option<i64> = None;
    for shard_segs in &window.shards {
        for seg in shard_segs {
            let (lo, hi) = seg.ts_range;
            win_min = Some(win_min.map_or(lo, |m| m.min(lo)));
            win_max = Some(win_max.map_or(hi, |m| m.max(hi)));
        }
    }
    let (Some(lo), Some(hi)) = (win_min, win_max) else {
        return false;
    };
    // Overlap: range_min <= hi AND range_max >= lo (with inclusivity).
    let above_lo = match range.max_ts {
        None => true,
        Some(max) if range.max_inclusive => max >= lo,
        Some(max) => max > lo,
    };
    let below_hi = match range.min_ts {
        None => true,
        Some(min) if range.min_inclusive => min <= hi,
        Some(min) => min < hi,
    };
    above_lo && below_hi
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use bqlite_storage::{read_tombstone_file, tombstone_file_path, Database};

    use crate::Engine;

    use super::*;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-engine-delete-{label}-{pid}-{seq}"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn create_db_with_events(path: &Path) -> (Database, Engine) {
        let mut db = Database::create(path).expect("create db");
        let engine = Engine::new();
        engine
            .query(
                "CREATE TABLE events (\
                     user_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE, \
                     amount INT\
                 )",
                &mut db,
            )
            .expect("create events table");
        (db, engine)
    }

    fn insert_three_entities(db: &mut Database, engine: &Engine) {
        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('alice', 1700000000100000000, 'view',  2), \
                 ('bob',   1700000000200000000, 'click', 3), \
                 ('carol', 1700000000300000000, 'view',  4)",
                db,
            )
            .expect("insert");
    }

    // ── Entity-level DELETE ─────────────────────────────────────────

    #[test]
    fn entity_equality_delete_writes_tombstone_and_returns_count() {
        let scratch = Scratch::new("entity-eq");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        let result = engine
            .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
            .expect("DELETE must succeed");

        // Two rows for alice → rows_affected = 2.
        assert_eq!(result.rows_affected, Some(2));
        assert!(result.is_empty(), "DELETE produces no result rows");

        // Subsequent SELECT must not see alice's rows (tombstone-aware
        // scan from TASK-434 filters them out).
        let after = engine.query("events", &mut db).expect("query after delete");
        let mut entities: Vec<String> = Vec::new();
        for batch in &after.rows {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                entities.push(col.value(i).to_string());
            }
        }
        entities.sort();
        entities.dedup();
        assert_eq!(entities, vec!["bob", "carol"]);
    }

    #[test]
    fn entity_in_list_delete_targets_multiple_shards() {
        let scratch = Scratch::new("entity-in");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        let result = engine
            .query(
                "DELETE FROM events WHERE user_id IN ('alice', 'bob')",
                &mut db,
            )
            .expect("DELETE IN must succeed");

        // alice has 2 rows + bob has 1 row → 3 affected.
        assert_eq!(result.rows_affected, Some(3));

        let after = engine.query("events", &mut db).expect("query after delete");
        let mut entities: Vec<String> = Vec::new();
        for batch in &after.rows {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                entities.push(col.value(i).to_string());
            }
        }
        entities.sort();
        entities.dedup();
        assert_eq!(entities, vec!["carol"]);
    }

    // ── seq_id / batch_id DELETE ───────────────────────────────────

    #[test]
    fn seq_id_in_delete_returns_input_set_count() {
        let scratch = Scratch::new("seq-id");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        // Per design SS11, |seq_ids| is the count regardless of
        // existence on disk.
        let result = engine
            .query("DELETE FROM events WHERE __seq_id IN (0, 1, 2)", &mut db)
            .expect("DELETE must succeed");
        assert_eq!(result.rows_affected, Some(3));
    }

    #[test]
    fn seq_id_in_with_duplicates_deduplicates_count() {
        // `__seq_id IN (5, 5)` must report 1, not 2 — `|input_set|`
        // means set cardinality, not list length.
        let scratch = Scratch::new("seq-id-dup");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        let result = engine
            .query("DELETE FROM events WHERE __seq_id IN (5, 5)", &mut db)
            .expect("DELETE must succeed");
        assert_eq!(result.rows_affected, Some(1));
    }

    #[test]
    fn batch_id_delete_counts_via_segment_metadata() {
        let scratch = Scratch::new("batch-id");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        // The single ingest call created one batch — batch_id 0 with
        // 4 rows. Some shard layouts may end up with 0; just assert
        // the count is 4 if batch_id 0 covers all four rows.
        let result = engine
            .query("DELETE FROM events WHERE __batch_id = 0", &mut db)
            .expect("DELETE batch must succeed");
        assert_eq!(result.rows_affected, Some(4));
    }

    // ── Time-range DELETE ──────────────────────────────────────────

    #[test]
    fn time_range_lt_deletes_old_events() {
        let scratch = Scratch::new("time-lt");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        // ts < 1700000000200000000 → drop alice (both rows) but keep
        // bob (= boundary, exclusive) and carol.
        let result = engine
            .query("DELETE FROM events WHERE ts < 1700000000200000000", &mut db)
            .expect("DELETE ts < must succeed");
        assert_eq!(result.rows_affected, Some(2));

        let after = engine.query("events", &mut db).expect("query after delete");
        let mut count = 0;
        for batch in &after.rows {
            count += batch.num_rows();
        }
        assert_eq!(count, 2, "bob and carol survive");
    }

    #[test]
    fn time_range_two_bounds_via_and_collapse_inclusive() {
        // Two-bound time range via AND; the classifier collapses to a
        // single inclusive-on-both-sides TimeRangeSpec. (The
        // equivalent BETWEEN form is exercised at the classifier
        // level in `bqlite-planner` — the parser does not yet accept
        // BETWEEN inside a DELETE WHERE.)
        let scratch = Scratch::new("time-and");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        let result = engine
            .query(
                "DELETE FROM events \
                 WHERE ts >= 1700000000000000000 AND ts <= 1700000000300000000",
                &mut db,
            )
            .expect("DELETE ts AND must succeed");
        assert_eq!(result.rows_affected, Some(4));

        let after = engine.query("events", &mut db).unwrap();
        assert!(after.is_empty(), "all rows tombstoned");
    }

    // ── Entity + seq_id mix (AsShardTarget) ────────────────────────

    #[test]
    fn entity_plus_seq_id_returns_seq_id_count() {
        let scratch = Scratch::new("entity-plus-seq");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        // entity narrows shards; rows_affected = |seq_ids|.
        let result = engine
            .query(
                "DELETE FROM events WHERE user_id = 'alice' AND __seq_id IN (0, 1)",
                &mut db,
            )
            .expect("DELETE must succeed");
        assert_eq!(result.rows_affected, Some(2));
    }

    // ── Idempotent retry (deletes.md §10) ──────────────────────────

    #[test]
    fn cheap_delete_is_idempotent_under_retry() {
        let scratch = Scratch::new("idempotent");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        let r1 = engine
            .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
            .expect("first DELETE");
        let r2 = engine
            .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
            .expect("retry DELETE");

        // Same predicate → same count both times.
        assert_eq!(r1.rows_affected, r2.rows_affected);

        // On-disk tombstone state is unchanged after the retry —
        // `merge` is set-union for entity_deletes.
        let table_entry = db.manifest().tables["events"].clone();
        for window in &table_entry.windows {
            for (shard_idx, shard_segs) in window.shards.iter().enumerate() {
                if shard_segs.is_empty() {
                    continue;
                }
                let path =
                    tombstone_file_path(db.root(), "events", window.window_id, shard_idx as u16);
                let tf = read_tombstone_file(&path).unwrap();
                // alice's entity tombstone landed in exactly one shard
                // (the one she hashes to). Other shards have no entry
                // for her. We can't know which shard without
                // recomputing, but every entry is a String("alice")
                // when present.
                for v in &tf.entity_deletes {
                    if let ScalarValue::String(s) = v {
                        assert_eq!(s, "alice");
                    }
                }
            }
        }
    }

    // ── Zero-match DELETE (deletes.md §11.2) ───────────────────────

    #[test]
    fn entity_delete_for_nonexistent_entity_returns_zero() {
        let scratch = Scratch::new("zero-match");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        let result = engine
            .query("DELETE FROM events WHERE user_id = 'eve'", &mut db)
            .expect("DELETE must succeed");
        assert_eq!(result.rows_affected, Some(0));
    }

    #[test]
    fn entity_delete_on_empty_table_returns_zero() {
        // Fresh table with no rows — every cheap-class DELETE should
        // succeed with rows_affected = 0 and write no tombstones.
        let scratch = Scratch::new("empty-table");
        let (mut db, engine) = create_db_with_events(scratch.path());

        let result = engine
            .query("DELETE FROM events WHERE user_id = 'alice'", &mut db)
            .expect("DELETE on empty table must succeed");
        assert_eq!(result.rows_affected, Some(0));
    }

    // ── DELETE on unknown table ────────────────────────────────────

    #[test]
    fn delete_on_unknown_table_errors_at_plan_time() {
        let scratch = Scratch::new("unknown-table");
        let mut db = Database::create(scratch.path()).expect("create db");
        let engine = Engine::new();

        let err = engine
            .query("DELETE FROM ghost WHERE user_id = 'alice'", &mut db)
            .expect_err("must error");
        let msg = err.to_string();
        assert!(msg.contains("ghost"), "got: {msg}");
    }

    // ── Non-cheap predicate without ALLOW SCAN ─────────────────────

    #[test]
    fn delete_without_allow_scan_rejects_non_cheap() {
        let scratch = Scratch::new("reject-non-cheap");
        let (mut db, engine) = create_db_with_events(scratch.path());

        let err = engine
            .query("DELETE FROM events WHERE event_type != 'spam'", &mut db)
            .expect_err("must error");
        assert!(err.to_string().contains("ALLOW SCAN"), "got: {err}");
    }

    // ── per-shard tombstone partitioning (entity hashes to one shard) ──

    #[test]
    fn entity_tombstone_lands_in_targeted_shard_only() {
        // alice hashes to a single shard; the build_tombstone_for_shard
        // helper should place her in that shard and skip every other
        // shard. This guards against the cross-shard fan-out
        // regression.
        let spec = CheapDeleteSpec {
            entity_keys: vec![ScalarValue::String("alice".into())],
            entity_role: EntityRole::AsTombstone,
            seq_ids: vec![],
            batch_ids: vec![],
            time_range: None,
        };
        let shard_count: u16 = 8;
        let alice_shard = shard_id_for(&EntityId::String("alice".into()), shard_count);

        let mut populated_shards: Vec<u16> = Vec::new();
        for shard in 0..shard_count {
            let tf = build_tombstone_for_shard_for_tests(&spec, shard, shard_count);
            if !tf.entity_deletes.is_empty() {
                populated_shards.push(shard);
            }
        }
        assert_eq!(populated_shards, vec![alice_shard]);
    }

    // ── Time-range write skips windows outside the bounds ──────────

    #[test]
    fn pure_time_range_skips_unrelated_windows() {
        let scratch = Scratch::new("time-skip-windows");
        let (mut db, engine) = create_db_with_events(scratch.path());
        insert_three_entities(&mut db, &engine);

        // Range that does not overlap the ingest window — every
        // event is at ~1700000000_000_000_000 ns, this targets a
        // far-past window.
        let result = engine
            .query("DELETE FROM events WHERE ts < 100", &mut db)
            .expect("DELETE must succeed");
        assert_eq!(result.rows_affected, Some(0));

        // No tombstone files should be written — every shard's
        // tombstones.json is absent.
        let table_entry = db.manifest().tables["events"].clone();
        for window in &table_entry.windows {
            for (shard_idx, shard_segs) in window.shards.iter().enumerate() {
                if shard_segs.is_empty() {
                    continue;
                }
                let path =
                    tombstone_file_path(db.root(), "events", window.window_id, shard_idx as u16);
                assert!(
                    !path.exists(),
                    "no tombstone file should be written for non-overlapping range; \
                     path = {}",
                    path.display()
                );
            }
        }
    }
}
