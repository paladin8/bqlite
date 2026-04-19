//! Size-tiered compaction (TASK-408).
//!
//! Implements `docs/design/storage/compaction-concurrency.md` §§2–9 for
//! a single `(table, window, shard)` job at a time.
//!
//! # Layering (CP1 surface)
//!
//! - [`CompactionConfig`] — user-facing thresholds and pool sizing.
//! - [`CoreBudget`] — the §4 semaphore. Compaction acquires one permit
//!   per job; queries (TASK-438, future) will acquire `worker_count`
//!   permits on start. Until TASK-438 lands, the budget is uncontested
//!   and the acquire/release pair is a cheap no-op.
//! - [`CompactionMetrics`] — observable backlog, exposed per
//!   compaction-concurrency.md §5 ("Observability requirement").
//!
//! # What this module deliberately does NOT do
//!
//! - It does not consult `tombstones.json` — TASK-434 / TASK-435 own
//!   the tombstone-aware filtering and reclamation extension.
//! - It does not run a 10-second `Arc::strong_count` reclamation sweep
//!   — superseded segment files are deleted immediately because today's
//!   `Database` does not hand out `Arc<Manifest>` snapshots; see the
//!   design doc's §12 implementation status.
//!
//! Later checkpoints (CP2–CP5) layer on the executor (`compact_one`),
//! the synchronous `Database::compact_now` API, and the background
//! scheduler. CP1 is intentionally limited to the configuration,
//! semaphore, and metric surfaces so they can be reused without
//! pulling in the executor's dependency graph.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

// ── Configuration ───────────────────────────────────────────────────────────

/// User-facing tunables for the compaction subsystem.
///
/// All fields have production-sensible defaults via
/// [`CompactionConfig::default`]; tests override individual fields with
/// the struct-update syntax. Defaults match
/// `docs/design/storage/compaction-concurrency.md` §3.1, §3.2, and
/// §8.3.
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    /// L0 segment count above which a `(window, shard)` becomes
    /// eligible. Matches compaction-concurrency.md §3.2 default.
    pub l0_count_trigger: u32,
    /// L0 total byte size above which a `(window, shard)` becomes
    /// eligible. Matches compaction-concurrency.md §3.2 default
    /// (256 MiB).
    pub l0_size_trigger_bytes: u64,
    /// Background scheduler pool size. Default `max(1, num_cores / 4)`
    /// per §3.1.
    pub pool_size: usize,
    /// Total core-budget permits. Default `num_cores` per §4.1.
    pub core_budget_permits: usize,
    /// Cooldown after a failed job before the same `(window, shard)`
    /// becomes eligible to retry. Matches §8.3 (60 s).
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
/// Compaction acquires permits before starting work; queries (when
/// TASK-438 lands) will acquire `worker_count` permits at start and
/// release on finalization. Built on `Mutex` + `Condvar` so we don't
/// take a new dependency.
///
/// The §4 design calls for the compaction worker to acquire one permit
/// per row-group boundary. The CP1 surface ships the type; the v1
/// executor in CP3 acquires one permit per job because it materialises
/// the whole merge in one pass (see §12.1 in the design doc for the
/// streaming follow-on that hoists the acquire/release back inside the
/// row-group loop).
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
    /// Construct a budget pre-loaded with `permits` and return it
    /// behind an `Arc` so it can be shared across the scheduler's
    /// worker threads.
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

    /// Currently available permits. Test/observability helper; the
    /// hot path acquires permits via [`Self::acquire`].
    pub fn available(&self) -> usize {
        *self.state.lock().expect("CoreBudget mutex poisoned")
    }
}

impl Drop for CoreBudgetPermit<'_> {
    fn drop(&mut self) {
        let mut g = self.budget.state.lock().expect("CoreBudget mutex poisoned");
        *g += 1;
        self.budget.cv.notify_one();
    }
}

// ── Metrics ─────────────────────────────────────────────────────────────────

/// Observable counters the operator can read at any time.
///
/// Surfaced per compaction-concurrency.md §5 ("Observability
/// requirement"). Backed by a `Mutex<HashMap>` because the
/// per-`(table, window, shard)` backlog set is sparse and small; an
/// atomic-per-key map would over-engineer a surface no hot path
/// consults.
#[derive(Debug, Default)]
pub struct CompactionMetrics {
    inner: Mutex<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    /// Per-`(table, window_id, shard_id)` L0 segment count, refreshed
    /// on every scheduler eligibility evaluation pass.
    backlog: HashMap<(String, u32, u32), u64>,
}

impl CompactionMetrics {
    /// Construct a fresh, empty metrics handle wrapped in an `Arc` so
    /// the scheduler and external observers can share it.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Replace the per-key L0 count for one bucket. Used by the
    /// scheduler's eligibility pass. A count of zero removes the
    /// entry so the snapshot stays compact.
    pub fn set_backlog(&self, table: &str, window_id: u32, shard_id: u32, l0_count: u64) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let key = (table.to_string(), window_id, shard_id);
        if l0_count == 0 {
            inner.backlog.remove(&key);
        } else {
            inner.backlog.insert(key, l0_count);
        }
    }

    /// Snapshot of every non-zero bucket, ordered arbitrarily.
    /// Allocates; intended for metrics scrape paths and tests, not the
    /// hot path.
    pub fn backlog_snapshot(&self) -> Vec<(String, u32, u32, u64)> {
        let inner = self.inner.lock().expect("metrics mutex poisoned");
        inner
            .backlog
            .iter()
            .map(|((t, w, s), c)| (t.clone(), *w, *s, *c))
            .collect()
    }
}

// ── Executor: compact_one ───────────────────────────────────────────────────

use ::arrow::array::Array;
use ::arrow::datatypes::{Field, Schema as ArrowSchema};
use ::arrow::record_batch::RecordBatch;

use bqlite_core::arrow::bql_type_to_arrow;
use bqlite_core::error::{BqliteError, Result};
use bqlite_core::property::PropertyValue;
use bqlite_core::storage::ColumnProjection;

use crate::database::Database;
use crate::manifest::{ColumnStats, SegmentMeta};
use crate::segment::layout::{SEGMENT_FORMAT_VERSION_V1, SEGMENT_FORMAT_VERSION_V2};
use crate::segment::merge::{KWayMergeScan, DEFAULT_MERGE_BATCH_ROWS};
use crate::segment::reader::SegmentFileReader;
use crate::segment::writer::{
    write_segment, PreparedColumnChunk, PreparedDictionary, PreparedFsstSymbolTable,
    PreparedRowGroup, SegmentWriteRequest,
};
use crate::writer::{build_column_chunk, merge_extrema, ColumnAggregate, DEFAULT_ROW_GROUP_SIZE};

/// Outcome of a single `(table, window, shard)` compaction job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionOutcome {
    /// IDs of the segments replaced by this compaction.
    pub input_segment_ids: Vec<u64>,
    /// IDs of the segments produced. v1 always outputs one segment;
    /// future subcompaction splits may produce many.
    pub output_segment_ids: Vec<u64>,
    /// Total byte size of the inputs before compaction.
    pub input_byte_size: u64,
    /// Byte size of the merged output.
    pub output_byte_size: u64,
}

/// Run one compaction job synchronously on the calling thread.
///
/// Picks every segment currently in `(table, window_id, shard_id)`,
/// k-way merges them in `(entity_id, ts)` order, re-encodes through
/// the latest selector, publishes the replacement via the
/// [`Database::replace_segments`] atomic primitive, and deletes the
/// superseded segment files.
///
/// **Pre-condition:** at least two input segments must be present.
/// Compacting a single segment would be wasteful churn; the scheduler
/// enforces this gate, and this function surfaces it as an error for
/// direct callers (tests / CLI).
///
/// **Failure semantics:**
///
/// - Any error before the manifest publish leaves the manifest
///   untouched. Any partial output file is cleaned up best-effort;
///   the next startup orphan sweep
///   ([`crate::segment::cleanup::reconcile_segments`]) reaps whatever
///   escapes.
/// - The publish itself uses the §6 atomic primitive, so a crash
///   during publish is impossible from the caller's perspective —
///   either the new manifest lands or the old one persists.
/// - After successful publish, input-file deletion is best-effort:
///   a transient unlink failure is recovered by the next startup
///   sweep.
///
/// This is a `&mut Database` call — one compaction at a time per
/// database. Concurrent compactions on disjoint `(table, window,
/// shard)` triples are a scheduler-level concern.
pub fn compact_one(
    db: &mut Database,
    table: &str,
    window_id: u32,
    shard_id: u32,
) -> Result<CompactionOutcome> {
    // ── 1. Snapshot inputs from the manifest. ───────────────────────
    let (shard_segments, table_schema, schema_version, entity_key_name, ts_name) = {
        let manifest = db.manifest();
        let entry = manifest.tables.get(table).ok_or_else(|| {
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
        let shard = win.shards.get(shard_id as usize).ok_or_else(|| {
            BqliteError::Execution(format!(
                "compact_one: shard {shard_id} out of range for window {window_id}"
            ))
        })?;
        if shard.len() < 2 {
            return Err(BqliteError::Execution(format!(
                "compact_one: need at least 2 input segments, found {}",
                shard.len()
            )));
        }
        (
            shard.clone(),
            entry.schema.clone(),
            entry.schema.version(),
            entry.schema.entity_key_column().name.clone(),
            entry.schema.timestamp_column().name.clone(),
        )
    };
    let input_ids: Vec<u64> = shard_segments.iter().map(|s| s.segment_id).collect();
    let input_byte_size: u64 = shard_segments.iter().map(|s| s.byte_size).sum();
    let max_input_level = shard_segments.iter().map(|s| s.level).max().unwrap_or(0);

    // ── 2. Build the canonical Arrow schema from the table schema. ──
    // Matches `writer::events_to_record_batch` and the reader's scan
    // plan, so every scan's output batch and the merger's output batch
    // all carry identical schemas.
    let arrow_fields: Vec<Field> = table_schema
        .columns()
        .iter()
        .map(|c| Field::new(&c.name, bql_type_to_arrow(&c.bql_type), c.nullable))
        .collect();
    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    // ── 3. Open each input and build a SegmentScan. ─────────────────
    let db_root = db.root().to_path_buf();
    let shared_schema = Arc::new(table_schema.clone());
    let mut scans: Vec<Box<dyn bqlite_core::storage::SegmentScan>> =
        Vec::with_capacity(shard_segments.len());
    for seg in &shard_segments {
        let path = segment_path(&db_root, table, window_id, shard_id, seg.segment_id);
        let reader = SegmentFileReader::open_shared(&path, shared_schema.clone())?;
        let scan = reader.scan(&ColumnProjection::all(), None)?;
        scans.push(Box::new(scan));
    }

    // ── 4. Resolve entity / ts key ordinals against the Arrow schema. ─
    let entity_key_col = arrow_schema.index_of(&entity_key_name).map_err(|_| {
        BqliteError::Execution(format!(
            "compact_one: entity key column '{entity_key_name}' missing from merged schema"
        ))
    })?;
    let ts_col = arrow_schema.index_of(&ts_name).map_err(|_| {
        BqliteError::Execution(format!(
            "compact_one: ts column '{ts_name}' missing from merged schema"
        ))
    })?;

    // ── 5. K-way merge inputs into one in-memory super-batch. ───────
    // Per design doc §12.1, v1 materialises the whole merged stream
    // in memory; the streaming row-group writer is a Wave 5 follow-on.
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
            "compact_one: merged stream was empty — every input is zero-row".into(),
        ));
    }
    let merged = ::arrow::compute::concat_batches(&arrow_schema, &merged_batches)
        .map_err(|e| BqliteError::Execution(format!("compact_one: concat_batches: {e}")))?;

    // ── 6. Plan row groups, respecting entity locality. ─────────────
    let groups =
        plan_row_groups_from_entity_column(merged.column(entity_key_col), DEFAULT_ROW_GROUP_SIZE);
    debug_assert!(!groups.is_empty());

    // ── 7. Encode each row group via the writer's shared helper. ────
    // `build_column_chunk` does nullability validation, null bitmap
    // derivation, encoding selection, and dict/FSST hoisting — the
    // same pipeline ingest uses.
    let mut prepared_groups: Vec<PreparedRowGroup> = Vec::with_capacity(groups.len());
    let mut segment_dicts: Vec<PreparedDictionary> = Vec::new();
    let mut fsst_tables: Vec<PreparedFsstSymbolTable> = Vec::new();
    let mut column_aggregates: Vec<ColumnAggregate> = table_schema
        .columns()
        .iter()
        .map(|c| ColumnAggregate {
            column_name: c.name.clone(),
            min: None,
            max: None,
            null_count: 0,
        })
        .collect();
    let mut promotes_to_v2 = false;
    for grp in &groups {
        let group_len = grp.end - grp.start;
        let group_batch = merged.slice(grp.start, group_len);
        let mut prepared_columns: Vec<PreparedColumnChunk> =
            Vec::with_capacity(table_schema.columns().len());
        for (col_ord, col_def) in table_schema.columns().iter().enumerate() {
            let array = group_batch.column(col_ord).clone();
            let chunk = build_column_chunk(
                col_ord as u32,
                col_def,
                &array,
                &mut segment_dicts,
                &mut fsst_tables,
            )?;
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

    // ── 8. Allocate monotonic ids for the output segment. ──────────
    //
    // Note on `__seq_id` remapping: we allocate a fresh contiguous
    // range for the merged output, which means each input row's
    // original `__seq_id` value is replaced. This matches the v1
    // segment-format contract from `manifest.rs::SegmentMeta` —
    // `__seq_id = first + n` is positional within a segment, so
    // preserving original ids would require non-contiguous ranges
    // (TASK-435 territory). External references keyed on a
    // pre-compaction `__seq_id` (notably row-id tombstones in
    // `tombstone.rs`) are out of scope for TASK-408 and are TASK-434
    // / TASK-435's job to translate or invalidate when they wire
    // tombstone-aware compaction.
    //
    // Counters bump even when we error out below (write_segment /
    // replace_segments failures retire the allocated ids as gaps);
    // §6.2 of `storage-format.md` allows gaps in all three counters.
    let new_segment_id = db.allocate_segment_id(table)?;
    let row_count = merged.num_rows() as u64;
    let seq_id_range = db.allocate_sequence_id_range(table, row_count)?;
    let batch_id = db.allocate_batch_id(table)?;
    let format_version = if promotes_to_v2 {
        SEGMENT_FORMAT_VERSION_V2
    } else {
        SEGMENT_FORMAT_VERSION_V1
    };
    let creation_ts_ns = current_timestamp_ns();
    let new_level = max_input_level.saturating_add(1);
    let request = SegmentWriteRequest {
        schema: table_schema.clone(),
        schema_version,
        row_groups: prepared_groups,
        dictionaries: segment_dicts,
        fsst_symbol_tables: fsst_tables,
        creation_timestamp_ns: creation_ts_ns,
        seq_id_range,
        batch_id,
        compaction_level: new_level,
        format_version,
    };

    // ── 9. Write the new segment file atomically. ───────────────────
    let new_path = segment_path(&db_root, table, window_id, shard_id, new_segment_id);
    if let Some(parent) = new_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            BqliteError::Io(std::io::Error::new(
                e.kind(),
                format!("compact_one: create segment dir {}: {e}", parent.display()),
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

    // ── 10. Derive entity_range / ts_range for the SegmentMeta. ────
    let (entity_range, ts_range) = compute_segment_ranges(&merged, entity_key_col, ts_col)?;

    // ── 11. Publish atomically via the CP2 primitive. ──────────────
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
        level: new_level,
        schema_version,
        row_count,
        byte_size: summary.byte_size,
        ts_range,
        entity_range,
        column_stats,
        created_at: creation_ts_ns,
        batch_id,
        seq_id_range,
    };
    if let Err(e) = db.replace_segments(table, window_id, shard_id, &input_ids, new_meta) {
        let _ = std::fs::remove_file(&new_path);
        return Err(e);
    }

    // ── 12. Reap the superseded input files (best-effort). ────────
    // The startup orphan sweep (TASK-239 `reconcile_segments`) handles
    // whatever escapes a transient unlink failure here.
    for old_id in &input_ids {
        let path = segment_path(&db_root, table, window_id, shard_id, *old_id);
        let _ = std::fs::remove_file(&path);
    }

    Ok(CompactionOutcome {
        input_segment_ids: input_ids,
        output_segment_ids: vec![new_segment_id],
        input_byte_size,
        output_byte_size: summary.byte_size,
    })
}

// ── compact_one helpers ─────────────────────────────────────────────────────

fn segment_path(
    db_root: &std::path::Path,
    table: &str,
    window_id: u32,
    shard_id: u32,
    segment_id: u64,
) -> std::path::PathBuf {
    db_root
        .join(table)
        .join("windows")
        .join(format!("w_{window_id:06}"))
        .join(format!("shard_{shard_id:02}"))
        .join(format!("segment_{segment_id}.seg"))
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
/// column. Mirrors [`crate::writer::plan_row_groups`] step-for-step
/// (the canonical reference for the §7.2 entity-locality invariant)
/// but operates against an Arrow column — the compaction path never
/// has a `&[Event]` slice.
///
/// Contract (lifted verbatim from the writer's `plan_row_groups`):
/// - When the input is empty, returns an empty vec.
/// - When the natural cut at `target_size` lands on an entity
///   boundary, the row group ends there.
/// - When the natural cut would split an entity, the boundary backs
///   up to the previous entity transition.
/// - When backing up would empty the current row group (i.e. one
///   entity is wider than `target_size`), the row group fills to
///   `target_size` and the entity continues into the next row group.
fn plan_row_groups_from_entity_column(
    entity_col: &::arrow::array::ArrayRef,
    target_size: usize,
) -> Vec<std::ops::Range<usize>> {
    assert!(target_size > 0, "target_size must be > 0");
    let n = entity_col.len();
    if n == 0 {
        return Vec::new();
    }
    // Build a cheap entity-equality probe that works for the three
    // entity-key array types the merge guarantees. Cloning the typed
    // array is an `Arc` bump (Arrow arrays are Arc-backed), not a
    // data copy.
    let entities_eq: Box<dyn Fn(usize, usize) -> bool> = if let Some(arr) =
        entity_col
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
        // The k-way merge validates the entity-key array type at
        // construction; this branch is unreachable in practice. Fail
        // loudly rather than silently produce a degenerate plan.
        unreachable!(
            "compact_one: unsupported entity-key array type {:?} reached row-group planner",
            entity_col.data_type()
        );
    };

    let mut out: Vec<std::ops::Range<usize>> = Vec::new();
    let mut start = 0usize;
    while start < n {
        let raw_end = (start + target_size).min(n);
        let end = if raw_end == n {
            // Last group, possibly short — the remainder fits.
            raw_end
        } else if !entities_eq(raw_end - 1, raw_end) {
            // Natural cut on an entity boundary.
            raw_end
        } else {
            // Mid-entity cut at raw_end. Walk back to the start of
            // the entity straddling it. If that walk would empty the
            // current row group (single entity wider than
            // target_size), fall back to raw_end so the entity fills
            // a target-sized group and continues into the next.
            let mut back = raw_end;
            while back > start && entities_eq(back - 1, raw_end) {
                back -= 1;
            }
            if back == start {
                raw_end
            } else {
                back
            }
        };
        out.push(start..end);
        start = end;
    }
    out
}

/// Compute `(entity_range, ts_range)` directly from the merged batch.
/// The batch is already `(entity_id, ts)` sorted, so entity_range is
/// simply `(first, last)`. Timestamps are only sorted *per entity*,
/// so ts_range requires a linear scan — two passes over an i64 array
/// are cheap.
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
        (
            PropertyValue::String(arr.value(0).to_string()),
            PropertyValue::String(arr.value(n - 1).to_string()),
        )
    } else if let Some(arr) = ent.as_any().downcast_ref::<StringArray>() {
        (
            PropertyValue::String(arr.value(0).to_string()),
            PropertyValue::String(arr.value(n - 1).to_string()),
        )
    } else if let Some(arr) = ent.as_any().downcast_ref::<Int64Array>() {
        (
            PropertyValue::Int(arr.value(0)),
            PropertyValue::Int(arr.value(n - 1)),
        )
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
    let vals = ts_arr.values();
    let mut tmin = vals[0];
    let mut tmax = vals[0];
    for v in &vals[1..] {
        if *v < tmin {
            tmin = *v;
        }
        if *v > tmax {
            tmax = *v;
        }
    }
    Ok((entity_range, (tmin, tmax)))
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
        m.set_backlog("events", 1, 0, 0); // zero -> never inserted
        let mut snap = m.backlog_snapshot();
        snap.sort();
        assert_eq!(
            snap,
            vec![
                ("events".to_string(), 0, 0, 5),
                ("events".to_string(), 0, 1, 7),
            ]
        );
        // Setting an existing entry to zero removes it.
        m.set_backlog("events", 0, 0, 0);
        let snap = m.backlog_snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0], ("events".to_string(), 0, 1, 7));
    }

    // ── compact_one executor tests (CP3) ────────────────────────────────────

    use bqlite_core::event::Event;
    use bqlite_core::property::BqlType;
    use bqlite_core::schema::{ColumnDef, TableSchema};
    use bqlite_core::time::Timestamp;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static COMPACT_SEQ: AtomicU64 = AtomicU64::new(0);

    struct ScratchDir {
        path: PathBuf,
    }

    impl ScratchDir {
        fn new(label: &str) -> Self {
            let seq = COMPACT_SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut p = std::env::temp_dir();
            p.push(format!("bqlite-compaction-{label}-{pid}-{seq}"));
            Self { path: p }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
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
        Event::new(
            bqlite_core::event::EntityId::String(entity.into()),
            Timestamp::from_nanos(ts_ns),
            event_type,
        )
    }

    fn ingest_one_segment(
        db: &mut Database,
        table: &str,
        window_id: u32,
        shard_id: u16,
        events: &[Event],
    ) -> SegmentMeta {
        use crate::writer::SegmentWriter;
        let batch_id = db.allocate_batch_id(table).unwrap();
        let mut w = SegmentWriter::new(db);
        w.write_bucket(table, window_id, shard_id, batch_id, events)
            .expect("write_bucket")
    }

    fn read_all_rows(db: &Database) -> Vec<(String, i64, String)> {
        use ::arrow::array::{Int64Array, StringArray, StringViewArray, TimestampNanosecondArray};
        let reader = db.segment_reader("events").unwrap();
        let mut out: Vec<(String, i64, String)> = Vec::new();
        for handle_res in reader.segments() {
            let handle = handle_res.unwrap();
            let mut scan = reader
                .open_segment(&handle, &ColumnProjection::all(), None)
                .unwrap();
            while let Some(batch) = scan.next_row_group().unwrap() {
                let entity = batch.column(0);
                let ts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let event_type = batch.column(2);
                let pull_entity = |i: usize| -> String {
                    if let Some(a) = entity.as_any().downcast_ref::<StringViewArray>() {
                        a.value(i).to_string()
                    } else if let Some(a) = entity.as_any().downcast_ref::<StringArray>() {
                        a.value(i).to_string()
                    } else if let Some(a) = entity.as_any().downcast_ref::<Int64Array>() {
                        a.value(i).to_string()
                    } else {
                        panic!("unsupported entity array type")
                    }
                };
                let pull_et = |i: usize| -> String {
                    if let Some(a) = event_type.as_any().downcast_ref::<StringViewArray>() {
                        a.value(i).to_string()
                    } else if let Some(a) = event_type.as_any().downcast_ref::<StringArray>() {
                        a.value(i).to_string()
                    } else {
                        panic!("unsupported event_type array type")
                    }
                };
                for i in 0..batch.num_rows() {
                    out.push((pull_entity(i), ts.value(i), pull_et(i)));
                }
            }
        }
        out
    }

    #[test]
    fn compact_one_merges_two_input_segments_into_one() {
        let scratch = ScratchDir::new("merge-two");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

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
        assert_eq!(
            outcome.input_segment_ids,
            vec![s1.segment_id, s2.segment_id]
        );
        assert_eq!(outcome.output_segment_ids.len(), 1);
        let out_id = outcome.output_segment_ids[0];

        // Manifest: only the compacted output survives.
        let entry = db.manifest().tables.get("events").unwrap();
        let live: Vec<u64> = entry.windows[0].shards[0]
            .iter()
            .map(|s| s.segment_id)
            .collect();
        assert_eq!(live, vec![out_id]);

        // Old files physically removed from disk.
        let p1 = scratch
            .path()
            .join("events/windows/w_000000/shard_00/segment_0.seg");
        let p2 = scratch
            .path()
            .join("events/windows/w_000000/shard_00/segment_1.seg");
        assert!(!p1.exists());
        assert!(!p2.exists());

        // Level promotion.
        let out_meta = &entry.windows[0].shards[0][0];
        assert!(out_meta.level >= 1, "compaction must promote level");
        assert_eq!(
            out_meta.row_count,
            (s1_events.len() + s2_events.len()) as u64
        );

        // Read-back: every input row is present, in `(entity, ts)` order.
        let rows = read_all_rows(&db);
        let expected = vec![
            ("alice".to_string(), 100, "click".to_string()),
            ("alice".to_string(), 200, "view".to_string()),
            ("bob".to_string(), 50, "click".to_string()),
            ("bob".to_string(), 250, "view".to_string()),
            ("carol".to_string(), 150, "click".to_string()),
            ("carol".to_string(), 175, "view".to_string()),
        ];
        assert_eq!(rows, expected);
    }

    #[test]
    fn compact_one_rejects_unknown_table() {
        let scratch = ScratchDir::new("unknown-table");
        let mut db = Database::create(scratch.path()).unwrap();
        let err = compact_one(&mut db, "nope", 0, 0).expect_err("must fail");
        assert!(matches!(err, BqliteError::Execution(_)), "got {err:?}");
    }

    #[test]
    fn compact_one_rejects_unknown_window() {
        let scratch = ScratchDir::new("unknown-window");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        // No ingest — window 0 does not exist yet.
        let err = compact_one(&mut db, "events", 0, 0).expect_err("must fail");
        assert!(matches!(err, BqliteError::Execution(_)), "got {err:?}");
    }

    #[test]
    fn compact_one_rejects_too_few_inputs() {
        let scratch = ScratchDir::new("too-few");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[make_event("alice", 100, "click")],
        );
        let err = compact_one(&mut db, "events", 0, 0).expect_err("must fail");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("at least 2"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn compact_one_promotes_level_above_max_input_level() {
        let scratch = ScratchDir::new("level-promote");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        let s1 = ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 1, "x")]);
        let s2 = ingest_one_segment(&mut db, "events", 0, 0, &[make_event("b", 2, "x")]);
        let _ = compact_one(&mut db, "events", 0, 0).unwrap();
        let entry = db.manifest().tables.get("events").unwrap();
        let surviving: Vec<&SegmentMeta> = entry.windows[0].shards[0].iter().collect();
        assert_eq!(surviving.len(), 1);
        let max_in = s1.level.max(s2.level);
        assert_eq!(surviving[0].level, max_in.saturating_add(1));
    }

    #[test]
    fn compact_one_preserves_rows_across_multiple_compactions() {
        // Recursive compaction: after two passes the answer is still
        // the exact multiset of input rows, in (entity, ts) order.
        let scratch = ScratchDir::new("recursive");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        let batches = vec![
            vec![make_event("a", 1, "x"), make_event("c", 3, "y")],
            vec![make_event("a", 2, "y"), make_event("b", 1, "z")],
            vec![make_event("b", 5, "x"), make_event("d", 7, "y")],
        ];
        for events in &batches {
            ingest_one_segment(&mut db, "events", 0, 0, events);
        }
        compact_one(&mut db, "events", 0, 0).unwrap();
        // Now there is only one segment; compact_one's >=2 gate fires.
        let err = compact_one(&mut db, "events", 0, 0).unwrap_err();
        assert!(matches!(err, BqliteError::Execution(_)));

        // Add two more segments and compact again — the level must
        // promote past the already-compacted level.
        ingest_one_segment(&mut db, "events", 0, 0, &[make_event("e", 2, "x")]);
        ingest_one_segment(&mut db, "events", 0, 0, &[make_event("f", 3, "x")]);
        let outcome = compact_one(&mut db, "events", 0, 0).unwrap();
        assert_eq!(outcome.input_segment_ids.len(), 3);
        let entry = db.manifest().tables.get("events").unwrap();
        let surviving = &entry.windows[0].shards[0];
        assert_eq!(surviving.len(), 1);
        assert!(
            surviving[0].level >= 2,
            "recursive compaction must promote level past the prior level, got {}",
            surviving[0].level
        );

        // Read-back preserves every row in (entity, ts) order.
        let rows = read_all_rows(&db);
        let expected = vec![
            ("a".to_string(), 1, "x".to_string()),
            ("a".to_string(), 2, "y".to_string()),
            ("b".to_string(), 1, "z".to_string()),
            ("b".to_string(), 5, "x".to_string()),
            ("c".to_string(), 3, "y".to_string()),
            ("d".to_string(), 7, "y".to_string()),
            ("e".to_string(), 2, "x".to_string()),
            ("f".to_string(), 3, "x".to_string()),
        ];
        assert_eq!(rows, expected);
    }

    #[test]
    fn compact_one_output_publish_survives_reopen() {
        // End-to-end durability: close the database after compaction,
        // reopen, and confirm the post-compaction state is exactly what
        // we observed in-memory.
        let scratch = ScratchDir::new("reopen");
        {
            let mut db = Database::create(scratch.path()).unwrap();
            db.create_table("events".into(), events_schema()).unwrap();
            ingest_one_segment(
                &mut db,
                "events",
                0,
                0,
                &[make_event("a", 1, "x"), make_event("b", 2, "y")],
            );
            ingest_one_segment(
                &mut db,
                "events",
                0,
                0,
                &[make_event("c", 1, "z"), make_event("d", 2, "x")],
            );
            compact_one(&mut db, "events", 0, 0).unwrap();
        }
        let db = Database::open(scratch.path()).expect("reopen");
        let entry = db.manifest().tables.get("events").unwrap();
        assert_eq!(entry.windows[0].shards[0].len(), 1);
        let rows = read_all_rows(&db);
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].0, "a");
        assert_eq!(rows[3].0, "d");
    }

    // ── Property test: compaction round-trips any event stream ──────────────
    //
    // Per CLAUDE.md §"Testing And Benchmarking", compaction is one of
    // the surfaces where a property test is the documented bar because
    // "for any X, Y must hold" is the natural way to state the invariant:
    //
    //   For any batched stream of events in the same (window, shard),
    //   the multiset of rows in the output segment equals the multiset
    //   of input rows.
    //
    // We hand-roll the generator to keep the test small and avoid
    // pulling in proptest as a dev-dep for one test; the strategies in
    // `tests/src/strategies.rs` are the Arrow-shaped generators the
    // doc references and are the right place to graduate this if the
    // fuzz surface grows.

    fn deterministic_event_stream(seed: u64, n: usize) -> Vec<Event> {
        // LCG — cheap, reproducible, good enough to exercise the
        // compaction pipeline with variable entity/ts distributions.
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mut next = |cap: u64| -> u64 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % cap.max(1)
        };
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            let entity = format!("e{:02}", next(8)); // 8 entities
            let ts = next(10_000) as i64 + 1;
            let et = match next(3) {
                0 => "click",
                1 => "view",
                _ => "scroll",
            };
            out.push(make_event(&entity, ts, et));
        }
        // Sort each ingest batch by (entity, ts) so it satisfies the
        // partitioner's pre-condition.
        out.sort_by(|a, b| {
            (
                entity_key_as_str(&a.entity),
                a.timestamp.as_nanos(),
                a.event_type.clone(),
            )
                .cmp(&(
                    entity_key_as_str(&b.entity),
                    b.timestamp.as_nanos(),
                    b.event_type.clone(),
                ))
        });
        out
    }

    fn entity_key_as_str(entity: &bqlite_core::event::EntityId) -> String {
        match entity {
            bqlite_core::event::EntityId::String(s) => s.clone(),
            bqlite_core::event::EntityId::Int(i) => i.to_string(),
        }
    }

    #[test]
    fn plan_row_groups_from_entity_column_handles_wide_entity_split() {
        // A single entity wider than target_size must (a) fill the
        // first group to target_size and (b) continue into the next
        // group, NOT collapse to N single-row groups (the bug a
        // previous reviewer caught).
        use ::arrow::array::{ArrayRef, StringViewArray};
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec!["alice"; 10]));
        let groups = plan_row_groups_from_entity_column(&arr, 4);
        // Expect exactly 3 groups: [0..4, 4..8, 8..10].
        assert_eq!(groups, vec![0..4, 4..8, 8..10]);
    }

    #[test]
    fn plan_row_groups_from_entity_column_cuts_on_entity_boundary() {
        // Mixed entities: cut should land on the boundary, not split
        // an entity.
        use ::arrow::array::{ArrayRef, StringViewArray};
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec![
            "a", "a", "a", // 0..3
            "b", "b", // 3..5
            "c", "c", "c", // 5..8
        ]));
        // target_size=4 would want to cut at 4 (mid-"b"); the
        // planner backs off to 3 (a→b boundary). The next group
        // starts at 3, raw_end=7 lands mid-"c", planner backs off
        // to the c-start at 5. Final group is 5..8.
        let groups = plan_row_groups_from_entity_column(&arr, 4);
        assert_eq!(groups, vec![0..3, 3..5, 5..8]);
    }

    #[test]
    fn plan_row_groups_from_entity_column_natural_cut_on_boundary() {
        use ::arrow::array::{ArrayRef, StringViewArray};
        let arr: ArrayRef = Arc::new(StringViewArray::from(vec![
            "a", "a", // 0..2
            "b", "b", // 2..4
            "c", "c", // 4..6
        ]));
        // target_size=2 means each cut already lands on a boundary.
        let groups = plan_row_groups_from_entity_column(&arr, 2);
        assert_eq!(groups, vec![0..2, 2..4, 4..6]);
    }

    #[test]
    fn property_compact_one_preserves_row_multiset_for_many_seeds() {
        // Ten seeds × 20–50 events × 2–5 segments. Each iteration is
        // an independent fresh database. The invariant:
        //   sort(pre-compaction rows) == sort(post-compaction rows).
        for seed in 0u64..10 {
            let scratch = ScratchDir::new(&format!("prop-seed-{seed}"));
            let mut db = Database::create(scratch.path()).unwrap();
            db.create_table("events".into(), events_schema()).unwrap();

            let segment_count = 2 + (seed % 4) as usize; // 2..=5
            let mut expected: Vec<(String, i64, String)> = Vec::new();
            for i in 0..segment_count {
                let n = 20 + ((seed.wrapping_add(i as u64)) % 31) as usize; // 20..=50
                let events = deterministic_event_stream(seed.wrapping_add(i as u64), n);
                for e in &events {
                    expected.push((
                        entity_key_as_str(&e.entity),
                        e.timestamp_nanos(),
                        e.event_type.clone(),
                    ));
                }
                ingest_one_segment(&mut db, "events", 0, 0, &events);
            }

            compact_one(&mut db, "events", 0, 0).expect("compact_one");
            let got = read_all_rows(&db);

            // Invariant 1: the multiset of rows is preserved.
            // Sort both sides by `(entity, ts, event_type)` so
            // duplicate `(entity, ts)` pairs with different event
            // types compare equal as multisets regardless of the
            // k-way merge's stable tie-break (scan-index).
            expected.sort();
            let mut got_sorted = got.clone();
            got_sorted.sort();
            assert_eq!(
                got_sorted, expected,
                "seed {seed}: compaction altered the row multiset"
            );

            // Invariant 2: the emitted order is `(entity, ts)`-sorted
            // (weaker than `(entity, ts, event_type)`-sorted —
            // equal-key rows preserve scan-index order per the merge's
            // tie-break contract, but `(entity, ts)` monotonicity is
            // the documented storage invariant).
            for pair in got.windows(2) {
                let (a, b) = (&pair[0], &pair[1]);
                let key_a = (&a.0, a.1);
                let key_b = (&b.0, b.1);
                assert!(
                    key_a <= key_b,
                    "seed {seed}: rows out of (entity, ts) order: {a:?} then {b:?}"
                );
            }
        }
    }
}
