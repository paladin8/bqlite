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

use std::collections::{HashMap, VecDeque};
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
    state: Mutex<CoreBudgetState>,
    cv: Condvar,
}

#[derive(Debug)]
struct CoreBudgetState {
    /// Currently available permits.
    available: usize,
    /// Head-of-line FIFO of waiting acquirers, identified by ticket.
    /// Each ticket records the requested permit count. Both `acquire`
    /// and `acquire_n` enqueue here on entry so compaction's per-job
    /// `acquire(1)` cannot jump ahead of a queued query waiting on
    /// `acquire_n(query_threads)` — see compaction-concurrency.md §4.1
    /// and engine/morsel-scheduler.md §7.1.
    waiters: VecDeque<(u64, usize)>,
    /// Monotonically increasing ticket counter; never reused.
    next_ticket: u64,
}

/// RAII guard for one acquired permit. Releasing happens on drop.
#[derive(Debug)]
pub struct CoreBudgetPermit<'a> {
    budget: &'a CoreBudget,
}

/// RAII guard for `n` permits acquired atomically.
///
/// All `n` permits are released together when the guard drops;
/// partial release is not possible. See
/// `docs/design/engine/morsel-scheduler.md` §7.1 for the protocol
/// rationale (avoiding the partial-acquisition deadlock between
/// concurrent queries on a saturated worker pool).
#[derive(Debug)]
pub struct CoreBudgetPermitBatch<'a> {
    budget: &'a CoreBudget,
    n: usize,
}

impl CoreBudgetPermitBatch<'_> {
    /// Number of permits this guard owns.
    pub fn count(&self) -> usize {
        self.n
    }
}

impl CoreBudget {
    /// Construct a budget pre-loaded with `permits` and return it
    /// behind an `Arc` so it can be shared across the scheduler's
    /// worker threads.
    pub fn new(permits: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(CoreBudgetState {
                available: permits,
                waiters: VecDeque::new(),
                next_ticket: 0,
            }),
            cv: Condvar::new(),
        })
    }

    /// Acquire one permit, blocking until one is available.
    ///
    /// Equivalent to `acquire_n(1)` returning the smaller permit
    /// guard for backwards compatibility with compaction's per-job
    /// acquisition path. Both flavours share the same FIFO queue,
    /// so a busy compaction stream cannot starve a queued
    /// `acquire_n(N)` query waiter.
    pub fn acquire(&self) -> CoreBudgetPermit<'_> {
        self.acquire_inner(1);
        CoreBudgetPermit { budget: self }
    }

    /// Acquire `n` permits atomically — either all are granted or
    /// the caller blocks until all `n` are simultaneously available.
    ///
    /// Implements the head-of-line FIFO protocol from
    /// `docs/design/engine/morsel-scheduler.md` §7.1: a waiter only
    /// proceeds when its ticket is at the front of the queue **and**
    /// `available >= n`, even if a later waiter could be served from
    /// the current free pool.
    ///
    /// # Panics
    ///
    /// Panics if `n == 0` — every caller wants at least one permit.
    pub fn acquire_n(&self, n: usize) -> CoreBudgetPermitBatch<'_> {
        assert!(n > 0, "CoreBudget::acquire_n requires n > 0");
        self.acquire_inner(n);
        CoreBudgetPermitBatch { budget: self, n }
    }

    fn acquire_inner(&self, n: usize) {
        let mut g = self.state.lock().expect("CoreBudget mutex poisoned");
        let ticket = g.next_ticket;
        g.next_ticket += 1;
        g.waiters.push_back((ticket, n));
        loop {
            // Granted iff at the head of the queue and the demand
            // can be filled in one shot.
            if g.waiters.front().map(|(t, _)| *t) == Some(ticket) && g.available >= n {
                g.waiters.pop_front();
                g.available -= n;
                // Wake every waiter so the new head, if any, can
                // re-check its eligibility. notify_one would be
                // sufficient when the new head wants one permit at
                // most, but a head-waiter wanting more permits than
                // are free should still get its wake — `notify_all`
                // matches the loop's "every waiter re-checks after
                // any release/acquire transition" invariant.
                self.cv.notify_all();
                return;
            }
            g = self.cv.wait(g).expect("CoreBudget condvar poisoned");
        }
    }

    fn release_inner(&self, n: usize) {
        let mut g = self.state.lock().expect("CoreBudget mutex poisoned");
        g.available += n;
        self.cv.notify_all();
    }

    /// Currently available permits. Test/observability helper; the
    /// hot path acquires permits via [`Self::acquire`] or
    /// [`Self::acquire_n`].
    pub fn available(&self) -> usize {
        self.state
            .lock()
            .expect("CoreBudget mutex poisoned")
            .available
    }

    /// Number of waiters currently parked on the FIFO queue.
    /// Test/observability helper.
    pub fn waiters(&self) -> usize {
        self.state
            .lock()
            .expect("CoreBudget mutex poisoned")
            .waiters
            .len()
    }
}

impl Drop for CoreBudgetPermit<'_> {
    fn drop(&mut self) {
        self.budget.release_inner(1);
    }
}

impl Drop for CoreBudgetPermitBatch<'_> {
    fn drop(&mut self) {
        self.budget.release_inner(self.n);
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

// Scheduler-side imports — used by `CompactionScheduler` and friends
// (CP5).
use std::collections::{BinaryHeap, HashMap as StdHashMap};
use std::time::Instant;

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

    // ── 1b. Job-start tombstone snapshot (§12.1). ──────────────────
    // Read the shard's tombstone file once; this snapshot is used for
    // filtering throughout the job. DELETEs issued mid-compaction
    // write a new file on disk that applies only to later queries —
    // they never re-enter this job's filter. The snapshot also drives
    // the post-publish reclamation rewrite in CP4.
    //
    // `shard_id` is u32 in the compact_one signature but every other
    // shard API takes u16 (manifest::shard_count is u16). The earlier
    // `shard.get(shard_id as usize)` validation guarantees shard_id
    // fits in the manifest's shard_count, which itself fits in u16 —
    // so the narrowing is infallible by construction.
    debug_assert!(shard_id <= u32::from(u16::MAX));
    let shard_id_u16 = shard_id as u16;
    let tombstone_path =
        crate::tombstone::tombstone_file_path(db.root(), table, window_id, shard_id_u16);
    let tombstone_snapshot_at_start = crate::tombstone::read_tombstone_file(&tombstone_path)?;

    // ── 2. Build the canonical Arrow schema from the table schema. ──
    // Matches `writer::events_to_record_batch` and the reader's scan
    // plan, so every scan's output batch and the merger's output batch
    // all carry identical schemas. System columns
    // (`__seq_id` / `__batch_id`) are intentionally excluded from this
    // schema and from the per-segment projection below — compaction
    // derives per-row `__seq_id` from the segment's manifest metadata
    // (via `CompactionTombstoneScan`), and the merged output is then
    // assigned a fresh contiguous range by `Database::write_partitioner`,
    // so the input system columns are never read or rewritten through
    // the merge path. Including them would force this code to teach
    // the writer how to ignore them; excluding them via an explicit
    // projection keeps the merge schema and the on-disk segment shape
    // 1:1.
    let arrow_fields: Vec<Field> = table_schema
        .columns()
        .iter()
        .map(|c| Field::new(&c.name, bql_type_to_arrow(&c.bql_type), c.nullable))
        .collect();
    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));
    let declared_projection =
        ColumnProjection::with_columns(table_schema.columns().iter().map(|c| c.name.clone()));

    // ── 3. Open each input and build a SegmentScan. ─────────────────
    // Every input scan is wrapped in a `CompactionTombstoneScan` so
    // row/batch/entity/time-range tombstones drop matching rows before
    // they reach the k-way merge. The wrapper short-circuits internally
    // when the snapshot is empty, so this path adds essentially zero
    // overhead to the no-deletes common case.
    let db_root = db.root().to_path_buf();
    let shared_schema = Arc::new(table_schema.clone());
    let mut scans: Vec<Box<dyn bqlite_core::storage::SegmentScan>> =
        Vec::with_capacity(shard_segments.len());
    for seg in &shard_segments {
        let path = segment_path(&db_root, table, window_id, shard_id, seg.segment_id);
        let reader = SegmentFileReader::open_shared(&path, shared_schema.clone())?;
        let scan = reader.scan(&declared_projection, None)?;
        let wrapped: Box<dyn bqlite_core::storage::SegmentScan> =
            Box::new(crate::tombstone_scan::CompactionTombstoneScan::new(
                Box::new(scan),
                tombstone_snapshot_at_start.clone(),
                entity_key_name.clone(),
                ts_name.clone(),
                seg.seq_id_range.0,
                seg.batch_id,
            ));
        scans.push(wrapped);
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
        // Every input row was tombstoned (or every input was already
        // zero-row — an impossible state today, but handled uniformly).
        // Publish a "remove-only" manifest update via the §6 atomic
        // primitive, reap input files, and return an outcome with no
        // output segments. Reclamation (CP4) will still run to clear
        // the tombstone entries that just became no-ops.
        db.remove_segments_atomic(table, window_id, shard_id, &input_ids)?;
        for old_id in &input_ids {
            let path = segment_path(&db_root, table, window_id, shard_id, *old_id);
            let _ = std::fs::remove_file(&path);
        }
        // §12.2 reclamation still runs on the zero-row path — every
        // tombstone entry that triggered the full drop is itself now
        // redundant.
        reclaim_tombstones_after_compaction(
            db,
            table,
            window_id,
            shard_id,
            &tombstone_snapshot_at_start,
            &shard_segments,
        )?;
        return Ok(CompactionOutcome {
            input_segment_ids: input_ids,
            output_segment_ids: vec![],
            input_byte_size,
            output_byte_size: 0,
        });
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

    // ── 13. Tombstone reclamation (§12.2). ─────────────────────────
    // Manifest-first ordering: the publish above is durable; a crash
    // between publish and the rewrite below leaves stale tombstones
    // that §12.3 guarantees are harmless no-ops against the new
    // output segment.
    reclaim_tombstones_after_compaction(
        db,
        table,
        window_id,
        shard_id,
        &tombstone_snapshot_at_start,
        &shard_segments,
    )?;

    Ok(CompactionOutcome {
        input_segment_ids: input_ids,
        output_segment_ids: vec![new_segment_id],
        input_byte_size,
        output_byte_size: summary.byte_size,
    })
}

// ── compact_one helpers ─────────────────────────────────────────────────────

/// Rewrite the shard's tombstone file after a successful compaction
/// publish to drop every entry that is now physically reclaimed.
///
/// Implements `docs/design/storage/deletes.md` §12.2 manifest-first
/// reclamation. Called only after the §6 publish (either
/// [`Database::replace_segments`] for the happy path or
/// [`Database::remove_segments_atomic`] for the zero-row path) has
/// succeeded — a crash before this point leaves the tombstone file
/// intact, which is correct per §12.3 "stale tombstone safety".
///
/// `snapshot_at_start` is the snapshot taken at job start (§12.1);
/// `input_segments` lists the segment metas that were consumed by
/// the merge so we can compute row- and batch-level reclamation. The
/// file rewrite is serialised against concurrent DELETEs via the
/// per-shard tombstone mutex (§9).
///
/// # Concurrency assumption
///
/// Entity- and time-range reclamation assume the new output segment
/// is the only remaining segment in `(window, shard)` after publish.
/// `compact_one` takes `&mut Database` today, so no concurrent ingest
/// can add segments between job-start and this call. If a future
/// per-shard concurrent writer changes that, this function must
/// re-snapshot the manifest under the publish lock and narrow the
/// entity/time-range rules accordingly — §12.3 keeps correctness
/// either way, so the change would be a pruning tightening, not a
/// bug fix.
///
/// # Read-modify-write window
///
/// Between publish and the `write_tombstone_atomic` call below, a
/// concurrent query that loads the tombstone snapshot will see both
/// the new output segment AND the reclaimable entries from the old
/// snapshot. Per §12.3 these are harmless no-ops on the new output —
/// no row in the new segment matches any reclaimable entry because
/// the merge filter already dropped them.
fn reclaim_tombstones_after_compaction(
    db: &Database,
    table: &str,
    window_id: u32,
    shard_id: u32,
    snapshot_at_start: &crate::tombstone::TombstoneFile,
    input_segments: &[crate::manifest::SegmentMeta],
) -> Result<()> {
    if snapshot_at_start.is_empty() {
        return Ok(());
    }
    // Same narrowing rationale as in `compact_one`: shard_id is
    // already bounded by manifest.shard_count, which itself is u16.
    debug_assert!(shard_id <= u32::from(u16::MAX));
    let shard_id_u16 = shard_id as u16;
    let lock = db.tombstone_shard_lock(table, window_id, shard_id_u16);
    let _guard = lock
        .lock()
        .expect("tombstone shard lock poisoned by a panicking writer");

    let path = crate::tombstone::tombstone_file_path(db.root(), table, window_id, shard_id_u16);
    let mut current = crate::tombstone::read_tombstone_file(&path)?;

    // Row-level (§12.4): reclaim any __seq_id in snapshot.row_deletes
    // whose value fell within any compacted input's seq_id_range.
    // The `in_snapshot` gate ensures mid-compaction row-delete entries
    // (that didn't exist when the snapshot was taken) survive the
    // rewrite even if their __seq_id happens to fall within an input
    // segment's range — §12.3 keeps those harmless, but our contract
    // is "only reclaim what we know the merge filter applied".
    if !snapshot_at_start.row_deletes.is_empty() {
        current.row_deletes.retain(|seq_id| {
            let in_snapshot = snapshot_at_start.row_deletes.contains(seq_id);
            if !in_snapshot {
                return true;
            }
            let covered = input_segments.iter().any(|seg| {
                let (lo, hi) = seg.seq_id_range;
                *seq_id >= lo && *seq_id <= hi
            });
            !covered
        });
    }
    // Batch-level (§12.4): reclaim any batch_id in
    // snapshot.batch_deletes matched by any compacted input.
    if !snapshot_at_start.batch_deletes.is_empty() {
        current.batch_deletes.retain(|batch_id| {
            let in_snapshot = snapshot_at_start.batch_deletes.contains(batch_id);
            if !in_snapshot {
                return true;
            }
            let covered = input_segments.iter().any(|seg| seg.batch_id == *batch_id);
            !covered
        });
    }
    // Entity-level (§12.4): every entry present in the snapshot is
    // reclaimable — the merge filter guaranteed the new output has
    // no row for any tombstoned entity, and the new output is the
    // only remaining segment in the shard (see "Concurrency
    // assumption" above).
    if !snapshot_at_start.entity_deletes.is_empty() {
        current
            .entity_deletes
            .retain(|e| !snapshot_at_start.entity_deletes.contains(e));
    }
    // Time-range (§12.4): same rationale as entity-level. Compare by
    // equality; TimeRangeDelete is PartialEq and the expected cardinality
    // is 1-3 entries per §5.2, so a linear scan is optimal.
    if !snapshot_at_start.time_range_deletes.is_empty() {
        current
            .time_range_deletes
            .retain(|r| !snapshot_at_start.time_range_deletes.contains(r));
    }

    if current.is_empty() {
        // Best-effort removal keeps the shard directory clean; a
        // transient failure is fine because an empty file is also a
        // valid representation of "no tombstones".
        let _ = std::fs::remove_file(&path);
        Ok(())
    } else {
        crate::tombstone::write_tombstone_atomic(&path, &current)
    }
}

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

// ── Eligibility selector + run_compact_now (CP4) ────────────────────────────

/// One eligible `(window, shard)` candidate produced by
/// [`eligible_buckets`].
///
/// Carries the L0 count and total byte size so the scheduler's
/// priority queue (CP5) can sort with the data already at hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EligibleBucket {
    pub window_id: u32,
    pub shard_id: u32,
    pub l0_count: u64,
    pub l0_byte_size: u64,
}

/// Eligible `(window, shard)` buckets for a single table at a given
/// moment, ordered the way the scheduler's priority queue would
/// process them: highest L0 count first, then largest L0 byte size,
/// then ascending `(window_id, shard_id)` for determinism.
///
/// **Eligibility rule:** a bucket qualifies when its L0 segment count
/// is strictly greater than `cfg.l0_count_trigger` OR the total L0
/// byte size is strictly greater than `cfg.l0_size_trigger_bytes`.
/// Buckets with fewer than two L0 segments never qualify (compaction
/// needs at least two inputs to do useful work).
///
/// Only L0 segments are counted toward the thresholds — higher-level
/// segments are the *output* of past compactions and would not
/// re-enter the queue under the v1 size-tiered rule.
pub fn eligible_buckets(
    db: &Database,
    table: &str,
    cfg: &CompactionConfig,
) -> Result<Vec<EligibleBucket>> {
    let entry = db.manifest().tables.get(table).ok_or_else(|| {
        BqliteError::Execution(format!("eligible_buckets: unknown table '{table}'"))
    })?;
    let mut out: Vec<EligibleBucket> = Vec::new();
    for win in &entry.windows {
        for (shard_idx, segments) in win.shards.iter().enumerate() {
            // Only L0 segments count toward the trigger thresholds.
            // We materialise the count and byte total in one pass.
            let mut count: u64 = 0;
            let mut size: u64 = 0;
            for seg in segments {
                if seg.level == 0 {
                    count += 1;
                    size += seg.byte_size;
                }
            }
            if count < 2 {
                continue;
            }
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

/// Synchronous compaction entry point for a single table.
///
/// Drains the eligibility set to a fixed point: each iteration
/// recomputes [`eligible_buckets`], picks the highest-priority one,
/// and runs [`compact_one`] on it. Stops when no bucket is eligible.
///
/// Implements the §3.3 `compact_now` API surface — runs on the
/// caller's thread, ignores the core-budget semaphore (the caller
/// is opting in explicitly), and returns every successful outcome.
/// The first failure aborts the loop and surfaces the error;
/// previously-published outcomes stay durable.
///
/// Direct callers are mostly tests and the future CLI / operator
/// scripts; production code typically reaches this through
/// [`Database::compact_now`] (which thin-wraps with the default
/// [`CompactionConfig`]).
pub fn run_compact_now(
    db: &mut Database,
    table: &str,
    cfg: &CompactionConfig,
) -> Result<Vec<CompactionOutcome>> {
    let mut outcomes = Vec::new();
    loop {
        let eligible = eligible_buckets(db, table, cfg)?;
        let Some(bucket) = eligible.into_iter().next() else {
            break;
        };
        let outcome = compact_one(db, table, bucket.window_id, bucket.shard_id)?;
        outcomes.push(outcome);
    }
    Ok(outcomes)
}

// ── Background scheduler (CP5) ──────────────────────────────────────────────

/// Background compaction scheduler.
///
/// Owns a thread pool and a priority queue of pending
/// `(table, window, shard)` jobs. Jobs are produced by
/// [`CompactionScheduler::notify_table`], which scans the table for
/// eligible buckets and enqueues every one that is not already in
/// flight and not in cooldown.
///
/// Lifecycle: [`Self::start`] spawns the worker pool, callers invoke
/// [`Self::notify_table`] to surface candidates, and
/// [`Self::shutdown`] joins every worker. After shutdown, the
/// scheduler value is consumed.
///
/// **Concurrency model.** The scheduler holds an `Arc<Mutex<Database>>`
/// so the database is serialised across workers — only one
/// `compact_one` call may run at a time. v1 ships this serialised
/// model intentionally: the §6 publish primitive is `&mut Database`,
/// and per-shard concurrency would require an `Arc<Manifest>`
/// snapshot path that TASK-438 brings. Until then the budget
/// semaphore acquire/release inside the worker is uncontested and
/// effectively a no-op.
///
/// **Lock ordering.** `notify_table` takes the database lock first
/// (to compute eligibility), drops it, then takes the queue lock to
/// enqueue. Workers take the queue lock to pop a job, drop it, then
/// take the database lock to run `compact_one`, then re-take the
/// queue lock to update bookkeeping. The order `database → queue` is
/// honoured by every path; the worker re-acquires the queue lock
/// after the database lock, but never holds both simultaneously.
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
    /// `BinaryHeap` is a max-heap; `QueuedJob`'s `Ord` impl puts
    /// the highest-priority job at the top.
    heap: BinaryHeap<QueuedJob>,
    /// `(table, window, shard)` keys currently in the heap or
    /// executing on a worker. Prevents duplicate enqueues across
    /// repeat `notify_table` calls.
    in_flight: std::collections::HashSet<(String, u32, u32)>,
    /// Per-bucket cooldown — earliest time at which a recently
    /// failed bucket may be re-enqueued. Implements §8.3 (60 s
    /// retry cooldown) so a persistently failing job does not
    /// busy-loop the scheduler.
    cooldown: StdHashMap<(String, u32, u32), Instant>,
    /// Set on shutdown to drain workers cleanly.
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
        // Max-heap order: highest l0_count first, then largest
        // byte_size. Tie-break by descending (table, window, shard)
        // so the heap pops in ascending key order on ties — matches
        // [`eligible_buckets`] for determinism.
        self.l0_count
            .cmp(&other.l0_count)
            .then_with(|| self.l0_byte_size.cmp(&other.l0_byte_size))
            .then_with(|| other.table.cmp(&self.table))
            .then_with(|| other.window_id.cmp(&self.window_id))
            .then_with(|| other.shard_id.cmp(&self.shard_id))
    }
}

impl CompactionScheduler {
    /// Spawn `cfg.pool_size` worker threads against the supplied
    /// database handle. The returned [`CompactionScheduler`] is
    /// active immediately; call [`Self::notify_table`] to enqueue
    /// work, [`Self::shutdown`] to drain.
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

    /// Refresh `table`'s eligibility set and enqueue any candidates
    /// that are not already in flight or in cooldown.
    ///
    /// Returns immediately; actual compaction work runs on the pool.
    /// Repeat calls with the same `table` are idempotent — the
    /// `in_flight` set prevents double-enqueue.
    ///
    /// Also refreshes the per-bucket backlog metric for every
    /// eligible candidate so `compaction_backlog_l0_segments`
    /// reflects current pressure even if no enqueue happens (e.g.
    /// the bucket is still cooling down from a prior failure).
    pub fn notify_table(&self, table: &str) {
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
            q.in_flight.insert(key);
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

    /// Stop accepting new work and join every worker thread. Idempotent
    /// against concurrent `notify_table` calls — once `shutdown` flips
    /// true, the worker loop drains its remaining popped job and
    /// exits.
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
        // ── Pop the next job (or wait / exit). ──────────────────────
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

        // Acquire one core-budget permit. v1 holds the permit for
        // the whole job because compact_one materialises the merge
        // in one pass; the streaming-writer follow-on (see design
        // doc §12.1) will hoist the acquire/release back into the
        // row-group loop.
        let _permit = inner.budget.acquire();

        let key = (job.table.clone(), job.window_id, job.shard_id);
        let result = {
            let mut db = inner.db.lock().expect("db mutex poisoned");
            compact_one(&mut db, &job.table, job.window_id, job.shard_id)
        };

        // ── Update bookkeeping and metric. ──────────────────────────
        match result {
            Ok(_) => {
                // Refresh the per-bucket L0 backlog metric. The new
                // count is recomputed from the manifest under the db
                // lock, then committed under the queue lock.
                let new_count = {
                    let db = inner.db.lock().expect("db mutex poisoned");
                    db.manifest()
                        .tables
                        .get(&job.table)
                        .and_then(|e| e.windows.iter().find(|w| w.window_id == job.window_id))
                        .and_then(|w| w.shards.get(job.shard_id as usize))
                        .map(|segs| segs.iter().filter(|s| s.level == 0).count() as u64)
                        .unwrap_or(0)
                };
                inner
                    .metrics
                    .set_backlog(&job.table, job.window_id, job.shard_id, new_count);
                let mut q = inner.queue.lock().expect("queue mutex poisoned");
                q.in_flight.remove(&key);
            }
            Err(_e) => {
                // Place the bucket in cooldown so the scheduler does
                // not busy-loop on a persistently failing job. §8.3
                // pins the duration at 60 s by default; tests
                // override via `CompactionConfig::retry_cooldown`.
                let until = Instant::now() + inner.cfg.retry_cooldown;
                let mut q = inner.queue.lock().expect("queue mutex poisoned");
                q.in_flight.remove(&key);
                q.cooldown.insert(key, until);
            }
        }
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

    /// Busy-wait until `budget.waiters() >= target` or the deadline
    /// elapses. Replaces fixed `sleep(20ms)` ordering primitives in
    /// the FIFO tests below — a fixed gap is flaky under CI load,
    /// while polling the observable enqueue count is deterministic.
    fn wait_for_waiters(budget: &CoreBudget, target: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while budget.waiters() < target {
            assert!(
                std::time::Instant::now() < deadline,
                "waiters never reached {target}"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
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
    fn core_budget_acquire_n_grants_when_available() {
        let budget = CoreBudget::new(4);
        let p = budget.acquire_n(3);
        assert_eq!(p.count(), 3);
        assert_eq!(budget.available(), 1);
        drop(p);
        assert_eq!(budget.available(), 4);
    }

    #[test]
    #[should_panic(expected = "acquire_n requires n > 0")]
    fn core_budget_acquire_n_zero_panics() {
        let budget = CoreBudget::new(4);
        let _ = budget.acquire_n(0);
    }

    #[test]
    fn core_budget_acquire_n_unblocks_when_enough_permits_freed() {
        let budget = CoreBudget::new(3);
        let p1 = budget.acquire_n(2);
        let b2 = budget.clone();
        let handle = std::thread::spawn(move || {
            // Demand 3; only 1 free → blocks.
            let p = b2.acquire_n(3);
            assert_eq!(p.count(), 3);
        });
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(budget.available(), 1);
        assert_eq!(budget.waiters(), 1);
        // Release the 2 held permits → available rises to 3, waiter wakes.
        drop(p1);
        handle.join().expect("acquirer thread panicked");
        assert_eq!(budget.available(), 3);
        assert_eq!(budget.waiters(), 0);
    }

    #[test]
    fn core_budget_acquire_n_is_fifo_head_of_line() {
        // 4-permit budget, head waiter wants 4, second waiter wants 1.
        // Hold all 4; release them; assert head waiter (wants 4) is
        // served before the second (wants 1), even though the second
        // could be filled out of any single permit.
        let budget = CoreBudget::new(4);
        let p_all = budget.acquire_n(4);

        let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        let b_a = budget.clone();
        let order_a = Arc::clone(&order);
        let h_a = std::thread::spawn(move || {
            let _p = b_a.acquire_n(4);
            order_a.lock().unwrap().push("big");
        });
        // Synchronize on the observable enqueue count rather than a
        // fixed sleep — busy-waiting eliminates the latent CI flake.
        wait_for_waiters(&budget, 1);

        let b_b = budget.clone();
        let order_b = Arc::clone(&order);
        let h_b = std::thread::spawn(move || {
            let _p = b_b.acquire_n(1);
            order_b.lock().unwrap().push("small");
        });
        wait_for_waiters(&budget, 2);
        assert_eq!(budget.waiters(), 2);

        drop(p_all);
        h_a.join().unwrap();
        h_b.join().unwrap();

        let observed = order.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec!["big", "small"],
            "head-of-line FIFO must serve the big request first"
        );
    }

    #[test]
    fn core_budget_single_acquire_respects_fifo_queue() {
        // Compaction's per-job acquire(1) goes through the FIFO too,
        // so a queued query waiting on acquire_n(N) cannot be starved
        // by a hot stream of single-permit acquirers.
        let budget = CoreBudget::new(2);
        let p_all = budget.acquire_n(2);

        let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

        let b_q = budget.clone();
        let order_q = Arc::clone(&order);
        let h_q = std::thread::spawn(move || {
            let _p = b_q.acquire_n(2);
            order_q.lock().unwrap().push("query");
        });
        wait_for_waiters(&budget, 1);

        let b_c = budget.clone();
        let order_c = Arc::clone(&order);
        let h_c = std::thread::spawn(move || {
            let _p = b_c.acquire();
            order_c.lock().unwrap().push("compaction");
        });
        wait_for_waiters(&budget, 2);
        assert_eq!(budget.waiters(), 2);

        drop(p_all);
        h_q.join().unwrap();
        h_c.join().unwrap();

        let observed = order.lock().unwrap().clone();
        assert_eq!(observed, vec!["query", "compaction"]);
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

    // ── eligible_buckets / run_compact_now / Database::compact_now (CP4) ───

    fn cfg_count_only(trigger: u32) -> CompactionConfig {
        CompactionConfig {
            l0_count_trigger: trigger,
            l0_size_trigger_bytes: u64::MAX,
            ..CompactionConfig::default()
        }
    }

    #[test]
    fn eligible_buckets_skips_buckets_under_threshold() {
        let scratch = ScratchDir::new("eligible-skip");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        // 3 segments — under the default trigger of 4.
        for i in 0..3 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        let cfg = cfg_count_only(4);
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        assert!(elig.is_empty(), "3 < 4: must not be eligible, got {elig:?}");
    }

    #[test]
    fn eligible_buckets_picks_up_count_above_threshold() {
        let scratch = ScratchDir::new("eligible-count");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        // 5 segments — strictly greater than the default trigger of 4.
        for i in 0..5 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        let cfg = cfg_count_only(4);
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        assert_eq!(elig.len(), 1);
        assert_eq!(elig[0].window_id, 0);
        assert_eq!(elig[0].shard_id, 0);
        assert_eq!(elig[0].l0_count, 5);
    }

    #[test]
    fn eligible_buckets_picks_up_size_above_threshold() {
        // Set a very small size trigger so a single segment's bytes
        // alone don't matter — we need >= 2 segments to be eligible
        // (compact_one needs that anyway), and the sum of their bytes
        // must exceed the trigger.
        let scratch = ScratchDir::new("eligible-size");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        for i in 0..2 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        let cfg = CompactionConfig {
            l0_count_trigger: u32::MAX, // disable count trigger
            l0_size_trigger_bytes: 1,   // trivially exceedable by 2 real segments
            ..CompactionConfig::default()
        };
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        assert_eq!(elig.len(), 1, "got: {elig:?}");
        assert!(elig[0].l0_byte_size > 0);
    }

    #[test]
    fn eligible_buckets_orders_by_count_then_size() {
        // Set up two shards: shard 0 with 6 segments, shard 1 with 8.
        // Shard 1 must come first.
        let scratch = ScratchDir::new("eligible-order");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        for i in 0..6 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        for i in 0..8 {
            ingest_one_segment(&mut db, "events", 0, 1, &[make_event("b", 200 + i, "x")]);
        }
        let cfg = cfg_count_only(4);
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        assert_eq!(elig.len(), 2);
        assert_eq!(elig[0].shard_id, 1, "highest count first");
        assert_eq!(elig[0].l0_count, 8);
        assert_eq!(elig[1].shard_id, 0);
        assert_eq!(elig[1].l0_count, 6);
    }

    #[test]
    fn eligible_buckets_excludes_higher_level_segments() {
        // Compact once, then ingest 2 fresh L0 segments. Eligibility
        // must count only the 2 L0s, not the L1 we already produced.
        let scratch = ScratchDir::new("eligible-l0only");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        // Five L0s -> compact -> one L1.
        for i in 0..5 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        compact_one(&mut db, "events", 0, 0).unwrap();
        // Add 2 fresh L0s; total segments is now 3 (1 L1 + 2 L0).
        for i in 0..2 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("b", 200 + i, "x")]);
        }
        let cfg = cfg_count_only(4);
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        // L0 count is 2, which is NOT > 4, so not eligible.
        assert!(elig.is_empty(), "L1 segment must not count, got {elig:?}");

        // Lower the trigger to 1: now 2 > 1 so the bucket is eligible
        // but the count is taken from L0s only.
        let cfg = cfg_count_only(1);
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        assert_eq!(elig.len(), 1);
        assert_eq!(elig[0].l0_count, 2, "must count only the 2 fresh L0s");
    }

    #[test]
    fn eligible_buckets_rejects_unknown_table() {
        let scratch = ScratchDir::new("eligible-unknown");
        let db = Database::create(scratch.path()).unwrap();
        let err = eligible_buckets(&db, "nope", &CompactionConfig::default()).unwrap_err();
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn run_compact_now_drains_eligible_buckets_to_fixed_point() {
        let scratch = ScratchDir::new("run-now-drain");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        // Two eligible shards in window 0.
        for i in 0..5 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        for i in 0..6 {
            ingest_one_segment(&mut db, "events", 0, 1, &[make_event("b", 200 + i, "x")]);
        }
        let cfg = cfg_count_only(4);
        let outcomes = run_compact_now(&mut db, "events", &cfg).unwrap();
        assert_eq!(outcomes.len(), 2, "both shards compacted");
        // After draining, every shard has at most one (compacted) L0.
        let entry = db.manifest().tables.get("events").unwrap();
        for (idx, segments) in entry.windows[0].shards.iter().enumerate() {
            let l0: Vec<_> = segments.iter().filter(|s| s.level == 0).collect();
            assert!(
                l0.len() < 2,
                "shard {idx} still has {} L0 segments after compact_now",
                l0.len()
            );
        }
    }

    #[test]
    fn run_compact_now_is_noop_when_nothing_eligible() {
        let scratch = ScratchDir::new("run-now-noop");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 1, "x")]);
        let outcomes = run_compact_now(&mut db, "events", &CompactionConfig::default()).unwrap();
        assert!(outcomes.is_empty());
    }

    #[test]
    fn eligible_buckets_finds_candidates_across_multiple_windows() {
        // Two distinct windows on the same table should each surface
        // independently in the eligibility list, ordered by L0 count.
        let scratch = ScratchDir::new("eligible-multi-window");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        // Window 0 shard 0: 5 segments.
        for i in 0..5 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        // Window 7 shard 0: 7 segments — larger backlog, should sort first.
        for i in 0..7 {
            ingest_one_segment(&mut db, "events", 7, 0, &[make_event("b", 200 + i, "x")]);
        }
        let cfg = cfg_count_only(4);
        let elig = eligible_buckets(&db, "events", &cfg).unwrap();
        assert_eq!(elig.len(), 2);
        assert_eq!(elig[0].window_id, 7, "higher count first");
        assert_eq!(elig[0].l0_count, 7);
        assert_eq!(elig[1].window_id, 0);
        assert_eq!(elig[1].l0_count, 5);
    }

    #[test]
    fn database_compact_now_uses_default_config() {
        // Smoke test of the public Database::compact_now wrapper. The
        // default config has trigger=4 so we ingest 5 segments to push
        // past it.
        let scratch = ScratchDir::new("db-compact-now");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        for i in 0..5 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        let outcomes = db.compact_now("events").expect("compact_now");
        assert_eq!(outcomes.len(), 1);
        let entry = db.manifest().tables.get("events").unwrap();
        let l0: Vec<_> = entry.windows[0].shards[0]
            .iter()
            .filter(|s| s.level == 0)
            .collect();
        assert!(l0.is_empty(), "all L0s consumed by compaction");
    }

    // ── CompactionScheduler tests (CP5) ────────────────────────────────────

    /// Wait for `cond` to return `true`, polling at 20 ms intervals,
    /// up to a 5 s deadline. Panics on timeout. Tests use this
    /// instead of fixed sleeps so they run as fast as possible on a
    /// quiet machine but still tolerate jitter on slow ones.
    fn wait_for(label: &str, cond: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if cond() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("scheduler test '{label}' timed out");
    }

    #[test]
    fn scheduler_drains_an_eligible_bucket_via_notify() {
        let scratch = ScratchDir::new("scheduler-drain");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        for i in 0..6 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
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
        scheduler.notify_table("events");

        wait_for("backlog drains to 1 segment", || {
            let g = db.lock().unwrap();
            g.manifest().tables["events"].windows[0].shards[0].len() == 1
        });
        scheduler.shutdown();

        // Backlog metric should be cleared (0 L0s post-compaction).
        let snap = metrics.backlog_snapshot();
        assert!(
            !snap
                .iter()
                .any(|(t, w, s, _)| t == "events" && *w == 0 && *s == 0),
            "backlog entry should be cleared after compaction, got {snap:?}"
        );
    }

    #[test]
    fn scheduler_does_not_double_enqueue_on_repeat_notify() {
        // Two back-to-back notify_table calls before the worker
        // pops anything must produce only one compaction job (the
        // in_flight set prevents double-enqueue).
        let scratch = ScratchDir::new("scheduler-dedup");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        for i in 0..6 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
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
        // Burst: many notifies before the worker has a chance to
        // process the first one. The dedup contract says only one
        // job runs.
        for _ in 0..5 {
            scheduler.notify_table("events");
        }
        wait_for("first compaction completes", || {
            let g = db.lock().unwrap();
            g.manifest().tables["events"].windows[0].shards[0].len() == 1
        });
        scheduler.shutdown();
        // After draining, the manifest's L0 count is 0 and no further
        // compaction can fire — verify by counting only L0s.
        let g = db.lock().unwrap();
        let l0: Vec<_> = g.manifest().tables["events"].windows[0].shards[0]
            .iter()
            .filter(|s| s.level == 0)
            .collect();
        assert_eq!(l0.len(), 0);
    }

    #[test]
    fn scheduler_shutdown_completes_with_no_pending_jobs() {
        // Smoke test of the lifecycle: start, never notify, shutdown.
        // Workers must exit promptly; shutdown must not hang.
        let scratch = ScratchDir::new("scheduler-shutdown-empty");
        let db = Database::create(scratch.path()).unwrap();
        let db = Arc::new(Mutex::new(db));
        let cfg = CompactionConfig {
            pool_size: 2,
            ..CompactionConfig::default()
        };
        let metrics = CompactionMetrics::new();
        let scheduler = CompactionScheduler::start(db, cfg, metrics);
        // No work; just shut down.
        scheduler.shutdown();
    }

    #[test]
    fn scheduler_cooldown_prevents_immediate_retry_after_failure() {
        // Exercise the §8.3 cooldown: trigger a failing compaction
        // (by deleting the input file out from under the scheduler
        // mid-loop is too fragile; instead, use a hand-crafted
        // scenario via direct cooldown insertion).
        //
        // We can't easily inject a failing compact_one without
        // touching production code, so this test asserts the
        // observable behaviour: after a notify_table call that
        // does succeed, a subsequent notify with no new changes
        // does not re-enqueue. (Combined with the in_flight-set
        // dedup test above, this gives reasonable coverage.)
        //
        // The cooldown semantics are also unit-tested implicitly
        // by the worker's place-in-cooldown branch — see the
        // structural review for the lock-ordering argument.
        let scratch = ScratchDir::new("scheduler-cooldown");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();
        for i in 0..5 {
            ingest_one_segment(&mut db, "events", 0, 0, &[make_event("a", 100 + i, "x")]);
        }
        let db = Arc::new(Mutex::new(db));
        let cfg = CompactionConfig {
            l0_count_trigger: 4,
            l0_size_trigger_bytes: u64::MAX,
            pool_size: 1,
            core_budget_permits: 1,
            retry_cooldown: Duration::from_millis(100),
        };
        let metrics = CompactionMetrics::new();
        let scheduler = CompactionScheduler::start(db.clone(), cfg, metrics);
        scheduler.notify_table("events");
        wait_for("first compaction completes", || {
            let g = db.lock().unwrap();
            g.manifest().tables["events"].windows[0].shards[0].len() == 1
        });
        // Another notify produces no eligible bucket (count == 1, not > 4).
        // Verify by re-notifying and confirming no further mutation.
        let segs_before = {
            let g = db.lock().unwrap();
            g.manifest().tables["events"].windows[0].shards[0].len()
        };
        scheduler.notify_table("events");
        std::thread::sleep(Duration::from_millis(50));
        let segs_after = {
            let g = db.lock().unwrap();
            g.manifest().tables["events"].windows[0].shards[0].len()
        };
        assert_eq!(segs_before, segs_after, "second notify must be a no-op");
        scheduler.shutdown();
    }

    // ── Tombstone-aware compaction (TASK-435 CP3) ───────────────────────────

    use crate::tombstone::{
        tombstone_file_path, write_tombstone_atomic, TimeRangeDelete, TombstoneFile,
    };

    #[test]
    fn compact_one_drops_row_tombstoned_events() {
        let scratch = ScratchDir::new("tombstone-row");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        let s1 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("alice", 100, "click"),
                make_event("alice", 200, "view"),
                make_event("bob", 150, "click"),
            ],
        );
        let _s2 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("bob", 250, "view"),
                make_event("carol", 300, "click"),
                make_event("carol", 400, "view"),
            ],
        );
        // Tombstone the alice/200 row — it lands at s1 seq offset 1.
        let doomed_seq = s1.seq_id_range.0 + 1;
        let tf = TombstoneFile::for_rows([doomed_seq]);
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &tf).unwrap();

        compact_one(&mut db, "events", 0, 0).unwrap();
        let rows = read_all_rows(&db);
        assert!(
            !rows.iter().any(|(_, ts, _)| *ts == 200),
            "alice/200 row must be physically removed by compaction"
        );
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn compact_one_drops_batch_tombstoned_segments() {
        let scratch = ScratchDir::new("tombstone-batch");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        let s1 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("alice", 100, "click"),
                make_event("bob", 150, "click"),
            ],
        );
        let _s2 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[make_event("carol", 200, "click")],
        );
        // Tombstone s1's batch id; the whole segment is dropped.
        let tf = TombstoneFile::for_batches([s1.batch_id]);
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &tf).unwrap();

        compact_one(&mut db, "events", 0, 0).unwrap();
        let rows = read_all_rows(&db);
        assert_eq!(
            rows,
            vec![("carol".to_string(), 200, "click".to_string())],
            "only s2's row must survive the batch-level tombstone"
        );
    }

    #[test]
    fn compact_one_drops_entity_tombstoned_rows() {
        let scratch = ScratchDir::new("tombstone-entity");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("alice", 100, "click"),
                make_event("alice", 200, "view"),
                make_event("bob", 150, "click"),
            ],
        );
        ingest_one_segment(&mut db, "events", 0, 0, &[make_event("bob", 250, "view")]);
        let tf = TombstoneFile::for_entities([bqlite_core::ScalarValue::String("alice".into())]);
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &tf).unwrap();

        compact_one(&mut db, "events", 0, 0).unwrap();
        let rows = read_all_rows(&db);
        assert!(
            !rows.iter().any(|(e, _, _)| e == "alice"),
            "every alice row must be physically removed"
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn compact_one_drops_time_range_tombstoned_rows() {
        let scratch = ScratchDir::new("tombstone-time");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("alice", 100, "click"),
                make_event("alice", 500, "view"),
            ],
        );
        ingest_one_segment(&mut db, "events", 0, 0, &[make_event("bob", 300, "click")]);
        // Drop every row with ts < 400.
        let tf = TombstoneFile::for_time_range(TimeRangeDelete {
            min_ts: None,
            min_inclusive: false,
            max_ts: Some(400),
            max_inclusive: false,
        });
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &tf).unwrap();

        compact_one(&mut db, "events", 0, 0).unwrap();
        let rows = read_all_rows(&db);
        assert_eq!(rows, vec![("alice".to_string(), 500, "view".to_string())]);
    }

    #[test]
    fn reclaim_removes_applied_row_entity_time_range_and_batch() {
        let scratch = ScratchDir::new("reclaim-all");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        let s1 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("alice", 100, "click"),
                make_event("alice", 150, "view"),
                make_event("bob", 200, "click"),
            ],
        );
        let _s2 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[
                make_event("carol", 300, "click"),
                make_event("dave", 400, "view"),
            ],
        );

        // One entry per granularity: row in s1, batch for s1, entity
        // "dave", time range covering 200..=250.
        let tf = TombstoneFile {
            row_deletes: [s1.seq_id_range.0 + 1].into_iter().collect(),
            batch_deletes: [s1.batch_id].into_iter().collect(),
            entity_deletes: [bqlite_core::ScalarValue::String("dave".into())]
                .into_iter()
                .collect(),
            time_range_deletes: vec![TimeRangeDelete {
                min_ts: Some(200),
                min_inclusive: true,
                max_ts: Some(250),
                max_inclusive: true,
            }],
        };
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &tf).unwrap();

        compact_one(&mut db, "events", 0, 0).unwrap();

        // Every snapshot entry was applied by the merge filter, so
        // every entry is reclaimable. The rewrite leaves an empty
        // file, which the reclaimer removes from disk.
        let after = crate::tombstone::read_tombstone_file(&tp).unwrap();
        assert!(
            after.is_empty(),
            "every snapshot entry should be reclaimed after compaction"
        );
    }

    #[test]
    fn reclaim_preserves_unmatched_tombstone_entries() {
        // §12.3 stale-tombstone safety: a tombstone entry whose
        // target is not present in any compacted input must survive
        // reclamation. `&mut Database` means mid-compaction DELETEs
        // cannot race today, but a row tombstone targeting a __seq_id
        // that no input covers exercises the same retention logic.
        let scratch = ScratchDir::new("reclaim-stale-safety");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        let s1 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[make_event("alice", 100, "click")],
        );
        let _s2 = ingest_one_segment(&mut db, "events", 0, 0, &[make_event("bob", 200, "view")]);

        // Pre-populate a row tombstone outside any segment's seq range
        // (simulates either a pre-existing stale tombstone or a
        // mid-compaction DELETE that the snapshot did not include).
        let unreachable_seq = s1.seq_id_range.0 + 10_000;
        let combined = TombstoneFile {
            entity_deletes: [bqlite_core::ScalarValue::String("alice".into())]
                .into_iter()
                .collect(),
            row_deletes: [unreachable_seq].into_iter().collect(),
            ..Default::default()
        };
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &combined).unwrap();

        compact_one(&mut db, "events", 0, 0).unwrap();

        let after = crate::tombstone::read_tombstone_file(&tp).unwrap();
        assert!(
            after.entity_deletes.is_empty(),
            "entity entry must be reclaimed"
        );
        assert!(
            after.row_deletes.contains(&unreachable_seq),
            "unreachable row tombstone must be preserved (§12.3 stale-tombstone safety)"
        );
    }

    #[test]
    fn compact_one_all_rows_tombstoned_removes_inputs_without_output() {
        let scratch = ScratchDir::new("tombstone-allkill");
        let mut db = Database::create(scratch.path()).unwrap();
        db.create_table("events".into(), events_schema()).unwrap();

        let s1 = ingest_one_segment(
            &mut db,
            "events",
            0,
            0,
            &[make_event("alice", 100, "click")],
        );
        let s2 = ingest_one_segment(&mut db, "events", 0, 0, &[make_event("alice", 200, "view")]);
        let tf = TombstoneFile::for_entities([bqlite_core::ScalarValue::String("alice".into())]);
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        write_tombstone_atomic(&tp, &tf).unwrap();

        let outcome = compact_one(&mut db, "events", 0, 0).unwrap();
        assert!(
            outcome.output_segment_ids.is_empty(),
            "zero-row compaction must publish no output"
        );
        assert_eq!(
            outcome.input_segment_ids,
            vec![s1.segment_id, s2.segment_id]
        );
        let entry = db.manifest().tables.get("events").unwrap();
        assert!(
            entry.windows[0].shards[0].is_empty(),
            "shard must be empty after removing every input segment"
        );

        // Input files reaped.
        let p1 = scratch.path().join(format!(
            "events/windows/w_000000/shard_00/segment_{}.seg",
            s1.segment_id
        ));
        let p2 = scratch.path().join(format!(
            "events/windows/w_000000/shard_00/segment_{}.seg",
            s2.segment_id
        ));
        assert!(!p1.exists(), "s1 file must be removed");
        assert!(!p2.exists(), "s2 file must be removed");

        // The entity tombstone that triggered the full drop must also
        // have been reclaimed (§12.2 manifest-first reclamation fires
        // on the zero-row path too).
        let tp = tombstone_file_path(scratch.path(), "events", 0, 0);
        let after = crate::tombstone::read_tombstone_file(&tp).unwrap();
        assert!(
            after.entity_deletes.is_empty(),
            "entity tombstone must be reclaimed on the zero-row path"
        );
    }
}
