//! Tombstone file storage and snapshot loading.
//!
//! Implements the `TombstoneFile` schema from `docs/design/storage/deletes.md`
//! SS5: four tombstone granularities (row, batch, entity, time-range), atomic
//! read/write via write + rename, per-query snapshot loading, and typed
//! helpers for building tombstone entries.
//!
//! # Tombstone Granularities
//!
//! | Granularity | Field | Use case |
//! |-------------|-------|----------|
//! | Row-level | `row_deletes` (`__seq_id`) | Delete specific events |
//! | Batch-level | `batch_deletes` (`__batch_id`) | Undo a bad ingest |
//! | Entity-level | `entity_deletes` (entity key) | GDPR right-to-erasure |
//! | Time-range | `time_range_deletes` | Retention cutoff |
//!
//! # Atomic I/O
//!
//! Tombstone files are updated atomically via the same write + rename
//! pattern used for manifest updates (see `database.rs`):
//! write to `tombstones.json.tmp`, fsync, rename over `tombstones.json`,
//! best-effort fsync parent directory.
//!
//! # Per-Query Snapshots
//!
//! [`TombstoneSnapshot`] captures a frozen view of all tombstones for
//! the `(window, shard)` pairs a query will touch, loaded once at query
//! bind time. See design doc SS6.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use arrow::array::{Array, Int64Array, StringViewArray, TimestampNanosecondArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;

use bqlite_core::error::{BqliteError, Result};
use bqlite_core::ScalarValue;
use serde::{Deserialize, Serialize};

// ──────────────────────────────────────────────────────────────────────────────
// Constants
// ──────────────────────────────────────────────────────────────────────────────

/// Name of the tombstone file within each shard directory.
pub const TOMBSTONE_FILE_NAME: &str = "tombstones.json";

/// Name of the transient file written during an atomic tombstone update.
pub const TOMBSTONE_TMP_FILE_NAME: &str = "tombstones.json.tmp";

// ──────────────────────────────────────────────────────────────────────────────
// TombstoneFile
// ──────────────────────────────────────────────────────────────────────────────

/// Active tombstones for a single `(window, shard)`.
///
/// Serialized as JSON. Updated atomically via write + rename
/// (same pattern as manifest updates). See design doc SS5.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TombstoneFile {
    /// Entity-level deletes: all events for these entities are deleted.
    pub entity_deletes: HashSet<ScalarValue>,

    /// Row-level deletes: specific sequence IDs.
    pub row_deletes: HashSet<u64>,

    /// Batch-level deletes: specific batch IDs.
    pub batch_deletes: HashSet<u64>,

    /// Time-range deletes. A row is tombstoned if its timestamp falls
    /// within **any** range in this Vec. Multiple ranges support
    /// independent time-range DELETE operations. See design doc SS5.2.
    pub time_range_deletes: Vec<TimeRangeDelete>,
}

/// A time range for time-range deletes.
///
/// Bounds are in nanoseconds since epoch. `None` = unbounded on that
/// side. See design doc SS5.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeRangeDelete {
    /// Lower bound (nanoseconds since epoch). `None` = unbounded below.
    pub min_ts: Option<i64>,
    /// Whether the lower bound is inclusive.
    pub min_inclusive: bool,

    /// Upper bound (nanoseconds since epoch). `None` = unbounded above.
    pub max_ts: Option<i64>,
    /// Whether the upper bound is inclusive.
    pub max_inclusive: bool,
}

impl TombstoneFile {
    /// Returns `true` if there are no tombstones of any kind.
    pub fn is_empty(&self) -> bool {
        self.entity_deletes.is_empty()
            && self.row_deletes.is_empty()
            && self.batch_deletes.is_empty()
            && self.time_range_deletes.is_empty()
    }

    /// Merge entries from `other` into `self`.
    ///
    /// Set-based entries (row, batch, entity) use union semantics.
    /// Time-range entries are appended with exact-match deduplication
    /// per design doc SS5.1 — an O(n) scan over the expected-small
    /// Vec (1--3 entries).
    pub fn merge(&mut self, other: &TombstoneFile) {
        self.row_deletes.extend(&other.row_deletes);
        self.batch_deletes.extend(&other.batch_deletes);
        self.entity_deletes
            .extend(other.entity_deletes.iter().cloned());
        for range in &other.time_range_deletes {
            if !self.time_range_deletes.contains(range) {
                self.time_range_deletes.push(range.clone());
            }
        }
    }

    /// Build a tombstone file containing only row-level deletes.
    pub fn for_rows(seq_ids: impl IntoIterator<Item = u64>) -> Self {
        Self {
            row_deletes: seq_ids.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Build a tombstone file containing only batch-level deletes.
    pub fn for_batches(batch_ids: impl IntoIterator<Item = u64>) -> Self {
        Self {
            batch_deletes: batch_ids.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Build a tombstone file containing only entity-level deletes.
    pub fn for_entities(entity_keys: impl IntoIterator<Item = ScalarValue>) -> Self {
        Self {
            entity_deletes: entity_keys.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Build a tombstone file containing a single time-range delete.
    pub fn for_time_range(range: TimeRangeDelete) -> Self {
        Self {
            time_range_deletes: vec![range],
            ..Default::default()
        }
    }

    /// Materialize the entity-delete set as type-specialized hash indices.
    ///
    /// Computed once per scan and reused across batches by the scan
    /// wrappers via [`TombstoneFilter::filter_batch_with_index`] /
    /// `TombstoneFilter::apply_entity_deletes_with_index`; the inline
    /// path through [`TombstoneFilter::filter_batch`] would otherwise
    /// rebuild it on every batch (see deletes.md §7 per-batch hot path).
    pub fn entity_delete_index(&self) -> EntityDeleteIndex {
        EntityDeleteIndex::from_tombstones(self)
    }
}

/// Precomputed lookup index for entity-level tombstones.
///
/// Splits `TombstoneFile::entity_deletes` (a `HashSet<ScalarValue>`)
/// into type-specialized sets so the filter loop can probe directly
/// on `&str` / `i64` without per-row allocation. Build once per
/// scan; pass by reference to
/// `TombstoneFilter::filter_batch_with_index` for every batch.
#[derive(Debug, Clone, Default)]
pub struct EntityDeleteIndex {
    str_set: HashSet<String>,
    int_set: HashSet<i64>,
}

impl EntityDeleteIndex {
    /// Build an index from a tombstone file's entity-delete set.
    pub fn from_tombstones(tombstones: &TombstoneFile) -> Self {
        let mut str_set = HashSet::new();
        let mut int_set = HashSet::new();
        for v in &tombstones.entity_deletes {
            match v {
                ScalarValue::String(s) => {
                    str_set.insert(s.clone());
                }
                ScalarValue::Int(n) => {
                    int_set.insert(*n);
                }
                // Other ScalarValue variants are not valid entity keys
                // (entity keys are String or Int per type-system.md §2.1)
                // — ignore defensively.
                _ => {}
            }
        }
        Self { str_set, int_set }
    }

    /// Returns `true` when neither set has entries.
    pub fn is_empty(&self) -> bool {
        self.str_set.is_empty() && self.int_set.is_empty()
    }

    /// Probe the string-key entity set. Used by encoded-path
    /// tombstone filtering (`encoded_tombstone.rs`) which decodes the
    /// entity-key column from a pinned chunk and probes per row.
    pub fn contains_str(&self, key: &str) -> bool {
        self.str_set.contains(key)
    }

    /// Probe the integer-key entity set. See [`Self::contains_str`].
    pub fn contains_int(&self, key: i64) -> bool {
        self.int_set.contains(&key)
    }
}

impl TimeRangeDelete {
    /// Returns `true` if `ts` (nanoseconds since epoch) falls within
    /// this range. Used by scan-time tombstone filtering (design doc SS7).
    pub fn contains_timestamp(&self, ts: i64) -> bool {
        let above_min = match self.min_ts {
            None => true,
            Some(min) if self.min_inclusive => ts >= min,
            Some(min) => ts > min,
        };
        let below_max = match self.max_ts {
            None => true,
            Some(max) if self.max_inclusive => ts <= max,
            Some(max) => ts < max,
        };
        above_min && below_max
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// I/O helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Resolve the on-disk path for a shard's tombstone file.
///
/// Layout matches the segment directory structure:
/// `<db_root>/<table>/windows/w_<window_id>/shard_<shard_id>/tombstones.json`
pub fn tombstone_file_path(db_root: &Path, table: &str, window_id: u32, shard_id: u16) -> PathBuf {
    db_root
        .join(table)
        .join("windows")
        .join(format!("w_{window_id:06}"))
        .join(format!("shard_{shard_id:02}"))
        .join(TOMBSTONE_FILE_NAME)
}

/// Read the tombstone file at `path`.
///
/// Returns an empty [`TombstoneFile`] if the file does not exist (no
/// tombstones for that shard yet).
pub fn read_tombstone_file(path: &Path) -> Result<TombstoneFile> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
            BqliteError::Execution(format!("corrupt tombstone file {}: {e}", path.display()))
        }),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(TombstoneFile::default()),
        Err(e) => Err(BqliteError::Io(io::Error::new(
            e.kind(),
            format!("read tombstone file {}: {e}", path.display()),
        ))),
    }
}

/// Write `tombstone` to `path` atomically via write + rename.
///
/// Follows the same pattern as manifest writes (design doc SS9.1):
///
/// 1. Ensure the parent (shard) directory exists.
/// 2. Write to `tombstones.json.tmp`; fsync.
/// 3. Rename over `tombstones.json`.
/// 4. Best-effort fsync parent directory.
pub fn write_tombstone_atomic(path: &Path, tombstone: &TombstoneFile) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| BqliteError::Execution("tombstone path has no parent directory".into()))?;

    // Ensure shard directory exists — it may not if no segments have
    // been written to this shard yet.
    fs::create_dir_all(parent).map_err(|e| {
        BqliteError::Io(io::Error::new(
            e.kind(),
            format!("create shard directory {}: {e}", parent.display()),
        ))
    })?;

    let tmp_path = parent.join(TOMBSTONE_TMP_FILE_NAME);

    let body = serde_json::to_vec_pretty(tombstone)
        .map_err(|e| BqliteError::Execution(format!("failed to serialize tombstone file: {e}")))?;

    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| {
                BqliteError::Io(io::Error::new(
                    e.kind(),
                    format!("create tombstone tmp {}: {e}", tmp_path.display()),
                ))
            })?;
        f.write_all(&body).map_err(|e| {
            BqliteError::Io(io::Error::new(
                e.kind(),
                format!("write tombstone tmp {}: {e}", tmp_path.display()),
            ))
        })?;
        f.sync_all().map_err(|e| {
            BqliteError::Io(io::Error::new(
                e.kind(),
                format!("fsync tombstone tmp {}: {e}", tmp_path.display()),
            ))
        })?;
    }

    fs::rename(&tmp_path, path).map_err(|e| {
        BqliteError::Io(io::Error::new(
            e.kind(),
            format!("rename tombstone tmp to {}: {e}", path.display()),
        ))
    })?;

    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }

    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────────
// TombstoneSnapshot
// ──────────────────────────────────────────────────────────────────────────────

/// A frozen snapshot of all tombstones for the `(window, shard)` pairs
/// a query will touch.
///
/// Loaded once at query bind time and shared (via `Arc`) across all
/// scan operators in that query. See design doc SS6.
#[derive(Debug, Clone, Default)]
pub struct TombstoneSnapshot {
    /// Key: `(window_id, shard_id)`. Only non-empty tombstone files are
    /// stored; absent keys mean no tombstones for that shard.
    shards: HashMap<(u32, u16), TombstoneFile>,
}

impl TombstoneSnapshot {
    /// Build an empty snapshot (no tombstones anywhere).
    pub fn empty() -> Self {
        Self {
            shards: HashMap::new(),
        }
    }

    /// Build a snapshot directly from `(window, shard) → TombstoneFile`
    /// entries. Empty files are dropped so [`TombstoneSnapshot::get`] on
    /// a shard with only empty tombstones returns `None`, matching the
    /// shape produced by [`load_tombstone_snapshot`].
    ///
    /// Intended for engine-side composition (e.g. merging snapshots
    /// across tables for a joined-source query) and tests that want to
    /// exercise the scan-time filter without writing tombstone files to
    /// disk.
    pub fn from_map<I>(entries: I) -> Self
    where
        I: IntoIterator<Item = ((u32, u16), TombstoneFile)>,
    {
        let mut shards: HashMap<(u32, u16), TombstoneFile> = HashMap::new();
        for (key, tf) in entries {
            if !tf.is_empty() {
                shards.insert(key, tf);
            }
        }
        Self { shards }
    }

    /// Retrieve the tombstone state for a specific `(window, shard)`.
    ///
    /// Returns `None` if there are no tombstones for that shard.
    pub fn get(&self, window_id: u32, shard_id: u16) -> Option<&TombstoneFile> {
        self.shards.get(&(window_id, shard_id))
    }

    /// Returns `true` if the snapshot contains no tombstones at all.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    /// Number of `(window, shard)` entries in the snapshot. Each entry
    /// holds a non-empty [`TombstoneFile`] — shards with no tombstones
    /// are not represented.
    pub fn len(&self) -> usize {
        self.shards.len()
    }
}

/// Load tombstone files for the given `(window_id, shard_id)` pairs
/// into a frozen snapshot.
///
/// Called once at query bind time (design doc SS6). Each file is read
/// independently; missing files (no tombstones for that shard) produce
/// no entry in the snapshot.
pub fn load_tombstone_snapshot(
    db_root: &Path,
    table: &str,
    targets: &[(u32, u16)],
) -> Result<TombstoneSnapshot> {
    let mut shards = HashMap::with_capacity(targets.len());
    for &(window_id, shard_id) in targets {
        let path = tombstone_file_path(db_root, table, window_id, shard_id);
        let tf = read_tombstone_file(&path)?;
        if !tf.is_empty() {
            shards.insert((window_id, shard_id), tf);
        }
    }
    Ok(TombstoneSnapshot { shards })
}

// ──────────────────────────────────────────────────────────────────────────────
// TombstoneFilter
// ──────────────────────────────────────────────────────────────────────────────

/// Applies tombstone checks to a [`RecordBatch`], removing rows that
/// match any entry in the associated [`TombstoneFile`].
///
/// Filter order follows `deletes.md` SS7.1:
/// batch → entity → row → time-range. A row is suppressed if **any**
/// check matches. Short-circuits when the tombstone file is empty.
pub struct TombstoneFilter<'a> {
    tombstones: &'a TombstoneFile,
    entity_key_col: &'a str,
    ts_col: &'a str,
}

impl<'a> TombstoneFilter<'a> {
    /// Build a filter for the given tombstone state.
    ///
    /// `entity_key_col` and `ts_col` are the column names in the
    /// `RecordBatch` to use for entity-level and time-range checks.
    pub fn new(tombstones: &'a TombstoneFile, entity_key_col: &'a str, ts_col: &'a str) -> Self {
        Self {
            tombstones,
            entity_key_col,
            ts_col,
        }
    }

    /// Filter `batch`, returning a new batch with tombstoned rows removed.
    ///
    /// Returns the batch unchanged when tombstones are empty (zero cost).
    ///
    /// Builds an entity-delete index inline. Scan wrappers that filter
    /// many batches against the same tombstone file should hold an
    /// [`EntityDeleteIndex`] across calls and use
    /// [`Self::filter_batch_with_index`] instead.
    pub fn filter_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let index = EntityDeleteIndex::from_tombstones(self.tombstones);
        self.filter_batch_with_index(batch, &index)
    }

    /// Filter `batch` using a caller-supplied precomputed entity index.
    ///
    /// Avoids per-batch HashSet construction in the entity-delete
    /// path — the wrappers in `tombstone_scan.rs` build one
    /// [`EntityDeleteIndex`] per scan and pass it through here for
    /// every batch.
    pub fn filter_batch_with_index(
        &self,
        batch: RecordBatch,
        entity_index: &EntityDeleteIndex,
    ) -> Result<RecordBatch> {
        if self.tombstones.is_empty() || batch.num_rows() == 0 {
            return Ok(batch);
        }

        let num_rows = batch.num_rows();
        let mut alive = vec![true; num_rows];

        // 1. Batch-level: __batch_id (SS7.1 — cheapest check)
        self.apply_batch_deletes(&batch, &mut alive)?;
        // 2. Entity-level
        self.apply_entity_deletes_with_index(&batch, &mut alive, entity_index)?;
        // 3. Row-level: __seq_id
        self.apply_row_deletes(&batch, &mut alive)?;
        // 4. Time-range
        self.apply_time_range_deletes(&batch, &mut alive)?;

        // Skip the Arrow filter call when no rows were removed — avoids
        // copying the entire batch for the common no-match case.
        if alive.iter().all(|&a| a) {
            return Ok(batch);
        }

        let mask = arrow::array::BooleanArray::from(alive);
        filter_record_batch(&batch, &mask).map_err(|e| {
            BqliteError::Execution(format!("tombstone filter_record_batch failed: {e}"))
        })
    }

    pub(crate) fn apply_batch_deletes(
        &self,
        batch: &RecordBatch,
        alive: &mut [bool],
    ) -> Result<()> {
        if self.tombstones.batch_deletes.is_empty() {
            return Ok(());
        }
        let col_idx = batch
            .schema()
            .index_of(bqlite_core::BATCH_ID_COLUMN)
            .map_err(|_| {
                BqliteError::Execution(format!(
                    "tombstone filter: batch tombstones exist but column '{}' not in batch",
                    bqlite_core::BATCH_ID_COLUMN
                ))
            })?;
        let col = batch.column(col_idx);
        let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            BqliteError::Execution(format!(
                "tombstone filter: {} column is not Int64Array",
                bqlite_core::BATCH_ID_COLUMN
            ))
        })?;
        for (i, flag) in alive.iter_mut().enumerate() {
            if *flag
                && self
                    .tombstones
                    .batch_deletes
                    .contains(&(arr.value(i) as u64))
            {
                *flag = false;
            }
        }
        Ok(())
    }

    /// Apply entity-level tombstones using a caller-supplied precomputed
    /// index. The index is built once per scan via
    /// [`TombstoneFile::entity_delete_index`] and reused across every
    /// batch — `filter_batch` builds an inline index for ad-hoc use.
    pub(crate) fn apply_entity_deletes_with_index(
        &self,
        batch: &RecordBatch,
        alive: &mut [bool],
        index: &EntityDeleteIndex,
    ) -> Result<()> {
        if index.is_empty() {
            return Ok(());
        }
        let col_idx = batch.schema().index_of(self.entity_key_col).map_err(|_| {
            BqliteError::Execution(format!(
                "tombstone filter: entity key column '{}' not found in batch",
                self.entity_key_col
            ))
        })?;
        let col = batch.column(col_idx);

        if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
            for (i, flag) in alive.iter_mut().enumerate() {
                if *flag && index.str_set.contains(arr.value(i)) {
                    *flag = false;
                }
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            for (i, flag) in alive.iter_mut().enumerate() {
                if *flag && index.int_set.contains(&arr.value(i)) {
                    *flag = false;
                }
            }
        } else {
            return Err(BqliteError::Execution(format!(
                "tombstone filter: unsupported entity key column type {:?}",
                col.data_type()
            )));
        }
        Ok(())
    }

    pub(crate) fn apply_row_deletes(&self, batch: &RecordBatch, alive: &mut [bool]) -> Result<()> {
        if self.tombstones.row_deletes.is_empty() {
            return Ok(());
        }
        let col_idx = batch
            .schema()
            .index_of(bqlite_core::SEQ_ID_COLUMN)
            .map_err(|_| {
                BqliteError::Execution(format!(
                    "tombstone filter: row tombstones exist but column '{}' not in batch",
                    bqlite_core::SEQ_ID_COLUMN
                ))
            })?;
        let col = batch.column(col_idx);
        let arr = col.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            BqliteError::Execution(format!(
                "tombstone filter: {} column is not Int64Array",
                bqlite_core::SEQ_ID_COLUMN
            ))
        })?;
        for (i, flag) in alive.iter_mut().enumerate() {
            if *flag && self.tombstones.row_deletes.contains(&(arr.value(i) as u64)) {
                *flag = false;
            }
        }
        Ok(())
    }

    pub(crate) fn apply_time_range_deletes(
        &self,
        batch: &RecordBatch,
        alive: &mut [bool],
    ) -> Result<()> {
        if self.tombstones.time_range_deletes.is_empty() {
            return Ok(());
        }
        let col_idx = batch.schema().index_of(self.ts_col).map_err(|_| {
            BqliteError::Execution(format!(
                "tombstone filter: timestamp column '{}' not found in batch",
                self.ts_col
            ))
        })?;
        let col = batch.column(col_idx);
        let ts_arr = col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| {
                BqliteError::Execution(format!(
                    "tombstone filter: timestamp column '{}' is not TimestampNanosecondArray",
                    self.ts_col
                ))
            })?;

        for (i, flag) in alive.iter_mut().enumerate() {
            if *flag {
                let ts = ts_arr.value(i);
                for range in &self.tombstones.time_range_deletes {
                    if range.contains_timestamp(ts) {
                        *flag = false;
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringViewArray, TimestampNanosecondArray};

    use super::*;

    // Per-test unique temp directory. Mirrors the `Scratch` pattern in
    // `database.rs` tests — no `tempfile` dev-dep needed.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-tombstone-{label}-{pid}-{seq}"));
            fs::create_dir_all(&path).expect("create scratch dir");
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

    // ── TombstoneFile defaults ──────────────────────────────────────────

    #[test]
    fn default_is_empty() {
        let tf = TombstoneFile::default();
        assert!(tf.is_empty());
        assert!(tf.entity_deletes.is_empty());
        assert!(tf.row_deletes.is_empty());
        assert!(tf.batch_deletes.is_empty());
        assert!(tf.time_range_deletes.is_empty());
    }

    // ── Typed constructors ──────────────────────────────────────────────

    #[test]
    fn for_rows_builds_row_deletes() {
        let tf = TombstoneFile::for_rows([1, 2, 3]);
        assert_eq!(tf.row_deletes, HashSet::from([1, 2, 3]));
        assert!(tf.batch_deletes.is_empty());
        assert!(tf.entity_deletes.is_empty());
        assert!(tf.time_range_deletes.is_empty());
        assert!(!tf.is_empty());
    }

    #[test]
    fn for_batches_builds_batch_deletes() {
        let tf = TombstoneFile::for_batches([10, 20]);
        assert_eq!(tf.batch_deletes, HashSet::from([10, 20]));
        assert!(tf.row_deletes.is_empty());
    }

    #[test]
    fn for_entities_builds_entity_deletes() {
        let tf = TombstoneFile::for_entities([
            ScalarValue::String("alice".into()),
            ScalarValue::String("bob".into()),
        ]);
        assert_eq!(tf.entity_deletes.len(), 2);
        assert!(tf
            .entity_deletes
            .contains(&ScalarValue::String("alice".into())));
    }

    #[test]
    fn for_time_range_builds_single_range() {
        let range = TimeRangeDelete {
            min_ts: Some(100),
            min_inclusive: true,
            max_ts: Some(200),
            max_inclusive: false,
        };
        let tf = TombstoneFile::for_time_range(range.clone());
        assert_eq!(tf.time_range_deletes, vec![range]);
        assert!(tf.row_deletes.is_empty());
    }

    // ── Merge ───────────────────────────────────────────────────────────

    #[test]
    fn merge_unions_sets() {
        let mut base = TombstoneFile::for_rows([1, 2]);
        let other = TombstoneFile::for_rows([2, 3]);
        base.merge(&other);
        assert_eq!(base.row_deletes, HashSet::from([1, 2, 3]));
    }

    #[test]
    fn merge_unions_batches() {
        let mut base = TombstoneFile::for_batches([10]);
        let other = TombstoneFile::for_batches([20]);
        base.merge(&other);
        assert_eq!(base.batch_deletes, HashSet::from([10, 20]));
    }

    #[test]
    fn merge_unions_entities() {
        let mut base = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let other = TombstoneFile::for_entities([ScalarValue::String("bob".into())]);
        base.merge(&other);
        assert_eq!(base.entity_deletes.len(), 2);
    }

    #[test]
    fn merge_appends_time_ranges_with_dedup() {
        let r1 = TimeRangeDelete {
            min_ts: Some(0),
            min_inclusive: true,
            max_ts: Some(100),
            max_inclusive: false,
        };
        let r2 = TimeRangeDelete {
            min_ts: Some(200),
            min_inclusive: true,
            max_ts: Some(300),
            max_inclusive: true,
        };
        let mut base = TombstoneFile::for_time_range(r1.clone());
        let other = TombstoneFile {
            time_range_deletes: vec![r1.clone(), r2.clone()],
            ..Default::default()
        };
        base.merge(&other);
        // r1 is deduplicated; only r1 + r2 remain.
        assert_eq!(base.time_range_deletes.len(), 2);
        assert_eq!(base.time_range_deletes[0], r1);
        assert_eq!(base.time_range_deletes[1], r2);
    }

    #[test]
    fn merge_across_granularities() {
        let mut base = TombstoneFile::for_rows([1]);
        let other = TombstoneFile {
            batch_deletes: HashSet::from([10]),
            entity_deletes: HashSet::from([ScalarValue::Int(42)]),
            time_range_deletes: vec![TimeRangeDelete {
                min_ts: None,
                min_inclusive: false,
                max_ts: Some(500),
                max_inclusive: true,
            }],
            ..Default::default()
        };
        base.merge(&other);
        assert_eq!(base.row_deletes, HashSet::from([1]));
        assert_eq!(base.batch_deletes, HashSet::from([10]));
        assert!(base.entity_deletes.contains(&ScalarValue::Int(42)));
        assert_eq!(base.time_range_deletes.len(), 1);
    }

    #[test]
    fn merge_into_empty() {
        let mut base = TombstoneFile::default();
        let other = TombstoneFile::for_rows([1, 2, 3]);
        base.merge(&other);
        assert_eq!(base.row_deletes, HashSet::from([1, 2, 3]));
    }

    #[test]
    fn merge_empty_into_existing() {
        let mut base = TombstoneFile::for_rows([1, 2]);
        let other = TombstoneFile::default();
        base.merge(&other);
        assert_eq!(base.row_deletes, HashSet::from([1, 2]));
    }

    // ── TimeRangeDelete::contains_timestamp ─────────────────────────────

    #[test]
    fn contains_inclusive_both() {
        let range = TimeRangeDelete {
            min_ts: Some(100),
            min_inclusive: true,
            max_ts: Some(200),
            max_inclusive: true,
        };
        assert!(!range.contains_timestamp(99));
        assert!(range.contains_timestamp(100));
        assert!(range.contains_timestamp(150));
        assert!(range.contains_timestamp(200));
        assert!(!range.contains_timestamp(201));
    }

    #[test]
    fn contains_exclusive_both() {
        let range = TimeRangeDelete {
            min_ts: Some(100),
            min_inclusive: false,
            max_ts: Some(200),
            max_inclusive: false,
        };
        assert!(!range.contains_timestamp(100));
        assert!(range.contains_timestamp(101));
        assert!(range.contains_timestamp(199));
        assert!(!range.contains_timestamp(200));
    }

    #[test]
    fn contains_unbounded_below() {
        let range = TimeRangeDelete {
            min_ts: None,
            min_inclusive: false,
            max_ts: Some(100),
            max_inclusive: false,
        };
        assert!(range.contains_timestamp(i64::MIN));
        assert!(range.contains_timestamp(0));
        assert!(range.contains_timestamp(99));
        assert!(!range.contains_timestamp(100));
    }

    #[test]
    fn contains_unbounded_above() {
        let range = TimeRangeDelete {
            min_ts: Some(100),
            min_inclusive: true,
            max_ts: None,
            max_inclusive: false,
        };
        assert!(!range.contains_timestamp(99));
        assert!(range.contains_timestamp(100));
        assert!(range.contains_timestamp(i64::MAX));
    }

    #[test]
    fn contains_fully_unbounded() {
        let range = TimeRangeDelete {
            min_ts: None,
            min_inclusive: false,
            max_ts: None,
            max_inclusive: false,
        };
        assert!(range.contains_timestamp(i64::MIN));
        assert!(range.contains_timestamp(0));
        assert!(range.contains_timestamp(i64::MAX));
    }

    // ── Serde roundtrip ─────────────────────────────────────────────────

    #[test]
    fn serde_roundtrip_empty() {
        let tf = TombstoneFile::default();
        let json = serde_json::to_string(&tf).unwrap();
        let deserialized: TombstoneFile = serde_json::from_str(&json).unwrap();
        assert_eq!(tf, deserialized);
    }

    #[test]
    fn serde_roundtrip_populated() {
        let tf = TombstoneFile {
            entity_deletes: HashSet::from([
                ScalarValue::String("alice".into()),
                ScalarValue::Int(42),
            ]),
            row_deletes: HashSet::from([1, 2, 3]),
            batch_deletes: HashSet::from([10]),
            time_range_deletes: vec![
                TimeRangeDelete {
                    min_ts: Some(100),
                    min_inclusive: true,
                    max_ts: Some(200),
                    max_inclusive: false,
                },
                TimeRangeDelete {
                    min_ts: None,
                    min_inclusive: false,
                    max_ts: Some(500),
                    max_inclusive: true,
                },
            ],
        };
        let json = serde_json::to_vec_pretty(&tf).unwrap();
        let deserialized: TombstoneFile = serde_json::from_slice(&json).unwrap();
        assert_eq!(tf, deserialized);
    }

    // ── I/O helpers ─────────────────────────────────────────────────────

    #[test]
    fn tombstone_path_format() {
        let path = tombstone_file_path(Path::new("/db"), "events", 1, 3);
        assert_eq!(
            path,
            PathBuf::from("/db/events/windows/w_000001/shard_03/tombstones.json")
        );
    }

    #[test]
    fn read_nonexistent_returns_empty() {
        let path = Path::new("/nonexistent/tombstones.json");
        let tf = read_tombstone_file(path).unwrap();
        assert!(tf.is_empty());
    }

    #[test]
    fn write_and_read_roundtrip() {
        let scratch = Scratch::new("write-read");
        let path = tombstone_file_path(scratch.path(), "events", 1, 0);

        let tf = TombstoneFile {
            row_deletes: HashSet::from([1, 2, 3]),
            batch_deletes: HashSet::from([10]),
            entity_deletes: HashSet::from([ScalarValue::String("alice".into())]),
            time_range_deletes: vec![TimeRangeDelete {
                min_ts: Some(0),
                min_inclusive: true,
                max_ts: Some(1000),
                max_inclusive: false,
            }],
        };

        write_tombstone_atomic(&path, &tf).unwrap();
        let loaded = read_tombstone_file(&path).unwrap();
        assert_eq!(tf, loaded);
    }

    #[test]
    fn write_creates_parent_dirs() {
        let scratch = Scratch::new("create-dirs");
        let path = tombstone_file_path(scratch.path(), "table", 42, 7);

        let tf = TombstoneFile::for_rows([99]);
        write_tombstone_atomic(&path, &tf).unwrap();

        let loaded = read_tombstone_file(&path).unwrap();
        assert_eq!(tf, loaded);
    }

    #[test]
    fn write_no_stale_tmp() {
        let scratch = Scratch::new("no-tmp");
        let path = tombstone_file_path(scratch.path(), "t", 1, 0);

        write_tombstone_atomic(&path, &TombstoneFile::for_rows([1])).unwrap();

        let shard_dir = path.parent().unwrap();
        let tmp = shard_dir.join(TOMBSTONE_TMP_FILE_NAME);
        assert!(!tmp.exists(), "tmp file should be cleaned up by rename");
    }

    #[test]
    fn write_overwrites_existing() {
        let scratch = Scratch::new("overwrite");
        let path = tombstone_file_path(scratch.path(), "t", 1, 0);

        let tf1 = TombstoneFile::for_rows([1, 2]);
        write_tombstone_atomic(&path, &tf1).unwrap();

        let tf2 = TombstoneFile::for_rows([3, 4, 5]);
        write_tombstone_atomic(&path, &tf2).unwrap();

        let loaded = read_tombstone_file(&path).unwrap();
        assert_eq!(loaded, tf2);
    }

    #[test]
    fn corrupt_tombstone_file_returns_error() {
        let scratch = Scratch::new("corrupt");
        let path = scratch.path().join(TOMBSTONE_FILE_NAME);
        fs::write(&path, b"not valid json").unwrap();

        let err = read_tombstone_file(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("corrupt tombstone file"), "got: {msg}");
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    #[test]
    fn empty_snapshot() {
        let snap = TombstoneSnapshot::empty();
        assert!(snap.is_empty());
        assert!(snap.get(0, 0).is_none());
    }

    #[test]
    fn load_snapshot_skips_empty_shards() {
        let scratch = Scratch::new("snap-skip");
        // Write tombstones only to shard 0.
        let path0 = tombstone_file_path(scratch.path(), "events", 1, 0);
        write_tombstone_atomic(&path0, &TombstoneFile::for_rows([100])).unwrap();

        let snap = load_tombstone_snapshot(scratch.path(), "events", &[(1, 0), (1, 1)]).unwrap();

        assert!(!snap.is_empty());
        assert!(snap.get(1, 0).is_some());
        assert!(snap.get(1, 1).is_none());
    }

    #[test]
    fn load_snapshot_multiple_windows() {
        let scratch = Scratch::new("snap-multi");

        let path_w1 = tombstone_file_path(scratch.path(), "events", 1, 0);
        write_tombstone_atomic(&path_w1, &TombstoneFile::for_rows([1])).unwrap();

        let path_w2 = tombstone_file_path(scratch.path(), "events", 2, 0);
        write_tombstone_atomic(&path_w2, &TombstoneFile::for_batches([10])).unwrap();

        let snap = load_tombstone_snapshot(scratch.path(), "events", &[(1, 0), (2, 0)]).unwrap();

        let tf1 = snap.get(1, 0).unwrap();
        assert!(tf1.row_deletes.contains(&1));

        let tf2 = snap.get(2, 0).unwrap();
        assert!(tf2.batch_deletes.contains(&10));
    }

    #[test]
    fn snapshot_get_returns_none_for_unknown_shard() {
        let scratch = Scratch::new("snap-unknown");
        let snap = load_tombstone_snapshot(scratch.path(), "events", &[(1, 0)]).unwrap();
        assert!(snap.get(99, 99).is_none());
    }

    #[test]
    fn from_map_drops_empty_entries() {
        let snap = TombstoneSnapshot::from_map([
            ((0, 0), TombstoneFile::for_rows([1, 2])),
            ((0, 1), TombstoneFile::default()), // empty — should be dropped
            ((1, 0), TombstoneFile::for_batches([42])),
        ]);
        assert_eq!(snap.len(), 2);
        assert!(snap.get(0, 0).is_some());
        assert!(snap.get(0, 1).is_none(), "empty entry dropped");
        assert!(snap.get(1, 0).is_some());
    }

    #[test]
    fn from_map_preserves_non_empty_contents() {
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let snap = TombstoneSnapshot::from_map([((7, 3), tf.clone())]);
        let got = snap.get(7, 3).expect("entry present");
        assert_eq!(got, &tf);
    }

    #[test]
    fn from_map_empty_input_is_empty() {
        let snap = TombstoneSnapshot::from_map(std::iter::empty());
        assert!(snap.is_empty());
        assert_eq!(snap.len(), 0);
    }

    // ── TombstoneFilter ────────────────────────────────────────────────

    fn make_filter_test_batch(ids: &[&str], tss: &[i64], evts: &[&str]) -> RecordBatch {
        use arrow::array::{ArrayRef, StringViewArray, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8View, false),
        ]));
        let id_arr: ArrayRef = Arc::new(StringViewArray::from(ids.to_vec()));
        let ts_arr: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(tss.iter().copied().map(Some).collect::<Vec<_>>())
                .with_timezone("UTC"),
        );
        let evt_arr: ArrayRef = Arc::new(StringViewArray::from(evts.to_vec()));
        RecordBatch::try_new(schema, vec![id_arr, ts_arr, evt_arr]).unwrap()
    }

    #[test]
    fn filter_noop_on_empty_tombstones() {
        let tf = TombstoneFile::default();
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(&["alice", "bob"], &[100, 200], &["click", "view"]);
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn filter_noop_on_empty_batch() {
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(&[], &[], &[]);
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn filter_entity_deletes_string() {
        let tf = TombstoneFile::for_entities([ScalarValue::String("alice".into())]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(
            &["alice", "bob", "alice", "carol"],
            &[100, 200, 300, 400],
            &["click", "view", "click", "view"],
        );
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "bob");
        assert_eq!(ids.value(1), "carol");
    }

    #[test]
    fn filter_entity_deletes_int() {
        use arrow::array::{ArrayRef, TimestampNanosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
        ]));
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let tss: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(100), Some(200), Some(300)])
                .with_timezone("UTC"),
        );
        let batch = RecordBatch::try_new(schema, vec![ids, tss]).unwrap();

        let tf = TombstoneFile::for_entities([ScalarValue::Int(2)]);
        let filter = TombstoneFilter::new(&tf, "user_id", "ts");
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 3);
    }

    #[test]
    fn filter_time_range_deletes() {
        let range = TimeRangeDelete {
            min_ts: Some(150),
            min_inclusive: true,
            max_ts: Some(250),
            max_inclusive: false,
        };
        let tf = TombstoneFile::for_time_range(range);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(
            &["a", "b", "c", "d"],
            &[100, 150, 200, 300],
            &["e1", "e2", "e3", "e4"],
        );
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let tss = result
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(tss.value(0), 100);
        assert_eq!(tss.value(1), 300);
    }

    #[test]
    fn filter_multiple_time_ranges() {
        let tf = TombstoneFile {
            time_range_deletes: vec![
                TimeRangeDelete {
                    min_ts: None,
                    min_inclusive: false,
                    max_ts: Some(100),
                    max_inclusive: true,
                },
                TimeRangeDelete {
                    min_ts: Some(300),
                    min_inclusive: false,
                    max_ts: None,
                    max_inclusive: false,
                },
            ],
            ..Default::default()
        };
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(
            &["a", "b", "c", "d"],
            &[50, 100, 200, 400],
            &["e1", "e2", "e3", "e4"],
        );
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 1);
        let tss = result
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(tss.value(0), 200);
    }

    #[test]
    fn filter_batch_deletes() {
        use arrow::array::ArrayRef;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("__batch_id", DataType::Int64, false),
        ]));
        let ids: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
        let tss: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(100), Some(200), Some(300)])
                .with_timezone("UTC"),
        );
        let batches: ArrayRef = Arc::new(Int64Array::from(vec![5, 10, 5]));
        let batch = RecordBatch::try_new(schema, vec![ids, tss, batches]).unwrap();

        let tf = TombstoneFile::for_batches([10]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let batch_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(batch_col.value(0), 5);
        assert_eq!(batch_col.value(1), 5);
    }

    #[test]
    fn filter_row_deletes() {
        use arrow::array::ArrayRef;
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        let schema = Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("__seq_id", DataType::Int64, false),
        ]));
        let ids: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
        let tss: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![Some(100), Some(200), Some(300)])
                .with_timezone("UTC"),
        );
        let seqs: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let batch = RecordBatch::try_new(schema, vec![ids, tss, seqs]).unwrap();

        let tf = TombstoneFile::for_rows([20]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 2);
        let seq_col = result
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(seq_col.value(0), 10);
        assert_eq!(seq_col.value(1), 30);
    }

    #[test]
    fn filter_combined_entity_and_time_range() {
        let tf = TombstoneFile {
            entity_deletes: HashSet::from([ScalarValue::String("alice".into())]),
            time_range_deletes: vec![TimeRangeDelete {
                min_ts: Some(250),
                min_inclusive: true,
                max_ts: None,
                max_inclusive: false,
            }],
            ..Default::default()
        };
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(
            &["alice", "bob", "carol", "bob"],
            &[100, 200, 300, 400],
            &["e1", "e2", "e3", "e4"],
        );
        let result = filter.filter_batch(batch).unwrap();
        // alice removed by entity; carol(300) and bob(400) removed by time-range
        assert_eq!(result.num_rows(), 1);
        let ids = result
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(ids.value(0), "bob");
    }

    #[test]
    fn filter_all_rows_removed() {
        let tf = TombstoneFile::for_entities([ScalarValue::String("a".into())]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(&["a", "a"], &[100, 200], &["e1", "e2"]);
        let result = filter.filter_batch(batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn filter_missing_seq_id_column_errors_when_needed() {
        let tf = TombstoneFile::for_rows([1]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(&["a"], &[100], &["e1"]);
        let err = filter.filter_batch(batch).unwrap_err();
        assert!(err.to_string().contains("__seq_id"), "got: {err}");
    }

    #[test]
    fn filter_missing_batch_id_column_errors_when_needed() {
        let tf = TombstoneFile::for_batches([1]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let batch = make_filter_test_batch(&["a"], &[100], &["e1"]);
        let err = filter.filter_batch(batch).unwrap_err();
        assert!(err.to_string().contains("__batch_id"), "got: {err}");
    }

    // Pins the invariant the entity-delete cache relies on: a single
    // precomputed `EntityDeleteIndex` reused across many batches must
    // produce bit-identical results to building the index inline per
    // batch (the prior `filter_batch` behavior).
    #[test]
    fn filter_batch_with_index_matches_inline_across_batches() {
        let tf = TombstoneFile::for_entities([
            ScalarValue::String("alice".into()),
            ScalarValue::String("carol".into()),
        ]);
        let filter = TombstoneFilter::new(&tf, "entity_id", "ts");
        let index = tf.entity_delete_index();

        let batches = vec![
            make_filter_test_batch(&["alice", "bob"], &[100, 200], &["e1", "e2"]),
            make_filter_test_batch(&["carol", "dave"], &[300, 400], &["e3", "e4"]),
            make_filter_test_batch(
                &["alice", "carol", "eve"],
                &[500, 600, 700],
                &["e5", "e6", "e7"],
            ),
        ];

        for batch in batches {
            let inline = filter.filter_batch(batch.clone()).unwrap();
            let cached = filter.filter_batch_with_index(batch, &index).unwrap();
            assert_eq!(inline.num_rows(), cached.num_rows());
            let inline_ids = inline
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            let cached_ids = cached
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            for i in 0..inline.num_rows() {
                assert_eq!(inline_ids.value(i), cached_ids.value(i));
            }
        }
    }
}
