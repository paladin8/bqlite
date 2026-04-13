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
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

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
}
