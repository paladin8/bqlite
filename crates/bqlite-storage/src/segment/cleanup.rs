//! Startup orphan-segment reconciliation (TASK-239).
//!
//! Implements the crash-safety contract from `docs/reliability.md`
//! §Crash Safety and `docs/design/storage-format.md` §7.4.
//!
//! [`reconcile_segments`] is called by `Database::open` after
//! the manifest is loaded but before any queries or ingests run. It
//! walks every `(window, shard)` directory under the database root and:
//!
//! 1. **Deletes `.tmp` segment files.** Any file ending in `.tmp` is a
//!    partially-written segment from a crash mid-ingest or mid-compaction
//!    and is unconditionally deleted.
//! 2. **Deletes manifest-orphaned segments.** Any `.seg` file whose path
//!    is not in the manifest's active segment set is an orphan — from a
//!    deferred compaction delete, a crashed manifest update, or a dropped
//!    table whose files weren't reaped. These are removed.
//!
//! The pass is **read-only with respect to the manifest** — it only
//! deletes files, never edits the manifest. Re-running on a clean
//! database is a no-op.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use bqlite_core::error::{BqliteError, Result};

use crate::manifest::Manifest;

/// Summary of what the reconciliation pass found and cleaned up.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Number of `.tmp` files deleted (partial writes from crashes).
    pub tmp_files_removed: u64,
    /// Number of orphaned `.seg` files deleted (not in manifest).
    pub orphan_segments_removed: u64,
}

/// Reconcile on-disk segment files against the manifest.
///
/// Must be called while the exclusive database lock is held, after the
/// manifest has been loaded but before any write operations begin.
///
/// # Errors
///
/// Returns `BqliteError::Io` if a directory walk or file deletion fails
/// with an error other than `NotFound` (which is tolerated because a
/// concurrent cleanup or filesystem race is benign).
pub fn reconcile_segments(db_root: &Path, manifest: &Manifest) -> Result<ReconcileReport> {
    let active = collect_active_segment_paths(db_root, manifest);
    let mut report = ReconcileReport::default();

    // Walk all top-level entries under db_root. Each directory that
    // contains a `windows/` subdirectory is treated as a table
    // directory, regardless of whether the table is still in the
    // manifest (dropped tables leave orphan files too).
    let entries = match fs::read_dir(db_root) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(e) => {
            return Err(BqliteError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "reconcile_segments: failed to read database root {}: {e}",
                    db_root.display()
                ),
            )));
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue, // skip unreadable entries
        };
        // Skip known non-table files at the root level.
        if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
            continue;
        }
        let table_dir = entry.path();
        let windows_dir = table_dir.join("windows");
        if windows_dir.is_dir() {
            walk_windows_dir(&windows_dir, &active, &mut report)?;
        }
    }

    Ok(report)
}

/// Build the set of absolute paths for every segment referenced by the
/// manifest. Uses the same path-construction logic as `writer.rs`'s
/// `segment_file_path`.
fn collect_active_segment_paths(db_root: &Path, manifest: &Manifest) -> HashSet<PathBuf> {
    let mut paths = HashSet::new();
    for (table_name, entry) in &manifest.tables {
        for window in &entry.windows {
            for (shard_idx, segments) in window.shards.iter().enumerate() {
                for seg in segments {
                    let path = db_root
                        .join(table_name)
                        .join("windows")
                        .join(format!("w_{:06}", window.window_id))
                        .join(format!("shard_{:02}", shard_idx))
                        .join(format!("segment_{}.seg", seg.segment_id));
                    paths.insert(path);
                }
            }
        }
    }
    paths
}

/// Walk every `w_*/shard_*/` directory under a `windows/` directory,
/// cleaning up orphan files.
fn walk_windows_dir(
    windows_dir: &Path,
    active: &HashSet<PathBuf>,
    report: &mut ReconcileReport,
) -> Result<()> {
    let window_entries = match fs::read_dir(windows_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(BqliteError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "reconcile_segments: failed to read windows dir {}: {e}",
                    windows_dir.display()
                ),
            )));
        }
    };

    for window_entry in window_entries {
        let window_entry = match window_entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if !window_entry
            .file_type()
            .map(|ft| ft.is_dir())
            .unwrap_or(false)
        {
            continue;
        }
        let window_dir = window_entry.path();
        let shard_entries = match fs::read_dir(&window_dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(BqliteError::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "reconcile_segments: failed to read window dir {}: {e}",
                        window_dir.display()
                    ),
                )));
            }
        };

        for shard_entry in shard_entries {
            let shard_entry = match shard_entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            if !shard_entry
                .file_type()
                .map(|ft| ft.is_dir())
                .unwrap_or(false)
            {
                continue;
            }
            clean_shard_dir(&shard_entry.path(), active, report)?;
        }
    }

    Ok(())
}

/// Inspect every file in a single shard directory and remove orphans.
fn clean_shard_dir(
    shard_dir: &Path,
    active: &HashSet<PathBuf>,
    report: &mut ReconcileReport,
) -> Result<()> {
    let file_entries = match fs::read_dir(shard_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(BqliteError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "reconcile_segments: failed to read shard dir {}: {e}",
                    shard_dir.display()
                ),
            )));
        }
    };

    for file_entry in file_entries {
        let file_entry = match file_entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = file_entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        if name.ends_with(".tmp") {
            // Rule 1: any `.tmp` file is a partial write — delete
            // unconditionally.
            best_effort_remove(&path)?;
            report.tmp_files_removed += 1;
        } else if name.ends_with(".seg") && !active.contains(&path) {
            // Rule 2: a `.seg` file not referenced by the manifest is
            // an orphan from a deferred compaction delete or a dropped
            // table.
            best_effort_remove(&path)?;
            report.orphan_segments_removed += 1;
        }
        // Other files (e.g., `.lock`, metadata) are left alone.
    }

    Ok(())
}

/// Delete a file, tolerating `NotFound` (already removed by a
/// concurrent cleanup or filesystem race).
fn best_effort_remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(BqliteError::Io(std::io::Error::new(
            e.kind(),
            format!(
                "reconcile_segments: failed to delete orphan {}: {e}",
                path.display()
            ),
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bqlite_core::property::{BqlType, PropertyValue};
    use bqlite_core::schema::{ColumnDef, TableSchema};

    use crate::manifest::{
        ColumnStats, Manifest, SegmentMeta, TableEntry, WindowManifest, DEFAULT_SHARD_COUNT,
    };

    // Per-test unique temp directory — mirrors the `Scratch` pattern
    // from database.rs tests. PID + monotonic counter gives in-process
    // uniqueness without pulling in `tempfile`.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-cleanup-{label}-{pid}-{seq}"));
            fs::create_dir_all(&path).unwrap();
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

    /// Build a minimal `SegmentMeta` for testing.
    fn test_segment_meta(segment_id: u64) -> SegmentMeta {
        SegmentMeta {
            segment_id,
            level: 0,
            schema_version: 1,
            row_count: 100,
            byte_size: 4096,
            ts_range: (0, 1_000_000),
            entity_range: (
                PropertyValue::String("a".into()),
                PropertyValue::String("z".into()),
            ),
            column_stats: vec![ColumnStats {
                column_name: "entity_id".into(),
                min: Some(PropertyValue::String("a".into())),
                max: Some(PropertyValue::String("z".into())),
                null_count: 0,
                distinct_count_estimate: None,
            }],
            created_at: 1_000_000,
            batch_id: 0,
            seq_id_range: (0, 99),
        }
    }

    /// Build a minimal `TableSchema` for testing.
    fn test_schema() -> TableSchema {
        TableSchema::new(
            "test",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .expect("minimal schema must pass §5.1 validation")
    }

    /// Build a manifest with one table containing one active segment at
    /// the given `(window_id, shard_id)`.
    fn manifest_with_segment(
        table: &str,
        window_id: u32,
        shard_id: usize,
        segment_id: u64,
    ) -> Manifest {
        let shard_count = DEFAULT_SHARD_COUNT as usize;
        let mut shards = vec![Vec::new(); shard_count];
        shards[shard_id] = vec![test_segment_meta(segment_id)];

        let mut manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        manifest.tables.insert(
            table.into(),
            TableEntry {
                schema: test_schema(),
                next_sequence_id: 0,
                next_batch_id: 1,
                next_segment_id: segment_id + 1,
                bootstrap_events_table: false,
                windows: vec![WindowManifest { window_id, shards }],
            },
        );
        manifest
    }

    /// Create the on-disk directory layout for a `(table, window, shard)`.
    fn create_shard_dir(root: &Path, table: &str, window_id: u32, shard_id: usize) -> PathBuf {
        let dir = root
            .join(table)
            .join("windows")
            .join(format!("w_{window_id:06}"))
            .join(format!("shard_{shard_id:02}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Write a dummy file at the given path.
    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, b"dummy").unwrap();
    }

    // ── Test: happy path — no orphans ───────────────────────────────

    #[test]
    fn clean_database_is_noop() {
        let scratch = Scratch::new("clean-noop");
        let root = scratch.path();

        let manifest = manifest_with_segment("events", 1, 0, 42);
        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("segment_42.seg"));

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.tmp_files_removed, 0);
        assert_eq!(report.orphan_segments_removed, 0);
        // The active segment file must still exist.
        assert!(shard_dir.join("segment_42.seg").exists());
    }

    // ── Test: active segment is never deleted ───────────────────────

    #[test]
    fn active_segment_never_deleted() {
        let scratch = Scratch::new("active-survives");
        let root = scratch.path();

        let manifest = manifest_with_segment("events", 1, 0, 99);
        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("segment_99.seg"));
        // Also place an orphan next to it.
        touch(&shard_dir.join("segment_0.seg"));

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.orphan_segments_removed, 1);
        // The active segment at id 99 must survive.
        assert!(shard_dir.join("segment_99.seg").exists());
        // The orphan at id 0 is removed.
        assert!(!shard_dir.join("segment_0.seg").exists());
    }

    // ── Test: crashed ingest — orphan .tmp file ─────────────────────

    #[test]
    fn crashed_ingest_tmp_file_removed() {
        let scratch = Scratch::new("crashed-ingest");
        let root = scratch.path();

        // A crashed ingest left a .tmp file but the manifest was never
        // updated, so the empty manifest lists no segments.
        let manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("segment_5.seg.tmp"));

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.tmp_files_removed, 1);
        assert_eq!(report.orphan_segments_removed, 0);
        assert!(!shard_dir.join("segment_5.seg.tmp").exists());
    }

    // ── Test: crashed compaction — orphan .tmp + orphan input seg ───

    #[test]
    fn crashed_compaction_cleanup() {
        let scratch = Scratch::new("crashed-compact");
        let root = scratch.path();

        // Before compaction, the manifest had segments 10 and 11. The
        // compaction wrote a new segment to .tmp and then crashed before
        // updating the manifest. The old manifest (still listing 10 and
        // 11) is what we load on the next startup.
        let shard_count = DEFAULT_SHARD_COUNT as usize;
        let mut shards = vec![Vec::new(); shard_count];
        shards[0] = vec![test_segment_meta(10), test_segment_meta(11)];

        let mut manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        manifest.tables.insert(
            "events".into(),
            TableEntry {
                schema: test_schema(),
                next_sequence_id: 0,
                next_batch_id: 1,
                next_segment_id: 20,
                bootstrap_events_table: false,
                windows: vec![WindowManifest {
                    window_id: 1,
                    shards,
                }],
            },
        );

        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("segment_10.seg")); // active input
        touch(&shard_dir.join("segment_11.seg")); // active input
        touch(&shard_dir.join("segment_20.seg.tmp")); // orphan compaction output

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.tmp_files_removed, 1);
        assert_eq!(report.orphan_segments_removed, 0);
        assert!(shard_dir.join("segment_10.seg").exists());
        assert!(shard_dir.join("segment_11.seg").exists());
        assert!(!shard_dir.join("segment_20.seg.tmp").exists());
    }

    // ── Test: crashed compaction completed — orphan old inputs ──────

    #[test]
    fn completed_compaction_orphan_inputs_removed() {
        let scratch = Scratch::new("compact-orphan-inputs");
        let root = scratch.path();

        // Compaction succeeded and the new manifest lists segment 20 as
        // the merged output. Segments 10 and 11 (old compaction inputs)
        // were not yet deleted from disk before the process stopped.
        let manifest = manifest_with_segment("events", 1, 0, 20);

        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("segment_20.seg")); // active output
        touch(&shard_dir.join("segment_10.seg")); // orphan old input
        touch(&shard_dir.join("segment_11.seg")); // orphan old input

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.tmp_files_removed, 0);
        assert_eq!(report.orphan_segments_removed, 2);
        assert!(shard_dir.join("segment_20.seg").exists());
        assert!(!shard_dir.join("segment_10.seg").exists());
        assert!(!shard_dir.join("segment_11.seg").exists());
    }

    // ── Test: dropped table directory cleaned up ────────────────────

    #[test]
    fn dropped_table_segments_cleaned() {
        let scratch = Scratch::new("dropped-table");
        let root = scratch.path();

        // The "old_table" was dropped from the manifest but its
        // directory and segment files remain on disk.
        let manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);

        let shard_dir = create_shard_dir(root, "old_table", 1, 0);
        touch(&shard_dir.join("segment_0.seg"));
        touch(&shard_dir.join("segment_1.seg"));
        touch(&shard_dir.join("segment_2.seg.tmp"));

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.tmp_files_removed, 1);
        assert_eq!(report.orphan_segments_removed, 2);
        assert!(!shard_dir.join("segment_0.seg").exists());
        assert!(!shard_dir.join("segment_1.seg").exists());
        assert!(!shard_dir.join("segment_2.seg.tmp").exists());
    }

    // ── Test: empty database — no table directories at all ──────────

    #[test]
    fn empty_database_is_noop() {
        let scratch = Scratch::new("empty-db");
        let root = scratch.path();

        let manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report, ReconcileReport::default());
    }

    // ── Test: idempotent — running twice yields same result ─────────

    #[test]
    fn idempotent_second_run_is_noop() {
        let scratch = Scratch::new("idempotent");
        let root = scratch.path();

        let manifest = manifest_with_segment("events", 1, 0, 5);
        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("segment_5.seg")); // active
        touch(&shard_dir.join("segment_0.seg")); // orphan
        touch(&shard_dir.join("segment_1.seg.tmp")); // tmp

        let r1 = reconcile_segments(root, &manifest).unwrap();
        assert_eq!(r1.tmp_files_removed, 1);
        assert_eq!(r1.orphan_segments_removed, 1);

        // Second run: everything is already clean.
        let r2 = reconcile_segments(root, &manifest).unwrap();
        assert_eq!(r2, ReconcileReport::default());
        assert!(shard_dir.join("segment_5.seg").exists());
    }

    // ── Test: non-segment files in shard dir are left alone ─────────

    #[test]
    fn non_segment_files_ignored() {
        let scratch = Scratch::new("non-seg-files");
        let root = scratch.path();

        let manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        let shard_dir = create_shard_dir(root, "events", 1, 0);
        touch(&shard_dir.join("notes.txt"));
        touch(&shard_dir.join("metadata.json"));

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report, ReconcileReport::default());
        assert!(shard_dir.join("notes.txt").exists());
        assert!(shard_dir.join("metadata.json").exists());
    }

    // ── Test: multiple tables, multiple windows, multiple shards ────

    #[test]
    fn multi_table_multi_window_multi_shard() {
        let scratch = Scratch::new("multi-table");
        let root = scratch.path();

        let shard_count = DEFAULT_SHARD_COUNT as usize;

        // Table "events": segment 1 in (window=1, shard=0)
        //                  segment 2 in (window=2, shard=1)
        let mut events_shards_w1 = vec![Vec::new(); shard_count];
        events_shards_w1[0] = vec![test_segment_meta(1)];
        let mut events_shards_w2 = vec![Vec::new(); shard_count];
        events_shards_w2[1] = vec![test_segment_meta(2)];

        // Table "clicks": segment 3 in (window=1, shard=0)
        let mut clicks_shards_w1 = vec![Vec::new(); shard_count];
        clicks_shards_w1[0] = vec![test_segment_meta(3)];

        let mut manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        manifest.tables.insert(
            "events".into(),
            TableEntry {
                schema: test_schema(),
                next_sequence_id: 0,
                next_batch_id: 1,
                next_segment_id: 3,
                bootstrap_events_table: false,
                windows: vec![
                    WindowManifest {
                        window_id: 1,
                        shards: events_shards_w1,
                    },
                    WindowManifest {
                        window_id: 2,
                        shards: events_shards_w2,
                    },
                ],
            },
        );
        manifest.tables.insert(
            "clicks".into(),
            TableEntry {
                schema: test_schema(),
                next_sequence_id: 0,
                next_batch_id: 1,
                next_segment_id: 4,
                bootstrap_events_table: false,
                windows: vec![WindowManifest {
                    window_id: 1,
                    shards: clicks_shards_w1,
                }],
            },
        );

        // On-disk: create active segments + orphans.
        let ev_s1 = create_shard_dir(root, "events", 1, 0);
        touch(&ev_s1.join("segment_1.seg")); // active
        touch(&ev_s1.join("segment_99.seg")); // orphan

        let ev_s2 = create_shard_dir(root, "events", 2, 1);
        touch(&ev_s2.join("segment_2.seg")); // active
        touch(&ev_s2.join("segment_88.seg.tmp")); // orphan tmp

        let cl_s1 = create_shard_dir(root, "clicks", 1, 0);
        touch(&cl_s1.join("segment_3.seg")); // active
        touch(&cl_s1.join("segment_77.seg")); // orphan

        let report = reconcile_segments(root, &manifest).unwrap();

        assert_eq!(report.tmp_files_removed, 1);
        assert_eq!(report.orphan_segments_removed, 2);

        // All active segments survive.
        assert!(ev_s1.join("segment_1.seg").exists());
        assert!(ev_s2.join("segment_2.seg").exists());
        assert!(cl_s1.join("segment_3.seg").exists());

        // All orphans removed.
        assert!(!ev_s1.join("segment_99.seg").exists());
        assert!(!ev_s2.join("segment_88.seg.tmp").exists());
        assert!(!cl_s1.join("segment_77.seg").exists());
    }
}
