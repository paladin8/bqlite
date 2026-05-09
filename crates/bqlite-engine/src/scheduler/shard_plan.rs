//! Engine-side helper that enumerates the populated shards of a
//! table from the manifest snapshot and constructs one
//! [`ShardSnapshot`] per shard.
//!
//! Used by the per-shard morsel dispatch (TASK-536). The helper is
//! manifest-only — it does not open segment files or decode zone maps.
//! Empty shards are elided: the design's empty-shard accounting (§3.6)
//! is handled by the morsel queue's drain logic, not by carrying
//! zero-segment snapshots through the dispatch path.

use std::collections::BTreeMap;
use std::sync::Arc;

use bqlite_core::storage::SegmentHandle;
use bqlite_core::{Result, TimeRange};
use bqlite_storage::Database;

use super::morsel::{ShardSnapshot, WindowSegments};

/// Enumerate one [`ShardSnapshot`] per shard that has at least one live
/// segment of `table` overlapping `time_range`. Group order is
/// shard-id ascending then window-id ascending — matching
/// `ManifestSegmentReader::segments` for stable test fixtures.
pub fn enumerate_shard_snapshots(
    db: &Database,
    table: &str,
    time_range: TimeRange,
) -> Result<Vec<ShardSnapshot>> {
    // Reuse the time-range-filtered reader so the snapshot covers
    // exactly the segments the per-shard dispatch will scan. Building
    // one base reader and grouping its handles keeps the manifest read
    // pattern identical to the legacy single-task path — anything we
    // hand to `Database::segment_reader_for_shard` later will see the
    // same segment set.
    let reader = db.segment_reader_for_time_range(table, time_range)?;
    let mut by_shard: BTreeMap<u32, BTreeMap<u64, Vec<SegmentHandle>>> = BTreeMap::new();
    for handle in reader.segments() {
        let h = handle?;
        by_shard
            .entry(h.shard_id)
            .or_default()
            .entry(h.window_id)
            .or_default()
            .push(h);
    }

    let mut out = Vec::with_capacity(by_shard.len());
    for (shard_id, windows) in by_shard {
        let mut win_vec: Vec<WindowSegments> = Vec::with_capacity(windows.len());
        for (window_id, handles) in windows {
            if handles.is_empty() {
                continue;
            }
            win_vec.push(WindowSegments {
                window_id,
                segments: Arc::from(handles),
            });
        }
        if win_vec.iter().any(|w| !w.segments.is_empty()) {
            out.push(ShardSnapshot {
                shard_id,
                windows: win_vec,
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::event::{EntityId, Event};
    use bqlite_core::time::Timestamp;
    use bqlite_storage::writer::SegmentWriter;
    use bqlite_storage::Database;

    fn test_dir(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let mut path = std::env::temp_dir();
        path.push(format!("bqlite-engine-shard-plan-{label}-{pid}-{seq}"));
        path
    }

    fn create_events_db(path: &std::path::Path) -> Database {
        use bqlite_core::property::BqlType;
        use bqlite_core::schema::{ColumnDef, TableSchema};
        let mut db = Database::create(path).expect("create db");
        let schema = TableSchema::new(
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
        .expect("schema");
        db.create_table("events".into(), schema)
            .expect("create table");
        db
    }

    fn write_segment(db: &mut Database, shard: u16, window: u32, count: u64) {
        let events: Vec<Event> = (0..count)
            .map(|i| {
                Event::new(
                    EntityId::from(format!("e{shard}_{i}").as_str()),
                    Timestamp((i as i64 + 1) * 1_000_000),
                    "click",
                )
            })
            .collect();
        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::new(db);
        writer
            .write_bucket("events", window, shard, batch_id, &events)
            .expect("write_bucket");
    }

    #[test]
    fn enumerate_returns_one_snapshot_per_populated_shard() {
        let path = test_dir("enum-populated");
        let mut db = create_events_db(&path);
        write_segment(&mut db, 0, 0, 3);
        write_segment(&mut db, 2, 0, 5);
        write_segment(&mut db, 5, 1, 2);

        let snaps = enumerate_shard_snapshots(&db, "events", TimeRange::unbounded()).unwrap();
        let mut shards: Vec<u32> = snaps.iter().map(|s| s.shard_id).collect();
        shards.sort();
        assert_eq!(shards, vec![0, 2, 5]);
        for s in &snaps {
            assert!(s.windows.iter().any(|w| !w.segments.is_empty()));
        }
        // Shard 2's snapshot must hold its 5-row segment.
        let s2 = snaps.iter().find(|s| s.shard_id == 2).unwrap();
        let total: u64 = s2.estimated_rows();
        assert_eq!(total, 5);

        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn enumerate_empty_table_yields_empty_vec() {
        let path = test_dir("enum-empty");
        let db = create_events_db(&path);
        let snaps = enumerate_shard_snapshots(&db, "events", TimeRange::unbounded()).unwrap();
        assert!(snaps.is_empty());
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn enumerate_unknown_table_errors() {
        let path = test_dir("enum-unknown");
        let db = create_events_db(&path);
        let err = enumerate_shard_snapshots(&db, "nope", TimeRange::unbounded()).err();
        assert!(err.is_some());
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn enumerate_groups_segments_by_window_within_shard() {
        let path = test_dir("enum-windows");
        let mut db = create_events_db(&path);
        write_segment(&mut db, 0, 0, 1);
        write_segment(&mut db, 0, 0, 2);
        write_segment(&mut db, 0, 1, 3);

        let snaps = enumerate_shard_snapshots(&db, "events", TimeRange::unbounded()).unwrap();
        assert_eq!(snaps.len(), 1);
        let s = &snaps[0];
        assert_eq!(s.shard_id, 0);
        // Two window groups (window_id 0 and 1).
        assert_eq!(s.windows.len(), 2);
        let win0 = s.windows.iter().find(|w| w.window_id == 0).unwrap();
        // Two segments in window 0.
        assert_eq!(win0.segments.len(), 2);
        let win1 = s.windows.iter().find(|w| w.window_id == 1).unwrap();
        assert_eq!(win1.segments.len(), 1);

        let _ = std::fs::remove_dir_all(&path);
    }
}
