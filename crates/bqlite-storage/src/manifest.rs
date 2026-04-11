//! Database manifest — the durable catalog and segment inventory.
//!
//! # Scope
//!
//! The full design in [`docs/design/storage-format.md`](../../../docs/design/storage-format.md)
//! §5.2 + §12.1 splits database-wide state (`db.json`) from per-table
//! manifests (`<table>/manifest.json`). Wave 1 (TASK-116) consolidated
//! both into a single `<db_root>/manifest.json` because no data was
//! being written yet. Wave 2 (TASK-217) keeps the consolidation — the
//! per-table split is a later wave — but extends [`TableEntry`] with
//! the full per-`(window, shard)` segment inventory from
//! storage-format.md §12.3 so that real ingest can land segments in
//! the manifest. When the per-table split eventually happens,
//! [`MANIFEST_FORMAT_VERSION`] is bumped and the open path learns to
//! read either shape.
//!
//! # What this module owns
//!
//! - The [`Manifest`] struct and its nested shapes:
//!   - [`TableEntry`] — per-table state (schema, ingest counters, and
//!     the active segment inventory).
//!   - [`WindowManifest`] — one time window's segments, bucketed by
//!     shard. Outer vec index is the shard id.
//!   - [`SegmentMeta`] — full per-segment metadata matching
//!     storage-format.md §12.3 `SegmentMeta`.
//!   - [`ColumnStats`] — per-column zone-map entries carried on
//!     every [`SegmentMeta`].
//! - Format-level constants: [`MANIFEST_FORMAT_VERSION`] and
//!   [`DEFAULT_SHARD_COUNT`].
//! - Constructors that freshly initialize an empty database:
//!   [`Manifest::new_empty`] stamps a random v4 UUID per
//!   storage-format.md §5.1 so later waves can use it as the SAMPLE
//!   seed and as an external identity handle.
//! - Data-only mutation helpers on [`Manifest`]:
//!   [`Manifest::add_segment`], [`Manifest::remove_segment`], and
//!   [`Manifest::snapshot_for_query`]. These are pure in-memory
//!   transformations; atomic disk persistence lives in the
//!   [`crate::database`] module.
//!
//! Atomic file I/O (`manifest.json.tmp` → `fsync` → `rename`) and the
//! exclusive `.lock` flock that serializes writers across processes
//! live in [`crate::database`], not here. Wave 2 wires the atomic
//! writer to an `update_manifest` closure helper that runs the
//! in-memory mutation and then re-persists the resulting manifest.
//!
//! # Serialization
//!
//! JSON via `serde_json`. [`BTreeMap`] (not `HashMap`) is the backing
//! container for `tables` so the serialized form is deterministic,
//! making corrupted-manifest error messages and snapshot-style tests
//! stable across runs. Within a table, segments are stored in
//! insertion order inside each `(window_id, shard_id)` cell —
//! insertion order equals monotonic `segment_id` order because ingest
//! only ever appends, which keeps the serialized form byte-stable.
//!
//! # Backward compatibility
//!
//! Wave 1 manifests serialized a top-level `segments: []` placeholder.
//! That field was removed in Wave 2; because serde ignores unknown
//! fields by default, Wave 2 still reads Wave 1 manifests cleanly.
//! TableEntry's new `windows` field is `#[serde(default)]`, so a Wave 1
//! table entry without windows deserializes with an empty inventory —
//! exactly what it would have had under Wave 1.

use std::collections::BTreeMap;

use bqlite_core::property::PropertyValue;
use bqlite_core::schema::TableSchema;
use serde::{Deserialize, Serialize};

/// Current manifest format version.
///
/// Bumped whenever the on-disk shape of [`Manifest`] changes in a way
/// that old readers cannot handle. Every stored manifest records the
/// version it was written with; mismatches fail the open path with a
/// clear error in [`crate::database`].
///
/// Wave 2 extends [`TableEntry`] with a new `windows` field but keeps
/// `format_version` at `1` because the extension is strictly additive:
/// Wave 1 readers that try to open a Wave 2 manifest would simply
/// ignore the unknown field. The version bump is reserved for changes
/// that old readers cannot safely ignore.
///
/// See `docs/reliability.md` (Versioning) — "version everything that
/// can change in the future and cause backwards-compatibility issues".
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Default shard count for newly initialized databases.
///
/// Matches the core count of modern hardware so that one shard-task per
/// core keeps all cores busy during query execution — see
/// `docs/design/storage-format.md` §5.1 and `docs/design/execution-model.md`
/// §9.3. The CLI `bqlite init --shards N` override (a later-wave task)
/// will flow through a `shard_count` argument on the follow-up
/// `Database` bootstrap API.
pub const DEFAULT_SHARD_COUNT: u16 = 32;

/// The root manifest, serialized as `<db_root>/manifest.json`.
///
/// Wave 1 froze the top-level shape (`format_version`, `database_uuid`,
/// `shard_count`, `tables`); Wave 2 extends [`TableEntry`] with the
/// per-`(window, shard)` segment inventory — see the module docs.
///
/// `Eq` is **not** derived because [`TableEntry::windows`] eventually
/// reaches [`PropertyValue::Float`] through [`ColumnStats`], and `f64`
/// is `PartialEq` but not `Eq`. Round-trip tests use `PartialEq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    /// Format version of this manifest file. Always equals
    /// [`MANIFEST_FORMAT_VERSION`] for manifests written by this crate;
    /// the open path rejects any other value.
    pub format_version: u32,

    /// Stable, randomly generated UUIDv4 stamped at init. Never
    /// rotates — `docs/design/storage-format.md` §5.1 promises this
    /// as the SAMPLE-determinism seed and external identity handle.
    /// Stored as its hyphenated string form so the manifest is
    /// human-readable.
    pub database_uuid: String,

    /// Number of hash shards every table in this database uses. Fixed
    /// at init time per `docs/design/storage-format.md` §5.1 — cross-
    /// table joins rely on a single shard count database-wide.
    pub shard_count: u16,

    /// Declared tables, keyed by table name. Each entry owns its own
    /// segment inventory via [`TableEntry::windows`]. Wave 1 starts
    /// this map empty; TASK-125 seeded a bootstrap `events` entry.
    #[serde(default)]
    pub tables: BTreeMap<String, TableEntry>,
}

/// Per-table state tracked in the shared Wave 1/2 manifest.
///
/// Wave 1 TASK-116 defined `schema`, `next_sequence_id`, `next_batch_id`,
/// and `bootstrap_events_table`. Wave 2 TASK-217 adds `windows` — the
/// per-`(window, shard)` active segment inventory from
/// storage-format.md §12.3.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableEntry {
    /// Authoritative current schema for this table.
    pub schema: TableSchema,

    /// Next sequence ID to assign on ingest (persisted for monotonicity
    /// across restarts — `docs/design/storage-format.md` §6.2 + §12.3).
    #[serde(default)]
    pub next_sequence_id: u64,

    /// Next batch ID to assign on ingest (persisted for monotonicity).
    #[serde(default)]
    pub next_batch_id: u64,

    /// `true` when this entry was seeded by the TASK-125 default-table
    /// bootstrap rather than user DDL. Later waves can retire the
    /// shortcut cleanly by scanning for this flag; Wave 1 writes it
    /// unconditionally so no manifest ever ends up in an unlabeled
    /// state.
    #[serde(default)]
    pub bootstrap_events_table: bool,

    /// Active segment inventory, bucketed by time window. Sorted by
    /// `window_id` ascending so serialized output is deterministic and
    /// snapshot-for-query can iterate in window order without a
    /// separate sort pass. Empty on fresh init; populated by
    /// [`Manifest::add_segment`] as ingest writes new segments.
    ///
    /// `#[serde(default)]` lets Wave 1 manifests (which never wrote
    /// this field) deserialize cleanly into a Wave 2 reader.
    #[serde(default)]
    pub windows: Vec<WindowManifest>,
}

/// Active segments for a single time window, indexed by shard.
///
/// `shards[shard_id]` holds the ordered list of active segments for
/// that shard in this window. `shards.len()` always equals the owning
/// manifest's `shard_count` at the moment the window was created;
/// empty shards carry an empty `Vec<SegmentMeta>`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowManifest {
    /// Window identifier — days since epoch for the window start,
    /// per storage-format.md §4.1 + §5.2.
    pub window_id: u32,

    /// Active segments per shard. Outer index = shard_id.
    pub shards: Vec<Vec<SegmentMeta>>,
}

/// Full per-segment metadata, matching storage-format.md §12.3.
///
/// Every field the segment writer needs to record for pruning,
/// compaction scheduling, and backfill. Zone maps are embedded inline
/// (see [`ColumnStats`]) so the planner can prune at segment
/// granularity without opening any segment files — storage-format.md
/// §12.3 "Why zone maps inline".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SegmentMeta {
    /// Unique segment identifier, assigned from the manifest's
    /// monotonic segment counter. Also forms the segment filename
    /// (`segment_<segment_id>.seg`, storage-format.md §5.2).
    pub segment_id: u64,

    /// Compaction level — `0` for freshly ingested L0 segments,
    /// higher for outputs of successive compaction passes. The level
    /// lives in the manifest, not the filename, because it can change
    /// across compaction cycles (storage-format.md §5.2).
    pub level: u8,

    /// Schema version this segment was written against. Used by the
    /// scan layer to fill columns added by later `ALTER TABLE ADD
    /// COLUMN` with NULL/DEFAULT values without rewriting old
    /// segments (storage-format.md §6.4).
    pub schema_version: u32,

    /// Total rows in the segment across every row group.
    pub row_count: u64,

    /// Segment file size in bytes.
    pub byte_size: u64,

    /// `(min_ts, max_ts)` — inclusive-inclusive timestamp range
    /// across every event in this segment. Raw `i64` nanoseconds so
    /// the on-disk shape exactly matches the documented §12.3
    /// `SegmentMeta.ts_range`; planner-side overlap checks lift to
    /// [`TimeRange`] internally.
    pub ts_range: (i64, i64),

    /// `(min_entity, max_entity)` — inclusive-inclusive entity-id
    /// range across every row in this segment, used for entity zone
    /// map pruning (storage-format.md §11.1). Strings compare
    /// lexicographically. Represented as [`PropertyValue`] so the
    /// same type carries through every zone-map surface.
    pub entity_range: (PropertyValue, PropertyValue),

    /// Per-column zone maps. One entry per column declared in the
    /// segment's schema at the time of writing, in the same order as
    /// that schema's column list — the writer lines the vec up with
    /// the segment's column order so the planner's pruning pass can
    /// index by position when it already knows the schema. Columns
    /// added afterwards are filled by the scan backfill path, not by
    /// new entries here.
    pub column_stats: Vec<ColumnStats>,

    /// Segment creation time, epoch nanoseconds UTC. Used by the
    /// compaction scheduler to pick older segments first.
    pub created_at: i64,

    /// Ingest batch that produced this segment. One ingest call =
    /// one batch ID (storage-format.md §6.2); carried through so
    /// batch-level deletes and debugging can trace rows back to the
    /// originating call.
    pub batch_id: u64,
}

/// Per-column statistics carried on every [`SegmentMeta`].
///
/// `min`/`max` are `None` exactly when the row-group is all-null for
/// the column; otherwise they hold the exact value extremes across
/// every non-null row. `distinct_count_estimate` is an optional
/// HyperLogLog approximation from ingest — `None` means the writer
/// did not produce one, not "zero distinct values".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnStats {
    /// Column name this entry describes.
    pub column_name: String,

    /// Minimum non-null value in the segment for this column, or
    /// `None` if every row in the segment is null.
    pub min: Option<PropertyValue>,

    /// Maximum non-null value in the segment for this column, or
    /// `None` if every row in the segment is null.
    pub max: Option<PropertyValue>,

    /// Number of null values in the segment for this column.
    pub null_count: u64,

    /// Optional HyperLogLog estimate of distinct cardinality from
    /// ingest. `None` means the writer did not capture one.
    #[serde(default)]
    pub distinct_count_estimate: Option<u64>,
}

impl WindowManifest {
    /// Construct a fresh window with `shard_count` empty shards.
    ///
    /// `shard_count` must match the owning [`Manifest::shard_count`]
    /// so that `shards[shard_id]` lookups are total.
    pub fn new(window_id: u32, shard_count: u16) -> Self {
        Self {
            window_id,
            shards: vec![Vec::new(); shard_count as usize],
        }
    }

    /// True when every shard in this window has zero active segments.
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(Vec::is_empty)
    }
}

impl Manifest {
    /// Build a fresh, empty manifest ready to be written to disk.
    ///
    /// Generates a random v4 UUID for `database_uuid`, sets
    /// `format_version` to [`MANIFEST_FORMAT_VERSION`], uses the
    /// supplied `shard_count`, and leaves `tables` empty.
    ///
    /// Callers should prefer [`Manifest::new_empty_default`] unless
    /// they are honouring a CLI `--shards` override.
    pub fn new_empty(shard_count: u16) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            database_uuid: uuid::Uuid::new_v4().to_string(),
            shard_count,
            tables: BTreeMap::new(),
        }
    }

    /// Build a fresh, empty manifest with the default shard count.
    pub fn new_empty_default() -> Self {
        Self::new_empty(DEFAULT_SHARD_COUNT)
    }

    /// True if the manifest's `format_version` matches the version
    /// this build of the crate knows how to read. The open path uses
    /// this check before handing the manifest out to the rest of the
    /// engine.
    pub fn is_supported_version(&self) -> bool {
        self.format_version == MANIFEST_FORMAT_VERSION
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bqlite_core::property::BqlType;
    use bqlite_core::schema::{ColumnDef, TableSchema};

    fn sample_events_schema() -> TableSchema {
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
        .expect("minimal schema must pass §5.1 validation")
    }

    /// Shared sample SegmentMeta for tests. Deliberately exercises
    /// every non-trivial field (per-column stats, entity_range, a
    /// non-default `level`) so round-trip tests catch regressions on
    /// any one of them.
    fn sample_segment(segment_id: u64, batch_id: u64, ts_range: (i64, i64)) -> SegmentMeta {
        SegmentMeta {
            segment_id,
            level: 0,
            schema_version: 1,
            row_count: 100,
            byte_size: 4096,
            ts_range,
            entity_range: (
                PropertyValue::String("alice".into()),
                PropertyValue::String("zebra".into()),
            ),
            column_stats: vec![
                ColumnStats {
                    column_name: "entity_id".into(),
                    min: Some(PropertyValue::String("alice".into())),
                    max: Some(PropertyValue::String("zebra".into())),
                    null_count: 0,
                    distinct_count_estimate: Some(42),
                },
                ColumnStats {
                    column_name: "event_type".into(),
                    min: Some(PropertyValue::String("click".into())),
                    max: Some(PropertyValue::String("view".into())),
                    null_count: 0,
                    distinct_count_estimate: None,
                },
            ],
            created_at: 1_700_000_000_000_000_000,
            batch_id,
        }
    }

    // ── Manifest::new_empty ─────────────────────────────────────────────────

    #[test]
    fn new_empty_uses_requested_shard_count() {
        let m = Manifest::new_empty(7);
        assert_eq!(m.format_version, MANIFEST_FORMAT_VERSION);
        assert_eq!(m.shard_count, 7);
        assert!(m.tables.is_empty());
    }

    #[test]
    fn new_empty_default_uses_default_shard_count() {
        let m = Manifest::new_empty_default();
        assert_eq!(m.shard_count, DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn new_empty_generates_distinct_uuids() {
        let a = Manifest::new_empty_default();
        let b = Manifest::new_empty_default();
        assert_ne!(
            a.database_uuid, b.database_uuid,
            "two fresh manifests should stamp different UUIDs"
        );
        // Hyphenated UUIDv4 string form — 36 chars including dashes.
        assert_eq!(a.database_uuid.len(), 36);
        assert_eq!(a.database_uuid.matches('-').count(), 4);
    }

    #[test]
    fn is_supported_version_rejects_unknown_version() {
        let mut m = Manifest::new_empty_default();
        m.format_version = MANIFEST_FORMAT_VERSION + 1;
        assert!(!m.is_supported_version());
    }

    // ── JSON round-trip ─────────────────────────────────────────────────────

    #[test]
    fn empty_manifest_roundtrips_through_json() {
        let before = Manifest::new_empty_default();
        let bytes = serde_json::to_vec_pretty(&before).expect("serialize");
        let after: Manifest = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(before, after);
    }

    #[test]
    fn manifest_with_table_roundtrips_through_json() {
        let mut before = Manifest::new_empty_default();
        before.tables.insert(
            "events".to_string(),
            TableEntry {
                schema: sample_events_schema(),
                next_sequence_id: 0,
                next_batch_id: 0,
                bootstrap_events_table: true,
                windows: Vec::new(),
            },
        );
        let bytes = serde_json::to_vec_pretty(&before).expect("serialize");
        let after: Manifest = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(before, after);
    }

    #[test]
    fn manifest_with_segment_inventory_roundtrips_through_json() {
        // Full round-trip including SegmentMeta / ColumnStats so any
        // future serde tweak that breaks on one of the nested types is
        // caught here rather than in a downstream Wave 2 ingest test.
        let mut before = Manifest::new_empty(4);
        let mut win = WindowManifest::new(7, 4);
        win.shards[2].push(sample_segment(1, 100, (1_000, 2_000)));
        win.shards[2].push(sample_segment(2, 100, (2_000, 3_000)));
        before.tables.insert(
            "events".to_string(),
            TableEntry {
                schema: sample_events_schema(),
                next_sequence_id: 17,
                next_batch_id: 3,
                bootstrap_events_table: false,
                windows: vec![win],
            },
        );

        let bytes = serde_json::to_vec_pretty(&before).expect("serialize");
        let after: Manifest = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(before, after);
    }

    #[test]
    fn manifest_json_contains_documented_fields() {
        let m = Manifest::new_empty(13);
        let json = serde_json::to_value(&m).expect("serialize");
        // The four top-level fields the Wave 2 scaffold ships.
        assert!(json.get("format_version").is_some(), "format_version field");
        assert!(json.get("database_uuid").is_some(), "database_uuid field");
        assert!(json.get("shard_count").is_some(), "shard_count field");
        assert!(json.get("tables").is_some(), "tables field");
        assert_eq!(json["format_version"], 1);
        assert_eq!(json["shard_count"], 13);
        assert!(
            json["tables"].as_object().unwrap().is_empty(),
            "tables map starts empty on a fresh manifest"
        );
    }

    // ── BTreeMap ordering ───────────────────────────────────────────────────

    #[test]
    fn tables_serialize_deterministically_regardless_of_insertion_order() {
        // Determinism matters for corrupted-manifest error messages and
        // snapshot-style tests. BTreeMap gives us sorted-by-key order
        // for free — two manifests with the same (uuid, tables) but
        // different insertion orders must serialize byte-for-byte
        // identically.
        let make = |order: &[&str]| {
            // Hold `uuid` and `shard_count` constant across both
            // manifests so any byte-level difference is attributable
            // to `tables` ordering alone.
            let mut m = Manifest {
                format_version: MANIFEST_FORMAT_VERSION,
                database_uuid: "00000000-0000-4000-8000-000000000000".to_string(),
                shard_count: DEFAULT_SHARD_COUNT,
                tables: BTreeMap::new(),
            };
            for name in order {
                m.tables.insert(
                    (*name).to_string(),
                    TableEntry {
                        schema: sample_events_schema(),
                        next_sequence_id: 0,
                        next_batch_id: 0,
                        bootstrap_events_table: false,
                        windows: Vec::new(),
                    },
                );
            }
            m
        };
        let a = make(&["pages", "events", "signups"]);
        let b = make(&["signups", "pages", "events"]);
        let a_bytes = serde_json::to_vec(&a).expect("serialize a");
        let b_bytes = serde_json::to_vec(&b).expect("serialize b");
        assert_eq!(
            a_bytes, b_bytes,
            "tables must serialize in BTreeMap order regardless of insertion order",
        );
    }

    // ── Backward-compat / forward-compat safety ─────────────────────────────

    #[test]
    fn missing_optional_fields_default_to_empty() {
        // A minimal hand-written manifest missing `tables` must still
        // deserialize. This is the compat surface for test fixtures
        // and for manifests that pre-date Wave 2's field additions.
        let json = format!(
            r#"{{"format_version":{MANIFEST_FORMAT_VERSION},"database_uuid":"00000000-0000-4000-8000-000000000000","shard_count":32}}"#
        );
        let m: Manifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m.shard_count, 32);
        assert!(m.tables.is_empty());
    }

    #[test]
    fn wave1_manifest_with_stale_segments_placeholder_still_deserializes() {
        // Wave 1 (TASK-116) serialized a top-level `segments: []`
        // placeholder that Wave 2 (TASK-217) removed. Serde's default
        // "ignore unknown fields" behaviour means Wave 1 manifests
        // still load — this test locks that contract in so nobody
        // accidentally turns on `deny_unknown_fields` and breaks
        // on-disk compatibility.
        let json = format!(
            r#"{{"format_version":{MANIFEST_FORMAT_VERSION},"database_uuid":"00000000-0000-4000-8000-000000000000","shard_count":32,"tables":{{}},"segments":[]}}"#
        );
        let m: Manifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m.shard_count, 32);
        assert!(m.tables.is_empty());
    }

    #[test]
    fn wave1_table_entry_without_windows_defaults_to_empty_inventory() {
        // Wave 1 TableEntry serialized without a `windows` field.
        // Wave 2 reads those manifests by defaulting `windows` to an
        // empty vec — a just-migrated database therefore starts Wave 2
        // with an empty segment inventory, which is correct because
        // Wave 1 never wrote real segments.
        //
        // We build the Wave 1 shape by serializing a Wave 2 manifest
        // and stripping `windows` from the JSON, rather than
        // hand-typing the full TableSchema serde shape (which is
        // annotated on private fields and would couple this test to
        // schema.rs internals).
        let mut wave2 = Manifest::new_empty_default();
        wave2.tables.insert(
            "events".to_string(),
            TableEntry {
                schema: sample_events_schema(),
                next_sequence_id: 5,
                next_batch_id: 2,
                bootstrap_events_table: true,
                windows: Vec::new(),
            },
        );
        let mut value = serde_json::to_value(&wave2).expect("serialize");
        // Remove the `windows` field from the events entry so the
        // JSON matches a Wave 1 manifest exactly.
        let entry = value["tables"]["events"]
            .as_object_mut()
            .expect("events entry");
        entry.remove("windows");

        let m: Manifest = serde_json::from_value(value).expect("deserialize");
        let entry = m.tables.get("events").expect("events entry present");
        assert_eq!(entry.next_sequence_id, 5);
        assert_eq!(entry.next_batch_id, 2);
        assert!(entry.bootstrap_events_table);
        assert!(
            entry.windows.is_empty(),
            "missing windows field deserializes into an empty inventory"
        );
    }

    // ── WindowManifest ──────────────────────────────────────────────────────

    #[test]
    fn new_window_allocates_one_empty_shard_vec_per_shard() {
        let w = WindowManifest::new(3, 4);
        assert_eq!(w.window_id, 3);
        assert_eq!(w.shards.len(), 4);
        assert!(w.shards.iter().all(Vec::is_empty));
        assert!(w.is_empty());
    }

    #[test]
    fn window_with_any_segment_is_not_empty() {
        let mut w = WindowManifest::new(3, 4);
        w.shards[1].push(sample_segment(1, 1, (0, 1)));
        assert!(!w.is_empty());
    }

    #[test]
    fn all_null_column_stats_roundtrips_through_json() {
        // The `min`/`max: Option<PropertyValue>` fields are explicitly
        // documented as `None` when every row is null. Exercise that
        // boundary through a full manifest round-trip — if a future
        // serde tweak accidentally rejects `None` here, the segment
        // writer for an entirely-null column would fail to persist.
        let mut seg = sample_segment(99, 1, (0, 10));
        seg.column_stats.push(ColumnStats {
            column_name: "all_null_extra".into(),
            min: None,
            max: None,
            null_count: seg.row_count,
            distinct_count_estimate: None,
        });

        let mut manifest = Manifest::new_empty(2);
        let mut win = WindowManifest::new(0, 2);
        win.shards[0].push(seg);
        manifest.tables.insert(
            "events".to_string(),
            TableEntry {
                schema: sample_events_schema(),
                next_sequence_id: 0,
                next_batch_id: 0,
                bootstrap_events_table: false,
                windows: vec![win],
            },
        );

        let bytes = serde_json::to_vec(&manifest).expect("serialize");
        let round: Manifest = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(round, manifest);
    }
}
