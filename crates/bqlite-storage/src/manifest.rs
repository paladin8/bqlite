//! Database manifest — the on-disk source of truth for the Wave 1
//! storage stub.
//!
//! # Scope
//!
//! The full design in [`docs/design/storage-format.md`](../../../docs/design/storage-format.md)
//! §5.2 + §12.1 splits database-wide state (`db.json`) from per-table
//! manifests (`<table>/manifest.json`). Wave 1 consolidates both into a
//! single `manifest.json` at the database root because no data is being
//! written yet, and a single file keeps the `open_or_create` contract
//! easy to reason about. TASK-125 will seed a bootstrap `events` table
//! entry into the shared `tables` map; later waves may split this into
//! per-table manifests, at which point [`MANIFEST_FORMAT_VERSION`] is
//! bumped and the open path learns to read either shape.
//!
//! # What this module owns
//!
//! - The [`Manifest`] struct and its nested [`TableEntry`] — the data
//!   shape the rest of the storage crate reads and writes.
//! - Format-level constants: [`MANIFEST_FORMAT_VERSION`] and
//!   [`DEFAULT_SHARD_COUNT`], the two knobs the Wave 1 init path needs.
//! - Constructors that freshly initialize an empty database:
//!   [`Manifest::new_empty`] stamps a random v4 UUID per
//!   storage-format.md §5.1 so later waves can use it as the SAMPLE seed
//!   and as an external identity handle.
//!
//! Atomic file I/O (`manifest.json.tmp` → `fsync` → `rename`) lives
//! in a follow-up `database` module in this crate, not here — this
//! module is pure data.
//!
//! # Serialization
//!
//! JSON via `serde_json`. [`BTreeMap`] (not `HashMap`) is the backing
//! container for `tables` so the serialized form is deterministic,
//! making corrupted-manifest error messages and snapshot-style tests
//! stable across runs. The cost is `O(log n)` lookup instead of `O(1)`,
//! which does not matter for a manifest that holds at most a handful of
//! tables in v1.

use std::collections::BTreeMap;

use bqlite_core::schema::TableSchema;
use serde::{Deserialize, Serialize};

/// Current manifest format version.
///
/// Bumped whenever the on-disk shape of [`Manifest`] changes in a way
/// that old readers cannot handle. Every stored manifest records the
/// version it was written with; mismatches fail the open path with a
/// clear error in the follow-up `database` module checkpoint.
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
/// Wave 1 holds exactly the fields TASK-116 specifies; future-wave
/// additions (per-window segment inventories, zone maps, tombstone
/// pointers, compaction state) layer on top without removing anything,
/// so this is the on-disk freeze point for v1.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

    /// Declared tables, keyed by table name. Wave 1 leaves this empty;
    /// TASK-125 seeds a bootstrap `events` entry so
    /// `bqlite query "events"` can parse-plan-execute against a fresh
    /// database.
    #[serde(default)]
    pub tables: BTreeMap<String, TableEntry>,

    /// Segment inventory placeholder. Wave 1 never writes here; later
    /// waves evolve this into the per-`(window, shard)` layout from
    /// `docs/design/storage-format.md` §12.3 `WindowManifest`.
    #[serde(default)]
    pub segments: Vec<SegmentEntry>,
}

/// Per-table state tracked in the shared Wave 1 manifest.
///
/// The field names match the `next_sequence_id` / `next_batch_id`
/// spelling in `docs/design/storage-format.md` §12.3 so that when the
/// manifest splits per-table in a later wave, only the container
/// changes and the field-level schema is stable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
}

/// Placeholder segment inventory entry — reserved so the Wave 1
/// manifest's JSON shape already has a stable `segments` array. No
/// fields yet; extended alongside the real segment format.
///
/// Serialized as an empty JSON object `{}`, which deserializes
/// unchanged and will still deserialize after later waves add fields
/// with `#[serde(default)]`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentEntry {}

impl Manifest {
    /// Build a fresh, empty manifest ready to be written to disk.
    ///
    /// Generates a random v4 UUID for `database_uuid`, sets
    /// `format_version` to [`MANIFEST_FORMAT_VERSION`], uses the
    /// supplied `shard_count`, and leaves `tables` / `segments` empty.
    ///
    /// Callers should prefer [`Manifest::new_empty_default`] unless
    /// they are honouring a CLI `--shards` override.
    pub fn new_empty(shard_count: u16) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            database_uuid: uuid::Uuid::new_v4().to_string(),
            shard_count,
            tables: BTreeMap::new(),
            segments: Vec::new(),
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

    // ── Manifest::new_empty ─────────────────────────────────────────────────

    #[test]
    fn new_empty_uses_requested_shard_count() {
        let m = Manifest::new_empty(7);
        assert_eq!(m.format_version, MANIFEST_FORMAT_VERSION);
        assert_eq!(m.shard_count, 7);
        assert!(m.tables.is_empty());
        assert!(m.segments.is_empty());
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
        // The five fields TASK-116 specifies.
        assert!(json.get("format_version").is_some(), "format_version field");
        assert!(json.get("database_uuid").is_some(), "database_uuid field");
        assert!(json.get("shard_count").is_some(), "shard_count field");
        assert!(json.get("tables").is_some(), "tables field");
        assert!(json.get("segments").is_some(), "segments field");
        assert_eq!(json["format_version"], 1);
        assert_eq!(json["shard_count"], 13);
        assert!(
            json["tables"].as_object().unwrap().is_empty(),
            "tables map starts empty in Wave 1"
        );
        assert!(
            json["segments"].as_array().unwrap().is_empty(),
            "segments array starts empty in Wave 1"
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
                segments: Vec::new(),
            };
            for name in order {
                m.tables.insert(
                    (*name).to_string(),
                    TableEntry {
                        schema: sample_events_schema(),
                        next_sequence_id: 0,
                        next_batch_id: 0,
                        bootstrap_events_table: false,
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
    fn missing_tables_and_segments_default_to_empty() {
        // A hand-written old-style manifest missing `tables` and
        // `segments` should still deserialize, filling empty defaults.
        // This gives us forward compatibility for minimal test
        // manifests.
        let json = format!(
            r#"{{"format_version":{MANIFEST_FORMAT_VERSION},"database_uuid":"00000000-0000-4000-8000-000000000000","shard_count":32}}"#
        );
        let m: Manifest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(m.shard_count, 32);
        assert!(m.tables.is_empty());
        assert!(m.segments.is_empty());
    }
}
