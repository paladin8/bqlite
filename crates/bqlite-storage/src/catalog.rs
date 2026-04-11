//! Manifest-backed [`Catalog`] implementation and the Wave 1 bootstrap
//! events table schema.
//!
//! # Where the bootstrap rule comes from
//!
//! TASK-125 resolves a gap between two constraints:
//!
//! 1. `docs/design/planner-pipeline.md` §4.1 requires the planner to
//!    hold a `Catalog` handle and raise an error for unknown tables.
//! 2. `docs/design/query-language.md` §29 forbids BQL DDL in v0 —
//!    there is no `CREATE TABLE` statement, only CLI initialization.
//!
//! Without a seeded table, `bqlite query "events"` cannot parse,
//! plan, and execute against a freshly-created database (the Wave 1
//! smoke test in TASK-123). The resolution is to have
//! [`crate::database::Database::open_or_create`] seed a single
//! default `events` table in the manifest on a fresh init. The
//! schema is the minimum required by `docs/design/type-system.md`
//! §5.1 — exactly three mandatory columns, each non-nullable:
//!
//! - `entity_id STRING NOT NULL` (entity key role)
//! - `ts TIMESTAMP NOT NULL` (event time role)
//! - `event_type STRING NOT NULL` (event type role)
//!
//! The seeded entry carries `bootstrap_events_table: true` so later
//! waves can retire the shortcut cleanly once `CREATE TABLE` DDL
//! lands (`docs/design/query-language.md` §29, a later-wave task).

use bqlite_core::catalog::{unknown_table_error, Catalog};
use bqlite_core::error::Result;
use bqlite_core::property::BqlType;
use bqlite_core::schema::{ColumnDef, TableSchema};

use crate::manifest::Manifest;

/// Name of the bootstrap events table seeded by TASK-125.
///
/// Exposed as a constant so tests and later-wave migration code can
/// reference the canonical name without repeating the string literal.
pub const BOOTSTRAP_EVENTS_TABLE_NAME: &str = "events";

/// Build the schema for the bootstrap events table.
///
/// Returns a fresh `TableSchema` each call so callers may mutate or
/// store it without coupling to a shared `OnceCell`. The construction
/// goes through `TableSchema::new`, so §5.1 validation runs every
/// call — if the rules ever tighten, this function fails fast at the
/// call site instead of returning a stale precomputed value.
///
/// # Panics
///
/// Panics if the hand-crafted schema fails §5.1 validation. This is
/// a load-bearing invariant: the bootstrap table is the one schema
/// the database absolutely must be able to construct, and a failure
/// here indicates an internal bug in either this function or
/// `TableSchema::new`.
pub fn bootstrap_events_schema() -> TableSchema {
    TableSchema::new(
        BOOTSTRAP_EVENTS_TABLE_NAME,
        vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ],
        "entity_id",
        "ts",
        "event_type",
    )
    .expect("bootstrap events schema must pass type-system.md §5.1 validation")
}

/// A [`Catalog`] backed by a borrowed reference to an in-memory
/// [`Manifest`].
///
/// Cheap to construct — holds only a reference. The lifetime is tied
/// to the manifest's, so callers typically build one per plan or per
/// query and drop it before the next manifest update. Later waves
/// that extend `Database` with a mutation API will need to snapshot
/// the manifest (via `Arc`) before handing out a catalog; Wave 1 is
/// read-only so the borrow is sufficient.
///
/// Implements [`std::fmt::Debug`] via `derive` so it satisfies the
/// `Catalog: Send + Sync + Debug` trait bound.
#[derive(Debug)]
pub struct ManifestCatalog<'m> {
    manifest: &'m Manifest,
}

impl<'m> ManifestCatalog<'m> {
    /// Wrap a borrowed manifest in a `Catalog` view.
    pub fn new(manifest: &'m Manifest) -> Self {
        Self { manifest }
    }

    /// Access the underlying manifest. Useful for diagnostics and
    /// tests that need to assert on non-catalog manifest state (e.g.
    /// checking the `bootstrap_events_table` flag on a seeded entry).
    pub fn manifest(&self) -> &'m Manifest {
        self.manifest
    }
}

impl Catalog for ManifestCatalog<'_> {
    fn resolve_table(&self, name: &str) -> Result<TableSchema> {
        self.manifest
            .tables
            .get(name)
            .map(|entry| entry.schema.clone())
            .ok_or_else(|| unknown_table_error(name))
    }

    fn list_tables(&self) -> Vec<&str> {
        // BTreeMap iteration is sorted, which is a stronger contract
        // than the trait's "stable order" requirement. Callers that
        // rely on sorted output are relying on an implementation
        // detail, not the trait — the sort is still free.
        self.manifest.tables.keys().map(String::as_str).collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use bqlite_core::error::BqliteError;

    use crate::manifest::{Manifest, TableEntry, DEFAULT_SHARD_COUNT};

    // ── bootstrap_events_schema ─────────────────────────────────────────────

    #[test]
    fn bootstrap_schema_matches_task_125_contract() {
        let schema = bootstrap_events_schema();
        assert_eq!(schema.name(), BOOTSTRAP_EVENTS_TABLE_NAME);
        assert_eq!(schema.columns().len(), 3);

        let entity_key = schema.entity_key_column();
        assert_eq!(entity_key.name, "entity_id");
        assert_eq!(entity_key.bql_type, BqlType::String);
        assert!(!entity_key.nullable, "entity_id must be NOT NULL");

        let ts = schema.timestamp_column();
        assert_eq!(ts.name, "ts");
        assert_eq!(ts.bql_type, BqlType::Timestamp);
        assert!(!ts.nullable, "ts must be NOT NULL");

        let et = schema.event_type_column();
        assert_eq!(et.name, "event_type");
        assert_eq!(et.bql_type, BqlType::String);
        assert!(!et.nullable, "event_type must be NOT NULL");
    }

    #[test]
    fn bootstrap_schema_is_stable_across_calls() {
        let a = bootstrap_events_schema();
        let b = bootstrap_events_schema();
        assert_eq!(a, b);
    }

    // ── ManifestCatalog ─────────────────────────────────────────────────────

    fn seeded_manifest() -> Manifest {
        let mut m = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        m.tables.insert(
            BOOTSTRAP_EVENTS_TABLE_NAME.to_string(),
            TableEntry {
                schema: bootstrap_events_schema(),
                next_sequence_id: 0,
                next_batch_id: 0,
                next_segment_id: 0,
                bootstrap_events_table: true,
                windows: Vec::new(),
            },
        );
        m
    }

    #[test]
    fn manifest_catalog_resolves_bootstrap_events_table() {
        let manifest = seeded_manifest();
        let cat = ManifestCatalog::new(&manifest);
        let schema = cat
            .resolve_table(BOOTSTRAP_EVENTS_TABLE_NAME)
            .expect("events must resolve");
        assert_eq!(schema, bootstrap_events_schema());
    }

    #[test]
    fn manifest_catalog_unknown_table_returns_plan_error() {
        let manifest = seeded_manifest();
        let cat = ManifestCatalog::new(&manifest);
        match cat.resolve_table("purchases") {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("purchases"), "got: {msg}");
                assert!(msg.contains("unknown table"), "got: {msg}");
            }
            Err(other) => panic!("expected Plan error, got {other:?}"),
            Ok(_) => panic!("purchases should not resolve"),
        }
    }

    #[test]
    fn manifest_catalog_lists_every_seeded_table() {
        let mut manifest = seeded_manifest();
        // Hand-seed a second table so we can verify multi-entry
        // listing without waiting for a real DDL path.
        let purchases = TableSchema::new(
            "purchases",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("purchased_at", BqlType::Timestamp),
                ColumnDef::required("kind", BqlType::String),
            ],
            "user_id",
            "purchased_at",
            "kind",
        )
        .expect("purchases schema");
        manifest.tables.insert(
            "purchases".to_string(),
            TableEntry {
                schema: purchases,
                next_sequence_id: 0,
                next_batch_id: 0,
                next_segment_id: 0,
                bootstrap_events_table: false,
                windows: Vec::new(),
            },
        );

        let cat = ManifestCatalog::new(&manifest);
        let mut names = cat.list_tables();
        names.sort();
        assert_eq!(names, vec!["events", "purchases"]);
    }

    #[test]
    fn manifest_catalog_lists_nothing_on_an_empty_manifest() {
        let manifest = Manifest::new_empty(DEFAULT_SHARD_COUNT);
        let cat = ManifestCatalog::new(&manifest);
        assert!(cat.list_tables().is_empty());
    }

    #[test]
    fn manifest_catalog_is_usable_as_dyn_catalog() {
        // Compile-time + runtime check that ManifestCatalog can stand
        // in for the `&dyn Catalog` the planner will hold at plan time.
        let manifest = seeded_manifest();
        let cat = ManifestCatalog::new(&manifest);
        let dyn_cat: &dyn Catalog = &cat;
        assert!(dyn_cat.resolve_table("events").is_ok());
        assert_eq!(dyn_cat.list_tables(), vec!["events"]);
    }
}
