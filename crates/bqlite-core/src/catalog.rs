//! Catalog — the trait the planner uses to resolve table names to
//! schemas.
//!
//! # Why this lives in `bqlite-core`
//!
//! The planner needs a handle it can call at plan time to answer
//! "what's the schema of this table?" — see
//! `docs/design/planner-pipeline.md` §4.1:
//!
//! > Look up primary table and every `JOIN` table in the catalog.
//! > Reject unknown tables.
//!
//! The planner crate (`bqlite-planner`) depends only on `bqlite-ast`
//! and `bqlite-core`, **not** on `bqlite-storage` — the dependency
//! direction forbids it (see `CLAUDE.md` / `docs/architecture.md`).
//! Putting the trait here lets the planner call
//! `catalog.resolve_table(name)` without pulling in the storage crate.
//! The concrete manifest-backed implementation lives in
//! `bqlite-storage::catalog` and is handed to the planner as
//! `&dyn Catalog`.
//!
//! # Error model
//!
//! `resolve_table` returns [`BqliteError::Plan`] for unknown tables.
//! That variant is the v0 stand-in for the typed `TypeError` enum
//! sketched in `docs/design/type-system.md` §12 — see the comment on
//! `BqliteError::Plan` in [`crate::error`]. A later-wave refactor may
//! widen the return type to a typed `TypeError`, at which point this
//! trait will change in lockstep (captured as a `[TRAIT]` task).
//!
//! # Object safety
//!
//! The trait is deliberately object-safe so callers can take
//! `&dyn Catalog`. This keeps the planner's signature decoupled from
//! any specific backing store — tests can pass an in-memory impl,
//! production uses the manifest-backed impl in `bqlite-storage`, and
//! future waves can bolt on network- or federation-backed catalogs
//! without touching the planner.

use crate::error::{BqliteError, Result};
use crate::schema::TableSchema;

/// A read-only view of the tables visible to a query.
///
/// Implementations must be thread-safe (`Send + Sync`) because the
/// engine's execution model shares the catalog across parallel
/// shard-tasks (see `docs/design/execution-model.md`). They must also
/// implement [`std::fmt::Debug`] so the planner can include the
/// catalog in trace output and debug assertions without a separate
/// printer abstraction — adding this bound later would be a breaking
/// change for every downstream impl, so it is paid for up front.
///
/// # Method contract
///
/// - [`Catalog::resolve_table`] returns the authoritative schema for
///   `name`, or [`BqliteError::Plan`] if no such table exists.
///   Implementations should include the requested name in the error
///   message so the planner can surface it back to the user.
/// - [`Catalog::list_tables`] returns every table name known to the
///   catalog, in an implementation-defined but stable order. The
///   returned `Vec` borrows its string contents from `self`, so the
///   references live only as long as the borrow of the catalog.
pub trait Catalog: Send + Sync + std::fmt::Debug {
    /// Look up the schema for `name`.
    ///
    /// Returns an owned [`TableSchema`] — callers that want to share
    /// the result across threads or cache it can wrap it in
    /// `Arc<TableSchema>` themselves.
    ///
    /// # Errors
    ///
    /// [`BqliteError::Plan`] when no table with `name` exists in the
    /// catalog. Other error variants are reserved for implementations
    /// that may fail to load metadata (e.g. an I/O error in a future
    /// distributed catalog).
    fn resolve_table(&self, name: &str) -> Result<TableSchema>;

    /// Enumerate every table name currently visible in the catalog.
    ///
    /// Iteration order is implementation-defined but stable for the
    /// lifetime of the borrow — callers that need sorted output must
    /// sort the returned slice themselves.
    fn list_tables(&self) -> Vec<&str>;
}

/// Helper for building an error when a catalog lookup fails.
///
/// Implementations are free to construct their own error strings;
/// this helper exists so every catalog — core tests, manifest-backed,
/// future network-backed — produces a message the planner can
/// string-match in error-path tests without coupling to a specific
/// backing store.
pub fn unknown_table_error(name: &str) -> BqliteError {
    BqliteError::Plan(format!("unknown table `{name}`"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::property::BqlType;
    use crate::schema::ColumnDef;

    use super::*;

    // A minimal in-memory `Catalog` implementation used to exercise
    // the trait surface and object safety without dragging in the
    // `bqlite-storage` crate. Concrete impls in downstream crates
    // should follow this shape.
    #[derive(Debug)]
    struct InMemoryCatalog {
        tables: BTreeMap<String, TableSchema>,
    }

    impl InMemoryCatalog {
        fn new() -> Self {
            Self {
                tables: BTreeMap::new(),
            }
        }

        fn with_table(mut self, schema: TableSchema) -> Self {
            self.tables.insert(schema.name().to_string(), schema);
            self
        }
    }

    impl Catalog for InMemoryCatalog {
        fn resolve_table(&self, name: &str) -> Result<TableSchema> {
            self.tables
                .get(name)
                .cloned()
                .ok_or_else(|| unknown_table_error(name))
        }

        fn list_tables(&self) -> Vec<&str> {
            self.tables.keys().map(String::as_str).collect()
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
        .expect("minimal events schema")
    }

    #[test]
    fn resolve_known_table_returns_owned_schema() {
        let cat = InMemoryCatalog::new().with_table(events_schema());
        let schema = cat.resolve_table("events").expect("events must resolve");
        assert_eq!(schema.name(), "events");
        assert_eq!(schema.columns().len(), 3);
    }

    #[test]
    fn resolve_unknown_table_returns_plan_error() {
        let cat = InMemoryCatalog::new();
        match cat.resolve_table("ghost") {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("ghost"), "error should name the table: {msg}");
                assert!(
                    msg.contains("unknown table"),
                    "error should use the standard phrasing: {msg}"
                );
            }
            Err(other) => panic!("expected Plan error, got {other:?}"),
            Ok(_) => panic!("ghost should not resolve"),
        }
    }

    #[test]
    fn list_tables_returns_every_registered_name() {
        let cat = InMemoryCatalog::new()
            .with_table(events_schema())
            .with_table(
                TableSchema::new(
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
                .expect("purchases schema"),
            );
        let mut names = cat.list_tables();
        // Sort in the test because the trait contract only promises
        // *stable*, not *sorted*, order — this in-memory impl happens
        // to be sorted thanks to BTreeMap, but the assertion should
        // not rely on that.
        names.sort();
        assert_eq!(names, vec!["events", "purchases"]);
    }

    #[test]
    fn list_tables_on_empty_catalog_is_empty() {
        let cat = InMemoryCatalog::new();
        assert!(
            cat.list_tables().is_empty(),
            "empty catalog should list no tables"
        );
    }

    #[test]
    fn trait_is_object_safe_behind_dyn_reference() {
        // The planner will hold `&dyn Catalog` — this test fails to
        // compile if we accidentally add a non-object-safe method.
        let cat = InMemoryCatalog::new().with_table(events_schema());
        let dyn_cat: &dyn Catalog = &cat;
        assert!(dyn_cat.resolve_table("events").is_ok());
        assert_eq!(dyn_cat.list_tables(), vec!["events"]);
    }

    #[test]
    fn trait_is_object_safe_behind_box() {
        // `Box<dyn Catalog>` ownership is the other shape the engine
        // needs — some future waves will hand a catalog off by value.
        let cat: Box<dyn Catalog> = Box::new(InMemoryCatalog::new().with_table(events_schema()));
        assert!(cat.resolve_table("events").is_ok());
        // Unknown-table lookup still surfaces the standard error.
        assert!(cat.resolve_table("nope").is_err());
    }

    #[test]
    fn unknown_table_error_helper_is_plan_variant() {
        match unknown_table_error("widgets") {
            BqliteError::Plan(msg) => {
                assert_eq!(msg, "unknown table `widgets`");
            }
            other => panic!("expected Plan, got {other:?}"),
        }
    }
}
