//! Physical-plan bind step — plain-data → executable operator tree.
//!
//! Per [`docs/design/planner-pipeline.md`](../../../docs/design/planner-pipeline.md)
//! §15, the planner emits a plain-data [`PhysicalPlan`] tree, not
//! `Box<dyn PhysicalOperator>` — the trait object lives in
//! `bqlite-operators`, which sits above `bqlite-planner` in the crate
//! dependency graph. Binding is the engine's responsibility: it
//! consumes the descriptor and materializes one concrete operator per
//! descriptor node, wiring in the runtime handles (segment readers,
//! cancellation tokens, memory budgets) that only the engine has
//! visibility into.
//!
//! ## Wave 1 scope
//!
//! The planner (TASK-115) only emits [`ScanPhysical`], so the bind
//! step's only arm is `bind_scan`. It:
//!
//! 1. Asks the [`Database`] for a `Box<dyn SegmentReader>` for the
//!    resolved table name (which returns
//!    `BqliteError::Plan(unknown table …)` if the planner's catalog
//!    view has drifted from the storage manifest — defensive, should
//!    be unreachable in a single-process session).
//! 2. Converts the owned box into an `Arc<dyn SegmentReader>` so the
//!    scan can share ownership with future parallel shard-tasks
//!    without changing the trait surface.
//! 3. Constructs a [`ScanOperator`] with `ColumnProjection::all` and
//!    no predicate — Wave 2's TASK-226/227/228 will thread these in
//!    when the planner starts producing non-trivial descriptors.
//!
//! Wave 2's TASK-232 grows this module with arms for `FilterPhysical`,
//! `ProjectPhysical`, and `LimitPhysical`, plus a DDL execution path
//! for `CreateTablePhysical`. For now the single arm keeps the
//! coupling between the engine and the concrete operator types
//! minimal.

use std::sync::Arc;

use bqlite_core::{ColumnProjection, Result, SegmentReader};
use bqlite_operators::{CancellationToken, PhysicalOperator, ScanOperator};
use bqlite_planner::{PhysicalPlan, ScanPhysical};
use bqlite_storage::Database;

/// Bind a plain-data [`PhysicalPlan`] into an executable
/// `Box<dyn PhysicalOperator>` tree rooted at the plan's top node.
///
/// Each descriptor arm is responsible for wiring in runtime handles
/// (`Database` for segment readers, the shared
/// [`CancellationToken`], later the memory budget and metrics sink).
/// The returned operator is ready to drive with `open → next_batch* →
/// close` per the [`PhysicalOperator`] lifecycle contract.
///
/// # Errors
///
/// Propagates any error from [`Database::segment_reader`] — in Wave 1
/// the only failure mode is an unknown table name (which should be
/// unreachable because the planner already resolved the table via the
/// same database's catalog).
pub fn bind_physical(plan: &PhysicalPlan, db: &Database) -> Result<Box<dyn PhysicalOperator>> {
    match plan {
        PhysicalPlan::Scan(scan) => bind_scan(scan, db),
    }
}

fn bind_scan(scan: &ScanPhysical, db: &Database) -> Result<Box<dyn PhysicalOperator>> {
    // `Database::segment_reader` returns a `Box<dyn SegmentReader>` —
    // the Wave 1 stub is the `EmptySegmentReader` TASK-116 wired in,
    // and Wave 2 will swap in a real segment-format reader. We hand
    // it to the scan as an `Arc` so later waves can share ownership
    // across parallel shard-tasks without changing the trait.
    let reader_box: Box<dyn SegmentReader> = db.segment_reader(&scan.table)?;
    let reader: Arc<dyn SegmentReader> = Arc::from(reader_box);

    // Wave 1: scan every column, no predicate pushdown, fresh
    // cancellation token. Wave 2's TASK-226 threads these in from the
    // ScanPhysical descriptor once the projection-pruning and
    // predicate-pushdown passes land.
    let op = ScanOperator::new(
        reader,
        ColumnProjection::all(),
        None,
        CancellationToken::new(),
    );
    Ok(Box::new(op))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use bqlite_core::OperatorSchema;
    use bqlite_planner::{plan, PhysicalPlan, ScanPhysical};
    use bqlite_storage::{bootstrap_events_schema, Database};

    use super::*;

    /// Per-test unique temp directory. Mirrors the pattern used in
    /// `bqlite_storage::database::tests` — process PID + monotonic
    /// counter is enough for in-process uniqueness without pulling
    /// `tempfile` into the dev-dependency closure.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-engine-bind-{label}-{pid}-{seq}"));
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

    fn bootstrap_scan_descriptor() -> PhysicalPlan {
        PhysicalPlan::Scan(ScanPhysical {
            table: "events".to_string(),
            output_schema: OperatorSchema::from_table(&bootstrap_events_schema()),
        })
    }

    #[test]
    fn bind_scan_produces_a_drivable_operator() {
        let scratch = Scratch::new("happy");
        let db = Database::open_or_create(scratch.path()).expect("open db");
        let descriptor = bootstrap_scan_descriptor();

        let mut op = bind_physical(&descriptor, &db).expect("bind must succeed");

        // Full PhysicalOperator lifecycle — the smoke test (TASK-123)
        // will drive this exact path through `Engine::query`.
        op.open().expect("open should succeed");
        assert!(
            op.next_batch()
                .expect("next_batch should succeed")
                .is_none(),
            "bootstrap events table has zero segments so the first pull must exhaust"
        );
        // Exhaustion is sticky.
        assert!(op.next_batch().unwrap().is_none());
        op.close().expect("close should succeed");
    }

    #[test]
    fn bind_scan_output_schema_matches_descriptor() {
        let scratch = Scratch::new("schema");
        let db = Database::open_or_create(scratch.path()).expect("open db");
        let descriptor = bootstrap_scan_descriptor();
        let expected = match &descriptor {
            PhysicalPlan::Scan(s) => s.output_schema.clone(),
        };

        let op = bind_physical(&descriptor, &db).expect("bind must succeed");
        assert_eq!(op.output_schema(), &expected);
    }

    #[test]
    fn bind_scan_reports_unknown_table_through_plan_error() {
        let scratch = Scratch::new("unknown");
        let db = Database::open_or_create(scratch.path()).expect("open db");
        let descriptor = PhysicalPlan::Scan(ScanPhysical {
            table: "ghost".to_string(),
            output_schema: OperatorSchema::from_table(&bootstrap_events_schema()),
        });

        match bind_physical(&descriptor, &db) {
            Err(bqlite_core::BqliteError::Plan(msg)) => {
                assert!(msg.contains("ghost"), "error should name the table: {msg}");
            }
            Err(other) => panic!("expected Plan error for unknown table, got {other:?}"),
            Ok(_) => panic!("expected Plan error for unknown table, got Ok"),
        }
    }

    #[test]
    fn bind_physical_composes_with_planner_output() {
        // End-to-end spot check: run the Wave 1 pipeline (parse ->
        // plan -> bind) against a real bootstrap database and confirm
        // every stage hands off the expected shape. This duplicates
        // the smoke test coverage (TASK-123) in miniature so that a
        // regression in the bind step is localized to *this* file
        // rather than surfacing as a generic smoke-test failure.
        let scratch = Scratch::new("compose");
        let db = Database::open_or_create(scratch.path()).expect("open db");
        let stmt = bqlite_parser::parse("events").expect("parse events");
        let catalog = db.catalog();
        let physical = plan(stmt, &catalog).expect("plan events");

        let mut op = bind_physical(&physical, &db).expect("bind succeeds");
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
        op.close().unwrap();
    }
}
