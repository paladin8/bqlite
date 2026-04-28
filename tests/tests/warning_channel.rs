//! Integration coverage for the per-query warning channel
//! (TASK-511). Exercises the end-to-end path:
//!
//! - `EntityOperator::take_pending_warnings` (operator side)
//! - `EntityOperatorAdapter` forwarding (engine side)
//! - `WarningSink` aggregation
//! - `ExecutionResult.warnings` surfacing
//!
//! See `docs/design/engine/cancellation.md` §7 for the protocol.

use std::sync::atomic::{AtomicU64, Ordering};

use bqlite_core::QueryWarning;
use bqlite_engine::{Database, Engine};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch_path(label: &str) -> std::path::PathBuf {
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let mut path = std::env::temp_dir();
    path.push(format!("bqlite-warning-channel-{label}-{pid}-{seq}"));
    path
}

struct Scratch {
    path: std::path::PathBuf,
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[test]
fn happy_path_query_carries_empty_warnings_vec() {
    // A query that runs no stateful operator must produce no warnings
    // and an empty `Vec` (not `None`) on the success path. This pins
    // the public API shape — callers can rely on `warnings.is_empty()`.
    let scratch = Scratch {
        path: scratch_path("happy"),
    };
    let mut db = Database::create(&scratch.path).expect("create db");
    let engine = Engine::new();
    engine
        .query(
            "CREATE TABLE events (\
                 entity_id STRING NOT NULL ENTITY KEY, \
                 ts TIMESTAMP NOT NULL EVENT TIME, \
                 event_type STRING NOT NULL EVENT TYPE\
             )",
            &mut db,
        )
        .expect("create table");
    let result = engine.query("events", &mut db).expect("query must succeed");
    assert!(
        result.warnings.is_empty(),
        "happy path must yield empty warnings: {:?}",
        result.warnings
    );
}

#[test]
fn parse_error_surfaces_through_execution_failure_with_empty_warnings() {
    // A parse-time error fires before any operator runs, so the
    // partial-warnings vec is empty. The contract is that the error
    // is wrapped in `ExecutionFailure` so callers can pattern-match
    // on `Err(ExecutionFailure { error, warnings })` uniformly.
    let scratch = Scratch {
        path: scratch_path("parse-err"),
    };
    let mut db = Database::create(&scratch.path).expect("create db");
    let engine = Engine::new();
    engine
        .query(
            "CREATE TABLE events (\
                 entity_id STRING NOT NULL ENTITY KEY, \
                 ts TIMESTAMP NOT NULL EVENT TIME, \
                 event_type STRING NOT NULL EVENT TYPE\
             )",
            &mut db,
        )
        .expect("create table");
    let err = engine
        .query("42invalid_query", &mut db)
        .expect_err("should parse-fail");
    assert!(matches!(err.error, bqlite_core::BqliteError::Parse(_)));
    assert!(
        err.warnings.is_empty(),
        "parse failure cannot record warnings"
    );
}

#[test]
fn warnings_overflow_aggregates_suppressed_count() {
    // Direct exercise of the WorkerContext cap: drive 1005 raw warnings
    // through the sink and verify the assembled list has 1000 entries
    // plus a trailing WarningsOverflow with suppressed_count = 5.
    use bqlite_engine::{WarningSink, WorkerContext};
    let sink = WarningSink::new();
    for i in 0..(WorkerContext::PER_WORKER_WARNING_CAP + 5) {
        sink.record(QueryWarning::EntityEventLimitExceeded {
            entity_id: format!("e{i}"),
            count: 1,
            limit: 1,
        });
    }
    let warnings = sink.into_warnings();
    assert_eq!(
        warnings.len(),
        WorkerContext::PER_WORKER_WARNING_CAP + 1,
        "1000 warnings + 1 overflow marker"
    );
    match warnings.last().unwrap() {
        QueryWarning::WarningsOverflow { suppressed_count } => {
            assert_eq!(*suppressed_count, 5);
        }
        other => panic!("expected trailing WarningsOverflow, got {other:?}"),
    }
}
