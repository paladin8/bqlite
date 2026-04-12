//! Text-in, rows-out query entry point.
//!
//! [`Engine::query`] is the single public surface exercised by the
//! CLI (TASK-119), future Python bindings (Wave 6), and the
//! end-to-end smoke test (TASK-123). It threads the compiler pipeline
//! — parse → plan → bind → drive — into one opinionated function:
//!
//! ```text
//! text + &mut Database
//!    │
//!    ▼
//!  bqlite_parser::parse ─▶ Statement (AST)
//!    │
//!    ▼
//!  bqlite_planner::plan(stmt, &catalog) ─▶ PhysicalPlan (plain data)
//!    │
//!    ▼
//!  bind_physical(plan, db) ─▶ Box<dyn PhysicalOperator>
//!    │                        (DDL executes during bind)
//!    ▼
//!  operator.open() → next_batch()* → close()
//!    │
//!    ▼
//!  ExecutionResult { schema, rows }
//! ```
//!
//! ## Why this lives in `bqlite-engine`
//!
//! The architecture forbids callers like `bqlite-cli` from importing
//! `bqlite-parser` / `bqlite-planner` / `bqlite-operators` directly
//! (see `docs/architecture.md` §"Dependency Direction"). The engine
//! is where all four get stitched together — any other layering would
//! either force a dep-direction violation or require duplicating the
//! query pipeline in every caller. TASK-118 is the task that
//! introduces the `bqlite-engine → bqlite-parser` edge for exactly
//! this reason; the dep-direction check in
//! `scripts/check-dep-direction.sh` is updated in the same PR.
//!
//! ## Wave 1 deferrals
//!
//! The Wave 1 implementation intentionally skips:
//!
//! - **Memory budgets / spill-to-disk** — the `Engine` struct holds
//!   no budget; every operator gets an unbounded allocation envelope.
//!   Wave 5 replaces this with the real memory-enforcement model.
//! - **Concurrency and parallelism** — the query runs in the calling
//!   thread. Wave 3+ introduces shard-per-thread execution.
//! - **Cancellation timers** — the bind step hands each operator a
//!   fresh `CancellationToken`, but the engine never signals it.
//!   Wave 5 wires in query-level timeouts and Ctrl-C handling.
//! - **Metrics** — the `Metrics` trait (TASK-112) is ignored until
//!   the metrics-to-span bridge lands in a later wave.
//! - **Query warnings** — `ExecutionResult` is just `{schema, rows}`;
//!   the warnings channel from `execution-model.md` arrives with the
//!   first operator that produces non-fatal diagnostics.
//!
//! These deferrals keep the Wave 1 surface tiny so later wave
//! additions can be purely additive (new fields on `ExecutionResult`,
//! new methods on `Engine`) without churning callers.

use arrow::record_batch::RecordBatch;

use bqlite_core::{BqliteError, OperatorSchema, Result};
use bqlite_storage::Database;

use crate::bind::bind_physical;

/// The result of a successfully executed query.
///
/// Carries the output [`OperatorSchema`] alongside the materialized
/// row batches. Holding the schema separately from the batches lets
/// callers render empty result sets (the Wave 1 smoke test's
/// `bqlite query "events"` against a fresh database produces zero
/// batches, and the CLI still needs to emit a header row).
///
/// Row batches are returned in the order the root operator produced
/// them — the engine does not re-sort, deduplicate, or re-chunk.
/// Every batch's Arrow schema matches `schema.to_arrow_schema()`.
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Output schema. Matches every batch in `rows`.
    pub schema: OperatorSchema,
    /// Materialized row batches. Empty when the query produced no
    /// rows — not a signal of failure.
    pub rows: Vec<RecordBatch>,
}

impl ExecutionResult {
    /// Total number of rows across every batch.
    ///
    /// Convenience for the CLI's truncation footer and for tests
    /// that only care about row count, not the row contents.
    pub fn row_count(&self) -> usize {
        self.rows.iter().map(|b| b.num_rows()).sum()
    }

    /// True when the result contains zero rows across every batch.
    ///
    /// A result with `rows.len() > 0` but every batch empty still
    /// counts as "no rows" — mid-stream empty batches are legal per
    /// the [`PhysicalOperator`](bqlite_operators::PhysicalOperator)
    /// contract, and callers that want to know "did the query
    /// actually produce anything" should ask this method rather than
    /// `rows.is_empty()`.
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }
}

/// The bqlite query engine — a stateless dispatcher that compiles a
/// text query and drives the resulting operator tree.
///
/// Wave 1 holds no configuration at all: `Engine::new()` and
/// `Engine::default()` are interchangeable. Later waves add memory
/// budget, thread-pool handle, warning sink, and metrics hooks as
/// additional fields with `Default` impls so existing callers keep
/// compiling unchanged.
#[derive(Debug, Default, Clone, Copy)]
pub struct Engine {
    // Wave 1: no fields yet. Kept as a struct (not a unit type)
    // because future waves will add non-Copy state (`Arc<ThreadPool>`,
    // `Arc<MetricsSink>`, ...) and changing a unit type to a struct
    // with state is a breaking API change.
    _private: (),
}

impl Engine {
    /// Construct a default `Engine`. Wave 1 has no configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse, plan, bind, and execute `text` against `db`, collecting
    /// every row batch into an [`ExecutionResult`].
    ///
    /// This is the single text-in, rows-out surface every bqlite
    /// caller goes through. See the module-level docs for the full
    /// compiler pipeline and the Wave 1 deferrals.
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Parse`] if the parser rejects the text.
    /// - [`BqliteError::Plan`] if the planner cannot resolve the
    ///   table or the statement shape is outside Wave 1's scope.
    /// - [`BqliteError::Io`] / [`BqliteError::Arrow`] /
    ///   [`BqliteError::Execution`] from the operator tree.
    /// - [`BqliteError::Cancelled`] — unreachable in Wave 1 because
    ///   the engine never signals cancellation, but the error
    ///   variant is still propagated verbatim from the operators.
    pub fn query(&self, text: &str, db: &mut Database) -> Result<ExecutionResult> {
        // 1. Parse. The Wave 1 parser only accepts a bare table
        //    identifier, so the text is always very short; we convert
        //    its typed `ParseError` into a `BqliteError::Parse(String)`
        //    because the unified error enum uses `String` for parse
        //    failures (see `bqlite_core::error::BqliteError::Parse`).
        let statement =
            bqlite_parser::parse(text).map_err(|e| BqliteError::Parse(e.to_string()))?;

        // 2. Plan. The database's `ManifestCatalog<'_>` implements
        //    `Catalog`, and the planner only needs a `&dyn Catalog` —
        //    the borrow lives only for this call.
        let catalog = db.catalog();
        let physical = bqlite_planner::plan(statement, &catalog)?;

        // Snapshot the root schema *before* binding. Binding consumes
        // the descriptor via `bind_physical` (by reference for Wave 1),
        // and the returned operator's `output_schema()` is the same
        // shape, but holding the clone here keeps `ExecutionResult`
        // construction independent of the operator's lifetime —
        // important for later waves that may drop the operator
        // between reading the last batch and returning.
        let schema = physical.output_schema().clone();

        // 3. Bind the plain-data descriptor into an executable
        //    operator tree. Handles data-plane operators (Scan,
        //    Filter, Project, Limit), DDL (which executes during
        //    bind), and metadata queries (Describe, Explain).
        let mut operator = bind_physical(&physical, db)?;

        // 4. Drive the operator tree to completion. `open` → zero or
        //    more `next_batch` → `close`. `close` runs even on the
        //    error path so that mmap / file handles / spill files
        //    are released promptly; see the `PhysicalOperator`
        //    lifecycle contract in
        //    `docs/design/operators/operator-traits.md` §4.2.
        let drive_result = drive_to_completion(operator.as_mut());
        // `close` is idempotent and must run regardless of
        // `drive_result`. We deliberately swallow a `close` error when
        // the query itself already failed — the caller needs to see
        // the original error, not a tear-down artifact. This is the
        // same "primary error wins" convention the standard library
        // uses in `Drop`-based cleanup paths.
        let close_result = operator.close();

        let rows = drive_result?;
        close_result?;

        Ok(ExecutionResult { schema, rows })
    }
}

/// Open the operator and pull every batch until exhaustion.
///
/// Returns the collected batches on success. On any operator error,
/// the caller is still responsible for calling `close` — this helper
/// does not, so that `Engine::query` can guarantee `close` runs on
/// both the happy and sad paths without double-closing here.
fn drive_to_completion(
    operator: &mut dyn bqlite_operators::PhysicalOperator,
) -> Result<Vec<RecordBatch>> {
    operator.open()?;
    let mut rows = Vec::new();
    while let Some(batch) = operator.next_batch()? {
        rows.push(batch);
    }
    Ok(rows)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use bqlite_storage::Database;

    use super::*;

    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-engine-query-{label}-{pid}-{seq}"));
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

    fn create_db_with_events(path: &Path) -> Database {
        let mut db = Database::create(path).expect("create db");
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
            .expect("create events table");
        db
    }

    // ── Happy path: the Wave 1 smoke test shape ─────────────────────

    #[test]
    fn query_events_on_fresh_database_returns_empty_result() {
        // This is the exact shape the Wave 1 acceptance gate (TASK-123)
        // drives through the CLI: a fresh database directory plus the
        // bare identifier `events` must return an empty ExecutionResult
        // — not an error, not a panic, just zero rows.
        let scratch = Scratch::new("smoke");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();

        let result = engine.query("events", &mut db).expect("query must succeed");

        assert!(result.is_empty(), "fresh database has no events");
        assert_eq!(result.row_count(), 0);
        // Schema must reflect the bootstrap events table plus the
        // `__seq_id` / `__batch_id` system columns from
        // `OperatorSchema::from_table`.
        let names: Vec<&str> = result
            .schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["entity_id", "ts", "event_type", "__seq_id", "__batch_id"]
        );
    }

    #[test]
    fn query_with_leading_whitespace_still_parses_and_plans() {
        // Defensive: the Wave 1 parser tolerates whitespace; the
        // engine wrapper must not over-trim or reject it.
        let scratch = Scratch::new("whitespace");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();

        let result = engine
            .query("  events\n", &mut db)
            .expect("query must succeed");
        assert!(result.is_empty());
    }

    // ── Parse failures ──────────────────────────────────────────────

    #[test]
    fn empty_query_returns_parse_error() {
        let scratch = Scratch::new("empty-parse");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();

        match engine.query("", &mut db) {
            Err(BqliteError::Parse(msg)) => {
                // Wave 2's parser surfaces the empty-input case as an
                // UnexpectedEof with the hint "expected a table name" —
                // more actionable than the Wave 1 stub's "empty input"
                // wording, but still satisfies the engine contract that
                // the parse error propagates cleanly as
                // `BqliteError::Parse`.
                assert!(
                    msg.contains("table name"),
                    "error should describe the expected source: {msg}"
                );
            }
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    #[test]
    fn garbage_query_returns_parse_error() {
        let scratch = Scratch::new("garbage-parse");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();

        match engine.query("42events", &mut db) {
            Err(BqliteError::Parse(_)) => {}
            other => panic!("expected Parse error, got {other:?}"),
        }
    }

    // ── Plan failures ───────────────────────────────────────────────

    #[test]
    fn unknown_table_returns_plan_error() {
        let scratch = Scratch::new("unknown-plan");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();

        match engine.query("ghost", &mut db) {
            Err(BqliteError::Plan(msg)) => {
                assert!(msg.contains("ghost"), "error should name the table: {msg}");
                assert!(
                    msg.contains("unknown table"),
                    "error should use the standard phrasing: {msg}"
                );
            }
            other => panic!("expected Plan error, got {other:?}"),
        }
    }

    // ── Engine surface basics ───────────────────────────────────────

    #[test]
    fn engine_default_matches_new() {
        let a: Engine = Engine::default();
        let b: Engine = Engine::new();
        // Both are zero-sized / `_private: ()`, so they must be
        // bit-for-bit equal — but we cannot derive `PartialEq` on a
        // zero-sized `_private` field without adding it to the public
        // surface, so compare by their Debug shape instead.
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
    }

    #[test]
    fn execution_result_row_count_and_is_empty_are_consistent() {
        // Empty result — row_count == 0, is_empty() true.
        let scratch = Scratch::new("rowcount");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let result = engine.query("events", &mut db).expect("events must plan");

        assert_eq!(result.row_count(), 0);
        assert!(result.is_empty());
    }
}
