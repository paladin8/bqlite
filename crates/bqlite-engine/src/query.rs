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

use bqlite_core::{BqliteError, OperatorSchema, QueryWarning};
use bqlite_planner::PhysicalPlan;
use bqlite_storage::Database;

use crate::bind::bind_physical;
use crate::context::{resolve_query_budget, EngineConfig, QueryContext, QueryOptions};

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
///
/// `rows_affected` is `Some(n)` for statements that mutate data and
/// have an exact count to report (DELETE per
/// `docs/design/storage/deletes.md` §11). `None` for SELECT, DDL,
/// EXPLAIN, and INSERT (which today returns no count). Callers that
/// want to render "n rows deleted" inspect this field.
///
/// `peak_memory_bytes` is `Some(n)` when the query was executed with a
/// real `MemoryTracker` (TASK-510 / `docs/design/engine/memory-budget.md`
/// §10) and reports the high-water mark of bytes reserved through the
/// per-query budget. `None` when the engine was running with the
/// `UnboundedMemory` stub — in that mode no peak is tracked. The field
/// is reported even when no operator yet calls `try_reserve`; expect
/// `Some(0)` for queries whose operators have not been wired against
/// the budget (operator-side wiring is TASK-512/513/514).
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// Output schema. Matches every batch in `rows`.
    pub schema: OperatorSchema,
    /// Materialized row batches. Empty when the query produced no
    /// rows — not a signal of failure.
    pub rows: Vec<RecordBatch>,
    /// Exact rows-affected count for mutating statements. `None`
    /// when the statement does not produce a count (queries, DDL,
    /// INSERT). DELETE always populates this with `Some(n)` per
    /// `docs/design/storage/deletes.md` §11.
    pub rows_affected: Option<u64>,
    /// Peak per-query memory bytes observed by the `MemoryTracker`
    /// since this query started executing. `None` for the unbounded
    /// budget path.
    pub peak_memory_bytes: Option<u64>,
    /// Per-query warnings recorded during execution. Empty when no
    /// stateful operator hit a per-entity cap. The order is
    /// per-`docs/design/engine/cancellation.md` §7.3 (record order,
    /// with a final `WarningsOverflow` appended when any warnings
    /// were suppressed).
    pub warnings: Vec<QueryWarning>,
}

/// Wrapper attached when the engine surfaces a fatal error alongside
/// any partial diagnostics the operators recorded before failure.
///
/// See `docs/design/engine/cancellation.md` §5.4. `From<BqliteError>`
/// is implemented so internal `?` propagation continues to work; the
/// failure case wraps with `warnings: Vec::new()` and the driver
/// stitches the partial warnings in at the boundary before returning.
#[derive(Debug)]
pub struct ExecutionFailure {
    pub error: BqliteError,
    pub warnings: Vec<QueryWarning>,
}

impl ExecutionFailure {
    pub fn new(error: BqliteError, warnings: Vec<QueryWarning>) -> Self {
        Self { error, warnings }
    }

    /// Pattern-friendly extraction for callers that only want the
    /// inner error. Equivalent to destructuring `failure.error`.
    pub fn into_error(self) -> BqliteError {
        self.error
    }
}

impl From<BqliteError> for ExecutionFailure {
    fn from(error: BqliteError) -> Self {
        Self {
            error,
            warnings: Vec::new(),
        }
    }
}

impl std::fmt::Display for ExecutionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for ExecutionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
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
/// `Engine` carries an [`EngineConfig`] (default per
/// `docs/design/engine/memory-budget.md` § 2.2) used to size each
/// query's [`QueryContext`]. `Engine::new()` and `Engine::default()`
/// remain interchangeable — both pick up the default config; hosts
/// that need a custom budget call `Engine::with_config(...)`. Later
/// waves add a thread-pool handle, warning sink, and metrics hooks as
/// additional fields without breaking existing callers.
#[derive(Debug, Default, Clone, Copy)]
pub struct Engine {
    config: EngineConfig,
}

impl Engine {
    /// Construct an engine with default configuration (3 GiB query
    /// budget, see `docs/design/engine/memory-budget.md` § 2.2).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an engine with a custom configuration.
    pub fn with_config(config: EngineConfig) -> Self {
        Self { config }
    }

    /// Returns the engine's effective configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Parse, plan, bind, and execute `text` against `db`, collecting
    /// every row batch into an [`ExecutionResult`].
    ///
    /// This is the single text-in, rows-out surface every bqlite
    /// caller goes through. See the module-level docs for the full
    /// compiler pipeline and the Wave 1 deferrals.
    ///
    /// Runs with the engine-level default [`QueryOptions`]. Use
    /// [`Engine::query_with_options`] to override per-query settings
    /// such as `memory_budget_bytes`.
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
    pub fn query(
        &self,
        text: &str,
        db: &mut Database,
    ) -> std::result::Result<ExecutionResult, ExecutionFailure> {
        self.query_with_options(text, db, &QueryOptions::default())
    }

    /// Parse, plan, bind, and execute `text` against `db` with
    /// per-query overrides supplied through `options`.
    ///
    /// Validates the requested memory budget against
    /// [`crate::context::MIN_QUERY_BUDGET_BYTES`]. A budget below the
    /// floor surfaces as `BqliteError::Execution` wrapped in an
    /// [`ExecutionFailure`]. Per-query warnings collected before any
    /// fatal error are stitched into the failure's `warnings` field
    /// (`docs/design/engine/cancellation.md` §5.4).
    pub fn query_with_options(
        &self,
        text: &str,
        db: &mut Database,
        options: &QueryOptions,
    ) -> std::result::Result<ExecutionResult, ExecutionFailure> {
        // Resolve the per-query budget *before* the catch_unwind
        // boundary so a configuration error surfaces as a clean
        // ExecutionFailure with no panic-handling overhead.
        let budget_bytes = match resolve_query_budget(&self.config, options) {
            Ok(b) => b,
            Err(e) => return Err(ExecutionFailure::from(e)),
        };
        let ctx = QueryContext::new(budget_bytes).with_spill_fs(db.spill_fs().clone());

        // `AssertUnwindSafe` is required because `&mut Database` is
        // not `UnwindSafe` by default. This is sound here per
        // `docs/design/engine/cancellation.md` §4.1: the engine owns
        // the database for the call's duration, and on unwind the
        // database is dropped along with the operator tree without
        // further observation. `QueryContext` is `Clone` and the
        // `WarningSink` it carries is `Send + Sync`, so observing
        // it across the unwind boundary is fine.
        //
        // The single-threaded driver catches its own panic so a
        // panic in any operator surfaces as `BqliteError::OperatorPanic`
        // rather than aborting the process. TASK-541 generalizes this
        // to per-worker `catch_unwind` boundaries.
        let inner_ctx = ctx.clone();
        let inner = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_query_inner(text, db, &inner_ctx)
        }));

        match inner {
            // Success: `run_query_inner` already drained its sink
            // clone into `ExecutionResult.warnings`. The outer ctx
            // owns a clone of the same sink — `into_warnings` is
            // idempotent (mem::take), so reading it on the failure
            // paths below sees the already-drained buffer.
            Ok(Ok(result)) => Ok(result),
            // Cooperative failure: pull the partial warnings the
            // operators recorded before the error fired.
            Ok(Err(error)) => Err(ExecutionFailure {
                error,
                warnings: ctx.warnings().clone().into_warnings(),
            }),
            // Worker panic: surface as `OperatorPanic`. `location` is
            // always `None` until TASK-541 installs the project-local
            // panic hook, per `cancellation.md` §4.1.
            Err(payload) => {
                let message = panic_message(payload);
                Err(ExecutionFailure {
                    error: BqliteError::OperatorPanic {
                        message,
                        location: None,
                    },
                    warnings: ctx.warnings().clone().into_warnings(),
                })
            }
        }
    }
}

/// Inner pipeline. Returns `bqlite_core::Result<ExecutionResult>` so
/// `?` propagation works inside the body; the outer `Engine::query`
/// translates errors into [`ExecutionFailure`] with the partial
/// warnings stitched in.
fn run_query_inner(
    text: &str,
    db: &mut Database,
    ctx: &QueryContext,
) -> bqlite_core::Result<ExecutionResult> {
    // 1. Parse. `parse()` returns a Vec: zero or more
    //    `Statement::DefineAlias` items followed by the terminal
    //    statement (query, DDL, …). Its typed `ParseError` is
    //    converted to `BqliteError::Parse(String)` because the
    //    unified error enum uses `String` for parse failures.
    //    Alias definitions are handled by the planner's
    //    `plan_script` entrypoint (TASK-425 CP4).
    let stmts = bqlite_parser::parse(text).map_err(|e| BqliteError::Parse(e.to_string()))?;

    // 2. Plan.
    let now_ns = {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| BqliteError::Execution(format!("system clock before Unix epoch: {e}")))?
            .as_nanos()
            .try_into()
            .unwrap_or(i64::MAX)
    };
    let catalog = db.catalog();
    let physical = bqlite_planner::plan_script(stmts, &catalog, now_ns)?;

    // DELETE is dispatched out-of-band rather than through the
    // bind step because it produces no result rows but does
    // populate `ExecutionResult::rows_affected` (deletes.md §11).
    // The DELETE path does not yet use the QueryContext (TASK-525
    // wires it together with the rest of the cancellation / budget
    // plumbing). It also produces no per-entity warnings, so the
    // sink stays empty.
    if let PhysicalPlan::Delete(d) = &physical {
        return crate::delete::execute_delete_statement(d, db);
    }

    let schema = physical.output_schema().clone();

    // 3. Bind the plain-data descriptor into an executable operator
    //    tree. The QueryContext threads through both the memory
    //    budget (per `docs/design/engine/memory-budget.md`) and the
    //    warning sink (per `cancellation.md` §7) so adapters that
    //    publish per-entity diagnostics can attach them to the
    //    per-query stream.
    let mut operator = bind_physical(&physical, db, ctx)?;

    // 4. Drive to completion with the standard "primary error wins"
    //    cleanup convention: `close` runs even on the error path so
    //    mmap handles / spill files are released promptly.
    let drive_result = drive_to_completion(operator.as_mut());
    let close_result = operator.close();
    let rows = drive_result?;
    close_result?;

    // Drop the operator tree before draining the warning sink so any
    // adapter clones of the sink are released first (matches
    // `cancellation.md` §5.1's leaf-first teardown ordering).
    drop(operator);

    Ok(ExecutionResult {
        schema,
        rows,
        rows_affected: None,
        peak_memory_bytes: ctx.peak_memory_bytes(),
        warnings: ctx.warnings().clone().into_warnings(),
    })
}

/// Extract a human-readable message from a `catch_unwind` payload.
/// Panic payloads are commonly `&'static str` or `String`; everything
/// else stringifies as a placeholder per
/// `docs/design/engine/cancellation.md` §4.1.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}

/// Open the operator and pull every batch until exhaustion.
///
/// Returns the collected batches on success. On any operator error,
/// the caller is still responsible for calling `close` — this helper
/// does not, so that `Engine::query` can guarantee `close` runs on
/// both the happy and sad paths without double-closing here.
fn drive_to_completion(
    operator: &mut dyn bqlite_operators::PhysicalOperator,
) -> bqlite_core::Result<Vec<RecordBatch>> {
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
    use crate::context::MIN_QUERY_BUDGET_BYTES;

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
            Err(ExecutionFailure {
                error: BqliteError::Parse(msg),
                ..
            }) => {
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
            Err(ExecutionFailure {
                error: BqliteError::Parse(_),
                ..
            }) => {}
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
            Err(ExecutionFailure {
                error: BqliteError::Plan(msg),
                ..
            }) => {
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

    // ── QueryContext / EngineConfig wiring (TASK-510) ───────────────

    #[test]
    fn engine_default_config_matches_design_defaults() {
        let engine = Engine::new();
        // Default per-query budget is 3 GiB per design § 2.2.
        assert_eq!(engine.config().query_memory_budget_bytes, 3 << 30);
    }

    #[test]
    fn query_reports_zero_peak_memory_when_no_operator_reserves() {
        // No operator yet calls try_reserve against the QueryContext
        // (operator-side wiring is TASK-512/513/514). Until then,
        // peak_memory_bytes is `Some(0)` for every query that runs
        // through the real tracker — the tracker is *present*, just
        // not yet *charged*.
        let scratch = Scratch::new("peak-zero");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let result = engine.query("events", &mut db).expect("must succeed");
        assert_eq!(result.peak_memory_bytes, Some(0));
    }

    #[test]
    fn query_with_options_below_floor_is_rejected() {
        let scratch = Scratch::new("floor-reject");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES - 1),
        };
        match engine.query_with_options("events", &mut db, &opts) {
            Err(ExecutionFailure {
                error: BqliteError::Execution(msg),
                ..
            }) => {
                assert!(
                    msg.contains("query memory budget too small"),
                    "error should mention the floor: {msg}"
                );
            }
            other => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn query_with_options_at_floor_is_accepted() {
        let scratch = Scratch::new("floor-accept");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
        };
        let result = engine
            .query_with_options("events", &mut db, &opts)
            .expect("must succeed at the floor");
        assert!(result.is_empty());
        // The peak is still tracked even though no operator reserved.
        assert_eq!(result.peak_memory_bytes, Some(0));
    }

    #[test]
    fn engine_with_config_threads_default_through_query() {
        // A custom EngineConfig is honoured by both `query` (default
        // options) and `query_with_options` (no override).
        let scratch = Scratch::new("custom-config");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::with_config(crate::EngineConfig {
            query_memory_budget_bytes: MIN_QUERY_BUDGET_BYTES,
            ..crate::EngineConfig::default()
        });
        assert_eq!(
            engine.config().query_memory_budget_bytes,
            MIN_QUERY_BUDGET_BYTES
        );
        let result = engine.query("events", &mut db).expect("must succeed");
        assert!(result.is_empty());
    }
}
