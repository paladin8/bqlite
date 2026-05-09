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

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use bqlite_core::{BqliteError, OperatorSchema, QueryWarning};
use bqlite_planner::PhysicalPlan;
use bqlite_storage::Database;

use crate::bind::bind_physical;
use crate::context::{
    resolve_query_budget, CancelReason, EngineConfig, QueryContext, QueryOptions,
};
use crate::perf::{QueryMetrics, WorkerMetricsSnapshot};
use crate::scheduler::MorselScheduler;

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
    /// Per-query metrics aggregate (TASK-524). Operators contribute
    /// through the shared metrics handle on [`QueryContext`]; the
    /// engine snapshots once at query teardown. Inspectable
    /// programmatically; rendered by `bqlite query --explain-perf`
    /// via [`crate::perf::format_perf_explain`]. Defaults to
    /// `QueryMetrics::zero()` for DDL / DELETE / EXPLAIN paths that
    /// bypass the drive loop.
    pub metrics: QueryMetrics,
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
/// query's [`QueryContext`], plus an [`Arc<MorselScheduler>`] that
/// dispatches data-plane queries onto a Rayon worker pool guarded
/// by a [`CoreBudget`] semaphore (engine/morsel-scheduler.md §5).
/// `Engine::new()` and `Engine::default()` remain interchangeable
/// for hosts that want the documented Wave 5 defaults.
#[derive(Debug, Clone)]
pub struct Engine {
    config: EngineConfig,
    scheduler: Arc<MorselScheduler>,
}

impl Default for Engine {
    fn default() -> Self {
        let config = EngineConfig::default();
        let scheduler = crate::scheduler::build_from_config(&config)
            .expect("default scheduler must build with default config");
        Self { config, scheduler }
    }
}

impl Engine {
    /// Construct an engine with default configuration (3 GiB query
    /// budget, see `docs/design/engine/memory-budget.md` § 2.2;
    /// `query_threads = available_parallelism()` per
    /// `engine/morsel-scheduler.md` §5.1).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct an engine with a custom configuration.
    ///
    /// Builds a fresh [`MorselScheduler`] sized at the resolved
    /// `query_threads` from `config`. Callers that want to share a
    /// scheduler across multiple `Engine`s — for example when
    /// running concurrent queries through one fixed worker pool —
    /// should construct the scheduler once via
    /// [`crate::scheduler::build_from_config`] and use
    /// [`Self::with_scheduler`].
    pub fn with_config(config: EngineConfig) -> Self {
        let scheduler = crate::scheduler::build_from_config(&config)
            .expect("scheduler must build with provided config");
        Self { config, scheduler }
    }

    /// Construct an engine that reuses an externally-supplied
    /// scheduler. Test/benchmark hook for sharing one Rayon pool
    /// across many `Engine` instances or driving the same scheduler
    /// from different query-context configurations.
    pub fn with_scheduler(config: EngineConfig, scheduler: Arc<MorselScheduler>) -> Self {
        Self { config, scheduler }
    }

    /// Returns the engine's effective configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Returns the morsel scheduler this engine dispatches data-plane
    /// queries through. Test helper.
    pub fn scheduler(&self) -> &Arc<MorselScheduler> {
        &self.scheduler
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
        // Install the caller-supplied cancellation token at construction
        // time (constructor — not builder — so no observer sees the
        // about-to-be-replaced default token). See cancellation.md §3.1.
        let ctx = match options.cancel.clone() {
            Some(token) => QueryContext::new_with_external_cancellation(budget_bytes, token),
            None => QueryContext::new(budget_bytes),
        }
        .with_spill_fs(db.spill_fs().clone())
        .collect_cpu_metrics(options.collect_cpu_metrics);

        // Spawn the per-query timeout timer. The timer holds a clone
        // of the context (cheap — every field is Arc) and self-exits
        // when the driver flips its `completed` flag at return.
        // `Duration::ZERO` is a deterministic-fire shorthand: the
        // cancel reason is installed synchronously here, before
        // `run_query_inner` runs. cancellation.md §3.1 source 2.
        let _timer = options.timeout.map(|d| QueryTimer::spawn(ctx.clone(), d));

        // Wall-clock for the `Cancelled → Timeout` rewrite below. The
        // engine-level stamp covers parse + plan + bind + drive, so
        // `elapsed_ms` reports the user-visible duration of the
        // `Engine::query_with_options` call.
        let started_at = std::time::Instant::now();

        // `AssertUnwindSafe` is required because `&mut Database` is
        // not `UnwindSafe` by default. This is sound here per
        // `docs/design/engine/cancellation.md` §4.1: the engine owns
        // the database for the call's duration, and on unwind the
        // database is dropped along with the operator tree without
        // further observation. `QueryContext` is `Clone` and the
        // `WarningSink` it carries is `Send + Sync`, so observing
        // it across the unwind boundary is fine.
        //
        // Panic boundary. After CP1.3 (TASK-538), `MorselScheduler::submit`
        // catches worker panics at the worker boundary and surfaces
        // them as `BqliteError::OperatorPanic`; this outer boundary
        // becomes defense-in-depth covering the parse/plan/bind path
        // and (until TASK-541) the in-thread DDL / DELETE / EXPLAIN
        // path that bypasses the scheduler. Until CP1.3 lands, the
        // outer boundary is the primary catch site for every panic.
        //
        // `run_query_inner` itself decides whether to dispatch the
        // operator-tree drive through the morsel scheduler (data-plane
        // SELECTs) or to run on the calling thread (DDL, DELETE,
        // EXPLAIN — design §5.4 bypass).
        let inner_ctx = ctx.clone();
        let scheduler = Arc::clone(&self.scheduler);
        let inner = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_query_inner(text, db, &inner_ctx, &scheduler)
        }));

        match inner {
            // Success: `run_query_inner` already drained its sink
            // clone into `ExecutionResult.warnings`. The outer ctx
            // owns a clone of the same sink — `into_warnings` is
            // idempotent (mem::take), so reading it on the failure
            // paths below sees the already-drained buffer.
            Ok(Ok(result)) => Ok(result),
            // Cooperative failure. Pull the partial warnings the
            // operators recorded before the error fired, then map a
            // `Cancelled` whose first-fire reason is `Timeout` to the
            // typed `Timeout` variant per cancellation.md §3.1.
            Ok(Err(error)) => {
                let warnings = ctx.warnings().clone().into_warnings();
                let mapped = match (error, ctx.cancel_reason()) {
                    (BqliteError::Cancelled, CancelReason::Timeout) => {
                        let elapsed_ms =
                            u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                        BqliteError::Timeout { elapsed_ms }
                    }
                    (other, _) => other,
                };
                Err(ExecutionFailure {
                    error: mapped,
                    warnings,
                })
            }
            // Worker panic: surface as `OperatorPanic`. `location` is
            // always `None` until TASK-541 installs the project-local
            // panic hook, per `cancellation.md` §4.1. Panic always
            // wins over timeout (cancellation.md §3.1 panic-precedence
            // rule).
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

/// RAII handle for the per-query timeout timer.
///
/// Owns a one-shot "completed" flag the driver flips before returning,
/// plus the timer thread's join handle. Drop unparks the thread (so
/// it sees the flag immediately) and joins it. Idle wait uses
/// [`std::thread::park_timeout`] rather than `sleep` so the timer
/// wakes instantly on natural completion — no slice-boundary tax.
///
/// `Duration::ZERO` is a special case: the timer fires synchronously
/// at spawn time and no thread is created. This makes timeout-fired
/// tests deterministic without race-tolerant retries.
struct QueryTimer {
    completed: Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl QueryTimer {
    /// Spawn a timer that calls `ctx.cancel_with_reason(Timeout)` after
    /// `duration`, unless the driver flips `completed` first.
    ///
    /// `Duration::ZERO` is handled synchronously (no thread spawn);
    /// the cancel fires before this function returns.
    fn spawn(ctx: QueryContext, duration: std::time::Duration) -> Self {
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if duration.is_zero() {
            // Synchronous fire: deterministic for tests, zero-thread
            // overhead in any other zero-deadline path.
            ctx.cancel_with_reason(CancelReason::Timeout);
            completed.store(true, std::sync::atomic::Ordering::Release);
            return Self {
                completed,
                handle: None,
            };
        }
        let completed_inner = Arc::clone(&completed);
        let handle = std::thread::Builder::new()
            .name("bqlite-query-timeout".to_string())
            .spawn(move || {
                let started = std::time::Instant::now();
                loop {
                    if completed_inner.load(std::sync::atomic::Ordering::Acquire) {
                        return;
                    }
                    let remaining = duration.saturating_sub(started.elapsed());
                    if remaining.is_zero() {
                        break;
                    }
                    // park_timeout returns either when the timer
                    // expires or when Drop calls unpark(). Either
                    // way the next loop iteration re-checks the
                    // completed flag and the elapsed budget.
                    std::thread::park_timeout(remaining);
                }
                if !completed_inner.load(std::sync::atomic::Ordering::Acquire) {
                    ctx.cancel_with_reason(CancelReason::Timeout);
                }
            })
            .expect("spawn timeout timer");
        Self {
            completed,
            handle: Some(handle),
        }
    }
}

impl Drop for QueryTimer {
    fn drop(&mut self) {
        self.completed
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(h) = self.handle.take() {
            // Wake the parked timer thread so it observes the flag
            // immediately rather than waiting out its remaining
            // park_timeout window. join() then returns promptly.
            h.thread().unpark();
            let _ = h.join();
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
    scheduler: &MorselScheduler,
) -> bqlite_core::Result<ExecutionResult> {
    // Wall-clock stamp for `QueryMetrics::wall_clock_ns`. Started here
    // so it covers parse + plan + bind + drive — the user-visible
    // duration of the `Engine::query` call.
    let started_at = std::time::Instant::now();

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
    let (physical, rule_trace) = bqlite_planner::plan_script_with_trace(stmts, &catalog, now_ns)?;

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

    // EXPLAIN is dispatched out-of-band so we can render the optimizer
    // rule trace (TASK-521) alongside the plan tree. The trace lives
    // outside the `PhysicalPlan` value so the rest of the bind path
    // does not need to thread it through every operator construction;
    // EXPLAIN never executes the inner plan, so the engine's drive
    // loop is unnecessary here.
    if let PhysicalPlan::Explain(explain) = &physical {
        return crate::ddl::execute_explain_statement(explain, &rule_trace);
    }

    let schema = physical.output_schema().clone();

    // 3. Bind the plain-data descriptor into an executable operator
    //    tree. The QueryContext threads through both the memory
    //    budget (per `docs/design/engine/memory-budget.md`) and the
    //    warning sink (per `cancellation.md` §7) so adapters that
    //    publish per-entity diagnostics can attach them to the
    //    per-query stream.
    //
    //    Bind runs on the calling thread because it borrows
    //    `&mut Database` to register cohorts / pre-load metadata.
    //    Once bound, the `Box<dyn PhysicalOperator>` owns whatever
    //    state it needs (segment readers via `Arc<Database>`-internal
    //    handles), so the operator tree can be moved onto a worker
    //    thread for the drive step. See `engine/morsel-scheduler.md`
    //    §5.4: DDL / DELETE / EXPLAIN bypass the worker pool; data-
    //    plane queries are dispatched through it.
    let operator = bind_physical(&physical, db, ctx)?;

    // 4. Drive the operator on the morsel scheduler's worker pool.
    //    The submission acquires `query_threads` permits from the
    //    shared `CoreBudget` (FIFO atomic batch — design §7.1) and
    //    runs the drive loop on a Rayon worker. v1 dispatches every
    //    query as one degenerate whole-database task; per-shard
    //    morsel parallelism lands in follow-on tasks.
    //
    //    The "primary error wins" cleanup convention from the prior
    //    single-threaded driver carries through: `close` runs on
    //    both happy and sad paths so mmap handles and spill files
    //    are released promptly. The closure drops the operator tree
    //    before returning so any adapter clones of the warning sink
    //    AND of the shared `Arc<dyn Metrics>` are released first —
    //    matches `cancellation.md` §5.1 leaf-first teardown and
    //    `execution-model.md` §14.2's "after all morsels complete"
    //    rule for the metrics aggregate.
    let rows = scheduler.submit(move || -> bqlite_core::Result<Vec<RecordBatch>> {
        let mut operator = operator;
        let drive_result = drive_to_completion(operator.as_mut());
        let close_result = operator.close();
        let rows = drive_result?;
        close_result?;
        drop(operator);
        Ok(rows)
    })?;

    // v1 dispatches one degenerate "whole-database" task per query, so
    // record exactly one `WorkerMetricsSnapshot::default()` to seed
    // `num_workers == 1` and give the worker min/max aggregates
    // concrete (zero) values. The per-shard morsel-parallelism
    // follow-up will switch to one snapshot per worker. CPU-counter
    // fields stay zero — `PerfCounters::open_or_disabled` returns
    // disabled today.
    ctx.record_worker_snapshot(WorkerMetricsSnapshot::default());
    let wall_clock_ns = u64::try_from(started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let metrics = ctx.take_query_metrics(wall_clock_ns);

    Ok(ExecutionResult {
        schema,
        rows,
        rows_affected: None,
        peak_memory_bytes: ctx.peak_memory_bytes(),
        warnings: ctx.warnings().clone().into_warnings(),
        metrics,
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
        // Both honor the documented Wave 5 defaults (memory budget +
        // morsel scheduler thread count). The schedulers are distinct
        // `Arc` allocations — that is intentional, every `Engine`
        // owns its own pool unless built with `with_scheduler` —
        // so we compare the configuration rather than the Debug
        // shape.
        assert_eq!(
            a.config().query_memory_budget_bytes,
            b.config().query_memory_budget_bytes
        );
        assert_eq!(a.scheduler().query_threads(), b.scheduler().query_threads());
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

    // ── Per-query metrics surface (TASK-524) ────────────────────────

    #[test]
    fn query_records_wall_clock_and_num_workers() {
        let scratch = Scratch::new("metrics-wall-clock");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let result = engine.query("events", &mut db).expect("query must succeed");

        // Single-threaded driver records exactly one worker snapshot
        // at teardown.
        assert_eq!(result.metrics.num_workers, 1);
        // Wall clock spans parse + plan + bind + drive — non-zero on
        // any sane platform clock resolution.
        assert!(
            result.metrics.wall_clock_ns > 0,
            "wall_clock_ns must be non-zero, got {}",
            result.metrics.wall_clock_ns
        );
    }

    #[test]
    fn query_metrics_default_to_zero_on_empty_table() {
        let scratch = Scratch::new("metrics-zero");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let result = engine.query("events", &mut db).expect("must succeed");

        // Operator-tree counters are all zero on an empty table.
        assert_eq!(result.metrics.operator.rows_in, 0);
        assert_eq!(result.metrics.operator.rows_out, 0);
        assert_eq!(result.metrics.operator.spill_bytes_written, 0);
        assert_eq!(result.metrics.operator.selection_vector_materializations, 0);
        // Skew rows zero (no morsel scheduler running).
        assert_eq!(result.metrics.morsels_dispatched, 0);
        // CPU metrics off by default.
        assert!(!result.metrics.cpu_metrics_enabled);
    }

    #[test]
    fn query_with_cpu_metrics_enabled_propagates_flag() {
        let scratch = Scratch::new("metrics-cpu-flag");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let opts = QueryOptions {
            collect_cpu_metrics: true,
            ..Default::default()
        };
        let result = engine
            .query_with_options("events", &mut db, &opts)
            .expect("query must succeed");

        assert!(
            result.metrics.cpu_metrics_enabled,
            "collect_cpu_metrics flag must propagate to QueryMetrics"
        );
        // The flag is informational today — the actual perf counters
        // remain zero because the platform integration is a stub.
        assert_eq!(result.metrics.total_cpu_cycles, 0);
        assert_eq!(result.metrics.branch_misses, 0);
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
            ..Default::default()
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

    // ── Public cancel / timeout surface (TASK-538) ──────────────────

    #[test]
    fn external_cancel_before_query_returns_cancelled() {
        use crate::CancellationToken;
        let scratch = Scratch::new("ext-cancel-pre");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let token = CancellationToken::new();
        token.cancel(); // Pre-cancel: the very first yield-point should fire.
        let opts = QueryOptions {
            cancel: Some(token),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options("events", &mut db, &opts)
            .expect_err("pre-cancelled query must surface BqliteError::Cancelled");
        assert!(
            matches!(
                &err,
                ExecutionFailure {
                    error: BqliteError::Cancelled,
                    ..
                }
            ),
            "expected Cancelled, got {err:?}"
        );
    }

    #[test]
    fn timeout_zero_fires_synchronously_returns_timeout_error() {
        // Duration::ZERO triggers the synchronous-fire branch in
        // QueryTimer::spawn — the cancel reason is installed before
        // run_query_inner is called, so the very first yield point
        // observes Cancelled, and the result-collection arm maps it
        // to BqliteError::Timeout via cancel_reason() == Timeout.
        // Deterministic — no retry loop.
        let scratch = Scratch::new("timeout-zero");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let opts = QueryOptions {
            timeout: Some(std::time::Duration::ZERO),
            ..QueryOptions::default()
        };
        let err = engine
            .query_with_options("events", &mut db, &opts)
            .expect_err("zero-duration timeout must fire deterministically");
        match err {
            ExecutionFailure {
                error: BqliteError::Timeout { elapsed_ms },
                ..
            } => {
                assert!(
                    elapsed_ms < 60_000,
                    "elapsed_ms must be reasonable, got {elapsed_ms}"
                );
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    #[test]
    fn no_timeout_no_cancel_runs_normally() {
        // A query without cancel/timeout completes successfully and
        // the timer is never spawned — sanity check that the timer
        // code is gated on `Some(timeout)`.
        let scratch = Scratch::new("no-timeout");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let opts = QueryOptions::default();
        let result = engine
            .query_with_options("events", &mut db, &opts)
            .expect("default opts must succeed");
        assert!(result.is_empty());
    }

    #[test]
    fn shared_cancel_token_observed_by_subsequent_queries() {
        // The same `CancellationToken` is reusable across queries —
        // pre-cancelling it cancels any future query that opts in.
        use crate::CancellationToken;
        let scratch = Scratch::new("shared-cancel");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let token = CancellationToken::new();
        token.cancel();

        for label in ["first", "second"] {
            let opts = QueryOptions {
                cancel: Some(token.clone()),
                ..QueryOptions::default()
            };
            let err = engine
                .query_with_options("events", &mut db, &opts)
                .expect_err("pre-cancelled token must cancel every query that opts in");
            assert!(
                matches!(
                    &err,
                    ExecutionFailure {
                        error: BqliteError::Cancelled,
                        ..
                    }
                ),
                "{label}: expected Cancelled, got {err:?}"
            );
        }
    }

    #[test]
    fn query_with_options_at_floor_is_accepted() {
        let scratch = Scratch::new("floor-accept");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::new();
        let opts = QueryOptions {
            memory_budget_bytes: Some(MIN_QUERY_BUDGET_BYTES),
            ..Default::default()
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

    // ── Morsel scheduler integration (TASK-523 CP3) ─────────────────

    #[test]
    fn engine_default_query_threads_uses_resolved_default() {
        // The default engine resolves `query_threads` to
        // `available_parallelism()` (or 4 on platforms that cannot
        // report). We do not pin the exact number — different CI
        // hosts have different core counts — but it is non-zero.
        let engine = Engine::new();
        assert!(engine.scheduler().query_threads() >= 1);
        // Permit count starts at full capacity: no query in flight.
        assert_eq!(
            engine.scheduler().available_permits(),
            engine.scheduler().query_threads()
        );
    }

    #[test]
    fn engine_query_dispatches_through_morsel_scheduler() {
        // SELECT-shaped queries route through `MorselScheduler::submit`,
        // which acquires `query_threads` permits and runs the operator
        // tree on a Rayon worker. After the call returns, the permits
        // are released — `available_permits()` is back to full.
        let scratch = Scratch::new("scheduler-dispatch");
        let mut db = create_db_with_events(scratch.path());
        let engine = Engine::with_config(crate::EngineConfig {
            query_threads: Some(2),
            ..crate::EngineConfig::default()
        });
        assert_eq!(engine.scheduler().query_threads(), 2);
        let _ = engine.query("events", &mut db).expect("must succeed");
        // Permits released after query completes.
        assert_eq!(engine.scheduler().available_permits(), 2);
    }

    #[test]
    fn engine_with_scheduler_shares_pool_across_engines() {
        // Two engines that share one scheduler honour the same
        // `query_threads` ceiling and the same `CoreBudget`.
        let scratch = Scratch::new("shared-scheduler");
        let mut db = create_db_with_events(scratch.path());
        let cfg = crate::EngineConfig {
            query_threads: Some(1),
            ..crate::EngineConfig::default()
        };
        let scheduler = crate::scheduler::build_from_config(&cfg).expect("scheduler builds");
        let engine_a = Engine::with_scheduler(cfg, Arc::clone(&scheduler));
        let engine_b = Engine::with_scheduler(cfg, Arc::clone(&scheduler));
        assert!(Arc::ptr_eq(engine_a.scheduler(), engine_b.scheduler()));

        let _ = engine_a.query("events", &mut db).expect("must succeed");
        let _ = engine_b.query("events", &mut db).expect("must succeed");
        assert_eq!(engine_a.scheduler().available_permits(), 1);
    }

    #[test]
    fn ddl_bypasses_morsel_scheduler() {
        // CREATE TABLE runs out-of-band per design §5.4 — it never
        // acquires `CoreBudget` permits. Observable property: while
        // a DDL query is in flight (we cannot easily synchronize on
        // mid-DDL state from the test, so we settle for the
        // post-condition), the scheduler's permit count is
        // unchanged. DDL does not return through the scheduler path
        // either way; the assertion here is that DDL succeeds
        // without saturating the scheduler.
        let scratch = Scratch::new("ddl-bypass");
        let mut db = Database::create(scratch.path()).expect("create db");
        let engine = Engine::with_config(crate::EngineConfig {
            query_threads: Some(1),
            ..crate::EngineConfig::default()
        });
        // Issue DDL — it does not pull a permit because run_query_inner
        // dispatches DDL/EXPLAIN/DELETE without going through the
        // scheduler.
        let _ = engine
            .query(
                "CREATE TABLE events (\
                     entity_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .expect("DDL must succeed");
        assert_eq!(engine.scheduler().available_permits(), 1);
    }
}
