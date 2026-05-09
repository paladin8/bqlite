//! # bqlite-engine
//!
//! Execution orchestration for bqlite.
//!
//! This crate is the single text-in, rows-out surface that every
//! caller — the CLI (`bqlite-cli`), the Python bindings
//! (`bqlite-ffi`), and eventually the top-level `bqlite` re-export —
//! goes through. Internally it ties together:
//!
//! - [`bqlite_parser`](bqlite_parser) — BQL text → AST
//! - [`bqlite_planner`](bqlite_planner) — AST → plain-data `PhysicalPlan`
//! - [`bqlite_operators`](bqlite_operators) — concrete `PhysicalOperator`
//!   implementations bound from the descriptor tree
//! - [`bqlite_storage`](bqlite_storage) — the `Database` handle that
//!   supplies the catalog and `SegmentReader`s
//!
//! The crate-boundary change that adds `bqlite-parser` as a dependency
//! is intentional — see the Wave 1 note in TASK-118 and the updated
//! dependency graphs in [`docs/architecture.md`](../../docs/architecture.md)
//! and [`CLAUDE.md`](../../CLAUDE.md). Without this edge, every caller
//! would have to import the parser directly, which the architecture
//! forbids (`bqlite-cli` / `bqlite-ffi` only depend on
//! `bqlite-engine`).
//!
//! ## Wave 2 surface (TASK-232)
//!
//! [`Engine::query`] parses the text, plans it against the database's
//! catalog, binds the resulting [`PhysicalPlan`] into a
//! `Box<dyn PhysicalOperator>`, drives it to exhaustion, and returns
//! an [`ExecutionResult`]. The bind step handles:
//!
//! - **Data-plane** (`Scan`, `Filter`, `Project`, `Limit`) —
//!   recursive operator construction.
//! - **DDL** (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`)
//!   — executes during bind via atomic manifest updates.
//! - **Metadata** (`DESCRIBE`, `EXPLAIN`) — builds a result batch
//!   during bind.
//!
//! Memory management, parallelism, cancellation timers, and the
//! metrics bridge (TASK-112) all land in later waves. The engine's
//! public API is deliberately narrow so those extensions can arrive
//! additively without churning callers.
//!
//! ## Re-exports
//!
//! The crate re-exports [`Database`] so that `bqlite-cli` (and any
//! other caller that only depends on `bqlite-engine` per the
//! architecture) can open a database without pulling in
//! `bqlite-storage` directly. It also re-exports [`PhysicalPlan`] so
//! tests and internal callers can pattern-match on bind results
//! without importing `bqlite-planner` directly.

pub mod bind;
pub mod cohort_pushdown;
pub mod context;
pub mod ddl;
pub mod delete;
pub mod ingest;
pub mod perf;
pub mod query;
pub mod render;
pub mod scheduler;
pub mod warning_sink;

pub use bind::bind_physical;
pub use bqlite_operators::CancellationToken;
pub use context::{
    CancelReason, EngineConfig, QueryContext, QueryOptions, DEFAULT_COMPACTION_BUDGET_BYTES,
    DEFAULT_INGEST_BUDGET_BYTES, DEFAULT_QUERY_BUDGET_BYTES, MIN_QUERY_BUDGET_BYTES,
};
pub use perf::{format_perf_explain, PerfCounters, QueryMetrics, WorkerMetricsSnapshot};
pub use query::{Engine, ExecutionFailure, ExecutionResult};
pub use render::{format_result_as_text, format_result_as_text_limited};
pub use warning_sink::{WarningSink, WorkerContext};

// Convenience re-exports so downstream crates only need to depend on
// `bqlite-engine`. This matches the architecture.md rule
// `bqlite-cli → engine` — the CLI must not import `bqlite-storage`,
// `bqlite-planner`, or `bqlite-core` directly. `init_tracing` is
// re-exported here so that `bqlite-cli` (TASK-119) can install the
// global subscriber at startup without reaching past the engine — the
// telemetry module itself lives in `bqlite-core` (TASK-122), but the
// architecture requires the CLI to go through engine for everything.
pub use bqlite_core::telemetry::init_tracing;
pub use bqlite_planner::PhysicalPlan;
pub use bqlite_storage::Database;
