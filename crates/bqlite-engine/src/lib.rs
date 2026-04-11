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
//! ## Wave 1 surface
//!
//! Only the happy path matters for the Wave 1 smoke test (TASK-123):
//!
//! 1. [`Engine::query`] parses the text, plans it against the
//!    database's catalog, binds the resulting [`PhysicalPlan`] into a
//!    `Box<dyn PhysicalOperator>`, drives it to exhaustion, and
//!    returns an [`ExecutionResult`].
//! 2. The Wave 1 planner and parser only accept a bare table
//!    reference, so the bind step currently has a single arm
//!    ([`ScanPhysical`]). Wave 2 extends this with filter / project /
//!    limit via TASK-232 and friends.
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
pub mod query;

pub use bind::bind_physical;
pub use query::{Engine, ExecutionResult};

// Convenience re-exports so downstream crates only need to depend on
// `bqlite-engine`. This matches the architecture.md rule
// `bqlite-cli → engine` — the CLI must not import `bqlite-storage` or
// `bqlite-planner` directly.
pub use bqlite_planner::PhysicalPlan;
pub use bqlite_storage::Database;
