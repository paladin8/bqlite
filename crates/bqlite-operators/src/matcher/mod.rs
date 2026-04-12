//! Sequence matcher module — NFA runtime, step counter, and variable bindings.
//!
//! This module tree implements the per-event simulation engine that
//! evaluates MATCH pipeline stages. The compiled NFA program
//! ([`bqlite_planner::CompiledNfa`]) is produced by the pattern compiler
//! (TASK-311) and consumed here at runtime.
//!
//! ## Sub-modules
//!
//! - [`nfa`] — General-path NFA runtime simulator (TASK-304). Thompson's
//!   algorithm with candidate-deque propagation, poison transitions,
//!   global time-window enforcement, and EMIT ALL support.

pub mod nfa;
