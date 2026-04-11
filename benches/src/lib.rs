//! # bqlite-benches
//!
//! Criterion benchmark harness for the bqlite workspace.
//!
//! This crate has no production code. It exists solely to anchor the
//! shared [`common`] helper module — located at `benches/common/mod.rs`
//! per TASKS.md TASK-121 — so every Criterion bench file under
//! `benches/benches/` can import helpers via
//! `use bqlite_benches::common::*`.
//!
//! The `#[path]` attribute below lets the task-specified directory
//! layout (`benches/common/mod.rs`, sibling to `src/`) stay verbatim
//! while still being a first-class Rust module from the crate root.
//!
//! ## Why the benches live in a workspace member crate
//!
//! A single `bqlite-benches` crate collects every Criterion benchmark
//! for the workspace. The alternative — a `benches/` directory inside
//! each production crate — would force every crate to carry a
//! `criterion` dev-dependency and would scatter the shared fixture and
//! data-generation helpers. Centralizing them here keeps the benchmark
//! surface independent of the production crates and makes it trivial
//! to introduce heavyweight dataset dependencies (e.g. the Wave 2
//! `purchases` reference fixture) without leaking them elsewhere.
//!
//! See `benches/README.md` for the per-bench authoring pattern.

#[path = "../common/mod.rs"]
pub mod common;
