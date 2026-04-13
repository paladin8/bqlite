//! # bqlite-tests
//!
//! Workspace-level test helper crate. Hosts the shared fixtures and
//! `proptest` strategies that bqlite's integration and property tests
//! reuse, and is the package whose `tests/` directory contains every
//! workspace-level test binary.
//!
//! ## Layout
//!
//! ```text
//! tests/                     ← package `bqlite-tests`
//! ├── Cargo.toml
//! ├── src/
//! │   ├── lib.rs             ← this file
//! │   ├── common.rs          ← integration helpers (TempDb, assert_*)
//! │   ├── csv.rs             ← deterministic CSV fixture generator (TASK-235)
//! │   ├── jsonl.rs           ← deterministic JSONL fixture generator (TASK-410)
//! │   └── strategies.rs      ← shared `proptest` strategies
//! ├── tests/                 ← Cargo's auto-discovered test binaries
//! │   ├── prop_property_value.rs
//! │   ├── prop_time.rs
//! │   ├── prop_arrow.rs
//! │   ├── common_smoke.rs
//! │   ├── wave2_acceptance.rs
//! │   └── jsonl_ingest.rs    ← JSONL ingest acceptance tests (TASK-410)
//! └── suite/                 ← README-only stubs for future Wave 2+ suites
//! ```
//!
//! Each `.rs` file under `tests/tests/` becomes its own `[[test]]` target
//! via Cargo's default auto-discovery. New test files just drop in — no
//! manifest edit required. Shared helpers and strategies are imported the
//! normal way, e.g.
//!
//! ```ignore
//! use bqlite_tests::common::{assert_batches_eq, TempDb};
//! use bqlite_tests::strategies::{arb_property_value, arb_time_range};
//! ```
//!
//! ## Dep-direction note
//!
//! `scripts/check-dep-direction.sh` walks `crates/*/` only, so this
//! package lives outside the enforced internal-dependency graph by
//! design. Test code is allowed to reach into any crate; adding deps
//! here is not a rule bypass.

pub mod common;
pub mod csv;
pub mod jsonl;
pub mod strategies;
