//! # bqlite-engine
//!
//! Execution orchestration for bqlite.
//!
//! This crate ties together the planner, operators, and storage layer:
//! - **Plan execution**: takes a physical plan and executes it against a database
//! - **Memory management**: enforces configurable memory budgets (default 4 GB)
//! - **Spill-to-disk**: sorted temporary files for intermediate results exceeding budget
//! - **Parallel execution**: entity-partitioned work distribution across threads
//! - **Slab allocator**: optional custom allocator for operator state (if profiling warrants)
//!
//! Results are returned as Arrow RecordBatches for zero-copy interop.
