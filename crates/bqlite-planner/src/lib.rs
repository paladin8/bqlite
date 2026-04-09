//! # bqlite-planner
//!
//! Query planner for bqlite.
//!
//! Transforms an AST into an executable plan through multiple stages:
//! 1. **Logical planning**: AST → logical plan (LogicalMatch, LogicalFunnel, etc.)
//! 2. **Optimization**: predicate pushdown, operator fusion, pattern recognition
//! 3. **Physical planning**: logical plan → physical plan (selects concrete operators)
//!
//! Every plan node declares its output schema (column names and types).
//! Schema compatibility is validated at plan construction time, making
//! operator composition (piping) type-safe.
//!
//! The planner does NOT depend on operator implementations — the mapping
//! from logical to physical is done via trait objects / registry.
