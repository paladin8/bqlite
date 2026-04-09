//! # bqlite-ast
//!
//! Abstract syntax tree types for the BQL query language.
//!
//! This crate defines the AST nodes produced by both the BQL text parser
//! (`bqlite-parser`) and the Rust/Python builder APIs. Both entry points
//! produce the same AST, which is then consumed by the planner.
//!
//! Key AST nodes include:
//! - Pattern expressions (sequence, negation, repetition)
//! - Predicate expressions (property filters, regex, comparisons)
//! - Operator nodes (match, funnel, retention, sessionize, aggregate)
//! - Time window and quantization expressions
//! - Table DDL and DML statements (CREATE TABLE, INSERT, DELETE)
