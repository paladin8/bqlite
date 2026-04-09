//! # bqlite-parser
//!
//! BQL text parser.
//!
//! Parses BQL query text into the AST types defined in `bqlite-ast`.
//! The parser handles:
//! - Pattern expressions: `match(A -> B -> C)`
//! - Time windows: `within 7d`
//! - Entity grouping: `by user_id`
//! - Property predicates: `WHERE amount > 50`, `WHERE query ~= ".*shoes.*"`
//! - Pipe operators: `| stats count, avg(duration)`
//! - Convenience wrappers: `funnel(...)`, `retention(...)`, `sessionize(...)`
//! - DDL/DML: `CREATE TABLE`, `INSERT INTO`, `DELETE FROM`
//!
//! The parser produces the same AST as the Rust and Python builder APIs.
