//! # bqlite CLI
//!
//! Command-line interface for the bqlite behavioral query engine.
//!
//! Commands:
//! - `bqlite init <path>` — initialize a new database
//! - `bqlite schema <path> <ddl>` — create a table with a schema
//! - `bqlite ingest <path> <table> --from <file>` — ingest data into a table
//! - `bqlite query <path> <bql>` — run a BQL query
//! - `bqlite inspect <path> [table]` — inspect database or table metadata
//! - `bqlite compact <path>` — compact storage segments
//! - `bqlite repl <path>` — interactive BQL shell

fn main() {
    println!("bqlite — embeddable behavioral query engine");
    println!("Not yet implemented. See TASKS.md for development roadmap.");
}
