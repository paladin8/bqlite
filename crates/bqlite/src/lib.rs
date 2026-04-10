//! # bqlite
//!
//! Embeddable behavioral query engine for temporal event streams.
//!
//! bqlite is a query engine purpose-built for temporal pattern queries over
//! ordered event streams partitioned by entity. It provides:
//! - **Sequence matching**: ordered event pattern matching with time windows
//! - **Funnels**: stepwise conversion analysis with held properties
//! - **Retention**: configurable retention matrices over custom intervals
//! - **Sessions**: inactivity-based and event-based session segmentation
//! - **Path analysis**: Sankey-style flow aggregation
//! - **Cohort analysis**: behavioral cohort materialization
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use bqlite::Database;
//!
//! let db = Database::open("./my_database/")?;
//! let results = db.query("match(signup -> purchase) within 7d by user_id")?;
//! ```
//!
//! This is the top-level re-export crate. It re-exports the public API from
//! the internal crates (`bqlite-core`, `bqlite-ast`, `bqlite-parser`, `bqlite-engine`).

pub use bqlite_ast as ast;
pub use bqlite_core as types;
pub use bqlite_engine as engine;
pub use bqlite_parser as parser;

// Re-export the error type and Result alias so users can write
// `use bqlite::{BqliteError, Result}` without drilling into internal crates.
pub use bqlite_core::{BqliteError, Result};
