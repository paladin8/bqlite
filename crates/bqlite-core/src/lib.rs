//! # bqlite-core
//!
//! Core types for the bqlite behavioral query engine.
//!
//! This crate defines the foundational types shared across all bqlite crates:
//! - `Event`: a timestamped occurrence with typed properties
//! - `Entity`: the thing events happen to (user, device, server, etc.)
//! - `Schema` / `TableSchema`: typed column definitions for event tables
//! - `Timestamp`: nanosecond-precision timestamps
//! - `PropertyValue`: dynamically typed event property values
//! - `EntityEventStream`: ordered stream of events for a single entity
//!
//! This crate has no internal dependencies — it sits at the bottom of the
//! dependency graph and is imported by every other bqlite crate.

pub mod error;
pub use error::{BqliteError, Result};

pub mod time;
pub use time::{Timestamp, TimeRange, duration};
