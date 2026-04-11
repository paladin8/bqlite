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
//! dependency graph and is imported by every other bqlite crates.

pub mod error;
pub mod telemetry;
pub use error::{BqliteError, Result};

pub mod time;
pub use time::{duration, TimeRange, Timestamp};

pub mod memory;
pub use memory::{MemoryBudget, MemoryReservation, SpillNotification, UnboundedMemory};

pub mod property;
pub use property::{BqlType, PropertyValue};

pub mod event;
pub use event::{EntityEventStream, EntityEventStreamError, EntityId, Event, VecEntityEventStream};

pub mod schema;
pub use schema::{
    ColumnDef, OperatorSchema, TableSchema, BATCH_ID_COLUMN, SEQ_ID_COLUMN, SYSTEM_COLUMN_PREFIX,
};

pub mod arrow;
pub use arrow::{
    arrow_to_bql_type, bql_type_to_arrow, column_def_to_field, property_value_to_arrow_type,
    table_schema_to_arrow,
};
