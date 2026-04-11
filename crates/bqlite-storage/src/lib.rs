//! # bqlite-storage
//!
//! Native storage engine for bqlite.
//!
//! This crate implements the entity-major storage format and all I/O:
//! - **Database directory management**: create, open, lock, inspect databases
//! - **Table metadata**: schema storage and retrieval
//! - **Memtable**: in-memory write buffer sorted by (entity, timestamp)
//! - **WAL**: write-ahead log for crash safety
//! - **Segment management**: immutable sorted segments in native format
//! - **Compaction**: background merge of segments to restore entity-locality
//! - **Ingest**: CSV, JSON, and Parquet → native format conversion
//! - **Merge scanning**: entity-complete stream across multiple segments
//! - **Compression**: dictionary, delta, and general-purpose codecs per column type
//! - **Indexes**: bloom filters on entity keys, zone maps on timestamps
//!
//! # Wave 1 surface
//!
//! Two modules are live so far:
//!
//! - [`manifest`] — the serialized on-disk manifest type ([`Manifest`])
//!   and the Wave 1 format-version / shard-count constants.
//! - [`database`] — [`Database::open_or_create`], which implements the
//!   v0 database-open contract: create the directory, acquire the
//!   advisory lock, and read or initialize `manifest.json` atomically.
//!
//! Real segment I/O, encodings, compaction, and ingest land in later
//! waves; the Wave 1 surface is what the smoke test (TASK-123) and the
//! planner/operator stubs (TASK-115, TASK-117) need to wire their
//! end-to-end paths.

pub mod database;
pub mod manifest;

pub use database::{
    empty_segment_reader, Database, LOCK_FILE_NAME, MANIFEST_FILE_NAME, MANIFEST_TMP_FILE_NAME,
};
pub use manifest::{
    Manifest, SegmentEntry, TableEntry, DEFAULT_SHARD_COUNT, MANIFEST_FORMAT_VERSION,
};
