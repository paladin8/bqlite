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
//! Only the manifest type is implemented so far. [`manifest::Manifest`]
//! describes the serialized on-disk shape; the `Database::open_or_create`
//! bootstrap and real segment I/O land in follow-up checkpoints of
//! TASK-116 and later waves.

pub mod manifest;

pub use manifest::{
    Manifest, SegmentEntry, TableEntry, DEFAULT_SHARD_COUNT, MANIFEST_FORMAT_VERSION,
};
