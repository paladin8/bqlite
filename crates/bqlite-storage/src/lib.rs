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
