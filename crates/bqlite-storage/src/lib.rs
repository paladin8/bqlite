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
//! # Wave 1 / early Wave 2 surface
//!
//! Live modules so far:
//!
//! - [`manifest`] — the serialized on-disk manifest type ([`Manifest`])
//!   and the Wave 1 format-version / shard-count constants, extended
//!   by TASK-217 with the real segment inventory ([`SegmentMeta`],
//!   [`WindowManifest`], [`ColumnStats`]).
//! - [`database`] — [`Database::open_or_create`], which implements the
//!   v0 database-open contract: create the directory, acquire the
//!   advisory lock, and read or initialize `manifest.json` atomically.
//! - [`catalog`] — the [`ManifestCatalog`] implementation of
//!   [`bqlite_core::Catalog`] and the TASK-125 bootstrap events table
//!   schema. `Database::open_or_create` seeds the bootstrap entry on
//!   fresh init so the planner has a resolvable `events` table.
//! - [`encoding`] — the column [`Encoding`] trait and concrete v1
//!   impls: [`Plain`] (TASK-206), [`Constant`] (TASK-210),
//!   [`BitPacking`] (TASK-209), [`Delta`] (TASK-208),
//!   [`Dictionary`] (TASK-207), and the LZ4 post-encoding wrapper
//!   (TASK-211). The byte layouts produced by every impl are pinned
//!   by `docs/design/storage/segment-format-v1.md` §9.
//! - [`ingest`] — the ingest partitioner landed by TASK-218, which
//!   routes incoming events to the correct `(shard, window)` bucket
//!   and hands sorted batches off to the writer.
//!
//! Segment I/O (TASK-213 / TASK-215) and compaction land in later
//! Wave 2 tasks; the current surface is what the smoke test (TASK-123)
//! and the planner/operator stubs (TASK-115, TASK-117) need to wire
//! their end-to-end paths.

pub mod catalog;
pub mod database;
pub mod encoding;
pub mod ingest;
pub mod manifest;
pub mod segment;
pub mod zone_map;

pub use catalog::{bootstrap_events_schema, ManifestCatalog, BOOTSTRAP_EVENTS_TABLE_NAME};
pub use database::{
    empty_segment_reader, Database, LOCK_FILE_NAME, MANIFEST_FILE_NAME, MANIFEST_TMP_FILE_NAME,
};
pub use encoding::{
    select_encoding, select_encoding_type, BitPacking, CompressionType, Constant, Delta,
    Dictionary, EncodedChunk, Encoding, EncodingType, Plain, SelectedEncoding,
};
pub use manifest::{
    ColumnStats, Manifest, SegmentMeta, TableEntry, WindowManifest, DEFAULT_SHARD_COUNT,
    MANIFEST_FORMAT_VERSION,
};
