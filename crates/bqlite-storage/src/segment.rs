//! Segment file I/O — the byte-level on-disk format defined by
//! `docs/design/storage/segment-format-v1.md` (TASK-201).
//!
//! This module owns the **file-format** slice of the storage crate.
//! Encoding of individual column payloads (Plain, Dictionary, Delta,
//! BitPacking, Constant, LZ4) lives in [`crate::encoding`]; this module
//! composes those encoded chunks into the framed container a segment
//! file is, and parses that framing back out.
//!
//! # Wave 2 surface
//!
//! - [`layout`] — plain-data structs that mirror the v1 on-disk layout
//!   one-to-one: [`layout::FooterV1`], [`layout::RowGroupIndex`],
//!   [`layout::ColumnChunkMeta`], [`layout::SegmentDictRef`]. These
//!   types derive `Serialize + Deserialize` so the footer body is
//!   nothing more than `postcard::to_allocvec(&FooterV1)` per §10.1.
//!   Also exposes the format-wide constants ([`layout::MAGIC`],
//!   [`layout::SEGMENT_FORMAT_VERSION`], [`layout::ROW_GROUP_SIZE_DEFAULT`],
//!   [`layout::CHECKSUM_SEED`]) and re-exports
//!   [`layout::CompressionType`] from [`crate::encoding`] so callers
//!   see a single canonical type.
//! - [`writer`] — low-level segment file writer (TASK-213). Accepts a
//!   [`writer::SegmentWriteRequest`] describing pre-encoded column
//!   chunks and emits the v1 byte layout atomically at a path, or
//!   composes the bytes in memory via [`writer::encode_segment`] for
//!   byte-exact testing.
//! - [`reader`] — low-level segment file reader (TASK-215). Opens a
//!   v1 segment file, validates every rule in §15, and exposes the
//!   parsed footer + loaded dictionaries for the scan iterator.
//! - [`merge`] — streaming k-way merge across L0 segment scans
//!   (TASK-219). Each Wave 2 query has to merge across every segment
//!   in its `(shard, window)` snapshot because compaction is not
//!   yet implemented.
//!
//! These tasks build on top of [`layout`] without reshaping it — the
//! on-disk format is frozen for Wave 2.

pub mod layout;
pub mod merge;
pub mod reader;
pub mod writer;
