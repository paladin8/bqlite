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
//!
//! TASK-215 lands the reader in `segment::reader` alongside this
//! module; both tasks build on top of [`layout`] without reshaping it
//! — the on-disk format is frozen for Wave 2.

pub mod layout;
pub mod writer;
