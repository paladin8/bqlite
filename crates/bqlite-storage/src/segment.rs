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
//!   [`layout::ColumnChunkMeta`], [`layout::SegmentDictRef`], and the
//!   [`layout::CompressionType`] discriminant. These types are
//!   `Serialize + Deserialize` via `postcard`; the footer body is
//!   nothing more than `postcard::to_allocvec(&FooterV1)` per §10.1.
//!   Also exposes the format-wide constants ([`layout::MAGIC`],
//!   [`layout::SEGMENT_FORMAT_VERSION`], [`layout::ROW_GROUP_SIZE_DEFAULT`],
//!   [`layout::CHECKSUM_SEED`]).
//!
//! TASK-213 lands the writer in `segment::writer`; TASK-215 lands the
//! reader in `segment::reader`. Both tasks build on top of [`layout`]
//! without reshaping it — the on-disk format is frozen for Wave 2.

pub mod layout;
