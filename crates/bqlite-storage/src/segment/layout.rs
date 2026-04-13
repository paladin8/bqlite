//! Plain-data types that mirror the v1 segment file layout.
//!
//! Every struct in this module is a direct translation of a section of
//! `docs/design/storage/segment-format-v1.md` §4 – §13. They are pure
//! data — no logic, no invariants enforced in constructors — so that
//! both the writer (TASK-213) and the reader (TASK-215) can trivially
//! compose or deconstruct them without impedance between the two.
//!
//! # Framing vs. footer body
//!
//! The segment file has two distinct serialization layers:
//!
//! - **Framing** — the 6-byte file header, the variable-length row
//!   groups, the segment-dictionaries region, the 8-byte checksum, and
//!   the 8-byte trailer. This layer is **hand-written** little-endian
//!   bytes; `postcard` is not involved. The writer emits these bytes
//!   directly; the reader parses them with `u16::from_le_bytes` /
//!   `u32::from_le_bytes` / `u64::from_le_bytes`.
//!
//! - **Footer body** — the variable-length block of structured metadata
//!   that closes the file (§10.1). This layer is a single
//!   `postcard::to_allocvec(&FooterV1)` call, per §10.4.
//!
//! Splitting the two is what keeps the writer simple — the framing is
//! 30 lines of hand-written `Vec::extend_from_slice` calls; the
//! footer body is one `postcard::to_allocvec`.
//!
//! # Cross-doc consistency
//!
//! If you change a field in [`FooterV1`] or [`ColumnChunkMeta`],
//! **also update** the field list in `segment-format-v1.md` §10.1 /
//! §10.2. The design doc is the authoritative contract; this module is
//! the runtime counterpart. Drift between the two is the single most
//! likely source of Wave 2 storage bugs.

use bqlite_core::{BqlType, PropertyValue, TableSchema};
use serde::{Deserialize, Serialize};

// The compression discriminant for a column chunk's payload (§8). Owned
// by [`crate::encoding::CompressionType`] because TASK-211 landed the
// codec and the discriminant enum together, and having two enums with
// the same name in the same crate would be a trap. Re-exported here so
// callers constructing segment layout values see the same type name in
// both places.
pub use crate::encoding::CompressionType;

// ─────────────────────────────────────────────────────────────────────────────
// Format-wide constants (§4)
// ─────────────────────────────────────────────────────────────────────────────

/// File magic bytes. ASCII `BQLT`. Appears at offset 0 (file header)
/// and again in the trailer. Matches `segment-format-v1.md` §4.
pub const MAGIC: [u8; 4] = *b"BQLT";

/// On-disk format version. v1 writers emit `1`; v1 readers reject any
/// other value as [`bqlite_core::BqliteError`] corruption.
pub const SEGMENT_FORMAT_VERSION: u16 = 1;

/// Alias for the v1 format version, for clarity when both versions
/// are in scope. The original [`SEGMENT_FORMAT_VERSION`] name is
/// retained so existing code that refers to it (which always means
/// "the v1 constant") is unambiguous.
pub const SEGMENT_FORMAT_VERSION_V1: u16 = 1;

/// On-disk format version for v2 segments. v2 extends v1 with six
/// new encoding discriminants and the FSST symbol tables region.
/// See `docs/design/storage/segment-format-v2.md` §3.
pub const SEGMENT_FORMAT_VERSION_V2: u16 = 2;

/// Default row-group size in rows. v1 writers always emit exactly this
/// many rows per row group, with one exception documented in §6: the
/// **last** row group in a segment may be short when the input does
/// not divide evenly. The value is recorded in
/// [`FooterV1::row_group_size_hint`] so later waves can vary it without
/// bumping the format version.
pub const ROW_GROUP_SIZE_DEFAULT: u32 = 65_536;

/// Seed passed to `twox_hash::XxHash64` when computing the segment-level
/// checksum (§12). Fixed at the xxHash64 default (`0`) so checksums are
/// reproducible across bqlite versions.
pub const CHECKSUM_SEED: u64 = 0;

/// Byte width of the file header (`magic[4] + version[2]`, §5).
pub const FILE_HEADER_LEN: usize = 6;

/// Byte width of the segment checksum (`u64 LE`, §12).
pub const CHECKSUM_LEN: usize = 8;

/// Byte width of the trailer (`footer_body_length: u32 LE + magic[4]`,
/// §13).
pub const TRAILER_LEN: usize = 8;

/// Total bytes the reader needs to address the file footer: the
/// checksum plus the trailer. A segment must be at least
/// `FILE_HEADER_LEN + CHECKSUM_LEN + TRAILER_LEN = 22` bytes long to be
/// a valid v1 file (§15 rule 1).
pub const FOOTER_SUFFIX_LEN: usize = CHECKSUM_LEN + TRAILER_LEN;

// ─────────────────────────────────────────────────────────────────────────────
// Footer body (§10.1 – §10.3)
// ─────────────────────────────────────────────────────────────────────────────

/// The segment footer body — everything the reader needs after framing
/// validation (§10.1).
///
/// Serialized once per segment via `postcard::to_allocvec(&FooterV1)`
/// and written into the file immediately after the segment dictionaries
/// region and before the 8-byte checksum. The `format_version` field is
/// duplicated from the file header so that a stray footer (e.g. in a
/// hex dump) is self-identifying; every v1 reader checks that it
/// equals [`SEGMENT_FORMAT_VERSION`] before trusting the rest of the
/// footer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FooterV1 {
    /// Must equal [`SEGMENT_FORMAT_VERSION`] (1). Redundant with the
    /// file header value but cheap to verify and invaluable for
    /// corruption diagnosis.
    pub format_version: u16,

    /// Full segment schema at write time (type-system.md §5). Readers
    /// use this both for decoding chunks and for backfilling columns
    /// added after the segment was written — see §14 "Schema evolution".
    pub schema: TableSchema,

    /// Schema version the segment was written against. Must match
    /// `schema.version()` at write time; stored redundantly because
    /// later waves may diverge the two.
    pub schema_version: u32,

    /// Total rows in the segment across all row groups. Equal to the
    /// sum of `row_groups[i].row_count`.
    pub row_count: u64,

    /// Number of row groups in [`Self::row_groups`]. Redundant with
    /// `row_groups.len()` but stored explicitly so the reader can
    /// preallocate before deserializing the vec.
    pub row_group_count: u32,

    /// Row-group size the writer used for all-but-the-last row group.
    /// v1 writers always emit [`ROW_GROUP_SIZE_DEFAULT`]; recording it
    /// here lets later waves vary it without bumping the format version.
    pub row_group_size_hint: u32,

    /// Creation timestamp in nanoseconds since epoch UTC
    /// (`SegmentMeta.created_at` in storage-format.md §12.3).
    pub creation_timestamp_ns: i64,

    /// Sequence-ID range covered by this segment as
    /// `(min_inclusive, max_inclusive)`. Both endpoints are actual
    /// `__seq_id` values present in the segment (§6.2). Empty segments
    /// are illegal, so `min_inclusive <= max_inclusive` always holds.
    pub seq_id_range: (u64, u64),

    /// The batch ID this segment was produced from (§6.2).
    pub batch_id: u64,

    /// Compaction tier. `0` for L0 ingest output in Wave 2; bumped by
    /// the Wave 4 compaction scheduler.
    pub compaction_level: u8,

    /// Segment-level dictionaries, one entry per dictionary-encoded
    /// column. Order matches the physical layout in the
    /// segment-dictionaries region (§11); entries are referenced from
    /// [`ColumnChunkMeta`] by index via a `dict_id` carried in the
    /// encoding params block.
    pub dictionaries: Vec<SegmentDictRef>,

    /// Per-row-group index. `row_groups[i]` describes row group `i`,
    /// which is the `i`-th row group in file order starting from
    /// offset [`FILE_HEADER_LEN`].
    pub row_groups: Vec<RowGroupIndex>,
}

/// Metadata for a single row group (§10.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowGroupIndex {
    /// Absolute byte offset of the row group's first column chunk in
    /// the segment file. Every column chunk in this row group lives in
    /// `[byte_offset, byte_offset + byte_length)`.
    pub byte_offset: u64,
    /// Total bytes occupied by this row group in the segment file.
    /// Equal to the sum of `columns[i].byte_length`.
    pub byte_length: u64,

    /// Row count for this row group. For every row group except
    /// (possibly) the last, this equals
    /// [`FooterV1::row_group_size_hint`]. Zero is illegal (§6).
    pub row_count: u64,

    /// Per-column metadata in column-ordinal order. Length equals
    /// `FooterV1.schema.columns().len()` plus any implicit system
    /// columns the writer snapshotted into the schema.
    pub columns: Vec<ColumnChunkMeta>,
}

/// Metadata for a single column chunk inside a row group (§10.2).
///
/// `row_count + null_count` equals the parent row group's `row_count`.
/// `row_count` is the **non-null** count — the number of values the
/// encoding's decoder will reconstruct. This matches the
/// `Encoding::encode` contract in `crate::encoding`: encodings operate
/// on dense arrays, and nulls are reconstructed from the null bitmap
/// stored as a prefix of the column chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnChunkMeta {
    /// Index into `FooterV1.schema.columns()`. Stored explicitly
    /// rather than relying on position so that a segment column chunk
    /// can point at its declared column under partial projections.
    pub column_ordinal: u32,

    /// Absolute offset of the first byte of this column chunk in the
    /// segment file. If the column is nullable this is the start of
    /// the null bitmap; otherwise it is the start of the encoding
    /// discriminant.
    pub byte_offset: u64,

    /// Total bytes — null bitmap (when present) + encoding header
    /// (discriminant + params + `uncompressed_payload_length`) +
    /// on-disk payload (compressed bytes when
    /// `compression == Lz4`, `uncompressed_payload_length` bytes
    /// otherwise).
    pub byte_length: u64,

    /// [`crate::encoding::EncodingType`] discriminant (§9). Kept as
    /// `u8` in the footer so the format is stable across crate moves
    /// of the enum itself.
    pub encoding: u8,

    /// [`CompressionType`] discriminant (§8). Kept as `u8` for the
    /// same reason.
    pub compression: u8,

    /// Non-null row count — the number of values the encoding's
    /// decoder produces.
    pub row_count: u64,

    /// Null row count. `row_count + null_count` equals the parent
    /// row group's `row_count`.
    pub null_count: u64,

    /// Inline zone-map minimum. `None` when every value in the chunk
    /// is null (i.e. `null_count == row_group.row_count`).
    pub zone_min: Option<PropertyValue>,
    /// Inline zone-map maximum. `None` when every value in the chunk
    /// is null.
    pub zone_max: Option<PropertyValue>,
}

/// Reference to a segment-level dictionary (§10.3).
///
/// A dictionary-encoded column has exactly one dictionary per segment,
/// shared by every row group whose chunk picks `Dictionary` encoding
/// for that column. The entry records where the dictionary value bytes
/// live in the segment-dictionaries region (§11) and the type they
/// decode to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDictRef {
    /// The column this dictionary is for. A column has at most one
    /// dictionary per segment.
    pub column_ordinal: u32,

    /// Absolute byte offset of the dictionary values in the segment
    /// file. Points into the segment-dictionaries region, between the
    /// last row group and the footer body.
    pub byte_offset: u64,
    /// Byte length of the dictionary values. `byte_offset + byte_length`
    /// is still inside the segment-dictionaries region.
    pub byte_length: u64,

    /// Number of distinct values in the dictionary.
    pub cardinality: u32,

    /// Type of the dictionary values. Values are serialized on disk as
    /// a Plain payload of this type (§10.3).
    pub value_type: BqlType,
}

// ─────────────────────────────────────────────────────────────────────────────
// v2 layout types (segment-format-v2.md §6 – §8)
// ─────────────────────────────────────────────────────────────────────────────

/// Reference to a segment-level FSST symbol table
/// (`segment-format-v2.md` §6.4).
///
/// One entry per FSST-encoded column. Each entry locates the symbol
/// table bytes inside the FSST symbol tables region (between the
/// segment dictionaries region and the footer body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FsstSymbolTableRef {
    /// The column this symbol table is for. A column has at most
    /// one FSST symbol table per segment.
    pub column_ordinal: u32,

    /// Absolute byte offset of the symbol table in the segment
    /// file. Points into the FSST symbol tables region.
    pub byte_offset: u64,
    /// Byte length of the symbol table data.
    pub byte_length: u64,

    /// Number of symbols in the table (1..=256).
    pub symbol_count: u16,
}

/// The v2 segment footer body — a strict superset of [`FooterV1`]
/// with one new field (`fsst_symbol_tables`). See
/// `segment-format-v2.md` §7.
///
/// FooterV2 is a separate struct rather than an extension of FooterV1
/// because FooterV1's postcard serialization is the v1 on-disk
/// contract — adding a field would change serialized bytes and break
/// v1 reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FooterV2 {
    /// Must equal [`SEGMENT_FORMAT_VERSION_V2`] (2).
    pub format_version: u16,

    /// Full segment schema at write time.
    pub schema: TableSchema,

    /// Schema version the segment was written against.
    pub schema_version: u32,

    /// Total rows across all row groups.
    pub row_count: u64,

    /// Number of row groups.
    pub row_group_count: u32,

    /// Row-group size hint.
    pub row_group_size_hint: u32,

    /// Creation timestamp in nanoseconds since epoch UTC.
    pub creation_timestamp_ns: i64,

    /// Sequence-ID range (min_inclusive, max_inclusive).
    pub seq_id_range: (u64, u64),

    /// Batch ID.
    pub batch_id: u64,

    /// Compaction tier.
    pub compaction_level: u8,

    /// Segment-level dictionaries — identical to FooterV1.
    pub dictionaries: Vec<SegmentDictRef>,

    /// Segment-level FSST symbol tables — NEW in v2.
    /// One entry per FSST-encoded column. Empty when no column
    /// uses FSST encoding.
    pub fsst_symbol_tables: Vec<FsstSymbolTableRef>,

    /// Per-row-group index — identical structure to FooterV1.
    /// `ColumnChunkMeta.encoding` may now contain v2 discriminants.
    pub row_groups: Vec<RowGroupIndex>,
}

/// Version-dispatched segment footer. The reader parses the file
/// header version and deserializes the footer body as the
/// corresponding variant. Accessor methods delegate to the active
/// variant so callers do not need to match.
///
/// See `segment-format-v2.md` §8.4.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentFooter {
    V1(FooterV1),
    V2(FooterV2),
}

impl SegmentFooter {
    pub fn format_version(&self) -> u16 {
        match self {
            Self::V1(f) => f.format_version,
            Self::V2(f) => f.format_version,
        }
    }

    pub fn schema(&self) -> &TableSchema {
        match self {
            Self::V1(f) => &f.schema,
            Self::V2(f) => &f.schema,
        }
    }

    pub fn schema_version(&self) -> u32 {
        match self {
            Self::V1(f) => f.schema_version,
            Self::V2(f) => f.schema_version,
        }
    }

    pub fn row_count(&self) -> u64 {
        match self {
            Self::V1(f) => f.row_count,
            Self::V2(f) => f.row_count,
        }
    }

    pub fn row_group_count(&self) -> u32 {
        match self {
            Self::V1(f) => f.row_group_count,
            Self::V2(f) => f.row_group_count,
        }
    }

    pub fn row_group_size_hint(&self) -> u32 {
        match self {
            Self::V1(f) => f.row_group_size_hint,
            Self::V2(f) => f.row_group_size_hint,
        }
    }

    pub fn creation_timestamp_ns(&self) -> i64 {
        match self {
            Self::V1(f) => f.creation_timestamp_ns,
            Self::V2(f) => f.creation_timestamp_ns,
        }
    }

    pub fn seq_id_range(&self) -> (u64, u64) {
        match self {
            Self::V1(f) => f.seq_id_range,
            Self::V2(f) => f.seq_id_range,
        }
    }

    pub fn batch_id(&self) -> u64 {
        match self {
            Self::V1(f) => f.batch_id,
            Self::V2(f) => f.batch_id,
        }
    }

    pub fn compaction_level(&self) -> u8 {
        match self {
            Self::V1(f) => f.compaction_level,
            Self::V2(f) => f.compaction_level,
        }
    }

    pub fn dictionaries(&self) -> &[SegmentDictRef] {
        match self {
            Self::V1(f) => &f.dictionaries,
            Self::V2(f) => &f.dictionaries,
        }
    }

    pub fn row_groups(&self) -> &[RowGroupIndex] {
        match self {
            Self::V1(f) => &f.row_groups,
            Self::V2(f) => &f.row_groups,
        }
    }

    /// FSST symbol table refs. Returns an empty slice for V1
    /// segments (which have no FSST region).
    pub fn fsst_symbol_tables(&self) -> &[FsstSymbolTableRef] {
        match self {
            Self::V1(_) => &[],
            Self::V2(f) => &f.fsst_symbol_tables,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests — round-trip on every nested record
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::{ColumnDef, TableSchema};

    fn sample_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Float),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    fn sample_footer() -> FooterV1 {
        FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: sample_schema(),
            schema_version: 0,
            row_count: 3,
            row_group_count: 1,
            row_group_size_hint: ROW_GROUP_SIZE_DEFAULT,
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, 2),
            batch_id: 42,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 2,
                byte_offset: 256,
                byte_length: 64,
                cardinality: 3,
                value_type: BqlType::String,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: FILE_HEADER_LEN as u64,
                byte_length: 240,
                row_count: 3,
                columns: vec![
                    ColumnChunkMeta {
                        column_ordinal: 0,
                        byte_offset: FILE_HEADER_LEN as u64,
                        byte_length: 48,
                        encoding: 6, // Constant
                        compression: CompressionType::None.discriminant(),
                        row_count: 3,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    ColumnChunkMeta {
                        column_ordinal: 1,
                        byte_offset: FILE_HEADER_LEN as u64 + 48,
                        byte_length: 48,
                        encoding: 2, // Delta
                        compression: CompressionType::None.discriminant(),
                        row_count: 3,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(1_700_000_000_000_000_000)),
                        zone_max: Some(PropertyValue::Timestamp(1_700_000_000_000_200_000)),
                    },
                    ColumnChunkMeta {
                        column_ordinal: 2,
                        byte_offset: FILE_HEADER_LEN as u64 + 96,
                        byte_length: 72,
                        encoding: 1, // Dictionary
                        compression: CompressionType::None.discriminant(),
                        row_count: 3,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("checkout".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    ColumnChunkMeta {
                        column_ordinal: 3,
                        byte_offset: FILE_HEADER_LEN as u64 + 168,
                        byte_length: 72,
                        encoding: 0, // Plain
                        compression: CompressionType::Lz4.discriminant(),
                        row_count: 2,
                        null_count: 1,
                        zone_min: Some(PropertyValue::Float(12.5)),
                        zone_max: Some(PropertyValue::Float(120.0)),
                    },
                ],
            }],
        }
    }

    #[test]
    fn footer_round_trips_through_postcard() {
        let footer = sample_footer();
        let bytes = postcard::to_allocvec(&footer).unwrap();
        let back: FooterV1 = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, footer);
    }

    #[test]
    fn footer_round_trip_is_deterministic() {
        // postcard output for a given value is required to be stable
        // across runs — we rely on this for reproducible segment bytes
        // in tests and for the segment-level checksum.
        let footer = sample_footer();
        let a = postcard::to_allocvec(&footer).unwrap();
        let b = postcard::to_allocvec(&footer).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn constants_match_segment_format_v1_spec() {
        // These values are pinned by segment-format-v1.md §4. Changing
        // them requires a format-version bump, so the test is a tripwire.
        assert_eq!(MAGIC, *b"BQLT");
        assert_eq!(SEGMENT_FORMAT_VERSION, 1);
        assert_eq!(ROW_GROUP_SIZE_DEFAULT, 65_536);
        assert_eq!(CHECKSUM_SEED, 0);
        assert_eq!(FILE_HEADER_LEN, 6);
        assert_eq!(CHECKSUM_LEN, 8);
        assert_eq!(TRAILER_LEN, 8);
        assert_eq!(FOOTER_SUFFIX_LEN, 16);
    }

    #[test]
    fn footer_format_version_matches_constant() {
        let footer = sample_footer();
        assert_eq!(footer.format_version, SEGMENT_FORMAT_VERSION);
    }

    // ── v2 tests ────────────────────────────────────────────────────────────

    #[test]
    fn v2_constants_match_spec() {
        assert_eq!(SEGMENT_FORMAT_VERSION_V1, 1);
        assert_eq!(SEGMENT_FORMAT_VERSION_V2, 2);
        assert_eq!(SEGMENT_FORMAT_VERSION, SEGMENT_FORMAT_VERSION_V1);
    }

    fn sample_footer_v2() -> FooterV2 {
        FooterV2 {
            format_version: SEGMENT_FORMAT_VERSION_V2,
            schema: sample_schema(),
            schema_version: 0,
            row_count: 3,
            row_group_count: 1,
            row_group_size_hint: ROW_GROUP_SIZE_DEFAULT,
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, 2),
            batch_id: 42,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 2,
                byte_offset: 256,
                byte_length: 64,
                cardinality: 3,
                value_type: BqlType::String,
            }],
            fsst_symbol_tables: vec![FsstSymbolTableRef {
                column_ordinal: 0,
                byte_offset: 320,
                byte_length: 128,
                symbol_count: 42,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: FILE_HEADER_LEN as u64,
                byte_length: 240,
                row_count: 3,
                columns: vec![ColumnChunkMeta {
                    column_ordinal: 0,
                    byte_offset: FILE_HEADER_LEN as u64,
                    byte_length: 48,
                    encoding: 6,
                    compression: CompressionType::None.discriminant(),
                    row_count: 3,
                    null_count: 0,
                    zone_min: Some(PropertyValue::String("u1".into())),
                    zone_max: Some(PropertyValue::String("u1".into())),
                }],
            }],
        }
    }

    #[test]
    fn footer_v2_round_trips_through_postcard() {
        let footer = sample_footer_v2();
        let bytes = postcard::to_allocvec(&footer).unwrap();
        let back: FooterV2 = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, footer);
    }

    #[test]
    fn footer_v2_round_trip_is_deterministic() {
        let footer = sample_footer_v2();
        let a = postcard::to_allocvec(&footer).unwrap();
        let b = postcard::to_allocvec(&footer).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn footer_v1_and_v2_serialize_differently() {
        // FooterV2 has the fsst_symbol_tables field, so even with
        // the same base data the serialized bytes differ.
        let v1 = sample_footer();
        let v2 = sample_footer_v2();
        let v1_bytes = postcard::to_allocvec(&v1).unwrap();
        let v2_bytes = postcard::to_allocvec(&v2).unwrap();
        assert_ne!(v1_bytes, v2_bytes);
    }

    #[test]
    fn segment_footer_accessors_delegate_v1() {
        let v1 = sample_footer();
        let sf = SegmentFooter::V1(v1.clone());
        assert_eq!(sf.format_version(), v1.format_version);
        assert_eq!(sf.schema(), &v1.schema);
        assert_eq!(sf.schema_version(), v1.schema_version);
        assert_eq!(sf.row_count(), v1.row_count);
        assert_eq!(sf.row_group_count(), v1.row_group_count);
        assert_eq!(sf.row_group_size_hint(), v1.row_group_size_hint);
        assert_eq!(sf.creation_timestamp_ns(), v1.creation_timestamp_ns);
        assert_eq!(sf.seq_id_range(), v1.seq_id_range);
        assert_eq!(sf.batch_id(), v1.batch_id);
        assert_eq!(sf.compaction_level(), v1.compaction_level);
        assert_eq!(sf.dictionaries(), &v1.dictionaries[..]);
        assert_eq!(sf.row_groups(), &v1.row_groups[..]);
        assert!(sf.fsst_symbol_tables().is_empty());
    }

    #[test]
    fn segment_footer_accessors_delegate_v2() {
        let v2 = sample_footer_v2();
        let sf = SegmentFooter::V2(v2.clone());
        assert_eq!(sf.format_version(), v2.format_version);
        assert_eq!(sf.schema(), &v2.schema);
        assert_eq!(sf.schema_version(), v2.schema_version);
        assert_eq!(sf.row_count(), v2.row_count);
        assert_eq!(sf.row_group_count(), v2.row_group_count);
        assert_eq!(sf.row_group_size_hint(), v2.row_group_size_hint);
        assert_eq!(sf.creation_timestamp_ns(), v2.creation_timestamp_ns);
        assert_eq!(sf.seq_id_range(), v2.seq_id_range);
        assert_eq!(sf.batch_id(), v2.batch_id);
        assert_eq!(sf.compaction_level(), v2.compaction_level);
        assert_eq!(sf.dictionaries(), &v2.dictionaries[..]);
        assert_eq!(sf.row_groups(), &v2.row_groups[..]);
        assert_eq!(sf.fsst_symbol_tables(), &v2.fsst_symbol_tables[..]);
    }

    #[test]
    fn fsst_symbol_table_ref_round_trips_through_postcard() {
        let r = FsstSymbolTableRef {
            column_ordinal: 3,
            byte_offset: 1024,
            byte_length: 256,
            symbol_count: 128,
        };
        let bytes = postcard::to_allocvec(&r).unwrap();
        let back: FsstSymbolTableRef = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, r);
    }
}
