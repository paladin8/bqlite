//! v1 segment-file reader (TASK-215).
//!
//! Parses a segment file's header, trailer, footer, and segment
//! dictionaries region per `docs/design/storage/segment-format-v1.md`
//! §4–§13 and validates every rule listed in §15. The resulting
//! [`SegmentFileReader`] owns the file bytes and a pre-resolved
//! [`FooterV1`] plus the eagerly-loaded segment-level dictionaries,
//! and exposes those as read-only accessors for the [`SegmentFileScan`]
//! iterator (lands in a later checkpoint) to decode row groups from.
//!
//! # Scope split
//!
//! This file owns two distinct layers:
//!
//! - **Framing + footer + dictionary load** (checkpoint 2 — this
//!   checkpoint). Everything needed to confirm a segment file is
//!   well-formed and to answer metadata queries about it without
//!   touching any row-group bytes.
//! - **Row-group decode + `SegmentScan` impl** (checkpoint 3). The
//!   lazy column-chunk iterator that materializes [`RecordBatch`]es
//!   from the parsed footer.
//!
//! Keeping the two layers in one file is intentional: the scan
//! borrows the reader's `Arc<[u8]>`, `Arc<FooterV1>`, and
//! `Arc<[DictionaryValues]>` directly, so they share a module and
//! its private helpers without a two-file split.
//!
//! # I/O strategy
//!
//! v1 reads the whole segment file into a `Vec<u8>` up front and
//! hands the bytes to the scan as an `Arc<[u8]>`. Mmap and buffered
//! pread land behind the same public API in later waves — see
//! `segment-format-v1.md` §17 open question 2. Wave 2 benches will
//! tell us whether the full-file-in-memory strategy is the
//! bottleneck before we optimize.

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use bqlite_core::{BqlType, BqliteError, Result, TableSchema};

use crate::segment::layout::{
    CompressionType, FooterV1, CHECKSUM_LEN, CHECKSUM_SEED, FILE_HEADER_LEN, FOOTER_SUFFIX_LEN,
    MAGIC, SEGMENT_FORMAT_VERSION, TRAILER_LEN,
};

// ─────────────────────────────────────────────────────────────────────────────
// Public types
// ─────────────────────────────────────────────────────────────────────────────

/// Eagerly-loaded values for one segment-level dictionary.
///
/// v1 holds every dictionary in memory for the lifetime of the
/// [`SegmentFileReader`]; see `segment-format-v1.md` §11 "Memory
/// cost". The variants match the applicable types for the Dictionary
/// encoding (`segment-format-v1.md` §9.2) — `Int` and `String` in
/// v1; later-wave additions grow this enum without breaking existing
/// readers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DictionaryValues {
    /// Dictionary of `Int` values, sorted ascending.
    Int(Vec<i64>),
    /// Dictionary of `String` values, sorted ascending (byte-wise
    /// lexicographic order per `segment-format-v1.md` §10.3).
    String(Vec<String>),
}

impl DictionaryValues {
    /// Number of distinct values in this dictionary.
    pub fn cardinality(&self) -> usize {
        match self {
            Self::Int(v) => v.len(),
            Self::String(v) => v.len(),
        }
    }
}

/// Reader over a single v1 segment file.
///
/// Constructed by reading a segment file from disk ([`Self::open`])
/// or from an in-memory byte buffer ([`Self::from_bytes`]). The
/// constructor runs every validation rule listed in
/// `segment-format-v1.md` §15 — a `SegmentFileReader` value is
/// therefore a proof that the underlying bytes are a well-formed
/// v1 segment.
///
/// Cloning a `SegmentFileReader` is cheap: the backing bytes, the
/// parsed footer, and the loaded dictionaries all live behind
/// `Arc`s so that many scans can share one reader without copying
/// the file.
#[derive(Clone)]
pub struct SegmentFileReader {
    /// Entire file in memory. The scan iterator reads column-chunk
    /// bytes directly out of this buffer via absolute offsets from
    /// [`crate::segment::layout::ColumnChunkMeta`].
    bytes: Arc<[u8]>,
    /// Parsed footer body. The reader guarantees this struct has
    /// passed every §15 validation rule.
    footer: Arc<FooterV1>,
    /// Segment-level dictionaries, indexed by
    /// [`crate::segment::layout::FooterV1::dictionaries`] position.
    /// Loaded eagerly on open per §11.
    dictionaries: Arc<[DictionaryValues]>,
    /// Current manifest schema the reader should project rows
    /// against, passed in by the caller. This is the target schema
    /// for name-based lookups during row-group decode (§14 schema
    /// evolution) — it may differ from the segment's write-time
    /// schema in [`FooterV1::schema`] when a column has been added
    /// via `ALTER TABLE ADD COLUMN` since the segment was written.
    current_schema: Arc<TableSchema>,
}

impl SegmentFileReader {
    /// Open a segment file from disk.
    ///
    /// Reads the entire file into memory, then runs the same
    /// validation path as [`Self::from_bytes`]. Any I/O error is
    /// returned as `BqliteError::Io`; any format error is
    /// `BqliteError::Corruption`.
    pub fn open<P: AsRef<Path>>(path: P, current_schema: TableSchema) -> Result<Self> {
        let bytes = fs::read(path)?;
        Self::from_bytes(bytes, current_schema)
    }

    /// Parse a segment file from an owned byte buffer.
    ///
    /// Runs every §15 validation rule in order (§15 rules 1–12)
    /// and eagerly loads every segment-level dictionary. On success
    /// the returned reader is guaranteed to satisfy the full format
    /// contract.
    pub fn from_bytes(bytes: Vec<u8>, current_schema: TableSchema) -> Result<Self> {
        validate_header(&bytes)?;
        let footer_body_length = parse_trailer(&bytes)?;
        validate_framing_lengths(bytes.len(), footer_body_length)?;

        let footer_body_start = bytes.len() - CHECKSUM_LEN - TRAILER_LEN - footer_body_length;
        let footer_body_end = bytes.len() - CHECKSUM_LEN - TRAILER_LEN;
        let footer_body_bytes = &bytes[footer_body_start..footer_body_end];

        let footer: FooterV1 = postcard::from_bytes(footer_body_bytes).map_err(|e| {
            BqliteError::Corruption(format!(
                "segment footer body failed to deserialize (postcard): {e}"
            ))
        })?;

        validate_footer(&footer, footer_body_start)?;
        verify_checksum(&bytes)?;

        let dictionaries = load_dictionaries(&bytes, &footer)?;

        Ok(Self {
            bytes: Arc::from(bytes.into_boxed_slice()),
            footer: Arc::new(footer),
            dictionaries: Arc::from(dictionaries.into_boxed_slice()),
            current_schema: Arc::new(current_schema),
        })
    }

    /// The parsed footer body. Guaranteed to have passed every
    /// §15 validation rule.
    pub fn footer(&self) -> &FooterV1 {
        &self.footer
    }

    /// The segment's write-time schema — the shape the column
    /// chunks inside the file are encoded against. Callers
    /// projecting against schema evolution should use
    /// [`Self::current_schema`] instead.
    pub fn write_time_schema(&self) -> &TableSchema {
        &self.footer.schema
    }

    /// The current manifest schema the reader was opened with.
    /// Used by the row-group decoder for name-based column lookups.
    pub fn current_schema(&self) -> &TableSchema {
        &self.current_schema
    }

    /// The eagerly-loaded segment-level dictionaries, in the same
    /// order as [`FooterV1::dictionaries`].
    pub fn dictionaries(&self) -> &[DictionaryValues] {
        &self.dictionaries
    }

    /// Number of row groups in this segment. Equal to
    /// `self.footer().row_groups.len()`.
    pub fn row_group_count(&self) -> usize {
        self.footer.row_groups.len()
    }

    /// Total row count across every row group in the segment.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }

    /// The underlying byte buffer. Exposed (crate-visible only) so
    /// the row-group decoder in the next checkpoint can pass the
    /// bytes to column-chunk parsing helpers without re-reading the
    /// file.
    #[allow(dead_code)] // used by the SegmentFileScan impl in checkpoint 3
    pub(crate) fn bytes(&self) -> &Arc<[u8]> {
        &self.bytes
    }
}

impl std::fmt::Debug for SegmentFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentFileReader")
            .field("file_size", &self.bytes.len())
            .field("format_version", &self.footer.format_version)
            .field("row_count", &self.footer.row_count)
            .field("row_group_count", &self.footer.row_group_count)
            .field("dictionaries", &self.dictionaries.len())
            .field("schema", &self.footer.schema.name())
            .finish()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// §15 validation — framing
// ─────────────────────────────────────────────────────────────────────────────

/// §15 rules 1, 2, 3 — file size minimum, header magic, format version.
fn validate_header(bytes: &[u8]) -> Result<()> {
    if bytes.len() < FILE_HEADER_LEN + FOOTER_SUFFIX_LEN {
        return Err(BqliteError::Corruption(format!(
            "segment file too short: {} bytes (minimum {} bytes for header + checksum + trailer)",
            bytes.len(),
            FILE_HEADER_LEN + FOOTER_SUFFIX_LEN,
        )));
    }
    if bytes[..4] != MAGIC {
        return Err(BqliteError::Corruption(format!(
            "segment file header magic mismatch: expected {:?}, got {:?}",
            MAGIC,
            &bytes[..4],
        )));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().expect("slice length checked above"));
    if version != SEGMENT_FORMAT_VERSION {
        return Err(BqliteError::Corruption(format!(
            "segment file format version mismatch: expected {SEGMENT_FORMAT_VERSION}, got {version}"
        )));
    }
    Ok(())
}

/// §15 rule 4 — trailer magic; §15 rule 5 — `footer_body_length` extract.
///
/// Returns the footer body length (in bytes) read out of the trailer.
///
/// # Precondition
///
/// The caller must have already run [`validate_header`], which checks
/// `bytes.len() >= FILE_HEADER_LEN + FOOTER_SUFFIX_LEN`. That is the
/// only reason this function can index into `bytes.len() - TRAILER_LEN`
/// without panicking. Never call `parse_trailer` directly.
fn parse_trailer(bytes: &[u8]) -> Result<usize> {
    let trailer_start = bytes.len() - TRAILER_LEN;
    let trailer = &bytes[trailer_start..];
    // [0..4] = footer_body_length u32 LE
    // [4..8] = trailer magic
    if trailer[4..8] != MAGIC {
        return Err(BqliteError::Corruption(format!(
            "segment file trailer magic mismatch: expected {:?}, got {:?}",
            MAGIC,
            &trailer[4..8],
        )));
    }
    let footer_body_length = u32::from_le_bytes(
        trailer[..4]
            .try_into()
            .expect("slice length checked by TRAILER_LEN"),
    ) as usize;
    Ok(footer_body_length)
}

/// §15 rule 5 — `footer_body_length` fits within the file.
fn validate_framing_lengths(file_size: usize, footer_body_length: usize) -> Result<()> {
    // `footer_body_length` must leave room for the header + itself +
    // the checksum + the trailer: 6 + L + 8 + 8 ≤ file_size.
    let required = FILE_HEADER_LEN
        .checked_add(footer_body_length)
        .and_then(|n| n.checked_add(FOOTER_SUFFIX_LEN));
    match required {
        Some(min) if min <= file_size => Ok(()),
        _ => Err(BqliteError::Corruption(format!(
            "segment file trailer claims footer_body_length = {footer_body_length}, \
             which does not fit in a {file_size}-byte file (needs at least \
             {FILE_HEADER_LEN} + {footer_body_length} + {FOOTER_SUFFIX_LEN} bytes)"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// §15 validation — footer body
// ─────────────────────────────────────────────────────────────────────────────

/// §15 rules 7–11 — footer internal consistency + byte-range bounds.
fn validate_footer(footer: &FooterV1, footer_body_start: usize) -> Result<()> {
    // Rule 7: format_version.
    if footer.format_version != SEGMENT_FORMAT_VERSION {
        return Err(BqliteError::Corruption(format!(
            "segment footer format_version mismatch: expected {SEGMENT_FORMAT_VERSION}, \
             got {}",
            footer.format_version
        )));
    }

    // Rule 8: row_group_count == row_groups.len() and sum of row counts.
    if footer.row_group_count as usize != footer.row_groups.len() {
        return Err(BqliteError::Corruption(format!(
            "segment footer row_group_count = {} but row_groups has {} entries",
            footer.row_group_count,
            footer.row_groups.len(),
        )));
    }
    if footer.row_groups.is_empty() {
        return Err(BqliteError::Corruption(
            "segment footer has zero row groups — an empty segment is illegal \
             per segment-format-v1.md §6"
                .to_string(),
        ));
    }
    let sum: Option<u64> = footer
        .row_groups
        .iter()
        .map(|rg| rg.row_count)
        .try_fold(0u64, u64::checked_add);
    match sum {
        Some(s) if s == footer.row_count => (),
        Some(s) => {
            return Err(BqliteError::Corruption(format!(
                "segment footer row_count = {} but row groups sum to {s}",
                footer.row_count
            )));
        }
        None => {
            return Err(BqliteError::Corruption(
                "segment footer row group row counts overflow u64".to_string(),
            ));
        }
    }

    // Row groups live strictly before the segment-dictionaries region
    // + footer body. The segment-dictionaries region is optional, so
    // its start is the first dictionary's byte_offset when there are
    // dictionaries, or `footer_body_start` otherwise.
    let row_groups_end_max = footer
        .dictionaries
        .iter()
        .map(|d| d.byte_offset)
        .min()
        .map(|off| off as usize)
        .unwrap_or(footer_body_start);

    // Rule 9: per-row-group byte ranges fit inside the row-groups
    // region [FILE_HEADER_LEN, row_groups_end_max).
    let mut expected_offset = FILE_HEADER_LEN as u64;
    for (i, rg) in footer.row_groups.iter().enumerate() {
        if rg.row_count == 0 {
            return Err(BqliteError::Corruption(format!(
                "row group {i} has row_count = 0; empty row groups are illegal (§6)"
            )));
        }
        if rg.byte_offset < expected_offset {
            return Err(BqliteError::Corruption(format!(
                "row group {i} byte_offset = {} overlaps previous row group (expected >= {expected_offset})",
                rg.byte_offset
            )));
        }
        let rg_end = rg.byte_offset.checked_add(rg.byte_length).ok_or_else(|| {
            BqliteError::Corruption(format!(
                "row group {i} byte range overflows u64 (offset {}, length {})",
                rg.byte_offset, rg.byte_length
            ))
        })?;
        if rg_end > row_groups_end_max as u64 {
            return Err(BqliteError::Corruption(format!(
                "row group {i} byte range [{}, {rg_end}) exceeds row-groups region end {row_groups_end_max}",
                rg.byte_offset
            )));
        }

        // Rule 10: per-column-chunk byte ranges fit inside the row
        // group + legal encoding/compression discriminants.
        let schema_col_count = footer.schema.columns().len();
        if rg.columns.len() != schema_col_count {
            return Err(BqliteError::Corruption(format!(
                "row group {i} has {} column chunks but schema has {schema_col_count} columns",
                rg.columns.len()
            )));
        }
        let mut chunk_cursor = rg.byte_offset;
        for (c, meta) in rg.columns.iter().enumerate() {
            if meta.column_ordinal as usize != c {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c}: metadata column_ordinal {} does not match position",
                    meta.column_ordinal
                )));
            }
            if meta.byte_offset < chunk_cursor {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c} byte_offset = {} overlaps previous chunk (expected >= {chunk_cursor})",
                    meta.byte_offset
                )));
            }
            let chunk_end = meta
                .byte_offset
                .checked_add(meta.byte_length)
                .ok_or_else(|| {
                    BqliteError::Corruption(format!(
                        "row group {i} column {c} byte range overflows u64"
                    ))
                })?;
            if chunk_end > rg_end {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c} byte range [{}, {chunk_end}) exceeds row group end {rg_end}",
                    meta.byte_offset
                )));
            }
            // Rule 10 continued: legal encoding discriminant.
            match meta.encoding {
                0 | 1 | 2 | 4 | 6 => (),
                other => {
                    return Err(BqliteError::Corruption(format!(
                        "row group {i} column {c} encoding {other} is not in the v1 set {{0,1,2,4,6}}"
                    )));
                }
            }
            // Rule 10 continued: legal compression discriminant.
            if CompressionType::from_discriminant(meta.compression).is_err() {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c} compression {} is not in the v1 set {{0,1}}",
                    meta.compression
                )));
            }
            // row_count + null_count == parent row group row_count.
            let total = meta.row_count.checked_add(meta.null_count).ok_or_else(|| {
                BqliteError::Corruption(format!(
                    "row group {i} column {c} row_count + null_count overflows u64"
                ))
            })?;
            if total != rg.row_count {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c} row_count {} + null_count {} != row group row_count {}",
                    meta.row_count, meta.null_count, rg.row_count,
                )));
            }
            chunk_cursor = chunk_end;
        }
        expected_offset = rg_end;
    }

    // Rule 11: per-dictionary byte ranges fit inside the
    // segment-dictionaries region, column_ordinal fits the schema.
    let schema_col_count = footer.schema.columns().len();
    let dict_region_start = expected_offset as usize;
    let dict_region_end = footer_body_start;
    if dict_region_end < dict_region_start {
        return Err(BqliteError::Corruption(format!(
            "footer body start {footer_body_start} is before the end of the row groups region {dict_region_start}"
        )));
    }
    let mut seen_columns: HashSet<u32> = HashSet::new();
    for (i, dict) in footer.dictionaries.iter().enumerate() {
        if (dict.column_ordinal as usize) >= schema_col_count {
            return Err(BqliteError::Corruption(format!(
                "dictionary {i} column_ordinal {} is out of schema bounds (< {schema_col_count})",
                dict.column_ordinal
            )));
        }
        if !seen_columns.insert(dict.column_ordinal) {
            return Err(BqliteError::Corruption(format!(
                "dictionary {i}: column_ordinal {} already has a dictionary — v1 allows at most one per column",
                dict.column_ordinal
            )));
        }
        let start = dict.byte_offset as usize;
        let end = (dict.byte_offset as usize)
            .checked_add(dict.byte_length as usize)
            .ok_or_else(|| {
                BqliteError::Corruption(format!("dictionary {i} byte range overflows usize"))
            })?;
        if start < dict_region_start || end > dict_region_end {
            return Err(BqliteError::Corruption(format!(
                "dictionary {i} byte range [{start}, {end}) escapes segment-dictionaries region [{dict_region_start}, {dict_region_end})"
            )));
        }
        // Per §10.3, v1 dictionaries only carry `Int` or `String`
        // values; anything else is a writer bug / corruption.
        match dict.value_type {
            BqlType::Int | BqlType::String => (),
            _ => {
                return Err(BqliteError::Corruption(format!(
                    "dictionary {i} has value_type {:?}; v1 dictionaries are Int or String only (§9.2)",
                    dict.value_type
                )));
            }
        }
    }

    // Row-group-size-hint sanity: v1 writers always emit the default
    // row-group size. Accept any positive value so later waves can
    // vary it without breaking the reader, but reject zero.
    if footer.row_group_size_hint == 0 {
        return Err(BqliteError::Corruption(
            "segment footer row_group_size_hint = 0 (must be positive)".to_string(),
        ));
    }
    // Soft sanity check: every row group's count ≤ row_group_size_hint
    // except possibly the last. This catches a corrupt writer that
    // emits over-sized row groups.
    let n = footer.row_groups.len();
    for (i, rg) in footer.row_groups.iter().enumerate() {
        let is_last = i + 1 == n;
        if !is_last && rg.row_count > footer.row_group_size_hint as u64 {
            return Err(BqliteError::Corruption(format!(
                "non-final row group {i} row_count {} exceeds row_group_size_hint {}",
                rg.row_count, footer.row_group_size_hint
            )));
        }
    }
    // seq_id_range monotonic.
    if footer.seq_id_range.0 > footer.seq_id_range.1 {
        return Err(BqliteError::Corruption(format!(
            "segment footer seq_id_range = ({}, {}): min exceeds max",
            footer.seq_id_range.0, footer.seq_id_range.1
        )));
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// §15 rule 12 — checksum
// ─────────────────────────────────────────────────────────────────────────────

/// §15 rule 12 — xxHash64 over `[0, file_size − 16)` matches the
/// 8-byte checksum at `[file_size − 16, file_size − 8)`.
///
/// v1 verifies the checksum unconditionally on open. A paranoid
/// mode that re-verifies on every read, or a skip mode for trusted
/// environments, is an additive future extension per
/// `segment-format-v1.md` §12.
fn verify_checksum(bytes: &[u8]) -> Result<()> {
    // The checksum bytes sit immediately before the trailer, and the
    // checksummed region is everything before the checksum bytes. The
    // same byte offset serves both the end of the xxHash input and the
    // start of the stored 8-byte hash.
    let checksum_boundary = bytes.len() - CHECKSUM_LEN - TRAILER_LEN;
    let stored_bytes: [u8; CHECKSUM_LEN] = bytes
        [checksum_boundary..checksum_boundary + CHECKSUM_LEN]
        .try_into()
        .expect("slice length checked by FOOTER_SUFFIX_LEN");
    let stored = u64::from_le_bytes(stored_bytes);
    let computed = ::twox_hash::XxHash64::oneshot(CHECKSUM_SEED, &bytes[..checksum_boundary]);
    if computed != stored {
        return Err(BqliteError::Corruption(format!(
            "segment file checksum mismatch: stored {stored:#x}, computed {computed:#x} \
             over {checksum_boundary} bytes"
        )));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Dictionary load (§10.3, §11)
// ─────────────────────────────────────────────────────────────────────────────

/// §11 — eagerly load every segment-level dictionary.
///
/// Dictionary bytes are a Plain payload of the column's
/// [`BqlType`] (§10.3): contiguous fixed-width values for `Int`,
/// `u32 LE` length prefixes + UTF-8 bytes per value for `String`.
/// We decode them directly into owned Rust vectors so the row-group
/// decoder in the next checkpoint can look up dictionary values by
/// code without touching the file buffer.
fn load_dictionaries(bytes: &[u8], footer: &FooterV1) -> Result<Vec<DictionaryValues>> {
    let mut out = Vec::with_capacity(footer.dictionaries.len());
    for (i, dict_ref) in footer.dictionaries.iter().enumerate() {
        let start = dict_ref.byte_offset as usize;
        let end = start + dict_ref.byte_length as usize;
        // Already validated by `validate_footer` — re-check here
        // defensively in case the rules drift.
        if end > bytes.len() {
            return Err(BqliteError::Corruption(format!(
                "dictionary {i} byte range [{start}, {end}) exceeds file length {}",
                bytes.len()
            )));
        }
        let region = &bytes[start..end];
        let values = decode_dictionary_region(region, &dict_ref.value_type, dict_ref.cardinality)
            .map_err(|e| {
            BqliteError::Corruption(format!(
                "dictionary {i} (column_ordinal {}): {e}",
                dict_ref.column_ordinal
            ))
        })?;
        if values.cardinality() != dict_ref.cardinality as usize {
            return Err(BqliteError::Corruption(format!(
                "dictionary {i}: footer says cardinality = {}, decoded {} values",
                dict_ref.cardinality,
                values.cardinality()
            )));
        }
        out.push(values);
    }
    Ok(out)
}

/// Decode one segment-level dictionary region to its typed values.
///
/// Returns a plain `String` error (wrapped into `Corruption` by the
/// caller) so the error message carries the dictionary index + column
/// ordinal in one place.
fn decode_dictionary_region(
    region: &[u8],
    value_type: &BqlType,
    cardinality: u32,
) -> std::result::Result<DictionaryValues, String> {
    let n = cardinality as usize;
    match value_type {
        BqlType::Int => {
            let needed = n * 8;
            if region.len() != needed {
                return Err(format!(
                    "Int dictionary region length {} does not match {} × 8 = {needed}",
                    region.len(),
                    n
                ));
            }
            let mut values = Vec::with_capacity(n);
            for i in 0..n {
                let off = i * 8;
                let bytes: [u8; 8] = region[off..off + 8]
                    .try_into()
                    .map_err(|_| "Int dictionary slice conversion failed".to_string())?;
                values.push(i64::from_le_bytes(bytes));
            }
            verify_sorted_ascending_int(&values)?;
            Ok(DictionaryValues::Int(values))
        }
        BqlType::String => {
            let mut values = Vec::with_capacity(n);
            let mut cursor = 0;
            for i in 0..n {
                if cursor + 4 > region.len() {
                    return Err(format!(
                        "String dictionary entry {i} length prefix out of bounds \
                         (cursor {cursor}, region {})",
                        region.len()
                    ));
                }
                let len_bytes: [u8; 4] = region[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| "String dictionary length slice conversion failed".to_string())?;
                let len = u32::from_le_bytes(len_bytes) as usize;
                cursor += 4;
                if cursor + len > region.len() {
                    return Err(format!(
                        "String dictionary entry {i} bytes out of bounds \
                         (cursor {cursor}, len {len}, region {})",
                        region.len()
                    ));
                }
                let s = std::str::from_utf8(&region[cursor..cursor + len])
                    .map_err(|e| format!("String dictionary entry {i} is not valid UTF-8: {e}"))?
                    .to_string();
                values.push(s);
                cursor += len;
            }
            if cursor != region.len() {
                return Err(format!(
                    "String dictionary region has {} trailing bytes after {n} entries",
                    region.len() - cursor
                ));
            }
            verify_sorted_ascending_string(&values)?;
            Ok(DictionaryValues::String(values))
        }
        other => Err(format!(
            "dictionary value_type {other:?} is not supported by v1 (Int / String only)"
        )),
    }
}

/// Dictionaries must be sorted ascending so codes resolve as
/// ordinals into a sorted sequence (§10.3). A writer bug that
/// leaves them unsorted would silently mis-decode every row group
/// — we catch it here.
fn verify_sorted_ascending_int(values: &[i64]) -> std::result::Result<(), String> {
    for w in values.windows(2) {
        if w[0] > w[1] {
            return Err(format!(
                "Int dictionary is not sorted ascending (saw {} > {})",
                w[0], w[1]
            ));
        }
    }
    Ok(())
}

fn verify_sorted_ascending_string(values: &[String]) -> std::result::Result<(), String> {
    for w in values.windows(2) {
        if w[0].as_str() > w[1].as_str() {
            return Err(format!(
                "String dictionary is not sorted ascending (saw {:?} > {:?})",
                w[0], w[1]
            ));
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Test-only fixture builder
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub(crate) mod test_fixture {
    //! Hand-built segment byte-stream constructor used by this
    //! module's unit tests (and shared with the row-group decoder
    //! tests in the next checkpoint). Deliberately dumb: it takes a
    //! pre-built [`FooterV1`] value plus raw row-group bytes plus
    //! raw dictionary-region bytes and emits the framed segment the
    //! §4 layout defines. No encoding selection, no writer logic.

    use super::*;

    /// Assemble a complete v1 segment from a pre-built footer and
    /// pre-laid-out row group / dictionary bytes.
    ///
    /// The `row_groups` byte slice is concatenated starting at
    /// offset [`FILE_HEADER_LEN`]; every [`RowGroupIndex`] in the
    /// caller's `footer` must reference offsets inside that region.
    /// Same rule for dictionaries (which sit between row groups and
    /// the footer body).
    ///
    /// This helper computes the correct trailer length and checksum
    /// — tests that want to break a field (magic, version, length,
    /// checksum) do so by post-processing the returned vector.
    pub(crate) fn build_segment(
        footer: &FooterV1,
        row_groups_bytes: &[u8],
        dictionaries_bytes: &[u8],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        // Header: magic + version LE
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
        // Row groups region
        out.extend_from_slice(row_groups_bytes);
        // Segment dictionaries region
        out.extend_from_slice(dictionaries_bytes);
        // Footer body (postcard)
        let footer_bytes = postcard::to_allocvec(footer).expect("postcard encode footer");
        out.extend_from_slice(&footer_bytes);
        // Checksum over everything written so far
        let checksum = ::twox_hash::XxHash64::oneshot(CHECKSUM_SEED, &out);
        out.extend_from_slice(&checksum.to_le_bytes());
        // Trailer: u32 LE footer body length + magic
        let footer_len = footer_bytes.len() as u32;
        out.extend_from_slice(&footer_len.to_le_bytes());
        out.extend_from_slice(&MAGIC);
        out
    }

    /// Build the bytes of a non-nullable Plain-encoded Int column
    /// chunk with no compression, per §7 + §9.1: no null bitmap,
    /// encoding header `discriminant=0`, empty params,
    /// `uncompressed_payload_length = 8 × row_count`, payload is the
    /// raw little-endian i64 values.
    pub(crate) fn plain_int_chunk(values: &[i64]) -> Vec<u8> {
        let mut out = Vec::new();
        // encoding discriminant
        out.push(0);
        // params: empty for Plain
        // uncompressed_payload_length
        let payload_len = (values.len() * 8) as u32;
        out.extend_from_slice(&payload_len.to_le_bytes());
        // payload
        for v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    /// Build a Plain-encoded `Int` dictionary region: `cardinality ×
    /// i64 LE`.
    pub(crate) fn int_dictionary_bytes(values: &[i64]) -> Vec<u8> {
        let mut out = Vec::with_capacity(values.len() * 8);
        for v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    /// Build a Plain-encoded `String` dictionary region:
    /// `cardinality × (u32 LE length + UTF-8 bytes)`.
    pub(crate) fn string_dictionary_bytes(values: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for v in values {
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        out
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::test_fixture::*;
    use super::*;
    use crate::segment::layout::{ColumnChunkMeta, RowGroupIndex, SegmentDictRef};
    use bqlite_core::{ColumnDef, PropertyValue, TableSchema};

    fn simple_int_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::required("amount", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    /// Build the `(footer, row_group_bytes)` pair for a minimal valid
    /// segment. Split out from [`build_minimal_segment`] so tests can
    /// mutate the footer (to force §15 rule failures) and re-pack the
    /// segment via [`build_segment`].
    ///
    /// Row-group layout: one row group, one column chunk for the
    /// `amount` column (Plain-encoded, non-nullable Int), plus empty
    /// / stub chunks for the other three columns so §15 rule 10 has
    /// the expected column count.
    ///
    /// Everything except the `amount` column uses a 0-byte Plain chunk
    /// with row_count = 0 and null_count = row group row count — which
    /// is valid under the Plain format for an all-null non-nullable
    /// column. This is a test fixture, not a real writer output, so
    /// we cheat a bit: non-nullable columns with a zero non-null
    /// count would be caught by the writer's invariants, but the
    /// reader's §15 rules don't reject them.
    fn build_minimal_parts(schema: TableSchema, amount_values: &[i64]) -> (FooterV1, Vec<u8>) {
        // Lay out the row group's column chunks contiguously starting
        // at FILE_HEADER_LEN.
        let mut row_group = Vec::new();

        let col0_chunk = plain_empty_chunk();
        let col0_offset = FILE_HEADER_LEN as u64;
        let col0_len = col0_chunk.len() as u64;
        let col1_chunk = plain_empty_chunk();
        let col1_offset = col0_offset + col0_len;
        let col1_len = col1_chunk.len() as u64;
        let col2_chunk = plain_empty_chunk();
        let col2_offset = col1_offset + col1_len;
        let col2_len = col2_chunk.len() as u64;
        let col3_chunk = plain_int_chunk(amount_values);
        let col3_offset = col2_offset + col2_len;
        let col3_len = col3_chunk.len() as u64;

        row_group.extend_from_slice(&col0_chunk);
        row_group.extend_from_slice(&col1_chunk);
        row_group.extend_from_slice(&col2_chunk);
        row_group.extend_from_slice(&col3_chunk);

        let rg_byte_length = row_group.len() as u64;
        let row_count = amount_values.len() as u64;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema,
            schema_version: 0,
            row_count,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, row_count.saturating_sub(1)),
            batch_id: 1,
            compaction_level: 0,
            dictionaries: vec![],
            row_groups: vec![RowGroupIndex {
                byte_offset: FILE_HEADER_LEN as u64,
                byte_length: rg_byte_length,
                row_count,
                columns: vec![
                    ColumnChunkMeta {
                        column_ordinal: 0,
                        byte_offset: col0_offset,
                        byte_length: col0_len,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: row_count,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 1,
                        byte_offset: col1_offset,
                        byte_length: col1_len,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: row_count,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 2,
                        byte_offset: col2_offset,
                        byte_length: col2_len,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: row_count,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 3,
                        byte_offset: col3_offset,
                        byte_length: col3_len,
                        encoding: 0,
                        compression: 0,
                        row_count,
                        null_count: 0,
                        zone_min: amount_values.iter().min().copied().map(PropertyValue::Int),
                        zone_max: amount_values.iter().max().copied().map(PropertyValue::Int),
                    },
                ],
            }],
        };
        (footer, row_group)
    }

    /// Build a valid minimal segment and return its bytes alongside
    /// the schema. Delegates to [`build_minimal_parts`] + [`build_segment`].
    fn build_minimal_segment(schema: TableSchema, amount_values: &[i64]) -> (Vec<u8>, TableSchema) {
        let (footer, row_group) = build_minimal_parts(schema.clone(), amount_values);
        (build_segment(&footer, &row_group, &[]), schema)
    }

    /// Build a minimal segment whose footer has been mutated in
    /// place by the caller. Used by the §15 rule failure tests.
    fn build_mutated_segment(
        schema: TableSchema,
        amount_values: &[i64],
        mutate: impl FnOnce(&mut FooterV1),
    ) -> Vec<u8> {
        let (mut footer, row_group) = build_minimal_parts(schema, amount_values);
        mutate(&mut footer);
        build_segment(&footer, &row_group, &[])
    }

    /// Empty plain column chunk — `encoding=0`, empty params,
    /// `uncompressed_payload_length=0`, empty payload. 5 bytes on
    /// disk (1 byte discriminant + 4 byte u32 LE length).
    fn plain_empty_chunk() -> Vec<u8> {
        let mut out = Vec::new();
        out.push(0);
        out.extend_from_slice(&0u32.to_le_bytes());
        out
    }

    // ── Framing validation ────────────────────────────────────────────

    #[test]
    fn rejects_file_too_short() {
        for len in [0, 1, 5, 10, 21] {
            let err = SegmentFileReader::from_bytes(vec![0; len], simple_int_schema()).unwrap_err();
            match err {
                BqliteError::Corruption(msg) => {
                    assert!(msg.contains("too short"), "got: {msg}");
                }
                other => panic!("expected Corruption, got {other:?}"),
            }
        }
    }

    #[test]
    fn rejects_bad_header_magic() {
        let (mut bytes, schema) = build_minimal_segment(simple_int_schema(), &[1, 2, 3]);
        bytes[0] = b'X';
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => assert!(msg.contains("header magic"), "got: {msg}"),
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_format_version() {
        let (mut bytes, schema) = build_minimal_segment(simple_int_schema(), &[1, 2, 3]);
        // Flip version bytes to 2.
        bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("format version"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_bad_trailer_magic() {
        let (mut bytes, schema) = build_minimal_segment(simple_int_schema(), &[1, 2, 3]);
        let len = bytes.len();
        bytes[len - 4] = b'X';
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => assert!(msg.contains("trailer magic"), "got: {msg}"),
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_bad_footer_body_length() {
        let (mut bytes, schema) = build_minimal_segment(simple_int_schema(), &[1, 2, 3]);
        let len = bytes.len();
        // Overwrite footer_body_length with a giant value so it
        // exceeds the file size.
        bytes[len - 8..len - 4].copy_from_slice(&u32::MAX.to_le_bytes());
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("footer_body_length"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_bad_footer_body_bytes() {
        let (mut bytes, schema) = build_minimal_segment(simple_int_schema(), &[1, 2, 3]);
        // Corrupt a byte in the middle of the postcard region (anywhere
        // inside the footer body, but not in the trailer or checksum).
        let len = bytes.len();
        let footer_start = len - FOOTER_SUFFIX_LEN - 5; // ~5 bytes into the footer
        bytes[footer_start] ^= 0xff;
        // The checksum will also fail (because we edited the checksummed
        // region) — we just assert the error is a Corruption.
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(_) => (),
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_checksum_mismatch() {
        let (mut bytes, schema) = build_minimal_segment(simple_int_schema(), &[1, 2, 3]);
        // Corrupt the first byte of the stored checksum (no touch to
        // the checksummed region). Only `verify_checksum` should fire.
        let len = bytes.len();
        let checksum_start = len - CHECKSUM_LEN - TRAILER_LEN;
        bytes[checksum_start] ^= 0xff;
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("checksum"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    // ── Happy path ────────────────────────────────────────────────────

    #[test]
    fn opens_minimal_valid_segment() {
        let schema = simple_int_schema();
        let (bytes, schema_clone) = build_minimal_segment(schema, &[10, 20, 30]);
        let reader = SegmentFileReader::from_bytes(bytes, schema_clone).expect("open succeeds");
        assert_eq!(reader.footer().row_count, 3);
        assert_eq!(reader.row_group_count(), 1);
        assert_eq!(reader.write_time_schema().name(), "events");
        assert!(reader.dictionaries().is_empty());
    }

    #[test]
    fn debug_impl_summarises_reader_metadata() {
        let schema = simple_int_schema();
        let (bytes, schema_clone) = build_minimal_segment(schema, &[1, 2, 3, 4, 5]);
        let reader = SegmentFileReader::from_bytes(bytes, schema_clone).unwrap();
        let s = format!("{reader:?}");
        assert!(s.contains("row_count: 5"));
        assert!(s.contains("row_group_count: 1"));
        assert!(s.contains("format_version: 1"));
    }

    #[test]
    fn clone_is_cheap_and_shares_state() {
        let schema = simple_int_schema();
        let (bytes, schema_clone) = build_minimal_segment(schema, &[1, 2, 3]);
        let a = SegmentFileReader::from_bytes(bytes, schema_clone).unwrap();
        let b = a.clone();
        assert_eq!(a.footer().row_count, b.footer().row_count);
        assert!(Arc::ptr_eq(a.bytes(), b.bytes()));
    }

    // ── Footer-body semantic validation ──────────────────────────────

    #[test]
    fn rejects_footer_row_count_mismatch() {
        let schema = simple_int_schema();
        let (good_bytes, schema_clone) = build_minimal_segment(schema.clone(), &[1, 2, 3]);
        // Rebuild with an inflated row_count. We can't easily edit the
        // postcard-encoded footer in place, so build a new footer struct
        // from scratch and reuse the row_group bytes from the good
        // segment. Easier route: construct the footer with a wrong
        // row_count and rebuild the full segment.
        let mut row_group = Vec::new();
        row_group.extend_from_slice(&plain_empty_chunk());
        row_group.extend_from_slice(&plain_empty_chunk());
        row_group.extend_from_slice(&plain_empty_chunk());
        row_group.extend_from_slice(&plain_int_chunk(&[1, 2, 3]));

        let rg_len = row_group.len() as u64;
        let col0 = FILE_HEADER_LEN as u64;
        let col1 = col0 + plain_empty_chunk().len() as u64;
        let col2 = col1 + plain_empty_chunk().len() as u64;
        let col3 = col2 + plain_empty_chunk().len() as u64;
        let bad_footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 99, // WRONG — row group has 3
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 2),
            batch_id: 1,
            compaction_level: 0,
            dictionaries: vec![],
            row_groups: vec![RowGroupIndex {
                byte_offset: FILE_HEADER_LEN as u64,
                byte_length: rg_len,
                row_count: 3,
                columns: vec![
                    ColumnChunkMeta {
                        column_ordinal: 0,
                        byte_offset: col0,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 3,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 1,
                        byte_offset: col1,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 3,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 2,
                        byte_offset: col2,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 3,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 3,
                        byte_offset: col3,
                        byte_length: 5 + 24,
                        encoding: 0,
                        compression: 0,
                        row_count: 3,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(3)),
                    },
                ],
            }],
        };
        let bytes = build_segment(&bad_footer, &row_group, &[]);
        match SegmentFileReader::from_bytes(bytes, schema_clone).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("row_count"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }

        // Confirm the good segment still opens so we know the
        // divergence is specifically the bad footer.
        SegmentFileReader::from_bytes(good_bytes, schema).expect("good segment still opens");
    }

    #[test]
    fn rejects_zero_row_groups() {
        let schema = simple_int_schema();
        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 0,
            row_group_count: 0,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 0),
            batch_id: 0,
            compaction_level: 0,
            dictionaries: vec![],
            row_groups: vec![],
        };
        let bytes = build_segment(&footer, &[], &[]);
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("zero row groups"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_footer_format_version_mismatch() {
        // §15 rule 7: footer.format_version must equal the v1
        // constant even though the *file header* already encoded it.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            f.format_version = 99;
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("format_version"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_illegal_compression_discriminant() {
        // §15 rule 10: compression must be in {0, 1}.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            f.row_groups[0].columns[3].compression = 7;
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("compression"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_column_ordinal_mismatch() {
        // §15 rule 10 (extended): the footer writer and reader both
        // expect column chunks to appear in table-schema ordinal
        // order. A metadata record whose `column_ordinal` doesn't
        // match its position is a writer bug.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            f.row_groups[0].columns[2].column_ordinal = 99;
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("column_ordinal"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_row_null_count_sum_mismatch() {
        // §15 rule 10 (extended): row_count + null_count must sum to
        // the parent row group's row_count.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            // Amount column claims 3 non-null rows, and we also set
            // null_count to 5 — totals to 8, not 3.
            f.row_groups[0].columns[3].null_count = 5;
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(
                    msg.contains("row_count") && msg.contains("null_count"),
                    "got: {msg}"
                )
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_column_chunk_range_exceeding_row_group() {
        // §15 rule 10: column chunk byte_offset + byte_length must
        // lie inside its parent row group's byte range.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            // Inflate the last column chunk's length so it runs past
            // the row group end.
            f.row_groups[0].columns[3].byte_length += 1024;
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("exceeds row group"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_dictionary_column_ordinal_out_of_bounds() {
        // §15 rule 11: dictionary.column_ordinal must address a
        // column in the footer schema.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            f.dictionaries.push(SegmentDictRef {
                column_ordinal: 99, // schema only has 4 columns
                byte_offset: f.row_groups[0].byte_offset + f.row_groups[0].byte_length,
                byte_length: 0,
                cardinality: 0,
                value_type: BqlType::Int,
            });
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("column_ordinal"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_row_group_size_hint_zero() {
        // Reader-side sanity: a zero row_group_size_hint is meaningless
        // and always a writer bug. Not verbatim in §15, but tightens
        // the contract slightly on the reader side.
        let schema = simple_int_schema();
        let bytes = build_mutated_segment(schema.clone(), &[1, 2, 3], |f| {
            f.row_group_size_hint = 0;
        });
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("row_group_size_hint"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    // ── Dictionary load ──────────────────────────────────────────────

    #[test]
    fn loads_segment_level_int_dictionary() {
        let schema = simple_int_schema();
        let dict_values: Vec<i64> = vec![10, 20, 30, 40];
        let dict_bytes = int_dictionary_bytes(&dict_values);
        let dict_region_len = dict_bytes.len() as u64;

        // One row group with empty chunks; the dictionary lives after
        // the row group but before the footer. The row-group bytes
        // are four 5-byte Plain empty chunks in a row.
        let mut rg_bytes = Vec::new();
        for _ in 0..4 {
            rg_bytes.extend_from_slice(&plain_empty_chunk());
        }
        let rg_byte_length = rg_bytes.len() as u64;
        let rg_off = FILE_HEADER_LEN as u64;
        let dict_off = rg_off + rg_byte_length;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 4,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 3),
            batch_id: 1,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 3, // amount — the Int column
                byte_offset: dict_off,
                byte_length: dict_region_len,
                cardinality: dict_values.len() as u32,
                value_type: BqlType::Int,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: rg_off,
                byte_length: rg_byte_length,
                row_count: 4,
                columns: vec![
                    ColumnChunkMeta {
                        column_ordinal: 0,
                        byte_offset: rg_off,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 1,
                        byte_offset: rg_off + 5,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 2,
                        byte_offset: rg_off + 10,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 3,
                        byte_offset: rg_off + 15,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    },
                ],
            }],
        };

        let bytes = build_segment(&footer, &rg_bytes, &dict_bytes);
        let reader = SegmentFileReader::from_bytes(bytes, schema).expect("open");
        assert_eq!(reader.dictionaries().len(), 1);
        match &reader.dictionaries()[0] {
            DictionaryValues::Int(v) => assert_eq!(v, &dict_values),
            other => panic!("expected Int dictionary, got {other:?}"),
        }
    }

    #[test]
    fn loads_segment_level_string_dictionary() {
        let schema = simple_int_schema();
        let dict_values: Vec<&str> = vec!["checkout", "signup", "view"];
        let dict_bytes = string_dictionary_bytes(&dict_values);
        let dict_region_len = dict_bytes.len() as u64;

        let mut rg_bytes = Vec::new();
        for _ in 0..4 {
            rg_bytes.extend_from_slice(&plain_empty_chunk());
        }
        let rg_byte_length = rg_bytes.len() as u64;
        let rg_off = FILE_HEADER_LEN as u64;
        let dict_off = rg_off + rg_byte_length;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 3,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 2),
            batch_id: 1,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 2, // event_type — a String column
                byte_offset: dict_off,
                byte_length: dict_region_len,
                cardinality: dict_values.len() as u32,
                value_type: BqlType::String,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: rg_off,
                byte_length: rg_byte_length,
                row_count: 3,
                columns: (0..4)
                    .map(|i| ColumnChunkMeta {
                        column_ordinal: i as u32,
                        byte_offset: rg_off + (i as u64 * 5),
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 3,
                        zone_min: None,
                        zone_max: None,
                    })
                    .collect(),
            }],
        };

        let bytes = build_segment(&footer, &rg_bytes, &dict_bytes);
        let reader = SegmentFileReader::from_bytes(bytes, schema).expect("open");
        match &reader.dictionaries()[0] {
            DictionaryValues::String(v) => {
                let expected: Vec<String> = dict_values.iter().map(|s| s.to_string()).collect();
                assert_eq!(v, &expected);
            }
            other => panic!("expected String dictionary, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsorted_int_dictionary() {
        let schema = simple_int_schema();
        // Build an unsorted Int dictionary — reader should reject it.
        let dict_values: Vec<i64> = vec![40, 10, 30, 20];
        let dict_bytes = int_dictionary_bytes(&dict_values);
        let dict_region_len = dict_bytes.len() as u64;

        let mut rg_bytes = Vec::new();
        for _ in 0..4 {
            rg_bytes.extend_from_slice(&plain_empty_chunk());
        }
        let rg_byte_length = rg_bytes.len() as u64;
        let rg_off = FILE_HEADER_LEN as u64;
        let dict_off = rg_off + rg_byte_length;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 4,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 3),
            batch_id: 1,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 3,
                byte_offset: dict_off,
                byte_length: dict_region_len,
                cardinality: dict_values.len() as u32,
                value_type: BqlType::Int,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: rg_off,
                byte_length: rg_byte_length,
                row_count: 4,
                columns: (0..4)
                    .map(|i| ColumnChunkMeta {
                        column_ordinal: i as u32,
                        byte_offset: rg_off + (i as u64 * 5),
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    })
                    .collect(),
            }],
        };

        let bytes = build_segment(&footer, &rg_bytes, &dict_bytes);
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("not sorted ascending"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unsorted_string_dictionary() {
        let schema = simple_int_schema();
        // "zebra", "apple" — clearly out of byte-wise lexicographic
        // order. The String sort-verify branch should catch this.
        let dict_values: Vec<&str> = vec!["zebra", "apple"];
        let dict_bytes = string_dictionary_bytes(&dict_values);
        let dict_region_len = dict_bytes.len() as u64;

        let mut rg_bytes = Vec::new();
        for _ in 0..4 {
            rg_bytes.extend_from_slice(&plain_empty_chunk());
        }
        let rg_byte_length = rg_bytes.len() as u64;
        let rg_off = FILE_HEADER_LEN as u64;
        let dict_off = rg_off + rg_byte_length;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 1,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 0),
            batch_id: 0,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 2, // event_type
                byte_offset: dict_off,
                byte_length: dict_region_len,
                cardinality: dict_values.len() as u32,
                value_type: BqlType::String,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: rg_off,
                byte_length: rg_byte_length,
                row_count: 1,
                columns: (0..4)
                    .map(|i| ColumnChunkMeta {
                        column_ordinal: i as u32,
                        byte_offset: rg_off + (i as u64 * 5),
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 1,
                        zone_min: None,
                        zone_max: None,
                    })
                    .collect(),
            }],
        };

        let bytes = build_segment(&footer, &rg_bytes, &dict_bytes);
        match SegmentFileReader::from_bytes(bytes, schema).unwrap_err() {
            BqliteError::Corruption(msg) => {
                assert!(msg.contains("not sorted ascending"), "got: {msg}")
            }
            other => panic!("expected Corruption, got {other:?}"),
        }
    }

    #[test]
    fn rejects_illegal_encoding_discriminant() {
        // Build a segment whose column-chunk metadata declares an
        // out-of-set encoding discriminant (`3` is reserved, not in
        // `{0, 1, 2, 4, 6}`). The column chunk bytes themselves do
        // not need to parse — the reader rejects the metadata long
        // before it touches the bytes — so we stub the row group
        // with three empty Plain chunks and only override the
        // metadata of the first.
        let one_col_schema = TableSchema::new(
            "t",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();

        let mut rg = Vec::new();
        rg.extend_from_slice(&plain_empty_chunk());
        rg.extend_from_slice(&plain_empty_chunk());
        rg.extend_from_slice(&plain_empty_chunk());
        let rg_byte_length = rg.len() as u64;
        let rg_off = FILE_HEADER_LEN as u64;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: one_col_schema.clone(),
            schema_version: 0,
            row_count: 1,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 0),
            batch_id: 0,
            compaction_level: 0,
            dictionaries: vec![],
            row_groups: vec![RowGroupIndex {
                byte_offset: rg_off,
                byte_length: rg_byte_length,
                row_count: 1,
                columns: vec![
                    ColumnChunkMeta {
                        column_ordinal: 0,
                        byte_offset: rg_off,
                        byte_length: 5,
                        encoding: 3, // illegal
                        compression: 0,
                        row_count: 0,
                        null_count: 1,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 1,
                        byte_offset: rg_off + 5,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 1,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 2,
                        byte_offset: rg_off + 10,
                        byte_length: 5,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 1,
                        zone_min: None,
                        zone_max: None,
                    },
                ],
            }],
        };

        let bytes = build_segment(&footer, &rg, &[]);
        match SegmentFileReader::from_bytes(bytes, one_col_schema).unwrap_err() {
            BqliteError::Corruption(msg) => assert!(msg.contains("encoding"), "got: {msg}"),
            other => panic!("expected Corruption, got {other:?}"),
        }
    }
}
