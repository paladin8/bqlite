//! v1 segment-file reader (TASK-215).
//!
//! Two layers live in this module:
//!
//! - [`SegmentFileReader`] — opens a v1 segment file from disk or
//!   an in-memory buffer, validates every §15 rule, parses the
//!   postcard footer, verifies the xxHash64 checksum, and eagerly
//!   loads every segment-level dictionary from the
//!   segment-dictionaries region. Pinned by
//!   `docs/design/storage/segment-format-v1.md` §4–§13.
//! - [`SegmentFileScan`] — a streaming row-group iterator produced
//!   by [`SegmentFileReader::scan`] and implementing the
//!   `bqlite_core::storage::SegmentScan` trait. Decodes column
//!   chunks on demand via the [`crate::encoding::Encoding`] trait,
//!   splices nulls back into dense Arrow arrays for nullable
//!   columns, handles schema-evolution backfill for columns
//!   introduced by `ALTER TABLE ADD COLUMN` after the segment was
//!   written, and prunes row groups whose zone maps are rejected
//!   by a pushed-down [`bqlite_core::storage::Predicate`].
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
//!
//! # Known limitations (deferred to later tasks)
//!
//! - **Default-value backfill for NOT NULL evolution columns.** If
//!   the current manifest schema declares a NOT NULL column the
//!   segment's write-time schema does not carry, the reader errors
//!   with `BqliteError::Schema`. Nullable evolution columns
//!   backfill with all-null arrays per §14.
//! - **Nested-type null splicing.** Splicing for `List` / `Map`
//!   columns is not implemented; the reader errors if asked to
//!   decode a nullable nested column. Non-nullable nested columns
//!   decode via the `Plain` encoding directly.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use ::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringViewArray,
    StringViewBuilder, TimestampNanosecondArray,
};
use ::arrow::buffer::{BooleanBuffer, NullBuffer};
use ::arrow::datatypes::{Field, Schema as ArrowSchema};
use bqlite_core::arrow::bql_type_to_arrow;
use bqlite_core::storage::{ColumnProjection, Predicate, SegmentScan, ZoneMap};
use bqlite_core::{BqlType, BqliteError, ColumnDef, Result, TableSchema};

use crate::encoding::dictionary::{
    payload_byte_count as dictionary_payload_byte_count, unpack_codes,
};
use crate::encoding::{
    decompress_lz4, Alp, BitPacking, BorrowedEncodedChunk, Constant, Delta, DoubleDelta, Encoding,
    EncodingType, ForEncoding, Pfor, Plain, Rle,
};
use crate::segment::layout::{
    ColumnChunkMeta, CompressionType, FooterV1, FooterV2, SegmentFooter, CHECKSUM_LEN,
    CHECKSUM_SEED, FILE_HEADER_LEN, FOOTER_SUFFIX_LEN, MAGIC, SEGMENT_FORMAT_VERSION_V1,
    SEGMENT_FORMAT_VERSION_V2, TRAILER_LEN,
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

/// Reader over a v1 or v2 segment file.
///
/// Constructed by reading a segment file from disk ([`Self::open`])
/// or from an in-memory byte buffer ([`Self::from_bytes`]). The
/// constructor runs every §15 validation rule (plus v2 additions
/// from `segment-format-v2.md` §11) — a `SegmentFileReader` value
/// is therefore a proof that the underlying bytes are a well-formed
/// segment.
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
    /// Parsed footer body (v1 or v2). The reader guarantees this
    /// struct has passed every validation rule.
    footer: Arc<SegmentFooter>,
    /// Segment-level dictionaries, indexed by footer dictionaries
    /// position. Loaded eagerly on open per §11.
    dictionaries: Arc<[DictionaryValues]>,
    /// Raw byte region for each segment-level dictionary, in the same
    /// order as [`Self::dictionaries`]. These are the on-disk
    /// dictionary-region bytes (Int: `n × i64 LE`; String:
    /// `[u32 LE length][utf8 bytes]…`) wrapped in an
    /// [`bqlite_core::encoded::ArcBytes`] so the encoded read path can
    /// hand them to `EncodedKind::Dictionary { values }` without
    /// reparsing. One copy at open; shared freely across scans.
    dict_bytes: Arc<[bqlite_core::encoded::ArcBytes]>,
    /// Current manifest schema the reader should project rows
    /// against, passed in by the caller. This is the target schema
    /// for name-based lookups during row-group decode (§14 schema
    /// evolution) — it may differ from the segment's write-time
    /// schema when a column has been added via `ALTER TABLE ADD
    /// COLUMN` since the segment was written.
    current_schema: Arc<TableSchema>,
}

impl SegmentFileReader {
    /// Open a segment file from disk.
    ///
    /// Reads the entire file into memory, then runs the same
    /// validation path as [`Self::from_bytes`]. Any I/O error is
    /// returned as `BqliteError::Io`; any format error is
    /// `BqliteError::Corruption`.
    ///
    /// Before the read, the opener issues a sequential-scan
    /// access-pattern hint to the kernel via
    /// [`crate::segment::advise::advise_sequential`] — Wave 2 only
    /// scans segments front-to-back, so `POSIX_FADV_SEQUENTIAL`
    /// is the single hint that actually matches every call path
    /// (see `docs/design/storage-format.md` §8.2 and TASK-243).
    /// The hint is advisory and uses the nearest platform-specific
    /// equivalent where available (`posix_fadvise` on Linux-like
    /// targets, Darwin `F_RDADVISE` on Apple targets).
    pub fn open<P: AsRef<Path>>(path: P, current_schema: TableSchema) -> Result<Self> {
        Self::open_shared(path, Arc::new(current_schema))
    }

    /// Open a segment file from disk with a pre-shared schema.
    ///
    /// Like [`Self::open`] but accepts an `Arc<TableSchema>` to avoid
    /// cloning the schema for every segment. Used by
    /// [`ManifestSegmentReader`] (TASK-247) which shares one Arc
    /// across all segments in a table.
    pub fn open_shared<P: AsRef<Path>>(path: P, current_schema: Arc<TableSchema>) -> Result<Self> {
        // Path comes from the manifest (trusted internal state), not user input.
        let mut file = fs::File::open(path)?; // nosemgrep

        // Hint the kernel before reading so it can start aggressive
        // readahead on the first page fault rather than waiting for
        // its own pattern detector to catch up.
        crate::segment::advise::advise_sequential(&file);

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Self::from_bytes_shared(bytes, current_schema)
    }

    /// Parse a segment file from an owned byte buffer.
    ///
    /// Runs every §15 validation rule in order (§15 rules 1–12)
    /// and eagerly loads every segment-level dictionary. On success
    /// the returned reader is guaranteed to satisfy the full format
    /// contract.
    pub fn from_bytes(bytes: Vec<u8>, current_schema: TableSchema) -> Result<Self> {
        Self::from_bytes_shared(bytes, Arc::new(current_schema))
    }

    /// Parse a segment file from an owned byte buffer with a
    /// pre-shared schema (TASK-247). Avoids wrapping the schema in
    /// a new `Arc` when the caller already holds one.
    pub fn from_bytes_shared(bytes: Vec<u8>, current_schema: Arc<TableSchema>) -> Result<Self> {
        let format_version = validate_header(&bytes)?;
        let footer_body_length = parse_trailer(&bytes)?;
        validate_framing_lengths(bytes.len(), footer_body_length)?;

        let footer_body_start = bytes.len() - CHECKSUM_LEN - TRAILER_LEN - footer_body_length;
        let footer_body_end = bytes.len() - CHECKSUM_LEN - TRAILER_LEN;
        let footer_body_bytes = &bytes[footer_body_start..footer_body_end];

        // Dispatch footer deserialization based on file header version.
        let footer: SegmentFooter = match format_version {
            SEGMENT_FORMAT_VERSION_V1 => {
                let v1: FooterV1 = postcard::from_bytes(footer_body_bytes).map_err(|e| {
                    BqliteError::Corruption(format!(
                        "v1 segment footer body failed to deserialize (postcard): {e}"
                    ))
                })?;
                SegmentFooter::V1(v1)
            }
            SEGMENT_FORMAT_VERSION_V2 => {
                let v2: FooterV2 = postcard::from_bytes(footer_body_bytes).map_err(|e| {
                    BqliteError::Corruption(format!(
                        "v2 segment footer body failed to deserialize (postcard): {e}"
                    ))
                })?;
                SegmentFooter::V2(v2)
            }
            // validate_header already rejects unknown versions, but
            // be explicit.
            _ => {
                return Err(BqliteError::Corruption(format!(
                    "unsupported segment format version {format_version}"
                )));
            }
        };

        validate_footer(&footer, footer_body_start, format_version)?;
        verify_checksum(&bytes)?;

        let dictionaries = load_dictionaries(&bytes, &footer)?;
        let dict_bytes = load_dict_bytes(&bytes, &footer);

        Ok(Self {
            bytes: Arc::from(bytes.into_boxed_slice()),
            footer: Arc::new(footer),
            dictionaries: Arc::from(dictionaries.into_boxed_slice()),
            dict_bytes: Arc::from(dict_bytes.into_boxed_slice()),
            current_schema,
        })
    }

    /// The parsed footer body (v1 or v2). Guaranteed to have passed
    /// every validation rule.
    pub fn footer(&self) -> &SegmentFooter {
        &self.footer
    }

    /// The segment's write-time schema — the shape the column
    /// chunks inside the file are encoded against. Callers
    /// projecting against schema evolution should use
    /// [`Self::current_schema`] instead.
    pub fn write_time_schema(&self) -> &TableSchema {
        self.footer.schema()
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
        self.footer.row_groups().len()
    }

    /// Total row count across every row group in the segment.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count()
    }

    /// The underlying byte buffer. Crate-private; used by the
    /// tests in this module to assert `Arc` sharing after a clone.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &Arc<[u8]> {
        &self.bytes
    }

    /// Construct a streaming scan over this segment.
    ///
    /// The scan materializes [`RecordBatch`]es one row group at a
    /// time via [`SegmentScan::next_row_group`]. Column chunks are
    /// decoded lazily — the reader touches a chunk's bytes only when
    /// the scan reaches that row group and that column is in the
    /// projection.
    ///
    /// `projection.is_all()` returns every column in the segment's
    /// write-time schema in ordinal order; an explicit column list
    /// returns exactly those columns in the requested order. Column
    /// names are resolved against the current manifest schema first;
    /// columns that don't exist in the current schema error with
    /// [`BqliteError::Schema`]. Columns that exist in the current
    /// schema but not in the segment's write-time schema are
    /// **backfilled with all-null values** — matching the schema
    /// evolution rule from `segment-format-v1.md` §14. Default
    /// values for backfilled columns are **not yet supported** (see
    /// the module-level TODO note).
    ///
    /// # Errors
    ///
    /// - [`BqliteError::Schema`] if any projected column is not in
    ///   the current schema, or if a column's type in the current
    ///   schema disagrees with the same column's type in the
    ///   segment's write-time schema.
    pub fn scan(
        &self,
        projection: &ColumnProjection,
        predicate: Option<Arc<dyn Predicate>>,
    ) -> Result<SegmentFileScan> {
        let plan = build_scan_plan(&self.current_schema, self.footer.schema(), projection)?;
        Ok(SegmentFileScan {
            bytes: self.bytes.clone(),
            footer: self.footer.clone(),
            dictionaries: self.dictionaries.clone(),
            dict_bytes: self.dict_bytes.clone(),
            plan,
            predicate,
            next_idx: 0,
            exhausted: false,
        })
    }
}

impl std::fmt::Debug for SegmentFileReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentFileReader")
            .field("file_size", &self.bytes.len())
            .field("format_version", &self.footer.format_version())
            .field("row_count", &self.footer.row_count())
            .field("row_group_count", &self.footer.row_group_count())
            .field("dictionaries", &self.dictionaries.len())
            .field("schema", &self.footer.schema().name())
            .finish()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scan — streaming row-group iterator
// ─────────────────────────────────────────────────────────────────────────────

/// Streaming iterator over a single segment's row groups.
///
/// Constructed via [`SegmentFileReader::scan`] and typically held
/// by a scan operator (TASK-230) for the lifetime of a query. Each
/// call to [`SegmentScan::next_row_group`] decodes one row group into
/// a [`RecordBatch`] and advances the iterator; dropping the scan
/// releases every borrowed `Arc`.
///
/// # Resource sharing
///
/// Clones the reader's `Arc<[u8]>`, `Arc<SegmentFooter>`, and
/// `Arc<[DictionaryValues]>`. A `SegmentFileScan` is therefore cheap
/// to create — the only per-scan allocations are the [`ScanPlan`]
/// and the predicate pointer.
pub struct SegmentFileScan {
    bytes: Arc<[u8]>,
    footer: Arc<SegmentFooter>,
    dictionaries: Arc<[DictionaryValues]>,
    /// Dict region bytes indexed by dict_id — see
    /// [`SegmentFileReader::dict_bytes`].
    dict_bytes: Arc<[bqlite_core::encoded::ArcBytes]>,
    plan: ScanPlan,
    predicate: Option<Arc<dyn Predicate>>,
    next_idx: usize,
    exhausted: bool,
}

/// Pre-resolved projection: for every output column, how to
/// materialize its Arrow array when decoding a row group.
///
/// Built once in [`SegmentFileReader::scan`] and kept immutable for
/// the scan's lifetime — the hot-path decoder walks this vector,
/// not the raw [`ColumnProjection`].
struct ScanPlan {
    /// Arrow schema for the output batches, matching `entries`
    /// order.
    arrow_schema: Arc<ArrowSchema>,
    entries: Vec<PlannedColumn>,
}

/// One output column's decode plan.
struct PlannedColumn {
    /// Output `BqlType` — the type the Arrow field in the batch
    /// schema must match.
    output_type: BqlType,
    /// Source of the values.
    source: PlannedColumnSource,
}

/// How a planned column's values come from a row group.
enum PlannedColumnSource {
    /// Decode a column chunk from the segment file at the given
    /// write-time ordinal. The cached [`ColumnDef`] carries the
    /// nullable flag and `bql_type` the decoder needs.
    FromSegment {
        write_time_ordinal: usize,
        write_time_col: ColumnDef,
    },
    /// Fabricate an all-null Arrow array of the output type — the
    /// column exists in the current schema but not in the segment's
    /// write-time schema, so the segment predates the `ALTER TABLE
    /// ADD COLUMN` that introduced it.
    BackfillAllNull,
}

fn build_scan_plan(
    current_schema: &TableSchema,
    write_time_schema: &TableSchema,
    projection: &ColumnProjection,
) -> Result<ScanPlan> {
    let mut entries: Vec<PlannedColumn> = Vec::new();
    let mut arrow_fields: Vec<Field> = Vec::new();

    // Always iterate columns in table-schema order so that
    // `CompiledNode::Column { index }` values (which are compiled
    // against the full table-schema ordinals) remain valid for any
    // pruned subset. For the "all columns" case this is the natural
    // order; for an explicit projection we filter the full schema
    // to the requested names, preserving schema order rather than
    // the projection's (potentially sorted) order.
    let column_names: Vec<String> = if projection.is_all() {
        current_schema
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect()
    } else {
        let projected: std::collections::HashSet<&str> =
            projection.columns().iter().map(String::as_str).collect();
        current_schema
            .columns()
            .iter()
            .filter(|c| projected.contains(c.name.as_str()))
            .map(|c| c.name.clone())
            .collect()
    };
    // Validate that every explicitly requested name appears in the
    // current schema (the schema-order iteration above silently drops
    // unknown names, so we need an explicit check).
    if !projection.is_all() {
        for name in projection.columns() {
            if current_schema.columns().iter().all(|c| c.name != *name) {
                return Err(BqliteError::Schema(format!(
                    "segment reader: column `{name}` not found in current schema `{}`",
                    current_schema.name()
                )));
            }
        }
    }

    for name in column_names {
        // `column_names` is built by filtering `current_schema.columns()` in
        // both the `is_all()` and explicit projection paths, so every name
        // here is guaranteed to be present in `current_schema`. The
        // `.ok_or_else()` below is unreachable for well-formed inputs and
        // exists only as a defensive guard against future code changes.
        let current_col = current_schema
            .columns()
            .iter()
            .find(|c| c.name == name)
            .ok_or_else(|| {
                BqliteError::Schema(format!(
                    "segment reader: column `{name}` not found in current schema `{}`",
                    current_schema.name()
                ))
            })?;

        let source = match write_time_schema
            .columns()
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == name)
        {
            Some((ordinal, wt_col)) => {
                if wt_col.bql_type != current_col.bql_type {
                    return Err(BqliteError::Schema(format!(
                        "segment reader: column `{name}` has type {:?} in the segment \
                             but {:?} in the current schema",
                        wt_col.bql_type, current_col.bql_type
                    )));
                }
                PlannedColumnSource::FromSegment {
                    write_time_ordinal: ordinal,
                    write_time_col: wt_col.clone(),
                }
            }
            None => {
                if !current_col.nullable {
                    return Err(BqliteError::Schema(format!(
                        "segment reader: column `{name}` is not in the segment's \
                             write-time schema and the current schema marks it NOT NULL; \
                             defaults are not yet supported by the reader"
                    )));
                }
                PlannedColumnSource::BackfillAllNull
            }
        };

        arrow_fields.push(Field::new(
            current_col.name.clone(),
            bql_type_to_arrow(&current_col.bql_type),
            current_col.nullable,
        ));
        entries.push(PlannedColumn {
            output_type: current_col.bql_type.clone(),
            source,
        });
    }

    Ok(ScanPlan {
        arrow_schema: Arc::new(ArrowSchema::new(arrow_fields)),
        entries,
    })
}

impl SegmentScan for SegmentFileScan {
    fn row_group_count(&self) -> usize {
        self.footer.row_groups().len()
    }

    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
        let rg = self.footer.row_groups().get(idx).ok_or_else(|| {
            BqliteError::Execution(format!(
                "segment reader: row group index {idx} out of range (total {})",
                self.footer.row_groups().len()
            ))
        })?;
        let mut map = HashMap::with_capacity(rg.columns.len());
        for meta in &rg.columns {
            let name = self
                .footer
                .schema()
                .columns()
                .get(meta.column_ordinal as usize)
                .map(|c| c.name.clone());
            if let Some(name) = name {
                map.insert(
                    name,
                    ZoneMap {
                        min: meta.zone_min.clone(),
                        max: meta.zone_max.clone(),
                        null_count: meta.null_count,
                        row_count: rg.row_count,
                    },
                );
            }
        }
        Ok(map)
    }

    fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            if self.next_idx >= self.footer.row_groups().len() {
                self.exhausted = true;
                return Ok(None);
            }
            let idx = self.next_idx;
            self.next_idx += 1;

            // Predicate pruning — two paths (TASK-247):
            //
            // 1. **Inline fast path:** when the predicate downcasts
            //    to a concrete `ScanPredicate` (pointer comparison,
            //    effectively free), evaluate conjuncts directly
            //    against the footer's `ColumnChunkMeta` zone-map
            //    fields without constructing a `HashMap`. This avoids
            //    a heap allocation per row-group and is the common
            //    production path (the scan operator always wraps a
            //    `ScanPredicate`).
            //
            // 2. **Trait fallback:** for any other `Predicate` impl
            //    (tests, future custom predicates), delegate to the
            //    original `zone_map::should_decode_row_group` which
            //    builds the `HashMap` and calls the trait method.
            if let Some(pred) = &self.predicate {
                if let Some(sp) = pred
                    .as_any()
                    .downcast_ref::<bqlite_core::storage::ScanPredicate>()
                {
                    let rg = &self.footer.row_groups()[idx];
                    if !crate::zone_map::accepts_row_group_inline(
                        sp,
                        rg,
                        self.footer.schema().columns(),
                    ) {
                        continue;
                    }
                } else {
                    let scan: &dyn SegmentScan = &*self;
                    if !crate::zone_map::should_decode_row_group(scan, &**pred, idx)? {
                        continue;
                    }
                }
            }

            return self.decode_row_group(idx).map(Some);
        }
    }

    fn next_encoded_row_group(&mut self) -> Result<Option<bqlite_core::encoded::EncodedBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            if self.next_idx >= self.footer.row_groups().len() {
                self.exhausted = true;
                return Ok(None);
            }
            let idx = self.next_idx;
            self.next_idx += 1;

            // Same zone-map pruning as `next_row_group`. Kept in
            // lockstep so the encoded path observes identical
            // row-group selection.
            if let Some(pred) = &self.predicate {
                if let Some(sp) = pred
                    .as_any()
                    .downcast_ref::<bqlite_core::storage::ScanPredicate>()
                {
                    let rg = &self.footer.row_groups()[idx];
                    if !crate::zone_map::accepts_row_group_inline(
                        sp,
                        rg,
                        self.footer.schema().columns(),
                    ) {
                        continue;
                    }
                } else {
                    let scan: &dyn SegmentScan = &*self;
                    if !crate::zone_map::should_decode_row_group(scan, &**pred, idx)? {
                        continue;
                    }
                }
            }

            return self.decode_encoded_row_group(idx).map(Some);
        }
    }
}

impl SegmentFileScan {
    /// Decode row group `idx` to a [`RecordBatch`], honoring the
    /// scan's projection plan.
    fn decode_row_group(&self, idx: usize) -> Result<RecordBatch> {
        let rg = &self.footer.row_groups()[idx];
        let row_count = rg.row_count as usize;

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.plan.entries.len());
        for entry in &self.plan.entries {
            let array = match &entry.source {
                PlannedColumnSource::FromSegment {
                    write_time_ordinal,
                    write_time_col,
                } => {
                    let meta = &rg.columns[*write_time_ordinal];
                    decode_column_chunk(
                        &self.bytes,
                        meta,
                        write_time_col,
                        row_count,
                        &self.dictionaries,
                        self.footer.format_version(),
                    )?
                }
                PlannedColumnSource::BackfillAllNull => {
                    backfill_all_null(&entry.output_type, row_count)?
                }
            };
            columns.push(array);
        }

        RecordBatch::try_new(self.plan.arrow_schema.clone(), columns).map_err(|e| {
            BqliteError::Execution(format!(
                "segment reader: failed to assemble row group {idx}: {e}"
            ))
        })
    }

    /// Build an [`EncodedBatch`] for row group `idx` using the CP2
    /// `pin_column_chunk` helper. Supported encodings return real
    /// `EncodedColumn::Encoded` views; everything else falls back to
    /// `EncodedColumn::Materialized` backed by the existing
    /// `decode_column_chunk` path.
    fn decode_encoded_row_group(&self, idx: usize) -> Result<bqlite_core::encoded::EncodedBatch> {
        use bqlite_core::encoded::EncodedColumn;

        let rg = &self.footer.row_groups()[idx];
        let row_count = rg.row_count as usize;

        let mut columns: Vec<EncodedColumn> = Vec::with_capacity(self.plan.entries.len());
        for entry in &self.plan.entries {
            let col = match &entry.source {
                PlannedColumnSource::FromSegment {
                    write_time_ordinal,
                    write_time_col,
                } => {
                    let meta = &rg.columns[*write_time_ordinal];
                    let bytes = self.bytes.clone();
                    let dictionaries = self.dictionaries.clone();
                    let wt_col = write_time_col.clone();
                    let meta_clone = meta.clone();
                    let format_version = self.footer.format_version();
                    crate::segment::encoded::pin_column_chunk(
                        &self.bytes,
                        meta,
                        write_time_col,
                        row_count,
                        Some(&self.dict_bytes),
                        format_version,
                        move || {
                            decode_column_chunk(
                                &bytes,
                                &meta_clone,
                                &wt_col,
                                row_count,
                                &dictionaries,
                                format_version,
                            )
                        },
                    )?
                }
                PlannedColumnSource::BackfillAllNull => EncodedColumn::Materialized {
                    array: backfill_all_null(&entry.output_type, row_count)?,
                    rows: row_count as u32,
                },
            };
            columns.push(col);
        }

        Ok(bqlite_core::encoded::EncodedBatch::new(
            row_count as u32,
            columns,
        ))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Column chunk decoder
// ─────────────────────────────────────────────────────────────────────────────

/// Decode one column chunk: parse null bitmap + encoding header,
/// (optionally) LZ4-decompress the payload, decode the dense values
/// either via the [`Encoding`] trait or the loaded segment-level
/// dictionary, and splice the null bitmap back into the dense result.
fn decode_column_chunk(
    bytes: &[u8],
    meta: &ColumnChunkMeta,
    write_time_col: &ColumnDef,
    row_group_row_count: usize,
    dictionaries: &[DictionaryValues],
    format_version: u16,
) -> Result<ArrayRef> {
    let chunk_start = meta.byte_offset as usize;
    let chunk_end = chunk_start + meta.byte_length as usize;
    if chunk_end > bytes.len() {
        return Err(BqliteError::Corruption(format!(
            "segment reader: column chunk for ordinal {} extends beyond file ({} > {})",
            meta.column_ordinal,
            chunk_end,
            bytes.len()
        )));
    }
    let chunk_bytes = &bytes[chunk_start..chunk_end];
    let mut cursor = 0usize;

    // 1. Null bitmap (if the column is nullable).
    //
    // TASK-247: reference the bitmap slice directly from the segment
    // bytes instead of copying into a Vec. `splice_nulls` already
    // takes `&[u8]`, so the downstream path is unchanged.
    let null_bitmap_range = if write_time_col.nullable {
        let bitmap_len = row_group_row_count.div_ceil(8);
        if chunk_bytes.len() < bitmap_len {
            return Err(BqliteError::Corruption(format!(
                "segment reader: column chunk for `{}` too short for null bitmap \
                 (expected at least {} bytes, got {})",
                write_time_col.name,
                bitmap_len,
                chunk_bytes.len()
            )));
        }
        cursor += bitmap_len;
        Some(0..bitmap_len)
    } else {
        None
    };

    // 2. Encoding discriminant.
    if chunk_bytes.len() <= cursor {
        return Err(BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}` missing encoding discriminant",
            write_time_col.name
        )));
    }
    let encoding_byte = chunk_bytes[cursor];
    cursor += 1;
    let encoding = EncodingType::from_discriminant_versioned(encoding_byte, format_version)
        .map_err(|e| {
            BqliteError::Corruption(format!(
                "segment reader: column chunk for `{}`: {e}",
                write_time_col.name
            ))
        })?;

    // 3. Encoding-specific params.
    let params_start = cursor;
    let params_len =
        parse_encoding_params_len(encoding, &chunk_bytes[cursor..], &write_time_col.bql_type)
            .map_err(|e| {
                BqliteError::Corruption(format!(
                    "segment reader: column chunk for `{}`: {e}",
                    write_time_col.name
                ))
            })?;
    cursor += params_len;
    let on_disk_params = &chunk_bytes[params_start..params_start + params_len];

    // 4. uncompressed_payload_length: u32 LE.
    if chunk_bytes.len() < cursor + 4 {
        return Err(BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}` missing uncompressed_payload_length",
            write_time_col.name
        )));
    }
    let uncompressed_payload_length = u32::from_le_bytes(
        chunk_bytes[cursor..cursor + 4]
            .try_into()
            .expect("slice length checked above"),
    ) as usize;
    cursor += 4;

    // 5. Payload bytes (compressed or raw).
    let on_disk_payload = &chunk_bytes[cursor..];

    // 6. Decompress if needed.
    let compression = CompressionType::from_discriminant(meta.compression).map_err(|e| {
        BqliteError::Corruption(format!(
            "segment reader: column chunk for `{}`: {e}",
            write_time_col.name
        ))
    })?;
    let uncompressed_payload: Cow<'_, [u8]> = match compression {
        CompressionType::None => {
            if on_disk_payload.len() != uncompressed_payload_length {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: column chunk for `{}`: uncompressed payload length \
                     disagrees with on-disk size ({} vs {})",
                    write_time_col.name,
                    uncompressed_payload_length,
                    on_disk_payload.len()
                )));
            }
            Cow::Borrowed(on_disk_payload)
        }
        CompressionType::Lz4 => Cow::Owned(
            decompress_lz4(on_disk_payload, uncompressed_payload_length).map_err(|e| {
                BqliteError::Corruption(format!(
                    "segment reader: column chunk for `{}`: lz4 decompress failed: {e}",
                    write_time_col.name
                ))
            })?,
        ),
    };

    // 7. Decode the dense non-null values. Every encoding except
    //    `Dictionary` routes through the encoding trait's borrowed
    //    fast path so the common uncompressed case can hand the
    //    decoder slices directly. `Dictionary` can decode directly
    //    from the already-loaded segment-level dictionary, avoiding
    //    per-row-group params reconstruction and reparse.
    let dense_array: ArrayRef = match encoding {
        EncodingType::Dictionary => {
            if on_disk_params.len() != 5 {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Dictionary on-disk params for `{}` are {} bytes (expected 5)",
                    write_time_col.name,
                    on_disk_params.len()
                )));
            }
            let dict_id = u32::from_le_bytes(
                on_disk_params[..4]
                    .try_into()
                    .expect("slice length checked above"),
            ) as usize;
            let code_bit_width = on_disk_params[4];
            if dict_id >= dictionaries.len() {
                return Err(BqliteError::Corruption(format!(
                    "segment reader: Dictionary dict_id {dict_id} out of range \
                     (segment has {} dictionaries)",
                    dictionaries.len()
                )));
            }
            let dict_values = &dictionaries[dict_id];
            decode_dictionary_chunk(
                uncompressed_payload.as_ref(),
                meta.row_count as usize,
                dict_values,
                code_bit_width,
                &write_time_col.bql_type,
            )?
        }
        // NOTE: When TASK-419 wires the v2 reader, FSST will need its
        // own match arm here (like Dictionary above) to access the
        // segment-level FSST symbol tables.
        EncodingType::Plain
        | EncodingType::Delta
        | EncodingType::DoubleDelta
        | EncodingType::BitPacking
        | EncodingType::Constant
        | EncodingType::Rle
        | EncodingType::Fsst
        | EncodingType::For
        | EncodingType::PFor
        | EncodingType::Alp => {
            let chunk = BorrowedEncodedChunk {
                encoding,
                params: on_disk_params,
                payload: uncompressed_payload.as_ref(),
                row_count: meta.row_count as usize,
            };
            dispatch_decode(encoding, chunk, &write_time_col.bql_type)?
        }
    };

    // 9. Splice nulls back in (if the column is nullable).
    if let Some(range) = null_bitmap_range {
        splice_nulls(
            &dense_array,
            &chunk_bytes[range],
            row_group_row_count,
            &write_time_col.bql_type,
        )
    } else {
        // Non-nullable column: the dense array must already have the
        // correct length.
        if dense_array.len() != row_group_row_count {
            return Err(BqliteError::Corruption(format!(
                "segment reader: column `{}` decoded to {} rows, expected {}",
                write_time_col.name,
                dense_array.len(),
                row_group_row_count
            )));
        }
        Ok(dense_array)
    }
}

/// Length in bytes of the on-disk encoding params block, computed
/// by consuming just enough of the buffer to know the full header
/// size. For every encoding except `Constant` this is a fixed
/// per-discriminant value; `Constant` needs the column type and the
/// `value_kind` byte to determine the length.
pub(super) fn parse_encoding_params_len_pub(
    encoding: EncodingType,
    after_discriminant: &[u8],
    col_type: &BqlType,
) -> std::result::Result<usize, String> {
    parse_encoding_params_len(encoding, after_discriminant, col_type)
}

fn parse_encoding_params_len(
    encoding: EncodingType,
    after_discriminant: &[u8],
    col_type: &BqlType,
) -> std::result::Result<usize, String> {
    match encoding {
        EncodingType::Plain => Ok(0),
        // `dict_id: u32 LE` + `code_bit_width: u8`.
        EncodingType::Dictionary => Ok(5),
        // `base_value: i64 LE` + `residual_bit_width: u8`.
        EncodingType::Delta => Ok(9),
        // `base_value: i64 LE` + `first_delta: i64 LE` + `dd_bit_width: u8`.
        EncodingType::DoubleDelta => Ok(17),
        // `min_value: i64 LE` + `bit_width: u8`.
        EncodingType::BitPacking => Ok(9),
        // `run_count: u32 LE`.
        EncodingType::Rle => Ok(4),
        // `block_size: u16 LE` + `block_count: u32 LE`.
        EncodingType::For => Ok(6),
        EncodingType::Constant => {
            if after_discriminant.is_empty() {
                return Err("Constant encoding missing value_kind byte".to_string());
            }
            let value_kind = after_discriminant[0];
            if value_kind == 1 {
                // All-null: exactly `value_kind`.
                return Ok(1);
            }
            if value_kind != 0 {
                return Err(format!("Constant value_kind {value_kind} is not 0 or 1"));
            }
            let literal_len = match col_type {
                BqlType::Bool => 1,
                BqlType::Int | BqlType::Float | BqlType::Timestamp => 8,
                BqlType::String => {
                    if after_discriminant.len() < 5 {
                        return Err(
                            "Constant encoding with String literal missing length prefix"
                                .to_string(),
                        );
                    }
                    let s_len = u32::from_le_bytes(
                        after_discriminant[1..5]
                            .try_into()
                            .expect("slice length checked above"),
                    ) as usize;
                    4 + s_len
                }
                BqlType::List(_) | BqlType::Map(_) => {
                    return Err(format!(
                        "Constant encoding does not support nested type {col_type:?}"
                    ));
                }
            };
            Ok(1 + literal_len)
        }
        // v2 encoding params sizes per segment-format-v2.md §5.
        // symbol_table_id(4)
        EncodingType::Fsst => Ok(4),
        // block_size(2) + block_count(4)
        EncodingType::PFor => Ok(6),
        // exponent(1) + factor(8) + patch_count(4) + for_block_size(2) + for_block_count(4)
        EncodingType::Alp => Ok(19),
    }
}

/// Decode a Dictionary-encoded chunk using the segment-level
/// dictionary values loaded when the reader opened the segment.
///
/// This bypasses the trait-level self-contained `params` round-trip:
/// the reader already has the dictionary in typed form, so rebuilding
/// `[type_tag][code_bit_width][cardinality][dict_values...]` just to
/// parse it again would be pure copy/reparse churn on every row group.
fn decode_dictionary_chunk(
    payload: &[u8],
    row_count: usize,
    dict_values: &DictionaryValues,
    code_bit_width: u8,
    ty: &BqlType,
) -> Result<ArrayRef> {
    let expected_payload_len = dictionary_payload_byte_count(row_count, code_bit_width);
    if payload.len() != expected_payload_len {
        return Err(BqliteError::Execution(format!(
            "segment reader: Dictionary payload length {} does not match \
             {row_count} rows at {code_bit_width}-bit codes ({expected_payload_len} bytes)",
            payload.len()
        )));
    }

    let codes = unpack_codes(payload, row_count, code_bit_width)?;
    match (dict_values, ty) {
        (DictionaryValues::Int(dict), BqlType::Int) => {
            let mut values = Vec::with_capacity(row_count);
            for code in codes {
                let idx = code as usize;
                if idx >= dict.len() {
                    return Err(BqliteError::Execution(format!(
                        "segment reader: Dictionary code {code} out of bounds \
                         for Int dictionary of size {}",
                        dict.len()
                    )));
                }
                values.push(dict[idx]);
            }
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        (DictionaryValues::String(dict), BqlType::String) => {
            let mut builder = StringViewBuilder::with_capacity(row_count);
            for code in codes {
                let idx = code as usize;
                if idx >= dict.len() {
                    return Err(BqliteError::Execution(format!(
                        "segment reader: Dictionary code {code} out of bounds \
                         for String dictionary of size {}",
                        dict.len()
                    )));
                }
                builder.append_value(&dict[idx]);
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        (DictionaryValues::Int(_), other) | (DictionaryValues::String(_), other) => {
            Err(BqliteError::Execution(format!(
                "segment reader: Dictionary values do not match requested type {other:?}"
            )))
        }
    }
}

/// Dispatch a borrowed chunk view to the concrete [`Encoding`] impl.
/// Box allocation is avoided by stack-dispatching each variant to its
/// zero-sized impl.
fn dispatch_decode(
    encoding: EncodingType,
    chunk: BorrowedEncodedChunk<'_>,
    ty: &BqlType,
) -> Result<ArrayRef> {
    match encoding {
        EncodingType::Plain => Plain.decode_borrowed(chunk, ty),
        EncodingType::Dictionary => Err(BqliteError::Execution(
            "segment reader: Dictionary chunks are decoded directly from loaded segment dictionaries"
                .into(),
        )),
        EncodingType::Delta => Delta.decode_borrowed(chunk, ty),
        EncodingType::DoubleDelta => DoubleDelta.decode_borrowed(chunk, ty),
        EncodingType::BitPacking => BitPacking.decode_borrowed(chunk, ty),
        EncodingType::Constant => Constant.decode_borrowed(chunk, ty),
        EncodingType::Rle => Rle.decode_borrowed(chunk, ty),
        EncodingType::For => ForEncoding.decode_borrowed(chunk, ty),
        EncodingType::Alp => Alp.decode_borrowed(chunk, ty),
        EncodingType::PFor => Pfor.decode_borrowed(chunk, ty),
        // Fsst decode is wired by TASK-416 through a dedicated path that
        // needs the segment-level symbol table; the reader arm above this
        // dispatch already carries a TODO pointing there.
        EncodingType::Fsst => Err(BqliteError::Execution(format!(
            "v2 encoding {encoding:?} decode not yet implemented (TASK-416)"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Null splicing
// ─────────────────────────────────────────────────────────────────────────────

/// Splice a dense Arrow array (no nulls, `non_null_count` rows)
/// back into a nullable array of `row_group_row_count` rows using
/// the given null bitmap.
///
/// The null bitmap is Arrow LSB-first: bit `i == 1` means row `i`
/// is non-null. Non-null rows consume values from `dense` in order.
/// Null rows carry a type-appropriate placeholder (`0` / empty
/// string / `false`) and have their null-buffer bit cleared.
fn splice_nulls(
    dense: &ArrayRef,
    null_bitmap: &[u8],
    row_group_row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    // Validate the dense length first — `Encoding::decode` returns
    // exactly `chunk.row_count` values, which equals
    // `row_group_row_count - null_count`. We recompute it from the
    // bitmap to catch a writer that wrote an inconsistent chunk.
    let non_null_count = count_set_bits(null_bitmap, row_group_row_count);
    if dense.len() != non_null_count {
        return Err(BqliteError::Corruption(format!(
            "segment reader: dense array length {} disagrees with non-null count \
             {} from the null bitmap ({} rows)",
            dense.len(),
            non_null_count,
            row_group_row_count
        )));
    }

    let boolean_buffer = BooleanBuffer::new(
        ::arrow::buffer::Buffer::from_slice_ref(null_bitmap),
        0,
        row_group_row_count,
    );
    let null_buffer = NullBuffer::new(boolean_buffer);

    match ty {
        BqlType::Int => splice_primitive_i64(dense, row_group_row_count, null_buffer),
        BqlType::Float => splice_primitive_f64(dense, row_group_row_count, null_buffer),
        BqlType::Timestamp => splice_primitive_timestamp(dense, row_group_row_count, null_buffer),
        BqlType::Bool => splice_bool(dense, row_group_row_count, null_buffer),
        BqlType::String => splice_string(dense, row_group_row_count, &null_buffer),
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "segment reader: null splicing for nested type {ty:?} is not yet implemented"
        ))),
    }
}

pub(super) fn count_set_bits_pub(bitmap: &[u8], len: usize) -> usize {
    count_set_bits(bitmap, len)
}

pub(super) fn splice_nulls_pub(
    dense: &ArrayRef,
    null_bitmap: &[u8],
    row_group_row_count: usize,
    ty: &BqlType,
) -> Result<ArrayRef> {
    splice_nulls(dense, null_bitmap, row_group_row_count, ty)
}

/// Count the number of bits set to 1 in the first `len` bits of
/// `bitmap` (LSB-first).
fn count_set_bits(bitmap: &[u8], len: usize) -> usize {
    let full_bytes = len / 8;
    let tail_bits = len % 8;
    let mut count: usize = 0;
    for b in &bitmap[..full_bytes] {
        count += b.count_ones() as usize;
    }
    if tail_bits > 0 {
        let last = bitmap[full_bytes];
        let mask = (1u8 << tail_bits) - 1;
        count += (last & mask).count_ones() as usize;
    }
    count
}

fn splice_primitive_i64(
    dense: &ArrayRef,
    row_group_row_count: usize,
    null_buffer: NullBuffer,
) -> Result<ArrayRef> {
    let dense = dense.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        BqliteError::Execution("segment reader: expected Int64Array dense input".into())
    })?;
    let mut full = vec![0i64; row_group_row_count];
    let mut dense_idx = 0usize;
    for (i, slot) in full.iter_mut().enumerate().take(row_group_row_count) {
        if null_buffer.is_valid(i) {
            *slot = dense.value(dense_idx);
            dense_idx += 1;
        }
    }
    let arr = Int64Array::new(full.into(), Some(null_buffer));
    Ok(Arc::new(arr))
}

fn splice_primitive_f64(
    dense: &ArrayRef,
    row_group_row_count: usize,
    null_buffer: NullBuffer,
) -> Result<ArrayRef> {
    let dense = dense
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            BqliteError::Execution("segment reader: expected Float64Array dense input".into())
        })?;
    let mut full = vec![0.0f64; row_group_row_count];
    let mut dense_idx = 0usize;
    for (i, slot) in full.iter_mut().enumerate().take(row_group_row_count) {
        if null_buffer.is_valid(i) {
            *slot = dense.value(dense_idx);
            dense_idx += 1;
        }
    }
    let arr = Float64Array::new(full.into(), Some(null_buffer));
    Ok(Arc::new(arr))
}

fn splice_primitive_timestamp(
    dense: &ArrayRef,
    row_group_row_count: usize,
    null_buffer: NullBuffer,
) -> Result<ArrayRef> {
    let dense = dense
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            BqliteError::Execution(
                "segment reader: expected TimestampNanosecondArray dense input".into(),
            )
        })?;
    let mut full = vec![0i64; row_group_row_count];
    let mut dense_idx = 0usize;
    for (i, slot) in full.iter_mut().enumerate().take(row_group_row_count) {
        if null_buffer.is_valid(i) {
            *slot = dense.value(dense_idx);
            dense_idx += 1;
        }
    }
    let arr = TimestampNanosecondArray::new(full.into(), Some(null_buffer)).with_timezone("UTC");
    Ok(Arc::new(arr))
}

fn splice_bool(
    dense: &ArrayRef,
    row_group_row_count: usize,
    null_buffer: NullBuffer,
) -> Result<ArrayRef> {
    let dense = dense
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            BqliteError::Execution("segment reader: expected BooleanArray dense input".into())
        })?;
    let mut full = vec![false; row_group_row_count];
    let mut dense_idx = 0usize;
    for (i, slot) in full.iter_mut().enumerate().take(row_group_row_count) {
        if null_buffer.is_valid(i) {
            *slot = dense.value(dense_idx);
            dense_idx += 1;
        }
    }
    let bool_buffer = BooleanBuffer::from_iter(full);
    let arr = BooleanArray::new(bool_buffer, Some(null_buffer));
    Ok(Arc::new(arr))
}

fn splice_string(
    dense: &ArrayRef,
    row_group_row_count: usize,
    null_buffer: &NullBuffer,
) -> Result<ArrayRef> {
    // Strings don't have the fixed-width "values buffer + null buffer"
    // layout the primitive arrays use, so splicing still has to
    // rebuild the view array. Use `StringViewBuilder` directly to
    // avoid the previous `Vec<Option<String>>` transient and its
    // per-row owned-string allocations.
    let dense = dense
        .as_any()
        .downcast_ref::<StringViewArray>()
        .ok_or_else(|| {
            BqliteError::Execution("segment reader: expected StringViewArray dense input".into())
        })?;
    let mut builder = StringViewBuilder::with_capacity(row_group_row_count);
    let mut dense_idx = 0usize;
    for i in 0..row_group_row_count {
        if null_buffer.is_valid(i) {
            builder.append_value(dense.value(dense_idx));
            dense_idx += 1;
        } else {
            builder.append_null();
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Fabricate an all-null Arrow array of the given BQL type for
/// schema-evolution backfill. Used when the current schema has a
/// column that the segment's write-time schema doesn't.
fn backfill_all_null(ty: &BqlType, row_count: usize) -> Result<ArrayRef> {
    match ty {
        BqlType::Int => {
            let values: Vec<Option<i64>> = (0..row_count).map(|_| None).collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        BqlType::Float => {
            let values: Vec<Option<f64>> = (0..row_count).map(|_| None).collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        BqlType::Bool => {
            let values: Vec<Option<bool>> = (0..row_count).map(|_| None).collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        BqlType::String => {
            let values: Vec<Option<&str>> = (0..row_count).map(|_| None).collect();
            Ok(Arc::new(StringViewArray::from(values)))
        }
        BqlType::Timestamp => {
            let values: Vec<Option<i64>> = (0..row_count).map(|_| None).collect();
            let arr = TimestampNanosecondArray::from(values).with_timezone("UTC");
            Ok(Arc::new(arr))
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "segment reader: backfill for nested type {ty:?} is not yet implemented"
        ))),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// §15 validation — framing
// ─────────────────────────────────────────────────────────────────────────────

/// §15 rules 1, 2, 3 — file size minimum, header magic, format version.
///
/// Returns the format version (`1` or `2`) on success.
fn validate_header(bytes: &[u8]) -> Result<u16> {
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
    if version != SEGMENT_FORMAT_VERSION_V1 && version != SEGMENT_FORMAT_VERSION_V2 {
        return Err(BqliteError::Corruption(format!(
            "segment file format version unsupported: expected 1 or 2, got {version}"
        )));
    }
    Ok(version)
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
///
/// `header_version` is the format version read from the file header
/// by [`validate_header`] — it must match `footer.format_version()`.
///
/// v2-specific FSST validation (§11 rules 13–15) is deferred to
/// TASK-416 when the FSST encoding implementation lands. Rule 15
/// (reject discriminant 11) is implicitly satisfied by the
/// `from_discriminant_versioned` call in the encoding check below.
fn validate_footer(
    footer: &SegmentFooter,
    footer_body_start: usize,
    header_version: u16,
) -> Result<()> {
    let format_version = footer.format_version();

    // Rule 7: format_version is recognized and matches the header.
    if format_version != SEGMENT_FORMAT_VERSION_V1 && format_version != SEGMENT_FORMAT_VERSION_V2 {
        return Err(BqliteError::Corruption(format!(
            "segment footer format_version unsupported: expected 1 or 2, \
             got {format_version}",
        )));
    }
    if format_version != header_version {
        return Err(BqliteError::Corruption(format!(
            "segment footer format_version {format_version} does not match \
             file header version {header_version}",
        )));
    }

    // Rule 8: row_group_count == row_groups.len() and sum of row counts.
    if footer.row_group_count() as usize != footer.row_groups().len() {
        return Err(BqliteError::Corruption(format!(
            "segment footer row_group_count = {} but row_groups has {} entries",
            footer.row_group_count(),
            footer.row_groups().len(),
        )));
    }
    if footer.row_groups().is_empty() {
        return Err(BqliteError::Corruption(
            "segment footer has zero row groups — an empty segment is illegal \
             per segment-format-v1.md §6"
                .to_string(),
        ));
    }
    let sum: Option<u64> = footer
        .row_groups()
        .iter()
        .map(|rg| rg.row_count)
        .try_fold(0u64, u64::checked_add);
    match sum {
        Some(s) if s == footer.row_count() => (),
        Some(s) => {
            return Err(BqliteError::Corruption(format!(
                "segment footer row_count = {} but row groups sum to {s}",
                footer.row_count()
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
        .dictionaries()
        .iter()
        .map(|d| d.byte_offset)
        .min()
        .map(|off| off as usize)
        .unwrap_or(footer_body_start);

    // Rule 9: per-row-group byte ranges fit inside the row-groups
    // region [FILE_HEADER_LEN, row_groups_end_max).
    let mut expected_offset = FILE_HEADER_LEN as u64;
    for (i, rg) in footer.row_groups().iter().enumerate() {
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
        let schema_col_count = footer.schema().columns().len();
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
            // Rule 10 continued: legal encoding discriminant for
            // this format version.
            if EncodingType::from_discriminant_versioned(meta.encoding, format_version).is_err() {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c} encoding {} is not valid for format version {format_version}",
                    meta.encoding,
                )));
            }
            // Rule 10 continued: legal compression discriminant.
            if CompressionType::from_discriminant(meta.compression).is_err() {
                return Err(BqliteError::Corruption(format!(
                    "row group {i} column {c} compression {} is not in the valid set {{0,1}}",
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
    let schema_col_count = footer.schema().columns().len();
    let dict_region_start = expected_offset as usize;
    let dict_region_end = footer_body_start;
    if dict_region_end < dict_region_start {
        return Err(BqliteError::Corruption(format!(
            "footer body start {footer_body_start} is before the end of the row groups region {dict_region_start}"
        )));
    }
    for (i, dict) in footer.dictionaries().iter().enumerate() {
        if (dict.column_ordinal as usize) >= schema_col_count {
            return Err(BqliteError::Corruption(format!(
                "dictionary {i} column_ordinal {} is out of schema bounds (< {schema_col_count})",
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
    if footer.row_group_size_hint() == 0 {
        return Err(BqliteError::Corruption(
            "segment footer row_group_size_hint = 0 (must be positive)".to_string(),
        ));
    }
    // Soft sanity check: every row group's count ≤ row_group_size_hint
    // except possibly the last. This catches a corrupt writer that
    // emits over-sized row groups.
    let n = footer.row_groups().len();
    for (i, rg) in footer.row_groups().iter().enumerate() {
        let is_last = i + 1 == n;
        if !is_last && rg.row_count > footer.row_group_size_hint() as u64 {
            return Err(BqliteError::Corruption(format!(
                "non-final row group {i} row_count {} exceeds row_group_size_hint {}",
                rg.row_count,
                footer.row_group_size_hint()
            )));
        }
    }
    // seq_id_range monotonic.
    if footer.seq_id_range().0 > footer.seq_id_range().1 {
        return Err(BqliteError::Corruption(format!(
            "segment footer seq_id_range = ({}, {}): min exceeds max",
            footer.seq_id_range().0,
            footer.seq_id_range().1
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
/// Build one [`bqlite_core::encoded::ArcBytes`] per segment-level
/// dictionary, wrapping its on-disk byte region (§11).
///
/// `validate_footer` already bounds-checked every dictionary range —
/// this helper assumes those invariants hold and clips any pathological
/// entry to the file end defensively. One copy per dictionary happens
/// at open; the returned handles are shared across scans via `Arc`.
fn load_dict_bytes(bytes: &[u8], footer: &SegmentFooter) -> Vec<bqlite_core::encoded::ArcBytes> {
    footer
        .dictionaries()
        .iter()
        .map(|dict_ref| {
            let start = (dict_ref.byte_offset as usize).min(bytes.len());
            let end = start
                .saturating_add(dict_ref.byte_length as usize)
                .min(bytes.len());
            let region = &bytes[start..end];
            Arc::<[u8]>::from(region)
        })
        .collect()
}

fn load_dictionaries(bytes: &[u8], footer: &SegmentFooter) -> Result<Vec<DictionaryValues>> {
    let mut out = Vec::with_capacity(footer.dictionaries().len());
    for (i, dict_ref) in footer.dictionaries().iter().enumerate() {
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
    use crate::segment::layout::SEGMENT_FORMAT_VERSION;

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
    use crate::encoding::EncodedChunk;
    use crate::segment::layout::{
        ColumnChunkMeta, RowGroupIndex, SegmentDictRef, SEGMENT_FORMAT_VERSION,
    };
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
        // Flip version bytes to 99 (truly unknown).
        bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
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
        assert_eq!(reader.footer().row_count(), 3);
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
        assert_eq!(a.footer().row_count(), b.footer().row_count());
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
        // out-of-set encoding discriminant (`99` is not a valid
        // encoding for any format version). The column chunk bytes
        // themselves do not need to parse — the reader rejects the
        // metadata long before it touches the bytes — so we stub
        // the row group with three empty Plain chunks and only
        // override the metadata of the first.
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
                        encoding: 99, // illegal — not a valid encoding discriminant
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

    // ── Round-trip tests (writer → reader → decoded values) ─────────
    //
    // These tests exercise the scan iterator end-to-end: build an
    // Arrow array, encode it via the `Encoding` trait, feed it to
    // TASK-213's writer, and read it back through this reader. The
    // round-trip is the strongest test we can write because it
    // validates every layer (encoding trait, writer byte layout,
    // reader parser, encoding decoder, null splicer) at once.

    use crate::encoding::{
        BitPacking as BitPackingEnc, Constant as ConstantEnc, Delta as DeltaEnc,
        Encoding as EncodingTrait, Plain as PlainEnc,
    };
    use crate::segment::writer::{
        encode_segment, PreparedColumnChunk, PreparedRowGroup, SegmentWriteRequest,
    };
    use ::arrow::array::{
        BooleanArray as ArrowBoolArray, Int64Array as ArrowIntArray,
        StringViewArray as ArrowStringView, TimestampNanosecondArray as ArrowTimestampArray,
    };
    use bqlite_core::storage::{ColumnProjection, ZoneMap as CoreZoneMap};

    /// Minimum schema for round-trip tests: entity_id (String),
    /// ts (Timestamp), event_type (String), amount (nullable Int).
    fn roundtrip_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    /// Build the "amount" column's LSB-first null bitmap for the
    /// round-trip tests. `valid[i] == true` means row `i` is
    /// non-null.
    fn build_null_bitmap(valid: &[bool]) -> Vec<u8> {
        let byte_count = valid.len().div_ceil(8);
        let mut out = vec![0u8; byte_count];
        for (i, v) in valid.iter().enumerate() {
            if *v {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        out
    }

    fn encode_plain_string(values: &[&str]) -> EncodedChunk {
        let arr: ArrowStringView = values.iter().map(|s| Some(*s)).collect();
        PlainEnc.encode(&arr).unwrap()
    }

    fn encode_plain_timestamp(values: &[i64]) -> EncodedChunk {
        let arr = ArrowTimestampArray::from(values.iter().map(|v| Some(*v)).collect::<Vec<_>>())
            .with_timezone("UTC");
        PlainEnc.encode(&arr).unwrap()
    }

    fn encode_plain_int(values: &[i64]) -> EncodedChunk {
        let arr = ArrowIntArray::from(values.iter().map(|v| Some(*v)).collect::<Vec<_>>());
        PlainEnc.encode(&arr).unwrap()
    }

    #[test]
    fn roundtrip_plain_encodings_all_non_nullable() {
        let schema = roundtrip_schema();
        let entity_values = ["u1", "u1", "u2", "u2"];
        let ts_values: Vec<i64> = vec![
            1_700_000_000_000_000_000,
            1_700_000_000_100_000_000,
            1_700_000_000_200_000_000,
            1_700_000_000_300_000_000,
        ];
        let event_values = ["view", "checkout", "view", "click"];
        let amount_values: Vec<i64> = vec![10, 20, 30, 40];

        // Encode every column with Plain.
        let entity_chunk = encode_plain_string(&entity_values);
        let ts_chunk = encode_plain_timestamp(&ts_values);
        let event_chunk = encode_plain_string(&event_values);
        let amount_chunk = encode_plain_int(&amount_values);
        // Non-null amount column still needs a null bitmap because
        // the schema declares it nullable.
        let amount_bitmap = build_null_bitmap(&[true, true, true, true]);

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 4,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: entity_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: ts_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(ts_values[0])),
                        zone_max: Some(PropertyValue::Timestamp(*ts_values.last().unwrap())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: event_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("checkout".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(amount_bitmap),
                        encoded: amount_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(10)),
                        zone_max: Some(PropertyValue::Int(40)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, 3),
            batch_id: 1,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        assert_eq!(reader.row_count(), 4);

        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().expect("one row group");
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 4);

        // Check each column's values.
        let entity_out = batch
            .column(0)
            .as_any()
            .downcast_ref::<ArrowStringView>()
            .unwrap();
        for (i, v) in entity_values.iter().enumerate() {
            assert_eq!(entity_out.value(i), *v, "entity_id row {i}");
        }

        let ts_out = batch
            .column(1)
            .as_any()
            .downcast_ref::<ArrowTimestampArray>()
            .unwrap();
        for (i, v) in ts_values.iter().enumerate() {
            assert_eq!(ts_out.value(i), *v, "ts row {i}");
        }

        let event_out = batch
            .column(2)
            .as_any()
            .downcast_ref::<ArrowStringView>()
            .unwrap();
        for (i, v) in event_values.iter().enumerate() {
            assert_eq!(event_out.value(i), *v, "event_type row {i}");
        }

        let amount_out = batch
            .column(3)
            .as_any()
            .downcast_ref::<ArrowIntArray>()
            .unwrap();
        for (i, v) in amount_values.iter().enumerate() {
            assert!(!amount_out.is_null(i), "amount row {i}");
            assert_eq!(amount_out.value(i), *v, "amount row {i}");
        }

        // Second pull returns None; further pulls stay None.
        assert!(scan.next_row_group().unwrap().is_none());
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn roundtrip_null_splicing_int_column() {
        // Nullable amount column with three real values and two nulls
        // interleaved. Dense Plain-encoded payload carries only the
        // non-null values.
        let schema = roundtrip_schema();
        let ts_values: Vec<i64> = vec![
            1_700_000_000_000_000_000,
            1_700_000_000_100_000_000,
            1_700_000_000_200_000_000,
            1_700_000_000_300_000_000,
            1_700_000_000_400_000_000,
        ];
        let valid = [true, false, true, false, true];
        let dense_amounts: Vec<i64> = vec![10, 30, 50];

        let amount_bitmap = build_null_bitmap(&valid);
        let amount_chunk = encode_plain_int(&dense_amounts);

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 5,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1", "u1", "u1", "u1", "u1"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&ts_values),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(ts_values[0])),
                        zone_max: Some(PropertyValue::Timestamp(*ts_values.last().unwrap())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(amount_bitmap),
                        encoded: amount_chunk,
                        compression: CompressionType::None,
                        null_count: 2,
                        zone_min: Some(PropertyValue::Int(10)),
                        zone_max: Some(PropertyValue::Int(50)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 4),
            batch_id: 2,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().expect("one row group");

        let amount_out = batch
            .column(3)
            .as_any()
            .downcast_ref::<ArrowIntArray>()
            .unwrap();
        assert_eq!(amount_out.len(), 5);
        let expected = [Some(10i64), None, Some(30), None, Some(50)];
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!amount_out.is_null(i), "row {i} should be non-null");
                    assert_eq!(amount_out.value(i), *v, "row {i}");
                }
                None => assert!(amount_out.is_null(i), "row {i} should be null"),
            }
        }
    }

    #[test]
    fn roundtrip_delta_encoding_timestamp() {
        // Delta-encoded monotonic timestamps.
        let schema = roundtrip_schema();
        let ts_values: Vec<i64> = vec![
            1_700_000_000_000_000_000,
            1_700_000_000_100_000_000,
            1_700_000_000_200_000_000,
            1_700_000_000_300_000_000,
        ];
        let ts_array =
            ArrowTimestampArray::from(ts_values.iter().map(|v| Some(*v)).collect::<Vec<_>>())
                .with_timezone("UTC");
        let ts_chunk = DeltaEnc.encode(&ts_array).unwrap();

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 4,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"; 4]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: ts_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(ts_values[0])),
                        zone_max: Some(PropertyValue::Timestamp(*ts_values.last().unwrap())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 4]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true, true, true])),
                        encoded: encode_plain_int(&[1, 2, 3, 4]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(4)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 3),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        let ts_out = batch
            .column(1)
            .as_any()
            .downcast_ref::<ArrowTimestampArray>()
            .unwrap();
        for (i, v) in ts_values.iter().enumerate() {
            assert_eq!(ts_out.value(i), *v);
        }
    }

    #[test]
    fn roundtrip_bitpacking_encoding_int() {
        // BitPacking for a narrow-range Int column.
        let schema = roundtrip_schema();
        let amount_values: Vec<i64> = vec![100, 110, 105, 120, 115];
        let amount_array = ArrowIntArray::from(amount_values.clone());
        let amount_chunk = BitPackingEnc.encode(&amount_array).unwrap();

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 5,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true; 5])),
                        encoded: amount_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(100)),
                        zone_max: Some(PropertyValue::Int(120)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 4),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        let amount_out = batch
            .column(3)
            .as_any()
            .downcast_ref::<ArrowIntArray>()
            .unwrap();
        for (i, v) in amount_values.iter().enumerate() {
            assert_eq!(amount_out.value(i), *v);
        }
    }

    #[test]
    fn roundtrip_constant_encoding_int() {
        // Constant encoding: every non-null value is the same.
        let schema = roundtrip_schema();
        let amount_values: Vec<i64> = vec![42; 6];
        let amount_array = ArrowIntArray::from(amount_values.clone());
        let amount_chunk = ConstantEnc.encode(&amount_array).unwrap();

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 6,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"; 6]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0; 6]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 6]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true; 6])),
                        encoded: amount_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(42)),
                        zone_max: Some(PropertyValue::Int(42)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 5),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        let amount_out = batch
            .column(3)
            .as_any()
            .downcast_ref::<ArrowIntArray>()
            .unwrap();
        for v in amount_out.iter() {
            assert_eq!(v, Some(42));
        }
    }

    #[test]
    fn roundtrip_lz4_compressed_plain_payload() {
        // LZ4-wrapped Plain payload for strings. The writer applies
        // LZ4 to the encoded bytes; the reader decompresses before
        // passing to `Plain::decode`.
        let schema = roundtrip_schema();
        // Enough repetition that LZ4 actually shrinks the payload.
        let event_values: Vec<&str> = (0..40).map(|_| "view").collect();
        let event_chunk = encode_plain_string(&event_values);

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 40,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"; 40]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0i64; 40]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: event_chunk,
                        compression: CompressionType::Lz4,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true; 40])),
                        encoded: encode_plain_int(&(0..40).collect::<Vec<_>>()),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(0)),
                        zone_max: Some(PropertyValue::Int(39)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 39),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        let event_out = batch
            .column(2)
            .as_any()
            .downcast_ref::<ArrowStringView>()
            .unwrap();
        for i in 0..40 {
            assert_eq!(event_out.value(i), "view");
        }
    }

    #[test]
    fn roundtrip_schema_evolution_backfills_added_column() {
        // Write a segment against a 4-column schema, then open it
        // with a 5-column current schema. The reader must backfill
        // the new column with all-nulls.
        let write_time_schema = roundtrip_schema();
        let mut current_columns = write_time_schema.columns().to_vec();
        current_columns.push(ColumnDef::nullable("device", BqlType::String));
        let current_schema =
            TableSchema::new("events", current_columns, "entity_id", "ts", "event_type").unwrap();

        let request = SegmentWriteRequest {
            schema: write_time_schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 2,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1", "u2"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[1, 2]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(1)),
                        zone_max: Some(PropertyValue::Timestamp(2)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view", "view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true])),
                        encoded: encode_plain_int(&[10, 20]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(10)),
                        zone_max: Some(PropertyValue::Int(20)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 1),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, current_schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 5);

        // The first 4 columns come from the segment.
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<ArrowStringView>()
                .unwrap()
                .value(0),
            "u1"
        );
        // The 5th column (`device`) is backfilled with nulls.
        let device_out = batch
            .column(4)
            .as_any()
            .downcast_ref::<ArrowStringView>()
            .unwrap();
        assert_eq!(device_out.len(), 2);
        assert!(device_out.is_null(0));
        assert!(device_out.is_null(1));
    }

    #[test]
    fn projection_selects_subset_in_schema_order() {
        // Projection requests columns in non-schema order (amount, entity_id)
        // but the reader always returns them in table-schema order so that
        // CompiledNode::Column { index } values remain valid after pruning.
        // roundtrip_schema order: entity_id(0), ts(1), event_type(2), amount(3).
        // Requesting [amount, entity_id] yields output [entity_id, amount].
        let schema = roundtrip_schema();
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 2,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["a", "b"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("a".into())),
                        zone_max: Some(PropertyValue::String("b".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[10, 20]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(10)),
                        zone_max: Some(PropertyValue::Timestamp(20)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view", "view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true])),
                        encoded: encode_plain_int(&[1, 2]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(2)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 1),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        // Request in non-schema order: amount before entity_id.
        let projection = ColumnProjection::with_columns(["amount", "entity_id"]);
        let mut scan = reader.scan(&projection, None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        // Output is in table-schema order: entity_id(0) < amount(3).
        assert_eq!(batch.schema().field(0).name(), "entity_id");
        assert_eq!(batch.schema().field(1).name(), "amount");
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<ArrowStringView>()
                .unwrap()
                .value(0),
            "a"
        );
    }

    #[derive(Debug)]
    struct RejectAllInts {
        referenced: Vec<String>,
    }
    impl RejectAllInts {
        fn new() -> Self {
            Self {
                referenced: vec!["amount".to_string()],
            }
        }
    }
    impl Predicate for RejectAllInts {
        fn accepts_zone(&self, column: &str, _zone: &CoreZoneMap) -> bool {
            column != "amount"
        }
        fn referenced_columns(&self) -> &[String] {
            &self.referenced
        }
    }

    #[test]
    fn predicate_prunes_row_group_when_zone_rejected() {
        let schema = roundtrip_schema();
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 2,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1", "u2"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0, 0]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view", "view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true])),
                        encoded: encode_plain_int(&[1, 2]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(2)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 1),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let predicate: Arc<dyn Predicate> = Arc::new(RejectAllInts::new());
        let mut scan = reader
            .scan(&ColumnProjection::all(), Some(predicate))
            .unwrap();
        // The only row group should be pruned — next_row_group returns None.
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn scan_predicate_prunes_row_group_via_zone_map_module() {
        // End-to-end test pinning the `ScanPredicate` -> `zone_map`
        // module -> real segment reader path. The row group has
        // amount in [10, 40]; a `ScanPredicate` that requires
        // `amount > 100` must prune it via `accepts_zone_group`
        // without touching any other column's decoder.
        use bqlite_core::storage::{RangeOp, ScanConjunct, ScanPredicate};

        let schema = roundtrip_schema();
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 2,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1", "u2"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0, 0]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view", "view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true])),
                        encoded: encode_plain_int(&[10, 40]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(10)),
                        zone_max: Some(PropertyValue::Int(40)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 1),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();

        // Pruning predicate: amount > 100 — must prune the single
        // row group (max=40 is below the threshold).
        let prune_pred: Arc<dyn Predicate> =
            Arc::new(ScanPredicate::new(vec![ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Gt,
                value: PropertyValue::Int(100),
            }]));
        let mut scan = reader
            .scan(&ColumnProjection::all(), Some(prune_pred))
            .unwrap();
        assert!(
            scan.next_row_group().unwrap().is_none(),
            "ScanPredicate `amount > 100` failed to prune row group with max=40"
        );

        // Accepting predicate: amount >= 10 — the row group has
        // max=40 > 10, so it survives and the reader materializes
        // both rows.
        let accept_pred: Arc<dyn Predicate> =
            Arc::new(ScanPredicate::new(vec![ScanConjunct::Range {
                column: "amount".into(),
                op: RangeOp::Ge,
                value: PropertyValue::Int(10),
            }]));
        let mut scan = reader
            .scan(&ColumnProjection::all(), Some(accept_pred))
            .unwrap();
        let batch = scan.next_row_group().unwrap().expect("one row group");
        assert_eq!(batch.num_rows(), 2);
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn predicate_prunes_row_group_even_when_column_not_projected() {
        // Zone-map pruning must be driven by the predicate's own
        // referenced columns, not the scan's projection — otherwise
        // `WHERE amount > 100 | SELECT user_id` silently loses
        // pruning on `amount`.
        let schema = roundtrip_schema();
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 2,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1", "u2"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0, 0]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view", "view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true])),
                        encoded: encode_plain_int(&[1, 2]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(2)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 1),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let predicate: Arc<dyn Predicate> = Arc::new(RejectAllInts::new());
        // Projection excludes `amount`; the predicate still references it.
        let projection = ColumnProjection::with_columns(["entity_id"]);
        let mut scan = reader.scan(&projection, Some(predicate)).unwrap();
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn roundtrip_dictionary_via_fixture_with_hoisted_form() {
        // Hand-craft a segment whose Dictionary column chunk uses
        // the on-disk hoisted form (dict_id + code_bit_width) and
        // whose segment-dictionaries region holds the values as a
        // Plain payload. The reader must reconstruct the
        // self-contained params, hand them to Dictionary::decode,
        // and produce the expected StringArray.
        //
        // Values: ["click", "view", "view", "click"] — cardinality 2,
        // so `code_bit_width = 1`. The dictionary column lives at
        // ordinal 2 in a minimal three-column schema.
        let schema = TableSchema::new(
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

        // Sorted dictionary values, per §10.3.
        let dict_values: Vec<&str> = vec!["click", "view"];
        let dict_bytes = string_dictionary_bytes(&dict_values);

        // Code stream for rows ["click", "view", "view", "click"]:
        // codes = [0, 1, 1, 0]. The Dictionary encoding's `decode`
        // wants a self-contained params block; our reader's job is
        // to rebuild it. We build the payload as a 1-bit-packed
        // code stream, padded up to the next 8-byte multiple.
        //
        // Bits laid out LSB-first: byte0 = 0b0000_0110 (codes 0,1,1,0
        // at positions 0..4, remaining bits 0). Pad to 8 bytes.
        let mut payload = vec![0u8; 8];
        payload[0] = 0b0000_0110;

        // On-disk encoding header: discriminant (1) + params (dict_id
        // u32 LE = 0 + code_bit_width u8 = 1) + uncompressed_payload_length u32 LE = 8.
        let mut chunk = Vec::new();
        chunk.push(EncodingType::Dictionary.discriminant());
        chunk.extend_from_slice(&0u32.to_le_bytes()); // dict_id
        chunk.push(1u8); // code_bit_width
        chunk.extend_from_slice(&8u32.to_le_bytes()); // uncompressed_payload_length
        chunk.extend_from_slice(&payload);

        // Row group: three column chunks, one per schema column. We
        // only care about the Dictionary chunk at ordinal 2.
        let col0 = plain_empty_chunk();
        let col1 = plain_empty_chunk();
        let col2 = chunk;

        let col0_off = FILE_HEADER_LEN as u64;
        let col0_len = col0.len() as u64;
        let col1_off = col0_off + col0_len;
        let col1_len = col1.len() as u64;
        let col2_off = col1_off + col1_len;
        let col2_len = col2.len() as u64;

        let mut row_group = Vec::new();
        row_group.extend_from_slice(&col0);
        row_group.extend_from_slice(&col1);
        row_group.extend_from_slice(&col2);

        let rg_len = row_group.len() as u64;
        let rg_end = col0_off + rg_len;
        let dict_off = rg_end;
        let dict_len = dict_bytes.len() as u64;

        let footer = FooterV1 {
            format_version: SEGMENT_FORMAT_VERSION,
            schema: schema.clone(),
            schema_version: 0,
            row_count: 4,
            row_group_count: 1,
            row_group_size_hint: 65_536,
            creation_timestamp_ns: 0,
            seq_id_range: (0, 3),
            batch_id: 0,
            compaction_level: 0,
            dictionaries: vec![SegmentDictRef {
                column_ordinal: 2,
                byte_offset: dict_off,
                byte_length: dict_len,
                cardinality: dict_values.len() as u32,
                value_type: BqlType::String,
            }],
            row_groups: vec![RowGroupIndex {
                byte_offset: col0_off,
                byte_length: rg_len,
                row_count: 4,
                columns: vec![
                    ColumnChunkMeta {
                        column_ordinal: 0,
                        byte_offset: col0_off,
                        byte_length: col0_len,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 1,
                        byte_offset: col1_off,
                        byte_length: col1_len,
                        encoding: 0,
                        compression: 0,
                        row_count: 0,
                        null_count: 4,
                        zone_min: None,
                        zone_max: None,
                    },
                    ColumnChunkMeta {
                        column_ordinal: 2,
                        byte_offset: col2_off,
                        byte_length: col2_len,
                        encoding: EncodingType::Dictionary.discriminant(),
                        compression: 0,
                        row_count: 4,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("click".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                ],
            }],
        };

        let bytes = build_segment(&footer, &row_group, &dict_bytes);
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        assert_eq!(reader.dictionaries().len(), 1);

        let projection = ColumnProjection::with_columns(["event_type"]);
        let mut scan = reader.scan(&projection, None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        let out = batch
            .column(0)
            .as_any()
            .downcast_ref::<ArrowStringView>()
            .unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(out.value(0), "click");
        assert_eq!(out.value(1), "view");
        assert_eq!(out.value(2), "view");
        assert_eq!(out.value(3), "click");
    }

    #[test]
    fn roundtrip_bool_column_with_nulls() {
        let schema = TableSchema::new(
            "t",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("flag", BqlType::Bool),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();

        let valid = [true, false, true, true];
        let dense_flags: Vec<bool> = vec![true, false, true];
        let dense_array = ArrowBoolArray::from(dense_flags.clone());
        let flag_chunk = PlainEnc.encode(&dense_array).unwrap();
        let flag_bitmap = build_null_bitmap(&valid);

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 4,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"; 4]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0; 4]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 4]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(flag_bitmap),
                        encoded: flag_chunk,
                        compression: CompressionType::None,
                        null_count: 1,
                        zone_min: Some(PropertyValue::Bool(false)),
                        zone_max: Some(PropertyValue::Bool(true)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 3),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();
        let flag_out = batch
            .column(3)
            .as_any()
            .downcast_ref::<ArrowBoolArray>()
            .unwrap();
        let expected = [Some(true), None, Some(false), Some(true)];
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!flag_out.is_null(i));
                    assert_eq!(flag_out.value(i), *v);
                }
                None => assert!(flag_out.is_null(i)),
            }
        }
    }

    #[test]
    fn roundtrip_nullable_string_column_with_interleaved_nulls() {
        // Close the last null-splicing coverage gap by exercising
        // the `splice_string` branch with an actual interleaved
        // null pattern (the schema-evolution test only hits the
        // all-null backfill branch).
        let schema = TableSchema::new(
            "t",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("label", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();

        // Rows: ["x", null, "y", null, "z"]
        let valid = [true, false, true, false, true];
        let dense_labels = ["x", "y", "z"];
        let label_chunk = encode_plain_string(&dense_labels);
        let label_bitmap = build_null_bitmap(&valid);

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 5,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[0; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 5]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(label_bitmap),
                        encoded: label_chunk,
                        compression: CompressionType::None,
                        null_count: 2,
                        zone_min: Some(PropertyValue::String("x".into())),
                        zone_max: Some(PropertyValue::String("z".into())),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 4),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let batch = scan.next_row_group().unwrap().unwrap();

        let label_out = batch
            .column(3)
            .as_any()
            .downcast_ref::<ArrowStringView>()
            .unwrap();
        assert_eq!(label_out.len(), 5);
        let expected = [Some("x"), None, Some("y"), None, Some("z")];
        for (i, exp) in expected.iter().enumerate() {
            match exp {
                Some(v) => {
                    assert!(!label_out.is_null(i), "row {i}");
                    assert_eq!(label_out.value(i), *v, "row {i}");
                }
                None => assert!(label_out.is_null(i), "row {i}"),
            }
        }
    }

    #[test]
    fn row_group_zone_maps_surface_inline_min_max() {
        let schema = roundtrip_schema();
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 3,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["a", "b", "c"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("a".into())),
                        zone_max: Some(PropertyValue::String("c".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[10, 20, 30]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(10)),
                        zone_max: Some(PropertyValue::Timestamp(30)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"; 3]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true, true])),
                        encoded: encode_plain_int(&[1, 2, 3]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(3)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 2),
            batch_id: 0,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema).unwrap();
        let scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let zones = scan.row_group_zone_maps(0).unwrap();
        assert_eq!(
            zones.get("amount").unwrap().min,
            Some(PropertyValue::Int(1))
        );
        assert_eq!(
            zones.get("amount").unwrap().max,
            Some(PropertyValue::Int(3))
        );
        assert_eq!(zones.get("amount").unwrap().row_count, 3);
        assert_eq!(
            zones.get("entity_id").unwrap().min,
            Some(PropertyValue::String("a".into()))
        );
    }

    // ── TASK-243: sequential-scan access-pattern hint ───────────────

    /// `SegmentFileReader::open` must issue a sequential-scan hint
    /// (via `posix_fadvise` on supported platforms, no-op elsewhere)
    /// before reading the file's bytes. Verified cross-platform via
    /// the test-only counter in `crate::segment::advise`.
    #[test]
    fn open_issues_sequential_scan_hint() {
        use crate::segment::advise::SEQUENTIAL_HINT_COUNT;
        use std::sync::atomic::Ordering;

        let schema = roundtrip_schema();

        // Smallest round-trip fixture: one row group, one row per
        // column. The hint is about the open path, not row-group
        // decode, so a single-row segment is enough.
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 1,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["u1"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: encode_plain_timestamp(&[1_700_000_000_000_000_000]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(1_700_000_000_000_000_000)),
                        zone_max: Some(PropertyValue::Timestamp(1_700_000_000_000_000_000)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: encode_plain_string(&["view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true])),
                        encoded: encode_plain_int(&[42]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(42)),
                        zone_max: Some(PropertyValue::Int(42)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, 0),
            batch_id: 1,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        // Serialize to bytes via the writer, then write to a
        // uniquely-named temp file (mirrors the pattern used by
        // `database.rs` tests so we don't pull in `tempfile` as a
        // dev-dep).
        let bytes = encode_segment(&request).expect("encode segment");

        let pid = std::process::id();
        let seq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let mut path = std::env::temp_dir();
        path.push(format!("bqlite-task243-open-{pid}-{seq}.seg"));
        std::fs::write(&path, &bytes).expect("write temp segment");

        // Snapshot the counter before the open and confirm the
        // open bumped it. Using `after > before` (rather than
        // `after - before == 1`) keeps the test robust against
        // other parallel tests that also open segments through
        // the same code path.
        let before = SEQUENTIAL_HINT_COUNT.load(Ordering::Relaxed);
        let reader = SegmentFileReader::open(&path, schema.clone()).expect("open segment");
        let after = SEQUENTIAL_HINT_COUNT.load(Ordering::Relaxed);

        assert!(
            after > before,
            "SegmentFileReader::open must issue a sequential-scan \
             hint at open time (counter was {before} \u{2192} {after})",
        );
        assert_eq!(reader.row_count(), 1);

        let _ = std::fs::remove_file(&path);
    }

    // ── CP2 differential tests ──────────────────────────────────────
    //
    // The zero-copy scan/filter plan (CP2) ships a new
    // `next_encoded_row_group` API that returns an `EncodedBatch`.
    // Pushing that batch through `materialize_encoded_batch` must
    // produce the same values as the classic `next_row_group` path.

    #[test]
    fn cp2_encoded_path_materializes_to_same_record_batch() {
        use bqlite_core::BqlType;

        let schema = roundtrip_schema();
        let entity_values = ["u1", "u1", "u2", "u2"];
        let ts_values: Vec<i64> = vec![
            1_700_000_000_000_000_000,
            1_700_000_000_100_000_000,
            1_700_000_000_200_000_000,
            1_700_000_000_300_000_000,
        ];
        let event_values = ["view", "checkout", "view", "click"];
        let amount_values: Vec<i64> = vec![10, 20, 30, 40];

        let entity_chunk = encode_plain_string(&entity_values);
        let ts_chunk = encode_plain_timestamp(&ts_values);
        let event_chunk = encode_plain_string(&event_values);
        let amount_chunk = encode_plain_int(&amount_values);
        let amount_bitmap = build_null_bitmap(&[true, true, true, true]);

        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 4,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: entity_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: ts_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(ts_values[0])),
                        zone_max: Some(PropertyValue::Timestamp(*ts_values.last().unwrap())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: event_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("checkout".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(amount_bitmap),
                        encoded: amount_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(10)),
                        zone_max: Some(PropertyValue::Int(40)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, 3),
            batch_id: 1,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };

        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema.clone()).unwrap();
        let types = vec![
            BqlType::String,
            BqlType::Timestamp,
            BqlType::String,
            BqlType::Int,
        ];

        // Materialized path.
        let mut scan1 = reader.scan(&ColumnProjection::all(), None).unwrap();
        let materialized = scan1.next_row_group().unwrap().expect("row group");

        // Encoded path → boundary materialization.
        let mut scan2 = reader.scan(&ColumnProjection::all(), None).unwrap();
        let encoded = scan2.next_encoded_row_group().unwrap().expect("row group");
        let columns =
            crate::segment::materialize::materialize_encoded_batch(&encoded, &types).unwrap();
        let rebuilt = ::arrow::array::RecordBatch::try_new(materialized.schema(), columns).unwrap();

        assert_eq!(materialized.num_rows(), rebuilt.num_rows());
        assert_eq!(materialized.num_columns(), rebuilt.num_columns());
        for i in 0..materialized.num_columns() {
            let a = materialized.column(i);
            let b = rebuilt.column(i);
            assert_eq!(
                format!("{a:?}"),
                format!("{b:?}"),
                "column {i} differs between materialized and encoded paths"
            );
        }
    }

    #[test]
    fn cp2_constant_encoding_pins_scalar_value() {
        // Build a segment using the standard roundtrip_schema where
        // entity_id is Constant-encoded (`u1` × 3). The CP2 encoded
        // path must pin the literal as a ScalarValue.
        use bqlite_core::encoded::{EncodedColumn, EncodedKind};
        use bqlite_core::scalar::ScalarValue;

        let schema = roundtrip_schema();
        let entity_chunk = ConstantEnc
            .encode(&ArrowStringView::from(vec!["u1", "u1", "u1"]))
            .unwrap();
        let ts_values: Vec<i64> = vec![
            1_700_000_000_000_000_000,
            1_700_000_000_100_000_000,
            1_700_000_000_200_000_000,
        ];
        let ts_chunk = encode_plain_timestamp(&ts_values);
        let event_chunk = encode_plain_string(&["a", "b", "c"]);
        let amount_chunk = encode_plain_int(&[1, 2, 3]);
        let request = SegmentWriteRequest {
            schema: schema.clone(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 3,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: entity_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: ts_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(ts_values[0])),
                        zone_max: Some(PropertyValue::Timestamp(*ts_values.last().unwrap())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: event_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("a".into())),
                        zone_max: Some(PropertyValue::String("c".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(build_null_bitmap(&[true, true, true])),
                        encoded: amount_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(3)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 2),
            batch_id: 1,
            compaction_level: 0,
            fsst_symbol_tables: vec![],
            format_version: 1,
        };
        let bytes = encode_segment(&request).unwrap();
        let reader = SegmentFileReader::from_bytes(bytes, schema).unwrap();
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let encoded = scan.next_encoded_row_group().unwrap().expect("row group");

        match &encoded.columns[0] {
            EncodedColumn::Encoded {
                kind: EncodedKind::Constant { value },
                rows,
                ..
            } => {
                assert_eq!(*rows, 3);
                assert_eq!(**value, ScalarValue::String("u1".to_string()));
            }
            other => panic!("expected entity_id as Encoded::Constant, got {other:?}"),
        }
    }
}
