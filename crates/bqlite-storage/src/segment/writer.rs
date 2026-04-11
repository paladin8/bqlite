//! Low-level segment file writer.
//!
//! Takes pre-encoded column chunks and the metadata needed to frame
//! them, and emits the v1 on-disk byte layout defined by
//! `docs/design/storage/segment-format-v1.md`. This module deliberately
//! knows nothing about encoding selection, sorting, or the manifest —
//! it is a byte-layout oracle that trusts its input.
//!
//! # Ownership split
//!
//! Wave 2 has three layers stacked on top of each other:
//!
//! | Layer | Owner | Input | Output |
//! |---|---|---|---|
//! | Encoding selection (§10.3) | TASK-212 `encoding::selector` | Arrow array | `EncodedChunk` + `CompressionType` |
//! | High-level writer orchestration (§7.2 entity boundaries, §9 row-group packing) | TASK-214 `writer.rs` | Sorted event stream | `SegmentWriteRequest` |
//! | **Byte-layout emission (this module)** | TASK-213 | [`SegmentWriteRequest`] | segment file on disk |
//!
//! The split lets each layer be tested in isolation: the selector only
//! needs Arrow arrays, the orchestrator only needs an in-memory
//! row-group builder, and this module only needs a hand-crafted
//! [`SegmentWriteRequest`] that compares the emitted bytes against the
//! v1 spec. None of the three layers has to mock out the other two.
//!
//! # Inputs the writer trusts
//!
//! - [`PreparedColumnChunk::encoded`] already passed through the
//!   encoding trait — `encoded.encoding` is the final on-disk
//!   discriminant, `encoded.params` the parameter byte block, and
//!   `encoded.payload` the **uncompressed** encoded payload.
//! - [`PreparedColumnChunk::compression`] is the caller's compression
//!   decision; the writer applies LZ4 when it is
//!   [`CompressionType::Lz4`] and writes the payload verbatim otherwise.
//!   The caller, not the writer, enforces the §10.7 "only compress
//!   when it shrinks by ≥10%" guard.
//! - [`PreparedColumnChunk::null_bitmap`] is already serialized in
//!   Arrow LSB-first form and has length `ceil(row_group.row_count / 8)`.
//!   The writer emits the bytes verbatim.
//! - [`PreparedColumnChunk::zone_min`] / `zone_max` are already the
//!   min/max of the chunk's non-null values, supplied by the caller.
//!   Writing them is a copy into the footer.
//!
//! Any invariant the input is expected to satisfy is checked by
//! [`validate_request`] up front and surfaced as
//! [`BqliteError::Execution`] — the writer never panics on bad input.
//!
//! # What the writer enforces on its own
//!
//! - Empty segments are illegal (§6). Zero row groups → error.
//! - Empty row groups are illegal (§6). Zero rows in a row group →
//!   error.
//! - `chunk.encoded.row_count + chunk.null_count` must equal the row
//!   group's `row_count` (every chunk covers every row in the group).
//! - Null bitmap presence matches the column's `nullable` flag per §7.
//! - Null bitmap length matches `ceil(row_group.row_count / 8)`.
//! - Footer body length fits in a `u32` (the trailer carries it as
//!   `u32 LE`).
//!
//! # Atomicity
//!
//! [`write_segment`] writes bytes to a sibling `<path>.tmp`, `fsync`s
//! the file, and then `rename`s it over the final path. A best-effort
//! `fsync` of the parent directory follows so the rename is durable on
//! filesystems that require it. This matches the pattern used by
//! [`crate::database`] for `manifest.json` and lets TASK-239's orphan
//! sweep (which looks for `*.tmp` files) reap partial writes.
//!
//! # Byte-exact test strategy
//!
//! The hard tests live in this module. [`encode_segment`] composes the
//! bytes in memory without touching the filesystem so unit tests can
//! assert byte-for-byte against the spec. The filesystem-touching
//! [`write_segment`] wraps it and is covered by a separate round-trip
//! test.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use bqlite_core::{BqlType, BqliteError, PropertyValue, Result, TableSchema};
use twox_hash::XxHash64;

use crate::encoding::{compress_lz4, CompressionType, EncodedChunk};
use crate::segment::layout::{
    ColumnChunkMeta, FooterV1, RowGroupIndex, SegmentDictRef, CHECKSUM_SEED, FILE_HEADER_LEN,
    MAGIC, ROW_GROUP_SIZE_DEFAULT, SEGMENT_FORMAT_VERSION,
};

// ─────────────────────────────────────────────────────────────────────────────
// Writer input types
// ─────────────────────────────────────────────────────────────────────────────

/// A fully-prepared column chunk — the unit of work the writer emits
/// into a row group.
///
/// All encoding work has already happened by the time a
/// `PreparedColumnChunk` exists: the values have been run through the
/// [`crate::encoding::Encoding`] trait, any selector-driven compression
/// decision has been recorded in [`Self::compression`], and the null
/// mask has been materialized into a separate bitmap. The writer
/// appends `null_bitmap || encoding header || on-disk payload` to the
/// output buffer and records a [`ColumnChunkMeta`] in the footer.
#[derive(Debug, Clone)]
pub struct PreparedColumnChunk {
    /// Position of this column in the segment schema. Must be a valid
    /// index into `SegmentWriteRequest.schema.columns()`.
    pub column_ordinal: u32,

    /// Null bitmap (LSB-first, `ceil(row_group.row_count / 8)` bytes).
    /// Per `segment-format-v1.md` §7, must be `Some` when the schema
    /// column is nullable and `None` when it is not. A nullable column
    /// whose chunk has zero nulls still writes an all-ones bitmap; the
    /// writer does not synthesize it for you.
    pub null_bitmap: Option<Vec<u8>>,

    /// Encoded dense values produced by [`crate::encoding::Encoding::encode`].
    /// The writer trusts `encoded.encoding`, `encoded.params`, and
    /// `encoded.payload` — they are the discriminant, parameter block,
    /// and uncompressed native payload the on-disk layout expects.
    pub encoded: EncodedChunk,

    /// Post-encoding compression codec. [`CompressionType::None`]
    /// writes the payload verbatim; [`CompressionType::Lz4`] wraps it
    /// via [`crate::encoding::compress_lz4`]. Selection rules live in
    /// TASK-212; the writer only executes the decision.
    pub compression: CompressionType,

    /// Null row count in this chunk. `encoded.row_count + null_count`
    /// must equal the parent row group's `row_count`.
    pub null_count: u64,

    /// Inline zone-map minimum — min of the non-null values.
    /// `None` iff the chunk is all-null.
    pub zone_min: Option<PropertyValue>,
    /// Inline zone-map maximum — max of the non-null values.
    /// `None` iff the chunk is all-null.
    pub zone_max: Option<PropertyValue>,
}

/// A row group handed to the writer.
///
/// Row groups are emitted back-to-back in the same order they appear
/// in [`SegmentWriteRequest::row_groups`]. Row-group sizing (the §6.2
/// "65,536 rows except possibly the last" rule) is the orchestrator's
/// responsibility; the writer only enforces that a row group is
/// non-empty.
#[derive(Debug, Clone)]
pub struct PreparedRowGroup {
    /// Total row count in the row group. Must be positive. Must equal
    /// `chunk.encoded.row_count + chunk.null_count` for every column
    /// chunk in the group.
    pub row_count: u64,
    /// Per-column chunks. Length must match the segment schema's
    /// column count; ordering is by declared ordinal.
    pub columns: Vec<PreparedColumnChunk>,
}

/// A pre-serialized segment-level dictionary.
///
/// The bytes are already in the on-disk format specified by §10.3
/// (Plain payload of the dictionary's value type, values sorted
/// ascending). The writer concatenates dictionaries in the order they
/// appear in [`SegmentWriteRequest::dictionaries`] and records each
/// one's placement in the footer's `dictionaries` vec. Callers that
/// encode `Dictionary` chunks are responsible for making sure the
/// `dict_id` stored in the chunk's encoding params matches the
/// dictionary's index in this vec.
#[derive(Debug, Clone)]
pub struct PreparedDictionary {
    /// Column this dictionary belongs to. At most one dictionary per
    /// `(segment, column)` pair in v1 (§10.3).
    pub column_ordinal: u32,
    /// Dictionary value bytes — Plain payload of [`Self::value_type`].
    pub bytes: Vec<u8>,
    /// Distinct-value count. Must match the number of values encoded
    /// in [`Self::bytes`]; the writer does not re-count them.
    pub cardinality: u32,
    /// Value type for decoding [`Self::bytes`] at read time.
    pub value_type: BqlType,
}

/// Everything the writer needs to emit a segment file.
#[derive(Debug, Clone)]
pub struct SegmentWriteRequest {
    /// Segment-schema snapshot (§10.1 `schema` field). Normally this
    /// matches the current manifest schema; for segments written
    /// before a later `ALTER TABLE ADD COLUMN`, it is an older
    /// snapshot that the reader backfills against per §14.
    pub schema: TableSchema,
    /// Version tag on [`Self::schema`] at write time. Stored in the
    /// footer's `schema_version` field.
    pub schema_version: u32,
    /// Row groups to write, in file order. Must be non-empty.
    pub row_groups: Vec<PreparedRowGroup>,
    /// Segment-level dictionaries, in footer order.
    pub dictionaries: Vec<PreparedDictionary>,
    /// Segment creation timestamp (nanoseconds since epoch UTC).
    pub creation_timestamp_ns: i64,
    /// `__seq_id` range `(min_inclusive, max_inclusive)` covered by
    /// this segment. Both endpoints must be actual values present in
    /// the segment (§6.2).
    pub seq_id_range: (u64, u64),
    /// Ingest batch that produced the segment (§6.2).
    pub batch_id: u64,
    /// Compaction tier. `0` for L0 ingest output in Wave 2.
    pub compaction_level: u8,
}

/// Summary returned after [`write_segment`] completes successfully.
///
/// The summary is the minimum set of values the caller (TASK-214 /
/// TASK-217) needs to register the freshly-written segment in the
/// manifest without re-parsing the file. The writer populates it from
/// the request plus the bytes it just emitted; no second pass over the
/// file is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentWriteSummary {
    /// Total bytes written, including the file header, row groups,
    /// dictionaries region, footer body, checksum, and trailer.
    pub byte_size: u64,
    /// Total rows in the segment across all row groups.
    pub row_count: u64,
    /// Row-group count. Equal to `request.row_groups.len()`.
    pub row_group_count: u32,
    /// Byte count of the postcard-serialized footer body — what the
    /// trailer stores as `u32 LE`.
    pub footer_body_length: u32,
    /// `xxHash64` of bytes `[0, file_size - 16)`. Stored at
    /// `file_size - 16` as `u64 LE`.
    pub checksum: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Public writer entry points
// ─────────────────────────────────────────────────────────────────────────────

/// Emit a segment file atomically at `path`.
///
/// Writes bytes to `<path>.tmp`, `fsync`s the temp file, renames it
/// over `path`, and best-effort `fsync`s the parent directory. A
/// partial write that crashes between the `write_all` and the `rename`
/// leaves a `*.tmp` file behind, which TASK-239's startup orphan sweep
/// will reap on the next `Database::open`.
pub fn write_segment(path: &Path, request: &SegmentWriteRequest) -> Result<SegmentWriteSummary> {
    let bytes = encode_segment(request)?;
    let summary = summary_for(request, &bytes);
    write_atomic(path, &bytes)?;
    Ok(summary)
}

/// Compose the segment bytes in memory and return them.
///
/// This is the byte-level primitive write_segment wraps. It is split
/// out so unit tests can assert byte-exact content against the spec
/// without touching the filesystem.
pub fn encode_segment(request: &SegmentWriteRequest) -> Result<Vec<u8>> {
    validate_request(request)?;

    let mut buf: Vec<u8> = Vec::new();

    // § File header (§5)
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
    debug_assert_eq!(buf.len(), FILE_HEADER_LEN);

    // § Row groups (§6, §7)
    let mut row_group_indices: Vec<RowGroupIndex> = Vec::with_capacity(request.row_groups.len());
    for row_group in &request.row_groups {
        let rg_start = buf.len() as u64;
        let mut columns: Vec<ColumnChunkMeta> = Vec::with_capacity(row_group.columns.len());

        for chunk in &row_group.columns {
            let chunk_start = buf.len() as u64;

            // Null bitmap (when the column is nullable).
            if let Some(bitmap) = &chunk.null_bitmap {
                buf.extend_from_slice(bitmap);
            }

            // Encoding header: discriminant | params | uncompressed_payload_length (u32 LE).
            buf.push(chunk.encoded.encoding.discriminant());
            buf.extend_from_slice(&chunk.encoded.params);
            let uncompressed_payload_length =
                u32::try_from(chunk.encoded.payload.len()).map_err(|_| {
                    BqliteError::Execution(format!(
                        "column chunk (ord {}): uncompressed payload length {} does not fit in u32",
                        chunk.column_ordinal,
                        chunk.encoded.payload.len()
                    ))
                })?;
            buf.extend_from_slice(&uncompressed_payload_length.to_le_bytes());

            // On-disk payload: verbatim or LZ4-wrapped.
            match chunk.compression {
                CompressionType::None => buf.extend_from_slice(&chunk.encoded.payload),
                CompressionType::Lz4 => {
                    let compressed = compress_lz4(&chunk.encoded.payload);
                    buf.extend_from_slice(&compressed);
                }
            }

            let chunk_len = buf.len() as u64 - chunk_start;
            columns.push(ColumnChunkMeta {
                column_ordinal: chunk.column_ordinal,
                byte_offset: chunk_start,
                byte_length: chunk_len,
                encoding: chunk.encoded.encoding.discriminant(),
                compression: chunk.compression.discriminant(),
                row_count: chunk.encoded.row_count as u64,
                null_count: chunk.null_count,
                zone_min: chunk.zone_min.clone(),
                zone_max: chunk.zone_max.clone(),
            });
        }

        let rg_len = buf.len() as u64 - rg_start;
        row_group_indices.push(RowGroupIndex {
            byte_offset: rg_start,
            byte_length: rg_len,
            row_count: row_group.row_count,
            columns,
        });
    }

    // § Segment dictionaries region (§11)
    let mut dict_refs: Vec<SegmentDictRef> = Vec::with_capacity(request.dictionaries.len());
    for dict in &request.dictionaries {
        let offset = buf.len() as u64;
        buf.extend_from_slice(&dict.bytes);
        let len = buf.len() as u64 - offset;
        dict_refs.push(SegmentDictRef {
            column_ordinal: dict.column_ordinal,
            byte_offset: offset,
            byte_length: len,
            cardinality: dict.cardinality,
            value_type: dict.value_type.clone(),
        });
    }

    // § Footer body (§10.1)
    let total_row_count: u64 = request.row_groups.iter().map(|rg| rg.row_count).sum();
    let footer = FooterV1 {
        format_version: SEGMENT_FORMAT_VERSION,
        schema: request.schema.clone(),
        schema_version: request.schema_version,
        row_count: total_row_count,
        row_group_count: request.row_groups.len() as u32,
        row_group_size_hint: ROW_GROUP_SIZE_DEFAULT,
        creation_timestamp_ns: request.creation_timestamp_ns,
        seq_id_range: request.seq_id_range,
        batch_id: request.batch_id,
        compaction_level: request.compaction_level,
        dictionaries: dict_refs,
        row_groups: row_group_indices,
    };
    let footer_bytes = postcard::to_allocvec(&footer)
        .map_err(|e| BqliteError::Execution(format!("failed to serialize segment footer: {e}")))?;
    let footer_body_length = u32::try_from(footer_bytes.len()).map_err(|_| {
        BqliteError::Execution(format!(
            "segment footer body too large: {} bytes does not fit in u32",
            footer_bytes.len()
        ))
    })?;
    buf.extend_from_slice(&footer_bytes);

    // § Checksum (§12)
    let checksum = XxHash64::oneshot(CHECKSUM_SEED, &buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    // § Trailer (§13)
    buf.extend_from_slice(&footer_body_length.to_le_bytes());
    buf.extend_from_slice(&MAGIC);

    Ok(buf)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internals
// ─────────────────────────────────────────────────────────────────────────────

/// Summary derived from a successfully-encoded segment. Kept separate
/// from [`encode_segment`] so the summary fields stay a pure function
/// of the request plus the emitted bytes, which makes the
/// `write_segment` wrapper trivially correct.
fn summary_for(request: &SegmentWriteRequest, bytes: &[u8]) -> SegmentWriteSummary {
    let byte_size = bytes.len() as u64;
    let trailer_start = bytes.len() - 8;
    let checksum_start = bytes.len() - 16;
    let footer_body_length = u32::from_le_bytes(
        bytes[trailer_start..trailer_start + 4]
            .try_into()
            .expect("4 bytes"),
    );
    let checksum = u64::from_le_bytes(
        bytes[checksum_start..checksum_start + 8]
            .try_into()
            .expect("8 bytes"),
    );
    let row_count: u64 = request.row_groups.iter().map(|rg| rg.row_count).sum();
    let row_group_count = request.row_groups.len() as u32;
    SegmentWriteSummary {
        byte_size,
        row_count,
        row_group_count,
        footer_body_length,
        checksum,
    }
}

/// Pre-flight validation: reject requests that violate §6 / §7 before
/// any bytes are emitted. Failing here produces a clean error instead
/// of an on-disk corruption.
fn validate_request(request: &SegmentWriteRequest) -> Result<()> {
    if request.row_groups.is_empty() {
        return Err(BqliteError::Execution(
            "segment must contain at least one row group (empty segments are illegal — §6)".into(),
        ));
    }

    let schema_column_count = request.schema.columns().len();

    for (rg_idx, rg) in request.row_groups.iter().enumerate() {
        if rg.row_count == 0 {
            return Err(BqliteError::Execution(format!(
                "row group {rg_idx}: row_count = 0 is illegal (segment-format-v1.md §6)"
            )));
        }
        if rg.columns.is_empty() {
            return Err(BqliteError::Execution(format!(
                "row group {rg_idx}: row group must contain at least one column chunk (§6)"
            )));
        }

        for (col_idx, chunk) in rg.columns.iter().enumerate() {
            // Column ordinal in range.
            let schema_col =
                request
                    .schema
                    .columns()
                    .get(chunk.column_ordinal as usize)
                    .ok_or_else(|| {
                        BqliteError::Execution(format!(
                            "row group {rg_idx} column {col_idx}: \
                             column_ordinal {} out of bounds (schema has {schema_column_count} columns)",
                            chunk.column_ordinal
                        ))
                    })?;

            // Chunk row arithmetic: non-null + null = row_group row_count.
            let chunk_total = chunk.encoded.row_count as u64 + chunk.null_count;
            if chunk_total != rg.row_count {
                return Err(BqliteError::Execution(format!(
                    "row group {rg_idx} column {col_idx} (`{}`): \
                     encoded non-null count ({}) + null count ({}) = {chunk_total} \
                     but parent row group row_count = {}",
                    schema_col.name, chunk.encoded.row_count, chunk.null_count, rg.row_count
                )));
            }

            // Null-bitmap presence vs schema nullability (§7).
            let expected_bitmap_len = rg.row_count.div_ceil(8) as usize;
            match (&chunk.null_bitmap, schema_col.nullable) {
                (Some(bitmap), true) => {
                    if bitmap.len() != expected_bitmap_len {
                        return Err(BqliteError::Execution(format!(
                            "row group {rg_idx} column {col_idx} (`{}`): \
                             null bitmap length {} != ceil(row_count / 8) = {expected_bitmap_len} \
                             (§7)",
                            schema_col.name,
                            bitmap.len()
                        )));
                    }
                }
                (None, true) => {
                    return Err(BqliteError::Execution(format!(
                        "row group {rg_idx} column {col_idx} (`{}`): \
                         nullable column requires a null bitmap (§7)",
                        schema_col.name
                    )));
                }
                (Some(_), false) => {
                    return Err(BqliteError::Execution(format!(
                        "row group {rg_idx} column {col_idx} (`{}`): \
                         non-nullable column must not carry a null bitmap (§7)",
                        schema_col.name
                    )));
                }
                (None, false) => {}
            }
        }
    }

    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp_path = tmp_path_for(path);

    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| io_ctx("create segment tmp file", &tmp_path, e))?;
        f.write_all(bytes)
            .map_err(|e| io_ctx("write segment tmp file", &tmp_path, e))?;
        f.sync_all()
            .map_err(|e| io_ctx("fsync segment tmp file", &tmp_path, e))?;
    }

    fs::rename(&tmp_path, path)
        .map_err(|e| io_ctx("rename segment tmp over final", &tmp_path, e))?;

    // Best-effort parent-dir fsync. Same rationale as `write_manifest_atomic`
    // in `database.rs`: some filesystems require a directory fsync for
    // the rename to be durable against an immediate power loss;
    // skipping it weakens durability but not correctness under process
    // crashes.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

fn tmp_path_for(path: &Path) -> PathBuf {
    // Suffix form (`segment-0001.bql.tmp`) so the TASK-239 startup
    // orphan sweep can find these by extension. Prefix form
    // (`.segment-0001.bql.tmp`) would hide them from directory listings
    // and trip the sweep's glob.
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".tmp");
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => PathBuf::from(name),
    }
}

fn io_ctx(action: &str, path: &Path, err: io::Error) -> BqliteError {
    BqliteError::Io(io::Error::new(
        err.kind(),
        format!("{action} {}: {err}", path.display()),
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{Encoding, EncodingType, Plain};
    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray};
    use bqlite_core::{ColumnDef, TableSchema};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    // ── Temp-dir helper ──────────────────────────────────────────────────────
    //
    // Mirrors `database.rs::Scratch` so we can keep the dev-dep set
    // small (no `tempfile` pull-in). PID + monotonic counter is enough
    // for in-process uniqueness; per-test directories are cleaned up on
    // drop.

    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-storage-seg-{label}-{pid}-{seq}"));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    // ── Fixtures ─────────────────────────────────────────────────────────────

    fn minimal_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap()
    }

    fn plain_encode_string(values: &[&str]) -> EncodedChunk {
        let array = StringArray::from(values.iter().map(|s| Some(*s)).collect::<Vec<_>>());
        Plain.encode(&array).unwrap()
    }

    fn plain_encode_timestamps(values: &[i64]) -> EncodedChunk {
        let array =
            TimestampNanosecondArray::from(values.iter().map(|v| Some(*v)).collect::<Vec<_>>())
                .with_timezone("UTC");
        Plain.encode(&array).unwrap()
    }

    fn plain_encode_ints(values: &[i64]) -> EncodedChunk {
        let array = Int64Array::from(values.iter().map(|v| Some(*v)).collect::<Vec<_>>());
        Plain.encode(&array).unwrap()
    }

    /// Construct a minimal-but-realistic three-column, one-row-group
    /// request. Used by every byte-layout test below as a known shape
    /// to assert against.
    fn sample_request() -> SegmentWriteRequest {
        let schema = minimal_schema();
        let entity_chunk = plain_encode_string(&["u1", "u1", "u2"]);
        let ts_chunk = plain_encode_timestamps(&[
            1_700_000_000_000_000_000,
            1_700_000_000_100_000_000,
            1_700_000_000_200_000_000,
        ]);
        let event_chunk = plain_encode_string(&["view", "checkout", "view"]);

        SegmentWriteRequest {
            schema,
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
                        zone_max: Some(PropertyValue::String("u2".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: ts_chunk,
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(1_700_000_000_000_000_000)),
                        zone_max: Some(PropertyValue::Timestamp(1_700_000_000_200_000_000)),
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
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 1_700_000_000_000_000_000,
            seq_id_range: (0, 2),
            batch_id: 7,
            compaction_level: 0,
        }
    }

    // ── Byte-exact framing assertions ───────────────────────────────────────

    #[test]
    fn encoded_segment_starts_with_magic_and_version() {
        let bytes = encode_segment(&sample_request()).unwrap();
        assert_eq!(&bytes[..4], b"BQLT", "file header magic");
        assert_eq!(
            u16::from_le_bytes(bytes[4..6].try_into().unwrap()),
            SEGMENT_FORMAT_VERSION,
            "file header version"
        );
    }

    #[test]
    fn encoded_segment_ends_with_trailer_magic() {
        let bytes = encode_segment(&sample_request()).unwrap();
        let tail = &bytes[bytes.len() - 4..];
        assert_eq!(tail, b"BQLT", "trailer magic");
    }

    #[test]
    fn encoded_segment_trailer_length_points_at_footer_start() {
        let bytes = encode_segment(&sample_request()).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;

        // The footer body sits immediately before the checksum + trailer.
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer_end = bytes.len() - 16;

        // Decode the footer body back and confirm it matches the request.
        let footer: FooterV1 = postcard::from_bytes(&bytes[footer_start..footer_end]).unwrap();
        assert_eq!(footer.format_version, SEGMENT_FORMAT_VERSION);
        assert_eq!(footer.row_count, 3);
        assert_eq!(footer.row_group_count, 1);
        assert_eq!(footer.row_groups.len(), 1);
        assert_eq!(footer.row_groups[0].columns.len(), 3);
        assert_eq!(footer.schema.name(), "events");
        assert_eq!(footer.batch_id, 7);
        assert_eq!(footer.seq_id_range, (0, 2));
    }

    #[test]
    fn encoded_segment_checksum_matches_xxhash64_over_prefix() {
        let bytes = encode_segment(&sample_request()).unwrap();
        let checksum_start = bytes.len() - 16;
        let stored = u64::from_le_bytes(
            bytes[checksum_start..checksum_start + 8]
                .try_into()
                .unwrap(),
        );
        let recomputed = XxHash64::oneshot(CHECKSUM_SEED, &bytes[..checksum_start]);
        assert_eq!(stored, recomputed, "checksum covers [0, file_size - 16)");
    }

    #[test]
    fn first_row_group_starts_right_after_the_file_header() {
        let bytes = encode_segment(&sample_request()).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();

        let first_rg = &footer.row_groups[0];
        assert_eq!(
            first_rg.byte_offset, FILE_HEADER_LEN as u64,
            "first row group begins at offset FILE_HEADER_LEN"
        );
        // Every column chunk in the row group is packed back-to-back and
        // the whole group ends where the row_group's byte_length says it
        // does.
        let mut cursor = first_rg.byte_offset;
        for chunk in &first_rg.columns {
            assert_eq!(chunk.byte_offset, cursor);
            cursor += chunk.byte_length;
        }
        assert_eq!(cursor, first_rg.byte_offset + first_rg.byte_length);
    }

    #[test]
    fn column_chunk_meta_records_encoding_and_compression_discriminants() {
        let bytes = encode_segment(&sample_request()).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();

        for col in &footer.row_groups[0].columns {
            assert_eq!(col.encoding, EncodingType::Plain.discriminant());
            assert_eq!(col.compression, CompressionType::None.discriminant());
            assert_eq!(col.row_count, 3);
            assert_eq!(col.null_count, 0);
            assert!(col.zone_min.is_some());
            assert!(col.zone_max.is_some());
        }
    }

    #[test]
    fn column_chunk_bytes_contain_encoding_discriminant_after_null_region() {
        // Non-nullable columns write the encoding discriminant at the
        // chunk's byte_offset directly (no null bitmap precedes it).
        let bytes = encode_segment(&sample_request()).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();

        for col in &footer.row_groups[0].columns {
            let start = col.byte_offset as usize;
            // Column is non-nullable → first byte is the encoding discriminant.
            assert_eq!(
                bytes[start],
                EncodingType::Plain.discriminant(),
                "chunk at offset {start} starts with Plain discriminant"
            );
            // Plain's params are empty, so u32 LE length appears next.
            let upl = u32::from_le_bytes(bytes[start + 1..start + 5].try_into().unwrap()) as usize;
            // The payload is verbatim (compression = None). byte_length =
            // 1 (disc) + 0 (params) + 4 (u32 LE) + upl (payload).
            assert_eq!(col.byte_length as usize, 1 + 4 + upl);
        }
    }

    #[test]
    fn segment_roundtrips_through_postcard_footer() {
        // End-to-end: encode, strip the footer body by computing its
        // offsets from the trailer, postcard-decode, verify every
        // value against the request.
        let req = sample_request();
        let bytes = encode_segment(&req).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();

        assert_eq!(footer.schema, req.schema);
        assert_eq!(footer.schema_version, req.schema_version);
        assert_eq!(footer.row_count, 3);
        assert_eq!(footer.seq_id_range, req.seq_id_range);
        assert_eq!(footer.batch_id, req.batch_id);
        assert_eq!(footer.compaction_level, req.compaction_level);
        assert_eq!(footer.row_group_size_hint, ROW_GROUP_SIZE_DEFAULT);
        assert_eq!(footer.creation_timestamp_ns, req.creation_timestamp_ns);
        assert!(footer.dictionaries.is_empty());
    }

    #[test]
    fn writer_is_deterministic_across_two_encodes() {
        // Same request → identical bytes. This is the determinism
        // property future reproducible-segment tests rely on.
        let req = sample_request();
        let a = encode_segment(&req).unwrap();
        let b = encode_segment(&req).unwrap();
        assert_eq!(a, b);
    }

    // ── Compression path ─────────────────────────────────────────────────────

    #[test]
    fn lz4_compression_wraps_payload_and_records_uncompressed_length() {
        // Build a schema whose property column is nullable so we also
        // exercise the null-bitmap path.
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("session_id", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();

        // Dense integer payload that LZ4 can shrink.
        let dense = Arc::new(Int64Array::from(vec![Some(42_i64); 64]));
        let encoded = Plain.encode(dense.as_ref()).unwrap();
        let uncompressed_len = encoded.payload.len();

        let row_count: u64 = 64;
        let bitmap = vec![0xFF_u8; row_count.div_ceil(8) as usize];

        let request = SegmentWriteRequest {
            schema,
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["u1"; 64]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: plain_encode_timestamps(
                            &(0..64)
                                .map(|i| 1_700_000_000_000_000_000 + i)
                                .collect::<Vec<_>>(),
                        ),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(1_700_000_000_000_000_000)),
                        zone_max: Some(PropertyValue::Timestamp(1_700_000_000_000_000_063)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["view"; 64]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(bitmap),
                        encoded,
                        compression: CompressionType::Lz4,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(42)),
                        zone_max: Some(PropertyValue::Int(42)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 63),
            batch_id: 0,
            compaction_level: 0,
        };

        let bytes = encode_segment(&request).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();

        let session_meta = &footer.row_groups[0].columns[3];
        assert_eq!(
            session_meta.compression,
            CompressionType::Lz4.discriminant()
        );

        // The on-disk column chunk layout for session_id:
        //   [null_bitmap (8 bytes)] [disc (1)] [params (0)] [u32 LE upl]
        //   [lz4-compressed payload]
        let chunk_start = session_meta.byte_offset as usize;
        let bitmap_len = 8; // ceil(64/8)
        let disc_offset = chunk_start + bitmap_len;
        assert_eq!(bytes[disc_offset], EncodingType::Plain.discriminant());
        let upl_offset = disc_offset + 1;
        let upl =
            u32::from_le_bytes(bytes[upl_offset..upl_offset + 4].try_into().unwrap()) as usize;
        assert_eq!(
            upl, uncompressed_len,
            "uncompressed_payload_length records original encoded payload size"
        );
        let payload_start = upl_offset + 4;
        let payload_end = session_meta.byte_offset as usize + session_meta.byte_length as usize;
        let compressed_payload = &bytes[payload_start..payload_end];
        // LZ4 should shrink 64 copies of i64=42 substantially.
        assert!(
            compressed_payload.len() < uncompressed_len,
            "lz4 compressed payload ({} bytes) should be smaller than original ({uncompressed_len} bytes)",
            compressed_payload.len()
        );
    }

    // ── Nullable column path ────────────────────────────────────────────────

    #[test]
    fn nullable_column_writes_bitmap_before_encoding_header() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("rank", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();

        // row_count = 3 rows, 1 null (row 1). Bitmap LSB-first: bit0=1, bit1=0, bit2=1 → 0b0000_0101 = 0x05.
        let bitmap = vec![0x05_u8];
        let non_null_values = plain_encode_ints(&[10, 30]);

        let request = SegmentWriteRequest {
            schema,
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 3,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["u1", "u1", "u1"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: plain_encode_timestamps(&[0, 1, 2]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(2)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["view", "view", "view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(bitmap),
                        encoded: non_null_values,
                        compression: CompressionType::None,
                        null_count: 1,
                        zone_min: Some(PropertyValue::Int(10)),
                        zone_max: Some(PropertyValue::Int(30)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 2),
            batch_id: 0,
            compaction_level: 0,
        };

        let bytes = encode_segment(&request).unwrap();

        // Decode the footer body to locate the rank column's byte range.
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();
        let rank = &footer.row_groups[0].columns[3];
        assert_eq!(rank.row_count, 2);
        assert_eq!(rank.null_count, 1);

        let start = rank.byte_offset as usize;
        assert_eq!(
            bytes[start], 0x05,
            "first byte of nullable chunk is the 0x05 LSB-first null bitmap"
        );
        // Next byte is the Plain encoding discriminant.
        assert_eq!(bytes[start + 1], EncodingType::Plain.discriminant());
    }

    // ── Dictionaries region ─────────────────────────────────────────────────

    #[test]
    fn dictionaries_region_lands_between_last_row_group_and_footer() {
        let mut request = sample_request();
        let dict_bytes = b"\x02\x00\x00\x00u1\x02\x00\x00\x00u2".to_vec();
        let dict_len = dict_bytes.len();
        request.dictionaries.push(PreparedDictionary {
            column_ordinal: 0,
            bytes: dict_bytes.clone(),
            cardinality: 2,
            value_type: BqlType::String,
        });

        let bytes = encode_segment(&request).unwrap();
        let trailer = &bytes[bytes.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap()) as usize;
        let footer_start = bytes.len() - 16 - footer_body_length;
        let footer: FooterV1 =
            postcard::from_bytes(&bytes[footer_start..bytes.len() - 16]).unwrap();

        assert_eq!(footer.dictionaries.len(), 1);
        let dref = &footer.dictionaries[0];
        let dict_start = dref.byte_offset as usize;
        let dict_end = dict_start + dref.byte_length as usize;
        assert_eq!(dref.byte_length as usize, dict_len);
        assert_eq!(&bytes[dict_start..dict_end], dict_bytes.as_slice());

        // The dictionary region sits between the last row group and the
        // footer body.
        let last_rg = footer.row_groups.last().unwrap();
        let row_groups_end = last_rg.byte_offset + last_rg.byte_length;
        assert_eq!(dref.byte_offset, row_groups_end);
        assert_eq!(dict_end, footer_start);
    }

    // ── Validation errors ──────────────────────────────────────────────────

    fn assert_execution_err(result: Result<Vec<u8>>, substr: &str) {
        match result {
            Ok(_) => panic!("expected Execution error containing `{substr}`, got Ok"),
            Err(BqliteError::Execution(msg)) => assert!(
                msg.contains(substr),
                "error message `{msg}` should contain `{substr}`"
            ),
            Err(other) => panic!("expected Execution error, got {other:?}"),
        }
    }

    #[test]
    fn empty_segment_is_rejected() {
        let request = SegmentWriteRequest {
            schema: minimal_schema(),
            schema_version: 0,
            row_groups: vec![],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 0),
            batch_id: 0,
            compaction_level: 0,
        };
        assert_execution_err(encode_segment(&request), "at least one row group");
    }

    #[test]
    fn empty_row_group_is_rejected() {
        let request = SegmentWriteRequest {
            schema: minimal_schema(),
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 0,
                columns: vec![],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 0),
            batch_id: 0,
            compaction_level: 0,
        };
        assert_execution_err(encode_segment(&request), "row_count = 0 is illegal");
    }

    #[test]
    fn chunk_row_count_mismatch_is_rejected() {
        let mut request = sample_request();
        // Drop one non-null value but leave the row group row_count at 3.
        request.row_groups[0].columns[0].encoded = plain_encode_string(&["u1", "u1"]);
        assert_execution_err(
            encode_segment(&request),
            "parent row group row_count = 3",
        );
    }

    #[test]
    fn null_bitmap_missing_on_nullable_column_is_rejected() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("rank", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();
        let request = SegmentWriteRequest {
            schema,
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 1,
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["u1"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: plain_encode_timestamps(&[0]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["view"]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: None, // missing!
                        encoded: plain_encode_ints(&[1]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(1)),
                        zone_max: Some(PropertyValue::Int(1)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 0),
            batch_id: 0,
            compaction_level: 0,
        };
        assert_execution_err(
            encode_segment(&request),
            "nullable column requires a null bitmap",
        );
    }

    #[test]
    fn null_bitmap_on_non_nullable_column_is_rejected() {
        let mut request = sample_request();
        request.row_groups[0].columns[0].null_bitmap = Some(vec![0xFF]);
        assert_execution_err(
            encode_segment(&request),
            "non-nullable column must not carry a null bitmap",
        );
    }

    #[test]
    fn null_bitmap_length_mismatch_is_rejected() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("entity_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("rank", BqlType::Int),
            ],
            "entity_id",
            "ts",
            "event_type",
        )
        .unwrap();
        let request = SegmentWriteRequest {
            schema,
            schema_version: 0,
            row_groups: vec![PreparedRowGroup {
                row_count: 16, // needs a 2-byte bitmap (ceil(16/8) = 2)
                columns: vec![
                    PreparedColumnChunk {
                        column_ordinal: 0,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["u1"; 16]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("u1".into())),
                        zone_max: Some(PropertyValue::String("u1".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 1,
                        null_bitmap: None,
                        encoded: plain_encode_timestamps(&[0; 16]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Timestamp(0)),
                        zone_max: Some(PropertyValue::Timestamp(0)),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 2,
                        null_bitmap: None,
                        encoded: plain_encode_string(&["view"; 16]),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::String("view".into())),
                        zone_max: Some(PropertyValue::String("view".into())),
                    },
                    PreparedColumnChunk {
                        column_ordinal: 3,
                        null_bitmap: Some(vec![0xFF]), // only 1 byte
                        encoded: plain_encode_ints(&(0..16).collect::<Vec<_>>()),
                        compression: CompressionType::None,
                        null_count: 0,
                        zone_min: Some(PropertyValue::Int(0)),
                        zone_max: Some(PropertyValue::Int(15)),
                    },
                ],
            }],
            dictionaries: vec![],
            creation_timestamp_ns: 0,
            seq_id_range: (0, 15),
            batch_id: 0,
            compaction_level: 0,
        };
        assert_execution_err(encode_segment(&request), "null bitmap length 1");
    }

    #[test]
    fn column_ordinal_out_of_bounds_is_rejected() {
        let mut request = sample_request();
        request.row_groups[0].columns[0].column_ordinal = 99;
        assert_execution_err(encode_segment(&request), "out of bounds");
    }

    // ── Atomic file write ───────────────────────────────────────────────────

    #[test]
    fn write_segment_produces_file_with_summary() {
        let dir = Scratch::new("write");
        let path = dir.path().join("segment-0001.bql");
        let summary = write_segment(&path, &sample_request()).unwrap();

        assert!(path.exists(), "segment file was created");
        // No orphan .tmp file left behind after a successful write.
        let tmp = path.with_extension("bql.tmp");
        assert!(!tmp.exists(), "no leftover temp file at {}", tmp.display());

        // Re-read and cross-check the summary against the file bytes.
        let on_disk = std::fs::read(&path).unwrap();
        assert_eq!(on_disk.len() as u64, summary.byte_size);
        assert_eq!(&on_disk[..4], b"BQLT");
        assert_eq!(&on_disk[on_disk.len() - 4..], b"BQLT");

        let trailer = &on_disk[on_disk.len() - 8..];
        let footer_body_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap());
        assert_eq!(footer_body_length, summary.footer_body_length);

        let checksum_bytes = &on_disk[on_disk.len() - 16..on_disk.len() - 8];
        let checksum = u64::from_le_bytes(checksum_bytes.try_into().unwrap());
        assert_eq!(checksum, summary.checksum);
        assert_eq!(summary.row_count, 3);
        assert_eq!(summary.row_group_count, 1);
    }

    #[test]
    fn write_segment_overwrites_existing_file_at_path() {
        let dir = Scratch::new("overwrite");
        let path = dir.path().join("segment-0001.bql");
        // Pre-existing file at the target path (simulating a retry).
        std::fs::write(&path, b"stale bytes").unwrap();

        write_segment(&path, &sample_request()).unwrap();

        let on_disk = std::fs::read(&path).unwrap();
        assert_eq!(&on_disk[..4], b"BQLT");
        assert_ne!(on_disk.as_slice(), b"stale bytes");
    }
}
