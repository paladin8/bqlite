//! Entity-sorted segment writer orchestration (TASK-214).
//!
//! Sits **above** the byte-layout writer in [`crate::segment::writer`]
//! and **below** the ingest partitioner in [`crate::ingest::partitioner`].
//! Its job is to take a sorted `(entity_id, timestamp)` event stream
//! out of the partitioner, group the rows into row groups that respect
//! the entity-boundary invariant in
//! `docs/design/storage/segment-format-v1.md` §7.2, run each column
//! chunk through the encoding selector, hoist any per-segment
//! dictionaries the selector produced, build a fully-prepared
//! [`SegmentWriteRequest`], emit the segment file atomically, and
//! atomically register the resulting [`SegmentMeta`] in the manifest.
//!
//! # Layered ownership
//!
//! | Layer | Owner | Module |
//! |---|---|---|
//! | Sorted event stream into `(window, shard)` buckets | TASK-218 | [`crate::ingest::partitioner`] |
//! | Encoding selection + LZ4 wrapping | TASK-212 | [`crate::encoding::selector`] |
//! | **Row-group packing, null/zone extraction, dictionary hoist** (this module) | **TASK-214** | **`crate::writer`** |
//! | Byte-level segment file emission | TASK-213 | [`crate::segment::writer`] |
//! | Manifest atomic update | TASK-217 | [`crate::manifest`] / [`crate::database`] |
//!
//! Each layer has a single responsibility and is independently
//! testable. The orchestrator below ties them together but never
//! reaches across layer boundaries — it never touches segment bytes
//! directly, never picks an encoding, never mutates the manifest
//! behind the [`Database`] API.
//!
//! # Entity-boundary invariant
//!
//! Per `segment-format-v1.md` §7.2:
//!
//! > An entity that fits in a single row group is never split across
//! > two row groups. Entities larger than `ROW_GROUP_SIZE_DEFAULT`
//! > occupy consecutive row groups within a segment.
//!
//! [`plan_row_groups`] implements this rule. It walks the input
//! starting from the row-group target size (default 65,536), and:
//!
//! - When the natural cut at `target_size` lands on an entity
//!   boundary, the row group ends there.
//! - When the natural cut lands mid-entity, it backs the boundary up
//!   to the previous entity transition. The current row group ends
//!   shorter than the target, and the partial entity becomes the
//!   first entity of the next row group.
//! - When backing up would empty the current row group, the entity is
//!   strictly larger than the target — the row group fills to the
//!   target and the entity continues into the next consecutive row
//!   group.
//!
//! The result is a sequence of row groups that (a) honor the
//! entity-locality invariant and (b) come as close to the target size
//! as the data permits.
//!
//! # Dictionary hoisting
//!
//! [`crate::encoding::Dictionary`] returns `EncodedChunk`s whose
//! `params` block inlines the dictionary values for round-trip-test
//! self-containment (see the dictionary module docs). The on-disk
//! segment layout pinned by §9.2, however, stores each column's
//! dictionary once at the segment level (in the segment dictionaries
//! region) and the per-row-group params shrink to a 5-byte
//! `dict_id (u32 LE) + code_bit_width (u8)` header.
//!
//! [`hoist_dictionary_chunk`] performs the conversion: it parses the
//! 6-byte trait-level header (`type_tag, code_bit_width, cardinality`)
//! and the variable-length dictionary block that follows it, registers
//! the dictionary as a [`PreparedDictionary`] in the segment-level
//! dictionaries vec, and rewrites the chunk's `params` to the on-disk
//! 5-byte form. The payload — the bit-packed code stream — is
//! unchanged.
//!
//! Wave 2 always allocates a fresh `dict_id` per `(row_group, column)`
//! pair; cross-row-group dictionary deduplication is a Wave 4
//! optimization that the segment-format spec leaves room for.
//!
//! # Test strategy
//!
//! The orchestration logic is split into pure functions that the
//! tests below exercise without touching the filesystem
//! ([`plan_row_groups`], [`events_to_record_batch`],
//! [`hoist_dictionary_chunk`]). The end-to-end path is covered by
//! tests that build a real [`Database`], call [`SegmentWriter::write_bucket`],
//! and re-read the resulting segment through
//! [`crate::segment::reader::SegmentFileReader`] to assert the row
//! values are exactly what went in.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, RecordBatch, StringViewArray, StringViewBuilder, TimestampNanosecondArray,
    TimestampNanosecondBuilder,
};
use ::arrow::compute;
use ::arrow::datatypes::{Field, Schema as ArrowSchema};
use bqlite_core::arrow::bql_type_to_arrow;
use bqlite_core::event::{EntityId, Event};
use bqlite_core::{BqlType, BqliteError, ColumnDef, PropertyValue, Result, TableSchema};

use crate::database::Database;
use crate::encoding::{select_encoding, EncodedChunk, EncodingType, SelectedEncoding};
use crate::ingest::partitioner::Partitioner;
use crate::manifest::{ColumnStats, SegmentMeta};
use crate::segment::layout::{SEGMENT_FORMAT_VERSION_V1, SEGMENT_FORMAT_VERSION_V2};
use crate::segment::writer::{
    write_segment, PreparedColumnChunk, PreparedDictionary, PreparedRowGroup, SegmentWriteRequest,
    SegmentWriteSummary,
};

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Default target row count per row group.
///
/// Matches the `ROW_GROUP_SIZE_DEFAULT` constant from
/// `segment-format-v1.md` §6.2 (which the byte-layout writer uses as
/// the `row_group_size_hint` it stamps into the footer).
pub const DEFAULT_ROW_GROUP_SIZE: usize = 65_536;

/// Tunable knobs for the segment writer orchestration.
#[derive(Debug, Clone, Copy)]
pub struct WriterConfig {
    /// Target row count per row group. The actual row count for any
    /// given row group may be smaller (entity-aligned cut, last group
    /// in the segment, or a single oversized entity that fills it).
    pub row_group_size: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SegmentWriter — high-level orchestrator
// ─────────────────────────────────────────────────────────────────────────────

/// High-level segment writer bound to a [`Database`].
///
/// Holds a mutable borrow of the database for the orchestration's
/// duration so that segment-id allocation, sequence-id reservation,
/// and manifest registration share a single critical section per
/// bucket. Constructed fresh for each ingest call — there is no
/// hidden state to amortize.
pub struct SegmentWriter<'a> {
    db: &'a mut Database,
    config: WriterConfig,
}

impl<'a> SegmentWriter<'a> {
    /// Construct a new writer with the default [`WriterConfig`].
    pub fn new(db: &'a mut Database) -> Self {
        Self {
            db,
            config: WriterConfig::default(),
        }
    }

    /// Construct a new writer with a custom [`WriterConfig`].
    pub fn with_config(db: &'a mut Database, config: WriterConfig) -> Self {
        Self { db, config }
    }

    /// Write one segment containing every event in `sorted_events` for
    /// the `(table_name, window_id, shard_id)` bucket and register it
    /// in the manifest.
    ///
    /// `sorted_events` must be pre-sorted by `(entity_id, timestamp)`
    /// — the partitioner's `drain_sorted` already produces this order.
    /// `batch_id` is the value [`Database::allocate_batch_id`] returned
    /// at the start of the ingest call; this writer does not allocate
    /// it because one ingest may produce many segments (one per
    /// bucket) and they all share the same `batch_id`.
    ///
    /// On success, returns the [`SegmentMeta`] that was added to the
    /// manifest. On failure, the manifest is left untouched (no
    /// partial registration) and any partial segment file is removed
    /// best-effort so the next startup orphan sweep has less to do.
    pub fn write_bucket(
        &mut self,
        table_name: &str,
        window_id: u32,
        shard_id: u16,
        batch_id: u64,
        sorted_events: &[Event],
    ) -> Result<SegmentMeta> {
        if sorted_events.is_empty() {
            return Err(BqliteError::Execution(
                "writer: sorted_events is empty — segments must contain at least one row \
                 (segment-format-v1.md §6)"
                    .into(),
            ));
        }

        // Schema snapshot for this segment — pulled from the live
        // manifest right before we start preparing the request so that
        // a concurrent ALTER TABLE ADD COLUMN that lands while we're
        // writing produces a segment carrying the older schema (which
        // is exactly what schema evolution §14 expects).
        let table_entry = self.db.manifest().tables.get(table_name).ok_or_else(|| {
            BqliteError::Execution(format!("writer: unknown table '{table_name}'"))
        })?;
        let schema = table_entry.schema.clone();
        let schema_version = schema.version();

        // Allocate the per-segment counters that the manifest tracks.
        let segment_id = self.db.allocate_segment_id(table_name)?;
        let row_count = sorted_events.len() as u64;
        let seq_id_range = self.db.allocate_sequence_id_range(table_name, row_count)?;

        // Prepare the segment in memory: row groups, encodings,
        // dictionaries, zone maps, segment meta skeleton.
        let prepared = match prepare_segment(
            &schema,
            schema_version,
            sorted_events,
            batch_id,
            seq_id_range,
            &self.config,
        ) {
            Ok(p) => p,
            Err(e) => {
                // Allocator side-effects already landed; let them
                // retire as gaps per the §6.2 gap-tolerance rule. The
                // alternative — rewinding the manifest counters — is
                // not safe under concurrent ingest.
                return Err(e);
            }
        };

        // Compose the SegmentMeta from the prepared aggregates plus
        // the byte_size we get back from the file write.
        let path = segment_file_path(self.db.root(), table_name, window_id, shard_id, segment_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                BqliteError::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "writer: failed to create segment directory {}: {e}",
                        parent.display()
                    ),
                ))
            })?;
        }

        let summary: SegmentWriteSummary = match write_segment(&path, &prepared.request) {
            Ok(s) => s,
            Err(e) => {
                // Best-effort cleanup of any partial file the
                // byte-level writer left behind. The TASK-239 startup
                // orphan sweep handles whatever escapes here.
                let _ = std::fs::remove_file(&path);
                return Err(e);
            }
        };

        let segment_meta = build_segment_meta(
            segment_id,
            schema_version,
            batch_id,
            &prepared,
            &summary,
            prepared.created_at_ns,
        );

        // Atomically register in the manifest. On failure, attempt to
        // remove the segment file so we don't leak it (the next
        // startup orphan sweep would catch it anyway).
        if let Err(e) =
            self.db
                .add_segment(table_name, window_id, shard_id.into(), segment_meta.clone())
        {
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }

        Ok(segment_meta)
    }

    /// Drain a [`Partitioner`] and write one segment per non-empty
    /// `(window, shard)` bucket. Returns the [`SegmentMeta`]s in
    /// drain order — ascending `(window_id, shard_id)`.
    ///
    /// All segments share the partitioner's `batch_id`. The first
    /// failed bucket aborts the drain — earlier segments stay
    /// committed (they were each registered atomically), later
    /// buckets are dropped on the floor.
    pub fn write_partitioner(
        &mut self,
        table_name: &str,
        partitioner: Partitioner,
    ) -> Result<Vec<SegmentMeta>> {
        let batch_id = partitioner.batch_id();
        let mut metas = Vec::new();
        for ((window_id, shard_id), events) in partitioner.drain_sorted() {
            if events.is_empty() {
                continue;
            }
            let meta = self.write_bucket(table_name, window_id, shard_id, batch_id, &events)?;
            metas.push(meta);
        }
        Ok(metas)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internals — pure functions, independently testable
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory prepared segment: the [`SegmentWriteRequest`] the byte
/// writer needs, plus the per-column aggregates the manifest's
/// [`SegmentMeta`] needs. Held together so the orchestrator can hand
/// both to their respective consumers without recomputing them.
struct PreparedSegment {
    request: SegmentWriteRequest,
    /// Whole-segment per-column aggregates for the manifest. Indexed
    /// by column ordinal (matches `schema.columns()`).
    column_aggregates: Vec<ColumnAggregate>,
    entity_range: (PropertyValue, PropertyValue),
    ts_range: (i64, i64),
    row_count: u64,
    created_at_ns: i64,
}

/// Whole-segment per-column min/max/null-count, accumulated across
/// every row group in the segment. Used to populate
/// [`SegmentMeta::column_stats`] without re-reading the segment file.
#[derive(Debug, Clone)]
struct ColumnAggregate {
    column_name: String,
    min: Option<PropertyValue>,
    max: Option<PropertyValue>,
    null_count: u64,
}

/// Convert a sorted `[Event]` slice into a single segment ready for
/// the byte writer.
fn prepare_segment(
    schema: &TableSchema,
    schema_version: u32,
    sorted_events: &[Event],
    batch_id: u64,
    seq_id_range: (u64, u64),
    config: &WriterConfig,
) -> Result<PreparedSegment> {
    debug_assert!(!sorted_events.is_empty());

    // 1. Materialize the whole bucket into one Arrow batch.
    let full_batch = events_to_record_batch(schema, sorted_events)?;

    // 2. Plan row group boundaries respecting entity locality.
    let groups = plan_row_groups(sorted_events, config.row_group_size);
    debug_assert!(!groups.is_empty());

    // 3. Walk row groups, encode columns, hoist dictionaries, gather
    //    aggregates.
    let mut prepared_groups: Vec<PreparedRowGroup> = Vec::with_capacity(groups.len());
    let mut dictionaries: Vec<PreparedDictionary> = Vec::new();
    let mut column_aggregates: Vec<ColumnAggregate> = schema
        .columns()
        .iter()
        .map(|c| ColumnAggregate {
            column_name: c.name.clone(),
            min: None,
            max: None,
            null_count: 0,
        })
        .collect();

    for group_range in &groups {
        let group_len = group_range.end - group_range.start;
        let group_batch = full_batch.slice(group_range.start, group_len);

        let mut prepared_columns: Vec<PreparedColumnChunk> =
            Vec::with_capacity(schema.columns().len());

        for (col_ordinal, col_def) in schema.columns().iter().enumerate() {
            let array = group_batch.column(col_ordinal).clone();
            let chunk = build_column_chunk(col_ordinal as u32, col_def, &array, &mut dictionaries)?;

            // Fold the chunk's stats into the segment-level aggregates.
            let agg = &mut column_aggregates[col_ordinal];
            agg.null_count += chunk.null_count;
            merge_extrema(&mut agg.min, &mut agg.max, &chunk.zone_min, &chunk.zone_max);

            prepared_columns.push(chunk);
        }

        prepared_groups.push(PreparedRowGroup {
            row_count: group_len as u64,
            columns: prepared_columns,
        });
    }

    // 4. Compute segment-level (entity_range, ts_range) directly from
    //    the input — they're cheap from a sorted slice.
    let entity_range = entity_range_for(sorted_events, schema)?;
    let ts_range = (
        sorted_events.first().unwrap().timestamp_nanos(),
        sorted_events.last().unwrap().timestamp_nanos(),
    );

    let created_at_ns = current_timestamp_ns();

    // Pick the on-disk format version from the encodings the selector
    // produced. Segments that only contain v1 codecs stay v1 so they
    // are readable by older binaries; the moment any Wave 4 codec
    // lands in a chunk, the writer promotes the whole segment to v2
    // per `segment-format-v2.md` §9.1 ("compaction always uses the
    // current writer, and the current writer produces v2"). This is
    // additive — a Wave 2 reader opening a v2 segment fails cleanly
    // at the file-header version check (§8.3).
    let format_version = derived_format_version(&prepared_groups);

    let request = SegmentWriteRequest {
        schema: schema.clone(),
        schema_version,
        row_groups: prepared_groups,
        dictionaries,
        creation_timestamp_ns: created_at_ns,
        seq_id_range,
        batch_id,
        compaction_level: 0,
        fsst_symbol_tables: vec![],
        format_version,
    };

    Ok(PreparedSegment {
        request,
        column_aggregates,
        entity_range,
        ts_range,
        row_count: sorted_events.len() as u64,
        created_at_ns,
    })
}

/// Build a [`SegmentMeta`] from a [`PreparedSegment`] plus the
/// byte-size returned by the segment writer.
fn build_segment_meta(
    segment_id: u64,
    schema_version: u32,
    batch_id: u64,
    prepared: &PreparedSegment,
    summary: &SegmentWriteSummary,
    created_at_ns: i64,
) -> SegmentMeta {
    let column_stats = prepared
        .column_aggregates
        .iter()
        .map(|agg| ColumnStats {
            column_name: agg.column_name.clone(),
            min: agg.min.clone(),
            max: agg.max.clone(),
            null_count: agg.null_count,
            distinct_count_estimate: None,
        })
        .collect();

    SegmentMeta {
        segment_id,
        level: 0,
        schema_version,
        row_count: prepared.row_count,
        byte_size: summary.byte_size,
        ts_range: prepared.ts_range,
        entity_range: prepared.entity_range.clone(),
        column_stats,
        created_at: created_at_ns,
        batch_id,
    }
}

// ── Format-version derivation ───────────────────────────────────────────────

/// Pick the on-disk format version for a prepared segment based on the
/// encodings the selector chose. If every chunk uses a v1-exclusive
/// codec (Plain, Dictionary, Delta, BitPacking, Constant) the result
/// is v1; the moment any Wave 4 codec appears in any chunk the result
/// is v2.
///
/// This mirrors `segment-format-v2.md` §9.1's rule that the current
/// writer always produces the newest format a chunk requires. v1
/// readers reject unknown file-header versions cleanly (§8.3), so
/// downgrading a fresh v2 segment on a Wave 2 binary is an
/// error-on-open — exactly what the spec intends.
fn derived_format_version(groups: &[PreparedRowGroup]) -> u16 {
    for rg in groups {
        for chunk in &rg.columns {
            if encoding_requires_v2(chunk.encoded.encoding) {
                return SEGMENT_FORMAT_VERSION_V2;
            }
        }
    }
    SEGMENT_FORMAT_VERSION_V1
}

/// Predicate matching `advanced-encodings.md` §1.2 — the six Wave 4
/// codecs that extend the v1 encoding set in v2.
fn encoding_requires_v2(e: EncodingType) -> bool {
    matches!(
        e,
        EncodingType::Rle
            | EncodingType::DoubleDelta
            | EncodingType::For
            | EncodingType::PFor
            | EncodingType::Fsst
            | EncodingType::Alp
    )
}

// ── Row-group planning ──────────────────────────────────────────────────────

/// Plan row group boundaries that honor the entity-locality invariant
/// from `segment-format-v1.md` §7.2.
///
/// Returns a list of `start..end` ranges into `events`. Every range is
/// non-empty, the union covers `[0, events.len())` exactly, and:
///
/// - When `events` is empty, returns an empty `Vec`.
/// - When the natural cut at `target_size` lands on an entity
///   boundary, the row group ends there.
/// - When the natural cut would split an entity, the boundary backs
///   up to the previous entity transition.
/// - When backing up would empty the current row group (i.e. one
///   single entity is wider than `target_size`), the row group fills
///   to `target_size` and the entity continues into the next
///   consecutive row group.
///
/// Pure function — no Arrow buffers, no allocation beyond the output
/// vec. The orchestrator passes the result to the segment writer
/// without further processing.
pub(crate) fn plan_row_groups(events: &[Event], target_size: usize) -> Vec<Range<usize>> {
    assert!(target_size > 0, "row_group target_size must be > 0");
    let n = events.len();
    let mut groups = Vec::new();
    let mut start = 0usize;

    while start < n {
        let raw_end = (start + target_size).min(n);
        let end = if raw_end == n {
            // The remainder fits in this group (last group, possibly short).
            raw_end
        } else if events[raw_end - 1].entity != events[raw_end].entity {
            // Natural cut at the entity boundary.
            raw_end
        } else {
            // Mid-entity at raw_end. Walk back to the start of the
            // entity that straddles the cut and end the row group there.
            let split_entity = &events[raw_end].entity;
            let mut back = raw_end;
            while back > start && &events[back - 1].entity == split_entity {
                back -= 1;
            }
            if back == start {
                // The entire current group is one entity that hasn't
                // ended yet — the entity is larger than target_size.
                // Emit a target-sized group; the entity will continue
                // in the next iteration.
                raw_end
            } else {
                back
            }
        };
        groups.push(start..end);
        start = end;
    }

    groups
}

// ── Event → Arrow RecordBatch ───────────────────────────────────────────────

/// Project a sorted event slice into an Arrow [`RecordBatch`] matching
/// `schema`'s column order.
///
/// Uses a single pass over the event slice to populate all column
/// builders simultaneously, minimizing cache-line re-reads for the
/// property `Vec` — each event's heap data is pulled into cache exactly
/// once instead of once per column.
///
/// Role columns (entity key, event time, event type) are populated
/// from `event.entity`, `event.timestamp`, and `event.event_type`
/// respectively. All other columns are looked up by name in
/// `event.properties`; missing properties become NULL on nullable
/// columns and surface a typed error on NOT NULL columns.
///
/// Wave 2 supports the primitive [`BqlType`] set (`Bool`, `Int`,
/// `Float`, `String`, `Timestamp`). Nested types (`List`, `Map`) error
/// with `BqliteError::Execution` — they are deferred to a later wave
/// alongside the encoding-layer support.
pub(crate) fn events_to_record_batch(
    schema: &TableSchema,
    events: &[Event],
) -> Result<RecordBatch> {
    let row_count = events.len();
    let cols = schema.columns();
    let n_cols = cols.len();
    let entity_idx = schema.entity_key_index();
    let ts_idx = schema.timestamp_index();
    let et_idx = schema.event_type_index();

    // Reject nested types up front so the single-pass loop can assume
    // only primitive types.
    for col in cols {
        if matches!(col.bql_type, BqlType::List(_) | BqlType::Map(_)) {
            return Err(BqliteError::Execution(format!(
                "writer: nested column `{}` (type {:?}) is not yet supported by Wave 2 ingest",
                col.name, col.bql_type
            )));
        }
    }

    // Pre-compute property hints for all property columns. Role columns
    // get `None` — they are populated from direct struct fields.
    let hints: Vec<Option<usize>> = (0..n_cols)
        .map(|i| {
            if i == entity_idx || i == ts_idx || i == et_idx {
                None
            } else {
                property_hint(events, &cols[i])
            }
        })
        .collect();

    // Per-column role tag, precomputed to avoid repeated comparisons
    // inside the inner loop.
    #[derive(Clone, Copy)]
    enum ColRole {
        EntityKey,
        Timestamp,
        EventType,
        Property,
    }
    let roles: Vec<ColRole> = (0..n_cols)
        .map(|i| {
            if i == entity_idx {
                ColRole::EntityKey
            } else if i == ts_idx {
                ColRole::Timestamp
            } else if i == et_idx {
                ColRole::EventType
            } else {
                ColRole::Property
            }
        })
        .collect();

    // Typed builder enum — holds one builder per column so the single-
    // pass loop can append to any column via a match on the enum arm.
    enum ColBuilder {
        StringView(StringViewBuilder),
        Int64(Int64Builder),
        Float64(Float64Builder),
        Bool(BooleanBuilder),
        TimestampNs(TimestampNanosecondBuilder),
    }

    let mut builders: Vec<ColBuilder> = cols
        .iter()
        .enumerate()
        .map(|(i, col)| {
            // The timestamp column uses a timezone-aware builder
            // regardless of whether it's declared as Timestamp in the
            // schema (the role column is always Timestamp(Ns, UTC)).
            if i == ts_idx {
                return ColBuilder::TimestampNs(
                    TimestampNanosecondBuilder::with_capacity(row_count).with_timezone("UTC"),
                );
            }
            match col.bql_type {
                BqlType::String => {
                    ColBuilder::StringView(StringViewBuilder::with_capacity(row_count))
                }
                BqlType::Int => ColBuilder::Int64(Int64Builder::with_capacity(row_count)),
                BqlType::Float => ColBuilder::Float64(Float64Builder::with_capacity(row_count)),
                BqlType::Bool => ColBuilder::Bool(BooleanBuilder::with_capacity(row_count)),
                BqlType::Timestamp => ColBuilder::TimestampNs(
                    TimestampNanosecondBuilder::with_capacity(row_count).with_timezone("UTC"),
                ),
                // Nested types were rejected above.
                BqlType::List(_) | BqlType::Map(_) => unreachable!(),
            }
        })
        .collect();

    // ── Single pass over all events ────────────────────────────────
    for event in events {
        for col_idx in 0..n_cols {
            match roles[col_idx] {
                ColRole::EntityKey => match (&mut builders[col_idx], &event.entity) {
                    (ColBuilder::StringView(b), EntityId::String(s)) => b.append_value(s),
                    (ColBuilder::Int64(b), EntityId::Int(n)) => b.append_value(*n),
                    _ => {
                        return Err(BqliteError::Execution(format!(
                            "writer: entity key column `{}` type mismatch with event entity {:?}",
                            cols[col_idx].name, event.entity
                        )));
                    }
                },
                ColRole::Timestamp => match &mut builders[col_idx] {
                    ColBuilder::TimestampNs(b) => b.append_value(event.timestamp_nanos()),
                    _ => unreachable!("timestamp column always uses TimestampNs builder"),
                },
                ColRole::EventType => match &mut builders[col_idx] {
                    ColBuilder::StringView(b) => b.append_value(&event.event_type),
                    _ => unreachable!("event-type column always uses StringView builder"),
                },
                ColRole::Property => {
                    let col_def = &cols[col_idx];
                    let value = fast_property_for(event, col_def, hints[col_idx])?;
                    match (&mut builders[col_idx], value) {
                        (ColBuilder::Bool(b), Some(PropertyValue::Bool(v))) => {
                            b.append_value(*v);
                        }
                        (ColBuilder::Int64(b), Some(PropertyValue::Int(n))) => {
                            b.append_value(*n);
                        }
                        (ColBuilder::Float64(b), Some(PropertyValue::Float(f))) => {
                            b.append_value(*f);
                        }
                        (ColBuilder::StringView(b), Some(PropertyValue::String(s))) => {
                            b.append_value(s);
                        }
                        (ColBuilder::TimestampNs(b), Some(PropertyValue::Timestamp(ns))) => {
                            b.append_value(*ns);
                        }
                        // All nullable builder types: append null.
                        (ColBuilder::Bool(b), Some(PropertyValue::Null) | None) => {
                            b.append_null();
                        }
                        (ColBuilder::Int64(b), Some(PropertyValue::Null) | None) => {
                            b.append_null();
                        }
                        (ColBuilder::Float64(b), Some(PropertyValue::Null) | None) => {
                            b.append_null();
                        }
                        (ColBuilder::StringView(b), Some(PropertyValue::Null) | None) => {
                            b.append_null();
                        }
                        (ColBuilder::TimestampNs(b), Some(PropertyValue::Null) | None) => {
                            b.append_null();
                        }
                        (_, Some(other)) => return Err(type_mismatch(col_def, other)),
                    }
                }
            }
        }
    }

    // ── Finish builders and assemble RecordBatch ───────────────────
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|b| -> ArrayRef {
            match b {
                ColBuilder::StringView(mut b) => Arc::new(b.finish()),
                ColBuilder::Int64(mut b) => Arc::new(b.finish()),
                ColBuilder::Float64(mut b) => Arc::new(b.finish()),
                ColBuilder::Bool(mut b) => Arc::new(b.finish()),
                ColBuilder::TimestampNs(mut b) => Arc::new(b.finish()),
            }
        })
        .collect();

    let arrow_fields: Vec<Field> = cols
        .iter()
        .map(|c| {
            let dt = bql_type_to_arrow(&c.bql_type);
            Field::new(&c.name, dt, c.nullable)
        })
        .collect();
    let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

    RecordBatch::try_new(arrow_schema, arrays).map_err(|e| {
        BqliteError::Execution(format!(
            "writer: failed to assemble RecordBatch for {row_count} events: {e}"
        ))
    })
}

/// Look up the property value for `col` on `event` using a precomputed
/// `hint_idx` to skip the O(n) linear scan in the common case where
/// every event carries properties in the same order (true for all
/// events produced by the CSV reader or INSERT VALUES path in a single
/// ingest call). Falls back to the full name scan when the hint misses.
///
/// Returns `None` when the property is absent. Errors when the column
/// is non-nullable and the property is missing or null.
#[inline]
fn fast_property_for<'a>(
    event: &'a Event,
    col: &ColumnDef,
    hint_idx: Option<usize>,
) -> Result<Option<&'a PropertyValue>> {
    let value = match hint_idx {
        Some(idx) if idx < event.properties.len() && event.properties[idx].0 == col.name => {
            Some(&event.properties[idx].1)
        }
        _ => event.get(&col.name),
    };
    match value {
        Some(PropertyValue::Null) | None if !col.nullable => Err(BqliteError::Execution(format!(
            "writer: NOT NULL column `{}` has no value for an event (entity={:?}, ts={})",
            col.name,
            event.entity,
            event.timestamp_nanos()
        ))),
        v => Ok(v),
    }
}

/// Precompute the index of `col` within the first event's properties
/// list. Returns `None` when `events` is empty or the column is not
/// present on the first event.
#[inline]
fn property_hint(events: &[Event], col: &ColumnDef) -> Option<usize> {
    events
        .first()
        .and_then(|e| e.properties.iter().position(|(k, _)| k == &col.name))
}

fn type_mismatch(col: &ColumnDef, value: &PropertyValue) -> BqliteError {
    BqliteError::Execution(format!(
        "writer: column `{}` expects type {:?} but got value {:?}",
        col.name, col.bql_type, value
    ))
}

// ── Column chunk preparation ────────────────────────────────────────────────

/// Build a [`PreparedColumnChunk`] for one column slice in one row group.
///
/// Splits the input into a null bitmap (for nullable columns) and the
/// dense non-null values, runs the dense values through the encoding
/// selector, hoists any segment-level dictionary the selector
/// produced, computes inline zone-map extrema, and assembles the
/// chunk struct the byte writer expects.
fn build_column_chunk(
    column_ordinal: u32,
    col_def: &ColumnDef,
    array: &ArrayRef,
    segment_dicts: &mut Vec<PreparedDictionary>,
) -> Result<PreparedColumnChunk> {
    let null_count = array.null_count();

    // Validate the nullability contract before doing any work.
    if !col_def.nullable && null_count > 0 {
        return Err(BqliteError::Execution(format!(
            "writer: NOT NULL column `{}` row group has {null_count} null value(s)",
            col_def.name
        )));
    }

    // Densify and bitmap.
    let null_bitmap = if col_def.nullable {
        Some(build_null_bitmap_lsb_first(array.as_ref()))
    } else {
        None
    };
    let dense = densify(array)?;

    // Zone-map extrema over non-null values, before encoding so that
    // an encoder failure still surfaces at the right column.
    let (zone_min, zone_max) = compute_zone_extrema(dense.as_ref(), &col_def.bql_type)?;

    // Encoding selection.
    let selected: SelectedEncoding = select_encoding(dense.as_ref(), &col_def.bql_type)?;
    let SelectedEncoding {
        chunk,
        compression,
        on_disk_payload: _,
    } = selected;

    // Dictionary hoist (no-op for every other encoding).
    let final_chunk = if chunk.encoding == EncodingType::Dictionary {
        hoist_dictionary_chunk(&chunk, column_ordinal, &col_def.bql_type, segment_dicts)?
    } else {
        chunk
    };

    Ok(PreparedColumnChunk {
        column_ordinal,
        null_bitmap,
        encoded: final_chunk,
        compression,
        null_count: null_count as u64,
        zone_min,
        zone_max,
    })
}

/// Strip nulls from `array` so the encoding layer sees a dense input.
fn densify(array: &ArrayRef) -> Result<ArrayRef> {
    if array.null_count() == 0 {
        return Ok(array.clone());
    }
    let nulls = array.nulls().expect("null_count > 0 implies a null buffer");
    let mask = BooleanArray::new(nulls.inner().clone(), None);
    compute::filter(array, &mask)
        .map_err(|e| BqliteError::Execution(format!("writer: failed to densify column: {e}")))
}

/// LSB-first null bitmap byte layout matching `segment-format-v1.md`
/// §7. Bit `i` in byte `i / 8` (LSB-first within the byte) is `1`
/// when row `i` is non-null.
///
/// For an array with no nulls, returns an all-ones bitmap of length
/// `ceil(row_count / 8)`. The byte writer expects `Some(bitmap)` for
/// every nullable column even when no rows are null.
fn build_null_bitmap_lsb_first(array: &dyn Array) -> Vec<u8> {
    let row_count = array.len();
    let bytes_needed = row_count.div_ceil(8);

    if let Some(nulls) = array.nulls() {
        let buf = nulls.inner();
        let offset = buf.offset();
        if offset % 8 == 0 {
            // Fast path: byte-aligned validity buffer — bulk copy the
            // bytes instead of iterating per-bit.
            let byte_offset = offset / 8;
            let src = &buf.values()[byte_offset..byte_offset + bytes_needed];
            let mut bitmap = src.to_vec();
            // Clear any trailing bits beyond row_count so downstream
            // consumers see exact zeros past the last valid row.
            if !row_count.is_multiple_of(8) {
                let mask = (1u8 << (row_count % 8)) - 1;
                bitmap[bytes_needed - 1] &= mask;
            }
            bitmap
        } else {
            // Slow path: non-byte-aligned offset requires per-bit copy.
            let mut bitmap = vec![0u8; bytes_needed];
            for i in 0..row_count {
                if buf.value(i) {
                    bitmap[i / 8] |= 1 << (i % 8);
                }
            }
            bitmap
        }
    } else {
        // No null buffer = all non-null. Fill with 0xFF and mask the
        // trailing byte.
        let mut bitmap = vec![0xFFu8; bytes_needed];
        if !row_count.is_multiple_of(8) {
            let mask = (1u8 << (row_count % 8)) - 1;
            bitmap[bytes_needed - 1] = mask;
        }
        bitmap
    }
}

// ── Zone map extraction ─────────────────────────────────────────────────────

/// Compute `(min, max)` of the dense (non-null) values in `array` for
/// the column's [`BqlType`]. Returns `(None, None)` for empty arrays.
fn compute_zone_extrema(
    array: &dyn Array,
    ty: &BqlType,
) -> Result<(Option<PropertyValue>, Option<PropertyValue>)> {
    if array.is_empty() {
        return Ok((None, None));
    }
    match ty {
        BqlType::Bool => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "writer: Bool zone-map: input is not BooleanArray".into(),
                    )
                })?;
            // false < true, so the running min and max collapse to
            // (any-false?, any-true?). Track both and decode at the
            // end — saves a per-row branch in the common all-same case.
            let mut has_false = false;
            let mut has_true = false;
            for i in 0..arr.len() {
                if arr.value(i) {
                    has_true = true;
                } else {
                    has_false = true;
                }
                if has_false && has_true {
                    break;
                }
            }
            // At least one bit is set because the array is non-empty
            // (caller checked) — so lo and hi are well-defined.
            let lo = !has_false; // if any false → lo = false; else lo = true
            let hi = has_true; // if any true → hi = true; else hi = false
            Ok((Some(PropertyValue::Bool(lo)), Some(PropertyValue::Bool(hi))))
        }
        BqlType::Int => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                BqliteError::Execution("writer: Int zone-map: input is not Int64Array".into())
            })?;
            let values = arr.values();
            let mut lo = values[0];
            let mut hi = values[0];
            for v in &values[1..] {
                if *v < lo {
                    lo = *v;
                }
                if *v > hi {
                    hi = *v;
                }
            }
            Ok((Some(PropertyValue::Int(lo)), Some(PropertyValue::Int(hi))))
        }
        BqlType::Float => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "writer: Float zone-map: input is not Float64Array".into(),
                    )
                })?;
            let values = arr.values();
            // Use total_cmp so NaN doesn't break the ordering — every
            // non-null float compares deterministically.
            let mut lo = values[0];
            let mut hi = values[0];
            for v in &values[1..] {
                if v.total_cmp(&lo).is_lt() {
                    lo = *v;
                }
                if v.total_cmp(&hi).is_gt() {
                    hi = *v;
                }
            }
            Ok((
                Some(PropertyValue::Float(lo)),
                Some(PropertyValue::Float(hi)),
            ))
        }
        BqlType::Timestamp => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "writer: Timestamp zone-map: input is not TimestampNanosecondArray".into(),
                    )
                })?;
            let values = arr.values();
            let mut lo = values[0];
            let mut hi = values[0];
            for v in &values[1..] {
                if *v < lo {
                    lo = *v;
                }
                if *v > hi {
                    hi = *v;
                }
            }
            Ok((
                Some(PropertyValue::Timestamp(lo)),
                Some(PropertyValue::Timestamp(hi)),
            ))
        }
        BqlType::String => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    BqliteError::Execution(
                        "writer: String zone-map: input is not StringViewArray".into(),
                    )
                })?;
            let mut lo: &str = arr.value(0);
            let mut hi: &str = arr.value(0);
            for i in 1..arr.len() {
                let v = arr.value(i);
                if v < lo {
                    lo = v;
                }
                if v > hi {
                    hi = v;
                }
            }
            Ok((
                Some(PropertyValue::String(lo.to_owned())),
                Some(PropertyValue::String(hi.to_owned())),
            ))
        }
        BqlType::List(_) | BqlType::Map(_) => Err(BqliteError::Execution(format!(
            "writer: zone map for nested type {ty:?} is not supported in Wave 2"
        ))),
    }
}

/// Fold a per-row-group `(min, max)` pair into the segment-level
/// running aggregate. The aggregate is updated in place; either
/// argument may be `None` (an all-null row group contributes nothing).
fn merge_extrema(
    agg_min: &mut Option<PropertyValue>,
    agg_max: &mut Option<PropertyValue>,
    chunk_min: &Option<PropertyValue>,
    chunk_max: &Option<PropertyValue>,
) {
    if let Some(cmin) = chunk_min {
        match agg_min {
            None => *agg_min = Some(cmin.clone()),
            Some(existing) if cmin < existing => *agg_min = Some(cmin.clone()),
            _ => {}
        }
    }
    if let Some(cmax) = chunk_max {
        match agg_max {
            None => *agg_max = Some(cmax.clone()),
            Some(existing) if cmax > existing => *agg_max = Some(cmax.clone()),
            _ => {}
        }
    }
}

/// Compute `(min_entity, max_entity)` for a sorted-by-(entity, ts)
/// event slice. Cheap because the input is already sorted: the first
/// and last entities bound the segment.
fn entity_range_for(
    events: &[Event],
    schema: &TableSchema,
) -> Result<(PropertyValue, PropertyValue)> {
    let first = entity_to_property_value(&events[0].entity, schema)?;
    let last = entity_to_property_value(&events[events.len() - 1].entity, schema)?;
    Ok((first, last))
}

fn entity_to_property_value(entity: &EntityId, schema: &TableSchema) -> Result<PropertyValue> {
    let col = schema.entity_key_column();
    match (entity, &col.bql_type) {
        (EntityId::String(s), BqlType::String) => Ok(PropertyValue::String(s.clone())),
        (EntityId::Int(n), BqlType::Int) => Ok(PropertyValue::Int(*n)),
        (entity, ty) => Err(BqliteError::Execution(format!(
            "writer: entity {entity:?} does not match entity-key column type {ty:?}"
        ))),
    }
}

// ── Dictionary hoist ────────────────────────────────────────────────────────

/// Trait-level Dictionary `params` header size: `type_tag (u8) +
/// code_bit_width (u8) + cardinality (u32 LE)`. The variable-length
/// dictionary block follows.
const DICT_TRAIT_PARAMS_HEADER: usize = 6;

/// Dictionary type tag for `Int`, matching the encoding module's
/// `TYPE_TAG_INT` constant.
const DICT_TYPE_TAG_INT: u8 = 0;
/// Dictionary type tag for `String`, matching the encoding module's
/// `TYPE_TAG_STRING` constant.
const DICT_TYPE_TAG_STRING: u8 = 1;

/// Convert a self-contained Dictionary [`EncodedChunk`] into the
/// on-disk hoisted form by registering its dictionary block as a
/// segment-level [`PreparedDictionary`] and rewriting the chunk's
/// `params` to the 5-byte `dict_id (u32 LE) + code_bit_width (u8)`
/// header from `segment-format-v1.md` §9.2.
///
/// The chunk's `payload` (the bit-packed code stream) is unchanged.
pub(crate) fn hoist_dictionary_chunk(
    chunk: &EncodedChunk,
    column_ordinal: u32,
    col_type: &BqlType,
    segment_dicts: &mut Vec<PreparedDictionary>,
) -> Result<EncodedChunk> {
    if chunk.encoding != EncodingType::Dictionary {
        return Err(BqliteError::Execution(format!(
            "writer: hoist_dictionary_chunk called with non-Dictionary chunk ({:?})",
            chunk.encoding
        )));
    }
    if chunk.params.len() < DICT_TRAIT_PARAMS_HEADER {
        return Err(BqliteError::Execution(format!(
            "writer: Dictionary trait params too short: {} bytes (expected ≥ {})",
            chunk.params.len(),
            DICT_TRAIT_PARAMS_HEADER
        )));
    }
    let type_tag = chunk.params[0];
    let code_bit_width = chunk.params[1];
    let cardinality = u32::from_le_bytes(
        chunk.params[2..6]
            .try_into()
            .expect("slice length checked above"),
    );

    let value_type = match type_tag {
        DICT_TYPE_TAG_INT => BqlType::Int,
        DICT_TYPE_TAG_STRING => BqlType::String,
        other => {
            return Err(BqliteError::Execution(format!(
                "writer: Dictionary type_tag {other} is not 0 (Int) or 1 (String)"
            )));
        }
    };
    if &value_type != col_type {
        return Err(BqliteError::Execution(format!(
            "writer: Dictionary type_tag denotes {value_type:?} but column type is {col_type:?}"
        )));
    }

    // The segment-level dictionary bytes are everything after the
    // 6-byte trait header. The trait dictionary block is already in
    // the on-disk format §10.3 expects (i64 LE for Int, length-prefix
    // + UTF-8 for String), so the slice is byte-for-byte what
    // PreparedDictionary needs.
    let dict_bytes = chunk.params[DICT_TRAIT_PARAMS_HEADER..].to_vec();

    let dict_id = u32::try_from(segment_dicts.len()).map_err(|_| {
        BqliteError::Execution("writer: too many segment dictionaries to fit a u32 dict_id".into())
    })?;
    segment_dicts.push(PreparedDictionary {
        column_ordinal,
        bytes: dict_bytes,
        cardinality,
        value_type,
    });

    let mut on_disk_params = Vec::with_capacity(5);
    on_disk_params.extend_from_slice(&dict_id.to_le_bytes());
    on_disk_params.push(code_bit_width);

    Ok(EncodedChunk {
        encoding: EncodingType::Dictionary,
        params: on_disk_params,
        payload: chunk.payload.clone(),
        row_count: chunk.row_count,
    })
}

// ── Filesystem path helpers ─────────────────────────────────────────────────

/// Compute the on-disk path for a segment file matching the directory
/// layout in `docs/design/storage-format.md` §5.2:
///
/// ```text
/// <db_root>/<table>/windows/w_<window_id_zero_padded_6>/shard_<shard_id_zero_padded_2>/segment_<segment_id>.seg
/// ```
fn segment_file_path(
    db_root: &Path,
    table: &str,
    window_id: u32,
    shard_id: u16,
    segment_id: u64,
) -> PathBuf {
    db_root
        .join(table)
        .join("windows")
        .join(format!("w_{window_id:06}"))
        .join(format!("shard_{shard_id:02}"))
        .join(format!("segment_{segment_id}.seg"))
}

/// Current wall-clock time in epoch nanoseconds. Used as the
/// segment's `created_at` stamp. A test seam exists below for
/// determinism — production code calls this once per segment so the
/// gap between successive segments is meaningful for compaction
/// scheduling.
fn current_timestamp_ns() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::reader::SegmentFileReader;
    use bqlite_core::storage::{ColumnProjection, SegmentScan};
    use bqlite_core::time::Timestamp;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    // ── Scratch directory helper ────────────────────────────────────────────

    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-writer-{label}-{pid}-{seq}"));
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

    // ── Schema fixtures ─────────────────────────────────────────────────────

    fn events_schema() -> TableSchema {
        TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::nullable("amount", BqlType::Int),
                ColumnDef::nullable("country", BqlType::String),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .expect("schema")
    }

    fn ev(user: &str, ts_ns: i64, event_type: &str) -> Event {
        Event::new(EntityId::from(user), Timestamp(ts_ns), event_type)
    }

    fn ev_with(
        user: &str,
        ts_ns: i64,
        event_type: &str,
        props: Vec<(&str, PropertyValue)>,
    ) -> Event {
        Event::with_properties(
            EntityId::from(user),
            Timestamp(ts_ns),
            event_type,
            props.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        )
    }

    fn open_db_with_events_table(path: &Path) -> Database {
        let mut db = Database::create(path).expect("create db");
        db.create_table("events".into(), events_schema())
            .expect("create events table");
        db
    }

    // ── plan_row_groups ─────────────────────────────────────────────────────

    #[test]
    fn plan_empty_input_returns_empty_groups() {
        let groups = plan_row_groups(&[], 4);
        assert!(groups.is_empty());
    }

    #[test]
    fn plan_single_event_one_group() {
        let events = vec![ev("alice", 1, "click")];
        let groups = plan_row_groups(&events, 8);
        assert_eq!(groups, vec![0..1]);
    }

    #[test]
    fn plan_under_target_one_group() {
        let events: Vec<Event> = (0..5).map(|i| ev("alice", i, "click")).collect();
        let groups = plan_row_groups(&events, 8);
        assert_eq!(groups, vec![0..5]);
    }

    #[test]
    fn plan_natural_entity_boundary() {
        // 4 events of "alice", 4 events of "bob", target = 4. Natural
        // cut at index 4 lands on the entity boundary.
        let mut events = Vec::new();
        events.extend((0..4).map(|i| ev("alice", i, "click")));
        events.extend((0..4).map(|i| ev("bob", i, "click")));
        let groups = plan_row_groups(&events, 4);
        assert_eq!(groups, vec![0..4, 4..8]);
    }

    #[test]
    fn plan_backs_up_to_entity_boundary_when_cut_splits_entity() {
        // 3 events of "alice", 5 events of "bob", target = 5. Natural
        // cut at index 5 lands inside the bob run; we back up to
        // index 3 (start of bob).
        let mut events = Vec::new();
        events.extend((0..3).map(|i| ev("alice", i, "click")));
        events.extend((0..5).map(|i| ev("bob", i, "click")));
        let groups = plan_row_groups(&events, 5);
        // First group: 3 alice rows. Second group: 5 bob rows.
        assert_eq!(groups, vec![0..3, 3..8]);
    }

    #[test]
    fn plan_oversized_entity_spans_consecutive_groups() {
        // 12 events of "alice" (single entity), target = 5. The
        // entity is larger than the target, so it occupies 3
        // consecutive row groups: [0..5), [5..10), [10..12).
        let events: Vec<Event> = (0..12).map(|i| ev("alice", i, "click")).collect();
        let groups = plan_row_groups(&events, 5);
        assert_eq!(groups, vec![0..5, 5..10, 10..12]);
    }

    #[test]
    fn plan_oversized_entity_followed_by_small_entities() {
        // 7 events of "alice", then 2 of "bob", 1 of "charlie",
        // target = 5. The 7-row alice spans the first row group
        // entirely [0..5). The second row group starts at the alice
        // tail (rows 5,6) and greedily fills to the target with bob
        // and charlie ([5..10), 5 rows). The entity-locality
        // invariant holds: alice (>target) occupies consecutive row
        // groups, bob and charlie (≤target) each live in a single
        // row group (they happen to share that group with the alice
        // tail, which is allowed because alice already exceeds the
        // target).
        let mut events = Vec::new();
        events.extend((0..7).map(|i| ev("alice", i, "click")));
        events.extend((0..2).map(|i| ev("bob", i, "click")));
        events.push(ev("charlie", 0, "click"));
        let groups = plan_row_groups(&events, 5);
        assert_eq!(groups, vec![0..5, 5..10]);
    }

    #[test]
    fn plan_three_small_entities_pack_into_one_group() {
        let mut events = Vec::new();
        events.extend((0..2).map(|i| ev("alice", i, "click")));
        events.extend((0..2).map(|i| ev("bob", i, "click")));
        events.extend((0..2).map(|i| ev("charlie", i, "click")));
        let groups = plan_row_groups(&events, 8);
        assert_eq!(groups, vec![0..6]);
    }

    #[test]
    fn plan_groups_cover_input_exactly() {
        // Property: the union of returned ranges equals [0, n) and
        // the ranges are non-overlapping and contiguous.
        for target in [1, 2, 3, 5, 8] {
            let mut events = Vec::new();
            events.extend((0..3).map(|i| ev("alice", i, "click")));
            events.extend((0..7).map(|i| ev("bob", i, "click")));
            events.extend((0..1).map(|i| ev("charlie", i, "click")));
            events.extend((0..4).map(|i| ev("dave", i, "click")));
            let groups = plan_row_groups(&events, target);
            assert!(!groups.is_empty(), "target = {target}");
            assert_eq!(groups[0].start, 0);
            assert_eq!(groups[groups.len() - 1].end, events.len());
            for w in groups.windows(2) {
                assert_eq!(w[0].end, w[1].start, "groups must be contiguous");
            }
            for r in &groups {
                assert!(r.end > r.start, "non-empty range");
            }
        }
    }

    // ── events_to_record_batch ──────────────────────────────────────────────

    #[test]
    fn events_to_record_batch_populates_role_columns() {
        let schema = events_schema();
        let events = vec![
            ev("alice", 100, "click"),
            ev("bob", 200, "view"),
            ev("alice", 300, "purchase"),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 5);

        // user_id (StringView)
        let user_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(user_id.value(0), "alice");
        assert_eq!(user_id.value(1), "bob");
        assert_eq!(user_id.value(2), "alice");

        // ts (TimestampNanosecond, UTC)
        let ts = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 100);
        assert_eq!(ts.value(1), 200);
        assert_eq!(ts.value(2), 300);

        // event_type (StringView)
        let et = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(et.value(0), "click");
        assert_eq!(et.value(1), "view");
        assert_eq!(et.value(2), "purchase");

        // amount (nullable Int) — all null because no events provided it
        let amount = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amount.null_count(), 3);
    }

    #[test]
    fn events_to_record_batch_picks_up_property_values() {
        let schema = events_schema();
        let events = vec![
            ev_with(
                "alice",
                100,
                "purchase",
                vec![
                    ("amount", PropertyValue::Int(42)),
                    ("country", PropertyValue::String("US".into())),
                ],
            ),
            ev_with(
                "bob",
                200,
                "purchase",
                vec![("amount", PropertyValue::Int(7))],
            ),
            ev("charlie", 300, "view"),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let amount = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amount.value(0), 42);
        assert_eq!(amount.value(1), 7);
        assert!(amount.is_null(2), "missing amount → null");

        let country = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(country.value(0), "US");
        assert!(country.is_null(1));
        assert!(country.is_null(2));
    }

    #[test]
    fn events_to_record_batch_errors_on_not_null_missing_property() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("user_id", BqlType::String),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("event_type", BqlType::String),
                ColumnDef::required("required_prop", BqlType::Int),
            ],
            "user_id",
            "ts",
            "event_type",
        )
        .unwrap();
        let events = vec![ev("alice", 100, "click")];
        let err = events_to_record_batch(&schema, &events).expect_err("missing must error");
        match err {
            BqliteError::Execution(msg) => {
                assert!(msg.contains("required_prop"), "got: {msg}");
                assert!(msg.contains("NOT NULL"), "got: {msg}");
            }
            other => panic!("expected Execution, got {other:?}"),
        }
    }

    #[test]
    fn events_to_record_batch_errors_on_type_mismatch() {
        let schema = events_schema();
        let events = vec![ev_with(
            "alice",
            100,
            "click",
            vec![("amount", PropertyValue::String("not-a-number".into()))],
        )];
        let err = events_to_record_batch(&schema, &events).expect_err("type mismatch must error");
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn events_to_record_batch_int_entity_key() {
        let schema = TableSchema::new(
            "events",
            vec![
                ColumnDef::required("uid", BqlType::Int),
                ColumnDef::required("ts", BqlType::Timestamp),
                ColumnDef::required("ev", BqlType::String),
            ],
            "uid",
            "ts",
            "ev",
        )
        .unwrap();
        let events = vec![
            Event::new(EntityId::from(1_i64), Timestamp(0), "click"),
            Event::new(EntityId::from(2_i64), Timestamp(1), "view"),
        ];
        let batch = events_to_record_batch(&schema, &events).unwrap();
        let uids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(uids.value(0), 1);
        assert_eq!(uids.value(1), 2);
    }

    // ── End-to-end: write a segment and read it back ────────────────────────

    #[test]
    fn write_bucket_round_trips_through_segment_reader() {
        let scratch = Scratch::new("e2e-roundtrip");
        let mut db = open_db_with_events_table(scratch.path());

        let events = vec![
            ev_with(
                "alice",
                1_700_000_000_000_000_000,
                "click",
                vec![
                    ("amount", PropertyValue::Int(10)),
                    ("country", PropertyValue::String("US".into())),
                ],
            ),
            ev_with(
                "alice",
                1_700_000_000_100_000_000,
                "view",
                vec![("amount", PropertyValue::Int(5))],
            ),
            ev_with(
                "bob",
                1_700_000_000_200_000_000,
                "click",
                vec![("country", PropertyValue::String("UK".into()))],
            ),
            ev("charlie", 1_700_000_000_300_000_000, "purchase"),
        ];

        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::new(&mut db);
        let meta = writer
            .write_bucket("events", 0, 0, batch_id, &events)
            .expect("write_bucket");

        assert_eq!(meta.row_count, 4);
        assert_eq!(meta.batch_id, batch_id);
        assert_eq!(meta.level, 0);
        assert_eq!(meta.schema_version, events_schema().version());
        assert_eq!(
            meta.ts_range,
            (1_700_000_000_000_000_000, 1_700_000_000_300_000_000)
        );
        assert_eq!(
            meta.entity_range,
            (
                PropertyValue::String("alice".into()),
                PropertyValue::String("charlie".into())
            )
        );

        // The segment is registered in the manifest at (window=0, shard=0).
        let registered = &db.manifest().tables["events"].windows[0].shards[0];
        assert_eq!(registered.len(), 1);
        assert_eq!(registered[0].segment_id, meta.segment_id);

        // The on-disk file lives at the §5.2 path.
        let path = scratch
            .path()
            .join("events")
            .join("windows")
            .join("w_000000")
            .join("shard_00")
            .join(format!("segment_{}.seg", meta.segment_id));
        assert!(path.exists(), "segment file written at {}", path.display());

        // Read it back through the SegmentFileReader and verify rows.
        let schema = events_schema();
        let reader = SegmentFileReader::open(&path, schema.clone()).expect("open segment");
        assert_eq!(reader.row_count(), 4);

        let mut scan = reader.scan(&ColumnProjection::all(), None).expect("scan");
        let mut total_rows = 0;
        while let Some(rg) = scan.next_row_group().expect("decode row group") {
            total_rows += rg.num_rows();
            // Sanity-check the entity-key column survived encoding.
            let user_id = rg
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("user_id is StringView");
            for i in 0..user_id.len() {
                let entity = user_id.value(i);
                assert!(["alice", "bob", "charlie"].contains(&entity));
            }
        }
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn write_bucket_uses_dictionary_encoding_for_low_cardinality_strings() {
        // Many rows of just two distinct event types — the selector
        // should pick Dictionary, which exercises the hoist path.
        let scratch = Scratch::new("e2e-dict");
        let mut db = open_db_with_events_table(scratch.path());

        let events: Vec<Event> = (0..200)
            .map(|i| {
                let user = format!("u{:03}", i % 10);
                let ts = 1_700_000_000_000_000_000 + i;
                let kind = if i % 2 == 0 { "click" } else { "view" };
                ev(&user, ts, kind)
            })
            .collect();
        // Sort by (entity, ts) since the writer assumes sorted input.
        let mut sorted = events;
        sorted.sort_by(|a, b| a.entity.cmp(&b.entity).then(a.timestamp.cmp(&b.timestamp)));

        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::new(&mut db);
        let meta = writer
            .write_bucket("events", 0, 0, batch_id, &sorted)
            .expect("write_bucket");
        assert_eq!(meta.row_count, 200);

        // Open and verify the segment-level dictionaries vector is
        // non-empty (the hoisted shape).
        let path = scratch
            .path()
            .join("events")
            .join("windows")
            .join("w_000000")
            .join("shard_00")
            .join(format!("segment_{}.seg", meta.segment_id));
        let reader = SegmentFileReader::open(&path, events_schema()).expect("open segment");
        assert!(
            !reader.dictionaries().is_empty(),
            "low-cardinality string column should produce a segment-level dictionary"
        );

        // Round-trip: scan and confirm we still get exactly 200 rows.
        let mut scan = reader.scan(&ColumnProjection::all(), None).unwrap();
        let mut rows = 0;
        while let Some(rg) = scan.next_row_group().unwrap() {
            rows += rg.num_rows();
        }
        assert_eq!(rows, 200);
    }

    #[test]
    fn low_cardinality_workload_stays_on_format_version_v1() {
        // A dictionary-heavy workload selects only v1 codecs
        // (Dictionary, Delta, Constant). The writer must keep the
        // segment on format_version 1 so old readers can still open it.
        let scratch = Scratch::new("v1-fv");
        let mut db = open_db_with_events_table(scratch.path());

        let events: Vec<Event> = (0..200)
            .map(|i| {
                let user = format!("u{:03}", i % 10);
                let ts = 1_700_000_000_000_000_000 + i;
                let kind = if i % 2 == 0 { "click" } else { "view" };
                ev(&user, ts, kind)
            })
            .collect();
        let mut sorted = events;
        sorted.sort_by(|a, b| a.entity.cmp(&b.entity).then(a.timestamp.cmp(&b.timestamp)));

        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::new(&mut db);
        let meta = writer
            .write_bucket("events", 0, 0, batch_id, &sorted)
            .expect("write_bucket");

        let path = scratch
            .path()
            .join("events")
            .join("windows")
            .join("w_000000")
            .join("shard_00")
            .join(format!("segment_{}.seg", meta.segment_id));
        let reader = SegmentFileReader::open(&path, events_schema()).unwrap();
        assert_eq!(reader.footer().format_version(), SEGMENT_FORMAT_VERSION_V1);
    }

    #[test]
    fn wave4_codec_promotes_segment_to_format_version_v2() {
        // Hand-build a row-group whose only chunk uses a v2 codec and
        // feed it straight into `derived_format_version` — the helper
        // must short-circuit to v2 the moment any chunk reports a
        // Wave 4 encoding. This test anchors the writer's v1→v2
        // promotion contract without having to construct a full
        // `Database` + `SegmentWriter` pipeline.
        let rg_v1 = PreparedRowGroup {
            row_count: 4,
            columns: vec![PreparedColumnChunk {
                column_ordinal: 0,
                null_bitmap: None,
                encoded: EncodedChunk {
                    encoding: EncodingType::Plain,
                    params: vec![],
                    payload: vec![],
                    row_count: 4,
                },
                compression: crate::encoding::CompressionType::None,
                null_count: 0,
                zone_min: None,
                zone_max: None,
            }],
        };
        assert_eq!(derived_format_version(&[rg_v1]), SEGMENT_FORMAT_VERSION_V1);

        for v2_enc in [
            EncodingType::Rle,
            EncodingType::DoubleDelta,
            EncodingType::For,
            EncodingType::PFor,
            EncodingType::Fsst,
            EncodingType::Alp,
        ] {
            let rg_v2 = PreparedRowGroup {
                row_count: 4,
                columns: vec![PreparedColumnChunk {
                    column_ordinal: 0,
                    null_bitmap: None,
                    encoded: EncodedChunk {
                        encoding: v2_enc,
                        params: vec![],
                        payload: vec![],
                        row_count: 4,
                    },
                    compression: crate::encoding::CompressionType::None,
                    null_count: 0,
                    zone_min: None,
                    zone_max: None,
                }],
            };
            assert_eq!(
                derived_format_version(&[rg_v2]),
                SEGMENT_FORMAT_VERSION_V2,
                "{v2_enc:?} must promote the segment to v2"
            );
        }
    }

    #[test]
    fn write_bucket_splits_large_entity_into_consecutive_row_groups() {
        // One entity with more rows than the row_group_size — the
        // segment must contain multiple row groups all belonging to
        // the same entity.
        let scratch = Scratch::new("e2e-large-entity");
        let mut db = open_db_with_events_table(scratch.path());

        let events: Vec<Event> = (0..2_500)
            .map(|i| ev("alice", 1_000 + i, "click"))
            .collect();
        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::with_config(
            &mut db,
            WriterConfig {
                row_group_size: 1_000,
            },
        );
        let meta = writer
            .write_bucket("events", 0, 0, batch_id, &events)
            .unwrap();
        assert_eq!(meta.row_count, 2_500);

        let path = scratch
            .path()
            .join("events")
            .join("windows")
            .join("w_000000")
            .join("shard_00")
            .join(format!("segment_{}.seg", meta.segment_id));
        let reader = SegmentFileReader::open(&path, events_schema()).unwrap();
        // 2,500 rows / 1,000 target = 3 row groups (1000, 1000, 500).
        assert_eq!(reader.row_group_count(), 3);
    }

    /// Multi-row-group dictionary round-trip: write enough rows to span
    /// 3+ row groups using a small `row_group_size`, with low-cardinality
    /// strings that trigger dictionary encoding in each row group. Each
    /// row group gets its own segment-level dictionary (`dict_id` per
    /// (row_group, column) pair). Verify that every decoded value matches
    /// the original event data — a dictionary lookup that resolves codes
    /// against the wrong `dict_id` would produce corrupted strings.
    #[test]
    fn write_bucket_multi_row_group_dictionary_round_trip() {
        let scratch = Scratch::new("e2e-multi-rg-dict");
        let mut db = open_db_with_events_table(scratch.path());

        // 10 entities × 50 rows each = 500 total rows.
        // row_group_size = 100 → at least 5 row groups, each with
        // dictionary-encoded `event_type` and `country` columns whose
        // dictionaries differ per row group (different entities land in
        // different row groups and carry different country values).
        let row_group_size = 100;
        let rows_per_entity = 50;
        let entity_count = 10;
        let total_rows = entity_count * rows_per_entity;

        let event_types = ["click", "view", "purchase"];
        let countries = ["US", "UK", "DE", "FR", "JP"];

        let mut events: Vec<Event> = Vec::with_capacity(total_rows);
        for eid in 0..entity_count {
            let user = format!("user_{eid:03}");
            for i in 0..rows_per_entity {
                let ts = 1_700_000_000_000_000_000_i64 + (eid * rows_per_entity + i) as i64;
                let et = event_types[(eid + i) % event_types.len()];
                // Vary country per entity so different row groups get
                // different dictionary contents.
                let country = countries[eid % countries.len()];
                events.push(ev_with(
                    &user,
                    ts,
                    et,
                    vec![
                        ("amount", PropertyValue::Int(i as i64 * 10)),
                        ("country", PropertyValue::String(country.into())),
                    ],
                ));
            }
        }

        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::with_config(&mut db, WriterConfig { row_group_size });
        let meta = writer
            .write_bucket("events", 0, 0, batch_id, &events)
            .expect("write_bucket");
        assert_eq!(meta.row_count as usize, total_rows);

        // Open and verify multiple row groups.
        let path = scratch
            .path()
            .join("events")
            .join("windows")
            .join("w_000000")
            .join("shard_00")
            .join(format!("segment_{}.seg", meta.segment_id));
        let schema = events_schema();
        let reader = SegmentFileReader::open(&path, schema.clone()).expect("open segment");
        assert!(
            reader.row_group_count() >= 3,
            "expected ≥3 row groups with row_group_size={row_group_size} and {total_rows} rows, \
             got {}",
            reader.row_group_count()
        );
        // Multiple dictionaries should have been hoisted (at least one per
        // dictionary-encoded column per row group).
        assert!(
            reader.dictionaries().len() >= reader.row_group_count(),
            "expected at least one segment-level dictionary per row group, \
             got {} dicts for {} row groups",
            reader.dictionaries().len(),
            reader.row_group_count()
        );

        // Read back all row groups and collect decoded values.
        let mut scan = reader.scan(&ColumnProjection::all(), None).expect("scan");
        let mut decoded_users: Vec<String> = Vec::new();
        let mut decoded_event_types: Vec<String> = Vec::new();
        let mut decoded_amounts: Vec<Option<i64>> = Vec::new();
        let mut decoded_countries: Vec<Option<String>> = Vec::new();
        let mut row_groups_read = 0usize;
        while let Some(rg) = scan.next_row_group().expect("decode row group") {
            row_groups_read += 1;
            let n = rg.num_rows();
            // user_id (col 0)
            let user_col = rg
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("user_id is StringView");
            for i in 0..n {
                decoded_users.push(user_col.value(i).to_string());
            }
            // event_type (col 2)
            let et_col = rg
                .column(2)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("event_type is StringView");
            for i in 0..n {
                decoded_event_types.push(et_col.value(i).to_string());
            }
            // amount (col 3, nullable Int)
            let amount_col = rg
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("amount is Int64");
            for i in 0..n {
                if amount_col.is_null(i) {
                    decoded_amounts.push(None);
                } else {
                    decoded_amounts.push(Some(amount_col.value(i)));
                }
            }
            // country (col 4, nullable String)
            let country_col = rg
                .column(4)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("country is StringView");
            for i in 0..n {
                if country_col.is_null(i) {
                    decoded_countries.push(None);
                } else {
                    decoded_countries.push(Some(country_col.value(i).to_string()));
                }
            }
        }

        assert_eq!(row_groups_read, reader.row_group_count());
        assert_eq!(decoded_users.len(), total_rows);

        // Verify every decoded row matches the original events.
        // The segment stores rows in entity-sorted order — same order
        // as `events`.
        for (row_idx, event) in events.iter().enumerate() {
            let expected_user = match &event.entity {
                EntityId::String(s) => s.as_str(),
                _ => panic!("expected string entity"),
            };
            assert_eq!(
                decoded_users[row_idx], expected_user,
                "user_id mismatch at row {row_idx}"
            );
            assert_eq!(
                decoded_event_types[row_idx], event.event_type,
                "event_type mismatch at row {row_idx} (possible dictionary corruption)"
            );
            let expected_amount = match event.get("amount") {
                Some(PropertyValue::Int(v)) => Some(*v),
                _ => None,
            };
            assert_eq!(
                decoded_amounts[row_idx], expected_amount,
                "amount mismatch at row {row_idx}"
            );
            let expected_country = match event.get("country") {
                Some(PropertyValue::String(s)) => Some(s.clone()),
                _ => None,
            };
            assert_eq!(
                decoded_countries[row_idx], expected_country,
                "country mismatch at row {row_idx} (possible dictionary corruption)"
            );
        }
    }

    #[test]
    fn write_bucket_assigns_consecutive_seq_ids_via_database_counter() {
        let scratch = Scratch::new("e2e-seq-ids");
        let mut db = open_db_with_events_table(scratch.path());

        let events1 = vec![ev("a", 1, "click"), ev("b", 2, "click")];
        let events2 = vec![
            ev("c", 3, "click"),
            ev("d", 4, "click"),
            ev("e", 5, "click"),
        ];

        let batch_id = db.allocate_batch_id("events").unwrap();
        let (m1, m2) = {
            let mut writer = SegmentWriter::new(&mut db);
            let m1 = writer
                .write_bucket("events", 0, 0, batch_id, &events1)
                .unwrap();
            let m2 = writer
                .write_bucket("events", 0, 0, batch_id, &events2)
                .unwrap();
            (m1, m2)
        };

        // segment_id is monotonic across writes.
        assert_eq!(m1.segment_id + 1, m2.segment_id);
        // The per-table sequence_id counter advanced by total rows.
        assert_eq!(db.manifest().tables["events"].next_sequence_id, 5);
    }

    #[test]
    fn write_bucket_empty_input_errors() {
        let scratch = Scratch::new("e2e-empty");
        let mut db = open_db_with_events_table(scratch.path());
        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::new(&mut db);
        let err = writer
            .write_bucket("events", 0, 0, batch_id, &[])
            .expect_err("empty must error");
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn write_bucket_unknown_table_errors() {
        let scratch = Scratch::new("e2e-unknown");
        let mut db = open_db_with_events_table(scratch.path());
        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut writer = SegmentWriter::new(&mut db);
        let events = vec![ev("a", 1, "click")];
        let err = writer
            .write_bucket("missing", 0, 0, batch_id, &events)
            .expect_err("unknown table must error");
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn write_partitioner_emits_one_segment_per_bucket() {
        let scratch = Scratch::new("e2e-partitioner");
        let mut db = open_db_with_events_table(scratch.path());

        // shard_count = 1 so every entity lands in shard 0; one
        // window since all timestamps are at day 0.
        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut partitioner = Partitioner::new(1, 30, batch_id, 1 << 20).unwrap();
        partitioner.push_event(ev("alice", 1_000, "click")).unwrap();
        partitioner.push_event(ev("bob", 2_000, "view")).unwrap();

        let mut writer = SegmentWriter::new(&mut db);
        let metas = writer
            .write_partitioner("events", partitioner)
            .expect("write_partitioner");
        assert_eq!(metas.len(), 1, "one bucket → one segment");
        assert_eq!(metas[0].row_count, 2);
    }

    // ── Dictionary hoist unit test ──────────────────────────────────────────

    #[test]
    fn hoist_dictionary_chunk_rewrites_params_and_registers_segment_dict() {
        // Build a hand-crafted self-contained Dictionary chunk for an
        // Int column with 2 values [10, 20] and codes [0, 1, 0].
        let mut params = Vec::new();
        params.push(DICT_TYPE_TAG_INT); // type_tag
        params.push(1u8); // code_bit_width
        params.extend_from_slice(&2u32.to_le_bytes()); // cardinality
        params.extend_from_slice(&10_i64.to_le_bytes());
        params.extend_from_slice(&20_i64.to_le_bytes());

        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params,
            payload: vec![0b0000_0010_u8], // codes 0,1,0 packed at width 1
            row_count: 3,
        };

        let mut dicts: Vec<PreparedDictionary> = Vec::new();
        let hoisted = hoist_dictionary_chunk(&chunk, 7, &BqlType::Int, &mut dicts).expect("hoist");

        // params shrank to 5 bytes: dict_id (u32 LE) + code_bit_width (u8)
        assert_eq!(hoisted.params.len(), 5);
        let dict_id = u32::from_le_bytes(hoisted.params[..4].try_into().unwrap());
        assert_eq!(dict_id, 0); // first hoist → dict_id 0
        assert_eq!(hoisted.params[4], 1); // code_bit_width preserved
        assert_eq!(hoisted.payload, chunk.payload, "payload unchanged");

        // The segment-level dictionary captured the value bytes (no header).
        assert_eq!(dicts.len(), 1);
        assert_eq!(dicts[0].column_ordinal, 7);
        assert_eq!(dicts[0].cardinality, 2);
        assert_eq!(dicts[0].value_type, BqlType::Int);
        let mut expected = Vec::new();
        expected.extend_from_slice(&10_i64.to_le_bytes());
        expected.extend_from_slice(&20_i64.to_le_bytes());
        assert_eq!(dicts[0].bytes, expected);
    }

    #[test]
    fn hoist_dictionary_chunk_rejects_short_params() {
        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params: vec![0; 3],
            payload: Vec::new(),
            row_count: 0,
        };
        let mut dicts: Vec<PreparedDictionary> = Vec::new();
        let err = hoist_dictionary_chunk(&chunk, 0, &BqlType::Int, &mut dicts).unwrap_err();
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn hoist_dictionary_chunk_rejects_type_mismatch() {
        // type_tag says Int but caller passes BqlType::String.
        let mut params = Vec::new();
        params.push(DICT_TYPE_TAG_INT);
        params.push(1u8);
        params.extend_from_slice(&1u32.to_le_bytes());
        params.extend_from_slice(&42_i64.to_le_bytes());
        let chunk = EncodedChunk {
            encoding: EncodingType::Dictionary,
            params,
            payload: vec![0],
            row_count: 1,
        };
        let mut dicts: Vec<PreparedDictionary> = Vec::new();
        let err = hoist_dictionary_chunk(&chunk, 0, &BqlType::String, &mut dicts).unwrap_err();
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    // ── Zone-map merge ──────────────────────────────────────────────────────

    #[test]
    fn merge_extrema_takes_running_min_and_max() {
        let mut agg_min: Option<PropertyValue> = None;
        let mut agg_max: Option<PropertyValue> = None;
        merge_extrema(
            &mut agg_min,
            &mut agg_max,
            &Some(PropertyValue::Int(5)),
            &Some(PropertyValue::Int(10)),
        );
        assert_eq!(agg_min, Some(PropertyValue::Int(5)));
        assert_eq!(agg_max, Some(PropertyValue::Int(10)));

        merge_extrema(
            &mut agg_min,
            &mut agg_max,
            &Some(PropertyValue::Int(3)),
            &Some(PropertyValue::Int(7)),
        );
        assert_eq!(agg_min, Some(PropertyValue::Int(3)));
        assert_eq!(agg_max, Some(PropertyValue::Int(10)));

        merge_extrema(
            &mut agg_min,
            &mut agg_max,
            &Some(PropertyValue::Int(20)),
            &Some(PropertyValue::Int(30)),
        );
        assert_eq!(agg_min, Some(PropertyValue::Int(3)));
        assert_eq!(agg_max, Some(PropertyValue::Int(30)));
    }

    #[test]
    fn merge_extrema_ignores_all_null_chunks() {
        let mut agg_min = Some(PropertyValue::Int(5));
        let mut agg_max = Some(PropertyValue::Int(10));
        merge_extrema(&mut agg_min, &mut agg_max, &None, &None);
        assert_eq!(agg_min, Some(PropertyValue::Int(5)));
        assert_eq!(agg_max, Some(PropertyValue::Int(10)));
    }

    // ── compute_zone_extrema (direct unit tests per type) ───────────────────

    #[test]
    fn zone_extrema_int_extreme_values() {
        let arr = Arc::new(Int64Array::from(vec![i64::MAX, 0, i64::MIN, 5])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(arr.as_ref(), &BqlType::Int).unwrap();
        assert_eq!(lo, Some(PropertyValue::Int(i64::MIN)));
        assert_eq!(hi, Some(PropertyValue::Int(i64::MAX)));
    }

    #[test]
    fn zone_extrema_int_single_value() {
        let arr = Arc::new(Int64Array::from(vec![42_i64])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(arr.as_ref(), &BqlType::Int).unwrap();
        assert_eq!(lo, Some(PropertyValue::Int(42)));
        assert_eq!(hi, Some(PropertyValue::Int(42)));
    }

    #[test]
    fn zone_extrema_float_handles_negative_zero_and_inf() {
        let arr = Arc::new(Float64Array::from(vec![
            f64::INFINITY,
            -1.0,
            -0.0,
            0.0,
            f64::NEG_INFINITY,
        ])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(arr.as_ref(), &BqlType::Float).unwrap();
        // total_cmp puts -inf below every finite, +inf above every finite.
        if let Some(PropertyValue::Float(lo)) = lo {
            assert!(lo.is_infinite() && lo.is_sign_negative());
        } else {
            panic!("expected Float lo");
        }
        if let Some(PropertyValue::Float(hi)) = hi {
            assert!(hi.is_infinite() && hi.is_sign_positive());
        } else {
            panic!("expected Float hi");
        }
    }

    #[test]
    fn zone_extrema_string_lexicographic() {
        let arr =
            Arc::new(StringViewArray::from(vec!["banana", "apple", "cherry", ""])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(arr.as_ref(), &BqlType::String).unwrap();
        assert_eq!(lo, Some(PropertyValue::String("".into())));
        assert_eq!(hi, Some(PropertyValue::String("cherry".into())));
    }

    #[test]
    fn zone_extrema_bool_all_false_then_mixed() {
        let all_false = Arc::new(BooleanArray::from(vec![false, false, false])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(all_false.as_ref(), &BqlType::Bool).unwrap();
        assert_eq!(lo, Some(PropertyValue::Bool(false)));
        assert_eq!(hi, Some(PropertyValue::Bool(false)));

        let all_true = Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(all_true.as_ref(), &BqlType::Bool).unwrap();
        assert_eq!(lo, Some(PropertyValue::Bool(true)));
        assert_eq!(hi, Some(PropertyValue::Bool(true)));

        let mixed = Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(mixed.as_ref(), &BqlType::Bool).unwrap();
        assert_eq!(lo, Some(PropertyValue::Bool(false)));
        assert_eq!(hi, Some(PropertyValue::Bool(true)));
    }

    #[test]
    fn zone_extrema_timestamp() {
        let arr = Arc::new(
            TimestampNanosecondArray::from(vec![100_i64, 50, 200, 75]).with_timezone("UTC"),
        ) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(arr.as_ref(), &BqlType::Timestamp).unwrap();
        assert_eq!(lo, Some(PropertyValue::Timestamp(50)));
        assert_eq!(hi, Some(PropertyValue::Timestamp(200)));
    }

    #[test]
    fn zone_extrema_empty_array_yields_none() {
        let arr = Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef;
        let (lo, hi) = compute_zone_extrema(arr.as_ref(), &BqlType::Int).unwrap();
        assert_eq!(lo, None);
        assert_eq!(hi, None);
    }

    // ── build_null_bitmap_lsb_first ─────────────────────────────────────────

    #[test]
    fn null_bitmap_all_non_null_is_all_ones_within_row_count() {
        // Row count not aligned to a byte boundary — verify trailing
        // bits in the last byte are zero (writer §7 trailing-bits rule).
        let arr = Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4, 5])) as ArrayRef;
        let bitmap = build_null_bitmap_lsb_first(arr.as_ref());
        // ceil(5/8) = 1 byte
        assert_eq!(bitmap.len(), 1);
        // bits 0..5 are 1, bits 5..8 are 0 → 0b0001_1111 = 0x1F
        assert_eq!(bitmap[0], 0x1F);
    }

    #[test]
    fn null_bitmap_mixed_nulls_lsb_first() {
        // Build a nullable Int array where rows 0, 2, 4 are non-null
        // and rows 1, 3 are null.
        let arr = Arc::new(Int64Array::from(vec![
            Some(10_i64),
            None,
            Some(20),
            None,
            Some(30),
        ])) as ArrayRef;
        let bitmap = build_null_bitmap_lsb_first(arr.as_ref());
        assert_eq!(bitmap.len(), 1);
        // LSB-first: bit0=1, bit1=0, bit2=1, bit3=0, bit4=1, bit5..7=0 → 0b0001_0101 = 0x15
        assert_eq!(bitmap[0], 0x15);
    }

    #[test]
    fn null_bitmap_two_byte_array() {
        // 10 rows: rows 0..8 non-null, rows 8..10 null.
        let arr = Arc::new(Int64Array::from(vec![
            Some(0_i64),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            None,
            None,
        ])) as ArrayRef;
        let bitmap = build_null_bitmap_lsb_first(arr.as_ref());
        assert_eq!(bitmap.len(), 2);
        assert_eq!(bitmap[0], 0xFF, "first byte all ones");
        assert_eq!(bitmap[1], 0x00, "second byte all zeros (rows 8,9 null)");
    }

    #[test]
    fn null_bitmap_empty_array() {
        let arr = Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef;
        let bitmap = build_null_bitmap_lsb_first(arr.as_ref());
        assert!(bitmap.is_empty());
    }

    // ── write_partitioner across multiple buckets ───────────────────────────

    #[test]
    fn write_partitioner_emits_one_segment_per_window_and_shard() {
        let scratch = Scratch::new("e2e-multi-bucket");
        let mut db = open_db_with_events_table(scratch.path());

        // shard_count = 4 with multiple distinct entities so the hash
        // distributes them across shards. Use day-aligned timestamps
        // in two different windows so the partitioner buckets them
        // into separate (window, shard) cells.
        let day_ns = 86_400 * 1_000_000_000_i64;
        let batch_id = db.allocate_batch_id("events").unwrap();
        let mut partitioner = Partitioner::new(4, 30, batch_id, 1 << 20).unwrap();
        // Window 0 (days 0..30): 4 distinct entities at day 5.
        for n in 0..4 {
            partitioner
                .push_event(ev(&format!("u{n}"), 5 * day_ns + n, "click"))
                .unwrap();
        }
        // Window 30 (days 30..60): 4 distinct entities at day 35.
        for n in 0..4 {
            partitioner
                .push_event(ev(&format!("u{n}"), 35 * day_ns + n, "view"))
                .unwrap();
        }

        let bucket_count = partitioner.bucket_count();
        assert!(
            bucket_count >= 2,
            "expected ≥2 distinct (window, shard) buckets, got {bucket_count}"
        );

        let mut writer = SegmentWriter::new(&mut db);
        let metas = writer
            .write_partitioner("events", partitioner)
            .expect("write_partitioner");
        assert_eq!(metas.len(), bucket_count);

        // Total rows across all returned metas equals the total
        // events pushed (8). Per-bucket row counts are preserved.
        let total_rows: u64 = metas.iter().map(|m| m.row_count).sum();
        assert_eq!(total_rows, 8);

        // Every emitted segment shares the same batch_id.
        for meta in &metas {
            assert_eq!(meta.batch_id, batch_id);
        }

        // Segment IDs are monotonic in drain order.
        for w in metas.windows(2) {
            assert!(w[0].segment_id < w[1].segment_id);
        }
    }
}
