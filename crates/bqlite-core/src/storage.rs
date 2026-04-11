//! `SegmentReader` — the v0 storage API consumed by scan operators.
//!
//! This module defines the contract between the storage layer
//! (`bqlite-storage`, which implements `SegmentReader` against real
//! segment files) and the scan operator (`bqlite-operators`, which
//! drives the reader through its trait). Both crates sit above
//! `bqlite-core` in the dependency graph — placing the trait here
//! avoids a cycle and lets in-memory fakes in test code implement it
//! without depending on the real storage crate.
//!
//! See [`docs/design/storage/reader-trait.md`](../../../docs/design/storage/reader-trait.md)
//! for the full design note — scope, crate placement, lifecycle,
//! error semantics, and how the Wave 1 surface projects onto the
//! richer storage-format.md §8–§11 model.
//!
//! ## Five pushdown hooks
//!
//! The task spec calls out five capabilities the trait must cover;
//! each maps to one of the types or methods in this module:
//!
//! 1. **Segment enumeration** — [`SegmentReader::segments`] yields
//!    lazy [`SegmentHandle`]s against the query snapshot.
//! 2. **Column projection** — [`ColumnProjection`] passed to
//!    [`SegmentReader::open_segment`] names the columns to decode.
//! 3. **Row-group iteration** — [`SegmentScan::next_row_group`]
//!    streams one `RecordBatch` per row-group.
//! 4. **Zone-map access** — [`SegmentScan::row_group_zone_maps`]
//!    exposes per-column min/max metadata for pruning.
//! 5. **Predicate pushdown** — an optional `Arc<dyn Predicate>` the
//!    reader may consult for zone-map filtering. The real predicate
//!    IR is a Wave 2 [DESIGN] task; [`Predicate`] here is a narrow
//!    one-method hook that extension is additive on.
//!
//! ## v0 scope exclusions
//!
//! - **Dictionary filter bitsets** (storage-format.md §8.2) — land
//!   with the encoding layer in a later wave, exposed as a
//!   segment-local API alongside `SegmentScan`, not as a trait
//!   method.
//! - **K-way merge across shards/windows** — sits above the
//!   per-segment reader and is a scan-operator concern.
//! - **Async I/O** — the trait is synchronous; an async variant may
//!   land later if single-core overlap becomes load-bearing.

use std::collections::HashMap;
use std::sync::Arc;

use ::arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::property::PropertyValue;
use crate::schema::TableSchema;

// ─────────────────────────────────────────────────────────────────────────────
// Supporting types
// ─────────────────────────────────────────────────────────────────────────────

/// A handle to a segment visible in the reader's query snapshot.
///
/// Cheap to clone — holds only metadata (five integer fields), no
/// pointers into the segment file. Scan operators may keep many
/// handles alive at once (e.g. one per shard per window when a
/// later-wave k-way merge lands) without worrying about allocation
/// cost. A handle that outlives its [`SegmentReader`] is useless but
/// not unsafe: passing it to [`SegmentReader::open_segment`] on a
/// different reader returns `BqliteError::Execution`.
///
/// # What this handle does *not* carry
///
/// The full `SegmentMeta` in the manifest (storage-format.md §12.3)
/// carries additional fields that are deliberately omitted from the
/// Wave 1 handle:
///
/// - **`level`** — compaction tier. Scan operators do not need it;
///   the reader decides pruning order internally.
/// - **`ts_range` / `entity_range`** — segment-level zone maps.
///   Wave 1 scan operators consult zone maps at the *row-group*
///   granularity through [`SegmentScan::row_group_zone_maps`], so
///   hoisting a segment-level version onto the handle is premature.
///   Adding them later is additive.
/// - **`batch_id` / `created_at` / `byte_size`** — useful for
///   compaction and debugging, not for the read path.
///
/// Keeping the handle minimal means scan operators never accidentally
/// branch on metadata that was not needed for the query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentHandle {
    /// Monotonically increasing segment identifier assigned from the
    /// manifest counter (storage-format.md §5.2 filename derivation,
    /// §12.3 `SegmentMeta.segment_id`). Unique across the database.
    pub segment_id: u64,
    /// Shard index within the database's fixed shard count
    /// (storage-format.md §5.1).
    pub shard_id: u32,
    /// Identifier of the time window this segment belongs to
    /// (storage-format.md §4.1 for the partition model,
    /// §5.2 `w_<days-since-epoch>` for the naming). `0` is legal and
    /// is the value the Wave 1 stub uses — window partitioning lands
    /// in a later wave.
    pub window_id: u64,
    /// Total rows in the segment across all row-groups.
    pub row_count: u64,
    /// Schema version the segment was written against
    /// (type-system.md §5, storage-format.md §6.4). Used by the
    /// reader to backfill columns added after the segment was
    /// written.
    pub schema_version: u32,
}

/// Per-row-group, per-column zone map (storage-format.md §11.1).
///
/// Stored inline in the segment footer per storage-format.md §9.4.
/// Readers load these once when a segment is opened and expose them
/// via [`SegmentScan::row_group_zone_maps`] for predicate pruning.
///
/// `min` and `max` use [`PropertyValue`], not Arrow scalars, so the
/// predicate's `accepts_zone` check can run without touching Arrow
/// — the readers convert from Arrow once at load time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ZoneMap {
    /// Column minimum, or `None` if the row-group is all-null for
    /// this column.
    pub min: Option<PropertyValue>,
    /// Column maximum, or `None` if the row-group is all-null.
    pub max: Option<PropertyValue>,
    /// Number of null values in the row-group for this column.
    pub null_count: u64,
    /// Number of rows in the row-group (the same value is also
    /// available from the scan; duplicated here so a `ZoneMap` is
    /// self-contained for pruning loops).
    pub row_count: u64,
}

impl ZoneMap {
    /// Construct a zone map for a row-group whose column has every
    /// row null. Both `min` and `max` are `None`.
    pub fn all_null(row_count: u64) -> Self {
        Self {
            min: None,
            max: None,
            null_count: row_count,
            row_count,
        }
    }

    /// True when no rows in this column survive a pruning filter —
    /// i.e. there are zero non-null rows (so no value can satisfy
    /// any non-null predicate).
    pub fn is_all_null(&self) -> bool {
        self.row_count != 0 && self.null_count == self.row_count
    }
}

/// Column projection hint passed to [`SegmentReader::open_segment`].
///
/// An empty projection is interpreted as "all declared columns plus
/// the `__seq_id` / `__batch_id` system columns, in table-schema
/// order". This keeps the common no-projection-pruning case a
/// single allocation cheaper than carrying an explicit `::All`
/// variant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnProjection {
    names: Vec<String>,
}

impl ColumnProjection {
    /// Projection naming every column in table-schema order —
    /// declared columns followed by the implicit `__seq_id` and
    /// `__batch_id` system columns. Matches the shape of
    /// `OperatorSchema::from_table(&table)`.
    ///
    /// Equivalent to `ColumnProjection::default()` — provided for
    /// intent clarity at call sites. Callers do **not** need to
    /// spell the system columns explicitly when they want them; use
    /// `all()` (or the default) and the reader includes them.
    pub fn all() -> Self {
        Self::default()
    }

    /// Projection naming an explicit column list in the desired
    /// output order. Column names are copied; duplicates are
    /// preserved verbatim (validation of the projection against the
    /// reader's schema happens inside `open_segment`).
    pub fn with_columns<I, S>(columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            names: columns.into_iter().map(Into::into).collect(),
        }
    }

    /// True if this projection means "every column".
    ///
    /// Readers check this first and fall through to a fast-path when
    /// no projection pruning is requested.
    pub fn is_all(&self) -> bool {
        self.names.is_empty()
    }

    /// Column names in projection order. Empty when [`Self::is_all`].
    pub fn columns(&self) -> &[String] {
        &self.names
    }

    /// Number of explicitly named columns. Zero when
    /// [`Self::is_all`].
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// True when no columns are explicitly named — which in this
    /// type means "project every column", not "no columns".
    /// Equivalent to [`Self::is_all`]; provided to satisfy Clippy's
    /// `len_without_is_empty` lint. Callers checking whether a
    /// projection selects zero columns should use `len() == 0`
    /// against a projection built with [`Self::with_columns`]; that
    /// shape is legal but is an application-level error for scan.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate
// ─────────────────────────────────────────────────────────────────────────────

/// Predicate pushdown hook consumed by [`SegmentReader::open_segment`].
///
/// The Wave 1 surface is deliberately one method wide. A real
/// predicate IR — with whole-batch evaluation, dictionary-aware
/// filtering, and fusion with the scan operator — is a Wave 2
/// `[DESIGN]` task. Extending this trait then is additive: readers
/// that only honour `accepts_zone` keep working, readers that opt
/// into the richer surface get more pruning.
///
/// Implementations are held behind `Arc` so many `SegmentScan`s can
/// share a single predicate without per-scan cloning.
pub trait Predicate: Send + Sync + std::fmt::Debug {
    /// True if the predicate **might** accept at least one row in a
    /// column range described by `zone`.
    ///
    /// Returning `false` tells the reader to skip the range entirely
    /// — the scan operator never sees those rows. Conservative
    /// implementations may always return `true`; this disables
    /// zone-map pruning but is always safe.
    ///
    /// `column` is the fully-qualified column name from the
    /// projection, not a position. Readers call this method once per
    /// row-group per referenced column; implementations must be
    /// cheap.
    fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool;
}

// ─────────────────────────────────────────────────────────────────────────────
// SegmentScan
// ─────────────────────────────────────────────────────────────────────────────

/// Streaming read over a single segment's row-groups.
///
/// Obtained from [`SegmentReader::open_segment`] and iterated to
/// completion (or dropped early). Dropping releases any OS resources
/// the scan holds — memory maps, file handles, decompression state.
///
/// One `SegmentScan` corresponds to one segment file. A scan
/// operator reads many segments by calling
/// [`SegmentReader::open_segment`] repeatedly; each call returns a
/// fresh `SegmentScan`.
pub trait SegmentScan: Send {
    /// Number of row-groups in this segment.
    ///
    /// Known before iteration starts (row-group count is in the
    /// segment footer per storage-format.md §9.2). Scan operators
    /// use it to size per-row-group scratch buffers and to drive
    /// zone-map pruning loops.
    fn row_group_count(&self) -> usize;

    /// Per-column zone maps for row-group `idx`.
    ///
    /// Column presence is not guaranteed — zone maps are absent
    /// when a row-group is all-null for a column, when the column
    /// was added after the segment was written (schema evolution),
    /// or when the storage layer chose not to maintain zone maps
    /// for the column. Callers check presence per column.
    ///
    /// Returning `Ok(HashMap::new())` is legal and means "no zone
    /// maps for this row-group" — the scan operator must then
    /// assume the row-group is not prunable and read it.
    ///
    /// Implementations should not load the row-group's data to
    /// answer this call; it is a metadata-only read off the footer.
    ///
    /// # Return type
    ///
    /// Returns an owned `HashMap` rather than `&HashMap`. The scan
    /// operator calls this once per row-group before deciding
    /// whether to decode it — the clone is amortized against the
    /// row-group decode cost (64K-row decompress + decode) and keeps
    /// stubs that compute zone maps lazily from having to cache a
    /// materialized map internally.
    fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>>;

    /// Yield the next row-group as a `RecordBatch`, or `Ok(None)`
    /// when the segment is exhausted.
    ///
    /// Batch shape:
    ///
    /// - Column order matches the [`ColumnProjection`] passed to
    ///   [`SegmentReader::open_segment`] — or table-schema order
    ///   when the projection was [`ColumnProjection::all`].
    /// - Each returned batch contains exactly one row-group worth of
    ///   rows (no concatenation, no splitting).
    /// - Nullable columns carry Arrow null bitmaps; non-nullable
    ///   columns do not (execution-model.md §3.7).
    /// - An empty batch is legal for row-groups the predicate fully
    ///   pruned. Consumers must tolerate zero-row batches.
    /// - Implementations may return `Ok(None)` early if they know
    ///   every remaining row-group is pruned.
    ///
    /// After the first `Ok(None)`, subsequent calls must continue
    /// returning `Ok(None)` without side effects.
    fn next_row_group(&mut self) -> Result<Option<RecordBatch>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// SegmentReader
// ─────────────────────────────────────────────────────────────────────────────

/// Read-side API for a table's segments — the entry point scan
/// operators use to read data out of the storage layer.
///
/// A `SegmentReader` is a per-query, per-table snapshot of the
/// manifest's live segment inventory (storage-format.md §7.6 query
/// snapshots). The engine obtains one from `Database` at query
/// start, hands it to the scan operator, and drops it when the
/// query completes.
///
/// Iteration pattern:
///
/// ```text
/// let reader: Box<dyn SegmentReader> = db.segment_reader("events")?;
/// for handle in reader.segments() {
///     let handle = handle?;
///     let mut scan = reader.open_segment(
///         &handle,
///         &ColumnProjection::all(),
///         None, // no predicate pushdown
///     )?;
///     while let Some(batch) = scan.next_row_group()? {
///         // process batch
///     }
/// }
/// ```
///
/// `Send + Sync` so a single reader can be shared across scan
/// threads in a later-wave parallel execution model. Implementations
/// that hold non-thread-safe state must serialize access internally.
pub trait SegmentReader: Send + Sync {
    /// The table schema this reader produces rows against.
    ///
    /// This is the *current* schema from the manifest
    /// (storage-format.md §12.2), not any individual segment's
    /// write-time schema. Implementations fill columns missing from
    /// older segments with NULL or the column's default value
    /// before returning a `RecordBatch`.
    fn schema(&self) -> &TableSchema;

    /// Enumerate segments visible to this reader's snapshot.
    ///
    /// Iteration order is implementation-defined but stable for the
    /// lifetime of the reader. Returning an iterator (rather than a
    /// `Vec`) lets large manifests stream lazily and keeps the
    /// Wave 1 stub trivial: the empty-iterator case is
    /// `Box::new(std::iter::empty())`.
    ///
    /// Errors yielded mid-iteration abort the query — the scan
    /// operator propagates them up without opening further
    /// segments.
    fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_>;

    /// Open a streaming scan over a segment.
    ///
    /// # Arguments
    ///
    /// - `handle` must be a value returned by [`Self::segments`] on
    ///   the same reader; opening a stale, unknown, or foreign
    ///   handle returns `BqliteError::Execution`.
    /// - `projection` names the columns to decode. `is_all()` means
    ///   "every declared column plus the `__seq_id` / `__batch_id`
    ///   system columns, in table-schema order". Any named column
    ///   that is not present in [`Self::schema`] returns
    ///   `BqliteError::Schema`.
    /// - `predicate` is an optional pushdown hint. Passing `None`
    ///   disables zone-map pruning for this scan; passing
    ///   `Some(pred)` lets the reader call `pred.accepts_zone(…)`
    ///   to skip row-groups.
    ///
    /// The returned `SegmentScan` owns the per-segment OS resources
    /// (a memory map, a file handle, or an in-memory buffer in the
    /// Wave 1 stub). Dropping it releases those resources; there is
    /// no explicit `close()`.
    fn open_segment(
        &self,
        handle: &SegmentHandle,
        projection: &ColumnProjection,
        predicate: Option<Arc<dyn Predicate>>,
    ) -> Result<Box<dyn SegmentScan>>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use ::arrow::array::{Int64Array, RecordBatch as ArrowRecordBatch, StringArray};
    use ::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use crate::error::BqliteError;
    use crate::property::BqlType;
    use crate::schema::ColumnDef;

    // ── SegmentHandle ────────────────────────────────────────────────────────

    #[test]
    fn segment_handle_is_clone_eq_hash() {
        let a = SegmentHandle {
            segment_id: 1,
            shard_id: 2,
            window_id: 3,
            row_count: 4,
            schema_version: 5,
        };
        let b = a.clone();
        assert_eq!(a, b);
        // Hashable — used as map keys by scan operators.
        let mut set = std::collections::HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    // ── ZoneMap ──────────────────────────────────────────────────────────────

    #[test]
    fn zone_map_default_is_empty() {
        let z = ZoneMap::default();
        assert_eq!(z.min, None);
        assert_eq!(z.max, None);
        assert_eq!(z.null_count, 0);
        assert_eq!(z.row_count, 0);
        assert!(!z.is_all_null()); // zero-row is not "all null"
    }

    #[test]
    fn zone_map_all_null_constructor() {
        let z = ZoneMap::all_null(64);
        assert_eq!(z.min, None);
        assert_eq!(z.max, None);
        assert_eq!(z.null_count, 64);
        assert_eq!(z.row_count, 64);
        assert!(z.is_all_null());
    }

    #[test]
    fn zone_map_is_all_null_requires_matching_counts() {
        let mostly = ZoneMap {
            min: Some(PropertyValue::Int(1)),
            max: Some(PropertyValue::Int(10)),
            null_count: 5,
            row_count: 10,
        };
        assert!(!mostly.is_all_null());
    }

    // ── ColumnProjection ─────────────────────────────────────────────────────

    #[test]
    fn column_projection_default_is_all() {
        let p = ColumnProjection::default();
        assert!(p.is_all());
        assert!(p.is_empty());
        assert_eq!(p.len(), 0);
        assert!(p.columns().is_empty());
    }

    #[test]
    fn column_projection_all_matches_default() {
        assert_eq!(ColumnProjection::all(), ColumnProjection::default());
    }

    #[test]
    fn column_projection_with_columns_preserves_order_and_duplicates() {
        let p = ColumnProjection::with_columns(["amount", "ts", "amount"]);
        assert!(!p.is_all());
        assert_eq!(p.len(), 3);
        assert_eq!(p.columns(), &["amount", "ts", "amount"]);
    }

    #[test]
    fn column_projection_with_columns_accepts_owned_and_borrowed_strings() {
        let _owned: ColumnProjection =
            ColumnProjection::with_columns(vec![String::from("a"), String::from("b")]);
        let _borrowed: ColumnProjection = ColumnProjection::with_columns(["a", "b"]);
    }

    // ── Predicate (object-safety + zone filter) ──────────────────────────────

    #[derive(Debug)]
    struct EventTypePrefixPredicate {
        prefix: &'static str,
    }

    impl Predicate for EventTypePrefixPredicate {
        fn accepts_zone(&self, column: &str, zone: &ZoneMap) -> bool {
            if column != "event_type" {
                // Predicate only talks about event_type; every other
                // column passes through.
                return true;
            }
            // Accept only if *some* string in [min, max] could start
            // with our prefix. A naive lexicographic check: the
            // maximum must be >= prefix.
            match &zone.max {
                Some(PropertyValue::String(s)) => s.as_str() >= self.prefix,
                _ => true,
            }
        }
    }

    #[test]
    fn predicate_is_object_safe() {
        let p: Arc<dyn Predicate> = Arc::new(EventTypePrefixPredicate { prefix: "p" });
        let yes = ZoneMap {
            min: Some(PropertyValue::String("apple".into())),
            max: Some(PropertyValue::String("zebra".into())),
            null_count: 0,
            row_count: 64,
        };
        let no = ZoneMap {
            min: Some(PropertyValue::String("apple".into())),
            max: Some(PropertyValue::String("orange".into())),
            null_count: 0,
            row_count: 64,
        };
        assert!(p.accepts_zone("event_type", &yes));
        assert!(!p.accepts_zone("event_type", &no));
        // Non-target columns pass through unconditionally.
        assert!(p.accepts_zone("other", &no));
    }

    // ── In-memory fake reader ────────────────────────────────────────────────
    //
    // Exercises the trait objects end-to-end to prove the contracts
    // compose: object safety, Box<dyn Iterator>, Arc<dyn Predicate>,
    // Box<dyn SegmentScan>. Later waves add real readers in
    // bqlite-storage; Wave 1 relies on this fake to validate the
    // trait surface before TASK-116 / TASK-117 land.

    struct FakeReader {
        schema: TableSchema,
        handles: Vec<SegmentHandle>,
    }

    struct FakeScan {
        batches: Vec<RecordBatch>,
        zone_maps: Vec<HashMap<String, ZoneMap>>,
        position: usize,
    }

    impl SegmentReader for FakeReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            Box::new(self.handles.iter().cloned().map(Ok))
        }

        fn open_segment(
            &self,
            handle: &SegmentHandle,
            _projection: &ColumnProjection,
            _predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            if !self.handles.iter().any(|h| h == handle) {
                return Err(BqliteError::Execution(format!(
                    "unknown segment handle {handle:?}"
                )));
            }
            // Build one row-group with a tiny RecordBatch matching
            // the minimal schema columns (entity_id, ts, event_type).
            let arrow_schema = Arc::new(ArrowSchema::new(vec![
                Field::new("entity_id", DataType::Utf8, false),
                Field::new("ts", DataType::Int64, false),
                Field::new("event_type", DataType::Utf8, false),
            ]));
            let batch = ArrowRecordBatch::try_new(
                arrow_schema,
                vec![
                    Arc::new(StringArray::from(vec!["u1", "u1", "u2"])),
                    Arc::new(Int64Array::from(vec![100_i64, 200, 300])),
                    Arc::new(StringArray::from(vec!["signup", "purchase", "signup"])),
                ],
            )
            .expect("valid RecordBatch");
            let mut zone_maps = HashMap::new();
            zone_maps.insert(
                "entity_id".to_string(),
                ZoneMap {
                    min: Some(PropertyValue::String("u1".into())),
                    max: Some(PropertyValue::String("u2".into())),
                    null_count: 0,
                    row_count: 3,
                },
            );
            zone_maps.insert(
                "ts".to_string(),
                ZoneMap {
                    min: Some(PropertyValue::Timestamp(100)),
                    max: Some(PropertyValue::Timestamp(300)),
                    null_count: 0,
                    row_count: 3,
                },
            );
            Ok(Box::new(FakeScan {
                batches: vec![batch],
                zone_maps: vec![zone_maps],
                position: 0,
            }))
        }
    }

    impl SegmentScan for FakeScan {
        fn row_group_count(&self) -> usize {
            self.batches.len()
        }

        fn row_group_zone_maps(&self, idx: usize) -> Result<HashMap<String, ZoneMap>> {
            self.zone_maps
                .get(idx)
                .cloned()
                .ok_or_else(|| BqliteError::Execution(format!("row-group {idx} out of range")))
        }

        fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
            if self.position >= self.batches.len() {
                return Ok(None);
            }
            let batch = self.batches[self.position].clone();
            self.position += 1;
            Ok(Some(batch))
        }
    }

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

    #[test]
    fn fake_reader_iterates_segments() {
        let reader = FakeReader {
            schema: minimal_schema(),
            handles: vec![
                SegmentHandle {
                    segment_id: 1,
                    shard_id: 0,
                    window_id: 0,
                    row_count: 3,
                    schema_version: 0,
                },
                SegmentHandle {
                    segment_id: 2,
                    shard_id: 0,
                    window_id: 0,
                    row_count: 3,
                    schema_version: 0,
                },
            ],
        };
        let segs: Vec<SegmentHandle> = reader.segments().map(|r| r.unwrap()).collect();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].segment_id, 1);
        assert_eq!(segs[1].segment_id, 2);
    }

    #[test]
    fn fake_reader_opens_and_drives_scan_to_exhaustion() {
        let reader = FakeReader {
            schema: minimal_schema(),
            handles: vec![SegmentHandle {
                segment_id: 1,
                shard_id: 0,
                window_id: 0,
                row_count: 3,
                schema_version: 0,
            }],
        };
        let handle = reader.segments().next().unwrap().unwrap();
        let mut scan = reader
            .open_segment(&handle, &ColumnProjection::all(), None)
            .unwrap();
        assert_eq!(scan.row_group_count(), 1);
        let zmaps = scan.row_group_zone_maps(0).unwrap();
        assert!(zmaps.contains_key("entity_id"));
        assert!(zmaps.contains_key("ts"));
        // First row-group has data.
        let batch = scan.next_row_group().unwrap().expect("one batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
        // Exhausted.
        assert!(scan.next_row_group().unwrap().is_none());
        // Idempotent after exhaustion.
        assert!(scan.next_row_group().unwrap().is_none());
    }

    #[test]
    fn fake_reader_rejects_unknown_handle() {
        let reader = FakeReader {
            schema: minimal_schema(),
            handles: vec![SegmentHandle {
                segment_id: 1,
                shard_id: 0,
                window_id: 0,
                row_count: 3,
                schema_version: 0,
            }],
        };
        let stale = SegmentHandle {
            segment_id: 999,
            shard_id: 0,
            window_id: 0,
            row_count: 0,
            schema_version: 0,
        };
        let err = match reader.open_segment(&stale, &ColumnProjection::all(), None) {
            Err(e) => e,
            Ok(_) => panic!("expected stale handle to be rejected"),
        };
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    #[test]
    fn reader_rejects_projection_with_unknown_column() {
        // A reader that validates the projection against its schema
        // and returns `Schema` on mismatch. Exercises the trait
        // contract in §4.3 / §5 of the design doc that real readers
        // return `BqliteError::Schema` when the projection names a
        // column that does not exist.
        struct ValidatingReader {
            schema: TableSchema,
            handles: Vec<SegmentHandle>,
        }
        impl SegmentReader for ValidatingReader {
            fn schema(&self) -> &TableSchema {
                &self.schema
            }
            fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
                Box::new(self.handles.iter().cloned().map(Ok))
            }
            fn open_segment(
                &self,
                handle: &SegmentHandle,
                projection: &ColumnProjection,
                _predicate: Option<Arc<dyn Predicate>>,
            ) -> Result<Box<dyn SegmentScan>> {
                if !self.handles.iter().any(|h| h == handle) {
                    return Err(BqliteError::Execution(format!(
                        "unknown segment handle {handle:?}"
                    )));
                }
                for name in projection.columns() {
                    if self.schema.column(name).is_none() {
                        return Err(BqliteError::Schema(format!(
                            "projection column `{name}` not in table `{}`",
                            self.schema.name()
                        )));
                    }
                }
                // Empty scan — we only need to prove the rejection
                // path fires before any row-group work.
                Ok(Box::new(FakeScan {
                    batches: Vec::new(),
                    zone_maps: Vec::new(),
                    position: 0,
                }))
            }
        }

        let reader = ValidatingReader {
            schema: minimal_schema(),
            handles: vec![SegmentHandle {
                segment_id: 1,
                shard_id: 0,
                window_id: 0,
                row_count: 0,
                schema_version: 0,
            }],
        };
        let handle = reader.segments().next().unwrap().unwrap();
        // Valid projection passes through.
        reader
            .open_segment(
                &handle,
                &ColumnProjection::with_columns(["entity_id", "ts"]),
                None,
            )
            .expect("valid projection should succeed");
        // Unknown column is rejected with Schema error.
        let bad = ColumnProjection::with_columns(["not_a_column"]);
        let err = match reader.open_segment(&handle, &bad, None) {
            Err(e) => e,
            Ok(_) => panic!("expected projection rejection"),
        };
        let msg = format!("{err}");
        assert!(
            matches!(err, BqliteError::Schema(_)) && msg.contains("not_a_column"),
            "{msg}"
        );
    }

    #[test]
    fn empty_reader_yields_no_segments() {
        // The shape TASK-116's storage stub returns: a reader with
        // no segments. Validates the trait works end-to-end in the
        // empty case that the Wave 1 smoke test relies on.
        struct EmptyReader {
            schema: TableSchema,
        }
        impl SegmentReader for EmptyReader {
            fn schema(&self) -> &TableSchema {
                &self.schema
            }
            fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
                Box::new(std::iter::empty())
            }
            fn open_segment(
                &self,
                _handle: &SegmentHandle,
                _projection: &ColumnProjection,
                _predicate: Option<Arc<dyn Predicate>>,
            ) -> Result<Box<dyn SegmentScan>> {
                Err(BqliteError::Execution(
                    "empty reader has no segments".into(),
                ))
            }
        }
        let reader: Box<dyn SegmentReader> = Box::new(EmptyReader {
            schema: minimal_schema(),
        });
        assert_eq!(reader.schema().name(), "events");
        let segs: Vec<_> = reader.segments().collect();
        assert_eq!(segs.len(), 0);
    }

    // ── Trait object bounds ──────────────────────────────────────────────────

    #[test]
    fn trait_objects_are_send_sync() {
        // These asserts pin the `Send + Sync` bounds at compile time —
        // removing them from the trait definition fails this test.
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn SegmentReader>();
        assert_send_sync::<dyn Predicate>();
        // SegmentScan is Send-only (not Sync) — scan state is
        // single-threaded. Pin just Send to catch regressions.
        fn assert_send<T: Send + ?Sized>() {}
        assert_send::<dyn SegmentScan>();
    }

    // ── Interior reference for predicate-shared readers ──────────────────────

    #[test]
    fn predicate_can_be_arced_and_shared() {
        // Validates that a single predicate can be passed to many
        // open_segment calls without cloning the inner value —
        // important for zero-alloc hot paths.
        #[derive(Debug)]
        struct CountingPredicate(AtomicUsize);
        impl Predicate for CountingPredicate {
            fn accepts_zone(&self, _column: &str, _zone: &ZoneMap) -> bool {
                self.0.fetch_add(1, Ordering::Relaxed);
                true
            }
        }
        let p: Arc<dyn Predicate> = Arc::new(CountingPredicate(AtomicUsize::new(0)));
        let z = ZoneMap::default();
        assert!(p.accepts_zone("a", &z));
        assert!(p.accepts_zone("b", &z));
        let other: Arc<dyn Predicate> = Arc::clone(&p);
        assert!(other.accepts_zone("c", &z));
    }
}
