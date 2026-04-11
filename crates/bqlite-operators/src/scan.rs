//! Wave 1 stub implementation of the scan operator.
//!
//! `ScanOperator` is the leaf of every v0 execution tree: it drives a
//! [`SegmentReader`] to completion and emits one [`RecordBatch`] per
//! row-group. The scan holds a per-query snapshot of the manifest
//! through the reader (see `docs/design/storage/reader-trait.md`),
//! opens each segment in turn, and forwards every row-group the
//! segment yields.
//!
//! ## Scope
//!
//! This is the Wave 1 stub called out by TASK-117: it actually drives
//! `SegmentReader::segments()` and `SegmentScan::next_row_group()` so
//! downstream planner + engine work has real types to wire to. It is
//! **not** the production scan operator. In particular:
//!
//! - Projection pruning is not implemented — the stub only supports
//!   [`ColumnProjection::all`] and the output schema is
//!   [`OperatorSchema::from_table`] of the reader's schema. Passing a
//!   non-all projection is accepted and forwarded to the reader, but
//!   the scan's `output_schema` still reflects the full table shape;
//!   TASK-2xx (projection pruning) will rebuild the schema from the
//!   projection and drop unreferenced columns.
//! - No k-way merge across segments — segments are read sequentially
//!   per `reader-trait.md §4`. K-way merge lands with the Wave 2
//!   segment format.
//! - No zone-map driven row-group skipping — the scan passes the
//!   optional predicate through to `open_segment` but does not itself
//!   consult `SegmentScan::row_group_zone_maps`.
//! - No metrics instrumentation (TASK-112's `Metrics` trait is wired
//!   in later waves).
//!
//! ## Lifecycle
//!
//! Follows the [`PhysicalOperator`] contract:
//!
//! 1. `open()` materializes the segment handle list from the reader's
//!    snapshot. Any error yielded by the reader's iterator surfaces
//!    from `open()` so that failure is visible before the first row
//!    flows.
//! 2. `next_batch()` walks the handles, opening each segment lazily
//!    and draining it row-group by row-group. Cancellation is
//!    observed at the top of every call via the shared
//!    [`CancellationToken`].
//! 3. `close()` drops the current scan and the handle list. It is
//!    idempotent — the engine may call it during tear-down even if
//!    `open()` failed or `next_batch()` was never called.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use bqlite_core::{
    BqliteError, ColumnProjection, OperatorSchema, Predicate, Result, SegmentHandle, SegmentReader,
    SegmentScan,
};

use crate::operator::{CancellationToken, PhysicalOperator};

/// Pull-based scan over a [`SegmentReader`].
///
/// Constructed with the reader, a column projection, an optional
/// predicate, and a cancellation token. Wave 1 only supports
/// [`ColumnProjection::all`] for the purposes of `output_schema`;
/// non-all projections are still forwarded to the reader (so the test
/// fixture can exercise the plumbing) but do not reshape the output
/// schema. Projection-driven schema narrowing is a later wave's task.
pub struct ScanOperator {
    reader: Arc<dyn SegmentReader>,
    projection: ColumnProjection,
    predicate: Option<Arc<dyn Predicate>>,
    cancel: CancellationToken,
    output_schema: OperatorSchema,
    /// Segment handles materialized from `reader.segments()` at
    /// `open()` time. `None` until `open()` has been called.
    segments: Option<Vec<SegmentHandle>>,
    /// Index of the next segment to open in `segments`.
    next_segment: usize,
    /// Active scan over the current segment, if any. Replaced when
    /// the current scan is exhausted and there is another segment.
    current: Option<Box<dyn SegmentScan>>,
    /// Latches to `true` on the first `Ok(None)` and keeps subsequent
    /// calls cheap and side-effect-free.
    exhausted: bool,
}

impl ScanOperator {
    /// Build a scan over every segment the reader enumerates.
    ///
    /// The output schema is derived from the reader's current table
    /// schema (see `bqlite_core::storage::SegmentReader::schema`),
    /// which matches the manifest's "schema authority" contract from
    /// `storage-format.md §12.2`.
    ///
    /// **Wave 1 caveat.** Non-[`ColumnProjection::all`] projections
    /// are accepted and forwarded to the reader so the plumbing is
    /// exercised, but `output_schema()` still reflects the full
    /// table schema. Projection-driven narrowing lands with the
    /// projection-pruning task in a later wave; callers that hit
    /// the schema mismatch today should pass [`ColumnProjection::all`].
    pub fn new(
        reader: Arc<dyn SegmentReader>,
        projection: ColumnProjection,
        predicate: Option<Arc<dyn Predicate>>,
        cancel: CancellationToken,
    ) -> Self {
        let output_schema = OperatorSchema::from_table(reader.schema());
        Self {
            reader,
            projection,
            predicate,
            cancel,
            output_schema,
            segments: None,
            next_segment: 0,
            current: None,
            exhausted: false,
        }
    }

    /// Convenience constructor that projects every column with no
    /// predicate pushdown and an unused cancellation token. The
    /// engine bind step (TASK-118) will use the full constructor; this
    /// shortcut keeps planner unit tests terse.
    pub fn full_scan(reader: Arc<dyn SegmentReader>) -> Self {
        Self::new(
            reader,
            ColumnProjection::all(),
            None,
            CancellationToken::new(),
        )
    }
}

impl PhysicalOperator for ScanOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        // Materialize the segment list eagerly. `SegmentReader::segments`
        // returns a lazy iterator; collecting here surfaces any reader
        // error before `next_batch` is first called, which matches the
        // "fail cleanly before results start flowing" clause in the
        // trait doc (`operator.rs` §PhysicalOperator::open).
        let handles: Result<Vec<SegmentHandle>> = self.reader.segments().collect();
        self.segments = Some(handles?);
        self.next_segment = 0;
        self.current = None;
        self.exhausted = false;
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        if self.cancel.is_cancelled() {
            return Err(BqliteError::Cancelled);
        }
        let segments = self
            .segments
            .as_ref()
            .expect("ScanOperator::next_batch called before open()");

        loop {
            // Ensure we have an active per-segment scan.
            if self.current.is_none() {
                if self.next_segment >= segments.len() {
                    self.exhausted = true;
                    return Ok(None);
                }
                let handle = &segments[self.next_segment];
                self.next_segment += 1;
                let scan =
                    self.reader
                        .open_segment(handle, &self.projection, self.predicate.clone())?;
                self.current = Some(scan);
            }

            let scan = self.current.as_mut().expect("current scan just set");
            match scan.next_row_group()? {
                Some(batch) => return Ok(Some(batch)),
                None => {
                    // Exhausted this segment — drop the scan and loop
                    // to advance to the next one (or signal overall
                    // exhaustion).
                    self.current = None;
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        // Dropping the current scan releases its OS resources
        // (mmap, file handle, decompression state) per the trait doc.
        // `close` must be idempotent: clearing already-None fields is
        // cheap and correct.
        self.current = None;
        self.segments = None;
        self.exhausted = true;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_core::{BqlType, ColumnDef, TableSchema, ZoneMap};

    use super::*;

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

    fn arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("event_type", DataType::Utf8, false),
        ]))
    }

    fn make_batch(ids: &[&str], tss: &[i64], evts: &[&str]) -> RecordBatch {
        let ids: ArrayRef = Arc::new(StringArray::from(ids.to_vec()));
        let tss: ArrayRef = Arc::new(Int64Array::from(tss.to_vec()));
        let evts: ArrayRef = Arc::new(StringArray::from(evts.to_vec()));
        RecordBatch::try_new(arrow_schema(), vec![ids, tss, evts]).unwrap()
    }

    /// Reader that yields a fixed list of segments, each carrying a
    /// fixed list of row-groups. Good enough to exercise multi-segment
    /// multi-row-group iteration without a real storage layer.
    ///
    /// The reader also captures the last `predicate` argument it saw
    /// through [`open_segment`] so tests can assert that predicate
    /// forwarding actually reaches the storage layer — a weaker
    /// assertion (compile-time type-check only) would miss a
    /// regression where the scan silently dropped the predicate.
    struct VecReader {
        schema: TableSchema,
        segments: Vec<(SegmentHandle, Vec<RecordBatch>)>,
        open_calls: AtomicUsize,
        last_predicate: Mutex<Option<Arc<dyn Predicate>>>,
    }

    impl VecReader {
        fn new(schema: TableSchema, segments: Vec<(SegmentHandle, Vec<RecordBatch>)>) -> Self {
            Self {
                schema,
                segments,
                open_calls: AtomicUsize::new(0),
                last_predicate: Mutex::new(None),
            }
        }

        fn open_calls(&self) -> usize {
            self.open_calls.load(Ordering::SeqCst)
        }

        fn last_predicate(&self) -> Option<Arc<dyn Predicate>> {
            self.last_predicate.lock().unwrap().clone()
        }
    }

    struct VecScan {
        batches: Vec<RecordBatch>,
        position: usize,
    }

    impl SegmentReader for VecReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            Box::new(self.segments.iter().map(|(h, _)| Ok(h.clone())))
        }

        fn open_segment(
            &self,
            handle: &SegmentHandle,
            _projection: &ColumnProjection,
            predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_predicate.lock().unwrap() = predicate;
            match self.segments.iter().find(|(h, _)| h == handle) {
                Some((_, batches)) => Ok(Box::new(VecScan {
                    batches: batches.clone(),
                    position: 0,
                })),
                None => Err(BqliteError::Execution(format!(
                    "unknown segment handle {handle:?}"
                ))),
            }
        }
    }

    impl SegmentScan for VecScan {
        fn row_group_count(&self) -> usize {
            self.batches.len()
        }

        fn row_group_zone_maps(&self, _idx: usize) -> Result<HashMap<String, ZoneMap>> {
            Ok(HashMap::new())
        }

        fn next_row_group(&mut self) -> Result<Option<RecordBatch>> {
            if self.position >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.position].clone();
            self.position += 1;
            Ok(Some(b))
        }
    }

    fn make_handle(segment_id: u64, row_count: u64) -> SegmentHandle {
        SegmentHandle {
            segment_id,
            shard_id: 0,
            window_id: 0,
            row_count,
            schema_version: 0,
        }
    }

    /// Reader that returns an error mid-iteration. Proves `open()`
    /// surfaces iterator errors instead of silently swallowing them.
    struct ErroringReader {
        schema: TableSchema,
    }

    impl SegmentReader for ErroringReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            let items: Vec<Result<SegmentHandle>> = vec![
                Ok(make_handle(1, 0)),
                Err(BqliteError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "segment list truncated",
                ))),
            ];
            Box::new(items.into_iter())
        }

        fn open_segment(
            &self,
            _handle: &SegmentHandle,
            _projection: &ColumnProjection,
            _predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            Err(BqliteError::Execution("unreachable".into()))
        }
    }

    /// Reader whose `open_segment` fails. Proves `next_batch()`
    /// surfaces per-segment open errors.
    struct OpenFailReader {
        schema: TableSchema,
        handle: SegmentHandle,
    }

    impl SegmentReader for OpenFailReader {
        fn schema(&self) -> &TableSchema {
            &self.schema
        }

        fn segments(&self) -> Box<dyn Iterator<Item = Result<SegmentHandle>> + Send + '_> {
            Box::new(std::iter::once(Ok(self.handle.clone())))
        }

        fn open_segment(
            &self,
            _handle: &SegmentHandle,
            _projection: &ColumnProjection,
            _predicate: Option<Arc<dyn Predicate>>,
        ) -> Result<Box<dyn SegmentScan>> {
            Err(BqliteError::Execution("segment not found".into()))
        }
    }

    /// Predicate that records each `accepts_zone` call so the test
    /// can verify it was forwarded through to the reader.
    #[derive(Debug, Default)]
    struct RecordingPredicate {
        calls: AtomicUsize,
    }

    impl Predicate for RecordingPredicate {
        fn accepts_zone(&self, _column: &str, _zone: &ZoneMap) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    // ── ScanOperator: construction and schema ────────────────────────────────

    #[test]
    fn scan_output_schema_matches_reader_table_schema() {
        let reader = Arc::new(VecReader::new(minimal_schema(), vec![]));
        let scan = ScanOperator::full_scan(reader);
        let schema = scan.output_schema();
        // `OperatorSchema::from_table` includes declared columns plus
        // the implicit system columns.
        let names: Vec<&str> = schema.columns().iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"entity_id"));
        assert!(names.contains(&"ts"));
        assert!(names.contains(&"event_type"));
    }

    // ── ScanOperator: empty reader ───────────────────────────────────────────

    #[test]
    fn empty_reader_yields_no_batches() {
        // This is the exact shape TASK-116's storage stub returns and
        // the smoke test at TASK-123 relies on: an open database with
        // zero segments must drain cleanly.
        let reader = Arc::new(VecReader::new(minimal_schema(), vec![]));
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();
        assert!(scan.next_batch().unwrap().is_none());
        // Exhaustion is sticky.
        assert!(scan.next_batch().unwrap().is_none());
        assert!(scan.next_batch().unwrap().is_none());
        scan.close().unwrap();
    }

    // ── ScanOperator: single segment ─────────────────────────────────────────

    #[test]
    fn single_segment_single_row_group_yields_one_batch() {
        let batch = make_batch(&["u1", "u1", "u2"], &[100, 200, 300], &["a", "b", "c"]);
        let expected_rows = batch.num_rows();
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![(make_handle(1, 3), vec![batch])],
        ));
        let open_counter = reader.clone();
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();

        let out = scan.next_batch().unwrap().expect("batch");
        assert_eq!(out.num_rows(), expected_rows);
        assert_eq!(out.num_columns(), 3);

        assert!(scan.next_batch().unwrap().is_none());
        assert_eq!(
            open_counter.open_calls(),
            1,
            "should open each segment exactly once"
        );
    }

    #[test]
    fn single_segment_multi_row_group_yields_every_batch() {
        let b1 = make_batch(&["u1"], &[10], &["x"]);
        let b2 = make_batch(&["u2", "u2"], &[20, 30], &["y", "z"]);
        let b3 = make_batch(&["u3"], &[40], &["w"]);
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![(make_handle(1, 4), vec![b1, b2, b3])],
        ));
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();

        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = scan.next_batch().unwrap() {
            rows += batch.num_rows();
            batches += 1;
        }
        assert_eq!(batches, 3);
        assert_eq!(rows, 4);
    }

    // ── ScanOperator: multi-segment ──────────────────────────────────────────

    #[test]
    fn multi_segment_advances_across_segments() {
        let s1_b1 = make_batch(&["u1"], &[1], &["a"]);
        let s2_b1 = make_batch(&["u2"], &[2], &["b"]);
        let s2_b2 = make_batch(&["u3"], &[3], &["c"]);
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![
                (make_handle(1, 1), vec![s1_b1]),
                (make_handle(2, 2), vec![s2_b1, s2_b2]),
            ],
        ));
        let open_counter = reader.clone();
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();

        let mut all_rows = Vec::new();
        while let Some(batch) = scan.next_batch().unwrap() {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..col.len() {
                all_rows.push(col.value(i).to_string());
            }
        }
        assert_eq!(all_rows, vec!["u1", "u2", "u3"]);
        assert_eq!(
            open_counter.open_calls(),
            2,
            "opens each segment exactly once"
        );
    }

    #[test]
    fn empty_segment_is_skipped_to_next_segment() {
        // A segment with zero row-groups should not terminate the
        // scan — the iterator must advance to the next segment.
        let real = make_batch(&["u1"], &[1], &["a"]);
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![(make_handle(1, 0), vec![]), (make_handle(2, 1), vec![real])],
        ));
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();
        let batch = scan.next_batch().unwrap().expect("next segment yields");
        assert_eq!(batch.num_rows(), 1);
        assert!(scan.next_batch().unwrap().is_none());
    }

    // ── ScanOperator: error propagation ──────────────────────────────────────

    #[test]
    fn open_surfaces_segment_iterator_error() {
        let reader = Arc::new(ErroringReader {
            schema: minimal_schema(),
        });
        let mut scan = ScanOperator::full_scan(reader);
        let err = scan.open().expect_err("iterator error should surface");
        assert!(matches!(err, BqliteError::Io(_)), "{err}");
    }

    #[test]
    fn next_batch_surfaces_open_segment_error() {
        let reader = Arc::new(OpenFailReader {
            schema: minimal_schema(),
            handle: make_handle(1, 0),
        });
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();
        let err = scan
            .next_batch()
            .expect_err("open_segment failure should surface");
        assert!(matches!(err, BqliteError::Execution(_)), "{err}");
    }

    // ── ScanOperator: cancellation ───────────────────────────────────────────

    #[test]
    fn cancelled_token_aborts_next_batch() {
        let batch = make_batch(&["u1"], &[1], &["a"]);
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![(make_handle(1, 1), vec![batch])],
        ));
        let cancel = CancellationToken::new();
        let mut scan = ScanOperator::new(reader, ColumnProjection::all(), None, cancel.clone());
        scan.open().unwrap();
        cancel.cancel();
        let err = scan.next_batch().expect_err("cancellation should abort");
        assert!(matches!(err, BqliteError::Cancelled), "{err}");
    }

    #[test]
    fn exhausted_scan_ignores_cancellation() {
        // Once the scan has reached exhaustion, subsequent
        // `next_batch` calls return `Ok(None)` without checking the
        // cancellation token. This matches the `Ok(None)` sticky
        // contract in the PhysicalOperator trait doc.
        let reader = Arc::new(VecReader::new(minimal_schema(), vec![]));
        let cancel = CancellationToken::new();
        let mut scan = ScanOperator::new(reader, ColumnProjection::all(), None, cancel.clone());
        scan.open().unwrap();
        assert!(scan.next_batch().unwrap().is_none());
        cancel.cancel();
        assert!(scan.next_batch().unwrap().is_none());
    }

    // ── ScanOperator: lifecycle ──────────────────────────────────────────────

    #[test]
    fn close_is_idempotent() {
        let reader = Arc::new(VecReader::new(minimal_schema(), vec![]));
        let mut scan = ScanOperator::full_scan(reader);
        scan.close().unwrap();
        scan.close().unwrap();
        scan.close().unwrap();
    }

    #[test]
    fn close_without_open_is_ok() {
        let reader = Arc::new(VecReader::new(minimal_schema(), vec![]));
        let mut scan = ScanOperator::full_scan(reader);
        scan.close().unwrap();
    }

    #[test]
    fn close_after_drain_is_ok() {
        let batch = make_batch(&["u1"], &[1], &["a"]);
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![(make_handle(1, 1), vec![batch])],
        ));
        let mut scan = ScanOperator::full_scan(reader);
        scan.open().unwrap();
        while scan.next_batch().unwrap().is_some() {}
        scan.close().unwrap();
    }

    #[test]
    fn predicate_is_forwarded_to_open_segment() {
        // Predicate is consulted inside the reader / segment scan —
        // the Wave 1 scan stub does not itself call `accepts_zone`,
        // but it must forward the `Arc<dyn Predicate>` to
        // `open_segment` unchanged so later waves can rely on it.
        //
        // We observe forwarding in two ways:
        //   1. VecReader captures the received predicate in
        //      `last_predicate`; after driving the scan, the captured
        //      predicate must be `Some(_)`.
        //   2. Calling `accepts_zone` on the captured predicate
        //      bumps the RecordingPredicate counter, proving the
        //      *same* Arc was forwarded (and not replaced by `None`).
        let recording: Arc<RecordingPredicate> = Arc::new(RecordingPredicate::default());
        let predicate: Arc<dyn Predicate> = Arc::clone(&recording) as Arc<dyn Predicate>;
        let batch = make_batch(&["u1"], &[1], &["a"]);
        let reader = Arc::new(VecReader::new(
            minimal_schema(),
            vec![(make_handle(1, 1), vec![batch])],
        ));
        let reader_for_assert = Arc::clone(&reader);
        let mut scan = ScanOperator::new(
            reader,
            ColumnProjection::all(),
            Some(predicate),
            CancellationToken::new(),
        );
        scan.open().unwrap();
        let _ = scan.next_batch().unwrap();

        // Captured predicate is non-None and points at the same
        // RecordingPredicate instance — calling it must increment
        // the original counter, not a separate one.
        let captured = reader_for_assert
            .last_predicate()
            .expect("reader must have received Some(predicate)");
        let zone = ZoneMap::default();
        assert!(captured.accepts_zone("entity_id", &zone));
        assert_eq!(recording.calls.load(Ordering::SeqCst), 1);
    }

    // ── Trait object compatibility ───────────────────────────────────────────

    #[test]
    fn scan_operator_is_trait_object() {
        // The engine bind step will materialize the scan into a
        // `Box<dyn PhysicalOperator>` tree. Pin object safety at
        // compile time.
        let reader = Arc::new(VecReader::new(minimal_schema(), vec![]));
        let scan: Box<dyn PhysicalOperator> = Box::new(ScanOperator::full_scan(reader));
        let _ = scan;
    }
}
