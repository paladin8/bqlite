//! Shared encoded read-path IR.
//!
//! Types in this module are the cross-crate vocabulary for the
//! encoded-preserving scan/filter path described in
//! `docs/design/storage/zero-copy-scan-filter.md`. They sit at the
//! boundary between `bqlite-storage` (which produces encoded batches
//! from segment bytes) and `bqlite-operators` (which runs selection-
//! first predicate kernels over them).
//!
//! # Lifetime-free public IR, borrowed views at kernel dispatch
//!
//! Every public type in this module is lifetime-free. Byte handles
//! are [`ArcBytes`] clones — cheap (one atomic refcount bump per
//! chunk open, not per row) and freely storable across crate
//! boundaries. Borrowed views ([`PinnedChunkRef`], [`EncodedColumnView`])
//! are constructed via `view()` methods inside a single kernel call
//! and do not cross crate boundaries.
//!
//! The copy budget from §3 of the design doc is unaffected: `ArcBytes`
//! is a pointer, not a payload copy.
//!
//! # Not an Arrow replacement
//!
//! `EncodedBatch` is **pre-materialization** and internal to the
//! fused scan/filter segment. The materialization boundary emits a
//! `FilteredBatch { RecordBatch, selection: None }` (see
//! `execution-model.md` §3.8). Downstream stateless operators keep
//! consuming `RecordBatch` unchanged.

use std::sync::Arc;

use ::arrow::array::ArrayRef;

use crate::scalar::ScalarValue;

// ─────────────────────────────────────────────────────────────────────────────
// Byte handles
// ─────────────────────────────────────────────────────────────────────────────

/// Shared, refcounted handle to a run of encoded bytes.
///
/// Constructed by the reader from a mmap-backed slice via
/// `Arc::<[u8]>::from(...)` (or from an owned `Vec<u8>` after LZ4
/// decompression). Cloning is cheap — one atomic fetch-add. Kernels
/// dereference through a borrowed view for the duration of one call.
pub type ArcBytes = Arc<[u8]>;

// ─────────────────────────────────────────────────────────────────────────────
// PinnedChunk
// ─────────────────────────────────────────────────────────────────────────────

/// An encoded column chunk pinned into shared memory.
///
/// `payload` is the encoded value bytes (possibly LZ4-decompressed
/// once). `nulls` is the validity bitmap — kept separate so the
/// encoded path never eagerly splices nulls into a dense Arrow array.
/// `params` is the encoding-specific header/metadata block parsed from
/// the segment, re-exposed so kernels can read run-ends, dictionary
/// sizes, FOR reference values, etc. without re-parsing.
///
/// See `docs/design/storage/zero-copy-scan-filter.md` §6.1.
#[derive(Debug, Clone)]
pub struct PinnedChunk {
    /// Encoded value bytes. For LZ4-wrapped chunks this is the
    /// decompressed buffer; for uncompressed chunks it is a slice of
    /// the mmap.
    pub payload: ArcBytes,
    /// Optional validity bitmap (LSB-first, one bit per row, `1 =
    /// valid`). Absent when the column has no nulls in this chunk.
    pub nulls: Option<ArcBytes>,
    /// Encoding-specific parameter block. Kernels interpret this per
    /// [`EncodedKind`].
    pub params: ArcBytes,
}

impl PinnedChunk {
    /// Build a borrowed view of this chunk for the duration of one
    /// kernel call. The returned `PinnedChunkRef` carries the same
    /// byte contents but can be passed into `&[u8]`-consuming code
    /// without cloning the `Arc`.
    #[inline]
    pub fn view(&self) -> PinnedChunkRef<'_> {
        PinnedChunkRef {
            payload: &self.payload,
            nulls: self.nulls.as_deref(),
            params: &self.params,
        }
    }
}

/// Borrowed view of a [`PinnedChunk`]. Lives for one kernel call.
#[derive(Debug, Clone, Copy)]
pub struct PinnedChunkRef<'a> {
    pub payload: &'a [u8],
    pub nulls: Option<&'a [u8]>,
    pub params: &'a [u8],
}

// ─────────────────────────────────────────────────────────────────────────────
// EncodedKind
// ─────────────────────────────────────────────────────────────────────────────

/// Encoding discriminator for an [`EncodedColumn`].
///
/// `Bool` is intentionally distinct from `PlainFixed { width: 1 }`:
/// its payload is bit-packed (one bit per row), not byte-per-row, and
/// its kernels operate on packed words rather than byte comparisons.
///
/// Variants carry encoding-specific `Arc`-backed side data such as
/// dictionary value buffers or FSST symbol tables. The raw param
/// block (as-written on disk) still lives in
/// [`PinnedChunk::params`]; these fields hold parsed handles to
/// sub-regions that are expensive to re-locate per kernel call.
#[derive(Debug, Clone)]
pub enum EncodedKind {
    /// Bit-packed booleans (one bit per row).
    Bool,
    /// Plain fixed-width little-endian values (`width` bytes per row).
    PlainFixed { width: u8 },
    /// Plain UTF-8 strings with an offsets array in `params`.
    PlainString,
    /// Dictionary-encoded: row payload is a code stream; `values` is
    /// the dictionary value buffer.
    Dictionary {
        /// Parsed dictionary value bytes (layout matches
        /// `PlainString` for string dicts, `PlainFixed` for numeric).
        values: ArcBytes,
    },
    /// Run-length encoded: payload is run-values, `params` carries
    /// run-ends.
    Rle,
    /// Constant-valued column: every row holds `value`.
    Constant { value: Arc<ScalarValue> },
    /// Delta-of-values encoding.
    Delta,
    /// Double-delta encoding.
    DoubleDelta,
    /// Bit-packing (fixed width, `width` bits per row).
    BitPacking { width: u8 },
    /// Frame-of-reference.
    For,
    /// Patched frame-of-reference.
    PFor,
    /// Adaptive lossless floating-point.
    Alp,
    /// Fast Static Symbol Table; `symbols` is the parsed symbol-table
    /// region.
    Fsst { symbols: ArcBytes },
}

// ─────────────────────────────────────────────────────────────────────────────
// EncodedColumn
// ─────────────────────────────────────────────────────────────────────────────

/// A single encoded column in an [`EncodedBatch`].
///
/// Either fully encoded (the normal path, kernels consume it directly)
/// or a transitional materialized fallback for encodings whose kernel
/// hasn't been written yet (see Checkpoints 2 and 6 of the plan).
/// Downstream code must accept either variant.
#[derive(Debug, Clone)]
pub enum EncodedColumn {
    /// Encoded form: pinned bytes plus an encoding kind.
    Encoded {
        chunk: PinnedChunk,
        kind: EncodedKind,
        /// Logical row count of this column.
        rows: u32,
    },
    /// Transitional fallback: a pre-materialized Arrow array. Used
    /// when storage cannot yet build an `Encoded` variant for a
    /// particular encoding. Semantically equivalent to the encoded
    /// form plus materialization; kernels fall back to
    /// `compute::filter` over this array.
    Materialized {
        array: ArrayRef,
        /// Logical row count of this column. Always equals
        /// `array.len() as u32`.
        rows: u32,
    },
}

impl EncodedColumn {
    /// Logical row count.
    #[inline]
    pub fn rows(&self) -> u32 {
        match self {
            EncodedColumn::Encoded { rows, .. } | EncodedColumn::Materialized { rows, .. } => *rows,
        }
    }

    /// Build a borrowed view of this column for one kernel call.
    #[inline]
    pub fn view(&self) -> EncodedColumnView<'_> {
        match self {
            EncodedColumn::Encoded { chunk, kind, rows } => EncodedColumnView::Encoded {
                chunk: chunk.view(),
                kind,
                rows: *rows,
            },
            EncodedColumn::Materialized { array, rows } => EncodedColumnView::Materialized {
                array,
                rows: *rows,
            },
        }
    }
}

/// Borrowed view of an [`EncodedColumn`], lifetime-bound to the
/// source. Kernels consume this directly; it never crosses crate
/// boundaries.
#[derive(Debug)]
pub enum EncodedColumnView<'a> {
    Encoded {
        chunk: PinnedChunkRef<'a>,
        kind: &'a EncodedKind,
        rows: u32,
    },
    Materialized {
        array: &'a ArrayRef,
        rows: u32,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// EncodedBatch
// ─────────────────────────────────────────────────────────────────────────────

/// One row-group's worth of encoded columns.
///
/// Produced by `SegmentScan::next_encoded_row_group`. Consumed by
/// predicate kernels. Every column has `rows() == row_count`.
#[derive(Debug, Clone)]
pub struct EncodedBatch {
    pub row_count: u32,
    pub columns: Vec<EncodedColumn>,
}

impl EncodedBatch {
    /// Construct a new batch and validate per-column row counts.
    pub fn new(row_count: u32, columns: Vec<EncodedColumn>) -> Self {
        debug_assert!(
            columns.iter().all(|c| c.rows() == row_count),
            "EncodedBatch column row-count mismatch"
        );
        Self { row_count, columns }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Selection vectors and row selection
// ─────────────────────────────────────────────────────────────────────────────

/// Sorted ascending row indices into an encoded batch.
///
/// Invariant: indices are strictly ascending and every element is less
/// than the source batch's `row_count`. Kernels emit selection vectors;
/// combinators (intersect / truncate / from_bool_mask) preserve the
/// invariant.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Vec<u32>,
}

impl SelectionVector {
    /// Construct an empty selection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct from a sorted-ascending index vector. Caller is
    /// responsible for the sort invariant; `debug_assert` validates it.
    pub fn from_sorted(indices: Vec<u32>) -> Self {
        debug_assert!(
            indices.windows(2).all(|w| w[0] < w[1]),
            "SelectionVector must be strictly ascending"
        );
        Self { indices }
    }

    /// Borrow the underlying index slice.
    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        &self.indices
    }

    /// Consume and return the underlying index vector.
    #[inline]
    pub fn into_vec(self) -> Vec<u32> {
        self.indices
    }

    /// Number of selected rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// True when no rows are selected.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

/// A contiguous run of rows in one source batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRun {
    /// First selected row index, inclusive.
    pub start: u32,
    /// Run length in rows.
    pub len: u32,
}

impl RowRun {
    #[inline]
    pub fn end(&self) -> u32 {
        self.start + self.len
    }
}

/// Kernel input/output selection.
///
/// `Runs` is the RLE-friendly fast path — preserves run shape through
/// filter kernels and is the performance-critical output form for the
/// RLE predicate kernels in Checkpoint 4. `Indices` is the safe
/// fallback; mixed-variant combinators coerce to `Indices`.
///
/// See `docs/design/storage/zero-copy-scan-filter.md` §7.2 for the full
/// contract on `intersect` behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowSelection {
    Indices(SelectionVector),
    Runs(Vec<RowRun>),
}

impl RowSelection {
    /// Empty selection (zero rows).
    pub fn empty() -> Self {
        RowSelection::Indices(SelectionVector::new())
    }

    /// Construct a `Runs` selection.
    pub fn from_runs(runs: Vec<RowRun>) -> Self {
        RowSelection::Runs(runs)
    }

    /// Construct an `Indices` selection.
    pub fn from_indices(sel: SelectionVector) -> Self {
        RowSelection::Indices(sel)
    }

    /// Logical row count (sum of run lengths, or index vector length).
    pub fn len(&self) -> usize {
        match self {
            RowSelection::Indices(sv) => sv.len(),
            RowSelection::Runs(runs) => runs.iter().map(|r| r.len as usize).sum(),
        }
    }

    /// True when no rows are selected.
    pub fn is_empty(&self) -> bool {
        match self {
            RowSelection::Indices(sv) => sv.is_empty(),
            RowSelection::Runs(runs) => runs.is_empty() || runs.iter().all(|r| r.len == 0),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stitched batches (merge output)
// ─────────────────────────────────────────────────────────────────────────────

/// A reference to one row in a specific source batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRef {
    /// Index into `StitchedBatch::sources`.
    pub source: u16,
    /// Row index inside that source batch.
    pub row: u32,
}

/// A contiguous run of rows in one source batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRun {
    /// Index into `StitchedBatch::sources`.
    pub source: u16,
    /// First row index inside that source batch.
    pub start: u32,
    /// Run length.
    pub len: u32,
}

/// Shape of a stitched merge output.
///
/// `SingleSource` is the cheapest form — the merge degenerated to
/// passing through one source batch, optionally narrowed by a
/// selection. Preserving `Option<RowSelection>` (rather than forcing
/// `SelectionVector`) lets an upstream RLE kernel emit runs that
/// survive through the merge boundary.
#[derive(Debug, Clone)]
pub enum StitchedRows {
    SingleSource {
        source: u16,
        selection: Option<RowSelection>,
    },
    Runs(Vec<SourceRun>),
    Indices(Vec<RowRef>),
}

/// Output of the stitched merge: a set of source `EncodedBatch`es
/// plus a reference shape describing merged row order.
#[derive(Debug, Clone)]
pub struct StitchedBatch {
    pub sources: Vec<EncodedBatch>,
    pub rows: StitchedRows,
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use ::arrow::array::Int64Array;

    fn chunk(bytes: &[u8]) -> PinnedChunk {
        PinnedChunk {
            payload: Arc::from(bytes.to_vec()),
            nulls: None,
            params: Arc::from(Vec::<u8>::new()),
        }
    }

    #[test]
    fn pinned_chunk_view_byte_equal_to_payload() {
        let bytes: &[u8] = b"encoded-column-bytes";
        let c = chunk(bytes);
        let v = c.view();
        assert_eq!(v.payload, &*c.payload);
        assert_eq!(v.payload, bytes);
        assert!(v.nulls.is_none());
    }

    #[test]
    fn pinned_chunk_view_preserves_nulls() {
        let c = PinnedChunk {
            payload: Arc::from(vec![0u8; 8]),
            nulls: Some(Arc::from(vec![0b1010u8])),
            params: Arc::from(Vec::<u8>::new()),
        };
        let v = c.view();
        assert_eq!(v.nulls, Some(&[0b1010u8][..]));
    }

    #[test]
    fn arc_shared_across_clones_not_per_row() {
        // One atomic bump per column view; zero per row.
        let c = chunk(b"x");
        let v1 = c.view();
        let v2 = c.view();
        // Both borrow the same Arc — no new allocation.
        assert_eq!(v1.payload.as_ptr(), v2.payload.as_ptr());
    }

    #[test]
    fn selection_vector_rejects_unsorted_in_debug() {
        // In release mode this construction is legal; the helper
        // documents the strictly-ascending invariant via `debug_assert`.
        let sv = SelectionVector::from_sorted(vec![0, 3, 5, 9]);
        assert_eq!(sv.as_slice(), &[0, 3, 5, 9]);
        assert_eq!(sv.len(), 4);
        assert!(!sv.is_empty());
    }

    #[test]
    fn selection_vector_empty() {
        let sv = SelectionVector::new();
        assert!(sv.is_empty());
        assert_eq!(sv.len(), 0);
    }

    #[test]
    fn row_run_end_is_start_plus_len() {
        let r = RowRun { start: 10, len: 4 };
        assert_eq!(r.end(), 14);
    }

    #[test]
    fn row_selection_len_sums_runs() {
        let sel = RowSelection::Runs(vec![
            RowRun { start: 0, len: 3 },
            RowRun { start: 7, len: 5 },
        ]);
        assert_eq!(sel.len(), 8);
        assert!(!sel.is_empty());
    }

    #[test]
    fn row_selection_empty() {
        assert!(RowSelection::empty().is_empty());
        assert!(RowSelection::Runs(vec![]).is_empty());
        assert!(RowSelection::Runs(vec![RowRun { start: 0, len: 0 }]).is_empty());
    }

    #[test]
    fn row_selection_from_constructors() {
        let runs = RowSelection::from_runs(vec![RowRun { start: 0, len: 2 }]);
        assert!(matches!(runs, RowSelection::Runs(_)));
        let indices = RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 2]));
        assert!(matches!(indices, RowSelection::Indices(_)));
    }

    #[test]
    fn encoded_batch_rows_match_column_rows() {
        let col = EncodedColumn::Encoded {
            chunk: chunk(b"payload"),
            kind: EncodedKind::PlainFixed { width: 8 },
            rows: 100,
        };
        let batch = EncodedBatch::new(100, vec![col]);
        assert_eq!(batch.row_count, 100);
        assert_eq!(batch.columns.len(), 1);
        assert_eq!(batch.columns[0].rows(), 100);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "row-count mismatch")]
    fn encoded_batch_panics_on_row_mismatch_in_debug() {
        let col = EncodedColumn::Encoded {
            chunk: chunk(b"payload"),
            kind: EncodedKind::PlainFixed { width: 8 },
            rows: 50,
        };
        let _ = EncodedBatch::new(100, vec![col]);
    }

    #[test]
    fn materialized_column_view_borrows_array() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let col = EncodedColumn::Materialized {
            array: array.clone(),
            rows: 3,
        };
        assert_eq!(col.rows(), 3);
        match col.view() {
            EncodedColumnView::Materialized { rows, array: a } => {
                assert_eq!(rows, 3);
                assert_eq!(a.len(), 3);
                // Arc::ptr_eq through the ArrayRef handle.
                assert!(Arc::ptr_eq(&array, a));
            }
            _ => panic!("expected materialized view"),
        }
    }

    #[test]
    fn stitched_single_source_carries_optional_selection() {
        let b = EncodedBatch::new(0, vec![]);
        let stitched = StitchedBatch {
            sources: vec![b],
            rows: StitchedRows::SingleSource {
                source: 0,
                selection: Some(RowSelection::from_runs(vec![RowRun { start: 0, len: 4 }])),
            },
        };
        match &stitched.rows {
            StitchedRows::SingleSource { source, selection } => {
                assert_eq!(*source, 0);
                assert!(matches!(selection, Some(RowSelection::Runs(_))));
            }
            _ => panic!("expected SingleSource"),
        }
    }

    #[test]
    fn stitched_indices_round_trip_row_count() {
        let refs = vec![
            RowRef { source: 0, row: 1 },
            RowRef { source: 1, row: 3 },
            RowRef { source: 0, row: 5 },
        ];
        let s = StitchedRows::Indices(refs.clone());
        if let StitchedRows::Indices(v) = s {
            assert_eq!(v.len(), refs.len());
            assert_eq!(v, refs);
        } else {
            panic!("expected Indices");
        }
    }

    #[test]
    fn stitched_runs_round_trip_row_count() {
        let runs = vec![
            SourceRun {
                source: 0,
                start: 0,
                len: 3,
            },
            SourceRun {
                source: 1,
                start: 10,
                len: 2,
            },
        ];
        let s = StitchedRows::Runs(runs.clone());
        if let StitchedRows::Runs(v) = s {
            let total: u32 = v.iter().map(|r| r.len).sum();
            assert_eq!(total, 5);
            assert_eq!(v, runs);
        } else {
            panic!("expected Runs");
        }
    }

    #[test]
    fn encoded_kind_variants_carry_expected_side_data() {
        let dict = EncodedKind::Dictionary {
            values: Arc::from(vec![0u8, 1, 2]),
        };
        match dict {
            EncodedKind::Dictionary { values } => assert_eq!(values.len(), 3),
            _ => panic!("expected Dictionary"),
        }
        let fsst = EncodedKind::Fsst {
            symbols: Arc::from(vec![9u8; 16]),
        };
        match fsst {
            EncodedKind::Fsst { symbols } => assert_eq!(symbols.len(), 16),
            _ => panic!("expected Fsst"),
        }
        let cst = EncodedKind::Constant {
            value: Arc::new(ScalarValue::Int(42)),
        };
        match cst {
            EncodedKind::Constant { value } => assert_eq!(*value, ScalarValue::Int(42)),
            _ => panic!("expected Constant"),
        }
    }
}
