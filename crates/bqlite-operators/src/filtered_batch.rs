//! `FilteredBatch` — stateless push-segment IR.
//!
//! `FilteredBatch { batch, selection: Option<SelectionVector> }` is the
//! input/output value of every stateless pipeline operator inside the
//! Wave 5 fused push segment (filter → project → limit), per
//! `docs/design/execution-model.md` §3.8.
//!
//! # What this type represents
//!
//! A `RecordBatch` plus an optional row mask. When `selection` is
//! `None`, every row in `batch` is live (a dense batch). When
//! `selection` is `Some(sv)`, only the indices in `sv` are live; the
//! underlying Arrow arrays are unchanged.
//!
//! # Contract (per §3.8)
//!
//! - **filter** narrows `selection` — it intersects a BooleanArray
//!   mask with the existing selection (or constructs a new one from
//!   `None`).
//! - **project** rewrites columns to match the post-selection row
//!   count; it does not materialize a dense batch until forced to.
//! - **limit** truncates `selection` when present, or slices the
//!   batch when `selection` is `None`.
//! - Materialization happens only at the push-segment boundary or
//!   when a downstream stateful operator demands a dense batch.
//!
//! # Boundary semantics
//!
//! The encoded-preserving scan/filter segment (from
//! `docs/design/storage/zero-copy-scan-filter.md`) emits
//! `FilteredBatch { batch, selection: None }` at the materialization
//! boundary: the selection has already collapsed and projection has
//! already been applied. Downstream stateless ops therefore inherit
//! a dense batch and apply §3.8 rules from there. Mid-segment
//! operators may still re-introduce a selection.

use arrow::record_batch::RecordBatch;

use bqlite_core::encoded::SelectionVector;

/// A `RecordBatch` plus an optional row-selection mask.
///
/// See the module-level doc for the semantic contract.
#[derive(Debug, Clone)]
pub struct FilteredBatch {
    /// Underlying Arrow row-group. Columns are not sliced —
    /// `selection` (if any) tells downstream consumers which rows are
    /// live.
    pub batch: RecordBatch,
    /// Optional row mask. `None` means "every row in `batch` is live"
    /// (equivalent to a dense batch).
    pub selection: Option<SelectionVector>,
}

impl FilteredBatch {
    /// Construct a dense `FilteredBatch` (no row mask).
    pub fn dense(batch: RecordBatch) -> Self {
        Self {
            batch,
            selection: None,
        }
    }

    /// Construct a selection-carrying `FilteredBatch`.
    pub fn with_selection(batch: RecordBatch, selection: SelectionVector) -> Self {
        Self {
            batch,
            selection: Some(selection),
        }
    }

    /// Number of live rows after applying `selection` (if any).
    ///
    /// For a dense batch, this equals `batch.num_rows()`.
    pub fn live_rows(&self) -> usize {
        match &self.selection {
            None => self.batch.num_rows(),
            Some(sv) => sv.len(),
        }
    }

    /// True when no rows are live.
    pub fn is_empty(&self) -> bool {
        self.live_rows() == 0
    }

    /// True when every row in the underlying batch is live.
    pub fn is_dense(&self) -> bool {
        self.selection.is_none()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn sample_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let ints: Vec<i64> = (0..rows as i64).collect();
        let strs: Vec<String> = (0..rows).map(|i| format!("v{i}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ints)),
                Arc::new(StringArray::from(strs)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn dense_has_no_selection() {
        let b = FilteredBatch::dense(sample_batch(4));
        assert!(b.is_dense());
        assert!(b.selection.is_none());
        assert_eq!(b.live_rows(), 4);
        assert!(!b.is_empty());
    }

    #[test]
    fn with_selection_reports_live_rows_from_mask() {
        let b = FilteredBatch::with_selection(
            sample_batch(10),
            SelectionVector::from_sorted(vec![0, 3, 7]),
        );
        assert!(!b.is_dense());
        assert_eq!(b.live_rows(), 3);
        assert!(!b.is_empty());
    }

    #[test]
    fn with_empty_selection_is_empty() {
        let b = FilteredBatch::with_selection(sample_batch(10), SelectionVector::new());
        assert_eq!(b.live_rows(), 0);
        assert!(b.is_empty());
    }

    #[test]
    fn dense_with_zero_rows_is_empty() {
        let b = FilteredBatch::dense(sample_batch(0));
        assert_eq!(b.live_rows(), 0);
        assert!(b.is_empty());
        assert!(b.is_dense());
    }

    #[test]
    fn imports_shared_selection_vector_type() {
        // The compile-time point here is that `SelectionVector` comes
        // from `bqlite_core::encoded`, not a local duplicate.
        fn take(_sv: SelectionVector) {}
        take(SelectionVector::from_sorted(vec![1, 2, 3]));
    }
}
