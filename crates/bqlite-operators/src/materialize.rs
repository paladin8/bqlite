//! Materialization boundary between the encoded scan/filter segment
//! and the stateless-operator pipeline (CP7 of the zero-copy scan/
//! filter plan).
//!
//! The fused scan/filter segment produces two artifacts:
//!
//! 1. An [`EncodedBatch`] of pinned column chunks.
//! 2. A [`RowSelection`] from selection-first predicate kernels.
//!
//! This module collapses those two artifacts into a single
//! `FilteredBatch` with dense rows (`selection: None`), which is what
//! downstream stateless operators (filter, project, limit) consume per
//! `docs/design/execution-model.md` §3.8.
//!
//! # Why one shared helper
//!
//! Every crossing from the encoded path to the post-boundary world
//! **must** go through this helper. If multiple call sites rebuild
//! record batches ad hoc, the copy-budget metrics diverge and
//! correctness guarantees (null semantics, projection order) drift. CP8
//! gates that budget explicitly — [`materialize_selected`] is the only
//! place that turns encoded bytes into a `RecordBatch`.

use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use bqlite_core::encoded::{EncodedBatch, RowSelection};
use bqlite_core::{BqlType, BqliteError, Result};
use bqlite_storage::materialize_encoded_column_selected;

use crate::filtered_batch::FilteredBatch;

/// Materialize an [`EncodedBatch`] narrowed by `selection` into a dense
/// [`FilteredBatch`] with `selection: None`.
///
/// Each column is decoded through
/// [`materialize_encoded_column_selected`], which dispatches to the
/// right per-encoding materializer and applies the row selection once.
/// The result is a `RecordBatch` whose row count equals
/// `selection.len()` (or `batch.row_count` when `selection` is `None`).
///
/// # Contract
///
/// - The output schema equals `schema`. Callers supplying the scan's
///   cached `Arc<ArrowSchema>` pay zero allocations for schema setup.
/// - Column order in `types` must match `schema`'s field order and
///   `batch.columns`' order.
/// - Returning a `FilteredBatch` with `selection: None` is the
///   boundary's §3.8 contract: downstream stateless ops treat the
///   batch as dense.
pub fn materialize_selected(
    batch: &EncodedBatch,
    selection: Option<&RowSelection>,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
) -> Result<FilteredBatch> {
    if batch.columns.len() != types.len() {
        return Err(BqliteError::Execution(format!(
            "materialize_selected: batch has {} columns but {} types were provided",
            batch.columns.len(),
            types.len(),
        )));
    }
    if batch.columns.len() != schema.fields().len() {
        return Err(BqliteError::Execution(format!(
            "materialize_selected: batch has {} columns but schema has {} fields",
            batch.columns.len(),
            schema.fields().len(),
        )));
    }
    let mut arrays = Vec::with_capacity(batch.columns.len());
    for (col, ty) in batch.columns.iter().zip(types.iter()) {
        arrays.push(materialize_encoded_column_selected(col, ty, selection)?);
    }
    let record = RecordBatch::try_new(schema, arrays).map_err(|e| {
        BqliteError::Execution(format!("materialize_selected: record-batch build failed: {e}"))
    })?;
    Ok(FilteredBatch::dense(record))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field};
    use bqlite_core::encoded::{EncodedColumn, RowRun, SelectionVector};

    fn i64_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new("x", DataType::Int64, true)]))
    }

    fn materialized_column(vals: Vec<i64>) -> EncodedColumn {
        let rows = vals.len() as u32;
        EncodedColumn::Materialized {
            array: Arc::new(Int64Array::from(vals)),
            rows,
        }
    }

    #[test]
    fn materialize_selected_dense_passes_every_row() {
        let col = materialized_column(vec![10, 20, 30, 40]);
        let batch = EncodedBatch::new(4, vec![col]);
        let fb =
            materialize_selected(&batch, None, &[BqlType::Int], i64_schema()).unwrap();
        assert!(fb.is_dense());
        assert_eq!(fb.batch.num_rows(), 4);
        assert!(fb.selection.is_none());
    }

    #[test]
    fn materialize_selected_with_runs_applies_selection() {
        let col = materialized_column(vec![10, 20, 30, 40, 50]);
        let batch = EncodedBatch::new(5, vec![col]);
        let sel = RowSelection::Runs(vec![
            RowRun { start: 0, len: 1 },
            RowRun { start: 3, len: 2 },
        ]);
        let fb = materialize_selected(&batch, Some(&sel), &[BqlType::Int], i64_schema()).unwrap();
        assert!(fb.is_dense(), "boundary produces dense batch");
        assert_eq!(fb.batch.num_rows(), 3);
        let ints = fb
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[10i64, 40, 50]);
    }

    #[test]
    fn materialize_selected_indices_selection() {
        let col = materialized_column(vec![10, 20, 30, 40]);
        let batch = EncodedBatch::new(4, vec![col]);
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 3]));
        let fb = materialize_selected(&batch, Some(&sel), &[BqlType::Int], i64_schema()).unwrap();
        assert_eq!(fb.batch.num_rows(), 2);
        let ints = fb
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[20i64, 40]);
    }

    #[test]
    fn materialize_selected_type_mismatch_errors() {
        let col = materialized_column(vec![1, 2]);
        let batch = EncodedBatch::new(2, vec![col]);
        let err =
            materialize_selected(&batch, None, &[BqlType::Int, BqlType::Int], i64_schema())
                .expect_err("wrong types count must fail");
        assert!(matches!(err, BqliteError::Execution(_)));
    }

    #[test]
    fn encoded_path_kernel_plus_boundary_matches_dense_expectation() {
        // Acceptance test: the CP3 kernel (RLE-preserving eq) followed
        // by the CP7 materialization boundary must produce the same
        // rows as a hand-computed dense expected output. This proves
        // the encoded scan → kernel → boundary path stays consistent
        // with the materialized path per §3.8.
        use crate::encoded_filter::{EncodedPredicateKernel, RleIntEqKernel};
        use bqlite_core::encoded::{EncodedKind, PinnedChunk};

        // RLE column: [1,1,1,2,2,1,1,1,1,1]
        // run_ends = [3, 5, 10], values = [1, 2, 1]
        let run_count: u32 = 3;
        let mut params = Vec::new();
        params.extend_from_slice(&run_count.to_le_bytes());
        let run_ends: [u32; 3] = [3, 5, 10];
        let run_vals: [i64; 3] = [1, 2, 1];
        let mut payload = Vec::new();
        for e in &run_ends {
            payload.extend_from_slice(&e.to_le_bytes());
        }
        for v in &run_vals {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        let col = EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(payload),
                nulls: None,
                params: Arc::from(params),
            },
            kind: EncodedKind::Rle,
            rows: 10,
        };
        let batch = EncodedBatch::new(10, vec![col]);

        // Kernel: value == 1 → runs [0,3) and [5,10).
        let kernel = RleIntEqKernel::new(1);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let sel = kernel.apply(&batch.columns[0].view(), &input);

        // Boundary: materialize selected rows. Expected values = 1
        // repeated 8 times (3 + 5).
        let fb =
            materialize_selected(&batch, Some(&sel), &[BqlType::Int], i64_schema()).unwrap();
        assert!(fb.is_dense());
        assert_eq!(fb.batch.num_rows(), 8);
        let ints = fb
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[1i64, 1, 1, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn materialize_selected_empty_selection_yields_zero_rows() {
        let col = materialized_column(vec![10, 20, 30]);
        let batch = EncodedBatch::new(3, vec![col]);
        let sel = RowSelection::empty();
        let fb = materialize_selected(&batch, Some(&sel), &[BqlType::Int], i64_schema()).unwrap();
        assert_eq!(fb.batch.num_rows(), 0);
        assert!(fb.is_dense());
    }
}
