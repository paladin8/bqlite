//! Selection helpers that bridge the encoded IR to Arrow compute.
//!
//! Kernel output is a [`RowSelection`] (runs or indices). The
//! materialization boundary turns that into either:
//!
//! - a dense `RecordBatch` (when every row is selected), or
//! - a `RecordBatch` + [`SelectionVector`], passed downstream as a
//!   [`crate::filtered_batch::FilteredBatch`] (per
//!   `docs/design/execution-model.md` §3.8).
//!
//! This module collects the small helpers that do that bridging so the
//! conversion rules live in one place.

use arrow::array::BooleanArray;
use bqlite_core::encoded::{RowSelection, SelectionVector};

/// Convert a [`RowSelection`] into an Arrow [`BooleanArray`] of length
/// `row_count` suitable for `arrow::compute::filter`. Every selected
/// row gets a `true` bit; every other row gets `false`.
pub fn selection_to_bool_array(sel: &RowSelection, row_count: u32) -> BooleanArray {
    let mut mask = vec![false; row_count as usize];
    match sel {
        RowSelection::Indices(sv) => {
            for &idx in sv.as_slice() {
                if (idx as usize) < mask.len() {
                    mask[idx as usize] = true;
                }
            }
        }
        RowSelection::Runs(runs) => {
            for r in runs {
                let start = r.start as usize;
                let end = (r.start + r.len) as usize;
                if start >= mask.len() {
                    continue;
                }
                let end = end.min(mask.len());
                mask[start..end].fill(true);
            }
        }
    }
    BooleanArray::from(mask)
}

/// Convert a [`RowSelection`] into a flat [`SelectionVector`]. Always
/// succeeds; `Runs` variants are expanded. Used by the materialization
/// boundary when producing a `FilteredBatch` that carries an explicit
/// row mask.
pub fn selection_as_vector(sel: &RowSelection) -> SelectionVector {
    sel.as_indices()
}

/// True when the selection logically covers every row in the batch.
pub fn is_dense(sel: &RowSelection, row_count: u32) -> bool {
    sel.len() == row_count as usize
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::encoded::{RowRun, SelectionVector};

    #[test]
    fn selection_to_bool_array_from_indices() {
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2, 4]));
        let arr = selection_to_bool_array(&sel, 5);
        let vals: Vec<Option<bool>> = arr.iter().collect();
        assert_eq!(
            vals,
            vec![Some(true), Some(false), Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn selection_to_bool_array_from_runs() {
        let sel = RowSelection::Runs(vec![
            RowRun { start: 0, len: 2 },
            RowRun { start: 4, len: 2 },
        ]);
        let arr = selection_to_bool_array(&sel, 6);
        let vals: Vec<bool> = arr.iter().map(|v| v.unwrap()).collect();
        assert_eq!(vals, vec![true, true, false, false, true, true]);
    }

    #[test]
    fn is_dense_detects_full_selection() {
        let dense = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3]));
        assert!(is_dense(&dense, 4));
        let sparse = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2]));
        assert!(!is_dense(&sparse, 4));
    }

    #[test]
    fn selection_as_vector_expands_runs() {
        let sel = RowSelection::Runs(vec![RowRun { start: 3, len: 2 }]);
        let sv = selection_as_vector(&sel);
        assert_eq!(sv.as_slice(), &[3, 4]);
    }
}
