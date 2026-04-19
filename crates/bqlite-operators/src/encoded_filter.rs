//! Selection-first predicate kernels over [`EncodedColumn`].
//!
//! A predicate kernel consumes an [`EncodedColumnView`] and the current
//! [`RowSelection`] for its batch, evaluates its predicate over just the
//! selected rows, and returns a narrowed [`RowSelection`]. This is the
//! "selection-first" contract from
//! `docs/design/storage/zero-copy-scan-filter.md` §7: a kernel never
//! materializes rows it has already been told to skip, and it never
//! evaluates rows the column bitmap marks null.
//!
//! # Kernel surface landing in CP3
//!
//! - [`ConstantEqKernel`] — constant-encoded column compared against a
//!   literal. Every live row collapses to either "all selected rows"
//!   or "no rows" by a single pointer-equal scalar comparison.
//!
//! The remaining kernels (Dict eq/IN, plain-fixed range, bool eq, null
//! checks on non-constant columns) land alongside the real
//! [`ScanOperator`] integration in CP7. The trait defined here is the
//! binding contract those kernels will also implement.
//!
//! # Non-goals for CP3
//!
//! This module is not yet wired into [`crate::scan::ScanOperator`]. The
//! dispatch path (`ScanPath::Encoded` → read
//! `next_encoded_row_group` → run kernels → materialize at boundary)
//! lands in CP7.

use bqlite_core::encoded::{
    EncodedColumnView, EncodedKind, PinnedChunkRef, RowRun, RowSelection, SelectionVector,
};
use bqlite_core::scalar::ScalarValue;

// ─────────────────────────────────────────────────────────────────────────────
// Kernel trait
// ─────────────────────────────────────────────────────────────────────────────

/// Selection-first predicate kernel.
///
/// Implementors evaluate a single predicate shape (e.g. "column == literal"
/// for a constant-encoded column) against the rows still live in
/// `input`, and return a narrowed [`RowSelection`].
///
/// Contract:
///
/// - The returned selection is a subset of `input`. Kernels never
///   widen the live row set.
/// - A kernel with no live rows returns [`RowSelection::empty()`].
/// - Null handling: the kernel consults `column`'s null bitmap. A null
///   at row `i` makes that row fail the predicate (SQL `NULL = x` is
///   `NULL`, which kernels treat as "not selected"). Null-specific
///   kernels (`IS NULL`) have their own variant.
pub trait EncodedPredicateKernel {
    fn apply(&self, column: &EncodedColumnView<'_>, input: &RowSelection) -> RowSelection;
}

// ─────────────────────────────────────────────────────────────────────────────
// ConstantEqKernel
// ─────────────────────────────────────────────────────────────────────────────

/// Kernel for `constant_column = literal`.
///
/// This is the cheapest filter shape in the encoded path: the column
/// already consists of exactly one logical value. The kernel performs
/// one scalar comparison. If the pinned constant does not equal
/// `literal`, every row fails and the kernel returns an empty
/// selection. If it does, the input selection is returned unchanged
/// (modulo null handling — nulls drop out of the live set).
pub struct ConstantEqKernel {
    pub literal: ScalarValue,
}

impl ConstantEqKernel {
    pub fn new(literal: ScalarValue) -> Self {
        Self { literal }
    }
}

impl EncodedPredicateKernel for ConstantEqKernel {
    fn apply(&self, column: &EncodedColumnView<'_>, input: &RowSelection) -> RowSelection {
        let (chunk, kind, rows) = match column {
            EncodedColumnView::Encoded { chunk, kind, rows } => (chunk, *kind, *rows),
            EncodedColumnView::Materialized { .. } => {
                // Materialized fallback is not this kernel's responsibility —
                // when a column lands as Materialized, the ScanOperator
                // dispatches to an Arrow-compute filter path instead.
                // CP7 owns that dispatch; until then we return the input
                // unchanged so callers can compose kernels independently.
                return input.clone();
            }
        };
        let pinned = match kind {
            EncodedKind::Constant { value } => value.as_ref(),
            _ => {
                // Wrong kernel for this encoding; caller's dispatch is a
                // bug. CP3 treats this defensively by passing input
                // through unchanged (the outer filter will still drop
                // non-matching rows at materialization). Future CPs may
                // promote this to a debug_assert.
                return input.clone();
            }
        };
        if pinned != &self.literal {
            return RowSelection::empty();
        }
        // Constant equals literal at every row. Null handling drops any
        // row whose bit is unset in the column's null bitmap (if any).
        apply_null_mask(chunk, rows, input)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Null-bitmap selection helper
// ─────────────────────────────────────────────────────────────────────────────

/// Narrow `input` by the column's null bitmap.
///
/// For encodings where "predicate satisfied" collapses to "row is not
/// null" (e.g. constant-column equality when the constant matches), the
/// live row set is `input ∩ valid-bits`. When the column has no nulls,
/// the input passes through unchanged.
fn apply_null_mask(
    chunk: &PinnedChunkRef<'_>,
    rows: u32,
    input: &RowSelection,
) -> RowSelection {
    let Some(bitmap) = chunk.nulls else {
        return input.clone();
    };
    // Iterate the input's row indices and keep those whose validity
    // bit is set. Works uniformly over Runs and Indices inputs by
    // expanding to indices first — the performance-sensitive
    // Runs-preserving path lands in CP4 where RLE is the primary
    // kernel shape.
    let mut out = Vec::with_capacity(input.len());
    let sv = input.as_indices();
    for &idx in sv.as_slice() {
        if idx >= rows {
            continue;
        }
        if bit_is_set(bitmap, idx as usize) {
            out.push(idx);
        }
    }
    RowSelection::Indices(SelectionVector::from_sorted(out))
}

/// LSB-first bit lookup (matches the segment-format v1 validity
/// bitmap layout).
#[inline]
fn bit_is_set(bitmap: &[u8], index: usize) -> bool {
    let byte = index >> 3;
    let bit = index & 7;
    byte < bitmap.len() && (bitmap[byte] >> bit) & 1 != 0
}

// ─────────────────────────────────────────────────────────────────────────────
// RleIntEqKernel — RLE-preserving equality for Int/Timestamp columns
// ─────────────────────────────────────────────────────────────────────────────

/// Kernel for `rle_int_or_timestamp_column = literal`.
///
/// This is CP4's reason to exist: for a low-cardinality column with
/// long runs, the filter output stays in [`RowSelection::Runs`] shape.
/// The cost is one comparison per run, not one per row — a large win
/// for event_type, country, os columns and similar.
///
/// # On-disk layout consumed
///
/// Matches `bqlite-storage/src/encoding/rle.rs`:
/// - `params` = `run_count: u32 LE` (4 bytes)
/// - `payload` = `[run_ends: u32 LE × run_count] || [i64 LE × run_count]`
///
/// Null handling: rows whose validity bit is unset are dropped after
/// run matching (runs are coerced to indices only when the null bitmap
/// splits them).
pub struct RleIntEqKernel {
    pub literal: i64,
}

impl RleIntEqKernel {
    pub fn new(literal: i64) -> Self {
        Self { literal }
    }
}

impl EncodedPredicateKernel for RleIntEqKernel {
    fn apply(&self, column: &EncodedColumnView<'_>, input: &RowSelection) -> RowSelection {
        let (chunk, rows) = match column {
            EncodedColumnView::Encoded { chunk, kind, rows } => match kind {
                EncodedKind::Rle => (chunk, *rows),
                _ => return input.clone(),
            },
            EncodedColumnView::Materialized { .. } => return input.clone(),
        };
        let matched = match parse_rle_int_runs(chunk, rows) {
            Some(runs) => select_runs_matching_i64(&runs, self.literal),
            None => return RowSelection::empty(),
        };
        // Intersect the predicate's matching runs with the input
        // selection. When both are runs, result stays as runs.
        let predicate_sel = RowSelection::from_runs(matched);
        let narrowed = RowSelection::intersect(&predicate_sel, input);
        // If the column has nulls, drop rows whose validity bit is
        // unset. This coerces to Indices when splitting runs.
        if chunk.nulls.is_some() {
            apply_null_mask(chunk, rows, &narrowed)
        } else {
            narrowed
        }
    }
}

/// Parse run_ends and i64 values for an RLE-encoded Int/Timestamp
/// column.
///
/// Returns `None` on malformed bytes (defensive — the reader already
/// validates segment integrity, but kernels prefer "no selection" over
/// panic on adversarial input).
fn parse_rle_int_runs(chunk: &PinnedChunkRef<'_>, rows: u32) -> Option<Vec<(u32, i64)>> {
    if chunk.params.len() < 4 {
        return None;
    }
    let run_count =
        u32::from_le_bytes(chunk.params[..4].try_into().ok()?) as usize;
    let needed = run_count
        .checked_mul(4)?
        .checked_add(run_count.checked_mul(8)?)?;
    if chunk.payload.len() < needed {
        return None;
    }
    let (ends_bytes, values_bytes) = chunk.payload.split_at(run_count * 4);
    let mut out = Vec::with_capacity(run_count);
    for i in 0..run_count {
        let end = u32::from_le_bytes(ends_bytes[i * 4..i * 4 + 4].try_into().ok()?);
        let val = i64::from_le_bytes(values_bytes[i * 8..i * 8 + 8].try_into().ok()?);
        out.push((end, val));
    }
    // Sanity: final run_end must equal the logical row count.
    if out.last().map(|(e, _)| *e).unwrap_or(0) != rows && run_count > 0 {
        return None;
    }
    Some(out)
}

/// Given parsed `(run_end, value)` pairs, emit the runs whose value
/// equals `literal`. Each output run is `[prev_end, run_end)`.
fn select_runs_matching_i64(runs: &[(u32, i64)], literal: i64) -> Vec<RowRun> {
    let mut out = Vec::new();
    let mut prev_end: u32 = 0;
    for &(end, val) in runs {
        if val == literal {
            out.push(RowRun {
                start: prev_end,
                len: end - prev_end,
            });
        }
        prev_end = end;
    }
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Materialized-fallback filter (CP6 Step 2 / compressed-codec fallback)
// ─────────────────────────────────────────────────────────────────────────────

/// Apply an Arrow-compute-produced boolean mask to an input selection.
///
/// The fallback path for compressed encodings that don't yet have a
/// tile-scratch kernel (Delta, DoubleDelta, BitPacking, FOR, PFOR,
/// ALP, FSST). CP2 pins those encodings as
/// [`bqlite_core::encoded::EncodedColumn::Materialized`], so the kernel
/// has a dense [`arrow::array::ArrayRef`] in hand and can call any
/// arrow-compute kernel (`eq`, `gt`, `lt_eq`, …) to produce a boolean
/// mask. This helper intersects that mask with the current selection.
///
/// Calling this on top of a per-tile decode is trivial: build the mask
/// from the tile scratch (via the same compute kernels) and feed it
/// here with the tile's global row offset — the mask indices are
/// already global because the caller constructs them that way.
///
/// # Parameters
///
/// - `mask` — a length-`row_count` boolean array. `true` rows are kept.
/// - `input` — the current [`RowSelection`].
///
/// # Semantics
///
/// Nulls in the mask are treated as `false` (SQL NULL filter semantics).
pub fn apply_materialized_mask(
    mask: &arrow::array::BooleanArray,
    input: &RowSelection,
) -> RowSelection {
    use arrow::array::Array;
    let mut out = Vec::with_capacity(input.len());
    let sv = input.as_indices();
    for &idx in sv.as_slice() {
        let i = idx as usize;
        if i >= mask.len() {
            continue;
        }
        if !mask.is_valid(i) {
            continue;
        }
        if mask.value(i) {
            out.push(idx);
        }
    }
    RowSelection::Indices(SelectionVector::from_sorted(out))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bqlite_core::encoded::{
        EncodedColumn, EncodedKind, PinnedChunk, RowRun, RowSelection, SelectionVector,
    };

    use super::*;

    fn constant_column(value: ScalarValue, rows: u32, nulls: Option<Vec<u8>>) -> EncodedColumn {
        EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(Vec::<u8>::new()),
                nulls: nulls.map(Arc::from),
                params: Arc::from(Vec::<u8>::new()),
            },
            kind: EncodedKind::Constant {
                value: Arc::new(value),
            },
            rows,
        }
    }

    #[test]
    fn constant_eq_matches_preserves_input_when_no_nulls() {
        let col = constant_column(ScalarValue::Int(7), 4, None);
        let kernel = ConstantEqKernel::new(ScalarValue::Int(7));
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 4 }]);
        let out = kernel.apply(&col.view(), &input);
        // With no nulls, the constant-matches case passes input through
        // via the null-mask helper (which coerces to Indices). The
        // logical row count must be equal.
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn constant_eq_mismatch_empties_selection() {
        let col = constant_column(ScalarValue::Int(7), 4, None);
        let kernel = ConstantEqKernel::new(ScalarValue::Int(8));
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3]));
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn constant_eq_drops_null_rows() {
        // 4 rows, bitmap = 0b1010 → rows 1 and 3 are valid, rows 0
        // and 2 are null.
        let col = constant_column(ScalarValue::Int(7), 4, Some(vec![0b1010u8]));
        let kernel = ConstantEqKernel::new(ScalarValue::Int(7));
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3]));
        let out = kernel.apply(&col.view(), &input);
        if let RowSelection::Indices(sv) = out {
            assert_eq!(sv.as_slice(), &[1, 3]);
        } else {
            panic!("expected Indices output");
        }
    }

    #[test]
    fn constant_eq_respects_input_narrowing() {
        let col = constant_column(ScalarValue::Int(7), 5, None);
        let kernel = ConstantEqKernel::new(ScalarValue::Int(7));
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 3]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn constant_eq_string_literal() {
        let col = constant_column(ScalarValue::String("u1".into()), 3, None);
        let kernel = ConstantEqKernel::new(ScalarValue::String("u1".into()));
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn wrong_encoding_passes_through_for_defense_in_depth() {
        // The kernel is written against Constant encoding. When the
        // dispatcher picks the wrong kernel, CP3 passes input through
        // rather than producing silently wrong filtering.
        let col = EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(Vec::<u8>::new()),
                nulls: None,
                params: Arc::from(Vec::<u8>::new()),
            },
            kind: EncodedKind::PlainFixed { width: 8 },
            rows: 2,
        };
        let kernel = ConstantEqKernel::new(ScalarValue::Int(1));
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 2);
    }

    fn rle_int_column(runs: &[(u32, i64)], row_count: u32, nulls: Option<Vec<u8>>) -> EncodedColumn {
        let run_count = runs.len() as u32;
        let params = run_count.to_le_bytes().to_vec();
        let mut payload = Vec::with_capacity(runs.len() * 12);
        for &(end, _) in runs {
            payload.extend_from_slice(&end.to_le_bytes());
        }
        for &(_, val) in runs {
            payload.extend_from_slice(&val.to_le_bytes());
        }
        EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(payload),
                nulls: nulls.map(Arc::from),
                params: Arc::from(params),
            },
            kind: EncodedKind::Rle,
            rows: row_count,
        }
    }

    #[test]
    fn rle_int_eq_preserves_runs_through_filter() {
        // 10 rows, runs: [A=1]x3, [A=2]x2, [A=1]x5
        //   run_ends = [3, 5, 10], values = [1, 2, 1]
        let col = rle_int_column(&[(3, 1), (5, 2), (10, 1)], 10, None);
        let kernel = RleIntEqKernel::new(1);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        match &out {
            RowSelection::Runs(runs) => {
                assert_eq!(
                    runs,
                    &vec![
                        RowRun { start: 0, len: 3 },
                        RowRun { start: 5, len: 5 },
                    ]
                );
            }
            _ => panic!("expected runs output; got indices"),
        }
        assert_eq!(out.len(), 8);
    }

    #[test]
    fn rle_int_eq_no_match_empty() {
        let col = rle_int_column(&[(3, 1), (5, 2), (10, 1)], 10, None);
        let kernel = RleIntEqKernel::new(99);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn rle_int_eq_narrows_with_input_runs() {
        let col = rle_int_column(&[(3, 1), (5, 2), (10, 1)], 10, None);
        let kernel = RleIntEqKernel::new(1);
        // Input restricts to rows [2..7)
        let input = RowSelection::from_runs(vec![RowRun { start: 2, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        match &out {
            RowSelection::Runs(runs) => {
                // Matches for value=1: rows [0..3) ∩ [2..7) = [2..3),
                // and rows [5..10) ∩ [2..7) = [5..7).
                assert_eq!(
                    runs,
                    &vec![
                        RowRun { start: 2, len: 1 },
                        RowRun { start: 5, len: 2 },
                    ]
                );
            }
            _ => panic!("expected runs"),
        }
    }

    #[test]
    fn rle_int_eq_null_bitmap_drops_null_rows() {
        // 10 rows, runs: value 1 for rows 0..3, 2 for 3..5, 1 for 5..10.
        // Nulls: rows 1 and 6 are null. Bitmap = 0b1111_1101_1111_1101 =
        // bytes [0xFD, 0xBF]  (LSB-first: byte0 bit1 = 0, byte1 bit(6-8)=bit-2 off = bit 6 off).
        //   row 0: bit 0 of byte 0 → 1
        //   row 1: bit 1 of byte 0 → 0 (null)
        //   row 2-7: bits 2..8 (byte 0 bits 2-7 plus byte 1 bit 0) → but
        //     row 6 → bit 6 of byte 0 → 0 (null)
        //   rows 3,4,5,7,8,9 → valid
        // byte0 = 0b1011_1101 = 0xBD; byte1 = 0b0000_0011 = 0x03
        let col = rle_int_column(
            &[(3, 1), (5, 2), (10, 1)],
            10,
            Some(vec![0xBDu8, 0x03u8]),
        );
        let kernel = RleIntEqKernel::new(1);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        // value-1 runs cover rows {0,1,2, 5,6,7,8,9}. After null drop:
        // exclude row 1 (null) and row 6 (null) → {0,2, 5,7,8,9}.
        let sv = out.as_indices();
        assert_eq!(sv.as_slice(), &[0, 2, 5, 7, 8, 9]);
    }

    #[test]
    fn rle_int_eq_wrong_encoding_passes_through() {
        let col = EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(Vec::<u8>::new()),
                nulls: None,
                params: Arc::from(Vec::<u8>::new()),
            },
            kind: EncodedKind::PlainFixed { width: 8 },
            rows: 2,
        };
        let kernel = RleIntEqKernel::new(42);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn apply_materialized_mask_narrows_selection() {
        use arrow::array::BooleanArray;
        let mask = BooleanArray::from(vec![true, false, true, true, false]);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3, 4]));
        let out = apply_materialized_mask(&mask, &input);
        let sv = out.as_indices();
        assert_eq!(sv.as_slice(), &[0, 2, 3]);
    }

    #[test]
    fn apply_materialized_mask_respects_input_narrowing() {
        use arrow::array::BooleanArray;
        let mask = BooleanArray::from(vec![true, true, true, true, true]);
        // Input already restricts to rows 1 and 3.
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 3]));
        let out = apply_materialized_mask(&mask, &input);
        assert_eq!(out.as_indices().as_slice(), &[1, 3]);
    }

    #[test]
    fn apply_materialized_mask_drops_null_rows() {
        use arrow::array::BooleanArray;
        // Mask with a null in position 2.
        let mask = BooleanArray::from(vec![Some(true), Some(true), None, Some(true)]);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 4 }]);
        let out = apply_materialized_mask(&mask, &input);
        assert_eq!(out.as_indices().as_slice(), &[0, 1, 3]);
    }

    #[test]
    fn materialized_column_passes_through() {
        use arrow::array::Int64Array;
        use bqlite_core::encoded::EncodedColumn;
        let col = EncodedColumn::Materialized {
            array: Arc::new(Int64Array::from(vec![1i64, 2, 3])),
            rows: 3,
        };
        let kernel = ConstantEqKernel::new(ScalarValue::Int(1));
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2]));
        let out = kernel.apply(&col.view(), &input);
        // CP3 path-through for materialized fallback — dispatcher
        // handles this case in CP7.
        assert_eq!(out.len(), 3);
    }
}
