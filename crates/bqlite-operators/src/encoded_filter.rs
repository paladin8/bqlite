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
    EncodedBatch, EncodedColumn, EncodedColumnView, EncodedKind, PinnedChunkRef, RowRun,
    RowSelection, SelectionVector,
};
use bqlite_core::scalar::ScalarValue;
use bqlite_core::{BqlType, PropertyValue, Result};
use bqlite_planner::compiled::{CompareOp, CompiledExpr, CompiledNode};

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

/// Kernel for `constant_column IN (literals…)` (single-value is
/// `IN (literal,)`).
///
/// The kernel surface is a slice of literals so every Eq kernel can be
/// reused for `IN (…)` shapes once the recognizer learns to lower them.
/// For Constant encoding the check is cheap either way: one equality
/// per literal against the pinned scalar value — if any matches, the
/// input passes through (modulo nulls); otherwise the selection is
/// empty.
pub struct ConstantEqKernel {
    pub literals: Vec<ScalarValue>,
}

impl ConstantEqKernel {
    pub fn new(literals: Vec<ScalarValue>) -> Self {
        Self { literals }
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
                return input.clone();
            }
        };
        let pinned = match kind {
            EncodedKind::Constant { value } => value.as_ref(),
            _ => {
                // Wrong kernel for this encoding; caller's dispatch is a
                // bug. We pass input through rather than panic so the
                // outer filter still drops non-matching rows at
                // materialization. Future CPs may promote to
                // debug_assert.
                return input.clone();
            }
        };
        if !self.literals.iter().any(|lit| pinned == lit) {
            return RowSelection::empty();
        }
        // The pinned value equals one of the literals at every row.
        // Null handling drops any row whose bit is unset in the
        // column's null bitmap (if any).
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
fn apply_null_mask(chunk: &PinnedChunkRef<'_>, rows: u32, input: &RowSelection) -> RowSelection {
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
    pub literals: Vec<i64>,
}

impl RleIntEqKernel {
    pub fn new(literals: Vec<i64>) -> Self {
        Self { literals }
    }
}

impl EncodedPredicateKernel for RleIntEqKernel {
    fn apply(&self, column: &EncodedColumnView<'_>, input: &RowSelection) -> RowSelection {
        let (chunk, logical_rows) = match column {
            EncodedColumnView::Encoded { chunk, kind, rows } => match kind {
                EncodedKind::Rle => (chunk, *rows),
                _ => return input.clone(),
            },
            EncodedColumnView::Materialized { .. } => return input.clone(),
        };
        if self.literals.is_empty() {
            return RowSelection::empty();
        }
        // RLE runs index into the dense (non-null) value stream, not
        // into logical rows. For non-nullable columns those spaces
        // coincide; for nullable columns the kernel must translate
        // via the validity bitmap.
        let runs = match parse_rle_int_runs(chunk) {
            Some(r) => r,
            None => return RowSelection::empty(),
        };
        let matches = |v: i64| self.literals.contains(&v);
        match chunk.nulls {
            None => {
                // Non-nullable: run_ends are logical row indices. Fast
                // run-preserving path.
                let matched = select_runs_matching_i64(&runs, &matches);
                let predicate_sel = RowSelection::from_runs(matched);
                RowSelection::intersect(&predicate_sel, input)
            }
            Some(bitmap) => {
                // Nullable: translate non-null-space runs to logical
                // row indices via a single bitmap pass. Output is
                // Indices since interspersed nulls break run shape.
                let matched_logical =
                    translate_runs_through_bitmap(&runs, &matches, bitmap, logical_rows);
                let predicate_sel =
                    RowSelection::Indices(SelectionVector::from_sorted(matched_logical));
                RowSelection::intersect(&predicate_sel, input)
            }
        }
    }
}

/// Parse run_ends and i64 values for an RLE-encoded Int/Timestamp
/// column.
///
/// `run_end[i]` indexes into the non-null value stream (matches the
/// writer in `bqlite-storage/src/encoding/rle.rs`). The final
/// `run_end` equals the non-null value count, which is `logical_rows`
/// for non-nullable columns and `popcount(nulls_bitmap)` otherwise.
///
/// Returns `None` on malformed bytes (defensive — the reader already
/// validates segment integrity, but kernels prefer "no selection" over
/// panic on adversarial input).
fn parse_rle_int_runs(chunk: &PinnedChunkRef<'_>) -> Option<Vec<(u32, i64)>> {
    if chunk.params.len() < 4 {
        return None;
    }
    let run_count = u32::from_le_bytes(chunk.params[..4].try_into().ok()?) as usize;
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
    Some(out)
}

/// Walk the validity bitmap in parallel with the RLE runs, emitting
/// logical row indices for every non-null value whose run matches
/// `literal`.
///
/// Runs must be sorted by `run_end` ascending (the encoder guarantees
/// this). The returned index vector is sorted ascending by
/// construction.
fn translate_runs_through_bitmap(
    runs: &[(u32, i64)],
    matches: &impl Fn(i64) -> bool,
    bitmap: &[u8],
    logical_rows: u32,
) -> Vec<u32> {
    let mut out = Vec::new();
    let mut non_null_pos: u32 = 0;
    let mut run_idx = 0usize;
    for logical in 0..logical_rows {
        if !bit_is_set(bitmap, logical as usize) {
            continue;
        }
        // Advance run_idx to the run whose half-open range
        // `[prev_end, run_end)` contains `non_null_pos`.
        while run_idx < runs.len() && runs[run_idx].0 <= non_null_pos {
            run_idx += 1;
        }
        if run_idx < runs.len() && matches(runs[run_idx].1) {
            out.push(logical);
        }
        non_null_pos += 1;
    }
    out
}

/// Given parsed `(run_end, value)` pairs, emit the runs whose value
/// satisfies `matches`. Each output run is `[prev_end, run_end)`.
/// Adjacent matching runs are merged so the output keeps
/// [`RowSelection::from_runs`]'s non-overlapping invariant tight.
fn select_runs_matching_i64(runs: &[(u32, i64)], matches: &impl Fn(i64) -> bool) -> Vec<RowRun> {
    let mut out: Vec<RowRun> = Vec::new();
    let mut prev_end: u32 = 0;
    for &(end, val) in runs {
        if matches(val) {
            if let Some(last) = out.last_mut() {
                if last.start + last.len == prev_end {
                    last.len = end - last.start;
                    prev_end = end;
                    continue;
                }
            }
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
// DictionaryEqKernel — equality on dictionary-encoded Int/String columns
// ─────────────────────────────────────────────────────────────────────────────

/// Kernel for `dictionary_column IN (literals…)` (single-value is
/// `IN (literal,)`).
///
/// This is the win-case for low-cardinality string columns like
/// `event_type`, `country`, `os`: binary-search each literal in the
/// sorted segment-level dictionary to get a small set of target codes,
/// then scan the bit-packed code stream once, emitting indices whose
/// code is in the target set. No per-row string comparison, no column
/// materialization.
///
/// Literals that don't appear in the dictionary are silently dropped
/// (they contribute zero rows). If every literal misses, the kernel
/// short-circuits to an empty selection without touching the payload.
///
/// # On-disk layout consumed
///
/// - `kind = EncodedKind::Dictionary { values }` where `values` is the
///   raw dict region (Int: `n × i64 LE`; String: `[u32 LE length]
///   [utf8 bytes]…`). Parsed once per apply call via the internal
///   helpers below.
/// - `chunk.payload` = bit-packed code stream.
/// - `chunk.params` = 5 bytes: `dict_id u32 LE + code_bit_width u8`.
///   Only `code_bit_width` (byte 4) matters here — `dict_id` was
///   already resolved at pin time into `values`.
///
/// Null handling: rows whose validity bit is unset are dropped after
/// code matching via the shared `apply_null_mask`-style logic.
pub struct DictionaryEqKernel {
    pub literals: Vec<ScalarValue>,
    pub col_type: BqlType,
}

impl DictionaryEqKernel {
    pub fn new(literals: Vec<ScalarValue>, col_type: BqlType) -> Self {
        Self { literals, col_type }
    }
}

impl EncodedPredicateKernel for DictionaryEqKernel {
    fn apply(&self, column: &EncodedColumnView<'_>, input: &RowSelection) -> RowSelection {
        let (chunk, values, rows) = match column {
            EncodedColumnView::Encoded { chunk, kind, rows } => match kind {
                EncodedKind::Dictionary { values } => (chunk, values.as_ref(), *rows),
                _ => return input.clone(),
            },
            EncodedColumnView::Materialized { .. } => return input.clone(),
        };
        if self.literals.is_empty() {
            return RowSelection::empty();
        }
        // `chunk.params` is the 5-byte on-disk Dictionary param block
        // (`dict_id u32 LE + code_bit_width u8`). Defensive: refuse to
        // proceed on a malformed params block — the reader already
        // validated this at pin time.
        if chunk.params.len() != 5 {
            return RowSelection::empty();
        }
        let code_bit_width = chunk.params[4];

        // Binary-search each literal in the sorted dictionary.
        // Dedup so `IN (20, 20, 30)` doesn't inflate `target_codes.len()`
        // past the real hit set — the fast path below compares
        // `target_codes.len() >= cardinality`, which would otherwise
        // fire spuriously on duplicates and select non-matching rows.
        let mut target_codes = match lookup_literal_codes(values, &self.col_type, &self.literals) {
            Ok(codes) => codes,
            // Type mismatch is a dispatcher bug. Fail closed (empty)
            // rather than pass input through — the contract forbids
            // widening the live row set, and the caller does not
            // re-apply the predicate on the materialized side.
            Err(()) => return RowSelection::empty(),
        };
        target_codes.sort_unstable();
        target_codes.dedup();
        if target_codes.is_empty() {
            // Every literal missed the dictionary — no row can match.
            return RowSelection::empty();
        }

        // Fast path: if every dict entry matches (cardinality ≤ hits),
        // the predicate is trivially true for every non-null row.
        // Skip the code-stream scan.
        let cardinality = dictionary_cardinality(values, &self.col_type);
        if let Some(card) = cardinality {
            if target_codes.len() >= card {
                return narrow_by_nulls(chunk.nulls, rows, input);
            }
        }

        // Decode the code stream. CP3's first-pass kernel materializes
        // codes into a Vec<u32>; the bit-level direct scan is a
        // follow-up optimization.
        //
        // `row_count` in the dict payload is the *non-null* row count
        // — for non-nullable columns this equals `rows`; for nullable
        // columns it equals `popcount(nulls_bitmap)`.
        let nonnull_row_count = match chunk.nulls {
            None => rows as usize,
            Some(bitmap) => popcount_bitmap(bitmap, rows as usize),
        };
        let codes = match bqlite_storage::encoding::dictionary::unpack_codes(
            chunk.payload,
            nonnull_row_count,
            code_bit_width,
        ) {
            Ok(c) => c,
            Err(_) => return RowSelection::empty(),
        };

        // Emit logical row indices where code ∈ target_codes.
        let is_target = |c: u32| target_codes.contains(&c);
        let matched_logical: Vec<u32> = match chunk.nulls {
            None => codes
                .iter()
                .enumerate()
                .filter_map(|(i, &c)| if is_target(c) { Some(i as u32) } else { None })
                .collect(),
            Some(bitmap) => {
                let mut out = Vec::new();
                let mut dense_pos: usize = 0;
                for logical in 0..rows {
                    if !bit_is_set(bitmap, logical as usize) {
                        continue;
                    }
                    if dense_pos < codes.len() && is_target(codes[dense_pos]) {
                        out.push(logical);
                    }
                    dense_pos += 1;
                }
                out
            }
        };
        let predicate_sel = RowSelection::Indices(SelectionVector::from_sorted(matched_logical));
        RowSelection::intersect(&predicate_sel, input)
    }
}

/// Binary-search each literal in the sorted dictionary region, returning
/// the codes for every literal that's present. `Err(())` signals a
/// type mismatch between the literal and the dictionary's logical type
/// — a dispatcher bug rather than user data.
fn lookup_literal_codes(
    values: &[u8],
    ty: &BqlType,
    literals: &[ScalarValue],
) -> std::result::Result<Vec<u32>, ()> {
    match ty {
        BqlType::Int | BqlType::Timestamp => {
            // Both Int and Timestamp dict regions are `n × i64 LE`.
            let dict = match parse_int_dict_bytes(values) {
                Some(d) => d,
                None => return Ok(Vec::new()),
            };
            let mut out = Vec::with_capacity(literals.len());
            for lit in literals {
                let needle = match (lit, ty) {
                    (ScalarValue::Int(v), _) | (ScalarValue::Timestamp(v), _) => *v,
                    _ => return Err(()),
                };
                if let Ok(idx) = dict.binary_search(&needle) {
                    out.push(idx as u32);
                }
            }
            Ok(out)
        }
        BqlType::String => {
            let dict = match parse_string_dict_bytes(values) {
                Some(d) => d,
                None => return Ok(Vec::new()),
            };
            let mut out = Vec::with_capacity(literals.len());
            for lit in literals {
                let needle: &str = match lit {
                    ScalarValue::String(s) => s.as_str(),
                    _ => return Err(()),
                };
                if let Ok(idx) = dict.binary_search_by(|probe| (*probe).cmp(needle)) {
                    out.push(idx as u32);
                }
            }
            Ok(out)
        }
        // Dictionary encoding only covers Int and String (§9.2); the
        // Timestamp arm above mirrors Int for parity with
        // `recognize_encoded_eq`, which accepts Timestamp literals as
        // i64 nanoseconds.
        _ => Err(()),
    }
}

fn dictionary_cardinality(values: &[u8], ty: &BqlType) -> Option<usize> {
    match ty {
        BqlType::Int | BqlType::Timestamp => {
            if !values.len().is_multiple_of(8) {
                return None;
            }
            Some(values.len() / 8)
        }
        BqlType::String => {
            // Walk the region once to count; cheap since dicts are small.
            let mut offset = 0usize;
            let mut count = 0usize;
            while offset < values.len() {
                if offset + 4 > values.len() {
                    return None;
                }
                let len = u32::from_le_bytes(values[offset..offset + 4].try_into().ok()?) as usize;
                offset += 4;
                if offset + len > values.len() {
                    return None;
                }
                offset += len;
                count += 1;
            }
            Some(count)
        }
        _ => None,
    }
}

fn parse_int_dict_bytes(bytes: &[u8]) -> Option<Vec<i64>> {
    if !bytes.len().is_multiple_of(8) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 8);
    for chunk in bytes.chunks_exact(8) {
        out.push(i64::from_le_bytes(chunk.try_into().ok()?));
    }
    Some(out)
}

fn parse_string_dict_bytes(bytes: &[u8]) -> Option<Vec<&str>> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if offset + 4 > bytes.len() {
            return None;
        }
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;
        if offset + len > bytes.len() {
            return None;
        }
        let s = std::str::from_utf8(&bytes[offset..offset + len]).ok()?;
        offset += len;
        out.push(s);
    }
    Some(out)
}

/// Count set bits in the first `bit_count` bits of `bitmap` (LSB-first).
fn popcount_bitmap(bitmap: &[u8], bit_count: usize) -> usize {
    let full_bytes = bit_count / 8;
    let tail_bits = bit_count % 8;
    let mut count = 0usize;
    for b in &bitmap[..full_bytes.min(bitmap.len())] {
        count += b.count_ones() as usize;
    }
    if tail_bits != 0 && full_bytes < bitmap.len() {
        let mask = (1u8 << tail_bits) - 1;
        count += (bitmap[full_bytes] & mask).count_ones() as usize;
    }
    count
}

/// Drop rows whose validity bit is unset. Used by the "dictionary has
/// a matching code for every row" fast path.
fn narrow_by_nulls(nulls: Option<&[u8]>, rows: u32, input: &RowSelection) -> RowSelection {
    let Some(bitmap) = nulls else {
        return input.clone();
    };
    let mut out = Vec::with_capacity(input.len());
    for &idx in input.as_indices().as_slice() {
        if idx >= rows {
            continue;
        }
        if bit_is_set(bitmap, idx as usize) {
            out.push(idx);
        }
    }
    RowSelection::Indices(SelectionVector::from_sorted(out))
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
// Shape recognition + dispatch (scan-operator entry point)
// ─────────────────────────────────────────────────────────────────────────────

/// Recognized shape: `column == literal` (commutative). The column is
/// identified by its table-schema ordinal — which coincides with the
/// position inside [`EncodedBatch::columns`] when the scan's projection
/// preserves table-schema order (the scan operator enforces this).
#[derive(Debug, Clone)]
pub struct EncodedEqShape {
    pub col_index: usize,
    pub literal: PropertyValue,
}

/// Try to match a compiled predicate against the shape the encoded path
/// can dispatch on today: a non-null equality between a column and a
/// literal. Returns `None` for every other shape — the caller keeps
/// those predicates in its residual post-filter list and enforces them
/// on the materialized batch via arrow-compute.
pub fn recognize_encoded_eq(expr: &CompiledExpr) -> Option<EncodedEqShape> {
    let CompiledNode::Compare {
        op: CompareOp::Equal,
        left,
        right,
        ..
    } = &expr.node
    else {
        return None;
    };
    let (col_index, literal) = match (&left.node, &right.node) {
        (CompiledNode::Column { index, .. }, CompiledNode::Literal(v)) => (*index, v.clone()),
        (CompiledNode::Literal(v), CompiledNode::Column { index, .. }) => (*index, v.clone()),
        _ => return None,
    };
    // `NULL = x` is SQL UNKNOWN — leave it to post-filter 3VL.
    if matches!(literal, PropertyValue::Null) {
        return None;
    }
    Some(EncodedEqShape { col_index, literal })
}

/// Partition a list of post-filter predicates into (encoded-eligible,
/// residual). Every entry in `residual` still needs arrow-compute
/// evaluation after materialization.
pub fn partition_encoded_eq(
    predicates: &[CompiledExpr],
) -> (Vec<EncodedEqShape>, Vec<CompiledExpr>) {
    let mut shapes = Vec::new();
    let mut residual = Vec::new();
    for p in predicates {
        match recognize_encoded_eq(p) {
            Some(s) => shapes.push(s),
            None => residual.push(p.clone()),
        }
    }
    (shapes, residual)
}

/// Apply one recognized `col == literal` predicate to an
/// [`EncodedBatch`] and return a narrowed [`RowSelection`].
///
/// Dispatch:
///
/// - `Constant` → [`ConstantEqKernel`] (cheapest path).
/// - `Rle` with Int/Timestamp literal → [`RleIntEqKernel`] (run-
///   preserving).
/// - Anything else (other encodings, or `Materialized` fallback from
///   `pin_column_chunk`) → materialize just that column and run
///   arrow-compute `eq` + [`apply_materialized_mask`]. This is the
///   same fallback path the bench measures under `realistic_rg`.
pub fn apply_encoded_eq(
    shape: &EncodedEqShape,
    batch: &EncodedBatch,
    input: &RowSelection,
    col_type: &BqlType,
) -> Result<RowSelection> {
    let col = &batch.columns[shape.col_index];
    let scalar = match property_value_to_scalar(&shape.literal) {
        Some(s) => s,
        // Unrepresentable literal (e.g. List/Map) — fall back to the
        // arrow-compute path, which either succeeds via broadcast or
        // returns an Execution error that the caller surfaces.
        None => return apply_fallback_eq(col, &shape.literal, input, col_type),
    };
    match col.view() {
        EncodedColumnView::Encoded { kind, .. } => match kind {
            EncodedKind::Constant { .. } => {
                let kernel = ConstantEqKernel::new(vec![scalar]);
                Ok(kernel.apply(&col.view(), input))
            }
            EncodedKind::Rle => match &shape.literal {
                PropertyValue::Int(v) | PropertyValue::Timestamp(v) => {
                    let kernel = RleIntEqKernel::new(vec![*v]);
                    Ok(kernel.apply(&col.view(), input))
                }
                _ => apply_fallback_eq(col, &shape.literal, input, col_type),
            },
            EncodedKind::Dictionary { .. } => {
                let kernel = DictionaryEqKernel::new(vec![scalar], col_type.clone());
                Ok(kernel.apply(&col.view(), input))
            }
            _ => apply_fallback_eq(col, &shape.literal, input, col_type),
        },
        EncodedColumnView::Materialized { .. } => {
            apply_fallback_eq(col, &shape.literal, input, col_type)
        }
    }
}

fn apply_fallback_eq(
    col: &EncodedColumn,
    literal: &PropertyValue,
    input: &RowSelection,
    col_type: &BqlType,
) -> Result<RowSelection> {
    use arrow::array::{Array, BooleanArray};
    use arrow::compute::kernels::cmp;
    let dense = bqlite_storage::materialize_encoded_column(col, col_type)?;
    let lit_arr = broadcast_literal_one(literal, col_type)?;
    let lit = arrow::array::Scalar::new(lit_arr);
    let mask = cmp::eq(&dense.as_ref(), &lit).map_err(|e| {
        bqlite_core::BqliteError::Execution(format!(
            "encoded fallback: arrow::compute::eq failed: {e}"
        ))
    })?;
    let mask_bool = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            bqlite_core::BqliteError::Execution(
                "encoded fallback: eq did not return BooleanArray".into(),
            )
        })?;
    Ok(apply_materialized_mask(mask_bool, input))
}

fn broadcast_literal_one(value: &PropertyValue, ty: &BqlType) -> Result<arrow::array::ArrayRef> {
    use arrow::array::{
        BooleanArray, Float64Array, Int64Array, StringViewArray, TimestampNanosecondArray,
    };
    use std::sync::Arc;
    match (value, ty) {
        (PropertyValue::Bool(b), BqlType::Bool) => {
            Ok(Arc::new(BooleanArray::from(vec![*b])) as arrow::array::ArrayRef)
        }
        (PropertyValue::Int(i), BqlType::Int) => {
            Ok(Arc::new(Int64Array::from(vec![*i])) as arrow::array::ArrayRef)
        }
        (PropertyValue::Int(i), BqlType::Timestamp) => Ok(Arc::new(
            TimestampNanosecondArray::from(vec![*i]).with_timezone("UTC"),
        ) as arrow::array::ArrayRef),
        (PropertyValue::Int(i), BqlType::Float) => {
            Ok(Arc::new(Float64Array::from(vec![*i as f64])) as arrow::array::ArrayRef)
        }
        (PropertyValue::Float(f), BqlType::Float) => {
            Ok(Arc::new(Float64Array::from(vec![*f])) as arrow::array::ArrayRef)
        }
        (PropertyValue::String(s), BqlType::String) => {
            Ok(Arc::new(StringViewArray::from(vec![s.as_str()])) as arrow::array::ArrayRef)
        }
        (PropertyValue::Timestamp(t), BqlType::Timestamp) => Ok(Arc::new(
            TimestampNanosecondArray::from(vec![*t]).with_timezone("UTC"),
        )
            as arrow::array::ArrayRef),
        (v, t) => Err(bqlite_core::BqliteError::Execution(format!(
            "encoded fallback: unsupported literal/type pair: {v:?} as {t}"
        ))),
    }
}

fn property_value_to_scalar(v: &PropertyValue) -> Option<ScalarValue> {
    match v {
        PropertyValue::Null => None,
        PropertyValue::Bool(b) => Some(ScalarValue::Bool(*b)),
        PropertyValue::Int(i) => Some(ScalarValue::Int(*i)),
        PropertyValue::Float(f) => Some(ScalarValue::Float(*f)),
        PropertyValue::String(s) => Some(ScalarValue::String(s.clone())),
        PropertyValue::Timestamp(t) => Some(ScalarValue::Timestamp(*t)),
        PropertyValue::List(_) | PropertyValue::Map(_) => None,
    }
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
        let kernel = ConstantEqKernel::new(vec![ScalarValue::Int(7)]);
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
        let kernel = ConstantEqKernel::new(vec![ScalarValue::Int(8)]);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2, 3]));
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn constant_eq_drops_null_rows() {
        // 4 rows, bitmap = 0b1010 → rows 1 and 3 are valid, rows 0
        // and 2 are null.
        let col = constant_column(ScalarValue::Int(7), 4, Some(vec![0b1010u8]));
        let kernel = ConstantEqKernel::new(vec![ScalarValue::Int(7)]);
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
        let kernel = ConstantEqKernel::new(vec![ScalarValue::Int(7)]);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![1, 3]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn constant_eq_string_literal() {
        let col = constant_column(ScalarValue::String("u1".into()), 3, None);
        let kernel = ConstantEqKernel::new(vec![ScalarValue::String("u1".into())]);
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
        let kernel = ConstantEqKernel::new(vec![ScalarValue::Int(1)]);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 2);
    }

    fn rle_int_column(
        runs: &[(u32, i64)],
        row_count: u32,
        nulls: Option<Vec<u8>>,
    ) -> EncodedColumn {
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
        let kernel = RleIntEqKernel::new(vec![1]);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        match &out {
            RowSelection::Runs(runs) => {
                assert_eq!(
                    runs,
                    &vec![RowRun { start: 0, len: 3 }, RowRun { start: 5, len: 5 },]
                );
            }
            _ => panic!("expected runs output; got indices"),
        }
        assert_eq!(out.len(), 8);
    }

    #[test]
    fn rle_int_eq_no_match_empty() {
        let col = rle_int_column(&[(3, 1), (5, 2), (10, 1)], 10, None);
        let kernel = RleIntEqKernel::new(vec![99]);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn rle_int_eq_narrows_with_input_runs() {
        let col = rle_int_column(&[(3, 1), (5, 2), (10, 1)], 10, None);
        let kernel = RleIntEqKernel::new(vec![1]);
        // Input restricts to rows [2..7)
        let input = RowSelection::from_runs(vec![RowRun { start: 2, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        match &out {
            RowSelection::Runs(runs) => {
                // Matches for value=1: rows [0..3) ∩ [2..7) = [2..3),
                // and rows [5..10) ∩ [2..7) = [5..7).
                assert_eq!(
                    runs,
                    &vec![RowRun { start: 2, len: 1 }, RowRun { start: 5, len: 2 },]
                );
            }
            _ => panic!("expected runs"),
        }
    }

    #[test]
    fn rle_int_eq_null_bitmap_translates_nonnull_runs_to_logical_indices() {
        // 10 logical rows, 2 nulls at rows 1 and 6 → 8 non-null values.
        // Null bitmap LSB-first: bits 0,2,3,4,5,7,8,9 set.
        //   byte0 = 0b10111101 = 0xBD (rows 0..=7)
        //   byte1 = 0b00000011 = 0x03 (rows 8..=9)
        //
        // Dense (non-null) value stream: [1,1,1, 2,2, 1,1,1]
        //   run_ends (non-null space) = [3, 5, 8], values = [1, 2, 1].
        //
        // Logical → non-null position mapping (bitmap walk):
        //   0→0, 2→1, 3→2, 4→3, 5→4, 7→5, 8→6, 9→7.
        //
        // Filter value=1: non-null positions {0,1,2, 5,6,7}. Through
        // the mapping: logical {0, 2, 3, 7, 8, 9}.
        let col = rle_int_column(&[(3, 1), (5, 2), (8, 1)], 10, Some(vec![0xBDu8, 0x03u8]));
        let kernel = RleIntEqKernel::new(vec![1]);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        let sv = out.as_indices();
        assert_eq!(sv.as_slice(), &[0, 2, 3, 7, 8, 9]);
    }

    #[test]
    fn rle_int_eq_null_bitmap_intersects_with_input() {
        // Same column as the previous test; narrow the input to
        // [2..8). Expected matches: logical {2, 3, 7}.
        let col = rle_int_column(&[(3, 1), (5, 2), (8, 1)], 10, Some(vec![0xBDu8, 0x03u8]));
        let kernel = RleIntEqKernel::new(vec![1]);
        let input = RowSelection::from_runs(vec![RowRun { start: 2, len: 6 }]);
        let out = kernel.apply(&col.view(), &input);
        let sv = out.as_indices();
        assert_eq!(sv.as_slice(), &[2, 3, 7]);
    }

    /// Differential test — drives the kernel through real RLE bytes
    /// produced by `bqlite_storage::Rle.encode`. Exercises the
    /// pin-time contract that `rows` is the *logical* row count when
    /// the column has nulls (not the encoder's non-null value count).
    ///
    /// Setup: 10 logical rows, nulls at rows 1 and 6. Dense non-null
    /// values [1,1,1, 2,2, 1,1,1] — which RLE compresses into
    /// run_ends [3,5,8], values [1,2,1]. The kernel must translate
    /// non-null-space runs to logical row indices {0,2,3,7,8,9} for
    /// literal=1.
    #[test]
    fn rle_int_eq_real_rle_bytes_with_nulls() {
        use arrow::array::Int64Array;
        use bqlite_storage::{Encoding, Rle};

        // Dense non-null values as the writer's RLE encoder sees them.
        let dense = Int64Array::from(vec![1, 1, 1, 2, 2, 1, 1, 1]);
        let chunk = Rle.encode(&dense).expect("rle encode");

        // Rebuild a PinnedChunk from the real on-disk bytes. `rows` on
        // EncodedColumn::Encoded is the *logical* row count — 10, not
        // the encoded 8.
        let pinned = PinnedChunk {
            payload: Arc::from(chunk.payload),
            nulls: Some(Arc::from(vec![0xBDu8, 0x03u8])),
            params: Arc::from(chunk.params),
        };
        let col = EncodedColumn::Encoded {
            chunk: pinned,
            kind: EncodedKind::Rle,
            rows: 10,
        };

        let kernel = RleIntEqKernel::new(vec![1]);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[0, 2, 3, 7, 8, 9]);
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
        let kernel = RleIntEqKernel::new(vec![42]);
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
        let kernel = ConstantEqKernel::new(vec![ScalarValue::Int(1)]);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1, 2]));
        let out = kernel.apply(&col.view(), &input);
        // CP3 path-through for materialized fallback — dispatcher
        // handles this case in CP7.
        assert_eq!(out.len(), 3);
    }

    // ─────────────────────────────────────────────────────────────────────
    // DictionaryEqKernel tests
    // ─────────────────────────────────────────────────────────────────────

    /// LSB-first bit packer matching the on-disk code stream layout.
    /// Kept local to the tests module; the storage-side packer is
    /// crate-private (the decode helpers are pub, the encode path
    /// flows through `Dictionary::encode` with its own params layout
    /// that doesn't match the on-disk 5-byte chunk params shape).
    fn pack_codes_lsb(codes: &[u32], bit_width: u8) -> Vec<u8> {
        let byte_len = (codes.len() * bit_width as usize).div_ceil(8);
        // Pad a full 8 bytes past the end so `unpack_codes` can do
        // aligned reads without tripping bounds checks.
        let mut out = vec![0u8; byte_len + 8];
        let mut bit_pos: usize = 0;
        for &code in codes {
            let mut value = code as u64;
            let mut remaining = bit_width as usize;
            let mut byte_idx = bit_pos / 8;
            let mut bit_in_byte = bit_pos % 8;
            while remaining > 0 {
                let free = 8 - bit_in_byte;
                let take = remaining.min(free);
                let chunk = (value & ((1u64 << take) - 1)) as u8;
                out[byte_idx] |= chunk << bit_in_byte;
                value >>= take;
                remaining -= take;
                bit_in_byte += take;
                if bit_in_byte == 8 {
                    bit_in_byte = 0;
                    byte_idx += 1;
                }
            }
            bit_pos += bit_width as usize;
        }
        out
    }

    fn int_dict_bytes(values: &[i64]) -> Vec<u8> {
        let mut out = Vec::with_capacity(values.len() * 8);
        for v in values {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    fn string_dict_bytes(values: &[&str]) -> Vec<u8> {
        let mut out = Vec::new();
        for s in values {
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        out
    }

    /// Build a dictionary-encoded column with the on-disk chunk shape
    /// the kernel consumes: `params` is the 5-byte `dict_id u32 LE +
    /// code_bit_width u8` block, `values` is the raw dict region
    /// (Int: n × i64 LE; String: [u32 LE length][bytes]…).
    fn dict_column(
        codes: &[u32],
        bit_width: u8,
        values: Vec<u8>,
        logical_rows: u32,
        nulls: Option<Vec<u8>>,
    ) -> EncodedColumn {
        let mut params = Vec::with_capacity(5);
        params.extend_from_slice(&0u32.to_le_bytes()); // dict_id
        params.push(bit_width);
        EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(pack_codes_lsb(codes, bit_width)),
                nulls: nulls.map(Arc::from),
                params: Arc::from(params),
            },
            kind: EncodedKind::Dictionary {
                values: Arc::from(values),
            },
            rows: logical_rows,
        }
    }

    #[test]
    fn dictionary_eq_int_hit_returns_matching_rows() {
        // Dict [10, 20, 30]. Rows values [10, 20, 30, 20, 10] →
        // codes [0, 1, 2, 1, 0] at 2-bit width. Filter == 20 → code 1
        // → rows {1, 3}.
        let col = dict_column(&[0, 1, 2, 1, 0], 2, int_dict_bytes(&[10, 20, 30]), 5, None);
        let kernel = DictionaryEqKernel::new(vec![ScalarValue::Int(20)], BqlType::Int);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[1, 3]);
    }

    #[test]
    fn dictionary_eq_int_literal_not_in_dict_returns_empty() {
        // Literal 99 binary-searches miss → `target_codes` empty →
        // kernel short-circuits without scanning the code stream.
        let col = dict_column(&[0, 1, 2, 1, 0], 2, int_dict_bytes(&[10, 20, 30]), 5, None);
        let kernel = DictionaryEqKernel::new(vec![ScalarValue::Int(99)], BqlType::Int);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn dictionary_eq_multi_value_in_list_hits_any() {
        // IN (10, 30) against dict [10, 20, 30] → target codes {0, 2}.
        // Values [10, 20, 30, 20, 10] → match rows {0, 2, 4}.
        let col = dict_column(&[0, 1, 2, 1, 0], 2, int_dict_bytes(&[10, 20, 30]), 5, None);
        let kernel = DictionaryEqKernel::new(
            vec![ScalarValue::Int(10), ScalarValue::Int(30)],
            BqlType::Int,
        );
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[0, 2, 4]);
    }

    #[test]
    fn dictionary_eq_multi_value_mix_of_hit_and_miss() {
        // IN (20, 99) — 99 misses, 20 hits code 1 → rows {1, 3}.
        let col = dict_column(&[0, 1, 2, 1, 0], 2, int_dict_bytes(&[10, 20, 30]), 5, None);
        let kernel = DictionaryEqKernel::new(
            vec![ScalarValue::Int(20), ScalarValue::Int(99)],
            BqlType::Int,
        );
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[1, 3]);
    }

    #[test]
    fn dictionary_eq_empty_literals_returns_empty() {
        let col = dict_column(&[0, 1], 1, int_dict_bytes(&[10, 20]), 2, None);
        let kernel = DictionaryEqKernel::new(vec![], BqlType::Int);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 2 }]);
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn dictionary_eq_all_dict_entries_match_fast_path() {
        // IN (10, 20) covers every entry in dict [10, 20] → kernel
        // skips the code-stream scan and returns the non-null input.
        let col = dict_column(&[0, 1, 0, 1], 1, int_dict_bytes(&[10, 20]), 4, None);
        let kernel = DictionaryEqKernel::new(
            vec![ScalarValue::Int(10), ScalarValue::Int(20)],
            BqlType::Int,
        );
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 4 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 4);
    }

    #[test]
    fn dictionary_eq_respects_input_narrowing() {
        // Filter == 10 → code 0 → logical rows {0, 4}. Input narrows
        // to [1..5), so only row 4 survives the intersection.
        let col = dict_column(&[0, 1, 2, 1, 0], 2, int_dict_bytes(&[10, 20, 30]), 5, None);
        let kernel = DictionaryEqKernel::new(vec![ScalarValue::Int(10)], BqlType::Int);
        let input = RowSelection::from_runs(vec![RowRun { start: 1, len: 4 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[4]);
    }

    #[test]
    fn dictionary_eq_string_hit() {
        // Dict ["click", "view"], rows ["view", "click", "view"] →
        // codes [1, 0, 1] at 1-bit width. Filter == "click" → rows {1}.
        let col = dict_column(
            &[1, 0, 1],
            1,
            string_dict_bytes(&["click", "view"]),
            3,
            None,
        );
        let kernel =
            DictionaryEqKernel::new(vec![ScalarValue::String("click".into())], BqlType::String);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 3 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[1]);
    }

    #[test]
    fn dictionary_eq_string_with_nulls() {
        // 6 logical rows, nulls at 1 and 4. Non-null values:
        //   row 0: "a", row 2: "b", row 3: "c", row 5: "a".
        // Dict ["a", "b", "c"] → dense codes [0, 1, 2, 0] at 2-bit.
        // Null bitmap LSB-first: bits 0,2,3,5 set.
        //   byte0 = 0b00101101 = 0x2D (rows 0..=5; bits 6,7 zero).
        // Filter == "a" → target code {0} → dense positions {0, 3}
        //   → logical rows {0, 5}.
        let col = dict_column(
            &[0, 1, 2, 0],
            2,
            string_dict_bytes(&["a", "b", "c"]),
            6,
            Some(vec![0x2Du8]),
        );
        let kernel =
            DictionaryEqKernel::new(vec![ScalarValue::String("a".into())], BqlType::String);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 6 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[0, 5]);
    }

    #[test]
    fn dictionary_eq_all_match_fast_path_with_nulls_drops_nulls() {
        // IN (10, 20) matches every dict entry → fast path. Nulls at
        // rows 1, 3 (bitmap 0b0101 = 0x05) must still be dropped.
        let col = dict_column(&[0, 0], 1, int_dict_bytes(&[10, 20]), 4, Some(vec![0x05u8]));
        let kernel = DictionaryEqKernel::new(
            vec![ScalarValue::Int(10), ScalarValue::Int(20)],
            BqlType::Int,
        );
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 4 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[0, 2]);
    }

    #[test]
    fn dictionary_eq_duplicate_literals_do_not_trigger_all_match_fast_path() {
        // Regression: a naive `target_codes.len() >= cardinality` compare
        // fires spuriously on `IN (20, 20, 20)` (three pushes, one unique
        // code) and selects every non-null row. Dedup in the kernel
        // prevents this — only rows with value 20 should match.
        let col = dict_column(&[0, 1, 2, 1, 0], 2, int_dict_bytes(&[10, 20, 30]), 5, None);
        let kernel = DictionaryEqKernel::new(
            vec![
                ScalarValue::Int(20),
                ScalarValue::Int(20),
                ScalarValue::Int(20),
            ],
            BqlType::Int,
        );
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 5 }]);
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.as_indices().as_slice(), &[1, 3]);
    }

    #[test]
    fn dictionary_eq_type_mismatch_returns_empty() {
        // A String literal against an Int dict column is a dispatcher
        // bug. The kernel fails closed (empty) rather than passing
        // input through, which would violate the "never widens" rule.
        let col = dict_column(&[0, 1], 1, int_dict_bytes(&[10, 20]), 2, None);
        let kernel = DictionaryEqKernel::new(vec![ScalarValue::String("10".into())], BqlType::Int);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 2 }]);
        let out = kernel.apply(&col.view(), &input);
        assert!(out.is_empty());
    }

    #[test]
    fn dictionary_eq_wrong_encoding_passes_through() {
        let col = EncodedColumn::Encoded {
            chunk: PinnedChunk {
                payload: Arc::from(Vec::<u8>::new()),
                nulls: None,
                params: Arc::from(Vec::<u8>::new()),
            },
            kind: EncodedKind::PlainFixed { width: 8 },
            rows: 2,
        };
        let kernel = DictionaryEqKernel::new(vec![ScalarValue::Int(10)], BqlType::Int);
        let input = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 1]));
        let out = kernel.apply(&col.view(), &input);
        assert_eq!(out.len(), 2);
    }
}
