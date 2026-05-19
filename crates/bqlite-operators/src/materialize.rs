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

use arrow::array::{ArrayRef, UInt32Array};
use arrow::compute;
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use bqlite_core::encoded::{EncodedBatch, RowSelection, StitchedBatch, StitchedRows};
use bqlite_core::metrics::Metrics;
use bqlite_core::{BqlType, BqliteError, Result};
use bqlite_storage::{
    materialize_encoded_column, materialize_encoded_column_selected_with_options,
};

use crate::filtered_batch::FilteredBatch;
use crate::selection::selection_to_bool_array;

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
    materialize_selected_with_metrics(batch, selection, types, schema, None)
}

/// Dict-aware variant of [`materialize_selected`] used by the scan
/// operator to keep low-cardinality `BqlType::String` columns in
/// `DictionaryArray<UInt8, Utf8View>` form across the boundary.
///
/// `schema` declares the **logical** Arrow types — typically
/// `Utf8View` for every String column. When a batch produces a
/// `DictionaryArray` for one of those columns the function rewrites
/// the corresponding field's `data_type` for that batch only, so
/// `RecordBatch::try_new` accepts the heterogeneous physical shape
/// without forcing every downstream caller to track which columns are
/// dictionary-encoded.
///
/// Downstream operators must already accept both shapes — that
/// invariant is recorded in `docs/design/storage/zero-copy-scan-filter.md`
/// and in the per-operator unit tests under `crates/bqlite-operators/src`.
pub fn materialize_selected_dict_pushthrough(
    batch: &EncodedBatch,
    selection: Option<&RowSelection>,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
    metrics: Option<&dyn Metrics>,
) -> Result<FilteredBatch> {
    materialize_selected_inner(batch, selection, types, schema, metrics, true)
}

/// Metrics-aware variant of [`materialize_selected`].
///
/// When `metrics` is `Some`, records the copy-budget counters defined
/// in `docs/design/storage/zero-copy-scan-filter.md` §3:
///
/// - `selected_rows_before_materialization` — the selection's logical
///   row count (or the batch row count when `selection` is `None`).
/// - `materialized_rows` — the output batch's final row count.
/// - `bytes_materialized_after_filter` — the output `RecordBatch`'s
///   total array-buffer bytes.
///
/// Upstream counters (`bytes_scanned`, `bytes_decompressed`,
/// `bytes_materialized_before_filter`) are recorded at the reader
/// level and don't belong here. CP8 bench wiring consumes these
/// counters from a shared [`Metrics`] handle threaded through the
/// scan operator (landing in a subsequent change).
pub fn materialize_selected_with_metrics(
    batch: &EncodedBatch,
    selection: Option<&RowSelection>,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
    metrics: Option<&dyn Metrics>,
) -> Result<FilteredBatch> {
    materialize_selected_inner(batch, selection, types, schema, metrics, false)
}

fn materialize_selected_inner(
    batch: &EncodedBatch,
    selection: Option<&RowSelection>,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
    metrics: Option<&dyn Metrics>,
    prefer_dict_string: bool,
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
    if let Some(m) = metrics {
        let selected_rows = selection
            .map(|s| s.len() as u64)
            .unwrap_or(batch.row_count as u64);
        m.record_selected_rows_before_materialization(selected_rows);
    }
    let mut arrays = Vec::with_capacity(batch.columns.len());
    for (col, ty) in batch.columns.iter().zip(types.iter()) {
        let arr = materialize_encoded_column_selected_with_options(
            col,
            ty,
            selection,
            prefer_dict_string,
        )?;
        arrays.push(arr);
    }
    let effective_schema = if prefer_dict_string {
        schema_matching_arrays(&schema, &arrays)?
    } else {
        schema
    };
    let record = RecordBatch::try_new(effective_schema, arrays).map_err(|e| {
        BqliteError::Execution(format!(
            "materialize_selected: record-batch build failed: {e}"
        ))
    })?;
    if let Some(m) = metrics {
        m.record_materialized_rows(record.num_rows() as u64);
        m.record_bytes_materialized_after_filter(record_batch_bytes(&record));
    }
    Ok(FilteredBatch::dense(record))
}

/// Rebuild `schema` so each field's `data_type` matches the
/// corresponding emitted array.
///
/// Only the data type can differ when `prefer_dict_string == true`:
/// String fields may resolve to `Dict<UInt8, Utf8View>` in one batch
/// and `Utf8View` in the next, depending on per-row-group encoding.
/// Field names, nullability, and metadata are preserved.
fn schema_matching_arrays(
    schema: &Arc<ArrowSchema>,
    arrays: &[ArrayRef],
) -> Result<Arc<ArrowSchema>> {
    if schema.fields().len() != arrays.len() {
        return Err(BqliteError::Execution(format!(
            "schema_matching_arrays: schema has {} fields but {} arrays",
            schema.fields().len(),
            arrays.len()
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(arrays.len());
    let mut differs = false;
    for (field, arr) in schema.fields().iter().zip(arrays.iter()) {
        if field.data_type() == arr.data_type() {
            fields.push(field.as_ref().clone());
        } else {
            differs = true;
            let mut updated = field.as_ref().clone();
            updated = updated.with_data_type(arr.data_type().clone());
            fields.push(updated);
        }
    }
    if !differs {
        return Ok(schema.clone());
    }
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// Collapse a [`FilteredBatch`] from the post-boundary kernel chain
/// into a contiguous [`RecordBatch`].
///
/// This is the §3.4 / §4.3 boundary helper from
/// `docs/design/engine/operator-fusion.md`. It is the **only** call
/// site that increments `selection_vector_materializations` —
/// short-circuit paths skip the metric, so the counter cleanly
/// measures actual-copy activity inside the segment driver.
///
/// Three paths:
///
/// 1. `selection: None` — return the inner batch unchanged. No copy,
///    no metric increment.
/// 2. `selection: Some(sv)` with `sv.len() == batch.num_rows()` —
///    every row is live (the all-pass shape per §3.3.1). Soundness:
///    `SelectionVector` is sorted-ascending and unique, so
///    `len() == num_rows()` ⇒ `sv == 0..num_rows()`. Return the inner
///    batch; no copy, no metric increment.
/// 3. `selection: Some(sv)` with `sv.len() < batch.num_rows()` —
///    rebuild a contiguous batch via `arrow::compute::filter_record_batch`,
///    incrementing the materialization counter by 1 (per call, not
///    per row, per §3.5).
///
/// The helper is intentionally distinct from [`materialize_selected`]:
/// that helper is the *encoded-path* boundary
/// (`EncodedBatch + RowSelection -> RecordBatch`); this one is the
/// *post-boundary* boundary (`FilteredBatch -> RecordBatch`). The two
/// share `selection_to_bool_array` but tag different metrics.
pub fn materialize_filtered_batch(fb: FilteredBatch, metrics: &dyn Metrics) -> Result<RecordBatch> {
    let FilteredBatch { batch, selection } = fb;
    match selection {
        None => Ok(batch),
        Some(sv) if sv.len() == batch.num_rows() => {
            // Sound by SelectionVector's strictly-ascending invariant
            // — a length-equal selection is the dense identity.
            Ok(batch)
        }
        Some(sv) => {
            metrics.record_selection_vector_materializations(1);
            let row_count_u32 = u32::try_from(batch.num_rows()).map_err(|_| {
                BqliteError::Execution(format!(
                    "materialize_filtered_batch: row count {} exceeds u32",
                    batch.num_rows()
                ))
            })?;
            let mask = selection_to_bool_array(
                &bqlite_core::encoded::RowSelection::from_indices(sv),
                row_count_u32,
            );
            arrow::compute::filter_record_batch(&batch, &mask).map_err(BqliteError::Arrow)
        }
    }
}

/// Approximate the total bytes of Arrow array buffers in a record
/// batch, by summing each column's `get_array_memory_size`.
fn record_batch_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|a| a.get_array_memory_size() as u64)
        .sum()
}

/// Materialize a [`StitchedBatch`] into a dense [`FilteredBatch`]
/// (zero-copy scan/filter §9, CP6/CP7 of the zero-copy scan/filter
/// plan).
///
/// The k-way merge's output form: zero or more encoded source batches
/// plus a [`StitchedRows`] reference describing merged row order. Three
/// shapes:
///
/// - [`StitchedRows::SingleSource`]: the cheapest — the merge
///   degenerated to pass-through of one source (common for
///   single-shard scans). Delegates to [`materialize_selected`] on the
///   single source.
/// - [`StitchedRows::Runs`]: contiguous runs from specific sources.
///   Each run decodes only its slice of the corresponding source.
/// - [`StitchedRows::Indices`]: fully interleaved rows — rare in
///   practice because entity locality is preserved at segment
///   boundaries. This path is the fallback: decode each source to a
///   dense column, then `take` the referenced indices per source and
///   concatenate.
///
/// This helper is the boundary that turns selection-first encoded
/// output into the dense [`FilteredBatch`] downstream operators
/// expect. Together with
/// [`bqlite_storage::segment::merge::EncodedKWayMergeScan`] (the
/// stitched-merge producer) and
/// [`bqlite_storage::EncodedTombstoneSource`] (the §8.4 selection-first
/// tombstone filter wired in by [`crate::scan::ScanOperator`]), it
/// ensures `arrow::compute::interleave` is no longer the default
/// scan/merge path — it survives only as the
/// `BQLITE_SCAN_PATH=materialized` debug fallback.
pub fn materialize_stitched(
    stitched: &StitchedBatch,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
) -> Result<FilteredBatch> {
    materialize_stitched_inner(stitched, types, schema, false)
}

/// Dict-aware variant of [`materialize_stitched`]. All three arms
/// honour dict push-through:
///
/// - **SingleSource** — delegates straight to
///   [`materialize_selected_inner`] with `prefer_dict_string=true`.
/// - **Runs** — materialises each per-source slice as Dict<UInt8,
///   Utf8View> when its segment encoding allows, then unifies the
///   per-segment dictionaries at concat time. Chunks that exceed the
///   u8 keyspace after unification, or columns whose chunks are mixed
///   Dict + StringView, fall back to a value-type cast + standard
///   `arrow::compute::concat`.
/// - **Indices** — same unification at concat time after `take` from
///   each Dict-encoded source.
pub fn materialize_stitched_dict_pushthrough(
    stitched: &StitchedBatch,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
) -> Result<FilteredBatch> {
    materialize_stitched_inner(stitched, types, schema, true)
}

fn materialize_stitched_inner(
    stitched: &StitchedBatch,
    types: &[BqlType],
    schema: Arc<ArrowSchema>,
    prefer_dict_string: bool,
) -> Result<FilteredBatch> {
    if schema.fields().len() != types.len() {
        return Err(BqliteError::Execution(format!(
            "materialize_stitched: schema has {} fields but {} types provided",
            schema.fields().len(),
            types.len(),
        )));
    }
    match &stitched.rows {
        StitchedRows::SingleSource { source, selection } => {
            let idx = *source as usize;
            let src = stitched.sources.get(idx).ok_or_else(|| {
                BqliteError::Execution(format!(
                    "materialize_stitched: SingleSource references source {idx} out of {}",
                    stitched.sources.len()
                ))
            })?;
            materialize_selected_inner(src, selection.as_ref(), types, schema, None, prefer_dict_string)
        }
        StitchedRows::Runs(runs) => {
            let arrays =
                materialize_stitched_runs(&stitched.sources, runs, types, prefer_dict_string)?;
            let effective_schema = if prefer_dict_string {
                schema_matching_arrays(&schema, &arrays)?
            } else {
                schema
            };
            let batch = RecordBatch::try_new(effective_schema, arrays).map_err(|e| {
                BqliteError::Execution(format!(
                    "materialize_stitched: record-batch build failed: {e}"
                ))
            })?;
            Ok(FilteredBatch::dense(batch))
        }
        StitchedRows::Indices(refs) => {
            let arrays =
                materialize_stitched_indices(&stitched.sources, refs, types, prefer_dict_string)?;
            let effective_schema = if prefer_dict_string {
                schema_matching_arrays(&schema, &arrays)?
            } else {
                schema
            };
            let batch = RecordBatch::try_new(effective_schema, arrays).map_err(|e| {
                BqliteError::Execution(format!(
                    "materialize_stitched: record-batch build failed: {e}"
                ))
            })?;
            Ok(FilteredBatch::dense(batch))
        }
    }
}

fn materialize_stitched_runs(
    sources: &[EncodedBatch],
    runs: &[bqlite_core::encoded::SourceRun],
    types: &[BqlType],
    prefer_dict_string: bool,
) -> Result<Vec<ArrayRef>> {
    let mut out: Vec<Vec<ArrayRef>> = (0..types.len()).map(|_| Vec::new()).collect();
    for run in runs {
        let src = sources.get(run.source as usize).ok_or_else(|| {
            BqliteError::Execution(format!(
                "materialize_stitched: run references source {}",
                run.source
            ))
        })?;
        if src.columns.len() != types.len() {
            return Err(BqliteError::Execution(format!(
                "materialize_stitched: source has {} columns but {} types provided",
                src.columns.len(),
                types.len(),
            )));
        }
        let sel = RowSelection::from_runs(vec![bqlite_core::encoded::RowRun {
            start: run.start,
            len: run.len,
        }]);
        for (col_idx, (col, ty)) in src.columns.iter().zip(types.iter()).enumerate() {
            let arr = materialize_encoded_column_selected_with_options(
                col,
                ty,
                Some(&sel),
                prefer_dict_string,
            )?;
            out[col_idx].push(arr);
        }
    }
    concat_chunks(out)
}

fn materialize_stitched_indices(
    sources: &[EncodedBatch],
    refs: &[bqlite_core::encoded::RowRef],
    types: &[BqlType],
    prefer_dict_string: bool,
) -> Result<Vec<ArrayRef>> {
    // Decode every referenced source once, then `take` per index.
    //
    // Optimization opportunity: group refs by source to limit `take`
    // calls. CP5 ships the correct but naive implementation; the merge
    // producer chooses between Runs and Indices based on interleave
    // density, so the bulk of workloads hit the Runs path first.
    let mut dense_sources: Vec<Option<Vec<ArrayRef>>> = vec![None; sources.len()];
    // Build the dense arrays for each unique source, once.
    for r in refs {
        let idx = r.source as usize;
        if dense_sources[idx].is_some() {
            continue;
        }
        let src = sources.get(idx).ok_or_else(|| {
            BqliteError::Execution(format!("materialize_stitched: ref references source {idx}"))
        })?;
        if src.columns.len() != types.len() {
            return Err(BqliteError::Execution(format!(
                "materialize_stitched: source has {} columns but {} types provided",
                src.columns.len(),
                types.len(),
            )));
        }
        let mut arrs = Vec::with_capacity(types.len());
        for (col, ty) in src.columns.iter().zip(types.iter()) {
            // Build a full-range selection so the per-encoding
            // materialiser stays on the selection-aware fast path —
            // that's the only path that respects `prefer_dict_string`.
            // The cost is one selection-vector walk per source, which
            // is amortised over every index that targets this source.
            let arr = if prefer_dict_string {
                let rows = match col {
                    bqlite_core::encoded::EncodedColumn::Materialized { rows, .. } => *rows,
                    bqlite_core::encoded::EncodedColumn::Encoded { rows, .. } => *rows,
                };
                let full = RowSelection::from_runs(vec![bqlite_core::encoded::RowRun {
                    start: 0,
                    len: rows,
                }]);
                materialize_encoded_column_selected_with_options(col, ty, Some(&full), true)?
            } else {
                materialize_encoded_column(col, ty)?
            };
            arrs.push(arr);
        }
        dense_sources[idx] = Some(arrs);
    }
    // Build per-column chunks by `take`-ing from each source block
    // and concatenating in row order.
    let mut chunks: Vec<Vec<ArrayRef>> = (0..types.len()).map(|_| Vec::new()).collect();
    let mut cursor = 0usize;
    while cursor < refs.len() {
        // Gather the longest contiguous run of the same source.
        let src_idx = refs[cursor].source;
        let start = cursor;
        while cursor < refs.len() && refs[cursor].source == src_idx {
            cursor += 1;
        }
        let dense = dense_sources[src_idx as usize].as_ref().unwrap();
        let indices: Vec<u32> = refs[start..cursor].iter().map(|r| r.row).collect();
        let index_array = UInt32Array::from(indices);
        for (col_idx, arr) in dense.iter().enumerate() {
            let picked = compute::take(arr.as_ref(), &index_array, None).map_err(|e| {
                BqliteError::Execution(format!(
                    "materialize_stitched: arrow::compute::take failed: {e}"
                ))
            })?;
            chunks[col_idx].push(picked);
        }
    }
    concat_chunks(chunks)
}

fn concat_chunks(mut chunks: Vec<Vec<ArrayRef>>) -> Result<Vec<ArrayRef>> {
    let mut out = Vec::with_capacity(chunks.len());
    for col_chunks in chunks.iter_mut() {
        if col_chunks.is_empty() {
            return Err(BqliteError::Execution(
                "materialize_stitched: column has no chunks to concatenate".into(),
            ));
        }
        // Three-way dispatch:
        //   1. No Dict chunks at all → plain concat (the original path,
        //      kept zero-overhead so non-string columns regress 0%).
        //   2. All chunks are Dict<UInt8, Utf8View> → try to unify their
        //      per-segment dictionaries and emit a single
        //      Dict<UInt8, Utf8View>; only falls through to plain concat
        //      if the union overflows the u8 keyspace.
        //   3. Mixed Dict + non-Dict → cast Dict chunks to value type
        //      and concat as a homogeneous non-Dict array.
        let any_dict = col_chunks
            .iter()
            .any(|a| matches!(a.data_type(), arrow::datatypes::DataType::Dictionary(_, _)));
        if !any_dict {
            let refs: Vec<&dyn arrow::array::Array> = col_chunks
                .iter()
                .map(|a| a.as_ref() as &dyn arrow::array::Array)
                .collect();
            let concatenated = compute::concat(&refs).map_err(|e| {
                BqliteError::Execution(format!(
                    "materialize_stitched: arrow::compute::concat failed: {e}"
                ))
            })?;
            out.push(concatenated);
            continue;
        }
        if col_chunks
            .iter()
            .all(|a| is_dict_uint8_utf8view(a.data_type()))
        {
            if let Some(unified) = try_unify_dict_uint8_utf8view(col_chunks)? {
                out.push(unified);
                continue;
            }
        }
        let normalized: Vec<ArrayRef> = col_chunks
            .iter()
            .map(normalize_dict_to_values)
            .collect::<Result<Vec<_>>>()?;
        let refs: Vec<&dyn arrow::array::Array> = normalized
            .iter()
            .map(|a| a.as_ref() as &dyn arrow::array::Array)
            .collect();
        let concatenated = compute::concat(&refs).map_err(|e| {
            BqliteError::Execution(format!(
                "materialize_stitched: arrow::compute::concat failed: {e}"
            ))
        })?;
        out.push(concatenated);
    }
    Ok(out)
}

fn is_dict_uint8_utf8view(dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;
    matches!(
        dt,
        DataType::Dictionary(k, v)
            if matches!(k.as_ref(), DataType::UInt8)
                && matches!(v.as_ref(), DataType::Utf8View)
    )
}

/// Decay a `Dict<_, Utf8View>` array to its value type for the
/// fallback concat path. Non-dict arrays pass through unchanged.
fn normalize_dict_to_values(a: &ArrayRef) -> Result<ArrayRef> {
    use arrow::datatypes::DataType;
    if let DataType::Dictionary(_, value_ty) = a.data_type() {
        compute::cast(a.as_ref(), value_ty.as_ref()).map_err(|e| {
            BqliteError::Execution(format!(
                "materialize_stitched: cast Dict->values failed: {e}"
            ))
        })
    } else {
        Ok(a.clone())
    }
}

/// Build a single `Dict<UInt8, Utf8View>` array that covers every
/// chunk by unifying their value tables.
///
/// Returns `Ok(None)` (caller falls back to cast+concat) when the
/// union of value strings exceeds the u8 keyspace (256 entries).
/// Storage-side dict materialisers append no nulls to the value
/// region — nulls live in the key bitmap — so this helper treats
/// dict values as a flat set of non-null `&str`s.
fn try_unify_dict_uint8_utf8view(chunks: &[ArrayRef]) -> Result<Option<ArrayRef>> {
    use ::arrow::array::{
        builder::UInt8Builder, Array, DictionaryArray, StringViewArray, StringViewBuilder,
    };
    use ::arrow::datatypes::UInt8Type;
    use std::collections::HashMap;

    if chunks.is_empty() {
        return Ok(None);
    }

    let mut value_to_code: HashMap<String, u8> = HashMap::new();
    let mut unified_values: Vec<String> = Vec::new();
    // Per-chunk remap from local code -> unified code.
    let mut remaps: Vec<Vec<u8>> = Vec::with_capacity(chunks.len());
    let mut total_keys: usize = 0;

    for chunk in chunks {
        total_keys += chunk.len();
        let dict = chunk
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .ok_or_else(|| {
                BqliteError::Execution(
                    "materialize_stitched: dict downcast failed (UInt8)".into(),
                )
            })?;
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .ok_or_else(|| {
                BqliteError::Execution(
                    "materialize_stitched: dict values downcast failed (Utf8View)".into(),
                )
            })?;
        let mut remap: Vec<u8> = Vec::with_capacity(values.len());
        for i in 0..values.len() {
            let s = if values.is_null(i) { "" } else { values.value(i) };
            let code = match value_to_code.get(s) {
                Some(&c) => c,
                None => {
                    if unified_values.len() >= 256 {
                        return Ok(None);
                    }
                    let c = unified_values.len() as u8;
                    unified_values.push(s.to_owned());
                    value_to_code.insert(s.to_owned(), c);
                    c
                }
            };
            remap.push(code);
        }
        remaps.push(remap);
    }

    let mut value_builder = StringViewBuilder::with_capacity(unified_values.len());
    for s in &unified_values {
        value_builder.append_value(s);
    }
    let unified_values_arr = value_builder.finish();

    let mut key_builder = UInt8Builder::with_capacity(total_keys);
    for (chunk, remap) in chunks.iter().zip(remaps.iter()) {
        let dict = chunk
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .unwrap();
        let keys = dict.keys();
        let raw: &[u8] = keys.values();
        match keys.nulls() {
            None => {
                for &code in raw {
                    key_builder.append_value(remap[code as usize]);
                }
            }
            Some(nulls) => {
                for (row, &code) in raw.iter().enumerate() {
                    if nulls.is_null(row) {
                        key_builder.append_null();
                    } else {
                        key_builder.append_value(remap[code as usize]);
                    }
                }
            }
        }
    }
    let unified_keys = key_builder.finish();

    let unified = DictionaryArray::<UInt8Type>::try_new(unified_keys, Arc::new(unified_values_arr))
        .map_err(|e| {
            BqliteError::Execution(format!(
                "materialize_stitched: unified DictionaryArray::try_new failed: {e}"
            ))
        })?;
    Ok(Some(Arc::new(unified)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field};
    use bqlite_core::encoded::{EncodedColumn, RowRef, RowRun, SelectionVector, SourceRun};

    fn i64_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![Field::new(
            "x",
            DataType::Int64,
            true,
        )]))
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
        let fb = materialize_selected(&batch, None, &[BqlType::Int], i64_schema()).unwrap();
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
        let err = materialize_selected(&batch, None, &[BqlType::Int, BqlType::Int], i64_schema())
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
        let kernel = RleIntEqKernel::new(vec![1]);
        let input = RowSelection::from_runs(vec![RowRun { start: 0, len: 10 }]);
        let sel = kernel.apply(&batch.columns[0].view(), &input);

        // Boundary: materialize selected rows. Expected values = 1
        // repeated 8 times (3 + 5).
        let fb = materialize_selected(&batch, Some(&sel), &[BqlType::Int], i64_schema()).unwrap();
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
    fn materialize_selected_with_metrics_records_boundary_counters() {
        use bqlite_core::metrics::AtomicMetrics;
        let col = materialized_column(vec![10, 20, 30, 40, 50]);
        let batch = EncodedBatch::new(5, vec![col]);
        let sel = RowSelection::from_indices(SelectionVector::from_sorted(vec![0, 2, 4]));
        let metrics = AtomicMetrics::default();
        let fb = materialize_selected_with_metrics(
            &batch,
            Some(&sel),
            &[BqlType::Int],
            i64_schema(),
            Some(&metrics),
        )
        .unwrap();
        assert_eq!(fb.batch.num_rows(), 3);
        let snap = metrics.snapshot();
        assert_eq!(snap.selected_rows_before_materialization, 3);
        assert_eq!(snap.materialized_rows, 3);
        assert!(snap.bytes_materialized_after_filter > 0);
    }

    #[test]
    fn materialize_selected_with_metrics_none_selection_counts_row_count() {
        use bqlite_core::metrics::AtomicMetrics;
        let col = materialized_column(vec![10, 20, 30]);
        let batch = EncodedBatch::new(3, vec![col]);
        let metrics = AtomicMetrics::default();
        let _ = materialize_selected_with_metrics(
            &batch,
            None,
            &[BqlType::Int],
            i64_schema(),
            Some(&metrics),
        )
        .unwrap();
        let snap = metrics.snapshot();
        assert_eq!(snap.selected_rows_before_materialization, 3);
        assert_eq!(snap.materialized_rows, 3);
    }

    #[test]
    fn stitched_single_source_delegates_to_materialize_selected() {
        let col = materialized_column(vec![10, 20, 30]);
        let source = EncodedBatch::new(3, vec![col]);
        let stitched = StitchedBatch {
            sources: vec![source],
            rows: StitchedRows::SingleSource {
                source: 0,
                selection: Some(RowSelection::from_indices(SelectionVector::from_sorted(
                    vec![0, 2],
                ))),
            },
        };
        let fb = materialize_stitched(&stitched, &[BqlType::Int], i64_schema()).unwrap();
        assert_eq!(fb.batch.num_rows(), 2);
        let ints = fb
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[10i64, 30]);
    }

    #[test]
    fn stitched_runs_concatenates_source_slices() {
        // Two sources: [1,2,3] and [10,20,30,40].
        // Runs: source 0 rows [1..3), source 1 rows [0..2), source 0 rows [0..1)
        // Expected output rows: [2, 3, 10, 20, 1]
        let s0 = EncodedBatch::new(3, vec![materialized_column(vec![1, 2, 3])]);
        let s1 = EncodedBatch::new(4, vec![materialized_column(vec![10, 20, 30, 40])]);
        let stitched = StitchedBatch {
            sources: vec![s0, s1],
            rows: StitchedRows::Runs(vec![
                SourceRun {
                    source: 0,
                    start: 1,
                    len: 2,
                },
                SourceRun {
                    source: 1,
                    start: 0,
                    len: 2,
                },
                SourceRun {
                    source: 0,
                    start: 0,
                    len: 1,
                },
            ]),
        };
        let fb = materialize_stitched(&stitched, &[BqlType::Int], i64_schema()).unwrap();
        let ints = fb
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[2i64, 3, 10, 20, 1]);
    }

    #[test]
    fn stitched_indices_interleaves_rows() {
        // Two sources: [100, 200, 300], [1000, 2000].
        // Refs in merge order: (src0, 2), (src1, 0), (src0, 0), (src1, 1), (src0, 1)
        // Expected rows: [300, 1000, 100, 2000, 200]
        let s0 = EncodedBatch::new(3, vec![materialized_column(vec![100, 200, 300])]);
        let s1 = EncodedBatch::new(2, vec![materialized_column(vec![1000, 2000])]);
        let stitched = StitchedBatch {
            sources: vec![s0, s1],
            rows: StitchedRows::Indices(vec![
                RowRef { source: 0, row: 2 },
                RowRef { source: 1, row: 0 },
                RowRef { source: 0, row: 0 },
                RowRef { source: 1, row: 1 },
                RowRef { source: 0, row: 1 },
            ]),
        };
        let fb = materialize_stitched(&stitched, &[BqlType::Int], i64_schema()).unwrap();
        let ints = fb
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ints.values(), &[300i64, 1000, 100, 2000, 200]);
    }

    // ── materialize_filtered_batch (TASK-518) ─────────────────────────

    #[test]
    fn materialize_filtered_batch_dense_input_returns_inner_batch_no_metric() {
        use crate::filtered_batch::FilteredBatch;
        use bqlite_core::metrics::AtomicMetrics;
        let metrics = AtomicMetrics::default();
        let batch = RecordBatch::try_new(
            i64_schema(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef],
        )
        .unwrap();
        let out =
            crate::materialize::materialize_filtered_batch(FilteredBatch::dense(batch), &metrics)
                .unwrap();
        assert_eq!(out.num_rows(), 3);
        // §4.3: short-circuit path — no metric increment.
        assert_eq!(metrics.snapshot().selection_vector_materializations, 0);
    }

    #[test]
    fn materialize_filtered_batch_full_cover_selection_short_circuits() {
        // selection.len() == batch.num_rows() ⇒ the all-pass dense
        // shape from §3.3.1. Sound by the SelectionVector
        // ascending+unique invariant. Should return inner batch
        // without copying or incrementing the metric.
        use crate::filtered_batch::FilteredBatch;
        use bqlite_core::metrics::AtomicMetrics;
        let metrics = AtomicMetrics::default();
        let batch = RecordBatch::try_new(
            i64_schema(),
            vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30])) as ArrayRef],
        )
        .unwrap();
        let sv = SelectionVector::from_sorted(vec![0, 1, 2]);
        let out = crate::materialize::materialize_filtered_batch(
            FilteredBatch::with_selection(batch, sv),
            &metrics,
        )
        .unwrap();
        assert_eq!(out.num_rows(), 3);
        assert_eq!(metrics.snapshot().selection_vector_materializations, 0);
    }

    #[test]
    fn materialize_filtered_batch_sparse_selection_copies_and_increments() {
        use crate::filtered_batch::FilteredBatch;
        use bqlite_core::metrics::AtomicMetrics;
        let metrics = AtomicMetrics::default();
        let batch = RecordBatch::try_new(
            i64_schema(),
            vec![Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40, 50])) as ArrayRef],
        )
        .unwrap();
        let sv = SelectionVector::from_sorted(vec![0, 2, 4]);
        let out = crate::materialize::materialize_filtered_batch(
            FilteredBatch::with_selection(batch, sv),
            &metrics,
        )
        .unwrap();
        assert_eq!(out.num_rows(), 3);
        let ints = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.values(), &[10i64, 30, 50]);
        // Exactly one materialization recorded — per call, not per row.
        assert_eq!(metrics.snapshot().selection_vector_materializations, 1);
    }

    #[test]
    fn materialize_filtered_batch_empty_selection_returns_zero_rows() {
        use crate::filtered_batch::FilteredBatch;
        use bqlite_core::metrics::AtomicMetrics;
        let metrics = AtomicMetrics::default();
        let batch = RecordBatch::try_new(
            i64_schema(),
            vec![Arc::new(Int64Array::from(vec![10_i64, 20])) as ArrayRef],
        )
        .unwrap();
        let sv = SelectionVector::new();
        let out = crate::materialize::materialize_filtered_batch(
            FilteredBatch::with_selection(batch, sv),
            &metrics,
        )
        .unwrap();
        assert_eq!(out.num_rows(), 0);
        // Empty selection still goes through the actual-copy path (it
        // is not a "full-cover" no-op for any non-empty batch).
        assert_eq!(metrics.snapshot().selection_vector_materializations, 1);
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

    // ── Dict<UInt8, Utf8View> unification at concat time ──────────────

    fn dict_chunk(values: &[&str], keys: &[Option<u8>]) -> ArrayRef {
        use ::arrow::array::{
            builder::UInt8Builder, DictionaryArray, StringViewBuilder,
        };
        use ::arrow::datatypes::UInt8Type;
        let mut vb = StringViewBuilder::with_capacity(values.len());
        for v in values {
            vb.append_value(v);
        }
        let values_arr = vb.finish();
        let mut kb = UInt8Builder::with_capacity(keys.len());
        for k in keys {
            match k {
                Some(c) => kb.append_value(*c),
                None => kb.append_null(),
            }
        }
        let keys_arr = kb.finish();
        Arc::new(
            DictionaryArray::<UInt8Type>::try_new(keys_arr, Arc::new(values_arr)).unwrap(),
        )
    }

    #[test]
    fn unify_dict_uint8_utf8view_concats_disjoint_dicts() {
        // Two chunks with disjoint dict values that fit in u8.
        // Chunk A: dict=["x","y"], keys=[0,1,0]
        // Chunk B: dict=["y","z"], keys=[1,0]
        // Unified: dict=["x","y","z"], keys=[0,1,0, 2,1]
        let chunks = vec![
            dict_chunk(&["x", "y"], &[Some(0), Some(1), Some(0)]),
            dict_chunk(&["y", "z"], &[Some(1), Some(0)]),
        ];
        let unified = super::try_unify_dict_uint8_utf8view(&chunks).unwrap().unwrap();
        let dict = unified
            .as_any()
            .downcast_ref::<::arrow::array::DictionaryArray<::arrow::datatypes::UInt8Type>>()
            .unwrap();
        assert_eq!(dict.keys().len(), 5);
        let vals = dict
            .values()
            .as_any()
            .downcast_ref::<::arrow::array::StringViewArray>()
            .unwrap();
        // Codes must resolve to the right strings.
        let resolved: Vec<&str> = (0..dict.keys().len())
            .map(|i| vals.value(dict.keys().value(i) as usize))
            .collect();
        assert_eq!(resolved, vec!["x", "y", "x", "z", "y"]);
    }

    #[test]
    fn unify_dict_uint8_utf8view_preserves_null_keys() {
        // Chunk with one null key. Output must keep the null at that
        // logical position.
        let chunks = vec![
            dict_chunk(&["a", "b"], &[Some(0), None, Some(1)]),
            dict_chunk(&["b"], &[Some(0)]),
        ];
        let unified = super::try_unify_dict_uint8_utf8view(&chunks).unwrap().unwrap();
        let dict = unified
            .as_any()
            .downcast_ref::<::arrow::array::DictionaryArray<::arrow::datatypes::UInt8Type>>()
            .unwrap();
        assert_eq!(dict.keys().len(), 4);
        assert!(!dict.keys().is_null(0));
        assert!(dict.keys().is_null(1));
        assert!(!dict.keys().is_null(2));
        assert!(!dict.keys().is_null(3));
    }

    #[test]
    fn unify_dict_uint8_utf8view_returns_none_on_u8_overflow() {
        // Build a chunk with 200 distinct strings, plus a second chunk
        // with 100 more disjoint strings. Union = 300, exceeds u8.
        let a_vals: Vec<String> = (0..200).map(|i| format!("a{i}")).collect();
        let b_vals: Vec<String> = (0..100).map(|i| format!("b{i}")).collect();
        let a_refs: Vec<&str> = a_vals.iter().map(|s| s.as_str()).collect();
        let b_refs: Vec<&str> = b_vals.iter().map(|s| s.as_str()).collect();
        let a_keys: Vec<Option<u8>> = (0..200).map(|i| Some(i as u8)).collect();
        let b_keys: Vec<Option<u8>> = (0..100).map(|i| Some(i as u8)).collect();
        let chunks = vec![dict_chunk(&a_refs, &a_keys), dict_chunk(&b_refs, &b_keys)];
        let unified = super::try_unify_dict_uint8_utf8view(&chunks).unwrap();
        assert!(unified.is_none(), "u8 overflow must fall back");
    }

    #[test]
    fn concat_chunks_dict_path_emits_single_dict_when_all_dict() {
        let chunks = vec![vec![
            dict_chunk(&["foo", "bar"], &[Some(0), Some(1)]),
            dict_chunk(&["bar", "baz"], &[Some(0), Some(1)]),
        ]];
        let out = super::concat_chunks(chunks).unwrap();
        assert_eq!(out.len(), 1);
        assert!(super::is_dict_uint8_utf8view(out[0].data_type()));
        assert_eq!(out[0].len(), 4);
    }

    #[test]
    fn concat_chunks_falls_back_to_stringview_on_mixed_shapes() {
        use ::arrow::array::{StringViewArray, StringViewBuilder};
        let mut vb = StringViewBuilder::with_capacity(2);
        vb.append_value("x");
        vb.append_value("z");
        let sv: ArrayRef = Arc::new(vb.finish());
        let chunks = vec![vec![dict_chunk(&["a", "b"], &[Some(0), Some(1)]), sv]];
        let out = super::concat_chunks(chunks).unwrap();
        assert_eq!(out.len(), 1);
        // Mixed Dict + StringView → cast to value type, concat as StringView.
        let strs = out[0]
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("fallback emits StringViewArray");
        assert_eq!(strs.len(), 4);
        assert_eq!(strs.value(0), "a");
        assert_eq!(strs.value(1), "b");
        assert_eq!(strs.value(2), "x");
        assert_eq!(strs.value(3), "z");
    }
}
