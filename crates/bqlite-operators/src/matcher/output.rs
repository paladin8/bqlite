//! Output `RecordBatch` construction from match results.
//!
//! Converts the internal match results ([`MatchCompletion`] and
//! [`PartialMatch`]) into Arrow `RecordBatch` conforming to the
//! demand-reduced output schema (match-operator.md §9, query-language.md
//! §4.12, §6.3).
//!
//! ## Step indexing
//!
//! `PartialMatch::step_reached` is 1-indexed (matching the step counter
//! and `state_to_step`). `MatchCompletion` rows always emit
//! `step_reached = num_steps`. Both are written directly into the
//! user-facing `step_reached` column.
//!
//! ## BRACKETS expansion (TASK-529)
//!
//! When `CompiledNfa.brackets` is `Some(BracketSpec)`, every
//! `(entity, binding track)` is expanded into N output rows — one per
//! bracket — with `bracket` (0-indexed) and `bracket_end` (the
//! anchor-relative bracket-upper-bound duration in nanoseconds) columns
//! populated. `step_reached` per bracket follows the rule documented in
//! the plan at `docs/superpowers/plans/2026-04-30-task-529-brackets-runtime.md`.

use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewBuilder,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_core::OperatorSchema;
use bqlite_planner::BracketSpec;

use super::bindings::BindingValue;
use super::nfa::{MatchCompletion, PartialMatch};

/// One materialized output row. Avoids per-column re-walking of
/// completions / partials and lets bracket expansion happen once.
struct OutputRow<'a> {
    /// `final_ts - anchor_ts` for completions; `None` for partials.
    match_duration: Option<i64>,
    /// 1-indexed step number reported in the `step_reached` column.
    step_reached: u8,
    /// Bound variable values (borrowed from the source completion/partial).
    bindings: &'a [BindingValue],
    /// Bracket index (0-indexed) for this row, when BRACKETS is active.
    bracket_idx: Option<i64>,
    /// Bracket upper-bound duration (nanoseconds, anchor-relative) for
    /// this row, when BRACKETS is active.
    bracket_end: Option<i64>,
}

/// Build an output `RecordBatch` from completions and partials.
///
/// Behaviour:
/// - Without `brackets`: one row per completion, plus one row per partial
///   when `emit_all` is true. Existing pre-TASK-529 contract.
/// - With `brackets`: row count expands to N per `(entity, track)` under
///   EMIT ALL; without EMIT ALL only brackets where the final step
///   completed are emitted (single bracket exclusive, contiguous tail
///   cumulative).
pub fn build_output_batch(
    output_schema: &OperatorSchema,
    completions: &[MatchCompletion],
    partials: &[PartialMatch],
    emit_all: bool,
    num_steps: u8,
    brackets: Option<&BracketSpec>,
) -> RecordBatch {
    let rows = build_rows(completions, partials, emit_all, num_steps, brackets);

    if rows.is_empty() {
        return empty_batch(output_schema);
    }

    let arrow_schema = output_schema.to_arrow_schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

    // Identify variable-binding columns: named with a leading `$`. The
    // index used to extract from `bindings` follows the order in which
    // binding columns appear in the schema.
    let var_col_indices: Vec<(usize, usize)> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name().starts_with('$'))
        .enumerate()
        .map(|(binding_idx, (field_idx, _))| (field_idx, binding_idx))
        .collect();

    for (field_idx, field) in arrow_schema.fields().iter().enumerate() {
        let col = match field.name().as_str() {
            "entity_id" => build_entity_id_column(rows.len(), field.data_type()),
            "match_duration" => build_match_duration_column(&rows),
            "step_reached" => build_step_reached_column(&rows),
            "bracket" => build_bracket_idx_column(&rows),
            "bracket_end" => build_bracket_end_column(&rows),
            name if name.starts_with('$') => {
                let binding_idx = var_col_indices
                    .iter()
                    .find(|(fi, _)| *fi == field_idx)
                    .map(|(_, bi)| *bi)
                    .unwrap();
                build_binding_column(&rows, binding_idx, field.data_type())
            }
            _ => {
                // Unimplemented column (match_events, step properties) —
                // emit typed NULL array matching the schema field's
                // declared data type.
                build_null_column(rows.len(), field.data_type())
            }
        };
        columns.push(col);
    }

    RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap_or_else(|e| {
        panic!("failed to build output RecordBatch: {e}");
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Row materialization
// ─────────────────────────────────────────────────────────────────────────────

/// Materialize the per-row representation, applying BRACKETS expansion
/// when requested. Bracket-aware path lives in [`expand_bracket_rows`];
/// the no-bracket path mirrors the pre-TASK-529 contract one-for-one.
fn build_rows<'a>(
    completions: &'a [MatchCompletion],
    partials: &'a [PartialMatch],
    emit_all: bool,
    num_steps: u8,
    brackets: Option<&BracketSpec>,
) -> Vec<OutputRow<'a>> {
    if let Some(spec) = brackets {
        return expand_bracket_rows(completions, partials, emit_all, num_steps, spec);
    }

    let mut rows =
        Vec::with_capacity(completions.len() + if emit_all { partials.len() } else { 0 });
    for c in completions {
        rows.push(OutputRow {
            match_duration: Some(c.final_ts - c.anchor_ts),
            step_reached: num_steps,
            bindings: &c.bindings,
            bracket_idx: None,
            bracket_end: None,
        });
    }
    if emit_all {
        for p in partials {
            rows.push(OutputRow {
                match_duration: None,
                step_reached: p.step_reached,
                bindings: &p.bindings,
                bracket_idx: None,
                bracket_end: None,
            });
        }
    }
    rows
}

/// Expand completions and partials into per-bracket rows under the
/// semantics in `query-language.md` §4.12 and the plan at
/// `docs/superpowers/plans/2026-04-30-task-529-brackets-runtime.md`.
///
/// Bracket window convention (right-closed):
/// - bracket 0: `[0, durations[0]]`
/// - bracket i > 0: `(durations[i-1], durations[i]]`
fn expand_bracket_rows<'a>(
    completions: &'a [MatchCompletion],
    partials: &'a [PartialMatch],
    emit_all: bool,
    num_steps: u8,
    spec: &BracketSpec,
) -> Vec<OutputRow<'a>> {
    let n = spec.durations.len();
    if n == 0 {
        return Vec::new();
    }

    // Worst-case capacity: every (completion, partial) emits N rows.
    let cap_partials = if emit_all { partials.len() } else { 0 };
    let mut rows = Vec::with_capacity((completions.len() + cap_partials) * n);

    // Reused per-(entity, track) scratch buffer holding exclusive
    // step_reached for each bracket. `compute_completion_steps` and
    // `compute_partial_steps` overwrite every slot, so prior contents
    // are intentionally not cleared between iterations.
    let mut per_bracket: Vec<u8> = vec![0; n];

    for c in completions {
        compute_completion_steps(&mut per_bracket, c, num_steps, spec);
        if spec.cumulative {
            apply_prefix_max(&mut per_bracket);
        }
        push_rows_for_track(
            &mut rows,
            &per_bracket,
            spec,
            Some(c.final_ts - c.anchor_ts),
            num_steps,
            &c.bindings,
            emit_all,
        );
    }

    if emit_all {
        for p in partials {
            compute_partial_steps(&mut per_bracket, p);
            if spec.cumulative {
                apply_prefix_max(&mut per_bracket);
            }
            push_rows_for_track(
                &mut rows,
                &per_bracket,
                spec,
                None,
                num_steps,
                &p.bindings,
                emit_all,
            );
        }
    }

    rows
}

/// Fill `out[b]` with the **exclusive** `step_reached` for each bracket
/// `b` given a completion. The result length is `out.len() == spec.durations.len()`.
fn compute_completion_steps(
    out: &mut [u8],
    completion: &MatchCompletion,
    num_steps: u8,
    spec: &BracketSpec,
) {
    let delta = completion.final_ts - completion.anchor_ts;
    debug_assert_eq!(out.len(), spec.durations.len());
    for (b, dur) in spec.durations.iter().enumerate() {
        let prev = if b == 0 { -1 } else { spec.durations[b - 1] };
        // Bracket window: bracket 0 = [0, dur_0]; bracket i>0 = (dur_{i-1}, dur_i].
        // delta_anchor = 0; delta_final = `delta` (≥ 0 by THEN-ordering).
        let final_in = delta > prev && delta <= *dur;
        let anchor_in = b == 0; // 0 ∈ [0, dur_0]
        out[b] = if final_in {
            num_steps
        } else if anchor_in {
            1
        } else {
            0
        };
    }
}

/// Fill `out[b]` with the **exclusive** `step_reached` for each bracket
/// `b` given a partial match. Only the anchor's timestamp is known, so
/// bracket 0 reports `1` and subsequent brackets report `0`. Longer
/// patterns whose intermediate-step timestamps are not tracked degrade
/// gracefully into this rule (see the plan's
/// "Per-bracket `step_reached` rule" item 4 for the design rationale).
///
/// Writes every slot unconditionally so the caller's reusable scratch
/// buffer carries no state across invocations.
fn compute_partial_steps(out: &mut [u8], partial: &PartialMatch) {
    if out.is_empty() {
        return;
    }
    out[0] = if partial.step_reached >= 1 { 1 } else { 0 };
    for slot in out.iter_mut().skip(1) {
        *slot = 0;
    }
}

/// Cumulative semantics: bracket M's `step_reached` becomes
/// `max(exclusive_step_reached[0..=M])`.
fn apply_prefix_max(out: &mut [u8]) {
    let mut running = 0u8;
    for slot in out.iter_mut() {
        if *slot > running {
            running = *slot;
        }
        *slot = running;
    }
}

/// Append output rows for one `(entity, binding track)` from a precomputed
/// per-bracket `step_reached` array.
///
/// Selection rule (post-cumulative):
/// - With `emit_all`: emit every bracket (N rows).
/// - Without `emit_all`: emit only brackets where the final step
///   completed (`step_reached == num_steps`). For exclusive that is at
///   most one bracket; for cumulative that is a contiguous tail.
fn push_rows_for_track<'a>(
    rows: &mut Vec<OutputRow<'a>>,
    per_bracket: &[u8],
    spec: &BracketSpec,
    match_duration: Option<i64>,
    num_steps: u8,
    bindings: &'a [BindingValue],
    emit_all: bool,
) {
    for (b, step) in per_bracket.iter().enumerate() {
        let emit = emit_all || *step == num_steps;
        if !emit {
            continue;
        }
        rows.push(OutputRow {
            match_duration,
            step_reached: *step,
            bindings,
            bracket_idx: Some(b as i64),
            bracket_end: Some(spec.durations[b]),
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Column builders
// ─────────────────────────────────────────────────────────────────────────────

/// Build the `entity_id` column. Filled with placeholder values — the
/// `EntityOperatorAdapter` overwrites these with the actual entity ID.
///
/// Produces a `StringViewArray` for string entity keys or an `Int64Array`
/// for integer entity keys, matching the schema's declared data type.
fn build_entity_id_column(num_rows: usize, data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Array::builder(num_rows);
            for _ in 0..num_rows {
                builder.append_value(0);
            }
            Arc::new(builder.finish())
        }
        _ => {
            let mut builder = StringViewBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                builder.append_value("");
            }
            Arc::new(builder.finish())
        }
    }
}

/// Build the `match_duration` column. Completions report
/// `final_ts - anchor_ts`; partial / EMIT-ALL rows report NULL.
fn build_match_duration_column(rows: &[OutputRow<'_>]) -> ArrayRef {
    let mut builder = Int64Array::builder(rows.len());
    for row in rows {
        match row.match_duration {
            Some(d) => builder.append_value(d),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Build the `step_reached` column.
fn build_step_reached_column(rows: &[OutputRow<'_>]) -> ArrayRef {
    let mut builder = Int64Array::builder(rows.len());
    for row in rows {
        builder.append_value(row.step_reached as i64);
    }
    Arc::new(builder.finish())
}

/// Build the `bracket` column. Bracket-aware rows always carry an
/// index; if BRACKETS is unset the schema does not include this column
/// at all, so the `None` branch only fires on a schema/row mismatch
/// (debug-asserted to catch the bug rather than silently emit NULL into
/// a non-nullable column).
fn build_bracket_idx_column(rows: &[OutputRow<'_>]) -> ArrayRef {
    let mut builder = Int64Array::builder(rows.len());
    for row in rows {
        debug_assert!(
            row.bracket_idx.is_some(),
            "bracket column requested for a row without a bracket index"
        );
        match row.bracket_idx {
            Some(b) => builder.append_value(b),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Build the `bracket_end` column. Carries the anchor-relative bracket
/// upper-bound duration in nanoseconds (per query-language.md §4.12).
fn build_bracket_end_column(rows: &[OutputRow<'_>]) -> ArrayRef {
    let mut builder = Int64Array::builder(rows.len());
    for row in rows {
        debug_assert!(
            row.bracket_end.is_some(),
            "bracket_end column requested for a row without a bracket end"
        );
        match row.bracket_end {
            Some(e) => builder.append_value(e),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

/// Build a variable-binding column. `binding_idx` selects which binding
/// to extract; the Arrow data type comes from the schema field.
fn build_binding_column(
    rows: &[OutputRow<'_>],
    binding_idx: usize,
    data_type: &DataType,
) -> ArrayRef {
    let total = rows.len();
    match data_type {
        DataType::Utf8View => {
            let mut builder = StringViewBuilder::with_capacity(total);
            for row in rows {
                match row.bindings.get(binding_idx) {
                    Some(BindingValue::String(s)) => builder.append_value(s.as_str()),
                    _ => builder.append_value(""),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Array::builder(total);
            for row in rows {
                match row.bindings.get(binding_idx) {
                    Some(BindingValue::Int(v)) => builder.append_value(*v),
                    _ => builder.append_value(0),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Array::builder(total);
            for row in rows {
                match row.bindings.get(binding_idx) {
                    Some(BindingValue::Float(f)) => builder.append_value(f.0),
                    _ => builder.append_value(0.0),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanArray::builder(total);
            for row in rows {
                match row.bindings.get(binding_idx) {
                    Some(BindingValue::Bool(b)) => builder.append_value(*b),
                    _ => builder.append_value(false),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let mut builder = TimestampNanosecondArray::builder(total);
            for row in rows {
                match row.bindings.get(binding_idx) {
                    Some(BindingValue::Timestamp(t)) => builder.append_value(*t),
                    _ => builder.append_value(0),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            let mut builder = StringViewBuilder::with_capacity(total);
            for row in rows {
                match row.bindings.get(binding_idx) {
                    Some(bv) => builder.append_value(format!("{bv:?}")),
                    None => builder.append_value(""),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

/// Build a typed NULL column for unimplemented output fields.
fn build_null_column(num_rows: usize, data_type: &DataType) -> ArrayRef {
    new_null_array(data_type, num_rows)
}

/// Build an empty `RecordBatch` with the given schema.
fn empty_batch(output_schema: &OperatorSchema) -> RecordBatch {
    let arrow_schema = Arc::new(output_schema.to_arrow_schema());
    RecordBatch::new_empty(arrow_schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use bqlite_ast::span::Span;
    use bqlite_core::{BqlType, ColumnDef};

    fn schema_with_duration() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "match_duration".into(),
                bql_type: BqlType::Int,
                nullable: true,
                default_value: None,
            },
        ])
        .unwrap()
    }

    fn schema_with_step_reached() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "step_reached".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            },
        ])
        .unwrap()
    }

    fn schema_with_brackets(extra: &[ColumnDef]) -> OperatorSchema {
        let mut cols = vec![
            ColumnDef {
                name: "entity_id".into(),
                bql_type: BqlType::String,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "step_reached".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "bracket".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            },
            ColumnDef {
                name: "bracket_end".into(),
                bql_type: BqlType::Int,
                nullable: false,
                default_value: None,
            },
        ];
        cols.extend_from_slice(extra);
        OperatorSchema::new(cols).unwrap()
    }

    fn brackets(durations: &[i64], cumulative: bool) -> BracketSpec {
        BracketSpec {
            durations: durations.to_vec(),
            cumulative,
            span: Span::EMPTY,
        }
    }

    fn pull_int(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        let arr = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..arr.len())
            .map(|i| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i))
                }
            })
            .collect()
    }

    // ── Pre-TASK-529 contract: no brackets ─────────────────────────────

    #[test]
    fn empty_results_produce_empty_batch() {
        let schema = schema_with_duration();
        let batch = build_output_batch(&schema, &[], &[], false, 3, None);
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn completions_produce_duration() {
        let schema = schema_with_duration();
        let completions = vec![
            MatchCompletion {
                anchor_ts: 100,
                final_ts: 400,
                bindings: Vec::new(),
            },
            MatchCompletion {
                anchor_ts: 500,
                final_ts: 900,
                bindings: Vec::new(),
            },
        ];
        let batch = build_output_batch(&schema, &completions, &[], false, 2, None);
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(
            pull_int(&batch, "match_duration"),
            vec![Some(300), Some(400)]
        );
    }

    #[test]
    fn emit_all_includes_partials_with_step_reached() {
        let schema = schema_with_step_reached();
        let completions = vec![MatchCompletion {
            anchor_ts: 100,
            final_ts: 300,
            bindings: Vec::new(),
        }];
        let partials = vec![PartialMatch {
            anchor_ts: 400,
            step_reached: 2,
            bindings: Vec::new(),
        }];
        let batch = build_output_batch(&schema, &completions, &partials, true, 3, None);
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(pull_int(&batch, "step_reached"), vec![Some(3), Some(2)]);
    }

    #[test]
    fn partials_ignored_without_emit_all() {
        let schema = schema_with_step_reached();
        let completions = vec![MatchCompletion {
            anchor_ts: 100,
            final_ts: 300,
            bindings: Vec::new(),
        }];
        let partials = vec![PartialMatch {
            anchor_ts: 400,
            step_reached: 1,
            bindings: Vec::new(),
        }];
        let batch = build_output_batch(&schema, &completions, &partials, false, 3, None);
        assert_eq!(batch.num_rows(), 1);
    }

    // ── TASK-529: BRACKETS exclusive ───────────────────────────────────

    #[test]
    fn brackets_exclusive_completion_in_first_bracket_emit_all() {
        // 2-step pattern, brackets [1d, 7d, 14d, 30d], completion at delta=0.5d.
        // delta=0.5d ∈ [0, 1d] → bracket 0.
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 12 * 60 * 60 * 1_000_000_000, // 12 hours
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            false,
        );
        let batch = build_output_batch(&schema, &completions, &[], true, 2, Some(&spec));
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![Some(2), Some(0), Some(0), Some(0)]
        );
        assert_eq!(
            pull_int(&batch, "bracket"),
            vec![Some(0), Some(1), Some(2), Some(3)]
        );
        assert_eq!(
            pull_int(&batch, "bracket_end"),
            vec![
                Some(86_400_000_000_000),
                Some(7 * 86_400_000_000_000),
                Some(14 * 86_400_000_000_000),
                Some(30 * 86_400_000_000_000),
            ]
        );
    }

    #[test]
    fn brackets_exclusive_completion_in_middle_bracket_emit_all() {
        // delta = 9d → bracket 2 (`(7d, 14d]`).
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 9 * 86_400_000_000_000,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            false,
        );
        let batch = build_output_batch(&schema, &completions, &[], true, 2, Some(&spec));
        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![Some(1), Some(0), Some(2), Some(0)]
        );
    }

    #[test]
    fn brackets_exclusive_completion_past_max_bucket_emit_all() {
        // delta = 50d > max_bracket=30d → completion not in any bracket.
        // Bracket 0 still has step 1 (anchor); others 0.
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 50 * 86_400_000_000_000,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            false,
        );
        let batch = build_output_batch(&schema, &completions, &[], true, 2, Some(&spec));
        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![Some(1), Some(0), Some(0), Some(0)]
        );
    }

    #[test]
    fn brackets_cumulative_completion_is_monotone_emit_all() {
        // delta = 9d → bracket 2 exclusively. Cumulative: brackets 2..3
        // see step_reached=2; brackets 0..1 see step_reached=1.
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 9 * 86_400_000_000_000,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            true,
        );
        let batch = build_output_batch(&schema, &completions, &[], true, 2, Some(&spec));
        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![Some(1), Some(1), Some(2), Some(2)]
        );
    }

    #[test]
    fn brackets_partial_only_under_emit_all() {
        // Partial: only step 1 reached. Bracket 0 → 1, others 0.
        let schema = schema_with_brackets(&[]);
        let partials = vec![PartialMatch {
            anchor_ts: 0,
            step_reached: 1,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            false,
        );
        let batch = build_output_batch(&schema, &[], &partials, true, 2, Some(&spec));
        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![Some(1), Some(0), Some(0), Some(0)]
        );
        assert_eq!(
            pull_int(&batch, "bracket"),
            vec![Some(0), Some(1), Some(2), Some(3)]
        );
    }

    #[test]
    fn brackets_partial_cumulative_propagates_anchor_to_all_brackets() {
        let schema = schema_with_brackets(&[]);
        let partials = vec![PartialMatch {
            anchor_ts: 0,
            step_reached: 1,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            true,
        );
        let batch = build_output_batch(&schema, &[], &partials, true, 2, Some(&spec));
        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![Some(1), Some(1), Some(1), Some(1)]
        );
    }

    // ── TASK-529: BRACKETS without EMIT ALL ─────────────────────────────

    #[test]
    fn brackets_without_emit_all_emits_only_completed_bracket_exclusive() {
        // delta=9d → bracket 2. Without EMIT ALL: only bracket 2 row.
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 9 * 86_400_000_000_000,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            false,
        );
        let batch = build_output_batch(&schema, &completions, &[], false, 2, Some(&spec));
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(pull_int(&batch, "bracket"), vec![Some(2)]);
        assert_eq!(pull_int(&batch, "step_reached"), vec![Some(2)]);
    }

    #[test]
    fn brackets_without_emit_all_emits_contiguous_tail_cumulative() {
        // delta=9d → bracket 2 exclusively; cumulative emits brackets 2 & 3.
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 9 * 86_400_000_000_000,
            bindings: Vec::new(),
        }];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            true,
        );
        let batch = build_output_batch(&schema, &completions, &[], false, 2, Some(&spec));
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(pull_int(&batch, "bracket"), vec![Some(2), Some(3)]);
        assert_eq!(pull_int(&batch, "step_reached"), vec![Some(2), Some(2)]);
    }

    #[test]
    fn brackets_without_emit_all_completion_past_max_emits_zero_rows() {
        let schema = schema_with_brackets(&[]);
        let completions = vec![MatchCompletion {
            anchor_ts: 0,
            final_ts: 100 * 86_400_000_000_000, // way past max_bracket
            bindings: Vec::new(),
        }];
        let spec = brackets(&[86_400_000_000_000, 7 * 86_400_000_000_000], false);
        let batch = build_output_batch(&schema, &completions, &[], false, 2, Some(&spec));
        assert_eq!(batch.num_rows(), 0);
    }

    // ── TASK-529: BRACKETS × variable bindings ──────────────────────────

    #[test]
    fn brackets_with_bindings_carry_binding_values_per_bracket_row() {
        // 2-step + brackets + a single $plan binding. Two completions
        // with different plan values. All four bracket rows for each
        // completion must carry the binding value.
        let schema = schema_with_brackets(&[ColumnDef {
            name: "$plan".into(),
            bql_type: BqlType::String,
            nullable: false,
            default_value: None,
        }]);
        let completions = vec![
            MatchCompletion {
                anchor_ts: 0,
                final_ts: 9 * 86_400_000_000_000,
                bindings: vec![BindingValue::String("free".into())],
            },
            MatchCompletion {
                anchor_ts: 0,
                final_ts: 20 * 86_400_000_000_000,
                bindings: vec![BindingValue::String("pro".into())],
            },
        ];
        let spec = brackets(
            &[
                86_400_000_000_000,
                7 * 86_400_000_000_000,
                14 * 86_400_000_000_000,
                30 * 86_400_000_000_000,
            ],
            false,
        );
        let batch = build_output_batch(&schema, &completions, &[], true, 2, Some(&spec));
        assert_eq!(batch.num_rows(), 8);

        let plan = batch
            .column_by_name("$plan")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringViewArray>()
            .unwrap();
        let plans: Vec<&str> = (0..plan.len()).map(|i| plan.value(i)).collect();
        assert_eq!(
            plans,
            vec!["free", "free", "free", "free", "pro", "pro", "pro", "pro"]
        );

        assert_eq!(
            pull_int(&batch, "step_reached"),
            vec![
                Some(1),
                Some(0),
                Some(2),
                Some(0), // free, delta=9d → bracket 2
                Some(1),
                Some(0),
                Some(0),
                Some(2), // pro, delta=20d → bracket 3
            ]
        );
    }

    // ── TASK-529: cumulative monotonicity property ──────────────────────

    use proptest::prelude::*;

    fn arb_durations() -> impl Strategy<Value = Vec<i64>> {
        // 1..=8 strictly ascending positive durations.
        prop::collection::vec(1i64..1_000, 1..=8).prop_map(|mut v| {
            v.sort();
            v.dedup();
            v
        })
    }

    proptest! {
        #[test]
        fn prop_cumulative_step_reached_is_monotone(
            durations in arb_durations(),
            // anchor at 0; final between 0 and 2 * max_duration so we
            // hit "in range", "out of range", and boundary cases.
            final_offset in 0i64..2_000,
            num_steps in 1u8..=4,
        ) {
            let n = durations.len();
            let spec = BracketSpec { durations: durations.clone(), cumulative: true, span: Span::EMPTY };
            let completions = [MatchCompletion {
                anchor_ts: 0,
                final_ts: final_offset,
                bindings: Vec::new(),
            }];
            let mut per_bracket = vec![0u8; n];
            compute_completion_steps(&mut per_bracket, &completions[0], num_steps, &spec);
            apply_prefix_max(&mut per_bracket);

            // Monotone non-decreasing.
            for w in per_bracket.windows(2) {
                prop_assert!(w[0] <= w[1], "cumulative step_reached must be monotone");
            }
        }

        #[test]
        fn prop_cumulative_partial_is_monotone(
            durations in arb_durations(),
            step_reached in 1u8..=4,
        ) {
            let n = durations.len();
            // The partial path doesn't read `BracketSpec.durations`
            // beyond `n`; constructing the spec here would only be
            // window-dressing.
            let _ = durations;
            let partial = PartialMatch { anchor_ts: 0, step_reached, bindings: Vec::new() };
            let mut per_bracket = vec![0u8; n];
            compute_partial_steps(&mut per_bracket, &partial);
            apply_prefix_max(&mut per_bracket);
            for w in per_bracket.windows(2) {
                prop_assert!(w[0] <= w[1]);
            }
        }
    }
}
