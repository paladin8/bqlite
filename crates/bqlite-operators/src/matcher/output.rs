//! Output `RecordBatch` construction from match results.
//!
//! Converts the internal match results ([`MatchCompletion`] and
//! [`PartialMatch`]) into Arrow `RecordBatch` conforming to the
//! demand-reduced output schema (match-operator.md §9).
//!
//! ## Step indexing
//!
//! `PartialMatch::step_reached` is 0-indexed (matching the compiled NFA).
//! The user-facing `step_reached` column is 1-indexed. This module
//! handles the translation.

use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewBuilder,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_core::OperatorSchema;

use super::bindings::BindingValue;
use super::nfa::{MatchCompletion, PartialMatch};

/// Build an output `RecordBatch` from completions and partials.
///
/// The batch schema is derived from `output_schema`. Columns are
/// populated based on their name:
///
/// - `entity_id`: Currently set to empty string (the adapter fills it).
/// - `match_duration`: `final_ts - anchor_ts` for completions, NULL for partials.
/// - `step_reached`: 1-indexed step number. For completions, `num_steps`.
///   For partials, `partial.step_reached + 1` (0→1 translation).
/// - `match_events`: Not yet implemented (requires NFA path tracking).
///
/// Returns a batch with `completions.len() + partials.len()` rows.
pub fn build_output_batch(
    output_schema: &OperatorSchema,
    completions: &[MatchCompletion],
    partials: &[PartialMatch],
    emit_all: bool,
    num_steps: u8,
) -> RecordBatch {
    let total_rows = completions.len() + if emit_all { partials.len() } else { 0 };
    if total_rows == 0 {
        // Return an empty batch with the correct schema.
        return empty_batch(output_schema);
    }

    let arrow_schema = output_schema.to_arrow_schema();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

    // Identify which variable binding columns exist in the schema. Variable
    // binding columns are named `$<name>` and their values come from the
    // `bindings` field on MatchCompletion/PartialMatch.
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
            "entity_id" => build_entity_id_column(total_rows, field.data_type()),
            "match_duration" => build_match_duration_column(completions, partials, emit_all),
            "step_reached" => build_step_reached_column(completions, partials, emit_all, num_steps),
            name if name.starts_with('$') => {
                // Variable binding column — extract binding values.
                let binding_idx = var_col_indices
                    .iter()
                    .find(|(fi, _)| *fi == field_idx)
                    .map(|(_, bi)| *bi)
                    .unwrap();
                build_binding_column(
                    completions,
                    partials,
                    emit_all,
                    binding_idx,
                    field.data_type(),
                )
            }
            _ => {
                // Unimplemented column (match_events, step properties) —
                // emit typed NULL array matching the schema field's data type.
                build_null_column(total_rows, field.data_type())
            }
        };
        columns.push(col);
    }

    RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap_or_else(|e| {
        panic!("failed to build output RecordBatch: {e}");
    })
}

/// Build the `entity_id` column. Filled with placeholder values — the
/// `EntityOperatorAdapter` replaces these with the actual entity ID.
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
            // Default to StringView for string entity keys.
            let mut builder = StringViewBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                builder.append_value("");
            }
            Arc::new(builder.finish())
        }
    }
}

/// Build the `match_duration` column.
///
/// For completions: `final_ts - anchor_ts`.
/// For partials (EMIT ALL): NULL.
fn build_match_duration_column(
    completions: &[MatchCompletion],
    partials: &[PartialMatch],
    emit_all: bool,
) -> ArrayRef {
    let total = completions.len() + if emit_all { partials.len() } else { 0 };
    let mut builder = Int64Array::builder(total);

    for c in completions {
        builder.append_value(c.final_ts - c.anchor_ts);
    }
    if emit_all {
        for _ in partials {
            builder.append_null();
        }
    }

    Arc::new(builder.finish())
}

/// Build the `step_reached` column.
///
/// For completions: `num_steps` (all steps matched).
/// For partials: `step_reached` (already represents the number of
/// completed steps — the step counter's `max_step_reached` and the
/// NFA's `state_to_step` values are both 1-indexed step counts).
fn build_step_reached_column(
    completions: &[MatchCompletion],
    partials: &[PartialMatch],
    emit_all: bool,
    num_steps: u8,
) -> ArrayRef {
    let total = completions.len() + if emit_all { partials.len() } else { 0 };
    let mut builder = Int64Array::builder(total);

    for _ in completions {
        builder.append_value(num_steps as i64);
    }
    if emit_all {
        for p in partials {
            builder.append_value(p.step_reached as i64);
        }
    }

    Arc::new(builder.finish())
}

/// Build a variable binding column from completion/partial binding values.
///
/// Each `MatchCompletion` and `PartialMatch` carries a `bindings: Vec<BindingValue>`.
/// `binding_idx` selects which binding to extract. The Arrow data type
/// is determined by the schema field.
fn build_binding_column(
    completions: &[MatchCompletion],
    partials: &[PartialMatch],
    emit_all: bool,
    binding_idx: usize,
    data_type: &DataType,
) -> ArrayRef {
    let total = completions.len() + if emit_all { partials.len() } else { 0 };

    // Collect all binding values in row order: completions first, then partials.
    let iter_completions = completions.iter().map(|c| c.bindings.get(binding_idx));
    let iter_partials = if emit_all {
        Some(partials.iter().map(|p| p.bindings.get(binding_idx)))
    } else {
        None
    };

    match data_type {
        DataType::Utf8View => {
            let mut builder = StringViewBuilder::with_capacity(total);
            for bv in iter_completions.chain(iter_partials.into_iter().flatten()) {
                match bv {
                    Some(BindingValue::String(s)) => builder.append_value(s.as_str()),
                    _ => builder.append_value(""),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Array::builder(total);
            for bv in iter_completions.chain(iter_partials.into_iter().flatten()) {
                match bv {
                    Some(BindingValue::Int(v)) => builder.append_value(*v),
                    _ => builder.append_value(0),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Array::builder(total);
            for bv in iter_completions.chain(iter_partials.into_iter().flatten()) {
                match bv {
                    Some(BindingValue::Float(f)) => builder.append_value(f.0),
                    _ => builder.append_value(0.0),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanArray::builder(total);
            for bv in iter_completions.chain(iter_partials.into_iter().flatten()) {
                match bv {
                    Some(BindingValue::Bool(b)) => builder.append_value(*b),
                    _ => builder.append_value(false),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let mut builder = TimestampNanosecondArray::builder(total);
            for bv in iter_completions.chain(iter_partials.into_iter().flatten()) {
                match bv {
                    Some(BindingValue::Timestamp(t)) => builder.append_value(*t),
                    _ => builder.append_value(0),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Fallback for unknown data types — should not be reached
            // for valid plans since the planner constrains binding types.
            let mut builder = StringViewBuilder::with_capacity(total);
            for bv in iter_completions.chain(iter_partials.into_iter().flatten()) {
                match bv {
                    Some(bv) => builder.append_value(format!("{bv:?}")),
                    None => builder.append_value(""),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

/// Build a typed NULL column for unimplemented output fields.
///
/// Uses `new_null_array` to produce a null array matching the schema
/// field's declared data type. This ensures `RecordBatch::try_new`
/// succeeds even for non-Int64 columns (variable bindings, step
/// properties, match_events map, etc.).
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

    #[test]
    fn empty_results_produce_empty_batch() {
        let schema = schema_with_duration();
        let batch = build_output_batch(&schema, &[], &[], false, 3);
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
        let batch = build_output_batch(&schema, &completions, &[], false, 2);
        assert_eq!(batch.num_rows(), 2);

        let durations = batch
            .column_by_name("match_duration")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(durations.value(0), 300);
        assert_eq!(durations.value(1), 400);
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
            step_reached: 2, // 2 steps completed
            bindings: Vec::new(),
        }];
        let batch = build_output_batch(&schema, &completions, &partials, true, 3);
        assert_eq!(batch.num_rows(), 2);

        let steps = batch
            .column_by_name("step_reached")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Completion: step_reached = num_steps = 3
        assert_eq!(steps.value(0), 3);
        // Partial: 2 steps completed → step_reached = 2
        assert_eq!(steps.value(1), 2);
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
        // emit_all = false: partials should be excluded.
        let batch = build_output_batch(&schema, &completions, &partials, false, 3);
        assert_eq!(batch.num_rows(), 1);
    }
}
