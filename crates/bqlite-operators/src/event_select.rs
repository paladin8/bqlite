//! `EventSelectOperator` — per-entity FIRST / LAST / NTH event selection.
//!
//! Implements `FIRST`, `LAST`, and `NTH` as a single
//! [`EventSelectOperator`] parameterized by [`EventSelectKind`].
//! The operator is an [`EntityOperator`] that scans one entity's
//! events at a time (via the `EntityOperatorAdapter` sub-batch
//! protocol) and emits **exactly 0 or 1 rows per entity** — the
//! selected qualifying event or nothing when no qualifying event
//! exists.
//!
//! ## Selection semantics
//!
//! 1. **Event-type filter**: rows whose `event_type` is not in the
//!    configured set are skipped immediately.
//! 2. **WHERE predicate**: applied per-qualifying row before position
//!    selection.
//! 3. **Position selection**: FIRST picks the earliest qualifying
//!    row by `(ts, __seq_id)` ascending; LAST picks the latest;
//!    NTH(n) picks the n-th (1-indexed).
//! 4. **Omission**: entities with no qualifying row produce no output.
//!
//! See `docs/design/operators/event-select-sample.md` Block A for the
//! full specification.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray,
    StringViewBuilder, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_core::{EntityId, OperatorSchema, ScalarValue};
use bqlite_planner::compiled::CompiledExpr;
use bqlite_planner::demand::{CompiledFusableAggregate, DemandCapabilities};
use bqlite_planner::logical::EventSelectKind;
use bqlite_planner::physical::EventSelectPhysical;

use crate::eval;
use crate::operator::EntityOperator;

// ─────────────────────────────────────────────────────────────────────────────
// Column-name constants
// ─────────────────────────────────────────────────────────────────────────────

const ENTITY_ID_COL: &str = "entity_id";
const TS_COL: &str = "ts";
const EVENT_TYPE_COL: &str = "event_type";

// ─────────────────────────────────────────────────────────────────────────────
// EventSelectInputMap
// ─────────────────────────────────────────────────────────────────────────────

/// Pre-resolved column indices for the always-needed columns.
///
/// Resolved once at operator construction from the input schema so
/// per-row access is an array index lookup rather than a name lookup.
///
/// ## Why there is no `seq_id_idx`
///
/// Same-`ts` tie-breaking is *defined* by `__seq_id` ordering
/// (`docs/design/operators/event-select-sample.md` §6), but the
/// operator does not need to physically read `__seq_id` values. The
/// scan runtime guarantees rows are emitted in `(entity_id, ts,
/// __seq_id)` ascending order (`bqlite-operators::scan` module docs
/// and `KWayMergeScan`'s sort key), so within a single sub-batch the
/// positional row order IS the `__seq_id` order. FIRST takes the first
/// qualifying row, LAST overwrites as rows arrive, and NTH counts in
/// arrival order — all three are equivalent to ascending-by-`__seq_id`
/// tie-breaking. Resolving `__seq_id` from the input schema would
/// couple this operator to scan-layer system-column materialisation
/// (a feature the Wave 2 scan does not yet expose, per
/// `bqlite-operators::scan` module header) without changing behaviour.
#[derive(Debug, Clone)]
pub struct EventSelectInputMap {
    /// Index of `entity_id` in the input batch.
    pub entity_id_idx: usize,
    /// Index of `ts` (timestamp) in the input batch.
    pub ts_idx: usize,
    /// Index of `event_type` in the input batch.
    pub event_type_idx: usize,
}

// ─────────────────────────────────────────────────────────────────────────────
// EventTypeCodeSet — per-batch dictionary optimisation
// ─────────────────────────────────────────────────────────────────────────────

/// Per-batch integer code set for efficient event-type membership
/// testing on dictionary-encoded `event_type` columns.
///
/// When the input batch has `event_type` encoded as
/// `DictionaryArray<Int32, Utf8View>`, the dictionary string values
/// are resolved once against the operator's event-type set and the
/// matching integer codes are stored here.  Per-row membership is
/// then an integer lookup (`O(1)`, no string comparison).
struct EventTypeCodeSet {
    matching_codes: HashSet<i32>,
}

impl EventTypeCodeSet {
    /// Build the code set from a dictionary column's value array and
    /// the operator's configured event-type set.
    fn from_dict(dict_values: &StringViewArray, event_types: &HashSet<String>) -> Self {
        let mut matching_codes = HashSet::new();
        for i in 0..dict_values.len() {
            if !dict_values.is_null(i) && event_types.contains(dict_values.value(i)) {
                matching_codes.insert(i as i32);
            }
        }
        EventTypeCodeSet { matching_codes }
    }

    #[inline]
    fn code_matches(&self, code: i32) -> bool {
        self.matching_codes.contains(&code)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CandidateRow
// ─────────────────────────────────────────────────────────────────────────────

/// A retained candidate row for one entity.
///
/// Stores one [`ScalarValue`] per column in the operator's output
/// schema.  Only the demanded forwarded columns are represented —
/// columns excluded by demand pruning never enter this struct.
#[derive(Debug)]
struct CandidateRow {
    /// One value per column in the operator's output schema.
    values: Vec<ScalarValue>,
}

// ─────────────────────────────────────────────────────────────────────────────
// EventSelectCandidate
// ─────────────────────────────────────────────────────────────────────────────

/// Selection-mode-specific per-entity state.
#[derive(Debug)]
enum EventSelectCandidate {
    /// FIRST: retain the first qualifying event (ascending input).
    First { row: Option<CandidateRow> },
    /// LAST: continuously overwrite with the latest qualifying event.
    Last { row: Option<CandidateRow> },
    /// NTH(n): count qualifying events; retain the n-th.
    Nth {
        n: u32,
        qualifying_count: u32,
        row: Option<CandidateRow>,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// EventSelectState
// ─────────────────────────────────────────────────────────────────────────────

/// Per-entity mutable state for [`EventSelectOperator`].
///
/// Created fresh by [`EventSelectOperator::create_state`] for each
/// entity and consumed by [`EventSelectOperator::finish_entity`].
pub struct EventSelectState {
    candidate: EventSelectCandidate,
    /// Set to `true` when FIRST or NTH has found its qualifying event.
    /// All remaining sub-batch rows for the entity are skipped.
    done: bool,
}

impl EventSelectState {
    #[inline]
    fn is_done(&self) -> bool {
        self.done
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EventSelectOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Physical `EntityOperator` for `FIRST`, `LAST`, and `NTH` operators.
///
/// The operator struct is immutable and `Send + Sync`; per-entity state
/// lives in [`EventSelectState`].  Constructed from an
/// [`EventSelectPhysical`] descriptor by the engine bind step (TASK-438).
///
/// See `docs/design/operators/event-select-sample.md` Block A for the
/// complete specification.
pub struct EventSelectOperator {
    /// Selection mode (FIRST, LAST, NTH(n)).
    kind: EventSelectKind,

    /// Event types eligible for selection (O(1) membership check).
    event_types: HashSet<String>,

    /// Optional compiled per-event WHERE predicate.
    predicate: Option<CompiledExpr>,

    /// Output schema (demanded forwarded columns + always-needed cols).
    output_schema: OperatorSchema,

    /// Pre-resolved always-needed column indices in the input batch.
    input_map: EventSelectInputMap,

    /// Maps each position in `output_schema.columns()` to the
    /// corresponding column index in the input batch.
    ///
    /// `output_col_to_input_idx[i]` is `Some(idx)` when the column
    /// exists in the input, `None` when it must be emitted as NULL
    /// (e.g. columns added by demand but absent from the input).
    output_col_to_input_idx: Vec<Option<usize>>,

    /// Column names required from the input batch (drives scan
    /// projection pruning in the engine's physical planning).
    required_column_names: Vec<String>,

    /// Fused aggregate descriptor when stateful-to-aggregate fusion is
    /// active (TASK-520). When `Some`, the physical descriptor's
    /// `output_schema` is the aggregate's output, while
    /// `pre_fusion_output_schema` carries the operator's native single-
    /// row-per-entity schema. The runtime always emits per-entity rows
    /// in the native schema; the fused adapter feeds them into a
    /// downstream `HashAccumulator`.
    fused_aggregate: Option<CompiledFusableAggregate>,
}

impl EventSelectOperator {
    /// Construct from a physical plan descriptor.
    ///
    /// `input_schema` is the output schema of the child physical plan
    /// (i.e. `desc.input.output_schema()`). Column indices are
    /// resolved once here so per-row access is branchless.
    pub fn new(desc: &EventSelectPhysical, input_schema: &OperatorSchema) -> Self {
        // TASK-520: stateful-to-aggregate fusion is now supported. When
        // the planner replaces `desc.output_schema` with the aggregate's,
        // the operator continues to build per-entity batches in the
        // native (pre-fusion) schema so `HashAccumulator::update_batch`
        // can resolve forwarded columns by name.
        let native_output_schema = desc.native_output_schema().clone();

        // Nth(0) is invalid: the spec says n >= 1 (event-select-sample.md
        // §4.2). The parser/planner enforces this; guard defensively here so
        // a programmatically-constructed descriptor with Nth(0) fails fast.
        if let EventSelectKind::Nth(0) = desc.kind {
            panic!("EventSelectOperator: Nth(0) is invalid; n must be >= 1");
        }

        // Build event-type set.
        let event_types: HashSet<String> = desc.event_types.iter().cloned().collect();

        // Pre-resolve always-needed indices in the input schema.
        let find_required = |name: &str| {
            input_schema
                .column(name)
                .unwrap_or_else(|| panic!("required column '{name}' not found in input schema"))
                .0
        };
        let input_map = EventSelectInputMap {
            entity_id_idx: find_required(ENTITY_ID_COL),
            ts_idx: find_required(TS_COL),
            event_type_idx: find_required(EVENT_TYPE_COL),
        };

        // Map each output schema column to its input index (may be None
        // for columns not present in the input — emitted as NULL).
        // Resolved against the native (pre-fusion) schema so per-entity
        // candidate-row construction keeps the same shape regardless of
        // whether the descriptor advertises an aggregate output.
        let output_col_to_input_idx: Vec<Option<usize>> = native_output_schema
            .columns()
            .iter()
            .map(|col| input_schema.column(&col.name).map(|(idx, _)| idx))
            .collect();

        // Build required column names:
        // always-needed + native output columns + predicate columns.
        // `__seq_id` is intentionally not required — see the doc on
        // `EventSelectInputMap`.
        let mut required = vec![
            ENTITY_ID_COL.to_string(),
            TS_COL.to_string(),
            EVENT_TYPE_COL.to_string(),
        ];
        // Add native-output columns.
        for col in native_output_schema.columns() {
            if !required.contains(&col.name) {
                required.push(col.name.clone());
            }
        }
        // Add columns referenced by the WHERE predicate (if any).
        if let Some(pred) = &desc.predicate {
            collect_predicate_columns(pred, &mut required);
        }

        Self {
            kind: desc.kind,
            event_types,
            predicate: desc.predicate.clone(),
            output_schema: native_output_schema,
            input_map,
            output_col_to_input_idx,
            required_column_names: required,
            fused_aggregate: desc.fused_aggregate.clone(),
        }
    }

    /// Returns `true` when stateful-to-aggregate fusion is active.
    pub fn is_fused(&self) -> bool {
        self.fused_aggregate.is_some()
    }

    /// Access the fused aggregate descriptor when fusion is active.
    pub fn fused_aggregate(&self) -> Option<&CompiledFusableAggregate> {
        self.fused_aggregate.as_ref()
    }

    /// Extract a [`CandidateRow`] for the qualifying event at `row_idx`
    /// in `batch`.  Only columns mapped in `output_col_to_input_idx`
    /// are extracted; unmapped columns become `ScalarValue::Null`.
    #[inline]
    fn extract_candidate_row(&self, batch: &RecordBatch, row_idx: usize) -> CandidateRow {
        let values = self
            .output_col_to_input_idx
            .iter()
            .enumerate()
            .map(|(out_idx, maybe_in_idx)| match maybe_in_idx {
                None => ScalarValue::Null,
                Some(in_idx) => {
                    // Use the output schema column type as a hint when
                    // the input might be dictionary-encoded.
                    let col = &self.output_schema.columns()[out_idx];
                    extract_scalar(batch.column(*in_idx).as_ref(), row_idx, &col.bql_type)
                }
            })
            .collect();
        CandidateRow { values }
    }

    /// Build a single-row output [`RecordBatch`] from a candidate row.
    fn build_output_batch(&self, row: CandidateRow) -> RecordBatch {
        let arrow_schema = self.output_schema.to_arrow_schema();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

        for (field, value) in arrow_schema.fields().iter().zip(row.values.iter()) {
            columns.push(scalar_to_single_row_array(value, field.data_type()));
        }

        RecordBatch::try_new(Arc::new(arrow_schema), columns)
            .unwrap_or_else(|e| panic!("EventSelectOperator: failed to build output batch: {e}"))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityOperator implementation
// ─────────────────────────────────────────────────────────────────────────────

impl EntityOperator for EventSelectOperator {
    type State = EventSelectState;

    fn create_state(&self, _entity_id: &EntityId) -> EventSelectState {
        let candidate = match self.kind {
            EventSelectKind::First => EventSelectCandidate::First { row: None },
            EventSelectKind::Last => EventSelectCandidate::Last { row: None },
            EventSelectKind::Nth(n) => EventSelectCandidate::Nth {
                n,
                qualifying_count: 0,
                row: None,
            },
        };
        EventSelectState {
            candidate,
            done: false,
        }
    }

    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn process_sub_batch(&self, state: &mut EventSelectState, batch: &RecordBatch) {
        // Early termination for FIRST and NTH once their candidate is
        // found (design doc §8.3, §8.4).
        if state.is_done() {
            return;
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return;
        }

        // Resolve the event_type column: try Dictionary<Int32, Utf8View>
        // (common encoded form) then fall back to StringViewArray.
        let event_type_col = batch.column(self.input_map.event_type_idx);
        let event_type_check = build_event_type_check(event_type_col, &self.event_types);

        // Evaluate the WHERE predicate across the whole sub-batch once
        // (vectorised) if present. NULL → treat as false (SQL WHERE
        // semantics), consistent with FilterOperator.
        //
        // Fail-safe: if `evaluate` returns an error (e.g., unsupported
        // FunctionCall like LIKE/REGEX) or produces a non-BooleanArray, we
        // treat every row in this batch as non-qualifying rather than
        // accidentally qualifying rows against a broken predicate.  This
        // ensures EventSelectOperator never over-selects events.
        let predicate_mask: Option<BooleanArray> = if let Some(pred) = self.predicate.as_ref() {
            match eval::evaluate(pred, batch) {
                Ok(arr) => match arr.as_any().downcast_ref::<BooleanArray>() {
                    Some(b) => Some(b.clone()),
                    None => return, // Unexpected result type — fail safe, skip batch.
                },
                Err(_) => return, // Eval failed — fail safe, skip batch.
            }
        } else {
            None
        };

        for row_idx in 0..num_rows {
            // Step 1: event-type filter (§5.1).
            if !event_type_check.matches(event_type_col.as_ref(), row_idx) {
                continue;
            }

            // Step 2: WHERE predicate (§5.2).
            if let Some(mask) = &predicate_mask {
                if mask.is_null(row_idx) || !mask.value(row_idx) {
                    continue;
                }
            }

            // This row is a qualifying event.
            match &mut state.candidate {
                EventSelectCandidate::First { row } => {
                    if row.is_none() {
                        *row = Some(self.extract_candidate_row(batch, row_idx));
                        state.done = true;
                        return; // No more rows needed for this entity.
                    }
                }
                EventSelectCandidate::Last { row } => {
                    // Always overwrite — last qualifying event so far
                    // (input is ascending so the last encountered is the latest).
                    *row = Some(self.extract_candidate_row(batch, row_idx));
                    // No early termination for LAST.
                }
                EventSelectCandidate::Nth {
                    n,
                    qualifying_count,
                    row,
                } => {
                    *qualifying_count += 1;
                    if *qualifying_count == *n {
                        *row = Some(self.extract_candidate_row(batch, row_idx));
                        state.done = true;
                        return; // No more rows needed for this entity.
                    }
                }
            }
        }
    }

    fn finish_entity(&self, state: EventSelectState) -> Option<RecordBatch> {
        let row = match state.candidate {
            EventSelectCandidate::First { row } => row,
            EventSelectCandidate::Last { row } => row,
            EventSelectCandidate::Nth { row, .. } => row,
        };
        // Entity is omitted if no qualifying event was found (§5.5).
        let row = row?;
        Some(self.build_output_batch(row))
    }

    fn required_columns(&self) -> &[String] {
        &self.required_column_names
    }

    fn supported_demands(&self) -> DemandCapabilities {
        // Mirrors `EventSelectPhysical::DEMAND_CAPS`. TASK-520 turned on
        // `supports_aggregation_fusion`.
        EventSelectPhysical::DEMAND_CAPS
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event-type check strategy
// ─────────────────────────────────────────────────────────────────────────────

/// Resolved per-batch event-type membership check strategy.
///
/// Chosen at sub-batch entry based on the encoding of the input
/// `event_type` column.
enum EventTypeCheck<'a> {
    /// `event_type` is `StringViewArray` — compare strings directly.
    StringView {
        arr: &'a StringViewArray,
        set: &'a HashSet<String>,
    },
    /// `event_type` is `DictionaryArray<Int32, Utf8View>` — code lookup.
    Dict {
        keys: &'a arrow::array::Int32Array,
        codes: EventTypeCodeSet,
    },
    /// Column format not recognised — always returns false.
    Unknown,
}

impl<'a> EventTypeCheck<'a> {
    #[inline]
    fn matches(&self, _col: &dyn Array, row_idx: usize) -> bool {
        match self {
            EventTypeCheck::StringView { arr, set } => {
                !arr.is_null(row_idx) && set.contains(arr.value(row_idx))
            }
            EventTypeCheck::Dict { keys, codes } => {
                if keys.is_null(row_idx) {
                    false
                } else {
                    codes.code_matches(keys.value(row_idx))
                }
            }
            EventTypeCheck::Unknown => false,
        }
    }
}

/// Build the per-batch event-type check strategy by examining the
/// encoding of `col`.
fn build_event_type_check<'a>(
    col: &'a dyn Array,
    event_types: &'a HashSet<String>,
) -> EventTypeCheck<'a> {
    // Try Dict<Int32, Utf8View> first — the common encoded form.
    if let Some(dict) = col
        .as_any()
        .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
    {
        if let Some(values) = dict.values().as_any().downcast_ref::<StringViewArray>() {
            let codes = EventTypeCodeSet::from_dict(values, event_types);
            return EventTypeCheck::Dict {
                keys: dict.keys(),
                codes,
            };
        }
    }
    // Fall back to StringViewArray.
    if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
        return EventTypeCheck::StringView {
            arr,
            set: event_types,
        };
    }
    EventTypeCheck::Unknown
}

// ─────────────────────────────────────────────────────────────────────────────
// Scalar extraction helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Extract a [`ScalarValue`] from `col` at `row_idx`.
///
/// Handles the common encoded forms:
/// - `Int64Array` → `ScalarValue::Int`
/// - `Timestamp(Nanosecond, _)` → `ScalarValue::Timestamp`
/// - `StringViewArray` → `ScalarValue::String`
/// - `DictionaryArray<Int32, Utf8View>` → `ScalarValue::String`
/// - `Float64Array` → `ScalarValue::Float`
/// - `BooleanArray` → `ScalarValue::Bool`
///
/// Returns `ScalarValue::Null` for null rows and unrecognised types.
/// `expected_bql_type` guides disambiguation when the Arrow type alone
/// is ambiguous (e.g. `Int64` used for both Int and Timestamp columns).
fn extract_scalar(
    col: &dyn Array,
    row_idx: usize,
    expected_bql_type: &bqlite_core::BqlType,
) -> ScalarValue {
    if col.is_null(row_idx) {
        return ScalarValue::Null;
    }
    match col.data_type() {
        DataType::Int64 => {
            let v = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column must downcast to Int64Array")
                .value(row_idx);
            match expected_bql_type {
                bqlite_core::BqlType::Timestamp => ScalarValue::Timestamp(v),
                _ => ScalarValue::Int(v),
            }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let v = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("Timestamp column must downcast to TimestampNanosecondArray")
                .value(row_idx);
            ScalarValue::Timestamp(v)
        }
        DataType::Utf8View => {
            let s = col
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Utf8View column must downcast to StringViewArray")
                .value(row_idx)
                .to_string();
            ScalarValue::String(s)
        }
        DataType::Float64 => {
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 column must downcast to Float64Array")
                .value(row_idx);
            ScalarValue::Float(v)
        }
        DataType::Boolean => {
            let b = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean column must downcast to BooleanArray")
                .value(row_idx);
            ScalarValue::Bool(b)
        }
        DataType::Dictionary(key_type, _) if matches!(key_type.as_ref(), DataType::Int32) => {
            // Dictionary<Int32, Utf8View> — resolve through the dictionary.
            let dict = col
                .as_any()
                .downcast_ref::<arrow::array::DictionaryArray<Int32Type>>()
                .expect("Dict<Int32, _> column must downcast");
            let key = dict.keys().value(row_idx);
            let values = dict.values();
            if let Some(sv) = values.as_any().downcast_ref::<StringViewArray>() {
                if sv.is_null(key as usize) {
                    ScalarValue::Null
                } else {
                    ScalarValue::String(sv.value(key as usize).to_string())
                }
            } else {
                ScalarValue::Null
            }
        }
        _ => ScalarValue::Null,
    }
}

/// Build a single-row Arrow array from a [`ScalarValue`] matching `data_type`.
fn scalar_to_single_row_array(value: &ScalarValue, data_type: &DataType) -> ArrayRef {
    match value {
        ScalarValue::Null => new_null_array(data_type, 1),
        ScalarValue::Bool(b) => Arc::new(BooleanArray::from(vec![*b])) as ArrayRef,
        ScalarValue::Int(n) => match data_type {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let arr = TimestampNanosecondArray::from(vec![*n]);
                match tz {
                    Some(tz_str) => Arc::new(arr.with_timezone(tz_str.as_ref())) as ArrayRef,
                    None => Arc::new(arr) as ArrayRef,
                }
            }
            _ => Arc::new(Int64Array::from(vec![*n])) as ArrayRef,
        },
        ScalarValue::Float(f) => Arc::new(Float64Array::from(vec![*f])) as ArrayRef,
        ScalarValue::String(s) => {
            let mut builder = StringViewBuilder::new();
            builder.append_value(s);
            Arc::new(builder.finish()) as ArrayRef
        }
        ScalarValue::Timestamp(ns) => match data_type {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let arr = TimestampNanosecondArray::from(vec![*ns]);
                match tz {
                    Some(tz_str) => Arc::new(arr.with_timezone(tz_str.as_ref())) as ArrayRef,
                    None => Arc::new(arr) as ArrayRef,
                }
            }
            _ => Arc::new(Int64Array::from(vec![*ns])) as ArrayRef,
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate column collection
// ─────────────────────────────────────────────────────────────────────────────

/// Walk a [`CompiledExpr`] tree and collect column names it references
/// into `names`, skipping duplicates.
fn collect_predicate_columns(expr: &CompiledExpr, names: &mut Vec<String>) {
    use bqlite_planner::compiled::CompiledNode;
    match &expr.node {
        CompiledNode::Column { name, .. } if !names.contains(name) => {
            names.push(name.clone());
        }
        CompiledNode::Column { .. } => {}
        CompiledNode::Literal(_) => {}
        CompiledNode::Arith { left, right, .. } => {
            collect_predicate_columns(left, names);
            collect_predicate_columns(right, names);
        }
        CompiledNode::Compare { left, right, .. } => {
            collect_predicate_columns(left, names);
            collect_predicate_columns(right, names);
        }
        CompiledNode::And { operands, .. } => {
            for op in operands {
                collect_predicate_columns(op, names);
            }
        }
        CompiledNode::Or { operands, .. } => {
            for op in operands {
                collect_predicate_columns(op, names);
            }
        }
        CompiledNode::Not(inner) => {
            collect_predicate_columns(inner, names);
        }
        CompiledNode::IsNull { input, .. } => {
            collect_predicate_columns(input, names);
        }
        CompiledNode::Unary { operand, .. } => {
            collect_predicate_columns(operand, names);
        }
        CompiledNode::Cast { input, .. } | CompiledNode::ImplicitCoerce { input, .. } => {
            collect_predicate_columns(input, names);
        }
        CompiledNode::InLiteralSet { input, .. } => {
            collect_predicate_columns(input, names);
        }
        CompiledNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_predicate_columns(arg, names);
            }
        }
        // Non-exhaustive: future variants are ignored for column collection.
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, Int64Array, StringViewBuilder,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema, PropertyValue, SEQ_ID_COLUMN};
    use bqlite_planner::compiled::{
        ArrowKernelId, CompareKernel, CompareOp, CompiledExpr, CompiledNode, FunctionId,
        FunctionKernel,
    };
    use bqlite_planner::expr::ScalarFunctionSig;
    use bqlite_planner::logical::EventSelectKind;
    use bqlite_planner::physical::EventSelectPhysical;

    use super::*;

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// Build a standard input OperatorSchema for tests.
    fn input_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required(SEQ_ID_COLUMN, BqlType::Int),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::required("amount", BqlType::Int),
        ])
        .unwrap()
    }

    /// Output schema for tests (entity_id + ts + event_type + amount).
    fn output_schema_with_amount() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::required("amount", BqlType::Int),
        ])
        .unwrap()
    }

    /// Output schema for tests without amount (minimal).
    fn output_schema_minimal() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    /// Build a fake `EventSelectPhysical` for test operators.
    fn make_physical(
        kind: EventSelectKind,
        event_types: Vec<&str>,
        output_schema: OperatorSchema,
    ) -> EventSelectPhysical {
        use bqlite_planner::physical::PhysicalPlan;
        // Use a minimal scan plan as the child.
        let scan_schema = input_schema();
        EventSelectPhysical {
            kind,
            event_types: event_types.iter().map(|s| s.to_string()).collect(),
            predicate: None,
            lookback: None,
            forwarded_columns: vec![],
            fused_aggregate: None,
            input: Box::new(PhysicalPlan::Scan(make_scan_physical(scan_schema))),
            output_schema,
            pre_fusion_output_schema: None,
        }
    }

    fn make_scan_physical(schema: OperatorSchema) -> bqlite_planner::physical::ScanPhysical {
        bqlite_planner::physical::ScanPhysical {
            table: "events".to_string(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: schema,
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        }
    }

    /// Build a RecordBatch from typed columns.
    ///
    /// `rows` is a `Vec<(entity_id, ts, seq_id, event_type, amount)>`.
    fn make_batch(rows: &[(&str, i64, i64, &str, i64)]) -> RecordBatch {
        let n = rows.len();

        let mut entity_id_b = StringViewBuilder::with_capacity(n);
        let mut ts_b = TimestampNanosecondArray::builder(n);
        let mut seq_id_b = Int64Array::builder(n);
        let mut event_type_b = StringViewBuilder::with_capacity(n);
        let mut amount_b = Int64Array::builder(n);

        for (eid, ts, seq, et, amt) in rows {
            entity_id_b.append_value(eid);
            ts_b.append_value(*ts);
            seq_id_b.append_value(*seq);
            event_type_b.append_value(et);
            amount_b.append_value(*amt);
        }

        let entity_id_arr: ArrayRef = Arc::new(entity_id_b.finish());
        let ts_arr: ArrayRef = Arc::new(ts_b.finish().with_timezone("UTC"));
        let seq_id_arr: ArrayRef = Arc::new(seq_id_b.finish());
        let event_type_arr: ArrayRef = Arc::new(event_type_b.finish());
        let amount_arr: ArrayRef = Arc::new(amount_b.finish());

        let schema = ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("amount", DataType::Int64, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                entity_id_arr,
                ts_arr,
                seq_id_arr,
                event_type_arr,
                amount_arr,
            ],
        )
        .unwrap()
    }

    /// Build a RecordBatch with dictionary-encoded event_type.
    fn make_batch_dict_event_type(rows: &[(&str, i64, i64, &str, i64)]) -> RecordBatch {
        let n = rows.len();

        // Collect unique event types for the dictionary.
        let mut dict_values: Vec<&str> = rows.iter().map(|(_, _, _, et, _)| *et).collect();
        dict_values.sort_unstable();
        dict_values.dedup();

        // Map event_type strings to dictionary indices.
        let keys: Vec<i32> = rows
            .iter()
            .map(|(_, _, _, et, _)| dict_values.iter().position(|&v| v == *et).unwrap() as i32)
            .collect();

        let mut dict_arr_b = StringViewBuilder::with_capacity(dict_values.len());
        for v in &dict_values {
            dict_arr_b.append_value(v);
        }
        let dict_arr = dict_arr_b.finish();

        let keys_arr = Int32Array::from(keys);
        let dict_encoded: ArrayRef =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys_arr, Arc::new(dict_arr)).unwrap());

        let mut entity_id_b = StringViewBuilder::with_capacity(n);
        let mut ts_b = TimestampNanosecondArray::builder(n);
        let mut seq_id_b = Int64Array::builder(n);
        let mut amount_b = Int64Array::builder(n);

        for (eid, ts, seq, _, amt) in rows {
            entity_id_b.append_value(eid);
            ts_b.append_value(*ts);
            seq_id_b.append_value(*seq);
            amount_b.append_value(*amt);
        }

        let entity_id_arr: ArrayRef = Arc::new(entity_id_b.finish());
        let ts_arr: ArrayRef = Arc::new(ts_b.finish().with_timezone("UTC"));
        let seq_id_arr: ArrayRef = Arc::new(seq_id_b.finish());
        let amount_arr: ArrayRef = Arc::new(amount_b.finish());

        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8View));
        let schema = ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
            Field::new("event_type", dict_type, false),
            Field::new("amount", DataType::Int64, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![entity_id_arr, ts_arr, seq_id_arr, dict_encoded, amount_arr],
        )
        .unwrap()
    }

    /// Run an operator over a sequence of sub-batches and call finish_entity.
    fn run_entity(op: &EventSelectOperator, sub_batches: &[RecordBatch]) -> Option<RecordBatch> {
        let mut state = op.create_state(&EntityId::from("u1"));
        for batch in sub_batches {
            op.process_sub_batch(&mut state, batch);
        }
        op.finish_entity(state)
    }

    // ── Basic FIRST tests ────────────────────────────────────────────────────

    #[test]
    fn first_selects_earliest_qualifying_event() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        // Three purchase events; FIRST should return ts=100.
        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10),
            ("u1", 200, 2, "purchase", 20),
            ("u1", 300, 3, "purchase", 30),
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        assert_eq!(result.num_rows(), 1);

        // Check ts is the earliest.
        let ts_col = result
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 100);

        // Check amount.
        let amt_col = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt_col.value(0), 10);
    }

    #[test]
    fn first_skips_non_matching_event_types() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "login", 0),
            ("u1", 200, 2, "purchase", 0),
            ("u1", 300, 3, "purchase", 0),
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let ts_col = result
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 200); // First purchase, not the login.
    }

    #[test]
    fn first_no_qualifying_event_produces_no_row() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[("u1", 100, 1, "login", 0), ("u1", 200, 2, "logout", 0)]);
        assert!(run_entity(&op, &[batch]).is_none());
    }

    #[test]
    fn first_empty_entity_produces_no_row() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        // No sub-batches — empty entity.
        assert!(run_entity(&op, &[]).is_none());
    }

    #[test]
    fn first_single_matching_event() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        let batch = make_batch(&[("u1", 42, 1, "purchase", 999)]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        assert_eq!(result.num_rows(), 1);
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 999);
    }

    // ── Basic LAST tests ─────────────────────────────────────────────────────

    #[test]
    fn last_selects_latest_qualifying_event() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10),
            ("u1", 200, 2, "purchase", 20),
            ("u1", 300, 3, "purchase", 30),
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let ts_col = result
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 300); // Last purchase.
    }

    #[test]
    fn last_no_qualifying_event_produces_no_row() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        let batch = make_batch(&[("u1", 100, 1, "login", 0)]);
        assert!(run_entity(&op, &[batch]).is_none());
    }

    #[test]
    fn last_interspersed_with_other_types() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "login", 0),
            ("u1", 200, 2, "purchase", 5),
            ("u1", 300, 3, "logout", 0),
            ("u1", 400, 4, "purchase", 99),
            ("u1", 500, 5, "login", 0),
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 99); // Latest purchase.
    }

    // ── Basic NTH tests ──────────────────────────────────────────────────────

    #[test]
    fn nth_1_is_equivalent_to_first() {
        let events = vec![
            ("u1", 100, 1, "purchase", 10),
            ("u1", 200, 2, "purchase", 20),
            ("u1", 300, 3, "purchase", 30),
        ];

        let desc_first = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let desc_nth1 = make_physical(
            EventSelectKind::Nth(1),
            vec!["purchase"],
            output_schema_with_amount(),
        );

        let op_first = EventSelectOperator::new(&desc_first, &input_schema());
        let op_nth1 = EventSelectOperator::new(&desc_nth1, &input_schema());

        let batch = make_batch(&events);
        let r_first = run_entity(&op_first, std::slice::from_ref(&batch)).unwrap();
        let r_nth1 = run_entity(&op_nth1, std::slice::from_ref(&batch)).unwrap();

        let ts_first = r_first
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        let ts_nth1 = r_nth1
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        assert_eq!(ts_first, ts_nth1);
    }

    #[test]
    fn nth_3_with_exactly_3_qualifying_events() {
        let desc = make_physical(
            EventSelectKind::Nth(3),
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10),
            ("u1", 200, 2, "login", 0),
            ("u1", 300, 3, "purchase", 20),
            ("u1", 400, 4, "purchase", 99),
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 99); // 3rd purchase.
    }

    #[test]
    fn nth_3_with_only_2_qualifying_events_produces_no_row() {
        let desc = make_physical(
            EventSelectKind::Nth(3),
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 0),
            ("u1", 200, 2, "purchase", 0),
            ("u1", 300, 3, "login", 0),
        ]);
        assert!(run_entity(&op, &[batch]).is_none());
    }

    #[test]
    fn nth_produces_no_row_when_no_qualifying_events() {
        let desc = make_physical(
            EventSelectKind::Nth(2),
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        let batch = make_batch(&[("u1", 100, 1, "login", 0)]);
        assert!(run_entity(&op, &[batch]).is_none());
    }

    // ── Same-ts tie-breaking (§5.4) ──────────────────────────────────────────

    #[test]
    fn first_same_ts_picks_smallest_seq_id() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        // Two purchases at the same ts; seq_id 1 < 2, so FIRST picks seq_id=1.
        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10),
            ("u1", 100, 2, "purchase", 20),
        ]);
        let result = run_entity(&op, &[batch]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 10); // seq_id=1 → amount=10.
    }

    #[test]
    fn last_same_ts_picks_largest_seq_id() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10),
            ("u1", 100, 2, "purchase", 20),
        ]);
        let result = run_entity(&op, &[batch]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 20); // seq_id=2 → amount=20.
    }

    // ── Multi-type event list (§6) ───────────────────────────────────────────

    #[test]
    fn first_from_event_type_list() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["login", "sso_login"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 0),
            ("u1", 200, 2, "sso_login", 0),
            ("u1", 300, 3, "login", 0),
        ]);
        let result = run_entity(&op, &[batch]).expect("row");
        let ts_col = result
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 200); // sso_login at ts=200 is earlier than login at ts=300.
    }

    // ── Multi-batch (sub-batch continuation) (§9.3) ──────────────────────────

    #[test]
    fn last_across_multiple_sub_batches() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch1 = make_batch(&[("u1", 100, 1, "purchase", 10), ("u1", 200, 2, "login", 0)]);
        let batch2 = make_batch(&[("u1", 300, 3, "purchase", 99), ("u1", 400, 4, "logout", 0)]);
        let result = run_entity(&op, &[batch1, batch2]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 99);
    }

    #[test]
    fn first_early_termination_across_sub_batches() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        // Qualifying event is in the first sub-batch; second should be skipped.
        let batch1 = make_batch(&[("u1", 100, 1, "purchase", 42)]);
        let batch2 = make_batch(&[("u1", 200, 2, "purchase", 999)]);

        let result = run_entity(&op, &[batch1, batch2]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // Must be the FIRST batch's event, not the second.
        assert_eq!(amt.value(0), 42);
    }

    // ── Dictionary-encoded event_type (§9.2) ─────────────────────────────────

    #[test]
    fn first_with_dictionary_encoded_event_type() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch_dict_event_type(&[
            ("u1", 100, 1, "login", 0),
            ("u1", 200, 2, "purchase", 55),
            ("u1", 300, 3, "purchase", 66),
        ]);
        let result = run_entity(&op, &[batch]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 55);
    }

    #[test]
    fn last_with_dictionary_encoded_event_type() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch_dict_event_type(&[
            ("u1", 100, 1, "purchase", 11),
            ("u1", 200, 2, "logout", 0),
            ("u1", 300, 3, "purchase", 77),
        ]);
        let result = run_entity(&op, &[batch]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 77);
    }

    // ── Output invariants ─────────────────────────────────────────────────────

    #[test]
    fn output_conforms_to_output_schema() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[("u1", 100, 1, "purchase", 42)]);
        let result = run_entity(&op, &[batch]).expect("row");

        // Schema fields must match operator's output_schema column count.
        assert_eq!(
            result.schema().fields().len(),
            op.output_schema().columns().len()
        );
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn entity_operator_required_columns_covers_user_columns() {
        // `__seq_id` is no longer in `required_columns()`: FIRST/LAST/NTH
        // rely on the scan's `(entity_id, ts, __seq_id)` emission order
        // for tie-breaking rather than reading `__seq_id` values (see
        // the doc on `EventSelectInputMap`).
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        let req = op.required_columns();

        assert!(req.contains(&"entity_id".to_string()));
        assert!(req.contains(&"ts".to_string()));
        assert!(req.contains(&"event_type".to_string()));
        assert!(
            !req.contains(&SEQ_ID_COLUMN.to_string()),
            "__seq_id is not a required input column"
        );
    }

    #[test]
    fn supported_demands_reports_forwarded_columns() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        let caps = op.supported_demands();

        assert!(caps.supports_forwarded_columns);
        // TASK-520 turned on stateful-to-aggregate fusion for EventSelect.
        assert!(caps.supports_aggregation_fusion);
        assert!(!caps.supports_step_reached);
    }

    #[test]
    fn demand_caps_match_physical_constant() {
        let desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());
        // Runtime caps must match the planner-side constant.
        assert_eq!(
            op.supported_demands(),
            bqlite_planner::physical::EventSelectPhysical::DEMAND_CAPS
        );
    }

    // ── TASK-520 fusion smoke test ──────────────────────────────────────
    //
    // Construction with `pre_fusion_output_schema: Some(_)` differing from
    // `output_schema` simulates the post-fusion descriptor shape. The
    // operator must use the native (pre-fusion) schema for slot-mapping
    // and required-column resolution.

    #[test]
    fn fused_construction_uses_native_schema_for_internal_state() {
        use bqlite_planner::demand::CompiledFusableAggregate;

        let mut desc = make_physical(
            EventSelectKind::First,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let native = desc.output_schema.clone();
        // Replace external output_schema with an unrelated aggregate
        // schema while keeping the native schema available via
        // `pre_fusion_output_schema`.
        let agg_schema =
            OperatorSchema::new(vec![ColumnDef::nullable("total", BqlType::Int)]).unwrap();
        desc.pre_fusion_output_schema = Some(native.clone());
        desc.output_schema = agg_schema.clone();
        desc.fused_aggregate = Some(CompiledFusableAggregate {
            aggregates: vec![],
            group_by: vec![],
            output_schema: agg_schema,
            max_groups: crate::aggregate::DEFAULT_MAX_GROUPS,
        });

        let op = EventSelectOperator::new(&desc, &input_schema());
        assert_eq!(op.output_schema(), &native);
        assert!(op.is_fused());
        assert!(op.fused_aggregate().is_some());
    }

    // ── NTH ordering edge cases ──────────────────────────────────────────────

    #[test]
    fn nth_5_counts_correctly_across_sub_batches() {
        let desc = make_physical(
            EventSelectKind::Nth(5),
            vec!["click"],
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        // 3 qualifying events in batch1, 3 in batch2; 5th is at ts=500.
        let batch1 = make_batch(&[
            ("u1", 100, 1, "click", 1),
            ("u1", 200, 2, "click", 2),
            ("u1", 300, 3, "click", 3),
        ]);
        let batch2 = make_batch(&[
            ("u1", 400, 4, "click", 4),
            ("u1", 500, 5, "click", 5),
            ("u1", 600, 6, "click", 6),
        ]);

        let result = run_entity(&op, &[batch1, batch2]).expect("row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 5); // 5th click → amount=5.
    }

    // ── create_state / finish_entity smoke test ──────────────────────────────

    #[test]
    fn finish_entity_without_process_produces_no_row() {
        let desc = make_physical(
            EventSelectKind::Last,
            vec!["purchase"],
            output_schema_minimal(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        // Call finish_entity directly without any sub-batches.
        let state = op.create_state(&EntityId::from("u_empty"));
        assert!(op.finish_entity(state).is_none());
    }

    #[test]
    fn operator_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<EventSelectOperator>();
    }

    // ── WHERE predicate tests ────────────────────────────────────────────────

    /// Build a `CompiledExpr` for `amount > threshold` against the test
    /// input schema (amount at column index 4).
    fn make_predicate_amount_gt(threshold: i64) -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::Compare {
                op: CompareOp::Greater,
                left: Box::new(CompiledExpr {
                    node: CompiledNode::Column {
                        index: 4,
                        name: "amount".into(),
                    },
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                right: Box::new(CompiledExpr {
                    node: CompiledNode::Literal(PropertyValue::Int(threshold)),
                    result_type: BqlType::Int,
                    nullable: false,
                }),
                kernel: CompareKernel::ArrowKernel(ArrowKernelId::GtInt),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    /// Build a `CompiledExpr` whose evaluation always fails
    /// (`FunctionCall` nodes are not implemented in Wave 2 eval).
    fn make_predicate_eval_fails() -> CompiledExpr {
        CompiledExpr {
            node: CompiledNode::FunctionCall {
                signature: std::sync::Arc::new(ScalarFunctionSig {
                    name: "like".to_string(),
                    params: vec![BqlType::String, BqlType::String],
                    return_type: BqlType::Bool,
                    nullable: false,
                }),
                args: vec![],
                kernel: FunctionKernel::Registered(FunctionId(0)),
            },
            result_type: BqlType::Bool,
            nullable: false,
        }
    }

    fn make_physical_with_predicate(
        kind: EventSelectKind,
        event_types: Vec<&str>,
        predicate: CompiledExpr,
        output_schema: OperatorSchema,
    ) -> EventSelectPhysical {
        use bqlite_planner::physical::PhysicalPlan;
        let scan_schema = input_schema();
        EventSelectPhysical {
            kind,
            event_types: event_types.iter().map(|s| s.to_string()).collect(),
            predicate: Some(predicate),
            lookback: None,
            forwarded_columns: vec![],
            fused_aggregate: None,
            input: Box::new(PhysicalPlan::Scan(make_scan_physical(scan_schema))),
            output_schema,
            pre_fusion_output_schema: None,
        }
    }

    #[test]
    fn first_where_predicate_filters_qualifying_events() {
        // FIRST purchase WHERE amount > 50: rows with amount ≤ 50 must be skipped.
        let desc = make_physical_with_predicate(
            EventSelectKind::First,
            vec!["purchase"],
            make_predicate_amount_gt(50),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10), // amount 10 ≤ 50 → skipped
            ("u1", 200, 2, "purchase", 30), // amount 30 ≤ 50 → skipped
            ("u1", 300, 3, "purchase", 99), // amount 99 > 50 → qualifies (FIRST)
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 99);
    }

    #[test]
    fn first_where_predicate_no_qualifying_event_produces_no_row() {
        // All amounts are ≤ 50 — no row should qualify.
        let desc = make_physical_with_predicate(
            EventSelectKind::First,
            vec!["purchase"],
            make_predicate_amount_gt(50),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 5),
            ("u1", 200, 2, "purchase", 50),
        ]);
        assert!(run_entity(&op, &[batch]).is_none());
    }

    #[test]
    fn nth_where_counts_only_predicate_matching_events() {
        // NTH(2) purchase WHERE amount > 20: 1st qualifying=amt99, 2nd=amt77.
        let desc = make_physical_with_predicate(
            EventSelectKind::Nth(2),
            vec!["purchase"],
            make_predicate_amount_gt(20),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 5),  // ≤20 → skipped
            ("u1", 200, 2, "purchase", 99), // >20 → 1st qualifying
            ("u1", 300, 3, "purchase", 15), // ≤20 → skipped
            ("u1", 400, 4, "purchase", 77), // >20 → 2nd qualifying (NTH(2))
            ("u1", 500, 5, "purchase", 88), // >20 → would be 3rd, not reached
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 77);
    }

    #[test]
    fn predicate_eval_error_skips_batch_fail_safe() {
        // A FunctionCall predicate that eval cannot handle must cause the
        // entire batch to be skipped (fail-safe), not qualify all rows.
        let desc = make_physical_with_predicate(
            EventSelectKind::First,
            vec!["purchase"],
            make_predicate_eval_fails(),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 42),
            ("u1", 200, 2, "purchase", 99),
        ]);
        // Eval error → fail-safe → no qualifying event → None.
        assert!(run_entity(&op, &[batch]).is_none());
    }

    #[test]
    fn predicate_eval_error_across_multiple_batches_produces_no_row() {
        // A failing predicate must skip every sub-batch, not just the first.
        // An entity spanning multiple sub-batches must still produce None.
        let desc = make_physical_with_predicate(
            EventSelectKind::First,
            vec!["purchase"],
            make_predicate_eval_fails(),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch1 = make_batch(&[("u1", 100, 1, "purchase", 42)]);
        let batch2 = make_batch(&[("u1", 200, 2, "purchase", 99)]);
        let batch3 = make_batch(&[("u1", 300, 3, "purchase", 7)]);
        // All three batches are skipped by the fail-safe → None.
        assert!(run_entity(&op, &[batch1, batch2, batch3]).is_none());
    }

    /// Build a batch with nullable amounts (Some → value, None → NULL).
    fn make_batch_nullable_amount(rows: &[(&str, i64, i64, &str, Option<i64>)]) -> RecordBatch {
        let n = rows.len();

        let mut entity_id_b = StringViewBuilder::with_capacity(n);
        let mut ts_b = TimestampNanosecondArray::builder(n);
        let mut seq_id_b = Int64Array::builder(n);
        let mut event_type_b = StringViewBuilder::with_capacity(n);
        let mut amount_b = Int64Array::builder(n);

        for (eid, ts, seq, et, amt) in rows {
            entity_id_b.append_value(eid);
            ts_b.append_value(*ts);
            seq_id_b.append_value(*seq);
            event_type_b.append_value(et);
            match amt {
                Some(v) => amount_b.append_value(*v),
                None => amount_b.append_null(),
            }
        }

        let entity_id_arr: ArrayRef = Arc::new(entity_id_b.finish());
        let ts_arr: ArrayRef = Arc::new(ts_b.finish().with_timezone("UTC"));
        let seq_id_arr: ArrayRef = Arc::new(seq_id_b.finish());
        let event_type_arr: ArrayRef = Arc::new(event_type_b.finish());
        let amount_arr: ArrayRef = Arc::new(amount_b.finish());

        let schema = ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("amount", DataType::Int64, true), // nullable
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                entity_id_arr,
                ts_arr,
                seq_id_arr,
                event_type_arr,
                amount_arr,
            ],
        )
        .unwrap()
    }

    #[test]
    fn predicate_null_mask_treated_as_non_qualifying() {
        // Rows where the predicate produces NULL (e.g. amount IS NULL → compare
        // with NULL propagates NULL) must be treated as non-qualifying.
        // Row 1: amount=NULL → amount > 50 = NULL → skip
        // Row 2: amount=30  → amount > 50 = false → skip
        // Row 3: amount=99  → amount > 50 = true  → qualifies (FIRST)
        let desc = make_physical_with_predicate(
            EventSelectKind::First,
            vec!["purchase"],
            make_predicate_amount_gt(50),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch_nullable_amount(&[
            ("u1", 100, 1, "purchase", None), // NULL amount → NULL predicate → skip
            ("u1", 200, 2, "purchase", Some(30)), // 30 ≤ 50 → skip
            ("u1", 300, 3, "purchase", Some(99)), // 99 > 50 → qualifies (FIRST)
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let ts = result
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0);
        assert_eq!(ts, 300); // Correct FIRST qualifying row.
    }

    #[test]
    fn last_where_predicate_selects_latest_qualifying() {
        // LAST purchase WHERE amount > 50 should pick the last qualifying row,
        // not the last purchase overall.
        let desc = make_physical_with_predicate(
            EventSelectKind::Last,
            vec!["purchase"],
            make_predicate_amount_gt(50),
            output_schema_with_amount(),
        );
        let op = EventSelectOperator::new(&desc, &input_schema());

        let batch = make_batch(&[
            ("u1", 100, 1, "purchase", 10), // ≤50 → not qualifying
            ("u1", 200, 2, "purchase", 99), // >50 → qualifying (2nd)
            ("u1", 300, 3, "purchase", 77), // >50 → qualifying (3rd = LAST)
            ("u1", 400, 4, "purchase", 5),  // ≤50 → not qualifying (would be overall LAST)
        ]);
        let result = run_entity(&op, &[batch]).expect("should produce a row");
        let amt = result
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amt.value(0), 77); // LAST qualifying, not overall last.
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Property tests (TASK-531, event-select-sample.md §22.1)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod proptests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringViewBuilder, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use proptest::prelude::*;

    use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema, SEQ_ID_COLUMN};
    use bqlite_planner::logical::EventSelectKind;
    use bqlite_planner::physical::{EventSelectPhysical, PhysicalPlan, ScanPhysical};

    use super::EventSelectOperator;
    use crate::operator::EntityOperator;

    // ── Schemas ──────────────────────────────────────────────────────────────

    fn input_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required(SEQ_ID_COLUMN, BqlType::Int),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    fn output_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .unwrap()
    }

    fn make_op(kind: EventSelectKind) -> EventSelectOperator {
        let scan = ScanPhysical {
            table: "events".to_string(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: input_schema(),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        };
        let desc = EventSelectPhysical {
            kind,
            event_types: vec!["purchase".to_string()],
            predicate: None,
            lookback: None,
            forwarded_columns: vec![],
            fused_aggregate: None,
            input: Box::new(PhysicalPlan::Scan(scan)),
            output_schema: output_schema(),
            pre_fusion_output_schema: None,
        };
        EventSelectOperator::new(&desc, &input_schema())
    }

    // ── Strategies ───────────────────────────────────────────────────────────

    /// Generate a sequence of (is_qualifying, ts_delta) pairs.
    ///
    /// ts_delta ∈ 1..=100 so timestamps are strictly increasing (no ties).
    /// is_qualifying = true → event_type "purchase" (the target type).
    fn arb_events() -> impl Strategy<Value = Vec<(bool, i64)>> {
        prop::collection::vec((any::<bool>(), 1i64..=100), 1..=32)
    }

    // ── Batch / output helpers ────────────────────────────────────────────────

    fn build_batch(events: &[(bool, i64)]) -> RecordBatch {
        let n = events.len();
        let mut ts_vals: Vec<i64> = Vec::with_capacity(n);
        let mut seq_vals: Vec<i64> = Vec::with_capacity(n);
        let mut eid_b = StringViewBuilder::with_capacity(n);
        let mut et_b = StringViewBuilder::with_capacity(n);

        let mut ts: i64 = 0;
        for (i, (is_q, delta)) in events.iter().enumerate() {
            ts += delta;
            ts_vals.push(ts);
            seq_vals.push(i as i64);
            eid_b.append_value("e1");
            et_b.append_value(if *is_q { "purchase" } else { "login" });
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
            Field::new("event_type", DataType::Utf8View, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(eid_b.finish()) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(ts_vals).with_timezone("UTC")) as ArrayRef,
                Arc::new(Int64Array::from(seq_vals)) as ArrayRef,
                Arc::new(et_b.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn build_batch_for_entity(entity_id: &str, events: &[(bool, i64)]) -> RecordBatch {
        let n = events.len();
        let mut ts_vals: Vec<i64> = Vec::with_capacity(n);
        let mut seq_vals: Vec<i64> = Vec::with_capacity(n);
        let mut eid_b = StringViewBuilder::with_capacity(n);
        let mut et_b = StringViewBuilder::with_capacity(n);

        let mut ts: i64 = 0;
        for (i, (is_q, delta)) in events.iter().enumerate() {
            ts += delta;
            ts_vals.push(ts);
            seq_vals.push(i as i64);
            eid_b.append_value(entity_id);
            et_b.append_value(if *is_q { "purchase" } else { "login" });
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new(SEQ_ID_COLUMN, DataType::Int64, false),
            Field::new("event_type", DataType::Utf8View, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(eid_b.finish()) as ArrayRef,
                Arc::new(TimestampNanosecondArray::from(ts_vals).with_timezone("UTC")) as ArrayRef,
                Arc::new(Int64Array::from(seq_vals)) as ArrayRef,
                Arc::new(et_b.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn run_op(op: &EventSelectOperator, batch: &RecordBatch) -> Option<RecordBatch> {
        run_op_as(op, batch, "e1")
    }

    fn run_op_as(
        op: &EventSelectOperator,
        batch: &RecordBatch,
        entity_id: &str,
    ) -> Option<RecordBatch> {
        let mut state = op.create_state(&EntityId::from(entity_id));
        op.process_sub_batch(&mut state, batch);
        op.finish_entity(state)
    }

    fn output_ts(out: &RecordBatch) -> i64 {
        out.column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(0)
    }

    /// Collect timestamps of qualifying events in arrival order.
    fn qualifying_ts(events: &[(bool, i64)]) -> Vec<i64> {
        let mut ts: i64 = 0;
        events
            .iter()
            .filter_map(|(is_q, delta)| {
                ts += delta;
                if *is_q {
                    Some(ts)
                } else {
                    None
                }
            })
            .collect()
    }

    // ── Properties ───────────────────────────────────────────────────────────

    proptest! {
        #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

        /// §22.1 (1) — output cardinality is exactly 0 or 1 rows per entity,
        /// for all three selection modes.
        #[test]
        fn output_cardinality_zero_or_one(events in arb_events()) {
            let batch = build_batch(&events);
            for kind in [EventSelectKind::First, EventSelectKind::Last, EventSelectKind::Nth(3)] {
                let op = make_op(kind);
                match run_op(&op, &batch) {
                    None => {}
                    Some(out) => prop_assert_eq!(out.num_rows(), 1),
                }
            }
        }

        /// §22.1 (2) — FIRST emits the row with minimum ts among qualifying events.
        ///
        /// Since ts is strictly increasing in the generated fixture, minimum ts
        /// equals the first qualifying event in arrival order.
        #[test]
        fn first_emits_min_ts(events in arb_events()) {
            let q = qualifying_ts(&events);
            let op = make_op(EventSelectKind::First);
            let out = run_op(&op, &build_batch(&events));
            if q.is_empty() {
                prop_assert!(out.is_none());
            } else {
                prop_assert_eq!(output_ts(&out.unwrap()), q[0]);
            }
        }

        /// §22.1 (3) — LAST emits the row with maximum ts among qualifying events.
        #[test]
        fn last_emits_max_ts(events in arb_events()) {
            let q = qualifying_ts(&events);
            let op = make_op(EventSelectKind::Last);
            let out = run_op(&op, &build_batch(&events));
            if q.is_empty() {
                prop_assert!(out.is_none());
            } else {
                prop_assert_eq!(output_ts(&out.unwrap()), *q.last().unwrap());
            }
        }

        /// §22.1 (4) and (5) — NTH(n): emits row at position n; omits when
        /// fewer than n qualifying events exist.
        #[test]
        fn nth_correct_position_and_omission(events in arb_events(), n in 1u32..=5) {
            let q = qualifying_ts(&events);
            let op = make_op(EventSelectKind::Nth(n));
            let out = run_op(&op, &build_batch(&events));
            if (q.len() as u32) < n {
                // (5) omission
                prop_assert!(out.is_none());
            } else {
                // (4) exactly n−1 qualifying events have smaller ts
                let emitted = output_ts(&out.unwrap());
                prop_assert_eq!(emitted, q[(n - 1) as usize]);
                let smaller = q.iter().filter(|&&t| t < emitted).count() as u32;
                prop_assert_eq!(smaller, n - 1);
            }
        }

        /// §22.1 (6) — entity isolation: running entity A does not affect
        /// the result for entity B (operator state is share-nothing).
        #[test]
        fn entity_isolation(events_a in arb_events(), events_b in arb_events()) {
            let op = make_op(EventSelectKind::First);
            let batch_a = build_batch_for_entity("ea", &events_a);
            let batch_b = build_batch_for_entity("eb", &events_b);

            // Baseline: entity B alone.
            let ts_b_alone = run_op_as(&op, &batch_b, "eb").map(|o| output_ts(&o));

            // Run entity A through its own state, then run entity B again.
            let mut state_a = op.create_state(&EntityId::from("ea"));
            op.process_sub_batch(&mut state_a, &batch_a);
            let _ = op.finish_entity(state_a);

            let ts_b_after = run_op_as(&op, &batch_b, "eb").map(|o| output_ts(&o));
            prop_assert_eq!(ts_b_alone, ts_b_after);
        }

        /// §22.1 (7) — NTH(1) ≡ FIRST for any event stream.
        #[test]
        fn nth_1_equiv_first(events in arb_events()) {
            let batch = build_batch(&events);
            let ts_first = run_op(&make_op(EventSelectKind::First), &batch).map(|o| output_ts(&o));
            let ts_nth1 = run_op(&make_op(EventSelectKind::Nth(1)), &batch).map(|o| output_ts(&o));
            prop_assert_eq!(ts_first, ts_nth1);
        }
    }
}
