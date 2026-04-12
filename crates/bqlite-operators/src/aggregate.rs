//! Aggregate accumulator framework.
//!
//! This module defines the `Accumulator` trait and its default
//! implementation `HashAccumulator`. These types power both the
//! standalone `AggregatePhysical` operator (TASK-307) and the
//! fused-downstream protocol that lets stateful operators
//! (`SequenceMatch`, `Sessionize`, `Attribute`) feed entities directly
//! into an accumulator without materializing intermediate rows.
//!
//! ## Trait hierarchy
//!
//! ```text
//!                   ┌──────────────┐
//!                   │  Accumulator │   (trait, dyn-safe)
//!                   └──────┬───────┘
//!                          │
//!               ┌──────────┴──────────┐
//!               │   HashAccumulator   │   (concrete default)
//!               └─────────────────────┘
//! ```
//!
//! `Accumulator` is object-safe so fused operators can hold
//! `Box<dyn Accumulator>` without knowing the concrete type.
//! `HashAccumulator` is the only built-in implementor; TASK-327 will
//! add DDSketch-based percentile accumulators by extending `AggState`
//! with a `Percentile` variant — the `Accumulator` trait itself does
//! not change.
//!
//! ## Crate placement
//!
//! `Accumulator`, `HashAccumulator`, `AggState`, `GroupKey`, and
//! `SumState` all live in `bqlite-operators` per the crate placement
//! table in execution-model.md §15. `AggFunction` and `ScalarValue`
//! live in `bqlite-core`.
//!
//! ## Memory accounting
//!
//! `HashAccumulator` tracks its own memory via `memory_usage()`.
//! The `max_groups` hard cap (default 1,000,000) is the sole overflow
//! defense in v1 — there is no spill-to-disk for aggregation state.
//! See execution-model.md §10.3–§10.4.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::mem;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringViewArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;

use bqlite_core::{AggFunction, BqlType, BqliteError, OperatorSchema, Result, ScalarValue};

// ──────────────────────────────────────────────────────────────────────────────
// GroupKey
// ──────────────────────────────────────────────────────────────────────────────

/// Compact group key for hash-based aggregation.
///
/// Wraps a `Vec<ScalarValue>` representing the tuple of GROUP BY column
/// values. The common case (1–3 group-by columns) fits in a single
/// cache line. For ungrouped aggregation, the key is an empty `Vec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupKey(pub Vec<ScalarValue>);

impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GroupKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl GroupKey {
    /// Creates an empty group key (for ungrouped aggregation).
    #[inline]
    pub fn empty() -> Self {
        GroupKey(Vec::new())
    }

    /// Creates a group key from a slice of scalar values.
    pub fn from_values(values: &[ScalarValue]) -> Self {
        GroupKey(values.to_vec())
    }

    /// Returns the number of columns in this group key.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` for ungrouped aggregation (empty key).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Estimated heap size in bytes (for memory accounting).
    pub fn heap_size(&self) -> usize {
        self.0.iter().map(scalar_heap_size).sum::<usize>()
            + self.0.capacity() * mem::size_of::<ScalarValue>()
    }
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Display for GroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, v) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{v}")?;
        }
        write!(f, ")")
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// SumState
// ──────────────────────────────────────────────────────────────────────────────

/// Track SUM without cross-type promotion.
///
/// `SUM(Int)` stays `Int`; `SUM(Float)` stays `Float`. Overflow on the
/// integer path wraps (Rust's default i64 wrapping addition); the float
/// path uses IEEE 754 addition. Both follow the principle of least
/// surprise for analytics workloads — explicit overflow detection is a
/// Wave 5 concern.
#[derive(Debug, Clone, PartialEq)]
pub enum SumState {
    /// SUM over Int columns — accumulates as i64.
    Int(i64),
    /// SUM over Float columns — accumulates as f64.
    Float(f64),
}

impl SumState {
    /// Merge another `SumState` into this one.
    ///
    /// Panics if the variants differ (a type error that should have
    /// been caught at plan time).
    pub fn merge(&mut self, other: &SumState) {
        match (self, other) {
            (SumState::Int(a), SumState::Int(b)) => *a = a.wrapping_add(*b),
            (SumState::Float(a), SumState::Float(b)) => *a += *b,
            _ => panic!("SumState::merge variant mismatch"),
        }
    }

    /// Convert to a `ScalarValue` for output.
    pub fn to_scalar(&self) -> ScalarValue {
        match self {
            SumState::Int(n) => ScalarValue::Int(*n),
            SumState::Float(f) => ScalarValue::Float(*f),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// AggState
// ──────────────────────────────────────────────────────────────────────────────

/// Per-function accumulator state.
///
/// One `AggState` instance exists per (group, aggregate function) pair
/// inside a `HashAccumulator`. Each variant is incrementally updatable
/// and supports pairwise merging for cross-shard reduction.
///
/// The `Percentile` variant (DDSketch) is stubbed here and implemented
/// by TASK-327. The `Variance` variant uses Welford's online algorithm
/// with a parallel merge formula — it is defined here for completeness
/// but not yet exposed as a BQL surface function.
#[derive(Debug, Clone)]
pub enum AggState {
    /// `COUNT(*)` — counts all rows.
    Count(u64),
    /// `COUNT(col)` — counts non-NULL values.
    CountNonNull(u64),
    /// `SUM(col)` — tracks Int or Float without cross-type promotion.
    Sum(SumState),
    /// `MIN(col)` — minimum non-NULL value seen.
    Min(Option<ScalarValue>),
    /// `MAX(col)` — maximum non-NULL value seen.
    Max(Option<ScalarValue>),
    /// `AVG(col)` — algebraic aggregate via `(sum, count)`.
    Avg { sum: f64, count: u64 },
    /// `COUNT_DISTINCT(col)` — exact distinct count via hash set.
    CountDistinct(HashSet<ScalarValue>),
    /// `VARIANCE(col)` — Welford's online algorithm.
    /// Not yet exposed as a BQL function; defined for completeness.
    Variance { count: u64, mean: f64, m2: f64 },
}

impl AggState {
    /// Create a new `AggState` for the given function and input type.
    ///
    /// `input_type` is the resolved BQL type of the aggregate argument
    /// column (None for `COUNT(*)`).
    pub fn new(function: AggFunction, input_type: Option<&BqlType>) -> Self {
        match function {
            AggFunction::Count => AggState::Count(0),
            AggFunction::CountColumn => AggState::CountNonNull(0),
            AggFunction::CountDistinct => AggState::CountDistinct(HashSet::new()),
            AggFunction::Sum => match input_type {
                Some(BqlType::Int) => AggState::Sum(SumState::Int(0)),
                Some(BqlType::Float) => AggState::Sum(SumState::Float(0.0)),
                _ => AggState::Sum(SumState::Int(0)),
            },
            AggFunction::Min => AggState::Min(None),
            AggFunction::Max => AggState::Max(None),
            AggFunction::Avg => AggState::Avg { sum: 0.0, count: 0 },
            AggFunction::P50 | AggFunction::P90 | AggFunction::P95 | AggFunction::P99 => {
                // DDSketch placeholder — TASK-327 replaces this.
                // For now, fall back to tracking sum/count (wrong but
                // compiles). The real percentile variant will be a
                // separate `Percentile(DDSketch)` added by TASK-327.
                AggState::Avg { sum: 0.0, count: 0 }
            }
        }
    }

    /// Update this accumulator with a single scalar value.
    ///
    /// NULL values are skipped for all functions except `COUNT(*)`.
    pub fn update(&mut self, value: &ScalarValue) {
        if value.is_null() {
            // COUNT(*) doesn't go through this path — it uses
            // update_count_star(). All other aggregates skip NULLs.
            return;
        }
        match self {
            AggState::Count(n) => *n += 1,
            AggState::CountNonNull(n) => *n += 1,
            AggState::Sum(sum) => match (sum, value) {
                (SumState::Int(acc), ScalarValue::Int(v)) => *acc = acc.wrapping_add(*v),
                (SumState::Float(acc), ScalarValue::Float(v)) => *acc += v,
                (SumState::Float(acc), ScalarValue::Int(v)) => *acc += *v as f64,
                _ => {}
            },
            AggState::Min(current) => match current {
                None => *current = Some(value.clone()),
                Some(cur) if value < cur => *current = Some(value.clone()),
                _ => {}
            },
            AggState::Max(current) => match current {
                None => *current = Some(value.clone()),
                Some(cur) if value > cur => *current = Some(value.clone()),
                _ => {}
            },
            AggState::Avg { sum, count } => {
                let v = match value {
                    ScalarValue::Int(n) => *n as f64,
                    ScalarValue::Float(f) => *f,
                    _ => return,
                };
                *sum += v;
                *count += 1;
            }
            AggState::CountDistinct(set) => {
                set.insert(value.clone());
            }
            AggState::Variance { count, mean, m2 } => {
                let v = match value {
                    ScalarValue::Int(n) => *n as f64,
                    ScalarValue::Float(f) => *f,
                    _ => return,
                };
                *count += 1;
                let delta = v - *mean;
                *mean += delta / *count as f64;
                let delta2 = v - *mean;
                *m2 += delta * delta2;
            }
        }
    }

    /// Increment the count for `COUNT(*)`.
    #[inline]
    pub fn update_count_star(&mut self) {
        if let AggState::Count(n) = self {
            *n += 1;
        }
    }

    /// Merge another `AggState` into this one (for cross-shard reduction).
    ///
    /// Panics on variant mismatch (a plan-time bug).
    pub fn merge(&mut self, other: &AggState) {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::CountNonNull(a), AggState::CountNonNull(b)) => *a += b,
            (AggState::Sum(a), AggState::Sum(b)) => a.merge(b),
            (AggState::Min(a), AggState::Min(b)) => match (a.as_ref(), b) {
                (_, None) => {}
                (None, Some(bv)) => *a = Some(bv.clone()),
                (Some(av), Some(bv)) if bv < av => *a = Some(bv.clone()),
                _ => {}
            },
            (AggState::Max(a), AggState::Max(b)) => match (a.as_ref(), b) {
                (_, None) => {}
                (None, Some(bv)) => *a = Some(bv.clone()),
                (Some(av), Some(bv)) if bv > av => *a = Some(bv.clone()),
                _ => {}
            },
            (AggState::Avg { sum: s1, count: c1 }, AggState::Avg { sum: s2, count: c2 }) => {
                *s1 += s2;
                *c1 += c2;
            }
            (AggState::CountDistinct(a), AggState::CountDistinct(b)) => {
                a.extend(b.iter().cloned());
            }
            (
                AggState::Variance {
                    count: n1,
                    mean: m1,
                    m2: s1,
                },
                AggState::Variance {
                    count: n2,
                    mean: m2,
                    m2: s2,
                },
            ) => {
                // Parallel Welford merge formula.
                if *n2 == 0 {
                    return;
                }
                let total = *n1 + *n2;
                let delta = *m2 - *m1;
                *s1 += *s2 + delta * delta * (*n1 as f64) * (*n2 as f64) / (total as f64);
                *m1 = (*m1 * (*n1 as f64) + *m2 * (*n2 as f64)) / (total as f64);
                *n1 = total;
            }
            _ => panic!("AggState::merge variant mismatch"),
        }
    }

    /// Finalize this accumulator to produce an output `ScalarValue`.
    pub fn finalize(&self) -> ScalarValue {
        match self {
            AggState::Count(n) => ScalarValue::Int(*n as i64),
            AggState::CountNonNull(n) => ScalarValue::Int(*n as i64),
            AggState::Sum(s) => s.to_scalar(),
            AggState::Min(v) => v.clone().unwrap_or(ScalarValue::Null),
            AggState::Max(v) => v.clone().unwrap_or(ScalarValue::Null),
            AggState::Avg { sum, count } => {
                if *count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float(*sum / *count as f64)
                }
            }
            AggState::CountDistinct(set) => ScalarValue::Int(set.len() as i64),
            AggState::Variance { count, m2, .. } => {
                if *count < 2 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float(*m2 / (*count - 1) as f64)
                }
            }
        }
    }

    /// Estimated heap size in bytes (for memory accounting).
    pub fn heap_size(&self) -> usize {
        match self {
            AggState::Count(_) | AggState::CountNonNull(_) => 0,
            AggState::Sum(_) => 0,
            AggState::Min(v) | AggState::Max(v) => v.as_ref().map_or(0, scalar_heap_size),
            AggState::Avg { .. } | AggState::Variance { .. } => 0,
            AggState::CountDistinct(set) => {
                // Rough estimate: bucket overhead + per-entry string heap.
                let bucket_overhead = set.capacity() * (mem::size_of::<ScalarValue>() + 24);
                let payload: usize = set.iter().map(scalar_heap_size).sum();
                bucket_overhead + payload
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Accumulator trait
// ──────────────────────────────────────────────────────────────────────────────

/// Aggregation accumulator protocol.
///
/// Receives incremental updates from fused entity operators and from
/// non-fused aggregate nodes. One accumulator per shard, shared across
/// the morsels of that shard; merged across shards after execution.
///
/// See execution-model.md §9.5 for the full protocol description.
pub trait Accumulator: Send {
    /// Update with the reduced values for one entity, match, session,
    /// or path. `group_key` is `None` for ungrouped aggregation.
    /// `values` contains one slot per aggregate function, in the order
    /// declared by the corresponding `FusableAggregate::functions` list.
    fn update(&mut self, group_key: Option<&[ScalarValue]>, values: &[ScalarValue]) -> Result<()>;

    /// Bulk update from a `RecordBatch` — the non-fused path used by
    /// the default `EntityOperator::finish_entity_into()` and by the
    /// plain `AggregatePhysical` node when no fusion is in effect.
    fn update_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Merge another accumulator into this one (for cross-shard
    /// reduction). Each shard produces one accumulator; they are merged
    /// pairwise on the coordinator after every shard's last morsel
    /// finishes.
    fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()>;

    /// Produce the final aggregated `RecordBatch`.
    fn finish(&self) -> Result<RecordBatch>;

    /// Current memory usage estimate — reported to the memory tracker
    /// and surfaced through query metrics. Aggregation does not spill
    /// in v1, so this is informational rather than a spill trigger.
    fn memory_usage(&self) -> usize;

    /// Downcast support for cross-shard merge.
    fn as_any(&self) -> &dyn std::any::Any;
}

// ──────────────────────────────────────────────────────────────────────────────
// HashAccumulator
// ──────────────────────────────────────────────────────────────────────────────

/// Flat hash-map accumulator from group key to per-function state.
///
/// This is the default (and only v1) `Accumulator` implementation.
/// It maintains a `HashMap<GroupKey, Vec<AggState>>` where each group
/// maps to one `AggState` per aggregate function.
///
/// Group cardinality is bounded by `max_groups` (default 1,000,000).
/// When the cap is reached and a new group is encountered, `update`
/// returns `BqliteError::Execution` with the overflow message.
/// There is no spill-to-disk for aggregation state in v1.
pub struct HashAccumulator {
    /// Per-group state. Key is the group-by values tuple.
    groups: HashMap<GroupKey, Vec<AggState>>,
    /// Schema of the final aggregated output.
    output_schema: OperatorSchema,
    /// Hard cap on distinct groups.
    max_groups: usize,
    /// The aggregate functions in order.
    functions: Vec<AggFunction>,
    /// The resolved input type per aggregate function.
    input_types: Vec<Option<BqlType>>,
    /// Column names in the input batch for group-by expressions.
    group_by_columns: Vec<String>,
    /// Column names in the input batch for aggregate arguments.
    agg_arg_columns: Vec<Option<String>>,
}

/// Default maximum groups for `HashAccumulator`.
pub const DEFAULT_MAX_GROUPS: usize = 1_000_000;

impl HashAccumulator {
    /// Create a new `HashAccumulator`.
    ///
    /// - `functions`: the aggregate functions in output order.
    /// - `input_types`: the resolved input BQL type per function
    ///   (`None` for `COUNT(*)`).
    /// - `output_schema`: the final output schema (group columns +
    ///   aggregate columns).
    /// - `group_by_columns`: column names in the input batch for
    ///   group-by key extraction (empty for ungrouped).
    /// - `agg_arg_columns`: column names in the input batch for
    ///   aggregate argument extraction (`None` for `COUNT(*)`).
    /// - `max_groups`: hard cap on distinct group count.
    pub fn new(
        functions: Vec<AggFunction>,
        input_types: Vec<Option<BqlType>>,
        output_schema: OperatorSchema,
        group_by_columns: Vec<String>,
        agg_arg_columns: Vec<Option<String>>,
        max_groups: usize,
    ) -> Self {
        Self {
            groups: HashMap::new(),
            output_schema,
            max_groups,
            functions,
            input_types,
            group_by_columns,
            agg_arg_columns,
        }
    }

    /// Returns the output schema.
    pub fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    /// Returns the number of distinct groups currently tracked.
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }

    /// Returns the maximum groups limit.
    pub fn max_groups(&self) -> usize {
        self.max_groups
    }

    /// Create the initial `Vec<AggState>` for a new group.
    fn create_group_states(&self) -> Vec<AggState> {
        self.functions
            .iter()
            .zip(self.input_types.iter())
            .map(|(func, input_type)| AggState::new(*func, input_type.as_ref()))
            .collect()
    }

    /// Get or create the state vector for a group key, enforcing
    /// the `max_groups` cap.
    fn get_or_create_group(&mut self, key: GroupKey) -> Result<&mut Vec<AggState>> {
        if !self.groups.contains_key(&key) {
            if self.groups.len() >= self.max_groups {
                return Err(BqliteError::Execution(format!(
                    "aggregation group cardinality limit exceeded: {} groups",
                    self.max_groups
                )));
            }
            let states = self.create_group_states();
            self.groups.insert(key.clone(), states);
        }
        Ok(self.groups.get_mut(&key).unwrap())
    }

    /// Extract a `ScalarValue` from a column array at the given row.
    fn extract_scalar(array: &ArrayRef, row: usize) -> ScalarValue {
        if array.is_null(row) {
            return ScalarValue::Null;
        }
        let dt = array.data_type();
        match dt {
            arrow::datatypes::DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                ScalarValue::Bool(arr.value(row))
            }
            arrow::datatypes::DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                ScalarValue::Int(arr.value(row))
            }
            arrow::datatypes::DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                ScalarValue::Float(arr.value(row))
            }
            arrow::datatypes::DataType::Utf8View => {
                let arr = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                ScalarValue::String(arr.value(row).to_owned())
            }
            arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                ScalarValue::Timestamp(arr.value(row))
            }
            _ => ScalarValue::Null,
        }
    }
}

impl Accumulator for HashAccumulator {
    fn update(&mut self, group_key: Option<&[ScalarValue]>, values: &[ScalarValue]) -> Result<()> {
        let key = match group_key {
            Some(k) => GroupKey::from_values(k),
            None => GroupKey::empty(),
        };
        let states = self.get_or_create_group(key)?;
        for (state, value) in states.iter_mut().zip(values.iter()) {
            match state {
                AggState::Count(_) => state.update_count_star(),
                _ => state.update(value),
            }
        }
        Ok(())
    }

    fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Resolve group-by column arrays.
        let group_arrays: Vec<ArrayRef> = self
            .group_by_columns
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("group-by column '{name}' not found in batch"))
                    .clone()
            })
            .collect();

        // Resolve aggregate argument column arrays.
        let agg_arrays: Vec<Option<ArrayRef>> = self
            .agg_arg_columns
            .iter()
            .map(|opt_name| {
                opt_name.as_ref().map(|name| {
                    batch
                        .column_by_name(name)
                        .unwrap_or_else(|| panic!("aggregate column '{name}' not found in batch"))
                        .clone()
                })
            })
            .collect();

        // Process each row.
        for row in 0..num_rows {
            // Build group key.
            let key = if group_arrays.is_empty() {
                GroupKey::empty()
            } else {
                let key_values: Vec<ScalarValue> = group_arrays
                    .iter()
                    .map(|arr| Self::extract_scalar(arr, row))
                    .collect();
                GroupKey(key_values)
            };

            let states = self.get_or_create_group(key)?;

            // Update each aggregate.
            for (i, state) in states.iter_mut().enumerate() {
                match state {
                    AggState::Count(_) => state.update_count_star(),
                    _ => {
                        if let Some(arr) = &agg_arrays[i] {
                            let value = Self::extract_scalar(arr, row);
                            state.update(&value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<HashAccumulator>()
            .ok_or_else(|| {
                BqliteError::Execution("Accumulator::merge: expected HashAccumulator".to_string())
            })?;

        for (key, other_states) in &other.groups {
            if let Some(self_states) = self.groups.get_mut(key) {
                for (s, o) in self_states.iter_mut().zip(other_states.iter()) {
                    s.merge(o);
                }
            } else {
                if self.groups.len() >= self.max_groups {
                    return Err(BqliteError::Execution(format!(
                        "aggregation group cardinality limit exceeded: {} groups",
                        self.max_groups
                    )));
                }
                self.groups.insert(key.clone(), other_states.clone());
            }
        }
        Ok(())
    }

    fn finish(&self) -> Result<RecordBatch> {
        use std::sync::Arc;

        let num_group_cols = self.group_by_columns.len();
        let num_agg_cols = self.functions.len();

        let schema = self.output_schema.to_arrow_schema();

        // Build column arrays for group-by columns + aggregate results.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + num_agg_cols);

        // Collect groups in deterministic order for testing.
        let mut sorted_groups: Vec<(&GroupKey, &Vec<AggState>)> = self.groups.iter().collect();
        sorted_groups.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Build group-by columns.
        for col_idx in 0..num_group_cols {
            let field = &schema.fields()[col_idx];
            let array = build_group_column(
                &sorted_groups,
                col_idx,
                field.data_type(),
                field.is_nullable(),
            );
            columns.push(array);
        }

        // Build aggregate result columns.
        for agg_idx in 0..num_agg_cols {
            let field = &schema.fields()[num_group_cols + agg_idx];
            let values: Vec<ScalarValue> = sorted_groups
                .iter()
                .map(|(_, states)| states[agg_idx].finalize())
                .collect();
            let array = scalars_to_arrow(&values, field.data_type(), field.is_nullable());
            columns.push(array);
        }

        RecordBatch::try_new(Arc::new(schema), columns).map_err(BqliteError::from)
    }

    fn memory_usage(&self) -> usize {
        let mut total = mem::size_of::<Self>();
        for (key, states) in &self.groups {
            total += key.heap_size();
            total += states.iter().map(|s| s.heap_size()).sum::<usize>();
            total += mem::size_of::<GroupKey>() + mem::size_of::<Vec<AggState>>();
        }
        // HashMap overhead.
        total += self.groups.capacity()
            * (mem::size_of::<GroupKey>() + mem::size_of::<Vec<AggState>>() + 8);
        total
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Estimated heap allocation for a single `ScalarValue`.
fn scalar_heap_size(v: &ScalarValue) -> usize {
    match v {
        ScalarValue::String(s) => s.capacity(),
        _ => 0,
    }
}

/// Build an Arrow array from group-key values at a given column index.
fn build_group_column(
    groups: &[(&GroupKey, &Vec<AggState>)],
    col_idx: usize,
    data_type: &arrow::datatypes::DataType,
    nullable: bool,
) -> ArrayRef {
    let values: Vec<ScalarValue> = groups
        .iter()
        .map(|(key, _)| {
            if col_idx < key.0.len() {
                key.0[col_idx].clone()
            } else {
                ScalarValue::Null
            }
        })
        .collect();
    scalars_to_arrow(&values, data_type, nullable)
}

/// Convert a slice of `ScalarValue` to an Arrow `ArrayRef`.
fn scalars_to_arrow(
    values: &[ScalarValue],
    data_type: &arrow::datatypes::DataType,
    _nullable: bool,
) -> ArrayRef {
    use arrow::array::{
        BooleanBuilder, Float64Builder, Int64Builder, StringViewBuilder, TimestampNanosecondBuilder,
    };
    use std::sync::Arc;

    match data_type {
        arrow::datatypes::DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Bool(b) => builder.append_value(*b),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        arrow::datatypes::DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int(n) => builder.append_value(*n),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        arrow::datatypes::DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Float(f) => builder.append_value(*f),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        arrow::datatypes::DataType::Utf8View => {
            let mut builder = StringViewBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::String(s) => builder.append_value(s),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let mut builder = TimestampNanosecondBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Timestamp(ns) => builder.append_value(*ns),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => {
            // Fallback: all nulls.
            let mut builder = Int64Builder::with_capacity(values.len());
            for _ in values {
                builder.append_null();
            }
            Arc::new(builder.finish())
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use bqlite_core::ColumnDef;

    fn make_output_schema(
        group_cols: &[(&str, BqlType, bool)],
        agg_cols: &[(&str, BqlType, bool)],
    ) -> OperatorSchema {
        let mut cols: Vec<ColumnDef> = Vec::new();
        for &(name, ref ty, nullable) in group_cols {
            if nullable {
                cols.push(ColumnDef::nullable(name, ty.clone()));
            } else {
                cols.push(ColumnDef::required(name, ty.clone()));
            }
        }
        for &(name, ref ty, nullable) in agg_cols {
            if nullable {
                cols.push(ColumnDef::nullable(name, ty.clone()));
            } else {
                cols.push(ColumnDef::required(name, ty.clone()));
            }
        }
        OperatorSchema::new(cols).unwrap()
    }

    // ── GroupKey ──────────────────────────────────────────────────────────────

    #[test]
    fn group_key_empty() {
        let key = GroupKey::empty();
        assert!(key.is_empty());
        assert_eq!(key.len(), 0);
    }

    #[test]
    fn group_key_from_values() {
        let key = GroupKey::from_values(&[ScalarValue::Int(1), ScalarValue::String("a".into())]);
        assert_eq!(key.len(), 2);
        assert!(!key.is_empty());
    }

    #[test]
    fn group_key_equality_and_hashing() {
        use std::collections::HashMap;
        let k1 = GroupKey::from_values(&[ScalarValue::Int(42)]);
        let k2 = GroupKey::from_values(&[ScalarValue::Int(42)]);
        assert_eq!(k1, k2);

        let mut map: HashMap<GroupKey, u32> = HashMap::new();
        map.insert(k1, 1);
        assert_eq!(map.get(&k2), Some(&1));
    }

    // ── AggState basics ──────────────────────────────────────────────────────

    #[test]
    fn count_star() {
        let mut state = AggState::new(AggFunction::Count, None);
        state.update_count_star();
        state.update_count_star();
        state.update_count_star();
        assert_eq!(state.finalize(), ScalarValue::Int(3));
    }

    #[test]
    fn count_column_skips_nulls() {
        let mut state = AggState::new(AggFunction::CountColumn, Some(&BqlType::Int));
        state.update(&ScalarValue::Int(1));
        state.update(&ScalarValue::Null);
        state.update(&ScalarValue::Int(3));
        assert_eq!(state.finalize(), ScalarValue::Int(2));
    }

    #[test]
    fn sum_int() {
        let mut state = AggState::new(AggFunction::Sum, Some(&BqlType::Int));
        state.update(&ScalarValue::Int(10));
        state.update(&ScalarValue::Int(20));
        state.update(&ScalarValue::Null);
        assert_eq!(state.finalize(), ScalarValue::Int(30));
    }

    #[test]
    fn sum_float() {
        let mut state = AggState::new(AggFunction::Sum, Some(&BqlType::Float));
        state.update(&ScalarValue::Float(1.5));
        state.update(&ScalarValue::Float(2.5));
        assert_eq!(state.finalize(), ScalarValue::Float(4.0));
    }

    #[test]
    fn min_max() {
        let mut min_state = AggState::new(AggFunction::Min, Some(&BqlType::Int));
        let mut max_state = AggState::new(AggFunction::Max, Some(&BqlType::Int));
        for v in [3, 1, 4, 1, 5, 9] {
            min_state.update(&ScalarValue::Int(v));
            max_state.update(&ScalarValue::Int(v));
        }
        assert_eq!(min_state.finalize(), ScalarValue::Int(1));
        assert_eq!(max_state.finalize(), ScalarValue::Int(9));
    }

    #[test]
    fn min_max_all_null() {
        let mut min_state = AggState::new(AggFunction::Min, Some(&BqlType::Int));
        min_state.update(&ScalarValue::Null);
        assert_eq!(min_state.finalize(), ScalarValue::Null);
    }

    #[test]
    fn avg_basic() {
        let mut state = AggState::new(AggFunction::Avg, Some(&BqlType::Int));
        state.update(&ScalarValue::Int(10));
        state.update(&ScalarValue::Int(20));
        state.update(&ScalarValue::Int(30));
        assert_eq!(state.finalize(), ScalarValue::Float(20.0));
    }

    #[test]
    fn avg_empty_is_null() {
        let state = AggState::new(AggFunction::Avg, Some(&BqlType::Int));
        assert_eq!(state.finalize(), ScalarValue::Null);
    }

    #[test]
    fn count_distinct() {
        let mut state = AggState::new(AggFunction::CountDistinct, Some(&BqlType::String));
        state.update(&ScalarValue::String("a".into()));
        state.update(&ScalarValue::String("b".into()));
        state.update(&ScalarValue::String("a".into()));
        state.update(&ScalarValue::Null);
        assert_eq!(state.finalize(), ScalarValue::Int(2));
    }

    // ── AggState merge ───────────────────────────────────────────────────────

    #[test]
    fn merge_count() {
        let mut a = AggState::Count(5);
        let b = AggState::Count(3);
        a.merge(&b);
        assert_eq!(a.finalize(), ScalarValue::Int(8));
    }

    #[test]
    fn merge_sum_int() {
        let mut a = AggState::Sum(SumState::Int(10));
        let b = AggState::Sum(SumState::Int(20));
        a.merge(&b);
        assert_eq!(a.finalize(), ScalarValue::Int(30));
    }

    #[test]
    fn merge_avg() {
        let mut a = AggState::Avg {
            sum: 10.0,
            count: 2,
        };
        let b = AggState::Avg {
            sum: 20.0,
            count: 3,
        };
        a.merge(&b);
        assert_eq!(a.finalize(), ScalarValue::Float(6.0)); // 30 / 5
    }

    #[test]
    fn merge_count_distinct() {
        let mut a = AggState::CountDistinct(HashSet::from([
            ScalarValue::String("x".into()),
            ScalarValue::String("y".into()),
        ]));
        let b = AggState::CountDistinct(HashSet::from([
            ScalarValue::String("y".into()),
            ScalarValue::String("z".into()),
        ]));
        a.merge(&b);
        assert_eq!(a.finalize(), ScalarValue::Int(3)); // x, y, z
    }

    // ── HashAccumulator ──────────────────────────────────────────────────────

    #[test]
    fn hash_accumulator_ungrouped_count() {
        let schema = make_output_schema(&[], &[("n", BqlType::Int, false)]);
        let mut acc = HashAccumulator::new(
            vec![AggFunction::Count],
            vec![None],
            schema,
            vec![],
            vec![None],
            DEFAULT_MAX_GROUPS,
        );

        acc.update(None, &[ScalarValue::Null]).unwrap();
        acc.update(None, &[ScalarValue::Null]).unwrap();
        acc.update(None, &[ScalarValue::Null]).unwrap();

        let batch = acc.finish().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 3);
    }

    #[test]
    fn hash_accumulator_grouped_sum() {
        let schema = make_output_schema(
            &[("country", BqlType::String, false)],
            &[("total", BqlType::Int, true)],
        );
        let mut acc = HashAccumulator::new(
            vec![AggFunction::Sum],
            vec![Some(BqlType::Int)],
            schema,
            vec!["country".into()],
            vec![Some("amount".into())],
            DEFAULT_MAX_GROUPS,
        );

        acc.update(
            Some(&[ScalarValue::String("US".into())]),
            &[ScalarValue::Int(100)],
        )
        .unwrap();
        acc.update(
            Some(&[ScalarValue::String("UK".into())]),
            &[ScalarValue::Int(200)],
        )
        .unwrap();
        acc.update(
            Some(&[ScalarValue::String("US".into())]),
            &[ScalarValue::Int(300)],
        )
        .unwrap();

        let batch = acc.finish().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(acc.num_groups(), 2);
    }

    #[test]
    fn hash_accumulator_max_groups_enforced() {
        let schema = make_output_schema(
            &[("id", BqlType::Int, false)],
            &[("n", BqlType::Int, false)],
        );
        let mut acc = HashAccumulator::new(
            vec![AggFunction::Count],
            vec![None],
            schema,
            vec!["id".into()],
            vec![None],
            2, // max 2 groups
        );

        acc.update(Some(&[ScalarValue::Int(1)]), &[ScalarValue::Null])
            .unwrap();
        acc.update(Some(&[ScalarValue::Int(2)]), &[ScalarValue::Null])
            .unwrap();
        // Third distinct group should fail.
        let result = acc.update(Some(&[ScalarValue::Int(3)]), &[ScalarValue::Null]);
        assert!(result.is_err());
    }

    #[test]
    fn hash_accumulator_memory_usage_positive() {
        let schema = make_output_schema(&[], &[("n", BqlType::Int, false)]);
        let acc = HashAccumulator::new(
            vec![AggFunction::Count],
            vec![None],
            schema,
            vec![],
            vec![None],
            DEFAULT_MAX_GROUPS,
        );
        assert!(acc.memory_usage() > 0);
    }

    // ── SumState ─────────────────────────────────────────────────────────────

    #[test]
    fn sum_state_merge_int() {
        let mut a = SumState::Int(10);
        a.merge(&SumState::Int(20));
        assert_eq!(a.to_scalar(), ScalarValue::Int(30));
    }

    #[test]
    fn sum_state_merge_float() {
        let mut a = SumState::Float(1.5);
        a.merge(&SumState::Float(2.5));
        assert_eq!(a.to_scalar(), ScalarValue::Float(4.0));
    }

    #[test]
    #[should_panic(expected = "SumState::merge variant mismatch")]
    fn sum_state_merge_mismatch_panics() {
        let mut a = SumState::Int(0);
        a.merge(&SumState::Float(0.0));
    }

    // ── Variance ─────────────────────────────────────────────────────────────

    #[test]
    fn variance_basic() {
        let mut state = AggState::Variance {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        };
        // Values: 2, 4, 4, 4, 5, 5, 7, 9 => variance = 4.571...
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            state.update(&ScalarValue::Float(v));
        }
        if let ScalarValue::Float(var) = state.finalize() {
            assert!((var - 4.571428571428571).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn variance_merge() {
        let mut a = AggState::Variance {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        };
        let mut b = AggState::Variance {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        };
        for v in [2.0, 4.0, 4.0, 4.0] {
            a.update(&ScalarValue::Float(v));
        }
        for v in [5.0, 5.0, 7.0, 9.0] {
            b.update(&ScalarValue::Float(v));
        }
        a.merge(&b);
        if let ScalarValue::Float(var) = a.finalize() {
            assert!((var - 4.571428571428571).abs() < 1e-10);
        } else {
            panic!("expected Float");
        }
    }

    #[test]
    fn variance_merge_with_empty_shard() {
        let mut a = AggState::Variance {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        };
        let empty = AggState::Variance {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        };
        for v in [2.0, 4.0, 6.0] {
            a.update(&ScalarValue::Float(v));
        }
        let expected = a.finalize();
        a.merge(&empty);
        assert_eq!(a.finalize(), expected);
    }

    // ── update_batch integration ─────────────────────────────────────────

    #[test]
    fn update_batch_ungrouped_count() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let schema = make_output_schema(&[], &[("n", BqlType::Int, false)]);
        let mut acc = HashAccumulator::new(
            vec![AggFunction::Count],
            vec![None],
            schema,
            vec![],
            vec![None],
            DEFAULT_MAX_GROUPS,
        );

        // Build a RecordBatch with 5 rows.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let batch = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();

        acc.update_batch(&batch).unwrap();

        let result = acc.finish().unwrap();
        assert_eq!(result.num_rows(), 1);
        let count_col = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 5);
    }

    #[test]
    fn update_batch_grouped_sum() {
        use arrow::array::{Int64Array, StringViewArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        let schema = make_output_schema(
            &[("country", BqlType::String, false)],
            &[("total", BqlType::Int, true)],
        );
        let mut acc = HashAccumulator::new(
            vec![AggFunction::Sum],
            vec![Some(BqlType::Int)],
            schema,
            vec!["country".into()],
            vec![Some("amount".into())],
            DEFAULT_MAX_GROUPS,
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("country", DataType::Utf8View, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let countries: ArrayRef =
            Arc::new(StringViewArray::from(vec!["US", "UK", "US", "UK", "US"]));
        let amounts: ArrayRef = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));
        let batch = RecordBatch::try_new(arrow_schema, vec![countries, amounts]).unwrap();

        acc.update_batch(&batch).unwrap();

        let result = acc.finish().unwrap();
        assert_eq!(result.num_rows(), 2);
        // Groups are sorted by GroupKey (Ord), so UK < US.
        let country_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let total_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(country_col.value(0), "UK");
        assert_eq!(total_col.value(0), 600); // 200 + 400
        assert_eq!(country_col.value(1), "US");
        assert_eq!(total_col.value(1), 900); // 100 + 300 + 500
    }

    // ── Accumulator merge integration ────────────────────────────────────

    #[test]
    fn hash_accumulator_merge_overlapping_groups() {
        let schema = make_output_schema(
            &[("country", BqlType::String, false)],
            &[("n", BqlType::Int, false)],
        );
        let mut acc1 = HashAccumulator::new(
            vec![AggFunction::Count],
            vec![None],
            schema.clone(),
            vec!["country".into()],
            vec![None],
            DEFAULT_MAX_GROUPS,
        );
        let mut acc2 = HashAccumulator::new(
            vec![AggFunction::Count],
            vec![None],
            schema,
            vec!["country".into()],
            vec![None],
            DEFAULT_MAX_GROUPS,
        );

        // Shard 1: US=2, UK=1
        acc1.update(
            Some(&[ScalarValue::String("US".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();
        acc1.update(
            Some(&[ScalarValue::String("US".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();
        acc1.update(
            Some(&[ScalarValue::String("UK".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();

        // Shard 2: US=1, DE=3
        acc2.update(
            Some(&[ScalarValue::String("US".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();
        acc2.update(
            Some(&[ScalarValue::String("DE".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();
        acc2.update(
            Some(&[ScalarValue::String("DE".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();
        acc2.update(
            Some(&[ScalarValue::String("DE".into())]),
            &[ScalarValue::Null],
        )
        .unwrap();

        // Merge shard 2 into shard 1.
        acc1.merge(Box::new(acc2)).unwrap();

        let result = acc1.finish().unwrap();
        assert_eq!(result.num_rows(), 3); // DE, UK, US
        assert_eq!(acc1.num_groups(), 3);

        let country_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let count_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Sorted by GroupKey: DE < UK < US
        assert_eq!(country_col.value(0), "DE");
        assert_eq!(count_col.value(0), 3);
        assert_eq!(country_col.value(1), "UK");
        assert_eq!(count_col.value(1), 1);
        assert_eq!(country_col.value(2), "US");
        assert_eq!(count_col.value(2), 3); // 2 + 1
    }
}
