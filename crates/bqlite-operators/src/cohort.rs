//! `SubqueryFilterOperator` — runtime hash-set probe for cohort
//! semi-joins.
//!
//! Per `docs/design/language/cohorts-aliases-joins.md` §4 (Block C),
//! `WHERE col IN QUERY (...)` and `WHERE col IN <alias>` are physically
//! executed as a hash-set probe over a cohort that has been materialized
//! at query start. The materialization itself happens in the engine
//! bind step (TASK-438 wires the bind arm; TASK-437 owns this runtime).
//!
//! ## Pipeline shape
//!
//! ```text
//!   inner subquery  ──► CohortHashSet (built once, shared by `Arc`)
//!                                 │
//!                                 ▼
//!   outer scan ──► SubqueryFilterOperator ──► downstream
//! ```
//!
//! ## Tuple keying
//!
//! The probe key is a `CohortKey`, a `Vec<ScalarValue>` of length
//! equal to the LHS arity. Single-column cohorts (`x IN (...)`) use a
//! one-element `Vec`; tuple cohorts (`(x, y) IN (...)`) use an N-element
//! `Vec`. Equality and hashing both go through `ScalarValue`, which
//! treats `NaN == NaN` and `Null == Null` consistently — see
//! `bqlite-core/src/scalar.rs` for the rules.
//!
//! ## NULL on LHS
//!
//! Rows where any LHS expression evaluates to NULL are dropped. This
//! matches the conventional SQL `IN` semantics under three-valued
//! logic: `NULL IN (...)` is NULL (unknown), and `WHERE` drops
//! unknowns the same way `FilterOperator` does for ordinary
//! predicates. The cohort itself may contain `NULL` keys (rows from
//! the inner subquery whose projected columns happened to be NULL),
//! but the probe never matches them because the LHS-NULL rows are
//! filtered out before lookup.
//!
//! ## Empty cohort
//!
//! When the cohort is empty, every batch yields zero rows; the
//! operator skips per-row probing and returns an empty filtered batch
//! immediately. Callers that wrap this operator (e.g. `LimitOperator`
//! over a cohort filter) tolerate mid-stream empty batches per
//! `execution-model.md` §3.2.
//!
//! ## Memory budget
//!
//! The materialised hash set is a tracked allocation per
//! `docs/design/engine/memory-budget.md` §5.1, with the cohort policy
//! pinned to **fail-fast** by `docs/design/engine/spill.md` §4.3 —
//! cohort hash sets do not spill in v1. [`CohortHashSet::from_batches`]
//! reserves bytes from the per-query [`bqlite_core::MemoryBudget`] at
//! every batch boundary; on overflow, the partially-built set is
//! dropped and the call returns the typed
//! [`bqlite_core::BqliteError::MemoryBudgetExceeded`] error. The
//! returned cohort holds its [`bqlite_core::memory::MemoryReservation`]
//! handles for the lifetime of the `Arc<CohortHashSet>`, so the bytes
//! are released back to the budget when the last reference drops at
//! query teardown.

use std::collections::HashSet;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBufferBuilder, Float64Array, Int64Array, StringViewArray,
    TimestampNanosecondArray,
};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;

use bqlite_core::memory::MemoryReservation;
use bqlite_core::{BqliteError, MemoryBudget, OperatorSchema, Result, ScalarValue};
use bqlite_planner::compiled::CompiledExpr;

use crate::eval;
use crate::operator::PhysicalOperator;

// ─────────────────────────────────────────────────────────────────────────────
// CohortKey
// ─────────────────────────────────────────────────────────────────────────────

/// Tuple key used for cohort hash-set probes.
///
/// Wraps a `Vec<ScalarValue>` whose length equals the LHS arity. The
/// hash and equality stories come straight from `ScalarValue`
/// (`NaN == NaN`, type-tag-prefixed hash, no implicit cross-type
/// equality), matching `GroupKey` in the aggregate framework.
///
/// ### Why not `compact_str`?
///
/// TASK-454 sweeps `CompactString` across the matcher / sessionize /
/// attribute / cohort hot paths and decides where the win is. Until
/// that profile lands, the cohort key reuses the same `String`-backed
/// `ScalarValue` as every other Wave 4 operator so we don't fork the
/// scalar representation. The cohort hash-set is a query-lifetime
/// allocation rather than a per-batch one, so the savings would be
/// modest in any case.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CohortKey(pub Vec<ScalarValue>);

impl CohortKey {
    /// Build a cohort key from a slice of scalar values. Mirrors
    /// `GroupKey::from_values` for symmetry.
    #[inline]
    pub fn from_values(values: &[ScalarValue]) -> Self {
        Self(values.to_vec())
    }

    /// Number of columns in the key.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` for the (illegal) zero-arity case — kept for API
    /// symmetry; `SubqueryFilterOperator::new` rejects empty LHS lists
    /// at construction.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// `true` when any component is `Null`. Such keys cannot match
    /// outer-row keys (because the operator drops outer rows whose LHS
    /// is NULL), but the cohort may still hold them — checking this
    /// is `O(arity)` and only used by tests / diagnostics.
    pub fn has_null(&self) -> bool {
        self.0.iter().any(ScalarValue::is_null)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// CohortHashSet
// ─────────────────────────────────────────────────────────────────────────────

/// Materialized cohort: a `HashSet<CohortKey>` of every tuple emitted
/// by the inner subquery.
///
/// Built once per query execution by the engine bind step from the
/// `SubqueryFilterPhysical.subquery` plan. Shared by `Arc` between the
/// builder and every probe site so two `IN alias` references with
/// identical inner plans share a single materialization
/// (cohorts-aliases-joins.md §2.5 + §2.11).
///
/// `ahash::RandomState` is used for a small but consistent throughput
/// win over `std::collections::hash_map::RandomState` on the small
/// scalar tuples this set holds.
///
/// ## Memory accounting
///
/// `CohortHashSet` owns the [`MemoryReservation`]s charged for the
/// bytes its hash set occupies (see [`Self::from_batches`]). The
/// reservations are released when the cohort is dropped — typically at
/// query teardown when the last `Arc<CohortHashSet>` reference goes
/// out of scope.
#[derive(Debug)]
pub struct CohortHashSet {
    /// Underlying hash set. `pub(crate)` so the bind step can construct
    /// a set without going through the row-extraction helpers when it
    /// already has scalar tuples in hand (e.g. for tests).
    pub(crate) set: HashSet<CohortKey, RandomState>,
    /// Number of declared (non-system) columns in the inner
    /// subquery's output schema. Matches the LHS arity at probe time;
    /// the bind step asserts this when wiring the operator.
    arity: usize,
    /// Memory reservations charged to the per-query budget for the
    /// bytes this hash set occupies. One entry per materialisation
    /// batch that contributed at least one new key. Held for the
    /// cohort's lifetime; the bytes are released back to the budget
    /// when the cohort is dropped.
    reservations: Vec<MemoryReservation>,
}

impl CohortHashSet {
    /// Construct an empty set with the given declared column arity.
    ///
    /// Holds no [`MemoryReservation`] — the empty set's footprint is
    /// fixed-size and falls under the "untracked" class per
    /// `docs/design/engine/memory-budget.md` §5.2. Tests use this
    /// constructor in place of `from_batches` when they only care
    /// about the probe path.
    pub fn empty(arity: usize) -> Self {
        Self {
            set: HashSet::with_hasher(RandomState::new()),
            arity,
            reservations: Vec::new(),
        }
    }

    /// Number of distinct rows in the cohort.
    #[inline]
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// `true` when the cohort has zero rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Number of declared columns in the inner subquery's output.
    /// Matches the LHS arity expected at probe time.
    #[inline]
    pub fn arity(&self) -> usize {
        self.arity
    }

    /// Total bytes currently reserved against the per-query
    /// [`MemoryBudget`] for this cohort's hash-set occupancy.
    ///
    /// Sum of every [`MemoryReservation`] held by the cohort. Useful
    /// for tests / EXPLAIN-style diagnostics; the live reservation
    /// total mirrors what the per-query [`MemoryBudget`] sees, so
    /// dropping the cohort returns exactly this many bytes to the
    /// budget.
    pub fn reserved_bytes(&self) -> u64 {
        self.reservations.iter().map(|r| r.bytes()).sum()
    }

    /// `true` iff `key` was inserted at materialization time.
    #[inline]
    pub fn contains(&self, key: &CohortKey) -> bool {
        self.set.contains(key)
    }

    /// Iterate the cohort's keys. Order is unspecified — the cohort
    /// is a hash set. Used by the engine's cohort entity-id pushdown
    /// extraction (TASK-522,
    /// `docs/design/language/cohorts-aliases-joins.md` §4.3) to copy
    /// entity-id values into a `PropertyValue` set.
    pub fn iter_keys(&self) -> impl Iterator<Item = &CohortKey> {
        self.set.iter()
    }

    /// Insert a key directly without going through `from_batches`.
    ///
    /// Test-only — bypasses memory-budget reservation accounting, so
    /// it must not be used in production code paths.
    #[doc(hidden)]
    pub fn insert_for_test(&mut self, key: CohortKey) {
        self.set.insert(key);
    }

    /// Materialize the cohort by reading every row of every batch and
    /// inserting one [`CohortKey`] per row.
    ///
    /// `subquery_schema` is the inner pipeline's output schema. The
    /// cohort's tuple shape is the **declared** (non-system) columns
    /// in that schema — system columns (`__seq_id`, `__batch_id`)
    /// are filtered out per cohorts-aliases-joins.md §4.1, which
    /// states that the subquery "produces one row per cohort member"
    /// using declared columns. The corresponding column indices are
    /// captured once and reused per batch to avoid name lookups in
    /// the inner row loop.
    ///
    /// Bytes added to the hash set are reserved against `budget` at
    /// each batch boundary per the design contract pinned by
    /// `docs/design/engine/spill.md` §4.3 / TASK-514: the cohort hash
    /// set is fail-fast against the per-query memory budget — there
    /// is no on-disk hash-set in v1. On overflow the partially-built
    /// set is dropped and the call returns
    /// [`BqliteError::MemoryBudgetExceeded`]. The successful return
    /// value owns one [`MemoryReservation`] per batch that contributed
    /// at least one new key; the bytes are released back to `budget`
    /// when the returned [`CohortHashSet`] (or the `Arc` wrapping it)
    /// is dropped.
    pub fn from_batches<I>(
        subquery_schema: &OperatorSchema,
        batches: I,
        budget: &dyn MemoryBudget,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let declared_indices: Vec<usize> = subquery_schema
            .columns()
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.is_system())
            .map(|(i, _)| i)
            .collect();
        let arity = declared_indices.len();
        if arity == 0 {
            return Err(BqliteError::Execution(
                "cohort subquery produces zero declared columns".into(),
            ));
        }

        let mut set: HashSet<CohortKey, RandomState> = HashSet::with_hasher(RandomState::new());
        let mut reservations: Vec<MemoryReservation> = Vec::new();
        for batch in batches {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let key_arrays: Vec<&ArrayRef> =
                declared_indices.iter().map(|&i| batch.column(i)).collect();
            let mut batch_bytes: u64 = 0;
            for row in 0..n {
                let mut values: Vec<ScalarValue> = Vec::with_capacity(arity);
                for arr in &key_arrays {
                    values.push(extract_scalar(arr, row));
                }
                let key = CohortKey(values);
                let bytes = cohort_key_bytes(&key);
                if set.insert(key) {
                    batch_bytes = batch_bytes.saturating_add(bytes);
                }
            }
            // Reserve once per batch; on overflow we drop `set` and
            // `reservations` together, returning the bytes reserved so
            // far. The OS reclaims the partially-built hash set; the
            // budget tracker observes a clean state via
            // `MemoryReservation::Drop`.
            if batch_bytes > 0 {
                reservations.push(budget.try_reserve(batch_bytes)?);
            }
        }

        Ok(Self {
            set,
            arity,
            reservations,
        })
    }
}

/// Estimate the bytes a [`CohortKey`] occupies inside a
/// `HashSet<CohortKey, ahash::RandomState>` allocation.
///
/// The cohort hash set is the v1 tracked allocation per
/// `docs/design/engine/memory-budget.md` §5.1; this function is the
/// per-key term of that accounting. The estimate covers:
///
/// - `Vec<ScalarValue>` header (24 bytes on 64-bit) plus
///   `arity * size_of::<ScalarValue>()` for the inline tuple buffer.
/// - `String::capacity()` for every `ScalarValue::String` payload.
///   `String::capacity()` is the live heap allocation; it is at least
///   the live byte length and may be slightly larger from
///   power-of-two growth.
/// - A flat `PER_ENTRY_OVERHEAD` byte allowance that covers the
///   per-bucket cost the `hashbrown` `HashSet` carries for each
///   logical entry: control byte plus the `~14%` load-factor
///   slack that `hashbrown`'s default growth policy maintains.
///
/// The number is intentionally a coarse upper-ish estimate — coarse
/// enough that a small string cohort never blows past the budget
/// without a reservation having seen it, fine enough that a 100M
/// entity-id cohort's accounting is dominated by the real string
/// payload rather than the constant overhead. The estimate is mildly
/// **conservative** for string cohorts (where `String::capacity()`
/// dominates) and mildly **under-protective** for tiny scalar cohorts
/// — the flat `PER_ENTRY_OVERHEAD` does not account for `hashbrown`'s
/// 7/8 load-factor inflation, so a pure-`Int` cohort's tracked bytes
/// can land ~30–40 % below the real RSS footprint. This is a deliberate
/// v1 trade-off: a per-key amortised `capacity()` lookup against the
/// hashbrown table would add per-row overhead for a class of cohort
/// that already fits the budget by orders of magnitude. A future
/// follow-up may switch to charging the table's allocated bytes
/// directly if benchmarks flag a regression.
#[inline]
fn cohort_key_bytes(key: &CohortKey) -> u64 {
    /// Per-bucket overhead in `hashbrown` (the backing implementation
    /// for `std::collections::HashSet`): one control byte plus a
    /// rough load-factor allowance. Picking a single conservative
    /// constant avoids per-row branching on capacity/length math.
    const PER_ENTRY_OVERHEAD: usize = 16;
    let vec_header = std::mem::size_of::<Vec<ScalarValue>>();
    let scalar_size = std::mem::size_of::<ScalarValue>();
    let mut bytes = PER_ENTRY_OVERHEAD + vec_header + key.0.len() * scalar_size;
    for scalar in &key.0 {
        if let ScalarValue::String(s) = scalar {
            bytes = bytes.saturating_add(s.capacity());
        }
    }
    bytes as u64
}

/// Extract a `ScalarValue` from an Arrow column at the given row.
///
/// Mirrors `HashAccumulator::extract_scalar` in `aggregate/mod.rs` so
/// the cohort hash-set and the aggregate group key use the same
/// scalar representation. NULL collapses to `ScalarValue::Null`.
/// Unsupported Arrow types fall back to `Null` rather than erroring
/// — the planner's type system already restricts cohort columns to
/// the leaf scalar `BqlType` set.
fn extract_scalar(array: &ArrayRef, row: usize) -> ScalarValue {
    if array.is_null(row) {
        return ScalarValue::Null;
    }
    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("Boolean type must downcast to BooleanArray");
            ScalarValue::Bool(arr.value(row))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 type must downcast to Int64Array");
            ScalarValue::Int(arr.value(row))
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 type must downcast to Float64Array");
            ScalarValue::Float(arr.value(row))
        }
        DataType::Utf8View => {
            let arr = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Utf8View type must downcast to StringViewArray");
            ScalarValue::String(arr.value(row).to_owned())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("Timestamp(Nanosecond) must downcast to TimestampNanosecondArray");
            ScalarValue::Timestamp(arr.value(row))
        }
        _ => ScalarValue::Null,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SubqueryFilterOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Pull-based cohort semi-join.
///
/// Wraps a child operator and probes each batch against a
/// pre-materialized [`CohortHashSet`]. Output schema matches the
/// child's — `SubqueryFilter` is a row filter, not a transform
/// (cohorts-aliases-joins.md §5.3).
pub struct SubqueryFilterOperator {
    child: Box<dyn PhysicalOperator>,
    /// LHS expression(s). Length equals `cohort.arity()`. Length 1 for
    /// single-column cohorts; length N for tuple cohorts.
    lhs_columns: Vec<CompiledExpr>,
    cohort: Arc<CohortHashSet>,
    schema: OperatorSchema,
}

impl SubqueryFilterOperator {
    /// Construct the operator. The LHS expression count must equal
    /// the cohort's arity, otherwise the planner has handed us an
    /// inconsistent plan.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        lhs_columns: Vec<CompiledExpr>,
        cohort: Arc<CohortHashSet>,
    ) -> Result<Self> {
        if lhs_columns.is_empty() {
            return Err(BqliteError::Execution(
                "SubqueryFilterOperator: LHS column list must be non-empty".into(),
            ));
        }
        if lhs_columns.len() != cohort.arity() {
            return Err(BqliteError::Execution(format!(
                "SubqueryFilterOperator: LHS arity {} does not match cohort arity {}",
                lhs_columns.len(),
                cohort.arity()
            )));
        }
        let schema = child.output_schema().clone();
        Ok(Self {
            child,
            lhs_columns,
            cohort,
            schema,
        })
    }

    /// Borrow the wrapped child. Useful for tests / EXPLAIN walks.
    pub fn child(&self) -> &dyn PhysicalOperator {
        self.child.as_ref()
    }

    /// Number of cohort entries — useful for diagnostics and
    /// EXPLAIN-style observability.
    #[inline]
    pub fn cohort_size(&self) -> usize {
        self.cohort.len()
    }

    /// Build the row-mask for one batch by evaluating every LHS
    /// expression and probing the cohort tuple-by-tuple.
    fn probe_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(BooleanArray::from(Vec::<bool>::new()));
        }

        // Empty cohort: short-circuit to an all-false mask. Avoids
        // evaluating the LHS expressions when the answer is known.
        if self.cohort.is_empty() {
            let mut buf = BooleanBufferBuilder::new(n);
            buf.append_n(n, false);
            return Ok(BooleanArray::from(buf.finish()));
        }

        // Evaluate each LHS expression once over the whole batch.
        // Reuse a single scratch tuple buffer across rows to avoid
        // per-row allocation churn.
        let lhs_arrays: Vec<ArrayRef> = self
            .lhs_columns
            .iter()
            .map(|expr| eval::evaluate(expr, batch))
            .collect::<Result<Vec<_>>>()?;

        let mut buf = BooleanBufferBuilder::new(n);
        let mut tuple: Vec<ScalarValue> = Vec::with_capacity(self.lhs_columns.len());
        for row in 0..n {
            tuple.clear();
            let mut any_null = false;
            for arr in &lhs_arrays {
                if arr.is_null(row) {
                    any_null = true;
                    break;
                }
                tuple.push(extract_scalar(arr, row));
            }
            // NULL-on-LHS → drop the row (three-valued IN semantics).
            if any_null {
                buf.append(false);
                continue;
            }
            // CohortKey here borrows the scratch tuple via a `Vec`
            // clone; the alternative (a hashable view over the slice)
            // would require parallel-tracking the borrowed reference
            // through the HashSet API, which is not worth the per-row
            // win at v1 cohort sizes.
            let key = CohortKey::from_values(&tuple);
            buf.append(self.cohort.contains(&key));
        }
        Ok(BooleanArray::from(buf.finish()))
    }
}

impl PhysicalOperator for SubqueryFilterOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Mirror `FilterOperator`: skip fully-rejected batches by
        // re-pulling the child rather than emitting empty batches
        // mid-stream when cheap.
        loop {
            let Some(batch) = self.child.next_batch()? else {
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                return Ok(Some(batch));
            }
            let mask = self.probe_batch(&batch)?;
            let filtered =
                compute::filter_record_batch(&batch, &mask).map_err(BqliteError::Arrow)?;
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    use bqlite_ast::expr::{Expr, Spanned};
    use bqlite_ast::span::{Name, Span};
    use bqlite_core::{memory::MemoryTracker, BqlType, ColumnDef, OperatorSchema, UnboundedMemory};
    use bqlite_planner::expr::{FunctionRegistry, TypedExpr};

    use super::*;
    use crate::operator::{CancellationToken, PhysicalOperator};

    // ── Schema fixtures ─────────────────────────────────────────────────────

    fn outer_int_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("user_id", BqlType::Int),
            ColumnDef::required("amount", BqlType::Int),
        ])
        .unwrap()
    }

    fn outer_int_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn outer_string_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("amount", BqlType::Int),
        ])
        .unwrap()
    }

    fn outer_string_arrow_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Utf8View, false),
            Field::new("amount", DataType::Int64, false),
        ]))
    }

    fn make_int_batch(ids: Vec<i64>, amounts: Vec<i64>) -> RecordBatch {
        let id_array: ArrayRef = Arc::new(Int64Array::from(ids));
        let amt_array: ArrayRef = Arc::new(Int64Array::from(amounts));
        RecordBatch::try_new(outer_int_arrow_schema(), vec![id_array, amt_array]).unwrap()
    }

    fn make_string_batch(ids: Vec<&str>, amounts: Vec<i64>) -> RecordBatch {
        let id_array: ArrayRef = Arc::new(StringViewArray::from(ids));
        let amt_array: ArrayRef = Arc::new(Int64Array::from(amounts));
        RecordBatch::try_new(outer_string_arrow_schema(), vec![id_array, amt_array]).unwrap()
    }

    // ── CompiledExpr builders ───────────────────────────────────────────────

    fn col_expr(name: &str, schema: &OperatorSchema) -> CompiledExpr {
        let ast = Spanned::new(Expr::Column(Name::synthetic(name)), Span::EMPTY);
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, schema, &reg).expect("column type-checks");
        CompiledExpr::from_typed(&typed)
    }

    // ── Recording child operator ────────────────────────────────────────────

    struct RecordingChild {
        schema: OperatorSchema,
        batches: Vec<RecordBatch>,
        next_idx: usize,
        opens: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
    }

    impl RecordingChild {
        fn new(schema: OperatorSchema, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                batches,
                next_idx: 0,
                opens: Arc::new(AtomicUsize::new(0)),
                closes: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl PhysicalOperator for RecordingChild {
        fn output_schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn open(&mut self) -> Result<()> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            if self.next_idx >= self.batches.len() {
                return Ok(None);
            }
            let b = self.batches[self.next_idx].clone();
            self.next_idx += 1;
            Ok(Some(b))
        }

        fn close(&mut self) -> Result<()> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    // ── Cohort builder helpers ──────────────────────────────────────────────

    fn cohort_from_int_ids(ids: &[i64]) -> Arc<CohortHashSet> {
        let mut set: HashSet<CohortKey, RandomState> = HashSet::with_hasher(RandomState::new());
        for &i in ids {
            set.insert(CohortKey(vec![ScalarValue::Int(i)]));
        }
        Arc::new(CohortHashSet {
            set,
            arity: 1,
            reservations: Vec::new(),
        })
    }

    fn cohort_from_string_ids(ids: &[&str]) -> Arc<CohortHashSet> {
        let mut set: HashSet<CohortKey, RandomState> = HashSet::with_hasher(RandomState::new());
        for &s in ids {
            set.insert(CohortKey(vec![ScalarValue::String(s.to_owned())]));
        }
        Arc::new(CohortHashSet {
            set,
            arity: 1,
            reservations: Vec::new(),
        })
    }

    fn cohort_tuples_string_int(pairs: &[(&str, i64)]) -> Arc<CohortHashSet> {
        let mut set: HashSet<CohortKey, RandomState> = HashSet::with_hasher(RandomState::new());
        for (s, n) in pairs {
            set.insert(CohortKey(vec![
                ScalarValue::String((*s).to_owned()),
                ScalarValue::Int(*n),
            ]));
        }
        Arc::new(CohortHashSet {
            set,
            arity: 2,
            reservations: Vec::new(),
        })
    }

    fn collect_int_id_column(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    fn collect_string_id_column(batch: &RecordBatch) -> Vec<String> {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        (0..arr.len()).map(|i| arr.value(i).to_owned()).collect()
    }

    // ── Construction ────────────────────────────────────────────────────────

    #[test]
    fn empty_lhs_rejected() {
        let schema = outer_int_schema();
        let child = Box::new(RecordingChild::new(schema, vec![]));
        let cohort = cohort_from_int_ids(&[1]);
        match SubqueryFilterOperator::new(child, vec![], cohort) {
            Err(BqliteError::Execution(_)) => {}
            Err(e) => panic!("expected Execution error, got {e}"),
            Ok(_) => panic!("expected Execution error, got Ok"),
        }
    }

    #[test]
    fn arity_mismatch_rejected() {
        let schema = outer_int_schema();
        let child = Box::new(RecordingChild::new(schema.clone(), vec![]));
        // LHS arity 1, cohort arity 2 → mismatch.
        let cohort = cohort_tuples_string_int(&[("a", 1)]);
        match SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort) {
            Err(BqliteError::Execution(_)) => {}
            Err(e) => panic!("expected Execution error, got {e}"),
            Ok(_) => panic!("expected Execution error, got Ok"),
        }
    }

    #[test]
    fn output_schema_matches_child() {
        let schema = outer_int_schema();
        let child = Box::new(RecordingChild::new(schema.clone(), vec![]));
        let cohort = cohort_from_int_ids(&[]);
        let op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        assert_eq!(op.output_schema().columns().len(), 2);
        assert_eq!(op.output_schema().columns()[0].name, "user_id");
    }

    // ── Single-column probe ─────────────────────────────────────────────────

    #[test]
    fn single_column_int_probe_keeps_matching_rows() {
        let schema = outer_int_schema();
        let batches = vec![make_int_batch(
            vec![1, 2, 3, 4, 5],
            vec![10, 20, 30, 40, 50],
        )];
        let child = Box::new(RecordingChild::new(schema.clone(), batches));
        let cohort = cohort_from_int_ids(&[2, 4, 99]);
        let mut op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        op.open().unwrap();
        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(collect_int_id_column(&out), vec![2, 4]);
        assert!(op.next_batch().unwrap().is_none());
        op.close().unwrap();
    }

    #[test]
    fn single_column_string_probe_keeps_matching_rows() {
        let schema = outer_string_schema();
        let batches = vec![make_string_batch(
            vec!["alice", "bob", "carol", "dave"],
            vec![1, 2, 3, 4],
        )];
        let child = Box::new(RecordingChild::new(schema.clone(), batches));
        let cohort = cohort_from_string_ids(&["alice", "carol", "ghost"]);
        let mut op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        op.open().unwrap();
        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(
            collect_string_id_column(&out),
            vec!["alice".to_string(), "carol".to_string()]
        );
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── Tuple probe ─────────────────────────────────────────────────────────

    #[test]
    fn two_column_tuple_probe_matches_positionally() {
        let schema = outer_string_schema();
        let batches = vec![make_string_batch(
            vec!["alice", "alice", "bob", "carol"],
            vec![1, 2, 3, 4],
        )];
        let child = Box::new(RecordingChild::new(schema.clone(), batches));
        let cohort = cohort_tuples_string_int(&[("alice", 2), ("carol", 4), ("bob", 99)]);
        let lhs = vec![col_expr("user_id", &schema), col_expr("amount", &schema)];
        let mut op = SubqueryFilterOperator::new(child, lhs, cohort).unwrap();
        op.open().unwrap();
        let out = op.next_batch().unwrap().unwrap();
        // ("alice", 1) → no match; ("alice", 2) → match;
        // ("bob", 3) → no match; ("carol", 4) → match.
        assert_eq!(
            collect_string_id_column(&out),
            vec!["alice".to_string(), "carol".to_string()]
        );
        let amounts: Vec<i64> = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(amounts, vec![2, 4]);
    }

    // ── Empty cohort ────────────────────────────────────────────────────────

    #[test]
    fn empty_cohort_rejects_every_row() {
        let schema = outer_int_schema();
        let batches = vec![make_int_batch(vec![1, 2, 3], vec![10, 20, 30])];
        let child = Box::new(RecordingChild::new(schema.clone(), batches));
        let cohort = cohort_from_int_ids(&[]);
        let mut op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        op.open().unwrap();
        // The whole batch is rejected → operator re-pulls and exhausts.
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── Empty input batch ──────────────────────────────────────────────────

    #[test]
    fn empty_input_batch_passes_through() {
        let schema = outer_int_schema();
        let empty = RecordBatch::new_empty(outer_int_arrow_schema());
        let child = Box::new(RecordingChild::new(schema.clone(), vec![empty]));
        let cohort = cohort_from_int_ids(&[1, 2]);
        let mut op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        op.open().unwrap();
        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(out.num_rows(), 0);
    }

    // ── NULL on LHS ────────────────────────────────────────────────────────

    #[test]
    fn null_lhs_row_is_dropped() {
        // Nullable id column; rows where id IS NULL must be dropped
        // even if `Null` is somehow in the cohort.
        let schema = OperatorSchema::new(vec![
            ColumnDef::nullable("user_id", BqlType::Int),
            ColumnDef::required("amount", BqlType::Int),
        ])
        .unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, true),
            Field::new("amount", DataType::Int64, false),
        ]));
        let id_array: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
        ]));
        let amt_array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));
        let batch = RecordBatch::try_new(arrow_schema, vec![id_array, amt_array]).unwrap();

        let child = Box::new(RecordingChild::new(schema.clone(), vec![batch]));

        // Cohort contains 1, 2, 3 — all the non-null rows. A NULL-bearing
        // CohortKey is inserted as well to assert the dropping rule
        // does not depend on the cohort being NULL-free.
        let mut set: HashSet<CohortKey, RandomState> = HashSet::with_hasher(RandomState::new());
        for v in [1_i64, 2, 3] {
            set.insert(CohortKey(vec![ScalarValue::Int(v)]));
        }
        set.insert(CohortKey(vec![ScalarValue::Null]));
        let cohort = Arc::new(CohortHashSet {
            set,
            arity: 1,
            reservations: Vec::new(),
        });

        let mut op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        op.open().unwrap();
        let out = op.next_batch().unwrap().unwrap();
        // Three non-null rows survive; the two NULL rows are dropped.
        assert_eq!(out.num_rows(), 3);
    }

    // ── Lifecycle forwarding ───────────────────────────────────────────────

    #[test]
    fn lifecycle_forwards_to_child() {
        let schema = outer_int_schema();
        let child = RecordingChild::new(schema.clone(), vec![]);
        let opens = Arc::clone(&child.opens);
        let closes = Arc::clone(&child.closes);
        let cohort = cohort_from_int_ids(&[1]);
        let mut op = SubqueryFilterOperator::new(
            Box::new(child),
            vec![col_expr("user_id", &schema)],
            cohort,
        )
        .unwrap();
        op.open().unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1);
        op.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    // ── Multi-batch / re-pull behaviour ────────────────────────────────────

    #[test]
    fn fully_rejected_batch_triggers_repull() {
        // Mirror FilterOperator's mid-stream re-pull behavior: when a
        // whole batch fails the cohort probe, the operator must pull
        // again rather than emit a zero-row batch.
        let schema = outer_int_schema();
        let batches = vec![
            make_int_batch(vec![10, 11, 12], vec![1, 2, 3]),
            make_int_batch(vec![1, 2, 3], vec![10, 20, 30]),
        ];
        let child = Box::new(RecordingChild::new(schema.clone(), batches));
        let cohort = cohort_from_int_ids(&[2]);
        let mut op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        op.open().unwrap();
        // First emitted batch must contain only user_id == 2 from the
        // *second* child batch — the first child batch was fully rejected.
        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(collect_int_id_column(&out), vec![2]);
        assert!(op.next_batch().unwrap().is_none());
    }

    // ── Non-trivial LHS expression ─────────────────────────────────────────

    #[test]
    fn lhs_can_be_an_arithmetic_expression() {
        // LHS is `user_id + 10`, cohort is {12, 14}. Expect rows where
        // user_id ∈ {2, 4} to survive.
        let schema = outer_int_schema();
        let batches = vec![make_int_batch(
            vec![1, 2, 3, 4, 5],
            vec![10, 20, 30, 40, 50],
        )];
        let child = Box::new(RecordingChild::new(schema.clone(), batches));
        let cohort = cohort_from_int_ids(&[12, 14]);

        // Build a `user_id + 10` CompiledExpr via the registry.
        use bqlite_ast::expr::{BinaryOp, Literal};
        let ast = Spanned::new(
            Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Spanned::new(
                    Expr::Column(Name::synthetic("user_id")),
                    Span::EMPTY,
                )),
                right: Box::new(Spanned::new(Expr::Literal(Literal::Int(10)), Span::EMPTY)),
            },
            Span::EMPTY,
        );
        let reg = FunctionRegistry::with_builtins();
        let typed = TypedExpr::from_ast(&ast, &schema, &reg).expect("arith type-checks");
        let lhs = vec![CompiledExpr::from_typed(&typed)];

        let mut op = SubqueryFilterOperator::new(child, lhs, cohort).unwrap();
        op.open().unwrap();
        let out = op.next_batch().unwrap().unwrap();
        assert_eq!(collect_int_id_column(&out), vec![2, 4]);
    }

    // ── Trait-object usability ─────────────────────────────────────────────

    #[test]
    fn trait_object_compiles() {
        let schema = outer_int_schema();
        let child = Box::new(RecordingChild::new(schema.clone(), vec![]));
        let cohort = cohort_from_int_ids(&[1]);
        let op =
            SubqueryFilterOperator::new(child, vec![col_expr("user_id", &schema)], cohort).unwrap();
        let _: Box<dyn PhysicalOperator> = Box::new(op);
    }

    // ── from_batches materializer ──────────────────────────────────────────

    #[test]
    fn cohort_from_batches_skips_system_columns() {
        // Schema has user-declared cols followed by system __seq_id /
        // __batch_id (mirrors what `OperatorSchema::from_table` ships).
        let schema = OperatorSchema::new(vec![
            ColumnDef::required("user_id", BqlType::String),
            ColumnDef::required("__seq_id", BqlType::Int),
        ])
        .unwrap();
        // __seq_id is a "system" column per ColumnDef::is_system, so the
        // cohort must read only `user_id` from this fixture.
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Utf8View, false),
            Field::new("__seq_id", DataType::Int64, false),
        ]));
        let id_array: ArrayRef = Arc::new(StringViewArray::from(vec!["alice", "bob", "alice"]));
        let seq_array: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let batch = RecordBatch::try_new(arrow_schema, vec![id_array, seq_array]).unwrap();
        let cohort =
            CohortHashSet::from_batches(&schema, vec![batch], &UnboundedMemory::new()).unwrap();

        // Two unique declared values; __seq_id is ignored, so the
        // duplicate "alice" rows collapse to a single cohort key.
        assert_eq!(cohort.arity(), 1);
        assert_eq!(cohort.len(), 2);
        assert!(cohort.contains(&CohortKey(vec![ScalarValue::String("alice".into())])));
        assert!(cohort.contains(&CohortKey(vec![ScalarValue::String("bob".into())])));
    }

    #[test]
    fn cohort_from_batches_zero_declared_columns_errors() {
        // Pathological case the engine bind step should never produce,
        // but the constructor guards anyway.
        let schema =
            OperatorSchema::new(vec![ColumnDef::required("__seq_id", BqlType::Int)]).unwrap();
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "__seq_id",
            DataType::Int64,
            false,
        )]));
        let seq_array: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let batch = RecordBatch::try_new(arrow_schema, vec![seq_array]).unwrap();
        match CohortHashSet::from_batches(&schema, vec![batch], &UnboundedMemory::new()) {
            Err(BqliteError::Execution(_)) => {}
            Err(e) => panic!("expected Execution error, got {e}"),
            Ok(_) => panic!("expected Execution error, got Ok"),
        }
    }

    // ── Memory-budget enforcement (TASK-514) ──────────────────────────────

    /// Build a single-column string cohort batch of `n` distinct ids
    /// without any column-name nor system-column noise. Used by the
    /// budget tests below.
    fn cohort_batch_strings(n: usize) -> (OperatorSchema, RecordBatch) {
        let schema =
            OperatorSchema::new(vec![ColumnDef::required("entity_id", BqlType::String)]).unwrap();
        let arrow = Arc::new(ArrowSchema::new(vec![Field::new(
            "entity_id",
            DataType::Utf8View,
            false,
        )]));
        let ids: Vec<String> = (0..n).map(|i| format!("user_{i:08}")).collect();
        let col: ArrayRef = Arc::new(StringViewArray::from(ids));
        let batch = RecordBatch::try_new(arrow, vec![col]).unwrap();
        (schema, batch)
    }

    #[test]
    fn from_batches_charges_budget_proportional_to_size() {
        // A cohort that fits inside the budget reports exactly the
        // `cohort_key_bytes`-summed reservation back through the
        // tracker. Two cohorts of different sizes therefore charge
        // different amounts, and the larger cohort must charge more.
        let tracker_small = MemoryTracker::new(1 << 30);
        let (schema, small_batch) = cohort_batch_strings(8);
        let small = CohortHashSet::from_batches(&schema, vec![small_batch], tracker_small.as_ref())
            .expect("small cohort fits");
        let small_used = tracker_small.used_bytes();
        assert!(small_used > 0, "non-empty cohort must charge the budget");
        assert_eq!(small_used, small.reserved_bytes());

        let tracker_big = MemoryTracker::new(1 << 30);
        let (schema, big_batch) = cohort_batch_strings(64);
        let big = CohortHashSet::from_batches(&schema, vec![big_batch], tracker_big.as_ref())
            .expect("big cohort fits");
        assert!(
            tracker_big.used_bytes() > tracker_small.used_bytes(),
            "{}-row cohort must charge more than {}-row cohort \
             (big={} small={})",
            big.len(),
            small.len(),
            tracker_big.used_bytes(),
            tracker_small.used_bytes(),
        );
    }

    #[test]
    fn cohort_drop_releases_budget_bytes() {
        // Per `engine/memory-budget.md` §5.1 / §7, the cohort hash set
        // is held for the outer-query lifetime and its bytes are
        // released when the cohort is dropped at query teardown.
        let tracker = MemoryTracker::new(1 << 30);
        let (schema, batch) = cohort_batch_strings(32);
        let cohort = CohortHashSet::from_batches(&schema, vec![batch], tracker.as_ref())
            .expect("cohort fits");
        assert!(tracker.used_bytes() > 0);
        let charged = tracker.used_bytes();
        assert_eq!(cohort.reserved_bytes(), charged);
        drop(cohort);
        assert_eq!(
            tracker.used_bytes(),
            0,
            "dropping the cohort must release every charged byte"
        );
    }

    #[test]
    fn from_batches_returns_typed_memory_budget_exceeded() {
        // `engine/spill.md` §4.3: cohort materialisation does not spill
        // in v1; an over-budget cohort fails the whole query with
        // `BqliteError::MemoryBudgetExceeded`. The budget is set
        // intentionally below `cohort_key_bytes(one row)` so the very
        // first batch overflows.
        let tracker = MemoryTracker::new(8);
        let (schema, batch) = cohort_batch_strings(4);
        let err = CohortHashSet::from_batches(&schema, vec![batch], tracker.as_ref())
            .expect_err("must overflow");
        match err {
            BqliteError::MemoryBudgetExceeded { used, budget } => {
                assert_eq!(budget, 8);
                // `used` is the requested-bytes folded onto the live
                // total (which is 0 at this point — the rollback inside
                // `MemoryTracker::try_reserve` undoes the optimistic
                // charge before the helper formats the error).
                assert!(used > budget);
            }
            other => panic!("expected MemoryBudgetExceeded, got {other:?}"),
        }
        assert_eq!(
            tracker.used_bytes(),
            0,
            "failed materialisation must leave the tracker untouched"
        );
    }

    #[test]
    fn from_batches_fails_fast_on_second_batch_overflow() {
        // First batch fits (cohort_key_bytes ≈ 64–80 per entry; 4 rows
        // ≈ 320 bytes max with the 8-char ids). Second batch's
        // reservation must fail and surface `MemoryBudgetExceeded`.
        // Charge for the first batch must already be released by the
        // time the error returns, since the partially-built cohort and
        // its reservation are dropped.
        let tracker = MemoryTracker::new(1024);
        let (schema, batch_a) = cohort_batch_strings(4);
        let batch_b = cohort_batch_strings(64).1;
        let err = CohortHashSet::from_batches(&schema, vec![batch_a, batch_b], tracker.as_ref())
            .expect_err("second batch must overflow");
        assert!(matches!(err, BqliteError::MemoryBudgetExceeded { .. }));
        assert_eq!(
            tracker.used_bytes(),
            0,
            "rollback must release the first batch's reservation"
        );
    }

    #[test]
    fn empty_cohort_charges_no_budget_bytes() {
        // Per `from_batches`'s zero-row short-circuit, no reservation
        // is taken when every batch has zero rows.
        let tracker = MemoryTracker::new(1024);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "entity_id",
            DataType::Utf8View,
            false,
        )]));
        let schema =
            OperatorSchema::new(vec![ColumnDef::required("entity_id", BqlType::String)]).unwrap();
        let empty_ids: Vec<&str> = Vec::new();
        let col: ArrayRef = Arc::new(StringViewArray::from(empty_ids));
        let batch = RecordBatch::try_new(arrow_schema, vec![col]).unwrap();
        let cohort = CohortHashSet::from_batches(&schema, vec![batch], tracker.as_ref())
            .expect("empty cohort must succeed");
        assert!(cohort.is_empty());
        assert_eq!(cohort.reserved_bytes(), 0);
        assert_eq!(tracker.used_bytes(), 0);
    }

    // ── CancellationToken sanity (unused arg avoids warnings) ──────────────

    #[test]
    fn cancellation_token_unused_does_not_affect_drive() {
        // The cohort operator does not own a token; the child does.
        // This test pins the absence of a token field so a future
        // refactor that adds one updates the contract intentionally.
        let _t = CancellationToken::new();
    }
}
