//! `AttributeOperator` — flat-row `ATTRIBUTE(...)` implementation (TASK-431, Wave 4).
//!
//! Implements the per-entity stateful operator defined by
//! [`docs/design/operators/attribute.md`](../../../docs/design/operators/attribute.md).
//!
//! For each entity the operator maintains a sliding-window deque of
//! touchpoint events. When a conversion event arrives it walks the deque
//! collecting qualifying touchpoints, emits one flat row per qualifier
//! (or a single LEFT-UNNEST row when no touchpoint qualifies), then
//! optionally adds itself to the deque if the event type is also in the
//! touchpoints list. The three-way row shape (normal, null-key, no-touchpoint)
//! is preserved per §4.1 of the design note.
//!
//! The operator is constructed from an [`AttributePhysical`] descriptor
//! produced by the planner (TASK-424) and consumed by the engine bind
//! step (TASK-438). In v1 it only emits flat rows — fused aggregate
//! shapes are the Wave 5 target per design §13.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringViewArray, StringViewBuilder, TimestampNanosecondArray,
    TimestampNanosecondBuilder,
};
use arrow::record_batch::RecordBatch;
use compact_str::CompactString;

use bqlite_core::{BqlType, EntityId, OperatorSchema};
use bqlite_planner::compiled::{CompiledExpr, CompiledNode};
use bqlite_planner::demand::{ColumnId, CompiledFusableAggregate, DemandCapabilities};
use bqlite_planner::physical::AttributePhysical;

use crate::eval;
use crate::operator::EntityOperator;

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

/// Per-entity touchpoint deque cap (design §9). Pathological entities with
/// more than this many touchpoints flush the current in-flight conversion
/// and skip the rest of the entity, emitting a diagnostic. Spill-to-disk
/// is a Wave 5 target (TASK-502).
pub const DEFAULT_ATTRIBUTE_DEQUE_CAP: usize = 1_000_000;

/// Operator-name tag used in the per-query entity-cap diagnostic channel.
pub const ATTRIBUTE_OP_NAME: &str = "ATTRIBUTE";

// ─────────────────────────────────────────────────────────────────────────────
// Per-query diagnostic record
// ─────────────────────────────────────────────────────────────────────────────

/// Per-query diagnostic emitted when an entity exceeds the per-entity
/// deque cap. Shape per design §9 — identical to SESSIONIZE's and the
/// existing per-entity-event-limit convention.
///
/// The diagnostic channel itself is owned by the engine (TASK-428 /
/// TASK-438); this struct captures the shape the operator uses to
/// populate the channel. Until the engine-side channel lands, callers
/// can read the diagnostics accumulated in per-entity state via
/// [`AttributeState::take_diagnostic`].
#[derive(Debug, Clone, PartialEq)]
pub struct EntityCapDiagnostic {
    /// Canonical representation of the entity that hit the cap.
    pub entity_id: EntityId,
    /// Number of events seen for that entity before the cap fired.
    pub event_count: u64,
    /// Operator name — always [`ATTRIBUTE_OP_NAME`] for this operator.
    pub operator: &'static str,
    /// Active cap value at the time the diagnostic fired.
    pub cap: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// AttributeOperator
// ─────────────────────────────────────────────────────────────────────────────

/// Physical `EntityOperator` implementing `ATTRIBUTE(...)`.
///
/// The operator is immutable (`&self`); all mutable state lives in
/// [`AttributeState`] created fresh per entity by
/// [`EntityOperator::create_state`]. See the design note for the full
/// contract.
///
/// Currently advertises `supports_forwarded_columns: true` only (matches
/// `AttributePhysical::DEMAND_CAPS`). Fused aggregate shapes are a Wave 5
/// target (§13); the descriptor still carries `fused_aggregate`, but in
/// v1 the operator rejects a non-`None` fusion at construction time.
pub struct AttributeOperator {
    /// Event types that trigger conversion emission. Stored as a hash set
    /// for O(1) membership checks on the hot path.
    conversion_events: HashSet<String>,
    /// Event types eligible as touchpoints.
    touchpoint_events: HashSet<String>,
    /// Lookback window in nanoseconds.
    window_ns: i64,
    /// Compiled expression evaluated per touchpoint row; its result
    /// becomes the `touchpoint_key` output column. Must type-check to
    /// `String` (§11).
    touchpoint_key: CompiledExpr,
    /// Forwarded conversion property names in the order they appear in
    /// the output schema.
    forwarded_conversion_columns: Vec<ColumnId>,
    /// Output schema as produced by the planner (design §4).
    output_schema: OperatorSchema,
    /// Required input columns: entity_id, ts, event_type, forwarded
    /// columns, and any column referenced by `touchpoint_key`.
    required_column_names: Vec<String>,
    /// Original query time range for scan-extension-aware emission
    /// filtering (§12). `None` when no outer time range is present.
    conversion_range: Option<(i64, i64)>,
    /// Per-entity touchpoint deque cap (§9).
    deque_cap: usize,
    /// Output entity_id type cached for fast state construction — driven
    /// by the `entity_id` column type in `output_schema`.
    entity_id_type: BqlType,
    /// BqlType per forwarded conversion column, cached from the output
    /// schema so builders can be materialized in one pass.
    forwarded_types: Vec<BqlType>,
    /// Fused aggregate descriptor when stateful-to-aggregate fusion is
    /// active (TASK-520). When `Some`, per-entity rows continue to be
    /// emitted in the operator's native flat-row schema; the engine
    /// adapter feeds them into a `HashAccumulator::update_batch` call.
    fused_aggregate: Option<CompiledFusableAggregate>,
}

impl std::fmt::Debug for AttributeOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttributeOperator")
            .field("conversion_events", &self.conversion_events)
            .field("touchpoint_events", &self.touchpoint_events)
            .field("window_ns", &self.window_ns)
            .field(
                "forwarded_conversion_columns",
                &self.forwarded_conversion_columns,
            )
            .field("conversion_range", &self.conversion_range)
            .field("deque_cap", &self.deque_cap)
            .finish()
    }
}

impl AttributeOperator {
    /// Construct from a physical plan descriptor produced by TASK-425.
    ///
    /// Returns an error if any invariant asserted by design §3/§13 is
    /// violated:
    ///
    /// * Empty `conversion_events` or `touchpoint_events` list.
    /// * `touchpoint_key` expression with a non-`String` result type.
    /// * Forwarded column with an unsupported type (the in-memory row
    ///   buffer supports Bool/Int/Float/String/Timestamp — List/Map are
    ///   deferred).
    pub fn from_physical(desc: &AttributePhysical) -> bqlite_core::Result<Self> {
        if desc.conversion_events.is_empty() {
            return Err(bqlite_core::BqliteError::Execution(
                "AttributeOperator requires at least one conversion event type".into(),
            ));
        }
        if desc.touchpoint_events.is_empty() {
            return Err(bqlite_core::BqliteError::Execution(
                "AttributeOperator requires at least one touchpoint event type".into(),
            ));
        }
        if desc.touchpoint_key.result_type != BqlType::String {
            return Err(bqlite_core::BqliteError::Execution(format!(
                "AttributeOperator touchpoint_key must resolve to String, got {:?}",
                desc.touchpoint_key.result_type
            )));
        }
        if desc.window_ns < 0 {
            return Err(bqlite_core::BqliteError::Execution(format!(
                "AttributeOperator window_ns must be >= 0, got {}",
                desc.window_ns
            )));
        }
        // TASK-520: stateful-to-aggregate fusion is supported. The
        // descriptor's `output_schema` may have been replaced by the
        // planner with the aggregate's output; per-entity rows are still
        // emitted in the native (pre-fusion) schema below.
        let native_output_schema = desc.native_output_schema().clone();

        // Resolve forwarded column BqlTypes against the native output
        // schema. The aggregate output schema (when fused) does not
        // necessarily contain forwarded columns; the native one always
        // does.
        let mut forwarded_types = Vec::with_capacity(desc.forwarded_conversion_columns.len());
        for name in &desc.forwarded_conversion_columns {
            let (_, col) = native_output_schema.column(name).ok_or_else(|| {
                bqlite_core::BqliteError::Schema(format!(
                    "AttributeOperator forwarded column `{name}` missing from native output schema"
                ))
            })?;
            assert_supported_forwarded_type(&col.bql_type, name)?;
            forwarded_types.push(col.bql_type.clone());
        }

        // Resolve entity_id output column type (String or Int) from the
        // native schema — the aggregate schema may have stripped or
        // renamed it.
        let entity_id_type = native_output_schema
            .column("entity_id")
            .ok_or_else(|| {
                bqlite_core::BqliteError::Schema(
                    "AttributeOperator native output schema is missing `entity_id`".into(),
                )
            })?
            .1
            .bql_type
            .clone();
        match entity_id_type {
            BqlType::String | BqlType::Int => {}
            ref other => {
                return Err(bqlite_core::BqliteError::Schema(format!(
                    "AttributeOperator `entity_id` column must be String or Int, got {other:?}"
                )))
            }
        }

        // Build the input-column demand set. The matcher walks the same
        // columns (entity_id, ts, event_type); forwarded columns plus
        // columns referenced by `touchpoint_key` round out the set.
        let mut required: Vec<String> = vec![
            "entity_id".to_string(),
            "ts".to_string(),
            "event_type".to_string(),
        ];
        for name in &desc.forwarded_conversion_columns {
            if !required.contains(name) {
                required.push(name.clone());
            }
        }
        collect_expr_columns(&desc.touchpoint_key, &mut required);

        Ok(Self {
            conversion_events: desc.conversion_events.iter().cloned().collect(),
            touchpoint_events: desc.touchpoint_events.iter().cloned().collect(),
            window_ns: desc.window_ns,
            touchpoint_key: desc.touchpoint_key.clone(),
            forwarded_conversion_columns: desc.forwarded_conversion_columns.clone(),
            output_schema: native_output_schema,
            required_column_names: required,
            conversion_range: desc.conversion_range,
            deque_cap: DEFAULT_ATTRIBUTE_DEQUE_CAP,
            entity_id_type,
            forwarded_types,
            fused_aggregate: desc.fused_aggregate.clone(),
        })
    }

    /// Returns `true` when stateful-to-aggregate fusion is active.
    pub fn is_fused(&self) -> bool {
        self.fused_aggregate.is_some()
    }

    /// Access the fused aggregate descriptor when fusion is active.
    pub fn fused_aggregate(&self) -> Option<&CompiledFusableAggregate> {
        self.fused_aggregate.as_ref()
    }

    /// Override the default per-entity deque cap. Intended for tests that
    /// want to exercise the cap path without materializing a million
    /// events; production code should leave the default alone.
    pub fn with_deque_cap(mut self, cap: usize) -> Self {
        self.deque_cap = cap.max(1);
        self
    }

    /// Effective per-entity deque cap — visible for tests.
    pub fn deque_cap(&self) -> usize {
        self.deque_cap
    }
}

fn assert_supported_forwarded_type(ty: &BqlType, name: &str) -> bqlite_core::Result<()> {
    match ty {
        BqlType::Bool | BqlType::Int | BqlType::Float | BqlType::String | BqlType::Timestamp => {
            Ok(())
        }
        other => Err(bqlite_core::BqliteError::Execution(format!(
            "AttributeOperator forwarded column `{name}` has unsupported type {other:?} \
             (only scalar types are supported in v1)"
        ))),
    }
}

/// Walk a compiled expression, pushing each referenced column name into
/// `out` (skipping duplicates). Used for `required_columns()` reporting
/// so upstream projection pruning can drop everything else.
fn collect_expr_columns(expr: &CompiledExpr, out: &mut Vec<String>) {
    match &expr.node {
        CompiledNode::Column { name, .. } => {
            if !out.contains(name) {
                out.push(name.clone());
            }
        }
        CompiledNode::Literal(_) => {}
        CompiledNode::Arith { left, right, .. } | CompiledNode::Compare { left, right, .. } => {
            collect_expr_columns(left, out);
            collect_expr_columns(right, out);
        }
        CompiledNode::Unary { operand, .. } => collect_expr_columns(operand, out),
        CompiledNode::Not(inner) => collect_expr_columns(inner, out),
        CompiledNode::Cast { input, .. } | CompiledNode::ImplicitCoerce { input, .. } => {
            collect_expr_columns(input, out)
        }
        CompiledNode::InLiteralSet { input, .. } => collect_expr_columns(input, out),
        CompiledNode::IsNull { input, .. } => collect_expr_columns(input, out),
        CompiledNode::And { operands, .. } | CompiledNode::Or { operands, .. } => {
            for op in operands {
                collect_expr_columns(op, out);
            }
        }
        CompiledNode::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_columns(arg, out);
            }
        }
        // `CompiledNode` is `#[non_exhaustive]`. A new variant that
        // holds a `CompiledExpr` child would silently drop its column
        // references here, which causes the scan to skip those columns
        // and the operator to panic at runtime in `column_as`. The
        // debug_assert makes the omission loud in test/ci so the walker
        // gets updated alongside the new variant; release builds stay
        // conservative (no-op) rather than crashing the query.
        other => {
            debug_assert!(
                false,
                "collect_expr_columns: unhandled CompiledNode variant {other:?} — update the walker"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-entity state
// ─────────────────────────────────────────────────────────────────────────────

/// One sliding-window touchpoint entry per design §8.1.
///
/// The key is stored as [`CompactString`] per the operator-side
/// guidance in [`CLAUDE.md`] and the TASK-454 CompactString sweep — most
/// real touchpoint keys (channel, campaign tags, `CONCAT(...)` outputs
/// under 24 bytes) fit inline and skip the heap entirely. See the
/// design note's §8.1 "minimal per-entry state" rationale.
///
/// [`CLAUDE.md`]: ../../../CLAUDE.md
#[derive(Debug, Clone)]
struct TouchpointDequeEntry {
    ts: i64,
    /// `None` when `touchpoint_key` evaluated to NULL — see §4.1 row
    /// shape 2.
    key: Option<CompactString>,
}

/// Per-entity mutable state. Persists across sub-batches; dropped by
/// [`EntityOperator::finish_entity`] after the entity's events are drained.
pub struct AttributeState {
    entity_id: EntityId,
    deque: VecDeque<TouchpointDequeEntry>,
    builders: OutputBuilders,
    /// Total events accepted into the deque so far for this entity
    /// (excluding overflow events). Drives the §9 cap check.
    touchpoints_seen: u64,
    /// Total events observed (conversion + touchpoint) — reported in
    /// the cap diagnostic so users know how far the entity got before
    /// truncation.
    events_seen: u64,
    /// Once true, further `process_sub_batch` calls are no-ops until the
    /// entity boundary.
    capped: bool,
    /// Diagnostic captured when the cap fires. Callers pick it up via
    /// [`AttributeState::take_diagnostic`].
    diagnostic: Option<EntityCapDiagnostic>,
}

impl AttributeState {
    /// Take the entity-cap diagnostic (if any). Intended for the engine
    /// bind step's per-query diagnostic channel — TASK-428 plumbing.
    ///
    /// **Callers must drain the diagnostic before
    /// [`EntityOperator::finish_entity`] consumes the state**, otherwise
    /// it is silently lost. Once TASK-438 lands the shared per-query
    /// channel, the `EntityOperatorAdapter` will drain diagnostics
    /// centrally so individual call sites no longer need to track this.
    pub fn take_diagnostic(&mut self) -> Option<EntityCapDiagnostic> {
        self.diagnostic.take()
    }

    /// Observe (for tests) whether the cap has fired without consuming
    /// the diagnostic.
    pub fn is_capped(&self) -> bool {
        self.capped
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Output row buffer
// ─────────────────────────────────────────────────────────────────────────────

/// Column-oriented row buffer fed by conversion emission. One builder per
/// output column; `finish_into_batch` produces the entity-scoped output
/// `RecordBatch`.
struct OutputBuilders {
    entity_id: EntityIdBuilder,
    conversion_ts: TimestampNanosecondBuilder,
    forwarded: Vec<ForwardedBuilder>,
    touchpoint_ts: TimestampNanosecondBuilder,
    touchpoint_key: StringViewBuilder,
    rows: usize,
}

impl OutputBuilders {
    fn new(entity_id_type: &BqlType, forwarded_types: &[BqlType]) -> Self {
        Self {
            entity_id: EntityIdBuilder::new(entity_id_type),
            // Timestamp columns always carry the UTC timezone per the
            // core canonical Arrow mapping (see bqlite_core::arrow).
            conversion_ts: TimestampNanosecondBuilder::new().with_timezone("UTC"),
            forwarded: forwarded_types.iter().map(ForwardedBuilder::new).collect(),
            touchpoint_ts: TimestampNanosecondBuilder::new().with_timezone("UTC"),
            touchpoint_key: StringViewBuilder::new(),
            rows: 0,
        }
    }

    fn append_row(
        &mut self,
        entity: &EntityId,
        conversion_ts: i64,
        forwarded_src: &[&dyn Array],
        forwarded_row: usize,
        touchpoint_ts: Option<i64>,
        touchpoint_key: Option<&str>,
    ) {
        self.entity_id.append(entity);
        self.conversion_ts.append_value(conversion_ts);
        for (builder, src) in self.forwarded.iter_mut().zip(forwarded_src.iter()) {
            builder.append_from_array(*src, forwarded_row);
        }
        match touchpoint_ts {
            Some(ts) => self.touchpoint_ts.append_value(ts),
            None => self.touchpoint_ts.append_null(),
        }
        match touchpoint_key {
            Some(k) => self.touchpoint_key.append_value(k),
            None => self.touchpoint_key.append_null(),
        }
        self.rows += 1;
    }

    fn finish_into_batch(self, output_schema: &OperatorSchema) -> RecordBatch {
        let Self {
            entity_id,
            mut conversion_ts,
            forwarded,
            mut touchpoint_ts,
            mut touchpoint_key,
            rows: _,
        } = self;
        // Assemble columns in the output_schema's declared order so we
        // don't silently shift columns if the planner ever rearranges
        // forwarded_columns relative to entity_id / conversion_ts /
        // touchpoint_ts / touchpoint_key.
        let mut forwarded_iter = forwarded.into_iter();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.columns().len());
        let mut entity_id_out: Option<EntityIdBuilder> = Some(entity_id);
        for col in output_schema.columns() {
            let array: ArrayRef = match col.name.as_str() {
                "entity_id" => entity_id_out
                    .take()
                    .expect("entity_id column emitted exactly once")
                    .finish(),
                "conversion_ts" => Arc::new(conversion_ts.finish()) as ArrayRef,
                "touchpoint_ts" => Arc::new(touchpoint_ts.finish()) as ArrayRef,
                "touchpoint_key" => Arc::new(touchpoint_key.finish()) as ArrayRef,
                _ => forwarded_iter
                    .next()
                    .expect("forwarded builder count matches schema")
                    .finish(),
            };
            columns.push(array);
        }
        debug_assert!(
            forwarded_iter.next().is_none(),
            "forwarded builder count did not match schema"
        );

        RecordBatch::try_new(Arc::new(output_schema.to_arrow_schema()), columns)
            .unwrap_or_else(|e| panic!("AttributeOperator failed to build output batch: {e}"))
    }
}

enum EntityIdBuilder {
    String(StringViewBuilder),
    Int(Int64Builder),
}

impl EntityIdBuilder {
    fn new(ty: &BqlType) -> Self {
        match ty {
            BqlType::String => EntityIdBuilder::String(StringViewBuilder::new()),
            BqlType::Int => EntityIdBuilder::Int(Int64Builder::new()),
            _ => panic!("entity_id output column must be String or Int"),
        }
    }

    fn append(&mut self, entity: &EntityId) {
        // `from_physical` validates that the output schema's `entity_id`
        // type matches one of String/Int, and the `EntityOperatorAdapter`
        // guarantees a single-typed entity key per query — so the
        // cross-type arms below are unreachable in a correctly-wired
        // engine. Panic rather than silently stringifying or clamping,
        // which would be a data-corruption path if wiring is ever wrong.
        match (self, entity) {
            (EntityIdBuilder::String(b), EntityId::String(s)) => b.append_value(s),
            (EntityIdBuilder::Int(b), EntityId::Int(i)) => b.append_value(*i),
            (EntityIdBuilder::String(_), EntityId::Int(_)) => panic!(
                "AttributeOperator: output schema declares entity_id=String but state holds Int — \
                 engine bind-step mismatch"
            ),
            (EntityIdBuilder::Int(_), EntityId::String(_)) => panic!(
                "AttributeOperator: output schema declares entity_id=Int but state holds String — \
                 engine bind-step mismatch"
            ),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            EntityIdBuilder::String(mut b) => Arc::new(b.finish()) as ArrayRef,
            EntityIdBuilder::Int(mut b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

/// Type-dispatched forwarded-column row buffer.
enum ForwardedBuilder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    String(StringViewBuilder),
    Timestamp(TimestampNanosecondBuilder),
}

impl ForwardedBuilder {
    fn new(ty: &BqlType) -> Self {
        match ty {
            BqlType::Bool => ForwardedBuilder::Bool(BooleanBuilder::new()),
            BqlType::Int => ForwardedBuilder::Int(Int64Builder::new()),
            BqlType::Float => ForwardedBuilder::Float(Float64Builder::new()),
            BqlType::String => ForwardedBuilder::String(StringViewBuilder::new()),
            BqlType::Timestamp => {
                ForwardedBuilder::Timestamp(TimestampNanosecondBuilder::new().with_timezone("UTC"))
            }
            _ => unreachable!("assert_supported_forwarded_type rejects List/Map at construction"),
        }
    }

    fn append_from_array(&mut self, src: &dyn Array, row: usize) {
        if src.is_null(row) {
            self.append_null();
            return;
        }
        match self {
            ForwardedBuilder::Bool(b) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("Bool forwarded column expects BooleanArray");
                b.append_value(arr.value(row));
            }
            ForwardedBuilder::Int(b) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int forwarded column expects Int64Array");
                b.append_value(arr.value(row));
            }
            ForwardedBuilder::Float(b) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("Float forwarded column expects Float64Array");
                b.append_value(arr.value(row));
            }
            ForwardedBuilder::String(b) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .expect("String forwarded column expects StringViewArray");
                b.append_value(arr.value(row));
            }
            ForwardedBuilder::Timestamp(b) => {
                let arr = src
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .expect("Timestamp forwarded column expects TimestampNanosecondArray");
                b.append_value(arr.value(row));
            }
        }
    }

    fn append_null(&mut self) {
        match self {
            ForwardedBuilder::Bool(b) => b.append_null(),
            ForwardedBuilder::Int(b) => b.append_null(),
            ForwardedBuilder::Float(b) => b.append_null(),
            ForwardedBuilder::String(b) => b.append_null(),
            ForwardedBuilder::Timestamp(b) => b.append_null(),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ForwardedBuilder::Bool(mut b) => Arc::new(b.finish()) as ArrayRef,
            ForwardedBuilder::Int(mut b) => Arc::new(b.finish()) as ArrayRef,
            ForwardedBuilder::Float(mut b) => Arc::new(b.finish()) as ArrayRef,
            ForwardedBuilder::String(mut b) => Arc::new(b.finish()) as ArrayRef,
            ForwardedBuilder::Timestamp(mut b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityOperator implementation
// ─────────────────────────────────────────────────────────────────────────────

impl EntityOperator for AttributeOperator {
    type State = AttributeState;

    fn create_state(&self, entity_id: &EntityId) -> Self::State {
        AttributeState {
            entity_id: entity_id.clone(),
            deque: VecDeque::new(),
            builders: OutputBuilders::new(&self.entity_id_type, &self.forwarded_types),
            touchpoints_seen: 0,
            events_seen: 0,
            capped: false,
            diagnostic: None,
        }
    }

    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn process_sub_batch(&self, state: &mut Self::State, batch: &RecordBatch) {
        if state.capped {
            return;
        }
        if batch.num_rows() == 0 {
            return;
        }

        // Resolve column arrays once per sub-batch. Missing required
        // columns surface as a debug-mode panic; a production engine
        // never wires an AttributeOperator without the adapter providing
        // entity_id/ts/event_type.
        let ts_col = column_as::<TimestampNanosecondArray>(batch, "ts");
        let ev_col = column_as::<StringViewArray>(batch, "event_type");

        // Evaluate the touchpoint_key expression once per sub-batch —
        // the whole batch is a single entity, so per-row evaluation
        // inside the hot loop would be strictly wasteful (§11).
        let key_array = eval::evaluate(&self.touchpoint_key, batch)
            .expect("touchpoint_key expression evaluation failed — planner should have rejected");
        let key_view = key_array
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("touchpoint_key must evaluate to StringViewArray (typed to String)");

        // Resolve forwarded-column source arrays once per sub-batch.
        // They are dispatched into per-type builders at append time.
        let forwarded_src: Vec<&dyn Array> = self
            .forwarded_conversion_columns
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap_or_else(|| {
                        panic!(
                            "AttributeOperator forwarded column `{name}` missing from input batch"
                        )
                    })
                    .as_ref()
            })
            .collect();

        for row in 0..batch.num_rows() {
            if state.capped {
                break;
            }
            state.events_seen = state.events_seen.saturating_add(1);

            let ts = ts_col.value(row);
            let event_type = ev_col.value(row);
            let is_conversion = self.conversion_events.contains(event_type);
            let is_touchpoint = self.touchpoint_events.contains(event_type);

            // §6.1 — prune deque entries that can no longer qualify for
            // any future conversion. Events arrive in ascending ts, so
            // `entry.ts < event.ts - window` is the correct gate.
            if self.window_ns > 0 {
                let cutoff = ts.saturating_sub(self.window_ns);
                while let Some(front) = state.deque.front() {
                    if front.ts < cutoff {
                        state.deque.pop_front();
                    } else {
                        break;
                    }
                }
            } else {
                // §16.1 `window: 0s`: strict-at-conversion rule already
                // guarantees no touchpoint qualifies — keep the deque
                // drained so memory doesn't accumulate.
                state.deque.clear();
            }

            // §6.2 — conversion emission. Fires before the self-add step
            // so a shared event_type doesn't attribute to itself.
            if is_conversion && self.is_in_conversion_range(ts) {
                self.emit_conversion(state, ts, row, &forwarded_src);
            }

            // §6.3 — touchpoint add. Strict-at-conversion rule means a
            // touchpoint at the exact conversion `ts` never qualifies
            // against the current event (§5.1); the add still happens so
            // a *later* conversion can see it.
            if is_touchpoint {
                if state.deque.len() >= self.deque_cap {
                    // §9 — flush already-emitted rows for the in-flight
                    // conversion (they are persisted in `state.builders`
                    // already), drop any state carrying forward work,
                    // record the diagnostic, and skip the remainder of
                    // the entity.
                    state.capped = true;
                    state.diagnostic = Some(EntityCapDiagnostic {
                        entity_id: state.entity_id.clone(),
                        event_count: state.events_seen,
                        operator: ATTRIBUTE_OP_NAME,
                        cap: self.deque_cap as u64,
                    });
                    state.deque.clear();
                    break;
                }
                let key = if key_view.is_null(row) {
                    None
                } else {
                    // CompactString::from on a `&str` stores inline when
                    // `len <= 24` — covers typical channel / campaign
                    // tags and short CONCAT(...) outputs without
                    // touching the heap.
                    Some(CompactString::from(key_view.value(row)))
                };
                state.deque.push_back(TouchpointDequeEntry { ts, key });
                state.touchpoints_seen = state.touchpoints_seen.saturating_add(1);
            }
        }
    }

    fn finish_entity(&self, state: Self::State) -> Option<RecordBatch> {
        if state.builders.rows == 0 {
            return None;
        }
        Some(state.builders.finish_into_batch(&self.output_schema))
    }

    fn required_columns(&self) -> &[String] {
        &self.required_column_names
    }

    fn supported_demands(&self) -> DemandCapabilities {
        // Always defers to the planner-side `DEMAND_CAPS` constant. After
        // TASK-520 this includes `supports_aggregation_fusion: true`.
        AttributePhysical::DEMAND_CAPS
    }

    fn take_pending_warnings(
        &self,
        state: &mut Self::State,
        _entity_id: &EntityId,
    ) -> Vec<bqlite_core::QueryWarning> {
        match state.take_diagnostic() {
            Some(diag) => vec![bqlite_core::QueryWarning::AttributeTouchpointCapExceeded {
                entity_id: diag.entity_id.to_string(),
                touchpoint_count: diag.event_count,
                cap: diag.cap,
            }],
            None => Vec::new(),
        }
    }
}

impl AttributeOperator {
    /// True when the conversion's `ts` falls within the planner's original
    /// query range — or when no range was threaded (unbounded scan). See
    /// §12 for the scan-extension rationale.
    fn is_in_conversion_range(&self, ts: i64) -> bool {
        match self.conversion_range {
            None => true,
            Some((start, end)) => ts >= start && ts < end,
        }
    }

    /// Emit the flat rows produced by a single conversion event. Walks
    /// the deque in ascending-ts order (FIFO) and emits either one row
    /// per qualifier or exactly one LEFT-UNNEST row.
    fn emit_conversion(
        &self,
        state: &mut AttributeState,
        conversion_ts: i64,
        row: usize,
        forwarded_src: &[&dyn Array],
    ) {
        let window = self.window_ns;
        let lookback_edge = conversion_ts.saturating_sub(window);

        let mut matched = 0usize;
        for entry in state.deque.iter() {
            // §5.1 — inclusive at lookback, strict at conversion.
            if entry.ts >= lookback_edge && entry.ts < conversion_ts {
                state.builders.append_row(
                    &state.entity_id,
                    conversion_ts,
                    forwarded_src,
                    row,
                    Some(entry.ts),
                    entry.key.as_deref(),
                );
                matched += 1;
            }
        }
        if matched == 0 {
            // §4.1 row shape 3 — LEFT-UNNEST row for un-attributed
            // conversions. Guarantees every in-range conversion yields
            // at least one output row (§17.2 cardinality invariant).
            state.builders.append_row(
                &state.entity_id,
                conversion_ts,
                forwarded_src,
                row,
                None,
                None,
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

fn column_as<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    let col = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("AttributeOperator input batch missing column `{name}`"));
    col.as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("AttributeOperator column `{name}` has unexpected Arrow type"))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, StringViewArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    use bqlite_ast::Span;
    use bqlite_core::{BqlType, ColumnDef, EntityId, OperatorSchema};
    use bqlite_planner::compiled::CompiledExpr;
    use bqlite_planner::expr::{TypedExpr, TypedExprKind};
    use bqlite_planner::physical::AttributePhysical;
    use bqlite_planner::PhysicalPlan;

    // ── Fixtures ─────────────────────────────────────────────────────────

    fn events_input_schema() -> OperatorSchema {
        OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("channel", BqlType::String),
            ColumnDef::nullable("amount", BqlType::Int),
        ])
        .unwrap()
    }

    fn input_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Utf8View, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("channel", DataType::Utf8View, true),
            Field::new("amount", DataType::Int64, true),
        ])
    }

    fn basic_output_schema(forwarded: &[(&str, BqlType)]) -> OperatorSchema {
        let mut cols = vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
        ];
        for (name, ty) in forwarded {
            cols.push(ColumnDef::nullable(*name, ty.clone()));
        }
        cols.push(ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp));
        cols.push(ColumnDef::nullable("touchpoint_key", BqlType::String));
        OperatorSchema::new(cols).unwrap()
    }

    fn channel_key_expr() -> CompiledExpr {
        // Column reference to `channel` (index 3 in the input schema).
        let typed = TypedExpr {
            kind: TypedExprKind::Column {
                column_index: 3,
                name: "channel".into(),
            },
            result_type: BqlType::String,
            nullable: true,
            span: Span::EMPTY,
        };
        CompiledExpr::from_typed(&typed)
    }

    fn build_operator(
        conversion: &[&str],
        touchpoints: &[&str],
        window_ns: i64,
        forwarded: &[&str],
        forwarded_types: &[BqlType],
        conversion_range: Option<(i64, i64)>,
    ) -> AttributeOperator {
        let mut forwarded_pairs: Vec<(&str, BqlType)> = Vec::new();
        for (name, ty) in forwarded.iter().zip(forwarded_types.iter()) {
            forwarded_pairs.push((*name, ty.clone()));
        }
        let out_schema = basic_output_schema(&forwarded_pairs);
        // A dummy child plan is fine — we never execute the plan tree in
        // these unit tests, only the EntityOperator surface.
        let child = PhysicalPlan::Scan(bqlite_planner::physical::ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_input_schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        });
        let desc = AttributePhysical {
            conversion_events: conversion.iter().map(|s| s.to_string()).collect(),
            touchpoint_events: touchpoints.iter().map(|s| s.to_string()).collect(),
            window_ns,
            touchpoint_key: channel_key_expr(),
            forwarded_conversion_columns: forwarded.iter().map(|s| s.to_string()).collect(),
            fused_aggregate: None,
            conversion_range,
            input: Box::new(child),
            output_schema: out_schema,
            pre_fusion_output_schema: None,
        };
        AttributeOperator::from_physical(&desc).expect("operator should construct")
    }

    fn batch_from(rows: &[(i64, &str, Option<&str>, Option<i64>)]) -> RecordBatch {
        let ts: Vec<i64> = rows.iter().map(|(ts, ..)| *ts).collect();
        let entity_ids: Vec<&str> = rows.iter().map(|_| "user-1").collect();
        let ev: Vec<&str> = rows.iter().map(|(_, e, _, _)| *e).collect();
        let channel: Vec<Option<&str>> = rows.iter().map(|(_, _, c, _)| *c).collect();
        let amount: Vec<Option<i64>> = rows.iter().map(|(_, _, _, a)| *a).collect();

        let entity_arr = StringViewArray::from(entity_ids);
        let ts_arr = TimestampNanosecondArray::from(ts).with_timezone("UTC");
        let ev_arr = StringViewArray::from(ev);
        let channel_arr = StringViewArray::from(channel);
        let amount_arr = Int64Array::from(amount);
        RecordBatch::try_new(
            Arc::new(input_arrow_schema()),
            vec![
                Arc::new(entity_arr),
                Arc::new(ts_arr),
                Arc::new(ev_arr),
                Arc::new(channel_arr),
                Arc::new(amount_arr),
            ],
        )
        .unwrap()
    }

    fn run_single_entity(
        op: &AttributeOperator,
        rows: &[(i64, &str, Option<&str>, Option<i64>)],
    ) -> RecordBatch {
        let mut state = op.create_state(&EntityId::String("user-1".into()));
        op.process_sub_batch(&mut state, &batch_from(rows));
        op.finish_entity(state)
            .unwrap_or_else(|| empty_output(op.output_schema()))
    }

    fn empty_output(schema: &OperatorSchema) -> RecordBatch {
        let arrow = schema.to_arrow_schema();
        RecordBatch::new_empty(Arc::new(arrow))
    }

    fn touchpoint_ts_col(batch: &RecordBatch) -> &TimestampNanosecondArray {
        batch
            .column_by_name("touchpoint_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
    }

    fn touchpoint_key_col(batch: &RecordBatch) -> &StringViewArray {
        batch
            .column_by_name("touchpoint_key")
            .unwrap()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
    }

    fn conversion_ts_col(batch: &RecordBatch) -> &TimestampNanosecondArray {
        batch
            .column_by_name("conversion_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
    }

    // ── Construction validation ──────────────────────────────────────────

    #[test]
    fn rejects_empty_conversion_list() {
        let out = basic_output_schema(&[]);
        let desc = AttributePhysical {
            conversion_events: vec![],
            touchpoint_events: vec!["click".into()],
            window_ns: 10,
            touchpoint_key: channel_key_expr(),
            forwarded_conversion_columns: vec![],
            fused_aggregate: None,
            conversion_range: None,
            input: Box::new(PhysicalPlan::Scan(bqlite_planner::physical::ScanPhysical {
                table: "events".into(),
                query_range: None,
                reader_range: None,
                scan_predicates: vec![],
                projected_columns: vec![],
                output_schema: events_input_schema(),
                entity_key_col: "entity_id".into(),
                timestamp_col: "ts".into(),
                sample: None,
            })),
            output_schema: out,
            pre_fusion_output_schema: None,
        };
        assert!(AttributeOperator::from_physical(&desc).is_err());
    }

    #[test]
    fn accepts_fused_aggregate_after_task520() {
        // TASK-520: ATTRIBUTE now supports stateful-to-aggregate fusion.
        // Construction must succeed when `fused_aggregate` is populated,
        // and internal state must be resolved against the native
        // (pre-fusion) schema rather than the aggregate output schema.
        use bqlite_planner::demand::CompiledFusableAggregate;
        let native = basic_output_schema(&[]);
        // Replace the external output_schema with a different aggregate
        // schema (the shape the planner pass produces). The operator
        // must still resolve `entity_id`, `conversion_ts`, etc. against
        // the native schema preserved in `pre_fusion_output_schema`.
        let agg_schema =
            OperatorSchema::new(vec![ColumnDef::nullable("total", BqlType::Int)]).unwrap();
        let desc = AttributePhysical {
            conversion_events: vec!["p".into()],
            touchpoint_events: vec!["c".into()],
            window_ns: 10,
            touchpoint_key: channel_key_expr(),
            forwarded_conversion_columns: vec![],
            fused_aggregate: Some(CompiledFusableAggregate {
                aggregates: vec![],
                group_by: vec![],
                output_schema: agg_schema.clone(),
                max_groups: crate::aggregate::DEFAULT_MAX_GROUPS,
            }),
            conversion_range: None,
            input: Box::new(PhysicalPlan::Scan(bqlite_planner::physical::ScanPhysical {
                table: "events".into(),
                query_range: None,
                reader_range: None,
                scan_predicates: vec![],
                projected_columns: vec![],
                output_schema: events_input_schema(),
                entity_key_col: "entity_id".into(),
                timestamp_col: "ts".into(),
                sample: None,
            })),
            output_schema: agg_schema,
            pre_fusion_output_schema: Some(native.clone()),
        };
        let op = AttributeOperator::from_physical(&desc).expect("fused construction");
        // Operator's self-reported output_schema is the native one (used
        // for per-entity batch construction). Aggregate schema lives on
        // the descriptor's `fused_aggregate.output_schema`.
        assert_eq!(op.output_schema(), &native);
        assert!(op.is_fused());
        assert!(op.fused_aggregate().is_some());
    }

    #[test]
    fn exposes_capability_matches_physical_descriptor() {
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        assert_eq!(op.supported_demands(), AttributePhysical::DEMAND_CAPS);
    }

    // ── §16.1 edge cases ─────────────────────────────────────────────────

    #[test]
    fn empty_entity_yields_no_rows() {
        let op = build_operator(&["purchase"], &["click"], 1_000, &[], &[], None);
        let state = op.create_state(&EntityId::String("user-1".into()));
        assert!(op.finish_entity(state).is_none());
    }

    #[test]
    fn only_touchpoints_yields_no_rows() {
        let op = build_operator(&["purchase"], &["click"], 1_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (10, "click", Some("ads"), None),
                (20, "click", Some("email"), None),
            ],
        );
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn only_conversions_emits_left_unnest_per_conversion() {
        let op = build_operator(&["purchase"], &["click"], 1_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[(10, "purchase", None, None), (200, "purchase", None, None)],
        );
        assert_eq!(out.num_rows(), 2);
        let ts_col = touchpoint_ts_col(&out);
        let key_col = touchpoint_key_col(&out);
        assert!(ts_col.is_null(0) && ts_col.is_null(1));
        assert!(key_col.is_null(0) && key_col.is_null(1));
    }

    #[test]
    fn inclusive_lookback_boundary_qualifies() {
        // Window of 1000 ns; touchpoint exactly at conversion_ts - 1000 qualifies.
        let op = build_operator(&["purchase"], &["click"], 1_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (0, "click", Some("ads"), None),
                (1_000, "purchase", None, None),
            ],
        );
        assert_eq!(out.num_rows(), 1);
        let ts_col = touchpoint_ts_col(&out);
        assert_eq!(ts_col.value(0), 0);
    }

    #[test]
    fn strict_at_conversion_boundary_excludes() {
        // Touchpoint at exact conversion instant does not qualify.
        let op = build_operator(&["purchase"], &["click"], 1_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (500, "click", Some("ads"), None),
                (500, "purchase", None, None),
            ],
        );
        assert_eq!(out.num_rows(), 1);
        let ts_col = touchpoint_ts_col(&out);
        assert!(ts_col.is_null(0)); // LEFT-UNNEST row
    }

    #[test]
    fn same_event_type_no_self_attribution() {
        let op = build_operator(&["E"], &["E"], 1_000, &[], &[], None);
        // Three E-events: the first is a lone conversion (LEFT-UNNEST),
        // the second attributes to the first, the third attributes to
        // the first two (emission before self-add).
        let out = run_single_entity(
            &op,
            &[
                (0, "E", Some("a"), None),
                (100, "E", Some("b"), None),
                (200, "E", Some("c"), None),
            ],
        );
        // 1 + 1 + 2 = 4 rows, none with touchpoint_ts == conversion_ts.
        assert_eq!(out.num_rows(), 4);
        let conv = conversion_ts_col(&out);
        let tp = touchpoint_ts_col(&out);
        for i in 0..out.num_rows() {
            if !tp.is_null(i) {
                assert!(tp.value(i) < conv.value(i));
            }
        }
    }

    #[test]
    fn multiple_conversions_share_same_touchpoint() {
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (0, "click", Some("ads"), None),
                (100, "purchase", None, None),
                (200, "purchase", None, None),
            ],
        );
        // Each purchase sees the click → 2 output rows.
        assert_eq!(out.num_rows(), 2);
        let tp = touchpoint_ts_col(&out);
        assert_eq!(tp.value(0), 0);
        assert_eq!(tp.value(1), 0);
    }

    #[test]
    fn null_touchpoint_key_emits_row_with_null_key() {
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[(0, "click", None, None), (100, "purchase", None, None)],
        );
        assert_eq!(out.num_rows(), 1);
        let tp_ts = touchpoint_ts_col(&out);
        let tp_k = touchpoint_key_col(&out);
        assert!(!tp_ts.is_null(0));
        assert!(tp_k.is_null(0));
    }

    #[test]
    fn zero_window_always_left_unnests() {
        let op = build_operator(&["purchase"], &["click"], 0, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (0, "click", Some("ads"), None),
                (100, "purchase", None, None),
            ],
        );
        assert_eq!(out.num_rows(), 1);
        let tp = touchpoint_ts_col(&out);
        assert!(tp.is_null(0));
    }

    #[test]
    fn conversion_range_excludes_conversions_outside() {
        // Scan widened backward to t=0, but original range is [500, 1000).
        let op = build_operator(
            &["purchase"],
            &["click"],
            10_000,
            &[],
            &[],
            Some((500, 1_000)),
        );
        let out = run_single_entity(
            &op,
            &[
                (0, "click", Some("ads"), None),
                (100, "purchase", None, None), // out-of-range — not emitted
                (600, "purchase", None, None), // in-range — attributes to t=0 click
            ],
        );
        assert_eq!(out.num_rows(), 1);
        let conv = conversion_ts_col(&out);
        assert_eq!(conv.value(0), 600);
    }

    #[test]
    fn single_conversion_single_touchpoint_across_sub_batches() {
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        let mut state = op.create_state(&EntityId::String("user-1".into()));
        op.process_sub_batch(&mut state, &batch_from(&[(10, "click", Some("ads"), None)]));
        op.process_sub_batch(&mut state, &batch_from(&[(100, "purchase", None, None)]));
        let out = op.finish_entity(state).unwrap();
        assert_eq!(out.num_rows(), 1);
        let tp = touchpoint_ts_col(&out);
        assert_eq!(tp.value(0), 10);
    }

    #[test]
    fn emission_order_is_ascending_touchpoint_ts() {
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (10, "click", Some("a"), None),
                (20, "click", Some("b"), None),
                (30, "click", Some("c"), None),
                (100, "purchase", None, None),
            ],
        );
        assert_eq!(out.num_rows(), 3);
        let tp = touchpoint_ts_col(&out);
        assert_eq!(tp.value(0), 10);
        assert_eq!(tp.value(1), 20);
        assert_eq!(tp.value(2), 30);
    }

    #[test]
    fn deque_pruning_drops_stale_entries() {
        let op = build_operator(&["purchase"], &["click"], 100, &[], &[], None);
        let out = run_single_entity(
            &op,
            &[
                (0, "click", Some("stale"), None),
                (500, "click", Some("fresh"), None),
                (510, "purchase", None, None),
            ],
        );
        assert_eq!(out.num_rows(), 1);
        let tp = touchpoint_ts_col(&out);
        assert_eq!(tp.value(0), 500);
    }

    #[test]
    fn deque_cap_flushes_and_skips_entity() {
        // Tiny cap so we can exercise the overflow path without
        // materializing a million events.
        let op =
            build_operator(&["purchase"], &["click"], 10_000, &[], &[], None).with_deque_cap(2);
        let mut state = op.create_state(&EntityId::String("user-1".into()));
        op.process_sub_batch(
            &mut state,
            &batch_from(&[
                (10, "click", Some("a"), None),
                (20, "click", Some("b"), None),
                (30, "purchase", None, None), // in-flight conversion — emits against current deque (2 rows).
                (40, "click", Some("c"), None), // would exceed cap → flush, skip rest.
                (50, "purchase", None, None), // dropped — entity capped.
                (60, "purchase", None, None), // dropped.
            ]),
        );
        assert!(state.is_capped());
        let diag = state.take_diagnostic().expect("cap diagnostic must fire");
        assert_eq!(diag.operator, ATTRIBUTE_OP_NAME);
        assert_eq!(diag.cap, 2);
        assert!(diag.event_count >= 4);
        let out = op.finish_entity(state).unwrap();
        // The first purchase emitted 2 rows; everything after was dropped.
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn take_pending_warnings_emits_attribute_touchpoint_warning() {
        let op =
            build_operator(&["purchase"], &["click"], 10_000, &[], &[], None).with_deque_cap(2);
        let mut state = op.create_state(&EntityId::String("user-1".into()));
        op.process_sub_batch(
            &mut state,
            &batch_from(&[
                (10, "click", Some("a"), None),
                (20, "click", Some("b"), None),
                (30, "purchase", None, None),
                (40, "click", Some("c"), None),
                (50, "purchase", None, None),
            ]),
        );
        assert!(state.is_capped());

        let warnings = op.take_pending_warnings(&mut state, &EntityId::String("user-1".into()));
        assert_eq!(warnings.len(), 1);
        match &warnings[0] {
            bqlite_core::QueryWarning::AttributeTouchpointCapExceeded {
                entity_id,
                touchpoint_count,
                cap,
            } => {
                assert_eq!(entity_id, "user-1");
                assert!(*touchpoint_count >= 4);
                assert_eq!(*cap, 2);
            }
            other => panic!("expected AttributeTouchpointCapExceeded, got {other:?}"),
        }

        // Diagnostic latch was consumed by the first drain.
        let again = op.take_pending_warnings(&mut state, &EntityId::String("user-1".into()));
        assert!(again.is_empty());
    }

    #[test]
    fn take_pending_warnings_empty_when_no_cap_diagnostic() {
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        let mut state = op.create_state(&EntityId::String("u".into()));
        op.process_sub_batch(
            &mut state,
            &batch_from(&[(10, "click", Some("a"), None), (20, "purchase", None, None)]),
        );
        let warnings = op.take_pending_warnings(&mut state, &EntityId::String("u".into()));
        assert!(warnings.is_empty());
    }

    // ── Forwarded conversion property ─────────────────────────────────────

    #[test]
    fn forwarded_conversion_column_flows_to_output() {
        let op = build_operator(
            &["purchase"],
            &["click"],
            10_000,
            &["amount"],
            &[BqlType::Int],
            None,
        );
        let out = run_single_entity(
            &op,
            &[
                (0, "click", Some("ads"), None),
                (100, "purchase", None, Some(42)),
            ],
        );
        assert_eq!(out.num_rows(), 1);
        let amount = out
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amount.value(0), 42);
    }

    #[test]
    fn forwarded_conversion_column_left_unnest_copies_value() {
        // Even for LEFT-UNNEST rows, the forwarded conversion properties
        // come from the conversion event — the conversion row exists,
        // only the touchpoint side is null.
        let op = build_operator(
            &["purchase"],
            &["click"],
            10_000,
            &["amount"],
            &[BqlType::Int],
            None,
        );
        let out = run_single_entity(&op, &[(100, "purchase", None, Some(42))]);
        assert_eq!(out.num_rows(), 1);
        let amount = out
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(amount.value(0), 42);
    }

    // ── Multi-type lists ──────────────────────────────────────────────────

    #[test]
    fn multi_type_conversion_and_touchpoint_lists() {
        let op = build_operator(
            &["purchase", "subscription"],
            &["ad_click", "email_open"],
            10_000,
            &[],
            &[],
            None,
        );
        let out = run_single_entity(
            &op,
            &[
                (10, "ad_click", Some("c1"), None),
                (20, "email_open", Some("c2"), None),
                (100, "subscription", None, None),
                (200, "purchase", None, None),
            ],
        );
        // 2 touchpoints × 2 conversions = 4 rows.
        assert_eq!(out.num_rows(), 4);
    }

    #[test]
    fn required_columns_includes_forwarded_and_expression_refs() {
        let op = build_operator(
            &["purchase"],
            &["click"],
            10_000,
            &["amount"],
            &[BqlType::Int],
            None,
        );
        let cols: Vec<&str> = op.required_columns().iter().map(|s| s.as_str()).collect();
        // entity_id, ts, event_type (required) + amount (forwarded) +
        // channel (referenced by `touchpoint_key`).
        for needed in ["entity_id", "ts", "event_type", "amount", "channel"] {
            assert!(cols.contains(&needed), "missing `{needed}` in {cols:?}");
        }
    }

    #[test]
    fn output_schema_is_preserved_verbatim() {
        let op = build_operator(&["p"], &["c"], 10_000, &[], &[], None);
        assert_eq!(op.output_schema().columns().len(), 4);
    }

    #[test]
    fn is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AttributeOperator>();
    }

    #[test]
    fn state_is_send() {
        // The `EntityOperator` contract (`type State: Send`) is enforced
        // by the trait definition, but a direct assertion documents the
        // expectation for readers of this module.
        fn assert_send<T: Send>() {}
        assert_send::<AttributeState>();
    }

    // ── Additional construction validation ──────────────────────────────

    fn build_scan_child() -> PhysicalPlan {
        PhysicalPlan::Scan(bqlite_planner::physical::ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: events_input_schema(),
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        })
    }

    fn base_desc() -> AttributePhysical {
        AttributePhysical {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["click".into()],
            window_ns: 1_000,
            touchpoint_key: channel_key_expr(),
            forwarded_conversion_columns: vec![],
            fused_aggregate: None,
            conversion_range: None,
            input: Box::new(build_scan_child()),
            output_schema: basic_output_schema(&[]),
            pre_fusion_output_schema: None,
        }
    }

    #[test]
    fn rejects_empty_touchpoint_list() {
        let mut desc = base_desc();
        desc.touchpoint_events = vec![];
        assert!(AttributeOperator::from_physical(&desc).is_err());
    }

    #[test]
    fn rejects_non_string_touchpoint_key() {
        let mut desc = base_desc();
        desc.touchpoint_key = CompiledExpr::from_typed(&TypedExpr {
            kind: TypedExprKind::Column {
                column_index: 4,
                name: "amount".into(),
            },
            result_type: BqlType::Int,
            nullable: true,
            span: Span::EMPTY,
        });
        assert!(AttributeOperator::from_physical(&desc).is_err());
    }

    #[test]
    fn rejects_negative_window() {
        let mut desc = base_desc();
        desc.window_ns = -1;
        assert!(AttributeOperator::from_physical(&desc).is_err());
    }

    #[test]
    fn rejects_missing_entity_id_in_output_schema() {
        let mut desc = base_desc();
        desc.output_schema = OperatorSchema::new(vec![
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_key", BqlType::String),
        ])
        .unwrap();
        assert!(AttributeOperator::from_physical(&desc).is_err());
    }

    #[test]
    fn rejects_unsupported_forwarded_type() {
        let mut desc = base_desc();
        desc.forwarded_conversion_columns = vec!["tags".into()];
        // Output schema carries the forwarded column as a List — rejected.
        desc.output_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
            ColumnDef::nullable("tags", BqlType::List(Box::new(BqlType::String))),
            ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_key", BqlType::String),
        ])
        .unwrap();
        assert!(AttributeOperator::from_physical(&desc).is_err());
    }

    // ── Additional §16.1 edge cases ─────────────────────────────────────

    #[test]
    fn conversion_at_upper_bound_is_excluded() {
        // Range is `[start, end)` — a conversion exactly at `end` is
        // treated as out-of-range per §12.
        let op = build_operator(
            &["purchase"],
            &["click"],
            10_000,
            &[],
            &[],
            Some((0, 1_000)),
        );
        let out = run_single_entity(
            &op,
            &[
                (500, "click", Some("ads"), None),
                (1_000, "purchase", None, None), // at upper bound — excluded
            ],
        );
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn finish_entity_returns_none_when_no_conversions_emitted() {
        // An entity with only touchpoints fires no conversion emissions
        // and therefore has zero output rows; `finish_entity` must
        // return `None` so the adapter can skip it.
        let op = build_operator(&["purchase"], &["click"], 10_000, &[], &[], None);
        let mut state = op.create_state(&EntityId::String("user-1".into()));
        op.process_sub_batch(&mut state, &batch_from(&[(10, "click", Some("ads"), None)]));
        assert!(op.finish_entity(state).is_none());
    }

    #[test]
    fn int_entity_id_output_path() {
        // Build an operator whose entity_id output column is `Int` so
        // the non-String `EntityIdBuilder` arm is exercised.
        let out_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::Int),
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_key", BqlType::String),
        ])
        .unwrap();
        // Input schema declares an Int entity_id so the input batch matches.
        let input_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::Int),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("channel", BqlType::String),
        ])
        .unwrap();
        let child = PhysicalPlan::Scan(bqlite_planner::physical::ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: input_schema,
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        });
        let desc = AttributePhysical {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["click".into()],
            window_ns: 10_000,
            touchpoint_key: channel_key_expr(),
            forwarded_conversion_columns: vec![],
            fused_aggregate: None,
            conversion_range: None,
            input: Box::new(child),
            output_schema: out_schema,
            pre_fusion_output_schema: None,
        };
        let op = AttributeOperator::from_physical(&desc).unwrap();
        let mut state = op.create_state(&EntityId::Int(42));

        // Build an input batch using Int64 entity_id column.
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("channel", DataType::Utf8View, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![42, 42])),
                Arc::new(TimestampNanosecondArray::from(vec![0, 100]).with_timezone("UTC")),
                Arc::new(StringViewArray::from(vec!["click", "purchase"])),
                Arc::new(StringViewArray::from(vec![Some("ads"), None])),
            ],
        )
        .unwrap();
        op.process_sub_batch(&mut state, &batch);
        let out = op.finish_entity(state).unwrap();
        assert_eq!(out.num_rows(), 1);
        let entity_col = out
            .column_by_name("entity_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(entity_col.value(0), 42);
    }

    #[test]
    #[should_panic(expected = "engine bind-step mismatch")]
    fn int_output_with_string_state_panics() {
        // Simulate an engine-wiring bug: output says Int but state holds a String.
        let out_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::Int),
            ColumnDef::required("conversion_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_ts", BqlType::Timestamp),
            ColumnDef::nullable("touchpoint_key", BqlType::String),
        ])
        .unwrap();
        let input_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::Int),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
            ColumnDef::nullable("channel", BqlType::String),
        ])
        .unwrap();
        let child = PhysicalPlan::Scan(bqlite_planner::physical::ScanPhysical {
            table: "events".into(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: input_schema,
            entity_key_col: "entity_id".into(),
            timestamp_col: "ts".into(),
            sample: None,
        });
        let desc = AttributePhysical {
            conversion_events: vec!["purchase".into()],
            touchpoint_events: vec!["click".into()],
            window_ns: 10_000,
            touchpoint_key: channel_key_expr(),
            forwarded_conversion_columns: vec![],
            fused_aggregate: None,
            conversion_range: None,
            input: Box::new(child),
            output_schema: out_schema,
            pre_fusion_output_schema: None,
        };
        let op = AttributeOperator::from_physical(&desc).unwrap();
        let mut state = op.create_state(&EntityId::String("user-1".into()));
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("entity_id", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("event_type", DataType::Utf8View, false),
            Field::new("channel", DataType::Utf8View, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampNanosecondArray::from(vec![100]).with_timezone("UTC")),
                Arc::new(StringViewArray::from(vec!["purchase"])),
                Arc::new(StringViewArray::from(vec![Option::<&str>::None])),
            ],
        )
        .unwrap();
        op.process_sub_batch(&mut state, &batch);
        // The panic fires when `finish_entity` flushes the output
        // builder and the String EntityId is appended to an Int builder.
        let _ = op.finish_entity(state);
    }
}
