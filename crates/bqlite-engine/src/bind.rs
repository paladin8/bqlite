//! Physical-plan bind step — plain-data → executable operator tree.
//!
//! Per [`docs/design/planner-pipeline.md`](../../../docs/design/planner-pipeline.md)
//! §15, the planner emits a plain-data [`PhysicalPlan`] tree, not
//! `Box<dyn PhysicalOperator>` — the trait object lives in
//! `bqlite-operators`, which sits above `bqlite-planner` in the crate
//! dependency graph. Binding is the engine's responsibility: it
//! consumes the descriptor and materializes one concrete operator per
//! descriptor node, wiring in the runtime handles (segment readers,
//! cancellation tokens, memory budgets) that only the engine has
//! visibility into.
//!
//! ## Wave 2 scope (TASK-232)
//!
//! The bind step handles the full Wave 2 descriptor set:
//!
//! - **Data-plane** (`Scan`, `Filter`, `Project`, `Limit`):
//!   recursively bind children and construct the corresponding
//!   operator from `bqlite-operators`.
//! - **DDL** (`CreateTable`, `DropTable`, `AlterTableAddColumn`):
//!   execute the mutation against the manifest during bind and
//!   return an empty [`crate::ddl::ResultOperator`].
//! - **Metadata** (`Describe`, `Explain`): compute the result
//!   batch during bind and return a
//!   [`crate::ddl::ResultOperator`] wrapping the batch.
//! - **INSERT** (`From`): execute via the CSV ingest pipeline
//!   (TASK-233). `Values` deferred to TASK-238.
//!
//! ## Wave 3 scope (TASK-323)
//!
//! Extends the bind step with Wave 3 physical operators:
//!
//! - **`SequenceMatch`**: wraps [`SequenceMatchOperator`] (an
//!   [`EntityOperator`]) in a `SequenceMatchAdapter` that detects entity
//!   boundaries in the child's output and drives the per-entity protocol.
//!   When `fused_aggregate` is set, routes each entity's match output into
//!   a [`HashAccumulator`] and emits a single aggregate result batch after
//!   all entities are processed.
//! - **`Aggregate`**: materializes a [`HashAggregateOperator`].
//! - **`Sort`**: materializes a [`SortOperator`].
//! - **`Distinct`**: materializes a [`DistinctOperator`].
//!
//! ## Wave 4 scope (TASK-438)
//!
//! Extends the bind step with the Wave 4 `EntityOperator`-based operators,
//! a fallback SAMPLE filter, and the joined-source merge:
//!
//! - **`Sessionize`**, **`EventSelect`**, **`Attribute`**: each implements
//!   [`EntityOperator`] and is wrapped by a generic
//!   [`EntityOperatorAdapter`] that detects entity boundaries and drives
//!   the per-entity `create_state` → `process_sub_batch*` → `finish_entity`
//!   protocol. Wave 4 operators do not support fused aggregates; the
//!   `fused_aggregate` field is asserted `None` by each operator constructor.
//! - **`Sample`** (fallback): when the optimizer's sample-pushdown pass
//!   cannot push SAMPLE into the scan layer (because a stateful operator
//!   sits between SAMPLE and the scan), the bind step materializes a
//!   [`SampleFilterOperator`] that applies the xxHash64 entity-level filter
//!   above the stateful stage.
//! - **`MergeSources`**: materializes a `MergeSourcesOperator` that
//!   performs an N-ary k-way merge over one `ScanOperator` per joined
//!   table.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringViewBuilder};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use bqlite_core::{
    BqlType, BqliteError, EntityId, OperatorSchema, PropertyValue, Result, SegmentReader, TimeRange,
};
use bqlite_operators::matcher::SequenceMatchState;
use bqlite_operators::operator::EntityOperator;
use bqlite_operators::{
    Accumulator, AttributeOperator, CohortHashSet, DistinctOperator, EventSelectOperator,
    FilterOperator, HashAccumulator, HashAggregateOperator, LimitOperator, PhysicalOperator,
    ProjectOperator, ScanOperator, SequenceMatchOperator, SessionizeOperator, SortOperator,
    SubqueryFilterOperator,
};
use bqlite_planner::compiled::{
    ArrowKernelId, CompareKernel, CompareOp, CompiledExpr, CompiledNode,
};
use bqlite_planner::{
    AttributePhysical, EventSelectPhysical, MergeSourcesPhysical, PhysicalPlan, SamplePhysical,
    ScanPhysical, SequenceMatchPhysical, SessionizePhysical, SubqueryFilterPhysical,
};
use bqlite_storage::Database;

use crate::context::QueryContext;
use crate::ddl::{
    build_describe_batch, build_explain_batch, execute_alter_table_add_column,
    execute_create_table, execute_drop_table, ResultOperator,
};
use crate::warning_sink::WarningSink;

// ─────────────────────────────────────────────────────────────────────────────
// SequenceMatchAdapter
// ─────────────────────────────────────────────────────────────────────────────

/// Accumulated state for the fused aggregate path.
///
/// When `SequenceMatchPhysical.fused_aggregate` is `Some`, the adapter
/// routes every entity's match output into this accumulator rather than
/// buffering per-entity batches. After the child is exhausted the adapter
/// emits one aggregate result batch from `finish`.
struct FusedAccState {
    accumulator: HashAccumulator,
    /// True when the accumulator has no GROUP BY clauses. Used to call
    /// `ensure_default_group` after processing all entities so that a
    /// zero-input `COUNT(*)` still emits one row with count = 0.
    ungrouped: bool,
    /// True once the aggregate result batch has been returned.
    emitted: bool,
}

/// Adapts a [`SequenceMatchOperator`] (which implements [`EntityOperator`])
/// into a [`PhysicalOperator`] suitable for the engine's pull pipeline.
///
/// The adapter:
/// 1. Opens and closes the child operator.
/// 2. Detects entity-id transitions in the child's output (the input is
///    entity-sorted).
/// 3. Routes each entity's rows through `create_state` →
///    `process_sub_batch*` → `finish_entity`.
/// 4. In the **non-fused** path, fills the placeholder `entity_id` column
///    in the match output batch with the actual entity id and buffers the
///    result.
/// 5. In the **fused** path, calls `finish_entity_into` to feed the match
///    results directly into a [`HashAccumulator`], then emits the
///    accumulator's aggregate result once after all entities are processed.
struct SequenceMatchAdapter {
    operator: SequenceMatchOperator,
    child: Box<dyn PhysicalOperator>,
    output_schema: OperatorSchema,
    /// Index of the `entity_id` column in the *child's* output schema.
    entity_id_col_idx: usize,
    current_entity: Option<EntityId>,
    current_state: Option<SequenceMatchState>,
    /// Non-fused path: output batches waiting to be returned.
    pending: VecDeque<RecordBatch>,
    /// Set once the child has been fully drained and the last entity
    /// finalized.
    exhausted: bool,
    fused: Option<FusedAccState>,
    /// Per-query warning sink. Drained once per entity at
    /// `finalize_entity` so cap-exceeded events surface in
    /// `ExecutionResult.warnings`. `None` for stand-alone tests
    /// constructed via the legacy single-arg path.
    warnings: Option<WarningSink>,
}

impl SequenceMatchAdapter {
    fn new_with_sink(
        desc: &SequenceMatchPhysical,
        child: Box<dyn PhysicalOperator>,
        warnings: Option<WarningSink>,
    ) -> Result<Self> {
        // The entity key column in the child's output uses the original name
        // from the table schema (e.g. "user_id"), not the "entity_id" alias
        // that the SequenceMatch output schema uses.
        let ek_col_name = entity_key_col_name(&desc.input);
        let entity_id_col_idx = child
            .output_schema()
            .columns()
            .iter()
            .position(|c| c.name == ek_col_name)
            .ok_or_else(|| {
                BqliteError::Schema(format!(
                    "SequenceMatchAdapter: entity key column '{ek_col_name}' \
                     not found in child output schema"
                ))
            })?;

        let operator = SequenceMatchOperator::new(desc);

        let fused = match &desc.fused_aggregate {
            None => None,
            Some(fa) => {
                // Build HashAccumulator from the fused aggregate descriptor.
                // The accumulator is driven via `finish_entity_into` which
                // calls `finish_entity` → produces an N-row match output
                // batch → calls `update_batch`. For COUNT(*) without GROUP BY,
                // this correctly counts N completions per entity.
                let functions = fa.aggregates.iter().map(|a| a.function).collect();
                // input_types: None for COUNT(*); None is used as a fallback
                // for other functions (the update_batch path doesn't need this).
                let input_types = fa.aggregates.iter().map(|_| None).collect();
                let group_by_columns = fa
                    .group_by
                    .iter()
                    .map(|(_, name)| name.clone())
                    .collect::<Vec<_>>();
                // agg_arg_columns: the column name in the INPUT batch (the
                // match output batch) to read the aggregate argument from.
                // For COUNT(*), arg is None. For other functions the output_name
                // is used as a best-effort column name; unsupported cases will
                // surface as a panic in update_batch if the column is absent.
                let agg_arg_columns = fa
                    .aggregates
                    .iter()
                    .map(|a| a.arg.as_ref().map(|_| a.output_name.clone()))
                    .collect::<Vec<_>>();
                let ungrouped = group_by_columns.is_empty();
                let accumulator = HashAccumulator::new(
                    functions,
                    input_types,
                    fa.output_schema.clone(),
                    group_by_columns,
                    agg_arg_columns,
                    fa.max_groups,
                );
                Some(FusedAccState {
                    accumulator,
                    ungrouped,
                    emitted: false,
                })
            }
        };

        Ok(Self {
            operator,
            child,
            output_schema: desc.output_schema.clone(),
            entity_id_col_idx,
            current_entity: None,
            current_state: None,
            pending: VecDeque::new(),
            exhausted: false,
            fused,
            warnings,
        })
    }

    /// Finalize a completed entity: route its match output to the pending
    /// batch queue (non-fused) or into the aggregate accumulator (fused).
    fn finalize_entity(&mut self, entity: EntityId, mut state: SequenceMatchState) -> Result<()> {
        // Drain per-entity diagnostics into the per-query warning sink
        // (if one is wired). Must happen before we move `state` into
        // the fused or non-fused finishers, both of which consume it.
        // See `docs/design/engine/cancellation.md` §7.4.
        if let Some(sink) = &self.warnings {
            sink.record_many(self.operator.take_pending_warnings(&mut state, &entity));
        }

        if let Some(fused) = &mut self.fused {
            // Fused path: SequenceMatchOperator::finish_entity_into builds
            // an intermediate match-output batch (using the saved
            // match_output_schema) and feeds it to the accumulator via
            // update_batch. For simple column-ref aggregates this resolves
            // columns by name; complex expressions (SUM(CAST(...))) are
            // blocked from fusion by the is_simple_column_ref eligibility
            // check and route through the non-fused HashAggregateOperator.
            let acc: &mut dyn Accumulator = &mut fused.accumulator;
            self.operator.finish_entity_into(state, acc)?;
        } else {
            // Non-fused path: collect per-entity output batches.
            if let Some(batch) = self.operator.finish_entity(state) {
                let batch = fill_entity_id(batch, &entity)?;
                self.pending.push_back(batch);
            }
        }
        Ok(())
    }

    /// Consume a child batch, splitting at entity boundaries and driving
    /// the per-entity `create_state` → `process_sub_batch*` protocol.
    fn process_child_batch(&mut self, child_batch: RecordBatch) -> Result<()> {
        let entity_col = child_batch.column(self.entity_id_col_idx).clone();
        let num_rows = child_batch.num_rows();
        let mut start = 0;

        while start < num_rows {
            let row_entity = extract_entity_id(&entity_col, start);

            // Detect entity boundary.
            if self.current_entity.as_ref() != Some(&row_entity) {
                // Finalize the previous entity (if any).
                if let (Some(prev_entity), Some(prev_state)) =
                    (self.current_entity.take(), self.current_state.take())
                {
                    self.finalize_entity(prev_entity, prev_state)?;
                }
                // Start a new entity.
                let new_state = self.operator.create_state(&row_entity);
                self.current_entity = Some(row_entity.clone());
                self.current_state = Some(new_state);
            }

            // Find the end of this entity's contiguous rows.
            let end = find_entity_end(&entity_col, start, &row_entity, num_rows);

            // Process sub-batch for the current entity.
            let sub_batch = child_batch.slice(start, end - start);
            // Safety: current_state was just set above or was already Some.
            let state = self.current_state.as_mut().expect("state must be set");
            self.operator.process_sub_batch(state, &sub_batch);

            start = end;
        }

        Ok(())
    }
}

impl PhysicalOperator for SequenceMatchAdapter {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // Return any buffered non-fused output batches.
            if let Some(batch) = self.pending.pop_front() {
                return Ok(Some(batch));
            }

            if self.exhausted {
                // Fused path: emit the accumulator result once after all
                // entities have been processed.
                if let Some(fused) = &mut self.fused {
                    if !fused.emitted {
                        fused.emitted = true;
                        if fused.ungrouped {
                            fused.accumulator.ensure_default_group()?;
                        }
                        let result = fused.accumulator.finish()?;
                        if result.num_rows() > 0 {
                            return Ok(Some(result));
                        }
                    }
                }
                return Ok(None);
            }

            match self.child.next_batch()? {
                None => {
                    // Child exhausted: finalize the last entity.
                    if let (Some(entity), Some(state)) =
                        (self.current_entity.take(), self.current_state.take())
                    {
                        self.finalize_entity(entity, state)?;
                    }
                    self.exhausted = true;
                    // Loop once more to drain pending / emit fused result.
                }
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.process_child_batch(batch)?;
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EntityOperatorAdapter — generic Wave 4 entity-operator driver (TASK-438)
// ─────────────────────────────────────────────────────────────────────────────

/// Generic adapter that wraps any [`EntityOperator`] implementor as a
/// [`PhysicalOperator`].
///
/// The adapter scans the child's entity-sorted output for entity-id
/// transitions, routing each entity's rows through the
/// `create_state` → `process_sub_batch*` → `finish_entity` protocol
/// required by [`EntityOperator`]. Output batches are buffered in
/// `pending` and returned one at a time on each `next_batch` call.
///
/// ## Wave 4 fused-aggregate deferrals
///
/// `SessionizeOperator`, `EventSelectOperator`, and `AttributeOperator`
/// all assert that their `fused_aggregate` field is `None` at operator
/// construction time (per the Wave 4/5 deferral contract). This adapter
/// therefore has no fused path — it always routes output through the
/// non-fused `pending` buffer. The fused path will be added in Wave 5
/// alongside the operator-side changes that enable it.
///
/// ## TODO(Wave 5 follow-up)
///
/// Per `docs/design/execution-model.md §4.1`, the adapter should gain:
/// - **Output-batch accumulation**: collect per-entity output batches into
///   a target-row-count buffer before returning to the caller, avoiding the
///   "one-row `RecordBatch` per entity pathology" for high-entity pipelines.
/// - **Cooperative cancellation**: check the [`CancellationToken`] from
///   `QueryContext` between entities so a cancelled query observes a
///   per-entity latency bound. The token is now plumbed through
///   `bind_physical` (TASK-510); the adapter just needs to read it
///   between sub-batches.
///
/// `SequenceMatchAdapter` has the same gap and will be updated at the same
/// time.
struct EntityOperatorAdapter<Op: EntityOperator> {
    operator: Op,
    child: Box<dyn PhysicalOperator>,
    output_schema: OperatorSchema,
    /// Index of the entity-key column inside the *child's* output batches.
    /// Used to detect entity-id transitions so the adapter can finalize
    /// one entity and open the next.
    entity_id_col_idx: usize,
    current_entity: Option<EntityId>,
    current_state: Option<Op::State>,
    /// Non-fused output batches waiting to be returned.
    pending: VecDeque<RecordBatch>,
    /// Set once the child has been fully drained and the last entity
    /// finalized.
    exhausted: bool,
    /// Per-query warning sink. Drained at every `finalize_entity` so
    /// per-entity diagnostics (cap-exceeded, etc.) reach the
    /// `ExecutionResult.warnings` surface. `None` for stand-alone
    /// tests constructed via the legacy single-arg path.
    warnings: Option<WarningSink>,
}

impl<Op: EntityOperator> EntityOperatorAdapter<Op> {
    fn new_with_sink(
        operator: Op,
        child: Box<dyn PhysicalOperator>,
        output_schema: OperatorSchema,
        entity_id_col_idx: usize,
        warnings: Option<WarningSink>,
    ) -> Self {
        Self {
            operator,
            child,
            output_schema,
            entity_id_col_idx,
            current_entity: None,
            current_state: None,
            pending: VecDeque::new(),
            exhausted: false,
            warnings,
        }
    }

    /// Finalize the in-flight entity: drain warnings, call `finish_entity`,
    /// and buffer the result.
    ///
    /// Unlike `SequenceMatchAdapter::finalize_entity`, the entity-id column
    /// is filled by the operator itself (Wave 4 operators always write the
    /// entity id into their output rows). The id is still required as an
    /// argument because [`EntityOperator::take_pending_warnings`] uses it
    /// to attribute the warning when the operator's state does not carry
    /// the id. See `docs/design/engine/cancellation.md` §7.4.
    fn finalize_entity(&mut self, entity: &EntityId, mut state: Op::State) -> Result<()> {
        if let Some(sink) = &self.warnings {
            sink.record_many(self.operator.take_pending_warnings(&mut state, entity));
        }
        if let Some(batch) = self.operator.finish_entity(state) {
            self.pending.push_back(batch);
        }
        Ok(())
    }

    /// Split `child_batch` at entity boundaries, routing each slice through
    /// `process_sub_batch`.
    fn process_child_batch(&mut self, child_batch: RecordBatch) -> Result<()> {
        let entity_col = child_batch.column(self.entity_id_col_idx).clone();
        let num_rows = child_batch.num_rows();
        let mut start = 0;

        while start < num_rows {
            let row_entity = extract_entity_id(&entity_col, start);

            // Detect entity boundary.
            if self.current_entity.as_ref() != Some(&row_entity) {
                if let (Some(prev_entity), Some(prev_state)) =
                    (self.current_entity.take(), self.current_state.take())
                {
                    self.finalize_entity(&prev_entity, prev_state)?;
                }
                let new_state = self.operator.create_state(&row_entity);
                self.current_entity = Some(row_entity.clone());
                self.current_state = Some(new_state);
            }

            let end = find_entity_end(&entity_col, start, &row_entity, num_rows);
            let sub_batch = child_batch.slice(start, end - start);
            let state = self.current_state.as_mut().expect("state must be set");
            self.operator.process_sub_batch(state, &sub_batch);
            start = end;
        }

        Ok(())
    }
}

impl<Op: EntityOperator> PhysicalOperator for EntityOperatorAdapter<Op> {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // Return any buffered output first.
            if let Some(batch) = self.pending.pop_front() {
                return Ok(Some(batch));
            }

            if self.exhausted {
                return Ok(None);
            }

            match self.child.next_batch()? {
                None => {
                    // Child exhausted: finalize the last in-flight entity.
                    if let (Some(entity), Some(state)) =
                        (self.current_entity.take(), self.current_state.take())
                    {
                        self.finalize_entity(&entity, state)?;
                    }
                    self.exhausted = true;
                    // Loop once more to drain `pending`.
                }
                Some(batch) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.process_child_batch(batch)?;
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SampleFilterOperator — fallback SAMPLE filter (TASK-438 CP2)
// ─────────────────────────────────────────────────────────────────────────────

/// Fallback [`PhysicalOperator`] that applies an entity-level SAMPLE filter
/// above a stateful pipeline stage.
///
/// When the planner's sample-pushdown pass (TASK-430) cannot push SAMPLE
/// into the scan layer — because a stateful operator (SequenceMatch,
/// Sessionize, EventSelect, Attribute) sits between the SAMPLE node and
/// the source scan — the `SamplePhysical` descriptor remains in the plan
/// tree and the engine bind step materialises it here.
///
/// ## Entity-level semantics
///
/// The filter evaluates each row's entity-id column via xxHash64 against a
/// fraction threshold, identically to the scan-layer
/// [`bqlite_storage::SampleFilter`]. Because the child output is
/// entity-sorted, consecutive rows with the same entity id produce the same
/// hash result: either all rows for that entity pass or all fail, preserving
/// the same deterministic entity-level sampling guarantee as the pushed-down
/// variant.
struct SampleFilterOperator {
    filter: bqlite_storage::SampleFilter,
    child: Box<dyn PhysicalOperator>,
    /// Index of the entity-key column in the *child's* output batches.
    entity_id_col_idx: usize,
    output_schema: OperatorSchema,
}

impl SampleFilterOperator {
    fn new(
        filter: bqlite_storage::SampleFilter,
        child: Box<dyn PhysicalOperator>,
        entity_id_col_idx: usize,
        output_schema: OperatorSchema,
    ) -> Self {
        Self {
            filter,
            child,
            entity_id_col_idx,
            output_schema,
        }
    }
}

impl PhysicalOperator for SampleFilterOperator {
    fn output_schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Short-circuit: fraction == 0.0 → no rows survive.
        if self.filter.is_empty_set() {
            return Ok(None);
        }

        loop {
            let batch = match self.child.next_batch()? {
                None => return Ok(None),
                Some(b) => b,
            };
            if batch.num_rows() == 0 {
                continue;
            }

            // Short-circuit: fraction == 1.0 → all rows survive.
            if self.filter.is_pass_through() {
                return Ok(Some(batch));
            }

            // Apply the entity-level hash filter row-by-row.
            let entity_col = batch.column(self.entity_id_col_idx);
            let mask = self.filter.apply_to_array(entity_col.as_ref())?;
            let filtered =
                compute::filter_record_batch(&batch, &mask).map_err(BqliteError::Arrow)?;

            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
            // All rows in this batch were filtered out — pull the next batch.
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Plan-tree helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Walk the input plan tree to find the entity key column name declared by
/// the innermost `Scan` node.
///
/// The `SequenceMatch` logical lowering renames the entity key column to
/// `"entity_id"` in the operator's *output* schema, but the *child*
/// operator (Scan / Filter) still exposes the column under its original
/// name (e.g. `"user_id"`). The adapter must detect entity boundaries by
/// looking for that original name in the child's output batches.
///
/// Wave 4 passthrough operators (`Sessionize`, `EventSelect`, `Attribute`,
/// `Sample`, `SubqueryFilter`) carry the entity key column through from
/// their input unchanged, so the function recurses into their children.
/// `MergeSources` normalises all entity-key columns to `"entity_id"` in
/// its output per cohorts-aliases-joins.md §3.8; the `_ => "entity_id"`
/// fallback covers that case (and any future unrecognised node shapes).
fn entity_key_col_name(plan: &PhysicalPlan) -> &str {
    match plan {
        PhysicalPlan::Scan(scan) => scan.entity_key_col.as_str(),
        PhysicalPlan::Filter(filter) => entity_key_col_name(&filter.input),
        PhysicalPlan::Project(proj) => entity_key_col_name(&proj.input),
        PhysicalPlan::Limit(limit) => entity_key_col_name(&limit.input),
        // Wave 4 passthrough operators — entity key column name propagates
        // unchanged through these nodes.
        PhysicalPlan::Sessionize(sess) => entity_key_col_name(&sess.input),
        PhysicalPlan::EventSelect(es) => entity_key_col_name(&es.input),
        PhysicalPlan::Attribute(attr) => entity_key_col_name(&attr.input),
        PhysicalPlan::Sample(s) => entity_key_col_name(&s.input),
        PhysicalPlan::SubqueryFilter(sqf) => entity_key_col_name(&sqf.input),
        // For other plan shapes (SequenceMatch output, MergeSources output,
        // or any future forward-compat shape), fall back to the normalised
        // column name used in the match / merged output schema.
        _ => "entity_id",
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Column-level helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Extract an [`EntityId`] from a column array at the given row index.
///
/// Dispatches on the Arrow data type: `Int64` → `EntityId::Int`,
/// everything else (assumed `Utf8View`) → `EntityId::String`.
fn extract_entity_id(col: &ArrayRef, row: usize) -> EntityId {
    match col.data_type() {
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("entity_id column declared as Int64 must be Int64Array");
            EntityId::Int(arr.value(row))
        }
        _ => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("entity_id column must be StringViewArray for non-Int64 type");
            EntityId::String(arr.value(row).to_owned())
        }
    }
}

/// Return the exclusive end index of the contiguous run of `entity` rows
/// starting at `start` in `col`.
fn find_entity_end(col: &ArrayRef, start: usize, entity: &EntityId, total: usize) -> usize {
    let mut end = start + 1;
    match entity {
        EntityId::String(s) => {
            let arr = col
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("string entity_id column must be StringViewArray");
            while end < total && arr.value(end) == s.as_str() {
                end += 1;
            }
        }
        EntityId::Int(n) => {
            let arr = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int entity_id column must be Int64Array");
            while end < total && arr.value(end) == *n {
                end += 1;
            }
        }
    }
    end
}

/// Replace the placeholder `entity_id` column in a match output batch with
/// the actual entity id value.
///
/// `build_output_batch` (in `bqlite-operators::matcher::output`) fills the
/// `entity_id` column with an empty string or zero. This function rebuilds
/// that column with the real entity id before the batch is returned to the
/// caller.
///
/// Returns the original batch unchanged if no `entity_id` field is present.
fn fill_entity_id(batch: RecordBatch, entity: &EntityId) -> Result<RecordBatch> {
    let schema = batch.schema();
    let entity_id_idx = match schema.fields().iter().position(|f| f.name() == "entity_id") {
        Some(idx) => idx,
        None => return Ok(batch),
    };

    let num_rows = batch.num_rows();
    let new_col: ArrayRef = match entity {
        EntityId::String(s) => {
            let mut builder = StringViewBuilder::with_capacity(num_rows);
            for _ in 0..num_rows {
                builder.append_value(s.as_str());
            }
            Arc::new(builder.finish())
        }
        EntityId::Int(n) => {
            let mut builder = Int64Array::builder(num_rows);
            for _ in 0..num_rows {
                builder.append_value(*n);
            }
            Arc::new(builder.finish())
        }
    };

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[entity_id_idx] = new_col;
    RecordBatch::try_new(schema, columns).map_err(BqliteError::from)
}

// ─────────────────────────────────────────────────────────────────────────────
// Bind entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Bind a plain-data [`PhysicalPlan`] into an executable
/// `Box<dyn PhysicalOperator>` tree rooted at the plan's top node.
///
/// Each descriptor arm is responsible for wiring in runtime handles
/// (`Database` for segment readers, the shared
/// [`CancellationToken`], later the memory budget and metrics sink).
/// The returned operator is ready to drive with `open → next_batch* →
/// close` per the [`PhysicalOperator`] lifecycle contract.
///
/// ## Data-plane descriptors
///
/// `Scan`, `Filter`, `Project`, `Limit` — recursively bind children
/// and construct the corresponding operator.
///
/// ## Wave 3 descriptors (TASK-323)
///
/// `Aggregate`, `Sort`, `Distinct` — bind the child and construct the
/// corresponding stateless operator. `SequenceMatch` — bind the child
/// and wrap in a [`SequenceMatchAdapter`] that drives the per-entity
/// `EntityOperator` protocol.
///
/// ## DDL descriptors
///
/// `CreateTable`, `DropTable`, `AlterTableAddColumn` — execute the
/// DDL mutation against the manifest during bind and return an empty
/// [`ResultOperator`].
///
/// ## Metadata descriptors
///
/// `Describe` — look up the table and build a four-column result
/// batch. `Explain` — format the plan tree as a single-column batch.
///
/// # Errors
///
/// Propagates any error from operator construction, DDL execution,
/// or catalog lookup.
pub fn bind_physical(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
) -> Result<Box<dyn PhysicalOperator>> {
    let mut cohorts = CohortCache::default();
    bind_physical_with_cache(plan, db, ctx, &mut cohorts)
}

/// Bind a `PhysicalPlan` while threading a [`CohortCache`] through every
/// recursive call so that identical cohort subqueries — produced by
/// repeated `IN alias` references or duplicate `IN QUERY (...)` shapes —
/// materialize exactly once per top-level [`Engine::query`] invocation.
///
/// See `docs/design/language/cohorts-aliases-joins.md` §2.5 + §2.11
/// (caching), §4.1 (hash-set probe), §4.2 (cohort materialization at
/// query start).
fn bind_physical_with_cache(
    plan: &PhysicalPlan,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    match plan {
        // ── Data-plane operators ─────────────────────────────────
        PhysicalPlan::Scan(scan) => bind_scan(scan, db, ctx),

        PhysicalPlan::Filter(filter) => {
            let child = bind_physical_with_cache(&filter.input, db, ctx, cohorts)?;
            Ok(Box::new(FilterOperator::new(
                child,
                filter.predicate.clone(),
                filter.tile_size,
            )))
        }

        PhysicalPlan::Project(project) => {
            let child = bind_physical_with_cache(&project.input, db, ctx, cohorts)?;
            Ok(Box::new(ProjectOperator::from_physical_items(
                child,
                project.expressions.clone(),
                project.output_schema.clone(),
            )))
        }

        PhysicalPlan::Limit(limit) => {
            let child = bind_physical_with_cache(&limit.input, db, ctx, cohorts)?;
            Ok(Box::new(LimitOperator::new(child, limit.count)))
        }

        // ── Wave 3 operators (TASK-323) ───────────────────────────
        PhysicalPlan::Sort(sort) => {
            let child = bind_physical_with_cache(&sort.input, db, ctx, cohorts)?;
            Ok(Box::new(SortOperator::with_spill(
                child,
                sort.keys.clone(),
                sort.max_rows,
                ctx.cancellation().clone(),
                ctx.memory().clone(),
                ctx.spill_fs().cloned(),
                ctx.spill_query_id(),
            )))
        }

        PhysicalPlan::Distinct(distinct) => {
            let child = bind_physical_with_cache(&distinct.input, db, ctx, cohorts)?;
            Ok(Box::new(DistinctOperator::new(
                child,
                distinct.max_groups,
                ctx.cancellation().clone(),
            )))
        }

        PhysicalPlan::Aggregate(agg) => {
            let child = bind_physical_with_cache(&agg.input, db, ctx, cohorts)?;
            Ok(Box::new(HashAggregateOperator::new(
                child,
                agg.aggregates.clone(),
                agg.group_by.clone(),
                agg.max_groups,
                agg.output_schema.clone(),
            )))
        }

        PhysicalPlan::SequenceMatch(seq) => {
            let child = bind_physical_with_cache(&seq.input, db, ctx, cohorts)?;
            Ok(Box::new(SequenceMatchAdapter::new_with_sink(
                seq,
                child,
                Some(ctx.warnings().clone()),
            )?))
        }

        // ── Wave 4 cohort runtime (TASK-437) ──────────────────────
        PhysicalPlan::SubqueryFilter(sqf) => bind_subquery_filter(sqf, db, ctx, cohorts),

        // ── DDL ──────────────────────────────────────────────────
        PhysicalPlan::CreateTable(ct) => {
            execute_create_table(ct, db)?;
            Ok(Box::new(ResultOperator::empty(ct.output_schema.clone())))
        }

        PhysicalPlan::DropTable(dt) => {
            execute_drop_table(dt, db)?;
            Ok(Box::new(ResultOperator::empty(dt.output_schema.clone())))
        }

        PhysicalPlan::AlterTableAddColumn(alter) => {
            execute_alter_table_add_column(alter, db)?;
            Ok(Box::new(ResultOperator::empty(alter.output_schema.clone())))
        }

        // ── Metadata ─────────────────────────────────────────────
        PhysicalPlan::Describe(desc) => {
            let batch = build_describe_batch(desc, db)?;
            Ok(Box::new(ResultOperator::new(
                desc.output_schema.clone(),
                vec![batch],
            )))
        }

        PhysicalPlan::Explain(explain) => {
            let batch = build_explain_batch(explain)?;
            Ok(Box::new(ResultOperator::new(
                explain.output_schema.clone(),
                vec![batch],
            )))
        }

        // ── DML ──────────────────────────────────────────────────
        PhysicalPlan::Insert(insert) => {
            crate::ingest::execute_insert(insert, db)?;
            Ok(Box::new(ResultOperator::empty(
                insert.output_schema.clone(),
            )))
        }

        // ── Wave 4 EntityOperator-based operators (TASK-438) ─────
        PhysicalPlan::Sessionize(sess) => bind_sessionize(sess, db, ctx, cohorts),

        PhysicalPlan::EventSelect(es) => bind_event_select(es, db, ctx, cohorts),

        PhysicalPlan::Attribute(attr) => bind_attribute(attr, db, ctx, cohorts),

        // ── Wave 4 Sample + MergeSources (TASK-438 CP2) ──────────
        PhysicalPlan::Sample(s) => bind_sample(s, db, ctx, cohorts),

        PhysicalPlan::MergeSources(merge) => bind_merge_sources(merge, db, ctx),

        // DELETE is intentionally not bound through this path —
        // `Engine::query` dispatches it to `crate::delete` directly so
        // the `rows_affected` count can flow into `ExecutionResult`
        // out-of-band (deletes.md §11). Reaching this arm means a
        // caller bypassed `Engine::query`, which is unsupported in
        // Wave 4.
        PhysicalPlan::Delete(_) => Err(BqliteError::Execution(
            "PhysicalPlan::Delete must be executed via Engine::query, \
             not bind_physical (deletes.md §11)"
                .into(),
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Cohort materialization for SubqueryFilter (TASK-437)
// ─────────────────────────────────────────────────────────────────────────────

/// Per-`bind_physical` cache of materialized cohorts.
///
/// Keyed by the inner subquery [`PhysicalPlan`]; identical inner plans
/// share one `Arc<CohortHashSet>` per top-level execution. Two `IN alias`
/// references that point at the same alias body — and two `IN QUERY (...)`
/// expressions whose inner pipelines lower to structurally equal physical
/// plans — therefore both materialize exactly once
/// (cohorts-aliases-joins.md §2.5 + §2.11).
///
/// Stored as a `Vec` of `(plan, cohort)` pairs and looked up by linear
/// scan with [`PhysicalPlan`]'s derived `PartialEq`. Realistic queries
/// reference at most a handful of cohorts, so the linear scan is cheaper
/// than hashing entire plan trees. If a future workload makes this a hot
/// path, a structural-hash key for `PhysicalPlan` is a Wave 5 follow-up.
#[derive(Default)]
struct CohortCache {
    entries: Vec<(PhysicalPlan, Arc<CohortHashSet>)>,
}

impl CohortCache {
    /// Return the cached cohort for `subquery` if one is present.
    fn get(&self, subquery: &PhysicalPlan) -> Option<Arc<CohortHashSet>> {
        self.entries
            .iter()
            .find(|(plan, _)| plan == subquery)
            .map(|(_, c)| Arc::clone(c))
    }

    /// Install a freshly materialized cohort under `subquery`'s key.
    fn insert(&mut self, subquery: PhysicalPlan, cohort: Arc<CohortHashSet>) {
        self.entries.push((subquery, cohort));
    }
}

/// Bind a [`SubqueryFilterPhysical`] — materialize its inner subquery
/// at query start (cohorts-aliases-joins.md §4.2), wire the resulting
/// `Arc<CohortHashSet>` into a [`SubqueryFilterOperator`], and recurse
/// into the outer input.
///
/// Cycle detection is handled at plan time by `resolve_alias` in
/// `crates/bqlite-planner/src/logical.rs`; by the time a `PhysicalPlan`
/// reaches the engine bind step it is guaranteed cycle-free, so the
/// runtime walk does not need to defend against infinite recursion.
fn bind_subquery_filter(
    sqf: &SubqueryFilterPhysical,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    let cohort = match cohorts.get(&sqf.subquery) {
        Some(existing) => existing,
        None => {
            // Materialize: bind the inner subquery (which may itself
            // contain SubqueryFilter — those recursive cohort lookups
            // share the same cache, so nested cohorts also materialize
            // once per top-level execution).
            let mut op = bind_physical_with_cache(&sqf.subquery, db, ctx, cohorts)?;
            let drive_result = drive_cohort_subquery(op.as_mut());
            // Match `Engine::query`'s "primary error wins" cleanup
            // convention: close the inner operator on both happy and
            // sad paths so file handles release promptly.
            let close_result = op.close();
            let batches = drive_result?;
            close_result?;
            let cohort = Arc::new(CohortHashSet::from_batches(
                sqf.subquery.output_schema(),
                batches,
                ctx.memory().as_ref(),
            )?);
            cohorts.insert((*sqf.subquery).clone(), Arc::clone(&cohort));
            cohort
        }
    };

    let child = bind_physical_with_cache(&sqf.input, db, ctx, cohorts)?;
    Ok(Box::new(SubqueryFilterOperator::new(
        child,
        sqf.lhs_columns.clone(),
        cohort,
    )?))
}

/// Open a freshly bound subquery operator and pull every batch into a
/// `Vec<RecordBatch>`. The caller is responsible for `close` on both
/// the happy and sad paths.
fn drive_cohort_subquery(op: &mut dyn PhysicalOperator) -> Result<Vec<RecordBatch>> {
    op.open()?;
    let mut batches = Vec::new();
    while let Some(b) = op.next_batch()? {
        batches.push(b);
    }
    Ok(batches)
}

fn bind_scan(
    scan: &ScanPhysical,
    db: &Database,
    ctx: &QueryContext,
) -> Result<Box<dyn PhysicalOperator>> {
    let reader_range = scan.reader_range.unwrap_or_else(TimeRange::unbounded);

    // `Database::segment_reader_for_time_range` returns a manifest-backed
    // `ManifestSegmentReader` that enumerates only live segments that overlap
    // `reader_range`. We hand it to the scan as an `Arc` so later waves can
    // share ownership across parallel shard-tasks.
    let reader_box: Box<dyn SegmentReader> =
        db.segment_reader_for_time_range(&scan.table, reader_range)?;
    let reader: Arc<dyn SegmentReader> = Arc::from(reader_box);

    // Thread the descriptor's projection and pushed predicates into
    // the scan operator. Both passes (TASK-227 predicate pushdown and
    // TASK-228 column pruning) run unconditionally in `plan()` before
    // `bind_scan` is reached, so `projected_columns` and
    // `scan_predicates` are already populated in the normal engine
    // path. `projected_columns` is empty only for manually constructed
    // `ScanPhysical` descriptors (e.g., unit tests that bypass `plan()`),
    // in which case `ScanOperator::new` falls back to `ColumnProjection::all()`.
    let mut scan_predicates = scan.scan_predicates.clone();
    // Keep the widened reader window visible to the scan so later MATCH
    // steps can complete after the source-range end. Step-0 entry is gated
    // separately inside the matcher using the source scan's `query_range`.
    scan_predicates.extend(build_time_range_predicates(scan, reader_range)?);

    // Per `docs/design/storage/deletes.md` §6, every query observes a
    // single tombstone snapshot taken once at bind time. Walk the
    // segments visible to the reader, collect the unique
    // `(window_id, shard_id)` pairs they live in, and load the
    // tombstones for that exact set. The snapshot is shared via `Arc`
    // so later waves that fan out one scan operator per shard-task
    // observe the same epoch.
    //
    // `load_tombstone_snapshot` returns an empty entry for any
    // `(window, shard)` whose tombstone file is missing — the common
    // path on a database with no DELETEs is therefore zero I/O for
    // tombstone resolution.
    let tombstones = load_scan_tombstones(reader.as_ref(), &scan.table, db)?;

    let mut op = ScanOperator::with_tombstones(
        reader.clone(),
        &scan.projected_columns,
        scan_predicates,
        ctx.cancellation().clone(),
        tombstones,
    )?;

    // Entity-level SAMPLE pushdown (TASK-430). When the planner's
    // sample-pushdown pass attaches a `SamplePushdown` to the scan,
    // materialize the `SampleFilter` against the live table schema so
    // the operator can evaluate the xxHash64 threshold per row. The
    // fraction has already been validated in `[0.0, 1.0]` at lowering
    // time (see `lower_sample`), so `from_pushdown` can only fail on
    // an unsupported entity-id type — unreachable in the production
    // pipeline.
    if let Some(sample) = &scan.sample {
        let filter = bqlite_storage::SampleFilter::from_pushdown(
            sample.fraction,
            sample.seed,
            reader.schema(),
        )?;
        op.with_sample_filter(Arc::new(filter));
    }

    Ok(Box::new(op))
}

/// Collect the unique `(window_id, shard_id)` pairs the reader will
/// touch and load the per-query tombstone snapshot for that exact set.
///
/// Falls back to an empty snapshot when the reader yields zero
/// segments — there is nothing to tombstone, so we save the disk
/// walk.
fn load_scan_tombstones(
    reader: &dyn SegmentReader,
    table: &str,
    db: &Database,
) -> Result<Arc<bqlite_storage::TombstoneSnapshot>> {
    use std::collections::HashSet;
    let mut targets: HashSet<(u32, u16)> = HashSet::new();
    for handle in reader.segments() {
        let h = handle?;
        let window = u32::try_from(h.window_id).map_err(|_| {
            bqlite_core::BqliteError::Execution(format!(
                "bind_scan: segment_id {} has window_id {} that overflows u32",
                h.segment_id, h.window_id
            ))
        })?;
        let shard = u16::try_from(h.shard_id).map_err(|_| {
            bqlite_core::BqliteError::Execution(format!(
                "bind_scan: segment_id {} has shard_id {} that overflows u16",
                h.segment_id, h.shard_id
            ))
        })?;
        targets.insert((window, shard));
    }
    if targets.is_empty() {
        return Ok(Arc::new(bqlite_storage::TombstoneSnapshot::empty()));
    }
    let mut targets: Vec<(u32, u16)> = targets.into_iter().collect();
    targets.sort_unstable();
    let snap = db.load_tombstone_snapshot(table, &targets)?;
    Ok(Arc::new(snap))
}

/// Build row-level timestamp predicates from a resolved `TimeRange`.
///
/// Returns an empty vec for [`TimeRange::unbounded`] so callers can extend
/// `scan_predicates` unconditionally.
fn build_time_range_predicates(
    scan: &ScanPhysical,
    time_range: TimeRange,
) -> Result<Vec<CompiledExpr>> {
    if time_range == TimeRange::unbounded() {
        return Ok(Vec::new());
    }

    let (ts_index, _) = scan
        .output_schema
        .column(&scan.timestamp_col)
        .ok_or_else(|| {
            BqliteError::Schema(format!(
                "scan bind: timestamp column `{}` missing from output schema",
                scan.timestamp_col
            ))
        })?;

    let timestamp_column = CompiledExpr {
        node: CompiledNode::Column {
            index: ts_index,
            name: scan.timestamp_col.clone(),
        },
        result_type: BqlType::Timestamp,
        nullable: false,
    };

    Ok(vec![
        compiled_timestamp_compare(
            timestamp_column.clone(),
            CompareOp::GreaterOrEqual,
            time_range.start.as_nanos(),
            ArrowKernelId::GeTimestamp,
        ),
        compiled_timestamp_compare(
            timestamp_column,
            CompareOp::Less,
            time_range.end.as_nanos(),
            ArrowKernelId::LtTimestamp,
        ),
    ])
}

/// Build a single compiled timestamp comparison predicate.
fn compiled_timestamp_compare(
    column: CompiledExpr,
    op: CompareOp,
    literal_ns: i64,
    kernel: ArrowKernelId,
) -> CompiledExpr {
    CompiledExpr {
        node: CompiledNode::Compare {
            op,
            left: Box::new(column),
            right: Box::new(CompiledExpr {
                node: CompiledNode::Literal(PropertyValue::Timestamp(literal_ns)),
                result_type: BqlType::Timestamp,
                nullable: false,
            }),
            kernel: CompareKernel::ArrowKernel(kernel),
        },
        result_type: BqlType::Bool,
        nullable: false,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 EntityOperator bind helpers (TASK-438 CP1)
// ─────────────────────────────────────────────────────────────────────────────

/// Shared logic: find the entity key column index in a child operator's
/// output schema, returning a typed `BqliteError::Schema` on failure.
fn resolve_entity_key_col(
    child: &dyn PhysicalOperator,
    ek_col_name: &str,
    operator_label: &str,
) -> Result<usize> {
    child
        .output_schema()
        .columns()
        .iter()
        .position(|c| c.name == ek_col_name)
        .ok_or_else(|| {
            BqliteError::Schema(format!(
                "{operator_label}: entity key column '{ek_col_name}' \
                 not found in child output schema"
            ))
        })
}

/// Bind a [`SessionizePhysical`] descriptor into a
/// [`EntityOperatorAdapter`]<[`SessionizeOperator`]>.
///
/// The `SessionizeOperator` expects its input schema to expose the entity
/// key column under the name it was given in the source table's schema
/// (commonly `"entity_id"`). `entity_key_col_name` walks the plan tree
/// to find that name, which the adapter uses to detect entity boundaries.
fn bind_sessionize(
    sess: &SessionizePhysical,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    let child = bind_physical_with_cache(&sess.input, db, ctx, cohorts)?;
    let ek_col_name = entity_key_col_name(&sess.input);
    let entity_id_col_idx =
        resolve_entity_key_col(child.as_ref(), ek_col_name, "SessionizeAdapter")?;
    let operator = SessionizeOperator::new(sess);
    Ok(Box::new(EntityOperatorAdapter::new_with_sink(
        operator,
        child,
        sess.output_schema.clone(),
        entity_id_col_idx,
        Some(ctx.warnings().clone()),
    )))
}

/// Bind an [`EventSelectPhysical`] descriptor into a
/// [`EntityOperatorAdapter`]<[`EventSelectOperator`]>.
///
/// The operator resolves column indices against the planner's view of
/// the child's output schema (`es.input.output_schema()`). Same-`ts`
/// tie-breaking relies on the scan runtime's `(entity_id, ts,
/// __seq_id)` emission order (positional tie-breaking) — see the doc
/// on `EventSelectInputMap` — so it no longer reads `__seq_id` values
/// out of batches, and the earlier runtime-vs-planner schema mismatch
/// for `__seq_id` is irrelevant here.
fn bind_event_select(
    es: &EventSelectPhysical,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    let child = bind_physical_with_cache(&es.input, db, ctx, cohorts)?;
    let ek_col_name = entity_key_col_name(&es.input);
    let entity_id_col_idx =
        resolve_entity_key_col(child.as_ref(), ek_col_name, "EventSelectAdapter")?;
    let operator = EventSelectOperator::new(es, es.input.output_schema());
    Ok(Box::new(EntityOperatorAdapter::new_with_sink(
        operator,
        child,
        es.output_schema.clone(),
        entity_id_col_idx,
        Some(ctx.warnings().clone()),
    )))
}

/// Bind an [`AttributePhysical`] descriptor into a
/// [`EntityOperatorAdapter`]<[`AttributeOperator`]>.
///
/// `AttributeOperator::from_physical` validates the descriptor (non-empty
/// conversion/touchpoint event lists, String touchpoint_key, non-negative
/// window, fused_aggregate == None) and returns a typed error on any
/// violation.
fn bind_attribute(
    attr: &AttributePhysical,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    let child = bind_physical_with_cache(&attr.input, db, ctx, cohorts)?;
    let ek_col_name = entity_key_col_name(&attr.input);
    let entity_id_col_idx =
        resolve_entity_key_col(child.as_ref(), ek_col_name, "AttributeAdapter")?;
    let operator = AttributeOperator::from_physical(attr)?;
    Ok(Box::new(EntityOperatorAdapter::new_with_sink(
        operator,
        child,
        attr.output_schema.clone(),
        entity_id_col_idx,
        Some(ctx.warnings().clone()),
    )))
}

// ─────────────────────────────────────────────────────────────────────────────
// Wave 4 Sample + MergeSources bind helpers (TASK-438 CP2)
// ─────────────────────────────────────────────────────────────────────────────

/// Bind a [`SamplePhysical`] descriptor into a [`SampleFilterOperator`].
///
/// This arm is reached only when the planner's sample-pushdown pass
/// (TASK-430) cannot push SAMPLE into the scan layer — typically because a
/// stateful operator (SequenceMatch, Sessionize, EventSelect, Attribute)
/// sits between the SAMPLE node and the source scan. In all other cases
/// the sample has been folded into the `ScanPhysical::sample` field and
/// the bind step sees only a bare `Scan` node.
fn bind_sample(
    sample: &SamplePhysical,
    db: &mut Database,
    ctx: &QueryContext,
    cohorts: &mut CohortCache,
) -> Result<Box<dyn PhysicalOperator>> {
    let child = bind_physical_with_cache(&sample.input, db, ctx, cohorts)?;
    let ek_col_name = entity_key_col_name(&sample.input);

    // Resolve the entity-key column index and type in a single schema walk.
    let (entity_id_col_idx, entity_type) = child
        .output_schema()
        .column(ek_col_name)
        .map(|(idx, coldef)| (idx, coldef.bql_type.clone()))
        .ok_or_else(|| {
            BqliteError::Schema(format!(
                "SampleFilter: entity key column '{ek_col_name}' not found in child schema"
            ))
        })?;

    let filter =
        bqlite_storage::SampleFilter::new(sample.fraction, sample.seed, ek_col_name, entity_type)?;

    Ok(Box::new(SampleFilterOperator::new(
        filter,
        child,
        entity_id_col_idx,
        sample.output_schema.clone(),
    )))
}

/// Bind a [`MergeSourcesPhysical`] descriptor into a
/// [`bqlite_operators::scan::MergeSourcesOperator`].
///
/// Each sub-table in `merge.tables` is bound independently into a
/// `ScanOperator`; the operator performs an N-ary k-way merge over all
/// sub-scans, emitting a unified entity-sorted event stream with a
/// `__source_table_id` discriminator column.
fn bind_merge_sources(
    merge: &MergeSourcesPhysical,
    db: &mut Database,
    ctx: &QueryContext,
) -> Result<Box<dyn PhysicalOperator>> {
    let sub_ops: Vec<Box<dyn PhysicalOperator>> = merge
        .tables
        .iter()
        .map(|scan| bind_scan(scan, db, ctx))
        .collect::<Result<Vec<_>>>()?;

    let sub_entity_key_cols: Vec<String> = merge
        .tables
        .iter()
        .map(|s| s.entity_key_col.clone())
        .collect();

    let sub_ts_cols: Vec<String> = merge
        .tables
        .iter()
        .map(|s| s.timestamp_col.clone())
        .collect();

    Ok(Box::new(bqlite_operators::scan::MergeSourcesOperator::new(
        sub_ops,
        sub_entity_key_cols,
        sub_ts_cols,
        merge.output_schema.clone(),
        merge.table_id_map.clone(),
        ctx.cancellation().clone(),
    )?))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow::array::Array;
    use bqlite_core::OperatorSchema;
    use bqlite_planner::{plan, PhysicalPlan, ScanPhysical};
    use bqlite_storage::{bootstrap_events_schema, Database};

    use super::*;

    /// Per-test unique temp directory. Mirrors the pattern used in
    /// `bqlite_storage::database::tests` — process PID + monotonic
    /// counter is enough for in-process uniqueness without pulling
    /// `tempfile` into the dev-dependency closure.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        path: PathBuf,
    }

    impl Scratch {
        fn new(label: &str) -> Self {
            let seq = SEQ.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let mut path = std::env::temp_dir();
            path.push(format!("bqlite-engine-bind-{label}-{pid}-{seq}"));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn create_db_with_bootstrap(path: &Path) -> Database {
        let mut db = Database::create(path).expect("create db");
        db.create_table("events".into(), bootstrap_events_schema())
            .expect("create events");
        db
    }

    fn bootstrap_scan_descriptor() -> PhysicalPlan {
        PhysicalPlan::Scan(ScanPhysical {
            table: "events".to_string(),
            query_range: None,
            reader_range: None,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema: OperatorSchema::from_table(&bootstrap_events_schema()),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        })
    }

    #[test]
    fn bind_scan_produces_a_drivable_operator() {
        let scratch = Scratch::new("happy");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = bootstrap_scan_descriptor();

        let mut op = bind_physical(&descriptor, &mut db, &QueryContext::unbounded())
            .expect("bind must succeed");

        // Full PhysicalOperator lifecycle — the smoke test (TASK-123)
        // will drive this exact path through `Engine::query`.
        op.open().expect("open should succeed");
        assert!(
            op.next_batch()
                .expect("next_batch should succeed")
                .is_none(),
            "bootstrap events table has zero segments so the first pull must exhaust"
        );
        // Exhaustion is sticky.
        assert!(op.next_batch().unwrap().is_none());
        op.close().expect("close should succeed");
    }

    #[test]
    fn bind_scan_output_schema_matches_descriptor_with_system_columns() {
        // Per `docs/design/storage/system-columns.md` §4.1, the scan
        // operator's `output_schema` now matches the planner's
        // descriptor: declared columns followed by the implicit
        // `__seq_id` / `__batch_id` system columns synthesised at
        // segment-read time from the footer's `seq_id_range` and
        // `batch_id`. Pre-TASK-508 the bound operator narrowed to
        // declared columns only because the reader did not synthesise
        // system columns yet; that carve-out is now closed.
        let scratch = Scratch::new("schema");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = bootstrap_scan_descriptor();

        let op = bind_physical(&descriptor, &mut db, &QueryContext::unbounded())
            .expect("bind must succeed");

        let op_names: Vec<&str> = op
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            op_names,
            vec!["entity_id", "ts", "event_type", "__seq_id", "__batch_id"]
        );

        let descriptor_names: Vec<&str> = descriptor
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(descriptor_names, op_names);
    }

    #[test]
    fn bind_scan_reports_unknown_table_through_plan_error() {
        let scratch = Scratch::new("unknown");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = PhysicalPlan::Scan(ScanPhysical {
            table: "ghost".to_string(),
            query_range: None,
            reader_range: None,
            scan_predicates: Vec::new(),
            projected_columns: Vec::new(),
            output_schema: OperatorSchema::from_table(&bootstrap_events_schema()),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        });

        match bind_physical(&descriptor, &mut db, &QueryContext::unbounded()) {
            Err(bqlite_core::BqliteError::Plan(msg)) => {
                assert!(msg.contains("ghost"), "error should name the table: {msg}");
            }
            Err(other) => panic!("expected Plan error for unknown table, got {other:?}"),
            Ok(_) => panic!("expected Plan error for unknown table, got Ok"),
        }
    }

    #[test]
    fn bind_physical_composes_with_planner_output() {
        // End-to-end spot check: run the Wave 1 pipeline (parse ->
        // plan -> bind) against a real bootstrap database and confirm
        // every stage hands off the expected shape. This duplicates
        // the smoke test coverage (TASK-123) in miniature so that a
        // regression in the bind step is localized to *this* file
        // rather than surfacing as a generic smoke-test failure.
        let scratch = Scratch::new("compose");
        let mut db = create_db_with_bootstrap(scratch.path());
        let mut stmts = bqlite_parser::parse("events").expect("parse events");
        assert_eq!(
            stmts.len(),
            1,
            "expected single statement, got {}",
            stmts.len()
        );
        let stmt = stmts.remove(0);
        let physical = {
            let catalog = db.catalog();
            plan(stmt, &catalog, 0).expect("plan events")
        };

        let mut op =
            bind_physical(&physical, &mut db, &QueryContext::unbounded()).expect("bind succeeds");
        op.open().unwrap();
        assert!(op.next_batch().unwrap().is_none());
        op.close().unwrap();
    }

    // ── TASK-244: end-to-end real-rows through manifest-backed reader ────

    /// Create a database with a 4-column events table suitable for
    /// INSERT VALUES round-trip tests.
    fn create_db_with_events_table(path: &Path) -> (Database, crate::Engine) {
        let mut db = Database::create(path).expect("create db");
        let engine = crate::Engine::new();
        engine
            .query(
                "CREATE TABLE events (\
                     user_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE, \
                     amount INT\
                 )",
                &mut db,
            )
            .expect("create events table");
        (db, engine)
    }

    #[test]
    fn insert_values_then_query_returns_real_rows() {
        // TASK-244 end-to-end: CREATE TABLE → INSERT VALUES → query
        // must return actual row data through the manifest-backed
        // reader, not zero rows from the old EmptySegmentReader stub.
        let scratch = Scratch::new("real-rows");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 42), \
                 ('bob',   1700000000100000000, 'view',  NULL)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");

        // Query the table — the scan operator drives through the
        // ManifestSegmentReader and returns real rows.
        let result = engine.query("events", &mut db).expect("query must succeed");
        assert_eq!(
            result.row_count(),
            2,
            "query must return 2 rows, not zero — the scan reads real segments"
        );

        // Collect entity names across all batches (rows may be split
        // across segments/shards).
        let mut entities: Vec<String> = Vec::new();
        let mut found_alice_amount = false;
        let mut found_bob_null = false;
        for batch in &result.rows {
            let entity_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("user_id column should be StringView");
            let amount_col = batch
                .column(3)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("amount column should be Int64");
            for i in 0..batch.num_rows() {
                let name = entity_col.value(i).to_string();
                if name == "alice" {
                    assert_eq!(amount_col.value(i), 42);
                    found_alice_amount = true;
                }
                if name == "bob" {
                    assert!(amount_col.is_null(i), "bob's amount must be NULL");
                    found_bob_null = true;
                }
                entities.push(name);
            }
        }
        entities.sort();
        assert_eq!(entities, vec!["alice", "bob"]);
        assert!(found_alice_amount, "alice's amount=42 must round-trip");
        assert!(found_bob_null, "bob's NULL amount must round-trip");
    }

    #[test]
    fn insert_values_then_where_returns_filtered_rows() {
        let scratch = Scratch::new("real-rows-filter");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 10), \
                 ('alice', 1700000000100000000, 'view',  20), \
                 ('bob',   1700000000200000000, 'click', 30)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");

        // BQL pipe syntax: table | where ...
        let result = engine
            .query("events | where event_type = 'click'", &mut db)
            .expect("filtered query");
        assert_eq!(
            result.row_count(),
            2,
            "WHERE event_type = 'click' should return 2 rows (alice + bob)"
        );
    }

    #[test]
    fn insert_values_then_limit_returns_bounded_rows() {
        let scratch = Scratch::new("real-rows-limit");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 10), \
                 ('bob',   1700000000100000000, 'view',  20), \
                 ('carol', 1700000000200000000, 'click', 30)",
                &mut db,
            )
            .expect("INSERT VALUES must succeed");

        let result = engine
            .query("events | limit 2", &mut db)
            .expect("query with LIMIT");
        assert_eq!(result.row_count(), 2, "LIMIT 2 should cap at 2 rows");
    }

    // ── TASK-323: Wave 3 pipeline shape end-to-end tests ────────────────

    #[test]
    fn wave3_stats_count_by_event_type() {
        // events | STATS COUNT(*) GROUP BY event_type
        // Tests the Aggregate bind arm (HashAggregateOperator).
        let scratch = Scratch::new("wave3-agg");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('alice', 1700000000100000000, 'click', 2), \
                 ('bob',   1700000000200000000, 'view',  3), \
                 ('carol', 1700000000300000000, 'click', 4)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query("events | stats n = count(*) group by event_type", &mut db)
            .expect("aggregate query");

        // Should have 2 groups: click (3 rows) and view (1 row).
        assert_eq!(result.row_count(), 2, "expect 2 groups (click, view)");

        // Collect (event_type, count) pairs and sort for determinism.
        let mut pairs: Vec<(String, i64)> = Vec::new();
        for batch in &result.rows {
            let event_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("event_type column must be StringView");
            let count_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("count column must be Int64");
            for i in 0..batch.num_rows() {
                pairs.push((event_col.value(i).to_string(), count_col.value(i)));
            }
        }
        pairs.sort();
        assert_eq!(
            pairs,
            vec![("click".to_string(), 3), ("view".to_string(), 1),],
            "aggregate counts must match"
        );
    }

    #[test]
    fn wave3_order_by_ts_desc_limit() {
        // events | ORDER BY ts DESC | LIMIT 3
        // Tests the Sort + Limit bind arms (SortOperator + LimitOperator).
        let scratch = Scratch::new("wave3-sort-limit");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 10), \
                 ('bob',   1700000000100000000, 'view',  20), \
                 ('carol', 1700000000200000000, 'click', 30), \
                 ('dave',  1700000000300000000, 'view',  40)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query("events | order by ts desc | limit 3", &mut db)
            .expect("sort-limit query");

        assert_eq!(result.row_count(), 3, "LIMIT 3 must cap at 3 rows");

        // Collect timestamps — should be descending.
        // The ts column is stored as Timestamp(Nanosecond, UTC).
        let mut timestamps: Vec<i64> = Vec::new();
        for batch in &result.rows {
            let ts_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .expect("ts column must be TimestampNanosecondArray");
            for i in 0..batch.num_rows() {
                timestamps.push(ts_col.value(i));
            }
        }
        // Verify strict descending order.
        for window in timestamps.windows(2) {
            assert!(
                window[0] >= window[1],
                "timestamps must be non-increasing (desc order): {:?}",
                timestamps
            );
        }
        // Top row must be the highest timestamp.
        assert_eq!(
            timestamps[0], 1700000000300000000,
            "first row after ORDER BY ts DESC must have the largest ts"
        );
    }

    #[test]
    fn wave3_select_distinct_event_type() {
        // events | SELECT DISTINCT event_type
        // Tests the Distinct bind arm (DistinctOperator).
        let scratch = Scratch::new("wave3-distinct");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('bob',   1700000000100000000, 'click', 2), \
                 ('carol', 1700000000200000000, 'view',  3), \
                 ('dave',  1700000000300000000, 'click', 4)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query("events | select distinct event_type", &mut db)
            .expect("distinct query");

        // Collect all event_type values.
        let mut seen: Vec<String> = Vec::new();
        for batch in &result.rows {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("event_type column must be StringView");
            for i in 0..batch.num_rows() {
                seen.push(col.value(i).to_string());
            }
        }
        seen.sort();
        seen.dedup();

        // After dedup, must still have exactly the unique values.
        assert_eq!(
            seen,
            vec!["click".to_string(), "view".to_string()],
            "DISTINCT must produce unique event_type values"
        );

        // No duplicate rows in the raw output (before dedup).
        let mut raw: Vec<String> = Vec::new();
        for batch in &result.rows {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("event_type column must be StringView");
            for i in 0..batch.num_rows() {
                raw.push(col.value(i).to_string());
            }
        }
        let unique_count = raw.iter().collect::<std::collections::HashSet<_>>().len();
        assert_eq!(
            raw.len(),
            unique_count,
            "DISTINCT output must have no duplicate rows, got: {:?}",
            raw
        );
    }

    #[test]
    fn wave3_match_then_stats_count() {
        // events | WHERE event_type = 'click' | MATCH FIRST SEQUENCE(click) | STATS COUNT(*)
        // Tests the fused SequenceMatch + aggregate bind arm.
        // Inserts 3 entities: alice and carol match the pattern, bob does not.
        // Expected result: COUNT(*) = 2 (entities that matched).
        let scratch = Scratch::new("wave3-match-stats");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('bob',   1700000000100000000, 'view',  2), \
                 ('carol', 1700000000200000000, 'click', 3)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query(
                "events | where event_type = 'click' | match first sequence(click) | stats n = count(*)",
                &mut db,
            )
            .expect("match-stats query");

        // Should return exactly one row with count = 2 (alice + carol matched).
        assert_eq!(
            result.row_count(),
            1,
            "COUNT(*) must produce exactly one row"
        );

        let mut total_count: i64 = 0;
        for batch in &result.rows {
            let count_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("count column must be Int64");
            for i in 0..batch.num_rows() {
                total_count += count_col.value(i);
            }
        }
        assert_eq!(
            total_count, 2,
            "COUNT(*) of matched entities must equal 2 (alice + carol)"
        );
    }

    // ── TASK-437: cohort SubqueryFilter end-to-end through Engine::query ──

    /// Helper: collect every value from the leftmost column of the
    /// result, downcast to `StringViewArray`.
    fn collect_first_string_col(result: &crate::ExecutionResult) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for batch in &result.rows {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringViewArray>()
                .expect("first column must be StringView");
            for i in 0..arr.len() {
                out.push(arr.value(i).to_owned());
            }
        }
        out.sort();
        out
    }

    #[test]
    fn cohort_in_query_single_column_filters_outer_rows() {
        // Inner cohort: entities that ever performed a 'click'.
        // Outer: every event whose entity_id is in the cohort.
        // Expected entities: alice + carol (bob never clicks).
        let scratch = Scratch::new("cohort-single-col");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('alice', 1700000000050000000, 'view',  2), \
                 ('bob',   1700000000100000000, 'view',  3), \
                 ('carol', 1700000000200000000, 'click', 4), \
                 ('dave',  1700000000300000000, 'view',  5)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query(
                "events | where user_id in query (\
                     events | where event_type = 'click' | select user_id\
                 )",
                &mut db,
            )
            .expect("cohort query");

        let entities = collect_first_string_col(&result);
        // alice (2 events) + carol (1 event) — sorted+deduped vector
        // contains the unique surviving entity ids.
        let mut unique: Vec<String> = entities.clone();
        unique.dedup();
        assert_eq!(unique, vec!["alice".to_string(), "carol".to_string()]);
        // Total surviving rows: alice has 2, carol has 1 → 3 rows.
        assert_eq!(result.row_count(), 3);
    }

    #[test]
    fn cohort_in_query_two_column_tuple_filters_outer_rows() {
        // Inner cohort: (user_id, event_type) tuples where amount >= 3.
        // Outer: only rows whose (user_id, event_type) tuple appears in
        // that cohort.
        let scratch = Scratch::new("cohort-tuple");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('alice', 1700000000050000000, 'view',  5), \
                 ('bob',   1700000000100000000, 'view',  2), \
                 ('carol', 1700000000200000000, 'click', 4), \
                 ('carol', 1700000000300000000, 'view',  1)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query(
                "events | where (user_id, event_type) in query (\
                     events | where amount >= 3 | select user_id, event_type\
                 )",
                &mut db,
            )
            .expect("tuple cohort query");

        // Cohort contains (alice, view) and (carol, click).
        // alice's 'view' row survives; carol's 'click' row survives;
        // alice's 'click', bob's 'view', carol's 'view' do not.
        let entities = collect_first_string_col(&result);
        assert_eq!(entities, vec!["alice".to_string(), "carol".to_string()]);
    }

    #[test]
    fn cohort_in_alias_resolves_and_filters() {
        // Same semantics as the single-column IN QUERY case, but
        // expressed via an alias definition (cohorts-aliases-joins.md
        // §2.1, §2.11).
        let scratch = Scratch::new("cohort-alias");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('bob',   1700000000100000000, 'view',  2), \
                 ('carol', 1700000000200000000, 'click', 3)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query(
                "clickers = events | where event_type = 'click' | select user_id\n\
                 events | where user_id in clickers",
                &mut db,
            )
            .expect("alias query");

        let mut entities = collect_first_string_col(&result);
        entities.dedup();
        assert_eq!(entities, vec!["alice".to_string(), "carol".to_string()]);
    }

    #[test]
    fn cohort_empty_inner_subquery_filters_everything_out() {
        // Inner cohort matches zero rows → outer must be empty (the
        // empty-cohort short-circuit in `SubqueryFilterOperator`).
        let scratch = Scratch::new("cohort-empty");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('bob',   1700000000100000000, 'view',  2)",
                &mut db,
            )
            .expect("insert");

        let result = engine
            .query(
                "events | where user_id in query (\
                     events | where event_type = 'never_seen' | select user_id\
                 )",
                &mut db,
            )
            .expect("empty cohort query");

        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn cohort_unknown_alias_errors_at_plan_time() {
        // `IN <alias>` references must resolve at plan time
        // (cohorts-aliases-joins.md §2.3) — undefined alias surfaces as
        // a Plan error before the engine starts execution.
        let scratch = Scratch::new("cohort-undef-alias");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        match engine.query("events | where user_id in ghost", &mut db) {
            Err(crate::query::ExecutionFailure {
                error: BqliteError::Plan(msg),
                ..
            }) => {
                assert!(
                    msg.contains("ghost"),
                    "error should name the unknown alias: {msg}"
                );
            }
            other => panic!("expected Plan error for undefined alias, got {other:?}"),
        }
    }

    /// TASK-514: a cohort whose materialised hash set would exceed the
    /// per-query memory budget surfaces the typed
    /// `BqliteError::MemoryBudgetExceeded` instead of growing
    /// unbounded. Uses a `QueryContext::new(64)` directly — going
    /// through `Engine::query_with_options` would clamp at the 512 MiB
    /// floor (`docs/design/engine/memory-budget.md` §8.2) which is
    /// far more than a unit-test cohort can possibly exceed.
    #[test]
    fn cohort_materialisation_fails_fast_on_budget_overflow() {
        let scratch = Scratch::new("cohort-budget-overflow");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice',   1700000000000000000, 'click', 1), \
                 ('bob',     1700000000050000000, 'click', 2), \
                 ('carol',   1700000000100000000, 'click', 3), \
                 ('dave',    1700000000150000000, 'click', 4)",
                &mut db,
            )
            .expect("insert");

        // Plan a query that needs to materialise a cohort.
        let mut stmts = bqlite_parser::parse(
            "events | where user_id in query (\
                 events | where event_type = 'click' | select user_id\
             )",
        )
        .expect("parse cohort query");
        let stmt = stmts.remove(0);
        let physical = {
            let cat = db.catalog();
            plan(stmt, &cat, 0).expect("plan cohort query")
        };

        // Budget chosen below `cohort_key_bytes(one row)` (≥ 16-byte
        // hashbrown overhead + 24-byte Vec header + one 32-byte
        // ScalarValue + the string capacity), so the very first
        // batch's reservation overshoots.
        let ctx = QueryContext::new(8);
        match bind_physical(&physical, &mut db, &ctx) {
            Err(BqliteError::MemoryBudgetExceeded { used, budget }) => {
                assert_eq!(budget, 8, "budget must echo the configured ceiling");
                assert!(
                    used > budget,
                    "MemoryBudgetExceeded must report a `used` past the budget: \
                     used={used} budget={budget}"
                );
            }
            Err(other) => panic!("expected MemoryBudgetExceeded, got {other:?}"),
            Ok(_) => panic!("expected MemoryBudgetExceeded, got Ok(_)"),
        }
        // The failed materialisation must release every byte it
        // briefly reserved — cohorts-aliases-joins.md §2.7 / spill.md
        // §4.3 require a clean fail with no leaked accounting.
        assert_eq!(
            ctx.memory().used_bytes(),
            0,
            "failed cohort materialisation must release every reserved byte"
        );
    }

    /// Direct unit test of the cache hit path: install one cohort,
    /// then look it up via a *separate, structurally equal* clone of
    /// the same `PhysicalPlan`. The cache must return `Some` and the
    /// returned `Arc` must point at the same allocation as the one
    /// installed (Arc::ptr_eq) — proving the two references collapse
    /// to one materialization, not two separate but equal sets.
    #[test]
    fn cohort_cache_get_returns_arc_for_equal_plan() {
        let scratch = Scratch::new("cohort-cache-direct");
        let db = create_db_with_bootstrap(scratch.path());

        // Build a trivial PhysicalPlan: a `Scan` of the bootstrap
        // events table. `PhysicalPlan` derives `PartialEq`, so two
        // independently constructed `Scan` descriptors with the same
        // table name compare equal.
        let mut stmts = bqlite_parser::parse("events").expect("parse events");
        let stmt = stmts.remove(0);
        let plan_a = {
            let cat = db.catalog();
            plan(stmt, &cat, 0).expect("plan A")
        };
        let mut stmts2 = bqlite_parser::parse("events").expect("parse events");
        let stmt2 = stmts2.remove(0);
        let plan_b = {
            let cat = db.catalog();
            plan(stmt2, &cat, 0).expect("plan B")
        };
        assert_eq!(
            plan_a, plan_b,
            "two `events` scans must be structurally equal for the cache test"
        );

        let cohort = Arc::new(CohortHashSet::empty(1));
        let mut cache = CohortCache::default();
        cache.insert(plan_a, Arc::clone(&cohort));

        let fetched = cache.get(&plan_b).expect("equal plan must hit the cache");
        assert!(
            Arc::ptr_eq(&fetched, &cohort),
            "cache hit must return the same Arc allocation, not a fresh one"
        );
    }

    /// Validate that two `IN alias` references against the same alias
    /// behave identically. End-to-end correctness leg of the cache; the
    /// `Arc::ptr_eq` guarantee is pinned by
    /// `cohort_cache_get_returns_arc_for_equal_plan` above.
    #[test]
    fn cohort_alias_referenced_twice_produces_consistent_results() {
        let scratch = Scratch::new("cohort-alias-twice");
        let (mut db, engine) = create_db_with_events_table(scratch.path());

        engine
            .query(
                "INSERT INTO events VALUES \
                 ('alice', 1700000000000000000, 'click', 1), \
                 ('bob',   1700000000100000000, 'view',  2), \
                 ('carol', 1700000000200000000, 'click', 3)",
                &mut db,
            )
            .expect("insert");

        // The terminal pipeline references the alias twice via an
        // AND'd predicate so the same cohort must be probed twice.
        let result = engine
            .query(
                "clickers = events | where event_type = 'click' | select user_id\n\
                 events | where user_id in clickers and user_id in clickers",
                &mut db,
            )
            .expect("alias-twice query");

        let mut entities = collect_first_string_col(&result);
        entities.dedup();
        assert_eq!(entities, vec!["alice".to_string(), "carol".to_string()]);
    }

    // ── TASK-438 CP1: Wave 4 EntityOperator bind arms ───────────────────

    /// Create a database with an "events" table whose entity key is named
    /// "entity_id" (as required by SessionizeOperator, EventSelectOperator,
    /// and AttributeOperator — all three hardcode that column name in their
    /// input schema lookups).
    fn create_db_with_entity_id_table(path: &Path) -> (Database, crate::Engine) {
        let mut db = Database::create(path).expect("create db");
        let engine = crate::Engine::new();
        engine
            .query(
                "CREATE TABLE events (\
                     entity_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .expect("create entity_id-keyed events table");
        (db, engine)
    }

    #[test]
    fn wave4_sessionize_empty_table_binds_and_runs() {
        // Smoke test: bind_sessionize must construct the operator tree without
        // error and return zero rows for an empty table.
        let scratch = Scratch::new("wave4-sess-empty");
        let (mut db, engine) = create_db_with_entity_id_table(scratch.path());

        let result = engine
            .query("events | sessionize(gap: 30m)", &mut db)
            .expect("sessionize on empty table must not error");

        assert_eq!(result.row_count(), 0, "empty table → 0 sessions");
    }

    #[test]
    fn wave4_event_select_first_empty_table_binds_and_runs() {
        // Smoke test: bind_event_select must construct the operator tree and
        // return zero rows for an empty table.
        //
        // NOTE: EventSelectOperator uses the planner's input schema (which
        // includes `__seq_id`) for column-index resolution; the current
        // ScanOperator runtime does not yet materialise `__seq_id` in
        // batches, so data-driven tests for EventSelect are deferred until
        // the scan layer gains `__seq_id` materialisation.
        let scratch = Scratch::new("wave4-esel-empty");
        let (mut db, engine) = create_db_with_entity_id_table(scratch.path());

        let result = engine
            .query("events | first(click)", &mut db)
            .expect("first(click) on empty table must not error");

        assert_eq!(result.row_count(), 0, "empty table → 0 selected events");
    }

    #[test]
    fn wave4_attribute_empty_table_binds_and_runs() {
        // Smoke test: bind_attribute must construct the operator tree and
        // return zero rows for an empty table.
        let scratch = Scratch::new("wave4-attr-empty");
        let (mut db, engine) = create_db_with_entity_id_table(scratch.path());

        let result = engine
            .query(
                "events | attribute(\
                     conversion: (purchase), \
                     touchpoints: (view, click), \
                     window: 7d, \
                     touchpoint_key: event_type\
                 )",
                &mut db,
            )
            .expect("attribute on empty table must not error");

        assert_eq!(result.row_count(), 0, "empty table → 0 attributions");
    }

    // ── TASK-438 CP2: SampleFilterOperator + MergeSources bind ──────────

    #[test]
    fn wave4_sample_fallback_bind_pass_through() {
        // `events | sessionize(gap: 30m) | sample(fraction: 1.0)` forces the
        // fallback path: the planner's sample-pushdown pass recognises
        // Sessionize as a stateful operator and leaves the SamplePhysical node
        // above it. The bind step must materialise a SampleFilterOperator with
        // fraction=1.0, which is a pass-through and returns all rows (0 for an
        // empty table).
        let scratch = Scratch::new("wave4-sample-pt");
        let (mut db, engine) = create_db_with_entity_id_table(scratch.path());

        let result = engine
            .query(
                "events | sessionize(gap: 30m) | sample(fraction: 1.0)",
                &mut db,
            )
            .expect("sample(fraction: 1.0) after sessionize on empty table must not error");

        assert_eq!(result.row_count(), 0, "empty table → 0 rows");
    }

    #[test]
    fn wave4_sample_fallback_bind_empty_set() {
        // `sample(fraction: 0.0)` on an empty table: the SampleFilterOperator
        // short-circuits to Ok(None) before pulling any child batches.
        let scratch = Scratch::new("wave4-sample-empty");
        let (mut db, engine) = create_db_with_entity_id_table(scratch.path());

        let result = engine
            .query(
                "events | sessionize(gap: 30m) | sample(fraction: 0.0)",
                &mut db,
            )
            .expect("sample(fraction: 0.0) must not error");

        assert_eq!(result.row_count(), 0, "fraction=0.0 → 0 rows");
    }

    #[test]
    fn wave4_merge_sources_bind_two_empty_tables() {
        // Directly construct a MergeSourcesPhysical over two empty bootstrap
        // tables and verify that bind_physical creates a valid operator that
        // returns 0 rows.
        use bqlite_core::{BqlType, ColumnDef, OperatorSchema};
        use bqlite_planner::{MergeSourcesPhysical, PhysicalPlan, ScanPhysical, SortDirection};

        let scratch = Scratch::new("wave4-merge-two-empty");
        let mut db = Database::create(scratch.path()).expect("create db");
        let engine = crate::Engine::new();

        // Create two tables with the same bootstrap schema.
        engine
            .query(
                "CREATE TABLE events_a (\
                     entity_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .expect("create events_a");
        engine
            .query(
                "CREATE TABLE events_b (\
                     entity_id STRING NOT NULL ENTITY KEY, \
                     ts TIMESTAMP NOT NULL EVENT TIME, \
                     event_type STRING NOT NULL EVENT TYPE\
                 )",
                &mut db,
            )
            .expect("create events_b");

        // Build scan descriptors for each sub-table.
        // For simplicity use the same declared-column schema.
        let sub_schema = OperatorSchema::new(vec![
            ColumnDef::required("entity_id", BqlType::String),
            ColumnDef::required("ts", BqlType::Timestamp),
            ColumnDef::required("event_type", BqlType::String),
        ])
        .expect("sub schema");

        let make_scan = |table: &str| ScanPhysical {
            table: table.to_string(),
            query_range: None,
            reader_range: None,
            scan_predicates: vec![],
            projected_columns: vec![],
            output_schema: sub_schema.clone(),
            entity_key_col: "entity_id".to_string(),
            timestamp_col: "ts".to_string(),
            sample: None,
        };

        // Combined output schema: qualified user columns + __source_table_id.
        let merged_schema = OperatorSchema::new(vec![
            ColumnDef::required("events_a.entity_id", BqlType::String),
            ColumnDef::required("events_a.ts", BqlType::Timestamp),
            ColumnDef::required("events_a.event_type", BqlType::String),
            ColumnDef::nullable("events_b.entity_id", BqlType::String),
            ColumnDef::nullable("events_b.ts", BqlType::Timestamp),
            ColumnDef::nullable("events_b.event_type", BqlType::String),
            ColumnDef::required("__source_table_id", BqlType::Int),
        ])
        .expect("merged schema");

        let merge_plan = PhysicalPlan::MergeSources(MergeSourcesPhysical {
            tables: vec![make_scan("events_a"), make_scan("events_b")],
            order: vec![
                ("entity_id".into(), SortDirection::Asc),
                ("ts".into(), SortDirection::Asc),
                ("__source_table_id".into(), SortDirection::Asc),
            ],
            table_id_map: vec!["events_a".into(), "events_b".into()],
            output_schema: merged_schema,
        });

        let mut op = bind_physical(&merge_plan, &mut db, &QueryContext::unbounded())
            .expect("bind_merge_sources must succeed");
        op.open().expect("open must succeed");
        // Both tables are empty — the merge operator must return None immediately.
        assert!(
            op.next_batch()
                .expect("next_batch must not error")
                .is_none(),
            "two empty tables must yield 0 rows"
        );
        op.close().expect("close must succeed");
    }
}
