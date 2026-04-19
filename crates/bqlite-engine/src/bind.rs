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

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringViewBuilder};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use bqlite_core::{
    BqlType, BqliteError, EntityId, OperatorSchema, PropertyValue, Result, SegmentReader, TimeRange,
};
use bqlite_operators::matcher::SequenceMatchState;
use bqlite_operators::operator::EntityOperator;
use bqlite_operators::{
    Accumulator, CancellationToken, DistinctOperator, FilterOperator, HashAccumulator,
    HashAggregateOperator, LimitOperator, PhysicalOperator, ProjectOperator, ScanOperator,
    SequenceMatchOperator, SortOperator,
};
use bqlite_planner::compiled::{
    ArrowKernelId, CompareKernel, CompareOp, CompiledExpr, CompiledNode,
};
use bqlite_planner::{PhysicalPlan, ScanPhysical, SequenceMatchPhysical};
use bqlite_storage::Database;

use crate::ddl::{
    build_describe_batch, build_explain_batch, execute_alter_table_add_column,
    execute_create_table, execute_drop_table, ResultOperator,
};

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
}

impl SequenceMatchAdapter {
    fn new(desc: &SequenceMatchPhysical, child: Box<dyn PhysicalOperator>) -> Result<Self> {
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
        })
    }

    /// Finalize a completed entity: route its match output to the pending
    /// batch queue (non-fused) or into the aggregate accumulator (fused).
    fn finalize_entity(&mut self, entity: EntityId, state: SequenceMatchState) -> Result<()> {
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
fn entity_key_col_name(plan: &PhysicalPlan) -> &str {
    match plan {
        PhysicalPlan::Scan(scan) => scan.entity_key_col.as_str(),
        PhysicalPlan::Filter(filter) => entity_key_col_name(&filter.input),
        PhysicalPlan::Project(proj) => entity_key_col_name(&proj.input),
        PhysicalPlan::Limit(limit) => entity_key_col_name(&limit.input),
        // For other plan shapes (unusual but forward-compat), fall back to
        // the renamed column name used in the match output schema.
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
pub fn bind_physical(plan: &PhysicalPlan, db: &mut Database) -> Result<Box<dyn PhysicalOperator>> {
    match plan {
        // ── Data-plane operators ─────────────────────────────────
        PhysicalPlan::Scan(scan) => bind_scan(scan, db),

        PhysicalPlan::Filter(filter) => {
            let child = bind_physical(&filter.input, db)?;
            Ok(Box::new(FilterOperator::new(
                child,
                filter.predicate.clone(),
                filter.tile_size,
            )))
        }

        PhysicalPlan::Project(project) => {
            let child = bind_physical(&project.input, db)?;
            Ok(Box::new(ProjectOperator::from_physical_items(
                child,
                project.expressions.clone(),
                project.output_schema.clone(),
            )))
        }

        PhysicalPlan::Limit(limit) => {
            let child = bind_physical(&limit.input, db)?;
            Ok(Box::new(LimitOperator::new(child, limit.count)))
        }

        // ── Wave 3 operators (TASK-323) ───────────────────────────
        PhysicalPlan::Sort(sort) => {
            let child = bind_physical(&sort.input, db)?;
            Ok(Box::new(SortOperator::new(
                child,
                sort.keys.clone(),
                sort.max_rows,
                CancellationToken::new(),
            )))
        }

        PhysicalPlan::Distinct(distinct) => {
            let child = bind_physical(&distinct.input, db)?;
            Ok(Box::new(DistinctOperator::new(
                child,
                distinct.max_groups,
                CancellationToken::new(),
            )))
        }

        PhysicalPlan::Aggregate(agg) => {
            let child = bind_physical(&agg.input, db)?;
            Ok(Box::new(HashAggregateOperator::new(
                child,
                agg.aggregates.clone(),
                agg.group_by.clone(),
                agg.max_groups,
                agg.output_schema.clone(),
            )))
        }

        PhysicalPlan::SequenceMatch(seq) => {
            let child = bind_physical(&seq.input, db)?;
            Ok(Box::new(SequenceMatchAdapter::new(seq, child)?))
        }

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

        // ── Wave 4 stubs ─────────────────────────────────────────
        // These arms will be implemented by TASK-438 (engine bind
        // extension for Wave 4 query nodes). For now, they return
        // unsupported errors so the crate compiles and downstream
        // tasks can land their variants independently.
        PhysicalPlan::Sessionize(_)
        | PhysicalPlan::EventSelect(_)
        | PhysicalPlan::Attribute(_)
        | PhysicalPlan::SubqueryFilter(_)
        | PhysicalPlan::Sample(_)
        | PhysicalPlan::MergeSources(_) => Err(BqliteError::Execution(
            "Wave 4 operator binding is not yet implemented (TASK-438)".into(),
        )),

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

fn bind_scan(scan: &ScanPhysical, db: &Database) -> Result<Box<dyn PhysicalOperator>> {
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

    let op = ScanOperator::with_tombstones(
        reader,
        &scan.projected_columns,
        scan_predicates,
        CancellationToken::new(),
        tombstones,
    )?;
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
        })
    }

    #[test]
    fn bind_scan_produces_a_drivable_operator() {
        let scratch = Scratch::new("happy");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = bootstrap_scan_descriptor();

        let mut op = bind_physical(&descriptor, &mut db).expect("bind must succeed");

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
    fn bind_scan_output_schema_reflects_declared_columns() {
        // The descriptor's `output_schema` widens to
        // `OperatorSchema::from_table` (declared columns + implicit
        // `__seq_id` / `__batch_id` system columns) because that's
        // the shape the planner uses to compose downstream
        // operators. The Wave 2 scan operator, however, narrows to
        // **declared columns only** — the segment reader does not
        // yet materialize system columns, and the k-way merge would
        // reject batches whose schema did not match the one passed
        // at construction. This test pins both facts explicitly so a
        // regression in either side surfaces as a clean assertion.
        let scratch = Scratch::new("schema");
        let mut db = create_db_with_bootstrap(scratch.path());
        let descriptor = bootstrap_scan_descriptor();

        let op = bind_physical(&descriptor, &mut db).expect("bind must succeed");

        let op_names: Vec<&str> = op
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(op_names, vec!["entity_id", "ts", "event_type"]);

        let descriptor_names: Vec<&str> = descriptor
            .output_schema()
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            descriptor_names,
            vec!["entity_id", "ts", "event_type", "__seq_id", "__batch_id"]
        );
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
        });

        match bind_physical(&descriptor, &mut db) {
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

        let mut op = bind_physical(&physical, &mut db).expect("bind succeeds");
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
}
