# TASK-520 — Stateful-to-aggregate fusion for Sessionize / EventSelect / Attribute

**Source**: `docs/design/engine/operator-fusion.md` §5.1 (decision matrix), §7.1 (TASK-520
follow-on), `docs/design/planner-pipeline.md` §7.4.2 / §7.4.3 / §7.4.4.

## Goal

Promote `SESSIONIZE`, `EventSelect` (FIRST/LAST/NTH), and `ATTRIBUTE` from "fusion deferred,
operator constructor panics on `fused_aggregate.is_some()`" to "fusion eligible, planner
detects, operator absorbs the downstream aggregate via `finish_entity_into`."

The work mirrors the existing `SequenceMatch` fusion path (TASK-320 / TASK-321) but adapted
to the simpler Wave 4 operators.

## Architecture decision: native vs. external output schema

When fusion fires, the operator's externally-advertised output schema flips to the
aggregate output schema. Internally the operator still emits per-entity batches in its
*native* schema — those batches are fed to `HashAccumulator::update_batch`, which resolves
columns by name.

The matcher solves this by reconstructing the native ("match output") schema at construction
time from minimal config. For Sessionize/EventSelect/Attribute the native schema depends on
forwarded columns and the input schema, so reconstruction is hairy.

**Chosen approach**: add a new field `pre_fusion_output_schema: Option<OperatorSchema>` on
each physical descriptor. The fusion pass stores the pre-replacement schema there before
overwriting `output_schema` with the aggregate schema. Operators read
`pre_fusion_output_schema.as_ref().unwrap_or(&output_schema)` to know the native schema.

Lowering populates it as `None`. Existing test fixtures stay valid.

## Checkpoints

### CP1 — Operator-side fusion infrastructure

1. Add `pre_fusion_output_schema: Option<OperatorSchema>` to:
   - `SessionizePhysical`, `EventSelectPhysical`, `AttributePhysical`
2. Update `DEMAND_CAPS` constants on all three to set
   `supports_aggregation_fusion: true`.
3. Operator changes (`sessionize.rs`, `event_select.rs`, `attribute.rs`):
   - Drop the `fused_aggregate.is_none()` panic at construction.
   - Resolve the native output schema as
     `desc.pre_fusion_output_schema.as_ref().unwrap_or(&desc.output_schema)` and use it
     for all internal batch building (Arrow schemas, slot mappings).
   - Store `fused_aggregate: Option<CompiledFusableAggregate>` on the operator.
   - Override `EntityOperator::supported_demands()` to advertise the new capability.
   - The default `EntityOperator::finish_entity_into` is sufficient because
     `finish_entity` will produce a batch in native schema.
4. Engine bind step (`bind.rs`):
   - Generalise `EntityOperatorAdapter` (or add a sibling) to support a fused path
     parallel to `SequenceMatchAdapter`'s `FusedAccState`. Build a `HashAccumulator`
     from `fused_aggregate`, route every entity through `finish_entity_into`, and
     emit one aggregate result batch on exhaustion.
   - When fused, the adapter's `output_schema()` returns
     `fused_aggregate.output_schema`; otherwise it returns the operator's
     `output_schema()`.
5. Test bar:
   - Existing operator unit tests must still pass unchanged.
   - Add at least one unit test per operator that constructs the operator with a
     populated `fused_aggregate`, runs entities through the adapter, and verifies the
     resulting aggregated batch.
   - Local CI green.

### CP2 — Planner detection + equivalence tests

1. Generalise the optimizer pass that lives at
   `crates/bqlite-planner/src/opt/fuse_match_aggregate.rs` (rename to
   `fuse_stateful_aggregate.rs`) to also detect:
   - `Aggregate(Sessionize(...))` → `Sessionize { fused_aggregate, output_schema:
     agg, pre_fusion_output_schema: original }`
   - `Aggregate(EventSelect(...))` → analogous
   - `Aggregate(Attribute(...))` → analogous
   - Eligibility: same as MATCH (group-by + agg arg expressions reference only the
     operator's native output columns; agg args must be simple column refs because the
     fused path uses `update_batch`).
2. Tests:
   - Pass-level unit tests parallel to the existing MATCH tests.
   - Integration / equivalence tests under `tests/tests/` that build identical
     pipelines, run with and without fusion, and assert row equivalence on the result
     stream.
3. Doc: update `docs/design/planner-pipeline.md` if the §7.4.x tables become normative
   (currently they stand as Wave 5 targets — flip wording to "implemented in TASK-520").

### CP3 — Design-doc updates + retire deferral comments

1. `docs/design/engine/operator-fusion.md` §5.1 matrix: flip the three rows from "No
   in v1 — default path" to "Yes — TASK-520 (this document discharged)".
2. `docs/design/operators/sessionize.md` §10, `event-select-sample.md` §12,
   `attribute.md` §13: mark the deferral resolved, point at TASK-520.
3. Retire any in-code "deferred to Wave 5" / "TASK-503" comments that the work
   actually closed.

## Plan-review revisions (2026-04-30)

After plan review, four blocking items folded into the checkpoints:

- **B1 verified**: `EntityOperator::finish_entity_into` already has a default impl in
  `operator.rs:316-325` that calls `finish_entity` → `update_batch`. We rely on it for
  the new operators; matcher's explicit override stays as is.
- **B2 — warning-drain ordering**: in the fused `finalize_entity` branch, drain
  `take_pending_warnings(&mut state, …)` *before* moving `state` into
  `finish_entity_into`. Mirrors `SequenceMatchAdapter::finalize_entity` (bind.rs:243-245).
- **B3 — ATTRIBUTE row-shape semantics**: `WHERE touchpoint_ts IS NOT NULL | STATS
  GROUP BY touchpoint_key` is the FilterThenAggregate shape and explicitly **out of
  scope** (still deferred). The fused path only handles `Aggregate(Attribute)` direct
  adjacency, where ATTRIBUTE's three-row-shape rows feed unchanged into
  `update_batch`. Equivalence holds by construction. The eligibility check refers to
  ATTRIBUTE's *pre-fusion advertised schema* (`entity_id`, `conversion_ts`,
  forwarded conversion props, `touchpoint_ts`, `touchpoint_key`), which is exactly
  what `pre_fusion_output_schema` preserves.
- **B4 — `is_simple_column_ref` for group-by keys**: `HashAccumulator::update_batch`
  resolves group-by columns by name (aggregate/mod.rs:662-672), so a fused group-by
  of `UPPER(col)` would panic at runtime. The new fusion-eligibility check applies
  `is_simple_column_ref` to *both* aggregate args *and* group-by exprs. Note: the
  existing `fuse_match_aggregate::is_eligible` has this latent bug for MATCH; we fix
  the helper in the same pass since it's now shared.

Plus selected suggestions adopted:

- **S2 / S3**: extend the existing generic `EntityOperatorAdapter` with an optional
  `fused: Option<FusedAccState>` field rather than forking. v1 takes the per-entity
  RecordBatch → `update_batch` shortcut (matches matcher's TASK-321 v1 bar). A direct
  `Accumulator::update(group_key, &values)` path is a follow-on.
- **S5 — `MaxGroupsExceeded` propagation**: the fused path returns `Err(_)` from
  `finalize_entity` via `?`, identical to matcher behaviour. Add a unit test per
  operator.
- **S6 — call-site retirement list**:
  - `crates/bqlite-operators/src/sessionize.rs:156-159`
  - `crates/bqlite-operators/src/event_select.rs:228-231`
  - `crates/bqlite-operators/src/attribute.rs:175-179`
  - `crates/bqlite-engine/src/bind.rs:380-388` (adapter doc block)
  - `DEMAND_CAPS` constants in `crates/bqlite-planner/src/physical.rs` for
    Sessionize/EventSelect/Attribute.

S1, S4, S7 noted as non-blocking follow-ups.

## Out of scope

- `Aggregate(Filter(stateful))` (FilterThenAggregate) — still deferred per the
  existing match-aggregate fusion pass scope.
- Cross-stateful-operator fusion (`Sessionize | Match | Stats`) — planner-pipeline.md
  §7.7 keeps this out of v1.
- Eager group emit / morsel partial-aggregate handoff — TASK-523.
