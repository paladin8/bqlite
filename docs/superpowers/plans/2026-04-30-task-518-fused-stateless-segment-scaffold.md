# TASK-518: Fused Stateless Segment Scaffold — Implementation Plan

**Owner**: agent-2
**Branch**: task/TASK-518
**Design**: `docs/design/engine/operator-fusion.md` (TASK-503)
**Reference**: `docs/design/execution-model.md` §3.8

## Scope

Land the runtime infrastructure described in `engine/operator-fusion.md` §3 and §4 without changing the `PhysicalOperator` external boundary. This is the scaffold the optimizer in TASK-519 will switch over to.

Deliverables (per design §7.1):

1. `selection_vector_materializations` and `selection_vector_dropped_rows` metric counters in `bqlite-core::metrics`.
2. `StatelessKernel` trait + Filter/Project kernel implementations in `bqlite-operators`.
3. `materialize_filtered_batch` helper alongside the existing `materialize_selected` boundary.
4. `FusedStatelessSegment` `PhysicalOperator` driver with the `KernelStep` enum (Filter/Project/Limit).
5. `FusedSegmentPhysical` planner descriptor + engine bind path (legacy bind path remains).
6. Unit tests directly constructing `FusedStatelessSegment`; equivalence tests against legacy operators.
7. Doc reconciliation: a §3.8 lead-in pointer to `engine/operator-fusion.md` in `execution-model.md`.

Not in scope (per §1, §7.1 of the design doc): the optimizer rule that emits `FusedSegmentPhysical` (TASK-519); stateful-to-aggregate fusion (TASK-520); per-kernel `kernel_calls`/`kernel_time_ns` metrics (TASK-524).

## Checkpoint Plan

### CP1 — Metrics counters (shared file)

`crates/bqlite-core/src/metrics.rs`. Adds:
- `MetricsSnapshot::selection_vector_materializations: u64`
- `MetricsSnapshot::selection_vector_dropped_rows: u64`
- `Metrics::record_selection_vector_materializations(&self, _n: u64)` (default no-op body)
- `Metrics::record_selection_vector_dropped_rows(&self, _n: u64)` (default no-op body)
- `AtomicMetrics` overrides for both.
- `is_zero` / `merge` extensions and tests.

This is an isolated additive change to a shared file — must merge first so downstream checkpoints can use the counters without conflict.

### CP2 — `StatelessKernel` trait + Filter/Project kernels + `materialize_filtered_batch`

`crates/bqlite-operators/`:
- New module `kernel.rs`:
  - `StatelessKernel` trait per §3.2 with `apply(&self, FilteredBatch) -> Result<FilteredBatch>`, `output_schema()`, `kernel_name()` (default `type_name`).
  - `FilterKernel` wrapping a `CompiledExpr` + tile size; reuses the existing `evaluate_tiled_mask` semantics. Output **always** carries `Some(sv)` per §3.3.1, even when every row passes (so `sv.len() == batch.num_rows()` is the all-pass signal — the type-level invariant the driver's short-circuit relies on).
  - `ProjectKernel` wrapping `Vec<ProjectionExpr>` + cached output schema / Arrow schema. Walks `S_in` once per output column and writes results into per-column buffers **pre-sized to `S_in.len()` (or `batch.num_rows()` when `S_in` is `None`)**, so the hot path does not reallocate. Output is always `selection: None` per §3.3.2.
- New free function in `materialize.rs`:
  - `materialize_filtered_batch(fb, metrics) -> Result<RecordBatch>` per §4.3, reusing `selection_to_bool_array`. Three paths: `selection: None` → return inner batch; `Some(sv)` with `sv.len() == batch.num_rows()` → return inner batch; otherwise → `arrow::compute::filter_record_batch` and `metrics.record_selection_vector_materializations(1)`. The metric is **only** recorded on the actual-copy path.
- Tests covering the §3.3 contract:
  - Filter narrows: any `selection: Some(_)` input yields `selection: Some(_)` output (intersect respected).
  - Filter all-pass invariant: `selection: None` input where every row passes yields `selection: Some(sv)` with `sv.len() == batch.num_rows()`. This pins §3.3.1's "always Some(_)" rule.
  - Project rewrites: dense output (`selection: None`), correct row count, columns reflect the post-selection rows.
  - Project pre-sizing: a property-style assertion on a few selection lengths that the resulting batch has `num_rows == sel.len()`.
  - `materialize_filtered_batch`:
    - No-op on dense input — returns `Ok(fb.batch)`, **does not** increment the metric.
    - Full-cover selection (`sv.len() == batch.num_rows()`) — same: returns the inner batch, no metric increment.
    - Sparse selection — returns a copy whose row count equals `sv.len()`, **and** the metric is incremented exactly once.

### CP3 — `FusedStatelessSegment` driver

`crates/bqlite-operators/src/fused_segment.rs`:
- `KernelStep` enum: `Filter(Arc<dyn StatelessKernel>)`, `Project(Arc<dyn StatelessKernel>)`, `Limit { /* no fields — driver owns the counter */ }` per §4.1. **No `LimitKernel` type** — Limit is a driver step.
- `FusedStatelessSegment` struct holding `child`, `kernels: Vec<KernelStep>`, `output_schema`, `sparsity_factor: f64`, `limit_remaining: Option<u64>`, `metrics: Arc<dyn Metrics>`.
- Constants: `SPARSITY_FACTOR_DEFAULT = 0.10`.
- `next_batch()` algorithm per §4.2:
  1. Cancellation check at top of loop.
  2. If `limit_remaining == Some(0)` → return `Ok(None)`.
  3. Pull child; if `None`, return `Ok(None)`. **If the child returned a 0-row `RecordBatch`, re-pull** (don't feed an empty `FilteredBatch` through the chain).
  4. Wrap as `FilteredBatch::dense(b)`.
  5. **For each step in `kernels`** (per-step granularity, per §3.4.1): record `selection_vector_dropped_rows += batch.num_rows() - live_rows()` for the input (per §3.5), then `maybe_materialize` (sparsity check that calls `materialize_filtered_batch` when `live_rows < sparsity_factor * batch.num_rows()`), then either invoke the kernel via `apply` or apply the in-driver `Limit` truncation to the FilteredBatch.
  6. If `live_rows() == 0` after the chain, `continue` (re-pull child).
  7. Outer-boundary `materialize_filtered_batch` on the result and return `Some(batch)`.
- Metric ownership:
  - `selection_vector_dropped_rows` accounting is owned by the driver, recorded **once per kernel-step input** (per-batch, not per-row), summing `batch.num_rows() - live_rows()` for every step's pre-kernel input. This matches §3.5.
  - `selection_vector_materializations` is recorded inside `materialize_filtered_batch` (the only call site per §4.3). The driver does not record it directly.
- Tests:
  - Filter narrows / Project rewrites / Limit truncates (single-kernel chains).
  - Multi-kernel chain `filter → project → limit` against expected values.
  - Sparsity boundary: at and just-above the threshold; assert that `selection_vector_materializations` reflects the expected number of in-chain materialization calls (1 per scan batch on dense; 2 per scan batch when sparsity fires once mid-chain — per §7.2).
  - `selection_vector_dropped_rows` accumulates across multi-step / multi-batch runs to exactly `Σ (input_rows − live_rows)` per step entry — pinned with a hand-computed test.
  - Empty-batch loop drives a child re-pull (both fully-rejecting filter and child-emits-0-row paths).
  - Cancellation between batches.
  - Equivalence with legacy `Limit(Project(Filter(scan)))` on hand-built scenarios (proxy for the load-bearing equivalence invariant in §7.2; full property-test variant lives in TASK-519 where the optimizer flips).

### CP4 — `FusedSegmentPhysical` descriptor + bind path

`crates/bqlite-planner/src/physical.rs`:
- `FusedSegmentPhysical` and `FusedSegmentStep` per §4.6.
- New `PhysicalPlan::FusedSegment` arm; output schema delegation.

`crates/bqlite-engine/src/bind.rs`:
- Bind path for `PhysicalPlan::FusedSegment` that walks `steps` (Filter → constructs `FilterKernel::new(predicate.clone(), tile_size)`; Project → constructs `ProjectKernel::new(items.clone(), output_schema)`; Limit → emits the driver `KernelStep::Limit` with the row budget tracked on the segment) and assembles a `FusedStatelessSegment`. **`tile_size` is threaded through verbatim from the descriptor — never defaulted on the bind path.**
- Tests: hand-built `FusedSegmentPhysical` binds to a working operator and produces identical output to the legacy `FilterPhysical → ProjectPhysical → LimitPhysical` chain on the same inputs.

### CP5 — Doc reconciliation

`docs/design/execution-model.md` §3.8 lead-in: a single short paragraph at the top of §3.8 pointing at `docs/design/engine/operator-fusion.md` for the implementation contract. INDEX.md already references operator-fusion.md (line 59), so no INDEX edit is needed.

## Risks / Open Questions

- The §3.8 lead-in text the design doc §7.3 promises to replace ("the Wave 2 filter/project/limit operators (TASK-231) deliberately ship without that infrastructure …") does not appear verbatim in the current execution-model.md. I will instead add a brief one-paragraph pointer at the top of §3.8 stating that the implementation contract lives in `engine/operator-fusion.md`. If a reviewer flags this as drift from §7.3, that is a doc-only follow-up.
- LIMIT-as-driver-step (not a kernel) is per §3.3.3 and §4.1. CP3 honours that — no `LimitKernel` type.

## Testing & Validation

Each checkpoint runs `scripts/local-ci.sh` end-to-end and a subagent code review on staged changes per AGENTS.md §Behavioral Requirement #4 / §Checkpoint Discipline.
