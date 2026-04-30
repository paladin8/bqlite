# TASK-519 — Refactor Filter / Project / Limit onto Fused Stateless Kernels

**Status**: draft
**Author**: agent-2
**Date**: 2026-04-30
**Depends on**: TASK-518 (scaffold); ships the optimizer flip + legacy retirement
**Owns**: `engine/operator-fusion.md` §6.4 + §7.1 row 2

---

## 1. Goal

Discharge TASK-519 per `docs/design/engine/operator-fusion.md` §7.1 row 2:

> Flip the optimizer to emit `FusedSegmentPhysical` for every stateless run. Delete `FilterPhysical` / `ProjectPhysical` / `LimitPhysical` and the legacy operator types. Extend the Wave 2 acceptance test (TASK-235 / TASK-245) so it asserts on the new descriptor and metrics, and extend `benches/wave2_*` so the new path is the default measurement.

After this task, no part of the code depends on the legacy stateless descriptors or operators. Every stateless segment is a `FusedStatelessSegment` driven by `KernelStep`s.

## 2. Pre-existing Substrate (TASK-518)

- `bqlite_operators::{FilteredBatch, StatelessKernel, FilterKernel, ProjectKernel, FusedStatelessSegment, KernelStep, materialize_filtered_batch}` already implement the runtime.
- `bqlite_planner::physical::{FusedSegmentPhysical, FusedSegmentStep}` descriptor already exists.
- `bqlite_engine::bind::bind_fused_segment` already binds the descriptor onto a `FusedStatelessSegment` operator at execution time.
- `selection_vector_materializations` and `selection_vector_dropped_rows` metric counters already live on `bqlite_core::metrics::AtomicMetrics`.
- The legacy descriptors (`FilterPhysical`, `ProjectPhysical`, `LimitPhysical`) and operators (`FilterOperator`, `ProjectOperator`, `LimitOperator`) still exist; lowering still emits the legacy descriptors and `bind_physical` still has legacy arms.

## 3. Target End-State

1. `lower_physical` emits `PhysicalPlan::FusedSegment` for the `LogicalPlan::{Filter, Project, Limit}` arms, coalescing adjacent stateless logical nodes into a single `FusedSegmentPhysical` whose `steps` mirror the chain top-down.
2. `pushdown_predicates`, `prune_columns`, `pushdown_sample`, `fuse_match_aggregate`, `entity_key_col_name`, `SequenceMatchAdapter::source_entry_range`, and `build_explain_node` all walk `PhysicalPlan::FusedSegment` instead of `Filter` / `Project` / `Limit`.
3. `PhysicalPlan::Filter`, `PhysicalPlan::Project`, `PhysicalPlan::Limit`, `FilterPhysical`, `ProjectPhysical`, `LimitPhysical`, `FilterOperator`, `ProjectOperator`, `LimitOperator`, and the legacy bind arms are deleted. `ProjectionExpr` is preserved (used by `ProjectKernel`); it lives next to the kernel.
4. `tests/tests/wave2_acceptance.rs` adds an `assert!` on the new EXPLAIN shape AND on `selection_vector_materializations` running through the fused path.
5. `benches/wave2/` gains an operator-level microbench (`fused_segment.rs`) measuring `scan_filter_project_limit_throughput` and `selection_vector_materializations_per_query` per the test bar in `engine/operator-fusion.md` §7.2.

## 4. Architecture Decisions

**D1 — Lowering coalesces, not the optimizer.** `lower_physical` traverses the logical tree top-down. When it lowers a stateless logical node (Filter / Project / Limit) and the lowered child is already a `FusedSegmentPhysical`, it appends the new step to the child's step vector. Otherwise it constructs a new single-step `FusedSegmentPhysical`. This avoids needing a separate "merge adjacent FusedSegments" optimizer pass — by construction, every stateless run is a single segment from the moment lowering finishes.

Rationale: Keeps the optimizer rule registry stable (no new rule needed), keeps lowering's time complexity O(N), and lets `pushdown_predicates` / `prune_columns` work against a single shape. An alternative (lower into single-step segments + a merge pass) is rejected because the merge pass would do nothing the lowering pass cannot do, and it would multiply trace entries.

**D2 — `pushdown_predicates` operates at the FusedSegment-prefix-Filter boundary.** A pushable Filter step exists only at index 0 of `FusedSegmentPhysical::steps` whenever the input is a `Scan`. The pass detects that shape, splits the predicate into pushable / residual conjuncts, mutates `Scan::scan_predicates`, and either drops the step (all-pushed) or rewrites the predicate (residual). When the resulting segment has zero steps, the pass returns the bare `Scan` directly.

Filter steps at index ≥ 1 are explicitly NOT pushed — when a `Project` step at index 0 has rewritten the schema, the Filter step's compiled-expression column references point into the Project's output names, not the Scan's columns. Pushing such a Filter into the Scan would silently misroute column lookups. The new `pushdown.rs` arm therefore only inspects `steps[0]`. A negative test (`FusedSegment(steps=[Project, Filter], input=Scan)`) asserts the Filter at index 1 stays put and the Scan's predicates remain empty.

The Wave 2 conservatism — pushdown only touches the `Filter[0]`-above-`Scan` shape, never reaching across other interior nodes — is preserved.

**D3 — `prune_columns` walks `steps` in reverse.** Demand starts with the segment's emitted column names and is propagated through each step right-to-left:
- `Limit`: identity.
- `Project`: replace demand with the union of column names referenced in every `ProjectPhysicalItem.expr`. (Mirrors the legacy `ProjectPhysical` arm exactly.)
- `Filter`: union demand with column names in `predicate`. (Mirrors the legacy arm.)
The final demand becomes the demand for the segment's `input`, which is then recursed into.

**D4 — Stateful passes walk through `FusedSegment` as a transparent box.** `fuse_match_aggregate`, `pushdown_sample`, `entity_key_col_name`, and `source_entry_range` recurse into `seg.input` exactly the way they used to recurse into `proj.input` / `filter.input` / `limit.input`. The legacy patterns "*either Filter or Project or Limit*" become a single `FusedSegment` arm.

**D5 — EXPLAIN renders one node per step, in reverse-step order.** `build_explain_node` for `FusedSegment` iterates `steps` *in reverse* so the last appended step becomes the outermost rendered `ExplainNode`. Because lowering appends in source order — Filter, then Project, then Limit (D1) — the rendered tree is `Limit ⟶ Project ⟶ Filter ⟶ <input>`, matching the legacy `Limit(Project(Filter(Scan)))` shape exactly. A unit test on a 3-step segment pins this iteration order and asserts the rendered string matches the legacy format byte-for-byte.

The Wave 2 acceptance test assertions stay valid:
- `plan_text.contains("Scan(purchases)")` — Scan's `scan_predicates` still render via `format_explain` exactly as before.
- `plan_text.contains("Limit 10")` — `Limit 10` is the topmost node after the rebound.
- `plan_text.contains("Project")` and the column slice — Project remains between Limit and Scan.
- `!plan_text.contains("Filter")` — when pushdown elides the Filter step, no `Filter` ExplainNode is emitted (only Project + Limit). When pushdown drops *every* step, the segment is replaced by the bare Scan and no FusedSegment wrapper renders at all.

**D6 — Metric assertion uses the existing `Metrics` interface.** The Wave 2 acceptance test today uses `Engine::query` which doesn't yet thread per-query metrics. To assert the metric counter without expanding the public API, the test composes the segment at the operator level using a hand-built fixture (mirroring the unit-test pattern in `fused_segment.rs`). The end-to-end query path remains unchanged.

**D7 — `ProjectionExpr` preservation.** `ProjectionExpr` is the input shape `ProjectKernel::new` consumes. Today it lives in `bqlite_operators::project` and `kernel.rs` imports it via `use crate::project::ProjectionExpr;` (`kernel.rs:40`). The legacy `ProjectOperator` also uses it. The `From<ProjectPhysicalItem> for ProjectionExpr` impl lives at `project.rs:60` and is the conversion `bind_fused_segment` already relies on (`bind.rs:1173`).

CP2 moves the type *and* the `From` impl into `kernel.rs`, deletes `project.rs`, flips the `kernel.rs` import (it now defines the type locally), and updates `bqlite_operators::lib.rs` to re-export `ProjectionExpr` from `kernel` so external callers (notably `bqlite-engine::bind`) see the type at the same path `bqlite_operators::ProjectionExpr` they did before. No callers outside the operators crate need to change their imports.

**D8 — `FilterPhysical::tile_size` semantics survive on `FusedSegmentStep::Filter`.** The `tile_size` invariant (clamped to `[MIN_FILTER_TILE_SIZE, MAX_FILTER_TILE_SIZE]`) already lives on `FusedSegmentStep::Filter`. The `clamp_filter_tile_size` helper stays in `physical.rs`. The constants `DEFAULT_FILTER_TILE_SIZE`, `MIN_FILTER_TILE_SIZE`, `MAX_FILTER_TILE_SIZE` survive — they are referenced by the kernel, the descriptor, and tests.

## 5. Checkpoint Plan

Three checkpoints. Each independently passes `scripts/local-ci.sh` and is reviewed by a code-review subagent before merging.

### CP1 — Lowering pivot + pass rewrites + EXPLAIN parity

**Files touched**:

- `crates/bqlite-planner/src/physical.rs` — rewrite the three `LogicalPlan::{Filter, Project, Limit}` arms in `lower_physical` to emit `FusedSegment` (D1). Update the Wave 2 lowering tests in this file to assert against the new shape.
- `crates/bqlite-planner/src/opt/pushdown.rs` — rewrite per D2; remove `Filter`/`Project`/`Limit` arms; add the `FusedSegment(Filter[0]+Scan)` arm + `FusedSegment` recursion arm. **Test churn**: every `PhysicalPlan::Filter(...)` fixture (`pushdown.rs:357`, `:529`, `:552`, `:586`, `:617`, `:644`, `:669`) is rebuilt as a `FusedSegment` fixture; expected-shape assertions adjust accordingly. Add the negative test from D2 (Filter at index ≥ 1 is not pushed).
- `crates/bqlite-planner/src/opt/prune.rs` — rewrite per D3; remove `Filter`/`Project`/`Limit` arms; add `FusedSegment` arm. **Test churn**: every `PhysicalPlan::Filter(...)` / `::Project(...)` / `::Limit(...)` fixture in the existing test module rebuilds as `FusedSegment`. Add the multi-step test from S2: `FusedSegment([Filter, Project, Limit])` over Scan, downstream demand contains a column not in the Project's expressions, and the Scan's `projected_columns` does NOT include that column.
- `crates/bqlite-planner/src/opt/sample_pushdown.rs` — replace per-variant traversal with a single `FusedSegment` arm. **Test churn**: tests at lines 415+, 436+, 456+, 592+, 619+, 674+ all rebuild fixtures as `FusedSegment`.
- `crates/bqlite-planner/src/opt/fuse_match_aggregate.rs` — replace `Filter`/`Project`/`Limit` recursion arms with a single `FusedSegment` arm. Tests at lines 765+, 813+, 983+ rebuild fixtures.
- `crates/bqlite-planner/src/explain.rs` — `build_explain_node` `FusedSegment` arm iterates `steps` in REVERSE so the last appended step (Limit) is the outermost rendered `ExplainNode` (D5). Remove the legacy `Filter`/`Project`/`Limit` arms. **Test churn**: any test that constructs a `PhysicalPlan::Filter(...)` / `::Project(...)` / `::Limit(...)` fixture for explain rendering rebuilds as `FusedSegment`. Add a unit test pinning the reverse-iteration rendering on a 3-step segment.
- `crates/bqlite-engine/src/bind.rs` — `entity_key_col_name`: replace the three legacy arms with a `FusedSegment` arm. The `Filter`/`Project`/`Limit` bind arms in `bind_physical_with_cache` (`bind.rs:955`, `:964`, `:973`) become dead but remain compileable until CP2 deletes the variants.
- `crates/bqlite-operators/src/matcher/mod.rs` — `source_entry_range` (`mod.rs:249-251`): replace three legacy arms with a `FusedSegment` arm.
- `tests/tests/wave2_acceptance.rs` — verify EXPLAIN assertions still pass (D5 preserves the legacy rendering shape). Add a new operator-level test that constructs `FusedStatelessSegment` directly with `AtomicMetrics` and asserts `selection_vector_materializations` + `selection_vector_dropped_rows` increment as expected on a representative chain (per D6 — co-located here, not in the operators crate, so the metric assertion lives next to the EXPLAIN check it complements).
- Add a property test (S6 / `engine/operator-fusion.md` §7.2): "for a chain of N filters with selectivity s, the materialization count is O(1) per scan batch plus an additional O(1) per intermediate filter that crosses the sparsity threshold." Lives in `crates/bqlite-operators/src/fused_segment.rs` (`#[cfg(test)] mod proptests`) so it has direct access to the segment internals.

**Acceptance**:

- `scripts/local-ci.sh` passes.
- All existing tests in the touched modules pass after their rewrites.
- New unit tests cover: lowering Filter/Project/Limit produces FusedSegment with single-step or merged steps; pushdown of `FusedSegment(Filter+Scan)`; prune walking through a multi-step segment; fuse_match_aggregate skipping a `FusedSegment`.
- `tests/tests/wave2_acceptance.rs` end-to-end test passes; the new metric assertion passes.

### CP2 — Delete legacy descriptors and operators

**Files touched**:

- `crates/bqlite-planner/src/physical.rs` — delete `FilterPhysical`, `ProjectPhysical`, `LimitPhysical` structs and the `PhysicalPlan::Filter` / `::Project` / `::Limit` variants. Delete their `output_schema()` arms. Keep `ProjectPhysicalItem` (used by `FusedSegmentStep::Project`). Keep `clamp_filter_tile_size` and the tile-size constants (consumed by `FusedSegmentStep::Filter` via `FilterPhysical::new`-style construction sites; verify visibility — they are independent items, not internal to the deleted struct). Delete the corresponding test-module fixtures (lines 1842+, 1909+, 1950+).
- `crates/bqlite-planner/src/lib.rs` — remove `FilterPhysical`, `ProjectPhysical`, `LimitPhysical` from re-exports (lib.rs:97).
- `crates/bqlite-engine/src/bind.rs` — delete the `PhysicalPlan::{Filter, Project, Limit}` arms in `bind_physical_with_cache` (`bind.rs:955–977`). Remove the `FilterOperator` / `ProjectOperator` / `LimitOperator` imports.
- `crates/bqlite-operators/src/{filter,project,limit}.rs` — delete files. Move `ProjectionExpr` and the `From<ProjectPhysicalItem> for ProjectionExpr` impl from `project.rs:52–60` into `kernel.rs` (D7). Flip the `kernel.rs` `use crate::project::ProjectionExpr;` import to a local definition.
- `crates/bqlite-operators/src/lib.rs` — remove `pub mod {filter, project, limit}`; remove their re-exports (`pub use filter::FilterOperator;`, `pub use limit::LimitOperator;`, `pub use project::{ProjectOperator, ProjectionExpr};`). Replace with `pub use kernel::{FilterKernel, ProjectKernel, ProjectionExpr, StatelessKernel};`.
- `crates/bqlite-operators/src/fused_segment.rs` — drop the `equivalent_to_legacy_filter_project_limit_chain` test (lines 749–805); the lowering tests in CP1 (and the property test) cover the same correctness invariant. Drop the `use crate::filter::FilterOperator; use crate::limit::LimitOperator; use crate::project::{ProjectOperator, ProjectionExpr};` imports in the test module; replace `ProjectionExpr` reference with the new `kernel::ProjectionExpr` path.
- Search the operators crate for any remaining test that builds a legacy operator (e.g. `event_select.rs:` mentioned `FilterOperator` — confirm via grep). Migrate or delete each such test in this CP.
- `crates/bqlite-operators/src/encoded_filter.rs` (S3 verification) — confirm `EncodedPredicateKernel` integration with `FilterKernel` is unchanged; no edits expected.

**Acceptance**:

- `scripts/local-ci.sh` passes.
- `cargo build` shows zero references to the deleted symbols.
- The Wave 2 acceptance test from CP1 still passes.

### CP3 — Operator-level FusedStatelessSegment microbench

**Files touched**:

- `benches/wave2/fused_segment.rs` (new) — two Criterion benchmarks per `engine/operator-fusion.md` §7.2 final paragraph:
  - `scan_filter_project_limit_throughput`: drives a hand-built `FusedStatelessSegment` (Scan → Filter → Project → Limit) over a 1M-row purchases fixture in dense-selectivity (≥ 0.5) and sparse-selectivity (≤ 0.1) configurations; reports rows/sec.
  - `selection_vector_materializations_per_query`: asserts the materialization count is exactly 1 per scan batch on dense input and exactly 2 when an intermediate filter triggers the sparsity boundary.
- `benches/Cargo.toml` — register the bench target.
- `benches/wave2/README.md` (if present) — document the new bench.

The benches use existing fixture helpers in `bqlite_benches::common`. Per `CLAUDE.md` performance conventions, the benches preserve `Utf8View` paths and avoid eager copies in the kernel chain.

**Acceptance**:

- `cargo bench --bench fused_segment -- --quick` runs without panic.
- The materialization-count microbench's hardcoded expectations pass.
- `scripts/local-ci.sh` passes. Criterion benches in `benches/` are not executed by `cargo test`; the workspace still compiles cleanly.

## 6. Risks and Mitigations

| Risk | Mitigation |
| ---- | ---------- |
| Lowering merge changes EXPLAIN output, breaking wave2_acceptance | D5 preserves the per-step rendering; CP1 verifies and patches the test in the same checkpoint if any string drifts. |
| Existing tests in `pushdown.rs` / `prune.rs` exhaustively check Filter/Project/Limit shapes | Tests are rewritten in the same CP that introduces the new shape; rewrites touch only assertion targets, not test intent. |
| Per-step EXPLAIN rendering double-counts when a future optimizer wants to elide an inner step | The current pushdown design already drops Filter steps. We never emit zero-step segments — when the last step disappears, we return the input directly. Asserted by a new unit test. |
| `clamp_filter_tile_size` becomes dead-code-private after legacy deletion | Keep it `pub(crate)` and re-export the constants; `FusedSegmentStep::Filter` still uses them at descriptor-construction time. |
| CP2's deletion of legacy operators breaks an external bench / test we don't control | Search the repo first; if any external user crate imports them, it lives in the same workspace and we own it. Confirmed by `grep` before CP2 starts. |

## 7. Out of Scope

- The user-facing `sparsity_factor` knob — deferred per `engine/operator-fusion.md` §8.1.
- Per-kernel cancellation granularity — deferred per §8.2.
- `--explain-perf` rendering of selection-vector metrics — owned by TASK-524.
- Stateful operator fusion (SESSIONIZE / EventSelect / ATTRIBUTE → STATS) — owned by TASK-520.
- Wave 5 acceptance gate (`tests/tests/wave5_acceptance.rs`) — owned by TASK-528.

## 8. Open Questions

None blocking. Will surface via `[NEEDS INPUT]` if any decision in CP1 turns out to be ambiguous against `engine/operator-fusion.md` or `planner-pipeline.md`.
