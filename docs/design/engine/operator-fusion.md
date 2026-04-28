# Fused Stateless Segment & Operator-Fusion Contract

**Wave**: 5
**Task**: TASK-503
**Status**: draft
**Depends on**: execution-model.md (§3.5–§3.8, §4.x, §8.4), operators/operator-traits.md, operators/aggregate-operator.md, operators/match-operator.md, operators/sessionize.md, operators/event-select-sample.md, operators/attribute.md, planner-pipeline.md (§5.3, §7.x), storage/zero-copy-scan-filter.md
**Depended on by**: TASK-518 (fused segment scaffold), TASK-519 (refactor Filter/Project/Limit onto kernels), TASK-520 (stateful-to-aggregate fusion for Sessionize / EventSelect / Attribute)

---

## 1. Purpose

`docs/design/execution-model.md` §3.8 documents a steady-state design for stateless operator chains: filter, project, and limit run inside a single fused **push segment** that exchanges `FilteredBatch` values rather than bare `RecordBatch`es. Wave 2 deliberately shipped the simpler copy-based versions of these operators (TASK-231) because the selection-vector chain only pays off when there is more than one stateless kernel inside the fused segment — and Wave 2 has no chain. This document is the implementation contract that flips §3.8 from "documented target" to "must-build".

It pins down:

- The exact `FilteredBatch` / `SelectionVector` / `StatelessKernel` surfaces that operators implement.
- The `materialize_filtered_batch` boundary helper and how it relates to the existing
  `materialize_selected` boundary at the encoded scan path (§3.8.6).
- The fused push-segment driver (the operator that wraps a chain of kernels and presents a normal `PhysicalOperator` boundary upstream and downstream).
- The three explicit materialization triggers (sparsity, push-segment boundary, aggregate hand-off) and the sparsity threshold's metric-validated form.
- The exact boundary between **stateless fusion** (this document) and **stateful-to-aggregate fusion** (`finish_entity_into()` overrides on stateful operators per planner-pipeline.md §7.4).
- v1 fusion-override decisions for every Wave 4 stateful operator (MATCH, SESSIONIZE, EventSelect FIRST/LAST/NTH, ATTRIBUTE).
- The follow-up `[IMPL]` tasks (TASK-518/519/520) and what each one is responsible for.

It does **not** cover:

- The optimizer rule that detects fusable shapes — that lives in planner-pipeline.md §7 and is implemented by TASK-521 / TASK-520. This document specifies the runtime surface those rules emit into.
- Cross-stateful-operator fusion (e.g. SESSIONIZE → MATCH). v1 leaves that off the menu per planner-pipeline.md §7.7.
- The encoded-preserving scan path itself (covered by storage/zero-copy-scan-filter.md). This document only specifies the boundary at which that path hands off to the §3.8 push segment.

---

## 2. Crate Layout and Dependency Direction

All of the new types live in `bqlite-operators`. None of them require new entries in `bqlite-core` beyond the `SelectionVector` type that already exists in `bqlite-core::encoded` (CLAUDE.md crate map; the type is shared between the encoded scan path and the post-boundary push segment).

| Symbol                          | Crate              | Notes                                                                     |
| ------------------------------- | ------------------ | ------------------------------------------------------------------------- |
| `FilteredBatch`                 | `bqlite-operators` | Already lands in `filtered_batch.rs` (TASK-515 follow-on); this document does not redefine it. |
| `SelectionVector`               | `bqlite-core::encoded` | Reused; selection vectors are produced by both the encoded path and post-boundary kernels. |
| `StatelessKernel`               | `bqlite-operators` | New trait (this document, TASK-518).                                       |
| `materialize_filtered_batch`    | `bqlite-operators` | New helper alongside the existing `materialize_selected` (this document, TASK-518). |
| `FusedStatelessSegment`         | `bqlite-operators` | New `PhysicalOperator` driver (this document, TASK-518).                  |
| `EntityOperator::finish_entity_into` overrides | `bqlite-operators` | Per-operator additions (this document, TASK-520).         |

The fused segment driver lives next to the kernels it drives. It does not need anything from `bqlite-engine`; engine bind code instantiates the driver directly the same way it instantiates a `FilterOperator` today.

---

## 3. Stateless Surface

### 3.1 `FilteredBatch`

`FilteredBatch` is already specified in execution-model.md §3.8.1 and implemented in `bqlite-operators::filtered_batch`. The shape:

```rust
pub struct FilteredBatch {
    pub batch: RecordBatch,
    pub selection: Option<SelectionVector>,
}
```

Semantics this document fixes:

1. `selection: None` means "every row in `batch` is live". This is the *cheapest* shape and is what the scan layer produces by default (and what the encoded-path materialization boundary §3.8.6 always produces).
2. `selection: Some(sv)` is a *post-filter* shape — `sv` is sorted-ascending row indices into `batch`. The columns are not sliced.
3. The push segment treats `FilteredBatch` as a value, not a reference. Cloning is cheap because Arrow column buffers are `Arc`-backed; `selection` is the only field that ever allocates, and it does so only when a kernel produces a fresh selection.

### 3.2 `StatelessKernel`

```rust
/// A pure function over `FilteredBatch`. Stateless kernels never observe
/// entity boundaries (the scan layer guarantees per-entity contiguity at
/// the batch level, so no kernel needs to track them).
pub trait StatelessKernel: Send + Sync {
    /// Apply this kernel to `input`. Implementations must respect the rules
    /// in §3.3 of operator-fusion.md (filter narrows, project rewrites,
    /// limit truncates) and must never copy row data unless they are
    /// crossing a §3.4 materialization trigger.
    fn apply(&self, input: FilteredBatch) -> Result<FilteredBatch>;

    /// Output schema after this kernel runs. Filter and Limit return their
    /// child's schema unchanged; Project returns the post-projection
    /// schema. The driver concatenates schemas as it builds the chain so
    /// the segment's outer `output_schema()` is the last kernel's schema.
    fn output_schema(&self) -> &OperatorSchema;

    /// Emit a kernel-level metric tag for `--explain-perf` (TASK-524) and
    /// the `selection_vector_materializations` counter (execution-model.md
    /// §16). Default is the kernel's type name.
    fn kernel_name(&self) -> &'static str { std::any::type_name::<Self>() }
}
```

Three points are load-bearing:

- **Single-method shape.** A kernel does not see `open()` / `close()` / `next_batch()`. The fused segment driver runs the lifecycle — kernels are pure functions over `FilteredBatch`. This keeps the inner per-batch loop branchless.
- **`Result<FilteredBatch>`.** Kernel evaluation can fail (a CAST overflow in a `Project` expression, a regex error, a budgeted allocation). Kernels return typed errors; the driver propagates them out of `next_batch`. The `Result` lives only at the kernel level — `apply` is called once per input batch, so the per-batch overhead is one branch, not per-row.
- **No mutable state.** Kernels are `&self`. `LIMIT` is the awkward case: it needs a remaining-row counter. Per §3.3.3, `LIMIT`'s state lives on the *segment driver*, not on the kernel — the kernel is constructed with the *initial* remaining budget but does not mutate it. The driver consults the budget before invoking the kernel.

### 3.3 Kernel Contracts

Three rules govern how kernels manipulate `FilteredBatch`. These are normative — every new kernel landed in TASK-519 or later must satisfy them, and `materialize_filtered_batch` is the only exception (it is allowed to copy because it *is* the boundary).

#### 3.3.1 Filter narrows the selection

```text
input:  FilteredBatch { batch: B, selection: S_in }
output: FilteredBatch { batch: B, selection: Some(S_out) }
```

The `Filter` kernel evaluates its predicate against the live rows of `B`. If `S_in` is `None`, predicate evaluation walks rows `0..B.num_rows()` in execution tiles (per execution-model.md §3.6) and writes surviving row indices into a fresh `SelectionVector`. If `S_in` is `Some(sv)`, evaluation visits only the indices in `sv` and intersects the result.

The output **always** has `selection: Some(_)`, even when every row passes. This is intentional: the next kernel pays one branch (`if selection.is_some()`) regardless, and forcing `Some` lets the driver detect "the filter passed everything" via `S_out.len() == B.num_rows()` rather than via `is_none()` checks. Materialization decisions (§3.4) compare `S_out.len()` against `B.num_rows()` and decide from there.

The Wave 2 tile loop (filter.rs `evaluate_tiled_mask`) is preserved verbatim inside the new kernel — predicate evaluation in tile-sized slices is the cache-residency optimization, the selection-vector chain is the cross-kernel optimization, and the two compose. The kernel's `apply` now returns a `FilteredBatch` instead of feeding the tile mask directly into `arrow::compute::filter_record_batch`.

When the predicate is a low-cardinality dictionary equality, the kernel is the existing `EncodedPredicateKernel` (encoded_filter.rs) rather than the row-by-row evaluator. Dictionary pushdown produces a `BitVec` over codes, then a single sweep writes surviving row indices into the selection vector — execution-model.md §3.8.5. The kernel surface does not change; the implementation strategy is internal.

#### 3.3.2 Project rewrites columns, not rows

`Project` is the **only** kernel that allocates fresh column buffers in the common case. Its contract:

```text
input:  FilteredBatch { batch: B, selection: S_in }
output: FilteredBatch { batch: B', selection: None }
```

The kernel walks `S_in` (or `0..B.num_rows()` when `None`) once per output column, evaluates the compiled expression at the surviving rows, and writes results into a fresh column buffer sized at `S_in.len()`. The output `RecordBatch` therefore already represents the post-filter shape, so the output `selection` is `None`.

This is also the natural materialization point for any selection-vector chain that has accumulated upstream — subsequent kernels run against a dense batch again. In a typical `scan → filter → project → limit` pipeline the selection vector lives only between filter and project, and the project pass writes once.

String materialization remains `StringViewArray` per CLAUDE.md and execution-model.md §3.7. The kernel's expression evaluator delegates to `bqlite-operators::eval` which already honors that contract.

#### 3.3.3 Limit truncates the selection

```text
input:  FilteredBatch { batch: B, selection: S_in }
output: FilteredBatch { batch: B,           selection: Some(S_in[..take]) }
        OR
        FilteredBatch { batch: B.slice(0, take), selection: None }
```

`LIMIT` does not need to copy or rewrite anything. When `S_in` is `Some(sv)` it returns a truncated slice of `sv`; when `S_in` is `None` it slices `B` itself. The take count is `min(remaining, S_in.len() | B.num_rows())`. Once the budget reaches zero the segment driver short-circuits subsequent batches (no kernel call).

The remaining-row counter is **not** part of the kernel's `&self` state. The driver owns it. `LIMIT` is therefore not implemented as a `StatelessKernel` at all — it is a dedicated variant of the driver's `KernelStep` enum (§4.1), and the driver applies it inline. This concentrates LIMIT's "I am stateful, but only barely" asymmetry on the driver, where it belongs, instead of contaminating the kernel trait with mutable-state plumbing that no other kernel needs.

### 3.4 Materialization Triggers

execution-model.md §3.8.3 enumerates three triggers. This document fixes their concrete forms.

#### 3.4.1 Sparsity threshold

Materialize when:

```text
selection.len() < SPARSITY_FACTOR * batch.num_rows()   // default 0.10
```

The threshold is a runtime constant (`SPARSITY_FACTOR_DEFAULT = 0.10`) and a planner-tunable knob on `FusedSegmentPhysical` (so EXPLAIN can show whether a non-default value is in effect). The check runs at the **entry** of every kernel that reads row data (Filter, Project, the `LIMIT` driver tail), before the kernel does its own work. Materialization is cheap to repeat — `materialize_filtered_batch` short-circuits when `selection.is_none()` — so kernels do not need to memoize "we already materialized".

The 0.10 default is the documented value from execution-model.md §3.8.3 and matches the heuristic DuckDB and Hyper landed on. We leave it as a knob rather than burning it in because the right number is workload-dependent and the metric counter (§3.5) makes it observable.

#### 3.4.2 Push-segment boundary

Always materialize at the segment's outer `next_batch()` boundary. The driver calls `materialize_filtered_batch` exactly once per batch immediately before returning, so external observers (the engine, the `EntityOperatorAdapter`, downstream non-fused operators) only ever see contiguous `RecordBatch`es. This is what keeps `PhysicalOperator::next_batch()` returning `Result<Option<RecordBatch>>` unchanged from the Wave 1/2 contract.

#### 3.4.3 Aggregate hand-off

A non-fused `AggregatePhysical` immediately downstream of a fused segment receives the *materialized* batch from §3.4.2; nothing additional is needed. The accumulator's `update_batch` always sees a contiguous `RecordBatch`. The materialization has already happened at the segment's outer boundary — there is no second materialization step.

`HashAccumulator::update_batch` does not need to know about selection vectors. This is intentional: aggregation fusion (the *stateful*-to-aggregate path, §5) is a different mechanism that bypasses `update_batch` entirely.

### 3.5 Metrics

| Metric                                | Scope     | Definition                                                                                          |
| ------------------------------------- | --------- | --------------------------------------------------------------------------------------------------- |
| `selection_vector_materializations`   | per worker | Number of `materialize_filtered_batch` calls. Already promised in execution-model.md §16; this document is what makes it load-bearing. |
| `selection_vector_dropped_rows`       | per worker | Sum of `(batch.num_rows() - selection.len())` across kernel inputs. Lets us answer "how much row work did the chain skip." |
| `kernel_calls`                        | per kernel | Number of times each kernel's `apply` was invoked. Tagged by `kernel_name()`.                       |
| `kernel_time_ns`                      | per kernel | Wall-clock spent inside `apply`. Tagged by `kernel_name()`.                                         |

The first two land in TASK-518 as part of the segment scaffold. The two kernel-level metrics land alongside the `--explain-perf` work (TASK-524). Until then, the segment driver only reports the segment-level pair — which is enough to validate the sparsity threshold.

---

## 4. Fused Push-Segment Driver

The driver is a `PhysicalOperator` named `FusedStatelessSegment`. It wraps an ordered list of `StatelessKernel` and presents a normal pull-based interface to the rest of the engine.

### 4.1 Shape

```rust
pub struct FusedStatelessSegment {
    child: Box<dyn PhysicalOperator>,
    kernels: Vec<KernelStep>,
    /// Cached output schema (the last kernel's schema).
    output_schema: OperatorSchema,
    /// Sparsity threshold; `0.10` by default per §3.4.1.
    sparsity_factor: f64,
    /// State for kernels that need it. Today only LIMIT, which keeps a
    /// remaining-row budget. The slot is `Option<u64>` so non-LIMIT kernels
    /// pay no memory cost.
    limit_remaining: Option<u64>,
    metrics: Arc<Metrics>,
}

/// One step in the kernel chain. Filter and Project use the trait method;
/// Limit takes the in-driver fast path so it can read/write the remaining
/// counter without `&mut self` on the kernel itself.
enum KernelStep {
    Filter(Arc<dyn StatelessKernel>),
    Project(Arc<dyn StatelessKernel>),
    Limit { /* nothing — driver owns the counter */ },
}
```

Why an enum and not a uniform `Vec<Arc<dyn StatelessKernel>>`? Because LIMIT's "stateful" piece (the remaining-row counter) does not belong on `&self`, and the alternative — a `Mutex<u64>` on the kernel — adds a per-batch lock to a hot path for no benefit. The enum keeps the kernel trait pure and concentrates the one piece of mutable state in the driver. Filter and Project get `Arc<dyn StatelessKernel>` so the same kernel can be shared across worker threads in a future morsel-driven implementation.

### 4.2 `next_batch()` Algorithm

```text
loop:
    if limit_remaining == Some(0): return Ok(None)

    pull = child.next_batch()?
    match pull:
        None    -> return Ok(None)
        Some(b) -> let mut fb = FilteredBatch::dense(b)

    for step in &self.kernels:
        fb = match step:
            Filter(k)  -> k.apply(maybe_materialize(fb))?
            Project(k) -> k.apply(maybe_materialize(fb))?
            Limit      -> apply_limit(fb, &mut self.limit_remaining)

    if fb.live_rows() == 0:
        # Skip empty batches per the operator-traits.md §3 "non-empty
        # batches" guidance: re-pull rather than surfacing a 0-row
        # RecordBatch to a downstream consumer that may not be tolerant.
        continue

    let out = materialize_filtered_batch(fb, &self.metrics)?
    return Ok(Some(out))
```

`maybe_materialize` is the §3.4.1 sparsity check. `apply_limit` shrinks `fb.selection` (or slices `fb.batch`) and decrements `limit_remaining`. `materialize_filtered_batch` is the boundary helper — see §4.3.

The empty-batch loop is an explicit choice. operator-traits.md §3 allows operators to surface mid-stream zero-row batches as a courtesy, but the segment driver treats zero-row batches as the indicator that the previous filter dropped everything and pulls again. This avoids the case where a downstream `EntityOperatorAdapter` has to handle a zero-row contiguous batch and gets confused about entity boundaries.

### 4.3 `materialize_filtered_batch`

```rust
pub fn materialize_filtered_batch(
    fb: FilteredBatch,
    metrics: &Metrics,
) -> Result<RecordBatch> {
    match fb.selection {
        None => Ok(fb.batch),
        Some(sv) if sv.len() == fb.batch.num_rows() => {
            // Selection covers every row — no copy needed. This is sound
            // because `SelectionVector` is sorted-ascending without
            // duplicates (§3.1, and the type-level invariant on
            // `bqlite_core::encoded::SelectionVector::from_sorted`), so
            // `len() == num_rows()` ⇒ `sv == 0..num_rows()`. The
            // `is_dense` helper in selection.rs already collapses this
            // case for the encoded path; we reuse it.
            Ok(fb.batch)
        }
        Some(sv) => {
            metrics.inc_selection_vector_materializations();
            let mask = selection_to_bool_array(
                &RowSelection::from_indices(sv), fb.batch.num_rows() as u32);
            arrow::compute::filter_record_batch(&fb.batch, &mask)
                .map_err(BqliteError::Arrow)
        }
    }
}
```

Three points:

1. **One implementation, one call site per trigger.** Sparsity, segment boundary, and any future trigger all go through this helper. Operators never invent their own materialization path. This preserves the copy-budget metric and prevents the "two implementations diverged" failure mode.
2. **Reuses `selection_to_bool_array`.** That helper already exists in `bqlite-operators::selection` and is proven by the encoded scan path. The new `materialize_filtered_batch` is a thin wrapper that builds a `RowSelection::Indices` and delegates.
3. **Distinct from `materialize_selected`.** `materialize_selected` is the *encoded-path* boundary (`EncodedBatch + RowSelection -> RecordBatch`). `materialize_filtered_batch` is the *post-boundary* boundary (`FilteredBatch -> RecordBatch`). The two share the bool-array helper but differ in their input type and the metric they tag (encoded path measures bytes via `Metrics::record_bytes_*`, post-boundary path measures count via `selection_vector_materializations`).

### 4.4 Interaction with the Encoded Scan Path (§3.8.6)

The encoded scan path produces a *dense* `FilteredBatch { batch, selection: None }` at its materialization boundary. `FusedStatelessSegment` happens to consume `RecordBatch`es from its child rather than `FilteredBatch`es, so the integration looks like:

```text
ScanOperator (encoded path)
    next_batch() -> RecordBatch    # already materialized at §3.8.6 boundary
        |
        v
FusedStatelessSegment
    pull -> wrap in FilteredBatch::dense
    run kernels (filter narrows, project rewrites, limit truncates)
    materialize_filtered_batch at the outer next_batch boundary
    next_batch() -> RecordBatch
```

Two design alternatives we explicitly rejected:

- **A.** Have the scan emit `FilteredBatch` directly, threading selection vectors through the operator boundary. *Rejected.* `FilteredBatch` is a stateless-segment-internal type per §3.8.3. Exposing it at the `PhysicalOperator` boundary forces every consumer (engine, FFI, EntityOperatorAdapter) to reason about selection vectors, which defeats the whole point of having §3.8.2's "the public boundary is `RecordBatch`" rule.
- **B.** Skip the §3.8.6 boundary and let post-boundary kernels see encoded columns. *Rejected.* The encoded IR (`EncodedColumn`, `PinnedChunk`) is pinned in `bqlite-core::encoded` and is only meaningful between the scan and the encoded kernels. Threading it past the segment driver couples post-boundary kernels to encoding details that should be invisible there.

The chosen design keeps the two boundaries layered: the encoded path's materialization is a strictly internal scan-side decision; the segment-level materialization is a strictly internal post-boundary decision. Each path can evolve without invalidating the other.

### 4.5 Cancellation

`FusedStatelessSegment::next_batch` checks `QueryContext::cancelled` once at the top of the loop, before pulling from the child. The kernel chain itself runs to completion within a single batch; a long-running predicate or projection respects cancellation only at batch granularity, not row granularity. This matches the existing `FilterOperator::next_batch` and `LimitOperator::next_batch` cancellation behavior and operator-traits.md §6.

The driver does **not** check cancellation between kernels in the chain, even when the chain is long. The per-batch overhead is one atomic-load; the per-kernel overhead would be N atomic-loads for an N-kernel chain. For Wave 5 latency targets the per-batch granularity is the right tradeoff (batch sizes are bounded by the scan emitter's row-group size — typically 64K rows — and Wave 4 ATTRIBUTE/SESSIONIZE per-entity caps).

### 4.6 Optimizer Hand-Off

The optimizer's "fuse adjacent stateless operators" pass runs over the logical plan and produces a single `FusedSegmentPhysical` descriptor in place of the `FilterPhysical → ProjectPhysical → LimitPhysical` chain. The descriptor carries:

```rust
pub struct FusedSegmentPhysical {
    pub input: Box<PhysicalNode>,
    /// Kernels in pull order. Empty if the optimizer found nothing to
    /// fuse — in that case the optimizer simply does not emit this
    /// node (the chain stays as separate physical descriptors).
    pub steps: Vec<FusedSegmentStep>,
    pub sparsity_factor: f64,
    pub output_schema: OperatorSchema,
}

pub enum FusedSegmentStep {
    Filter { predicate: CompiledExpr, tile_size: usize },
    Project(Vec<ProjectPhysicalItem>),
    Limit(u64),
}
```

The bind step (TASK-518) translates `FusedSegmentPhysical` into a `FusedStatelessSegment` operator, the same way TASK-232 already translates `FilterPhysical` / `ProjectPhysical` / `LimitPhysical` into the legacy operators.

The optimizer rule that emits `FusedSegmentPhysical` is part of TASK-519, not TASK-518. TASK-518 builds the runtime infrastructure and drives it via a hand-written segment in unit tests; TASK-519 wires the optimizer.

---

## 5. Stateless ↔ Stateful Boundary

This document specifies *stateless* fusion (§3, §4). The other half of operator fusion — fusing a *stateful* operator into the aggregate immediately downstream of it — is governed by `EntityOperator::finish_entity_into()` overrides and the planner-pipeline.md §7.4 fusion eligibility rules. The contract here is:

1. **Stateless segments never fuse into a downstream aggregate.** A `FusedStatelessSegment` always materializes at its outer boundary (§3.4.2). Aggregation of the resulting `RecordBatch` happens via `HashAccumulator::update_batch` per the existing aggregate-operator.md §5 path.

   This is intentional. A stateless segment is one or more pure functions over a batch; the aggregate sees a dense `RecordBatch`. Trying to push the aggregate into the segment would require the segment to know about group keys, accumulator merging, and `MaxGroupsExceeded` — a coupling that buys nothing because `HashAccumulator::update_batch` is already vectorized over a dense batch.

2. **Stateful operators *do* fuse into a downstream aggregate**, via `EntityOperator::finish_entity_into()`. The default implementation (operator-traits.md §6, execution-model.md §4) materializes a per-entity `RecordBatch` and calls `update_batch`. The per-operator override (§5.1 below) updates the accumulator directly without that intermediate `RecordBatch`.

3. **The boundary is never crossed.** A pipeline like `scan -> filter -> project -> match -> stats(group by)` becomes:

   ```
   FusedStatelessSegment(filter, project)        # §3, §4
     -> EntityOperatorAdapter(SequenceMatchOperator with finish_entity_into)
        -> /* HashAccumulator lives inside MATCH; no separate Aggregate node */
   ```

   The two fusion mechanisms compose without overlapping. Stateless fusion compresses the pre-MATCH chain; stateful-to-aggregate fusion compresses the post-MATCH "build per-entity row → aggregate" hop.

### 5.1 `finish_entity_into` Overrides — v1 Decisions

For each Wave 4 stateful operator, this document fixes whether v1 ships a `finish_entity_into` override or accepts the default `finish_entity` → `update_batch` path. The decision matrix:

| Operator           | v1 override?       | Rationale | Source             |
| ------------------ | ------------------ | --------- | ------------------ |
| `SequenceMatch` (MATCH) | **Yes** — already shipped | The fused match-aggregate is the canonical example: the per-row hot loop has zero demand-related branches and the layered-extraction hooks (§4.2 of execution-model.md, §7.5 of planner-pipeline.md) update the accumulator directly without building an intermediate `RecordBatch`. The shape lives at `crates/bqlite-operators/src/matcher/mod.rs:377`. | match-operator.md §3, sequence-matching.md §13.4 |
| `Sessionize`       | **No in v1** — default path  | SESSIONIZE in v1 emits full per-session rows to a downstream STATS. The fusion targets in operators/sessionize.md §10.1 (session-fold count, fold sum/avg over forwarded columns) require holding per-session accumulators inside the operator and respecting an additional `eager_group_emit` option that the operator does not yet implement. Defer to TASK-520. | operators/sessionize.md §10 |
| `EventSelect` (FIRST/LAST/NTH) | **No in v1** — default path | Per-entity state is a single candidate row. The intermediate `RecordBatch` is one row × *N* columns; materializing it and feeding it to STATS is essentially the same work as a fused override would do. Defer to TASK-520 only if benchmarks show the per-entity allocation dominates. | operators/event-select-sample.md §12 |
| `Attribute`        | **No in v1** — default path | ATTRIBUTE already emits flat per-touchpoint rows; the unfused path is efficient. The operator currently rejects a populated `fused_aggregate` at construction (`crates/bqlite-operators/src/attribute.rs:175`). Defer to TASK-520 along with the three shapes in planner-pipeline.md §7.4.4. | operators/attribute.md §13 |

Two decisions are baked into this matrix and are worth surfacing:

- **MATCH is the only v1 fused stateful operator.** This was a Wave 3 decision — sequence-matching.md §13.4 specified it and the operator implementation has been live since TASK-321. This document does not re-litigate it.
- **TASK-520 (Stateful-to-aggregate fusion for SESSIONIZE / EventSelect / Attribute) is in scope.** The decision to defer the *implementation* to a follow-up task does not retire the design; planner-pipeline.md §7.4.2 / §7.4.3 / §7.4.4 stand. TASK-520 is responsible for the override implementations *and* the planner detection that sets `fused_aggregate`. The current operator-side construction-time assertions (`fused_aggregate.is_none()`) are temporary guards that TASK-520 will remove.

### 5.2 Why Not Push Stateful Operators Into Stateless Fusion

The other obvious-looking design — let the segment driver own a stateful kernel as one of the steps in its kernel chain — is ruled out by the entity-alignment guarantee. Stateful operators rely on `EntityOperatorAdapter` to feed them sub-batches that all belong to the same entity. The stateless segment driver does not know about entities and is one layer below the adapter. Pushing a stateful kernel into the segment chain would require either:

- Replicating entity boundary detection inside the segment driver (duplicating the adapter), or
- Threading a `StatefulKernel` trait through the segment with hooks for entity boundaries (effectively a second adapter, with its own `finish_entity` semantics).

Neither is worth the saving. The stateless segment runs once per scan-emitted batch; the entity adapter runs once per entity. Fusing the two would couple two independent dimensions of the pipeline — batch granularity vs. entity granularity — for no measurable benefit. The §5 boundary stays.

---

## 6. Pipeline Examples

### 6.1 `scan | where x > 0 | select y, z | limit 100`

```
FusedStatelessSegment {
    child: ScanOperator,
    kernels: [
        Filter { predicate: x > 0 },
        Project { items: [y, z] },
        Limit  { remaining: 100 },
    ],
}
```

Execution: scan emits a dense `RecordBatch`; the segment wraps it in `FilteredBatch::dense`; Filter narrows to `Some(sv_x_gt_0)`; Project rewrites columns at the post-selection row count and resets `selection: None`; Limit truncates the batch when needed; the boundary materializes (no-op because Project already produced a dense batch) and the segment returns `RecordBatch`.

Result: zero copy on the filter step, one copy on the project step (already minimum for a column-rewriting operator), zero copy on the limit step.

### 6.2 `scan | match step1 -> step2 | stats COUNT(*) GROUP BY entity`

```
EntityOperatorAdapter {
    child: ScanOperator,
    operator: SequenceMatchOperator {
        config: MatchExecutionConfig {
            fused_accumulator: Some(HashAccumulator { ... }),
            ..
        },
    },
}
```

No `FusedStatelessSegment` because there is no stateless chain. The match operator's `finish_entity_into` updates the accumulator directly via the layered-extraction hook (`acc.update(group_key, &reduced_values)`). The downstream `Aggregate` physical node is removed by the optimizer's fusion pass.

### 6.3 `scan | where region = 'US' | match step1 -> step2 | stats COUNT(*) GROUP BY plan`

```
EntityOperatorAdapter {
    child: FusedStatelessSegment {
        child: ScanOperator,
        kernels: [Filter { region = 'US' }],
    },
    operator: SequenceMatchOperator {
        config: MatchExecutionConfig {
            fused_accumulator: Some(HashAccumulator { ... }),
            step_properties: [ /* extracts s.plan */ ],
            ..
        },
    },
}
```

Both fusion mechanisms compose: stateless fusion collapses the pre-MATCH filter (a degenerate one-kernel chain — see §6.4 for when that's worthwhile), then the entity adapter feeds the segment's output to MATCH, and MATCH's fused accumulator handles the aggregate without a separate physical node.

### 6.4 Single-Kernel Chains

A "fused" segment with one kernel is structurally equivalent to the corresponding plain operator (`FilterOperator`, `ProjectOperator`, `LimitOperator`). The optimizer should still emit a `FusedStatelessSegment` for these cases because:

- The `selection_vector_materializations` metric becomes the canonical observability signal for the entire post-Wave-2 stateless surface, not "Wave 5 chains only."
- Single-kernel segments compose cheaply with each other across optimizer passes; if a later pass decides to add a kernel, it does so by appending to `FusedSegmentPhysical::steps` rather than re-introducing the legacy descriptors.

The Wave 1/2 plain-operator descriptors (`FilterPhysical`, `ProjectPhysical`, `LimitPhysical`) and operators (`FilterOperator`, `ProjectOperator`, `LimitOperator`) are retired by TASK-519. The optimizer rule emits `FusedSegmentPhysical` exclusively for stateless work. (The legacy descriptors remain in the codebase only as long as TASK-519 is in flight, then are deleted in the same checkpoint that flips the bind step over.)

---

## 7. Migration Plan and Test Bar

### 7.1 Follow-on `[IMPL]` Tasks

| Task     | Scope                                                                                                            |
| -------- | ---------------------------------------------------------------------------------------------------------------- |
| TASK-518 | Land `StatelessKernel`, `materialize_filtered_batch`, `FusedStatelessSegment`, plus the `selection_vector_materializations` and `selection_vector_dropped_rows` metric counters. The kernel implementations for Filter / Project / Limit are written in this task. The optimizer keeps emitting the legacy descriptors; the bind step has both code paths (one for legacy, one for `FusedSegmentPhysical`) but the optimizer exercises only the legacy path. Unit tests construct `FusedStatelessSegment` directly. |
| TASK-519 | Flip the optimizer to emit `FusedSegmentPhysical` for every stateless run. Delete `FilterPhysical` / `ProjectPhysical` / `LimitPhysical` and the legacy operator types. Extend the Wave 2 acceptance test (TASK-235 / TASK-245) so it asserts on the new descriptor and metrics, and extend `benches/wave2_*` so the new path is the default measurement. |
| TASK-520 | Implement `finish_entity_into` overrides for SESSIONIZE / EventSelect / ATTRIBUTE per planner-pipeline.md §7.4.2 / §7.4.3 / §7.4.4. Remove the construction-time `fused_aggregate.is_none()` guards from those operators. Add the planner detection passes for each shape and equivalence tests against the unfused path. |

These three tasks are sequenced: TASK-518 ships scaffolding and one round of unit tests, TASK-519 makes the new path load-bearing across the codebase, TASK-520 extends the same approach across the Wave 4 stateful operators.

### 7.2 Test Bar

Unit tests added by TASK-518:

- Filter narrows: every kernel respects `selection: Some(_)` input and produces `selection: Some(_)` output regardless of selectivity.
- Project rewrites: `selection: None` output, column count matches the project descriptor, output row count equals input post-selection row count.
- Limit truncates: budget honored across multiple input batches; `selection: Some(_)` and `selection: None` cases both covered.
- Sparsity boundary: at `selection.len() / batch.num_rows() == sparsity_factor`, materialization happens at the next kernel; just above, it does not.
- Empty batches loop: zero-row `FilteredBatch` after a fully-rejecting filter triggers a child re-pull rather than a zero-row return.
- Cancellation between batches: after the cancellation flag is set, `next_batch` returns `Err(Cancelled)` on the next call.

Property tests (TASK-518 or TASK-519, the property-test bar from CLAUDE.md applies):

- **Equivalence with legacy operators.** For arbitrary `(scan, filter predicate, project items, limit)` 4-tuples, the result of `FusedStatelessSegment(...)` equals the result of `Limit(Project(Filter(scan)))` row-for-row. This is the load-bearing correctness invariant for the migration.
- **Materialization count bound.** For a chain of *N* filters with selectivity *s*, the number of `materialize_filtered_batch` calls is *O(1)* per scan batch (segment boundary) plus an additional *O(1)* if any intermediate `s` falls below the sparsity threshold. Expressed as: `mat_calls <= 1 + (num batches × num_kernels_below_sparsity)`.

Benchmarks (TASK-519 finalizes them):

- `scan_filter_project_limit_throughput` — must beat the Wave 2 baseline (`Limit(Project(Filter(scan)))`) on the standard 1M-row purchases fixture by ≥1.2× on the dense-selectivity case (selectivity ≥ 0.5) and ≥1.5× on the sparse-selectivity case (selectivity ≤ 0.1). Numbers are aspirational targets, not gates — the Wave 5 gate is "no regression" plus a directional improvement that beats the noise floor.
- `selection_vector_materializations_per_query` — a microbench that asserts the count is exactly 1 per scan batch on a dense-selectivity case (only the boundary materialization runs) and exactly 2 per scan batch when an intermediate filter triggers sparsity-boundary materialization.

### 7.3 Reconciliation With Existing Docs

This document updates the picture in two places. The owners-doc updates land in the same checkpoint as TASK-518 unless otherwise noted:

- `docs/design/execution-model.md` §3.8: change the lead-in note from "the Wave 2 filter/project/limit operators (TASK-231) deliberately ship without that infrastructure because a fused push segment is required to make the selection-vector chain pay off" to point at `engine/operator-fusion.md` for the implementation contract. The §3.8.1–§3.8.5 normative content stays. (Done by TASK-518.)
- `docs/design/INDEX.md`: add an "Engine" subsection under "Per-subsystem implementation notes" referencing this document. (Done by this task — INDEX.md is updated in the same checkpoint that lands operator-fusion.md.)

`docs/design/operators/match-operator.md`, `operators/sessionize.md`, `operators/event-select-sample.md`, and `operators/attribute.md` already point at TASK-503 as the Wave 5 fusion target. No additional doc updates are required from those owners — the Wave 5 fusion task list in each is the contract this document discharges, and TASK-520 carries the per-operator doc updates as it implements the overrides.

---

## 8. Open Decisions

The following decisions are deferred but do not block TASK-518 / TASK-519 / TASK-520:

1. **Sparsity factor knob exposure.** Whether `sparsity_factor` is a database-level config (`OPEN OPTIONS`), a per-query hint, or a bench-only knob. Default `0.10` is fine for v1; the knob lands on `FusedSegmentPhysical` so the planner can mutate it, but the user-facing surface is TBD. Punted to TASK-524 / `--explain-perf`.
2. **Per-kernel cancellation granularity.** Whether long-running predicates (regex, complex string ops) should check `QueryContext::cancelled` between rows. Left out of v1; will be reconsidered if bench latencies show batch-granularity cancellation dominating. Tracked alongside TASK-505's cancellation latency bound work.
3. **Batched limit consumption.** Whether `Limit` should signal "I am at zero, please tear me down" to the child rather than waiting for the next pull. Today the driver short-circuits on the next call; a more aggressive teardown would close the child immediately. Deferred — the resource cost of one extra pulled-and-discarded batch is bounded by `target_output_rows`.
