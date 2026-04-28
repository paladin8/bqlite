# Aggregate + Hash-Accumulator Architecture

> **Status**: DRAFT
> **Task**: TASK-308
> **Depends on**: none
> **Depended on by**: TASK-307 (hash aggregate operator), TASK-309 (wave 3 lowering), TASK-317 (operator stubs), TASK-318 (planner lowering), TASK-320 (match-aggregate fusion), TASK-321 (sequence match operator), TASK-327 (DDSketch percentiles)

---

## 1. Design Goals

The aggregate framework serves three constraints from execution-model.md:

**Zero intermediate allocation on the fused path (Belief 1).** When a stateful entity operator (MATCH, SESSIONIZE, ATTRIBUTE) feeds directly into an aggregation, the fused path updates accumulators without materializing per-entity rows. This is the dominant performance win for funnel and retention queries.

**Bounded memory with no spill (v1 constraint).** Group cardinality is hard-capped at `max_groups` (default 1,000,000). At ~100 bytes per group, 1M groups fit within the 3 GiB query budget. Spill-to-disk is deferred to v2. The cap is enforced inside `HashAccumulator::update` and surfaces as a typed error.

**Extensibility for new accumulators.** TASK-327 adds DDSketch-based percentile accumulators (`P50`, `P90`, `P95`, `P99`) by extending `AggState` with a `Percentile` variant. The `Accumulator` trait surface does not change — extensibility flows through `AggState` variants and `AggFunction` enum members.

---

## 2. Trait Hierarchy

```text
                  ┌──────────────┐
                  │  Accumulator │   (trait, dyn-safe, Send)
                  └──────┬───────┘
                         │
              ┌──────────┴──────────┐
              │   HashAccumulator   │   (concrete default impl)
              └─────────────────────┘
```

`Accumulator` is the protocol trait. `HashAccumulator` is the sole v1 implementor. The trait is object-safe so fused operators can hold `Box<dyn Accumulator>`.

---

## 3. The `Accumulator` Trait

```rust
pub trait Accumulator: Send {
    fn update(
        &mut self,
        group_key: Option<&[ScalarValue]>,
        values: &[ScalarValue],
    ) -> Result<()>;

    fn update_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()>;

    fn finish(&self) -> Result<RecordBatch>;

    fn memory_usage(&self) -> usize;

    fn as_any(&self) -> &dyn Any;
}
```

### 3.1 Method Contract

| Method | Called by | Frequency |
|--------|-----------|-----------|
| `update(group_key, values)` | Fused entity operators via `finish_entity_into` | Once per entity/match/session |
| `update_batch(batch)` | Non-fused `AggregatePhysical` operator; default `finish_entity_into` | Once per upstream `RecordBatch` |
| `merge(other)` | Coordinator, after all shards complete | `num_shards - 1` times |
| `finish()` | Coordinator, after final merge | Once |
| `memory_usage()` | Memory tracker (informational in v1) | Periodically |
| `as_any()` | `merge()` for downcasting | Same as merge |

### 3.2 Error Model

- `update` / `update_batch`: returns `Err(BqliteError::Execution(...))` when `max_groups` is exceeded by a new group key.
- `merge`: returns `Err` if the merged result would exceed `max_groups`, or if the other accumulator is not the expected concrete type.
- `finish`: returns `Err(BqliteError::Arrow(...))` on Arrow construction failure (should not happen with correct schemas).

### 3.3 Deviations from execution-model.md §9.5

The implementation adds `Result` return types to all mutating methods and `finish`. execution-model.md §9.5 shows bare returns (no `Result`) because that section predates the `max_groups` enforcement decision. The `Result` additions are intentional — they let `max_groups` overflow surface as a typed error rather than a panic.

`finish` takes `&self` instead of `&mut self`. This allows calling `finish` multiple times (useful for debugging and testing) without consuming the accumulator.

---

## 4. `HashAccumulator` — The Default Implementation

```rust
pub struct HashAccumulator {
    groups: HashMap<GroupKey, Vec<AggState>>,
    output_schema: OperatorSchema,
    max_groups: usize,
    functions: Vec<AggFunction>,
    input_types: Vec<Option<BqlType>>,
    group_by_columns: Vec<String>,
    agg_arg_columns: Vec<Option<String>>,
}
```

### 4.1 GroupKey

```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupKey(pub Vec<ScalarValue>);
```

The group-by values tuple. For ungrouped aggregation (`STATS COUNT(*)` without `GROUP BY`), the key is an empty `Vec`. The `ScalarValue` type provides `Eq + Hash + Ord` with proper float handling (NaN == NaN via `total_cmp`, consistent hashing via `to_bits()`).

### 4.2 Construction

```rust
HashAccumulator::new(
    functions: Vec<AggFunction>,
    input_types: Vec<Option<BqlType>>,
    output_schema: OperatorSchema,
    group_by_columns: Vec<String>,
    agg_arg_columns: Vec<Option<String>>,
    max_groups: usize,
)
```

The physical planner constructs a `HashAccumulator` from the `AggregatePhysical` descriptor. Each aggregate function gets one `AggState` slot per group, initialized by `AggState::new(function, input_type)`.

### 4.3 Group Cardinality Limit

The `max_groups` cap (default `DEFAULT_MAX_GROUPS = 1_000_000`) is enforced inside `get_or_create_group()`. When the cap is reached and a new group arrives, the method returns `Err(BqliteError::Execution(...))`. The error propagates upward through the operator tree and aborts the query.

There is no spill-to-disk for aggregation state in v1. At ~100 bytes per group, 1M groups occupy ~100 MB — well within the 3 GiB query budget. Aggregate is on the fail-fast row of the per-operator policy in [`engine/memory-budget.md`](../engine/memory-budget.md) § 7 and the v1 spill surface in [`engine/spill.md`](../engine/spill.md) § 3.

---

## 5. Per-Function Accumulator State: `AggState`

```rust
pub enum AggState {
    Count(u64),
    CountNonNull(u64),
    Sum(SumState),
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg { sum: f64, count: u64 },
    CountDistinct(HashSet<ScalarValue>),
    Variance { count: u64, mean: f64, m2: f64 },
}
```

### 5.1 Null Propagation

All aggregate functions skip NULL input values (per type-system.md §3.3). Specifically:

- `COUNT(*)` counts all rows regardless of NULLs — it uses `update_count_star()`, not `update(value)`.
- `COUNT(col)` counts non-NULL values.
- `SUM`, `AVG`, `MIN`, `MAX`, percentiles: skip NULLs. If all inputs are NULL, the result is NULL (except COUNT variants which return 0).
- `COUNT_DISTINCT(col)`: NULL values are not inserted into the hash set.

### 5.2 Type Preservation

- `SUM(Int)` returns `Int` (via `SumState::Int`). `SUM(Float)` returns `Float` (via `SumState::Float`). No cross-type promotion.
- `AVG` always returns `Float` regardless of input type.
- `MIN`/`MAX` preserve the input type.
- `COUNT` variants always return non-nullable `Int`.

### 5.3 Mergeability

Every `AggState` variant supports pairwise merge for cross-shard reduction:

| `AggState` | Merge operation |
|------------|----------------|
| `Count` | Sum the counts |
| `CountNonNull` | Sum the counts |
| `Sum` | Sum the sums |
| `Min` | Take the smaller |
| `Max` | Take the larger |
| `Avg` | Sum the sums and the counts |
| `CountDistinct` | Set union |
| `Variance` | Parallel Welford merge formula |

Merge panics on variant mismatch — this would indicate a plan-time bug where different shards were configured with different aggregate functions.

### 5.4 SumState

```rust
pub enum SumState {
    Int(i64),
    Float(f64),
}
```

Tracks SUM without cross-type promotion. Integer overflow wraps (Rust's default). The float path uses IEEE 754 addition. Explicit overflow detection is a Wave 5 concern.

---

## 6. Aggregate Function Enum: `AggFunction`

```rust
pub enum AggFunction {
    Count,          // COUNT(*)
    CountColumn,    // COUNT(col)
    CountDistinct,  // COUNT_DISTINCT(col)
    Sum,            // SUM(col)
    Min,            // MIN(col)
    Max,            // MAX(col)
    Avg,            // AVG(col)
    P50, P90, P95, P99,  // Percentiles (TASK-327)
}
```

Lives in `bqlite-core` so both `bqlite-planner` (for `FusableAggregate`) and `bqlite-operators` (for `HashAccumulator`) can reference it.

### 6.1 Output Type Rules

Per type-system.md §6.4:

| Function | Input | Output | Nullable |
|----------|-------|--------|----------|
| `COUNT(*)` | none | `Int` | no |
| `COUNT(col)` | any | `Int` | no |
| `COUNT_DISTINCT(col)` | any | `Int` | no |
| `SUM(Int)` | `Int` | `Int` | yes |
| `SUM(Float)` | `Float` | `Float` | yes |
| `AVG(Int\|Float)` | `Int\|Float` | `Float` | yes |
| `MIN(col)` | `Int\|Float\|String\|Timestamp` | same | yes |
| `MAX(col)` | `Int\|Float\|String\|Timestamp` | same | yes |
| `P50–P99` | `Int\|Float` | `Float` | yes |

### 6.2 Incremental Computability

All Wave 3 aggregates are incrementally computable (updatable one entity at a time). This is a design invariant — it means fusion eligibility never fails due to an unsupported aggregate function (execution-model.md §8.4).

---

## 7. Output Schema Rules

The output schema of an `Aggregate` node is:

1. **Group columns first**, in the order declared in `GROUP BY`. Each column retains its input type and nullability.
2. **Aggregate columns next**, one per aggregate function, in the order declared in `STATS`. Column names are the explicit aliases from the BQL source (query-language.md §7.1 requires explicit naming — no anonymous aggregates).

Example:
```
STATS total = SUM(amount), n = COUNT(*) GROUP BY country, device
```
Output schema: `[country: String, device: String, total: Int, n: Int]`.

The `OperatorSchema` is computed at plan time and carried on the `AggregatePhysical` descriptor. It never changes during execution.

---

## 8. Aggregate Expression Compilation

Aggregate expressions are compiled through the two-stage expression pipeline from TASK-205:

1. **AST → TypedAggExpr**: The planner resolves the `AggItem` from the parser AST into a `TypedAggExpr` containing the resolved `AggFunction`, a typed argument expression (`TypedExpr`), and a group-by context.

2. **TypedAggExpr → CompiledAgg**: The physical planner compiles the typed aggregate into a `CompiledAgg` containing the `AggFunction`, a `CompiledExpr` for the argument (if any), and `CompiledExpr` entries for each group-by key.

The `HashAccumulator` uses `CompiledExpr` evaluation (via the `eval` module) to extract group-by key values and aggregate argument values from each `RecordBatch` in the `update_batch` path.

---

## 9. Fused Downstream Protocol

The fused-downstream protocol lets stateful entity operators (MATCH, SESSIONIZE, ATTRIBUTE) feed entities directly into an accumulator without materializing intermediate rows.

### 9.1 EntityOperator Extension

```rust
pub trait EntityOperator: Send + Sync {
    // ... existing methods ...

    fn finish_entity_into(
        &self,
        state: Self::State,
        accumulator: &mut dyn Accumulator,
    ) -> Result<()> {
        // Default: materialize and batch-update.
        if let Some(batch) = self.finish_entity(state) {
            accumulator.update_batch(&batch)?;
        }
        Ok(())
    }
}
```

The default implementation calls `finish_entity()` and feeds the result into the accumulator, propagating any `max_groups` overflow error. Operators that support fusion override this to skip the per-entity `RecordBatch` materialization entirely. For example, a fused MATCH step-counter directly calls `accumulator.update(group_key, &[ScalarValue::Int(step_count)])` without creating any Arrow arrays.

### 9.2 Adapter Integration

The `EntityOperatorAdapter` (landing with TASK-307/TASK-321) selects the path:

- If `fused_accumulator` is `Some`: calls `finish_entity_into(state, accumulator)`.
- If `fused_accumulator` is `None`: calls `finish_entity(state)` and appends to `output_buffer`.

### 9.3 Fusion Eligibility

An `Aggregate` fuses into an upstream stateful operator when all conditions from planner-pipeline.md §7.2 are met:

1. **Adjacency**: the aggregate is immediately downstream (optionally with an intervening `Filter`).
2. **Incremental computability**: all aggregate functions are incremental (always true for the v1 set).
3. **Group-by key availability**: every group-by expression references columns in the stateful operator's output.
4. **No ordering dependency**: no `ORDER BY` between the stateful operator and the aggregate.

The optimizer's Pass 6 detects fusable patterns, extracts a `FusableAggregate`, and sets `fused_downstream` on the stateful operator's physical descriptor. The physical planner emits a single fused physical operator.

---

## 10. Extensibility Contract (TASK-327)

TASK-327 adds DDSketch-based percentile accumulators by:

1. Adding a `Percentile(DDSketch)` variant to `AggState`.
2. Implementing `AggState::new()` for `P50`/`P90`/`P95`/`P99` to create `Percentile(DDSketch::new())`.
3. Implementing `update`, `merge`, and `finalize` on the new variant.

No changes to the `Accumulator` trait, `HashAccumulator` struct, or `AggFunction` enum are needed — the `AggFunction` variants (`P50`–`P99`) already exist and are wired to select the correct `AggState` at construction time. Currently they fall back to `Avg` as a placeholder; TASK-327 replaces this.

DDSketch properties:
- ~1–2 KB per group per percentile.
- Constant-time merge via `DDSketch::merge()`.
- Bounded relative error (configurable, default 1%).
- Incremental — satisfies the fusion eligibility invariant.

---

## 11. Crate Placement

| Type | Crate | Rationale |
|------|-------|-----------|
| `ScalarValue` | `bqlite-core` | Shared scalar type for group keys and values; needed by both planner and operators |
| `AggFunction` | `bqlite-core` | Shared aggregate function enum; needed by both planner and operators |
| `Accumulator` trait | `bqlite-operators` | Consumed by operators and the engine |
| `HashAccumulator` | `bqlite-operators` | Default accumulator implementation |
| `AggState`, `SumState` | `bqlite-operators` | Per-function accumulator state |
| `GroupKey` | `bqlite-operators` | Hash-map key type for group-by |
| `FusableAggregate` | `bqlite-planner` | Optimizer's fusion descriptor (planner-pipeline.md §5.3) |

---

## 12. Memory Accounting

`HashAccumulator::memory_usage()` reports the estimated heap size:

- Per-group overhead: `sizeof(GroupKey) + sizeof(Vec<AggState>)` + map bucket overhead.
- Per-key payload: string heap allocations in group key values.
- Per-state payload: `CountDistinct` hash set entries; all other states are fixed-size.

This is informational in v1 — there is no spill trigger. The `max_groups` cap is the only defense against runaway cardinality.

---

## 13. Decision Summary

| Question | Decision | Rationale |
|----------|----------|-----------|
| Accumulator trait location | `bqlite-operators` | Consumed by operators and engine; follows crate map |
| ScalarValue location | `bqlite-core` | Needed by both planner and operators without new dep edges |
| Group key representation | `Vec<ScalarValue>` with custom Hash/Eq | Simple, correct, handles floats via total_cmp |
| Float hashing | `to_bits()` for Hash, `total_cmp` for Eq | NaN == NaN for GROUP BY correctness |
| Aggregation spill | None in v1; hard `max_groups` cap | Simplicity; 1M groups × ~100 bytes = ~100 MB |
| Percentile mechanism | DDSketch (TASK-327) | Constant-time merge, bounded error, incremental |
| Variance algorithm | Welford's online with parallel merge | Numerically stable, mergeable |
| Fusion protocol | `finish_entity_into` default impl on EntityOperator | Non-breaking additive extension; operators override for zero-alloc |
| SUM overflow | Wrapping (i64) / IEEE 754 (f64) | Consistent with Rust defaults; explicit detection deferred |
