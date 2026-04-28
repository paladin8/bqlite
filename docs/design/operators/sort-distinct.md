# Sort + Distinct Operator Contracts

**Wave**: 3
**Task**: TASK-310
**Status**: draft
**Unblocks**: TASK-317 (logical/physical plan variants), TASK-322 (operator implementations)
**Depends on**: none (design-only; implementation references TASK-205, TASK-307)

---

## 1. Scope

This note specifies the contracts for the two non-matching stateful operators shipped in Wave 3:

- **`SortOperator`** — materializes the entire input in memory, applies Arrow `lexsort`, and emits sorted output batches. Bounded by a hard `max_rows` cap; no spill (deferred to Wave 5 TASK-502).
- **`DistinctOperator`** — streams input batches through a hash-set filter, emitting only first-occurrence rows. Bounded by a hard `max_groups` cap; no spill.

Both operators are **relational** (not entity-streaming): they sit above all entity-aware operators in the Wave 3 plan tree, receive rows in arbitrary order, and do **not** preserve `(entity_id, ts)` ordering. This is safe because no downstream operator in Wave 3 requires entity-sorted input.

What this doc does **not** cover:

- The full planner lowering rules for `Sort` / `Distinct` logical nodes — see `planner/wave3-lowering.md` (TASK-309).
- Logical plan node field definitions — see `planner/logical-plan-nodes.md`.
- The hash-key kernel and `GroupKey` type used by `DistinctOperator` — see `operators/aggregate-operator.md` (TASK-308) and `execution-model.md §9.5`.
- Expression compilation — see `planner/expression-compilation.md` (TASK-205).

---

## 2. Relationship to Other Docs

| Topic | Authoritative doc | Role here |
|---|---|---|
| `PhysicalOperator` trait lifecycle | `operators/operator-traits.md` | Both operators implement this trait. |
| `CompiledExpr` for sort keys | `planner/expression-compilation.md` (TASK-205) | Sort key expressions are compiled via this pipeline. |
| `GroupKey` / hash-key kernel | `operators/aggregate-operator.md` (TASK-308) | `DistinctOperator` reuses the hash-key kernel from the aggregate operator. |
| Null-ordering rules | `query-language.md §15` | NULLs last in ASC, NULLs first in DESC — this doc enforces that convention. |
| Error variant usage | `operators/operator-traits.md §4.3` | Both operators raise `BqliteError::Execution` on cap overflow. |
| Plan tree placement | `planner/wave3-lowering.md` (TASK-309) | Defines where Sort / Distinct appear in the logical plan; this doc only specifies operator-level behavior. |
| Sort spill (future) | Wave 5 TASK-513 (protocol: `engine/spill.md`) | Sort spill is explicitly deferred from Wave 3; this doc specifies the no-spill hard-cap policy for Wave 3. The Wave 5 protocol is owned by `engine/spill.md` § 6.1. |

### 2.1 Deviation from execution-model.md §10.3

`execution-model.md §10.3` ("Spill-to-Disk", written at Wave 0) described sort spill (sorted runs merged from disk) as a v1 feature. **Wave 3 deliberately does not implement sort spill.** The rationale:

- `SortOperator` sits at the top of a post-aggregation pipeline where input cardinality is already bounded by the aggregate's `max_groups` cap (typically 1M rows, ~100 bytes each → ~100 MB). A hard `max_rows` cap at the same cardinality makes spill unnecessary for the Wave 3 workload.
- Spill adds implementation complexity (temp files, merge-sort pass, cleanup on cancel/error) that is disproportionate to Wave 3's scope.
- The hard-cap error gives query authors a clear signal to add a pre-sort aggregate or `LIMIT`.

Sort spill is tracked as Wave 5 TASK-513; the on-disk file layout, naming, spill-root configuration, and cleanup contract are owned by [`engine/spill.md`](../engine/spill.md) (TASK-502). When TASK-513 ships, `SortOperator` participates by construction — every sort op receives the engine's `Arc<dyn MemoryBudget>` and registers a spill handler — so no per-physical-descriptor opt-in field is needed; the engine's `EngineConfig::spill_root` is the single configuration point. The Wave 3 in-memory hard-cap path is preserved as the zero-spill case (no run is written, the merge degenerates to "emit the in-memory run").

---

## 3. SortOperator

### 3.1 Physical Descriptor

```rust
/// Physical descriptor for Sort — carried on the physical plan.
/// Materialized into a `SortOperator` instance by the engine bind step (TASK-323).
pub struct SortPhysical {
    /// Compiled sort keys in priority order (primary, secondary, ...).
    /// Each key is a `CompiledExpr` that evaluates to a scalar value
    /// over an input batch, plus a direction.
    pub keys: Vec<(CompiledExpr, SortDirection)>,

    /// Hard cap on total input rows.  Default: 10_000_000 (10M).
    /// The operator returns `BqliteError::Execution` if this is exceeded.
    pub max_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}
```

`SortDirection` is shared with the logical plan's `Sort` node. The `CompiledExpr` here is the physical form produced by TASK-205's compilation pipeline from a `TypedExpr`.

**Descriptor vs. bind-time fields.** `SortPhysical` carries only static, plan-time configuration. The engine bind step (TASK-323) supplies the runtime-only fields — the child `Box<dyn PhysicalOperator>` and the `Arc<CancellationToken>` — at operator construction time. These are not serialized into the descriptor.

### 3.2 Algorithm

`SortOperator` implements `PhysicalOperator` (operator-traits.md §4). Its execution proceeds in two phases:

**Phase 1 — accumulation** (`open()` + first-call lazy drain):

```
open():
  set buffer = empty RecordBatch list
  set total_rows = 0

next_batch() — phase 1:
  while input.next_batch() returns Some(batch):
    total_rows += batch.num_rows()
    if total_rows > max_rows:
      return Err(BqliteError::Execution(
        "SortOperator: input row count {total_rows} exceeds max_rows limit {max_rows}"
      ))
      # matches Section 6 error contract: {total_rows} → {n}, {max_rows} → {limit}
    push batch into buffer
  # transition to phase 2 on first next_batch() call after input exhausted
```

**Phase 2 — sort and drain**:

```
  concat buffer into a single RecordBatch (zero-copy via Arrow concat_batches)
  build Arrow SortOptions per key:
    Asc  → SortOptions { descending: false, nulls_first: false }  # NULLs last
    Desc → SortOptions { descending: true,  nulls_first: true  }  # NULLs first
  evaluate each CompiledExpr over the concatenated batch → Array columns
  call arrow::compute::lexsort_to_indices(sort_columns, None)
  call arrow::compute::take(batch, indices) → sorted RecordBatch
  split sorted batch into output_batches of target size (DEFAULT_OUTPUT_BATCH_SIZE rows)
  drain output_batches one per next_batch() call
  return Ok(None) when drained
```

`DEFAULT_OUTPUT_BATCH_SIZE` is the pipeline-wide constant `65_536` from `operator-traits.md §4.2`. This is not a field on `SortPhysical` — it is a shared constant, not a per-operator setting. TASK-322 must reference this constant by name (not hardcode the integer) so that the value can be changed in one place if the pipeline default ever changes. The batch-size target is soft: the final output batch may be smaller than `DEFAULT_OUTPUT_BATCH_SIZE` when the total sorted row count is not a multiple.

The implementation is single-threaded (no parallel merge-sort). Parallelism is at the query-level (multiple shards), not within the operator.

### 3.3 Null Ordering

Null ordering follows `query-language.md §15` (ORDER BY / LIMIT) and the Oracle/DuckDB/BigQuery convention:

| Direction | NULL position | Arrow `SortOptions` |
|---|---|---|
| `ASC` | **last** | `nulls_first: false` |
| `DESC` | **first** | `nulls_first: true` |

This is a fixed, unconditional policy. BQL does not expose `NULLS FIRST` / `NULLS LAST` modifiers in its `ORDER BY` surface syntax (Wave 3 scope). The convention matches DuckDB, BigQuery, and Oracle. Note that Postgres has the same default null-ordering behavior (NULLs last in ASC, NULLs first in DESC), but it also exposes explicit `NULLS FIRST` / `NULLS LAST` override syntax that BQL does not support in Wave 3.

### 3.4 Stability

Arrow's `lexsort_to_indices` is **stable**: rows with equal sort keys appear in their original input order in the output. Wave 3 ships stable sort only. An unstable variant (for performance) may be offered in a later wave but requires an AST/physical flag to control it — out of scope here.

### 3.5 Output Schema

The output schema is **identical to the input schema**: same columns, same types, same nullability. `SortOperator::output_schema()` returns a clone of its input's `OperatorSchema`.

The sort operation never adds, removes, or renames columns.

### 3.6 Memory Accounting

The operator accumulates all input rows in memory before sorting. Rough bound:

```
memory ≈ total_input_bytes × 2
```

(×2 for the `take` indices plus the rearranged output batch). At `max_rows = 10M` rows and a representative row width of ~100 bytes, peak memory is ~2 GB — within the query memory budget.

The operator **does not** register with a `MemoryBudget` tracker in Wave 3 (the trait shipped in TASK-111; the production `MemoryTracker` implementation lands with TASK-510 per `engine/memory-budget.md`). The `max_rows` hard cap is the sole protection against unbounded memory growth in Wave 3.

### 3.7 Cancellation

At the top of every `next_batch()` call, the operator checks `CancellationToken::is_cancelled()` (operator-traits.md §5). If the token is set, it returns `Err(BqliteError::Cancelled)` immediately — before accumulating a new input batch or draining an output batch.

### 3.8 Entity Ordering

`SortOperator` does **not** preserve `(entity_id, ts)` ordering. After Sort, rows are ordered by the user-specified sort keys only. This intentionally violates the `(entity_id, ts)` batch invariant stated in `operator-traits.md §4.2`; that invariant applies to the entity-streaming layer, not to post-aggregation relational operators. In the Wave 3 plan tree, Sort always sits above entity-aware operators (`SequenceMatchOperator`, the entity scan adapter). No operator below Sort in the plan may rely on entity-sorted input.

---

## 4. DistinctOperator

### 4.1 Physical Descriptor

```rust
/// Physical descriptor for Distinct — carried on the physical plan.
/// Materialized into a `DistinctOperator` instance by the engine bind step (TASK-323).
pub struct DistinctPhysical {
    /// Hard cap on distinct row count.  Default: 1_000_000 (1M).
    /// Matches the aggregate operator's `max_groups` default.
    pub max_groups: usize,
}
```

Unlike `SortPhysical`, `DistinctPhysical` carries no key expressions: the key is always **all columns** of the input schema, computed via the hash-key kernel from TASK-307.

**Descriptor vs. bind-time fields.** Same as `SortPhysical`: the engine bind step (TASK-323) supplies the child operator and cancellation token at construction time.

### 4.2 Algorithm

`DistinctOperator` implements `PhysicalOperator` and processes input **streaming** (batch-by-batch), avoiding full materialization:

```
open():
  seen = HashSet<GroupKey>::new()

next_batch():
  check CancellationToken
  batch = input.next_batch()?
  if batch is None: return Ok(None)

  for each row in batch:
    key = hash_key_kernel(row)   # TASK-307's GroupKey construction
    if key not in seen:
      if seen.len() >= max_groups:
        return Err(BqliteError::Execution(
          "DistinctOperator: distinct row count exceeds max_groups limit {max_groups}"
        ))
        # matches Section 6 error contract: {max_groups} → {limit}
      seen.insert(key)
      mark row as included
    else:
      mark row as excluded

  build output batch from included rows via Arrow selection vector
  if output batch is empty: recurse (loop to next input batch)
  return Ok(Some(output_batch))
```

The selection vector (a `BooleanArray` or `UInt32Array` of row indices) is used with `arrow::compute::filter` or `arrow::compute::take` to produce the output batch without copying data row-by-row.

**Note on the pseudocode**: the `for each row in batch` loop is high-level. The actual TASK-307 hash-key kernel constructs `GroupKey` values columnar-batch-at-a-time (not one-at-a-time from a materialized row), consistent with the project convention of avoiding per-row heap allocation in hot loops (CLAUDE.md performance conventions). TASK-322 must follow the TASK-307 kernel interface, not implement a naive row-at-a-time loop.

### 4.3 GroupKey Construction

`DistinctOperator` reuses the hash-key kernel introduced by TASK-307 (aggregate operator). The key is constructed from **all columns** of the input batch, in column order. This matches `SELECT DISTINCT`'s semantics: two rows are equal if and only if every column value is equal (SQL three-valued: NULL = NULL for equality in DISTINCT, per standard SQL).

```rust
// Conceptual form; exact type lives in bqlite-operators per TASK-307
pub struct GroupKey(SmallVec<[ScalarValue; 4]>);
```

Hashing uses `AHashMap` / `AHashSet` for performance. The `GroupKey` type's `Hash` and `Eq` implementations treat `NULL` as equal to `NULL` (grouping semantics, not SQL comparison semantics).

### 4.4 NULL Handling in Deduplication

Following standard SQL `SELECT DISTINCT` semantics:

- `NULL = NULL` is **true** for deduplication purposes (two rows both having NULL in the same column are considered duplicates).
- This differs from SQL comparison semantics (`NULL = NULL` is NULL/unknown) but matches the GROUP BY / DISTINCT convention.

The `GroupKey::Eq` implementation handles this explicitly by comparing `ScalarValue::Null == ScalarValue::Null` as `true`.

### 4.5 Output Schema

The output schema is **identical to the input schema**: same columns, same types, same nullability. `DistinctOperator::output_schema()` returns a clone of its input's `OperatorSchema`.

`Distinct` never adds or removes columns. The `SELECT DISTINCT` pipeline lowers to `Distinct(Project(...))` — the projection step is separate.

### 4.6 Memory Accounting

Memory is dominated by the `HashSet<GroupKey>`. At `max_groups = 1M` and ~80 bytes per `GroupKey` entry (SmallVec inline storage + hash table overhead), peak memory is ~80 MB — well within the query budget.

Like `SortOperator`, `DistinctOperator` does not register with a `MemoryBudget` tracker in Wave 3. The `max_groups` hard cap is the sole overflow protection. Wave 5 (TASK-510 per `engine/memory-budget.md` § 7) wires Distinct into the per-query budget; on overflow Distinct fails fast with `MemoryBudgetExceeded` (no spill).

### 4.7 Entity Ordering

`DistinctOperator` does **not** preserve `(entity_id, ts)` ordering. Rows are emitted in the order they first appear in the input stream, which is determined by the upstream operator's output order (typically unordered or aggregate-result order). Like `SortOperator`, this intentionally violates the `operator-traits.md §4.2` entity-ordering invariant, which applies only below the relational layer. No operator below Distinct in the plan may require entity-sorted input.

---

## 5. Plan Tree Placement

Both operators are **post-aggregation relational operators** in Wave 3. Valid plan positions:

```
Limit
  └── Sort                    ← ORDER BY ... LIMIT N
        └── Distinct          ← SELECT DISTINCT ... ORDER BY
              └── Aggregate   ← STATS
                    └── SequenceMatch / Scan / Filter
```

**Invariant**: Sort and Distinct only appear above the entity-streaming layer. The scan layer (`ScanPhysical`) and any `EntityOperator` wrapped in an adapter always sit below. This is enforced by the planner's wave3-lowering.md rules.

**Position relative to each other**:
- `ORDER BY ... SELECT DISTINCT` (if both appear) lowers to `Sort(Distinct(Project(...)))`.
- `SELECT DISTINCT ... ORDER BY` (same AST but the planner normalizes this) also lowers to `Sort(Distinct(Project(...)))`.

In both cases Sort is the outermost operator: sorting after deduplication avoids sorting duplicates that will be discarded.

---

## 6. Error Contract

Both operators raise `BqliteError::Execution` on cap overflow (operator-traits.md §4.3). The error message includes the limit and the row count at the time of overflow.

| Operator | Error condition | Message template |
|---|---|---|
| `SortOperator` | input rows > `max_rows` | `"SortOperator: input row count {n} exceeds max_rows limit {limit}"` |
| `DistinctOperator` | distinct keys > `max_groups` | `"DistinctOperator: distinct row count exceeds max_groups limit {limit}"` |

Both errors are fatal — the query is aborted. There is no partial output and no spill fallback in Wave 3.

**Default cap values**:

| Operator | Cap name | Default | Rationale |
|---|---|---|---|
| `SortOperator` | `max_rows` | 10,000,000 | 10M rows at ~100 bytes each = ~1 GB; headroom in a 3 GiB query budget |
| `DistinctOperator` | `max_groups` | 1,000,000 | Matches the aggregate operator default; same GroupKey overhead |

Caps are specified in the physical descriptor, not hardcoded in the operator. The engine bind step reads them from the descriptor, allowing query-level overrides (future `WITH (max_rows = N)` hint or engine config).

---

## 7. Close + Teardown

Both operators implement `close()` per the operator-traits.md §4.4 lifecycle. `SortOperator::close()` drops the accumulated buffer and output batch list. `DistinctOperator::close()` drops the `HashSet`. Both are idempotent — double-close is safe.

On error (either cap overflow or upstream error), the engine calls `close()` which frees all accumulated memory. No temp files are involved (Wave 3 ships no spill).

---

## 8. Forward References

| Wave | Task | Change |
|---|---|---|
| Wave 5 | TASK-513 (protocol: TASK-502 / `engine/spill.md`) | Sort spill: sorted runs to temp files (Arrow IPC stream per `engine/spill.md` § 6.1), merge-sort pass at end-of-input. No `SortPhysical` opt-in field — every Sort participates by construction once TASK-513 lands; spill root configured engine-wide via `EngineConfig::spill_root`. |
| Wave 5 | — | `NULLS FIRST` / `NULLS LAST` modifiers in `ORDER BY` syntax. `SortDirection` gains a `null_position` field. |
| Wave 5+ | — | Unstable sort variant for performance when key uniqueness is known. Controlled by a flag on `SortPhysical`. |
| Wave 5 | TASK-510 (per `engine/memory-budget.md`) | `MemoryBudget` integration: both operators reserve through the query's `MemoryBudget` (TASK-111 trait, `MemoryTracker` impl) instead of relying solely on the hard cap. Sort registers a spill handler (TASK-513); Distinct fails fast on overflow. |
