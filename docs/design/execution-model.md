# Execution Model Design

> **Status**: DRAFT
> **Task**: TASK-003
> **Depends on**: TASK-005 (type system), TASK-001 (storage format)
> **Depended on by**: TASK-004 (sequence matching)

---

## 1. Design Goals

The execution model serves four constraints from [core-beliefs.md](../core-beliefs.md):

**Performance (Belief 1).** The engine must deliver >1 GB/s scan throughput per core. Vectorized stateless operators, SIMD-friendly batch processing, and operator fusion eliminate overhead between pipeline stages. Late materialization and demand propagation avoid decoding data that no operator needs.

**Entity-first data model (Belief 3).** Every query implicitly operates per-entity. The execution model partitions work by entity so that temporal operators (sequence matching, sessionization) see one entity's events at a time in timestamp order — no hash lookups, no out-of-order assembly.

**Memory-conscious (Belief 6).** Queries execute within a bounded memory budget (default 3 GB for query execution, see storage-format.md Section 13). Sub-batch streaming ensures that even entities with millions of events never blow the budget. Operators that exceed their allocation spill to disk.

**Strongly-typed pipelines (Belief 8).** Every operator declares its output schema at plan time. The planner validates schema compatibility across the entire pipeline before execution begins. There are no runtime type errors.

---

## 2. Operator Categories

The engine has two fundamentally different operator categories, each with its own execution protocol:

### 2.1 Stateful Temporal Operators

Operators that need to see an entity's events in timestamp order: sequence matching, sessionization, event sub-selection (FIRST/LAST/NTH), attribution. These are inherently **entity-at-a-time** — they maintain per-entity state, process events sequentially, and emit a result (or nothing) when the entity stream ends. Executed via a **pull-based** protocol where the adapter pulls entity sub-batches from upstream.

Each stateful operator has its own optimal execution strategy:

| Operator | Execution Strategy | State Per Entity |
|---|---|---|
| Sequence match (MATCH) | NFA or specialized fast path (see Section 8) | Active state set + timestamps + held properties |
| Sessionization (SESSIONIZE) | Streaming fold | Current session ID + last event timestamp |
| Event sub-selection (FIRST/LAST/NTH) | Single-event extraction with predicate filter | At most one retained event |
| Attribution (ATTRIBUTE) | Sliding window deque of qualifying touchpoints; auto-unnested emission on conversion | Deque entries are `(ts, pre-computed touchpoint_key)` pairs — minimal per-touchpoint state |

All stateful operators share the `EntityOperator` interface (Section 4) despite having different internal strategies.

**ATTRIBUTE auto-unnests.** Rather than emitting a list of touchpoints per conversion, ATTRIBUTE emits **one row per `(entity, conversion, matched-touchpoint)` triple** with a single pre-computed `touchpoint_key: String` column (query-language.md §14.3). This keeps the output schema fully within BQL's scalar type system (no `List(Struct)` / `List(Map)` workaround) and removes the need for a separate UNNEST operator.

### 2.2 Stateless Columnar Operators

Operators that process batches without regard for entity boundaries: filter, project, scalar expressions, aggregation, PIVOT. These are **vectorized** — they operate on Arrow columnar batches using SIMD-friendly kernels. Executed via a **push-based** protocol within fused pipeline segments (Section 3).

**Window functions** (LAG, LEAD, ROW_NUMBER, running aggregates) are stateful per-entity operators — they need to see an entity's events in order to compute window values. They implement `EntityOperator` (Section 7.3).

### 2.3 Composition

Both categories compose in a single pipeline. A typical query:

```
scan → filter(event_type) → sequence_match(pattern) → aggregate(counts)
  ↑ push (vectorized)       ↑ pull (entity-at-a-time)  ↑ push (fused)
```

The filter is push-based and vectorized, the sequence match is pull-based and entity-at-a-time, and the aggregation is either push-based or fused into the sequence match operator (Section 8).

---

## 3. Hybrid Push/Pull Pipeline

### 3.1 Design Rationale

The engine uses a **hybrid execution model**: push-based for stateless operators and pull-based for stateful entity operators.

**Push-based stateless operators** process batches as they arrive from upstream. Adjacent stateless operators (scan → filter → project) are fused into a single push pipeline segment with no virtual function call overhead between them. This matches the approach used by DuckDB's pipeline model and Hyper's morsel-driven execution for maximum vectorized throughput.

**Pull-based stateful operators** pull entity sub-batches on demand. The `EntityOperatorAdapter` drives the pull loop, requesting sub-batches from its push-based input. This is natural for entity-at-a-time processing where the operator controls the consumption rate and maintains state across sub-batches.

**Pipeline breakers** — operators that must see all input before producing output (aggregation, ORDER BY, PIVOT) — define segment boundaries. Each push segment runs until it hits a breaker, which accumulates results and feeds the next segment.

### 3.2 PhysicalOperator Trait

All operators implement a common interface:

```rust
pub trait PhysicalOperator: Send {
    /// The output schema of this operator, determined at plan time.
    fn output_schema(&self) -> &OperatorSchema;

    /// Pull the next batch of results. Returns None when exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, OperatorError>;
}
```

`output_schema()` is the same name used by `EntityOperator` (Section 4), so an `EntityOperatorAdapter` can forward the call unchanged when presenting its inner operator's schema to downstream consumers.

The pull interface is the external API for consumers (the engine, Python bindings, CLI). Internally, fused push segments are wrapped in a `PhysicalOperator` that drives the push loop inside `next_batch()`.

### 3.3 Cancellation

Cancellation uses a shared `Arc<AtomicBool>` flag, not a method on the operator trait. This avoids data races between the cancelling thread and the executing thread:

```rust
pub struct QueryContext {
    /// Set to true to cancel the query. Checked between batches.
    cancelled: Arc<AtomicBool>,
    /// Query timeout. The engine sets `cancelled` when this elapses.
    timeout: Option<Duration>,
    /// Memory tracker for this query (shared across shard-tasks).
    memory: Arc<MemoryTracker>,
}
```

`QueryContext` is shared across shard-tasks via `Arc`. It contains only thread-safe fields. Metrics and warnings are collected per-shard-task in thread-local structs and merged after completion:

```rust
/// Per-shard-task state. Not shared across threads.
pub struct ShardTaskContext {
    query: Arc<QueryContext>,
    metrics: QueryMetrics,
    warnings: Vec<QueryWarning>,
}
```

Operators receive a reference to `QueryContext` at construction time and check `cancelled` at natural yield points (between batches, between entity sub-batches). Worst-case cancellation latency is one batch processing time.

### 3.4 Query Timeout

The engine spawns a lightweight timer when a query starts. If the query has a timeout (configurable per-query or via a global default), the timer sets the `cancelled` flag after the timeout elapses. The next yield point in any shard-task observes the flag and returns `Err(OperatorError::Cancelled)`. The engine maps that to `ExecutionError::Timeout` when the timeout fired, or `ExecutionError::Cancelled` for caller-initiated cancellation. This provides fast stopping without polling overhead — the flag check is a single atomic load.

### 3.5 Entity-Aligned Batches

The fundamental batch discipline: **never split an entity across batches**. Given `(entity_id, timestamp)` sort order from the storage layer, each batch extends to the next entity boundary. This means:

- Stateless operators (filter, project) process these batches in vectorized fashion, preserving entity alignment — they never need to track entity boundaries.
- Stateful operators can process a complete entity (or sub-batch sequence) without coordinating with other operators about entity transitions.

### 3.6 Batch Size

Batches target **65,536 rows** (64K), the same size as storage row-groups. Note that row-groups and batches are distinct concepts — a row-group is a storage-level unit within a segment file, while a batch is an execution-level unit flowing through the pipeline. They happen to share the same size target for alignment: within a single segment, one row-group produces one batch with no splitting or buffering. Across a k-way merge of multiple segments, batches are assembled from multiple row-groups. The scan accumulates rows from the merge until either:

1. The target row count is reached **and** the current entity has ended, or
2. The end of the shard's data is reached.

This alignment between row-group and batch size eliminates an unnecessary boundary and simplifies the scan layer. Small entities (10-100 events) pack many per batch; a single large entity (100K+ events) is handled via sub-batch streaming (Section 5) with the same 64K batch size.

### 3.7 RecordBatch Schema Conventions

All `RecordBatch` values flowing through the pipeline obey a small set of conventions that operators can rely on without re-deriving them per-query.

**Column ordering.** Batches produced by the scan layer lay out columns in a fixed order:

1. `entity_id` (first)
2. `ts` — timestamp (second)
3. `event_type` (third)
4. `__seq_id` — sequence identifier (fourth)
5. Remaining property columns sorted by **encoded size ascending** — the narrowest columns first, for cache efficiency in vectorized scans.

The property column ordering is decided at ingest/compaction time by the storage layer (storage-format.md §3.4 encoding selection has access to per-chunk sizes), not at plan time. Smaller columns first means more columns fit in cache lines during vectorized filter and project passes.

**Reference columns by name, not position.** Projection pruning removes and reorders columns between the scan and the first operator that references them, so any operator that hard-codes column indices is fragile. Every operator looks up columns by name through its `OperatorSchema`. The one exception is the `EntityOperatorAdapter`, which caches `entity_id_col_idx` once at construction.

**Dictionary encoding preservation.** The scan layer produces columns in whatever encoding the storage provides. For string columns, this is most commonly `DictionaryArray<Int32, Utf8View>` from a single-segment read. Downstream operators must handle dictionary columns without forcing materialization:

- **For filtering.** At scan setup (before iterating rows), the scan precomputes a **dictionary filter bitset** per `(segment, filtered string column)` pair:

  ```rust
  /// Computed once per (segment, filtered column) pair at scan setup.
  struct DictFilterBitset {
      /// bitset[i] = true iff dictionary entry i satisfies the predicate.
      /// For `event_type IN ('signup', 'purchase')`, the bitset is true at the
      /// dictionary codes for those two strings.
      matching_codes: BitVec,
  }
  ```

  During row iteration, filtering is an integer bitset lookup — no string comparison in the hot loop. Each of the `k` segment files in a k-way merge has its own dictionary, so each has its own precomputed bitset.

- **For materialization.** When downstream needs the actual string value (output, variable binding, step property forwarding), decode via a single dictionary lookup per row. This only runs on rows that pass filtering, which is typically a small fraction of the input.

**Timestamp format.** The `ts` column is always `Int64` nanoseconds (Arrow `Timestamp(Nanosecond, UTC)`). No timezone conversion at query time — all conversion happens at ingest (type-system.md §7.2 width consolidation).

**Null bitmaps.** Nullable columns always carry an Arrow-compatible null bitmap. Non-nullable columns (`entity_id`, `ts`, `event_type`, `__seq_id`) never do — operators skip the null check entirely on these columns, and the storage layer does not allocate bitmaps for them. The schema declares which columns are nullable; the decoder trusts the schema and does not insert defensive checks.

---

## 4. Entity Operator Interface

Stateful temporal operators implement a separate trait that the engine wraps inside a `PhysicalOperator` adapter:

```rust
/// Stateful per-entity operator.
/// The operator itself is immutable (&self) — all mutable state lives in State.
/// This makes the compiled operator safely shareable across shard-tasks.
pub trait EntityOperator: Send + Sync {
    /// Per-entity mutable state. Created fresh for each entity.
    type State: Send;

    /// Create initial state for a new entity. The `entity_id` is passed so
    /// operators that need per-entity warning attribution can capture it;
    /// most operators ignore the argument.
    fn create_state(&self, entity_id: &ScalarValue) -> Self::State;

    /// Output schema for this operator's results.
    fn output_schema(&self) -> &OperatorSchema;

    /// Process a sub-batch of events for the current entity.
    /// The adapter guarantees that the rows in `batch` are:
    ///   - all for the same entity_id,
    ///   - sorted by timestamp ascending,
    ///   - no more than SUB_BATCH_SIZE rows (configurable, default 65,536).
    fn process_sub_batch(
        &self,
        state: &mut Self::State,
        batch: &RecordBatch,
    );

    /// Extract results after all sub-batches for this entity.
    /// Called exactly once per entity, after the last `process_sub_batch()`.
    /// Returns `None` if this entity produces no output rows (e.g. the entity
    /// did not match the pattern). Consumes `state` — there is no reuse.
    fn finish_entity(&self, state: Self::State) -> Option<RecordBatch>;

    /// Fused aggregation path. If the operator has a fused accumulator, the
    /// adapter calls this INSTEAD of `finish_entity()`. Updates the accumulator
    /// directly without materializing per-entity rows.
    /// The default implementation calls `finish_entity()` and feeds the result
    /// into the accumulator — operators override for zero-materialization fusion.
    fn finish_entity_into(
        &self,
        state: Self::State,
        accumulator: &mut dyn Accumulator,
    ) {
        if let Some(batch) = self.finish_entity(state) {
            accumulator.update_batch(&batch);
        }
    }

    /// The set of input columns this operator actually reads.
    /// Drives projection pruning at the scan layer (Section 8).
    fn required_columns(&self) -> &[String];

    /// Advertise what demand-based strategies this operator supports.
    /// The planner uses this to select the cheapest strategy that satisfies
    /// downstream demand.
    fn supported_demands(&self) -> DemandCapabilities;
}
```

**Key design choices:**

- **`&self` is immutable.** The compiled operator (NFA program, predicates, schema, configuration) is shared across all shard-tasks via `Arc`. All mutable state lives in `Self::State`, created fresh per entity by `create_state`. This is what makes `Send + Sync` sound even though each shard-task runs independently — no shard-task can mutate the operator itself.
- **`create_state(entity_id)` instead of `State: Default`** — allows operator configuration to influence the initial state, and gives the operator access to the entity identifier for warning attribution (`QueryWarning::EntityEventLimitExceeded`, `ActiveStateLimitExceeded`, etc.).
- **`finish_entity()` is the sole completion signal** — no `is_last` flag on `process_sub_batch()`. The adapter calls `process_sub_batch()` for every sub-batch, then `finish_entity()` exactly once. Clean single-responsibility: `process_sub_batch` accumulates, `finish_entity` emits and consumes the state.
- **`finish_entity()` returns `Option<RecordBatch>`** not `Option<Row>` — a single-row `RecordBatch` for operators that emit one result per entity, or a multi-row `RecordBatch` for operators like SESSIONIZE (one row per session) or windowed operators (one row per input event). Avoids an undefined `Row` type and keeps everything in Arrow's type system.
- **`finish_entity_into()` has a default implementation.** Non-fused operators leave it alone; fused operators override it to skip the per-entity `RecordBatch` materialization entirely.
- **No `Result<...>` returns on the hot-path methods.** Errors surface through different channels to keep the inner loop branch-free:
  - **Memory pressure** is caught at the allocation site (`MemoryTracker::try_reserve` inside operator internals). Operators that cannot spill set an error flag on the shared `QueryContext` and return early from `process_sub_batch`; the adapter observes the flag between sub-batches and aborts the query.
  - **Cancellation** is checked against `QueryContext::cancelled` between sub-batches, not inside the per-event loop.
  - **Invariant violations** panic — the engine catches panics at the shard-task boundary and surfaces them as `ExecutionError::OperatorPanic`.

### 4.1 EntityOperatorAdapter

The engine wraps an `EntityOperator` in a `PhysicalOperator` adapter that handles entity boundary detection and sub-batch routing:

```rust
pub struct EntityOperatorAdapter<O: EntityOperator> {
    operator: O,
    input: Box<dyn PhysicalOperator>,

    // Per-entity tracking
    current_entity: Option<ScalarValue>,
    current_state: Option<O::State>,

    /// Leftover rows after an entity boundary split. Processed on the next
    /// `next_batch()` call before pulling more input.
    pending_batch: Option<RecordBatch>,

    /// Cached index of the entity_id column in the input's schema.
    entity_id_col_idx: usize,

    // Output accumulation: collect per-entity outputs until `target_output_rows`
    // is reached, to avoid emitting one-row batches per entity.
    output_buffer: Vec<RecordBatch>,
    target_output_rows: usize,                  // default ~8,192

    ctx: Arc<QueryContext>,
}
```

The adapter's `next_batch()` implementation runs this boundary-detection loop:

1. **Drain any pending sub-batch** first. If `pending_batch` is `Some`, process it as if it had just been pulled (it holds the rows that belong to a new entity after a split).
2. **Pull the next batch from input.** If the input is exhausted and no in-progress entity remains, drain `output_buffer` and return.
3. **Scan the entity_id column** for a value change. Because the data is sorted by `(entity_id, timestamp)`, a value change is always a boundary.
4. **No boundary found.** The entire batch belongs to `current_entity`. Call `operator.process_sub_batch(state, &batch)` and continue to the next iteration.
5. **Boundary found at row N.**
   a. Slice `batch[0..N]` — this is the last sub-batch for the current entity. Call `process_sub_batch(state, &slice)`.
   b. Call `finish_entity(state)` (or `finish_entity_into(state, accumulator)` when fused). Append any returned `RecordBatch` to `output_buffer`.
   c. Slice `batch[N..]` and store it as `pending_batch` for the next iteration.
   d. Call `create_state()` for the new entity and update `current_entity`.
   e. **Re-enter the loop at step 1.** Step 1 drains the slice we just stashed in 5c before any further pulls — this is the "leftover slice belongs to the new entity" path. Do not fall through to step 2.
6. **Input exhausted** (step 2 observed `None` and there is no in-progress entity). At this point `pending_batch` is guaranteed to be empty, because step 1 always drains it before step 2 can pull again. Call `finish_entity()` on the final in-progress entity if any, drain `output_buffer`, and return `None` on subsequent calls.

Between iterations, the adapter checks `QueryContext::cancelled` so cooperative cancellation observes a per-entity upper bound on latency. Output is only returned from `next_batch()` once `output_buffer`'s total row count reaches `target_output_rows`, which prevents the common "one-row `RecordBatch` per entity" pathology in non-fused pipelines.

The `pending_batch` slot is what makes this loop work across `next_batch()` boundaries. If a boundary lands mid-batch and the freshly-started entity's sub-batch is non-trivial, the adapter does **not** recurse into another pull — it stashes the leftover slice and returns to the caller with whatever output is ready. The next call picks up the leftover slice first, preserving the "process the current entity before pulling more" invariant.

**`finish_entity()` overhead.** Creating a single-row `RecordBatch` per entity involves schema validation and buffer allocation. For 10M entities in a non-fused pipeline, this is 10M small allocations. In practice this is acceptable — Arrow's `RecordBatch::try_new()` is lightweight for single-row batches (~100ns), and the fused path (Section 8.4) eliminates it entirely for the common aggregate case. The output-buffer accumulation above amortizes the cost by concatenating many single-row batches into one output batch before returning.

### 4.2 Layered Extraction for Stateful Operators

A single stateful operator often needs to support many different downstream shapes — MATCH alone serves bare `COUNT(*)`, step-counter funnel counts, match-detail extraction, step-property forwarding, and fused aggregations. Rather than implementing a separate code path per shape, stateful operators use **layered extraction**: a fixed inner loop with independently toggled optional hooks that run at match/session/event completion.

```rust
pub struct MatchExecutionConfig {
    // Core (always runs): NFA / step counter transitions + step_reached tracking.
    pub track_match_duration: bool,
    pub track_match_events: bool,
    pub step_properties: Vec<StepPropertyExtraction>,
    pub fused_accumulator: Option<Box<dyn Accumulator>>,
}

pub struct StepPropertyExtraction {
    pub step_index: u8,
    pub column_name: String,
    pub bql_type: BqlType,
}
```

**The inner per-event loop has zero demand-related branches.** All feature toggles are evaluated only at match/session/event completion, which is infrequent compared to the per-event transition hot path. This pattern is load-bearing for performance: the step-counter fast path (sequence-matching.md §10.3) cannot afford per-event `if` checks for "should we materialize `match_events`" or "should we extract `s.plan`". Layered extraction moves all such decisions to completion time.

```rust
fn on_match_complete(&mut self) {
    if self.config.track_match_duration {
        // compute last_step_ts - anchor_ts
    }
    if self.config.track_match_events {
        // build Map(String, Timestamp)
    }
    for extraction in &self.config.step_properties {
        // extract value from retained event reference
    }
    if let Some(acc) = &mut self.config.fused_accumulator {
        // Reduced values are laid out in FusableAggregate::functions order;
        // see sequence-matching.md §13.4 for the concrete MATCH version.
        acc.update(group_key.as_deref(), &reduced_values);
    } else {
        self.output_batch.push(/* ... */);
    }
}
```

SESSIONIZE, event sub-selection (FIRST/LAST/NTH), and ATTRIBUTE use the same pattern. The set of optional hooks differs per operator, but the principle is identical: branch only at completion, never in the per-event hot loop.

The `MatchExecutionConfig` (and its counterparts for other stateful operators) is populated by the physical planner during demand propagation — see planner-pipeline.md §7.5 and §9.4. The planner reads the downstream `DemandSet`, resolves each `StepPropertyRef` into a `StepPropertyExtraction` by looking up the step index in the compiled pattern, and enables the layered-extraction hooks that correspond to the downstream's demanded columns.

---

## 5. Sub-Batch Streaming for Large Entities

### 5.1 Problem

Most entities are small (hundreds to low thousands of events), but power-law distributions mean some entities have millions of events. The scan layer must not materialize all events for a large entity into a single batch — this would blow the memory budget.

### 5.2 Mechanism

When the scan layer encounters an entity whose events span multiple row-groups, it naturally produces multiple batches for that entity (one per row-group, each 64K rows). The `EntityOperatorAdapter` calls `process_sub_batch(state, batch)` for each batch, maintaining the same state across all of them.

The contract:

- Sub-batches for one entity arrive consecutively (guaranteed by sort order).
- The `EntityOperator` maintains compact state across sub-batches (NFA state, session counter, etc.).
- The scan drops each batch's data before producing the next — only one batch is in memory at a time.
- `finish_entity()` is called only after all sub-batches for the entity have been processed.

### 5.3 Entity Event Limit

To prevent pathological entities from consuming unbounded resources, the planner injects an **entity event limiter** as a post-filter operator early in the pipeline — after predicate pushdown and stateless filters, but before stateful entity operators. This is a lightweight stateless operator that counts rows per entity (by tracking entity_id changes) and drops rows beyond the configured limit (default: 10 million events per entity).

By injecting the limiter early, the expensive work (stateful entity processing, aggregation) is avoided for pathological entities. The scan and filter stages still process the excess rows, but these are vectorized and cheap compared to NFA evaluation.

When the limit is triggered:

1. Rows beyond the limit for that entity are dropped from the batch.
2. A `QueryWarning::EntityEventLimitExceeded { entity_id, count }` is recorded in the shard-task's `ShardTaskContext`.
3. Processing continues with the next entity.

This is not a fatal error — the query completes with the skipped entity flagged in the result metadata.

---

## 6. Type Dispatch in Vectorized Kernels

### 6.1 Strategy: Arrow Compute Kernels with Monomorphized Hot Paths

Stateless operators delegate to Arrow's compute kernels (via the `arrow` crate) for type-generic operations — comparisons, arithmetic, casts, string operations. These kernels handle type dispatch internally via Rust's enum matching and are well-optimized.

For hot-path operations where Arrow kernel overhead is measurable (tight filter loops, scalar expression evaluation), the planner generates a **monomorphized kernel** at plan time. The type is known at plan time (Belief 8 — strongly-typed pipelines), so the planner selects a type-specific function pointer:

```rust
/// Plan-time resolved function pointer for a scalar expression.
pub enum TypedKernel {
    IntIntToInt(fn(&Int64Array, &Int64Array) -> Int64Array),
    FloatFloatToFloat(fn(&Float64Array, &Float64Array) -> Float64Array),
    IntFloatToFloat(fn(&Int64Array, &Float64Array) -> Float64Array),
    StringToBool(fn(&StringViewArray, &StringViewArray) -> BooleanArray),
    // ... one variant per (input_types → output_type) combination
}
```

This avoids per-row type dispatch at execution time. The cost is a match on the kernel variant once per batch (not per row). The type system's small type set (7 variants in `BqlType`) keeps the number of kernel variants manageable.

### 6.2 Dictionary and RLE Dispatch

Operators that operate on compressed representations (dictionary-encoded, RLE) use Arrow's native `DictionaryArray` and `RunEndEncodedArray` types. Filter and GROUP BY kernels dispatch on the array type:

- `DictionaryArray<Int32, Utf8View>` → filter/group on codes, resolve dictionary for output.
- `RunEndEncodedArray` → iterate run ends for aggregation, skip repeated values.

This is not a separate dispatch mechanism — it is handled by Arrow's array type hierarchy. The planner notes which columns will arrive in compressed form (from the storage layer's late materialization) and selects appropriate kernels.

---

## 7. Pipeline Stages

A query pipeline consists of five stages:

### Stage 1: Scan

Reads segments from relevant `(window, shard)` pairs. Applies predicate pushdown at the segment reader. Performs k-way merge across windows within a shard (see storage-format.md Section 8). Produces entity-aligned batches with late materialization — encoded columns (dictionary, RLE, constant) stay in their compressed Arrow representation.

### Stage 2: Stateless Transforms

Filter, project, scalar expressions. Push-based vectorized processing on entity-aligned batches using Arrow compute kernels and monomorphized hot paths (Section 6). Preserves entity alignment. Operates on compressed representations where possible (dictionary-encoded filters resolve to code comparisons). Adjacent stateless operators are fused into a single push segment.

### Stage 3: Stateful Entity Processing

Sequence match, sessionize, event sub-selection, attribution. Pull-based processing via the `EntityOperator` interface. Selects execution strategy based on the demand set (Section 8). Optionally fuses with downstream aggregation.

### Stage 4: Aggregation

If not fused with Stage 3, standard hash aggregation on the output of the entity stage via `HashAccumulator` (Section 9.4). Partial aggregation per shard, final merge across shards via `Accumulator::merge`. Group cardinality is bounded by `max_groups` (default 1M); overflow produces `OperatorError::MaxGroupsExceeded`. There is no aggregation spill in v1 — see Section 10.3.

### Stage 5: Output

Final projection, ordering (if requested), limit, result collection as Arrow `RecordBatch`es.

### 7.1 LIMIT Pushdown

When the query includes a `LIMIT N`, the pipeline short-circuits after N result rows are produced. The `cancelled` flag in `QueryContext` is set, stopping all shard-tasks at their next yield point. For queries with `ORDER BY` + `LIMIT`, all shards must complete before the final merge-sort can apply the limit — LIMIT pushdown applies only when no cross-shard ordering is required.

### 7.2 ORDER BY Across Shards

When the query includes `ORDER BY`, each shard produces locally-sorted results. A final k-way merge-sort (binary heap, k = num_shards) across shard results produces the globally-sorted output. For `ORDER BY` on non-entity columns (e.g., aggregate values), each shard's partial results are sorted locally, then merge-sorted.

### 7.3 Additional Operator Execution

**Window functions (OVER).** Window functions (LAG, LEAD, ROW_NUMBER, running aggregates) are stateful per-entity operators that emit one row per input row rather than one row per entity. They implement `EntityOperator` with `finish_entity()` returning a multi-row `RecordBatch` containing the entity's full output. The adapter handles this naturally — it collects multi-row results into the output buffer like any other `RecordBatch`. Within-entity ordering is guaranteed by the scan's timestamp sort order.

**SAMPLE.** Entity sampling is implemented at the scan level using a hash-based filter: `wyhash(entity_id) % 10000 < fraction * 10000` for fractional sampling, or a deterministic hash-based selection for fixed-count sampling. The scan skips entities that don't pass the filter before producing any batches for them. SAMPLE uses a **different hash function** (wyhash) than sharding (xxhash64) to ensure uniform sampling across shards — using the same hash would sample entire shards rather than a uniform fraction of entities from each shard. Results are deterministic and reproducible for the same entity set.

**IN (subquery).** The inner query executes first, materializing a hash set of entity IDs (or compound keys). The outer query's scan or filter stage probes this hash set to filter rows. For the common case of entity-level cohort filtering (`WHERE (user_id) IN (subquery)`), this is an entity-level semi-join — the hash set is built once and probed per entity in the outer scan. Memory for the hash set is bounded by the inner query's result cardinality; for very large inner results, the hash set spills to a temporary on-disk hash table.

**PIVOT.** PIVOT is a pipeline breaker that accumulates group-by results, then reshapes them into wide-form output. The set of pivot values must be known at plan time (provided as a literal list, or inferred from the query structure for operators like RETENTION). Execution: hash aggregation keyed on `(group_by_keys, pivot_column)`, then a final reshaping pass that pivots rows into columns.

---

## 8. Demand Propagation and Operator Fusion

### 8.1 Core Principle

An operator fusion is valid whenever the downstream consumer needs strictly less information than the upstream producer materializes. This is a generalization of projection pushdown into stateful operators.

### 8.2 Demand Propagation

Each operator declares at plan time what capabilities it needs from its input. The planner propagates these requirements **upstream** through the pipeline:

1. **Output schema:** what the operator can produce (all fields, full detail).
2. **Required input columns:** what the operator reads from its input.
3. **`DemandSet`:** what downstream operators need from this operator's output — propagated backward by the planner.

**`DemandSet` is the formal type** carried by the backward pass. It is defined in `bqlite-planner` and specified by planner-pipeline.md §9.3. The key fields:

- `columns: HashSet<ColumnId>` — columns the downstream needs.
- `needs_match_detail: bool` — whether `match_events` / `match_duration` are needed.
- `needs_step_reached: bool` — whether the `step_reached` column is needed.
- `step_properties: Vec<StepPropertyRef>` — per-(step, column) demand bits for named step property forwarding.
- `forwarded: Vec<ColumnId>` — forwarded columns from SESSIONIZE / ATTRIBUTE (demand-driven column carrying).
- `fused_aggregate` / `fused_filter` — set by the optimizer's fusion pass.

The **per-(step, column) `step_properties`** field is finer-grained than a plain column set. A downstream reference to `s.plan` adds `(step_name: "s", column_name: "plan")` — not a column named `s.plan` or a materialized `match_events` map. The stateful operator uses this information to retain exactly the referenced properties from the matched events at the moment the corresponding step is consumed, and to discard everything else. Planner-pipeline.md §8.2 specifies the semantics; type-system.md §6.1 documents how these demands add first-class columns to the MATCH output schema.

The physical planner walks the pipeline backward, propagating demand. Stateful operators inspect the demand set and select the cheapest execution strategy that satisfies it:

```
SequenceMatch receives demand = {columns: {entity_id, step_reached}}
  → Uses step-counter strategy (funnel fast path)

SequenceMatch receives demand = {columns: {entity_id}, no match detail}
  → Uses boolean-match strategy (cheapest: just "did any full match occur?")

SequenceMatch receives demand = {
  columns: {entity_id, step_reached},
  step_properties: [(s, country, String)],
}
  → Uses step-counter + step-property forwarding strategy (retain 's.country' at step 1)

SequenceMatch receives demand = {needs_match_detail: true}
  → Uses full NFA with match materialization (most expensive, full detail)
```

### 8.3 This Subsumes Funnel and Retention Optimization

No need for specific optimizer rules that pattern-match "this looks like a funnel." The generic demand mechanism causes the sequence match operator to automatically select the step-counter strategy when downstream only needs per-step counts. Retention similarly: when downstream only needs per-bracket boolean presence, the operator adapts to bitmap mode automatically.

### 8.4 Aggregation Fusion

The biggest performance win: when a stateful operator feeds directly into an aggregation with no intervening operator that needs per-row data, the stateful operator can accumulate the aggregate internally via `finish_entity_into()`.

**Without fusion:**
```
SequenceMatch emits one row per entity {entity_id, step_reached}
  → Aggregate accumulates counts
  → N intermediate rows for N entities
```

**With fusion:**
```
SequenceMatch maintains step_counts: [u64; num_steps] internally
  → For each entity, increments step_counts[step_reached]
  → At end, emits single result row with counts
  → Zero intermediate allocation, zero per-entity output
```

Fusion is valid when:

- The aggregation function is **incrementally computable** — `COUNT`, `SUM`, `MIN`, `MAX`, `AVG` (via sum + count), and percentile estimates (via **DDSketch**: bounded relative error, ~1–2 KB sketch per group, constant-time merge). `AVG` is not associative but is incrementally computable via `(sum, count)` state.
- No operator between the stateful operator and the aggregation needs per-entity rows (no `HAVING` that filters on match results, no `ORDER BY` on per-entity data).
- `GROUP BY` keys, if any, are available inside the stateful operator (held properties, cohort time).

### 8.5 GROUP BY With Fusion

For `STATS COUNT(*) GROUP BY held.plan` after a sequence match, the fused operator maintains a `HashMap<GroupKey, Vec<AggState>>` (the same layout as `HashAccumulator`, Section 9.4). For each entity it resolves the group key (held property value) and updates the right `AggState` slot. Still no per-entity materialization.

Group cardinality is bounded by `max_groups` (default: 1,000,000). If exceeded, the fused accumulator raises `OperatorError::MaxGroupsExceeded` — v1 does not spill aggregation state (Section 10.3).

### 8.6 Fusion Opportunities

| Logical Pattern | Fused Strategy | Why It's Faster |
|---|---|---|
| Linear match + per-step count | Step counter with internal counts | Single-pass, no NFA, no intermediate rows |
| Cohort entry + bracket check + count | Bitmap accumulator with internal counts | Single-pass bit vector, no materialized brackets |
| Match + HAVING matched + count | Boolean match counter | Skips match detail materialization |
| Sessionize + aggregate per session | Session fold with internal accumulator | No materialized session_id column |

### 8.7 Fallback Guarantee

The general-purpose path (full materialization + separate aggregation) must always work. Fusion and demand-based strategy selection are pure performance optimizations. If the optimizer cannot apply them, the query is still correct, just slower.

---

## 9. Parallelism Model

### 9.1 Shard-Per-Thread

One query task per shard. Each task runs the full pipeline (scan → filter → entity operator → partial aggregate) independently. No shared mutable state between tasks during execution. Partial results are merged on a coordinator thread after all shard-tasks complete.

### 9.2 Why Shard Is the Parallelism Unit

Temporal operators need all of an entity's events across time in order. An entity's events across windows must be processed by the same thread. Since entities are hash-pinned to shards (storage-format.md Section 5.1), one thread per shard keeps entity streams intact. The k-way merge across windows happens within each shard-task.

### 9.3 Thread Pool and Query Queuing

- Fixed worker thread pool sized to `num_cores` (configurable). Implemented using Rayon's thread pool for work-stealing and efficient task scheduling.
- Default shard count: **32** (storage-format.md). With 32 shards and a pool of `num_cores` threads, shard-tasks are distributed across the pool — all cores stay busy even when `num_shards > num_cores`.
- **Query queuing:** queries submit shard-tasks to the worker pool in FIFO order. If the pool has available threads, the query's shard-tasks start immediately. If all threads are busy (e.g., another query is running), the new query's shard-tasks queue behind the in-progress work and execute as threads become available.
- **Concurrent queries are possible.** Multiple queries can have shard-tasks in flight simultaneously if the thread pool has capacity. The memory budget is divided across the fixed number of worker threads — each thread has a per-thread budget of `query_budget / num_worker_threads`. This is stable regardless of how many queries are active, because the number of worker threads is fixed.
- Shard-tasks from different queries may interleave on the pool, but shard-tasks within a single query are independent (no shared mutable state between them).

### 9.4 Partial Aggregation and Final Merge

Each shard-task produces partial aggregation results. The coordinator thread performs a final merge:

- `COUNT` / `SUM`: sum the partial values.
- `MIN` / `MAX`: min/max across partials.
- `AVG`: algebraic aggregate — each shard tracks `(sum, count)`; final merge computes `total_sum / total_count`.
- `P50` / `P90` / `P95` / `P99`: each shard collects a DDSketch; final merge combines sketches (constant-time under DDSketch's merge operator) and extracts quantiles with bounded relative error.
- `COUNT_DISTINCT`: each shard maintains an exact set; final merge unions those sets.

Non-aggregated results (selection queries) are concatenated across shards and optionally merge-sorted (k-way binary heap, k = num_shards) for `ORDER BY`.

The `Accumulator` trait supports both incremental updates and cross-shard merging:

```rust
/// Receives incremental updates from fused entity operators and from
/// non-fused aggregate nodes. One accumulator per shard-task; merged
/// across shards after execution.
pub trait Accumulator: Send {
    /// Update with the reduced values for one entity, match, session, or path.
    /// `group_key` is `None` for ungrouped aggregation. `values` contains
    /// one slot per aggregate function, in the order declared by the
    /// corresponding `FusableAggregate::functions` list.
    fn update(
        &mut self,
        group_key: Option<&[ScalarValue]>,
        values: &[ScalarValue],
    );

    /// Bulk update from a `RecordBatch` — the non-fused path used by the
    /// default `EntityOperator::finish_entity_into()` and by the plain
    /// `AggregatePhysical` node when no fusion is in effect.
    fn update_batch(&mut self, batch: &RecordBatch);

    /// Merge another accumulator into this one (for cross-shard reduction).
    /// Each shard produces one accumulator; they are merged pairwise on the
    /// coordinator after all shard-tasks finish.
    fn merge(&mut self, other: Box<dyn Accumulator>);

    /// Produce the final aggregated `RecordBatch`.
    fn finish(&mut self) -> RecordBatch;

    /// Current memory usage estimate — reported to the memory tracker and
    /// surfaced through query metrics for observability. Aggregation does not
    /// spill in v1 (Section 10.3), so this is informational rather than a
    /// spill trigger.
    fn memory_usage(&self) -> usize;
}
```

**Concrete implementation: `HashAccumulator`.** The default aggregate accumulator is a flat hash map from group key to per-function state:

```rust
pub struct HashAccumulator {
    /// Per-group state. Key is the group-by values tuple.
    groups: HashMap<GroupKey, Vec<AggState>>,
    /// Schema of the final aggregated output.
    output_schema: OperatorSchema,
    /// Hard cap on distinct groups before `update()` returns an error.
    /// Default: 1,000,000. Configurable per query.
    max_groups: usize,
}

/// Compact group key — `SmallVec` inline storage avoids heap allocation for
/// the common case of 1–3 group-by columns.
#[derive(Eq, PartialEq, Hash, Clone)]
pub struct GroupKey(SmallVec<[ScalarValue; 4]>);
```

**Per-function state: `AggState`.** Every aggregate in v1 is incrementally computable, which is what enables the fusion framework (Section 8.4 and planner-pipeline.md §7.2). The per-function state enum is:

```rust
pub enum AggState {
    Count(u64),
    Sum(SumState),                           // tracks i64 or f64 without cross-type promotion
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    Avg { sum: f64, count: u64 },            // algebraic; not associative but incremental
    CountDistinct(HashSet<ScalarValue>),
    Percentile(DDSketch),                    // ~1–2 KB per group, constant-time merge
    Variance { count: u64, mean: f64, m2: f64 },  // Welford's algorithm; parallel merge formula
}

pub enum SumState {
    Int(i64),
    Float(f64),
}
```

**Mergeability.** Every `AggState` variant supports a pairwise merge operation for cross-shard reduction — this is a hard requirement of the v1 aggregate list:

| `AggState`       | Merge operation                                        |
| ---------------- | ------------------------------------------------------ |
| `Count`          | Sum the counts                                         |
| `Sum`            | Sum the sums                                           |
| `Min`            | Take the smaller                                       |
| `Max`            | Take the larger                                        |
| `Avg`            | Sum the sums and the counts                            |
| `CountDistinct`  | Set union                                              |
| `Percentile`     | `DDSketch::merge()` — constant-time native operation   |
| `Variance`       | Parallel Welford merge formula                         |

**Group cardinality limit.** The limit is enforced inside `update()`: if `groups.len() >= max_groups` and the incoming `group_key` is new, the accumulator raises an error (surfaced through `QueryContext` as described in Section 4). There is no spill-to-disk for aggregation state in v1 — the combination of bounded groups + the memory budget provides a hard upper bound on accumulator memory. Spill is a v2 concern.

For fused entity operators that use `finish_entity_into()`, each shard-task produces its own accumulator(s). The coordinator merges them via `Accumulator::merge()` to produce the final result.

---

## 10. Memory Management

### 10.1 Memory Tracking

The engine uses a **hierarchical memory tracker** that enforces the query budget at runtime:

```rust
pub struct MemoryTracker {
    /// Total bytes currently allocated by this query.
    used: AtomicU64,
    /// Maximum bytes this query may allocate.
    budget: u64,
}

impl MemoryTracker {
    /// Try to reserve `bytes` of memory. Returns Err if budget would be exceeded.
    /// Callers should attempt to spill before propagating the error.
    pub fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation, OperatorError>;

    /// Release a reservation, returning memory to the budget.
    pub fn release(&self, reservation: MemoryReservation);
}
```

Every allocation that grows with data size — aggregation hash tables, hash sets for IN subqueries, sort buffers for ORDER BY, decoded column buffers — goes through `MemoryTracker::try_reserve()`. Small fixed-size allocations (operator structs, NFA state) are not tracked individually.

**Spill protocol.** When `try_reserve()` returns `Err`, the operator should attempt to spill some of its state to disk (Section 10.3) and retry. If the operator does not support spilling (or spilling fails to free enough memory), it propagates the `MemoryBudgetExceeded` error, which aborts the query. This means spill is the *preferred* response to memory pressure, but error is the *fallback* for operators that cannot spill.

Each shard-task shares the same `MemoryTracker` instance (via `QueryContext`). The atomic counter provides contention-free tracking across threads. The tracker is hierarchical: the query budget is a child of the engine-wide budget, which is itself bounded by the configured memory limit.

### 10.2 Per-Shard-Task Memory

Each shard-task uses:

| Component | Typical size | Bound |
|---|---|---|
| K-way merge read buffers | k × 4 MB (k = windows) | Configurable buffer size |
| Current batch | ~5 MB (64K rows × ~10 cols × 8 bytes) | One row-group |
| Operator state | 10-100 bytes per entity | Compact by design |
| Partial aggregation state | ~100 bytes per group | Hard cap at `max_groups` (default 1M); error on overflow |
| Decoded column data | Variable | Only demanded columns decoded |

At most `num_cores` shard-tasks run simultaneously (the thread pool bounds concurrency). On a 16-core machine: 16 × (24 MB merge buffers + 5 MB batch) ≈ 464 MB base. On a 32-core machine: 32 × 29 MB ≈ 930 MB. Both fit within the 3 GB query budget with headroom for aggregation state. The per-thread budget (`3 GB / num_cores`) provides a stable upper bound regardless of shard count or query concurrency.

### 10.3 Spill-to-Disk

When an operator's memory reservation exceeds the remaining budget, it spills intermediate state to disk. Two operators support spill in v1; a third has a hard cap instead.

**Sort spill (for ORDER BY).** When the sort buffer exceeds its allocation:
1. Sort the current buffer in memory.
2. Write the sorted run to a temporary file.
3. After all input is consumed, merge-sort the sorted runs from disk.

**IN subquery spill.** When the hash set for IN filtering exceeds memory:
1. Write the hash set to a temporary on-disk hash table (sorted file with binary search).
2. Probe the on-disk table for each entity during the outer query scan.

**Aggregation: no spill in v1.** `HashAccumulator` enforces a hard cap (`max_groups`, default 1M) at `update()` time and returns an error on overflow — see Section 9.4. At ~100 bytes per group, 1M groups fit comfortably in the query budget, and analytics queries rarely exceed that. External hash aggregation is deferred to v2; the cap is the v1 backstop.

Spill files (sort, IN subquery) are written to a configurable temp directory (default: the database directory). They are cleaned up when the query completes or is cancelled.

### 10.4 Aggregation State Bounds

Running aggregation state is small per group (counts, sums, min/max — see `AggState` in Section 9.4). The `max_groups` hard cap (default 1M) is enforced inside `HashAccumulator::update()` and is the only defense against runaway cardinality. When the cap is hit, the query fails with `OperatorError::MaxGroupsExceeded { limit }`, not a spill. 1M groups × ~100 bytes = ~100 MB — well within the 3 GB query budget, with no spill complexity to manage.

---

## 11. Compaction Scheduling

### 11.1 Compaction Thread Pool

Compaction runs on a separate pool of up to `num_cores` threads, independent from query worker threads. The number of **active** compaction threads is dynamically bounded:

```
active_compaction_threads ≤ num_cores - active_query_threads
```

This is a resource management decision, not a concurrency concern — compaction and queries can safely run concurrently due to manifest-based MVCC (storage-format.md Section 7.6). The bound ensures compaction uses only spare CPU and I/O capacity, yielding to queries when the machine is busy.

- **When query load is low:** compaction uses most cores, clearing backlog quickly.
- **When query load is high:** compaction scales down to zero active threads, resuming when query threads free up.
- **Mechanism:** compaction tasks check the query thread pool's active count before each work chunk and yield if no spare capacity remains.

Since each `(window, shard)` compacts independently (storage-format.md Section 7.1), compaction is embarrassingly parallel — multiple compaction tasks can run simultaneously on different `(window, shard)` pairs when capacity permits.

### 11.2 Interruptible Compaction

Compaction work is chunked — process one row-group's worth of data, then check whether to yield. If query load increases and spare capacity drops, compaction tasks pause mid-merge and resume when capacity returns.

Compaction state is suspendable: the k-way merge iterators hold their position, and the output segment is written incrementally (append row-groups as produced).

### 11.3 Manifest Contention

Compaction and ingest for the same table both need to update that table's manifest, so they contend on a **per-table** manifest lock (storage-format.md Section 14.3). Different tables never contend with each other. The actual segment writes are concurrent and lock-free; only the final manifest update is serialized. Since manifest updates are fast (write JSON, fsync, rename), the lock is held briefly.

---

## 12. Error Handling

### 12.1 Error Propagation

Errors propagate upward through the pipeline in two layers. Operators return `OperatorError`. The engine wraps that in `ExecutionError` when surfacing the failure to callers. The pipeline is torn down and resources (including spill files) are released.

```rust
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    #[error("memory budget exceeded: {used} bytes used, {budget} bytes budgeted")]
    MemoryBudgetExceeded { used: u64, budget: u64 },

    #[error("aggregation group cardinality limit exceeded: {limit} groups")]
    MaxGroupsExceeded { limit: usize },

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("type error: {0}")]
    Type(#[from] TypeError),

    #[error("query cancelled")]
    Cancelled,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error(transparent)]
    Operator(#[from] OperatorError),

    #[error("query timed out after {elapsed_ms}ms")]
    Timeout { elapsed_ms: u64 },
}
```

### 12.2 Query Warnings

Non-fatal conditions (entity event limit exceeded, etc.) are recorded as warnings in `QueryContext`, not as errors:

```rust
pub enum QueryWarning {
    EntityEventLimitExceeded { entity_id: String, count: u64, limit: u64 },
}
```

Warnings are attached to the query result and surfaced to the caller. They do not abort the query. Each shard-task caps warnings at **1,000 entries** to prevent unbounded growth (e.g., bot-heavy datasets where many entities exceed the limit). When the cap is reached, a final `WarningsOverflow { suppressed_count }` warning is recorded.

---

## 13. Python Integration

### 13.1 Execution Flow

```
Python: db.query("MATCH(signup -> purchase) WITHIN 7d BY user_id | STATS COUNT(*)")
  → PyO3 boundary: release GIL
  → Rust: parse → plan → execute on thread pool
  → Rust: collect all results as Arrow RecordBatches
  → PyO3 boundary: reacquire GIL
  → Python: receives PyArrow Table (zero-copy)
```

### 13.2 Key Properties

- **GIL released during execution.** Python threads are not blocked while Rust processes the query. Other Python threads can run concurrently.
- **Zero-copy results.** Arrow RecordBatches are returned via PyArrow's zero-copy FFI interface. No serialization/deserialization.
- **All parallelism in Rust.** Python never sees the shard threads. The Python API is single-threaded from the caller's perspective.
- **Results fully materialized.** The query completes and all results are collected before returning to Python. No streaming iterator — results are a single PyArrow Table. This simplifies the API and avoids GIL/lifetime complexity.
- **Concurrent queries from Python threads.** If multiple Python threads issue queries concurrently (possible since the GIL is released during execution), queries queue in FIFO order waiting for worker threads (Section 9.3). The Python call blocks until the query completes.

---

## 14. Metrics and Observability

### 14.1 Per-Query Metrics

Collected during execution with minimal overhead (atomic counters, no allocation):

| Metric | Scope | Description |
|---|---|---|
| `rows_scanned` | per shard | Total rows read from segments |
| `rows_after_pushdown` | per shard | Rows surviving predicate pushdown |
| `rows_after_filter` | per shard | Rows surviving stateless filters |
| `entities_processed` | per shard | Entities fed to the entity operator |
| `entities_matched` | per shard | Entities producing non-None results |
| `entities_skipped` | per shard | Entities exceeding event limit |
| `bytes_scanned` | per shard | Raw bytes read from disk |
| `bytes_decoded` | per shard | Bytes of column data actually decoded |
| `segments_scanned` | per shard | Number of segment files opened |
| `segments_pruned` | per shard | Segments skipped by zone maps |
| `spill_bytes_written` | per shard | Bytes spilled to temporary files |
| `elapsed_ns` | per shard, total | Wall-clock time |

### 14.2 Collection

Each shard-task maintains its own `QueryMetrics` in its `ShardTaskContext` (Section 3.3). After all shard-tasks complete, the coordinator sums metrics across shards and attaches the totals to the query result. Warnings are similarly concatenated (up to the per-shard cap). The overhead is negligible — one counter increment per batch, not per row. No atomic operations needed since each `ShardTaskContext` is thread-local.

---

## 15. Crate Placement

| Type | Crate | Rationale |
|---|---|---|
| `PhysicalOperator` trait | `bqlite-operators` | Operators implement this; `bqlite-engine` consumes it via trait object |
| `EntityOperator` trait | `bqlite-operators` | Temporal operators implement this |
| `EntityOperatorAdapter` | `bqlite-operators` | Wraps `EntityOperator` into `PhysicalOperator` |
| `Accumulator` trait | `bqlite-operators` | Aggregation accumulator protocol |
| `HashAccumulator`, `AggState`, `GroupKey`, `SumState` | `bqlite-operators` | Default accumulator implementation |
| `DictFilterBitset` | `bqlite-storage` | Scan-time precomputed dictionary filter |
| `TypedKernel` | `bqlite-operators` | Monomorphized vectorized kernels |
| `OperatorError` | `bqlite-operators` | Operator-facing execution failures |
| `DemandSet` / `DemandCapabilities` | `bqlite-planner` | Plan-time demand propagation |
| `ExecutionError` | `bqlite-engine` | Query-facing wrapper around operator failures and timeouts |
| `QueryContext` / `QueryMetrics` | `bqlite-engine` | Execution-time state and metrics |
| `MemoryTracker` | `bqlite-engine` | Memory budget enforcement |
| Thread pool, query scheduler | `bqlite-engine` | Orchestration |
| Shard-task coordinator | `bqlite-engine` | Parallel execution |

This follows the dependency direction in CLAUDE.md: `bqlite-engine` depends on `bqlite-operators`, `bqlite-planner`, `bqlite-storage`, and `bqlite-core`.

---

## 16. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Execution model | Hybrid push (stateless) / pull (stateful), entity-aligned batches | Push maximizes vectorized throughput; pull suits entity-at-a-time processing |
| Batch size | 65,536 rows (matches row-group size), entity-aligned | One row-group = one batch; no splitting overhead |
| Large entity handling | Sub-batch streaming + injected entity event limiter (10M default) | Bounded memory and CPU per entity; limiter injected early in pipeline |
| Entity boundary detection | Inferred from entity_id column changes across and within batches | No separate signaling needed; data is sorted |
| Entity completion signal | `finish_entity()` method (no `is_last` flag) | Single responsibility: process_sub_batch accumulates, finish_entity emits |
| Operator interface | `PhysicalOperator` (pull) + `EntityOperator` (entity-streaming) + adapter | Separates batch mechanics from entity processing logic |
| Operator fusion | Generic demand propagation upstream from consumer to producer | Subsumes funnel/retention optimization automatically |
| Aggregation fusion | Incrementally computable aggregates fused into entity operator | Zero intermediate materialization; includes AVG, percentiles |
| Type dispatch | Arrow kernels + plan-time monomorphized hot paths | Per-batch dispatch, not per-row |
| Parallelism unit | One task per shard | Keeps entity streams intact, no cross-thread coordination |
| Thread pool | Rayon, fixed size = num_cores, queries queue FIFO | All cores utilized; concurrent queries share the pool |
| Default shard count | 32 | One shard per core on modern hardware; thread pool handles scheduling |
| Compaction scheduling | Up to num_cores threads, bounded by spare capacity | Uses idle cores; scales down under query load |
| Memory management | Hierarchical MemoryTracker; per-thread budget = query_budget / num_cores | Runtime enforcement with stable per-thread bounds |
| Spill-to-disk | Sort spill and IN subquery spill only; aggregation has hard `max_groups` cap | Aggregation spill deferred to v2; hard cap keeps the v1 engine small |
| Query timeout | Timer sets AtomicBool cancel flag; cooperative checking | Fast stopping, no polling overhead |
| Error handling | `OperatorError` in operators, wrapped by engine `ExecutionError`; warnings for non-fatal conditions | Keeps crate boundaries clean while preserving typed failures |
| Python integration | GIL released, zero-copy Arrow, results fully materialized | Simple API, no streaming complexity |

---

## 17. Open Questions for Other Design Docs

These questions are intentionally deferred to the design docs that own them:

- **Sequence Matching (TASK-004):** NFA construction from patterns. Thompson's algorithm vs. specialized fast paths. Held property binding and state multiplication. Time window enforcement within the NFA. Negation semantics. How does the sequence matcher implement `EntityOperator::supported_demands()` to advertise step-counter, boolean-match, and full-NFA strategies?
- **Query Language (TASK-002):** How do `HAVING`, `ORDER BY`, and `LIMIT` interact with the pipeline stages? When can `LIMIT` be pushed down to short-circuit shard execution? Exact syntax for query timeout specification. IN subquery syntax and scoping rules. PIVOT value list syntax.
