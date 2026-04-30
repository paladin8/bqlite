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

**Memory-conscious (Belief 6).** Queries execute within a bounded memory budget (default 3 GiB for query execution, see [`engine/memory-budget.md`](engine/memory-budget.md) — the canonical source — and storage-format.md Section 13 for the operational summary). Sub-batch streaming ensures that even entities with millions of events never blow the budget. Operators either spill (sort, cohort) or fail with `MemoryBudgetExceeded` per the per-operator policy table in `engine/memory-budget.md` § 7.

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
| Sessionization (SESSIONIZE) | Streaming fold | Current session ID + open-session event buffer (rows for the in-progress session) + session start/last timestamps + event count |
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
    /// Set to true to cancel the query. Checked between batches and between morsels.
    cancelled: Arc<AtomicBool>,
    /// Query timeout. The engine sets `cancelled` when this elapses.
    timeout: Option<Duration>,
    /// Memory tracker for this query (shared across all workers running this query).
    memory: Arc<MemoryTracker>,
    /// Tile size for stateless kernels in this query, decided at plan time
    /// (Section 3.6). Workers read this once at morsel start.
    tile_size: usize,
}
```

`QueryContext` is shared across workers via `Arc`. It contains only thread-safe fields. Per-worker mutable state lives in `WorkerContext`, which is created fresh when a worker begins draining a shard's morsels and merged into the query's totals after the worker's last morsel for that shard finishes:

```rust
/// Per-worker state for the duration of one (worker, shard) session.
/// Not shared across threads. A worker that processes morsels from
/// multiple shards holds a sequence of WorkerContext instances, one per
/// (worker, shard) pair.
pub struct WorkerContext {
    query: Arc<QueryContext>,
    /// Identity of the shard whose morsels this context is currently draining.
    shard_id: u16,
    metrics: QueryMetrics,
    warnings: Vec<QueryWarning>,
}
```

Aggregation is *not* held in `WorkerContext` — partial accumulators are per-shard (Section 9.5), accessed via a per-shard mutex from any worker draining that shard's morsels.

Operators receive a reference to `QueryContext` at construction time and check `cancelled` at natural yield points (between batches, between entity sub-batches, between morsels). Worst-case cancellation latency is one batch processing time.

### 3.4 Query Timeout

The engine spawns a lightweight timer when a query starts. If the query has a timeout (configurable per-query or via a global default), the timer sets the `cancelled` flag after the timeout elapses. The next yield point in any worker observes the flag and returns `Err(OperatorError::Cancelled)`. The engine maps that to `ExecutionError::Timeout` when the timeout fired, or `ExecutionError::Cancelled` for caller-initiated cancellation. This provides fast stopping without polling overhead — the flag check is a single atomic load.

### 3.5 Entity-Aligned Batches

The fundamental batch discipline: **never split an entity across batches**. Given `(entity_id, timestamp)` sort order from the storage layer, each batch extends to the next entity boundary. This means:

- Stateless operators (filter, project) process these batches in vectorized fashion, preserving entity alignment — they never need to track entity boundaries.
- Stateful operators can process a complete entity (or sub-batch sequence) without coordinating with other operators about entity transitions.

### 3.6 Batches and Execution Tiles

The pipeline distinguishes **three** sizing concepts that used to share a single number:

1. **Storage row-groups (~65,536 rows).** A storage-level unit within a segment file. Sized for encoding amortization, dictionary efficiency, and zone-map selectivity. Set by the storage layer (storage-format.md §3.3) and not visible to operators directly.
2. **`RecordBatch` (~64K rows, entity-aligned).** The unit the scan layer hands across the operator boundary in `next_batch()`. Each batch extends to the next entity boundary and may pack many small entities or hold a sub-batch slice of one large entity. The 64K target keeps batch metadata overhead low and aligns naturally with one row-group of one segment in the common single-segment case.
3. **Execution tile (default 2,048 rows).** The inner unit that stateless vectorized kernels operate on. A `RecordBatch` is iterated in tile-sized chunks; each kernel call sees one tile of input. Tiles are never split across an entity boundary — the final tile of a batch shrinks if the next row would belong to a different entity.

**Why three sizes.** A 64K row-group is good for encoding amortization but oversized for hot vectorized kernels: tight filter and arithmetic loops want their working set to fit in L1/L2, branchy filters want short selection vectors, and cancellation latency wants short yield-point intervals. DuckDB defaults to 2,048-row vectors for the same reasons. Storing in 64K chunks while *executing* in 2K tiles gets the encoding win without paying the cache cost.

**Tile sizing.** Default tile size is **2,048 rows**, configurable via `QueryContext::tile_size`. The planner may select a smaller tile (1,024) for highly branchy filters or a larger tile (4,096) for trivial projections — this is a single integer in `PhysicalPlan` set during physical planning, not a runtime decision. The tile size is fixed for the duration of a single batch's traversal so operators can size scratch buffers once at batch entry. Tile sizes outside `[1024, 4096]` are rejected at plan time.

**Scan layer batching rule.** The scan accumulates rows from the merge until either:

1. The target batch size is reached **and** the current entity has ended, or
2. The end of the shard's data is reached.

This is unchanged from before — the scan still produces entity-aligned `RecordBatch`es. What changed is that downstream stateless operators iterate the batch in tiles rather than processing it as a single 64K chunk. Stateful entity operators continue to receive whole entity sub-batches (Section 4) — tiling is a stateless-kernel concern.

**Sub-batch streaming for large entities.** A single entity with millions of events still streams across multiple `RecordBatch` calls (Section 5), and each batch is itself iterated in tiles. The entity-boundary invariant survives at both levels: no batch crosses an entity boundary, no tile crosses an entity boundary. Small entities (10–100 events) pack many per batch; a single large entity (100K+ events) is handled via sub-batch streaming (Section 5) at the **batch** level, not the tile level.

### 3.7 RecordBatch Schema Conventions

All `RecordBatch` values flowing through the pipeline obey a small set of conventions that operators can rely on without re-deriving them per-query.

**Column ordering.** Batches produced by the scan layer lay out columns in a fixed order — declared columns in table-schema order followed by the implicit system columns:

1. `entity_id` (first)
2. `ts` — timestamp (second)
3. `event_type` (third)
4. Remaining property columns sorted by **encoded size ascending** — the narrowest columns first, for cache efficiency in vectorized scans.
5. `__seq_id` — sequence identifier (synthesised from segment-footer `seq_id_range`).
6. `__batch_id` — ingest batch identifier (synthesised from segment-footer `batch_id`).

The property column ordering is decided at ingest/compaction time by the storage layer (storage-format.md §3.4 encoding selection has access to per-chunk sizes), not at plan time. Smaller columns first means more columns fit in cache lines during vectorized filter and project passes. The two system columns appear at the end of the projected batch (matching `OperatorSchema::from_table`), per `docs/design/storage/system-columns.md` §3 — they are not stored as on-disk column chunks and are synthesised at row-group decode time.

**Reference columns by name, not position.** Projection pruning removes and reorders columns between the scan and the first operator that references them, so any operator that hard-codes column indices is fragile. Every operator looks up columns by name through its `OperatorSchema`. The one exception is the `EntityOperatorAdapter`, which caches `entity_id_col_idx` once at construction.

**String materialization is always Utf8View.** Whenever a string column has to be materialized as a flat (non-dictionary) array — projection output, variable binding, step property forwarding, FSST-decoded payloads, fall-through paths in operators that don't yet support dictionary input — the result is an Arrow `StringViewArray` (`DataType::Utf8View`), never a `StringArray` (`DataType::Utf8`). Three reasons:

- Short strings (≤12 bytes) live inline in the view header — no offset/value buffer indirection. Event types, country codes, device names, and most categorical IDs never miss the view header.
- The view header carries a 4-byte prefix for long strings, so equality and prefix comparisons short-circuit before touching the value buffer. Most string comparisons in the engine terminate in the first 4 bytes.
- It composes uniformly with `DictionaryArray<Int32, Utf8View>`. The dictionary entry buffer is itself a `StringViewArray`, so dictionary materialization is a per-row code-to-view copy with no string allocation.

The `bqlite-storage` decoders already produce `StringViewArray` (see `crates/bqlite-storage/src/encoding/dictionary.rs`); operators must do the same. No code path is allowed to round-trip through `StringArray` for convenience — if a kernel only exists in flat-Utf8 form, wrap it in a small adapter that keeps the storage representation as Utf8View.

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

**Null bitmaps.** Nullable columns always carry an Arrow-compatible null bitmap. Non-nullable columns (`entity_id`, `ts`, `event_type`, `__seq_id`, `__batch_id`) never do — operators skip the null check entirely on these columns, and the storage layer does not allocate bitmaps for them (the synthesised system-column arrays are built without a null buffer). The schema declares which columns are nullable; the decoder trusts the schema and does not insert defensive checks.

### 3.8 Selection Vectors

Filters are the most common operator in any pipeline, and naively materializing a filtered batch by copying surviving rows into fresh Arrow buffers wastes memory bandwidth on every selective predicate. bqlite makes **selection vectors a first-class output of the filter operator**. Materialization (copy-and-shrink into a contiguous `RecordBatch`) is an explicit, demand-driven decision, not the default — see §3.8.3 for the terminology distinction between this and segment-level compaction.

> **Implementation contract.** The runtime shapes that realize this design — `StatelessKernel`, `FilterKernel` / `ProjectKernel`, the `materialize_filtered_batch` boundary helper, the `FusedStatelessSegment` driver, and the `FusedSegmentPhysical` planner descriptor — live in [`docs/design/engine/operator-fusion.md`](engine/operator-fusion.md). That document is the load-bearing spec for §3.8.1–§3.8.5; the present subsection establishes the vocabulary and §3.8.6 covers the encoded-path boundary that feeds the push segment.

#### 3.8.1 The `FilteredBatch` Type

Stateless operators consume and produce `FilteredBatch`, not bare `RecordBatch`. The type is declared in `bqlite-operators` and is the surface every push-based stateless operator works against:

```rust
/// A view over a `RecordBatch` plus an optional selection vector.
/// `selection == None` means "all rows", which is the cheapest, most
/// common shape produced by the scan layer before any filter has run.
pub struct FilteredBatch {
    pub batch: RecordBatch,
    pub selection: Option<SelectionVector>,
}

/// A row-index list into the parent `RecordBatch`. Sorted ascending so
/// downstream kernels can rely on monotonic access patterns.
pub struct SelectionVector {
    /// Sorted, ascending row indices into `batch`. Always `<= batch.num_rows()`.
    /// `len()` is the post-filter row count.
    indices: Vec<u32>,
}
```

`SelectionVector` is intentionally `Vec<u32>`, not a bitmap. Sequential indices are what the downstream Arrow kernels actually want, the size is bounded by the batch row count (so `u32` is always safe), and "sparse" selections compress naturally because most filtered batches keep contiguous runs.

#### 3.8.2 Operator Contract

Stateless vectorized operators implement a small extension trait over `PhysicalOperator`:

```rust
pub trait StatelessKernel {
    /// Apply this kernel to the input view, returning a new view over the
    /// same underlying RecordBatch (or, if the kernel rewrites columns
    /// like `Project` does, over a freshly allocated one).
    fn apply(&self, input: FilteredBatch) -> FilteredBatch;
}
```

Three rules govern how kernels manipulate the selection vector:

1. **Filter narrows the selection.** `Filter` evaluates its predicate against the selected rows of `input.batch`, intersects the result with `input.selection`, and returns a new `FilteredBatch` with the same `batch` reference and a narrower `selection`. No row data is copied.
2. **Project rewrites columns, not rows.** `Project` allocates a new `RecordBatch` containing exactly the projected columns, but **at the post-selection row count** — i.e. the projection kernel walks the existing selection vector and writes only the surviving rows into the new column buffers. The output `FilteredBatch` has `selection: None` because the new batch already represents the filtered shape.
3. **Limit truncates the selection.** `Limit` shortens the selection vector (or, when `selection == None`, slices the batch) to the remaining row budget. The underlying `batch` is left untouched.

A few intentional consequences:

- Adjacent filters compose cheaply: `filter(a) → filter(b)` builds a single selection vector via two passes over the predicate, never copying row data.
- Projection is the **only** stateless kernel that allocates new column buffers in the common case. Everything else manipulates indices into the scan's batch.
- Stateful entity operators (Section 4) do not see selection vectors. The `EntityOperatorAdapter` is responsible for materializing any pending selection into a contiguous `RecordBatch` slice before calling `process_sub_batch` — see §3.8.3.

#### 3.8.3 Materialization Triggers

> **Terminology.** "Materialization" here means "collapse a `FilteredBatch { batch, selection }` into a fresh contiguous `RecordBatch` containing only the selected rows." This is distinct from storage-format.md §7 "Compaction", which is segment-level LSM merging. Keeping the two concepts linguistically separated is load-bearing because they sit in different layers of the engine and both come up in benchmark/metric conversation.

The selection vector is not free to carry forever. `FilteredBatch` is an *internal* shape inside the fused push segment of stateless operators described in this subsection (§3.8); it never crosses the outer `PhysicalOperator::next_batch()` boundary of that segment. (A separate, earlier materialization boundary exists *inside* the scan/filter segment when the encoded read path is used — see §3.8.6. That boundary also emits a `FilteredBatch`, which then becomes the input to the push segment described here.) Three conditions trigger explicit materialization at the §3.8 boundary (a fresh `RecordBatch` containing only the selected rows is produced, and `selection` is reset to `None`):

1. **Sparsity threshold.** When `selection.len() < 0.10 * batch.num_rows()`, the indirection cost on subsequent kernel passes exceeds the cost of one bulk copy. Materialization happens at the sparsity-detecting kernel's entry point, before its own work.
2. **Push segment boundary.** The push segment that wraps a stateless kernel chain materializes at its outer `PhysicalOperator::next_batch()` boundary. The `EntityOperatorAdapter` and any other downstream `PhysicalOperator` consumer always observe a contiguous `RecordBatch` — they never see a selection vector. This keeps the public operator trait surface unchanged: `FilteredBatch` is a stateless-segment-internal type only.
3. **Hand-off to aggregation.** When a non-fused `AggregatePhysical` follows a stateless segment, the segment boundary in (2) is what materializes; `HashAccumulator::update_batch` always sees a contiguous `RecordBatch`.

Materialization is implemented once, in `bqlite-operators::materialize_filtered_batch(FilteredBatch) -> RecordBatch`, and reused at every trigger site. Operators never invent their own materialization path.

#### 3.8.4 Why Not a Bitmap

Arrow's `BooleanArray` is a natural-looking choice for selection. We picked `Vec<u32>` for three reasons:

- The downstream pattern is "iterate the surviving rows, do work per row." Iteration over a `Vec<u32>` is one cache-line read per ~16 rows; iteration over a bitmap costs a popcount per word plus a bit-walk. The bitmap wins only at very high selectivity where materialization is cheap anyway.
- `arrow::compute::filter` (which is the fallback path inside `materialize_filtered_batch`) takes a `BooleanArray`. We can build it from `Vec<u32>` cheaply on the rare materialization path; the reverse (bitmap → indices) is the wrong default.
- `Vec<u32>` composes with the dictionary-filter precomputation in §3.7: a dictionary filter produces a `BitVec` over codes, then a single pass writes surviving row indices into the selection vector.

#### 3.8.5 Interaction With Dictionary Pushdown

The `DictFilterBitset` from §3.7 is the precomputation step; the selection vector is the result. The flow:

```
ScanOperator
  ├─ produces RecordBatch with DictionaryArray<Int32, Utf8View>
  ├─ FilterOperator pulls the batch
  │   └─ uses DictFilterBitset to mark surviving codes
  │   └─ writes surviving row indices into a SelectionVector
  └─ FilteredBatch { batch, selection: Some(sv) } flows downstream
```

This means dictionary pushdown is no longer "filter the batch in place" — it is the canonical example of producing a selection vector without touching the underlying value buffers. The dictionary stays the dictionary, the codes stay the codes, the selection vector tells everyone downstream which rows count.

#### 3.8.6 Pre-Boundary Encoded Read Path

Subsections §3.8.1–§3.8.5 describe the pipeline *after* row data has been materialized into Arrow buffers. A separate, earlier stage applies when the scan operator runs on the encoded read path (see `docs/design/storage/zero-copy-scan-filter.md` for the end-to-end design and `docs/design/storage/reader-trait.md` §5.3 for the trait-level contract):

```
SegmentScan
  ├─ emits EncodedBatch (Arc-backed encoded columns: PlainFixed, Dictionary,
  │                      Rle, Constant, Delta, BitPacking, For, Fsst, ...)
  │
  ├─ encoded kernels (scan/filter) operate directly on EncodedBatch +
  │   RowSelection (Indices | Runs) without decoding full columns
  │
  └─ materialization boundary
      └─ collapses the selection and decodes surviving rows into a
         RecordBatch, emitting FilteredBatch { batch, selection: None }
```

Three rules govern this pre-boundary stage:

1. **Encoded IR is internal.** `EncodedBatch`, `EncodedColumn`, `PinnedChunk`, and `RowSelection` live in `bqlite-core::encoded`. They are the input/output currency between the scan and the encoded kernels only. Nothing downstream of the materialization boundary ever sees them.
2. **Materialization happens exactly once per segment.** The encoded path does not produce a selection-carrying `FilteredBatch`; it produces a *dense* `FilteredBatch { batch, selection: None }` at the boundary. The §3.8 push segment inherits a dense batch and applies §3.8.1–§3.8.3 rules from there. A mid-segment stateless kernel in the §3.8 chain may still re-introduce a selection — that reintroduction is governed by §3.8.2, not by this subsection.
3. **Boundary placement is additive.** The encoded read path is selected by `ScanPath::Encoded` (or `Auto` when the heuristic opts in). When `ScanPath::Materialized` is used, the scan emits `FilteredBatch::dense(record_batch)` directly and there is no encoded kernel stage at all. Downstream operators cannot tell which path produced their input.

The copy-budget invariant enforced by this stage — 0 payload copies on the uncompressed read path and exactly one on the LZ4 decompression path, measured in bytes via `Metrics::record_bytes_*` — is documented and regression-tested in `docs/design/storage/zero-copy-scan-filter.md`. This subsection only fixes the execution-model vocabulary: "materialization" can mean *either* the §3.8.3 push-segment boundary *or* the §3.8.6 scan/filter boundary, and benchmark / metric discussion must say which one.

---

## 4. Entity Operator Interface

Stateful temporal operators implement a separate trait that the engine wraps inside a `PhysicalOperator` adapter:

```rust
/// Stateful per-entity operator.
/// The operator itself is immutable (&self) — all mutable state lives in State.
/// This makes the compiled operator safely shareable across workers.
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
    ///
    /// `process_sub_batch` receives a whole sub-batch, not a tile. Stateful
    /// operators that maintain per-entity state are not tile-iterated — see
    /// Section 3.6 ("Batches and Execution Tiles"). Tiling is a stateless-
    /// kernel optimization and stops at the `EntityOperatorAdapter` boundary.
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

- **`&self` is immutable.** The compiled operator (NFA program, predicates, schema, configuration) is shared across all workers via `Arc`. All mutable state lives in `Self::State`, created fresh per entity by `create_state`. This is what makes `Send + Sync` sound even though each worker runs morsels independently — no worker can mutate the operator itself.
- **`create_state(entity_id)` instead of `State: Default`** — allows operator configuration to influence the initial state, and gives the operator access to the entity identifier for warning attribution (`QueryWarning::EntityEventLimitExceeded`, `ActiveStateLimitExceeded`, etc.).
- **`finish_entity()` is the sole completion signal** — no `is_last` flag on `process_sub_batch()`. The adapter calls `process_sub_batch()` for every sub-batch, then `finish_entity()` exactly once. Clean single-responsibility: `process_sub_batch` accumulates, `finish_entity` emits and consumes the state.
- **`finish_entity()` returns `Option<RecordBatch>`** not `Option<Row>` — a single-row `RecordBatch` for operators that emit one result per entity, or a multi-row `RecordBatch` for operators like SESSIONIZE (one row per session) or windowed operators (one row per input event). Avoids an undefined `Row` type and keeps everything in Arrow's type system.
- **`finish_entity_into()` has a default implementation.** Non-fused operators leave it alone; fused operators override it to skip the per-entity `RecordBatch` materialization entirely.
- **No `Result<...>` returns on the hot-path methods.** Errors surface through different channels to keep the inner loop branch-free:
  - **Memory pressure** is caught at the allocation site (`MemoryTracker::try_reserve` inside operator internals). Operators that cannot spill set an error flag on the shared `QueryContext` and return early from `process_sub_batch`; the adapter observes the flag between sub-batches and aborts the query.
  - **Cancellation** is checked against `QueryContext::cancelled` between sub-batches, not inside the per-event loop.
  - **Invariant violations** panic — the engine catches panics at the morsel boundary (one `catch_unwind` per worker per morsel) and surfaces them as `ExecutionError::OperatorPanic`.

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
2. A `QueryWarning::EntityEventLimitExceeded { entity_id, count }` is recorded in the worker's `WorkerContext`.
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

If not fused with Stage 3, standard hash aggregation on the output of the entity stage via `HashAccumulator` (Section 9.5). Partial aggregation per shard (one accumulator per shard, fed by all the workers that drained its morsels), final merge across shards via `Accumulator::merge`. Group cardinality is bounded by `max_groups` (default 1M); overflow produces `OperatorError::MaxGroupsExceeded`. There is no aggregation spill in v1 — see Section 10.3.

### Stage 5: Output

Final projection, ordering (if requested), limit, result collection as Arrow `RecordBatch`es.

### 7.1 LIMIT Pushdown

When the query includes a `LIMIT N`, the pipeline short-circuits after N result rows are produced. The `cancelled` flag in `QueryContext` is set, stopping all workers at their next yield point (between morsels for the workers that aren't yet inside one, between batches for those in the middle of a morsel). For queries with `ORDER BY` + `LIMIT`, all shards must complete before the final merge-sort can apply the limit — LIMIT pushdown applies only when no cross-shard ordering is required.

### 7.2 ORDER BY Across Shards

When the query includes `ORDER BY`, each shard produces locally-sorted results. A final k-way merge-sort (binary heap, k = num_shards) across shard results produces the globally-sorted output. For `ORDER BY` on non-entity columns (e.g., aggregate values), each shard's partial results are sorted locally, then merge-sorted.

### 7.3 Additional Operator Execution

**Window functions (OVER).** Window functions (LAG, LEAD, ROW_NUMBER, running aggregates) are stateful per-entity operators that emit one row per input row rather than one row per entity. They implement `EntityOperator` with `finish_entity()` returning a multi-row `RecordBatch` containing the entity's full output. The adapter handles this naturally — it collects multi-row results into the output buffer like any other `RecordBatch`. Within-entity ordering is guaranteed by the scan's timestamp sort order.

**SAMPLE.** Entity sampling is implemented at the scan level using a stable fraction threshold over `xxHash64`: `xxhash64(entity_id_bytes, seed) < fraction * u64::MAX`. The scan skips entities that don't pass the filter before producing any batches for them. The `entity_id_bytes` input is the canonical serialization of the entity key value (`UTF-8` bytes for `String`, little-endian 8 bytes for `Int`). Results are deterministic and reproducible for the same entity set and seed.

**IN (subquery).** The inner query executes first, materializing a hash set of entity IDs (or compound keys). The outer query's scan or filter stage probes this hash set to filter rows. For the common case of entity-level cohort filtering (`WHERE (user_id) IN (subquery)`), this is an entity-level semi-join — the hash set is built once and probed per entity in the outer scan. In v1 the hash set does not spill; it is bounded by the query's memory budget, and queries that exceed that budget fail rather than silently truncating or spilling cohort state.

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

### 9.1 Shards, Morsels, Workers

The parallelism model has three distinct concepts that used to be conflated under "shard-task":

| Concept | Owns | Bounded by |
|---|---|---|
| **Shard** | A contiguous slice of entity space (`xxhash64(entity_id) % num_shards == s`) | `num_shards` (default 32, set at database init — storage-format.md §5.1) |
| **Morsel** | A contiguous entity-id range *within* one shard, generated dynamically | The shard's segment inventory and current load |
| **Worker** | A thread in the engine's Rayon worker pool | `num_cores` |

A query's execution proceeds as: for each shard the query touches, the engine generates morsels lazily and feeds them to a single per-query lock-free MPMC queue. Workers pull morsels with **centralized lock-free dispatch + consumer-side load balancing** — any worker can pull any shard's morsels off the shared queue, which gives the load-balancing behaviour of a work-stealing scheduler without per-worker steal-deque infrastructure. Each worker runs the full pipeline (scan → stateless segment → entity operator → partial aggregate) on its current morsel and produces a partial result. See `engine/morsel-scheduler.md` §4.1–§4.2 for the queue / dispatch protocol.

### 9.2 Why Three Concepts

Each layer carries a different invariant:

- **Shard = correctness boundary.** Entities are hash-pinned to shards (storage-format.md §5.1). Cross-table joins (storage-format.md §5.1, query-language.md §19) and the k-way window merge inside a shard rely on this. Shards never get split, never get reassigned at query time, and never share entities. Distributed execution will eventually use shard as the unit of distribution; this layer is the future-proofing.
- **Morsel = execution boundary.** Behavioral data is power-law distributed: a 1% slice of entities can hold 30%+ of events. With one task per shard, the unluckiest worker bottlenecks the whole query. Morsels let the scheduler refill idle workers from busy shards without violating shard ownership. The target morsel size is **~64 row-groups (≈4M rows)** at the high end, **single-row-group** at the low end — the morsel generator picks based on the shard's segment inventory and the current query budget.
- **Worker = scheduling boundary.** Workers are the only threads that hold operator state. The pool is fixed at `num_cores`; queries queue at the pool boundary, not at the morsel boundary.

### 9.3 The Single-Entity Invariant

Stateful temporal operators need all of an entity's events in timestamp order, across windows. The morsel generator preserves this by:

1. **Cutting morsels on entity boundaries.** A morsel is always a half-open `[entity_lo, entity_hi)` range over the shard's sort order. The generator advances through the shard's segments (after the k-way window merge) until it has accumulated approximately the target row count, then snaps the upper bound to the next entity boundary. The next morsel begins at that boundary.
2. **Routing all of an entity's segments to the same morsel.** Because shards are hash-pinned and window merges happen *inside* a shard, the k-way merge for an entity's events lands in exactly one morsel — the one whose entity range contains the entity's hash bucket.
3. **Forbidding mid-entity preemption.** Once a worker starts processing a morsel, it runs the morsel to completion before checking for cancellation between morsels. Within a morsel, cancellation checks happen between batches (Section 3.3) and between sub-batches inside `EntityOperatorAdapter` (§4.1).

The contract is: every entity touched by the query is fully processed by exactly one worker on exactly one morsel. No entity is split, no entity is processed twice.

### 9.4 Thread Pool, Morsel Queue, Query Queuing

- **Worker pool.** Fixed-size Rayon thread pool of `num_cores` workers (configurable through `EngineConfig::query_threads`). Shared across queries; compaction has its own pool and acquires from a `CoreBudget` semaphore with the same contract (Section 11, `engine/morsel-scheduler.md` §7). Sharing one `CoreBudget` *instance* between the engine and the storage compaction scheduler — so a running query actually pre-empts new compaction permit acquisitions — is forward-compatible follow-on work; the v1 engine constructs its own `CoreBudget`, and the public `acquire_n` contract is identical regardless of who owns the underlying instance.
- **Morsel queue.** **One lock-free MPMC queue per query**, fed by **one generator per shard** (the per-shard generators push into the shared queue). Workers pull morsels with `try_pop`. Lazy generation keeps in-flight memory bounded by `2 × num_workers` morsel descriptors, not by the total morsel count for the shard. See `engine/morsel-scheduler.md` §3.5 / §4.1 for the per-query / per-shard split and the lazy-generation contract.
- **Default shard count: 32** (storage-format.md). On machines with `num_cores < 32`, multiple shards' morsels interleave on the same worker — that is the *point* of the morsel queue. On machines with `num_cores > 32`, the morsel generator can produce more morsels than there are shards, keeping all cores busy on skewed workloads.
- **Query queuing.** Queries submit their morsel generators to the engine's query queue in FIFO order. The serialization point is the shared `CoreBudget` semaphore: each query atomically acquires `query_threads` permits at submit time via `CoreBudget::acquire_n` (`engine/morsel-scheduler.md` §7.1) and holds them until finalize. Compaction acquires permits per row group and releases at row-group boundaries, so a queued query is unblocked once active compaction drops below the query's permit demand. Queries do **not** preempt each other mid-morsel.
- **Concurrent queries.** Multiple queries can have morsels in flight if the pool has capacity. The memory budget is divided across the fixed worker pool (Section 10.2), so per-worker bounds are stable regardless of how many queries are active.

### 9.5 Partial Aggregation and Final Merge

Each query owns one **per-shard `Mutex<Box<dyn Accumulator>>`** (an `AccumulatorHandle`), constructed by the coordinator at query start. Workers running morsels for shard *S* lock that handle's mutex at fused-entity-operator `finish_entity_into` boundaries (per-entity grain — design `engine/morsel-scheduler.md` §6.2 / §6.3). Multiple morsels in the same shard mutate the same accumulator, and the coordinator thread then performs a final merge across shards:

- `COUNT` / `SUM`: sum the partial values.
- `MIN` / `MAX`: min/max across partials.
- `AVG`: algebraic aggregate — each shard tracks `(sum, count)`; final merge computes `total_sum / total_count`.
- `P50` / `P90` / `P95` / `P99`: each shard collects a DDSketch; final merge combines sketches (constant-time under DDSketch's merge operator) and extracts quantiles with bounded relative error.
- `COUNT_DISTINCT`: each shard maintains an exact set; final merge unions those sets.

Non-aggregated results (selection queries) are concatenated across shards and optionally merge-sorted (k-way binary heap, k = num_shards) for `ORDER BY`.

The `Accumulator` trait supports both incremental updates and cross-shard merging:

```rust
/// Receives incremental updates from fused entity operators and from
/// non-fused aggregate nodes. One accumulator per shard, shared across
/// the morsels of that shard via per-worker handoff; merged across
/// shards after execution. Workers serialize updates within a shard via
/// the shard's per-shard mutex on its accumulator handle.
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
    /// Each shard produces one accumulator (fed by all the workers that ran
    /// morsels for that shard); they are merged pairwise on the coordinator
    /// after every shard's last morsel finishes.
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

For fused entity operators that use `finish_entity_into()`, each shard maintains a single accumulator that all of the shard's morsels feed into via worker handoff. The coordinator merges per-shard accumulators via `Accumulator::merge()` to produce the final result. Per-shard partials are *not* further subdivided per morsel — that would force the coordinator to do `num_shards × morsels_per_shard` merges instead of `num_shards`, with no correctness benefit.

---

## 10. Memory Management

> The reservation/release contract, tracked allocation classes,
> per-operator spill-vs-fail policy, and the `MemoryBudget` ↔
> `QueryContext` wiring are canonical in
> [`engine/memory-budget.md`](engine/memory-budget.md). This section
> sketches the surface and the per-worker working-set arithmetic; for
> the authoritative trait shape, defaults, and policy table see that
> doc.

### 10.1 Memory Tracking

Every query carries an `Arc<dyn MemoryBudget>` on its `QueryContext`. In production the concrete implementation is a `MemoryTracker` (TASK-510) backed by a single `AtomicU64` used-bytes counter and a fixed `budget`. In tests the `UnboundedMemory` stub is used (`bqlite-core::memory`).

```rust
pub trait MemoryBudget: Send + Sync {
    fn try_reserve(&self, bytes: u64) -> Result<MemoryReservation>;
    fn register_spill_handler(&self, handler: Arc<dyn SpillNotification>);
    fn used_bytes(&self) -> u64;
    fn budget_bytes(&self) -> u64;
}
```

Every allocation that grows with data size — aggregation hash tables, hash sets for IN subqueries, sort buffers for ORDER BY, decoded column buffers, sequence-match output buffers — goes through `MemoryBudget::try_reserve()`. Small fixed-size allocations (operator structs, compiled NFA, per-entity state, per-tile scratch) are not tracked individually. The tracked/untracked classification is enumerated in `engine/memory-budget.md` § 5.

**Spill-vs-fail.** When `try_reserve()` returns `Err`, the budget itself first invokes any registered spill handlers (§ 10.3, `engine/memory-budget.md` § 4). If no handler frees enough bytes, the call fails and the operator propagates `MemoryBudgetExceeded`, aborting the query. Spill is the preferred response only for operators that explicitly opt in (sort, cohort/IN-subquery once TASK-502 freezes the protocol). All other operators fail fast — the per-operator policy table is in `engine/memory-budget.md` § 7.

There is one tracker per query, shared across every worker draining that query's morsels via `Arc`. Contention is one atomic on the success path. There is no engine-wide parent tracker in v1; concurrent queries each carry their own budget. Adding cross-query global accounting is a future-wave addendum to `engine/memory-budget.md`.

### 10.2 Per-Worker Memory

Each *worker* (not each morsel, not each shard) holds the live working set for one in-flight morsel. Workers reuse the same buffers across morsels from the same shard, so the per-worker memory ceiling does not scale with morsel count:

| Component | Owner | Typical size | Bound |
|---|---|---|---|
| K-way merge read buffers | Worker (per active morsel) | k × 4 MB (k = windows in the morsel's shard) | Configurable buffer size |
| Current batch | Worker | ~5 MB (64K rows × ~10 cols × 8 bytes) | One row-group |
| Stateless tile scratch | Worker | ~32 KB (one tile worth) | `tile_size × max_columns × 8 bytes` |
| Operator state | Worker (per active entity) | 10–100 bytes per entity | Compact by design |
| Partial aggregation state | **Shard** (shared across that shard's workers) | ~100 bytes per group | Hard cap at `max_groups` (default 1M); error on overflow |
| Decoded column data | Worker | Variable | Only demanded columns decoded |

Per-worker memory is dominated by the merge read buffers and the current batch — together ~29 MB. Multiplied by `num_cores` workers and added to the per-shard accumulator state, the working set for a query is `num_cores × 29 MB + num_shards × accumulator_bytes`, which is independent of how many morsels the query produces.

On a 16-core machine: `16 × 29 MB + 32 × ~3 MB ≈ 560 MB`. On a 32-core machine: `32 × 29 MB + 32 × ~3 MB ≈ 1.0 GB`. Both fit within the 3 GiB query budget with headroom for the worst-case accumulator. The per-worker working set (`3 GiB / num_cores`) is an **arithmetic decomposition** for planning, not a runtime sub-budget; all reservations land in the single per-query `MemoryBudget` (see `engine/memory-budget.md` § 6). Adding more morsels does not add to the live working set, because the worker pool's `num_cores` ceiling caps in-flight morsels at `num_cores`.

Per-shard accumulator state is bounded by `max_groups × ~100 bytes ≈ 100 MB` per shard, but in practice the partial accumulators are much smaller because each shard sees only its slice of group keys.

### 10.3 Spill-to-Disk

> The on-disk file layout, naming, spill-root configuration, RAII
> cleanup contract, and crash-recovery sweep are owned by
> [`engine/spill.md`](engine/spill.md) (TASK-502). This section
> sketches the surface and which operators participate; for the
> authoritative protocol see that doc.

The v1 spill surface is small and deliberately so:

- **Sort spill (ORDER BY).** When `MemoryBudget::try_reserve` would otherwise return `Err`, `SortOperator` writes the in-memory run to a temporary file as an Arrow IPC stream, drops the in-memory copy and its reservation, and at end-of-input k-way-merges the spilled runs (and any in-memory residual) into the output stream. Implementation: TASK-513.
- **Ingest partitioner spill.** The `(window_id, shard_id)` partitioner self-triggers spill against its own `Partitioner::budget_bytes` ceiling (256 MiB default; outside `QueryContext`). Each spilled bucket is sorted by `(entity_id, ts)` before being written; `drain_sorted` becomes a k-way merge that preserves the original ordering and `batch_id` contract. Implementation: TASK-512.
- **Cohort / IN-subquery: no spill in v1.** Cohort hash sets (`MergeSources`, `SubqueryFilter`) are tracked allocation classes per `engine/memory-budget.md` § 5.1; if `try_reserve` fails during materialisation the query aborts with `BqliteError::MemoryBudgetExceeded`. The on-disk hash-set sketch in earlier drafts of this section is retired — `engine/spill.md` § 4.3 documents why an on-disk binary-search probe is the wrong v1 trade. TASK-514 wires the budget integration with no spill code path.
- **Aggregation: no spill in v1.** `HashAccumulator` enforces a hard cap (`max_groups`, default 1M) at `update()` time and returns an error on overflow — see Section 9.5 and `engine/memory-budget.md` § 7. External hash aggregation is deferred past v1; the cap is the v1 backstop.

Every spill file lives under `<spill_root>/<query_id>/<purpose>-<seq>.spill`, with `<spill_root>` defaulting to `<db_root>/spill/`. Cleanup is RAII (`TempSpillFile` per `engine/cancellation.md` § 5.2), with a per-query belt-and-braces `rm_rf` after the operator tree drops and a whole-spill-root reclamation at engine open as crash recovery (`engine/spill.md` § 5.4 / § 9).

### 10.4 Aggregation State Bounds

Running aggregation state is small per group (counts, sums, min/max — see `AggState` in Section 9.5). The `max_groups` hard cap (default 1M) is enforced inside `HashAccumulator::update()` and is the only defense against runaway cardinality. When the cap is hit, the query fails with `OperatorError::MaxGroupsExceeded { limit }`, not a spill. 1M groups × ~100 bytes = ~100 MB — well within the 3 GiB query budget, with no spill complexity to manage.

---

## 11. Compaction Scheduling

### 11.1 Compaction Thread Pool

Compaction runs on a separate pool of up to `num_cores` threads, independent from query worker threads. The number of **active** compaction threads is dynamically bounded by the shared `CoreBudget` semaphore (`storage/compaction-concurrency.md` §4 / `engine/morsel-scheduler.md` §7):

```
active_compaction_threads ≤ num_cores - active_query_threads
```

The semaphore is loaded with `num_cores` permits at engine startup. Queries call `CoreBudget::acquire_n(query_threads)` at submit time (`engine/morsel-scheduler.md` §7.1) and hold those permits until finalize. Compaction acquires one permit per row group and releases at row-group boundaries. The arithmetic invariant above is therefore enforced naturally by the semaphore — no separate "active count" check is required.

This is a resource management decision, not a concurrency concern — compaction and queries can safely run concurrently due to manifest-based MVCC (storage-format.md Section 7.6). The semaphore ensures compaction uses only spare CPU and I/O capacity, yielding to queries when the machine is busy.

- **When query load is low:** compaction uses most cores, clearing backlog quickly.
- **When query load is high:** compaction scales down to zero active threads, resuming when query threads free up.
- **Mechanism:** compaction acquires its per-row-group permit through the same `CoreBudget` semaphore queries use. A queued query holds `query_threads` permits up front, so compaction's next `acquire()` blocks until the query releases on finalize.

Since each `(window, shard)` compacts independently (storage-format.md Section 7.1), compaction is embarrassingly parallel — multiple compaction tasks can run simultaneously on different `(window, shard)` pairs when capacity permits.

### 11.2 Interruptible Compaction

Compaction work is chunked — process one row-group's worth of data, then check whether to yield. If query load increases and spare capacity drops, compaction tasks pause mid-merge and resume when capacity returns.

Compaction state is suspendable: the k-way merge iterators hold their position, and the output segment is written incrementally (append row-groups as produced).

### 11.3 Manifest Contention

Compaction and ingest for the same table both need to update that table's manifest, so they contend on a **per-table** manifest lock (storage-format.md Section 14.3). Different tables never contend with each other. The actual segment writes are concurrent and lock-free; only the final manifest update is serialized. Since manifest updates are fast (write JSON, fsync, rename), the lock is held briefly.

---

## 12. Error Handling

### 12.1 Error Propagation

Operators and the engine unify on `bqlite_core::BqliteError` (per
`docs/design/operators/operator-traits.md` §2). The earlier sketch of
separate `OperatorError` / `ExecutionError` enums is **superseded** by
this rule.

Variants relevant to runtime failures:

- `BqliteError::Cancelled` — caller cancellation or LIMIT
  short-circuit (the LIMIT case never reaches the user).
- `BqliteError::Timeout { elapsed_ms }` — query exceeded its
  configured timeout. The engine's per-query timer fires
  `CancelReason::Timeout`, then `token.cancel()`.
- `BqliteError::OperatorPanic { message, location }` — a worker
  panicked. Caught at the morsel boundary by `catch_unwind`; peer
  workers exit at their next yield point via cascading
  `token.cancel()`.
- `BqliteError::MemoryBudgetExceeded { used, budget }` — per-query
  memory budget exhausted with no spillable handler willing to free
  bytes.
- `BqliteError::MaxGroupsExceeded { limit }` — `HashAccumulator` /
  `DistinctOperator` group-cardinality cap.
- `BqliteError::Io` / `BqliteError::Arrow` / `BqliteError::Schema` /
  `BqliteError::Plan` / `BqliteError::Execution` /
  `BqliteError::Corruption` — domain-specific failures unchanged
  from earlier waves.

The first-fire CAS on `QueryContext::reason` and the precedence rule
(panic > cancel > timeout > LimitHit) live in
`docs/design/engine/cancellation.md` §3.1 — that note is the single
source of truth for cancellation/timeout/panic attribution.

### 12.2 Query Warnings

Non-fatal conditions are surfaced through `bqlite_core::QueryWarning`
and attached to the result as `ExecutionResult::warnings` (success
path) or `ExecutionFailure::warnings` (error path). Per-worker
1,000-entry caps, coordinator merge, and `WarningsOverflow` ordering
are specified in `docs/design/engine/cancellation.md` §7. Operators
record warnings via `EntityOperator::take_pending_warnings` so the
hot path never sees engine-orchestration types.

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
- **Concurrent queries from Python threads.** If multiple Python threads issue queries concurrently (possible since the GIL is released during execution), queries queue in FIFO order waiting for worker threads (Section 9.4). The Python call blocks until the query completes.

---

## 14. Metrics and Observability

### 14.1 Per-Query Metrics

Collected during execution with minimal overhead (counter increments per batch, no allocation). Metrics live in the per-worker `WorkerContext` (Section 3.3) and are summed across all workers at query completion.

#### Throughput and shape

| Metric | Scope | Description |
|---|---|---|
| `rows_scanned` | per worker | Total rows read from segments |
| `rows_after_pushdown` | per worker | Rows surviving predicate pushdown |
| `rows_after_filter` | per worker | Rows surviving stateless filters |
| `selection_vector_materializations` | per worker | Number of `materialize_filtered_batch` calls in the fused stateless segment path (§3.8.3). Zero in Wave 2 where no fused segment exists; becomes load-bearing in Wave 5. |
| `entities_processed` | per worker | Entities fed to the entity operator |
| `entities_matched` | per worker | Entities producing non-None results |
| `entities_skipped` | per worker | Entities exceeding event limit |
| `bytes_scanned` | per worker | Raw bytes read from disk |
| `bytes_decoded` | per worker | Bytes of column data actually decoded |
| `bytes_decoded_lazily` | per worker | Bytes of dictionary/RLE/constant columns *not* expanded (decoded count only because something downstream forced materialization) |
| `segments_scanned` | per worker | Number of segment files opened |
| `segments_pruned` | per worker | Segments skipped by zone maps |
| `row_groups_pruned` | per worker | Row-groups skipped within scanned segments |
| `marks_pruned` | per worker | Mark-level skips (when storage-format §11 marks are enabled — zero on Wave 2) |
| `spill_bytes_written` | per worker | Bytes spilled to temporary files |
| `elapsed_ns` | per worker, total | Wall-clock time |

#### CPU-cost metrics

These are the metrics that turn "is the query fast" into "is the query *expensive*." Sampled, not counted on every batch — see §14.3 for sampling protocol.

| Metric | Scope | Description |
|---|---|---|
| `gb_per_sec_scanned` | per query, derived | `bytes_scanned_total / elapsed_ns_total / num_cores`. The headline GB/s/core throughput number. |
| `cycles_per_event` | per query, derived | `total_cpu_cycles / events_processed`. Compares directly across hardware. |
| `decode_to_operator_ratio` | per query, derived | Wall fraction spent inside encoding decode vs. inside operator logic. Sampled via `perf` counters or fallback wall-clock instrumentation per worker. |
| `bytes_decoded_to_scanned` | per query, derived | `bytes_decoded / bytes_scanned`. Lower is better — late materialization is working when this is well below 1. |
| `branch_misses` | per worker, sampled | Branch-prediction miss count from `perf_event_open` when available; zero on platforms without `PERF_COUNT_HW_BRANCH_MISSES`. |
| `llc_misses` | per worker, sampled | Last-level cache miss count from `perf_event_open` when available; the primary signal for "are tiles too big." |

#### Skew and parallelism metrics

The morsel scheduler is only valuable if we can see when it works and when it doesn't.

| Metric | Scope | Description |
|---|---|---|
| `morsels_dispatched` | per query | Total morsels generated across all shards |
| `morsels_per_shard_max` | per query | Largest morsel count for any single shard |
| `morsels_per_shard_min` | per query | Smallest morsel count for any single shard |
| `worker_idle_ns_p50` / `_p99` | per query | How long workers spent waiting on `morsel_queue.pop()` — P50 and P99 across all workers |
| `worker_busy_ns_max` / `_min` | per query | Per-worker total busy time, max and min — the spread is the skew signal |
| `entity_event_skew_p99` | per worker | 99th-percentile event count for any entity inside this worker's morsels |

#### Compaction interaction metrics

| Metric | Scope | Description |
|---|---|---|
| `compaction_active_ns` | per query | Wall time during which any compaction was active in the worker pool — for diagnosing query/compaction interference |

The full table is implemented incrementally — Wave 2 ships rows/throughput; CPU-cost and skew rows land alongside the morsel scheduler in Wave 5. Metrics that depend on a feature not yet shipped (`marks_pruned`, `morsels_*`) report zero until the feature lands.

### 14.3 Sampling Protocol for CPU-Cost Metrics

`branch_misses` and `llc_misses` come from `perf_event_open` on Linux and `kpc` on macOS. Both APIs have non-trivial setup cost, so they are not enabled per-batch. Instead:

- The engine opens one perf-event group per worker at query start (if the query is configured to collect CPU-cost metrics — opt-in via `QueryContext::collect_cpu_metrics`).
- The group is read once per morsel boundary, summing into the worker's `WorkerContext`.
- On platforms without perf counters, the metrics report zero and the per-query derived numbers reflect that absence.

CPU-cost metric collection adds <1% overhead per batch when enabled. It is off by default; the benchmark suite (TASK-236, TASK-507) turns it on for the bench job, and the CLI exposes it via `bqlite query --explain-perf`.

### 14.2 Collection

Each worker maintains its own `QueryMetrics` in its `WorkerContext` (Section 3.3). After all morsels complete, the coordinator sums metrics across all workers and attaches the totals to the query result. Warnings are similarly concatenated (up to the per-worker cap, with `suppressed_count` summed across workers). The overhead is negligible — one counter increment per batch, not per row. No atomic operations needed since each `WorkerContext` is thread-local.

---

## 15. Crate Placement

| Type | Crate | Rationale |
|---|---|---|
| `PhysicalOperator` trait | `bqlite-operators` | Operators implement this; `bqlite-engine` consumes it via trait object |
| `EntityOperator` trait | `bqlite-operators` | Temporal operators implement this |
| `EntityOperatorAdapter` | `bqlite-operators` | Wraps `EntityOperator` into `PhysicalOperator` |
| `FilteredBatch`, `SelectionVector` | `bqlite-operators` | Stateless-segment-internal filter representation (Section 3.8) |
| `StatelessKernel` trait | `bqlite-operators` | Extension trait stateless kernels implement on top of `PhysicalOperator` |
| `materialize_filtered_batch` | `bqlite-operators` | Single source of truth for selection-vector → contiguous `RecordBatch` materialization |
| `Accumulator` trait | `bqlite-operators` | Aggregation accumulator protocol |
| `HashAccumulator`, `AggState`, `GroupKey`, `SumState` | `bqlite-operators` | Default accumulator implementation |
| `DictFilterBitset` | `bqlite-storage` | Scan-time precomputed dictionary filter |
| `TypedKernel` | `bqlite-operators` | Monomorphized vectorized kernels |
| `OperatorError` | `bqlite-operators` | Operator-facing execution failures |
| `DemandSet` / `DemandCapabilities` | `bqlite-planner` | Plan-time demand propagation — `DemandSet` (planner-side) and `DemandCapabilities` (operator-side) per `demand-protocol.md` §4. TASK-427 relocated `DemandCapabilities` from its Wave 1 scaffold home in `bqlite-core` to `bqlite-planner::demand`, replacing the placeholder enum with a 7-field bool struct. |
| Parser orchestration (`Engine::query(text, db)`) | `bqlite-engine` | TASK-118 added `bqlite-parser` as a direct dep of `bqlite-engine` so the engine owns the single text-in, rows-out surface the CLI and future Python bindings call. See architecture.md "Dependency Direction" and CLAUDE.md for the updated graph (`bqlite-engine → parser, planner, operators, storage, core`). |
| `ExecutionError` | `bqlite-engine` | Query-facing wrapper around operator failures and timeouts |
| `QueryContext` / `QueryMetrics` | `bqlite-engine` | Execution-time state and metrics |
| `WorkerContext` | `bqlite-engine` | Per-(worker, shard) state — metrics, warnings, current shard identity (Section 3.3) |
| `MorselGenerator`, `MorselQueue` | `bqlite-engine` | Morsel-driven execution scheduler (Section 9.4) |
| `MemoryTracker` (impl of `MemoryBudget`) | `bqlite-engine` | Memory budget enforcement; trait surface in `bqlite-core::memory`, contract in `engine/memory-budget.md` |
| Thread pool, query scheduler | `bqlite-engine` | Orchestration |
| Shard-task coordinator | `bqlite-engine` | Parallel execution — drives morsel queues per shard |

This follows the dependency direction in CLAUDE.md: `bqlite-engine` depends on `bqlite-parser`, `bqlite-operators`, `bqlite-planner`, `bqlite-storage`, and `bqlite-core`.

---

## 16. Decision Summary

| Question | Decision | Rationale |
|---|---|---|
| Execution model | Hybrid push (stateless) / pull (stateful), entity-aligned batches | Push maximizes vectorized throughput; pull suits entity-at-a-time processing |
| `RecordBatch` size | ~65,536 rows, entity-aligned, hand-off unit between operators | Aligns with one row-group of one segment in the common single-segment case |
| Execution tile size | Default 2,048 rows, plan-time choice in `[1024, 4096]` | Cache-resident vectorized kernels; matches DuckDB's vector size for the same reasons |
| Stateful operator input | Whole entity sub-batches, not tiles | Tiling is a stateless-kernel optimization; stateful operators see full sub-batches via `EntityOperatorAdapter` |
| Filter output representation | Selection vector over the input batch (Section 3.8) | Avoids per-filter copy; materialization triggers when sparse or required by downstream |
| Large entity handling | Sub-batch streaming + injected entity event limiter (10M default) | Bounded memory and CPU per entity; limiter injected early in pipeline |
| Entity boundary detection | Inferred from entity_id column changes across and within batches | No separate signaling needed; data is sorted |
| Entity completion signal | `finish_entity()` method (no `is_last` flag) | Single responsibility: process_sub_batch accumulates, finish_entity emits |
| Operator interface | `PhysicalOperator` (pull) + `EntityOperator` (entity-streaming) + adapter | Separates batch mechanics from entity processing logic |
| Operator fusion | Generic demand propagation upstream from consumer to producer | Subsumes funnel/retention optimization automatically |
| Aggregation fusion | Incrementally computable aggregates fused into entity operator | Zero intermediate materialization; includes AVG, percentiles |
| Type dispatch | Arrow kernels + plan-time monomorphized hot paths | Per-batch dispatch, not per-row |
| Parallelism unit | Shard = ownership boundary, morsel = execution boundary, worker = scheduling boundary | Shard preserves entity-stream invariant; morsels balance load and absorb skew |
| Morsel granularity | Entity-range slice within a shard, single entity never split | Preserves "one entity, one worker" while load-balancing across cores |
| Thread pool | Rayon, fixed size = num_cores, queries queue FIFO | All cores utilized; concurrent queries share the pool |
| Default shard count | 32 | Distributed-ready partitioning unit; the worker pool drains shards via morsels |
| Compaction scheduling | Up to num_cores threads, bounded by spare capacity | Uses idle cores; scales down under query load |
| Memory management | Single per-query `MemoryBudget` (TASK-111 trait, `MemoryTracker` impl in TASK-510), shared across workers via `Arc`; per-worker working set is a planning hint = query_budget / num_cores. See `engine/memory-budget.md` (canonical). | Runtime enforcement with stable per-worker bounds, independent of morsel count |
| Spill-to-disk | Sort spill and IN subquery spill only; aggregation has hard `max_groups` cap | Aggregation spill deferred to v2; hard cap keeps the v1 engine small |
| Query timeout | Timer sets AtomicBool cancel flag; cooperative checking | Fast stopping, no polling overhead |
| Error handling | `OperatorError` in operators, wrapped by engine `ExecutionError`; warnings for non-fatal conditions | Keeps crate boundaries clean while preserving typed failures |
| Python integration | GIL released, zero-copy Arrow, results fully materialized | Simple API, no streaming complexity |

---

## 17. Open Questions for Other Design Docs

These questions are intentionally deferred to the design docs that own them:

- **Sequence Matching (TASK-004):** NFA construction from patterns. Thompson's algorithm vs. specialized fast paths. Held property binding and state multiplication. Time window enforcement within the NFA. Negation semantics. How does the sequence matcher implement `EntityOperator::supported_demands()` to advertise step-counter, boolean-match, and full-NFA strategies?
- **Query Language (TASK-002):** How do `HAVING`, `ORDER BY`, and `LIMIT` interact with the pipeline stages? When can `LIMIT` be pushed down to short-circuit shard execution? Exact syntax for query timeout specification. IN subquery syntax and scoping rules. PIVOT value list syntax.
