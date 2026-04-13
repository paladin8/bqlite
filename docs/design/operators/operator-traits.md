# PhysicalOperator + EntityOperator traits

**Wave**: 1
**Task**: TASK-108
**Status**: draft — frozen for Wave 1, extended by later waves

## 1. Scope

This note finalizes the **v0 trait surface** for physical operator execution and pins down the rules every operator crate agrees to across crate boundaries. Concretely:

- The two traits an operator implements — `PhysicalOperator` (pull-based batch iterator) and `EntityOperator` (stateful per-entity operator).
- The lifecycle (`open` / `next_batch` / `close`) every operator honours.
- How errors, cancellation, and sub-batch streaming surface through the trait API.
- Where the traits live in the dependency graph and why.

It does **not** design the `EntityOperatorAdapter` implementation, the fused-accumulator path, aggregation fusion, layered extraction, the full `QueryContext`, or metrics collection — those are deferred to later waves and are mentioned here only as forward references. Wave 1's job is to ship a trait surface stable enough that Wave 2+ operator work never has to rebase behind a trait change. After Wave 1 the trait surface is frozen; any later change requires a high-priority `[TRAIT]` task.

The authoritative background for the execution model is [execution-model.md](../execution-model.md). This note is narrower: it is the contract operator crates hold to, not the full execution story.

## 2. Relationship to the existing design docs

The surface defined here is a **minimal v0 compatible projection** of the richer model documented in execution-model.md §3–§5. Specifically:

| execution-model.md feature | Wave 1 trait surface | Rationale |
|---|---|---|
| `PhysicalOperator::output_schema()` / `next_batch()` (§3.2) | Lands verbatim. | These are the hot-path contract consumers rely on. |
| `open` / `close` lifecycle (implied, not in §3.2) | Added as trait methods with default no-op bodies. | Operators that hold OS resources (segment readers, spill files) need an explicit teardown hook. Defaulted so stateless operators ignore them. |
| `EntityOperator::create_state` / `process_sub_batch` / `finish_entity` / `required_columns` (§4) | Lands. | Core entity-at-a-time contract that scan + adapter + stateful operators all need. |
| `EntityOperator::finish_entity_into` + fused accumulator (§4) | **Deferred.** | Requires the `Accumulator` trait, which is a Wave 4+ dependency. Operators that want fusion later can override a defaulted method added in that wave without breaking existing impls. |
| `EntityOperator::supported_demands` + `DemandCapabilities` (§4, §8) | Lands (TASK-110 scaffold, upgraded to real protocol by TASK-427). | The real `DemandCapabilities` struct (7 bool fields) and `DemandPropagation` trait live in `bqlite-planner::demand` per `demand-protocol.md` §2–§5. `EntityOperator::supported_demands()` mirrors the trait with the same default. |
| `ScalarValue` for entity-id arg (§4) | Replaced with `bqlite_core::EntityId`. | `ScalarValue` is a DataFusion concept; `EntityId` is our native String/Int newtype from TASK-105 and is what ingest + fixtures already produce. |
| `QueryContext { cancelled, timeout, memory }` (§3.3) | Split: operators hold a lightweight `CancellationToken` (introduced here) and a `&dyn MemoryBudget` (TASK-111). The full `QueryContext` is a `bqlite-engine` concern and is composed from these pieces later. | Keeps `bqlite-operators` from pulling in engine types prematurely. The engine builds a `QueryContext` that *contains* a `CancellationToken`. |
| `OperatorError` distinct from `ExecutionError` (§12) | **Unified under `bqlite_core::BqliteError`.** | TASK-102 already shipped `BqliteError` as the project-wide unified error, and it covers every variant operators need (`Execution`, `Cancelled`, `Io`, `Arrow`, `Schema`, `Plan`). Splitting into a separate `OperatorError` today would introduce pointless conversion boilerplate. A later wave can introduce `OperatorError` as a typed subset that projects into `BqliteError` if the distinction becomes load-bearing. |

### 2.1 Planner-pipeline doc consistency

TASK-108 originally called for a follow-up doc task to fix an inconsistency in planner-pipeline.md §15 that placed `PhysicalOperator` in `bqlite-engine`. **That inconsistency has already been resolved** — the current §15 (line 1400) correctly places the trait in `bqlite-operators`, and the prose at §4 (line 917) and the physical-plan description (line 1061) are consistent with this file. No follow-up doc task is needed.

## 3. Crate placement

| Item | Crate | Why |
|---|---|---|
| `PhysicalOperator` trait | `bqlite-operators` | Operators in this crate implement it; `bqlite-engine` consumes it via `Box<dyn PhysicalOperator>` in its bind step. Placing the trait in `bqlite-operators` preserves the `operators → core, storage, planner` dependency rule without forcing `bqlite-operators` to depend on `bqlite-engine` (which would create a cycle). |
| `EntityOperator` trait | `bqlite-operators` | Same rationale. Stateful temporal operators are the primary implementor and live in this crate. |
| `CancellationToken` | `bqlite-operators` | Lightweight shared flag, no orchestration logic. The engine's `QueryContext` holds one of these and hands `Arc<CancellationToken>` to operator constructors. |
| `BqliteError` | `bqlite-core` | Already landed by TASK-102 and re-used as the operator error type. |
| `OperatorSchema` | `bqlite-core` | Already landed by TASK-106. Propagated unchanged through the plan tree. |
| `EntityOperatorAdapter` impl | `bqlite-operators` | Not in Wave 1 — the adapter implementation is a later task. The trait lives here now so the adapter drops into place without rearranging crates. |

The operators crate's `Cargo.toml` already declares `bqlite-core`, `bqlite-storage`, `bqlite-planner`, `arrow`, and `thiserror` — this task does not widen that dep set.

## 4. PhysicalOperator trait

### 4.1 Definition

```rust
use arrow::record_batch::RecordBatch;
use bqlite_core::{OperatorSchema, Result};

/// Pull-based physical operator.
///
/// Every stateless operator (scan, filter, project, limit, ...) and every
/// `EntityOperatorAdapter` wrapping a stateful operator implements this trait.
/// It is the single interface `bqlite-engine` drives during execution.
pub trait PhysicalOperator: Send {
    /// The operator's output schema, known at plan time and stable for the
    /// lifetime of the operator.
    fn output_schema(&self) -> &OperatorSchema;

    /// Called once before the first `next_batch()` call. Default: no-op.
    ///
    /// Implementations that need to acquire OS resources (segment readers,
    /// spill files, thread-local caches) do so here so that any failure
    /// surfaces cleanly before results start flowing. Stateless combinators
    /// leave this defaulted.
    fn open(&mut self) -> Result<()> {
        Ok(())
    }

    /// Pull the next batch of rows. Returns `Ok(None)` when the operator is
    /// exhausted; subsequent calls after `Ok(None)` must continue returning
    /// `Ok(None)` without side effects.
    ///
    /// Errors abort the query — the engine tears down the operator tree and
    /// propagates the error to the caller via `Engine::query`.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Called once after iteration completes (whether by exhaustion or
    /// error) to release OS resources. Default: no-op.
    ///
    /// Implementations must be idempotent — the engine may call `close()`
    /// defensively during teardown after an error.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
```

### 4.2 Batch invariants

Callers rely on these without re-checking them per batch. Producers that violate them are incorrect, not just inefficient.

- **Entity alignment.** A `RecordBatch` returned from `next_batch()` never splits an entity across batches. All rows for a given `entity_id` are contiguous within a single batch, or are streamed as sub-batches to the same `EntityOperator` without interleaving from another entity.
- **Sort order.** Rows are sorted by `(entity_id, ts)` ascending. Downstream stateful operators rely on this for single-pass scanning.
- **Schema stability.** Every batch produced matches `output_schema()`. Column order, types, and nullability are fixed for the life of the operator.
- **Non-empty batches.** `next_batch()` should return `Ok(None)` rather than an empty `RecordBatch` when exhausted. Producers may emit empty batches only as a courtesy for filters that drop every row of an input batch; consumers must tolerate them.

The batch-size target is 65,536 rows to match storage row-groups (execution-model.md §3.6). This is a soft target — smaller batches are legal at entity boundaries or at end-of-stream.

### 4.3 Error propagation

All operator errors travel as `bqlite_core::BqliteError`. The relevant variants:

| Variant | Meaning | Who raises it |
|---|---|---|
| `Io` | Underlying I/O failed | Scan layer, segment reader |
| `Arrow` | Arrow kernel or batch construction failed | Any operator using Arrow compute |
| `Schema` | Runtime schema mismatch (should be rare after plan-time validation) | Any operator |
| `Execution` | Memory budget exceeded, group cardinality exceeded, or any other runtime execution failure | Aggregation, sort, spill manager |
| `Cancelled` | Cancellation flag observed at a yield point | Any operator |
| `Plan` / `Parse` | Should not appear at runtime — these are plan-time errors | — |

A later wave may introduce a typed `OperatorError` subset; if so, the trait's return type changes through a `[TRAIT]` task. For Wave 1, `BqliteError` is the single currency.

### 4.4 Lifecycle and tear-down

```
┌─────────────┐  open()  ┌──────────────┐  next_batch()*  ┌─────────────┐  close()  ┌─────────────┐
│ constructed ├─────────▶│    opened    ├────────────────▶│   drained   ├──────────▶│   closed    │
└─────────────┘          └──────────────┘                 └─────────────┘           └─────────────┘
                                │                                ▲
                                │                                │
                                └────── next_batch() returns Ok(None) or Err
```

Rules:

1. `open()` is called exactly once before the first `next_batch()`. The engine may inline a default no-op.
2. `next_batch()` is called repeatedly until it returns `Ok(None)` or `Err(_)`.
3. After `Ok(None)` or `Err(_)`, the engine calls `close()` exactly once. `close()` must tolerate being called without ever having seen a successful `next_batch()` (e.g. the operator tree was torn down because a sibling failed during `open()`).
4. An operator that fails inside `open()` should still accept a subsequent `close()`. The engine's tear-down is a single blanket `close()` across the whole tree.
5. After `close()`, calling any other method is a programming error; operators may panic or return `Err(BqliteError::Execution(...))` — there is no requirement to behave gracefully.

## 5. Cancellation

### 5.1 Design

Cancellation is **cooperative** and **flag-based**. Per execution-model.md §3.3 it is deliberately *not* a method on the trait: a method would invite one thread to mutate operator state while another is mid-pull, which is a data race.

```rust
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

/// Shared cancellation flag passed to operators at construction time.
///
/// Callers set the flag by invoking `cancel()`. Operators check it at
/// natural yield points (between batches or between entity sub-batches)
/// via `is_cancelled()` and return `Err(BqliteError::Cancelled)` when
/// they observe it.
///
/// `CancellationToken` is `Clone` — all clones share the same underlying
/// `Arc<AtomicBool>`. Engine code creates one token per query and clones
/// it into every operator in the tree.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self { Self::default() }

    /// Returns `true` once any holder has called `cancel()`.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Mark the token cancelled. Idempotent — repeated calls are cheap.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}
```

`Relaxed` ordering is sufficient because:

- There is no other shared state whose visibility the cancellation flag is supposed to synchronize — it is a hint, not a handshake.
- A missed-by-one-batch observation is already the cancellation latency bound (§5.2); stronger ordering would only trade a few nanoseconds of atomic cost for no observable behaviour change.

### 5.2 Check frequency

Operators check `CancellationToken::is_cancelled()`:

- **`PhysicalOperator::next_batch()`**: once at the top of each call, before doing any real work. An operator that observes a cancelled token returns `Err(BqliteError::Cancelled)` immediately.
- **`EntityOperator::process_sub_batch()`**: cancellation is checked by the wrapping `EntityOperatorAdapter` between sub-batches, not inside the inner per-event loop. This keeps the hot loop branch-free while still capping worst-case cancellation latency at one sub-batch.

The worst-case latency is one batch's processing time. Operators must not hold cancellation checks inside tight inner loops.

### 5.3 Ownership

Operators receive an `Arc<CancellationToken>` (or a cloned `CancellationToken` — `Clone` is cheap because the `Arc` is already inside) at construction time, not via a method argument. The engine creates one per query and hands copies to every operator.

The `QueryContext` / timeout / memory-budget wrapper is an engine-side concern (execution-model.md §3.3). Wave 1 operators only see the token.

## 6. EntityOperator trait

### 6.1 Definition

```rust
use arrow::record_batch::RecordBatch;
use bqlite_core::{EntityId, OperatorSchema};

/// Stateful per-entity operator.
///
/// The operator itself (`&self`) is immutable — all mutable state lives in
/// `Self::State` and is created fresh per entity by `create_state`. This
/// makes compiled operators safely shareable across shard-tasks.
///
/// Instances are wrapped in an `EntityOperatorAdapter` (added in a later
/// wave) that converts them into `PhysicalOperator`s by scanning an input
/// stream for entity-id changes and routing sub-batches through this trait.
pub trait EntityOperator: Send + Sync {
    /// Per-entity mutable state. Created fresh for each entity.
    type State: Send;

    /// Create initial state for a new entity.
    ///
    /// The `entity_id` is passed so operators that need per-entity warning
    /// attribution (event-limit exceeded, active-state overflow, etc.) can
    /// capture it. Most operators ignore the argument.
    fn create_state(&self, entity_id: &EntityId) -> Self::State;

    /// Output schema for this operator's results. Same contract as
    /// `PhysicalOperator::output_schema()`.
    fn output_schema(&self) -> &OperatorSchema;

    /// Process a sub-batch of events for the current entity.
    ///
    /// The adapter guarantees that:
    ///   - every row in `batch` belongs to the same entity,
    ///   - rows are sorted by timestamp ascending,
    ///   - sub-batch row count is bounded by the pipeline batch size.
    ///
    /// Returns `()` rather than `Result<()>` because the hot path is
    /// intentionally branch-free. Recoverable errors (memory pressure,
    /// warnings) are surfaced via the separate channels described in §6.3.
    /// Invariant violations panic — the engine catches panics at the
    /// shard-task boundary.
    fn process_sub_batch(&self, state: &mut Self::State, batch: &RecordBatch);

    /// Extract results for the entity after all sub-batches have been
    /// processed. Called exactly once per entity, after the final
    /// `process_sub_batch`. Consumes `state` — there is no reuse.
    ///
    /// Returns `None` if the entity produces no output rows (e.g. the
    /// pattern did not match). Returns `Some(batch)` with one or more rows
    /// for operators that emit one row per entity, one row per session,
    /// one row per match, etc.
    fn finish_entity(&self, state: Self::State) -> Option<RecordBatch>;

    /// The set of input columns this operator actually reads.
    ///
    /// Used by the planner's projection-pruning pass to drop unreferenced
    /// columns at the scan layer. Returning an empty slice means the
    /// operator is metadata-only (e.g. `COUNT(DISTINCT entity_id)`).
    fn required_columns(&self) -> &[String];
}
```

### 6.2 Why `&self` is immutable

The compiled operator carries plan-time configuration — NFA programs, compiled predicates, schema, extraction config. This configuration is shared across every shard-task in a parallel query via `Arc`. If the trait allowed `&mut self`, `Send + Sync` would be unsound: one shard would be mutating what another is reading.

All mutable state lives in `Self::State`, created fresh per entity by `create_state`. This is why `process_sub_batch` takes `&mut Self::State`, not `&mut self`.

The associated `State` type is intentionally owned per entity rather than pooled. Sequence matchers, sessionizers, and window functions produce compact state (tens to hundreds of bytes per entity) and pooling introduces lifetime complexity that costs more than it saves.

### 6.3 Sub-batch streaming

Most entities fit in a single sub-batch. Large entities (power-law distribution — think a bot that fired 5M events) cross row-group boundaries and are streamed to the operator as multiple sub-batches. The contract the adapter honours:

1. Sub-batches for a single entity arrive **consecutively**, with no other entity interleaved.
2. The operator maintains compact state in `Self::State` across sub-batches.
3. The scan drops each sub-batch's data before producing the next — only one sub-batch per entity is resident at a time.
4. `finish_entity()` is called exactly once, after the final `process_sub_batch()`.

For Wave 1, no stateful operators exist yet, so the sub-batch contract is documented here but not exercised. TASK-117 (scan/filter/project stubs) will drive a stateless `PhysicalOperator` tree; `EntityOperator` implementors arrive in later waves. The trait definition freezes the contract so those later waves inherit it unchanged.

### 6.4 Adapter preview

The adapter is introduced in a later wave. Its shape (from execution-model.md §4.1):

```
next_batch()
  ├─ drain pending sub-batch from prior call → process_sub_batch
  ├─ pull input.next_batch()
  │    ├─ None + no in-progress entity → finish_entity, drain output buffer, return None
  │    └─ Some(batch)
  │        ├─ scan entity_id column for boundary
  │        ├─ no boundary → process_sub_batch(full batch), loop
  │        └─ boundary at row N
  │             ├─ process_sub_batch(batch[0..N])
  │             ├─ finish_entity(state) → append to output buffer
  │             ├─ stash batch[N..] as pending
  │             ├─ create_state(new entity)
  │             └─ re-enter loop
  └─ output_buffer full → return concatenated batch
```

The adapter is a `PhysicalOperator` implementation wrapping a child `Box<dyn PhysicalOperator>` input and an owned `EntityOperator`. The cancellation token is checked between iterations, not inside the inner `process_sub_batch` loop.

Several concerns that live inside the adapter — the `output_buffer` amortization of one-row-per-entity emits, `target_output_rows` tuning, interaction with fused accumulators — are adapter-implementation concerns, not trait concerns, and belong to the task that lands the adapter.

## 7. Wave 1 deferrals

These are intentionally out of scope for TASK-108. Later tasks can add them without breaking existing implementors:

1. **`DemandCapabilities` / `supported_demands()`** — TASK-110 introduced the scaffold; TASK-427 replaced the placeholder enum with the real `DemandCapabilities` struct (7 bool fields) in `bqlite-planner::demand`. The `EntityOperator::supported_demands()` method remains a defaulted extension. See `docs/design/planner/demand-protocol.md` for the full protocol.
2. **`finish_entity_into` + `Accumulator` trait** — aggregation fusion is introduced in a later wave. The fused method is defaulted to fall back on `finish_entity`, so existing operators need no change.
3. **`Metrics` hook** — TASK-112 introduces per-operator metric counters (rows in/out, bytes, wall time). These are woven through a separate `Metrics` trait, not baked into the operator interface; operators collect their own counters and publish through the metrics surface.
4. **`QueryWarning` channel** — non-fatal warnings (entity event limit, active-state cap) are collected in a `ShardTaskContext` that the engine owns. Wave 1 operator stubs have no warnings to emit.
5. **`EntityOperatorAdapter` implementation** — the trait is frozen here; the adapter lands in the first wave that ships an `EntityOperator` implementor.
6. **Full `QueryContext`** — the engine-level orchestration wrapper holding the cancellation token, timeout timer, memory budget, and metrics is a `bqlite-engine` concern. Wave 1 operators see the pieces individually.

## 8. Open questions

None blocking Wave 1. The Wave 1 trait surface is small enough that every open question is already forward-referenced to a later task:

- Demand propagation protocol: TASK-110 (scaffold) → Wave 4 `[DESIGN]` task (real protocol).
- Fused aggregation path: later wave when the `Accumulator` trait lands.
- Metrics surface: TASK-112.
- Adapter implementation: first task that ships an `EntityOperator` implementor.
