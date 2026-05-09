# Morsel Scheduler & Query/Compaction Capacity Sharing

**Wave**: 5
**Task**: TASK-506
**Status**: draft
**Depends on**: execution-model.md (§3.3, §3.4, §3.5, §4.x, §7, §9, §10, §11, §14), storage/compaction-concurrency.md (§3, §4), engine/operator-fusion.md (§4)
**Depended on by**: TASK-523 (morsel scheduler + partial-aggregate handoff), TASK-524 (CPU/skew metrics + `--explain-perf` surface), TASK-525 (memory-pressure / spill / cancellation stress suite), TASK-526 (Wave 5 benchmark gate)

---

## 1. Purpose

`docs/design/execution-model.md` §9 documents the steady-state parallelism model — shards are the correctness boundary, morsels are the execution boundary, workers are the scheduling boundary. §14 documents the metrics rows that make that scheduler observable. Both are *target* designs: the engine today (`crates/bqlite-engine/src/query.rs`) drives every query single-threaded on the calling thread, with no morsel concept, no worker pool, no per-shard partial accumulator, and no integration with the compaction `CoreBudget` semaphore that `docs/design/storage/compaction-concurrency.md` §4 already ships.

This document is the implementation contract that flips §9 and §14 from "documented target" to "must-build". It pins down:

- The morsel generation algorithm, boundary rules, and target size policy (§3).
- The lock-free per-query MPMC morsel queue, consumer-side worker dispatch rule, and lazy generation invariant (§4).
- The engine query queue, worker pool sizing, and concurrent-query semantics (§5).
- Per-shard partial-aggregate ownership, worker→shard handoff, and coordinator merge (§6).
- The query/compaction capacity-sharing protocol — query-side `CoreBudget` permit acquisition that §4 of compaction-concurrency.md left as TASK-438/Wave 5 work (§7).
- The exact metrics the runtime must expose for §14's skew / parallelism / compaction-interaction rows, plus the sampling protocol that makes them cheap (§8).
- Cancellation, panic isolation, and timeout interaction at the morsel boundary (§9, forward-references the TASK-505 cancellation contract).
- Test bar, benchmark coverage, and follow-on `[IMPL]` tasks (§10, §11).

It does **not** cover:

- The memory tracker reservation/release contract — that is TASK-501. This document only specifies the points at which the morsel scheduler talks to it.
- The spill protocol — TASK-502. Spill cleanup ordering at morsel/query boundaries is forward-referenced.
- Cancellation/timeout/warning typing and latency bounds — TASK-505. This document specifies *where* checks happen (between morsels, between batches inside a morsel, between sub-batches inside `EntityOperatorAdapter`); the *typed errors* and the *cleanup ordering* are TASK-505's job.
- The optimizer rule that decides which queries become parallel-eligible — Wave 5 queries are unconditionally parallel-eligible at this layer; the planner does not gate this. (DDL and metadata queries bypass the morsel scheduler entirely; see §5.4.)

---

## 2. Relationship to Existing Design Docs

This document refines `execution-model.md` §9 and §14 with implementation-level detail. Where it appears to conflict with §9/§14, the conflict is intentional and `execution-model.md` should be updated in the same checkpoint as the corresponding `[IMPL]` task that lands the change. Specifically:

| execution-model.md surface              | This document    | Change                                                                                       |
| --------------------------------------- | ---------------- | -------------------------------------------------------------------------------------------- |
| §9.1 "work-stealing scheduler"          | §4.1, §4.2       | Replaces "work-stealing" with **centralized lock-free dispatch + consumer-side load balancing** — one MPMC queue per query, workers pull on idle. The two are operationally similar at morsel granularity but have different contention models; see §4.1. |
| §9.4 "lazy MPMC queue per `(query, shard)`" | §4.1, §4.2   | Reframed as one MPMC queue *per query*, with one generator per shard pushing into it. (§9.4 already supports this reading; this document fixes the wording.) |
| §9.5 "thread-local accumulator owned by the morsel generator's `(query, shard)` context" | §6.2, §6.3 | The "thread-local" phrasing in §9.5 is a relic and contradicts the same paragraph's "one per shard" + "per-shard mutex on its accumulator handle" wording. This document pins the canonical answer: one `Mutex<Box<dyn Accumulator>>` per shard per query, owned by the coordinator, mutated by every worker that pulls a morsel for that shard. The §9.5 thread-local phrasing is fixed in the TASK-523 reconciliation (§11.1). |
| §11.1 "active query threads" check      | §7.1, §7.2       | Replaces the active-count phrasing with the `CoreBudget` semaphore protocol from compaction-concurrency.md §4. |
| §14.1 skew rows ("worker_idle_ns_*", "morsels_per_shard_*") | §8.2 | Specifies sampling, units, and per-worker collection points. |

Where this document and `execution-model.md` conflict, this document wins for Wave 5 implementation; `execution-model.md` is the conceptual reference and is updated in the TASK-523 / TASK-524 checkpoints.

This document also forward-references three sibling Wave 5 [DESIGN] tasks:

- **TASK-501** (`docs/design/engine/memory-budget.md`) — query/per-worker budget split, reservation/release contract.
- **TASK-502** (`docs/design/engine/spill.md`) — what spills, where, and how cleanup interacts with cancellation.
- **TASK-505** (`docs/design/engine/cancellation.md`) — typed error mapping, panic isolation, latency bounds, cleanup ordering.

These docs are sibling tasks landing in the same wave; this document defers to them on the surfaces it points at. Where a forward-reference resolves to "TBD" because the sibling doc has not yet landed, this document marks the gap explicitly.

---

## 3. Morsel Generation

### 3.1 Definition

A **morsel** is a half-open `[entity_lo, entity_hi)` range over the entity-id sort order *within a single shard*, bundled with the shard's segment inventory for that range. Concretely:

```rust
pub struct Morsel {
    /// The shard this morsel belongs to. All entities in the morsel
    /// satisfy `xxhash64(entity_id) % num_shards == shard_id`.
    pub shard_id: ShardId,
    /// Entity-id range, half-open.
    pub entity_lo: EntityId,
    pub entity_hi: EntityId,
    /// The per-window segment lists for this shard, restricted to
    /// segments whose `entity_range` overlaps `[entity_lo, entity_hi)`.
    /// Each entry is one window's contributing segments; the worker
    /// k-way merges across windows when it processes the morsel.
    pub segments: SmallVec<[WindowSegments; 4]>,
    /// Approximate row count for budget metrics. Computed from
    /// segment row counts; not guaranteed exact post-tombstone.
    pub estimated_rows: u64,
    /// True iff this is the last morsel emitted by the morsel
    /// generator for this shard. The accumulator-handle drop path
    /// uses this flag to signal shard-done to the coordinator (§4.3).
    pub is_shard_final: bool,
}

pub struct WindowSegments {
    pub window_id: WindowId,
    /// Reference to the segments contributing to this morsel's
    /// entity range. `Arc<[SegmentRef]>` keeps the descriptor cheap
    /// to clone across morsels and avoids materializing per-segment
    /// metadata into the morsel — segment files are not opened, zone
    /// maps are not decoded; pruning already happened at plan time.
    pub segments: Arc<[SegmentRef]>,
}
```

The morsel is the unit of work handed to a worker. Workers run the full pipeline (scan → fused stateless segment → entity operator → partial aggregate handoff) on the morsel and produce nothing externally except updates to the shard's per-shard accumulator and per-worker metrics.

### 3.2 Generator Per Shard

For each query, the engine instantiates one `MorselGenerator` per shard the query touches:

```rust
pub struct MorselGenerator {
    shard_id: ShardId,
    /// Per-window segment iterators, after zone-map / tombstone /
    /// predicate pruning at query plan time.
    window_cursors: SmallVec<[WindowCursor; 4]>,
    /// Target row count for the next morsel; recomputed every morsel
    /// from the current load signal (§3.4).
    target_rows: u64,
    /// Last entity boundary seen by the generator. Always equal to
    /// `entity_hi` of the previous morsel; the next morsel begins here.
    next_entity_lo: EntityId,
    /// Generated count (for metrics).
    morsels_emitted: u64,
}
```

The generator advances by walking the shard's k-way merged stream of segment row-group descriptors, accumulating row count until it reaches `target_rows`, then snapping `entity_hi` to the next entity boundary. The cursor is purely metadata-driven — it never decodes column data, never opens segment files, and never reads the storage `SegmentReader` interface. It only consults segment metadata (entity range, row count, zone maps) from the shard's manifest snapshot.

**Why a metadata-only generator:** Morsel generation must be cheap so it can run lazily on the coordinator thread without contending with workers for I/O. Decoding even one column to determine boundaries would add millisecond-scale latency per morsel. The shard's manifest already knows entity ranges per segment, which is enough.

### 3.3 Boundary Rules

Three invariants govern morsel boundaries:

1. **Entity-aligned.** `entity_lo` and `entity_hi` must each fall on an entity boundary — the row immediately before `entity_lo` and the row at `entity_hi - 1` must belong to different entities (or `entity_lo` / `entity_hi` is the shard's first / last entity). This is the §9.3 single-entity invariant: every entity is fully processed by exactly one worker on exactly one morsel.

2. **Half-open contiguity.** The next morsel's `entity_lo` equals the previous morsel's `entity_hi`. Every entity in the shard is covered by exactly one morsel. There are no gaps and no overlaps.

3. **Last morsel.** The final morsel for a shard sets `is_shard_final = true` on the morsel and `entity_hi` to the largest `EntityId` in the shard (inclusive of the last entity, with the half-open contract enforced by the worker treating "no more rows after this entity" as the natural end). The flag is what the coordinator uses to decide that all of a shard's worker handoffs are done and the shard's accumulator can be merged into the cross-shard reduction (§6.4). No synthetic "max+1" sentinel `EntityId` is required — `EntityId` is opaque (could be a hash or a user string in future surfaces) and arithmetic on it is not defined.

**Why half-open over closed:** Closed ranges are ambiguous when an entity straddles a window boundary (it does not — entities live in one shard — but the half-open convention is the standard for range-based work units and it makes the "next morsel begins at previous `entity_hi`" rule arithmetic instead of conditional).

**Why a flag for the last morsel rather than an `EntityId` sentinel:** `EntityId` is opaque — a 64-bit hash today, possibly a string or other type in future surfaces — so "max + 1" is not portable. Storing the flag on the morsel keeps the worker's hot path branch-free and decouples the bookkeeping from the `EntityId` representation.

### 3.4 Target Size Policy

`execution-model.md` §9.2 states the target morsel size is **~64 row-groups (≈4M rows)** at the high end and **single-row-group** at the low end. This document fixes the policy:

```rust
pub struct MorselSizePolicy {
    /// High-water target row count. Default: 4_000_000.
    pub high_target_rows: u64,
    /// Low-water target row count (one row group's worth). Default: 65_536.
    pub low_target_rows: u64,
    /// Adaptive halving threshold: if the *running query-wide*
    /// `worker_idle_ns_p99` exceeds this value at a halving-decision
    /// point, the query-wide target row count halves for every
    /// shard's subsequent morsels, down to `low_target_rows`.
    /// Default: 5 ms.
    pub halve_idle_threshold_ns: u64,
    /// Minimum gap (in morsels emitted across all shards) between
    /// consecutive halving decisions. The first decision happens
    /// after the first `halving_warmup_morsels` morsels; subsequent
    /// decisions happen every `halving_warmup_morsels` morsels
    /// thereafter, until the target reaches `low_target_rows`.
    /// Default: 4 × num_workers.
    pub halving_warmup_morsels: u64,
}

/// Per-query, runtime sticky-halving state. Owned by the query
/// coordinator and shared across every `MorselGenerator` of the
/// query via `Arc<MorselSizeState>`. Each generator reads
/// `current_target_rows` at the start of every morsel emission;
/// the coordinator's drain pump is the only writer.
pub struct MorselSizeState {
    /// Current row-count target for the next morsel from any shard.
    /// Monotonically non-increasing across the query's lifetime
    /// (sticky halving — see "Why sticky halving" below).
    pub current_target_rows: AtomicU64,
    /// Total morsels emitted across all shards. Used to gate the
    /// next halving check at every multiple of `halving_warmup_morsels`.
    pub morsels_emitted_total: AtomicU64,
}
```

The default `current_target_rows` is `high_target_rows`. The signal is **query-wide** `worker_idle_ns_p99`, not per-shard, and the resulting halving decision applies to **every** shard's generator (because the state is shared via `Arc<MorselSizeState>`). The decision is made by the coordinator's drain pump after the first `halving_warmup_morsels` morsels and at every additional `halving_warmup_morsels` morsels thereafter. Halving is **multi-step**: each decision either keeps the current target or halves it (full → half → quarter → … → `low_target_rows`), and the target never grows back. Once `current_target_rows == low_target_rows`, halving stops being checked.

**Why query-wide signal:** Per-shard idle attribution would require splitting the worker pool's idle time across shards by sampling the worker's currently-bound shard at idle moments. That is implementable but adds atomic accounting per worker per pop. The query-wide signal is one DDSketch read on the coordinator's running `QueryMetrics::worker_idle_ns` sketch — that sketch is already updated as workers complete morsels and merge their per-worker sketches into `Mutex<QueryMetrics>` at guard-drop (§8.3). The coordinator's halving check reads the running merged sketch's p99 under the same `Mutex<QueryMetrics>` lock that workers use to merge — one extra sub-microsecond critical section every `halving_warmup_morsels` morsels. It captures the same property — "the query is starving for morsels" — without per-shard noise. In the (unusual) case where one shard's morsels are huge and another shard's morsels are tiny, the query-wide signal still triggers halving on the huge-morsel shard, which is the one we want to halve.

**Why sticky halving (no grow-back):** Idle pressure is itself a noisy signal — workers may briefly idle between cancellation ticks, around accumulator-merge moments, or when a single huge morsel is in flight. Allowing halving to reverse direction creates an oscillation risk where target size flaps between halved and full as transient idle blips come and go. Sticky halving accepts that the worst-case morsel size for a query is whatever halving decided early, which is upper-bounded by `high_target_rows` and lower-bounded by `low_target_rows`. The cost is minor: a query that halves spuriously processes morsels at half size for the rest of its life, which adds at most 2× the per-morsel scheduling overhead — measured in microseconds, not visible in workloads where the overhead is dominated by morsel processing cost.

**Why adaptive halving over fixed size:** Behavioral data is power-law distributed; a query over a shard with one outlier 100M-event entity should not see that entity served as a single 100M-row morsel that monopolizes a worker. The generator does not know entity size *a priori*, but the scheduler observes idle time as workers finish small morsels and wait for the generator to produce more. Halving on idle pressure is the simplest control loop that captures this without a cost model.

**Why not split below `low_target_rows`:** A morsel smaller than one row group cannot possibly amortize the per-morsel overhead (k-way merge setup, scan operator open, accumulator handle acquisition). Below this threshold the morsel queue is no longer the right level of parallelism — that is a query-level bottleneck the scheduler cannot fix.

**What about a single huge entity in one shard?** If the shard contains one entity whose event count alone exceeds `high_target_rows`, the morsel generator still emits exactly one morsel for that entity. The single-entity invariant (§3.3) wins over the size target. Inside the morsel, the worker's `EntityOperatorAdapter` (§4.1 of operator-fusion.md, execution-model.md §4.1) sub-batches the entity to keep memory bounded. The metric `entity_event_skew_p99` (§8.2) surfaces this case so operators can see when a query is bottlenecked on a single entity.

### 3.5 Lazy Generation

Morsel generators do not eagerly enumerate all morsels at query start. Instead:

- The coordinator pushes the *next* morsel from each shard's generator onto the query's morsel queue when the queue's depth drops below `2 × num_workers`.
- A "drain-pump" thread (the coordinator itself, time-sliced) services pull requests from workers via `try_pop` and refills from the per-shard generators in priority order (§4.2).

**Queue depth bound:** `2 × num_workers` morsels in flight at any time. This is enough to keep workers fed across the latency of one generator step (typically µs — pure metadata work) and bounds the in-flight memory: at most `2 × num_workers` morsel descriptors plus the segment-list metadata each carries.

**Why not eager:** Eager generation forces the coordinator to materialize every morsel descriptor before workers start, which adds startup latency proportional to the shard inventory and pins memory for descriptors that won't run for seconds. Lazy generation amortizes the work over the query lifetime and bounds in-flight memory by the worker pool's throughput, not by the shard count.

**Why not lock-free generator:** The generator's per-step work is microseconds; making it lock-free would gain nothing. It runs on the coordinator thread; workers never touch it.

### 3.6 Empty Shards

If a shard has zero segments after pruning (zone maps eliminated everything, or the table is empty for that shard), its `MorselGenerator` emits zero morsels and immediately marks itself drained. The shard's accumulator is initialized to "empty" and merged into the cross-shard reduction as a no-op. This keeps the shard count constant in the coordinator's bookkeeping regardless of pruning effectiveness.

**Why not skip the empty shard entirely:** Stable bookkeeping. Metrics like `morsels_per_shard_min` need the empty shard's zero count to surface "this shard contributed nothing", which is a useful signal for operators tuning shard counts.

---

## 4. Morsel Queue and Worker Dispatch

### 4.1 One Lock-Free MPMC Queue Per Query

Each query has exactly one morsel queue, shared across all of its shards' generators (producers) and all workers participating in the query (consumers).

```rust
pub struct MorselQueue {
    /// Lock-free MPMC queue. Capacity is `2 × num_workers` slots
    /// (§3.5); pushes return `Err(Full)` when the queue is full and
    /// the coordinator's drain pump retries on the next worker-pop
    /// notification. This is the natural backpressure on lazy
    /// generation — the coordinator never blocks while holding any
    /// other lock, so the wake path is deadlock-free.
    queue: crossbeam::queue::ArrayQueue<Morsel>,
    /// Coordinator-side wake notification: workers signal this
    /// condvar after every successful `pop()` so the drain pump
    /// can retry a previously-failed `push`. Cheap — one notify
    /// per morsel processed, not per batch.
    pop_notify: Condvar,
    /// Worker-side wake notification: the coordinator signals this
    /// condvar after every successful `push()` so any parked
    /// worker can re-attempt `take_next_query_morsel` (§4.2). Also
    /// signaled when `all_generators_drained` flips, so parked
    /// workers wake to observe the terminal state.
    push_notify: Condvar,
    /// Set when every per-shard generator is drained. Workers
    /// returning from `try_pop` empty stop pulling once this is set.
    all_generators_drained: AtomicBool,
}
```

**Producer protocol (coordinator drain pump).** The drain pump's loop is:

```text
loop:
    if queue.len() >= capacity:
        wait on pop_notify (with timeout, so cancellation can break it)
        continue
    pull next morsel from per-shard generator round-robin
    if no shard has any more morsels:
        set all_generators_drained = true
        break
    if queue.push(morsel) is Err(Full):
        // racing with another producer is impossible (single-producer);
        // a Full result here means the workers consumed slower than the
        // generator produced. Wait on pop_notify and retry.
        wait on pop_notify
        retry the push (the morsel is held in a local slot until the
        push succeeds, so we don't drop morsels)
```

There is one drain pump per query — the coordinator thread services every shard's generator for that query in round-robin. Single-producer ⇒ no `Err(Full)`-after-fix race. Workers ⇒ pure consumers; they never push.

**Why `crossbeam::ArrayQueue` rather than `crossbeam::channel::bounded`:** `ArrayQueue::push` is non-blocking and returns `Err` cleanly, which makes the producer's wait-then-retry path explicit. `channel::bounded(N).send()` blocks the producer until a receiver pops, which would force the coordinator to *be* a thread different from the one that wakes workers — adding a second control-thread for no benefit. The explicit condvar is the simpler shape for a single-producer drain pump.

The queue uses `crossbeam::queue::ArrayQueue` (or equivalent fixed-capacity MPMC structure). Workers acquire morsels via `queue.pop()`; the drain pump acquires via `queue.push()`. Both are lock-free.

**Why per-query, not per-shard:** A per-shard queue would force workers to commit to a shard before pulling, which re-introduces the shard-task bottleneck the morsel model exists to eliminate. With a single per-query queue, any worker can pick up morsels from any shard, which gives **consumer-side load balancing** without per-worker steal-deque infrastructure.

**Why a centralized queue rather than true work-stealing deques:** Per-worker deques with cross-worker stealing on idle (Rayon's join model, Tokio's task scheduler) are the right design when the unit of work is microsecond-scale and the queue itself is a contention point. Morsels are tens-of-milliseconds units; a single lock-free MPMC queue is not a contention bottleneck at that granularity. The shared queue also makes round-robin across queries (§4.2) trivial — every worker can see every active query's queue without coordinating per-worker deque visibility. We adopt the shared-queue design and accept that "work-stealing" in the §9.1 wording is descriptive of the load-balancing *behavior*, not the *mechanism*.

**Why not share a queue across queries:** The memory budget (TASK-501) is per-query, the cancellation token (TASK-505) is per-query, and metrics (§8) are per-query. A single per-query queue keeps every per-query resource bound to that query's lifecycle. Concurrent queries each have their own queue and feed the same worker pool through `WorkerHandle::take_next_query_morsel()` which round-robins across active queries.

### 4.2 Worker Acquisition Rule

When a worker is idle, it calls `WorkerHandle::take_next_query_morsel()`:

1. Round-robin across the engine's currently active query queues, starting from the worker's last-served query (rotated forward each pull).
2. For each query, attempt `queue.pop()` (non-blocking).
3. The first non-empty pop wins; the worker binds its `WorkerContext` (§6.1) to the popped morsel's `(query, shard)` and runs the morsel.
4. If every query's queue is empty *and* every query has set `all_generators_drained`, the worker parks on each active query's `MorselQueue::push_notify` condvar (§4.1) until a new morsel is pushed or `all_generators_drained` flips.

Round-robin with last-served rotation is intentionally simple — fairness across queries within "broad strokes", with no priority weighting. This is the same fairness model as the FIFO query queue (§5.1): the engine treats every query as equal-priority. Workload-shaping (priority lanes, query classes) is explicitly out of scope for v1.

**Why round-robin over priority queue:** Priority introduces tail-latency landmines (a long-running high-priority query starves background queries indefinitely). FIFO at submit time + round-robin at the worker level gives predictable progress for every running query, and the engine's overall throughput is the sum of throughputs — there is nothing to maximize globally that round-robin gives up.

**Why park on drain-with-empty-queues:** Workers should not spin-poll empty queues. Parking on a condvar costs microseconds per park/wake and makes idle-time CPU usage zero, which the metrics (§8.2 `worker_idle_ns_*`) measure correctly.

### 4.3 Per-Worker Pull Contract

`WorkerHandle::take_next_query_morsel()` returns a `WorkerMorselGuard`. The `'q` lifetime is the lifetime of the query's coordinator state — every reference inside the guard borrows from a query-scoped `Arc`-rooted graph, so the guard is `'static`-equivalent in the worker thread but is borrow-checked against the query coordinator at the engine level:

```rust
pub struct WorkerMorselGuard<'q> {
    pub morsel: Morsel,
    /// The per-shard accumulator handle for the morsel's shard.
    /// Workers call `accumulator.lock()` only at finish-entity boundaries
    /// for fused entity operators; never during sub-batch processing.
    pub accumulator: &'q Mutex<Box<dyn Accumulator>>,
    /// Per-worker context for this morsel (metrics, warnings, shard id).
    /// `WorkerContext` lifetime spans this guard.
    pub worker_ctx: WorkerContext<'q>,
    /// Drop hook: decrements `AccumulatorHandle::outstanding_morsels`
    /// and, if the decrement reaches zero AND the per-shard generator
    /// has already set `AccumulatorHandle::total_emitted` (i.e. no
    /// further morsels for this shard will be pushed), signals the
    /// coordinator that the shard is done. This check is independent
    /// of `morsel.is_shard_final` — the *flag* on the morsel is used
    /// only by the morsel generator's bookkeeping (§3.3); shard-done
    /// always derives from `outstanding == 0 && total_emitted.is_set()`
    /// so it remains correct under arbitrary worker pull ordering.
    _shard_done_hook: ShardDoneHook<'q>,
}

pub struct ShardDoneHook<'q> {
    handle: &'q AccumulatorHandle,
    coordinator: &'q QueryCoordinator,
    is_shard_final: bool,
}
```

In practice, `&'q ...` references are obtained by holding `Arc<QueryCoordinator>` and `Arc<AccumulatorHandle>` clones inside the guard — the coordinator hands those `Arc`s to the worker on `take_next_query_morsel()` and the worker drops them with the guard. The visible-to-callers shape is `'q`-borrowed; the implementation uses `Arc` to satisfy the `Send` requirement of the worker pool's task-passing path. The `'q` lifetime is shorthand for "lives until the query's coordinator finalizes" and is compile-time enforced by the engine's coordinator-owns-everything story.

The guard's drop logic decrements `AccumulatorHandle::outstanding_morsels` (§6.2) and, when the decrement reaches zero *and* `AccumulatorHandle::total_emitted` has been set by the generator, signals the coordinator that the shard's accumulator is ready for the cross-shard merge (§6.4). This pair of conditions is robust under any worker pull ordering — even if the morsel flagged `is_shard_final` happens to be processed before earlier morsels' guards drop, the shard is not signaled "done" until the last in-flight morsel's guard drops with `total_emitted` already set.

**Why the guard rather than explicit signaling:** RAII makes shard completion observable without each worker remembering to call `shard_done()` on every code path. Panic, error return, normal completion — all release the guard, all run the drop hook.

---

## 5. Engine-Level Query Queuing and Worker Pool

### 5.1 Worker Pool

The engine owns one fixed-size **`Rayon` thread pool** sized at `num_cores` workers (configurable via `EngineConfig::query_threads`). The pool is shared across queries and used only for query work — compaction has its own pool (compaction-concurrency.md §3.1). The two pools share the `CoreBudget` semaphore (§7).

**Why Rayon:** Existing, well-maintained, well-instrumented thread pool with stable scheduling primitives. The morsel queue lives one level above Rayon's join/scope abstraction — workers are long-lived and pull from the morsel queue in a custom loop, not Rayon-spawned tasks. Rayon is the thread-pool host, not the scheduler.

**Why not `tokio::spawn_blocking` or a custom pool:** Rayon's worker affinity, panic propagation, and configuration story are mature; we get all of that for free. `tokio` pulls in async-runtime infrastructure the engine has no other use for.

**Worker count default:** `query_threads = num_cores`, where `num_cores` is `std::thread::available_parallelism()`. On platforms where that returns `Err`, default to 4. The default may be overridden per-engine (`EngineConfig::query_threads`) but never per-query — every query uses the full pool, with morsel queue round-robin (§4.2) providing fairness across concurrent queries.

### 5.2 Query Queue (FIFO)

Queries arrive at the engine via `Engine::query(text, db)` (single text-in, rows-out surface — see crate map). The submission flow:

1. Parse, plan, and bind. (Single-threaded; happens on the caller's thread.)
2. Build the query's `QueryContext` (cancellation, timeout, memory tracker — TASK-501/505).
3. Build the per-shard `MorselGenerator`s and the `MorselQueue`.
4. Acquire `query_threads` permits from `CoreBudget` (§7).
5. Add the query to the engine's active-queries list; signal the coordinator that a new query is available.
6. Block the calling thread on `query_done_signal.wait()` until the coordinator marks the query complete.
7. Collect the per-shard accumulators' final merged results, return to the caller.

Queries that arrive while `CoreBudget` has fewer than `query_threads` permits available block at step 4 — that is the FIFO query queue. Permit acquisition is FIFO via the semaphore's wait queue (compaction-concurrency.md §4.1).

**Why FIFO at the permit gate, not the morsel gate:** "Queries do not preempt each other mid-morsel" (execution-model.md §9.4). A query that has acquired its permits owns them for the query's lifetime; it will not be paused once it starts. Compaction interleaving happens at the *compaction* level by yielding compaction permits to queued queries (§7.2).

**Why permit-count == `query_threads`, not 1:** A query that acquires only one permit and then dispatches morsels to all `num_cores` workers would let the worker pool do work without holding the permits, which defeats the point of the semaphore — compaction would then race query workers for cores even though the query has all `num_cores` of them busy. Acquiring `query_threads` permits up front matches what the query actually consumes.

### 5.3 Concurrent Queries

If `2 × query_threads` permits are configured (the engine semaphore is the same `CoreBudget` whose total is `num_cores`, so this is implicitly capped at `num_cores` permits), at most one full-width query runs at a time on a `num_cores == query_threads` deployment. Two concurrent queries can run only when each acquires fewer permits than the full pool — but v1 does not support fractional acquisition. The result: **on a saturated worker pool, queries are serial; on an under-saturated pool (compaction quiet, only one query active), the second query queues behind the first.**

This is the same model `execution-model.md` §9.4 specifies. It is intentionally simple. Concurrent multi-query throughput optimization is a v2 concern; the design prioritizes per-query latency.

**Why serialize rather than time-slice:** Time-sliced queries thrash CPU caches, fight over memory budget allocations, and generally hurt both queries' tail latencies. The behavioral-analytics use case is interactive — a single dashboard query running at full pool width and finishing in 100ms is strictly better than two queries each finishing in 250ms.

### 5.4 DDL and Metadata Bypass

DDL (CREATE TABLE, etc.) and metadata queries (SHOW TABLES, EXPLAIN) bypass the morsel scheduler entirely. They run on the calling thread, never acquire `CoreBudget` permits, and never touch the worker pool. They return synchronously from `Engine::query()`.

**Why bypass:** These queries do not produce row data from segments — they manipulate the manifest (DDL) or read in-memory state (EXPLAIN). They produce single-row results in microseconds; the scheduler infrastructure adds overhead with zero parallelism benefit.

---

## 6. Partial Aggregation Ownership

### 6.1 WorkerContext

```rust
pub struct WorkerContext<'a> {
    /// Shared cross-worker state.
    pub query: Arc<QueryContext>,
    /// Identity of the shard whose morsels this context is currently
    /// processing. Re-bound at every morsel pull.
    pub shard_id: ShardId,
    /// Per-worker, per-morsel metrics; merged into the query's totals
    /// when the worker pool drains for this query.
    pub metrics: QueryMetrics,
    /// Per-worker warnings (TASK-511; bounded by the per-worker cap
    /// before the cross-worker concatenation defined by TASK-505).
    pub warnings: Vec<QueryWarning>,
    /// Reference to the per-shard accumulator handle the worker locks
    /// at fused-entity-operator finish boundaries.
    pub accumulator: &'a Mutex<Box<dyn Accumulator>>,
}
```

`WorkerContext` is created fresh inside each `WorkerMorselGuard` (§4.3) and never crosses a thread boundary except through the guard. After the morsel finishes, the worker's metrics increments are summed into the query's totals atomically (one summation per morsel completion, not per batch — see §8.3 collection protocol).

**Why per-morsel rather than per-worker-lifetime:** Workers process morsels from many `(query, shard)` pairs in their lifetime; binding the context to the morsel makes the shard identity obvious in every metric increment and makes the accumulator handoff explicit at morsel boundaries.

### 6.2 Per-Shard Accumulator Handle

Each query maintains one `AccumulatorHandle` per shard:

```rust
pub struct AccumulatorHandle {
    /// The per-shard accumulator. Created once at query start;
    /// mutated by every worker that processes a morsel in this shard.
    inner: Mutex<Box<dyn Accumulator>>,
    /// Number of morsels still in flight for this shard. Decremented
    /// by `WorkerMorselGuard::drop` (§4.3).
    outstanding_morsels: AtomicU64,
    /// Total morsels emitted by this shard's generator. Set when the
    /// generator drains. The shard is "done" when
    /// `outstanding_morsels == 0 && total_emitted_set`.
    total_emitted: OnceCell<u64>,
}
```

Workers acquire the mutex only at fused-entity-operator finish boundaries (per-entity, not per-event) when calling `EntityOperator::finish_entity_into(&mut accumulator, ...)`. For non-fused queries (plain `AggregatePhysical` without entity-operator fusion), workers acquire the mutex once per `update_batch` call — execution-model.md §9.5 already specifies this protocol.

**Why a mutex, not lock-free:** Multiple workers *can* hold morsels for the same shard concurrently (the §3.3 single-entity invariant says no entity is split across workers — it does *not* say that concurrent morsels of the same shard cannot coexist; a shard with N morsels will see up to `min(N, num_workers)` workers contend on its accumulator mutex). Contention is per-`finish_entity`, which fires once per entity, not once per row. At default 4M-row morsels with ~10K entities per morsel, the lock is acquired on the order of 10K times per morsel for ~microsecond hold times — that is one lock acquisition every few microseconds across all workers, well below the noise floor of contended cache-coherence. A lock-free hash accumulator would add substantial complexity (hazard pointers, custom allocator, retry loops on update) without measurable benefit; we ship the mutex and revisit if `worker_busy_ns_*` skew (§8.2) ever attributes time to lock acquisition.

**Why one mutex per shard, not one per `(shard, group_key)`:** Group-key-grained locking would split contention across keys, but the cost is one lock per group (potentially 1M locks for the default `max_groups`). The per-shard mutex bounds memory at `num_shards` locks regardless of group cardinality.

### 6.3 Worker → Shard Handoff

When a worker pulls a morsel for shard *S*:

1. The morsel guard captures `&accumulator_handles[S].inner` (§4.3).
2. The worker runs the pipeline. For each entity, the entity operator processes sub-batches. On `finish_entity()`, it calls `finish_entity_into(&mut *acc.lock())` — the mutex is held only for the duration of the per-entity merge.
3. On morsel completion, the guard's drop hook decrements `outstanding_morsels`.
4. The generator sets `total_emitted` when it produces its last morsel for the shard (the one with `is_shard_final = true`).
5. If after the decrement `outstanding_morsels == 0` and `total_emitted` is set, the coordinator is signaled that shard *S*'s accumulator is final. Either ordering of "last morsel decrement" and "generator sets `total_emitted`" works — the test is conjunctive, and both writes are observable to the coordinator's check via the same `Mutex<QueryCoordinatorState>`.

**Per-entity lock granularity:** Acquiring the mutex once per entity (not once per batch and not once per row) trades a tiny amount of contention for a clean ownership story. The alternative — per-worker thread-local accumulators that merge at morsel-drop time — re-introduces the "one accumulator per morsel" cost the §9.5 spec explicitly rejects ("`num_shards × morsels_per_shard` merges instead of `num_shards`").

### 6.4 Coordinator Cross-Shard Merge

When all shards have signaled "done", the coordinator performs the cross-shard reduction:

1. For each shard *S*, take ownership of `accumulator_handles[S].inner` (no concurrent workers possible — every morsel is drained).
2. Pairwise-merge accumulators: `final = shards[0]; for i in 1..N: final.merge(shards[i])`.
3. Call `final.finish()` to materialize the result `RecordBatch`.
4. Concatenate non-aggregated rows across shards (selection queries) and optionally k-way merge-sort for `ORDER BY` (execution-model.md §7.2).
5. Hand the result to the caller's blocked thread (§5.2 step 6).

The merge is single-threaded on the coordinator thread. With `num_shards = 32` and accumulator merge at constant time per merge (DDSketch, hash-set union, scalar sum), total merge time is O(N × accumulator_size) = O(32 × ~3 MB) ≈ tens of milliseconds even at the 1M-group cap.

**Why single-threaded merge:** The cross-shard merge runs once per query; parallelizing it shaves a few ms at most, while serializing it makes the merge code trivially correct (no merge tree, no balanced reduction concerns).

**Why pairwise sequential:** Each `Accumulator::merge` is in-place; pairwise is the natural shape. A balanced tree (parallel merge across `log N` levels) is a v2 candidate if shard counts grow large.

**Memory peak during merge.** All `num_shards` accumulators are resident simultaneously when the coordinator starts the pairwise reduction — the post-merge structure absorbs each peer in turn but the input peers are not freed until merged. At default `num_shards = 32` and ~3 MB per accumulator (1M-group cap, ~100 bytes per group), the merge peak is ~96 MB, which the memory tracker (TASK-501) must account for as a coordinator-side reservation. The morsel-scheduler implementer must reserve this peak via `MemoryTracker::try_reserve` before initiating the merge. Out-of-budget at merge time fails the query with `MemoryBudgetExceeded`; spill at merge time is not in scope (TASK-502 explicitly defers aggregation spill to v2).

---

## 7. Query/Compaction Capacity Sharing

### 7.1 Shared Semaphore: `CoreBudget`

The engine and the compaction scheduler share one `CoreBudget` semaphore initialized with `num_cores` permits at engine startup. This is the same semaphore specified by `docs/design/storage/compaction-concurrency.md` §4 and already implemented in `crates/bqlite-storage/src/compaction.rs` (CoreBudget + CoreBudgetPermit RAII guard). This document specifies the **query-side acquisition** that compaction-concurrency.md §12 ("SS4 query-side permit acquisition is TASK-438's job") left as a forward reference.

The TASK-523 implementer extends `CoreBudget` with an atomic batch acquisition primitive:

```rust
impl CoreBudget {
    /// Acquire `n` permits atomically — either all are granted
    /// or the caller blocks until all `n` are simultaneously
    /// available. Required by the engine's query-start path to
    /// avoid the partial-acquisition deadlock between concurrent
    /// queries (see "Why atomic acquire_n" below).
    pub fn acquire_n(&self, n: usize) -> CoreBudgetPermitBatch;
}
```

`CoreBudgetPermitBatch` is an RAII guard that drops `n` permits at once. The implementation is the standard "wait on condvar until count >= n, then subtract n" loop under the existing `Mutex<usize> + Condvar` pair (compaction-concurrency.md §4 already specifies this primitive); no fundamental change to the semaphore's internal data structure.

```rust
// In bqlite-engine, at query start (§5.2 step 4):
let permits = core_budget.acquire_n(query_threads);
// permits live for the lifetime of the query, dropped at finalization;
// dropping releases all `query_threads` permits in one atomic step.
```

**Why atomic `acquire_n` rather than a loop of `acquire()`:** A naive `for _ in 0..query_threads { core_budget.acquire() }` loop deadlocks under concurrent submission. Imagine two queries A and B both racing to acquire 16 permits on a 16-core machine: A grabs 8, B grabs 8, both block forever waiting for permit #9. Atomic batch acquisition guarantees that a query only holds permits when it can hold *all* of them, so a loser of the race holds zero permits while it waits — no deadlock cycle is possible.

**Why acquire all `query_threads` permits up front:** A query that acquires permits one at a time (per worker per morsel) would interleave with compaction at the worker granularity, which adds 32 acquire/release cycles per morsel and exposes the query to mid-execution compaction starvation. Up-front acquisition matches §5.2's "queries do not preempt each other mid-morsel" rule and gives compaction a stable signal — "this many cores are owned by the query right now."

**FIFO at the batch-acquire wait queue:** `acquire_n` enters a FIFO wait queue inside the semaphore. When permits become available, they are granted to the longest-waiting batch first; if the head waiter wants more permits than are currently available, no later waiter can jump ahead, even if the later waiter wants fewer. This is **head-of-line FIFO**, which slightly under-utilizes permits when a small batch could fit while a large batch waits — but it is the simplest fairness story and matches compaction-concurrency.md §4.1's FIFO-on-permits framing. A "shortest-job-first" or work-conserving variant is a v2 candidate.

**What if `query_threads > num_cores`?** Configuration error; the engine rejects it at `EngineConfig::validate()`. `query_threads <= num_cores` is invariant. With this invariant, `acquire_n(query_threads)` always *eventually* succeeds — the only contention is with other queries (each capped at `query_threads ≤ num_cores`) and compaction (which releases per row group).

**What about concurrent queries:** As specified in §5.3, a second query that arrives while the first holds all permits blocks at the semaphore until the first releases. This is identical to compaction's pause-at-row-group-boundary protocol from compaction-concurrency.md §4.2; both subsystems pause on the same semaphore.

### 7.2 Compaction Yields to Queries (Already Specified)

This direction is already specified by compaction-concurrency.md §4:

- Compaction workers acquire one permit per row-group, release at row-group boundary.
- When a query acquires `query_threads` permits, the next compaction `acquire()` blocks until a query permit is released (i.e., until the query finishes).
- Compaction's `compact_now` synchronous API bypasses the semaphore (compaction-concurrency.md §3.3), but never causes worker oversubscription because it runs on the caller's thread, not on the compaction worker pool.

**No new logic required from the engine side beyond the §7.1 acquisition.** The semaphore enforces the `active_compaction_threads ≤ num_cores - active_query_threads` invariant naturally.

### 7.3 No Mid-Query Compaction Pause Negotiation

If query load suddenly increases mid-execution, the running query keeps its permits — compaction's pause is the *only* yield mechanism. The engine does not signal compaction "please pause more aggressively" or revoke permits. This keeps the protocol symmetric with §7.2 and avoids livelock scenarios where compaction is repeatedly preempted just before completing a row group.

**Why no preemption:** Once a worker pool starts a morsel, that morsel must run to completion (§9.3 single-entity invariant). The grain at which we can yield is one morsel — typically tens of milliseconds. Compaction yields at one row group — typically also tens of milliseconds. Both grains are short enough that we do not need a finer-grained negotiation.

### 7.4 Compaction-Active Metric

The engine exposes `compaction_active_ns` (per query, §8.2): wall-clock time during which any compaction worker held a `CoreBudget` permit while the query was also running. This is the single observability signal for "did compaction interfere with this query?" The query coordinator computes it by polling the compaction scheduler's "currently held permits" counter once per morsel boundary (cheap; `compaction_scheduler.permits_held()` returns an atomic load).

**Why poll at morsel boundary:** Polling more frequently adds atomic contention; polling less frequently misses short compactions. Morsel boundaries already happen tens of times per second per worker; that is the right resolution.

**Why this signal and not "compaction permits stolen from query":** The query, by §7.1, holds all `query_threads` permits for its lifetime — it cannot have a permit "stolen". The interference is the other way: queries arriving mid-compaction wait for compaction to release permits. `compaction_active_ns` captures the wall-time view of that interference and is the directly actionable signal for tuning the compaction backlog.

---

## 8. Metrics

### 8.1 What This Document Owns

`docs/design/execution-model.md` §14 enumerates the full per-query metrics surface. This document specifies:

- The Wave 5 skew/parallelism rows that depend on the morsel scheduler (§14.1 "Skew and parallelism metrics" — `morsels_dispatched`, `morsels_per_shard_max/min`, `worker_idle_ns_p50/p99`, `worker_busy_ns_max/min`, `entity_event_skew_p99`).
- The `compaction_active_ns` row from §14.1 "Compaction interaction metrics".
- The collection points (where in the worker / coordinator code each counter is incremented).
- The sampling protocol that bounds overhead.

Other rows (rows_scanned, bytes_scanned, segments_pruned, etc.) are owned by the operator/scan layer and are out of scope for this document.

### 8.2 Wave 5 Counters

| Metric                          | Where collected                                     | Type      | Notes                                                              |
| ------------------------------- | --------------------------------------------------- | --------- | ------------------------------------------------------------------ |
| `morsels_dispatched`            | Coordinator, on every successful `queue.push`       | u64 sum   | One increment per morsel.                                          |
| `morsels_per_shard_max`         | Coordinator, after all shards drained               | u64 max   | Computed from per-shard `MorselGenerator::morsels_emitted`.        |
| `morsels_per_shard_min`         | Coordinator, after all shards drained               | u64 min   | Includes empty shards (zero) — the empty-shard signal (§3.6).      |
| `worker_idle_ns_p50` / `_p99`   | Per-worker `WorkerContext::metrics` per pull        | DDSketch  | Time spent in `take_next_query_morsel`'s park-or-pop wait.         |
| `worker_busy_ns_max` / `_min`   | Per-worker `WorkerContext::metrics` per morsel      | u64 sum/per-worker; min/max across workers at finalize | Wall time from morsel pop to morsel guard drop. |
| `entity_event_skew_p99`         | Per-worker `WorkerContext::metrics` per entity      | DDSketch  | Updated on each `finish_entity` with that entity's event count.    |
| `compaction_active_ns`          | Coordinator, per morsel boundary poll               | u64 sum   | See §7.4.                                                          |

`worker_idle_ns_*` and `entity_event_skew_p99` use `sketches-ddsketch` (CLAUDE.md dependency conventions §1) for constant-memory percentiles. One sketch per worker for `worker_idle_ns`; one per worker for `entity_event_skew`. Sketches merge across workers at query finalization (constant-time merge).

**TASK-537 v1 simplifications.** Until sub-shard morsel halving raises the per-worker sample count into the hundreds, the engine collapses both DDSketch-based metrics into simpler shapes that produce comparable signal at zero new dependency cost:

- `worker_idle_ns_p50` / `_p99` are derived from the per-worker idle-time *totals* (one `u64` per worker) via the running min/max protocol in `QueryMetrics::record_worker` — `worker_idle_ns_p50` is the cross-worker minimum, `worker_idle_ns_p99` is the cross-worker maximum. With ≤ `num_cores` samples per query the running min/max is the right approximation; switch to DDSketch when the per-worker pull count grows past one digit.
- `entity_event_skew_p99` is reported at the worker as the p99-vs-p50 spread of *per-morsel processed-event counts*, not per-entity event counts. The per-entity sample lands when `EntityOperatorAdapter` exposes a `finish_entity` metrics hook; until then the per-morsel proxy carries the same "this worker saw a hot tail" signal at much lower wiring cost.
- `total_cpu_cycles` / `branch_misses` / `llc_misses` come from `perf_event_open` on Linux when the kernel honours the syscall (i.e. `CAP_PERFMON` is granted or `/proc/sys/kernel/perf_event_paranoid` permits it). On macOS and on a Linux box where the syscall is refused, the counters report zero and the CLI labels the rows as `not collected (no CAP_PERFMON)`.

### 8.3 Collection Protocol

- **Per-batch counters** (rows_scanned, bytes_scanned, etc.) are local `u64` increments inside the worker — no atomics, no contention. Owned by operators, not this document.
- **Per-morsel counters** (`worker_idle_ns_*`, `worker_busy_ns_*`) are sampled at morsel-pull time and morsel-drop time using `Instant::now()`. Two `Instant::now()` calls per morsel × `morsels_per_shard_max` morsels per query is a microsecond-scale total cost.
- **Per-entity counters** (`entity_event_skew_p99`) are sampled at `finish_entity` boundaries. The entity event count is already tracked by `EntityOperatorAdapter` for sub-batch streaming (execution-model.md §5); the metric is one DDSketch insert per entity, not per event.
- **Coordinator-side counters** (`morsels_dispatched`, `morsels_per_shard_*`, `compaction_active_ns`) are simple `u64` updates on the coordinator thread, no synchronization needed.

**Per-worker metric merging:** `WorkerContext::metrics` is per-worker, per-morsel. On guard drop, it is merged into the query's coordinator-owned `QueryMetrics` via a single `Mutex<QueryMetrics>` lock (one lock per morsel, ~tens to hundreds per query, microseconds-scale contention). The DDSketch fields use `DDSketch::merge_with(other)` which is constant-time per merge (sketches-ddsketch native operation).

**Why a mutex on `QueryMetrics`:** Same reasoning as §6.2 — contention is per-morsel, not per-batch, so a lock is cheap. The alternative (atomic-only counters + per-worker thread-local sketches merged at the end) saves the lock at the cost of duplicating the merge logic that DDSketch already provides.

### 8.4 Opt-In Sampled Metrics

CPU-cost metrics (`branch_misses`, `llc_misses`, `cycles_per_event`) are opt-in via `QueryContext::collect_cpu_metrics`, sampled once per morsel boundary as specified in `execution-model.md` §14.3. This document does not redefine the protocol; it observes that the morsel boundary is the natural sample point and that the worker's `WorkerMorselGuard` is the natural place to read perf counters into the metric.

**Latency budget for sampled metrics:** ≤1% per-batch overhead when enabled (per execution-model.md §14.3). The sample-once-per-morsel protocol enforces this — at default morsel size (~4M rows) the per-batch overhead of one perf-counter read is below the noise floor.

### 8.5 `--explain-perf` Surface

CLI surfacing of these metrics is TASK-524 scope. This document specifies that the metrics are *available* in `QueryMetrics` after every query and that the CLI / FFI may render any subset. The default (`bqlite query`) reports only `rows_returned`, `elapsed_ns`, and (if non-zero) `warnings`; the perf surface is opt-in to keep the default output terse.

---

## 9. Cancellation, Panic, and Timeout at the Morsel Boundary

The full typed error / cleanup-ordering / latency-bound contract is owned by TASK-505 (`docs/design/engine/cancellation.md`). This document specifies only the morsel-scheduler-side surface:

### 9.1 Cancellation Check Points

- **Between morsels.** Workers check `query.cancelled.load()` immediately after a successful `queue.pop()`; if set, the worker drops the morsel guard without running the morsel and pulls the next morsel.
- **Between batches inside a morsel.** Operators check `query.cancelled.load()` between batches (execution-model.md §3.3); first observed `true` returns `Err(Cancelled)`.
- **Between sub-batches inside `EntityOperatorAdapter`.** §4.1 of execution-model.md specifies the same check at sub-batch granularity for large-entity processing.

The grain is "check after every yield point that already exists"; the morsel scheduler does not introduce new yield points, only respects existing ones.

### 9.2 Panic Isolation

Each morsel runs inside a `std::panic::catch_unwind` boundary (one per worker per morsel). On panic:

1. The morsel guard's drop hook runs (decrement outstanding, signal shard-done if applicable). The accumulator mutex is poisoned only if the panic happened *while holding the lock*; in that case the entire query is unrecoverable, and the next worker that calls `accumulator.lock()` observes `Err(PoisonError)`.
2. **Poison surfacing.** On `Err(PoisonError)` from `accumulator.lock()`, the worker downgrades the poison into `BqliteError::OperatorPanic { message: "accumulator mutex poisoned by upstream panic", location }` per the cancellation-doc surface (TASK-505, `engine/cancellation.md` §4.3) and proceeds with the same teardown path as a direct panic observation. Shard / entity context is encoded in `location: Option<String>`. Workers do **not** call `into_inner()` to recover the inner accumulator — its state is by definition partially-updated and merging it into the cross-shard reduction would silently produce wrong results.
3. The panic payload is converted to `BqliteError::OperatorPanic { message, location }` (cancellation.md §4.3) and stored on the worker's per-query "first error" cell (`OnceCell<BqliteError>`).
4. The query's cancellation token is set, propagating teardown to other workers.
5. The worker returns to the morsel pull loop; subsequent pulls drain the queue (returning quickly without running each morsel) until the query's queues are empty.

**Why drain-after-cancel rather than panic-and-bail-immediately:** If one worker panics while others are mid-morsel, those others must finish their current batch / sub-batch (graceful operator teardown, spill cleanup) before the query terminates. Draining the queue with cancellation set means they observe the cancel at the next yield point and unwind cleanly.

**Why one `catch_unwind` per worker per morsel:** Per-batch `catch_unwind` would be fine-grained but adds overhead to every batch. Per-morsel matches the worker's "long-lived loop with morsel-scoped invariants" model.

### 9.3 Timeout

Timeouts are a *soft* cancel: a timer thread sets `query.cancelled` after `query.timeout` elapses. From the scheduler's perspective there is no difference between a caller-invoked cancel and a timeout-triggered cancel — both flow through §9.1.

**Latency bound:** The worst-case observable latency from cancel-flag-set to query-error-return is **(time to finish the current batch) + (time to pop and discard the queued morsels) + (time to finalize per-shard accumulators and return)**. With default batch size (~65k rows) and default morsel size (~4M rows), the dominant term is the in-flight batch — typically tens of milliseconds for cheap operators, hundreds for expensive ones. The TASK-505 contract may tighten this further (e.g., per-row checks for long-running predicates); until then, this is the bound.

### 9.4 Spill / Temp File Cleanup

When a worker observes cancellation mid-morsel, it must release any spilled state. The cleanup ordering is TASK-502 / TASK-505 scope; this document specifies only that the morsel guard's drop hook is the trigger point — when the guard drops (success, error, or panic), any cleanup the operators registered during morsel processing runs before the worker returns to the pull loop.

**Why anchor cleanup to morsel-guard drop, not to the cancel signal:** Cleanup is per-morsel state (spilled sort runs for an in-progress morsel, half-built per-entity accumulators). The morsel guard already exists as the per-morsel ownership token; reusing it for cleanup avoids a parallel cleanup-tracking structure.

---

## 10. Test Bar

### 10.1 Unit Tests (TASK-523)

- **Morsel boundary correctness.** For a synthetic shard with N entities and predetermined boundaries, `MorselGenerator` emits morsels covering exactly `[0, N)` with no overlaps, no gaps, and entity-aligned boundaries.
- **Single-entity-per-worker invariant.** Property test: for any random distribution of entity event counts in a shard, every entity is seen by exactly one morsel and exactly one worker.
- **Adaptive halving.** Inject artificial worker idle time after the first 4 morsels of a shard; assert that subsequent morsels are half the size of the initial morsel and stop halving at `low_target_rows`.
- **Last-morsel signaling.** Drop a `WorkerMorselGuard` for the shard's last morsel and assert the coordinator observes the shard-done signal exactly once.
- **Empty shards.** A query whose pruning eliminates an entire shard still shows the shard in `morsels_per_shard_min` (zero) and contributes a no-op accumulator merge.
- **Mid-flight cancellation.** Cancel a long query mid-morsel; assert no shard's accumulator is published, no orphan temp files remain, and `BqliteError::Cancelled` surfaces (cancellation.md §4.3).
- **Panic isolation.** A panic in one worker's morsel does not leak across workers; the query returns `BqliteError::OperatorPanic`; other in-flight morsels drain cleanly.
- **`CoreBudget` contention.** Spawn one query holding all permits and a second query; assert the second query blocks until the first finalizes, and assert `compaction_active_ns` is zero for the first query if no compaction was active.

### 10.2 Property Tests (TASK-523, CLAUDE.md §11 bar)

- **Equivalence with single-threaded baseline.** For arbitrary `(query, fixture)` pairs from `tests/src/strategies.rs` (Arrow-shaped generators), assert that the parallel result equals the single-threaded result. Equality semantics differ by query shape — see "What equality means" below.
- **Aggregate result equivalence.** For arbitrary `GROUP BY` queries with exact aggregates (`COUNT` / `SUM` / `MIN` / `MAX` / `AVG` / `COUNT_DISTINCT`), assert exact row-for-row equality after sorting by group-key.
- **Approximate aggregate equivalence within ε.** For percentile aggregates (`P50` / `P90` / `P95` / `P99` from DDSketch), assert that the parallel result is within DDSketch's relative-error guarantee (default α = 0.01) of the single-threaded result. **Not** row-for-row equality — the merged sketch is approximate by design.
- **Cancellation latency bound.** For arbitrary queries, assert that after `cancelled` is set, the query returns within `2 × max_batch_processing_time` (the §9.3 latency bound). Measured via a synthetic operator with predictable batch costs.

**What equality means.** The parallel and single-threaded results compare under one of three rules depending on query shape:

1. **`SELECT … ORDER BY <stable key>`:** exact row-for-row equality. The k-way merge across shards (§6.4 step 4) makes the order deterministic.
2. **`SELECT …` without `ORDER BY`:** **multiset equality** (sort both results on every projected column lexicographically, then compare). Selection queries without `ORDER BY` are not order-stable across shards because shard interleaving order depends on worker scheduling. The TASK-523 implementer must sort both sides before equating; the test fixture's strategy generators must produce queries whose row contents are unique-per-result-row enough that the sort-based comparison is meaningful (or use a multiset-with-counts comparison for queries that can produce duplicate rows).
3. **`SELECT aggregates GROUP BY keys`:** sort both results by group-key tuple, then compare per-row. Exact equality for the exact aggregates listed above; ε-equality for percentile aggregates.

Property test generators must classify each generated query into one of the three buckets and apply the right comparator. Tests should not silently fall back to one comparator (e.g., always-multiset) because that masks ordering bugs in the `ORDER BY` case.

### 10.3 Stress Tests (TASK-525)

Per the TASK-525 task definition, this document's spec is exercised by:

- Hard memory budget exhaustion on parallel aggregation paths.
- Spill fallback on parallel sort.
- Concurrent DELETE / query snapshot isolation under real morsel scheduling.
- Timeout cleanup of temp files mid-morsel.
- Warning-channel overflow (per-worker warnings exceeding the cap).

These are TASK-525 scope but listed here so the morsel-scheduler implementation can spot-check the protocol's behaviour as it lands.

### 10.4 Benchmarks (TASK-524, TASK-526)

- `morsel_skew_throughput` — one shard with a power-law entity-size distribution, measure throughput at 4 / 8 / 16 / 32 cores; the morsel scheduler should achieve > 0.7 × linear scaling on a synthetic 80/20 skew.
- `parallel_aggregate_throughput` — `COUNT(*) GROUP BY entity_property` on a 10M-row fixture; measure speedup vs single-threaded baseline. Target: ≥ 4× on 8 cores.
- `query_compaction_interference` — long-running query overlapped with active compaction; measure the query's wall time and `compaction_active_ns` correlate.
- `worker_idle_ns_*` self-check — high-skew query, assert `worker_idle_ns_p99 > 0` (otherwise the metric is broken or the workload is too well-balanced).

These are TASK-526 scope; this document specifies the bench surface.

---

## 11. Migration Plan and Follow-On `[IMPL]` Tasks

| Task     | Scope                                                                                                                   |
| -------- | ----------------------------------------------------------------------------------------------------------------------- |
| TASK-523 | Land `MorselGenerator`, `MorselQueue`, `WorkerHandle`, `WorkerContext`, `AccumulatorHandle`, the engine-level query queue, and the `CoreBudget` query-side acquisition (§7.1). Wire the worker-pool dispatch loop. Replace `query.rs`'s single-threaded `drive_to_completion` with the morsel-scheduler entry point. Reconcile `execution-model.md` §9 wording in the same checkpoint per §2 of this document. |
| TASK-524 | Land the §8.2 metrics counters (skew rows, compaction-active row), wire the per-worker → per-query merge protocol, expose the `--explain-perf` CLI surface, and the FFI accessor. CPU-cost sampling protocol per execution-model.md §14.3 lands in this task. |
| TASK-525 | Stress suite per §10.3. Depends on TASK-510 (memory tracker), TASK-511 (warning channel), TASK-512 (ingest spill), TASK-513 (sort spill), TASK-514 (cohort spill), TASK-523. |
| TASK-526 | Bench gate per §10.4. Depends on the morsel scheduler being live (TASK-523) and metrics (TASK-524).                       |

The four tasks are sequenced: TASK-523 ships the scheduler scaffold, TASK-524 makes it observable, TASK-525 stress-tests the cancellation/spill/budget interactions, TASK-526 locks in the performance baseline.

### 11.1 Reconciliation With Existing Docs

Updates landed in TASK-523's checkpoint (in-tree at the same time as the scheduler):

- `docs/design/execution-model.md` §9.1: replace "Workers pull morsels with a work-stealing scheduler" with the centralized-MPMC-queue + consumer-side-load-balancing description per §4.1/§4.2 of this document. The behavioral property is the same; the mechanism description was misleading.
- `docs/design/execution-model.md` §9.4: change "Per-query lock-free MPMC queue" wording to disambiguate "one queue per query, one generator per shard" per §4.1 of this document. Add forward-reference to `engine/morsel-scheduler.md` for the generator algorithm and adaptive sizing.
- `docs/design/execution-model.md` §9.5: drop the "thread-local accumulator owned by the morsel generator's `(query, shard)` context" phrasing in favor of the per-shard `Mutex<Box<dyn Accumulator>>` owned by the query coordinator, per §6.2 of this document. The same paragraph already describes the per-shard mutex; this is a wording fix to remove the contradictory "thread-local" claim.
- `docs/design/execution-model.md` §11.1: replace the active-count-check phrasing with the `CoreBudget` semaphore protocol per §7.
- `docs/design/execution-model.md` §14: when TASK-524 implements the metric counters, rationalize the §14.2 / §14.3 ordering (the source-doc subsections are out of order today — `14.3` precedes `14.2`).
- `docs/design/INDEX.md`: add this document to the "Engine" subsection of "Per-subsystem implementation notes" (alongside `engine/operator-fusion.md`). **Done in this checkpoint.**
- `docs/design/storage/compaction-concurrency.md` §4.4: update the "Query execution (TASK-438 onwards) acquires its permits on query start via the same semaphore" wording — TASK-438 is the engine bind step extension, not the query-side acquisition; TASK-523 lands the acquisition per §7.1 of this document. The right wording is "Query execution (TASK-523) acquires its permits on query start via `CoreBudget::acquire_n`."
- `docs/design/storage/compaction-concurrency.md` §12: drop the "SS4 query-side permit acquisition is TASK-438's job" caveat (replaced by §7.1 here, implemented by TASK-523). Add the `acquire_n` extension to the TASK-408-or-TASK-523 ownership boundary.

`docs/design/operators/aggregate-operator.md` already specifies the `Accumulator::merge` contract and per-shard partial accumulation; no changes needed there.

### 11.2 TASK-536 Reconciliation: Real Per-Shard Dispatch

The TASK-523 scaffold landed the queue, accumulator handle, worker guard, and a `run_degenerate` stub but kept `Engine::query` on a single whole-database task. TASK-536 closes that gap:

- **`MorselScheduler::run_per_shard`** is the multi-morsel dispatch entry point. It pushes one morsel per non-empty `ShardSnapshot` onto the queue, spawns up to `query_threads` Rayon workers, and returns the per-shard `AccumulatorHandle`s plus per-worker scratch contexts (one `PerWorkerCtx` per Rayon thread that pulled at least one morsel). Permits are acquired atomically via `CoreBudget::acquire_n` at the start of the call, identical to `submit`. Each worker wraps its closure in `std::panic::catch_unwind` so a panic in one shard's worker converts to `BqliteError::OperatorPanic` in the first-error slot rather than tearing down the Rayon scope (design §9.2). Workers check the cancellation token at the top of every pull iteration (design §9.1 between-morsels yield point).
- **Plan classification in `Engine::query`.** A new `classify_dispatch` walks the planner output and routes:
  - `PhysicalPlan::Aggregate(...)` over a per-shard-safe input → `PerShardAggregate`: bind the aggregate's input per shard, drive each shard's tree, feed batches into a per-shard `HashAccumulator` parked on the `AccumulatorHandle`, pairwise-merge across shards on the coordinator (design §6.4), call `ensure_default_group_if_ungrouped()` then `finish()` to materialise the final batch.
  - Pure data-plane (`Scan`, `FusedSegment` chain without a `Limit` step) → `PerShardConcat`: bind the whole tree per shard, drive each, concat outputs. The order of shards in the output is multiset-equivalent to the single-task baseline; the result equals the legacy answer up to row order. `Limit` inside the chain forces fallback because applying the cap per-shard would multiply the result by the populated-shard count.
  - Everything else (`Sort`, `Distinct`, `MergeSources`, `SubqueryFilter`, `SequenceMatch`, the entity-operator family, `Sample`, DDL/DELETE/EXPLAIN) → `SingleTask`: the legacy whole-database path. v1 trades parallelism for correctness on shapes the per-shard model has not yet been validated against; later tasks can lift specific shapes (top-level Sort with k-way merge, Limit re-applied at the coordinator, etc.) into the per-shard set.
- **Per-worker `WorkerMetricsSnapshot`.** The engine records one default snapshot per unique Rayon worker thread that pulled at least one morsel (`rayon::current_thread_index` dedupe), so `num_workers` reflects actual parallelism on the multi-shard fixture instead of the legacy `1` seed. CPU and wall-time fields stay zero — TASK-537 fills them in.
- **`bind_physical_for_shard`.** Every leaf `ScanPhysical` opens `Database::segment_reader_for_shard` instead of `segment_reader_for_time_range` when the dispatch sets a shard filter. The `Option<u32> shard_filter` is threaded through every recursive bind call site as a function parameter; adding a new recursive call site forces a compile error if it doesn't thread the filter.
- **v1 generator scope.** `MorselGenerator::degenerate` still emits exactly one whole-shard morsel per shard (`EntityRange::All`). The §3.4 adaptive halving control loop reading `MorselSizeState::current_target_rows` is wired but not exercised by the v1 dispatch; sub-shard morsel splitting lands in a follow-on task once individual operators learn to take an entity-range parameter.

---

## 12. Decision Summary

| Question                                                | Decision                                                                       | Rationale                                                                            |
| ------------------------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| Morsel definition                                       | `[entity_lo, entity_hi)` slice within one shard, segment metadata bundled      | Half-open arithmetic; per-shard correctness boundary (§3.1)                          |
| Morsel boundary alignment                               | Always entity-aligned, never splits an entity                                  | Single-entity invariant (execution-model.md §9.3)                                    |
| Morsel target size                                      | `4M rows` high-water, `65k` low-water, halve on idle pressure after warmup     | Workload-skew adaptive without a cost model (§3.4)                                   |
| Generator input                                         | Manifest snapshot metadata only; no segment decode                             | Microsecond-scale generator step keeps lazy generation cheap (§3.2)                  |
| Generator placement                                     | One per `(query, shard)`, lazy, runs on coordinator thread                     | Bounded in-flight memory (§3.5)                                                      |
| Morsel queue                                            | One lock-free MPMC `crossbeam::ArrayQueue` per query, capacity `2 × num_workers` | Single shared queue gives work-stealing without explicit stealing logic (§4.1)       |
| Worker pull strategy                                    | Round-robin across active queries' queues, last-served rotation                | Fairness without priority landmines (§4.2)                                           |
| Worker pool                                             | Rayon, fixed `query_threads = num_cores`, queries queue at `CoreBudget`        | Mature pool host; serialization at permit gate, not morsel gate (§5.1, §5.2)         |
| Concurrent queries                                      | Serial on a saturated pool; FIFO at `CoreBudget` permit acquisition            | Simple, predictable per-query latency (§5.3)                                         |
| Per-shard accumulator ownership                         | One `Mutex<Box<dyn Accumulator>>` per shard, per query; lock per `finish_entity` | Bounded contention; one mutex per shard; per-entity grain (§6.2)                     |
| Cross-shard merge                                       | Single-threaded pairwise on coordinator thread                                 | Constant-time merges; tens of ms total at default scale (§6.4)                       |
| Query/compaction sharing                                | Shared `CoreBudget` semaphore; queries acquire `query_threads` permits up front | Symmetric with compaction's row-group pause; no preemption (§7.1)                    |
| Compaction interference signal                          | `compaction_active_ns` polled per morsel boundary                              | Single observability number; cheap to collect (§7.4)                                 |
| Skew metrics                                            | DDSketch for `worker_idle_ns` and `entity_event_skew`; sums for the rest       | Constant memory, native merge (§8.2)                                                 |
| Metric collection grain                                 | Per-morsel summed into `QueryMetrics` via `Mutex<QueryMetrics>`                | Microsecond-scale contention; reuses existing primitives (§8.3)                      |
| Cancellation grain                                      | Between morsels (worker), between batches (operator), between sub-batches (adapter) | No new yield points; respects existing ones (§9.1)                                   |
| Panic isolation                                         | `catch_unwind` per worker per morsel                                           | Per-morsel matches worker loop scope; per-batch is too fine (§9.2)                   |
| DDL / metadata bypass                                   | Run on caller thread, never touch worker pool or `CoreBudget`                  | No parallelism benefit; scheduler overhead is pure cost (§5.4)                       |

---

## 13. Open Decisions

The following decisions are deferred but do not block TASK-523 / TASK-524 / TASK-525 / TASK-526:

1. **Concurrent multi-query scheduling.** v1 serializes queries at the `CoreBudget` permit gate. A v2 follow-on may introduce fractional permit acquisition (a query taking `query_threads / 2` permits), but only after benchmarks (TASK-526) characterize the contention model. The current design is forward-compatible: changing `query_threads` is a config knob, and the morsel queue's worker-round-robin already supports multiple concurrent queries the moment the permit gate allows it.

2. **Priority lanes for queries.** v1 is FIFO at the permit gate and round-robin at the worker. Operator scripts ("kill the long-running query, prioritize this dashboard refresh") are a v2+ topic — likely surfaced as a separate "interactive" query class with its own permit pool. No code in this document precludes that.

3. **Generator sub-shard parallelism.** §3.2 generates morsels sequentially per shard. If profile data shows the generator becoming a bottleneck (unlikely at 4M-row default morsel size — the generator is microsecond-scale per morsel), a v2 follow-on may parallelize generation by entity-range partitioning within a shard. Out of scope here; the per-shard-sequential generator is the v1 baseline.

4. **`compaction_active_ns` precision.** Polling per morsel boundary may miss compaction bursts shorter than one morsel (~tens of ms). v1 accepts that imprecision; if benchmarks (TASK-526) show shorter-than-morsel compactions are common enough to matter, v2 may add a wake-on-compaction-permit-acquired hook. The metric is informational; missing short bursts does not affect correctness.

5. **Generator-side morsel coalescing for tiny shards.** Shards with < `low_target_rows` total emit one underfilled morsel. A v2 follow-on may merge tiny morsels across shards (breaking the §3.3 invariant — would need a different correctness story for cross-shard coalesced morsels). v1 keeps the simple invariant and accepts the underfill cost.

6. **Adaptive halving's idle-time threshold.** The default 5 ms `halve_idle_threshold_ns` is a guess. TASK-526 benches measure whether the threshold needs tuning per workload class; a config knob (`MorselSizePolicy::halve_idle_threshold_ns`) is exposed so the engine config can override per deployment.

These open decisions match the boundaries of compaction-concurrency.md §13 ("future concerns"): each is a real tradeoff, each has a v1 default, each is forward-compatible with the v2 follow-up.
