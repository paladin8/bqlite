# Reference Recommendations

Additional recommendations for bqlite after reviewing Appendix A of `.ai/BOOTSTRAP.md`, the current architecture/design docs, and primary-source material on the referenced systems, libraries, and papers.

This document is intentionally scoped to ideas that are not already clearly captured in:

- [`docs/architecture.md`](architecture.md)
- [`docs/design/storage-format.md`](design/storage-format.md)
- [`docs/design/execution-model.md`](design/execution-model.md)
- [`docs/design/planner-pipeline.md`](design/planner-pipeline.md)
- [`docs/design/sequence-matching.md`](design/sequence-matching.md)
- [`docs/design/type-system.md`](design/type-system.md)

It is not a restatement of already-settled directions like entity-major sort order, Arrow-based interchange, demand propagation, generic fusion, FastLanes/FSST/ALP support, or the use of immutable segments.

## 1. Current Docs Already Cover The Big Ideas Well

The existing design docs already capture most of the high-value Appendix A takeaways:

- entity-major columnar storage with `(entity_id, timestamp)` ordering
- hybrid execution with vectorized stateless operators and entity-streaming stateful operators
- demand propagation / fusion as the main optimizer mechanism
- dictionary/FSST/ALP/FastLanes-inspired encodings
- late materialization and compressed Arrow representations
- shard-aligned parallelism and per-shard partial aggregation
- row-group zone maps and compaction-aware segment design

The recommendations below focus on the remaining gaps: execution granularity, pruning granularity, skew handling, adaptive compaction/indexing, compressed execution depth, ingest/runtime details, and benchmark strategy.

## 2. Executive Summary

If only a few net-new ideas get adopted, these are the highest-leverage ones:

1. Decouple storage row-group size from execution vector size. Keep 64K-ish storage groups if they benchmark well, but process them in 1K-4K execution tiles rather than 64K batches.
2. Keep shards as the ownership boundary, but schedule multiple morsels per shard for load balancing, fairness, and skew resistance.
3. Add finer-grained marks inside row-groups. A sparse primary index over 4K-8K-row marks is a better pruning unit than a whole 64K row-group.
4. Make secondary indexes selective and entity-aware. Start with event-type/entity-presence bitmaps and a few cheap skip indexes rather than broad inverted indexing.
5. Parallelize large compactions via entity-range subcompaction. A single `(window, shard)` merge can be split across cores without violating entity locality.
6. Make selection vectors first-class and compact adaptively. Filters should not eagerly materialize new batches unless selectivity demands it.
7. Push compressed execution deeper. Equality/IN/RLE/FOR/dictionary work should stay compressed longer than the current docs explicitly require.
8. Treat precomputed behavioral aggregates as an explicit opt-in feature family once the raw engine is solid, not as a vague future capability.

## 3. Cross-Technology Audit

This section records the incremental takeaway from each Appendix A technology after comparing it to the current docs.

### 3.1 Execution Engines

| Technology | What the current docs already have | Net-new takeaway |
|---|---|---|
| DuckDB vectorized execution | Vectorized stateless pipeline, compressed vectors, Arrow arrays | bqlite should copy DuckDB's separation between storage units and execution vectors. The current 64K batch target is too large for hot vectorized kernels. |
| HyPer morsel-driven parallelism | shard-local parallelism, thread-local state, final merge | "One shard = one task" is too coarse under skew. Preserve shard ownership, but add intra-shard morsels. |
| Kersten et al. compiled vs vectorized | SIMD skepticism for branchy temporal logic is already reflected | Good confirmation that SIMD effort belongs in scan/filter/aggregate, not the NFA core. |
| Sneller AVX-512 SQL VM | >1 GB/s/core aspiration already aligns with project goals | Do not jump to handwritten SIMD. Exhaust auto-vectorized kernels, compressed execution, and pruning first. |

### 3.2 Storage / Indexing Systems

| Technology | What the current docs already have | Net-new takeaway |
|---|---|---|
| ClickHouse MergeTree | immutable sorted parts, codecs, zone maps, custom format | Add ClickHouse-style marks/granules inside row-groups and make skip indexes conditional on sort-key correlation. |
| RocksDB compaction family | immutable-segment compaction; size-tiered compaction chosen | Keep size-tiered (query-performance engines do not need LSM-style temperature tiers), but borrow RocksDB's subcompaction to parallelize large merges. |
| ScyllaDB shard-per-core | shared-nothing principle already informs per-shard execution | Add optional core pinning / NUMA-local arenas, not as a default requirement but as an expert mode. |
| Apache Druid | time partitioning, dictionary encoding, Roaring bitmaps discussed | Druid's bitmap lesson is strongest for selective dimensions. For bqlite that means event-type/entity-presence indexes first, not full property indexing. |
| Firebolt primary + aggregating indexes | appendix already pointed at aggregating indexes as promising v2 | Make aggregating indexes explicit and query-shaped. Also borrow the sparse primary-index granule idea. |
| Redpanda thread-per-core | shared-nothing thread-per-core intuition already fits | Use it as validation for optional pinned-worker mode and core-local buffer reuse, not as evidence that bqlite should always pin everything. |
| Rockset converged index | appendix already notes the idea | Avoid a broad converged-index ambition early. Keep the index portfolio narrow and tied to actual behavioral-query hot paths. |

### 3.3 Formats / Libraries / Papers

| Technology | What the current docs already have | Net-new takeaway |
|---|---|---|
| FastLanes | adopted in spirit for integer lanes and SIMD-friendly decode | Fine-grained access matters as much as the codec itself; don't bind execution to whole row-groups. |
| FSST | already chosen for high-cardinality strings | Standardize on view/prefix-based string materialization so the decode win is not lost later in the pipeline. |
| ALP | already included for floating-point columns | Skip post-codec LZ4 on ALP output when the ratio threshold says it is not paying; do not add a second, heavier post-codec. |
| Roaring bitmaps | already discussed as deferred | Use only where the unit of skipping matches the execution model: granules or entity ordinals, not arbitrary row-level indexes everywhere. |
| Apache Arrow | already core to interchange and in-memory execution | Lean harder on Arrow View / Run-End / Dictionary layouts and the C Data Interface, especially at the Python boundary. |
| Vortex | current docs intentionally avoid depending on it as a container | Borrow its ideas more aggressively: small postscript, file-level stats, registry-like metadata, and compressed compute interfaces. |
| Parquet | ingest only, already settled | No design change, but Parquet page/index ideas reinforce the case for intra-row-group marks in bqlite's native format. |
| simdjson / simd-json | appendix mentions ingest acceleration | JSON ingest should be streaming and schema-aware, not DOM-based. |
| mimalloc | appendix suggests a global allocator swap | Benchmark before adopting globally; embeddable-library constraints matter more than they do in a standalone database server. |
| BtrBlocks | current docs defer sampling-based encoding selection | Promote empirical, sampled encoder choice earlier than v2 if compression becomes a hot bottleneck. |
| LSM compaction design-space paper | not yet reflected in detail | Use its primitives to define compaction policy explicitly: trigger, layout, granularity, movement policy. |

## 4. Recommended Additions

### 4.1 Decouple Storage Row-Groups From Execution Vectors

Current gap:

- `docs/design/execution-model.md` currently targets 65,536-row execution batches to match row-groups.

Recommendation:

- Keep row-groups as a storage/compression unit if 64K continues to benchmark well.
- Process each row-group as a sequence of smaller execution vectors, ideally starting around 1,024-4,096 rows and tuning empirically.
- Preserve entity-boundary guarantees by allowing the scan to extend or shrink the final tile around an entity boundary rather than forcing a single monolithic 64K batch through the whole pipeline.

Why this is worth doing:

- DuckDB's default vector size is 2,048 tuples, and its vector formats are explicitly optimized around fixed-size execution vectors rather than storage chunks.
- FastLanes and Vortex both emphasize fine-grained access to keep decompression and compute cache-friendly.
- 64K execution vectors are good for amortization but bad for cache residency, cancellation latency, branchy filters, and pipelines where selectivity drops quickly.

Net effect:

- better cache behavior for stateless kernels
- lower cancellation/query-timeout latency
- easier adaptive compaction of sparse selections
- simpler future NUMA/local scheduling

Suggested doc change:

- change "row-group size = batch size" from a design principle into an implementation convenience that the engine is free to break.

### 4.2 Keep Shards As Ownership, But Add Intra-Shard Morsels

Current gap:

- one query task per shard is the current parallelism model.

Recommendation:

- preserve shard alignment as the logical ownership boundary
- split work inside a shard into entity-range morsels or mark-range morsels
- let worker threads pull morsels dynamically
- keep thread-local operator state and accumulators; merge at the end exactly as today

Why this is worth doing:

- HyPer's morsel-driven model exists because coarse partitions do not load balance well.
- behavioral/event workloads are usually power-law distributed; some shards or entity ranges will be much hotter than others.
- this also improves fairness when multiple queries share the same pool.

Important constraint:

- a single entity must still be fully owned by one morsel; do not split an entity across workers.

A practical shape for bqlite:

- shard = correctness boundary
- morsel = execution boundary
- worker = scheduling/NUMA boundary

### 4.3 Add Marks Inside Row-Groups

Current gap:

- pruning is row-group granularity today.

Recommendation:

- keep large row-groups for encoding efficiency
- add smaller marks or granules inside each row-group, likely in the 4K-8K-row range
- for each mark, store:
  - first `(entity_id, timestamp, __seq_id)`
  - min/max for a few key columns
  - byte offsets into the larger column chunk
  - optional tiny per-mark value sets for selected low-cardinality columns

Why this is worth doing:

- ClickHouse stores the first values of each granule and uses them for sparse primary-index pruning.
- Firebolt's primary index also works over granules, with smaller intra-tablet ranges for selective filtering.
- Parquet's page/column index separation also points the same direction: large write-time chunks, smaller read-time skip units.

This is especially valuable for bqlite because:

- single-entity lookups otherwise still pay a full row-group cost
- event-type filters for MATCH are often selective enough that 64K is too coarse
- sequence operators benefit if candidate entities can be narrowed before the hot loop

### 4.4 Add A Thin Runtime Synopsis Layer

Current gap:

- the planner is intentionally statistics-free.

Recommendation:

- keep the optimizer rule-based at the logical level
- add lightweight physical synopses that the scan/physical planner can consult:
  - rows and bytes per segment/mark
  - event-type counts
  - approximate distinct entity count
  - max events for any single entity in the segment
  - maybe anchor-step selectivity hints once MATCH is implemented

Why this is worth doing:

- this is not a full cardinality-estimation system
- it is just enough data to pick between:
  - plain scan vs secondary index probe
  - step-counter vs more expensive execution path
  - aggressive vs conservative morsel sizing
  - whether a query is likely to violate group/event limits

This is the main missing bridge between the storage-format doc and the physical-planning doc.

### 4.5 Make Secondary Indexes Selective And Entity-Aware

Current gap:

- Bloom filters and Roaring bitmaps are broadly deferred.

Recommendation:

- do not introduce a general "index every interesting column" policy
- instead, add only a very small index portfolio first:
  - cheap min/max or set indexes on low-cardinality, frequently filtered dimensions
  - Roaring bitmaps for `event_type`
  - entity-presence bitmaps or granule-presence bitmaps for rare mandatory steps in MATCH
  - Bloom filters only for true point-lookups on high-cardinality strings when sort-order correlation is weak

Why this is worth doing:

- ClickHouse's skip-index guidance is effectively "cheap indexes broadly, heavy indexes sparingly, and only when they match real query patterns."
- Druid's dictionary + bitmap design is powerful specifically because its dimensions are a good fit for it.
- Rockset is a useful warning sign here: broad converged indexing is powerful, but it is also a write-amplifying commitment.

For bqlite specifically:

- event-type bitmaps are much more justified than arbitrary property bitmaps
- the right bitmap unit is probably not raw rows; it is marks or entity ordinals
- if the bitmap does not shrink the set of candidate entities materially, it should not exist

### 4.6 Add A Bitmap-Assisted MATCH Candidate Path

Current gap:

- current docs push predicates down, but still assume the surviving event stream is scanned normally.

Recommendation:

- for eligible patterns, add a pre-NFA candidate phase:
  - intersect event-type/entity-presence bitmaps for mandatory positive steps
  - optionally choose the rarest required step as an anchor generator
  - only feed candidate entities or anchor positions into the step-counter/NFA path

Where it helps:

- long funnels with a rare middle or final step
- retention-style queries where only a minority of entities satisfy the anchor and follow-up event set

Why this is not already covered:

- existing pushdown removes rows
- this recommendation removes entities or anchor positions before the hot state-machine loop

Risk:

- semantics must stay exact
- this should be a second-phase optimization after the baseline step-counter/NFA is stable

### 4.7 Parallelize Large Compactions With Subcompaction

Current gap:

- storage format uses size-tiered compaction within `(window, shard)` with no inner parallelism. A single very large merge is bound to one core.

Recommendation:

- when compacting a very large `(window, shard)`, split by entity range and merge the ranges in parallel, analogous to RocksDB subcompaction.
- the boundary picker must snap to entity-id transitions so no entity is split across subcompaction outputs.

Why this is worth doing:

- stabilized windows can grow to tens or hundreds of gigabytes per shard; serial compaction of those leaves cores idle and lengthens the window during which reads see extra sorted runs.
- the read path already relies on entity locality, so subcompaction boundaries align naturally with the invariant rather than fighting it.

Explicit non-goals:

- **no temperature-aware compaction.** bqlite compacts every window for query performance. There is no separate "cold window" policy, no per-bucket trigger set, no one-shot consolidation phase. Older windows are compacted the same way as hot ones.
- **no storage-focused compression policy.** We stay on LZ4 uniformly. Swapping in ZSTD for older windows would buy storage at the cost of decode CPU — exactly the wrong trade for a query-performance engine.

### 4.8 Make Selection Vectors First-Class

Current gap:

- the current docs talk about compressed arrays and pruning, but not about selection-vector-first execution as the main post-filter representation.

Recommendation:

- filters should usually produce selection vectors over existing batches rather than copied batches
- dictionary vectors, constant vectors, and RLE-like layouts should survive through multiple stateless operators
- only compact physically when:
  - the selection gets too sparse
  - a downstream operator needs contiguous buffers
  - benchmark data says compaction is now cheaper than carrying the indirection

Why this is worth doing:

- DuckDB's dictionary vectors and sliced vectors make this a first-class optimization.
- DuckDB's recent chunk-compaction work shows that sparse selections do eventually become a liability, so compaction should be adaptive, not absent.

This is one of the biggest low-level wins still missing from the docs.

### 4.9 Push Compressed Execution Deeper

Current gap:

- the docs say compressed forms can flow through the pipeline, but they do not yet force specific kernels to exploit that aggressively.

Recommendation:

- explicitly plan for kernels that work on:
  - dictionary codes for equality / IN / low-cardinality GROUP BY
  - run-end encoded arrays for COUNT / SUM / MIN / MAX when valid
  - FOR/Delta residuals for range checks when possible
  - per-event step bitmasks for MATCH predicate dispatch

This should be a defined implementation goal, not just a nice property of Arrow arrays.

Why this is worth doing:

- DuckDB and Vortex both get a large fraction of their performance by not flattening everything too early.
- bqlite has even more to gain because event_type and a few core dimensions are likely to remain low-cardinality and heavily filtered.

### 4.10 Standardize On View/Prefix-Based String Materialization

Current gap:

- the docs mention `Utf8View`, but mostly in passing.

Recommendation:

- make `Utf8View` or an equivalent prefix-carrying representation the standard materialized string form
- keep short strings inline when possible
- preserve 4-byte prefixes for early-out comparisons
- prefer dictionary-of-view layouts over dictionary-of-flat-utf8 layouts

Why this is worth doing:

- Arrow's view layout explicitly stores short strings inline and stores a prefix for long strings because comparisons often terminate in the first few bytes.
- DuckDB's `string_t` follows the same pattern.
- this matches bqlite's workload well: event types, devices, countries, page names, campaign IDs, and similar strings are short and frequently compared.

### 4.11 Make Precomputed Behavioral Aggregates Explicit

Current gap:

- the appendix points toward Firebolt-style aggregating indexes, but the design docs do not yet convert that into a concrete direction.

Recommendation:

- once the raw engine is solid, add explicit user-declared behavioral aggregate indexes or projections, not automatic magic
- focus on stable, repeated dashboard-style queries:
  - daily funnel counts
  - weekly retention by cohort
  - session summary tables
  - top-N event or path summaries

Design guidance:

- group keys should match filter/pruning patterns, with low-cardinality keys first when beneficial
- the physical layout of the precomputed index matters just as much as the aggregation itself
- maintenance and vacuum/defragmentation policy need to be part of the design from day one

Why this is worth doing:

- Firebolt's docs are clear that aggregating indexes are effective because they are query-shaped, automatically maintained, and physically organized to prune well.

Why this should stay opt-in:

- they increase write cost
- they complicate mutation/backfill semantics
- they are easy to overbuild "just in case"

### 4.12 Make JSON Ingest Streaming And Schema-Aware

Current gap:

- JSON ingest is covered functionally, but not at hot-path depth.

Recommendation:

- use `simd-json` or equivalent in a streaming, validating path
- parse directly into typed builders / column writers
- avoid building `serde_json::Value` trees on the hot ingest path
- parallelize parse -> validate -> partition -> encode with bounded queues

Why this is worth doing:

- simdjson's published work and production library both exist because DOM-style JSON parsing burns instructions and branches that modern SIMD parsing avoids.
- bqlite is especially sensitive because JSON is the slow-path ingest format compared with Parquet, so it needs the most help.

### 4.13 Benchmark Allocators Before Choosing A Global One

Current gap:

- the appendix mentions MiMalloc as a likely win.

Recommendation:

- do not make `mimalloc` the default global allocator without benchmarks in:
  - pure Rust query execution
  - query + compaction concurrency
  - Python embedding via PyO3
  - macOS and Linux separately
- even if `mimalloc` wins, still build worker-local scratch arenas / object reuse so allocator choice matters less.

Why this is worth doing:

- mimalloc's design is attractive for bqlite: free-list sharding, eager purging, first-class heaps, low-contention concurrent frees.
- but bqlite is an embeddable library. A global allocator swap changes behavior for the whole host process, not just bqlite.

Recommendation shape:

- benchmark first
- if beneficial, expose allocator choice as an integration option before making it the default

### 4.14 Add Explicit Readahead / Access-Pattern Hints

Current gap:

- current docs mention OS readahead abstractly and mention prefetch only as a later possibility.

Recommendation:

- add scan-path hooks for `Sequential` / `WillNeed` style advice on mmapped files or equivalent read paths
- use different hinting for:
  - long sequential segment scans
  - random-ish point lookups
  - compaction

Why this is worth doing:

- `memmap2` already exposes the right access-pattern hints.
- bqlite's access patterns are predictable enough that the engine can provide useful OS hints instead of hoping the kernel infers everything.

This is a lower-priority recommendation than the batching/pruning work, but it is cheap and nicely aligned with an embedded engine.

### 4.15 Expand Benchmarks From Query Latency To Cost Metrics

Current gap:

- core beliefs call for frequent microbenchmarking, but the docs do not yet pin down the performance counters that matter most.

Recommendation:

- benchmark and report:
  - GB/s/core scanned
  - cycles per event
  - bytes decoded vs bytes scanned
  - branch misses
  - LLC misses
  - rows/marks pruned before decode
  - fraction of time in decompression vs operator logic
  - small-batch compaction rate
  - skew sensitivity by entity distribution

Workload matrix:

- warm vs cold cache
- low vs high selectivity filters
- rare-step funnels
- high-cardinality GROUP BY
- pathological large-entity cases

This is the measurement layer needed to decide among the earlier recommendations rationally.

## 5. Things Worth Deferring

These are interesting ideas from the surveyed systems, but they should stay behind the recommendations above.

### 5.1 Handwritten SIMD / AVX-512 Everywhere

Sneller proves that handwritten SIMD can be extraordinary. It does not prove that bqlite should start there.

Recommendation:

- defer handwritten AVX2/AVX-512 kernels until after:
  - execution vector sizing is fixed
  - selection-vector execution exists
  - compressed kernels exist
  - marks and skip indexes exist

Reason:

- those changes are likely to buy more speed per unit of engineering effort, with less platform lock-in.

### 5.2 Broad Converged Indexing

Rockset's converged index is powerful, but it is also a large storage/maintenance commitment.

Recommendation:

- do not build row store + column store + inverted index for all data
- instead build a narrow, behavioral-query-specific index portfolio

### 5.3 Full Cost-Based Optimization

Recommendation:

- keep the logical optimizer rule-based
- add lightweight physical synopses only

Reason:

- bqlite's hot path is mostly in execution and pruning, not in complex join ordering
- the storage/model shape is specialized enough that a full cost model will not be the first bottleneck

### 5.4 Automatically Built Aggregating Indexes

Recommendation:

- only build them explicitly
- no automatic "shadow projections" based on observed queries yet

Reason:

- too easy to create hidden write amplification and storage bloat

## 6. Suggested Adoption Order

### Phase 1: Should Happen Early

- decouple execution vector size from row-group size
- add selection-vector-first execution
- add marks inside row-groups
- add intra-shard morsels
- expand benchmark metrics

### Phase 2: After The Core Read Path Stabilizes

- add thin runtime synopses
- add selective secondary indexes
- add entity-range subcompaction for large `(window, shard)` merges
- add string view/prefix standardization everywhere

### Phase 3: Once v1 Semantics Are Proven

- bitmap-assisted MATCH candidate generation
- explicit aggregating indexes / precomputed behavioral summaries
- optional pinned-worker / NUMA-aware expert mode
- allocator default changes, if benchmarks justify them

## 7. Sources

Primary sources reviewed for this document:

- DuckDB execution format: <https://duckdb.org/docs/stable/internals/vector.html>
- DuckDB data chunk compaction: <https://duckdb.org/library/data-chunk-compaction/>
- HyPer morsel-driven parallelism: <https://portal.fis.tum.de/en/publications/morsel-driven-parallelism-a-numa-aware-query-evaluation-framework/>
- Compiled vs vectorized queries: <https://portal.fis.tum.de/en/publications/everything-you-always-wanted-to-know-about-compiled-and-vectorize/>
- ClickHouse best practices: <https://clickhouse.com/blog/10-best-practice-tips>
- ClickHouse compression discussion: <https://clickhouse.com/blog/lz4-compression-in-clickhouse>
- RocksDB compaction overview: <https://github.com/facebook/rocksdb/wiki/Compaction>
- RocksDB leveled compaction: <https://github.com/facebook/rocksdb/wiki/Leveled-Compaction>
- RocksDB universal compaction: <https://github.com/facebook/rocksdb/wiki/universal-compaction>
- RocksDB subcompaction: <https://github.com/facebook/rocksdb/wiki/Subcompaction>
- LSM compaction design space paper: <https://vldb.org/pvldb/vol14/p2216-sarkar.pdf>
- ScyllaDB shard-per-core: <https://www.scylladb.com/product/technology/shard-per-core-architecture/>
- Redpanda architecture: <https://docs.redpanda.com/25.1/get-started/architecture/>
- Redpanda thread-per-core buffer management: <https://www.redpanda.com/blog/tpc-buffers>
- Apache Druid segments: <https://druid.apache.org/docs/latest/design/segments/>
- Apache Druid rollup: <https://druid.apache.org/docs/latest/ingestion/rollup/>
- Firebolt primary index: <https://docs.firebolt.io/overview/indexes/primary-index>
- Firebolt aggregating index: <https://docs.firebolt.io/overview/indexes/aggregating-index>
- Sneller SQL VM in AVX-512: <https://sneller.ai/blog/sql-vm-in-avx-512/>
- Sneller performance overview: <https://sneller.ai/blog/querying-terabytes-of-json-per-second/>
- Rockset converged indexing: <https://dev.to/rocksetcloud/converged-indexing-the-secret-sauce-behind-rockset-s-fast-queries-3hp8>
- FastLanes paper page: <https://ir.cwi.nl/pub/32992>
- FSST paper PDF: <https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf>
- ALP overview and paper link: <https://duckdb.org/library/alp/>
- Apache Arrow columnar format: <https://arrow.apache.org/docs/format/Columnar.html>
- Apache Arrow C Data Interface: <https://arrow.apache.org/docs/13.0/format/CDataInterface.html>
- Parquet concepts: <https://parquet.apache.org/docs/concepts/>
- Parquet file format: <https://parquet.apache.org/docs/file-format/>
- Vortex overview: <https://docs.vortex.dev/python/>
- Vortex file format: <https://docs.vortex.dev/specs/file-format>
- simdjson library: <https://github.com/simdjson/simdjson>
- simdjson paper page: <https://lemire.me/fr/publication/arxiv190208318/>
- mimalloc: <https://github.com/microsoft/mimalloc>
- Rayon thread-pool builder: <https://docs.rs/rayon/latest/rayon/struct.ThreadPoolBuilder.html>
- memmap2 advice API: <https://docs.rs/memmap2/latest/memmap2/enum.Advice.html>
