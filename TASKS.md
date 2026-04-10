# bqlite — Task List

## Wave 0: Design Phase

Single-session deep dives with human review. Produces design documents before implementation begins.

### TASK-001: Storage Format Design
**Status**: draft
**Output**: docs/design/storage-format.md
**Description**: Design the native segment format, entity-sorted columnar row-groups, segment metadata, comprehensive encoding layer (dictionary, delta, double-delta, bitpacking, RLE, constant, FSST, FOR, PFOR, ALP, frequency encoding, LZ4), near-zero-copy Arrow decode, late materialization, batch-only ingestion with batch IDs, time-window partitioning, entity-hash sharding, size-tiered compaction, zone maps, tombstone-based deletes (row/batch/entity), manifest catalog, concurrency between readers and compaction, database directory layout.

### TASK-002: Query Language Design
**Status**: unclaimed
**Output**: docs/design/query-language.md
**Description**: Complete BQL grammar specification, operator output schemas (exact column names and types), pipe composition rules, event type quoting syntax, property predicate syntax, variable binding syntax, time literal syntax, aggregation function list, error message strategy. Must also design:
- **Cohorts**: query-computed entity sets (e.g., "users who converted in the last 7 days") that can be joined with any other query as a reusable filter or grouping dimension.
- **Event sub-selection**: extracting specific events from a match — e.g., "the first time an entity did X", "the timestamp when entity completed step Z in an A→B→Z funnel". These are per-entity event references, not aggregates.
- **Result aliases**: a declarative naming mechanism (`AS` or similar) for query results — cohorts, event sub-selections, intermediate pipe stages — that can be referenced in downstream computation. This enables multi-step analytical pipelines where named intermediate results compose naturally.

**Open design questions**:
- Cohort materialization: are cohorts computed inline (subquery-style) or materialized/cached for reuse across queries? What's the persistence model?
- Event sub-selection output schema: does a sub-selection produce full events, timestamps, or structured match records? What columns does `select first(X)` yield?
- Alias scoping: can aliases reference results from earlier pipe stages? Are they lexically scoped to a single query or can they span a session/script? Lazy vs eager evaluation?
- Cohort × query join semantics: is a cohort join an entity-level semi-join (filter to entities in the cohort) or can it also carry cohort-level properties into the downstream query?

### TASK-003: Execution Model Design
**Status**: draft
**Output**: docs/design/execution-model.md
**Description**: Pull-based iterator protocol with PhysicalOperator and EntityOperator traits, entity-aligned batches with sub-batch streaming, demand propagation and generic operator fusion, shard-per-thread parallelism, compaction scheduling, memory budget enforcement, error handling and cancellation, Python integration, per-query metrics.

### TASK-004: Sequence Matching Design
**Status**: draft
**Output**: docs/design/sequence-matching.md
**Description**: Thompson's NFA simulation, global time window enforcement, negation via poison transitions, repetition, $variable binding with independent match tracks, two match modes (FIRST/ALL), EMIT ALL for funnel analysis, tiered execution strategies (step counter fast path for linear patterns, full NFA for general), candidate deque propagation with deferred consumption, filter pushdown to scan layer, aggregation fusion at window expiry.

### TASK-005: Type System Design
**Status**: draft
**Output**: docs/design/type-system.md
**Description**: Supported data types (string, int, float, bool, timestamp, list, map), null handling, type coercion rules, schema declaration syntax, schema validation at plan construction time, Arrow type mapping.

---

## Wave 1: Foundation
Core types, AST, storage format basics, CI pipeline. Establishes shared interfaces before parallel work.

*Tasks to be defined after Wave 0 design review.*

---

## Wave 2: Storage Engine
Native format, compaction, ingest from CSV/JSON/Parquet, entity-partitioned scan.

*Tasks to be defined after Wave 1.*

---

## Wave 3: Parser and Planner
BQL grammar, logical plan, optimizer, physical planner, schema validation.

*Tasks to be defined after Wave 2.*

---

## Wave 4: Operators
Sequence matcher, funnel, retention, sessionizer, filter, aggregate, paths. High parallelism — most tasks are independent.

*Tasks to be defined after Wave 3.*

---

## Wave 5: Engine
Execution orchestration, memory management, spill-to-disk.

*Tasks to be defined after Wave 4.*

---

## Wave 6: CLI and Python
Command-line interface, PyO3 bindings, Python API.

*Tasks to be defined after Wave 5.*

---

## Wave 7: Polish
Benchmarks, documentation, error messages, edge cases.

*Tasks to be defined after Wave 6.*
