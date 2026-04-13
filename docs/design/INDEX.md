# Design Documents

Deep-dive design documents for bqlite. These cover the detailed technical decisions behind each major subsystem.

## Wave 0

1. **storage-format.md** -- Native segment format, entity-sorted layout, column compression, tombstoning, compaction (STATUS: draft)
2. **query-language.md** -- Complete BQL grammar, pipeline composition, MATCH surface syntax, FUNNEL/RETENTION sugar, aliases, cohorts via IN, error strategy (STATUS: draft)
3. **execution-model.md** -- Iterator protocol, entity-aware batching, memory management (STATUS: draft)
4. **sequence-matching.md** -- NFA construction, time windows, negation, variable bindings, match modes, EMIT ALL (STATUS: draft)
5. **type-system.md** -- Data types, null handling, coercion, Arrow mapping (STATUS: draft)

6. **planner-pipeline.md** -- Logical plan nodes, AST lowering, optimizer rules, physical planning, DemandCapabilities, schema validation (STATUS: draft)

## Per-subsystem implementation notes

Implementation-level design notes that refine a Wave 0 direction doc for a specific task or wave. Organized by subsystem.

### Operators

- **operators/operator-traits.md** — `PhysicalOperator` + `EntityOperator` trait surface, lifecycle, cancellation, sub-batch streaming (TASK-108, Wave 1)
- **operators/match-operator.md** — MATCH operator architecture: state layout, EntityOperator integration, output schema, emission points, active-state cap, demand-driven column reduction (TASK-301, Wave 3)
- **operators/matcher-strategy.md** — Compile-time pattern classifier and strategy selection matrix: `PatternClass` enum, classification predicates, demand override rules, variable-binding interaction with step counter, fallback behavior, microbenchmark methodology (TASK-302, Wave 3)
- **operators/sort-distinct.md** — `SortOperator` + `DistinctOperator` contracts: physical descriptors, key compilation, null-ordering rules, hard-cap overflow policy, plan-tree placement, no-spill rationale (TASK-310, Wave 3)
- **operators/sessionize.md** — `SessionizeOperator` architecture: gap-exclusive boundary rules, end-event membership and list syntax, `session_id`/`session_duration` output schema, per-entity session buffer and emission timing, `WITHIN SESSION` interaction with MATCH, demand-driven column forwarding, per-entity event cap with diagnostic, fused aggregate deferral to Wave 5, benchmark/edge-case matrix (TASK-405, Wave 4)
- **operators/aggregate-operator.md** — `Accumulator` trait + `HashAccumulator` architecture: `AggState`/`GroupKey`/`SumState` types, per-function state and merge rules, null propagation, output schema rules, aggregate expression compilation through `CompiledExpr`, fused-downstream protocol via `finish_entity_into`, DDSketch extensibility contract for TASK-327 (TASK-308, Wave 3)
- **operators/compactstring-evaluation.md** — CompactString (compact_str 0.9) evaluation for matcher hot paths: microbenchmark methodology, go/no-go recommendation (conditional go for `BindingValue::String` only), migration boundaries, alternative approaches (interning, Arc\<str\>) (TASK-332, Wave 3)
- **operators/attribute.md** — `AttributeOperator` architecture: sliding-window deque, window boundary rules (inclusive-at-lookback, strict-at-conversion), three-way row-shape emission, emit-before-add per-event ordering, `touchpoint_key` expression surface, per-entity deque cap with diagnostic, scan-range extension, demand-driven conversion forwarding, composition rules, edge-case matrix (TASK-406, Wave 4)

### Language

- **language/grammar-framework.md** — Parser implementation technology (hand-rolled recursive descent + custom lexer), error strategy (halt on first error), span tracking, production-addition recipe, `WITH (...)` option-list surface for INSERT FROM (TASK-203, Wave 2)
- **language/cohorts-aliases-joins.md** — Cohort materialization, alias binding, and entity-aligned source JOINs: alias scoping/caching/cycle-detection, `IN QUERY`/`IN alias` equivalence, multi-column positional binding, `MergeSources` n-ary merge operator, `__source_table_id` discriminator, `SubqueryFilter` hash-set probe with entity-id pushdown (TASK-407, Wave 4)

### Planner

- **planner/logical-plan-nodes.md** — authoritative enumeration of logical plan nodes across all waves, with Wave 2 depth nodes (Scan, Filter, Project, Limit, DDL/DML, Explain) fully specified and later-wave nodes stubbed so the catalog doesn't churn (TASK-204, Wave 2)
- **planner/expression-compilation.md** — two-stage expression pipeline (`Expr → TypedExpr → CompiledExpr`), type-check rules, kernel-selection between Arrow compute and monomorphized fast paths, null propagation, scalar function registry, predicate-pushdown integration (TASK-205, Wave 2)
- **planner/wave3-lowering.md** — AST→logical lowering rules for Match/Stats/OrderBy/Distinct, `DemandSet` type definition, backward demand propagation algorithm, step-property resolution, fusion setup protocol, schema validation rules for step references and variable bindings through aggregates (TASK-309, Wave 3)
- **planner/demand-protocol.md** — `DemandCapabilities` protocol: operator-side capability advertisement struct (plain struct with bool fields), `DemandPropagation` trait, crate placement in `bqlite-planner::demand`, capability matching during physical planning, `const DEMAND_CAPS` on physical descriptors, unmet-demand error policy, scaffold retirement plan (TASK-409, Wave 4)

### Storage

- **storage/reader-trait.md** — `SegmentReader` + `SegmentScan` trait surface, segment enumeration, column projection, row-group iteration, zone-map access, predicate pushdown (TASK-109, Wave 1)
- **storage/segment-format-v1.md** — byte-level v1 segment file layout: header, row groups, column chunks, footer body, checksum, trailer; v1 encoding set (Plain, Dictionary, Delta, BitPacking, Constant) with LZ4 post-encoding compression (TASK-201, Wave 2)
- **storage/predicate-pushdown.md** — scan-to-storage pushdown protocol: `ScanPredicate` / `ScanConjunct` shapes, pushable-conjunct taxonomy, zone-map acceptance rules, dictionary-aware filtering, post-filter fallback, Wave 2 `Predicate` trait extension (TASK-202, Wave 2)
- **storage/compaction-concurrency.md** — compaction concurrency protocol: unit of work (`(window, shard)`), dedicated scheduler thread pool + `compact_now` sync API, core-budget semaphore with row-group-boundary pause, 5-step atomic manifest publication, `Arc`-refcount reclamation sweep, startup orphan cleanup, tombstone interaction ordering (TASK-403, Wave 4)
- **storage/advanced-encodings.md** — Wave 4 advanced encoding research: go/no-go evaluation of RLE, DoubleDelta, FOR, PFOR, FSST, ALP, and Frequency encoding against the v1 baseline, with compression ratio, decode throughput, and predicate-pushdown analysis per candidate (TASK-401, Wave 4)
- **storage/deletes.md** — tombstone and delete semantics: cheap-class predicate taxonomy, `ALLOW SCAN` opt-in for full-scan deletes, `TombstoneFile` schema with four granularities, per-query tombstone snapshots, scan-time filtering order, concurrent-delete serialization, cross-shard crash atomicity via idempotent retry, exact `rows_affected` return, compaction-time reclamation ordering (TASK-404, Wave 4)
