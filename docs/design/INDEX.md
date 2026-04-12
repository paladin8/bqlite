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
- **operators/aggregate-operator.md** — `Accumulator` trait + `HashAccumulator` architecture: `AggState`/`GroupKey`/`SumState` types, per-function state and merge rules, null propagation, output schema rules, aggregate expression compilation through `CompiledExpr`, fused-downstream protocol via `finish_entity_into`, DDSketch extensibility contract for TASK-327 (TASK-308, Wave 3)

### Language

- **language/grammar-framework.md** — Parser implementation technology (hand-rolled recursive descent + custom lexer), error strategy (halt on first error), span tracking, production-addition recipe, `WITH (...)` option-list surface for INSERT FROM (TASK-203, Wave 2)

### Planner

- **planner/logical-plan-nodes.md** — authoritative enumeration of logical plan nodes across all waves, with Wave 2 depth nodes (Scan, Filter, Project, Limit, DDL/DML, Explain) fully specified and later-wave nodes stubbed so the catalog doesn't churn (TASK-204, Wave 2)
- **planner/expression-compilation.md** — two-stage expression pipeline (`Expr → TypedExpr → CompiledExpr`), type-check rules, kernel-selection between Arrow compute and monomorphized fast paths, null propagation, scalar function registry, predicate-pushdown integration (TASK-205, Wave 2)

### Storage

- **storage/reader-trait.md** — `SegmentReader` + `SegmentScan` trait surface, segment enumeration, column projection, row-group iteration, zone-map access, predicate pushdown (TASK-109, Wave 1)
- **storage/segment-format-v1.md** — byte-level v1 segment file layout: header, row groups, column chunks, footer body, checksum, trailer; v1 encoding set (Plain, Dictionary, Delta, BitPacking, Constant) with LZ4 post-encoding compression (TASK-201, Wave 2)
- **storage/predicate-pushdown.md** — scan-to-storage pushdown protocol: `ScanPredicate` / `ScanConjunct` shapes, pushable-conjunct taxonomy, zone-map acceptance rules, dictionary-aware filtering, post-filter fallback, Wave 2 `Predicate` trait extension (TASK-202, Wave 2)
