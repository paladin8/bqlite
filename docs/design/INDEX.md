# Design Documents

Deep-dive design documents for bqlite. These cover the detailed technical decisions behind each major subsystem.

## Wave 0

1. **storage-format.md** -- Native segment format, entity-sorted layout, column compression, tombstoning, compaction (STATUS: draft)
2. **query-language.md** -- Complete BQL grammar, pipeline composition, MATCH surface syntax, FUNNEL/RETENTION sugar, aliases, cohorts via IN, error strategy (STATUS: draft)
3. **execution-model.md** -- Iterator protocol, entity-aware batching, memory management (STATUS: draft)
4. **sequence-matching.md** -- NFA construction, time windows, negation, variable bindings, match modes, EMIT ALL (STATUS: draft)
5. **type-system.md** -- Data types, null handling, coercion, Arrow mapping (STATUS: draft)

6. **planner-pipeline.md** -- Logical plan nodes, AST lowering, optimizer rules, physical planning, DemandCapabilities, schema validation (STATUS: draft)

## Infrastructure

7. **agent-workflow.md** -- Parallel agent containers, task locking, git workflow, cmux integration (STATUS: draft)
