# Architecture

## Crate Map

| Crate | Purpose |
|-------|---------|
| `bqlite` | Top-level re-export crate |
| `bqlite-core` | Core types: Event, Entity, Schema, Timestamp, PropertyValue, EntityEventStream, TableSchema |
| `bqlite-ast` | AST types shared by parser and builder APIs |
| `bqlite-storage` | Native storage format, compaction, ingest from CSV/JSON/Parquet, entity-sorted columnar layout, merge scanning, database directory management |
| `bqlite-parser` | BQL text → AST |
| `bqlite-planner` | AST → logical plan → optimizer → physical plan, schema validation at plan construction time |
| `bqlite-operators` | Physical operator implementations: scan, filter, sequence, funnel, retention, sessionize, aggregate, cohort, paths, limit |
| `bqlite-engine` | Execution orchestration, memory management, spill-to-disk, plan execution, slab allocator |
| `bqlite-cli` | Command-line interface |
| `bqlite-ffi` | C ABI surface for PyO3 Python bindings |

## Dependency Direction

Enforced in CI:

```
bqlite-core          (no internal deps)
bqlite-ast           → core
bqlite-storage       → core
bqlite-parser        → ast
bqlite-planner       → ast, core
bqlite-operators     → core, storage, planner
bqlite-engine        → planner, operators, storage, core
bqlite-cli           → engine
bqlite-ffi           → engine
bqlite (top-level)   → core, ast, parser, engine
```

```
                    ┌──────────┐
                    │  bqlite  │  (top-level re-export)
                    └────┬─────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
    ┌──────────┐  ┌────────────┐  ┌──────────┐
    │  parser  │  │   engine   │  │   ast    │
    └────┬─────┘  └─────┬──────┘  └────┬─────┘
         │              │              │
         ▼         ┌────┼────┐         ▼
       ┌─────┐     │    │    │      ┌──────┐
       │ ast │     ▼    ▼    ▼      │ core │
       └──┬──┘  plan  ops  stor     └──────┘
          │      │    │      │
          ▼      ▼    ▼      ▼
        core   core  core   core
```

## Data Flow: Compiler Pipeline

```
BQL Text
  │
  ▼
┌──────────────────┐
│  Parser          │  bqlite-parser
│  (BQL → AST)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  AST             │  bqlite-ast
│  (syntax tree)   │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Logical Planner │  bqlite-planner
│  (AST → logical  │
│   plan)          │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Optimizer       │  bqlite-planner
│  (rewrite rules, │
│   predicate      │
│   pushdown)      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Physical Planner│  bqlite-planner
│  (logical plan → │
│   physical plan) │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Execution       │  bqlite-engine + bqlite-operators
│  Engine          │
└────────┬─────────┘
         │
         ▼
      Results
```

## Execution Model

- **Pull-based iterator** with entity-aware batching.
- **Entity-sorted columnar layout** -- data is stored in columnar row-groups sorted by `(entity_id, timestamp)`, giving entity locality while preserving columnar encoding and compression benefits.
- **Entity-at-a-time** for stateful temporal operators (sequence matching, sessionization, funnels).
- **Columnar batches** for stateless operators (filter, project, aggregate).
