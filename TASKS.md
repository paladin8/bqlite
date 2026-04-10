# bqlite — Task List

This file is the authoritative plan for bqlite. It lists every wave and every known task, and is the reference both humans and autonomous agents consult to decide what to build next.

**It does not track task status.** Lock files in `tasks/active/` and done markers in `tasks/completed/` are the source of truth for what is claimed and what is complete. For the execution protocol — task claiming, checkpoints, git workflow — see [AGENTS.md](AGENTS.md).

The plan will be revised as work progresses. Later waves are intentionally loose; they get fleshed out as earlier waves ship and we learn what was actually needed.

---

## How We Plan and Execute Work

The rules below govern how tasks and waves are structured. New tasks added at any time must conform.

### Waves

A **wave** is a substantial body of work that delivers a specific capability. Waves are not subsystem layers — a single wave typically cuts across storage, parser, planner, operators, and engine simultaneously. This is what lets 4+ agents work in parallel from day one of the wave.

Every wave satisfies:

1. **Capability-shaped.** The wave is named by what the user can do when it lands (e.g. "Scan & Filter MVP", "Pattern Matching MVP"), not by which crate it touches.
2. **Substantial.** Tens of tasks. A wave with fewer than ~15 tasks is probably a subset of another wave and should be merged.
3. **Parallelizable.** At least 4 independent tasks must be claimable at wave start. Most waves support 8-15 concurrent agents.
4. **Unblocking.** A wave is *done* when its capability ships — not when every possible ancillary task in its subsystems is complete. Remaining refinements roll into later waves.

### Tasks

Every task satisfies:

1. **1-2 days of senior engineer effort.** Tasks larger than this split. Tasks smaller than ~half a day fold into a neighbor.
2. **Explicit dependencies.** Each task lists its `Depends on:` set. Tasks with no internal deps are the parallelism budget for the wave.
3. **Self-contained checkpoints.** The implementing agent breaks the task into checkpoints per AGENTS.md — compile, test, lint, merge to main.
4. **Names its key output paths.** Code tasks name the 1-2 primary files they create or substantially modify (not every touched file). Design tasks name the doc path they produce.

### Task tags

Tasks may carry one or more tags in their header:

- `[DESIGN]` — produces an implementation-level design note under `docs/design/<subsystem>/` as its only deliverable. If the component is non-trivial, the design task is paired with one or more `[IMPL]` tasks in the same wave that depend on it. Design tasks **always live in the wave that needs them, never backfilled into Wave 0**. Wave 0 sets direction; design-first tasks resolve implementation detail.
- `[IMPL]` — implements a component. May depend on a `[DESIGN]` task in the same wave; simple components skip the design task entirely.
- `[TRAIT]` — changes a cross-crate trait surface. Rare by design. Trait tasks are **high-priority merge-first**: they must land and propagate before any dependent task starts, to avoid cascading rebases. Wave 1 freezes the v0 trait set; any post-Wave-1 trait change requires a `[TRAIT]` task.

Risk is orthogonal to tag — a `[DESIGN]`, `[IMPL]`, or `[TRAIT]` can all be risky. Risky work is surfaced via the anchor-task mechanism below, not a separate tag.

### Anchor tasks

Each wave has a set of **anchor tasks** that are explicitly enumerated upfront. Anchor tasks are chosen for:

- Major unblockers (trait freezes, interface finalizations, design tasks that gate large portions of the wave)
- High-risk or high-uncertainty work (novel algorithms, performance unknowns, cross-cutting concerns)
- Tasks that touch multiple subsystems and benefit from being sequenced explicitly

Non-anchor tasks are filled in as the wave progresses, either by agents discovering work during implementation or by the human operator adding them. New non-anchor tasks must still satisfy the task rules above.

### Task numbering

Tasks are numbered by wave:

| Wave | Range                  |
|------|------------------------|
| 0    | TASK-001 — TASK-099    |
| 1    | TASK-100 — TASK-199    |
| 2    | TASK-200 — TASK-299    |
| 3    | TASK-300 — TASK-399    |
| 4    | TASK-400 — TASK-499    |
| 5    | TASK-500 — TASK-599    |
| 6    | TASK-600 — TASK-699    |
| 7    | TASK-700 — TASK-799    |

Anchor tasks typically take the low numbers in a wave's range. New non-anchor tasks take the next available number within the wave. Numbers are never reused — a cancelled task leaves its number retired.

### Wave 0 is a special case

Wave 0 is the only wave whose output is design documents rather than code. Its docs establish high-level direction (storage format strategy, query language shape, execution model, etc.) but deliberately leave implementation-level detail open. That detail is resolved by `[DESIGN]` tasks inside later waves. **Never add new design docs to Wave 0 after implementation has begun** — instead, file a `[DESIGN]` task in the appropriate wave.

### Design document layout

Design documents live under `docs/design/`, organized by subsystem for navigability:

```
docs/design/
├── storage/       segment format, encodings, compaction, ingest
├── language/      grammar productions, MATCH surface, cohorts, aliases
├── planner/       logical plan nodes, optimizer rules, cost model
├── operators/     sequence matcher strategies, sessionize, retention, aggregates
├── engine/        memory budget, spill-to-disk, fusion, cancellation
├── interfaces/    CLI, Python API, FFI surface
├── types/         type system extensions
├── benchmarks/    benchmark suite architecture, datasets, baselines
└── *.md           Wave 0 top-level direction docs
```

The subsystem directories roughly parallel the Wave 0 docs but don't have to map 1:1 — use whichever directory makes a note easiest to find later. Wave 0 direction docs stay at the `docs/design/` root.

---

## Wave 0: Design Phase

Single-session deep dives with human review. Produces design documents before implementation begins.

### TASK-001: Storage Format Design
**Output**: docs/design/storage-format.md
**Description**: Design the native segment format, entity-sorted columnar row-groups, segment metadata, comprehensive encoding layer (dictionary, delta, double-delta, bitpacking, RLE, constant, FSST, FOR, PFOR, ALP, frequency encoding, LZ4), near-zero-copy Arrow decode, late materialization, batch-only ingestion with batch IDs, time-window partitioning, entity-hash sharding, size-tiered compaction, zone maps, tombstone-based deletes (row/batch/entity), manifest catalog, concurrency between readers and compaction, database directory layout.

### TASK-002: Query Language Design
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
**Output**: docs/design/execution-model.md
**Description**: Pull-based iterator protocol with PhysicalOperator and EntityOperator traits, entity-aligned batches with sub-batch streaming, demand propagation and generic operator fusion, shard-per-thread parallelism, compaction scheduling, memory budget enforcement, error handling and cancellation, Python integration, per-query metrics.

### TASK-004: Sequence Matching Design
**Output**: docs/design/sequence-matching.md
**Description**: Thompson's NFA simulation, global time window enforcement, negation via poison transitions, repetition, $variable binding with independent match tracks, two match modes (FIRST/ALL), EMIT ALL for funnel analysis, tiered execution strategies (step counter fast path for linear patterns, full NFA for general), candidate deque propagation with deferred consumption, filter pushdown to scan layer, aggregation fusion at window expiry.

### TASK-005: Type System Design
**Output**: docs/design/type-system.md
**Description**: Supported data types (string, int, float, bool, timestamp, list, map), null handling, type coercion rules, schema declaration syntax, schema validation at plan construction time, Arrow type mapping.

### TASK-006: Planner Pipeline Design
**Output**: docs/design/planner-pipeline.md
**Description**: Complete compiler pipeline from AST to executable physical plan. Logical plan node catalog (Scan, Filter, Project, Match, Funnel, Aggregate, Sessionize, Retention, etc.), AST-to-logical-plan lowering (how pipe syntax desugars into a plan tree), optimizer rules (predicate pushdown, projection pruning, constant folding, filter-before-match reordering), physical planning (logical-to-physical mapping, strategy selection e.g. StepCounter vs NFA), DemandCapabilities propagation protocol (formalize the demand system referenced by execution-model.md and sequence-matching.md), schema validation algorithm (when type checking runs, how TypeErrors propagate, plan-time vs runtime checks), plan serialization/explain output.

**Open design questions**:
- Cost model: rule-based only or cost-based with cardinality estimates? If cost-based, where do statistics come from (zone maps, manifest metadata)?
- Optimizer rule ordering: fixed pass order or iterative until fixpoint?
- Multi-query optimization: can shared scan/filter subplans be detected and deduplicated across queries in the same session?
- Plan caching: should compiled physical plans be cached for repeated queries with different parameters?

---

## Wave 1: Foundation & Skeleton

**Goal.** By the end of Wave 1, `bqlite query "events"` parses, plans, executes against an auto-bootstrapped `events` table in a freshly created database directory, and returns an empty result set. Shared types ship, the v0 trait surface is frozen, every subsystem crate (core, ast, parser, planner, storage, operators, engine, cli) has a working stub, and CI + bench harness + logging are green.

**Scope exclusions.** The top-level `bqlite` re-export crate and `bqlite-ffi` already exist as compile-only scaffolds from initial crate creation and are not extended in Wave 1 — the public Rust and Python APIs land in Wave 6. The builder API mentioned historically in architecture docs is deferred; no Wave 1 task exists for it.

**Size.** 25 tasks.
**Parallelism.** 6-8 agents concurrent at peak.

Wave 1 is deliberately trait-heavy — it's the only wave where `[TRAIT]` is the norm rather than the exception. After Wave 1, the trait surface is frozen and any change requires a high-priority `[TRAIT]` task.

All 25 Wave 1 tasks are enumerated below (no placeholder slots — Wave 1 is the next thing to execute, so it is fully planned).

### TASK-101: [IMPL] Dependency direction check
**Output**: scripts/check-dep-direction.sh, .github/workflows/ci.yml
**Depends on**: none
**Description**: Script walks each crate's Cargo.toml, verifies internal deps match the dependency graph in docs/architecture.md, and fails with a clear error on violation. Wire as a CI step alongside the existing build/test/clippy/fmt jobs.

### TASK-102: [IMPL] Error type hierarchy
**Output**: crates/bqlite-core/src/error.rs
**Depends on**: none
**Description**: `thiserror`-based `BqliteError` enum in bqlite-core, re-exported from the top-level crate. Covers I/O, schema mismatches, parse errors, plan errors, execution errors, cancellation. Conversion impls from `std::io::Error` and `arrow::error::ArrowError`.

### TASK-103: [IMPL] Timestamp and time-range types
**Output**: crates/bqlite-core/src/time.rs
**Depends on**: none
**Description**: `Timestamp` newtype over `i64` epoch nanoseconds, UTC, matching the `Timestamp(Nanosecond, Some("UTC"))` Arrow mapping frozen in docs/design/type-system.md §2.1-§2.2. `TimeRange` with inclusive/exclusive bounds, ordering, arithmetic helpers (duration math stays in i64 nanos — no Duration type per type-system.md §2.2). Serde impls for debug/logging.

### TASK-104: [IMPL] PropertyValue type
**Output**: crates/bqlite-core/src/property.rs
**Depends on**: TASK-102
**Description**: Scalar variants (bool, int, float, string, timestamp), null, list, map. Follows the type system defined in docs/design/type-system.md. Includes equality, ordering, and Display impls.

### TASK-105: [IMPL] EntityId and Event primitives
**Output**: crates/bqlite-core/src/event.rs
**Depends on**: TASK-103, TASK-104
**Description**: `EntityId` newtype, `Event { entity, timestamp, type, properties }`, and an entity-aligned iteration trait that later operators implement. Zero-copy where possible.

### TASK-106: [IMPL] TableSchema and OperatorSchema
**Output**: crates/bqlite-core/src/schema.rs
**Depends on**: TASK-104
**Description**: Two schema types per docs/design/type-system.md §5:
- `ColumnDef` + `TableSchema` — declared table shape, designated entity-id column, timestamp column, event-type column, per-column property schema, `__seq_id`/`__batch_id` system columns, and the schema-creation-time validation rules (§5.1).
- `OperatorSchema` — the contract between piped operators (§5.2): ordered `Vec<ColumnDef>` output shape, `column(name)` lookup, `to_arrow_schema()`, and `validate_against(required)` compatibility check.

Both types are foundational: TableSchema is what the catalog (TASK-125) returns, OperatorSchema is what the planner propagates through the plan tree (used by TASK-108 and the Wave 2 logical-plan work).

### TASK-107: [IMPL] Arrow type mapping
**Output**: crates/bqlite-core/src/arrow.rs
**Depends on**: TASK-104, TASK-106
**Description**: Bidirectional conversion between `PropertyValue`/`TableSchema` and Arrow `DataType`/`Schema`. Handles nested types (list, map) and null semantics.

### TASK-108: [DESIGN][TRAIT] PhysicalOperator + EntityOperator traits
**Output**: docs/design/operators/operator-traits.md, crates/bqlite-operators/src/operator.rs
**Depends on**: TASK-105, TASK-106
**Description**: The core execution contract. Design note first checkpoint covers: pull-based iterator protocol, `OperatorSchema` propagation, open/next/close lifecycle, error propagation, cancellation hook, the entity-aligned batching layer `EntityOperator` adds on top, and sub-batch streaming. Impl second checkpoint lands the trait definitions in `bqlite-operators` per docs/design/execution-model.md §13.2 module map — the trait cannot live in `bqlite-engine` because `bqlite-operators` does not depend on `bqlite-engine`, so placing the trait in engine would block operator impls (TASK-117) from implementing it. Also file a follow-up doc task to correct docs/design/planner-pipeline.md §15 line 1397 which inconsistently lists the trait in `bqlite-engine`. Merge-first — later Phase D tasks depend on this.

### TASK-109: [DESIGN][TRAIT] SegmentReader trait
**Output**: docs/design/storage/reader-trait.md, crates/bqlite-core/src/storage.rs
**Depends on**: TASK-106, TASK-107
**Description**: Storage API consumed by scan operators. Design note covers: segment enumeration, column projection, row-group iteration, zone-map access hook, predicate pushdown hook. Impl lands the trait. Merge-first.

### TASK-110: [TRAIT] DemandCapabilities protocol scaffold
**Output**: crates/bqlite-core/src/demand.rs
**Depends on**: TASK-108
**Description**: Placeholder enum + propagation trait so operator stubs can implement it from day 1. Real protocol details (which capabilities exist, how they propagate, the fusion implications) are resolved by a [DESIGN] task in a later wave. Keep the v0 surface minimal so we can extend without breaking existing impls.

### TASK-111: [TRAIT] MemoryBudget trait
**Output**: crates/bqlite-core/src/memory.rs
**Depends on**: TASK-102
**Description**: Byte-accounting trait, reservation API, spill notification hook. Stub enforcement only — the real enforcement model is designed in Wave 5.

### TASK-112: [TRAIT] Metrics trait
**Output**: crates/bqlite-core/src/metrics.rs
**Depends on**: TASK-108
**Description**: Per-operator metric counters (rows in, rows out, bytes, wall time), query-level aggregation hook. Designed to compose with the telemetry setup in TASK-122.

### TASK-113: [IMPL] AST node skeletons
**Output**: crates/bqlite-ast/src/lib.rs
**Depends on**: TASK-104
**Description**: Statement, expression, pattern, and source-reference AST types. Covers enough surface for Wave 2's grammar to slot in without restructuring. No parser logic — just the data types.

### TASK-114: [IMPL] Parser stub
**Output**: crates/bqlite-parser/src/lib.rs
**Depends on**: TASK-113
**Description**: Hand-rolled mini-parser accepting a single identifier — a bare table name like `events` — and producing the corresponding `Scan { table: "events" }` AST node. No keywords, no operators, no error recovery. This is deliberately throwaway; the real grammar framework is a Wave 2 [DESIGN] task.

### TASK-115: [IMPL] Planner stub
**Output**: crates/bqlite-planner/src/lib.rs
**Depends on**: TASK-113, TASK-125
**Description**: AST → logical plan stub → physical plan stub. `plan(statement, catalog: &dyn Catalog)` entry point resolves the scanned table via the catalog (returning a `TypeError` for unknown tables, per planner-pipeline.md §4.1), builds a minimal logical node enum (just `Scan { schema: TableSchema }` for now), and lowers it one-to-one to a plain-data physical descriptor (`ScanPhysical`) per planner-pipeline.md §15 — the planner emits plain data, not trait objects. No optimizer pass. The returned physical descriptor is consumed by the engine's bind step (TASK-118). Does not depend on TASK-108 directly because the planner never holds a `PhysicalOperator` value.

### TASK-116: [IMPL] Storage stub and database bootstrap
**Output**: crates/bqlite-storage/src/{lib,database,manifest}.rs
**Depends on**: TASK-106, TASK-109
**Description**: `Database::open_or_create(path)` implements the full v0 database-open contract from docs/design/storage-format.md §5 + §12 + §14 and docs/reliability.md — even though nothing is stored yet, Wave 1 freezes the on-disk shape so later waves don't have to retrofit it:

- **Directory layout.** Create `<path>/`, `<path>/manifest.json`, and acquire `<path>/.lock` via `flock()` (storage-format.md §14.1). Release the lock on drop. A second concurrent open returns a clear error.
- **Manifest contents on empty-database init.** Write `manifest.json` with: `format_version: 1` (reliability.md §Versioning), a freshly generated `database_uuid` (v4, never rotates — storage-format.md §5.1), `shard_count: 32` with override hook for future `bqlite init --shards N`, an empty `tables: {}` map (populated by TASK-125's bootstrap), per-table counters `{ next_seq_id: 0, next_batch_id: 0 }` ready to be added, and a `segments: []` inventory.
- **Manifest atomicity.** Writes go `manifest.json.tmp` → `fsync` → `rename` per storage-format.md §12.3.
- **Open behavior.** Existing databases load `manifest.json` and validate `format_version` (rejecting unknown versions). Empty or missing directory triggers init. Corrupted manifest returns a typed error.
- **SegmentReader.** Implements TASK-109's trait returning an empty segment iterator. No real format yet.

The smoke test (TASK-123) depends on this: it creates a fresh temp directory, opens it, and must observe a valid, versioned, UUID-stamped manifest that later waves can keep extending.

### TASK-117: [IMPL] Operator stubs
**Output**: crates/bqlite-operators/src/{scan,filter,project}.rs
**Depends on**: TASK-108, TASK-109
**Description**: Scan/filter/project operators implementing `PhysicalOperator`. Scan actually calls into `SegmentReader::segments()` and drives the iterator (not hard-coded to return empty). Filter and project wrap a child operator and are no-ops in this stub. Gives downstream planner and engine real types to wire to.

### TASK-118: [IMPL] Engine stub, query API, and physical-plan bind step
**Output**: crates/bqlite-engine/src/{lib,query,bind}.rs, crates/bqlite-engine/Cargo.toml, docs/architecture.md, CLAUDE.md
**Depends on**: TASK-114, TASK-115, TASK-117, TASK-125
**Description**: Engine's public `Engine::query(text: &str, db: &Database) -> Result<ExecutionResult>` entry point — the single surface the CLI, Python bindings, and eventually the top-level `bqlite` crate call. Internally it:

1. **Parses** the text via `bqlite-parser` into a `Statement` (Wave 1 accepts the one-identifier grammar from TASK-114).
2. **Plans** by calling `bqlite-planner` with the database's `&dyn Catalog` (from TASK-125), producing the plain-data physical descriptor per planner-pipeline.md §15.
3. **Binds** the plain-data descriptor into a `Box<dyn PhysicalOperator>` tree. The bind step lives in engine per planner-pipeline.md §15 line 1404 — planner never holds trait objects. For Wave 1 the only binding is `ScanPhysical` → `bqlite_operators::ScanOperator`.
4. **Drives** the resulting operator tree to completion, collecting output batches, returning `ExecutionResult { schema: OperatorSchema, rows: Vec<RecordBatch> }`.

No memory management, no concurrency, no cancellation yet.

**Crate-boundary change.** Adds `bqlite-parser` to `bqlite-engine`'s `Cargo.toml` and updates the dependency graphs in `docs/architecture.md` and `CLAUDE.md` to show `bqlite-engine → parser, planner, operators, storage, core`. This preserves the `bqlite-cli → engine` constraint (architecture.md line 30) while giving engine a single text-in, rows-out API — without this, CLI would need direct parser/planner deps, which the architecture forbids. TASK-101's dep-direction check must be updated in the same PR.

### TASK-119: [IMPL] CLI stub
**Output**: crates/bqlite-cli/src/main.rs
**Depends on**: TASK-118, TASK-122
**Description**: `bqlite query "<bql>" --db <path>` subcommand. Opens the database via `bqlite_engine::Database::open_or_create(path)`, calls `engine.query(text, &db)` — the single text-in entry point from TASK-118 — and prints the `ExecutionResult` as a simple text table (even if empty: "0 rows"). CLI only depends on `bqlite-engine` per architecture.md line 30; it does not import `bqlite-parser` or `bqlite-planner` directly. Initializes the tracing subscriber from TASK-122.

### TASK-120: [IMPL] Integration test fixture framework
**Output**: tests/common/mod.rs
**Depends on**: TASK-106
**Description**: Temp-dir database helpers, fixture loader stub (CSV support lands in Wave 2 — Wave 1 just provides the harness), assertion helpers that compare result sets by value. Documents the integration-test pattern that later waves copy.

### TASK-121: [IMPL] Benchmark harness
**Output**: benches/README.md, benches/common/mod.rs, root Cargo.toml bench entries
**Depends on**: TASK-101
**Description**: Criterion set up in the workspace, per-crate bench harness pattern documented so later waves drop microbenchmarks in without thinking about it. Smoke benchmark that measures a no-op so the harness itself is exercised in CI.

### TASK-122: [IMPL] Logging and tracing setup
**Output**: crates/bqlite-core/src/telemetry.rs
**Depends on**: TASK-102
**Description**: `tracing` crate wiring — env-controlled level (`BQLITE_LOG`), a `tracing_subscriber` that writes to stderr, a query-level span with structured fields (query_id, query_text), operator-level child spans. CLI initializes the subscriber at startup. Later waves extend this with the metrics-to-span bridge, so getting the surface right now avoids a [TRAIT] task later.

### TASK-123: [IMPL] End-to-end smoke test
**Output**: tests/smoke.rs
**Depends on**: TASK-119, TASK-120
**Description**: Runs `bqlite query "events"` against an empty database directory (created on the fly) and asserts OK + empty result. This is the Wave 1 acceptance gate — if this passes, Wave 1 is done.

### TASK-124: [IMPL] Property-test harness
**Output**: tests/prop/mod.rs, tests/prop/property_value.rs
**Depends on**: TASK-104
**Description**: Adds `proptest` as a dev-dep, writes one round-trip test on `PropertyValue` as a template, documents the pattern in tests/prop/README.md. Later waves add real property tests for storage encodings, parser round-trips, and the sequence matcher.

### TASK-125: [IMPL] Catalog trait and bootstrap events table
**Output**: crates/bqlite-core/src/catalog.rs, crates/bqlite-storage/src/catalog.rs
**Depends on**: TASK-106, TASK-116
**Description**: Resolves the gap between "the planner requires a `Catalog` handle to resolve tables" (planner-pipeline.md §4.1 line 200) and "database initialization is CLI-only, no BQL DDL in v0" (query-language.md §29 line 1911) — without which the Wave 1 smoke test `bqlite query "events"` cannot parse-plan-execute.

- **`Catalog` trait in `bqlite-core`.** Minimal surface: `resolve_table(name: &str) -> Result<TableSchema, TypeError>`, `list_tables() -> Vec<&str>`. The planner takes `&dyn Catalog` at plan time and never depends on storage directly (preserving the planner → ast, core dependency rule).
- **Manifest-backed impl in `bqlite-storage`.** Reads the `tables: { <name>: TableSchema }` map from the manifest written by TASK-116. The impl is the value returned by `Database::catalog() -> &dyn Catalog`.
- **Bootstrap rule.** When `Database::open_or_create(path)` initializes a fresh manifest (TASK-116), it seeds the `tables` map with a single default `events` table whose schema is the minimum required by type-system.md §5.1 validation: `entity_id STRING NOT NULL (ENTITY KEY)`, `ts TIMESTAMP NOT NULL (EVENT TIME)`, `event_type STRING NOT NULL (EVENT TYPE)`. This is a Wave 1 shortcut — proper `CREATE TABLE` DDL execution is Wave 2's parser + planner work. The bootstrap rule is documented in the manifest as `bootstrap_events_table: true` so later waves can distinguish seeded state from user state and retire the shortcut cleanly.

Unlocks: TASK-115 planner stub (needs `Catalog` to resolve `events`), TASK-118 engine query API (wires catalog into the planner call), TASK-123 smoke test (needs a resolvable `events` table in a freshly created database).

---

## Wave 2: Scan & Filter MVP

**Goal.** Real queries return real data. Segment format v1, CSV ingest, scan + filter + project, minimal logical and physical planner.
**Size.** ~28-34 tasks.
**Parallelism.** 8-12 agents.
**Acceptance.** `bqlite query "events | filter event_type = 'click'"` against an ingested CSV returns correct rows.

Anchors below are deliberately front-loaded with design tasks — Wave 2 is where the real interfaces get decided. Non-anchor tasks (individual encodings, individual parser productions, additional logical nodes, CSV edge cases, etc.) are added as the wave kicks off and as these anchors resolve.

### TASK-201: [DESIGN] Segment format v1
**Output**: docs/design/storage/segment-format-v1.md
**Depends on**: TASK-109
**Description**: V1 scope only: dictionary, delta, bitpacking, LZ4, constant. No ALP/FSST/PFOR (those are Wave 4). Defines the on-disk layout, column encoding descriptor, zone map block, row-group index, and file footer. Unblocks every other Wave 2 storage task.

### TASK-202: [DESIGN] Scan interface and predicate pushdown protocol
**Output**: docs/design/storage/predicate-pushdown.md
**Depends on**: TASK-109, TASK-201
**Description**: How the scan operator asks the storage layer to push down equality, range, and set predicates. Zone-map evaluation order, fallback to post-filter, cost/benefit heuristics. Cross-cutting between storage, operators, and planner — risky.

### TASK-203: [DESIGN] Parser grammar framework
**Output**: docs/design/language/grammar-framework.md
**Depends on**: TASK-114
**Description**: Decides hand-rolled vs parser generator (chumsky/pest/lalrpop/nom), error-recovery strategy, span tracking for diagnostics, how new productions are added. Unblocks every post-stub parser task across Waves 2-4.

### TASK-204: [DESIGN] Logical plan node catalog
**Output**: docs/design/planner/logical-plan-nodes.md
**Depends on**: TASK-115
**Description**: Comprehensive enumeration of every logical plan node expected across the project (Scan, Filter, Project, Match, Funnel, Aggregate, Sessionize, Retention, Limit, Sort, Distinct, Cohort, etc.), their input/output schemas, the AST constructs that lower to them, and the rewrites that apply to them. Aims to be comprehensive but will be revised as later waves reveal missing pieces.

### TASK-205: [IMPL] Encoding trait + dictionary + delta reference implementations
**Output**: crates/bqlite-storage/src/encoding/{mod,dictionary,delta}.rs
**Depends on**: TASK-201
**Description**: The encoding trait + two reference implementations. Establishes the pattern that every other encoding in Waves 2 and 4 follows. Includes property tests (via TASK-124 harness) for encode/decode round trips.

### TASK-206: [IMPL] Entity-sorted segment writer v1
**Output**: crates/bqlite-storage/src/writer.rs
**Depends on**: TASK-201, TASK-205
**Description**: Accepts a sorted stream of events, groups by entity, encodes columns per the v1 format, writes segments to disk, updates the manifest. Pairs with TASK-201.

### TASK-207: [IMPL] Entity-sorted scan operator
**Output**: crates/bqlite-operators/src/scan.rs (replaces Wave 1 stub)
**Depends on**: TASK-202, TASK-206
**Description**: Full entity-sorted scan over real segments. Pushdown hook wired. Foundation for every temporal operator in later waves.

### TASK-208: [IMPL] CSV ingest pipeline
**Output**: crates/bqlite-storage/src/ingest/csv.rs
**Depends on**: TASK-206
**Description**: Stream a CSV file, map columns to a declared schema, sort by (entity_id, timestamp), emit batches into the segment writer. Handles schema mismatch errors cleanly.

Additional Wave 2 tasks (individual parser productions, individual logical plan nodes, individual optimizer-free physical lowerings, zone map implementation, manifest format, additional encodings, filter expression evaluator, projection implementation, error-recovery paths, CSV edge cases, CLI ingest subcommand, etc.) will be added as the anchors resolve and implementation reveals the specifics. Target total: 28-34 tasks.

---

## Wave 3: Pattern Matching MVP

**Goal.** Funnel queries work end-to-end. MATCH operator, aggregates, limit, sort.
**Size.** ~22-28 tasks.
**Parallelism.** 6-10 agents.
**Acceptance.** A 3-step funnel query over an ingested CSV returns correct conversion counts per step.

### TASK-301: [DESIGN] MATCH operator architecture
**Output**: docs/design/operators/match-operator.md
**Depends on**: TASK-204, TASK-207
**Description**: Connects the Wave 0 sequence-matching direction to an actual operator implementation. Operator state, input expectations, output schema, how bindings are surfaced, EMIT ALL semantics. Risky.

### TASK-302: [DESIGN] Sequence matcher strategy selection
**Output**: docs/design/operators/matcher-strategy.md
**Depends on**: TASK-301
**Description**: When the step-counter fast path applies vs the general NFA path. Selection rules at plan time, fallback behavior, performance expectations for each. Risky.

### TASK-303: [DESIGN] Pattern AST and MATCH grammar production
**Output**: docs/design/language/pattern-grammar.md
**Depends on**: TASK-203
**Description**: Pattern syntax, repetition/negation/variable-binding AST nodes, how patterns compose with pipe stages. Unblocks all parser MATCH work.

### TASK-304: [IMPL] NFA builder + Thompson simulation
**Output**: crates/bqlite-operators/src/matcher/nfa.rs
**Depends on**: TASK-301
**Description**: General NFA path — Thompson construction from pattern AST, simulation with poison transitions for negation, time window enforcement. Risky core.

### TASK-305: [IMPL] Step counter fast path
**Output**: crates/bqlite-operators/src/matcher/step_counter.rs
**Depends on**: TASK-302
**Description**: Fast path for linear patterns — no NFA, just a step counter with time-window checks. Implementation parallel to TASK-304.

### TASK-306: [IMPL] Variable binding tracks
**Output**: crates/bqlite-operators/src/matcher/bindings.rs
**Depends on**: TASK-304
**Description**: Independent match tracks for $-variable bindings per the Wave 0 sequence-matching design. Risky — semantics are subtle.

### TASK-307: [IMPL] Hash aggregate operator
**Output**: crates/bqlite-operators/src/aggregate.rs
**Depends on**: TASK-108, TASK-207
**Description**: Foundational operator for funnel output and most analytics queries. count/sum/avg/min/max/distinct_count. Works on columnar batches.

Additional Wave 3 tasks: individual pattern grammar productions, limit/sort operators, EMIT ALL output assembly, MATCH lowering in the planner, matcher microbenchmarks, integration tests for common funnel shapes.

---

## Wave 4: Advanced Analytics

**Goal.** All major query primitives working: cohorts, sessionize, retention, attribution, full encoding suite, compaction, tombstones, JSON/Parquet ingest.
**Size.** ~35-45 tasks.
**Parallelism.** 10-15 agents.
**Acceptance.** Retention curves, cohort-joined funnels, sessionized aggregates, and attribution queries all run against compacted segments with live deletes.

### TASK-401: [DESIGN] Advanced encoding research
**Output**: docs/design/storage/advanced-encodings.md
**Depends on**: TASK-201
**Description**: Reference implementations + microbenchmarks for FSST, ALP, PFOR, FOR, double-delta, RLE, frequency encoding. Very risky — some may not be worth shipping. Deliverable is a go/no-go recommendation per encoding with evidence.

### TASK-402: [DESIGN] Encoding selection policy
**Output**: docs/design/storage/encoding-selection.md
**Depends on**: TASK-401
**Description**: Given the encodings that survived TASK-401, the policy the writer uses to pick an encoding per column. Sampling strategy, per-type defaults, override syntax.

### TASK-403: [DESIGN] Compaction concurrency protocol
**Output**: docs/design/storage/compaction-concurrency.md
**Depends on**: TASK-201
**Description**: How readers and compaction coexist without locking. Snapshot semantics, manifest-swap protocol, merge-scan integration. Risky and cross-cutting.

### TASK-404: [DESIGN] Tombstone and delete semantics
**Output**: docs/design/storage/deletes.md
**Depends on**: TASK-201, TASK-403
**Description**: Row/batch/entity-level deletes, merge-scan integration, reclaim during compaction.

### TASK-405: [DESIGN] SESSIONIZE operator
**Output**: docs/design/operators/sessionize.md
**Depends on**: TASK-108
**Description**: Session boundary definitions, inactivity gap, custom predicates, output schema.

### TASK-406: [DESIGN] Attribution operator
**Output**: docs/design/operators/attribution.md
**Depends on**: TASK-301
**Description**: Which prior events caused which subsequent outcomes. Models: first-touch, last-touch, linear, time-decay, positional. Output schema.

### TASK-407: [DESIGN] Cohort materialization and alias binding
**Output**: docs/design/language/cohorts-and-aliases.md
**Depends on**: TASK-204
**Description**: Resolves the open questions from TASK-002 — inline vs materialized cohorts, alias scoping, cohort × query join semantics, eager vs lazy evaluation.

### TASK-408: [IMPL] Compaction scheduler
**Output**: crates/bqlite-storage/src/compaction.rs
**Depends on**: TASK-403
**Description**: Size-tiered compaction with time-window partitioning. Implements the concurrency protocol from TASK-403. Risky.

### TASK-409: [IMPL] Cohort materialization runtime
**Output**: crates/bqlite-operators/src/cohort.rs, crates/bqlite-planner/src/cohort.rs
**Depends on**: TASK-407
**Description**: Materialized cohort support per TASK-407. Planner integration for cohort joins. Operator for cohort membership evaluation.

### TASK-410: [IMPL] JSON and Parquet ingest paths
**Output**: crates/bqlite-storage/src/ingest/{json,parquet}.rs
**Depends on**: TASK-208
**Description**: Follows the CSV ingest pattern. Parquet path reuses Arrow decode; JSON path handles nested property objects.

Additional Wave 4 tasks: individual encoding implementations from TASK-401 outcomes, SESSIONIZE impl, retention operator, attribution impl, cohort grammar productions, alias binding in planner, FUNNEL and RETENTION syntactic sugar, tombstone writer, tombstone-aware merge scan, compaction microbenchmarks, integration tests for each new feature.

---

## Wave 5: Production Quality & Performance

**Goal.** Bounded memory under load, spill-to-disk, complete optimizer, fused operators, cancellation, microbench-tuned.
**Size.** ~22-28 tasks.
**Parallelism.** 6-10 agents.
**Acceptance.** Queries over billion-event datasets complete under a 4 GB memory budget. Benchmark regressions gate CI.

### TASK-501: [DESIGN] Memory budget enforcement model
**Output**: docs/design/engine/memory-budget.md
**Depends on**: TASK-111
**Description**: Every operator's reservation contract, overcommit policy, spill triggers, error handling on OOM. Cross-cutting — touches every operator.

### TASK-502: [DESIGN] Spill-to-disk protocol
**Output**: docs/design/engine/spill.md
**Depends on**: TASK-501
**Description**: Which operators spill, spill file format, spill directory management, resume semantics. Risky.

### TASK-503: [DESIGN] Operator fusion
**Output**: docs/design/engine/operator-fusion.md
**Depends on**: TASK-110
**Description**: Which operators fuse, the fusion rewriter's place in the planner, code generation vs template strategies, DemandCapabilities integration. Risky.

### TASK-504: [DESIGN] Cost model and statistics source
**Output**: docs/design/planner/cost-model.md
**Depends on**: TASK-204
**Description**: Resolves the open questions from TASK-006 — rule-based vs cost-based, where statistics come from (zone maps, manifest metadata), how cardinality is estimated, which optimizer rules become cost-gated. Risky.

### TASK-505: [DESIGN] Cancellation and timeout protocol
**Output**: docs/design/engine/cancellation.md
**Depends on**: TASK-108
**Description**: How cancellation propagates through the operator tree, cleanup responsibilities, how timeouts are enforced, how cancellation interacts with spilled state.

### TASK-506: [IMPL] Optimizer rule set
**Output**: crates/bqlite-planner/src/optimizer/
**Depends on**: TASK-204, TASK-504
**Description**: The full optimizer rule set — predicate pushdown, projection pruning, constant folding, filter-before-match reordering, match-pushdown to scan, cost-gated fusion decisions. Each rule is its own sub-task filled in as Wave 5 progresses.

### TASK-507: [IMPL] Per-operator microbenchmark coverage audit
**Output**: benches/coverage-report.md
**Depends on**: TASK-121
**Description**: Audit every operator introduced in Waves 2-4 for microbenchmark coverage. File follow-up tasks for any missing benches. Ensures the core belief "microbenchmark frequently" has actually happened.

Additional Wave 5 tasks: individual optimizer rule implementations, spill implementations per spillable operator, fusion implementations for specific operator pairs, cancellation plumbing per operator, property tests, stress tests, memory-pressure integration tests.

---

## Wave 6: Interfaces

**Goal.** Embeddable via CLI, Python, and C ABI.
**Size.** ~15-20 tasks.
**Parallelism.** 4-6 agents.
**Acceptance.** `pip install bqlite` runs on macOS and Linux, CLI subcommands work against a real database.

### TASK-601: [DESIGN] Python API surface
**Output**: docs/design/interfaces/python-api.md
**Depends on**: TASK-118
**Description**: Idiomatic Python wrapper over the engine API. Query, ingest, iterate results, type coercion, error mapping, async support question.

### TASK-602: [DESIGN] CLI command structure
**Output**: docs/design/interfaces/cli.md
**Depends on**: TASK-119
**Description**: `query`, `ingest`, `explain`, `repl`, `compact`, `stats` subcommands, flag conventions, output formats.

### TASK-603: [IMPL] PyO3 integration skeleton
**Output**: crates/bqlite-ffi/src/python.rs, python/bqlite/__init__.py
**Depends on**: TASK-601
**Description**: PyO3 module declaration, Event type binding, Database and Query type wrappers, result iteration, error translation.

### TASK-604: [IMPL] C ABI surface
**Output**: crates/bqlite-ffi/src/c.rs
**Depends on**: TASK-118
**Description**: Stable, versioned C ABI. Opaque handle types, error-code returns, minimal allocation ownership rules.

Additional Wave 6 tasks: CLI subcommand implementations, repl line editing, explain output formatting, Python test suite, Python packaging (wheels for macOS/Linux), example scripts.

---

## Wave 7: Polish

**Goal.** Shippable: benchmarks, docs, error quality, edge cases.
**Size.** ~10-15 tasks.
**Parallelism.** 4-6 agents.
**Acceptance.** Public benchmark numbers, a complete getting-started guide, documented error taxonomy, edge-case test audit closed.

### TASK-701: [DESIGN] End-to-end benchmark suite
**Output**: docs/design/benchmarks/suite.md
**Depends on**: TASK-121
**Description**: Datasets, query mix, comparison baselines, reporting format. Drives the public benchmark story.

### TASK-702: [DESIGN] User documentation plan
**Output**: docs/design/interfaces/docs-plan.md
**Depends on**: TASK-601, TASK-602
**Description**: Getting started, query language guide, operator reference, API reference, FAQ. Audience segmentation and reading order.

### TASK-703: [IMPL] Error message audit
**Output**: tests/errors/
**Depends on**: TASK-102
**Description**: Every user-facing error has a test asserting the message is clear, actionable, and mentions the source location where applicable.

### TASK-704: [IMPL] Edge case audit
**Output**: tests/edge_cases/
**Depends on**: TASK-120
**Description**: Systematic review of edge cases — empty datasets, single-event entities, segment boundary crossings, huge entities, zero-time-range queries, schema mismatches, ingest partial failures.

Additional Wave 7 tasks: benchmark dataset acquisition, benchmark runner, public benchmark report, getting-started guide, query language guide, operator reference, error taxonomy document, README polish.
