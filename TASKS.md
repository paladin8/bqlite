# bqlite — Task List

This file is the authoritative plan for bqlite. It lists every wave and every known task, and is the reference both humans and autonomous agents consult to decide what to build next.

**It does not track task status.** Lock files in `tasks/active/` and done markers in `tasks/completed/` are the source of truth for what is claimed and what is complete. For the execution protocol — task claiming, checkpoints, git workflow — see [AGENTS.md](AGENTS.md).

The plan will be revised as work progresses. Later waves are intentionally loose; they get fleshed out as earlier waves ship and we learn what was actually needed.

> **Structural rule: waves are flat task lists, never sub-organized.** Do not group tasks within a wave under "phases", "tracks", "milestones", or any other sub-heading. Each task stands alone with its `Depends on:` set — that's the only structure. Agents pick work by scanning for unclaimed tasks whose dependencies are satisfied; any extra hierarchy gets in the way of that scan and invites drift between the grouping and the real dependency graph. When a wave starts to feel like it needs sub-sections, that's a signal to either (a) split it into two waves or (b) trust the dependency edges to do the organizing.

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
5. **Sequential across waves.** A wave does not begin until the previous wave is fully complete — every task in the prior wave has a `.done` marker in `tasks/completed/` and the wave's acceptance gate has been verified. This serializes inter-wave dependencies, makes acceptance gates meaningful, and prevents agents from claiming tasks whose upstream foundations are still in flight. Within a wave, tasks parallelize freely subject to their own `Depends on:` sets.

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

Anchor tasks typically take the low numbers in a wave's range. New non-anchor tasks take the next available number within the wave. Numbers are never reused — a cancelled task leaves its number retired. The final number in each wave range (199, 299, 399, ...) is reserved for that wave's quality audit task, which updates `docs/quality-score.md` as a wave-closing reflective pass.

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
**Description**: Native segment format, entity-sorted columnar row-groups, segment file structure with versioning and checksums, v1 encoding layer (dictionary, delta, bitpacking, constant, LZ4) with extension points for deferred encodings (FSST, ALP, PFOR, FOR, frequency, double-delta, RLE) researched in Wave 4, near-zero-copy Arrow decode, late materialization, batch-only ingestion with `__seq_id` / `__batch_id`, time-window partitioning, entity-hash sharding, size-tiered compaction with manifest-swap atomicity, k-way merge read path, zone maps (bloom filters deferred), tombstone-based deletes (row/batch/entity), manifest catalog with UUID-stamped database identity, reader/compaction concurrency via `flock` + immutable segments, list/map column encoding, database directory layout.

### TASK-002: Query Language Design
**Output**: docs/design/query-language.md
**Description**: Complete BQL grammar, pipeline composition rules, event-type quoting, property predicate and variable binding syntax, time and duration literals, expression language, error-message strategy, and the full per-operator surface:
- **MATCH**: sequence patterns with named steps, property predicates, time windows, negation (WITHOUT), alternation, repetition, IMMEDIATELY, variable bindings, BRACKETS (retention time slicing), match modes (FIRST / ALL / EMIT ALL).
- **FUNNEL / RETENTION**: convenience sugar desugared in the planner to MATCH + STATS.
- **Pipeline operators**: SESSIONIZE, STATS (aggregation + GROUP BY), WHERE, SELECT, LET, CASE, OVER (window functions), ORDER BY, LIMIT, PIVOT, IN (set membership with inline or subquery right-hand side).
- **Entity operators**: FIRST / LAST / NTH / SAMPLE / ATTRIBUTE — the per-entity event references that cover event sub-selection.
- **Aliases**: `name = pipeline` declarations that let named intermediate results compose into multi-stage analyses; cohort-style semi-joins are expressed via `IN QUERY alias`, and entity-level joins via cross-table JOIN.
- **DML / DDL**: INSERT, DELETE, CREATE TABLE, DESCRIBE, EXPLAIN.
- **Error strategy**: parser halts on first error; categorized plan-time type errors.

Resolved design questions (including cohort materialization, alias scoping, event sub-selection output, and cross-step property access) are recorded in §30 of the delivered doc.

### TASK-003: Execution Model Design
**Output**: docs/design/execution-model.md
**Description**: Hybrid push/pull iterator protocol with PhysicalOperator and EntityOperator traits, entity-aligned RecordBatches with sub-batch streaming for oversized entities, layered extraction for stateful operators, demand propagation driving generic operator fusion (including aggregation fusion via `finish_entity_into`), shard-per-thread parallelism with partial aggregation and final merge, compaction scheduling and interruptibility, memory budget enforcement with spill-to-disk, error propagation and query warnings, cancellation and timeout semantics, Python integration via PyO3, per-query metrics and span-based observability.

### TASK-004: Sequence Matching Design
**Output**: docs/design/sequence-matching.md
**Description**: Thompson's NFA simulation, global time window enforcement, negation via poison transitions, repetition, `$variable` bindings with independent match tracks per distinct value, match modes (MATCH FIRST, MATCH ALL non-overlapping, EMIT ALL for funnel-shape aggregation), tiered execution strategies (single-event bypass, step-counter fast path for linear patterns, full NFA for the general case) with a strategy-selection matrix, candidate deque propagation with deferred consumption and anchor consumption strategy, filter pushdown to the scan layer across three levels (event-type, property predicate, step predicate bitmask), aggregation fusion at window expiry with compact-step-counter state for the fused path, demand-driven output schema reduction, active-state and entity-event safety valves.

### TASK-005: Type System Design
**Output**: docs/design/type-system.md
**Description**: `BqlType` enum (string, int, float, bool, timestamp, list, map) with design rationale, nullability model under SQL three-valued logic, null propagation and COALESCE, implicit coercion rules and explicit `CAST`, arithmetic type rules, `TableSchema` and `OperatorSchema` with schema evolution rules, per-operator output schemas for every operator in the language (MATCH, FUNNEL, RETENTION, SESSIONIZE, STATS, WHERE, SELECT, FIRST/LAST/NTH, OVER, IN, PIVOT, SAMPLE, ORDER BY, LIMIT, ATTRIBUTE), bidirectional Arrow type mapping with round-trip guarantee, `PropertyValue` for dynamic typing at ingest, `CREATE TABLE` / `DESCRIBE` schema declaration syntax, scalar function catalog with type signatures, variable binding type inference, `TypeError` taxonomy, plan-construction-time validation sequence.

### TASK-006: Planner Pipeline Design
**Output**: docs/design/planner-pipeline.md
**Description**: Complete compiler pipeline from AST to executable physical plan. Parser output as a flat pipeline; planner converts it to a tree during lowering. Logical plan node catalog (Scan, Filter, Project, Match, Funnel, Retention, Sessionize, Stats, Attribute, Pivot, Order, Limit, Over, FusedDownstream, etc.) with schema computation rules. Integrated schema validation at construction time via `TypedExpr` (no separate type-check pass; a `LogicalPlan` value is provably valid). FUNNEL/RETENTION/LET desugaring in the planner. Six-pass rule-based optimizer (expression inlining, predicate pushdown, scan predicate extraction from MATCH, projection pruning via backward demand collection, constant folding, general fusion detection). General fusion framework targeting any stateful operator fused with an adjacent `(filter →)? aggregate`, with layered extraction and column forwarding driven by per-`(step, column)` demand bits. Physical planning with strategy selection (StepCounter vs NFA) emitting plain-data physical descriptors — `bqlite-planner` never holds `PhysicalOperator` trait objects; the engine's bind step materializes them. `DemandSet` propagation protocol formalizing the demand system referenced by execution-model.md and sequence-matching.md. Structured `ExplainNode` tree for CLI rendering. Rule-based (not cost-based) optimizer with fixed pass order; no multi-query optimization or plan caching in v1 — resolved questions recorded in §14 of the delivered doc.

---

## Wave 1: Foundation & Skeleton

**Goal.** By the end of Wave 1, `bqlite query "events"` parses, plans, executes against an auto-bootstrapped `events` table in a freshly created database directory, and returns an empty result set. Shared types ship, the v0 trait surface is frozen, every subsystem crate (core, ast, parser, planner, storage, operators, engine, cli) has a working stub, and CI + bench harness + logging are green.

**Scope exclusions.** The top-level `bqlite` re-export crate and `bqlite-ffi` already exist as compile-only scaffolds from initial crate creation and are not extended in Wave 1 — the public Rust and Python APIs land in Wave 6. The builder API mentioned historically in architecture docs is deferred; no Wave 1 task exists for it.

**Size.** 26 tasks.
**Parallelism.** 6-8 agents concurrent at peak.

Wave 1 is deliberately trait-heavy — it's the only wave where `[TRAIT]` is the norm rather than the exception. After Wave 1, the trait surface is frozen and any change requires a high-priority `[TRAIT]` task.

All 26 Wave 1 tasks are enumerated below (no placeholder slots — Wave 1 is the next thing to execute, so it is fully planned).

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
**Description**: The core execution contract. Design note first checkpoint covers: pull-based iterator protocol, `OperatorSchema` propagation, open/next/close lifecycle, error propagation, cancellation hook, the entity-aligned batching layer `EntityOperator` adds on top, and sub-batch streaming. Impl second checkpoint lands the trait definitions in `bqlite-operators` per docs/design/execution-model.md §13.2 module map — the trait cannot live in `bqlite-engine` because `bqlite-operators` does not depend on `bqlite-engine`, so placing the trait in engine would block operator impls (TASK-117) from implementing it. Also file a follow-up doc task to correct docs/design/planner-pipeline.md §15 line 1397 which inconsistently lists the trait in `bqlite-engine`. Merge-first — downstream operator, planner, and engine tasks depend on this.

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
**Description**: Adds `proptest` as a dev-dep, writes one round-trip test on `PropertyValue` as a template, documents the pattern in the `bqlite-tests` package README (now at tests/README.md after the post-TASK-124 restructure). Later waves add real property tests for storage encodings, parser round-trips, and the sequence matcher.

### TASK-125: [IMPL] Catalog trait and bootstrap events table
**Output**: crates/bqlite-core/src/catalog.rs, crates/bqlite-storage/src/catalog.rs
**Depends on**: TASK-106, TASK-116
**Description**: Resolves the gap between "the planner requires a `Catalog` handle to resolve tables" (planner-pipeline.md §4.1 line 200) and "database initialization is CLI-only, no BQL DDL in v0" (query-language.md §29 line 1911) — without which the Wave 1 smoke test `bqlite query "events"` cannot parse-plan-execute.

- **`Catalog` trait in `bqlite-core`.** Minimal surface: `resolve_table(name: &str) -> Result<TableSchema, TypeError>`, `list_tables() -> Vec<&str>`. The planner takes `&dyn Catalog` at plan time and never depends on storage directly (preserving the planner → ast, core dependency rule).
- **Manifest-backed impl in `bqlite-storage`.** Reads the `tables: { <name>: TableSchema }` map from the manifest written by TASK-116. The impl is the value returned by `Database::catalog() -> &dyn Catalog`.
- **Bootstrap rule.** When `Database::open_or_create(path)` initializes a fresh manifest (TASK-116), it seeds the `tables` map with a single default `events` table whose schema is the minimum required by type-system.md §5.1 validation: `entity_id STRING NOT NULL (ENTITY KEY)`, `ts TIMESTAMP NOT NULL (EVENT TIME)`, `event_type STRING NOT NULL (EVENT TYPE)`. This is a Wave 1 shortcut — proper `CREATE TABLE` DDL execution is Wave 2's parser + planner work. The bootstrap rule is documented in the manifest as `bootstrap_events_table: true` so later waves can distinguish seeded state from user state and retire the shortcut cleanly.

Unlocks: TASK-115 planner stub (needs `Catalog` to resolve `events`), TASK-118 engine query API (wires catalog into the planner call), TASK-123 smoke test (needs a resolvable `events` table in a freshly created database).

### TASK-199: [IMPL] Wave 1 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-123
**Description**: Wave-closing reflective pass on per-crate quality. Score every crate in the workspace on each dimension in docs/quality-score.md (Tests, API, Docs, Benchmarks) and assign an overall A-F grade. Gather evidence with `cargo test -p <crate>` (test count + pass rate), `cargo bench -p <crate> --no-run` (bench presence), and rustdoc coverage of public items. Record a one-line justification per cell — extend the table format if a flat grade cell is too terse to be useful. If any crate lands below C on any dimension, file a follow-up task addressing the gap (same or next wave) rather than silently accepting the grade. Wave 1 is not declared done until this audit lands and any below-C follow-ups are at least filed.

---

## Wave 2: Scan & Filter MVP

**Goal.** Real queries return real data over user-declared schemas. Segment format v1 with the full v1 encoding set, CSV ingest with column remapping, schema DDL (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`, `DESCRIBE`), `INSERT` (both `VALUES` and `FROM`), `EXPLAIN`, explicit `bqlite init` / split `Database::open` and `Database::create`, retirement of the Wave 1 bootstrap `events` table, scan + filter + select + limit operators, predicate pushdown, projection pruning, zone-map-based row-group skipping, startup reconciliation of orphaned segment files.

**Scope exclusions.** `DELETE` is deferred to Wave 4 alongside tombstones (TASK-404, TASK-410 territory) — the AST already models it, but without the tombstone format on disk there is nothing for the planner to lower it onto. Wave 2 parsers and planners therefore do not handle `DELETE`.

**Size.** ~43 tasks (TASK-242 retired during post-Wave-2 architecture reconciliation; see the task stub for rationale).
**Parallelism.** 10-14 agents at peak.

**Acceptance.** The following script runs end-to-end against a database created via `bqlite init /path/to/db` and returns the expected rows. Surface keywords match the grammar in query-language.md §26: `WHERE` for row filtering, `INSERT ... VALUES` for literal tuples, `WITH (k: v, ...)` option lists using `:` as the key/value separator.

```bql
CREATE TABLE purchases (
  user_id STRING ENTITY KEY,
  ts TIMESTAMP EVENT TIME,
  event STRING EVENT TYPE,
  amount FLOAT,
  country STRING
);

-- Small literal insert for REPL-style tests
INSERT INTO purchases VALUES
    ('u1', '2026-03-01T10:00:00Z', 'view',     12.50, 'US'),
    ('u1', '2026-03-01T10:05:00Z', 'checkout', 120.00, 'US');

-- Bulk load from file with column remapping
INSERT INTO purchases
FROM 'data.csv'
WITH (format: 'csv', map: (uid AS user_id, time AS ts, evt AS event));

purchases
| where event = 'checkout' AND amount > 100
| select user_id, ts, amount
| limit 100;

EXPLAIN purchases | where event = 'checkout' | select user_id;

DESCRIBE purchases;
ALTER TABLE purchases ADD COLUMN referrer STRING;
DROP TABLE purchases;
```

Source columns not named in the `map` clause pass through if their name matches a table column; otherwise INSERT errors. The CLI auto-injects `LIMIT 1000` when a query has no explicit limit and prints a truncation footer; `--no-limit` and `--limit N` override. A `bqlite query` call against a directory that does not yet contain a manifest returns a typed error pointing at `bqlite init`, not an implicit fresh database.

**Performance gate** (blocks wave acceptance; verified by TASK-236 on the reference dataset):

*Reference dataset:* 100M-row synthetic `purchases` stream — 10k distinct `user_id`, 20 distinct `event` types, timestamps spanning 90 days monotonic-within-entity, 7 property columns of mixed types (ints, floats, low-cardinality strings), materialized as CSV on local NVMe.

*Reference hardware:* Apple M3 Pro, 36GB RAM, macOS 14+, release build (`cargo bench --profile=release-lto`), `/tmp` on APFS SSD. CI gate runs on GitHub Actions `ubuntu-latest` (4 vCPU) with a **1.5× relaxed target** across all numbers below.

| Metric | Target |
|---|---|
| End-to-end acceptance query (cold cache, single thread) | **< 1 s** |
| Columnar scan decode, int64, no predicate | ≥ 200M rows/sec |
| Filter with pushed-down equality on dictionary-encoded column | ≥ 500M rows/sec effective |
| CSV ingest throughput (parse → sort → encode → write) | ≥ 100 MB/s |
| Compression ratio (segment bytes / raw CSV bytes) | **≤ 10%** |
| Zone-map pruning effectiveness on the acceptance query | ≥ 80% of row-groups skipped |

Regression gate triggers if any bench slips >10% vs. the previous green main. The bench suite itself is TASK-236; the CI job, baseline capture, and comparison machinery that enforce the gate are TASK-241.

Wave 2 is where the real interfaces get decided, so design anchors are front-loaded. After the anchors land, the encoding and storage tasks form the longest parallelism vein — the 6 encoding tasks plus the writer/reader/zone-map/manifest tasks give 10+ agents work the moment the trait lands. Rule 5 applies: Wave 2 does not begin until every Wave 1 task is complete.

### TASK-201: [DESIGN] Segment format v1
**Output**: docs/design/storage/segment-format-v1.md
**Depends on**: TASK-109
**Description**: Finalize the byte-level v1 layout per storage-format.md §9: file header (magic + version), row-group size (65,536 rows), column chunk header (encoding descriptor, compression, null bitmap, row count, byte range), zone-map block (min/max per column per row-group), footer (schema, row-group index, dictionaries, checksum, footer length, trailing magic). Encoding set is frozen at **Plain, Dictionary, Delta, BitPacking, Constant** with **LZ4** as the post-encoding compression layer. No FSST/ALP/PFOR/FOR/DoubleDelta/RLE/Frequency — those are Wave 4. Unblocks every other Wave 2 storage task.

### TASK-202: [DESIGN] Scan interface and predicate pushdown protocol
**Output**: docs/design/storage/predicate-pushdown.md
**Depends on**: TASK-109, TASK-201
**Description**: How the scan operator asks the storage layer to push down equality, range, and set predicates. Scan-side capability advertisement (which `CompiledExpr` shapes a scan accepts), zone-map evaluation order, fallback to post-filter when a predicate can't be pushed, interaction with dictionary-encoded columns (predicate rewritten against the dictionary). Cross-cutting between storage, operators, and planner — risky.

### TASK-203: [DESIGN] Parser grammar framework
**Output**: docs/design/language/grammar-framework.md
**Depends on**: TASK-114
**Description**: Decides hand-rolled vs parser generator (chumsky/pest/lalrpop/nom), error-recovery strategy (Wave 0 language doc pins "halt on first error" — design must confirm), span tracking for diagnostics, how new productions are added, and the surface for the colon-separated WITH option list `WITH (format: 'csv', map: (src AS dst, ...))` whose AST shape is fixed by TASK-237. Unblocks every post-stub parser task across Waves 2-4.

### TASK-204: [DESIGN] Logical plan node catalog (Wave 2 subset + forward map)
**Output**: docs/design/planner/logical-plan-nodes.md
**Depends on**: TASK-115
**Description**: Comprehensive enumeration of logical plan nodes expected across the project (Scan, Filter, Project, Limit, CreateTable, DropTable, AlterTableAddColumn, Describe, Insert, Explain at Wave 2 depth; Match/Funnel/Aggregate/Sessionize/Retention/Sort/Distinct/Cohort/Delete stubbed for later waves — Delete pairs with Wave 4's tombstone work). For each: input/output schemas, which AST constructs lower to them, the rewrite rules that apply. Wave 2 implements the subset marked "Wave 2 depth"; the rest are documented so the catalog doesn't churn across waves.

### TASK-205: [DESIGN] Expression compilation model
**Output**: docs/design/planner/expression-compilation.md
**Depends on**: TASK-115, TASK-204
**Description**: AST expression → `TypedExpr` (schema-resolved, type-checked per type-system.md §10) → `CompiledExpr` (runtime-evaluable over Arrow batches). Defines the `CompiledExpr` struct that ScanPhysical/FilterPhysical/ProjectPhysical all carry. Compilation-target selection between Arrow compute kernels and monomorphized hot paths, null propagation under three-valued logic, and how compiled predicates are surfaced to the pushdown protocol from TASK-202. Unblocks TASK-225 and everything downstream of the expression compiler.

### TASK-206: [IMPL] Encoding trait + Plain reference impl + property-test pattern
**Output**: crates/bqlite-storage/src/encoding/{mod,plain}.rs
**Depends on**: TASK-201
**Description**: The `Encoding` trait (encode/decode/estimate_size/applicable_to), the Plain reference implementation covering every primitive type the v1 format supports, and the round-trip property-test pattern (via the TASK-124 harness) that every subsequent encoding task copies. Establishes the pattern that every other encoding in Wave 2 and in Wave 4's advanced-encoding work follows. No selector logic — that's TASK-212.

### TASK-207: [IMPL] Dictionary encoding
**Output**: crates/bqlite-storage/src/encoding/dictionary.rs
**Depends on**: TASK-206
**Description**: Per-column-chunk dictionary + bit-packed code stream. Handles strings and low-cardinality ints. Round-trip property tests per the TASK-206 pattern. Surfaces dictionary to segment footer so predicates can be rewritten against codes at pushdown time (TASK-202).

### TASK-208: [IMPL] Delta encoding
**Output**: crates/bqlite-storage/src/encoding/delta.rs
**Depends on**: TASK-206
**Description**: First-value + bit-packed deltas. Primarily for monotonic-ish timestamps within an entity. Handles signed overflow. Round-trip property tests.

### TASK-209: [IMPL] BitPacking encoding
**Output**: crates/bqlite-storage/src/encoding/bitpacking.rs
**Depends on**: TASK-206
**Description**: Variable-width bit packing for small integer ranges. Width derived from min/max of the chunk. Round-trip property tests including all-zero, single-value, and full-width edge cases.

### TASK-210: [IMPL] Constant encoding
**Output**: crates/bqlite-storage/src/encoding/constant.rs
**Depends on**: TASK-206
**Description**: Zero-data encoding for chunks where every non-null value is identical. Stores the constant in the chunk header. Null mask handled separately. Round-trip property tests.

### TASK-211: [IMPL] LZ4 compression wrapper
**Output**: crates/bqlite-storage/src/encoding/lz4.rs
**Depends on**: TASK-206
**Description**: Post-encoding compression layer applied to the encoded byte stream of any encoding. Uses `lz4_flex`. Configurable acceleration, defaults tuned for small chunks. Round-trip tests.

### TASK-212: [IMPL] Encoding selector heuristic
**Output**: crates/bqlite-storage/src/encoding/selector.rs
**Depends on**: TASK-207, TASK-208, TASK-209, TASK-210, TASK-211
**Description**: Per storage-format.md §10.3: sample a chunk, score each applicable encoding (encoded size + decode cost estimate), pick the lowest-bytes encoding with ties broken by decode cost. Decides whether to apply the LZ4 wrapper. Unit tests cover every encoding-pick path plus the degenerate "Plain is always a legal fallback" invariant.

### TASK-213: [IMPL] Segment file format writer (low-level)
**Output**: crates/bqlite-storage/src/segment/writer.rs
**Depends on**: TASK-201, TASK-206
**Description**: Takes encoded column chunks + chunk metadata and emits v1 on-disk bytes: file header, row groups, zone-map block, footer, trailing checksum + magic. Pure byte-layout code — no encoding selection, no sorting, no manifest interaction. Atomic via temp-file + rename. Unit tests assert byte-exact layout for a small hand-crafted row group.

### TASK-214: [IMPL] Entity-sorted segment writer orchestration
**Output**: crates/bqlite-storage/src/writer.rs
**Depends on**: TASK-212, TASK-213, TASK-217, TASK-218
**Description**: High-level writer — accepts a sorted `(entity_id, ts)` event stream from the partitioner (TASK-218), groups into row groups respecting entity boundaries, selects per-column encodings via TASK-212, emits bytes via TASK-213, and atomically registers the new segment in the manifest (TASK-217). Honors the entity-boundary invariant from storage-format.md §7.2 (no entity straddles a row-group unless it exceeds row-group size, in which case it occupies consecutive row groups within a segment). Integration tests validate round-trip through a real reader.

### TASK-215: [IMPL] Segment file reader + row-group iterator
**Output**: crates/bqlite-storage/src/segment/reader.rs
**Depends on**: TASK-201, TASK-206
**Description**: Parses a segment file's footer (schema, row-group index, dictionaries, zone maps), validates checksum + magic, and exposes a lazy row-group iterator that materializes column chunks on demand. Decodes chunks back to Arrow arrays via the encoding trait. Zero-copy where the encoding permits. Implements the `SegmentReader` trait from TASK-109.

### TASK-216: [IMPL] Zone map write + read + predicate pruning
**Output**: crates/bqlite-storage/src/zone_map.rs
**Depends on**: TASK-202, TASK-213, TASK-215
**Description**: Writer extracts per-column min/max per row group during encoding and emits them into the zone-map block in the footer. Reader loads zone maps eagerly at segment open (they're small). Pruning evaluator accepts a pushed-down `CompiledExpr` and returns the set of row groups that cannot be skipped. Correctness tests assert no false negatives (a row that would match must never be pruned) and measure false-positive rate against synthetic workloads.

### TASK-217: [IMPL] Manifest v1 segment inventory + atomic updates
**Output**: crates/bqlite-storage/src/manifest.rs
**Depends on**: TASK-116
**Description**: Extends TASK-116's bootstrap manifest to track segments per `(table, window, shard)` with full `SegmentMeta` per storage-format.md §12.3 (segment_id, level, schema_version, row_count, byte_size, ts_range, entity_range, per-column stats, created_at, batch_id). Atomic updates via `manifest.json.tmp` → `fsync` → `rename`. Read-write API: `add_segment`, `remove_segment`, `snapshot_for_query(table, time_range, shard)`. Concurrency controlled by the existing `flock` guard.

### TASK-218: [IMPL] Ingest partitioner
**Output**: crates/bqlite-storage/src/ingest/partitioner.rs
**Depends on**: TASK-217
**Description**: Routes incoming events to the correct `(shard, window)` bucket: `shard = xxhash64(entity_id) % shard_count` from the manifest config, `window = floor(ts / window_size)`. Buffers and sorts each bucket by `(entity_id, ts)` before handing the sorted stream to the writer (TASK-214). Memory-bounded — spills to an on-disk external sort when the buffer exceeds a configurable budget (stub: just error loudly for Wave 2; real spill is Wave 5). Assigns each ingest call a fresh `batch_id` from the manifest counter.

### TASK-219: [IMPL] K-way merge scan across L0 segments
**Output**: crates/bqlite-storage/src/segment/merge.rs
**Depends on**: TASK-215, TASK-217
**Description**: Given a shard and a time range, asks the manifest for all matching L0 segments and produces a merged `(entity_id, ts)`-ordered event stream across them. Loser-tree or binary-heap k-way merge; streaming, not materializing. Critical for Wave 2 because there's no compaction yet — every ingest produces a new L0 segment and queries must merge across all of them.

### TASK-220: [IMPL] Parser framework bootstrap + expression grammar
**Output**: crates/bqlite-parser/src/{lib,expr}.rs
**Depends on**: TASK-203
**Description**: Instantiates the framework chosen in TASK-203, replaces the Wave 1 one-identifier stub, and lands the expression grammar: literals (int, float, string, bool, timestamp, duration), identifiers, property access (`table.col`), unary/binary arithmetic, comparisons, `AND`/`OR`/`NOT`, parentheses, `IS NULL` / `IS NOT NULL`. Rich span tracking for diagnostics. Halt-on-first-error per language-doc §policy. Unit tests cover every operator precedence edge.

### TASK-221: [IMPL] Schema DDL + EXPLAIN productions
**Output**: crates/bqlite-parser/src/ddl.rs
**Depends on**: TASK-220
**Description**: All four schema-DDL parser productions plus EXPLAIN, matching the AST shapes already in `crates/bqlite-ast/src/statement.rs`:

- `CREATE TABLE <name> (<col> <type> [role] [NOT NULL] [DEFAULT <lit>], ...);` where `role` ∈ `ENTITY KEY | EVENT TIME | EVENT TYPE`. Multi-role-per-column validation is deferred to the planner.
- `DROP TABLE <name>;` — no `IF EXISTS` modifier per query-language.md §20.4.
- `ALTER TABLE <name> ADD COLUMN <col> <type> [NOT NULL] [DEFAULT <lit>];` — only `ADD COLUMN` in v1, lowering to the existing `AlterAction::AddColumn` AST variant.
- `DESCRIBE <name>;` — emits the catalog schema for a table (output columns documented in query-language.md §20.5).
- `EXPLAIN <pipeline>` — parser-level wrapper producing an `Explain(Pipeline)` AST node. Pipelines only; DDL/DML are not EXPLAIN-able in v1, matching `Statement::Explain(Pipeline)` in the AST.

Parser tests only — plan-time semantic validation is TASK-226 / TASK-232. `DELETE` is intentionally out of scope (deferred to Wave 4 with tombstones).

### TASK-222: [IMPL] INSERT FROM production with column remapping
**Output**: crates/bqlite-parser/src/dml.rs
**Depends on**: TASK-220, TASK-237
**Description**: `INSERT INTO <table> FROM <path-literal> WITH (<key>: <value>, ...);` matching the option syntax fixed in query-language.md §20.1 (colon-separated key/value pairs, not `=`). Recognized option keys include `format: 'csv' | 'jsonl' | 'parquet'`, `delimiter: <string>`, `header: <bool>`, and `map: (<src> AS <dst>, ...)`. The `map` clause uses the structured `(src AS dst, ...)` AST shape introduced by TASK-237 — without that prerequisite the AST cannot represent the mapping and this task will not compile. Unmapped source columns default to passthrough when the source name matches a table column. Parser tests for the happy paths and the obvious mistakes (trailing commas, duplicate source names, missing WITH, unknown format, malformed `map` entry). Plan-time validation of the actual mapping against the target table schema is TASK-226.

### TASK-223: [IMPL] Pipeline + where/select/limit productions
**Output**: crates/bqlite-parser/src/pipeline.rs
**Depends on**: TASK-220
**Description**: The `|` pipeline operator and the Wave 2 verbs, matching the BQL grammar in query-language.md §26: `where <expr>`, `select <col-or-expr-with-alias>, ...`, `limit <int>`. Keywords are case-insensitive (§29 line 1916). The AST stages these into `PipelineStage::{Where, Select, Limit}` per `crates/bqlite-ast/src/operator.rs`. A pipeline starts with a table reference (identifier) and chains verbs. Parser tests cover associativity, nested expressions in WHERE, multi-column select with `expr AS alias`, and edge cases (empty pipeline, limit without argument).

### TASK-224: [IMPL] Logical plan enum + AST → logical lowering
**Output**: crates/bqlite-planner/src/logical.rs
**Depends on**: TASK-204
**Description**: Concrete `LogicalPlan` enum for Wave 2 scope: `Scan`, `Filter`, `Project`, `Limit`, `CreateTable`, `DropTable`, `AlterTableAddColumn`, `Describe`, `Insert`, `Explain`. Each node carries its `OperatorSchema` computed at construction time (planner-pipeline.md §5) — for DDL nodes that produce no rows, the schema is an empty or single-status-column shape per query-language.md §20.4-§20.5. Lowering walks an AST statement and produces the root `LogicalPlan`, resolving table names via the catalog (and reporting `unknown table` for missing references). `Insert` lowering handles both `InsertBody::Values` and `InsertBody::From`. Schema validation happens at construction, not as a separate pass.

### TASK-225: [IMPL] Expression compilation (TypedExpr + CompiledExpr)
**Output**: crates/bqlite-planner/src/expr.rs
**Depends on**: TASK-205
**Description**: Implements TASK-205's design: schema-resolved `TypedExpr` construction with type checking against the scalar-function catalog, and compilation into `CompiledExpr` (the runtime form carried by physical operators). Dispatches between Arrow compute kernels and monomorphized hot paths. Handles three-valued-logic null propagation. Surfaces a `supported_pushdown_shape()` query so the pushdown pass (TASK-227) can ask "can storage evaluate this?".

### TASK-226: [IMPL] Physical plan descriptors + logical → physical lowering
**Output**: crates/bqlite-planner/src/physical.rs
**Depends on**: TASK-224, TASK-225
**Description**: Plain-data physical descriptors per planner-pipeline.md §15: `ScanPhysical`, `FilterPhysical`, `ProjectPhysical`, `LimitPhysical`, `CreateTablePhysical`, `DropTablePhysical`, `AlterTableAddColumnPhysical`, `DescribePhysical`, `InsertPhysical`, `ExplainPhysical`. No trait objects — descriptors are `Clone + Serialize`. Lowering converts each `LogicalPlan` node to its physical counterpart, invoking the expression compiler (TASK-225) to produce the `CompiledExpr` values the descriptors carry. DDL/DML nodes get simple one-to-one lowerings; real execution logic lives in the engine.

### TASK-227: [IMPL] Predicate pushdown optimizer pass
**Output**: crates/bqlite-planner/src/opt/pushdown.rs
**Depends on**: TASK-202, TASK-226
**Description**: Walks the physical plan looking for `FilterPhysical` directly above a `ScanPhysical`. For each conjunct, asks TASK-202's protocol "can the scan evaluate this?". Pushable conjuncts move into `ScanPhysical.scan_predicates`; the residue stays in `FilterPhysical` (which is elided if nothing remains). Does not mutate trees where selectivity is unknown — conservative per Wave 2 scope. Unit tests include the zero-residue, full-residue, and partial-pushdown cases.

### TASK-228: [IMPL] Projection pruning optimizer pass
**Output**: crates/bqlite-planner/src/opt/prune.rs
**Depends on**: TASK-226
**Description**: Backward demand-set collection per planner-pipeline.md §6.6 Pass 4. Walks the physical plan bottom-up, accumulating the set of columns each operator needs, forwarding demand through operators that pass columns through. At `ScanPhysical`, the accumulated demand becomes `projected_columns`. Unit tests verify that a query selecting 2 columns from a 10-column table results in a scan reading exactly 2 columns.

### TASK-229: [IMPL] EXPLAIN tree builder + text formatter
**Output**: crates/bqlite-planner/src/explain.rs
**Depends on**: TASK-227, TASK-228
**Description**: When planning an `Explain` node, capture the plan tree after every optimizer pass and build a structured `ExplainNode` tree annotating: logical nodes, optimizer rewrites (pushed-down predicates, pruned columns), and the final physical descriptors. Text formatter renders the tree as indented plain text — no Unicode polish. Used by the CLI and by tests to verify optimizer behavior declaratively.

### TASK-230: [IMPL] Entity-sorted scan operator
**Output**: crates/bqlite-operators/src/scan.rs (replaces Wave 1 stub)
**Depends on**: TASK-202, TASK-216, TASK-219, TASK-225
**Description**: Full entity-sorted scan over real segments. Consults the manifest for matching segments, opens them via TASK-215's reader, merges across segments via TASK-219's k-way merge, applies zone-map pruning via TASK-216, and evaluates pushed-down `CompiledExpr` predicates against materialized row groups. Respects `projected_columns` so only demanded columns are decoded. Foundation for every temporal operator in later waves.

### TASK-231: [IMPL] Filter + Project + Limit operators
**Output**: crates/bqlite-operators/src/{filter,project,limit}.rs
**Depends on**: TASK-225
**Description**: Three vectorized stateless operators over Arrow `RecordBatch`es, replacing the Wave 1 pass-through stubs. Each operator implements `PhysicalOperator` and returns `RecordBatch` at the `next_batch()` boundary — Wave 2 does **not** implement the fused stateless push segment or the `FilteredBatch` / `SelectionVector` chain from execution-model.md §3.8. That design is the steady-state target, but it only pays off when multiple stateless kernels chain inside a fused push segment, which is a Wave 5 `[DESIGN]`/`[IMPL]` (TASK-503) concern — see the Wave 5 note at the bottom of this file. Wave 2 ships the correct-but-simpler copy-based implementation that the fusion work will later refactor.

What Wave 2 does implement:

- **Filter.** Evaluates a `CompiledExpr` predicate against the input batch and returns a filtered batch via `arrow::compute::filter`. **Iterates the input batch in execution tiles** (default 2,048 rows) when evaluating the predicate, for cache-friendly access per execution-model.md §3.6: the predicate kernel walks the batch in tile-sized slices (`batch.slice(start, len)`), computes a `BooleanArray` per tile, and concatenates the tile-level boolean arrays into one mask that drives a single final `arrow::compute::filter` call. The tile loop is purely for L1/L2 residency during predicate evaluation — there is no selection-vector chain across operators. Tile size is a construction-time parameter on `FilterOperator::new`, defaulted to 2,048 and clamped to `[1024, 4096]`; TASK-226's `FilterPhysical` descriptor carries the value so the bind step (TASK-232) can pass it through.

  Dictionary-encoded string columns are filtered via the precomputed `DictFilterBitset` from execution-model.md §3.7: the scan hands the bitset over at construction time, and the filter's per-tile predicate evaluation does an integer bitset lookup instead of a string comparison in the hot loop.

- **Project.** Evaluates a list of output `CompiledExpr` expressions (with aliases) and assembles the output schema. **String materialization always produces `StringViewArray`** (`DataType::Utf8View`), never `StringArray`, matching the execution-model.md §3.7 contract and the existing storage-layer decoders (`crates/bqlite-storage/src/encoding/dictionary.rs` already produces `StringViewArray`). If an Arrow compute kernel on the target path only has a flat-Utf8 variant, wrap it in a small adapter rather than round-tripping through `StringArray`.

- **Limit.** Counts rows across batches and halts its child early once the limit is reached. Single counter, stateless beyond that.

All three respect the entity-aligned batch contract (execution-model.md §3.5) — they never need to track entity boundaries. Null-aware per three-valued logic.

**What is not in scope.** No `FilteredBatch`, no `SelectionVector`, no `StatelessKernel` trait, no `materialize_filtered_batch` helper, no fused push segment driver, no cross-operator selection-vector propagation. Those are Wave 5 (TASK-503 and the fusion implementation tasks it spawns), which refactors these three operators rather than replacing them.

### TASK-232: [IMPL] Engine bind step extension + DDL execution path
**Output**: crates/bqlite-engine/src/{bind,ddl}.rs
**Depends on**: TASK-217, TASK-226, TASK-230, TASK-231
**Description**: Extends TASK-118's bind step to materialize `Box<dyn PhysicalOperator>` from every Wave 2 physical descriptor. For data-plane nodes (`ScanPhysical`, `FilterPhysical`, `ProjectPhysical`, `LimitPhysical`), bind returns an operator tree. DDL nodes invoke execution closures that operate directly on the manifest via TASK-217's atomic update API:

- `CreateTablePhysical` — validates the schema, errors on name collision, atomically registers the new table.
- `DropTablePhysical` — errors on missing table, atomically removes the table entry. Wave 2 also drops the table's segments from the manifest inventory; on-disk segment files are reaped by TASK-239's startup orphan-cleanup pass on the next open.
- `AlterTableAddColumnPhysical` — errors on missing table or duplicate column name, appends the new `ColumnDef` to the schema, bumps `schema_version`, and atomically writes the manifest. The new column reads as NULL (or DEFAULT if specified) for all existing rows; no segment rewrite is needed because reads project by column name against the per-segment schema snapshot.
- `DescribePhysical` — looks up the table, formats its column metadata as the four-column result (`name`, `type`, `nullable`, `role`) per query-language.md §20.5, returns it as a single result batch.
- `ExplainPhysical` — formats the captured `ExplainNode` tree as a single-column result batch.

Unit tests for every descriptor variant including the error paths (missing table on DROP/ALTER/DESCRIBE, duplicate column on ALTER, name collision on CREATE).

### TASK-233: [IMPL] Engine INSERT execution + CSV reader integration
**Output**: crates/bqlite-engine/src/ingest.rs, crates/bqlite-storage/src/ingest/csv.rs
**Depends on**: TASK-214, TASK-218, TASK-222, TASK-232
**Description**: When the engine binds an `InsertPhysical` carrying an `InsertBody::From`, it opens the source file, constructs a streaming CSV reader (format-dispatched via the `format` key in the `WITH (...)` option list), applies the structured `map` clause from the AST shape introduced by TASK-237 to rename source columns, converts rows to `PropertyValue` per the target schema, and feeds the stream through the partitioner (TASK-218) into the writer (TASK-214). Source columns not in the map default to passthrough if their name matches a target column; extra source columns error. Missing required columns error. Type mismatches error with row numbers. CSV reader handles quoting, escaping, multi-line fields, and common delimiter variations. The literal `InsertBody::Values` arm is handled separately by TASK-238, which feeds its own row stream through the same partitioner + writer pipeline.

### TASK-234: [IMPL] CLI `ingest` subcommand + result formatter with auto-limit
**Output**: crates/bqlite-cli/src/{ingest,format}.rs
**Depends on**: TASK-233
**Description**: Two independent CLI extensions landing in one task because both are small:
- `bqlite ingest <path> --table <name> [--format csv|json|parquet] [--map "src=dst,..."]` — a thin wrapper that constructs the equivalent `INSERT INTO ... FROM ... WITH (...)` statement and hands it to `Engine::query`, so the CLI never touches parser/planner directly.
- Result formatter that, for any query with no explicit `| limit`, injects `LIMIT 1000` at the CLI boundary (not inside the engine) and prints a truncation footer like `... 49,999,000 rows omitted (use --limit N or --no-limit)`. Explicit `| limit` in the query suppresses auto-injection. `--no-limit` disables truncation; `--limit N` overrides the default.

### TASK-235: [IMPL] Wave 2 acceptance test + CSV fixture loader
**Output**: tests/tests/wave2_acceptance.rs, tests/src/csv.rs (new module re-exported from `tests/src/lib.rs`)
**Depends on**: TASK-234, TASK-238, TASK-240
**Description**: CSV fixture loader extends the Wave 1 harness (TASK-120) — deterministic synthetic-data generator producing the `purchases` schema at parameterized scale. Integration test runs the full acceptance script against a fresh temp directory: `Database::create` (or `bqlite init`), then `CREATE TABLE`, then `INSERT ... VALUES` (small literal batch), then `INSERT ... FROM` the synthetic CSV at 1M-row scale, then the `where`/`select`/`limit` pipeline query, then `EXPLAIN`, then `DESCRIBE`, `ALTER TABLE ADD COLUMN`, and `DROP TABLE`. Asserts exact result rows for the data-plane query and the expected `ExplainNode` structure (pushed-down predicate, pruned columns); for DDL paths, asserts the post-state of the manifest. A 100M-row variant lives behind `#[ignore]` and runs in the bench job. Failure here is the Wave 2 acceptance-gate trip.

### TASK-236: [IMPL] Wave 2 benchmark suite
**Output**: benches/wave2/{scan,encoding,ingest,acceptance}.rs
**Depends on**: TASK-214, TASK-215, TASK-230, TASK-231
**Description**: Criterion benches covering the Wave 2 performance gate:
- `scan` — columnar decode throughput, int64 / string / float, with and without zone-map pruning.
- `encoding` — per-encoding encode/decode microbenches for Plain, Dictionary, Delta, BitPacking, Constant, and the LZ4 wrapper overhead.
- `ingest` — CSV ingest throughput end-to-end on the reference dataset.
- `acceptance` — the full 100M-row acceptance query.

Each bench asserts its target from the Wave 2 performance gate table as a hard ceiling at the bench level — a single run that misses the target fails the bench in `cargo test --all-targets` (the harness pattern from TASK-121). Local reference targets are verified on the pinned reference machine before the wave is declared complete. Continuous regression comparison against the previous green main is the responsibility of TASK-241, which wires the bench subset into a dedicated CI workflow with baseline persistence and the >10% slip gate.

**Bench-side metric reporting** (execution-model.md §14.1). Every bench prints, in addition to its primary throughput number, the cost-side metrics that turn "fast" into "expensive" or "cheap":
- `gb_per_sec_scanned` — derived as `bytes_scanned / elapsed / num_cores`. The headline GB/s/core number that anchors regression triage.
- `bytes_decoded_to_scanned` — the late-materialization signal. Lower is better.
- `cycles_per_event` — opt-in (`bqlite query --explain-perf` mode), printed when `perf_event_open`/`kpc` is available.

Selection-vector-related metrics (`selection_vector_materializations` and friends from execution-model.md §14.1) are **not** in scope for Wave 2 — they depend on the fused stateless push segment that ships in Wave 5. Wave 2 benches ignore those rows, and Wave 5 extends this bench suite when it builds out the fusion path.

These are surfaced via the Criterion bench's `Throughput::Bytes` plus a custom `eprintln!` line per bench iteration. Capturing them at bench time (rather than only when running real queries) is what makes them load-bearing for the regression gate in TASK-241 — once the gate exists, slips in `bytes_decoded_to_scanned` or `cycles_per_event` count just like slips in the headline throughput numbers.

### TASK-237: [DESIGN][IMPL] INSERT FROM column-remapping language + AST extension
**Output**: docs/design/query-language.md, crates/bqlite-ast/src/statement.rs
**Depends on**: none
**Description**: Merge-first AST + design-doc extension that unblocks TASK-222's parser work and TASK-233's CSV ingest path. Without this task the AST cannot represent the `map: (src AS dst, ...)` clause that the Wave 2 acceptance script and goal text both rely on.

- **Language doc.** Extends query-language.md §20.1 with the `map` option for `INSERT ... FROM`: `WITH (format: 'csv', map: (uid AS user_id, time AS ts, evt AS event))`. Documents that unmapped source columns pass through by name match, that duplicate `dst` names error, and that the rule lives alongside the existing `format`/`delimiter`/`header` options.
- **AST.** Replaces or extends the current `InsertOption { key: Name, value: Literal }` shape so the `map` clause is representable. Either (a) add a structured `map: Option<Vec<ColumnMapping>>` field on `InsertBody::From` alongside the existing flat option list, or (b) introduce an `InsertOptionValue` enum that admits both literal values and a column-mapping list. Decision recorded in this task's design note section so TASK-222 can implement against the chosen shape. Add round-trip serde tests for the new shape.
- **No parser work.** TASK-222 still owns parsing.

This is merge-first because every dependent task assumes the AST shape exists. It is intentionally pre-numbered (anchor-style) so an agent can claim it before the rest of Wave 2 starts.

### TASK-238: [IMPL] INSERT VALUES end-to-end
**Output**: crates/bqlite-parser/src/dml.rs, crates/bqlite-planner/src/{logical,physical}.rs (additions), crates/bqlite-engine/src/ingest.rs (additions)
**Depends on**: TASK-220, TASK-224, TASK-226, TASK-232, TASK-233
**Description**: Implements the literal-tuple form of INSERT, which the AST already models as `InsertBody::Values(Vec<Vec<Literal>>)` and which query-language.md §20.1 documents alongside `INSERT ... FROM`. Without this task the Wave 2 goal "CREATE TABLE / INSERT / EXPLAIN" only ships half of INSERT.

- **Parser.** Adds the `INSERT INTO <table> VALUES (lit, lit, ...), (lit, lit, ...);` production to TASK-222's `dml.rs`. Positional only, no column list, literals only — matches the AST's `Vec<Vec<Literal>>` shape and the §20.1 v1 restriction.
- **Planner.** `Insert` logical/physical lowering grows a `Values` arm carrying the literal tuples directly (no `CompiledExpr`, since literals don't need compilation).
- **Engine.** When binding an `InsertPhysical { body: Values(rows) }`, the engine validates each row against the target table's `TableSchema` (arity, type coercion to `PropertyValue`, NOT NULL, role-column population), assigns a fresh `batch_id`, and feeds the rows through the same partitioner + writer pipeline TASK-233 uses for CSV. Type mismatches and NOT NULL violations error with the offending row index.
- **Tests.** Round-trip test: `CREATE TABLE` → `INSERT VALUES` → scan the table back, assert the rows. Error tests cover wrong arity, wrong type, NULL into NOT NULL, and rejected literal kinds.

### TASK-239: [IMPL] Startup orphan segment + manifest reconciliation
**Output**: crates/bqlite-storage/src/database.rs (cleanup pass), crates/bqlite-storage/src/segment/cleanup.rs (new)
**Depends on**: TASK-214, TASK-217
**Description**: Implements the crash-safety contract from `docs/reliability.md` §Crash Safety and `docs/design/storage-format.md` §7.4. Today `Database::open_or_create` only sweeps `manifest.json.tmp`; once Wave 2 starts writing real segment files via TASK-213/214, every startup must reconcile on-disk segment state against the manifest:

- **Sweep `.tmp` segment files.** Walk every `(window, shard)` directory under the database root. Any file ending in `.tmp` is a partially-written segment from a crash mid-ingest or mid-compaction and is unconditionally deleted.
- **Sweep manifest-orphaned segments.** Build the set of segment file names referenced by the manifest's segment inventory. Any non-`.tmp` segment file in a `(window, shard)` directory that is not in the active set is an orphan from a deferred compaction delete and is removed. Files that *are* in the active set are left untouched.
- **Idempotent and safe.** The pass is read-only with respect to the manifest — it only deletes files, never edits the manifest. Re-running the pass on a clean database is a no-op.
- **Tests.** Crashed-ingest scenario (orphan `.tmp`), crashed-compaction scenario (orphan output `.tmp` and orphan input segment), happy-path open with no orphans (no deletions, manifest unchanged), and a regression test that an active segment listed in the manifest is *never* deleted even if its file looks unusual.

Without this task, the storage layer leaks disk space across crashes and the reliability doc describes behavior the code does not implement.

### TASK-240: [IMPL] Database open/create split + `bqlite init` + bootstrap retirement
**Output**: crates/bqlite-storage/src/database.rs, crates/bqlite-cli/src/main.rs, tests/smoke.rs
**Depends on**: TASK-232
**Description**: Aligns runtime behavior with query-language.md §29 line 1911 ("Database init | CLI-only (`bqlite init`)") and retires the Wave 1 bootstrap `events` table now that real `CREATE TABLE` exists end-to-end (TASK-221 + TASK-224 + TASK-226 + TASK-232).

- **API split.** `Database::open_or_create` is removed. Replaced by:
  - `Database::create(path) -> Result<Database>` — initializes a fresh database directory, manifest, and `.lock`. Errors with `BqliteError::Execution` if the directory already contains a manifest. Implicitly opens the database it just created (callers don't need a follow-up `open` call).
  - `Database::open(path) -> Result<Database>` — opens an existing database. Errors if the directory has no manifest, with a clear message pointing the user at `bqlite init`.
- **Bootstrap removal.** `Database::create` does not seed the `events` table. The `bootstrap_events_table: bool` field on `TableEntry` stays in the manifest schema for read-compatibility with Wave 1 databases (so a Wave 1 database opened by Wave 2 still works), but `Database::create` never sets it.
- **CLI.** New `bqlite init <path> [--shards N]` subcommand that calls `Database::create`. The existing `bqlite query` subcommand calls `Database::open` and surfaces a typed "database not initialized; run `bqlite init`" error when the directory is missing or empty.
- **Test updates.** The Wave 1 smoke test (`tests/smoke.rs`, originally landed by TASK-123) is rewritten to: create the database, `CREATE TABLE events (...)`, then run `bqlite query "events"`. The integration-test fixture helper from TASK-120 grows a `TempDb::create()` constructor matching the new API; existing `TempDb::open_or_create` callers migrate.
- **Migration note.** A short paragraph in `docs/reliability.md` documents that manifests carrying `bootstrap_events_table: true` are read-compatible but no longer produced.

### TASK-241: [IMPL] Wave 2 benchmark CI job + baseline + regression gate
**Output**: .github/workflows/bench.yml, scripts/bench-compare.sh
**Depends on**: TASK-236
**Description**: Wires up the regression-gate machinery promised in the Wave 2 header. Without this task the perf gate is aspirational — TASK-236 only writes the benches.

- **Workflow.** New `bench.yml` GitHub Actions workflow runs the Wave 2 bench subset (`benches/wave2/*`) on `ubuntu-latest` (4 vCPU) using the **1.5×-relaxed CI targets** from the Wave 2 perf-gate table.
- **Baseline capture on main.** On every push to `main`, the workflow runs the bench subset and uploads the Criterion `estimates.json` outputs as a workflow artifact named `bench-baseline-main`. The most recent artifact is the canonical "previous green main" baseline.
- **Comparison on PR.** On pull requests, the workflow runs the same bench subset, downloads the latest `bench-baseline-main` artifact, and runs `scripts/bench-compare.sh` to diff each metric. If any metric slips >10% on at least 3 consecutive Criterion samples (the consecutive-sample rule protects against single noisy runs on shared hardware), the job fails and the PR is blocked.
- **Opt-out.** PRs labeled `bench-skip` and draft PRs bypass the gate — for docs-only changes and similar.
- **Reference-hardware verification stays manual.** The pinned Apple M3 Pro numbers remain verified by hand before the wave is declared complete; the CI gate uses only the relaxed CI targets.

### TASK-242: [RETIRED]
**Status**: Retired during the post-Wave-2 architecture reconciliation. Originally scoped as "FilteredBatch + SelectionVector + execution tile scaffold" after execution-model.md §3.8 introduced selection vectors as a steady-state design. Review found the selection-vector half of the task only pays off under a fused stateless push segment, which is a Wave 5 concern (TASK-503 "operator fusion" territory) — Wave 2 has no operator chain that can carry a `FilteredBatch` across operator boundaries, because the `PhysicalOperator::next_batch()` boundary is `RecordBatch`. The execution-tile half was small enough to fold into TASK-231 directly (filter operator's constructor grows a `tile_size` parameter; the tile loop is a 10-line helper). Number retired per the "numbers are never reused" rule. The full execution-model.md §3.8 design remains the implementation target for Wave 5 — see the forward reference in TASK-503.

### TASK-243: [IMPL] posix_fadvise sequential-scan hint
**Output**: crates/bqlite-storage/src/segment/reader.rs (additions), crates/bqlite-storage/src/segment/merge.rs (additions)
**Depends on**: TASK-215, TASK-219
**Description**: Reconciliation task added after storage-format.md §8.2 introduced explicit access-pattern hints. Wave 2's segment reader (TASK-215) and merge scan (TASK-219) shipped without `posix_fadvise` integration; this task wires in the single sequential-scan hint that is actually actionable in Wave 2.

One deliverable, deliberately tiny:

- **Sequential scan hint** at segment open. When `SegmentReader` is constructed for a full-segment scan (every call path in Wave 2), it issues `posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL)` on Unix, no-op on other platforms. Wrapped in a tiny `posix_fadvise_compat` shim so the call is a no-op on Windows and on macOS where `posix_fadvise` is unavailable (macOS's `fcntl(F_RDADVISE)` equivalent is a separate follow-up).

**Not in scope for Wave 2.**
- `Random` hint — paired with the single-entity lookup path from storage-format.md §8.3, which Wave 2 does not ship. The hint lands alongside the lookup path in Wave 4.
- `WillNeed` hint — paired with compaction reads, which are Wave 4 (TASK-408).
- `memmap2::Advice` — the Wave 2 storage layer uses `pread`/BufReader, not mmap (verified: no `memmap2` imports anywhere in `crates/bqlite-storage`). The mmap path and its advice calls are a Wave 5 concern. The `memmap2::Advice` mention in storage-format.md §8.2 is kept because the doc describes the steady state; this task does not touch it.

Tests: a smoke test that opens a segment and asserts `posix_fadvise` was invoked (via a test-only counter or a stubbed `libc::posix_fadvise`), plus a re-run of the existing TASK-215 reader test suite to confirm no regressions. The hint is advisory — the kernel may ignore it, so the test asserts the call, not the side effect.

This task is intentionally tiny (~half a day) and exists to keep the implementation honest with storage-format.md §8.2.

### TASK-299: [IMPL] Wave 2 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-235, TASK-236, TASK-237, TASK-238, TASK-239, TASK-240, TASK-241, TASK-243
**Description**: Same audit pattern as TASK-199, rescored after Wave 2. Wave 2 is the first wave with a real performance gate, so the Benchmarks dimension must reflect whether the Wave 2 perf-gate targets are met on reference hardware — not merely whether benches exist. bqlite-storage (segment format, encodings, ingest), bqlite-planner (pushdown, projection pruning, EXPLAIN), and bqlite-operators (scan/filter/project/limit) will see the biggest grade movements. Any crate slipping vs. its Wave 1 grade is flagged in the commit message. Below-C grades get follow-up tasks; Wave 3 does not start until those are filed.

---

## Wave 3: Pattern Matching MVP

**Goal.** Funnel queries work end-to-end. MATCH operator, aggregates, limit, sort.
**Size.** ~22-28 tasks.
**Parallelism.** 6-10 agents.
**Acceptance.** A 3-step funnel query over an ingested CSV returns correct conversion counts per step.

### TASK-301: [DESIGN] MATCH operator architecture
**Output**: docs/design/operators/match-operator.md
**Depends on**: TASK-204, TASK-230
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
**Depends on**: TASK-108, TASK-230
**Description**: Foundational operator for funnel output and most analytics queries. count/sum/avg/min/max/distinct_count. Works on columnar batches.

Additional Wave 3 tasks: individual pattern grammar productions, limit/sort operators, EMIT ALL output assembly, MATCH lowering in the planner, matcher microbenchmarks, integration tests for common funnel shapes.

### TASK-399: [IMPL] Wave 3 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-301, TASK-302, TASK-303, TASK-304, TASK-305, TASK-306, TASK-307
**Description**: Same audit pattern as TASK-199, rescored after Wave 3. Focus on the crates Wave 3 grew substantially — bqlite-operators (MATCH + hash aggregate + matcher strategies) and bqlite-parser (pattern grammar). The Tests dimension specifically checks coverage of matcher edge cases: variable-binding tracks, negation, repetition, time-window expiry, EMIT ALL semantics, and the step-counter vs NFA strategy-selection boundary. Any crate slipping vs. Wave 2 is flagged. Below-C grades get follow-up tasks; Wave 4 does not start until those are filed.

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
**Depends on**: TASK-233
**Description**: Follows the CSV ingest pattern established in Wave 2 (TASK-233). Parquet path reuses Arrow decode; JSON path handles nested property objects.

Additional Wave 4 tasks: individual encoding implementations from TASK-401 outcomes, SESSIONIZE impl, retention operator, attribution impl, cohort grammar productions, alias binding in planner, FUNNEL and RETENTION syntactic sugar, tombstone writer, tombstone-aware merge scan, compaction microbenchmarks, integration tests for each new feature.

### TASK-499: [IMPL] Wave 4 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-401, TASK-402, TASK-403, TASK-404, TASK-405, TASK-406, TASK-407, TASK-408, TASK-409, TASK-410
**Description**: Same audit pattern as TASK-199, rescored after Wave 4. Wave 4 adds compaction, tombstones, advanced encodings, SESSIONIZE, attribution, cohorts, and JSON/Parquet ingest — bqlite-storage and bqlite-operators carry the bulk of the new surface. The Benchmarks dimension must reflect the advanced-encoding evidence from TASK-401 and the compaction microbenches. Cross-cutting concerns (tombstone-aware merge scan, compaction concurrency) need integration-test evidence, not just unit tests. Below-C grades get follow-up tasks; Wave 5 does not start until those are filed.

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

**Load-bearing forward reference:** execution-model.md §3.8 already specifies the steady-state stateless-segment design — `FilteredBatch`, `SelectionVector`, `StatelessKernel`, `materialize_filtered_batch`, and the three explicit materialization triggers (sparsity, push-segment boundary, aggregation hand-off — note the deliberate "materialization" terminology in §3.8.3, distinct from storage-format.md §7 "compaction"). The Wave 2 filter/project/limit operators (TASK-231) deliberately ship without that infrastructure because a fused push segment is required to make the selection-vector chain pay off. This design task is the point at which §3.8 moves from "documented target" to "implemented contract." It should produce the `[IMPL]` tasks that refactor TASK-231's operators into kernels that implement `StatelessKernel` and plug into a new fused-segment driver, **not** leave §3.8 to a later wave. See also the TASK-242 retirement stub for the history of how this design got deferred from Wave 2.

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

### TASK-599: [IMPL] Wave 5 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-501, TASK-502, TASK-503, TASK-504, TASK-505, TASK-506, TASK-507
**Description**: Same audit pattern as TASK-199, rescored after Wave 5. Wave 5 is the production-quality wave — the audit is a hard gate, not a reflective pass. Every crate is expected to be at least B across all dimensions; anything below B ships only with a named owner, a concrete remediation plan, and human sign-off before Wave 6 begins. The Benchmarks dimension specifically verifies that regression gates are wired up in CI and have been green for at least one full merge cycle. Any below-B grade is a blocker, not a follow-up.

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

### TASK-699: [IMPL] Wave 6 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-601, TASK-602, TASK-603, TASK-604
**Description**: Same audit pattern as TASK-199, rescored after Wave 6. This is the first wave where the top-level `bqlite` re-export crate and `bqlite-ffi` move from compile-only scaffolds to real content — both are expected at B or better, and the audit is the place to stop deferring their grades with `-`. CLI UX polish and Python API ergonomics are scored under API, not Docs. Python packaging (wheel build success on both target platforms) counts as a Tests-dimension signal for bqlite-ffi. Below-B grades block Wave 7 from starting.

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

### TASK-799: [IMPL] Wave 7 quality audit — shippable grade
**Output**: docs/quality-score.md
**Depends on**: TASK-701, TASK-702, TASK-703, TASK-704
**Description**: Final pre-ship audit. Same audit pattern as TASK-199, but the standard is an A on every dimension for every crate on the public surface (bqlite, bqlite-core, bqlite-cli, bqlite-ffi). Internal crates may ship at B only if the concrete gap keeping them from A is documented with a rationale and a post-1.0 follow-up task. The public benchmark report from TASK-701 supplies the Benchmarks evidence for the entire workspace. Anything below this standard blocks release — the audit is the last green light before tagging 1.0.
