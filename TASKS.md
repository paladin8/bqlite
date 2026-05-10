# bqlite — Task List

This file is the authoritative plan for bqlite. It lists every wave and every known task, and is the reference both humans and autonomous agents consult to decide what to build next.

**It does not track task status.** Lock files in `tasks/active/` and done markers in `tasks/completed/` are the source of truth for what is claimed and what is complete. For the execution protocol — task claiming, checkpoints, git workflow — see [AGENTS.md](AGENTS.md).

The plan will be revised as work progresses. Later waves are intentionally loose; they get fleshed out as earlier waves ship and we learn what was actually needed.

> **Structural rule: waves are flat task lists, never sub-organized.** Do not group tasks within a wave under "phases", "tracks", "milestones", or any other sub-heading. Each task stands alone with its `Depends on:` set — that's the only structure. Agents pick work by scanning for unclaimed tasks whose dependencies are satisfied; any extra hierarchy gets in the way of that scan and invites drift between the grouping and the real dependency graph. When a wave starts to feel like it needs sub-sections, that's a signal to either (a) split it into two waves or (b) trust the dependency edges to do the organizing.

## Formatting Contract

`scripts/agents/task_tool.py` parses this file directly, so keep task records machine-readable as well as human-readable. The parser is intentionally simple; if you need to change the format, update the script in the same change.

- Every task header must stay on one line in the form `### TASK-NNN: [TAG][TAG] Title` or `### TASK-NNN: Title`.
- Put all tags in the header immediately after `TASK-NNN:` with no prose between them. Keep tags bracketed as `[TAG]`; do not use bullets or inline code for tags.
- Every task must include exactly one `**Depends on**:` line. Use either `none` or a comma-separated list of task IDs such as `TASK-101, TASK-102`.
- Keep dependency IDs on that single `**Depends on**:` line. Do not continue them onto following lines or replace them with prose like "same as above".
- Claimable implementation tasks should always carry exactly one difficulty tag: `[EASY]` or `[HARD]`. If a task is intentionally unroutable for the fleet, say so explicitly in the task text and expect agents to stop for input.
- Retired tasks should keep their `### TASK-NNN:` header and use the `[RETIRED]` tag so numbering remains stable.
- `**Output**:` and `**Description**:` labels should keep their current spelling so humans and tools can find them consistently, even though the task tool only requires the header and `Depends on` line today.

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
2. **Explicit dependencies.** Each task lists its `Depends on:` set. Tasks with no internal deps are the parallelism budget for the wave. Dependencies are **intra-wave only** — since waves execute sequentially (Rule 5), every task in a prior wave is guaranteed complete before the current wave starts, so cross-wave edges are redundant and clutter the dependency graph.
3. **Self-contained checkpoints.** The implementing agent breaks the task into checkpoints per AGENTS.md — compile, test, lint, merge to main.
4. **Names its key output paths.** Code tasks name the 1-2 primary files they create or substantially modify (not every touched file). Design tasks name the doc path they produce.

### Task tags

Tasks may carry one or more tags in their header:

- `[EASY]` — routing hint for the autonomous fleet: default to the cheaper execution pool (`sonnet` at `high` effort). Use for implementation work whose shape is already well-resolved by the existing design docs and task text, even when the task is still real engineering work. Local parser/planner/operator/storage tasks often remain `[EASY]` if the spec is concrete and mistakes are cheap to catch with normal tests and review. `EASY` does **not** mean "trivial" or "safe to skip review/CI" — it is only a model-selection hint.
- `[HARD]` — routing hint for the autonomous fleet: default to the expensive reasoning pool (`opus` at `high` effort). Reserve this for work where extra reasoning depth is likely to materially change the outcome: all `[DESIGN]` tasks, trait/interface freezes, on-disk format changes, crash-safety or recovery logic, concurrency/atomicity semantics, novel algorithms, performance-gate work, whole-wave acceptance/audit gates, or anything whose wrong first pass is expensive to unwind. A task is **not** `[HARD]` merely because it touches multiple files or takes a day or two.
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

### TASK-101: [EASY][IMPL] Dependency direction check
**Output**: scripts/check-dep-direction.sh, .github/workflows/ci.yml
**Depends on**: none
**Description**: Script walks each crate's Cargo.toml, verifies internal deps match the dependency graph in docs/architecture.md, and fails with a clear error on violation. Wire as a CI step alongside the existing build/test/clippy/fmt jobs.

### TASK-102: [EASY][IMPL] Error type hierarchy
**Output**: crates/bqlite-core/src/error.rs
**Depends on**: none
**Description**: `thiserror`-based `BqliteError` enum in bqlite-core, re-exported from the top-level crate. Covers I/O, schema mismatches, parse errors, plan errors, execution errors, cancellation. Conversion impls from `std::io::Error` and `arrow::error::ArrowError`.

### TASK-103: [EASY][IMPL] Timestamp and time-range types
**Output**: crates/bqlite-core/src/time.rs
**Depends on**: none
**Description**: `Timestamp` newtype over `i64` epoch nanoseconds, UTC, matching the `Timestamp(Nanosecond, Some("UTC"))` Arrow mapping frozen in docs/design/type-system.md §2.1-§2.2. `TimeRange` with inclusive/exclusive bounds, ordering, arithmetic helpers (duration math stays in i64 nanos — no Duration type per type-system.md §2.2). Serde impls for debug/logging.

### TASK-104: [EASY][IMPL] PropertyValue type
**Output**: crates/bqlite-core/src/property.rs
**Depends on**: TASK-102
**Description**: Scalar variants (bool, int, float, string, timestamp), null, list, map. Follows the type system defined in docs/design/type-system.md. Includes equality, ordering, and Display impls.

### TASK-105: [EASY][IMPL] EntityId and Event primitives
**Output**: crates/bqlite-core/src/event.rs
**Depends on**: TASK-103, TASK-104
**Description**: `EntityId` newtype, `Event { entity, timestamp, type, properties }`, and an entity-aligned iteration trait that later operators implement. Zero-copy where possible.

### TASK-106: [EASY][IMPL] TableSchema and OperatorSchema
**Output**: crates/bqlite-core/src/schema.rs
**Depends on**: TASK-104
**Description**: Two schema types per docs/design/type-system.md §5:
- `ColumnDef` + `TableSchema` — declared table shape, designated entity-id column, timestamp column, event-type column, per-column property schema, `__seq_id`/`__batch_id` system columns, and the schema-creation-time validation rules (§5.1).
- `OperatorSchema` — the contract between piped operators (§5.2): ordered `Vec<ColumnDef>` output shape, `column(name)` lookup, `to_arrow_schema()`, and `validate_against(required)` compatibility check.

Both types are foundational: TableSchema is what the catalog (TASK-125) returns, OperatorSchema is what the planner propagates through the plan tree (used by TASK-108 and the Wave 2 logical-plan work).

### TASK-107: [EASY][IMPL] Arrow type mapping
**Output**: crates/bqlite-core/src/arrow.rs
**Depends on**: TASK-104, TASK-106
**Description**: Bidirectional conversion between `PropertyValue`/`TableSchema` and Arrow `DataType`/`Schema`. Handles nested types (list, map) and null semantics.

### TASK-108: [HARD][DESIGN][TRAIT] PhysicalOperator + EntityOperator traits
**Output**: docs/design/operators/operator-traits.md, crates/bqlite-operators/src/operator.rs
**Depends on**: TASK-105, TASK-106
**Description**: The core execution contract. Design note first checkpoint covers: pull-based iterator protocol, `OperatorSchema` propagation, open/next/close lifecycle, error propagation, cancellation hook, the entity-aligned batching layer `EntityOperator` adds on top, and sub-batch streaming. Impl second checkpoint lands the trait definitions in `bqlite-operators` per docs/design/execution-model.md §13.2 module map — the trait cannot live in `bqlite-engine` because `bqlite-operators` does not depend on `bqlite-engine`, so placing the trait in engine would block operator impls (TASK-117) from implementing it. Also file a follow-up doc task to correct docs/design/planner-pipeline.md §15 line 1397 which inconsistently lists the trait in `bqlite-engine`. Merge-first — downstream operator, planner, and engine tasks depend on this.

### TASK-109: [HARD][DESIGN][TRAIT] SegmentReader trait
**Output**: docs/design/storage/reader-trait.md, crates/bqlite-core/src/storage.rs
**Depends on**: TASK-106, TASK-107
**Description**: Storage API consumed by scan operators. Design note covers: segment enumeration, column projection, row-group iteration, zone-map access hook, predicate pushdown hook. Impl lands the trait. Merge-first.

### TASK-110: [HARD][TRAIT] DemandCapabilities protocol scaffold
**Output**: crates/bqlite-core/src/demand.rs
**Depends on**: TASK-108
**Description**: Placeholder enum + propagation trait so operator stubs can implement it from day 1. Real protocol details (which capabilities exist, how they propagate, the fusion implications) are resolved by a [DESIGN] task in a later wave. Keep the v0 surface minimal so we can extend without breaking existing impls.

### TASK-111: [HARD][TRAIT] MemoryBudget trait
**Output**: crates/bqlite-core/src/memory.rs
**Depends on**: TASK-102
**Description**: Byte-accounting trait, reservation API, spill notification hook. Stub enforcement only — the real enforcement model is designed in Wave 5.

### TASK-112: [HARD][TRAIT] Metrics trait
**Output**: crates/bqlite-core/src/metrics.rs
**Depends on**: TASK-108
**Description**: Per-operator metric counters (rows in, rows out, bytes, wall time), query-level aggregation hook. Designed to compose with the telemetry setup in TASK-122.

### TASK-113: [EASY][IMPL] AST node skeletons
**Output**: crates/bqlite-ast/src/lib.rs
**Depends on**: TASK-104
**Description**: Statement, expression, pattern, and source-reference AST types. Covers enough surface for Wave 2's grammar to slot in without restructuring. No parser logic — just the data types.

### TASK-114: [EASY][IMPL] Parser stub
**Output**: crates/bqlite-parser/src/lib.rs
**Depends on**: TASK-113
**Description**: Hand-rolled mini-parser accepting a single identifier — a bare table name like `events` — and producing the corresponding `Scan { table: "events" }` AST node. No keywords, no operators, no error recovery. This is deliberately throwaway; the real grammar framework is a Wave 2 [DESIGN] task.

### TASK-115: [EASY][IMPL] Planner stub
**Output**: crates/bqlite-planner/src/lib.rs
**Depends on**: TASK-113, TASK-125
**Description**: AST → logical plan stub → physical plan stub. `plan(statement, catalog: &dyn Catalog)` entry point resolves the scanned table via the catalog (returning a `TypeError` for unknown tables, per planner-pipeline.md §4.1), builds a minimal logical node enum (just `Scan { schema: TableSchema }` for now), and lowers it one-to-one to a plain-data physical descriptor (`ScanPhysical`) per planner-pipeline.md §15 — the planner emits plain data, not trait objects. No optimizer pass. The returned physical descriptor is consumed by the engine's bind step (TASK-118). Does not depend on TASK-108 directly because the planner never holds a `PhysicalOperator` value.

### TASK-116: [HARD][IMPL] Storage stub and database bootstrap
**Output**: crates/bqlite-storage/src/{lib,database,manifest}.rs
**Depends on**: TASK-106, TASK-109
**Description**: `Database::open_or_create(path)` implements the full v0 database-open contract from docs/design/storage-format.md §5 + §12 + §14 and docs/reliability.md — even though nothing is stored yet, Wave 1 freezes the on-disk shape so later waves don't have to retrofit it:

- **Directory layout.** Create `<path>/`, `<path>/manifest.json`, and acquire `<path>/.lock` via `flock()` (storage-format.md §14.1). Release the lock on drop. A second concurrent open returns a clear error.
- **Manifest contents on empty-database init.** Write `manifest.json` with: `format_version: 1` (reliability.md §Versioning), a freshly generated `database_uuid` (v4, never rotates — storage-format.md §5.1), `shard_count: 32` with override hook for future `bqlite init --shards N`, an empty `tables: {}` map (populated by TASK-125's bootstrap), per-table counters `{ next_seq_id: 0, next_batch_id: 0 }` ready to be added, and a `segments: []` inventory.
- **Manifest atomicity.** Writes go `manifest.json.tmp` → `fsync` → `rename` per storage-format.md §12.3.
- **Open behavior.** Existing databases load `manifest.json` and validate `format_version` (rejecting unknown versions). Empty or missing directory triggers init. Corrupted manifest returns a typed error.
- **SegmentReader.** Implements TASK-109's trait returning an empty segment iterator. No real format yet.

The smoke test (TASK-123) depends on this: it creates a fresh temp directory, opens it, and must observe a valid, versioned, UUID-stamped manifest that later waves can keep extending.

### TASK-117: [EASY][IMPL] Operator stubs
**Output**: crates/bqlite-operators/src/{scan,filter,project}.rs
**Depends on**: TASK-108, TASK-109
**Description**: Scan/filter/project operators implementing `PhysicalOperator`. Scan actually calls into `SegmentReader::segments()` and drives the iterator (not hard-coded to return empty). Filter and project wrap a child operator and are no-ops in this stub. Gives downstream planner and engine real types to wire to.

### TASK-118: [HARD][IMPL] Engine stub, query API, and physical-plan bind step
**Output**: crates/bqlite-engine/src/{lib,query,bind}.rs, crates/bqlite-engine/Cargo.toml, docs/architecture.md, CLAUDE.md
**Depends on**: TASK-114, TASK-115, TASK-117, TASK-125
**Description**: Engine's public `Engine::query(text: &str, db: &Database) -> Result<ExecutionResult>` entry point — the single surface the CLI, Python bindings, and eventually the top-level `bqlite` crate call. Internally it:

1. **Parses** the text via `bqlite-parser` into a `Statement` (Wave 1 accepts the one-identifier grammar from TASK-114).
2. **Plans** by calling `bqlite-planner` with the database's `&dyn Catalog` (from TASK-125), producing the plain-data physical descriptor per planner-pipeline.md §15.
3. **Binds** the plain-data descriptor into a `Box<dyn PhysicalOperator>` tree. The bind step lives in engine per planner-pipeline.md §15 line 1404 — planner never holds trait objects. For Wave 1 the only binding is `ScanPhysical` → `bqlite_operators::ScanOperator`.
4. **Drives** the resulting operator tree to completion, collecting output batches, returning `ExecutionResult { schema: OperatorSchema, rows: Vec<RecordBatch> }`.

No memory management, no concurrency, no cancellation yet.

**Crate-boundary change.** Adds `bqlite-parser` to `bqlite-engine`'s `Cargo.toml` and updates the dependency graphs in `docs/architecture.md` and `CLAUDE.md` to show `bqlite-engine → parser, planner, operators, storage, core`. This preserves the `bqlite-cli → engine` constraint (architecture.md line 30) while giving engine a single text-in, rows-out API — without this, CLI would need direct parser/planner deps, which the architecture forbids. TASK-101's dep-direction check must be updated in the same PR.

### TASK-119: [EASY][IMPL] CLI stub
**Output**: crates/bqlite-cli/src/main.rs
**Depends on**: TASK-118, TASK-122
**Description**: `bqlite query "<bql>" --db <path>` subcommand. Opens the database via `bqlite_engine::Database::open_or_create(path)`, calls `engine.query(text, &db)` — the single text-in entry point from TASK-118 — and prints the `ExecutionResult` as a simple text table (even if empty: "0 rows"). CLI only depends on `bqlite-engine` per architecture.md line 30; it does not import `bqlite-parser` or `bqlite-planner` directly. Initializes the tracing subscriber from TASK-122.

### TASK-120: [EASY][IMPL] Integration test fixture framework
**Output**: tests/common/mod.rs
**Depends on**: TASK-106
**Description**: Temp-dir database helpers, fixture loader stub (CSV support lands in Wave 2 — Wave 1 just provides the harness), assertion helpers that compare result sets by value. Documents the integration-test pattern that later waves copy.

### TASK-121: [EASY][IMPL] Benchmark harness
**Output**: benches/README.md, benches/common/mod.rs, root Cargo.toml bench entries
**Depends on**: TASK-101
**Description**: Criterion set up in the workspace, per-crate bench harness pattern documented so later waves drop microbenchmarks in without thinking about it. Smoke benchmark that measures a no-op so the harness itself is exercised in CI.

### TASK-122: [EASY][IMPL] Logging and tracing setup
**Output**: crates/bqlite-core/src/telemetry.rs
**Depends on**: TASK-102
**Description**: `tracing` crate wiring — env-controlled level (`BQLITE_LOG`), a `tracing_subscriber` that writes to stderr, a query-level span with structured fields (query_id, query_text), operator-level child spans. CLI initializes the subscriber at startup. Later waves extend this with the metrics-to-span bridge, so getting the surface right now avoids a [TRAIT] task later.

### TASK-123: [EASY][IMPL] End-to-end smoke test
**Output**: tests/smoke.rs
**Depends on**: TASK-119, TASK-120
**Description**: Runs `bqlite query "events"` against an empty database directory (created on the fly) and asserts OK + empty result. This is the Wave 1 acceptance gate — if this passes, Wave 1 is done.

### TASK-124: [EASY][IMPL] Property-test harness
**Output**: tests/prop/mod.rs, tests/prop/property_value.rs
**Depends on**: TASK-104
**Description**: Adds `proptest` as a dev-dep, writes one round-trip test on `PropertyValue` as a template, documents the pattern in the `bqlite-tests` package README (now at tests/README.md after the post-TASK-124 restructure). Later waves add real property tests for storage encodings, parser round-trips, and the sequence matcher.

### TASK-125: [HARD][IMPL] Catalog trait and bootstrap events table
**Output**: crates/bqlite-core/src/catalog.rs, crates/bqlite-storage/src/catalog.rs
**Depends on**: TASK-106, TASK-116
**Description**: Resolves the gap between "the planner requires a `Catalog` handle to resolve tables" (planner-pipeline.md §4.1 line 200) and "database initialization is CLI-only, no BQL DDL in v0" (query-language.md §29 line 1911) — without which the Wave 1 smoke test `bqlite query "events"` cannot parse-plan-execute.

- **`Catalog` trait in `bqlite-core`.** Minimal surface: `resolve_table(name: &str) -> Result<TableSchema, TypeError>`, `list_tables() -> Vec<&str>`. The planner takes `&dyn Catalog` at plan time and never depends on storage directly (preserving the planner → ast, core dependency rule).
- **Manifest-backed impl in `bqlite-storage`.** Reads the `tables: { <name>: TableSchema }` map from the manifest written by TASK-116. The impl is the value returned by `Database::catalog() -> &dyn Catalog`.
- **Bootstrap rule.** When `Database::open_or_create(path)` initializes a fresh manifest (TASK-116), it seeds the `tables` map with a single default `events` table whose schema is the minimum required by type-system.md §5.1 validation: `entity_id STRING NOT NULL (ENTITY KEY)`, `ts TIMESTAMP NOT NULL (EVENT TIME)`, `event_type STRING NOT NULL (EVENT TYPE)`. This is a Wave 1 shortcut — proper `CREATE TABLE` DDL execution is Wave 2's parser + planner work. The bootstrap rule is documented in the manifest as `bootstrap_events_table: true` so later waves can distinguish seeded state from user state and retire the shortcut cleanly.

Unlocks: TASK-115 planner stub (needs `Catalog` to resolve `events`), TASK-118 engine query API (wires catalog into the planner call), TASK-123 smoke test (needs a resolvable `events` table in a freshly created database).

### TASK-199: [HARD][IMPL] Wave 1 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-123
**Description**: Wave-closing reflective pass on per-crate quality. Score every crate in the workspace on each dimension in docs/quality-score.md (Tests, API, Docs, Benchmarks) and assign an overall A-F grade. Gather evidence with `cargo test -p <crate>` (test count + pass rate), `cargo bench -p <crate> --no-run` (bench presence), and rustdoc coverage of public items. Record a one-line justification per cell — extend the table format if a flat grade cell is too terse to be useful. If any crate lands below C on any dimension, file a follow-up task addressing the gap (same or next wave) rather than silently accepting the grade. Wave 1 is not declared done until this audit lands and any below-C follow-ups are at least filed.

---

## Wave 2: Scan & Filter MVP

**Goal.** Real queries return real data over user-declared schemas. Segment format v1 with the full v1 encoding set, CSV ingest with column remapping, schema DDL (`CREATE TABLE`, `DROP TABLE`, `ALTER TABLE ADD COLUMN`, `DESCRIBE`), `INSERT` (both `VALUES` and `FROM`), `EXPLAIN`, explicit `bqlite init` / split `Database::open` and `Database::create`, retirement of the Wave 1 bootstrap `events` table, scan + filter + select + limit operators, predicate pushdown, projection pruning, zone-map-based row-group skipping, startup reconciliation of orphaned segment files.

**Scope exclusions.** `DELETE` is deferred to Wave 4 alongside tombstones (TASK-404 / TASK-433 territory) — the AST already models it, but without the tombstone format on disk there is nothing for the planner to lower it onto. Wave 2 parsers and planners therefore do not handle `DELETE`.

**Size.** ~48 tasks (TASK-242 retired during post-Wave-2 architecture reconciliation; TASK-244 through TASK-248 were added after post-wave acceptance validation found remaining correctness/performance gaps).
**Parallelism.** 10-14 agents at peak.

**Acceptance.** The following script runs end-to-end against a database created via `bqlite init /path/to/db` and returns the expected rows. Surface keywords match the grammar in query-language.md §26: `WHERE` for row filtering, `INSERT ... VALUES` for literal tuples, `WITH (k: v, ...)` option lists using `:` as the key/value separator.

```bql
CREATE TABLE purchases (
  user_id STRING ENTITY KEY,
  ts TIMESTAMP EVENT TIME,
  event_type STRING EVENT TYPE,
  amount INT,
  category STRING
);

-- Small literal insert for REPL-style tests
INSERT INTO purchases VALUES
    ('user_0', 1700000000000000000, 'purchase', 999, 'manual'),
    ('user_1', 1700000001000000000, 'refund', NULL, NULL);

-- Bulk load from file with column remapping
INSERT INTO purchases
FROM 'data.csv'
WITH (format: 'csv', map: (uid AS user_id, ts_ns AS ts, kind AS event_type));

purchases
| where event_type = 'purchase' AND amount > 4500
| select user_id, amount
| limit 10;

EXPLAIN purchases
| where event_type = 'purchase' AND amount > 4500
| select user_id, amount
| limit 10;

DESCRIBE purchases;
ALTER TABLE purchases ADD COLUMN source STRING;
DROP TABLE purchases;
```

Source columns not named in the `map` clause pass through if their name matches a table column; otherwise INSERT errors. The CLI auto-injects `LIMIT 1000` when a query has no explicit limit and prints a truncation footer; `--no-limit` and `--limit N` override. A `bqlite query` call against a directory that does not yet contain a manifest returns a typed error pointing at `bqlite init`, not an implicit fresh database.

**Performance gate** (blocks wave acceptance; verified by TASK-236 on the reference dataset):

*Reference dataset:* 100M-row synthetic `purchases` stream — 10k distinct `user_id`, 20 distinct `event_type` values, timestamps spanning 90 days monotonic-within-entity, 7 property columns of mixed types (ints, floats, low-cardinality strings), materialized as CSV on local NVMe.

*Reference hardware:* Apple M2 Max, macOS 14+, release build (`cargo bench --profile=release-lto`), `/tmp` on APFS SSD. CI gate runs on GitHub Actions `ubuntu-latest` (4 vCPU) with a **1.5× relaxed target** across all numbers below.

| Metric | Target |
|---|---|
| End-to-end acceptance query (cold cache, single thread) | **< 1 s** |
| Columnar scan decode, int64, no predicate | ≥ 200M rows/sec |
| Filter with pushed-down equality on dictionary-encoded column | ≥ 500M rows/sec effective |
| CSV ingest throughput (parse → sort → encode → write) | ≥ 100 MB/s |
| Compression ratio (segment bytes / raw CSV bytes) | **≤ 10%** |
| Zone-map pruning effectiveness on the acceptance query | ≥ 80% of row-groups skipped |

Regression gate triggers if any bench slips >10% vs. the previous green main. The bench suite itself is TASK-236; the CI job, baseline capture, and comparison machinery that enforce the gate are TASK-241.

**Post-wave acceptance reconciliation.** Validation after the nominal Wave 2 close found that the checked-in runtime still returned zero rows through `Database::segment_reader()`, the acceptance test was asserting manifest row counts rather than exact query results, and the benchmark harness was only exercising scaled-down fixtures without load-bearing target failures. TASK-244 through TASK-248 close those gaps; Wave 2 is not complete until they land.

Wave 2 is where the real interfaces get decided, so design anchors are front-loaded. After the anchors land, the encoding and storage tasks form the longest parallelism vein — the 6 encoding tasks plus the writer/reader/zone-map/manifest tasks give 10+ agents work the moment the trait lands. Rule 5 applies: Wave 2 does not begin until every Wave 1 task is complete.

### TASK-201: [HARD][DESIGN] Segment format v1
**Output**: docs/design/storage/segment-format-v1.md
**Depends on**: TASK-109
**Description**: Finalize the byte-level v1 layout per storage-format.md §9: file header (magic + version), row-group size (65,536 rows), column chunk header (encoding descriptor, compression, null bitmap, row count, byte range), zone-map block (min/max per column per row-group), footer (schema, row-group index, dictionaries, checksum, footer length, trailing magic). Encoding set is frozen at **Plain, Dictionary, Delta, BitPacking, Constant** with **LZ4** as the post-encoding compression layer. No FSST/ALP/PFOR/FOR/DoubleDelta/RLE/Frequency — those are Wave 4. Unblocks every other Wave 2 storage task.

### TASK-202: [HARD][DESIGN] Scan interface and predicate pushdown protocol
**Output**: docs/design/storage/predicate-pushdown.md
**Depends on**: TASK-109, TASK-201
**Description**: How the scan operator asks the storage layer to push down equality, range, and set predicates. Scan-side capability advertisement (which `CompiledExpr` shapes a scan accepts), zone-map evaluation order, fallback to post-filter when a predicate can't be pushed, interaction with dictionary-encoded columns (predicate rewritten against the dictionary). Cross-cutting between storage, operators, and planner — risky.

### TASK-203: [HARD][DESIGN] Parser grammar framework
**Output**: docs/design/language/grammar-framework.md
**Depends on**: TASK-114
**Description**: Decides hand-rolled vs parser generator (chumsky/pest/lalrpop/nom), error-recovery strategy (Wave 0 language doc pins "halt on first error" — design must confirm), span tracking for diagnostics, how new productions are added, and the surface for the colon-separated WITH option list `WITH (format: 'csv', map: (src AS dst, ...))` whose AST shape is fixed by TASK-237. Unblocks every post-stub parser task across Waves 2-4.

### TASK-204: [HARD][DESIGN] Logical plan node catalog (Wave 2 subset + forward map)
**Output**: docs/design/planner/logical-plan-nodes.md
**Depends on**: TASK-115
**Description**: Comprehensive enumeration of logical plan nodes expected across the project (Scan, Filter, Project, Limit, CreateTable, DropTable, AlterTableAddColumn, Describe, Insert, Explain at Wave 2 depth; Match/Funnel/Aggregate/Sessionize/Retention/Sort/Distinct/Cohort/Delete stubbed for later waves — Delete pairs with Wave 4's tombstone work). For each: input/output schemas, which AST constructs lower to them, the rewrite rules that apply. Wave 2 implements the subset marked "Wave 2 depth"; the rest are documented so the catalog doesn't churn across waves.

### TASK-205: [HARD][DESIGN] Expression compilation model
**Output**: docs/design/planner/expression-compilation.md
**Depends on**: TASK-115, TASK-204
**Description**: AST expression → `TypedExpr` (schema-resolved, type-checked per type-system.md §10) → `CompiledExpr` (runtime-evaluable over Arrow batches). Defines the `CompiledExpr` struct that ScanPhysical/FilterPhysical/ProjectPhysical all carry. Compilation-target selection between Arrow compute kernels and monomorphized hot paths, null propagation under three-valued logic, and how compiled predicates are surfaced to the pushdown protocol from TASK-202. Unblocks TASK-225 and everything downstream of the expression compiler.

### TASK-206: [EASY][IMPL] Encoding trait + Plain reference impl + property-test pattern
**Output**: crates/bqlite-storage/src/encoding/{mod,plain}.rs
**Depends on**: TASK-201
**Description**: The `Encoding` trait (encode/decode/estimate_size/applicable_to), the Plain reference implementation covering every primitive type the v1 format supports, and the round-trip property-test pattern (via the TASK-124 harness) that every subsequent encoding task copies. Establishes the pattern that every other encoding in Wave 2 and in Wave 4's advanced-encoding work follows. No selector logic — that's TASK-212.

### TASK-207: [EASY][IMPL] Dictionary encoding
**Output**: crates/bqlite-storage/src/encoding/dictionary.rs
**Depends on**: TASK-206
**Description**: Per-column-chunk dictionary + bit-packed code stream. Handles strings and low-cardinality ints. Round-trip property tests per the TASK-206 pattern. Surfaces dictionary to segment footer so predicates can be rewritten against codes at pushdown time (TASK-202).

### TASK-208: [EASY][IMPL] Delta encoding
**Output**: crates/bqlite-storage/src/encoding/delta.rs
**Depends on**: TASK-206
**Description**: First-value + bit-packed deltas. Primarily for monotonic-ish timestamps within an entity. Handles signed overflow. Round-trip property tests.

### TASK-209: [EASY][IMPL] BitPacking encoding
**Output**: crates/bqlite-storage/src/encoding/bitpacking.rs
**Depends on**: TASK-206
**Description**: Variable-width bit packing for small integer ranges. Width derived from min/max of the chunk. Round-trip property tests including all-zero, single-value, and full-width edge cases.

### TASK-210: [EASY][IMPL] Constant encoding
**Output**: crates/bqlite-storage/src/encoding/constant.rs
**Depends on**: TASK-206
**Description**: Zero-data encoding for chunks where every non-null value is identical. Stores the constant in the chunk header. Null mask handled separately. Round-trip property tests.

### TASK-211: [EASY][IMPL] LZ4 compression wrapper
**Output**: crates/bqlite-storage/src/encoding/lz4.rs
**Depends on**: TASK-206
**Description**: Post-encoding compression layer applied to the encoded byte stream of any encoding. Uses `lz4_flex`. Configurable acceleration, defaults tuned for small chunks. Round-trip tests.

### TASK-212: [EASY][IMPL] Encoding selector heuristic
**Output**: crates/bqlite-storage/src/encoding/selector.rs
**Depends on**: TASK-207, TASK-208, TASK-209, TASK-210, TASK-211
**Description**: Per storage-format.md §10.3: sample a chunk, score each applicable encoding (encoded size + decode cost estimate), pick the lowest-bytes encoding with ties broken by decode cost. Decides whether to apply the LZ4 wrapper. Unit tests cover every encoding-pick path plus the degenerate "Plain is always a legal fallback" invariant.

### TASK-213: [HARD][IMPL] Segment file format writer (low-level)
**Output**: crates/bqlite-storage/src/segment/writer.rs
**Depends on**: TASK-201, TASK-206
**Description**: Takes encoded column chunks + chunk metadata and emits v1 on-disk bytes: file header, row groups, zone-map block, footer, trailing checksum + magic. Pure byte-layout code — no encoding selection, no sorting, no manifest interaction. Atomic via temp-file + rename. Unit tests assert byte-exact layout for a small hand-crafted row group.

### TASK-214: [HARD][IMPL] Entity-sorted segment writer orchestration
**Output**: crates/bqlite-storage/src/writer.rs
**Depends on**: TASK-212, TASK-213, TASK-217, TASK-218
**Description**: High-level writer — accepts a sorted `(entity_id, ts)` event stream from the partitioner (TASK-218), groups into row groups respecting entity boundaries, selects per-column encodings via TASK-212, emits bytes via TASK-213, and atomically registers the new segment in the manifest (TASK-217). Honors the entity-boundary invariant from storage-format.md §7.2 (no entity straddles a row-group unless it exceeds row-group size, in which case it occupies consecutive row groups within a segment). Integration tests validate round-trip through a real reader.

### TASK-215: [HARD][IMPL] Segment file reader + row-group iterator
**Output**: crates/bqlite-storage/src/segment/reader.rs
**Depends on**: TASK-201, TASK-206
**Description**: Parses a segment file's footer (schema, row-group index, dictionaries, zone maps), validates checksum + magic, and exposes a lazy row-group iterator that materializes column chunks on demand. Decodes chunks back to Arrow arrays via the encoding trait. Zero-copy where the encoding permits. Implements the `SegmentReader` trait from TASK-109.

### TASK-216: [HARD][IMPL] Zone map write + read + predicate pruning
**Output**: crates/bqlite-storage/src/zone_map.rs
**Depends on**: TASK-202, TASK-213, TASK-215
**Description**: Writer extracts per-column min/max per row group during encoding and emits them into the zone-map block in the footer. Reader loads zone maps eagerly at segment open (they're small). Pruning evaluator accepts a pushed-down `CompiledExpr` and returns the set of row groups that cannot be skipped. Correctness tests assert no false negatives (a row that would match must never be pruned) and measure false-positive rate against synthetic workloads.

### TASK-217: [HARD][IMPL] Manifest v1 segment inventory + atomic updates
**Output**: crates/bqlite-storage/src/manifest.rs
**Depends on**: TASK-116
**Description**: Extends TASK-116's bootstrap manifest to track segments per `(table, window, shard)` with full `SegmentMeta` per storage-format.md §12.3 (segment_id, level, schema_version, row_count, byte_size, ts_range, entity_range, per-column stats, created_at, batch_id). Atomic updates via `manifest.json.tmp` → `fsync` → `rename`. Read-write API: `add_segment`, `remove_segment`, `snapshot_for_query(table, time_range, shard)`. Concurrency controlled by the existing `flock` guard.

### TASK-218: [EASY][IMPL] Ingest partitioner
**Output**: crates/bqlite-storage/src/ingest/partitioner.rs
**Depends on**: TASK-217
**Description**: Routes incoming events to the correct `(shard, window)` bucket: `shard = xxhash64(entity_id) % shard_count` from the manifest config, `window = floor(ts / window_size)`. Buffers and sorts each bucket by `(entity_id, ts)` before handing the sorted stream to the writer (TASK-214). Memory-bounded — spills to an on-disk external sort when the buffer exceeds a configurable budget (stub: just error loudly for Wave 2; real spill is Wave 5). Assigns each ingest call a fresh `batch_id` from the manifest counter.

### TASK-219: [EASY][IMPL] K-way merge scan across L0 segments
**Output**: crates/bqlite-storage/src/segment/merge.rs
**Depends on**: TASK-215, TASK-217
**Description**: Given a shard and a time range, asks the manifest for all matching L0 segments and produces a merged `(entity_id, ts)`-ordered event stream across them. Loser-tree or binary-heap k-way merge; streaming, not materializing. Critical for Wave 2 because there's no compaction yet — every ingest produces a new L0 segment and queries must merge across all of them.

### TASK-220: [EASY][IMPL] Parser framework bootstrap + expression grammar
**Output**: crates/bqlite-parser/src/{lib,expr}.rs
**Depends on**: TASK-203
**Description**: Instantiates the framework chosen in TASK-203, replaces the Wave 1 one-identifier stub, and lands the expression grammar: literals (int, float, string, bool, timestamp, duration), identifiers, property access (`table.col`), unary/binary arithmetic, comparisons, `AND`/`OR`/`NOT`, parentheses, `IS NULL` / `IS NOT NULL`. Rich span tracking for diagnostics. Halt-on-first-error per language-doc §policy. Unit tests cover every operator precedence edge.

### TASK-221: [EASY][IMPL] Schema DDL + EXPLAIN productions
**Output**: crates/bqlite-parser/src/ddl.rs
**Depends on**: TASK-220
**Description**: All four schema-DDL parser productions plus EXPLAIN, matching the AST shapes already in `crates/bqlite-ast/src/statement.rs`:

- `CREATE TABLE <name> (<col> <type> [role] [NOT NULL] [DEFAULT <lit>], ...);` where `role` ∈ `ENTITY KEY | EVENT TIME | EVENT TYPE`. Multi-role-per-column validation is deferred to the planner.
- `DROP TABLE <name>;` — no `IF EXISTS` modifier per query-language.md §20.4.
- `ALTER TABLE <name> ADD COLUMN <col> <type> [NOT NULL] [DEFAULT <lit>];` — only `ADD COLUMN` in v1, lowering to the existing `AlterAction::AddColumn` AST variant.
- `DESCRIBE <name>;` — emits the catalog schema for a table (output columns documented in query-language.md §20.5).
- `EXPLAIN <pipeline>` — parser-level wrapper producing an `Explain(Pipeline)` AST node. Pipelines only; DDL/DML are not EXPLAIN-able in v1, matching `Statement::Explain(Pipeline)` in the AST.

Parser tests only — plan-time semantic validation is TASK-226 / TASK-232. `DELETE` is intentionally out of scope (deferred to Wave 4 with tombstones).

### TASK-222: [EASY][IMPL] INSERT FROM production with column remapping
**Output**: crates/bqlite-parser/src/dml.rs
**Depends on**: TASK-220, TASK-237
**Description**: `INSERT INTO <table> FROM <path-literal> WITH (<key>: <value>, ...);` matching the option syntax fixed in query-language.md §20.1 (colon-separated key/value pairs, not `=`). Recognized option keys include `format: 'csv' | 'jsonl' | 'parquet'`, `delimiter: <string>`, `header: <bool>`, and `map: (<src> AS <dst>, ...)`. The `map` clause uses the structured `(src AS dst, ...)` AST shape introduced by TASK-237 — without that prerequisite the AST cannot represent the mapping and this task will not compile. Unmapped source columns default to passthrough when the source name matches a table column. Parser tests for the happy paths and the obvious mistakes (trailing commas, duplicate source names, missing WITH, unknown format, malformed `map` entry). Plan-time validation of the actual mapping against the target table schema is TASK-226.

### TASK-223: [EASY][IMPL] Pipeline + where/select/limit productions
**Output**: crates/bqlite-parser/src/pipeline.rs
**Depends on**: TASK-220
**Description**: The `|` pipeline operator and the Wave 2 verbs, matching the BQL grammar in query-language.md §26: `where <expr>`, `select <col-or-expr-with-alias>, ...`, `limit <int>`. Keywords are case-insensitive (§29 line 1916). The AST stages these into `PipelineStage::{Where, Select, Limit}` per `crates/bqlite-ast/src/operator.rs`. A pipeline starts with a table reference (identifier) and chains verbs. Parser tests cover associativity, nested expressions in WHERE, multi-column select with `expr AS alias`, and edge cases (empty pipeline, limit without argument).

### TASK-224: [EASY][IMPL] Logical plan enum + AST → logical lowering
**Output**: crates/bqlite-planner/src/logical.rs
**Depends on**: TASK-204
**Description**: Concrete `LogicalPlan` enum for Wave 2 scope: `Scan`, `Filter`, `Project`, `Limit`, `CreateTable`, `DropTable`, `AlterTableAddColumn`, `Describe`, `Insert`, `Explain`. Each node carries its `OperatorSchema` computed at construction time (planner-pipeline.md §5) — for DDL nodes that produce no rows, the schema is an empty or single-status-column shape per query-language.md §20.4-§20.5. Lowering walks an AST statement and produces the root `LogicalPlan`, resolving table names via the catalog (and reporting `unknown table` for missing references). `Insert` lowering handles both `InsertBody::Values` and `InsertBody::From`. Schema validation happens at construction, not as a separate pass.

### TASK-225: [HARD][IMPL] Expression compilation (TypedExpr + CompiledExpr)
**Output**: crates/bqlite-planner/src/expr.rs
**Depends on**: TASK-205
**Description**: Implements TASK-205's design: schema-resolved `TypedExpr` construction with type checking against the scalar-function catalog, and compilation into `CompiledExpr` (the runtime form carried by physical operators). Dispatches between Arrow compute kernels and monomorphized hot paths. Handles three-valued-logic null propagation. Surfaces a `supported_pushdown_shape()` query so the pushdown pass (TASK-227) can ask "can storage evaluate this?".

### TASK-226: [EASY][IMPL] Physical plan descriptors + logical → physical lowering
**Output**: crates/bqlite-planner/src/physical.rs
**Depends on**: TASK-224, TASK-225
**Description**: Plain-data physical descriptors per planner-pipeline.md §15: `ScanPhysical`, `FilterPhysical`, `ProjectPhysical`, `LimitPhysical`, `CreateTablePhysical`, `DropTablePhysical`, `AlterTableAddColumnPhysical`, `DescribePhysical`, `InsertPhysical`, `ExplainPhysical`. No trait objects — descriptors are `Clone + Serialize`. Lowering converts each `LogicalPlan` node to its physical counterpart, invoking the expression compiler (TASK-225) to produce the `CompiledExpr` values the descriptors carry. DDL/DML nodes get simple one-to-one lowerings; real execution logic lives in the engine.

### TASK-227: [EASY][IMPL] Predicate pushdown optimizer pass
**Output**: crates/bqlite-planner/src/opt/pushdown.rs
**Depends on**: TASK-202, TASK-226
**Description**: Walks the physical plan looking for `FilterPhysical` directly above a `ScanPhysical`. For each conjunct, asks TASK-202's protocol "can the scan evaluate this?". Pushable conjuncts move into `ScanPhysical.scan_predicates`; the residue stays in `FilterPhysical` (which is elided if nothing remains). Does not mutate trees where selectivity is unknown — conservative per Wave 2 scope. Unit tests include the zero-residue, full-residue, and partial-pushdown cases.

### TASK-228: [EASY][IMPL] Projection pruning optimizer pass
**Output**: crates/bqlite-planner/src/opt/prune.rs
**Depends on**: TASK-226
**Description**: Backward demand-set collection per planner-pipeline.md §6.6 Pass 4. Walks the physical plan bottom-up, accumulating the set of columns each operator needs, forwarding demand through operators that pass columns through. At `ScanPhysical`, the accumulated demand becomes `projected_columns`. Unit tests verify that a query selecting 2 columns from a 10-column table results in a scan reading exactly 2 columns.

### TASK-229: [EASY][IMPL] EXPLAIN tree builder + text formatter
**Output**: crates/bqlite-planner/src/explain.rs
**Depends on**: TASK-227, TASK-228
**Description**: When planning an `Explain` node, capture the plan tree after every optimizer pass and build a structured `ExplainNode` tree annotating: logical nodes, optimizer rewrites (pushed-down predicates, pruned columns), and the final physical descriptors. Text formatter renders the tree as indented plain text — no Unicode polish. Used by the CLI and by tests to verify optimizer behavior declaratively.

### TASK-230: [HARD][IMPL] Entity-sorted scan operator
**Output**: crates/bqlite-operators/src/scan.rs (replaces Wave 1 stub)
**Depends on**: TASK-202, TASK-216, TASK-219, TASK-225
**Description**: Full entity-sorted scan over real segments. Consults the manifest for matching segments, opens them via TASK-215's reader, merges across segments via TASK-219's k-way merge, applies zone-map pruning via TASK-216, and evaluates pushed-down `CompiledExpr` predicates against materialized row groups. Respects `projected_columns` so only demanded columns are decoded. Foundation for every temporal operator in later waves.

### TASK-231: [EASY][IMPL] Filter + Project + Limit operators
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

### TASK-232: [HARD][IMPL] Engine bind step extension + DDL execution path
**Output**: crates/bqlite-engine/src/{bind,ddl}.rs
**Depends on**: TASK-217, TASK-226, TASK-230, TASK-231
**Description**: Extends TASK-118's bind step to materialize `Box<dyn PhysicalOperator>` from every Wave 2 physical descriptor. For data-plane nodes (`ScanPhysical`, `FilterPhysical`, `ProjectPhysical`, `LimitPhysical`), bind returns an operator tree. DDL nodes invoke execution closures that operate directly on the manifest via TASK-217's atomic update API:

- `CreateTablePhysical` — validates the schema, errors on name collision, atomically registers the new table.
- `DropTablePhysical` — errors on missing table, atomically removes the table entry. Wave 2 also drops the table's segments from the manifest inventory; on-disk segment files are reaped by TASK-239's startup orphan-cleanup pass on the next open.
- `AlterTableAddColumnPhysical` — errors on missing table or duplicate column name, appends the new `ColumnDef` to the schema, bumps `schema_version`, and atomically writes the manifest. The new column reads as NULL (or DEFAULT if specified) for all existing rows; no segment rewrite is needed because reads project by column name against the per-segment schema snapshot.
- `DescribePhysical` — looks up the table, formats its column metadata as the four-column result (`name`, `type`, `nullable`, `role`) per query-language.md §20.5, returns it as a single result batch.
- `ExplainPhysical` — formats the captured `ExplainNode` tree as a single-column result batch.

Unit tests for every descriptor variant including the error paths (missing table on DROP/ALTER/DESCRIBE, duplicate column on ALTER, name collision on CREATE).

### TASK-233: [HARD][IMPL] Engine INSERT execution + CSV reader integration
**Output**: crates/bqlite-engine/src/ingest.rs, crates/bqlite-storage/src/ingest/csv.rs
**Depends on**: TASK-214, TASK-218, TASK-222, TASK-232
**Description**: When the engine binds an `InsertPhysical` carrying an `InsertBody::From`, it opens the source file, constructs a streaming CSV reader (format-dispatched via the `format` key in the `WITH (...)` option list), applies the structured `map` clause from the AST shape introduced by TASK-237 to rename source columns, converts rows to `PropertyValue` per the target schema, and feeds the stream through the partitioner (TASK-218) into the writer (TASK-214). Source columns not in the map default to passthrough if their name matches a target column; extra source columns error. Missing required columns error. Type mismatches error with row numbers. CSV reader handles quoting, escaping, multi-line fields, and common delimiter variations. The literal `InsertBody::Values` arm is handled separately by TASK-238, which feeds its own row stream through the same partitioner + writer pipeline.

### TASK-234: [EASY][IMPL] CLI `ingest` subcommand + result formatter with auto-limit
**Output**: crates/bqlite-cli/src/{ingest,format}.rs
**Depends on**: TASK-233
**Description**: Two independent CLI extensions landing in one task because both are small:
- `bqlite ingest <path> --table <name> [--format csv|json|parquet] [--map "src=dst,..."]` — a thin wrapper that constructs the equivalent `INSERT INTO ... FROM ... WITH (...)` statement and hands it to `Engine::query`, so the CLI never touches parser/planner directly.
- Result formatter that, for any query with no explicit `| limit`, injects `LIMIT 1000` at the CLI boundary (not inside the engine) and prints a truncation footer like `... 49,999,000 rows omitted (use --limit N or --no-limit)`. Explicit `| limit` in the query suppresses auto-injection. `--no-limit` disables truncation; `--limit N` overrides the default.

### TASK-235: [HARD][IMPL] Wave 2 acceptance test + CSV fixture loader
**Output**: tests/tests/wave2_acceptance.rs, tests/src/csv.rs (new module re-exported from `tests/src/lib.rs`)
**Depends on**: TASK-234, TASK-238, TASK-240
**Description**: CSV fixture loader extends the Wave 1 harness (TASK-120) — deterministic synthetic-data generator producing the `purchases` schema at parameterized scale. Integration test runs the full acceptance script against a fresh temp directory: `Database::create` (or `bqlite init`), then `CREATE TABLE`, then `INSERT ... VALUES` (small literal batch), then `INSERT ... FROM` the synthetic CSV at 1M-row scale, then the `where`/`select`/`limit` pipeline query, then `EXPLAIN`, then `DESCRIBE`, `ALTER TABLE ADD COLUMN`, and `DROP TABLE`. Asserts exact result rows for the data-plane query and the expected `ExplainNode` structure (pushed-down predicate, pruned columns); for DDL paths, asserts the post-state of the manifest. A 100M-row variant lives behind `#[ignore]` and runs in the bench job. Failure here is the Wave 2 acceptance-gate trip.

### TASK-236: [HARD][IMPL] Wave 2 benchmark suite
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

### TASK-237: [HARD][DESIGN][IMPL] INSERT FROM column-remapping language + AST extension
**Output**: docs/design/query-language.md, crates/bqlite-ast/src/statement.rs
**Depends on**: none
**Description**: Merge-first AST + design-doc extension that unblocks TASK-222's parser work and TASK-233's CSV ingest path. Without this task the AST cannot represent the `map: (src AS dst, ...)` clause that the Wave 2 acceptance script and goal text both rely on.

- **Language doc.** Extends query-language.md §20.1 with the `map` option for `INSERT ... FROM`: `WITH (format: 'csv', map: (uid AS user_id, time AS ts, evt AS event))`. Documents that unmapped source columns pass through by name match, that duplicate `dst` names error, and that the rule lives alongside the existing `format`/`delimiter`/`header` options.
- **AST.** Replaces or extends the current `InsertOption { key: Name, value: Literal }` shape so the `map` clause is representable. Either (a) add a structured `map: Option<Vec<ColumnMapping>>` field on `InsertBody::From` alongside the existing flat option list, or (b) introduce an `InsertOptionValue` enum that admits both literal values and a column-mapping list. Decision recorded in this task's design note section so TASK-222 can implement against the chosen shape. Add round-trip serde tests for the new shape.
- **No parser work.** TASK-222 still owns parsing.

This is merge-first because every dependent task assumes the AST shape exists. It is intentionally pre-numbered (anchor-style) so an agent can claim it before the rest of Wave 2 starts.

### TASK-238: [EASY][IMPL] INSERT VALUES end-to-end
**Output**: crates/bqlite-parser/src/dml.rs, crates/bqlite-planner/src/{logical,physical}.rs (additions), crates/bqlite-engine/src/ingest.rs (additions)
**Depends on**: TASK-220, TASK-224, TASK-226, TASK-232, TASK-233
**Description**: Implements the literal-tuple form of INSERT, which the AST already models as `InsertBody::Values(Vec<Vec<Literal>>)` and which query-language.md §20.1 documents alongside `INSERT ... FROM`. Without this task the Wave 2 goal "CREATE TABLE / INSERT / EXPLAIN" only ships half of INSERT.

- **Parser.** Adds the `INSERT INTO <table> VALUES (lit, lit, ...), (lit, lit, ...);` production to TASK-222's `dml.rs`. Positional only, no column list, literals only — matches the AST's `Vec<Vec<Literal>>` shape and the §20.1 v1 restriction.
- **Planner.** `Insert` logical/physical lowering grows a `Values` arm carrying the literal tuples directly (no `CompiledExpr`, since literals don't need compilation).
- **Engine.** When binding an `InsertPhysical { body: Values(rows) }`, the engine validates each row against the target table's `TableSchema` (arity, type coercion to `PropertyValue`, NOT NULL, role-column population), assigns a fresh `batch_id`, and feeds the rows through the same partitioner + writer pipeline TASK-233 uses for CSV. Type mismatches and NOT NULL violations error with the offending row index.
- **Tests.** Round-trip test: `CREATE TABLE` → `INSERT VALUES` → scan the table back, assert the rows. Error tests cover wrong arity, wrong type, NULL into NOT NULL, and rejected literal kinds.

### TASK-239: [HARD][IMPL] Startup orphan segment + manifest reconciliation
**Output**: crates/bqlite-storage/src/database.rs (cleanup pass), crates/bqlite-storage/src/segment/cleanup.rs (new)
**Depends on**: TASK-214, TASK-217
**Description**: Implements the crash-safety contract from `docs/reliability.md` §Crash Safety and `docs/design/storage-format.md` §7.4. Today `Database::open_or_create` only sweeps `manifest.json.tmp`; once Wave 2 starts writing real segment files via TASK-213/214, every startup must reconcile on-disk segment state against the manifest:

- **Sweep `.tmp` segment files.** Walk every `(window, shard)` directory under the database root. Any file ending in `.tmp` is a partially-written segment from a crash mid-ingest or mid-compaction and is unconditionally deleted.
- **Sweep manifest-orphaned segments.** Build the set of segment file names referenced by the manifest's segment inventory. Any non-`.tmp` segment file in a `(window, shard)` directory that is not in the active set is an orphan from a deferred compaction delete and is removed. Files that *are* in the active set are left untouched.
- **Idempotent and safe.** The pass is read-only with respect to the manifest — it only deletes files, never edits the manifest. Re-running the pass on a clean database is a no-op.
- **Tests.** Crashed-ingest scenario (orphan `.tmp`), crashed-compaction scenario (orphan output `.tmp` and orphan input segment), happy-path open with no orphans (no deletions, manifest unchanged), and a regression test that an active segment listed in the manifest is *never* deleted even if its file looks unusual.

Without this task, the storage layer leaks disk space across crashes and the reliability doc describes behavior the code does not implement.

### TASK-240: [HARD][IMPL] Database open/create split + `bqlite init` + bootstrap retirement
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

### TASK-241: [EASY][IMPL] Wave 2 benchmark CI job + baseline + regression gate
**Output**: .github/workflows/bench.yml, scripts/bench-compare.sh
**Depends on**: TASK-236
**Description**: Wires up the regression-gate machinery promised in the Wave 2 header. Without this task the perf gate is aspirational — TASK-236 only writes the benches.

- **Workflow.** New `bench.yml` GitHub Actions workflow runs the Wave 2 bench subset (`benches/wave2/*`) on `ubuntu-latest` (4 vCPU) using the **1.5×-relaxed CI targets** from the Wave 2 perf-gate table.
- **Baseline capture on main.** On every push to `main`, the workflow runs the bench subset and uploads the Criterion `estimates.json` outputs as a workflow artifact named `bench-baseline-main`. The most recent artifact is the canonical "previous green main" baseline.
- **Comparison on PR.** On pull requests, the workflow runs the same bench subset, downloads the latest `bench-baseline-main` artifact, and runs `scripts/bench-compare.sh` to diff each metric. If any metric slips >10% on at least 3 consecutive Criterion samples (the consecutive-sample rule protects against single noisy runs on shared hardware), the job fails and the PR is blocked.
- **Opt-out.** PRs labeled `bench-skip` and draft PRs bypass the gate — for docs-only changes and similar.
- **Reference-hardware verification stays manual.** The pinned Apple M2 Max numbers remain verified by hand before the wave is declared complete; the CI gate uses only the relaxed CI targets.

### TASK-242: [RETIRED]
**Status**: Retired during the post-Wave-2 architecture reconciliation. Originally scoped as "FilteredBatch + SelectionVector + execution tile scaffold" after execution-model.md §3.8 introduced selection vectors as a steady-state design. Review found the selection-vector half of the task only pays off under a fused stateless push segment, which is a Wave 5 concern (TASK-503 "operator fusion" territory) — Wave 2 has no operator chain that can carry a `FilteredBatch` across operator boundaries, because the `PhysicalOperator::next_batch()` boundary is `RecordBatch`. The execution-tile half was small enough to fold into TASK-231 directly (filter operator's constructor grows a `tile_size` parameter; the tile loop is a 10-line helper). Number retired per the "numbers are never reused" rule. The full execution-model.md §3.8 design remains the implementation target for Wave 5 — see the forward reference in TASK-503.

### TASK-243: [EASY][IMPL] posix_fadvise sequential-scan hint
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

### TASK-244: [HARD][IMPL] Manifest-backed `SegmentReader` reconciliation
**Output**: crates/bqlite-storage/src/database.rs, crates/bqlite-storage/src/segment/reader.rs, crates/bqlite-engine/src/bind.rs (tests)
**Depends on**: TASK-215, TASK-217, TASK-230
**Description**: Replace the lingering Wave 1 `EmptySegmentReader` wiring in `Database::segment_reader()` with the real manifest-backed query snapshot that Wave 2's scan path was always supposed to consume. `segments()` enumerates the table's live segment inventory from `Manifest::snapshot_for_query`, `open_segment()` resolves the on-disk segment path and opens TASK-215's `SegmentFileReader`, and the reader applies the current table schema when older segments are missing newly-added columns. Stable per-query ordering, schema-evolution backfill (`NULL` / default), and foreign-handle rejection remain part of the contract. Keep `empty_segment_reader()` only as an explicit test helper, not the production query path. End-to-end tests must prove `CREATE TABLE → INSERT VALUES/FROM → query` returns real rows through the normal engine bind/scan path rather than by inspecting the manifest directly.

### TASK-245: [EASY][IMPL] Wave 2 acceptance and CLI tests against real rows
**Output**: tests/tests/wave2_acceptance.rs, tests/smoke.rs, crates/bqlite-cli/src/{main,ingest}.rs (tests)
**Depends on**: TASK-229, TASK-233, TASK-238, TASK-240, TASK-244
**Description**: Rework the Wave 2 acceptance-facing tests so they assert the published user-visible behavior rather than the old zero-row stub path. `wave2_acceptance.rs` must assert exact result rows for the `where/select/limit` query, exact `ExplainNode` evidence of pushed predicates and pruned columns, real `DESCRIBE` output, and the expected post-state after `ALTER TABLE ADD COLUMN` / `DROP TABLE`. CLI tests must cover non-empty query output after ingest, auto-injected `LIMIT 1000`, truncation footer text, and the `--limit` / `--no-limit` overrides. Delete or rewrite comments that treat zero-row scans as expected Wave 2 behavior. The ignored 100M-row variant must exercise the same real scan path, not a manifest-only proxy.

### TASK-246: [HARD][IMPL] Reference-dataset bench harness + hard target enforcement
**Output**: benches/wave2/{scan,encoding,ingest,acceptance}.rs, benches/common/mod.rs, .github/workflows/bench.yml, scripts/bench-compare.sh
**Depends on**: TASK-235, TASK-236, TASK-241
**Description**: Reconcile the benchmark suite with the published Wave 2 performance gate in this file. The bench harness gains a dual-mode dataset strategy: CI-scaled fixtures remain for regression-noise control, but reference-mode runs execute the true 100M-row acceptance query and the reference CSV profile from the Wave 2 header on the pinned machine. Bench targets become load-bearing again: reference-mode runs fail when the acceptance query exceeds 1 s, int64 decode or pushed-down equality miss their floors, ingest falls below 100 MB/s, compression ratio exceeds 10%, or zone-map pruning on the acceptance query is below 80%. Bench output must be machine-readable and preserve both scaled-CI and reference-hardware numbers in the uploaded artifacts so TASK-241's regression gate and manual release sign-off are comparing the same metrics.

### TASK-247: [HARD][IMPL] Scan-path performance closure for the acceptance query
**Output**: crates/bqlite-storage/src/{database.rs,segment/reader.rs,segment/merge.rs,zone_map.rs}, crates/bqlite-operators/src/scan.rs, benches/wave2/{scan,acceptance}.rs
**Depends on**: TASK-216, TASK-227, TASK-230, TASK-243, TASK-244, TASK-246
**Description**: Profile and optimize the real query scan path until it meets the Wave 2 scan-side gate on reference hardware. This is the closure task for the numbers that currently miss by a wide margin: the end-to-end acceptance query, columnar decode throughput, pushed-down equality throughput, and zone-map pruning effectiveness. Expected work includes eliminating redundant segment-byte copies and reopen churn in the hot path, reusing projection/decoder planning across row-groups where legal, ensuring dictionary/equality pushdown stays in code space rather than devolving to row-level string comparisons, and making pruning effective for the actual acceptance predicate (`event_type = 'purchase' AND amount > 4500`) rather than only for synthetic microbench filters. Success criteria are the published Wave 2 targets in the header, measured by TASK-246's reference-mode benches.

### TASK-248: [HARD][IMPL] CSV ingest throughput closure
**Output**: crates/bqlite-engine/src/ingest.rs, crates/bqlite-storage/src/{ingest/csv.rs,ingest/partitioner.rs,writer.rs}, benches/wave2/ingest.rs
**Depends on**: TASK-214, TASK-218, TASK-233, TASK-246
**Description**: Bring parse → sort → encode → write throughput up to the Wave 2 `>= 100 MB/s` ingest target on the reference dataset. Bench-driven optimization pass over the real ingest path: reduce string and `PropertyValue` cloning, stream source rows into partition buckets with less intermediate materialization, amortize schema coercion work, minimize temporary allocations in the CSV reader and partitioner, and make sure writer batching does not pessimize the selector/encoder hot loops. The success criterion is not a synthetic microbench win; it is TASK-246's reference-mode ingest bench clearing the published Wave 2 floor while preserving exact ingest semantics and error reporting.

### TASK-299: [HARD][IMPL] Wave 2 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-235, TASK-236, TASK-237, TASK-238, TASK-239, TASK-240, TASK-241, TASK-243, TASK-244, TASK-245, TASK-246, TASK-247, TASK-248
**Description**: Same audit pattern as TASK-199, rescored after Wave 2. Wave 2 is the first wave with a real performance gate, so the Benchmarks dimension must reflect whether the Wave 2 perf-gate targets are met on reference hardware — not merely whether benches exist. bqlite-storage (segment format, encodings, ingest), bqlite-planner (pushdown, projection pruning, EXPLAIN), and bqlite-operators (scan/filter/project/limit) will see the biggest grade movements. Any crate slipping vs. its Wave 1 grade is flagged in the commit message. Below-C grades get follow-up tasks; Wave 3 does not start until those are filed.

---

## Wave 3: Pattern Matching MVP

**Goal.** Funnel queries work end-to-end. MATCH operator over entity-sorted batches, hash aggregate with GROUP BY, sort, distinct, FUNNEL terminal sugar desugared in the planner into `MATCH FIRST ... EMIT ALL → Aggregate(SUM(CAST(step_reached >= N AS INT)))`, returning correct per-step conversion counts over a CSV fixture. Step-counter fast path benchmarked against NFA baseline.

**Scope exclusions.**
- **RETENTION sugar** is deferred to Wave 4 alongside `BRACKETS` and `WITHIN SESSION` — the AST models `PipelineStage::Retention`, but session windows and retention brackets both depend on SESSIONIZE and the cohort-join story landing in Wave 4.
- **Cohorts, aliases, `IN QUERY`** are deferred to Wave 4 per TASK-407.
- **SESSIONIZE and attribution** are Wave 4 (TASK-405, TASK-406).
- **Percentile aggregates (`P50`/`P90`/`P95`/`P99`)** are deferred to TASK-327, which ships after the core aggregate machinery (TASK-307) stabilizes. DDSketch integration is complex enough to merit its own task, and funnels (the Wave 3 acceptance gate) do not use percentiles. The v1 aggregate set (query-language.md §7.1) is complete only after TASK-327 lands.
- **Spill-to-disk for hash aggregate and sort** is deferred to Wave 5 (TASK-502); Wave 3 operators are hard-capped and return a typed error on overflow.
- **Fused push segments and `StatelessKernel`** are Wave 5 (TASK-503); Wave 3's new operators ship as plain `PhysicalOperator` / `EntityOperator` impls like TASK-231's filter/project/limit.

**Size.** ~33 tasks.
**Parallelism.** 6-10 agents.
**Acceptance.** (1) A 3-step funnel query (`events LAST 30d | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d`) over an ingested CSV returns correct per-step conversion counts (named `signup`, `activation`, `purchase` per query-language.md §6.1). (2) The step-counter fast path, measured on the query body rather than fixture setup or ingest, benchmarks within 2x of the Wave 2 scan-only baseline on the same dataset (validates TASK-302's perf expectation). EMIT ALL, negation, variable bindings, and time-window expiry all covered by integration tests.

Wave 3 is a strategy wave: the design anchors (301, 302, 308, 309, 310) land first and front-load almost all the risk — after them, the parser, pattern compiler, NFA, aggregate, sort/distinct, lowering, and operator tracks run in parallel. The pattern AST and `PipelineStage::{Match, Stats, OrderBy, Funnel, Retention}` variants were already shipped by Wave 2's TASK-221 / TASK-224; Wave 3 does not re-build them.

### TASK-301: [HARD][DESIGN] MATCH operator architecture
**Output**: docs/design/operators/match-operator.md
**Depends on**: none
**Description**: Connects sequence-matching.md to an implementable operator. Pins down: operator state layout (per-entity NFA candidate deques vs. step-counter tracks), the `EntityOperator` integration (entity boundary detection, `finish_entity()` semantics, sub-batch streaming of oversized entities per execution-model.md §5), output schema including `entity_id`, `$var` binding columns, step-property columns, `step_reached` under EMIT ALL, and the emission points (match completion, window expiry, entity end). Specifies the active-state cap (execution-model.md §16.1, default 10,000 candidates per entity) and the typed error returned on overflow. Also specifies how demand-driven column reduction (execution-model.md §8) avoids materializing unreferenced step properties. Risky anchor — unblocks TASK-304, TASK-306, TASK-311, TASK-321.

### TASK-302: [HARD][DESIGN] Sequence matcher strategy selection
**Output**: docs/design/operators/matcher-strategy.md
**Depends on**: TASK-301
**Description**: The compile-time classifier that decides step-counter vs. general NFA path. Spells out the pattern-class predicates (linear, no branching, no repetition, no negation → step counter; everything else → NFA), variable-binding interaction (step counter supports bindings via `StepCounterTrack.bindings`), the classification output type (`PatternClass` enum) carried on `CompiledNfa`, and the fallback behavior when a step-counter-eligible pattern has unsupported predicates. Includes the microbenchmark methodology TASK-325 follows to validate the per-strategy perf expectations. Risky — directly controls Wave 3's perf story.

### TASK-303: [EASY][DESIGN] Pattern grammar surface + AST reconciliation
**Output**: docs/design/language/pattern-grammar.md
**Depends on**: none
**Description**: Reconciliation note, not a new AST. Wave 2 already shipped the pattern AST in `bqlite-ast/src/pattern.rs` (`MatchPattern`, `Step`, `StepEvent`, `MatchMode`, `Repetition`, `Exclusion`, `MatchWindow`, `BracketSpec`) and the `PipelineStage::{Match, Stats, OrderBy, Funnel, Retention}` variants in `operator.rs` as part of TASK-221 / TASK-224. This task produces the grammar-facing design doc: the concrete syntax for `MATCH (FIRST|ALL) SEQUENCE(step THEN step THEN ...) [WITHIN d] [EMIT ALL]`, step syntax (`[name:] event [WHERE predicate] [IMMEDIATELY] [+|*] [WITHOUT excl]`), alternation inside sequences, `$var` binding references, and a gap analysis identifying any shipped-AST field that the design doc does not cover (or vice versa). Precise token-to-AST map that TASK-312 / TASK-313 implement directly. Unblocks all parser pattern work.

### TASK-304: [HARD][IMPL] NFA runtime simulator
**Output**: crates/bqlite-operators/src/matcher/nfa.rs
**Depends on**: TASK-301, TASK-311
**Description**: General-path NFA runtime per sequence-matching.md §9-10. Consumes the `CompiledNfa` (types + Thompson construction owned by TASK-311) and implements the three-phase per-event simulation: phase 1 processes existing candidates in reverse-order (expiry checks, poison-transition kills for `WITHOUT` negation, forward propagation on matching transitions); phase 2 starts new candidates when the current event matches step 0; phase 3 deduplicates per `(state, binding_track)` and checks accept state. Global time-window enforcement via `anchor_ts` — candidates whose `event.ts - anchor_ts > global_window` are expired front-of-deque O(1) amortized. No binding logic (that's TASK-306). No step-counter code (that's TASK-305). Unit tests cover every transition shape in the sequence-matching design doc including multi-step forward, poison kill mid-sequence, and window expiry at boundary. Property tests using generators from `tests/src/strategies.rs` cover the NFA invariants that example tests cannot exhaust: candidate deque ordering stability under random event interleaving, no false-negative matches (every match found by a brute-force reference evaluator is also found by the NFA), window-expiry monotonicity (once expired, a candidate never reappears), and poison-kill completeness (no candidate survives past a negation scope it should have been killed in). Risky core.

### TASK-305: [EASY][IMPL] Step counter fast path
**Output**: crates/bqlite-operators/src/matcher/step_counter.rs
**Depends on**: TASK-302, TASK-306, TASK-311
**Description**: `StepCounterTrack { bindings, current_step, anchor_ts, last_step_ts, match_count, max_step_reached, scan_from }` and its per-event advance function, selected when `CompiledNfa.pattern_class == Linear`. Implements exactly the scan-from rebinding behavior from sequence-matching.md §10.3 — MATCH ALL resets `scan_from` to the last-advanced event, MATCH FIRST returns after first match. Supports variable bindings via the shared `BindingValue` type (TASK-306 ships the type; the step counter consumes it). No NFA path. Unit-test perf smoke: sanity that a 3-step linear pattern beats the NFA path on the same input.

### TASK-306: [HARD][IMPL] Variable binding tracks
**Output**: crates/bqlite-operators/src/matcher/bindings.rs
**Depends on**: TASK-304
**Description**: Independent-per-binding match tracks per sequence-matching.md §8. Defines `BindingValue` (`Bool | Int | Float(FloatOrd<f64>) | String(CompactString) | Timestamp(i64)`), the `BindingKey = SmallVec<[BindingValue; 2]>` track identity, the `HashMap<BindingKey, Track>` per entity, bind-on-first-reference / check-on-subsequent-reference semantics (including NULL → fail-predicate → no track created), and the active-track limit enforcement. Plumbs into both the NFA (TASK-304) and the step counter (TASK-305). Risky — semantics are the trickiest part of the matcher. Ships property tests as a primary deliverable: generators from `tests/src/strategies.rs` produce random event streams with varying binding cardinalities and track counts, and the property suite validates binding-track invariants (track identity stability, NULL short-circuit, active-track cap enforcement, bind-once-then-check semantics) against a brute-force reference matcher. These property tests are the primary correctness evidence for binding semantics.

### TASK-307: [HARD][IMPL] Hash aggregate operator
**Output**: crates/bqlite-operators/src/aggregate.rs
**Depends on**: TASK-308, TASK-317
**Description**: `AggregateOperator` — hash-grouped aggregation implementing `PhysicalOperator`. Per TASK-308, ships the `HashAccumulator` trait and built-in accumulators for `COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`, `COUNT_DISTINCT(col)`. Percentile accumulators (`P50`/`P90`/`P95`/`P99`) are deferred to TASK-327. Group keys derived from a `Vec<CompiledExpr>` via TASK-205's expression kernel. State is `HashMap<GroupKey, AccumulatorState>` with the `max_groups` hard cap (default 1M per execution-model.md §9.4) returning `AggregateError::GroupLimitExceeded` on overflow — no spill in Wave 3. Works on columnar Arrow batches; pre-sizes builders per CLAUDE.md performance conventions. Supports both standalone use and the fused-downstream path emitted by MATCH/SESSIONIZE in later waves (Wave 3 only wires the standalone path end-to-end). Foundational — every funnel output and every `STATS` stage routes through it.

### TASK-308: [HARD][DESIGN][TRAIT] Aggregate + hash-accumulator architecture
**Output**: docs/design/operators/aggregate-operator.md
**Depends on**: none
**Description**: The design note TASK-307 and TASK-320 both consume. Pins down: the `HashAccumulator` trait surface (`init`, `update_batch`, `merge`, `finalize`, `size_bytes`), the built-in accumulator set for Wave 3 (`COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`, `COUNT_DISTINCT(col)` — percentiles deferred to TASK-327), state-sizing rules feeding `max_groups` accounting, null-propagation per type-system.md, how aggregate expressions are compiled through TASK-205's `CompiledExpr` kernel, the output schema rules (`output_schema`: group columns + one column per aggregate with stable synthetic names), and the fused-downstream protocol that lets MATCH/SESSIONIZE feed entities directly into an accumulator without a standalone Aggregate operator node (the protocol ships as a hook in Wave 3; the matcher consumes it in TASK-321). Also specifies the `HashAccumulator` extensibility contract that TASK-327 uses to add DDSketch-based percentile accumulators without modifying existing code. Merge-first — blocks TASK-307, TASK-317, TASK-318, TASK-320, TASK-321.

### TASK-309: [HARD][DESIGN] Logical lowering + demand propagation for Match / Stats / OrderBy
**Output**: docs/design/planner/wave3-lowering.md
**Depends on**: TASK-301, TASK-308
**Description**: Spec-level design for the AST → logical lowering that TASK-318 implements. Covers: (1) `PipelineStage::Match` → `LogicalPlan::SequenceMatch { pattern, mode, emit_all, window, brackets: None, step_properties, fused_downstream: None, input, output_schema }`, (2) `PipelineStage::Stats` → `LogicalPlan::Aggregate { aggregates, group_by, input, output_schema }`, (3) `PipelineStage::OrderBy` → `LogicalPlan::Sort`, (4) `PipelineStage::Select { distinct: true }` → `LogicalPlan::Distinct(Project(...))`. Specifies the `DemandSet { columns, needs_match_detail, needs_step_reached, step_properties: Vec<(step_name, column_name)>, fused_aggregate }` shape, the backward-propagation algorithm (consumer → producer, stopping at `Scan`), and how `fused_downstream` is populated by TASK-320 after demand resolves. Schema-validation rules for `step_name.column` references, for `$var` references surviving through aggregate group-by, and for the `step_reached` synthetic column under EMIT ALL. Risky because the demand-set shape persists into Waves 4-5.

### TASK-310: [EASY][DESIGN] Sort + Distinct operator contracts
**Output**: docs/design/operators/sort-distinct.md
**Depends on**: none
**Description**: Smaller design note for the two non-matching stateful operators Wave 3 ships. `SortOperator` — key compilation via TASK-205, stable vs unstable (Wave 3 ships stable), in-memory-only with a hard `max_rows` cap and typed overflow error (spill is Wave 5 TASK-502), null-ordering rules from type-system.md, output schema unchanged from input. `DistinctOperator` — hash-set dedup reusing TASK-307's hash-key kernel, `max_groups` cap matching aggregate, output schema unchanged from input. Specifies how both operators interact with entity ordering — neither preserves `(entity_id, ts)` order, which is fine because they sit above any entity-aware operator in the Wave 3 plan tree. Unblocks TASK-317 + TASK-322.

### TASK-311: [HARD][IMPL] Pattern compiler: SequencePattern → CompiledNfa
**Output**: crates/bqlite-planner/src/compile.rs
**Depends on**: TASK-301, TASK-302
**Description**: Plan-time pattern compilation per sequence-matching.md §14.3 (crate placement: `CompiledNfa`, `PatternClass` → `bqlite-planner`). Takes a `SequencePattern` AST node (defined in `bqlite-ast::pattern` as `MatchPattern`) and produces `CompiledNfa { states: Vec<NfaState>, accept_state, relevant_event_types, pattern_class, variable_bindings, global_window, emit_all, state_to_step }`. Implements Thompson construction (step → state, alternation → branching, repetition → self-loops, epsilon elimination), adds poison transitions for `WITHOUT` negation scopes, computes `state_to_step` via BFS, runs the pattern classifier from TASK-302 (`PatternClass::{Linear, Branching, Repeating, Negating, WithBindings}` — composed flags), collects event-type filters for scan-level pushdown, resolves `$var` references to `BindingRef` indices, validates negation targets are in-scope, and emits structured errors for malformed patterns. Lives in `bqlite-planner` because compilation requires schema access for validation and the result is carried on the physical descriptor (never on the logical node — that carries the AST `SequencePattern` per planner-pipeline.md §5.2). Invoked during logical→physical lowering in TASK-318. Unit tests include one-step-to-many-step patterns, every pattern-class bucket, and every validation error.

### TASK-312: [EASY][IMPL] Parser pattern productions
**Output**: crates/bqlite-parser/src/pattern.rs
**Depends on**: TASK-303
**Description**: Hand-rolled productions for pattern syntax that feed the `MatchPattern` AST. Covers: `SEQUENCE ( step ( THEN step )+ )`, step shape `[name :] event [WHERE expr] [IMMEDIATELY] [+|*] [WITHOUT event]`, event alternation `( e1 OR e2 OR ... )` inside a step slot, `$var` binding references inside WHERE predicates, `WITHIN <duration>` window clause, and precise span tracking for diagnostics. Reuses TASK-220's expression parser for WHERE predicates. Halt-on-first-error per language-doc §policy. Separate from TASK-313 so the pattern sub-grammar can be unit-tested in isolation, matching the separation Wave 2 used for `expr.rs` vs `pipeline.rs`.

### TASK-313: [EASY][IMPL] Parser MATCH pipeline stage
**Output**: crates/bqlite-parser/src/pipeline.rs (extension)
**Depends on**: TASK-312
**Description**: `MATCH (FIRST | ALL) <sequence> [WITHIN <duration>] [EMIT ALL]` pipeline stage production — delegates to TASK-312 for the `<sequence>` non-terminal, parses optional `WITHIN` and trailing `EMIT ALL` in canonical order per query-language.md §4 (modifiers out of order are a parse error), and emits `PipelineStage::Match { pattern: MatchPattern, span }`. Lands the `mode` + `emit_all` flags (`MatchMode::{First, All}` in the AST, with `emit_all: bool` as a sibling field). Unit tests cover every mode combination, modifier ordering enforcement, plus error cases for missing keywords.

### TASK-314: [EASY][IMPL] Parser STATS stage
**Output**: crates/bqlite-parser/src/pipeline.rs (extension)
**Depends on**: none
**Description**: `STATS <agg_list> [GROUP BY <group_list>]` production emitting `PipelineStage::Stats { aggregates: Vec<AggItem>, group_by: Vec<GroupItem>, span }` per query-language.md §7.2 and §26. BQL uses `GROUP BY`, not bare `BY` — the parser must reject `STATS ... BY ...` as a syntax error. Supports the full v1 aggregate keyword set: `COUNT`, `COUNT_DISTINCT`, `SUM`, `MIN`, `MAX`, `AVG`, `P50`, `P90`, `P95`, `P99` per query-language.md §7.1. Arg expressions compiled through TASK-220's expression parser. `COUNT_DISTINCT(col)` is the only distinct form — `COUNT(DISTINCT col)` is a parse error per query-language.md §7.3. The `AggItem`, `GroupItem`, and the `Stats` variant already exist in `bqlite-ast::operator`; this task lands the surface-to-AST production only. (Percentile runtime is TASK-327; the parser accepts the keywords regardless.)

### TASK-315: [EASY][IMPL] Parser ORDER BY stage + SORT alias
**Output**: crates/bqlite-parser/src/pipeline.rs (extension)
**Depends on**: none
**Description**: `ORDER BY <expr> [ASC|DESC] (, <expr> [ASC|DESC])*` and the `SORT` alias per query-language.md §13. Emits `PipelineStage::OrderBy { items: Vec<OrderItem>, span }`. Unit tests cover default-direction-is-ASC, mixed direction, and the `SORT`/`ORDER BY` equivalence.

### TASK-316: [EASY][IMPL] Parser FUNNEL stage
**Output**: crates/bqlite-parser/src/pipeline.rs (extension)
**Depends on**: TASK-303, TASK-312
**Description**: `FUNNEL( step THEN step [THEN step]... ) WITHIN <duration>` production per query-language.md §6.1, emitting `PipelineStage::Funnel(Funnel { steps, window, span })`. Reuses TASK-312's pattern step sub-grammar — FUNNEL accepts the full MATCH step grammar (named steps, property constraints, variable bindings, WITHOUT exclusions, alternation, repetition, IMMEDIATELY). The `Funnel` struct and the variant already exist in `bqlite-ast::operator`. FUNNEL is **terminal sugar** — it cannot be followed by `| STATS` or any downstream pipe stage; the parser emits an error if the user attempts to pipe after FUNNEL. Desugaring lives in TASK-319. RETENTION is explicitly out of scope — its AST variant stays unparsed in Wave 3 per the scope exclusions.

### TASK-317: [EASY][IMPL] Logical + physical plan variants for Wave 3
**Output**: crates/bqlite-planner/src/{logical.rs,physical.rs}
**Depends on**: TASK-309, TASK-310, TASK-311
**Description**: Adds the four Wave 3 logical nodes per planner-pipeline.md §5.2: `SequenceMatch { pattern: SequencePattern, mode, emit_all, window, brackets: None, step_properties, fused_downstream: Option<FusedDownstream>, input, output_schema }` (carries the **AST** pattern — compilation to `CompiledNfa` happens during logical→physical lowering in TASK-318), `Aggregate { aggregates: Vec<(String, AggFunc, Expr)>, group_by: Vec<Expr>, input, output_schema }`, `Sort { keys: Vec<(Expr, SortDirection)>, input, output_schema }`, `Distinct { input, output_schema }`. Adds the four physical descriptors per planner-pipeline.md §10: `SequenceMatchPhysical { compiled_nfa: CompiledNfa, strategy: MatchStrategy, demand: DemandSet, execution_config, fused_aggregate }`, `AggregatePhysical`, `SortPhysical`, `DistinctPhysical` — materialized into operators by the engine bind step (TASK-323). Replaces the "later waves" comment blocks in `logical.rs` and `physical.rs`. Schema-validation implementations follow logical-plan-nodes.md §5.1. Also extends the EXPLAIN tree builder (TASK-229) with pretty-print arms for all four new plan variants — otherwise `EXPLAIN` on a Wave 3 query panics or prints `<unknown>`.

### TASK-318: [HARD][IMPL] AST → logical lowering + logical → physical lowering for Wave 3
**Output**: crates/bqlite-planner/src/{logical.rs,physical.rs}
**Depends on**: TASK-311, TASK-313, TASK-314, TASK-315, TASK-317
**Description**: Two-phase extension of the planner for Wave 3 nodes.

**Phase 1 — AST → logical.** Extends Wave 2's lowering (TASK-224) per TASK-309: `PipelineStage::Match` → `LogicalPlan::SequenceMatch { pattern: SequencePattern, ... }` (carries the AST pattern, NOT the compiled NFA), `PipelineStage::Stats` → `Aggregate`, `PipelineStage::OrderBy` → `Sort`, and `SELECT DISTINCT` → `Distinct(Project(...))` (the existing Wave 2 TODO in `logical.rs:508`). Ships backward demand propagation that fills the `step_properties` field on `SequenceMatch` by walking the consumer chain for `step_name.column` references and writing a `DemandSet` through to the scan node.

**Phase 2 — logical → physical.** Extends Wave 2's physical lowering (TASK-226) for the four new nodes. For `SequenceMatch`: invokes TASK-311's pattern compiler (`SequencePattern` → `CompiledNfa`) with schema context, writes the compiled form onto `SequenceMatchPhysical` alongside strategy selection and demand, and pushes `CompiledNfa.relevant_event_types` into the scan's predicate list via Wave 2's pushdown pass (TASK-227) so the matcher never sees irrelevant events per sequence-matching.md §12. For `Aggregate`, `Sort`, `Distinct`: straightforward descriptor mapping.

Error cases: unknown step names, binding references crossing an aggregate group-by boundary, type errors in aggregate arg expressions. Integration tests cover every Wave 3 pipeline shape end-to-end through both lowering phases.

### TASK-319: [EASY][IMPL] FUNNEL desugaring pass
**Output**: crates/bqlite-planner/src/opt/desugar_funnel.rs
**Depends on**: TASK-316, TASK-318
**Description**: AST-level rewrite pass run before TASK-318's logical lowering, per query-language.md §6.1 and planner-pipeline.md §4.3. Rewrites `PipelineStage::Funnel(steps, window)` into two pipeline stages: `MATCH FIRST SEQUENCE(steps) WITHIN <window> EMIT ALL` followed by `STATS step1_name = SUM(CAST(step_reached >= 1 AS INT)), step2_name = SUM(CAST(step_reached >= 2 AS INT)), ...` — one named `SUM(CAST(...))` per step, with aggregate output names derived from step names or event types per query-language.md §6.1 naming rules. Raises `TypeError::NameCollision` when two steps produce the same output name without explicit step names. Downstream lowering sees only `SequenceMatch + Aggregate`. Unit tests cover 2-step and 3-step funnels, named steps (`s: signup`), and the name-collision error case.

### TASK-320: [EASY][IMPL] Match-aggregate fusion optimizer pass
**Output**: crates/bqlite-planner/src/opt/fuse_match_aggregate.rs
**Depends on**: TASK-308, TASK-318
**Description**: Detects `Aggregate(SequenceMatch(...))` where the aggregate's demand set can be fulfilled directly from the match's per-match output (the desugared funnel shape: `SUM(CAST(step_reached >= N AS INT))` per step), populates `SequenceMatch.fused_downstream` with the compiled aggregate, and elides the standalone `Aggregate` node. Conservative — only fuses when the match and aggregate reference no columns outside the set surfaced by the match's output schema (`entity_id`, `step_reached`, bound variables). Unit tests cover the fused-funnel shape, the unfused "match then arbitrary project then aggregate" shape, and the mixed case with filters between match and aggregate (must not fuse).

### TASK-321: [HARD][IMPL] SequenceMatchOperator
**Output**: crates/bqlite-operators/src/sequence_match.rs
**Depends on**: TASK-304, TASK-305, TASK-306, TASK-307, TASK-317
**Description**: The physical `SequenceMatchOperator` that ties the three matcher pieces (TASK-304 NFA, TASK-305 step counter, TASK-306 bindings) into an `EntityOperator` implementation. Receives a `CompiledNfa` (produced by TASK-311 during physical lowering and carried on `SequenceMatchPhysical`) via the engine bind step. On construction, dispatches on `CompiledNfa.pattern_class` to pick the strategy per TASK-302. Implements `process_sub_batch()` honoring the entity-alignment contract from execution-model.md §4-5 (state persists across sub-batches for oversized entities), calls `finish_entity()` on boundary detection, and assembles per-match output rows into an Arrow `RecordBatch` builder sized per TASK-301's output schema rules. Consumes `fused_aggregate` (TASK-308) when populated by TASK-320 to feed matches into the hash aggregator without materializing the intermediate `match_events` map. Enforces the active-candidate cap from TASK-301 and returns the typed overflow error. Note: `bqlite-operators` depends on `bqlite-planner` per architecture.md, so importing `CompiledNfa` from the planner crate is legal.

### TASK-322: [EASY][IMPL] Sort + Distinct operators
**Output**: crates/bqlite-operators/src/{sort.rs,distinct.rs}
**Depends on**: TASK-307, TASK-310, TASK-317
**Description**: Two small `PhysicalOperator` implementations per TASK-310. `SortOperator` collects input batches into a single materialized `RecordBatch`, compiles sort keys through TASK-205's kernel, and emits Arrow `lexsort` output in stable form. `DistinctOperator` reuses TASK-307's hash-key kernel to dedup incoming rows against a `HashSet<GroupKey>`, emitting only first-occurrence rows. Both honor the hard-cap overflow error from TASK-310 — no spill. Output schemas unchanged from input.

### TASK-323: [EASY][IMPL] Engine bind step extension for Wave 3 nodes
**Output**: crates/bqlite-engine/src/bind.rs
**Depends on**: TASK-307, TASK-317, TASK-318, TASK-321, TASK-322
**Description**: Extends TASK-232's bind step to materialize `Box<dyn PhysicalOperator>` for `SequenceMatchPhysical`, `AggregatePhysical`, `SortPhysical`, and `DistinctPhysical`. Each bind arm instantiates the corresponding operator from the descriptor, wires in the child operator, and forwards the `output_schema` through. No new planner or storage logic — pure bind wiring. End-to-end engine tests confirm every Wave 3 pipeline shape over a fixture: `events | WHERE | MATCH | STATS` and `events | STATS COUNT(*) GROUP BY event` return correct aggregate counts, `events | ORDER BY ts DESC | LIMIT 10` asserts exact row ordering (not just non-emptiness), and `events | SELECT DISTINCT event` asserts no duplicate rows in the result.

### TASK-324: [HARD][IMPL] Matcher integration test suite
**Output**: tests/integration/matcher/
**Depends on**: TASK-318, TASK-321, TASK-323
**Description**: Integration-level test suite reusing the TASK-120 fixture framework. Covers the full matcher semantics matrix from sequence-matching.md: linear patterns, branching alternation, one-or-more repetition, zero-or-more repetition, `WITHOUT` negation (including eager kill inside the negation scope), `$var` binding tracks (single binding, multiple bindings per track, NULL binding short-circuit), `IMMEDIATELY` modifier, time-window expiry (before, at boundary, after), EMIT ALL with each step reached, MATCH ALL rebinding per entity, and the entity-sub-batch streaming path for oversized entities. Each test ingests a small CSV fixture, runs the query through the engine, and asserts the exact result set. Pattern-class coverage forms the Tests-dimension evidence for TASK-399's audit.

### TASK-325: [HARD][IMPL] Wave 3 benchmark suite + matcher microbenchmarks
**Output**: benches/wave3/
**Depends on**: TASK-321, TASK-307, TASK-322
**Description**: Criterion benches per CLAUDE.md performance conventions, following the TASK-236 Wave 2 bench layout. Covers: (1) NFA vs. step-counter fast path on the same linear 3-step funnel pattern (validates the TASK-302 perf expectation), (2) hash aggregate throughput by group count (10, 1k, 1M), (3) sort throughput by row count, (4) distinct throughput by dedup ratio, (5) end-to-end 3-step funnel over a 100M-event synthetic dataset from `tests/src/strategies.rs`. Reuses the TASK-241 CI bench infrastructure with Wave 3 targets. Wave 3 budgets are captured inline in the suite's README and promoted into the `bench.yml` regression gate.

### TASK-326: [HARD][IMPL] Wave 3 acceptance test
**Output**: tests/wave3_acceptance.rs
**Depends on**: TASK-307, TASK-319, TASK-321, TASK-323
**Description**: The Wave 3 correctness gate per the header. Ingests a synthetic CSV event stream via the TASK-233 ingest path, runs `events LAST 30d | FUNNEL(signup THEN activation THEN purchase) WITHIN 7d`, and asserts the per-step conversion counts (`signup`, `activation`, `purchase` output columns per query-language.md §6.1 naming rules) match hand-computed expected values. Also runs the equivalent desugared form `events LAST 30d | MATCH FIRST SEQUENCE(signup THEN activation THEN purchase) WITHIN 7d EMIT ALL | STATS signup = SUM(CAST(step_reached >= 1 AS INT)), activation = SUM(CAST(step_reached >= 2 AS INT)), purchase = SUM(CAST(step_reached >= 3 AS INT))` and asserts result equality with the FUNNEL form (validating TASK-319's desugaring). Wave 3 is done when this test passes in CI on both macOS and Linux.

### TASK-327: [HARD][IMPL] DDSketch percentile accumulators (P50/P90/P95/P99)
**Output**: crates/bqlite-operators/src/aggregate/percentile.rs
**Depends on**: TASK-307, TASK-308
**Description**: Implements the `P50`, `P90`, `P95`, `P99` aggregate functions via DDSketch per type-system.md §6.4 and planner-pipeline.md §7.2. DDSketch's bounded relative error (~1–2 KB per group) and constant-time `merge` are load-bearing for the fused-downstream protocol: percentile accumulators must be incrementally computable so they never block fusion (planner-pipeline.md §7.2 line 668). Ships four `HashAccumulator` implementations sharing one `DdSketch` backend: `P50Accumulator`, `P90Accumulator`, `P95Accumulator`, `P99Accumulator`. Each wraps a `DdSketch` (relative accuracy 0.01 default, configurable) with the `init`/`update_batch`/`merge`/`finalize`/`size_bytes` surface from TASK-308. Input must be `Int` or `Float` per type-system.md §6.4; other types are a type-check error in the planner. Null values skipped. `finalize` returns `Float` (nullable — empty sketch returns NULL). Unit tests verify relative accuracy on known distributions (uniform, exponential, bimodal), merge associativity, and that sketch size stays within 2 KB per group up to 10M observations. Benchmark: percentile throughput by group count alongside TASK-325's aggregate suite.

### TASK-328: [HARD][IMPL] Source time-range parsing + Wave 3 acceptance-query closure
**Output**: crates/bqlite-parser/src/parser.rs, crates/bqlite-planner/src/logical.rs, tests/wave3_acceptance.rs
**Depends on**: TASK-313, TASK-316, TASK-318
**Description**: Close the acceptance-gap where Wave 3's canonical query shape (`events LAST 30d | FUNNEL(...) WITHIN 7d`) is documented but not executable end-to-end. Implement source-level `LAST <duration>` and `BETWEEN <ts> AND <ts>` parsing on pipeline sources, thread the AST time-range through logical/physical lowering, and make the Wave 3 acceptance tests use the exact query form from the wave header rather than a no-time-range substitute. The planner must still apply the existing MATCH/FUNNEL scan-extension rule (`LAST 30d` widened by `WITHIN 7d`) so boundary matches remain visible. Add parser, planner, EXPLAIN, and end-to-end tests proving the stated range is preserved, widened correctly for pattern completion, and reflected in user-visible plans.

### TASK-329: [HARD][IMPL] End-to-end MATCH variable-binding support + integration closure
**Output**: crates/bqlite-planner/src/{logical,compile}.rs, tests/tests/matcher_integration.rs
**Depends on**: TASK-306, TASK-311, TASK-321, TASK-324
**Description**: Close the current gap where `$var` bindings work in the pattern compiler and operator kernels but are not accepted through the full parse -> plan -> execute path. Extend step-predicate type-checking and lowering so `column = $var` and its commuted form are valid inside MATCH step predicates before MATCH output columns exist, while still rejecting unsupported variable usage outside MATCH or in non-equality contexts. Add full integration coverage for single-binding, multi-binding, NULL short-circuit, MATCH ALL rebinding, and mixed binding + negation cases so Wave 3's acceptance statement about variable bindings being integration-tested becomes literally true.

### TASK-330: [HARD][IMPL] Matcher benchmark matrix + perf-gate alignment closure
**Output**: benches/wave3/matcher.rs, benches/wave3/README.md, .github/workflows/bench.yml
**Depends on**: TASK-302, TASK-325
**Description**: Reconcile the shipped matcher benches with the TASK-302 design doc and the Wave 3 acceptance header. Expand the matcher microbench suite to cover the full scenario matrix from `matcher-strategy.md` §8.1 (`LinearSimple`, `LinearImmediate`, `LinearWithNegation`, `LinearWithBindings`, `LinearFull`, `GeneralNfa`, repetition, and the match-events-demanded escalation path), and construct each benchmark from a compiled pattern with an explicit `PatternClass` assertion so classifier drift cannot silently invalidate the measurement. The hard target must reflect the wave header: the query-time fast path on the reference dataset, not just step-counter-vs-NFA relative speedup, must be recorded into `target/bench-results.json` and enforced in reference mode. The existing step-counter-vs-NFA comparison remains as diagnostic evidence, but it is no longer the only matcher performance claim.

### TASK-331: [HARD][IMPL] 100M funnel query pprof pass + query-only performance closure
**Output**: docs/perf/wave3-funnel-pprof.md, benches/wave3/funnel.rs
**Depends on**: TASK-330
**Description**: Install and document `pprof` for the Wave 3 reference bench workflow, then run a profiling pass on the 100M-event funnel query with the measurement scoped to query execution only (fixture generation, ingest, and one-time setup stay outside the timed region). Capture flamegraphs / top stacks for the reference funnel query, land the highest-leverage optimizations for the matcher + aggregate path, and write up the before/after numbers and remaining bottlenecks in `docs/perf/wave3-funnel-pprof.md`. The benchmark harness should support reusing a prepared database or equivalent warm fixture so the hard target is load-bearing for query performance rather than setup overhead.

### TASK-332: [HARD][DESIGN] CompactString evaluation for matcher hot paths
**Output**: docs/design/operators/compactstring-evaluation.md
**Depends on**: TASK-331
**Description**: Investigate whether adopting `compactstring` is worth it in Wave 3's matcher-heavy paths, especially binding values, retained step properties, and any other profile-identified string-heavy state. Compare the current `String`-based implementation against a narrowly-scoped `CompactString` prototype using the 100M funnel query and representative matcher microbenches, measuring query latency, allocation count, and resident memory. Deliver a go / no-go recommendation with clear thresholds and migration boundaries: if the win is material, spell out the exact surfaces safe to convert; if not, document why the additional dependency and conversion churn are not justified.

### TASK-399: [HARD][IMPL] Wave 3 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-301, TASK-302, TASK-303, TASK-304, TASK-305, TASK-306, TASK-307, TASK-308, TASK-309, TASK-310, TASK-311, TASK-318, TASK-321, TASK-323, TASK-324, TASK-325, TASK-326, TASK-327, TASK-328, TASK-329, TASK-330, TASK-331, TASK-332
**Description**: Same audit pattern as TASK-199 / TASK-299, rescored after Wave 3 and re-run after the post-wave closure tasks land. Focus on the crates Wave 3 grew substantially — `bqlite-operators` (MATCH, matcher strategies, hash aggregate, DDSketch percentiles, sort, distinct), `bqlite-parser` (pattern + MATCH + STATS + ORDER BY + FUNNEL productions, plus source time-range parsing), and `bqlite-planner` (four new logical plan variants + four physical descriptors + pattern compilation + FUNNEL desugaring + match-aggregate fusion pass). The Tests dimension specifically checks the matcher edge-case coverage from TASK-324 and TASK-329: variable-binding tracks, negation, repetition, time-window expiry, EMIT ALL semantics, the step-counter vs NFA strategy-selection boundary, and full end-to-end `$var` binding coverage. The Benchmarks dimension specifically checks that TASK-325 and TASK-330/TASK-331 together evidence the perf expectation from TASK-302 on query-time measurement rather than setup overhead. The audit must also verify that the canonical `LAST 30d | FUNNEL(...)` acceptance query now runs end-to-end and record the CompactString recommendation from TASK-332. Any crate slipping vs. Wave 2 is flagged. Below-C grades get follow-up tasks; Wave 4 does not start until those are filed.

---

## Wave 4: Advanced Analytics

**Goal.** Advanced analytics end-to-end: RETENTION sugar, SESSIONIZE, FIRST/LAST/NTH, deterministic SAMPLE, ATTRIBUTE, cohorts and aliases, entity-aligned source JOINs, live DELETEs via tombstones, size-tiered compaction, advanced encodings, and JSON/Parquet ingest.

**Scope exclusions.**
- **Window-function-powered attribution models** remain out of scope. Wave 4 ships the raw `ATTRIBUTE(...)` row form; first-touch / last-touch / time-decay are still expressed later via window functions over that output.
- **Secondary indexes** remain later-wave work. Wave 4 ships the size-tiered compaction path already described in `storage-format.md`; there is no temperature-aware or cold-window compaction direction on the roadmap.
- **Persistent aliases / materialized cohorts** remain out of scope. Alias caching in Wave 4 is per top-level query execution only.
- **General relational joins** remain out of scope. Wave 4 only ships the entity-aligned source `JOIN` form already specified in `query-language.md`.

**Size.** 57 tasks.
**Parallelism.** 12-16 agents at peak.
**Acceptance.** Retention curves, cohort-filtered and joined-source funnels, sessionized aggregates, FIRST/LAST/NTH queries, deterministic SAMPLE, and ATTRIBUTE queries all run against compacted segments with live deletes. JSONL + Parquet ingest work end-to-end, and the advanced-encoding / compaction benchmarks are green on the reference machine.

### TASK-401: [HARD][DESIGN] Advanced encoding research
**Output**: docs/design/storage/advanced-encodings.md
**Depends on**: none
**Description**: Reference implementations + microbenchmarks for `RLE`, `DoubleDelta`, `FOR`, `PFOR`, `FSST`, `ALP`, and frequency encoding against the Wave 2 baseline set. For each candidate, record compression ratio, decode throughput, predicate-pushdown implications, segment-format impact, and implementation complexity on representative datasets (monotonic timestamps, low-cardinality strings, repeated values, skewed categorical columns, floats). Deliverable is a go / no-go recommendation per encoding with evidence, plus the exact set of codecs Wave 4 should carry forward into the v2 segment format work.

### TASK-402: [HARD][DESIGN] Segment format v2 + encoding selection policy
**Output**: docs/design/storage/segment-format-v2.md
**Depends on**: TASK-401
**Description**: Freezes the Wave 4 on-disk format extension for the codecs that survive TASK-401: new encoding IDs, footer/body metadata, any per-segment auxiliary blocks (for example FSST symbol tables), reader compatibility rules for mixed v1/v2 databases, compaction rewrite policy, and the selector heuristics the writer uses to choose among the expanded codec set.

### TASK-403: [HARD][DESIGN] Compaction concurrency protocol
**Output**: docs/design/storage/compaction-concurrency.md
**Depends on**: none
**Description**: How readers and compaction coexist without read-path locks. Freezes the unit of work (`(window, shard)`), manifest publication protocol, failure recovery, active-count cooperation with query threads, and restart/orphan cleanup expectations. Size-tiered compaction is the only compaction direction on the roadmap — there is no temperature-aware or cold-window path to carve boundaries for.

### TASK-404: [HARD][DESIGN] Tombstone and delete semantics
**Output**: docs/design/storage/deletes.md
**Depends on**: TASK-403
**Description**: Precise semantics for row-, batch-, entity-, and time-range deletes: predicate classes the planner recognizes cheaply, how a DELETE maps to shard-local tombstone files, visibility rules for concurrent queries, scan-time filtering order, compaction-time reclamation, and warning/error behavior for deletes that require a full scan to discover the affected rows.

### TASK-405: [HARD][DESIGN] SESSIONIZE operator
**Output**: docs/design/operators/sessionize.md
**Depends on**: none
**Description**: Full operator-level note for `SESSIONIZE(gap: ..., end: ...)`: boundary rules, `session_id` / `session_duration` output schema, `WITHIN SESSION` interaction with MATCH, downstream demand/forwarding requirements, fused aggregate shapes that are worth supporting in v1, state caps for pathological entities, and the benchmark / edge-case matrix the implementation must satisfy.

### TASK-406: [HARD][DESIGN] ATTRIBUTE operator
**Output**: docs/design/operators/attribute.md
**Depends on**: none
**Description**: Execution-focused design for the flat-row `ATTRIBUTE(...)` operator described in `query-language.md`: sliding touchpoint deque, `touchpoint_key` typing, left-unnest semantics for unattributed conversions, forwarded conversion-property handling, scan time-range extension, fused aggregate opportunities, and explicit confirmation that built-in credit-allocation modes are out of scope until window functions land.

### TASK-407: [HARD][DESIGN] Cohort materialization, alias binding, and entity joins
**Output**: docs/design/language/cohorts-aliases-joins.md
**Depends on**: none
**Description**: Resolves the Wave 4 language/runtime questions around `alias = pipeline`, `IN QUERY (...)`, bare `IN alias`, multi-column cohort keys, alias cycle detection, per-query caching, and entity-aligned source `JOIN` planning/runtime semantics. This is the design anchor for both cohort execution and the joined-source scan path promised by `query-language.md` §19.

### TASK-408: [HARD][IMPL] Compaction executor + scheduler
**Output**: crates/bqlite-storage/src/compaction.rs
**Depends on**: TASK-403, TASK-419
**Description**: Implements the Wave 4 size-tiered compaction path for a single table: pick eligible `(window, shard)` inputs, k-way merge them in entity order, re-encode through the latest selector, publish the replacement segments atomically, and cooperate with query load per the concurrency protocol from TASK-403. This task lands the plain compaction path; tombstone-aware filtering and reclamation are layered on by TASK-434 / TASK-435.

### TASK-409: [HARD][DESIGN][TRAIT] DemandCapabilities protocol
**Output**: docs/design/planner/demand-protocol.md
**Depends on**: none
**Description**: Replaces the Wave 1 scaffold with the real operator-side capability advertisement used by `SequenceMatch`, `Sessionize`, `EventSelect`, and `Attribute`. Freezes the shape of `DemandCapabilities`, its relationship to planner-side `DemandSet`, crate placement, forwarding/fusion capability bits, and the migration path away from `bqlite-core`'s placeholder enum so the implementation can land as a single merge-first trait change.

### TASK-410: [EASY][IMPL] JSONL ingest path
**Output**: crates/bqlite-storage/src/ingest/json.rs, crates/bqlite-engine/src/ingest.rs
**Depends on**: none
**Description**: Extends the Wave 2 `INSERT ... FROM` pipeline to JSONL. Parses objects into the existing row-coercion pipeline, handles nested property objects and row-numbered schema errors, honors the existing `map: (...)` remapping surface, and plugs into the shared integration-test fixture loader. Parquet is TASK-449.

### TASK-411: [HARD][DESIGN] EventSelect and SAMPLE operators
**Output**: docs/design/operators/event-select-sample.md
**Depends on**: none
**Description**: Defines the operator contracts for `FIRST`, `LAST`, `NTH`, and `SAMPLE`: selection semantics, per-event candidate filtering order, output schema, omitted-entity rules, deterministic sampling with explicit seed vs database-UUID-derived seed, scan-pushdown contract for sampling, and which downstream demand / fusion cases are worth supporting in Wave 4.

### TASK-412: [HARD][IMPL] Segment-format-v2 reader/writer scaffolding
**Output**: crates/bqlite-storage/src/segment/{layout,reader,writer}.rs
**Depends on**: TASK-402
**Description**: Adds the structural support for segment format v2 without yet landing every codec: new version constants, encoding discriminants, any new footer metadata blocks, v1/v2 reader coexistence, v2 writer plumbing, and mixed-version tests. This is the merge-first format task the individual encoding implementations build on.

### TASK-413: [EASY][IMPL] RLE encoding
**Output**: crates/bqlite-storage/src/encoding/rle.rs
**Depends on**: TASK-401
**Description**: Implements run-length encoding for highly repetitive columns using the same `Encoding` trait / property-test pattern established in Wave 2. If TASK-401 concludes RLE is not worth shipping, retire this task with a short note linking back to the benchmark evidence instead of silently leaving the number unused.

### TASK-414: [EASY][IMPL] DoubleDelta encoding
**Output**: crates/bqlite-storage/src/encoding/double_delta.rs
**Depends on**: TASK-401
**Description**: Implements second-order delta encoding for strongly monotonic integer/timestamp sequences, including overflow edge cases, null handling, round-trip property tests, and microbenchmarks against the existing Delta path. As with TASK-413, retire instead of implementing if TASK-401 records a no-go decision.

### TASK-415: [EASY][IMPL] FOR encoding
**Output**: crates/bqlite-storage/src/encoding/for.rs
**Depends on**: TASK-401
**Description**: Implements frame-of-reference integer encoding: base value selection, bit-packed residuals, decode hot loop, property tests for overflow and the degenerate full-width fallback, and microbenchmarks. Retire with a note linking to TASK-401 evidence if the research task records a no-go. PFOR is TASK-450.

### TASK-416: [HARD][IMPL] FSST encoding
**Output**: crates/bqlite-storage/src/encoding/fsst.rs
**Depends on**: TASK-401
**Description**: String-focused FSST implementation, including symbol-table construction, encode/decode, integration with the segment-format-v2 metadata model from TASK-402, and benchmarks against dictionary + plain + LZ4 on realistic event/property string columns.

### TASK-417: [HARD][IMPL] ALP encoding
**Output**: crates/bqlite-storage/src/encoding/alp.rs
**Depends on**: TASK-401
**Description**: Floating-point ALP codec for numeric columns where the research task shows a real win. Covers the codec implementation itself, null handling, property tests over representative float distributions, and the decode-performance evidence needed to keep it in the selector race.

### TASK-418: [EASY][IMPL] Frequency encoding
**Output**: crates/bqlite-storage/src/encoding/frequency.rs
**Depends on**: TASK-401
**Description**: Implements the frequency-sorted dictionary-style codec evaluated in TASK-401, including its applicability heuristic, property tests, and direct comparisons against the plain dictionary path. Retire if the research task records it as non-competitive.

### TASK-419: [HARD][IMPL] Advanced encoding selector integration + reader/writer compatibility
**Output**: crates/bqlite-storage/src/{encoding/mod.rs,encoding/selector.rs,segment/reader.rs,segment/writer.rs}
**Depends on**: TASK-412, TASK-413, TASK-414, TASK-415, TASK-450, TASK-416, TASK-417, TASK-418
**Description**: Registers the surviving codecs with the selector, wires their metadata into reader/writer paths, and proves mixed-version read/write compatibility end-to-end. This is the task that turns the per-codec modules into a real Wave 4 storage format rather than a set of isolated experiments.

### TASK-420: [EASY][IMPL] Parser RETENTION + SESSIONIZE stages
**Output**: crates/bqlite-parser/src/pipeline.rs, crates/bqlite-ast/src/operator.rs
**Depends on**: TASK-405
**Description**: Adds terminal `RETENTION(...)` sugar and `SESSIONIZE(gap: ..., end: ...)` to the pipeline parser, including the small AST shape update needed for `SESSIONIZE end:` to accept either a single event ref or a parenthesized event list. Unit tests cover parameter ordering, duplicate-key diagnostics, duplicate names inside `end:` lists, terminal-operator restrictions for RETENTION, and the parser-level surface for downstream `WITHIN SESSION` queries.

### TASK-421: [EASY][IMPL] Parser FIRST/LAST/NTH + SAMPLE stages
**Output**: crates/bqlite-parser/src/pipeline.rs, crates/bqlite-ast/src/operator.rs
**Depends on**: TASK-411
**Description**: Adds `FIRST(event_or_list [WHERE ...] [, lookback: ...])`, `LAST(event_or_list [WHERE ...])`, `NTH(event_or_list [WHERE ...], n [, lookback: ...])`, and fraction-only `SAMPLE(fraction: ..., seed: ...)` to the pipeline parser. Includes the small AST updates for EventSelect event-type lists, optional `lookback`, and removal of the `SampleSpec::Count` variant. Tests cover the per-operator arity rules, `NTH(event WHERE ..., n)` argument order, positive-`n` validation, LAST rejecting `lookback`, duplicate names within event lists, fraction boundary validation, and preservation of the updated AST variants.

### TASK-422: [EASY][IMPL] Parser ATTRIBUTE stage
**Output**: crates/bqlite-parser/src/pipeline.rs, crates/bqlite-ast/src/operator.rs
**Depends on**: TASK-406
**Description**: Adds `ATTRIBUTE(conversion: ..., touchpoints: ..., window: ..., touchpoint_key: ...)` to the parser, including the small AST update needed for `conversion:` and `touchpoints:` to accept either a single event ref or a parenthesized event list. Covers duplicate/missing key diagnostics, duplicate names within each list, overlap between the two lists, and expression parsing for `touchpoint_key`. No planner work here — the task is purely about surface syntax and span-accurate diagnostics.

### TASK-423: [EASY][IMPL] Parser alias definitions
**Output**: crates/bqlite-parser/src/parser.rs
**Depends on**: TASK-407
**Description**: Extends the top-level parser from `pipeline` to `(alias_def)* pipeline` so reusable aliases can precede a query. Covers the `alias = pipeline` production and span-accurate errors while preserving the language rule that alias shadowing is allowed and resolves last-wins rather than producing a duplicate-name diagnostic. Planner/runtime semantics are downstream; `IN QUERY` / bare `IN alias` are TASK-451; source `JOIN` is TASK-452.

### TASK-424: [HARD][IMPL] Logical + physical plan variants for Wave 4 query nodes
**Output**: crates/bqlite-planner/src/{logical.rs,physical.rs,explain.rs}
**Depends on**: TASK-405, TASK-406, TASK-407, TASK-411
**Description**: Adds the Wave 4 query-side plan variants promised by `logical-plan-nodes.md`: richer `Sessionize`, `EventSelect`, `Sample`, `SubqueryFilter`, and `Attribute` nodes, plus the `MergeSources` physical node for entity-aligned JOINs, `__source_table_id` / table-id-map schema plumbing, and EXPLAIN rendering. `Delete` remains owned by TASK-453 so the delete/tombstone work stays grouped with storage semantics.

### TASK-425: [HARD][IMPL] AST → logical lowering + logical → physical lowering for Wave 4
**Output**: crates/bqlite-planner/src/{logical.rs,physical.rs,expr.rs}
**Depends on**: TASK-420, TASK-421, TASK-422, TASK-423, TASK-451, TASK-452, TASK-424
**Description**: Extends the planner to lower the new Wave 4 query nodes, bind aliases in source order with cycle detection, type-check `touchpoint_key` / forwarded conversion-property references, validate table-qualified references in joined-source queries, and emit the corresponding physical descriptors. Also threads scan-range extension for ATTRIBUTE windows and EventSelect `lookback` (uniformly across joined tables), materializes cohort subqueries before the main pipeline runs, and carries the metadata the joined-source runtime needs for entity-aligned merge execution.

### TASK-426: [EASY][IMPL] RETENTION desugaring pass
**Output**: crates/bqlite-planner/src/opt/desugar_retention.rs
**Depends on**: TASK-420, TASK-425
**Description**: Planner-side rewrite from `RETENTION(...)` sugar to `SequenceMatch(FIRST, brackets, emit_all=true) -> Aggregate(...)`, mirroring the pattern used for FUNNEL in Wave 3. Covers bracket naming, cumulative vs non-cumulative bracket semantics, scan-range widening by the maximum bracket, and EXPLAIN output that still points back to the original RETENTION span when possible.

### TASK-427: [HARD][IMPL][TRAIT] DemandCapabilities relocation + planner/operator wiring
**Output**: crates/bqlite-planner/src/demand.rs, crates/bqlite-core/src/demand.rs, crates/bqlite-operators/src/operator.rs
**Depends on**: TASK-409, TASK-424
**Description**: Lands the real `DemandCapabilities` protocol: move or re-export it into its final crate home, replace the Wave 1 placeholder enum with the real capability shape, and wire planner/operator matching for forwarding/fusion-sensitive stateful operators. This is merge-first because it changes a cross-crate trait surface that Sessionize / EventSelect / Attribute all build on.

### TASK-428: [HARD][IMPL] SessionizeOperator
**Output**: crates/bqlite-operators/src/sessionize.rs
**Depends on**: TASK-405, TASK-424, TASK-427
**Description**: Implements the entity-streaming `SessionizeOperator`: gap/end-event session boundaries, `session_id` / `session_duration` emission, sub-batch continuation for oversized entities, and the fused-aggregate hooks the design doc blesses as worth supporting. Includes exhaustive boundary tests at the exact inactivity threshold and around explicit end events. Consider `CompactString` for any short string fields carried in per-entity session state (see TASK-454).

### TASK-429: [EASY][IMPL] EventSelectOperator
**Output**: crates/bqlite-operators/src/event_select.rs
**Depends on**: TASK-411, TASK-424, TASK-427
**Description**: Implements `FIRST`, `LAST`, and `NTH` as one entity operator parameterized by `EventSelectKind`, including event-type lists, optional per-event predicates, same-`ts` tie-breaking by `__seq_id`, forwarded property demand, omission of entities with no qualifying event, and exact handling of the "third qualifying event" semantics for `NTH(... WHERE ...)`.

### TASK-430: [HARD][IMPL] SAMPLE pushdown path
**Output**: crates/bqlite-planner/src/physical.rs, crates/bqlite-operators/src/scan.rs, crates/bqlite-storage/src/segment/reader.rs
**Depends on**: TASK-411, TASK-425
**Description**: Makes fraction-only `SAMPLE` real and cheap by pushing deterministic entity sampling all the way into the scan path, so unsampled entities never reach the merge/read hot loop. Covers explicit seed handling, default seed derivation from the database UUID, xxHash64-based entity hashing over the canonical entity-id bytes, pushdown through stateless WHERE / SELECT / LET chains, and sample-spec threading through the physical plan. Joined-source SAMPLE correctness is extended by TASK-436.

### TASK-431: [HARD][IMPL] AttributeOperator
**Output**: crates/bqlite-operators/src/attribute.rs
**Depends on**: TASK-406, TASK-424, TASK-427
**Description**: Implements the flat-row `ATTRIBUTE(...)` operator: maintain the sliding touchpoint deque, evaluate `touchpoint_key`, retain demanded conversion properties, emit LEFT-UNNEST rows for unattributed conversions, and support the fused aggregate cases the design doc approves. The task's tests must prove exact behavior when multiple conversions share the same touchpoints, when the window boundary is hit exactly, and when no touchpoints qualify. Consider `CompactString` for `touchpoint_key` values and any demanded string properties held in the deque (see TASK-454).

### TASK-432: [HARD][IMPL] Tombstone file storage + snapshot loader
**Output**: crates/bqlite-storage/src/tombstone.rs, crates/bqlite-storage/src/database.rs
**Depends on**: TASK-404
**Description**: Adds the concrete tombstone-file API described by the delete design: atomic read/write helpers, shard/window targeting, query-start snapshot loading, and typed helpers for row / batch / entity / time-range deletes. This is the storage-layer foundation both DELETE execution and tombstone-aware scans depend on.

### TASK-433: [EASY][IMPL] DELETE statement parser
**Output**: crates/bqlite-parser/src/dml.rs, crates/bqlite-ast/src/statement.rs
**Depends on**: TASK-404
**Description**: Parses `DELETE FROM ... WHERE ... [ALLOW SCAN]` into the DELETE AST node the logical plan consumes, including the small AST update that records the `ALLOW SCAN` opt-in flag. Covers predicate expression parsing, the `ALLOW SCAN` suffix, span-accurate diagnostics for unsupported shapes, rejection of `JOIN` after `DELETE FROM <table>`, and the table-reference surface. Planner lowering and engine execution are TASK-453.

### TASK-434: [HARD][IMPL] Tombstone-aware scan + merge path
**Output**: crates/bqlite-operators/src/scan.rs, crates/bqlite-storage/src/segment/merge.rs
**Depends on**: TASK-404, TASK-432
**Description**: Applies tombstones in the read path after pushdown but before rows leave the scan layer, preserving the exact visibility rules from the delete design. Covers row / batch / entity / time-range checks, query-snapshot isolation, and merge correctness across windows for queries that span multiple segments.

### TASK-435: [HARD][IMPL] Tombstone reclamation during compaction
**Output**: crates/bqlite-storage/src/compaction.rs, crates/bqlite-storage/src/tombstone.rs
**Depends on**: TASK-408, TASK-432, TASK-434
**Description**: Extends compaction so tombstoned rows are physically omitted from compacted outputs and fully reclaimed tombstones are removed from the shard snapshot once the new segments are published. This is the task that turns live deletes from "logically hidden forever" into "hidden immediately, reclaimed eventually."

### TASK-436: [HARD][IMPL] Joined-source scan runtime (+ SAMPLE extension)
**Output**: crates/bqlite-operators/src/scan.rs, crates/bqlite-storage/src/segment/merge.rs
**Depends on**: TASK-407, TASK-425, TASK-430, TASK-434
**Description**: Implements the entity-aligned source `JOIN` form described in `query-language.md` §19 by extending the scan/runtime path to open multiple source tables, align them on entity key, and emit the combined joined schema the planner resolved. Also extends the TASK-430 SAMPLE pushdown to joined-source scans so deterministic entity sampling remains correct across the merged inputs. This is not a general-purpose join operator — it is the specialized source merge path for tables that already share the database's shard function and entity-key type.

### TASK-437: [HARD][IMPL] Cohort subquery runtime + alias execution cache
**Output**: crates/bqlite-operators/src/cohort.rs, crates/bqlite-engine/src/query.rs
**Depends on**: TASK-407, TASK-425
**Description**: Executes `SubqueryFilterPhysical` by materializing inner-query hash sets, supports both inline `IN QUERY (...)` and bare `IN alias` references, caches alias results per top-level query execution, detects alias cycles cleanly, and supports both single-column and tuple cohort keys. Consider `CompactString` for string-typed cohort keys held in the hash set (see TASK-454).

### TASK-438: [EASY][IMPL] Engine bind step extension for Wave 4 query nodes
**Output**: crates/bqlite-engine/src/bind.rs
**Depends on**: TASK-424, TASK-425, TASK-428, TASK-429, TASK-430, TASK-431, TASK-436, TASK-437
**Description**: Extends the bind step to materialize runtime trees for `SessionizePhysical`, `EventSelectPhysical`, `SamplePhysical`, `SubqueryFilterPhysical`, `AttributePhysical`, and joined-source scans. `DELETE` remains out of scope for this task because TASK-453 executes it as a statement-level engine path rather than a bound query pipeline.

### TASK-439: [HARD][IMPL] Advanced analytics integration tests
**Output**: tests/integration/advanced_analytics/
**Depends on**: TASK-426, TASK-428, TASK-429, TASK-430, TASK-431, TASK-436, TASK-437, TASK-438
**Description**: End-to-end integration matrix for the new query primitives: session boundary edge cases, `WITHIN SESSION`, RETENTION bracket semantics (including cumulative mode), FIRST/LAST/NTH with candidate predicates, event-type lists, and `lookback:` widening, deterministic fraction-only SAMPLE behavior, joined-source queries, cohort semi-joins, ATTRIBUTE left-unnest semantics, and exact downstream aggregate results on realistic fixtures.

### TASK-440: [HARD][IMPL] Delete + compaction integration tests
**Output**: tests/integration/deletes/
**Depends on**: TASK-408, TASK-433, TASK-453, TASK-434, TASK-435
**Description**: Integration suite for live-delete correctness: delete-by-entity, delete-by-batch, delete-by-`__seq_id`, time-range delete, rejection of non-cheap predicates without `ALLOW SCAN`, exact `rows_affected` accounting, query-snapshot visibility during concurrent tombstone updates, and physical reclamation after compaction. This is the correctness evidence the wave acceptance and quality audit lean on for the storage side.

### TASK-441: [HARD][IMPL] Advanced analytics benchmark suite
**Output**: benches/wave4/
**Depends on**: TASK-408, TASK-410, TASK-449, TASK-419, TASK-430, TASK-431, TASK-436, TASK-437
**Description**: Criterion benches for the real Wave 4 performance story: advanced-encoding compression/decode comparisons, compaction throughput and read-amplification reduction, JSONL / Parquet ingest throughput, SAMPLE pushdown savings, ATTRIBUTE latency on realistic conversion/touchpoint ratios, and cohort / joined-source query overhead. The suite's README records the reference-machine targets and the bench gate promotes them into CI.

### TASK-442: [HARD][IMPL] Wave 4 acceptance test
**Output**: tests/wave4_acceptance.rs
**Depends on**: TASK-408, TASK-410, TASK-449, TASK-438, TASK-439, TASK-440, TASK-441
**Description**: The Wave 4 correctness gate per the header. Ingest JSONL and Parquet fixtures, run a sessionized retention query, a cohort-filtered joined-source funnel, a deterministic SAMPLE + FIRST/LAST/NTH query, and an ATTRIBUTE query, then apply live deletes, trigger compaction, and assert that the post-compaction answers remain identical to the pre-compaction logical answers. Wave 4 is not done until this test passes on both macOS and Linux.

### TASK-443: [EASY][IMPL] RETENTION semantic audit
**Output**: docs/reviews/wave4-retention-audit.md
**Depends on**: TASK-426, TASK-438, TASK-439
**Description**: Targeted reading pass: does the shipped RETENTION behavior make sense, and does it match `query-language.md` / `planner-pipeline.md` / the Wave 4 acceptance queries? Walk bracket ordering, cumulative vs non-cumulative semantics, `EMIT ALL` in the desugared match form, scan-range widening by the maximum bracket, aggregate naming, and EXPLAIN fidelity. Record a promise-vs-evidence matrix and flag anywhere the semantics feel wrong or drift from the docs. Drift and missing coverage are filed as follow-up tasks (rolled up in TASK-455), not fixed here.

### TASK-444: [EASY][IMPL] SESSIONIZE semantic audit
**Output**: docs/reviews/wave4-sessionize-audit.md
**Depends on**: TASK-428, TASK-438, TASK-439
**Description**: Targeted reading pass on `SESSIONIZE(gap: ..., end: ...)`: do the semantics make sense and match the design note? Walk gap-boundary handling, `end:` event precedence, `session_id` monotonicity per entity, `session_duration` calculation, sub-batch continuity for oversized entities, `WITHIN SESSION` interaction with MATCH, and fused vs unfused aggregate equivalence. Record a promise-vs-evidence matrix; drift and missing coverage are filed as follow-up tasks (rolled up in TASK-455), not fixed here.

### TASK-445: [EASY][IMPL] EventSelect + SAMPLE semantic audit
**Output**: docs/reviews/wave4-event-select-sample-audit.md
**Depends on**: TASK-429, TASK-430, TASK-436, TASK-438, TASK-439
**Description**: Targeted reading pass on `FIRST` / `LAST` / `NTH` and deterministic `SAMPLE`: do the semantics make sense and match the design note? Walk event-type lists, candidate-predicate ordering, omission of entities with no qualifying event, NTH indexing, `lookback:` widening, projection/forwarding correctness, sample determinism across repeated runs, explicit-seed vs database-UUID-derived seed behavior, fraction-only semantics, and pushdown correctness on single-table and joined-source scans. Record a promise-vs-evidence matrix; drift and missing coverage are filed as follow-up tasks (rolled up in TASK-455), not fixed here.

### TASK-446: [EASY][IMPL] ATTRIBUTE semantic audit
**Output**: docs/reviews/wave4-attribute-audit.md
**Depends on**: TASK-431, TASK-438, TASK-439
**Description**: Targeted reading pass on flat-row `ATTRIBUTE(...)`: do the semantics make sense and match the language/type-system/planner docs? Walk lookback-window boundaries, `touchpoint_key` typing, forwarded conversion-property access, multiple-touchpoint emission, LEFT-UNNEST behavior for unattributed conversions, no-touchpoint cases, and fused-aggregate equivalence to the unfused row-materializing path. Record a promise-vs-evidence matrix; drift and missing coverage are filed as follow-up tasks (rolled up in TASK-455), not fixed here.

### TASK-447: [EASY][IMPL] Cohort, alias, and joined-source semantic audit
**Output**: docs/reviews/wave4-cohort-alias-join-audit.md
**Depends on**: TASK-436, TASK-437, TASK-438, TASK-439
**Description**: Targeted reading pass on the Wave 4 composition features with the highest semantic surface: alias resolution and per-query caching, alias-cycle diagnostics, inline `IN QUERY (...)` vs bare `IN alias` equivalence, multi-column cohort keys, required table qualification inside joined-source queries, no-self-join enforcement, and the entity-aligned source-join semantics from `query-language.md` §19. Do the semantics make sense and match the docs? Record a promise-vs-evidence matrix; drift and missing coverage are filed as follow-up tasks (rolled up in TASK-455), not fixed here.

### TASK-448: [EASY][IMPL] Delete, tombstone, and compaction semantic audit
**Output**: docs/reviews/wave4-delete-tombstone-audit.md
**Depends on**: TASK-433, TASK-453, TASK-434, TASK-435, TASK-440
**Description**: Targeted reading pass on live-delete semantics: do they make sense and match the design? Walk immediate visibility of deletes, query-start snapshot isolation, planner routing of cheap delete predicate classes, `ALLOW SCAN` opt-in for full-scan shapes, exact `rows_affected` behavior, scan-time tombstone application ordering relative to pushdown/post-filter, compaction-time reclamation, and mixed workloads where deletes interact with joined-source reads, cohorts, or long-running scans. Record a promise-vs-evidence matrix; drift and missing coverage are filed as follow-up tasks (rolled up in TASK-455), not fixed here.

### TASK-449: [EASY][IMPL] Parquet ingest path
**Output**: crates/bqlite-storage/src/ingest/parquet.rs, crates/bqlite-engine/src/ingest.rs
**Depends on**: none
**Description**: Extends the Wave 2 `INSERT ... FROM` pipeline to Parquet. Consumes Arrow batches via arrow-rs, applies the width-consolidation rules from the type system doc, reuses the partitioner/writer path from Wave 2, honors the existing `map: (...)` remapping surface, and plugs into the shared integration-test fixture loader. JSONL is TASK-410.

### TASK-450: [HARD][IMPL] PFOR encoding
**Output**: crates/bqlite-storage/src/encoding/pfor.rs
**Depends on**: TASK-401, TASK-415
**Description**: Patched frame-of-reference integer codec built on the FOR scaffolding from TASK-415. Covers patch-list layout, sparse-outlier handling, decode hot loop including patch application, and property tests for overflow and the worst-case all-patched fallback. Retire with a note linking to TASK-401 evidence if the research task records a no-go. Use the fastpfor crate instead of implementing manually.

### TASK-451: [EASY][IMPL] Parser `IN QUERY` and bare `IN alias` expressions
**Output**: crates/bqlite-parser/src/expr.rs
**Depends on**: TASK-407
**Description**: Adds inline `IN QUERY (...)` and bare `IN alias` forms to the expression grammar. Covers single-column and tuple cohort keys at the syntax level, plus duplicate/empty-tuple diagnostics. Semantic resolution is downstream in TASK-425 / TASK-437.

### TASK-452: [EASY][IMPL] Parser entity-aligned source JOIN
**Output**: crates/bqlite-parser/src/parser.rs
**Depends on**: TASK-407
**Description**: Parses entity-aligned source joins per `query-language.md` §19: `source := name time_range? (JOIN name)*`. Covers join-list parsing, forbidden self-joins at the syntactic level, and table-qualified reference surface syntax. Planner-level validation of qualification rules is TASK-425.

### TASK-453: [HARD][IMPL] DELETE planner + engine tombstone writer
**Output**: crates/bqlite-planner/src/logical.rs, crates/bqlite-engine/src/query.rs
**Depends on**: TASK-404, TASK-432, TASK-433, TASK-434
**Description**: Lowers the parsed DELETE statement onto the delete execution plan, routes cheap predicate classes directly to tombstone updates, rejects non-cheap shapes unless `ALLOW SCAN` is present, reuses the tombstone-aware scan path for full-scan deletes, returns exact `rows_affected`, and owns the per-shard tombstone-write serialization / idempotent retry contract from the delete design. Owns the statement-level delete plan node because the feature is inseparable from tombstone semantics.

### TASK-454: [EASY][IMPL] CompactString adoption in matcher variable binding + Wave 4 hot paths
**Output**: crates/bqlite-operators/src/matcher/ (and other identified hot paths)
**Depends on**: TASK-399
**Description**: Applies the `CompactString` recommendation from the Wave 3 quality audit (TASK-332 / TASK-399) to the matcher variable-binding path as the primary target — the track binding slots, per-step captured values, and any propagated binding dictionaries that currently use `String`. Also sweeps the Wave 4 call-outs (TASK-428 sessionize state, TASK-431 ATTRIBUTE touchpoint-key deque, TASK-437 cohort hash-set keys) and promotes to `CompactString` where it reduces allocation pressure on representative workloads, backed by benchmark evidence. Retire any subtask where profiling shows no win.

### TASK-455: [EASY][IMPL] Wave 4 post-acceptance closure
**Output**: TASKS.md, crates as needed
**Depends on**: TASK-442, TASK-443, TASK-444, TASK-445, TASK-446, TASK-447, TASK-448
**Description**: Collects the gaps surfaced by the Wave 4 acceptance run (TASK-442) and the targeted semantic audits (TASK-443 through TASK-448). For each flagged drift, missing test, or small implementation gap, either fix it in place or convert it into a concrete follow-up task filed under Wave 5 intake. Mirrors the Wave 3 pattern where closure tasks (e.g. TASK-329, TASK-332) landed after the acceptance gate to turn audit notes into merged work. Wave 4 is not closed until this task is empty or explicitly resolved.

### TASK-456: [EASY][IMPL] Wave 4 docs and examples refresh
**Output**: docs/design/language/*, docs/design/operators/*, crates/bqlite-cli/ (as needed)
**Depends on**: TASK-425, TASK-426, TASK-428, TASK-429, TASK-430, TASK-431, TASK-436, TASK-437, TASK-453
**Description**: Updates the user-facing language and operator docs, plus any CLI help strings, to include worked examples for the features Wave 4 ships: RETENTION, SESSIONIZE + `WITHIN SESSION`, FIRST/LAST/NTH, deterministic SAMPLE, ATTRIBUTE, cohorts and aliases, entity-aligned source JOIN, and live DELETE. Ensures newcomers to the codebase can find a runnable example for each new pipeline stage without having to read a design note.

### TASK-499: [HARD][IMPL] Wave 4 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-401, TASK-402, TASK-403, TASK-404, TASK-405, TASK-406, TASK-407, TASK-408, TASK-409, TASK-410, TASK-449, TASK-411, TASK-419, TASK-425, TASK-427, TASK-438, TASK-439, TASK-440, TASK-441, TASK-442, TASK-443, TASK-444, TASK-445, TASK-446, TASK-447, TASK-448, TASK-454, TASK-455, TASK-456
**Description**: Same audit pattern as TASK-199 / TASK-299 / TASK-399, rescored after Wave 4. Focus on the crates Wave 4 grows the most: `bqlite-storage` (advanced encodings, tombstones, compaction, JSON/Parquet ingest), `bqlite-planner` (alias / cohort lowering, joined-source planning, new logical/physical nodes, real demand protocol), and `bqlite-operators` (SESSIONIZE, EventSelect, SAMPLE pushdown, ATTRIBUTE, joined-source scan support). The Tests dimension must explicitly account for the two new integration suites, the full acceptance test, and the targeted semantic audits in TASK-443 through TASK-448 (rolled up by TASK-455); the Benchmarks dimension must reflect the codec and compaction evidence from TASK-441, not just the existence of benches. Any crate slipping vs. Wave 3 is flagged. Below-C grades get follow-up tasks; Wave 5 does not start until those are filed.

---

## Wave 5: Production Quality & Performance

**Goal.** The steady-state engine lands: enforced memory budgets, spill where the design blesses it, fused scan/filter execution, multi-core morsel scheduling, real cancellation/warnings, and regression-gated performance.
**Size.** ~29 tasks.
**Parallelism.** 8-12 agents.
**Acceptance.** Under the documented default query budget, large multi-shard analytical queries run to completion with bounded memory, spill/timeout behavior matches the shipped design notes, and the Wave 5 regression gate is green on the reference machine.

**Drafting note.** This initial Wave 5 list is being written while TASK-455 is still open. A few tasks below intentionally absorb known Wave 4 carryovers that clearly belong to the steady-state engine if TASK-455 leaves them unresolved. If TASK-455 lands one of those items before Wave 5 begins, retire the corresponding Wave 5 task rather than renumbering the wave.

### TASK-501: [HARD][DESIGN] Memory budget enforcement model
**Output**: docs/design/engine/memory-budget.md
**Depends on**: none
**Description**: Freeze the real reservation/release contract for query-time memory: tracked allocation classes, per-worker vs per-query budget splits, fixed-size untracked state, operator-level spill-vs-fail policy, and how TASK-111's `MemoryBudget` trait maps onto engine `QueryContext`. This task must explicitly reconcile the remaining doc drift on the default query budget (some docs still say 3 GB, older Wave 5 text said 4 GB) and update all conflicting design notes in the same checkpoint.

### TASK-502: [HARD][DESIGN] Spill-to-disk protocol
**Output**: docs/design/engine/spill.md
**Depends on**: TASK-501
**Description**: Decide exactly which structures spill in v1, how spill files are laid out and named, which temp directory is used, how cleanup works on timeout/cancellation/panic/crash, and what still fails fast instead of spilling. This is the design gate that reconciles the current cross-doc conflict between execution-model.md (sort + IN-subquery spill, aggregate no spill) and older task text that pointed at broader spill work.

### TASK-503: [HARD][DESIGN] Fused stateless segment and operator-fusion contract
**Output**: docs/design/engine/operator-fusion.md
**Depends on**: none
**Description**: Turn execution-model.md §3.8 from "documented target" into an implementation contract: `FilteredBatch`, `SelectionVector`, `StatelessKernel`, the fused push-segment driver, materialization triggers, and the exact boundary between stateless fusion and stateful-to-aggregate fusion. The note must also decide which Wave 4 stateful operators get `finish_entity_into()` overrides in v1 and which remain intentionally unfused.

**Load-bearing forward reference:** execution-model.md §3.8 already specifies the steady-state stateless-segment design — `FilteredBatch`, `SelectionVector`, `StatelessKernel`, `materialize_filtered_batch`, and the three explicit materialization triggers (sparsity, push-segment boundary, aggregation hand-off — note the deliberate "materialization" terminology in §3.8.3, distinct from storage-format.md §7 "compaction"). The Wave 2 filter/project/limit operators (TASK-231) deliberately ship without that infrastructure because a fused push segment is required to make the selection-vector chain pay off. This design task is the point at which §3.8 moves from "documented target" to "implemented contract." It should produce the `[IMPL]` tasks that refactor TASK-231's operators into kernels that implement `StatelessKernel` and plug into a new fused-segment driver, **not** leave §3.8 to a later wave. See also the TASK-242 retirement stub for the history of how this design got deferred from Wave 2.

### TASK-504: [HARD][DESIGN] Optimizer direction reconciliation + statistics source
**Output**: docs/design/planner/optimizer-direction.md
**Depends on**: none
**Description**: Reconcile planner-pipeline.md's Wave 0 "rule-based only" v1 promise with the newer desire to make fusion/pushdown decisions data-aware. Decide whether Wave 5 stays purely rule-based, adopts narrow heuristic gating, or introduces a true cost layer; define the statistics sources (manifest metadata, zone maps, runtime counters) and which rules are allowed to consult them.

### TASK-505: [HARD][DESIGN] Cancellation, timeout, and warning protocol
**Output**: docs/design/engine/cancellation.md
**Depends on**: none
**Description**: Freeze how caller cancellation, timeout expiry, panic cleanup, spill cleanup, and operator warnings interact in the real engine. Includes typed error mapping, warning-cap behavior, cleanup ordering for temporary files, and the latency bounds at batch, sub-batch, and morsel boundaries.

### TASK-506: [HARD][DESIGN] Morsel scheduler and query/compaction sharing
**Output**: docs/design/engine/morsel-scheduler.md
**Depends on**: none
**Description**: Implementation-level note for execution-model.md §9/§14: morsel generation, work stealing, query queuing, partial-aggregate ownership, query-vs-compaction capacity sharing, and the exact metrics the runtime must expose. Risky because it locks the engine's steady-state multi-core shape.

### TASK-507: [EASY][IMPL] Per-operator microbenchmark coverage audit
**Output**: benches/coverage-report.md
**Depends on**: none
**Description**: Audit every hot path introduced in Waves 2-4 for bench coverage, including EventSelect, joined-source scan, tombstone filtering, Sessionize end-event lists, and compaction/tombstone reclamation. Any missing or non-load-bearing bench becomes a concrete Wave 5 task rather than lingering as an audit note.

### TASK-508: [HARD][IMPL] System-column materialization in scan and joined-source output
**Depends on**: none
**Description**: Materialize `__seq_id` and `__batch_id` end-to-end in ScanOperator and MergeSources, reconcile nullability/type rules, and make the system-column contract explicit in the docs. This is the correctness unblocker for EventSelect runtime, joined-source scans, and row/batch tombstone filtering.

### TASK-509: [EASY][IMPL] System-column follow-up correctness suite
**Output**: tests/tests/wave5_system_columns.rs
**Depends on**: TASK-508
**Description**: Un-ignore and strengthen the joined-source, EventSelect, SESSIONIZE, and row/batch DELETE cases that were blocked by missing system columns. Includes joined-source + DELETE, DELETE + cohort, and query-snapshot isolation coverage promoted out of the Wave 4 audit notes.

### TASK-510: [HARD][IMPL] Memory tracker enforcement scaffold
**Depends on**: TASK-501
**Description**: Land the real query-scoped memory tracker with reservations/releases, RAII guards, per-worker budget derivation, and QueryContext plumbing through the engine. No spill yet — just correct accounting and typed `MemoryBudgetExceeded` surfacing.

### TASK-511: [HARD][IMPL] Structured execution errors and warning channel
**Depends on**: TASK-505
**Description**: Replace the remaining stringly planner/execution errors with structured variants where the docs already promise them, and plumb `QueryWarning` collection/result surfacing so Sessionize, Attribute, and future memory-pressure diagnostics can ship without another API pass.

### TASK-512: [HARD][IMPL] Ingest partitioner external spill
**Depends on**: TASK-502, TASK-510
**Description**: Replace the Wave 2 "error loudly when the buffer exceeds budget" path with external spill + merge for the `(shard, window)` partitioner. Must preserve `(entity_id, ts)` ordering, batch-id assignment, and crash cleanup semantics.

### TASK-513: [HARD][IMPL] Sort spill runs + on-disk merge
**Depends on**: TASK-502, TASK-510
**Description**: Implement sorted-run spill and final k-way merge for ORDER BY. Cancellation and temp-file cleanup must match TASK-505's protocol; ORDER BY correctness must remain identical to the in-memory path.

### TASK-514: [HARD][IMPL] Cohort materialization memory enforcement
**Depends on**: TASK-502, TASK-510
**Description**: Make cohort / `IN QUERY` materialization respect the real budget. The implementation follows TASK-502's decision: either spill to the on-disk structure that task specifies or fail the whole query with a typed out-of-budget error, and update the user-facing docs to state the chosen behavior explicitly.

### TASK-515: [HARD][IMPL] Copy-budget instrumentation + null-mask preservation
**Depends on**: none
**Description**: First zero-copy checkpoint from storage/zero-copy-scan-filter.md §12: add `bytes_materialized_before_filter`-style metrics, preserve null masks instead of row-group-wide null splicing, and make the current copy budget measurable before behavior changes.

### TASK-516: [HARD][IMPL] Dictionary/RLE selection-first scan path
**Depends on**: TASK-515
**Description**: Implement encoded-view filtering for dictionary and RLE columns, producing row selections rather than dense arrays before filter. This is the first real payoff checkpoint for the zero-copy design and should measurably lower pre-filter materialization on common predicates.

### TASK-517: [HARD][IMPL] Merge without `interleave` + late materialization boundary
**Depends on**: TASK-515
**Description**: Replace row-group-wide interleave copies with stitched row references over encoded sources and materialize only at the scan/filter boundary. Must preserve tombstone filtering, joined-source semantics, and projection pruning behavior.

### TASK-518: [HARD][IMPL] Fused stateless segment scaffold
**Depends on**: TASK-503
**Description**: Land `FilteredBatch`, `SelectionVector`, `StatelessKernel`, `materialize_filtered_batch`, and the push-segment driver from execution-model.md §3.8 without changing the external `PhysicalOperator` boundary.

### TASK-519: [HARD][IMPL] Refactor Filter / Project / Limit onto fused stateless kernels
**Depends on**: TASK-518
**Description**: Move the Wave 2 stateless operators onto the fused-segment infrastructure, add selection-vector metrics, and extend the Wave 2 bench suite so the new path is load-bearing rather than optional.

### TASK-520: [HARD][IMPL] Stateful-to-aggregate fusion for Sessionize / EventSelect / Attribute
**Depends on**: TASK-503
**Description**: Implement the remaining v1 fusion shapes already described in planner-pipeline.md and the Wave 4 operator notes: planner detection, physical descriptors, `finish_entity_into()` overrides, and equivalence tests against the unfused path.

### TASK-521: [HARD][IMPL] Optimizer framework + rule-trace surface
**Depends on**: TASK-504
**Description**: Replace the current placeholder optimizer skeleton with a real pass pipeline, rule registry, and EXPLAIN-visible rule trace / stats surface. This is the merge-first scaffold for later Wave 5 rule work so new rules can land without re-litigating the optimizer shape.

### TASK-522: [HARD][IMPL] Cohort/entity pushdown into scan
**Depends on**: TASK-504
**Description**: Implement the deferred entity-id pushdown from the cohort/join design note: extend `ScanPredicate` with entity-set membership, exploit shard/segment skipping, and wire the pushed filter from materialized cohorts into the outer scan. Performance-only if the pushdown cannot apply; correctness must remain identical to the post-scan probe path.

### TASK-523: [HARD][IMPL] Morsel scheduler + partial-aggregate handoff
**Depends on**: TASK-506
**Description**: Implement the engine-side morsel generator, work queue, worker handoff, and per-shard partial-aggregate ownership model from execution-model.md §9. This is the main multi-core execution checkpoint for Wave 5.

### TASK-524: [HARD][IMPL] CPU/skew metrics + `--explain-perf` surface
**Depends on**: TASK-506
**Description**: Implement the Wave 5-only metrics rows from execution-model.md §14 — selection-vector materializations, morsel skew, worker idle/busy spread, spill bytes, and sampled CPU-cost metrics when available — and surface them through CLI/EXPLAIN tooling without making perf collection mandatory in normal queries.

### TASK-525: [HARD][IMPL] Memory-pressure, spill, and cancellation stress suite
**Output**: tests/tests/wave5_runtime_stress.rs
**Depends on**: TASK-510, TASK-511, TASK-512, TASK-513, TASK-514, TASK-523
**Description**: Stress tests for hard budget exhaustion, spill fallback, concurrent DELETE/query snapshot isolation under real runtime scheduling, timeout cleanup of temp files, and warning-channel overflow behavior.

### TASK-526: [HARD][IMPL] Wave 5 benchmark suite + regression-gate refresh
**Output**: benches/wave5/
**Depends on**: TASK-507, TASK-516, TASK-517, TASK-519, TASK-520, TASK-522, TASK-523, TASK-524, TASK-527
**Description**: Add benchmark groups and CI baselines for the new execution path: zero-copy scan/filter copy budget, fused stateless segment, stateful-to-aggregate fusion, morsel-scheduler skew behavior, spill overhead, and cohort pushdown savings. Extends the existing bench gate rather than creating a one-off suite.

### TASK-527: [HARD][IMPL] Scan-adjacent optimizer rule pack
**Depends on**: TASK-521
**Description**: Implement the first concrete Wave 5 rule pack beyond framework plumbing: safe filter-before-match reordering, MATCH/EventSelect predicate extraction that can become scan pushdown, and the heuristic or cost-gated materialization/fusion decisions blessed by TASK-504. This is the task that turns the optimizer lane from "framework exists" into "the planner is actually smarter on production workloads."

### TASK-529: [HARD][IMPL] BRACKETS runtime emission in SequenceMatch operator
**Output**: crates/bqlite-operators/src/matcher/ (compile.rs, nfa.rs, step_counter.rs, output.rs), crates/bqlite-planner/src/physical.rs
**Depends on**: TASK-425, TASK-426, TASK-427
**Status note**: The original-blocker panic ("Column 'bracket' is declared as non-nullable but contains null values") is already mitigated. Commit `670b2d5` (TASK-439 followup CP4) relaxed the planner's `MATCH` lowering to declare `bracket` / `bracket_end` as nullable, and commit `e505bfc` (TASK-499 followup, audit P1 #4) made the matcher output layer's null emission for those columns explicit and self-documenting. The four originally-blocked RETENTION integration tests are already un-ignored and asserting `row_count() > 0` (not per-bracket rates). The work below is the remaining feature: actually emitting per-bracket rows so RETENTION reports real bracket-indexed retention.
**Description**: Implements per-bracket row emission in SequenceMatch, completing RETENTION end-to-end (TASK-443 audit finding R1). The Wave 4 physical planner discards `BracketSpec` at `physical.rs:1237` (`brackets: _`), the compiled NFA carries no bracket state, and `output.rs` deliberately emits a single row per match with null `bracket` / `bracket_end` until this task lands. Required: (a) add `brackets: Option<BracketSpec>` to `SequenceMatchPhysical` and forward from logical; (b) extend `CompiledNfa` to carry bracket durations + `cumulative` flag; (c) implement per-bracket emission in `output.rs` — for EMIT ALL with brackets, one row per `(entity, binding track, bracket)` with `step_reached` set to the highest step completed within `[prev_duration, duration)`; (d) implement cumulative partial-sum: bracket N's `step_reached` is `max(step_reached[0..=N])`; (e) clarify and implement `bracket_end` semantics in `query-language.md §4.12` — absolute epoch (`anchor_ts + duration_ns`) vs relative duration (R3); (f) once emission produces non-null brackets, tighten the `bracket` / `bracket_end` nullability in `MATCH` lowering back to non-null where the spec allows it (reverse the 670b2d5 relaxation); (g) add `brackets`/`cumulative` to `ExplainNode::SequenceMatch`, align with `planner-pipeline.md §10.2` (R4); (h) strengthen the two un-ignored RETENTION integration tests to assert specific bracket-indexed `retention_rate` values rather than `row_count() > 0`; (i) proptest for cumulative bracket monotonicity (R6); (j) test BRACKETS × variable-binding composition §30.6 (R7). TASK-455 already fixed R2 (ascending-order validation in `lower_match`).

### TASK-530: [RETIRED]
**Status**: Retired before scheduling. Originally scoped as "WITHIN SESSION expiry in NFA compiler" to address TASK-444 audit finding A — the planner coalescing `MatchWindow::WithinSession` to `None` so `SESSIONIZE | MATCH … WITHIN SESSION` accepted cross-session matches. The work landed early as TASK-499 audit P0 #1 (commit `031cdf5`, 2026-04-26): `compile.rs` now sets `CompiledNfa.session_window` on `WithinSession`, all three matcher paths (`nfa.rs`, `bindings.rs`, `step_counter.rs`) track per-entity `last_session_id` and call `expire_all_candidates` on session boundary, `matcher/mod.rs` adds `session_id` to `required_column_names`, and both integration tests (`within_session_match_expires_across_boundary`, `within_session_match_composes_with_downstream_stats`) are un-ignored. Number retired per the "numbers are never reused" rule.

### TASK-531: [EASY][IMPL] EventSelect property tests and benchmarks
**Output**: crates/bqlite-operators/src/event_select.rs (proptest module), benches/wave4/event_select.rs
**Depends on**: TASK-508
**Description**: Adds property tests and benchmarks for EventSelect once the `__seq_id` scan-materialization blocker (TASK-508) is resolved. Concrete spinoff of TASK-507 for this operator. Property tests (TASK-445 finding E2): (1) output cardinality exactly 0 or 1 row per entity; (2) FIRST emits row with min `(ts, __seq_id)` among qualifying events; (3) LAST emits max `(ts, __seq_id)`; (4) NTH(n) emits row with exactly n−1 qualifying events at smaller `(ts, __seq_id)`; (5) omission iff fewer than `n` qualifying events; (6) entity isolation; (7) NTH(1) ≡ FIRST. Benchmarks (E3, with explicit targets): FIRST ≥200M events/s/core (10M events, 100K entities), LAST ≥100M, NTH(5) ≥150M, FIRST+WHERE ≥150M, event-type list match (no string alloc), per-entity memory <2 KB at 10 demanded columns, entity boundary overhead <500 ns. Bench file at `benches/wave4/event_select.rs` using `bqlite_benches::common` generators.

### TASK-532: [EASY][IMPL] ATTRIBUTE composition and WITHIN SESSION integration coverage
**Output**: tests/tests/wave4_advanced_analytics_attribute.rs, tests/tests/wave4_advanced_analytics_sessionize.rs
**Depends on**: TASK-509
**Description**: Adds the Wave 4 integration coverage that existing TASK-509 doesn't enumerate. TASK-509 already covers un-ignoring joined-source/EventSelect/SESSIONIZE/row+batch DELETE tests and the joined-source+DELETE / DELETE+cohort / snapshot-isolation cases; the WITHIN SESSION runtime fix landed early via the retired TASK-530 (commit `031cdf5`), and the integration tests there already pass. This task adds: (1) `events LAST <d> | ATTRIBUTE(window: <d>, …)` end-to-end scan-extension path (TASK-446 A4); (2) multi-type `ATTRIBUTE(conversion: (purchase, subscription), touchpoints: (ad_click, email_open), …)` (A5); (3) `SESSIONIZE | ATTRIBUTE` composition (A6); (4) proptest for WITHIN SESSION semantics verifying no cross-session matches (TASK-444 §12) — strengthens the now-passing example tests with a property-based assertion. Note: TASK-448 F4 (concurrent DELETE + query isolation under real scheduling) is owned by TASK-525's stress suite; the per-query snapshot correctness argument is supported by code inspection and TASK-509's sequential coverage.

### TASK-533: [EASY][IMPL] Wave 4 minor planner and operator correctness fixes
**Output**: crates/bqlite-planner/src/logical.rs, crates/bqlite-planner/src/explain.rs, crates/bqlite-operators/src/event_select.rs, crates/bqlite-operators/src/sessionize.rs, crates/bqlite-core/src/error.rs
**Depends on**: none
**Description**: Collects the minor correctness fixes and missing guards identified by Wave 4 semantic audits (TASK-443–448) that are too small for individual tasks but more numerous than TASK-455's in-place closure absorbed. (1) TASK-447 J8: alias-name table-name collision check in `lower_statement_with_aliases` / `push_definition` — consult `catalog.resolve_table(name)` before inserting, reject with `BqliteError::Plan("alias name '...' shadows table '...'")`, add test `alias_name_matching_table_name_is_rejected`. (2) TASK-446 A2: explicit `MATCH | ATTRIBUTE` planner rejection in `lower_attribute` ("ATTRIBUTE cannot consume MATCH output; MATCH emits per-match rows rather than raw event rows"), with planner unit test. (3) TASK-447 J3 (planner-only subset of TASK-511): add `AliasCycle { path: Vec<String> }` and `IncompatibleCohortShape { lhs_arity: usize, rhs_arity: usize }` variants to `BqliteError` (or new `TypeError` sub-enum) replacing string-formatted `Plan` errors at `logical.rs:1635–1645` and `logical.rs:1681–1731`; coordinate scope with TASK-511. (4) TASK-445 E6: document `forwarded_columns` as unused-in-v1 on `EventSelectPhysical` — comment that the demand-driven forwarding path derives column availability from `output_schema` instead. (5) TASK-447 J2: decide whether to add `BqlType::SmallInt`/`Int8` to bqlite-core or update `cohorts-aliases-joins.md §3.8` to accept `Int` (i64) for `__source_table_id`; implement chosen path; update `type-system.md` §7.2 with the `__source_table_id` section. (6) TASK-444 finding B: replace the hardcoded `"entity_id"` literal in `sessionize.rs:201` (`push_name("entity_id", ...)`) with the actual entity-key column name propagated through `SessionizePhysical` — without this, any table whose entity key isn't literally named `entity_id` panics at SESSIONIZE construction. (7) TASK-446 A1: verify-only — commit `fda797e` (TASK-499 followup P2 #2) already aligned the `lower_attribute` rustdoc with `window: 0s` acceptance, and the actual guard accepts `window == 0` per spec §16.1. The remaining work is purely confirming a plan-level test asserts zero window does not error; if missing, add it.

### TASK-534: [EASY][IMPL] Tombstone scan filter microbenchmark
**Output**: benches/wave4/tombstone_scan.rs
**Depends on**: TASK-507
**Description**: Add Criterion benches for both tombstone scan paths in `crates/bqlite-storage/src/tombstone_scan.rs`. (1) `TombstoneScanWrapper` (query-time): measure `TombstoneFilter::filter_batch_with_index` throughput on 65 536-row batches with entity-delete sets of 0 / 100 / 10 000 entries, a time-range delete covering 10% of rows, and a mixed-granularity case (entity + time-range + row simultaneously); report surviving rows/second and a `[floor]` regression tripwire per granularity combination. (2) `CompactionTombstoneScan` (compaction-time): seed segments with 1% / 5% / 10% entity-tombstoned rows and measure `Database::compact_now` throughput relative to the clean-segment baseline in `compaction.rs`; record `[floor]` asserting the tombstoned-to-clean throughput ratio does not exceed 2× at 10% tombstone density. Both groups run in CI mode (scaled-down fixtures) and reference mode. Also register a `[[bench]]` entry for `tombstone_scan` in `benches/Cargo.toml` and add the new metrics and floor targets to `benches/wave4/README.md` per the wave4 bench gate protocol.

### TASK-535: [EASY][IMPL] Sessionize multi-end-event-type benchmark
**Output**: benches/wave4/sessionize.rs (extend existing file)
**Depends on**: TASK-507
**Description**: Extend `benches/wave4/sessionize.rs` to add a `bench_multi_end_event` group covering end-event lists of 1 / 3 / 5 types in both `StringViewArray` and `Dictionary<Int32, Utf8View>` variants — the `EndEventCodeSet` fast path from `sessionize.md §8.2` is currently benchmarked only for a single end-event type. Use the same 10 000 and 100 000-event scale points as the existing `bench_throughput` group. Add a `[floor]` tripwire asserting the 3-type dictionary case is no more than 1.5× slower than the 1-type dictionary baseline (`EndEventCodeSet.matching_codes` is a `HashSet<i32>`, so O(1) probe cost at ≤5 entries should be near-free). Record all variants via `BenchResultCollector` so the CI bench gate picks up regressions.

### TASK-528: [HARD][IMPL] Wave 5 acceptance gate
**Output**: tests/tests/wave5_acceptance.rs
**Depends on**: TASK-509, TASK-512, TASK-513, TASK-514, TASK-517, TASK-519, TASK-520, TASK-522, TASK-523, TASK-524, TASK-525, TASK-526, TASK-527, TASK-529, TASK-531, TASK-532, TASK-533
**Description**: End-to-end gate for the wave. Runs a large multi-shard analytical query under the documented budget, proves cancellation/timeout cleanup on a long-running query, verifies that sort/ingest/cohort behavior follows the chosen spill policy, and asserts that the fused/zero-copy path produces exactly the same results as the fallback path on the reference fixtures.

Additional Wave 5 tasks: individual optimizer rule implementations, spill implementations per spillable operator, fusion implementations for specific operator pairs, cancellation plumbing per operator, property tests, stress tests, memory-pressure integration tests.

### TASK-599: [HARD][IMPL] Wave 5 quality audit
**Output**: docs/quality-score.md
**Depends on**: TASK-528
**Description**: Same audit pattern as TASK-199, rescored after Wave 5. Wave 5 is the production-quality wave — the audit is a hard gate, not a reflective pass. Every crate is expected to be at least B across all dimensions; anything below B ships only with a named owner, a concrete remediation plan, and human sign-off before Wave 6 begins. The Benchmarks dimension specifically verifies that regression gates are wired up in CI and have been green for at least one full merge cycle. Any below-B grade is a blocker, not a follow-up.

---

## Wave 5 closure follow-ups

These tasks were filed on 2026-05-08 after a parallel completion audit (`docs/reviews/wave5-hard-task-audit.md`) found that several Wave 5 tasks shipped scaffold-level work plus self-documented "deferred" comments rather than the full payoff their task text describes. They are scoped to close those gaps before Wave 6 begins. TASK-541 is reserved per `engine/cancellation.md` §8 for the morsel-scheduler timeout/panic plumbing. TASK-545 was filed on 2026-05-09 to carry the sub-shard morsel-generator work that `morsel-scheduler.md` §11.2 / §13.3 explicitly defer past TASK-536's v1 boundary; it is a Wave 5+ follow-on that does not block Wave 6.

### TASK-536: [HARD][IMPL] Real per-shard morsel dispatch (TASK-523 closure)
**Depends on**: TASK-523
**Description**: Replace the "one degenerate whole-database task per query" dispatch path documented at `crates/bqlite-engine/src/query.rs:454-487` with the per-shard morsel parallelism the TASK-523 spec and `morsel-scheduler.md` §11.2 call for. Required: (a) `MorselGenerator::degenerate` emits one morsel per non-empty `ShardSnapshot` covering the shard's full entity range (`EntityRange::All`), per `morsel-scheduler.md` §11.2's v1 boundary; sub-shard `(shard, entity-range)` halving and the §3.4 adaptive control loop are explicitly out of scope per §13.3 ("open decisions: generator sub-shard parallelism") and tracked separately under TASK-545; (b) the engine submits one Rayon task per morsel, each acquiring its own `WorkerMorselGuard` from the queue with the existing FIFO `CoreBudget` permit semantics; (c) per-shard partial-aggregate ownership lands — each worker writes into its own `AccumulatorHandle` and the coordinator merges on shard-done; (d) `WorkerMetricsSnapshot` is recorded once per Rayon worker thread that pulled at least one morsel (deduped by `rayon::current_thread_index`), not once per query, so `worker_busy_ns_min/_max` and `morsels_per_shard_min/_max` carry real values; (e) keep DDL/DELETE/EXPLAIN on the bypass path from §5.4 so they remain single-tasked; (f) extend `wave5_acceptance.rs` to assert `metrics.morsels_per_shard_min > 0` and `metrics.num_workers >= 1` on the multi-shard fixture (the lower bound tolerates single-core CI runners; the v1 generator's `min == max == 1` is also pinned to lock in the §11.2 boundary). Out of scope: sub-shard morsel-range halving (TASK-545); morsel-skew adaptive control loop (TASK-545); CPU counters (TASK-537). **Closed**: shipped per §11.2 v1 scope on 2026-05-09 via commits d6848a4 / b50d7d3 / c32a8d2 / 4042516 / 3f47f73 / ffe7c32 / 46d04bc; the original spec wording "one morsel per `(shard, entity-range)` slice" was tightened on 2026-05-09 to match the design doc's explicit v1 boundary, with TASK-545 carrying the sub-shard slicing.

### TASK-537: [HARD][IMPL] Real worker timing + CPU counter integration (TASK-524 closure)
**Depends on**: TASK-536
**Description**: Make the `--explain-perf` rows from TASK-524 carry real values rather than placeholders. Required: (a) instrument the worker driver loop to record `Instant::now()` deltas for busy time (work) and idle time (waiting on queue / waiting on permit), populating `WorkerMetricsSnapshot::worker_idle_ns` and `worker_busy_ns` per-morsel; (b) compute `entity_event_skew_p99` as the p99-vs-p50 spread of per-morsel processed-events count once TASK-536 emits one snapshot per worker; (c) implement `PerfCounters::open_or_disabled()` for Linux via `perf_event_open` (RAII close on drop), graceful disable on permission failure (`CAP_PERFMON` not granted) — this currently returns disabled on every platform per `crates/bqlite-engine/src/perf.rs:265-267`; (d) macOS `kpc` integration is a v2 follow-on (note in code, gate behind cfg flag); (e) upgrade `benches/wave5/morsel_skew.rs:15-19` from a wall-clock-only tripwire to assertions that `entity_event_skew_p99 > 0` on a deliberately skewed fixture, and that `worker_idle_ns_p50` is bounded; (f) update the `--explain-perf` CLI rendering to label any cfg-gated CPU rows clearly when disabled (e.g. `cycles: not collected (no CAP_PERFMON)`) rather than printing zero. Reference: `execution-model.md` §14, `morsel-scheduler.md` §8.

### TASK-538: [HARD][IMPL] Public cancellation/timeout API + acceptance coverage (TASK-525, TASK-528 closure)
**Depends on**: TASK-505
**Description**: `engine/cancellation.md` §6.2 anticipates a per-query cancel/timeout knob on the public `Engine::query` surface. Today the surface only accepts a SQL string; cancellation is reachable only through `QueryContext::cancellation()` from inside the engine, which is why `wave5_runtime_stress.rs:21-23` and `wave5_acceptance.rs:14-21` carve cancellation/timeout out as "contract level" tests. Required: (a) add `Engine::query_with(sql: &str, opts: QueryOptions)` accepting `QueryOptions { cancel: Option<CancellationToken>, timeout: Option<Duration>, memory_budget_bytes: Option<usize> }` (the budget knob is already on `QueryContext`); leave the existing `Engine::query(sql)` as a thin wrapper for SQL-only callers; (b) wire `timeout` to a per-query timer that fires `CancellationToken::cancel(CancelReason::Timeout)` from a coordinator task, matching cancellation.md §3.2 yield-point latency bounds; (c) add `wave5_runtime_stress.rs` end-to-end coverage for: external-cancel mid-scan, timeout fires before completion, timeout cleanup of spill files; (d) extend `wave5_acceptance.rs` band 2 to drive cancellation through the new public API rather than at the contract level; (e) move TASK-541's per-(worker, morsel) `catch_unwind` into scope here so timeout + panic interact under the new API. Out of scope: per-statement cancel from the CLI (Wave 6 / TASK-538b).

### TASK-539: [EASY][IMPL] Ingest partitioner spill stress + acceptance coverage (TASK-525, TASK-528 closure)
**Depends on**: TASK-512
**Description**: TASK-512 landed external ingest spill, but `wave5_runtime_stress.rs:28` calls it out-of-scope and `wave5_acceptance.rs:32-34` says ingest partitioner spill "is not yet covered directly." Required: (a) add `ingest_partitioner_spill` scenario to `wave5_runtime_stress.rs::spill_fallback` band — drive `Database::ingest` past the partitioner budget so spill files are created and merged on drain, assert the resulting batches preserve `(entity_id, ts)` ordering across the spilled-vs-resident boundary, and that no spill artefacts persist after `Database::flush()`; (b) add an ingest-spill leg to `wave5_acceptance.rs` band 3 that exercises a multi-shard ingest large enough to trigger spill at `MIN_QUERY_BUDGET_BYTES`, then runs an analytical query against the resulting database to assert no spill-induced row corruption. Reuse the proptest fixtures in `crates/bqlite-storage/src/ingest/partitioner.rs::tests` for the seed.

### TASK-540: [EASY][IMPL] Same-database concurrent DELETE/query stress (TASK-525 closure)
**Depends on**: TASK-525
**Description**: `wave5_runtime_stress.rs:329-555::snapshot_isolation` covers delete-between-queries and concurrent queries on **two separate database paths**, but not concurrent DELETE/query on the **same** database under scheduler pressure — which is the case Wave 5's snapshot-isolation contract actually pins (`storage/deletes.md` §9). Required: (a) add `delete_concurrent_with_query_on_same_db` test using a single `Database` with `ENGINE_QUERY_THREADS=1` to force scheduler contention; spawn a writer thread issuing `DELETE WHERE entity_id IN (...)` while a reader thread runs `SELECT COUNT(*) FROM events`; assert the reader's snapshot is internally consistent (`COUNT(*)` agrees with the rowset visible at its query-start time per the snapshot-isolation contract); (b) repeat with `ENGINE_QUERY_THREADS=4` to exercise the morsel queue under contention once TASK-536 lands real per-shard dispatch; (c) include a 1000-iteration loop variant gated behind `cfg(stress)` for nightly runs.

### TASK-542: [EASY][IMPL] Rustdoc warning cleanup pass
**Depends on**: none
**Description**: Wave 5 audit Finding 1 records the trajectory 33 → 41 → 67 → 94 (+27 in Wave 5). Without a cleanup pass the projection is bqlite-storage Docs ↓ B+ → B and bqlite-engine Docs ↓ B → C+ in Wave 6. Required: (a) replace private-item rustdoc links with plain backticks across the Wave 5 additions enumerated at `docs/quality-score.md:151-158` (`try_reserve`, `TombstoneScanWrapper`, `Partitioner::estimated_event_size`, `with_spill_dir`, `entity_delete_index`, `lex`/`parser`/`error`, `coalesce_scan_predicates`/`fuse_match_aggregate` collisions, `order_stateless_filters::rank`, `crate::finalize_physical`, `clamp_filter_tile_size`, `pre_fusion_output_schema` (×3), `bind_cohorts` (×2), `DeleteFilter`, `bqlite_storage::SampleFilter`, `SortSpillHandler` (×2), `SubqueryFilterOperator`, `SpillCleanup`, `CoreBudget`, `cycles_per_event`, `Mutex`, `finish`); (b) re-alias the `coalesce_scan_predicates` and `fuse_match_aggregate` function/module collisions by importing the function with `use … as` or moving the helper to a sibling module; (c) document the 1 persistent `bqlite` top-level re-export collision as expected (or fix it via `#[doc(no_inline)]`); (d) target: `cargo doc --workspace --no-deps` produces ≤67 warnings (Wave 4 baseline); (e) add a CI gate in `.github/workflows/ci.yml` that runs `cargo doc --workspace --no-deps -- -D warnings` on a `nightly` toolchain step or a non-blocking `warn` count check, so trajectory drift surfaces in PRs rather than at audit time.

### TASK-543: [EASY][IMPL] Wave 4 + Wave 5 follow-up bench CI integration
**Depends on**: TASK-526
**Description**: TASK-526 added 5 Wave 5 bench groups and they are now in `bench.yml`'s invocation list (commit on 2026-05-08). The remaining 14 wave-scoped bench groups registered in `benches/Cargo.toml` are not yet in CI: `tombstone_scan`, `fused_segment`, `sessionize`, `attribute`, `event_select`, `cohort_join`, `compaction`, `sample`, `encoding_matrix`, `wave4_ingest`, `pfor`, `scan_encoded`, `compactstring_eval`, `funnel_profile`. Required: (a) add the 14 benches to all three `bench.yml` jobs (bench-baseline, bench-gate, bench-reference) so the regression gate covers every wave-scoped Criterion group; (b) confirm `bench-compare.sh`'s 10% × 3-sample threshold scales sensibly to ~30 groups (it should — the script handles each metric independently — but a sanity run on a no-op PR is warranted); (c) update `docs/quality-score.md` Wave 5 status to remove the "remaining 14" caveat once the workflow is updated and one full main-merge cycle has elapsed with the gate green.

### TASK-544: [EASY][DESIGN] bqlite + bqlite-ffi below-B grade remediation
**Output**: docs/quality-score.md
**Depends on**: none
**Description**: TASK-599's hard gate (`TASKS.md:1392-1395`) requires below-B grades to "ship only with a named owner, a concrete remediation plan, and human sign-off." `bqlite` (Tests C, Docs C+, Benchmarks C) and `bqlite-ffi` (C across all four dimensions) violate that bar. Required: (a) for `bqlite`: decide whether the re-export crate gets `#[doc = include_str!("../README.md")]`-driven Docs uplift + at least 1 doctest per re-exported public type to reach Tests B and Docs B (preferred), or whether the audit explicitly accepts the re-export-only model and records that decision with a one-line rationale per dimension; (b) for `bqlite-ffi`: explicitly defer to Wave 6 with a recorded named-owner entry citing TASK-603 (PyO3) and TASK-604 (C ABI) as the remediation plan; the C grades stay until Wave 6 lands, but the audit's gate honesty is satisfied; (c) update `docs/quality-score.md` Wave 5 status with an explicit "named owner / remediation plan / human sign-off" table replacing the current open-gap acknowledgement; (d) once landed, the per-crate Benchmarks-C pattern (5 crates) gets the same treatment — either accept the workspace-bench model with rationale or file individual benches.

### TASK-545: [HARD][IMPL] Sub-shard morsel generator + operator entity-range awareness
**Depends on**: TASK-536, TASK-537
**Description**: Land the §3.4 adaptive halving control loop and per-`(shard, entity-range)` morsel slicing that `morsel-scheduler.md` §11.2 / §13.3 explicitly defer past TASK-536's v1 scope. Today `MorselGenerator::degenerate` emits one whole-shard morsel per shard (`EntityRange::All`); operators receive each morsel as the entire shard and rely on that. Required: (a) implement `MorselGenerator::adaptive(snapshot, Arc<MorselSizeState>)` per §3.4 — emits multiple morsels per shard with `EntityRange::Bounded { lo, hi }` honoring the entity-aligned boundary rule from §3.3 (next morsel's `lo` equals previous morsel's `hi`, never splits an entity), respecting `current_target_rows` between `low_target_rows` and `high_target_rows`; (b) wire the coordinator's drain pump to read `worker_idle_ns_p99` from the running `QueryMetrics` sketch and halve `current_target_rows` once every `halving_warmup_morsels` morsels (sticky, multi-step, never grows back) per §3.4; (c) teach every morsel-consuming operator to honor `EntityRange::Bounded` — at minimum `ScanPhysical` (entity-range filter pushed into segment reader), `FusedSegment` chains, and the `Aggregate`/`PerShardAggregate` driver path; the §3.3 single-entity invariant must hold so an entity is processed by exactly one morsel; (d) extend `Database::segment_reader_for_shard` (or a new `segment_reader_for_shard_range`) to accept an `EntityRange` and only return rows whose `entity_id` falls in `[lo, hi)`; (e) update `wave5_acceptance.rs` to drop the `morsels_per_shard_min == max == 1` pin from line 372-375 and instead assert `morsels_per_shard_max > morsels_per_shard_min` on a deliberately-large multi-shard fixture (one shard with > `high_target_rows` rows so it must split); (f) add a per-`(shard, entity-range)` correctness proptest reusing `tests/src/strategies.rs` Arrow generators — for any partition of the entity space, the multi-morsel result must equal the single-morsel baseline up to row order; (g) update `morsel-scheduler.md` §11.2 to drop the v1-degenerate caveat and §13.3 open decision #3 to mark the sub-shard parallelism resolved. Out of scope: cross-shard morsel coalescing for tiny shards (§13.5 — separate decision); priority lanes (§13.2). This is a multi-crate refactor; expect changes in `bqlite-engine` (scheduler), `bqlite-storage` (segment reader range filter), `bqlite-operators` (entity-range awareness in scan/fused/aggregate drivers), and `tests` (acceptance + property suites).

---

## Wave 6: Interfaces

**Goal.** Embeddable via CLI, Python, and C ABI.
**Size.** ~15-20 tasks.
**Parallelism.** 4-6 agents.
**Acceptance.** `pip install bqlite` runs on macOS and Linux, CLI subcommands work against a real database.

### TASK-601: [DESIGN] Python API surface
**Output**: docs/design/interfaces/python-api.md
**Depends on**: none
**Description**: Idiomatic Python wrapper over the engine API. Query, ingest, iterate results, type coercion, error mapping, async support question.

### TASK-602: [DESIGN] CLI command structure
**Output**: docs/design/interfaces/cli.md
**Depends on**: none
**Description**: `query`, `ingest`, `explain`, `repl`, `compact`, `stats` subcommands, flag conventions, output formats.

### TASK-603: [IMPL] PyO3 integration skeleton
**Output**: crates/bqlite-ffi/src/python.rs, python/bqlite/__init__.py
**Depends on**: TASK-601
**Description**: PyO3 module declaration, Event type binding, Database and Query type wrappers, result iteration, error translation.

### TASK-604: [IMPL] C ABI surface
**Output**: crates/bqlite-ffi/src/c.rs
**Depends on**: none
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
**Depends on**: none
**Description**: Datasets, query mix, comparison baselines, reporting format. Drives the public benchmark story.

### TASK-702: [DESIGN] User documentation plan
**Output**: docs/design/interfaces/docs-plan.md
**Depends on**: none
**Description**: Getting started, query language guide, operator reference, API reference, FAQ. Audience segmentation and reading order.

### TASK-703: [IMPL] Error message audit
**Output**: tests/errors/
**Depends on**: none
**Description**: Every user-facing error has a test asserting the message is clear, actionable, and mentions the source location where applicable.

### TASK-704: [IMPL] Edge case audit
**Output**: tests/edge_cases/
**Depends on**: none
**Description**: Systematic review of edge cases — empty datasets, single-event entities, segment boundary crossings, huge entities, zero-time-range queries, schema mismatches, ingest partial failures.

Additional Wave 7 tasks: benchmark dataset acquisition, benchmark runner, public benchmark report, getting-started guide, query language guide, operator reference, error taxonomy document, README polish.

### TASK-799: [IMPL] Wave 7 quality audit — shippable grade
**Output**: docs/quality-score.md
**Depends on**: TASK-701, TASK-702, TASK-703, TASK-704
**Description**: Final pre-ship audit. Same audit pattern as TASK-199, but the standard is an A on every dimension for every crate on the public surface (bqlite, bqlite-core, bqlite-cli, bqlite-ffi). Internal crates may ship at B only if the concrete gap keeping them from A is documented with a rationale and a post-1.0 follow-up task. The public benchmark report from TASK-701 supplies the Benchmarks evidence for the entire workspace. Anything below this standard blocks release — the audit is the last green light before tagging 1.0.
