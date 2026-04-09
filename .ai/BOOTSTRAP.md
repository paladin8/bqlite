# bqlite — Bootstrap Document

> **bqlite**: An embeddable, high-performance query engine for temporal event sequence analysis.
>
> GitHub description: `Embeddable behavioral query engine — temporal pattern matching, funnels, retention, and cohort analysis over entity event streams, powered by Rust and Apache Arrow.`

This document is the founding specification for bqlite. It is intended to be read by Claude Code to bootstrap the repository, project structure, development workflow, and harness engineering setup. Read this document in full before taking any action.

---

## 1. Project Identity

- **Name**: bqlite
- **Repository**: `paladin8/bqlite`
- **License**: MIT
- **Language**: Rust (engine) + Python (DSL/bindings via PyO3)
- **Tagline**: Embeddable behavioral query engine for temporal event streams

bqlite is a query engine purpose-built for temporal pattern queries over ordered event streams partitioned by entity. It fills the gap between general-purpose OLAP engines (DuckDB, ClickHouse) that require tortured SQL for behavioral queries, and SaaS analytics platforms (Amplitude, Mixpanel) that aren't embeddable or open-source.

The core data model is:
- A **database**: a directory on disk containing table metadata, storage segments, and consistency mechanisms (analogous to a SQLite file or RocksDB directory). All CLI and API operations target a database directory. A lock file prevents concurrent access.
- A **table**: a named, strongly-typed collection of entity event streams within a database. Tables have a declared schema (entity key, timestamp column, event type column, property columns with types). Queries can reference and join across tables.
- An **event**: a nanosecond-precision timestamp, an event type, and a set of typed properties conforming to the table's schema.
- An **entity**: the thing events happen to (user, device, server, transaction — configurable per table via the entity key column).

The same engine handles product analytics (user behavioral funnels), infrastructure monitoring (server event sequences), fraud detection (transaction patterns), security (audit log analysis), IoT (device telemetry), and gameplay analytics.

---

## 2. Design Principles

These principles govern every design decision. When in doubt, refer back here.

1. **Performance is the top priority.** Vectorized execution, cache-aware data layouts, lock-free design, optimized compression, predicate pushdown, operator fusion. bqlite should be the fastest engine for its problem domain by a wide margin. Consider custom memory allocation (slab allocator) where it provides measurable benefit.

2. **Powerful primitives over specialized features.** The query language exposes composable primitives (sequence matching, windowing, filtering, aggregation). Funnels, retention, and cohorts are expressible as compositions of these primitives — they are convenience wrappers, not special cases. The primitives must be powerful enough to express features like holding a property constant across funnel steps, custom retention brackets, and complex session definitions. The execution engine may use specialized operators under the hood, but the language remains general.

3. **Entity-first data model.** Every query implicitly operates per-entity. The storage format, scan layer, and operators are all designed around the entity-partitioned access pattern.

4. **Clean compiler architecture.** Strict separation between the query language (frontend), logical planning, optimization, physical planning, and execution. New optimizations and physical operators can be added without changing the language.

5. **Embeddable, not a server.** bqlite is a library — `pip install bqlite` or a Rust crate dependency. No server process, no deployment, no configuration beyond pointing at a database directory. Think SQLite, not PostgreSQL.

6. **Memory-conscious.** Explicit memory budgets (default 4 GB), spill-to-disk for large intermediate results, streaming evaluation where possible. Queries over billions of events should work with bounded memory.

7. **Distributed-ready architecture.** We are not building distributed execution in v1, but the architecture should not preclude it. Physical plans should be partitionable. State should be serializable. Nothing should assume single-node.

8. **Strongly-typed pipelines.** Every operator has a well-defined input and output schema. Operators can be piped into each other with compile-time (at plan time) type checking. The planner rejects queries where schemas don't align, with clear error messages.

---

## 3. Architecture Overview

### 3.1 Compiler Pipeline

bqlite uses a classic multi-stage compiler architecture:

```
BQL text ──→ Parser ──→ AST
                          │
Rust builder API ──→ AST  │  (both produce the same AST)
                          │
Python builder API ──→ AST│
                          ▼
                    Logical Planner
                          │
                          ▼
                    Logical Plan
                    (LogicalMatch, LogicalFunnel,
                     LogicalFilter, LogicalAggregate, ...)
                     Each node declares its output schema.
                          │
                          ▼
                      Optimizer
                    (pattern recognition,
                     predicate pushdown,
                     operator fusion)
                          │
                          ▼
                    Optimized Logical Plan
                          │
                          ▼
                    Physical Planner
                    (selects concrete operators:
                     general NFA vs optimized funnel,
                     hash vs sort aggregate, ...)
                          │
                          ▼
                    Physical Plan
                          │
                          ▼
                    Execution Engine
                          │
                          ▼
                      Results (Arrow RecordBatches)
```

The key discipline: the language and logical plan know nothing about execution strategies. Optimization rules operate on logical plans. The physical planner maps logical operators to concrete implementations. New specialized physical operators (e.g., an optimized funnel evaluator) can be added without changing the language or logical plan.

Every logical and physical plan node declares its **output schema** — the columns and types it produces. The planner validates schema compatibility at plan construction time, not at execution time. This makes operator composition (piping) type-safe.

### 3.2 Execution Model

**Pull-based iterator with entity-aware batching.** Downstream operators pull from upstream. The scan layer yields entity-complete chunks: all events for entity X, then all events for entity Y. This means stateful temporal operators (sequence matcher, funnel evaluator) can maintain per-entity state as local variables rather than hash map lookups — dramatically better cache behavior.

For stateful temporal operators (sequence, funnel, retention): process one entity at a time. For stateless operators (filter, project, aggregate): operate on columnar batches.

The pipeline: scan layer partitions data by entity → feeds entity streams into temporal operators → results collected into columnar batches → aggregation operators reduce.

### 3.3 Storage and Data Layout

> **⚠️ DEEP DIVE REQUIRED**: Storage design is a critical area requiring extensive design work before implementation. The decisions here (segment structure, compaction strategy, merge vs partial state, WAL design, concurrency control) have cascading effects on query performance, ingest throughput, and operational complexity. This section describes the high-level direction; a detailed design doc (docs/design/storage-format.md) must be produced during the design phase.

**Entity-major storage layout.** This is a key architectural choice that differentiates bqlite from general-purpose engines. Data is laid out entity-major: all events for a given entity are contiguous, pre-sorted by timestamp. This makes temporal pattern matching a single sequential scan per entity with optimal cache behavior.

**Database directory model.** A bqlite database is a directory on disk (analogous to RocksDB or SQLite). It contains:
- Table metadata (schemas, configuration)
- Storage segments (the actual event data in native format)
- Write-ahead log (WAL) for crash safety
- Lock file preventing concurrent access from multiple processes
- Compaction state and metadata

**LSM-tree-inspired storage engine.** The high-level model is similar to RocksDB:
- **In-memory buffer (memtable)**: new events are written to an in-memory buffer, sorted by (entity, timestamp).
- **WAL**: writes are durably logged before acknowledging, enabling crash recovery.
- **Flush**: when the memtable exceeds a size threshold, it is flushed to disk as a new sorted segment.
- **Compaction**: background compaction merges multiple segments, re-sorting by entity to restore entity-locality and reclaiming space from deleted events.
- **Multiple sorted segments**: at any point, multiple segments may exist. The scan layer merge-scans across them to produce entity-complete streams.

This model supports incremental ingest (insert new events without rewriting the entire dataset) and deletion (tombstone markers, reclaimed during compaction).

**Raw format ingest is always pre-processed.** There is no query-time sort-merge of raw CSV/JSON/Parquet files. All data must be ingested into the native format via `INSERT` statements or the `bqlite ingest` CLI command before querying. This simplifies the query engine — it only ever reads the native format.

Important design questions to resolve (flagged for deep dive):
- Segment size targets and compaction triggers
- Compaction strategy (leveled vs tiered vs hybrid)
- How to handle entities that span multiple segments during compaction
- Whether operators need to checkpoint partial entity state across segment boundaries, or whether the merge-scan layer always produces fully-merged entity streams
- Index structures (bloom filters on entity keys, min/max indexes on timestamps)
- Compression strategy per column type
- Concurrency between readers and compaction

The native format should apply custom compression optimized for event data: dictionary encoding for event types, delta encoding for timestamps, appropriate encoding for property values by type.

### 3.4 Timestamps and Time

- Internal resolution: **nanoseconds** (i64 epoch nanos).
- Storage: **variable-length encoded** — if data only has second or millisecond resolution, storage should not waste bytes on unused precision.
- The query language supports **time quantization** as a first-class concept — `quantize(timestamp, 1h)`, `by day`, `by week` — implemented as efficient truncation operations, not expensive formatting.

### 3.5 Memory Management

- All operators must respect a configurable **memory budget** (default: 4 GB).
- Intermediate results that exceed the budget must **spill to disk** (sorted temporary files, memory-mapped for re-read).
- Entity-at-a-time processing naturally bounds per-entity memory, but aggregation state across millions of entities requires explicit management.
- The scan layer should support memory-mapped I/O for large data files.
- Consider implementing a **custom slab allocator** for operator state if profiling shows the general allocator is a bottleneck. Event processing involves many small, short-lived allocations with predictable sizes — a slab allocator could eliminate fragmentation and reduce allocation overhead significantly.

### 3.6 Entity Cardinality and Safety

bqlite must handle both:
- **Wide**: millions of entities with hundreds of events each (product analytics shape)
- **Tall**: thousands of entities with millions of events each (IoT/infra shape)

Some queries explode on tall entities. Queries should support an **entity event limit** — if an entity has more events matching some criteria than a configured threshold, that entity is skipped and flagged in the result metadata (not silently dropped). This prevents a single pathological entity from consuming unbounded resources.

---

## 4. Query Language (BQL)

> **⚠️ DEEP DIVE REQUIRED**: The query language design requires extensive work during the design phase. The examples below illustrate the direction; final syntax, operator schemas, and composition rules must be specified in docs/design/query-language.md before implementation begins.

### 4.1 Core Concepts

BQL is a purpose-built language for temporal pattern queries. It is NOT SQL. The fundamental operations are:
- **Data manipulation**: insert events into tables, delete events from tables (no updates)
- **Sequence matching**: ordered event pattern matching with time windows
- **Aggregation**: statistical reduction over entity results
- **Selection**: retrieving per-entity match details
- **Composition**: piping operator outputs into downstream operators with schema validation

Every operator has a **declared output schema**. Operators pipe into each other (`|`) with compile-time schema checking. This is analogous to SQL subqueries producing typed tables, but with temporal semantics.

### 4.2 Tables and Data Manipulation

```sql
-- Create a table with a typed schema
CREATE TABLE events (
  user_id STRING ENTITY KEY,
  ts TIMESTAMP,
  event_type STRING EVENT TYPE,
  amount FLOAT,
  query STRING,
  device STRING
)

-- Insert events (from literal values, CSV, JSON, Parquet files)
INSERT INTO events FROM 'events.csv'
INSERT INTO events FROM 'events.parquet'
INSERT INTO events VALUES ('user1', '2024-01-01T00:00:00Z', 'signup', ...)

-- Delete events matching criteria
DELETE FROM events WHERE ts < '2023-01-01'
DELETE FROM events WHERE event_type = 'debug'
```

### 4.3 Event Type Names

Event type/name is a **first-class concept** in BQL — it is the primary identifier used in pattern matching. However, the event type column is also available as a normal string column that can be filtered, grouped, and projected like any other property.

Since event type names may contain spaces, BQL uses bracket notation for quoting:

```sql
-- Simple event names (no spaces) are bare identifiers
match(signup -> purchase) ...

-- Event names with spaces use bracket notation
match([Sign Up] -> [Add to Cart] -> [Complete Purchase]) ...

-- The event type column is also usable as a regular string column
... WHERE event_type ~= ".*purchase.*"
... | stats count by event_type
```

The exact quoting syntax is TBD — brackets `[Sign Up]`, backticks `` `Sign Up` ``, or double quotes `"Sign Up"` are all options to evaluate during the language design deep dive.

### 4.4 Pattern Matching and Temporal Queries

```sql
-- Sequence pattern matching
match(signup -> add_to_cart -> purchase)
  within 7d
  by user_id

-- Pattern with property filters (rich predicate support including regex)
match(search WHERE query ~= ".*shoes.*" -> purchase WHERE amount > 50)
  within 1d
  by user_id

-- Negation: events that must NOT occur between steps
match(signup -> !unsubscribe ->* purchase)
  within 30d
  by user_id

-- Repetition quantifiers
match(page_view{3,} -> purchase)
  within 1h
  by user_id

-- Hold a property constant across steps (e.g., same product category)
match(view WHERE category = $c -> purchase WHERE category = $c)
  within 7d
  by user_id

-- Aggregation pipeline (pipe into downstream operators)
match(A -> B) within 7d by user_id
  | where matched = true
  | stats count, avg(match_duration), p99(match_duration)

-- Selection: return matching entities and their match details
match(error -> crash) within 1h by device_id
  | select entity_id, match_events, match_duration

-- Time quantization
match(purchase) by user_id
  | stats count by quantize(timestamp, 1d)
```

### 4.5 Funnel and Retention (Convenience Wrappers)

Funnels and retention are **strictly convenience wrappers** over the primitive operators. Every funnel or retention query must be expressible using the primitives directly. The wrappers exist for ergonomics, not for functionality that can't be achieved otherwise.

```sql
-- Funnel (sugar over sequence matching + step-wise aggregation)
funnel(signup, add_to_cart, checkout, purchase)
  within 7d
  by user_id

-- Funnel with held property (same product through entire funnel)
funnel(view WHERE product_id = $p, add_to_cart WHERE product_id = $p, purchase WHERE product_id = $p)
  within 7d
  by user_id

-- Retention with standard intervals
retention(
  entry: signup,
  returning: any,
  intervals: weekly,
  periods: 12
) by user_id

-- Retention with custom/unbounded brackets
retention(
  entry: signup,
  returning: purchase,
  brackets: [1d, 7d, 14d, 30d, 90d, unbounded]
) by user_id
```

### 4.6 Sessions

Sessions are defined by a combination of **inactivity timeout** and/or **explicit end events**. The sessionizer is an upstream operator — its output (events annotated with session IDs) feeds into downstream operators.

```sql
-- Basic inactivity-based sessions
sessionize(gap: 30m) by user_id

-- Session ends on explicit event OR inactivity
sessionize(gap: 30m, end_events: [logout, app_close]) by user_id

-- Inactivity only on specific events (e.g., only user-initiated events reset the timer)
sessionize(gap: 30m, active_events: [click, scroll, keypress]) by user_id

-- Funnel conversion within sessions
sessionize(gap: 30m) by user_id
  | match(search -> purchase) within session

-- Average session length
sessionize(gap: 30m) by user_id
  | stats avg(session_duration), p50(session_duration), count
```

### 4.7 Path Analysis (Sankey)

Path analysis / Sankey diagram aggregation is a complex query type that aggregates event sequences into flow graphs:

```sql
-- Top paths through the product (Sankey-style)
paths(depth: 5, from: signup)
  by user_id
  | stats count by path
  | order by count desc
  | limit 20
```

This requires a dedicated design spec — the output schema (paths as arrays, counts, branching factors) and the aggregation strategy need careful thought.

### 4.8 Cross-Table Queries

Tables can be joined for queries that need to correlate events across different data sources:

```sql
-- Correlate user events with server-side events
match(events.click -> server_logs.error)
  within 1s
  by user_id
```

The join semantics (entity key alignment, timestamp ordering across tables) require design work.

### 4.9 Builder APIs

The Rust and Python builder APIs produce the same AST as the text parser. Both are first-class citizens.

Rust:
```rust
use bqlite::{seq, event, Query};

let query = seq(event("search").times(2..), event("purchase"))
    .within("7d")
    .by("user_id")
    .stats(["count", "avg(match_duration)"]);
```

Python:
```python
import bqlite

db = bqlite.open("./my_database/")

result = db.query("""
    match(search{2,} -> purchase) within 7d by user_id
""")

# Or via builder
from bqlite import seq, event
pattern = seq(event("search").times(2, None), event("purchase")).within("7d")
result = db.match(pattern, by="user_id")

# Funnel sugar
result = db.funnel("signup", "add_to_cart", "purchase", within="7d")

# Retention sugar
result = db.retention(entry="signup", returning="any", intervals="weekly", periods=12)
```

### 4.10 Query Results

Every query produces per-entity results that can optionally be aggregated:
- **Per-entity boolean**: did this entity match the pattern?
- **Per-entity match data**: the specific events that matched, timestamps, properties, durations
- **Aggregates over entities**: conversion rates, timing distributions, counts

Results are returned as Arrow record batches for zero-copy interop with the Python ecosystem (pandas, polars, pyarrow).

---

## 5. Crate Structure

```
bqlite/
├── Cargo.toml                  # Workspace root
├── CLAUDE.md                   # Agent entry point (~100 lines, table of contents)
├── TASKS.md                    # Task list for agent coordination
├── docs/                       # System of record for all documentation
│   ├── architecture.md         # Domain map and crate layering
│   ├── core-beliefs.md         # Agent-first operating principles
│   ├── quality-score.md        # Per-crate quality grades
│   ├── reliability.md          # Operational requirements
│   └── design/                 # Feature design specs
│       ├── INDEX.md
│       ├── storage-format.md   # ⚠️ DEEP DIVE — see Section 3.3
│       ├── query-language.md   # ⚠️ DEEP DIVE — see Section 4
│       ├── sequence-matching.md
│       └── ...
├── .claude/
│   └── skills/                 # Task-specific agent playbooks
│       ├── implement-operator/
│       │   └── SKILL.md
│       ├── add-parser-production/
│       │   └── SKILL.md
│       ├── add-test-fixture/
│       │   └── SKILL.md
│       └── fix-ci/
│           └── SKILL.md
├── crates/
│   ├── bqlite/                 # Top-level re-export crate (what users depend on)
│   │                           #   Re-exports Database, Query, seq(), event(), etc.
│   ├── bqlite-core/            # Core types: Event, Entity, Schema, Timestamp,
│   │                           #   PropertyValue, EntityEventStream, TableSchema
│   ├── bqlite-storage/         # Native storage format, segment management,
│   │                           #   WAL, memtable, compaction, ingest from
│   │                           #   CSV/JSON/Parquet, compression,
│   │                           #   entity-major layout, merge scanning,
│   │                           #   database directory management, lock file
│   ├── bqlite-parser/          # BQL text → AST
│   ├── bqlite-ast/             # AST types shared by parser and builders
│   ├── bqlite-planner/         # AST → logical plan → optimizer → physical plan
│   │                           #   Schema validation at plan construction time
│   ├── bqlite-operators/       # Physical operator implementations
│   │   ├── src/
│   │   │   ├── scan.rs         # Entity-partitioned scan with merge support
│   │   │   ├── filter.rs       # Predicate evaluation (including regex)
│   │   │   ├── sequence.rs     # General NFA-based temporal pattern matcher
│   │   │   ├── funnel.rs       # Optimized funnel evaluator
│   │   │   ├── retention.rs    # Retention matrix computer
│   │   │   ├── sessionize.rs   # Session segmentation (gap + end events)
│   │   │   ├── aggregate.rs    # Hash/sort aggregation
│   │   │   ├── cohort.rs       # Behavioral cohort materializer
│   │   │   ├── paths.rs        # Sankey-style path aggregation
│   │   │   ├── limit.rs        # Entity event limit enforcement
│   │   │   └── ...
│   ├── bqlite-engine/          # Execution orchestration, memory management,
│   │                           #   spill-to-disk, plan execution, slab allocator
│   ├── bqlite-cli/             # Command-line interface
│   └── bqlite-ffi/             # C ABI surface for PyO3 bindings
├── python/
│   └── bqlite/
│       ├── __init__.py         # High-level Python API
│       ├── _native.pyd         # PyO3 bindings
│       ├── builders.py         # seq(), event(), fluent API
│       ├── sugar.py            # .funnel(), .retention(), .cohort()
│       └── viz.py              # Optional: rendering helpers
├── tests/
│   └── suite/                  # Integration test fixtures:
│       │                       #   input events + query + expected output
│       ├── sequence/
│       ├── funnel/
│       ├── retention/
│       ├── sessionize/
│       ├── paths/
│       └── ...
├── benches/                    # Criterion benchmarks
└── .devcontainer/              # Dev container for agent execution
    ├── Dockerfile
    └── devcontainer.json
```

### 5.1 Dependency Direction (Enforced in CI)

```
bqlite-core          (no internal deps)
    ↓
bqlite-ast           (depends on core)
    ↓
bqlite-storage       (depends on core)
bqlite-parser        (depends on ast)
bqlite-planner       (depends on ast, core)
    ↓
bqlite-operators     (depends on core, storage, planner)
    ↓
bqlite-engine        (depends on planner, operators, storage, core)
    ↓
bqlite-cli           (depends on engine)
bqlite-ffi           (depends on engine)
bqlite (top-level)   (re-exports engine, builders from ast — this is what
                      users import: `use bqlite::{seq, event, Database}`)
```

Note: `bqlite-operators` depends on `bqlite-planner` because it needs the physical plan node types that the planner defines. The planner does NOT depend on operators — the mapping from logical to physical is done via trait objects / registry, not direct imports.

Dependency direction is enforced by CI — a crate may only depend on crates above it in this ordering. A structural test validates this. Violations block merges.

---

## 6. CLI Interface

All CLI operations target a **database directory**. The database directory is the fundamental unit — it contains table metadata, storage segments, WAL, compaction state, and a lock file.

```bash
# Initialize a new database
bqlite init ./mydb

# Create a table with a schema
bqlite schema ./mydb "CREATE TABLE events (
  user_id STRING ENTITY KEY,
  ts TIMESTAMP,
  event_type STRING EVENT TYPE,
  amount FLOAT
)"

# Ingest data into a table
bqlite ingest ./mydb events --from events.csv
bqlite ingest ./mydb events --from events.parquet
bqlite ingest ./mydb events --from events.json

# Query
bqlite query ./mydb "match(signup -> purchase) within 7d by user_id"

# Output formats
bqlite query ./mydb "..." --format table     # default: pretty table to stdout
bqlite query ./mydb "..." --format json
bqlite query ./mydb "..." --format csv

# Inspect database metadata
bqlite inspect ./mydb
bqlite inspect ./mydb events    # inspect specific table

# Compact storage segments
bqlite compact ./mydb

# REPL
bqlite repl ./mydb
```

---

## 7. Performance Targets and Approach

These are aspirational targets that inform design decisions:

- **Scan throughput**: >1 GB/s of event data on a single core
- **Sequence matching**: millions of entities with hundreds of events each evaluated in seconds
- **Memory**: bounded at a configurable budget (default 4 GB), spill to disk for larger workloads
- **Startup**: instant — no server, no warmup, no JVM

Techniques to employ:
- **Entity-major data layout**: sequential scan per entity, optimal cache utilization
- **Vectorized stateless operators**: filter, project, aggregate use SIMD-friendly columnar processing
- **Lock-free parallel execution**: partition entities across threads, no shared mutable state between entity evaluations
- **Predicate pushdown**: push property filters into the scan layer, skip irrelevant events before they reach operators
- **Operator fusion**: the physical planner may fuse adjacent operators (e.g., scan + filter, or sequence match + funnel aggregation) into a single pass
- **Custom compression**: dictionary encoding for event types, delta encoding for timestamps, adaptive encoding for property values
- **Lazy evaluation**: pull-based execution avoids materializing intermediate results unless necessary
- **Memory-mapped I/O**: for large data files
- **Spill-to-disk**: sorted temp files for intermediate results exceeding memory budget
- **Custom slab allocator**: for operator state with predictable allocation patterns (evaluate during profiling)

---

## 8. Harness Engineering Setup

bqlite follows the harness engineering methodology. The repository is structured for agent legibility and autonomous development.

### 8.1 CLAUDE.md

The CLAUDE.md file is ~100 lines and serves as a **table of contents**, not an encyclopedia. It points to deeper sources of truth in `docs/` and `.claude/skills/`. It includes:
- Repository overview (1-2 sentences)
- Crate map with one-line descriptions
- Dependency direction rules
- Build commands (`cargo build`, `cargo test`, `cargo clippy`, `cargo bench`)
- Pointer to docs/architecture.md
- Pointer to docs/core-beliefs.md
- Pointer to docs/quality-score.md
- Pointer to TASKS.md
- Coding conventions: idiomatic Rust, standard error handling with `thiserror` for library errors, `anyhow` for CLI/test errors, Rust 2021 edition

### 8.2 docs/ Directory

- **architecture.md**: Full crate map, dependency direction, data flow diagrams
- **core-beliefs.md**: The design principles from Section 2 of this document
- **quality-score.md**: Markdown table grading each crate (A-F) on: test coverage, API completeness, documentation, performance benchmarks. Updated as development progresses.
- **reliability.md**: Requirements like "all I/O operations must have timeouts", "all operators must respect memory budget", "all errors must be typed and recoverable"
- **design/**: Per-feature design specs indexed in design/INDEX.md. Each spec includes: motivation, API surface, data flow, edge cases, test plan.

### 8.3 Skills

`.claude/skills/` contains task-specific playbooks in the format `.claude/skills/{skill-name}/SKILL.md`:
- **implement-operator/SKILL.md**: How to add a new physical operator — trait to implement, where to register it, test fixture format, benchmark template
- **add-parser-production/SKILL.md**: How to add new syntax to BQL — grammar location, AST node to add, planner mapping, test expectations
- **add-test-fixture/SKILL.md**: How to write integration tests — input event format, query format, expected output format, directory structure
- **fix-ci/SKILL.md**: How to diagnose and fix CI failures — common patterns, clippy lint resolution, test debugging

### 8.4 CI Pipeline

CI runs on every push to main:
1. `cargo fmt --check` — formatting
2. `cargo clippy -- -D warnings` — lints
3. `cargo test` — all unit and integration tests
4. `cargo test --test structural_deps` — dependency direction validation
5. `cargo bench` — performance benchmarks (results recorded, regressions flagged)
6. Code coverage report (target: track coverage per crate, flag decreases)
7. Python tests: `cd python && pip install -e . && pytest`

---

## 9. Multi-Agent Development Workflow

bqlite is developed using multiple concurrent Claude Code agents working in parallel, coordinated through git.

### 9.1 Architecture

```
GitHub repo (paladin8/bqlite) ← remote origin
  │
  ├── agent-container-1: clone → work → push to main
  ├── agent-container-2: clone → work → push to main
  ├── agent-container-3: clone → work → push to main
  ├── ...up to 8 containers
```

Each agent runs in a Docker devcontainer with the full Rust toolchain, Python, and all dependencies pre-installed. Each container has its own local clone of the repository.

### 9.2 Devcontainer

The `.devcontainer/` directory defines the agent execution environment:
- Rust stable toolchain + clippy + rustfmt
- Python 3.11+ with pip, maturin, pytest
- Arrow dependencies
- Git configured for push access
- `cargo` and `clippy` pre-warmed

### 9.3 Agent Loop

Each agent runs in a continuous loop:

```
1. git pull origin main
2. Read TASKS.md, identify unclaimed tasks
3. Claim a task by writing a lock file to tasks/active/<task-id>.lock
   and pushing immediately
   - If push fails (another agent claimed it), pick a different task
4. Read the task specification and relevant docs/design/ files
5. Implement the task:
   a. Write code
   b. Write tests (emphasize edge cases and high coverage)
   c. Run cargo test (use --fast flag for quick feedback, full suite before commit)
   d. Run cargo clippy
   e. Spawn a subagent to code review the changes (multiple rounds for complex changes)
   f. Fix any issues from tests, clippy, or code review; iterate
   g. Update relevant documentation in docs/
6. Commit all changes with a descriptive message referencing the task ID
7. git pull --rebase origin main (incorporate other agents' work)
8. Resolve any merge conflicts
   - If conflicts are too complex to resolve cleanly, abandon the local work
     and restart the task on fresh main
9. Push to main
10. Remove the lock file, commit and push
11. Return to step 1
```

### 9.4 Agent Behavioral Requirements

Agents must follow these practices at all times:

1. **Flag architecture/design decisions for human review.** When a task involves a significant design choice (new abstraction, interface change, performance tradeoff), the agent must document the decision and its alternatives in the relevant docs/design/ file and flag it for human review rather than making the call unilaterally.

2. **Document decisions religiously.** Every non-trivial decision must be captured in documentation — design docs, code comments, or CLAUDE.md updates. Documentation must be updated before committing, not as a separate follow-up.

3. **Emphasize test and benchmark coverage.** Write tests for every code path, including edge cases (empty inputs, single-event entities, entity event limits, spill-to-disk triggers, segment boundary crossings). Add benchmarks for performance-critical paths. Identify and test edge cases proactively, don't just test the happy path.

4. **Code review via subagents.** After implementing a change, spawn a subagent to review the code. For complex changes (new operators, storage engine modifications, planner changes), do multiple review rounds. The reviewer should check: correctness, performance implications, API ergonomics, error handling, documentation completeness, and test coverage.

5. **Always run tests and update docs before committing.** No commit should be made without `cargo test` passing, `cargo clippy` clean, and documentation updated. This is non-negotiable.

6. **Regularly consider refactoring.** After completing a task, evaluate whether the code would benefit from refactoring for maintainability, interface intuitiveness, or extensibility. Small, focused refactoring is encouraged; large refactors should be filed as separate tasks.

7. **Performance-first mindset.** Every implementation decision should consider performance implications. Prefer zero-copy over allocation, iterators over collections, stack over heap, cache-friendly access patterns over random access. When in doubt, benchmark.

### 9.5 Task Structure (TASKS.md)

TASKS.md is the coordination hub. Each task has:

```markdown
## TASK-042: Implement hash aggregate operator

**Status**: unclaimed | active:<agent-id> | complete
**Crate**: bqlite-operators
**Depends on**: TASK-010 (core types), TASK-020 (scan operator)
**Design doc**: docs/design/aggregation.md

**Description**: Implement a hash-based aggregation operator that groups
entity match results by specified keys and computes aggregate functions
(count, sum, avg, min, max, percentiles).

**Acceptance criteria**:
- Operator implements the PhysicalOperator trait
- Supports count, sum, avg, min, max, p50, p90, p95, p99
- Respects memory budget, spills to disk for large group-by cardinality
- Tests in tests/suite/aggregate/ all pass
- Benchmark added to benches/aggregate.rs
```

Tasks are organized in waves:
- **Wave 0**: Design phase — extensive design deep dives on storage format, query language, operator schemas, and execution model. Produces detailed design docs. Done with a single Claude Code session with human review.
- **Wave 1**: Foundation — core types, AST, storage format basics, CI pipeline
- **Wave 2**: Storage engine — native format, WAL, memtable, compaction, ingest from CSV/JSON/Parquet, entity-partitioned scan
- **Wave 3**: Parser and planner — BQL grammar, logical plan, optimizer, physical planner, schema validation
- **Wave 4**: Operators — sequence matcher, funnel, retention, sessionizer, filter, aggregate, paths (high parallelism — most tasks are independent)
- **Wave 5**: Engine — execution orchestration, memory management, spill-to-disk
- **Wave 6**: CLI and Python — command-line interface, PyO3 bindings, Python API
- **Wave 7**: Polish — benchmarks, documentation, error messages, edge cases

### 9.6 Conflict Prevention

- Agents work on different crates/modules, minimizing merge conflicts
- Each operator is a separate file — two agents rarely touch the same file
- Shared interfaces (traits in bqlite-core) are established in Wave 1 before parallel work begins
- If a push fails due to conflicts, the agent rebases, resolves, and retries
- If conflicts are too complex, the agent abandons its local work and restarts the task on fresh main
- The lock file mechanism prevents two agents from working on the same task

### 9.7 Test-Driven Acceptance

Every task has mechanical acceptance criteria. "Does this pass?" replaces "does this look right?" Agents iterate in a loop until:
- `cargo test` passes (including the task's specific test fixtures)
- `cargo clippy -- -D warnings` is clean
- The code compiles with no warnings
- Documentation is updated
- Code review subagent has approved

This is what enables autonomous operation — no human judgment required to determine if a task is done.

### 9.8 Practical Notes

- **Time blindness**: Agents can lose track of time. The loop should include periodic progress markers. If an agent has been working on a single task for more than 30 minutes without a successful test run, it should checkpoint its progress, commit WIP, and consider decomposing the task.
- **Test sampling**: For fast feedback during development, agents can run a subset of tests. The full suite must pass before committing.
- **Entity spanning segments**: Integration tests should include cases where an entity's events span multiple storage segments, to validate merge-scan and partial state handling.
- **Test fixture format**: Each integration test is a directory containing: `input.json` (events to ingest), `query.bql` (the BQL query to run), and `expected.json` (the expected output). A test harness ingests the input, runs the query, and compares output to expected. This format makes it trivial for agents to write new tests — no Rust code needed, just data files.
- **Synthetic data generation**: Include a utility (Rust binary or Python script) that generates realistic synthetic event data at configurable scale (entity count, events per entity, event type distribution, property distributions). This is essential for benchmarking and for testing at scale without real data.

---

## 10. V1 Scope

The v1 milestone is: a working CLI that can create databases, define table schemas, ingest event data from CSV/JSON/Parquet, evaluate BQL queries, and print results as tables/JSON/CSV.

### 10.1 V1 Operators

Core operators for v1:
- **Scan**: entity-partitioned scan with merge support across segments
- **Filter**: property predicate evaluation including regex
- **Sequence matcher**: NFA-based temporal pattern matching with time windows, negation, repetition, property predicates, held properties
- **Funnel evaluator**: convenience wrapper over sequence matching + stepwise aggregation, supporting held properties
- **Retention computer**: retention matrix over configurable time intervals with custom/unbounded brackets
- **Sessionizer**: session segmentation with inactivity gap AND/OR explicit end events, usable as upstream operator for downstream queries (funnel within session, session length stats)
- **Aggregate**: hash-based aggregation with standard functions (count, sum, avg, min, max, percentiles, count_distinct)
- **Path aggregator**: Sankey-style path analysis
- **Entity limit**: per-entity event count enforcement with skip-and-flag behavior

### 10.2 V1 Non-Goals

- Distributed execution (architecture should not preclude it)
- Real-time / streaming ingestion (batch ingest only)
- GUI or web interface
- User authentication or multi-tenancy
- UPDATE operations (INSERT and DELETE only)

---

## 11. What To Do With This Document

This document should be read by Claude Code to bootstrap the bqlite repository. **The bootstrapping session should ONLY scaffold the repository structure — it should NOT implement any engine logic, operators, parsers, or storage code.** Stub crates with empty `lib.rs` files and placeholder types are sufficient. The goal is a repo that compiles, has CI passing, and is ready for the design phase.

The bootstrapping process should:

1. Initialize the git repository and Cargo workspace
2. Create the crate structure from Section 5, including the top-level `bqlite` re-export crate
3. Add stub `lib.rs` files with doc comments describing each crate's purpose
4. Set up the CLAUDE.md file per Section 8.1
5. Create the docs/ directory with initial content per Section 8.2 (architecture.md should contain the crate map and dependency rules; core-beliefs.md should contain Section 2 verbatim; quality-score.md should have the empty grading table; design/INDEX.md should list the deep-dive topics)
6. Create the .claude/skills/ directory with initial playbooks per Section 8.3 (using `.claude/skills/{skill-name}/SKILL.md` format)
7. Set up CI (GitHub Actions) per Section 8.4
8. Set up the .devcontainer/ per Section 9.2
9. Create the TASKS.md structure with Wave 0 tasks defined (see below), and placeholders for later waves
10. Create the Python package structure with a minimal `pyproject.toml` and stub `__init__.py`
11. Create a minimal test fixture directory structure under `tests/suite/`
12. Ensure `cargo build`, `cargo test`, and `cargo clippy` pass on the empty project
13. Make an initial commit

### 11.1 Wave 0 Tasks (Design Phase)

After bootstrapping, development proceeds with Wave 0 — an extensive design phase using a single Claude Code session with active human collaboration. Wave 0 produces the following design documents:

1. **docs/design/storage-format.md** — Native segment format, entity-major layout, segment metadata, compression strategy per column type, WAL design, memtable structure, flush triggers, compaction strategy (leveled vs tiered vs hybrid), segment merging, index structures (bloom filters, zone maps, bitmap indexes), concurrency between readers and compaction, database directory layout. Reference: Appendix A.2 and A.3.

2. **docs/design/query-language.md** — Complete BQL grammar specification, operator output schemas (exact column names and types for each operator), pipe composition rules, event type quoting syntax, property predicate syntax, variable binding syntax ($c for held properties), time literal syntax, aggregation function list, error message strategy. Reference: Section 4.

3. **docs/design/execution-model.md** — Pull-based iterator protocol, entity-aware batching interface, operator trait signatures, memory budget enforcement, spill-to-disk strategy, parallel execution model (entity partitioning across threads), merge-scan protocol for multi-segment reads, partial entity state handling.

4. **docs/design/sequence-matching.md** — NFA construction from patterns, time window enforcement, negation semantics, repetition quantifier semantics, held property variable binding, performance characteristics, relationship to funnel/retention/session operators.

5. **docs/design/type-system.md** — Supported data types (string, int, float, bool, timestamp, list, map), null handling, type coercion rules, schema declaration syntax, schema validation at plan construction time, Arrow type mapping.

Only after these design docs are complete and reviewed does multi-agent development begin with Wave 1.

---

## Appendix A: High-Performance Database Techniques — Research Reference

This appendix catalogs techniques, systems, and libraries that inform bqlite's design. It should be consulted during the Wave 0 design phase and referenced throughout development.

### A.1 Execution Engine Design

**Vectorized Execution (DuckDB model)**
DuckDB processes data in fixed-size vectors (default 2048 tuples) that fit in L1 cache. Operations run over entire vectors using tight loops that LLVM auto-vectorizes into SIMD instructions. This eliminates per-row function call overhead and maximizes CPU throughput. DuckDB uses a **push-based** model where DataChunks are pushed through the operator tree. Different vector representations (flat, constant, dictionary, sequence) allow compressed execution — e.g., if all values in a vector are constant, compute the result once and emit a constant vector.

*bqlite relevance*: Our stateless operators (filter, aggregate) should use vectorized execution over columnar batches. The vector size should be tuned to L1 cache (typically 1024-2048 elements). Our entity-at-a-time stateful operators are a different model — they benefit from sequential access patterns rather than SIMD parallelism.

**Morsel-Driven Parallelism (HyPer model)**
The HyPer paper introduced morsel-driven parallelism: work is divided into fixed-size "morsels" (e.g., 10K tuples). Worker threads pick morsels from a global queue, process them through a pipeline, and deposit results. Each thread maintains thread-local state for operators like hash aggregation. A final parallel combine step merges thread-local results. This avoids exchange-operator overhead and scales linearly with cores on NUMA architectures.

*bqlite relevance*: Entity-partitioned data naturally creates morsels — each entity (or group of entities) is a morsel. Worker threads can process independent entities in parallel with zero shared state. The merge step is only needed for global aggregation. This maps perfectly to our architecture.

**Compiled vs Vectorized (Kersten et al., VLDB 2018)**
The "Everything You Always Wanted to Know About Compiled and Vectorized Query Engines" paper found that vectorized and compiled approaches achieve similar performance when properly implemented. SIMD benefits are most pronounced when data fits in cache; for memory-bound operations (hash table probing, random access), SIMD provides minimal benefit. The paper suggests hybrid approaches: vectorized scans with compiled inner loops.

*bqlite relevance*: Our NFA-based sequence matcher is inherently branchy and stateful — it won't vectorize well via SIMD. Focus SIMD efforts on the scan/filter/aggregate path. Don't over-invest in SIMD for the temporal operators.

### A.2 Storage Engine Design

**ClickHouse MergeTree**
ClickHouse stores data in sorted parts (immutable directories). Each INSERT creates a new part; background merges consolidate parts. Key innovations: **sparse primary indexes** (one entry per 8192-row granule, fits entirely in RAM), **zone maps** (min/max per segment for pruning), **skipping indexes** (bloom filters, minmax, set indexes per granule block), and **aggressive compression** (LZ4 default, ZSTD for cold data, specialized codecs like Delta for timestamps, Gorilla for floats, T64 for integers, LowCardinality for low-cardinality strings). The sort key determines physical data order and dramatically improves both compression ratios and query performance.

*bqlite relevance*: Our entity-major sort order is analogous to ClickHouse's ORDER BY — it's the foundation for both compression and query performance. We should implement zone maps (min/max entity_id and timestamp per segment) for segment pruning, and bloom filters on entity keys for point lookups. The granule concept (groups of rows indexed together) maps to our segment design. ClickHouse's specialized codecs (Delta for timestamps, Dictionary for event types) are directly applicable.

**RocksDB LSM Compaction Strategies**
RocksDB implements three compaction families: **Leveled** (lower space amplification, higher write amplification — default), **Universal/Tiered** (lower write amplification, higher space amplification), and **Tiered+Leveled** hybrid (tiered for small levels, leveled for large). Key tradeoffs: leveled compaction has ~10x write amplification but 1.1x space amplification; tiered has ~2-4x write amplification but up to 2x space amplification. The hybrid approach used by default in RocksDB (L0 is tiered, rest is leveled) balances both.

*bqlite relevance*: Our workload is append-heavy (event ingestion) with rare deletes — this favors tiered compaction for lower write amplification. However, our query performance depends heavily on entity-locality, which compaction must restore. A tiered approach at smaller levels (fast ingest) transitioning to leveled at larger levels (entity-locality) seems right. The deep dive must quantify the compaction cost of re-sorting by entity vs. by timestamp.

**ScyllaDB Shard-Per-Core Architecture**
ScyllaDB (built on the Seastar C++ framework) pins one thread per CPU core, with each core owning its own shard of data, memory, network I/O, and storage I/O. Zero shared state between cores — all cross-shard communication is explicit message passing. This eliminates locks, context switches, and cache bounces, enabling linear scaling with core count and predictable latency. Memory is pre-allocated per-core, avoiding allocator contention.

*bqlite relevance*: While we're building an embeddable library (not a server), the shard-per-core principle applies to our parallel execution. Each worker thread should own its partition of entities with pre-allocated memory. No shared mutable state between threads during query execution. The pre-allocated memory model is relevant to our slab allocator consideration.

**Apache Druid Segment Design**
Druid stores data in immutable segments partitioned by time. Each segment contains: columnar storage per column, dictionary encoding for string dimensions, compressed bitmap indexes (Roaring) per dimension value for fast filtering, and pre-computed rollups for common aggregations. The bitmap indexes enable fast AND/OR filter composition. Druid uses both Roaring and Concise bitmap compression, with Roaring being faster for filters matching many values.

*bqlite relevance*: Bitmap indexes on event types could dramatically accelerate pattern matching — instead of scanning all events, compute the intersection of bitmaps for the event types in the pattern. Dictionary encoding for event types is a must. Roaring bitmaps are available in Rust (`roaring-rs`). Pre-computed rollups could cache common aggregations.

**Firebolt F3 Format and Index Types**
Firebolt uses three index types working together: **primary indexes** (sparse, determine physical sort order — same concept as ClickHouse's ORDER BY), **aggregating indexes** (pre-computed materializations of aggregate queries, automatically maintained during ingest — like auto-updating materialized views), and **join indexes** (cached join results for frequently joined dimension tables). The F3 (Firebolt File Format) stores data and indexes together in "tablets" that are automatically merged and optimized. Firebolt's query optimizer scans all available indexes at runtime and selects the best fit — including partial index matches where the index contains more columns than the query needs. Their key insight: aggressive data pruning via sparse indexes means queries scan a tiny fraction of the total data, enabling sub-second responses on hundreds of TB.

*bqlite relevance*: The aggregating index concept maps directly to our use case — common behavioral queries (daily funnel conversion, weekly retention) could be pre-computed and incrementally maintained as new events are ingested. The auto-maintenance during ingest is key — users define the aggregation once, and it stays current without manual rebuilds. This could be a powerful v2 feature: `CREATE AGGREGATING INDEX daily_funnel ON events AS funnel(signup, purchase) within 7d by user_id GROUP BY quantize(timestamp, 1d)`. The tablet-based storage with automatic merging is also analogous to our LSM segment compaction.

**Sneller — AVX-512 Vectorized SQL Engine**
Sneller is a SQL engine for JSON that achieves >1 GB/s/core scanning throughput by implementing its entire query VM in hand-written AVX-512 assembly (250+ core primitives). It uses a hybrid row/columnar format ("zion") where top-level fields are hashed into 16 buckets, each compressed separately — queries that don't touch all fields skip irrelevant buckets during decompression. Approximately half of query execution time is spent in decompression, the other half in the SQL engine itself.

*bqlite relevance*: Sneller's >1 GB/s/core target validates our aspiration. Their "bucketized" compression — where queries only decompress the columns they need — is directly applicable to our segment format. Their approach of hand-written SIMD assembly is extreme but demonstrates what's possible when you control the execution layer. For bqlite, leveraging LLVM auto-vectorization and libraries like FastLanes is more practical than hand-written assembly, but Sneller sets the performance bar.

**Redpanda — Thread-Per-Core for Streaming**
Redpanda (built on the Seastar framework like ScyllaDB) applies the shard-per-core model to event streaming. Each CPU core owns its partition data with dedicated I/O. Relevant to bqlite not as a query engine but as validation that the thread-per-core, shared-nothing model works for event data workloads at high throughput. Redpanda also uses the Raft consensus protocol per-partition, demonstrating that per-partition isolation enables linear scaling.

**Rockset — Converged Index (acquired by OpenAI, 2024)**
Rockset's key innovation was the "converged index" — simultaneously maintaining a column store, row store, and inverted index over the same data, choosing the optimal access path per query. For semi-structured data, it combined columnar scan performance with inverted index point-lookup speed. Rockset also built on RocksDB with a custom LSM compaction strategy tuned for their converged index workload.

*bqlite relevance*: The converged index idea is worth noting for the storage deep dive — maintaining both a columnar layout (for aggregate scans) and bitmap indexes (for point filters on event types/properties) over the same segment data is a form of converged indexing. Rockset's experience with RocksDB-based storage for analytical workloads is also informative for our LSM design choices.

### A.3 Compression Techniques

**FastLanes (Afroozeh & Boncz, PVLDB 2023)**
FastLanes is a compression layout for integers that achieves >100 billion integers/second decoding with scalar code (no hand-written SIMD). It interleaves values in a specific pattern that enables the compiler to auto-vectorize the decode loop. It's used in Vortex and is one of the fastest integer decompression schemes available.

*bqlite relevance*: Timestamp columns (stored as delta-encoded nanosecond integers) are a perfect fit for FastLanes. Entity ID columns (dictionary-encoded integers) would also benefit. Consider using the `vortex-fastlanes` Rust crate or implementing the layout directly.

**FSST (Boncz, Neumann & Leis, PVLDB 2020)**
Fast Static Symbol Table compression achieves fast random-access string compression. It builds a symbol table from a sample and compresses strings using 1-byte codes. Decompression is extremely fast (branch-free lookup table). Used in DuckDB and Vortex.

*bqlite relevance*: Event type names and string properties could be FSST-compressed for compact storage with fast decompression. Consider for string columns that don't have low enough cardinality for dictionary encoding.

**ALP (Afroozeh, Kuffo & Boncz, SIGMOD 2023)**
Adaptive Lossless floating-Point compression exploits the observation that most real-world floats have few significant digits. ALP encodes floats as scaled integers, achieving much better compression than generic algorithms.

*bqlite relevance*: Relevant if event properties include floating-point metrics (prices, latencies, scores). Not a day-one priority but worth adding for the native format.

**Delta + LZ4/ZSTD (ClickHouse pattern)**
ClickHouse chains codecs: apply Delta encoding first (converts monotonic sequences to small values), then compress with LZ4 (fast) or ZSTD (compact). This dramatically improves compression of timestamp columns.

*bqlite relevance*: Direct match for our timestamp columns. Delta-encode nanosecond timestamps, then compress with LZ4 for scan-heavy workloads or ZSTD for storage-heavy. This should be the default timestamp compression strategy.

**Roaring Bitmaps**
Roaring divides the integer space into 65536-element chunks. Each chunk uses the optimal representation: sorted array for sparse, bitmap for medium density, run-length encoding for dense. This provides fast set operations (AND, OR, NOT) with good compression. Used by ClickHouse, Druid, Lucene, and many others. Rust crate: `roaring-rs`.

*bqlite relevance*: Use for bitmap indexes on event types (which entity has which event type), for entity-level filter results (which entities matched this predicate), and potentially for tracking which segments contain which entities (bloom filter alternative).

### A.4 Format and Library Ecosystem

**Apache Arrow (arrow-rs)**
In-memory columnar format providing zero-copy interop across languages. Key features: RecordBatch as the data exchange unit, support for nested types, dictionary encoding, null bitmaps, and extensive compute kernels. The `arrow-rs` crate is the canonical Rust implementation.

*bqlite relevance*: Arrow RecordBatches are our query result format and internal data exchange format between operators. Use `arrow-rs` for in-memory representation, Parquet reading (for ingest), and Python interop via PyArrow.

**Vortex**
Next-generation columnar file format built in Rust, aspiring successor to Parquet. Claims 100-200x faster random access and 2-10x faster scans. Key innovations: cascading compression (nested encodings like FastLanes + Delta), compute on compressed data (filter pushdown without decompression), self-describing format with WASM decoders for forward compatibility. Built on Arrow, integrates with DataFusion.

*bqlite relevance*: Vortex's encoding architecture (FastLanes, FSST, ALP, BtrBlocks-style compression selection) is a goldmine of ideas for our native format. Consider using Vortex directly for the columnar storage within segments, or borrowing its encoding selection strategy (sample data, try encodings, pick the best). The `vortex` Rust crate is Apache 2.0 licensed.

**Parquet (parquet-rs)**
Columnar file format for bulk data. Row groups, column chunks, page-level encoding, predicate pushdown via min/max statistics. The `parquet` crate in `arrow-rs` provides reading/writing.

*bqlite relevance*: Parquet is an ingest format only — we read it during `INSERT`/`ingest`, not during queries. Use `parquet-rs` for the reader; our native format will be different (entity-major, not row-group-major).

**simdjson**
SIMD-accelerated JSON parser achieving 4x+ speedup over conventional parsers. Uses AVX2/AVX-512 for parallel character classification, structural indexing, and validation. Available in Rust as `simd-json`.

*bqlite relevance*: Use for JSON ingest path. The `simd-json` Rust crate provides the same performance characteristics as the C++ original. Also relevant if we support JSON-typed properties.

**MiMalloc**
Microsoft's high-performance memory allocator. Drop-in replacement for malloc with better multi-threaded performance. Vortex recommends it. Rust crate: `mimalloc`.

*bqlite relevance*: Use as the global allocator (`#[global_allocator] static GLOBAL_ALLOC: MiMalloc = MiMalloc;`). Simple win with no code changes required. Evaluate against jemalloc as well.

### A.5 Key Academic Papers

These papers should be read during the Wave 0 design phase:

1. **"Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age"** (Leis et al., SIGMOD 2014) — The parallelism model we should adopt.
2. **"MonetDB/X100: Hyper-Pipelining Query Execution"** (Boncz et al., CIDR 2005) — Foundation of vectorized execution.
3. **"Everything You Always Wanted to Know About Compiled and Vectorized Query Engines"** (Kersten et al., VLDB 2018) — Comparison of execution strategies, when SIMD helps vs doesn't.
4. **"BtrBlocks: Efficient Columnar Compression for Data Lakes"** (Kuschewski et al., SIGMOD 2023) — Adaptive compression selection strategy used by Vortex.
5. **"The FastLanes Compression Layout"** (Afroozeh & Boncz, PVLDB 2023) — State-of-the-art integer compression.
6. **"FSST: Fast Random Access String Compression"** (Boncz et al., PVLDB 2020) — Fast string compression.
7. **"Constructing and Analyzing the LSM Compaction Design Space"** (Sarkar et al., VLDB 2021) — Comprehensive analysis of compaction strategies and their tradeoffs.
8. **"Parsing Gigabytes of JSON per Second"** (Langdale & Lemire, VLDB Journal 2019) — SIMD-accelerated parsing techniques.

### A.6 Rust Crate Dependencies to Evaluate

| Crate | Purpose | Notes |
|-------|---------|-------|
| `arrow` / `arrow-rs` | In-memory columnar format, Parquet reader | Core dependency |
| `parquet` | Parquet file reading for ingest | Part of arrow-rs |
| `roaring` | Roaring bitmap indexes | Used by ClickHouse, Druid |
| `mimalloc` | High-performance allocator | Drop-in global allocator |
| `simd-json` | SIMD-accelerated JSON parsing | For JSON ingest |
| `regex` | Regular expression matching | For property predicates |
| `criterion` | Benchmarking | For performance tests |
| `thiserror` / `anyhow` | Error handling | Library vs CLI errors |
| `maturin` | Rust-Python binding build tool | For PyO3 bindings |
| `pyo3` | Python bindings | Core Python integration |
| `lz4` / `zstd` | Compression codecs | For segment compression |
| `memmap2` | Memory-mapped file I/O | For segment reading |
| `crossbeam` | Lock-free data structures | For parallel execution |
| `rayon` | Work-stealing parallelism | For entity-parallel execution |
| `vortex` | Columnar encodings (FastLanes, FSST, ALP) | Evaluate for native format |
| `pest` / `winnow` / `lalrpop` | Parser generators | For BQL parser |
| `comfy-table` | Pretty table output | For CLI table formatting |
| `clap` | CLI argument parsing | For bqlite CLI |
| `serde` / `serde_json` | Serialization | For metadata, JSON output |

### A.7 Systems to Benchmark Against

During development, benchmark bqlite against these systems on behavioral query workloads:

1. **DuckDB** — Express funnels/retention as SQL, compare query time
2. **ClickHouse** — Same SQL comparison, especially on large datasets
3. **Firebolt** — If accessible; their aggregating indexes are the closest existing concept to bqlite's pre-computed behavioral queries
4. **Polars** — DataFrame-based approach to the same queries
5. **Retentioneering** (Python) — The closest open-source behavioral analysis tool
6. **Raw pandas** — Baseline for "how most people do this today"

The goal is to demonstrate that bqlite is 10-100x faster than SQL-based approaches for behavioral queries, due to the combination of purpose-built operators and entity-major storage.
