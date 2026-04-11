# bqlite

Embeddable behavioral query engine — temporal pattern matching, funnels, retention, and cohort analysis over entity event streams, powered by Rust and Apache Arrow.

## Crate Map

| Crate | Purpose |
|-------|---------|
| `bqlite` | Top-level re-export crate (what users depend on) |
| `bqlite-core` | Core types: Event, Entity, Schema, Timestamp, PropertyValue |
| `bqlite-ast` | AST types shared by parser and builder APIs |
| `bqlite-storage` | Native storage format, compaction, ingest, entity-sorted columnar layout |
| `bqlite-parser` | BQL text → AST |
| `bqlite-planner` | AST → logical plan → optimizer → physical plan |
| `bqlite-operators` | Physical operator implementations (scan, filter, sequence, funnel, etc.) |
| `bqlite-engine` | Execution orchestration, memory management, spill-to-disk |
| `bqlite-cli` | Command-line interface |
| `bqlite-ffi` | C ABI surface for PyO3 Python bindings |

## Dependency Direction

```
bqlite-core          (no internal deps)
bqlite-ast           → core
bqlite-storage       → core
bqlite-parser        → ast
bqlite-planner       → ast, core
bqlite-operators     → core, storage, planner  (+ ast test-only)
bqlite-engine        → parser, planner, operators, storage, core
bqlite-cli           → engine
bqlite-ffi           → engine
bqlite (top-level)   → core, ast, parser, engine
```

A crate may only depend on crates above it in this ordering. Violations block merges.

## Build Commands

```bash
cargo build                                               # build all crates
cargo test                                                # run all tests
cargo clippy --all-targets --all-features -- -D warnings  # lint (must be clean)
cargo bench                                               # run benchmarks
cargo fmt --check                                         # formatting check
```

## Coding Conventions

- Rust 2021 edition
- `thiserror` for library error types, `anyhow` for CLI/test errors
- Idiomatic Rust: prefer iterators, zero-copy, stack over heap
- Every operator respects configurable memory budgets
- All errors must be typed and recoverable

## Documentation

- [Architecture](docs/architecture.md) — crate map, dependency direction, data flow
- [Core Beliefs](docs/core-beliefs.md) — design principles governing all decisions
- [Quality Score](docs/quality-score.md) — per-crate quality grades
- [Reliability](docs/reliability.md) — operational requirements
- [Design Specs](docs/design/INDEX.md) — per-feature design documents

## Task Coordination

See [TASKS.md](TASKS.md) for the development roadmap and task assignments.

## Agent Coordination

See [AGENTS.md](AGENTS.md) for the autonomous agent operating protocol (task claiming, checkpoints, git workflow).
