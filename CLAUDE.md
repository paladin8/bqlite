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

## Dependency Conventions

- Prefer built-in std crates (e.g. `std::collections::BinaryHeap`) or popular, well-maintained crates over rolling custom low-level logic. Examples: `compact_str` for small-string optimization, `fastpfor` / `fastlanes` for integer compression, `sketches-ddsketch` for approximate quantiles.
- Before implementing a non-trivial algorithm or data structure from scratch, check whether a maintained crate already provides it. Custom implementations are justified only when no suitable crate exists or when integration constraints make a dependency impractical.

## Performance Conventions

- Preserve entity locality: storage, scan, and operator code must not silently break `(entity_id, timestamp)` ordering or split an entity across row groups / batches unless the design doc explicitly allows a streaming boundary for oversized entities.
- Treat Arrow `Utf8View` / `StringViewArray` as the canonical materialized string form. Do not round-trip through `StringArray` (`Utf8`) for convenience in hot paths.
- Preserve dictionary / compressed representations as far down the pipeline as practical. For low-cardinality string predicates, prefer code-based filtering or precomputed dictionary bitsets over per-row string comparisons.
- Avoid eager materialization in stateless operators. Prefer batch slices, views, selection vectors, and pre-sized builders; copying rows into fresh buffers should be an explicit, justified choice.
- Avoid per-row heap work in hot loops. Pre-size `Vec`s and Arrow builders, reuse scratch buffers across tiles/batches where possible, and keep working sets cache-friendly.
- Keep operator memory bounded by design. Any new buffering/stateful path must define its memory accounting and spill/overflow behavior up front rather than growing unboundedly.
- Follow the pipeline schema conventions from `docs/design/execution-model.md`: reference columns by name through `OperatorSchema`, keep timestamps as `Timestamp(Nanosecond, UTC)`, and preserve non-nullability guarantees for core columns.

## Testing And Benchmarking

- Hot-path changes should come with benchmark coverage or benchmark updates in `benches/`. If a change affects scan, filter, encoding, ingest, merge, or sequence-matching performance, measure it.
- Use property tests for components with large input spaces and clear invariants: codecs, planner rewrites, merge/order guarantees, compaction, and sequence evaluation. Example tests alone are not enough for these surfaces; `docs/core-beliefs.md` §11 is the default bar, and `tests/src/strategies.rs` is the canonical source of Arrow-shaped generators to reuse.

## Documentation

- [Architecture](docs/architecture.md) — crate map, dependency direction, data flow
- [Core Beliefs](docs/core-beliefs.md) — design principles governing all decisions
- [Quality Score](docs/quality-score.md) — per-crate quality grades
- [Reliability](docs/reliability.md) — operational requirements
- [Design Specs](docs/design/INDEX.md) — per-feature design documents

## Task Coordination

See [TASKS.md](TASKS.md) for the development roadmap and task assignments.

## Agent Coordination

See [AGENTS.md](AGENTS.md) for the autonomous agent operating protocol (single-task execution, checkpoint discipline, git workflow). Fleet infrastructure and architecture details live in [scripts/agents/README.md](scripts/agents/README.md).
