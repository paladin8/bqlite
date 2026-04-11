# bqlite

An embeddable, high-performance behavioral query engine for temporal event-sequence analysis — funnels, retention, cohorts, and pattern matching over entity event streams. Built in Rust on Apache Arrow, with a purpose-built query language (BQL) that makes sequence-oriented questions a first-class citizen.

> **Status:** early development. Interfaces, file formats, and the BQL surface are all subject to change.

## What it does

Traditional analytics engines treat events as rows in a wide table and make you hand-roll window functions and self-joins to answer questions about *order* and *timing*. bqlite is the opposite: it stores data entity-sorted and column-oriented, and its execution model is built around operators that traverse per-entity event streams in time order. That makes temporal queries — "users who did A then B then C within 7 days" — cheap and composable instead of costly and awkward.

## Crate layout

| Crate | Purpose |
|-------|---------|
| `bqlite` | Top-level re-export crate that users depend on |
| `bqlite-core` | Event, Entity, Schema, Timestamp, PropertyValue |
| `bqlite-ast` | AST shared by parser and builder APIs |
| `bqlite-storage` | Native columnar format, ingest, compaction |
| `bqlite-parser` | BQL text → AST |
| `bqlite-planner` | AST → logical plan → optimizer → physical plan |
| `bqlite-operators` | Physical operator implementations |
| `bqlite-engine` | Execution orchestration, memory, spill-to-disk |
| `bqlite-cli` | Command-line interface |
| `bqlite-ffi` | C ABI for PyO3 Python bindings |

Full dependency direction and data flow live in [docs/architecture.md](docs/architecture.md). Design principles are in [docs/core-beliefs.md](docs/core-beliefs.md).

## Build

```bash
cargo build                                               # build all crates
cargo test                                                # run all tests
cargo clippy --all-targets --all-features -- -D warnings  # lint
cargo bench                                               # benchmarks
cargo fmt --check                                         # formatting
```

---

## Multi-agent development workflow

bqlite is being developed in parallel by a fleet of autonomous Claude Code agents, each running in its own Docker container. The tooling, quick-start guide, and architecture details live in [scripts/agents/README.md](scripts/agents/README.md). The behavioral protocol each agent follows is in [AGENTS.md](AGENTS.md).
