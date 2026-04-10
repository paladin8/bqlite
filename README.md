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

bqlite is being developed in parallel by a fleet of autonomous Claude Code agents, each running in its own Docker container against its own clone of the repo. Agents coordinate entirely through git — they claim tasks with lock files, work on `task/TASK-NNN` branches, and fast-forward merge to `main` at every checkpoint. A human watches all of them at once through a [cmux](https://github.com/cmux-sh/cmux) workspace with one tab per agent and can interrupt or redirect any agent at any time.

The full design lives in [docs/design/agent-workflow.md](docs/design/agent-workflow.md). The behavioral protocol each agent runs is in [AGENTS.md](AGENTS.md). This section is a practical getting-started guide.

### How it works

```
Host (macOS)
├── scripts/launch-fleet.sh    ← builds image, starts N containers
├── scripts/attach-fleet.sh    ← opens cmux with one tab per agent
├── scripts/status.sh          ← prints task-claim state from git
├── scripts/stop-fleet.sh      ← tears down the fleet
│
├── bqlite-agent-1 … bqlite-agent-N   (Docker containers)
│     each runs: git clone + `claude` driven by AGENTS.md
│
└── cmux workspace
      Tab: agent-1   Tab: agent-2   …   Tab: agent-N
```

Container lifecycle and Claude Code sessions are deliberately separated: containers survive cmux closing, and a crashed Claude session doesn't take its container down with it. You can reattach, restart individual agents, or scale the fleet without disturbing in-flight work.

### Prerequisites

- Docker Desktop (with enough RAM — 4 agents fit comfortably in 32 GB; 8 agents effectively want 64 GB)
- A running SSH agent with a key authorized to push to the repo (`ssh-add -l` should list a key)
- [cmux](https://github.com/cmux-sh/cmux) installed on the host
- A Claude Max subscription auth state in `~/.claude/` (mounted read-only into each container)

### Launching the fleet

```bash
# 1. Build the devcontainer image (cached) and start N idle containers
scripts/launch-fleet.sh 4

# 2. Open a cmux workspace with one tab per running container;
#    each tab docker-execs into its container and starts Claude Code
#    with a system prompt that points it at AGENTS.md.
scripts/attach-fleet.sh
```

At that point every agent enters its loop: pull `main`, find an unclaimed task whose dependencies are satisfied, claim it via a lock file, implement it in small checkpoints, and merge each checkpoint to `main`. You can scroll through the cmux tabs to watch progress or talk to any agent directly.

### Monitoring and control

```bash
scripts/status.sh       # who's working on what, what's done
scripts/stop-fleet.sh   # stop and remove all bqlite-agent-* containers
```

`status.sh` does a shallow clone into a tempdir and reads `tasks/active/` (lock files) and `tasks/completed/` (done markers) — the same files agents use to coordinate — so it always reflects the canonical state in git.

### How agents coordinate

- **Task definitions** live in [TASKS.md](TASKS.md) (human-authored, agents read-only).
- **Claiming** is an atomic push: an agent creates `tasks/active/TASK-NNN.lock` and tries to push. Git rejects concurrent pushes to the same ref, so at most one agent wins any race.
- **Branches** are named `task/TASK-NNN`. Every commit message is prefixed with the task ID.
- **Checkpoints** are the unit of progress. Each one must `cargo build`, `cargo test`, and pass `cargo clippy -- -D warnings`, and is fast-forward-merged to `main` immediately — agents do not accumulate work locally.
- **Completion** moves the lock file to `tasks/completed/TASK-NNN.done` with a `completed_at` timestamp.
- **Stale locks** (crashed agents) are detected after 45 minutes of no activity on the task branch or `main` and can be broken by any other agent.

### When agents ask for help

Agents are told to stop and ask rather than guess on architecture decisions, ambiguous task criteria, or messy merge conflicts. They prefix blocking messages with `[NEEDS INPUT]` so you can scan the cmux tabs and see which agents are waiting. Responding in that tab unblocks the agent directly.

### Adjusting agent behavior

Two files shape what agents do:

1. **[CLAUDE.md](CLAUDE.md)** — project quick reference, auto-loaded by Claude Code in every session (human or agent).
2. **[AGENTS.md](AGENTS.md)** — the autonomous operating protocol: the loop, claiming, checkpoint discipline, git conventions, behavioral requirements.

Edit AGENTS.md to change how agents operate across the board. Detailed per-subsystem guidance lives in `docs/design/` — agents are expected to read the relevant design doc before touching a task.
