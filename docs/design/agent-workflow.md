# Agent Workflow Design

> **Status**: DRAFT
> **Date**: 2026-04-09

Design for running 4-8 concurrent Claude Code agents in Docker containers to develop bqlite in parallel. Covers container lifecycle, task coordination, git workflow, and agent behavioral guidance.

---

## 1. Architecture Overview

```
Host (macOS)
├── scripts/launch-fleet.sh    ← starts N Docker containers
├── scripts/attach-fleet.sh    ← opens cmux with tabs into each agent
├── scripts/status.sh          ← reads git to show task assignments
│
├── bqlite-agent-1 (Docker container)
│   ├── git clone of paladin8/bqlite
│   └── claude (Claude Code session, driven by AGENTS.md)
├── bqlite-agent-2
│   └── ...
├── ...up to bqlite-agent-8
│
└── cmux (terminal multiplexer)
    ├── Tab: agent-1 (interactive Claude Code session)
    ├── Tab: agent-2
    └── ...
```

Each agent runs in its own Docker container with a fresh clone of the repository. Agents coordinate through git — lock files for task claiming, branches for work-in-progress, fast-forward merges to main. The human monitors all agents via cmux tabs and can interact with any agent at any time.

---

## 2. Container Lifecycle

### 2.1 Two-Phase Launch

**Phase 1: `scripts/launch-fleet.sh N`**
- Builds the devcontainer Docker image (once, cached thereafter)
- Starts N detached containers named `bqlite-agent-1` through `bqlite-agent-N`
- Each container clones the repo, configures git identity, then idles
- Idempotent: skips containers that are already running

**Phase 2: `scripts/attach-fleet.sh`**
- Uses cmux CLI to create a workspace with one tab per running container
- Each tab runs `docker exec -it -w /workspace bqlite-agent-N claude --system-prompt ...`
- Claude Code starts, reads CLAUDE.md automatically, and the system prompt directs it to read AGENTS.md and begin the autonomous loop

Separating container lifecycle from Claude Code sessions means:
- Containers survive if cmux closes — reattach later with `attach-fleet.sh`
- If a Claude Code session crashes, the container stays alive — restart claude in that tab
- You can stop and restart individual agents without affecting others

### 2.2 Devcontainer Image

The `.devcontainer/Dockerfile` provides the full build environment:

```dockerfile
FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

RUN apt-get update && apt-get install -y \
    cmake \
    git \
    openssh-client \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code
RUN curl -fsSL https://claude.ai/install.sh | bash

# Pre-warm cargo registry
RUN cargo search --limit 1 serde

# Python tooling for later waves
RUN pip install maturin pytest
```

### 2.3 Volume Mounts

| Host path | Container path | Mode | Purpose |
|---|---|---|---|
| `~/.claude/` | `/home/vscode/.claude/` | `ro` | Max subscription auth tokens |
| `$SSH_AUTH_SOCK` | `/ssh-agent` | — | SSH agent forwarding for git push |

Auth tokens are mounted read-only. Claude Code needs to write session state to `~/.claude/` during operation. Since the mount is read-only, the launch script creates a writable overlay: it copies auth-relevant files from the read-only mount into a container-local `~/.claude/` directory at startup, giving each agent its own writable copy of the auth state while keeping the host directory untouched.

Git push uses SSH agent forwarding so the container can push via the host's SSH key without copying any secrets into the container.

### 2.4 Container Startup (Inline Entrypoint)

The launch script passes setup commands inline to `docker run` (no separate entrypoint script needed):

```bash
docker run -d --name "$NAME" \
  -e AGENT_ID="agent-$i" \
  -v "$HOME/.claude:/home/vscode/.claude:ro" \
  -v "$SSH_AUTH_SOCK:/ssh-agent" \
  -e SSH_AUTH_SOCK=/ssh-agent \
  -w /workspace \
  "$IMAGE" \
  bash -c "
    git clone git@github.com:paladin8/bqlite.git /workspace &&
    cd /workspace &&
    git config user.name \"bqlite-agent-$i\" &&
    git config user.email \"bqlite-agent-$i@agent.local\" &&
    exec tail -f /dev/null
  "
```

Note: double quotes so `$i` expands on the host at launch time. The repo URL is hardcoded rather than passed as an env var to avoid quoting issues.

---

## 3. Git Workflow

### 3.1 Individual Clones

Each container gets its own fresh `git clone`. No shared working directories, no worktrees across container boundaries. This provides complete isolation — one agent's uncommitted state never affects another.

### 3.2 Per-Task Branches

Agents work on branches named `task/TASK-NNN`. This provides:
- Visibility into in-flight work (`git branch -r` shows all active task branches)
- Easy rollback (delete a branch to undo a bad task)
- Clean separation between agents' uncommitted work

### 3.3 Fast-Forward Merge to Main

After each checkpoint, agents merge their branch to main:

```
1. git checkout main
2. git pull origin main
3. git merge task/TASK-NNN --ff-only
4. git push origin main
5. If push fails: git pull --rebase origin main && git push origin main
6. If merge --ff-only fails: git checkout task/TASK-NNN && git rebase main, then retry from step 1
```

If rebase conflicts are too complex to resolve cleanly, the agent abandons its local work and restarts the task on fresh main. Simple rule: never force-push, never create merge commits.

### 3.4 Commit Convention

All commits reference the task ID:

```
TASK-042: Add hash aggregate operator stub and module registration
TASK-042: Implement count/sum/avg aggregation functions
TASK-042: Add test fixtures for aggregate edge cases
```

### 3.5 Checkpoint Discipline

Agents break every task into the smallest self-contained units of progress. Each checkpoint must:

1. Compile cleanly (`cargo build`)
2. Pass all tests (`cargo test`)
3. Pass clippy (`cargo clippy --all-targets --all-features -- -D warnings`)
4. Be merged to main immediately — do not accumulate checkpoints

**Shared file priority**: Changes to shared files (Cargo.toml, `lib.rs` module declarations, trait definitions in bqlite-core) should be their own checkpoint, merged before dependent work. This minimizes the window where other agents' pulls are stale.

**Ideal checkpoint**: One that only adds new files (a new module, a new test fixture directory). These have zero conflict risk.

---

## 4. Task Coordination

### 4.1 Directory Structure

```
tasks/
├── active/           # Lock files for in-progress tasks
│   ├── .gitkeep
│   └── TASK-042.lock
└── completed/        # Completion markers for finished tasks
    ├── .gitkeep
    └── TASK-005.done
```

TASKS.md remains the task definition file (human-authored, agents read-only). Lock files and done markers are the coordination mechanism.

### 4.2 Lock File Format

`tasks/active/TASK-042.lock`:

```json
{
  "agent_id": "agent-3",
  "task_id": "TASK-042",
  "claimed_at": "2026-04-09T15:30:00Z",
  "branch": "task/TASK-042",
  "description": "Implement hash aggregate operator"
}
```

### 4.3 Claiming Protocol

1. `git pull origin main`
2. Scan `tasks/active/` for existing lock files and `tasks/completed/` for done markers
3. Read TASKS.md, select an unclaimed task whose `Depends on` tasks all have corresponding `.done` markers in `tasks/completed/`
4. Create `tasks/active/TASK-042.lock` with the agent's metadata
5. `git add tasks/active/TASK-042.lock && git commit -m "TASK-042: claimed by agent-3" && git push origin main`
6. **If push fails**: another agent committed concurrently. `git reset HEAD~1 && git checkout -- tasks/`, pull, go back to step 2 and pick a different task

The push-to-main is the atomic operation. Git rejects concurrent pushes to the same ref, so only one agent's claim succeeds for any race condition.

### 4.4 Completion Protocol

When a task's final checkpoint is merged:

1. `git mv tasks/active/TASK-042.lock tasks/completed/TASK-042.done`
2. Enrich the `.done` file with completion metadata:
   ```json
   {
     "agent_id": "agent-3",
     "task_id": "TASK-042",
     "claimed_at": "2026-04-09T15:30:00Z",
     "completed_at": "2026-04-09T16:45:00Z",
     "branch": "task/TASK-042",
     "description": "Implement hash aggregate operator"
   }
   ```
3. `git commit -m "TASK-042: completed" && git push origin main`

### 4.5 Stale Lock Detection

If an agent crashes, its lock file persists. Other agents detect stale locks:

1. For each file in `tasks/active/`, read `claimed_at` from the lock file
2. Check the task branch for recent activity: `git log -1 origin/task/TASK-NNN --format=%cI`
3. A lock is stale if **all** of the following are true:
   - `claimed_at` is older than 45 minutes
   - The task branch either doesn't exist OR has no commits in the last 45 minutes
   - No commits on main reference the task ID in the last 45 minutes (catches agents that are actively merging checkpoints without updating the branch)
4. To break a stale lock: remove the lock file, commit, and push (same atomic push protocol — if two agents race to break the same stale lock, only one succeeds)

The 45-minute timeout is defined as a constant `STALE_LOCK_TIMEOUT_MINUTES` at the top of AGENTS.md so it can be tuned.

### 4.6 Why Not Update TASKS.md Directly

Multiple agents writing to TASKS.md would cause constant merge conflicts (all agents editing the same file). Lock files in separate directories avoid this entirely — each agent creates/removes its own unique file, so commits never conflict unless two agents claim the same task (handled by the atomic push).

The `scripts/status.sh` script reads lock and done files to produce a human-readable status report, making TASKS.md modifications unnecessary.

---

## 5. Agent Guidance

Three layers with distinct responsibilities:

### 5.1 CLAUDE.md (Existing — Minor Update)

Already serves as the project quick reference. Add one line pointing to AGENTS.md:

```markdown
## Agent Coordination

- [AGENTS.md](AGENTS.md) — Operating protocol for autonomous agent sessions
```

This is all. CLAUDE.md stays focused on project reference, not agent behavior.

### 5.2 AGENTS.md (New — Checked Into Repo)

The complete operating protocol for autonomous agents. Contains:

**Identity and environment**: Agent ID from `$AGENT_ID`, working directory is `/workspace`.

**The loop**: Pull → scan tasks → claim → implement → checkpoint → merge → repeat. Full step-by-step protocol with exact git commands.

**Task claiming protocol**: The lock file mechanism from Section 4, written as agent-executable instructions.

**Checkpoint discipline**: Break tasks into small units. Merge after every checkpoint. Shared files get their own checkpoint. Each checkpoint must compile, test, and lint clean.

**Git workflow**: Branch naming, commit messages, merge protocol, conflict resolution strategy.

**Behavioral requirements** (distilled from BOOTSTRAP.md §9.4):
- Flag architecture/design decisions for human review rather than deciding unilaterally
- Document every non-trivial decision before committing
- Write tests for every code path including edge cases
- Run code review via subagent for complex changes
- Always run `cargo test` + `cargo clippy` before committing
- Consider refactoring opportunities after completing a task
- Performance-first mindset: zero-copy, iterators, cache-friendly patterns

**When to ask for human input**: Architecture decisions, ambiguous acceptance criteria, complex merge conflicts. Since agents run in interactive cmux tabs, the agent simply prints its question and waits — the human sees it in the cmux tab and responds directly. For visibility, agents should prefix blocking questions with `[NEEDS INPUT]` so the human can scan tabs quickly.

### 5.3 System Prompt (CLI Flag at Launch)

The `attach-fleet.sh` script starts Claude Code with:

```bash
claude --system-prompt "You are $AGENT_ID, an autonomous agent building bqlite. \
Read AGENTS.md for your complete operating protocol. Begin the agent loop now."
```

This is the minimal trigger that separates agent sessions from human sessions. CLAUDE.md loads automatically. The system prompt points to AGENTS.md. AGENTS.md contains the full protocol.

### 5.4 Skills (Existing — Extend Per Wave)

Existing skills (`implement-operator`, `add-parser-production`, `add-test-fixture`, `fix-ci`) provide task-type-specific checklists. AGENTS.md instructs agents to check `.claude/skills/` for applicable playbooks before starting a task.

New skills can be added per wave. For Wave 0, a `design-deep-dive` skill could encode the design document authoring process.

---

## 6. Host-Side Scripts

### 6.1 `scripts/launch-fleet.sh N`

Builds the devcontainer image and starts N containers. Idempotent — skips already-running containers. Validates SSH agent is running before starting.

### 6.2 `scripts/attach-fleet.sh`

Creates a cmux workspace with one tab per running agent container. Each tab `docker exec`s into the container and starts Claude Code with the system prompt. Uses cmux CLI (`new-workspace`, `new-surface`, `send`, `send-key`).

### 6.3 `scripts/stop-fleet.sh`

Stops and removes all `bqlite-agent-*` containers.

### 6.4 `scripts/status.sh`

Clones the repo (shallow, into a temp directory) and reads `tasks/active/` and `tasks/completed/` to print a summary of task assignments and completions. Cleans up the temp directory on exit.

---

## 7. Practical Considerations

### 7.1 Max Subscription Rate Limits

Running 4-8 concurrent Claude Code sessions on one Max subscription may hit rate limits. Mitigation:
- Start with 4 agents, scale up based on observed limits
- Claude Code handles 429 responses with automatic retry/backoff
- AGENTS.md notes that rate limit pauses are expected, not errors
- Agents should not interpret slow responses as failures

### 7.2 Resource Usage

Each container runs a full Rust toolchain. On a Mac with 32GB+ RAM:
- 4 agents: comfortable
- 6 agents: workable if builds aren't simultaneous
- 8 agents: likely needs 64GB RAM or agents will swap during cargo builds

`cargo build` is the bottleneck — it's CPU and memory intensive. Agents naturally stagger their builds (they're at different points in their loops), which helps.

### 7.3 Conflict Patterns by Wave

| Wave | Conflict risk | Why |
|---|---|---|
| Wave 0 (Design) | None | Single-session, human-driven |
| Wave 1 (Foundation) | Medium | Shared types, Cargo.toml changes |
| Wave 2-3 (Storage, Parser) | Low-Medium | Different crates, some shared interfaces |
| Wave 4 (Operators) | Low | Each operator is a separate file |
| Wave 5-7 (Engine, CLI, Polish) | Low | Largely independent modules |

Wave 1 benefits most from checkpoint discipline. After Wave 1 establishes shared interfaces, later waves have minimal conflicts.

### 7.4 Agent Crash Recovery

If a container dies:
1. Its task's lock file persists in git
2. Other agents detect the stale lock after 45 minutes
3. The stale lock gets broken and the task becomes reclaimable
4. The human can also restart the specific container: `scripts/launch-fleet.sh` re-creates missing containers, `scripts/attach-fleet.sh` reconnects cmux

Any work the crashed agent pushed to its task branch is preserved in git. The recovering agent (or a new one) can pick up from the branch.

---

## 8. File Inventory

Files to create or modify:

| File | Action | Purpose |
|---|---|---|
| `AGENTS.md` | Create | Agent operating protocol |
| `CLAUDE.md` | Modify | Add pointer to AGENTS.md |
| `tasks/active/.gitkeep` | Create | Lock file directory |
| `tasks/completed/.gitkeep` | Create | Completion marker directory |
| `.devcontainer/Dockerfile` | Modify | Add Claude Code, jq, ssh tooling |
| `scripts/launch-fleet.sh` | Create | Start N agent containers |
| `scripts/attach-fleet.sh` | Create | cmux tabs into fleet |
| `scripts/stop-fleet.sh` | Create | Tear down fleet |
| `scripts/status.sh` | Create | Show task assignment state |
