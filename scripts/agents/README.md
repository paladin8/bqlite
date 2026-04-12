# bqlite agent fleet

Tooling for running a fleet of autonomous Claude Code agents that build bqlite in parallel. Each agent runs in its own Docker container against its own clone of the repo. Agents coordinate entirely through git — they claim tasks with lock files, work on `task/TASK-NNN` branches, and fast-forward merge to `main` at every checkpoint. A human watches all of them at once through a [cmux](https://github.com/cmux-sh/cmux) workspace with one tab per agent and can interrupt or redirect any agent at any time.

The behavioral protocol each agent runs is in [AGENTS.md](../../AGENTS.md) at the repo root.

---

## Quick start

### Prerequisites

- Docker Desktop (enough RAM — 4 agents fit comfortably in 32 GB; 8 agents effectively want 64 GB)
- A running SSH agent with a key authorized to push to the repo (`ssh-add -l` should list a key)
- [cmux](https://github.com/cmux-sh/cmux) installed on the host
- A Claude Max subscription auth state in `~/.claude/` on the host (copied into each container at launch)
- A long-lived `CLAUDE_CODE_OAUTH_TOKEN` exported in the shell that runs `launch-fleet.sh` — on macOS the host credential lives in the Keychain and the copy into the container does not carry it, so agents auth via the env var. Generate one with `claude setup-token`.

### Launching the fleet

Run from the repo root:

```bash
# 1. Build the devcontainer image (cached after first run) and start N idle containers.
scripts/agents/launch-fleet.sh 4

# 2. Open a cmux workspace with one tab per container.
#    -c, --count N  = attach the first N running containers
#    -w N           = wave number; agents only claim tasks in TASK-N00..TASK-N99
#    -n N           = optional per-agent task quota; agent exits cleanly after
#                     N *successful* tasks (failed/released tasks do not count,
#                     but see the failure cap under "When tasks keep failing")
scripts/agents/attach-fleet.sh -w 4 -c 4
scripts/agents/attach-fleet.sh -w 4 -c 2 -n 2       # each agent stops after 2 successes
```

Each cmux tab execs into its container and runs `agent_wrapper.py`, which then claims tasks and invokes `claude` one task at a time. You can scroll through the cmux tabs to watch progress or talk to any agent directly.

### Monitoring and control

```bash
scripts/agents/status.sh       # who's working on what, what's done
scripts/agents/stop-fleet.sh   # stop and remove all bqlite-agent-* containers
```

`status.sh` does a shallow clone into a tempdir and reads `tasks/active/` (lock files) and `tasks/completed/` (done markers) — the same files agents use to coordinate — so it always reflects the canonical state in git.

Container lifecycle and Claude Code sessions are deliberately separated: containers survive cmux closing, and a crashed Claude session doesn't take its container down with it. You can reattach, restart individual agents, or scale the fleet without disturbing in-flight work.

### When agents ask for help

Agents are told to stop and ask rather than guess on architecture decisions, ambiguous task criteria, or messy merge conflicts. An agent emits `[NEEDS INPUT]` followed by its question as the last line of its turn and then exits. The wrapper sees the marker in captured stdout, prints a `>` prompt in the cmux tab, reads one line from the TTY, and resumes the same claude session with that reply as the next user message. Give specific, scoped answers — a one-line reply may not be enough context for an open-ended question.

---

## Architecture

```
Host (macOS)
├── scripts/agents/launch-fleet.sh    ← builds image, starts N idle containers
├── scripts/agents/attach-fleet.sh    ← opens cmux with one tab per agent
├── scripts/agents/status.sh          ← prints task-claim state from git
├── scripts/agents/stop-fleet.sh      ← tears down the fleet
│
├── bqlite-agent-1 … bqlite-agent-N   (Docker containers)
│     each runs: git clone + agent_wrapper.py in a cmux tab
│
└── cmux workspace
      Tab: agent-1   Tab: agent-2   …   Tab: agent-N
```

### Two-phase launch

**Phase 1: `launch-fleet.sh N`** builds the devcontainer image (cached thereafter), starts N detached containers named `bqlite-agent-1` … `bqlite-agent-N`, and lets each one clone the repo, install Claude Code plugins, and wait idle. Idempotent: skips containers that are already running.

**Phase 2: `attach-fleet.sh -w WAVE [...]`** uses the cmux CLI to create a workspace with one tab per running container. Each tab `docker exec`s into its container and runs `python3 /workspace/scripts/agents/agent_wrapper.py WAVE POOL [MAX_TASKS]`.

Separating container lifecycle from Claude Code sessions means:
- Containers survive if cmux closes — reattach later with `attach-fleet.sh`
- If a Claude Code session crashes, the container stays alive — restart the tab
- You can stop and restart individual agents without affecting others

### Devcontainer image

The `.devcontainer/Dockerfile` at the repo root provides the build environment: Rust toolchain, cmake, jq, openssh-client, Claude Code CLI, Python tooling (maturin, pytest). `launch-fleet.sh` builds it with Docker's layer cache so first run is slow, subsequent runs are instant.

### Volume mounts

| Host path | Container path | Mode | Purpose |
|---|---|---|---|
| `~/.claude/` | `/home/vscode/.claude-host/` | `ro` | Max subscription auth state, copied into a writable `/root/.claude/` at startup |
| `/run/host-services/ssh-auth.sock` | `/ssh-agent` | — | SSH agent forwarding so the container can push via the host's key without copying secrets |

Auth is read-only on the mount; the container copies auth files into `/root/.claude/` at startup so claude can write session state there. Plugin state is deliberately excluded from the copy because host paths (`/Users/<host>/.claude/plugins/…`) don't resolve inside the container; plugins are freshly installed in-container against container-local paths.

### Wrapper / claude control-plane split

Each agent container runs `scripts/agents/agent_wrapper.py`, which owns the fleet loop. The wrapper:

1. Claims one task from the agent's wave via `scripts/agents/task_tool.py` — any tagged, dep-satisfied task is eligible regardless of difficulty
2. Picks the claude model based on the claimed task's `[EASY]` / `[HARD]` tag (Sonnet for EASY, Opus for HARD)
3. Invokes `claude -p --verbose --output-format stream-json` once, with a prompt naming the single task to implement
4. Inspects git state after claude exits:
   - `tasks/completed/<id>.done` on origin/main → task succeeded, increment quota, loop
   - Lock still held → retry once with `claude -p -c "resume and finish …"`
   - Still incomplete after retry → release the lock so another agent can pick it up, count as a consecutive failure
5. Parses the streamed JSON events live into a human-readable trace (tool calls, tool results, assistant text), detects `[NEEDS INPUT]` in assistant text, reads one line of human reply from the cmux tab's TTY, and resumes the same session with `claude -p -c "<reply>"`
6. Tracks the batch quota (`-n` flag from `attach-fleet.sh`) and exits cleanly when hit
7. Exits when the wave is drained (no claimable, blocked, or active work left)

There is no Stop hook. `claude -p` exits naturally when the model's turn is done; the wrapper decides whether to launch another session. The marker protocol that earlier versions used (`[WAVE COMPLETE]`, `[END LOOP]`, `[BATCH COMPLETE]`) has been removed — only `[NEEDS INPUT]` survives, detected by substring scan of captured stdout.

This split keeps the reasoning claude has to do scoped to one task at a time and keeps the batching/restart/retry/backoff logic in plain Python where it can be unit-tested.

### Task difficulty tags

Every task in `TASKS.md` is tagged either `[EASY]` or `[HARD]`. The wrapper picks the claude model **per task**, not per container, so any agent can claim any tagged task in its wave:

| Tag | Model | Effort |
|---|---|---|
| `[EASY]` | `claude-sonnet-4-6` | `high` |
| `[HARD]` | `claude-opus-4-6[1m]` | `high` |

There is no container-level pool assignment — all running wrappers draw from the same claimable queue. This keeps the fleet well-utilized even when the wave is unbalanced (e.g. mostly HARD tasks with a single EASY trailing). It also means there is no implicit cap on concurrent Opus runs: if the next N claimable tasks are all `[HARD]`, every agent will pick Opus at once. For a Max subscription this is fine (CC backs off on 429s); for per-token billing, factor that in before sizing the fleet.

---

## Git workflow

### Individual clones

Each container gets its own fresh `git clone`. No shared working directories, no worktrees across container boundaries. This provides complete isolation — one agent's uncommitted state never affects another.

### Per-task branches

Agents work on branches named `task/TASK-NNN`. This provides:
- Visibility into in-flight work (`git branch -r` shows all active task branches)
- Easy rollback (delete a branch to undo a bad task)
- Clean separation between agents' uncommitted work

### Fast-forward merge to main

After each checkpoint, agents merge their branch to main:

```
1. git checkout main
2. git pull origin main
3. git merge task/TASK-NNN --ff-only
4. git push origin main
5. If push fails:            git pull --rebase origin main && git push origin main
6. If --ff-only fails:       git checkout task/TASK-NNN && git rebase main, then retry from 1
```

Simple rule: never force-push, never create merge commits.

### Commit convention

All commits reference the task ID:

```
TASK-042: Add hash aggregate operator stub and module registration
TASK-042: Implement count/sum/avg aggregation functions
TASK-042: Add test fixtures for aggregate edge cases
```

### Checkpoint discipline

Agents break every task into the smallest self-contained units of progress. Each checkpoint must pass `scripts/local-ci.sh` (fmt, dep-direction, clippy, build, test) and a subagent code review, and be fast-forward-merged to main before the next checkpoint starts. Shared-file changes (`Cargo.toml`, `lib.rs` module declarations, trait definitions in `bqlite-core`) get their own dedicated checkpoint so other agents' pulls are stale for as short a window as possible.

---

## Task coordination

### Directory layout

```
tasks/
├── active/           # Lock files for in-progress tasks  (tasks/active/TASK-042.lock)
│   └── .gitkeep
└── completed/        # Completion markers for finished tasks (tasks/completed/TASK-042.done)
    └── .gitkeep
```

`TASKS.md` remains the task definition file (human-authored, agents read-only). Lock files and done markers are the coordination mechanism.

### Lock file format

`tasks/active/TASK-042.lock`:

```json
{
  "agent_id": "agent-3",
  "task_id": "TASK-042",
  "claimed_at": "2026-04-11T15:30:00Z",
  "branch": "task/TASK-042",
  "description": "Implement hash aggregate operator"
}
```

### Claim protocol

The wrapper calls `task_tool.py claim_next`, which:

1. Syncs `main`
2. Parses `TASKS.md` and filters to the agent's wave (any `[EASY]` or `[HARD]` tagged task is eligible)
3. Verifies dependency completion from `tasks/completed/`
4. Detects and breaks stale locks (see below)
5. Writes `tasks/active/TASK-NNN.lock`, commits, pushes to `origin/main` — the push-to-main is atomic, so concurrent claims from different agents can only produce one winner
6. If the push loses the race, resets the claim commit, restores `tasks/`, pulls, and retries internally (up to 5 attempts)

### Completion protocol

When the task's final checkpoint is merged to `main`, the agent moves its lock file to `tasks/completed/TASK-NNN.done` with a `completed_at` timestamp added, commits, and pushes. The wrapper detects success by checking for the done marker on `origin/main` after claude exits.

### Stale locks

A lock is stale if **all** of the following are true:
- `claimed_at` is older than 45 minutes (`STALE_LOCK_TIMEOUT_MINUTES`)
- The task branch doesn't exist OR has no commits in the last 45 minutes
- No commits on `main` reference the task ID in the last 45 minutes

`task_tool.py` breaks stale locks atomically (same push-to-main protocol) before trying to claim a fresh task. The 45-minute timeout is defined as `STALE_LOCK_TIMEOUT_MINUTES` in `task_tool.py` and can be overridden per-process via the env var of the same name.

### Backoff when nothing is claimable

When `claim_next` returns `no_claimable` but there are still blocked or active tasks in the wave, the wrapper backs off on a **2 min → 5 min → 10 min → 20 min → 60 min** ladder, then stays at 60 min indefinitely until a task becomes claimable. The backoff resets after any successful claim. Dependency unblocks, newly filed non-anchor tasks, and stale-lock breaks all change what's claimable between scans.

When `claim_next` returns `no_claimable` with *nothing* remaining (no claimable, blocked, or active work), the wrapper declares the wave drained and exits cleanly.

### When a wave has untagged tasks (`missing_difficulty`)

A task is claimable only if its title carries an `[EASY]` or `[HARD]` tag — the wrapper uses the tag to pick the claude model. If `task_tool.claim_next` finds no tagged claimable tasks but discovers wave tasks that are missing the tag and otherwise ready (dependencies satisfied, no lock), it returns `status: missing_difficulty` with the list of offenders attached.

The wrapper treats this as operator input required: it prints a `[NEEDS INPUT]` banner naming the untagged tasks and retries every 60 seconds. The wave does **not** back off further and does **not** declare itself drained — it is blocked until a human edits `TASKS.md` to tag the offenders and pushes the fix.

If you launched the fleet and one or more agents appear to be spinning in place — claiming nothing but not exiting — check their cmux tab output for `[NEEDS INPUT]`. The usual cause is an untagged task. Adding `[EASY]` or `[HARD]` (or `[RETIRED]`) to the task header and pushing `main` unblocks the next poll cycle.

### When tasks keep failing

Only *successful* tasks count toward the `-n` quota. If a task run fails to produce a done marker on `origin/main` (even after the one-shot `-c` resume), the wrapper releases the lock so another agent can try, does not count the attempt toward `-n`, and increments a consecutive-failure counter. After **3** consecutive failures with no successes in between, the wrapper gives up and exits with `too_many_failures` (non-zero exit code). Any successful task in between resets the counter.

The cap exists so a structurally broken task (compile error, bad migration, environment problem) cannot cause infinite claim-release-retry churn. If an agent exits with `too_many_failures`, check its cmux tab output for the actual claude failures, fix the root cause, and restart the fleet.

---

## Scripts in this directory

| File | Purpose |
|---|---|
| `agent_wrapper.py` | Per-container Python entry point — owns the fleet loop, runs claude once per task |
| `task_tool.py` | Task-file parser, claim/release/stale-lock logic, importable from the wrapper |
| `launch-fleet.sh` | Build image, start N idle containers |
| `attach-fleet.sh` | Open cmux with one tab per agent, invoking the wrapper in each tab |
| `status.sh` | Print active/completed task state + running container status |
| `stop-fleet.sh` | Stop and remove all `bqlite-agent-*` containers |
| `cmux-notify.sh` | Notification hook installed into containers so claude alerts surface as cmux toasts |
| `test_task_tool.py` | Unit tests for `task_tool.py` |
| `test_task_tool_integration.py` | End-to-end tests against a tmp git repo |
| `test_agent_wrapper.py` | Unit tests for the wrapper state machine |
| `test_agent_wrapper_integration.py` | End-to-end wrapper test with a fake `claude` binary on PATH |

Run the tests with:

```bash
python3 -m unittest discover -s scripts/agents -p 'test_*.py' -v
```

---

## Practical considerations

### Max subscription rate limits

Running 4–8 concurrent Claude Code sessions on one Max subscription may hit rate limits. Claude Code handles 429 responses with automatic retry/backoff; the wrapper treats pauses as normal and does not count them as failures.

### Resource usage

Each container runs a full Rust toolchain. On a Mac with 32 GB+ RAM:
- 4 agents: comfortable
- 6 agents: workable if builds aren't simultaneous
- 8 agents: likely needs 64 GB RAM or agents will swap during `cargo build`

`cargo build` is the bottleneck — it's CPU and memory intensive. Agents naturally stagger their builds (they're at different points in their loops), which helps.

### Agent crash recovery

If a container dies mid-task:
1. Its task's lock file persists in git
2. Other agents detect the stale lock after 45 minutes and break it
3. The task becomes claimable again
4. `launch-fleet.sh` re-creates missing containers; `attach-fleet.sh` reconnects cmux

Any work the crashed agent pushed to its task branch is preserved in git. The recovering agent (or a new one) can pick up from the branch state on the next claim.
