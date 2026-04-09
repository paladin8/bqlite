# Agent Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the infrastructure for running 4-8 concurrent Claude Code agents in Docker containers, coordinated through git-based task locking.

**Architecture:** Docker containers each clone the repo independently. Agents claim tasks via atomic git push of lock files. Host-side scripts manage container lifecycle and cmux integration. AGENTS.md encodes the full autonomous operating protocol.

**Tech Stack:** Docker, bash, git, cmux CLI, jq

**Spec:** [docs/design/agent-workflow.md](../../design/agent-workflow.md)

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `tasks/active/.gitkeep` | Create | Lock file directory for in-progress task claims |
| `tasks/completed/.gitkeep` | Create | Completion marker directory for finished tasks |
| `AGENTS.md` | Create | Complete autonomous agent operating protocol |
| `CLAUDE.md` | Modify | Add pointer to AGENTS.md |
| `.devcontainer/Dockerfile` | Modify | Add Claude Code, jq, openssh-client |
| `scripts/launch-fleet.sh` | Create | Build image and start N agent containers |
| `scripts/attach-fleet.sh` | Create | Open cmux workspace with tabs into each agent |
| `scripts/stop-fleet.sh` | Create | Stop and remove all agent containers |
| `scripts/status.sh` | Create | Show current task assignments from git |

---

### Task 1: Task Coordination Directories

**Files:**
- Create: `tasks/active/.gitkeep`
- Create: `tasks/completed/.gitkeep`

- [ ] **Step 1: Create the directory structure**

```bash
mkdir -p tasks/active tasks/completed
touch tasks/active/.gitkeep tasks/completed/.gitkeep
```

- [ ] **Step 2: Commit**

```bash
git add tasks/active/.gitkeep tasks/completed/.gitkeep
git commit -m "Add task coordination directories for agent lock files"
```

---

### Task 2: AGENTS.md — Agent Operating Protocol

**Files:**
- Create: `AGENTS.md`

- [ ] **Step 1: Write AGENTS.md**

Create `AGENTS.md` at the repo root with the following content:

```markdown
# Agent Operating Protocol

Instructions for autonomous Claude Code agents building bqlite in parallel. Read this file in full before starting work.

## Identity

Your agent ID is set in the `AGENT_ID` environment variable (e.g., `agent-1`). Your working directory is `/workspace`.

## Configuration

```
STALE_LOCK_TIMEOUT_MINUTES=45
```

## The Loop

Run this loop continuously:

1. `git pull origin main`
2. Scan `tasks/active/` for existing lock files and `tasks/completed/` for done markers
3. Read `TASKS.md` — select an unclaimed task whose **Depends on** tasks all have `.done` markers in `tasks/completed/`
4. Claim the task (see Task Claiming Protocol)
5. Read the task's design doc and any relevant source files
6. Check `.claude/skills/` for an applicable playbook — follow it if one exists
7. Implement in small checkpoints (see Checkpoint Discipline)
8. After the final checkpoint, mark the task complete (see Completion Protocol)
9. Return to step 1

## Task Claiming Protocol

1. Create `tasks/active/TASK-NNN.lock` with this JSON content:
   ```json
   {
     "agent_id": "<your AGENT_ID>",
     "task_id": "TASK-NNN",
     "claimed_at": "<current UTC ISO-8601 timestamp>",
     "branch": "task/TASK-NNN",
     "description": "<task description from TASKS.md>"
   }
   ```
2. `git add tasks/active/TASK-NNN.lock && git commit -m "TASK-NNN: claimed by <agent_id>" && git push origin main`
3. **If push fails**: another agent committed concurrently. Run:
   ```bash
   git reset HEAD~1
   git checkout -- tasks/
   git pull origin main
   ```
   Then go back to the loop and pick a different task.
4. After a successful push, create your working branch:
   ```bash
   git checkout -b task/TASK-NNN
   ```

## Completion Protocol

When the task's final checkpoint is merged to main:

1. Update the lock file to a done marker:
   ```bash
   git mv tasks/active/TASK-NNN.lock tasks/completed/TASK-NNN.done
   ```
2. Edit the `.done` file to add `completed_at`:
   ```json
   {
     "agent_id": "<your AGENT_ID>",
     "task_id": "TASK-NNN",
     "claimed_at": "<original claim time>",
     "completed_at": "<current UTC ISO-8601 timestamp>",
     "branch": "task/TASK-NNN",
     "description": "<task description>"
   }
   ```
3. `git commit -m "TASK-NNN: completed" && git push origin main`

## Stale Lock Detection

Before claiming a task, check all lock files in `tasks/active/`:

A lock is stale if ALL of the following are true:
- `claimed_at` is older than `STALE_LOCK_TIMEOUT_MINUTES`
- The task branch (`origin/task/TASK-NNN`) either doesn't exist or has no commits in the last `STALE_LOCK_TIMEOUT_MINUTES`
- No commits on `origin/main` reference the task ID in the last `STALE_LOCK_TIMEOUT_MINUTES`

To break a stale lock: remove the lock file, commit, and push. The atomic push protocol applies — if two agents race to break the same lock, only one succeeds.

## Checkpoint Discipline

Break every task into the smallest self-contained units of progress. Each checkpoint must:

1. Compile: `cargo build`
2. Pass tests: `cargo test`
3. Pass lint: `cargo clippy --all-targets --all-features -- -D warnings`
4. Be merged to main immediately — do not accumulate checkpoints

**Merge protocol for each checkpoint:**

```bash
git checkout main
git pull origin main
git merge task/TASK-NNN --ff-only
git push origin main
```

If `--ff-only` fails:
```bash
git checkout task/TASK-NNN
git rebase main
git checkout main
git merge task/TASK-NNN --ff-only
git push origin main
```

If push fails: `git pull --rebase origin main && git push origin main`

If rebase conflicts are too complex to resolve cleanly, abandon local work and restart the task on fresh main.

After merging, continue work on the task branch:
```bash
git checkout task/TASK-NNN
```

**Shared file priority:** Changes to shared files (`Cargo.toml`, `lib.rs` module declarations, trait definitions in `bqlite-core`) must be their own checkpoint, merged before dependent work begins. This minimizes the window where other agents' pulls are stale.

**Ideal checkpoint:** One that only adds new files — zero conflict risk.

## Git Conventions

- **Branch naming:** `task/TASK-NNN`
- **Commit messages:** `TASK-NNN: <description>`
  ```
  TASK-042: Add hash aggregate operator stub and module registration
  TASK-042: Implement count/sum/avg aggregation functions
  TASK-042: Add test fixtures for aggregate edge cases
  ```
- **Never** force-push. **Never** create merge commits.

## Behavioral Requirements

1. **Flag design decisions for human review.** When a task involves a significant design choice (new abstraction, interface change, performance tradeoff), document the decision and alternatives in the relevant `docs/design/` file and prefix your message with `[NEEDS INPUT]` to alert the human.

2. **Document decisions before committing.** Every non-trivial decision must be captured in documentation — design docs, code comments, or CLAUDE.md updates. Update docs in the same checkpoint as the code change, not as a follow-up.

3. **Write thorough tests.** Test every code path including edge cases: empty inputs, single-event entities, entity event limits, segment boundary crossings. Add benchmarks for performance-critical paths.

4. **Code review via subagent.** After implementing a complex change (new operator, storage engine modification, planner change), spawn a subagent to review the code for: correctness, performance, API ergonomics, error handling, documentation, and test coverage.

5. **Always validate before committing.** No commit without:
   - `cargo test` passing
   - `cargo clippy --all-targets --all-features -- -D warnings` clean
   - Documentation updated

6. **Consider refactoring.** After completing a task, evaluate whether the code benefits from refactoring. Small, focused refactoring is encouraged. Large refactors should be filed as separate tasks.

7. **Performance-first mindset.** Prefer zero-copy over allocation, iterators over collections, stack over heap, cache-friendly access patterns over random access. When in doubt, benchmark.

## When to Ask for Human Input

Prefix your message with **[NEEDS INPUT]** so the human can spot it in their cmux tabs:

- Architecture or design decisions with multiple valid approaches
- Ambiguous acceptance criteria in a task definition
- Merge conflicts you cannot resolve cleanly
- Any situation where proceeding could waste significant work if the wrong path is chosen

Wait for a response in the interactive session before proceeding.

## Rate Limits

Rate limit pauses (429 responses) are expected with multiple agents sharing a subscription. Claude Code handles retries automatically. Do not interpret pauses as errors — just wait.
```

- [ ] **Step 2: Commit**

```bash
git add AGENTS.md
git commit -m "Add AGENTS.md — autonomous agent operating protocol"
```

---

### Task 3: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md` (append after the Skills section)

- [ ] **Step 1: Add agent coordination section to CLAUDE.md**

Append the following to the end of `CLAUDE.md`:

```markdown

## Agent Coordination

See [AGENTS.md](AGENTS.md) for the autonomous agent operating protocol (task claiming, checkpoints, git workflow).
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "Add AGENTS.md pointer to CLAUDE.md"
```

---

### Task 4: Update Devcontainer

**Files:**
- Modify: `.devcontainer/Dockerfile`
- Modify: `.devcontainer/devcontainer.json`

- [ ] **Step 1: Update Dockerfile**

Replace the contents of `.devcontainer/Dockerfile` with:

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

# Pre-warm cargo registry to speed up first build
RUN cargo search --limit 1 serde

# Python tooling for later waves
RUN pip install maturin pytest
```

- [ ] **Step 2: Update devcontainer.json to set working directory**

Replace the contents of `.devcontainer/devcontainer.json` with:

```json
{
  "name": "bqlite",
  "build": {
    "dockerfile": "Dockerfile"
  },
  "features": {
    "ghcr.io/devcontainers/features/python:1": {
      "version": "3.11"
    }
  },
  "postCreateCommand": "cargo build && pip install maturin pytest",
  "workspaceFolder": "/workspace",
  "customizations": {
    "vscode": {
      "extensions": [
        "rust-lang.rust-analyzer",
        "tamasfe.even-better-toml"
      ]
    }
  }
}
```

- [ ] **Step 3: Verify the Docker image builds**

```bash
docker build -t bqlite-agent -f .devcontainer/Dockerfile .
```

Expected: image builds successfully (this may take a few minutes on first run due to Rust toolchain).

- [ ] **Step 4: Commit**

```bash
git add .devcontainer/Dockerfile .devcontainer/devcontainer.json
git commit -m "Update devcontainer with Claude Code, jq, and ssh tooling"
```

---

### Task 5: scripts/launch-fleet.sh

**Files:**
- Create: `scripts/launch-fleet.sh`

- [ ] **Step 1: Write launch-fleet.sh**

Create `scripts/launch-fleet.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

N="${1:-4}"
IMAGE="bqlite-agent"
REPO_URL="git@github.com:paladin8/bqlite.git"

# Validate SSH agent is running
if [ -z "${SSH_AUTH_SOCK:-}" ]; then
  echo "ERROR: SSH_AUTH_SOCK is not set. Start your SSH agent first:"
  echo "  eval \$(ssh-agent -s) && ssh-add"
  exit 1
fi

# Build image (uses Docker cache after first run)
echo "Building devcontainer image..."
docker build -t "$IMAGE" -f .devcontainer/Dockerfile . -q

echo "Starting $N agent containers..."
for i in $(seq 1 "$N"); do
  NAME="bqlite-agent-$i"

  # Skip if already running
  if docker ps -q -f "name=^${NAME}$" 2>/dev/null | grep -q .; then
    echo "  $NAME: already running, skipping"
    continue
  fi

  # Remove stopped container with same name if it exists
  docker rm -f "$NAME" 2>/dev/null || true

  docker run -d \
    --name "$NAME" \
    -e AGENT_ID="agent-$i" \
    -v "$HOME/.claude:/home/vscode/.claude-host:ro" \
    -v "${SSH_AUTH_SOCK}:/ssh-agent" \
    -e SSH_AUTH_SOCK=/ssh-agent \
    -w /workspace \
    "$IMAGE" \
    bash -c "
      # Copy auth files to writable location
      mkdir -p /home/vscode/.claude
      cp -r /home/vscode/.claude-host/* /home/vscode/.claude/ 2>/dev/null || true

      # Clone and configure
      git clone $REPO_URL /workspace &&
      cd /workspace &&
      git config user.name \"bqlite-agent-$i\" &&
      git config user.email \"bqlite-agent-${i}@agent.local\" &&
      echo \"Container bqlite-agent-$i ready\" &&
      exec tail -f /dev/null
    "

  echo "  $NAME: started"
done

echo ""
echo "Fleet ready. Run: scripts/attach-fleet.sh"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/launch-fleet.sh
```

- [ ] **Step 3: Verify the script parses correctly**

```bash
bash -n scripts/launch-fleet.sh
```

Expected: no output (no syntax errors).

- [ ] **Step 4: Commit**

```bash
git add scripts/launch-fleet.sh
git commit -m "Add launch-fleet.sh — start N agent Docker containers"
```

---

### Task 6: scripts/attach-fleet.sh

**Files:**
- Create: `scripts/attach-fleet.sh`

- [ ] **Step 1: Write attach-fleet.sh**

Create `scripts/attach-fleet.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

CONTAINERS=$(docker ps --filter "name=bqlite-agent-" --format "{{.Names}}" | sort -V)

if [ -z "$CONTAINERS" ]; then
  echo "No running bqlite-agent containers found."
  echo "Run scripts/launch-fleet.sh first."
  exit 1
fi

COUNT=$(echo "$CONTAINERS" | wc -l | tr -d ' ')
echo "Attaching to $COUNT agent containers via cmux..."

# Create a cmux workspace for the fleet
cmux new-workspace "bqlite agents"

for CONTAINER in $CONTAINERS; do
  AGENT_NUM="${CONTAINER##*-}"
  SURFACE=$(cmux new-surface --type terminal)

  SYSTEM_PROMPT="You are ${CONTAINER}, an autonomous agent building bqlite. Read AGENTS.md for your complete operating protocol. Begin the agent loop now."

  cmux send --surface "$SURFACE" \
    "docker exec -it -w /workspace $CONTAINER claude --system-prompt '$SYSTEM_PROMPT'"
  cmux send-key --surface "$SURFACE" enter

  echo "  Tab created for $CONTAINER"
done

cmux notify "Fleet attached: $COUNT agents"
echo "Done. Switch to cmux to interact with agents."
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/attach-fleet.sh
```

- [ ] **Step 3: Verify syntax**

```bash
bash -n scripts/attach-fleet.sh
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add scripts/attach-fleet.sh
git commit -m "Add attach-fleet.sh — open cmux tabs into agent fleet"
```

---

### Task 7: scripts/stop-fleet.sh

**Files:**
- Create: `scripts/stop-fleet.sh`

- [ ] **Step 1: Write stop-fleet.sh**

Create `scripts/stop-fleet.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

RUNNING=$(docker ps -q --filter "name=bqlite-agent-")

if [ -z "$RUNNING" ]; then
  echo "No running bqlite-agent containers found."
  exit 0
fi

COUNT=$(echo "$RUNNING" | wc -l | tr -d ' ')
echo "Stopping $COUNT agent containers..."

docker ps -q --filter "name=bqlite-agent-" | xargs docker stop
docker ps -aq --filter "name=bqlite-agent-" | xargs docker rm

echo "Fleet stopped and cleaned up."
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/stop-fleet.sh
```

- [ ] **Step 3: Verify syntax**

```bash
bash -n scripts/stop-fleet.sh
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add scripts/stop-fleet.sh
git commit -m "Add stop-fleet.sh — tear down agent fleet"
```

---

### Task 8: scripts/status.sh

**Files:**
- Create: `scripts/status.sh`

- [ ] **Step 1: Write status.sh**

Create `scripts/status.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Shallow clone to read task state
git clone --depth 1 -q git@github.com:paladin8/bqlite.git "$TMPDIR" 2>/dev/null

echo "=== Active Tasks ==="
ACTIVE_COUNT=0
for lock in "$TMPDIR"/tasks/active/*.lock; do
  [ -f "$lock" ] || continue
  ACTIVE_COUNT=$((ACTIVE_COUNT + 1))
  jq -r '"\(.agent_id)  \(.task_id)  \(.description)  (since \(.claimed_at))"' "$lock"
done
if [ "$ACTIVE_COUNT" -eq 0 ]; then
  echo "  (none)"
fi

echo ""
echo "=== Completed Tasks ==="
DONE_COUNT=0
for done in "$TMPDIR"/tasks/completed/*.done; do
  [ -f "$done" ] || continue
  DONE_COUNT=$((DONE_COUNT + 1))
  jq -r '"\(.task_id)  \(.description)  (completed \(.completed_at // "unknown"))"' "$done"
done
if [ "$DONE_COUNT" -eq 0 ]; then
  echo "  (none)"
fi

echo ""
echo "=== Container Status ==="
docker ps --filter "name=bqlite-agent-" --format "{{.Names}}\t{{.Status}}" 2>/dev/null || echo "  (docker not available)"

echo ""
echo "Active: $ACTIVE_COUNT  Completed: $DONE_COUNT"
```

- [ ] **Step 2: Make executable**

```bash
chmod +x scripts/status.sh
```

- [ ] **Step 3: Verify syntax**

```bash
bash -n scripts/status.sh
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add scripts/status.sh
git commit -m "Add status.sh — show task assignments and container state"
```

---

### Task 9: Smoke Test

Verify everything works end-to-end without actually launching agents.

- [ ] **Step 1: Verify Docker image builds**

```bash
docker build -t bqlite-agent -f .devcontainer/Dockerfile . -q
```

Expected: image ID printed (no errors).

- [ ] **Step 2: Verify all scripts parse cleanly**

```bash
bash -n scripts/launch-fleet.sh
bash -n scripts/attach-fleet.sh
bash -n scripts/stop-fleet.sh
bash -n scripts/status.sh
```

Expected: no output from any command.

- [ ] **Step 3: Verify AGENTS.md is well-formed**

```bash
test -f AGENTS.md && echo "AGENTS.md exists" || echo "MISSING"
grep -q "STALE_LOCK_TIMEOUT_MINUTES" AGENTS.md && echo "Config constant present" || echo "MISSING"
grep -q "Task Claiming Protocol" AGENTS.md && echo "Claiming protocol present" || echo "MISSING"
grep -q "Checkpoint Discipline" AGENTS.md && echo "Checkpoint discipline present" || echo "MISSING"
```

Expected: all four lines print the success message.

- [ ] **Step 4: Verify CLAUDE.md links to AGENTS.md**

```bash
grep -q "AGENTS.md" CLAUDE.md && echo "Link present" || echo "MISSING"
```

Expected: "Link present"

- [ ] **Step 5: Verify task directories exist**

```bash
test -d tasks/active && test -d tasks/completed && echo "Task dirs OK" || echo "MISSING"
```

Expected: "Task dirs OK"
