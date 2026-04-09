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

1. `git fetch origin && git pull origin main` (fetch all refs so task branch info is current)
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
