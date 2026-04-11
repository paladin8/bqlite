# Agent Operating Protocol

Instructions for autonomous Claude Code agents building bqlite in parallel. Read this file in full before starting work.

## Identity

Your agent ID is set in the `AGENT_ID` environment variable (e.g., `agent-1`). Your working directory is `/workspace`.
The wrapper also sets `TASK_DIFFICULTY_POOL` to `EASY` or `HARD`. `EASY` agents run Sonnet at high effort and may only claim tasks tagged `[EASY]`; `HARD` agents run Opus at high effort and may only claim tasks tagged `[HARD]`.

## Configuration

```
STALE_LOCK_TIMEOUT_MINUTES=45
```

## The Loop

Run this loop continuously:

1. Run `python3 scripts/task_tool.py claim-next --wave "$TASK_WAVE" --difficulty "$TASK_DIFFICULTY_POOL" --agent-id "$AGENT_ID"` to sync `main`, parse `TASKS.md`, enforce wave+difficulty restrictions, detect stale locks, and atomically claim the next eligible task
2. Interpret the script's `status` field:
   - `"claimed"` — continue with the returned task. The response may also include `missing_difficulty_tasks`; these are informational only and do not block you, but flag them to a human if they persist across cycles.
   - `"no_claimable"` — follow the backoff schedule.
   - `"missing_difficulty"` — your pool has no claimable work and the only remaining wave tasks are untagged. Emit `[NEEDS INPUT]` so the task list can be fixed rather than guessing.
3. Read the task's design doc and any relevant source files
4. Implement in small checkpoints (see Checkpoint Discipline)
5. After the final checkpoint, mark the task complete (see Completion Protocol)
6. Return to step 1

## Ending Your Turn

You run inside a wrapper script (`scripts/agent-wrapper.sh`) that watches for control markers in your final message. You **must** end every turn with exactly one of:

- `[END LOOP]` — you want a fresh context before continuing. The wrapper relaunches `claude` with a clean conversation; you will re-read AGENTS.md and rescan task state. Use this after completing a task when your context is large, or any time you judge that a fresh slate will help you reason about the next task.
- `[WAVE COMPLETE]` — there are no more claimable tasks in your wave. Only emit this when you have actually verified one of: (a) every task in the wave has a `.done` marker in `tasks/completed/`, or (b) every remaining task is claimed by another live agent or permanently blocked, **and** you have exhausted the full backoff schedule in the next section with no progress. The wrapper exits and does not relaunch.
- `[NEEDS INPUT]` — you need a human decision (see *When to Ask for Human Input* below). The wrapper pauses and then resumes your session with `claude --continue` so the human can reply inline with the full context preserved. Include your question in the same message as the marker.

If you end a turn without one of these markers, the Stop hook blocks you and injects a message beginning with "Do not end your turn without an explicit marker…". That message is not an error — it is the normal signal to return to step 1 of the agent loop. Continue executing; do not emit `[NEEDS INPUT]` asking what happened.

## Backoff When No Tasks Are Claimable

If `scripts/task_tool.py claim-next` reports no unclaimed task matching your `TASK_DIFFICULTY_POOL` whose dependencies are satisfied — all remaining tasks in your pool are either claimed, completed, untagged, or blocked by unfinished dependencies — sleep and retry on the following schedule:

**2 min → 5 min → 10 min → 20 min → 60 min**, then stay at 60 min indefinitely until a task becomes claimable.

Reset the backoff to 2 min after successfully claiming any task. Do not exit the loop and do not report the wave as done based on a single empty scan — other agents may be mid-task and their completions will unblock more work. Dependency unblocks, newly filed non-anchor tasks, and stale-lock breaks all change what's claimable between scans.
Do not claim a task tagged for the other pool just because your pool is empty.

## Task Claiming Protocol

Use `python3 scripts/task_tool.py claim-next --wave "$TASK_WAVE" --difficulty "$TASK_DIFFICULTY_POOL" --agent-id "$AGENT_ID"` instead of hand-rolling the claim steps. The script:

1. Syncs `main`
2. Parses `TASKS.md`
3. Filters to your assigned wave and difficulty pool
4. Verifies dependency completion from `tasks/completed/`
5. Detects and breaks stale locks when the AGENTS.md stale-lock rules say it is safe
6. Writes `tasks/active/TASK-NNN.lock`, commits it, pushes it to `origin/main`, and checks out `task/TASK-NNN`

If the script loses a push race, it resets the temporary claim commit, restores `tasks/`, pulls `main`, and retries internally. Agents should treat the script as the source of truth for whether a task was actually claimed.

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

`scripts/task_tool.py claim-next` performs stale-lock detection before claiming a task. If you need to reason about the output or debug the script, the stale-lock rule is:

A lock is stale if ALL of the following are true:
- `claimed_at` is older than `STALE_LOCK_TIMEOUT_MINUTES`
- The task branch (`origin/task/TASK-NNN`) either doesn't exist or has no commits in the last `STALE_LOCK_TIMEOUT_MINUTES`
- No commits on `origin/main` reference the task ID in the last `STALE_LOCK_TIMEOUT_MINUTES`

To break a stale lock: remove the lock file, commit, and push. The atomic push protocol applies — if two agents race to break the same lock, only one succeeds.

## Checkpoint Discipline

Break every task into the smallest self-contained units of progress. Each checkpoint must:

1. Pass `scripts/local-ci.sh` (mirrors `.github/workflows/ci.yml`: fmt, dep-direction, clippy, build, test)
2. Pass a subagent code review of the staged changes (see Behavioral Requirement #4)
3. Be reconciled against the task's design doc in `docs/design/`. Re-read the design doc and confirm the staged changes match it. Any drift must be resolved before merging — either correct the implementation, or update the design doc in the same checkpoint to reflect a deliberate change (and note the reason in the commit message). Never merge code that silently diverges from the spec.
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

   **When to reach for property tests (`proptest`):** when the code under test has a large input state space and at least one output invariant you can state without re-implementing the code. Good candidates: parser/printer roundtrips, encoder/decoder symmetry, optimizer rewrites preserving result equivalence, k-way merge stability across input orderings, compaction preserving the logical row set, sequence matchers matching the spec on arbitrary event streams. Heuristic: if you can say *"for any X, Y must hold"*, write a property test. If the only invariants you can express are *"on this specific input, produce this specific output"*, example tests are sufficient. One or two well-chosen properties beat dozens of shallow unit cases. See Core Belief #11.

4. **Code review via subagent — mandatory before every commit.** Before every commit (not just complex changes), spawn a subagent to review the staged diff for: correctness, performance, API ergonomics, error handling, documentation, and test coverage. If the reviewer raises any blocking issue, address it and re-review before committing.

5. **Always validate before committing.** No commit without:
   - `scripts/local-ci.sh` passing end-to-end (mirrors the GitHub Actions CI)
   - Subagent code review completed with no blocking findings (see #4)
   - Implementation reconciled against the task's design doc in `docs/design/` — if the design changed, update the doc in the same checkpoint
   - Documentation updated in the same checkpoint as the code change

6. **Consider refactoring.** After completing a task, evaluate whether the code benefits from refactoring. Small, focused refactoring is encouraged. Large refactors should be filed as separate tasks.

7. **Performance-first mindset.** Prefer zero-copy over allocation, iterators over collections, stack over heap, cache-friendly access patterns over random access. When in doubt, benchmark.

## When to Ask for Human Input

End your turn with **[NEEDS INPUT]** (see *Ending Your Turn*) so the wrapper pauses and the human can reply in the resumed session:

- Architecture or design decisions with multiple valid approaches
- Ambiguous acceptance criteria in a task definition
- `task_tool.py` returns `status: "missing_difficulty"` — your pool has drained and the only remaining wave work is untagged, so a human must decide the pool routing before anyone can proceed
- Merge conflicts you cannot resolve cleanly
- Any situation where proceeding could waste significant work if the wrong path is chosen

Include the question itself in the same message as the marker. The wrapper will resume the same session with `claude --continue` so the human sees the full context.

## Rate Limits

Rate limit pauses (429 responses) are expected with multiple agents sharing a subscription. Claude Code handles retries automatically. Do not interpret pauses as errors — just wait.
