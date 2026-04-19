# Agent Operating Protocol

Instructions for an autonomous Claude Code agent executing a single bqlite task. Read this file in full before starting work.

## Identity

Your agent ID is set in the `AGENT_ID` environment variable (e.g., `agent-1`). Your working directory is `/workspace`. Each task in `TASKS.md` carries either an `[EASY]` or `[HARD]` tag — the wrapper picks the claude model based on that tag (Sonnet for `[EASY]`, Opus 4.7 for `[HARD]`). There is no pool assignment at the container level; any agent can be handed any tagged task in its wave.

## Your Job

You have been handed exactly one task. The lock file at `tasks/active/TASK-NNN.lock` is already yours, and you have been dropped onto the `task/TASK-NNN` branch. Your job is to take it all the way from here to a merged, marked-done state:

1. Read the task definition in `TASKS.md` and the relevant design doc under `docs/design/`
2. Check for a task note at `tasks/notes/TASK-NNN.md` and read it in full if present (see *Task Notes*)
3. If the task seems complex at all, build a development plan *before* writing code (see *Planning Before Implementation*)
4. If a plan was produced, have it reviewed by a subagent before implementation (see *Plan Review via Subagent*)
5. Implement the task in small checkpoints (see *Checkpoint Discipline*)
6. After the final checkpoint merges to `main`, mark the task complete (see *Completion Protocol*)
7. End your turn

Do not claim another task. Do not start a loop. When you finish, the wrapper will launch a fresh session for the next task.

## Task Notes

Before planning or implementation, check `tasks/notes/TASK-NNN.md` for a task note. These are human-authored, task-scoped briefings that capture semantics decisions, constraints, or context that are *not* in `TASKS.md` or the design docs — typically the output of a human-assisted semantics discussion held before the task was handed to an agent.

If a task note exists:

- Read it in full before writing a plan or touching code.
- Treat its decisions as authoritative. They override your own judgment on the points they cover, and they take precedence over inferences you would otherwise draw from `TASKS.md` or a design doc that has not yet been updated to reflect them.
- If the note contradicts the existing design doc, the note wins; reconcile the design doc in the same checkpoint as the code change.
- If the note leaves a sub-question open, surface it via `[NEEDS INPUT]` rather than guessing — the note's existence means a human is engaged on the semantics for this task.

If no task note exists, proceed normally. Absence is not a blocker.

## Planning Before Implementation

If the task seems complex at all — multiple components, non-trivial algorithms, unclear decomposition, cross-crate changes, or anything where the path from task definition to working code is not immediately obvious — write a development plan *before* touching code.

Use the `superpowers:writing-plans` skill to produce the plan. Save it under `docs/superpowers/plans/YYYY-MM-DD-<task-slug>.md`. A good plan:

- Breaks the work into checkpoint-sized units that each satisfy *Checkpoint Discipline* (pass local-ci, mergeable independently)
- Identifies shared-file changes up front so they can be scheduled as their own early checkpoint
- Calls out decisions that need human input *before* you burn implementation time on them — surface these via `[NEEDS INPUT]` rather than guessing
- Reconciles against the relevant `docs/design/` spec; if the plan requires spec changes, note that explicitly

When the task is genuinely trivial (single-file change, mechanical edit, obvious fix), skip the plan and go straight to implementation. When in doubt, plan — the cost of a short plan is small compared to the cost of ripping up a half-finished implementation.

### Plan Review via Subagent

After writing the plan and before beginning implementation, spawn a subagent to review it. The reviewer must evaluate the plan against three criteria:

1. **Completeness** — Does the plan cover every requirement in the task definition, task note (if present), and relevant design doc? Are there missing checkpoints, untested edge cases, or unaddressed error paths?
2. **Correctness** — Are the proposed abstractions, type signatures, and data flows consistent with the crate map, dependency direction, and existing APIs? Does the plan respect the invariants documented in `docs/core-beliefs.md` and the relevant `docs/design/` spec? Would the proposed changes break any existing contract?
3. **Performance** — Does the plan follow the performance conventions in `CLAUDE.md`? Are there unnecessary allocations, eager materializations, or hot-path heap work that could be avoided? Does the plan preserve entity locality, dictionary/compressed representations, and zero-copy guarantees where applicable?

The subagent should receive the full plan text, the task definition from `TASKS.md`, the task note (if any), and paths to the relevant design docs. Its output should be a structured review with:

- **Blocking issues** — problems that must be fixed before implementation starts
- **Suggestions** — non-blocking improvements worth considering
- **Verdict** — `APPROVE` or `REVISE`

If the verdict is `REVISE`, address every blocking issue, update the plan file, and re-review. Do not begin implementation until the plan review returns `APPROVE`.

If the plan is short enough that a review would be purely ceremonial (e.g., a two-step plan for a single-file change), you may skip the review — but if you wrote a plan at all, the default expectation is that it gets reviewed.

## Completion Protocol

When the task's final checkpoint is merged to main:

1. Move the lock file to a done marker:
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

A task is not done until the `.done` marker is on `origin/main`.

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

- **Branch naming:** `task/TASK-NNN` (already checked out for you)
- **Commit messages:** `TASK-NNN: <description>`
  ```
  TASK-042: Add hash aggregate operator stub and module registration
  TASK-042: Implement count/sum/avg aggregation functions
  TASK-042: Add test fixtures for aggregate edge cases
  ```
- **Never** force-push. **Never** create merge commits.

## Behavioral Requirements

1. **Flag design decisions for human review.** When a task involves a significant design choice (new abstraction, interface change, performance tradeoff), document the decision and alternatives in the relevant `docs/design/` file and ask a human before committing — see *When to Ask for Human Input*.

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

When you need a human decision you cannot resolve alone, end your turn with a message whose last line begins with `[NEEDS INPUT]` followed by your question on its own line. For example:

```
I've drafted two approaches for the aggregate spill strategy in TASK-412 but
I want confirmation before committing to one.

[NEEDS INPUT] Should spilled aggregate partitions be written into the segment
directory tree alongside data segments, or into a sibling `spill/` tree? The
tradeoffs are documented in docs/design/TASK-412.md.
```

A human will reply with one line on the cmux tab, and your next turn will resume with that reply as the user message. Keep questions specific and scoped — a one-line reply may not be enough context for an open-ended question.

Good reasons to use `[NEEDS INPUT]`:

- Architecture or design decisions with multiple valid approaches
- Ambiguous acceptance criteria in a task definition
- Merge conflicts you cannot resolve cleanly
- Any situation where proceeding could waste significant work if the wrong path is chosen

## Rate Limits

Rate limit pauses (429 responses) are expected with multiple agents sharing a subscription. Claude Code handles retries automatically. Do not interpret pauses as errors — just wait.
