# Agent Wrapper Python Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the bash agent wrapper and Stop-hook marker protocol with a Python wrapper that owns task selection, batching, retries, and NEEDS INPUT handling, invoking `claude` once per task.

**Architecture:** One `claude -p --verbose` invocation per task. Prompt is scoped to "implement TASK-NNN end-to-end, mark done, exit." The Python wrapper owns the completed-task counter, exponential backoff, retry-once-on-incomplete, and NEEDS INPUT stdin capture. `task_tool.py` functions are imported directly (no subprocess + JSON parsing). The Stop hook marker protocol is deleted.

**Tech Stack:** Python 3.11+ stdlib (no new deps), bash for the two host-side orchestration scripts that invoke the wrapper.

**Spec:** This plan document + discussion in the conversation that produced it.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `scripts/task_tool.py` | Modify | Add `release_lock()`, `task_done_path()`, `task_lock_path()`, `is_wave_drained()` helpers |
| `scripts/test_task_tool.py` | Modify | Unit tests for the new path helpers and `is_wave_drained()` |
| `scripts/test_task_tool_integration.py` | Modify | Integration test covering `release_lock` end-to-end (commit + push) |
| `scripts/agent_wrapper.py` | Create | Python wrapper entry point — arg parsing, main loop, claude invocation, NEEDS INPUT capture |
| `scripts/test_agent_wrapper.py` | Create | Unit tests for pure functions (args, prompt, output scanning) |
| `scripts/test_agent_wrapper_integration.py` | Create | Integration tests using a fake `claude` binary on PATH + tmp git repo |
| `scripts/attach-fleet.sh` | Modify | Change `agent_cmd` to invoke `python3 /workspace/scripts/agent_wrapper.py` |
| `scripts/launch-fleet.sh` | Modify | Remove Stop-hook install line and remove `Stop` from the settings.json hook patch |
| `scripts/agent-wrapper.sh` | Delete | Replaced by `agent_wrapper.py` |
| `scripts/stop-agent-loop.sh` | Delete | No longer needed — no marker protocol |
| `AGENTS.md` | Modify | Trim marker protocol and multi-task loop sections; pivot to single-task execution |
| `docs/design/agent-workflow.md` | Modify | Short update describing the new wrapper/claude control-plane split |

---

## Conventions used throughout this plan

- Tests use `unittest` (matches existing `scripts/test_task_tool.py`).
- Run a specific test with: `python3 -m unittest scripts.test_agent_wrapper.AgentWrapperUnitTests.test_name -v` (add `scripts/` to `PYTHONPATH` if needed, or run as `python3 scripts/test_agent_wrapper.py SomeTests.test_name`).
- Commits should follow the repo convention of a single scoping line; no task-ID prefix is needed for wrapper changes since this work isn't filed as a TASK-NNN in TASKS.md.
- After every code step, run `python3 -m unittest discover -s scripts -p 'test_*.py' -v` to catch regressions in the existing test suite.

---

## Task 1: Add `release_lock` and path helpers to `task_tool.py`

**Why:** The wrapper needs a way to release a lock it previously claimed when a task run fails, and needs trivially-callable helpers for lock / done path resolution. Keeping these inside `task_tool.py` co-locates them with `claim_next` so the git command style stays consistent.

**Files:**
- Modify: `scripts/task_tool.py` (add functions near existing lock helpers, ~line 338)
- Modify: `scripts/test_task_tool.py` (append new test class)
- Modify: `scripts/test_task_tool_integration.py` (append new integration test case)

- [ ] **Step 1: Write failing unit tests for the path helpers**

Append to `scripts/test_task_tool.py` (after the existing `TaskToolTests` class):

```python
class TaskToolPathHelperTests(unittest.TestCase):
    def test_task_lock_path_points_to_active_dir(self) -> None:
        path = task_tool.task_lock_path("TASK-042")
        self.assertEqual(path.name, "TASK-042.lock")
        self.assertEqual(path.parent.name, "active")

    def test_task_done_path_points_to_completed_dir(self) -> None:
        path = task_tool.task_done_path("TASK-042")
        self.assertEqual(path.name, "TASK-042.done")
        self.assertEqual(path.parent.name, "completed")

    def test_is_wave_drained_when_nothing_claimable(self) -> None:
        classified = {
            "claimable": [],
            "claimable_missing_difficulty": [],
            "blocked": [],
            "active": [],
            "completed": [],
            "other_pool_claimable": [],
        }
        self.assertTrue(task_tool.is_wave_drained(classified))

    def test_is_wave_drained_false_when_blocked_work_remains(self) -> None:
        dummy = task_tool.Task(
            task_id="TASK-201",
            number=201,
            wave=2,
            title="dep-blocked task",
            tags=("EASY",),
            depends_on=("TASK-200",),
        )
        classified = {
            "claimable": [],
            "claimable_missing_difficulty": [],
            "blocked": [dummy],
            "active": [],
            "completed": [],
            "other_pool_claimable": [],
        }
        self.assertFalse(task_tool.is_wave_drained(classified))

    def test_is_wave_drained_false_when_active_work_remains(self) -> None:
        dummy = task_tool.Task(
            task_id="TASK-202",
            number=202,
            wave=2,
            title="active task",
            tags=("EASY",),
            depends_on=(),
        )
        classified = {
            "claimable": [],
            "claimable_missing_difficulty": [],
            "blocked": [],
            "active": [dummy],
            "completed": [],
            "other_pool_claimable": [],
        }
        self.assertFalse(task_tool.is_wave_drained(classified))
```

- [ ] **Step 2: Run the new tests and confirm they fail with AttributeError**

```bash
cd /Users/jeffrey.wang/coding/bqlite
python3 -m unittest scripts.test_task_tool.TaskToolPathHelperTests -v
```

Expected: 5 errors like `AttributeError: module 'task_tool' has no attribute 'task_lock_path'`.

- [ ] **Step 3: Implement the four helpers in `task_tool.py`**

Add immediately after `ensure_task_branch` (~line 369) in `scripts/task_tool.py`:

```python
def task_lock_path(task_id: str) -> Path:
    return ACTIVE_DIR / f"{task_id}.lock"


def task_done_path(task_id: str) -> Path:
    return COMPLETED_DIR / f"{task_id}.done"


def is_wave_drained(classified: dict[str, list[Task]]) -> bool:
    """True iff there is no remaining work in this wave/pool and nothing
    another agent could unblock us on. Used by agent_wrapper.py to decide
    whether to exit the fleet loop vs. continue backing off.
    """
    return not (
        classified["claimable"]
        or classified["claimable_missing_difficulty"]
        or classified["blocked"]
        or classified["active"]
        or classified["other_pool_claimable"]
    )
```

- [ ] **Step 4: Run the path-helper tests and confirm they pass**

```bash
python3 -m unittest scripts.test_task_tool.TaskToolPathHelperTests -v
```

Expected: `OK` — 5 tests pass.

- [ ] **Step 5: Write failing integration test for `release_lock`**

Append to `scripts/test_task_tool_integration.py` (inside `TaskToolIntegrationTests`, after the last test method):

```python
    def test_release_lock_removes_lock_and_pushes(self) -> None:
        repo = self.make_repo("""
            ### TASK-101: [EASY][IMPL] Releasable task
            **Depends on**: none
        """)
        claim = self.run_task_tool(
            repo,
            "claim-next",
            "--wave", "1",
            "--difficulty", "EASY",
            "--agent-id", "agent-1",
        )
        self.assertEqual(claim["status"], "claimed")
        lock_path = repo / "tasks" / "active" / "TASK-101.lock"
        self.assertTrue(lock_path.exists())

        env = os.environ.copy()
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        result = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, %r); "
             "import task_tool; "
             "task_tool.sync_main(); "
             "print(task_tool.release_lock('TASK-101', 'agent-1', 'test run'))" %
             str(SCRIPT_PATH.parent)],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(f"release_lock failed: {result.stderr}")

        run_git(repo, "pull", "--ff-only", "origin", "main")
        self.assertFalse(lock_path.exists())
        log = run_git(repo, "log", "--oneline", "-n", "3").stdout
        self.assertIn("TASK-101: released by agent-1", log)
```

- [ ] **Step 6: Run the integration test and confirm it fails**

```bash
python3 -m unittest scripts.test_task_tool_integration.TaskToolIntegrationTests.test_release_lock_removes_lock_and_pushes -v
```

Expected: `AttributeError: module 'task_tool' has no attribute 'release_lock'` (raised inside the subprocess — check the captured output for the message).

- [ ] **Step 7: Implement `release_lock` in `task_tool.py`**

Add after `write_lock` (~line 360) in `scripts/task_tool.py`:

```python
def release_lock(task_id: str, agent_id: str, note: str) -> bool:
    """Release a previously-claimed lock by removing the lock file and pushing.

    Used by agent_wrapper.py when a task run exits without writing a done
    marker, so another agent can pick the task up rather than waiting for the
    stale-lock timer. Returns True if the release was pushed successfully,
    False if the push lost a race (caller should re-sync and decide what to
    do next).
    """
    sync_main()
    lock_path = task_lock_path(task_id)
    if not lock_path.exists():
        return True
    lock_path.unlink()
    git("add", str(lock_path.relative_to(ROOT)), check=True)
    commit_message = f"{task_id}: released by {agent_id} ({note})"
    return commit_and_push(commit_message)
```

- [ ] **Step 8: Run the integration test and confirm it passes**

```bash
python3 -m unittest scripts.test_task_tool_integration.TaskToolIntegrationTests.test_release_lock_removes_lock_and_pushes -v
```

Expected: `OK`.

- [ ] **Step 9: Run the full script test suite to confirm no regressions**

```bash
python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

Expected: all tests pass.

- [ ] **Step 10: Commit**

```bash
git add scripts/task_tool.py scripts/test_task_tool.py scripts/test_task_tool_integration.py
git commit -m "$(cat <<'EOF'
task_tool: add release_lock and path helpers for Python agent wrapper

Adds module-level helpers the upcoming agent_wrapper.py needs for single-task
execution: task_lock_path/task_done_path for filesystem checks, is_wave_drained
for end-of-wave detection, and release_lock for handing an incomplete task back
to the pool when a claude run exits without marking it done.
EOF
)"
```

---

## Task 2: Create `agent_wrapper.py` skeleton and argument parsing

**Why:** Establish the file, its CLI surface (matching the bash wrapper's positional args so `attach-fleet.sh` only needs a one-line change), difficulty-pool → model mapping, and import-level wiring to `task_tool`. Pure functions only — no subprocess calls yet.

**Files:**
- Create: `scripts/agent_wrapper.py`
- Create: `scripts/test_agent_wrapper.py`

- [ ] **Step 1: Write failing unit tests for argument parsing**

Create `scripts/test_agent_wrapper.py`:

```python
#!/usr/bin/env python3

import pathlib
import sys
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

import agent_wrapper


class AgentWrapperArgsTests(unittest.TestCase):
    def test_parse_args_accepts_wave_and_pool(self) -> None:
        config = agent_wrapper.parse_args(["3", "EASY"])
        self.assertEqual(config.wave, 3)
        self.assertEqual(config.difficulty_pool, "EASY")
        self.assertEqual(config.model, "claude-sonnet-4-6")
        self.assertEqual(config.effort, "high")
        self.assertIsNone(config.max_tasks)

    def test_parse_args_accepts_max_tasks(self) -> None:
        config = agent_wrapper.parse_args(["3", "HARD", "2"])
        self.assertEqual(config.max_tasks, 2)
        self.assertEqual(config.model, "claude-opus-4-6[1m]")

    def test_parse_args_lowercases_pool_input(self) -> None:
        config = agent_wrapper.parse_args(["1", "easy"])
        self.assertEqual(config.difficulty_pool, "EASY")

    def test_parse_args_rejects_negative_wave(self) -> None:
        with self.assertRaises(agent_wrapper.WrapperConfigError):
            agent_wrapper.parse_args(["-1", "EASY"])

    def test_parse_args_rejects_bad_pool(self) -> None:
        with self.assertRaises(agent_wrapper.WrapperConfigError):
            agent_wrapper.parse_args(["1", "SPICY"])

    def test_parse_args_rejects_zero_max_tasks(self) -> None:
        with self.assertRaises(agent_wrapper.WrapperConfigError):
            agent_wrapper.parse_args(["1", "EASY", "0"])

    def test_wave_range_label_wave_zero(self) -> None:
        self.assertEqual(
            agent_wrapper.wave_range_label(0),
            "TASK-001 through TASK-099",
        )

    def test_wave_range_label_higher_wave(self) -> None:
        self.assertEqual(
            agent_wrapper.wave_range_label(3),
            "TASK-300 through TASK-399",
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests and confirm they fail with ImportError**

```bash
python3 -m unittest scripts.test_agent_wrapper -v
```

Expected: `ModuleNotFoundError: No module named 'agent_wrapper'`.

- [ ] **Step 3: Create `agent_wrapper.py` with the dataclass, argument parsing, and wave label helper**

Create `scripts/agent_wrapper.py`:

```python
#!/usr/bin/env python3
"""Python agent wrapper for the bqlite fleet.

Owns the autonomous loop end-to-end: claims tasks via task_tool, invokes
`claude -p --verbose` once per task, inspects git state to decide pass/retry/
release, handles NEEDS INPUT stdin interactions, and tracks per-agent batch
quotas. Replaces the previous scripts/agent-wrapper.sh + scripts/stop-agent-
loop.sh pair.
"""

from __future__ import annotations

import dataclasses
import pathlib
import sys
from typing import Optional


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import task_tool  # noqa: E402  (intentional after sys.path tweak)


class WrapperConfigError(RuntimeError):
    """User-visible configuration error; mapped to non-zero exit code."""


_DIFFICULTY_POOLS = {
    "EASY": {"model": "claude-sonnet-4-6", "effort": "high", "tag": "[EASY]"},
    "HARD": {"model": "claude-opus-4-6[1m]", "effort": "high", "tag": "[HARD]"},
}


@dataclasses.dataclass(frozen=True)
class WrapperConfig:
    wave: int
    difficulty_pool: str
    difficulty_tag: str
    model: str
    effort: str
    max_tasks: Optional[int]


def parse_args(argv: list[str]) -> WrapperConfig:
    if len(argv) < 2 or len(argv) > 3:
        raise WrapperConfigError(
            "usage: agent_wrapper.py <wave> <difficulty_pool> [max_tasks]"
        )

    wave_raw = argv[0]
    pool_raw = argv[1].upper()
    max_tasks_raw = argv[2] if len(argv) == 3 else None

    if not wave_raw.isdigit():
        raise WrapperConfigError(
            f"wave must be a non-negative integer, got {wave_raw!r}"
        )
    wave = int(wave_raw)

    if pool_raw not in _DIFFICULTY_POOLS:
        raise WrapperConfigError(
            f"difficulty_pool must be EASY or HARD, got {argv[1]!r}"
        )
    pool_cfg = _DIFFICULTY_POOLS[pool_raw]

    max_tasks: Optional[int] = None
    if max_tasks_raw is not None:
        if not max_tasks_raw.isdigit() or int(max_tasks_raw) < 1:
            raise WrapperConfigError(
                f"max_tasks must be a positive integer, got {max_tasks_raw!r}"
            )
        max_tasks = int(max_tasks_raw)

    return WrapperConfig(
        wave=wave,
        difficulty_pool=pool_raw,
        difficulty_tag=pool_cfg["tag"],
        model=pool_cfg["model"],
        effort=pool_cfg["effort"],
        max_tasks=max_tasks,
    )


def wave_range_label(wave: int) -> str:
    if wave == 0:
        return "TASK-001 through TASK-099"
    return f"TASK-{wave}00 through TASK-{wave}99"
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
python3 -m unittest scripts.test_agent_wrapper -v
```

Expected: `OK` — 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/agent_wrapper.py scripts/test_agent_wrapper.py
git commit -m "$(cat <<'EOF'
Add agent_wrapper.py skeleton with arg parsing and pool config

First step of the Python wrapper refactor. Ports argument validation and
difficulty-pool → model mapping from scripts/agent-wrapper.sh so downstream
tasks can build the loop on top without regressing CLI compatibility with
attach-fleet.sh.
EOF
)"
```

---

## Task 3: Add task prompt composition and NEEDS INPUT scanning

**Why:** These are pure functions the main loop will call. Isolating them is cheap and lets us unit-test the exact prompt string we send to claude and the substring detection logic without stubbing subprocesses.

**Files:**
- Modify: `scripts/agent_wrapper.py` (append after `wave_range_label`)
- Modify: `scripts/test_agent_wrapper.py` (append new test class)

- [ ] **Step 1: Write failing tests for prompt composition and scanning**

Append to `scripts/test_agent_wrapper.py` (before `if __name__ == "__main__":`):

```python
class AgentWrapperPromptTests(unittest.TestCase):
    def _sample_task(self) -> dict:
        return {
            "task_id": "TASK-305",
            "title": "Implement fancy operator",
            "wave": 3,
            "tags": ["HARD", "IMPL"],
            "depends_on": ["TASK-304"],
            "difficulty": "HARD",
        }

    def test_task_prompt_includes_task_id_and_title(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("TASK-305", prompt)
        self.assertIn("Implement fancy operator", prompt)
        self.assertIn("agent-2", prompt)

    def test_task_prompt_instructs_to_read_design_docs(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("docs/design", prompt)
        self.assertIn("before writing code", prompt.lower())

    def test_task_prompt_instructs_to_mark_done_and_exit(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("tasks/active/TASK-305.lock", prompt)
        self.assertIn("tasks/completed/TASK-305.done", prompt)
        self.assertIn("Do not claim another task", prompt)

    def test_task_prompt_mentions_needs_input_contract(self) -> None:
        prompt = agent_wrapper.task_prompt(self._sample_task(), "agent-2")
        self.assertIn("[NEEDS INPUT]", prompt)

    def test_scan_needs_input_detects_marker(self) -> None:
        output = "... working on task ...\n[NEEDS INPUT] Which operator variant?\n"
        self.assertTrue(agent_wrapper.has_needs_input(output))

    def test_scan_needs_input_false_without_marker(self) -> None:
        output = "... working on task ...\ndone.\n"
        self.assertFalse(agent_wrapper.has_needs_input(output))

    def test_system_prompt_contains_agent_and_pool(self) -> None:
        text = agent_wrapper.system_prompt("agent-3", "HARD", "[HARD]")
        self.assertIn("agent-3", text)
        self.assertIn("HARD", text)
        self.assertIn("AGENTS.md", text)
```

- [ ] **Step 2: Run the tests and confirm they fail with AttributeError**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperPromptTests -v
```

Expected: errors like `AttributeError: module 'agent_wrapper' has no attribute 'task_prompt'`.

- [ ] **Step 3: Implement the three helpers in `agent_wrapper.py`**

Append to `scripts/agent_wrapper.py` (after `wave_range_label`):

```python
NEEDS_INPUT_MARKER = "[NEEDS INPUT]"


def system_prompt(agent_name: str, difficulty_pool: str, difficulty_tag: str) -> str:
    return (
        f"You are {agent_name}, an autonomous agent building bqlite. "
        f"Read AGENTS.md for your complete operating protocol. "
        f"Your assigned difficulty pool is {difficulty_pool}; "
        f"only claim tasks tagged {difficulty_tag} unless a human explicitly "
        f"changes your assignment. The wrapper handles task claiming, batching, "
        f"and restart logic — your job is to execute one task at a time from "
        f"start to merge."
    )


def task_prompt(task: dict, agent_name: str) -> str:
    task_id = task["task_id"]
    title = task["title"]
    return (
        f"You ({agent_name}) have already been assigned {task_id}: {title}. "
        f"The lock file at tasks/active/{task_id}.lock is yours. "
        f"Before writing code, read docs/design/INDEX.md and any per-feature "
        f"design doc under docs/design/ that covers this task. "
        f"Implement the task following the checkpoint discipline in AGENTS.md: "
        f"each checkpoint must pass scripts/local-ci.sh, be reviewed by a code-"
        f"review subagent, and be fast-forward merged to main before the next "
        f"checkpoint starts. "
        f"When the final checkpoint is merged, move tasks/active/{task_id}.lock "
        f"to tasks/completed/{task_id}.done (adding a completed_at field), "
        f"commit, and push. Then end your turn. "
        f"Do not claim another task — the wrapper will handle the next one. "
        f"If you hit a design or architecture decision you cannot resolve alone, "
        f"emit {NEEDS_INPUT_MARKER} followed by your question on its own line "
        f"and then end your turn; the wrapper will forward a human reply back "
        f"into your session."
    )


def has_needs_input(output: str) -> bool:
    return NEEDS_INPUT_MARKER in output
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperPromptTests -v
```

Expected: `OK` — 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/agent_wrapper.py scripts/test_agent_wrapper.py
git commit -m "agent_wrapper: add task/system prompt builders and NEEDS INPUT scan"
```

---

## Task 4: Add claude invocation that streams output and captures it

**Why:** The wrapper needs to `subprocess.Popen` claude, mirror its stdout to the cmux tab in real time, *and* accumulate the full output so it can be scanned for `[NEEDS INPUT]` after exit. This is the one piece of the wrapper that touches the real world, so we isolate it behind a seam that the integration tests can swap out with a fake binary.

**Files:**
- Modify: `scripts/agent_wrapper.py`
- Modify: `scripts/test_agent_wrapper.py`

- [ ] **Step 1: Write a failing test that stubs `claude` via a fake script on PATH**

Append to `scripts/test_agent_wrapper.py`:

```python
import os
import subprocess
import tempfile
import textwrap


class AgentWrapperClaudeInvocationTests(unittest.TestCase):
    def _make_fake_claude(self, script_body: str) -> tuple[pathlib.Path, dict]:
        tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="fake-claude-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(tmpdir)], check=False)
        )
        fake = tmpdir / "claude"
        fake.write_text("#!/usr/bin/env bash\n" + script_body)
        fake.chmod(0o755)
        env = os.environ.copy()
        env["PATH"] = f"{tmpdir}:{env.get('PATH', '')}"
        return fake, env

    def test_run_claude_captures_stdout(self) -> None:
        _, env = self._make_fake_claude('echo "hello from claude"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="ignored",
            model="fake-model",
            effort="high",
            system_prompt_text="fake sys",
            resume=False,
            env=env,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("hello from claude", result.output)

    def test_run_claude_sets_resume_flag(self) -> None:
        # Fake claude echoes its argv so we can inspect flags.
        _, env = self._make_fake_claude('printf "ARGV:%s\\n" "$@"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="do the thing",
            model="fake-model",
            effort="high",
            system_prompt_text=None,
            resume=True,
            env=env,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("ARGV:-c", result.output)
        self.assertIn("ARGV:do the thing", result.output)
        self.assertNotIn("--append-system-prompt", result.output)

    def test_run_claude_passes_system_prompt_on_fresh(self) -> None:
        _, env = self._make_fake_claude('printf "ARGV:%s\\n" "$@"\nexit 0\n')
        result = agent_wrapper.run_claude(
            prompt="fresh work",
            model="fake-model",
            effort="high",
            system_prompt_text="you are fake agent",
            resume=False,
            env=env,
        )
        self.assertIn("ARGV:--append-system-prompt", result.output)
        self.assertIn("ARGV:you are fake agent", result.output)

    def test_run_claude_propagates_nonzero_exit(self) -> None:
        _, env = self._make_fake_claude('echo "boom" >&2\nexit 7\n')
        result = agent_wrapper.run_claude(
            prompt="ignored",
            model="fake-model",
            effort="high",
            system_prompt_text="fake sys",
            resume=False,
            env=env,
        )
        self.assertEqual(result.returncode, 7)
```

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperClaudeInvocationTests -v
```

Expected: `AttributeError: module 'agent_wrapper' has no attribute 'run_claude'`.

- [ ] **Step 3: Implement `run_claude` and its result dataclass**

Append to `scripts/agent_wrapper.py`:

```python
import os
import subprocess


@dataclasses.dataclass
class ClaudeRunResult:
    returncode: int
    output: str


def run_claude(
    *,
    prompt: str,
    model: str,
    effort: str,
    system_prompt_text: Optional[str],
    resume: bool,
    env: Optional[dict[str, str]] = None,
) -> ClaudeRunResult:
    """Run `claude -p --verbose` once, streaming stdout live to our own
    stdout and capturing the full output for later scanning.

    When `resume` is True, passes `-c` and omits `--append-system-prompt` (a
    resumed session inherits the prior system prompt). When False, passes the
    provided `system_prompt_text` via `--append-system-prompt`.
    """
    args = [
        "claude",
        "-p",
        "--verbose",
        "--model",
        model,
        "--effort",
        effort,
        "--permission-mode",
        "bypassPermissions",
    ]
    if resume:
        args.append("-c")
    elif system_prompt_text is not None:
        args.extend(["--append-system-prompt", system_prompt_text])
    args.append(prompt)

    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env if env is not None else os.environ.copy(),
    )
    assert proc.stdout is not None

    captured: list[str] = []
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        captured.append(line)
    proc.wait()
    return ClaudeRunResult(returncode=proc.returncode, output="".join(captured))
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperClaudeInvocationTests -v
```

Expected: `OK` — 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/agent_wrapper.py scripts/test_agent_wrapper.py
git commit -m "agent_wrapper: add run_claude with live-streaming stdout capture"
```

---

## Task 5: Add per-task execution with retry-on-incomplete and NEEDS INPUT handling

**Why:** This is the state machine that turns a single claim into a done marker (or a released lock). It invokes `run_claude`, inspects git state via the Task 1 helpers, decides whether to retry with `-c`, loops to capture human replies when the agent emits `[NEEDS INPUT]`, and reports a final outcome to the main loop.

**Files:**
- Modify: `scripts/agent_wrapper.py`
- Modify: `scripts/test_agent_wrapper.py`

- [ ] **Step 1: Write failing tests for `execute_task`**

Append to `scripts/test_agent_wrapper.py`:

```python
class FakeClaude:
    def __init__(self, scripted_runs: list[dict]) -> None:
        self.scripted = list(scripted_runs)
        self.calls: list[dict] = []

    def __call__(
        self,
        *,
        prompt: str,
        model: str,
        effort: str,
        system_prompt_text,
        resume: bool,
        env=None,
    ) -> "agent_wrapper.ClaudeRunResult":
        self.calls.append(
            {
                "prompt": prompt,
                "resume": resume,
                "system_prompt_text": system_prompt_text,
            }
        )
        if not self.scripted:
            raise AssertionError("FakeClaude ran out of scripted runs")
        scripted = self.scripted.pop(0)
        side_effect = scripted.get("side_effect")
        if side_effect is not None:
            side_effect()
        return agent_wrapper.ClaudeRunResult(
            returncode=scripted.get("returncode", 0),
            output=scripted.get("output", ""),
        )


class AgentWrapperExecuteTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="agent-wrapper-ut-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(self.tmp)], check=False)
        )
        (self.tmp / "tasks" / "active").mkdir(parents=True)
        (self.tmp / "tasks" / "completed").mkdir(parents=True)
        self._orig_active = agent_wrapper.task_tool.ACTIVE_DIR
        self._orig_completed = agent_wrapper.task_tool.COMPLETED_DIR
        agent_wrapper.task_tool.ACTIVE_DIR = self.tmp / "tasks" / "active"
        agent_wrapper.task_tool.COMPLETED_DIR = self.tmp / "tasks" / "completed"
        self.addCleanup(self._restore_dirs)

        self.task = {
            "task_id": "TASK-101",
            "title": "Ship thing",
            "wave": 1,
            "tags": ["EASY"],
            "depends_on": [],
            "difficulty": "EASY",
        }
        (self.tmp / "tasks" / "active" / "TASK-101.lock").write_text("{}")

    def _restore_dirs(self) -> None:
        agent_wrapper.task_tool.ACTIVE_DIR = self._orig_active
        agent_wrapper.task_tool.COMPLETED_DIR = self._orig_completed

    def _mark_done(self) -> None:
        (self.tmp / "tasks" / "active" / "TASK-101.lock").unlink()
        (self.tmp / "tasks" / "completed" / "TASK-101.done").write_text("{}")

    def test_execute_task_returns_completed_when_done_marker_appears(self) -> None:
        fake = FakeClaude([
            {"side_effect": self._mark_done, "output": "all done\n"},
        ])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: "unused",
        )
        self.assertEqual(outcome, "completed")
        self.assertEqual(len(fake.calls), 1)
        self.assertFalse(fake.calls[0]["resume"])

    def test_execute_task_retries_once_on_incomplete(self) -> None:
        fake = FakeClaude([
            {"output": "stopped early\n"},
            {"side_effect": self._mark_done, "output": "retry wrap\n"},
        ])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: "unused",
        )
        self.assertEqual(outcome, "completed")
        self.assertEqual(len(fake.calls), 2)
        self.assertTrue(fake.calls[1]["resume"])

    def test_execute_task_returns_incomplete_after_retry_also_fails(self) -> None:
        fake = FakeClaude([
            {"output": "first try stopped\n"},
            {"output": "second try also stopped\n"},
        ])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: "unused",
        )
        self.assertEqual(outcome, "incomplete")
        self.assertEqual(len(fake.calls), 2)

    def test_execute_task_handles_needs_input_with_resume(self) -> None:
        fake = FakeClaude([
            {"output": "[NEEDS INPUT] which variant?\n"},
            {"side_effect": self._mark_done, "output": "got reply, finishing\n"},
        ])
        replies = iter(["use variant A"])
        outcome = agent_wrapper.execute_task(
            self.task,
            agent_name="agent-1",
            model="m",
            effort="high",
            claude_runner=fake,
            read_human_reply=lambda: next(replies),
        )
        self.assertEqual(outcome, "completed")
        self.assertEqual(len(fake.calls), 2)
        self.assertTrue(fake.calls[1]["resume"])
        self.assertEqual(fake.calls[1]["prompt"], "use variant A")
```

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperExecuteTaskTests -v
```

Expected: `AttributeError: module 'agent_wrapper' has no attribute 'execute_task'`.

- [ ] **Step 3: Implement `execute_task`**

Append to `scripts/agent_wrapper.py`:

```python
from typing import Callable


def _task_state(task_id: str) -> str:
    if task_tool.task_done_path(task_id).exists():
        return "completed"
    if task_tool.task_lock_path(task_id).exists():
        return "lock_held"
    return "missing"


def _run_once_with_needs_input_loop(
    *,
    initial_prompt: str,
    initial_system_prompt: Optional[str],
    initial_resume: bool,
    claude_runner: Callable[..., "ClaudeRunResult"],
    read_human_reply: Callable[[], str],
    model: str,
    effort: str,
) -> "ClaudeRunResult":
    """Invoke claude, then keep re-invoking with -c and a captured human reply
    for as long as the output contains [NEEDS INPUT]. Returns the final run."""
    result = claude_runner(
        prompt=initial_prompt,
        model=model,
        effort=effort,
        system_prompt_text=initial_system_prompt,
        resume=initial_resume,
    )
    while has_needs_input(result.output):
        reply = read_human_reply()
        result = claude_runner(
            prompt=reply,
            model=model,
            effort=effort,
            system_prompt_text=None,
            resume=True,
        )
    return result


def execute_task(
    task: dict,
    *,
    agent_name: str,
    model: str,
    effort: str,
    claude_runner: Callable[..., "ClaudeRunResult"],
    read_human_reply: Callable[[], str],
) -> str:
    """Run one claim → done cycle for a single task.

    Returns "completed" if the done marker is present after the run,
    "incomplete" if the lock is still held after a retry, or "missing" if
    something else removed the lock without producing a done marker (unusual
    — caller should escalate).
    """
    task_id = task["task_id"]
    sys_prompt_text = system_prompt(agent_name, task["difficulty"], f"[{task['difficulty']}]")
    prompt = task_prompt(task, agent_name)

    _run_once_with_needs_input_loop(
        initial_prompt=prompt,
        initial_system_prompt=sys_prompt_text,
        initial_resume=False,
        claude_runner=claude_runner,
        read_human_reply=read_human_reply,
        model=model,
        effort=effort,
    )

    state = _task_state(task_id)
    if state == "completed":
        return "completed"

    if state == "lock_held":
        resume_prompt = (
            f"You are resuming work on {task_id}. The lock file is still in "
            f"tasks/active/. Finish the task: run scripts/local-ci.sh, review, "
            f"merge the final checkpoint to main, then move the lock to "
            f"tasks/completed/{task_id}.done and push. Do not claim another task."
        )
        _run_once_with_needs_input_loop(
            initial_prompt=resume_prompt,
            initial_system_prompt=None,
            initial_resume=True,
            claude_runner=claude_runner,
            read_human_reply=read_human_reply,
            model=model,
            effort=effort,
        )
        state = _task_state(task_id)
        if state == "completed":
            return "completed"
        return "incomplete"

    return "missing"
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperExecuteTaskTests -v
```

Expected: `OK` — 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/agent_wrapper.py scripts/test_agent_wrapper.py
git commit -m "agent_wrapper: add execute_task with retry and NEEDS INPUT loop"
```

---

## Task 6: Add the top-level main loop

**Why:** Wire together `parse_args` → `claim_next` → `execute_task` → counter/backoff/wave-done. This is the piece `attach-fleet.sh` actually invokes.

**Files:**
- Modify: `scripts/agent_wrapper.py`
- Modify: `scripts/test_agent_wrapper.py`

- [ ] **Step 1: Write failing tests for `run_fleet_loop`**

Append to `scripts/test_agent_wrapper.py`:

```python
class AgentWrapperMainLoopTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="agent-wrapper-loop-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(self.tmp)], check=False)
        )
        (self.tmp / "tasks" / "active").mkdir(parents=True)
        (self.tmp / "tasks" / "completed").mkdir(parents=True)
        self._orig_active = agent_wrapper.task_tool.ACTIVE_DIR
        self._orig_completed = agent_wrapper.task_tool.COMPLETED_DIR
        agent_wrapper.task_tool.ACTIVE_DIR = self.tmp / "tasks" / "active"
        agent_wrapper.task_tool.COMPLETED_DIR = self.tmp / "tasks" / "completed"
        self.addCleanup(self._restore_dirs)

    def _restore_dirs(self) -> None:
        agent_wrapper.task_tool.ACTIVE_DIR = self._orig_active
        agent_wrapper.task_tool.COMPLETED_DIR = self._orig_completed

    def _make_task(self, task_id: str, difficulty: str = "EASY") -> dict:
        return {
            "task_id": task_id,
            "title": f"Do {task_id}",
            "wave": 1,
            "tags": [difficulty],
            "depends_on": [],
            "difficulty": difficulty,
        }

    def test_run_fleet_loop_exits_after_max_tasks(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY", "2"])

        def fake_claim_next(**kwargs) -> dict:
            task_id = f"TASK-10{fake_claim_next.counter}"
            fake_claim_next.counter += 1
            (self.tmp / "tasks" / "active" / f"{task_id}.lock").write_text("{}")
            return {"status": "claimed", "task": self._make_task(task_id)}
        fake_claim_next.counter = 1

        def fake_execute_task(task, **kwargs) -> str:
            tid = task["task_id"]
            (self.tmp / "tasks" / "active" / f"{tid}.lock").unlink()
            (self.tmp / "tasks" / "completed" / f"{tid}.done").write_text("{}")
            return "completed"

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=fake_execute_task,
            release_lock=lambda *a, **k: True,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "batch_complete")
        self.assertEqual(fake_claim_next.counter, 3)  # 2 calls made

    def test_run_fleet_loop_exits_on_wave_drained(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY"])
        states = iter([
            {"status": "no_claimable", "claimable": [], "blocked": [],
             "active": [], "completed_count": 5,
             "other_pool_claimable": [], "wave": 1, "difficulty": "EASY"},
        ])
        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=lambda **k: next(states),
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "wave_complete")

    def test_run_fleet_loop_releases_lock_on_incomplete_task(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY", "1"])
        claim_calls = iter([
            {"status": "claimed", "task": self._make_task("TASK-101")},
        ])

        def fake_claim_next(**kwargs) -> dict:
            return next(claim_calls)

        released: list[str] = []

        def fake_release(task_id, agent_id, note):
            released.append(task_id)
            return True

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=lambda *a, **k: "incomplete",
            release_lock=fake_release,
            sleep=lambda _: None,
        )
        self.assertEqual(outcome, "batch_complete")  # quota was 1
        self.assertEqual(released, ["TASK-101"])

    def test_run_fleet_loop_backs_off_then_retries_on_no_claimable(self) -> None:
        config = agent_wrapper.parse_args(["1", "EASY", "1"])

        scenarios = iter([
            # no_claimable with blocked work remaining → back off and retry
            {"status": "no_claimable", "claimable": [],
             "blocked": [{"task_id": "TASK-199"}], "active": [],
             "completed_count": 0, "other_pool_claimable": [],
             "wave": 1, "difficulty": "EASY"},
            # Now something claimable appears
            {"status": "claimed", "task": self._make_task("TASK-102")},
        ])

        def fake_claim_next(**kwargs) -> dict:
            return next(scenarios)

        sleeps: list[float] = []

        outcome = agent_wrapper.run_fleet_loop(
            config,
            agent_name="agent-1",
            claim_next=fake_claim_next,
            execute_task=lambda *a, **k: "completed",
            release_lock=lambda *a, **k: True,
            sleep=lambda seconds: sleeps.append(seconds),
        )
        self.assertEqual(outcome, "batch_complete")
        self.assertEqual(sleeps, [120])  # first step of 2/5/10/20/60-min ladder
```

- [ ] **Step 2: Run the tests and confirm they fail**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperMainLoopTests -v
```

Expected: `AttributeError: module 'agent_wrapper' has no attribute 'run_fleet_loop'`.

- [ ] **Step 3: Implement `run_fleet_loop` and `main`**

Append to `scripts/agent_wrapper.py`:

```python
BACKOFF_SCHEDULE_SECONDS = [120, 300, 600, 1200, 3600]


def _wave_range_to_status_banner(config: WrapperConfig, agent_name: str) -> str:
    return (
        f"=== {agent_name} launching on Wave {config.wave} "
        f"({wave_range_label(config.wave)}) "
        f"pool={config.difficulty_pool} model={config.model} ==="
    )


def _classified_is_drained(payload: dict) -> bool:
    return (
        not payload.get("claimable")
        and not payload.get("blocked")
        and not payload.get("active")
        and not payload.get("other_pool_claimable")
        and not payload.get("claimable_missing_difficulty")
    )


def run_fleet_loop(
    config: WrapperConfig,
    *,
    agent_name: str,
    claim_next: Callable[..., dict],
    execute_task: Callable[..., str],
    release_lock: Callable[..., bool],
    sleep: Callable[[float], None],
) -> str:
    completed = 0
    backoff_idx = 0
    print(_wave_range_to_status_banner(config, agent_name), flush=True)

    while True:
        if config.max_tasks is not None and completed >= config.max_tasks:
            print(
                f"=== {agent_name}: batch complete ({completed}/{config.max_tasks} "
                f"tasks done). Exiting. ===",
                flush=True,
            )
            return "batch_complete"

        result = claim_next(
            wave=config.wave,
            difficulty=config.difficulty_pool,
            agent_id=agent_name,
        )
        status = result.get("status")

        if status == "claimed":
            task = result["task"]
            outcome = execute_task(
                task,
                agent_name=agent_name,
                model=config.model,
                effort=config.effort,
                claude_runner=run_claude,
                read_human_reply=read_human_reply,
            )
            if outcome == "completed":
                completed += 1
                backoff_idx = 0
                continue
            print(
                f"=== {agent_name}: {task['task_id']} did not complete "
                f"(outcome={outcome}); releasing lock ===",
                flush=True,
            )
            release_lock(task["task_id"], agent_name, f"outcome={outcome}")
            completed += 1  # quota counts attempts, not successes
            continue

        if status == "no_claimable":
            if _classified_is_drained(result):
                print(
                    f"=== {agent_name}: wave {config.wave} drained for pool "
                    f"{config.difficulty_pool}. Exiting. ===",
                    flush=True,
                )
                return "wave_complete"
            delay = BACKOFF_SCHEDULE_SECONDS[
                min(backoff_idx, len(BACKOFF_SCHEDULE_SECONDS) - 1)
            ]
            print(
                f"=== {agent_name}: no claimable tasks, sleeping {delay}s ===",
                flush=True,
            )
            sleep(delay)
            backoff_idx += 1
            continue

        if status == "missing_difficulty":
            print(
                f"=== {agent_name}: [NEEDS INPUT] wave {config.wave} has "
                f"untagged claimable tasks; please tag or retire them. Retrying "
                f"in 60s. ===",
                flush=True,
            )
            sleep(60)
            continue

        if status == "error":
            print(
                f"=== {agent_name}: task_tool error: {result.get('error')}. "
                f"Retrying in 60s. ===",
                flush=True,
            )
            sleep(60)
            continue

        print(
            f"=== {agent_name}: unexpected claim_next status {status!r}; "
            f"sleeping 60s then retrying ===",
            flush=True,
        )
        sleep(60)


def read_human_reply() -> str:
    """Prompt the human for a reply to a NEEDS INPUT question and return one
    line. Reads from /dev/tty directly so it works when stdout is being
    streamed or tee'd."""
    sys.stdout.write(
        "\n"
        "=== [NEEDS INPUT] detected — type your one-line reply below and press "
        "Enter. The wrapper will resume the claude session with your reply as "
        "the next user message. ===\n> "
    )
    sys.stdout.flush()
    try:
        with open("/dev/tty", "r") as tty:
            return tty.readline().strip()
    except OSError:
        return sys.stdin.readline().strip()


def main(argv: list[str]) -> int:
    try:
        config = parse_args(argv)
    except WrapperConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    agent_name = os.environ.get("AGENT_ID", "agent")

    try:
        outcome = run_fleet_loop(
            config,
            agent_name=agent_name,
            claim_next=task_tool.claim_next,
            execute_task=execute_task,
            release_lock=task_tool.release_lock,
            sleep=_sleep_with_tty_drain,
        )
    except KeyboardInterrupt:
        print(f"\n=== {agent_name}: interrupted. Exiting. ===", flush=True)
        return 130

    return 0 if outcome in {"wave_complete", "batch_complete"} else 1


def _sleep_with_tty_drain(duration: float) -> None:
    """Drain stray keystrokes from the TTY while sleeping so ambient typing
    in the cmux tab doesn't bleed into the next claude invocation as stdin."""
    import select
    try:
        with open("/dev/tty", "r") as tty:
            remaining = duration
            while remaining > 0:
                ready, _, _ = select.select([tty], [], [], min(remaining, 1.0))
                if ready:
                    tty.readline()
                remaining -= 1.0
    except OSError:
        import time
        time.sleep(duration)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

- [ ] **Step 4: Run the main-loop tests and confirm they pass**

```bash
python3 -m unittest scripts.test_agent_wrapper.AgentWrapperMainLoopTests -v
```

Expected: `OK` — 4 tests pass.

- [ ] **Step 5: Run the full script test suite**

```bash
python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

Expected: all existing tests still pass alongside the new ones.

- [ ] **Step 6: Commit**

```bash
git add scripts/agent_wrapper.py scripts/test_agent_wrapper.py
git commit -m "agent_wrapper: add main loop with backoff, quota, wave-drained exit"
```

---

## Task 7: End-to-end integration test with a fake `claude` binary

**Why:** The unit tests cover the wrapper's state machine with injected fakes, but we want at least one path-level test that actually invokes the wrapper as a subprocess against a tmp git repo with a stubbed `claude` on `PATH`. This catches integration issues (module import, path resolution, CLI exit codes) that unit tests can miss.

**Files:**
- Create: `scripts/test_agent_wrapper_integration.py`

- [ ] **Step 1: Write the integration test**

Create `scripts/test_agent_wrapper_integration.py`:

```python
#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
WRAPPER_PATH = SCRIPTS_DIR / "agent_wrapper.py"


def run_git(cwd: pathlib.Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )


class AgentWrapperIntegrationTests(unittest.TestCase):
    def make_repo(self, tasks_md: str) -> pathlib.Path:
        temp_root = pathlib.Path(tempfile.mkdtemp(prefix="agent-wrapper-it-"))
        self.addCleanup(
            lambda: subprocess.run(["rm", "-rf", str(temp_root)], check=False)
        )

        origin = temp_root / "origin.git"
        seed = temp_root / "seed"
        work = temp_root / "work"

        run_git(temp_root, "init", "--bare", origin.name)
        run_git(temp_root, "init", "-b", "main", seed.name)
        run_git(seed, "config", "user.name", "Wrapper Test")
        run_git(seed, "config", "user.email", "wrapper@example.com")

        (seed / "TASKS.md").write_text(textwrap.dedent(tasks_md).lstrip())
        (seed / "tasks" / "active").mkdir(parents=True, exist_ok=True)
        (seed / "tasks" / "completed").mkdir(parents=True, exist_ok=True)
        (seed / "tasks" / "active" / ".gitkeep").write_text("")
        (seed / "tasks" / "completed" / ".gitkeep").write_text("")

        run_git(seed, "add", ".")
        run_git(seed, "commit", "-m", "seed")
        run_git(seed, "remote", "add", "origin", str(origin))
        run_git(seed, "push", "-u", "origin", "main")
        run_git(origin, "symbolic-ref", "HEAD", "refs/heads/main")

        run_git(temp_root, "clone", str(origin), work.name)
        run_git(work, "config", "user.name", "Wrapper Test")
        run_git(work, "config", "user.email", "wrapper@example.com")
        return work

    def install_fake_claude(self, repo: pathlib.Path) -> pathlib.Path:
        """Install a fake `claude` executable that reads the last line of its
        final argument (the prompt), finds a TASK-NNN ID, and moves the lock
        file to completed. Outputs to stdout so the wrapper can capture it.
        """
        bin_dir = repo.parent / "bin"
        bin_dir.mkdir(exist_ok=True)
        fake = bin_dir / "claude"
        fake.write_text(textwrap.dedent('''\
            #!/usr/bin/env bash
            set -euo pipefail
            prompt="${!#}"   # last positional arg = prompt
            echo "fake-claude received prompt"
            task_id=$(echo "$prompt" | grep -oE 'TASK-[0-9]{3}' | head -1 || true)
            if [ -z "$task_id" ]; then
              echo "fake-claude: no task id in prompt"
              exit 0
            fi
            repo_root=$(git rev-parse --show-toplevel)
            lock="$repo_root/tasks/active/${task_id}.lock"
            done_file="$repo_root/tasks/completed/${task_id}.done"
            if [ -f "$lock" ]; then
              git -C "$repo_root" mv "$lock" "$done_file"
              git -C "$repo_root" commit -m "${task_id}: completed" >/dev/null
              git -C "$repo_root" push origin main >/dev/null
              echo "fake-claude: marked $task_id done"
            fi
        '''))
        fake.chmod(0o755)
        return bin_dir

    def test_single_task_quota_runs_to_completion(self) -> None:
        repo = self.make_repo("""
            ### TASK-101: [EASY][IMPL] Single simple task
            **Depends on**: none
        """)
        bin_dir = self.install_fake_claude(repo)

        env = os.environ.copy()
        env["PATH"] = f"{bin_dir}:{env['PATH']}"
        env["BQLITE_TASK_TOOL_ROOT"] = str(repo)
        env["AGENT_ID"] = "agent-test-1"

        result = subprocess.run(
            [sys.executable, str(WRAPPER_PATH), "1", "EASY", "1"],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode != 0:
            self.fail(
                f"wrapper exited {result.returncode}\n"
                f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
            )

        run_git(repo, "pull", "--ff-only", "origin", "main")
        done = repo / "tasks" / "completed" / "TASK-101.done"
        self.assertTrue(done.exists(), "expected TASK-101.done after wrapper run")
        self.assertIn("batch complete", result.stdout)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the integration test**

```bash
python3 -m unittest scripts.test_agent_wrapper_integration -v
```

Expected: `OK` — 1 test passes. If it fails, inspect captured stdout/stderr; the fake-claude script assumes CWD is inside the repo and uses `git rev-parse --show-toplevel` to find the checkout.

- [ ] **Step 3: Run the entire script test suite one more time**

```bash
python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add scripts/test_agent_wrapper_integration.py
git commit -m "agent_wrapper: add end-to-end integration test with fake claude"
```

---

## Task 8: Flip `attach-fleet.sh` to invoke the Python wrapper

**Why:** Hand control from the bash wrapper to the Python one. Keep the bash wrapper file in the tree for this task so a bad roll-out can be reverted with a one-line change.

**Files:**
- Modify: `scripts/attach-fleet.sh` (line ~125)

- [ ] **Step 1: Update the `agent_cmd` function to invoke python3**

In `scripts/attach-fleet.sh`, change the current `agent_cmd` function from:

```bash
agent_cmd() {
  local container="$1"
  local difficulty_pool="$2"
  local args="${WAVE} ${difficulty_pool}"
  if [ -n "$MAX_TASKS" ]; then
    args="${args} ${MAX_TASKS}"
  fi
  echo "docker exec -it -e IS_SANDBOX=1 -e TASK_DIFFICULTY_POOL=${difficulty_pool} -w /workspace ${container} /workspace/scripts/agent-wrapper.sh ${args}"
}
```

to:

```bash
agent_cmd() {
  local container="$1"
  local difficulty_pool="$2"
  local args="${WAVE} ${difficulty_pool}"
  if [ -n "$MAX_TASKS" ]; then
    args="${args} ${MAX_TASKS}"
  fi
  echo "docker exec -it -e IS_SANDBOX=1 -e TASK_DIFFICULTY_POOL=${difficulty_pool} -w /workspace ${container} python3 /workspace/scripts/agent_wrapper.py ${args}"
}
```

Update the comment immediately above the function from `"The wrapper script runs claude in a restart loop driven by the Stop hook's control markers."` to `"The wrapper runs claude once per task; task selection, batching, and NEEDS INPUT capture are owned by agent_wrapper.py."`.

- [ ] **Step 2: Verify the script still parses cleanly**

```bash
bash -n scripts/attach-fleet.sh
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add scripts/attach-fleet.sh
git commit -m "attach-fleet: invoke Python agent_wrapper.py instead of the bash wrapper"
```

---

## Task 9: Remove the Stop hook from launch-fleet.sh and delete the old scripts

**Why:** With the Python wrapper in place and attach-fleet.sh pointed at it, the old bash wrapper and the Stop-hook marker protocol are dead weight. Deleting them in a separate commit keeps this change reversible and the diff obvious.

**Files:**
- Modify: `scripts/launch-fleet.sh`
- Delete: `scripts/agent-wrapper.sh`
- Delete: `scripts/stop-agent-loop.sh`

- [ ] **Step 1: Remove the `Stop` hook from the settings.json patch in launch-fleet.sh**

In `scripts/launch-fleet.sh` change this line:

```bash
      PATCH='{"skipDangerousModePermissionPrompt":true,"permissions":{"defaultMode":"bypassPermissions"},"hooks":{"Notification":[{"matcher":"","hooks":[{"type":"command","command":"/root/.claude/hooks/cmux-notify.sh"}]}],"Stop":[{"matcher":"","hooks":[{"type":"command","command":"/root/.claude/hooks/stop-agent-loop.sh"}]}]}}'
```

to:

```bash
      PATCH='{"skipDangerousModePermissionPrompt":true,"permissions":{"defaultMode":"bypassPermissions"},"hooks":{"Notification":[{"matcher":"","hooks":[{"type":"command","command":"/root/.claude/hooks/cmux-notify.sh"}]}]}}'
```

- [ ] **Step 2: Remove the stop-hook install line**

Delete this line from `scripts/launch-fleet.sh` (inside the inline `bash -c`):

```bash
      install -m 755 /workspace/scripts/stop-agent-loop.sh /root/.claude/hooks/stop-agent-loop.sh &&
```

Make sure the preceding `install` line still terminates with ` &&` and the following `echo` line is still reached.

- [ ] **Step 3: Delete the two old scripts**

```bash
git rm scripts/agent-wrapper.sh scripts/stop-agent-loop.sh
```

- [ ] **Step 4: Verify launch-fleet.sh still parses cleanly**

```bash
bash -n scripts/launch-fleet.sh
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add scripts/launch-fleet.sh
git commit -m "$(cat <<'EOF'
launch-fleet: drop Stop-hook install; delete bash agent wrapper

The Python wrapper (scripts/agent_wrapper.py) now owns the entire fleet loop,
so there is no marker protocol for a Stop hook to mediate. Each claude
invocation runs one task in print mode and exits on its own.
EOF
)"
```

---

## Task 10: Trim AGENTS.md to a single-task protocol

**Why:** Agents now receive one task per session with the wrapper handling the loop. The "Ending Your Turn" marker section is obsolete, the "Loop" section is no longer the agent's responsibility, and the backoff schedule moved to the wrapper. What remains is single-task execution discipline: read the design doc, implement with checkpoints, merge, mark done.

**Files:**
- Modify: `AGENTS.md`

- [ ] **Step 1: Replace the Identity, Loop, and Ending Your Turn sections**

In `AGENTS.md`, replace everything from the `# Agent Operating Protocol` header down through the `## Backoff When No Tasks Are Claimable` section (everything before `## Task Claiming Protocol`) with:

```markdown
# Agent Operating Protocol

Instructions for autonomous Claude Code agents building bqlite. Read this file in full before starting work.

## Identity

Your agent ID is set in the `AGENT_ID` environment variable (e.g., `agent-1`). Your working directory is `/workspace`. The wrapper also sets `TASK_DIFFICULTY_POOL` to `EASY` or `HARD`.

`EASY` agents run Sonnet at high effort and only receive tasks tagged `[EASY]`. `HARD` agents run Opus at high effort and only receive tasks tagged `[HARD]`. The wrapper enforces pool routing before handing you a task — you cannot accidentally pick up work from the other pool.

## How You Are Invoked

You run inside `scripts/agent_wrapper.py`, which:

1. Claims one task from your wave/pool via `scripts/task_tool.py claim_next`
2. Launches `claude -p --verbose` with a per-task prompt that names the task you should implement
3. After you end your turn, inspects the filesystem to decide pass / retry-once / release-lock
4. Repeats for the next task, subject to the agent's batch quota

Your responsibility is to execute **one task from claim to merged done marker**. Do not try to claim another task — the wrapper will launch a fresh session for the next one. Do not emit control markers like `[WAVE COMPLETE]` or `[END LOOP]`; those belonged to the old protocol and are now dead. The only marker that still matters is `[NEEDS INPUT]` (see *When to Ask for Human Input*).

## Configuration

```
STALE_LOCK_TIMEOUT_MINUTES=45
```
```

- [ ] **Step 2: Update the "When to Ask for Human Input" section to reflect the new NEEDS INPUT handling**

In `AGENTS.md`, replace the `## When to Ask for Human Input` section with:

```markdown
## When to Ask for Human Input

When you need a human decision you cannot resolve alone, end your turn with a message whose last line begins with `[NEEDS INPUT]` followed by your question on its own line. For example:

```
I've drafted two approaches for the aggregate spill strategy in TASK-412 but
I want confirmation before committing to one.

[NEEDS INPUT] Should spilled aggregate partitions be written into the segment
directory tree alongside data segments, or into a sibling `spill/` tree? The
tradeoffs are documented in docs/design/TASK-412.md.
```

The wrapper captures your output, detects the marker, prompts the human for a one-line reply on the cmux tab, and re-enters the same session with `claude -p --verbose -c "<human reply>"`. You pick up where you left off with the human's answer as the next user message. Keep questions specific and scoped — a one-line reply may not be enough context for an open-ended question.

Good reasons to use `[NEEDS INPUT]`:

- Architecture or design decisions with multiple valid approaches
- Ambiguous acceptance criteria in a task definition
- Merge conflicts you cannot resolve cleanly
- Any situation where proceeding could waste significant work if the wrong path is chosen

Do **not** use `[NEEDS INPUT]` as a generic "I'm done" marker — just end your turn. The wrapper detects task completion by checking whether you moved the lock file to `tasks/completed/`, not by scanning for any special marker.
```

- [ ] **Step 3: Verify the remaining sections still read coherently**

```bash
python3 -c "content = open('AGENTS.md').read(); print('has task claiming:', 'Task Claiming Protocol' in content); print('has checkpoint discipline:', 'Checkpoint Discipline' in content); print('has needs input:', '[NEEDS INPUT]' in content); print('dead markers gone:', '[WAVE COMPLETE]' not in content and '[END LOOP]' not in content and '[BATCH COMPLETE]' not in content)"
```

Expected:
```
has task claiming: True
has checkpoint discipline: True
has needs input: True
dead markers gone: True
```

- [ ] **Step 4: Commit**

```bash
git add AGENTS.md
git commit -m "$(cat <<'EOF'
AGENTS.md: trim to single-task protocol for Python wrapper

Removes the [END LOOP] / [WAVE COMPLETE] / [BATCH COMPLETE] marker protocol
(now dead), the multi-task loop section (now owned by agent_wrapper.py), and
the backoff schedule (also owned by the wrapper). [NEEDS INPUT] remains as
the one marker the wrapper still interprets — reframed around the new stdin-
capture flow.
EOF
)"
```

---

## Task 11: Update `docs/design/agent-workflow.md` to describe the new split

**Why:** The design doc still describes the marker protocol and Stop hook architecture. A short update keeps future readers from being surprised that the code doesn't match the doc.

**Files:**
- Modify: `docs/design/agent-workflow.md`

- [ ] **Step 1: Add a new Section 2.5 after "Container Startup"**

In `docs/design/agent-workflow.md`, add this section immediately before `## 3. Git Workflow`:

```markdown
### 2.5 Wrapper / Claude Control-Plane Split

Each agent container runs `scripts/agent_wrapper.py`, which owns the fleet loop. The wrapper:

1. Claims one task from the agent's wave/pool via `scripts/task_tool.py`
2. Invokes `claude -p --verbose` once, with a prompt naming the single task to implement
3. Inspects git state after claude exits: `tasks/completed/<id>.done` means success, a leftover lock means retry-once with `-c`, still-incomplete after retry means release the lock and back off
4. Captures `claude` stdout live, detects `[NEEDS INPUT]`, reads one line of human reply from the cmux tab's TTY, and resumes the same session with `claude -p --verbose -c "<reply>"`
5. Tracks the batch quota (`-n` flag from `attach-fleet.sh`) and exits cleanly when hit
6. Exits when the wave is drained for this pool (no claimable, blocked, active, or other-pool work left)

There is no Stop hook. `claude -p` exits naturally when the model's turn is done; the wrapper decides whether to launch another session. The older marker protocol (`[WAVE COMPLETE]`, `[END LOOP]`, `[BATCH COMPLETE]`) has been removed — only `[NEEDS INPUT]` survives, and it is detected by substring scan of the captured claude output rather than by a Stop hook.

This split keeps the reasoning Claude has to do scoped to one task at a time and keeps the batching/restart/retry logic in plain Python where it can be unit-tested.
```

- [ ] **Step 2: Update Section 5.3 (System Prompt)**

Replace the contents of Section 5.3 with:

```markdown
### 5.3 System Prompt (Per-Task)

`scripts/agent_wrapper.py` builds the system prompt dynamically for each task, embedding the agent's identity, pool assignment, and a pointer to `AGENTS.md`. The task-specific instructions (which task to implement, where to find the design doc, what "done" looks like) are passed as the user message. CLAUDE.md loads automatically. The wrapper passes `-p --verbose --permission-mode bypassPermissions` so the session is non-interactive, tool calls are visible in the cmux tab, and the permission gate does not block autonomous work.
```

- [ ] **Step 3: Commit**

```bash
git add docs/design/agent-workflow.md
git commit -m "agent-workflow design: document wrapper/claude control-plane split"
```

---

## Task 12: Smoke test against a live container

**Why:** Unit + integration tests cover the wrapper logic, but the real thing depends on the actual `claude` binary, the container's Python environment, and `attach-fleet.sh`'s exec plumbing. A single live-container smoke test before calling this done catches boring issues like PATH, python version, missing stdlib modules, etc.

**Files:** none — this is a manual verification step.

- [ ] **Step 1: Launch one fresh container**

```bash
scripts/launch-fleet.sh 1
```

Expected: container `bqlite-agent-1` comes up and reports "ready". Wait ~30s for git clone and plugin install to finish.

- [ ] **Step 2: Pull the latest code inside the container**

```bash
docker exec bqlite-agent-1 bash -c "cd /workspace && git pull --ff-only"
```

Expected: the new `scripts/agent_wrapper.py` is present at `/workspace/scripts/`.

- [ ] **Step 3: Run the wrapper's unit tests inside the container**

```bash
docker exec bqlite-agent-1 bash -c "cd /workspace && python3 -m unittest discover -s scripts -p 'test_*.py' -v"
```

Expected: all tests pass. If `test_task_tool_integration` or `test_agent_wrapper_integration` fail because of missing `git config user.email` in the container, fix by adding `git config --global user.name / user.email` before running.

- [ ] **Step 4: Run the wrapper against wave 4 with a 1-task quota**

Replace `4` with whichever wave currently has claimable EASY work. Check `TASKS.md` / `scripts/status.sh` first.

```bash
docker exec -it -e AGENT_ID=agent-smoke -w /workspace bqlite-agent-1 python3 /workspace/scripts/agent_wrapper.py 4 EASY 1
```

Expected:
- Banner line "=== agent-smoke launching on Wave 4 (TASK-400 through TASK-499) pool=EASY model=claude-sonnet-4-6 ==="
- One task claimed, one claude session, live tool-call output visible
- On success, `=== agent-smoke: batch complete (1/1 tasks done). Exiting. ===` and exit code 0
- On failure, review captured output; fix and re-run

- [ ] **Step 5: Confirm the done marker landed on origin/main**

```bash
docker exec bqlite-agent-1 bash -c "cd /workspace && git fetch origin && git log origin/main --oneline -n 5"
```

Expected: a recent commit from `agent-smoke` with the `TASK-NNN: completed` message.

- [ ] **Step 6: Stop the smoke container**

```bash
scripts/stop-fleet.sh
```

- [ ] **Step 7: If everything above passed, the plan is done**

If any step fails, the relevant previous task needs a fix. Common issues and which task to revisit:

| Failure mode | Task to revisit |
|---|---|
| `ModuleNotFoundError: task_tool` when running wrapper | Task 2 — check the `sys.path.insert` at the top of `agent_wrapper.py` |
| Wrapper claims a task but never invokes claude | Task 6 — check that `claim_next` is being called with the right kwargs |
| `claude -p` output is silent in the cmux tab | Task 4 — check stdout flushing in `run_claude` |
| NEEDS INPUT reply not delivered to claude | Task 5 — check that `_run_once_with_needs_input_loop` passes the reply as `initial_prompt` with `resume=True` |
| Done marker never appears even though tests pass | Task 1 — check `release_lock` isn't firing when the task actually completed (`_task_state` should return "completed" first) |

---

## Self-Review Checklist

Running through this plan against the spec one more time:

**Spec coverage:**
- [x] Python wrapper replacing bash wrapper — Tasks 2–6
- [x] Import task_tool directly — Task 2, Step 3 (`import task_tool`)
- [x] One-task-per-claude-session prompt — Task 3
- [x] Retry-once-on-incomplete via `-c` — Task 5
- [x] NEEDS INPUT stdin capture — Task 5 + Task 6 (`read_human_reply`)
- [x] Batch counter — Task 6
- [x] Backoff on no-claimable — Task 6 (matches AGENTS.md ladder 120/300/600/1200/3600)
- [x] Wave-drained exit detection — Task 1 (`is_wave_drained`) + Task 6
- [x] Stop hook deletion — Task 9
- [x] AGENTS.md trim — Task 10
- [x] Design doc update — Task 11
- [x] Smoke test — Task 12

**Placeholder scan:** none; every step shows the actual code or command.

**Type consistency:**
- `ClaudeRunResult` declared in Task 4 Step 3, used in Tasks 5 and 6
- `WrapperConfig` declared in Task 2 Step 3, used in Task 6
- `execute_task` signature matches between Task 5 Step 3 declaration and Task 6 Step 3 invocation via `run_fleet_loop`
- `release_lock(task_id, agent_id, note)` signature matches between Task 1 Step 7 and Task 6 Step 3
- `task_tool.claim_next` kwargs used in Task 6 (`wave=..., difficulty=..., agent_id=...`) match the real `claim_next` signature (the existing function also takes `no_sync` and `max_attempts`, which fall back to defaults when omitted — but the real signature requires them, so they need to be passed)

**Correction needed:** In Task 6 Step 3, the call to `claim_next(wave=..., difficulty=..., agent_id=...)` omits `no_sync` and `max_attempts`, which are required positional kwargs in the existing function signature. Fix: pass `no_sync=False, max_attempts=5` in that call. (This is noted here so the executor catches it; if the TDD-style tests already cover this path through a fake `claim_next`, the test won't detect the real-signature mismatch.)

**Applied fix:** in Task 6 Step 3, the real-signature `claim_next` call should be updated to:

```python
result = claim_next(
    wave=config.wave,
    difficulty=config.difficulty_pool,
    agent_id=agent_name,
    no_sync=False,
    max_attempts=5,
)
```

Make this change before running the Task 7 integration test.
