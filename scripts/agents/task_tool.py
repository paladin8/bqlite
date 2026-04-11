#!/usr/bin/env python3
"""Task selection and claiming utility for the bqlite agent fleet.

This script centralizes the expensive, repetitive parts of the agent loop:

- parsing TASKS.md for task metadata
- filtering by wave, difficulty, completion, claims, and dependencies
- detecting stale locks
- claiming the next eligible task with the required git push workflow

It prints JSON to stdout so agents can consume the result deterministically.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_ROOT = Path(__file__).resolve().parent.parent.parent
ROOT = Path(os.environ.get("BQLITE_TASK_TOOL_ROOT", str(DEFAULT_ROOT))).resolve()
TASKS_MD = ROOT / "TASKS.md"
ACTIVE_DIR = ROOT / "tasks" / "active"
COMPLETED_DIR = ROOT / "tasks" / "completed"
TASK_HEADER_RE = re.compile(r"^### (TASK-(\d{3})): ((?:\[[^\]]+\])*)(.*)$")
TASK_ID_RE = re.compile(r"TASK-\d{3}")
LOCK_TIMEOUT_MINUTES = int(os.environ.get("STALE_LOCK_TIMEOUT_MINUTES", "45"))


class TaskToolError(RuntimeError):
    """Operational error that should produce a non-zero exit code."""


@dataclasses.dataclass(frozen=True)
class Task:
    task_id: str
    number: int
    wave: int
    title: str
    tags: tuple[str, ...]
    depends_on: tuple[str, ...]

    @property
    def difficulty(self) -> str | None:
        if "EASY" in self.tags:
            return "EASY"
        if "HARD" in self.tags:
            return "HARD"
        return None

    @property
    def retired(self) -> bool:
        return "RETIRED" in self.tags


def json_out(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso8601(raw: str) -> dt.datetime:
    normalized = raw.replace("Z", "+00:00")
    return dt.datetime.fromisoformat(normalized)


def run(
    args: list[str],
    *,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=ROOT,
        check=check,
        capture_output=capture_output,
        text=True,
    )


def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run(["git", *args], check=check)


def require_clean_worktree() -> None:
    status = git("status", "--porcelain", check=True).stdout.strip()
    if status:
        raise TaskToolError(
            "working tree is not clean; refusing to claim or break locks with local changes present"
        )


def sync_main() -> None:
    require_clean_worktree()
    git("checkout", "main", check=True)
    git("fetch", "origin", check=True)
    git("pull", "--ff-only", "origin", "main", check=True)


def cleanup_after_failed_push() -> None:
    head_exists = run(["git", "rev-parse", "--verify", "HEAD"], check=False)
    if head_exists.returncode == 0:
        git("reset", "--mixed", "HEAD~1", check=True)
    git("restore", "--worktree", "--staged", "tasks", check=True)
    git("pull", "--ff-only", "origin", "main", check=True)


def commit_and_push(commit_message: str) -> bool:
    git("commit", "-m", commit_message, check=True)
    pushed = git("push", "origin", "main", check=False)
    if pushed.returncode == 0:
        return True
    cleanup_after_failed_push()
    return False


def parse_tasks() -> dict[str, Task]:
    return parse_tasks_text(TASKS_MD.read_text())


def parse_tasks_text(markdown: str) -> dict[str, Task]:
    tasks: dict[str, Task] = {}
    current: dict[str, Any] | None = None

    for raw_line in markdown.splitlines():
        header_match = TASK_HEADER_RE.match(raw_line)
        if header_match:
            if current is not None:
                task = _materialize_task(current)
                tasks[task.task_id] = task

            task_id = header_match.group(1)
            number = int(header_match.group(2))
            tags = tuple(re.findall(r"\[([^\]]+)\]", header_match.group(3)))
            title = header_match.group(4).strip()
            current = {
                "task_id": task_id,
                "number": number,
                "tags": tags,
                "title": title,
                "depends_on": (),
            }
            continue

        if current is None:
            continue

        if raw_line.startswith("**Depends on**:"):
            depends_raw = raw_line.split(":", 1)[1].strip()
            if depends_raw.lower() == "none":
                current["depends_on"] = ()
            else:
                current["depends_on"] = tuple(TASK_ID_RE.findall(depends_raw))

    if current is not None:
        task = _materialize_task(current)
        tasks[task.task_id] = task

    return tasks


def _materialize_task(raw: dict[str, Any]) -> Task:
    number = int(raw["number"])
    wave = 0 if number < 100 else number // 100
    tags = tuple(raw["tags"])
    if "EASY" in tags and "HARD" in tags:
        raise TaskToolError(
            f"{raw['task_id']} has both [EASY] and [HARD] tags; pick one"
        )
    return Task(
        task_id=str(raw["task_id"]),
        number=number,
        wave=wave,
        title=str(raw["title"]),
        tags=tags,
        depends_on=tuple(raw["depends_on"]),
    )


def read_json_files(directory: Path, suffix: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob(f"*{suffix}")):
        if not path.is_file():
            continue
        with path.open() as handle:
            result[path.stem] = json.load(handle)
    return result


def load_state() -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    active = read_json_files(ACTIVE_DIR, ".lock")
    completed = read_json_files(COMPLETED_DIR, ".done")
    return active, completed


def task_sort_key(task: Task) -> tuple[int, str]:
    return (task.number, task.task_id)


def is_dependency_satisfied(task: Task, completed_ids: set[str]) -> bool:
    return all(dep in completed_ids for dep in task.depends_on)


def classify_tasks(
    *,
    tasks: dict[str, Task],
    wave: int,
    difficulty: str,
    active: dict[str, dict[str, Any]],
    completed: dict[str, dict[str, Any]],
) -> dict[str, list[Task]]:
    completed_ids = set(completed)
    scoped = [task for task in tasks.values() if task.wave == wave]

    claimable_missing_difficulty = [
        task
        for task in scoped
        if task.difficulty is None
        and not task.retired
        and task.task_id not in active
        and task.task_id not in completed_ids
        and is_dependency_satisfied(task, completed_ids)
    ]

    eligible = [
        task
        for task in scoped
        if task.difficulty == difficulty and not task.retired
    ]

    claimable = [
        task
        for task in eligible
        if task.task_id not in active
        and task.task_id not in completed_ids
        and is_dependency_satisfied(task, completed_ids)
    ]

    blocked = [
        task
        for task in eligible
        if task.task_id not in active
        and task.task_id not in completed_ids
        and not is_dependency_satisfied(task, completed_ids)
    ]

    active_tasks = [task for task in eligible if task.task_id in active]
    completed_tasks = [task for task in eligible if task.task_id in completed_ids]
    other_pool_claimable = [
        task
        for task in scoped
        if task.difficulty not in (None, difficulty)
        and not task.retired
        and task.task_id not in active
        and task.task_id not in completed_ids
        and is_dependency_satisfied(task, completed_ids)
    ]

    return {
        "claimable_missing_difficulty": sorted(
            claimable_missing_difficulty, key=task_sort_key
        ),
        "claimable": sorted(claimable, key=task_sort_key),
        "blocked": sorted(blocked, key=task_sort_key),
        "active": sorted(active_tasks, key=task_sort_key),
        "completed": sorted(completed_tasks, key=task_sort_key),
        "other_pool_claimable": sorted(other_pool_claimable, key=task_sort_key),
    }


def parse_difficulty(raw: str) -> str:
    value = raw.upper()
    if value not in {"EASY", "HARD"}:
        raise TaskToolError(f"difficulty must be EASY or HARD, got {raw!r}")
    return value


def task_to_json(task: Task) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "title": task.title,
        "wave": task.wave,
        "tags": list(task.tags),
        "depends_on": list(task.depends_on),
        "difficulty": task.difficulty,
    }


def lock_is_stale(task_id: str, metadata: dict[str, Any], *, now: dt.datetime) -> bool:
    claimed_at_raw = metadata.get("claimed_at")
    branch = metadata.get("branch", f"task/{task_id}")

    if not claimed_at_raw:
        return False

    claimed_at = parse_iso8601(str(claimed_at_raw))
    timeout = dt.timedelta(minutes=LOCK_TIMEOUT_MINUTES)
    if now - claimed_at <= timeout:
        return False

    ref = f"refs/remotes/origin/{branch}"
    branch_info = git(
        "for-each-ref", "--format=%(committerdate:iso8601-strict)", ref, check=False
    ).stdout.strip()
    if branch_info:
        branch_commit_time = parse_iso8601(branch_info)
        if now - branch_commit_time <= timeout:
            return False

    since = (now - timeout).isoformat()
    recent_main_refs = git(
        "log",
        "origin/main",
        f"--since={since}",
        f"--grep={task_id}",
        "--format=%H",
        "-n",
        "1",
        check=False,
    ).stdout.strip()
    if recent_main_refs:
        return False

    return True


def break_stale_lock(task_id: str) -> bool:
    lock_path = ACTIVE_DIR / f"{task_id}.lock"
    if not lock_path.exists():
        return True
    lock_path.unlink()
    git("add", str(lock_path.relative_to(ROOT)), check=True)
    return commit_and_push(f"{task_id}: break stale lock")


def write_lock(task: Task, agent_id: str) -> dict[str, Any]:
    claimed_at = now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "agent_id": agent_id,
        "task_id": task.task_id,
        "claimed_at": claimed_at,
        "branch": f"task/{task.task_id}",
        "description": task.title,
    }
    lock_path = ACTIVE_DIR / f"{task.task_id}.lock"
    lock_path.write_text(json.dumps(payload, indent=2) + "\n")
    git("add", str(lock_path.relative_to(ROOT)), check=True)
    return payload


def ensure_task_branch(task_id: str) -> None:
    """Check out the task branch, preserving any prior work pushed to origin.

    Order of precedence:
    1. If a local branch exists, check it out and fast-forward from the remote
       when possible. This covers the "same container, same task resumed"
       case.
    2. Else if `refs/remotes/origin/task/TASK-NNN` exists, create a local
       tracking branch from it. This covers the reclaim path where an earlier
       agent crashed mid-task after pushing to its task branch: a fresh clone
       has no local branch, but `sync_main()` has already fetched the remote
       refs, so we create the local branch from the remote head so the
       recovering agent sees the prior work instead of a clean main.
    3. Otherwise create a fresh branch from the current checkout (main, just
       synced and carrying the new lock commit).
    """
    branch = f"task/{task_id}"
    local_ref = f"refs/heads/{branch}"
    remote_ref = f"refs/remotes/origin/{branch}"

    local_exists = (
        git("show-ref", "--verify", "--quiet", local_ref, check=False).returncode == 0
    )
    remote_exists = (
        git("show-ref", "--verify", "--quiet", remote_ref, check=False).returncode == 0
    )

    if local_exists:
        git("checkout", branch, check=True)
        if remote_exists:
            git("merge", "--ff-only", f"origin/{branch}", check=False)
    elif remote_exists:
        git("checkout", "-b", branch, f"origin/{branch}", check=True)
    else:
        git("checkout", "-b", branch, check=True)


def task_lock_path(task_id: str) -> Path:
    return ACTIVE_DIR / f"{task_id}.lock"


def task_done_path(task_id: str) -> Path:
    return COMPLETED_DIR / f"{task_id}.done"


def task_state_on_origin(task_id: str) -> str:
    """Return "completed" / "lock_held" / "missing" for a task, as visible on
    origin/main.

    Used by agent_wrapper.py after a claude run exits so that a claude that
    wrote the done marker locally but failed to push does not get counted as
    a success. Fetches origin/main first so the check sees the latest remote
    state without touching the working tree or the current branch.
    """
    git("fetch", "origin", "main", check=False)
    done_rev = f"origin/main:tasks/completed/{task_id}.done"
    lock_rev = f"origin/main:tasks/active/{task_id}.lock"
    if git("cat-file", "-e", done_rev, check=False).returncode == 0:
        return "completed"
    if git("cat-file", "-e", lock_rev, check=False).returncode == 0:
        return "lock_held"
    return "missing"


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


def claim_next(
    *,
    wave: int,
    difficulty: str,
    agent_id: str,
    no_sync: bool,
    max_attempts: int,
) -> dict[str, Any]:
    stale_broken: list[str] = []

    for _ in range(max_attempts):
        if not no_sync:
            sync_main()

        tasks = parse_tasks()
        active, completed = load_state()

        for task_id in sorted(active):
            if lock_is_stale(task_id, active[task_id], now=now_utc()):
                if break_stale_lock(task_id):
                    stale_broken.append(task_id)
                    break
                continue
        else:
            classified = classify_tasks(
                tasks=tasks,
                wave=wave,
                difficulty=difficulty,
                active=active,
                completed=completed,
            )
            missing_difficulty_tasks = [
                task_to_json(task)
                for task in classified["claimable_missing_difficulty"]
            ]

            if classified["claimable"]:
                selected = classified["claimable"][0]
                lock_payload = write_lock(selected, agent_id)
                if commit_and_push(f"{selected.task_id}: claimed by {agent_id}"):
                    ensure_task_branch(selected.task_id)
                    return {
                        "status": "claimed",
                        "wave": wave,
                        "difficulty": difficulty,
                        "task": task_to_json(selected),
                        "lock": lock_payload,
                        "missing_difficulty_tasks": missing_difficulty_tasks,
                        "stale_locks_broken": stale_broken,
                    }
                continue

            if missing_difficulty_tasks:
                return {
                    "status": "missing_difficulty",
                    "wave": wave,
                    "difficulty": difficulty,
                    "tasks": missing_difficulty_tasks,
                    "stale_locks_broken": stale_broken,
                }

            return {
                "status": "no_claimable",
                "wave": wave,
                "difficulty": difficulty,
                "claimable": [],
                "blocked": [task_to_json(task) for task in classified["blocked"]],
                "active": [task_to_json(task) for task in classified["active"]],
                "completed_count": len(classified["completed"]),
                "other_pool_claimable": [
                    task_to_json(task) for task in classified["other_pool_claimable"]
                ],
                "stale_locks_broken": stale_broken,
            }

    raise TaskToolError("exhausted claim attempts due to repeated git races")


def list_tasks(
    *,
    wave: int,
    difficulty: str,
    no_sync: bool,
) -> dict[str, Any]:
    if not no_sync:
        sync_main()

    tasks = parse_tasks()
    active, completed = load_state()
    classified = classify_tasks(
        tasks=tasks,
        wave=wave,
        difficulty=difficulty,
        active=active,
        completed=completed,
    )
    return {
        "status": "ok",
        "wave": wave,
        "difficulty": difficulty,
        "claimable_missing_difficulty": [
            task_to_json(task) for task in classified["claimable_missing_difficulty"]
        ],
        "claimable": [task_to_json(task) for task in classified["claimable"]],
        "blocked": [task_to_json(task) for task in classified["blocked"]],
        "active": [task_to_json(task) for task in classified["active"]],
        "completed_count": len(classified["completed"]),
        "other_pool_claimable": [
            task_to_json(task) for task in classified["other_pool_claimable"]
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser, *, needs_agent: bool) -> None:
        subparser.add_argument("--wave", type=int, required=True)
        subparser.add_argument("--difficulty", required=True)
        subparser.add_argument(
            "--no-sync",
            action="store_true",
            help="skip git checkout/fetch/pull; useful for local dry runs and tests",
        )
        if needs_agent:
            subparser.add_argument(
                "--agent-id",
                default=os.environ.get("AGENT_ID", ""),
                help="agent identifier to record in the lock file",
            )
            subparser.add_argument(
                "--max-attempts",
                type=int,
                default=5,
                help="maximum claim retry attempts after git races",
            )

    list_parser = subparsers.add_parser("list", help="list tasks for a wave/pool")
    add_common(list_parser, needs_agent=False)

    claim_parser = subparsers.add_parser(
        "claim-next", help="claim the next eligible task for a wave/pool"
    )
    add_common(claim_parser, needs_agent=True)

    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        difficulty = parse_difficulty(args.difficulty)
        if args.command == "list":
            json_out(list_tasks(wave=args.wave, difficulty=difficulty, no_sync=args.no_sync))
            return 0

        agent_id = args.agent_id.strip()
        if not agent_id:
            raise TaskToolError("--agent-id is required for claim-next")

        result = claim_next(
            wave=args.wave,
            difficulty=difficulty,
            agent_id=agent_id,
            no_sync=args.no_sync,
            max_attempts=args.max_attempts,
        )
        json_out(result)
        return 0
    except TaskToolError as exc:
        json_out({"status": "error", "error": str(exc)})
        return 1
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        payload = {
            "status": "error",
            "error": f"command failed: {' '.join(exc.cmd)}",
            "returncode": exc.returncode,
        }
        if stdout:
            payload["stdout"] = stdout
        if stderr:
            payload["stderr"] = stderr
        json_out(payload)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
