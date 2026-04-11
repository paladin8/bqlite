#!/usr/bin/env python3
"""Python agent wrapper for the bqlite fleet.

Owns the autonomous loop end-to-end: claims tasks via task_tool, invokes
`claude -p --verbose` once per task, inspects git state to decide pass/retry/
release, handles NEEDS INPUT stdin interactions, and tracks per-agent batch
quotas.
"""

from __future__ import annotations

import dataclasses
import os
import pathlib
import subprocess
import sys
from typing import Callable, Optional


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import task_tool  # noqa: E402  (intentional after sys.path tweak)


NEEDS_INPUT_MARKER = "[NEEDS INPUT]"


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
    with proc.stdout as stream:
        for line in stream:
            sys.stdout.write(line)
            sys.stdout.flush()
            captured.append(line)
    proc.wait()
    return ClaudeRunResult(returncode=proc.returncode, output="".join(captured))


def _filesystem_task_state(task_id: str) -> str:
    """Filesystem-only task state check. Kept for unit tests — production
    code uses task_tool.task_state_on_origin, which fetches origin/main first
    so a claude run that wrote the done marker locally but failed to push
    does not get counted as a success."""
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
    claude_runner: Callable[..., ClaudeRunResult],
    read_human_reply: Callable[[], str],
    model: str,
    effort: str,
) -> ClaudeRunResult:
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
    claude_runner: Callable[..., ClaudeRunResult],
    read_human_reply: Callable[[], str],
    task_state_fn: Optional[Callable[[str], str]] = None,
) -> str:
    """Run one claim → done cycle for a single task.

    `task_state_fn(task_id) -> "completed" | "lock_held" | "missing"` is the
    authoritative "did the agent actually finish?" check. Defaults to
    task_tool.task_state_on_origin, which fetches origin/main before checking
    so a claude run that wrote files locally but never pushed cannot be
    misreported as a success. Unit tests override this with a filesystem-only
    helper bound to their tmp directory.

    Returns "completed" if the done marker landed on origin/main, "incomplete"
    if the lock is still held after a retry, or "missing" if something else
    removed the lock without producing a done marker.
    """
    if task_state_fn is None:
        task_state_fn = task_tool.task_state_on_origin

    task_id = task["task_id"]
    sys_prompt_text = system_prompt(
        agent_name, task["difficulty"], f"[{task['difficulty']}]"
    )
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

    state = task_state_fn(task_id)
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
        state = task_state_fn(task_id)
        if state == "completed":
            return "completed"
        return "incomplete"

    return "missing"


BACKOFF_SCHEDULE_SECONDS = [120, 300, 600, 1200, 3600]
CONSECUTIVE_FAILURE_CAP = 3


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
    consecutive_failures = 0
    backoff_idx = 0
    print(_wave_range_to_status_banner(config, agent_name), flush=True)

    while True:
        if config.max_tasks is not None and completed >= config.max_tasks:
            print(
                f"=== {agent_name}: batch complete "
                f"({completed}/{config.max_tasks} tasks done). Exiting. ===",
                flush=True,
            )
            return "batch_complete"

        if consecutive_failures >= CONSECUTIVE_FAILURE_CAP:
            print(
                f"=== {agent_name}: {consecutive_failures} consecutive task "
                f"failures — something is wrong, giving up. ===",
                flush=True,
            )
            return "too_many_failures"

        result = claim_next(
            wave=config.wave,
            difficulty=config.difficulty_pool,
            agent_id=agent_name,
            no_sync=False,
            max_attempts=5,
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
                consecutive_failures = 0
                backoff_idx = 0
                continue
            print(
                f"=== {agent_name}: {task['task_id']} did not complete "
                f"(outcome={outcome}); releasing lock ===",
                flush=True,
            )
            release_lock(task["task_id"], agent_name, f"outcome={outcome}")
            consecutive_failures += 1
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


def _sleep_with_tty_drain(duration: float) -> None:
    """Drain stray keystrokes from the TTY while sleeping so ambient typing
    in the cmux tab doesn't bleed into the next claude invocation as stdin."""
    import select
    import time

    try:
        with open("/dev/tty", "r") as tty:
            remaining = duration
            while remaining > 0:
                ready, _, _ = select.select([tty], [], [], min(remaining, 1.0))
                if ready:
                    tty.readline()
                remaining -= 1.0
    except OSError:
        time.sleep(duration)


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


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
