#!/usr/bin/env python3
"""Python agent wrapper for the bqlite fleet.

Owns the autonomous loop end-to-end: claims tasks via task_tool, invokes
`claude -p --verbose` once per task, inspects git state to decide pass/retry/
release, handles NEEDS INPUT stdin interactions, and tracks per-agent batch
quotas.
"""

from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import pty
import subprocess
import sys
from typing import Any, Callable, Optional


SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import task_tool  # noqa: E402  (intentional after sys.path tweak)


NEEDS_INPUT_MARKER = "[NEEDS INPUT]"

# Absolute path to the claude binary inside the fleet container. Using the
# absolute path avoids relying on PATH being wired up the same way under
# `docker exec python3 ...` and under `docker exec bash -lc '... python3 ...'`
# (the targeted-attach flow), which burned us once. Tests (and anything
# that needs a different binary) can override via the BQLITE_CLAUDE_BIN env
# var; the path is resolved at run_claude call time so tests can set it
# per-invocation without re-importing the module.
_DEFAULT_CLAUDE_BIN = "/root/.local/bin/claude"


def _claude_bin() -> str:
    return os.environ.get("BQLITE_CLAUDE_BIN", _DEFAULT_CLAUDE_BIN)


class WrapperConfigError(RuntimeError):
    """User-visible configuration error; mapped to non-zero exit code."""


# Per-task model routing: each TASKS.md entry is tagged [EASY] or [HARD],
# and the wrapper picks the matching claude model when it runs the task.
# Agents no longer belong to a pool — any wrapper can claim any tagged,
# dep-satisfied task in its wave and spin up the right model for it.
_MODEL_FOR_DIFFICULTY = {
    "EASY": {"model": "claude-sonnet-4-6", "effort": "high"},
    "HARD": {"model": "claude-opus-4-6[1m]", "effort": "high"},
}


@dataclasses.dataclass(frozen=True)
class WrapperConfig:
    wave: int
    max_tasks: Optional[int]


def parse_args(argv: list[str]) -> WrapperConfig:
    if len(argv) < 1 or len(argv) > 2:
        raise WrapperConfigError(
            "usage: agent_wrapper.py <wave> [max_tasks]"
        )

    wave_raw = argv[0]
    max_tasks_raw = argv[1] if len(argv) == 2 else None

    if not wave_raw.isdigit():
        raise WrapperConfigError(
            f"wave must be a non-negative integer, got {wave_raw!r}"
        )
    wave = int(wave_raw)

    max_tasks: Optional[int] = None
    if max_tasks_raw is not None:
        if not max_tasks_raw.isdigit() or int(max_tasks_raw) < 1:
            raise WrapperConfigError(
                f"max_tasks must be a positive integer, got {max_tasks_raw!r}"
            )
        max_tasks = int(max_tasks_raw)

    return WrapperConfig(wave=wave, max_tasks=max_tasks)


def wave_range_label(wave: int) -> str:
    if wave == 0:
        return "TASK-001 through TASK-099"
    return f"TASK-{wave}00 through TASK-{wave}99"


def system_prompt(agent_name: str) -> str:
    return (
        f"You are {agent_name}, an autonomous agent building bqlite. "
        f"Read AGENTS.md for your complete operating protocol. "
        f"The wrapper handles task claiming, batching, and restart logic — "
        f"your job is to execute one task at a time from start to merge."
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
    """Result of a claude -p invocation.

    `output` is the extracted assistant text (concatenated text_delta events)
    and is what [NEEDS INPUT] scanning runs against — scanning thinking
    tokens or tool inputs would be a false-positive magnet.

    `raw_output` is every byte that came off the pty, useful for tests and
    post-hoc debugging.
    """
    returncode: int
    output: str
    raw_output: str = ""


@dataclasses.dataclass
class _StreamState:
    assistant_text_parts: list[str] = dataclasses.field(default_factory=list)
    in_text_stream: bool = False

    def assistant_text(self) -> str:
        return "".join(self.assistant_text_parts)


def _preview_tool_input(input_data: Any) -> str:
    if not isinstance(input_data, dict):
        text = str(input_data)
        return text[:100]
    # Promote common interesting fields first.
    for key in ("file_path", "path", "command", "pattern", "query", "url"):
        if key in input_data and isinstance(input_data[key], (str, int, float)):
            val = str(input_data[key])
            return val[:100] if len(val) <= 100 else val[:97] + "..."
    try:
        text = json.dumps(input_data, separators=(",", ":"))
    except (TypeError, ValueError):
        text = str(input_data)
    return text[:100] if len(text) <= 100 else text[:97] + "..."


def _tool_result_preview(content: Any) -> str:
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        text = " ".join(parts)
    else:
        text = str(content)
    stripped = text.strip()
    if not stripped:
        return "(empty)"
    first_line = stripped.splitlines()[0]
    return first_line[:140] if len(first_line) <= 140 else first_line[:137] + "..."


def _format_stream_event(event: dict, state: _StreamState) -> Optional[tuple[str, str]]:
    """Turn one stream-json event into a (kind, content) pair for display.

    kind is "inline" for text_delta streams that should be written without a
    trailing newline, or "line" for standalone lines. Returns None for events
    the wrapper intentionally silences (hook chatter, empty message
    boundaries, thinking-token noise).
    """
    t = event.get("type")

    if t == "system":
        subtype = event.get("subtype")
        if subtype == "init":
            session_id = str(event.get("session_id", ""))[:8]
            tools = event.get("tools", [])
            cwd = event.get("cwd", "?")
            return ("line", f"• session {session_id} cwd={cwd} tools={len(tools)}")
        return None

    if t == "stream_event":
        inner = event.get("event", {})
        it = inner.get("type")
        if it == "content_block_start":
            block = inner.get("content_block", {}) or {}
            if block.get("type") == "thinking":
                return ("line", "◇ thinking…")
            return None
        if it == "content_block_stop":
            return None
        if it == "content_block_delta":
            delta = inner.get("delta", {})
            dt = delta.get("type")
            if dt == "text_delta":
                text = delta.get("text", "")
                if not text:
                    return None
                state.assistant_text_parts.append(text)
                return ("inline", text)
            # thinking_delta / input_json_delta / signature_delta are all
            # silenced — they duplicate information we already print on
            # content_block_start (thinking indicator, tool call name).
            return None
        return None

    if t == "assistant":
        msg = event.get("message", {})
        lines = []
        for block in msg.get("content", []) or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "tool_use":
                name = block.get("name", "tool")
                preview = _preview_tool_input(block.get("input"))
                lines.append(f"🔧 {name}({preview})")
        if lines:
            return ("line", "\n".join(lines))
        return None

    if t == "user":
        msg = event.get("message", {})
        for block in msg.get("content", []) or []:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                preview = _tool_result_preview(block.get("content"))
                return ("line", f"↩ {preview}")
        return None

    if t == "result":
        subtype = event.get("subtype", "")
        turns = event.get("num_turns", 0)
        duration_ms = event.get("duration_ms", 0) or 0
        return ("line", f"✓ result ({subtype}, {turns} turns, {duration_ms/1000:.1f}s)")

    if t == "rate_limit_event":
        info = event.get("rate_limit_info", {}) or {}
        return (
            "line",
            f"⚠ rate limit: {info.get('status','?')} "
            f"type={info.get('rateLimitType','?')} "
            f"util={info.get('utilization','?')}",
        )

    return None


def run_claude(
    *,
    prompt: str,
    model: str,
    effort: str,
    system_prompt_text: Optional[str],
    resume: bool,
    env: Optional[dict[str, str]] = None,
) -> ClaudeRunResult:
    """Run `claude -p --verbose --output-format stream-json` once, parsing
    each JSON event as it arrives and pretty-printing human-readable lines
    (tool calls, tool results, streamed text, rate-limit warnings) to the
    cmux tab. Assistant text blocks are accumulated into
    `ClaudeRunResult.output` for [NEEDS INPUT] detection.

    claude is attached to a pseudo-terminal so its stdio stays line-buffered
    and events stream as they happen instead of stalling in a 4 KB pipe
    buffer. Plain `-p --verbose` in text mode does NOT stream intermediate
    progress — the print format only emits the final assistant text — so
    `--output-format stream-json --include-partial-messages` is the only way
    to actually observe what the model is doing.

    When `resume` is True, passes `-c` and omits `--append-system-prompt` (a
    resumed session inherits the prior system prompt). When False, passes the
    provided `system_prompt_text` via `--append-system-prompt`.
    """
    args = [
        _claude_bin(),
        "-p",
        "--verbose",
        "--output-format",
        "stream-json",
        "--include-partial-messages",
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

    master_fd, slave_fd = pty.openpty()
    try:
        proc = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
            env=env if env is not None else os.environ.copy(),
        )
    finally:
        # The parent does not write to the slave side; closing it here lets
        # reads on the master return EOF cleanly once the child exits.
        os.close(slave_fd)

    state = _StreamState()
    raw_chunks: list[str] = []
    buf = b""

    def _emit(kind: str, content: str) -> None:
        if kind == "inline":
            sys.stdout.write(content)
            state.in_text_stream = True
        else:  # "line"
            if state.in_text_stream:
                sys.stdout.write("\n")
                state.in_text_stream = False
            sys.stdout.write(content)
            if not content.endswith("\n"):
                sys.stdout.write("\n")
        sys.stdout.flush()

    def _handle_line(line_bytes: bytes) -> None:
        line_text = line_bytes.decode("utf-8", errors="replace")
        if not line_text.strip():
            return
        try:
            event = json.loads(line_text)
        except json.JSONDecodeError:
            # Non-JSON: probably terminal escape sequences or a stderr panic
            # line. Echo it so a broken claude doesn't go unnoticed.
            if line_text.strip().startswith("{"):
                # Partial JSON that failed to parse — warn once per chunk so
                # we don't silently drop real events.
                if state.in_text_stream:
                    sys.stdout.write("\n")
                    state.in_text_stream = False
                sys.stdout.write(f"⚠ stream-json parse error: {line_text[:160]}\n")
                sys.stdout.flush()
            return
        formatted = _format_stream_event(event, state)
        if formatted is None:
            return
        kind, content = formatted
        _emit(kind, content)

    try:
        while True:
            try:
                chunk = os.read(master_fd, 4096)
            except OSError:
                # Linux raises EIO when the slave side closes; macOS returns
                # empty bytes. Treat both as EOF.
                break
            if not chunk:
                break
            raw_chunks.append(chunk.decode("utf-8", errors="replace"))
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                _handle_line(line)
        if buf.strip():
            _handle_line(buf)
            buf = b""
    finally:
        os.close(master_fd)

    if state.in_text_stream:
        sys.stdout.write("\n")
        sys.stdout.flush()

    proc.wait()
    return ClaudeRunResult(
        returncode=proc.returncode,
        output=state.assistant_text(),
        raw_output="".join(raw_chunks),
    )


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
    sys_prompt_text = system_prompt(agent_name)
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
        f"({wave_range_label(config.wave)}) ==="
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
    reset_worktree: Optional[Callable[[], None]] = None,
) -> str:
    """Run the autonomous fleet loop until quota/wave-drain/failure-cap.

    `reset_worktree` is called at the top of every loop iteration before
    claiming a task, to wipe any scraps from a prior iteration so dirty
    state cannot accumulate across tasks and trip require_clean_worktree().
    Production passes task_tool.reset_to_origin; unit tests default to
    None (no-op) so they can drive the loop against their fake git state.
    """
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

        try:
            if reset_worktree is not None:
                reset_worktree()
            # difficulty=None: the wrapper no longer belongs to a pool —
            # it claims whatever tagged, dep-satisfied task is next in the
            # wave and selects the claude model based on the task's own tag.
            #
            # no_sync=True: reset_worktree above already fetched origin/main
            # and forced the worktree to match it, so claim_next's internal
            # sync_main would just re-fetch for no gain. task_tool's own
            # commit-race cleanup still re-fetches on retry iterations
            # inside claim_next, so this does not widen the race window.
            result = claim_next(
                wave=config.wave,
                difficulty=None,
                agent_id=agent_name,
                no_sync=True,
                max_attempts=5,
            )
            status = result.get("status")

            if status == "claimed":
                task = result["task"]
                difficulty = task.get("difficulty")
                model_cfg = _MODEL_FOR_DIFFICULTY.get(difficulty) if difficulty else None
                if model_cfg is None:
                    # Defensive: claim_next is supposed to exclude untagged
                    # tasks via the missing_difficulty path, so landing here
                    # means something odd happened. Release the lock and
                    # treat it as a consecutive failure so we back off.
                    print(
                        f"=== {agent_name}: claimed {task['task_id']} has no "
                        f"recognized difficulty (got {difficulty!r}); releasing ===",
                        flush=True,
                    )
                    release_lock(
                        task["task_id"], agent_name, "unrecognized difficulty"
                    )
                    consecutive_failures += 1
                    continue
                print(
                    f"=== {agent_name}: claimed {task['task_id']} [{difficulty}] — "
                    f"{task['title']} ===",
                    flush=True,
                )
                outcome = execute_task(
                    task,
                    agent_name=agent_name,
                    model=model_cfg["model"],
                    effort=model_cfg["effort"],
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
                        f"=== {agent_name}: wave {config.wave} drained. "
                        f"Exiting. ===",
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
        except subprocess.CalledProcessError as exc:
            # A git command under task_tool blew up mid-iteration. Typical
            # causes: a concurrent push left us in a weird state that
            # cleanup_after_failed_push could not recover from, a transient
            # network error on fetch/push, or a genuinely broken repo. Log
            # with as much detail as we have, count this against the
            # consecutive-failure cap, and back off so the next pass has a
            # chance to recover instead of crashing the wrapper.
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            cmd = " ".join(exc.cmd) if isinstance(exc.cmd, (list, tuple)) else str(exc.cmd)
            print(
                f"=== {agent_name}: git command failed mid-iteration: "
                f"{cmd} (exit {exc.returncode}) ===",
                flush=True,
            )
            if stderr:
                print(f"    stderr: {stderr[:500]}", flush=True)
            if stdout:
                print(f"    stdout: {stdout[:500]}", flush=True)
            consecutive_failures += 1
            sleep(60)
            continue
        except task_tool.TaskToolError as exc:
            # Operational error from task_tool itself — typically a dirty
            # worktree that `require_clean_worktree()` rejected before the
            # claim flow could start. attach-fleet.sh's targeted mode runs
            # reset-worktree.sh to prevent this at startup, but handle it
            # defensively so a dirty-state scenario mid-run does not crash
            # the wrapper either.
            print(
                f"=== {agent_name}: task_tool error mid-iteration: {exc} ===",
                flush=True,
            )
            consecutive_failures += 1
            sleep(60)
            continue


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
            reset_worktree=task_tool.reset_to_origin,
        )
    except KeyboardInterrupt:
        print(f"\n=== {agent_name}: interrupted. Exiting. ===", flush=True)
        return 130

    return 0 if outcome in {"wave_complete", "batch_complete"} else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
