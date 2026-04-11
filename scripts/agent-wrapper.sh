#!/usr/bin/env bash
# Outer loop for a bqlite fleet agent inside its container.
#
# Runs claude in a loop, driven by sentinels written by the Stop hook
# (scripts/stop-agent-loop.sh). The hook forces the model to emit an explicit
# control marker before ending a turn; this wrapper decides what to do next:
#
#   wave-complete sentinel -> exit.
#   needs-input sentinel   -> resume with --continue so the human can reply.
#   (no sentinel)          -> relaunch claude with a fresh context.
#
# Usage: agent-wrapper.sh <wave> [max_tasks]

set -uo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <wave> [max_tasks]" >&2
  exit 1
fi

WAVE="$1"
MAX_TASKS="${2:-}"

if ! [[ "$WAVE" =~ ^[0-9]+$ ]]; then
  echo "error: wave must be a non-negative integer, got '$WAVE'" >&2
  exit 1
fi

if [ "$WAVE" -eq 0 ]; then
  WAVE_RANGE="TASK-001 through TASK-099"
else
  WAVE_RANGE="TASK-${WAVE}00 through TASK-${WAVE}99"
fi

SENTINEL_DIR="/tmp/bqlite-fleet"
mkdir -p "$SENTINEL_DIR"
# Clear sentinels on startup so a re-attach after a completed wave restarts
# cleanly. The agent will rescan tasks/completed/ on its first fresh turn and
# re-emit [WAVE COMPLETE] immediately if the wave really is done; the cost is
# one wasted claude startup, which beats leaving stale sentinels that would
# block a legitimate restart.
rm -f "$SENTINEL_DIR/wave-complete" "$SENTINEL_DIR/needs-input"

AGENT_NAME="${AGENT_ID:-agent}"
SYSTEM_PROMPT="You are ${AGENT_NAME}, an autonomous agent building bqlite. Read AGENTS.md for your complete operating protocol."

initial_prompt() {
  local msg="Begin the agent loop now. Only claim tasks in Wave ${WAVE} (${WAVE_RANGE}); skip any task outside that range."
  if [ -n "$MAX_TASKS" ]; then
    msg="${msg} Stop after completing ${MAX_TASKS} task(s) by emitting [END LOOP] instead of claiming another task."
  fi
  printf '%s' "$msg"
}

run_fresh() {
  claude --model 'claude-opus-4-6[1m]' \
         --effort high \
         --permission-mode bypassPermissions \
         --append-system-prompt "$SYSTEM_PROMPT" \
         "$(initial_prompt)"
}

# No --append-system-prompt here: --continue inherits the system prompt from
# the resumed session, so re-applying it would double-stack the instructions.
run_resume() {
  claude --model 'claude-opus-4-6[1m]' \
         --effort high \
         --permission-mode bypassPermissions \
         --continue
}

# Sleep for N seconds while draining stray keystrokes from the TTY. If the
# user types into the cmux tab between claude invocations, those characters
# would otherwise sit in the terminal input buffer and be consumed by the
# next claude as unintended stdin.
nap() {
  local duration="$1"
  read -t "$duration" -r _ </dev/tty 2>/dev/null || true
}

FAST_FAIL_THRESHOLD_SECONDS=10
FAST_FAIL_LIMIT=5
BACKOFF_SECONDS=300
consecutive_fast=0

while true; do
  if [ -f "$SENTINEL_DIR/wave-complete" ]; then
    echo ""
    echo "=== Wave ${WAVE} complete. ${AGENT_NAME} exiting. ==="
    break
  fi

  start=$(date +%s)
  if [ -f "$SENTINEL_DIR/needs-input" ]; then
    echo ""
    echo "=== [NEEDS INPUT] ${AGENT_NAME} is waiting on you. Resuming last session with --continue. ==="
    rm -f "$SENTINEL_DIR/needs-input"
    run_resume || true
  else
    echo ""
    echo "=== [$(date -u +%FT%TZ)] ${AGENT_NAME} launching fresh claude session on Wave ${WAVE} ==="
    run_fresh || true
  fi
  end=$(date +%s)

  if [ "$((end - start))" -lt "$FAST_FAIL_THRESHOLD_SECONDS" ]; then
    consecutive_fast=$((consecutive_fast + 1))
  else
    consecutive_fast=0
  fi

  if [ "$consecutive_fast" -ge "$FAST_FAIL_LIMIT" ]; then
    echo ""
    echo "=== ${AGENT_NAME}: ${consecutive_fast} fast failures in a row — sleeping ${BACKOFF_SECONDS}s before retry ==="
    nap "$BACKOFF_SECONDS"
    consecutive_fast=0
  else
    nap 2
  fi
done
