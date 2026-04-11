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
# Usage: agent-wrapper.sh <wave> <difficulty_pool> [max_tasks]

set -uo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <wave> <difficulty_pool> [max_tasks]" >&2
  exit 1
fi

WAVE="$1"
DIFFICULTY_POOL_RAW="$2"
MAX_TASKS="${3:-}"

if ! [[ "$WAVE" =~ ^[0-9]+$ ]]; then
  echo "error: wave must be a non-negative integer, got '$WAVE'" >&2
  exit 1
fi

if [ "$WAVE" -eq 0 ]; then
  WAVE_RANGE="TASK-001 through TASK-099"
else
  WAVE_RANGE="TASK-${WAVE}00 through TASK-${WAVE}99"
fi

case "${DIFFICULTY_POOL_RAW^^}" in
  EASY)
    DIFFICULTY_POOL="EASY"
    DIFFICULTY_TAG="[EASY]"
    MODEL_ALIAS="claude-sonnet-4-6[1m]"
    EFFORT_LEVEL="high"
    ;;
  HARD)
    DIFFICULTY_POOL="HARD"
    DIFFICULTY_TAG="[HARD]"
    MODEL_ALIAS="claude-opus-4-6[1m]"
    EFFORT_LEVEL="high"
    ;;
  *)
    echo "error: difficulty_pool must be EASY or HARD, got '${DIFFICULTY_POOL_RAW}'" >&2
    exit 1
    ;;
esac

export TASK_DIFFICULTY_POOL="$DIFFICULTY_POOL"
export TASK_WAVE="$WAVE"

SENTINEL_DIR="/tmp/bqlite-fleet"
mkdir -p "$SENTINEL_DIR"
# Clear sentinels on startup so a re-attach after a completed wave restarts
# cleanly. The agent will rescan tasks/completed/ on its first fresh turn and
# re-emit [WAVE COMPLETE] immediately if the wave really is done; the cost is
# one wasted claude startup, which beats leaving stale sentinels that would
# block a legitimate restart.
rm -f "$SENTINEL_DIR/wave-complete" "$SENTINEL_DIR/needs-input"

AGENT_NAME="${AGENT_ID:-agent}"
SYSTEM_PROMPT="You are ${AGENT_NAME}, an autonomous agent building bqlite. Read AGENTS.md for your complete operating protocol. Your assigned difficulty pool is ${DIFFICULTY_POOL}; only claim tasks tagged ${DIFFICULTY_TAG} unless a human explicitly changes your assignment. Use scripts/task_tool.py rather than hand-rolling task selection or lock-file git workflows."

initial_prompt() {
  local msg="Begin the agent loop now. Use \`python3 scripts/task_tool.py claim-next --wave ${WAVE} --difficulty ${DIFFICULTY_POOL} --agent-id \$AGENT_ID\` to select and claim work instead of manually writing locks or doing the claim git flow. Only claim tasks in Wave ${WAVE} (${WAVE_RANGE}) that are tagged ${DIFFICULTY_TAG}; skip tasks outside that range or with any other difficulty tag. If the script reports status: missing_difficulty (your pool has no claimable work and the only remaining wave tasks lack an [EASY]/[HARD] tag), emit [NEEDS INPUT] rather than guessing. If it reports status: no_claimable, follow the AGENTS.md backoff schedule instead of claiming a mismatched task."
  if [ -n "$MAX_TASKS" ]; then
    msg="${msg} Stop after completing ${MAX_TASKS} task(s) by emitting [END LOOP] instead of claiming another task."
  fi
  printf '%s' "$msg"
}

run_fresh() {
  claude --model "$MODEL_ALIAS" \
         --effort "$EFFORT_LEVEL" \
         --permission-mode bypassPermissions \
         --append-system-prompt "$SYSTEM_PROMPT" \
         "$(initial_prompt)"
}

# No --append-system-prompt here: --continue inherits the system prompt from
# the resumed session, so re-applying it would double-stack the instructions.
run_resume() {
  claude --model "$MODEL_ALIAS" \
         --effort "$EFFORT_LEVEL" \
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
    echo "=== [NEEDS INPUT] ${AGENT_NAME} (${DIFFICULTY_POOL} pool) is waiting on you. Resuming last session with --continue. ==="
    rm -f "$SENTINEL_DIR/needs-input"
    run_resume || true
  else
    echo ""
    echo "=== [$(date -u +%FT%TZ)] ${AGENT_NAME} launching fresh claude session on Wave ${WAVE} (${DIFFICULTY_POOL} pool -> ${MODEL_ALIAS} ${EFFORT_LEVEL}) ==="
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
