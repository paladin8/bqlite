#!/usr/bin/env bash
# Stop hook for bqlite fleet agents.
#
# Claude Code fires this whenever the model ends a turn. We inspect recent
# assistant messages for an explicit control marker:
#
#   [WAVE COMPLETE] -> wave is finished; wrapper exits.
#   [END LOOP]      -> agent wants a fresh context; wrapper relaunches claude.
#   [NEEDS INPUT]   -> agent is waiting on a human; wrapper resumes with -c.
#
# Without a marker we block and force the model back into the agent loop,
# unless stop_hook_active is already set (which would cause an infinite
# block-retry loop — we respect the break-out contract by allowing the stop).
#
# We deliberately run WITHOUT `set -e`: if jq fails to parse a malformed
# transcript we want to fall through to the default branch (block and
# instruct the model to keep looping), not abort with no response to Claude.

set -uo pipefail

SENTINEL_DIR="/tmp/bqlite-fleet"
mkdir -p "$SENTINEL_DIR"

payload="$(cat)"

stop_hook_active="$(printf '%s' "$payload" | jq -r '.stop_hook_active // false')"
transcript="$(printf '%s' "$payload" | jq -r '.transcript_path // empty')"

# Grab the most recent assistant text block in the whole transcript. A turn
# is split across many JSONL entries (thinking, tool_use, text), and a
# fixed-window filter fails when the tail happens to be all tool_use; since
# the Stop hook only fires when the model's turn actually ends with a text
# block, the last text block in the file is the one we care about.
last_text=""
if [ -n "$transcript" ] && [ -f "$transcript" ]; then
  last_text="$(
    jq -sr '
      [ .[]
        | select(.type == "assistant")
        | .message.content[]?
        | select(.type? == "text")
        | .text
      ]
      | last // ""
    ' "$transcript" 2>/dev/null
  )" || last_text=""
fi

# Honor markers regardless of stop_hook_active — the contract only prohibits
# re-blocking, not recording the agent's explicit signal.
case "$last_text" in
  *"[WAVE COMPLETE]"*)
    touch "$SENTINEL_DIR/wave-complete"
    exit 0
    ;;
  *"[NEEDS INPUT]"*)
    touch "$SENTINEL_DIR/needs-input"
    exit 0
    ;;
  *"[END LOOP]"*)
    exit 0
    ;;
esac

# No marker. If we already blocked once this turn, allow the stop so the
# wrapper can relaunch with a fresh context instead of infinite-looping.
if [ "$stop_hook_active" = "true" ]; then
  exit 0
fi

jq -n '{
  decision: "block",
  reason: "Do not end your turn without an explicit marker. Emit [WAVE COMPLETE] if every task in this wave is done or blocked, [NEEDS INPUT] if you need human input per AGENTS.md, or [END LOOP] if you want a fresh context before continuing. Otherwise return to step 1 of the agent loop in AGENTS.md and keep working."
}'
