#!/usr/bin/env bash
set -euo pipefail

MAX_TASKS=""
WAVE=""

usage() {
  echo "Usage: $0 -w WAVE [-n MAX_TASKS]"
  echo "  -w WAVE       Wave number (required) - agents only claim tasks in this wave"
  echo "  -n MAX_TASKS  Stop each agent after completing this many tasks (default: unlimited)"
  exit 1
}

while getopts ":n:w:h" opt; do
  case "$opt" in
    n) MAX_TASKS="$OPTARG" ;;
    w) WAVE="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

if [ -z "$WAVE" ]; then
  echo "error: -w WAVE is required" >&2
  usage
fi

if ! [[ "$WAVE" =~ ^[0-9]+$ ]]; then
  echo "error: -w must be a non-negative integer" >&2
  exit 1
fi

if [ -n "$MAX_TASKS" ] && ! [[ "$MAX_TASKS" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: -n must be a positive integer" >&2
  exit 1
fi

CONTAINERS=$(docker ps --filter "name=^bqlite-agent-[0-9]+$" --format "{{.Names}}" | sort -V)

if [ -z "$CONTAINERS" ]; then
  echo "No running bqlite-agent containers found."
  echo "Run scripts/launch-fleet.sh first."
  exit 1
fi

COUNT=$(echo "$CONTAINERS" | wc -l | tr -d ' ')
SUFFIX=" (Wave ${WAVE} only)"
if [ -n "$MAX_TASKS" ]; then
  SUFFIX="${SUFFIX} (max $MAX_TASKS task(s) per agent)"
fi
echo "Attaching to $COUNT agent containers via cmux${SUFFIX}..."

# Build the docker exec command for an agent container. The wrapper script
# runs claude in a restart loop driven by the Stop hook's control markers.
agent_cmd() {
  local container="$1"
  local args="${WAVE}"
  if [ -n "$MAX_TASKS" ]; then
    args="${args} ${MAX_TASKS}"
  fi
  echo "docker exec -it -e IS_SANDBOX=1 -w /workspace ${container} /workspace/scripts/agent-wrapper.sh ${args}"
}

FIRST=true
for CONTAINER in $CONTAINERS; do
  CMD=$(agent_cmd "$CONTAINER")

  if [ "$FIRST" = true ]; then
    # First agent: create workspace with the command already running
    WORKSPACE=$(cmux new-workspace --name "bqlite agents" --command "$CMD" | grep -o 'workspace:[0-9]*')
    sleep 0.5
    # Discover the initial surface that new-workspace created for us
    SURFACE=$(cmux list-pane-surfaces --workspace "$WORKSPACE" 2>/dev/null | grep -o 'surface:[0-9]*' | head -1)
    FIRST=false
  else
    # Additional agents: create a new surface, then respawn it with the command
    SURFACE=$(cmux new-surface --type terminal --workspace "$WORKSPACE" | grep -o 'surface:[0-9]*')
    sleep 0.5
    cmux respawn-pane --surface "$SURFACE" --workspace "$WORKSPACE" --command "$CMD"
  fi

  # Name the tab after the agent
  if [ -n "$SURFACE" ]; then
    cmux rename-tab --workspace "$WORKSPACE" --surface "$SURFACE" "$CONTAINER" >/dev/null
  fi

  echo "  Tab created for $CONTAINER"
done

cmux select-workspace --workspace "$WORKSPACE"
cmux notify --title "bqlite fleet" --body "Fleet attached: $COUNT agents"
echo "Done. Switch to cmux to interact with agents."
