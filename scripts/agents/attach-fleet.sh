#!/usr/bin/env bash
set -euo pipefail

MAX_TASKS=""
WAVE=""
EASY_AGENTS="0"
HARD_AGENTS="0"

usage() {
  echo "Usage: $0 -w WAVE [--easy N] [--hard N] [-n MAX_TASKS]"
  echo "  -w WAVE       Wave number (required) - agents only claim tasks in this wave"
  echo "  --easy N      Number of containers to attach as EASY agents (sonnet high)"
  echo "  --hard N      Number of containers to attach as HARD agents (opus high)"
  echo "  -n MAX_TASKS  Stop each agent after completing this many tasks (default: unlimited)"
  echo "                 Containers are assigned in sorted order: EASY first, then HARD"
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    -n)
      MAX_TASKS="${2:-}"
      shift 2
      ;;
    -w)
      WAVE="${2:-}"
      shift 2
      ;;
    --easy)
      EASY_AGENTS="${2:-}"
      shift 2
      ;;
    --hard)
      HARD_AGENTS="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
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

if ! [[ "$EASY_AGENTS" =~ ^[0-9]+$ ]]; then
  echo "error: --easy must be a non-negative integer" >&2
  exit 1
fi

if ! [[ "$HARD_AGENTS" =~ ^[0-9]+$ ]]; then
  echo "error: --hard must be a non-negative integer" >&2
  exit 1
fi

REQUESTED_TOTAL=$((EASY_AGENTS + HARD_AGENTS))
if [ "$REQUESTED_TOTAL" -le 0 ]; then
  echo "error: request at least one agent via --easy and/or --hard" >&2
  exit 1
fi

CONTAINERS=$(docker ps --filter "name=^bqlite-agent-[0-9]+$" --format "{{.Names}}" | sort -V)

if [ -z "$CONTAINERS" ]; then
  echo "No running bqlite-agent containers found."
  echo "Run scripts/agents/launch-fleet.sh first."
  exit 1
fi

COUNT=$(echo "$CONTAINERS" | wc -l | tr -d ' ')
if [ "$REQUESTED_TOTAL" -gt "$COUNT" ]; then
  echo "error: requested $REQUESTED_TOTAL agents (${EASY_AGENTS} easy, ${HARD_AGENTS} hard) but only $COUNT containers are running" >&2
  exit 1
fi

SUFFIX=" (Wave ${WAVE} only; easy=${EASY_AGENTS}, hard=${HARD_AGENTS})"
if [ -n "$MAX_TASKS" ]; then
  SUFFIX="${SUFFIX} (max $MAX_TASKS task(s) per agent)"
fi
echo "Attaching to $REQUESTED_TOTAL of $COUNT agent containers via cmux${SUFFIX}..."

# launch-fleet.sh returns as soon as `docker run -d` finishes, but the
# in-container setup (git clone, plugin install, hook install) runs in the
# background. Attaching before that completes produces a "stat
# /workspace/scripts/agents/agent_wrapper.py: no such file" exec error.
# Poll each container's /workspace for the wrapper before proceeding.
wait_for_ready() {
  local container="$1"
  local deadline=$(($(date +%s) + 120))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if docker exec "$container" test -f /workspace/scripts/agents/agent_wrapper.py 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "error: $container not ready after 120s — /workspace/scripts/agents/agent_wrapper.py never appeared" >&2
  echo "  check 'docker logs $container' for setup failures" >&2
  return 1
}

# Build the docker exec command for an agent container. The Python wrapper
# runs claude once per task; task selection, batching, and NEEDS INPUT
# capture are owned by agent_wrapper.py.
agent_cmd() {
  local container="$1"
  local difficulty_pool="$2"
  local args="${WAVE} ${difficulty_pool}"
  if [ -n "$MAX_TASKS" ]; then
    args="${args} ${MAX_TASKS}"
  fi
  echo "docker exec -it -e IS_SANDBOX=1 -e TASK_DIFFICULTY_POOL=${difficulty_pool} -w /workspace ${container} python3 /workspace/scripts/agents/agent_wrapper.py ${args}"
}

declare -a CONTAINER_ARRAY=()
while IFS= read -r container; do
  [ -n "$container" ] && CONTAINER_ARRAY+=("$container")
done <<<"$CONTAINERS"

attach_one() {
  local idx="$1"
  local difficulty_pool="$2"
  local container="${CONTAINER_ARRAY[$idx]}"
  local cmd pool_label
  wait_for_ready "$container" || exit 1
  cmd=$(agent_cmd "$container" "$difficulty_pool")
  pool_label=$(printf '%s' "$difficulty_pool" | tr '[:upper:]' '[:lower:]')

  if [ "$idx" -eq 0 ]; then
    WORKSPACE=$(cmux new-workspace --name "bqlite agents" --command "$cmd" | grep -o 'workspace:[0-9]*')
    sleep 0.5
    SURFACE=$(cmux list-pane-surfaces --workspace "$WORKSPACE" 2>/dev/null | grep -o 'surface:[0-9]*' | head -1)
  else
    SURFACE=$(cmux new-surface --type terminal --workspace "$WORKSPACE" | grep -o 'surface:[0-9]*')
    sleep 0.5
    cmux respawn-pane --surface "$SURFACE" --workspace "$WORKSPACE" --command "$cmd"
  fi

  if [ -n "$SURFACE" ]; then
    cmux rename-tab --workspace "$WORKSPACE" --surface "$SURFACE" "${container} (${pool_label})" >/dev/null
  fi

  echo "  Tab created for $container [$difficulty_pool]"
}

for ((i = 0; i < EASY_AGENTS; i++)); do
  attach_one "$i" "EASY"
done

for ((i = 0; i < HARD_AGENTS; i++)); do
  attach_one "$((EASY_AGENTS + i))" "HARD"
done

if [ "$REQUESTED_TOTAL" -lt "$COUNT" ]; then
  echo "  Skipped $((COUNT - REQUESTED_TOTAL)) running container(s) that were not assigned to a pool"
fi

cmux select-workspace --workspace "$WORKSPACE"
cmux notify --title "bqlite fleet" --body "Fleet attached: $REQUESTED_TOTAL agents (${EASY_AGENTS} easy, ${HARD_AGENTS} hard)"
echo "Done. Switch to cmux to interact with agents."
