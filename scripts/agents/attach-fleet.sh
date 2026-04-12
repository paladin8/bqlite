#!/usr/bin/env bash
set -euo pipefail

MAX_TASKS=""
WAVE=""
EASY_AGENTS="0"
HARD_AGENTS="0"
declare -a ATTACH_EASY=()
declare -a ATTACH_HARD=()

usage() {
  cat <<EOF
Usage: $0 -w WAVE [mode flags] [-n MAX_TASKS]

  -w WAVE             Wave number (required) — agents only claim tasks in this wave
  -n MAX_TASKS        Stop each agent after N successful tasks (default: unlimited)

Count-based mode (initial fleet attachment):
  --easy N            First N containers (sorted by number) get the EASY pool
  --hard N            Next N containers get the HARD pool

Targeted mode (recovery or per-container attachment):
  --attach-easy NUM   Attach bqlite-agent-NUM as an EASY agent (repeatable)
  --attach-hard NUM   Attach bqlite-agent-NUM as a HARD agent (repeatable)

Count-based (--easy/--hard) and targeted (--attach-easy/--attach-hard) flags
cannot be combined in a single invocation.

Targeted mode reuses an existing "bqlite agents" cmux workspace if one is
already open and just appends new tabs to it; if no such workspace exists,
a new one is created. This lets you recover a crashed agent without
disturbing tabs for other containers still running in the same fleet.

Examples:
  # Initial fleet: 3 easy + 1 hard, 1 task each
  $0 -w 2 --easy 3 --hard 1 -n 1

  # Recovery: re-attach just agent-4 as EASY in the existing workspace
  $0 -w 2 --attach-easy 4 -n 1

  # Recovery: re-attach agents 3 and 5 as HARD, 4 as EASY
  $0 -w 2 --attach-hard 3 --attach-hard 5 --attach-easy 4 -n 1
EOF
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
    --attach-easy)
      ATTACH_EASY+=("${2:-}")
      shift 2
      ;;
    --attach-hard)
      ATTACH_HARD+=("${2:-}")
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

# Mode resolution: count-based vs targeted, mutually exclusive.
COUNT_MODE=false
TARGETED_MODE=false
if [ "$EASY_AGENTS" != "0" ] || [ "$HARD_AGENTS" != "0" ]; then
  COUNT_MODE=true
fi
if [ "${#ATTACH_EASY[@]}" -gt 0 ] || [ "${#ATTACH_HARD[@]}" -gt 0 ]; then
  TARGETED_MODE=true
fi

if $COUNT_MODE && $TARGETED_MODE; then
  echo "error: cannot combine --easy/--hard (count-based) with --attach-easy/--attach-hard (targeted)" >&2
  exit 1
fi

if ! $COUNT_MODE && ! $TARGETED_MODE; then
  echo "error: must specify either --easy/--hard (count) or --attach-easy/--attach-hard (targeted)" >&2
  usage
fi

if $COUNT_MODE; then
  if ! [[ "$EASY_AGENTS" =~ ^[0-9]+$ ]]; then
    echo "error: --easy must be a non-negative integer" >&2
    exit 1
  fi
  if ! [[ "$HARD_AGENTS" =~ ^[0-9]+$ ]]; then
    echo "error: --hard must be a non-negative integer" >&2
    exit 1
  fi
fi

CONTAINERS=$(docker ps --filter "name=^bqlite-agent-[0-9]+$" --format "{{.Names}}" | sort -V)

if [ -z "$CONTAINERS" ]; then
  echo "No running bqlite-agent containers found."
  echo "Run scripts/agents/launch-fleet.sh first."
  exit 1
fi

COUNT=$(echo "$CONTAINERS" | wc -l | tr -d ' ')

declare -a CONTAINER_ARRAY=()
while IFS= read -r container; do
  [ -n "$container" ] && CONTAINER_ARRAY+=("$container")
done <<<"$CONTAINERS"

# Build ordered (container, pool) assignment lists for the chosen mode.
declare -a ASSIGNMENTS_CONTAINER=()
declare -a ASSIGNMENTS_POOL=()

if $COUNT_MODE; then
  REQUESTED_TOTAL=$((EASY_AGENTS + HARD_AGENTS))
  if [ "$REQUESTED_TOTAL" -le 0 ]; then
    echo "error: request at least one agent via --easy and/or --hard" >&2
    exit 1
  fi
  if [ "$REQUESTED_TOTAL" -gt "$COUNT" ]; then
    echo "error: requested $REQUESTED_TOTAL agents (${EASY_AGENTS} easy, ${HARD_AGENTS} hard) but only $COUNT containers are running" >&2
    exit 1
  fi
  idx=0
  for ((i = 0; i < EASY_AGENTS; i++)); do
    ASSIGNMENTS_CONTAINER+=("${CONTAINER_ARRAY[$idx]}")
    ASSIGNMENTS_POOL+=("EASY")
    idx=$((idx + 1))
  done
  for ((i = 0; i < HARD_AGENTS; i++)); do
    ASSIGNMENTS_CONTAINER+=("${CONTAINER_ARRAY[$idx]}")
    ASSIGNMENTS_POOL+=("HARD")
    idx=$((idx + 1))
  done
  BANNER_MODE="easy=${EASY_AGENTS}, hard=${HARD_AGENTS}"
else
  # Targeted mode: each --attach-easy/--attach-hard value is a container
  # number (e.g. 4 → bqlite-agent-4). Validate each one is a running
  # container before attaching anything.
  resolve_target() {
    local n="$1"
    local pool="$2"
    if ! [[ "$n" =~ ^[1-9][0-9]*$ ]]; then
      local flag
      flag=$(printf '%s' "$pool" | tr '[:upper:]' '[:lower:]')
      echo "error: --attach-${flag} argument must be a positive integer (container number), got '$n'" >&2
      exit 1
    fi
    local container="bqlite-agent-$n"
    if ! docker ps --format "{{.Names}}" --filter "name=^${container}$" 2>/dev/null | grep -qx "$container"; then
      echo "error: $container is not running; launch it first with scripts/agents/launch-fleet.sh" >&2
      exit 1
    fi
    ASSIGNMENTS_CONTAINER+=("$container")
    ASSIGNMENTS_POOL+=("$pool")
  }
  # The ${ARRAY[@]+"${ARRAY[@]}"} dance is required to iterate an empty
  # array cleanly on bash 3.2 (the default macOS host bash) under `set -u`.
  for n in ${ATTACH_EASY[@]+"${ATTACH_EASY[@]}"}; do resolve_target "$n" "EASY"; done
  for n in ${ATTACH_HARD[@]+"${ATTACH_HARD[@]}"}; do resolve_target "$n" "HARD"; done
  BANNER_MODE="easy=${#ATTACH_EASY[@]} (targeted), hard=${#ATTACH_HARD[@]} (targeted)"
fi

REQUESTED_TOTAL=${#ASSIGNMENTS_CONTAINER[@]}
SUFFIX=" (Wave ${WAVE}; ${BANNER_MODE})"
if [ -n "$MAX_TASKS" ]; then
  SUFFIX="${SUFFIX} (max $MAX_TASKS task(s) per agent)"
fi
echo "Attaching $REQUESTED_TOTAL agent container(s) via cmux${SUFFIX}..."

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
#
# In targeted/recovery mode (cleanup=clean), the command first runs
# reset-worktree.sh inside the container so a dirty state from a crashed
# prior run does not immediately crash the new wrapper via
# require_clean_worktree(). In count-based mode the containers are assumed
# to be fresh clones and no reset is needed.
agent_cmd() {
  local container="$1"
  local difficulty_pool="$2"
  local cleanup="${3:-}"
  local args="${WAVE} ${difficulty_pool}"
  if [ -n "$MAX_TASKS" ]; then
    args="${args} ${MAX_TASKS}"
  fi
  if [ "$cleanup" = "clean" ]; then
    echo "docker exec -it -e IS_SANDBOX=1 -e TASK_DIFFICULTY_POOL=${difficulty_pool} -w /workspace ${container} bash -lc 'bash /workspace/scripts/agents/reset-worktree.sh && exec python3 /workspace/scripts/agents/agent_wrapper.py ${args}'"
  else
    echo "docker exec -it -e IS_SANDBOX=1 -e TASK_DIFFICULTY_POOL=${difficulty_pool} -w /workspace ${container} python3 /workspace/scripts/agents/agent_wrapper.py ${args}"
  fi
}

# Look up an existing "bqlite agents" cmux workspace if one is already open,
# so targeted/recovery runs add tabs to it instead of spawning a duplicate
# workspace. Prints the workspace ref (e.g. workspace:30) or empty if none.
find_existing_workspace() {
  cmux list-workspaces 2>/dev/null \
    | grep -E '[[:space:]]+bqlite agents([[:space:]]|$)' \
    | grep -oE 'workspace:[0-9]+' \
    | head -1
}

WORKSPACE=""

attach_one() {
  local container="$1"
  local difficulty_pool="$2"
  local cmd pool_label surface cleanup=""
  if $TARGETED_MODE; then
    cleanup="clean"
  fi
  wait_for_ready "$container" || exit 1
  cmd=$(agent_cmd "$container" "$difficulty_pool" "$cleanup")
  pool_label=$(printf '%s' "$difficulty_pool" | tr '[:upper:]' '[:lower:]')

  if [ -z "$WORKSPACE" ]; then
    # First agent to attach in this invocation. Try to reuse an existing
    # workspace from a prior run so we land in the same cmux tab group.
    WORKSPACE=$(find_existing_workspace || true)
    if [ -n "$WORKSPACE" ]; then
      echo "  Reusing existing workspace ${WORKSPACE}"
      surface=$(cmux new-surface --type terminal --workspace "$WORKSPACE" | grep -o 'surface:[0-9]*')
      sleep 0.5
      cmux respawn-pane --surface "$surface" --workspace "$WORKSPACE" --command "$cmd"
    else
      WORKSPACE=$(cmux new-workspace --name "bqlite agents" --command "$cmd" | grep -o 'workspace:[0-9]*')
      sleep 0.5
      surface=$(cmux list-pane-surfaces --workspace "$WORKSPACE" 2>/dev/null | grep -o 'surface:[0-9]*' | head -1)
    fi
  else
    surface=$(cmux new-surface --type terminal --workspace "$WORKSPACE" | grep -o 'surface:[0-9]*')
    sleep 0.5
    cmux respawn-pane --surface "$surface" --workspace "$WORKSPACE" --command "$cmd"
  fi

  if [ -n "$surface" ]; then
    cmux rename-tab --workspace "$WORKSPACE" --surface "$surface" "${container} (${pool_label})" >/dev/null
  fi

  echo "  Tab attached: ${container} [${difficulty_pool}]"
}

for ((i = 0; i < ${#ASSIGNMENTS_CONTAINER[@]}; i++)); do
  attach_one "${ASSIGNMENTS_CONTAINER[$i]}" "${ASSIGNMENTS_POOL[$i]}"
done

if $COUNT_MODE && [ "$REQUESTED_TOTAL" -lt "$COUNT" ]; then
  echo "  Skipped $((COUNT - REQUESTED_TOTAL)) running container(s) that were not assigned to a pool"
fi

cmux select-workspace --workspace "$WORKSPACE"
cmux notify --title "bqlite fleet" --body "Fleet attached: $REQUESTED_TOTAL agent(s)"
echo "Done. Switch to cmux to interact with agents."
