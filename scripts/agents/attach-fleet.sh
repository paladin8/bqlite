#!/usr/bin/env bash
set -euo pipefail

MAX_TASKS=""
WAVE=""
COUNT="0"
declare -a ATTACH_TARGETS=()

usage() {
  cat <<EOF
Usage: $0 -w WAVE [mode flags] [-n MAX_TASKS]

  -w WAVE             Wave number (required) — agents only claim tasks in this wave
  -n MAX_TASKS        Stop each agent after N successful tasks (default: unlimited)

Count-based mode (initial fleet attachment):
  -c N, --count N     Attach the first N running containers (sorted by number)

Targeted mode (recovery or per-container attachment):
  --attach NUM        Attach bqlite-agent-NUM (repeatable)

Count-based (-c) and targeted (--attach) flags cannot be combined in a
single invocation.

Difficulty is now per-task: each wrapper claims any dep-satisfied tagged
task in the wave and picks the claude model (Sonnet for [EASY], Opus 4.7 for
[HARD]) based on the task's own tag. There is no pool routing at the
container level anymore.

Targeted mode reuses an existing "bqlite agents" cmux workspace if one is
already open and appends new tabs to it; if no such workspace exists, a
new one is created. This lets you recover a crashed agent without
disturbing tabs for other containers still running. Targeted mode also
runs reset-worktree.sh inside the container before the wrapper starts, so
a dirty worktree from a prior crash cannot trip require_clean_worktree().

Examples:
  # Initial fleet: attach 4 containers, 1 successful task each
  $0 -w 2 -c 4 -n 1

  # Recovery: re-attach just agent-4 in the existing workspace
  $0 -w 2 --attach 4 -n 1

  # Recovery: re-attach agents 3, 4, and 5 at once
  $0 -w 2 --attach 3 --attach 4 --attach 5 -n 1
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
    -c|--count)
      COUNT="${2:-}"
      shift 2
      ;;
    --attach)
      ATTACH_TARGETS+=("${2:-}")
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
if [ "$COUNT" != "0" ]; then
  COUNT_MODE=true
fi
if [ "${#ATTACH_TARGETS[@]}" -gt 0 ]; then
  TARGETED_MODE=true
fi

if $COUNT_MODE && $TARGETED_MODE; then
  echo "error: cannot combine -c/--count (count-based) with --attach (targeted)" >&2
  exit 1
fi

if ! $COUNT_MODE && ! $TARGETED_MODE; then
  echo "error: must specify either -c/--count (count) or --attach (targeted)" >&2
  usage
fi

if $COUNT_MODE && ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: -c/--count must be a positive integer" >&2
  exit 1
fi

CONTAINERS=$(docker ps --filter "name=^bqlite-agent-[0-9]+$" --format "{{.Names}}" | sort -V)

if [ -z "$CONTAINERS" ]; then
  echo "No running bqlite-agent containers found."
  echo "Run scripts/agents/launch-fleet.sh first."
  exit 1
fi

CONTAINER_COUNT=$(echo "$CONTAINERS" | wc -l | tr -d ' ')

declare -a CONTAINER_ARRAY=()
while IFS= read -r container; do
  [ -n "$container" ] && CONTAINER_ARRAY+=("$container")
done <<<"$CONTAINERS"

# Build the ordered list of container names to attach for the chosen mode.
declare -a ASSIGNMENTS_CONTAINER=()

if $COUNT_MODE; then
  if [ "$COUNT" -gt "$CONTAINER_COUNT" ]; then
    echo "error: requested $COUNT containers but only $CONTAINER_COUNT are running" >&2
    exit 1
  fi
  for ((i = 0; i < COUNT; i++)); do
    ASSIGNMENTS_CONTAINER+=("${CONTAINER_ARRAY[$i]}")
  done
  BANNER_MODE="count=${COUNT}"
else
  # Targeted mode: each --attach value is a container number. Validate
  # each one is a running container before attaching anything.
  for n in ${ATTACH_TARGETS[@]+"${ATTACH_TARGETS[@]}"}; do
    if ! [[ "$n" =~ ^[1-9][0-9]*$ ]]; then
      echo "error: --attach argument must be a positive integer (container number), got '$n'" >&2
      exit 1
    fi
    container="bqlite-agent-$n"
    if ! docker ps --format "{{.Names}}" --filter "name=^${container}$" 2>/dev/null | grep -qx "$container"; then
      echo "error: $container is not running; launch it first with scripts/agents/launch-fleet.sh" >&2
      exit 1
    fi
    ASSIGNMENTS_CONTAINER+=("$container")
  done
  BANNER_MODE="targeted=${#ATTACH_TARGETS[@]}"
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
# runs claude once per task and picks the model from the task's [EASY] /
# [HARD] tag — there is no TASK_DIFFICULTY_POOL assignment at the
# container level anymore.
#
# In targeted/recovery mode (cleanup=clean), the command first runs
# reset-worktree.sh inside the container so a dirty state from a crashed
# prior run does not immediately crash the new wrapper via
# require_clean_worktree(). In count-based mode the containers are assumed
# to be fresh clones and no reset is needed.
agent_cmd() {
  local container="$1"
  local cleanup="${2:-}"
  local args="${WAVE}"
  if [ -n "$MAX_TASKS" ]; then
    args="${args} ${MAX_TASKS}"
  fi
  if [ "$cleanup" = "clean" ]; then
    echo "docker exec -it -e IS_SANDBOX=1 -w /workspace ${container} bash -lc 'bash /workspace/scripts/agents/reset-worktree.sh && exec python3 /workspace/scripts/agents/agent_wrapper.py ${args}'"
  else
    echo "docker exec -it -e IS_SANDBOX=1 -w /workspace ${container} python3 /workspace/scripts/agents/agent_wrapper.py ${args}"
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
  local cmd surface cleanup=""
  if $TARGETED_MODE; then
    cleanup="clean"
  fi
  wait_for_ready "$container" || exit 1
  cmd=$(agent_cmd "$container" "$cleanup")

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
    cmux rename-tab --workspace "$WORKSPACE" --surface "$surface" "$container" >/dev/null
  fi

  echo "  Tab attached: ${container}"
}

for ((i = 0; i < ${#ASSIGNMENTS_CONTAINER[@]}; i++)); do
  attach_one "${ASSIGNMENTS_CONTAINER[$i]}"
done

if $COUNT_MODE && [ "$REQUESTED_TOTAL" -lt "$CONTAINER_COUNT" ]; then
  echo "  Skipped $((CONTAINER_COUNT - REQUESTED_TOTAL)) running container(s) that were not assigned"
fi

cmux select-workspace --workspace "$WORKSPACE"
cmux notify --title "bqlite fleet" --body "Fleet attached: $REQUESTED_TOTAL agent(s)"
echo "Done. Switch to cmux to interact with agents."
