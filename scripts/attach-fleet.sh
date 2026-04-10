#!/usr/bin/env bash
set -euo pipefail

CONTAINERS=$(docker ps --filter "name=^bqlite-agent-[0-9]+$" --format "{{.Names}}" | sort -V)

if [ -z "$CONTAINERS" ]; then
  echo "No running bqlite-agent containers found."
  echo "Run scripts/launch-fleet.sh first."
  exit 1
fi

COUNT=$(echo "$CONTAINERS" | wc -l | tr -d ' ')
echo "Attaching to $COUNT agent containers via cmux..."

# Build the docker exec command for an agent container
agent_cmd() {
  local container="$1"
  local prompt="You are ${container}, an autonomous agent building bqlite. Read AGENTS.md for your complete operating protocol. Begin the agent loop now."
  echo "docker exec -it -w /workspace ${container} claude --system-prompt '${prompt}'"
}

FIRST=true
for CONTAINER in $CONTAINERS; do
  CMD=$(agent_cmd "$CONTAINER")

  if [ "$FIRST" = true ]; then
    # First agent: create workspace with the command already running
    WORKSPACE=$(cmux new-workspace --name "bqlite agents" --command "$CMD" | grep -o 'workspace:[0-9]*')
    FIRST=false
  else
    # Additional agents: create a new surface, then respawn it with the command
    SURFACE=$(cmux new-surface --type terminal --workspace "$WORKSPACE" | grep -o 'surface:[0-9]*')
    sleep 0.5
    cmux respawn-pane --surface "$SURFACE" --workspace "$WORKSPACE" --command "$CMD"
  fi

  echo "  Tab created for $CONTAINER"
done

cmux select-workspace --workspace "$WORKSPACE"
cmux notify --title "bqlite fleet" --body "Fleet attached: $COUNT agents"
echo "Done. Switch to cmux to interact with agents."
