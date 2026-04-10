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

# Create a cmux workspace for the fleet
WORKSPACE=$(cmux new-workspace "bqlite agents" | grep -o 'workspace:[0-9]*')

for CONTAINER in $CONTAINERS; do
  AGENT_NUM="${CONTAINER##*-}"
  SURFACE=$(cmux new-surface --type terminal --workspace "$WORKSPACE" | grep -o 'surface:[0-9]*')

  SYSTEM_PROMPT="You are ${CONTAINER}, an autonomous agent building bqlite. Read AGENTS.md for your complete operating protocol. Begin the agent loop now."

  cmux send --surface "$SURFACE" \
    "docker exec -it -w /workspace $CONTAINER claude --system-prompt '$SYSTEM_PROMPT'"
  cmux send-key --surface "$SURFACE" enter

  echo "  Tab created for $CONTAINER"
done

cmux notify "Fleet attached: $COUNT agents"
echo "Done. Switch to cmux to interact with agents."
