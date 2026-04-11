#!/usr/bin/env bash
set -euo pipefail

RUNNING=$(docker ps -q --filter "name=^bqlite-agent-[0-9]+$")

if [ -z "$RUNNING" ]; then
  echo "No running bqlite-agent containers found."
  exit 0
fi

COUNT=$(echo "$RUNNING" | wc -l | tr -d ' ')
echo "Stopping $COUNT agent containers..."

docker ps -q --filter "name=^bqlite-agent-[0-9]+$" | xargs docker stop || true
docker ps -aq --filter "name=^bqlite-agent-[0-9]+$" | xargs docker rm || true

echo "Fleet stopped and cleaned up."
