#!/usr/bin/env bash
set -euo pipefail

RUNNING=$(docker ps -q --filter "name=bqlite-agent-")

if [ -z "$RUNNING" ]; then
  echo "No running bqlite-agent containers found."
  exit 0
fi

COUNT=$(echo "$RUNNING" | wc -l | tr -d ' ')
echo "Stopping $COUNT agent containers..."

docker ps -q --filter "name=bqlite-agent-" | xargs docker stop
docker ps -aq --filter "name=bqlite-agent-" | xargs docker rm

echo "Fleet stopped and cleaned up."
