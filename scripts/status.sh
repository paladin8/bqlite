#!/usr/bin/env bash
set -euo pipefail

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# Shallow clone to read task state
git clone --depth 1 -q git@github.com:paladin8/bqlite.git "$TMPDIR" 2>/dev/null

echo "=== Active Tasks ==="
ACTIVE_COUNT=0
for lock in "$TMPDIR"/tasks/active/*.lock; do
  [ -f "$lock" ] || continue
  ACTIVE_COUNT=$((ACTIVE_COUNT + 1))
  jq -r '"\(.agent_id)  \(.task_id)  \(.description)  (since \(.claimed_at))"' "$lock"
done
if [ "$ACTIVE_COUNT" -eq 0 ]; then
  echo "  (none)"
fi

echo ""
echo "=== Completed Tasks ==="
DONE_COUNT=0
for done in "$TMPDIR"/tasks/completed/*.done; do
  [ -f "$done" ] || continue
  DONE_COUNT=$((DONE_COUNT + 1))
  jq -r '"\(.task_id)  \(.description)  (completed \(.completed_at // "unknown"))"' "$done"
done
if [ "$DONE_COUNT" -eq 0 ]; then
  echo "  (none)"
fi

echo ""
echo "=== Container Status ==="
docker ps --filter "name=bqlite-agent-" --format "{{.Names}}\t{{.Status}}" 2>/dev/null || echo "  (docker not available)"

echo ""
echo "Active: $ACTIVE_COUNT  Completed: $DONE_COUNT"
