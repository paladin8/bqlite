#!/usr/bin/env bash
set -euo pipefail

N="${1:-4}"
IMAGE="bqlite-agent"
REPO_URL="git@github.com:paladin8/bqlite.git"

# Validate SSH agent is running
if [ -z "${SSH_AUTH_SOCK:-}" ]; then
  echo "ERROR: SSH_AUTH_SOCK is not set. Start your SSH agent first:"
  echo "  eval \$(ssh-agent -s) && ssh-add"
  exit 1
fi

# Build image (uses Docker cache after first run)
echo "Building devcontainer image..."
docker build -t "$IMAGE" -f .devcontainer/Dockerfile . -q

echo "Starting $N agent containers..."
for i in $(seq 1 "$N"); do
  NAME="bqlite-agent-$i"

  # Skip if already running
  if docker ps -q -f "name=^${NAME}$" 2>/dev/null | grep -q .; then
    echo "  $NAME: already running, skipping"
    continue
  fi

  # Remove stopped container with same name if it exists
  docker rm -f "$NAME" 2>/dev/null || true

  docker run -d \
    --name "$NAME" \
    -e AGENT_ID="agent-$i" \
    -v "$HOME/.claude:/home/vscode/.claude-host:ro" \
    -v "${SSH_AUTH_SOCK}:/ssh-agent" \
    -e SSH_AUTH_SOCK=/ssh-agent \
    -w /workspace \
    "$IMAGE" \
    bash -c "
      # Copy auth files to writable location
      mkdir -p /home/vscode/.claude
      cp -r /home/vscode/.claude-host/* /home/vscode/.claude/ 2>/dev/null || true

      # Clone and configure
      git clone $REPO_URL /workspace &&
      cd /workspace &&
      git config user.name \"bqlite-agent-$i\" &&
      git config user.email \"bqlite-agent-${i}@agent.local\" &&
      echo \"Container bqlite-agent-$i ready\" &&
      exec tail -f /dev/null
    "

  echo "  $NAME: started"
done

echo ""
echo "Fleet ready. Run: scripts/attach-fleet.sh"
