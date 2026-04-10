#!/usr/bin/env bash
set -euo pipefail

N="${1:-4}"
IMAGE="bqlite-agent"
REPO_URL="git@github.com:paladin8/bqlite.git"

# Validate Docker is running
if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is not running. Start Docker Desktop first."
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
    --mount type=bind,src=/run/host-services/ssh-auth.sock,target=/ssh-agent \
    -e SSH_AUTH_SOCK=/ssh-agent \
    -w /workspace \
    "$IMAGE" \
    bash -c "
      # Copy auth files to writable location (container runs as root)
      mkdir -p /root/.claude
      cp -r /home/vscode/.claude-host/* /root/.claude/ 2>/dev/null || true
      chmod -R 600 /root/.claude/* 2>/dev/null || true

      # Add GitHub to known hosts
      mkdir -p /root/.ssh
      ssh-keyscan -t ed25519 github.com >> /root/.ssh/known_hosts 2>/dev/null

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
