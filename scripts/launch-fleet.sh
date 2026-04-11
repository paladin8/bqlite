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

# OAuth token pass-through. On macOS, host credentials live in the Keychain,
# so the /home/vscode/.claude-host copy does not carry them. Inject a long-lived
# token via env var instead. Generate one with: claude setup-token
if [ -z "${CLAUDE_CODE_OAUTH_TOKEN:-}" ]; then
  echo "WARNING: CLAUDE_CODE_OAUTH_TOKEN is not set on the host."
  echo "  Agents may fail to authenticate inside the container."
  echo "  Run 'claude setup-token' on the host, then export the token before"
  echo "  re-running this script."
  echo ""
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
    -e IS_SANDBOX=1 \
    -e CLAUDE_CODE_OAUTH_TOKEN="${CLAUDE_CODE_OAUTH_TOKEN:-}" \
    -v "$HOME/.claude:/home/vscode/.claude-host:ro" \
    --mount type=bind,src=/run/host-services/ssh-auth.sock,target=/ssh-agent \
    -e SSH_AUTH_SOCK=/ssh-agent \
    -w /workspace \
    "$IMAGE" \
    bash -c "
      # Copy auth files to writable location (container runs as root)
      mkdir -p /root/.claude
      cp -r /home/vscode/.claude-host/* /root/.claude/ 2>/dev/null || true

      # Merge bypass-permissions + cmux notification hook into settings.json
      # (preserves host config if present)
      PATCH='{\"skipDangerousModePermissionPrompt\":true,\"permissions\":{\"defaultMode\":\"bypassPermissions\"},\"hooks\":{\"Notification\":[{\"matcher\":\"\",\"hooks\":[{\"type\":\"command\",\"command\":\"/root/.claude/hooks/cmux-notify.sh\"}]}]}}'
      if [ -f /root/.claude/settings.json ]; then
        jq --argjson patch \"\$PATCH\" '. + \$patch' /root/.claude/settings.json > /tmp/settings.json && mv /tmp/settings.json /root/.claude/settings.json
      else
        echo \"\$PATCH\" > /root/.claude/settings.json
      fi

      # Pre-seed /root/.claude.json: skip onboarding + trust /workspace
      jq -n '{
        hasCompletedOnboarding: true,
        lastOnboardingVersion: \"2.1.63\",
        projects: {
          \"/workspace\": {
            allowedTools: [],
            mcpContextUris: [],
            mcpServers: {},
            enabledMcpjsonServers: [],
            disabledMcpjsonServers: [],
            hasTrustDialogAccepted: true,
            projectOnboardingSeenCount: 1,
            hasClaudeMdExternalIncludesApproved: true,
            hasClaudeMdExternalIncludesWarningShown: true,
            hasCompletedProjectOnboarding: true
          }
        }
      }' > /root/.claude.json

      chmod -R 600 /root/.claude/* /root/.claude.json 2>/dev/null || true

      # Ensure Claude Code plugins are present. The host-copy above may have
      # left installed_plugins.json in whatever state the host has, which does
      # not necessarily include what the fleet needs. Re-add the marketplace
      # and (re)install; failures are tolerated so an already-present
      # marketplace or plugin does not abort container startup.
      claude plugin marketplace add anthropics/claude-plugins-official || true
      claude plugin install rust-analyzer-lsp@claude-plugins-official || true
      claude plugin install superpowers@claude-plugins-official || true

      # Add GitHub to known hosts
      mkdir -p /root/.ssh
      ssh-keyscan -t ed25519 github.com >> /root/.ssh/known_hosts 2>/dev/null

      # Clone and configure
      git clone $REPO_URL /workspace &&
      cd /workspace &&
      git config user.name \"Jeffrey Wang\" &&
      git config user.email \"jeffreyw314159@gmail.com\" &&
      mkdir -p /root/.claude/hooks &&
      install -m 755 /workspace/scripts/cmux-notify.sh /root/.claude/hooks/cmux-notify.sh &&
      echo \"Container bqlite-agent-$i ready\" &&
      exec tail -f /dev/null
    "

  echo "  $NAME: started"
done

echo ""
echo "Fleet ready. Run: scripts/attach-fleet.sh"
