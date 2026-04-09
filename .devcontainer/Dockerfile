FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

RUN apt-get update && apt-get install -y \
    cmake \
    git \
    openssh-client \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code
RUN curl -fsSL https://claude.ai/install.sh | bash
ENV PATH="/root/.local/bin:/home/vscode/.local/bin:${PATH}"

# Pre-warm cargo registry to speed up first build
RUN cargo search --limit 1 serde

# Note: Python tooling (maturin, pytest) is installed via
# postCreateCommand in devcontainer.json, after the Python
# devcontainer feature makes pip available.
