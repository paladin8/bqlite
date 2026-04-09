FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

RUN apt-get update && apt-get install -y \
    cmake \
    git \
    openssh-client \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code
RUN curl -fsSL https://claude.ai/install.sh | bash

# Pre-warm cargo registry to speed up first build
RUN cargo search --limit 1 serde

# Python tooling for later waves
RUN pip install maturin pytest
