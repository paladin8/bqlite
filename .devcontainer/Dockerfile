FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# cmake is needed for building certain native dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    && rm -rf /var/lib/apt/lists/*
