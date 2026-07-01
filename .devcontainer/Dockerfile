# .devcontainer/Dockerfile
FROM python:3.13-slim

# Install system utilities and development tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root developer user
RUN groupadd -g 1000 vscode && \
    useradd -u 1000 -g vscode -s /bin/bash -m vscode

# Preinstall python development tools globally
RUN pip install --no-cache-dir \
    ruff \
    black \
    mypy \
    bandit \
    coverage \
    pytest

USER vscode
