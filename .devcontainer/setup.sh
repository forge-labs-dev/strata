#!/usr/bin/env bash
set -euo pipefail

# Install uv (Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# Install Python dependencies and build Rust extension
uv sync

# Build the frontend
cd frontend
npm ci
npm run build
