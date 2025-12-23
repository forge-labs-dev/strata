# Strata: Snapshot-aware serving layer for Iceberg tables
#
# Build: DOCKER_BUILDKIT=1 docker build -t strata .
#
# Run (recommended - persists cache and metadata across restarts):
#   docker run --rm -p 8765:8765 \
#     -v strata_state:/home/strata/.strata \
#     -v /path/to/warehouse:/data \
#     strata
#
# Volumes:
#   /home/strata/.strata  - State directory (cache + metadata), use named volume
#   /data                 - Mount your Iceberg warehouse here
#
# Without the named volume, cache is lost on container restart!
#
# Multi-stage build:
# 1. Builder: uses uv + Rust to build the wheel (with BuildKit caching)
# 2. Runtime: minimal image with just the wheel installed
#
# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Builder (uv + Rust)
# =============================================================================
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# Install Rust and build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install Rust with pinned toolchain for reproducible builds
ARG RUST_VERSION=1.92.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy source code
WORKDIR /build
COPY . .

# Build the wheel using uv (with BuildKit cache mounts for faster rebuilds)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    uv build --wheel --out-dir dist

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS runtime

# Copy and install the wheel (as root, before switching user)
COPY --from=builder /build/dist/*.whl /tmp/
RUN uv pip install --system --no-cache /tmp/*.whl && rm /tmp/*.whl

# Create non-root user
RUN useradd --create-home --shell /bin/bash strata

# Create directories for cache and data (as root, then chown)
RUN mkdir -p /home/strata/.strata/cache /data && \
    chown -R strata:strata /home/strata/.strata /data

# Switch to non-root user
USER strata
WORKDIR /home/strata

# Declare volumes for persistence across container restarts
# - /home/strata/.strata: State directory (cache + meta.sqlite)
# - /data: Mount point for local warehouse data
VOLUME ["/home/strata/.strata", "/data"]

# Python runtime settings
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Strata configuration defaults (can be overridden)
ENV STRATA_HOST=0.0.0.0
ENV STRATA_PORT=8765
ENV STRATA_CACHE_DIR=/home/strata/.strata/cache
ENV STRATA_METADATA_DB=/home/strata/.strata/meta.sqlite

# Health check (uses stdlib to avoid extra dependencies)
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8765/health').read()"

# Expose the default port
EXPOSE 8765

# Run the server (python -m is more robust for K8s than console scripts)
CMD ["python", "-m", "strata"]
