#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

NOTEBOOK_PARENT_DIR="/tmp/strata-notebooks"
SERVER_LOG="/tmp/strata-devcontainer.log"
SERVER_URL="http://127.0.0.1:8765/health"

mkdir -p "$NOTEBOOK_PARENT_DIR"

if ! curl -fsS "$SERVER_URL" >/dev/null 2>&1; then
    nohup env \
        STRATA_DEPLOYMENT_MODE=personal \
        uv run python -m strata \
        >"$SERVER_LOG" 2>&1 &
fi

for _ in $(seq 1 30); do
    if curl -fsS "$SERVER_URL" >/dev/null 2>&1; then
        exit 0
    fi
    sleep 1
done

echo "Strata server did not become ready; see $SERVER_LOG" >&2
exit 1
