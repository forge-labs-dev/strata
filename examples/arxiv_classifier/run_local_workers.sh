#!/usr/bin/env bash
# Launch two Strata notebook HTTP executors on localhost so the arXiv
# classifier demo can dispatch cells to different workers without cloud
# deployment. Each process is just `strata.notebook.remote_executor` bound
# to a different port.
#
#   df-cluster → http://127.0.0.1:9000/v1/execute (see notebook.toml)
#   gpu-fly    → http://127.0.0.1:9001/v1/execute
#
# Kill the workers with Ctrl-C (the trap below forwards SIGINT to children).
set -euo pipefail

cleanup() {
    echo
    echo "[workers] stopping..."
    kill "${DF_PID:-}" "${GPU_PID:-}" 2>/dev/null || true
    wait "${DF_PID:-}" "${GPU_PID:-}" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

echo "[workers] starting df-cluster on port 9000..."
uv run python -m strata.notebook.remote_executor --port 9000 --log-level warning &
DF_PID=$!

echo "[workers] starting gpu-fly on port 9001..."
uv run python -m strata.notebook.remote_executor --port 9001 --log-level warning &
GPU_PID=$!

echo "[workers] waiting for health..."
for port in 9000 9001; do
    for _ in $(seq 1 30); do
        if curl -sf "http://127.0.0.1:$port/health" >/dev/null; then
            echo "[workers] port $port healthy"
            break
        fi
        sleep 0.2
    done
done

echo "[workers] ready. Open the arxiv_classifier notebook in Strata and run cells."
echo "[workers] df-cluster pid=$DF_PID, gpu-fly pid=$GPU_PID"
wait
