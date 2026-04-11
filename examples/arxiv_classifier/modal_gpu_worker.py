"""Modal deployment for the gpu-fly Strata notebook worker.

Wraps :func:`strata.notebook.remote_executor.create_notebook_executor_app`
as a Modal ASGI endpoint running on an A10G GPU. The deployed URL becomes
the ``gpu-fly`` worker registered in ``notebook.toml`` for the
arxiv_classifier demo.

Deploy:
    modal deploy examples/arxiv_classifier/modal_gpu_worker.py

After deploy, Modal prints the endpoint URL. Update notebook.toml's
``[[workers]]`` section for ``gpu-fly`` so its ``config.url`` points at
``<modal-url>/v1/execute``.

Run interactively (faster iteration during development, no persistent
deployment):
    modal serve examples/arxiv_classifier/modal_gpu_worker.py

Cost notes:
    A10G is ~$1.10/hr while running. With ``scaledown_window=60`` the
    container sleeps 60s after the last request, so a 5-minute demo
    incurs ~$0.10. The first request after sleep pays a cold-start
    penalty of ~10-20s while Modal pulls the image and boots the GPU.
    Pre-warm before YC review sessions by sending a `/health` request.
"""

from __future__ import annotations

import modal

# All ML and Strata dependencies baked into the image. Order matters: the
# Rust toolchain must be installed before pip can build Strata's native
# extension via maturin. Modal caches the image so this only runs once
# per dependency change.
gpu_image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("curl", "build-essential", "git", "ca-certificates")
    .run_commands(
        "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal",
    )
    .env({"PATH": "/root/.cargo/bin:/usr/local/bin:/usr/bin:/bin"})
    .pip_install(
        "maturin>=1.5",
        # FastAPI / executor protocol
        "fastapi>=0.115",
        "uvicorn>=0.30",
        "httpx>=0.27",
        "python-multipart>=0.0.9",
        # Data layer
        "pyarrow>=18.0.0",
        "pandas>=2.0.0",
        "numpy>=1.26.0",
        # ML stack — torch first so subsequent pkgs see the right wheel
        "torch>=2.3",
        "sentence-transformers>=3.0",
        "scikit-learn>=1.5",
        # Strata itself, built from main. Update the ref when promoting
        # the demo to a stable tag.
        "strata @ git+https://github.com/forge-labs-dev/strata.git@main",
    )
)

app = modal.App("strata-gpu-worker", image=gpu_image)


@app.function(
    gpu="A10G",
    # Sleep after 60s of idle to keep costs near zero between demo sessions.
    # Increase this (e.g. 600) before a YC review to avoid cold starts.
    scaledown_window=60,
    # One concurrent execution per container — cell harnesses are
    # CPU/GPU-exclusive and should not be pipelined inside one VM.
    max_containers=1,
    timeout=600,
)
@modal.asgi_app()
def gpu_executor():
    """Expose the Strata notebook HTTP executor as a Modal ASGI app."""
    from strata.notebook.remote_executor import create_notebook_executor_app

    return create_notebook_executor_app()
