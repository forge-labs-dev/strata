# Environment Management

Each notebook has its own isolated Python environment managed by [uv](https://docs.astral.sh/uv/).

## How it works

When you create a notebook, Strata:

1. Generates a `pyproject.toml` with `pyarrow` as the default dependency
2. Runs `uv sync` to create a `.venv/` and `uv.lock`
3. All cell execution uses this notebook-local venv

The environment lockfile hash (`sha256(uv.lock)`) participates in provenance. Changing the environment invalidates all cached cell outputs.

## Python version

At notebook creation time, you can select a Python version from the versions configured on the server. The default is the server's own Python version.

!!! note
    The available versions depend on the server's `STRATA_NOTEBOOK_PYTHON_VERSIONS` configuration. On the hosted preview, both 3.12 and 3.13 are available.

## Installing packages

### From the UI

Open the **Environment** panel in the sidebar. Type a package name (e.g., `pandas`) and click **Add**. The operation runs asynchronously — you can continue editing cells while it installs.

### Import from requirements.txt

In the Environment panel, click **Import** and paste or upload a `requirements.txt` file. Strata previews the changes before applying them.

### Export

Click **Export** to download the current dependencies as `requirements.txt`.

## Environment operations

All environment mutations (add, remove, sync, import) run as **async jobs**. The UI shows:

- Current job status (running, success, failed)
- Recent operation history
- Resolved package count and lockfile hash

## Cache invalidation

When you install or remove a package:

1. `uv sync` runs to update `uv.lock`
2. The lockfile hash changes
3. All cells become **stale** (their provenance no longer matches)
4. Re-running any cell recomputes with the new environment

This ensures cached results are never served from a stale environment.

## Environment files

```
my_notebook/
├── pyproject.toml    # Package declarations
├── uv.lock           # Locked dependency graph
└── .venv/            # Virtual environment (not committed)
```

The `pyproject.toml` and `uv.lock` are the source of truth. The `.venv/` is recreated by `uv sync` when needed.
