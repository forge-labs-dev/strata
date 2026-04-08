"""Parse notebook directory and load notebook.toml + cell sources."""

from __future__ import annotations

import tomllib
from datetime import UTC, datetime
from pathlib import Path

from strata.notebook.models import (
    CellMeta,
    CellOutput,
    CellState,
    MountSpec,
    NotebookState,
    NotebookToml,
    WorkerSpec,
)


def parse_notebook(directory: Path) -> NotebookState:
    """Parse notebook directory, load notebook.toml and cell files.

    Args:
        directory: Path to notebook directory

    Returns:
        NotebookState with all cells loaded

    Raises:
        FileNotFoundError: If notebook.toml is missing
    """
    directory = Path(directory)
    notebook_toml_path = directory / "notebook.toml"

    if not notebook_toml_path.exists():
        raise FileNotFoundError(f"notebook.toml not found at {notebook_toml_path}")

    # Read notebook.toml
    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    # Parse into NotebookToml
    # Get created_at and updated_at, defaulting to now if not present
    created_at = toml_data.get("created_at")
    if created_at is None:
        created_at = datetime.now(tz=UTC)

    updated_at = toml_data.get("updated_at")
    if updated_at is None:
        updated_at = datetime.now(tz=UTC)

    notebook_toml = NotebookToml(
        notebook_id=toml_data.get("notebook_id", ""),
        name=toml_data.get("name", "Untitled Notebook"),
        created_at=created_at,
        updated_at=updated_at,
        worker=toml_data.get("worker"),
        timeout=toml_data.get("timeout"),
        env=toml_data.get("env", {}),
        workers=[WorkerSpec(**worker) for worker in toml_data.get("workers", [])],
        cells=[CellMeta(**cell_meta) for cell_meta in toml_data.get("cells", [])],
        mounts=[MountSpec(**m) for m in toml_data.get("mounts", [])],
        artifacts=toml_data.get("artifacts", {}),
        environment=toml_data.get("environment", {}),
        cache=toml_data.get("cache", {}),
    )

    # Load cell sources
    cells_dir = directory / "cells"
    cell_states: list[CellState] = []

    # Build notebook-level mount defaults (keyed by name for cell overrides)
    notebook_mounts = {m.name: m for m in notebook_toml.mounts}
    artifact_entries = notebook_toml.artifacts if isinstance(notebook_toml.artifacts, dict) else {}

    for cell_meta in notebook_toml.cells:
        cell_file = cells_dir / cell_meta.file
        source = ""

        if cell_file.exists():
            with open(cell_file, encoding="utf-8") as f:
                source = f.read()

        # Resolve mounts: notebook-level defaults, overridden by cell-level
        resolved_mounts = dict(notebook_mounts)
        for m in cell_meta.mounts:
            resolved_mounts[m.name] = m
        resolved_worker = cell_meta.worker or notebook_toml.worker
        resolved_timeout = (
            cell_meta.timeout if cell_meta.timeout is not None else notebook_toml.timeout
        )
        resolved_env = dict(notebook_toml.env)
        resolved_env.update(cell_meta.env)
        display_outputs: list[CellOutput] = []
        raw_artifacts = artifact_entries.get(cell_meta.id, {})
        if isinstance(raw_artifacts, dict):
            raw_displays = raw_artifacts.get("display_outputs")
            if isinstance(raw_displays, list):
                for raw_display in raw_displays:
                    if not isinstance(raw_display, dict):
                        continue
                    try:
                        display_outputs.append(CellOutput(**raw_display))
                    except Exception:
                        continue
            raw_display = raw_artifacts.get("display")
            if not display_outputs and isinstance(raw_display, dict):
                try:
                    display_outputs = [CellOutput(**raw_display)]
                except Exception:
                    display_outputs = []

        cell_states.append(
            CellState(
                id=cell_meta.id,
                source=source,
                language=cell_meta.language,
                order=cell_meta.order,
                worker=resolved_worker,
                worker_override=cell_meta.worker,
                timeout=resolved_timeout,
                timeout_override=cell_meta.timeout,
                env=resolved_env,
                env_overrides=dict(cell_meta.env),
                mounts=list(resolved_mounts.values()),
                mount_overrides=list(cell_meta.mounts),
                display_outputs=display_outputs,
                display_output=display_outputs[-1] if display_outputs else None,
            )
        )

    # Sort by order
    cell_states.sort(key=lambda c: c.order)

    return NotebookState(
        id=notebook_toml.notebook_id,
        name=notebook_toml.name,
        worker=notebook_toml.worker,
        timeout=notebook_toml.timeout,
        env=dict(notebook_toml.env),
        workers=list(notebook_toml.workers),
        mounts=list(notebook_toml.mounts),
        cells=cell_states,
        path=directory,
        created_at=notebook_toml.created_at,
        updated_at=notebook_toml.updated_at,
    )
