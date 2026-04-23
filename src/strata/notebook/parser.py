"""Parse notebook directory and load notebook.toml + cell sources."""

from __future__ import annotations

import tomllib
from datetime import UTC, datetime
from pathlib import Path

import tomli_w

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

    # Move runtime fields (display outputs, cache block) out of
    # notebook.toml on first open of a legacy notebook. The helper is
    # additive and a no-op once the migration has happened.
    from strata.notebook.runtime_state import (
        load_runtime_state,
        migrate_from_legacy_notebook_toml,
    )
    from strata.notebook.writer import _env_has_meaningful_content

    has_legacy_cache = "cache" in toml_data
    has_legacy_environment = isinstance(toml_data.get("environment"), dict) and bool(
        toml_data.get("environment")
    )
    # Drop an ``[env]`` block that has no meaningful content (empty, or
    # only blanked sensitive-key placeholders). This cleans up pollution
    # from earlier runs where a user typed an API key in the Runtime
    # panel and the sensitive-key blanking left an empty slot in the
    # committed notebook.toml.
    legacy_env = toml_data.get("env")
    has_empty_env_block = isinstance(legacy_env, dict) and not _env_has_meaningful_content(
        legacy_env
    )
    needs_rewrite = migrate_from_legacy_notebook_toml(directory, toml_data) or has_legacy_cache
    if needs_rewrite or has_legacy_environment or has_empty_env_block:
        toml_data.pop("artifacts", None)
        toml_data.pop("cache", None)
        toml_data.pop("environment", None)
        if has_empty_env_block:
            toml_data.pop("env", None)
        _rewrite_notebook_toml(notebook_toml_path, toml_data)
    # Even when nothing was migrated (runtime.json already exists), drop
    # the legacy sections from the in-memory parse result so downstream
    # code does not see them — the authoritative values live in
    # runtime.json from here on.
    toml_data.pop("artifacts", None)
    toml_data.pop("cache", None)
    toml_data.pop("environment", None)

    runtime_state = load_runtime_state(directory)
    runtime_cells = runtime_state.get("cells", {})

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
        ai=toml_data.get("ai", {}),
        secret_manager=toml_data.get("secret_manager", {}),
        artifacts=toml_data.get("artifacts", {}),
        environment=toml_data.get("environment", {}),
        cache=toml_data.get("cache", {}),
    )

    # Load cell sources
    cells_dir = directory / "cells"
    cell_states: list[CellState] = []

    # Build notebook-level mount defaults (keyed by name for cell overrides)
    notebook_mounts = {m.name: m for m in notebook_toml.mounts}

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
        runtime_cell = runtime_cells.get(cell_meta.id, {})
        if isinstance(runtime_cell, dict):
            raw_displays = runtime_cell.get("display_outputs")
            if isinstance(raw_displays, list):
                for raw_display in raw_displays:
                    if not isinstance(raw_display, dict):
                        continue
                    try:
                        display_outputs.append(CellOutput(**raw_display))
                    except Exception:
                        continue
            raw_display = runtime_cell.get("display")
            if not display_outputs and isinstance(raw_display, dict):
                try:
                    display_outputs = [CellOutput(**raw_display)]
                except Exception:
                    display_outputs = []

        # Restore console output from .strata/console/
        from strata.notebook.writer import load_cell_console_output

        console_stdout, console_stderr = load_cell_console_output(directory, cell_meta.id)

        # Persisted execution provenance from ``.strata/runtime.json``.
        # compute_staleness() compares these against freshly-computed
        # hashes, so hydrating them at open lets a reopened notebook
        # correctly classify cells as READY / STALE without a
        # re-execution.
        def _str_or_none(value: object) -> str | None:
            return value if isinstance(value, str) and value else None

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
                console_stdout=console_stdout,
                console_stderr=console_stderr,
                last_provenance_hash=_str_or_none(runtime_cell.get("last_provenance_hash")),
                last_source_hash=_str_or_none(runtime_cell.get("last_source_hash")),
                last_env_hash=_str_or_none(runtime_cell.get("last_env_hash")),
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
        secret_manager_config=dict(notebook_toml.secret_manager),
        cells=cell_states,
        path=directory,
        created_at=notebook_toml.created_at,
        updated_at=notebook_toml.updated_at,
    )


def _rewrite_notebook_toml(path: Path, toml_data: dict) -> None:
    """Write a pre-parsed TOML dict back to disk (used by migration).

    Unlike ``write_notebook_toml`` this preserves whatever shape the
    caller has constructed, including fields not modelled by
    ``NotebookToml``. Used when the migration helper has already
    stripped legacy runtime sections from ``toml_data``.
    """
    with open(path, "wb") as f:
        tomli_w.dump(toml_data, f)
