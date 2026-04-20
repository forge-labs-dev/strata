"""Persistent per-notebook runtime state.

``notebook.toml`` holds stable notebook configuration — cell list,
worker config, notebook-level env, mounts. Anything that changes on
every execution or background sync (display outputs, per-cell
provenance hashes, the last ``uv sync`` timestamp) lives here
instead. Storing it separately keeps ``notebook.toml`` diff-friendly
for version control and means example notebooks don't churn under
Git every time someone runs them.

The file is ``.strata/runtime.json`` and is gitignored alongside the
rest of ``.strata/``.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
_RUNTIME_FILENAME = "runtime.json"


def runtime_state_path(notebook_dir: Path) -> Path:
    return Path(notebook_dir) / ".strata" / _RUNTIME_FILENAME


def load_runtime_state(notebook_dir: Path) -> dict[str, Any]:
    """Return the runtime-state document, or a fresh empty shell."""
    path = runtime_state_path(notebook_dir)
    if not path.exists():
        return _empty_state()
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (ValueError, OSError):
        return _empty_state()
    if not isinstance(data, dict):
        return _empty_state()
    data.setdefault("schema_version", SCHEMA_VERSION)
    data.setdefault("cells", {})
    data.setdefault("environment", {})
    if not isinstance(data["cells"], dict):
        data["cells"] = {}
    if not isinstance(data["environment"], dict):
        data["environment"] = {}
    return data


def save_runtime_state(notebook_dir: Path, state: dict[str, Any]) -> None:
    """Atomically persist the runtime-state document."""
    path = runtime_state_path(notebook_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Strip any completely empty cell records so the file stays tidy.
    cells = state.get("cells", {})
    if isinstance(cells, dict):
        state["cells"] = {cid: entry for cid, entry in cells.items() if entry}
    state["schema_version"] = SCHEMA_VERSION

    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=path.parent,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp_name, path)


def _empty_state() -> dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "cells": {}, "environment": {}}


def get_cell_entry(state: dict[str, Any], cell_id: str) -> dict[str, Any]:
    """Return the mutable per-cell entry, creating it on demand."""
    cells = state.setdefault("cells", {})
    entry = cells.get(cell_id)
    if not isinstance(entry, dict):
        entry = {}
        cells[cell_id] = entry
    return entry


def prune_cell_entry(state: dict[str, Any], cell_id: str) -> None:
    """Remove the per-cell entry entirely — callers do this when the
    cell is deleted so runtime state follows the structural config."""
    cells = state.get("cells")
    if isinstance(cells, dict):
        cells.pop(cell_id, None)


def migrate_from_legacy_notebook_toml(
    notebook_dir: Path,
    toml_data: dict[str, Any],
) -> bool:
    """One-time migration of runtime fields out of notebook.toml.

    Returns ``True`` when at least one field was migrated so callers
    know to rewrite notebook.toml without the legacy sections.

    Scope for this migration step:

    * ``artifacts.<cell_id>.display_outputs`` / ``display`` →
      ``runtime.json`` ``cells.<cell_id>.display_outputs`` / ``display``.
    * The ``[cache]`` section is dropped because it's never been used.

    Migrations for environment metadata and per-cell provenance hashes
    land in later commits; this helper is additive and re-entrant, so
    running it twice is harmless.
    """
    state = load_runtime_state(notebook_dir)
    migrated = False

    legacy_artifacts = toml_data.get("artifacts")
    if isinstance(legacy_artifacts, dict):
        for cell_id, cell_artifacts in legacy_artifacts.items():
            if not isinstance(cell_artifacts, dict):
                continue
            entry = get_cell_entry(state, cell_id)
            raw_outputs = cell_artifacts.get("display_outputs")
            if isinstance(raw_outputs, list) and "display_outputs" not in entry:
                cleaned = [dict(output) for output in raw_outputs if isinstance(output, dict)]
                if cleaned:
                    entry["display_outputs"] = cleaned
                    migrated = True
            raw_display = cell_artifacts.get("display")
            if isinstance(raw_display, dict) and raw_display and "display" not in entry:
                entry["display"] = dict(raw_display)
                migrated = True

    if migrated:
        save_runtime_state(notebook_dir, state)

    return migrated
