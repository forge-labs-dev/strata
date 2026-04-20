"""Tests for ``.strata/runtime.json`` — the per-notebook runtime state
that lives outside ``notebook.toml`` so example notebooks don't churn
under Git every time someone runs them.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from strata.notebook.runtime_state import (
    SCHEMA_VERSION,
    get_cell_entry,
    load_runtime_state,
    migrate_from_legacy_notebook_toml,
    runtime_state_path,
    save_runtime_state,
)


def test_load_returns_empty_shell_when_file_missing(tmp_path: Path):
    state = load_runtime_state(tmp_path)
    assert state == {"schema_version": SCHEMA_VERSION, "cells": {}, "environment": {}}


def test_save_and_load_roundtrip(tmp_path: Path):
    state = {
        "schema_version": SCHEMA_VERSION,
        "cells": {"c1": {"display_outputs": [{"content_type": "json/object"}]}},
        "environment": {},
    }
    save_runtime_state(tmp_path, state)

    path = runtime_state_path(tmp_path)
    assert path.exists()
    reloaded = load_runtime_state(tmp_path)
    assert reloaded == state


def test_save_strips_empty_cell_entries(tmp_path: Path):
    state = {
        "schema_version": SCHEMA_VERSION,
        "cells": {"c1": {"display_outputs": [{}]}, "empty": {}},
        "environment": {},
    }
    save_runtime_state(tmp_path, state)

    reloaded = load_runtime_state(tmp_path)
    assert "c1" in reloaded["cells"]
    assert "empty" not in reloaded["cells"]


def test_get_cell_entry_creates_on_demand(tmp_path: Path):
    state = load_runtime_state(tmp_path)
    entry = get_cell_entry(state, "c1")
    entry["display_outputs"] = []
    save_runtime_state(tmp_path, state)

    reloaded = load_runtime_state(tmp_path)
    assert reloaded["cells"]["c1"] == {"display_outputs": []}


def test_load_tolerates_corrupt_file(tmp_path: Path):
    path = runtime_state_path(tmp_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not valid json {")
    state = load_runtime_state(tmp_path)
    assert state == {"schema_version": SCHEMA_VERSION, "cells": {}, "environment": {}}


def test_migration_moves_artifacts_to_runtime_state(tmp_path: Path):
    toml_data = {
        "notebook_id": "nb",
        "cells": [],
        "artifacts": {
            "c1": {
                "display_outputs": [
                    {"content_type": "json/object", "artifact_uri": "strata://..."},
                ],
                "display": {"content_type": "json/object"},
            },
            "c2": {"display_outputs": []},
        },
    }

    migrated = migrate_from_legacy_notebook_toml(tmp_path, toml_data)
    assert migrated is True

    state = load_runtime_state(tmp_path)
    assert state["cells"]["c1"]["display_outputs"][0]["artifact_uri"] == "strata://..."
    assert state["cells"]["c1"]["display"] == {"content_type": "json/object"}
    assert "c2" not in state["cells"]  # empty entries pruned on save


def test_migration_is_noop_when_runtime_state_already_populated(tmp_path: Path):
    state = load_runtime_state(tmp_path)
    entry = get_cell_entry(state, "c1")
    entry["display_outputs"] = [{"content_type": "pickle/object"}]
    save_runtime_state(tmp_path, state)

    toml_data = {
        "artifacts": {
            "c1": {
                "display_outputs": [{"content_type": "json/object"}],
            }
        }
    }
    migrated = migrate_from_legacy_notebook_toml(tmp_path, toml_data)
    assert migrated is False
    reloaded = load_runtime_state(tmp_path)
    # Existing entry wins — migration must not overwrite fresh state.
    assert reloaded["cells"]["c1"]["display_outputs"][0]["content_type"] == "pickle/object"


def test_migration_without_legacy_artifacts_returns_false(tmp_path: Path):
    toml_data = {"notebook_id": "nb", "cells": []}
    migrated = migrate_from_legacy_notebook_toml(tmp_path, toml_data)
    assert migrated is False
    # And no runtime.json file was created since there was nothing to write.
    assert not runtime_state_path(tmp_path).exists()


@pytest.fixture
def notebook_with_legacy_toml(tmp_path: Path):
    import tomllib

    import tomli_w

    from strata.notebook.writer import (
        add_cell_to_notebook,
        create_notebook,
        write_cell,
    )

    notebook_dir = create_notebook(tmp_path, "LegacyMigrationTest", initialize_environment=False)
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = 1")

    notebook_toml_path = notebook_dir / "notebook.toml"
    with open(notebook_toml_path, "rb") as f:
        data = tomllib.load(f)
    data["artifacts"] = {
        "c1": {
            "display_outputs": [
                {
                    "content_type": "json/object",
                    "artifact_uri": "strata://artifact/legacy@v=1",
                }
            ],
        }
    }
    data["cache"] = {}
    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(data, f)

    return notebook_dir


def test_parse_notebook_migrates_and_rewrites_toml_on_first_open(
    notebook_with_legacy_toml: Path,
):
    """Opening a legacy notebook migrates the runtime fields out and
    the on-disk notebook.toml no longer carries the ``artifacts`` or
    ``cache`` sections."""
    import tomllib

    from strata.notebook.parser import parse_notebook

    state = parse_notebook(notebook_with_legacy_toml)
    cell = next(c for c in state.cells if c.id == "c1")
    assert cell.display_outputs
    assert cell.display_outputs[0].artifact_uri == "strata://artifact/legacy@v=1"

    runtime = load_runtime_state(notebook_with_legacy_toml)
    assert runtime["cells"]["c1"]["display_outputs"][0]["artifact_uri"] == (
        "strata://artifact/legacy@v=1"
    )

    with open(notebook_with_legacy_toml / "notebook.toml", "rb") as f:
        rewritten = tomllib.load(f)
    assert "artifacts" not in rewritten
    assert "cache" not in rewritten


def test_update_cell_display_outputs_writes_to_runtime_json(tmp_path: Path):
    import tomllib

    from strata.notebook.writer import (
        add_cell_to_notebook,
        create_notebook,
        update_cell_display_outputs,
        write_cell,
    )

    notebook_dir = create_notebook(tmp_path, "WriteTest", initialize_environment=False)
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = 1")

    update_cell_display_outputs(
        notebook_dir,
        "c1",
        [{"content_type": "json/object", "bytes": 10}],
    )

    runtime = load_runtime_state(notebook_dir)
    assert runtime["cells"]["c1"]["display_outputs"] == [
        {"content_type": "json/object", "bytes": 10}
    ]
    assert runtime["cells"]["c1"]["display"] == {"content_type": "json/object", "bytes": 10}

    # notebook.toml must NOT gain an artifacts section.
    with open(notebook_dir / "notebook.toml", "rb") as f:
        toml_data = tomllib.load(f)
    assert "artifacts" not in toml_data


def test_update_cell_display_outputs_clears_entry(tmp_path: Path):
    from strata.notebook.writer import (
        add_cell_to_notebook,
        create_notebook,
        update_cell_display_outputs,
        write_cell,
    )

    notebook_dir = create_notebook(tmp_path, "ClearTest", initialize_environment=False)
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = 1")

    update_cell_display_outputs(
        notebook_dir,
        "c1",
        [{"content_type": "json/object"}],
    )
    update_cell_display_outputs(notebook_dir, "c1", None)

    runtime = load_runtime_state(notebook_dir)
    assert "c1" not in runtime["cells"]
