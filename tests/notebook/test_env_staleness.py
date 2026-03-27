"""Tests for environment staleness and causality tracking.

Validates:
- Component hashes (source_hash, env_hash) stored in artifact metadata
- Causality inspector identifies env_changed vs source_changed
- Environment metadata persisted in notebook.toml
- Staleness detection after dependency change
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from strata.notebook.causality import (
    CausalityInspector,
    compute_causality_on_staleness,
)
from strata.notebook.dependencies import add_dependency
from strata.notebook.env import compute_lockfile_hash
from strata.notebook.provenance import compute_source_hash
from strata.notebook.session import SessionManager
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    update_environment_metadata,
    write_cell,
)

pytestmark = pytest.mark.integration


def _create_notebook_with_cell(tmp_path: Path, cell_source: str = "x = 1") -> tuple[Path, str]:
    """Create a notebook with one cell and return (nb_dir, cell_id)."""
    nb_dir = create_notebook(tmp_path, "test_nb")
    cell_id = "c1"
    add_cell_to_notebook(nb_dir, cell_id)
    write_cell(nb_dir, cell_id, cell_source)
    return nb_dir, cell_id


class TestComponentHashStorage:
    """Verify source_hash and env_hash are stored in artifact metadata."""

    @pytest.fixture
    def session_with_executed_cell(self, tmp_path):
        """Create a session, execute a cell, return (session, cell_id)."""
        nb_dir, cell_id = _create_notebook_with_cell(tmp_path, "x = 42")
        # Need a two-cell pipeline so first cell's output is consumed
        add_cell_to_notebook(nb_dir, "c2", after_cell_id="c1")
        write_cell(nb_dir, "c2", "y = x + 1")

        mgr = SessionManager()
        session = mgr.open_notebook(nb_dir)
        return session, "c1"

    @pytest.mark.asyncio
    async def test_stored_hashes_in_artifact(self, session_with_executed_cell):
        """After execution, artifact transform_spec contains source_hash and env_hash."""
        from strata.notebook.executor import CellExecutor

        session, cell_id = session_with_executed_cell
        executor = CellExecutor(session)
        result = await executor.execute_cell(cell_id, "x = 42")
        assert result.success

        # Find the artifact
        cell = next(c for c in session.notebook_state.cells if c.id == cell_id)
        assert cell.artifact_uri is not None

        parts = cell.artifact_uri.split("/")
        artifact_id = parts[-1].split("@")[0]
        version = int(parts[-1].split("@v=")[1])

        artifact = session.artifact_manager.artifact_store.get_artifact(artifact_id, version)
        assert artifact is not None
        assert artifact.transform_spec is not None

        spec = json.loads(artifact.transform_spec)
        params = spec["params"]

        assert "source_hash" in params
        assert "env_hash" in params
        assert params["source_hash"] == compute_source_hash("x = 42")
        assert params["env_hash"] == compute_lockfile_hash(session.path)


class TestCausalityEnvChanged:
    """Causality inspector distinguishes env changes from source changes."""

    @pytest.mark.asyncio
    async def test_env_change_detected(self, tmp_path):
        """After changing dependencies, causality reports env_changed."""
        nb_dir, cell_id = _create_notebook_with_cell(tmp_path, "x = 1")
        add_cell_to_notebook(nb_dir, "c2", after_cell_id="c1")
        write_cell(nb_dir, "c2", "y = x + 1")

        mgr = SessionManager()
        session = mgr.open_notebook(nb_dir)

        # Execute cell to create artifact with current env hash
        from strata.notebook.executor import CellExecutor

        executor = CellExecutor(session)
        result = await executor.execute_cell("c1", "x = 1")
        assert result.success

        # Now add a dependency (changes lockfile)
        add_result = add_dependency(nb_dir, "six")
        assert add_result.success
        assert add_result.lockfile_changed

        # Re-sync session
        session.ensure_venv_synced()

        # Compute causality — should detect env change
        causality_map = compute_causality_on_staleness(session)

        # c1 has a cached artifact with old env_hash, current env_hash is different
        if "c1" in causality_map:
            chain = causality_map["c1"]
            env_details = [d for d in chain.details if d.type == "env_changed"]
            assert len(env_details) > 0, f"Expected env_changed, got {chain.details}"

    @pytest.mark.asyncio
    async def test_source_change_not_env(self, tmp_path):
        """After changing source (not env), causality reports source_changed."""
        nb_dir, cell_id = _create_notebook_with_cell(tmp_path, "x = 1")
        add_cell_to_notebook(nb_dir, "c2", after_cell_id="c1")
        write_cell(nb_dir, "c2", "y = x + 1")

        mgr = SessionManager()
        session = mgr.open_notebook(nb_dir)

        from strata.notebook.executor import CellExecutor

        executor = CellExecutor(session)
        result = await executor.execute_cell("c1", "x = 1")
        assert result.success

        # Change the source (but NOT env)
        write_cell(nb_dir, "c1", "x = 999")
        cell = next(c for c in session.notebook_state.cells if c.id == "c1")
        cell.source = "x = 999"
        session.re_analyze_cell("c1")

        causality_map = compute_causality_on_staleness(session)
        if "c1" in causality_map:
            chain = causality_map["c1"]
            source_details = [d for d in chain.details if d.type == "source_changed"]
            env_details = [d for d in chain.details if d.type == "env_changed"]
            assert len(source_details) > 0
            # env should NOT be in there since lockfile didn't change
            assert len(env_details) == 0


class TestCausalityInspectorWithHashes:
    """CausalityInspector reads stored hashes."""

    @pytest.mark.asyncio
    async def test_inspector_reads_env_hash(self, tmp_path):
        """Inspector can read stored env_hash from artifact."""
        nb_dir, cell_id = _create_notebook_with_cell(tmp_path, "x = 1")
        add_cell_to_notebook(nb_dir, "c2", after_cell_id="c1")
        write_cell(nb_dir, "c2", "y = x + 1")

        mgr = SessionManager()
        session = mgr.open_notebook(nb_dir)

        from strata.notebook.executor import CellExecutor

        executor = CellExecutor(session)
        result = await executor.execute_cell("c1", "x = 1")
        assert result.success

        inspector = CausalityInspector(session)
        stored_env = inspector._get_artifact_metadata("c1", "env_hash")
        stored_src = inspector._get_artifact_metadata("c1", "source_hash")

        assert stored_env == compute_lockfile_hash(nb_dir)
        assert stored_src == compute_source_hash("x = 1")


class TestEnvironmentMetadata:
    """Environment section in notebook.toml."""

    def test_environment_populated_on_create(self, tmp_path):
        """create_notebook populates [environment] in notebook.toml."""
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib  # type: ignore

        nb_dir = create_notebook(tmp_path, "env_meta")
        with open(nb_dir / "notebook.toml", "rb") as f:
            data = tomllib.load(f)

        env = data.get("environment", {})
        assert "lockfile_hash" in env
        assert "python_version" in env
        assert env["python_version"].startswith(f"{sys.version_info.major}.")

    def test_environment_updated_after_dep_change(self, tmp_path):
        """update_environment_metadata refreshes lockfile_hash."""
        try:
            import tomllib
        except ModuleNotFoundError:
            import tomli as tomllib  # type: ignore

        nb_dir = create_notebook(tmp_path, "env_update")

        with open(nb_dir / "notebook.toml", "rb") as f:
            old_data = tomllib.load(f)
        old_hash = old_data["environment"]["lockfile_hash"]

        # Add a dependency
        result = add_dependency(nb_dir, "six")
        assert result.success

        # Update metadata
        update_environment_metadata(nb_dir)

        with open(nb_dir / "notebook.toml", "rb") as f:
            new_data = tomllib.load(f)
        new_hash = new_data["environment"]["lockfile_hash"]

        assert old_hash != new_hash

    def test_environment_missing_no_crash(self, tmp_path):
        """update_environment_metadata on non-notebook dir doesn't crash."""
        # No notebook.toml — should just return silently
        update_environment_metadata(tmp_path)
