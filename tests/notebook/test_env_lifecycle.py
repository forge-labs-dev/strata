"""Tests for environment lifecycle: venv creation, sync, and lockfile hashing.

Validates that:
- create_notebook() produces pyproject.toml and runs uv sync
- ensure_venv_synced() sets venv_python on the session
- Lockfile hash changes when dependencies change
- _uv_sync is best-effort (graceful failure)
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from strata.notebook.env import compute_lockfile_hash
from strata.notebook.session import NotebookSession
from strata.notebook.writer import _uv_sync, create_notebook, update_environment_metadata

pytestmark = pytest.mark.integration


class TestCreateNotebookVenv:
    """create_notebook() should scaffold pyproject.toml and run uv sync."""

    def test_pyproject_toml_created(self, tmp_path: Path):
        """Notebook creation writes pyproject.toml."""
        nb_dir = create_notebook(tmp_path, "my_nb")
        assert (nb_dir / "pyproject.toml").exists()
        content = (nb_dir / "pyproject.toml").read_text()
        assert 'name = "my_nb"' in content
        assert 'requires-python = ">=3.12"' in content

    def test_uv_lock_created(self, tmp_path: Path):
        """uv sync produces uv.lock."""
        nb_dir = create_notebook(tmp_path, "test_lock")
        # uv sync should have run and produced uv.lock
        assert (nb_dir / "uv.lock").exists()

    def test_venv_created(self, tmp_path: Path):
        """uv sync produces .venv/ directory."""
        nb_dir = create_notebook(tmp_path, "test_venv")
        assert (nb_dir / ".venv").is_dir()

    def test_uv_sync_failure_does_not_raise(self, tmp_path: Path):
        """If uv is not on PATH, create_notebook still succeeds."""
        with patch("strata.notebook.writer.subprocess.run", side_effect=FileNotFoundError):
            nb_dir = create_notebook(tmp_path, "no_uv")
            # Notebook dir is created, just no venv
            assert (nb_dir / "notebook.toml").exists()
            assert (nb_dir / "pyproject.toml").exists()


class TestUvSyncHelper:
    """_uv_sync() helper function."""

    def test_returns_true_on_success(self, tmp_path: Path):
        """Successful sync returns True."""
        nb_dir = create_notebook(tmp_path, "sync_ok")
        # Already synced during creation, but calling again is idempotent
        assert _uv_sync(nb_dir) is True

    def test_returns_false_when_uv_missing(self, tmp_path: Path):
        """Returns False when uv is not available."""
        with patch("strata.notebook.writer.subprocess.run", side_effect=FileNotFoundError):
            assert _uv_sync(tmp_path) is False

    def test_returns_false_on_timeout(self, tmp_path: Path):
        """Returns False on timeout."""
        with patch(
            "strata.notebook.writer.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="uv sync", timeout=60),
        ):
            assert _uv_sync(tmp_path) is False

    def test_returns_false_on_failure(self, tmp_path: Path):
        """Returns False when uv sync exits non-zero."""
        with patch(
            "strata.notebook.writer.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                returncode=1, cmd="uv sync", stderr=b"some error"
            ),
        ):
            assert _uv_sync(tmp_path) is False


class TestSessionVenvPython:
    """ensure_venv_synced() sets session.venv_python."""

    def test_venv_python_set_after_sync(self, tmp_path: Path):
        """After opening a notebook, session.venv_python points to .venv/bin/python."""
        nb_dir = create_notebook(tmp_path, "venv_session")
        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)
        session.ensure_venv_synced()

        assert session.venv_python is not None
        assert "python" in str(session.venv_python)
        # Should point to the notebook's venv
        assert session.venv_python.exists()
        assert ".venv" in str(session.venv_python)
        assert session.environment_sync_state == "ready"
        assert session.environment_sync_error is None
        assert session.environment_sync_notice is None
        assert session.environment_last_synced_at is not None
        assert session.environment_last_sync_duration_ms is not None
        assert session.environment_interpreter_source == "venv"

    def test_venv_python_fallback_when_uv_missing(self, tmp_path: Path):
        """When uv is missing but .venv exists, keep using the notebook venv."""
        nb_dir = create_notebook(tmp_path, "no_uv_session")
        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)

        with patch("strata.notebook.writer.subprocess.run", side_effect=FileNotFoundError):
            session.ensure_venv_synced()

        assert session.venv_python is not None
        assert session.venv_python.exists()
        assert ".venv" in str(session.venv_python)
        assert session.environment_sync_state == "ready"
        assert session.environment_sync_error is None
        assert session.environment_sync_notice is not None
        assert session.environment_interpreter_source == "venv"

    def test_path_fallback_when_uv_missing_and_no_venv(self, tmp_path: Path):
        """Without uv and without .venv, the session falls back to PATH python."""
        import shutil

        nb_dir = create_notebook(tmp_path, "no_uv_no_venv")
        shutil.rmtree(nb_dir / ".venv")

        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)

        with patch("strata.notebook.writer.subprocess.run", side_effect=FileNotFoundError):
            session.ensure_venv_synced()

        assert session.venv_python == Path("python")
        assert session.environment_sync_state == "failed"
        assert session.environment_sync_error is not None
        assert session.environment_sync_notice is None
        assert session.environment_interpreter_source == "path"

    def test_refresh_environment_runtime_reuses_existing_venv(self, tmp_path: Path):
        """Dependency changes should refresh runtime state without a second uv sync."""
        nb_dir = create_notebook(tmp_path, "refresh_runtime")
        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)

        with patch("strata.notebook.session._uv_sync") as mock_uv_sync:
            session.refresh_environment_runtime()

        mock_uv_sync.assert_not_called()
        assert session.venv_python is not None
        assert session.venv_python.exists()
        assert ".venv" in str(session.venv_python)
        assert session.environment_sync_state == "ready"
        assert session.environment_sync_error is None
        assert session.environment_sync_notice is None
        assert session.environment_interpreter_source == "venv"

    def test_refresh_environment_runtime_falls_back_when_venv_missing(
        self, tmp_path: Path
    ):
        """If the notebook venv is missing, refresh should fall back to uv sync."""
        import shutil

        nb_dir = create_notebook(tmp_path, "refresh_runtime_fallback")
        shutil.rmtree(nb_dir / ".venv")

        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)

        with patch.object(session, "ensure_venv_synced") as mock_sync:
            session.refresh_environment_runtime()

        mock_sync.assert_called_once()


class TestDependencyChangeRefresh:
    """Dependency mutations should reuse the synced environment when possible."""

    @pytest.mark.asyncio
    async def test_on_dependencies_changed_uses_runtime_refresh(self, tmp_path: Path):
        """Post-mutation refresh should not trigger a second uv sync."""
        nb_dir = create_notebook(tmp_path, "dependency_refresh")
        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)

        with patch.object(session, "refresh_environment_runtime") as mock_refresh:
            with patch.object(session, "ensure_venv_synced") as mock_sync:
                with patch.object(
                    session,
                    "_invalidate_warm_pool_for_environment_change",
                ) as mock_invalidate:
                    with patch(
                        "strata.notebook.session.update_environment_metadata"
                    ) as mock_update_metadata:
                        await session.on_dependencies_changed()

        mock_refresh.assert_called_once()
        mock_sync.assert_not_called()
        mock_invalidate.assert_awaited_once()
        mock_update_metadata.assert_called_once_with(nb_dir)


class TestEnvironmentMetadata:
    """Environment metadata persisted to notebook.toml."""

    def test_update_environment_metadata_records_runtime_fields(self, tmp_path: Path):
        """Environment metadata should include richer sidebar status fields."""
        import tomllib

        nb_dir = create_notebook(tmp_path, "env_metadata")
        update_environment_metadata(nb_dir)

        with open(nb_dir / "notebook.toml", "rb") as f:
            data = tomllib.load(f)

        environment = data["environment"]
        assert "lockfile_hash" in environment
        assert "python_version" in environment
        assert "declared_package_count" in environment
        assert "resolved_package_count" in environment
        assert "has_lockfile" in environment
        assert "last_synced_at" in environment

    def test_serialize_environment_state_includes_runtime_details(self, tmp_path: Path):
        """Live environment state should expose runtime source and sync metadata."""
        nb_dir = create_notebook(tmp_path, "env_state")
        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)
        session.ensure_venv_synced()

        environment = session.serialize_environment_state()
        assert environment["interpreter_source"] == "venv"
        assert "sync_notice" in environment
        assert "last_sync_duration_ms" in environment


class TestLockfileHash:
    """compute_lockfile_hash() produces consistent hashes."""

    def test_hash_with_lockfile(self, tmp_path: Path):
        """Hash is deterministic for a given lockfile."""
        nb_dir = create_notebook(tmp_path, "hash_test")
        h1 = compute_lockfile_hash(nb_dir)
        h2 = compute_lockfile_hash(nb_dir)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex digest

    def test_sentinel_hash_without_lockfile(self, tmp_path: Path):
        """Without uv.lock, returns hash of empty string."""
        import hashlib

        expected = hashlib.sha256(b"").hexdigest()
        assert compute_lockfile_hash(tmp_path) == expected

    def test_hash_changes_on_lockfile_modification(self, tmp_path: Path):
        """Modifying uv.lock changes the hash."""
        nb_dir = create_notebook(tmp_path, "hash_change")
        h1 = compute_lockfile_hash(nb_dir)

        # Modify the lockfile
        lockfile = nb_dir / "uv.lock"
        lockfile.write_text(lockfile.read_text() + "\n# extra\n")

        h2 = compute_lockfile_hash(nb_dir)
        assert h1 != h2
