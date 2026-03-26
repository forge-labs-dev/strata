"""Tests for environment lifecycle: venv creation, sync, and lockfile hashing.

Validates that:
- create_notebook() produces pyproject.toml and runs uv sync
- ensure_venv_synced() sets venv_python on the session
- Lockfile hash changes when dependencies change
- _uv_sync is best-effort (graceful failure)
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from strata.notebook.env import compute_lockfile_hash
from strata.notebook.session import NotebookSession
from strata.notebook.writer import _uv_sync, create_notebook


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

    def test_venv_python_fallback_when_uv_missing(self, tmp_path: Path):
        """When uv is missing, session.venv_python falls back to 'python'."""
        nb_dir = create_notebook(tmp_path, "no_uv_session")
        from strata.notebook.parser import parse_notebook

        state = parse_notebook(nb_dir)
        session = NotebookSession(state, nb_dir)

        with patch("strata.notebook.writer.subprocess.run", side_effect=FileNotFoundError):
            session.ensure_venv_synced()

        assert session.venv_python == Path("python")


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
