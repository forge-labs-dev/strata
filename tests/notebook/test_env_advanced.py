"""Tests for Phase 4 features: ModuleNotFoundError detection, export, locking.

Validates:
- _detect_missing_module extracts package names
- Module-to-package mapping (e.g. PIL → Pillow)
- CellExecutionResult includes suggest_install field
- Export endpoint produces valid zip
- Concurrent dependency lock
"""

from __future__ import annotations

import io
import json
import tempfile
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from strata.notebook.dependencies import _get_notebook_lock
from strata.notebook.executor import _detect_missing_module
from tests.notebook.e2e_fixtures import (
    NotebookBuilder,
    create_test_app,
    open_notebook_session,
)


class TestDetectMissingModule:
    """_detect_missing_module extracts package names from errors."""

    def test_simple_module(self):
        """Standard module name extraction."""
        assert _detect_missing_module(
            "ModuleNotFoundError: No module named 'requests'", ""
        ) == "requests"

    def test_submodule(self):
        """Submodule import extracts top-level."""
        assert _detect_missing_module(
            "ModuleNotFoundError: No module named 'numpy.core'", ""
        ) == "numpy"

    def test_double_quotes(self):
        """Double-quoted module name."""
        assert _detect_missing_module(
            'ModuleNotFoundError: No module named "pandas"', ""
        ) == "pandas"

    def test_from_stderr(self):
        """Extraction from stderr."""
        assert _detect_missing_module(
            "", "ModuleNotFoundError: No module named 'flask'"
        ) == "flask"

    def test_pil_to_pillow(self):
        """PIL maps to Pillow."""
        assert _detect_missing_module(
            "ModuleNotFoundError: No module named 'PIL'", ""
        ) == "Pillow"

    def test_sklearn_to_scikit(self):
        """sklearn maps to scikit-learn."""
        assert _detect_missing_module(
            "ModuleNotFoundError: No module named 'sklearn'", ""
        ) == "scikit-learn"

    def test_cv2_to_opencv(self):
        """cv2 maps to opencv-python."""
        assert _detect_missing_module(
            "ModuleNotFoundError: No module named 'cv2'", ""
        ) == "opencv-python"

    def test_yaml_to_pyyaml(self):
        """yaml maps to pyyaml."""
        assert _detect_missing_module(
            "ModuleNotFoundError: No module named 'yaml'", ""
        ) == "pyyaml"

    def test_no_match(self):
        """Non-matching error returns None."""
        assert _detect_missing_module("TypeError: foo", "") is None

    def test_empty(self):
        """Empty strings return None."""
        assert _detect_missing_module("", "") is None


class TestExportEndpoint:
    """GET /{id}/export produces a valid zip."""

    @pytest.fixture
    def setup(self):
        app = create_test_app()
        client = TestClient(app)
        with tempfile.TemporaryDirectory() as tmpdir:
            yield client, Path(tmpdir)

    def test_export_contains_expected_files(self, setup):
        """Export zip contains notebook.toml, pyproject.toml, provenance.json."""
        client, tmp = setup
        nb = NotebookBuilder(tmp).add_cell("c1", "x = 1")

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.get(f"/v1/notebooks/{sid}/export")
            assert resp.status_code == 200
            assert resp.headers["content-type"] == "application/zip"

            buf = io.BytesIO(resp.content)
            with zipfile.ZipFile(buf) as zf:
                names = zf.namelist()
                assert "notebook.toml" in names
                assert "pyproject.toml" in names
                assert "provenance.json" in names

    def test_export_contains_cells(self, setup):
        """Export zip contains cell source files."""
        client, tmp = setup
        nb = NotebookBuilder(tmp).add_cell("c1", "x = 1").add_cell("c2", "y = 2")

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.get(f"/v1/notebooks/{sid}/export")
            buf = io.BytesIO(resp.content)
            with zipfile.ZipFile(buf) as zf:
                names = zf.namelist()
                assert "cells/c1.py" in names
                assert "cells/c2.py" in names

    def test_export_provenance_json_valid(self, setup):
        """provenance.json is valid JSON with expected structure."""
        client, tmp = setup
        nb = NotebookBuilder(tmp).add_cell("c1", "x = 1")

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.get(f"/v1/notebooks/{sid}/export")
            buf = io.BytesIO(resp.content)
            with zipfile.ZipFile(buf) as zf:
                prov = json.loads(zf.read("provenance.json"))
                assert "notebook_id" in prov
                assert "lockfile_hash" in prov
                assert "dag" in prov
                assert "cells" in prov
                assert "c1" in prov["cells"]
                assert "source_hash" in prov["cells"]["c1"]

    def test_export_includes_uv_lock(self, setup):
        """Export includes uv.lock when present."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.get(f"/v1/notebooks/{sid}/export")
            buf = io.BytesIO(resp.content)
            with zipfile.ZipFile(buf) as zf:
                names = zf.namelist()
                assert "uv.lock" in names

    def test_export_404_for_unknown(self, setup):
        """Export for unknown notebook returns 404."""
        client, tmp = setup
        resp = client.get("/v1/notebooks/nonexistent/export")
        assert resp.status_code == 404


class TestConcurrentLock:
    """Per-notebook locking for concurrent uv operations."""

    def test_same_dir_gets_same_lock(self, tmp_path):
        """Same notebook dir returns the same lock object."""
        lock1 = _get_notebook_lock(tmp_path)
        lock2 = _get_notebook_lock(tmp_path)
        assert lock1 is lock2

    def test_different_dirs_get_different_locks(self, tmp_path):
        """Different notebook dirs get different locks."""
        dir1 = tmp_path / "nb1"
        dir2 = tmp_path / "nb2"
        dir1.mkdir()
        dir2.mkdir()
        lock1 = _get_notebook_lock(dir1)
        lock2 = _get_notebook_lock(dir2)
        assert lock1 is not lock2
