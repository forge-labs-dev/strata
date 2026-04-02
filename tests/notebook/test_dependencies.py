"""Tests for dependency management: add, remove, list, REST + WS endpoints.

Validates:
- dependencies.py core operations (list, add, remove)
- REST endpoints (GET/POST/DELETE /v1/notebooks/{id}/dependencies)
- WebSocket messages (dependency_add, dependency_remove → dependency_changed)
- Lockfile hash change detection
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from strata.notebook.dependencies import (
    DependencyChangeResult,
    add_dependency,
    list_dependencies,
    remove_dependency,
)
from strata.notebook.executor import CellExecutor
from strata.notebook.models import CellStaleness, CellStatus
from strata.notebook.session import DependencyMutationOutcome
from strata.notebook.writer import create_notebook
from tests.notebook.e2e_fixtures import (
    NotebookBuilder,
    create_test_app,
    open_notebook_session,
    ws_connect,
)

pytestmark = pytest.mark.integration

# ============================================================================
# Core dependency operations
# ============================================================================


class TestListDependencies:
    """list_dependencies() parses pyproject.toml."""

    def test_empty_notebook(self, tmp_path: Path):
        """Newly created notebook includes pyarrow as the default dependency."""
        nb_dir = create_notebook(tmp_path, "empty")
        deps = list_dependencies(nb_dir)
        names = [d.name for d in deps]
        assert names == ["pyarrow"]

    def test_after_add(self, tmp_path: Path):
        """After adding a dep, it appears in the list."""
        nb_dir = create_notebook(tmp_path, "with_dep")
        result = add_dependency(nb_dir, "six")
        assert result.success
        deps = list_dependencies(nb_dir)
        names = [d.name for d in deps]
        assert "six" in names

    def test_with_version_specifier(self, tmp_path: Path):
        """Version specifiers are parsed correctly."""
        nb_dir = create_notebook(tmp_path, "versioned")
        add_dependency(nb_dir, "six>=1.0")
        deps = list_dependencies(nb_dir)
        six_dep = next((d for d in deps if d.name == "six"), None)
        assert six_dep is not None
        assert six_dep.specifier is not None
        assert ">=" in six_dep.specifier

    def test_no_pyproject(self, tmp_path: Path):
        """No pyproject.toml → empty list."""
        deps = list_dependencies(tmp_path)
        assert deps == []


class TestAddDependency:
    """add_dependency() calls uv add."""

    def test_add_package(self, tmp_path: Path):
        """Adding a real package succeeds."""
        nb_dir = create_notebook(tmp_path, "add_test")
        result = add_dependency(nb_dir, "six")
        assert result.success
        assert result.action == "add"
        assert result.package == "six"
        assert result.lockfile_changed is True

    def test_add_already_present(self, tmp_path: Path):
        """Adding an existing dependency is idempotent."""
        nb_dir = create_notebook(tmp_path, "double_add")
        add_dependency(nb_dir, "six")
        result = add_dependency(nb_dir, "six")
        # uv add is idempotent — should still succeed
        assert result.success

    def test_add_nonexistent_package(self, tmp_path: Path):
        """Adding a package that doesn't exist fails."""
        nb_dir = create_notebook(tmp_path, "bad_pkg")
        result = add_dependency(nb_dir, "this-package-definitely-does-not-exist-xyz123")
        assert result.success is False
        assert result.error is not None

    def test_add_when_uv_missing(self, tmp_path: Path):
        """Returns failure when uv is not available."""
        with patch(
            "strata.notebook.dependencies.subprocess.run",
            side_effect=FileNotFoundError,
        ):
            result = add_dependency(tmp_path, "requests")
            assert result.success is False
            assert result.error is not None
            assert "uv not found" in result.error


class TestRemoveDependency:
    """remove_dependency() calls uv remove."""

    def test_remove_package(self, tmp_path: Path):
        """Removing an added package succeeds."""
        nb_dir = create_notebook(tmp_path, "remove_test")
        add_dependency(nb_dir, "six")
        result = remove_dependency(nb_dir, "six")
        assert result.success
        assert result.action == "remove"
        assert result.lockfile_changed is True

        # Verify it's gone
        deps = list_dependencies(nb_dir)
        names = [d.name for d in deps]
        assert "six" not in names

    def test_remove_nonexistent(self, tmp_path: Path):
        """Removing a package that isn't present fails."""
        nb_dir = create_notebook(tmp_path, "remove_missing")
        result = remove_dependency(nb_dir, "this-package-not-installed")
        assert result.success is False
        assert result.error is not None


# ============================================================================
# REST API tests
# ============================================================================


class TestDependencyRESTEndpoints:
    """REST endpoints for dependency management."""

    @pytest.fixture
    def setup(self):
        app = create_test_app()
        client = TestClient(app)
        with tempfile.TemporaryDirectory() as tmpdir:
            yield client, Path(tmpdir)

    def test_list_dependencies_empty(self, setup):
        """GET /dependencies on a fresh notebook returns empty list."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.get(f"/v1/notebooks/{sid}/dependencies")
            assert resp.status_code == 200
            data = resp.json()
            assert "dependencies" in data
            assert isinstance(data["dependencies"], list)
            assert "environment" in data
            assert "sync_state" in data["environment"]

    def test_add_dependency_rest(self, setup):
        """POST /dependencies adds a package."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.post(
                f"/v1/notebooks/{sid}/dependencies",
                json={"package": "six"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["success"] is True
            assert data["package"] == "six"
            assert "environment" in data
            assert "declared_package_count" in data["environment"]
            assert "stale_cell_count" in data

            # Verify via list
            resp2 = client.get(f"/v1/notebooks/{sid}/dependencies")
            deps = resp2.json()["dependencies"]
            names = [d["name"] for d in deps]
            assert "six" in names

    def test_add_dependency_rest_returns_updated_cells(self, setup):
        """Dependency changes return refreshed cell statuses after env invalidation."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)
        nb.add_cell("c1", "x = 1")
        nb.add_cell("c2", "y = x + 1", after="c1")
        nb.add_cell("c3", "print(y)", after="c2")

        with open_notebook_session(client, nb.path) as (sid, session):
            async def _prime_cells():
                executor = CellExecutor(session)
                result1 = await executor.execute_cell("c1", "x = 1")
                result2 = await executor.execute_cell("c2", "y = x + 1")
                assert result1.success
                assert result2.success

            asyncio.run(_prime_cells())
            session.compute_staleness()
            statuses_before = {
                cell.id: cell.status for cell in session.notebook_state.cells
            }
            assert statuses_before["c1"] == CellStatus.READY
            assert statuses_before["c2"] == CellStatus.READY

            resp = client.post(
                f"/v1/notebooks/{sid}/dependencies",
                json={"package": "six"},
            )

            assert resp.status_code == 200
            data = resp.json()
            assert "cells" in data
            statuses = {cell["id"]: cell["status"] for cell in data["cells"]}
            assert statuses["c1"] == "idle"
            assert statuses["c2"] == "idle"

    def test_remove_dependency_rest(self, setup):
        """DELETE /dependencies/{package} removes a package."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            # Add first
            client.post(
                f"/v1/notebooks/{sid}/dependencies",
                json={"package": "six"},
            )

            # Remove
            resp = client.delete(f"/v1/notebooks/{sid}/dependencies/six")
            assert resp.status_code == 200
            data = resp.json()
            assert data["success"] is True
            assert "environment" in data
            assert "stale_cell_ids" in data

            # Verify removed
            resp2 = client.get(f"/v1/notebooks/{sid}/dependencies")
            deps = resp2.json()["dependencies"]
            names = [d["name"] for d in deps]
            assert "six" not in names

    def test_add_bad_package_rest(self, setup):
        """POST /dependencies with invalid package returns 400."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            resp = client.post(
                f"/v1/notebooks/{sid}/dependencies",
                json={"package": "this-pkg-does-not-exist-xyz123"},
            )
            assert resp.status_code == 400

    def test_list_dependencies_404(self, setup):
        """GET /dependencies for unknown notebook returns 404."""
        client, tmp = setup
        resp = client.get("/v1/notebooks/nonexistent/dependencies")
        assert resp.status_code == 404


# ============================================================================
# WebSocket tests
# ============================================================================


class TestDependencyWebSocket:
    """WebSocket messages for dependency management."""

    @pytest.fixture
    def setup(self):
        app = create_test_app()
        client = TestClient(app)
        with tempfile.TemporaryDirectory() as tmpdir:
            yield client, Path(tmpdir)

    def test_dependency_add_via_ws(self, setup):
        """dependency_add message → dependency_changed broadcast."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                ws.send("dependency_add", {"package": "six"})
                msg = ws.receive_until("dependency_changed")

                assert msg["payload"]["action"] == "add"
                assert msg["payload"]["package"] == "six"
                assert msg["payload"]["success"] is True
                assert msg["payload"]["lockfile_changed"] is True

    def test_dependency_remove_via_ws(self, setup):
        """dependency_remove message → dependency_changed broadcast."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Add first
                ws.send("dependency_add", {"package": "six"})
                ws.receive_until("dependency_changed")
                ws.clear()

                # Remove
                ws.send("dependency_remove", {"package": "six"})
                msg = ws.receive_until("dependency_changed")

                assert msg["payload"]["action"] == "remove"
                assert msg["payload"]["package"] == "six"
                assert msg["payload"]["success"] is True

    def test_dependency_add_missing_package(self, setup):
        """dependency_add without package field → error."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                ws.send("dependency_add", {})
                msg = ws.receive_until("error")
                assert "package" in msg["payload"]["error"].lower()

    def test_dependency_changed_includes_dep_list(self, setup):
        """dependency_changed includes updated dependency list."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                ws.send("dependency_add", {"package": "six"})
                msg = ws.receive_until("dependency_changed")

                deps = msg["payload"]["dependencies"]
                assert isinstance(deps, list)
                names = [d["name"] for d in deps]
                assert "six" in names
                assert "environment" in msg["payload"]
                assert "declared_package_count" in msg["payload"]["environment"]

    def test_dependency_add_via_ws_broadcasts_cell_status_updates(
        self, setup, monkeypatch
    ):
        """Lockfile-changing dependency updates broadcast refreshed cell status."""
        client, tmp = setup
        nb = NotebookBuilder(tmp)
        nb.add_cell("c1", "x = 1")
        nb.add_cell("c2", "y = x + 1", after="c1")

        with open_notebook_session(client, nb.path) as (sid, session):
            async def fake_mutate_dependency(self, package, *, action):
                assert action == "add"
                result = DependencyChangeResult(
                    success=True,
                    package=package,
                    action=action,
                    lockfile_changed=True,
                    dependencies=[],
                )
                staleness_map = {
                    "c1": CellStaleness(status=CellStatus.IDLE),
                    "c2": CellStaleness(status=CellStatus.IDLE),
                }
                return DependencyMutationOutcome(
                    result=result,
                    staleness_map=staleness_map,
                )

            monkeypatch.setattr(type(session), "mutate_dependency", fake_mutate_dependency)

            with ws_connect(client, sid) as ws:
                ws.send("dependency_add", {"package": "six"})
                changed = ws.receive_until("dependency_changed")
                assert changed["payload"]["lockfile_changed"] is True
                assert changed["payload"]["package"] == "six"
                assert "cells" in changed["payload"]
                assert changed["payload"]["stale_cell_count"] == 2
                assert "environment" in changed["payload"]

                status1 = ws.receive_until("cell_status", cell_id="c1")
                status2 = ws.receive_until("cell_status", cell_id="c2")
                assert status1["payload"]["status"] == "idle"
                assert status2["payload"]["status"] == "idle"
