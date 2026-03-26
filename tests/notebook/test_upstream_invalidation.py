"""Test that editing an upstream cell invalidates downstream cache.

Reproduces the scenario:
1. Cell 1: x = 1
2. Cell 2: y = x + 1
3. Cell 3: print(y)
4. Run all three cells
5. Edit cell 1 to: x = 2
6. Run cell 2 → should NOT be a cache hit (upstream changed)
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from strata.notebook.executor import CellExecutor
from strata.notebook.parser import parse_notebook
from strata.notebook.session import NotebookSession
from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell
from tests.notebook.e2e_fixtures import (
    NotebookBuilder,
    create_test_app,
    execute_cell_and_wait,
    open_notebook_session,
    ws_connect,
)


@pytest.fixture
def pipeline_notebook(tmp_path):
    """Create a 3-cell pipeline: x=1 → y=x+1 → print(y)."""
    notebook_dir = create_notebook(tmp_path, "pipeline")

    add_cell_to_notebook(notebook_dir, "c1", None)
    write_cell(notebook_dir, "c1", "x = 1")

    add_cell_to_notebook(notebook_dir, "c2", "c1")
    write_cell(notebook_dir, "c2", "y = x + 1")

    add_cell_to_notebook(notebook_dir, "c3", "c2")
    write_cell(notebook_dir, "c3", "print(y)")

    notebook_state = parse_notebook(notebook_dir)
    session = NotebookSession(notebook_state, notebook_dir)
    return session


class TestUpstreamInvalidation:
    """Editing an upstream cell must invalidate downstream caches."""

    @pytest.mark.asyncio
    async def test_edit_upstream_invalidates_downstream(self, pipeline_notebook):
        """After editing c1 from x=1 to x=2, c2 must not cache hit."""
        session = pipeline_notebook

        # Verify DAG is correct
        assert session.dag is not None
        assert "c1" in session.dag.variable_producer.get("x", "")
        assert "c2" in session.dag.variable_producer.get("y", "")

        # Step 1: Execute all three cells in order
        executor = CellExecutor(session)
        r1 = await executor.execute_cell("c1", "x = 1")
        assert r1.success, f"c1 failed: {r1.error}"
        assert r1.cache_hit is False  # First run, no cache

        r2 = await executor.execute_cell("c2", "y = x + 1")
        assert r2.success, f"c2 failed: {r2.error}"
        assert r2.cache_hit is False  # First run, no cache

        r3 = await executor.execute_cell("c3", "print(y)")
        assert r3.success, f"c3 failed: {r3.error}"

        # Step 2: Verify cache works (re-run c2 without changes → cache hit)
        executor2 = CellExecutor(session)
        r2_cached = await executor2.execute_cell("c2", "y = x + 1")
        assert r2_cached.success
        assert r2_cached.cache_hit is True, "Expected cache hit when nothing changed"

        # Step 3: Edit c1 source from x=1 to x=2
        cell1 = next(c for c in session.notebook_state.cells if c.id == "c1")
        cell1.source = "x = 2"
        write_cell(session.path, "c1", "x = 2")

        # Re-analyze DAG (simulates what cell_source_update does)
        session.re_analyze_cell("c1")
        session.compute_staleness()

        # Step 4: Re-run c2 — must NOT be a cache hit
        executor3 = CellExecutor(session)
        r2_after_edit = await executor3.execute_cell("c2", "y = x + 1")
        assert r2_after_edit.success, f"c2 failed after edit: {r2_after_edit.error}"
        assert r2_after_edit.cache_hit is False, (
            "c2 should NOT cache hit after upstream c1 was edited. "
            f"c1.artifact_uri={cell1.artifact_uri}"
        )

    @pytest.mark.asyncio
    async def test_edit_upstream_produces_correct_value(self, pipeline_notebook):
        """After editing c1, c2's output should reflect the new value."""
        session = pipeline_notebook

        # Run pipeline
        executor = CellExecutor(session)
        await executor.execute_cell("c1", "x = 1")
        r2 = await executor.execute_cell("c2", "y = x + 1")
        assert r2.success
        # y should be 2 (x=1, y=x+1=2)
        assert r2.outputs.get("y", {}).get("preview") == 2

        # Edit c1 to x = 100
        cell1 = next(c for c in session.notebook_state.cells if c.id == "c1")
        cell1.source = "x = 100"
        write_cell(session.path, "c1", "x = 100")
        session.re_analyze_cell("c1")
        session.compute_staleness()

        # Re-run c2 — y should now be 101
        executor2 = CellExecutor(session)
        r2_new = await executor2.execute_cell("c2", "y = x + 1")
        assert r2_new.success, f"c2 failed: {r2_new.error}"
        assert r2_new.cache_hit is False
        assert r2_new.outputs.get("y", {}).get("preview") == 101, (
            f"Expected y=101 but got {r2_new.outputs.get('y', {}).get('preview')}"
        )


class TestUpstreamInvalidationE2E:
    """Same tests but through REST+WS path, matching the UI flow."""

    @pytest.fixture
    def setup(self):
        app = create_test_app()
        client = TestClient(app)
        with tempfile.TemporaryDirectory() as tmpdir:
            yield client, Path(tmpdir)

    def test_edit_c1_via_rest_then_run_c2_via_ws(self, setup):
        """Reproduce: edit cell 1 via REST PUT, run cell 2 via WS.

        This is exactly what the UI does:
        - CodeMirror onChange → PUT /cells/{id} (REST)
        - Shift+Enter → cell_execute (WS)
        """
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = 1")
            .add_cell("c2", "y = x + 1", after="c1")
            .add_cell("c3", "print(y)", after="c2")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Step 1: Run all 3 cells
                r1 = execute_cell_and_wait(ws, "c1")
                assert r1["payload"].get("cache_hit") is not True

                r2 = execute_cell_and_wait(ws, "c2")
                assert r2["payload"].get("cache_hit") is not True

                r3 = execute_cell_and_wait(ws, "c3")

                # Step 2: Edit c1 via REST (like the UI does)
                resp = client.put(
                    f"/v1/notebooks/{sid}/cells/c1",
                    json={"source": "x = 2"},
                )
                assert resp.status_code == 200
                # The REST response includes updated statuses
                data = resp.json()
                # Cell 1 should now be idle/stale
                c1_status = None
                for c in data.get("cells", []):
                    if c["id"] == "c1":
                        c1_status = c["status"]
                assert c1_status == "idle", (
                    f"After editing c1, expected status='idle' but got '{c1_status}'"
                )

                ws.clear()

                # Step 3: Run c2 via WS (like clicking Run in the UI)
                r2_after = execute_cell_and_wait(ws, "c2")
                assert r2_after["type"] == "cell_output"
                assert r2_after["payload"].get("cache_hit") is not True, (
                    "c2 should NOT cache hit after c1 was edited from x=1 to x=2"
                )

    def test_edit_c1_via_rest_then_run_c2_produces_correct_value(self, setup):
        """After editing c1 from x=1 to x=100 via REST, c2 should produce y=101."""
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = 1")
            .add_cell("c2", "y = x + 1", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Run both cells
                execute_cell_and_wait(ws, "c1")
                r2 = execute_cell_and_wait(ws, "c2")
                assert r2["payload"]["outputs"]["y"]["preview"] == 2

                # Edit c1 via REST
                resp = client.put(
                    f"/v1/notebooks/{sid}/cells/c1",
                    json={"source": "x = 100"},
                )
                assert resp.status_code == 200
                ws.clear()

                # Run c2 via WS — should produce y=101
                r2_new = execute_cell_and_wait(ws, "c2")
                assert r2_new["type"] == "cell_output"
                assert r2_new["payload"].get("cache_hit") is not True
                y_value = r2_new["payload"]["outputs"]["y"]["preview"]
                assert y_value == 101, f"Expected y=101 but got {y_value}"
