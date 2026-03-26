"""E2E tests: artifact caching and provenance deduplication.

Validates that re-executing an unchanged cell produces a cache hit,
and that changing source produces a cache miss.

Note: Cache hits only apply to cells whose outputs are consumed by
downstream cells (stored in the artifact store via consumed_variables).
Leaf cells without downstream consumers don't get their outputs stored.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.notebook.e2e_fixtures import (
    NotebookBuilder,
    create_test_app,
    execute_cell_and_wait,
    open_notebook_session,
    ws_connect,
)


@pytest.fixture
def setup():
    app = create_test_app()
    client = TestClient(app)
    with tempfile.TemporaryDirectory() as tmpdir:
        yield client, Path(tmpdir)


class TestCacheHit:
    """Re-executing an unchanged cell whose output is consumed should cache."""

    def test_upstream_cell_cache_hit(self, setup):
        """Execute c1→c2 pipeline twice — c1 should cache on second run."""
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = 42")
            .add_cell("c2", "y = x + 1", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # First execution — run c1 then c2
                r1 = execute_cell_and_wait(ws, "c1")
                assert r1["type"] == "cell_output"

                execute_cell_and_wait(ws, "c2")
                ws.clear()

                # Re-execute c1 — same source, same inputs
                r2 = execute_cell_and_wait(ws, "c1")
                assert r2["type"] == "cell_output"
                assert r2["payload"].get("cache_hit") is True

    def test_cache_hit_reports_execution_method(self, setup):
        """Cache hits should report execution_method='cached'."""
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = 1")
            .add_cell("c2", "y = x + 1", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                execute_cell_and_wait(ws, "c1")
                execute_cell_and_wait(ws, "c2")
                ws.clear()

                r2 = execute_cell_and_wait(ws, "c1")
                if r2["payload"].get("cache_hit"):
                    assert r2["payload"].get("execution_method") == "cached"


class TestCacheMiss:
    """Changing cell source should invalidate the cache."""

    def test_source_change_invalidates(self, setup):
        """Editing source → re-execute should be a cache miss."""
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = 1")
            .add_cell("c2", "y = x + 1", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Execute both
                execute_cell_and_wait(ws, "c1")
                execute_cell_and_wait(ws, "c2")

                # Update source via WebSocket
                ws.update_source("c1", "x = 2")
                ws.receive_until("dag_update")
                ws.clear()

                # Re-execute — should be cache miss
                r2 = execute_cell_and_wait(ws, "c1")
                assert r2["type"] == "cell_output"
                assert r2["payload"].get("cache_hit") is not True


class TestCascadeCache:
    """Cache behavior across multi-cell cascades."""

    def test_cascade_then_direct_rerun(self, setup):
        """Run full cascade, then re-run upstream directly — cache hit."""
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = 1")
            .add_cell("c2", "y = x + 1", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # First: cascade execution (c2 triggers c1)
                execute_cell_and_wait(ws, "c2")
                ws.clear()

                # Re-execute c1 directly — should be cache hit
                r = execute_cell_and_wait(ws, "c1")
                assert r["type"] == "cell_output"
                assert r["payload"].get("cache_hit") is True

    def test_leaf_cell_not_cached(self, setup):
        """A leaf cell (no downstream consumers) is not cached."""
        client, tmp = setup
        nb = NotebookBuilder(tmp).add_cell("c1", "x = 42")

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                r1 = execute_cell_and_wait(ws, "c1")
                assert r1["type"] == "cell_output"

                r2 = execute_cell_and_wait(ws, "c1")
                assert r2["type"] == "cell_output"
                # Leaf cells don't get cached because their outputs
                # aren't consumed by any downstream cell
                assert r2["payload"].get("cache_hit") is not True
