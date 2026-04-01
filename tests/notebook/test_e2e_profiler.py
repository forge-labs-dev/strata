"""E2E tests: notebook profiling summary over the live WebSocket path."""

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
    """Create app, client, and temp directory."""
    app = create_test_app()
    client = TestClient(app)
    with tempfile.TemporaryDirectory() as tmpdir:
        yield client, Path(tmpdir)


def _request_profiling_summary(ws) -> dict:
    ws.send("profiling_request", {})
    return ws.receive_until("profiling_summary")["payload"]


class TestNotebookProfiling:
    """Profiling should reflect repeated live execution history."""

    def test_profiling_summary_reports_repeated_cache_hits(self, setup):
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

                first_cached = execute_cell_and_wait(ws, "c1")
                assert first_cached["type"] == "cell_output"
                assert first_cached["payload"].get("cache_hit") is True

                second_cached = execute_cell_and_wait(ws, "c1")
                assert second_cached["type"] == "cell_output"
                assert second_cached["payload"].get("cache_hit") is True

                payload = _request_profiling_summary(ws)

                assert payload["cache_hits"] == 2
                assert payload["cache_misses"] == 2
                c1_profile = next(cp for cp in payload["cell_profiles"] if cp["cell_id"] == "c1")
                assert c1_profile["cache_hit"] is True
                assert c1_profile["execution_count"] == 3

    def test_profiling_summary_accumulates_cache_savings(self, setup):
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell("c1", "x = sum(range(2000))")
            .add_cell("c2", "y = x + 1", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                execute_cell_and_wait(ws, "c1")
                execute_cell_and_wait(ws, "c2")
                ws.clear()

                execute_cell_and_wait(ws, "c1")
                execute_cell_and_wait(ws, "c1")

                payload = _request_profiling_summary(ws)

                assert payload["cache_hits"] == 2
                assert payload["cache_savings_ms"] >= 0
                c1_profile = next(cp for cp in payload["cell_profiles"] if cp["cell_id"] == "c1")
                assert c1_profile["execution_count"] == 3
                assert c1_profile["cache_hit"] is True

    def test_profiling_summary_counts_new_cache_miss_after_source_edit(self, setup):
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

                cached = execute_cell_and_wait(ws, "c1")
                assert cached["payload"].get("cache_hit") is True

                ws.update_source("c1", "x = 2")
                ws.receive_until("dag_update")
                ws.clear()

                rerun = execute_cell_and_wait(ws, "c1")
                assert rerun["type"] == "cell_output"
                assert rerun["payload"].get("cache_hit") is not True

                payload = _request_profiling_summary(ws)

                assert payload["cache_hits"] == 1
                assert payload["cache_misses"] == 3
                c1_profile = next(cp for cp in payload["cell_profiles"] if cp["cell_id"] == "c1")
                assert c1_profile["execution_count"] == 3
                assert c1_profile["cache_hit"] is False
