"""E2E tests: cross-cell source-backed module exports."""

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


def _put_notebook_workers(client: TestClient, session_id: str, workers: list[dict]) -> dict:
    response = client.put(
        f"/v1/notebooks/{session_id}/workers",
        json={"workers": workers},
    )
    assert response.status_code == 200
    return response.json()


def _put_notebook_worker(client: TestClient, session_id: str, worker: str | None) -> dict:
    response = client.put(
        f"/v1/notebooks/{session_id}/worker",
        json={"worker": worker},
    )
    assert response.status_code == 200
    return response.json()


class TestLocalModuleExports:
    """Local execution should support source-backed exports across cells."""

    def test_cross_cell_function_export(self, setup):
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell(
                "c1",
                "import math\n\ndef area(r):\n    return math.pi * r * r",
            )
            .add_cell("c2", "result = round(area(2), 5)", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                result1 = execute_cell_and_wait(ws, "c1")
                assert result1["type"] == "cell_output"
                assert result1["payload"]["outputs"]["area"]["content_type"] == "module/cell"

                result2 = execute_cell_and_wait(ws, "c2")
                assert result2["type"] == "cell_output"
                assert result2["payload"]["outputs"]["result"]["preview"] == 12.56637

    def test_cross_cell_class_export(self, setup):
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell(
                "c1",
                "class Box:\n"
                "    def __init__(self, value):\n"
                "        self.value = value\n",
            )
            .add_cell("c2", "result = Box(7).value", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                result1 = execute_cell_and_wait(ws, "c1")
                assert result1["type"] == "cell_output"
                assert result1["payload"]["outputs"]["Box"]["content_type"] == "module/cell"

                result2 = execute_cell_and_wait(ws, "c2")
                assert result2["type"] == "cell_output"
                assert result2["payload"]["outputs"]["result"]["preview"] == 7

    def test_cross_cell_exported_class_instance(self, setup):
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell(
                "c1",
                "class Person:\n"
                "    name = 'John'\n"
                "    age = 20\n"
                "\n"
                "    def __str__(self):\n"
                "        return f'{self.name}:{self.age}'\n",
            )
            .add_cell("c2", "p = Person()", after="c1")
            .add_cell("c3", "rendered = str(p)", after="c2")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                result1 = execute_cell_and_wait(ws, "c1")
                assert result1["type"] == "cell_output"
                assert result1["payload"]["outputs"]["Person"]["content_type"] == "module/cell"

                result2 = execute_cell_and_wait(ws, "c2")
                assert result2["type"] == "cell_output"
                assert result2["payload"]["outputs"]["p"]["content_type"] == "module/cell-instance"

                result3 = execute_cell_and_wait(ws, "c3")
                assert result3["type"] == "cell_output"
                assert result3["payload"]["outputs"]["rendered"]["preview"] == "John:20"


class TestDirectHttpModuleExports:
    """Direct HTTP workers should preserve source-backed exports too."""

    def test_function_export_over_direct_http_worker(self, setup, notebook_executor_server):
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell(
                "c1",
                "import math\n\ndef area(r):\n    return math.pi * r * r",
            )
            .add_cell("c2", "result = round(area(2), 5)", after="c1")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            catalog = _put_notebook_workers(
                client,
                sid,
                [
                    {
                        "name": "gpu-http",
                        "backend": "executor",
                        "runtime_id": "gpu-http-a100",
                        "config": {"url": notebook_executor_server["execute_url"]},
                    }
                ],
            )
            assert any(worker["name"] == "gpu-http" for worker in catalog["workers"])
            _put_notebook_worker(client, sid, "gpu-http")

            with ws_connect(client, sid) as ws:
                result1 = execute_cell_and_wait(ws, "c1")
                assert result1["type"] == "cell_output"
                assert result1["payload"]["execution_method"] == "executor"
                assert result1["payload"]["remote_worker"] == "gpu-http"
                assert result1["payload"]["remote_transport"] == "direct"
                assert result1["payload"]["outputs"]["area"]["content_type"] == "module/cell"

                result2 = execute_cell_and_wait(ws, "c2")
                assert result2["type"] == "cell_output"
                assert result2["payload"]["execution_method"] == "executor"
                assert result2["payload"]["remote_worker"] == "gpu-http"
                assert result2["payload"]["remote_transport"] == "direct"
                assert result2["payload"]["outputs"]["result"]["preview"] == 12.56637

    def test_class_instance_export_over_direct_http_worker(
        self,
        setup,
        notebook_executor_server,
    ):
        client, tmp = setup
        nb = (
            NotebookBuilder(tmp)
            .add_cell(
                "c1",
                "class Person:\n"
                "    name = 'John'\n"
                "    age = 20\n"
                "\n"
                "    def __str__(self):\n"
                "        return f'{self.name}:{self.age}'\n",
            )
            .add_cell("c2", "p = Person()", after="c1")
            .add_cell("c3", "rendered = str(p)", after="c2")
        )

        with open_notebook_session(client, nb.path) as (sid, session):
            catalog = _put_notebook_workers(
                client,
                sid,
                [
                    {
                        "name": "gpu-http",
                        "backend": "executor",
                        "runtime_id": "gpu-http-a100",
                        "config": {"url": notebook_executor_server["execute_url"]},
                    }
                ],
            )
            assert any(worker["name"] == "gpu-http" for worker in catalog["workers"])
            _put_notebook_worker(client, sid, "gpu-http")

            with ws_connect(client, sid) as ws:
                result1 = execute_cell_and_wait(ws, "c1")
                result2 = execute_cell_and_wait(ws, "c2")
                result3 = execute_cell_and_wait(ws, "c3")

                assert result1["type"] == "cell_output"
                assert result1["payload"]["remote_transport"] == "direct"
                assert result1["payload"]["outputs"]["Person"]["content_type"] == "module/cell"

                assert result2["type"] == "cell_output"
                assert result2["payload"]["remote_transport"] == "direct"
                assert result2["payload"]["outputs"]["p"]["content_type"] == "module/cell-instance"

                assert result3["type"] == "cell_output"
                assert result3["payload"]["remote_transport"] == "direct"
                assert result3["payload"]["outputs"]["rendered"]["preview"] == "John:20"
