"""Tests for the notebook HTTP executor."""

from __future__ import annotations

import json

import httpx

from strata.notebook.remote_bundle import unpack_notebook_output_bundle
from strata.notebook.remote_executor import NOTEBOOK_EXECUTOR_PROTOCOL_VERSION


def test_remote_executor_health(notebook_executor_server):
    """The notebook executor should expose its protocol and health."""
    response = httpx.get(f"{notebook_executor_server['base_url']}/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"
    assert NOTEBOOK_EXECUTOR_PROTOCOL_VERSION in payload["capabilities"]["protocol_versions"]


def test_remote_executor_executes_simple_cell(tmp_path, notebook_executor_server):
    """The HTTP executor should return a notebook result bundle."""
    metadata = {
        "protocol_version": NOTEBOOK_EXECUTOR_PROTOCOL_VERSION,
        "source": "x = 1 + 1",
        "timeout_seconds": 10.0,
        "inputs": {},
        "mounts": [],
        "env": {},
    }

    response = httpx.post(
        notebook_executor_server["execute_url"],
        files={
            "metadata": (
                "metadata.json",
                json.dumps(metadata).encode("utf-8"),
                "application/json",
            ),
        },
        timeout=10.0,
    )

    assert response.status_code == 200
    assert (
        response.headers["X-Strata-Notebook-Executor-Protocol"]
        == NOTEBOOK_EXECUTOR_PROTOCOL_VERSION
    )

    bundle_path = tmp_path / "bundle.tar.gz"
    bundle_path.write_bytes(response.content)
    unpacked = unpack_notebook_output_bundle(bundle_path, tmp_path / "unpacked")

    assert unpacked["success"] is True
    assert unpacked["variables"]["x"]["preview"] == 2
    assert (tmp_path / "unpacked" / "x.json").exists()


def test_remote_executor_rejects_file_mounts(notebook_executor_server):
    """Remote execution should reject notebook-declared file:// mounts."""
    metadata = {
        "protocol_version": NOTEBOOK_EXECUTOR_PROTOCOL_VERSION,
        "source": "x = 1",
        "timeout_seconds": 10.0,
        "inputs": {},
        "mounts": [
            {
                "name": "raw_data",
                "uri": "file:///tmp/data",
                "mode": "ro",
                "pin": None,
            }
        ],
        "env": {},
    }

    response = httpx.post(
        notebook_executor_server["execute_url"],
        files={
            "metadata": (
                "metadata.json",
                json.dumps(metadata).encode("utf-8"),
                "application/json",
            ),
        },
        timeout=10.0,
    )

    assert response.status_code == 400
    assert "does not support file:// mount" in response.json()["detail"]
