"""Tests for the notebook HTTP executor."""

from __future__ import annotations

import io
import json
import tarfile
import uuid

import httpx

from strata.artifact_store import TransformSpec
from strata.notebook.remote_bundle import SCHEMA_VERSION, unpack_notebook_output_bundle
from strata.notebook.remote_executor import (
    NOTEBOOK_EXECUTOR_MANIFEST_VERSION,
    NOTEBOOK_EXECUTOR_PROTOCOL_VERSION,
    NOTEBOOK_EXECUTOR_TRANSFORM_REF,
)
from strata.transforms.signed_urls import generate_build_manifest
from strata.types import EXECUTOR_PROTOCOL_HEADER, EXECUTOR_PROTOCOL_VERSION


def test_remote_executor_health(notebook_executor_server):
    """The notebook executor should expose its protocol and health."""
    response = httpx.get(f"{notebook_executor_server['base_url']}/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "healthy"
    assert EXECUTOR_PROTOCOL_VERSION in payload["capabilities"]["protocol_versions"]
    assert NOTEBOOK_EXECUTOR_TRANSFORM_REF in payload["capabilities"]["transform_refs"]
    assert (
        payload["capabilities"]["features"]["notebook_protocol_version"]
        == NOTEBOOK_EXECUTOR_PROTOCOL_VERSION
    )
    assert payload["capabilities"]["features"]["pull_model"] is True


def test_remote_executor_executes_simple_cell_via_notebook_endpoint(
    tmp_path,
    notebook_executor_server,
):
    """The compatibility notebook endpoint should return a notebook result bundle."""
    metadata = {
        "protocol_version": NOTEBOOK_EXECUTOR_PROTOCOL_VERSION,
        "source": "x = 1 + 1",
        "timeout_seconds": 10.0,
        "inputs": {},
        "mounts": [],
        "env": {},
    }

    response = httpx.post(
        notebook_executor_server["notebook_execute_url"],
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

    bundle_path = tmp_path / "bundle.tar"
    bundle_path.write_bytes(response.content)
    unpacked = unpack_notebook_output_bundle(bundle_path, tmp_path / "unpacked")

    assert unpacked["success"] is True
    assert unpacked["variables"]["x"]["preview"] == 2
    assert (tmp_path / "unpacked" / "x.json").exists()


def test_remote_executor_executes_simple_cell_via_executor_v1(
    tmp_path,
    notebook_executor_server,
):
    """The standard executor v1 endpoint should support notebook_cell@v1."""
    metadata = {
        "protocol_version": EXECUTOR_PROTOCOL_VERSION,
        "build_id": "b-test",
        "tenant": None,
        "principal": None,
        "provenance_hash": "abc123",
        "transform": {
            "ref": NOTEBOOK_EXECUTOR_TRANSFORM_REF,
            "code_hash": "def456",
            "params": {
                "source": "x = 1 + 1",
                "timeout_seconds": 10.0,
                "mounts": [],
                "env": {},
            },
        },
        "inputs": [],
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
        headers={EXECUTOR_PROTOCOL_HEADER: EXECUTOR_PROTOCOL_VERSION},
        timeout=10.0,
    )

    assert response.status_code == 200
    assert response.headers[EXECUTOR_PROTOCOL_HEADER] == EXECUTOR_PROTOCOL_VERSION
    assert (
        response.headers["X-Strata-Notebook-Executor-Protocol"]
        == NOTEBOOK_EXECUTOR_PROTOCOL_VERSION
    )

    bundle_path = tmp_path / "bundle-v1.tar"
    bundle_path.write_bytes(response.content)
    unpacked = unpack_notebook_output_bundle(bundle_path, tmp_path / "unpacked-v1")

    assert unpacked["success"] is True
    assert unpacked["variables"]["x"]["preview"] == 2


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
        notebook_executor_server["notebook_execute_url"],
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


def test_remote_executor_executes_signed_manifest_build(
    notebook_executor_server,
    notebook_build_server,
):
    """The manifest endpoint should execute, upload, and finalize a notebook bundle."""
    artifact_store = notebook_build_server["artifact_store"]
    build_store = notebook_build_server["build_store"]
    base_url = notebook_build_server["base_url"]

    input_artifact_id = f"input-{uuid.uuid4().hex[:8]}"
    input_version = artifact_store.create_artifact(
        artifact_id=input_artifact_id,
        provenance_hash=f"prov-{input_artifact_id}",
        transform_spec=TransformSpec(
            executor="notebook/cell@v1",
            params={"content_type": "json/object"},
            inputs=[],
        ),
    )
    input_bytes = json.dumps(2).encode("utf-8")
    artifact_store.write_blob(input_artifact_id, input_version, input_bytes)
    artifact_store.finalize_artifact(
        input_artifact_id,
        input_version,
        schema_json="",
        row_count=0,
        byte_size=len(input_bytes),
    )

    output_artifact_id = f"output-{uuid.uuid4().hex[:8]}"
    output_version = artifact_store.create_artifact(
        artifact_id=output_artifact_id,
        provenance_hash=f"prov-{output_artifact_id}",
        transform_spec=TransformSpec(
            executor=NOTEBOOK_EXECUTOR_TRANSFORM_REF,
            params={},
            inputs=[f"strata://artifact/{input_artifact_id}@v={input_version}"],
        ),
    )

    build_id = f"build-{uuid.uuid4().hex[:8]}"
    params = {
        "source": "result = value + 1",
        "timeout_seconds": 10.0,
        "mounts": [],
        "env": {},
        "input_specs": {
            "value": {
                "uri": f"strata://artifact/{input_artifact_id}@v={input_version}",
                "content_type": "json/object",
            }
        },
        "output_format": "notebook-output-bundle@v1",
        "_dispatch_mode": "external",
    }
    build_store.create_build(
        build_id=build_id,
        artifact_id=output_artifact_id,
        version=output_version,
        executor_ref=NOTEBOOK_EXECUTOR_TRANSFORM_REF,
        executor_url=notebook_executor_server["execute_url"],
        input_uris=[f"strata://artifact/{input_artifact_id}@v={input_version}"],
        params=params,
    )
    build_store.start_build(build_id)

    manifest = generate_build_manifest(
        base_url=base_url,
        build_id=build_id,
        metadata={
            "build_id": build_id,
            "artifact_id": output_artifact_id,
            "version": output_version,
            "executor_ref": NOTEBOOK_EXECUTOR_TRANSFORM_REF,
            "params": params,
        },
        input_artifacts=[(input_artifact_id, input_version)],
        max_output_bytes=notebook_build_server["config"].max_transform_output_bytes,
        url_expiry_seconds=notebook_build_server["config"].signed_url_expiry_seconds,
    )

    response = httpx.post(
        notebook_executor_server["manifest_execute_url"],
        json=manifest.to_dict(),
        timeout=20.0,
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["protocol_version"] == NOTEBOOK_EXECUTOR_MANIFEST_VERSION

    build = build_store.get_build(build_id)
    assert build is not None
    assert build.state == "ready"

    blob = artifact_store.read_blob(output_artifact_id, output_version)
    assert blob is not None
    bundle_path = notebook_build_server["config"].cache_dir / "manifest-bundle.tar"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_path.write_bytes(blob)
    unpacked = unpack_notebook_output_bundle(bundle_path, bundle_path.parent / "manifest-bundle")
    assert unpacked["success"] is True
    assert unpacked["variables"]["result"]["preview"] == 3


def test_remote_executor_manifest_reports_finalize_failure(
    notebook_executor_server,
    notebook_build_server,
):
    """Manifest execution should surface finalize failures as 502s."""
    artifact_store = notebook_build_server["artifact_store"]
    build_store = notebook_build_server["build_store"]
    base_url = notebook_build_server["base_url"]

    input_artifact_id = f"input-{uuid.uuid4().hex[:8]}"
    input_version = artifact_store.create_artifact(
        artifact_id=input_artifact_id,
        provenance_hash=f"prov-{input_artifact_id}",
        transform_spec=TransformSpec(
            executor="notebook/cell@v1",
            params={"content_type": "json/object"},
            inputs=[],
        ),
    )
    input_bytes = json.dumps(2).encode("utf-8")
    artifact_store.write_blob(input_artifact_id, input_version, input_bytes)
    artifact_store.finalize_artifact(
        input_artifact_id,
        input_version,
        schema_json="",
        row_count=0,
        byte_size=len(input_bytes),
    )

    output_artifact_id = f"output-{uuid.uuid4().hex[:8]}"
    output_version = artifact_store.create_artifact(
        artifact_id=output_artifact_id,
        provenance_hash=f"prov-{output_artifact_id}",
        transform_spec=TransformSpec(
            executor=NOTEBOOK_EXECUTOR_TRANSFORM_REF,
            params={},
            inputs=[f"strata://artifact/{input_artifact_id}@v={input_version}"],
        ),
    )

    build_id = f"build-{uuid.uuid4().hex[:8]}"
    params = {
        "source": "result = value + 1",
        "timeout_seconds": 10.0,
        "mounts": [],
        "env": {},
        "input_specs": {
            "value": {
                "uri": f"strata://artifact/{input_artifact_id}@v={input_version}",
                "content_type": "json/object",
            }
        },
        "output_format": "notebook-output-bundle@v1",
        "_dispatch_mode": "external",
    }
    build_store.create_build(
        build_id=build_id,
        artifact_id=output_artifact_id,
        version=output_version,
        executor_ref=NOTEBOOK_EXECUTOR_TRANSFORM_REF,
        executor_url=notebook_executor_server["execute_url"],
        input_uris=[f"strata://artifact/{input_artifact_id}@v={input_version}"],
        params=params,
    )
    build_store.start_build(build_id)

    manifest = generate_build_manifest(
        base_url=base_url,
        build_id=build_id,
        metadata={
            "build_id": build_id,
            "artifact_id": output_artifact_id,
            "version": output_version,
            "executor_ref": NOTEBOOK_EXECUTOR_TRANSFORM_REF,
            "params": params,
        },
        input_artifacts=[(input_artifact_id, input_version)],
        max_output_bytes=notebook_build_server["config"].max_transform_output_bytes,
        url_expiry_seconds=notebook_build_server["config"].signed_url_expiry_seconds,
    ).to_dict()
    manifest["finalize_url"] = f"{base_url}/v1/builds/{build_id}/missing-finalize"

    response = httpx.post(
        notebook_executor_server["manifest_execute_url"],
        json=manifest,
        timeout=20.0,
    )

    assert response.status_code == 502
    assert "Failed to finalize notebook bundle build" in response.json()["detail"]


def test_service_finalize_rejects_incomplete_notebook_bundle(notebook_build_server):
    """Finalize should reject malformed notebook bundles and fail the artifact."""
    artifact_store = notebook_build_server["artifact_store"]
    build_store = notebook_build_server["build_store"]
    base_url = notebook_build_server["base_url"]

    artifact_id = f"invalid-bundle-{uuid.uuid4().hex[:8]}"
    version = artifact_store.create_artifact(
        artifact_id=artifact_id,
        provenance_hash=f"prov-{artifact_id}",
        transform_spec=TransformSpec(
            executor=NOTEBOOK_EXECUTOR_TRANSFORM_REF,
            params={},
            inputs=[],
        ),
    )
    build_id = f"build-{uuid.uuid4().hex[:8]}"
    build_store.create_build(
        build_id=build_id,
        artifact_id=artifact_id,
        version=version,
        executor_ref=NOTEBOOK_EXECUTOR_TRANSFORM_REF,
        executor_url="http://executor.invalid/v1/execute",
        input_uris=[],
        params={"output_format": "notebook-output-bundle@v1"},
    )
    build_store.start_build(build_id)

    bundle_bytes = io.BytesIO()
    with tarfile.open(fileobj=bundle_bytes, mode="w") as tar:
        manifest_bytes = json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "success": True,
                "variables": {
                    "result": {
                        "content_type": "json/object",
                        "file": "files/result.json",
                    }
                },
                "stdout_file": "stdout.txt",
                "stderr_file": "stderr.txt",
            }
        ).encode("utf-8")
        info = tarfile.TarInfo(name="manifest.json")
        info.size = len(manifest_bytes)
        tar.addfile(info, io.BytesIO(manifest_bytes))
        for name in ("stdout.txt", "stderr.txt"):
            payload = b""
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))

    artifact_store.write_blob(artifact_id, version, bundle_bytes.getvalue())

    response = httpx.post(
        f"{base_url}/v1/builds/{build_id}/finalize",
        json={"output_format": "notebook-output-bundle@v1"},
        timeout=10.0,
    )

    assert response.status_code == 400
    assert "Invalid notebook output bundle" in response.json()["detail"]

    build = build_store.get_build(build_id)
    assert build is not None
    assert build.state == "failed"

    artifact = artifact_store.get_artifact(artifact_id, version)
    assert artifact is not None
    assert artifact.state == "failed"
