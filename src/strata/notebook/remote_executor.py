"""HTTP executor app for notebook cell execution."""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from strata.notebook.models import MountSpec
from strata.notebook.mounts import MountResolver, parse_mount_uri
from strata.notebook.remote_bundle import pack_notebook_output_bundle

NOTEBOOK_EXECUTOR_PROTOCOL_VERSION = "notebook-cell-v1"


async def _run_harness(
    harness_path: Path,
    manifest_path: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Run the notebook harness with one manifest file."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(harness_path),
        str(manifest_path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout_seconds,
        )
    except TimeoutError:
        proc.kill()
        await proc.wait()
        raise TimeoutError()

    result_path = manifest_path.parent / "manifest.json"
    if not result_path.exists():
        raise RuntimeError(
            f"Harness did not produce manifest.json: {stderr.decode()}"
        )

    with open(result_path, encoding="utf-8") as f:
        return json.load(f)


def create_notebook_executor_app() -> FastAPI:
    """Create a standalone notebook executor HTTP app."""
    app = FastAPI(
        title="Strata Notebook Executor",
        description="Reference notebook executor for remote notebook workers",
        version="1.0.0",
    )

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {
            "status": "healthy",
            "capabilities": {
                "protocol_versions": [NOTEBOOK_EXECUTOR_PROTOCOL_VERSION],
                "executor_refs": ["notebook_cell@v1"],
            },
        }

    @app.post("/v1/notebook-execute")
    async def execute(http_request: Request) -> Response:
        form = await http_request.form()
        metadata_file = form.get("metadata")
        if metadata_file is None or isinstance(metadata_file, str):
            raise HTTPException(status_code=400, detail="Missing metadata")

        try:
            metadata = json.loads((await metadata_file.read()).decode("utf-8"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid metadata: {exc}")

        protocol_version = metadata.get("protocol_version")
        if protocol_version != NOTEBOOK_EXECUTOR_PROTOCOL_VERSION:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported protocol version: {protocol_version}. "
                    f"Expected: {NOTEBOOK_EXECUTOR_PROTOCOL_VERSION}"
                ),
            )

        source = str(metadata.get("source", ""))
        timeout_seconds = float(metadata.get("timeout_seconds", 30.0))
        raw_inputs = metadata.get("inputs", {})
        raw_mounts = metadata.get("mounts", [])
        runtime_env = metadata.get("env", {})

        if not isinstance(raw_inputs, dict):
            raise HTTPException(status_code=400, detail="inputs must be an object")
        if not isinstance(raw_mounts, list):
            raise HTTPException(status_code=400, detail="mounts must be a list")

        mount_specs = [MountSpec(**mount) for mount in raw_mounts]
        for mount in mount_specs:
            scheme, _path = parse_mount_uri(mount.uri)
            if scheme == "file":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Remote execution does not support file:// mount "
                        f"'{mount.name}'"
                    ),
                )

        with tempfile.TemporaryDirectory(prefix="strata_notebook_executor_") as tmpdir:
            output_dir = Path(tmpdir)

            inputs: dict[str, dict[str, str]] = {}
            for var_name, spec in raw_inputs.items():
                if not isinstance(spec, dict):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Input spec for {var_name} must be an object",
                    )
                file_name = Path(str(spec.get("file", ""))).name
                if not file_name:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Input {var_name} is missing file name",
                    )
                upload = form.get(file_name)
                if upload is None or isinstance(upload, str):
                    raise HTTPException(
                        status_code=400,
                        detail=f"Missing uploaded input file: {file_name}",
                    )
                data = await upload.read()
                with open(output_dir / file_name, "wb") as f:
                    f.write(data)
                inputs[var_name] = {
                    "content_type": str(spec.get("content_type", "pickle/object")),
                    "file": file_name,
                }

            mount_resolver = MountResolver(
                cache_dir=output_dir / "mount_cache",
            )
            resolved_mounts = await mount_resolver.prepare_mounts(mount_specs)
            manifest_mounts = {
                name: {
                    "uri": rm.spec.uri,
                    "mode": rm.spec.mode.value,
                    "local_path": str(rm.local_path),
                }
                for name, rm in resolved_mounts.items()
            }

            manifest = {
                "source": source,
                "inputs": inputs,
                "output_dir": str(output_dir),
                "mounts": manifest_mounts,
                "env": runtime_env,
            }
            manifest_path = output_dir / "manifest.json"
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump(manifest, f)

            harness_path = Path(__file__).parent / "harness.py"
            try:
                result = await _run_harness(
                    harness_path,
                    manifest_path,
                    timeout_seconds,
                )
                if result.get("success", False):
                    await mount_resolver.sync_back(resolved_mounts)
            except TimeoutError:
                return JSONResponse(
                    status_code=408,
                    content={
                        "success": False,
                        "error": f"Cell execution timed out after {timeout_seconds}s",
                    },
                )
            except Exception as exc:
                return JSONResponse(
                    status_code=500,
                    content={"success": False, "error": str(exc)},
                )

            bundle_path = output_dir / "notebook-output-bundle.tar.gz"
            pack_notebook_output_bundle(bundle_path, result, output_dir)
            return Response(
                content=bundle_path.read_bytes(),
                media_type="application/gzip",
                headers={
                    "X-Strata-Notebook-Executor-Protocol": NOTEBOOK_EXECUTOR_PROTOCOL_VERSION,
                },
            )

    return app
