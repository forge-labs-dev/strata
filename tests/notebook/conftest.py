"""Notebook-local pytest fixtures for fast default test runs."""

from __future__ import annotations

import asyncio
import json
import shlex
import sys
from pathlib import Path

import pytest


def _fake_uv_sync(notebook_dir: Path, *, timeout: int = 60) -> bool:
    """Create a minimal local env scaffold without invoking ``uv``."""
    del timeout
    notebook_dir = Path(notebook_dir)

    lockfile = notebook_dir / "uv.lock"
    if not lockfile.exists():
        lockfile.write_text(
            '\n'.join([
                "version = 1",
                'requires-python = ">=3.12"',
                "",
                "[[package]]",
                'name = "pyarrow"',
                'version = "0.0.0"',
                "",
            ]),
            encoding="utf-8",
        )

    venv_python = notebook_dir / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True, exist_ok=True)
    if not venv_python.exists():
        venv_python.write_text(
            (
                "#!/bin/sh\n"
                f'exec {shlex.quote(sys.executable)} "$@"\n'
            ),
            encoding="utf-8",
        )
        venv_python.chmod(0o755)

    return True


@pytest.fixture(autouse=True)
def fast_notebook_env(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest):
    """Stub notebook env setup unless a test explicitly opts into integration."""
    async def _noop_start(self):
        self._started = True

    if not request.node.get_closest_marker("warm_pool"):
        monkeypatch.setattr("strata.notebook.pool.WarmProcessPool.start", _noop_start)

    if request.node.get_closest_marker("integration"):
        return

    monkeypatch.setattr("strata.notebook.writer._uv_sync", _fake_uv_sync)
    monkeypatch.setattr("strata.notebook.session._uv_sync", _fake_uv_sync)

    async def _run_harness_direct(
        self,
        manifest_path: Path,
        venv_python: Path,
        timeout_seconds: float,
    ) -> dict[str, object]:
        """Run the harness directly with Python instead of ``uv run``."""
        cmd = [str(venv_python), str(self.harness_path), str(manifest_path)]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(self.session.path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=timeout_seconds,
            )
        except asyncio.CancelledError:
            proc.kill()
            await asyncio.shield(proc.wait())
            raise
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise

        result_path = manifest_path.parent / "manifest.json"
        if not result_path.exists():
            raise RuntimeError(
                f"Harness did not produce manifest.json: {stderr.decode()}"
            )

        with open(result_path) as f:
            return json.load(f)

    monkeypatch.setattr("strata.notebook.executor.CellExecutor._run_harness", _run_harness_direct)
