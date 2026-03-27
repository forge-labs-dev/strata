"""Inspect REPL — on-demand interactive exploration of cell artifacts.

Opens a subprocess with a cell's input variables pre-loaded, accepts
eval expressions, and returns results. The subprocess stays alive until
explicitly closed, allowing multiple evaluations without re-loading.

Communication protocol:
  Parent → Child: JSON lines on stdin  {"expr": "df.describe()"}
  Child → Parent: JSON lines on stdout {"ok": true, "result": "...", "type": "str"}
                                     or {"ok": false, "error": "..."}
  Special commands:
    {"cmd": "ping"}  → {"ok": true, "result": "pong"}
    {"cmd": "close"} → process exits
"""

from __future__ import annotations

import asyncio
import json
import logging
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)

# Harness script injected into the inspect subprocess.
# serializer.py is copied into the same temp dir so Path(__file__).parent works.
_INSPECT_HARNESS = textwrap.dedent(r'''
import importlib.util
import io
import json
import sys
import traceback
from pathlib import Path

def _load_serializer():
    _p = Path(__file__).parent / "serializer.py"
    _spec = importlib.util.spec_from_file_location("_nb_serializer", _p)
    _m = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_m)
    return _m

_ser = _load_serializer()


def _repr_value(value, max_len=4000):
    """Produce a display-friendly representation."""
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return value.to_string(max_rows=20, max_cols=15)
        if isinstance(value, pd.Series):
            return value.to_string(max_rows=20)
    except ImportError:
        pass

    try:
        import pyarrow as pa
        if isinstance(value, pa.Table):
            return value.to_pandas().to_string(max_rows=20, max_cols=15)
    except ImportError:
        pass

    r = repr(value)
    if len(r) > max_len:
        r = r[:max_len] + "... (truncated)"
    return r


def _detect_type(value):
    """Return a human-readable type string."""
    t = type(value).__name__
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return f"DataFrame ({value.shape[0]} rows x {value.shape[1]} cols)"
        if isinstance(value, pd.Series):
            return f"Series ({len(value)} items)"
    except ImportError:
        pass
    try:
        import numpy as np
        if isinstance(value, np.ndarray):
            return f"ndarray {value.shape}"
    except ImportError:
        pass
    return t


def main():
    """Read JSON commands from stdin, evaluate, write results to stdout."""
    # Load manifest from argv[1]
    manifest_path = sys.argv[1]
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Build namespace from inputs
    namespace = {}
    inputs = manifest.get("inputs", {})
    output_dir = manifest.get("output_dir", "/tmp")

    for var_name, spec in inputs.items():
        content_type = spec.get("content_type", "")
        file_name = spec.get("file", "")
        if not file_name:
            continue
        full_path = Path(output_dir) / file_name
        if not full_path.exists():
            continue
        try:
            namespace[var_name] = _ser.deserialize_value(content_type, full_path)
        except Exception as e:
            namespace[var_name] = f"<load error: {e}>"

    # Signal ready
    sys.stdout.write(json.dumps({"ok": True, "result": "ready", "type": "str"}) + "\n")
    sys.stdout.flush()

    # REPL loop: read JSON lines from stdin, evaluate, write results
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            cmd = json.loads(line)
        except json.JSONDecodeError as e:
            sys.stdout.write(json.dumps({"ok": False, "error": f"JSON parse error: {e}"}) + "\n")
            sys.stdout.flush()
            continue

        # Special commands
        if cmd.get("cmd") == "ping":
            sys.stdout.write(json.dumps({"ok": True, "result": "pong", "type": "str"}) + "\n")
            sys.stdout.flush()
            continue
        if cmd.get("cmd") == "close":
            break

        # Evaluate expression
        expr = cmd.get("expr", "")
        if not expr:
            sys.stdout.write(json.dumps({"ok": False, "error": "Empty expression"}) + "\n")
            sys.stdout.flush()
            continue

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        capture_out = io.StringIO()
        capture_err = io.StringIO()

        try:
            sys.stdout = capture_out
            sys.stderr = capture_err

            # Try eval first (expression), then exec (statement)
            try:
                result = eval(expr, namespace)
                sys.stdout = old_stdout
                sys.stderr = old_stderr

                display = _repr_value(result)
                result_type = _detect_type(result)
                stdout_text = capture_out.getvalue()

                response = {
                    "ok": True,
                    "result": display,
                    "type": result_type,
                }
                if stdout_text:
                    response["stdout"] = stdout_text

                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()

            except SyntaxError:
                # Not an expression — try exec (statement)
                exec(expr, namespace)
                sys.stdout = old_stdout
                sys.stderr = old_stderr

                stdout_text = capture_out.getvalue()
                response = {
                    "ok": True,
                    "result": stdout_text if stdout_text else "(no output)",
                    "type": "None",
                }
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()

        except Exception as e:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            tb = traceback.format_exc()
            sys.stdout.write(json.dumps({"ok": False, "error": tb}) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
''').lstrip()


class InspectSession:
    """An active inspect REPL session for a cell.

    Spawns a subprocess with the cell's input artifacts pre-loaded,
    then accepts eval commands and returns results.

    Attributes:
        cell_id: Cell being inspected
        process: The subprocess running the REPL
        ready: Whether the subprocess has finished loading
    """

    def __init__(self, cell_id: str):
        self.cell_id = cell_id
        self.process: asyncio.subprocess.Process | None = None
        self.ready = False
        self._manifest_dir: Path | None = None

    async def start(
        self,
        session: NotebookSession,
        timeout_seconds: float = 15,
    ) -> str:
        """Start the inspect subprocess.

        Resolves the cell's upstream inputs, writes them to a temp dir,
        then spawns a Python process with those inputs loaded.

        Args:
            session: NotebookSession instance
            timeout_seconds: Startup timeout

        Returns:
            Status message ("ready" or error)
        """
        import tempfile

        from strata.notebook.executor import CellExecutor

        # Create temp dir for input files (persist for session lifetime)
        self._manifest_dir = Path(tempfile.mkdtemp(prefix="strata_inspect_"))

        # Materialise upstreams then load input blobs for this cell
        executor = CellExecutor(session, session.warm_pool)
        await executor._materialize_upstreams(self.cell_id)
        input_specs = executor._load_input_blobs(
            self.cell_id, self._manifest_dir
        )

        # Write manifest
        manifest = {
            "inputs": input_specs,
            "output_dir": str(self._manifest_dir),
        }
        manifest_path = self._manifest_dir / "inspect_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f)

        # Copy serializer.py alongside the harness so the harness can load it
        import shutil
        _serializer_src = Path(__file__).parent / "serializer.py"
        shutil.copy2(_serializer_src, self._manifest_dir / "serializer.py")

        # Write the inspect harness to a temp file
        harness_path = self._manifest_dir / "_inspect_harness.py"
        with open(harness_path, "w") as f:
            f.write(_INSPECT_HARNESS)

        # Spawn subprocess
        cmd = [
            "uv", "run", "--directory", str(session.path),
            "python", str(harness_path), str(manifest_path),
        ]

        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(session.path),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Wait for "ready" signal
        try:
            assert self.process.stdout is not None
            line = await asyncio.wait_for(
                self.process.stdout.readline(),
                timeout=timeout_seconds,
            )
            msg = json.loads(line.decode().strip())
            if msg.get("ok") and msg.get("result") == "ready":
                self.ready = True
                return "ready"
            return msg.get("error", "Unknown startup error")
        except TimeoutError:
            await self.close()
            return "Inspect process timed out during startup"
        except Exception as e:
            await self.close()
            return f"Inspect startup failed: {e}"

    async def evaluate(
        self, expr: str, timeout_seconds: float = 10
    ) -> dict[str, Any]:
        """Evaluate an expression in the inspect subprocess.

        Args:
            expr: Python expression or statement to evaluate
            timeout_seconds: Eval timeout

        Returns:
            Dict with ok, result/error, type, stdout
        """
        if not self.ready or self.process is None:
            return {"ok": False, "error": "Inspect session not ready"}

        if self.process.returncode is not None:
            self.ready = False
            return {"ok": False, "error": "Inspect process has exited"}

        try:
            # Send expression
            assert self.process.stdin is not None
            assert self.process.stdout is not None
            cmd = json.dumps({"expr": expr}) + "\n"
            self.process.stdin.write(cmd.encode())
            await self.process.stdin.drain()

            # Read result
            line = await asyncio.wait_for(
                self.process.stdout.readline(),
                timeout=timeout_seconds,
            )
            if not line:
                self.ready = False
                return {"ok": False, "error": "Inspect process closed unexpectedly"}

            return json.loads(line.decode().strip())

        except TimeoutError:
            return {"ok": False, "error": f"Evaluation timed out after {timeout_seconds}s"}
        except Exception as e:
            return {"ok": False, "error": f"Evaluation failed: {e}"}

    async def close(self) -> None:
        """Close the inspect subprocess and clean up."""
        if self.process is not None:
            try:
                if self.process.returncode is None:
                    # Send close command
                    assert self.process.stdin is not None
                    cmd = json.dumps({"cmd": "close"}) + "\n"
                    self.process.stdin.write(cmd.encode())
                    await self.process.stdin.drain()
                    # Give it a moment to exit gracefully
                    try:
                        await asyncio.wait_for(self.process.wait(), timeout=2)
                    except TimeoutError:
                        self.process.kill()
                        await self.process.wait()
            except Exception:
                try:
                    self.process.kill()
                    await self.process.wait()
                except Exception:
                    pass

        self.ready = False
        self.process = None

        # Clean up temp dir
        if self._manifest_dir and self._manifest_dir.exists():
            import shutil
            try:
                shutil.rmtree(self._manifest_dir)
            except Exception:
                pass
            self._manifest_dir = None


class InspectManager:
    """Manages inspect sessions across notebooks.

    One inspect session can be open per cell at a time.
    """

    def __init__(self):
        self._sessions: dict[str, InspectSession] = {}

    async def open_session(
        self,
        cell_id: str,
        notebook_session: NotebookSession,
    ) -> tuple[InspectSession, str]:
        """Open an inspect session for a cell.

        If a session already exists for this cell, close it first.

        Args:
            cell_id: Cell to inspect
            notebook_session: Parent notebook session

        Returns:
            Tuple of (InspectSession, status_message)
        """
        # Close existing session for this cell
        if cell_id in self._sessions:
            await self._sessions[cell_id].close()
            del self._sessions[cell_id]

        inspect = InspectSession(cell_id)
        status = await inspect.start(notebook_session)

        if inspect.ready:
            self._sessions[cell_id] = inspect

        return inspect, status

    async def get_session(self, cell_id: str) -> InspectSession | None:
        """Get an active inspect session for a cell.

        Args:
            cell_id: Cell ID

        Returns:
            InspectSession or None
        """
        session = self._sessions.get(cell_id)
        if session and not session.ready:
            # Session died — clean up
            await session.close()
            del self._sessions[cell_id]
            return None
        return session

    async def close_session(self, cell_id: str) -> None:
        """Close an inspect session.

        Args:
            cell_id: Cell ID
        """
        session = self._sessions.pop(cell_id, None)
        if session:
            await session.close()

    async def close_all(self) -> None:
        """Close all inspect sessions."""
        for session in self._sessions.values():
            await session.close()
        self._sessions.clear()
