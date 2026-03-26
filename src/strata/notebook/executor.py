"""Cell executor — the ``materialize`` primitive for notebook cells.

Each ``execute_cell`` call is a ``materialize(inputs, transform) → artifact``
operation:

1. **Materialize upstream inputs** — for every upstream variable this cell
   needs, look in the artifact store.  Cache hit → done.  Cache miss →
   recursively ``execute_cell`` on the upstream so it produces the artifact.
2. **Compute provenance** — now that all upstream artifacts exist we can
   build the deterministic hash ``sha256(sorted_input_hashes + source_hash
   + env_hash)``.
3. **Cache check** — if an artifact with matching provenance already exists,
   return immediately (cache hit).
4. **Execute** — spawn the harness subprocess, passing resolved input blobs.
5. **Store outputs** — persist every consumed variable as an artifact.

The cascade planner (``cascade.py``) is a *UI-level* optimisation that
previews which cells will run.  The executor itself is self-contained: you
can call ``execute_cell`` on *any* cell and it will recursively materialise
the full upstream DAG.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from strata.notebook.env import compute_lockfile_hash
from strata.notebook.provenance import compute_provenance_hash, compute_source_hash

if TYPE_CHECKING:
    from strata.notebook.pool import WarmProcessPool
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)

# Well-known module → PyPI package name mappings where they differ.
_MODULE_TO_PACKAGE: dict[str, str] = {
    "cv2": "opencv-python",
    "PIL": "Pillow",
    "sklearn": "scikit-learn",
    "yaml": "pyyaml",
    "bs4": "beautifulsoup4",
    "attr": "attrs",
    "dateutil": "python-dateutil",
    "jose": "python-jose",
    "dotenv": "python-dotenv",
    "gi": "pygobject",
}


def _detect_missing_module(error: str, stderr: str) -> str | None:
    """Parse ModuleNotFoundError to extract package name.

    Returns the PyPI package name to suggest for ``uv add``, or None.
    """
    import re

    combined = f"{error}\n{stderr}"
    m = re.search(r"ModuleNotFoundError: No module named ['\"]([^'\"]+)['\"]", combined)
    if not m:
        return None
    module = m.group(1).split(".")[0]  # top-level module
    return _MODULE_TO_PACKAGE.get(module, module)


class CellExecutionResult:
    """Result from executing a cell.

    Attributes:
        cell_id: ID of the executed cell
        success: Whether execution succeeded
        stdout: Captured standard output
        stderr: Captured standard error
        outputs: Dict of output variable name -> metadata
        duration_ms: Execution duration in milliseconds
        error: Error message if execution failed
        cache_hit: Whether execution was skipped due to cache hit
        artifact_uri: URI of stored artifact (if any)
        execution_method: How the cell was executed (cached, warm, cold)
        mutation_warnings: List of mutation warnings (M6)
    """

    def __init__(
        self,
        cell_id: str,
        success: bool,
        stdout: str = "",
        stderr: str = "",
        outputs: dict[str, Any] | None = None,
        duration_ms: float = 0,
        error: str | None = None,
        cache_hit: bool = False,
        artifact_uri: str | None = None,
        execution_method: str = "cold",
        mutation_warnings: list[dict[str, Any]] | None = None,
        suggest_install: str | None = None,
    ):
        self.cell_id = cell_id
        self.success = success
        self.stdout = stdout
        self.stderr = stderr
        self.outputs = outputs or {}
        self.duration_ms = duration_ms
        self.error = error
        self.cache_hit = cache_hit
        self.artifact_uri = artifact_uri
        self.execution_method = execution_method  # cold, warm, cached
        self.mutation_warnings = mutation_warnings or []
        self.suggest_install = suggest_install  # e.g. "requests"

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        d: dict[str, Any] = {
            "cell_id": self.cell_id,
            "status": "ready" if self.success else "error",
            "stdout": self.stdout,
            "stderr": self.stderr,
            "outputs": self.outputs,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "cache_hit": self.cache_hit,
            "artifact_uri": self.artifact_uri,
            "execution_method": self.execution_method,
            "mutation_warnings": self.mutation_warnings,
        }
        if self.suggest_install:
            d["suggest_install"] = self.suggest_install
        return d


class CellExecutor:
    """Executes notebook cells — the ``materialize`` primitive.

    Each ``execute_cell`` call ensures all upstream artifacts exist
    (recursively materialising them on cache miss), then checks the
    cache for this cell, and finally executes + stores on a miss.

    Attributes:
        session: NotebookSession for the notebook
        harness_path: Path to the harness script
        pool: Optional WarmProcessPool for fast execution (M6)
    """

    def __init__(self, session: NotebookSession, pool: WarmProcessPool | None = None):
        self.session = session
        self.harness_path = Path(__file__).parent / "harness.py"
        self.pool = pool
        # Guard against DAG cycles during recursive materialisation.
        # Per-instance is correct: cycles are only meaningful within a single
        # execute_cell() recursive tree. Each top-level call creates a fresh
        # CellExecutor, so the guard resets between independent executions.
        self._materializing: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def execute_cell(
        self, cell_id: str, source: str, timeout_seconds: float = 30
    ) -> CellExecutionResult:
        """Materialise a cell: ensure inputs → cache check → execute → store.

        This is the single entry-point that implements
        ``materialize(inputs, transform) → artifact``.
        """
        start_time = time.time()

        # --- cycle guard --------------------------------------------------
        if cell_id in self._materializing:
            return CellExecutionResult(
                cell_id=cell_id,
                success=False,
                error=(
                    f"Cycle detected: cell {cell_id} is already being "
                    f"materialised (stack: {self._materializing})"
                ),
            )
        self._materializing.add(cell_id)

        try:
            return await self._materialize(cell_id, source, timeout_seconds, start_time)
        finally:
            self._materializing.discard(cell_id)

    # ------------------------------------------------------------------
    # Core: the materialize pipeline
    # ------------------------------------------------------------------

    async def _materialize(
        self,
        cell_id: str,
        source: str,
        timeout_seconds: float,
        start_time: float,
    ) -> CellExecutionResult:
        try:
            # ① Materialise every upstream cell whose artifact is missing.
            #   This is the recursive ``materialize`` call — each upstream
            #   that is a cache miss will itself execute its own upstreams.
            await self._materialize_upstreams(cell_id)

            # ② Compute provenance (all upstream artifact_uris are now set).
            source_hash = compute_source_hash(source)
            env_hash = compute_lockfile_hash(self.session.path)
            input_hashes = self._collect_input_hashes(cell_id)
            provenance_hash = compute_provenance_hash(
                input_hashes, source_hash, env_hash,
            )

            logger.info(
                "execute_cell %s: source_hash=%s env_hash=%s "
                "input_hashes=%s provenance=%s",
                cell_id,
                source_hash[:12],
                env_hash[:12],
                [h[:12] for h in input_hashes],
                provenance_hash[:12],
            )

            # ③ Cache check for THIS cell.
            artifact_mgr = self.session.get_artifact_manager()
            consumed_vars = (
                self.session.dag.consumed_variables.get(cell_id, set())
                if self.session.dag
                else set()
            )

            cached_artifact = None
            if consumed_vars:
                first_var = sorted(consumed_vars)[0]
                var_prov = hashlib.sha256(
                    f"{provenance_hash}:{first_var}".encode()
                ).hexdigest()
                cached_artifact = artifact_mgr.find_cached(var_prov)
            else:
                cached_artifact = artifact_mgr.find_cached(provenance_hash)

            # Validate cache hit: every consumed variable must have a
            # canonical artifact whose provenance matches.  The global
            # find_by_provenance can return artifacts from old notebook
            # sessions (same SQLite DB, different notebook_id).  We must
            # verify the LOCAL canonical artifact exists AND has the
            # expected provenance hash — not just that it exists.
            notebook_id = self.session.notebook_state.id
            if cached_artifact is not None and consumed_vars:
                for var_name in consumed_vars:
                    canonical_id = (
                        f"nb_{notebook_id}_cell_{cell_id}_var_{var_name}"
                    )
                    var_prov = hashlib.sha256(
                        f"{provenance_hash}:{var_name}".encode()
                    ).hexdigest()
                    canonical_art = (
                        artifact_mgr.artifact_store.get_latest_version(
                            canonical_id,
                        )
                    )
                    if (
                        canonical_art is None
                        or canonical_art.provenance_hash != var_prov
                    ):
                        logger.info(
                            "Cache hit for cell %s invalidated: "
                            "canonical artifact %s %s "
                            "(provenance hit was %s@v=%d, "
                            "expected provenance %s).",
                            cell_id,
                            canonical_id,
                            "not found"
                            if canonical_art is None
                            else f"has stale provenance {canonical_art.provenance_hash[:12]}",
                            cached_artifact.id,
                            cached_artifact.version,
                            var_prov[:12],
                        )
                        cached_artifact = None
                        break

            logger.info(
                "execute_cell %s: consumed_vars=%s cache_hit=%s",
                cell_id,
                consumed_vars,
                cached_artifact is not None,
            )

            if cached_artifact is not None:
                # Cache hit — update cell state and return.
                duration_ms = (time.time() - start_time) * 1000
                cell = next(
                    (c for c in self.session.notebook_state.cells if c.id == cell_id),
                    None,
                )
                if cell:
                    # Populate per-variable URIs from canonical artifacts
                    for var_name in consumed_vars:
                        canonical_id = (
                            f"nb_{notebook_id}_cell_{cell_id}_var_{var_name}"
                        )
                        canonical_art = (
                            artifact_mgr.artifact_store.get_latest_version(
                                canonical_id,
                            )
                        )
                        if canonical_art:
                            uri = (
                                f"strata://artifact/{canonical_art.id}"
                                f"@v={canonical_art.version}"
                            )
                            cell.artifact_uris[var_name] = uri
                            cell.artifact_uri = uri  # backward compat
                return CellExecutionResult(
                    cell_id=cell_id,
                    success=True,
                    outputs={},
                    duration_ms=duration_ms,
                    cache_hit=True,
                    artifact_uri=(
                        f"strata://artifact/{cached_artifact.id}"
                        f"@v={cached_artifact.version}"
                    ),
                    execution_method="cached",
                )

            # ④ Cache miss — execute the cell.
            with tempfile.TemporaryDirectory() as tmpdir:
                output_dir = Path(tmpdir)

                # Load upstream blobs into output_dir for the harness.
                # All artifacts are guaranteed to exist after step ①.
                input_specs = self._load_input_blobs(cell_id, output_dir)

                # Write the manifest the harness expects.
                manifest = {
                    "source": source,
                    "inputs": input_specs,
                    "output_dir": str(output_dir),
                }
                manifest_path = output_dir / "manifest.json"
                with open(manifest_path, "w") as f:
                    json.dump(manifest, f)

                venv_path = self.session.venv_python or Path("python")

                # Try warm pool first (M6).
                execution_method = "cold"
                result = None
                if self.pool is not None:
                    from strata.notebook.pool import PooledCellExecutor

                    pool_result = await PooledCellExecutor.execute_with_pool(
                        self.pool,
                        manifest_path,
                        self.session.path,
                        timeout_seconds,
                    )
                    if pool_result is not None:
                        result = pool_result
                        execution_method = "warm"
                        logger.debug("Executed cell %s with warm process", cell_id)

                # Fall back to cold spawn.
                if result is None:
                    result = await self._run_harness(
                        manifest_path, venv_path, timeout_seconds,
                    )
                    execution_method = "cold"

                duration_ms = (time.time() - start_time) * 1000
                exec_result = self._parse_result(
                    cell_id, result, duration_ms, execution_method,
                )

                # ⑤ Store output artifacts for consumed variables.
                if exec_result.success:
                    stored_ok = self._store_outputs(
                        cell_id,
                        output_dir,
                        provenance_hash,
                        input_hashes,
                        source_hash=source_hash,
                        env_hash=env_hash,
                    )
                    if not stored_ok:
                        logger.error(
                            "Cell %s executed OK but artifact storage failed.",
                            cell_id,
                        )
                        exec_result = CellExecutionResult(
                            cell_id=cell_id,
                            success=False,
                            stdout=exec_result.stdout,
                            stderr=exec_result.stderr,
                            outputs=exec_result.outputs,
                            duration_ms=exec_result.duration_ms,
                            error=(
                                "Cell executed successfully but failed to "
                                "store output artifacts. Check server logs."
                            ),
                            execution_method=exec_result.execution_method,
                        )

                return exec_result

        except TimeoutError:
            duration_ms = (time.time() - start_time) * 1000
            return CellExecutionResult(
                cell_id=cell_id,
                success=False,
                duration_ms=duration_ms,
                error=f"Cell execution timed out after {timeout_seconds}s",
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return CellExecutionResult(
                cell_id=cell_id,
                success=False,
                duration_ms=duration_ms,
                error=f"Execution failed: {e}",
            )

    # ------------------------------------------------------------------
    # ① Materialise upstream cells
    # ------------------------------------------------------------------

    async def _materialize_upstreams(self, cell_id: str) -> None:
        """Ensure every upstream variable has a *current* artifact.

        Always recursively calls ``execute_cell`` on each upstream.
        This is correct because ``execute_cell`` has its own provenance-
        based cache check — an unchanged upstream returns instantly as a
        cache hit, while a stale upstream (e.g. source edited) re-executes.

        The previous approach only checked artifact *existence*, which
        missed the case where an upstream's source changed but its old
        artifact still existed in the store.
        """
        cell = next(
            (c for c in self.session.notebook_state.cells if c.id == cell_id),
            None,
        )
        if cell is None or not cell.upstream_ids:
            return

        # We only need to execute a given upstream once even if it
        # produces multiple variables we reference.
        executed_upstreams: set[str] = set()

        for upstream_id in cell.upstream_ids:
            if upstream_id in executed_upstreams:
                continue

            upstream_cell = next(
                (c for c in self.session.notebook_state.cells if c.id == upstream_id),
                None,
            )
            if upstream_cell is None:
                continue

            # Always materialise the upstream. execute_cell() will
            # return immediately on cache hit (provenance matches),
            # or re-execute if the upstream is stale.
            result = await self.execute_cell(
                upstream_id, upstream_cell.source,
            )
            if not result.success:
                raise RuntimeError(
                    f"Failed to materialise upstream cell {upstream_id}: "
                    f"{result.error}"
                )
            executed_upstreams.add(upstream_id)

    # ------------------------------------------------------------------
    # ② Collect input hashes (upstream artifacts are guaranteed to exist)
    # ------------------------------------------------------------------

    def _collect_input_hashes(self, cell_id: str) -> list[str]:
        """Read provenance hashes from upstream artifacts.

        Called *after* ``_materialize_upstreams`` so every upstream
        artifact is populated. Uses per-variable ``artifact_uris`` dict
        when available, falling back to the legacy ``artifact_uri`` field.
        """
        cell = next(
            (c for c in self.session.notebook_state.cells if c.id == cell_id),
            None,
        )
        if cell is None or not cell.upstream_ids:
            return []

        artifact_mgr = self.session.get_artifact_manager()
        hashes: list[str] = []

        for upstream_id in cell.upstream_ids:
            upstream_cell = next(
                (c for c in self.session.notebook_state.cells if c.id == upstream_id),
                None,
            )
            if upstream_cell is None:
                continue

            # Collect URIs: prefer per-variable dict, fall back to single URI
            uris = list(upstream_cell.artifact_uris.values())
            if not uris and upstream_cell.artifact_uri:
                uris = [upstream_cell.artifact_uri]

            for uri in sorted(uris):  # sorted for deterministic ordering
                try:
                    parts = uri.split("/")
                    artifact_id = parts[-1].split("@")[0]
                    version = int(parts[-1].split("@v=")[1])
                    artifact = artifact_mgr.artifact_store.get_artifact(
                        artifact_id, version,
                    )
                    if artifact:
                        hashes.append(artifact.provenance_hash)
                except (IndexError, ValueError):
                    pass

        return hashes

    # ------------------------------------------------------------------
    # ④-a Load input blobs (guaranteed to exist after step ①)
    # ------------------------------------------------------------------

    def _load_input_blobs(
        self, cell_id: str, output_dir: Path,
    ) -> dict[str, dict[str, str]]:
        """Load upstream variable blobs from the artifact store.

        All upstream artifacts are guaranteed to exist because
        ``_materialize_upstreams`` has already run.  This method simply
        reads blobs and writes them to *output_dir* for the harness.
        """
        cell = next(
            (c for c in self.session.notebook_state.cells if c.id == cell_id),
            None,
        )
        if cell is None:
            return {}

        artifact_mgr = self.session.get_artifact_manager()
        notebook_id = self.session.notebook_state.id
        input_specs: dict[str, dict[str, str]] = {}

        for upstream_id in cell.upstream_ids:
            upstream_cell = next(
                (c for c in self.session.notebook_state.cells if c.id == upstream_id),
                None,
            )
            if upstream_cell is None:
                continue

            referenced_vars = [
                v for v in cell.references if v in upstream_cell.defines
            ]

            for var_name in referenced_vars:
                artifact_id = (
                    f"nb_{notebook_id}_cell_{upstream_id}_var_{var_name}"
                )
                try:
                    artifact = artifact_mgr.artifact_store.get_latest_version(
                        artifact_id,
                    )
                    if artifact is None:
                        # Should not happen after _materialize_upstreams,
                        # but guard defensively.
                        logger.error(
                            "Artifact %s still missing after upstream "
                            "materialisation — skipping variable '%s'.",
                            artifact_id,
                            var_name,
                        )
                        continue

                    blob_data = artifact_mgr.load_artifact_data(
                        artifact_id, artifact.version,
                    )

                    # Determine content type.
                    content_type = "pickle/object"
                    if artifact.transform_spec:
                        try:
                            spec = json.loads(artifact.transform_spec)
                            ct = spec.get("params", {}).get("content_type")
                            if ct:
                                content_type = ct
                        except (ValueError, KeyError):
                            pass

                    ext_map = {
                        "arrow/ipc": ".arrow",
                        "json/object": ".json",
                        "pickle/object": ".pickle",
                        "module/import": ".module.json",
                    }
                    ext = ext_map.get(content_type, ".pickle")
                    input_file = output_dir / f"{var_name}{ext}"
                    with open(input_file, "wb") as f:
                        f.write(blob_data)

                    input_specs[var_name] = {
                        "content_type": content_type,
                        "file": f"{var_name}{ext}",
                    }
                    logger.info(
                        "Loaded input %s from artifact store "
                        "(%s@v=%d, %d bytes, %s)",
                        var_name,
                        artifact_id,
                        artifact.version,
                        len(blob_data),
                        content_type,
                    )
                except Exception:
                    logger.exception(
                        "Failed to load input %s from artifact store",
                        var_name,
                    )

        return input_specs

    # ------------------------------------------------------------------
    # ⑤ Store output artifacts
    # ------------------------------------------------------------------

    def _store_outputs(
        self,
        cell_id: str,
        output_dir: Path,
        provenance_hash: str,
        input_hashes: list[str],
        *,
        source_hash: str = "",
        env_hash: str = "",
    ) -> bool:
        """Persist consumed output variables as artifacts.

        Returns True if every consumed variable was stored, False
        otherwise.
        """
        cell = next(
            (c for c in self.session.notebook_state.cells if c.id == cell_id),
            None,
        )
        if cell is None or self.session.dag is None:
            return True

        artifact_mgr = self.session.get_artifact_manager()
        consumed_vars = self.session.dag.consumed_variables.get(cell_id, set())

        try:
            output_files = list(output_dir.iterdir())
        except Exception:
            output_files = []

        logger.info(
            "_store_outputs %s: consumed_vars=%s output_files=%s",
            cell_id,
            consumed_vars,
            [f.name for f in output_files],
        )

        if not consumed_vars:
            return True

        all_stored = True

        for var_name in consumed_vars:
            found = False
            for ext in [".arrow", ".json", ".pickle", ".module.json"]:
                output_file = output_dir / f"{var_name}{ext}"
                if output_file.exists():
                    found = True
                    try:
                        with open(output_file, "rb") as f:
                            blob_data = f.read()

                        content_type_map = {
                            ".arrow": "arrow/ipc",
                            ".json": "json/object",
                            ".pickle": "pickle/object",
                            ".module.json": "module/import",
                        }
                        content_type = content_type_map.get(ext, "pickle/object")

                        var_provenance = hashlib.sha256(
                            f"{provenance_hash}:{var_name}".encode()
                        ).hexdigest()

                        artifact_version = artifact_mgr.store_cell_output(
                            cell_id=cell_id,
                            variable_name=var_name,
                            blob_data=blob_data,
                            content_type=content_type,
                            provenance_hash=var_provenance,
                            input_versions={h: h for h in input_hashes},
                            source_hash=source_hash,
                            env_hash=env_hash,
                        )
                        uri = (
                            f"strata://artifact/{artifact_version.id}"
                            f"@v={artifact_version.version}"
                        )
                        cell.artifact_uris[var_name] = uri
                        cell.artifact_uri = uri  # backward compat
                        logger.info(
                            "Stored output %s for cell %s as %s@v=%d "
                            "(%d bytes, %s)",
                            var_name,
                            cell_id,
                            artifact_version.id,
                            artifact_version.version,
                            len(blob_data),
                            content_type,
                        )
                    except Exception:
                        logger.exception(
                            "Failed to store output %s for cell %s",
                            var_name,
                            cell_id,
                        )
                        all_stored = False
                    break

            if not found:
                logger.warning(
                    "_store_outputs %s: no output file for consumed var %s "
                    "(looked for %s.arrow/.json/.pickle in %s)",
                    cell_id,
                    var_name,
                    var_name,
                    output_dir,
                )
                all_stored = False

        return all_stored

    # ------------------------------------------------------------------
    # Harness helpers
    # ------------------------------------------------------------------

    async def _run_harness(
        self, manifest_path: Path, venv_python: Path, timeout_seconds: float,
    ) -> dict[str, Any]:
        """Run the harness script via uv."""
        cmd = [
            "uv",
            "run",
            "--directory",
            str(self.session.path),
            "python",
            str(self.harness_path),
            str(manifest_path),
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(self.session.path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
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

        with open(result_path) as f:
            return json.load(f)

    def _parse_result(
        self,
        cell_id: str,
        result: dict,
        duration_ms: float,
        execution_method: str = "cold",
    ) -> CellExecutionResult:
        """Parse harness result into a CellExecutionResult."""
        if not result.get("success", False):
            error_msg = result.get("error", "Unknown error")
            stderr = result.get("stderr", "")
            suggest = _detect_missing_module(error_msg, stderr)
            return CellExecutionResult(
                cell_id=cell_id,
                success=False,
                stdout=result.get("stdout", ""),
                stderr=stderr,
                error=error_msg,
                duration_ms=duration_ms,
                execution_method=execution_method,
                suggest_install=suggest,
            )

        outputs = {}
        variables = result.get("variables", {})
        for var_name, output_meta in variables.items():
            if "error" in output_meta:
                outputs[var_name] = {
                    "content_type": "error",
                    "error": output_meta["error"],
                    "type": output_meta.get("type", "unknown"),
                }
            else:
                outputs[var_name] = output_meta

        mutation_warnings = result.get("mutation_warnings", [])

        return CellExecutionResult(
            cell_id=cell_id,
            success=True,
            stdout=result.get("stdout", ""),
            stderr=result.get("stderr", ""),
            outputs=outputs,
            duration_ms=duration_ms,
            execution_method=execution_method,
            mutation_warnings=mutation_warnings,
        )
