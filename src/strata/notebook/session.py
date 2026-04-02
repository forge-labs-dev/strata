"""Session management for open notebooks."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import subprocess
import time as _time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from strata.notebook.analyzer import analyze_cell
from strata.notebook.annotations import parse_annotations
from strata.notebook.causality import CausalityChain, compute_causality_on_staleness
from strata.notebook.dag import CellAnalysisWithId, NotebookDag, build_dag
from strata.notebook.dependencies import DependencyChangeResult, list_dependencies
from strata.notebook.env import compute_execution_env_hash, compute_lockfile_hash
from strata.notebook.models import (
    CellStaleness,
    CellStatus,
    NotebookState,
)
from strata.notebook.mounts import MountFingerprinter, resolve_cell_mounts
from strata.notebook.parser import parse_notebook
from strata.notebook.provenance import compute_provenance_hash, compute_source_hash
from strata.notebook.workers import (
    build_worker_catalog,
    resolve_worker_spec,
    worker_runtime_identity,
    worker_supports_notebook_execution,
)
from strata.notebook.writer import _uv_sync, update_environment_metadata

if TYPE_CHECKING:
    from strata.notebook.artifact_integration import NotebookArtifactManager
    from strata.notebook.pool import WarmProcessPool

logger = logging.getLogger(__name__)


@dataclass
class ExecutionSample:
    """One execution timing sample for profiling and estimates."""

    duration_ms: float
    cache_hit: bool


@dataclass
class DependencyMutationOutcome:
    """Result of a notebook dependency mutation."""

    result: DependencyChangeResult
    staleness_map: dict[str, CellStaleness]


class NotebookSession:
    """Holds state for one open notebook.

    Attributes:
        id: Session ID
        notebook_state: Current notebook state
        path: Path to notebook directory
        venv_python: Path to python executable in notebook venv
        dag: The computed DAG for the notebook
        artifact_manager: NotebookArtifactManager for this notebook (M4)
    """

    def __init__(self, notebook_state: NotebookState, path: Path):
        """Initialize a notebook session.

        Args:
            notebook_state: NotebookState from parser
            path: Path to notebook directory
        """
        self.id: str = str(uuid.uuid4())
        self.notebook_state = notebook_state
        self.path = Path(path)
        self.venv_python: Path | None = None
        self.dag: NotebookDag | None = None

        # M4: Initialize artifact manager
        from strata.notebook.artifact_integration import NotebookArtifactManager

        self.artifact_manager = NotebookArtifactManager(
            notebook_id=notebook_state.id,
            artifact_dir=path / ".strata" / "artifacts",
        )

        # M6: Initialize warm process pool (optional)
        self.warm_pool: WarmProcessPool | None = None

        # Session TTL tracking
        self.last_accessed: float = _time.time()

        # v1.1: Execution history for profiling and duration estimates.
        self.execution_history: dict[str, list[ExecutionSample]] = {}

        # v1.1: Causality chains for stale cells
        self.causality_map: dict[str, CausalityChain] = {}

        # Environment/runtime sync state for the current notebook venv.
        self.environment_sync_state: str = "unknown"
        self.environment_sync_error: str | None = None
        self.environment_last_synced_at: int | None = None
        self.environment_python_version: str = ""

        # Analyze all cells and build DAG
        self._analyze_and_build_dag()

    def touch(self) -> None:
        """Record recent activity for TTL accounting."""
        self.last_accessed = _time.time()

    def reload(self) -> None:
        """Reload notebook state from disk."""
        previous_cells = {cell.id: cell.model_copy(deep=True) for cell in self.notebook_state.cells}
        previous_runtime_identities = {
            cell.id: self._effective_worker_runtime_identity(cell)
            for cell in self.notebook_state.cells
        }
        self.notebook_state = parse_notebook(self.path)
        # Re-analyze all cells and rebuild DAG
        self._analyze_and_build_dag()
        self.compute_staleness()
        self._restore_ready_runtime_state(previous_cells, previous_runtime_identities)

    def _analyze_and_build_dag(self) -> None:
        """Analyze all cells and build the DAG.

        Updates notebook_state with defines/references/upstream/downstream/isLeaf.
        """
        # Analyze each cell
        cell_analyses = []
        for cell in self.notebook_state.cells:
            analysis = analyze_cell(cell.source)
            cell_analyses.append(
                CellAnalysisWithId(
                    id=cell.id,
                    defines=analysis.defines,
                    references=analysis.references,
                )
            )
            # Update cell with analysis results
            cell.defines = analysis.defines
            cell.references = analysis.references

        # Build DAG
        try:
            self.dag = build_dag(cell_analyses)

            # Update cells with DAG information
            for cell in self.notebook_state.cells:
                cell.upstream_ids = self.dag.cell_upstream.get(cell.id, [])
                cell.downstream_ids = self.dag.cell_downstream.get(cell.id, [])
                cell.is_leaf = cell.id in self.dag.leaves

        except ValueError as e:
            # Cycle detected — log but don't crash
            logger.warning("Cycle detected in DAG: %s", e)
            self.dag = None

    def _restore_ready_runtime_state(
        self,
        previous_cells: dict[str, Any],
        previous_runtime_identities: dict[str, str | None],
    ) -> None:
        """Preserve ready leaf-like runtime state across metadata-only reloads."""
        for cell in self.notebook_state.cells:
            previous = previous_cells.get(cell.id)
            can_restore_ready_state = (
                previous is not None
                and previous.source == cell.source
                and previous.status == CellStatus.READY
                and cell.status == CellStatus.IDLE
                and previous.worker == cell.worker
                and previous.worker_override == cell.worker_override
                and previous.env == cell.env
                and previous.env_overrides == cell.env_overrides
                and previous.upstream_ids == cell.upstream_ids
                and previous.downstream_ids == cell.downstream_ids
                and previous.mounts == cell.mounts
                and previous.is_leaf == cell.is_leaf
                and previous_runtime_identities.get(cell.id)
                == self._effective_worker_runtime_identity(cell)
            )
            if not can_restore_ready_state:
                continue

            assert previous is not None
            cell.status = CellStatus.READY
            cell.staleness = CellStaleness(status=CellStatus.READY, reasons=[])
            cell.artifact_uri = previous.artifact_uri
            cell.artifact_uris = dict(previous.artifact_uris)
            cell.cache_hit = previous.cache_hit
            cell.execution_method = previous.execution_method
            cell.remote_worker = previous.remote_worker
            cell.remote_transport = previous.remote_transport
            cell.remote_build_id = previous.remote_build_id
            cell.remote_build_state = previous.remote_build_state
            cell.remote_error_code = previous.remote_error_code
            cell.last_provenance_hash = previous.last_provenance_hash
            cell.last_source_hash = previous.last_source_hash
            cell.last_env_hash = previous.last_env_hash
            self.causality_map.pop(cell.id, None)

    def re_analyze_cell(self, cell_id: str) -> None:
        """Re-analyze a single cell and rebuild the DAG.

        Args:
            cell_id: ID of the cell to re-analyze
        """
        # Find the cell
        cell = next((c for c in self.notebook_state.cells if c.id == cell_id), None)
        if not cell:
            return

        # Re-analyze just this cell
        analysis = analyze_cell(cell.source)
        cell.defines = analysis.defines
        cell.references = analysis.references

        # Rebuild full DAG (since one cell changed, downstream may be affected)
        self._analyze_and_build_dag()

    def get_artifact_manager(self) -> NotebookArtifactManager:
        """Get the artifact manager for this session.

        Returns:
            NotebookArtifactManager instance
        """
        return self.artifact_manager

    def compute_staleness(self) -> dict[str, CellStaleness]:
        """Compute staleness status for all cells.

        Walk cells in topological order and check if cached artifacts
        match the current provenance hash. Updates cell.staleness.
        Also computes causality chains for stale cells (v1.1).

        Returns:
            Dict mapping cell_id -> CellStaleness
        """
        staleness_map: dict[str, CellStaleness] = {}
        stale_cells: set[str] = set()  # Track stale cells for propagation
        if self.dag is None:
            # No DAG — all cells are idle
            for cell in self.notebook_state.cells:
                staleness_map[cell.id] = CellStaleness(status=CellStatus.IDLE)
            self._apply_staleness_map(staleness_map)
            self.causality_map = {}
            return staleness_map

        # Walk cells in topological order
        for cell_id in self.dag.topological_order:
            cell = next(
                (c for c in self.notebook_state.cells if c.id == cell_id), None
            )
            if cell is None:
                continue

            # If ANY upstream cell is stale, this cell is also stale —
            # its inputs will change once the upstream re-runs, so its
            # cached artifact (based on old inputs) is invalid.
            has_stale_upstream = any(
                uid in stale_cells for uid in cell.upstream_ids
            )

            if has_stale_upstream:
                staleness_map[cell_id] = CellStaleness(status=CellStatus.IDLE, reasons=[])
                stale_cells.add(cell_id)
                continue

            effective_worker = self._effective_worker_name(cell)
            worker_spec = resolve_worker_spec(
                self.notebook_state,
                effective_worker,
            )
            if not worker_supports_notebook_execution(worker_spec):
                staleness_map[cell_id] = CellStaleness(status=CellStatus.IDLE, reasons=[])
                stale_cells.add(cell_id)
                continue

            # Compute current provenance hash
            source_hash = compute_source_hash(cell.source)
            runtime_env = self._collect_runtime_env(cell)
            env_hash = compute_execution_env_hash(
                self.path,
                runtime_env,
                runtime_identity=self._effective_worker_runtime_identity(cell),
            )

            # Get input hashes from upstream artifacts. Use the same
            # per-variable artifact selection as execution, not the legacy
            # single artifact_uri field.
            input_hashes = self._collect_input_hashes(cell_id)
            mount_fingerprints, has_rw_mount = self._collect_mount_fingerprints(cell)

            if has_rw_mount:
                staleness_map[cell_id] = CellStaleness(status=CellStatus.IDLE, reasons=[])
                stale_cells.add(cell_id)
                continue

            provenance_hash = compute_provenance_hash(
                input_hashes + mount_fingerprints, source_hash, env_hash
            )

            # Check if cached artifact exists.
            # The executor stores per-variable provenance hashes:
            #   sha256(f"{provenance_hash}:{var_name}")
            # so we must check with the same scheme.
            cached_outputs = self._resolve_cached_outputs(cell_id, provenance_hash)

            if cached_outputs is None:
                can_preserve_uncached_ready = (
                    cell.is_leaf
                    and cell.status == CellStatus.READY
                    and cell.last_provenance_hash == provenance_hash
                )
                if can_preserve_uncached_ready:
                    staleness_map[cell_id] = CellStaleness(status=CellStatus.READY, reasons=[])
                else:
                    # No cached artifact — cell is stale/idle unless we can
                    # prove it still matches the last successful uncached run.
                    staleness_map[cell_id] = CellStaleness(status=CellStatus.IDLE, reasons=[])
                    stale_cells.add(cell_id)
            else:
                # Artifact exists — mark as ready
                staleness_map[cell_id] = CellStaleness(status=CellStatus.READY, reasons=[])
                # Populate per-variable artifact URIs
                for var_name, (artifact_id, version) in cached_outputs.items():
                    uri = f"strata://artifact/{artifact_id}@v={version}"
                    cell.artifact_uris[var_name] = uri
                    cell.artifact_uri = uri  # backward compat

        self._apply_staleness_map(staleness_map)

        # v1.1: Compute causality chains for stale cells
        self.causality_map = compute_causality_on_staleness(self)

        return staleness_map

    def _apply_staleness_map(
        self, staleness_map: dict[str, CellStaleness]
    ) -> None:
        """Persist computed staleness back onto in-memory cell state."""
        for cell in self.notebook_state.cells:
            staleness = staleness_map.get(cell.id)
            if staleness is None:
                continue
            cell.staleness = staleness
            cell.status = staleness.status
            if staleness.status != CellStatus.READY:
                cell.cache_hit = False

    def mark_executed_ready(self, cell_id: str) -> None:
        """Preserve a just-executed cell as ready in backend state.

        Some cells, especially leaves, are intentionally not cacheable via the
        canonical artifact path. They should still appear as successfully run
        immediately after execution, even though a later staleness recompute
        may otherwise classify them as idle.
        """
        cell = next((c for c in self.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            return

        cell.staleness = CellStaleness(status=CellStatus.READY, reasons=[])
        cell.status = CellStatus.READY
        self.causality_map.pop(cell_id, None)

    def apply_execution_result_metadata(self, cell_id: str, result: Any) -> None:
        """Persist transient execution metadata onto the session cell state."""
        cell = next((c for c in self.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            return

        cell.execution_method = result.execution_method

        if (
            result.remote_worker
            or result.remote_transport
            or result.remote_build_id
            or result.remote_build_state
            or result.remote_error_code
        ):
            cell.remote_worker = result.remote_worker
            cell.remote_transport = result.remote_transport
            if result.execution_method == "cached":
                if result.remote_build_id is not None:
                    cell.remote_build_id = result.remote_build_id
                if result.remote_build_state is not None:
                    cell.remote_build_state = result.remote_build_state
                if result.remote_error_code is not None:
                    cell.remote_error_code = result.remote_error_code
            else:
                cell.remote_build_id = result.remote_build_id
                cell.remote_build_state = result.remote_build_state
                cell.remote_error_code = result.remote_error_code
            return

        if result.execution_method != "cached":
            cell.remote_worker = None
            cell.remote_transport = None
            cell.remote_build_id = None
            cell.remote_build_state = None
            cell.remote_error_code = None

    def record_successful_execution_provenance(
        self,
        cell_id: str,
        provenance_hash: str,
        source_hash: str,
        env_hash: str,
    ) -> None:
        """Persist the last successful execution provenance for uncached cells."""
        cell = next((c for c in self.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            return
        cell.last_provenance_hash = provenance_hash
        cell.last_source_hash = source_hash
        cell.last_env_hash = env_hash

    def serialize_cell(self, cell: Any) -> dict[str, Any]:
        """Serialize a cell with causality and flattened staleness reasons."""
        data = cell.model_dump()
        data["staleness_reasons"] = (
            [reason.value for reason in cell.staleness.reasons]
            if cell.staleness and cell.staleness.reasons
            else []
        )
        annotations = parse_annotations(cell.source)
        data["annotations"] = {
            "worker": annotations.worker,
            "timeout": annotations.timeout,
            "env": annotations.env,
            "mounts": [mount.model_dump() for mount in annotations.mounts],
        }
        causality = self.causality_map.get(cell.id)
        if causality is not None:
            data["causality"] = causality.to_dict()
        return data

    def serialize_cells(self) -> list[dict[str, Any]]:
        """Serialize all cells with runtime-derived metadata."""
        return [self.serialize_cell(cell) for cell in self.notebook_state.cells]

    def serialize_notebook_state(self) -> dict[str, Any]:
        """Serialize notebook state with enriched cell metadata."""
        data = self.notebook_state.model_dump()
        data["cells"] = self.serialize_cells()
        data["environment"] = self.serialize_environment_state()
        return data

    def _probe_python_version(self, python_executable: Path) -> str:
        """Return ``major.minor.micro`` for a Python interpreter when available."""
        try:
            result = subprocess.run(
                [
                    str(python_executable),
                    "-c",
                    (
                        "import sys; "
                        "print("
                        "f'{sys.version_info.major}."
                        "{sys.version_info.minor}."
                        "{sys.version_info.micro}'"
                        ")"
                    ),
                ],
                cwd=str(self.path),
                capture_output=True,
                check=True,
                text=True,
                timeout=10,
            )
        except Exception:
            return ""

        return result.stdout.strip()

    def _resolved_package_count(self) -> int:
        """Count resolved packages from ``uv.lock`` when present."""
        lockfile = self.path / "uv.lock"
        if not lockfile.exists():
            return 0

        try:
            import tomllib

            with open(lockfile, "rb") as f:
                data = tomllib.load(f)
        except Exception:
            logger.debug("Failed to parse uv.lock for %s", self.path, exc_info=True)
            return 0

        packages = data.get("package", [])
        return len(packages) if isinstance(packages, list) else 0

    def serialize_environment_state(self) -> dict[str, Any]:
        """Serialize the live notebook environment state for the UI."""
        dependencies = list_dependencies(self.path)
        return {
            "python_version": self.environment_python_version,
            "lockfile_hash": compute_lockfile_hash(self.path),
            "package_count": len(dependencies),
            "declared_package_count": len(dependencies),
            "resolved_package_count": self._resolved_package_count(),
            "sync_state": self.environment_sync_state,
            "sync_error": self.environment_sync_error,
            "last_synced_at": self.environment_last_synced_at,
            "has_lockfile": (self.path / "uv.lock").exists(),
            "venv_python": str(self.venv_python) if self.venv_python else None,
        }

    def serialize_worker_catalog(self) -> list[dict[str, Any]]:
        """Serialize the worker catalog visible to this notebook."""
        return build_worker_catalog(self.notebook_state)

    def _resolve_cached_outputs(
        self, cell_id: str, provenance_hash: str
    ) -> dict[str, tuple[str, int]] | None:
        """Return canonical output artifacts matching current provenance.

        The cache lookup is valid only if every consumed variable for this cell
        has a canonical artifact in this notebook whose provenance matches the
        per-variable hash used by the executor.
        """
        consumed_vars = (
            self.dag.consumed_variables.get(cell_id, set())
            if self.dag
            else set()
        )
        if consumed_vars:
            first_var = sorted(consumed_vars)[0]
            lookup_hash = hashlib.sha256(
                f"{provenance_hash}:{first_var}".encode()
            ).hexdigest()
        else:
            lookup_hash = provenance_hash

        cached = self.artifact_manager.find_cached(lookup_hash)
        if cached is None:
            return None

        if not consumed_vars:
            return {}

        notebook_id = self.notebook_state.id
        cached_outputs: dict[str, tuple[str, int]] = {}
        for var_name in sorted(consumed_vars):
            canonical_id = (
                f"nb_{notebook_id}_cell_{cell_id}_var_{var_name}"
            )
            expected_hash = hashlib.sha256(
                f"{provenance_hash}:{var_name}".encode()
            ).hexdigest()
            canonical = self.artifact_manager.artifact_store.get_latest_version(
                canonical_id,
            )
            if canonical is None or canonical.provenance_hash != expected_hash:
                return None
            cached_outputs[var_name] = (canonical.id, canonical.version)

        return cached_outputs

    def _collect_input_hashes(self, cell_id: str) -> list[str]:
        """Read provenance hashes from upstream artifacts for staleness checks."""
        cell = next(
            (c for c in self.notebook_state.cells if c.id == cell_id),
            None,
        )
        if cell is None or not cell.upstream_ids:
            return []

        hashes: list[str] = []
        for upstream_id in cell.upstream_ids:
            upstream_cell = next(
                (c for c in self.notebook_state.cells if c.id == upstream_id),
                None,
            )
            if upstream_cell is None:
                continue

            uris = list(upstream_cell.artifact_uris.values())
            if not uris and upstream_cell.artifact_uri:
                uris = [upstream_cell.artifact_uri]

            for uri in sorted(uris):
                try:
                    parts = uri.split("/")
                    artifact_id = parts[-1].split("@")[0]
                    version = int(parts[-1].split("@v=")[1])
                    artifact = self.artifact_manager.artifact_store.get_artifact(
                        artifact_id, version
                    )
                    if artifact:
                        hashes.append(artifact.provenance_hash)
                except (IndexError, ValueError):
                    pass

        return hashes

    def _collect_mount_fingerprints(self, cell: Any) -> tuple[list[str], bool]:
        """Return deterministic mount provenance components for a cell.

        Cell mounts already include notebook defaults from parser.py. Source
        annotations can override them again at execution time, so staleness
        must merge both layers exactly like the executor does.
        """
        annotations = parse_annotations(cell.source)
        merged_mounts = resolve_cell_mounts([], cell.mounts, annotations.mounts)

        mount_fingerprints: list[str] = []
        has_rw_mount = False
        for mount in sorted(merged_mounts, key=lambda m: m.name):
            fingerprint = MountFingerprinter.fingerprint_mount_sync(mount)
            if fingerprint is None:
                has_rw_mount = True
            else:
                mount_fingerprints.append(f"{mount.name}:{fingerprint}")

        return mount_fingerprints, has_rw_mount

    def _collect_runtime_env(self, cell: Any) -> dict[str, str]:
        """Return the effective runtime env for a cell with annotation precedence."""
        annotations = parse_annotations(cell.source)
        runtime_env = dict(cell.env)
        runtime_env.update(annotations.env)
        return runtime_env

    def _effective_worker_name(self, cell: Any) -> str | None:
        """Return the effective worker name with annotation precedence."""
        annotations = parse_annotations(cell.source)
        if annotations.worker:
            return annotations.worker
        if cell.worker:
            return cell.worker
        return self.notebook_state.worker

    def _effective_worker_runtime_identity(self, cell: Any) -> str | None:
        """Return the worker runtime identity used in provenance."""
        return worker_runtime_identity(
            self.notebook_state,
            self._effective_worker_name(cell),
        )

    def record_execution(
        self, cell_id: str, duration_ms: float, cache_hit: bool
    ) -> None:
        """Record a cell execution for profiling (v1.1).

        Args:
            cell_id: ID of the executed cell
            duration_ms: Execution duration in milliseconds
            cache_hit: Whether this was a cache hit
        """
        if cell_id not in self.execution_history:
            self.execution_history[cell_id] = []
        self.execution_history[cell_id].append(
            ExecutionSample(duration_ms=duration_ms, cache_hit=cache_hit)
        )

    def get_estimated_duration(self, cell_id: str) -> int:
        """Get estimated execution duration based on history.

        Args:
            cell_id: Cell ID

        Returns:
            Estimated duration in ms, or 0 if no history
        """
        history = self.execution_history.get(cell_id, [])
        for sample in reversed(history):
            if not sample.cache_hit:
                return int(sample.duration_ms)
        return 0

    def get_profiling_summary(self) -> dict:
        """Get notebook-level profiling summary (v1.1).

        Returns:
            Dict with total execution time, cache savings, artifact sizes,
            and per-cell profiling data.
        """
        total_execution_ms = 0
        cache_hits = 0
        cache_misses = 0
        total_artifact_bytes = 0

        cell_profiles = []
        for cell in self.notebook_state.cells:
            history = self.execution_history.get(cell.id, [])
            last_duration = history[-1].duration_ms if history else 0
            is_cached = history[-1].cache_hit if history else cell.cache_hit

            total_execution_ms += int(
                sum(sample.duration_ms for sample in history)
            )
            cache_hits += sum(1 for sample in history if sample.cache_hit)
            cache_misses += sum(1 for sample in history if not sample.cache_hit)

            cell_name = cell.defines[0] if cell.defines else cell.id
            cell_profiles.append({
                "cell_id": cell.id,
                "cell_name": cell_name,
                "status": cell.status,
                "duration_ms": int(last_duration),
                "cache_hit": is_cached,
                "artifact_uri": cell.artifact_uri,
                "execution_count": len(history),
            })

        # Estimate cache savings: sum of historical durations for cached cells
        cache_savings_ms = 0
        for cell in self.notebook_state.cells:
            history = self.execution_history.get(cell.id, [])
            last_non_cached_duration: int | None = None
            for sample in history:
                if sample.cache_hit:
                    if last_non_cached_duration is not None:
                        cache_savings_ms += last_non_cached_duration
                else:
                    last_non_cached_duration = int(sample.duration_ms)

        return {
            "total_execution_ms": int(total_execution_ms),
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "cache_savings_ms": cache_savings_ms,
            "total_artifact_bytes": total_artifact_bytes,
            "cell_profiles": cell_profiles,
        }

    def ensure_venv_synced(self) -> None:
        """Ensure venv is set up by running ``uv sync``.

        Idempotent — typically <1 s when venv already exists.
        On failure the session still opens (venv_python falls back to
        ``python`` in PATH) so tests without ``uv`` keep working.
        """
        ok = _uv_sync(self.path)
        self.environment_last_synced_at = int(_time.time() * 1000)

        # Locate python inside the venv created by uv
        venv_python = self.path / ".venv" / "bin" / "python"
        if ok and venv_python.exists():
            self.venv_python = venv_python
            self.environment_sync_state = "ready"
            self.environment_sync_error = None
            self.environment_python_version = self._probe_python_version(venv_python)
        else:
            self.venv_python = Path("python")
            if ok:
                self.environment_sync_state = "fallback"
                self.environment_sync_error = (
                    "uv sync succeeded but the notebook venv interpreter was not "
                    "found; using python from PATH."
                )
                self.environment_python_version = self._probe_python_version(self.venv_python)
                logger.warning(
                    "uv sync succeeded but .venv/bin/python not found in %s",
                    self.path,
                )
            else:
                self.environment_sync_state = "failed"
                self.environment_sync_error = (
                    "uv sync failed or uv is unavailable; notebook execution will "
                    "fall back to python from PATH."
                )
                self.environment_python_version = self._probe_python_version(self.venv_python)

    async def _invalidate_warm_pool_for_environment_change(self) -> None:
        """Invalidate the warm pool after the runtime environment changes."""
        if self.warm_pool is None:
            return
        try:
            self.warm_pool.python_executable = str(
                self.venv_python or Path("python")
            )
            await self.warm_pool.invalidate()
            logger.info("Warm pool invalidated after environment change")
        except Exception:
            logger.exception("Failed to invalidate warm pool")

    async def sync_environment(self) -> dict[str, CellStaleness]:
        """Re-sync the notebook environment and refresh runtime metadata."""
        old_hash = compute_lockfile_hash(self.path)
        await asyncio.to_thread(self.ensure_venv_synced)
        await self._invalidate_warm_pool_for_environment_change()

        try:
            await asyncio.to_thread(update_environment_metadata, self.path)
        except Exception:
            logger.exception("Failed to update environment metadata")

        new_hash = compute_lockfile_hash(self.path)
        if new_hash != old_hash:
            return self.compute_staleness()
        return {}

    async def on_dependencies_changed(self) -> None:
        """React to dependency changes (lockfile updated).

        Re-syncs venv, invalidates warm pool, and recomputes lockfile hash
        for provenance.  Called after ``uv add`` / ``uv remove``.
        """
        # 1. Re-sync venv so venv_python is up-to-date and invalidate warm pool.
        await asyncio.to_thread(self.ensure_venv_synced)
        await self._invalidate_warm_pool_for_environment_change()

        # 2. Recompute lockfile hash (triggers cache invalidation on next exec)
        new_hash = compute_lockfile_hash(self.path)
        logger.info(
            "Lockfile hash updated to %.12s after dependency change", new_hash
        )

        # 3. Persist environment metadata in notebook.toml
        try:
            await asyncio.to_thread(update_environment_metadata, self.path)
        except Exception:
            logger.exception("Failed to update environment metadata")

    async def mutate_dependency(
        self, package: str, *, action: str
    ) -> DependencyMutationOutcome:
        """Apply a dependency mutation without blocking the event loop."""
        from strata.notebook.dependencies import add_dependency, remove_dependency

        if action == "add":
            op = add_dependency
        elif action == "remove":
            op = remove_dependency
        else:
            raise ValueError(f"Unknown dependency action: {action}")

        result = await asyncio.to_thread(op, self.path, package)

        staleness_map: dict[str, CellStaleness] = {}
        if getattr(result, "success", False) and getattr(result, "lockfile_changed", False):
            await self.on_dependencies_changed()
            staleness_map = self.compute_staleness()

        return DependencyMutationOutcome(
            result=result,
            staleness_map=staleness_map,
        )


class SessionManager:
    """Manages multiple open notebooks by ID.

    Sessions are evicted after ``SESSION_TTL_SECONDS`` of inactivity
    or when ``MAX_SESSIONS`` is exceeded (oldest evicted first).
    """

    MAX_SESSIONS = 50
    SESSION_TTL_SECONDS = 4 * 3600  # 4 hours

    def __init__(self):
        """Initialize session manager."""
        self._sessions: dict[str, NotebookSession] = {}

    def open_notebook(self, directory: Path) -> NotebookSession:
        """Open a notebook directory.

        Args:
            directory: Path to notebook directory

        Returns:
            NotebookSession for the opened notebook
        """
        notebook_state = parse_notebook(Path(directory))
        session = NotebookSession(notebook_state, Path(directory))

        # Ensure venv is synced (idempotent, typically <1s)
        try:
            session.ensure_venv_synced()
        except Exception as e:
            # Log warning but don't fail — notebook can still be opened,
            # it just won't be able to execute cells
            logger.warning("Failed to sync venv: %s", e)

        # M6: Initialize and start warm process pool
        try:
            from strata.notebook.pool import WarmProcessPool
            session.warm_pool = WarmProcessPool(
                notebook_dir=Path(directory),
                pool_size=2,
                python_executable=session.venv_python or Path("python"),
            )
            # Start pool in background (don't block on notebook open)
            import asyncio
            try:
                task = asyncio.get_running_loop().create_task(session.warm_pool.start())
                session.warm_pool.track_background_task(task)
            except RuntimeError:
                pass  # No running loop; pool stays cold until first acquire
        except Exception as e:
            logger.warning("Failed to initialize warm pool: %s", e)

        session.compute_staleness()

        self._evict_stale()
        self._sessions[session.id] = session
        return session

    def get_session(self, session_id: str) -> NotebookSession | None:
        """Get a session by ID, updating its last-accessed timestamp.

        Args:
            session_id: Session ID

        Returns:
            NotebookSession or None if not found
        """
        session = self._sessions.get(session_id)
        if session is not None:
            session.touch()
        return session

    def _has_active_websocket(self, session_id: str) -> bool:
        """Return whether a notebook session currently has connected sockets."""
        try:
            from strata.notebook.ws import _notebook_connections
        except Exception:
            return False
        return bool(_notebook_connections.get(session_id))

    def _evict_stale(self) -> None:
        """Remove sessions not accessed within TTL and enforce max count."""
        now = _time.time()
        stale = [
            sid
            for sid, s in self._sessions.items()
            if (
                not self._has_active_websocket(sid)
                and now - s.last_accessed > self.SESSION_TTL_SECONDS
            )
        ]
        for sid in stale:
            logger.info("Evicting stale session %s", sid)
            self.close_session(sid)

        # Enforce max count — evict oldest if over limit
        while len(self._sessions) >= self.MAX_SESSIONS:
            evictable = [
                sid for sid in self._sessions
                if not self._has_active_websocket(sid)
            ]
            if not evictable:
                logger.warning(
                    "Session limit exceeded (%d) but all sessions have active websockets",
                    len(self._sessions),
                )
                break
            oldest_id = min(
                evictable, key=lambda sid: self._sessions[sid].last_accessed
            )
            logger.info("Evicting oldest session %s (max %d reached)", oldest_id, self.MAX_SESSIONS)
            self.close_session(oldest_id)

    def close_session(self, session_id: str) -> None:
        """Close a session and release resources.

        Args:
            session_id: Session ID
        """
        session = self._sessions.pop(session_id, None)
        if session is None:
            return
        # Drain warm pool if present
        if session.warm_pool is not None:
            import asyncio

            try:
                asyncio.get_running_loop().create_task(session.warm_pool.drain())
            except RuntimeError:
                session.warm_pool.shutdown_nowait()

    def list_sessions(self) -> list[str]:
        """List all open session IDs.

        Returns:
            List of session IDs
        """
        return list(self._sessions.keys())
