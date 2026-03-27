"""Session management for open notebooks."""

from __future__ import annotations

import asyncio
import logging
import time as _time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from strata.notebook.analyzer import analyze_cell
from strata.notebook.causality import CausalityChain, compute_causality_on_staleness
from strata.notebook.dag import CellAnalysisWithId, NotebookDag, build_dag
from strata.notebook.env import compute_lockfile_hash
from strata.notebook.models import CellStaleness, CellStatus, NotebookState
from strata.notebook.parser import parse_notebook
from strata.notebook.provenance import compute_provenance_hash, compute_source_hash
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

    result: object
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

        # Analyze all cells and build DAG
        self._analyze_and_build_dag()

    def reload(self) -> None:
        """Reload notebook state from disk."""
        self.notebook_state = parse_notebook(self.path)
        # Re-analyze all cells and rebuild DAG
        self._analyze_and_build_dag()

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
        env_hash = compute_lockfile_hash(self.path)

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

            # Compute current provenance hash
            source_hash = compute_source_hash(cell.source)

            # Get input hashes from upstream artifacts
            input_hashes = []
            for upstream_id in cell.upstream_ids:
                upstream_cell = next(
                    (c for c in self.notebook_state.cells if c.id == upstream_id),
                    None,
                )
                if upstream_cell and upstream_cell.artifact_uri:
                    try:
                        parts = upstream_cell.artifact_uri.split("/")
                        artifact_id = parts[-1].split("@")[0]
                        version = int(parts[-1].split("@v=")[1])
                        artifact = self.artifact_manager.artifact_store.get_artifact(
                            artifact_id, version
                        )
                        if artifact:
                            input_hashes.append(artifact.provenance_hash)
                    except (IndexError, ValueError):
                        pass

            provenance_hash = compute_provenance_hash(
                input_hashes, source_hash, env_hash
            )

            # Check if cached artifact exists.
            # The executor stores per-variable provenance hashes:
            #   sha256(f"{provenance_hash}:{var_name}")
            # so we must check with the same scheme.
            consumed_vars = (
                self.dag.consumed_variables.get(cell_id, set())
                if self.dag
                else set()
            )
            if consumed_vars:
                import hashlib
                first_var = sorted(consumed_vars)[0]
                lookup_hash = hashlib.sha256(
                    f"{provenance_hash}:{first_var}".encode()
                ).hexdigest()
            else:
                lookup_hash = provenance_hash
            cached = self.artifact_manager.find_cached(lookup_hash)

            # Validate: find_by_provenance can return artifacts from old
            # notebook sessions sharing the same SQLite DB.  Verify the
            # canonical artifact for THIS notebook/cell has matching provenance.
            if cached is not None and consumed_vars:
                notebook_id = self.notebook_state.id
                first_var = sorted(consumed_vars)[0]
                canonical_id = (
                    f"nb_{notebook_id}_cell_{cell_id}_var_{first_var}"
                )
                canonical = self.artifact_manager.artifact_store.get_latest_version(
                    canonical_id,
                )
                if canonical is None or canonical.provenance_hash != lookup_hash:
                    cached = None

            if cached is None:
                # No cached artifact — cell is stale
                staleness_map[cell_id] = CellStaleness(status=CellStatus.IDLE, reasons=[])
                stale_cells.add(cell_id)
            else:
                # Artifact exists — mark as ready
                staleness_map[cell_id] = CellStaleness(status=CellStatus.READY, reasons=[])
                # Populate per-variable artifact URIs
                notebook_id = self.notebook_state.id
                for var_name in consumed_vars:
                    canonical_id = (
                        f"nb_{notebook_id}_cell_{cell_id}_var_{var_name}"
                    )
                    canonical = self.artifact_manager.artifact_store.get_latest_version(
                        canonical_id,
                    )
                    if canonical:
                        uri = f"strata://artifact/{canonical.id}@v={canonical.version}"
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
            is_cached = cell.cache_hit

            if is_cached:
                cache_hits += 1
            elif cell.status == "ready":
                cache_misses += 1

            total_execution_ms += last_duration

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
            if cell.cache_hit:
                history = self.execution_history.get(cell.id, [])
                for sample in reversed(history):
                    if not sample.cache_hit:
                        cache_savings_ms += int(sample.duration_ms)
                        break

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

        # Locate python inside the venv created by uv
        venv_python = self.path / ".venv" / "bin" / "python"
        if ok and venv_python.exists():
            self.venv_python = venv_python
        else:
            self.venv_python = Path("python")
            if ok:
                logger.warning(
                    "uv sync succeeded but .venv/bin/python not found in %s",
                    self.path,
                )


    async def on_dependencies_changed(self) -> None:
        """React to dependency changes (lockfile updated).

        Re-syncs venv, invalidates warm pool, and recomputes lockfile hash
        for provenance.  Called after ``uv add`` / ``uv remove``.
        """
        # 1. Re-sync venv so venv_python is up-to-date
        await asyncio.to_thread(self.ensure_venv_synced)

        # 2. Invalidate warm process pool (workers have stale env)
        if self.warm_pool is not None:
            try:
                self.warm_pool.python_executable = str(
                    self.venv_python or Path("python")
                )
                await self.warm_pool.invalidate()
                logger.info("Warm pool invalidated after dependency change")
            except Exception:
                logger.exception("Failed to invalidate warm pool")

        # 3. Recompute lockfile hash (triggers cache invalidation on next exec)
        new_hash = compute_lockfile_hash(self.path)
        logger.info(
            "Lockfile hash updated to %.12s after dependency change", new_hash
        )

        # 4. Persist environment metadata in notebook.toml
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
                asyncio.get_running_loop().create_task(session.warm_pool.start())
            except RuntimeError:
                pass  # No running loop; pool stays cold until first acquire
        except Exception as e:
            logger.warning("Failed to initialize warm pool: %s", e)

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
            session.last_accessed = _time.time()
        return session

    def _evict_stale(self) -> None:
        """Remove sessions not accessed within TTL and enforce max count."""
        now = _time.time()
        stale = [
            sid
            for sid, s in self._sessions.items()
            if now - s.last_accessed > self.SESSION_TTL_SECONDS
        ]
        for sid in stale:
            logger.info("Evicting stale session %s", sid)
            self.close_session(sid)

        # Enforce max count — evict oldest if over limit
        while len(self._sessions) >= self.MAX_SESSIONS:
            oldest_id = min(
                self._sessions, key=lambda sid: self._sessions[sid].last_accessed
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
                pass  # No running loop; best-effort

    def list_sessions(self) -> list[str]:
        """List all open session IDs.

        Returns:
            List of session IDs
        """
        return list(self._sessions.keys())
