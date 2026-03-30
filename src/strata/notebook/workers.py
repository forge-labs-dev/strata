"""Notebook worker catalog helpers."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx

from strata.notebook.models import NotebookState, WorkerBackendType, WorkerSpec
from strata.notebook.remote_executor import NOTEBOOK_EXECUTOR_TRANSFORM_REF

_HEALTH_CACHE_TTL_SECONDS = 5.0
_worker_health_cache: dict[str, tuple[float, str]] = {}


@dataclass(frozen=True)
class WorkerPolicy:
    """Effective worker policy for one notebook in the current deployment mode."""

    service_mode: bool
    definitions_editable: bool
    effective_workers: dict[str, WorkerSpec]
    server_workers: dict[str, WorkerSpec]


def get_builtin_local_worker() -> WorkerSpec:
    """Return the implicit built-in local worker."""
    return WorkerSpec(name="local", backend=WorkerBackendType.LOCAL)


def _parse_worker_specs(raw_specs: Any) -> list[WorkerSpec]:
    """Parse worker specs from an untyped config list."""
    if not isinstance(raw_specs, list):
        return []

    parsed: list[WorkerSpec] = []
    for raw_spec in raw_specs:
        if not isinstance(raw_spec, dict):
            continue
        try:
            parsed.append(WorkerSpec.model_validate(raw_spec))
        except Exception:
            continue
    return parsed


def _load_worker_policy(notebook_state: NotebookState) -> WorkerPolicy:
    """Load the effective notebook worker policy for the current deployment."""
    try:
        from strata.server import get_state

        state = get_state()
        config = state.config
        service_mode = config.deployment_mode == "service"
        server_workers = {
            worker.name: worker
            for worker in _parse_worker_specs(
                config.transforms_config.get("notebook_workers", [])
            )
        }
    except Exception:
        service_mode = False
        server_workers = {}

    builtin = get_builtin_local_worker()
    if service_mode:
        effective_workers = {"local": builtin, **server_workers}
    else:
        effective_workers = {
            "local": builtin,
            **{worker.name: worker for worker in notebook_state.workers},
        }

    return WorkerPolicy(
        service_mode=service_mode,
        definitions_editable=not service_mode,
        effective_workers=effective_workers,
        server_workers=server_workers,
    )


def notebook_worker_definitions_editable(notebook_state: NotebookState) -> bool:
    """Return whether notebook-scoped worker definitions can be edited."""
    return _load_worker_policy(notebook_state).definitions_editable


def validate_worker_assignment(
    notebook_state: NotebookState,
    worker_name: str | None,
) -> str | None:
    """Validate a requested worker assignment against deployment policy.

    Returns ``None`` when the assignment is allowed. In personal mode, worker
    assignments remain permissive so notebooks can reference future workers.
    In service mode, assignments must resolve against the server-managed
    worker registry.
    """
    normalized_name = (worker_name or "").strip()
    if not normalized_name or normalized_name == "local":
        return None

    policy = _load_worker_policy(notebook_state)
    if not policy.service_mode:
        return None

    if normalized_name in policy.effective_workers:
        return None

    return (
        f"Worker '{normalized_name}' is not allowed in service mode. "
        "Choose a server-managed worker."
    )


def get_worker_execution_error(
    notebook_state: NotebookState,
    worker_name: str | None,
) -> str | None:
    """Return an execution-facing worker policy error, if any."""
    normalized_name = (worker_name or "").strip() or "local"
    policy_error = validate_worker_assignment(notebook_state, normalized_name)
    if policy_error is not None:
        return f"Execution failed: {policy_error}"
    return None


def resolve_worker_spec(
    notebook_state: NotebookState,
    worker_name: str | None,
) -> WorkerSpec | None:
    """Resolve a worker name against the effective worker policy.

    ``None`` and ``"local"`` map to the implicit built-in local worker.
    In personal mode, notebook-scoped worker definitions are visible here.
    In service mode, only the server-managed registry is visible.
    """
    normalized_name = (worker_name or "").strip()
    if not normalized_name or normalized_name == "local":
        return get_builtin_local_worker()

    return _load_worker_policy(notebook_state).effective_workers.get(normalized_name)


def worker_runtime_identity(
    notebook_state: NotebookState,
    worker_name: str | None,
) -> str | None:
    """Return the runtime identity that should participate in provenance."""
    worker = resolve_worker_spec(notebook_state, worker_name)
    if worker is None:
        return None

    if worker.runtime_id:
        return f"{worker.backend.value}:{worker.runtime_id}"

    if worker.config:
        config_json = json.dumps(worker.config, sort_keys=True, separators=(",", ":"))
        config_hash = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
        return f"{worker.backend.value}:{worker.name}:{config_hash}"

    return f"{worker.backend.value}:{worker.name}"


def is_embedded_executor_worker(worker: WorkerSpec | None) -> bool:
    """Return True when the worker uses the local embedded executor path."""
    if worker is None or worker.backend != WorkerBackendType.EXECUTOR:
        return False
    url = str(worker.config.get("url", "")).strip()
    return url in {"embedded://local", "embedded://notebook"}


def is_http_executor_worker(worker: WorkerSpec | None) -> bool:
    """Return True when the worker points at an HTTP notebook executor."""
    if worker is None or worker.backend != WorkerBackendType.EXECUTOR:
        return False
    url = str(worker.config.get("url", "")).strip()
    scheme = urlparse(url).scheme.lower()
    return scheme in {"http", "https"}


def worker_supports_notebook_execution(worker: WorkerSpec | None) -> bool:
    """Return whether notebook execution can run on this worker today."""
    if worker is None:
        return False
    if worker.backend == WorkerBackendType.LOCAL:
        return True
    return is_embedded_executor_worker(worker) or is_http_executor_worker(worker)


def build_worker_catalog(notebook_state: NotebookState) -> list[dict[str, Any]]:
    """Build a UI-facing worker catalog for a notebook.

    The catalog always includes the built-in ``local`` worker, notebook-scoped
    worker definitions, and synthetic unavailable entries for any referenced
    worker names that no longer exist in the configured registry.
    """
    policy = _load_worker_policy(notebook_state)
    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_worker(
        worker: WorkerSpec,
        *,
        source: str,
        health: str,
        allowed: bool,
    ) -> None:
        if worker.name in seen:
            return
        seen.add(worker.name)
        catalog.append(
            {
                "name": worker.name,
                "backend": worker.backend.value,
                "runtime_id": worker.runtime_id,
                "config": worker.config,
                "source": source,
                "health": health,
                "allowed": allowed,
            }
        )

    add_worker(
        get_builtin_local_worker(),
        source="builtin",
        health="healthy",
        allowed=True,
    )

    if policy.service_mode:
        for worker in policy.server_workers.values():
            health = (
                "healthy"
                if worker.backend == WorkerBackendType.LOCAL
                or is_embedded_executor_worker(worker)
                else "unknown"
            )
            add_worker(worker, source="server", health=health, allowed=True)

        for worker in notebook_state.workers:
            add_worker(
                worker,
                source="notebook",
                health="unavailable",
                allowed=False,
            )
    else:
        for worker in notebook_state.workers:
            health = (
                "healthy"
                if worker.backend == WorkerBackendType.LOCAL
                or is_embedded_executor_worker(worker)
                else "unknown"
            )
            add_worker(worker, source="notebook", health=health, allowed=True)

    referenced = set()
    if notebook_state.worker:
        referenced.add(notebook_state.worker)
    for cell in notebook_state.cells:
        if cell.worker:
            referenced.add(cell.worker)

    for worker_name in sorted(referenced):
        if worker_name in seen:
            continue
        add_worker(
            WorkerSpec(name=worker_name, backend=WorkerBackendType.EXECUTOR),
            source="referenced",
            health="unavailable",
            allowed=False,
        )

    return catalog


def _health_url_for_worker(worker: WorkerSpec) -> str | None:
    """Map a worker config URL to its health endpoint."""
    if not is_http_executor_worker(worker):
        return None

    raw_url = str(worker.config.get("url", "")).strip()
    if not raw_url:
        return None

    parsed = urlparse(raw_url)
    path = parsed.path or ""
    if path.endswith("/v1/execute"):
        path = path[: -len("/v1/execute")] + "/health"
    elif path.endswith("/v1/notebook-execute"):
        path = path[: -len("/v1/notebook-execute")] + "/health"
    elif not path or path == "/":
        path = "/health"
    else:
        path = f"{path.rstrip('/')}/health"

    return urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


async def probe_worker_health(
    worker: WorkerSpec,
    *,
    timeout_seconds: float = 1.5,
    force_refresh: bool = False,
) -> str:
    """Probe a worker health endpoint and return a UI health string."""
    if worker.backend == WorkerBackendType.LOCAL or is_embedded_executor_worker(worker):
        return "healthy"
    if not is_http_executor_worker(worker):
        return "unknown"

    health_url = _health_url_for_worker(worker)
    if not health_url:
        return "unknown"

    now = time.time()
    cached = _worker_health_cache.get(health_url)
    if (
        not force_refresh
        and cached is not None
        and now - cached[0] < _HEALTH_CACHE_TTL_SECONDS
    ):
        return cached[1]

    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.get(health_url)
        if response.status_code != 200:
            health = "unavailable"
        else:
            payload = response.json()
            capabilities = payload.get("capabilities", {})
            transform_refs = capabilities.get("transform_refs", [])
            status = str(payload.get("status", "unknown"))
            if (
                status == "healthy"
                and isinstance(transform_refs, list)
                and NOTEBOOK_EXECUTOR_TRANSFORM_REF in transform_refs
            ):
                health = "healthy"
            elif status == "healthy":
                health = "unknown"
            else:
                health = "unavailable"
    except Exception:
        health = "unavailable"

    _worker_health_cache[health_url] = (now, health)
    return health


async def build_worker_catalog_with_health(
    notebook_state: NotebookState,
    *,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """Build a worker catalog enriched with live health probes where possible."""
    catalog = build_worker_catalog(notebook_state)
    policy = _load_worker_policy(notebook_state)
    worker_by_name = {
        worker.name: worker for worker in policy.effective_workers.values()
    }

    for entry in catalog:
        name = str(entry.get("name", ""))
        if name == "local":
            entry["health"] = "healthy"
            continue
        if entry.get("allowed") is False:
            entry["health"] = "unavailable"
            continue

        worker = worker_by_name.get(name)
        if worker is None:
            continue

        entry["health"] = await probe_worker_health(
            worker,
            force_refresh=force_refresh,
        )

    return catalog
