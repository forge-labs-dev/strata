"""Notebook worker catalog helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib.parse import urlparse

from strata.notebook.models import NotebookState, WorkerBackendType, WorkerSpec


def get_builtin_local_worker() -> WorkerSpec:
    """Return the implicit built-in local worker."""
    return WorkerSpec(name="local", backend=WorkerBackendType.LOCAL)


def resolve_worker_spec(
    notebook_state: NotebookState,
    worker_name: str | None,
) -> WorkerSpec | None:
    """Resolve a worker name against notebook-scoped definitions.

    ``None`` and ``"local"`` map to the implicit built-in local worker.
    Unknown names return ``None``.
    """
    if not worker_name or worker_name == "local":
        return get_builtin_local_worker()

    for worker in notebook_state.workers:
        if worker.name == worker_name:
            return worker

    return None


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
    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_worker(
        worker: WorkerSpec,
        *,
        source: str,
        health: str,
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
            }
        )

    add_worker(
        get_builtin_local_worker(),
        source="builtin",
        health="healthy",
    )

    for worker in notebook_state.workers:
        health = (
            "healthy"
            if worker.backend == WorkerBackendType.LOCAL
            or is_embedded_executor_worker(worker)
            else "unknown"
        )
        add_worker(worker, source="notebook", health=health)

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
        )

    return catalog
