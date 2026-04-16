"""Cross-reference validation for cell source annotations.

Runs on notebook open and reload only — never during live editing.
Diagnostics are advisory and never block execution.
"""

from __future__ import annotations

from strata.notebook.annotations import parse_annotations
from strata.notebook.models import AnnotationDiagnostic, CellState, NotebookState

_BUILTIN_WORKER_NAMES = frozenset({"local"})


def validate_cell_annotations(
    cell: CellState,
    notebook_state: NotebookState,
) -> list[AnnotationDiagnostic]:
    """Validate a cell's annotations against notebook-wide context."""
    diagnostics: list[AnnotationDiagnostic] = []
    annotations = parse_annotations(cell.source)

    if annotations.worker:
        known = {w.name for w in notebook_state.workers} | _BUILTIN_WORKER_NAMES
        if annotations.worker not in known:
            line = _find_annotation_line(cell.source, "worker")
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="warn",
                    code="worker_unknown",
                    message=(
                        f"`@worker {annotations.worker}` is not declared in this notebook. "
                        "Execution will fail until the worker is added."
                    ),
                    line=line,
                )
            )

    return diagnostics


def _find_annotation_line(source: str, directive: str) -> int | None:
    for i, line in enumerate(source.splitlines(), start=1):
        stripped = line.strip()
        if not stripped.startswith("#"):
            if stripped:
                break
            continue
        if f"@{directive}" in stripped.lower():
            return i
    return None
