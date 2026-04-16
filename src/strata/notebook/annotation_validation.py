"""Cross-reference validation for cell source annotations.

Runs on notebook open, reload, and after WS source flush — never
during active typing.  Diagnostics are advisory and never block
execution.
"""

from __future__ import annotations

import re

from strata.notebook.annotations import parse_annotations
from strata.notebook.models import AnnotationDiagnostic, CellState, NotebookState

_BUILTIN_WORKER_NAMES = frozenset({"local"})
_SUPPORTED_MOUNT_SCHEMES = frozenset({"file", "s3", "gs", "gcs", "az", "azure"})
_ANNOTATION_RE = re.compile(r"^#\s*@(\w+)\s*(.*?)\s*$")


def validate_cell_annotations(
    cell: CellState,
    notebook_state: NotebookState,
) -> list[AnnotationDiagnostic]:
    """Validate a cell's annotations against notebook-wide context."""
    diagnostics: list[AnnotationDiagnostic] = []
    annotations = parse_annotations(cell.source)

    # --- worker_unknown ---
    if annotations.worker:
        known = {w.name for w in notebook_state.workers} | _BUILTIN_WORKER_NAMES
        if annotations.worker not in known:
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="warn",
                    code="worker_unknown",
                    message=(
                        f"`@worker {annotations.worker}` is not declared in this notebook. "
                        "Execution will fail until the worker is added."
                    ),
                    line=_find_annotation_line(cell.source, "worker"),
                )
            )

    # --- mount checks ---
    notebook_mount_names = {m.name for m in notebook_state.mounts}
    for mount in annotations.mounts:
        line = _find_annotation_line(cell.source, "mount", mount.name)

        # mount_uri_unsupported
        scheme = mount.uri.split("://")[0].lower() if "://" in mount.uri else ""
        if not scheme or scheme not in _SUPPORTED_MOUNT_SCHEMES:
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="warn",
                    code="mount_uri_unsupported",
                    message=(
                        f"`@mount {mount.name}` uses unsupported URI scheme "
                        f"'{scheme or mount.uri}'. "
                        f"Supported: {', '.join(sorted(_SUPPORTED_MOUNT_SCHEMES))}."
                    ),
                    line=line,
                )
            )

        # mount_shadows_notebook
        if mount.name in notebook_mount_names:
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="info",
                    code="mount_shadows_notebook",
                    message=(
                        f"`@mount {mount.name}` overrides the notebook-level "
                        f"mount with the same name."
                    ),
                    line=line,
                )
            )

    # --- timeout_not_numeric / env_malformed ---
    # The parser silently swallows these, so we re-scan raw lines.
    for lineno, line_text in _annotation_lines(cell.source):
        match = _ANNOTATION_RE.match(line_text.strip())
        if not match:
            continue
        key = match.group(1).lower()
        value = match.group(2).strip()

        if key == "timeout":
            if not value:
                diagnostics.append(
                    AnnotationDiagnostic(
                        severity="warn",
                        code="timeout_not_numeric",
                        message="`@timeout` requires a numeric value (seconds).",
                        line=lineno,
                    )
                )
            else:
                try:
                    t = float(value)
                    if t <= 0:
                        diagnostics.append(
                            AnnotationDiagnostic(
                                severity="warn",
                                code="timeout_not_numeric",
                                message=f"`@timeout {value}` must be a positive number.",
                                line=lineno,
                            )
                        )
                except ValueError:
                    diagnostics.append(
                        AnnotationDiagnostic(
                            severity="warn",
                            code="timeout_not_numeric",
                            message=f"`@timeout {value}` is not a valid number.",
                            line=lineno,
                        )
                    )

        elif key == "env":
            eq_idx = value.find("=")
            if eq_idx <= 0:
                diagnostics.append(
                    AnnotationDiagnostic(
                        severity="warn",
                        code="env_malformed",
                        message=(
                            f"`@env {value}` is malformed. Expected format: `@env KEY=value`."
                        ),
                        line=lineno,
                    )
                )

    return diagnostics


def _find_annotation_line(source: str, directive: str, needle: str | None = None) -> int | None:
    """Return 1-based line number of the first matching annotation."""
    for lineno, line_text in _annotation_lines(source):
        lowered = line_text.strip().lstrip("#").strip().lower()
        if not lowered.startswith(f"@{directive.lower()}"):
            continue
        if needle is None or needle.lower() in lowered:
            return lineno
    return None


def _annotation_lines(source: str):
    """Yield (1-based lineno, line_text) for the leading comment block."""
    for i, line in enumerate(source.splitlines(), start=1):
        stripped = line.strip()
        if not stripped.startswith("#"):
            if stripped:
                break
            continue
        yield i, line
