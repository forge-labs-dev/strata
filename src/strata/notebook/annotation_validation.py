"""Cross-reference validation for cell source annotations.

Runs on notebook open, reload, and after WS source flush — never
during active typing.  Diagnostics are advisory and never block
execution.
"""

from __future__ import annotations

import ast
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
    if cell.language == "prompt":
        return _validate_prompt_cell_annotations(cell)
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

    diagnostics.extend(_validate_loop_annotation(cell, annotations, notebook_state))

    return diagnostics


def _validate_prompt_cell_annotations(cell: CellState) -> list[AnnotationDiagnostic]:
    """Surface prompt-cell annotation errors (e.g. malformed ``@output_schema``).

    Called only for ``language == "prompt"`` cells. Python-cell validators
    (worker/mount/timeout/env/loop) don't apply here.
    """
    from strata.notebook.prompt_analyzer import analyze_prompt_cell

    analysis = analyze_prompt_cell(cell.source)
    diagnostics: list[AnnotationDiagnostic] = []
    if analysis.output_schema_error:
        diagnostics.append(
            AnnotationDiagnostic(
                severity="warn",
                code="prompt_output_schema_invalid",
                message=analysis.output_schema_error,
                line=_find_annotation_line(cell.source, "output_schema"),
            )
        )
    return diagnostics


def _validate_loop_annotation(
    cell: CellState,
    annotations,
    notebook_state: NotebookState,
) -> list[AnnotationDiagnostic]:
    """Validate ``@loop`` / ``@loop_until`` directives."""
    diagnostics: list[AnnotationDiagnostic] = []
    loop = annotations.loop
    if loop is None:
        return diagnostics

    loop_line = _find_annotation_line(cell.source, "loop")
    until_line = _find_annotation_line(cell.source, "loop_until") or loop_line

    if loop.max_iter <= 0:
        diagnostics.append(
            AnnotationDiagnostic(
                severity="error",
                code="loop_missing_max_iter",
                message=(
                    "`@loop` requires a positive `max_iter=<N>`. "
                    "The loop cell must declare a safety bound on the iteration count."
                ),
                line=loop_line,
            )
        )

    if not loop.carry:
        diagnostics.append(
            AnnotationDiagnostic(
                severity="error",
                code="loop_missing_carry",
                message=(
                    "`@loop` requires `carry=<variable>`. "
                    "The carry variable is threaded between iterations."
                ),
                line=loop_line,
            )
        )
    elif cell.defines and loop.carry not in cell.defines:
        diagnostics.append(
            AnnotationDiagnostic(
                severity="warn",
                code="loop_carry_unknown",
                message=(
                    f"`@loop carry={loop.carry}` does not match any top-level "
                    f"assignment in the cell. The cell must rebind "
                    f"`{loop.carry}` each iteration for the loop to make progress."
                ),
                line=loop_line,
            )
        )

    if loop.until_expr:
        try:
            ast.parse(loop.until_expr, mode="eval")
        except SyntaxError as exc:
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="error",
                    code="loop_until_syntax_error",
                    message=(
                        f"`@loop_until` expression is not a valid Python expression: {exc.msg}."
                    ),
                    line=until_line,
                )
            )

    if loop.start_from_cell is not None:
        known_cells = {c.id for c in notebook_state.cells}
        if loop.start_from_cell not in known_cells:
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="error",
                    code="loop_start_from_unknown",
                    message=(
                        f"`@loop start_from={loop.start_from_cell}@iter="
                        f"{loop.start_from_iter}` references a cell that does "
                        f"not exist in this notebook."
                    ),
                    line=loop_line,
                )
            )
        elif loop.start_from_cell == cell.id:
            diagnostics.append(
                AnnotationDiagnostic(
                    severity="error",
                    code="loop_start_from_unknown",
                    message=(
                        "`@loop start_from` must reference a different cell — "
                        "a loop cell cannot seed itself from its own iterations."
                    ),
                    line=loop_line,
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
