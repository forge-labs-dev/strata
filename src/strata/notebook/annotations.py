"""Parse cell-level annotations from leading comment blocks.

Annotations are metadata directives in the first contiguous comment block
of a cell.  They control execution routing, mount overrides, timeouts,
environment variables, and loop unrolling.

Supported annotations::

    # @name <display name>        — Human-readable cell name for DAG display
    # @worker <name>              — Route to a named worker backend
    # @timeout <seconds>          — Override execution timeout (per iteration for loops)
    # @mount <name> <uri> [mode]  — Add/override a filesystem mount
    # @env <KEY>=<value>          — Set an environment variable for this cell
    # @loop max_iter=<N> carry=<var> [start_from=<cell>@iter=<k>]
                                  — Mark the cell as a loop; run the body up to N times,
                                    threading `carry` between iterations.
    # @loop_until <expression>    — Optional termination predicate evaluated in the
                                    cell namespace after each iteration.

Annotations do **not** affect the cell's ``defines``/``references`` analysis.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from strata.notebook.models import MountMode, MountSpec


@dataclass
class LoopAnnotation:
    """Parsed ``@loop`` / ``@loop_until`` directives for a loop cell.

    Attributes:
        max_iter: Safety bound on the iteration count.
        carry: Name of the variable threaded between iterations. On iter 0 it is
            read from upstream cells (or ``start_from``); on iter k>0 it is
            rebound from iter k-1's output artifact before the body runs.
        until_expr: Optional Python expression evaluated in the cell namespace
            after each iteration. Truthy result terminates the loop early.
        start_from_cell: Optional cell id whose existing iteration artifact
            seeds iter 0's carry. ``None`` means seed from upstream as usual.
        start_from_iter: Iteration index paired with ``start_from_cell``.
    """

    max_iter: int
    carry: str
    until_expr: str | None = None
    start_from_cell: str | None = None
    start_from_iter: int | None = None


# Pattern for annotation lines: # @<key> <rest>
_ANNOTATION_RE = re.compile(r"^#\s*@(\w+)\s*(.*?)\s*$")


@dataclass
class CellAnnotations:
    """Parsed annotations from a cell's leading comment block."""

    worker: str | None = None
    timeout: float | None = None
    mounts: list[MountSpec] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)

    # Prompt cell annotations
    name: str | None = None
    model: str | None = None
    temperature: float | None = None
    output_type: str | None = None
    max_tokens: int | None = None
    system_prompt: str | None = None

    # Loop cell annotations
    loop: LoopAnnotation | None = None


def parse_annotations(source: str) -> CellAnnotations:
    """Extract annotations from the leading comment block of a cell.

    Only the first contiguous block of ``#``-prefixed lines is scanned.
    Once a non-comment, non-blank line is encountered, parsing stops.

    Returns:
        CellAnnotations with all parsed directives.
    """
    result = CellAnnotations()

    for line in source.splitlines():
        stripped = line.strip()

        # Skip blank lines within the comment block
        if not stripped:
            continue

        # Stop at the first non-comment line
        if not stripped.startswith("#"):
            break

        match = _ANNOTATION_RE.match(stripped)
        if not match:
            continue

        key = match.group(1).lower()
        value = match.group(2).strip()

        if key == "worker":
            result.worker = value or None

        elif key == "timeout":
            try:
                result.timeout = float(value)
            except ValueError:
                pass  # Silently ignore malformed timeout

        elif key == "mount":
            mount = _parse_mount_annotation(value)
            if mount is not None:
                result.mounts.append(mount)

        elif key == "env":
            eq_idx = value.find("=")
            if eq_idx > 0:
                env_key = value[:eq_idx].strip()
                env_val = value[eq_idx + 1 :].strip()
                result.env[env_key] = env_val

        elif key == "name":
            if value:
                result.name = value

        elif key == "model":
            result.model = value or None

        elif key == "temperature":
            try:
                result.temperature = float(value)
            except ValueError:
                pass

        elif key == "output":
            result.output_type = value or None

        elif key == "max_tokens":
            try:
                result.max_tokens = int(value)
            except ValueError:
                pass

        elif key == "system":
            result.system_prompt = value or None

        elif key == "loop":
            _merge_loop_annotation(result, value)

        elif key == "loop_until":
            if value:
                if result.loop is None:
                    result.loop = LoopAnnotation(max_iter=0, carry="", until_expr=value)
                else:
                    result.loop.until_expr = value

    return result


_LOOP_START_FROM_RE = re.compile(r"^(?P<cell>[^@]+)@iter=(?P<iter>-?\d+)$")


def _merge_loop_annotation(result: CellAnnotations, value: str) -> None:
    """Merge ``@loop key=value key=value ...`` into ``result.loop``.

    Multiple ``@loop`` lines accumulate into the same ``LoopAnnotation``;
    later lines override earlier ones for any key they set.
    """
    if result.loop is None:
        result.loop = LoopAnnotation(max_iter=0, carry="")

    loop = result.loop
    for token in value.split():
        if "=" not in token:
            continue
        k, _, v = token.partition("=")
        k = k.strip()
        v = v.strip()
        if not k or not v:
            continue

        if k == "max_iter":
            try:
                loop.max_iter = int(v)
            except ValueError:
                continue
        elif k == "carry":
            loop.carry = v
        elif k == "until":
            loop.until_expr = v
        elif k == "start_from":
            match = _LOOP_START_FROM_RE.match(v)
            if match is not None:
                loop.start_from_cell = match.group("cell").strip()
                try:
                    loop.start_from_iter = int(match.group("iter"))
                except ValueError:
                    loop.start_from_cell = None
                    loop.start_from_iter = None


def _parse_mount_annotation(value: str) -> MountSpec | None:
    """Parse a ``@mount`` annotation value.

    Format: ``<name> <uri> [ro|rw]``

    Examples::

        @mount raw_data s3://bucket/prefix ro
        @mount scratch file:///tmp/work rw
        @mount data s3://bucket/data          # defaults to ro
    """
    parts = value.split()
    if len(parts) < 2:
        return None

    name = parts[0]
    uri = parts[1]
    mode = MountMode.READ_ONLY

    if len(parts) >= 3 and parts[2] in ("ro", "rw"):
        mode = MountMode(parts[2])

    # Validate name is a valid Python identifier
    if not name.isidentifier():
        return None

    return MountSpec(name=name, uri=uri, mode=mode)
