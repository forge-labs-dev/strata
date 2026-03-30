"""Parse cell-level annotations from leading comment blocks.

Annotations are metadata directives in the first contiguous comment block
of a cell.  They control execution routing, mount overrides, timeouts,
and environment variables.

Supported annotations::

    # @worker <name>              — Route to a named worker backend
    # @timeout <seconds>          — Override execution timeout
    # @mount <name> <uri> [mode]  — Add/override a filesystem mount
    # @env <KEY>=<value>          — Set an environment variable for this cell

Annotations do **not** affect the cell's ``defines``/``references`` analysis.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from strata.notebook.models import MountMode, MountSpec

# Pattern for annotation lines: # @<key> <rest>
_ANNOTATION_RE = re.compile(r"^#\s*@(\w+)\s*(.*?)\s*$")


@dataclass
class CellAnnotations:
    """Parsed annotations from a cell's leading comment block."""

    worker: str | None = None
    timeout: float | None = None
    mounts: list[MountSpec] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)


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

    return result


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
