"""Helpers for explicit notebook display-only values."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Markdown:
    """Explicit markdown display wrapper for notebook cells.

    Plain strings remain plain JSON/scalar outputs. Returning ``Markdown(...)``
    opt-ins to the richer ``text/markdown`` display path.
    """

    text: str

    def _repr_markdown_(self) -> str:
        """Return notebook markdown representation."""
        return self.text

    def __str__(self) -> str:
        return self.text
