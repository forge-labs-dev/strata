"""Driver adapter registry.

Adapters register themselves at import time; the executor looks them
up by name (matched against ``ConnectionSpec.driver``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from strata.notebook.sql.adapter import DriverAdapter


_REGISTRY: dict[str, "DriverAdapter"] = {}


def register_adapter(adapter: "DriverAdapter") -> None:
    """Register a ``DriverAdapter`` under its ``name``.

    Called at module import time by each driver implementation.
    Re-registration replaces the previous entry; tests that swap
    adapters in and out can rely on this.
    """
    _REGISTRY[adapter.name] = adapter


def get_adapter(name: str) -> "DriverAdapter":
    """Look up the adapter registered for ``name``.

    Raises ``KeyError`` with the known-driver list when ``name`` isn't
    registered — matches how the executor wants to surface
    "unknown driver" to users.
    """
    if name not in _REGISTRY:
        known = ", ".join(sorted(_REGISTRY)) or "(none registered)"
        raise KeyError(
            f"unknown SQL driver: {name!r}. Known drivers: {known}",
        )
    return _REGISTRY[name]


def known_drivers() -> list[str]:
    """List currently registered driver names, sorted."""
    return sorted(_REGISTRY)


def _reset_for_tests() -> None:
    """Drop all registrations. Test-only helper."""
    _REGISTRY.clear()
