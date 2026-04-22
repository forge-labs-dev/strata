"""Secret-provider protocol + shared return type."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Protocol


class SecretProviderError(RuntimeError):
    """Raised for unrecoverable provider-config errors (unknown provider,
    invalid shape, etc.). Network / auth failures come back through
    ``SecretFetchResult.error`` instead so the caller can surface them
    without breaking the session open."""


@dataclass(frozen=True)
class SecretFetchResult:
    """What a provider returns from a ``fetch`` call.

    ``secrets`` is always a dict — empty on error, populated on
    success. ``error`` carries a user-facing message for the Runtime
    panel; ``fetched_at`` is an ISO-8601 timestamp (UTC) so the UI can
    display "last refreshed N seconds ago".

    A partial fetch (some secrets returned, some missing) is encoded as
    success with a non-empty ``secrets`` and ``error = None``. Providers
    that can't distinguish "partial" from "full success" just return
    whatever they got.
    """

    secrets: dict[str, str] = field(default_factory=dict)
    source: str = ""
    fetched_at: str = ""
    error: str | None = None

    @classmethod
    def failure(cls, source: str, message: str) -> SecretFetchResult:
        return cls(
            secrets={},
            source=source,
            fetched_at=_now_iso(),
            error=message,
        )


def _now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")


class SecretProvider(Protocol):
    """Minimal interface every secret-manager integration implements.

    ``fetch(config)`` is the only hot path — it's called on session open
    and on explicit refresh. Providers must not raise on network /
    auth errors; they return a ``SecretFetchResult`` with the message
    in ``error``. The session loop treats that as "keep running; show
    the error in the UI so the user can fix auth."
    """

    name: str

    def fetch(self, config: dict[str, Any]) -> SecretFetchResult: ...
