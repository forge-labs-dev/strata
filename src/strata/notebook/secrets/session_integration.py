"""Glue code between ``SessionManager`` and the secret-provider layer.

Two public helpers:

* :func:`fetch_configured_secrets` — read the ``[secrets]`` config off
  a ``NotebookState``, pick a provider, and return a
  :class:`SecretFetchResult`. Never raises — errors land in
  ``result.error``.

* :func:`apply_secrets_to_notebook_state` — call the above, merge the
  fetched secrets into ``state.env`` (values typed in the Runtime
  panel win), and write ``env_sources`` / ``env_fetch_error`` /
  ``env_fetched_at`` so the UI can label each key's origin.

Keeping the merge policy in one place means the session code never has
to reason about precedence directly — it just calls this on open and
on refresh.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from strata.notebook.secrets.provider import SecretFetchResult
from strata.notebook.secrets.registry import get_provider

if TYPE_CHECKING:
    from strata.notebook.models import NotebookState


MANUAL_SOURCE = "manual"


def fetch_configured_secrets(state: NotebookState) -> SecretFetchResult | None:
    """Return the fetch result for ``state``'s configured provider.

    Returns ``None`` when the notebook has no ``[secrets]`` block at all
    — callers can distinguish "no provider" from "provider errored"
    that way. When a provider is configured but the name is unknown or
    the provider constructor raises, returns a ``SecretFetchResult``
    with an error message so the UI can display it.
    """
    config = state.secrets_config
    if not config:
        return None
    provider_name = str(config.get("provider") or "").strip().lower()
    if not provider_name:
        return SecretFetchResult.failure(
            "",
            "[secrets] block is present but 'provider' is not set — add provider = \"infisical\".",
        )
    try:
        provider = get_provider(provider_name)
    except Exception as exc:  # SecretProviderError or anything weirder
        return SecretFetchResult.failure(provider_name, str(exc))
    try:
        return provider.fetch(dict(config))
    except Exception as exc:
        # Defensive: the protocol says fetch should not raise, but a
        # buggy provider shouldn't take the session down.
        return SecretFetchResult.failure(provider_name, f"provider raised: {exc}")


def apply_secrets_to_notebook_state(state: NotebookState) -> SecretFetchResult | None:
    """Fetch + merge secrets into ``state.env`` in-place.

    Merge policy: fetched secrets populate env where the key isn't
    already present, OR where the existing value is an empty / blanked
    placeholder (sensitive-key blanking writes ``""`` on disk).
    Non-empty values already in ``state.env`` — the ones the user
    actively set via the Runtime panel this session — override the
    provider. This keeps "override for a single session" working.

    Always stamps ``env_sources`` so every key in ``state.env`` has a
    provenance label the UI can render.
    """
    result = fetch_configured_secrets(state)

    state.env_sources = {key: MANUAL_SOURCE for key in state.env}

    if result is None:
        state.env_fetch_error = None
        state.env_fetched_at = None
        return None

    state.env_fetched_at = result.fetched_at
    state.env_fetch_error = result.error

    for key, value in result.secrets.items():
        existing = state.env.get(key)
        if existing is None or existing == "":
            state.env[key] = value
            state.env_sources[key] = result.source
        # else: manual override wins; keep state.env_sources[key] = MANUAL

    return result
