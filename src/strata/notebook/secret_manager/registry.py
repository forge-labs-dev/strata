"""Provider registry — string → SecretProvider instance lookup.

Instances are cached per process since providers are stateless aside
from the HTTP client they hold (and they construct that on demand).
Adding a new provider is a one-line change in ``_build``.
"""

from __future__ import annotations

from strata.notebook.secret_manager.provider import SecretProvider, SecretProviderError

_cache: dict[str, SecretProvider] = {}


def get_provider(name: str) -> SecretProvider:
    """Return the provider named ``name``, constructing on first use.

    Raises ``SecretProviderError`` for unknown names so ``notebook.toml``
    typos surface at session open rather than as a silent empty fetch.
    """
    if name in _cache:
        return _cache[name]
    provider = _build(name)
    _cache[name] = provider
    return provider


def _build(name: str) -> SecretProvider:
    if name == "infisical":
        from strata.notebook.secret_manager.infisical import InfisicalProvider

        return InfisicalProvider()
    raise SecretProviderError(f"Unknown secret provider: {name!r}")


def _reset_for_tests() -> None:
    """Test-only hook to clear the cache between tests that install mocks."""
    _cache.clear()
