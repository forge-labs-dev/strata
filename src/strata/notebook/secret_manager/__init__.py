"""External secret-manager integration for notebook env vars.

A notebook can declare a ``[secret_manager]`` section in ``notebook.toml``
pointing at a provider like Infisical. On session open, the configured
provider is consulted and the returned secrets are merged into the
notebook's env map before cell execution — so an ``OPENAI_API_KEY``
stored in Infisical flows into every cell just like a key typed into
the Runtime panel, but without having to re-enter it each session.

Precedence: values already in the notebook's ``[env]`` (typed
manually in the Runtime panel) win over provider-fetched values, so
a user can always override for a single session.

Current scope: Infisical via service-token auth. The ``SecretProvider``
protocol is deliberately small so adding Vault / AWS Secrets Manager /
Doppler later is a one-file drop-in.
"""

from __future__ import annotations

from strata.notebook.secret_manager.provider import (
    SecretFetchResult,
    SecretProvider,
    SecretProviderError,
)
from strata.notebook.secret_manager.registry import get_provider
from strata.notebook.secret_manager.session_integration import (
    apply_secrets_to_notebook_state,
    fetch_configured_secrets,
)

__all__ = [
    "SecretFetchResult",
    "SecretProvider",
    "SecretProviderError",
    "apply_secrets_to_notebook_state",
    "fetch_configured_secrets",
    "get_provider",
]
