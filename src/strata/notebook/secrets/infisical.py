"""Infisical secret-manager integration via service-token auth.

Uses the Infisical REST API (``/api/v3/secrets/raw``) directly — no
SDK dependency. The service token must be set in the process
environment as ``INFISICAL_TOKEN``; everything else (``project_id``,
``environment``, ``path``) comes from the notebook's ``[secrets]``
block so the non-sensitive routing info is version-controlled.

Service tokens are the simplest auth model Infisical offers for
machine access. Users with machine-identity + client-id/secret auth
can still use this integration by minting a service token — we keep
the token handling trivial so the flow is obvious.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

from strata.notebook.secrets.provider import SecretFetchResult, _now_iso

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://app.infisical.com"
_DEFAULT_ENVIRONMENT = "dev"
_DEFAULT_PATH = "/"
_DEFAULT_TIMEOUT = 10.0


class InfisicalProvider:
    """Pulls secrets from an Infisical project via service-token auth."""

    name = "infisical"

    def fetch(self, config: dict[str, Any]) -> SecretFetchResult:
        token = os.environ.get("INFISICAL_TOKEN")
        if not token:
            return SecretFetchResult.failure(
                self.name,
                "INFISICAL_TOKEN not set in the process environment. "
                "Export it in the shell that launched Strata.",
            )

        project_id = config.get("project_id") or os.environ.get("INFISICAL_PROJECT_ID")
        if not project_id:
            return SecretFetchResult.failure(
                self.name,
                "project_id missing — set it in notebook.toml [secrets] or via INFISICAL_PROJECT_ID.",
            )

        environment = (
            config.get("environment")
            or os.environ.get("INFISICAL_ENVIRONMENT")
            or _DEFAULT_ENVIRONMENT
        )
        secret_path = config.get("path") or os.environ.get("INFISICAL_PATH") or _DEFAULT_PATH
        base_url = (config.get("base_url") or _DEFAULT_BASE_URL).rstrip("/")

        try:
            resp = httpx.get(
                f"{base_url}/api/v3/secrets/raw",
                params={
                    "workspaceId": project_id,
                    "environment": environment,
                    "secretPath": secret_path,
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=_DEFAULT_TIMEOUT,
            )
        except httpx.RequestError as exc:
            return SecretFetchResult.failure(
                self.name,
                f"network error contacting Infisical: {exc}",
            )

        if resp.status_code == 401:
            return SecretFetchResult.failure(
                self.name,
                "Infisical rejected the token (401). Check INFISICAL_TOKEN scope / expiry.",
            )
        if resp.status_code == 404:
            return SecretFetchResult.failure(
                self.name,
                "Infisical returned 404 — project_id / environment / path combination "
                "does not resolve to a secrets scope.",
            )
        if resp.status_code >= 400:
            return SecretFetchResult.failure(
                self.name,
                f"Infisical API returned HTTP {resp.status_code}: {resp.text[:200]}",
            )

        try:
            payload = resp.json()
        except ValueError:
            return SecretFetchResult.failure(
                self.name,
                "Infisical response was not JSON — upstream API may have changed.",
            )

        secrets: dict[str, str] = {}
        for entry in payload.get("secrets") or []:
            key = entry.get("secretKey")
            value = entry.get("secretValue")
            if isinstance(key, str) and isinstance(value, str):
                secrets[key] = value

        return SecretFetchResult(
            secrets=secrets,
            source=self.name,
            fetched_at=_now_iso(),
            error=None,
        )
