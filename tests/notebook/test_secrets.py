"""Tests for secret-manager integration (provider, session merge, route)."""

from __future__ import annotations

import httpx
import pytest

from strata.notebook.models import NotebookState
from strata.notebook.secrets.infisical import InfisicalProvider
from strata.notebook.secrets.provider import SecretFetchResult, SecretProviderError
from strata.notebook.secrets.registry import _reset_for_tests, get_provider
from strata.notebook.secrets.session_integration import (
    MANUAL_SOURCE,
    apply_secrets_to_notebook_state,
    fetch_configured_secrets,
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def setup_method(self) -> None:
        _reset_for_tests()

    def test_infisical_provider_resolves(self) -> None:
        provider = get_provider("infisical")
        assert provider.name == "infisical"

    def test_unknown_provider_raises(self) -> None:
        with pytest.raises(SecretProviderError):
            get_provider("nope")

    def test_instances_are_cached(self) -> None:
        a = get_provider("infisical")
        b = get_provider("infisical")
        assert a is b


# ---------------------------------------------------------------------------
# Infisical provider
# ---------------------------------------------------------------------------


class TestInfisicalProvider:
    def test_missing_token_returns_error(self, monkeypatch) -> None:
        monkeypatch.delenv("INFISICAL_TOKEN", raising=False)
        result = InfisicalProvider().fetch({"project_id": "p"})
        assert result.secrets == {}
        assert result.error is not None
        assert "INFISICAL_TOKEN" in result.error

    def test_missing_project_id_returns_error(self, monkeypatch) -> None:
        monkeypatch.setenv("INFISICAL_TOKEN", "tok")
        monkeypatch.delenv("INFISICAL_PROJECT_ID", raising=False)
        result = InfisicalProvider().fetch({})
        assert result.secrets == {}
        assert "project_id" in (result.error or "")

    def test_parses_secrets_from_api_response(self, monkeypatch) -> None:
        monkeypatch.setenv("INFISICAL_TOKEN", "tok")

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v3/secrets/raw"
            assert dict(request.url.params) == {
                "workspaceId": "proj",
                "environment": "prod",
                "secretPath": "/trading",
            }
            assert request.headers["Authorization"] == "Bearer tok"
            return httpx.Response(
                200,
                json={
                    "secrets": [
                        {"secretKey": "ALPACA_API_KEY", "secretValue": "AKSECRET"},
                        {"secretKey": "DEBUG", "secretValue": "true"},
                        {"secretKey": "MISSING_VALUE"},  # skipped — no secretValue
                    ]
                },
            )

        transport = httpx.MockTransport(handler)
        original_client = httpx.Client
        monkeypatch.setattr(
            httpx,
            "get",
            lambda *a, **kw: httpx.Client(transport=transport).get(*a, **kw),
        )

        result = InfisicalProvider().fetch(
            {"project_id": "proj", "environment": "prod", "path": "/trading"}
        )
        assert result.error is None
        assert result.secrets == {"ALPACA_API_KEY": "AKSECRET", "DEBUG": "true"}
        assert result.source == "infisical"
        assert result.fetched_at  # stamped
        # Reset the httpx patch via teardown (fixture scope)
        _ = original_client

    def test_401_surfaces_auth_error(self, monkeypatch) -> None:
        monkeypatch.setenv("INFISICAL_TOKEN", "tok")
        transport = httpx.MockTransport(
            lambda r: httpx.Response(401, json={"error": "unauthorized"}),
        )
        monkeypatch.setattr(
            httpx,
            "get",
            lambda *a, **kw: httpx.Client(transport=transport).get(*a, **kw),
        )
        result = InfisicalProvider().fetch({"project_id": "proj"})
        assert result.secrets == {}
        assert "401" in (result.error or "") or "token" in (result.error or "").lower()

    def test_network_error_returns_error(self, monkeypatch) -> None:
        monkeypatch.setenv("INFISICAL_TOKEN", "tok")

        def boom(*args, **kwargs):
            raise httpx.ConnectError("cannot connect")

        monkeypatch.setattr(httpx, "get", boom)
        result = InfisicalProvider().fetch({"project_id": "proj"})
        assert result.secrets == {}
        assert "network" in (result.error or "").lower()


# ---------------------------------------------------------------------------
# Session merge
# ---------------------------------------------------------------------------


def _state(
    *,
    env: dict[str, str] | None = None,
    secrets_config: dict | None = None,
) -> NotebookState:
    return NotebookState(
        id="test",
        env=env or {},
        secrets_config=secrets_config or {},
    )


class TestApplySecretsToNotebookState:
    def test_no_secrets_block_stamps_manual_sources(self) -> None:
        state = _state(env={"DEBUG": "true", "LOG_LEVEL": "info"})
        result = apply_secrets_to_notebook_state(state)
        assert result is None
        assert state.env == {"DEBUG": "true", "LOG_LEVEL": "info"}
        assert state.env_sources == {"DEBUG": MANUAL_SOURCE, "LOG_LEVEL": MANUAL_SOURCE}
        assert state.env_fetch_error is None

    def test_fetched_secrets_fill_empty_values(self, monkeypatch) -> None:
        # Existing env has the key as a blanked sensitive placeholder —
        # typical state after reload from disk.
        state = _state(
            env={"OPENAI_API_KEY": "", "DEBUG": "true"},
            secrets_config={"provider": "infisical", "project_id": "p"},
        )
        _install_fake_provider(
            monkeypatch,
            secrets={"OPENAI_API_KEY": "sk-real", "NEW_KEY": "added"},
        )
        result = apply_secrets_to_notebook_state(state)
        assert result is not None and result.error is None
        assert state.env["OPENAI_API_KEY"] == "sk-real"
        assert state.env["DEBUG"] == "true"
        assert state.env["NEW_KEY"] == "added"
        assert state.env_sources["OPENAI_API_KEY"] == "infisical"
        assert state.env_sources["DEBUG"] == MANUAL_SOURCE
        assert state.env_sources["NEW_KEY"] == "infisical"

    def test_manual_override_wins_over_fetched(self, monkeypatch) -> None:
        state = _state(
            env={"OPENAI_API_KEY": "session-override"},
            secrets_config={"provider": "infisical", "project_id": "p"},
        )
        _install_fake_provider(monkeypatch, secrets={"OPENAI_API_KEY": "from-infisical"})
        apply_secrets_to_notebook_state(state)
        assert state.env["OPENAI_API_KEY"] == "session-override"
        assert state.env_sources["OPENAI_API_KEY"] == MANUAL_SOURCE

    def test_fetch_error_surfaces_on_state(self, monkeypatch) -> None:
        state = _state(
            env={"DEBUG": "true"},
            secrets_config={"provider": "infisical", "project_id": "p"},
        )
        _install_fake_provider(monkeypatch, error="Infisical rejected the token")
        result = apply_secrets_to_notebook_state(state)
        assert result is not None
        assert state.env_fetch_error == "Infisical rejected the token"
        # Existing env is untouched on failure.
        assert state.env == {"DEBUG": "true"}

    def test_unknown_provider_name_surfaces_as_error(self) -> None:
        state = _state(
            env={},
            secrets_config={"provider": "vault"},
        )
        result = apply_secrets_to_notebook_state(state)
        assert result is not None
        assert "vault" in (result.error or "").lower() or "unknown" in (result.error or "").lower()
        assert state.env_fetch_error == result.error

    def test_missing_provider_field_is_flagged(self) -> None:
        state = _state(secrets_config={"project_id": "p"})
        result = fetch_configured_secrets(state)
        assert result is not None
        assert "provider" in (result.error or "").lower()


def _install_fake_provider(
    monkeypatch,
    *,
    secrets: dict[str, str] | None = None,
    error: str | None = None,
) -> None:
    """Swap the Infisical provider with a canned result."""
    from strata.notebook.secrets import registry

    class _Fake:
        name = "infisical"

        def fetch(self, config):
            if error is not None:
                return SecretFetchResult.failure("infisical", error)
            return SecretFetchResult(
                secrets=dict(secrets or {}),
                source="infisical",
                fetched_at="2026-04-22T00:00:00Z",
            )

    registry._cache["infisical"] = _Fake()
    monkeypatch.setattr(registry, "_cache", registry._cache)


# ---------------------------------------------------------------------------
# Route surface
# ---------------------------------------------------------------------------


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Open a notebook via the test client so we can hit /secrets/refresh."""
    from fastapi.testclient import TestClient

    from strata.notebook.routes import get_session_manager
    from strata.notebook.writer import add_cell_to_notebook, create_notebook

    # Fresh session manager per test.
    mgr = get_session_manager()
    try:
        nb_dir = create_notebook(tmp_path, "Secrets Route Test")
        add_cell_to_notebook(nb_dir, "c1")

        # Inject a [secrets] block so the refresh path has something to do.
        notebook_toml = nb_dir / "notebook.toml"
        with open(notebook_toml, "a", encoding="utf-8") as f:
            f.write('\n[secrets]\nprovider = "infisical"\nproject_id = "p"\n')

        from tests.notebook.e2e_fixtures import create_test_app

        app = create_test_app()
        tc = TestClient(app)
        resp = tc.post("/v1/notebooks/open", json={"path": str(nb_dir)})
        assert resp.status_code == 200, resp.text
        session_id = resp.json()["session_id"]
        yield tc, session_id, monkeypatch
    finally:
        _reset_for_tests()


class TestRefreshEndpoint:
    def test_refresh_returns_env_sources(self, client) -> None:
        tc, session_id, monkeypatch = client
        _install_fake_provider(monkeypatch, secrets={"ALPACA_API_KEY": "AKROT8"})
        resp = tc.post(f"/v1/notebooks/{session_id}/secrets/refresh")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["env"]["ALPACA_API_KEY"] == "AKROT8"
        assert body["env_sources"]["ALPACA_API_KEY"] == "infisical"
        assert body["env_fetch_error"] is None

    def test_refresh_surfaces_fetch_error(self, client) -> None:
        tc, session_id, monkeypatch = client
        _install_fake_provider(monkeypatch, error="Infisical down")
        resp = tc.post(f"/v1/notebooks/{session_id}/secrets/refresh")
        assert resp.status_code == 200
        body = resp.json()
        assert body["env_fetch_error"] == "Infisical down"

    def test_refresh_unknown_notebook_returns_404(self, client) -> None:
        tc, _session_id, _ = client
        resp = tc.post("/v1/notebooks/does-not-exist/secrets/refresh")
        assert resp.status_code == 404
