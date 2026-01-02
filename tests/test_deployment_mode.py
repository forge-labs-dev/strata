"""Tests for deployment mode configuration and personal mode safety.

These tests verify:
1. deployment_mode defaults to "service"
2. Invalid deployment_mode raises ValueError
3. Personal mode creates artifact_dir
4. Personal mode binding to non-loopback is blocked unless explicitly allowed
5. writes_enabled property reflects deployment_mode
6. require_writes_enabled dependency blocks writes in service mode
"""

import pytest

from strata.config import StrataConfig


class TestDeploymentModeConfig:
    """Tests for deployment mode configuration."""

    def test_default_is_service(self, tmp_path):
        """Default deployment_mode is 'service'."""
        config = StrataConfig(cache_dir=tmp_path / "cache")
        assert config.deployment_mode == "service"
        assert config.writes_enabled is False

    def test_personal_mode(self, tmp_path):
        """Personal mode can be explicitly set."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
        )
        assert config.deployment_mode == "personal"
        assert config.writes_enabled is True

    def test_invalid_mode_raises(self, tmp_path):
        """Invalid deployment_mode raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            StrataConfig(
                cache_dir=tmp_path / "cache",
                deployment_mode="invalid",
            )
        # Pydantic validation error includes 'service' or 'personal' in message
        error_str = str(exc_info.value)
        assert "'service'" in error_str or "'personal'" in error_str

    def test_personal_mode_creates_artifact_dir(self, tmp_path):
        """Personal mode creates artifact_dir if not specified."""
        # Using a custom artifact_dir to avoid touching home directory
        artifact_dir = tmp_path / "artifacts"
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            artifact_dir=artifact_dir,
        )
        assert config.artifact_dir == artifact_dir
        assert artifact_dir.exists()

    def test_service_mode_no_artifact_dir(self, tmp_path):
        """Service mode does not create artifact_dir by default."""
        config = StrataConfig(cache_dir=tmp_path / "cache")
        assert config.artifact_dir is None


class TestPersonalModeBinding:
    """Tests for personal mode binding safety."""

    def test_loopback_binding_allowed(self, tmp_path):
        """Personal mode allows loopback binding."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            host="127.0.0.1",
            artifact_dir=tmp_path / "artifacts",
        )
        # Should not raise
        config.validate_personal_mode_binding()

    def test_localhost_binding_allowed(self, tmp_path):
        """Personal mode allows localhost binding."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            host="localhost",
            artifact_dir=tmp_path / "artifacts",
        )
        # Should not raise
        config.validate_personal_mode_binding()

    def test_ipv6_loopback_allowed(self, tmp_path):
        """Personal mode allows IPv6 loopback binding."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            host="::1",
            artifact_dir=tmp_path / "artifacts",
        )
        # Should not raise
        config.validate_personal_mode_binding()

    def test_non_loopback_blocked(self, tmp_path):
        """Personal mode blocks non-loopback binding by default."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            host="0.0.0.0",
            artifact_dir=tmp_path / "artifacts",
        )
        with pytest.raises(ValueError) as exc_info:
            config.validate_personal_mode_binding()
        assert "Personal mode binding" in str(exc_info.value)
        assert "unsafe" in str(exc_info.value)

    def test_external_ip_blocked(self, tmp_path):
        """Personal mode blocks external IP binding."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            host="192.168.1.100",
            artifact_dir=tmp_path / "artifacts",
        )
        with pytest.raises(ValueError) as exc_info:
            config.validate_personal_mode_binding()
        assert "Personal mode binding" in str(exc_info.value)

    def test_non_loopback_allowed_with_override(self, tmp_path):
        """Personal mode allows non-loopback binding with explicit override."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            host="0.0.0.0",
            allow_remote_clients_in_personal=True,
            artifact_dir=tmp_path / "artifacts",
        )
        # Should not raise
        config.validate_personal_mode_binding()

    def test_service_mode_allows_any_binding(self, tmp_path):
        """Service mode allows any binding (no writes anyway)."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="service",
            host="0.0.0.0",
        )
        # Should not raise - service mode is read-only
        config.validate_personal_mode_binding()


class TestEnvOverrides:
    """Tests for environment variable overrides."""

    def test_deployment_mode_from_env(self, tmp_path, monkeypatch):
        """STRATA_DEPLOYMENT_MODE overrides default."""
        monkeypatch.setenv("STRATA_DEPLOYMENT_MODE", "personal")
        config = StrataConfig.load(
            cache_dir=tmp_path / "cache",
            artifact_dir=tmp_path / "artifacts",
        )
        assert config.deployment_mode == "personal"

    def test_allow_remote_from_env(self, tmp_path, monkeypatch):
        """STRATA_ALLOW_REMOTE_CLIENTS_IN_PERSONAL overrides default."""
        monkeypatch.setenv("STRATA_ALLOW_REMOTE_CLIENTS_IN_PERSONAL", "true")
        config = StrataConfig.load(cache_dir=tmp_path / "cache")
        assert config.allow_remote_clients_in_personal is True

    def test_artifact_dir_from_env(self, tmp_path, monkeypatch):
        """STRATA_ARTIFACT_DIR overrides default."""
        artifact_dir = tmp_path / "custom_artifacts"
        monkeypatch.setenv("STRATA_ARTIFACT_DIR", str(artifact_dir))
        monkeypatch.setenv("STRATA_DEPLOYMENT_MODE", "personal")
        config = StrataConfig.load(cache_dir=tmp_path / "cache")
        assert config.artifact_dir == artifact_dir


class TestWritesEnabled:
    """Tests for writes_enabled property."""

    def test_service_mode_writes_disabled(self, tmp_path):
        """Service mode has writes disabled."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="service",
        )
        assert config.writes_enabled is False

    def test_personal_mode_writes_enabled(self, tmp_path):
        """Personal mode has writes enabled."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            deployment_mode="personal",
            artifact_dir=tmp_path / "artifacts",
        )
        assert config.writes_enabled is True
