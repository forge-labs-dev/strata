"""Configuration for Strata with pyproject.toml and environment variable support."""

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from strata.types import CacheGranularity


def _get_env_overrides() -> dict:
    """Get configuration overrides from environment variables.

    Supported environment variables:
    - STRATA_HOST: Server host
    - STRATA_PORT: Server port
    - STRATA_CACHE_DIR: Cache directory path
    - STRATA_MAX_CACHE_SIZE_BYTES: Maximum cache size in bytes
    - AWS_REGION / STRATA_S3_REGION: S3 region
    - AWS_ACCESS_KEY_ID / STRATA_S3_ACCESS_KEY: S3 access key
    - AWS_SECRET_ACCESS_KEY / STRATA_S3_SECRET_KEY: S3 secret key
    - STRATA_S3_ENDPOINT_URL: S3 endpoint URL (for MinIO, LocalStack)
    - STRATA_S3_ANONYMOUS: Use anonymous access (set to "true")
    - STRATA_METADATA_DB: Path to SQLite metadata database
    """
    overrides = {}

    if host := os.environ.get("STRATA_HOST"):
        overrides["host"] = host

    if port := os.environ.get("STRATA_PORT"):
        overrides["port"] = int(port)

    if cache_dir := os.environ.get("STRATA_CACHE_DIR"):
        overrides["cache_dir"] = Path(cache_dir)

    if max_cache := os.environ.get("STRATA_MAX_CACHE_SIZE_BYTES"):
        overrides["max_cache_size_bytes"] = int(max_cache)

    # S3 configuration (prefer STRATA_* but fall back to AWS_* for compatibility)
    if s3_region := os.environ.get("STRATA_S3_REGION") or os.environ.get("AWS_REGION"):
        overrides["s3_region"] = s3_region

    if s3_access_key := os.environ.get("STRATA_S3_ACCESS_KEY") or os.environ.get(
        "AWS_ACCESS_KEY_ID"
    ):
        overrides["s3_access_key"] = s3_access_key

    if s3_secret_key := os.environ.get("STRATA_S3_SECRET_KEY") or os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    ):
        overrides["s3_secret_key"] = s3_secret_key

    if s3_endpoint := os.environ.get("STRATA_S3_ENDPOINT_URL"):
        overrides["s3_endpoint_url"] = s3_endpoint

    if os.environ.get("STRATA_S3_ANONYMOUS", "").lower() == "true":
        overrides["s3_anonymous"] = True

    if metadata_db := os.environ.get("STRATA_METADATA_DB"):
        overrides["metadata_db"] = Path(metadata_db)

    return overrides


def _find_pyproject() -> Path | None:
    """Find pyproject.toml in current or parent directories."""
    current = Path.cwd()
    for parent in [current, *current.parents]:
        candidate = parent / "pyproject.toml"
        if candidate.exists():
            return candidate
    return None


def _load_from_pyproject() -> dict:
    """Load strata configuration from pyproject.toml [tool.strata] section."""
    pyproject_path = _find_pyproject()
    if pyproject_path is None:
        return {}

    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)

    return data.get("tool", {}).get("strata", {})


@dataclass
class StrataConfig:
    """Configuration for Strata server and client.

    Configuration is loaded from pyproject.toml [tool.strata] section,
    with programmatic overrides taking precedence.

    Example pyproject.toml:
        [tool.strata]
        host = "0.0.0.0"
        port = 8765
        cache_dir = "/tmp/strata-cache"
        max_cache_size_bytes = 10737418240
        cache_granularity = "row_group_projection"  # or "row_group"
        batch_size = 65536
        catalog_name = "default"

        # Resource limits (backpressure)
        max_concurrent_scans = 100
        max_tasks_per_scan = 1000
        plan_timeout_seconds = 30.0
        scan_timeout_seconds = 300.0
        max_response_bytes = 536870912  # 512 MB

        # Metadata persistence
        metadata_db = "/var/lib/strata/meta.sqlite"

        # S3 storage backend (optional)
        s3_region = "us-east-1"
        s3_endpoint_url = "http://localhost:9000"  # For MinIO/LocalStack
        s3_anonymous = false  # Set true for public buckets

        [tool.strata.catalog_properties]
        type = "sql"
        uri = "sqlite:///catalog.db"

    Cache granularity options:
        - row_group_projection: Cache per row-group + projection (default, finest)
        - row_group: Cache per row-group only, project on read (coarser, reuses cache)
    """

    # Server settings
    host: str = "127.0.0.1"
    port: int = 8765

    # Cache settings
    cache_dir: Path = field(default_factory=lambda: Path.home() / ".strata" / "cache")
    max_cache_size_bytes: int = 10 * 1024 * 1024 * 1024  # 10 GB default
    cache_granularity: CacheGranularity = CacheGranularity.ROW_GROUP_PROJECTION

    # Fetcher settings
    batch_size: int = 65536  # rows per batch when reading Parquet

    # Catalog settings (for pyiceberg)
    catalog_name: str = "default"
    catalog_properties: dict[str, str] = field(default_factory=dict)

    # Resource limits (backpressure)
    max_concurrent_scans: int = 100  # Max scans executing at once
    max_tasks_per_scan: int = 1000  # Max row groups per scan (prevents OOM)
    plan_timeout_seconds: float = 30.0  # 30 second timeout for planning
    scan_timeout_seconds: float = 300.0  # 5 minute timeout per scan
    max_response_bytes: int = 512 * 1024 * 1024  # 512 MB (streaming keeps memory O(row group))

    # Metadata database (for catalog metadata persistence)
    metadata_db: Path | None = None  # Defaults to ~/.strata/meta.sqlite

    # S3 settings (for s3:// URIs)
    # These are passed to PyArrow's S3FileSystem
    s3_region: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_endpoint_url: str | None = None  # For MinIO, LocalStack, etc.
    s3_anonymous: bool = False  # Use anonymous access (public buckets)

    def __post_init__(self) -> None:
        if isinstance(self.cache_dir, str):
            self.cache_dir = Path(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Set default metadata_db if not specified
        if self.metadata_db is None:
            self.metadata_db = Path.home() / ".strata" / "meta.sqlite"
        elif isinstance(self.metadata_db, str):
            self.metadata_db = Path(self.metadata_db)
        # Ensure parent directory exists
        self.metadata_db.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load(cls, **overrides) -> "StrataConfig":
        """Load configuration with precedence: defaults < pyproject.toml < env vars < overrides.

        Args:
            **overrides: Values that override all other settings

        Returns:
            StrataConfig instance
        """
        file_config = _load_from_pyproject()
        env_config = _get_env_overrides()

        # Merge: defaults < pyproject.toml < env vars < overrides
        merged = {**file_config, **env_config, **overrides}

        # Handle cache_dir path conversion
        if "cache_dir" in merged and isinstance(merged["cache_dir"], str):
            merged["cache_dir"] = Path(merged["cache_dir"])

        return cls(**merged)

    @property
    def server_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def get_s3_filesystem(self):
        """Create a PyArrow S3FileSystem from configuration.

        Returns:
            Configured S3FileSystem for reading Parquet files from S3

        Raises:
            ImportError: If pyarrow.fs is not available
        """
        import pyarrow.fs as pafs

        kwargs = {}

        if self.s3_region:
            kwargs["region"] = self.s3_region

        if self.s3_access_key and self.s3_secret_key:
            kwargs["access_key"] = self.s3_access_key
            kwargs["secret_key"] = self.s3_secret_key

        if self.s3_endpoint_url:
            kwargs["endpoint_override"] = self.s3_endpoint_url

        if self.s3_anonymous:
            kwargs["anonymous"] = True

        return pafs.S3FileSystem(**kwargs)
