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
    - STRATA_TRACING_ENABLED: Enable/disable OpenTelemetry tracing
    - OTEL_EXPORTER_OTLP_ENDPOINT: OpenTelemetry OTLP endpoint
    - OTEL_SERVICE_NAME: Service name for tracing (default: strata)
    - STRATA_FETCH_PARALLELISM: Max concurrent row group fetches per scan
    - STRATA_MAX_FETCH_WORKERS: Max threads in fetch pool (32-64 recommended)
    - STRATA_ARROW_MEMORY_POOL: PyArrow memory pool (default, system, jemalloc, mimalloc)
    - STRATA_INTERACTIVE_SLOTS: QoS slots for interactive/dashboard queries
    - STRATA_BULK_SLOTS: QoS slots for bulk/ETL queries
    - STRATA_PER_CLIENT_INTERACTIVE: Max concurrent interactive queries per client
    - STRATA_PER_CLIENT_BULK: Max concurrent bulk queries per client
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

    if fetch_parallelism := os.environ.get("STRATA_FETCH_PARALLELISM"):
        overrides["fetch_parallelism"] = int(fetch_parallelism)

    if max_fetch_workers := os.environ.get("STRATA_MAX_FETCH_WORKERS"):
        overrides["max_fetch_workers"] = int(max_fetch_workers)

    if arrow_memory_pool := os.environ.get("STRATA_ARROW_MEMORY_POOL"):
        overrides["arrow_memory_pool"] = arrow_memory_pool

    # QoS slot configuration
    if interactive_slots := os.environ.get("STRATA_INTERACTIVE_SLOTS"):
        overrides["interactive_slots"] = int(interactive_slots)

    if bulk_slots := os.environ.get("STRATA_BULK_SLOTS"):
        overrides["bulk_slots"] = int(bulk_slots)

    # Per-client fairness caps
    if per_client_interactive := os.environ.get("STRATA_PER_CLIENT_INTERACTIVE"):
        overrides["per_client_interactive"] = int(per_client_interactive)

    if per_client_bulk := os.environ.get("STRATA_PER_CLIENT_BULK"):
        overrides["per_client_bulk"] = int(per_client_bulk)

    # Adaptive concurrency control
    if os.environ.get("STRATA_ADAPTIVE_ENABLED", "").lower() == "true":
        overrides["adaptive_enabled"] = True

    if adaptive_interval := os.environ.get("STRATA_ADAPTIVE_INTERVAL"):
        overrides["adaptive_interval_seconds"] = float(adaptive_interval)

    if adaptive_target := os.environ.get("STRATA_ADAPTIVE_TARGET_P95_MS"):
        overrides["adaptive_target_p95_ms"] = float(adaptive_target)

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
        fetch_parallelism = 4  # Max concurrent row group fetches
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
    fetch_parallelism: int = 4  # Max concurrent row group fetches per scan
    # Thread pool for fetch operations - caps total I/O concurrency
    # Sized for typical 8-16 core box; increase for high-core-count servers
    max_fetch_workers: int = 32  # Max threads in fetch pool (32-64 recommended)

    # Catalog settings (for pyiceberg)
    catalog_name: str = "default"
    catalog_properties: dict[str, str] = field(default_factory=dict)

    # Resource limits (backpressure)
    max_concurrent_scans: int = 100  # Max scans executing at once
    max_tasks_per_scan: int = 1000  # Max row groups per scan (prevents OOM)
    plan_timeout_seconds: float = 30.0  # 30 second timeout for planning
    scan_timeout_seconds: float = 300.0  # 5 minute timeout per scan
    max_response_bytes: int = 512 * 1024 * 1024  # 512 MB (streaming keeps memory O(row group))

    # QoS: Two-tier admission control
    # Interactive queries (dashboards) get dedicated slots to avoid bulk query starvation
    # Defaults tuned for typical 8-16 core box supporting bursts of many users
    interactive_slots: int = 32  # Slots for small/fast queries (dashboard bursts)
    bulk_slots: int = 8  # Slots for large/slow queries (ETL workloads)
    # Classification thresholds: query is "interactive" if BOTH conditions met
    interactive_max_bytes: int = 10 * 1024 * 1024  # 10 MB estimated response
    interactive_max_columns: int = 10  # Max columns for interactive
    # Queue timeouts: how long to wait for a slot before returning 429
    # These provide predictable UX: "start within N seconds or get clean retry signal"
    interactive_queue_timeout: float = 10.0  # 10s - dashboards should wait and succeed
    bulk_queue_timeout: float = 30.0  # 30s - bulk queries are patient, queue properly
    # Per-client fairness: prevent one client from monopolizing capacity
    # Set to 0 to disable per-client caps (only use global slots)
    per_client_interactive: int = 2  # Max concurrent interactive queries per client
    per_client_bulk: int = 1  # Max concurrent bulk queries per client

    # Metadata database (for catalog metadata persistence)
    metadata_db: Path | None = None  # Defaults to ~/.strata/meta.sqlite

    # S3 settings (for s3:// URIs)
    # These are passed to PyArrow's S3FileSystem
    s3_region: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_endpoint_url: str | None = None  # For MinIO, LocalStack, etc.
    s3_anonymous: bool = False  # Use anonymous access (public buckets)

    # Memory pool settings (for tuning GC behavior)
    # Options: "default", "system", "jemalloc", "mimalloc"
    # - default: Use PyArrow's default pool (usually mimalloc if available)
    # - system: Use system malloc (useful for debugging with valgrind)
    # - jemalloc: Better fragmentation handling (if available)
    # - mimalloc: Lower latency allocation (if available)
    arrow_memory_pool: str | None = None  # None means don't change the default

    # Rate limiting settings
    rate_limit_enabled: bool = True
    rate_limit_global_rps: float = 1000.0  # Global requests per second
    rate_limit_global_burst: float = 100.0  # Max burst above rate
    rate_limit_client_rps: float = 100.0  # Per-client requests per second
    rate_limit_client_burst: float = 20.0  # Per-client burst
    rate_limit_scan_rps: float = 50.0  # Scan endpoint requests per second
    rate_limit_warm_rps: float = 10.0  # Warm endpoint requests per second

    # S3 timeout settings (passed to PyArrow S3FileSystem)
    s3_connect_timeout_seconds: float = 10.0  # Connection timeout
    s3_request_timeout_seconds: float = 30.0  # Per-request timeout

    # Fetch timeout settings
    fetch_timeout_seconds: float = 60.0  # Timeout for fetching a single row group

    # Adaptive concurrency control (Netflix-style)
    # Dynamically adjusts slot counts based on p95 latency and queue pressure.
    # Disabled by default - enable for production workloads with variable load.
    adaptive_enabled: bool = False  # Enable adaptive concurrency control
    adaptive_interval_seconds: float = 5.0  # How often to check and adjust
    adaptive_target_p95_ms: float = 500.0  # Target p95 latency in milliseconds
    adaptive_min_interactive: int = 4  # Minimum interactive slots (floor)
    adaptive_max_interactive: int = 64  # Maximum interactive slots (ceiling)
    adaptive_min_bulk: int = 2  # Minimum bulk slots (floor)
    adaptive_max_bulk: int = 32  # Maximum bulk slots (ceiling)
    adaptive_hysteresis: int = 3  # Consecutive signals needed before adjustment

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

    def get_timeout_config(self) -> dict:
        """Get all timeout-related configuration as a dictionary.

        Returns:
            Dictionary with all timeout settings organized by category.
        """
        return {
            "planning": {
                "plan_timeout_seconds": self.plan_timeout_seconds,
            },
            "scanning": {
                "scan_timeout_seconds": self.scan_timeout_seconds,
            },
            "qos_queue": {
                "interactive_queue_timeout": self.interactive_queue_timeout,
                "bulk_queue_timeout": self.bulk_queue_timeout,
            },
            "fetching": {
                "fetch_timeout_seconds": self.fetch_timeout_seconds,
            },
            "s3": {
                "s3_connect_timeout_seconds": self.s3_connect_timeout_seconds,
                "s3_request_timeout_seconds": self.s3_request_timeout_seconds,
            },
        }

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

        # Apply timeout settings
        kwargs["connect_timeout"] = self.s3_connect_timeout_seconds
        kwargs["request_timeout"] = self.s3_request_timeout_seconds

        return pafs.S3FileSystem(**kwargs)

    def configure_arrow_memory_pool(self) -> str | None:
        """Configure PyArrow's global memory pool based on settings.

        This affects all PyArrow allocations in the process. Should be called
        once at server startup before any Arrow operations.

        Returns:
            The name of the configured pool, or None if no change was made.

        Raises:
            ValueError: If the specified pool is not available.
        """
        import pyarrow as pa

        if self.arrow_memory_pool is None:
            return None

        pool_name = self.arrow_memory_pool.lower()

        if pool_name == "default":
            # Use PyArrow's default (no change needed)
            return pa.default_memory_pool().backend_name

        if pool_name == "system":
            pa.set_memory_pool(pa.system_memory_pool())
            return "system"

        if pool_name == "jemalloc":
            try:
                pool = pa.jemalloc_memory_pool()
                pa.set_memory_pool(pool)
                return "jemalloc"
            except Exception as e:
                raise ValueError(f"jemalloc memory pool not available: {e}") from e

        if pool_name == "mimalloc":
            try:
                pool = pa.mimalloc_memory_pool()
                pa.set_memory_pool(pool)
                return "mimalloc"
            except Exception as e:
                raise ValueError(f"mimalloc memory pool not available: {e}") from e

        raise ValueError(
            f"Unknown memory pool: {self.arrow_memory_pool}. "
            f"Options: default, system, jemalloc, mimalloc"
        )
