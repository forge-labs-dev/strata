"""Configuration for Strata with pyproject.toml and environment variable support."""

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from strata.types import CacheGranularity

# ---------------------------------------------------------------------------
# ACL Configuration Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AclRule:
    """Single ACL rule for access control.

    Rules are matched in order. A rule matches if:
    - Principal matches (or rule principal is "*" for any)
    - Tenant matches (if specified in rule)
    - At least one table pattern matches

    Attributes:
        principal: Principal ID pattern ("*" for any principal)
        tenant: Optional tenant ID (None means any tenant)
        tables: Tuple of table patterns (glob-style, e.g., "file:db.*")
    """

    principal: str = "*"
    tenant: str | None = None
    tables: tuple[str, ...] = ()


@dataclass
class AclConfig:
    """Access control list configuration.

    ACL evaluation order:
    1. Deny rules are checked first - if any match, access is denied
    2. Allow rules are checked - if any match, access is allowed
    3. Default action is applied (allow or deny)

    Attributes:
        default: Default action when no rules match ("allow" or "deny")
        deny_rules: List of deny rules (checked first)
        allow_rules: List of allow rules (checked second)
    """

    default: str = "allow"  # "allow" | "deny"
    deny_rules: list[AclRule] = field(default_factory=list)
    allow_rules: list[AclRule] = field(default_factory=list)


def _get_env_overrides() -> dict:
    """Get configuration overrides from environment variables.

    Supported environment variables:
    - STRATA_HOST: Server host
    - STRATA_PORT: Server port
    - STRATA_CACHE_DIR: Cache directory path
    - STRATA_MAX_CACHE_SIZE_BYTES: Maximum cache size in bytes
    - STRATA_CATALOG_URI: Catalog database URI (PostgreSQL recommended for production)
      Example: postgresql://user:pass@localhost:5432/iceberg_catalog
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

    # Catalog URI (for PostgreSQL or other SQL backends)
    # Example: postgresql://user:pass@localhost:5432/iceberg_catalog
    if catalog_uri := os.environ.get("STRATA_CATALOG_URI"):
        # Merge into catalog_properties
        if "catalog_properties" not in overrides:
            overrides["catalog_properties"] = {}
        overrides["catalog_properties"]["uri"] = catalog_uri

    # Multi-tenancy configuration
    if os.environ.get("STRATA_MULTI_TENANT_ENABLED", "").lower() == "true":
        overrides["multi_tenant_enabled"] = True

    if tenant_header := os.environ.get("STRATA_TENANT_HEADER"):
        overrides["tenant_header"] = tenant_header

    if os.environ.get("STRATA_REQUIRE_TENANT_HEADER", "").lower() == "true":
        overrides["require_tenant_header"] = True

    if default_tenant_interactive := os.environ.get("STRATA_DEFAULT_TENANT_INTERACTIVE_SLOTS"):
        overrides["default_tenant_interactive_slots"] = int(default_tenant_interactive)

    if default_tenant_bulk := os.environ.get("STRATA_DEFAULT_TENANT_BULK_SLOTS"):
        overrides["default_tenant_bulk_slots"] = int(default_tenant_bulk)

    # Trusted proxy authentication settings
    if auth_mode := os.environ.get("STRATA_AUTH_MODE"):
        overrides["auth_mode"] = auth_mode

    if proxy_token := os.environ.get("STRATA_PROXY_TOKEN"):
        overrides["proxy_token"] = proxy_token

    if proxy_token_header := os.environ.get("STRATA_PROXY_TOKEN_HEADER"):
        overrides["proxy_token_header"] = proxy_token_header

    if principal_header := os.environ.get("STRATA_PRINCIPAL_HEADER"):
        overrides["principal_header"] = principal_header

    if scopes_header := os.environ.get("STRATA_SCOPES_HEADER"):
        overrides["scopes_header"] = scopes_header

    if os.environ.get("STRATA_HIDE_FORBIDDEN_AS_NOT_FOUND", "").lower() == "true":
        overrides["hide_forbidden_as_not_found"] = True

    # Deployment mode settings
    if deployment_mode := os.environ.get("STRATA_DEPLOYMENT_MODE"):
        overrides["deployment_mode"] = deployment_mode

    if os.environ.get("STRATA_ALLOW_REMOTE_CLIENTS_IN_PERSONAL", "").lower() == "true":
        overrides["allow_remote_clients_in_personal"] = True

    if artifact_dir := os.environ.get("STRATA_ARTIFACT_DIR"):
        overrides["artifact_dir"] = Path(artifact_dir)

    # Server-mode transforms
    if os.environ.get("STRATA_TRANSFORMS_ENABLED", "").lower() == "true":
        if "transforms_config" not in overrides:
            overrides["transforms_config"] = {}
        overrides["transforms_config"]["enabled"] = True

    # Build QoS configuration
    if build_qos_interactive := os.environ.get("STRATA_BUILD_QOS_INTERACTIVE_SLOTS"):
        overrides["build_qos_interactive_slots"] = int(build_qos_interactive)
    if build_qos_bulk := os.environ.get("STRATA_BUILD_QOS_BULK_SLOTS"):
        overrides["build_qos_bulk_slots"] = int(build_qos_bulk)
    if build_qos_per_tenant_interactive := os.environ.get("STRATA_BUILD_QOS_PER_TENANT_INTERACTIVE"):
        overrides["build_qos_per_tenant_interactive"] = int(build_qos_per_tenant_interactive)
    if build_qos_per_tenant_bulk := os.environ.get("STRATA_BUILD_QOS_PER_TENANT_BULK"):
        overrides["build_qos_per_tenant_bulk"] = int(build_qos_per_tenant_bulk)
    if build_qos_bytes_per_day := os.environ.get("STRATA_BUILD_QOS_BYTES_PER_DAY"):
        overrides["build_qos_bytes_per_day"] = int(build_qos_bytes_per_day)

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


def _parse_acl_config(raw: dict) -> AclConfig:
    """Parse ACL configuration from pyproject.toml [tool.strata.acl] section.

    Expected format:
        [tool.strata.acl]
        default = "deny"

        deny = [
          { principal = "*", tables = ["file:finance.*", "s3:pii.*"] }
        ]

        allow = [
          { principal = "bi-dashboard", tables = ["file:db.*"] },
          { tenant = "data-platform", tables = ["file:analytics.*"] }
        ]

    Args:
        raw: Dictionary from pyproject.toml acl section

    Returns:
        Parsed AclConfig object
    """
    if not raw:
        return AclConfig()

    deny_rules = []
    for rule_dict in raw.get("deny", []):
        deny_rules.append(
            AclRule(
                principal=rule_dict.get("principal", "*"),
                tenant=rule_dict.get("tenant"),
                tables=tuple(rule_dict.get("tables", [])),
            )
        )

    allow_rules = []
    for rule_dict in raw.get("allow", []):
        allow_rules.append(
            AclRule(
                principal=rule_dict.get("principal", "*"),
                tenant=rule_dict.get("tenant"),
                tables=tuple(rule_dict.get("tables", [])),
            )
        )

    return AclConfig(
        default=raw.get("default", "allow"),
        deny_rules=deny_rules,
        allow_rules=allow_rules,
    )


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

    # Multi-tenancy settings
    # When enabled, tenant context is extracted from request headers for isolation
    multi_tenant_enabled: bool = False  # Enable multi-tenancy mode
    tenant_header: str = "X-Tenant-ID"  # HTTP header to extract tenant ID from
    require_tenant_header: bool = False  # If True, reject requests without tenant header
    # Per-tenant default QoS quotas (used when tenant has no custom config)
    default_tenant_interactive_slots: int = 32  # Default interactive slots per tenant
    default_tenant_bulk_slots: int = 8  # Default bulk slots per tenant

    # Trusted proxy authentication settings
    # When auth_mode="trusted_proxy", Strata trusts identity headers from the proxy.
    # The proxy MUST strip client-supplied X-Strata-* headers and inject trusted values.
    auth_mode: str = "none"  # "none" | "trusted_proxy"
    proxy_token_header: str = "X-Strata-Proxy-Token"  # Header containing proxy secret
    proxy_token: str | None = None  # Expected proxy token (from STRATA_PROXY_TOKEN env)
    principal_header: str = "X-Strata-Principal"  # Header containing user/service ID
    scopes_header: str = "X-Strata-Scopes"  # Header containing space-separated scopes
    hide_forbidden_as_not_found: bool = True  # Return 404 instead of 403 for denied access

    # Access control list configuration
    # Loaded from [tool.strata.acl] section in pyproject.toml
    acl_config: AclConfig = field(default_factory=AclConfig)

    # Deployment mode: "service" (shared server) or "personal" (local notebook/dev)
    # - service: Read-only cache server, write endpoints return 403
    # - personal: Enables artifact store, materialize API, uploads
    deployment_mode: str = "service"  # "service" | "personal"
    # Safety: personal mode only binds to loopback by default
    # Set True to allow non-loopback binding (dangerous if not firewalled)
    allow_remote_clients_in_personal: bool = False
    # Artifact storage directory (personal mode only)
    # Defaults to ~/.strata/artifacts if not set
    artifact_dir: Path | None = None

    # Server-mode transforms configuration
    # When enabled, allows materialize in service mode with external executors
    # Transforms config is loaded from [tool.strata.transforms] section
    transforms_config: dict = field(default_factory=dict)

    # Build runner configuration (server-mode transforms)
    build_runner_poll_interval_ms: int = 500  # How often to poll for pending builds
    build_runner_max_concurrent: int = 10  # Global limit on concurrent builds
    build_runner_max_per_tenant: int = 3  # Per-tenant limit on concurrent builds
    build_runner_default_timeout: float = 300.0  # Default build timeout
    build_runner_default_max_output: int = 1024 * 1024 * 1024  # 1 GB default

    # Pull model (Stage 2) configuration
    # When enabled, executors pull inputs and push outputs via signed URLs
    # instead of Strata streaming data through itself (push model)
    pull_model_enabled: bool = False  # Enable pull model for executor execution
    signed_url_expiry_seconds: float = 600.0  # How long signed URLs are valid (10 min)

    # Build QoS configuration (quotas and backpressure for builds)
    # This controls admission at the API layer, rejecting builds before they're
    # created if the system is at capacity. Returns 429 with Retry-After header.
    build_qos_interactive_slots: int = 16  # Max concurrent interactive builds (globally)
    build_qos_bulk_slots: int = 8  # Max concurrent bulk builds (globally)
    build_qos_per_tenant_interactive: int = 4  # Max concurrent interactive builds per tenant
    build_qos_per_tenant_bulk: int = 2  # Max concurrent bulk builds per tenant
    build_qos_interactive_timeout: float = 5.0  # Queue timeout for interactive (seconds)
    build_qos_bulk_timeout: float = 15.0  # Queue timeout for bulk (seconds)
    build_qos_per_tenant_timeout: float = 1.0  # Per-tenant slot wait timeout (seconds)
    # Optional daily byte quota per tenant (None = unlimited)
    build_qos_bytes_per_day: int | None = None
    # Classification thresholds: build is "bulk" if exceeds either threshold
    build_qos_bulk_bytes_threshold: int = 100 * 1024 * 1024  # 100MB output = bulk
    build_qos_bulk_inputs_threshold: int = 5  # >5 inputs = bulk

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

        # Validate deployment_mode
        if self.deployment_mode not in ("service", "personal"):
            raise ValueError(
                f"deployment_mode must be 'service' or 'personal', got '{self.deployment_mode}'"
            )

        # Set default artifact_dir for personal mode
        if self.artifact_dir is None and self.deployment_mode == "personal":
            self.artifact_dir = Path.home() / ".strata" / "artifacts"
        elif isinstance(self.artifact_dir, str):
            self.artifact_dir = Path(self.artifact_dir)

        # Ensure artifact_dir exists in personal mode
        if self.deployment_mode == "personal" and self.artifact_dir is not None:
            self.artifact_dir.mkdir(parents=True, exist_ok=True)

    def validate_personal_mode_binding(self) -> None:
        """Validate that personal mode binding is safe.

        In personal mode, binding to non-loopback addresses exposes the server
        to the network, which is dangerous since personal mode enables writes.

        Raises:
            ValueError: If personal mode binds to non-loopback without explicit allow
        """
        if self.deployment_mode != "personal":
            return

        # Check if host is loopback
        loopback_hosts = {"127.0.0.1", "localhost", "::1"}
        is_loopback = self.host in loopback_hosts

        if not is_loopback and not self.allow_remote_clients_in_personal:
            raise ValueError(
                f"Personal mode binding to '{self.host}' is unsafe. "
                f"Personal mode enables write endpoints (artifacts, uploads). "
                f"Either bind to 127.0.0.1/localhost, or set "
                f"allow_remote_clients_in_personal=True if you have firewall protection."
            )

    @property
    def writes_enabled(self) -> bool:
        """Check if write endpoints are enabled (personal mode only)."""
        return self.deployment_mode == "personal"

    @property
    def server_transforms_enabled(self) -> bool:
        """Check if server-mode transforms are enabled.

        Server-mode transforms allow materialize in service mode with
        external executors. Requires transforms_config with enabled=true.
        """
        return self.deployment_mode == "service" and self.transforms_config.get("enabled", False)

    @property
    def max_transform_output_bytes(self) -> int:
        """Get max transform output size in bytes."""
        return self.build_runner_default_max_output

    def get_build_qos_config(self):
        """Create BuildQoSConfig from Strata configuration.

        Returns:
            BuildQoSConfig instance for initializing BuildQoS.
        """
        from strata.transforms.build_qos import BuildQoSConfig

        return BuildQoSConfig(
            interactive_slots=self.build_qos_interactive_slots,
            bulk_slots=self.build_qos_bulk_slots,
            per_tenant_interactive=self.build_qos_per_tenant_interactive,
            per_tenant_bulk=self.build_qos_per_tenant_bulk,
            interactive_queue_timeout=self.build_qos_interactive_timeout,
            bulk_queue_timeout=self.build_qos_bulk_timeout,
            per_tenant_timeout=self.build_qos_per_tenant_timeout,
            bytes_per_day_limit=self.build_qos_bytes_per_day,
            classify_by_estimated_bytes=self.build_qos_bulk_bytes_threshold,
            classify_by_input_count=self.build_qos_bulk_inputs_threshold,
        )

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

        # Parse ACL configuration from [tool.strata.acl] section
        if "acl" in merged:
            merged["acl_config"] = _parse_acl_config(merged.pop("acl"))

        # Store transforms configuration from [tool.strata.transforms] section
        if "transforms" in merged:
            merged["transforms_config"] = merged.pop("transforms")

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
