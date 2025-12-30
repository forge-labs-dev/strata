"""Tenant types and context for multi-tenancy support.

This module provides:
- TenantConfig: Per-tenant configuration (QoS slots, rate limits, feature flags)
- Context management: get_tenant_id() / set_tenant_id() for request-scoped tenant context
- DEFAULT_TENANT_ID: Fallback for backward compatibility with single-tenant deployments
"""

import contextvars
import re
import time
from dataclasses import dataclass, field

# Context variable for tenant-scoped data (request-scoped via middleware)
_tenant_context: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "tenant_id", default=None
)

# Default tenant for backward compatibility (single-tenant mode)
DEFAULT_TENANT_ID = "_default"

# Tenant ID validation constraints
MAX_TENANT_ID_LENGTH = 64
TENANT_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")


def validate_tenant_id(tenant_id: str) -> tuple[bool, str | None]:
    """Validate tenant ID format.

    Returns (is_valid, error_message).
    Error message is None if valid.

    Valid tenant IDs:
    - 1-64 characters
    - Start with alphanumeric
    - Contain only alphanumeric, underscore, hyphen
    - Examples: "acme-corp", "tenant_123", "MyTenant"

    Invalid:
    - Empty string
    - Too long (>64 chars)
    - Start with _ or -
    - Contain special characters
    - Examples: "", "_private", "-bad", "has spaces", "has@special"
    """
    if not tenant_id:
        return False, "Tenant ID cannot be empty"

    if len(tenant_id) > MAX_TENANT_ID_LENGTH:
        return False, f"Tenant ID exceeds maximum length of {MAX_TENANT_ID_LENGTH} characters"

    if not TENANT_ID_PATTERN.match(tenant_id):
        return False, (
            "Tenant ID must start with alphanumeric and contain only "
            "alphanumeric characters, underscores, and hyphens"
        )

    return True, None


@dataclass(frozen=True)
class TenantConfig:
    """Per-tenant configuration and limits.

    Loaded from tenant registry on startup or from external config.
    All optional fields default to None, meaning "use global defaults".
    """

    tenant_id: str

    # QoS quotas (None means use global defaults)
    interactive_slots: int | None = None
    bulk_slots: int | None = None
    per_client_interactive: int | None = None
    per_client_bulk: int | None = None

    # Rate limits (None means use global defaults)
    requests_per_second: float | None = None
    burst: float | None = None

    # Size limits
    max_cache_size_bytes: int | None = None
    max_response_bytes: int | None = None

    # Feature flags
    enabled: bool = True  # Can disable tenant without deleting

    def effective_interactive_slots(self, default: int) -> int:
        """Get interactive slots, falling back to default if not set."""
        return self.interactive_slots if self.interactive_slots is not None else default

    def effective_bulk_slots(self, default: int) -> int:
        """Get bulk slots, falling back to default if not set."""
        return self.bulk_slots if self.bulk_slots is not None else default

    def effective_per_client_interactive(self, default: int) -> int:
        """Get per-client interactive limit, falling back to default if not set."""
        return self.per_client_interactive if self.per_client_interactive is not None else default

    def effective_per_client_bulk(self, default: int) -> int:
        """Get per-client bulk limit, falling back to default if not set."""
        return self.per_client_bulk if self.per_client_bulk is not None else default


@dataclass
class TenantQuotas:
    """Runtime state for per-tenant resource tracking.

    Maintains LRU-evictable state for metrics and rate limiting.
    Created lazily when a tenant first makes a request.
    """

    tenant_id: str

    # QoS limiters (created lazily by server)
    interactive_limiter: object | None = None  # ResizableLimiter
    bulk_limiter: object | None = None  # ResizableLimiter

    # Rate limiter bucket (created lazily)
    rate_bucket: object | None = None  # TokenBucket

    # Per-tenant aggregate metrics
    total_scans: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    bytes_from_cache: int = 0
    bytes_from_storage: int = 0
    rows_returned: int = 0

    # Last access for LRU eviction
    last_access: float = field(default_factory=time.time)

    def touch(self) -> None:
        """Update last access time for LRU tracking."""
        self.last_access = time.time()

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        total_requests = self.cache_hits + self.cache_misses
        return {
            "tenant_id": self.tenant_id,
            "total_scans": self.total_scans,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": (
                round(self.cache_hits / total_requests, 3) if total_requests > 0 else 0.0
            ),
            "bytes_from_cache": self.bytes_from_cache,
            "bytes_from_storage": self.bytes_from_storage,
            "rows_returned": self.rows_returned,
        }


def get_tenant_id() -> str:
    """Get current tenant ID from context, defaulting to _default.

    Returns the tenant ID set by the tenant middleware for the current request.
    If no tenant context is set, returns DEFAULT_TENANT_ID for backward
    compatibility with single-tenant deployments.
    """
    return _tenant_context.get() or DEFAULT_TENANT_ID


def set_tenant_id(tenant_id: str) -> contextvars.Token:
    """Set tenant ID in context. Returns token for reset.

    Called by tenant middleware at the start of each request.
    The returned token can be used with reset_tenant_id() to restore
    the previous context (useful for cleanup in finally blocks).
    """
    return _tenant_context.set(tenant_id)


def reset_tenant_id(token: contextvars.Token) -> None:
    """Reset tenant context to previous value using token from set_tenant_id()."""
    _tenant_context.reset(token)


def clear_tenant_context() -> None:
    """Clear tenant context (set to None)."""
    _tenant_context.set(None)
