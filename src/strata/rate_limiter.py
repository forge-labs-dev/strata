"""Rate limiting for request throttling.

Implements a token bucket algorithm for per-client rate limiting.
This complements the existing QoS admission control by adding
request-rate throttling to prevent abuse and ensure fair usage.
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import Protocol


class Clock(Protocol):
    """Protocol for time sources (for testing)."""

    def time(self) -> float:
        """Return current time in seconds."""
        ...


class SystemClock:
    """Default clock using system time."""

    def time(self) -> float:
        return time.time()


@dataclass
class TokenBucket:
    """Token bucket for rate limiting.

    Tokens are added at a fixed rate up to a maximum capacity.
    Each request consumes one token. If no tokens are available,
    the request is rejected.
    """

    capacity: float  # Maximum tokens
    refill_rate: float  # Tokens per second
    tokens: float = field(init=False)
    last_update: float = field(init=False)
    _clock: Clock = field(default_factory=SystemClock)

    def __post_init__(self) -> None:
        self.tokens = self.capacity
        self.last_update = self._clock.time()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = self._clock.time()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_update = now

    def acquire(self, tokens: float = 1.0) -> bool:
        """Try to acquire tokens. Returns True if successful."""
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def tokens_available(self) -> float:
        """Return current number of available tokens."""
        self._refill()
        return self.tokens

    def time_until_available(self, tokens: float = 1.0) -> float:
        """Return seconds until requested tokens will be available."""
        self._refill()
        if self.tokens >= tokens:
            return 0.0
        needed = tokens - self.tokens
        return needed / self.refill_rate


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    # Global rate limits (all clients combined)
    global_requests_per_second: float = 1000.0
    global_burst: float = 100.0  # Max burst above rate

    # Per-client rate limits
    client_requests_per_second: float = 100.0
    client_burst: float = 20.0

    # Per-endpoint rate limits (optional overrides)
    scan_requests_per_second: float = 50.0
    scan_burst: float = 10.0
    warm_requests_per_second: float = 10.0
    warm_burst: float = 5.0

    # Cleanup settings
    client_ttl_seconds: float = 300.0  # Remove idle clients after 5 minutes

    # Whether rate limiting is enabled
    enabled: bool = True


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""

    allowed: bool
    limit_type: str | None = None  # "global", "client", "endpoint"
    retry_after_seconds: float | None = None
    tokens_remaining: float | None = None


class RateLimiter:
    """Multi-level rate limiter with global, per-client, and per-endpoint limits."""

    def __init__(
        self,
        config: RateLimitConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.config = config or RateLimitConfig()
        self._clock = clock or SystemClock()
        self._lock = Lock()

        # Global bucket
        self._global_bucket = TokenBucket(
            capacity=self.config.global_burst,
            refill_rate=self.config.global_requests_per_second,
            _clock=self._clock,
        )

        # Per-client buckets
        self._client_buckets: dict[str, TokenBucket] = {}
        self._client_last_seen: dict[str, float] = {}

        # Per-endpoint buckets
        self._endpoint_buckets: dict[str, TokenBucket] = {}

        # Stats
        self._stats = {
            "total_requests": 0,
            "allowed_requests": 0,
            "rejected_global": 0,
            "rejected_client": 0,
            "rejected_endpoint": 0,
        }

    def _get_client_bucket(self, client_id: str) -> TokenBucket:
        """Get or create a bucket for a client."""
        if client_id not in self._client_buckets:
            self._client_buckets[client_id] = TokenBucket(
                capacity=self.config.client_burst,
                refill_rate=self.config.client_requests_per_second,
                _clock=self._clock,
            )
        self._client_last_seen[client_id] = self._clock.time()
        return self._client_buckets[client_id]

    def _get_endpoint_bucket(self, endpoint: str) -> TokenBucket | None:
        """Get or create a bucket for an endpoint (if configured)."""
        if endpoint == "/v1/scan":
            if endpoint not in self._endpoint_buckets:
                self._endpoint_buckets[endpoint] = TokenBucket(
                    capacity=self.config.scan_burst,
                    refill_rate=self.config.scan_requests_per_second,
                    _clock=self._clock,
                )
            return self._endpoint_buckets[endpoint]
        elif endpoint.startswith("/v1/cache/warm"):
            key = "/v1/cache/warm"
            if key not in self._endpoint_buckets:
                self._endpoint_buckets[key] = TokenBucket(
                    capacity=self.config.warm_burst,
                    refill_rate=self.config.warm_requests_per_second,
                    _clock=self._clock,
                )
            return self._endpoint_buckets[key]
        return None

    def check(
        self,
        client_id: str,
        endpoint: str | None = None,
    ) -> RateLimitResult:
        """Check if a request should be allowed.

        Args:
            client_id: Unique identifier for the client (e.g., IP address)
            endpoint: Optional endpoint path for endpoint-specific limits

        Returns:
            RateLimitResult indicating whether the request is allowed
        """
        if not self.config.enabled:
            return RateLimitResult(allowed=True)

        with self._lock:
            self._stats["total_requests"] += 1

            # Check global limit first
            if not self._global_bucket.acquire():
                self._stats["rejected_global"] += 1
                return RateLimitResult(
                    allowed=False,
                    limit_type="global",
                    retry_after_seconds=self._global_bucket.time_until_available(),
                    tokens_remaining=0,
                )

            # Check per-client limit
            client_bucket = self._get_client_bucket(client_id)
            if not client_bucket.acquire():
                self._stats["rejected_client"] += 1
                return RateLimitResult(
                    allowed=False,
                    limit_type="client",
                    retry_after_seconds=client_bucket.time_until_available(),
                    tokens_remaining=0,
                )

            # Check per-endpoint limit (if applicable)
            if endpoint:
                endpoint_bucket = self._get_endpoint_bucket(endpoint)
                if endpoint_bucket and not endpoint_bucket.acquire():
                    self._stats["rejected_endpoint"] += 1
                    return RateLimitResult(
                        allowed=False,
                        limit_type="endpoint",
                        retry_after_seconds=endpoint_bucket.time_until_available(),
                        tokens_remaining=0,
                    )

            self._stats["allowed_requests"] += 1
            return RateLimitResult(
                allowed=True,
                tokens_remaining=client_bucket.tokens_available(),
            )

    def cleanup_stale_clients(self) -> int:
        """Remove client buckets that haven't been used recently.

        Returns:
            Number of clients removed
        """
        with self._lock:
            now = self._clock.time()
            stale = [
                client_id
                for client_id, last_seen in self._client_last_seen.items()
                if now - last_seen > self.config.client_ttl_seconds
            ]
            for client_id in stale:
                del self._client_buckets[client_id]
                del self._client_last_seen[client_id]
            return len(stale)

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_clients": len(self._client_buckets),
                "global_tokens_available": self._global_bucket.tokens_available(),
                "enabled": self.config.enabled,
            }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        with self._lock:
            self._stats = {
                "total_requests": 0,
                "allowed_requests": 0,
                "rejected_global": 0,
                "rejected_client": 0,
                "rejected_endpoint": 0,
            }


# Global rate limiter instance
_rate_limiter: RateLimiter | None = None


def get_rate_limiter() -> RateLimiter | None:
    """Get the global rate limiter instance."""
    return _rate_limiter


def init_rate_limiter(config: RateLimitConfig | None = None) -> RateLimiter:
    """Initialize the global rate limiter."""
    global _rate_limiter
    _rate_limiter = RateLimiter(config)
    return _rate_limiter


def reset_rate_limiter() -> None:
    """Reset the global rate limiter (for testing)."""
    global _rate_limiter
    _rate_limiter = None
