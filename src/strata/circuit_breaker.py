"""Circuit breaker for external dependency protection.

Implements the circuit breaker pattern to protect against cascading failures
when external dependencies (S3, metadata store) become unavailable.

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Dependency is failing, requests fail fast
- HALF_OPEN: Testing if dependency has recovered
"""

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for a circuit breaker."""

    # Number of failures before opening circuit
    failure_threshold: int = 5
    # Number of successes in half-open to close circuit
    success_threshold: int = 3
    # Time in seconds before attempting recovery
    reset_timeout_seconds: float = 30.0
    # Optional name for identification
    name: str = "default"


@dataclass
class CircuitStats:
    """Statistics for a circuit breaker."""

    state: CircuitState
    failure_count: int
    success_count: int
    total_calls: int
    total_failures: int
    total_successes: int
    total_rejections: int  # Calls rejected when open
    last_failure_at: float | None
    last_success_at: float | None
    opened_at: float | None  # When circuit was last opened
    half_opened_at: float | None  # When circuit entered half-open

    def to_dict(self) -> dict:
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "total_rejections": self.total_rejections,
            "last_failure_at": self.last_failure_at,
            "last_success_at": self.last_success_at,
            "opened_at": self.opened_at,
            "half_opened_at": self.half_opened_at,
        }


class CircuitOpenError(Exception):
    """Raised when circuit is open and request is rejected."""

    def __init__(self, name: str, message: str = ""):
        self.name = name
        self.message = message or f"Circuit breaker '{name}' is open"
        super().__init__(self.message)


class CircuitBreaker:
    """Circuit breaker for protecting external dependencies.

    Usage:
        breaker = CircuitBreaker(CircuitBreakerConfig(name="s3"))

        # Option 1: Context manager
        with breaker:
            result = call_external_service()

        # Option 2: Decorator
        @breaker
        def call_external_service():
            ...

        # Option 3: Call method
        result = breaker.call(call_external_service)
    """

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self._lock = threading.Lock()

        # State
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0

        # Timestamps
        self._opened_at: float | None = None
        self._half_opened_at: float | None = None
        self._last_failure_at: float | None = None
        self._last_success_at: float | None = None

        # Lifetime counters
        self._total_calls = 0
        self._total_failures = 0
        self._total_successes = 0
        self._total_rejections = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, transitioning to half-open if timeout expired."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to_half_open()
            return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt recovery."""
        if self._opened_at is None:
            return False
        elapsed = time.time() - self._opened_at
        return elapsed >= self.config.reset_timeout_seconds

    def _transition_to_half_open(self) -> None:
        """Transition to half-open state."""
        self._state = CircuitState.HALF_OPEN
        self._half_opened_at = time.time()
        self._success_count = 0
        self._failure_count = 0

    def _transition_to_open(self) -> None:
        """Transition to open state."""
        self._state = CircuitState.OPEN
        self._opened_at = time.time()
        self._failure_count = 0
        self._success_count = 0

    def _transition_to_closed(self) -> None:
        """Transition to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at = None
        self._half_opened_at = None

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._total_calls += 1
            self._total_successes += 1
            self._last_success_at = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to_closed()
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._total_calls += 1
            self._total_failures += 1
            self._last_failure_at = time.time()
            self._failure_count += 1

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open immediately opens circuit
                self._transition_to_open()
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._transition_to_open()

    def allow_request(self) -> bool:
        """Check if a request should be allowed."""
        current_state = self.state  # This may transition to half-open

        if current_state == CircuitState.CLOSED:
            return True
        elif current_state == CircuitState.OPEN:
            with self._lock:
                self._total_rejections += 1
            return False
        else:  # HALF_OPEN
            return True  # Allow requests to test recovery

    def call(self, func: Callable[[], T]) -> T:
        """Execute a function with circuit breaker protection.

        Args:
            func: Function to execute

        Returns:
            Result of the function

        Raises:
            CircuitOpenError: If circuit is open
            Exception: If function raises and circuit should propagate
        """
        if not self.allow_request():
            raise CircuitOpenError(self.config.name)

        try:
            result = func()
            self.record_success()
            return result
        except Exception:
            self.record_failure()
            raise

    def __enter__(self) -> "CircuitBreaker":
        """Context manager entry - check if request allowed."""
        if not self.allow_request():
            raise CircuitOpenError(self.config.name)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - record success or failure."""
        if exc_type is None:
            self.record_success()
        else:
            self.record_failure()
        return False  # Don't suppress exceptions

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator to wrap a function with circuit breaker protection."""

        def wrapper(*args, **kwargs) -> T:
            if not self.allow_request():
                raise CircuitOpenError(self.config.name)
            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except Exception:
                self.record_failure()
                raise

        return wrapper

    def get_stats(self) -> CircuitStats:
        """Get current circuit breaker statistics."""
        with self._lock:
            return CircuitStats(
                state=self._state,
                failure_count=self._failure_count,
                success_count=self._success_count,
                total_calls=self._total_calls,
                total_failures=self._total_failures,
                total_successes=self._total_successes,
                total_rejections=self._total_rejections,
                last_failure_at=self._last_failure_at,
                last_success_at=self._last_success_at,
                opened_at=self._opened_at,
                half_opened_at=self._half_opened_at,
            )

    def reset(self) -> None:
        """Reset circuit breaker to initial closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._opened_at = None
            self._half_opened_at = None
            self._last_failure_at = None
            self._last_success_at = None
            self._total_calls = 0
            self._total_failures = 0
            self._total_successes = 0
            self._total_rejections = 0


@dataclass
class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""

    breakers: dict[str, CircuitBreaker] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def get_or_create(
        self, name: str, config: CircuitBreakerConfig | None = None
    ) -> CircuitBreaker:
        """Get an existing circuit breaker or create a new one."""
        with self._lock:
            if name not in self.breakers:
                cfg = config or CircuitBreakerConfig(name=name)
                self.breakers[name] = CircuitBreaker(cfg)
            return self.breakers[name]

    def get(self, name: str) -> CircuitBreaker | None:
        """Get a circuit breaker by name."""
        return self.breakers.get(name)

    def get_all_stats(self) -> dict[str, dict]:
        """Get stats for all circuit breakers."""
        with self._lock:
            return {name: cb.get_stats().to_dict() for name, cb in self.breakers.items()}

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            for cb in self.breakers.values():
                cb.reset()
            self.breakers.clear()


# Global registry
_registry: CircuitBreakerRegistry | None = None
_registry_lock = threading.Lock()


def get_circuit_breaker_registry() -> CircuitBreakerRegistry:
    """Get the global circuit breaker registry."""
    global _registry
    with _registry_lock:
        if _registry is None:
            _registry = CircuitBreakerRegistry()
        return _registry


def reset_circuit_breakers() -> None:
    """Reset the global circuit breaker registry."""
    global _registry
    with _registry_lock:
        if _registry is not None:
            _registry.reset_all()
        _registry = None


def get_circuit_breaker(
    name: str, config: CircuitBreakerConfig | None = None
) -> CircuitBreaker:
    """Get or create a circuit breaker by name.

    Args:
        name: Unique name for the circuit breaker
        config: Optional configuration (only used if creating new breaker)

    Returns:
        CircuitBreaker instance
    """
    registry = get_circuit_breaker_registry()
    return registry.get_or_create(name, config)
