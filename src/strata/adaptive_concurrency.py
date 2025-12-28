"""Adaptive concurrency control for Strata QoS.

Implements Netflix-style adaptive concurrency limiting based on latency signals:
- Starts at configured slot counts
- Monitors p95 latency and queue wait time
- Increases slots when latency is good and queue pressure is rising
- Decreases slots when latency exceeds target
- Uses hysteresis to prevent control loop flapping

The controller adjusts both interactive and bulk tier semaphores independently.

Key component: ResizableLimiter
    Unlike asyncio.Semaphore, ResizableLimiter tracks capacity vs in_use separately.
    This allows correct dynamic resizing - decreasing capacity takes effect as
    active requests complete, rather than fighting with normal release() calls.
"""

import asyncio
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field

logger = logging.getLogger("strata.adaptive")


class ResizableLimiter:
    """A resizable concurrency limiter for dynamic capacity adjustment.

    Unlike asyncio.Semaphore, this limiter tracks capacity and in_use count
    separately, allowing correct dynamic resizing:
    - Increasing capacity immediately allows more concurrent requests
    - Decreasing capacity takes effect as active requests complete

    This is essential for adaptive concurrency control where we need to
    adjust slot counts without fighting normal request completion.

    Thread-safety: All operations are protected by an asyncio.Lock.
    """

    def __init__(self, capacity: int):
        self._capacity = capacity
        self._in_use = 0
        self._lock = asyncio.Lock()
        self._cv = asyncio.Condition(self._lock)

    @property
    def capacity(self) -> int:
        """Current capacity (max concurrent requests)."""
        return self._capacity

    @property
    def in_use(self) -> int:
        """Current number of active requests."""
        return self._in_use

    @property
    def available(self) -> int:
        """Number of available slots."""
        return max(0, self._capacity - self._in_use)

    async def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a slot, optionally with timeout.

        Args:
            timeout: Maximum seconds to wait (None = wait forever)

        Returns:
            True if acquired, False if timeout expired
        """
        async with self._cv:
            if timeout is None:
                # Wait indefinitely
                while self._in_use >= self._capacity:
                    await self._cv.wait()
                self._in_use += 1
                return True

            # Wait with timeout
            loop = asyncio.get_running_loop()
            end = loop.time() + timeout
            while self._in_use >= self._capacity:
                remaining = end - loop.time()
                if remaining <= 0:
                    return False
                try:
                    await asyncio.wait_for(self._cv.wait(), timeout=remaining)
                except TimeoutError:
                    # Check one more time in case we were notified
                    if self._in_use >= self._capacity:
                        return False
            self._in_use += 1
            return True

    async def release(self) -> None:
        """Release a slot, waking one waiting acquirer."""
        async with self._cv:
            if self._in_use <= 0:
                raise RuntimeError("release() called without matching acquire()")
            self._in_use -= 1
            self._cv.notify(1)

    async def resize(self, new_capacity: int) -> None:
        """Resize the limiter's capacity.

        If capacity increases, waiting acquirers are woken to compete for slots.
        If capacity decreases, the change takes effect as active requests complete.

        Args:
            new_capacity: New maximum concurrent requests (must be >= 1)
        """
        if new_capacity < 1:
            raise ValueError("capacity must be >= 1")
        async with self._cv:
            old_capacity = self._capacity
            self._capacity = new_capacity
            if new_capacity > old_capacity:
                # Wake all waiters to compete for new slots
                self._cv.notify_all()

    def get_stats(self) -> dict:
        """Get limiter statistics (non-async, for metrics)."""
        return {
            "capacity": self._capacity,
            "in_use": self._in_use,
            "available": max(0, self._capacity - self._in_use),
        }


@dataclass
class AdaptiveConfig:
    """Configuration for adaptive concurrency controller.

    Attributes:
        enabled: Whether adaptive control is enabled
        adjustment_interval_seconds: How often to check and adjust (seconds)
        latency_target_p95_ms: Target p95 latency in milliseconds
        queue_wait_threshold_ms: Queue wait time that indicates pressure
        min_slots_interactive: Minimum interactive slots (floor)
        max_slots_interactive: Maximum interactive slots (ceiling)
        min_slots_bulk: Minimum bulk slots (floor)
        max_slots_bulk: Maximum bulk slots (ceiling)
        increase_step: Slots to add when conditions are good
        decrease_step: Slots to remove when latency is high
        hysteresis_count: Number of consecutive signals needed to change
        window_size: Number of samples to keep for rolling p95
    """

    enabled: bool = False  # Disabled by default (opt-in)
    adjustment_interval_seconds: float = 5.0
    latency_target_p95_ms: float = 500.0  # 500ms target p95
    queue_wait_threshold_ms: float = 100.0  # 100ms queue wait = pressure
    min_slots_interactive: int = 4
    max_slots_interactive: int = 64
    min_slots_bulk: int = 2
    max_slots_bulk: int = 32
    increase_step: int = 1
    decrease_step: int = 1
    hysteresis_count: int = 3  # 3 consecutive signals needed
    window_size: int = 100  # Keep last 100 samples for p95


class RollingLatencyWindow:
    """Thread-safe rolling window for latency percentile calculation.

    Uses a circular buffer to maintain the most recent N observations,
    allowing accurate p95 calculation over a sliding window.
    """

    def __init__(self, size: int = 100):
        self._size = size
        self._lock = threading.Lock()
        self._samples: deque[float] = deque(maxlen=size)
        self._count = 0  # Total samples seen (for metrics)

    def record(self, latency_ms: float) -> None:
        """Record a latency observation."""
        with self._lock:
            self._samples.append(latency_ms)
            self._count += 1

    def get_p95(self) -> float | None:
        """Calculate p95 from the current window.

        Returns:
            p95 latency in ms, or None if not enough samples.
        """
        with self._lock:
            if len(self._samples) < 10:
                # Need at least 10 samples for meaningful percentile
                return None
            sorted_samples = sorted(self._samples)

        n = len(sorted_samples)
        idx = max(0, math.ceil(n * 0.95) - 1)
        return sorted_samples[idx]

    def get_stats(self) -> dict:
        """Get statistics from the current window."""
        with self._lock:
            if not self._samples:
                return {
                    "count": 0,
                    "window_size": 0,
                    "p50_ms": None,
                    "p95_ms": None,
                    "p99_ms": None,
                    "min_ms": None,
                    "max_ms": None,
                    "avg_ms": None,
                }
            sorted_samples = sorted(self._samples)
            count = len(sorted_samples)

        def pct(p: float) -> float:
            idx = max(0, math.ceil(count * p) - 1)
            return sorted_samples[idx]

        return {
            "count": self._count,
            "window_size": count,
            "p50_ms": round(pct(0.50), 2),
            "p95_ms": round(pct(0.95), 2),
            "p99_ms": round(pct(0.99), 2),
            "min_ms": round(sorted_samples[0], 2),
            "max_ms": round(sorted_samples[-1], 2),
            "avg_ms": round(sum(sorted_samples) / count, 2),
        }

    def reset(self) -> None:
        """Reset the window."""
        with self._lock:
            self._samples.clear()
            self._count = 0


@dataclass
class TierState:
    """Per-tier state for adaptive control.

    Tracks the control loop state for a single tier (interactive or bulk).
    """

    name: str
    current_slots: int
    min_slots: int
    max_slots: int
    latency_window: RollingLatencyWindow = field(default_factory=RollingLatencyWindow)
    queue_wait_window: RollingLatencyWindow = field(default_factory=RollingLatencyWindow)

    # Hysteresis counters (positive = increase signals, negative = decrease signals)
    consecutive_increase_signals: int = 0
    consecutive_decrease_signals: int = 0

    # Last adjustment info for observability
    last_adjustment_time: float = 0.0
    last_adjustment_direction: str = ""  # "increase", "decrease", ""
    last_p95_ms: float | None = None
    last_queue_wait_p95_ms: float | None = None

    # Cumulative stats (event counts, not slot counts)
    increase_events: int = 0
    decrease_events: int = 0


class AdaptiveConcurrencyController:
    """Adaptive concurrency controller for QoS tiers.

    Monitors latency and queue wait time to dynamically adjust slot counts.
    Uses hysteresis to prevent flapping (requires N consecutive signals).

    Decision logic (per tier):
    1. If p95 > target: signal decrease (system is overloaded)
    2. Elif p95 < target AND queue_wait > threshold: signal increase (room for more)
    3. Else: reset signals (stable state)

    After hysteresis_count consecutive signals, adjust slots by step size.

    Uses ResizableLimiter instead of asyncio.Semaphore for correct dynamic resizing.
    """

    def __init__(
        self,
        config: AdaptiveConfig,
        interactive_limiter: ResizableLimiter,
        bulk_limiter: ResizableLimiter,
    ):
        self.config = config
        self._interactive_limiter = interactive_limiter
        self._bulk_limiter = bulk_limiter

        # Per-tier state
        self._interactive = TierState(
            name="interactive",
            current_slots=interactive_limiter.capacity,
            min_slots=config.min_slots_interactive,
            max_slots=config.max_slots_interactive,
            latency_window=RollingLatencyWindow(config.window_size),
            queue_wait_window=RollingLatencyWindow(config.window_size),
        )
        self._bulk = TierState(
            name="bulk",
            current_slots=bulk_limiter.capacity,
            min_slots=config.min_slots_bulk,
            max_slots=config.max_slots_bulk,
            latency_window=RollingLatencyWindow(config.window_size),
            queue_wait_window=RollingLatencyWindow(config.window_size),
        )

        # Background task handle
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def record_latency(self, tier: str, latency_ms: float) -> None:
        """Record a completed request latency for adaptive control.

        Args:
            tier: "interactive" or "bulk"
            latency_ms: Request latency in milliseconds
        """
        if tier == "interactive":
            self._interactive.latency_window.record(latency_ms)
        elif tier == "bulk":
            self._bulk.latency_window.record(latency_ms)
        else:
            logger.warning("Unknown tier for latency recording", extra={"tier": tier})

    def record_queue_wait(self, tier: str, wait_ms: float) -> None:
        """Record queue wait time for adaptive control.

        Queue wait is the time a request spent waiting for a slot before
        being admitted. High queue wait indicates demand exceeds capacity,
        which is the signal to increase slots (if latency is good).

        Args:
            tier: "interactive" or "bulk"
            wait_ms: Time spent waiting in queue (milliseconds)
        """
        if tier == "interactive":
            self._interactive.queue_wait_window.record(wait_ms)
        elif tier == "bulk":
            self._bulk.queue_wait_window.record(wait_ms)
        else:
            logger.warning("Unknown tier for queue wait recording", extra={"tier": tier})

    async def start(self) -> None:
        """Start the adaptive control background loop."""
        if not self.config.enabled:
            logger.info("Adaptive concurrency control is disabled")
            return

        self._stop_event.clear()
        self._task = asyncio.create_task(self._control_loop())
        logger.info(
            "Adaptive concurrency control started",
            extra={
                "interval_seconds": self.config.adjustment_interval_seconds,
                "target_p95_ms": self.config.latency_target_p95_ms,
                "hysteresis": self.config.hysteresis_count,
            },
        )

    async def stop(self) -> None:
        """Stop the adaptive control background loop."""
        if self._task is not None:
            self._stop_event.set()
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            logger.info("Adaptive concurrency control stopped")

    async def _control_loop(self) -> None:
        """Background loop that periodically checks and adjusts concurrency."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(self.config.adjustment_interval_seconds)

                # Get current queue wait from server state (passed via record methods)
                # For now, we'll rely on latency signals only
                # Queue wait could be added as a separate signal method

                await self._evaluate_and_adjust(self._interactive, self._interactive_limiter)
                await self._evaluate_and_adjust(self._bulk, self._bulk_limiter)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Adaptive control loop error: {e}", exc_info=True)

    async def _evaluate_and_adjust(
        self,
        tier: TierState,
        limiter: ResizableLimiter,
    ) -> None:
        """Evaluate a tier and adjust slots if needed.

        Decision logic:
        1. If p95 > target: signal decrease (system is overloaded)
        2. Elif p95 < 80% of target AND queue_wait_p95 > threshold: signal increase
           (latency is good AND there's queue pressure, so we have room for more)
        3. Else: reset signals (stable state)

        The queue pressure requirement for increases prevents over-opening during
        "fast-but-memory-hungry" periods where latency is low but we shouldn't
        add more concurrent work.

        Apply change after hysteresis_count consecutive signals.
        """
        p95 = tier.latency_window.get_p95()
        queue_wait_p95 = tier.queue_wait_window.get_p95()
        tier.last_p95_ms = p95
        tier.last_queue_wait_p95_ms = queue_wait_p95

        # Not enough latency data yet
        if p95 is None:
            return

        target = self.config.latency_target_p95_ms
        queue_threshold = self.config.queue_wait_threshold_ms

        if p95 > target:
            # Latency too high - signal decrease
            tier.consecutive_decrease_signals += 1
            tier.consecutive_increase_signals = 0

            if tier.consecutive_decrease_signals >= self.config.hysteresis_count:
                await self._adjust_slots(tier, limiter, -self.config.decrease_step)
                tier.consecutive_decrease_signals = 0

        elif p95 < target * 0.8 and queue_wait_p95 is not None and queue_wait_p95 > queue_threshold:
            # Latency well under target AND queue pressure exists - signal increase
            # Both conditions must be true:
            # 1. p95 < 80% of target (using 80% as headroom to avoid oscillation)
            # 2. queue_wait_p95 > threshold (requests are waiting, demand exists)
            # This prevents over-opening when latency is low but there's no demand.
            tier.consecutive_increase_signals += 1
            tier.consecutive_decrease_signals = 0

            if tier.consecutive_increase_signals >= self.config.hysteresis_count:
                await self._adjust_slots(tier, limiter, self.config.increase_step)
                tier.consecutive_increase_signals = 0

        else:
            # Latency in acceptable range OR no queue pressure - reset signals
            tier.consecutive_increase_signals = 0
            tier.consecutive_decrease_signals = 0

    async def _adjust_slots(
        self,
        tier: TierState,
        limiter: ResizableLimiter,
        delta: int,
    ) -> None:
        """Adjust limiter capacity by delta slots.

        Uses ResizableLimiter.resize() for correct dynamic adjustment:
        - Increasing capacity immediately allows more concurrent requests
        - Decreasing capacity takes effect as active requests complete

        We bound the result to [min_slots, max_slots].
        """
        new_slots = tier.current_slots + delta
        new_slots = max(tier.min_slots, min(tier.max_slots, new_slots))

        if new_slots == tier.current_slots:
            return

        # Resize the limiter - this is the correct way to adjust capacity
        await limiter.resize(new_slots)

        direction = "increase" if new_slots > tier.current_slots else "decrease"
        if direction == "increase":
            tier.increase_events += 1
        else:
            tier.decrease_events += 1

        tier.current_slots = new_slots
        tier.last_adjustment_time = time.time()
        tier.last_adjustment_direction = direction

        logger.info(
            f"Adaptive concurrency adjusted {tier.name}",
            extra={
                "tier": tier.name,
                "direction": direction,
                "new_slots": tier.current_slots,
                "p95_ms": tier.last_p95_ms,
                "target_p95_ms": self.config.latency_target_p95_ms,
            },
        )

    def get_metrics(self) -> dict:
        """Get adaptive control metrics for observability."""
        return {
            "enabled": self.config.enabled,
            "target_p95_ms": self.config.latency_target_p95_ms,
            "queue_wait_threshold_ms": self.config.queue_wait_threshold_ms,
            "hysteresis_count": self.config.hysteresis_count,
            "interactive": {
                "current_slots": self._interactive.current_slots,
                "min_slots": self._interactive.min_slots,
                "max_slots": self._interactive.max_slots,
                "last_p95_ms": self._interactive.last_p95_ms,
                "last_queue_wait_p95_ms": self._interactive.last_queue_wait_p95_ms,
                "consecutive_increase_signals": self._interactive.consecutive_increase_signals,
                "consecutive_decrease_signals": self._interactive.consecutive_decrease_signals,
                "increase_events": self._interactive.increase_events,
                "decrease_events": self._interactive.decrease_events,
                "latency_stats": self._interactive.latency_window.get_stats(),
                "queue_wait_stats": self._interactive.queue_wait_window.get_stats(),
            },
            "bulk": {
                "current_slots": self._bulk.current_slots,
                "min_slots": self._bulk.min_slots,
                "max_slots": self._bulk.max_slots,
                "last_p95_ms": self._bulk.last_p95_ms,
                "last_queue_wait_p95_ms": self._bulk.last_queue_wait_p95_ms,
                "consecutive_increase_signals": self._bulk.consecutive_increase_signals,
                "consecutive_decrease_signals": self._bulk.consecutive_decrease_signals,
                "increase_events": self._bulk.increase_events,
                "decrease_events": self._bulk.decrease_events,
                "latency_stats": self._bulk.latency_window.get_stats(),
                "queue_wait_stats": self._bulk.queue_wait_window.get_stats(),
            },
        }
