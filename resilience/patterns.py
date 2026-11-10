"""Exponential Jitter Backoff, Bulkhead concurrency limiter, and Token Bucket rate limiter."""

from __future__ import annotations

import asyncio
import inspect
import random
import threading
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from typing import Any, TypeVar

T = TypeVar("T")


class BulkheadFullError(RuntimeError):
    """Raised when concurrent capacity of a Bulkhead is saturated."""


class RetryPolicy:
    """Retry helper implementing capped exponential backoff with optional AWS Full Jitter."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.05,
        max_delay: float = 2.0,
        jitter: bool = True,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if base_delay < 0.0 or max_delay < base_delay:
            raise ValueError("Invalid delay bounds")
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self.retry_on = retry_on

    def compute_delay(self, attempt: int) -> float:
        """Compute backoff delay in seconds for 0-based retry attempt."""
        cap = min(self.max_delay, self.base_delay * (2**attempt))
        if not self.jitter or cap == 0.0:
            return cap
        return random.uniform(0.0, cap)

    async def execute_async(
        self,
        fn: Callable[..., Awaitable[T] | T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Run fn up to max_attempts with exponential jitter backoff between failures."""
        last_exc: BaseException | None = None
        for attempt in range(self.max_attempts):
            try:
                outcome = fn(*args, **kwargs)
                return await outcome if inspect.isawaitable(outcome) else outcome  # type: ignore[return-value]
            except self.retry_on as exc:
                last_exc = exc
                if attempt + 1 >= self.max_attempts:
                    break
                delay = self.compute_delay(attempt)
                if delay > 0.0:
                    await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc


class Bulkhead:
    """Concurrency bulkhead isolating downstream call pools with reject-on-saturation."""

    def __init__(self, max_concurrent: int = 16) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self.max_concurrent = max_concurrent
        self._lock = threading.Lock()
        self._in_flight = 0

    def try_acquire(self) -> bool:
        with self._lock:
            if self._in_flight >= self.max_concurrent:
                return False
            self._in_flight += 1
            return True

    def release(self) -> None:
        with self._lock:
            self._in_flight = max(0, self._in_flight - 1)

    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    @contextmanager
    def guard(self):
        """Synchronous bulkhead guard raising BulkheadFullError if saturated."""
        if not self.try_acquire():
            raise BulkheadFullError(f"Bulkhead limit ({self.max_concurrent}) reached")
        try:
            yield
        finally:
            self.release()

    @asynccontextmanager
    async def guard_async(self):
        """Asynchronous bulkhead guard raising BulkheadFullError if saturated."""
        if not self.try_acquire():
            raise BulkheadFullError(f"Bulkhead limit ({self.max_concurrent}) reached")
        try:
            yield
        finally:
            self.release()


class TokenBucketRateLimiter:
    """Thread-safe token-bucket rate limiter using monotonic timestamps."""

    def __init__(self, rate_per_sec: float, capacity: int) -> None:
        if rate_per_sec <= 0.0 or capacity < 1:
            raise ValueError("rate_per_sec and capacity must be positive")
        self.rate_per_sec = rate_per_sec
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def allow(self, tokens: float = 1.0) -> bool:
        if tokens <= 0.0:
            return True
        with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self._last_refill)
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
            self._last_refill = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False
