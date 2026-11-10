"""Thread-safe Circuit Breaker with monotonic clock, sliding window, and half-open admission control."""

from __future__ import annotations

import inspect
import threading
import time
from collections import deque
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any, TypeVar

T = TypeVar("T")


class State(StrEnum):
    """Circuit breaker FSM states."""

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(RuntimeError):
    """Raised when a call is rejected because the circuit breaker is OPEN or half-open saturated."""


class CircuitBreaker:
    """Thread-safe Circuit Breaker supporting count/windowed failures and bounded HALF_OPEN probes."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 5.0,
        half_open_max_calls: int = 1,
        window_size: int | None = None,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1")
        if recovery_timeout <= 0.0:
            raise ValueError("recovery_timeout must be positive")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be at least 1")
        if window_size is not None and window_size < failure_threshold:
            raise ValueError("window_size must be at least failure_threshold")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.window_size = window_size

        self._lock = threading.RLock()
        self._state = State.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_in_flight = 0
        self._half_open_successes = 0
        self._outcomes: deque[bool] | None = deque(maxlen=window_size) if window_size else None

    @property
    def state(self) -> State:
        with self._lock:
            return self._state

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failure_count

    def allow_request(self) -> bool:
        """Check whether a request may proceed under current breaker state."""
        with self._lock:
            if self._state == State.CLOSED:
                return True

            if self._state == State.OPEN:
                if (time.monotonic() - self._last_failure_time) > self.recovery_timeout:
                    self._state = State.HALF_OPEN
                    self._half_open_in_flight = 1
                    self._half_open_successes = 0
                    return True
                return False

            # HALF_OPEN: admit bounded concurrent probe requests
            if self._half_open_in_flight < self.half_open_max_calls:
                self._half_open_in_flight += 1
                return True
            return False

    def record_success(self) -> None:
        """Record a successful invocation and close circuit after required half-open probes."""
        with self._lock:
            if self._state == State.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._half_open_successes += 1
                if self._half_open_successes >= self.half_open_max_calls:
                    self._reset_to_closed()
                return

            if self._outcomes is not None:
                self._outcomes.append(True)
                self._failure_count = sum(1 for ok in self._outcomes if not ok)
            else:
                self._failure_count = 0
            self._state = State.CLOSED

    def record_failure(self) -> None:
        """Record a failed invocation and trip breaker OPEN when threshold is reached."""
        with self._lock:
            self._last_failure_time = time.monotonic()

            if self._state == State.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._half_open_successes = 0
                self._state = State.OPEN
                return

            if self._outcomes is not None:
                self._outcomes.append(False)
                self._failure_count = sum(1 for ok in self._outcomes if not ok)
            else:
                self._failure_count += 1

            if self._failure_count >= self.failure_threshold:
                self._state = State.OPEN

    def _reset_to_closed(self) -> None:
        self._state = State.CLOSED
        self._failure_count = 0
        self._half_open_in_flight = 0
        self._half_open_successes = 0
        if self._outcomes is not None:
            self._outcomes.clear()

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute a synchronous callable guarded by the circuit breaker."""
        if not self.allow_request():
            raise CircuitOpenError("Circuit breaker is OPEN")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    async def call_async(
        self, fn: Callable[..., Awaitable[T] | T], *args: Any, **kwargs: Any
    ) -> T:
        """Execute an async or sync callable guarded by the circuit breaker."""
        if not self.allow_request():
            raise CircuitOpenError("Circuit breaker is OPEN")
        try:
            outcome = fn(*args, **kwargs)
            result = await outcome if inspect.isawaitable(outcome) else outcome
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result  # type: ignore[return-value]

    def stats(self) -> dict[str, Any]:
        """Return current circuit breaker state and counters."""
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "half_open_max_calls": self.half_open_max_calls,
                "half_open_in_flight": self._half_open_in_flight,
            }
