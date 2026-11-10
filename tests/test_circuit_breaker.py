import time

import pytest
from fastapi.testclient import TestClient

from resilience.api import app
from resilience.circuit_breaker import CircuitBreaker, CircuitOpenError, State
from resilience.patterns import Bulkhead, BulkheadFullError, RetryPolicy, TokenBucketRateLimiter


def test_circuit_breaker_and_half_open_probe_limit() -> None:
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.05, half_open_max_calls=1)
    assert cb.allow_request() is True
    cb.record_failure()
    cb.record_failure()
    assert cb.state == State.OPEN
    assert cb.allow_request() is False

    time.sleep(0.08)
    # First probe transitions to HALF_OPEN; concurrent second probe is rejected while first is active
    assert cb.allow_request() is True
    assert cb.state == State.HALF_OPEN
    assert cb.allow_request() is False

    cb.record_success()
    assert cb.state == State.CLOSED
    assert cb.failure_count == 0


def test_sliding_window_circuit_breaker_and_call_wrapper() -> None:
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0, window_size=4)
    cb.record_failure()
    cb.record_success()
    cb.record_success()
    cb.record_success()
    # Initial failure slid out of window of size 4, so next single failure does NOT open breaker
    cb.record_failure()
    assert cb.state == State.CLOSED

    def boom() -> None:
        raise ValueError("downstream error")

    with pytest.raises(ValueError):
        cb.call(boom)
    assert cb.state == State.OPEN
    with pytest.raises(CircuitOpenError):
        cb.call(lambda: "blocked")


@pytest.mark.asyncio
async def test_retry_backoff_bulkhead_and_rate_limiter() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient fault")
        return "ok"

    policy = RetryPolicy(max_attempts=4, base_delay=0.005, max_delay=0.02, jitter=False)
    assert policy.compute_delay(0) == 0.005
    assert policy.compute_delay(1) == 0.01
    assert await policy.execute_async(flaky) == "ok"
    assert attempts == 3

    bh = Bulkhead(max_concurrent=1)
    with bh.guard():
        assert bh.in_flight == 1
        with pytest.raises(BulkheadFullError):
            with bh.guard():
                pass
    assert bh.in_flight == 0

    limiter = TokenBucketRateLimiter(rate_per_sec=10.0, capacity=2)
    assert limiter.allow() is True
    assert limiter.allow() is True
    assert limiter.allow() is False


def test_http_api_breaker_lifecycle() -> None:
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    assert client.get("/api/v1/breaker/stats").status_code == 200

    # Reset state cleanly via non-failing call or trip fresh
    ok = client.get("/api/v1/proxy/test")
    assert ok.status_code == 200
    assert ok.json()["status"] == "success"

    assert client.get("/api/v1/proxy/test", params={"fail": "true"}).status_code == 200
    assert client.get("/api/v1/proxy/test", params={"fail": "true"}).status_code == 200

    blocked = client.get("/api/v1/proxy/test")
    assert blocked.status_code == 503
