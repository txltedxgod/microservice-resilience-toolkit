"""FastAPI service demonstrating Circuit Breaker, Bulkhead, and Retry resilience patterns."""

from typing import Any

from fastapi import FastAPI, HTTPException

from resilience.circuit_breaker import CircuitBreaker
from resilience.patterns import Bulkhead, TokenBucketRateLimiter

app = FastAPI(title="Microservice Resilience Toolkit", version="1.0.0")
cb = CircuitBreaker(failure_threshold=2, recovery_timeout=3.0)
bulkhead = Bulkhead(max_concurrent=16)
rate_limiter = TokenBucketRateLimiter(rate_per_sec=100.0, capacity=200)


@app.get("/health", tags=["Monitoring"])
def health_check() -> dict[str, Any]:
    """Liveness check returning circuit breaker and bulkhead telemetry."""
    return {
        "status": "healthy",
        "circuit_breaker": cb.stats(),
        "bulkhead_in_flight": bulkhead.in_flight,
    }


@app.get("/api/v1/proxy/test", tags=["Resilience"])
def test_call(fail: bool = False) -> dict[str, str]:
    """Simulate downstream call guarded by rate limiter, bulkhead, and circuit breaker."""
    if not rate_limiter.allow():
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    if not cb.allow_request():
        raise HTTPException(status_code=503, detail="Circuit Breaker is OPEN")
    with bulkhead.guard():
        if fail:
            cb.record_failure()
            return {"status": "failed, recorded failure"}
        cb.record_success()
        return {"status": "success"}


@app.get("/api/v1/breaker/stats", tags=["Telemetry"])
def breaker_stats() -> dict[str, Any]:
    """Return current circuit breaker metrics."""
    return cb.stats()
