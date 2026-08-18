from resilience.circuit_breaker import CircuitBreaker, State

def test_circuit_breaker():
    cb = CircuitBreaker(failure_threshold=2)
    assert cb.allow_request() is True
    cb.record_failure()
    cb.record_failure()
    assert cb.state == State.OPEN
    assert cb.allow_request() is False
