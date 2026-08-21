import time
import pytest

class SimpleCircuitBreaker:
    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 1.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_state_change = time.time()

    def record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            self.last_state_change = time.time()

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

    def allow_request(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if time.time() - self.last_state_change > self.recovery_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
        return True

def test_circuit_breaker_transition():
    cb = SimpleCircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
    assert cb.allow_request()
    cb.record_failure()
    assert cb.allow_request()
    cb.record_failure()
    assert not cb.allow_request()
    time.sleep(0.15)
    assert cb.allow_request()  # Transitions to HALF_OPEN
