from fastapi import FastAPI, HTTPException
from resilience.circuit_breaker import CircuitBreaker

app = FastAPI(title="Microservice Resilience Toolkit", version="0.1.0")
cb = CircuitBreaker(failure_threshold=2, recovery_timeout=3.0)

@app.get("/api/v1/proxy/test")
def test_call(fail: bool = False):
    if not cb.allow_request():
        raise HTTPException(status_code=503, detail="Circuit Breaker is OPEN")
    if fail:
        cb.record_failure()
        return {"status": "failed, recorded failure"}
    cb.record_success()
    return {"status": "success"}
