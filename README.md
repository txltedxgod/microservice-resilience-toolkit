# Microservice Resilience Toolkit

[![CI](https://github.com/txltedxgod/microservice-resilience-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/txltedxgod/microservice-resilience-toolkit/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)

Thread-safe resilience patterns for distributed Python 3.11+ microservices: sliding-window `CircuitBreaker` with bounded `HALF_OPEN` admission control, AWS Full Jitter `RetryPolicy`, concurrency `Bulkhead`, and monotonic `TokenBucketRateLimiter`.

## Testing
```bash
ruff check .
pytest -v tests/
```
