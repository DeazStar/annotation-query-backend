import os

from resilience.circuit_breaker import CircuitBreaker
from resilience.exceptions import ResilienceError, classify_database_exception
from resilience.retry_policy import get_retry_policy


def build_circuit_breaker() -> CircuitBreaker:
    threshold = int(os.getenv("NEO4J_CB_THRESHOLD", "3"))
    recovery_timeout = float(os.getenv("NEO4J_CB_RECOVERY_TIMEOUT", "30"))
    return CircuitBreaker(threshold=threshold, recovery_timeout=recovery_timeout)


def execute_with_resilience(circuit_breaker: CircuitBreaker, func):
    def _attempt():
        try:
            return func()
        except ResilienceError:
            raise
        except Exception as exc:
            raise classify_database_exception(exc, backend="neo4j") from exc
    def _with_retry():
        return get_retry_policy()(_attempt)

    return circuit_breaker.call(_with_retry)
