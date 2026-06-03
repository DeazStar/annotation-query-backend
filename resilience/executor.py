from typing import Callable, TypeVar

from resilience.circuit_breaker import CircuitBreaker
from resilience.retry_policy import get_retry_policy

T = TypeVar("T")


def execute_with_resilience(
    operation: Callable[[], T],
    circuit_breaker: CircuitBreaker,
    retry_policy=None,
) -> T:
    retry = retry_policy or get_retry_policy()

    def _attempt() -> T:
        return retry(operation)

    return circuit_breaker.call(_attempt)
