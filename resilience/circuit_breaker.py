import time
import logging
import threading
from enum import Enum

from resilience.exceptions import CircuitOpenError, PersistentDatabaseError

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, threshold: int = 3, recovery_timeout: float = 30.0):
        self._threshold = threshold
        self._recovery_timeout = recovery_timeout
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def _should_attempt_recovery(self) -> bool:
        return time.monotonic() - self._last_failure_time >= self._recovery_timeout

    def call(self, func, *args, **kwargs):
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_recovery():
                    self._state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker: OPEN -> HALF_OPEN (attempting recovery)")
                else:
                    raise CircuitOpenError("Circuit breaker is OPEN; call rejected")

        try:
            result = func(*args, **kwargs)
        except PersistentDatabaseError:
            with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.monotonic()
                if self._state == CircuitState.HALF_OPEN:
                    logger.warning("Circuit breaker: HALF_OPEN -> OPEN (recovery failed)")
                    self._state = CircuitState.OPEN
                elif self._failure_count >= self._threshold:
                    logger.warning("Circuit breaker: CLOSED -> OPEN (threshold reached)")
                    self._state = CircuitState.OPEN
            raise
        else:
            with self._lock:
                if self._state == CircuitState.HALF_OPEN:
                    logger.info("Circuit breaker: HALF_OPEN -> CLOSED (recovery succeeded)")
                    self._state = CircuitState.CLOSED
                self._failure_count = 0
            return result
