import time
from unittest.mock import Mock

import pytest
from tenacity import Retrying, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from resilience.circuit_breaker import CircuitBreaker, CircuitState
from resilience.exceptions import (
    PersistentDatabaseError,
    TransientDatabaseError,
    classify_neo4j_error,
)


class TestErrorClassification:
    def test_transient_neo_transient_error(self):
        exc = classify_neo4j_error("Neo.TransientError.Transaction.DeadlockDetected")
        assert isinstance(exc, TransientDatabaseError)

    def test_transient_invalid_format(self):
        exc = classify_neo4j_error("Neo.ClientError.Request.InvalidFormat")
        assert isinstance(exc, TransientDatabaseError)

    def test_transient_service_unavailable(self):
        exc = classify_neo4j_error("ServiceUnavailable")
        assert isinstance(exc, TransientDatabaseError)

    def test_persistent_security(self):
        exc = classify_neo4j_error("Neo.ClientError.Security.Unauthorized")
        assert isinstance(exc, PersistentDatabaseError)

    def test_persistent_schema(self):
        exc = classify_neo4j_error("Neo.ClientError.Schema.ConstraintValidationFailed")
        assert isinstance(exc, PersistentDatabaseError)

    def test_persistent_statement(self):
        exc = classify_neo4j_error("Neo.ClientError.Statement.SyntaxError")
        assert isinstance(exc, PersistentDatabaseError)

    def test_empty_code_returns_transient(self):
        exc = classify_neo4j_error("")
        assert isinstance(exc, TransientDatabaseError)

    def test_unknown_code_defaults_to_transient(self):
        exc = classify_neo4j_error("Neo.ClientError.General.Unknown")
        assert isinstance(exc, TransientDatabaseError)


class TestTenacityRetry:
    def test_retries_on_transient_error(self):
        policy = Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=0.01, max=0.1),
            retry=retry_if_exception_type(TransientDatabaseError),
            reraise=True,
        )
        mock_fn = Mock(side_effect=TransientDatabaseError("deadlock"))

        with pytest.raises(TransientDatabaseError):
            policy(mock_fn)

        assert mock_fn.call_count == 3

    def test_no_retry_on_persistent_error(self):
        policy = Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=0.01, max=0.1),
            retry=retry_if_exception_type(TransientDatabaseError),
        )
        mock_fn = Mock(side_effect=PersistentDatabaseError("unauthorized"))

        with pytest.raises(PersistentDatabaseError):
            policy(mock_fn)

        assert mock_fn.call_count == 1

    def test_successful_call_passes_through(self):
        policy = Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential_jitter(initial=0.01, max=0.1),
            retry=retry_if_exception_type(TransientDatabaseError),
        )
        mock_fn = Mock(return_value="result")

        result = policy(mock_fn)

        assert result == "result"
        assert mock_fn.call_count == 1


class TestCircuitBreaker:
    def test_closed_by_default(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_persistent_errors(self):
        cb = CircuitBreaker(threshold=2, recovery_timeout=30)
        mock_fn = Mock(side_effect=PersistentDatabaseError("unauthorized"))

        for _ in range(2):
            with pytest.raises(PersistentDatabaseError):
                cb.call(mock_fn)

        assert cb.state == CircuitState.OPEN
        assert mock_fn.call_count == 2

    def test_rejects_calls_when_open(self):
        cb = CircuitBreaker(threshold=1, recovery_timeout=30)
        mock_fn = Mock(side_effect=PersistentDatabaseError("fail"))

        with pytest.raises(PersistentDatabaseError):
            cb.call(mock_fn)
        assert cb.state == CircuitState.OPEN

        with pytest.raises(PersistentDatabaseError, match="Circuit breaker is OPEN"):
            cb.call(mock_fn)
        assert mock_fn.call_count == 1

    def test_half_open_to_closed_on_success(self):
        cb = CircuitBreaker(threshold=1, recovery_timeout=0.05)
        mock_fn = Mock(side_effect=PersistentDatabaseError("fail"))

        with pytest.raises(PersistentDatabaseError):
            cb.call(mock_fn)
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)

        mock_fn.side_effect = None
        mock_fn.return_value = "success"

        result = cb.call(mock_fn)
        assert result == "success"
        assert cb.state == CircuitState.CLOSED

    def test_half_open_back_to_open_on_failure(self):
        cb = CircuitBreaker(threshold=1, recovery_timeout=0.05)
        mock_fn = Mock(side_effect=PersistentDatabaseError("fail"))

        with pytest.raises(PersistentDatabaseError):
            cb.call(mock_fn)
        assert cb.state == CircuitState.OPEN

        time.sleep(0.06)

        with pytest.raises(PersistentDatabaseError):
            cb.call(mock_fn)
        assert cb.state == CircuitState.OPEN

    def test_failure_count_resets_on_success(self):
        cb = CircuitBreaker(threshold=2, recovery_timeout=30)
        mock_fn = Mock(side_effect=PersistentDatabaseError("fail"))

        with pytest.raises(PersistentDatabaseError):
            cb.call(mock_fn)
        assert cb._failure_count == 1

        mock_fn.side_effect = None
        mock_fn.return_value = "ok"
        cb.call(mock_fn)
        assert cb._failure_count == 0
        assert cb.state == CircuitState.CLOSED

    def test_transient_error_does_not_affect_failure_count(self):
        cb = CircuitBreaker(threshold=2, recovery_timeout=30)
        mock_fn = Mock(side_effect=TransientDatabaseError("transient"))

        for _ in range(5):
            with pytest.raises(TransientDatabaseError):
                cb.call(mock_fn)

        assert cb._failure_count == 0
        assert cb.state == CircuitState.CLOSED
