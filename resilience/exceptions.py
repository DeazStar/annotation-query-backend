class ResilienceError(Exception):
    pass


class TransientDatabaseError(ResilienceError):
    pass


class PersistentDatabaseError(ResilienceError):
    pass


class CircuitOpenError(PersistentDatabaseError):
    pass


_TRANSIENT_EXCEPTION_TYPES = (ConnectionError, TimeoutError, OSError)

_TRANSIENT_MESSAGE_MARKERS = (
    "connection",
    "timeout",
    "timed out",
    "unavailable",
    "temporarily",
    "reset",
    "broken pipe",
    "connection refused",
    "try again",
)


def _transient_exception_types():
    types = _TRANSIENT_EXCEPTION_TYPES
    try:
        import requests

        types = types + (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        )
    except ImportError:
        pass
    return types


def _classify_by_network(exc: BaseException) -> ResilienceError | None:
    if isinstance(exc, _transient_exception_types()):
        return TransientDatabaseError(str(exc))

    message = str(exc).lower()
    if any(marker in message for marker in _TRANSIENT_MESSAGE_MARKERS):
        return TransientDatabaseError(str(exc))
    return None


def classify_neo4j_error(error_code: str) -> ResilienceError:
    if not error_code:
        return TransientDatabaseError("Unknown error (empty error code)")

    if error_code.startswith("Neo.TransientError"):
        return TransientDatabaseError(error_code)

    if error_code == "Neo.ClientError.Request.InvalidFormat":
        return TransientDatabaseError(error_code)

    if error_code == "ServiceUnavailable":
        return TransientDatabaseError(error_code)

    if error_code.startswith(
        ("Neo.ClientError.Schema", "Neo.ClientError.Security", "Neo.ClientError.Statement")
    ):
        return PersistentDatabaseError(error_code)

    return TransientDatabaseError(error_code)


def _classify_neo4j_backend(exc: BaseException) -> ResilienceError:
    try:
        from neo4j.exceptions import Neo4jError, ServiceUnavailable, SessionExpired
    except ImportError:
        network = _classify_by_network(exc)
        return network if network else PersistentDatabaseError(str(exc))

    if isinstance(exc, Neo4jError):
        code = getattr(exc, "code", None) or ""
        return classify_neo4j_error(code)
    if isinstance(exc, (ServiceUnavailable, SessionExpired, ConnectionError, TimeoutError, OSError)):
        return TransientDatabaseError(str(exc))

    network = _classify_by_network(exc)
    return network if network else PersistentDatabaseError(str(exc))


def _classify_mork_backend(exc: BaseException) -> ResilienceError:
    network = _classify_by_network(exc)
    return network if network else PersistentDatabaseError(str(exc))


def classify_database_exception(
    exc: BaseException,
    backend: str = "neo4j",
) -> ResilienceError:
    if isinstance(exc, ResilienceError):
        return exc

    if backend == "mork":
        return _classify_mork_backend(exc)
    return _classify_neo4j_backend(exc)


def map_neo4j_exception(exc: BaseException) -> ResilienceError:
    return classify_database_exception(exc, backend="neo4j")


def classify_mork_error(exc: BaseException) -> ResilienceError:
    return classify_database_exception(exc, backend="mork")
