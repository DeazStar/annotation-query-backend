class ThreadStopException(Exception):
    def __init__(self, message):
        super().__init__(message)

import neo4j.exceptions

# Safely aggregate transient exceptions based on the installed driver version
NEO4J_TRANSIENT_EXCEPTIONS = tuple(
    getattr(neo4j.exceptions, name)
    for name in [
        "ServiceUnavailable",
        "TransientError",
        "SessionExpired",
        "ReadServiceUnavailable",
        "WriteServiceUnavailable",
        "ConnectionReadTimeout",
    ]
    if hasattr(neo4j.exceptions, name)
)

def is_neo4j_transient(exc: Exception) -> bool:
    """Returns True if the exception is a temporary Neo4j network/cluster glitch."""
    return isinstance(exc, NEO4J_TRANSIENT_EXCEPTIONS)
