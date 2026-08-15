class ThreadStopException(Exception):
    def __init__(self, message):
        super().__init__(message)

import requests.exceptions

MORK_TRANSIENT_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ChunkedEncodingError,
)

def is_mork_transient(exc: Exception) -> bool:
    """Returns True if the exception is a temporary MORK network glitch."""
    if isinstance(exc, MORK_TRANSIENT_EXCEPTIONS):
        return True

    if isinstance(exc, requests.exceptions.HTTPError):
        response = getattr(exc, "response", None)
        if response is not None:
            return response.status_code in {429, 500, 502, 503, 504}

    return False
