from abc import ABC, abstractmethod


class PathsBackendInterface(ABC):
    """Backend contract for retrieving candidate paths."""

    @abstractmethod
    def find_paths(self, request: dict) -> dict:
        """Return backend candidates without applying presentation policy."""
        raise NotImplementedError
