from abc import ABC


__all__ = [
    'FatalArgumentError',
    'FatalError',
    'FatalRuntimeError'
]


class FatalError(ABC, Exception):
    """High-level, unrecoverable application error. Prefer not use this class itself, create a specific subclass.
        This type of error probably shouldn't be caught except at the highest scope."""

    def __init__(self, message: str) -> None:
        """
            :param message: Description of the error. This is typically printed to the console, so should be informative
                enough for a standard user.
        """

        super().__init__(message)
        self.message = message


class FatalArgumentError(FatalError):
    """Indicates that command line arguments are invalid."""

    # TODO: usage info


class FatalRuntimeError(FatalError):
    """Indicates an unrecoverable error only detectable at runtime (e.g. I/O error)."""
