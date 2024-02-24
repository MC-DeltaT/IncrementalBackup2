__all__ = [
    'CommandArgumentError',
    'CommandError',
    'CommandRuntimeError'
]


class CommandError(Exception):
    """High-level, unrecoverable application error. Prefer not use this class itself, create a specific subclass.
        This type of error probably shouldn't be caught except at the highest scope."""

    def __init__(self, message: str) -> None:
        """
            :param message: Description of the error. This is typically printed to the console, so should be informative
                enough for a standard user.
        """

        super().__init__(message)
        self.message = message


class CommandArgumentError(CommandError):
    """Indicates that command line arguments are invalid."""

    def __init__(self, message: str, usage: str) -> None:
        """
            :param message: Specific description of the error.
            :param usage: Program usage information string to display to the user.
        """

        super().__init__(message)
        self.usage = usage


class CommandRuntimeError(CommandError):
    """Indicates an unrecoverable error that can't be detected in advance (e.g. I/O error)."""
