import sys


__all__ = [
    'print_error',
    'print_warning'
]


def print_error(message: str, /) -> None:
    """Prints an error message to stdout. Should be used for fatal errors."""

    # Don't import stderr from sys, if we do then Pytest output capture won't work.
    print(f'ERROR: {message}', file=sys.stderr, flush=True)


def print_warning(message: str, /) -> None:
    """Prints a warning message to stdout. Should be used for nonfatal errors."""

    print(f'WARNING: {message}', flush=True)
