from sys import stderr


__all__ = [
    'print_error',
    'print_warning'
]


def print_error(message: str) -> None:
    """Prints an error message to stdout. Should be used for fatal errors."""

    print(f'ERROR: {message}', file=stderr)


def print_warning(message: str) -> None:
    """Prints a warning message to stdout. Should be used for nonfatal errors."""

    print(f'WARNING: {message}')
