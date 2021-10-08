import os.path


__all__ = [
    'path_name_equal'
]


def path_name_equal(name1: str, name2: str) -> bool:
    """Checks if two path components are the same, using case sensitivity appropriate for the current system."""

    return os.path.normcase(name1) == os.path.normcase(name2)
