from os import PathLike
import os.path
from typing import Union


__all__ = [
    'path_name_equal',
    'StrPath'
]


StrPath = Union[str, PathLike[str]]


def path_name_equal(name1: str, name2: str, /) -> bool:
    """Checks if two path components are the same, using case sensitivity appropriate for the current system."""

    return os.path.normcase(name1) == os.path.normcase(name2)
