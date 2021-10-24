from pathlib import Path
from typing import Set


__all__ = [
    'dir_entries'
]


def dir_entries(path: Path, /) -> Set[str]:
    """Gets a set of the names of a directory's entries."""

    return set((e.name for e in path.iterdir()))

