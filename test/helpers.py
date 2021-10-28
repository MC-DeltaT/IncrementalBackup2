from functools import reduce
from hashlib import md5
from operator import xor
from os import PathLike
from pathlib import Path
from typing import Hashable, Sequence, Set


__all__ = [
    'AssertFilesystemUnmodified',
    'compute_file_hash',
    'compute_filesystem_hash',
    'compute_directory_hash',
    'dir_entries',
    'unordered_equal'
]


class AssertFilesystemUnmodified:
    """Context object that asserts that the content of a path is the same when exiting as when entering."""

    def __init__(self, path: PathLike) -> None:
        self.path = Path(path)

    def __enter__(self):
        self.hash_before = compute_filesystem_hash(self.path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.hash_after = compute_filesystem_hash(self.path)
        assert self.hash_after == self.hash_before


def unordered_equal(sequence1: Sequence[Hashable], sequence2: Sequence[Hashable]) -> bool:
    """Checks if two sequences contain the same items, ignoring ordering."""

    return len(sequence1) == len(sequence2) and set(sequence1) == set(sequence2)


def dir_entries(path: Path, /) -> Set[str]:
    """Gets a set of the names of a directory's entries."""

    return set((e.name for e in path.iterdir()))


def compute_file_hash(path: Path, /) -> bytes:
    """Computes a 16-byte hash of a file that is based on the file's name, contents, and last modified time.
        If a file is not modified, it will always have the same hash value.
    """

    with open(path, 'rb') as file:
        hasher = md5()
        hasher.update(path.name.encode('utf8', 'ignore'))
        hasher.update(str(path.stat().st_mtime).encode('utf8', 'ignore'))
        while True:
            block = file.read(8192)
            if not block:
                break
            hasher.update(block)
        return hasher.digest()


def compute_directory_hash(path: Path, /) -> bytes:
    """Computes a 16-byte hash of a directory and all of its contents. The hash is based on the directory's name and
        file contents (see `compute_file_hash()`).
        If a directory and its contents are not modified, it will always have the same hash value.
    """

    # Methodology is very important.
    # Use of xor on file and subdirectory hashes ensures that the order of enumerating entries doesn't matter
    # (filesystem usually doesn't guarantee any particular order).
    # Combining directory name and child hashes using MD5 ensures that the tree structure of the filesystem is
    # significant (if we just xor'd all file hashes together then the directory structure wouldn't make a difference).
    files = [p for p in path.iterdir() if p.is_file()]
    file_hashes = [int.from_bytes(compute_file_hash(f), 'little', signed=False) for f in files]
    files_hash = reduce(xor, file_hashes, 0).to_bytes(16, 'little', signed=False)
    subdirectories = [p for p in path.iterdir() if p.is_dir()]
    subdirectory_hashes = [int.from_bytes(compute_directory_hash(s), 'little', signed=False) for s in subdirectories]
    subdirectories_hash = reduce(xor, subdirectory_hashes, 0).to_bytes(16, 'little', signed=False)

    hasher = md5()
    hasher.update(path.name.encode('utf8', 'ignore'))
    hasher.update(files_hash)
    hasher.update(subdirectories_hash)
    return hasher.digest()


def compute_filesystem_hash(path: Path, /) -> bytes:
    """Computes a 16-byte deterministic hash of a file or directory.
        See `compute_file_hash()` and `compute_directory_hash()`.
    """

    if path.is_file():
        return compute_file_hash(path)
    elif path.is_dir():
        return compute_directory_hash(path)
    else:
        raise ValueError('Path not file or directory')
