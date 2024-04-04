from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from hashlib import md5
from operator import xor
from os import environ, PathLike, utime
from pathlib import Path
import subprocess
import sys
from typing import Any, Optional, Sequence, Union

from incremental_backup.meta.complete_info import BackupCompleteInfo, write_backup_complete_info_file
from incremental_backup.meta.manifest import BackupManifest, write_backup_manifest_file
from incremental_backup.meta.meta import BackupMetadata, COMPLETE_INFO_FILENAME, create_new_backup_directory, \
    DATA_DIRECTORY_NAME, MANIFEST_FILENAME, START_INFO_FILENAME
from incremental_backup.meta.start_info import BackupStartInfo, write_backup_start_info_file


__all__ = [
    'AssertFilesystemUnmodified',
    'compute_directory_hash',
    'compute_file_hash',
    'compute_filesystem_hash',
    'dir_entries',
    'MakeBackup',
    'run_application',
    'unordered_equal',
    'write_file_with_mtime',
]


class AssertFilesystemUnmodified:
    """Context object that asserts that the content of the specified paths is the same when exiting as when entering."""

    def __init__(self, *paths: Union[str, PathLike[str]]) -> None:
        assert paths
        self.paths = tuple(map(Path, paths))

    def __enter__(self):
        self.hashes_before = tuple(map(self._hash_filesystem, self.paths))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.hashes_after = tuple(map(self._hash_filesystem, self.paths))
        assert self.hashes_after == self.hashes_before

    @staticmethod
    def _hash_filesystem(path: Path):
        """Like compute_filesystem_hash, but returns `None` if the path doesn't exist, to allow checking that a path
            is not created.
        """

        try:
            return compute_filesystem_hash(path)
        except ValueError:
            return None


def unordered_equal(sequence1: Sequence[Any], sequence2: Sequence[Any]) -> bool:
    """Checks if two sequences contain the same items, ignoring ordering."""

    if len(sequence1) != len(sequence2):
        return False

    # Check 1-to-1 correspondence of items between sequences without hashing.
    in_sequence1: list[bool] = [False for _ in range(len(sequence2))]
    for item1 in sequence1:
        for i, item2 in enumerate(sequence2):
            if item1 == item2 and not in_sequence1[i]:
                in_sequence1[i] = True
                break
        else:
            return False
    return True


def dir_entries(path: Path, /) -> set[str]:
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


def write_file_with_mtime(file: Path, contents: str, m_a_time: datetime, encoding: Optional[str] = None) -> None:
    """Writes text to a file and sets the last modified and access times."""

    file.write_text(contents, encoding=encoding)
    timestamp = m_a_time.timestamp()
    utime(file, (timestamp, timestamp))


@dataclass(frozen=True)
class MakeBackup:
    start_info: bool
    manifest: bool
    manifest_copied_files: bool
    manifest_removed_files: bool
    manifest_removed_directories: bool
    complete_info: bool
    data_dir: bool
    data: bool

    @classmethod
    def empty(cls):
        return cls(
            start_info=True,
            manifest=True,
            manifest_copied_files=False,
            manifest_removed_files=False,
            manifest_removed_directories=False,
            complete_info=True,
            data_dir=True,
            data=False
        )

    @classmethod
    def valid(cls, copied_files: bool = True, removed_files: bool = True, removed_directories: bool = True):
        return cls(
            start_info=True,
            manifest=True,
            manifest_copied_files=copied_files,
            manifest_removed_files=removed_files,
            manifest_removed_directories=removed_directories,
            complete_info=True,
            data_dir=True,
            data=copied_files
        )

    @classmethod
    def invalid(cls):
        return cls(
            start_info=True,
            manifest=False,
            manifest_copied_files=False,
            manifest_removed_files=False,
            manifest_removed_directories=False,
            complete_info=False,
            data_dir=True,
            data=False
        )

    def __call__(self, target_directory: Path) -> tuple[Path, BackupMetadata]:
        backup_dir = target_directory / create_new_backup_directory(target_directory)
        
        data_dir = backup_dir / DATA_DIRECTORY_NAME
        if self.data_dir:
            data_dir.mkdir()
            if self.data:
                (data_dir / 'foo.txt').write_text('hello!')

        start_info = BackupStartInfo(datetime.now(timezone.utc))
        if self.start_info:
            write_backup_start_info_file(backup_dir / START_INFO_FILENAME, start_info)

        copied_files = ['foo.txt'] if self.manifest_copied_files else []
        removed_files = ['bar.txt'] if self.manifest_removed_files else []
        removed_directories = ['qux'] if self.manifest_removed_directories else []
        manifest = BackupManifest(
            BackupManifest.Directory('', copied_files, removed_files, removed_directories))
        if self.manifest:
            write_backup_manifest_file(backup_dir / MANIFEST_FILENAME, manifest)
        
        complete_info = BackupCompleteInfo(datetime.now(timezone.utc), False)
        if self.complete_info:
            write_backup_complete_info_file(backup_dir / COMPLETE_INFO_FILENAME, complete_info)

        return backup_dir, BackupMetadata(backup_dir.name, start_info, manifest)


def run_application(*arguments: str) -> subprocess.CompletedProcess[str]:
    """Runs the incremental backup program with the given arguments in a new process and returns the results."""
    
    args = [sys.executable, '-m', 'incremental_backup'] + list(arguments)
    # Some Unicode error if running from a Windows terminal, so we have to force UTF-8 encoding.
    env = environ.copy()
    env['PYTHONIOENCODING'] = 'utf-8'
    return subprocess.run(args, capture_output=True, encoding='utf8', env=env)
