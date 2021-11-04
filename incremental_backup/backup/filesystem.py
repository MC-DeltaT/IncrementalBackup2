from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from os import PathLike
import os.path
from pathlib import Path
from typing import Callable, Iterable, List

from .exclude import ExcludePattern, is_path_excluded


__all__ = [
    'Directory',
    'File',
    'scan_filesystem',
    'ScanFilesystemCallbacks',
    'ScanFilesystemResults'
]


@dataclass
class File:
    name: str
    last_modified: datetime


@dataclass
class Directory:
    name: str
    files: List[File] = field(default_factory=list)
    subdirectories: List['Directory'] = field(default_factory=list)


@dataclass(frozen=True)
class ScanFilesystemCallbacks:
    """Callbacks/hooks for events that occur during `scan_filesystem()`."""

    on_exclude: Callable[[Path], None] = lambda path: None
    """Called when a file or directory is matched by `exclude_patterns` and is excluded."""

    on_listdir_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when an error is raised when requesting the entries in a directory.
        First argument is the directory path, second argument is the raised exception."""

    on_metadata_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when an error is raised when requesting file or directory metadata.
        First argument is the path of the file/directory being queried, second argument is the raised exception."""


@dataclass(frozen=True)
class ScanFilesystemResults:
    tree: Directory
    """Root of the tree representation of the filesystem (the root itself represents the directory requested to be
        scanned)."""

    paths_skipped: bool
    """Indicates if any paths were skipped due to I/O errors (does not include paths matched by exclude patterns)."""


def scan_filesystem(path: PathLike, /, exclude_patterns: Iterable[ExcludePattern],
                    callbacks: ScanFilesystemCallbacks = ScanFilesystemCallbacks()) -> ScanFilesystemResults:
    """Produces a tree representation of the filesystem at a given directory.

        If any paths cannot be accessed for any reason, they will be skipped and excluded from the constructed tree.

        :param path: The path of the directory to scan.
        :param exclude_patterns: Compiled exclude patterns. If a directory or file matches any of these, it and its
            descendents are not included in the scan.
        :param callbacks: Callbacks/hooks for certain events during scanning. See `ScanFilesystemCallbacks`.
    """

    path = Path(path)

    paths_skipped = False
    root = Directory('')
    search_stack: List[Callable[[], None]] = []
    path_segments: List[str] = []
    tree_node_stack = [root]
    is_root = True

    def pop_path_segment() -> None:
        del path_segments[-1]

    def pop_tree_node() -> None:
        del tree_node_stack[-1]

    def visit_directory(search_directory: Path, /) -> None:
        if is_root:
            directory_path = '/'
        else:
            path_segments.append(os.path.normcase(search_directory.name))
            search_stack.append(pop_path_segment)
            directory_path = '/' + '/'.join(path_segments) + '/'

        if is_path_excluded(directory_path, exclude_patterns):
            (callbacks.on_exclude)(search_directory)
        else:
            if is_root:
                tree_node = root
            else:
                # Pretty sure it is impossible to re-enter a directory during the search.
                tree_node = Directory(search_directory.name)
                tree_node_stack[-1].subdirectories.append(tree_node)
                tree_node_stack.append(tree_node)
                search_stack.append(pop_tree_node)

            try:
                children = list(search_directory.iterdir())
            except OSError as e:
                (callbacks.on_listdir_error)(search_directory, e)
            else:
                files: List[File] = []
                subdirectories: List[Path] = []
                for child in children:
                    try:
                        if child.is_file():
                            file_path = directory_path + os.path.normcase(child.name)
                            if is_path_excluded(file_path, exclude_patterns):
                                (callbacks.on_exclude)(child)
                            else:
                                last_modified = datetime.fromtimestamp(os.path.getmtime(child), tz=timezone.utc)
                                files.append(File(child.name, last_modified))
                        elif child.is_dir():
                            subdirectories.append(child)
                    except OSError as e:
                        (callbacks.on_metadata_error)(child, e)

                tree_node.files.extend(files)
                # Need to use partial instead of lambda to avoid name rebinding issues.
                search_stack.extend(partial(visit_directory, d) for d in reversed(subdirectories))

    search_stack.append(partial(visit_directory, path))
    while search_stack:
        search_stack.pop()()
        is_root = False

    return ScanFilesystemResults(root, paths_skipped)
