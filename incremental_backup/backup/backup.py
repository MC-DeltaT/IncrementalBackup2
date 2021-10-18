import os.path
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
import os.path
from pathlib import Path
import re
import shutil
from typing import Callable, Iterable, List, Optional, Tuple

from .. import filesystem
from ..meta import BackupManifest
from .plan import BackupPlan
from .sum import BackupSum


__all__ = [
    'BackupResults',
    'compile_exclude_pattern',
    'do_backup',
    'execute_backup_plan',
    'is_path_excluded',
    'scan_filesystem'
]


@dataclass
class BackupResults:
    paths_skipped: bool
    files_copied: int
    files_removed: int


def do_backup(source_path: Path, destination_path: Path, exclude_patterns: Iterable[re.Pattern], backup_sum: BackupSum,
              on_exclude: Optional[Callable[[Path], None]] = None,
              on_listdir_error: Optional[Callable[[Path, OSError], None]] = None,
              on_metadata_error: Optional[Callable[[Path, OSError], None]] = None,
              on_mkdir_error: Optional[Callable[[Path, OSError], None]] = None,
              on_copy_error: Optional[Callable[[Path, Path, OSError], None]] = None) \
        -> Tuple[BackupResults, BackupManifest]:
    """Performs a backup operation, consisting of scanning the source filesystem, constructing the backup plan, and
        enacting the backup plan.

        Please see `scan_filesystem()`, `BackupPlan`, and `execute_backup_plan()` for more details.
    """

    source_tree, paths_skipped = scan_filesystem(
        source_path, exclude_patterns, on_exclude, on_listdir_error, on_metadata_error)
    backup_plan = BackupPlan.new(source_tree, backup_sum)
    results, manifest = execute_backup_plan(backup_plan, source_path, destination_path, on_mkdir_error, on_copy_error)
    results.paths_skipped |= paths_skipped
    return results, manifest


def execute_backup_plan(backup_plan: BackupPlan, source_path: Path, destination_path: Path,
                        on_mkdir_error: Optional[Callable[[Path, OSError], None]] = None,
                        on_copy_error: Optional[Callable[[Path, Path, OSError], None]] = None) \
        -> Tuple[BackupResults, BackupManifest]:
    """Enacts a backup plan, copying files and creating the backup manifest.

        If a file cannot be backup up (i.e. copied), it is ignored and excluded from the manifest.

        If a directory cannot be created, no files will be backed up into it or its (planned) child directories.
        Any files planned to be backed up within it will not be copied and will be excluded from the manifest.
        However, any removed files or directories within it will still be recorded in the manifest.

        :param backup_plan: The backup plan to enact. Should be based off `source_path`, otherwise the results will be
            nonsense.
        :param source_path: The backup source directory; where files are copied from.
        :param destination_path: The location to copy files to. Files directly contained in the backup source directory
            will become directly contained by this directory.
        :param on_mkdir_error: Called when an error is raised creating a directory.
        :param on_copy_error: Called when an error is raised copying a file.
        :return: First element is the backup results, second element is the backup manifest.
    """

    if on_mkdir_error is None:
        on_mkdir_error = lambda p, e: None
    if on_copy_error is None:
        on_copy_error = lambda s, d, e: None

    results = BackupResults(False, 0, 0)
    manifest = BackupManifest()
    search_stack: List[Callable[[], None]] = []
    manifest_stack = [manifest.root]
    path_segments: List[str] = []
    is_root = True

    def pop_manifest_node() -> None:
        del manifest_stack[-1]

    def pop_path_segment() -> None:
        del path_segments[-1]

    def visit_directory(search_directory: BackupPlan.Directory, mkdir_failed: bool) -> None:
        if is_root:
            manifest_directory = manifest.root
        else:
            path_segments.append(search_directory.name)
            search_stack.append(pop_path_segment)

            manifest_directory = BackupManifest.Directory(search_directory.name)
            manifest_stack[-1].subdirectories.append(manifest_directory)
            manifest_stack.append(manifest_directory)
            search_stack.append(pop_manifest_node)

        manifest_directory.removed_files = search_directory.removed_files
        results.files_removed += len(search_directory.removed_files)
        manifest_directory.removed_directories = search_directory.removed_directories

        # Once we fail to create a destination directory, or the current directory doesn't contain any more files to
        # copy, no need to try to create the destination directory or copy any files.
        if (not mkdir_failed) and search_directory.contains_copied_files:
            relative_directory_path = Path(*path_segments)
            destination_directory_path = destination_path / relative_directory_path

            try:
                destination_directory_path.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                results.paths_skipped = True
                mkdir_failed = True

                on_mkdir_error(destination_directory_path, e)
            else:
                for file in search_directory.copied_files:
                    relative_file_path = relative_directory_path / file
                    source_file_path = source_path / relative_file_path
                    destination_file_path = destination_path / relative_file_path
                    try:
                        shutil.copy2(source_file_path, destination_file_path)
                    except OSError as e:
                        results.paths_skipped = True
                        on_copy_error(source_file_path, destination_file_path, e)
                    else:
                        manifest_directory.copied_files.append(file)
                        results.files_copied += 1

        # Keep searching through child directories if:
        #   a) destination directory was created successfully, or
        #   b) destination directory creation failed, but there are still removed items to be recorded in the manifest.
        # Need to use partial instead of lambda to avoid name rebinding issues.
        search_stack.extend(partial(visit_directory, d, mkdir_failed) for d in reversed(search_directory.subdirectories)
                            if not mkdir_failed or d.contains_removed_items)

    search_stack.append(partial(visit_directory, backup_plan.root, False))
    while search_stack:
        search_stack.pop()()
        is_root = False

    return results, manifest


def scan_filesystem(path: Path, exclude_patterns: Iterable[re.Pattern],
                    on_exclude: Optional[Callable[[Path], None]] = None,
                    on_listdir_error: Optional[Callable[[Path, OSError], None]] = None,
                    on_metadata_error: Optional[Callable[[Path, OSError], None]] = None) \
        -> Tuple[filesystem.Directory, bool]:
    """Produces a tree representation of the filesystem at a given directory.

        If any paths cannot be accessed for any reason, they will be skipped and excluded from the constructed tree.

        :param path: The path of the directory to scan.
        :param exclude_patterns: Compiled exclude patterns. If a directory or file matches any of these, it and its
            descendents are not included in the scan.
        :param on_exclude: Called when a file or directory is matched by `exclude_patterns` and is excluded.
        :param on_listdir_error: Called when an error is raised when requesting the entries in a directory.
        :param on_metadata_error: Called when an error is raised when requesting file or directory metadata.
        :return: First element is the root of the tree representation of the filesystem (the root represents the
            directory at `path`). Second element indicates if any paths were skipped due to I/O errors (does not include
            paths matched by `exclude_patterns`).
    """

    if on_exclude is None:
        on_exclude = lambda p: None
    if on_listdir_error is None:
        on_listdir_error = lambda p, e: None
    if on_metadata_error is None:
        on_metadata_error = lambda p, e: None

    paths_skipped = False
    root = filesystem.Directory('')
    search_stack: List[Callable[[], None]] = []
    path_segments: List[str] = []
    tree_node_stack = [root]
    is_root = True

    def pop_path_segment() -> None:
        del path_segments[-1]

    def pop_tree_node() -> None:
        del tree_node_stack[-1]

    def visit_directory(search_directory: Path) -> None:
        if is_root:
            directory_path = '/'
        else:
            path_segments.append(os.path.normcase(search_directory.name))
            search_stack.append(pop_path_segment)
            directory_path = '/' + '/'.join(path_segments) + '/'

        if is_path_excluded(directory_path, exclude_patterns):
            on_exclude(search_directory)
        else:
            if is_root:
                tree_node = root
            else:
                # TODO? need to handle re-entering directories?

                tree_node = filesystem.Directory(search_directory.name)
                tree_node_stack[-1].subdirectories.append(tree_node)
                tree_node_stack.append(tree_node)
                search_stack.append(pop_tree_node)

            try:
                children = list(search_directory.iterdir())
            except OSError as e:
                on_listdir_error(search_directory, e)
            else:
                files: List[filesystem.File] = []
                subdirectories: List[Path] = []
                for child in children:
                    try:
                        if child.is_file():
                            file_path = directory_path + os.path.normcase(child.name)
                            if is_path_excluded(file_path, exclude_patterns):
                                on_exclude(child)
                            else:
                                last_modified = datetime.fromtimestamp(os.path.getmtime(child), tz=timezone.utc)
                                files.append(filesystem.File(child.name, last_modified))
                        elif child.is_dir():
                            subdirectories.append(child)
                    except OSError as e:
                        on_metadata_error(child, e)

                tree_node.files.extend(files)
                # Need to use partial instead of lambda to avoid name rebinding issues.
                search_stack.extend(partial(visit_directory, d) for d in reversed(subdirectories))

    search_stack.append(partial(visit_directory, path))
    while search_stack:
        search_stack.pop()()
        is_root = False

    return root, paths_skipped


def compile_exclude_pattern(pattern: str) -> re.Pattern:
    """Compiles a path exclude pattern provided by the user into a regex object (for performance reasons, so we don't
        have to recompile patterns on every call to `is_path_excluded()`."""

    return re.compile(pattern, re.DOTALL)


def is_path_excluded(path: str, exclude_patterns: Iterable[re.Pattern]) -> bool:
    """Checks if a path is matched by any path exclude pattern.

        :param path: The path in question. Should be an absolute POSIX-style path, where the root is the backup source
            directory. Paths that are directories should end in a forward slash ('/'). Paths that are files should not
            end in a forward slash. Path components should be normalised with `os.path.normcase()`.
        :param exclude_patterns: Compiled path exclude patterns, from `compile_exclude_pattern()`.
    """

    return any(pattern.fullmatch(path) for pattern in exclude_patterns)
