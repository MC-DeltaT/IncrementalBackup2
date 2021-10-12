import os.path
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
import os.path
from pathlib import Path
import re
import shutil
from typing import Callable, Iterable, List, Tuple

from . import filesystem
from .backup_meta import BackupManifest, BackupSum, prune_backup_manifest
from .utility import path_name_equal


__all__ = [
    'BackupResults',
    'compile_exclude_pattern',
    'compute_backup_plan',
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


def do_backup(source_path: Path, destination_path: Path, exclude_patterns: Iterable[re.Pattern],
              backup_sum: BackupSum) -> Tuple[BackupResults, BackupManifest]:
    source_tree, paths_skipped = scan_filesystem(source_path, exclude_patterns)
    backup_plan = compute_backup_plan(source_tree, backup_sum)
    results, manifest = execute_backup_plan(backup_plan, source_path, destination_path)
    results.paths_skipped |= paths_skipped
    return results, manifest


def execute_backup_plan(backup_plan: BackupManifest, source_path: Path, destination_path: Path) \
        -> Tuple[BackupResults, BackupManifest]:
    results = BackupResults(False, 0, 0)
    search_stack: List[Callable[[], None]] = []
    path_segments: List[str] = []
    is_root = True

    def pop_path_segment() -> None:
        del path_segments[-1]

    def visit_directory(search_directory: BackupManifest.Directory) -> None:
        if not is_root:
            path_segments.append(search_directory.name)
            search_stack.append(pop_path_segment)

        relative_directory_path = Path(*path_segments)
        destination_directory_path = destination_path / relative_directory_path

        try:
            destination_directory_path.mkdir(parents=True, exist_ok=True)
        except OSError:
            # TODO: user feedback

            # TODO? Remove directory from manifest completely?
            search_directory.copied_files = []
            search_directory.removed_files = []
            search_directory.removed_directories = []
            search_directory.subdirectories = []
        else:
            for file in search_directory.copied_files:
                relative_file_path = relative_directory_path / file
                source_file_path = source_path / relative_file_path
                destination_file_path = destination_path / relative_file_path
                # TODO: error handling
                shutil.copy2(source_file_path, destination_file_path)
                results.files_copied += 1

            results.files_removed += search_directory.removed_files

            # Need to use partial instead of lambda to avoid name rebinding issues.
            search_stack.extend(partial(visit_directory, d) for d in search_directory.subdirectories)

    search_stack.append(partial(visit_directory, backup_plan.root))
    while search_stack:
        search_stack.pop()()
        is_root = False

    return results, backup_plan


def compute_backup_plan(source_tree: filesystem.Directory, backup_sum: BackupSum) -> BackupManifest:
    """Computes a tentative/planned backup manifest ("backup plan") for a backup operation given the current source file
        tree and previous backup sum."""

    manifest = BackupManifest()
    search_stack: List[Callable[[], None]] = []
    path_segments: List[str] = []
    manifest_stack = [manifest.root]
    is_root = True

    def pop_path_segment() -> None:
        del path_segments[-1]

    def pop_manifest_node() -> None:
        del manifest_stack[-1]

    def visit_directory(search_directory: filesystem.Directory) -> None:
        if is_root:
            manifest_directory = manifest.root
        else:
            manifest_directory = next(
                (d for d in manifest_stack[-1].subdirectories if path_name_equal(d.name, search_directory.name)), None)
            if manifest_directory is None:
                manifest_directory = BackupManifest.Directory(search_directory.name)
                manifest_stack[-1].subdirectories.append(manifest_directory)
                manifest_stack.append(manifest_directory)
                search_stack.append(pop_manifest_node)
            path_segments.append(search_directory.name)
            search_stack.append(pop_path_segment)

        backup_sum_directory = backup_sum.find_directory(path_segments)
        if backup_sum_directory is None:
            # Nothing backed up here so far, only possibility is new files to back up.
            manifest_directory.copied_files.extend(f.name for f in search_directory.files)
        else:
            # Something backed up here before, could have new files, modified files, removed files, removed
            # subdirectories.

            for current_file in search_directory.files:
                backed_up_file = next(
                    (f for f in backup_sum_directory.files if path_name_equal(f.name, current_file.name)), None)
                # File never backed up or modified since last backup.
                if backed_up_file is None \
                        or current_file.last_modified > backed_up_file.last_backup.start_info.start_time:
                    manifest_directory.copied_files.append(current_file.name)

            manifest_directory.removed_files.extend(
                f.name for f in backup_sum_directory.files
                if not any(path_name_equal(f.name, f2.name) for f2 in search_directory.files))

            manifest_directory.removed_directories.extend(
                d.name for d in backup_sum_directory.subdirectories
                if not any(path_name_equal(d.name, d2.name) for d2 in search_directory.subdirectories))

        # Need to use partial instead of lambda to avoid name rebinding issues.
        search_stack.extend(partial(visit_directory, d) for d in reversed(search_directory.subdirectories))

    search_stack.append(partial(visit_directory, source_tree))
    while search_stack:
        search_stack.pop()()
        is_root = False

    prune_backup_manifest(manifest)
    return manifest


def scan_filesystem(path: Path, exclude_patterns: Iterable[re.Pattern]) -> Tuple[filesystem.Directory, bool]:
    """Produces a tree representation of the filesystem at a given directory.

        :param path: The path of the directory to scan.
        :param exclude_patterns: Compiled exclude patterns. If a directory or file matches any of these, it and its
            descendents are not included in the scan.
        :return: First element is the root of the tree representation of the filesystem (the root represents the
            directory at `path`). Second element indicates if any paths were skipped due to I/O errors (does not include
            paths matched by `exclude_patterns`).
    """

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

        directory_excluded = is_path_excluded(directory_path, exclude_patterns)

        # TODO: user feedback for excluded path

        if not directory_excluded:
            if is_root:
                tree_node = root
            else:
                tree_node = filesystem.Directory(search_directory.name)
                tree_node_stack[-1].subdirectories.append(tree_node)
                tree_node_stack.append(tree_node)
                search_stack.append(pop_tree_node)

            files: List[filesystem.File] = []
            subdirectories: List[Path] = []
            # TODO: error: failed to enumerate directory, skipping
            children = list(search_directory.iterdir())
            for child in children:
                # TODO: error: failed to access file metadata, skipping
                if child.is_file():
                    file_path = directory_path + os.path.normcase(child.name)
                    if not is_path_excluded(file_path, exclude_patterns):
                        last_modified = datetime.fromtimestamp(os.path.getmtime(child), tz=timezone.utc)
                        files.append(filesystem.File(child.name, last_modified))
                elif child.is_dir():
                    subdirectories.append(child)

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
