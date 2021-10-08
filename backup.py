import os.path
from dataclasses import dataclass
from datetime import datetime, timezone
import os.path
from pathlib import Path
import re
from typing import Iterable, List, Tuple, Union

from backup_manifest import BackupManifest, prune_backup_manifest
from backup_sum import BackupSum
import filesystem
from utility import path_name_equal


__all__ = [
    'BackupResults',
    'compile_exclude_pattern',
    'compute_backup_plan',
    'do_backup',
    'is_path_excluded',
    'scan_filesystem'
]


@dataclass
class BackupResults:
    paths_skipped: bool
    files_copied: int
    files_removed: int
    directories_removed: int


def do_backup(source_path: Path, target_path: Path, exclude_patterns: Iterable[re.Pattern],
              backup_sum: BackupSum) -> BackupResults:
    results = BackupResults(False, 0, 0, 0)
    source_tree, paths_skipped = scan_filesystem(source_path, exclude_patterns)
    results.paths_skipped |= paths_skipped
    backup_manifest = compute_backup_plan(source_tree, backup_sum)
    # TODO
    return results


def compute_backup_plan(source_tree: filesystem.Directory, backup_sum: BackupSum) -> BackupManifest:
    manifest = BackupManifest()
    search_stack: List[Union[filesystem.Directory, None]] = [source_tree]
    path: List[str] = []
    manifest_stack = [manifest.root]
    is_root = True
    while search_stack:
        search_directory = search_stack.pop()
        if search_directory is None:
            manifest_stack.pop()
            path.pop()
        else:
            if is_root:
                manifest_directory = manifest.root
            else:
                manifest_directory = next(
                    (d for d in manifest_stack[-1] if path_name_equal(d.name, search_directory.name)), None)
                if manifest_directory is None:
                    manifest_directory = BackupManifest.Directory(search_directory.name)
                    manifest_stack[-1].subdirectories.append(manifest_directory)
                path.append(search_directory.name)

            backup_sum_directory = backup_sum.find_directory(path)
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

            search_stack.append(None)
            search_stack.extend(reversed(search_directory.subdirectories))
        is_root = False

    prune_backup_manifest(manifest)
    return manifest


def scan_filesystem(path: Path, exclude_patterns: Iterable[re.Pattern]) -> Tuple[filesystem.Directory, bool]:
    paths_skipped = False
    root = filesystem.Directory('')
    search_stack: List[Union[Path, str]] = [path]
    tree_node_stack = [root]
    path_segments: List[str] = []
    is_root = True
    while search_stack:
        search_node = search_stack.pop()
        # TODO? change to using functions as search nodes?
        if isinstance(search_node, str):
            if search_node == 'pop_tree_node':
                tree_node_stack.pop()
            elif search_node == 'pop_path_segment':
                path_segments.pop()
            else:
                raise AssertionError(f'Unexpected search node {repr(search_node)}')
        else:
            if is_root:
                directory_path = '/'
            else:
                path_segments.append(os.path.normcase(search_node.name))
                search_stack.append('pop_path_segment')
                directory_path = '/' + '/'.join(path_segments) + '/'

            directory_excluded = is_path_excluded(directory_path, exclude_patterns)

            # TODO: user feedback for excluded path

            if not directory_excluded:
                if is_root:
                    tree_node = root
                else:
                    tree_node = filesystem.Directory(search_node.name)
                    tree_node_stack[-1].subdirectories.append(tree_node)
                    tree_node_stack.append(tree_node)
                    search_stack.append('pop_tree_node')

                files: List[filesystem.File] = []
                subdirectories: List[Path] = []
                # TODO: error: failed to enumerate directory, skipping
                children = list(search_node.iterdir())
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
                search_stack.extend(reversed(subdirectories))
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
