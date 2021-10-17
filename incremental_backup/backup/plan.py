from dataclasses import dataclass, field
from functools import partial
from typing import Callable, List

from .. import filesystem
from ..utility import path_name_equal
from .sum import BackupSum


__all__ = [
    'BackupPlan'
]


@dataclass
class BackupPlan:
    """The data required to perform a backup operation.
        Describes which files are to be copied, as well as information for creating the backup manifest."""

    @dataclass
    class Directory:
        name: str
        copied_files: List[str] = field(default_factory=list)
        removed_files: List[str] = field(default_factory=list)
        removed_directories: List[str] = field(default_factory=list)
        subdirectories: List['BackupPlan.Directory'] = field(default_factory=list)
        contained_copied_files: int = 0
        contained_removed_files: int = 0
        contained_removed_directories: int = 0

    root: Directory = field(default_factory=lambda: BackupPlan.Directory(''))

    @classmethod
    def new(cls, source_tree: filesystem.Directory, backup_sum: BackupSum) -> 'BackupPlan':
        # TODO

        plan = cls()
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

        return plan
