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
        contains_copied_files: bool = False
        """Indicates if this directory or any of its descendents contain any copied files."""
        contains_removed_items: bool = False
        """Indicates if this directory or any of its descendents contain any removed files or removed directories."""

    root: Directory = field(default_factory=lambda: BackupPlan.Directory(''))

    @classmethod
    def new(cls, source_tree: filesystem.Directory, backup_sum: BackupSum) -> 'BackupPlan':
        """Constructs a backup plan from the current backup source directory state and previous backup sum."""

        plan = cls()
        plan_directories = [plan.root]

        search_stack: List[Callable[[], None]] = []
        path_segments: List[str] = []
        plan_stack = [plan.root]
        is_root = True

        def pop_path_segment() -> None:
            del path_segments[-1]

        def pop_plan_node() -> None:
            del plan_stack[-1]

        def visit_directory(search_directory: filesystem.Directory) -> None:
            if is_root:
                plan_directory = plan.root
            else:
                # Assume filesystem tree doesn't re-enter the same directory. I think this will never happen, if it does
                # then I don't think it will cause real issues, just yield unoptimised tree structure.
                plan_directory = cls.Directory(search_directory.name)
                plan_stack[-1].subdirectories.append(plan_directory)
                plan_stack.append(plan_directory)
                search_stack.append(pop_plan_node)
                plan_directories.append(plan_directory)
                path_segments.append(search_directory.name)
                search_stack.append(pop_path_segment)

            # TODO? simultaneously traverse filesystem tree and backup sum tree to avoid searching backup sum every time
            backup_sum_directory = backup_sum.find_directory(path_segments)
            if backup_sum_directory is None:
                # Nothing backed up here so far, only possibility is new files to back up.
                plan_directory.copied_files.extend(f.name for f in search_directory.files)
            else:
                # Something backed up here before, could have new files, modified files, removed files, removed
                # subdirectories.

                for current_file in search_directory.files:
                    backed_up_file = next(
                        (f for f in backup_sum_directory.files if path_name_equal(f.name, current_file.name)), None)
                    # File never backed up or modified since last backup.
                    if backed_up_file is None \
                            or current_file.last_modified > backed_up_file.last_backup.start_info.start_time:
                        plan_directory.copied_files.append(current_file.name)

                plan_directory.removed_files.extend(
                    f.name for f in backup_sum_directory.files
                    if not any(path_name_equal(f.name, f2.name) for f2 in search_directory.files))

                plan_directory.removed_directories.extend(
                    d.name for d in backup_sum_directory.subdirectories
                    if not any(path_name_equal(d.name, d2.name) for d2 in search_directory.subdirectories))

            # Need to use partial instead of lambda to avoid name rebinding issues.
            search_stack.extend(partial(visit_directory, d) for d in reversed(search_directory.subdirectories))

        search_stack.append(partial(visit_directory, source_tree))
        while search_stack:
            search_stack.pop()()
            is_root = False

        # Calculate contains_copied_files and contained_removed_items, and prune empty directories.
        for directory in reversed(plan_directories):
            directory.contains_copied_files = len(directory.copied_files) > 0
            directory.contains_removed_items = len(directory.removed_files) + len(directory.removed_directories) > 0
            nonempty_subdirectories: List[BackupPlan.Directory] = []
            for subdirectory in directory.subdirectories:
                # Ok, values for child are always calculated before parent.
                directory.contains_copied_files |= subdirectory.contains_copied_files
                directory.contains_removed_items |= subdirectory.contains_removed_items
                if subdirectory.contains_copied_files or subdirectory.contains_removed_items:
                    nonempty_subdirectories.append(subdirectory)
            directory.subdirectories = nonempty_subdirectories

        return plan
