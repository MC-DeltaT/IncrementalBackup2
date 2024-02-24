from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
import shutil
from typing import Callable, Optional

from incremental_backup.backup import filesystem
from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta import BackupManifest
from incremental_backup._utility import path_name_equal, StrPath


__all__ = [
    'BackupPlan',
    'execute_backup_plan',
    'ExecuteBackupPlanCallbacks',
    'ExecuteBackupPlanResults'
]


@dataclass
class BackupPlan:
    """The data required to perform a backup operation.
        Describes which files are to be copied, as well as information for creating the backup manifest."""

    @dataclass
    class Directory:
        name: str
        copied_files: list[str] = field(default_factory=list)
        removed_files: list[str] = field(default_factory=list)
        removed_directories: list[str] = field(default_factory=list)
        subdirectories: list['BackupPlan.Directory'] = field(default_factory=list)
        contains_copied_files: bool = False
        """Indicates if this directory or any of its descendents contain any copied files."""
        contains_removed_items: bool = False
        """Indicates if this directory or any of its descendents contain any removed files or removed directories."""
        removed_directory_file_count: int = 0
        """The total number of files previously contained in the removed subdirectories."""

    root: Directory = field(default_factory=lambda: BackupPlan.Directory(''))

    @classmethod
    def new(cls, source_tree: filesystem.Directory, backup_sum: BackupSum) -> 'BackupPlan':
        """Constructs a backup plan from the backup source directory state and previous backup sum."""

        plan = cls()
        plan_directories = [plan.root]

        search_stack: list[Callable[[], None]] = []
        backup_sum_stack: list[Optional[BackupSum.Directory]] = [backup_sum.root]
        plan_stack = [plan.root]
        is_root = True

        def pop_backup_sum_node() -> None:
            del backup_sum_stack[-1]

        def pop_plan_node() -> None:
            del plan_stack[-1]

        def visit_directory(search_directory: filesystem.Directory, /) -> None:
            if is_root:
                plan_directory = plan.root
                backup_sum_directory = backup_sum.root
            else:
                # Assume filesystem tree doesn't re-enter the same directory. I think this will never happen, if it does
                # then I don't think it will cause real issues, just yield unoptimised tree structure.
                plan_directory = cls.Directory(search_directory.name)
                plan_stack[-1].subdirectories.append(plan_directory)
                plan_stack.append(plan_directory)
                search_stack.append(pop_plan_node)
                plan_directories.append(plan_directory)

                if backup_sum_stack[-1] is None:
                    backup_sum_directory = None
                else:
                    backup_sum_directory = next(
                        (d for d in backup_sum_stack[-1].subdirectories
                         if path_name_equal(d.name, search_directory.name)), None)
                backup_sum_stack.append(backup_sum_directory)
                search_stack.append(pop_backup_sum_node)

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
                    if (backed_up_file is None
                            or current_file.last_modified > backed_up_file.last_backup.start_info.start_time):
                        plan_directory.copied_files.append(current_file.name)

                plan_directory.removed_files.extend(
                    f.name for f in backup_sum_directory.files
                    if not any(path_name_equal(f.name, f2.name) for f2 in search_directory.files))

                removed_directories = \
                    [d for d in backup_sum_directory.subdirectories
                     if not any(path_name_equal(d.name, d2.name) for d2 in search_directory.subdirectories)]
                plan_directory.removed_directories.extend(d.name for d in removed_directories)
                plan_directory.removed_directory_file_count = sum(d.count_contained_files() for d in removed_directories)

            # Need to use partial instead of lambda to avoid name rebinding issues.
            search_stack.extend(partial(visit_directory, d) for d in reversed(search_directory.subdirectories))

        search_stack.append(partial(visit_directory, source_tree))
        while search_stack:
            search_stack.pop()()
            is_root = False

        # Calculate contains_copied_files and contains_removed_items, and prune empty directories.
        for directory in reversed(plan_directories):
            directory.contains_copied_files = len(directory.copied_files) > 0
            directory.contains_removed_items = len(directory.removed_files) + len(directory.removed_directories) > 0
            nonempty_subdirectories: list[BackupPlan.Directory] = []
            for subdirectory in directory.subdirectories:
                # Ok, values for child are always calculated before parent.
                directory.contains_copied_files |= subdirectory.contains_copied_files
                directory.contains_removed_items |= subdirectory.contains_removed_items
                if subdirectory.contains_copied_files or subdirectory.contains_removed_items:
                    nonempty_subdirectories.append(subdirectory)
            directory.subdirectories = nonempty_subdirectories

        return plan


@dataclass(frozen=True)
class ExecuteBackupPlanResults:
    """Return results of `execute_backup_plan()`."""

    manifest: BackupManifest
    paths_skipped: bool
    files_copied: int
    files_removed: int


@dataclass(frozen=True)
class ExecuteBackupPlanCallbacks:
    """Callbacks for events that occur in `execute_backup_plan()`."""

    on_mkdir_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when an error is raised creating a directory.
        First argument is the directory path, second argument is the raised exception."""

    on_copy_error: Callable[[Path, Path, OSError], None] = lambda src, dest, error: None
    """Called when an error is raised copying a file.
        First argument is the source path, second argument is the destination path, third argument is the raised
        exception."""


def execute_backup_plan(backup_plan: BackupPlan, source_directory: StrPath, destination_directory: StrPath,
                        callbacks: ExecuteBackupPlanCallbacks = ExecuteBackupPlanCallbacks()) \
        -> ExecuteBackupPlanResults:
    """Enacts a backup plan, copying files and creating the backup manifest.

        If a file cannot be backup up (i.e. copied), it is ignored and excluded from the manifest.

        If a directory cannot be created, no files will be backed up into it or its (planned) child directories.
        Any files planned to be backed up within it will not be copied and will be excluded from the manifest.
        However, any removed files or directories within it will still be recorded in the manifest.

        :param backup_plan: The backup plan to enact. Should be based off `source_directory`, otherwise the results will
            be nonsense.
        :param source_directory: The backup source directory; where files are copied from.
        :param destination_directory: The location to copy files to. Need not exist. This directory itself represents
            the backup source directory.
        :param callbacks: Callbacks for certain events during execution. See `ExecuteBackupPlanCallbacks`.
    """

    manifest = BackupManifest()
    paths_skipped = False
    files_copied = 0
    files_removed = 0
    search_stack: list[Callable[[], None]] = []
    manifest_stack = [manifest.root]
    path_segments: list[str] = []
    is_root = True

    def pop_manifest_node() -> None:
        del manifest_stack[-1]

    def pop_path_segment() -> None:
        del path_segments[-1]

    def visit_directory(search_directory: BackupPlan.Directory, /, mkdir_failed: bool) -> None:
        nonlocal paths_skipped
        nonlocal files_copied
        nonlocal files_removed

        if not is_root:
            path_segments.append(search_directory.name)
            search_stack.append(pop_path_segment)

        copied_files: list[str] = []

        # Once we fail to create a destination directory, or the current directory doesn't contain any more files to
        # copy, no need to try to create the destination directory or copy any files.
        if (not mkdir_failed) and search_directory.contains_copied_files:
            relative_directory_path = Path(*path_segments)
            destination_directory_path = destination_directory / relative_directory_path

            try:
                destination_directory_path.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                paths_skipped = True
                mkdir_failed = True

                (callbacks.on_mkdir_error)(destination_directory_path, e)
            else:
                for file in search_directory.copied_files:
                    relative_file_path = relative_directory_path / file
                    source_file_path = source_directory / relative_file_path
                    destination_file_path = destination_directory / relative_file_path
                    try:
                        shutil.copy2(source_file_path, destination_file_path)
                    except OSError as e:
                        paths_skipped = True

                        (callbacks.on_copy_error)(source_file_path, destination_file_path, e)
                    else:
                        copied_files.append(file)
                        files_copied += 1

        # Keep searching through child directories if:
        #   a) destination directory was created successfully, or
        #   b) destination directory creation failed, but there are still removed items to be recorded in the manifest.
        children_to_visit = [d for d in reversed(search_directory.subdirectories)
                             if not mkdir_failed or d.contains_removed_items]

        # Only need to create and fill in the manifest entry if there is anything to put in it. I.e. if there are copied
        # files or removed files/directories to be recorded, or child entries. This may not always be true if creating
        # the destination directory failed.
        if copied_files or search_directory.removed_files or search_directory.removed_directories or children_to_visit:
            if is_root:
                manifest_directory = manifest.root
            else:
                manifest_directory = BackupManifest.Directory(search_directory.name)
                manifest_stack[-1].subdirectories.append(manifest_directory)
                manifest_stack.append(manifest_directory)
                search_stack.append(pop_manifest_node)

            manifest_directory.copied_files = copied_files
            manifest_directory.removed_files = search_directory.removed_files
            manifest_directory.removed_directories = search_directory.removed_directories
            files_removed += len(search_directory.removed_files) + search_directory.removed_directory_file_count

        # Need to use partial instead of lambda to avoid name rebinding issues.
        search_stack.extend(partial(visit_directory, d, mkdir_failed) for d in children_to_visit)

    search_stack.append(partial(visit_directory, backup_plan.root, False))
    while search_stack:
        search_stack.pop()()
        is_root = False

    return ExecuteBackupPlanResults(manifest, paths_skipped, files_copied, files_removed)
