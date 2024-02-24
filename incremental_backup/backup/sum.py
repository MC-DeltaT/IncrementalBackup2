from dataclasses import dataclass, field
from typing import Iterable, Union

from incremental_backup.meta import BackupManifest, BackupMetadata
from incremental_backup._utility import path_name_equal


__all__ = [
    'BackupSum'
]


@dataclass
class BackupSum:
    """Represents the result of applying a sequence of backups.
        That is, reconstructs the state of the source directory given backup data.
    """

    @dataclass
    class File:
        name: str

        last_backup: BackupMetadata
        """The metadata of the last backup which copied this file."""

    @dataclass
    class Directory:
        name: str
        files: list['BackupSum.File'] = field(default_factory=list)
        subdirectories: list['BackupSum.Directory'] = field(default_factory=list)

        def count_contained_files(self) -> int:
            """Calculates the total number of files contained in this directory and all its descendents."""

            count = 0
            search_stack: list[BackupSum.Directory] = [self]
            while search_stack:
                search_directory = search_stack.pop()
                count += len(search_directory.files)
                search_stack.extend(search_directory.subdirectories)
            return count

    root: Directory = field(default_factory=lambda: BackupSum.Directory(''))
    """The root of the reconstructed file/directory structure.
        This object represents the backup source directory.
    """

    @classmethod
    def from_backups(cls, backups: Iterable[BackupMetadata], /) -> 'BackupSum':
        """Constructs a backup sum from previous backup metadata.

            :param backups: 0 or more backups to sum. Should all be for the same source directory, or the results will
                be meaningless.
        """

        backup_sum = cls()

        backups_sorted = sorted(backups, key=lambda backup: backup.start_info.start_time)

        # list of all directories. Parent will always occur before child in list.
        directories: list[BackupSum.Directory] = [backup_sum.root]

        for backup in backups_sorted:
            search_stack: list[Union[BackupManifest.Directory, None]] = [backup.manifest.root]
            sum_stack = [backup_sum.root]
            is_root = True
            while search_stack:
                search_directory = search_stack.pop()
                if search_directory is None:
                    del sum_stack[-1]
                else:
                    if not is_root:
                        sum_directory = next(
                            (d for d in sum_stack[-1].subdirectories if path_name_equal(d.name, search_directory.name)),
                            None)
                        if sum_directory is None:
                            sum_directory = BackupSum.Directory(search_directory.name)
                            sum_stack[-1].subdirectories.append(sum_directory)
                            directories.append(sum_directory)
                        sum_stack.append(sum_directory)

                    for copied_file in search_directory.copied_files:
                        prev_file = next(
                            (f for f in sum_stack[-1].files if path_name_equal(f.name, copied_file)), None)
                        if prev_file is None:
                            sum_stack[-1].files.append(BackupSum.File(copied_file, backup))
                        else:
                            prev_file.last_backup = backup

                    for removed_file in search_directory.removed_files:
                        sum_stack[-1].files = \
                            [f for f in sum_stack[-1].files if not path_name_equal(f.name, removed_file)]

                    for removed_directory in search_directory.removed_directories:
                        sum_stack[-1].subdirectories = \
                            [d for d in sum_stack[-1].subdirectories if not path_name_equal(d.name, removed_directory)]

                    search_stack.append(None)
                    search_stack.extend(reversed(search_directory.subdirectories))
                is_root = False

        # Calculate if each directory has nonempty descendents has and remove empty directories.
        # Empty = contains nothing or only directories.
        nonempty_map: dict[int, bool] = {}
        for directory in reversed(directories):
            nonempty = len(directory.files) > 0
            nonempty_subdirectories: list[BackupSum.Directory] = []
            for subdirectory in directory.subdirectories:
                # Ok, emptiness of child is always calculated before parent.
                sub_nonempty = nonempty_map[id(subdirectory)]
                if sub_nonempty:
                    nonempty_subdirectories.append(subdirectory)
                    nonempty = True
            nonempty_map[id(directory)] = nonempty
            directory.subdirectories = nonempty_subdirectories

        return backup_sum
