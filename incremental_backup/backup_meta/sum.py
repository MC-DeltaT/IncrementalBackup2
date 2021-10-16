from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Union

from ..utility import path_name_equal
from .manifest import BackupManifest
from .metadata import BackupMetadata


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
        files: List['BackupSum.File'] = field(default_factory=list)
        subdirectories: List['BackupSum.Directory'] = field(default_factory=list)

    root: Directory
    """The root of the reconstructed file/directory structure.
        This object represents the backup source directory.
    """

    def find_directory(self, path: Iterable[str]) -> Optional[Directory]:
        """Finds a directory within the backup sum by path.

            :param path: Sequence of directory names forming the path to the directory, relative to the backup source
                directory (i.e. root of the backup sum).
            :return: The requested directory if it exists in the backup sum, else `None`.
        """

        directory = self.root
        for name in path:
            directory = next((d for d in directory.subdirectories if path_name_equal(d.name, name)), None)
            if directory is None:
                break
        return directory

    @classmethod
    def from_backups(cls, backups: Iterable[BackupMetadata]) -> 'BackupSum':
        """Constructs a backup sum from previous backup metadata.

            :param backups: 0 or more backups to sum. Should all be for the same source directory, or the results will
                be meaningless.
        """

        root = cls._construct_tree(backups)
        cls._prune_tree(root)
        return cls(root)

    @staticmethod
    def _construct_tree(backups: Iterable[BackupMetadata]) -> Directory:
        """Reconstructs the file/directory tree from the given backups."""

        root = BackupSum.Directory('')

        backups_sorted = sorted(backups, key=lambda b: b.start_info.start_time)

        # TODO? calculate tree pruning content counts while constructing the tree?

        for backup in backups_sorted:
            search_stack: List[Union[BackupManifest.Directory, None]] = [backup.manifest.root]
            sum_stack = [root]
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

        return root

    @staticmethod
    def _prune_tree(root: Directory) -> None:
        """Removes directories that don't have any descendents which are files."""

        # Find all directories first.
        directories: List[BackupSum.Directory] = []
        search_stack: List[BackupSum.Directory] = [root]
        while search_stack:
            directory = search_stack.pop()
            directories.append(directory)
            search_stack.extend(directory.subdirectories)

        # Calculate how many non-empty descendents each directory has.
        # Empty = contains nothing or only directories.
        content_counts: Dict[int, int] = {}
        for directory in reversed(directories):
            content_counts[id(directory)] = \
                sum(content_counts[id(d)] for d in directory.subdirectories) + len(directory.files)

        # Traverse the tree again and remove directories that are empty.
        # Note: root never gets removed.
        search_stack: List[BackupSum.Directory] = [root]
        while search_stack:
            directory = search_stack.pop()
            to_remove: List[int] = []
            for i, subdirectory in enumerate(directory.subdirectories):
                if content_counts[id(subdirectory)] == 0:
                    to_remove.append(i)
                else:
                    search_stack.append(subdirectory)
            delta = 0
            for i in to_remove:
                del directory.subdirectories[i + delta]
                delta -= 1
