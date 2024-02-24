from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
import shutil
from typing import Callable, Optional, Sequence

from incremental_backup.backup import BackupSum
from incremental_backup.meta import BackupMetadata, DATA_DIRECTORY_NAME, read_backups, ReadBackupsCallbacks
from incremental_backup._utility import StrPath


__all__ = [
    'perform_restore',
    'restore_files',
    'RestoreError',
    'RestoreFilesCallbacks',
    'RestoreFilesResults',
    'RestoreCallbacks',
    'RestoreResults'
]


@dataclass(frozen=True)
class RestoreFilesResults:
    """Return results of `restore_files()`."""

    files_restored: int
    paths_skipped: bool


@dataclass(frozen=True)
class RestoreFilesCallbacks:
    """Callbacks for events that occur in `restore_files()`."""

    on_mkdir_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when an error is raised creating a directory.
        First argument is the directory path, second argument is the raised exception."""

    on_copy_error: Callable[[Path, Path, OSError], None] = lambda src, dest, error: None
    """Called when an error is raised copying a file.
        First argument is the source path, second argument is the destination path, third argument is the raised
        exception."""


def restore_files(backup_target_directory: StrPath, backup_sum: BackupSum, destination_directory: StrPath,
                  callbacks: RestoreFilesCallbacks = RestoreFilesCallbacks()) -> RestoreFilesResults:
    """Restores files and directories from backups to a new location.

        :param backup_target_directory: The directory containing the backups which are being restored. I.e. the
            "target directory" from the backup creation operation.
        :param backup_sum: Sum of backups to restore files from.
        :param destination_directory: Directory where files will be restored to. Need not exist.
        :param callbacks: Callbacks for certain events during execution. See `RestoreFilesCallbacks`.
    """

    paths_skipped = False
    files_restored = 0
    search_stack: list[Callable[[], None]] = []
    path_segments: list[str] = []
    is_root = True

    def pop_path_segment() -> None:
        del path_segments[-1]

    def visit_directory(search_directory: BackupSum.Directory, /) -> None:
        nonlocal paths_skipped
        nonlocal files_restored

        if not is_root:
            path_segments.append(search_directory.name)
            search_stack.append(pop_path_segment)

        relative_directory_path = Path(*path_segments)
        directory_path = destination_directory / relative_directory_path

        try:
            directory_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            paths_skipped = True

            (callbacks.on_mkdir_error)(directory_path, e)
        else:
            for file in search_directory.files:
                relative_file_path = relative_directory_path / file.name
                source_file_path = Path(
                    backup_target_directory, file.last_backup.name, DATA_DIRECTORY_NAME, relative_file_path)
                destination_file_path = destination_directory / relative_file_path

                try:
                    shutil.copy2(source_file_path, destination_file_path)
                except OSError as e:
                    paths_skipped = True

                    (callbacks.on_copy_error)(source_file_path, destination_file_path, e)
                else:
                    files_restored += 1

            # Need to use partial instead of lambda to avoid name rebinding issues.
            search_stack.extend(partial(visit_directory, d) for d in reversed(search_directory.subdirectories))

    search_stack.append(partial(visit_directory, backup_sum.root))
    while search_stack:
        search_stack.pop()()
        is_root = False

    return RestoreFilesResults(files_restored, paths_skipped)


@dataclass(frozen=True)
class RestoreResults:
    """Return results of `perform_restore()`."""

    files_restored: int
    paths_skipped: bool


@dataclass(frozen=True)
class RestoreCallbacks:
    """Callbacks for events that occur in `perform_restore()`."""

    on_before_read_previous_backups: Callable[[], None] = lambda: None
    """Called just before reading previous backups from the backup target directory."""

    read_backups: ReadBackupsCallbacks = ReadBackupsCallbacks()
    """Callbacks for `read_backups()`."""

    on_after_read_previous_backups: Callable[[Sequence[BackupMetadata]], None] = lambda backups: None
    """Called just after the previous backups have been read from the target directory.
        Argument is the collection of backup metadatas (in arbitrary order)."""

    on_selected_backups: Callable[[Sequence[BackupMetadata]], None] = lambda backups: None
    """Called when the backups to be used for the restore operation have been selected.
        Argument is the collection of backup metadatas (in arbitrary order)."""

    on_before_initialise_restore: Callable[[], None] = lambda: None
    """Called just before creating the destination directory (if necessary)."""

    on_before_restore_files: Callable[[], None] = lambda: None
    """Called just before copying files to the destination."""

    restore_files: RestoreFilesCallbacks = RestoreFilesCallbacks()
    """Callbacks for `restore_files()`."""


def perform_restore(backup_target_directory: StrPath, destination_directory: StrPath,
                    backup_name: Optional[str] = None, backup_time: Optional[datetime] = None,
                    callbacks: RestoreCallbacks = RestoreCallbacks()) -> RestoreResults:
    """Restores files and directories from existing backups.

        :param backup_target_directory: The directory containing the backups which are being restored. I.e. the
            "target directory" from the backup creation operation.
        :param destination_directory: Directory where files will be restored to. Need not exist.
        :param backup_name: If specified, only backups up to and including this backup (chronologically) will be used to
            restore files. Cannot be specified if `backup_time` is also specified.
        :param backup_time: If specified, only backups up to and including this time will be used to restore files.
            Cannot be specified if `backup_name` is also specified.
        :param callbacks: Callbacks for certain events during execution. See `RestoreCallbacks`.
        :return: Summary information for the restore operation.
        :except ValueError: If both `backup_name` and `backup_time` are not `None`.
        :except RestoreError: If an error occurs that prevents the restore operation from completing. See `RestoreError`.
    """

    return _RestoreOperation(
        backup_target_directory, destination_directory, backup_name, backup_time, callbacks).perform_restore()


class _RestoreOperation:
    """Implementation of the backup restore operation."""

    def __init__(self, backup_target_directory: StrPath, destination_directory: StrPath,
                 backup_name: Optional[str] = None, backup_time: Optional[datetime] = None,
                 callbacks: RestoreCallbacks = RestoreCallbacks()) -> None:
        """
            :except ValueError: If both `backup_name` and `backup_time` are not `None`.
        """

        if backup_name is not None and backup_time is not None:
            raise ValueError('backup_name and backup_time should not both be specified.')

        self.backup_name = backup_name
        self.backup_time = backup_time
        self.backup_target_directory = Path(backup_target_directory)
        self.destination_directory = Path(destination_directory)
        self.callbacks = callbacks

    def perform_restore(self) -> RestoreResults:
        """Restores files from the specified backups.

            :except RestoreError: If an error occurs that prevents the restore operation from completing. See
                `RestoreError`.
        """

        self._validate_backup_target_directory()
        self._validate_destination_directory()

        previous_backups = self._read_previous_backups()
        selected_backups = self._select_backups_to_restore(previous_backups)
        backup_sum = BackupSum.from_backups(selected_backups)

        (self.callbacks.on_before_initialise_restore)()
        self._create_destination()

        results = self._restore_files(backup_sum)

        return results

    def _validate_backup_target_directory(self) -> None:
        """Validates the backup target directory.
            Should mostly prevent other parts of the restore operation from failing strangely for invalid inputs.

            :except Restore: If the backup target directory is not an accessible directory.
        """

        try:
            if not self.backup_target_directory.exists():
                raise RestoreError(f'Backup target directory not found')
            if not self.backup_target_directory.is_dir():
                raise RestoreError(f'Backup target directory is not a directory')
        except OSError as e:
            raise RestoreError(f'Failed to query backup target directory: {e}') from e

    def _validate_destination_directory(self) -> None:
        """Validates the restore destination directory.
            Should mostly prevent other parts of the restore operation from failing strangely for invalid inputs.

            :except RestoreError: If the destination directory is inaccessible, or exists and is not an empty directory.
        """

        try:
            if self.destination_directory.exists():
                if self.destination_directory.is_dir() and tuple(self.destination_directory.iterdir()):
                    raise RestoreError('Destination directory must be nonexistent or empty')
        except OSError as e:
            raise RestoreError(f'Failed to query destination directory: {e}') from e

    def _read_previous_backups(self) -> Sequence[BackupMetadata]:
        """Reads all existing backups' metadata from the backup target directory.

            If any backup's metadata cannot be read, skips that backup.

            :except RestoreError: If the target directory cannot be enumerated.
        """

        (self.callbacks.on_before_read_previous_backups)()

        try:
            backups = read_backups(self.backup_target_directory, self.callbacks.read_backups)
        except OSError as e:
            raise RestoreError(f'Failed to enumerate backup target directory: {e}') from e
        backups = tuple(backups)

        (self.callbacks.on_after_read_previous_backups)(backups)

        return backups

    def _select_backups_to_restore(self, previous_backups: Sequence[BackupMetadata], /) -> Sequence[BackupMetadata]:
        """Returns backups from `self.previous_backups` requested to restore files from, based on `self.backup_name`
            and `self.backup_time`.

            If `backup_name` is specified, all backups whose start time is <= `backup_name`'s start time are selected.

            If `backup_time` is specified all backups whose start time is <= `backup_time` are selected.

            If neither `backup_name` nor `backup_time` are specified, all previous backups are selected.

            :except RestoreError: If `backup_name` is specified but that backup is not found.
        """

        if self.backup_name is not None:
            backup = next((b for b in previous_backups if b.name == self.backup_name), None)
            if backup is None:
                raise RestoreError('Requested backup not found')
            backup_time = backup.start_info.start_time
        else:
            backup_time = self.backup_time
        if backup_time is None:
            selected_backups = previous_backups
        else:
            selected_backups = tuple(b for b in previous_backups if b.start_info.start_time <= backup_time)

        (self.callbacks.on_selected_backups)(selected_backups)

        return selected_backups

    def _create_destination(self) -> None:
        """Creates the restore destination directory.

            Not strictly necessary to do here, because it will be created anyway in `restore_files()`. But if we attempt
            to create it here first, then we can fail with an informative error in the case the path is not accessible.

            :except RestoreError: If the directory could not be created.
        """

        try:
            self.destination_directory.mkdir(exist_ok=True)
        except OSError as e:
            raise RestoreError(f'Failed to create destination directory: {e}') from e

    def _restore_files(self, backup_sum: BackupSum) -> RestoreResults:
        """Copies files and directories from backups specified by `backup_sum` to the destination directory.

            :return: Summary information from the operation.
        """

        (self.callbacks.on_before_restore_files)()

        restore_results = restore_files(
            self.backup_target_directory, backup_sum, self.destination_directory, self.callbacks.restore_files)

        return RestoreResults(restore_results.files_restored, restore_results.paths_skipped)


class RestoreError(Exception):
    """Raised when restoring files from backups fails such that the operation cannot continue.

        Some cases where this exception is raised:
         - The backup target directory can't be accessed at all.
         - The specified backup name doesn't exist.
         - The destination directory couldn't be created.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
