from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

from incremental_backup.backup.filesystem import scan_filesystem, ScanFilesystemCallbacks
from incremental_backup.backup.plan import BackupPlan, execute_backup_plan, ExecuteBackupPlanCallbacks
from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta import BackupCompleteInfo, BackupDirectoryCreationError, BackupManifest, \
    BackupMetadata, BackupStartInfo, COMPLETE_INFO_FILENAME, create_new_backup_directory, \
    DATA_DIRECTORY_NAME, MANIFEST_FILENAME, read_backups, ReadBackupsCallbacks, START_INFO_FILENAME, \
    write_backup_complete_info_file, write_backup_manifest_file, write_backup_start_info_file
from incremental_backup.path_exclude import PathExcludePattern
from incremental_backup._utility import StrPath


__all__ = [
    'BackupCallbacks',
    'BackupError',
    'BackupResults',
    'perform_backup'
]


@dataclass(frozen=True)
class BackupResults:
    """Return results of `perform_backup()`."""

    backup_path: Path
    start_info: BackupStartInfo
    manifest: BackupManifest
    complete_info: BackupCompleteInfo
    files_copied: int
    files_removed: int


@dataclass(frozen=True)
class BackupCallbacks:
    """Callbacks for events that occur during `perform_backup()`."""

    on_before_read_previous_backups: Callable[[], None] = lambda: None
    """Called just before reading previous backups from the target directory."""

    read_backups: ReadBackupsCallbacks = ReadBackupsCallbacks()
    """Callbacks for `read_backups()`."""

    on_after_read_previous_backups: Callable[[Sequence[BackupMetadata]], None] = lambda backups: None
    """Called just after the previous backups have been read from the target directory.
        Argument is the collection of backup metadatas (in arbitrary order)."""

    on_before_initialise_backup: Callable[[], None] = lambda: None
    """Called just before creating and initialising the new backup directory."""

    on_created_backup_directory: Callable[[Path], None] = lambda path: None
    """Called just after creating the new backup directory.
        Argument is the path to the directory."""

    on_before_scan_source: Callable[[], None] = lambda: None
    """Called just before scanning the source directory."""

    scan_source: ScanFilesystemCallbacks = ScanFilesystemCallbacks()
    """Callbacks for `scan_filesystem()`."""

    on_before_copy_files: Callable[[], None] = lambda: None
    """Called just before copying files to the backup."""

    execute_plan: ExecuteBackupPlanCallbacks = ExecuteBackupPlanCallbacks()
    """Callbacks for `execute_backup_plan()`."""

    on_before_save_metadata: Callable[[], None] = lambda: None
    """Called just before saving the manifest and completion information to file."""

    on_write_complete_info_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when writing the backup completion information file fails.
        First argument is the path to the file, second argument is the raised exception."""


def perform_backup(source_directory: StrPath, target_directory: StrPath, exclude_patterns: Iterable[PathExcludePattern],
                   callbacks: BackupCallbacks = BackupCallbacks()) -> BackupResults:
    """Performs the entire operation of creating a new backup, including creating the backup directory, copying files,
        and saving metadata.

        :param source_directory: Directory to back up.
        :param target_directory: Directory to create the new backup in, and where any previous backups are read from.
            Need not exist.
        :param exclude_patterns: Patterns to match paths which will be excluded from the backup.
        :param callbacks: Callbacks for certain events during execution. See `BackupCallbacks`.
        :return: Metadata and summary information for the backup operation.
        :except BackupError: If an error occurs that prevents the backup operation from creating a valid backup. See
            `BackupError`.
    """

    return _BackupOperation(source_directory, target_directory, exclude_patterns, callbacks).perform_backup()


class _BackupOperation:
    """Implementation of the backup creation operation."""

    def __init__(self, source_directory: StrPath, target_directory: StrPath,
                 exclude_patterns: Iterable[PathExcludePattern], callbacks: BackupCallbacks = BackupCallbacks()) -> None:
        self.source_directory = Path(source_directory)
        self.target_directory = Path(target_directory)
        self.exclude_patterns = tuple(exclude_patterns)
        self.callbacks = callbacks

        self._init_working_state()

    def perform_backup(self) -> BackupResults:
        """Creates a new backup.

            :except BackupError: If an error occurs that prevents the backup operation from creating a valid backup. See
                `BackupError`.
        """

        self._init_working_state()

        self._validate_source_directory()
        self._validate_target_directory()

        previous_backups = self._read_previous_backups()
        backup_sum = BackupSum.from_backups(previous_backups)

        (self.callbacks.on_before_initialise_backup)()
        self._create_backup_directory()
        assert self.backup_path is not None
        data_path = self._create_data_directory(self.backup_path)
        self._create_start_info()

        backup_plan = self._compute_backup_plan(backup_sum)
        self._back_up_files(data_path, backup_plan)

        (self.callbacks.on_before_save_metadata)()
        self._save_manifest()
        self._save_complete_info()

        assert self.start_info is not None
        assert self.manifest is not None
        assert self.complete_info is not None
        assert self.files_copied is not None
        assert self.files_removed is not None
        return BackupResults(
            self.backup_path, self.start_info, self.manifest, self.complete_info, self.files_copied, self.files_removed)

    def _init_working_state(self) -> None:
        """Initialises various shared data used by and operated on by the methods in this class."""

        self.backup_path: Optional[Path] = None
        self.start_info: Optional[BackupStartInfo] = None
        self.manifest: Optional[BackupManifest] = None
        self.complete_info: Optional[BackupCompleteInfo] = None
        self.paths_skipped: Optional[bool] = None
        self.files_copied: Optional[int] = None
        self.files_removed: Optional[int] = None

    def _validate_source_directory(self) -> None:
        """Validates the backup source directory.
            Should mostly prevent other parts of the backup operation from failing strangely for invalid inputs.

            :except BackupError: If the source directory is not an accessible directory.
        """

        try:
            if not self.source_directory.exists():
                raise BackupError('Source directory not found')
            if not self.source_directory.is_dir():
                raise BackupError('Source directory is not a directory')
        except OSError as e:
            raise BackupError(f'Failed to query source directory: {e}') from e

    def _validate_target_directory(self) -> None:
        """Validates the backup target directory.
            Should mostly prevent other parts of the backup operation from failing strangely for invalid inputs.

            :except BackupError: If the target directory is inaccessible, or exists and is not a directory.
        """

        try:
            if self.target_directory.exists() and not self.target_directory.is_dir():
                raise BackupError('Target directory is not a directory')
        except OSError as e:
            raise BackupError(f'Failed to query target directory: {e}') from e

    def _read_previous_backups(self) -> Sequence[BackupMetadata]:
        """Reads existing backups' metadata from the backup target directory.

            If any backup's metadata cannot be read, skips that backup.

            :except BackupError: If the target directory cannot be enumerated.
        """

        (self.callbacks.on_before_read_previous_backups)()

        try:
            if not self.target_directory.exists():
                backups = []
            else:
                backups = read_backups(self.target_directory, self.callbacks.read_backups)
        except OSError as e:
            raise BackupError(f'Failed to enumerate target directory: {e}') from e
        backups = tuple(backups)

        (self.callbacks.on_after_read_previous_backups)(backups)

        return backups

    def _create_backup_directory(self) -> None:
        """Creates a new backup directory within the target directory.

            :except BackupError: If the directory could not be created.
        """

        try:
            backup_name = create_new_backup_directory(self.target_directory)
        except BackupDirectoryCreationError as e:
            raise BackupError(str(e)) from e

        backup_path = self.target_directory / backup_name
        self.backup_path = backup_path

        (self.callbacks.on_created_backup_directory)(backup_path)

    @staticmethod
    def _create_data_directory(backup_path: Path, /) -> Path:
        """Creates the backup data directory (directory which contains the copied files).

            :return: Path to the created directory.
            :except BackupError: If the directory could not be created.
        """

        path = backup_path / DATA_DIRECTORY_NAME
        try:
            path.mkdir(parents=True, exist_ok=False)
        except OSError as e:
            raise BackupError(f'Failed to create backup data directory: {e}') from e
        return path

    def _create_start_info(self) -> None:
        """Creates and writes the backup start information to file within the backup directory.

            :except BackupError: If the file could not be written to.
        """

        start_info = BackupStartInfo(datetime.now(timezone.utc))
        self.start_info = start_info
        assert self.backup_path is not None
        file_path = self.backup_path / START_INFO_FILENAME
        try:
            write_backup_start_info_file(file_path, start_info)
        except OSError as e:
            raise BackupError(f'Failed to write backup start information file: {e}') from e

    def _compute_backup_plan(self, backup_sum: BackupSum) -> BackupPlan:
        """Scans the source directory and generates the backup plan."""

        (self.callbacks.on_before_scan_source)()

        scan_results = scan_filesystem(self.source_directory, self.exclude_patterns, self.callbacks.scan_source)
        self.paths_skipped = self.paths_skipped or scan_results.paths_skipped
        backup_plan = BackupPlan.new(scan_results.tree, backup_sum)
        return backup_plan

    def _back_up_files(self, destination_path: StrPath, backup_plan: BackupPlan) -> None:
        """Backs up files from the source directory to the backup directory according to the backup plan."""

        (self.callbacks.on_before_copy_files)()

        execute_results = execute_backup_plan(
            backup_plan, self.source_directory, destination_path, self.callbacks.execute_plan)

        self.paths_skipped = self.paths_skipped or execute_results.paths_skipped
        self.manifest = execute_results.manifest
        self.complete_info = BackupCompleteInfo(datetime.now(timezone.utc), self.paths_skipped)
        self.files_copied = execute_results.files_copied
        self.files_removed = execute_results.files_removed

    def _save_manifest(self) -> None:
        """Writes the backup manifest to file within the backup directory.

            :except BackupError: If the file could not be written to.
        """

        assert self.backup_path is not None
        file_path = self.backup_path / MANIFEST_FILENAME
        try:
            assert self.manifest is not None
            write_backup_manifest_file(file_path, self.manifest)
        except OSError as e:
            raise BackupError(f'Failed to write backup manifest file: {e}') from e

    def _save_complete_info(self) -> None:
        """Writes the backup completion information to file within the backup directory.

            It is not a fatal error if this operation fails, since the completion information is not required by the
            application at this time."""

        assert self.backup_path is not None
        file_path = self.backup_path / COMPLETE_INFO_FILENAME
        try:
            assert self.complete_info is not None
            write_backup_complete_info_file(file_path, self.complete_info)
        except OSError as e:
            # Not fatal since the completion info isn't currently used by the software.
            (self.callbacks.on_write_complete_info_error)(file_path, e)


class BackupError(Exception):
    """Raised when creating a backup fails such that a valid backup cannot be produced.

        Some cases where this exception is raised:
         - The source directory cannot be accessed at all.
         - A new backup directory couldn't be created.
         - Writing the backup start information file failed.
         - Writing the backup manifest file failed.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
