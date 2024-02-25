from dataclasses import dataclass
from pathlib import Path
from random import Random
from typing import Callable, Union

from incremental_backup.meta.manifest import BackupManifest, BackupManifestParseError, read_backup_manifest_file
from incremental_backup.meta.start_info import BackupStartInfo, BackupStartInfoParseError, read_backup_start_info_file
from incremental_backup._utility import StrPath


__all__ = [
    'BACKUP_DIRECTORY_CREATION_RETRIES',
    'BACKUP_NAME_LENGTH',
    'BackupDirectoryCreationError',
    'BackupMetadata',
    'COMPLETE_INFO_FILENAME',
    'create_new_backup_directory',
    'DATA_DIRECTORY_NAME',
    'generate_backup_name',
    'check_if_probably_backup',
    'MANIFEST_FILENAME',
    'read_backup_metadata',
    'read_backups',
    'ReadBackupsCallbacks',
    'START_INFO_FILENAME'
]


MANIFEST_FILENAME = 'manifest.json'
"""The name of the backup manifest file within a backup directory."""

START_INFO_FILENAME = 'start.json'
"""The name of the backup start information file within a backup directory."""

COMPLETE_INFO_FILENAME = 'completion.json'
"""The name of the backup completion information file within a backup directory."""

DATA_DIRECTORY_NAME = 'data'
"""The name of the backup data directory within a backup directory."""


def check_if_probably_backup(directory: StrPath, /) -> bool:
    """Checks if a directory is likely to be a backup directory.

        If the directory is a valid backup, and is accessible, then this function will return `True`.
        If the directory is not a valid backup, then this function will probably return `False`, but may return `True`.

        :except OSError: If querying the directory or its contents failed (excluding if the directory or its expected
            contents don't exist).
    """

    directory = Path(directory)

    start_info_path = directory / START_INFO_FILENAME
    manifest_path = directory / MANIFEST_FILENAME
    # Do not check for exact length of backup name, in case we change it in the future.
    # Also check name properties first to avoid hitting the filesystem if possible.
    name = directory.name
    return (len(name) >= 10 and name.isascii() and name.isalnum() and directory.is_dir() and start_info_path.is_file()
            and manifest_path.is_file())


@dataclass(frozen=True)
class BackupMetadata:
    """All useful metadata for a backup."""

    name: str
    start_info: BackupStartInfo
    manifest: BackupManifest

    # Backup completion information is not here because it is currently not read by the application.


def read_backup_metadata(backup_directory: StrPath, /) -> BackupMetadata:
    """Reads the metadata of a backup, i.e. the name, start information, and manifest.

        :except OSError: If a metadata file could not be read.
        :except BackupStartInfoParseError: If the backup start information file could not be parsed.
        :except BackupManifestParseError: If the backup manifest file could not be parsed.
    """

    backup_directory = Path(backup_directory)
    name = backup_directory.name
    start_info = read_backup_start_info_file(backup_directory / START_INFO_FILENAME)
    manifest = read_backup_manifest_file(backup_directory / MANIFEST_FILENAME)
    return BackupMetadata(name, start_info, manifest)


@dataclass(frozen=True)
class ReadBackupsCallbacks:
    on_query_entry_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when querying an entry in the target directory fails.
        First argument is the path to the file/directory, second argument is the raised exception."""

    on_read_metadata_error: Callable[[Path, Union[OSError, BackupStartInfoParseError, BackupManifestParseError]], None] \
        = lambda path, error: None
    """Called when reading the metadata of a backup fails.
        First argument is the path of the backup, second argument is the raised exception."""


def read_backups(directory: StrPath, /, callbacks: ReadBackupsCallbacks = ReadBackupsCallbacks()) \
        -> list[BackupMetadata]:
    """Reads all backups present in a directory.

        If a backup is not valid or cannot be read, it is skipped.

        :except OSError: If the directory cannot be accessed.
    """

    directory = Path(directory)

    entries = list(directory.iterdir())

    backups: list[BackupMetadata] = []
    for entry in entries:
        # Try to just read the backup metadata first, because check_if_probably_backup() is quite slow. That way we only
        # pay the cost of check_if_probably_backup() if a directory is not backup, which is unlikely to occur in typical
        # usage.
        try:
            metadata = read_backup_metadata(entry)
        except (OSError, BackupStartInfoParseError, BackupManifestParseError) as read_error:
            # Could be: a valid backup and a filesystem error occurred, or a backup with malformed metadata, or
            # something that's not a backup at all.
            try:
                if check_if_probably_backup(entry):
                    (callbacks.on_read_metadata_error)(entry, read_error)
                # If the entry doesn't look like a backup at all then just ignore it.
            except OSError as query_error:
                (callbacks.on_query_entry_error)(entry, query_error)
        else:
            backups.append(metadata)

    return backups


BACKUP_NAME_LENGTH = 16
"""The length of a backup directory name."""


def generate_backup_name(random_gen: Random = Random(), /) -> str:
    """Generates a (very likely) unique name for a backup.
        The name has length `BACKUP_NAME_LENGTH` and consists of only lowercase ASCII alphabetic characters and digits.
    """

    chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
    name = ''.join(random_gen.choices(chars, k=BACKUP_NAME_LENGTH))
    return name


BACKUP_DIRECTORY_CREATION_RETRIES = 20
"""The number of times to retry creating a new backup directory before failing."""


def create_new_backup_directory(target_directory: StrPath, /) -> str:
    """Creates a new backup directory in the given directory (which may not necessarily exist).
        Will try up to `BACKUP_DIRECTORY_CREATION_RETRIES` to create a new directory before failing.

        :return: Name of the new backup directory.
        :except BackupDirectoryCreationError: If the directory could not be created (due to filesystem errors or name
            conflicts) within the required number of retries.
    """

    retries = BACKUP_DIRECTORY_CREATION_RETRIES
    while True:
        name = generate_backup_name()
        path = Path(target_directory, name)
        try:
            path.mkdir(parents=True, exist_ok=False)
        except OSError as e:
            if retries <= 0:
                raise BackupDirectoryCreationError(str(e)) from e
            else:
                retries -= 1
        else:
            return name


class BackupDirectoryCreationError(Exception):
    """Raised when creating a backup directory fails due to filesystem errors or name conflicts."""

    def __init__(self, reason: str) -> None:
        super().__init__(f'Failed to create backup directory: {reason}')
        self.reason = reason
