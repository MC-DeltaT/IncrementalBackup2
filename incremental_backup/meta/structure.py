from os import PathLike
from pathlib import Path
import random


__all__ = [
    'BACKUP_DIRECTORY_CREATION_RETRIES',
    'BACKUP_NAME_LENGTH',
    'BackupDirectoryCreationError',
    'COMPLETE_INFO_FILENAME',
    'create_new_backup_directory',
    'DATA_DIRECTORY_NAME',
    'generate_backup_name',
    'LOG_FILENAME',
    'MANIFEST_FILENAME',
    'START_INFO_FILENAME'
]


MANIFEST_FILENAME = 'manifest.json'
"""The name of the backup manifest file within a backup directory."""

START_INFO_FILENAME = 'start.json'
"""The name of the backup start information file within a backup directory."""

COMPLETE_INFO_FILENAME = 'completion.json'
"""The name of the backup completion information file within a backup directory."""

LOG_FILENAME = 'log.txt'
"""The name of the application log file within a backup directory."""

DATA_DIRECTORY_NAME = 'data'
"""The name of the backup data directory within a backup directory."""

BACKUP_NAME_LENGTH = 16
"""The length of the backup directory name."""

BACKUP_DIRECTORY_CREATION_RETRIES = 20
"""The number of times to retry creating a new backup directory before failing."""


def generate_backup_name() -> str:
    """Generates a (very likely) unique name for a backup.
        The name has length `BACKUP_NAME_LENGTH` and consists of only lowercase alphabetic characters and digits.
    """

    chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
    name = ''.join(random.choices(chars, k=BACKUP_NAME_LENGTH))
    return name


def create_new_backup_directory(target_directory: PathLike, /) -> str:
    """Creates a new backup directory in the given directory.
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
