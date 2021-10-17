from dataclasses import dataclass
from datetime import datetime
import json
from os import PathLike
from typing import NoReturn


__all__ = [
    'BackupStartInfo',
    'BackupStartInfoParseError',
    'read_backup_start_info',
    'write_backup_start_info'
]


@dataclass
class BackupStartInfo:
    """Information pertaining to the start of a backup operation."""

    start_time: datetime
    """The UTC time at which the backup operated started (just before any files were copied)."""


def write_backup_start_info(path: PathLike, value: BackupStartInfo) -> None:
    """Writes backup start information to file.

        :except OSError: If the file could not be written to.
    """

    json_data = {
        'start_time': value.start_time.isoformat()
    }
    with open(path, 'w', encoding='utf8') as file:
        json.dump(json_data, file, indent=4, ensure_ascii=False)


def read_backup_start_info(path: PathLike) -> BackupStartInfo:
    """Reads backup start information from file.

        :except OSError: If the file could not be read.
        :except BackupStartInfoParseError: If the file is not valid backup start information.
    """

    def parse_error(reason: str) -> NoReturn:
        raise BackupStartInfoParseError(str(path), reason)

    def parse_error_from(reason: str, e: Exception) -> NoReturn:
        raise BackupStartInfoParseError(str(path), reason) from e

    try:
        with open(path, 'r', encoding='utf8') as file:
            json_data = json.load(file)
    except json.JSONDecodeError as e:
        parse_error_from(str(e), e)

    if not isinstance(json_data, dict):
        parse_error('Expected an object')

    fields = {'start_time'}
    if set(json_data.keys()) != fields:
        parse_error(f'Expected fields {fields}')

    try:
        start_time = datetime.fromisoformat(json_data['start_time'])
    except (TypeError, ValueError) as e:
        parse_error_from('Field "start_time" must be an ISO-8601 date string', e)

    return BackupStartInfo(start_time)


class BackupStartInfoParseError(Exception):
    """Raised when a backup start information file cannot be parsed due to invalid format."""

    def __init__(self, file_path: str, reason: str) -> None:
        super().__init__(f'Failed to parse backup start info file "{file_path}": {reason}')
        self.file_path = file_path
        self.reason = reason
