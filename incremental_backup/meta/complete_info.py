from dataclasses import dataclass
from datetime import datetime
import json
from os import PathLike
from typing import NoReturn


__all__ = [
    'BackupCompleteInfo',
    'BackupCompleteInfoParseError',
    'read_backup_complete_info',
    'write_backup_complete_info'
]


@dataclass
class BackupCompleteInfo:
    """Information pertaining to the completion of a backup operation."""

    end_time: datetime
    """The UTC time the backup operation finished (just after the last file was copied)."""

    paths_skipped: bool
    """Indicates if any paths were skipped due to filesystem errors (does NOT include explicitly excluded paths)."""


def write_backup_complete_info(path: PathLike, value: BackupCompleteInfo, /) -> None:
    """Writes backup completion information to file.

        :except OSError: If the file could not be written to.
    """

    json_data = {
        'end_time': value.end_time.isoformat(),
        'paths_skipped': value.paths_skipped
    }
    with open(path, 'w', encoding='utf8') as file:
        json.dump(json_data, file, indent=4, ensure_ascii=False)


def read_backup_complete_info(path: PathLike, /) -> BackupCompleteInfo:
    """Reads backup completion information from file.

        :except OSError: If the file could not be read.
        :except BackupCompleteInfoParseError: If the file is not valid backup completion information.
    """

    def parse_error(reason: str, /) -> NoReturn:
        raise BackupCompleteInfoParseError(str(path), reason)

    def parse_error_from(reason: str, e: Exception, /) -> NoReturn:
        raise BackupCompleteInfoParseError(str(path), reason) from e

    try:
        with open(path, 'r', encoding='utf8') as file:
            json_data = json.load(file)
    except json.JSONDecodeError as e:
        parse_error_from(str(e), e)

    if not isinstance(json_data, dict):
        parse_error('Expected an object')

    fields = {'end_time', 'paths_skipped'}
    if set(json_data.keys()) != fields:
        parse_error(f'Expected fields {fields}')

    try:
        end_time = datetime.fromisoformat(json_data['end_time'])
    except (TypeError, ValueError) as e:
        parse_error_from('Field "end_time" must be an ISO-8601 date string', e)

    if not isinstance(json_data['paths_skipped'], bool):
        parse_error('Field "paths_skipped" must be a boolean')
    paths_skipped = json_data['paths_skipped']

    return BackupCompleteInfo(end_time, paths_skipped)


class BackupCompleteInfoParseError(Exception):
    """Raised when a backup completion information file cannot be parsed due to invalid format."""

    def __init__(self, file_path: str, reason: str) -> None:
        super().__init__(f'Failed to parse backup start info file "{file_path}": {reason}')
        self.file_path = file_path
        self.reason = reason
