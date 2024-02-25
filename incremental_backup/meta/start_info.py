from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, cast, NoReturn, Optional

from incremental_backup._utility import StrPath


__all__ = [
    'BackupStartInfo',
    'BackupStartInfoParseError',
    'deserialise_backup_start_info',
    'read_backup_start_info_file',
    'serialise_backup_start_info',
    'write_backup_start_info_file'
]


@dataclass(frozen=True)
class BackupStartInfo:
    """Information pertaining to the start of a backup operation."""

    start_time: datetime
    """The UTC time at which the backup operated started (just before any files were copied)."""


def serialise_backup_start_info(value: BackupStartInfo, /) -> str:
    """Writes backup start information to a string."""

    json_data = {
        'start_time': value.start_time.isoformat()
    }
    return json.dumps(json_data, indent=4, ensure_ascii=False)


def write_backup_start_info_file(path: StrPath, value: BackupStartInfo, /) -> None:
    """Writes backup start information to file.

        :except OSError: If the file could not be written to.
    """

    with open(path, 'w', encoding='utf8') as file:
        file.write(serialise_backup_start_info(value))


def deserialise_backup_start_info(string: str) -> BackupStartInfo:
    """Reads backup start information from a string.

        :except BackupStartInfoParseError: If the string is not valid backup start information.
    """

    def parse_error(reason: str, e: Optional[Exception] = None, /) -> NoReturn:
        if e is None:
            raise BackupStartInfoParseError(reason)
        else:
            raise BackupStartInfoParseError(reason) from e

    try:
        json_data = json.loads(string)
    except json.JSONDecodeError as e:
        parse_error(str(e), e)

    if not isinstance(json_data, dict):
        parse_error('Expected an object')
    json_data = cast(dict[Any, Any], json_data)

    fields = {'start_time'}
    if set(json_data.keys()) != fields:
        parse_error(f'Expected fields {fields}')

    try:
        start_time = datetime.fromisoformat(json_data['start_time'])
    except (TypeError, ValueError) as e:
        parse_error('Field "start_time" must be an ISO-8601 date string', e)

    return BackupStartInfo(start_time)


def read_backup_start_info_file(path: StrPath, /) -> BackupStartInfo:
    """Reads backup start information from file.

        :except OSError: If the file could not be read.
        :except BackupStartInfoParseError: If the file is not valid backup start information.
    """

    try:
        with open(path, 'r', encoding='utf8') as file:
            return deserialise_backup_start_info(file.read())
    except BackupStartInfoParseError as e:
        # TODO: may be nicer to raise from the cause of e
        raise BackupStartInfoParseError(e.reason, str(path)) from e


class BackupStartInfoParseError(Exception):
    """Raised when a backup start information file cannot be parsed due to invalid format."""

    def __init__(self, reason: str, file_path: Optional[str] = None) -> None:
        if file_path is None:
            message = f'Failed to parse backup start info: {reason}'
        else:
            message = f'Failed to parse backup start info file "{file_path}": {reason}'
        super().__init__(message)
        self.reason = reason
        self.file_path = file_path
