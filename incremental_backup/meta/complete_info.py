from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, cast, NoReturn, Optional

from incremental_backup._utility import StrPath


__all__ = [
    'BackupCompleteInfo',
    'BackupCompleteInfoParseError',
    'deserialise_backup_complete_info',
    'read_backup_complete_info_file',
    'serialise_backup_complete_info',
    'write_backup_complete_info_file'
]


@dataclass(frozen=True)
class BackupCompleteInfo:
    """Information pertaining to the completion of a backup operation."""

    end_time: datetime
    """The UTC time the backup operation finished (just after the last file was copied)."""

    paths_skipped: bool
    """Indicates if any paths were skipped due to filesystem errors (does NOT include explicitly excluded paths)."""


def serialise_backup_complete_info(value: BackupCompleteInfo, /) -> str:
    """Writes backup completion information to a string."""

    json_data = {
        'end_time': value.end_time.isoformat(),
        'paths_skipped': value.paths_skipped
    }
    return json.dumps(json_data, indent=4, ensure_ascii=False)


def write_backup_complete_info_file(path: StrPath, value: BackupCompleteInfo, /) -> None:
    """Writes backup completion information to file.

        :except OSError: If the file could not be written to.
    """

    with open(path, 'w', encoding='utf8') as file:
        file.write(serialise_backup_complete_info(value))


def deserialise_backup_complete_info(string: str, /) -> BackupCompleteInfo:
    """Reads backup completion information from a string.

        :except BackupCompleteInfoParseError: If the string is not valid backup completion information.
    """

    def parse_error(reason: str, e: Optional[Exception] = None, /) -> NoReturn:
        if e is None:
            raise BackupCompleteInfoParseError(reason)
        else:
            raise BackupCompleteInfoParseError(reason) from e

    try:
        json_data = json.loads(string)
    except json.JSONDecodeError as e:
        parse_error(str(e), e)

    if not isinstance(json_data, dict):
        parse_error('Expected an object')
    json_data = cast(dict[Any, Any], json_data)

    fields = {'end_time', 'paths_skipped'}
    if set(json_data.keys()) != fields:
        parse_error(f'Expected fields {fields}')

    try:
        end_time = datetime.fromisoformat(json_data['end_time'])
    except (TypeError, ValueError) as e:
        parse_error('Field "end_time" must be an ISO-8601 date string', e)

    if not isinstance(json_data['paths_skipped'], bool):
        parse_error('Field "paths_skipped" must be a boolean')
    paths_skipped = json_data['paths_skipped']

    return BackupCompleteInfo(end_time, paths_skipped)


def read_backup_complete_info_file(path: StrPath, /) -> BackupCompleteInfo:
    """Reads backup completion information from file.

        :except OSError: If the file could not be read.
        :except BackupCompleteInfoParseError: If the file is not valid backup completion information.
    """

    try:
        with open(path, 'r', encoding='utf8') as file:
            return deserialise_backup_complete_info(file.read())
    except BackupCompleteInfoParseError as e:
        # TODO: may be nicer to raise from the cause of e
        raise BackupCompleteInfoParseError(e.reason, str(path)) from e


class BackupCompleteInfoParseError(Exception):
    """Raised when a backup completion information file cannot be parsed due to invalid format."""

    def __init__(self, reason: str, file_path: Optional[str] = None) -> None:
        if file_path is None:
            message = f'Failed to parse backup start info: {reason}'
        else:
            message = f'Failed to parse backup start info file "{file_path}": {reason}'
        super().__init__(message)
        self.reason = reason
        self.file_path = file_path
