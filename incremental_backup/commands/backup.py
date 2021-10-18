from argparse import ArgumentTypeError
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
from typing import Iterable, List, Sequence, Tuple

from ..backup import BackupSum, BackupResults, compile_exclude_pattern, do_backup
from ..meta import BackupCompleteInfo, BackupDirectoryCreationError, BackupManifest, BackupManifestParseError, \
    BackupMetadata, BackupStartInfo, BackupStartInfoParseError, COMPLETE_INFO_FILENAME, \
    create_new_backup_directory, DATA_DIRECTORY_NAME, MANIFEST_FILENAME, read_backup_metadata, START_INFO_FILENAME, \
    write_backup_complete_info, write_backup_manifest, write_backup_start_info


__all__ = [
    'add_arg_subparser',
    'COMMAND_NAME',
    'entrypoint',
]


COMMAND_NAME = 'backup'


def add_arg_subparser(subparser) -> None:
    parser = subparser.add_parser(COMMAND_NAME, description='Creates a new backup.', help='Creates a new backup.')
    parser.add_argument('source_dir', action='store', type=parse_source_directory, help='Directory to back up.')
    parser.add_argument('target_dir', action='store', type=parse_target_directory, help='Directory to back up into.')
    parser.add_argument(
        '--exclude-pattern', action='append', type=compile_exclude_pattern, required=False,
        help='Path pattern(s) to exclude. Can be specified more than once')


def entrypoint(args) -> None:
    source_path: Path = args.source_dir
    target_path: Path = args.target_dir
    exclude_patterns: Sequence[re.Pattern] = args.exclude_pattern or ()

    try:
        print_config(source_path, target_path, exclude_patterns)
        print()

        print('Reading previous backups')
        previous_backups = read_previous_backups(target_path)
        backup_sum = BackupSum.from_backups(previous_backups)

        print('Initialising backup')
        backup_path = create_backup_directory(target_path)
        data_path = create_data_directory(backup_path)
        create_start_info(backup_path)

        print('Running backup operation')
        results, manifest = perform_backup(source_path, data_path, exclude_patterns, backup_sum)

        print('Saving metadata')
        save_manifest(backup_path, manifest)
        create_complete_info(backup_path, results.paths_skipped)

        print_results(results)

        sys.exit(EXIT_CODE_SUCCESS)
    except FatalError as e:
        error(str(e))
        sys.exit(e.exit_code)
    except Exception as e:
        error(f'Unhandled exception: {repr(e)}')
        sys.exit(EXIT_CODE_LOGIC_ERROR)


def parse_source_directory(path: str) -> Path:
    path = Path(path)
    if not path.exists():
        raise ArgumentTypeError('Directory not found')
    if not path.is_dir():
        raise ArgumentTypeError('Must be a directory')
    return path


def parse_target_directory(path: str) -> Path:
    path = Path(path)
    if path.exists() and not path.is_dir():
        raise ArgumentTypeError('Must be a directory')
    return path


def print_config(source_path: Path, target_path: Path, exclude_patterns: Iterable[re.Pattern]) -> None:
    print(f'Source directory: {source_path}')
    print(f'Target directory: {target_path}')
    print('Exclude patterns:')
    if exclude_patterns:
        for pattern in exclude_patterns:
            print(f'  {pattern.pattern}')
    else:
        print('  <none>')


def read_previous_backups(target_path: Path) -> List[BackupMetadata]:
    def is_probably_backup(directory: Path) -> bool:
        start_info_path = directory / START_INFO_FILENAME
        manifest_path = directory / MANIFEST_FILENAME
        # Note: do not check for length of backup name, in case we change it in the future.
        return directory.name.isalnum() and start_info_path.is_file() and manifest_path.is_file()

    if not target_path.exists():
        return []

    try:
        subdirectories = [d for d in target_path.iterdir() if d.is_dir()]
    except OSError as e:
        raise FatalError(f'Failed to enumerate previous backups: {e}', EXIT_CODE_RUNTIME_ERROR) from e

    backups: List[BackupMetadata] = []
    for directory in subdirectories:
        if is_probably_backup(directory):
            try:
                metadata = read_backup_metadata(directory)
            except (OSError, BackupStartInfoParseError, BackupManifestParseError) as e:
                warning(f'Failed to read metadata of previous backup {directory.name}: {e}')
            else:
                backups.append(metadata)
        # TODO? should we give feedback to the user if a directory is not a backup?

    print(f'Read {len(backups)} previous backups')

    return backups


def create_backup_directory(target_path: Path) -> Path:
    try:
        backup_name = create_new_backup_directory(target_path)
    except BackupDirectoryCreationError as e:
        raise FatalError(str(e), EXIT_CODE_RUNTIME_ERROR) from e
    print(f'Backup name: {backup_name}')
    return target_path / backup_name


def create_data_directory(backup_path: Path) -> Path:
    path = backup_path / DATA_DIRECTORY_NAME
    try:
        path.mkdir(parents=True, exist_ok=False)
    except OSError as e:
        raise FatalError(f'Failed to create backup data directory: {e}', EXIT_CODE_RUNTIME_ERROR) from e
    return path


def create_start_info(backup_path: Path) -> None:
    start_info = BackupStartInfo(datetime.now(timezone.utc))
    file_path = backup_path / START_INFO_FILENAME
    try:
        write_backup_start_info(file_path, start_info)
    except OSError as e:
        raise FatalError(f'Failed to write backup start information file: {e}', EXIT_CODE_RUNTIME_ERROR) from e


def perform_backup(source_path: Path, destination_path: Path, exclude_patterns: Iterable[re.Pattern],
                   backup_sum: BackupSum) -> Tuple[BackupResults, BackupManifest]:
    def on_exclude(path: Path) -> None:
        # Path matched exclude patterns so was excluded from backup.
        print(f'Excluded path "{path}"')

    def on_listdir_error(directory: Path, e: OSError) -> None:
        # Failed to query directory contents while scanning source directory.
        warning(f'Failed to enumerate directory "{directory}": {e}')

    def on_metadata_error(path: Path, e: OSError) -> None:
        # Failed to query file/directory metadata when scanning source directory.
        warning(f'Failed to get metadata of "{path}": {e}')

    def on_mkdir_error(directory: Path, e: OSError) -> None:
        # Failed to create a directory when backing up files.
        warning(f'Failed to create directory "{directory}": {e}')

    def on_copy_error(source: Path, destination: Path, e: OSError) -> None:
        # Failed to back up a file.
        warning(f'Failed to copy file "{source}" to "{destination}": {e}')

    return do_backup(source_path, destination_path, exclude_patterns, backup_sum, on_exclude=on_exclude,
                     on_listdir_error=on_listdir_error, on_metadata_error=on_metadata_error,
                     on_mkdir_error=on_mkdir_error, on_copy_error=on_copy_error)


def save_manifest(backup_path: Path, manifest: BackupManifest) -> None:
    file_path = backup_path / MANIFEST_FILENAME
    try:
        write_backup_manifest(file_path, manifest)
    except OSError as e:
        raise FatalError(f'Failed to write backup manifest file: {e}', EXIT_CODE_RUNTIME_ERROR) from e


def create_complete_info(backup_path: Path, paths_skipped: bool) -> None:
    complete_info = BackupCompleteInfo(datetime.now(timezone.utc), paths_skipped)
    file_path = backup_path / COMPLETE_INFO_FILENAME
    try:
        write_backup_complete_info(file_path, complete_info)
    except OSError as e:
        # Not fatal since the completion info isn't currently used by the software.
        warning(f'Failed to write backup completion information file: {e}')


def print_results(results: BackupResults) -> None:
    print(f'+{results.files_copied} / -{results.files_removed} files')


def warning(message: str) -> None:
    print(f'WARNING: {message}')


def error(message: str) -> None:
    print(f'ERROR: {message}', file=sys.stderr)


EXIT_CODE_SUCCESS = 0
EXIT_CODE_RUNTIME_ERROR = 1
# Invalid usage is handled by argparse, uses code 2.
EXIT_CODE_LOGIC_ERROR = 3


class FatalError(Exception):
    def __init__(self, message: str, exit_code: int) -> None:
        super().__init__(message)
        self.message = message
        self.exit_code = exit_code
