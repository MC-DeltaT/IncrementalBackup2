from datetime import datetime, timezone
from pathlib import Path
import re
from typing import List, Sequence, Tuple

from ..backup import BackupSum, BackupResults, compile_exclude_pattern, do_backup
from ..exception import FatalRuntimeError
from ..meta import BackupCompleteInfo, BackupDirectoryCreationError, BackupManifest, BackupManifestParseError, \
    BackupMetadata, BackupStartInfo, BackupStartInfoParseError, COMPLETE_INFO_FILENAME, \
    create_new_backup_directory, DATA_DIRECTORY_NAME, MANIFEST_FILENAME, read_backup_metadata, START_INFO_FILENAME, \
    write_backup_complete_info, write_backup_manifest, write_backup_start_info
from ..utility import print_warning
from .command import Command


__all__ = [
    'BackupCommand'
]


# TODO? use some sort of customisable logger instead of printing straight to the console for info and warnings


class BackupCommand(Command):
    """The program command which creates new backups."""

    COMMAND_STRING = 'backup'

    def __init__(self, arguments, /) -> None:
        """
            :param arguments: The parsed command line arguments object acquired from argparse.
        """

        super().__init__(arguments)
        self.source_path: Path = arguments.source_dir
        self.target_path: Path = arguments.target_dir
        self.exclude_patterns: Sequence[re.Pattern] = arguments.exclude or ()

    def run(self) -> None:
        """Executes the backup command.

            :except FatalError: If a fatal error occurs.
        """

        self._validate_source_directory()
        self._validate_target_directory()

        self._print_config()
        print()

        print('Reading previous backups')
        previous_backups = self._read_previous_backups()
        backup_sum = BackupSum.from_backups(previous_backups)

        print('Initialising backup')
        backup_path = self._create_backup_directory()
        data_path = self._create_data_directory(backup_path)
        self._create_start_info(backup_path)

        print('Running backup operation')
        results, manifest = self._do_backup(data_path, backup_sum)

        print('Saving metadata')
        self._save_manifest(backup_path, manifest)
        self._create_complete_info(backup_path, results.paths_skipped)

        self._print_results(results)

    @staticmethod
    def add_arg_subparser(subparser, /) -> None:
        """Adds the command line argument subparser for the backup command."""

        parser = subparser.add_parser(
            BackupCommand.COMMAND_STRING, description='Creates a new backup.', help='Creates a new backup.')
        parser.add_argument('source_dir', action='store', type=Path, help='Directory to back up.')
        parser.add_argument('target_dir', action='store', type=Path, help='Directory to back up into.')
        parser.add_argument(
            '--exclude', action='append', type=compile_exclude_pattern, required=False,
            help='Path pattern(s) to exclude. Can be specified more than once')

    def _validate_source_directory(self) -> None:
        """Validates the backup source directory.
            Should mostly prevent other parts of the program from failing strangely for invalid inputs.
        """

        try:
            if not self.source_path.exists():
                raise FatalRuntimeError('Source directory not found')
            if not self.source_path.is_dir():
                raise FatalRuntimeError('Source directory is not a directory')
        except OSError as e:
            raise FatalRuntimeError(f'Failed to query source directory: {e}') from e

    def _validate_target_directory(self) -> None:
        """Validates the backup target directory.
            Should mostly prevent other parts of the program from failing strangely for invalid inputs.
        """

        try:
            if self.target_path.exists() and not self.target_path.is_dir():
                raise FatalRuntimeError('Target directory is not a directory')
        except OSError as e:
            raise FatalRuntimeError(f'Failed to query target directory: {e}') from e

    def _print_config(self) -> None:
        """Prints the configuration of the application to stdout."""

        print(f'Source directory: {self.source_path}')
        print(f'Target directory: {self.target_path}')
        print('Exclude patterns:')
        if self.exclude_patterns:
            for pattern in self.exclude_patterns:
                print(f'  {pattern.pattern}')
        else:
            print('  <none>')

    def _read_previous_backups(self) -> List[BackupMetadata]:
        """Reads existing backups' metadata from the backup target directory.

            If any backup's metadata cannot be read, prints a warning to the console and skips that backup.

            :except FatalError: If `target_path` cannot be enumerated.
        """

        def is_probably_backup(directory: Path) -> bool:
            start_info_path = directory / START_INFO_FILENAME
            manifest_path = directory / MANIFEST_FILENAME
            # Note: do not check for length of backup name, in case we change it in the future.
            return (directory.name.isascii() and directory.name.isalnum() and start_info_path.is_file()
                    and manifest_path.is_file())

        try:
            if not self.target_path.exists():
                return []
            subdirectories = [d for d in self.target_path.iterdir() if d.is_dir()]
        except OSError as e:
            raise FatalRuntimeError(f'Failed to enumerate previous backups: {e}') from e

        backups: List[BackupMetadata] = []
        for directory in subdirectories:
            if is_probably_backup(directory):
                try:
                    metadata = read_backup_metadata(directory)
                except (OSError, BackupStartInfoParseError, BackupManifestParseError) as e:
                    print_warning(f'Failed to read metadata of previous backup {directory.name}: {e}')
                else:
                    backups.append(metadata)
            else:
                print_warning(f'Found directory in target directory that is not a valid backup: "{directory.name}"')

        print(f'Read {len(backups)} previous backups')

        return backups

    def _create_backup_directory(self) -> Path:
        """Creates a new backup directory within the target directory. Prints the name of the directory to the console.

            :return: Path to the created directory.
            :except FatalError: If the directory could not be created.
        """

        try:
            backup_name = create_new_backup_directory(self.target_path)
        except BackupDirectoryCreationError as e:
            raise FatalRuntimeError(str(e)) from e
        print(f'Backup name: {backup_name}')
        return self.target_path / backup_name

    @staticmethod
    def _create_data_directory(backup_path: Path, /) -> Path:
        """Creates the backup data directory (directory which contains the copied files).

            :return: Path to the created directory.
            :except FatalError: If the directory could not be created.
        """

        path = backup_path / DATA_DIRECTORY_NAME
        try:
            path.mkdir(parents=True, exist_ok=False)
        except OSError as e:
            raise FatalRuntimeError(f'Failed to create backup data directory: {e}') from e
        return path

    @staticmethod
    def _create_start_info(backup_path: Path, /) -> None:
        """Writes the backup start information to file within the backup directory.

            :except FatalError: If the file could not be written to.
        """

        start_info = BackupStartInfo(datetime.now(timezone.utc))
        file_path = backup_path / START_INFO_FILENAME
        try:
            write_backup_start_info(file_path, start_info)
        except OSError as e:
            raise FatalRuntimeError(f'Failed to write backup start information file: {e}') from e

    def _do_backup(self, destination_path: Path, backup_sum: BackupSum) -> Tuple[BackupResults, BackupManifest]:
        """Performs the backup operation. Just a wrapper around `backup.do_backup()`.

            Prints informational messages and warnings to the console as appropriate.
        """

        def on_exclude(path: Path) -> None:
            # Path matched exclude patterns so was excluded from backup.
            print(f'Excluded path "{path}"')

        def on_listdir_error(directory: Path, e: OSError) -> None:
            # Failed to query directory contents while scanning source directory.
            print_warning(f'Failed to enumerate directory "{directory}": {e}')

        def on_metadata_error(path: Path, e: OSError) -> None:
            # Failed to query file/directory metadata when scanning source directory.
            print_warning(f'Failed to get metadata of "{path}": {e}')

        def on_mkdir_error(directory: Path, e: OSError) -> None:
            # Failed to create a directory when backing up files.
            print_warning(f'Failed to create directory "{directory}": {e}')

        def on_copy_error(source: Path, destination: Path, e: OSError) -> None:
            # Failed to back up a file.
            print_warning(f'Failed to copy file "{source}" to "{destination}": {e}')

        return do_backup(self.source_path, destination_path, self.exclude_patterns, backup_sum,
                         on_exclude=on_exclude,
                         on_listdir_error=on_listdir_error, on_metadata_error=on_metadata_error,
                         on_mkdir_error=on_mkdir_error, on_copy_error=on_copy_error)

    @staticmethod
    def _save_manifest(backup_path: Path, manifest: BackupManifest, /) -> None:
        """Writes the backup manifest to file within the backup directory.

            :except FatalError: If the file could not be written to.
        """

        file_path = backup_path / MANIFEST_FILENAME
        try:
            write_backup_manifest(file_path, manifest)
        except OSError as e:
            raise FatalRuntimeError(f'Failed to write backup manifest file: {e}') from e

    @staticmethod
    def _create_complete_info(backup_path: Path, /, paths_skipped: bool) -> None:
        """Writes the backup completion information to file within the backup directory.

            If this operation fails, just prints a warning to the console, since the completion information is not required
            by the application at this time.
        """

        complete_info = BackupCompleteInfo(datetime.now(timezone.utc), paths_skipped)
        file_path = backup_path / COMPLETE_INFO_FILENAME
        try:
            write_backup_complete_info(file_path, complete_info)
        except OSError as e:
            # Not fatal since the completion info isn't currently used by the software.
            print_warning(f'Failed to write backup completion information file: {e}')

    @staticmethod
    def _print_results(results: BackupResults, /) -> None:
        """Prints backup results to the console."""

        print(f'+{results.files_copied} / -{results.files_removed} files')
