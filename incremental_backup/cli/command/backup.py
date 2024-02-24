import argparse
from pathlib import Path
from typing import Sequence

from incremental_backup.backup import BackupCallbacks, BackupError, BackupResults, ExecuteBackupPlanCallbacks, \
    perform_backup, ScanFilesystemCallbacks
from incremental_backup.cli.command.command import Command
from incremental_backup.cli.command.exception import CommandRuntimeError
from incremental_backup.meta import ReadBackupsCallbacks
from incremental_backup.path_exclude import PathExcludePattern
from incremental_backup._utility import print_warning


__all__ = [
    'BackupCommand'
]


class BackupCommand(Command):
    """The program command which creates new backups."""

    COMMAND_STRING = 'backup'

    @staticmethod
    def add_arg_subparser(subparser, /) -> None:
        """Adds the argparse subparser for the backup command."""

        parser = subparser.add_parser(
            BackupCommand.COMMAND_STRING, description='Creates a new backup.', help='Creates a new backup.')
        parser.add_argument('source_dir', type=Path, help='Directory to back up.')
        parser.add_argument('target_dir', type=Path, help='Directory to back up into.')
        parser.add_argument(
            '--exclude', nargs='+', type=PathExcludePattern, required=False, help='Path patterns to exclude.')

    def __init__(self, arguments: argparse.Namespace, /) -> None:
        """
            :param arguments: The parsed command line arguments object acquired from argparse.
        """

        super().__init__(arguments)
        self.source_path: Path = arguments.source_dir
        self.target_path: Path = arguments.target_dir
        self.exclude_patterns: Sequence[PathExcludePattern] = arguments.exclude or ()

    def run(self) -> None:
        """Executes the backup command.

            :except CommandRuntimeError: If an error occurs such that a valid backup cannot be produced.
        """

        self._print_config()

        callbacks = self._backup_callbacks()

        try:
            results = perform_backup(self.source_path, self.target_path, self.exclude_patterns, callbacks)
        except BackupError as e:
            raise CommandRuntimeError(str(e)) from e

        self._print_results(results)

    @staticmethod
    def _backup_callbacks() -> BackupCallbacks:
        """Creates the callbacks for `perform_backup()`."""

        return BackupCallbacks(
            on_before_read_previous_backups=lambda: print('Reading previous backups'),
            read_backups=ReadBackupsCallbacks(
                on_query_entry_error=lambda path, error:
                    print_warning(f'Failed to query entry in target directory "{path}": {error}'),
                on_read_metadata_error=lambda path, error:
                    print_warning(f'Failed to read metadata of previous backup {path.name}: {error}'),
            ),
            on_after_read_previous_backups=lambda backups: print(f'Read {len(backups)} previous backups'),
            on_before_initialise_backup=lambda: print('Initialising backup'),
            on_created_backup_directory=lambda path: print(f'Backup name: {path.name}'),
            on_before_scan_source=lambda: print('Scanning source directory'),
            scan_source=ScanFilesystemCallbacks(
                on_exclude=lambda path: print(f'Excluded path "{path}"'),
                on_listdir_error=lambda path, error: print_warning(f'Failed to enumerate directory "{path}": {error}'),
                on_metadata_error=lambda path, error: print_warning(f'Failed to get metadata of "{path}": {error}')
            ),
            on_before_copy_files=lambda: print('Copying files'),
            execute_plan=ExecuteBackupPlanCallbacks(
                on_mkdir_error=lambda path, error: print_warning(f'Failed to create directory "{path}": {error}'),
                on_copy_error=lambda src, dest, error:
                    print_warning(f'Failed to copy file "{src}" to "{dest}": {error}')
            ),
            on_before_save_metadata=lambda: print('Saving metadata'),
            on_write_complete_info_error=lambda path, error:
                print_warning(f'Failed to write backup completion information file: {error}')
        )

    def _print_config(self) -> None:
        """Prints the configuration of the application to stdout."""

        print(f'Source directory: {self.source_path}')
        print(f'Target directory: {self.target_path}')
        print('Exclude patterns:')
        if self.exclude_patterns:
            for pattern in self.exclude_patterns:
                print(f'  {pattern}')
        else:
            print('  <none>')
        print()

    @staticmethod
    def _print_results(results: BackupResults, /) -> None:
        """Prints backup results to the console."""

        print(f'+{results.files_copied} / -{results.files_removed} files')
