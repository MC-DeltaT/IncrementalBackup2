import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from incremental_backup.cli.command.command import Command
from incremental_backup.cli.command.exception import CommandRuntimeError
from incremental_backup.meta import ReadBackupsCallbacks
from incremental_backup.restore import perform_restore, RestoreCallbacks, RestoreError, RestoreFilesCallbacks, RestoreResults
from incremental_backup._utility import print_warning


__all__ = [
    'RestoreCommand'
]


# TODO? a command line flag to abort the restore if any backup can't be read


class RestoreCommand(Command):
    """The program command which restores files from backups."""

    COMMAND_STRING = 'restore'

    @staticmethod
    def add_arg_subparser(subparser, /) -> None:
        """Adds the argparse subparser for the restore command."""

        parser = subparser.add_parser(RestoreCommand.COMMAND_STRING, description='Restores files from backups.',
                                      help='Restores files from backups.')
        parser.add_argument('backup_target_dir', type=Path, help='Directory containing backups to restore from.')
        parser.add_argument('destination_dir', type=Path, help='Directory to restore into.')
        parser.add_argument('backup_or_time', type=RestoreCommand._parse_backup_name_or_time,
                            nargs='?', help='Name or timestamp of latest backup to restore.')

    def __init__(self, arguments: argparse.Namespace, /) -> None:
        """
            :param arguments: The parsed command line arguments object acquired from argparse.
        """

        super().__init__(arguments)
        self.backup_target_directory: Path = arguments.backup_target_dir
        self.destination_directory: Path = arguments.destination_dir
        if arguments.backup_or_time is None:
            backup_name = None
            backup_time = None
        elif isinstance(arguments.backup_or_time, datetime):
            backup_name = None
            backup_time = arguments.backup_or_time
        else:
            backup_name = arguments.backup_or_time
            backup_time = None
        self.backup_name: Optional[str] = backup_name
        self.backup_time: Optional[datetime] = backup_time

    def run(self) -> None:
        """Executes the restore command.

            :except CommandRuntimeError: If an error occurs such that the restore operation cannot continue.
        """

        self._print_config()

        callbacks = self._restore_callbacks()

        try:
            results = perform_restore(
                self.backup_target_directory, self.destination_directory, self.backup_name, self.backup_time, callbacks)
        except RestoreError as e:
            raise CommandRuntimeError(str(e)) from e

        self._print_results(results)

    @staticmethod
    def _parse_backup_name_or_time(name_or_time: str, /) -> Union[str, datetime]:
        """Parses the "backup name or timestamp" command line argument.
            Valid values are backup names (i.e. alphanumeric) and ISO-8601 timestamps.

            :except ArgumentTypeError: If the value is neither a valid backup name nor timestamp.
        """

        if name_or_time.isascii() and name_or_time.isalnum() and len(name_or_time) >= 10:
            # Is probably a backup name.
            return name_or_time

        try:
            time = datetime.fromisoformat(name_or_time)
            if time.tzinfo is None:
                local_tz = datetime.now().astimezone().tzinfo
                time = time.replace(tzinfo=local_tz)
            return time
        except ValueError:
            pass

        raise argparse.ArgumentTypeError('Must be a backup name or ISO-8601 timestamp.')

    @staticmethod
    def _restore_callbacks() -> RestoreCallbacks:
        """Creates the callbacks for `perform_restore()`."""

        return RestoreCallbacks(
            on_before_read_previous_backups=lambda: print('Reading previous backups'),
            read_backups=ReadBackupsCallbacks(
                on_query_entry_error=lambda path, error:
                    print_warning(f'Failed to query entry in backup target directory "{path}": {error}'),
                on_read_metadata_error=lambda path, error:
                    print_warning(f'Failed to read metadata of previous backup {path.name}: {error}'),
            ),
            on_after_read_previous_backups=lambda backups: print(f'Read {len(backups)} previous backups'),
            on_selected_backups=lambda backups: print(f'Using {len(backups)} for restore'),
            on_before_initialise_restore=lambda: print('Initialising restoration'),
            on_before_restore_files=lambda: print('Copying files'),
            restore_files=RestoreFilesCallbacks(
                on_mkdir_error=lambda path, error: print_warning(f'Failed to create directory "{path}": {error}'),
                on_copy_error=lambda src, dest, error:
                    print_warning(f'Failed to copy file "{src}" to "{dest}": {error}')
            )
        )

    def _print_config(self) -> None:
        """Prints the configuration of the application to stdout."""

        print(f'Backup target directory: {self.backup_target_directory}')
        print(f'Destination directory: {self.destination_directory}')
        if self.backup_name is not None:
            print(f'Restore up to backup {self.backup_name}')
        elif self.backup_time is not None:
            print(f'Restore up to {self.backup_time.isoformat()}')
        else:
            print('Restore up to latest backup')
        print()

    @staticmethod
    def _print_results(results: RestoreResults, /) -> None:
        """Prints restore results to the console."""

        print(f'Restored {results.files_restored} files')
