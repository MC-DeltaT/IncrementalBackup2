import argparse
from pathlib import Path

from incremental_backup.cli.command.command import Command


__all__ = [
    'PruneCommand'
]


class PruneCommand(Command):
    COMMAND_STRING = 'prune'

    @staticmethod
    def add_arg_subparser(subparser) -> None:
        parser = subparser.add_parser(PruneCommand.COMMAND_STRING, description='Removes unneeded backups.',
                                      help='Removes unneeded backups.')
        parser.add_argument('backup_target_dir', type=Path, help='Directory containing backups to restore from.')
        # TODO: commit flag (dry run without flag)
        # TODO: remove empty flag

    def __init__(self, arguments: argparse.Namespace, /) -> None:
        """
            :param arguments: The parsed command line arguments object acquired from argparse.
        """

        super().__init__(arguments)
        self.backup_target_directory: Path = arguments.backup_target_dir

    def run(self) -> None:
        """Executes the backup command.

            :except CommandRuntimeError: If an error occurs such that a valid backup cannot be produced.
        """

        self._print_config()

        # TODO

        self._print_results(results)

    def _print_config(self) -> None:
        """Prints the configuration of the application to stdout."""

        print(f'Backup target directory: {self.backup_target_directory}')
        # TODO

    def _print_results(self, results) -> None:
        ... # TODO
