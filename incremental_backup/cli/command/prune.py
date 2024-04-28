import argparse
from pathlib import Path

from incremental_backup._utility import print_warning
from incremental_backup.cli.command.command import Command
from incremental_backup.cli.command.exception import (
    CommandArgumentError,
    CommandRuntimeError,
)
from incremental_backup.meta import ReadBackupsCallbacks
from incremental_backup.prune import (
    BackupPrunabilityOptions,
    PruneBackupsCallbacks,
    PruneBackupsConfig,
    PruneBackupsError,
    PruneBackupsResults,
    prune_backups,
)

__all__ = ["PruneCommand"]


class PruneCommand(Command):
    """The program command which deletes unneeded backups."""

    COMMAND_STRING = "prune"

    @staticmethod
    def add_arg_subparser(subparser, /) -> None:
        """Adds the argparse subparser for the prune command."""

        parser = subparser.add_parser(
            PruneCommand.COMMAND_STRING,
            description="Removes unneeded backups.",
            help="Removes unneeded backups.",
        )
        parser.add_argument(
            "backup_target_dir",
            type=Path,
            help="Directory containing backups to operate on.",
        )
        parser.add_argument(
            "--commit",
            action="store_true",
            default=False,
            help="If not specified, don't delete anything.",
        )
        prune_modes_parser = parser.add_argument_group("Prune modes")
        # TODO: (breaking) replace underscore with hypen in flag name
        prune_modes_parser.add_argument(
            "--delete_empty",
            action="store_true",
            default=False,
            help="Delete backups not containing any changes.",
        )

    def __init__(self, arguments: argparse.Namespace) -> None:
        """
        :param arguments: The parsed command line arguments object acquired from argparse.

        :except CommandArgumentError: If the arguments are invalid.
        """

        super().__init__(arguments)
        self.backup_target_directory: Path = arguments.backup_target_dir
        self.commit: bool = arguments.commit
        self.delete_empty: bool = arguments.delete_empty

        if not self.delete_empty:
            raise CommandArgumentError("At least one prune mode must be specified.")

    def run(self) -> None:
        """Executes the prune command.

        :except CommandRuntimeError: If an error occurs such that the prune operation cannot continue.
        """

        self._print_config()

        config = self._prune_backups_config()
        callbacks = self._prune_backups_callbacks()

        try:
            results = prune_backups(self.backup_target_directory, config, callbacks)
        except PruneBackupsError as e:
            raise CommandRuntimeError(str(e)) from e

        self._print_results(results)

    def _print_config(self) -> None:
        """Prints the configuration of the application to stdout."""

        print(f"Backup target directory: {self.backup_target_directory}")
        if not self.commit:
            print("Dry run: True")
        print("Deleting:")
        if self.delete_empty:
            print("  Empty backups")
        print()

    def _prune_backups_config(self) -> PruneBackupsConfig:
        return PruneBackupsConfig(
            not self.commit,
            BackupPrunabilityOptions(prune_empty=self.delete_empty, prune_other_data=False),
        )

    @staticmethod
    def _prune_backups_callbacks() -> PruneBackupsCallbacks:
        """Creates the callbacks for `prune_backups()`."""

        return PruneBackupsCallbacks(
            on_before_read_backups=lambda: print("Reading backups"),
            read_backups=ReadBackupsCallbacks(
                on_query_entry_error=lambda path, error: print_warning(
                    f'Failed to query entry in backup target directory "{path}": {error}'
                ),
                on_read_metadata_error=lambda path, error: print_warning(
                    f"Failed to read metadata of backup {path.name}: {error}"
                ),
            ),
            on_after_read_backups=lambda backups: print(f"Read {len(backups)} backups"),
            on_selected_backups=lambda backups: print(f"Pruning {len(backups)} backups"),
            on_delete_error=lambda path, error: print_warning(f'Failed to delete backup "{path}": {error}'),
        )

    def _print_results(self, results: PruneBackupsResults) -> None:
        """Prints backup prune results to the console."""

        print()
        if not self.commit:
            print("DRY RUN - simulated results only")
        backup_count = results.total_backups_removed + results.backups_remaining
        removed_percent = 100 * results.total_backups_removed / backup_count if backup_count > 0 else 0
        print(f"Deleted {results.total_backups_removed} / {backup_count} backups ({removed_percent:.2f}%)")
        print(f"  {results.empty_backups_removed} empty")
