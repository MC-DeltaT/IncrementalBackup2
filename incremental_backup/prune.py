import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from incremental_backup._utility import StrPath
from incremental_backup.meta import (
    COMPLETE_INFO_FILENAME,
    DATA_DIRECTORY_NAME,
    MANIFEST_FILENAME,
    START_INFO_FILENAME,
    BackupMetadata,
    ReadBackupsCallbacks,
    read_backups,
)

__all__ = [
    "BackupPrunabilityOptions",
    "is_backup_prunable",
    "prune_backups",
    "PruneBackupsCallbacks",
    "PruneBackupsError",
    "PruneBackupsResults",
]


@dataclass
class BackupPrunabilityOptions:
    """Options for deciding what backups may be pruned."""

    prune_empty: bool
    """If true, backups containing no copied or removed files are eligible for deletion."""

    prune_other_data: bool
    """If true, allow deleting backups containing files not recognised by this application."""


def is_backup_prunable(
    backup_path: StrPath,
    backup_metadata: BackupMetadata,
    options: BackupPrunabilityOptions,
    /,
) -> bool:
    """Checks if a backup is useless and can be deleted.

    :except OSError: If querying the backup contents failed.
    """

    backup_path = Path(backup_path)

    def is_backup_empty() -> bool:
        manifest_root = backup_metadata.manifest.root
        manifest_nonempty = (
            manifest_root.copied_files
            or manifest_root.removed_files
            or manifest_root.removed_directories
            or manifest_root.subdirectories
        )
        if manifest_nonempty:
            return False

        data_dir = backup_path / DATA_DIRECTORY_NAME
        # Can raise OSError
        data_nonempty = len(list(data_dir.iterdir())) > 0
        if data_nonempty:
            return False

        return True

    def backup_contains_other_data() -> bool:
        # Can raise OSError
        backup_contents = {entry.name for entry in backup_path.iterdir()}
        expected_contents = {
            START_INFO_FILENAME,
            MANIFEST_FILENAME,
            COMPLETE_INFO_FILENAME,
            DATA_DIRECTORY_NAME,
        }
        return backup_contents != expected_contents

    prunable = False
    if options.prune_empty and is_backup_empty():
        prunable = True

    if prunable and (not options.prune_other_data) and backup_contains_other_data():
        # If there is other data than just the backup, don't delete.
        prunable = False

    return prunable


@dataclass
class PruneBackupsConfig:
    dry_run: bool
    """If true, simulate the prune operation without modifying the filesystem."""

    prunability_options: BackupPrunabilityOptions


@dataclass(frozen=True)
class PruneBackupsCallbacks:
    """Callbacks for events that occur in `prune_backups()`."""

    on_before_read_backups: Callable[[], None] = lambda: None
    """Called just before reading backups from the backup target directory."""

    read_backups: ReadBackupsCallbacks = ReadBackupsCallbacks()
    """Callbacks for reading backups."""

    on_after_read_backups: Callable[[Sequence[BackupMetadata]], None] = lambda backups: None
    """Called just after the backups have been read from the target directory.
        Argument is the collection of backup metadatas (in arbitrary order)."""

    on_selected_backups: Callable[[Sequence[BackupMetadata]], None] = lambda backups: None
    """Called after selecting which backups to prune."""

    on_delete_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when an error is raised deleting a file or directory.
        First argument is the path, second argument is the raised exception."""


@dataclass(frozen=True)
class PruneBackupsResults:
    """Return results of `prune_backups()`."""

    empty_backups_removed: int
    """The number of backups removed because they contained no changed data."""

    total_backups_removed: int
    """Total number of backups removed."""

    backups_remaining: int
    """The number of valid backups not removed."""


def prune_backups(
    backup_target_directory: StrPath,
    config: PruneBackupsConfig,
    callbacks: PruneBackupsCallbacks = PruneBackupsCallbacks(),
) -> PruneBackupsResults:
    """Deletes backups which are not useful.

    :param backup_target_directory: The directory containing the backups which are being examined. I.e. the
        "target directory" from the backup creation operation.
    :param config: Options to tune what and how backups are pruned.
    :param callbacks: Callbacks for certain events during execution. See `PruneBackupsCallbacks`.
    :return: Summary information for the prune operation.
    :except PruneBackupsError: If an error occurs that prevents the prune operation from completing.
    """

    backup_target_directory = Path(backup_target_directory)

    callbacks.on_before_read_backups()

    try:
        backups = read_backups(backup_target_directory, callbacks.read_backups)
    except OSError as e:
        raise PruneBackupsError(f"Failed to query backup target directory: {e}") from e
    callbacks.on_after_read_backups(tuple(backups))

    prunable_backups: list[BackupMetadata] = []
    for backup in backups:
        backup_path = backup_target_directory / backup.name
        try:
            is_prunable = is_backup_prunable(backup_path, backup, config.prunability_options)
        except OSError as e:
            # TODO: this is kinda dodgy, can we get better error handling?
            callbacks.read_backups.on_read_metadata_error(Path(e.filename), e)
        else:
            if is_prunable:
                prunable_backups.append(backup)
    callbacks.on_selected_backups(tuple(prunable_backups))

    empty_backups_removed = 0
    for backup in prunable_backups:
        success = True

        if not config.dry_run:
            backup_path = backup_target_directory / backup.name

            def on_rmtree_error(function: Any, path: str, exc_info: tuple[type[OSError], OSError, Any]) -> None:
                callbacks.on_delete_error(Path(path), exc_info[1])
                nonlocal success
                success = False

            try:
                shutil.rmtree(backup_path, ignore_errors=False, onerror=on_rmtree_error)
            except OSError as e:
                # Unclear if exceptions can still occur when onerror is provided.
                callbacks.on_delete_error(backup_path, e)
                success = False

        if success:
            empty_backups_removed += 1

    total_backups_removed = empty_backups_removed
    backups_remaining = len(backups) - total_backups_removed
    return PruneBackupsResults(empty_backups_removed, total_backups_removed, backups_remaining)


class PruneBackupsError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
