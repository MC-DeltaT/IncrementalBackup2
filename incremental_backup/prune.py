from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
import shutil
from typing import Any, Callable

from incremental_backup.meta import BackupMetadata, COMPLETE_INFO_FILENAME, DATA_DIRECTORY_NAME, MANIFEST_FILENAME, \
    read_backups, ReadBackupsCallbacks, START_INFO_FILENAME
from incremental_backup._utility import StrPath


# TODO
__all__ = [
    'is_backup_prunable',
    'prune_backups',
    'PruneBackupsCallbacks',
    'PruneBackupsResults'
]


def is_backup_prunable(backup_path: StrPath, backup_metadata: BackupMetadata, /) -> bool:
    """Checks if a backup is useless and can be deleted.
    
        :except OSError: If querying the backup contents failed.
    """

    # True if the backup manifest is empty and the data directory is empty.
    # Notably, always false if the backup is invalid. Don't delete directories that we don't understand.

    backup_path = Path(backup_path)

    manifest_root = backup_metadata.manifest.root
    manifest_nonempty = (manifest_root.copied_files or manifest_root.removed_files
        or manifest_root.removed_directories or manifest_root.subdirectories)
    if manifest_nonempty:
        return False

    # Can raise OSError
    backup_contents = {entry.name for entry in backup_path.iterdir()}
    # If there is other data than just the backup, don't delete.
    expected_contents = {START_INFO_FILENAME, MANIFEST_FILENAME, COMPLETE_INFO_FILENAME, DATA_DIRECTORY_NAME}
    if backup_contents != expected_contents:
        return False

    data_dir = backup_path / DATA_DIRECTORY_NAME
    # Can raise OSError
    data_nonempty = len(list(data_dir.iterdir())) != 0
    if data_nonempty:
        return False
    
    return True


# TODO


@dataclass(frozen=True)
class PruneBackupsResults:
    """Return results of `prune_backups()`."""

    empty_backups_removed: int
    """The number of backups removed because they contained no changed data."""


@dataclass(frozen=True)
class PruneBackupsCallbacks:
    """Callbacks for events that occur in `prune_backups()`."""

    read_backups: ReadBackupsCallbacks = ReadBackupsCallbacks()
    """Callbacks for reading backups."""

    on_after_read_backups: Callable[[Sequence[BackupMetadata]], None] = lambda backups: None
    """Called just after the backups have been read from the target directory.
        Argument is the collection of backup metadatas (in arbitrary order)."""

    on_delete_error: Callable[[Path, OSError], None] = lambda path, error: None
    """Called when an error is raised deleting a file or directory.
        First argument is the path, second argument is the raised exception."""


def prune_backups(backup_target_directory: StrPath, dry_run: bool,
        callbacks: PruneBackupsCallbacks = PruneBackupsCallbacks()) -> PruneBackupsResults:
    """Deletes backups which are not useful.
    
        :param backup_target_directory: The directory containing the backups which are being examined. I.e. the
            "target directory" from the backup creation operation.
        :param dry_run: If true, simulate the operation but don't delete any backups.
        :param callbacks: Callbacks for certain events during execution. See `PruneBackupsCallbacks`.
        :return: Summary information for the prune operation.
    """

    backup_target_directory = Path(backup_target_directory)

    # TODO: error handling
    backups = read_backups(backup_target_directory, callbacks.read_backups)

    prunable_backups: list[Path] = []
    for backup in backups:
        backup_path = backup_target_directory / backup.name
        try:
            is_prunable = is_backup_prunable(backup_path, backup)
        except OSError as e:
            # TODO: this is kinda dodgy, can we get better error handling?
            callbacks.read_backups.on_read_metadata_error(Path(e.filename), e)
        else:
            if is_prunable:
                prunable_backups.append(backup_path)

    empty_backups_removed = 0
    for backup_path in prunable_backups:
        success = True

        if not dry_run:
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
    
    return PruneBackupsResults(empty_backups_removed)
