from dataclasses import dataclass
from pathlib import Path

from incremental_backup.meta import BackupMetadata, DATA_DIRECTORY_NAME, read_backups, ReadBackupsCallbacks
from incremental_backup._utility import StrPath


# TODO
__all__ = [
    'get_prunable_backups',
    'PrunableBackup'
]


@dataclass(frozen=True)
class PrunableBackup:
    name: str


def get_prunable_backups(directory: StrPath, /, callbacks: ReadBackupsCallbacks = ReadBackupsCallbacks()) \
        -> list[PrunableBackup]:
    """Gets backups which are not useful and may be deleted.

        :except OSError: If the directory cannot be accessed.
    """

    def is_prunable(backup: BackupMetadata, /) -> bool:
        # True if the backup manifest is empty and the data directory is empty.
        # Notably, always false if the backup is invalid. Don't delete directories that we don't understand.

        manifest_root = backup.manifest.root
        manifest_empty = not (manifest_root.copied_files or manifest_root.removed_files
            or manifest_root.removed_directories or manifest_root.subdirectories)
        if not manifest_empty:
            return False

        data_dir = Path(directory) / backup.name / DATA_DIRECTORY_NAME
        try:
            data_empty = len(list(data_dir.iterdir())) == 0
        except OSError as e:
            callbacks.on_query_entry_error(data_dir, e)
            return False

        return data_empty

    backups = read_backups(directory, callbacks)
    return [PrunableBackup(backup.name) for backup in backups if is_prunable(backup)]


# TODO: remove backups with empty manifest and empty data directory
# TODO: need to support dry run


@dataclass(frozen=True)
class PruneBackupsResults:
    """Return results of `prune_backups()`."""

    empty_backups_removed: int


@dataclass(frozen=True)
class PruneBackupsCallbacks:
    """Callbacks for events that occur in `prune_backups()`."""

    read_backups: ReadBackupsCallbacks = ReadBackupsCallbacks()
    """Callbacks for `read_backups()`."""


# TODO
