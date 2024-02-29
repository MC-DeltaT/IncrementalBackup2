from dataclasses import dataclass

from incremental_backup.meta import DATA_DIRECTORY_NAME, read_backups, ReadBackupsCallbacks
from incremental_backup._utility import StrPath


__all__ = [
    'prune_backups',
    'PruneBackupsCallbacks',
    'PruneBackupsResults'
]



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
