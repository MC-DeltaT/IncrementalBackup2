from dataclasses import dataclass

from backup_manifest import BackupManifest
from backup_start_info import BackupStartInfo


__all__ = [
    'BackupMetadata'
]


@dataclass
class BackupMetadata:
    """All metadata for a backup."""

    name: str
    start_info: BackupStartInfo
    manifest: BackupManifest

    # Note backup completion information is not here because it is currently not read by the application.
