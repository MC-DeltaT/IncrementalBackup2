from dataclasses import dataclass
from os import PathLike
from pathlib import Path

from .manifest import BackupManifest, read_backup_manifest
from .start_info import BackupStartInfo, read_backup_start_info
from .structure import MANIFEST_FILENAME, START_INFO_FILENAME


__all__ = [
    'BackupMetadata',
    'read_backup_metadata'
]


@dataclass
class BackupMetadata:
    """All metadata for a backup."""

    name: str
    start_info: BackupStartInfo
    manifest: BackupManifest

    # Note backup completion information is not here because it is currently not read by the application.


def read_backup_metadata(backup_directory: PathLike, /) -> BackupMetadata:
    """Reads the metadata of a backup, i.e. the name, start information, and manifest.

        :except OSError: If a metadata file could not be read.
        :except BackupStartInfoParseError: If the backup start information file could not be parsed.
        :except BackupManifestParseError: If the backup manifest file could not be parsed.
    """

    backup_directory = Path(backup_directory)
    name = backup_directory.name
    start_info = read_backup_start_info(backup_directory / START_INFO_FILENAME)
    manifest = read_backup_manifest(backup_directory / MANIFEST_FILENAME)
    return BackupMetadata(name, start_info, manifest)
