from datetime import datetime, timezone

import pytest

from incremental_backup.meta.manifest import BackupManifest
from incremental_backup.meta.metadata import BackupMetadata, read_backup_metadata
from incremental_backup.meta.start_info import BackupStartInfo
from incremental_backup.meta.structure import MANIFEST_FILENAME, START_INFO_FILENAME


def test_read_backup_metadata_ok(tmpdir) -> None:
    backup_dir = tmpdir / 'a65jh8t7opui7sa'
    start_info_path = backup_dir / START_INFO_FILENAME
    manifest_path = backup_dir / MANIFEST_FILENAME

    backup_dir.ensure_dir()
    with open(start_info_path, 'w', encoding='utf8') as file:
        file.write('{"start_time": "2021-11-22T16:15:04+00:00"}')
    with open(manifest_path, 'w', encoding='utf8') as file:
        file.write('[{"n": "", "cf": ["foo.txt", "bar.bmp"]}, {"n": "qux", "rd": ["baz"]}]')

    actual = read_backup_metadata(backup_dir)

    expected = BackupMetadata(
        'a65jh8t7opui7sa',
        BackupStartInfo(datetime(2021, 11, 22, 16, 15, 4, tzinfo=timezone.utc)),
        BackupManifest(BackupManifest.Directory('', copied_files=['foo.txt', 'bar.bmp'], subdirectories=[
            BackupManifest.Directory('qux', removed_directories=['baz'])
        ]))
    )

    assert actual == expected


def test_read_backup_metadata_nonexistent_dir(tmpdir) -> None:
    backup_dir = tmpdir / '567lkjh2378dsfg3'
    with pytest.raises(FileNotFoundError):
        read_backup_metadata(backup_dir)


def test_read_backup_metadata_missing_file(tmpdir) -> None:
    backup_dir = tmpdir / '12lk789xcx542'
    manifest_path = backup_dir / MANIFEST_FILENAME

    backup_dir.ensure_dir()
    with open(manifest_path, 'w', encoding='utf8') as file:
        file.write(
            '[{"n": "", "rf": ["a", "bc", "d.efg"]}, {"n": "running", "cf": ["out", "of.ideas"]}, "^1",'
            '{"n": "hmm", "rf": ["foo.pdf"], "cf": ["magic", "flower.ino"]}]')

    with pytest.raises(FileNotFoundError):
        read_backup_metadata(backup_dir)
