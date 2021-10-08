from datetime import datetime, timezone

import pytest

from backup_manifest import BackupManifest
from backup_meta import BACKUP_NAME_LENGTH, BackupDirectoryCreationError, create_new_backup_directory, \
    generate_backup_name, MANIFEST_FILENAME, read_backup_metadata, START_INFO_FILENAME
from backup_metadata import BackupMetadata
from backup_start_info import BackupStartInfo


def test_generate_backup_name() -> None:
    names = [generate_backup_name() for _ in range(153)]
    assert all(len(name) == BACKUP_NAME_LENGTH for name in names)
    assert all(name.isalnum() for name in names)
    assert all(name.casefold() == name for name in names)
    assert len(set(names)) == len(names)        # Very very low chance all names are the same.


def test_create_new_backup_directory_nonexistent(tmpdir) -> None:
    target_dir = tmpdir / 'target_dir'
    create_new_backup_directory(target_dir)
    assert target_dir.exists()
    entries = target_dir.listdir()
    assert len(entries) == 1
    name = entries[0].basename
    assert len(name) == BACKUP_NAME_LENGTH
    assert name.isalnum()


def test_create_new_backup_directory_invalid(tmpdir) -> None:
    target_dir = tmpdir / 'target_dir'
    target_dir.ensure()     # Note: creates file, not directory

    with pytest.raises(BackupDirectoryCreationError):
        create_new_backup_directory(target_dir)


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
            '[{"n": "", "rf": ["a", "bc", "d.efg"]}, {"n": "running", "cf": ["out", "of.ideas"]}, "^",'
            '{"n": "hmm", "rf": ["foo.pdf"], "cf": ["magic", "flower.ino"]}]')

    with pytest.raises(FileNotFoundError):
        read_backup_metadata(backup_dir)
