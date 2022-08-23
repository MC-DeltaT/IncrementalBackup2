from datetime import datetime, timezone
from pathlib import Path

import pytest

from incremental_backup.meta.manifest import BackupManifest, BackupManifestParseError
from incremental_backup.meta.meta import BACKUP_NAME_LENGTH, BackupDirectoryCreationError, BackupMetadata, \
    check_if_probably_backup, COMPLETE_INFO_FILENAME, create_new_backup_directory, DATA_DIRECTORY_NAME, \
    generate_backup_name, MANIFEST_FILENAME, read_backup_metadata, read_backups, ReadBackupsCallbacks, \
    START_INFO_FILENAME
from incremental_backup.meta.start_info import BackupStartInfo

from helpers import AssertFilesystemUnmodified, unordered_equal


def test_backup_filenames() -> None:
    assert len({COMPLETE_INFO_FILENAME, DATA_DIRECTORY_NAME, MANIFEST_FILENAME, START_INFO_FILENAME}) == 4
    assert COMPLETE_INFO_FILENAME.isascii()
    assert DATA_DIRECTORY_NAME.isascii()
    assert MANIFEST_FILENAME.isascii()
    assert START_INFO_FILENAME.isascii()


def test_check_if_probably_backup_true(tmpdir: Path) -> None:
    backup_path = tmpdir / '8e54tpnw4958t94'
    backup_path.mkdir()
    (backup_path / 'start.json').write_text('{"start_time": "2014-03-10T10:24:30.345667+00:00"}', encoding='utf8')
    (backup_path / 'manifest.json').write_text('[{"n": "", "cf": ["foobarqux"]}]', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        assert check_if_probably_backup(backup_path)


def test_check_if_probably_backup_nonexistent(tmpdir: Path) -> None:
    backup_path = tmpdir / 'askdfuhiserhu'

    with AssertFilesystemUnmodified(tmpdir):
        assert not check_if_probably_backup(backup_path)


def test_check_if_probably_backup_file(tmpdir: Path) -> None:
    backup_path = tmpdir / '459867w3gsdfsafd'
    backup_path.touch()

    with AssertFilesystemUnmodified(tmpdir):
        assert not check_if_probably_backup(backup_path)


def test_check_if_probably_backup_invalid_name(tmpdir: Path) -> None:
    backup_path = tmpdir / 'this is-not a_backup123'
    backup_path.mkdir()
    (backup_path / 'start.json').write_text('{"start_time": "2014-03-10T10:24:30.345667+00:00"}', encoding='utf8')
    (backup_path / 'manifest.json').write_text('[{"n": "", "cf": ["foobarqux"]}]', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        assert not check_if_probably_backup(backup_path)


def test_check_if_probably_backup_missing_start_info(tmpdir: Path) -> None:
    backup_path = tmpdir / '345t7hf0345bv9wc'
    backup_path.mkdir()
    (backup_path / 'manifest.json').write_text('[{"n": "", "cf": ["foobarqux"]}]', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        assert not check_if_probably_backup(backup_path)


def test_check_if_probably_backup_missing_manifest(tmpdir: Path) -> None:
    backup_path = tmpdir / '485390efdt76er'
    backup_path.mkdir()
    (backup_path / 'start.json').write_text('{"start_time": "2014-03-10T10:24:30.345667+00:00"}', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        assert not check_if_probably_backup(backup_path)


def test_read_backup_metadata_ok(tmpdir: Path) -> None:
    backup_dir = tmpdir / 'a65jh8t7opui7sa'
    start_info_path = backup_dir / 'start.json'
    manifest_path = backup_dir / 'manifest.json'

    backup_dir.mkdir()
    start_info_path.write_text('{"start_time": "2021-11-22T16:15:04+00:00"}', encoding='utf8')
    manifest_path.write_text('[{"n": "", "cf": ["foo.txt", "bar.bmp"]}, {"n": "qux", "rd": ["baz"]}]', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_metadata(backup_dir)

    expected = BackupMetadata(
        'a65jh8t7opui7sa',
        BackupStartInfo(datetime(2021, 11, 22, 16, 15, 4, tzinfo=timezone.utc)),
        BackupManifest(BackupManifest.Directory('', copied_files=['foo.txt', 'bar.bmp'], subdirectories=[
            BackupManifest.Directory('qux', removed_directories=['baz'])
        ]))
    )

    assert actual == expected


def test_read_backup_metadata_nonexistent_dir(tmpdir: Path) -> None:
    backup_dir = tmpdir / '567lkjh2378dsfg3'
    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(FileNotFoundError):
            read_backup_metadata(backup_dir)


def test_read_backup_metadata_missing_file(tmpdir: Path) -> None:
    backup_dir = tmpdir / '12lk789xcx542'
    manifest_path = backup_dir / 'manifest.json'

    backup_dir.mkdir()
    manifest_path.write_text(
        '[{"n": "", "rf": ["a", "bc", "d.efg"]}, {"n": "running", "cf": ["out", "of.ideas"]}, "^1",'
        '{"n": "hmm", "rf": ["foo.pdf"], "cf": ["magic", "flower.ino"]}]',
        encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(FileNotFoundError):
            read_backup_metadata(backup_dir)


def test_read_backups(tmpdir: Path) -> None:
    backup1_path = tmpdir / '495gw459g8w34fy07wfg'
    backup1_path.mkdir()
    (backup1_path / 'start.json').write_text('{"start_time": "2020-11-04T22:32:17.458067+00:00"}', encoding='utf8')
    (backup1_path / 'manifest.json').write_text('[{"n": "", "rf": ["foo.bar"]}]', encoding='utf8')

    backup2_path = tmpdir / '7453t0y27354ytfwyh'
    backup2_path.mkdir()
    (backup2_path / 'start.json').write_text('{"start_time": "2020-11-04T22:34:33.203584+00:00"}', encoding='utf8')
    (backup2_path / 'manifest.json').write_text('[{"n": ""}, {"n": "dir", "rd": ["rdir"]}]', encoding='utf8')

    not_backup1_path = tmpdir / 'not a backup'
    not_backup1_path.mkdir()

    not_backup2_path = tmpdir / '45tnvfjhgey5g'
    not_backup2_path.touch()

    invalid_backup_path = tmpdir / '1q023dfjasgsfdgh'
    invalid_backup_path.mkdir()
    (invalid_backup_path / 'start.json').write_text(
        '{"start_time": "2020-11-04T22:00:03.039476+00:00"}', encoding='utf8')
    (invalid_backup_path / 'manifest.json').write_text('{"n": "", "cf=["foo', encoding='utf8')

    # Not sure how to cause on_query_entry_error and on_read_metadata_error with OSError.

    read_metadata_errors: list[tuple[Path, Exception]] = []
    callbacks = ReadBackupsCallbacks(
        on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
        on_read_metadata_error=lambda path, error: read_metadata_errors.append((path, error))
    )

    with AssertFilesystemUnmodified(tmpdir):
        actual_backups = read_backups(tmpdir, callbacks)

    expected_backups = (
        BackupMetadata('495gw459g8w34fy07wfg',
            BackupStartInfo(datetime(2020, 11, 4, 22, 32, 17, 458067, timezone.utc)),
            BackupManifest(BackupManifest.Directory('', removed_files=['foo.bar']))),
        BackupMetadata('7453t0y27354ytfwyh',
            BackupStartInfo(datetime(2020, 11, 4, 22, 34, 33, 203584, timezone.utc)),
            BackupManifest(BackupManifest.Directory('',
                subdirectories=[BackupManifest.Directory('dir', removed_directories=['rdir'])])))
    )

    assert unordered_equal(actual_backups, expected_backups)

    assert len(read_metadata_errors) == 1
    assert next(True for t in read_metadata_errors if t[0] == invalid_backup_path and type(t[1]) is BackupManifestParseError)


def test_backup_name_length() -> None:
    assert BACKUP_NAME_LENGTH >= 10


def test_generate_backup_name() -> None:
    names = [generate_backup_name() for _ in range(24356)]
    assert all(len(name) == BACKUP_NAME_LENGTH for name in names)
    assert all(name.isascii() and name.isalnum() for name in names)
    assert all(name.casefold() == name for name in names)
    assert len(set(names)) == len(names)        # Very low chance any names are the same.


def test_create_new_backup_directory_nonexistent(tmpdir: Path) -> None:
    target_dir = tmpdir / 'target_dir'
    create_new_backup_directory(target_dir)
    assert target_dir.exists()
    entries = list(target_dir.iterdir())
    assert len(entries) == 1
    name = entries[0].name
    assert len(name) == BACKUP_NAME_LENGTH
    assert name.isascii() and name.isalnum()


def test_create_new_backup_directory_invalid(tmpdir: Path) -> None:
    target_dir = tmpdir / 'target_dir'
    target_dir.touch()

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(BackupDirectoryCreationError):
            create_new_backup_directory(target_dir)
