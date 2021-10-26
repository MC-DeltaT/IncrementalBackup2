from datetime import datetime, timezone

import pytest

from incremental_backup.meta.start_info import BackupStartInfo, BackupStartInfoParseError, read_backup_start_info, \
    write_backup_start_info

from helpers import AssertFilesystemUnmodified


def test_write_backup_start_info(tmpdir) -> None:
    path = tmpdir / 'start_info.json'
    start_info = BackupStartInfo(datetime(2021, 8, 13, 16, 54, 33, 1234, tzinfo=timezone.utc))
    write_backup_start_info(path, start_info)
    with open(path, 'r', encoding='utf8') as file:
        data = file.read()
    expected = '{\n    "start_time": "2021-08-13T16:54:33.001234+00:00"\n}'
    assert data == expected


def test_read_backup_start_info_valid(tmpdir) -> None:
    path = tmpdir / 'start_info_valid.json'
    with open(path, 'w', encoding='utf8') as file:
        file.write('{"start_time": "2020-12-30T09:34:10.123456+00:00"}')

    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_start_info(path)

    expected = BackupStartInfo(datetime(2020, 12, 30, 9, 34, 10, 123456, tzinfo=timezone.utc))
    assert actual == expected


def test_read_backup_start_info_invalid(tmpdir) -> None:
    datas = (
        '',
        'null'
        '{}',
        '[]',
        '2020-05-01T09:34:10.123456',
        '{"start_time": "2000-01-01T01:01:01"',
        '{"start_time": 1.20}'
        '{"start_time": ""',
        '{"start_time": "2100-02-14T00:00:00", "foo": 75.23}'
    )

    for i, data in enumerate(datas):
        path = tmpdir / f'start_info_invalid_{i}.json'
        with open(path, 'w', encoding='utf8') as file:
            file.write(data)

        with AssertFilesystemUnmodified(tmpdir):
            with pytest.raises(BackupStartInfoParseError):
                read_backup_start_info(path)


def test_read_backup_start_info_nonexistent(tmpdir) -> None:
    path = tmpdir / 'start_info_nonexistent.json'
    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(FileNotFoundError):
            read_backup_start_info(path)


def test_write_read_backup_start_info(tmpdir) -> None:
    path = tmpdir / 'start_info.json'
    start_info = BackupStartInfo(datetime(2000, 12, 2, 4, 3, 1, 405, tzinfo=timezone.utc))
    write_backup_start_info(path, start_info)
    actual = read_backup_start_info(path)
    assert actual == start_info
