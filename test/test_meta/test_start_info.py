from datetime import datetime, timezone
from pathlib import Path

import pytest

from incremental_backup.meta.start_info import BackupStartInfo, BackupStartInfoParseError, read_backup_start_info_file, \
    write_backup_start_info_file

from helpers import AssertFilesystemUnmodified


def test_write_backup_start_info_file(tmpdir: Path) -> None:
    path = tmpdir / 'start_info.json'
    start_info = BackupStartInfo(datetime(2021, 8, 13, 16, 54, 33, 1234, tzinfo=timezone.utc))
    write_backup_start_info_file(path, start_info)
    data = path.read_text(encoding='utf8')
    expected = '{\n    "start_time": "2021-08-13T16:54:33.001234+00:00"\n}'
    assert data == expected


def test_read_backup_start_info_file_valid(tmpdir: Path) -> None:
    path = tmpdir / 'start_info_valid.json'
    path.write_text('{"start_time": "2020-12-30T09:34:10.123456+00:00"}', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_start_info_file(path)

    expected = BackupStartInfo(datetime(2020, 12, 30, 9, 34, 10, 123456, tzinfo=timezone.utc))
    assert actual == expected


def test_read_backup_start_info_file_invalid(tmpdir: Path) -> None:
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
        path.write_text(data, encoding='utf8')

        with AssertFilesystemUnmodified(tmpdir):
            with pytest.raises(BackupStartInfoParseError):
                read_backup_start_info_file(path)


def test_read_backup_start_info_file_nonexistent(tmpdir: Path) -> None:
    path = tmpdir / 'start_info_nonexistent.json'
    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(FileNotFoundError):
            read_backup_start_info_file(path)


def test_write_read_backup_start_info_file(tmpdir: Path) -> None:
    path = tmpdir / 'start_info.json'
    start_info = BackupStartInfo(datetime(2000, 12, 2, 4, 3, 1, 405, tzinfo=timezone.utc))
    write_backup_start_info_file(path, start_info)
    actual = read_backup_start_info_file(path)
    assert actual == start_info
