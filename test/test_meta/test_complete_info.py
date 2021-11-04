from datetime import datetime, timezone

import pytest

from incremental_backup.meta.complete_info import BackupCompleteInfo, BackupCompleteInfoParseError, \
    read_backup_complete_info, write_backup_complete_info

from helpers import AssertFilesystemUnmodified


def test_write_backup_complete_info(tmpdir) -> None:
    path = tmpdir / 'complete_info.json'
    complete_info = BackupCompleteInfo(datetime(2021, 8, 13, 16, 54, 33, 1234, tzinfo=timezone.utc), True)
    write_backup_complete_info(path, complete_info)
    data = path.read_text(encoding='utf8')
    expected = '{\n    "end_time": "2021-08-13T16:54:33.001234+00:00",\n    "paths_skipped": true\n}'
    assert data == expected


def test_read_backup_complete_info_valid(tmpdir) -> None:
    path = tmpdir / 'complete_info_valid.json'
    path.write_text('{"end_time": "2020-12-30T09:34:10.123456+00:00", "paths_skipped": false}', encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_complete_info(path)

    expected = BackupCompleteInfo(datetime(2020, 12, 30, 9, 34, 10, 123456, tzinfo=timezone.utc), False)
    assert actual == expected


def test_read_backup_complete_info_invalid(tmpdir) -> None:
    datas = (
        '',
        'null'
        '{}',
        '[]',
        'true',
        '{"end_time": "2057-02-03T02:06:09"',
        '{"end_time": true, "paths_skipped": 123}'
        '{"end_time": ""',
        '{"end_time": "2100-02-14T10:20:30", "paths_skipped": true, "foo": 75.23}'
    )

    for i, data in enumerate(datas):
        path = tmpdir / f'complete_info_invalid_{i}.json'
        path.write_text(data, encoding='utf8')

        with AssertFilesystemUnmodified(tmpdir):
            with pytest.raises(BackupCompleteInfoParseError):
                read_backup_complete_info(path)


def test_read_backup_complete_info_nonexistent(tmpdir) -> None:
    path = tmpdir / 'backup_complete_info.json'
    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(FileNotFoundError):
            read_backup_complete_info(path)


def test_write_read_backup_complete_info(tmpdir) -> None:
    path = tmpdir / 'complete_info.json'
    complete_info = BackupCompleteInfo(datetime(1986, 3, 17, 9, 53, 26, 8765, tzinfo=timezone.utc), True)
    write_backup_complete_info(path, complete_info)
    actual = read_backup_complete_info(path)
    assert actual == complete_info
