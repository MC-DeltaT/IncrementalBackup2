from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from incremental_backup.backup.backup import BackupCallbacks, BackupError, perform_backup
from incremental_backup.backup.filesystem import ScanFilesystemCallbacks
from incremental_backup.backup.plan import ExecuteBackupPlanCallbacks
from incremental_backup.meta.manifest import BackupManifest, BackupManifestParseError, read_backup_manifest_file
from incremental_backup.meta.meta import ReadBackupsCallbacks
from incremental_backup.meta.start_info import BackupStartInfoParseError
from incremental_backup.path_exclude import PathExcludePattern

from helpers import AssertFilesystemUnmodified, dir_entries, unordered_equal, write_file_with_mtime


# TODO: clean up tests, this is quite messy. should abstract more and have more predictable file names.


def test_perform_backup_nonexistent_source(tmpdir: Path) -> None:
    source_path = tmpdir / 'source'

    target_path = tmpdir / 'target'
    (target_path / 'gmnp98w4ygf97/data').mkdir(parents=True)
    (target_path / 'gmnp98w4ygf97/data/a file').write_text('uokhrg jsdhfg8a7i4yfgw')

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: pytest.fail('Unexpected on_before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            pytest.fail(f'Unexpected on_after_read_previous_backups: {backups=}'),
        on_before_initialise_backup=lambda: pytest.fail('Unexpected on_before_initialise_backup'),
        on_created_backup_directory=lambda path: pytest.fail(f'Unexpected on_create_backup_directory: {path=}'),
        on_before_scan_source=lambda: pytest.fail('Unexpected on_before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: pytest.fail('Unexpected on_before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: pytest.fail('Unexpected on_before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(BackupError):
            perform_backup(source_path, target_path, (), callbacks)


def test_perform_backup_source_is_file(tmpdir: Path) -> None:
    source_path = tmpdir / 'source'
    source_path.write_text('hello world!')

    target_path = tmpdir / 'target'
    (target_path / '34gf98w34fgy/data').mkdir(parents=True)
    (target_path / '34gf98w34fgy/data/something').write_text('3w4g809uw58g039ghur')

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: pytest.fail('Unexpected on_before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            pytest.fail(f'Unexpected on_after_read_previous_backups: {backups=}'),
        on_before_initialise_backup=lambda: pytest.fail('Unexpected on_before_initialise_backup'),
        on_created_backup_directory=lambda path: pytest.fail(f'Unexpected on_create_backup_directory: {path=}'),
        on_before_scan_source=lambda: pytest.fail('Unexpected on_before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: pytest.fail('Unexpected on_before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: pytest.fail('Unexpected on_before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(BackupError):
            perform_backup(source_path, target_path, (), callbacks)


def test_perform_backup_target_is_file(tmpdir: Path) -> None:
    source_path = tmpdir / 'source'
    source_path.mkdir()
    (source_path / 'foo').write_text('some text here')

    target_path = tmpdir / 'target'
    target_path.write_text('hello world!')

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: pytest.fail('Unexpected on_before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            pytest.fail(f'Unexpected on_after_read_previous_backups: {backups=}'),
        on_before_initialise_backup=lambda: pytest.fail('Unexpected on_before_initialise_backup'),
        on_created_backup_directory=lambda path: pytest.fail(f'Unexpected on_create_backup_directory: {path=}'),
        on_before_scan_source=lambda: pytest.fail('Unexpected on_before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: pytest.fail('Unexpected on_before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: pytest.fail('Unexpected on_before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(BackupError):
            perform_backup(source_path, target_path, (), callbacks)


def test_perform_backup_new_target(tmpdir: Path) -> None:
    # Target directory doesn't exist.

    source_path = tmpdir / '\u1246\uA76D3fje_s\xDDrC\u01FC'
    source_path.mkdir()
    (source_path / 'foo.txt').write_text('it is Sunday')
    (source_path / 'bar').mkdir()
    (source_path / 'bar/qux').write_text('something just something')

    target_path = (tmpdir / 'mypath\uFDEA\uBDF3/doesnt\xDFFEXIsT')

    actual_callbacks: list[Any] = []

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_before_initialise_backup=lambda: actual_callbacks.append('before_initialise_backup'),
        on_created_backup_directory=lambda path: actual_callbacks.append(('created_backup_directory', path)),
        on_before_scan_source=lambda: actual_callbacks.append('before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: actual_callbacks.append('before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: actual_callbacks.append('before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        results = perform_backup(source_path, target_path, (), callbacks)
    end_time = datetime.now(timezone.utc)

    assert target_path.is_dir()
    backup_paths = list(target_path.iterdir())
    assert len(backup_paths) == 1
    backup_path = backup_paths[0]

    assert actual_callbacks == [
        'before_read_previous_backups',
        ('after_read_previous_backups', ()),
        'before_initialise_backup',
        ('created_backup_directory', backup_path),
        'before_scan_source',
        'before_copy_files',
        'before_save_metadata',
    ]

    expected_manifest = BackupManifest(
        BackupManifest.Directory('', copied_files=['foo.txt'], subdirectories=[
            BackupManifest.Directory('bar', copied_files=['qux'])
        ]))

    assert results.backup_path == backup_path
    actual_start_time = results.start_info.start_time
    assert abs((start_time - actual_start_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert results.manifest == expected_manifest
    actual_end_time = results.complete_info.end_time
    assert abs((end_time - actual_end_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert not results.complete_info.paths_skipped
    assert results.files_copied == 2
    assert results.files_removed == 0

    assert backup_path.name.isascii() and backup_path.name.isalnum() and len(backup_path.name) >= 10
    assert dir_entries(backup_path) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup_path / 'data') == {'foo.txt', 'bar'}
    assert (backup_path / 'data/foo.txt').read_text() == 'it is Sunday'
    assert dir_entries(backup_path / 'data/bar') == {'qux'}
    assert (backup_path / 'data/bar/qux').read_text() == 'something just something'

    actual_start_info_str = (backup_path / 'start.json').read_text(encoding='utf8')
    expected_start_info_str = f'{{\n    "start_time": "{actual_start_time.isoformat()}"\n}}'
    assert actual_start_info_str == expected_start_info_str

    actual_complete_info_str = (backup_path / 'completion.json').read_text(encoding='utf8')
    expected_complete_info_str = f'{{\n    "end_time": "{actual_end_time.isoformat()}",\n    "paths_skipped": false\n}}'
    assert actual_complete_info_str == expected_complete_info_str

    expected_manifest_str = \
'''[
{
"n": "",
"cf": [
"foo.txt"
]
},
{
"n": "bar",
"cf": [
"qux"
]
}
]'''
    actual_manifest_str = (backup_path / 'manifest.json').read_text(encoding='utf8')
    assert actual_manifest_str == expected_manifest_str


def test_perform_backup_no_previous_backups(tmpdir: Path) -> None:
    # Target directory exists but is empty.

    source_path = tmpdir / 'rubbish\xC2with/\u5647\uBDC1\u9C87 chars'
    source_path.mkdir(parents=True)
    (source_path / 'it\uAF87.\u78FAis').write_text('Wednesday my dudes')
    (source_path / '\x55\u6677\u8899\u0255').mkdir()
    (source_path / '\x55\u6677\u8899\u0255/funky file name').write_text('<^ funky <> file <> data ^>')

    target_path = (tmpdir / 'I  lik\uCECE  tr\uAAAAins')
    target_path.mkdir()

    actual_callbacks: list[Any] = []

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_before_initialise_backup=lambda: actual_callbacks.append('before_initialise_backup'),
        on_created_backup_directory=lambda path: actual_callbacks.append(('created_backup_directory', path)),
        on_before_scan_source=lambda: actual_callbacks.append('before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: actual_callbacks.append('before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: actual_callbacks.append('before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        results = perform_backup(source_path, target_path, (), callbacks)
    end_time = datetime.now(timezone.utc)

    assert target_path.is_dir()
    backup_paths = list(target_path.iterdir())
    assert len(backup_paths) == 1
    backup_path = backup_paths[0]

    assert actual_callbacks == [
        'before_read_previous_backups',
        ('after_read_previous_backups', ()),
        'before_initialise_backup',
        ('created_backup_directory', backup_path),
        'before_scan_source',
        'before_copy_files',
        'before_save_metadata',
    ]

    expected_manifest = BackupManifest(
        BackupManifest.Directory('', copied_files=['it\uAF87.\u78FAis'], subdirectories=[
            BackupManifest.Directory('\x55\u6677\u8899\u0255', copied_files=['funky file name'])
        ]))

    assert results.backup_path == backup_path
    actual_start_time = results.start_info.start_time
    assert abs((start_time - actual_start_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert results.manifest == expected_manifest
    actual_end_time = results.complete_info.end_time
    assert abs((end_time - actual_end_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert not results.complete_info.paths_skipped
    assert results.files_copied == 2
    assert results.files_removed == 0

    assert backup_path.name.isascii() and backup_path.name.isalnum() and len(backup_path.name) >= 10
    assert dir_entries(backup_path) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup_path / 'data') == {'it\uAF87.\u78FAis', '\x55\u6677\u8899\u0255'}
    assert (backup_path / 'data/it\uAF87.\u78FAis').read_text() == 'Wednesday my dudes'
    assert dir_entries(backup_path / 'data/\x55\u6677\u8899\u0255') == {'funky file name'}
    assert (backup_path / 'data/\x55\u6677\u8899\u0255/funky file name').read_text() == \
           '<^ funky <> file <> data ^>'

    actual_start_info_str = (backup_path / 'start.json').read_text(encoding='utf8')
    expected_start_info_str = f'{{\n    "start_time": "{actual_start_time.isoformat()}"\n}}'
    assert actual_start_info_str == expected_start_info_str

    actual_complete_info_str = (backup_path / 'completion.json').read_text(encoding='utf8')
    expected_complete_info_str = f'{{\n    "end_time": "{actual_end_time.isoformat()}",\n    "paths_skipped": false\n}}'
    assert actual_complete_info_str == expected_complete_info_str

    expected_manifest_str = \
'''[
{
"n": "",
"cf": [
"it\uAF87.\u78FAis"
]
},
{
"n": "\x55\u6677\u8899\u0255",
"cf": [
"funky file name"
]
}
]'''
    actual_manifest_str = (backup_path / 'manifest.json').read_text(encoding='utf8')
    assert actual_manifest_str == expected_manifest_str


def test_perform_backup_some_previous_backups(tmpdir: Path) -> None:
    # Target directory has some previous backups.

    target_path = tmpdir / 'put the data here!'
    target_path.mkdir()

    backup1_path = target_path / 'sadhf8o3947yfqgfaw'
    backup1_path.mkdir()
    (backup1_path / 'start.json').write_text('{"start_time": "2021-06-20T03:37:27.435676+00:00"}', encoding='utf8')
    (backup1_path / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["root\uA63Bfile1.mp4", "ro\u2983ot_fi\x90le2.exe"]},
            {"n": "dir1\u1076\u0223", "cf": ["dir1\u1076\u0223_file1", "dir1\u1076\u0223_file@@.tij"]},
            {"n": "dirXYZ", "cf": ["dirXYZ_file.ino"]}]''',
        encoding='utf8')
    (backup1_path / 'data').mkdir()
    (backup1_path / 'data/root\uA63Bfile1.mp4').write_text('rootfile1.mp4 backup1')
    (backup1_path / 'data/ro\u2983ot_fi\x90le2.exe').write_text('root_file2.exe backup1')
    (backup1_path / 'data/dir1\u1076\u0223').mkdir()
    (backup1_path / 'data/dir1\u1076\u0223/dir1\u1076\u0223_file1').write_text('dir1_file1 backup1')
    (backup1_path / 'data/dir1\u1076\u0223/dir1\u1076\u0223_file@@.tij').write_text('dir1_file@@.tij backup1')
    (backup1_path / 'data/dir1\u1076\u0223/dirXYZ').mkdir()
    (backup1_path / 'data/dir1\u1076\u0223/dirXYZ/dirXYZ_file.ino').write_text('dirXYZ_file.ino backup1')
    (backup1_path / 'completion.json').write_text(
        '{"end_time": "2021-06-20T03:38:28.435676+00:00", "paths_skipped": false}', encoding='utf8')

    backup2_path = target_path / 'gsel45o8ise45ytq87'
    backup2_path.mkdir()
    (backup2_path / 'start.json').write_text('{"start_time": "2021-07-01T13:52:21.983451+00:00"}', encoding='utf8')
    (backup2_path / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["root_file3.txt"], "rf": ["root\uA63Bfile1.mp4"]},
            {"n": "dir1\u1076\u0223", "cf": ["dir1\u1076\u0223_file1"]},
            "^1",
            {"n": "temp", "cf": ["x.y"]}]''',
        encoding='utf8')
    (backup2_path / 'data').mkdir()
    (backup2_path / 'data/root_file3.txt').write_text('root_file3.txt backup2')
    (backup2_path / 'data/dir1\u1076\u0223').mkdir()
    (backup2_path / 'data/dir1\u1076\u0223/dir1\u1076\u0223_file1').write_text('dir1_file1 backup2')
    (backup2_path / 'data/temp').mkdir()
    (backup2_path / 'data/temp/x.y').write_text('x.y backup2')
    (backup2_path / 'completion.json').write_text(
        '{"start_time": "2021-07-01T13:55:46.983451+00:00", "paths_skipped": false}', encoding='utf8')

    backup3_path = target_path / '0345guyes8yfg73'
    backup3_path.mkdir()
    (backup3_path / 'start.json').write_text('{"start_time": "2021-09-18T09:47:11.879254+00:00"}', encoding='utf8')
    (backup3_path / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["root\uA63Bfile1.mp4", "ro\u2983ot_fi\x90le2.exe"]},
            {"n": "dir2", "cf": ["\uF000\uBAA4\u3404\xEA\uAEF1"]},
            "^1",
            {"n": "dir1\u1076\u0223", "rd": ["dirXYZ"]}]''',
        encoding='utf8')
    (backup3_path / 'data').mkdir()
    (backup3_path / 'data/root\uA63Bfile1.mp4').write_text('rootfile1.mp4 backup3')
    (backup3_path / 'data/ro\u2983ot_fi\x90le2.exe').write_text('root_file2.exe backup3')
    (backup3_path / 'data/dir2').mkdir()
    (backup3_path / 'data/dir2/\uF000\uBAA4\u3404\xEA\uAEF1').write_text('foobar backup3')
    (backup3_path / 'completion.json').write_text(
        '{"start_time": "2021-09-18T09:48:07.879254+00:00", "paths_skipped": false}', encoding='utf8')

    source_path = tmpdir / 'this\u865Cneeds\u4580to\u9B93bebackedup'
    source_path.mkdir()
    write_file_with_mtime(source_path / 'root\uA63Bfile1.mp4', 'rootfile1.mp4 backup3',
                          datetime(2021, 9, 5, 0, 43, 16, tzinfo=timezone.utc))     # Existing unmodified
    # ro\u2983ot_fi\x90le2.exe removed
    (source_path / 'root_file3.txt').write_text('root_file3.txt new content')   # Existing modified
    (source_path / 'dir1\u1076\u0223').mkdir()  # Existing
    write_file_with_mtime(source_path / 'dir1\u1076\u0223/dir1\u1076\u0223_file1', 'dir1_file1 backup2',
                          datetime(2021, 7, 1, 9, 32, 59, tzinfo=timezone.utc))     # Existing unmodified
    (source_path / 'dir1\u1076\u0223/dir1\u1076\u0223_file@@.tij').write_text('something NEW')  # Existing modified
    (source_path / 'dir1\u1076\u0223/dir1\u1076\u0223_file3').write_text('dir1_file3 new')      # New
    # dir2 / \uF000\uBAA4\u3404\xEA\uAEF1 removed
    (source_path / 'dir2/dir2_\u45631').mkdir(parents=True)     # New
    (source_path / 'dir2/dir2_\u45631/myfile.myfile').write_text('myfile and also mycontents')  # New
    (source_path / 'temp').mkdir()      # Existing, excluded (removed)
    # temp / x.y removed
    (source_path / 'temp/\u7669.\u5AAB').write_text('magic')    # New, excluded
    (source_path / 'new_dir!').mkdir()      # New
    (source_path / 'new_dir!/new file').write_text('its a new file!')       # New

    exclude_patterns = ('/temp/',)
    exclude_patterns = tuple(map(PathExcludePattern, exclude_patterns))

    actual_callbacks: list[Any] = []

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_before_initialise_backup=lambda: actual_callbacks.append('before_initialise_backup'),
        on_created_backup_directory=lambda path: actual_callbacks.append(('created_backup_directory', path)),
        on_before_scan_source=lambda: actual_callbacks.append('before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: actual_callbacks.append(('exclude', path)),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: actual_callbacks.append('before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: actual_callbacks.append('before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        results = perform_backup(source_path, target_path, exclude_patterns, callbacks)
    end_time = datetime.now(timezone.utc)

    backup_path = (set(target_path.iterdir()) - {backup1_path, backup2_path, backup3_path}).pop()

    assert len(actual_callbacks) == 8
    assert actual_callbacks[0] == 'before_read_previous_backups'
    assert actual_callbacks[1][0] == 'after_read_previous_backups'
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal(
        [b.name for b in actual_callbacks[1][1]], ['sadhf8o3947yfqgfaw', 'gsel45o8ise45ytq87', '0345guyes8yfg73'])
    assert actual_callbacks[2] == 'before_initialise_backup'
    assert actual_callbacks[3] == ('created_backup_directory', backup_path)
    assert actual_callbacks[4] == 'before_scan_source'
    assert actual_callbacks[5] == ('exclude', source_path / 'temp')
    assert actual_callbacks[6] == 'before_copy_files'
    assert actual_callbacks[7] == 'before_save_metadata'

    assert results.backup_path == backup_path
    actual_start_time = results.start_info.start_time
    assert abs((start_time - actual_start_time).total_seconds()) < METADATA_TIME_TOLERANCE
    actual_end_time = results.complete_info.end_time
    assert abs((end_time - actual_end_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert not results.complete_info.paths_skipped
    assert results.files_copied == 5
    assert results.files_removed == 3

    actual_manifest = results.manifest
    assert actual_manifest.root.copied_files == ['root_file3.txt']
    assert actual_manifest.root.removed_files == ['ro\u2983ot_fi\x90le2.exe']
    assert actual_manifest.root.removed_directories == ['temp']
    assert len(actual_manifest.root.subdirectories) == 3
    dir1 = next(d for d in actual_manifest.root.subdirectories if d.name == 'dir1\u1076\u0223')
    assert unordered_equal(dir1.copied_files, ('dir1\u1076\u0223_file@@.tij', 'dir1\u1076\u0223_file3'))
    assert dir1.removed_files == []
    assert dir1.removed_directories == []
    assert dir1.subdirectories == []
    dir2 = next(d for d in actual_manifest.root.subdirectories if d.name == 'dir2')
    assert dir2 == BackupManifest.Directory('dir2', removed_files=['\uF000\uBAA4\u3404\xEA\uAEF1'], subdirectories=[
        BackupManifest.Directory('dir2_\u45631', copied_files=['myfile.myfile'])
    ])
    new_dir = next(d for d in actual_manifest.root.subdirectories if d.name == 'new_dir!')
    assert new_dir == BackupManifest.Directory('new_dir!', copied_files=['new file'])

    assert backup_path.name.isascii() and backup_path.name.isalnum() and len(backup_path.name) >= 10
    assert dir_entries(backup_path) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup_path / 'data') == {'root_file3.txt', 'dir1\u1076\u0223', 'dir2', 'new_dir!'}
    assert (backup_path / 'data/root_file3.txt').read_text() == 'root_file3.txt new content'
    assert dir_entries(backup_path / 'data/dir1\u1076\u0223') == \
           {'dir1\u1076\u0223_file@@.tij', 'dir1\u1076\u0223_file3'}
    assert (backup_path / 'data/dir1\u1076\u0223/dir1\u1076\u0223_file@@.tij').read_text() == 'something NEW'
    assert (backup_path / 'data/dir1\u1076\u0223/dir1\u1076\u0223_file3').read_text() == 'dir1_file3 new'
    assert dir_entries(backup_path / 'data/dir2') == {'dir2_\u45631'}
    assert dir_entries(backup_path / 'data/dir2/dir2_\u45631') == {'myfile.myfile'}
    assert (backup_path / 'data/dir2/dir2_\u45631/myfile.myfile').read_text() == 'myfile and also mycontents'
    assert dir_entries(backup_path / 'data/new_dir!') == {'new file'}
    assert (backup_path / 'data/new_dir!/new file').read_text() == 'its a new file!'

    actual_start_info_str = (backup_path / 'start.json').read_text(encoding='utf8')
    expected_start_info_str = f'{{\n    "start_time": "{actual_start_time.isoformat()}"\n}}'
    assert actual_start_info_str == expected_start_info_str

    actual_complete_info_str = (backup_path / 'completion.json').read_text(encoding='utf8')
    expected_complete_info_str = f'{{\n    "end_time": "{actual_end_time.isoformat()}",\n    "paths_skipped": false\n}}'
    assert actual_complete_info_str == expected_complete_info_str

    # Don't think it's feasible to check the manifest without parsing it, because filesystem ordering is not guaranteed.
    actual_manifest_from_file = read_backup_manifest_file(backup_path / 'manifest.json')
    assert actual_manifest_from_file == actual_manifest


def test_perform_backup_some_invalid_backups(tmpdir: Path) -> None:
    # Target directory has some previous backups and invalid/not backups.

    target_path = tmpdir / 'foo \u115A\xBA\u7AD9bar\u82C5\u5C70'
    target_path.mkdir()

    # Missing start information.
    invalid1 = target_path / '9458guysd9gyw37'
    invalid1.mkdir()
    (invalid1 / 'manifest.json').write_text('[{"n": ""}]', encoding='utf8')
    (invalid1 / 'completion.json').write_text(
        '{"end_time": "2021-04-05T17:33:03.435734+00:00", "paths_skipped": false}', encoding='utf8')

    # Missing manifest.
    invalid2 = target_path / '859tfhgsidth574shg'
    invalid2.mkdir()
    (invalid2 / 'start.json').write_text('{"start_time": "2021-02-18T14:33:03.435734+00:00"}', encoding='utf8')
    (invalid2 / 'completion.json').write_text(
        '{"end_time": "2021-02-18T17:33:03.234723+00:00", "paths_skipped": false}', encoding='utf8')

    # Malformed start information.
    invalid3 = target_path / '90435fgjwf43fy43'
    invalid3.mkdir()
    (invalid3 / 'start.json').write_text('{"start_time": ', encoding='utf8')
    (invalid3 / 'manifest.json').write_text('[{"n": "", "cf": ["foo.txt"]}]', encoding='utf8')
    (invalid3 / 'completion.json').write_text(
        '{"end_time": "2021-02-03T04:55:44.123654+00:00", "paths_skipped": true}', encoding='utf8')

    # Malformed manifest.
    invalid4 = target_path / '038574tq374gfh'
    invalid4.mkdir()
    (invalid4 / 'start.json').write_text('{"start_time": "2021-01-11T14:28:41.435734+00:00"}', encoding='utf8')
    (invalid4 / 'manifest.json').write_text('', encoding='utf8')
    (invalid4 / 'completion.json').write_text(
        '{"end_time": "2021-01-11T17:33:03.203463+00:00", "paths_skipped": false}', encoding='utf8')

    # Directory name not alphanumeric.
    invalid5 = target_path / 'not @lph&numer!c'
    invalid5.mkdir()

    # Not a directory.
    invalid6 = target_path / '78034rg086a7wtf'
    invalid6.write_text('hey this isnt a backup directory!')

    backup1 = target_path / '83547tgwyedfg'
    backup1.mkdir()
    (backup1 / 'data').mkdir()
    (backup1 / 'data/foo.txt').write_text('foo.txt backup1')
    (backup1 / 'data/bar').mkdir()
    (backup1 / 'data/bar/qux.png').write_text('qux.png backup1')
    (backup1 / 'start.json').write_text('{"start_time": "2021-01-01T01:01:01.000001+00:00"}', encoding='utf8')
    (backup1 / 'manifest.json').write_text(
        '[{"n": "", "cf": ["foo.txt"]}, {"n": "bar", "cf": ["qux.png"]}]', encoding='utf8')
    (backup1 / 'completion.json').write_text(
        '{"end_time": "2021-01-01T02:02:02.000002+00:00", "paths_skipped": false}', encoding='utf8')

    backup2 = target_path / '6789345g3w4ywfd'
    backup2.mkdir()
    (backup2 / 'data').mkdir()
    (backup2 / 'data/dir').mkdir()
    (backup2 / 'data/dir/file').write_text('file backup2')
    (backup2 / 'start.json').write_text('{"start_time": "2021-04-06T08:10:12.141618+00:00"}', encoding='utf8')
    (backup2 / 'manifest.json').write_text(
        '[{"n": "", "rf": ["foo.txt"]}, {"n": "dir", "cf": ["file"]}]', encoding='utf8')
    (backup2 / 'completion.json').write_text(
        '{"end_time": "2021-04-06T08:11:02.247678+00:00", "paths_skipped": false}', encoding='utf8')

    source_path = tmpdir / '\uBDD6-D-\uE13D_\uBF42'
    source_path.mkdir()
    (source_path / 'new.txt').write_text('new.txt NEW')     # New
    # bar removed
    (source_path / 'dir').mkdir()
    write_file_with_mtime(source_path / 'dir/file', 'file backup2',
                          datetime(2021, 4, 5, 9, 32, 59, tzinfo=timezone.utc))     # Existing unmodified

    actual_callbacks: list[Any] = []

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error: actual_callbacks.append(('invalid_backup', path, error)),
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_before_initialise_backup=lambda: actual_callbacks.append('before_initialise_backup'),
        on_created_backup_directory=lambda path: actual_callbacks.append(('created_backup_directory', path)),
        on_before_scan_source=lambda: actual_callbacks.append('before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: actual_callbacks.append('before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: actual_callbacks.append('before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        results = perform_backup(source_path, target_path, (), callbacks)
    end_time = datetime.now(timezone.utc)

    backup_path = (set(target_path.iterdir()) -
                   {invalid1, invalid2, invalid3, invalid4, invalid5, invalid6, backup1, backup2}).pop()

    assert len(actual_callbacks) == 9
    assert actual_callbacks[0] == 'before_read_previous_backups'
    assert unordered_equal([(c, path, type(error)) for c, path, error in actual_callbacks[1:3]], [
        ('invalid_backup', target_path / '038574tq374gfh', BackupManifestParseError),
        ('invalid_backup', target_path / '90435fgjwf43fy43', BackupStartInfoParseError)
    ])
    assert actual_callbacks[3][0] == 'after_read_previous_backups'
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal([b.name for b in actual_callbacks[3][1]], ['83547tgwyedfg', '6789345g3w4ywfd'])
    assert actual_callbacks[4] == 'before_initialise_backup'
    assert actual_callbacks[5] == ('created_backup_directory', backup_path)
    assert actual_callbacks[6] == 'before_scan_source'
    assert actual_callbacks[7] == 'before_copy_files'
    assert actual_callbacks[8] == 'before_save_metadata'

    expected_manifest = BackupManifest(
        BackupManifest.Directory('', copied_files=['new.txt'], removed_directories=['bar']))

    assert results.backup_path == backup_path
    actual_start_time = results.start_info.start_time
    assert abs((start_time - actual_start_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert results.manifest == expected_manifest
    actual_end_time = results.complete_info.end_time
    assert abs((end_time - actual_end_time).total_seconds()) < METADATA_TIME_TOLERANCE
    assert not results.complete_info.paths_skipped
    assert results.files_copied == 1
    assert results.files_removed == 1

    assert backup_path.name.isascii() and backup_path.name.isalnum() and len(backup_path.name) >= 10
    assert dir_entries(backup_path) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup_path / 'data') == {'new.txt'}
    assert (backup_path / 'data/new.txt').read_text() == 'new.txt NEW'

    actual_start_info_str = (backup_path / 'start.json').read_text(encoding='utf8')
    expected_start_info_str = f'{{\n    "start_time": "{actual_start_time.isoformat()}"\n}}'
    assert actual_start_info_str == expected_start_info_str

    actual_complete_info_str = (backup_path / 'completion.json').read_text(encoding='utf8')
    expected_complete_info_str = f'{{\n    "end_time": "{actual_end_time.isoformat()}",\n    "paths_skipped": false\n}}'
    assert actual_complete_info_str == expected_complete_info_str

    actual_manifest_str = (backup_path / 'manifest.json').read_text(encoding='utf8')
    expected_manifest_str = \
'''[
{
"n": "",
"cf": [
"new.txt"
],
"rd": [
"bar"
]
}
]'''

    assert actual_manifest_str == expected_manifest_str


def test_perform_backup_skip_empty(tmpdir: Path) -> None:
    # Target directory exists but is empty.

    source_path = tmpdir / 'source'
    source_path.mkdir()

    target_path = tmpdir / 'target'
    target_path.mkdir()

    actual_callbacks: list[Any] = []

    callbacks = BackupCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_before_initialise_backup=lambda: actual_callbacks.append('before_initialise_backup'),
        on_created_backup_directory=lambda path: actual_callbacks.append(('created_backup_directory', path)),
        on_before_scan_source=lambda: actual_callbacks.append('before_scan_source'),
        scan_source=ScanFilesystemCallbacks(
            on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
            on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
            on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
        ),
        on_before_copy_files=lambda: actual_callbacks.append('before_copy_files'),
        execute_plan=ExecuteBackupPlanCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error}=')
        ),
        on_before_save_metadata=lambda: actual_callbacks.append('before_save_metadata'),
        on_write_complete_info_error=lambda path, error:
            pytest.fail(f'Unexpected on_write_complete_info_error: {path=} {error=}')
    )

    with AssertFilesystemUnmodified(tmpdir):
        results = perform_backup(source_path, target_path, (), callbacks, skip_empty=True)

    assert results is None

    assert actual_callbacks == [
        'before_read_previous_backups',
        ('after_read_previous_backups', ()),
        'before_scan_source'
    ]


METADATA_TIME_TOLERANCE = 5     # Seconds
