from datetime import datetime, timezone
import os
import re

from incremental_backup.main import script_main
from incremental_backup.meta.manifest import BackupManifest, read_backup_manifest

from helpers import AssertFilesystemUnmodified, dir_entries, unordered_equal


# Doesn't matter what this is, it isn't used by the program.
PROGRAM_NAME_ARG = 'incremental_backup.py'


def test_no_args() -> None:
    exit_code = script_main((PROGRAM_NAME_ARG,))
    assert exit_code == 1


def test_invalid_command() -> None:
    exit_code = script_main((PROGRAM_NAME_ARG, 'magic_command', 'foo', 'bar'))
    assert exit_code == 1


def test_backup_no_args() -> None:
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup'))
    assert exit_code == 1


def test_backup_nonexistent_source(tmpdir) -> None:
    source_path = tmpdir / 'source'
    target_path = tmpdir / 'target'
    (target_path / 'gmnp98w4ygf97' / 'data').mkdir(parents=True)
    (target_path / 'gmnp98w4ygf97' / 'data' / 'a file').write_text('uokhrg jsdhfg8a7i4yfgw')
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 2


def test_backup_source_is_file(tmpdir) -> None:
    source_path = tmpdir / 'source'
    source_path.write_text('hello world!')
    target_path = tmpdir / 'target'
    (target_path / '34gf98w34fgy' / 'data').mkdir(parents=True)
    (target_path / '34gf98w34fgy' / 'data' / 'something').write_text('3w4g809uw58g039ghur')
    with AssertFilesystemUnmodified(tmpdir):
        exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 2


def test_backup_target_is_file(tmpdir) -> None:
    source_path = tmpdir / 'source'
    source_path.mkdir()
    (source_path / 'foo').write_text('some text here')
    target_path = tmpdir / 'target'
    target_path.write_text('hello world!')
    with AssertFilesystemUnmodified(tmpdir):
        exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 2


def test_backup_new_target(tmpdir) -> None:
    # Target directory doesn't exist.

    source_path = tmpdir / '\u1246\uA76D3fje_s\xDDrC\u01FC'
    source_path.mkdir()
    (source_path / 'foo.txt').write_text('it is Sunday')
    (source_path / 'bar').mkdir()
    (source_path / 'bar' / 'qux').write_text('something just something')

    target_path = (tmpdir / 'mypath\uFDEA\uBDF3' / 'doesnt\xDFFEXIsT')

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    end_time = datetime.now(timezone.utc)

    assert exit_code == 0

    assert target_path.is_dir()
    backups = list(target_path.iterdir())
    assert len(backups) == 1
    backup = backups[0]
    assert backup.name.isalnum() and len(backup.name) >= 10
    assert dir_entries(backup) == {'data', 'start.json', 'manifest.json', 'completion.json'}
    assert dir_entries(backup / 'data') == {'foo.txt', 'bar'}
    assert (backup / 'data' / 'foo.txt').read_text() == 'it is Sunday'
    assert dir_entries(backup / 'data' / 'bar') == {'qux'}
    assert (backup / 'data' / 'bar' / 'qux').read_text() == 'something just something'

    TIME_TOLERANCE = 5      # Seconds

    actual_start_info = (backup / 'start.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "start_time": "(.+)"\n}', actual_start_info)
    assert match
    actual_start_time = datetime.fromisoformat(match.group(1))
    assert abs((start_time - actual_start_time).total_seconds()) < TIME_TOLERANCE

    actual_complete_info = (backup / 'completion.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "end_time": "(.+)",\n    "paths_skipped": false\n}', actual_complete_info)
    assert match
    actual_end_time = datetime.fromisoformat(match.group(1))
    assert abs((end_time - actual_end_time).total_seconds()) < TIME_TOLERANCE

    expected_manifest = \
"""[
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
]"""
    actual_manifest = (backup / 'manifest.json').read_text(encoding='utf8')
    assert actual_manifest == expected_manifest


def test_backup_no_previous_backups(tmpdir) -> None:
    # Target directory exists but is empty.

    source_path = tmpdir / 'rubbish\xC2with' / '\u5647\uBDC1\u9C87 chars'
    source_path.mkdir(parents=True)
    (source_path / 'it\uAF87.\u78FAis').write_text('Wednesday my dudes')
    (source_path / '\x55\u6677\u8899\u0255').mkdir()
    (source_path / '\x55\u6677\u8899\u0255' / 'funky file name').write_text('<^ funky <> file <> data ^>')

    target_path = (tmpdir / 'I  lik\uCECE  tr\uAAAAins')
    target_path.mkdir()

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    end_time = datetime.now(timezone.utc)

    assert exit_code == 0

    assert target_path.is_dir()
    backups = list(target_path.iterdir())
    assert len(backups) == 1
    backup = backups[0]
    assert backup.name.isalnum() and len(backup.name) >= 10
    assert dir_entries(backup) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup / 'data') == {'it\uAF87.\u78FAis', '\x55\u6677\u8899\u0255'}
    assert (backup / 'data' / 'it\uAF87.\u78FAis').read_text() == 'Wednesday my dudes'
    assert dir_entries(backup / 'data' / '\x55\u6677\u8899\u0255') == {'funky file name'}
    assert (backup / 'data' / '\x55\u6677\u8899\u0255' / 'funky file name').read_text() == '<^ funky <> file <> data ^>'

    TIME_TOLERANCE = 5  # Seconds

    start_info = (backup / 'start.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "start_time": "(.+)"\n}', start_info)
    assert match
    actual_start_time = datetime.fromisoformat(match.group(1))
    assert abs((start_time - actual_start_time).total_seconds()) < TIME_TOLERANCE

    complete_info = (backup / 'completion.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "end_time": "(.+)",\n    "paths_skipped": false\n}', complete_info)
    assert match
    actual_end_time = datetime.fromisoformat(match.group(1))
    assert abs((end_time - actual_end_time).total_seconds()) < TIME_TOLERANCE

    expected_manifest = \
"""[
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
]"""
    actual_manifest = (backup / 'manifest.json').read_text(encoding='utf8')
    assert actual_manifest == expected_manifest


def test_backup_some_previous_backups(tmpdir) -> None:
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
    (backup1_path / 'data' / 'root\uA63Bfile1.mp4').write_text('rootfile1.mp4 backup1')
    (backup1_path / 'data' / 'ro\u2983ot_fi\x90le2.exe').write_text('root_file2.exe backup1')
    (backup1_path / 'data' / 'dir1\u1076\u0223').mkdir()
    (backup1_path / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file1').write_text('dir1_file1 backup1')
    (backup1_path / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file@@.tij').write_text('dir1_file@@.tij backup1')
    (backup1_path / 'data' / 'dir1\u1076\u0223' / 'dirXYZ').mkdir()
    (backup1_path / 'data' / 'dir1\u1076\u0223' / 'dirXYZ' / 'dirXYZ_file.ino').write_text('dirXYZ_file.ino backup1')
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
    (backup2_path / 'data' / 'root_file3.txt').write_text('root_file3.txt backup2')
    (backup2_path / 'data' / 'dir1\u1076\u0223').mkdir()
    (backup2_path / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file1').write_text('dir1_file1 backup2')
    (backup2_path / 'data' / 'temp').mkdir()
    (backup2_path / 'data' / 'temp' / 'x.y').write_text('x.y backup2')
    (backup2_path / 'completion.json').write_text(
        '{"start_time": "2021-07-01T13:55:46.983451+00:00", "paths_skipped": false}', encoding='utf8')

    backup3_path = target_path / 'O9I763i7gto87TGi73'
    backup3_path.mkdir()
    (backup3_path / 'start.json').write_text('{"start_time": "2021-09-18T09:47:11.879254+00:00"}', encoding='utf8')
    (backup3_path / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["root\uA63Bfile1.mp4", "ro\u2983ot_fi\x90le2.exe"]},
            {"n": "dir2", "cf": ["\uF000\uBAA4\u3404\xEA\uAEF1"]},
            "^1",
            {"n": "dir1\u1076\u0223", "rd": ["dirXYZ"]}]''',
        encoding='utf8')
    (backup3_path / 'data').mkdir()
    (backup3_path / 'data' / 'root\uA63Bfile1.mp4').write_text('rootfile1.mp4 backup3')
    (backup3_path / 'data' / 'ro\u2983ot_fi\x90le2.exe').write_text('root_file2.exe backup3')
    (backup3_path / 'data' / 'dir2').mkdir()
    (backup3_path / 'data' / 'dir2' / '\uF000\uBAA4\u3404\xEA\uAEF1').write_text('foobar backup3')
    (backup3_path / 'completion.json').write_text(
        '{"start_time": "2021-09-18T09:48:07.879254+00:00", "paths_skipped": false}', encoding='utf8')

    source_path = tmpdir / 'this\u865Cneeds\u4580to\u9B93bebackedup'
    source_path.mkdir()
    (source_path / 'root\uA63Bfile1.mp4').write_text('rootfile1.mp4 backup3')   # Existing unmodified
    modified_time = datetime(2021, 9, 5, 0, 43, 16, tzinfo=timezone.utc).timestamp()
    os.utime(source_path / 'root\uA63Bfile1.mp4', (modified_time, modified_time))
    # ro\u2983ot_fi\x90le2.exe removed
    (source_path / 'root_file3.txt').write_text('root_file3.txt new content')   # Existing modified
    (source_path / 'dir1\u1076\u0223').mkdir()  # Existing
    (source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file1').write_text('dir1_file1 backup2')  # Existing unmodified
    modified_time = datetime(2021, 7, 1, 9, 32, 59, tzinfo=timezone.utc).timestamp()
    os.utime(source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file1', (modified_time, modified_time))
    (source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file@@.tij').write_text('something NEW')  # Existing modified
    (source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file3').write_text('dir1_file3 new')      # New
    # dir2 / \uF000\uBAA4\u3404\xEA\uAEF1 removed
    (source_path / 'dir2' / 'dir2_\u45631').mkdir(parents=True)     # New
    (source_path / 'dir2' / 'dir2_\u45631' / 'myfile.myfile').write_text('myfile and also mycontents')  # New
    (source_path / 'temp').mkdir()      # Existing, excluded (removed)
    # temp / x.y removed
    (source_path / 'temp' / '\u7669.\u5AAB').write_text('magic')    # New, excluded
    (source_path / 'new_dir!').mkdir()      # New
    (source_path / 'new_dir!' / 'new file').write_text('its a new file!')       # New

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        exit_code = script_main(
            (PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path), '--exclude-pattern=/temp/'))
    end_time = datetime.now(timezone.utc)

    assert exit_code == 0

    backup = next(d for d in target_path.iterdir() if d not in {backup1_path, backup2_path, backup3_path})
    assert backup.name.isalnum() and len(backup.name) >= 10
    assert dir_entries(backup) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup / 'data') == {'root_file3.txt', 'dir1\u1076\u0223', 'dir2', 'new_dir!'}
    assert (backup / 'data' / 'root_file3.txt').read_text() == 'root_file3.txt new content'
    assert dir_entries(backup / 'data' / 'dir1\u1076\u0223') == \
           {'dir1\u1076\u0223_file@@.tij', 'dir1\u1076\u0223_file3'}
    assert (backup / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file@@.tij').read_text() == 'something NEW'
    assert (backup / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file3').read_text() == 'dir1_file3 new'
    assert dir_entries(backup / 'data' / 'dir2') == {'dir2_\u45631'}
    assert dir_entries(backup / 'data' / 'dir2' / 'dir2_\u45631') == {'myfile.myfile'}
    assert (backup / 'data' / 'dir2' / 'dir2_\u45631' / 'myfile.myfile').read_text() == 'myfile and also mycontents'
    assert dir_entries(backup / 'data' / 'new_dir!') == {'new file'}
    assert (backup / 'data' / 'new_dir!' / 'new file').read_text() == 'its a new file!'

    TIME_TOLERANCE = 5  # Seconds

    start_info = (backup / 'start.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "start_time": "(.+)"\n}', start_info)
    assert match
    actual_start_time = datetime.fromisoformat(match.group(1))
    assert abs((start_time - actual_start_time).total_seconds()) < TIME_TOLERANCE

    complete_info = (backup / 'completion.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "end_time": "(.+)",\n    "paths_skipped": false\n}', complete_info)
    assert match
    actual_end_time = datetime.fromisoformat(match.group(1))
    assert abs((end_time - actual_end_time).total_seconds()) < TIME_TOLERANCE

    # Don't think it's feasible to check the manifest without parsing it, because filesystem ordering is not guaranteed.
    manifest = read_backup_manifest(backup / 'manifest.json')
    assert manifest.root.copied_files == ['root_file3.txt']
    assert manifest.root.removed_files == ['ro\u2983ot_fi\x90le2.exe']
    assert manifest.root.removed_directories == ['temp']
    assert len(manifest.root.subdirectories) == 3
    dir1 = next(d for d in manifest.root.subdirectories if d.name == 'dir1\u1076\u0223')
    assert unordered_equal(dir1.copied_files, ('dir1\u1076\u0223_file@@.tij', 'dir1\u1076\u0223_file3'))
    assert dir1.removed_files == []
    assert dir1.removed_directories == []
    assert dir1.subdirectories == []
    dir2 = next(d for d in manifest.root.subdirectories if d.name == 'dir2')
    assert dir2.copied_files == []
    assert dir2.removed_files == ['\uF000\uBAA4\u3404\xEA\uAEF1']
    assert dir2.removed_directories == []
    assert len(dir2.subdirectories) == 1
    dir2_1 = dir2.subdirectories[0]
    assert dir2_1 == BackupManifest.Directory('dir2_\u45631', copied_files=['myfile.myfile'])
    new_dir = next(d for d in manifest.root.subdirectories if d.name == 'new_dir!')
    assert new_dir == BackupManifest.Directory('new_dir!', copied_files=['new file'])


def test_backup_some_invalid_backups(tmpdir) -> None:
    # Target directory has some previous backups and invalid/not backups.

    # TODO
    assert False
