from datetime import datetime, timezone
import re

from incremental_backup.meta.manifest import BackupManifest, read_backup_manifest

from helpers import AssertFilesystemUnmodified, dir_entries, run_application, unordered_equal, write_file_with_mtime


# TODO? check program console output


def test_no_args() -> None:
    process = run_application(())
    assert process.returncode == 1


def test_invalid_command() -> None:
    process = run_application(('magic_command', 'foo', 'bar'))
    assert process.returncode == 1


def test_backup_no_args() -> None:
    process = run_application(('backup',))
    assert process.returncode == 1


def test_backup_normal(tmpdir) -> None:
    # Typical application usage: few existing backups, some exclude patterns.

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
            {"n": ".git", "cf": ["x.y"]}]''',
        encoding='utf8')
    (backup2_path / 'data').mkdir()
    (backup2_path / 'data' / 'root_file3.txt').write_text('root_file3.txt backup2')
    (backup2_path / 'data' / 'dir1\u1076\u0223').mkdir()
    (backup2_path / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file1').write_text('dir1_file1 backup2')
    (backup2_path / 'data' / '.git').mkdir()
    (backup2_path / 'data' / '.git' / 'x.y').write_text('x.y backup2')
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
    (backup3_path / 'data' / 'root\uA63Bfile1.mp4').write_text('rootfile1.mp4 backup3')
    (backup3_path / 'data' / 'ro\u2983ot_fi\x90le2.exe').write_text('root_file2.exe backup3')
    (backup3_path / 'data' / 'dir2').mkdir()
    (backup3_path / 'data' / 'dir2' / '\uF000\uBAA4\u3404\xEA\uAEF1').write_text('foobar backup3')
    (backup3_path / 'completion.json').write_text(
        '{"start_time": "2021-09-18T09:48:07.879254+00:00", "paths_skipped": false}', encoding='utf8')

    source_path = tmpdir / 'this\u865Cneeds\u4580to\u9B93bebackedup'
    source_path.mkdir()
    write_file_with_mtime(source_path / 'root\uA63Bfile1.mp4', 'rootfile1.mp4 backup3',
                          datetime(2021, 9, 5, 0, 43, 16, tzinfo=timezone.utc))     # Existing unmodified
    # ro\u2983ot_fi\x90le2.exe removed
    (source_path / 'root_file3.txt').write_text('root_file3.txt new content')   # Existing modified
    (source_path / 'dir1\u1076\u0223').mkdir()  # Existing
    write_file_with_mtime(source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file1', 'dir1_file1 backup2',
                          datetime(2021, 7, 1, 9, 32, 59, tzinfo=timezone.utc))     # Existing unmodified
    (source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file@@.tij').write_text('something NEW')  # Existing modified
    (source_path / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file3').write_text('dir1_file3 new')      # New
    # dir2 / \uF000\uBAA4\u3404\xEA\uAEF1 removed
    (source_path / 'dir2' / 'dir2_\u45631').mkdir(parents=True)     # New
    (source_path / 'dir2' / 'dir2_\u45631' / 'myfile.myfile').write_text('myfile and also mycontents')  # New
    (source_path / '.git').mkdir()      # Existing, excluded (removed)
    # .git / x.y removed
    (source_path / '.git' / '\u7669.\u5AAB').write_text('magic')    # New, excluded
    (source_path / 'new_dir!').mkdir()      # New
    (source_path / 'new_dir!' / 'new file').write_text('its a new file!')       # New

    start_time = datetime.now(timezone.utc)
    with AssertFilesystemUnmodified(source_path):
        process = run_application(('backup', str(source_path), str(target_path), '--exclude', r'.*/\.git/'))
    end_time = datetime.now(timezone.utc)

    assert process.returncode == 0

    backup_path = (set(target_path.iterdir()) - {backup1_path, backup2_path, backup3_path}).pop()

    assert backup_path.name.isascii() and backup_path.name.isalnum() and len(backup_path.name) >= 10
    assert dir_entries(backup_path) == {'data', 'start.json', 'manifest.json', 'completion.json'}

    assert dir_entries(backup_path / 'data') == {'root_file3.txt', 'dir1\u1076\u0223', 'dir2', 'new_dir!'}
    assert (backup_path / 'data' / 'root_file3.txt').read_text() == 'root_file3.txt new content'
    assert dir_entries(backup_path / 'data' / 'dir1\u1076\u0223') == \
           {'dir1\u1076\u0223_file@@.tij', 'dir1\u1076\u0223_file3'}
    assert (backup_path / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file@@.tij').read_text() == 'something NEW'
    assert (backup_path / 'data' / 'dir1\u1076\u0223' / 'dir1\u1076\u0223_file3').read_text() == 'dir1_file3 new'
    assert dir_entries(backup_path / 'data' / 'dir2') == {'dir2_\u45631'}
    assert dir_entries(backup_path / 'data' / 'dir2' / 'dir2_\u45631') == {'myfile.myfile'}
    assert (backup_path / 'data' / 'dir2' / 'dir2_\u45631' / 'myfile.myfile').read_text() == 'myfile and also mycontents'
    assert dir_entries(backup_path / 'data' / 'new_dir!') == {'new file'}
    assert (backup_path / 'data' / 'new_dir!' / 'new file').read_text() == 'its a new file!'

    actual_start_info = (backup_path / 'start.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "start_time": "(.+)"\n}', actual_start_info)
    assert match
    actual_start_time = datetime.fromisoformat(match.group(1))
    assert abs((actual_start_time - start_time).total_seconds()) < METADATA_TIME_TOLERANCE

    actual_complete_info = (backup_path / 'completion.json').read_text(encoding='utf8')
    match = re.fullmatch('{\n    "end_time": "(.+)",\n    "paths_skipped": false\n}', actual_complete_info)
    assert match
    actual_end_time = datetime.fromisoformat(match.group(1))
    assert abs((actual_end_time - end_time).total_seconds()) < METADATA_TIME_TOLERANCE

    # Don't think it's feasible to check the manifest without parsing it, because filesystem ordering is not guaranteed.
    actual_manifest = read_backup_manifest(backup_path / 'manifest.json')
    assert actual_manifest.root.copied_files == ['root_file3.txt']
    assert actual_manifest.root.removed_files == ['ro\u2983ot_fi\x90le2.exe']
    assert actual_manifest.root.removed_directories == ['.git']
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


METADATA_TIME_TOLERANCE = 5     # Seconds
