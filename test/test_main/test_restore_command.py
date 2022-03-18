from pathlib import Path

from helpers import AssertFilesystemUnmodified, dir_entries, run_application


# TODO? check program console output


def test_restore_no_args() -> None:
    process = run_application('restore')
    assert process.returncode == 1


def test_restore_too_few_args(tmpdir: Path) -> None:
    with AssertFilesystemUnmodified(tmpdir):
        process = run_application('restore', str(tmpdir))
    assert process.returncode == 1


def test_restore_all(tmpdir: Path) -> None:
    # Neither backup name nor time specified, restore from all backups.

    target_dir = tmpdir / 'backups'
    target_dir.mkdir()

    backup1_dir = target_dir / 'ws3e48ohitv'
    backup1_dir.mkdir()
    (backup1_dir / 'start.json').write_text('{"start_time": "2022-03-12T11:53:22.954665+00:00"}', encoding='utf8')
    backup1_data_dir = backup1_dir / 'data'
    backup1_data_dir.mkdir()
    (backup1_data_dir / 'foo.jpg').write_text('hello world')
    (backup1_data_dir / 'manama').write_text('goodbye world')
    (backup1_data_dir / 'myDir').mkdir()
    (backup1_data_dir / 'myDir' / 'bar-qux').write_text('first content')
    (backup1_dir / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["foo.jpg", "manama"]},
            {"n": "myDir", "cf": ["bar-qux"]}]''',
        encoding='utf8')

    backup2_dir = target_dir / '9w384rapw9ssa'
    backup2_dir.mkdir()
    (backup2_dir / 'start.json').write_text('{"start_time": "2022-04-12T11:53:22.954665+00:00"}', encoding='utf8')
    backup2_data_dir = backup2_dir / 'data'
    backup2_data_dir.mkdir()
    (backup2_data_dir / 'yes.no').write_text('hello world 2')
    (backup2_dir / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["yes.no"]},
            {"n": "myDir", "rf": ["bar-qux"]}]''',
        encoding='utf8')

    backup3_dir = target_dir / '98P678676h9645'
    backup3_dir.mkdir()
    (backup3_dir / 'start.json').write_text('{"start_time": "2022-04-25T14:50:59.430968+00:00"}', encoding='utf8')
    backup3_data_dir = backup3_dir / 'data'
    backup3_data_dir.mkdir()
    (backup3_data_dir / 'myDir').mkdir()
    (backup3_data_dir / 'myDir' / 'bar-qux').write_text('final content')
    (backup3_dir / 'manifest.json').write_text(
        '''[{"n": ""},
            {"n": "myDir", "cf": ["bar-qux"]}]''',
        encoding='utf8')

    destination_dir = tmpdir / 'destination'

    with AssertFilesystemUnmodified(target_dir):
        process = run_application('restore', str(target_dir), str(destination_dir))

    assert process.returncode == 0

    assert dir_entries(destination_dir) == {'foo.jpg', 'manama', 'yes.no', 'myDir'}
    assert (destination_dir / 'foo.jpg').read_text() == 'hello world'
    assert (destination_dir / 'manama').read_text() == 'goodbye world'
    assert (destination_dir / 'yes.no').read_text() == 'hello world 2'
    assert dir_entries(destination_dir / 'myDir') == {'bar-qux'}
    assert (destination_dir / 'myDir' / 'bar-qux').read_text() == 'final content'


def test_restore_name(tmpdir: Path) -> None:
    # Backup name specified, restore up to that backup.

    target_dir = tmpdir / 'backups'
    target_dir.mkdir()

    backup1_dir = target_dir / 'ws3e48ohitv'
    backup1_dir.mkdir()
    (backup1_dir / 'start.json').write_text('{"start_time": "2022-03-12T11:53:22.954665+00:00"}', encoding='utf8')
    backup1_data_dir = backup1_dir / 'data'
    backup1_data_dir.mkdir()
    (backup1_data_dir / 'foo.jpg').write_text('hello world')
    (backup1_data_dir / 'manama').write_text('goodbye world')
    (backup1_data_dir / 'myDir').mkdir()
    (backup1_data_dir / 'myDir' / 'bar-qux').write_text('first content')
    (backup1_dir / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["foo.jpg", "manama"]},
            {"n": "myDir", "cf": ["bar-qux"]}]''',
        encoding='utf8')

    backup2_dir = target_dir / '9w384rapw9ssa'
    backup2_dir.mkdir()
    (backup2_dir / 'start.json').write_text('{"start_time": "2022-04-12T11:53:22.954665+00:00"}', encoding='utf8')
    backup2_data_dir = backup2_dir / 'data'
    backup2_data_dir.mkdir()
    (backup2_data_dir / 'yes.no').write_text('hello world 2')
    (backup2_dir / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["yes.no"]},
            {"n": "myDir", "rf": ["bar-qux"]}]''',
        encoding='utf8')

    backup3_dir = target_dir / '98P678676h9645'
    backup3_dir.mkdir()
    (backup3_dir / 'start.json').write_text('{"start_time": "2022-04-25T14:50:59.430968+00:00"}', encoding='utf8')
    backup3_data_dir = backup3_dir / 'data'
    backup3_data_dir.mkdir()
    (backup3_data_dir / 'myDir').mkdir()
    (backup3_data_dir / 'myDir' / 'bar-qux').write_text('final content')
    (backup3_dir / 'manifest.json').write_text(
        '''[{"n": ""},
            {"n": "myDir", "cf": ["bar-qux"]}]''',
        encoding='utf8')

    destination_dir = tmpdir / 'destination'

    with AssertFilesystemUnmodified(target_dir):
        process = run_application('restore', str(target_dir), str(destination_dir), '9w384rapw9ssa')

    assert process.returncode == 0

    assert dir_entries(destination_dir) == {'foo.jpg', 'manama', 'yes.no'}
    assert (destination_dir / 'foo.jpg').read_text() == 'hello world'
    assert (destination_dir / 'manama').read_text() == 'goodbye world'
    assert (destination_dir / 'yes.no').read_text() == 'hello world 2'


def test_restore_time(tmpdir: Path) -> None:
    # Backup time specified, restore up to that time.

    target_dir = tmpdir / 'backups'
    target_dir.mkdir()

    backup1_dir = target_dir / 'ws3e48ohitv'
    backup1_dir.mkdir()
    (backup1_dir / 'start.json').write_text('{"start_time": "2022-03-12T11:53:22.954665+00:00"}', encoding='utf8')
    backup1_data_dir = backup1_dir / 'data'
    backup1_data_dir.mkdir()
    (backup1_data_dir / 'foo.jpg').write_text('hello world')
    (backup1_data_dir / 'manama').write_text('goodbye world')
    (backup1_data_dir / 'myDir').mkdir()
    (backup1_data_dir / 'myDir' / 'bar-qux').write_text('first content')
    (backup1_dir / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["foo.jpg", "manama"]},
            {"n": "myDir", "cf": ["bar-qux"]}]''',
        encoding='utf8')

    backup2_dir = target_dir / '9w384rapw9ssa'
    backup2_dir.mkdir()
    (backup2_dir / 'start.json').write_text('{"start_time": "2022-04-12T11:53:22.954665+00:00"}', encoding='utf8')
    backup2_data_dir = backup2_dir / 'data'
    backup2_data_dir.mkdir()
    (backup2_data_dir / 'yes.no').write_text('hello world 2')
    (backup2_dir / 'manifest.json').write_text(
        '''[{"n": "", "cf": ["yes.no"]},
            {"n": "myDir", "rf": ["bar-qux"]}]''',
        encoding='utf8')

    backup3_dir = target_dir / '98P678676h9645'
    backup3_dir.mkdir()
    (backup3_dir / 'start.json').write_text('{"start_time": "2022-04-25T14:50:59.430968+00:00"}', encoding='utf8')
    backup3_data_dir = backup3_dir / 'data'
    backup3_data_dir.mkdir()
    (backup3_data_dir / 'myDir').mkdir()
    (backup3_data_dir / 'myDir' / 'bar-qux').write_text('final content')
    (backup3_dir / 'manifest.json').write_text(
        '''[{"n": ""},
            {"n": "myDir", "cf": ["bar-qux"]}]''',
        encoding='utf8')

    destination_dir = tmpdir / 'destination'

    with AssertFilesystemUnmodified(target_dir):
        process = run_application('restore', str(target_dir), str(destination_dir), '2022-04-13T05:17:28.135791')

    assert process.returncode == 0

    assert dir_entries(destination_dir) == {'foo.jpg', 'manama', 'yes.no'}
    assert (destination_dir / 'foo.jpg').read_text() == 'hello world'
    assert (destination_dir / 'manama').read_text() == 'goodbye world'
    assert (destination_dir / 'yes.no').read_text() == 'hello world 2'
