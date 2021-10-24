from datetime import datetime, timezone
from pathlib import Path
import re

from incremental_backup.main import script_main

from helpers import dir_entries


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
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 2


def test_backup_source_is_file(tmpdir) -> None:
    source_path = tmpdir / 'source'
    target_path = tmpdir / 'target'
    source_path.ensure()
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 2


def test_backup_target_is_file(tmpdir) -> None:
    source_path = tmpdir / 'source'
    target_path = tmpdir / 'target'
    source_path.ensure_dir()
    target_path.ensure()
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 2


def test_backup_new_target(tmpdir) -> None:
    # Target directory doesn't exist.

    tmpdir = Path(tmpdir)

    source_path = tmpdir / '\u1246\uA76D3fje_s\xDDrC\u01FC'
    source_path.mkdir()
    (source_path / 'foo.txt').write_text('it is Sunday')
    (source_path / 'bar').mkdir()
    (source_path / 'bar' / 'qux').write_text('something just something')

    target_path = (tmpdir / 'mypath\uFDEA\uBDF3' / 'doesnt\xDFFEXIsT')

    start_time = datetime.now(timezone.utc)
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

    tmpdir = Path(tmpdir)

    source_path = tmpdir / 'rubbish\xC2with' / '\u5647\uBDC1\u9C87 chars'
    source_path.mkdir(parents=True)
    (source_path / 'it\uAF87.\u78FAis').write_text('Wednesday my dudes')
    (source_path / '\x55\u6677\u8899\u0255').mkdir()
    (source_path / '\x55\u6677\u8899\u0255' / 'funky file name').write_text('<^ funky <> file <> data ^>')

    target_path = (tmpdir / 'I  lik\uCECE  tr\uAAAAins')
    target_path.mkdir()

    start_time = datetime.now(timezone.utc)
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

    tmpdir = Path(tmpdir)

    source_path = tmpdir / 'this\u865Cneeds\u4580to\u9B93bebackedup'
    source_path.mkdir()
    # TODO

    target_path = tmpdir / 'put the data here!'
    target_path.mkdir()

    backup1_path = target_path / 'sadhf8o3947yfqgfaw'
    backup1_path.mkdir()
    (backup1_path / 'start.json').write_text('{"start_time": "2021-06-20T03:37:27.435676+00:00"}', encoding='utf8')
    # TODO

    backup2_path = target_path / 'gsel45o8ise45ytq87'
    backup2_path.mkdir()
    (backup2_path / 'start.json').write_text('{"start_time": "2021-07-01T13:52:21.983451+00:00"}', encoding='utf8')
    # TODO

    backup3_path = target_path / 'O9I763i7gto87TGi73'
    backup3_path.mkdir()
    (backup3_path / 'start.json').write_text('{"start_time": "2021-09-18T09:47:11.879254+00:00"}', encoding='utf8')
    # TODO

    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path), '--exclude-pattern=/temp/'))

    assert exit_code == 0

    # TODO
    assert False


def test_backup_some_invalid_backups(tmpdir) -> None:
    # Target directory has some previous backups and invalid/not backups.

    # TODO
    assert False
