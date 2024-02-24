from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta.meta import BackupMetadata, ReadBackupsCallbacks
from incremental_backup.restore import perform_restore, restore_files, RestoreCallbacks, RestoreError, \
    RestoreFilesCallbacks, RestoreFilesResults, RestoreResults

from helpers import AssertFilesystemUnmodified, dir_entries, unordered_equal


def test_restore_files_empty(tmpdir: Path) -> None:
    target_dir = tmpdir / 'backups'
    target_dir.mkdir()

    destination_dir = tmpdir / 'destination'

    backup_sum = BackupSum()

    callbacks = RestoreFilesCallbacks(
        on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
        on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
    )

    with AssertFilesystemUnmodified(target_dir):
        actual_results = restore_files(target_dir, backup_sum, destination_dir, callbacks)

    expected_results = RestoreFilesResults(0, False)
    assert actual_results == expected_results

    assert destination_dir.exists()
    assert dir_entries(destination_dir) == set()


def test_restore_files(tmpdir: Path) -> None:
    # Test with some file I/O errors.

    target_dir = tmpdir / 'backups'
    target_dir.mkdir()

    # I can't be bothered filling in the rest of the metadata here, it shouldn't be used anyway.
    backup1 = BackupMetadata('apwerfuhv4835t', None, None)
    backup1_data_dir = target_dir / 'apwerfuhv4835t/data'
    backup1_data_dir.mkdir(parents=True)
    (backup1_data_dir / 'foo.txt').write_text('some thing here')
    (backup1_data_dir / 'dir1').mkdir()
    (backup1_data_dir / 'dir1/dir1_file1.1').write_text('dir1_file1 text')

    backup2 = BackupMetadata('sfoynbsebo8756s', None, None)
    backup2_data_dir = target_dir / 'sfoynbsebo8756s/data'
    backup2_data_dir.mkdir(parents=True)
    (backup2_data_dir / 'dir1').mkdir()
    (backup2_data_dir / 'dir1/dir1_file2.png').write_text('45-jsduyfgv4 tyq73 4bt')
    (backup2_data_dir / 'dir2').mkdir()
    (backup2_data_dir / 'dir2/dir2_file').write_text(' < > & ~ & < >')

    backup3 = BackupMetadata('dlrtioyuw3405g', None, None)
    backup3_data_dir = target_dir / 'dlrtioyuw3405g/data'
    backup3_data_dir.mkdir(parents=True)
    (backup3_data_dir / 'dir2/dir2\u5487\u45FE').mkdir(parents=True)
    (backup3_data_dir / 'dir2/dir2\u5487\u45FE/the\uECF1file\u23FDyes').write_text('final file data')

    destination_dir = tmpdir / 'destination'

    backup_sum = BackupSum(BackupSum.Directory('',
        files=[BackupSum.File('foo.txt', backup1), BackupSum.File('nonexistent.file', backup2)], subdirectories=[
            BackupSum.Directory('dir1',
                files=[BackupSum.File('dir1_file1.1', backup1), BackupSum.File('dir1_file2.png', backup2)],
                subdirectories=[BackupSum.Directory('mkdir_error', files=[BackupSum.File('file', backup2)])]),
            BackupSum.Directory('dir2', files=[BackupSum.File('dir2_file', backup2)], subdirectories=[
                BackupSum.Directory('nonexistentContents',
                    files=[BackupSum.File('foo', backup1), BackupSum.File('bar.jpg', backup3)]),
                BackupSum.Directory('dir2\u5487\u45FE', files=[BackupSum.File('the\uECF1file\u23FDyes', backup3)]),
            ])
    ]))

    # Force mkdir error.
    (destination_dir / 'dir1').mkdir(parents=True)
    (destination_dir / 'dir1/mkdir_error').touch()

    mkdir_errors: list[tuple[Path, Exception]] = []
    copy_errors: list[tuple[Path, Path, Exception]] = []
    callbacks = RestoreFilesCallbacks(
        on_mkdir_error=lambda path, error: mkdir_errors.append((path, error)),
        on_copy_error=lambda src, dest, error: copy_errors.append((src, dest, error))
    )

    with AssertFilesystemUnmodified(target_dir):
        actual_results = restore_files(target_dir, backup_sum, destination_dir, callbacks)

    (destination_dir / 'dir1/mkdir_error').unlink(missing_ok=False)

    assert [(p, type(e)) for p, e in mkdir_errors] == [(destination_dir / 'dir1/mkdir_error', FileExistsError)]
    assert [(s, d, type(e)) for s, d, e in copy_errors] == [
        (target_dir / 'sfoynbsebo8756s/data/nonexistent.file', destination_dir / 'nonexistent.file', FileNotFoundError),
        (target_dir / 'apwerfuhv4835t/data/dir2/nonexistentContents/foo', destination_dir / 'dir2/nonexistentContents/foo', FileNotFoundError),
        (target_dir / 'dlrtioyuw3405g/data/dir2/nonexistentContents/bar.jpg', destination_dir / 'dir2/nonexistentContents/bar.jpg', FileNotFoundError),
    ]

    expected_results = RestoreFilesResults(5, True)
    assert actual_results == expected_results

    assert dir_entries(destination_dir) == {'foo.txt', 'dir1', 'dir2'}
    assert (destination_dir / 'foo.txt').read_text() == 'some thing here'
    assert dir_entries(destination_dir / 'dir1') == {'dir1_file1.1', 'dir1_file2.png'}
    assert (destination_dir / 'dir1/dir1_file1.1').read_text() == 'dir1_file1 text'
    assert (destination_dir / 'dir1/dir1_file2.png').read_text() == '45-jsduyfgv4 tyq73 4bt'
    assert dir_entries(destination_dir / 'dir2') == {'dir2_file', 'dir2\u5487\u45FE', 'nonexistentContents'}
    assert (destination_dir / 'dir2/dir2_file').read_text() == ' < > & ~ & < >'
    assert dir_entries(destination_dir / 'dir2/dir2\u5487\u45FE') == {'the\uECF1file\u23FDyes'}
    assert (destination_dir / 'dir2/dir2\u5487\u45FE/the\uECF1file\u23FDyes').read_text() == 'final file data'
    assert dir_entries(destination_dir / 'dir2/nonexistentContents') == set()


def test_perform_restore_invalid_args(tmpdir: Path) -> None:
    target_dir = tmpdir / 'backups'
    destination_dir = tmpdir / 'destination'
    backup_name = '49538tsnuefdgy'
    backup_time = datetime(2021, 11, 15, 19, 15, 23, tzinfo=timezone.utc)

    callbacks = RestoreCallbacks(
        on_before_read_previous_backups=lambda: pytest.fail('Unexpected on_before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            pytest.fail(f'Unexpected on_after_read_previous_backups: {backups=}'),
        on_selected_backups=lambda backups: pytest.fail(f'Unexpected on_selected_backups: {backups=}'),
        on_before_initialise_restore=lambda: pytest.fail(f'Unexpected on_before_initialise_restore'),
        on_before_restore_files=lambda: pytest.fail(f'Unexpected on_before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(ValueError):
            perform_restore(target_dir, destination_dir, backup_name, backup_time, callbacks)


def test_perform_restore_nonexistent_target(tmpdir: Path) -> None:
    # Backup target directory doesn't exist.

    target_dir = tmpdir / 'backups'

    destination_dir = tmpdir / 'destination'

    callbacks = RestoreCallbacks(
        on_before_read_previous_backups=lambda: pytest.fail('Unexpected on_before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            pytest.fail(f'Unexpected on_after_read_previous_backups: {backups=}'),
        on_selected_backups=lambda backups: pytest.fail(f'Unexpected on_selected_backups: {backups=}'),
        on_before_initialise_restore=lambda: pytest.fail(f'Unexpected on_before_initialise_restore'),
        on_before_restore_files=lambda: pytest.fail(f'Unexpected on_before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(RestoreError):
            perform_restore(target_dir, destination_dir, callbacks=callbacks)


def test_perform_restore_nonempty_destination(tmpdir: Path) -> None:
    # Destination directory exists and contains files. Should not attempt to restore files.

    target_dir = tmpdir / 'backups'
    backup1_dir = target_dir / '98we45t70vwh3478tr'
    backup1_dir.mkdir(parents=True)
    (backup1_dir / 'start.json').write_text('{"start_time": "2002-11-22T05:50:12.341256+00:00"}', encoding='utf8')
    (backup1_dir / 'manifest.json').write_text('[{"n": "", "cf": ["foo"]}]', encoding='utf8')
    (backup1_dir / 'completion.json').write_text(
        '{"start_time": "2002-11-22T05:50:20.495673+00:00", "paths_skipped": false}', encoding='utf8')
    (backup1_dir / 'data').mkdir()
    (backup1_dir / 'data/foo').write_text('Hello world!')

    destination_dir = tmpdir / 'destination'
    destination_dir.mkdir()
    (destination_dir / 'foo').write_text('Do not overwrite me!')
    (destination_dir / 'bar.txt').write_text('Nor me!')
    (destination_dir / 'qux').mkdir()

    callbacks = RestoreCallbacks(
        on_before_read_previous_backups=lambda: pytest.fail('Unexpected on_before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            pytest.fail(f'Unexpected on_after_read_previous_backups: {backups=}'),
        on_selected_backups=lambda backups: pytest.fail(f'Unexpected on_selected_backups: {backups=}'),
        on_before_initialise_restore=lambda: pytest.fail(f'Unexpected on_before_initialise_restore'),
        on_before_restore_files=lambda: pytest.fail(f'Unexpected on_before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(target_dir, destination_dir):
        with pytest.raises(RestoreError):
            perform_restore(target_dir, destination_dir, callbacks=callbacks)


def test_perform_restore_nonexistent_backup(tmpdir: Path) -> None:
    # backup_name is specified but that backup doesn't exist - shouldn't restore anything.

    target_dir = tmpdir / 'backups'
    target_dir.mkdir()

    backup1_dir = target_dir / 'E49YHUDFIG'
    backup1_dir.mkdir()
    (backup1_dir / 'start.json').write_text('{"start_time": "2002-11-22T05:50:12.341256+00:00"}', encoding='utf8')
    (backup1_dir / 'manifest.json').write_text('[{"n": "", "cf": ["foo"]}]', encoding='utf8')
    (backup1_dir / 'completion.json').write_text(
        '{"start_time": "2002-11-22T05:50:20.495673+00:00", "paths_skipped": false}', encoding='utf8')
    (backup1_dir / 'data').mkdir()
    (backup1_dir / 'data/foo').write_text('Hello world!')

    destination_dir = tmpdir / 'destination'

    callbacks = RestoreCallbacks(
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_selected_backups=lambda backups: pytest.fail(f'Unexpected on_selected_backups: {backups=}'),
        on_before_initialise_restore=lambda: pytest.fail(f'Unexpected on_before_initialise_restore'),
        on_before_restore_files=lambda: pytest.fail(f'Unexpected on_before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(target_dir, destination_dir):
        with pytest.raises(RestoreError):
            perform_restore(target_dir, destination_dir, backup_name='3498tisg4o8aoe', callbacks=callbacks)


def test_perform_restore_all(tmpdir: Path) -> None:
    # Neither backup_name nor backup_time is specified, restore all backups.

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

    actual_callbacks: list[Any] = []
    callbacks = RestoreCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_selected_backups=lambda backups: actual_callbacks.append(('selected_backups', backups)),
        on_before_initialise_restore=lambda: actual_callbacks.append('before_initialise_restore'),
        on_before_restore_files=lambda: actual_callbacks.append('before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(target_dir):
        actual_results = perform_restore(target_dir, destination_dir, callbacks=callbacks)

    assert len(actual_callbacks) == 5
    assert actual_callbacks[0] == 'before_read_previous_backups'
    assert actual_callbacks[1][0] == 'after_read_previous_backups'
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal([b.name for b in actual_callbacks[1][1]], ['ws3e48ohitv', '9w384rapw9ssa', '98P678676h9645'])
    assert actual_callbacks[2][0] == 'selected_backups'
    assert unordered_equal([b.name for b in actual_callbacks[2][1]], ['ws3e48ohitv', '9w384rapw9ssa', '98P678676h9645'])
    assert actual_callbacks[3] == 'before_initialise_restore'
    assert actual_callbacks[4] == 'before_restore_files'

    assert actual_results == RestoreResults(4, False)

    assert dir_entries(destination_dir) == {'foo.jpg', 'manama', 'yes.no', 'myDir'}
    assert (destination_dir / 'foo.jpg').read_text() == 'hello world'
    assert (destination_dir / 'manama').read_text() == 'goodbye world'
    assert (destination_dir / 'yes.no').read_text() == 'hello world 2'
    assert dir_entries(destination_dir / 'myDir') == {'bar-qux'}
    assert (destination_dir / 'myDir' / 'bar-qux').read_text() == 'final content'


def test_perform_restore_name(tmpdir: Path) -> None:
    # backup_name is specified, restore up to that backup.

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

    actual_callbacks: list[Any] = []
    callbacks = RestoreCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_selected_backups=lambda backups: actual_callbacks.append(('selected_backups', backups)),
        on_before_initialise_restore=lambda: actual_callbacks.append('before_initialise_restore'),
        on_before_restore_files=lambda: actual_callbacks.append('before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(target_dir):
        actual_results = perform_restore(target_dir, destination_dir, backup_name='9w384rapw9ssa', callbacks=callbacks)

    assert len(actual_callbacks) == 5
    assert actual_callbacks[0] == 'before_read_previous_backups'
    assert actual_callbacks[1][0] == 'after_read_previous_backups'
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal([b.name for b in actual_callbacks[1][1]], ['ws3e48ohitv', '9w384rapw9ssa', '98P678676h9645'])
    assert actual_callbacks[2][0] == 'selected_backups'
    assert unordered_equal([b.name for b in actual_callbacks[2][1]], ['ws3e48ohitv', '9w384rapw9ssa'])
    assert actual_callbacks[3] == 'before_initialise_restore'
    assert actual_callbacks[4] == 'before_restore_files'

    assert actual_results == RestoreResults(3, False)

    assert dir_entries(destination_dir) == {'foo.jpg', 'manama', 'yes.no'}
    assert (destination_dir / 'foo.jpg').read_text() == 'hello world'
    assert (destination_dir / 'manama').read_text() == 'goodbye world'
    assert (destination_dir / 'yes.no').read_text() == 'hello world 2'


def test_perform_restore_time(tmpdir: Path) -> None:
    # backup_time is specified, restore up to that time.

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

    actual_callbacks: list[Any] = []
    callbacks = RestoreCallbacks(
        on_before_read_previous_backups=lambda: actual_callbacks.append('before_read_previous_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_previous_backups=lambda backups:
            actual_callbacks.append(('after_read_previous_backups', backups)),
        on_selected_backups=lambda backups: actual_callbacks.append(('selected_backups', backups)),
        on_before_initialise_restore=lambda: actual_callbacks.append('before_initialise_restore'),
        on_before_restore_files=lambda: actual_callbacks.append('before_restore_files'),
        restore_files=RestoreFilesCallbacks(
            on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected on_mkdir_error: {path=} {error=}'),
            on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected on_copy_error: {src=} {dest=} {error=}')
        )
    )

    with AssertFilesystemUnmodified(target_dir):
        actual_results = perform_restore(
            target_dir, destination_dir, backup_time=datetime(2022, 4, 12, 11, 53, 22, 954665, tzinfo=timezone.utc),
            callbacks=callbacks)

    assert len(actual_callbacks) == 5
    assert actual_callbacks[0] == 'before_read_previous_backups'
    assert actual_callbacks[1][0] == 'after_read_previous_backups'
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal([b.name for b in actual_callbacks[1][1]], ['ws3e48ohitv', '9w384rapw9ssa', '98P678676h9645'])
    assert actual_callbacks[2][0] == 'selected_backups'
    assert unordered_equal([b.name for b in actual_callbacks[2][1]], ['ws3e48ohitv', '9w384rapw9ssa'])
    assert actual_callbacks[3] == 'before_initialise_restore'
    assert actual_callbacks[4] == 'before_restore_files'

    assert actual_results == RestoreResults(3, False)

    assert dir_entries(destination_dir) == {'foo.jpg', 'manama', 'yes.no'}
    assert (destination_dir / 'foo.jpg').read_text() == 'hello world'
    assert (destination_dir / 'manama').read_text() == 'goodbye world'
    assert (destination_dir / 'yes.no').read_text() == 'hello world 2'
