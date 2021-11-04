from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pytest

from incremental_backup.backup import filesystem
from incremental_backup.backup.plan import BackupPlan, execute_backup_plan, ExecuteBackupPlanCallbacks, \
    ExecuteBackupPlanResults
from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta.manifest import BackupManifest
from incremental_backup.meta.meta import BackupMetadata
from incremental_backup.meta.start_info import BackupStartInfo

from helpers import AssertFilesystemUnmodified, dir_entries


def test_backup_plan_directory_init() -> None:
    directory = BackupPlan.Directory('\x12\u3409someName*&^%#$#%34')
    assert directory.name == '\x12\u3409someName*&^%#$#%34'
    assert directory.copied_files == []
    assert directory.removed_files == []
    assert directory.removed_directories == []
    assert directory.subdirectories == []
    assert not directory.contains_copied_files
    assert not directory.contains_removed_items


def test_backup_plan_init() -> None:
    plan = BackupPlan()
    expected_root = BackupPlan.Directory('')
    assert plan.root == expected_root


def test_backup_plan_new() -> None:
    # Typical input data: nonempty backup sum, nonempty source tree.

    backup1 = BackupMetadata(
        '324t9uagfkjhds', BackupStartInfo(datetime(2010, 1, 8, 12, 34, 22, tzinfo=timezone.utc)), None)
    backup2 = BackupMetadata(
        'h3f4078394fgh', BackupStartInfo(datetime(2010, 5, 1, 23, 4, 2, tzinfo=timezone.utc)), None)
    backup3 = BackupMetadata(
        '45gserwafdagwaeiu', BackupStartInfo(datetime(2010, 10, 1, 4, 6, 32, tzinfo=timezone.utc)), None)
    backup_sum = BackupSum(BackupSum.Directory('',
        files=[BackupSum.File('file_x.pdf', backup1), BackupSum.File('file_y', backup3)],
        subdirectories=[
            BackupSum.Directory('dir_a',
                files=[BackupSum.File('file_a_a.txt', backup1), BackupSum.File('file_a_b.png', backup2),
                       BackupSum.File('file_a_c.exe', backup3)]),
            BackupSum.Directory('dir_b', files=[BackupSum.File('foo', backup2)]),
            BackupSum.Directory('dir_c', files=[BackupSum.File('bar.lnk', backup1)], subdirectories=[
                BackupSum.Directory('dir_c_a', files=[BackupSum.File('file_c_a_qux.a', backup1)])
            ]),
            BackupSum.Directory('extra_dir', files=[BackupSum.File('yeah_man', backup3)])
        ]
    ))
    source_tree = filesystem.Directory('',
        files=[
            # file_x.pdf removed
            filesystem.File('file_z', datetime(2010, 7, 3, 8, 9, 3, tzinfo=timezone.utc)),           # New
            filesystem.File('file_y', datetime(2010, 11, 2, 3, 30, 30, tzinfo=timezone.utc)),        # Existing modified
        ],
        subdirectories=[
            filesystem.Directory('dir_a',       # Existing
                files=[
                    filesystem.File('file_a_d.docx', datetime(2010, 9, 9, 9, 9, 9, tzinfo=timezone.utc)),    # New
                    filesystem.File('file_a_a.txt', datetime(2010, 1, 7, 12, 34, 22, tzinfo=timezone.utc)),  # Existing unmodified
                    # file_a_c.exe removed
                    filesystem.File('file_a_b.png', datetime(2010, 6, 2, 20, 1, 1, tzinfo=timezone.utc)),  # Existing modified
                ],
                subdirectories=[
                    filesystem.Directory('dir_a_a'),    # New
                    filesystem.Directory('dir_a_b',     # New
                        files=[filesystem.File('new_file', datetime(2011, 1, 1, 1, 1, 1, tzinfo=timezone.utc))])  # New
                ]),
            filesystem.Directory('dir_b',       # Existing
                files=[filesystem.File('foo', datetime(2010, 5, 1, 12, 4, 2, tzinfo=timezone.utc))]), # Existing unmodified
            filesystem.Directory('dir_c',       # Existing
                files=[filesystem.File('bar.lnk', datetime(2009, 11, 23, 22, 50, 12, tzinfo=timezone.utc))]), # Existing unmodified
                # dir_c_a removed
            # extra_dir removed
            filesystem.Directory('new_dir1'),       # New
            filesystem.Directory('new_dir2', subdirectories=[       # New
                filesystem.Directory('new_dir_nested')      # New
            ]),
            filesystem.Directory('new_dir_big', subdirectories=[            # New
                filesystem.Directory('another new dir', subdirectories=[    # New
                    filesystem.Directory('final new dir... maybe',          # New
                        files=[filesystem.File('wrgauh', datetime(2012, 12, 12, 12, 12, 21, tzinfo=timezone.utc))])  # New
                ])
            ])
        ])

    actual_plan = BackupPlan.new(source_tree, backup_sum)

    expected_plan = BackupPlan(BackupPlan.Directory('',
        copied_files=['file_z', 'file_y'], removed_files=['file_x.pdf'], removed_directories=['extra_dir'],
        contains_copied_files=True, contains_removed_items=True,
        subdirectories=[
            BackupPlan.Directory('dir_a',
                copied_files=['file_a_d.docx', 'file_a_b.png'], removed_files=['file_a_c.exe'],
                contains_copied_files=True, contains_removed_items=True,
                subdirectories=[
                    BackupPlan.Directory('dir_a_b', copied_files=['new_file'], contains_copied_files=True)
                ]),
            BackupPlan.Directory('dir_c', removed_directories=['dir_c_a'], contains_removed_items=True),
            BackupPlan.Directory('new_dir_big', contains_copied_files=True, subdirectories=[
                BackupPlan.Directory('another new dir', contains_copied_files=True, subdirectories=[
                    BackupPlan.Directory('final new dir... maybe', copied_files=['wrgauh'], contains_copied_files=True)
                ])
            ])
        ]))

    assert actual_plan == expected_plan


def test_backup_plan_new_empty_sum() -> None:
    # Empty backup sum and nonempty source tree.

    source_tree = filesystem.Directory('',
        files=[
            filesystem.File('1 file', datetime(1999, 3, 2, 1, 2, 55, tzinfo=timezone.utc)),
            filesystem.File('two files', datetime.now(timezone.utc)),
        ],
        subdirectories=[
            filesystem.Directory('seriously',
                files=[
                    filesystem.File('running.out', datetime(2010, 10, 12, 13, 14, 16, tzinfo=timezone.utc)),
                    filesystem.File('of_names.jpg', datetime(2015, 10, 10, 10, 10, 10, tzinfo=timezone.utc))
                ],
                subdirectories=[
                    filesystem.Directory('empty'),
                    filesystem.Directory('NOT EMPTY',
                        files=[filesystem.File('foo&bar', datetime(2001, 3, 2, 4, 1, 5, tzinfo=timezone.utc))])
                ]),
            filesystem.Directory('LAST_dir',
                files=[filesystem.File('qux', datetime(2020, 2, 20, 20, 20, 20, 42, tzinfo=timezone.utc))])
        ])
    backup_sum = BackupSum()

    actual_plan = BackupPlan.new(source_tree, backup_sum)

    expected_plan = BackupPlan(BackupPlan.Directory('',
        copied_files=['1 file', 'two files'], contains_copied_files=True,
        subdirectories=[
            BackupPlan.Directory('seriously', copied_files=['running.out', 'of_names.jpg'], contains_copied_files=True,
                subdirectories=[
                    BackupPlan.Directory('NOT EMPTY', copied_files=['foo&bar'], contains_copied_files=True)
                ]),
            BackupPlan.Directory('LAST_dir', copied_files=['qux'], contains_copied_files=True)
        ]))

    assert actual_plan == expected_plan


def test_backup_plan_new_all_removed() -> None:
    # Nonempty backup sum and empty source tree.

    source_tree = filesystem.Directory('', files=[], subdirectories=[])

    backup1 = BackupMetadata(
        '6759rt6rt6rt6', BackupStartInfo(datetime(2010, 3, 5, 12, 49, 56, tzinfo=timezone.utc)), None)
    backup2 = BackupMetadata(
        '5436eli8frfcz', BackupStartInfo(datetime(2010, 5, 1, 23, 4, 2, tzinfo=timezone.utc)), None)

    backup_sum = BackupSum(BackupSum.Directory('',
        files=[BackupSum.File('foo.bmp', backup1), BackupSum.File('bar', backup2)],
        subdirectories=[
            BackupSum.Directory('dir1', files=[
                BackupSum.File('dir1_file1.jpeg', backup2), BackupSum.File('dir1_file2', backup1)]),
            BackupSum.Directory('dir2', files=[BackupSum.File('dir2_file1.xlsx', backup1)], subdirectories=[
                BackupSum.Directory('dir1_dir1', files=[BackupSum.File('some file', backup2)], subdirectories=[
                    BackupSum.Directory('last directory!', files=[BackupSum.File('last_FILE.file', backup2)])
                ])
            ])
        ]))

    actual_plan = BackupPlan.new(source_tree, backup_sum)

    expected_plan = BackupPlan(BackupPlan.Directory(
        '', removed_files=['foo.bmp', 'bar'], removed_directories=['dir1', 'dir2'], contains_removed_items=True))

    assert actual_plan == expected_plan


def test_execute_backup_plan(tmpdir) -> None:
    source_path = tmpdir / 'source'
    source_path.mkdir()
    (source_path / 'Modified.txt').write_text('this is modified.txt')
    (source_path / 'file2').touch()
    (source_path / 'another file.docx').write_text('this is another file')
    (source_path / 'my directory').mkdir()
    (source_path / 'my directory' / 'modified1.baz').write_text('foo bar qux')
    (source_path / 'my directory' / 'an unmodified file').write_text('qux bar foo')
    (source_path / 'unmodified_dir').mkdir()
    (source_path / 'unmodified_dir' / 'some_file.png').write_text('doesnt matter')
    (source_path / 'unmodified_dir' / 'more files.md').write_text('doesnt matter2')
    (source_path / 'unmodified_dir' / 'lastFile.jkl').write_text('doesnt matter3')
    (source_path / 'something').mkdir()
    (source_path / 'something' / 'qwerty').mkdir()
    (source_path / 'something' / 'qwerty' / 'wtoeiur').write_text('content')
    (source_path / 'something' / 'qwerty' / 'do not copy').write_text('magic contents')
    (source_path / 'something' / 'uh oh').mkdir()
    (source_path / 'something' / 'uh oh' / 'failure1').write_text('this file wont be copied!')
    (source_path / 'something' / 'uh oh' / 'another_dir').mkdir()
    (source_path / 'something' / 'uh oh' / 'another_dir' / 'failure__2.bin').write_text('something important')

    destination_path = tmpdir / 'destination'
    destination_path.mkdir()

    plan = BackupPlan(BackupPlan.Directory('',
        copied_files=['Modified.txt', 'file2', 'nonexistent-file.yay'],
        removed_files=['file removed'], removed_directories=['removed dir'],
        contains_copied_files=True, contains_removed_items=True,
        subdirectories=[
            BackupPlan.Directory('my directory', copied_files=['modified1.baz'], removed_directories=['qux'],
                                 contains_copied_files=True, contains_removed_items=True),
            BackupPlan.Directory('something', contains_copied_files=True, subdirectories=[
                BackupPlan.Directory('qwerty', copied_files=['wtoeiur'], contains_copied_files=True),
                BackupPlan.Directory('uh oh', copied_files=['failure1'], contains_copied_files=True, subdirectories=[
                    BackupPlan.Directory('another_dir', copied_files=['failure__2.bin'], contains_copied_files=True)
                ])
            ]),
            BackupPlan.Directory('nonexistent_directory', copied_files=['flower'], removed_files=['zxcv'],
                                 contains_copied_files=True, contains_removed_items=True),
            BackupPlan.Directory('no_copied_files', removed_files=['foo', 'bar', 'notqux'], contains_removed_items=True)
        ]))

    # Create this file to force a directory creation failure.
    (destination_path / 'something').mkdir(parents=True)
    (destination_path / 'something' / 'uh oh').touch()

    mkdir_errors: List[Tuple[Path, OSError]] = []
    copy_errors: List[Tuple[Path, Path, OSError]] = []
    callbacks = ExecuteBackupPlanCallbacks(
        on_mkdir_error=lambda p, e: mkdir_errors.append((p, e)),
        on_copy_error=lambda s, d, e: copy_errors.append((s, d, e)))

    with AssertFilesystemUnmodified(source_path):
        actual_results = execute_backup_plan(plan, source_path, destination_path, callbacks)

    (destination_path / 'something' / 'uh oh').unlink(missing_ok=False)

    expected_manifest = BackupManifest(BackupManifest.Directory('',
        copied_files=['Modified.txt', 'file2'],
        removed_files=['file removed'], removed_directories=['removed dir'],
        subdirectories=[
            BackupManifest.Directory('my directory', copied_files=['modified1.baz'], removed_directories=['qux']),
            BackupManifest.Directory('something', subdirectories=[
                BackupManifest.Directory('qwerty', copied_files=['wtoeiur'])
            ]),
            BackupManifest.Directory('nonexistent_directory', removed_files=['zxcv']),
            BackupManifest.Directory('no_copied_files', removed_files=['foo', 'bar', 'notqux'])
        ]))
    expected_results = ExecuteBackupPlanResults(
        manifest=expected_manifest, paths_skipped=True, files_copied=4, files_removed=5)

    assert dir_entries(destination_path) == \
           {'Modified.txt', 'file2', 'my directory', 'something', 'nonexistent_directory'}
    assert (destination_path / 'Modified.txt').read_text() == 'this is modified.txt'
    assert (destination_path / 'file2').read_text() == ''
    assert dir_entries(destination_path / 'my directory') == {'modified1.baz'}
    assert (destination_path / 'my directory' / 'modified1.baz').read_text() == 'foo bar qux'
    assert dir_entries(destination_path / 'something') == {'qwerty'}
    assert dir_entries(destination_path / 'something' / 'qwerty') == {'wtoeiur'}
    assert (destination_path / 'something' / 'qwerty' / 'wtoeiur').read_text() == 'content'
    assert dir_entries(destination_path / 'nonexistent_directory') == set()

    assert actual_results == expected_results

    assert len(mkdir_errors) == 1
    assert mkdir_errors[0][0] == destination_path / 'something' / 'uh oh'
    assert isinstance(mkdir_errors[0][1], FileExistsError)

    assert len(copy_errors) == 2
    assert copy_errors[0][0] == source_path / 'nonexistent-file.yay'
    assert copy_errors[0][1] == destination_path / 'nonexistent-file.yay'
    assert isinstance(copy_errors[0][2], FileNotFoundError)
    assert copy_errors[1][0] == source_path / 'nonexistent_directory' / 'flower'
    assert copy_errors[1][1] == destination_path / 'nonexistent_directory' / 'flower'
    assert isinstance(copy_errors[1][2], FileNotFoundError)


def test_execute_backup_plan_empty_plan(tmpdir) -> None:
    # Empty backup plan and empty source directory.

    source_path = tmpdir / 'source'
    source_path.mkdir()

    destination_path = tmpdir / 'destination'
    destination_path.mkdir()

    plan = BackupPlan()

    callbacks = ExecuteBackupPlanCallbacks(
        on_mkdir_error=lambda path, error: pytest.fail(f'Unexpected mkdir error: {path=} {error=}'),
        on_copy_error=lambda src, dest, error: pytest.fail(f'Unexpected copy error: {src=} {dest=} {error=}'))

    with AssertFilesystemUnmodified(source_path):
        actual_results = execute_backup_plan(plan, source_path, destination_path, callbacks)

    expected_results = ExecuteBackupPlanResults(BackupManifest(), False, 0, 0)
    assert actual_results == expected_results

    assert dir_entries(destination_path) == set()
