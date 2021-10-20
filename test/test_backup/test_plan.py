from datetime import datetime, timezone

from incremental_backup.backup.plan import BackupPlan
from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta.manifest import BackupManifest
from incremental_backup.meta.metadata import BackupMetadata
from incremental_backup.meta.start_info import BackupStartInfo
from incremental_backup.utility import filesystem


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
    backup1 = BackupMetadata(
        '324t9uagfkjhds',
        BackupStartInfo(datetime(2010, 1, 8, 12, 34, 22, tzinfo=timezone.utc)),
        BackupManifest())
    backup2 = BackupMetadata(
        'h3f4078394fgh',
        BackupStartInfo(datetime(2010, 5, 1, 23, 4, 2, tzinfo=timezone.utc)),
        BackupManifest())
    backup3 = BackupMetadata(
        '45gserwafdagwaeiu',
        BackupStartInfo(datetime(2010, 10, 1, 4, 6, 32, tzinfo=timezone.utc)),
        BackupManifest())
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


def test_backup_plan_new_empty_sum(tmpdir) -> None:
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
    backup_sum = BackupSum.from_backups(())

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
