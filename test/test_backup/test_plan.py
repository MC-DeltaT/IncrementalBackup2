from datetime import datetime

from incremental_backup import filesystem
from incremental_backup.backup.plan import BackupPlan
from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta.manifest import BackupManifest
from incremental_backup.meta.metadata import BackupMetadata
from incremental_backup.meta.start_info import BackupStartInfo


def test_backup_plan_new() -> None:
    backup1 = BackupMetadata('324t9uagfkjhds', BackupStartInfo(datetime(2010, 1, 8, 12, 34, 22)), BackupManifest())
    backup2 = BackupMetadata('h3f4078394fgh', BackupStartInfo(datetime(2010, 5, 1, 23, 4, 2)), BackupManifest())
    backup3 = BackupMetadata('45gserwafdagwaeiu', BackupStartInfo(datetime(2010, 10, 1, 4, 6, 32)), BackupManifest())
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
            filesystem.File('file_z', datetime(2010, 7, 3, 8, 9, 3)),           # New
            filesystem.File('file_y', datetime(2010, 11, 2, 3, 30, 30)),        # Existing modified
        ],
        subdirectories=[
            filesystem.Directory('dir_a',       # Existing
                files=[
                    filesystem.File('file_a_d.docx', datetime(2010, 9, 9, 9, 9, 9)),    # New
                    filesystem.File('file_a_a.txt', datetime(2010, 1, 7, 12, 34, 22)),  # Existing unmodified
                    # file_a_c.exe removed
                    filesystem.File('file_a_b.png', datetime(2010, 6, 2, 20, 1, 1)),  # Existing modified
                ],
                subdirectories=[
                    filesystem.Directory('dir_a_a'),    # New
                    filesystem.Directory('dir_a_b',     # New
                        files=[filesystem.File('new_file', datetime(2011, 1, 1, 1, 1, 1))])     # New
                ]),
            filesystem.Directory('dir_b',       # Existing
                files=[filesystem.File('foo', datetime(2010, 5, 1, 12, 4, 2))]),        # Existing unmodified
            filesystem.Directory('dir_c',       # Existing
                files=[filesystem.File('bar.lnk', datetime(2009, 11, 23, 22, 50, 12))]),      # Existing unmodified
                # dir_c_a removed
            # extra_dir removed
            filesystem.Directory('new_dir1'),       # New
            filesystem.Directory('new_dir2', subdirectories=[       # New
                filesystem.Directory('new_dir_nested')      # New
            ]),
            filesystem.Directory('new_dir_big', subdirectories=[            # New
                filesystem.Directory('another new dir', subdirectories=[    # New
                    filesystem.Directory('final new dir... maybe',          # New
                        files=[filesystem.File('wrgauh', datetime(2012, 12, 12, 12, 12, 21))])      # New
                ])
            ])
        ])

    actual_plan = BackupPlan.new(source_tree, backup_sum)

    expected_manifest = BackupManifest(BackupManifest.Directory('',
        copied_files=['file_z', 'file_y'], removed_files=['file_x.pdf'], removed_directories=['extra_dir'],
        subdirectories=[
            BackupManifest.Directory('dir_a',
                copied_files=['file_a_d.docx', 'file_a_b.png'], removed_files=['file_a_c.exe'],
                subdirectories=[
                    BackupManifest.Directory('dir_a_b', copied_files=['new_file'])
                ]),
            BackupManifest.Directory('dir_c', removed_directories=['dir_c_a']),
            BackupManifest.Directory('new_dir_big', subdirectories=[
                BackupManifest.Directory('another new dir', subdirectories=[
                    BackupManifest.Directory('final new dir... maybe', copied_files=['wrgauh'])
                ])
            ])
        ]))

    assert actual_plan == expected_plan

    # TODO: test content counts
