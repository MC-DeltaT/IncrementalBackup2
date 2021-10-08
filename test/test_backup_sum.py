from datetime import datetime, timezone

from backup_manifest import BackupManifest
from backup_metadata import BackupMetadata
from backup_start_info import BackupStartInfo
from backup_sum import BackupSum


def test_backup_sum_empty() -> None:
    backup_sum = BackupSum(())
    assert backup_sum.root == BackupSum.Directory('', [], [])

    backup_sum = BackupSum((
        BackupMetadata('aergfkhj45', BackupStartInfo(datetime.now(timezone.utc)), BackupManifest()),
        BackupMetadata('08594ghwe984', BackupStartInfo(datetime.now(timezone.utc)), BackupManifest()),
        BackupMetadata('2534698h', BackupStartInfo(datetime.now(timezone.utc)), BackupManifest()),
    ))
    expected_root = BackupSum.Directory('')
    assert backup_sum.root == expected_root


def test_backup_sum_one_backup() -> None:
    metadata = BackupMetadata('5k46j25b25h652b', BackupStartInfo(datetime.now(timezone.utc)), BackupManifest(
        BackupManifest.Directory('', copied_files=['a'], removed_directories=['b'], subdirectories=[
            BackupManifest.Directory('c', copied_files=['ca'], removed_files=['cb'], subdirectories=[
                BackupManifest.Directory('cc', subdirectories=[
                    BackupManifest.Directory('cca', removed_directories=['ccaa'])
                ])
            ]),
            BackupManifest.Directory('d', subdirectories=[
                BackupManifest.Directory('da', subdirectories=[
                    BackupManifest.Directory('daa', copied_files=['daaa'])
                ])
            ]),
            BackupManifest.Directory('e')
        ])
    ))

    backup_sum = BackupSum((metadata,))

    expected_root = BackupSum.Directory('', files=[BackupSum.File('a', metadata)], subdirectories=[
        BackupSum.Directory('c', files=[BackupSum.File('ca', metadata)]),
        BackupSum.Directory('d', subdirectories=[
            BackupSum.Directory('da', subdirectories=[
                BackupSum.Directory('daa', files=[BackupSum.File('daaa', metadata)])
            ])
        ])
    ])

    assert backup_sum.root == expected_root


def test_backup_sum_multiple_backups() -> None:
    metadata1 = BackupMetadata('456jkh2', BackupStartInfo(datetime(2021, 4, 12, 13, 54, 23, tzinfo=timezone.utc)), BackupManifest(
        BackupManifest.Directory('', copied_files=['foo', 'bar'], subdirectories=[
            BackupManifest.Directory('a', copied_files=['a_file1'], subdirectories=[
                BackupManifest.Directory('aa', copied_files=['aa_file1', 'aa_file2'], subdirectories=[
                    BackupManifest.Directory('aaa')
                ]),
                BackupManifest.Directory('ab', copied_files=['ab_file1', 'ab_file2', 'ab_file3', 'ab_file4'])
            ]),
            BackupManifest.Directory('b', copied_files=['b_file1']),
            BackupManifest.Directory('c')
        ])
    ))
    metadata2 = BackupMetadata('98065hjgghgj', BackupStartInfo(datetime(2021, 5, 11, 6, 6, 36, tzinfo=timezone.utc)), BackupManifest(
        BackupManifest.Directory('', copied_files=['bar', 'qux'], subdirectories=[
            BackupManifest.Directory('a', removed_directories=['aa'], subdirectories=[
                BackupManifest.Directory('ab')
            ]),
            BackupManifest.Directory('c', removed_files=['c_file'])
        ])
    ))
    metadata3 = BackupMetadata('87hgf5jnka', BackupStartInfo(datetime(2021, 5, 11, 6, 6, 37, tzinfo=timezone.utc)), BackupManifest(
        BackupManifest.Directory('', removed_files=['foo'], removed_directories=['d'], subdirectories=[
            BackupManifest.Directory('a', subdirectories=[
                BackupManifest.Directory('ab', copied_files=['ab_file3'])
            ]),
            BackupManifest.Directory('b', copied_files=['b_file2'], removed_files=['b_file1'], subdirectories=[
                BackupManifest.Directory('ba', removed_directories=['baa'])
            ])
        ])
    ))

    backup_sum = BackupSum((metadata2, metadata1, metadata3))

    expected_root = BackupSum.Directory('',
        files=[BackupSum.File('bar', metadata2), BackupSum.File('qux', metadata2)],
        subdirectories=[
            BackupSum.Directory('a', files=[BackupSum.File('a_file1', metadata1)], subdirectories=[
                BackupSum.Directory('ab', files=[
                    BackupSum.File('ab_file1', metadata1),
                    BackupSum.File('ab_file2', metadata1),
                    BackupSum.File('ab_file3', metadata3),
                    BackupSum.File('ab_file4', metadata1)
                ])
            ]),
            BackupSum.Directory('b', files=[BackupSum.File('b_file2', metadata3)]),
        ])

    assert backup_sum.root == expected_root
