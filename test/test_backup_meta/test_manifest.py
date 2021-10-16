import pytest

from incremental_backup.backup_meta.manifest import BackupManifest, BackupManifestParseError, prune_backup_manifest, \
    read_backup_manifest, write_backup_manifest


def test_backup_manifest_construct() -> None:
    manifest = BackupManifest()
    expected = BackupManifest(BackupManifest.Directory('', [], []))
    assert manifest == expected


def test_prune_backup_manifest_empty() -> None:
    manifest = BackupManifest()
    prune_backup_manifest(manifest)
    expected = BackupManifest()
    assert manifest == expected


def test_prune_backup_manifest_prune_some() -> None:
    manifest = BackupManifest(BackupManifest.Directory('', copied_files=['copied_file'], subdirectories=[
        BackupManifest.Directory('a', subdirectories=[
            BackupManifest.Directory('aa', subdirectories=[
                BackupManifest.Directory('aaa')
            ])
        ]),
        BackupManifest.Directory('b'),
        BackupManifest.Directory('c', subdirectories=[
            BackupManifest.Directory('ca', subdirectories=[
                BackupManifest.Directory('caa')
            ]),
            BackupManifest.Directory('cb', removed_files=['removed_file_cb'])
        ]),
        BackupManifest.Directory('d', removed_directories=['removed_dir_d'])
    ]))

    prune_backup_manifest(manifest)

    expected = BackupManifest(BackupManifest.Directory('', copied_files=['copied_file'], subdirectories=[
        BackupManifest.Directory('c', subdirectories=[
            BackupManifest.Directory('cb', removed_files=['removed_file_cb'])
        ]),
        BackupManifest.Directory('d', removed_directories=['removed_dir_d'])
    ]))

    assert manifest == expected


def test_prune_backup_manifest_all() -> None:
    manifest = BackupManifest(BackupManifest.Directory('', subdirectories=[
        BackupManifest.Directory('foo'),
        BackupManifest.Directory('bar', subdirectories=[
            BackupManifest.Directory('bar_nested')
        ]),
        BackupManifest.Directory('qux', subdirectories=[
            BackupManifest.Directory('nested_a', subdirectories=[
                BackupManifest.Directory('nested_a_a')
            ]),
            BackupManifest.Directory('nested_b', subdirectories=[
                BackupManifest.Directory('nested_b_a', subdirectories=[
                    BackupManifest.Directory('foo'),
                    BackupManifest.Directory('bar')
                ]),
                BackupManifest.Directory('nested_b_b', subdirectories=[
                    BackupManifest.Directory('final-dir')
                ])
            ])
        ])
    ]))

    prune_backup_manifest(manifest)

    expected = BackupManifest()

    assert manifest == expected


def test_write_backup_manifest(tmpdir) -> None:
    path = tmpdir / 'manifest.json'

    backup_manifest = BackupManifest(BackupManifest.Directory('',
        copied_files=['file1.txt'], removed_files=['file2'], removed_directories=['file3.jpg'],
        subdirectories=[
            BackupManifest.Directory('foo', removed_directories=['qux', 'foo'], subdirectories=[
                BackupManifest.Directory('bar', copied_files=['great\nfile"name.pdf']),
            ]),
            BackupManifest.Directory('very very longish\u5673kinda long name', subdirectories=[
                BackupManifest.Directory('qwerty', subdirectories=[
                    BackupManifest.Directory('asdf', subdirectories=[
                        BackupManifest.Directory('zxcvbnm', removed_files=['faz', 'qaz', 'bazinga'])
                    ])
                ])
            ])
        ]))

    write_backup_manifest(path, backup_manifest)

    with open(path, 'r', encoding='utf8') as file:
        actual = file.read()

    expected = \
"""[
{
"n": "",
"cf": [
"file1.txt"
],
"rf": [
"file2"
],
"rd": [
"file3.jpg"
]
},
{
"n": "foo",
"rd": [
"qux",
"foo"
]
},
{
"n": "bar",
"cf": [
"great\\nfile\\"name.pdf"
]
},
"^2",
{
"n": "very very longish\u5673kinda long name"
},
{
"n": "qwerty"
},
{
"n": "asdf"
},
{
"n": "zxcvbnm",
"rf": [
"faz",
"qaz",
"bazinga"
]
}
]"""

    assert actual == expected


def test_read_backup_manifest_valid(tmpdir) -> None:
    path = tmpdir / 'manifest_valid.json'
    contents = """[
        {"n": "", "cf": ["myfile675"], "rf": []},
        {"n": "dir1", "cf": ["running", "out"]},
        "^1",
        {"n": "of", "rd": ["name", "ideas"]},
        {"n": "yeah", "cf": ["I", "am"]},
        "^2",
        {"n": "finally", "rd": ["barz", "wumpus"], "rf": ["w\x23o\x64r\x79l\u8794d\u1234"]}
    ]"""
    with open(path, 'w', encoding='utf8') as file:
        file.write(contents)
    actual = read_backup_manifest(path)
    expected = BackupManifest(BackupManifest.Directory('', copied_files=['myfile675'], subdirectories=[
        BackupManifest.Directory('dir1', copied_files=['running', 'out']),
        BackupManifest.Directory('of', removed_directories=['name', 'ideas'], subdirectories=[
            BackupManifest.Directory('yeah', copied_files=['I', 'am']),
        ]),
        BackupManifest.Directory('finally', removed_directories=['barz', 'wumpus'],
                                 removed_files=['w\x23o\x64r\x79l\u8794d\u1234'])
    ]))
    assert actual == expected

    with open(path, 'r', encoding='utf8') as file:
        contents_after = file.read()
    assert contents_after == contents


def test_read_backup_manifest_empty(tmpdir) -> None:
    path = tmpdir / 'manifest_empty_1.json'
    contents = '[]'
    with open(path, 'w', encoding='utf8') as file:
        file.write(contents)
    actual = read_backup_manifest(path)
    expected = BackupManifest()
    assert actual == expected
    with open(path, 'r', encoding='utf8') as file:
        contents_after = file.read()
    assert contents_after == contents

    path = tmpdir / 'manifest_empty_2.json'
    contents = '[{"n": ""}]'
    with open(path, 'w', encoding='utf8') as file:
        file.write(contents)
    actual = read_backup_manifest(path)
    expected = BackupManifest()
    assert actual == expected
    with open(path, 'r', encoding='utf8') as file:
        contents_after = file.read()
    assert contents_after == contents


def test_read_backup_manifest_invalid(tmpdir) -> None:
    datas = (
        '',
        '{}',
        'null',
        '29',
        '[null]'
        '["^"]',
        '["^1"]',
        '["^4"]',
        '[{"n": ""}, "^"]',
        '[{"n": ""}, "^1"]',
        '[{"n": ""}, "^2"]',
        '[{"n": ""}, {}]',
        '[{"n": ""}, {"n": "foo"},',
        '[{"n": ""}, {"n": "baz"}, ""]',
        '[{"n": ""}, {"n": "baz"}, true]',
        '[{"n": "", "unknown": []}]',
        '[{"n": "", "cf": ["f1", "f2", 42]}]',
        '[{"n": "", "rf": ["ab", True]}]',
        '[{"n": "", "rd": ["bar", null, "qux"]}]',
        '[{"n": "", "cf": ["f1"], "rf": ["f2"], "extra": "value"}]'
    )

    for i, data in enumerate(datas):
        path = tmpdir / f'manifest_invalid_{i}.json'
        with open(path, 'w', encoding='utf8') as file:
            file.write(data)
        with pytest.raises(BackupManifestParseError):
            read_backup_manifest(path)

        with open(path, 'r', encoding='utf8') as file:
            contents_after = file.read()
        assert contents_after == data


def test_read_backup_manifest_nonexistent(tmpdir) -> None:
    path = tmpdir / 'manifest_nonexistent.json'
    with pytest.raises(FileNotFoundError):
        read_backup_manifest(path)
