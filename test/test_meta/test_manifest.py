from pathlib import Path

import pytest

from incremental_backup.meta.manifest import BackupManifest, BackupManifestParseError, read_backup_manifest, \
    write_backup_manifest

from helpers import AssertFilesystemUnmodified


def test_backup_manifest_directory_init() -> None:
    directory = BackupManifest.Directory('NotSure_how_m4ny_M\u2390re names-I can think OF')
    assert directory.name == 'NotSure_how_m4ny_M\u2390re names-I can think OF'
    assert directory.copied_files == []
    assert directory.removed_files == []
    assert directory.removed_directories == []
    assert directory.subdirectories == []


def test_backup_manifest_init() -> None:
    manifest = BackupManifest()
    expected_root = BackupManifest.Directory('')
    assert manifest.root == expected_root


def test_write_backup_manifest(tmpdir: Path) -> None:
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

    actual = path.read_text(encoding='utf8')

    expected = \
'''[
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
]'''

    assert actual == expected


def test_read_backup_manifest_valid(tmpdir: Path) -> None:
    path = tmpdir / 'manifest_valid.json'
    contents = '''[
        {"n": "", "cf": ["myfile675"], "rf": []},
        {"n": "dir1", "cf": ["running", "out"]},
        "^1",
        {"n": "of", "rd": ["name", "ideas"]},
        {"n": "yeah", "cf": ["I", "am"]},
        "^2",
        {"n": "finally", "rd": ["barz", "wumpus"], "rf": ["w\x23o\x64r\x79l\u8794d\u1234"]}
    ]'''
    path.write_text(contents, encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
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


def test_read_backup_manifest_empty(tmpdir: Path) -> None:
    path = tmpdir / 'manifest_empty_1.json'
    path.write_text('[]', encoding='utf8')
    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_manifest(path)
    expected = BackupManifest()
    assert actual == expected

    path = tmpdir / 'manifest_empty_2.json'
    path.write_text('[{"n": ""}]', encoding='utf8')
    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_manifest(path)
    expected = BackupManifest()
    assert actual == expected


def test_read_backup_manifest_invalid(tmpdir: Path) -> None:
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
        '[{"n": "", "cf": ["f1"], "rf": ["f2"], "extra": "value"}]',
        '[{n: "", "cf": ["f1"]}]',
        '[{"n": "", "cf": ["something"]}, {"n": "mydir", '
    )

    for i, data in enumerate(datas):
        path = tmpdir / f'manifest_invalid_{i}.json'
        path.write_text(data, encoding='utf8')

        with AssertFilesystemUnmodified(tmpdir):
            with pytest.raises(BackupManifestParseError):
                read_backup_manifest(path)


def test_read_backup_manifest_nonexistent(tmpdir: Path) -> None:
    path = tmpdir / 'manifest_nonexistent.json'
    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(FileNotFoundError):
            read_backup_manifest(path)


def test_read_backup_manifest_directory_reentry(tmpdir: Path) -> None:
    path = tmpdir / 'manifest_reentrant.json'
    contents = '''[
        {"n": ""},
        {"n": "mydir", "cf": ["copied_file1"], "rf": ["rf1"], "rd": ["rdir1"]},
        "^1",
        {"n": "mydir", "rd": ["removed_dir1"], "cf": ["cf2"], "rf": ["removed_f_2"]}
    ]'''
    path.write_text(contents, encoding='utf8')

    with AssertFilesystemUnmodified(tmpdir):
        actual = read_backup_manifest(path)

    expected = BackupManifest(BackupManifest.Directory('', subdirectories=[
        BackupManifest.Directory('mydir',
            copied_files=['copied_file1', 'cf2'], removed_files=['rf1', 'removed_f_2'],
            removed_directories=['rdir1', 'removed_dir1']),
    ]))
    assert actual == expected
