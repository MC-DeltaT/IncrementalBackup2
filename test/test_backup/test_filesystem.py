from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest

from incremental_backup.backup.exclude import ExcludePattern
from incremental_backup.backup.filesystem import Directory, scan_filesystem, ScanFilesystemCallbacks

from helpers import AssertFilesystemUnmodified, unordered_equal


def test_directory_init() -> None:
    directory = Directory('foo\u6856_BAR~!@#$%^&')
    assert directory.name == 'foo\u6856_BAR~!@#$%^&'
    assert directory.files == []
    assert directory.subdirectories == []


def test_scan_filesystem_no_excludes(tmpdir) -> None:
    time = datetime.now(timezone.utc)
    (tmpdir / 'a').mkdir()
    (tmpdir / 'a' / 'aA').mkdir()
    (tmpdir / 'a' / 'ab').mkdir()
    (tmpdir / 'b').mkdir()
    (tmpdir / 'b' / 'ba').mkdir()
    (tmpdir / 'b' / 'ba' / 'file_ba_1.jpg').touch()
    (tmpdir / 'b' / 'ba' / 'FILE_ba_2.txt').touch()
    (tmpdir / 'b' / 'bb').mkdir()
    (tmpdir / 'b' / 'bb' / 'bba').mkdir()
    (tmpdir / 'b' / 'bb' / 'bba' / 'bbaa').mkdir()
    (tmpdir / 'b' / 'bb' / 'bba' / 'bbaa' / 'file\u4569_bbaa').touch()
    (tmpdir / 'C').mkdir()
    (tmpdir / 'file.txt').touch()

    # Not sure how to test error situations.

    callbacks = ScanFilesystemCallbacks(
        on_exclude=lambda path: pytest.fail(f'Unexpected on_exclude: {path=}'),
        on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
        on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
    )

    with AssertFilesystemUnmodified(tmpdir):
        results = scan_filesystem(tmpdir, (), callbacks)

    assert not results.paths_skipped

    root = results.tree
    assert root.name == ''
    assert len(root.files) == 1 and len(root.subdirectories) == 3
    file = root.files[0]
    assert file.name == 'file.txt' and abs((file.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    a = next(d for d in root.subdirectories if d.name == 'a')
    assert a.files == [] and len(a.subdirectories) == 2
    aa = next(d for d in a.subdirectories if d.name == 'aA')
    assert aa.files == [] and aa.subdirectories == []
    ab = next(d for d in a.subdirectories if d.name == 'ab')
    assert ab.files == [] and ab.subdirectories == []
    b = next(d for d in root.subdirectories if d.name == 'b')
    assert b.files == []
    ba = next(d for d in b.subdirectories if d.name == 'ba')
    assert len(ba.files) == 2 and ba.subdirectories == []
    file_ba_1 = next(f for f in ba.files if f.name == 'file_ba_1.jpg')
    assert abs((file_ba_1.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    file_ba_2 = next(f for f in ba.files if f.name == 'FILE_ba_2.txt')
    assert abs((file_ba_2.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    bb = next(d for d in b.subdirectories if d.name == 'bb')
    assert bb.files == [] and len(bb.subdirectories) == 1
    bba = next(d for d in bb.subdirectories if d.name == 'bba')
    assert bba.files == [] and len(bba.subdirectories) == 1
    bbaa = next(d for d in bba.subdirectories if d.name == 'bbaa')
    assert len(bbaa.files) == 1 and bbaa.subdirectories == []
    file_bbaa = bbaa.files[0]
    assert file_bbaa.name == 'file\u4569_bbaa' and abs((file_bbaa.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    c = next(d for d in root.subdirectories if d.name == 'C')
    assert c.files == [] and c.subdirectories == []


def test_scan_filesystem_some_excludes(tmpdir) -> None:
    exclude_patterns = (r'.*/\.git/', '/temp/', '/un\xEFi\uC9F6c\u91F5ode\\.txt', r'.*\.bin')
    exclude_patterns = tuple(map(ExcludePattern, exclude_patterns))

    time = datetime.now(timezone.utc)
    (tmpdir / 'un\xEFi\uC9F6c\u91F5ode.txt').touch()
    (tmpdir / 'foo.jpg').touch()
    (tmpdir / 'temp').mkdir()
    (tmpdir / 'temp' / 'a_file').touch()
    (tmpdir / 'temp' / 'a_dir' / 'b_dir').mkdir(parents=True)
    (tmpdir / 'Code' / 'project').mkdir(parents=True)
    (tmpdir / 'Code' / 'project' / 'README').touch()
    (tmpdir / 'Code' / 'project' / 'src').mkdir()
    (tmpdir / 'Code' / 'project' / 'src' / 'main.cpp').touch()
    (tmpdir / 'Code' / 'project' / 'bin').mkdir()
    (tmpdir / 'Code' / 'project' / 'bin' / 'artifact.bin').touch()
    (tmpdir / 'Code' / 'project' / 'bin' / 'Program.exe').touch()
    (tmpdir / 'Code' / 'project' / '.git').mkdir()
    (tmpdir / 'Code' / 'project' / '.git' / 'somefile').touch()
    (tmpdir / 'empty').mkdir()

    actual_excludes: List[Path] = []
    callbacks = ScanFilesystemCallbacks(
        on_exclude=lambda path: actual_excludes.append(path),
        on_listdir_error=lambda path, error: pytest.fail(f'Unexpected on_listdir_error: {path=} {error=}'),
        on_metadata_error=lambda path, error: pytest.fail(f'Unexpected on_metadata_error: {path=} {error=}')
    )

    with AssertFilesystemUnmodified(tmpdir):
        results = scan_filesystem(tmpdir, exclude_patterns, callbacks)

    assert not results.paths_skipped

    root = results.tree
    assert root.name == ''
    assert len(root.files) == 1 and len(root.subdirectories) == 2
    foo_jpg = next(f for f in root.files if f.name == 'foo.jpg')
    assert abs((foo_jpg.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    code = next(d for d in root.subdirectories if d.name == 'Code')
    assert len(code.files) == 0 and len(code.subdirectories) == 1
    project = next(d for d in code.subdirectories if d.name == 'project')
    assert len(project.files) == 1 and len(project.subdirectories) == 2
    readme = next(f for f in project.files if f.name == 'README')
    assert abs((readme.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    src = next(d for d in project.subdirectories if d.name == 'src')
    assert len(src.files) == 1 and len(src.subdirectories) == 0
    main_cpp = next(f for f in src.files if f.name == 'main.cpp')
    assert abs((main_cpp.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    bin_ = next(d for d in project.subdirectories if d.name == 'bin')
    assert len(bin_.files) == 1 and len(bin_.subdirectories) == 0
    program_exe = next(f for f in bin_.files if f.name == 'Program.exe')
    assert abs((program_exe.last_modified - time).total_seconds()) < FILE_MODIFY_TIME_TOLERANCE
    empty = next(d for d in root.subdirectories if d.name == 'empty')
    assert len(empty.files) == 0 and len(empty.subdirectories) == 0

    expected_excludes = (
        tmpdir / 'un\xEFi\uC9F6c\u91F5ode.txt', tmpdir / 'temp', tmpdir / 'Code' / 'project' / '.git',
        tmpdir / 'Code' / 'project' / 'bin' / 'artifact.bin')
    assert unordered_equal(actual_excludes, expected_excludes)


# Tolerance on file last modification time for testing scan_filesystem().
FILE_MODIFY_TIME_TOLERANCE = 5      # Seconds
