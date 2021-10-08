import re
from datetime import datetime, timezone
from pathlib import Path

from backup import compile_exclude_pattern, is_path_excluded, scan_filesystem


def test_scan_filesystem_no_excludes(tmpdir) -> None:
    tmpdir = Path(tmpdir)

    time = datetime.now(timezone.utc)
    (tmpdir / 'a').mkdir(parents=True)
    (tmpdir / 'a' / 'aA').mkdir(parents=True)
    (tmpdir / 'a' / 'ab').mkdir(parents=True)
    (tmpdir / 'b').mkdir(parents=True)
    (tmpdir / 'b' / 'ba').mkdir(parents=True)
    (tmpdir / 'b' / 'ba' / 'file_ba_1.jpg').touch()
    (tmpdir / 'b' / 'ba' / 'FILE_ba_2.txt').touch()
    (tmpdir / 'b' / 'bb').mkdir(parents=True)
    (tmpdir / 'b' / 'bb' / 'bba').mkdir(parents=True)
    (tmpdir / 'b' / 'bb' / 'bba' / 'bbaa').mkdir(parents=True)
    (tmpdir / 'b' / 'bb' / 'bba' / 'bbaa' / 'file\u4569_bbaa').touch()
    (tmpdir / 'C').mkdir(parents=True)
    (tmpdir / 'file.txt').touch()

    root, paths_skipped = scan_filesystem(tmpdir, ())

    assert not paths_skipped

    assert root.name == ''
    assert len(root.files) == 1 and len(root.subdirectories) == 3
    file = root.files[0]
    assert file.name == 'file.txt' and abs((file.last_modified - time).seconds) < 5
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
    assert abs((file_ba_1.last_modified - time).seconds) < 5
    file_ba_2 = next(f for f in ba.files if f.name == 'FILE_ba_2.txt')
    assert abs((file_ba_2.last_modified - time).seconds) < 5
    bb = next(d for d in b.subdirectories if d.name == 'bb')
    assert bb.files == [] and len(bb.subdirectories) == 1
    bba = next(d for d in bb.subdirectories if d.name == 'bba')
    assert bba.files == [] and len(bba.subdirectories) == 1
    bbaa = next(d for d in bba.subdirectories if d.name == 'bbaa')
    assert len(bbaa.files) == 1 and bbaa.subdirectories == []
    file_bbaa = bbaa.files[0]
    assert file_bbaa.name == 'file\u4569_bbaa' and abs((file_bbaa.last_modified - time).seconds) < 5
    c = next(d for d in root.subdirectories if d.name == 'C')
    assert c.files == [] and c.subdirectories == []


def test_scan_filesystem_some_excludes(tmpdir) -> None:
    tmpdir = Path(tmpdir)

    exclude_patterns = (r'.*/\.git/', '/temp/', '/un\xEFi\uC9F6c\u91F5ode\\.txt', r'.*\.bin')
    exclude_patterns = tuple(map(compile_exclude_pattern, exclude_patterns))

    time = datetime.now(timezone.utc)
    (tmpdir / 'un\xEFi\uC9F6c\u91F5ode.txt').touch()
    (tmpdir / 'foo.txt').touch()
    (tmpdir / 'temp').mkdir(parents=True)
    (tmpdir / 'temp' / 'a_file').touch()
    (tmpdir / 'temp' / 'a_dir').mkdir(parents=True)
    (tmpdir / 'temp' / 'a_dir' / 'b_dir').mkdir(parents=True)
    (tmpdir / 'Code' / 'project').mkdir(parents=True)
    (tmpdir / 'Code' / 'project' / 'src').mkdir(parents=True)
    (tmpdir / 'Code' / 'project' / 'src' / 'main.cpp').touch()
    (tmpdir / 'Code' / 'project' / 'bin').mkdir(parents=True)
    (tmpdir / 'Code' / 'project' / 'bin' / 'artifact.bin').touch()
    (tmpdir / 'Code' / 'project' / 'bin' / 'program.exe').touch()
    (tmpdir / 'Code' / 'project' / '.git').mkdir(parents=True)
    (tmpdir / 'Code' / 'project' / '.git' / 'somefile').touch()
    (tmpdir / 'empty').mkdir(parents=True)

    root, paths_skipped = scan_filesystem(tmpdir, exclude_patterns)

    assert not paths_skipped

    # TODO
    assert root.name == ''
    assert len(root.files) == 1 and len(root.subdirectories) == 2


def test_compute_backup_plan() -> None:
    """TODO"""


def test_compile_exclude_pattern() -> None:
    p1 = compile_exclude_pattern('')
    assert isinstance(p1, re.Pattern)
    assert p1.pattern == ''
    assert p1.flags == re.UNICODE | re.DOTALL

    p2 = compile_exclude_pattern('/foo/bar/dir/')
    assert isinstance(p2, re.Pattern)
    assert p2.pattern == '/foo/bar/dir/'
    assert p2.flags == re.UNICODE | re.DOTALL

    p3 = compile_exclude_pattern(r'/a/file\.txt')
    assert isinstance(p3, re.Pattern)
    assert p3.pattern == r'/a/file\.txt'
    assert p3.flags == re.UNICODE | re.DOTALL

    p4 = compile_exclude_pattern(r'.*/\.git/')
    assert isinstance(p4, re.Pattern)
    assert p4.pattern == r'.*/\.git/'
    assert p4.flags == re.UNICODE | re.DOTALL


def test_is_path_excluded_no_patterns() -> None:
    assert not is_path_excluded('/', ())
    assert not is_path_excluded('/foo/bar', ())
    assert not is_path_excluded('/some/Directory/FILE', ())
    assert not is_path_excluded('/longer/path/to/file/with/un\u1234ico\u7685de\xFA.jpg', ())


def test_is_path_excluded_ascii_paths() -> None:
    patterns = ('/foo/dir_b/some_dir/', '/path/to/file', r'/path/to/file_with_ext\.jpg', r'/\$RECYCLE\.BIN/')
    patterns = tuple(map(compile_exclude_pattern, patterns))

    assert is_path_excluded('/foo/dir_b/some_dir/', patterns)
    assert is_path_excluded('/path/to/file', patterns)
    assert is_path_excluded('/path/to/file_with_ext.jpg', patterns)
    assert is_path_excluded('/$RECYCLE.BIN/', patterns)
    assert not is_path_excluded('/foo/dir_b/some_dir', patterns)
    assert not is_path_excluded('/foo', patterns)
    assert not is_path_excluded('/foo/', patterns)
    assert not is_path_excluded('/foo/dir_b/some_dir/qux/', patterns)
    assert not is_path_excluded('/foo/dir_b/some_dir/qux', patterns)
    assert not is_path_excluded('/path/to/file/', patterns)
    assert not is_path_excluded('/path/to/file_with_ext', patterns)
    assert not is_path_excluded('/ayy', patterns)
    assert not is_path_excluded('/a/b/c/d/e/f/', patterns)


def test_is_path_excluded_case_sensitivity() -> None:
    patterns = ('/all/lowercase', '/ALL/UPPERCASE/', '/mixd/CASE/Path')
    patterns = tuple(map(compile_exclude_pattern, patterns))

    assert is_path_excluded('/all/lowercase', patterns)
    assert is_path_excluded('/ALL/UPPERCASE/', patterns)
    assert is_path_excluded('/mixd/CASE/Path', patterns)
    assert not is_path_excluded('/ALL/LOWERCASE', patterns)
    assert not is_path_excluded('/all/uppercase', patterns)
    assert not is_path_excluded('/MIXD/case/pATH', patterns)
    assert not is_path_excluded('/All/lowercase', patterns)
    assert not is_path_excluded('/ALL/UPPERCASe/', patterns)
    assert not is_path_excluded('/MIXd/CaSe/PaTH', patterns)


def test_is_path_excluded_unicode_paths() -> None:
    patterns = ('/dir\n/with/U\u6A72n\xDFiC\u6D42o\u5B4Dd\xFFE/', '/\u3764\u3245\u6475/qux/\u023Ffile\\.pdf',
                '/un\xEFi\uC9F6c\u91F5ode\\.txt')
    patterns = tuple(map(compile_exclude_pattern, patterns))

    assert is_path_excluded('/dir\n/with/U\u6A72n\xDFiC\u6D42o\u5B4Dd\xFFE/', patterns)
    assert is_path_excluded('/\u3764\u3245\u6475/qux/\u023Ffile.pdf', patterns)
    assert is_path_excluded('/un\xEFi\uC9F6c\u91F5ode.txt', patterns)
    assert not is_path_excluded('/dir\n/with/U\u6A72n\xDFiC\u6D42o\u5B4Dd\xFFE', patterns)
    assert not is_path_excluded('/dir\n/with/U\u6A72n\xDFiC\u6D42o\u5B4Dd\x00E/', patterns)
    assert not is_path_excluded('/dir/with/U\u6A72n\xDFiC\u6D42o\u5B4Dd\xFFE/', patterns)
    assert not is_path_excluded('/\u3764\u3245\u6475/qux/\u023Ffile.pdf/', patterns)
    assert not is_path_excluded('/\u3764\u3245a/qux/\u023Ffile.pdf', patterns)


def test_is_path_excluded_advanced() -> None:
    patterns = (r'.*/\.git/', r'.*/__pycache__/')
    patterns = tuple(map(compile_exclude_pattern, patterns))

    assert is_path_excluded('/.git/', patterns)
    assert is_path_excluded('/my/code/project/.git/', patterns)
    assert is_path_excluded('/__pycache__/', patterns)
    assert is_path_excluded('/i/do/coding/__pycache__/', patterns)
    assert is_path_excluded('/i/do/coding/__pycache__/', patterns)
    assert not is_path_excluded('/.git', patterns)
    assert not is_path_excluded('/.git/magic/', patterns)
    assert not is_path_excluded('/.git/file', patterns)
    assert not is_path_excluded('/foo.git/', patterns)
    assert not is_path_excluded('/.git.bar/', patterns)
    assert not is_path_excluded('/__pycache__/yeah/man/', patterns)
