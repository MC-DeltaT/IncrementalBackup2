import re

from incremental_backup.path_exclude import PathExcludePattern, is_path_excluded


def test_path_exclude_pattern_init() -> None:
    p1 = PathExcludePattern('')
    assert isinstance(p1.pattern, re.Pattern)
    assert p1.pattern.pattern == ''
    assert p1.pattern.flags == re.UNICODE | re.DOTALL

    p2 = PathExcludePattern('/foo/bar/dir/')
    assert isinstance(p2.pattern, re.Pattern)
    assert p2.pattern.pattern == '/foo/bar/dir/'
    assert p2.pattern.flags == re.UNICODE | re.DOTALL

    p3 = PathExcludePattern(r'/a/file\.txt')
    assert isinstance(p3.pattern, re.Pattern)
    assert p3.pattern.pattern == r'/a/file\.txt'
    assert p3.pattern.flags == re.UNICODE | re.DOTALL

    p4 = PathExcludePattern(r'.*/\.git/')
    assert isinstance(p4.pattern, re.Pattern)
    assert p4.pattern.pattern == r'.*/\.git/'
    assert p4.pattern.flags == re.UNICODE | re.DOTALL


def test_is_path_excluded_no_patterns() -> None:
    assert not is_path_excluded('/', ())
    assert not is_path_excluded('/foo/bar', ())
    assert not is_path_excluded('/some/Directory/FILE', ())
    assert not is_path_excluded('/longer/path/to/file/with/un\u1234ico\u7685de\xFA.jpg', ())


def test_is_path_excluded_ascii_paths() -> None:
    patterns = ('/foo/dir_b/some_dir/', '/path/to/file', r'/path/to/file_with_ext\.jpg', r'/\$RECYCLE\.BIN/')
    patterns = tuple(map(PathExcludePattern, patterns))

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
    patterns = tuple(map(PathExcludePattern, patterns))

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
    patterns = tuple(map(PathExcludePattern, patterns))

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
    patterns = tuple(map(PathExcludePattern, patterns))

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
