import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

from incremental_backup.backup.backup import BackupResults, compile_exclude_pattern, do_backup, execute_backup_plan, \
    is_path_excluded, scan_filesystem
from incremental_backup.backup.plan import BackupPlan
from incremental_backup.backup.sum import BackupSum
from incremental_backup.meta.manifest import BackupManifest
from incremental_backup.meta.metadata import BackupMetadata
from incremental_backup.meta.start_info import BackupStartInfo


def test_scan_filesystem_no_excludes(tmpdir) -> None:
    tmpdir = Path(tmpdir)

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

    # Note sure how to test error situations.

    root, paths_skipped = scan_filesystem(tmpdir, ())

    MODIFY_TIME_TOLERANCE = 5       # Seconds

    assert not paths_skipped

    assert root.name == ''
    assert len(root.files) == 1 and len(root.subdirectories) == 3
    file = root.files[0]
    assert file.name == 'file.txt' and abs((file.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
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
    assert abs((file_ba_1.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    file_ba_2 = next(f for f in ba.files if f.name == 'FILE_ba_2.txt')
    assert abs((file_ba_2.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    bb = next(d for d in b.subdirectories if d.name == 'bb')
    assert bb.files == [] and len(bb.subdirectories) == 1
    bba = next(d for d in bb.subdirectories if d.name == 'bba')
    assert bba.files == [] and len(bba.subdirectories) == 1
    bbaa = next(d for d in bba.subdirectories if d.name == 'bbaa')
    assert len(bbaa.files) == 1 and bbaa.subdirectories == []
    file_bbaa = bbaa.files[0]
    assert file_bbaa.name == 'file\u4569_bbaa' and abs((file_bbaa.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    c = next(d for d in root.subdirectories if d.name == 'C')
    assert c.files == [] and c.subdirectories == []


def test_scan_filesystem_some_excludes(tmpdir) -> None:
    tmpdir = Path(tmpdir)

    exclude_patterns = (r'.*/\.git/', '/temp/', '/un\xEFi\uC9F6c\u91F5ode\\.txt', r'.*\.bin')
    exclude_patterns = tuple(map(compile_exclude_pattern, exclude_patterns))

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

    root, paths_skipped = scan_filesystem(tmpdir, exclude_patterns)

    MODIFY_TIME_TOLERANCE = 5       # Seconds

    assert not paths_skipped

    assert root.name == ''
    assert len(root.files) == 1 and len(root.subdirectories) == 2
    foo_jpg = next(f for f in root.files if f.name == 'foo.jpg')
    assert abs((foo_jpg.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    code = next(d for d in root.subdirectories if d.name == 'Code')
    assert len(code.files) == 0 and len(code.subdirectories) == 1
    project = next(d for d in code.subdirectories if d.name == 'project')
    assert len(project.files) == 1 and len(project.subdirectories) == 2
    readme = next(f for f in project.files if f.name == 'README')
    assert abs((readme.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    src = next(d for d in project.subdirectories if d.name == 'src')
    assert len(src.files) == 1 and len(src.subdirectories) == 0
    main_cpp = next(f for f in src.files if f.name == 'main.cpp')
    assert abs((main_cpp.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    bin_ = next(d for d in project.subdirectories if d.name == 'bin')
    assert len(bin_.files) == 1 and len(bin_.subdirectories) == 0
    program_exe = next(f for f in bin_.files if f.name == 'Program.exe')
    assert abs((program_exe.last_modified - time).seconds) < MODIFY_TIME_TOLERANCE
    empty = next(d for d in root.subdirectories if d.name == 'empty')
    assert len(empty.files) == 0 and len(empty.subdirectories) == 0


def test_execute_backup_plan(tmpdir) -> None:
    tmpdir = Path(tmpdir)

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
    on_mkdir_error = lambda p, e: mkdir_errors.append((p, e))

    copy_errors: List[Tuple[Path, Path, OSError]] = []
    on_copy_error = lambda s, d, e: copy_errors.append((s, d, e))

    results, manifest = execute_backup_plan(plan, source_path, destination_path,
                                            on_mkdir_error=on_mkdir_error, on_copy_error=on_copy_error)

    (destination_path / 'something' / 'uh oh').unlink(missing_ok=False)

    expected_results = BackupResults(paths_skipped=True, files_copied=4, files_removed=5)

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

    assert set(destination_path.iterdir()) == {
        destination_path / 'Modified.txt', destination_path / 'file2',
        destination_path / 'my directory', destination_path / 'something', destination_path / 'nonexistent_directory'}
    assert (destination_path / 'Modified.txt').read_text() == 'this is modified.txt'
    assert (destination_path / 'file2').read_text() == ''
    assert set((destination_path / 'my directory').iterdir()) == {destination_path / 'my directory' / 'modified1.baz'}
    assert (destination_path / 'my directory' / 'modified1.baz').read_text() == 'foo bar qux'
    assert set((destination_path / 'something').iterdir()) == {destination_path / 'something' / 'qwerty'}
    assert set((destination_path / 'something' / 'qwerty').iterdir()) == \
           {destination_path / 'something' / 'qwerty' / 'wtoeiur'}
    assert (destination_path / 'something' / 'qwerty' / 'wtoeiur').read_text() == 'content'
    assert set((destination_path / 'nonexistent_directory').iterdir()) == set()

    assert results == expected_results
    assert manifest == expected_manifest

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


def test_do_backup(tmpdir) -> None:
    tmpdir = Path(tmpdir)

    source_path = tmpdir / 'source'
    source_path.mkdir()
    (source_path / 'why why why').write_text('some gibberish')      # Existing modified
    (source_path / 'akrhjbgd').write_text('190234856 19243857 123746809 9045')      # Existing unmodified
    (source_path / 'new').write_text('   x   ')     # New
    (source_path / 'empty').mkdir()
    (source_path / 'no_changes').mkdir()
    (source_path / 'no_changes' / 'a file').write_text('f00_b4r_qu*')       # Existing unmodified
    (source_path / 'no_changes' / 'still_no_changes').mkdir()
    (source_path / 'no_changes' / 'still_no_changes' / 'un.modified').write_text('the same')    # Existing unmodified
    (source_path / 'foo').mkdir()
    (source_path / 'foo' / 'bar.avi').write_text('ignoreme!')       # New, but excluded
    (source_path / 'amazing_code_proj').mkdir()
    (source_path / 'amazing_code_proj' / '.git').mkdir()        # Existing, but excluded -> removed, hm
    (source_path / 'amazing_code_proj' / '.git' / 'config').touch()     # Existing modified, but excluded
    (source_path / 'amazing_code_proj' / '.git' / 'objects').mkdir()
    (source_path / 'amazing_code_proj' / '.git' / 'objects' / 'something').touch()      # New, but excluded
    (source_path / 'amazing_code_proj' / 'README.md').write_text('A really COOL project')       # New
    (source_path / 'amazing_code_proj' / 'src').mkdir()
    (source_path / 'amazing_code_proj' / 'src' / 'main.py').write_text('from mylib import foo ; foo("hello world")')    # Existing unmodified
    (source_path / 'amazing_code_proj' / 'src' / 'mylib.py').write_text('def foo(x): print(x)')     # Existing modified
    (source_path / 'disappear').mkdir()

    backup_past = BackupMetadata(
        '43q86y55wysh', BackupStartInfo(datetime(2018, 2, 4, 6, 9, 19, tzinfo=timezone.utc)), None)
    backup_future = BackupMetadata(
        '43q86y55wysh', BackupStartInfo(datetime.now(timezone.utc) + timedelta(days=1)), None)
    backup_sum = BackupSum(BackupSum.Directory('',
        files=[
            BackupSum.File('why why why', backup_past),
            BackupSum.File('akrhjbgd', backup_future),
            BackupSum.File('removed.file', backup_past)
        ],
        subdirectories=[
            BackupSum.Directory('no_changes', files=[BackupSum.File('a file', backup_future)],
                subdirectories=[
                    BackupSum.Directory('still_no_changes', files=[BackupSum.File('un.modified', backup_future)])
                ]),
            BackupSum.Directory('disappear', files=[BackupSum.File('to-be REmoved', backup_past)]),
            BackupSum.Directory('amazing_code_proj', subdirectories=[
                BackupSum.Directory('.git', files=[BackupSum.File('config', backup_past)]),
                BackupSum.Directory('src', files=[
                    BackupSum.File('main.py', backup_future), BackupSum.File('mylib.py', backup_past)])
            ])
        ]))

    destination_path = tmpdir / 'destination'
    destination_path.mkdir()

    exclude_patterns = ('/foo/bar.avi', r'.*/\.git/')
    exclude_patterns = tuple(map(compile_exclude_pattern, exclude_patterns))

    # Not sure how to test error situations.

    actual_results, actual_manifest = do_backup(source_path, destination_path, exclude_patterns, backup_sum)

    expected_results = BackupResults(False, files_copied=4, files_removed=2)

    assert actual_results == expected_results

    assert set(actual_manifest.root.copied_files) == {'new', 'why why why'}
    assert actual_manifest.root.removed_files == ['removed.file']
    assert actual_manifest.root.removed_directories == []
    assert len(actual_manifest.root.subdirectories) == 2
    amazing_code_proj = next(d for d in actual_manifest.root.subdirectories if d.name == 'amazing_code_proj')
    assert amazing_code_proj == BackupManifest.Directory('amazing_code_proj',
        copied_files=['README.md'], removed_directories=['.git'],
        subdirectories=[
            BackupManifest.Directory('src', copied_files=['mylib.py'])
        ])
    disappear = next(d for d in actual_manifest.root.subdirectories if d.name == 'disappear')
    assert disappear == BackupManifest.Directory('disappear', removed_files=['to-be REmoved'])

    assert set(destination_path.iterdir()) == \
           {destination_path / 'why why why', destination_path / 'new', destination_path / 'amazing_code_proj'}
    assert (destination_path / 'why why why').read_text() == 'some gibberish'
    assert (destination_path / 'new').read_text() == '   x   '
    assert set((destination_path / 'amazing_code_proj').iterdir()) == \
           {destination_path / 'amazing_code_proj' / 'README.md', destination_path / 'amazing_code_proj' / 'src'}
    assert set((destination_path / 'amazing_code_proj' / 'src').iterdir()) == \
           {destination_path / 'amazing_code_proj' / 'src' / 'mylib.py'}


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
