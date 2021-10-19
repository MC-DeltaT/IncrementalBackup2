from incremental_backup.main import script_main


# Doesn't matter what this is, it isn't used by the program.
PROGRAM_NAME_ARG = 'incremental_backup.py'


def test_no_args() -> None:
    exit_code = script_main((PROGRAM_NAME_ARG,))
    assert exit_code == 1


def test_invalid_command() -> None:
    exit_code = script_main((PROGRAM_NAME_ARG, 'magic_command', 'foo', 'bar'))
    assert exit_code == 1


def test_backup_no_args() -> None:
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup'))
    assert exit_code == 1


def test_backup_nonexistent_source(tmpdir) -> None:
    source_path = tmpdir / 'source'
    target_path = tmpdir / 'target'
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 1


def test_backup_source_is_file(tmpdir) -> None:
    source_path = tmpdir / 'source'
    target_path = tmpdir / 'target'
    source_path.ensure()
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 1


def test_backup_target_is_file(tmpdir) -> None:
    source_path = tmpdir / 'source'
    target_path = tmpdir / 'target'
    source_path.ensure_dir()
    target_path.ensure()
    exit_code = script_main((PROGRAM_NAME_ARG, 'backup', str(source_path), str(target_path)))
    assert exit_code == 1
