from helpers import AssertFilesystemUnmodified, run_application


# TODO? check program console output


def test_restore_no_args() -> None:
    process = run_application('restore')
    assert process.returncode == 1


def test_restore_too_few_args(tmpdir) -> None:
    with AssertFilesystemUnmodified(tmpdir):
        process = run_application('restore', str(tmpdir))
    assert process.returncode == 1


def test_restore_all() -> None:
    # Neither backup name nor time specified, restore from all backups.

    # TODO
    assert False


def test_restore_name() -> None:
    # Backup name specified, restore up to that backup.

    # TODO
    assert False


def test_restore_time() -> None:
    # Backup time specified, restore up to that time.

    # TODO
    assert False
