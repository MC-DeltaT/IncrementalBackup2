import pytest

from incremental_backup.meta.structure import BACKUP_NAME_LENGTH, BackupDirectoryCreationError, \
    create_new_backup_directory, generate_backup_name

from helpers import AssertFilesystemUnmodified


def test_backup_name_length() -> None:
    assert BACKUP_NAME_LENGTH >= 10


def test_generate_backup_name() -> None:
    names = [generate_backup_name() for _ in range(24356)]
    assert all(len(name) == BACKUP_NAME_LENGTH for name in names)
    assert all(name.isascii() and name.isalnum() for name in names)
    assert all(name.casefold() == name for name in names)
    assert len(set(names)) == len(names)        # Very low chance any names are the same.


def test_create_new_backup_directory_nonexistent(tmpdir) -> None:
    target_dir = tmpdir / 'target_dir'
    create_new_backup_directory(target_dir)
    assert target_dir.exists()
    entries = list(target_dir.iterdir())
    assert len(entries) == 1
    name = entries[0].name
    assert len(name) == BACKUP_NAME_LENGTH
    assert name.isascii() and name.isalnum()


def test_create_new_backup_directory_invalid(tmpdir) -> None:
    target_dir = tmpdir / 'target_dir'
    target_dir.touch()

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(BackupDirectoryCreationError):
            create_new_backup_directory(target_dir)
