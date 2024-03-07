from pathlib import Path

from incremental_backup.prune import BackupPrunabilityOptions, is_backup_prunable

from helpers import write_valid_backup


def test_is_backup_prunable_nonempty(tmpdir: Path) -> None:
    backup = write_valid_backup(tmpdir, has_copied_files=True, has_data=True)

    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=False))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=True))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=False))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=True))


def test_is_backup_prunable_empty(tmpdir: Path) -> None:
    backup = write_valid_backup(
        tmpdir, has_copied_files=False, has_removed_files=False, has_removed_directories=False, has_data=False)

    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=False))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=True))
    assert is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=False))
    assert is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=True))


def test_is_backup_prunable_removed_only(tmpdir: Path) -> None:
    backup = write_valid_backup(
        tmpdir, has_copied_files=False, has_removed_files=True, has_removed_directories=True, has_data=False)

    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=False))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=True))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=False))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=True))


def test_is_backup_prunable_other_data(tmpdir: Path) -> None:
    backup = write_valid_backup(
        tmpdir, has_copied_files=False, has_removed_files=False, has_removed_directories=False, has_data=False)
    (tmpdir / 'my_quirky_data.abc').write_text('yes')

    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=False))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=False, prune_other_data=True))
    assert not is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=False))
    assert is_backup_prunable(tmpdir, backup, BackupPrunabilityOptions(prune_empty=True, prune_other_data=True))


# TODO: test prune_backups
