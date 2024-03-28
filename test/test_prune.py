from pathlib import Path
from typing import Any

import pytest

from incremental_backup.meta.meta import ReadBackupsCallbacks
from incremental_backup.prune import BackupPrunabilityOptions, is_backup_prunable, prune_backups, PruneBackupsCallbacks, \
    PruneBackupsConfig, PruneBackupsError

from helpers import AssertFilesystemUnmodified, write_valid_backup


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


def test_prune_backups_nonexistent_target(tmpdir: Path) -> None:
    # Backup target directory doesn't exist.

    target_dir = tmpdir / 'backups'

    config = PruneBackupsConfig(False, BackupPrunabilityOptions(True, False))

    actual_callbacks: list[Any] = []

    callbacks = PruneBackupsCallbacks(
        on_before_read_backups=lambda: actual_callbacks.append('on_before_read_backups'),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f'Unexpected on_query_entry_error: {path=} {error=}'),
            on_read_metadata_error=lambda path, error:
                pytest.fail(f'Unexpected on_read_metadata_error: {path=} {error=}')
        ),
        on_after_read_backups=lambda backups: pytest.fail(f'Unexpected on_after_read_backups: {backups=}'),
        on_selected_backups=lambda backups: pytest.fail(f'Unexpected on_selected_backups: {backups=}'),
        on_delete_error=lambda path, error: pytest.fail(f'Unexpected on_delete_error: {path=} {error=}'),
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(PruneBackupsError):
            prune_backups(target_dir, config, callbacks)
    
    assert actual_callbacks == [
        'on_before_read_backups'
    ]


def test_prune_backups_invalid_backup(tmpdir: Path) -> None:
    # Some backups have invalid metadata.

    # TODO
    pytest.fail()


def test_prune_backups_delete_empty(tmpdir: Path) -> None:
    # Normal behaviour of deleting empty backups.

    # TODO
    pytest.fail()
