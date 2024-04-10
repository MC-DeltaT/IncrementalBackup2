from pathlib import Path
from typing import Any

import pytest

from incremental_backup.meta.meta import ReadBackupsCallbacks
from incremental_backup.prune import (
    BackupPrunabilityOptions,
    PruneBackupsCallbacks,
    PruneBackupsConfig,
    PruneBackupsError,
    PruneBackupsResults,
    is_backup_prunable,
    prune_backups,
)

from test.helpers import AssertFilesystemUnmodified, MakeBackup, unordered_equal


def test_is_backup_prunable_nonempty(tmpdir: Path) -> None:
    backup_path, backup_metadata = MakeBackup.valid(copied_files=True)(tmpdir)

    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=False),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=True),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=False),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=True),
    )


def test_is_backup_prunable_empty(tmpdir: Path) -> None:
    backup_path, backup_metadata = MakeBackup.empty()(tmpdir)

    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=False),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=True),
    )
    assert is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=False),
    )
    assert is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=True),
    )


def test_is_backup_prunable_removed_only(tmpdir: Path) -> None:
    backup_path, backup_metadata = MakeBackup.valid(copied_files=False, removed_files=True, removed_directories=True)(
        tmpdir
    )

    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=False),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=True),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=False),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=True),
    )


def test_is_backup_prunable_other_data(tmpdir: Path) -> None:
    backup_path, backup_metadata = MakeBackup.empty()(tmpdir)
    (backup_path / "my_quirky_data.abc").write_text("yes")

    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=False),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=False, prune_other_data=True),
    )
    assert not is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=False),
    )
    assert is_backup_prunable(
        backup_path,
        backup_metadata,
        BackupPrunabilityOptions(prune_empty=True, prune_other_data=True),
    )


def test_prune_backups_nonexistent_target(tmpdir: Path) -> None:
    # Backup target directory doesn't exist.

    target_dir = tmpdir / "backups"

    config = PruneBackupsConfig(False, BackupPrunabilityOptions(True, False))

    actual_callbacks: list[Any] = []
    callbacks = PruneBackupsCallbacks(
        on_before_read_backups=lambda: actual_callbacks.append("on_before_read_backups"),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f"Unexpected on_query_entry_error: {path=} {error=}"),
            on_read_metadata_error=lambda path, error: pytest.fail(
                f"Unexpected on_read_metadata_error: {path=} {error=}"
            ),
        ),
        on_after_read_backups=lambda backups: pytest.fail(f"Unexpected on_after_read_backups: {backups=}"),
        on_selected_backups=lambda backups: pytest.fail(f"Unexpected on_selected_backups: {backups=}"),
        on_delete_error=lambda path, error: pytest.fail(f"Unexpected on_delete_error: {path=} {error=}"),
    )

    with AssertFilesystemUnmodified(tmpdir):
        with pytest.raises(PruneBackupsError):
            prune_backups(target_dir, config, callbacks)

    assert actual_callbacks == ["on_before_read_backups"]


def test_prune_backups_invalid_backup(tmpdir: Path) -> None:
    # Some backups have invalid metadata.

    backup1_path, backup1_metadata = MakeBackup.empty()(tmpdir)
    backup2_path, backup2_metadata = MakeBackup.valid()(tmpdir)
    backup3_path, _ = MakeBackup.invalid()(tmpdir)

    config = PruneBackupsConfig(False, BackupPrunabilityOptions(True, False))

    # Note that since the backup is invalid, it will just be ignored without triggering any error handling.

    actual_callbacks: list[Any] = []
    callbacks = PruneBackupsCallbacks(
        on_before_read_backups=lambda: actual_callbacks.append("on_before_read_backups"),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f"Unexpected on_query_entry_error: {path=} {error=}"),
            on_read_metadata_error=lambda path, error: pytest.fail(
                f"Unexpected on_read_metadata_error: {path=} {error=}"
            ),
        ),
        on_after_read_backups=lambda backups: actual_callbacks.append(("on_after_read_backups", backups)),
        on_selected_backups=lambda backups: actual_callbacks.append(("on_selected_backups", backups)),
        on_delete_error=lambda path, error: pytest.fail(f"Unexpected on_delete_error: {path=} {error=}"),
    )

    with AssertFilesystemUnmodified(backup2_path, backup3_path):
        results = prune_backups(tmpdir, config, callbacks)

    assert not backup1_path.exists()

    assert results == PruneBackupsResults(empty_backups_removed=1, total_backups_removed=1, backups_remaining=1)

    assert actual_callbacks[0] == "on_before_read_backups"
    assert actual_callbacks[1][0] == "on_after_read_backups"
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal(
        (backup1_metadata.name, backup2_metadata.name),
        [backup.name for backup in actual_callbacks[1][1]],
    )
    assert actual_callbacks[2][0] == "on_selected_backups"
    assert [backup1_metadata.name] == [backup.name for backup in actual_callbacks[2][1]]


def test_prune_backups_delete_empty(tmpdir: Path) -> None:
    # Normal behaviour of deleting empty backups.

    backup1_path, backup1_metadata = MakeBackup.empty()(tmpdir)
    backup2_path, backup2_metadata = MakeBackup.valid()(tmpdir)
    backup3_path, backup3_metadata = MakeBackup.empty()(tmpdir)

    config = PruneBackupsConfig(False, BackupPrunabilityOptions(True, False))

    actual_callbacks: list[Any] = []
    callbacks = PruneBackupsCallbacks(
        on_before_read_backups=lambda: actual_callbacks.append("on_before_read_backups"),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f"Unexpected on_query_entry_error: {path=} {error=}"),
            on_read_metadata_error=lambda path, error: pytest.fail(
                f"Unexpected on_read_metadata_error: {path=} {error=}"
            ),
        ),
        on_after_read_backups=lambda backups: actual_callbacks.append(("on_after_read_backups", backups)),
        on_selected_backups=lambda backups: actual_callbacks.append(("on_selected_backups", backups)),
        on_delete_error=lambda path, error: pytest.fail(f"Unexpected on_delete_error: {path=} {error=}"),
    )

    with AssertFilesystemUnmodified(backup2_path):
        results = prune_backups(tmpdir, config, callbacks)

    assert not backup1_path.exists()
    assert not backup3_path.exists()

    assert results == PruneBackupsResults(empty_backups_removed=2, total_backups_removed=2, backups_remaining=1)

    assert actual_callbacks[0] == "on_before_read_backups"
    assert actual_callbacks[1][0] == "on_after_read_backups"
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal(
        (backup1_metadata.name, backup2_metadata.name, backup3_metadata.name),
        [backup.name for backup in actual_callbacks[1][1]],
    )
    assert actual_callbacks[2][0] == "on_selected_backups"
    assert unordered_equal(
        (backup1_metadata.name, backup3_metadata.name),
        [backup.name for backup in actual_callbacks[2][1]],
    )


def test_prune_backups_dry_run(tmpdir: Path) -> None:
    # Normal behaviour of deleting empty backups.

    _, backup1_metadata = MakeBackup.empty()(tmpdir)
    _, backup2_metadata = MakeBackup.valid()(tmpdir)
    _, backup3_metadata = MakeBackup.empty()(tmpdir)

    config = PruneBackupsConfig(True, BackupPrunabilityOptions(True, False))

    actual_callbacks: list[Any] = []
    callbacks = PruneBackupsCallbacks(
        on_before_read_backups=lambda: actual_callbacks.append("on_before_read_backups"),
        read_backups=ReadBackupsCallbacks(
            on_query_entry_error=lambda path, error: pytest.fail(f"Unexpected on_query_entry_error: {path=} {error=}"),
            on_read_metadata_error=lambda path, error: pytest.fail(
                f"Unexpected on_read_metadata_error: {path=} {error=}"
            ),
        ),
        on_after_read_backups=lambda backups: actual_callbacks.append(("on_after_read_backups", backups)),
        on_selected_backups=lambda backups: actual_callbacks.append(("on_selected_backups", backups)),
        on_delete_error=lambda path, error: pytest.fail(f"Unexpected on_delete_error: {path=} {error=}"),
    )

    with AssertFilesystemUnmodified(tmpdir):
        results = prune_backups(tmpdir, config, callbacks)

    assert results == PruneBackupsResults(empty_backups_removed=2, total_backups_removed=2, backups_remaining=1)

    assert actual_callbacks[0] == "on_before_read_backups"
    assert actual_callbacks[1][0] == "on_after_read_backups"
    # I can't be bothered testing that all the metadata is the same, I assume it is otherwise other things will likely
    # break anyway
    assert unordered_equal(
        (backup1_metadata.name, backup2_metadata.name, backup3_metadata.name),
        [backup.name for backup in actual_callbacks[1][1]],
    )
    assert actual_callbacks[2][0] == "on_selected_backups"
    assert unordered_equal(
        (backup1_metadata.name, backup3_metadata.name),
        [backup.name for backup in actual_callbacks[2][1]],
    )
