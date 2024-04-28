# Incremental Backup Tool - Prune Command

This command is used to remove unnecessary backups.

## Usage

```
python -m incremental_backup prune <backup_target_dir> [--commit] [--delete_empty]
```

`<backup_target_dir>` - The path of the directory containing the backups to restore.
This corresponds to the `target_dir` argument of the `backup` command.

`--commit` - If specified, delete prunable backups. If not specified, simulate the prune operation without modifying the filesystem.

### Prune modes

`--delete_empty` - If specified, backups without any recorded file changes are pruned.

## Theory of Operation

Over time, you will probably accumulate a large number of backups. This may slow down the backup operation, since reading previous backups takes time. The prune command allows unnecessary backups to be easily and safely deleted.

By default, the prune command does nothing. You must specify one of the prune mode flags (list above) to specify which backups are classified as prunable. When run with the `--commit` flag, all prunable backups will be permanently deleted.

## Error Handling

Since this command performs many file I/O operations, there are many opportunities for unpredictable errors to occur.
In general, this command tries to delete as many valid, prunable backups as possible, even when errors occur.

Some common nonfatal error cases and how they are handled:

- A backup can't be read or is invalid. It will be excluded and never deleted.
- Deleting a backup fails. It will be left as-is - possibly in an invalid state, but this is ok, as other backup operations will ignore it.

These nonfatal errors will produce a warning on the console and the backup operation will continue.

Fatal error cases:

- The backup directory can't be read at all (i.e. the path doesn't exist or isn't accessible).

### Program Exit Codes

- 0 - The operation completed successfully, possibly with some warnings (i.e. nonfatal errors).
- 1 - The command line arguments are invalid.
- 2 - The operation could not be completed due to a fatal runtime error.
- -1 - The operation was aborted due to a programmer error - sorry in advance.
