# Incremental Backup Tool - Restore Command

This command is used to restore files previously backed up with the `backup` command (see [BackupUsage.md](BackupUsage.md)).

## Usage

```
python3 -m incremental_backup restore <backup_target_dir> <destination_dir> [<backup_or_time>]
```

`<backup_target_dir>` - The path of the directory containing the backups to restore.
This corresponds to the `target_dir` argument of the `backup` command.

`<destination_dir>` - The path of the directory to restore data into.
It must either not exist, or be an empty directory.

`<backup_or_time>` - The name or ISO 8601 timestamp of the latest backup to be included when restoring.
If this is a backup name, then all backups up to and including that backup are included.
If this is a timestamp, then all backups whose creation time are less than or equal to that time are included.

## Theory of Operation

This command amalgamates existing incremental backups to reconstruct the latest state of the backed-up filesystem into a specified location.
The `backup_or_time` argument can optionally be used to reconstruct the state of the filesystem at an earlier point in time.

## Error Handling

Since this command performs many file I/O operations, there are many opportunities for unpredictable errors to occur.
In general, this command tries to restore as many files as possible, even when errors occur.  

Here are some of the most common nonfatal error cases and how they are handled:

- A backup can't be read or is invalid. It will be excluded.
- A directory can't be created in the destination directory. All files which would have been restored into it will be skipped.
- A file can't be copied to the destination directory. It will be skipped.

These nonfatal errors will produce a warning on the console and the backup operation will continue.

Here are some of the fatal error cases:

- The backup directory can't be read at all (i.e. the path doesn't exist or isn't accessible).
- The destination directory can't be created.

In the worst error case, the restore operation will just fail to restore some files.
In particular, the backup data will not be modified under any circumstances.
