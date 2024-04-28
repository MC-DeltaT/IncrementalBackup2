# Incremental Backup Tool

**An easy-to-use, no fluff incremental file backup application.**

## Features

- **Efficient backup** - copies and stores only changed files.
- **Simple backup format** - data is stored as plain files, metadata is JSON.
- **No configuration** - all the tool needs to know is where the data is, and where to put the backups.
- **Focus on failure** - designed with failure in mind to keep your data intact.
- **CLI and API usage**

## Requirements

- Windows or Linux system
- Python 3.9 or newer

## Usage

**Create a backup:**

```
python -m incremental_backup backup /path/to/back/up /safe/backup/location
```

This will back up files from `/path/to/back/up` into `/safe/backup/location`.

Run the same command again to do subsequent backups - it's that easy! Backups in `/safe/backup/location` will be automatically read to determine what needs to be copied.

For details, see [docs/BackupUsage.md](./docs/BackupUsage.md).

**Restore files:**

```
python -m incremental_backup restore /safe/backup/location /restore/to/here
```

This restores the backed up files from `/safe/backup/location` into `/restore/to/here`.

For details, see [docs/RestoreUsage.md](./docs/RestoreUsage.md).

## Disclaimer

This application is intended for low-risk personal use.
I have genuinely tried to make it as robust as possible, but if you use this software and lose all your data as a result, that's not my responsibility.

## Development and Contributing

For details on developing or otherwise contributing to the project, please see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Background

Unlike Linux, which has awesome tools like rsync, Windows does not seem to have a good selection of free backup tools.
There is the Windows system image backup, but that does full backups only. There is also File History, but that is janky and largely opaque.  
Thus, I created this tool, and aimed for it to be as simple, open, and robust as possible.

This project is a successor to my initial attempts at an incremental backup tool for Windows, available [here](https://github.com/MC-DeltaT/IncrementalBackup).  
Please see [docs/ChangesFromOriginal.md](./docs/ChangesFromOriginal.md) for details.  
If you have used the original incremental backup tool, please note that this version is **NOT backwards compatible** with the original.
