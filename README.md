# Incremental Backup Tool (MK 2)

by Reece Jones

## Purpose

Unlike Linux, which has awesome tools like rsync, Windows does not seem to have a good selection of free backup tools.
There is the Windows system image backup, but that does full backups only. There is also File History, but that is janky and largely opaque.

Thus, I created this tool. Some of its design goals are:

- free, open source
- as simple as possible (no installation, no GUI, no fluff)
- robust
- fast
- efficient
- transparent backup format

This project is a successor to my initial attempts at an incremental backup tool for Windows, available [here](https://github.com/MC-DeltaT/IncrementalBackup).  
Please see [ChangesFromOriginal.md](ChangesFromOriginal.md) for details.  
If you have used the original incremental backup tool, please note that this version is **NOT backwards compatible** with the original.

## Disclaimer

This application is intended for low-risk personal use.
I have genuinely tried to make it as robust as possible, but if you use this software and lose all your data as a result, that's not my responsibility.

## Requirements

General usage:

- Windows or Linux system
- Python 3.9 or newer

## Usage

The Python package is executable via a command line interface.
The interface uses multiple "commands" for different functionality.
Usage is as follows:

```
python3 -m incremental_backup <command> <command_args>
```

(You may have to adjust the Python command based on your system configuration.)

Commands:

- `backup` - Creates a new backup. See [BackupUsage.md](BackupUsage.md) for details.
- `restore` - Restores files from backups. See [RestoreUsage.md](RestoreUsage.md) for details.

To start using this application, you probably want to have a look at [BackupUsage.md](BackupUsage.md).

### Program Exit Codes

- 0 - The operation completed successfully, possibly with some warnings (i.e. nonfatal errors).
- 1 - The command line arguments are invalid.
- 2 - The operation could not be completed due to a runtime error (typically would be a file I/O error).
- -1 - The operation was aborted due to a programmer error - sorry in advance.
