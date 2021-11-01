# Incremental Backup Tool (MK 2)

by Reece Jones

## Purpose

Unlike Linux, which has awesome tools like rsync, Windows does not seem to have a good selection of free backup tools.
There is the Windows system image backup, but that does full backups only. There is also File History, but that is janky and largely opaque.
Thus, I created this tool. Some of its design goals:

 - free, open source
 - as simple as possible (no installation, no GUI, no fluff)
 - robust
 - fast
 - efficient
 - transparent backup format

This project is a successor to my initial attempts at an incremental backup tool for Windows, available [here](https://github.com/MC-DeltaT/IncrementalBackup).  
Please see `ChangesFromOriginal.md` for details.  
If you have used the original incremental backup tool, please note that this version is **NOT backwards compatible** with the original.

At this time, there is the ability to back up files, but no ability to restore them.
I will probably implement this feature soon.
In the meantime, you can use the provided APIs to implement this yourself - it would be quite easy.

## Disclaimer

This application is intended for low-risk personal use.
I have genuinely tried to make it as robust as possible, but if you use this software and lose all your data as a result, that's not my responsibility.

## Requirements

 - Windows or Linux system
 - Python 3.8 or newer
 - \[Only for testing\] Pytest (any recent version should do)

## Application Structure

Important files and directories:

 - `incremental_backup.py` - The command line script.
 - `incremental_backup/` - Library components.
   - `backup/` - High-level backup functionality.
   - `commands/` - Entrypoints for the command line commands (see also the _Usage_ section).
   - `meta/` - Functionality related to backup metadata and structure.
   - `utility/` - Miscellaneous helper functionality.
   - `main.py` - program entrypoint.
 - `test/` - Test code. Each directory/file corresponds to the module in `incremental_backup/` it tests.
 
The `incremental_backup/` directory is a Python package which contains almost all application functionality and can be easily used in a library-like manner.

## Usage

The command line interface of the application is a single Python script with multiple "commands" for different functionality.
Usage of the application is as follows:

```
python3 incremental_backup.py <command> <command_args>
```

(You may have to adjust the Python command based on your system configuration.)

Commands:

 - `backup` - Creates a new backup. See `BackupUsage.md` for details.

To start using this application, you probably want to have a look at `BackupUsage.md`.

### Program Exit Codes

 - 0 - The operation completed successfully, possibly with some warnings (i.e. nonfatal errors).
 - 1 - The command line arguments are invalid.
 - 2 - The operation could not be completed due to a runtime error (typically would be a file I/O error).
 - -1 - The operation was aborted due to a programmer error - sorry in advance.

## Running Tests

With your working directory as the project root directory:

```
python3 -m pytest ./
```

This runs a suite of unit and integration tests.
