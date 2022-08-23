# Incremental Backup Tool - Backup Command

This command is used to create new incremental backups.

## Usage

```
python3 incremental_backup.py backup <source_dir> <target_dir> [--exclude <exclude_pattern1> [<exclude_pattern2> ...]]
```

`<source_dir>` - The path of the directory to be backed up.

`<target_dir>` - The path of the directory to back up to. It need not exist.  
It's highly recommended that `<target_dir>` is not contained within `<source_dir>` (except if you explicitly exclude `<target_dir>`).

`<exclude_pattern>` - Regular expressions to match paths in the source directory that will be excluded from the backup.
Please see the _Path Exclude Patterns_ section for details.

## Theory of Operation

The premise of this command is for it to be run regularly with the same source and target directories.
Backups of the source directory will be accumulated in the target directory, which are read during the next backup to determine which files to copy (hence "incremental" backup).
This model allows use of the application without any installation or data stored elsewhere on the system.  
Note that different source directories should use different target directories, otherwise the backups will interfere with each other (this is different to the behaviour of the original incremental backup tool).

Determining if a file should be copied is based on the last write time metadata.
The program will check for the latest previous backup which contains the file.
If the file has been modified since that backup, the file is copied, otherwise it is not copied. (If there are no previous backups, all files are copied.)  
Note that if you change files' last write times, or mess with the system clock, this application may not work as expected.

Please see [BackupFormat.md](BackupFormat.md) for specific technical information on how the backups are stored.

## Path Exclude Patterns

These are patterns which can match files and directories in the backup source directory to exclude them from a backup.  
If any pattern fully matches a file, that file is ignored. If any pattern fully matches a directory, that directory and all of its descendents are ignored.  
Each pattern is a regular expression, as specified by the Python `re` module, compiled with the `re.DOTALL` flag. Matching is done with `re.fullmatch()`.  

The patterns are matched against source directory paths, represented in a specific format.
This format uses POSIX-style absolute paths, with the root directory representing the backup source directory.  
Paths for directories always end in a directory separator, i.e. forward slash (`/`). Paths for files never end in a directory separator.  
Additionally, path components are case-normalised with Python's `os.path.normcase()`, which converts to lowercase on Windows (on other operating systems, path components are unchanged).  
For example, with the backup source directory `C:\Users\Jade`:

- The directory `C:\Users\Jade` is represented as `/`.
- The directory `C:\Users\Jade\Desktop` is represented as `/desktop/`.
- The file `C:\Users\Jade\Desktop\cat.jpg` is represented as `/desktop/cat.jpg`.

Note that if a previous backup included a file/directory which is then marked as excluded in a later backup, that file/directory will count as "removed" in the later backup.
The effect of this is that if a restore operation is then performed, the excluded file/directory will not be restored.

### Examples

Exclude the directory `$RECYCLE.BIN` in the backup source directory: `/\$recycle\.bin/`  
Exclude all files with the `.bin` extension: `.*\.bin`  
Exclude all Git directories: `.*/\.git/`

## Error Handling

Since this command performs many file I/O operations, there are many opportunities for unpredictable errors to occur.
In general, this command tries to back up as much of the source directory as possible, even when errors occur.  

Here are some of the most common nonfatal error cases and how they are handled:

- A directory or file in the source directory can't be read. It will be skipped.
- A directory can't be created in the backup directory. All files which would have been backed up into it will be skipped.
- A file can't be copied to the backup directory. It will be skipped.

These nonfatal errors will produce a warning on the console and the backup operation will continue.

Here are some of the fatal error cases:

- The source directory can't be read at all (i.e. the path doesn't exist or isn't accessible).
- The target directory exists but can't be read at all.
- The backup directory can't be created.
- The backup start information file can't be written.
- The backup manifest file can't be written.

The command is designed to fail securely.
The backup manifest is written to file last, and without a valid manifest a backup will not be considered during future backup operations.
Therefore, at worst, files will be copied but the backup won't be used by this application.
