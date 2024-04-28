# Incremental Backup Tool - Changes from the Original Incremental Backup

This project is designed to be an improved replacement for the original incremental backup tool available [here](https://github.com/MC-DeltaT/IncrementalBackup).

Rather than try to refactor the mess of code in the original, I thought it would be easier to start over from scratch.
This allowed significant changes to be made where necessary to achieve my improvement goals.  
As a result of the changes, this version is **not backwards compatible** with the original version.
If you try to use this version with backup data from the original version, or vice-versa, it will not work.

## Goals

My aims with this version, compared to the original, are:

- more efficient backup data storage
- simpler metadata structure and formats
- more readable and maintainable code
- test code
- better library API for integration into user code

## Major Changes

This version is written in Python, as it is a bit more flexible than C#.
Among many other improvements, this allows the application to easily run on Linux too.

In general, I have put more effort into writing more readable and maintainable code.
Function and class interfaces are cleaner, and error handling is improved. Almost all functionality could be reused and integration into one's own application. Test code actually exists and has decent coverage.

Backup target directories are now assumed to be for a single backup source directory only: it is up to the user to back up different source directories to different target directories.  
This simplifies the management of backups, allowing elimination of the index file. Comparing source directories was already a bit dodgy, and could easily fail for aliased paths.

The backup manifest file is a new JSON format and is no longer written incrementally. In the original version, I justified the use of a custom line-based file format with lower memory requirements and greater robustness.  
However, the lower memory usage doesn't make sense, because when reading previous backups, the entire manifest must be stored in memory anyway.  
And I don't think Windows nor Linux guarantee each line write to be an atomic operation (although I suspect this may be the case for small writes), so there was always a chance the backup manifest could be corrupted, even writing it line-by-line.

Once I accepted the idea of allowing the whole backup source filesystem to be in memory at once, it allowed optimisations of the backup copy operations.  
Now, a "backup plan" is created in advance, which knows exactly what files to copy and what files/directories are removed. The plan can be pruned to improve efficiency of the backup operation.  
Specifically, directories which contain - directly or indirectly - no modified/new files are no longer created in the backup data. This optimisation should significantly reduce the time required to create a backup for large source directory hierarchies.
Previously most of the time spent in a backup operation would be creating empty directories.  
Additionally, the backup plan allows many useless entries to be pruned from the backup manifest.
