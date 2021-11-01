# Incremental Backup Tool - Backup Format Specification

This document specifies the internal structure of backups. That is, what this application stores in the target directory.  
For general backing up of files, you probably don't need to know this information.
If you are looking to extend this tool or integrate it with another system, you probably will need to know.

Note that manually editing the backup data is possible, but not recommended.
It might cause the application to behave in an unexpected manner (although I have tried to make it fail securely).

## Target Directory

The target directory is where the backup command stores all the backup data it creates.

Each backup is fully contained within its own directory.
These directories are named with 16 random lowercase ASCII alphanumeric characters.  
The contents of a backup directory is described in the _Backup Directory_ section.

## Backup Directory

Each backup is contained within a subdirectory of the target directory.
The backup consists of 1 directory for data, and 3 metadata files.

The data directory, named `data`, contains the directories and files copied from the source directory.
The structure of the directories and files are identical to that of the source directory (basically as if you copy and pasted the source directory using File Explorer).
The `data` directory itself represents the source directory (i.e. the contents of the source directory become the contents of the `data` directory).

The 3 metadata files are as follows:

 - `start.json` - contains some startup information. See section _Backup Start Information File_.
 - `manifest.json` - lists the files and directories backed up. See section _Backup Manifest File_.
 - `completion.json` - contains some results of the backup. See section _Backup Completion Information File_.

## Backup Start Information File

Name: `start.json`

This file contains backup metadata concerning the start of the backup operation.

It is a UTF-8-encoded JSON file, consisting of a single object with the following properties:
 - `start_time` \[string\] - The UTC time just before the first file was backed up.
   It is a string in ISO 8601 format (specifically, the format produced by Python's `datetime.isoformat()`).
   This property is used by future backups in conjunction with the backup manifest to determine which files to back up (i.e. if the file has been modified since `start_time`).

This file is created just before beginning to copy files. Every backup considered valid by this application shall have this file.

## Backup Manifest File

Name: `manifest.json`

This file lists all directories and files successfully backed up.

This file's structure is derived from the depth-first search used to explore the backup source directory.
It would be very inefficient to store the full path of every directory and file that was backed up.
Instead, we track the depth-first search, which implicitly stores the path to the current search directory as part of its operation.
Any files recorded are known to be contained within the current search directory.  
This file represents a sequence of visited directories and directory backtracks.  
The current search directory begins as the backup source directory.

It is a UTF-8-encoded JSON file, consisting of a list. Each entry in the list is either an object or a string.

An object entry represents entering a direct subdirectory of the current search directory. The object has the following properties:

 - `n` \[string\] - The name of the directory. This is optional for the backup source directory.
 - `cf` \[list of string\] - A list of names of files directly contained in this directory which were modified or created since the last backup, and thus were copied.
 - `rf` \[list of string\] - A list of names of files directly contained in this directory which were removed since the last backup.
 - `rd` \[list of string\] - A list of names of directories directly contained in this directory which were removed since the last backup.

Each of `cf`, `rf`, and `rd` are only present if they are nonempty, to save space.

A string entry represents backtracking the current search directory to one of its ancestors.  
Such an entry has the format `^n`, where `n` is an integer greater than 0, specifying the number of single backtracks to perform.  
This shall never cause a backtrack past the backup source directory.  
The format of these entries allows multiple backtracks at once while using as little file space as possible.
To save even more space, trailing backtrack entries are not stored in the manifest, as they are not required.

All other directories and files not recorded in the backup manifest are assumed to be unable to be read or unmodified.
There is no need to explicitly store this information.

Every backup considered valid by this application shall have this file.

## Backup Completion Information File

Name: `completion.json`

This file contains backup metadata concerning the end of the backup operation.

It is a UTF-8-encoded JSON file, consisting of a single object with the following properties:
 - `end_time` \[string\] - The UTC time just before the command completes.
   It is a string in ISO 8601 format (specifically, the format produced by Python's `datetime.isoformat()`).
 - `paths_skipped` \[boolean] - Indicates if any directories or files were not backed up due to file I/O errors.
   It does not include paths that were specified by the user to be excluded.

This file will not be present if an error occurred while trying to create it.
It is not critical that this file exists.

At this time, this file is not used by the application.
It is only created because it seems like important information that may be useful later.
