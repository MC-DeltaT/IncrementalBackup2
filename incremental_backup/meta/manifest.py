from dataclasses import dataclass, field
import json
from os import PathLike
from typing import Dict, Iterator, List, NoReturn, Tuple, Union

from ..utility import path_name_equal


__all__ = [
    'BackupManifest',
    'BackupManifestParseError',
    'read_backup_manifest',
    'write_backup_manifest'
]


@dataclass
class BackupManifest:
    """Lists the files and directories copied and removed (compared to the previous backup).
        The data is represented in a tree structure like a filesystem.
    """

    @dataclass
    class Directory:
        name: str
        copied_files: List[str] = field(default_factory=list)
        removed_files: List[str] = field(default_factory=list)
        removed_directories: List[str] = field(default_factory=list)
        subdirectories: List['BackupManifest.Directory'] = field(default_factory=list)

    root: Directory = field(default_factory=lambda: BackupManifest.Directory(''))
    """The root of the manifest tree. This object represents the backup source directory."""


def write_backup_manifest(path: PathLike, value: BackupManifest, /) -> None:
    """Writes a backup manifest to file.

        :except OSError: If the file could not be written to.
    """

    def search(manifest: BackupManifest, /) -> Iterator[Union[BackupManifest.Directory, None]]:
        stack: List[Union[BackupManifest.Directory, None]] = [manifest.root]
        while stack:
            node = stack.pop()
            yield node
            if node is not None:
                stack.append(None)
                stack.extend(reversed(node.subdirectories))

    def compress_backtracks(nodes: Iterator[Union[BackupManifest.Directory, None]], /) \
            -> Iterator[Union[BackupManifest.Directory, int]]:
        backtrack_count = 0
        for node in nodes:
            if node is None:
                backtrack_count += 1
            else:
                if backtrack_count > 0:
                    yield backtrack_count
                    backtrack_count = 0
                yield node
        # Also trims trailing backtracks since they are not required.

    def node_to_object(node: Union[BackupManifest.Directory, int], /) -> Union[Dict[str, str], str]:
        if isinstance(node, int):
            return f'^{node}'
        else:
            obj = {'n': node.name}
            if node.copied_files:
                obj['cf'] = node.copied_files
            if node.removed_files:
                obj['rf'] = node.removed_files
            if node.removed_directories:
                obj['rd'] = node.removed_directories
            return obj

    nodes = list(compress_backtracks(search(value)))
    json_data = [node_to_object(node) for node in nodes]

    with open(path, 'w', encoding='utf8') as file:
        json.dump(json_data, file, indent=0, ensure_ascii=False)


def read_backup_manifest(path: PathLike, /) -> BackupManifest:
    """Reads a backup manifest from file.

        :except OSError: If the file could not be read.
        :except BackupManifestParseError: If the file is not a valid backup manifest.
    """

    def parse_error(reason: str, /) -> NoReturn:
        raise BackupManifestParseError(str(path), reason)

    def parse_error_from(reason: str, e: Exception, /) -> NoReturn:
        raise BackupManifestParseError(str(path), reason) from e

    def parse_directory_entry(entry: dict, entry_num: int, /) -> Tuple[str, List[str], List[str], List[str]]:
        try:
            name = entry.pop('n')
        except KeyError as e:
            # Can allow the name to be missing for the source directory, it's not used anyway.
            if entry_num == 1:
                name = ''
            else:
                parse_error_from(f'Entry {entry_num}: missing required field "n"', e)
        if not isinstance(name, str):
            parse_error(f'Entry {entry_num}: field "n" must be a string')

        copied_files = entry.pop('cf', [])
        if not isinstance(copied_files, list) or not all(isinstance(f, str) for f in copied_files):
            parse_error(f'Entry {entry_num}: field "cf" must be a list of strings')

        removed_files = entry.pop('rf', [])
        if not isinstance(removed_files, list) or not all(isinstance(f, str) for f in removed_files):
            parse_error(f'Entry {entry_num}: field "rf" must be a list of strings')

        removed_directories = entry.pop('rd', [])
        if not isinstance(removed_directories, list) or not all(isinstance(f, str) for f in removed_directories):
            parse_error(f'Entry {entry_num}: field "rd" must be a list of strings')

        if extra_fields := list(entry.keys()):
            parse_error(f'Entry {entry_num}: invalid fields {extra_fields}')

        return name, copied_files, removed_files, removed_directories

    def parse_backtrack(entry: str, entry_num: int, /) -> int:
        if not entry.startswith('^'):
            parse_error(f'Entry: {entry_num}: invalid value, backtrack must be in form "^n"')

        try:
            backtracks = int(entry[1:])
        except ValueError:
            pass
        else:
            if backtracks >= 1:
                return backtracks
        parse_error(f'Entry {entry_num}: invalid backtrack amount, must be positive integer')

    try:
        with open(path, 'r', encoding='utf8') as file:
            json_data = json.load(file)
    except json.JSONDecodeError as e:
        parse_error_from(str(e), e)

    if not isinstance(json_data, list):
        parse_error('Expected a list')

    backup_manifest = BackupManifest()
    directory_stack = []
    for entry_num, entry in enumerate(json_data, 1):
        if isinstance(entry, str):
            backtracks = parse_backtrack(entry, entry_num)

            # Backtrack to parent directory.
            if len(directory_stack) <= backtracks:
                parse_error(f'Entry {entry_num}: cannot backtrack past backup source directory')
            del directory_stack[-backtracks:]
        elif isinstance(entry, dict):
            # Directory entry.

            name, copied_files, removed_files, removed_directories = parse_directory_entry(entry, entry_num)

            if entry_num == 1:
                # Root directory. Unfortunately we need to handle this case differently, a bit inelegant...
                directory = backup_manifest.root
                directory.copied_files = copied_files
                directory.removed_files = removed_files
                directory.removed_directories = removed_directories
            else:
                # Not root directory.

                # We explicitly allow re-entering a directory. It shouldn't occur in practice, though.
                directory = next((d for d in directory_stack[-1].subdirectories if path_name_equal(d.name, name)), None)
                if directory is None:
                    # We haven't entered this directory yet, need to create it.
                    directory = BackupManifest.Directory(name, copied_files, removed_files, removed_directories)
                    directory_stack[-1].subdirectories.append(directory)
                else:
                    # Already entered this directory, need to update it.

                    # Technically we should check if these have already been added, but I don't think it will cause any
                    # issues, and checking would cost performance.
                    directory.copied_files.extend(copied_files)
                    directory.removed_files.extend(removed_files)
                    directory.removed_directories.extend(removed_directories)
            directory_stack.append(directory)
        else:
            parse_error(f'Entry {entry_num}: invalid value, expected object or string')

    return backup_manifest


class BackupManifestParseError(Exception):
    """Raised when a backup manifest file cannot be parsed due to invalid format."""

    def __init__(self, file_path: str, reason: str) -> None:
        super().__init__(f'Failed to parse backup manifest file "{file_path}": {reason}')
        self.file_path = file_path
        self.reason = reason
