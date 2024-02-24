from collections.abc import Sequence

from incremental_backup.cli.command.command import Command
from incremental_backup.cli.command.backup import BackupCommand
from incremental_backup.cli.command.restore import RestoreCommand


__all__ = [
    'COMMAND_CLASSES',
    'get_command_class'
]


COMMAND_CLASSES: Sequence[type[Command]] = (
    BackupCommand,
    RestoreCommand
)
"""List of all commands recognised by the program.
    Add or remove commands here.
"""


def get_command_class(command_string: str, /) -> type[Command]:
    """Obtains a command class from its command line string.
    
        :except ValueError: If the command is not found.
    """

    for command in COMMAND_CLASSES:
        if command.COMMAND_STRING == command_string:
            return command
    raise ValueError('Command not found')
