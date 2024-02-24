import argparse
from typing import ClassVar, Protocol


__all__ = [
    'Command'
]


class Command(Protocol):
    """Contains the functionality for one "command" of the incremental backup tool.
        A command is a specific mode of operation, i.e. backup, restore, etc. The principle is the same as Git commands,
        e.g. "git add", "git commit".
    """

    COMMAND_STRING: ClassVar[str]
    """Name of the command as specified in the command line arguments."""

    @staticmethod
    def add_arg_subparser(subparser, /) -> None:
        """Adds the argparse subparser for the command."""

        raise NotImplementedError()

    def __init__(self, arguments: argparse.Namespace, /) -> None:
        """
            :param arguments: The parsed command line arguments object acquired from argparse.
        """

    def run(self) -> None:
        """Executes the command."""

        raise NotImplementedError()
