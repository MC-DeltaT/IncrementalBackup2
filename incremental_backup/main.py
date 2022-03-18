import argparse
import sys
from typing import Mapping, NoReturn, Sequence

from .commands import BackupCommand, Command, CommandArgumentError, CommandError, RestoreCommand
from .utility import print_error


__all__ = [
    'api_entrypoint',
    'script_entrypoint',
    'script_main'
]


def script_entrypoint() -> NoReturn:
    """Process-level entrypoint of the incremental backup program.
        Collects arguments from `sys.argv`, performs all processing, then terminates the process with `sys.exit()`."""

    exit_code = script_main(sys.argv)
    sys.exit(exit_code)


def script_main(arguments: Sequence[str], /) -> int:
    """Intermediate entrypoint function which may be handy for testing purposes.

        :param arguments: The program command line arguments.
        :return: Process exit code.
    """

    # Strip off the "program name" argument.
    arguments = arguments[1:]

    try:
        api_entrypoint(arguments)
        return EXIT_CODE_SUCCESS
    except CommandArgumentError as e:
        print(e.usage, file=sys.stderr)
        print(e.message, file=sys.stderr)
        return EXIT_CODE_INVALID_ARGUMENTS
    except CommandError as e:
        print_error(str(e))
        return EXIT_CODE_GENERAL_ERROR
    except Exception as e:
        print_error(f'Unhandled exception: {repr(e)}')
        return EXIT_CODE_LOGIC_ERROR


def api_entrypoint(arguments: Sequence[str], /) -> None:
    """API-level entrypoint of the incremental backup program.

        :param arguments: The program command line arguments. Should not include the "program name" zeroth argument.
        :except CommandArgumentError: If the command line arguments are invalid.
        :except CommandError: If some other fatal error occurs.
    """

    arg_parser = get_argument_parser()

    parsed_arguments = arg_parser.parse_args(arguments)

    command_class = COMMAND_CLASS_MAP[parsed_arguments.command]
    command_instance = command_class(parsed_arguments)
    command_instance.run()


def get_argument_parser() -> argparse.ArgumentParser:
    """Creates the command line argument parser. Adds subparsers for each command."""

    arg_parser = ArgumentParser('incremental_backup.py', description='Incremental backup tool.')
    arg_subparser = arg_parser.add_subparsers(title='commands', required=True, dest='command')

    for cls in COMMAND_CLASSES:
        cls.add_arg_subparser(arg_subparser)

    return arg_parser


class ArgumentParser(argparse.ArgumentParser):
    """Custom `argparse.ArgumentParser` implementation so we can throw exceptions for invalid arguments instead of
        exiting the process."""

    def error(self, message: str) -> NoReturn:
        full_message = f'{self.prog}: error: {message}'     # Same as base ArgumentParser
        raise CommandArgumentError(full_message, self.format_usage())


COMMAND_CLASSES: Sequence[type[Command]] = (
    BackupCommand,
    RestoreCommand
)


COMMAND_CLASS_MAP: Mapping[str, type[Command]] = {
    cls.COMMAND_STRING: cls for cls in COMMAND_CLASSES
}
"""Maps from a command's command line string to its class."""


# Process exit codes.
EXIT_CODE_SUCCESS = 0
EXIT_CODE_INVALID_ARGUMENTS = 1
EXIT_CODE_GENERAL_ERROR = 2
EXIT_CODE_LOGIC_ERROR = -1
