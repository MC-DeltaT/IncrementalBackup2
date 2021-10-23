import argparse
import sys
from typing import NoReturn, Sequence

from .commands import backup as backup_command
from .exception import FatalArgumentError, FatalError
from .utility import print_error


__all__ = [
    'api_entrypoint',
    'script_entrypoint',
    'script_main'
]


COMMAND_MODULES = (
    backup_command,
)


COMMAND_ENTRYPOINT_MAP = {
    module.COMMAND_NAME: module.entrypoint for module in COMMAND_MODULES
}
"""Maps from the name of a command to its entrypoint function."""


# Process exit codes.
EXIT_CODE_SUCCESS = 0
EXIT_CODE_INVALID_ARGUMENTS = 1
EXIT_CODE_GENERAL_ERROR = 2
EXIT_CODE_LOGIC_ERROR = -1


def script_entrypoint() -> NoReturn:
    """Process-level entrypoint of the incremental backup program.
        Collects arguments from `sys.argv`, performs all processing, then terminates the process with `sys.exit()`."""

    exit_code = script_main(sys.argv)
    sys.exit(exit_code)


def script_main(arguments: Sequence[str], /) -> int:
    """Intermediate entrypoint function which is handy for testing purposes.

        :param arguments: The program command line arguments.
        :return: Process exit code.
    """

    # Strip off the "program name" argument.
    arguments = arguments[1:]

    try:
        api_entrypoint(arguments)
        return EXIT_CODE_SUCCESS
    except FatalArgumentError as e:
        print(e.usage, file=sys.stderr)
        print(e.message, file=sys.stderr)
        return EXIT_CODE_INVALID_ARGUMENTS
    except FatalError as e:
        print_error(str(e))
        return EXIT_CODE_GENERAL_ERROR
    except Exception as e:
        print_error(f'Unhandled exception: {repr(e)}')
        return EXIT_CODE_LOGIC_ERROR


def api_entrypoint(arguments: Sequence[str], /) -> None:
    """API-level entrypoint of the incremental backup program.

        :param arguments: The program command line arguments. Should not include the "program name" zeroth argument.
        :except FatalArgumentError: If the command line arguments are invalid.
        :except FatalError: If some other fatal error occurs.
    """

    arg_parser = get_argument_parser()

    parsed_arguments = arg_parser.parse_args(arguments)

    command_entrypoint = COMMAND_ENTRYPOINT_MAP[parsed_arguments.command]
    command_entrypoint(parsed_arguments)


def get_argument_parser() -> argparse.ArgumentParser:
    """Creates the command line argument parser. Adds subparsers for each command."""

    arg_parser = ArgumentParser('incremental_backup.py', description='Incremental backup utility.')
    arg_subparser = arg_parser.add_subparsers(title='commands', required=True, dest='command')

    for module in COMMAND_MODULES:
        module.add_arg_subparser(arg_subparser)

    return arg_parser


class ArgumentParser(argparse.ArgumentParser):
    """Custom `argparse.ArgumentParser` implementation so we can throw exceptions for invalid arguments instead of
        exiting the process."""

    def error(self, message: str) -> NoReturn:
        full_message = f'{self.prog}: error: {message}'     # Same as base ArgumentParser
        raise FatalArgumentError(full_message, self.format_usage())
