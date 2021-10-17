import argparse
import pathlib
import sys

from incremental_backup.commands import backup_command


COMMAND_MAP = {
    'backup': backup_command
}


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser('incremental_backup.py', description='Incremental backup utility.')
    arg_subparsers = arg_parser.add_subparsers(title='commands', required=True, dest='command')

    backup_arg_parser = arg_subparsers.add_parser('backup', description='Creates a new backup.',
                                                  help='Creates a new backup.')
    backup_arg_parser.add_argument(
        'source_dir', action='store', type=pathlib.Path, help='Directory to back up.')
    backup_arg_parser.add_argument(
        'target_dir', action='store', type=pathlib.Path, help='Directory to back up into.')
    backup_arg_parser.add_argument(
        '--exclude-pattern', action='append', required=False,
        help='Path pattern(s) to exclude. Can be specified more than once')

    args = arg_parser.parse_args(sys.argv[1:])

    COMMAND_MAP[args.command](args)
