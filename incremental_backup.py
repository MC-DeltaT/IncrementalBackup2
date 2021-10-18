import argparse
import sys

from incremental_backup.commands import backup as backup_command


COMMAND_ENTRYPOINT_MAP = {
    backup_command.COMMAND_NAME: backup_command.entrypoint
}


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser('incremental_backup.py', description='Incremental backup utility.')
    arg_subparser = arg_parser.add_subparsers(title='commands', required=True, dest='command')
    backup_command.add_arg_subparser(arg_subparser)

    args = arg_parser.parse_args(sys.argv[1:])
    COMMAND_ENTRYPOINT_MAP[args.command](args)
