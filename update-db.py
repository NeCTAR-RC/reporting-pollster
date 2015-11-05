#!/usr/bin/env python
#
# Database connection support
#

from reporting.common.config import Config
from reporting.common.DB import DB
from reporting.entities.entities import Entity
from datetime import datetime, timedelta
import argparse

tables = [
    'aggregate',
    'hypervisor',
    'project',
    'user',
    'role',
    'flavour',
    'instance',
    'volume',
    'image'
]

#
# So how do we do this . . .
#
# Firstly, we do the basic updates by simply pulling in data and inserting it
# into the local database. This requires a split between pulling the data out
# of the database and putting into the local database - hence the doubling up
# of the queryies.
#
# In addition to that we need to collect data for a number of other things,
# including swift and more complicated procedural database transactions. But
# for now we just reimplement what we've got.
#
# Since we'll be operating on a dedicated local database for updates we can
# treat a lot of this stuff more brutally than we would if we were trying to
# share space with other databases. For a lot of this stuff the fact we'll be
# pulling data from a remote host means the local DB won't be the bottleneck,
# too.
#
#


def parse_args():

    parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
    parser.add_argument('-c', '--config-file', action='store',
                        required=False, help='specify config file')
    parser.add_argument('--force-update', action='store_true', required=False,
                        help="ignore last update time and force a full update")
    parser.add_argument('--last-updated', action='store',
                        required=False, help=(
                            "Specify a last updated date for this run. "
                            "Should be in the format 'YYYYMMDD'"
                            ))
    parser.add_argument('--last-day', action='store_true', required=False,
                        help="update the last day's worth of data")
    parser.add_argument('--last-week', action='store_true', required=False,
                        help="update the last week's worth of data")
    parser.add_argument('--last-month', action='store_true', required=False,
                        help="update the last month's worth of data")
    parser.add_argument('-f', '--full-run', action='store_true',
                        required=False, default=False, help="execute a full query/update run")
    parser.add_argument('--debug', action='count', help="increase debug level")
    args = parser.parse_args()
    print args
    return args


def main():
    args = parse_args()

    cfg = Config()
    if 'config_file' in args:
        Config.reload_config(args.config_file)

    for table in tables:
        entity = Entity.from_table_name(table, args)
        entity.process()


if __name__ == '__main__':
    main()
