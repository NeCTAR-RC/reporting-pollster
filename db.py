#!/usr/bin/env python
#
# Database connection support
#

import config
import mysql.connector
from datetime import datetime, timedelta
import argparse

tables = [
    'hypervisor',
    'project',
    'user',
    'role',
    'flavour',
    'instance',
    'volume',
    'image'
]

# query strings
queries = {
    'metadata': {
        'query': (
            "select last_update from metadata "
            "where table_name = %s limit 1"
        ),
        'update': (
            "insert into metadata (table_name, last_update) "
            "values (%s, null) "
            "on duplicate key update last_update = null"
        ),
    },
    'hypervisor': {
        'query': (
            "select id, hypervisor_hostname, host_ip, vcpus, "
            "memory_mb, local_gb from nova.compute_nodes"
        ),
        'query_last_updated': (
            "select id, hypervisor_hostname, host_ip, vcpus, "
            "memory_mb, local_gb from nova.compute_nodes "
            "where deleted_at > %s or updated_at > %s"
        ),
        'update': (
            "replace into hypervisor "
            "(id, hostname, ip_address, cpus, memory, local_storage) "
            "values (%s, %s, %s, %s, %s, %s)"
        ),
    },
    'project': {
        # this can't be filtered usefully, so we leave out the _last_updated
        # version
        'query': (
            "select distinct kp.id, kp.name, kp.enabled, i.hard_limit, "
            "c.hard_limit, r.hard_limit, g.total_limit, v.total_limit, "
            "s.total_limit "
            "from keystone.project as kp left outer join "
            "( select  *  from  nova.quotas where deleted = 0 "
            "and resource = 'ram' ) "
            "as r on kp.id = r.project_id left outer join "
            "( select  *  from  nova.quotas where deleted = 0 "
            "and resource = 'instances' ) "
            "as i on kp.id = i.project_id left outer join "
            "( select  *  from  nova.quotas where deleted = 0 "
            "and resource = 'cores' ) "
            "as c on kp.id = c.project_id left outer join "
            "( select project_id, "
            "sum(if(hard_limit>=0,hard_limit,0)) as total_limit "
            "from cinder.quotas where deleted = 0 "
            "and resource like 'gigabytes%' "
            "group by project_id ) "
            "as g on kp.id = g.project_id left outer join "
            "( select project_id, "
            "sum(if(hard_limit>=0,hard_limit,0)) as total_limit "
            "from cinder.quotas where deleted = 0 "
            "and resource like 'volumes%' "
            "group by project_id ) "
            "as v on kp.id = v.project_id left outer join "
            "( select project_id, "
            "sum(if(hard_limit>=0,hard_limit,0)) as total_limit "
            "from cinder.quotas where deleted = 0 "
            "and resource like 'snapshots%' "
            "group by project_id ) "
            "as s on kp.id = s.project_id"
        ),
        'update': (
            "replace into project "
            "(id, display_name, enabled, quota_instances, quota_vcpus, "
            "quota_memory, quota_volume_total, quota_snapshot, "
            "quota_volume_count) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        ),
    },
    'user': {
        # as with the project query, this has no way to filter based on
        # updated time
        'query': (
            "select ku.id, ru.displayname, ru.email, ku.default_project_id, "
            "ku.enabled "
            "from "
            "keystone.user as ku join rcshibboleth.user as ru "
            "on ku.id = ru.user_id"
        ),
        'update': (
            "replace into user "
            "(id, name, email, default_project, enabled) "
            "values (%s, %s, %s, %s, %s)"
        ),
    },
    'role': {
        # and here again
        'query': (
            "select kr.name, ka.actor_id, ka.target_id "
            "from keystone.assignment as ka join keystone.role as kr "
            "on ka.role_id = kr.id "
            "where ka.type = 'UserProject' "
            "AND EXISTS(select * from keystone.user ku "
            "WHERE ku.id =  ka.actor_id) "
            "AND EXISTS(select * from keystone.project kp "
            "WHERE kp.id = ka.target_id)"
        ),
        'update': (
            "replace into role "
            "(role, user, project) "
            "values (%s, %s, %s)"
        ),
    },
    'flavour': {
        'query': (
            "select id, flavorid, name, vcpus, memory_mb, root_gb as root, "
            "ephemeral_gb, is_public "
            "from nova.instance_types"
        ),
        # finally, one we can filter usefully
        'query_last_updated': (
            "select id, flavorid, name, vcpus, memory_mb, root_gb as root, "
            "ephemeral_gb, is_public "
            "from nova.instance_types "
            "where deleted_at > %s or updated_at > %s"
        ),
        'update': (
            "replace into flavour "
            "(id, uuid, name, vcpus, memory, root, ephemeral, public) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s)"
        ),
    },
    'instance': {
        'query': (
            "select project_id, uuid, display_name, vcpus, memory_mb, "
            "root_gb, ephemeral_gb, instance_type_id, user_id, created_at, "
            "deleted_at, if(deleted<>0,false,true), host, availability_zone "
            "from nova.instances"
        ),
        'query_last_updated': (
            "select project_id, uuid, display_name, vcpus, memory_mb, "
            "root_gb, ephemeral_gb, instance_type_id, user_id, created_at, "
            "deleted_at, if(deleted<>0,false,true), host, availability_zone "
            "from nova.instances "
            "where deleted_at > %s or updated_at > %s"
        ),
        'update': (
            "replace into instance "
            "(project_id, id, name, vcpus, memory, root, ephemeral, flavour, "
            "created_by, created, deleted, active, hypervisor, "
            "availability_zone) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        ),
    },
    'volume': {
        'query': (
            "select id, project_id, display_name, size, created_at, "
            "deleted_at, if(attach_status='attached',true,false), "
            "instance_uuid, availability_zone from cinder.volumes"
        ),
        'query_last_updated': (
            "select id, project_id, display_name, size, created_at, "
            "deleted_at, if(attach_status='attached',true,false), "
            "instance_uuid, availability_zone from cinder.volumes "
            "where deleted_at > %s or updated_at > %s"
        ),
        'update': (
            "replace into volume "
            "(id, project_id, display_name, size, created, deleted, attached, "
            "instance_uuid, availability_zone) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        ),
    },
    'image': {
        'query': (
            "select id, owner, name, size, status, is_public, created_at, "
            "deleted_at from glance.images"
        ),
        'query_last_updated': (
            "select id, owner, name, size, status, is_public, created_at, "
            "deleted_at from glance.images "
            "where deleted_at > %s or updated_at > %s"
        ),
        'update': (
            "replace into image "
            "(id, project_id, name, size, status, public, created, deleted) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s)"
        ),
    }
}

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


class Connector(object):
    """
    Wrap the connection between the databases.
    """

    def __init__(self, args):
        self.args = args

        self.cfg = config.Config()
        if 'config_file' in args:
            self.cfg.load_config(args.config_file)

        self.force_update = False
        if 'force_update' in args:
            self.force_update = True

        self.default_lu = self.get_default_last_update()

        self.remote = self.get_remote_connection()
        self.local = self.get_local_connection()

    def get_remote_connection(self):
        remote_creds = self.cfg.get_remote()
        remote = mysql.connector.connect(**remote_creds)
        print remote.get_server_info()
        return remote

    def get_local_connection(self):
        local_creds = self.cfg.get_local()
        local = mysql.connector.connect(**local_creds)
        print local.get_server_info()
        return local

    def get_default_last_update(self):
        last_update = None
        if 'last_updated' in self.args:
            last_update = datetime.strptime(self.args.last_updated, "%Y%m%d")
        if 'last_day' in self.args:
            print "last day"
            last_update = datetime.now() - timedelta(days=1)
        if 'last_week' in self.args:
            print "last week"
            last_update = datetime.now() - timedelta(weeks=1)
        if 'last_month' in self.args:
            print "last month"
            last_update = datetime.now() - timedelta(days=30)
        return last_update

    def get_last_update(self, table):
        if self.default_lu:
            return self.default_lu
        cursor = self.local.cursor()
        cursor.execute(queries['metadata']['query'], (table, ))
        row = cursor.fetchone()
        res = None
        if row:
            res = row[0]
        return res

    def dry_run(self, table):
        print "Processing table " + table

        last_update = self.get_last_update(table)
        if 'force_update' in self.args:
            last_update = False

        if last_update and 'query_last_updated' in queries[table]:
            q = queries[table]['query_last_updated']
            print "Query: " + q % (last_update, last_update)
        else:
            print "Query: " + queries[table]['query']
        print "Update: " + queries[table]['update']

    def process_table(self, table):
        if table not in queries:
            print "Unrecognised table " + table
            return
        start = datetime.now()
        rcursor = self.remote.cursor()
        lcursor = self.local.cursor()

        if not self.args.full_run:
            self.dry_run(table)
            return

        last_update = self.get_last_update(table)
        if 'force_update' in self.args:
            last_update = False

        if last_update:
            print "Updating from " + last_update.isoformat()

        if last_update and 'query_last_updated' in queries[table]:
            print "Processing table " + table + " (last updated)"
            query = queries[table]['query_last_updated']
            args = (last_update, last_update)
            rcursor.execute(query, args)
        else:
            print "Processing table " + table
            query = queries[table]['query']
            rcursor.execute(query)
        rdata = rcursor.fetchall()
        print repr(rcursor.rowcount) + " rows returned"
        middle = datetime.now()
        query = queries[table]['update']
        lcursor.executemany(query, rdata)
        print repr(lcursor.rowcount) + " rows updated"
        self.update_metadata(table)
        self.local.commit()
        print "Warnings: %s" % (lcursor.fetchwarnings())
        end = datetime.now()
        print "Elapsed time:"
        print "query:\t""{0}".format((middle-start).total_seconds())
        print "update:\t{0}".format((end-middle).total_seconds())

    # update the metadata table
    def update_metadata(self, table):
        cursor = self.local.cursor()
        cursor.execute(queries['metadata']['update'], (table, ))


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
    args = parser.parse_args()
    print args
    return args


def main():
    args = parse_args()

    conn = Connector(args)

    for table in tables:
        conn.process_table(table)


if __name__ == '__main__':
    main()
