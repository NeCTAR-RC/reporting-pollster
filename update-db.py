#!/usr/bin/env python
#
# Database connection support
#

import config
from common.config import Config
from common.DB import DB
from entities.entities import Entity
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
