#
# Entities being reported on.
#
# At the moment this maps mostly to tables in the local database, but it
# doesn't have to - in the case of the instances table there are a number
# of things we may derive from the same data source, though they're mostly
# aggregates of some sort or another.
#
# We can't really make each object map to an actual physical-ish entity,
# because that would be quite wasteful both in live memory and processing.
# What we can probably do is make each class a mapping tool from the
# source(es) to the data store. Thus when you say "extract" that would pull
# in all the source data in bulk, and when you say "load" it would put it
# all in the data store. The actual live object would keep the source data
# in memory while it was working, but wouldn't try to map each chunk to an
# in-memory object.

import sys
from common.DB import DB
import entities


class Entity(object):
    """
    Top level generic - all entities inherit from this

    Required methods raise NotImplementedError()
    """

    metadata_query = (
            "select last_update from metadata "
            "where table_name = %s limit 1"
    )
    metadata_update = (
            "insert into metadata (table_name, last_update) "
            "values (%s, null) "
            "on duplicate key update last_update = null"
    ) 

    def __init__(self, args):
        self.args = args
        self.data = []
        self.dry_run = not self.args.full_run
        self.last_update = None

    @classmethod
    def from_table_name(cls, table, args):
        """
        Get an entity object given the table name
        """
        entity = None
        for i in dir(entities.entities):
            entity = getattr(entities.entities, i)
            try:
                t = getattr(entity, 'table')
                if t == table:
                    # break here rather than return, so that the actual object
                    # instantiation isn't wrapped in a try: block
                    break
            except AttributeError:
                pass

        return entity(args)

    def _extract_all(self):
        cursor = DB.remote_cursor()
        cursor.execute(self.queries['query'])
        self.db_data = cursor.fetchall()

    def _extract_all_dry_run(self):
        print "Extracting data for " + self.table + " table"
        if 'debug' in self.args:
            print "Query: " + self.queries['query']

    def _extract_all_last_update(self):
        cursor = DB.remote_cursor()
        cursor.execute(self.queries['query_last_update'],
                       (self.last_update, self.last_update))
        self.db_data = cursor.fetchall()

    def _extract_dry_run_last_update(self):
        print "Extracting data for " + self.table + " table (last update)"
        if 'debug' in self.args:
            query = self.queries['query_last_update']
            print "Query: " + query % (self.last_update, self.last_update)

    def _extract_no_last_update(self):
        """
        Can be used when no last_update is available for this entity
        """
        if self.dry_run:
            self._extract_all_dry_run()
        else:
            self._extract_all()

    def _extract_with_last_update(self):
        self.last_update = self.get_last_update()
        if 'force_update' in self.args:
            self.last_update = False
        method_name = "_extract_all"
        if self.dry_run:
            method_name = "_extract_dry_run"

        if self.last_update:
            method_name = method_name + "_last_update"

        # yey reflection
        method = getattr(self, method_name)
        method()

    def extract(self):
        """
        Extract, from whatever sources are necessary, the data that this
        entity requires

        This may make use of one of the utility functions above.
        """
        raise NotImplementedError()

    def transform(self):
        """
        Transform the data loaded via extract() into the format to be loaded
        using load()
        """
        raise NotImplementedError()

    def load(self):
        """
        Load data about this entity into the data store.
        """
        raise NotImplementedError()

    def process(self):
        """
        Wrapper for the extract/load loop
        """
        self.extract()
        self.transform()
        self.load()

    @classmethod
    def _get_default_last_update(cls, args):
        last_update = None
        if 'last_updated' in args:
            last_update = datetime.strptime(self.args.last_updated, "%Y%m%d")
        if 'last_day' in args:
            print "last day"
            last_update = datetime.now() - timedelta(days=1)
        if 'last_week' in args:
            print "last week"
            last_update = datetime.now() - timedelta(weeks=1)
        if 'last_month' in args:
            print "last month"
            last_update = datetime.now() - timedelta(days=30)
        return last_update

    @classmethod
    def _get_last_update(cls, table):
        """
        Get the time that the data was updated most recently, so that we can
        process only the updated data.
        """
        cursor = DB.local_cursor()
        cursor.execute(cls.metadata_query, (table, ))
        row = cursor.fetchone()
        res = None
        if row:
            res = row[0]
        return res

    def get_last_update(self):
        last_update = self._get_default_last_update(self.args)
        if not last_update:
            last_update = self._get_last_update(self.table)
        return last_update

    @classmethod
    def set_last_update(cls, table):
        """
        Set the last_update field to the current time for the given table
        """
        cursor = DB.local_cursor()
        cursor.execute(cls.metadata_update, (table, ))
        

class Hypervisor(Entity):
    """
    Hypervisor entity, uses the hypervisor table locally and the
    nova.compute_nodes table on the remote end. This may also make use of the
    nova apis for some information.
    """

    queries = {
        'query': (
            "select id, hypervisor_hostname, host_ip, vcpus, "
            "memory_mb, local_gb from nova.compute_nodes"
        ),
        'query_last_update': (
            "select id, hypervisor_hostname, host_ip, vcpus, "
            "memory_mb, local_gb from nova.compute_nodes "
            "where deleted_at > %s or updated_at > %s"
        ),
        'update': (
            "replace into hypervisor "
            "(id, hostname, ip_address, cpus, memory, local_storage) "
            "values (%s, %s, %s, %s, %s, %s)"
        ),
    }

    table = "hypervisor"

    def __init__(self, args):
        super(Hypervisor, self).__init__(args)
        self.db_data = []

    # PUll all the data from whatever sources we need, and assemble them here
    #
    # Right now this is entirely the database.

    def extract(self):
        self._extract_with_last_update()

    # ded simple until we have more than one data source
    def transform(self):
        self.data = self.db_data

    def _load_dry_run(self):
        print "Loading data for hypervisor table"
        if 'debug' in self.args:
            print "Query: " + self.queries['update']

    def _load(self):
        cursor = DB.local_cursor()
        cursor.execute(self.queries['update'],
                       self.data)
        self.set_last_update()

    def load(self):
        if self.dry_run:
            self._load_dry_run()
        else:
            self._load()


class Project(Entity):
    """
    Project entity, using the project table locally and the keystone.project
    table remotely. Also the allocations database, and probably the keystone
    apis too.
    """
    queries = {
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
    }

    table = "project"

    def __init__(self, args):
        super(Project, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_no_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass


class User(Entity):
    """
    User entity, using the user table locally and the keystone.user table
    remotely, along with the rcshibboleth.user table.
    """
    queries = {
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
    }

    table = "user"

    def __init__(self, args):
        super(User, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_no_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass

class Role(Entity):
    """
    Roles map between users and entities. This is a subset of the full range
    of mappings listed in keystone.roles, filtered to include only the
    user/project relations.
    """

    queries = {
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
    }

    table = "role"
 
    def __init__(self, args):
        super(Role, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_no_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass


    def get_last_updated(self):
        return False

class Flavour(Entity):
    """
    Flavour entity, using the flavour table locally and the nova.instance_types
    table remotely.
    """

    queries = {
        'query': (
            "select id, flavorid, name, vcpus, memory_mb, root_gb as root, "
            "ephemeral_gb, is_public "
            "from nova.instance_types"
        ),
        'query_last_update': (
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
    }

    table = "flavour"
 
    def __init__(self, args):
        super(Flavour, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_with_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass


class Instance(Entity):
    """
    Instance entity, using the instance table locally and the nova.instances
    table remotely.
    """
    queries = {
        'query': (
            "select project_id, uuid, display_name, vcpus, memory_mb, "
            "root_gb, ephemeral_gb, instance_type_id, user_id, created_at, "
            "deleted_at, if(deleted<>0,false,true), host, availability_zone "
            "from nova.instances"
        ),
        'query_last_update': (
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
    }

    table = "instance"
 
    def __init__(self, args):
        super(Instance, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_with_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass


class Volume(Entity):
    """
    Volume entity, using the volume table locally and the cinder.volumes table
    remotely.
    """
    queries = {
        'query': (
            "select id, project_id, display_name, size, created_at, "
            "deleted_at, if(attach_status='attached',true,false), "
            "instance_uuid, availability_zone from cinder.volumes"
        ),
        'query_last_update': (
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
    }

    table = "volume"
 
    def __init__(self, args):
        super(Volume, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_with_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass


class Image(Entity):
    """
    Image entity, using the image table locally and the glance.image table
    remotely.
    """
    queries = {
        'query': (
            "select id, owner, name, size, status, is_public, created_at, "
            "deleted_at from glance.images"
        ),
        'query_last_update': (
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

    table = "image"

    def __init__(self, args):
        super(Image, self).__init__(args)
        self.db_data = []

    def extract(self):
        self._extract_with_last_update()

    def transform(self):
        self.data = self.db_data

    def load(self):
        pass


