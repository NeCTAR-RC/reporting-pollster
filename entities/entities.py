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
from db import DB


# Note: not using the abc module beause I can't be arsed.
class Entity(object):
    """
    Top level generic - all entities inherit from this

    Required methods are listed here
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

    def extract(self):
        """
        Extract, from whatever sources are necessary, the data that this
        entity requires
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
    def get_default_last_update(cls, args):
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
        last_update = self.get_default_last_update(cls, args)
        if not last_update:
            last_update = self._get_last_update(table)
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
    }

    table = "hypervisor"

    def __init__(self, args):
        self.args = args
        # Rows from the compute_nodes table in nova
        self.db_data = []
        # data from the nova API
        self.api_data = []
        self.data = []
        self.dry_run = self.args.dry_run
        self.last_update = None

    # PUll all the data from whatever sources we need, and assemble them here
    #
    # Right now this is entirely the database.

    def _extract_all(self):
        cursor = DB.remote_cursor()
        cursor.execute(self.queries['query'])
        self.db_data = cursor.fetchall()

    def _extract_all_last_update(self):
        cursor = DB.remote_cursor()
        cursor.execute(self.queries['query_last_update'],
                       (self.last_update, self.last_update))
        self.db_data = cursor.fetchall()

    def _extract_dry_run(self):
        print "Extracting data for hypervisor table"
        if 'debug' in self.args:
            print "Query: " + self.queries['query']
    
    def _extract_dry_run_last_update(self):
        print "Extracting data for  hypervisor table (last update)"
        if 'debug' in self.args:
            query = self.queries['query_last_update']
            print "Query: " + query % (self.last_update, self.last_update)

    def extract(self):
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
    def __init__(self, args):
        self.args = args
        self.data = []
        self.db_data = []
        


class User(Entity):
    """
    User entity, using the user table locally and the keystone.user table
    remotely, along with the rcshibboleth.user table.
    """

    def get_last_updated(self):
        return False

class Role(Entity):
    """
    Roles map between users and entities. This is a subset of the full range
    of mappings listed in keystone.roles, filtered to include only the
    user/project relations.
    """

    def get_last_updated(self):
        return False

class Flavour(Entity):
    """
    Flavour entity, using the flavour table locally and the nova.instance_types
    table remotely.
    """

class Instance(Entity):
    """
    Instance entity, using the instance table locally and the nova.instances
    table remotely.
    """

class Volume(Entity):
    """
    Volume entity, using the volume table locally and the cinder.volumes table
    remotely.
    """

class Image(Entity):
    """
    Image entity, using the image table locally and the glance.image table
    remotely.
    """
