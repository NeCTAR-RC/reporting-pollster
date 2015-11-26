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

import pickle
from datetime import datetime
from datetime import timedelta
import novaclient.v2.client as nvclient
from reporting_pollster.common.DB import DB
from reporting_pollster.common.config import Config
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
        self.extract_time = timedelta()
        self.transform_time = timedelta()
        self.load_time = timedelta()

    # this really needs to be done properly with logging and stuff, but for
    # now this will do
    def _debug(self, msg):
        if 'debug' in self.args:
            print msg

    def _info(self, msg):
        if 'quiet' not in self.args:
            print msg

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
        self._info("Extracting data for table " + self.table)
        cursor = DB.remote_cursor()
        cursor.execute(self.queries['query'])
        self.db_data = cursor.fetchall()
        self._debug("Rows returned: %d" % (cursor.rowcount))

    def _extract_dry_run(self):
        self._info("Extracting data for " + self.table + " table")
        self._debug("Query: " + self.queries['query'])

    def _extract_all_last_update(self):
        self._info("Extracting data for " + self.table
                   + " table (last_update)")
        cursor = DB.remote_cursor()
        cursor.execute(self.queries['query_last_update'],
                       (self.last_update, self.last_update))
        self.db_data = cursor.fetchall()
        self._debug("Rows returned: %d" % (cursor.rowcount))

    def _extract_dry_run_last_update(self):
        self._info("Extracting data for " + self.table
                   + " table (last update)")
        query = self.queries['query_last_update']
        self._debug("Query: " + query % (self.last_update, self.last_update))

    def _extract_no_last_update(self):
        """
        Can be used when no last_update is available for this entity
        """
        if self.dry_run:
            self._extract_dry_run()
        else:
            self._extract_all()

    def _extract_with_last_update(self):
        """
        Can be used when a last_update value is meaningfull for this entity
        """
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

    def _load_dry_run(self):
        self._info("Loading data for " + self.table + " table")
        self._debug("Query: " + self.queries['update'])

    # Note: we really need to give some consideration to the use of
    # transactions - right now we only have one case where the entity code
    # uses a transaction above this level, but it's hard to know what other
    # cases may come up as the processing gets more sophisticated. For now
    # we're going to assume that the high level wrappers (load, _load_simple)
    # have complete ownership of the transaction, but the lower stuff
    # (_load_many and _run_sql_cursor) don't. Hence the user needs to make
    # sure they handle transactions and commits themselves.
    def _load(self):
        self._info("Loading data for " + self.table + " table")
        cursor = DB.local_cursor()
        # necessary because it's entirely possible for a last_update query to
        # return no data
        if len(self.data) > 0:
            cursor.executemany(self.queries['update'],
                               self.data)
            DB.local().commit()
            self._debug("Rows updated: %d" % (cursor.rowcount))
        self.set_last_update()

    def _load_simple(self):
        if self.dry_run:
            self._load_dry_run()
        else:
            self._load()

    def _load_many(self, query, data):
        if self.dry_run:
            self._debug("Special query: " + query)
        else:
            cursor = DB.local_cursor()
            cursor.executemany(query, data)
            self._debug("Rows updated: %d" % (cursor.rowcount))

    # seems a bit silly, but this captures the dry_run and debug logic
    #
    # Note: since we don't own the cursor we don't do any cursor-specific
    # debugging output
    def _run_sql_cursor(self, cursor, query):
        if self.dry_run:
            self._debug("Generic query: " + query)
        else:
            cursor.execute(query)

    def load(self):
        """
        Load data about this entity into the data store.
        """
        raise NotImplementedError()

    def _get_timing(self):
        msg = (
            "Process timing (%s):" % (self.table)
            + "\textract: %f" % (self.extract_time.total_seconds())
            + "\ttransform: %f" % (self.transform_time.total_seconds())
            + "\tload: %f" % (self.load_time.total_seconds())
        )
        return msg

    def process(self):
        """
        Wrapper for the extract/load loop
        """
        self._debug("Processing table " + self.table)
        self.extract()
        self.transform()
        self.load()

        self._debug(self._get_timing())

    @classmethod
    def _get_default_last_update(cls, args):
        last_update = None
        if 'last_updated' in args:
            last_update = datetime.strptime(args.last_updated, "%Y%m%d")
        if 'last_day' in args:
            cls._debug("Update for last day")
            last_update = datetime.now() - timedelta(days=1)
        if 'last_week' in args:
            cls._debug("Update for last week")
            last_update = datetime.now() - timedelta(weeks=1)
        if 'last_month' in args:
            cls._debug("Update for last month")
            last_update = datetime.now() - timedelta(days=30)
        return last_update

    @classmethod
    def _get_last_update(cls, table):
        """
        Get the time that the data was updated most recently, so that we can
        process only the updated data.
        """
        cursor = DB.local_cursor(dictionary=False)
        cursor.execute(cls.metadata_query, (table, ))
        row = cursor.fetchone()
        res = None
        if row:
            res = row[0]
        return res

    def get_last_update(self, table=None):
        if not table:
            table = self.table
        last_update = self._get_default_last_update(self.args)
        if not last_update:
            last_update = self._get_last_update(table)
        if not last_update:
            self._debug("No last update value available")
        else:
            self._debug("Last update: %s" % (last_update.isoformat()))
        return last_update

    def set_last_update(self, table=None):
        """
        Set the last_update field to the current time for the given table
        """
        if not table:
            table = self.table
        if self.dry_run:
            self._debug("Setting last update on table " + table)
            return

        cursor = DB.local_cursor(dictionary=False)
        cursor.execute(self.metadata_update, (table, ))
        DB.local().commit()


class Aggregate(Entity):
    """
    Aggregate entity, which is used by OpenStack as part of its scheduling
    logic. Sadly, this is probably entirely API dependant.
    """

    # this seems a bit silly, but it allows us to use the load_simple method.
    queries = {
        'update':  (
            "replace into aggregate (id, availability_zone, name, created, "
            "deleted, active) values (%(id)s, %(availability_zone)s, "
            "%(name)s, %(created)s, %(deleted)s, %(active)s)"
        ),
    }

    aggregate_host_cleanup = (
        "delete from aggregate_host"
    )

    aggregate_host_query = (
        "replace into aggregate_host (id, availability_zone, host) "
        "values (%(id)s, %(availability_zone)s, %(host)s)"
    )

    table = "aggregate"

    def __init__(self, args):
        super(Aggregate, self).__init__(args)
        self.api_data = []
        self.agg_data = []
        self.agg_host_data = []
        self.data = []
        novacreds = Config.get_nova()
        self.novaclient = nvclient.Client(**novacreds)

    def new_agg_record(self):
        return {
            'id': None,
            'availability_zone': None,
            'name': None,
            'created': None,
            'deleted': None,
            'active': None,
        }

    def new_agg_host_record(self):
        return {
            'id': None,
            'availability_zone': None,
            'host': None,
        }

    def extract(self):
        start = datetime.now()
        # NeCTAR requires hypervisors details from the API
        if not self.dry_run:
            self.api_data = self.novaclient.aggregates.list()
        else:
            self._info("Extracting API data for the aggregate table")
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        # We have two separate pieces of data here: the aggregate itself, and
        # the list of hosts assigned to each aggregate. We're dealing with them
        # in the same place because the data all comes from one API query.
        for aggregate in self.api_data:
            agg = self.new_agg_record()
            id = aggregate.id.split('!', 1)[1]
            (az, id) = id.split('@')
            agg['id'] = id
            agg['availability_zone'] = az
            agg['name'] = aggregate.name
            agg['created'] = aggregate.created_at
            agg['deleted'] = aggregate.deleted_at
            agg['active'] = not aggregate.deleted
            self.agg_data.append(agg)

            for host in aggregate.hosts:
                h = self.new_agg_host_record()
                h['id'] = id
                h['availability_zone'] = az
                h['host'] = host.split('.')[0]
                self.agg_host_data.append(h)

        self.data = self.agg_data
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()

        # the aggregate table is simple to deal with.
        self._load_simple()

        # we need to be a little careful with the aggregate_host table, because
        # it's a real pain to know if hosts have been removed (we capture all
        # the additions, of course, but not the removals). So we need to delete
        # everything and start afresh with each update. To avoid people seeing
        # things in an odd state we need to wrap this in a transaction.
        if not self.dry_run:
            DB.local().start_transaction()
            cursor = DB.local_cursor()
            self._run_sql_cursor(cursor, self.aggregate_host_cleanup)
            self._load_many(self.aggregate_host_query, self.agg_host_data)
            DB.local().commit()

        self.load_time = datetime.now() - start


class Hypervisor(Entity):
    """
    Hypervisor entity, uses the hypervisor table locally and the
    nova.compute_nodes table on the remote end. This may also make use of the
    nova apis for some information.
    """

    queries = {
        'query': (
            "select id, 'nova' as availability_zone, hypervisor_hostname, "
            "host_ip, vcpus, memory_mb, local_gb from nova.compute_nodes"
        ),
        'query_last_update': (
            "select id, 'nova' as availability_zone, hypervisor_hostname, "
            "host_ip, vcpus, memory_mb, local_gb from nova.compute_nodes "
            "where ifnull(deleted_at, now()) > %s or updated_at > %s"
        ),
        'update': (
            "replace into hypervisor "
            "(id, availability_zone, hostname, ip_address, cpus, memory, "
            "local_storage, last_seen) "
            "values (%(id)s, %(availability_zone)s, %(hostname)s, "
            "%(ip_address)s, %(cpus)s, %(memory)s, %(local_storage)s, null)"
        ),
    }

    table = "hypervisor"

    def __init__(self, args):
        super(Hypervisor, self).__init__(args)
        self.db_data = []
        self.api_data = []
        self.data = []
        novacreds = Config.get_nova()
        self.novaclient = nvclient.Client(**novacreds)

    # PUll all the data from whatever sources we need, and assemble them here
    #
    # Right now this is entirely the database.

    def new_record(self):
        return {
            'id': None,
            'availability_zone': None,
            'hostname': None,
            'ip_address': None,
            'cpus': None,
            'memory': None,
            'local_storage': None
        }

    def extract(self):
        start = datetime.now()
        # NeCTAR requires hypervisors details from the API
        if not self.dry_run:
            self.api_data = self.novaclient.hypervisors.list()
        else:
            self._info("Extracting API data for the hypervisor table")
        self.extract_time = datetime.now() - start

    # ded simple until we have more than one data source
    def transform(self):
        start = datetime.now()
        for hypervisor in self.api_data:
            r = self.new_record()
            # the cell/hypervisor id is in the form:
            # nectar!cell@id, where the cell is the availability zone.
            (az, hid) = hypervisor.id.split('!', 1)[1].split('@')
            r['id'] = hid
            r['availability_zone'] = az
            r['hostname'] = hypervisor.hypervisor_hostname
            r['ip_address'] = hypervisor.host_ip
            r['cpus'] = hypervisor.vcpus
            r['memory'] = hypervisor.memory_mb
            r['local_storage'] = hypervisor.local_gb
            self.data.append(r)
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class Project(Entity):
    """
    Project entity, using the project table locally and the keystone.project
    table remotely. Also the allocations database, and probably the keystone
    apis too.
    """
    queries = {
        'query': (
            "select distinct kp.id as id, kp.name as display_name, "
            "kp.description as description, kp.enabled as enabled, "
            "kp.name like 'pt-%' as personal, "
            "false as has_instances, "
            "i.hard_limit as quota_instances, c.hard_limit as quota_vcpus, "
            "r.hard_limit as quota_memory, "
            "g.total_limit as quota_volume_total, "
            "s.total_limit as quota_snapshots, "
            "v.total_limit as quota_volume_count "
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
            "(id, display_name, organisation, description, enabled, personal, "
            "quota_instances, quota_vcpus, quota_memory, quota_volume_total, "
            "quota_snapshot, quota_volume_count) "
            "values (%(id)s, %(display_name)s, %(organisation)s, "
            "%(description)s, %(enabled)s, %(personal)s, "
            "%(quota_instances)s, %(quota_vcpus)s, %(quota_memory)s, "
            "%(quota_volume_total)s, %(quota_snapshots)s, "
            "%(quota_volume_count)s)"
        ),
    }

    tenant_owner_query = (
        "select ka.target_id as tenant, ka.actor_id as user, "
        "rc.shibboleth_attributes as shib_attr "
        "from keystone.assignment as ka join rcshibboleth.user as rc "
        "on ka.actor_id = rc.user_id "
        "where ka.type = 'UserProject' and ka.role_id = "
        "(select id from keystone.role where name = 'TenantManager')"
    )

    tenant_member_query = (
        "select ka.target_id as tenant, ka.actor_id as user, "
        "rc.shibboleth_attributes as shib_attr "
        "from keystone.assignment as ka join rcshibboleth.user as rc "
        "on ka.actor_id = rc.user_id "
        "where ka.type = 'UserProject' and ka.role_id = "
        "(select id from keystone.role where name = 'Member')"
    )

    table = "project"

    def __init__(self, args):
        super(Project, self).__init__(args)
        self.db_data = []
        self.tenant_owner_data = []

    def new_record(self):
        return {
            'id': None,
            'display_name': None,
            'organisation': None,
            'description': None,
            'enabled': None,
            'personal': None,
            'quota_instances': None,
            'quota_vcpus': None,
            'quota_memory': None,
            'quota_volume_total': None,
            'quota_volume_count': None,
            'quota_snapshot': None,
        }

    def extract(self):
        start = datetime.now()
        self._extract_no_last_update()
        cursor = DB.remote_cursor()
        cursor.execute(self.tenant_owner_query)
        self.tenant_owner_data = cursor.fetchall()
        cursor.execute(self.tenant_member_query)
        self.tenant_member_data = cursor.fetchall()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        # we have the data we pulled from the project database, but we now
        # need to merge in the tenant owner data. We iterate over the database
        # results and pull in the tenant owner info and organisation stuff . .
        #
        # This is nowhere near perfect - there are a lot of logical holes. But
        # we want this approximation for the moment.

        def new_tenant_owner():
            return {
                'tenant': None,
                'user': None,
                'shib_attr': None,
            }

        # convert the raw database result set into something we can query
        # by tenant id
        def tenant_role_to_dict(data):
            tdict = {}
            for t in data:
                id = t['tenant']
                shib_attr = pickle.loads(t['shib_attr'])
                to = new_tenant_owner()
                to['tenant'] = id
                to['user'] = t['user']
                to['shib_attr'] = shib_attr
                tdict[id] = to
            return tdict

        # Build the tenant owner dict
        tod = tenant_role_to_dict(self.tenant_owner_data)
        # and since we need to get useful information for personal trials . . .
        tmd = tenant_role_to_dict(self.tenant_member_data)

        # now we walk the main result set and fill it out in full
        self.data = []
        for tenant in self.db_data:
            t = self.new_record()
            for key in tenant.keys():
                t[key] = tenant[key]
            # personal trials do not have a TenantManager - leave these null
            try:
                shib_attr = tod[tenant['id']]['shib_attr']
            except KeyError:
                try:
                    shib_attr = tmd[tenant['id']]['shib_attr']
                except KeyError:
                    self.data.append(t)
                    continue
            # this is a bit nasty, but it does two things: firstly, it picks
            # up the two useful shibboleth attributes that we can use here
            # namely 'organisation' and 'homeorganisation', of which
            # organisation is the more useful since it's an actual name rather
            # than a domain, and the sorted keys mean organisation overrides
            # homeorganisation; and secondly it picks up the stupid stupid
            # misspelling that's used by some organisations: they spelled it
            # 'orginisation'. Stupid. I picked a substring that would match
            # for US spellings, too, though I don't know if that's an issue
            # here.
            keys = shib_attr.keys()
            keys.sort()
            for k in keys:
                if ('organi' in k or 'orgini' in k) and 'type' not in k:
                    t['organisation'] = shib_attr[k]
            # there are still some cases where there's no organisation set,
            # even with all that. In those cases we use the email domain
            if not t['organisation']:
                t['organisation'] = shib_attr['mail'].split('@')[1]
            self.data.append(t)

        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class User(Entity):
    """
    User entity, using the user table locally and the keystone.user table
    remotely, along with the rcshibboleth.user table.
    """
    queries = {
        'query': (
            "select ku.id as id, ru.displayname as name, ru.email as email, "
            "ku.default_project_id as default_project, ku.enabled as enabled "
            "from "
            "keystone.user as ku join rcshibboleth.user as ru "
            "on ku.id = ru.user_id"
        ),
        'update': (
            "replace into user "
            "(id, name, email, default_project, enabled) "
            "values (%(id)s, %(name)s, %(email)s, %(default_project)s, "
            "%(enabled)s)"
        ),
    }

    table = "user"

    def __init__(self, args):
        super(User, self).__init__(args)
        self.db_data = []

    def extract(self):
        start = datetime.now()
        self._extract_no_last_update()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class Role(Entity):
    """
    Roles map between users and entities. This is a subset of the full range
    of mappings listed in keystone.roles, filtered to include only the
    user/project relations.
    """

    queries = {
        'query': (
            "select kr.name as role, ka.actor_id as user, "
            "ka.target_id as project "
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
            "values (%(role)s, %(user)s, %(project)s)"
        ),
    }

    table = "role"

    def __init__(self, args):
        super(Role, self).__init__(args)
        self.db_data = []

    def extract(self):
        start = datetime.now()
        self._extract_no_last_update()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class Flavour(Entity):
    """
    Flavour entity, using the flavour table locally and the nova.instance_types
    table remotely.
    """

    queries = {
        'query': (
            "select id, flavorid as uuid, name, vcpus, memory_mb as memory, "
            "root_gb as root, ephemeral_gb as ephemeral, is_public as public, "
            "not deleted as active "
            "from nova.instance_types"
        ),
        'query_last_update': (
            "select id, flavorid as uuid, name, vcpus, memory_mb as memory, "
            "root_gb as root, ephemeral_gb as ephemeral, is_public as public, "
            "not deleted as active "
            "from nova.instance_types "
            "where ifnull(deleted_at, now()) > %s or updated_at > %s"
        ),
        'update': (
            "replace into flavour "
            "(id, uuid, name, vcpus, memory, root, ephemeral, public, active) "
            "values (%(id)s, %(uuid)s, %(name)s, %(vcpus)s, %(memory)s, "
            "%(root)s, %(ephemeral)s, %(public)s, %(active)s)"
        ),
    }

    table = "flavour"

    def __init__(self, args):
        super(Flavour, self).__init__(args)
        self.db_data = []

    def extract(self):
        start = datetime.now()
        self._extract_with_last_update()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class Instance(Entity):
    """
    Instance entity, using the instance table locally and the nova.instances
    table remotely.
    """
    queries = {
        'query': (
            "select project_id, uuid as id, display_name as name, vcpus, "
            "memory_mb as memory, root_gb as root, ephemeral_gb as ephemeral, "
            "instance_type_id as flavour, user_id as created_by, "
            "created_at as created, deleted_at as deleted, "
            "if(deleted<>0,false,true) as active, host as hypervisor, "
            "availability_zone "
            "from nova.instances order by created_at"
        ),
        'query_last_update': (
            "select project_id, uuid as id, display_name as name, vcpus, "
            "memory_mb as memory, root_gb as root, ephemeral_gb as ephemeral, "
            "instance_type_id as flavour, user_id as created_by, "
            "created_at as created, deleted_at as deleted, "
            "if(deleted<>0,false,true) as active, host as hypervisor, "
            "availability_zone "
            "from nova.instances "
            "where ifnull(deleted_at, now()) > %s or updated_at > %s "
            "order by created_at"
        ),
        'update': (
            "replace into instance "
            "(project_id, id, name, vcpus, memory, root, ephemeral, flavour, "
            "created_by, created, deleted, active, hypervisor, "
            "availability_zone) "
            "values (%(project_id)s, %(id)s, %(name)s, %(vcpus)s, %(memory)s, "
            "%(root)s, %(ephemeral)s, %(flavour)s, %(created_by)s, "
            "%(created)s, %(deleted)s, %(active)s, %(hypervisor)s, "
            "%(availability_zone)s)"
        ),
    }

    hist_agg_query = (
        "replace into historical_usage "
        "(day, vcpus, memory, local_storage) "
        "values (%(day)s, %(vcpus)s, %(memory)s, %(local_storage)s)"
    )

    has_instance_update_query = (
        "update project set has_instances = true "
        "where id = %(project_id)"
    )

    table = "instance"

    def __init__(self, args):
        super(Instance, self).__init__(args)
        self.db_data = []
        self.hist_agg_data = []
        self.has_instance_data = []

    def extract(self):
        start = datetime.now()
        self._extract_with_last_update()
        self.extract_time = datetime.now() - start

    def new_hist_agg(self, date):
        return {
            'day': date,
            'vcpus': 0,
            'memory': 0,
            'local_storage': 0
        }

    def new_has_instance_update(self, project):
        return {
            'project_id': project
        }

    def generate_hist_agg_data(self):
        # the data should be ordered by created_at, so we start by taking the
        # created_at value and use that as the starting point.
        def date_to_day(date):
            return datetime(date.year,
                            date.month,
                            date.day)
        hist_agg = {}
        has_instance_data = []
        if len(self.db_data) > 0:
            # create a list of records to be added to the historical_usate
            # table
            #
            # How to handle a partial update? Well, we need to make sure that
            # we don't put a partial day's update in, which means we need to
            # have all the data for the state of things from the last_update
            # time point forwards, rather than just the instances that have
            # been updated. So we need to change the last_updated query to
            # return all instances that were active at that point. That would
            # mean where created_at < last_update and deleted_at > last_update
            #
            # we already have a last update value
            if not self.last_update:
                orig_day = date_to_day(self.db_data[0]['created'])
            else:
                orig_day = date_to_day(self.last_update)
            # generate our storage dictionary, starting from the start date
            # we determined above
            day = orig_day
            while day < date_to_day(datetime.now()):
                hist_agg[day.strftime("%s")] = self.new_hist_agg(day)
                day = day + timedelta(1)
            # Iterate over the list of instances, and update the
            # historical usage records for each one.
            for instance in self.db_data:
                # here we start from the created date, and then if that's
                # before the start date we found above we use that start date
                # instead
                day = date_to_day(instance['created'])
                if day < orig_day:
                    day = orig_day
                deleted = date_to_day(datetime.now())
                if instance['deleted']:
                    deleted = date_to_day(instance['deleted'])
                while day < deleted:
                    key = day.strftime("%s")
                    hist_agg[key]['vcpus'] += instance['vcpus']
                    hist_agg[key]['memory'] += instance['memory']
                    hist_agg[key]['local_storage'] += (instance['root']
                                                       + instance['ephemeral'])
                    day = day + timedelta(1)
                if instance['project_id'] not in has_instance_data:
                    has_instance_data.append(instance['project_id'])
            keys = hist_agg.keys()
            keys.sort()
            for key in keys:
                self.hist_agg_data.append(hist_agg[key])
            for proj in has_instance_data:
                self.has_instance_data.append(
                    self.new_has_instance_update(proj)
                )

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.generate_hist_agg_data()
        self.transform_time = datetime.now() - start

    def _load_hist_agg(self):
        self._debug("Loading data for historical_usage table")
        # necessary because it's entirely possible for a last_update query to
        # return no data
        if len(self.hist_agg_data) > 0 or self.dry_run:
            self._load_many(self.hist_agg_query, self.hist_agg_data)
            DB.local().commit()
            # note that we never /use/ this to determine whether to update or
            # not, this is for informational purposes only
            self.set_last_update(table="historical_usage")

    def _load_has_instance_data(self):
        self._debug("Updating project table with has instance data")
        if len(self.has_instance_data) > 0 or self.dry_run:
            self._load_many(self.has_instance_update_query,
                            self.has_instance_data)
            DB.local().commit()

    def load(self):
        start = datetime.now()
        # comment out for sanity while testing
        self._load_simple()
        self._load_hist_agg()
        self._load_has_instance_data()
        self.load_time = datetime.now() - start


class Volume(Entity):
    """
    Volume entity, using the volume table locally and the cinder.volumes table
    remotely.
    """
    queries = {
        'query': (
            "select id, project_id, display_name, size, "
            "created_at as created, deleted_at as deleted, "
            "if(attach_status='attached',true,false) as attached, "
            "instance_uuid, availability_zone, not deleted as active "
            "from cinder.volumes "
        ),
        'query_last_update': (
            "select id, project_id, display_name, size, "
            "created_at as created, deleted_at as deleted, "
            "if(attach_status='attached',true,false) as attached, "
            "instance_uuid, availability_zone, not deleted as active "
            "from cinder.volumes "
            "where ifnull(deleted_at, now()) > %s or updated_at > %s"
        ),
        'update': (
            "replace into volume "
            "(id, project_id, display_name, size, created, deleted, attached, "
            "instance_uuid, availability_zone, active) "
            "values (%(id)s, %(project_id)s, %(display_name)s, %(size)s, "
            "%(created)s, %(deleted)s, %(attached)s, %(instance_uuid)s, "
            "%(availability_zone)s, %(active)s)"
        ),
    }

    table = "volume"

    def __init__(self, args):
        super(Volume, self).__init__(args)
        self.db_data = []

    def extract(self):
        start = datetime.now()
        self._extract_with_last_update()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class Image(Entity):
    """
    Image entity, using the image table locally and the glance.image table
    remotely.
    """
    queries = {
        'query': (
            "select id, owner as project_id, name, size, status, "
            "is_public as public, created_at as created, "
            "deleted_at as deleted, not deleted as active "
            "from glance.images"
        ),
        'query_last_update': (
            "select id, owner as project_id, name, size, status, "
            "is_public as public, created_at as created, "
            "deleted_at as deleted, not deleted as active "
            "from glance.images "
            "where ifnull(deleted_at, now()) > %s or updated_at > %s"
        ),
        'update': (
            "replace into image "
            "(id, project_id, name, size, status, public, created, deleted, "
            "active) values (%(id)s, %(project_id)s, %(name)s, %(size)s, "
            "%(status)s, %(public)s, %(created)s, %(deleted)s, %(active)s)"
        ),
    }

    table = "image"

    def __init__(self, args):
        super(Image, self).__init__(args)
        self.db_data = []

    def extract(self):
        start = datetime.now()
        self._extract_with_last_update()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start
