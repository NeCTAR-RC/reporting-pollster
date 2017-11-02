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

from datetime import datetime
from datetime import timedelta
import logging
import pickle

from reporting_pollster.common.config import Config
from reporting_pollster.common.DB import DB
from reporting_pollster import entities


class TableNotFound(Exception):
    """A handler for the requested table was not found
    """
    def __init__(self, table):
        self.table = table


class Entity(object):
    """Top level generic - all entities inherit from this

    Required methods raise NotImplementedError()
    """

    metadata_query = (
        "select last_update from metadata "
        "where table_name = %s limit 1"
    )
    # The class level data cache
    _cache = {}

    def __init__(self, args):
        self.args = args
        self.dbs = Config.get_dbs()
        self.data = []
        self.dry_run = not self.args.full_run
        self.last_update = None
        self.this_update_start = None
        self.last_update_window = args.last_update_window
        self.extract_time = timedelta()
        self.transform_time = timedelta()
        self.load_time = timedelta()

        # We can't simply use parameters here because you can't specify the
        # table name as a parameter - it has to be a plain token in the SQL.
        # Since we're directly manipulating the SQL string, we may as well
        # drop the quoted table name into the values tuple as well . . .
        #
        # Adding support for manually setting the last update timestamp.
        self.metadata_update_template = (
            "insert into metadata (table_name, last_update, row_count) "
            "values ('{table}', %(last_update)s, "
            " (select count(*) from {table})) "
            "on duplicate key update last_update=%(last_update)s, "
            "row_count=(select count(*) from {table})"
        )

    @classmethod
    def from_table_name(cls, table, args):
        """Get an entity object given the table name"""
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

        if not entity:
            raise TableNotFound(table)

        return entity(args)

    @classmethod
    def get_table_names(cls, user_tables=None):
        # no need for order here, just membership and intersection
        tables = set()
        for i in dir(entities.entities):
            entity = getattr(entities.entities, i)
            try:
                tables.add(getattr(entity, 'table'))
            except AttributeError:
                pass
        if user_tables:
            # we take the intersection of the two sets here
            user_tables = set(user_tables)
        # back to an ordered form now
            tables = user_tables & tables
        tables = [t for t in tables]
        # silly workaround time: the aggregate update process caches data that
        # the hypervisor update process relies on, so we push the hypervisor
        # update to the end of the list
        if 'hypervisor' in tables:
            tables.remove('hypervisor')
            tables.append('hypervisor')
        # XXX: the hypervisor table /must/ run with the aggregate table - I
        # need to figure out a way to handle that with user specified lists of
        # tables . . .
        #
        # silly workaround time, part two: the instances update process caches
        # data that the project table update requires, so push the project
        # update to the end of the list
        if 'project' in tables:
            tables.remove('project')
            tables.append('project')
        # XXX: they project table /must/ run with the instances table. As
        # above, that requires some thought when dealing with user specified
        # table lists. The 'has_instances' column is a shortcut column, so it
        # may not be that much of an issue in practise, but that needs to be
        # looked into.
        #
        # It's also worth noting that this is an issue right now - the next
        # instance table update will update the has_instances column, but only
        # for projects that have instances mentioned in the last_update period,
        # with only a full instance table update capturing everything. I'm not
        # sure how best to handle this - possibly with a local query against
        # the existing reporting.instance data?
        #
        # Note: this whole dependency thing needs a better implementation, with
        # classes declaring their dependencies and then some algorithmic
        # process coming up with a processing order.
        return tables

    @classmethod
    def _cache_data(cls, key, data):
        """Stash some data in a class-level cache so that it can be re-used by
        other Entity object instantiations. This is used to support derived
        updates across multiple Entity object instantiations within a single
        run.
        """
        # the implementation is stupid simple . . .
        cls._cache[key] = data

    @classmethod
    def _get_cached_data(cls, key):
        """Retrieve data cached by another (or potentially this) Entity object
        instantiation.
        """
        return cls._cache[key]

    @classmethod
    def drop_cached_data(cls):
        """Drop any cached data.
        """
        cls._cache = {}

    def dup_record(self, record):
        """Trivial utility method.
        Probably doesn't seem important, but it avoids any confusion between
        the return type of the database query (which may simply emulate a dict
        type) and a "real" dict
        """
        t = {}
        for key in record.keys():
            t[key] = record[key]
        return t

    def _format_query(self, qname):
        """This is designed to handle the case where the database name is
        non-standard. Database names in the relevant queries need to be
        converted to format string entities like '{nova}' or '{keystone}' for
        this to work.

        Note that placeholders in the actual queries use either %s style
        formatting, or %(name)s style - while these are both valid python
        string formatting codes, they're in the queries because that's what
        the mysql connector uses. The "".format() method is used here because
        it doesn't clash with query placeholders.
        """
        return self.queries[qname].format(**self.dbs)

    def _extract_all(self):
        logging.info("Extracting data for table %s", self.table)
        cursor = DB.remote_cursor()
        cursor.execute(self._format_query('query'))
        self.db_data = cursor.fetchall()
        logging.debug("Rows returned: %d", cursor.rowcount)

    def _extract_dry_run(self):
        logging.info("Extracting data for %s table", self.table)
        logging.debug("Query: %s", self._format_query('query'))

    def _extract_all_last_update(self):
        logging.info("Extracting data for %s table (last_update)", self.table)
        query = self._format_query('query_last_update')
        cursor = DB.remote_cursor()
        cursor.execute(query, {'last_update': self.last_update})
        self.db_data = cursor.fetchall()
        logging.debug("Rows returned: %d", cursor.rowcount)

    def _extract_dry_run_last_update(self):
        logging.info("Extracting data for %s table (last update)", self.table)
        query = self._format_query('query_last_update')
        logging.debug("Query: %s", query % {'last_update': self.last_update})

    def _extract_no_last_update(self):
        """Can be used when no last_update is available for this entity
        """
        if self.dry_run:
            self._extract_dry_run()
        else:
            self._extract_all()

    def _extract_with_last_update(self):
        """Can be used when a last_update value is meaningfull for this entity
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
        """Extract, from whatever sources are necessary, the data that this
        entity requires

        This may make use of one of the utility functions above.
        """
        raise NotImplementedError()

    def transform(self):
        """Transform the data loaded via extract() into the format to be loaded
        using load()
        """
        raise NotImplementedError()

    def _load_dry_run(self):
        logging.info("Loading data for %s table", self.table)
        logging.debug("Query: %s", self._format_query('update'))

    # Note: we really need to give some consideration to the use of
    # transactions - right now we only have one case where the entity code
    # uses a transaction above this level, but it's hard to know what other
    # cases may come up as the processing gets more sophisticated. For now
    # we're going to assume that the high level wrappers (load, _load_simple)
    # have complete ownership of the transaction, but the lower stuff
    # (_load_many and _run_sql_cursor) don't. Hence the user needs to make
    # sure they handle transactions and commits themselves.
    def _load(self):
        logging.info("Loading data for %s table", self.table)
        cursor = DB.local_cursor()
        # necessary because it's entirely possible for a last_update query to
        # return no data
        if len(self.data) > 0:
            cursor.executemany(self._format_query('update'),
                               self.data)
            DB.local().commit()
            logging.debug("Rows updated: %d", cursor.rowcount)
        self.set_last_update()

    def _load_simple(self):
        if self.dry_run:
            self._load_dry_run()
        else:
            self._load()

    # Note: this is a low-level function that doesn't do any commits - the
    # caller is expected to handle database consistency
    def _load_many(self, qname, data):
        q = self._format_query(qname)
        if self.dry_run:
            logging.debug("Special query: %s", q)
        else:
            cursor = DB.local_cursor()
            cursor.executemany(q, data)
            logging.debug("Rows updated: %d", cursor.rowcount)

    # seems a bit silly, but this captures the dry_run and debug logic
    #
    # Note: since we don't own the cursor we don't do any cursor-specific
    # debugging output or commits
    def _run_sql_cursor(self, cursor, qname):
        q = self._format_query(qname)
        if self.dry_run:
            logging.debug("Generic query: %s", q)
        else:
            cursor.execute(q)

    def load(self):
        """Load data about this entity into the data store.
        """
        raise NotImplementedError()

    def _get_timing(self):
        msg = (
            "Process timing (%s):" % (self.table) +
            "\textract: %f" % (self.extract_time.total_seconds()) +
            "\ttransform: %f" % (self.transform_time.total_seconds()) +
            "\tload: %f" % (self.load_time.total_seconds())
        )
        return msg

    def process(self):
        """Wrapper for the extract/load loop
        """
        logging.debug("Processing table %s", self.table)
        self.this_update_start = datetime.now()
        self.extract()
        self.transform()
        self.load()

        logging.debug(self._get_timing())

    def _get_default_last_update(self, args):
        last_update = None
        if 'last_updated' in args:
            last_update = datetime.strptime(args.last_updated, "%Y%m%d")
        if 'last_day' in args:
            logging.debug("Update for last day")
            last_update = datetime.now() - timedelta(days=1)
        if 'last_week' in args:
            logging.debug("Update for last week")
            last_update = datetime.now() - timedelta(weeks=1)
        if 'last_month' in args:
            logging.debug("Update for last month")
            last_update = datetime.now() - timedelta(days=30)
        return last_update

    def _get_last_update(self, table):
        """Get the time that the data was updated most recently, so that we can
        process only the updated data.
        """
        cursor = DB.local_cursor()
        cursor.execute(self.metadata_query, (table, ))
        row = cursor.fetchone()
        res = None
        if row:
            res = row['last_update']
            res = res - timedelta(seconds=self.last_update_window)
        return res

    def get_last_update(self, table=None):
        if not table:
            table = self.table
        last_update = self._get_default_last_update(self.args)
        if not last_update:
            last_update = self._get_last_update(table)
        if not last_update:
            logging.debug("No last update value available")
        else:
            logging.debug("Last update: %s", last_update.isoformat())
        return last_update

    def set_last_update(self, table=None, last_update=None):
        """Set the last_update field for the given table
        """
        if not table:
            table = self.table
        if self.dry_run:
            logging.debug("Setting last update on table %s", table)
            return
        # the user can specify a different last update time, otherwise we use
        # the start point of the processing loop for this entity
        if not last_update:
            last_update = self.this_update_start

        cursor = DB.local_cursor(dictionary=False)
        query = self.metadata_update_template.format(**{'table': table})
        cursor.execute(query, {'last_update': last_update})
        DB.local().commit()

    @staticmethod
    def _begin(conn):
        """Older versions of pymysql don't support a begin() or
        start_transaction(), so we wrap that functionality here
        """
        try:
            conn.begin()
        except AttributeError:
            pass


class Aggregate(Entity):
    """Aggregate entity, which is used by OpenStack as part of its scheduling
    logic. This pulls its data from the APIs.
    """

    # this seems a bit silly, but it allows us to use the load_simple method.
    queries = {
        'update': (
            "replace into aggregate (id, availability_zone, name, created, "
            "deleted, active) values (%(id)s, %(availability_zone)s, "
            "%(name)s, %(created)s, %(deleted)s, %(active)s)"
        ),
        'aggregate_host_cleanup': (
            "delete from aggregate_host"
        ),
        'aggregate_host': (
            "replace into aggregate_host (id, availability_zone, host) "
            "values (%(id)s, %(availability_zone)s, %(host)s)"
        ),
        'hypervisor_az_update': (
            "update hypervisor set availability_zone = %(availability_zone)s "
            "where host = %(host)s"
        ),
    }

    table = "aggregate"

    def __init__(self, args):
        super(Aggregate, self).__init__(args)
        self.api_data = []
        self.agg_data = []
        self.agg_host_data = []
        self.hypervisor_az_data = {}
        self.data = []
        self.novaclient = Config.get_nova_client()

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
            logging.info("Extracting API data for the aggregate table")
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        # We have two separate pieces of data here: the aggregate itself, and
        # the list of hosts assigned to each aggregate. We're dealing with them
        # in the same place because the data all comes from one API query.
        for aggregate in self.api_data:
            agg = self.new_agg_record()
            # Newton and above moves aggregates out of the cells to the top
            # level, meaning the aggregate ID format changes from a string
            # (with routing information) to a globally unique integer. This
            # makes for a break in the information contained in the
            # aggregate/aggregate_host tables, but the result will be a little
            # more meaningful (currently the 'availability_zone' field is
            # actually the cell name rather than the actual availability zone).
            #
            # Note that this code is the canonical source of the hypervisor->AZ
            # mapping, and hence updates the hypervisor table in addition to
            # the aggregate and aggregate_host tables.
            if type(aggregate.id) == int:
                id = aggregate.id
                az = aggregate.availability_zone
            else:
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
                hname = host.split('.')[0]
                h = self.new_agg_host_record()
                h['id'] = id
                h['availability_zone'] = az
                h['host'] = hname
                self.agg_host_data.append(h)

                self.hypervisor_az_data[hname] = az

        self.data = self.agg_data
        Entity._cache_data('hypervisor_az', self.hypervisor_az_data)
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()

        # the aggregate table is simple to deal with.
        self._load_simple()

        # the aggregate_host table is a pretty simple many to many mapping
        # (hosts can be in more than one aggregate). This is the authoritative
        # method of determining the availability zone that hypervisors are
        # members of.
        #
        # Because there's no temporal data associated with any of this we're
        # adding our own - a simple 'last_seen' timestamp which will allow
        # users of this data to figure out which entries are current and which
        # are historical (and at the same time they'll be able to track the
        # history of the data).
        #
        # Note that there's a window here where the hypervisor table and the
        # aggregate table can get out of sync - we've cached the hypervisor/AZ
        # mapping /now/, but if there are changes between now and when the
        # hypervisor queries happen they can be out of sync. There's no way to
        # avoid this, though, outside of wrapping /everything/ in a big
        # transaction, which I'd really like to avoid.
        if not self.dry_run:
            self._load_many('aggregate_host', self.agg_host_data)
            self.set_last_update(table='aggregate_host')

        self.load_time = datetime.now() - start


class Hypervisor(Entity):
    """Hypervisor entity. This uses the hypervisor table locally, and gets its
    source data from the Nova APIs.
    """

    queries = {
        'query': (
            "select id, 'nova' as availability_zone, hypervisor_hostname, "
            "host_ip, vcpus, memory_mb, local_gb from {nova}.compute_nodes"
        ),
        'query_last_update': (
            "select id, 'nova' as availability_zone, hypervisor_hostname, "
            "host_ip, vcpus, memory_mb, local_gb from {nova}.compute_nodes "
            "where ifnull(deleted_at, now()) > %(last_update)s "
            "   or updated_at > %(last_update)s"
        ),
        'update': (
            "replace into hypervisor "
            "(id, availability_zone, host, hostname, ip_address, cpus, "
            "memory, local_storage, last_seen) "
            "values (%(id)s, %(availability_zone)s, %(host)s, %(hostname)s, "
            "%(ip_address)s, %(cpus)s, %(memory)s, %(local_storage)s, null)"
        ),
    }

    table = "hypervisor"

    def __init__(self, args):
        super(Hypervisor, self).__init__(args)
        self.db_data = []
        self.api_data = []
        self.data = []
        self.novaclient = Config.get_nova_client()
        self.hypervisor_az_data = {}

    # PUll all the data from whatever sources we need, and assemble them here
    #
    # Right now this is entirely the database.

    def new_record(self):
        return {
            'id': None,
            'availability_zone': None,
            'host': None,
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
            logging.info("Extracting API data for the hypervisor table")
        try:
            self.hypervisor_az_data = Entity._get_cached_data("hypervisor_az")
        except KeyError:
            pass
        self.extract_time = datetime.now() - start

    # ded simple until we have more than one data source
    def transform(self):
        start = datetime.now()
        for hypervisor in self.api_data:
            r = self.new_record()
            # here the availability zone is actually the cell name, for
            # historical reasons. With the change to Newton, this will become
            # the actual availability zone, with this table updated by the
            # aggregate update process (which is the canonical source of the
            # hypervisor->aggregate->AZ mapping). This update makes use of the
            # host part of the hypervisor_hostname field, which is used by nova
            # as the key in the host aggregate relationship.
            (cell, hid) = hypervisor.id.split('!', 1)[1].split('@')
            hname = hypervisor.hypervisor_hostname.split('.')[0]
            try:
                az = self.hypervisor_az_data[hname]
            except KeyError:
                # use the cell name - this provides some historical consistency
                az = cell
            r['id'] = int(hid)
            r['availability_zone'] = az
            r['host'] = hname
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
    """Project entity, using the project table locally and the keystone.project
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
            "from {keystone}.project as kp left outer join "
            "( select  *  from  {nova}.quotas where deleted = 0 "
            "and resource = 'ram' ) "
            "as r on kp.id = r.project_id left outer join "
            "( select  *  from  {nova}.quotas where deleted = 0 "
            "and resource = 'instances' ) "
            "as i on kp.id = i.project_id left outer join "
            "( select  *  from  {nova}.quotas where deleted = 0 "
            "and resource = 'cores' ) "
            "as c on kp.id = c.project_id left outer join "
            "( select project_id, "
            "sum(if(hard_limit>=0,hard_limit,0)) as total_limit "
            "from {cinder}.quotas where deleted = 0 "
            "and resource like 'gigabytes%' "
            "group by project_id ) "
            "as g on kp.id = g.project_id left outer join "
            "( select project_id, "
            "sum(if(hard_limit>=0,hard_limit,0)) as total_limit "
            "from {cinder}.quotas where deleted = 0 "
            "and resource like 'volumes%' "
            "group by project_id ) "
            "as v on kp.id = v.project_id left outer join "
            "( select project_id, "
            "sum(if(hard_limit>=0,hard_limit,0)) as total_limit "
            "from {cinder}.quotas where deleted = 0 "
            "and resource like 'snapshots%' "
            "group by project_id ) "
            "as s on kp.id = s.project_id"
        ),
        'update': (
            "replace into project "
            "(id, display_name, organisation, description, enabled, personal, "
            "has_instances, quota_instances, quota_vcpus, quota_memory, "
            "quota_volume_total, quota_snapshot, quota_volume_count) "
            "values (%(id)s, %(display_name)s, %(organisation)s, "
            "%(description)s, %(enabled)s, %(personal)s, %(has_instances)s, "
            "%(quota_instances)s, %(quota_vcpus)s, %(quota_memory)s, "
            "%(quota_volume_total)s, %(quota_snapshots)s, "
            "%(quota_volume_count)s)"
        ),
        'tenant_owner': (
            "select ka.target_id as tenant, ka.actor_id as user, "
            "rc.shibboleth_attributes as shib_attr "
            "from {keystone}.assignment as ka join {rcshibboleth}.user as rc "
            "on ka.actor_id = rc.user_id "
            "where ka.type = 'UserProject' and ka.role_id = "
            "(select id from {keystone}.role where name = 'TenantManager')"
        ),
        'tenant_member': (
            "select ka.target_id as tenant, ka.actor_id as user, "
            "rc.shibboleth_attributes as shib_attr "
            "from {keystone}.assignment as ka join {rcshibboleth}.user as rc "
            "on ka.actor_id = rc.user_id "
            "where ka.type = 'UserProject' and ka.role_id = "
            "(select id from {keystone}.role where name = 'Member')"
        ),
    }

    table = "project"

    def __init__(self, args):
        super(Project, self).__init__(args)
        self.db_data = []
        self.tenant_owner_data = []
        self.tenant_member_data = []
        self.has_instance_data = {}

    def new_record(self):
        return {
            'id': None,
            'display_name': None,
            'organisation': None,
            'description': None,
            'enabled': None,
            'personal': None,
            'has_instances': False,
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
        cursor.execute(self._format_query('tenant_owner'))
        self.tenant_owner_data = cursor.fetchall()
        cursor.execute(self._format_query('tenant_member'))
        self.tenant_member_data = cursor.fetchall()
        try:
            self.has_instance_data = Entity._get_cached_data('has_instance')
        except KeyError:
            pass
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
            # default is set to False in the new_record() method
            if t['id'] in self.has_instance_data:
                t['has_instances'] = True
            self.data.append(t)

        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start


class User(Entity):
    """User entity, using the user table locally and the keystone.user table
    remotely, along with the rcshibboleth.user table.
    """
    queries = {
        'query': (
            "select ku.id as id, ru.displayname as name, ru.email as email, "
            "ku.default_project_id as default_project, ku.enabled as enabled "
            "from "
            "{keystone}.user as ku join {rcshibboleth}.user as ru "
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
    """Roles map between users and entities. This is a subset of the full range
    of mappings listed in keystone.roles, filtered to include only the
    user/project relations.
    """

    queries = {
        'query': (
            "select kr.name as role, ka.actor_id as user, "
            "ka.target_id as project "
            "from {keystone}.assignment as ka join {keystone}.role as kr "
            "on ka.role_id = kr.id "
            "where ka.type = 'UserProject' "
            "AND EXISTS(select * from {keystone}.user ku "
            "WHERE ku.id =  ka.actor_id) "
            "AND EXISTS(select * from {keystone}.project kp "
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
    """Flavour entity, using the flavour table locally and the nova.instance_types
    table remotely.
    """

    queries = {
        'query': (
            "select id, flavorid as uuid, name, vcpus, memory_mb as memory, "
            "root_gb as root, ephemeral_gb as ephemeral, is_public as public, "
            "not deleted as active "
            "from {nova}.instance_types"
        ),
        'query_last_update': (
            "select id, flavorid as uuid, name, vcpus, memory_mb as memory, "
            "root_gb as root, ephemeral_gb as ephemeral, is_public as public, "
            "not deleted as active "
            "from {nova}.instance_types "
            "where ifnull(deleted_at, now()) > %(last_update)s "
            "   or updated_at > %(last_update)s"
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
    """Instance entity, using the instance table locally and the nova.instances
    table remotely.
    """
    queries = {
        'query': (
            "select project_id, uuid as id, display_name as name, vcpus, "
            "memory_mb as memory, root_gb as root, ephemeral_gb as ephemeral, "
            "instance_type_id as flavour, user_id as created_by, "
            "created_at as created, deleted_at as deleted, "
            "if(deleted<>0,false,true) as active, host as hypervisor, "
            "availability_zone, cell_name "
            "from {nova}.instances order by created_at"
        ),
        'query_last_update': (
            "select project_id, uuid as id, display_name as name, vcpus, "
            "memory_mb as memory, root_gb as root, ephemeral_gb as ephemeral, "
            "instance_type_id as flavour, user_id as created_by, "
            "created_at as created, deleted_at as deleted, "
            "if(deleted<>0,false,true) as active, host as hypervisor, "
            "availability_zone, cell_name "
            "from {nova}.instances "
            "where ifnull(deleted_at, now()) > %(last_update)s "
            "   or updated_at > %(last_update)s "
            "order by created_at"
        ),
        'update': (
            "replace into instance "
            "(project_id, id, name, vcpus, memory, root, ephemeral, flavour, "
            "created_by, created, deleted, active, hypervisor, "
            "availability_zone, cell_name) "
            "values (%(project_id)s, %(id)s, %(name)s, %(vcpus)s, %(memory)s, "
            "%(root)s, %(ephemeral)s, %(flavour)s, %(created_by)s, "
            "%(created)s, %(deleted)s, %(active)s, %(hypervisor)s, "
            "%(availability_zone)s, %(cell_name)s)"
        ),
        'hist_agg': (
            "replace into historical_usage "
            "(day, vcpus, memory, local_storage) "
            "values (%(day)s, %(vcpus)s, %(memory)s, %(local_storage)s)"
        ),
    }

    table = "instance"

    def __init__(self, args):
        super(Instance, self).__init__(args)
        self.db_data = []
        self.hist_agg_data = []
        self.has_instance_data = {}

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

    def generate_hist_agg_data(self):
        # the data should be ordered by created_at, so we start by taking the
        # created_at value and use that as the starting point.
        def date_to_day(date):
            return datetime(date.year,
                            date.month,
                            date.day)
        hist_agg = {}
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
                    hist_agg[key]['local_storage'] += (instance['root'] +
                                                       instance['ephemeral'])
                    day = day + timedelta(1)
                self.has_instance_data[instance['project_id']] = True
            keys = hist_agg.keys()
            keys.sort()
            for key in keys:
                self.hist_agg_data.append(hist_agg[key])

    def transform(self):
        start = datetime.now()
        self.data = self.db_data
        self.generate_hist_agg_data()
        Entity._cache_data('has_instance', self.has_instance_data)
        self.transform_time = datetime.now() - start

    def _load_hist_agg(self):
        logging.debug("Loading data for historical_usage table")
        # necessary because it's entirely possible for a last_update query to
        # return no data
        if len(self.hist_agg_data) > 0 or self.dry_run:
            self._load_many('hist_agg', self.hist_agg_data)
            DB.local().commit()
            # note that we never /use/ this to determine whether to update or
            # not, this is for informational purposes only
            self.set_last_update(table="historical_usage")

    def load(self):
        start = datetime.now()
        # comment out for sanity while testing
        self._load_simple()
        self._load_hist_agg()
        self.load_time = datetime.now() - start


class Volume(Entity):
    """Volume entity, using the volume table locally and the cinder.volumes table
    remotely.
    """
    queries = {
        'query': (
            "select distinct v.id, v.project_id, v.display_name, v.size, "
            "v.created_at as created, v.deleted_at as deleted, "
            "if(v.attach_status='attached',true,false) as attached, "
            "a.instance_uuid, v.availability_zone, not v.deleted as active "
            "from {cinder}.volumes as v left join "
            "{cinder}.volume_attachment as a "
            "on v.id = a.volume_id and a.deleted = 0"
        ),
        'query_last_update': (
            "select distinct v.id, v.project_id, v.display_name, v.size, "
            "v.created_at as created, v.deleted_at as deleted, "
            "if(v.attach_status='attached',true,false) as attached, "
            "a.instance_uuid, v.availability_zone, not v.deleted as active "
            "from {cinder}.volumes as v left join "
            "{cinder}.volume_attachment as a "
            "on v.id = a.volume_id and a.deleted = 0 "
            "where ifnull(v.deleted_at, now()) > %(last_update)s "
            "   or v.updated_at > %(last_update)s"
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
    """Image entity, using the image table locally and the glance.image table
    remotely.
    """
    queries = {
        'query': (
            "select id, owner as project_id, name, size, status, "
            "is_public as public, created_at as created, "
            "deleted_at as deleted, not deleted as active "
            "from {glance}.images"
        ),
        'query_last_update': (
            "select id, owner as project_id, name, size, status, "
            "is_public as public, created_at as created, "
            "deleted_at as deleted, not deleted as active "
            "from {glance}.images "
            "where ifnull(deleted_at, now()) > %(last_update)s "
            "   or updated_at > %(last_update)s"
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


class Allocation(Entity):
    """Allocation data, using the allocation table locally and the
    dashboard.rcallocation_allocationrequest table remotely.
    """
    queries = {
        'query': (
            "SELECT ra.id, project_id, project_name, "
            "  contact_email, approver_email, "
            "  ci.email as chief_investigator, status, start_date, end_date, "
            "  modified_time, "
            "  field_of_research_1, for_percentage_1, "
            "  field_of_research_2, for_percentage_2, "
            "  field_of_research_3, for_percentage_3, "
            "  funding_national_percent as funding_national, "
            "  funding_node "
            "FROM {dashboard}.rcallocation_allocationrequest as ra "
            "LEFT JOIN {dashboard}.rcallocation_chiefinvestigator as ci "
            "ON ra.id = ci.allocation_id "
            "WHERE "
            "  parent_request_id is null and status in ('A', 'X', 'J') "
            "ORDER BY modified_time; "
        ),
        'query_last_update': (
            "SELECT ra.id, project_id, project_name, "
            "  contact_email, approver_email, "
            "  ci.email as chief_investigator, status, start_date, end_date, "
            "  modified_time, "
            "  field_of_research_1, for_percentage_1, "
            "  field_of_research_2, for_percentage_2, "
            "  field_of_research_3, for_percentage_3, "
            "  funding_national_percent as funding_national, "
            "  funding_node "
            "FROM {dashboard}.rcallocation_allocationrequest as ra "
            "LEFT JOIN {dashboard}.rcallocation_chiefinvestigator as ci "
            "ON ra.id = ci.allocation_id "
            "WHERE "
            "  parent_request_id is null and status in ('A', 'X', 'J') "
            "  AND modified_time  >= %(last_update)s "
            "ORDER BY modified_time; "
        ),
        'update': (
            "REPLACE INTO allocation "
            "(id, project_id, project_name, contact_email, approver_email, "
            "  chief_investigator, status, start_date, end_date, "
            "  modified_time, "
            "  field_of_research_1, for_percentage_1, "
            "  field_of_research_2, for_percentage_2, "
            "  field_of_research_3, for_percentage_3, "
            "  funding_national, funding_node ) "
            "VALUES (%(id)s, %(project_id)s, %(project_name)s, "
            "  %(contact_email)s, %(approver_email)s, %(chief_investigator)s, "
            "  %(status)s, %(start_date)s, %(end_date)s, "
            "  %(modified_time)s, "
            "  %(field_of_research_1)s, %(for_percentage_1)s, "
            "  %(field_of_research_2)s, %(for_percentage_2)s, "
            "  %(field_of_research_3)s, %(for_percentage_3)s, "
            "  %(funding_national)s, %(funding_node)s)"
        ),
        'tenant_allocation_id': (
            "select id as project_id, "
            "replace(replace(substring(extra, "
            "                          locate('allocation_id', extra)+16, 5), "
            "                '\"', "
            "                ''), "
            "        '}}', "
            "        '') as allocation_id "
            "from {keystone}.project where name not like 'pt-%' "
            "and extra like '%allocation_id%'"
        ),
    }

    table = "allocation"

    def __init__(self, args):
        super(Allocation, self).__init__(args)
        self.db_data = []
        self.tenant_allocation_data = []

    def extract(self):
        start = datetime.now()
        self._extract_with_last_update()
        cursor = DB.remote_cursor()
        cursor.execute(self._format_query('tenant_allocation_id'))
        self.tenant_allocation_data = cursor.fetchall()
        self.extract_time = datetime.now() - start

    def transform(self):
        start = datetime.now()
        # create a dict keyed on the project_id
        project_alloc = {}
        for t in self.tenant_allocation_data:
            project_alloc[t['project_id']] = t['allocation_id']
        # and on allocation_id
        allocs_by_id = {}
        for t in self.db_data:
            allocs_by_id[t['id']] = t
        # go through the raw data and look for duplicates
        alloc_dict = {}
        alloc_null_tenant = []
        for alloc in self.db_data:
            project_id = alloc['project_id']
            # deal with null tenant_uuids
            if not project_id or project_id == "":
                alloc['project_id'] = None
                alloc_null_tenant.append(self.dup_record(alloc))
            elif project_id not in alloc_dict:
                alloc_dict[project_id] = self.dup_record(alloc)
            else:
                # duplicated project_id - de-dup using the keystone
                # data
                try:
                    alloc_id = project_alloc[project_id]
                    alloc = self.dup_record(allocs_by_id[alloc_id])
                    alloc_dict[project_id] = alloc
                except KeyError:
                    # the project has duplicate alloctaion records
                    # but no allocation_id in keystone - this should
                    # be logged as a bogus allocation
                    # Note: this should not happen, as manual verification
                    # of the allocations data has shown that all
                    # duplicated project_ids have an allocation_id in
                    # keystone
                    logging.info("Bogus allocation id %s for project %s",
                                 alloc['id'], project_id)

        # now pull it into the final data table
        self.data = []
        keys = alloc_dict.keys()
        keys.sort()
        for k in keys:
            self.data.append(alloc_dict[k])
        for a in alloc_null_tenant:
            self.data.append(a)
        self.transform_time = datetime.now() - start

    def load(self):
        start = datetime.now()
        self._load_simple()
        self.load_time = datetime.now() - start
