#
# Config setup
#

import logging
import os.path

from ConfigParser import SafeConfigParser
from keystoneauth1 import loading
from keystoneauth1 import session
from novaclient import client as nvclient

from reporting_pollster.common import credentials


class ConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg


config_file = "./reporting.conf"

# defaults for testing
remote = {
    'user': 'reporting-test',
    'passwd': 'Testing out the system',
    'database': 'reporting2',
    'host': '127.0.0.1',
    'port': 33306,
}

local = {
    'user': 'reporting-test',
    'passwd': 'Testing out the system',
    'database': 'reporting2',
    'host': '127.0.0.1',
    'port': 3306,
}

# defaults for both testing and production
dbs = {
    'keystone': 'keystone',
    'nova': 'nova',
    'cinder': 'cinder',
    'glance': 'glance',
    'rcshibboleth': 'rcshibboleth',
    'dashboard': 'dashboard',
}


def sanitise_db_creds(creds):
    """Clean up certain values in the credentials to make sure that the DB driver
    doesn't get confused.
    """
    tmp = {}
    for name, value in creds.items():
        if name == 'port':
            tmp[name] = int(value)
        elif name == 'password':
            tmp['passwd'] = value
        else:
            tmp[name] = value
    return tmp


def verify_nova_creds(nova_version, creds):
    client = Config.get_nova_client(nova_version, creds)
    # will return success quickly or fail quickly
    logging.debug("Testing nova credentials")
    try:
        client.availability_zones.list()
    except Exception as e:
        raise ConfigError(
            "Validating Nova credentials failed: %s" % (e.message)
        )


class Config(object):
    """Configuration wrapper class.
    """
    remote = None
    local = None
    nova = None
    nova_api_version = '2'
    dbs = None
    config_file = None

    def __init__(self):
        self.load_defaults()

    @classmethod
    def reload_config(cls, filename):
        cls.config_file = filename
        logging.info("Loading configuration from %s", filename)
        if not os.path.isfile(filename):
            raise ConfigError("Configuration file %s not found" % (filename))
        cls.remote = None
        cls.local = None
        cls.nova = None
        cls.dbs = None
        # check environment first, override later
        cls.load_nova_environment()

        parser = SafeConfigParser()
        parser.read(filename)
        if not parser.has_section('remote'):
            raise ConfigError("No Remote DB Config")
        if not parser.has_section('local'):
            raise ConfigError("No Local DB Config")
        if not parser.has_section('nova') and cls.nova is None:
            raise ConfigError("No Nova Creds")

        creds = {}
        for (name, value) in parser.items('remote'):
            creds[name] = value
        cls.remote = sanitise_db_creds(creds)
        creds = {}
        for (name, value) in parser.items('local'):
            creds[name] = value
        cls.local = sanitise_db_creds(creds)
        if parser.has_section('nova'):
            creds = {}
            for (name, value) in parser.items('nova'):
                creds[name] = value
            try:
                cls.nova = cls.extract_nova_version(creds)
            except KeyError:
                raise ConfigError("No Valid Nova Creds")
        if not parser.has_section('databases'):
            logging.info("No database mapping defined - using default")
            cls.dbs = dbs
        else:
            cls.dbs = {}
            for (name, value) in parser.items('databases'):
                cls.dbs[name] = value
            if dbs.keys() != cls.dbs.keys():
                raise ConfigError("Invalid DB Mapping")
        verify_nova_creds(cls.nova_api_version, cls.nova)

    @classmethod
    def load_config(cls, filename):
        if cls.remote and cls.local and cls.nova and cls.dbs:
            return
        cls.reload_config(filename)

    @classmethod
    def load_defaults(cls):
        if cls.config_file and os.path.isfile(cls.config_file):
            cls.reload_config(cls.config_file)
        else:
            logging.debug("Loading in-build default configuration")
            cls.remote = sanitise_db_creds(remote)
            cls.local = sanitise_db_creds(local)
            cls.dbs = dbs
            cls.load_nova_environment()
        verify_nova_creds(cls.nova_api_version, cls.nova)

    @classmethod
    def extract_nova_version(cls, creds):
        try:
            cls.nova_api_version = creds['version']
            del creds['version']
            return creds
        except KeyError:
            logging.debug("Trying to load invalid nova credentials")
            raise

    @classmethod
    def load_nova_environment(cls):
        try:
            creds = credentials.get_nova_credentials()
            cls.nova = cls.extract_nova_version(creds)
        except KeyError:
            logging.info("Loading nova credentials from environment failed")

    @classmethod
    def get_remote(cls):
        if not cls.remote:
            cls.load_defaults()
        return cls.remote

    @classmethod
    def get_local(cls):
        if not cls.local:
            cls.load_defaults()
        return cls.local

    @classmethod
    def get_nova(cls):
        if not cls.nova:
            cls.load_defaults()
        return cls.nova

    @classmethod
    def get_nova_api_version(cls):
        if not cls.nova:
            # is read from the nova section, so if nova is not loaded we
            # need to make sure there won't be a clash if it defines a
            # different value to the default
            cls.load_defaults()
        return cls.nova_api_version

    @classmethod
    def get_dbs(cls):
        if not cls.dbs:
            cls.load_defaults()
        return cls.dbs

    @classmethod
    def get_nova_client(cls, nova_version=None, creds=None):
        if not nova_version:
            nova_version = cls.get_nova_api_version()
        if not creds:
            creds = cls.get_nova()
        loader = loading.get_plugin_loader("password")
        auth = loader.load_from_options(**creds)
        sess = session.Session(auth=auth)
        return nvclient.Client(nova_version, session=sess)
