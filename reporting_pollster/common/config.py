#
# Config setup
#

import os.path
import sys
import logging
from ConfigParser import SafeConfigParser
from reporting_pollster.common import credentials
from novaclient import client as nvclient

config_file = "./reporting.conf"

# defaults for testing
remote = {
    'user': 'reporting-test',
    'password': 'Testing out the system',
    'database': 'reporting2',
    'host': '127.0.0.1',
    'port': 33306,
}

local = {
    'user': 'reporting-test',
    'password': 'Testing out the system',
    'database': 'reporting2',
    'host': '127.0.0.1',
    'port': 3306,
}

dbs = {
    'keystone': 'keystone',
    'nova': 'nova',
    'cinder': 'cinder',
    'glance': 'glance',
    'rcshibboleth': 'rcshibboleth',
}


def verify_nova_creds(nova_version, creds):
    client = nvclient.Client(nova_version, **creds)
    # will return success quickly or fail quickly
    logging.debug("Testing nova credentials")
    try:
        client.availability_zones.list()
    except Exception as e:
        logging.critical("Exception returned: %s", e.message)
        sys.exit(1)


class Config(object):
    """
    Configuration wrapper class.
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
            logging.critical("Configuration file not found - failing")
            sys.exit(1)
        cls.remote = None
        cls.local = None
        cls.nova = None
        parser = SafeConfigParser()
        parser.read(filename)
        if not parser.has_section('remote'):
            logging.critical("No remote database configuration - failing")
            sys.exit(1)
        if not parser.has_section('local'):
            logging.critical("No local database configuration - failing")
            sys.exit(1)
        if not parser.has_section('nova') and cls.nova is None:
            logging.critical("No nova credentials provided - failing")
            sys.exit(1)
        cls.remote = {}
        cls.local = {}
        cls.nova = {}
        cls.dbs = {}
        for (name, value) in parser.items('remote'):
            cls.remote[name] = value
        for (name, value) in parser.items('local'):
            cls.local[name] = value
        for (name, value) in parser.items('nova'):
            cls.nova[name] = value
        cls.extract_nova_version()
        if not parser.has_section('databases'):
            logging.info("No database mapping defined - using default")
            cls.dbs = dbs
        else:
            for (name, value) in parser.items('databases'):
                cls.dbs[name] = value
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
            cls.remote = remote
            cls.local = local
            cls.dbs = dbs
            try:
                cls.nova = credentials.get_nova_credentials()
                cls.extract_nova_version()
            except KeyError:
                logging.critical(
                    "Loading nova credentials from environment failed"
                )
                sys.exit(1)
        verify_nova_creds(cls.nova_api_version, cls.nova)

    @classmethod
    def extract_nova_version(cls):
        if cls.nova['version']:
            cls.nova_api_version = cls.nova['version']
            del cls.nova['version']

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
