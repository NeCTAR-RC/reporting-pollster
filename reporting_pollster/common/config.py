#
# Config setup
#

import os.path
import sys
from ConfigParser import SafeConfigParser
from reporting_pollster.common import credentials

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


class Config(object):
    """
    Configuration wrapper class.
    """
    remote = None
    local = None
    nova = None

    def __init__(self):
        self.load_defaults()

    @classmethod
    def reload_config(cls, filename):
        print "Loading configuration from " + filename
        if not os.path.isfile(filename):
            print "Config file not found - failing"
            sys.exit(1)
        cls.remote = None
        cls.local = None
        parser = SafeConfigParser()
        parser.read(filename)
        if not parser.has_section('remote'):
            print "No remote database configuration - failing"
            sys.exit(1)
        if not parser.has_section('local'):
            print "No local database configuration - failing"
            sys.exit(1)
        if not parser.has_section('nova') and cls.nova is None:
            print "No nova credentials provided - failing"
            sys.exit(1)
        cls.remote = {}
        cls.local = {}
        cls.nova = {}
        for (name, value) in parser.items('remote'):
            cls.remote[name] = value
        for (name, value) in parser.items('local'):
            cls.local[name] = value
        for (name, value) in parser.items('nova'):
            cls.nova[name] = value

    @classmethod
    def load_config(cls, filename):
        if cls.remote and cls.local:
            return
        cls.reload_config(filename)

    @classmethod
    def load_defaults(cls):
        # pull credentials from the environment, but override with the config
        # file
        try:
            cls.nova = credentials.get_nova_credentials()
        except KeyError:
            print "Loading nova credentials from environment failed"
        if os.path.isfile(config_file):
            cls.reload_config(config_file)
        else:
            print "loading in-built default configuration"
            cls.remote = remote
            cls.local = local
            cls.nova = {}

    @classmethod
    def get_remote(cls):
        if not cls.remote:
            cls.load_config()
        return cls.remote

    @classmethod
    def get_local(cls):
        if not cls.local:
            cls.load_config()
        return cls.local

    @classmethod
    def get_nova(cls):
        if not cls.nova:
            cls.load_config()
        return cls.nova
