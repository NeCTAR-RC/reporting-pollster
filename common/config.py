#
# Config setup
#

import sys
from ConfigParser import SafeConfigParser

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

    def __init__(self):
        self.load_config()

    @classmethod
    def load_config(cls, filename=config_file):
        if cls.remote and cls.local:
            return
        print "Loading configuration from " + filename
        parser = SafeConfigParser()
        parser.read(filename)
        if not parser.has_section('remote'):
            print "No remote database configuration - failing"
            sys.exit(1)
        if not parser.has_section('local'):
            print "No local database configuration - failing"
            sys.exit(1)
        cls.remote = {}
        cls.local = {}
        for (name, value) in parser.items('remote'):
            cls.remote[name] = value
        for (name, value) in parser.items('local'):
            cls.local[name] = value

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
