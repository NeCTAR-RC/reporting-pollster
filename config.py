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

    def __init__(self, filename=None):
        print "Initialising configuration"
        self.remote = {}
        self.local = {}

        if not filename:
            self.remote = remote.copy()
            self.local = local.copy()
        else:
            self.load_config(filename)

    def load_config(self, filename=config_file):
        print "Loading configuration from " + filename
        parser = SafeConfigParser()
        parser.read(filename)
        if not parser.has_section('remote'):
            print "No remote database configuration - failing"
            sys.exit(1)
        if not parser.has_section('local'):
            print "No local database configuration - failing"
            sys.exit(1)
        self.remote = {}
        self.local = {}
        for (name, value) in parser.items('remote'):
            self.remote[name] = value
        for (name, value) in parser.items('local'):
            self.local[name] = value

    def get_remote(self):
        return self.remote

    def get_local(self):
        return self.local
