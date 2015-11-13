#
# Config setup
#

import os.path
import sys
from ConfigParser import SafeConfigParser
from reporting_pollster.common import credentials
import novaclient.v2.client as nvclient

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

def verify_nova_creds(creds):
    client = nvclient.Client(**creds)
    # will return success quickly or fail quickly
    print "Testing nova credentials"
    try:
        client.availability_zones.list()
    except Exception as e:
        print "Credentials don't appear to be valid"
        print "Exception returned:", e
        sys.exit(1)


class Config(object):
    """
    Configuration wrapper class.
    """
    remote = None
    local = None
    nova = None
    config_file = None

    def __init__(self):
        self.load_defaults()

    @classmethod
    def reload_config(cls, filename):
        cls.config_file = filename
        print "Loading configuration from " + filename
        if not os.path.isfile(filename):
            print "Config file not found - failing"
            sys.exit(1)
        cls.remote = None
        cls.local = None
        cls.nova = None
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
        verify_nova_creds(cls.nova)

    @classmethod
    def load_config(cls, filename):
        if cls.remote and cls.local and cls.nova:
            return
        cls.reload_config(filename)

    @classmethod
    def load_defaults(cls):
        if cls.config_file and os.path.isfile(cls.config_file):
            cls.reload_config(cls.config_file)
        else:
            print "loading in-built default configuration"
            cls.remote = remote
            cls.local = local
            try:
                cls.nova = credentials.get_nova_credentials()
            except KeyError:
                print "Loading nova credentials from environment failed"
                sys.exit(1)
        verify_nova_creds(cls.nova)

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
