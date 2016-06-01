#
# Database wrapper - maintain one connection to each target, and provide
# cursor factory functions
#

import logging
import mysql.connector
from reporting_pollster.common.config import Config


class DB(object):
    """
    Wrap the database connections.
    """

    remote_creds = None
    remote_conn = None
    local_creds = None
    local_conn = None

    @classmethod
    def remote(cls):
        if not cls.remote_conn:
            cls.remote_creds = Config.get_remote()
            cls.remote_conn = mysql.connector.connect(**cls.remote_creds)
            logging.debug("Remote server version: %s",
                          cls.remote_conn.get_server_info())
        return cls.remote_conn

    @classmethod
    def remote_cursor(cls, dictionary=True):
        if not cls.remote_conn:
            cls.remote()
        return cls.remote().cursor(dictionary=dictionary)

    @classmethod
    def local(cls):
        if not cls.local_conn:
            cls.local_creds = Config.get_local()
            cls.local_conn = mysql.connector.connect(**cls.local_creds)
            logging.debug("Local server version: %s",
                          cls.local_conn.get_server_info())
        return cls.local_conn

    @classmethod
    def local_cursor(cls, dictionary=True):
        if not cls.local_conn:
            cls.local()
        return cls.local().cursor(dictionary=dictionary)
