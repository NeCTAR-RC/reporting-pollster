#
# Not used at the moment, but will be needed later
#

import os


def get_nova_credentials():
    d = {}
    d['version'] = '2'
    d['username'] = os.environ['OS_USERNAME']
    try:
        d['user_domain_name'] = os.environ['OS_USER_DOMAIN_NAME']
    except KeyError:
        d['user_domain_name'] = 'default'
    d['password'] = os.environ['OS_PASSWORD']
    d['auth_url'] = os.environ['OS_AUTH_URL']
    d['project_name'] = os.environ['OS_PROJECT_NAME']
    try:
        d['project_domain_name'] = os.environ['OS_PROJECT_DOMAIN_NAME']
    except KeyError:
        d['project_domain_name'] = 'default'
    return d


def get_keystone_credentials():
    d = {}
    d['username'] = os.environ['OS_USERNAME']
    d['password'] = os.environ['OS_PASSWORD']
    d['auth_url'] = os.environ['OS_AUTH_URL']
    d['tenant_name'] = os.environ['OS_TENANT_NAME']
    return d
