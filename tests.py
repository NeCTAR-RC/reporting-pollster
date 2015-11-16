#!/usr/bin/env python

import unittest
from mock import MagicMock, patch
from reporting_pollster.entities.entities import Aggregate, Hypervisor, Project
import pickle

# What to test . . .
#
# This code is almost completely involved with database to database
# translation, which makes it hard to do tests. Fortunately most of the basic
# code is simple and can live without tests (though they'd be nice); the
# interesting stuff that needs testing is generally the transformation code,
# which takes the extracted data and munges it into something else. That's what
# I'll target with this initial test suite.

aggregate_data = [
    {
        "id": "nectar!test!test-1@1",
        "name": "test-1 aggregate in cell test",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "test1.example.com",
            "test2.example.com",
            "test3.example.com",
        ]
    },
    {
        "id": "nectar!test!test-2@2",
        "name": "test-2 aggregate in cell test",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "test4.example.com",
            "test5.example.com",
            "test6.example.com",
        ]
    },
    {
        "id": "nectar!test@1",
        "name": "test cell",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "test-1.example.com",
            "test-2.example.com",
            "test-3.example.com",
        ]
    },
    {
        "id": "nectar!prod-1@1",
        "name": "prod cell",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "prod-01.example.com",
            "prod-02.example.com",
            "prod-03.example.com",
        ]
    },
    {
        "id": "nectar!site-1@1",
        "name": "site-1 cell",
        "created_at": "yesterday",
        "deleted_at": "today",
        "deleted": True,
        "hosts": [
            "s101.example.com",
            "s102.example.com",
            "s103.example.com",
        ]
    }
]


hypervisor_data = [
    {
        "id": "nectar!test!test-1@1",
        "hypervisor_hostname": "test01.example.com",
        "host_ip": "1.2.3.1",
        "vcpus": 32,
        "memory_mb": 256*1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@1",
        "hypervisor_hostname": "test02.example.com",
        "host_ip": "1.2.3.2",
        "vcpus": 32,
        "memory_mb": 256*1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@3",
        "hypervisor_hostname": "test03.example.com",
        "host_ip": "1.2.3.3",
        "vcpus": 32,
        "memory_mb": 256*1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@4",
        "hypervisor_hostname": "test04.example.com",
        "host_ip": "1.2.3.4",
        "vcpus": 32,
        "memory_mb": 256*1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@5",
        "hypervisor_hostname": "test05.example.com",
        "host_ip": "1.2.3.5",
        "vcpus": 32,
        "memory_mb": 256*1024,
        "local_gb": 2000,
    },
]


proj_db_data = [
    {
        'id': 'uuid1',
        'display_name': 'Project 1',
        'description': 'The first project',
        'enabled': True,
        'personal': False,
        'quota_instances': 100,
        'quota_vcpus': 200,
        'quota_memory': 2048*1024*1024,
        'quota_volume_total': 100000,
        'quota_snapshots': 35,
        'quota_volume_count': 50,
    },
    {
        'id': 'uuid2',
        'display_name': 'Project 2',
        'description': 'The second project',
        'enabled': True,
        'personal': False,
        'quota_instances': 50,
        'quota_vcpus': 100,
        'quota_memory': 2048*1024*1024,
        'quota_volume_total': 500000,
        'quota_snapshots': 35,
        'quota_volume_count': 20,
    },
    {
        'id': 'uuid3',
        'display_name': 'pt-1',
        'description': 'pt for someone',
        'enabled': True,
        'personal': True,
        'quota_instances': 2,
        'quota_vcpus': 2,
        'quota_memory': 2048*1024,
        'quota_volume_total': 0,
        'quota_snapshots': 0,
        'quota_volume_count': 0,
    },
]


shib_data1 = {
    'organisation': "A real University",
    'mail': 'someone@somewhere',
}


shib_data2 = {
    'orginisation': "Not a real University",
    'mail': 'someone@somewhere.else',
}


shib_data3 = {
    'mail': 'someone@somewhere.entirely.else',
}


proj_tenant_owner_data = [
    {
        'tenant': 'uuid1',
        'user': 'user1',
        'shib_attr': pickle.dumps(shib_data1),
    },
    {
        'tenant': 'uuid1',
        'user': 'user2',
        'shib_attr': pickle.dumps(shib_data1),
    },
    {
        'tenant': 'uuid1',
        'user': 'user3',
        'shib_attr': pickle.dumps(shib_data1),
    },
    {
        'tenant': 'uuid2',
        'user': 'user4',
        'shib_attr': pickle.dumps(shib_data2),
    },
]


proj_tenant_member_data = [
    {
        'tenant': 'uuid1',
        'user': 'user1',
        'shib_attr': pickle.dumps(shib_data1),
    },
    {
        'tenant': 'uuid1',
        'user': 'user2',
        'shib_attr': pickle.dumps(shib_data1),
    },
    {
        'tenant': 'uuid1',
        'user': 'user3',
        'shib_attr': pickle.dumps(shib_data1),
    },
    {
        'tenant': 'uuid2',
        'user': 'user4',
        'shib_attr': pickle.dumps(shib_data2),
    },
    {
        'tenant': 'uuid3',
        'user': 'user5',
        'shib_attr': pickle.dumps(shib_data3),
    },
]


def create_mock_array(data):
    accum = []
    for i in data:
        accum.append(MagicMock(**i))
    return accum


class test_all(unittest.TestCase):

    default_args = {
        'full_run': False,
    }

    def setUp(self):
        self.args = MagicMock(**self.default_args)

    def tearDown(self):
        del(self.args)

    # The aggregate transform does two things: it converts the aggregates
    # API data into the aggregate table format, and extracts the
    # aggregate_host data at the same time.
    #
    # Input is the Aggregate.api_data array, which is an array of
    # novaclient.v2.aggregates.Aggregate instances. We're using mock to
    # emulate a small chunk of these.
    @patch('novaclient.v2.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_aggregate_transform(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "nothing"}
        nvclient.Client.return_value = "novaclient"
        agg = Aggregate(self.args)
        agg.api_data = create_mock_array(aggregate_data)
        agg.transform()
        self.assertEqual(len(agg.agg_data), 5)
        self.assertEqual(len(agg.agg_host_data), 15)
        self.assertEqual(agg.agg_data[0]['active'], True)
        self.assertEqual(agg.agg_data[-1]['active'], False)
        self.assertEqual(agg.agg_host_data[0], {
                         'id': '1',
                         'availability_zone': 'test!test-1',
                         'host': 'test1'})

    @patch('novaclient.v2.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_hypervisor_transform(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "Nothing"}
        nvclient.Client.return_value = "novaclient"
        hyp = Hypervisor(self.args)
        hyp.api_data = create_mock_array(hypervisor_data)
        hyp.transform()
        self.assertEqual(len(hyp.data), 5)
        self.assertEqual(hyp.data[0]['availability_zone'], 'test!test-1')

    def test_project_transform(self):
        proj = Project(self.args)
        proj.db_data = proj_db_data
        proj.tenant_owner_data = proj_tenant_owner_data
        proj.tenant_member_data = proj_tenant_member_data
        proj.transform()
        self.assertEqual(len(proj.data), 3)
        # project 1 is owned by A real University
        self.assertEqual(proj.data[0]['organisation'],
                         "A real University")
        # project 2 has the stupid misspelling of organisation
        self.assertEqual(proj.data[1]['organisation'],
                         "Not a real University")
        # project 3 is a PT from somewhere.entirely.else
        self.assertEqual(proj.data[-1]['organisation'],
                         "somewhere.entirely.else")
