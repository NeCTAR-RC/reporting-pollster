#!/usr/bin/env python
import copy
import datetime
import pickle
import unittest

from mock import MagicMock
from mock import patch

from reporting_pollster.entities.entities import Aggregate
from reporting_pollster.entities.entities import Allocation
from reporting_pollster.entities.entities import Hypervisor
from reporting_pollster.entities.entities import Instance
from reporting_pollster.entities.entities import Project


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
        "id": 1,
        "availability_zone": "az1",
        "name": "test-1 aggregate in cell test",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "test01",
            "test02",
            "test03",
        ]
    },
    {
        "id": 2,
        "availability_zone": "az2",
        "name": "test-2 aggregate in cell test",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "test04",
            "test05",
            "test06",
        ]
    },
    {
        "id": 3,
        "name": "test cell",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "test-1",
            "test-2",
            "test-3",
        ]
    },
    {
        "id": 4,
        "name": "prod cell",
        "created_at": "today",
        "deleted_at": "never",
        "deleted": False,
        "hosts": [
            "prod-01",
            "prod-02",
            "prod-03",
        ]
    },
    {
        "id": 5,
        "name": "site-1 cell",
        "created_at": "yesterday",
        "deleted_at": "today",
        "deleted": True,
        "hosts": [
            "s101",
            "s102",
            "s103",
        ]
    }
]


hypervisor_data = [
    {
        "id": "nectar!test!test-1@1",
        "host": "test01",
        "hypervisor_hostname": "test01.example.com",
        "host_ip": "1.2.3.1",
        "vcpus": 32,
        "memory_mb": 256 * 1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@1",
        "host": "test02",
        "hypervisor_hostname": "test02.example.com",
        "host_ip": "1.2.3.2",
        "vcpus": 32,
        "memory_mb": 256 * 1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@3",
        "host": "test03",
        "hypervisor_hostname": "test03.example.com",
        "host_ip": "1.2.3.3",
        "vcpus": 32,
        "memory_mb": 256 * 1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@4",
        "host": "test04",
        "hypervisor_hostname": "test04.example.com",
        "host_ip": "1.2.3.4",
        "vcpus": 32,
        "memory_mb": 256 * 1024,
        "local_gb": 2000,
    },
    {
        "id": "nectar!test!test-1@5",
        "host": "test05",
        "hypervisor_hostname": "test05.example.com",
        "host_ip": "1.2.3.5",
        "vcpus": 32,
        "memory_mb": 256 * 1024,
        "local_gb": 2000,
    },
]

hypervisor_az_data = {
    "test01": "az1",
    "test02": "az1",
    "test03": "az1",
    "test04": "az2",
    "test05": "az2",
}


proj_db_data = [
    {
        'id': 'uuid1',
        'display_name': 'Project 1',
        'description': 'The first project',
        'enabled': True,
        'personal': False,
        'has_instances': False,
        'quota_instances': 100,
        'quota_vcpus': 200,
        'quota_memory': 2048 * 1024 * 1024,
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
        'has_instances': False,
        'quota_instances': 50,
        'quota_vcpus': 100,
        'quota_memory': 2048 * 1024 * 1024,
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
        'has_instances': False,
        'quota_instances': 2,
        'quota_vcpus': 2,
        'quota_memory': 2048 * 1024,
        'quota_volume_total': 0,
        'quota_snapshots': 0,
        'quota_volume_count': 0,
    },
]


instance_data = [
    {
        'project_id': 'uuid1',
        'uuid': 'i_uuid1',
        'name': 'instance 1',
        'vcpus': 1,
        'memory': 2048,
        'root': 20,
        'ephemeral': 50,
        'flavour': 'flavour1',
        'created_by': 'user1',
        'created': datetime.datetime(2015, 11, 22, 13, 56),
        'deleted': False,
        'active': True,
        'hypervisor': 'test03',
        'availability_zone': 'test01',
        'cell_name': 'test01!cell!1'
    },
    {
        'project_id': 'uuid3',
        'uuid': 'i_uuid2',
        'name': 'instance 2',
        'vcpus': 1,
        'memory': 2048,
        'root': 20,
        'ephemeral': 50,
        'flavour': 'flavour1',
        'created_by': 'user5',
        'created': datetime.datetime(2015, 11, 23, 0, 1),
        'deleted': False,
        'active': True,
        'hypervisor': 'test04',
        'availability_zone': 'test01',
        'cell_name': 'test01!cell!2'
    },
    {
        'project_id': 'uuid1',
        'uuid': 'i_uuid3',
        'name': 'instance 3',
        'vcpus': 4,
        'memory': 8096,
        'root': 20,
        'ephemeral': 150,
        'flavour': 'flavour2',
        'created_by': 'user1',
        'created': datetime.datetime(2015, 11, 23, 5, 5),
        'deleted': datetime.datetime(2015, 11, 24, 19, 40),
        'active': False,
        'hypervisor': 'test05',
        'availability_zone': 'test01',
        'cell_name': 'test01!cell!3'
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


format_query_orig = (
    "select one, two, three, four "
    "from {nova}.test "
    "where four = %s and three >= %s"
    )

format_query_correct = (
    "select one, two, three, four "
    "from not_nova.test "
    "where four = %s and three >= %s"
    )

alloc_data = [
    {
        'id': 1,
        'project_id': 'uuid1',
        'project_name': 'tenant 1',
        'contact_email': 'user@foo.bar',
        'approver_email': 'admin.user@foo.bar',
        'status': 'A',
        'modified_time': 'now',
        'field_of_research_1': '1234',
        'for_percentage_1': 40,
        'field_of_research_2': '2345',
        'for_percentage_2': 30,
        'field_of_research_3': '3456',
        'for_percentage_3': 30,
        'funding_national': 100,
        'funding_node': None,
    },
    {
        'id': 2,
        'project_id': 'uuid2',
        'project_name': 'tenant 2',
        'contact_email': 'user2@foo.bar',
        'approver_email': 'admin.user@foo.bar',
        'status': 'A',
        'modified_time': 'now',
        'field_of_research_1': '1234',
        'for_percentage_1': 40,
        'field_of_research_2': '2345',
        'for_percentage_2': 30,
        'field_of_research_3': '3456',
        'for_percentage_3': 30,
        'funding_national': 100,
        'funding_node': None,
    },
    {
        'id': 3,
        'project_id': 'uuid3',
        'project_name': 'tenant 3',
        'contact_email': 'user@foo.baz',
        'approver_email': 'admin.user@foo.baz',
        'status': 'A',
        'modified_time': 'now',
        'field_of_research_1': '1234',
        'for_percentage_1': 40,
        'field_of_research_2': '2345',
        'for_percentage_2': 30,
        'field_of_research_3': '3456',
        'for_percentage_3': 30,
        'funding_national': 100,
        'funding_node': None,
    },
    {
        'id': 4,
        'project_id': 'uuid3',
        'project_name': 'tenant 3',
        'contact_email': 'user@foo.baz',
        'approver_email': 'admin.user@foo.baz',
        'status': 'A',
        'modified_time': 'now',
        'field_of_research_1': '1234',
        'for_percentage_1': 40,
        'field_of_research_2': '2345',
        'for_percentage_2': 30,
        'field_of_research_3': '3456',
        'for_percentage_3': 30,
        'funding_national': 100,
        'funding_node': None,
    },
    {
        'id': 5,
        'project_id': None,
        'project_name': 'tenant 4',
        'contact_email': 'user@foo.baz',
        'approver_email': 'admin.user@foo.baz',
        'status': 'A',
        'modified_time': 'now',
        'field_of_research_1': '1234',
        'for_percentage_1': 40,
        'field_of_research_2': '2345',
        'for_percentage_2': 30,
        'field_of_research_3': '3456',
        'for_percentage_3': 30,
        'funding_national': 100,
        'funding_node': None,
    },
    {
        'id': 6,
        'project_id': '',
        'project_name': 'tenant 5',
        'contact_email': 'user@foo.baz',
        'approver_email': 'admin.user@foo.baz',
        'status': 'A',
        'modified_time': 'now',
        'field_of_research_1': '1234',
        'for_percentage_1': 40,
        'field_of_research_2': '2345',
        'for_percentage_2': 30,
        'field_of_research_3': '3456',
        'for_percentage_3': 30,
        'funding_national': 100,
        'funding_node': None,
    },

]

project_allocations = [
    {
        'project_id': 'uuid1',
        'allocation_id': 1,
    },
    {
        'project_id': 'uuid2',
        'allocation_id': 2,
    },
    {
        'project_id': 'uuid3',
        'allocation_id': 4,
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
    @patch('novaclient.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_aggregate_transform(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "nothing"}
        Config.get_nova_api_version.return_value = '2'
        nvclient.Client.return_value = "novaclient"
        agg = Aggregate(self.args)
        agg.api_data = create_mock_array(aggregate_data)
        agg.transform()
        hyp_az_data = Aggregate._get_cached_data('hypervisor_az')
        self.assertEqual(len(agg.agg_data), 5)
        self.assertEqual(len(agg.agg_host_data), 15)
        self.assertEqual(agg.agg_data[0]['active'], True)
        self.assertEqual(agg.agg_data[-1]['active'], False)
        self.assertEqual(agg.agg_host_data[0], {
                         'id': 1,
                         'availability_zone': 'az1',
                         'host': 'test01'})
        self.assertEqual(hyp_az_data['test01'], 'az1')
        self.assertEqual(hyp_az_data['test05'], 'az2')

    @patch('novaclient.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_hypervisor_transform(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "Nothing"}
        Config.get_nova_api_version.return_value = '2'
        nvclient.Client.return_value = "novaclient"
        hyp = Hypervisor(self.args)
        hyp.api_data = create_mock_array(hypervisor_data)
        hyp.hypervisor_az_data = hypervisor_az_data
        hyp.transform()
        self.assertEqual(len(hyp.data), 5)
        self.assertEqual(hyp.data[0]['availability_zone'], 'az1')
        self.assertEqual(hyp.data[4]['availability_zone'], 'az2')

    @patch('novaclient.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_project_transform(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "Nothing"}
        Config.get_nova_api_version.return_value = '2'
        nvclient.Client.return_value = "novaclient"
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

    @patch('novaclient.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_instance_transform(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "Nothing"}
        nvclient.Client.return_value = "novaclient"
        inst = Instance(self.args)
        inst.db_data = copy.deepcopy(instance_data)
        inst.transform()
        self.assertEqual(len(inst.has_instance_data), 2)
        self.assertEqual(inst.has_instance_data['uuid1'], True)
        self.assertEqual(inst.hist_agg_data[0]['vcpus'], 1)
        self.assertEqual(inst.hist_agg_data[1]['vcpus'], 6)
        self.assertEqual(inst.hist_agg_data[2]['vcpus'], 2)
        self.assertEqual(inst.hist_agg_data[0]['memory'], 2048)
        self.assertEqual(inst.hist_agg_data[1]['memory'], 12192)
        self.assertEqual(inst.hist_agg_data[2]['memory'], 4096)
        self.assertEqual(inst.hist_agg_data[0]['local_storage'], 70)
        self.assertEqual(inst.hist_agg_data[1]['local_storage'], 310)
        self.assertEqual(inst.hist_agg_data[2]['local_storage'], 140)
        # the copy of the original data should not have changed
        self.assertEqual(inst.data, instance_data)

    @patch('novaclient.client')
    @patch('reporting_pollster.entities.entities.Config')
    def test_format_query(self, Config, nvclient):
        Config.get_nova.return_value = {"Creds": "Nothing"}
        Config.get_dbs.return_value = {"nova": "not_nova"}
        Config.get_nova_api_version.return_value = '2'
        nvclient.Client.return_value = "novaclient"
        # can't instantiate Entity, since it's abstract;
        # can use any subclass that does not override _format_query
        entity = Aggregate(self.args)
        entity.queries = {"testing": format_query_orig}
        self.assertEqual(entity._format_query('testing'), format_query_correct)

    @patch('reporting_pollster.entities.entities.Config')
    def test_allocation_transform(self, Config):
        Config.get_dbs.return_value = {"Creds": "Nothing"}
        allocs = Allocation(self.args)
        allocs.db_data = copy.deepcopy(alloc_data)
        allocs.tenant_allocation_data = copy.deepcopy(project_allocations)
        allocs.transform()
        self.assertEqual(len(allocs.data), 5)
        self.assertEqual(allocs.data[0]['project_id'], 'uuid1')
        self.assertEqual(allocs.data[2]['project_id'], 'uuid3')
        self.assertEqual(allocs.data[2]['id'], 4)
        self.assertIsNone(allocs.data[3]['project_id'])
        self.assertIsNone(allocs.data[4]['project_id'])


if __name__ == '__main__':
    unittest.main()
