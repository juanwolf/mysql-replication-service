from configparser import ConfigParser
from unittest import TestCase

from replicator.replicator import Replicator


class ReplicatorTest(TestCase):

    def test_main_init_should_contain_an_array_of_tables_with_one_element(self):
        # Given
        config_parser = ConfigParser()
        config_parser['mysql'] = {
            'host' : 'test_host',
            'port' : '1111',
            'user' : 'user',
            'password' : 'password',
            'server_id' : '1',
            'tables' : 'account',
            'index_label' : 'id'
        }
        config_parser['core'] = {
            'modules_path' : 'path/to/modules'
        }

        # When
        replicator = Replicator(config_parser)

        # Then
        self.assertEqual(replicator.tables, ["account"])

    def test_main_init_should_contain_an_array_of_tables_with_two_element(self):
        # Given
        config_parser = ConfigParser()
        config_parser['mysql'] = {
            'host' : 'test_host',
            'port' : '1111',
            'user' : 'user',
            'password' : 'password',
            'server_id' : '1',
            'tables' : 'account, testbase',
            'index_label' : 'id'
        }
        config_parser['core'] = {
            'modules_path' : 'path/to/modules'
        }

        # When
        replicator = Replicator(config_parser)

        # Then
        self.assertEqual(replicator.tables, ["account", 'testbase'])

