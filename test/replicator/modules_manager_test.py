from configparser import ConfigParser
import unittest
from modules.elasticsearch_module import ElasticsearchModule
from replicator.modules_manager import ModulesManager

class ModuleDetectorTest(unittest.TestCase):

    def setUp(self):
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
        config_parser['elasticsearch'] = {
            'host' : 'ddd',
            'port' : '1111'
        }
        config_parser['core'] = {
            'modules_path' : '/home/juanwolf/devel/mysql-elasticsearch-replicator/modules/'
        }
        self.modules_manager = ModulesManager(config_parser)


    def test_modules_manager_should_find_the_elasticsearch_module(self):
        # given
        # when
        # then
        self.assertIn('elasticsearch', self.modules_manager.modules_available)

    def test_modules_manager_should_create_instance_of_es_module(self):
        # given
        # when
        self.modules_manager.generate_modules_instances()
        # then
        self.assertEqual(len(self.modules_manager.instances), 1)
        self.assertIsInstance(self.modules_manager.instances[0], ElasticsearchModule)

if __name__ == '__main__':
    unittest.main()
