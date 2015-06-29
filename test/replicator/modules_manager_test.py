from configparser import ConfigParser
import unittest
from modules.elasticsearch_module import ElasticsearchModule
from modules_manager import ModulesManager

class ModuleDetectorTest(unittest.TestCase):

    def setUp(self):
        config_parser = ConfigParser()
        config_parser.read('test/resources/config.ini')
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
        self.assertIsInstance(self.modules_manager.instances[0], ElasticsearchModule)
