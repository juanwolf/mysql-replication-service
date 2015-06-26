from configparser import ConfigParser
import unittest
from modules_manager import ModulesDetector


class ModuleDetectorTest(unittest.TestCase):

    def setUp(self):
        config_parser = ConfigParser()
        config_parser.read('resources/conf/config.ini')
        self.module_detector = ModulesDetector(config_parser)


    def test_modules_detector_should_find_the_elasticsearch_module(self):
        # given
        # when
        # then
        self.assertIn('elasticsearch', self.module_detector.modules_available)
