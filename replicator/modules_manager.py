from configparser import ConfigParser
import os
import re
from modules.elasticsearch_module import ElasticsearchModule
import modules_available


class ModulesManager:
    def __init__(self, config_parser=None):
        self.config_parser = config_parser
        self.modules_path = self.config_parser.get('core', 'modules_path')

    @property
    def modules_available(self):
        modules_found = []
        for file in os.listdir(self.modules_path):
            match = re.search('(\w+)_module.py', file)
            if match:
                modules_found.append(match.group(1))
        return modules_found

    def generate_modules_instances(self):
        self.instances = []

        for module in self.modules_available:
            if module in self.config_parser.sections():
                instance = modules_available.modules_available[module](self.config_parser)
                self.instances.append(instance)



if __name__ == "__main__":
    config_parser = ConfigParser()
    config_parser.read('resources/conf/config.ini')
    modules_manager = ModulesManager(config_parser)
    modules_manager.generate_modules_instances()
    print(modules_manager.instances)