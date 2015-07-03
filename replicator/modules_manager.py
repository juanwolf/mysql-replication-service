from configparser import ConfigParser
import os
import re
from modules.elasticsearch_module import ElasticsearchModule


"""
Write here the identifier you want in the config file to generate automatically an instance of your module during
the replication.
A little example, I want that during my replication all the data are sent to an elasticsearch then
modules_available = {
  'elasticsearch' = ElasticsearchModule
}
"""
modules_available = {
    'elasticsearch': ElasticsearchModule
}

class ModulesManager:
    """
    Manage the modules in the project. The manager will instantiate a module if it's contained in the config file.
    CONTRIBUTORS : Be sure, you've added the module in the module_available variable, or the manager will not be
    able to link it to a section in the config_file.
    """
    def __init__(self, config_parser):
        self.config_parser = config_parser
        self.modules_path = self.config_parser.get('core', 'modules_path')
        self.instances = []

    @property
    def modules_available(self):
        modules_found = []
        for file in os.listdir(self.modules_path):
            match = re.search('(\w+)_module.py', file)
            if match:
                modules_found.append(match.group(1))
        return modules_found

    def generate_modules_instances(self):
        for module in self.modules_available:
            if module in self.config_parser.sections():
                instance = modules_available[module](self.config_parser)
                self.instances.append(instance)

    def insert_data_all_modules(self, index, doc_type, id, doc):
        for module in self.instances:
            module.insert(index, doc_type, id, doc)

    def update_data_all_modules(self, index, doc_type, id, doc):
        for module in self.instances:
            module.insert(index, doc_type, id, doc)

    def remove_data_all_modules(self, index, doc_type, id):
        for module in self.instances:
            module.delete(index, doc_type, id)

    def get_module_information(self, module_name):
        for instance in self.instances:
            if isinstance(instance, modules_available[module_name]):
                return instance.server_information
        return None

if __name__ == "__main__":
    config_parser = ConfigParser()
    config_parser.read('resources/conf/config.ini')
    modules_manager = ModulesManager(config_parser)
    modules_manager.generate_modules_instances()
    print(modules_manager.instances)