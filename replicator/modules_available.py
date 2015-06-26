from modules.elasticsearch_module import ElasticsearchModule

__author__ = 'juanwolf'

"""
Write here the identifier you want in the config file to generate automatically an instance of your module during
the replication.
A little example, I want that during my replication all the data are sent to an elasticsearch then
modules_available = {
  'elasticsearch' = ElasticsearchModule
}
"""
modules_available = {
    'elasticsearch' : ElasticsearchModule
}