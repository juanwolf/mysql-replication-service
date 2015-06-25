from configparser import ConfigParser
from elasticsearch import Elasticsearch
from abstract_module import AbstractModule


class ElasticsearchModule(AbstractModule):
    """
    Override the elastic API. (Not really usefull for the moment)
    """

    def load_settings(self):
        parser = ConfigParser()
        parser.read('resources/conf/elasticsearch.ini')
        self.SETTINGS = {
            "host" : parser.get('elasticsearch', 'host'),
            "port" : parser.get('elasticsearch', 'port')
        }

    def __init__(self):
        self.es_stream = Elasticsearch()

    def insert(self, index, doc_type, id, doc):
        self.es_stream.index(index, doc_type, doc, id)

    def delete(self, index, doc_type, id):
        self.es_stream.delete(index, doc_type, id)

    def update(self, index, doc_type, id, doc):
        self.insert(index, doc_type, id, doc)