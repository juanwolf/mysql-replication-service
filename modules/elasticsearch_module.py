from elasticsearch import Elasticsearch
from replicator.abstract_module import AbstractModule


class ElasticsearchModule(AbstractModule):
    """
    Override the elastic API. (Not really usefull for the moment)
    """

    def load_settings(self, config_parser):
        self.SETTINGS = {
            "host": config_parser.get('elasticsearch', 'host'),
            "port": config_parser.get('elasticsearch', 'port')
        }

    def __init__(self, config_parser):
        self.load_settings(config_parser)
        self.es_stream = Elasticsearch()

    def insert(self, index, doc_type, id, doc):
        self.es_stream.index(index, doc_type, doc, id)

    def delete(self, index, doc_type, id):
        self.es_stream.delete(index, doc_type, id)

    def update(self, index, doc_type, id, doc):
        self.insert(index, doc_type, id, doc)