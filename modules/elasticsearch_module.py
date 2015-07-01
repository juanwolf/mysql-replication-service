from elasticsearch import Elasticsearch
from replicator.abstract_module import AbstractModule


class ElasticsearchModule(AbstractModule):
    """
    Override the elastic API. (Not really usefull for the moment)
    """
    def __init__(self, config_parser):
        self.SETTINGS = {
            "host": config_parser.get('elasticsearch', 'host'),
            "port": config_parser.get('elasticsearch', 'port')
        }
        self.es_stream = Elasticsearch()
        self.insert_number = 0
        self.update_number = 0
        self.delete_number = 0

    def insert(self, index, doc_type, id, doc):
        self.es_stream.index(index, doc_type, doc, id)
        self.insert_number += 1

    def delete(self, index, doc_type, id):
        self.es_stream.delete(index, doc_type, id)
        self.delete_number += 1

    def update(self, index, doc_type, id, doc):
        self.insert(index, doc_type, id, doc)
        self.update_number += 1

    @property
    def server_information(self):
        return {
            'elastic_avalaible': self.es_stream.ping(),
            'insert_number': self.insert_number,
            'update_number': self.update_number,
            'delete_number': self.delete_number
        }