from elasticsearch import Elasticsearch


class ElasticSender:
    """
    Override the elastic API. (Not really usefull for the moment)
    """

    def __init__(self):
        self.es_stream = Elasticsearch()

    def insert_or_update(self, index, doc_type, id, doc):
        self.es_stream.index(index, doc_type, doc, id)

    def delete(self, index, doc_type, id):
        self.es_stream.delete(index, doc_type, id)
