class AbstractModule:

    def load_settings(self, items):
        raise NotImplementedError

    def insert(self, index, doc_type, id, doc):
        raise NotImplementedError

    def update(self, index, doc_type, id, doc):
        raise NotImplementedError

    def delete(self, index, doc_type, id):
        raise NotImplementedError
