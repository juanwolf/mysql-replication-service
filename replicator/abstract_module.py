class AbstractModule:

    def load_settings(self):
        raise NotImplementedError

    def insert(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError
