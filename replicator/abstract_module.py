class AbstractModule:

    def check_configuration(self, config_parser):
        """
        Stop the service if the configuration it's not respected
        """
        raise NotImplementedError

    def __index__(self, config_parser):
        raise NotImplementedError

    def insert(self, index, doc_type, id, doc):
        raise NotImplementedError

    def update(self, index, doc_type, id, doc):
        raise NotImplementedError

    def delete(self, index, doc_type, id):
        raise NotImplementedError

    @property
    def server_information(self):
        """
        Return the information that the server could display.
        """
