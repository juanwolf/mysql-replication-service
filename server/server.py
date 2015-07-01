import cherrypy


class ReplicatorServer(object):

    def __init__(self, replicator):
        self.replicator = replicator

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        main_json = {
            'transaction_number': self.replicator.transaction_manager.number_of_transactions,
            'creation_number':  self.replicator.transaction_manager.number_of_create_request,
            'deletion_number': self.replicator.transaction_manager.number_of_delete_request,
            'update_number': self.replicator.transaction_manager.number_of_update_request,
            'latency': self.replicator.transaction_manager.latency
        }
        modules_json = {}
        for module in self.replicator.modules_manager.modules_available:
            modules_json = { module: self.replicator.modules_manager.get_module_information(module)
                             for module in self.replicator.modules_manager.modules_available }
        main_json['modules'] = modules_json
        return main_json



if __name__ == '__main__':
    cherrypy.quickstart(ReplicatorServer())
