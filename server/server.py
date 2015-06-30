import cherrypy


class ReplicatorServer(object):

    def __init__(self, replicator):
        self.replicator = replicator

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def index(self):
        return {
            'transaction_numberNumber' : self.replicator.transaction_manager.number_of_transactions,
            'creation_number' :  self.replicator.transaction_manager.number_of_create_request,
            'deletion_number' : self.replicator.transaction_manager.number_of_delete_request,
            'update_number' : self.replicator.transaction_manager.number_of_update_request,
            'latency' : self.replicator.transaction_manager.latency,
        }



if __name__ == '__main__':
    cherrypy.quickstart(ReplicatorServer())
