from concurrent.futures import thread
import json
from threading import Thread
import cherrypy
from replicator.replicator import Replicator


class ReplicatorServer(object):

    def __init__(self):
        self.replicator = Replicator()
        self.thread_service = Thread(None, self.replicator.start(), 'thread_service')

    @cherrypy.expose
    def index(self):
        return json.dumps({
            "transaction_numberNumber" : self.replicator.transaction_manager.number_of_transactions,
            "creation_number" :  self.replicator.transaction_manager.number_of_create_request,
            "deletion_number" : self.replicator.transaction_manager.number_of_delete_request,
            "update_number" : self.replicator.transaction_manager.number_of_update_request,
            "latency" : self.replicator.transaction_manager.latency,
        })



if __name__ == '__main__':
    cherrypy.quickstart(ReplicatorServer())
