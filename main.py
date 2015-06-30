from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
from threading import Thread
import cherrypy
from replicator.replicator import Replicator
from server import ReplicatorServer

__author__ = 'juanwolf'


config_parser = ConfigParser()
config_parser.read('resources/conf/config.ini')
replicator = Replicator(config_parser)

executor = ThreadPoolExecutor(max_workers = 2)
executor.submit(replicator.start)
executor.submit(cherrypy.quickstart(ReplicatorServer(replicator)))