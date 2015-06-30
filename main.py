#!/bin/python3
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
import logging
import cherrypy
import click
from replicator.replicator import Replicator
from server.server import ReplicatorServer

__author__ = 'juanwolf'

@click.command()
@click.option('--server',is_flag=True, help="Run a little cherrypy server to check stats of the replication.")
def main(server):
    """
    Run the mysql replicator.
    """
    config_parser = ConfigParser()
    config_parser.read('resources/conf/config.ini')
    replicator = Replicator(config_parser)

    executor = ThreadPoolExecutor(max_workers = 2)
    executor.submit(replicator.start)

    logger = logging.getLogger('replicator')
    if server:
        if config_parser.has_section('server') :
            server_config = {
                'server.socket_host' : config_parser.get('server', 'socket_host'),
                'server.socket_port' : config_parser.getint('server', 'socket_port'),
                'log.access_file' : config_parser.get('server', 'log.access_file'),
                'log.error_file' : config_parser.get('server', 'log.error_file')
            }
            cherrypy.config.update(server_config)
            logger.info('Server will be started at %s:%d' % (server_config['server.socket_host'],
                                                             server_config['server.socket_port']))
        else:
            logger.info('No settings found, using the default one. (localhost:8080)')
        executor.submit(cherrypy.quickstart(ReplicatorServer(replicator)))


if __name__ == '__main__':
    main()