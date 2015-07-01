#!/bin/python3
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
import logging
import cherrypy
import click
import sys
from replicator.replicator import Replicator
from server.server import ReplicatorServer

__author__ = 'juanwolf'

log_levels = {
    'CRITICAL': 50,
    'FATAL': 50,
    'ERROR': 40,
    'WARNING': 30,
    'WARN': 30,
    'INFO': 20,
    'DEBUG': 10,
    'DEFAULT': 20
}

def check_server_configuration(config_parser, logger):
    if config_parser.get('server', 'socket_host', fallback=None) is None:
        logger.error('The server section should contain a socket_host value')
        sys.exit(2)
    if config_parser.getint('server', 'socket_port', fallback=None) is None:
        logger.error('The server section should contain a socket_port value')
        sys.exit(2)
    if config_parser.get('server', 'log.access_file', fallback=None) is None:
        logger.error('The server section should contain a log.access_file value')
        sys.exit(2)
    if config_parser.get('server', 'log.error_file', fallback=None) is None:
        logger.error('The server section should contain a log.error_file value')
        sys.exit(2)

@click.command()
@click.option('--server', is_flag=True, help="Run a little cherrypy server to check stats of the replication.")
def main(server):
    """
    Run the mysql replicator.
    """

    config_parser = ConfigParser()
    config_parser.read('resources/conf/config.ini')

    log_level = log_levels.get(config_parser.get('core', 'log.level'))
    log_filename = config_parser.get('core', 'log.path')
    logging.basicConfig(level=log_level,
                        format='%(levelname)s:%(asctime)s:%(message)s',
                        filename=log_filename)
    replicator = Replicator(config_parser)
    executor = ThreadPoolExecutor(max_workers=2)
    executor.submit(replicator.start)
    logger = logging.getLogger('replicator')
    if server:
        if config_parser.has_section('server'):
            check_server_configuration(config_parser, logger)
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