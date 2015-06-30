from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser
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
    if server:
        executor.submit(cherrypy.quickstart(ReplicatorServer(replicator)))


if __name__ == '__main__':
    main()