from configparser import ConfigParser
import json
import logging
import sys

from elasticsearch import Elasticsearch
from pymysqlreplication import BinLogStreamReader

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)

from transaction_manager import TransactionManager

class Replicator:
    def __init__(self):
        parser = ConfigParser()
        parser.read('resources/conf/config.ini')
        parser.read('/etc/masternaut/mysql-elasticsearch-replicator.ini')

        self.logger = logging.getLogger('replicator')
        logging.basicConfig(level=logging.DEBUG)
        self.MYSQL_SETTINGS = {
            "host": parser.get('mysql', 'host'),
            "port": parser.getint('mysql', 'port'),
            "user": parser.get('mysql', 'user'),
            "passwd": parser.get('mysql', 'password')
        }

        self.transaction_manager = TransactionManager()

    def start(self):
        # server_id is your slave identifier, it should be unique.
        # set blocking to True if you want to block and wait for the next event at
        # the end of the stream

        if self.transaction_manager.get_last_request_sent_timestamp() == '':
            stream = BinLogStreamReader(connection_settings=self.MYSQL_SETTINGS, server_id=3,
                                         only_tables=["Account"],
                                         only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                         blocking=True)
        else:
            timestamp = self.transaction_manager.get_last_request_sent_timestamp() + 1
            stream = BinLogStreamReader(connection_settings=self.MYSQL_SETTINGS,
                                                 only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                                 server_id=3,
                                                 only_tables=["Account"],
                                                 blocking=True,
                                                 skip_to_timestamp=timestamp)

        self.logger.info("Connected to the database at %s:%d with user %s" % (self.MYSQL_SETTINGS.get("host"),
                                                                   self.MYSQL_SETTINGS.get("port"),
                                                                   self.MYSQL_SETTINGS.get("user")))
        es_stream = Elasticsearch()
        for binlogevent in stream:
            for row in binlogevent.rows:
                event = {"schema": binlogevent.schema, "table": binlogevent.table}
                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event = dict(list(event.items()) + list(row["values"].items()))
                    document_id_to_remove = row["values"]["Id"]
                    self.transaction_manager.last_request_sent_timestamp = binlogevent.timestamp
                    self.transaction_manager.write_last_request_sent(binlogevent)
                    doc = es_stream.delete(index=binlogevent.schema, doc_type=binlogevent.table, id=document_id_to_remove)
                    self.transaction_manager.write_last_request_success(binlogevent)
                    self.transaction_manager.last_success_timestamp = binlogevent.timestamp
                    self.logger.info("Deleted document for id %d" % document_id_to_remove)
                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"
                    event = dict(list(event.items()) + list(row["after_values"].items()))
                    document_id_to_update = row["before_values"]["Id"]
                    updated_body = row["after_values"]
                    self.transaction_manager.last_request_sent_timestamp = binlogevent.timestamp
                    self.transaction_manager.write_last_request_sent(binlogevent)
                    doc = es_stream.index(index=binlogevent.schema, doc_type=binlogevent.table, id=document_id_to_update, body=updated_body)
                    self.transaction_manager.write_last_request_success(binlogevent)
                    self.transaction_manager.last_success_timestamp = binlogevent.timestamp
                    self.logger.info("Document for id %d updated to %s" % (document_id_to_update, row["after_values"]))
                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event = dict(list(event.items()) + list(row["values"].items()))
                    document_id_to_add = row["values"]["Id"]
                    self.transaction_manager.last_request_sent_timestamp = binlogevent.timestamp
                    self.transaction_manager.write_last_request_sent(binlogevent)
                    doc = es_stream.index(index=binlogevent.schema, doc_type=binlogevent.table, body=row["values"], id=document_id_to_add)
                    self.transaction_manager.write_last_request_success(binlogevent)
                    self.transaction_manager.last_success_timestamp = binlogevent.timestamp
                    self.logger.info("Adding document %s to the elastic search" % row["values"])
                self.logger.info(json.dumps(event))
                sys.stdout.flush()

if __name__ == "__main__":
    replicator = Replicator()
    replicator.start()

