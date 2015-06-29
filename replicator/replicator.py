from configparser import ConfigParser
import json
import logging
import sys

from elasticsearch import Elasticsearch
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import XidEvent

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
from modules_manager import ModulesManager

from transaction_manager import TransactionManager

class Replicator:
    def __init__(self):
        parser = ConfigParser()
        parser.read('resources/conf/config.ini')
        parser.read('/etc/masternaut/mysql-elasticsearch-replicator/config.ini')

        self.logger = logging.getLogger('replicator')
        logging.basicConfig(level=logging.DEBUG)
        self.MYSQL_SETTINGS = {
            "host": parser.get('mysql', 'host'),
            "port": parser.getint('mysql', 'port'),
            "user": parser.get('mysql', 'user'),
            "passwd": parser.get('mysql', 'password')
        }
        self.server_id = parser.getint('mysql', 'server_id')
        self.transaction_manager = TransactionManager()
        self.modules_manager = ModulesManager(config_parser=parser)

    def start(self):
        # server_id is your slave identifier, it should be unique.
        # set blocking to True if you want to block and wait for the next event at
        # the end of the stream

        if self.transaction_manager.get_last_request_sent_timestamp() == '':
            stream = BinLogStreamReader(connection_settings=self.MYSQL_SETTINGS, server_id=self.server_id,
                                         only_tables=["account"],
                                         only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                         blocking=True)
        else:
            timestamp = self.transaction_manager.get_last_request_sent_timestamp() + 1
            stream = BinLogStreamReader(connection_settings=self.MYSQL_SETTINGS,
                                         only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, XidEvent],
                                         server_id=self.server_id,
                                         only_tables=["account"],
                                         blocking=True,
                                         skip_to_timestamp=timestamp)

        self.logger.info("Connected to the database at %s:%d with user %s" % (self.MYSQL_SETTINGS.get("host"),
                                                                   self.MYSQL_SETTINGS.get("port"),
                                                                   self.MYSQL_SETTINGS.get("user")))
        for binlogevent in stream:
            if isinstance(binlogevent, XidEvent):
                self.logger.info("Xid Event detected")
            else:
                for row in binlogevent.rows:
                    event = {"schema": binlogevent.schema, "table": binlogevent.table}
                    if isinstance(binlogevent, DeleteRowsEvent):
                        self.logger.info("Deletion event detected.")
                        event["action"] = "delete"
                        event = dict(list(event.items()) + list(row["values"].items()))
                        document_id_to_remove = row["values"]["Id"]
                        self.transaction_manager.last_request_sent_timestamp = binlogevent.timestamp
                        self.transaction_manager.write_last_request_sent(binlogevent)
                        self.modules_manager.remove_data_all_modules(index=binlogevent.schema, doc_type=binlogevent.table, id=document_id_to_remove)
                        self.transaction_manager.write_last_request_success(binlogevent)
                        self.transaction_manager.last_success_timestamp = binlogevent.timestamp

                        self.logger.info("Deleted document for id %d" % document_id_to_remove)
                    elif isinstance(binlogevent, UpdateRowsEvent):
                        self.logger.info("Updated event detected.")
                        event["action"] = "update"
                        event = dict(list(event.items()) + list(row["after_values"].items()))
                        document_id_to_update = row["before_values"]["Id"]
                        updated_body = row["after_values"]
                        self.transaction_manager.last_request_sent_timestamp = binlogevent.timestamp
                        self.transaction_manager.write_last_request_sent(binlogevent)
                        self.modules_manager.update_data_all_modules(index=binlogevent.schema, doc_type=binlogevent.table, id=document_id_to_update, doc=updated_body)
                        self.transaction_manager.write_last_request_success(binlogevent)
                        self.transaction_manager.last_success_timestamp = binlogevent.timestamp
                        self.logger.info("Document for id %d updated to %s" % (document_id_to_update, row["after_values"]))
                    elif isinstance(binlogevent, WriteRowsEvent):
                        self.logger.info("Insert event detected.")
                        event["action"] = "insert"
                        event = dict(list(event.items()) + list(row["values"].items()))
                        document_id_to_add = row["values"]["Id"]
                        self.transaction_manager.last_request_sent_timestamp = binlogevent.timestamp
                        self.transaction_manager.write_last_request_sent(binlogevent)
                        self.modules_manager.insert_data_all_modules(index=binlogevent.schema, doc_type=binlogevent.table, doc=row["values"], id=document_id_to_add)
                        self.transaction_manager.write_last_request_success(binlogevent)
                        self.transaction_manager.last_success_timestamp = binlogevent.timestamp
                        self.logger.info("Adding document %s to the elastic search" % row["values"])
    #                self.logger.info(json.dumps(event))
                sys.stdout.flush()

if __name__ == "__main__":
    replicator = Replicator()
    replicator.start()

