from configparser import ConfigParser
import json
import sys

from elasticsearch import Elasticsearch
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)

from mysql_connector import MySQLConnector
from transaction_manager import TransactionManager

parser = ConfigParser()
parser.read('resources/conf/config.ini')

MYSQL_SETTINGS = {
    "host": parser.get('mysql', 'host'),
    "port": parser.getint('mysql', 'port'),
    "user": parser.get('mysql', 'user'),
    "passwd": parser.get('mysql', 'password')
}



def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    print(MYSQL_SETTINGS)
    transaction_manager = TransactionManager()
    if transaction_manager.get_last_request_sent_timestamp() == '':
        mysql_connector = MySQLConnector(MYSQL_SETTINGS, [DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent], blocking=True,
                                     id_label="Id")
    else:
        timestamp = transaction_manager.get_last_request_sent_timestamp() + 1
        mysql_connector = MySQLConnector(MYSQL_SETTINGS, [DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent], blocking=True,
                                        id_label="Id", skip_to_timestamp=timestamp)

    print("Connected to the database at %s:%d with user %s" % (MYSQL_SETTINGS.get("host"),
                                                               MYSQL_SETTINGS.get("port"),
                                                               MYSQL_SETTINGS.get("user")))
    es_stream = Elasticsearch()
    for binlogevent in mysql_connector.get_stream():
        for row in binlogevent.rows:
            event = {"schema": binlogevent.schema, "table": binlogevent.table}
            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event = dict(list(event.items()) + list(row["values"].items()))
                document_id_to_remove = row["values"]["Id"]
                transaction_manager.write_last_request_sent(binlogevent)
                doc = es_stream.delete(index="fleet", doc_type="account", id=document_id_to_remove)
                transaction_manager.write_last_request_success(binlogevent)
                print("Deleted document for id %d" % document_id_to_remove)
            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event = dict(list(event.items()) + list(row["after_values"].items()))
                document_id_to_update = row["before_values"]["Id"]
                updated_body = row["after_values"]
                transaction_manager.write_last_request_sent(binlogevent)
                doc = es_stream.index(index="fleet", doc_type="account", id=document_id_to_update, body=updated_body)
                transaction_manager.write_last_request_success(binlogevent)
                print("Document for id %d updated to %s" % (document_id_to_update, row["after_values"]))
            elif isinstance(binlogevent, WriteRowsEvent):
                event["action"] = "insert"
                event = dict(list(event.items()) + list(row["values"].items()))
                document_id_to_add = row["values"]["Id"]
                print(row['values'])
                transaction_manager.write_last_request_sent(binlogevent)
                doc = es_stream.index(index="fleet", doc_type="account", body=row["values"], id=document_id_to_add)
                transaction_manager.write_last_request_success(binlogevent)
                print("Adding ", doc)
            print(json.dumps(event))
            sys.stdout.flush()


if __name__ == "__main__":
    main()