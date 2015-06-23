from configparser import ConfigParser
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
import sys


parser = ConfigParser()
parser.read('resources/config.ini')

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
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                blocking=True)
    # TODO Not reexecute action already found
    es_stream = Elasticsearch()
    for binlogevent in stream:
        for row in binlogevent.rows:
            event = {"schema": binlogevent.schema, "table": binlogevent.table}
            if isinstance(binlogevent, DeleteRowsEvent):
                event["action"] = "delete"
                event = dict(list(event.items()) + list(row["values"].items()))
            elif isinstance(binlogevent, UpdateRowsEvent):
                event["action"] = "update"
                event = dict(list(event.items()) + list(row["after_values"].items()))
            elif isinstance(binlogevent, WriteRowsEvent):

                event["action"] = "insert"
                event = dict(list(event.items()) + list(row["values"].items()))
                print(row['values'])
                doc = es_stream.index(index="fleet", doc_type="account", body=row["values"], timestamp=datetime.now())
                print("Adding ", doc)
            print(json.dumps(event))
            sys.stdout.flush()

    stream.close()


if __name__ == "__main__":
    main()