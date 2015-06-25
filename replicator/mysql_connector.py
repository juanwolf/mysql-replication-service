from pymysqlreplication import BinLogStreamReader


class MySQLConnector:
    """
    The Elastic Replicator create a stream to the MySQL database
    and allow to send the specific row to the elastic.
    """

    def __init__(self, mysql_settings, events, blocking, id_label="_id", skip_to_timestamp=None):
        self.stream = BinLogStreamReader(connection_settings=mysql_settings, server_id=3,
                                         only_events=events,
                                         blocking=blocking,
                                         skip_to_timestamp=skip_to_timestamp)
        self.id_label = id_label

    def get_id_label(self):
        return self.id_label

    def get_stream(self):
        return self.stream

    def get_id(self, row):
        return row[self.id_label]

    def __del__(self):
        self.stream.close()
