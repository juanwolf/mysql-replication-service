from configparser import ConfigParser
import logging
import sys
from pymysqlreplication import BinLogStreamReader

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
from replicator.modules_manager import ModulesManager

from replicator.transaction_manager import TransactionManager



class Replicator:

    def __check_configuration(self, parser):
        """
        Check that the configuration is usable. For example, it will raise an error.
        """
        if not parser.has_section('core'):
            self.logger.error('The config file should contain a core section with at least the module_path specified')
            sys.exit(1)

        else:
            if parser.get('core', 'modules_path', fallback=None) is None:
                self.logger.error('The configuration file should contain at least the modules_path value in core section.')
                sys.exit(1)

        if not parser.has_section('mysql'):
            self.logger.error('The config file should contain a mysql section.')
            sys.exit(1)

        else:
            if parser.get('mysql', 'host', fallback=None) is None:
                self.logger.error('The config file should contain the host value in mysql section.')
                sys.exit(1)

            if parser.get('mysql', 'port', fallback=None) is None:
                self.logger.error('The config file should contain the port value in mysql section.')
                sys.exit(1)

            if parser.get('mysql', 'user', fallback=None) is None:
                self.logger.error('The config file should contain the user in mysql section.')
                sys.exit(1)

            if parser.get('mysql', 'password', fallback=None) is None:
                self.logger.error('The config file should contain the password of the user in mysql section.')
                sys.exit(1)

            if parser.get('mysql', 'server_id', fallback=None) is None:
                self.logger.error('The config file should contain the server_id in mysql section.')
                sys.exit(1)

            if parser.get('mysql', 'tables', fallback=None) is not None:
                tables = [table.strip() for table in parser.get('mysql', 'tables').split(',')]
                for table in tables:
                    if not parser.has_section(table):
                        self.logger.error('The config file should contain a section about the table : %s' % table)
                        exit(1)





    def __init__(self, parser):
        """
        :param parser:
         the config_parser  MUST be initialized and had read a least one file.
        """
        self.logger = logging.getLogger('replicator')
        # Feel free to comment this section if you don't want a log stream in the console.
        if parser.get('core', 'log.level') == 'DEBUG':
            self.logger.addHandler(logging.StreamHandler())

        self.__check_configuration(parser)
        self.MYSQL_SETTINGS = {
            "host": parser.get('mysql', 'host'),
            "port": parser.getint('mysql', 'port'),
            "user": parser.get('mysql', 'user'),
            "passwd": parser.get('mysql', 'password')
        }
        if parser.get('mysql', 'databases', fallback=None) is not None:
            self.databases = [db.strip() for db in parser.get('mysql', 'databases').split(',')]

        else:
            self.databases = None

        if parser.get('mysql', 'tables', fallback=None) is not None:
            self.tables = [table.strip() for table in parser.get('mysql', 'tables').split(',')]

        else:
            self.tables = None

        self.server_id = parser.getint('mysql', 'server_id')
        self.transaction_manager = TransactionManager()
        self.modules_manager = ModulesManager(config_parser=parser)
        self.indexes_label = {
            table: parser.get(table, 'index_label')
            for table in self.tables
        }

    def start(self):
        # server_id is your slave identifier, it should be unique.
        # set blocking to True if you want to block and wait for the next event at
        # the end of the stream
        self.modules_manager.generate_modules_instances()
        if hasattr(self.transaction_manager, 'last_request_sent'):
            stream = BinLogStreamReader(connection_settings=self.MYSQL_SETTINGS,
                                        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                        server_id=self.server_id,
                                        only_schemas=self.databases,
                                        only_tables=self.tables,
                                        blocking=True,
                                        resume_stream=True,
                                        log_pos=self.transaction_manager.last_request_sent)

        else:
            stream = BinLogStreamReader(connection_settings=self.MYSQL_SETTINGS, server_id=self.server_id,
                                        only_schemas=self.databases,
                                        only_tables=self.tables,
                                        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
                                        blocking=True)

        self.logger.info("Connected to the database at %s:%d with user %s" % (self.MYSQL_SETTINGS.get("host"),
                                                                              self.MYSQL_SETTINGS.get("port"),
                                                                              self.MYSQL_SETTINGS.get("user")))

        for binlogevent in stream:
            for row in binlogevent.rows:
                event = {"schema": binlogevent.schema, "table": binlogevent.table}
                if isinstance(binlogevent, DeleteRowsEvent):
                    self.logger.info("Deletion event detected.")
                    event["action"] = "delete"
                    event = dict(list(event.items()) + list(row["values"].items()))
                    document_id_to_remove = row["values"][self.indexes_label[binlogevent.table]]
                    self.transaction_manager.write_last_request_log_pos(stream, binlogevent)
                    self.modules_manager.remove_data_all_modules(index=binlogevent.schema, doc_type=binlogevent.table, id=document_id_to_remove)
                    self.transaction_manager.number_of_delete_request += 1
                    self.transaction_manager.write_last_success_log_pos(stream, binlogevent)
                    self.logger.info("Deleted document for id {0}".format(document_id_to_remove))

                elif isinstance(binlogevent, UpdateRowsEvent):
                    self.logger.info("Updated event detected.")
                    event["action"] = "update"
                    event = dict(list(event.items()) + list(row["after_values"].items()))
                    document_id_to_update = row["before_values"][self.indexes_label[binlogevent.table]]
                    updated_body = row["after_values"]
                    self.transaction_manager.write_last_request_log_pos(stream, binlogevent)
                    self.modules_manager.update_data_all_modules(index=binlogevent.schema,
                                                                 doc_type=binlogevent.table,
                                                                 id=document_id_to_update,
                                                                 doc=updated_body)
                    self.transaction_manager.number_of_update_request += 1
                    self.transaction_manager.write_last_success_log_pos(stream, binlogevent)
                    self.logger.info("Document for id {0} updated to {1}".format(document_id_to_update, row["after_values"]))

                elif isinstance(binlogevent, WriteRowsEvent):
                    self.logger.info("Insert event detected.")
                    event["action"] = "insert"
                    event = dict(list(event.items()) + list(row["values"].items()))
                    document_id_to_add = row["values"][self.indexes_label[binlogevent.table]]
                    self.transaction_manager.write_last_request_log_pos(stream, binlogevent)
                    self.modules_manager.insert_data_all_modules(index=binlogevent.schema, doc_type=binlogevent.table,
                                                                 doc=row["values"], id=document_id_to_add)
                    self.transaction_manager.write_last_success_log_pos(stream, binlogevent)
                    self.transaction_manager.number_of_create_request += 1
                    self.logger.info("Adding document {0} to the elastic search".format(row["values"]))
                    #self.logger.info(json.dumps(event))

            sys.stdout.flush()

if __name__ == "__main__":
    parser = ConfigParser()
    parser.read('resources/conf/config.ini')
    parser.read('/etc/masternaut/mysql-elasticsearch-replicator/config.ini')
    replicator = Replicator(parser)
    replicator.start()

