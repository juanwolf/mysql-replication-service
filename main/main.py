from pymysqlreplication import BinLogStreamReader

MYSQL_SETTINGS = {
    "host": "db",
    "port": 3306,
    "user": "root",
    "passwd": ""
}


def main():
    # server_id is your slave identifier, it should be unique.
    # set blocking to True if you want to block and wait for the next event at
    # the end of the stream
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id=3,
                                blocking=True)

    for binlogevent in stream:
        binlogevent.dump()

    stream.close()


if __name__ == "__main__":
    main()