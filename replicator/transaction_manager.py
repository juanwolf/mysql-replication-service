class TransactionManager:

    def __init__(self):
        self._last_success_file = open('resources/transaction/last_success.tmp', 'r+')
        self._last_request_file = open('resources/transaction/last_request.tmp', 'r+')
        line = self._last_success_file.readline()
        if line != "":
            self.last_success = int(line)
        line = self._last_request_file.readline()
        if line != "":
            self.last_request_sent = int(line)
        self.last_success_timestamp = 0
        self.last_request_sent_timestamp = 0
        self.number_of_create_request = 0
        self.number_of_delete_request = 0
        self.number_of_update_request = 0

    @property
    def number_of_transactions(self):
        return self.number_of_create_request + self.number_of_delete_request + self.number_of_update_request

    @property
    def latency(self):
        return self.last_success_timestamp - self.last_request_sent_timestamp

    @property
    def is_synchronized(self):
        return self.last_request_sent_timestamp == self.last_success_timestamp

    def __get_timestamp(self, log_event):
        return log_event.timestamp

    def write_last_request_log_pos(self, stream, log_event):
        log_pos = stream.log_pos
        self._last_request_file.seek(0)
        self._last_request_file.write('%d' % log_pos)
        self.last_request_sent_timestamp = self.__get_timestamp(log_event)

    def write_last_success_log_pos(self, stream, log_event):
        log_pos = stream.log_pos
        self._last_success_file.seek(0)
        self._last_success_file.write('%d' % log_pos)
        self.last_success_timestamp = log_event.timestamp


    def __del__(self):
        self._last_request_file.close()
        self._last_success_file.close()
