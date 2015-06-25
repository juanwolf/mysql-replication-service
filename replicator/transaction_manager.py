class TransactionManager:

    def __init__(self):
        self._last_success_file = open('resources/transaction/last_success.tmp', 'r+')
        self._last_request_file = open('resources/transaction/last_request.tmp', 'r+')
        self.last_success_timestamp = int(self._last_success_file.readline())
        self.last_request_sent_timestamp = int(self._last_request_file.readline())

    def get_last_success_id(self):
        return self.last_success_id

    def get_last_request_sent_id(self):
        return self.last_request_sent_id

    def get_last_success_timestamp(self):
        return self.last_success_timestamp

    def get_last_request_sent_timestamp(self):
        return self.last_request_sent_timestamp

    def is_synchronized(self):
        return self.get_last_request_sent_timestamp() == self.get_last_success_timestamp()

    def write_last_request_success(self, log_event):
        timestamp = self.__get_timestamp(log_event)
        self._last_success_file.seek(0)
        self._last_success_file.write('%d' % timestamp)

    def write_last_request_sent(self, log_event):
        timestamp = self.__get_timestamp(log_event)
        self._last_request_file.seek(0)
        self._last_request_file.write('%d' % timestamp)

    def __get_timestamp(self, log_event):
        return log_event.timestamp

    def __del__(self):
        self._last_request_file.close()
        self._last_success_file.close()
